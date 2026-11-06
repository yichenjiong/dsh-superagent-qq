/** QQ gateway lifecycle: one GatewayClient per configured bot account; message
 *  events become Triggers (stable ids dedupe server redeliveries); connection
 *  state reports to health each poll round, which also throttles reconnection
 *  (one attempt per 5s round). File/video attachments are saved to disk before
 *  receive; images become markers the assessment downloads on demand. */
import { mkdir, rename } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'
import type { SourceContextLike, TriggerLike } from './types.ts'
import { channelConfig, HEALTH_ENTRY } from './qq-kind.ts'
import type { QqAccount, QqConfig } from './qq-kind.ts'
import { GatewayClient, type GatewayEvent } from './gateway.ts'
import { attachmentKey, downloadQqMediaToFile, isImageContentType, mediaKindOf, type MediaDownloadOpts } from './robot.ts'

export interface QqAttachment { content_type?: string; filename?: string; url?: string; [key: string]: unknown }

/** Inbound message payload (`d`) of GROUP_AT_MESSAGE_CREATE / C2C_MESSAGE_CREATE;
 *  field names verified against real traffic by scripts/probe.mjs (M1 gate);
 *  attachments[{content_type, filename, url}] is an M2 spike item. */
export interface QqMessagePayload {
  id?: string
  content?: string
  group_openid?: string
  user_openid?: string
  timestamp?: string
  attachments?: QqAttachment[]
  author?: { member_openid?: string; user_openid?: string; [key: string]: unknown }
  [key: string]: unknown
}

export function pause(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) { resolve(); return }
    const timer = setTimeout(() => { signal.removeEventListener('abort', stop); resolve() }, ms)
    const stop = () => { clearTimeout(timer); resolve() }
    signal.addEventListener('abort', stop, { once: true })
  })
}

/** Sanitize an attachment's original filename before embedding it in the
 *  trigger text: strip control characters and newlines, collapse whitespace,
 *  and cap the length (prompt-injection hardening). */
const safeFileName = (name: string): string =>
  name.replace(/[\u0000-\u001f\u007f]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 80)

const isoAt = (d: QqMessagePayload): string => {
  const ts = typeof d.timestamp === 'string' ? d.timestamp : undefined
  if (ts) {
    const parsed = new Date(ts)
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString()
  }
  return new Date().toISOString()
}

const MEDIA_ROOT = join(homedir(), '.dsh', 'qq-attachments')

const MEDIA_EXTS: Record<string, string> = {
  'video/mp4': '.mp4', 'video/quicktime': '.mov', 'video/x-matroska': '.mkv', 'video/webm': '.webm',
  'audio/mpeg': '.mp3', 'audio/mp4': '.m4a', 'audio/aac': '.aac', 'audio/wav': '.wav', 'audio/x-wav': '.wav', 'audio/amr': '.amr',
  'application/pdf': '.pdf', 'application/msword': '.doc', 'application/vnd.ms-excel': '.xls',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
  'application/zip': '.zip', 'text/plain': '.txt', 'text/markdown': '.md',
}

const extFromName = (name: string): string | undefined => {
  const m = /\.[a-z0-9]{1,6}$/i.exec(name)
  return m ? m[0].toLowerCase() : undefined
}

export interface DownloadedMedia { kind: 'file' | 'video' | 'audio'; url: string; key: string; path?: string; name: string; error?: string }

/** 收消息即把 file/video/audio 附件落盘到 ~/.dsh/qq-attachments/<msgId>/，路径
 *  注入 trigger 文本供 agent 的 read/bash 工具消化；单件失败降级不阻塞。
 *  图片不走这里：以 [qq-image:KEY] 标记内嵌，评估时按需经 threadContext.image 下载。 */
export async function downloadMessageMedia(d: QqMessagePayload, opts?: { root?: string; download?: MediaDownloadOpts }): Promise<DownloadedMedia[]> {
  const attachments = Array.isArray(d.attachments) ? d.attachments : []
  const items: { kind: 'file' | 'video' | 'audio'; url: string; key: string; name: string }[] = []
  for (const a of attachments) {
    if (!a || typeof a.url !== 'string' || !a.url || isImageContentType(a.content_type)) continue
    items.push({ kind: mediaKindOf(a.content_type), url: a.url, key: attachmentKey(a.url),
      name: (typeof a.filename === 'string' ? a.filename : '').trim() })
  }
  if (items.length === 0) return []
  const dir = join(opts?.root ?? MEDIA_ROOT, (String(d.id ?? 'msg')).replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 60))
  const out: DownloadedMedia[] = []
  for (const [i, item] of items.entries()) {
    try {
      await mkdir(dir, { recursive: true })
      const base = (item.name.replace(/\.[^./\\]*$/, '') || 'media-' + (i + 1)).replace(/[^\w\u4e00-\u9fa5.-]/g, '_').slice(0, 60)
      const partPath = join(dir, base + '.part')
      // 断点续传：.part 保留跨重推进度；mime 到手后 rename 成带扩展名的最终名。
      const { bytes, mime } = await downloadQqMediaToFile(item.url, partPath, opts?.download)
      const ext = MEDIA_EXTS[mime] ?? extFromName(item.name) ?? (item.kind === 'video' ? '.mp4' : item.kind === 'audio' ? '.mp3' : '')
      const path = join(dir, base + ext)
      await rename(partPath, path)
      out.push({ ...item, path })
      console.error('[qq] media saved: kind=' + item.kind + ' bytes=' + bytes + ' path=' + path)
    } catch (error) {
      out.push({ ...item, error: String(error).slice(0, 160) })
      console.error('[qq] media download failed: kind=' + item.kind + ' ' + String(error).slice(0, 200))
    }
  }
  return out
}

/** Event -> Trigger; undefined for anything we cannot map (logged and skipped). */
export function buildTrigger(account: QqAccount, d: QqMessagePayload, media: DownloadedMedia[] = [], at?: string): TriggerLike | undefined {
  const msgId = String(d.id ?? '')
  const content = String(d.content ?? '').trim()
  const attachments = Array.isArray(d.attachments) ? d.attachments : []
  if (!msgId || (!content && attachments.length === 0)) return undefined
  const groupOpenid = typeof d.group_openid === 'string' && d.group_openid ? d.group_openid : undefined
  // Real C2C events carry the user openid only in author.user_openid (the
  // top-level user_openid is empty); group events still use top-level group_openid.
  const userOpenid = typeof d.user_openid === 'string' && d.user_openid
    ? d.user_openid
    : !(typeof d.group_openid === 'string' && d.group_openid) && typeof d.author?.user_openid === 'string' && d.author.user_openid
      ? d.author.user_openid
      : undefined
  // 群聊发件人在 author.member_openid；单聊在 author.user_openid / user_openid（字段名 M1 真机核对）。
  const actorId = String(d.author?.member_openid ?? d.author?.user_openid ?? d.user_openid ?? '')
  if (!actorId) return undefined
  const atIso = at || isoAt(d)
  // 标记 KEY 由附件 url 派生（attachmentKey），与消息原文解耦：用户手打的假标记
  // 因哈希无法对上任何真实附件而失效；提取侧只认原文出现过的 key（qqImageKeys）。
  const markers: string[] = []
  const imageCount: { n: number } = { n: 0 }
  const stored: { url: string; contentType?: string; filename?: string; key: string }[] = []
  for (const a of attachments) {
    if (!a || typeof a.url !== 'string' || !a.url) continue
    const key = attachmentKey(a.url)
    stored.push({
      url: a.url,
      ...(typeof a.content_type === 'string' && a.content_type ? { contentType: a.content_type } : {}),
      ...(typeof a.filename === 'string' && a.filename ? { filename: a.filename } : {}),
      key,
    })
    if (isImageContentType(a.content_type)) { markers.push('[qq-image:' + key + ']'); imageCount.n += 1 }
    else markers.push('[qq-file:' + key + ']')
  }
  let text = content
  if (markers.length > 0) text = text ? text + '\n' + markers.join('\n') : markers.join('\n')
  if (media.length > 0) {
    const notes = media.map((m) => {
      const label = m.kind === 'video' ? '视频' : m.kind === 'audio' ? '语音' : '文件'
      if (m.path) {
        const hint = m.kind === 'video' ? '如需理解内容，可用 ffmpeg 抽帧后用 read_image 查看' : '用 read/bash 工具查看内容'
        return '[' + label + '已保存: ' + m.path + (m.name ? '（原名: ' + safeFileName(m.name) + '）' : '') + ' —— ' + hint + ']'
      }
      return '[QQ' + label + '下载失败' + (m.error ? ': ' + m.error : '') + ']'
    })
    text = text ? text + '\n' + notes.join('\n') : notes.join('\n')
  }
  text = text.trim()
  if (!text) return undefined
  if (imageCount.n > 0) console.error('[qq] images embedded: n=' + imageCount.n + ' msgId=' + msgId)
  if (groupOpenid) {
    return {
      id: 'qq:' + msgId,
      source: 'qq',
      threads: [{ source: 'qq', id: 'g:' + groupOpenid }],
      actor: { source: 'qq', id: actorId },
      human: true,
      addressed: 'mention',
      at: atIso,
      text,
      channel: { source: 'qq', thread: 'qq:g:' + groupOpenid,
        address: { account: account.appId, scope: 'group', openid: groupOpenid, msgId } },
      meta: { accountId: account.appId, scopeKind: 'group', groupOpenid, messageId: msgId,
        ...(stored.length > 0 ? { attachments: stored } : {}) },
    }
  }
  if (userOpenid) {
    return {
      id: 'qq:' + msgId,
      source: 'qq',
      threads: [{ source: 'qq', id: 'c:' + userOpenid }],
      actor: { source: 'qq', id: actorId },
      human: true,
      addressed: 'direct',
      at: atIso,
      text,
      channel: { source: 'qq', thread: 'qq:c:' + userOpenid,
        address: { account: account.appId, scope: 'c2c', openid: userOpenid, msgId } },
      meta: { accountId: account.appId, scopeKind: 'c2c', messageId: msgId,
        ...(stored.length > 0 ? { attachments: stored } : {}) },
    }
  }
  return undefined
}

const HANDLED_EVENTS = new Set(['GROUP_AT_MESSAGE_CREATE', 'C2C_MESSAGE_CREATE'])

async function handleEvent(source: SourceContextLike, account: QqAccount, event: GatewayEvent, dumped: Set<string>): Promise<void> {
  const t = String(event.t ?? '')
  if (!HANDLED_EVENTS.has(t)) {
    console.error('[qq] event ignored: t=' + (t || '(none)'))
    return
  }
  const d = (event.d ?? {}) as QqMessagePayload
  if (process.env['QQ_DEBUG_PAYLOAD'] === '1' && !dumped.has(account.appId)) {
    dumped.add(account.appId)
    // 每账号首条消息 dump 原始 payload（≤500 字符）：核对真实字段名（M1/M2，含 attachments）。
    console.error('[qq] first payload dump (' + account.appId + ' ' + t + '): ' + JSON.stringify(d).slice(0, 500))
  }
  // 附件先落盘（带超时降级），receive 时路径已就位；图片只嵌标记，不在此下载。
  const media = await downloadMessageMedia(d)
  const trigger = buildTrigger(account, d, media)
  if (!trigger) {
    console.error('[qq] skip msg: t=' + t + ' id=' + String(d.id ?? '') + ' (missing fields after parse)')
    return
  }
  console.error('[qq] msg in: t=' + t + ' msgId=' + String(d.id ?? '') + ' scope=' + String(trigger.meta.scopeKind)
    + ' actor=' + trigger.actor.id + ' media=' + media.length + ' text="' + trigger.text.slice(0, 60) + '"')
  await source.receive(trigger)
}

interface WatchState {
  hash: string
  clients: Map<string, GatewayClient>
  /** first raw message payload per account, dumped once for field-name verification (M1) */
  dumped: Set<string>
}

function disconnectAll(state: WatchState): void {
  for (const client of state.clients.values()) client.disconnect()
  state.clients.clear()
}

export async function ensureClients(source: SourceContextLike, state: WatchState, config: QqConfig): Promise<void> {
  const hash = JSON.stringify([config.accounts, config.groups, config.directMessages])
  if (state.hash !== hash) {
    // 配置变化：全部断开重建（含被移除的账号）。
    disconnectAll(state)
    state.dumped.clear()
    state.hash = hash
  }
  for (const account of config.accounts) {
    let client = state.clients.get(account.appId)
    if (!client) {
      client = new GatewayClient(account, {
        onEvent: (event) => {
          void handleEvent(source, account, event, state.dumped).catch((error: unknown) => {
            console.error('[qq] event handling failed (' + account.appId + '): ' + String(error).slice(0, 200))
          })
        },
        onConn: (connected) => { console.error('[qq] ' + account.appId + (connected ? ' gateway ready' : ' gateway closed')) },
      })
      state.clients.set(account.appId, client)
      try {
        await client.connect()
      } catch (error) {
        state.clients.delete(account.appId)
        source.report(HEALTH_ENTRY, { state: 'error', detail: account.appId + ': ' + String(error).slice(0, 200) })
        continue
      }
    } else if (!client.isConnected()) {
      // 每轮 5s 重连即节流；偶发双连由引擎按 trigger id 去重兜底。
      try {
        await client.connect()
      } catch (error) {
        source.report(HEALTH_ENTRY, { state: 'error', detail: account.appId + ': ' + String(error).slice(0, 200) })
        continue
      }
    }
  }
  const total = config.accounts.length
  const connected = config.accounts.filter((a) => state.clients.get(a.appId)?.isConnected()).length
  if (connected === total) {
    source.report(HEALTH_ENTRY, { state: 'ready', detail: 'gateway connected (' + total + ' bots)' })
  } else {
    source.report(HEALTH_ENTRY, { state: 'error', detail: 'connected ' + connected + '/' + total + '; reconnecting on next round' })
  }
}

export async function watch(source: SourceContextLike, signal: AbortSignal): Promise<void> {
  const state: WatchState = { hash: '', clients: new Map(), dumped: new Set() }
  while (!signal.aborted) {
    try {
      const config = source.config<QqConfig>('qq')
      const accounts = config?.accounts ?? []
      const valid = accounts.length > 0 && accounts.every((a) => a.appId && a.appSecret)
      if (config && valid) {
        channelConfig.current = config
        await ensureClients(source, state, config)
      } else {
        disconnectAll(state)
        state.hash = ''
        channelConfig.current = undefined
        source.report(HEALTH_ENTRY, { state: 'paused', detail: 'waiting for appId / AppSecret in settings' })
      }
    } catch (error) {
      source.report(HEALTH_ENTRY, { state: 'error', detail: String(error).slice(0, 300) })
    }
    await pause(5000, signal)
  }
  disconnectAll(state)
}
