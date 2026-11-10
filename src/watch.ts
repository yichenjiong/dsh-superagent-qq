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
import type { QqAccount, QqConfig, QqGroup } from './qq-kind.ts'
import { GatewayClient, type GatewayEvent } from './gateway.ts'
import { attachmentKey, downloadQqMediaToFile, isImageContentType, mediaKindOf, sendPassiveReply, type MediaDownloadOpts } from './robot.ts'

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

/** 清洗附件原名后再拼进 trigger 文本：去掉控制字符与换行、压缩空白、限制长度，
 *  避免文件名里的换行/指令性文字被当成上下文（提示词注入面）。 */
const safeFileName = (name: string): string =>
  name.replace(/[\u0000-\u001f\u007f]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 80)

/** A binding code as the core prints it: BIND-XXXX-XXXX, case and separators insensitive. */
const BIND_CODE = /\bBIND[\s_-]*([A-Za-z0-9]{4})[\s_-]*([A-Za-z0-9]{4})\b/i
export const bindCodeOf = (text: string): string | undefined => {
  const match = BIND_CODE.exec(text)
  return match ? 'BIND-' + match[1]!.toUpperCase() + '-' + match[2]!.toUpperCase() : undefined
}
/** What a group message asks for: a prompt, a code to redeem, a bound group, or a rebound one. */
export type GroupRoute =
  | { kind: 'none' }
  | { kind: 'prompt' }
  | { kind: 'redeem'; code: string }
  | { kind: 'rebind'; code: string }
  | { kind: 'bound'; repository: string }
export function groupRoute(config: QqConfig | undefined, appId: string, groupOpenid: string | undefined,
  text: string): GroupRoute {
  if (!groupOpenid) return { kind: 'none' }
  const row: QqGroup | undefined = config?.groups.find(item => item.groupOpenid === groupOpenid && item.appId === appId)
  const code = bindCodeOf(text)
  const bound = row?.repository || ''
  if (code) return bound ? { kind: 'rebind', code } : { kind: 'redeem', code }
  if (bound) return { kind: 'bound', repository: bound }
  return { kind: 'prompt' }
}
/** The connection identity: accounts, direct-message switch, and group membership. A repository change must not reconnect. */
export function connectionHash(config: QqConfig): string {
  return JSON.stringify([config.accounts, config.directMessages,
    [...config.groups].map(group => [group.appId, group.groupOpenid]).sort((a, b) => String(a).localeCompare(String(b)))])
}

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
  // 实测 C2C 事件只在 author.user_openid 里带用户 openid（顶层 user_openid 为空）；群事件仍看顶层 group_openid。
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

interface EventDeps {
  send?: typeof sendPassiveReply
  download?: typeof downloadMessageMedia
}
const bindReply = (result: Awaited<ReturnType<NonNullable<SourceContextLike['redeemBind']>>>, rebound: boolean): string => {
  if (result.ok) return `${rebound ? '已改绑到' : '已绑定'} ${result.project}（${result.repository}）`
  switch (result.reason) {
    case 'expired': return '这个绑定码已过期，请私聊管理员重新获取。'
    case 'used': return '这个绑定码已经用过了，请私聊管理员重新获取。'
    case 'live-ticket': return '这个群还有进行中的工单，不能改绑；请新建群，或先结束工单。'
    case 'already-bound': return '这个群已经绑定项目了。'
    case 'source-not-installed': return '绑定服务不可用，请确认 QQ 插件与核心版本匹配。'
    case 'conflict': return '保存绑定时出错，请重试。'
    default: return '绑定码无效，请私聊管理员获取新的绑定码。'
  }
}

export async function handleEvent(source: SourceContextLike, account: QqAccount, event: GatewayEvent, dumped: Set<string>,
  deps: EventDeps = {}): Promise<void> {
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
  const send = deps.send ?? sendPassiveReply
  const download = deps.download ?? downloadMessageMedia
  const msgId = String(d.id ?? '')
  const groupOpenid = typeof d.group_openid === 'string' && d.group_openid ? d.group_openid : undefined
  const config = source.config<QqConfig>('qq')
  const text = String(d.content ?? '')
  const group = groupRoute(config, account.appId, groupOpenid, text)
  const reply = async (body: string): Promise<void> => {
    if (!groupOpenid || !msgId) return
    try {
      await send(account.appId, account.appSecret, { scope: 'group', openid: groupOpenid, msgId }, body)
    } catch (error) {
      console.error('[qq] bind reply failed: group=' + groupOpenid + ' ' + String(error).slice(0, 200))
    }
  }
  // Binding is decided before downloading anything: an unbound group or a code message must not fetch its attachments.
  if (group.kind === 'prompt') {
    console.error('[qq] unbound group: appId=' + account.appId + ' group=' + groupOpenid + ' msgId=' + msgId)
    await reply('这个群还没绑定项目，请私聊管理员获取绑定码。')
    return
  }
  if (group.kind === 'redeem' || group.kind === 'rebind') {
    console.error('[qq] bind code: appId=' + account.appId + ' group=' + groupOpenid + ' msgId=' + msgId + ' rebind=' + (group.kind === 'rebind'))
    const groupName = config?.groups.find(row => row.appId === account.appId && row.groupOpenid === groupOpenid)?.name
    const result = source.redeemBind
      ? await source.redeemBind(group.code, { appId: account.appId, groupOpenid: groupOpenid!, ...(groupName ? { groupName } : {}),
        actorId: String(d.author?.member_openid ?? d.author?.user_openid ?? d.user_openid ?? '') })
      : { ok: false as const, reason: 'source-not-installed' as const }
    await reply(bindReply(result, group.kind === 'rebind'))
    return
  }
  // 附件先落盘（带超时降级），receive 时路径已就位；图片只嵌标记，不在此下载。
  const media = await download(d)
  const trigger = buildTrigger(account, d, media)
  if (!trigger) {
    console.error('[qq] skip msg: t=' + t + ' id=' + msgId + ' (missing fields after parse)')
    return
  }
  if (group.kind === 'bound') {
    trigger.channel!.address.repository = group.repository
    trigger.meta.repository = group.repository
  }
  console.error('[qq] msg in: t=' + t + ' msgId=' + msgId + ' scope=' + String(trigger.meta.scopeKind)
    + ' actor=' + trigger.actor.id + ' media=' + media.length + ' repository=' + String(trigger.meta.repository ?? '-')
    + ' text="' + trigger.text.slice(0, 60) + '"')
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
  const hash = connectionHash(config)
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
