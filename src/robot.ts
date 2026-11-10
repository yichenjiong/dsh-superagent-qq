/** QQ official-bot passive reply helpers + attachment downloads.
 *
 * Contract sources: official docs bot.q.qq.com/wiki/develop/api-v2 —
 * server-inter/message/send-receive/send (read 2026-11-04): a passive reply is
 * POST {API_BASE}/v2/groups/{group_openid}/messages or
 * {API_BASE}/v2/users/{user_openid}/messages with body
 * { msg_type: 0, msg_id, msg_seq, content }; msg_id quotes the inbound message
 * and opens the passive-reply window, msg_seq orders several replies under one
 * msg_id (starts at 1, must increase per msg_id). REST auth headers come from
 * gateway.ts authHeaders (QQBot token + x-union-appid).
 * 被动回复窗口时长与单条 msg_id 的回复条数上限：M2 真机实测后再固化行为
 * （窗口过期/超条的降级策略，例如改发主动消息或静默丢弃，待实测数据）。
 *
 * Attachment downloads (M2 wiring, field names pending real-traffic spike):
 * message events carry d.attachments[{content_type, filename, url}]. Whether
 * the url needs auth headers is UNKNOWN — we fetch it directly with no headers
 * and fail loudly (error text points at the spike); do not guess headers until
 * verified on a real bot. Segmented/resumable download mirrors the dingtalk
 * sibling plugin's downloadDingtalkMediaToFile (Range + retry backoff + cap).
 */
import { mkdir, open, realpath, stat, unlink } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { constants as FS } from 'node:fs'
import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { homedir } from 'node:os'
import { API_BASE, authHeaders } from './gateway.ts'

/** Per-inbound-msg_id reply sequence numbers; the map is capped to bound
 *  memory on long-running processes (evict the oldest msg_id when full). */
const MESSAGE_SEQ_LIMIT = 2000
/** msgSeq 两层模型（SPEC v3 §4）：
 *  - msgCounters[msgId]：同一入站消息下所有出站消息的递增序号；
 *  - messageSeqs[messageKey]：某条出站消息（含 #0/#1 文件、#summary）已分配的序号，
 *    Outbox 重试必须复用，否则 QQ 会把它当成新消息重复发送。
 *  每次分配同步落盘；落盘失败只降级为内存态，不影响发送。 */
/** Resolved lazily: tests (and operators) may point QQ_SEQ_STORE at a temp file
 *  after this module was imported. */
const seqStorePath = (): string => process.env.QQ_SEQ_STORE || join(homedir(), '.dsh', 'qq-msg-seq.json')
const messageSeqs = new Map<string, { msgId: string; seq: number }>()
const msgCounters = new Map<string, number>()
let loadedPath: string | undefined

function loadSeqStore(): void {
  const path = seqStorePath()
  // 换存储路径（测试/运维）就从新文件重新加载；只有成功加载过才记下路径，
  // 读取抛错后下次仍会重试并继续 fail-closed。
  if (loadedPath === path) return
  // 换存储就彻底换状态：旧表残留会让新 store 继承别人的 seq。
  // 先撤销标记：这次读取失败时，切回旧路径必须重新加载，而不是直接用空表。
  loadedPath = undefined
  messageSeqs.clear()
  msgCounters.clear()
  let text: string
  try {
    text = readFileSync(path, 'utf8')
  } catch (error) {
    // 文件不存在 = 首次运行；其它错误（权限/IO）绝不能当空表，否则旧 seq 丢失。
    if ((error as { code?: string }).code === 'ENOENT') { loadedPath = path; return }
    throw new Error('qq seq store unreadable: ' + String(error).slice(0, 120))
  }
  try {
    const parsed = JSON.parse(text) as unknown
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('not an object')
    const raw = parsed as { seqs?: unknown; counters?: unknown }
    for (const key of ['seqs', 'counters'] as const) {
      const value = raw[key]
      if (value === undefined || value === null || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(key + ' is missing or not an object')
      }
    }
    for (const [key, value] of Object.entries(raw.seqs as Record<string, { msgId?: unknown; seq?: unknown }>)) {
      const seq = value?.seq
      const msgId = value?.msgId
      if (!key || typeof msgId !== 'string' || !msgId || typeof seq !== 'number' || !Number.isInteger(seq) || seq < 1) {
        throw new Error('invalid seq entry: ' + key)
      }
      messageSeqs.set(key, { msgId, seq })
    }

    for (const [key, value] of Object.entries(raw.counters as Record<string, unknown>)) {
      if (!key || typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
        throw new Error('invalid counter entry: ' + key)
      }
      msgCounters.set(key, value)
    }
    // 计数器是派生值：把落后的计数器抬到已分配的最大 seq，避免重新分配撞号。
    for (const entry of messageSeqs.values()) {
      const counter = msgCounters.get(entry.msgId) ?? 0
      if (counter < entry.seq) msgCounters.set(entry.msgId, entry.seq)
    }
    loadedPath = path
  } catch (error) {
    // 损坏的存储同样 fail-closed：从这里开始分配 seq 会重复发送。
    throw new Error('qq seq store corrupt: ' + String(error).slice(0, 120))
  }
}

function flushSeqStore(): void {
  try {
    const store = seqStorePath()
    mkdirSync(dirname(store), { recursive: true, mode: 0o700 })
    const temp = store + '.tmp'
    writeFileSync(temp, JSON.stringify({
      seqs: Object.fromEntries(messageSeqs),
      counters: Object.fromEntries(msgCounters),
    }), { mode: 0o600 })
    renameSync(temp, store)
  } catch (error) {
    // 落盘失败会让重启后的 seq 复位（重复发送或 400）：宁可让这条回复失败重试，
    // 也不要带着可能重复的 seq 假装成功。
    const detail = String(error).slice(0, 160)
    console.error('[qq] seq store flush failed, failing closed: ' + detail)
    throw new Error('qq seq store unavailable: ' + detail)
  }
}

/** Next seq for one inbound msg_id (text path). */
export function nextMsgSeq(msgId: string): number {
  loadSeqStore()
  const next = (msgCounters.get(msgId) ?? 0) + 1
  if (!msgCounters.has(msgId) && msgCounters.size >= MESSAGE_SEQ_LIMIT) {
    const oldest = msgCounters.keys().next().value
    if (oldest !== undefined) msgCounters.delete(oldest)
  }
  msgCounters.set(msgId, next)
  flushSeqStore()
  return next
}

/** Stable seq for one outbound message; retries reuse it (idempotent). */
export function messageSeqFor(messageKey: string, msgId: string): number {
  loadSeqStore()
  const known = messageSeqs.get(messageKey)
  if (known) {
    // 命中也要确认已落盘：上次 flush 失败过，重试时拿到的就是没持久化的 seq。
    flushSeqStore()
    return known.seq
  }
  const seq = nextMsgSeq(msgId)
  messageSeqs.set(messageKey, { msgId, seq })
  if (messageSeqs.size > MESSAGE_SEQ_LIMIT) {
    const oldest = messageSeqs.keys().next().value
    if (oldest !== undefined) messageSeqs.delete(oldest)
  }
  flushSeqStore()
  return seq
}

/** How many passive replies this inbound msg_id already consumed. */
export function usedSeqsOf(msgId: string): number {
  loadSeqStore()
  return msgCounters.get(msgId) ?? 0
}

export const PASSIVE_REPLY_LIMIT = { c2c: 4, group: 5 } as const

export interface PassiveReplyTarget { scope: 'group' | 'c2c'; openid: string; msgId: string }

/** Send one passive reply quoting the inbound message (msg_id); throws on
 *  missing address fields and on non-2xx responses so the Outbox retries. */
export async function sendPassiveReply(
  appId: string,
  appSecret: string,
  target: PassiveReplyTarget,
  text: string,
  messageKey?: string,
): Promise<void> {
  if (!target.openid) throw new Error('sendPassiveReply: openid required')
  if (!target.msgId) throw new Error('sendPassiveReply: msgId required (passive replies must quote the inbound message)')
  const url = API_BASE + (target.scope === 'group'
    ? '/v2/groups/' + encodeURIComponent(target.openid) + '/messages'
    : '/v2/users/' + encodeURIComponent(target.openid) + '/messages')
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(await authHeaders(appId, appSecret)) },
    body: JSON.stringify({
      msg_type: 0,
      msg_id: target.msgId,
      msg_seq: messageKey ? messageSeqFor(messageKey, target.msgId) : nextMsgSeq(target.msgId),
      content: text,
    }),
    signal: AbortSignal.timeout(15_000),
  })
  if (!res.ok) {
    const detail = await res.text().catch(() => '')
    if (isDuplicateSeqReply(res.status, detail)) return // already delivered by an earlier attempt
    if (isExpiredMsgIdReply(res.status, detail)) throw new ReplyGoneError('reply window closed')
    throw new Error('passive reply failed: HTTP ' + res.status + ' ' + detail.slice(0, 200))
  }
}

// ===== Attachments =====

export const IMAGE_MEDIA_TYPES: Record<string, string> = {
  'image/png': 'image/png',
  'image/jpeg': 'image/jpeg',
  'image/jpg': 'image/jpeg',
  'image/webp': 'image/webp',
  'image/gif': 'image/gif',
}

export function isImageContentType(contentType: unknown): boolean {
  return typeof contentType === 'string' && contentType.toLowerCase().startsWith('image/')
}

export function mediaKindOf(contentType: unknown): 'video' | 'audio' | 'file' {
  const ct = typeof contentType === 'string' ? contentType.toLowerCase() : ''
  if (ct.startsWith('video/')) return 'video'
  if (ct.startsWith('audio/')) return 'audio'
  return 'file'
}

/** Deterministic marker KEY derived from the attachment url (FNV-1a, forward
 *  + reverse rounds -> 16 hex chars). It is only a locator: user-typed markers
 *  in message text are ignored unless the recomputed hash matches an
 *  attachment the message actually carries (see threadContext.image). */
export function attachmentKey(url: string): string {
  let forward = 0x811c9dc5
  for (let i = 0; i < url.length; i++) {
    forward ^= url.charCodeAt(i)
    forward = Math.imul(forward, 0x01000193)
  }
  let reverse = 0x811c9dc5 ^ url.length
  for (let i = url.length - 1; i >= 0; i--) {
    reverse ^= url.charCodeAt(i) * 31
    reverse = Math.imul(reverse, 0x01000193)
  }
  return (forward >>> 0).toString(16).padStart(8, '0') + (reverse >>> 0).toString(16).padStart(8, '0')
}

/** Non-2xx error carrying the explicit auth-spike note (headers are NOT guessed). */
async function attachmentHttpError(res: { status: number; text(): Promise<string> }): Promise<Error> {
  const detail = await res.text().catch(() => '')
  return new Error('attachment download failed: HTTP ' + res.status + ' ' + detail.slice(0, 200)
    + ' — attachments url 可能需要鉴权头（契约未实测，M2 真机 spike 核对）')
}

export interface MediaDownloadOpts { maxBytes?: number; budgetMs?: number; segmentBytes?: number; attempts?: number }

/** Segmented streaming download to a .part file: Range resume + retry backoff
 *  + hard size cap; a server without Range support (200 full) restarts from
 *  zero. The caller renames the .part once the mime is known. */
export async function downloadQqMediaToFile(url: string, partPath: string, opts?: MediaDownloadOpts): Promise<{ bytes: number; mime: string }> {
  const maxBytes = opts?.maxBytes ?? 200 * 1024 * 1024
  const deadline = Date.now() + (opts?.budgetMs ?? 15 * 60_000)
  const segment = opts?.segmentBytes ?? 8 * 1024 * 1024
  const maxAttempts = opts?.attempts ?? 4
  let offset = 0
  try { offset = (await stat(partPath)).size } catch { /* first run */ }
  if (offset > maxBytes) {
    await unlink(partPath).catch(() => {})
    throw new Error('media exceeds ' + maxBytes + ' byte limit')
  }
  await mkdir(dirname(partPath), { recursive: true })
  let mime = ''
  let attempt = 0
  let lastError = 'unknown'
  while (Date.now() <= deadline) {
    try {
      for (;;) {
        if (offset > maxBytes) {
          await unlink(partPath).catch(() => {})
          throw new Error('media exceeds ' + maxBytes + ' byte limit')
        }
        const res = await fetch(url, {
          headers: offset > 0 ? { Range: 'bytes=' + offset + '-' + (offset + segment - 1) } : {},
          signal: AbortSignal.timeout(30_000),
        })
        if (!mime) mime = (res.headers.get('content-type') ?? '').split(';')[0]?.trim().toLowerCase() ?? ''
        if (res.status === 206) {
          const buf = Buffer.from(await res.arrayBuffer())
          if (buf.length === 0) throw new Error('empty range response')
          const handle = await open(partPath, 'a')
          try { await handle.write(buf) } finally { await handle.close() }
          offset += buf.length
          const total = Number(/\/(\d+)$/.exec(res.headers.get('content-range') ?? '')?.[1] ?? -1)
          if (total >= 0 && offset >= total) return { bytes: offset, mime }
          if (total < 0 && buf.length < segment) return { bytes: offset, mime }
          continue
        }
        if (res.status === 200) {
          // Server ignores Range: stream the full body (offset restarts at 0).
          if (offset > 0) { offset = 0; await unlink(partPath).catch(() => {}) }
          const handle = await open(partPath, 'w')
          try {
            for await (const chunk of res.body ?? []) {
              const buf = Buffer.from(chunk as Uint8Array)
              await handle.write(buf)
              offset += buf.length
              if (offset > maxBytes) throw new Error('media exceeds ' + maxBytes + ' byte limit')
            }
          } finally { await handle.close() }
          return { bytes: offset, mime }
        }
        throw await attachmentHttpError(res)
      }
    } catch (error) {
      lastError = String(error).slice(0, 200)
      if (lastError.includes('byte limit')) throw error
      attempt += 1
      if (attempt >= maxAttempts || Date.now() > deadline) break
      await new Promise((resolve) => setTimeout(resolve, Math.min(500 * 2 ** (attempt - 1), 8_000)))
    }
  }
  throw new Error('media download failed after ' + attempt + ' attempts at ' + offset + ' bytes: ' + lastError)
}

/** Whole-file image download (memory path); mime must be in the image whitelist. */
export async function downloadQqImage(url: string): Promise<{ data: Buffer; mediaType: string }> {
  const res = await fetch(url, { signal: AbortSignal.timeout(60_000) })
  if (!res.ok) throw await attachmentHttpError(res)
  const mime = (res.headers.get('content-type') ?? '').split(';')[0]?.trim().toLowerCase() ?? ''
  const mediaType = IMAGE_MEDIA_TYPES[mime]
  if (!mediaType) throw new Error('unsupported image type: ' + (mime || 'unknown'))
  const data = Buffer.from(await res.arrayBuffer())
  return { data, mediaType }
}

// ===== Outbound files (msg_type 7) — SPEC v3 =====
//
// Semantics ported from @tencent-connect/qqbot-nodejs 1.0.4 (media-chunked.ts):
// upload_prepare -> per-part PUT -> upload_part_finish -> /files -> msg_type 7.
// The one-shot base64 path is not used (HTTP 500 code 850012 on a 938 KB PDF).

/** QQ media file_type. Anything not listed maps to 4 (generic file) — QQ only
 *  accepts mp4 video and silk/mp3/wav/ogg voice; mov/webm/m4a would fail. */
const FILE_TYPE_BY_EXT: Record<string, number> = {
  '.png': 1, '.jpg': 1, '.jpeg': 1, '.webp': 1, '.gif': 1,
  '.mp4': 2,
  '.mp3': 3, '.wav': 3, '.ogg': 3, '.silk': 3, '.slk': 3,
}

export function mediaFileTypeOf(fileName: string): number {
  const match = /\.[A-Za-z0-9]{1,6}$/.exec(fileName)
  return match ? (FILE_TYPE_BY_EXT[match[0].toLowerCase()] ?? 4) : 4
}

export const OUTBOUND_EXTS = new Set([
  '.pdf', '.md', '.txt', '.csv', '.json', '.log', '.zip', '.tar', '.gz',
  '.png', '.jpg', '.jpeg', '.webp', '.gif', '.mp4',
  '.mp3', '.wav', '.ogg', '.silk', '.slk',
])

/** Official `UPLOAD_PREPARE_FALLBACK_CODE`: daily file capacity exceeded.
 *  Retrying cannot help. 850012 (one-shot path, HTTP 500) is NOT this code. */
const UPLOAD_DAILY_LIMIT_CODE = 40093002
const PART_RETRY_CODE = 40093001
const MD5_10M_SIZE = 10_002_432
const MAX_CONCURRENCY = 4

/** The passive-reply window closed (msg_id expired): a retry can never succeed,
 *  so the channel reports `gone` instead of throwing into the Outbox retry loop. */
export class ReplyGoneError extends Error {
  override readonly name = 'ReplyGoneError'
}

export function isExpiredMsgIdReply(status: number, body: string): boolean {
  if (status !== 400) return false
  let parsed: { code?: unknown; message?: unknown } = {}
  try { parsed = JSON.parse(body) as { code?: unknown; message?: unknown } } catch { return false }
  if (parsed.code === 40034005) return true
  const message = typeof parsed.message === 'string' ? parsed.message : ''
  return /msg_id/i.test(message) && /(过期|expired)/i.test(message)
}

export class UploadDailyLimitError extends Error {
  override readonly name = 'UploadDailyLimitError'
}

/** QQ answers a retried passive reply with this pair when msg_id+msg_seq repeat. */
export function isDuplicateSeqReply(status: number, body: string): boolean {
  if (status !== 400) return false
  let parsed: { code?: unknown; message?: unknown } = {}
  try { parsed = JSON.parse(body) as { code?: unknown; message?: unknown } } catch { return false }
  if (typeof parsed.code !== 'number') return false
  const message = typeof parsed.message === 'string' ? parsed.message : ''
  // 同时要求 msg_seq 与“重复”语义：普通的参数错误 400 不能被当成已送达。
  return /msg_?seq/i.test(message) && /(重复|duplicate|already)/i.test(message)
}

async function qqApi(
  appId: string,
  appSecret: string,
  method: 'POST',
  path: string,
  body: unknown,
): Promise<Record<string, unknown>> {
  const res = await fetch(API_BASE + path, {
    method,
    headers: { 'content-type': 'application/json', ...(await authHeaders(appId, appSecret)) },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60_000),
  })
  const text = await res.text().catch(() => '')
  let parsed: Record<string, unknown> = {}
  try { parsed = text ? JSON.parse(text) as Record<string, unknown> : {} } catch { /* non-JSON */ }
  if (!res.ok) {
    if (parsed.code === UPLOAD_DAILY_LIMIT_CODE) throw new UploadDailyLimitError('daily file capacity exceeded: ' + text.slice(0, 160))
    const error = new Error('qq api ' + path + ' HTTP ' + res.status + ' ' + text.slice(0, 160)) as Error & { code?: number; httpStatus?: number }
    if (typeof parsed.code === 'number') error.code = parsed.code
    // 业务码（40034001 等）不是 HTTP 状态，重试判定必须分开看。
    error.httpStatus = res.status
    throw error
  }
  return parsed
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** Retry with backoff, bounded by an absolute deadline. Part-finish gets a
 *  long window (official: up to 10 min); everything else stays short. */
/** Retry only transient failures: no API code (network/timeout) or a code in
 *  `retryCodes`. Anything else (400/401/403 …) fails at once — retrying a
 *  request the server rejected cannot help and burns the passive window. */
async function retryUntil<T>(label: string, deadline: number, run: () => Promise<T>, retryCodes: number[] = []): Promise<T> {
  let attempt = 0
  let last: unknown
  for (;;) {
    attempt += 1
    try { return await run() } catch (error) {
      last = error
      if (error instanceof UploadDailyLimitError) throw error
      const { code, httpStatus } = error as { code?: number; httpStatus?: number }
      const transient = httpStatus === undefined || httpStatus >= 500 || (code !== undefined && retryCodes.includes(code))
      if (!transient || Date.now() + 500 >= deadline) break
      await sleep(Math.min(500 * 2 ** (attempt - 1), 8_000))
    }
  }
  throw new Error(label + ' failed after ' + attempt + ' attempts: ' + String(last).slice(0, 160))
}

const sanitizeFileName = (name: string): string =>
  name.replace(/[\u0000-\u001f\u007f]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 80) || 'file'

export interface UploadedFile { fileInfo: string; ttl: number }

/** Open a file without following symlinks; returns a handle plus its size. */
async function openRegularFile(filePath: string): Promise<{ handle: Awaited<ReturnType<typeof open>>; size: number; name: string }> {
  const flags = FS.O_RDONLY | (FS.O_NOFOLLOW ?? 0)
  const handle = await open(filePath, flags)
  try {
    const info = await handle.stat()
    if (!info.isFile()) throw new Error('not a regular file')
    return { handle, size: info.size, name: sanitizeFileName(filePath.split('/').pop() ?? 'file') }
  } catch (error) {
    await handle.close().catch(() => {})
    throw error
  }
}

/** md5 / sha1 / md5 of the first 10,002,432 bytes, streamed in 1 MiB slices. */
async function hashHandle(handle: Awaited<ReturnType<typeof open>>, size: number): Promise<{ md5: string; sha1: string; md5_10m: string }> {
  const md5 = createHash('md5')
  const sha1 = createHash('sha1')
  const md5hi = createHash('md5')
  const buffer = Buffer.allocUnsafe(Math.min(1 << 20, Math.max(size, 1)))
  let offset = 0
  while (offset < size) {
    const { bytesRead } = await handle.read(buffer, 0, Math.min(buffer.length, size - offset), offset)
    if (bytesRead <= 0) break
    const chunk = buffer.subarray(0, bytesRead)
    md5.update(chunk)
    sha1.update(chunk)
    if (offset < MD5_10M_SIZE) md5hi.update(chunk.subarray(0, Math.min(bytesRead, MD5_10M_SIZE - offset)))
    offset += bytesRead
  }
  return { md5: md5.digest('hex'), sha1: sha1.digest('hex'), md5_10m: md5hi.digest('hex') }
}

export interface UploadQqFileOptions {
  /** Absolute wall-clock budget for the whole upload (group window is 5 min). */
  deadline?: number
  /** Upper bound for the file; checked again after opening. */
  maxBytes?: number
  /** realpath the resolver approved; a mismatch means the file was swapped. */
  expectedReal?: string
}

/** Upload one local file in chunks and return its `file_info` token. */
export async function uploadQqFile(
  appId: string,
  appSecret: string,
  target: { scope: 'group' | 'c2c'; openid: string },
  filePath: string,
  fileName?: string,
  opts?: UploadQqFileOptions,
): Promise<UploadedFile> {
  if (!target.openid) throw new Error('uploadQqFile: openid required')
  const deadline = opts?.deadline ?? Date.now() + 4 * 60_000
  const { handle, size, name } = await openRegularFile(filePath)
  try {
    if (opts?.maxBytes !== undefined && size > opts.maxBytes) {
      throw new Error('file is ' + size + ' bytes, over the ' + opts.maxBytes + ' byte cap')
    }
    const info = await handle.stat()
    if (info.nlink > 1) throw new Error(name + ' is a hard link')
    if (opts?.expectedReal) {
      const nowReal = await realpath(filePath).catch(() => undefined)
      if (nowReal !== opts.expectedReal) throw new Error(name + ' changed after validation')
    }
    const base = target.scope === 'group'
      ? '/v2/groups/' + encodeURIComponent(target.openid)
      : '/v2/users/' + encodeURIComponent(target.openid)
    const baseName = sanitizeFileName(fileName ?? name)
    const hashes = await hashHandle(handle, size)
    const prepared = await retryUntil('upload_prepare', deadline, () => qqApi(appId, appSecret, 'POST', base + '/upload_prepare', {
      file_type: mediaFileTypeOf(baseName),
      file_name: baseName,
      file_size: size,
      ...hashes,
    }), [850012])
    const uploadId = String(prepared.upload_id ?? '')
    const blockSize = Number(prepared.block_size ?? 0)
    const parts = Array.isArray(prepared.parts) ? prepared.parts as { index?: number; presigned_url?: string }[] : []
    if (!uploadId || !blockSize || parts.length === 0) throw new Error('upload_prepare: unexpected response shape')

    const queue = [...parts].sort((a, b) => Number(a.index) - Number(b.index))
    const errors: Error[] = []
    const worker = async (): Promise<void> => {
      for (;;) {
        const part = queue.shift()
        if (!part) return
        const index = Number(part.index ?? 0)
        const url = String(part.presigned_url ?? '')
        if (!index || !url) throw new Error('upload_prepare: bad part ' + index)
        const start = (index - 1) * blockSize
        const length = Math.min(blockSize, size - start)
        if (length <= 0) throw new Error('upload_prepare: part ' + index + ' out of range')
        const buffer = Buffer.allocUnsafe(length)
        const { bytesRead } = await handle.read(buffer, 0, length, start)
        if (bytesRead !== length) throw new Error('short read on part ' + index)
        await retryUntil('part ' + index + ' put', deadline, async () => {
          // Pre-signed COS URL: no Authorization header, only the length.
          const res = await fetch(url, {
            method: 'PUT',
            headers: { 'content-length': String(length) },
            body: buffer,
            signal: AbortSignal.timeout(Math.max(10_000, deadline - Date.now())),
          })
          if (!res.ok) {
            const putError = new Error('COS part PUT HTTP ' + res.status + ' ' + (await res.text().catch(() => '')).slice(0, 120)) as Error & { httpStatus?: number }
            putError.httpStatus = res.status
            throw putError
          }
        })
        await retryUntil('part ' + index + ' finish', deadline, () => qqApi(appId, appSecret, 'POST', base + '/upload_part_finish', {
          upload_id: uploadId,
          part_index: index,
          block_size: length,
          md5: createHash('md5').update(buffer).digest('hex'),
        }), [PART_RETRY_CODE])
      }
    }
    await Promise.all(Array.from({ length: Math.min(MAX_CONCURRENCY, parts.length) }, () => worker().catch((error: unknown) => {
      errors.push(error instanceof Error ? error : new Error(String(error)))
    })))
    if (errors.length > 0) throw errors[0]

    const completed = await retryUntil('upload complete', deadline, () => qqApi(appId, appSecret, 'POST', base + '/files', { upload_id: uploadId }), [850012])
    const fileInfo = String(completed.file_info ?? '')
    if (!fileInfo) throw new Error('upload complete: no file_info')
    return { fileInfo, ttl: Number(completed.ttl ?? 0) }
  } finally {
    await handle.close().catch(() => {})
  }
}

/** Send one file as a rich-media passive reply. `messageKey` makes retries
 *  idempotent; `used` lets the caller respect the 4/5 reply budget. */
export async function sendPassiveFile(
  appId: string,
  appSecret: string,
  target: PassiveReplyTarget,
  file: { path: string; name?: string },
  content: string,
  messageKey: string,
  opts?: UploadQqFileOptions,
): Promise<void> {
  if (!target.openid) throw new Error('sendPassiveFile: openid required')
  if (!target.msgId) throw new Error('sendPassiveFile: msgId required')
  const uploaded = await uploadQqFile(appId, appSecret, target, file.path, file.name, opts)
  const url = API_BASE + (target.scope === 'group'
    ? '/v2/groups/' + encodeURIComponent(target.openid) + '/messages'
    : '/v2/users/' + encodeURIComponent(target.openid) + '/messages')
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(await authHeaders(appId, appSecret)) },
    body: JSON.stringify({
      msg_type: 7,
      media: { file_info: uploaded.fileInfo },
      content,
      msg_id: target.msgId,
      msg_seq: messageSeqFor(messageKey, target.msgId),
    }),
    signal: AbortSignal.timeout(30_000),
  })
  if (res.ok) return
  const body = await res.text().catch(() => '')
  if (isDuplicateSeqReply(res.status, body)) return // already delivered by an earlier attempt
  if (isExpiredMsgIdReply(res.status, body)) throw new ReplyGoneError('reply window closed')
  throw new Error('passive file failed: HTTP ' + res.status + ' ' + body.slice(0, 160))
}
