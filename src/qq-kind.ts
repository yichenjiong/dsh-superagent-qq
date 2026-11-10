/** The 'qq' Source kind: QQ official bot messages arriving via the WebSocket
 *  gateway, replies through the OpenAPI as passive replies. Shape mirrors the
 *  bundled github/feishu kinds (docs/008-cookbook.md) and the dingtalk/mail
 *  sibling plugins; the engine duck-types it at runtime. Image attachments
 *  arrive as [qq-image:KEY] markers in the trigger text and are downloaded on
 *  demand through threadContext.image (dingtalk-kind precedent). */
import { z } from 'zod'
import { constants as FS } from 'node:fs'
import { open, realpath } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join, resolve, sep } from 'node:path'
import type { Localized, OutboundMessageLike, SourceKindLike, TriggerLike } from './types.ts'
import { attachmentKey, downloadQqImage, isImageContentType, OUTBOUND_EXTS, PASSIVE_REPLY_LIMIT, ReplyGoneError, sendPassiveFile, sendPassiveReply, usedSeqsOf } from './robot.ts'

export interface QqAccount { name: string; appId: string; appSecret: string }
export interface QqGroup { appId: string; groupOpenid: string; name: string; repository?: string }

export const HEALTH_ENTRY = 'qqGateway'
const dmRowId = 'direct-messages'

export const QqConfigSchema = z.object({
  accounts: z.array(z.object({
    name: z.string().default(''),
    appId: z.string().default(''),
    appSecret: z.string().default(''),
  })).default([]),
  groups: z.array(z.object({
    appId: z.string().default(''),
    groupOpenid: z.string().default(''),
    name: z.string().default(''),
    repository: z.string().trim().regex(/^([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)?$/, 'owner/name').optional(),
  })).default([]),
  directMessages: z.boolean().default(true),
  sendAck: z.boolean().default(true),
  /** c2c user openids allowed to create projects from a direct message; empty disables project creation. */
  operators: z.array(z.union([z.string().trim().min(1), z.object({ openid: z.string().trim().min(1) }).strict()])).default([]),
  /** 出站文件：staging 根 + 手工投递根 + 按渠道的尺寸上限。 */
  files: z.object({
    ticketRoot: z.string().trim().min(1).optional(),
    allowedRoots: z.array(z.string().trim().min(1)).optional(),
    maxFileBytes: z.number().int().positive().optional(),
    groupMaxFileBytes: z.number().int().positive().optional(),
    maxFilesPerReply: z.number().int().positive().optional(),
  }).strict().optional(),
}).strict()
export type QqConfig = z.infer<typeof QqConfigSchema>

// channel.send reads config through this holder; watch() refreshes it every round.
export const channelConfig: { current?: QqConfig } = {}

/** 从消息文本提取图片 key（与 buildTrigger 内嵌的 [qq-image:KEY] 标记配对）。
 *  KEY 是附件 url 的派生哈希（robot.attachmentKey），提取只认原文出现过的 key；
 *  image() 还会按 url 重算哈希核对，用户手打的假标记无法对上真实附件。 */
const qqImageKeys = (text: string): string[] =>
  [...new Set([...text.matchAll(/\[qq-image:([^\]]+)\]/g)].map((m) => m[1] ?? ''))].filter(Boolean)

// ===== Outbound file markers (SPEC v3) =====

/** A reply may carry `[[qq-file:/abs/path]]`; only QQ interprets it. The
 *  marker is stripped from every outbound text, ack and notice included. */
const FILE_MARKER = /\[\[qq-file:([^\]\r\n]{1,600})\]\]/g

export function extractFileMarkers(text: string): { paths: string[]; rest: string } {
  const paths: string[] = []
  for (const match of text.matchAll(FILE_MARKER)) {
    const value = (match[1] ?? '').trim()
    if (value) paths.push(value)
  }
  const rest = text.replace(FILE_MARKER, '').replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim()
  return { paths: [...new Set(paths)], rest }
}

/** How many files one reply may carry: replyLimit minus already-used replies,
 *  keeping one slot for the failure summary. */
export function replyBudget(policy: OutboundFilePolicy, used: number): number {
  const available = Math.max(0, policy.replyLimit - used)
  return Math.min(policy.maxFiles, Math.max(0, available - 1))
}

export interface OutboundFilePolicy {
  ticketRoot: string
  allowedRoots: string[]
  maxBytes: number
  maxFiles: number
  replyLimit: number
}

const MAX_FILE_BYTES = 50 * 1024 * 1024
const GROUP_MAX_FILE_BYTES = 20 * 1024 * 1024
const MAX_FILES_PER_REPLY = 3
const TICKET_ID = /^[A-Za-z0-9_-]{8,64}$/

const expandHome = (value: string): string =>
  value === '~' ? homedir() : value.startsWith('~/') ? join(homedir(), value.slice(2)) : value

export function filePolicyOf(config: QqConfig, scope: 'group' | 'c2c'): OutboundFilePolicy {
  const files = config.files ?? {}
  return {
    ticketRoot: resolve(expandHome(files.ticketRoot ?? join(homedir(), '.dsh', 'qq-outbox'))),
    allowedRoots: (files.allowedRoots ?? [join(homedir(), '.dsh', 'qq-outbox', 'manual')]).map((root) => resolve(expandHome(root))),
    maxBytes: scope === 'group'
      ? (files.groupMaxFileBytes ?? GROUP_MAX_FILE_BYTES)
      : (files.maxFileBytes ?? MAX_FILE_BYTES),
    maxFiles: files.maxFilesPerReply ?? MAX_FILES_PER_REPLY,
    replyLimit: scope === 'group' ? PASSIVE_REPLY_LIMIT.group : PASSIVE_REPLY_LIMIT.c2c,
  }
}

/** A root of `/` or an empty string would accept the whole filesystem. */
const usableRoot = (root: string): string | undefined => {
  const normalized = root.endsWith(sep) ? root.slice(0, -1) : root
  if (!normalized || normalized === '/') return undefined
  return normalized
}

const within = (real: string, root: string): boolean => real === root || real.startsWith(root + sep)

/** Resolve one marker path: absolute, regular file, no symlink, no hardlink,
 *  inside this ticket's staging directory or a configured drop root, allowed
 *  extension, under the size cap. Throws with a basename-only reason. */
export async function resolveOutboundFile(
  candidate: string,
  policy: OutboundFilePolicy,
  ticketId: string | undefined,
): Promise<{ path: string; name: string; size: number }> {
  if (!candidate.startsWith('/')) throw new Error('path must be absolute')
  const name = candidate.split(sep).pop() ?? 'file'
  if (ticketId !== undefined && !TICKET_ID.test(ticketId)) throw new Error('invalid ticket id; cannot scope the file')
  let size = 0
  let handle
  try {
    handle = await open(candidate, FS.O_RDONLY | (FS.O_NOFOLLOW ?? 0))
  } catch (error) {
    if ((error as { code?: string }).code === 'ELOOP') throw new Error(name + ': symbolic links are not allowed')
    throw new Error('cannot open ' + name)
  }
  try {
    const info = await handle.stat()
    if (!info.isFile()) throw new Error(name + ' is not a regular file')
    if (info.nlink > 1) throw new Error(name + ' is a hard link')
    size = info.size
  } finally {
    await handle.close().catch(() => {})
  }
  const real = await realpath(candidate).catch(() => undefined)
  if (!real) throw new Error('cannot resolve ' + name)
  const roots = [
    ticketId ? usableRoot(join(policy.ticketRoot, ticketId)) : undefined,
    ...policy.allowedRoots.map(usableRoot),
  ].filter((root): root is string => !!root)
  if (roots.length === 0) throw new Error('no usable allowed root configured')
  for (const root of roots) {
    const realRoot = await realpath(root).catch(() => resolve(root))
    if (within(real, realRoot)) {
      const ext = (/\.[A-Za-z0-9]{1,6}$/.exec(name)?.[0] ?? '').toLowerCase()
      if (!OUTBOUND_EXTS.has(ext)) throw new Error(name + ' has a disallowed type')
      if (size > policy.maxBytes) throw new Error(name + ' is over the ' + policy.maxBytes + ' byte cap')
      return { path: real, name, size }
    }
  }
  throw new Error(name + ' is outside the allowed roots')
}

export const qqKind = {
  kind: 'qq',
  title: { zh: 'QQ 机器人', en: 'QQ bot' },
  description: {
    zh: '直连 QQ 官方机器人平台：群内 @ 机器人或机器人单聊即可派活，回复以被动回复直接回 QQ。',
    en: 'Direct QQ official bot via the WebSocket gateway: @ the bot in a group or DM it to dispatch; replies go back as passive replies.',
  },
  schema: QqConfigSchema,
  fields: [
    { key: 'accounts', type: 'rows', dialog: true, label: { zh: '机器人账号', en: 'Bot accounts' }, columns: [
      { key: 'name', type: 'text', label: { zh: '名称', en: 'Name' }, optional: true, hint: { zh: '仅用于面板显示', en: 'Display only' } },
      { key: 'appId', type: 'text', label: { zh: 'AppID', en: 'AppID' } },
      { key: 'appSecret', type: 'text', label: { zh: 'AppSecret', en: 'AppSecret' }, hint: { zh: '明文存于引擎配置（本地 sqlite，仅本机可读）', en: 'Stored in engine config (local sqlite, readable only on this machine)' } },
    ] },
    { key: 'groups', type: 'rows', label: { zh: '接收的群', en: 'Group chats' }, columns: [
      { key: 'appId', type: 'text', label: { zh: 'AppID', en: 'AppID' } },
      { key: 'groupOpenid', type: 'text', label: { zh: '群 openid', en: 'Group openid' }, hint: { zh: '群内 @ 机器人发一条消息，日志会打印', en: 'Mention the bot once in the group; the log prints it' } },
      { key: 'name', type: 'text', label: { zh: '群名', en: 'Name' }, optional: true },
      { key: 'repository', type: 'text', label: { zh: '项目仓库', en: 'Project repository' }, optional: true,
        hint: { zh: 'owner/name；留空表示未绑定，由绑定码写入', en: 'owner/name; empty or none means unbound, written by a binding code' } },
    ] },
    { key: 'directMessages', type: 'switch', label: { zh: '接收机器人单聊', en: 'Receive bot direct messages' } },
    { key: 'operators', type: 'rows', label: { zh: '可建项目的私聊用户', en: 'Operators who may create projects' },
      hint: { zh: '填 user_openid；只有这些私聊用户能建私有仓并拿绑定码，留空则关闭建项目', en: 'user_openid values; only these direct-message users may create a private repository and get a binding code; empty disables it' },
      columns: [{ key: 'openid', type: 'text', label: { zh: 'user_openid', en: 'user_openid' } }] },
    { key: 'sendAck', type: 'switch', label: { zh: '「已收到」回执', en: '“Received” acknowledgement' }, hint: { zh: '收到消息先被动回复「已收到」，终答随后', en: 'Passively reply “received” on receipt; the final answer follows' } },
  ],
  health: ['qqGateway'],
  scope(input: { meta: Record<string, unknown> }, config: QqConfig) {
    const accountId = String(input.meta.accountId ?? '')
    if (!config.accounts.some((a) => a.appId === accountId)) {
      return { listened: false, reason: 'qq: account not configured: ' + accountId }
    }
    if (input.meta.scopeKind === 'c2c') {
      if (!config.directMessages) return { listened: false, reason: 'qq: direct messages disabled' }
      return { listened: true, observed: false, row: dmRowId }
    }
    const groupOpenid = String(input.meta.groupOpenid ?? '')
    const row = config.groups.find((g) => g.groupOpenid === groupOpenid && g.appId === accountId)
    if (!row) return { listened: false, reason: 'qq: group not configured: ' + groupOpenid }
    // A configured group without a repository is not bound yet; its messages only redeem a code or get a prompt.
    if (!row.repository) return { listened: false, reason: 'qq: group has no bound repository: ' + groupOpenid }
    return { listened: true, observed: false, row: row.groupOpenid }
  },
  /** A direct message answers questions but never starts a ticket; a group starts one only once bound. */
  assignable(trigger: TriggerLike, config: QqConfig) {
    if (trigger.meta.scopeKind === 'c2c') return false
    const accountId = String(trigger.meta.accountId ?? '')
    const groupOpenid = String(trigger.meta.groupOpenid ?? '')
    const row = config.groups.find((g) => g.groupOpenid === groupOpenid && g.appId === accountId)
    return !!row?.repository
  },
  channel: {
    async send(_host: unknown, message: OutboundMessageLike): Promise<'sent' | 'gone'> {
      const config = channelConfig.current
      if (!config) throw new Error('qq: channel not ready (no config yet)')
      const address = message.channel.address as Record<string, string>
      // 「已收到」回执可在设置关闭：ack 在开关关闭时直接按已发送处理，不真正发消息。
      if (message.kind === 'acknowledgement' && config.sendAck === false) return 'sent'
      const account = config.accounts.find((a) => a.appId === address.account)
      if (!account) throw new Error('qq: account not configured: ' + String(address.account ?? ''))
      const scope: 'group' | 'c2c' = address.scope === 'group' ? 'group' : 'c2c'
      const target = { scope, openid: address.openid ?? '', msgId: address.msgId ?? '' }
      try {
      const parsed = extractFileMarkers(message.text)
      const policy = filePolicyOf(config, scope)
      const wantedAll = message.kind === 'reply' ? parsed.paths : []
      // 预算：已用 seq 可能包含 ack/进度；留 1 次给失败汇总或超额提示。
      const used = usedSeqsOf(target.msgId)
      // 预留 1 次给失败汇总/超额提示，其余才给文件。
      const budget = replyBudget(policy, used)
      const overflow = wantedAll.length - budget
      let caption = parsed.rest
      if (overflow > 0) {
        caption = (caption ? caption + '\n' : '') + '还有 ' + overflow + ' 个文件没发（回复次数不够）。'
      }
      if (used >= policy.replyLimit) {
        console.error('[qq] reply dropped: passive reply limit reached, msgId=' + String(address.msgId ?? '(none)'))
        return 'sent'
      }
      if (budget === 0 || wantedAll.length === 0) {
        // 空文本不发：QQ 会 400，还白占一次被动回复配额。
        if (!caption) {
          console.error('[qq] reply skipped: empty text, msgId=' + String(address.msgId ?? '(none)'))
          return 'sent'
        }
        await sendPassiveReply(account.appId, account.appSecret, target, caption, message.id)
        console.error('[qq] reply sent: kind=' + message.kind + ' msgId=' + String(address.msgId ?? '(none)'))
        return 'sent'
      }
      const files: { path: string; name: string; index: number }[] = []
      const failures: string[] = []
      // 下标取原列表位置：某个文件 resolve 失败再重试成功，也不会让后面的文件换 seq key。
      for (const [index, candidate] of wantedAll.slice(0, budget).entries()) {
        try { files.push({ ...(await resolveOutboundFile(candidate, policy, message.ticketId)), index }) }
        catch (error) { failures.push(String(error).replace(/^Error: /, '').slice(0, 120)) }
      }
      // 群聊被动窗口 5 分钟，上传最多占 4 分钟；单聊 60 分钟，留足余量。
      const deadline = Date.now() + (scope === 'group' ? 4 * 60_000 : 20 * 60_000)
      let sentFiles = 0
      let pendingCaption = caption
      for (const file of files) {
        try {
          await sendPassiveFile(account.appId, account.appSecret, target, { path: file.path, name: file.name },
            pendingCaption, message.id + '#' + file.index, { deadline, maxBytes: policy.maxBytes, expectedReal: file.path })
          pendingCaption = ''
          sentFiles += 1
        } catch (error) {
          failures.push(file.name + '：' + String(error).replace(/^Error: /, '').slice(0, 110))
        }
      }
      if (sentFiles > 0) {
        // 文件已发出后，汇总失败只记日志：不因汇总失败重试文件。
        if (failures.length > 0) {
          try {
            await sendPassiveReply(account.appId, account.appSecret, target, '没发出去：' + failures.join('；'), message.id + '#summary')
          } catch (error) {
            console.error('[qq] summary failed: ' + String(error).slice(0, 160))
          }
        }
        console.error('[qq] reply sent: kind=' + message.kind + ' files=' + sentFiles + ' msgId=' + String(address.msgId ?? '(none)'))
        return 'sent'
      }
      // 全部失败：能发文本就发文本，窗口已过则记录后静默。
      const fallback = (pendingCaption ? pendingCaption + '\n' : '') + '文件发送失败：' + failures.join('；')
      try {
        await sendPassiveReply(account.appId, account.appSecret, target, fallback, message.id + '#fallback')
      } catch (error) {
        // 文件没发出去、文本也发不出去：交给 Outbox 重试（seq 固化保证幂等），不能假装成功。
        console.error('[qq] fallback failed: ' + String(error).slice(0, 160))
        throw error
      }
      console.error('[qq] reply fell back to text: kind=' + message.kind + ' msgId=' + String(address.msgId ?? '(none)'))
      return 'sent'
      } catch (error) {
        if (error instanceof ReplyGoneError) {
          console.error('[qq] reply gone (msg_id expired): msgId=' + String(address.msgId ?? '(none)'))
          return 'gone'
        }
        throw error
      }
    },

    link(address: Record<string, string>) {
      const label = 'QQ ' + String(address.openid ?? '').slice(0, 12)
      return { url: '', label: { zh: label, en: label } }
    },
  },
  threadContext: {
    messageId(trigger: TriggerLike): string {
      return String(trigger.meta.messageId ?? '')
    },
    // 图片支持（先例：引擎 feishu-image.ts / dingtalk-kind.ts）——images 从文本提取
    // 标记 key，image 下载后经 attachments.saveImage 注入会话。
    images(text: string): string[] {
      return qqImageKeys(text)
    },
    async image(agent: unknown, _commands: unknown, trigger: TriggerLike, message: { id: string; text: string }, key: string, _stateDir: string, signal: AbortSignal) {
      if (!qqImageKeys(message.text).includes(key)) throw new Error('Choose an image key from this source message')
      // 附件定位：trigger.meta.attachments 由 buildTrigger 落盘（url/contentType/filename/key），
      // 这里按 key 重算 url 哈希核对，防伪造 meta 拉取任意地址。
      const stored = (trigger.meta as { attachments?: { url?: string; contentType?: string; filename?: string; key?: string }[] }).attachments
      const list = Array.isArray(stored) ? stored : []
      const att = list.find((a) => a.key === key)
      if (!att || typeof att.url !== 'string' || !att.url || attachmentKey(att.url) !== key) {
        throw new Error('qq: image attachment not found in this message: ' + key)
      }
      if (!isImageContentType(att.contentType)) throw new Error('qq: attachment is not an image: ' + key)
      const host = agent as {
        ctx?: { get?: (name: string) => unknown }
        session?: { requestHeader?: () => { config?: { provider?: string; model?: string } } }
        options?: { provider?: string; model?: string }
      }
      const attachments = host.ctx?.get?.('attachments') as {
        saveImage: (input: { data: Buffer; mediaType: string; name: string }) => Promise<unknown>
        imageLimits?: { maxImageBytes?: number; maxMessageImageBytes?: number }
      } | undefined
      if (!attachments?.saveImage) throw new Error('Image reading is unavailable: no attachment service is mounted')
      const route = host.session?.requestHeader?.()?.config
      const provider = route?.provider ?? host.options?.provider
      const model = route?.model ?? host.options?.model
      const llm = host.ctx?.get?.('llm') as { resolveModelInfo?: (p: string, m: string, s: AbortSignal) => Promise<{ inputModalities?: string[] }> } | undefined
      if (!llm?.resolveModelInfo || !provider || !model) throw new Error('Image reading is unavailable: the current model route is unresolved')
      const info = await llm.resolveModelInfo(provider, model, signal)
      if (!info.inputModalities?.includes('image')) throw new Error('Model ' + model + ' does not support image input; the image has not been inspected')
      signal.throwIfAborted()
      // url 直取（不带任何鉴权头）：失败报错显式指向 spike，待真机核对后再补。
      const media = await downloadQqImage(att.url)
      const maxBytes = Math.min(attachments.imageLimits?.maxImageBytes ?? 20 * 1024 * 1024, attachments.imageLimits?.maxMessageImageBytes ?? 20 * 1024 * 1024)
      if (media.data.length > maxBytes) throw new Error('The image exceeds the ' + maxBytes + ' byte limit')
      const name = 'qq-' + (key.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 24) || 'image') + '.' + media.mediaType.split('/')[1]
      const image = await attachments.saveImage({ data: media.data, mediaType: media.mediaType, name })
      signal.throwIfAborted()
      return { source_message_id: message.id, image_key: key, image }
    },
    async read(_commands: unknown, trigger: TriggerLike, part: 'thread' | 'root' | 'parent', _cursor: string | undefined, _config: QqConfig) {
      // v1: the assessment reads the trigger's own message; conversation
      // history needs more API permissions (M2+, aligned with dingtalk/mail v1).
      const own = { id: String(trigger.meta.messageId ?? trigger.id), from: trigger.actor.id, at: trigger.at, text: trigger.text }
      return { messages: part === 'parent' ? [] : [own], body: {}, status: undefined }
    },
  },
  /** 把 staging 目录下发给 worker：引擎支持 SourceKind.environment（dsh-superagent
   *  src/sources/kind.ts:118），值按工单计算，worker 无需猜路径。 */
  environment: {
    variables: [
      { key: 'DSH_SUPERAGENT_QQ_OUTBOX', description: {
        zh: '要把文件发给 QQ 时，先把它拷进这个目录，再在最终回复里写 [[qq-file:绝对路径]]。没有工单时为空。',
        en: 'To send a file to QQ, copy it into this directory first, then write [[qq-file:/absolute/path]] in the final reply. Empty when there is no ticket.' } },
    ],
    values: ({ config, ticket }: { config: QqConfig; ticket?: { id: string } }) => {
      const files = config.files ?? {}
      const root = resolve(expandHome(files.ticketRoot ?? join(homedir(), '.dsh', 'qq-outbox')))
      const id = ticket?.id ?? ''
      return { DSH_SUPERAGENT_QQ_OUTBOX: TICKET_ID.test(id) ? join(root, id) : '' }
    },
  },
  policy: {
    schema: z.object({
      allowed: z.array(z.object({ id: z.string(), name: z.string().optional() })).default([]),
    }).strict(),
    fields: [
      { key: 'allowed', type: 'rows', label: { zh: '允许派活的成员', en: 'Allowed members' }, columns: [
        { key: 'id', type: 'text', label: { zh: '成员 openid', en: 'Member openid' } },
        { key: 'name', type: 'text', label: { zh: '名称', en: 'Name' }, optional: true },
      ] },
    ],
    explicit: { zh: '群里 @ 机器人，或在机器人单聊里发言', en: 'Mentioned the bot in a chat, or messaged the bot directly' },
    rows(config: Record<string, unknown>) {
      // Defensive read: raw stored config, never parse the whole object here.
      const groups = (Array.isArray(config.groups) ? config.groups : []) as QqGroup[]
      const rows: { id: string; label: Localized | string; note: Localized; cells: { listedUnaddressed: boolean; unlistedAddressed: boolean; unlistedUnaddressed: boolean } }[] = groups.map((g) => ({
        id: g.groupOpenid,
        label: g.name || g.groupOpenid,
        note: { zh: g.repository ? '群聊（@ 可指派，仓库 ' + g.repository + '）' : '群聊（未绑定项目，不能派活）', en: g.repository ? 'Group chat (mention to assign; ' + g.repository + ')' : 'Group chat (no project bound; no work)' },
        cells: { listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false },
      }))
      if (config.directMessages !== false) {
        rows.push({ id: dmRowId, label: { zh: '机器人单聊', en: 'Bot direct messages' }, note: { zh: '单聊消息均为明确指派', en: 'DMs are explicit assignments' }, cells: { listedUnaddressed: true, unlistedAddressed: true, unlistedUnaddressed: true } })
      }
      return rows
    },
  },
} as unknown as SourceKindLike
