/** QQ api-v2 WebSocket gateway client + access-token cache.
 *
 * Contract sources: official docs bot.q.qq.com/wiki/develop/api-v2 (read 2026-11-04) —
 * AppID+AppSecret -> accessToken (~7200s) via bots.qq.com/app/getAppAccessToken;
 * WS URL via GET api.sgroup.qq.com/gateway; opcodes DISPATCH=0 HEARTBEAT=1 IDENTIFY=2
 * RESUME=6 RECONNECT=7 INVALID_SESSION=9 HELLO=10 HEARTBEAT_ACK=11;
 * intents GROUP_AND_C2C_EVENT = 1 << 25 (group @-events + C2C events).
 * Identify payload and event shapes are verified against real traffic by
 * scripts/probe.mjs before the engine wiring is trusted (M1 gate).
 *
 * Resume (M2 stretch): READY carries d.session_id; the client keeps it and the
 * last dispatch sequence in memory. After a close that is not an
 * INVALID_SESSION, the next HELLO sends RESUME (op 6, token + session_id +
 * seq) instead of IDENTIFY; a RESUME that fails (op 9, or the socket closing
 * before RESUMED) clears the session so the following attempt identifies
 * fresh. Sessions live only inside one GatewayClient instance — the watch
 * loop reuses the instance on its 5s reconnect rounds (resume) and builds a
 * fresh one when the config hash changes (identify).
 */
import type { QqAccount } from './qq-kind.ts'

export const API_BASE = 'https://api.sgroup.qq.com'
export const GROUP_AND_C2C_EVENT = 1 << 25

const OP_DISPATCH = 0
const OP_HEARTBEAT = 1
const OP_IDENTIFY = 2
const OP_RESUME = 6
const OP_RECONNECT = 7
const OP_INVALID_SESSION = 9
const OP_HELLO = 10
const WS_OPEN = 1 // WHATWG WebSocket readyState OPEN

export interface TokenCache { token: string; expiresAt: number }
const tokenCaches = new Map<string, TokenCache>()

/** Per-app token with a 60s renewal margin (dingtalk tokenCache pattern). */
export async function getAppAccessToken(appId: string, appSecret: string): Promise<string> {
  const now = Date.now()
  const cached = tokenCaches.get(appId)
  if (cached && cached.expiresAt > now + 60_000) return cached.token
  const res = await fetch('https://bots.qq.com/app/getAppAccessToken', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ appId, clientSecret: appSecret }),
    signal: AbortSignal.timeout(15_000),
  })
  // The QQ OpenAPI returns snake_case fields (access_token/expires_in) in practice;
  // some environments return camelCase. Accept both.
  const body = (await res.json().catch(() => ({}))) as {
    accessToken?: string; access_token?: string; expiresIn?: string | number; expires_in?: string | number
  }
  const token = body.accessToken ?? body.access_token
  if (!res.ok || !token) throw new Error('getAppAccessToken failed: HTTP ' + res.status + ' ' + JSON.stringify(body).slice(0, 200))
  const expiresIn = Number(body.expiresIn ?? body.expires_in ?? 7200)
  tokenCaches.set(appId, { token, expiresAt: now + (Number.isFinite(expiresIn) ? expiresIn : 7200) * 1000 })
  return token
}

/** REST auth headers; x-union-appid follows the official bot-node-sdk precedent. */
export async function authHeaders(appId: string, appSecret: string): Promise<Record<string, string>> {
  const token = await getAppAccessToken(appId, appSecret)
  return { authorization: 'QQBot ' + token, 'x-union-appid': appId }
}

export interface GatewayEvent { op: number; s?: number; t?: string; d?: unknown }

interface Handlers {
  onEvent: (event: GatewayEvent) => void
  onConn: (connected: boolean) => void
}

/** Wraps the platform WebSocket so the transport stays swappable; the watch
 *  loop owns reconnection (5s rounds), mirroring the dingtalk StreamClient. */
export class GatewayClient {
  connected = false
  private ws?: WebSocket
  private heartbeat?: NodeJS.Timeout
  private lastSeq = 0
  private sessionId?: string
  /** true between sending RESUME and seeing RESUMED; a close in that window
   *  means the resume never confirmed -> drop the session (identify fallback). */
  private resumePending = false

  constructor(private readonly account: QqAccount, private readonly handlers: Handlers) {}

  async connect(): Promise<void> {
    const headers = await authHeaders(this.account.appId, this.account.appSecret)
    const res = await fetch(API_BASE + '/gateway', { headers, signal: AbortSignal.timeout(15_000) })
    const body = (await res.json().catch(() => ({}))) as { url?: string }
    if (!res.ok || !body.url) throw new Error('gateway resolve failed: HTTP ' + res.status + ' ' + JSON.stringify(body).slice(0, 200))
    const ws = new WebSocket(body.url)
    this.ws = ws
    ws.onmessage = (e: { data: unknown }) => {
      let payload: GatewayEvent | undefined
      try { payload = JSON.parse(String(e.data)) as GatewayEvent } catch { return }
      if (!payload) return
      void this.handlePayload(payload)
    }
    ws.onclose = () => { this.teardown(); this.handlers.onConn(false) }
    ws.onerror = () => { /* the close event follows; nothing to do here */ }
  }

  private async handlePayload(payload: GatewayEvent): Promise<void> {
    switch (payload.op) {
      case OP_HELLO: {
        const d = payload.d as { heartbeat_interval?: number } | undefined
        const interval = Math.max(5000, Number(d?.heartbeat_interval ?? 30_000))
        await this.resumeOrIdentify()
        this.heartbeat = setInterval(() => {
          try { this.ws?.send(JSON.stringify({ op: OP_HEARTBEAT, d: this.lastSeq })) } catch { /* close handles */ }
        }, interval)
        break
      }
      case OP_DISPATCH: {
        if (typeof payload.s === 'number') this.lastSeq = payload.s
        if (payload.t === 'READY') {
          const d = payload.d as { session_id?: string } | undefined
          if (typeof d?.session_id === 'string' && d.session_id) this.sessionId = d.session_id
          this.resumePending = false
          this.connected = true
          this.handlers.onConn(true)
          break
        }
        if (payload.t === 'RESUMED') {
          this.resumePending = false
          this.connected = true
          this.handlers.onConn(true)
          break
        }
        this.handlers.onEvent(payload)
        break
      }
      case OP_RECONNECT:
        // The server asks for a fresh connection but keeps the session: close;
        // the watch loop reconnects on its next round and resumes.
        this.disconnect()
        break
      case OP_INVALID_SESSION:
        // Resume was refused (or the session died): identify fresh next time.
        this.sessionId = undefined
        this.resumePending = false
        this.disconnect()
        break
      default:
        break // HEARTBEAT_ACK and unknown opcodes carry nothing for us
    }
  }

  /** HELLO answer: RESUME when we hold a live session, IDENTIFY otherwise. */
  private async resumeOrIdentify(): Promise<void> {
    try {
      const token = await getAppAccessToken(this.account.appId, this.account.appSecret)
      if (this.sessionId && this.lastSeq > 0) {
        this.resumePending = true
        this.ws?.send(JSON.stringify({ op: OP_RESUME, d: { token: 'QQBot ' + token, session_id: this.sessionId, seq: this.lastSeq } }))
        return
      }
      this.resumePending = false
      this.ws?.send(JSON.stringify({ op: OP_IDENTIFY, d: { token: 'QQBot ' + token, intents: GROUP_AND_C2C_EVENT, shard: [0, 1] } }))
    } catch (error) {
      console.error('[qq] identify failed: ' + String(error).slice(0, 200) + ' — the watch loop retries')
    }
  }

  disconnect(): void {
    try { this.ws?.close() } catch { /* already down */ }
    this.teardown()
  }

  private teardown(): void {
    if (this.heartbeat) { clearInterval(this.heartbeat); this.heartbeat = undefined }
    // A close before RESUMED means the resume never confirmed: fall back to a
    // fresh IDENTIFY on the next connect (session/seq kept only when confirmed).
    if (this.resumePending) { this.sessionId = undefined; this.resumePending = false }
    this.connected = false
  }

  isConnected(): boolean { return this.connected && this.ws?.readyState === WS_OPEN }
}
