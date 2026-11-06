#!/usr/bin/env node
/** M1 real-machine probe for the QQ official bot gateway (no dependencies).
 *
 * Env: QQ_APPID, QQ_SECRET. Usage: node scripts/probe.mjs [seconds] (default 60).
 * Steps: getAppAccessToken -> GET /gateway -> native WebSocket -> HELLO (op 10)
 * -> IDENTIFY (op 2, intents GROUP_AND_C2C_EVENT = 1 << 25, shard [0,1])
 * -> heartbeat every d.heartbeat_interval with the last sequence number.
 * Every received frame prints one line: "<< op= t= s= d=(<=600 chars)".
 * Then @ the bot in a group / DM it to see GROUP_AT_MESSAGE_CREATE /
 * C2C_MESSAGE_CREATE frames — the dump verifies the field names the Source
 * translates (d.id, d.content, d.group_openid, d.user_openid, d.timestamp,
 * d.author.member_openid, d.author.user_openid).
 */

const appId = process.env.QQ_APPID
const appSecret = process.env.QQ_SECRET
if (!appId || !appSecret) {
  console.error('env QQ_APPID / QQ_SECRET required')
  process.exit(1)
}
const seconds = Number(process.argv[2] ?? 60) || 60
const INTENTS = 1 << 25

async function getAppAccessToken() {
  const res = await fetch('https://bots.qq.com/app/getAppAccessToken', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ appId, clientSecret: appSecret }),
    signal: AbortSignal.timeout(15_000),
  })
  const body = await res.json().catch(() => ({}))
  const token = body.accessToken ?? body.access_token
  if (!res.ok || !token) {
    throw new Error('getAppAccessToken failed: HTTP ' + res.status + ' ' + JSON.stringify(body).slice(0, 300))
  }
  console.log('>> token ok, expiresIn=' + body.expiresIn)
  return token
}

const token = await getAppAccessToken()
const res = await fetch('https://api.sgroup.qq.com/gateway', {
  headers: { authorization: 'QQBot ' + token, 'x-union-appid': appId },
  signal: AbortSignal.timeout(15_000),
})
const gateway = await res.json().catch(() => ({}))
if (!res.ok || !gateway.url) {
  console.error('gateway resolve failed: HTTP ' + res.status + ' ' + JSON.stringify(gateway).slice(0, 300))
  process.exit(1)
}
console.log('>> gateway: ' + gateway.url)

const ws = new WebSocket(gateway.url)
let lastSeq = 0
ws.onopen = () => console.log('>> ws open, intents=' + INTENTS)
ws.onmessage = (e) => {
  let p
  try { p = JSON.parse(String(e.data)) } catch { return }
  const d = JSON.stringify(p.d ?? null) ?? 'null'
  console.log('<< op=' + p.op + ' t=' + (p.t ?? '-') + ' s=' + (p.s ?? '-') + ' d=' + d.slice(0, 600))
  if (typeof p.s === 'number') lastSeq = p.s
  if (p.op === 10) {
    const interval = Math.max(5000, Number(p.d?.heartbeat_interval ?? 30_000))
    console.log('>> heartbeat every ' + interval + 'ms')
    ws.send(JSON.stringify({ op: 2, d: { token: 'QQBot ' + token, intents: INTENTS, shard: [0, 1] } }))
    console.log('>> IDENTIFY sent (op=2)')
    setInterval(() => {
      try { ws.send(JSON.stringify({ op: 1, d: lastSeq })) } catch { /* close handles */ }
    }, interval)
  }
}
ws.onclose = (e) => {
  console.log('>> ws close: code=' + (e?.code ?? '?') + ' reason=' + (e?.reason ?? ''))
  process.exit(1)
}
ws.onerror = () => console.log('>> ws error')

setTimeout(() => {
  console.log('>> probe window over (' + seconds + 's)')
  try { ws.close() } catch { /* already down */ }
  process.exit(0)
}, seconds * 1000)
