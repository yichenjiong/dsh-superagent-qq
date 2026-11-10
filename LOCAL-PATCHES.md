# Local patches (not upstream)

This checkout is a local build of https://github.com/Shuai-S/dsh-superagent-qq
(main @ 62a6d72, package version 0.1.1), installed into the `superagent` DSH
profile as `link:/home/jiong/dsh-superagent-qq`.

Re-apply these after updating from upstream; each was needed on 2026-11-07.

1. `src/gateway.ts` — QQ `getAppAccessToken` returns snake_case
   (`access_token`, `expires_in`); accept both snake_case and camelCase.
   Without this the gateway never connects and the bot shows offline.

2. `src/watch.ts` — C2C (`C2C_MESSAGE_CREATE`) events carry the user openid
   only in `author.user_openid`; the top-level `user_openid` is empty. Fall
   back to `author.user_openid` for non-group messages. Without this C2C
   messages are silently dropped (`buildTrigger` returns undefined).

3. `src/watch.ts` — harden prompt-injection surface: attachment `filename` is
   sanitized (control characters/newlines collapsed, trimmed to 80 chars)
   before being embedded in the trigger text; raw first-payload dump is now
   gated behind `QQ_DEBUG_PAYLOAD=1`.

4. `scripts/probe.mjs` — same token-field fix as (1), for local diagnostics.

Note: the published npm tarball `dsh-superagent-qq@0.1.0` is missing
`cordis.patch.yml`, so it cannot mount; this local build fixes that by using
the repository checkout (which includes the patch and locale files).

Build: `pnpm install && pnpm build`; the profile links this directory, so a
rebuild plus a dsh-superagent restart picks up changes.

5. `src/qq-kind.ts`, `src/watch.ts` — Phase 5.5 (2026-11-09): group rows gained
   `repository`; a configured group's trigger carries `channel.address.repository`
   and `meta.repository`; a message in an unbound group redeems a one-time
   binding code or is answered without creating a ticket; attachments are not
   downloaded before that decision; the client hash no longer includes group
   repositories, so rebinding does not reconnect every bot.

6. `src/qq-kind.ts` — Phase 5.5 fix (2026-11-09): `operators` lists the
   `user_openid` values allowed to create a project from a direct message
   (rows of `{ openid }` or plain strings; empty disables it). Direct-message
   answers and `orchestrator_project_create` use this list; group member
   openids are never operators.

7. `src/robot.ts`, `src/qq-kind.ts`, `tests/files.test.ts` — Phase 6 (2026-11-10): outbound files.
   A `reply` may carry `[[qq-file:/abs/path]]`; the QQ channel uploads it (chunked:
   upload_prepare -> part PUT -> part_finish -> /files) and sends `msg_type: 7`.
   - Staging: `<files.ticketRoot>/<ticketId>` (default `~/.dsh/qq-outbox`), plus
     `files.allowedRoots` (default `~/.dsh/qq-outbox/manual`). Worker learns the
     directory from `DSH_SUPERAGENT_QQ_OUTBOX` (SourceKind.environment).
   - Path checks: absolute, O_NOFOLLOW, regular file, nlink == 1, realpath prefix,
     extension whitelist, size cap (group 20 MB / c2c 50 MB).
   - `msg_seq` is persisted in `~/.dsh/qq-msg-seq.json` (0600) as two layers:
     `counters[msgId]` (increasing) and `seqs[messageKey]` (reused on retry).
     A corrupt or unreadable store fails closed; a counter behind an allocated
     seq is repaired upward.
   - Only `reply` may send files; `acknowledgement` and `notice` strip the marker.
   - quota: 5 passive replies per group message, 4 per direct message; files are
     budgeted as `limit - used - 1` (one slot reserved for the failure summary).
   - 40093002 (daily capacity) fails fast; 850012 stays retryable.
