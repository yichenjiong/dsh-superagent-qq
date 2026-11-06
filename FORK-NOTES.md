# Fork notes (yichenjiong/dsh-superagent-qq)

This fork carries local fixes for issues found while running the plugin with
the dsh-superagent prototype on 2026-11-07. Each commit above is standalone and
can be sent upstream.

Fixes in this fork:

1. `fix(token)` - the QQ OpenAPI `getAppAccessToken` response is snake_case
   (`access_token`, `expires_in`); the gateway read camelCase and never
   connected (bot showed offline).
2. `fix(c2c)` - `C2C_MESSAGE_CREATE` carries the peer openid in
   `author.user_openid`; the top-level `user_openid` is empty, so every direct
   message was dropped by `buildTrigger`.
3. `harden` - attachment filenames are sanitized before being embedded in the
   trigger text (prompt-injection surface), and the first raw payload dump is
   gated behind `QQ_DEBUG_PAYLOAD=1` so message content/attachment URLs are not
   logged by default.

Also note: the published npm tarball `dsh-superagent-qq@0.1.0` lacks
`cordis.patch.yml`, so installing from npm cannot mount the plugin; install
from source (`pnpm install && pnpm build`, then link the directory into a
profile) until the package is republished.

Verified end-to-end on 2026-11-07: QQ DM -> Trigger -> assessment -> ticket ->
executor commit/PR -> passive reply back in QQ.
