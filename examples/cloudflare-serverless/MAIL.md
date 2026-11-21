# Antfly Mail

An authenticated email-style application built from Cloudflare Workers, Email
Service, Queues and R2, with Antfly WASM providing durable auth metadata, mailbox
records and text search. It is a separate example entry point; the operator API
and the performance qualification profiles are unchanged.

Implemented:

- Better Auth signup, password hashing, verification, recovery, database sessions
  and logout through a private auth Worker.
- A private mailbox per verified user. Server-side session membership selects
  storage; browser tenant/owner headers do not.
- Inbound Email Worker handling, raw MIME retention, bounded parsing, private
  attachments, threading metadata and Antfly search.
- Compose/reply, a durable outbox, provider acceptance status, and an atomic
  per-mailbox send-intent limit (20/hour by default, configurable up to 1,000 with
  `MAIL_SENDS_PER_HOUR`). Unknown submissions are never automatically resent.
- Queue replay and restart-safe receipt/projection state, with periodic bounded
  reconciliation of pending work.
- Message deletion: a message disappears from the inbox, search and every
  on-device copy, and its stored MIME, attachments and search document are
  purged.
- An installable PWA with opt-in on-device semantic search (see below).

This is webmail, with no IMAP/POP server or Gmail synchronization. Mailboxes are
currently private per-user inboxes; shared mailbox memberships, folders/drafts,
mailbox deletion/retention controls (trash, undo, automatic expiry) and
delivery-event reconciliation are future work. Account deletion is disabled in this app until the mailbox lifecycle is
implemented. The generic auth adapter retains its own account-deletion support.

## On-device search (PWA)

The app installs as a progressive web app. Users can opt in to keeping a
**private copy of their mailbox in the browser** and searching it by meaning
with EmbeddingGemma 2, which runs on their device through Antfly's WASM
inference. Message text and embeddings never leave the device, and search keeps
working offline. It is off by default; signing out deletes every on-device copy.

How it works:

1. The page pulls new messages, text only, from `GET /mail/sync?cursor=`, an
   append-only per-mailbox log.
2. It embeds them in batches with `@antfly/web`'s `embed()` task.
3. A dedicated worker writes them to an embedded Antfly database: a full-text
   index plus an external 256-dimension vector index.
4. The database's files live in OPFS. Each page's messages and the next
   cursor are committed in one batch, so a reload resumes where it left off.
5. Searches run keyword and semantic retrieval locally and merge them with RRF.

See the [design](../../docs/design/cloudflare-mail-service.md#on-device-search-progressive-web-app)
for consistency, privacy and limits.

Enable it on a deployment:

```sh
# 1. Build the browser runtime into mail-public/local/ (Zig 0.17 + pnpm).
#    The WASM modules exceed the 25 MiB Workers asset limit, so --upload puts
#    them in the R2 bucket under the same versioned path.
#    Optional: LOCAL_EMBEDDING_DIMENSIONS=128|256|512|768, LOCAL_EMBEDDING_MAX_TOKENS.
npx wrangler r2 bucket create antfly-mail-models
ANTFLY_ZIG=/path/to/zig npm run build:local -- --upload

# 2. Set the secret that signs short-lived model download grants.
npx wrangler secret put LOCAL_MODEL_GRANT_SECRET --config wrangler.mail-app.jsonc

# 3. Publish a text-only copy of EmbeddingGemma 2 to the same private bucket.
antfly inference pull hf:google/embeddinggemma-2:bf16-safetensors-bundle-v1
npm run publish:local-model -- \
  --model-dir ~/.antfly/inference/models/google/embeddinggemma-2--antfly-... \
  --upload --bucket antfly-mail-models
```

The publish script removes the image/audio towers, which the browser never
loads, and writes a `catalog.json` with SHA-256 pins that the browser verifies.
Files are served same-origin from `/local/models/`. The browser's model cache
downloads without cookies, so the signed-in catalog response gives each file a
one-hour HMAC grant URL; files are never served without a session or a valid
grant. `npm run dev:mail` generates a local grant secret. The
weights keep their upstream license; serving them is subject to its terms. For
`npm run dev:mail`, add `--local` to both upload commands to use the local
Miniflare bucket.
Without `mail-public/local/` or a published catalog, the opt-in panel reports
that on-device search is not installed, and the rest of the app is unaffected.

Building the embedded database WASM needs a lot of memory: its LLVM step
declares a 20 GiB ceiling. On a 16 GiB machine, add swap and pass the
ceiling explicitly (`zig build wasm -j1 --maxrss 21474836480` took about 28
minutes with 12 GiB of swap), or build on a larger host and copy
`zig/zig-out/antfly-wasm/`. Then run `LOCAL_SKIP_ZIG=1 npm run build:local`.

To try the whole feature in headless Chromium with the real model and stubbed
server APIs:

```sh
npm run publish:local-model -- --model-dir <bundle> --out dist/local-model
LOCAL_SEARCH_MODEL=dist/local-model node test/local-search-browser.mjs
```

Limits:

- Embedding runs on the WASM CPU backend, so first sync of a large mailbox
  takes time. It runs in the background and resumes after reloads.
- The on-device copy can be open in one tab at a time, because OPFS access
  handles are exclusive. Other tabs fall back to server search.
- Deletions reach a device at its next sync (immediately on the device that
  deleted). Messages are never edited, so there is nothing else to propagate.

## Local setup

From the repository root, install/build the auth workspace dependencies:

```sh
pnpm --dir ts install --frozen-lockfile --filter @antfly/better-auth...
pnpm --dir ts --filter @antfly/better-auth build
```

From `examples/cloudflare-serverless`:

```sh
npm ci
npm run build
npm run dev:mail
```

The native build requires the repository's pinned Zig toolchain. The dev command
creates isolated configs under `dist/mail-local/`, generates a private local auth
secret in the auth Worker's `.dev.vars`, and starts both Workers at
`http://127.0.0.1:8787`. Set `MAIL_DEV_PORT` to choose another port. Preserve the
local secret and Wrangler state to retain sessions across restarts. No live
resources or DNS rules are created by the dev command.

Local development bypasses email verification only with `AUTH_DEVELOPMENT=true`
and a loopback HTTP auth origin. Production uses HTTPS and verification. A local
capture Worker records outgoing messages without an SMTP binding; the integration test uses an explicit
capture Worker and never sends real mail. Use the private test fixture to drive
inbound events and inspect verification/recovery emails.

Run local checks (`test:mail` includes the on-device search tests):

```sh
npm run test:mail
npm run test:mail-workerd
pnpm --dir ../../ts --filter @antfly/better-auth test:workers
```

The workerd command rebuilds fresh bundles automatically. Its native integration
checks verified login for two users, SMTP-envelope recipient routing, Antfly
search, cross-user message/attachment/outbox isolation, reply addressing, logout
and persisted reopen. Test guards throw on platform `ctx.storage.sql` or
`ctx.storage.kv` access. Core data still uses Cloudflare's SQLite-backed actor
registration type, but the application does not read or write that database.

## Live deployment

`wrangler.mail-auth.jsonc` and `wrangler.mail-app.jsonc` are templates. Before
activation, choose an origin and dedicated mail domain, change placeholder
resource names, provision the three R2 buckets and both Queues/DLQs, install a
stable `BETTER_AUTH_SECRET` on the private auth Worker, then deploy the auth and
app Workers. Both must agree on `AUTH_ORIGIN` and `MAIL_DOMAIN`. Set the app's
`AUTH_SERVICE` binding to the deployed private auth Worker.

Onboard the domain for Email Routing and Email Sending, configure SPF/DKIM/DMARC
and MX records, and route the provisioned mailbox addresses to the mail app
Worker. A catch-all may route to the Worker, which rejects unprovisioned aliases.
The templates intentionally omit Wrangler `addresses`, so deploying them does
not modify an existing domain's Email Routing rules. Do not replace a production
mail domain's MX records just to try this example.

Cloudflare Email Sending is currently **transactional-only**. Use an appropriate
customer-correspondence/transactional application; this example does not claim
Cloudflare is a general personal-email or bulk-marketing delivery provider.
Arbitrary-recipient sending requires Workers Paid and an onboarded sender domain.
See [Email Service](https://developers.cloudflare.com/email-service/) and the
[transactional restriction](https://developers.cloudflare.com/email-service/reference/faq/).
No live domain or SMTP delivery was qualified by the local tests.

## Storage and failure semantics

Auth runs in one private metadata owner using `@antfly/better-auth/workers` and
Antfly's native transactional metadata engine on R2. Each mailbox has its own
fenced authority and collection. The app never exposes the raw operator API,
auth rows or R2 keys as browser capabilities. Sessions are verified before
message lookup, search, attachment retrieval and send intent creation. Mutating
mail routes require the configured Origin. Email HTML is not rendered; the UI
uses text nodes. Attachments download as octet-stream with `nosniff`.

Raw MIME and large immutable projections are prepared in private R2 outside
metadata transactions. Metadata holds bounded receipt and object references.
The queue adopts one immutable projection and submits an idempotent native WAL
mutation keyed by the receipt, then publishes the index. A compare-and-delete
publication hint prevents an older publisher from erasing a newer commit's work.

SMTP invocations receive separate delivery receipts. Raw MIME and attachments
are stored per delivery (`raw/<receipt>`, `attachments/<receipt>/<index>`) so a
deleted message's bytes can be removed without reference counting; content
hashes still verify every object.

Deleting a message (`DELETE /mail/messages/:id`) works in two steps:

1. **Mark.** One metadata transaction marks the receipt deleted, removes it
   from the inbox order, appends a sync tombstone and records a purge intent.
   The message is then hidden from listing, lookup, search and attachment
   download.
2. **Purge.** The message's search document is removed and the R2 objects it
   owns are deleted. Only then is the receipt reduced to a small tombstone.

The purge runs inline, and the queue and the scheduled sweep retry it until it
completes. A deletion racing an in-flight ingest re-arms the purge, with a new
mutation id, for anything the ingest writes afterwards. Objects written before
per-delivery keys (`raw/<sha256>`, `attachments/<sha256>`) may be shared by
identical deliveries and are left in place. Repeated RFC Message-IDs and identical messages
remain separate deliveries; absent a provider delivery ID, SMTP re-delivery
cannot be distinguished perfectly from a legitimate identical message. Queue
replays of an existing receipt do not duplicate it. Ambiguous thread parents
start a new thread rather than merging unrelated messages.

Limits: 5 MiB raw MIME, 512 KiB decoded text, 16 attachments and 4 MiB total
attachment bytes; outgoing text is capped at 64 KiB to keep outbox records within
the native metadata record budget. Structural parsing limits also apply. MIME
parse-limit violations become visible quarantine messages with the original
retained privately. Other processing failures retry through the configured DLQ.
This is not malware scanning, spam classification, or a complete retention policy.

Outbox states distinguish `pending`, `sending`, `accepted`, `rejected`, and
`unknown`. Provider acceptance is not confirmed recipient delivery. A lost
submission acknowledgement or expired claim becomes unknown and is not resent.
Duplicate client operation IDs must have identical content. Neither a Message-ID
nor the current Cloudflare sending API is assumed to guarantee exactly-once SMTP
delivery. The transport boundary can be replaced without changing mailbox/auth
ownership. See the [design](../../docs/design/cloudflare-mail-service.md).
