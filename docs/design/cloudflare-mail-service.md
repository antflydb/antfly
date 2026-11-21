# Authenticated Cloudflare mail example

This extends #1278 with application login and an email-style inbox. Better Auth
owns identity and sessions, Antfly owns durable application/auth state, R2 holds
private MIME and attachment objects, and Cloudflare supplies inbound routing and
outbound email transport. Work is isolated from the performance qualification
branch in `feat/cloudflare-mail-auth`.

## Login and persistence

The current Better Auth integration uses relational SQL sessions on Node or a
remote Antfly API owner. The document-oriented Worker API does not implement
that protocol. Workers support supplies a real atomic persistence boundary;
a SQL statement parser in JavaScript or a second SQLite database is not the
intended solution.

The Workers entry supplies a structured Better Auth adapter over Antfly's existing transactional
key/value metadata engine, with indexed identity/session lookup, atomic unique
claims, guarded verification-token consumption, transactional account creation,
rollback and committed-outcome handling. Run the adapter and transaction
callbacks inside one private auth actor, keeping requests and native I/O in
that actor's lifetime. Never replay application transaction callbacks after an
uncertain write. Reject unsupported schema changes and bound scans/transactions.

Browser requests use Better Auth session cookies. The application derives all
mailbox memberships and storage routes from the verified user. Browser-supplied
owner/tenant IDs, cursors and visibility tokens do not grant access. Auth tables,
operator credentials and raw object-store bindings are private capabilities.

## Email transport

Cloudflare Email Routing invokes an Email Worker's `email` handler. Validate the
SMTP envelope recipient against provisioned aliases, retain bounded raw MIME in
private R2 storage, and persist an ingest receipt before queueing MIME parsing
and Antfly indexing. Resolve Bcc delivery through the envelope, not the To
header. Do not assume RFC Message-ID is unique or trustworthy. Use delivery
receipts scoped to recipient and raw-content digest, retaining collision and
retry behavior explicitly.

Messages, attachment metadata, drafts, thread membership, labels and send intent
belong in Antfly. Large original MIME and attachment bytes belong in R2, exposed
only through session-authorized application routes. Parse MIME with a maintained
parser, enforce body/attachment/decompression budgets, display plain text by
default, and do not execute email HTML or fetch remote images automatically.

An outbox persists intent before a Queue dispatches Cloudflare Email Service.
Preserve accepted, queued, bounced, suppressed and unknown states. An ambiguous
send cannot be blindly retried: there is no assumed provider idempotency
contract. A stable Message-ID helps threading; it does not guarantee delivery
exactly once. Better Auth verification/recovery mail uses this transport too,
with transaction commit preceding external delivery.

Cloudflare's sending product is documented as **transactional-only**. The
proposed first example is a shared support/customer-correspondence inbox, pending
confirmation of the intended product scope. A general personal-mail service
needs an outbound provider appropriate to that use case. Keep the transport
replaceable rather than claiming Gmail-equivalent sending support.

Sources checked October 29, 2026:

- [Inbound routing](https://developers.cloudflare.com/email-service/get-started/route-emails/)
- [Workers sending API](https://developers.cloudflare.com/email-service/api/send-emails/workers-api/)
- [Transactional-only restriction](https://developers.cloudflare.com/email-service/reference/faq/)
- [Sending limits](https://developers.cloudflare.com/email-service/platform/limits/)
- [Domain configuration](https://developers.cloudflare.com/email-service/configuration/domains/)

## Local implementation and live rollout

Local work can cover real auth flows, persistence/reopen, isolation, inbound
MIME fixtures, duplicate delivery, queue replay, threading, private attachment
reads, outbox transitions and ambiguous sends without emailing anyone. Use a
local capture transport for verification/recovery mail and outbound tests.

Live activation requires a selected Cloudflare-managed domain and Email Service
onboarding, sender entitlement and DNS authentication. Do not change an existing
production domain's MX records or send real test mail as a side effect of local
implementation. Select a dedicated mail domain/subdomain and review the concrete
routing/DNS changes before activation. Arbitrary-recipient sending requires the
paid sending product; verified-destination behavior is a separate restricted
mode. Cloudflare acceptance is not proof of recipient inbox placement.


## Implemented example and validation

The [mail example](../../examples/cloudflare-serverless/MAIL.md) has two production
Worker entry points: a private Better Auth/native metadata owner and a mail app
with native per-mailbox authorities. A third capture Worker is used only in the
generated loopback development configuration. It has no SMTP binding. The
browser UI supports verified login/recovery, inbox/search, attachment download,
compose/reply and outbox inspection. Outbound intent quotas are atomic per mailbox.

Current ownership is one private mailbox per user. Shared memberships, labels,
drafts, account/content deletion and delivery-event reconciliation remain future
work. Account deletion is disabled in the application until that lifecycle is
implemented. Provider acceptance remains distinct from recipient delivery.

Live SMTP events receive independent UUID receipts. Immutable content hashes
deduplicate raw bytes only; identical messages and repeated Message-IDs remain
separate deliveries. Queue replay of an existing receipt remains idempotent.
Projection metadata is small and transactional; large immutable projection bytes
are prepared in R2 and adopted before native WAL mutation. New publication hints
are protected from an earlier publisher's cleanup with a token comparison.

Local validation passes the 121-test auth suite (including all 109 upstream
adapter cases), the 388-test Node regression run, and nine mail/UI tests including
two additional DOM cases. Actual workerd checks verified signup/login for two
users, injected native transaction rollback with no verification mail release,
queue replay, repeated identical messages, envelope/Bcc routing, Antfly search,
private attachments, replies, outbox isolation, persisted reopen and logout.
Guards reject application SQL/KV storage access. The local HTTP dev command also
passes signup, session, inbox/outbox and logout using capture-only delivery.
The browser connector could not initialize, so the UI received DOM/HTTP checks
rather than a visual browser review.

The native integration reused the unchanged qualified WASM artifacts from
`9b81fa0021`; no native engine code was changed or rebuilt for this auth work.
These local checks do not qualify a live domain, SMTP delivery or the performance
gate. No existing MX/DNS/routing records were modified and no real mail was sent.

## On-device search (progressive web app)

The mail app is also an installable PWA that can keep an **opt-in, private copy
of one mailbox in the browser** and search it by meaning. Retrieval runs at the
edge in the literal sense: on the user's device, with no inference service. The
server copy stays authoritative. The local copy is a rebuildable cache.

```
Mailbox actor ──/mail/sync (text only, cursor)──▶ page
                                                   │ EmbeddingGemma 2 (@antfly/web, WASM CPU)
                                                   ▼
                       database worker: Antfly embedded WASM (hosted profile)
                                                   │ HostFileStorage (sync file ops)
                                                   ▼
                          OPFS: pool of sync access handles (slot files)
```

### Sync log

Each mailbox actor keeps an append-only log (`mail/v1/sync/log/<seq>`) written
in the **same transaction** that marks a message indexed and lists it. A
per-message marker makes recording idempotent across queue replay. Mailboxes
created before the log existed are backfilled in bounded steps by walking the
received-order index; markers prevent duplicates when new mail races the
backfill. A cursor beyond the log's head, for example after a server-side
restore, gets `reset: true` and the replica rebuilds from the start instead of
failing on every sync. `GET /mail/sync?cursor=<seq>` returns up to 25 messages
after the cursor, oldest first: id, thread, subject, addresses, time, attachment names and
sizes, and text truncated to 64 KiB. It omits attachment bytes, R2 keys and
digests. The session selects the actor, as for every other route; a cursor is
only a position and grants nothing. Deletion and edits are not synchronized,
because the app has no message deletion yet. When it gains one, the log needs
tombstone entries.

### Embeddings

`@antfly/web` gained an `embed()` task for EmbeddingGemma 2. It is the same
browser encoder path as embedding-similarity decisions: the task prefix
(`RETRIEVAL_DOCUMENT` / `RETRIEVAL_QUERY`), tokenization, encoder and
Matryoshka truncation, plus the same `model_identity`. Unlike decisions, inputs
are truncated to `max_tokens` (default 512, configured to 256 here) instead of
rejected, because documents are routinely long. Requests carry at most 32 inputs
and 16,384 tokens. Execution is WASM CPU only; the browser has no WebGPU path for
this encoder yet.

The operator publishes the model once with `publish-local-model.mjs`. The
browser never loads the media towers, so the script removes exactly those
tensors (the regex the runtime uses to skip them). That roughly halves the
download and keeps every file under `@antfly/web`'s 1 GiB catalog file limit.
The bundle's own integrity receipt pins the unstripped weights, so it is
replaced by catalog SHA-256 pins, which `downloadCatalogModel` verifies and
caches in OPFS. Weights are served same-origin from a private R2 bucket, which keeps
`connect-src 'self'`. The deployment must not become an open mirror, but
`@antfly/web` downloads pinned files with `credentials: "omit"`, so a cookie
cannot authorize them. Instead, the session-authenticated catalog response
rewrites each file URL with a one-hour grant: an HMAC-SHA-256 over the path and
expiry, keyed by `LOCAL_MODEL_GRANT_SECRET`. The weight route accepts a session
or a valid, unexpired grant for that exact path. Rotating the secret revokes
outstanding grants. The weights keep their upstream license.

Vectors are written as an **external** `dense_vector` index (cosine, 256 dims by
default) through `_embeddings`, so the embedded database needs no embedder
callback. Queries combine a full-text match on one `content` field (subject,
sender, body) with the query vector, merged with RRF. The local space records
the model id, file pins, dimensions and token limit. A copy built for a
different space is discarded and rebuilt, never mixed.

### Persistence in the browser

The database's files live directly in OPFS. `HostFileStorage` implements
Antfly's `Storage` interface over a small synchronous file-operation table,
which the WASM bundle imports from the host (`antfly_host_fs_*`).
`antfly_embedded_opfs_fs.mjs` supplies that table in the database worker.

OPFS's synchronous API works only on files that are already open; opening,
renaming and deleting are asynchronous. So, like SQLite's `opfs-sahpool` VFS,
the backend pre-opens a pool of physical slot files and keeps the logical
namespace inside them:

- **Slots.** Each slot carries two checksummed header records (generation +
  path) ahead of its data. Create, rename and delete overwrite the older record
  and flush before returning. A torn write therefore leaves the previous record
  in force, and each namespace change is all-or-nothing.
- **Rename.** The moved slot's record (with a newer generation) is flushed
  before the replaced destination is freed, and reopen keeps the higher
  generation. That gives the engine the atomic rename its publication protocol
  relies on.
- **Data.** File data is durable at `sync`.
- **Capacity.** The pool grows asynchronously between database calls
  (`ensureCapacity`). An operation that finds no free slot fails cleanly rather
  than losing data.
- **One tab at a time.** Access handles are exclusive, so a second tab is told
  the index is open elsewhere.

The sync cursor and vector-space descriptor are a `_local/state` document
written in the same batch as each page of messages. Cursor and data therefore
commit together through the database's own log, and a reload resumes after the
last committed page. Only caches stay in WASM memory, so the copy is no longer
bounded by the 2 GiB wasm32 memory.

### Privacy and lifecycle

- **Off by default.** Enabling asks for confirmation and shows the model's
  download size, then requests persistent storage.
- **Text and vectors stay on the device.** The server sees only `/mail/sync`
  reads, the same data the inbox already shows that user.
- **Sign-out removes every on-device copy** and the remembered offline identity,
  for shared machines. The database worker releases its exclusive OPFS handles
  before the copy is deleted. The offline identity is removed first, so offline
  reopening is impossible even if deletion fails (for example, while another
  tab holds the copy). Sign-out always completes. A failed deletion is retried
  on the next page load and blocks the copy from being reopened until it
  succeeds. The model cache is kept because it contains no user data.
- **"Remove from this device"** deletes one mailbox's copy.
- **Offline.** The service worker caches the shell, the content-addressed runtime
  (immutable), and the runtime config and model catalog (network-first). It
  never caches mail, auth or weights. If the server is unreachable at startup
  and this device holds an opted-in copy, the app opens into on-device search
  instead of the sign-in screen; messages open from the local copy.
- **CSP.** It adds only `'wasm-unsafe-eval'`, `worker-src 'self'` and
  `manifest-src 'self'`. Everything stays same-origin.

### Validation and limits

Verified end to end in headless Chromium (`test/local-search-browser.mjs`).
The test uses the real `mail-public` app, the Worker's CSP, the runtime from
`build-local.mjs` and the real EmbeddingGemma 2 checkpoint
(`google/embeddinggemma-2` @914f7f8, stripped by `publish-local-model.mjs`);
only the server APIs are stubbed.

- Opting in, downloading and loading the model, then syncing and embedding 5
  messages into OPFS took 59 s.
- Device-only searches ("how much do I owe the plumber", "when does my plane
  leave", "weekend noodle plans with friends") each ranked the right message
  first, in about 3 s.
- After a reload the copy reopened from OPFS without re-indexing, and the
  next query was also correct.

Other checks:

- **Real checkpoint in the browser runtime**
  (`test-embedding-gemma2-retrieval.mjs`): five queries each retrieve their
  message, with a clear gap to the rest (0.855 against at most 0.606). The
  552 MiB text-only copy embeds identically to the 1.49 GB bundle (cosine
  above 0.99999).
- **OPFS pool:** `wasm_opfs_fs_test.mjs` simulates OPFS with separate durable
  and volatile bytes. It covers the storage contract, unsynced-data loss,
  interrupted renames, torn header records, deletion, capacity and the
  exclusive lock. `wasm_opfs_fs_browser_test.mjs` repeats the round trip on
  real sync access handles in Chromium, and with `ANTFLY_WASM` runs a real
  database through 20 batches, a reload and searches.
- **Native and WASM:** a native embedded API test runs a hosted database
  through `HostFileStorage`; the WASM smoke test does the same through the
  real module and the pool.
- **Mail example:** `test/mail-local.test.mjs` covers the sync log and reset,
  route scoping and model grants, CSP and caching, document and query shapes,
  model stripping, the opt-in UI, sign-out wipe and retry, offline start and
  service-worker caching.

Issues these runs found and fixed:

- Dense search was compiled out of the WASM build.
- The browser runtime rejected the real checkpoint's scalar tensors.
- External vector indexes need a `field`.
- The pool's reserve was too small for an index build's file burst.

Limits and remaining work:

- **Deployment.** A real Cloudflare deployment (R2, Better Auth, Email
  Routing) has not been run; Workers behavior is covered by the Node tests
  above.
- **Browsers.** Only Chromium was exercised. Safari and Firefox support sync
  access handles in workers, but they are unverified here.
- **Throughput.** Embedding runs on WASM CPU, at roughly a few seconds per
  message on this host, so a large mailbox's first sync takes a long time.
  It runs in the background and resumes after reloads.
- **Build memory.** The embedded database WASM link needs about 20 GiB.
- **Deletes and edits** are not synchronized, because the app has no
  deletion yet.
