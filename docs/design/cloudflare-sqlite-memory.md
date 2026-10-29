# SQLite memory failure: attribution and native isolation

The historical compatibility profile reported a real `SQLITE_NOMEM` during a
live collection pass after 591 completed tasks. Its precise cause remains
unreproduced. The application-level long-term solution is the existing
Antfly-native coordinator: Antfly pages, WAL, collection frontier and marks
persist in R2, without application SQL or platform KV operations. This removes
the SQLite journal from the database path; it does not claim a Cloudflare
platform allocator fix.

## Separate the three kinds of evidence

1. The [historical live investigation](cloudflare-native-audit.md#extended-sqlite-investigation)
   records cursor draining, bounded frontier work, non-writing pressure
   diagnostics and retryable 503 responses. The dedicated live SQLite soak
   completed 1,340,160 statements without reproducing the failure. Retain those
   compatibility protections and the unresolved historical diagnosis.
2. Recent passing local workerd logs deliberately contain `SQLITE_NOMEM`.
   `test/workerd.test.mjs` replaces `objectCall` and `collection` with methods
   that throw that literal. Earlier read-state/proof/multiprovider reports
   incorrectly described those local lines as recovered platform memory
   pressure. The original sealed logs remain unchanged. The fixture now labels
   the synthetic error and reports each injected operation as a named subtest.
3. Real resource failures in a deployed or local runtime are separate events.
   The production classifier, physical-I/O counters, sanitized tail events and
   gate rejection rules are unchanged. A successful retry cannot erase a real
   allocation failure, including one arriving after its caller timed out.

## Verify independence at the host boundary

The actual-workerd native R2 fixture now makes both `ctx.storage.sql` and
`ctx.storage.kv` throw `UnexpectedPlatformDatabaseAccess` before coordinator
initialization. Native initialization, writes, queries, receipt replay,
publication, both collectors, reindex and recovery must work with that guard.
The existing erase-platform-storage/reopen checks still apply. The legacy
migration source retains its real SQLite implementation; only the native target
has the guard. This catches a regression back to platform database authority.

This test checks application calls. Durable Objects still use a supported
Cloudflare namespace backend and may have platform-internal memory use. R2,
JavaScript, WASM, transport buffers and concurrent requests remain part of the
isolate budget. Keep bounded admission and require clean real-runtime evidence.

## Approved deployment and live check

The approved disposable tokens are installed and verified on the existing
`antfly-1278-g-mvslyh876561-1-{b,c}` pair. Reviewed candidate `489e28c436`
is deployed; control `c44896ac8c` retains its original reviewed bundles. The
candidate and control app versions are respectively
`5c69789c-2560-486c-b9ad-271174960717` and
`e1576bfa-91d3-4bb3-9174-cd79c099b7e6`.

Operational evidence is kept separately from the immutable proposal under
`examples/cloudflare-serverless/dist/qualification/sqlite-memory-live-mvtpxbwt/`.
The paired diagnostic uses four tenants of 512 documents, three minutes of
fixed arrivals and all eight live diagnostic streams. Setup is bounded to
30 minutes. It does not consume cold epochs or qualify the three paired
one-hour trials. Token authorization is complete; qualification still depends
on measured latency, exact verification, independent diagnostic coverage and
zero real CPU/memory failures.
