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

Validation passes all 334 Node tests, all 33 full workerd checks (31 parent
cases plus the two newly named fault-injection subtests), and all 12 native R2
workerd cases repeated with the SQL/KV guard enabled. The dedicated guarded
collection/reindex/eviction case also passes separately. The first sandboxed
workerd attempt failed to bind loopback sockets (`EPERM`); its failed log remains
preserved separately from the successful loopback-enabled runs. No native module
or production classifier changed for this correction.

This test checks application calls. Durable Objects still use a supported
Cloudflare namespace backend and may have platform-internal memory use. R2,
JavaScript, WASM, transport buffers and concurrent requests remain part of the
isolate budget. Keep bounded admission and require clean real-runtime evidence.

## Approved deployment and live check

The approved disposable tokens were installed and verified on the existing
`antfly-1278-g-mvslyh876561-1-{b,c}` pair. Reviewed candidate `489e28c436`
was deployed; control `c44896ac8c` retained its original reviewed bundles. The
executed candidate and control app versions were respectively
`5c69789c-2560-486c-b9ad-271174960717` and
`e1576bfa-91d3-4bb3-9174-cd79c099b7e6`.

Operational evidence is kept separately from the immutable proposal under
`examples/cloudflare-serverless/dist/qualification/sqlite-memory-live-mvtpxbwt/`.
The paired diagnostic used four tenants of 512 documents, three minutes of
fixed arrivals and all eight live diagnostic streams. Setup was bounded to
30 minutes. It did not consume cold epochs or qualify the three paired
one-hour trials. Token authorization was complete; qualification still depends
on measured latency, exact verification, independent diagnostic coverage and
zero real CPU/memory failures.

## Completed short diagnostic

Execution `10d29abf-29df-42c7-a3c7-784418e1f539` finished as a failed diagnostic,
not qualification. Both sides published all four 512-document seed corpora.
The offered workload ran six 30-second rounds over three minutes. Candidate
final publication convergence and all four final counts passed; control final
publication convergence failed. Full exact-body/receipt verification was not
part of this diagnostic and is not claimed.

The control recorded 16 client/round/monitor errors and the candidate 10. Those
are report entries, not unique failing physical invocations. Control failures
include network loss and Durable Object storage-startup/reset messages;
candidate failures include the R2 binding's internal-error code 10001. These
messages identify the boundary where failure surfaced, not its root cause.
Successful read/write p95 was 29,485/31,719 ms (67/23 samples) on control and
38,222/39,425 ms (68/24) on candidate. Failed requests are also retained; these
small, incomplete populations cannot establish a latency improvement or SLO.

All eight live streams report zero observed resource signals, including no
`SQLITE_NOMEM`, CPU-limit or memory-limit event. However, connection gaps occur,
and candidate app/writer/builder streams report abandoned native I/O. Persisted
windows include sampling and unsettled event sets. Independent invocation
populations do not reconcile. Consequently zero observed signals is not proof
of zero actual resource failures, and this run fails the unchanged coverage
gate. The 100 reserved physical cold epochs remain unused.

The next runtime investigation must address cancellation/drain completion,
R2/actor failure recovery and publication convergence; diagnostic ingestion and
independent population completeness remain separate qualification requirements.
Do not hide internal errors, count a successful retry as erasing a resource
failure, enlarge resource limits or replace complete diagnostics with sampled
estimates. The compatibility-profile historical SQLite diagnosis remains open;
the current native path's independence is covered by the guarded workerd tests.

## Verified retirement

Both approved stacks are deleted after evidence collection. The independent
post-cleanup inventory verifies zero scoped Workers, Durable Object namespaces
(including the original namespace IDs), Queues/DLQs or R2 buckets, down from
8 Workers, 6 namespaces, 4 queues and 2 buckets. The R2 drain acknowledged
3,835 control objects and 2,855 candidate objects, with no transport retries.
Original and operational disposable app-token copies and the temporary
observability-token copy are removed. Reports and pre-cleanup configuration
snapshots remain local. The 336 artifacts in the prior sealed batch-GC manifest
were independently hash-verified unchanged. Repository cache collection was
attempted; its active-build/process-inspection guard skipped deletion.

This closes this disposable deployment's cleanup, not qualification or the
historical platform allocator diagnosis. Future live trials require fresh owned
resources and complete diagnostic evidence.
