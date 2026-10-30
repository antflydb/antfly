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

## Diagnostic memory hardening and the next hosted reproduction

The October 30 follow-up distinguishes prevention from root-cause proof. The
historical live SQLite failure remains unreproduced; the native R2 storage path
already removes application SQL/KV. The incomplete later hosted captures do not
establish a new occurrence of that same failure. Do not describe an intentional
local injection or a native `OutOfMemory` as a recovered live `SQLITE_NOMEM`.

Current official references support investigating the complete isolate:

- [Cloudflare memory metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/)
  describe a 128 MB isolate shared by Worker code and potentially multiple
  Durable Objects, with sampled memory quantiles rather than per-object peaks.
- [Worker limits](https://developers.cloudflare.com/workers/platform/limits/)
  count JavaScript and WebAssembly memory and recommend streaming large bodies.
- [SQLite's result-code definition](https://www.sqlite.org/rescode.html#nomem)
  identifies an allocation failure; the code alone does not identify which
  allocation or caller exhausted memory. The current
  [workerd SQLite implementation](https://github.com/cloudflare/workerd/blob/main/src/workerd/util/sqlite.c%2B%2B)
  has per-database memory scopes. Inferring the original production cause from
  these mechanisms would be premature.

The new observer's per-upload byte bound did not limit simultaneous parsing
across invocations. It also buffered an entire compressed body before making
another decompressed copy. These are concrete, independently fixable pressure
risks, not a demonstrated explanation of the historical SQL failure.

The next observer release adds an isolate-wide diagnostic admission lane:

- One heavy parse/capture executes at a time, with at most four waiting callers
  and a 15-second admission wait bound. Waiting HTTP bodies remain unread.
  Tickets carry no shared request promise, callback, transport or body. Each
  caller waits using its own timer and performs I/O in its own invocation.
  This protects the receiver, independent Tail observer and read-only reader in
  their separate deployments. It is not per-tenant admission that would multiply
  the memory allowance across colocated sources.
- Gzip magic detection and decompression stream from the bounded wire input.
  The original 6 MiB wire/decompressed bounds remain; no second complete
  compressed-body buffer is created. OTLP and Tail persistence reuse sanitized
  records rather than serializing and reparsing a duplicate object tree.
- JSON preflight counts at most 262,144 allocation nodes and depth 32 before
  parsing, including NDJSON and embedded log bodies. Byte limits alone do not
  bound allocation expansion from tiny JSON values. A complexity rejection is
  durable; an oversized/complex log remains a truncation/gap signal. It cannot
  silently become an empty clean diagnostic.
- Resource failures at proof/archive R2 calls emit only fixed component,
  boundary, storage and operation labels. Original errors and causes survive;
  no key, token, URL, arbitrary exception text or body is logged.

These bounds reduce identifiable application pressure. They do not measure
absolute heap usage or fix Cloudflare's internal SQLite allocator. Admission
exhaustion/expiry is a failed delivery and prevents a clean trial. The observed
queue/active counts on receiver health are per current isolate and are not a
fleet-wide memory census.

Two gate-evidence gaps were corrected as part of the investigation. Reader
health now includes the receiver's rejection prefix in addition to the Tail
observer's rejection prefix; a bounded successful shape sample cannot hide a
later export failure. A read-only reader resource failure returns a fixed
resource code to the driver. The driver retains a bounded failure ledger across
all capture attempts, saves it with the report and carries it into offline
reconciliation. Later successful reads do not erase it or permit a clean
rehearsal verdict. The reader remains read-only.

Local validation includes five simultaneous one-MiB gzip uploads in actual
workerd, complete archive record counts, FIFO/cancellation/overflow/expiry,
gzip split-magic and expansion/wire rejection, JSON depth/node amplification,
safe boundary labels, receiver rejections beyond the successful health sample,
and a test-only reader `SQLITE_NOMEM` followed by a successful response. The
latter still fails the run verdict. The five-upload experiment observes a
single active parser; it is not a measurement of production peak memory.

For the next hosted attempt, preserve corrected control `c44896ac8c` and its
original WASM modules, use the same newly sealed observer format for both
sides, and create fresh workload identities. Start with the existing short
four-tenant/512-document rehearsal, not the reserved cold epochs or hour-long
trials. Check all source/receiver rejection prefixes and retain every driver
resource signal. Re-run exact body/receipt verification and collector/restart
checks. Any real `SQLITE_NOMEM` remains a failed trial even if retry succeeds.

If it recurs, the component/boundary identifies the next investigation:
application SQLite means the compatibility path or a regression; native WASM
allocation means the native working set; proof/archive R2 means the diagnostic
binding path; a storage-startup/reset exception before Antfly work remains a
provider investigation. Capture exact Worker versions, time ranges, sanitized
failure codes, sampled isolate metrics and a minimal reproducer for Cloudflare.
A sampled low-memory interval or a successful run cannot establish the absence
of every transient allocator fault.

### Hosted rejection and bounded Tail publication pipeline

The first approved deployment of this hardening used candidate `07456d5cb3`,
corrected control `c44896ac8c` and observer
`antfly-1278-observe-mvv6mn5tacc2`. The initial attempt,
`d81c381a-7073-423f-802d-1302086be55e`, was stopped during seeding because real
Tail deliveries exceeded the monolithic admission queue. The final health
sample retained `ObserverTailFailed:TelemetryDiagnosticAdmissionExceeded`.
Its original report, manifests and diagnostics remain preserved. No measured
read/write workload completed, and no latency or allocator-absence claim follows
from this failed attempt. Its client and reader ledgers recorded no resource
signals, but incomplete ingestion prevents using that as exhaustive evidence.

Holding the parser permit during R2 writes was the bottleneck. Tail publication
now separates preparation from persistence:

1. Decoded provider inputs have a conservative aggregate 16 MiB input charge.
   A single Tail preparation lane can queue up to 64 small tickets only while
   that byte charge fits; HTTP receiver/reader admission retains its original
   four-waiter bound. Input estimates include strings, property names and node
   overhead, without copying or logging raw values.
2. Preparation produces bounded encoded archive/counter/witness objects, rather
   than retaining arbitrary parsed object trees during slow writes. Before
   releasing the parser, the caller owns an independent write permit: at most
   16 active deliveries and 16 MiB of total prepared bytes. A single Tail
   dispatch's prepared output is additionally capped at 8 MiB + 64 KiB.
3. The originating caller persists up to four source scopes concurrently. Each
   scope commits its archive before its counter and real witness pointers.
   All physical writes settle before its byte reservation is released, including
   when a peer write fails. No promise, transport or request context migrates
   to another caller. Completed layout booleans are cached by bucket identity;
   pending I/O is never cached across invocations.

This overlaps binding latency within an explicit byte budget. It does not
increase the application's memory limit, loosen diagnostic settlement or discard
failed events. The local regression sends 32 simultaneous deliveries across
all eight sources and checks all 256 counter objects, caller-context behavior,
byte ceilings and complete release after success/failure. Tests in actual
workerd exercise the same burst and observe overlapping publication with a
single active preparation lane. The native R2 SQL/KV-forbidden regression also
passes collection, reindexing, eviction and erased-platform-storage recovery.

Capture retries now have explicit generations. `begin-capture` requires all
local rehearsal reports to be complete, archives the previous exact manifests
and failed report, and assigns new source capture IDs plus an observer capture
UUID. New global observer/receiver rejection prefixes use that UUID; the reader
and driver pin it. Old rejection objects and failed reports remain under their
original prefixes. Versioned `revise-observer`, `revise-reader` and
`revise-receiver` actions retain prior bundles and verify the newly deployed
version before another rehearsal. This creates a separate trial, never a way
to clear a failure inside an existing trial.

The same failed hosted generation also retained 16
`TelemetryJsonComplexityExceeded` receiver rejections in its final bounded
health sample. Valid bulk exports can exceed a whole-payload node ceiling even
when each sanitized storage chunk fits. The receiver now validates complete
JSON syntax with a depth-bounded, non-materializing walk, then materializes only
bounded resource metadata and record slices. Whole-payload byte, envelope,
resource, scope, record and chunk limits remain. Every parsed record slice stays
within the existing JSON allocation-node guard; oversized embedded log JSON
still becomes a diagnostic gap. Invalid trailing syntax is rejected before any
chunk is published. Raw provider bodies are not spooled to storage.

The dense-export regression uses 6,000 records and 16 attributes per record,
below the original six-MiB body limit but above the former whole-tree node
ceiling. It preserves all records in Node and actual workerd while parsing only
bounded slices. The revised local suite passes 479 Node tests and seven focused
workerd integrations, including the dense export and 32-caller Tail burst. The
first failed hosted generation remains failed; these local results authorize a
new reproduction, not relabeling the old evidence.

The rehearsal driver now claims each paired corpus execution in the run
manifests and closes that claim after its report completes. A fresh retry must
use `prepare-corpus`; it cannot silently reuse a failed or completed corpus.
The one older completed rehearsal predates that claim. An explicit
`--adopt-completed-rehearsal` transition verifies its source/module/bundle/corpus
identity and unused recovery actors, records its report hash in the transition,
and archives the original manifests before assigning fresh namespace identities.
It does not amend the failed report or confer qualification.
