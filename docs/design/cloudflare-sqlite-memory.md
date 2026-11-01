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

### Receiver publication and diagnostic-only setup replay

The second generation (`e70dc832-870c-46a2-aeeb-be9ce175c335`, rehearsal
`8680ef80-6f4e-4980-8d71-a3a0a0fa3f94`) retained 21 receiver
`TelemetryDiagnosticAdmissionExceeded` rejections in its final health sample.
All four candidate seed corpora completed, but setup had not completed on the
control and no measured workload ran. A separately saved settled-minute
preflight had native Tail records but no matching canonical trace population.
It correctly failed rather than inferring coverage from received logs.
Deployed settings confirm logs/traces enabled at sampling rate one; the owned
export destinations report delivery errors. Neither incomplete generation
establishes absence of allocator faults.

The receiver now prepares and persists separately as well. It coalesces
sanitized rows by source across decoded resource fragments, with at most 1,000
rows / four MiB per archive and a global four-MiB sanitized buffering charge.
Prepared encoded bytes fit the same 16-MiB write budget used by the Tail
pipeline. Up to four source batches persist concurrently. Encoded payload
references are cleared before their reservation is released, including error
paths; all peer physical writes have already settled. Waiting HTTP bodies stay
unread behind one preparation lane, with at most 64 small tickets and the
existing 15-second admission wait. The additional waiting tickets do not admit
additional decoded JSON trees. The read-only reader retains four waiting slots.

A 1,000-record/eight-source fixture produces eight coalesced archive batches
instead of one batch per source per decode fragment. Actual-workerd tests send
32 concurrent compressed exports, verify all 8,192 archived records and the
prepared-byte ceiling, and check the dense 6,000-record case. Rejection sampling
remains bounded on health; a new authenticated read-only `/rejections` paginator
can inspect later scoped errors without reading archives or other generations.
The destination-status lifecycle action returns only owned destination status;
optional diagnostic excerpts redact the disposable token and bearer values.

`run-paired-rehearsal --resume-setup=<completed-report>` is explicitly limited to
failed diagnostic setup with no measured workload, no client resource failure,
no used cold epochs, unchanged code/module/bundle/corpus identity and a matching
closed execution claim. It replays every seed mutation with the original stable
operation IDs; it does not append a second seed prefix. The new diagnostic has
its own execution/attempt ledger and before/after witnesses, carries the prior
report hash and seed execution ID, and still performs full body/receipt
verification after its measured workload. Full gate trials and measured runs
are not resumed by this facility. The original failed reports remain failed.

Qualification additionally reads the two owned Cloudflare destination job statuses.
Both signals must have a successful delivery after the measured interval, and
any delivery error during capture or settlement remains disqualifying after a
later success. Missing, stale, disabled or mismatched destination status fails
closed. This retains observed provider delivery failures that may occur before
JavaScript persists a rejection marker. It is additional evidence, not an
allocator root-cause claim.


### Preserve provider delivery history across recovery

The third reproduction observed Cloudflare clearing `jobStatus.last_error`
after recovery. A final destination snapshot therefore cannot establish that
no earlier delivery error occurred. The driver now starts a serial status
monitor before its boundary witnesses, persists each bounded observation in
its report, and joins one final query after settlement. Errors observed during
the capture remain disqualifying even if the last status clears them. Missing
queries, changed scope/configuration, a monitoring gap over 75 seconds, a late
start, an early stop or an exhausted 256-observation budget fail closed.
The history and the final successful delivery status are both required;
reconciliation cannot promote an old report that did not collect this history.

Polling still cannot exclude failures cleared entirely between observations.
The exhaustive source terminal population, fixed boundary resource ledgers and
all capture/settlement checks remain required. No provider API percentile or
last-success timestamp alone proves absence of allocator failures. Tests cover
an error followed by an empty `last_error`, unavailable observations, changed
identity, monitoring gaps and serial request settlement. All 491 Node tests pass;
the previously passing eight receiver/Tail workerd integrations and guarded
native R2 integration use unchanged Worker code for this driver-only increment.


### Hosted workload result and remaining admission/capture costs

A fresh corpus (`84f4b595-addc-4afa-8766-77a3ed3aed48`) and observation capture
(`1a900bc3-2a43-4348-89e1-6ad4759fe3f6`) ran rehearsal
`81304f14-db04-4579-a52f-1c557e8106d3` on driver `e523695c56`. Both sides
completed the unchanged six-round workload and verified 2,052 exact bodies and
eight receipt replays. Final client errors and client resource-error ledgers
are empty. Corrected control `c44896ac8c` and the candidate's sealed application
bundles/WASM remain unchanged throughout this reproduction.

| Measured p95 | Control | Candidate | Original short-run threshold |
| --- | ---: | ---: | ---: |
| Read, 72 logical calls/side | 9,128 ms | 1,508 ms | 1,000 ms |
| Write, 24 logical calls/side | 10,257 ms | 2,087 ms | 3,000 ms |

Timings include scheduling and transport retries. Candidate has one retried
read; control has two retried reads and two retried writes. Candidate write
latency passes the short-run threshold; read latency fails. These results do
not establish the stronger fourfold targets or provider-placement equivalence.

An independent closed measured minute (`18:40:00`–`18:40:59.999` UTC) reconciles
all eight sources, 354 canonical/native terminal invocations, both app physical
attempt ledgers and real before/after witnesses for quiet sources. It passes
both separated observations with zero resource failures. The saved result is
`minute-probe-e1063207-8deb-4658-9038-14cfbafbd465/result.json` under the observer
directory. The earlier third-generation setup minute also passes its six active
roles. These are explicitly minute-scoped checks, not whole-run qualification.

Whole-run collection exceeded the intended ten-minute capture budget during
its first observation pass. The pre-fix loop only checked the deadline between
full eight-source passes and did not propagate cancellation into its current
source traversal. The collector was stopped; its original partial report stays
unchanged, with a separate `capture-interruption.json` recording the report hash
and forced termination. It remains incomplete and cannot pass the gate.
The native SQL/KV-forbidden regression remains passing, but the historical
provider `SQLITE_NOMEM` is still unreproduced and not root-caused.

The Node capture client now propagates the original aggregate deadline and
manual cancellation through active requests, response reads and source/window
pagination. A cancelled capture records a diagnostic failure and closes its
report instead of traversing the remaining sources. UTC-aligned logical cuts
also fetch each physical minute once instead of routinely reading it in two
adjacent elapsed-minute slices. Catalogs explicitly identify their partition
scheme; old elapsed-minute catalogs remain readable. Inclusive one-millisecond
edge slices retain exact counts, identities and real boundary witnesses. The
original byte/object/terminal, per-window and aggregate time limits remain.
A three-minute fixture reduces physical minute scans from five to three per
observation, while proving the same entire app ledger.

Read profiling uses sanitized Tail archives, not the compact proof rows that
intentionally omit benign timings. In one measured minute, 24 candidate lookups
have native lookup I/O p95 512 ms, while several slow requests spend about
1.0–1.25 seconds in read admission. These measurements overlap and must not be
added as independent percentile components. They identify admission and
preparation reuse as the next read-latency target; cutting only document-body
I/O is insufficient. Candidate direct reads execute in the app-reader runtime,
so an empty private-reader population must not be mistaken for missing app
reads. Further admission experiments should preserve the current authority,
lease-expiry, caller ownership and collection-fence semantics.

The replay-only attempt before this fresh run stalled at control tenant A /
256 published documents. Fresh setup subsequently progressed past that point
and completed. Stable seed operation IDs guarantee replay identity, not liveness
of an older control after interrupted publication; the replay is retained as a
failed reproduction, with no claim that its cause is established.


### Next twofold target: bound metadata recovery on the read path

The current targets are read p95 at most 754 ms and write p95 at most 1,044 ms,
half of the latest candidate measurements. They are objectives, not measured
improvements. Further archival profiling identifies concrete dependency chains:
one slow admission performs ten GETs in ten I/O waves in 1,233 ms; another
performs nine GETs in nine waves in 1,202 ms. Both have zero kernel queue delay.
Their native document lookup stages are 331 ms and 384 ms, respectively.
Existing fenced leases, admission cohorts, fused metadata reads and warm
floor/slot batching are already implemented; merely enabling those features
again does not address this observed recovery path.

First measure checkpoint lag and recovery cause for every admission: initial
observation, expired certificate, owner/epoch change, checkpoint advancement,
lease miss and admitted fallback. Audit the existing idle-checkpoint lifecycle
and its busy/retry/cancellation outcomes before adding a new persisted format.
A checkpoint should be scheduled according to reader replay cost as well as
commit cadence, with coalesced background work and a recoverable dirty revision.
Foreground durability must not depend on successful publication of that hint.

If reliable checkpoint publication still permits long tails under active
maintenance or high commit rates, extend the shared native metadata journal
with an authenticated serving checkpoint and/or seekable immutable recovery
index. The checkpoint includes the accepted root, owner/epoch, serving LSN and
GC fence. It is a starting point for fresh validation, never authority supplied
by an untrusted cache. Resolve the remaining independent floor/next-slot checks
in a bounded native I/O wave; avoid replaying nine or ten entries serially just
to admit an ordinary point read. Keep old journal recovery as the strict fallback
for changed owners, stale hints and concurrent advancement. This belongs in the
shared object-store engine, preserving R2/S3 typed ETags and GCS generations.

For writes, retain the existing small immutable WAL commits and deferred index
folding. Capture writer-authority, refresh, preparation, admission wait and
finalization durations separately before attributing its p95. Target duplicate
proof/recovery work, retain prepared immutable uploads across a rebase, and
overlap independent proof/data reads where the native dependency graph permits.
The durable commit remains the acknowledgement boundary. A twofold write gain
is still a hypothesis pending that phase attribution.

Use local actual-workerd/native-module comparisons with metadata lag of 1/8/32
entries, 31-second idle gaps, 50/100/200 ms binding delays, concurrent
read/write/maintenance, cache loss and namespace churn. Reverse candidate/control
order with identical keys, modules, corpus and resource limits. Report first-use
and warm p95 separately, plus metadata GETs and dependent I/O waves. Qualify the
winner on the original offered workload before consuming hour-long/cold trials.
Measure compute-to-storage placement independently: hints are optimization
requests, not evidence that the services are colocated. Preserve lease lifetimes,
ownership checks, request-owned I/O, memory caps, exact body/receipt verification
and all original gates throughout these experiments.

Cleanup of the explicitly approved disposable deployment is complete. Independent
before/after inventory verifies removal of all 11 Workers, six Durable Object
namespaces, four Queues/DLQs and four R2 buckets; no other `antfly-1278-*` test
resources remain in that account. The observer drain encountered one transport
reset, was resumed idempotently, and completed. Reports and sanitized selected
archives remain local. All 494 final Node tests pass; the unchanged Worker
pipeline retains its eight actual-workerd passes and SQL/KV-forbidden native R2
integration pass. The four recent evidence closures reverify all 349 artifacts
with no missing files or changed hashes. The older 163-artifact availability gap
remains historical and is not relabeled as recovered by this audit.

The hosted investigation is sealed at
`examples/cloudflare-serverless/dist/qualification/diagnostic-memory-hosted-1793388433394/evidence.json`:
1,069 verified artifacts / 91,038,670 bytes, source `e4d0be5d7d`, manifest SHA-256
`8c4dd3573fe315aa598eeaee719bd83247b91c7dbf988d8a3828042f50122444`.
It retains all four attempts, the explicit collector interruption, selected
sanitized archives, both passing minute checks, frozen deployed bundles and
source revisions, test logs and independent cleanup inventory. An earlier seal
had an incorrect top-level source label due to a script variable being reused;
it remains preserved as invalid and is explicitly superseded by this seal.

The implemented recovery/preparation changes and full local comparisons are in
[Bounded metadata recovery and reusable immutable preparation](serverless-metadata-recovery.md).
It adds shared native bounded journal catch-up, direct advanced-checkpoint
rebasing with the original proof anchor, bounded checkpoint repair, metadata
observation affinity and write-plan reuse after authority refresh. The corrected
final metadata-only matrix verifies all 108 candidate observations and measures
2.7–2.8× faster 32-entry cold/expired recovery at 100 ms binding delay. Native
coverage protects warm tails through seven commits from speculative reads.
All 499 Node tests and the three guarded workerd recovery/write-reuse checks
pass against the final modules; the shared native suite passes 188 tests with
two live-provider cases skipped.

All 18 final app cases pass correctness checks, but do not establish another
overall twofold gain. Warm 64-document cases are essentially unchanged; large
512-document cases vary substantially between orders. The apparent idle app
benefit reverses with order. Affinity and extra write read-ahead remain off in
the proposed hosted profile. Larger slow reads still wait in the shared native
pool despite having no warm prefetch; resumable native metadata I/O is a
possible next architectural step, with a pre-I/O authenticated clock anchor.
It is not implemented or claimed as a measured gain here.

The revised disposable hosted pair was approved, deployed and measured; its
original undeployed proposal remains preserved. The 180-second rehearsal passes
exact-body/receipt checks on both sides, but candidate read p95 is 1,922 ms
against the unchanged 1,000 ms target; write p95 is 1,507 ms against 3,000 ms.
The independent collector exhausted its original ten-minute aggregate deadline
after six first-pass source catalogs, before a complete second observation.
Two missing sources were preserved afterward as explicitly non-qualifying
postmortem captures; they do not repair that original failure. Client resource
errors and captured observer resource failures are empty, but coverage remains
incomplete. This does not demonstrate that `SQLITE_NOMEM` is fixed.
The original gate remains unpassed. Current coverage requirements are in
[independent HTTP qualification](cloudflare-independent-http.md).

## Bounded delivery, complete replay and startup readiness

The next collector advertises window inventories and ETag-pinned grouped object
reads. Four request-owned calls pipeline groups under the original 32-object /
4 MiB response, 8 KiB request, per-window byte/object and aggregate time caps.
Small inventories include their objects directly. Independent second observations
re-list and re-read storage; there is no cross-observation cache. One reader page
reservation survives serialization until HTTP EOF or cancellation, preventing
four outstanding callers from retaining four decoded evidence pages. All started
calls settle on errors, including recovered resource failures.

On the same frozen 28-minute source (5,178 verified objects), modeled 50 ms HTTP /
20 ms storage delay falls from about 20 seconds to 10–11 seconds in both orders.
Physical GET counts remain equal; metadata LISTs fall 207 → 60. This is local
retrieval throughput, not live export completeness or allocator qualification.

Actual-workerd replay of all eight historical source catalogs now completes two
observations in 301,132 ms, within the original 600,000 ms budget. **It still
fails six sources' reconciliation.** Missing canonical terminals concentrate in
the first 0.8–16 seconds after the initial witness, including six candidate HTTP
200 acknowledgements present in native evidence but absent from canonical
export. Two control-reader extra terminals occur near the final boundary and
have approximately 30-second RPC wall times. Startup delivery and RPC lifetime
are hypotheses, not established causes. Original reports and all gaps remain
unchanged; late historical source preservation is never qualification evidence.

A separately generated, explicitly synthetic known-complete fixture exercises
all eight sources over 28 minutes, 16,384 terminals and 32,808 R2 objects. Both
independent observations reconcile all eight sources in 463,780 ms (7m44s), with
four client calls outstanding, one decoded reader page and zero active calls at
completion. This proves bounded collection of complete input fits the existing
deadline in this local model. It does not demonstrate complete hosted delivery.

The driver now requires a preflight before database setup: each of eight sources
must provide a positive intermediate witness terminal in both independent native
and canonical observations, with the original versions and boundary witnesses.
Empty reconciled sources are insufficient. Closure remains 60 seconds and
observations remain separated by 30 seconds. At most two fresh, five-minute
preflight attempts are allowed; every failed attempt is retained. Resource
failures terminate preflight immediately and remain in the whole-run resource
ledger. Qualification workload, original ten-minute settlement, cold epochs and
latency thresholds are unchanged. Preflight is explicitly outside qualification
and does not claim physical cold startup performance.

Data-only RPC results now materialize caller-owned bytes and explicitly dispose
both the pipelined promise and result group on success or failure. The workerd
test verifies real RPC object disposal and invalidation of unexpected returned
capabilities. It does not establish a bound on remote destructor scheduling or
prove that this fixes the historical long RPC tails or `SQLITE_NOMEM`.
[Cloudflare's RPC lifecycle documentation](https://developers.cloudflare.com/workers/runtime-apis/rpc/lifecycle/)
recommends explicit disposal and describes asynchronous remote cleanup.

All 515 Node tests pass, including preflight gaps/retry preservation, positive
both-plane evidence, cancellation and resource-failure rejection. The hosted
memory error remains unresolved until a new complete live capture covers the
original workload without recovered resource failures. No cloud resources were
created for these local experiments.


RPC result ownership also covers maintenance admission/completion, metadata
collection/migration, mail session/directory/message/attachment/outbox DTOs and
both auth-service forwarding layers. HTTP fetch and streamed responses keep their
existing ownership contract. A regression checks the actual pipelined RPC promise
and result together, without a manual disposal step hiding a missing release.

Window reconciliation now reports an unexpected HTTP invocation only for an
actual canonical attempt absent from the ledger. Missing ledger attempts still
fail with `ObserverPhysicalAttemptMissing`, including unacknowledged transports;
no reconciliation acceptance criterion is relaxed. Tests assert both directions.
The original historical report remains unchanged, including its older labels.

Final validation passes 515 Node tests and 24 actual-workerd tests: one RPC
ownership test, one advisory-cursor test, five observer tests, all sixteen native
R2 tests and the mail integration. The mail run exercises auth, mailbox isolation,
native search, attachments and restart without the prior RPC-disposal warning.
The expected invalid-password warning remains part of that test. The native R2
suite also exercises a full 60-second collector grace period. The shared native
object-store suite remains 192 passed / two live-provider skips, with 38 focused
native checks passing. Guarded cache GC was attempted when disk space became low
and skipped because a native build was active or process inspection unavailable;
the guard was not bypassed. Final modules, native test binary, logs and control
source had already been frozen outside build caches.


### Detached observation lifetime verification

The warm native continuation experiment releases its WASM slot while the exact
journal GET is blocked. An actual-workerd regression admits another kernel job
at zero reserved reader bytes, times out the first request after five seconds,
and checks that its auxiliary permit remains reserved until physical I/O settles.
An injected late `SQLITE_NOMEM` remains visible with matching lifecycle identity;
it is not relabeled absence or a successful read. Writer application SQL/KV
access remains forbidden in this fixture. This tests error retention and lifetime
accounting, not the cause or resolution of the historical hosted memory error.

Bounded completed immutable-header retention keeps all original lease/condition
checks and cache ceilings. A split-worker publication/restart test verifies real
admission, then an older adapter without `admit` verifies compatibility. Both
new switches remain off by default. Provenance now seals those switches and the
previous advisory-hint switch, closing an omission without relaxing any gate.
The local performance ranges and their regressions are recorded in
[the native design](serverless-metadata-recovery.md). No cloud resources were
created, and the hosted gate and recovered memory error remain unqualified.


### Detached immutable point lifetime verification

The point-dependency continuation fixture now performs the same guarded workerd
lifetime exercise on an immutable artifact GET. The reader has zero reserved
native workspace while the GET is blocked and another job can complete. Its
30-second logical timeout retains the point permit and caller background lifetime
until physical settlement. An injected late `SQLITE_NOMEM` is retained with its
matching lifecycle identity. Writer SQL/KV access remains forbidden. An older
WASM module lacks the capability export and retains ordinary serial I/O.

Final validation passes 537 Node tests, 54 workerd checks (53 standard plus mail),
198 shared object-store checks with two live-provider skips, and 32 focused
native checks. All 49 final local performance cases drain background work before
collecting counters and pass with no recovered resource failures. This confirms
accounting/error propagation for tested paths; it does not reproduce, identify
or fix the historical hosted allocator failure. No new cloud resources were
created. The hosted gate and historical memory error remain unresolved.

## Resident serving does not resolve the historical error

The [resident increment](resident-serving-shards.md) passes actual-workerd
publication, both collectors, concurrent mutation, expiry and restart while
application SQL/KV getters throw. Authoritative data remains native Antfly in
object storage. Platform Durable Object infrastructure can still use its own
storage implementation; avoiding application SQL is not a proof about the
historical provider allocator failure. The shared detached-read scheduler retains
charged buffers/permits through timeout and reports an injected late
`SQLITE_NOMEM`; these are lifetime regression tests, not a reproduced root cause
or resolution of the original live error. No new hosted qualification ran.
