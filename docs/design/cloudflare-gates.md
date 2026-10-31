# Cloudflare performance acceptance gate

This gate evaluates the split R2/native profile on declared workloads. It is not
an unconditional production SLO or a test of every embedding distribution.
The evaluator fails closed when evidence is missing. Local tests and smoke runs
cannot substitute for the live soak or recovery populations.

## Implementation being qualified

Publication has two durable boundaries. `beginPublication` acquires the native
HEAD/FENCE lease and protects the current source generation in one coordinator
transaction. It returns a bounded snapshot and lease identity. The native builder
uses that guard while uploading immutable artifacts; progress updates remain
operation-local. `finalizePublication` validates the original, still-live native
lease, mandatory HEAD/FENCE/floor/reindex dependencies and every written key.
It then atomically commits the proposed head, offsets/pins and native release.
It does not hold a metadata transaction open across the build. A concurrent WAL
append does not invalidate an immutable prefix build; the durable scheduler's
fresh watermark check admits subsequent publication work.

A failed or canceled build abandons tentative metadata and attempts native lease
release. A lost finalize acknowledgement can leave a completely committed head;
recovery and receipt checks must handle that outcome. A stale or expired owner
cannot finalize. The server validates read-set completeness and monotonic pins,
rather than trusting the staging adapter. Immutable orphan uploads remain subject
to existing fenced collection. No persisted root or manifest format changes are
introduced by this protocol. `BATCHED_PUBLICATION=false` selects the old path.
Deploy compatible native/host code on writer, reader, builder, then app.

Warm read admission observes a native committed prefix without joining the
writer's transaction queue. `immutable_log.observe` authenticates fresh checkpoint
and seal state but never acquires ownership, repairs markers, rotates epochs or
writes checkpoints. A frozen root supplies migration, HEAD, WAL, floor and pin
values. The observation is bounded to five seconds, below metadata collection's
grace period. Missing/expiring pins and cold admission return to the serialized
pin-establishing path. No authoritative writer cursor or mutable row cache is
updated by an observer. `READ_ONLY_SNAPSHOTS=false` disables this path.

A default two-millisecond read cohort can share an observation. It closes before
observation starts: callers arriving later receive another fresh frontier.
There is no TTL cache of live heads. Each caller receives an owned copy.
For ordinary reads without a pinned cursor, the protected view is prepared before
physical WASM admission. This allows independent metadata I/O to overlap instead
of occupying the two reader kernels. Its original thirty-second metadata/WAL
grace deadline includes queue time. Cursor-selected versions retain lazy admission;
short-lived publication work leases still open only after kernel admission.
`READ_VIEW_BATCH_MS` admits 0–10 ms. Final publication checks, native retirement
checks and visibility-token validation remain in force.

Maintenance yields at durable boundaries. Publication checkpoints an admitted
WAL prefix; its record budget adapts toward `PUBLICATION_SLICE_MS` (default
2,000 ms) within the existing byte/record limits. This is an adaptive target,
not preemption in the middle of an artifact write. Metadata groups execute one
maintenance callback before yielding. Kernel admission prioritizes foreground
work, rotates tenant identities and promotes work after five seconds. Active
bounded steps run to completion; queued lease time does not count as execution.

Fully paged serving trees have no flat snapshot to rebuild. Compaction protects
and validates those roots before writer acquisition and returns a genuine no-op.
It does not advance writer fencing. Legacy flat compaction retains explicit
document/artifact limits; it is outside this paged-profile latency qualification.
Use the existing checkpointed reindex path for supported legacy migrations.

The split app has no native slots and admits at most 32 HTTP requests, each with
the existing 1 MiB body limit. Streamed inputs share a 4 MiB byte budget that
remains charged while their parsed objects are dispatched; rejected/canceled
requests release their reservation. Reader, writer and builder budgets remain bounded
independently. Missing remote bindings fail closed in the split profile. The
compatibility app retains its previous admission limit. Increasing app admission
does not increase native pool sizes or actor input budgets.

## Fixed acceptance policy

`qualification/gate-policy.mjs` requires three distinct paired executions. Each
uses separate baseline/candidate deployments, four tenants and 512 documents with 1 KiB payloads
per tenant. Operation IDs are unique to the execution; rerunning a smoke test
cannot turn measured writes into receipt replays.

| Dimension | Required evidence |
| --- | --- |
| Warm read | p95 at most 1,000 ms; at least 1,000 samples per side |
| Durable write | p95 at most 3,000 ms; at least 300 samples per side |
| Regression | Candidate warm read/write p95 no worse than the paired baseline |
| Soak | At least one hour at the fixed offered workload |
| Recovery | 100 distinct fresh physical epochs per candidate; p95 at most 5,000 ms, no retries, correct count |
| Correctness | 2,052 exact count/body checks (every document) and eight seed/latest receipt replays per side; no failed requests |
| Publication | Every tenant sampled in every minute; lag at most 4,096 records and final convergence |
| Queue | At least 60 samples; backlog at most 128, oldest at most 15 minutes, zero dead-letter backlog |
| Stability | Last ten backlog samples no more than four above the p95 of minutes 15–24, with a floor of four |
| ANN | Both corpora below: equal/better recall with no additional mean physical GETs or bytes at 8/32/full effort |
| Failure tests | Content-addressed passing native, Node and persisted Worker test logs |
| Resource diagnostics | Eight continuously connected service streams, no observed memory/CPU failures |

Setup uses one tenant at a time and waits for publication after each 64-document
seed batch. Both sides use the same preparation policy and a single thirty-minute
setup deadline; reports record it as `setup_policy`. This bounds unpublished work
on the original control's single builder lane. It changes neither the 2,048 seed
documents nor the measured fixed-arrival workload, retry budgets or thresholds.
A failed preparation remains a failed report; never replay its measured corpus.

The fixed arrival interval is 30 seconds per deployment. Each arrival schedules
four writes, twelve fenced reads and, every fourth arrival, four compactions.
Each minute also samples all four publication frontiers and Queue/DLQ metrics.
Publication, convergence, Queue/DLQ and backlog-stability requirements apply to
both sides of the pair. A broken control cannot qualify a candidate.
After the soak and again after recovery, verification checks every document,
with bounded concurrency, as well as original/latest receipts.
Cron runs publication repair, existing metadata collection policy and scheduled
data GC. Thus the offered write load is 0.133 writes/s per deployment, plus reads,
maintenance and monitoring; this first gate is not a throughput claim for larger
rates. Baseline/candidate launch order alternates. Request latency starts at the
scheduled arrival and includes retries. Pending rounds are bounded; overload
fails the run instead of silently dropping offered traffic.

The fifteen-minute oldest-message allowance includes delayed GC continuations
and generation-pin retention. It does not replace publication-lag/convergence
checks. Real-time backlog and DLQ counts use the
[Cloudflare Queues metrics API](https://developers.cloudflare.com/changelog/post/2026-04-28-improved-queues-metrics/).

Recovery epochs change physical actor names while retaining the same logical
R2 namespace. Prior owners must be fenced. They do not prove complete platform
isolate eviction; shared compiled modules and bounded immutable caches may remain
warm. Report the result as fresh-actor recovery, with this limitation attached.

ANN uses 10,000 deterministic synthetic 384-dimensional vectors and 1,000 model
embeddings of generated topic documents, each with at least ten held-out queries.
The model fixture is frozen and hashed before either comparison. Generated model
texts are not a production corpus. The fixture accepts another supplied embedding
corpus for additional distribution qualification. Both runs use the same benchmark
code, deterministic test entropy/logical clock, 32 MiB builder and actual 16 MiB
reader modules, and the reader's 8 MiB working-set budget. Exact controls must
match independently computed brute-force cosine neighbors. No recall concession
or byte tolerance is introduced. Fixture clocks/entropy are confined to the local
benchmark process and provide no deployed latency or lease-expiry evidence.

## Reproduce and audit

Prepare a detached baseline worktree at the chosen reviewed commit. Build its
WASM or restore an independently hash-verified artifact; install its pinned Node
dependencies. From the candidate example directory:

```sh
node qualification/prepare-gate.mjs ACCOUNT_ID BASELINE_WORKTREE 3
node qualification/gate-lifecycle.mjs GATE_JSON seal
node qualification/gate-lifecycle.mjs GATE_JSON deploy
node qualification/gate-lifecycle.mjs GATE_JSON upload-token
node qualification/run-gate.mjs GATE_JSON 1 --seconds=120
```

`seal` copies each service into an immutable bundle directory and hashes its JS
and WASM. Dependency deployments are ordered writer/reader/builder/app; independent
profiles can run in parallel. The lifecycle checks account, bucket, Worker and
local token destinations before mutation. It installs only the locally generated
disposable tokens, never account credentials. Assistant-driven credential uploads
and permanent deletion need the explicit authorization required by approval review.

Smoke reports are named separately and do not qualify the gate. After inspecting
smoke latency and fixing the bottleneck, run trials 1, 2 and 3 with the default
3,600-second duration, independently or concurrently on their separate pairs.
The runner resumes paused profiles before seeding. Final candidate recovery
pauses Queue delivery and cron, waits for bounded work, and selects fresh actors.
Recovery actor names are single-use per pair. Before selecting the first recovery
epoch, the runner journals `cold_execution_id`. Further trials, capacity runs and
`resume` reject that pair: reusing its actors would lose freshness and could wake
fenced old owners. Keep the candidate paused for audit/cleanup after recovery.
A warm smoke run can precede its full trial; `--cold` consumes the pair.
Reports pin every data response to the expected app version; service bundle hashes
and deployment versions remain in the run manifests. Sealed bundle bytes are
rehashed before deployment and client startup, and evaluation checks report
identity, full/reader module hashes and all service bundle hashes against the
sealed manifests. Every execution is archived. The evaluator checks the actual offered window,
arrival period and round count, along with finite latency/attempt measurements.
Reports include scheduled, started and finished timestamps so client scheduling
delay can be distinguished from request execution. Overlapping HTTP write replies
are reconciled by durable LSN, independently for each document and tenant fence;
reply order cannot overwrite a newer acknowledged expectation.

Use `model-fixture.mjs RUN_JSON OUTPUT` to capture the bounded model corpus through
the configured test app's AI Gateway route. Alternatively, append
`--account-api` to use the existing local Cloudflare account session with the
official account inference API and `cf-aig-gateway-id: default`. This mode
installs no Worker secret and records `served_version: null`; it is provider
corpus evidence, not evidence that the deployed app path works. Credentials
remain local and are sent only as authentication to `api.cloudflare.com`.
Refresh an expired existing session with `wrangler whoami` before capture. The
[account API documentation](https://developers.cloudflare.com/ai-gateway/usage/rest-api/#call-a-workers-ai-model)
describes this route. Run `benchmark.mjs --deterministic`
with each profile's `--wasm` and `--reader-wasm` paths, once at 10,000 synthetic
documents and once at 1,000 with `--embedding-fixture=OUTPUT`. Store reports as
`ann-synthetic-{baseline,candidate}.json` and `ann-real-{baseline,candidate}.json`
in the gate directory, and the model corpus as `model-embeddings.json`.

```sh
node qualification/gate-evidence.mjs GATE_JSON NATIVE_LOG NODE_LOG WORKER_LOG
node qualification/evaluate-gate.mjs GATE_JSON
```

ANN evidence also requires full corpus acceptance/publication and passing cold,
warm, update, deletion and compaction stages. Retaining successful recall samples
from an otherwise failed benchmark cannot qualify it. Benchmark processes return
a failing exit status when a stage fails.

The evidence manifest binds test logs and both ANN populations to hashes and the
candidate/baseline native modules. Evaluation rechecks artifact hashes. Missing
hour-long trials, missing recovery samples, mixed versions, memory errors returned
to requests, missing fault/ANN evidence or a failed threshold leave the gate red;
the evaluator exits with status 2. Synthetic unit tests of the evaluator are not
performance evidence.

After qualification, archive scoped platform/R2 analytics before deleting resources.
These samples are not absolute memory peaks or finalized billing. Approved
`gate-lifecycle.mjs GATE_JSON cleanup` detaches the app's actor bindings, stops
private actors and Queue consumption, waits for quiescence, drains through the
R2-only qualification endpoint, then deletes the app, Queues and bucket. Steps
are journaled for retry; local reports remain and disposable token files are
removed. `pause` disables cron/delivery; `resume` restores them for another run.


## Separate capacity exploration

`run-capacity.mjs` reuses the same bounded fixed-arrival driver without changing
the acceptance workload. Use a fresh resumed disposable pair after qualification,
or explore the 512-document workload before that pair's recovery phase:

```sh
node qualification/run-capacity.mjs GATE_JSON 1 --seconds=600 --documents=512 --periods=30000,15000,7500,3750
```

Each period schedules the same four writes and twelve reads per arrival, plus
maintenance and minute-level frontier/Queue monitoring. The offered rates rise
from 0.133 to 1.067 writes/s and 0.4 to 3.2 reads/s per deployment. Each stage
converges and checks every document and receipts before proceeding. It stops on
candidate latency/backlog limits, resource failures or client overload. Short
stages are exploratory: their small sample populations do not establish an SLO.
Results are saved separately with `qualified: false` and cannot replace a gate
trial. Per-phase and per-tenant summaries include p50/p95/p99, scheduling delay,
request execution time and retries.

Increase corpus size in a separate run, for example `--documents=4096` followed
by `--documents=16384`, while holding the arrival schedule fixed. This separates
corpus growth from offered-load growth. Up to 100,000 documents per tenant are
accepted by the harness; that is an input limit, not a demonstrated capacity.
Use a fresh pair to reduce corpus size or after recovery has consumed its
physical actors. The seed deliberately does not remove existing test documents. The fixed gate must still use its original
512-document corpus and untouched acceptance thresholds.

A capacity boundary is a measured result. Inspect foreground queue wait,
metadata RPCs, R2 wait/bytes and native CPU before changing concurrency, prefix
budgets or cache policy. Repeat both profiles after a fix. Do not trade away
fencing, durable acknowledgements, recall or memory bounds to move the boundary.


## Bounded exhaustive vector search

The split R2 app routes vector `search_effort: 1` through the native exact-scan
kernel when `PAGED_PROJECTIONS=true`. Approximate searches keep their existing
route selection. The scan consumes the FP32 exact-vector tree directly, rather
than treating FP16 cluster payloads as the precision oracle. A near-tie regression
checks a case in which FP16 rounding collapses distinct vectors.

Each native step reopens the same immutable generation, validates its retirement
floor and acquires its normal read protection. State binds namespace, vector,
limit, selected index checksum, generation and published visibility cutoff. An
exclusive 36-byte tree key plus a decimal visited-record count defines progress.
Completion requires covering the root's full record count. Later publication
cannot rebase a resumed query. `fresh` is checked on initial capture; resuming
continues that captured view. Required visibility still must fit the captured
published prefix, and pending embeddings fail the existing readiness checks.

The native top-k is an owned worst-first heap with a bounded identity map.
Multiple projections of one document retain its best exact distance across all
steps. Both heap and map have at most `k` entries (`1..100`), and retained IDs
have an additional byte bound. Ranking preserves the native positive-similarity predicate and exact-distance
tie breaking. Updates cost O(log k), with O(1) expected identity lookup. Scanning remains O(N*d), and final body materialization occurs
only after full coverage. Final materialized bodies are capped at 1 MiB.

The shared native page engine packs newly written vector leaves and coalesces
requested packed ranges. Exact scanning uses rank-based resume, bounded forward
read-ahead and the existing authenticated cache. The [implementation design](cloudflare-performance.md#shared-native-exact-scan-io)
records the packing fallback, ownership and compatibility rules. Logical page
admission still counts each requested page, even when one physical GET serves
several pages; all physical reads and bytes count in qualification evidence.

A step admits at most 512 records, further reduced by dimensions toward 1 MiB of
raw vectors. It has a 4 MiB root/page-read allowance, at most 128 page reads and
a cooperative one-second scan-loop elapsed target. Budget exhaustion after progress yields
an exclusive continuation; failure before progress remains an explicit error.
The usual 8 MiB reader working set, 32 MiB transport limit and 1,024-operation
limit remain. The runtime also bounds each native step to five seconds. There is
no cumulative budget reset inside a native request.

The app orchestrates distinct reader RPCs, releasing actor/kernel admission after
each step. It defaults to at most four inline steps and a one-second inline
window, tested between steps. Otherwise the response is explicitly incomplete:

```json
{"complete":false,"hits":[],"cursor":"opaque authenticated continuation"}
```

Clients resubmit the same query with `cursor` until `complete: true`. Partial
candidate heaps are never presented as hits. The cursor carries bounded native
state, protected by HMAC-SHA-256 over the protocol, tenant, public query identity,
expiry and state. Verification precedes any read RPC. Private `exact_scan`,
`exact_state` and `exact_records` input fields are rejected by the public app.
The native scan ABI is a trusted host interface; it does not authenticate callers.

`EXACT_SCAN_CURSOR_KEY` can supply a dedicated app-only signing secret. Otherwise
the existing operator `API_TOKEN` signs a protocol-scoped payload. Secret rotation
invalidates old cursors. Payloads are capped at 128 KiB; tokens authenticate but
do not encrypt their state. Store them as sensitive query capabilities. No
account credential is placed in a cursor or sent to a private reader.

The idle validity window is four minutes, renewed only by a successful progress
step, with a fixed one-hour absolute lifetime. Every step obtains fresh generation
protection; the idle window stays below the normal minimum read-pin lifetime.
Expiry, retirement and cancellation fail closed. Duplicate resumes are safe,
recomputing the same immutable range. There are no per-query durable job records,
leases or cleanup queues; existing shared read pins expire under normal collection
policy. Cancellation does not remove another reader's shared pin.

App knobs `EXACT_SCAN_INLINE_STEPS` (1..8, default 4), `EXACT_SCAN_INLINE_MS`
(0..2,000, default 1,000) and `EXACT_SCAN_RECORDS` (1..512, default 512) can lower
or bound scheduling slices. They do not increase native resource maxima.

Deploy compatible private reader/native code before enabling the new app code.
The trusted native ABI adds optional scan fields; an old reader must fail closed
rather than return approximate or partial results as exact answers.

Use `benchmark.mjs --exact-continuations --reader-wasm=...` to exercise the new
path. It records every native request's resource peaks and aggregates all scan
steps in each recall query's physical GET/byte counts. FP32 exhaustive I/O must
be reported separately from the old FP16 full-effort control; existing no-extra-
I/O gate thresholds are not silently relaxed for the precision change. Local
completion proves neither live latency nor the overall Cloudflare gate.


The [shared vector-block design](cloudflare-vector-blocks.md) specifies format-3
serving runs, residual repacks, authenticated deferred refinement and optional
revision-bound original-source reuse. These storage changes do not alter this
acceptance policy. ANN evidence now rejects unequal workload settings and query
populations before comparing physical traffic.


### Preserve cold actors until warm prerequisites pass

The runner validates the completed hour, every warm request, exact body/receipt
verification, publication/Queue coverage and locally hashed fault/ANN evidence
before marking the reserved physical epochs consumed. A failure stops before
any cold call and leaves those actors unused. This preflight uses the same
assessment as the final evaluator with only cold coverage/latency deferred; it
cannot produce a final gate pass. Short diagnostics cannot consume the reserved
actors. The final evaluator always requires all 100 distinct epochs, one HTTP
attempt each, and the original five-second cold p95.

Sealed profiles also record the optional runtime generation and location hint.
Every role must use the same policy; changed configuration or a report from
another placement profile fails provenance verification. The frozen control
keeps its existing policy. Compute generations do not reset physical cold-use
records or database receipts.


The paired client also retries recognized network timeouts/resets on warm reads
and mutations carrying a stable operation ID. It reuses the identical request
body and preserves the original scheduled/start times, 60-second total budget
and 20-attempt cap. These retries apply identically to both sides; elapsed time
and attempt count include them. Mutations without an operation ID are not
replayed after an ambiguous transport result. Existing resource-error observations
remain disqualifying even if a later attempt succeeds. Cold calls now stop after
the first transport or HTTP failure, rather than issuing a retry that the final
one-attempt policy would reject anyway. Prior failed reports are not rewritten.


The Cloudflare native-owner identity is scoped to the provider's physical actor,
deployment version and storage namespace. Actor reconstruction within one
deployment can resume that owner under Durable Object serialization. Deployment
or physical-actor changes retain native fencing; hosts without validated provider
metadata use random owners. Read-only log recovery starts with one slot and
adapts only after validated contiguous progress, retaining fresh seal probes and
all native recovery budgets. Neither optimization changes acceptance thresholds.


Optional edge immutable caching keeps R2 as the authority. Its 64 KiB per-object
limit and existing immutable-name allowlist exclude mutable heads/floors and
negative results. Every native fencing and retirement check still applies after
a cache hit. R2 physical traffic is counted below the cache; cache access has
separate metrics. A cache-enabled profile must pass the same warm and cold
criteria, including recovery after loss of volatile state and exact verification.

Cache admission permits at most 32 Cache API calls per operation and 960 combined
instrumented R2/cache calls. Two background fills and 512 KiB of charged response
data are allowed per isolate; excess fills are skipped without queuing. These
limits preserve the existing acceptance thresholds and native request budgets.

Full immutable cache entries may satisfy a conditional range read only after
validating their digest and requested ETag. Cache-enabled profiles pin a UUID
cache scope for the physical R2 bucket incarnation across all four roles, in
addition to the native logical namespace and deployment profile.


The optional direct app reader still obtains a fresh native coordinator read
view and uses the existing compact query module and 8 MiB working budget. Its
32 MiB app kernel limit, selection and coordinator cohort setting are recorded
in the sealed profile. The reserved physical epochs exercise coordinator
recovery; complete app Worker isolate eviction remains an explicit limitation.
The local restart fixture separately verifies recovery with platform storage
erased and the optional edge cache disabled.

Queued callers may share a read cohort only until native kernel admission (and
always before transport observation). Completed snapshots remain request-bound.
Read and mutation-metadata caller allowances are separate and bounded; native
fencing, visibility, deadlines and acceptance limits are unchanged.

## Retrying with fresh paired corpora

Repeated diagnostics overwrite the same documents but append new WAL records and
publication history. They are useful durability/fragmentation stress, but their
accumulated setup is not a fresh 512-document qualification corpus. Preserve
those reports separately, including setup timeouts and failed convergence.

After stopping the previous execution and pausing both sides' Queue delivery and
cron, run `node qualification/prepare-corpus.mjs GATE_JSON TRIAL COMPLETED_REPORT`.
It requires a closed latest execution and unused cold markers, validates both
sealed deployments, archives the report/configuration/manifests and prepares one
new shared corpus identity. It changes only the default table and four tenants' logical table
incarnations and metadata storage identities. Existing object data stays in R2.
Old-incarnation Queue messages use the existing incarnation mismatch handling;
no queue or data is purged by this command.

Deploy the existing sealed bundles on both sides, resume delivery, then run the
normal fresh seed and one-hour qualification. Do not reseal the frozen control
or change resource limits, algorithms, ANN fixtures or thresholds. New preparation
records the corpus routes in each manifest/report; all roles must agree, paired
corpus IDs must match, and the three trials must have distinct corpus IDs.
Setup replay rejects a changed corpus. A consumed recovery marker still requires
a new deployment pair; changing a table incarnation never clears it.


## Read-capsule candidate rollout

For the format-three candidate described in
[performance design](cloudflare-performance.md#native-read-capsules), seal with
`INLINE_METADATA_ROOTS=false`, deploy every compatible role, then run
`node qualification/gate-lifecycle.mjs GATE_JSON enable-inline-metadata -c`.
The lifecycle probes the three sealed native variants and activates readers
before writers. The frozen corrected control remains unchanged. Profile hashes,
format activation and deployment versions must precede fresh qualification.

Store completed logs and benchmark reports under the worktree's ignored
`dist/qualification/` directory before hashing them into evidence. Temporary
system-directory logs may disappear between sessions; a missing artifact
invalidates its evidence manifest. Preserve such manifests as historical failed
evidence and regenerate current evidence from available, verified artifacts.


Each new runner also captures eight sanitized live diagnostic streams. Missing
roles, connection gaps and memory/CPU failures reject warm preflight and final
qualification even when an HTTP retry succeeds. Completed journals are hashed
locally; session URLs and credentials are never saved. The [performance
design](cloudflare-performance.md#resource-failures-after-a-deadline) describes
limits and late-settlement coverage. Historical reports without these diagnostics
remain preserved and cannot qualify under the updated evaluator.


The common client recognizes the exact `ManifestReadLeaseExpired` HTTP 409 as
recoverable only for stable, idempotent warm requests. Re-entry acquires fresh
read admission while retaining the identical visibility fence, request body and
operation identity. The original sixty-second/twenty-attempt limits and elapsed
latency measurement remain unchanged. Cold requests, unsafe mutations,
compaction and resource-bearing or unknown messages do not gain this retry.
This corrects the client classification without changing the frozen control.
