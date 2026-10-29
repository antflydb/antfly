# Native idle-client qualification

The manifest fixes counts and acceptance thresholds before the candidate runs.
Run against a built native binary:

```sh
python3 zig/qualification/workload-pooling/run_local.py \
  --binary /path/to/antfly \
  --manifest zig/qualification/workload-pooling/macos-small.json \
  --output /tmp/pooling-idle-receipt.json
```

The launcher creates isolated temporary storage and a random authenticated admin,
starts native HTTP and pgwire listeners, measures the process, closes all peers,
and joins shutdown. Passwords are passed through child environments and omitted
from the receipt. Data and server logs remain in the temporary directory printed
in the receipt. It never connects to user deployment data.

The probe records RSS, virtual address size, thread count, numeric file descriptor
count, status latency, idle peaks, post-close recovery, binary SHA-256, configuration
and exit status. It verifies that every offered pgwire startup socket remains open;
connect success alone would conceal immediate listener shedding.

This small profile covers 32 HTTP keep-alive sockets plus 32 pre-authentication
pgwire startup sockets. The recovery allowance includes allocator/runtime caches;
it requires FD recovery but does not require cached worker threads or RSS to drop
immediately. It does not prove per-thread committed stack size, cursor snapshot
cost, authenticated pooling behavior, throughput, Linux/container envelopes, or
distributed recovery. Those need their own fixed manifests and receipts. Do not
use this result to enable automatic sizing or query waiting by default.

The original macOS receipts retain the failed 146 MiB idle-growth measurement.
The shared signal-stack candidate passes the unchanged 128 MiB RSS growth and
recovery limits; the manifest has not been relaxed. See the implementation ledger
and the receipt's binary hash for the measured revision.

Run the authenticated cursor profile with the isolated launcher:

```sh
python3 zig/qualification/workload-pooling/run_local.py \
  --binary /path/to/antfly \
  --manifest zig/qualification/workload-pooling/macos-authenticated-cursors.json \
  --output /tmp/pooling-authenticated-receipt.json
```

This profile creates a relational fixture only in the launcher's isolated
storage. It retains 32 authenticated pgwire sessions and 32 HTTP peers, with 16
cursors held across COMMIT. It verifies exact integers above 2^53 and all remaining
cursor rows, then closes cursors and sessions and measures resource recovery.
Fixture failures produce a failed receipt. Credentials and raw pgwire error
messages are excluded. It covers retained cursors; extended-protocol prepared
portals and slow-consumer overload still require their own performance profiles.

`linux-combined-pressure.json` retains 32 authenticated pgwire peers, eight
cursors across COMMIT, 32 HTTP keep-alive peers and eight unfinished 4 MiB
uploads throughout the open-loop phases. The mixed phases also include slow
response readers and existing-row writes. The profile keeps the original
latency, throughput, RSS, descriptor and recovery gates, requires measured
waiting, and compares three rotated repetitions against main and fail-fast.
Setup rejects cursor errors; verification rejects closed upload sockets,
reconnected HTTP peers or missing cursor rows. All retained clients close before
descriptor recovery is sampled. Declared upload bytes describe the offered
pressure; they are not a claim about actual allocator capacity or total RSS.

Linux default startup also initializes the optional local ChatGPT credential
store. Zig's non-iterable Linux directory handles use `O_PATH`, which cannot be
used with `fchmod`. The candidate opens an iterable handle before enforcing
`0700`. When an older baseline needs the same fix, CI applies
`baseline-linux-startup.patch` and retains the applied diff alongside its build
artifacts. Such results compare against patched main; the patch does not change
pooling policy or performance thresholds. Original startup failures are retained
and cannot be interpreted as latency or throughput measurements.

Older main also runs idle pgwire sessions on the shared outbound API executor.
Those long-lived tasks exhaust its concurrency quota and close later accepted
connections. The candidate uses a listener-owned executor bounded by the
configured connection count. CI applies `baseline-pgwire-transport.patch` when
needed and retains the applied diff; it changes only listener task ownership,
leaving main's SQL admission and allocation policies intact. These are patched
main comparisons. The saturation regression retains 32 sockets while an
independent outbound executor is full, then verifies joined shutdown and port
reuse. Historical failed setup receipts remain failed; fresh measurements are
required to qualify the repaired baseline.

## Resource ledger and operator controls

[RESOURCE_OWNERSHIP.md](RESOURCE_OWNERSHIP.md) records nested versus independent
owners, acquisition order and progress boundaries. Native data/standalone API
allocations now charge the node memory manager used by managed storage/models;
HTTP application output and pgwire heap charge the corresponding API envelopes.
Raw transport buffers, native stacks/TLS and backend mappings still need separate
qualification. A total-process RSS guarantee is not implied.

Authenticated administrators can GET `/admin/v1/workload/query` for configured
and effective query limits, policy version, queue timing, retiring waiter count,
drain/suspension state and allocation envelope counters. PUT replaces a policy:

```json
{
  "expected_version": 1,
  "capacity": 2,
  "waiting": {"max_queued_requests": 2, "max_queued_bytes": 1048576, "max_wait_ms": 100},
  "suspended": false
}
```

Limits may shrink and later return to startup reservations. A stale version
returns 409; increasing finite capacity, queue size/bytes or wait ceiling beyond
startup reservations returns 422. Explicit startup capacity zero remains
unlimited; changing a finite startup capacity to zero is rejected. Queue shrinking
retires newest excess waiters; existing grants survive. Suspension retires queued
work and rejects fresh queries while transaction controls remain available.
Existing waiters keep their original finite wait ceiling/deadline; a replacement
wait ceiling applies to subsequent enqueue operations. PUT
`{"expected_version":2,"action":"drain"}` permanently closes query admission.
Quiescence requires active and queued counts and notified-but-unretired entries
to reach zero; transport/background-owner joins are separate shutdown barriers.
Policies are node-local and reset to configuration after restart.

## Reproducible pressure comparison

`run_matrix.py` runs origin/main (when supplied), candidate fail-fast, and candidate
fixed waiting in independent temporary stores. Manifests with a runnable policy
also compare candidate runnable scheduling directly against candidate fail-fast,
candidate fixed waiting and origin/main. The same-binary comparisons validate
both complete manifest policies before normalizing only the declared scheduler
and waiting differences; memory and admission capacity remain equal. Candidate fail-fast uses the same
startup heaps and configuration as fixed waiting, with only query waiting disabled. `load_clients.py` records every
offered/submitted/completed/successful/failed/generator-dropped request, zero
retries, successful and all-response p95/p99, resource peaks, control latency and
recovery. Latency starts at the scheduled offer time, including client queueing.
Mixed workloads validate exact query rows, issue writes, and delay reading large
results. The generator has a finite task envelope; dropping offered work fails
its manifest gate. Successful throughput excludes failures and includes drain
elapsed time. A status code without valid rows is not a successful query.

```sh
python3 zig/qualification/workload-pooling/run_matrix.py \
  --candidate /path/to/candidate/antfly --baseline /path/to/main/antfly \
  --manifest zig/qualification/workload-pooling/macos-mixed-load.json \
  --repetitions 3 \
  --output-dir /tmp/fresh-pooling-comparison
```

The Linux profiles require exact enforced cgroup-v2 limits and fail on macOS or
an unlimited Linux host. Build native GNU Linux binaries outside the measurement
container, then run the small profiles with two CPUs, 2 GiB, 512 PIDs and 512 FDs;
the large profile uses four CPUs, 4 GiB, 768 PIDs and 1024 FDs. The Dockerfile and
manual `Zig Workload Pooling Qualification` workflow provide that environment,
build the selected baseline with the same stripped ReleaseSafe optimization and pinned
Nix glibc-linked Zig toolchain, verify compiler version and dynamic linkage, run
the GNU signal-stack fixture, and retain all receipts. Small/large authenticated profiles
also exercise extended-protocol prepared portal suspension/continuation.

Repeated matrices rotate the variant order so each binary/policy occupies every
position. Every repetition retains its own receipts and comparisons; one failed
repetition fails the matrix. The manual workflow uses three repetitions with
ReleaseSafe binaries. Per-phase receipts also separate cheap queries, expensive
queries, writes and slow readers, record server CPU consumed and preserve
before/after operator counters for candidate policies. Operator snapshots occur
outside the timed phase. Per-kind comparison ratios are diagnostic; the original
aggregate latency and successful-throughput gates remain unchanged. The fixed
variant must also report SQL waiting as effective after executing each phase;
startup configuration alone cannot qualify an unavailable worker pool. CI builds
portable x86_64 GNU binaries on the existing heavy build runner, then measures
them on a separate Ubuntu runner in the enforced small/large Docker envelopes.

The separate `macos-waiting-pressure.json` and `linux-waiting-pressure.json`
profiles use one execution slot and offer 750 requests/s during sustained load.
They retain the mixed-load resource and performance gates and require at least
one completed admission wait in that phase. The original 200 requests/s profile
remains a separate measurement: on ReleaseSafe it can measure overhead without
forcing requests to queue. A pressure receipt with no completed waits fails.

The comparison requires identical manifest hashes, enforced limits, offered
work and zero retries. It checks a 25% low-load/recovery p95 regression ceiling
and 90% successful-throughput floor, in addition to each profile's absolute
limits. These are proposed release gates fixed before measurement, not achieved
performance claims. Passing one comparison never enables automatic defaults;
Linux, whole-node ownership and distributed-progress gates remain explicit.
Receipts cannot be overwritten accidentally. `test_harness.py` verifies the
comparison and resource gates fail closed.

The cold and warm macOS load profiles are separate measurements. The warm
manifest explicitly records warm-up operations and uses cheap low-load/recovery
queries; it is not a replacement for a failed cold profile. Historical development
[receipts](receipts/2026-10-06/README.md) retain both outcomes and binary provenance.
The API accounting port shares the actual storage owner in linked composition.
Managed inference and public HTTP transport now share the node accounting owner.
Unmanaged native and independently supplied service allocations retain their owners;
see [the ownership ledger](RESOURCE_OWNERSHIP.md) before interpreting health
counters as a whole-process cap.

Same-binary comparisons reject configuration differences other than waiting and
the ephemeral pgwire bind port. Each new receipt preserves a redacted server log
beside its measurements. Missing idle control samples fail with `null`, never a
passing zero. Final Linux comparisons currently fail sustained throughput; see
[recorded outcomes](receipts/2026-10-06/README.md).


## Runnable scheduler comparison

`linux-runnable-pressure.json` adds an explicit fixed `runnable_policy` to the
combined retention profile. `run_matrix.py` then also runs `candidate-runnable`,
comparing it with `candidate-fixed` and main. The candidate pair shares the same
query waiting, operation admission, memory ceilings, workload and retained peers;
only runnable scheduling changes. Existing manifests keep their original variants.
Use four rotated repetitions when including main, fixed, fail-fast and runnable:

```sh
python3 zig/qualification/workload-pooling/run_matrix.py \
  --candidate /path/to/candidate/antfly --baseline /path/to/main/antfly \
  --manifest zig/qualification/workload-pooling/linux-runnable-pressure.json \
  --repetitions 4 --output-dir /tmp/fresh-runnable-comparison
```

Authenticated workload snapshots include `runnable` counters. Before and after
each phase, the probe requires enabled scheduling, stable configured capacities,
monotonic grant/yield counters and effective capacities matching the manifest.
Successful query and write operations require corresponding grants during that
phase. Sustained overload requires at least one cooperative yield. The existing
request accounting, no-retry, latency, throughput, retained-peer and resource
limits still apply. Same-binary comparisons reject any undeclared admission or
memory change. The server build and compiler qualification stay outside the
measurement container.

Phase receipts also include `runnable_diagnostics.service`: versioned active
execution-lease wall slot-time deltas for query general, write, inference,
background, query bounded and query analytical. Estimates, observations,
unobserved estimates, refunds, debt payments and clipped corrections are separate
counter arrays; debt and rebate are end-of-phase gauges. The probe rejects
counter resets, malformed classes, unsupported contracts, changing availability
or quantum, and corrections outside the scheduler's 64-quantum bound. Older
snapshots without service telemetry report `available: false`, without fabricated
zero observations. A phase may declare `runnable_observed_slot_ns_min` before a
run to require measured service; estimates and grants cannot satisfy that gate.
`linux-runnable-pressure.json` now requires at least one observed slot-nanosecond
in every runnable phase. The comparison also reconstructs service deltas from
both workload snapshots and rejects missing, invalid or inconsistent diagnostics,
even if the receipt's outer `passed` flag is true. This gate is declared before
new measurements; historical receipts without observations cannot satisfy it.
These observations do not establish thread CPU utilization, complete operator
coverage, or fair service shares. Existing latency, throughput and resource
thresholds remain unchanged.

This profile has not yet passed a current-source runtime matrix. It does not
complete the concurrency sweep, inference/background fairness, distributed
progress, durability, or automatic sizing gates. Runtime receipts are required
before drawing performance conclusions or enabling defaults.


## Declared concurrency sweep

`run_sweep.py` creates distinct hashed manifests for client worker ceilings
1, 5, 10, 20, 30, 40, 60, 80 and the overload point 160, then runs all four
variants at every point. Mixed burst/overload offer rates scale with worker count
from the reference profile, keeping the offered rate per worker fixed. Cheap
low-load/recovery rates, warmup, retained peers, server policy, the pending-client
bound, resource limits and every existing acceptance threshold stay fixed.
This is a client worker ceiling; phase receipts report the observed active-client
peak and workers actually used rather than assuming every worker ran at once.
Per-kind receipts reconcile offered, submitted, completed and dropped work.
The runnable variant fails if any offered workload kind has no successful work.

Prepare and inspect the complete declarations before measurement:

```sh
python3 zig/qualification/workload-pooling/run_sweep.py \
  --manifest zig/qualification/workload-pooling/linux-runnable-pressure.json \
  --prepare-only --output-dir /tmp/declared-runnable-sweep
```

Use a different fresh directory for measurement, supplying `--candidate` and
`--baseline` instead of `--prepare-only`. Four repetitions rotate every variant
through every position. Each point retains its own matrix and failures; an early
failure does not skip later points. Preparation is explicitly unmeasured and
cannot pass qualification. Missing or failed matrix receipts fail the sweep.
A point with generator drops fails its existing envelope gate; receipts must be
reviewed before changing any future workload declaration. No current-source
sweep has passed, and automatic defaults remain unqualified.

## Guarded native publication CPU return

Automatic runnable policy reserves one publication-return CPU inside the node's
slot ceiling, alongside independent health and cleanup CPUs. Its minimum is four
logical slots, including one ordinary CPU. Fixed policies opt in with
`publication_slots = 1`; a zero or busy reserve keeps the publisher's existing
CPU and never waits to acquire the reserve under a DB fence.

Top-level managed table batches can release ordinary CPU at an independently
owned native WAL readiness wait and resume on publication CPU while retaining
their apply, snapshot and policy fences. The original operation, class, memory,
cancellation and deadline remain authoritative. The scope returns to ordinary
CPU after its DB work and physical joins retire. Denied return after a successful
primary commit reports `DurabilityOutcomeUnknown`; it does not fence an otherwise
healthy backend or claim that the mutation was rejected before commit. Metrics
expose the effective reserve as `antfly_runnable_publication_capacity_slots`.

Nested capture/producer scopes, child executions, multi-CPU teams and providers
without the owned native readiness contract retain their existing CPU policy.
The `antfly-guarded-db-commit-test` gate exercises actual bound table callbacks,
managed filesystem DBs, native WAL I/O and reopen recovery, with deterministic
physical pause hooks. It includes an ordinary status request holding the sole
ordinary CPU while waiting on the publisher's apply fence, independent health
progress, cancellation/deadline joins and bounded ordinary-return refusal. This
focused evidence does not qualify HTTP transport overload or every engine.

## Native directory publication pressure

`macos-guarded-publication-pressure.json` selects the standalone `local`
directory engine and a four-slot automatic policy. It requires an observed
publication reserve of one CPU, query/write progress, bounded waiting, status
latency and resource recovery during mixed load. Eight HTTP peers, eight pgwire
sessions, four held cursors and two unfinished uploads remain live through the
phases. Changing, fixed-size payloads force updates instead of repeating the
seeded postimage. Successful queries and writes must record grants in their
respective runnable classes; unexpected server errors fail qualification.
The native HTTP batch worker and its inline fallback retain a write capsule
and the original request authority through physical completion. Historical
profiles keep their original byte pattern and file-engine selection.

```sh
python3 zig/qualification/workload-pooling/run_local.py \
  --binary zig/zig-out/bin/antfly \
  --manifest zig/qualification/workload-pooling/macos-guarded-publication-pressure.json \
  --policy runnable --output /tmp/guarded-publication-pressure.json
```

After load and client retirement, the launcher acknowledges one fresh primary
write, verifies its exact large integer and payload over HTTP and pgwire, joins
server shutdown, and reopens the same isolated store. Both protocols must return
the same postimage after restart. The receipt retains both exit statuses,
postimage hashes and harness hashes; missing restart evidence fails the run.
This is joined-restart evidence. Crash/power-loss durability and comparisons
across all engines require their own runs. This profile checks status progress;
operator-policy drain testing remains in the existing dedicated profiles.

Admission-aware backend batch reads, scoped reads and cursor opening use the
owning runtime's checked dispatcher, matching ordinary backend reads. Raw Zig
error identities cannot cross storage archives: contention previously surfaced
as `InvalidSourceSnapshot` instead of backpressure. Synchronous admission relays
preserve the caller's original rejection, discard partial values and retire a
cursor returned after rejection. Focused dispatcher tests cover those paths;
a native file-engine mixed-load run completed every phase without unexpected
server errors and with query, write and slow-reader progress.

Resident LSM inspection and both maintenance-step policies now carry the
original lifetime restriction through the checked physical engine interface.
The shared borrowed-I/O storage adapter supplies admission-aware range reads
for owned recovery cursors, reuses its caller's executor, checks lifetime between
64 KiB windows and closes the descriptor before CPU reentry. Its allocated-range
wrapper also uses checked dispatch. This adds no host-path publication permission.

Dedicated metrics-listener runs distinguish real background work from absence
of warnings: both directory and file engines recorded flushes and completed
compactions under mixed load, with zero maintenance worker failures. The original
profiles then completed without maintenance or publication-recovery errors and
verified exact HTTP/pgwire postimages through joined restart. The directory
profile passed all gates; file descriptor recovery remains failed. These local
runs do not qualify distributed or serverless deployment paths.

The restart probe runs independently of load/resource acceptance when the
server remains live. A failed load receipt stays failed even when exact HTTP
and pgwire postimages survive joined shutdown and reopen; the receipt records
the load exit code and both server exit codes separately. Native file-engine
mixed load now has this exact-postimage restart evidence.

File-engine qualification remains open because the unchanged cold-baseline
descriptor-recovery gate still fails. A diagnostic ownership census matched all
21 artifact-marker descriptors to their live registry sources: 16 idle-expirable
cache sources and five explicit readers, with no callbacks in flight. The source
cache and live full-text segments retain storage independently of connections;
this census does not prove process-wide descriptor bounds or qualify the failed
gate. The retained-cursor suite's two reproduced baseline failures are now
resolved in their assertions: normalization OOM enumeration disables optional
in-place allocator growth, and predecision accounting identifies live manager
metadata before verifying its release at destruction. All 36 retained-admission
tests pass without skips or leaks; the failing baseline receipts remain available.
The resource, server-error and restart gates remain unchanged. A passing
directory-engine receipt does not qualify the file engine.

Directory overload/restart has a passing native receipt, but repeatability is
still open: a repeat of the same binary and semantically identical harness
observed warm-up capacity rejections and no successful slow-reader operation
in the sustained-overload phase. Preserve these gates rather than treating the
single passing run as stable qualification of the whole design.


## Serverless WAL ingress demand

The namespace and table `ingest-batch` routes reserve one runnable CPU for
sequential encoding and WAL acceptance. Other write routes retain their
configured build-team width. Original request cancellation, finite protected
completion, WAL acknowledgement and response retirement remain in the existing
owning execution path.

The focused serverless gate covers both WAL handlers with only one general
slot and a configured two-slot build demand, including original rejection
identity and durable acknowledgement after foreground rejection. The managed
native listener also exercises automatic defaults and a two-slot general pool
with a cooperative competing CPU owner, real FsWal ingestion, background
publication and exact published document reads. The competing owner checkpoints
so the scheduler can honor age-protected wide maintenance work; permanently
pinning a synthetic slot correctly caused bounded capacity rejections.

The production server builds and the focused gate passes. These listener tests
use local filesystem providers; they do not qualify cloud/distributed providers
or all serverless maintenance waits.

The H2 lifecycle probe now distinguishes peer-visible disconnect from local
socket close. Diagnostics reproduced a closed client descriptor and a still
established server socket despite correctly installed zero linger. An acknowledged
PING boundary drains preceding control frames before the abort case; it neither
grants response credit nor retires the stalled owner. The probe still requires
zero scheduler operations, retained bytes, foreground/control allocations, active
requests and physical connections within the original five-second limit, before
server shutdown.

The original unfenced abort also remains covered. That case and an additional
fully silent peer use explicit 1.5-second request and 250-millisecond idle
ceilings, proving bounded ownership when no immediate close notification reaches
the server. These are local native-listener checks of the existing production
request/idle policies; they do not promise five-second detection of an unseen
abort under arbitrary policy or network conditions. Ten consecutive runs of
the complete focused serverless binary pass all five lifecycle cases and the
managed ingestion/publication checks. Published-read polling may observe
retryable capacity responses before success within its existing retry bound.
The failed baseline and native socket/stack diagnostics remain available.


## Serverless maintenance execution phases

Managed orchestration now starts with one background CPU. Waiting for the
runtime or publisher mutex returns CPU on the same execution and executor;
reentry precedes the next lock attempt, so queued CPU never retains that lock.
Synchronous public-batch materialization borrows the request's existing producer
capability for these waits and its bounded retry sleeps.

The original maintenance authority survives namespace leases and graph-source
protection. Owner-only publication checkpoints remain separate from parallel
cancellation checks. Graph kernels grow the existing team before joined work
and restore its previous width before encoding/publication, including compatible
HITS pairs and rejected computation. Enrichment retains its configured stage
width pending finer provider/model integration.

Public batches reserve their bounded completion task and metadata before
possible WAL acceptance. The task borrows the existing request executor and
execution; the physical WAL reuses its activated completion. Optional sync waits
retain the original narrower sync restriction and leave terminal-response time
inside the finite completion. Foreground rejection and scheduler close after
acceptance preserve the committed/pending response. This introduces no new
per-request executor or independent scheduler root.

The focused native gate exercises real HTTP ingestion and `full_index` batches,
filesystem WAL/publication, and exact published postimages. A work-cost probe
checks one-CPU orchestration with a configured two-CPU build width. The native
page-graph builder verifies authority propagation through leases and publication;
separate legacy-payload/kernel fixtures cover shared HITS work, serial restoration
and healthy reuse after rejection. Background and request-borrowed mutex tests
preserve original rejection while a competing request owns all general CPU.

This is an integrated CPU-phase and synchronous-caller increment. Admission
around physical provider I/O, finer joined-kernel participant scheduling, and
model/provider waits remain required to complete the maintenance milestone.
Local filesystem evidence does not qualify remote/cloud providers or all engines;
the existing full-qualification waiver and failed file-engine resource gate remain.

Maintenance artifact uploads also carry an operation-local provider context from
the normal catalog and builder entrypoints. Shared object-store handles never
receive that owner's execution pointer. Filesystem providers hash before lending
CPU, join the prepared durable mutation and release its content locks before CPU
return. S3 and GCS forward the same operation's wait control to their existing
HTTP transport readiness and pool waits; cancellation monitors retain only the
lifetime predicate.

Page upload batches reserve bounded child metadata before publishing workers.
The coordinator parks its complete existing team before child activation and
reenters after every physical task joins and its child entry retires. Deferred
reservation supports an existing foreground team as well as serial background
orchestration. Original lifetime, clock, class and accepted-completion authority
remain inherited; no child creates a fresh admission root or deadline.

The provider integration remains part of the open maintenance milestone.
WAL and catalog mutations, GC/enrichment progress and work-lease calls,
retention/pruning mutations, direct filesystem artifact adapters, and model/kernel
participant waits still need complete physical-boundary integration. Transport forwarding alone does not
qualify remote providers. The existing file-engine resource-gate failure and
full-qualification waiver are unchanged.


### Maintenance metadata provider ownership

Operation-local store views now carry the original execution through managed
publication, status/planning, synchronous materialization, compaction, pruning
and enrichment. The views share the original backend and retained lease-cache
owners; neither mutable provider handles nor cache mutexes are copied. The
new view packets are included in background admission and public-batch completion
metadata before those operations can publish work or accept WAL.

Manifest encoding and decoding, WAL decoding and catalog reconstruction retain
CPU. Object-backed reads lend CPU at file open/stat/header/body reads, bounded
filesystem iterator steps and existing S3/GCS transport waits. Opened descriptors
remain owned through CPU return; denied return closes them through the provider's
original executor. Provider bodies and decoded outputs remain operation-owned
through the final lifetime check. Legacy adapters without contextual callbacks
keep their existing CPU ownership.

Catalog read sequences retain their existing mutex. Their physical waits use the
scheduler's nonwaiting publication reserve; a disabled or busy reserve keeps CPU
instead of queuing for ordinary CPU while holding that mutex. HEAD publication
uses the same verified ETag and lease record in its conditional provider write;
contextual publishers can finish without taking a legacy caller's local mutex.
Original rejection after physical publication discards only the response and
preserves the durable HEAD.

Focused native evidence combines the actual listener's HTTP ingestion, public
batch and published-query paths with filesystem-backed maintenance fixtures.
The latter perform successive publications, compaction and lease-aware pruning,
verify exact public postimages, exercise enabled/disabled catalog return capacity,
and reject owned reads after CPU return. A separate physical HEAD-write fixture
preserves durable publication after original-scope rejection. Enrichment checks
retain the stable idempotent WAL identity over both memory and filesystem object
providers, including an already funded two-CPU write owner. These fixture results
do not qualify cloud transports or prove physical deletion while read leases
still protect old versions.

The full maintenance milestone remains open for remaining mutation/retention
provider boundaries, immutable artifact reads and provider/model/kernel
participation. Remote-provider behavior and the original cross-engine/resource
gates also remain open; the full-qualification waiver still applies.

### Maintenance kernel participant integration

Graph materialization now admits each joined worker group at its actual width.
Small kernels retain serial capacity even when the configured maximum is larger;
iterative kernels restore their prior width before reductions, iteration
checkpoints and encoding. The coordinator alone changes scheduler ownership.
Workers retain lifetime cancellation, and error cleanup cancels and joins their
group before restoring capacity. Both ordinary metric materialization and the
shared paired-HITS publication path use these hooks. Serial vector construction
uses cooperative owner checkpoints at its existing cancellation boundaries.

Foreground maintenance borrows the ingress owner's funded width through the
existing ABI/native adapter. It does not resize that owner or create another
root. Requests exceeding the borrowed width fail before worker publication.
Explicitly disabled scheduling retains its existing empty-capsule behavior.
The native-only width callback does not change the public runnable ABI.

Focused evidence lives in `serverless-kernel-participants-v5-debug.json` and its
log under the cumulative integration receipts. Native scheduler fixtures cross
both the 128-Ki-edge and 32-Ki-node thresholds for PageRank, eigenvector and both
HITS outputs, compare encoded results byte-for-byte against serial execution,
and exercise lease rejection, cleanup and an ingress-borrowed team. The existing
paired-HITS fixture preserves its shared-work budget and durable artifact
checks while correctly retaining one lane for its small graph. A vector fixture
queues a real query behind the sole ordinary lane, proves it progresses at a
construction checkpoint, checks protected control admission and verifies root
retirement. These are production-code integration fixtures; the separate normal
listener regression tests do not constitute a large-graph endpoint load test.

The maintenance milestone remains open for invocation-scoped model execution,
model construction/discovery/asset waits, serial graph preparation checkpoints,
remaining provider mutation/retention boundaries and immutable artifact reads.
Cross-engine resource gates, including
the unresolved file-engine descriptor bound, remain open. Full GNU/full
qualification remains waived; no cloud or all-engine qualification is claimed.

### Maintenance model invocation integration

Managed enrichment restores serial capacity after provider construction. Sparse
and dense embedding calls, media generation probes and paged media execution
now bind a synchronous invocation scope to the original maintenance execution.
Each call admits its configured team and restores its previous width before
fallback, document encoding or WAL publication. Foreground calls consume their
existing funded team through the ABI/native adapter without creating a root or
resizing the ingress owner. The adapter can now expose that borrowed team to the
existing inference host's worker phase handoff.

The invocation copies the configured provider descriptor and preserves its
routing, transport, progress and narrower request lifetime. Cached embedder and
chunk-provider handles never receive stack-scoped execution callbacks; durable
chunk-provider construction rejects an invocation-bound grant. Lifetime
polls serialize renewable source/maintenance authority; only the coordinator
changes CPU ownership. Owned model output is discarded when original authority
fails after provider return. Genuine provider failures retain the configured
fallback policy; admission, cancellation, source-lease and narrower-scope
failures propagate without publishing a derived WAL mutation. Legacy providers
without contextual callbacks retain their synchronous compatibility behavior
and do not gain physical-wait control from this integration.

The invocation packet is included in `CatalogService.Scope.retained_bytes`,
which funds both background roots and preaccepted foreground completion work.
No request creates a separate executor or persists its grant in a model owner.

Evidence: `serverless-model-invocations-v6-debug.json` and its log in the
cumulative receipts. A filesystem-backed production enrichment fixture covers
sparse, dense and media routes with owned and borrowed executions, including
parked-capacity reuse by queries and protected control, model fallback, rejection
after owned output allocation, narrower request authority and unchanged cached
handles. It checks durable WAL advancement and complete root retirement. A separate
queued-team fixture rejects narrower invocation authority while an unrelated
owner continues holding the lane needed for admission. The
existing enrichment, media scan and inference handoff tests provide regression
coverage. These use deterministic model callbacks and real scheduler/provider
plumbing; they do not qualify a downloaded model, cloud deployment or the
installed CLI binary.

The maintenance milestone remains open for physical waits in remote model
transports, provider/model construction and discovery, asset loading, remaining
provider mutation/retention and immutable artifact reads, and serial graph
preparation checkpoints. The original cross-engine resource gates remain open,
including the file-engine descriptor bound. Full GNU/full qualification remains
waived.

### Maintenance remote model HTTP waits

Embedding dispatch now creates a request-local HTTP wait adapter and an owned
credential-cache overlay. The adapter forwards the existing execution to
outbound socket and pool waits without modifying persistent clients or cached
model entries. Antfly JSON/numeric/framed requests, OpenAI-compatible and Cohere
requests, and Gemini/Vertex and Bedrock model requests carry these controls.
Remote media chunk requests use the same adapter. CPU returns to the actual
saved class through checked reentry; response buffers remain transport-owned
until that succeeds. Original fallible cancellation survives provider adapters
and is rechecked before owned decoded output transfers to the caller.

Capability discovery and model activation carry the operation's controls.
Callers waiting on a shared discovery flight lend their own grants while
retaining their own lifetime and flight reference. Retired-flight and query
retry backoff also return CPU on the original executor. Discovery keeps its
separate activation budget; only an explicit caller deadline narrows it, rather
than starting the default inference timeout before a cold model is ready.

Request overlays borrow shared cache synchronization and own their authorization
cache allocations. Nested HTTP invocation packets are funded in the existing
background/accepted-completion metadata reservation. Persistent models and
clients retain no invocation control callbacks.

Evidence is recorded in `serverless-model-http-v7-debug.json` and its log under
the cumulative integration receipts. Real loopback HTTP fixtures exercise both
OpenAI-compatible responses and cold Antfly discovery/model requests, including
query/control progress during waits, retained metadata, and original authority
rejection while response reentry waits behind another owner. A two-caller test
proves shared discovery performs one catalog request while both callers return
CPU, then completes both model invocations. Existing activation, caller-budget,
framing, pinning, retry and model invocation tests provide regression coverage.
Responses are deterministic fixtures; this does not qualify live cloud services,
TLS across a deployment, downloaded models, or a rebuilt installed CLI.

The maintenance milestone remains open for credential resolution/refresh and
quota waits, provider/model construction and asset loading, lease renewal and
remaining mutation/retention/immutable-artifact provider boundaries, and serial
graph preparation checkpoints. Original cross-engine resource gates, including
the file-engine descriptor bound, remain open. Full GNU/full qualification is
still waived.

### Maintenance credential refresh and quota waits

HTTP attempt observers now receive the request's execution-wait control.
Provider quota admission returns CPU during capacity, pacing and cooldown waits,
after releasing the limiter mutex. Only successful admission owns a permit;
cancellation during a wait never releases another caller's concurrency slot.
The controls reach normal managed model requests through their existing shared
clients, without storing invocation callbacks in the limiter registry.

Google and AWS credential HTTP exchanges and refresh waiters carry the same
caller control and deadline. Google token refresh now runs outside the token
cache mutex, with one refresh owner and bounded polling by waiters. Fresh tokens
and valid fallback snapshots retain their existing cache behavior; caller
allocation and authority checks precede publication/transfer. Rejected return
from a credential exchange cannot publish a replacement token or silently use a
cached fallback. AWS snapshot/caller-copy cleanup also preserves ownership when
refresh or shutdown rejects transfer. Managed Bedrock calls pass their existing
executor explicitly for credential files rather than creating a request-local
filesystem executor.

Evidence: `serverless-credential-quota-v5-debug.json`, the prior fresh integration
run `serverless-credential-quota-v3-debug.json`, and their logs in the cumulative
receipts. The real model HTTP quota fixture proves query/control progress while
admission waits and verifies that rejection neither sends a request nor releases
an unowned permit. Real loopback OAuth and STS fixtures cover refresh owners,
shared waiters, mutex availability during network waits, one exchange for two
callers, rejected CPU return, unpublished rejected credentials and root retirement.
Dedicated Google/AWS test targets run the libraries' actual allocation,
cancellation, source-authority and shutdown regression tests; consumer filters
alone did not discover those separate-module tests. Fixture tokens and responses
are deterministic; live cloud identities and services are not qualified here.

The maintenance milestone remains open for credential-file physical wait
boundaries, provider/model construction and asset loading, lease renewal and
remaining mutation/retention/immutable-artifact provider boundaries, and serial
graph preparation checkpoints. The original cross-engine resource gates remain
open, including the file-engine descriptor bound. Full GNU/full qualification
remains waived; the installed CLI was not rebuilt for this increment.


### Maintenance credential-file admission

Normal AWS profile, web-identity and ECS authorization-token file reads, Google
ADC/configuration and external-account subject-token reads, and Vertex project
lookup now carry the invocation's execution-wait control. The bounded file
adapter lends CPU only for physical open/read operations; buffer growth and
credential parsing run after checked CPU return. A descriptor opened before a
rejected return is closed with its original filesystem authority, and partially
read buffers remain owned until success or cleanup. Existing credential-file
size limits remain enforced, including files requiring several bounded reads.

Google credential-source construction runs outside the manager mutex and uses
the manager's supplied I/O runtime. Concurrent candidates resolve publication
under the mutex, then dispose the losing candidate after unlocking. Invocation
controls are never retained in cached sources. Configuration/project discovery
also checks caller authority before transferring allocated results.

Evidence: `serverless-credential-files-v3-debug.json` and its log in the cumulative
receipts: 68 serverless integration tests, 31 local partition tests, 17 AWS and
14 Google library tests pass; native-borrow, inference-owner and cleanup targets
reuse unchanged successful results. The new production-loader fixture uses real
temporary credential files with real scheduler admission. Its deterministic gate
pauses CPU return after actual open/read completion; it does not simulate slow OS
storage. It proves query/control progress, unlocked Google/AWS caches, one cached
source for concurrent Google candidates, rejection before cache publication, and
retirement of all execution roots/retained-byte charges. The bounded-reader test
covers chunking, exact limits and overflow. No live cloud identity is involved.

The broader maintenance milestone remains open for AWS vendor-process waits,
provider/model construction and asset loading, lease renewal, remaining mutation/
retention/immutable-artifact provider boundaries, and serial graph preparation.
Cross-engine resource qualification, including the original file-engine descriptor
gate, remains open. Full GNU/full qualification remains waived; the installed CLI
was not rebuilt for this increment.


### Independent CPU admission for native model loading

Shared model and composite-asset loads now acquire one independent execution
grant on their existing loader task. The grant borrows the flight's collective
lifetime, remains owned through synchronous construction and final checks, and
finishes before the task releases its flight ownership. Final-fence rejection
retires an already published primary model or destroys an unpublished composite
candidate. Callers park while awaiting independently admitted loads and resume
before adopting handles; warm completed lookups avoid a new wait transition.
Configured admission always requires a guaranteed concurrent loader, including
callers without request controls; an exhausted async pool cannot execute it
inline under the waiting caller's grant.

In-process nodes install their existing execution-admission provider on the
model manager. Supervised workers use private wire version 17 and carry loader
CPU start, park, checkpoint, reentry, cancellation and finish over dedicated
resource RPC. The parent records grants in a separate pinned ledger; scheduler
waiting holds neither the shared resource mutex nor that ledger's registry
mutex. Transport close cancels queued callbacks, and physical reap drops the
generation's registry ownership while active callbacks retain their pins.

Controlled resource RPC retains pending reply storage after cancellation,
sends the owner an explicit cancellation, and joins the original reply before
returning. A lost or uncertain mutation reply fences the child. CPU admission
and the one-worker native CPU scope stay on the actual loader worker; no request
callback or borrowed caller grant is retained by an asynchronous model load.
Without a configured execution owner, existing offline/disabled paths retain
their prior behavior rather than parking around an uncharged loader.

Focused evidence is recorded in `serverless-model-load-waits-v5-debug.json`,
`serverless-model-load-waits-v6-debug.json`, and
`native-model-load-waits-v7-debug.json` in the cumulative integration directory. Real scheduler/pipe fixtures cover
shared loader ownership, query/control progress, one departing caller, final
cancellation during loading and queued CPU reentry, and pinned callback retirement.
Constructor fixtures use a tiny local native model and local Whisper metadata;
the constructor cases disable the async pool and cover controlled and legacy
callers, admission denial before memory-domain creation, and final-fence rollback.
These are deterministic native integration cases, not downloaded model or live
cloud qualification. The production worker entry receives a separate semantic
compile check. Full GNU/full qualification remains waived.

This completes independent loader admission and shared-flight waiting within the
maintenance milestone. Physical model-file/download waits inside construction,
provider/model asset preparation, vendor-process credential waits, lease renewal,
remaining mutation/retention/immutable-artifact boundaries, and serial graph
preparation remain open. Cross-engine resource gates, including the original
file-engine descriptor bound, remain open. The installed CLI is not rebuilt by
these focused tests.


### Physical native model-file waits

The shared native `c_file` path now returns CPU around descriptor open, size/stat
queries, bounded positional reads, and read-only mapping syscalls. Reads advance
in at most 1 MiB chunks. Path/buffer allocation, size checks, hashing, parsing and
mapped-data consumption retain the loader's CPU grant. Newly opened descriptors
and mappings are disposed if checked CPU return rejects them; partially read
buffers and existing descriptors retain their cleanup owners.

Physical-return refusal remains sticky within the synchronous checkpoint scope,
including nested scopes, so optional metadata fallback cannot turn a stopped
loader into fresh work. Scope teardown restores the previous invocation state.
Model/composite construction failure paths replay the owning control before
publishing an error. Mapping stat failures preserve the original admission cause
rather than collapsing it into `StatFailed`.

Evidence: `native-model-file-waits-v5-debug.json` in the cumulative integration
receipts. Six selected native tests include actual local model/composite
construction with physical-wait callbacks, refusal before resident admission,
descriptor recovery after open/stat/read refusal, exact chunked reads, limits,
and nested refusal propagation. The separate real-owner fixture drives the
production reader and mapper through actual scheduler capsules: queries and
control work run at each loan boundary, original width and retained bytes return
on success, and all roots retire after cancellation at open/stat/read-or-map
return. Its gates are deterministic around real temporary-file operations; this
is not a slow-disk latency or throughput measurement. The production worker
entry also passes its semantic compile check.

This closes the shared local file-syscall path within the maintenance milestone.
Mapped page faults, parallel prefetch ownership and other backend-specific I/O
remain outside this evidence. Downloads use a separate pull entry and still need
admission plus physical network/disk wait integration. Credential subprocesses,
lease renewal, other maintenance provider boundaries and graph preparation
checkpoints remain open, along with the original cross-engine resource gates
(including file-engine descriptor recovery). Full GNU/full qualification remains
waived; the installed CLI is not rebuilt by these focused tests.


### Query-template remote input waits

Query-time embedding templates now enter an invocation-local execution scope
before evaluating remoteText, remoteMedia and remotePDF. The normal distributed
semantic-query planner already forwards its query runnable, I/O, deadline and
cancellation into this managed entry. Rendering and subsequent provider dispatch
retain that request's authority. Configured and invocation cancellation compose;
request callbacks and mutable failure state are not cached in the shared entry.
Only the owning worker performs park/reentry transitions. Transport monitors
inspect lifetime without reading the owner's mutable failure state. A refused
transition stops rendering instead of becoming an inline helper-error directive;
the outer scope returns its exact original cause before provider dispatch.

The scraping HTTP path forwards the existing objectstore cancellation token's
execution-wait control on every redirect and pinned-range request. Response
classification and result transfer check the original context. S3 already
forwards this token to its HEAD/GET transport. Allowed local files now lend
capacity around physical open, descriptor and allowlist canonicalization, and
bounded positional/streaming reads. Owned-open refusal closes the descriptor.
Controlled reads use at most 1 MiB per syscall, with scratch/result allocation
and growth on CPU. Callers without execution transitions retain the original
buffered reader. Path allowlists, symlink confinement, response limits, and the
fail-closed policy for unsupported file deadlines remain in force.

Evidence is in `template-download-waits-v4-debug.json` in the cumulative receipts
directory. The integration fixture calls the production ManagedEmbedder template
API and real renderer, uses a real scheduler with a two-slot background grant,
and downloads through a real loopback HTTP listener before provider dispatch.
It proves independent query/control progress while waiting, retained-byte
ownership, restoration of the original width on success, exact source-revocation
failure while queued for response reentry, no embedding dispatch after that
failure, and complete root retirement. This is an integration fixture through
production APIs, not an end-to-end serverless semantic-query endpoint test.
The old entry-based test-only renderer bypass was removed after the fixture
exposed it. Separate actual-file tests inject deterministic return refusal at
every physical boundary, check exact bytes and overflow, and preserve path and
deadline denials. Pinned-range listener tests also assert paired execution waits.
Existing remote-content security and renderer regressions remain covered.
These checks are not cloud/TLS or disk/network throughput qualification.

Remaining in the broader remote-asset milestone: native inference media-download
contexts still carry cancellation without execution-wait ownership; normal
serverless semantic-query dispatch needs per-invocation runnable forwarding;
remaining asset-producer provider and capability paths need controlled waits.
Registry pulls are explicitly node-free CLI/SDK operations and must not be
mistaken for a model loader owned by the node scheduler. The wider maintenance
work and original cross-engine resource gates, including file-engine descriptor
recovery, remain open. Full GNU/full qualification remains waived; the installed
CLI is not rebuilt by these focused tests.


### Serverless semantic-query context and native media waits

Normal serverless ingress now creates a stack-stable read adapter on its existing
execution for public table queries, table search, and namespace search. The
trusted request context carries that team through public-query, joined-query and
graph-search adapters into semantic query resolution. It also retains diagnostics,
the original deadline and fallible cancellation. Expected lifetime rejection is
checked before adapters translate or log generic query errors.

ManagedEmbedder exposes synchronous context-bearing dense and template query
entry points. Each applies the invocation context to a request overlay and
combines configured and invocation cancellation. A cached query embedder retains
its own cache/credential owners without retaining a request runnable, cancellation
callback or transport-wait pointer. Per-table construction receives the same
request scope, including discovery before query dispatch. The existing
cancellation-only API remains available for callers without an I/O context.

The native inference server now creates a local download-wait adapter for each
synchronous media fetch and for the full lifetime of a pinned range pipeline.
Physical transport waits use the current worker's ResourceWait/CheckpointScope;
watchdogs only inspect request lifetime. The same control reaches HTTP, S3 and
allowed local files through scraping. Parsing, allocation, result transfer and
source-cache work remain runnable. Refused CPU return remains sticky in the
native checkpoint scope, and final checks recover the original cause before
optional per-item fallback or result transfer. Neither a request callback nor a
borrowed grant is stored in an asynchronous loader or cached transport.

Evidence: `semantic-invocation-waits-v3-debug.json` exercises the normal handler,
actual filesystem-backed catalog/WAL/publication and vector query execution,
and a real loopback model/template HTTP listener. Across all three semantic
routes it checks dense and remote-template calls with successful query results,
source cancellation while queued for reentry, and expiration of the original
request deadline. Another query and the health control route progress while the
model/template request is parked. Rejected template fetches never dispatch an
embedding request; response retirement releases request admission, retained
bytes and roots. Cached embedder request fields remain unchanged. The fixture
uses direct handler ingress plus real provider transport, rather than an
external client over a deployed serverless listener.

`native-media-download-waits-v1-debug.json` records 47 filtered native tests,
including actual multi-chunk file downloads through the native server helper,
rejection at every physical return, exact original refusal after optional
fallback checks, and existing independent-loader/model-file regressions. The
production worker entry also passes its semantic compile check. Native media
loan boundaries use deterministic callbacks around actual file I/O; this is not
native live-network/cloud, TLS or latency/throughput qualification.

Remaining in the remote-asset milestone: asset-producer provider/capability
paths still need transport wait controls through their full invocation lifetime.
The wider maintenance work (credential subprocesses, lease renewal, remaining
mutation/retention/immutable-artifact boundaries and serial graph preparation)
and original cross-engine gates remain open, including file-engine descriptor
recovery. Full GNU/full qualification remains waived. These checks do not rebuild
the installed CLI or establish completion of the full design.


### Asset-provider transport waits and compatibility workers

Asset-producer invocation boundaries now own their HTTP wait state through
capability/memory discovery, scalar and reported batch execution, and raster
availability checks. Direct Antfly requests and reader, transcription and
extraction adapters forward invocation controls to transport waits. Google
credential refresh and remote-audio downloads receive the same control; download
controls remain present when no cancellation predicate was supplied. Cloudflare
attempts also forward the control. Request overlays bind the synchronized
capability-cache owner without copying its mutable mutex/cache state.

Generation owns a separate synchronous wait scope around each actual backend
call and Antfly text batch. Its OpenAI/Antfly, Vertex/Gemini, Bedrock and Anthropic
transports use that scope; temporary transport pointers are cleared after every
physical request joins. Constrained-generation discovery scopes its singleflight
wait and HTTP control similarly. Parsing, formatting, allocation and result
transfer keep CPU. Expected source/return refusal cannot become successful
capability fallback. HttpWait publishes transition refusal through an atomic
latch for transport monitors; mutable owner-only failure state remains private
to the owner, and final readiness checks preserve the original error.

Compatibility fanout reserves every owned child capsule before publishing a
wave. Each physical worker activates its own child under the original source
restriction, builds an invocation-local provider scope, and retires that child
after its provider consumers return. The coordinator lends its grant until all
workers physically join, then reacquires its original class and width before
copying results. Failed publication cancels/joins published work before retiring
unpublished reservations. A serial QueueFull retry uses the restored parent
scope; no parallel worker borrows the parent's single-worker capability.
Legacy unscheduled calls retain bounded compatibility width without acquiring
fresh request authority.

`asset-provider-waits-v9-focused.json` records a focused integration fixture
through the production Producer APIs, real scheduler capsules and a concurrent
loopback HTTP listener. Twelve success/revocation cases cover readers,
transcription, generation, extraction, capability discovery, and two concurrent
compatibility transcriptions. All lend capacity for unrelated query/control
work; rejected reentry preserves the original source error and retires roots,
children and retained bytes. The shared Runtime retains no invocation wait or
runnable pointer. This is synchronous producer integration, not a deployed
end-to-end table enrichment campaign or a live cloud/TLS qualification.

`asset-provider-waits-v11-final.json` adds affected standalone library suites,
serverless regression coverage and the production worker semantic compile
check. The named `antfly-asset-provider-wait-test` target isolates the production
fixture for efficient feedback; its equivalent case remains part of the
serverless execution regression suite. Full GNU/full qualification remains
waived and the installed CLI is not rebuilt by these targets.

This integrates the audited remote HTTP provider/capability paths within the
remote-asset milestone. Vendor-specific subprocess/native asset-preparation
paths still require audit and integration; no subprocess/native-throughput claim
follows from these HTTP fixtures. Credential subprocesses, lease renewal,
remaining maintenance mutation/retention/immutable-artifact boundaries, serial
graph preparation and original cross-engine gates remain open, including the
file-engine descriptor recovery bound. The full design remains incomplete.


### Credential vendor subprocess execution

AWS configured-profile refresh now forwards the invocation execution wait to
vendor-command resolution and credential export. PATH access, bounded pipe-read
waits and process-exit waits lend the existing CPU grant. Spawn setup, output
buffer growth, JSON parsing and credential publication remain runnable. Fixed
pipe buffers allow stdout/stderr to drain concurrently without allocating while
parked; each captured stream retains its existing 64 KiB bound. The process
budget starts before environment/PATH setup and never renews after that setup.

The process monitor publishes its terminal stop before canceling and joining the
physical worker. Checked CPU return composes the operation ceiling with the
original source authority, so both request expiry and a shorter process timeout
remain Timeout through cancellation cleanup. Physical task cancellation used for
teardown cannot replace that cause with Canceled. This required an existing
worktree-local pinned-Zig overlay correction: a failed POSIX child wait retains
its identifier and pipe ownership for the caller's deferred kill/reap. Successful
waits still clean up normally; the installed toolchain remains unchanged.

The focused production fixture calls the real AWS CredentialCache profile path,
reads a real temporary profile file through the supplied Io authority, and
launches a real local subprocess. Its authority substitutes a deterministic shell
command for AWS; it does not execute a user's AWS CLI or authenticate to AWS.
Success, source revocation during a wait, revocation during queued CPU return,
request deadline and shorter process timeout each run with pipes open and with
pipes closed before exit. Additional wait/queued-return cases make the authority
report its source rejection only once, checking that the first cause stays
published through cleanup. It checks unrelated query and protected-control
progress, the refresh mutex remaining available, physical child retirement before
return, publication only on success, and complete root/retained-byte retirement.
A separate real-process capture check drains both pipes across multiple fixed
buffers and verifies each output cap rejects independently.

Evidence: `credential-process-waits-v8-focused.json` records the fourteen real
scheduler/subprocess cases and credential regression suite.
`credential-process-waits-v9-final.json` records the affected serverless, AWS,
Google, HTTP, request-lifetime, runtime HTTP-control and worker semantic checks.
The HTTP TLS fixture uses its existing `ANTFLY_TEST_TLS_PYTHON` override with
`/opt/homebrew/bin/python3`; Apple's older Python lacks TLS 1.3 support.
The worktree overlay's four Python preparation tests and Apache boundary check
also pass. These checks do not rebuild the installed CLI.

Credential vendor export is the production milestone covered here. Vendor-native
model asset preparation, lease renewal, remaining mutation/retention/immutable-
artifact boundaries, serial graph preparation, and original cross-engine gates
remain open, including file-engine descriptor recovery. Full GNU/full
qualification remains waived; this is not live AWS/cloud/TLS qualification.


### Native model artifact identity and planning I/O

Native model identity, managed receipt validation, tokenizer-planning digests,
EmbeddingGemma metadata signatures and KV file identities now lend the existing
synchronous execution grant during supplied-Io access, canonicalization, opens,
stats, reads and legacy digest-memo writes. Allocation, JSON parsing, hashing,
identity comparison and cache publication remain runnable. Receipt/path limits,
contained-symlink checks, checksum verification and generation comparisons stay
in force. Successful opens are owned until checked return; rejected return closes
the descriptor before propagating the original source failure. Read buffers are
fixed at 64 KiB during physical waits; dynamic buffer growth happens after return.

Optional metadata and legacy memo fallbacks recheck the sticky original execution
refusal before treating failed I/O as absent data. A failed memo write may remain
an optional optimization failure, but rejected CPU return cannot publish the
in-memory digest cache. KV identity now snapshots and publishes under its short
cache mutex while hashing outside it. The existing cancellable execution lane
coordinates cold validators; canceled validation releases the lane so another
caller can retry. Cache/model mutexes in these audited identity paths are released
before filesystem operations and checked CPU reentry.

The real API-owner scheduler fixture loads a managed receipt and verifies its
actual artifact checksum. It checks query/control progress at every loan, restores
the original two-slot grant on success, and rejects every physical return in
separate runs. Each run recovers the process descriptor count, retires retained
bytes and releases its root. The fixture uses a real temporary artifact; it does
not load a downloaded model or exercise cloud storage. Native ModelManager tests
call the production model-identity, tokenizer-planning and KV identity paths over
multi-buffer fixture files. Every physical return can be refused, with original
error replay and no rejected identity cache publication. Loaded KV digest-cache
cases also assert the cache mutex is available at every physical wait, release
the validation lane on every refusal, retry successfully, and reuse the warm
digest. No backend session is constructed by that cache fixture. These deterministic
refusal tests supplement the real scheduler-owner integration.

Evidence: `artifact-preparation-v4-final.json` records the focused native suite,
actual API-owner file/receipt integration and production worker semantic check.
All 33 selected native tests passed without skips; the build completed all 32
steps. The Apache product boundary and staged license-header checks pass.
`artifact-preparation-final.json` binds the tested input hashes to the committed
source snapshots. The filters are compile-time filters, avoiding an unrelated
full native/GNU qualification build.

The milestone covers native artifact identity and planning I/O. It does not prove
that mapped-page faults, vendor framework preparation, GPU construction or every
model registry/download operation lend capacity. Those paths still require
explicit audit and qualification. Lease renewal, remaining maintenance provider
boundaries, serial graph preparation and original cross-engine gates (including
file-engine descriptor recovery) remain open. Full GNU/full qualification remains
waived, and the installed CLI is not rebuilt by the focused checks.


### Joined held-work-lease renewal

Ordinary and bootstrap held work leases now start one physically joined renewal
worker before their builder/compactor/enricher proceeds. The worker is reserved
under the original maintenance root, retains its original clock/cancellation
borrow, releases CPU between attempts, and uses one control slot when renewal is
due. A startup handoff prevents an activation race with the parent's CPU team.
The parent regains its original class and width before production work resumes.
Release stops and joins renewal before releasing the durable fencing token.
Progress-store provider views carry their original request context into held
leases. Convenience build calls therefore inherit the caller's execution, Io,
deadline and authority even without an explicit maintenance-token argument.
Existing explicit maintenance controls compose with that provider context.

A held-lease maintenance checkpoint now reads published refusal and lease
lifetime rather than performing provider I/O. Cached lease checks retain the
acquisition's supplied wall clock plus a suspend-inclusive authority deadline;
request clocks and deadlines are inherited without renewal. Model watchdogs and graph workers
therefore do not renew work leases or borrow the invocation owner's CPU grant.
Explicit publication renewal and periodic renewal serialize through the same
lease-local lane. Contenders lend their own capacity while waiting; checks do
not take that lane. A takeover, source cancellation or original deadline remains
terminal and cannot authorize publication with a new lease or a renewed request
budget.

Object-backed renewal forwards the operation's execution context through the
existing physical provider boundaries, including filesystem, S3 and GCS client
adapters. Native filesystem renewal uses the supplied Io, the existing namespace
file lock, the exact owner/token check, atomic HEAD replacement and directory
sync. It lends physical file waits while JSON, allocation and fencing remain
runnable. The namespace file lock serializes native renewal with legacy writers;
no process-local metadata cache mutex is held across its I/O.

This required an explicit protected-control child capability, rather than
promoting ordinary children after publication. Runnable ABI 22 adds the checked
control-child reservation flag; legacy prefixes and malformed flags fail before
worker IO is borrowed. The original root remains alive through every child join.
Control children and cleanup together cannot consume the last control registry
slot or more than half of control retained-byte capacity. Both opaque owner and
native-borrow forwarding preserve that reservation and its original lifetime.
The SQL completion adapter also enforces its existing one-slot coordinator rule
before reserving completion children; ordinary producer team reservations keep
their existing path. Public publication routes report exhausted scheduler/control
capacity as HTTP 503 with a one-second retry hint.

The production fixture covers native and object-backed filesystem stores,
ordinary/bootstrap leases, absolute/relative native roots, success, cancellation,
original deadline and durable takeover. It executes real reads, conditional metadata updates, fsync and rename,
with a deterministic barrier at a sync boundary to hold physical I/O. While that
barrier is held, query and health work make progress and repeated lease checks
perform no new provider I/O. Publication revalidates the exact token; every case
joins renewal and retires its root/retained bytes. This is real local-provider
integration with injected waiting, not cloud or slow-disk performance evidence.

Evidence: `lease-renewal-v11-final.json` records affected serverless publication,
scheduler cleanup, control-owner/borrow/ABI, SQL completion, real worker-pipe and
worker semantic checks. `lease-renewal-v12-clock.json` adds the supplied-clock
fixture, shifted one day behind the host clock, across all sixteen provider/
lease/outcome combinations; all production source hashes match the broader run.
The only subsequent Zig change is the clock fixture itself. Both runs pass with
unchanged inputs. `lease-renewal-final.json` binds tested bytes to committed
snapshots. Staged licensing and the Apache product boundary checks pass.

Manifest read-pin renewal and external lake snapshot heartbeats remain separate
open lease paths. Initial lease acquisition/release and remaining maintenance
mutation/retention/immutable-artifact provider boundaries still need their full
scheduling audit. Vendor framework/registry download preparation, serial graph
preparation and original cross-engine recovery gates (including file-engine
FD recovery) remain open. Full GNU/full qualification remains waived.


### Maintenance manifest source pins (2026-10-29)

Enrichment and graph publication now hold joined source-pin scopes. Their
model/watchdog/parallel checks only inspect cached authority and sticky failures;
they never renew a pin or mutate the owner's CPU grant. A work-lease renewal
worker accepts bounded, intrusive source registrations, so nested graph scopes
share its protected control child. A scope without a parent renewal worker owns
one and propagates it to its children. Detachment removes a registration before
joining its in-flight callback; the callback completes its event publication
under the registry lock before the scope can retire.

Renewal checks both existing Unix and suspend-inclusive source deadlines before
and after provider I/O. The retirement floor still brackets durable monotonic pin
publication. A retired version may use its existing rights, but cannot renew
those rights; one source's refusal does not poison independent registered readers.
Native filesystem pin reads/CAS use supplied Io, bounded numeric reads, namespace
file locking, atomic replacement and file/directory sync. Object-backed pin
reads/CAS use the existing physical provider adapters and conditional ETags.
Formatting, allocation and parsing remain runnable. Ordinary native CAS callers
reserve return CPU while retaining the namespace lock; renewal children use
their already protected control return lane.

The model-invocation integration exposed a scheduler dependency: protected
control children must not prevent the parent's ordinary CPU team from resizing.
Per-parent control-child accounting permits that resize while ordinary children
still require a join. Original lifetime, byte ownership and parent retirement
checks remain in force. Synchronous unadmitted callers whose Io cannot create
concurrent tasks use a joined local timer executor; original source cancellation
and physical provider Io remain authoritative. Admitted callers never take that
fallback.

Focused verification uses real native and object-backed filesystem providers
with deterministic sync barriers. It covers passive checks, unrelated query and
health progress, cancellation, expiry during I/O, physical callback detachment,
eight nested source scopes, retirement and independent-reader failure isolation.
Normal builder, compactor and enrichment callers are exercised separately,
including native and opaque model teams and durable WAL checks. This is local
provider integration, not live-cloud or slow-disk performance qualification.

The broader read-protection milestone remains open: external lake snapshot
heartbeats still need shared admitted renewal and joined cleanup. Query/catalog
read-pin acquisition paths, initial work-lease acquisition/release, remaining
maintenance provider boundaries and the previously documented cross-engine
recovery gates still need their remaining audits. Full GNU/full qualification
remains waived.

Evidence: `read-pin-renewal-v7-final.json` records the six passing production,
serverless execution, native/opaque owner, SQL team and cleanup targets, with
unchanged source inputs. `read-pin-renewal-v9-unadmitted.json` separately verifies
unadmitted callers through native and object-backed pin acquisition. All
production source hashes match the broad run; only this regression case
was added afterward. `read-pin-renewal-final.json` binds
these source bytes to the committed snapshots. Licensing and Apache boundary
checks pass. No full qualification or live-cloud claim is made.


### Lake snapshot heartbeat authority and provider boundaries (2026-10-29, in progress)

Snapshot-pin acquisition, renewal and retirement now use the existing physical
provider adapters. Allocation, numeric parsing and CAS formatting stay outside
physical object waits. The configured snapshot resolver forwards the original
read context into artifact-store opening, so an expired or refused source fails
before credential/provider initialization.

Owner checks are passive: they inspect the original request/root lifetime,
sticky provider refusal, and cached Unix/suspend-inclusive source deadlines.
Background renewal does not invoke foreground checkpoint or wait hooks. Each
provider attempt is bounded by the minimum of its ten-second ceiling and the
original read deadline. Renewal checks old source rights through every physical
wait and before publishing its new local bounds, so a delayed response cannot
revive an expired reader. Precise cancellation, deadline and retirement errors
survive the heartbeat instead of becoming a generic lease-expired flag. Other
provider failures retain the existing unavailable-reader contract.

This exposed an existing coupling in ServingSource.protectContext: snapshot
protection replaced the caller's additional checkpoint. A separate passive
read-lease hook now preserves both lease protection and the current owner's
policy checkpoint across execution rebinding. Lifetime polling runs the lease
hook; runnable owner checkpoints continue to validate policy.

The real periodic worker is tested against a filesystem object provider with
an injected uncancelable sync barrier. Coverage includes initial acquisition
lending its original query CPU, passive checks during renewal, cancellation,
original expiry, source-authority expiry, retirement and destruction physically
joining the blocked callback. The configured S3 resolver's refusal tests stop
before credential/provider opening; they do not claim a live S3 operation. SQL
retained-cut, catalog/WAL/pin admission and maintenance source-pin tests exercise
the normal surrounding callers.

The lake heartbeat milestone is NOT complete. Periodic heartbeat tasks still
need shared, admitted execution. The next production path is a bounded renewal
service shared across snapshot readers, with per-reader original lifetime checks,
physical callback detachment/join and server shutdown. Its operation/byte limits
must preserve health progress; a control child per snapshot would exhaust the
small protected reserve for ordinary multi-table reads. The current tests prove
source correctness and initial-provider waiting, not admitted periodic CPU or
live-cloud throughput. The remaining full-design and cross-engine recovery gates
listed above remain open; full qualification remains waived.

Evidence: lake-heartbeat-v7-final.json records the focused lake admission,
maintenance source-pin renewal, provider-opening, real text-retirement, SQL scan
startup and physical read-resource targets. lake-heartbeat-final.json binds their
tested source hashes to the committed bytes.
