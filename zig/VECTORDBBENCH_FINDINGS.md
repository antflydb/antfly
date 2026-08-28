# VectorDBBench Findings

This is the working evidence log for the 50K and 1M Antfly VectorDBBench
investigation. Keep benchmark-harness changes separate from product fixes: a
vector-only control should not do full-text work, while normal Antfly users who
combine full-text and vector indexes must still get bounded memory and stable
ingest throughput.

## Benchmark contract

The representative runs use the upstream VectorDBBench runner through Antfly's
public `/db/v1` HTTP API, one shard, four concurrent load workers, batches of
100, packed little-endian float32 vectors, and `sync_level=write`. Readiness is
measured by polling the public dense-index status until all expected vectors are
published. This exercises the public server, table lifecycle, primary store,
replay journal, asynchronous derived-index worker, and dense HBC index.

`sync_level=write` does not wait for full-text or dense-index completion. It is
not `full_text` or `full_index` synchronization.

The provisioned dense-ingest guardrail is useful for repeatable internal
regressions, but it is not equivalent to VectorDBBench: it bypasses HTTP, uses a
single producer, and originally generated sorted keys. Its throughput must not
be reported as the end-to-end result.

## Confirmed findings

### The public create-table default adds unrelated full-text work

Creating a table with `{"num_shards":1}` automatically creates
`full_text_index_v0`. Although `sync_level=write` does not wait for it, that
index consumes replay records and performs full-text indexing asynchronously
during the vector load.

The Antfly VectorDBBench adapter now deletes `full_text_index_v0` through the
public API before it creates the external dense index. This is the default for
ANN benchmark comparability; set `ANTFLY_VDBBENCH_KEEP_DEFAULT_FULL_TEXT=1` for
an explicit mixed-index diagnostic.

This is benchmark isolation, not the product fix for mixed workloads.

### Type erasure silently changed point probes into snapshot reads

The runtime LSM deliberately has two read contracts. A snapshot read clones
the mutable generation so a multi-operation transaction remains stable across
concurrent writes. A point probe reads the current tip without cloning it.

`DocStore.get` incorrectly opened a snapshot for a single copied value and now
uses a point probe. More importantly, two generic storage adapters erased the
optional probe operation:

- `DocStore.backendStore()` did not advertise the store's `beginProbe` or
  `beginCurrentScan` operations. Transaction metadata lookups that requested a
  probe therefore fell back to `beginRead`.
- The namespace-erased store used by dense HBC indexing did not advertise
  `beginProbe`. HBC's `beginProbeOrRead` therefore also fell back to a full
  snapshot. Preserving this operation is a valid general fix, but the 50K A/B
  below showed that it was not the source of the remaining primary-store clone
  volume.

These are product bugs rather than benchmark-specific paths: any workload that
uses the generic transaction or namespaced index adapters can pay for repeated
whole-mutable-state copies.

A primary-only public-API control makes the first bug unambiguous. Before the
adapter fix, inserting 5,000 official shuffled rows took 0.951 s and issued 208
bound-read snapshot clones totaling 1,096,456,770 bytes. With probe/current-scan
operations preserved, the same control took 0.744 s and all clone classes
combined fell to 15 calls and 27,613,796 bytes: 97.5% less copied data and about
22% less elapsed time.

The first fix alone was not sufficient for dense indexing. One official 50K
vector-only run still issued 694 bound-read clones totaling 5.77 GB. Propagating
namespace probes improved readiness but did not reduce the clone counter: the
next run issued 725 bound-read clones totaling 5.63 GB. That negative result
localized the remaining cost back to the primary store.

The primary hot path was transaction intent resolution. A normal public batch
collects an intent prefix, validates its atomic revision, and resolves it under
the DB apply lock. The generic prefix helper nevertheless opened a stable read
snapshot for each scan, cloning unrelated table state.

The first replacement used a current-tip cursor. It reduced bound-read copies
to 29.8 MB in a 50K run, but a correct linear cursor had to retain the backend
mutex while walking mutable state. That serialized writers and regressed load
time to 69.95 s, so this implementation was rejected rather than hidden behind
a benchmark switch.

New transaction revisions now publish a durable, sorted intent-key manifest in
the same backend batch as their intents and transaction record. Collection and
resolution use current-tip point reads for those exact keys, avoiding both a
whole-table snapshot and a long-held scan lock. An in-flight transaction from
an older binary falls back to one stable prefix scan; its next intent write
publishes a complete manifest, preserving rolling-upgrade compatibility. The
full set of 108 transaction-related tests passes, including public HTTP,
restart, recovery, distributed coordination, transforms, and idempotent retry
cases.

The first manifest production run exposed a second lifecycle bug: resolution
deleted the manifest but retained the terminal transaction record. The recovery
worker revisited those records, could not distinguish them from pre-manifest
in-flight transactions, and repeatedly took the compatibility snapshot. An
LLDB trace captured the exact stack as `runRecoveryPageWithConfig` ->
`TxnManager.hasIntents` -> `backend_scan.scanPrefix`. That run issued 337 bound
snapshots totaling 2.39 GB. Resolution now retains an explicit four-byte empty
manifest until terminal metadata cleanup, so recovery uses the point-read path
without weakening legacy recovery.

The clone-byte figures are cumulative allocation/copy work, not simultaneous
RSS. Peak single-clone size in the diagnostic runs was about 20--22 MiB.

The accepted follow-up keeps that compatibility while batching ordinary intent
lookups with sorted `getMany` calls. The erased storage interfaces now preserve
the sorted multi-get operation too, and LSM transactions merge their private
overlay with current committed values without manufacturing a read snapshot.
This benefits normal multi-document writes and recovery, not just this load.

### Raising the WAL checkpoint floor alone is not a cloning fix

The primary WAL can be larger than the mutable table state because a document
write also persists replay metadata and embedding artifacts. A 1 MiB adaptive
checkpoint floor was associated with many small flushes and growing L0 debt, so
a 32 MiB floor aligned with the normal primary flush window is under test.

That change alone lets mutable state remain larger for longer and therefore can
make each unnecessary snapshot clone larger. It must be evaluated only after
removing the point-read cloning path. The floor is not accepted based on the
initial A/B observation alone.

### Sorted direct ingest is not representative of the load

VectorDBBench reads `shuffle_train.parquet`; request keys are not globally
sorted. The primary LSM reported direct-bulk attempts but zero direct-bulk
successes in the measured runs, with fallbacks split between a non-empty
backend and batches below the direct-ingest threshold. Optimizing only the
sorted synthetic guardrail would be a benchmark-specific shortcut.

### Dense replay memory estimates must use the configured dimension

The replay admission estimate previously assumed a 384-dimensional vector.
OpenAI 50K uses 1,536 dimensions and Cohere 1M uses 768, so the estimate could
under-reserve replay memory by 4x or 2x. Managed dense index references now
carry the actual configured vector-byte estimate, with the old fallback only
for callers that lack dimension metadata.

### Dense replay had repeated scalar reloads and unbounded finish work

Several independent HBC paths turned a logically batched replay window back
into per-vector storage traffic:

- identity and vector-id mappings were fetched one at a time through erased
  stores;
- leaf split range metadata was read through one point transaction per member;
- split and quantized-refresh work reloaded transformed vectors one at a time,
  including vectors that were already present in the active batch;
- quantized rebuild and leaf splitting could be deferred to an outer bulk
  finish, concentrating a large hidden tail in one publish operation.

The product paths now preserve sorted multi-get through both erased interfaces,
load split metadata and transformed vectors in batches, reuse the matrix that a
split already materialized, and keep live replay maintenance inside bounded
publish windows. The offline outer-finish mode remains available explicitly;
it is no longer the live public-write default.

### Replay and compaction need separate admission lanes

Deferring every compaction while a dense replay worker is active protects replay
latency but lets the primary L0 grow until public writers must compact in the
foreground. Conversely, letting primary maintenance consume all shared capacity
can starve the derived worker and inflate the replay journal. Resource admission
now distinguishes replay-priority work from soft background compaction, and the
derived backlog tracker applies a bounded 16-sequence drain window (high water
200, resume at 100) instead of allowing an unbounded wait.

The completed 50K and 1M runs show that this keeps replay lag bounded and
eliminates the old clone explosion. The 1M investigation exposed a separate
general LSM bug: persisted compaction releases the backend lock while building
its output, and concurrent flushes prepend newer L0 runs. Publication compared
the original positional slices, so it discarded valid completed work merely
because the same immutable input IDs had shifted right. Continuous traffic
could therefore starve compaction until ingestion stopped.

Publication now relocates the exact immutable input IDs and recomputes the
target-level overlap closure against the current run version. It accepts a
pure positional shift, but still rejects output if an input disappeared or a
new overlapping target run makes the plan genuinely stale. Focused tests cover
both cases. This is a storage-engine correctness/progress fix, not a benchmark
batch-size shortcut.

### The public write coalescer could starve its elected request

The coalescer elected one HTTP request as queue drainer. Under a continuously
replenished four-worker queue it kept that request inside the drain loop even
after its own entry had committed. Other requests continued to make progress,
but the elected public handler hit the client's exact 120-second timeout. This
was a public-API fairness bug, not an HBC timeout.

The drainer now hands ownership to a waiting request as soon as its own entry is
complete. A controlled concurrency test proves that the old owner returns while
a successor is still blocked. The 1M run with this change crossed the former
failure boundary without a timeout while dense replay remained within a few
hundred sequences of the source.

### Compile reservations should describe reality, not require `-j1`

ReleaseFast compilation remains normally parallel. Scheduler reservations now
cover observed macOS peaks for the API (11 GiB), storage/distributed runtime (18 GiB),
inference runtime (16 GiB), and CLI (3 GiB), plus 12 GiB for the broad data
runtime test and 14 GiB for the metadata public simulation. These are scheduling
claims, not process memory limits; they let the build graph overlap roots that
fit without requiring a global `-j1` workaround.

### Replay readers should snapshot only their append-only lane

The public table API creates `full_text_index_v0` by default. `sync_level=write`
does not wait for that index, but its replay worker still runs and previously
opened a broad current-scan transaction on every wake. That transaction cloned
the complete mutable primary state even though the worker only needed one
append-only replay-key range. At 50K with both full-text and dense indexing,
this produced 3.51 GB of cumulative copying across 296 calls.

The runtime LSM now snapshots only the requested mutable replay lane and pins
the immutable/run generation at the same backend-lock linearization point. The
merge cursor, tombstone handling, ordering, callback lifetime, and generic
backend fallback remain unchanged. Exact replay visibility also uses the
bounded all-lane iterator and compares the returned sequence; it does not infer
that sequence gaps are visible. A legacy store without replay lanes retains the
old current-scan fallback.

With both default full-text and dense indexing enabled, the final current-main
50K check copied 46.3 MB rather than 3.51 GB (about 76x less), completed in
41.78 seconds rather than 46.32 seconds, and published all 50,001 full-text
documents and all 50,000 vectors. This removes unrelated copying from the
general default-index product path; it is not a VectorDBBench-only full-text
disable.

### A lower checkpoint floor remains counterproductive

After the lane snapshot fix removed the original confounder, a controlled 50K
vector-only A/B compared the 32 MiB product default with a 1 MiB floor. The
32 MiB build completed in 38.35 seconds with 59 final L0 runs. The 1 MiB build
took 41.75 seconds, produced 147 final L0 runs and 86 flushes, and had the same
roughly 591 MB physical-footprint peak. It reduced cumulative clone bytes from
about 107 MB to 81 MB, but increased rotations and flush fragmentation. The
32 MiB default is retained.

### Compaction needs job-level high-water telemetry

Cumulative compaction time did not reveal whether maintenance consisted of
many bounded jobs or one user-visible latency and working-set spike. The LSM
now reports completed-job count, input/output byte totals, and the largest
completed input, output, and duration. Aggregation adds counters while
preserving maxima across primary and derived backends.

The instrumented 1M runs completed with full query visibility and zero final
hard debt, but observed largest jobs of 2.57--2.99 GB lasting 52.6--63.9
seconds. The 2.99 GB job exceeded the configured 2 GiB target through the
intentional oversized-single-job progress escape hatch. Simply changing the
numeric cap to 512 MiB would not bound this case: a single broad L0 source can
overlap several gigabytes of target-level runs, and rejecting the minimum
overlap-closed plan would reintroduce compaction starvation. The next design
experiment must make those closures partition-aware while retaining the
oversized escape hatch for guaranteed progress.

### Smaller leveled files do not split an overlap-closed job

A follow-up set the primary LSM's preferred run-file size to its 128 MiB
base-level target while retaining the separate 512 MiB physical admission
limit. This was deliberately a layout preference rather than a smaller maximum
record size. It produced the intended finer leveled geometry: after shutdown,
the primary L2 contained 27 runs with a 136.7 MB largest physical file instead
of a handful of roughly 512 MB files.

That geometry did not bound compaction. The 1M public-API lifecycle still
admitted 14 oversized selections, and its largest overlap-closed job consumed
2.562 GB of input, produced 2.454 GB of output, and lasted 55.15 seconds. The
run completed with all 1,000,000 vectors query-visible and zero final hard debt,
but took 1,753.77 seconds, rewrote 27.70 GB across 36 jobs, and peaked at
3.72 GB RSS / 1.50 GB process physical footprint. Those are regressions from
the 1,617.08-second, 3.19 GB RSS / 1.30 GB control, while the maximum job is
effectively unchanged from 2.57 GB. The 50K gate was also slightly slower at
43.97 seconds versus the current-main 38.72--43.14-second range.

The preference is therefore rejected. Splitting a completed compaction into
smaller output files does not split its input closure: a broad L0 range still
expands through overlapping target runs and older L0 runs before planning can
apply its byte budget. The next safe experiment must narrow persisted L0 source
ranges themselves (without workload-specific key assumptions), then combine
that with a lower soft input budget. The oversized-single-job escape remains
necessary for a minimum correct closure that cannot be divided.

The same run isolated a separate late-load pause. Status publication stopped
for about 84 seconds near 785K rows and then recovered without a client timeout.
A live stack sample showed the elected primary writer waiting in bounded
derived-backlog admission, the other public writers waiting for that group
operation, and the dense worker normalizing and splitting an oversized HBC
leaf. This was not the primary compaction above. It is one diagnostic sample,
not yet a product-change justification, but it shows that dense structural work
can consume most of the public client's 120-second timeout budget even while
the replay/backpressure invariants behave as designed.

### External indexes need an external-coverage admission fence

The public table API writes a readiness document before it creates the
caller-populated dense index. Managed admission previously treated any live
source document as proof that the new index required a full source rebuild.
That is correct for projected and generated indexes, but false for an
`external: true` dense or sparse index: unrelated source documents cannot
produce caller-owned vector artifacts. The single readiness row therefore
caused three unnecessary durable-repair attempts and added about 36 seconds to
the otherwise vector-only 50K lifecycle.

Managed admission now recognizes the external-coverage contract. It installs
the index at an activation fence only after a streaming key scan proves that no
matching caller-owned artifact predates the catalog entry, initializes its
artifact counter, and lets ordinary post-admission replay populate new
artifacts. If matching artifacts already exist, admission retains the durable
rebuild path. The public API still provides end-to-end durability and
query-visibility checks; this does not bypass replay or weaken `full_index`
synchronization.

The 50K public-API A/B completed in 42.08 seconds (34.99 seconds inserting and
7.08 seconds catching up), versus 77.90 seconds before the fix. It published
all 50,000 vectors with zero repair runs or attempts. Peak RSS was 1.42 GB,
attributable demand was 974.7 MB, and cumulative snapshot copying was 177.95 MB
across 42 calls at final status. This restores the established 30--40-second
band within normal single-run host variation while removing work that was
incorrect for every externally populated index, not just this benchmark.

### Smaller HBC replay batches trade away too much throughput

Two follow-ups tested whether bounding dense HBC apply size would reduce the
late-load leaf-normalization pause. Lowering the existing replay-window policy
to 4,096 items completed the 50K lifecycle in 92.41 seconds. It fragmented the
same source stream into 27 replay finalizations and was rejected.

A narrower prototype retained the large replay transaction but split only
known-new, insert-only HBC applies into 4,096-item calls. It lowered peak RSS
from 1.42 GB to 1.11 GB and the largest measured HBC apply from 2.35 seconds to
1.62 seconds, but increased the lifecycle to 46.45 seconds and increased HBC
finalization work. The prototype is also rejected: partial rollback across
chunks is more complex, and a roughly 10% throughput regression is not a good
general default for this memory reduction. A future HBC change should make
leaf normalization incrementally publishable or schedulable while preserving
one replay transaction, rather than fragmenting replay or its apply operation.

### Foreground pressure must honor the compaction-input budget

Background compaction selection honored `max_compaction_input_bytes`, but the
hard write-pressure path invoked an unbounded L0 selector. This made the option
ineffective precisely when L0 crossed its hard limit and public writes were
most exposed to compaction latency. The pressure path now uses the same input
budget while preserving its direct, scheduler-independent progress lane and
the oversized minimum-closure escape. Tests cover a fitting bounded window,
explicit no-oversize overload, and minimum-job progress when no correct closure
fits.

A 50K public-API gate with a 768 MiB experimental budget completed in 42.23
seconds (35.53 seconds inserting and 6.70 seconds catching up), essentially
unchanged from the 42.08-second external-admission result. Its one pressure job
was 235.8 MB, all 50,000 vectors were visible, and there was no overload.

The 1M lifecycle rejected 768 MiB as the product default. It completed with all
1,000,000 vectors visible and zero final hard debt, but took 1,854.21 seconds
(1,830.20 seconds inserting plus 24.01 seconds catching up). Twenty pressure
events completed 21 steps without overload, yet the smaller windows rewrote
27.08 GB and a late indivisible closure still reached 2.753 GB / 58.5 seconds.
Peak RSS was 3.64 GB, the process physical-footprint ledger peaked at 1.40 GB,
and cumulative mutable-snapshot copies were 1.96 GB. The experiment therefore
keeps the foreground-budget correctness fix but restores the 2 GiB product
default; lowering the number alone increases rewrite frequency without solving
broad shuffled-key L0 ranges.

The 2 GiB/default-budget 50K gate completed in 41.63 seconds (32.50 seconds
inserting plus 9.13 seconds catching up). It did not reach hard pressure, all
50,000 vectors were published and query-visible, and conservative attributable
demand peaked at 1.31 GB.

The isolated 2 GiB/default-budget 1M control retained the option fix and
completed in 1,603.35 seconds (1,545.39 seconds inserting plus 57.95 seconds
catching up). All 1,000,000 vectors were published and query-visible, replay reached
10,001/10,001, and repair remained clean. Eighteen pressure events completed 19
steps with no overload and left 57 L0 runs / zero hard debt. Compactions
consumed 29.31 GB and produced 22.48 GB; the largest minimum closure was still
2.599 GB / 51.60 seconds because the oversized progress escape correctly
admits an indivisible closure. Mutable-snapshot copies totaled 1.81 GB. Peak
RSS was 5.63 GB including reclaimable file cache, while the process physical-
footprint ledger peaked at 896 MB and conservative attributable demand at 1.90
GB. This is within the timing band of the 1,617.08-second instrumented control,
but makes the documented input-budget policy effective under hard pressure and
preserves liveness without changing the general default.

## Measurements

Unless a row explicitly reports a mean and range, the times below are
one-machine diagnostics rather than publication-grade means.

### Posting segment/WAL query gate

The earlier `spfresh-v2` diagnostic established that write throughput alone is
not a valid acceptance criterion. On a synthetic 5K-by-1536 public-shaped
workload, the first lazy segment implementation improved ingest from 377.6 ms
to 244.8 ms and reduced measured writes from 671.6 MB to 303.1 MB, but query
p50 regressed from about 0.32 ms to 6.65 ms and recall changed. Publishing a
coherent posting base, centroid directory, and RaBitQ checkpoint removed the
tail replay and restored query behavior (p50 0.309 ms, p95 0.422 ms, recall
0.188 versus 0.190), but also gave back nearly all of the write gain (368.9 ms
and 660.4 MB). The next design therefore uses an immutable packed checkpoint
plus a bounded committed WAL overlay; an unbounded lazy base/delta tail is
rejected even when its load number is attractive.

The current-main internal HBC reference now measures the same read invariants
before and after reopen. For 5,000 synthetic 1,536-dimensional vectors loaded
in batches of 100, one sample reported:

| Phase | p50 | p95 | p99 | QPS | sampled recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| immediately after ingest | 0.305 ms | 1.548 ms | 2.718 ms | 1,887 | 0.680 |
| after close/reopen, warm | 0.444 ms | 3.082 ms | 4.070 ms | 1,052 | 0.680 |

The internal online-coalesced build took 1.495 seconds (3,343 vectors/s) and
reported 222.9 MB written. This is not the public HTTP VectorDBBench load path
and must not be compared directly with the 50K lifecycle times below; it is a
fast query/recall regression gate for storage-format experiments. The cold
first query after reopen was 14.7 ms, which is tracked separately from the
warm distribution rather than hidden in an average.

A closer `public_ingest` arm now uses the derived replay path's external-vector
loader, known-new/coalesced batches, deferred RaBitQ rebuild policy, and one
bulk publication session. Three current-main 5K-by-1536 samples averaged
233.3 ms to build (21.4K vectors/s; 229.5--238.5 ms range) and 33.74 MB written.
Recall@10 was 0.670 in every before/after-reopen sample. After reopen, warm
query p50 averaged 0.340 ms, p95 1.405 ms, p99 1.756 ms, and throughput about
1,920 QPS. Immediately after ingest, p50 averaged 0.339 ms and p95 1.597 ms.
This is the matched internal gate for future storage arms; public HTTP E2E
latency and the real VectorDBBench corpus remain required before promotion.

The first current-main shadow checkpoint experiment exported the final packed
node and RaBitQ values after that same 5K public-ingest build. Fifty-six live
postings occupied a 2.098 MB immutable segment, versus 33.741 MB written while
building through the LSM (about 16.1x less final live payload than cumulative
write traffic). Three checkpoints took 9.69--10.13 ms and segment admission
took 7 microseconds. This does not yet measure the cost of appending/fsyncing a
WAL or replace the LSM at runtime, but it confirms that checkpoint generation
is small relative to the roughly 230--245 ms build.

The first reader recomputed CRC32 on every point access and measured about 52
microseconds p50, which would be unacceptable across many postings per query.
The format reader now keeps atomic per-entry verification state: the first
base-plus-RaBitQ verification was about 58 microseconds p50 in a follow-up,
while already-verified point lookups were below the benchmark clock's
1-microsecond resolution at p95 and 1 microsecond at p99. Verification failures
are memoized as well as successes. The runtime design should either preverify
hot/pinned postings or let their first cache admission pay this cost once; it
must never checksum an unchanged payload on every query.

The public qualification harness now keeps query behavior in the same evidence
bundle as load and memory. A current-main 50K vector-only control inserted in
28.88 seconds and became ready in 37.55 seconds. Its live public-API serial
search measured 98.49% recall@100 with 2.5 ms p50, 4.2 ms p95, and 9.7 ms p99.
The short concurrency curve peaked at 481 QPS with four clients; sixteen
clients were already saturated at 356 QPS and 452.9 ms p95. After a clean
restart, recall was 98.06%, while cold-cache latency rose to 3.9 ms p50,
9.8 ms p95, and 412.6 ms p99. The process was fully caught up by the final
status sample. Segment/WAL candidates must therefore preserve recall and beat
both live and post-reopen latency distributions, not only the load timer.

The first durable posting-store layer now publishes in the order immutable
segment, empty next-generation WAL, then checksummed `CURRENT`; old artifacts
are deleted only after `CURRENT` is durable. WAL appends require an initial
checkpoint, expose only complete committed batches, poison an ambiguous writer
after an append/sync error, and atomically discard incomplete or uncommitted
tails before accepting another append after reopen. This is storage-level
plumbing only: HBC still uses the LSM until transaction-aware runtime wiring can
prove that a checkpoint and WAL tail cover the same source sequence.

| Run | Insert | Ready/load | Notes |
| --- | ---: | ---: | --- |
| 50K OpenAI, batch 100, four workers, original public path | 46.39 s | 48.42 s | Default full-text index present |
| 50K OpenAI, batch 100, four workers, vector-only, 32 MiB checkpoint-floor experiment | 40.36 s | 42.38 s | Public HTTP; dense ready at 50,000 |
| 50K OpenAI, vector-only, first erased-adapter fix | 35.25 s | 41.29 s | Still 6.10 GB cumulative clones; exposed namespaced-adapter fallback |
| 50K OpenAI, vector-only, namespace probe propagation | 32.28 s | 32.31 s | 5.93 GB cumulative clones remained; disproved namespace path as clone source |
| 50K OpenAI, vector-only, lock-held intent cursor (rejected) | 67.93 s | 69.95 s | Bound-read clones fell to 29.8 MB, but backend lock contention serialized writers |
| 50K OpenAI, vector-only, intent manifest before terminal marker | 63.75 s | 74.41 s | Recovery re-scanned retained terminal records: 337 bound clones / 2.39 GB |
| 50K, bounded clones, first safe candidate | 65.10 s | 87.61 s | 238.7 MB clones; memory fixed but speed regression unacceptable |
| 50K, batched identity/mapping reads | -- | 59.12 s | Removed dense scalar mapping traffic |
| 50K, prefix-compressed batch block reuse | 28.03 s | 36.33 s | 83.7 MB clones; first bounded-memory recovery of the old speed band |
| 50K, HBC routing cache | 32.15 s | 38.46 s | 169 MB clones; 737 MB demand, 1.91 GB RSS |
| 50K, bounded backlog candidate | 31.61 s | 46.15 s | 166 MB clones; replay finish tail still visible |
| 50K, batched quantized refresh | 41.46 s | 43.81 s | 202.8 MB clones; 534 MB demand, 1.72 GB RSS |
| 50K, fair public coalescer | 41.40 s | 48.36 s | 157.2 MB clones; 636 MB demand, 1.79 GB RSS; timing contaminated by concurrent host compilation |
| 50K, fair maintenance + rate-limited sampler | 27.78 s | 29.88 s | 249.0 MB clones; 673 MB demand, 1.89 GB RSS; zero final replay lag |
| 50K, relocated compaction publication | 25.05 s | 27.53 s | 103.7 MB clones; 708.5 MB demand, 1.96 GB RSS; 59 final L0 runs / zero debt |
| 50K, clean merged control, vector-only | 27.14 s | 40.35 s | 168.5 MB clones; 601.9 MB physical footprint, 1.85 GB RSS |
| 50K, replay-lane snapshot, vector-only, current-main three-run mean | 30.73 s | 41.65 s (38.72--43.14 s range) | 92.1 MB mean clones; 531.0 MB mean physical footprint, 1.67 GB mean RSS; complete visibility, no overload |
| 50K, clean merged control, full-text + dense | 41.79 s | 46.32 s | 3.51 GB clones; both indexes ready |
| 50K, replay-lane snapshot, full-text + dense, current main | 32.90 s | 41.78 s | 46.3 MB clones; 702.4 MB physical footprint, 1.66 GB RSS; both indexes ready |
| 50K, replay-lane snapshot, 1 MiB checkpoint floor (rejected) | 28.40 s | 41.75 s | 80.5 MB clones; 147 final L0 runs / 86 flushes; no physical-footprint benefit |
| 50K, 128 MiB preferred primary runs (rejected) | 37.41 s | 43.97 s | Finer output layout; 1.61 GB RSS; slightly slower than the current-main range |
| 50K, external-coverage admission fence | 34.99 s | 42.08 s | Zero repair attempts; 177.95 MB clones, 974.7 MB demand, 1.42 GB RSS |
| 50K, 4,096-item replay windows (rejected) | 55.46 s | 92.41 s | 27 replay finalizations; 769 MB demand, 1.48 GB RSS |
| 50K, internal 4,096-item HBC applies (rejected) | 39.13 s | 46.45 s | 1.11 GB RSS but slower and more complex; prototype reverted |
| 50K, foreground 768 MiB pressure budget | 35.53 s | 42.23 s | One 235.8 MB pressure job; zero overload; 722 MB demand, 1.55 GB RSS |
| 50K, foreground budget wired, 2 GiB default | 32.50 s | 41.63 s | No hard-pressure event; 186.3 MB table clones, 1.31 GB demand, 1.50 GB RSS |
| 1M Cohere, batch 100, four workers, original public path | incomplete | projected about 45–50 min | Throughput fell from about 30.8K docs/min in minute one to about 21.1K docs/min in minute four |
| 1M, bounded clones + fair coalescer control | incomplete at 783,201 rows / 1,867 s | -- | Two exact 120 s timeouts from primary L0 pressure; 3.83 GB clones, 1.40 GB demand, 3.76 GB peak RSS; 200 ms `vmmap` and overlapping compilers contaminate speed |
| 1M, rate-limited sampler + maintenance fair turn | incomplete at 592,001 rows / about 725 s | -- | No timeout, but live dense bulk mode still reached 537 aggregate L0 runs; 1.59 GB clones, 888 MB demand, 3.02 GB peak RSS |
| 1M, primary + dense hard pressure before relocation | 1,536.04 s | 1,644.37 s | 825 final L0 runs / 582 debt; 16 compactions from 26 pressure events, 1.60 GB demand, 3.89 GB RSS |
| 1M, relocated compaction publication | 1,099.85 s | 1,110.66 s | Full E2E completion; 10.81 s catch-up, 111 final L0 runs / zero debt, 1.20 GB demand, 4.21 GB RSS |
| 1M, instrumented replay-lane snapshot | 1,605.87 s | 1,617.08 s | Full E2E completion; 1.65 GB clones, 1.30 GB physical footprint, 3.19 GB RSS; zero debt; 2.57 GB / 52.6 s largest compaction |
| 1M, 128 MiB preferred primary runs (rejected) | 1,751.71 s | 1,753.77 s | 2.07 GB clones, 1.50 GB physical footprint, 3.72 GB RSS; zero debt; 2.562 GB / 55.15 s largest compaction |
| 1M, foreground 768 MiB pressure budget (rejected) | 1,830.20 s | 1,854.21 s | 27.08 GB compaction input; 2.753 GB / 58.5 s largest minimum closure; 1.40 GB ledger, 3.64 GB RSS; zero debt |
| 1M, foreground budget wired, 2 GiB default | 1,545.39 s | 1,603.35 s | 29.31 GB compaction input; 2.599 GB / 51.60 s largest minimum closure; 896 MB ledger, 1.90 GB demand; 57 L0 runs / zero debt |

The original partial 1M run reached about 105K documents with 542 flushes, 212 L0 runs,
and roughly 23.6 GB of cumulative mutable-snapshot clone bytes. Dense HBC work
accounted for only about 21 seconds of the roughly 260-second sample, pointing
to primary-store/replay work as the dominant slowdown.

The later 1M control passed the coalescer's original early starvation boundary,
but was stopped after two later exact 120-second timeouts made a full timing
invalid. At capture it had 783,201 source rows, 773,400 indexed rows, and only
98 replay sequences of lag. The primary had 665 L0 runs / 4.03 GB against a
256-run hard limit, 733 flushes, and nine write-pressure compactions. A sampled
timeout stack showed the maintenance worker inside LSM compaction while public
handlers waited in bounded derived-backlog admission. This distinguishes the
remaining large-compaction latency from the already-fixed coalescer ownership
starvation.

The partial control's 3.83 GB of clone work at 783K rows is about 46x fewer
bytes per row than the original 23.6 GB at 105K, but it is still cumulative
work worth reducing. Its attributable demand peak was 1.40 GB, peak RSS was
3.76 GB, and the conservative demand-plus-host-wired diagnostic was 1.90 GB.

Rate-limiting `vmmap` and adding a fair maintenance turn improved the next 1M
curve materially: it reached 592K rows in about 725 seconds without a timeout,
versus 435K in 592 seconds and 517K in 782 seconds in the prior control. It was
still stopped because aggregate L0 grew to 537 runs / 2.21 GB with 349 runs of
hard-limit debt. The dense HBC LSM remained in bulk transaction mode throughout
live replay, and the default bulk policy suppresses hard write pressure for a
finite offline builder. A continuously replenished public replay stream is not
finite, so the dense profile now keeps bulk coalescing while explicitly
preserving hard L0 enforcement.

Enabling hard pressure for both primary and dense profiles produced the first
complete diagnostic, but did not make every assist productive. It took
1,536.04 seconds to insert and 108.33 seconds to catch up (1,644.37 seconds
total), finishing with 825 aggregate L0 runs / 582 runs of summed hard-limit
debt. Only 16 compactions published across 26 pressure events. This mismatch
led to the unlocked-publication investigation above.

With input-ID relocation, all 24 pressure events in the next full run published
24 compactions, with zero overloads. The aggregate L0 repeatedly crossed the
summed bound by a few runs while compaction was in flight, then recovered under
continued writes; it finished at 111 L0 runs / zero debt. Upstream
VectorDBBench reported 1,099.85 seconds of insertion and 10.81 seconds of public
readiness catch-up, or 1,110.66 seconds (18.51 minutes) total. All 1,000,000
vectors were published and query-visible, with no request timeout. This is
32.5% faster than the immediately preceding full run and 63% faster than the
reported roughly 3,000-second run. The host remained available to unrelated
work, so it is a contended-host result rather than a clean throughput ceiling;
it does not yet establish the hoped-for roughly 13-minute result.

The relocated-publication run peaked at 1.20 GB attributable physical
footprint, 4.21 GB RSS, and 2.31 GB in the separate footprint-plus-host-wired
diagnostic. It copied 2.09 GB of mutable snapshots across 324 calls, with a
25.2 MB largest copy. Compared with the preceding full run, attributable demand
fell 25%, clone bytes fell 22.6%, and the dense catch-up tail fell 90%; RSS rose
about 8%, reflecting a larger cache-inclusive peak rather than allocator
demand.

The latest clean bounded-memory 50K result is 27.53 seconds ready, faster than
both the 29.88-second safe result and the earlier 32.31-second clone-heavy
result. It copied 103.7 MB rather than roughly 6 GB of mutable state and ended
with zero replay lag and zero L0 hard debt. The recovered result confirms that
the 30--40-second target was conservative; 87 seconds was not redefined as
success.

The follow-up 1M lifecycle is not evidence that 1,617 seconds is the new
expected throughput. A same-code point-visibility variant completed in 1,572
seconds, while the earlier relocated-publication lifecycle completed in 1,111
seconds; both follow-up runs performed roughly 21--29 GB of compaction input
and admitted multi-gigabyte individual jobs. Their dense indexes stayed close
to the source, both finished with complete visibility and zero debt, and the
visibility variants differed by only about 3%. The large run-to-run wall-time
spread therefore remains a compaction scheduling/rewrite-amplification finding,
not a reason to weaken replay correctness or redefine the target upward.

## Immutable posting segment and WAL experiment

The first read-serving prototype now snapshots packed HBC nodes, posting
maintenance state, and RaBitQ checkpoints into one checksummed immutable file.
It publishes that file, an empty next-generation WAL, and a checksummed
`CURRENT` pointer in crash-safe order. Activation requires exact upstream
source-sequence coverage. Any ordinary write disables the sidecar before its
transaction starts, so the initial checkpoint experiment cannot serve stale
derived state.

On the internal public-ingest-shaped 5K x 1,536-dimensional workload, the
segment was 2.10 MB versus 33.74 MB written by the HBC LSM build and took about
19 ms to publish. Across matched 500-query samples, recall@10 remained 0.696.
Warm no-metadata latency was neutral: median p50/p95/p99 was
0.278/0.968/1.601 ms with the segment and 0.277/0.972/1.614 ms with the LSM,
while median throughput was 2,492 versus 2,484 QPS. The median cold first query
fell from 12.41 ms to 6.63 ms, with storage reads falling from 246 to 79 and
bytes from 5.33 MB to 0.49 MB. The remaining reads are source-document metadata
needed by the external vector loader, not HBC node or quantized payloads.

Startup initially read and checksummed the segment twice. Retaining the bytes
from store admission cut median activation from 8.24 ms to 4.18 ms. A later
three-sample run measured about 12.1 ms from the start of HBC reopen through
the first completed sidecar query, less than the LSM control's 12.41 ms query
alone. A mapped or range-backed reader remains worth exploring at larger
segments, but eager single-read admission is already small enough for the 5K
prototype and preserves lazy per-payload checksum verification.

Synthetic committed WAL tails make the next trade-off explicit:

| Tail | WAL bytes | Append time | Activation | Cold query | Warm p50 / p95 / p99 | Warm QPS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 batches | 0 | -- | 4.18 ms median | 6.63 ms median | 0.278 / 0.968 / 1.601 ms median | 2,492 median |
| 100 batches | 3.83 MB | 35.3 ms | 19.3 ms | 2.66 ms | 0.294 / 1.010 / 1.628 ms | 2,415 |
| 1,000 batches | 37.63 MB | 2.15 s | 163.5 ms | 3.19 ms | 0.322 / 1.411 / 2.154 ms | 2,013 |

The tail rows are single diagnostic samples and rewrite one full posting
snapshot per synthetic batch; they are not claimed as public VectorDBBench
throughput. They do show that reopening and validating the segment for every
append was wrong (100 batches originally took 878 ms), that a retained writer
and non-fsync derived appends reduce that to 35 ms, and that an O(1)
posting-kind latest-value index is necessary on the query path. They also show
that a 37 MB tail is already too deep for this 2.1 MB segment: recovery and
warm tail latency both regress substantially.

The next prototype captures the posting ids touched by one authoritative HBC
transaction, waits for that transaction to commit, then snapshots only those
committed postings into one derived-WAL batch. Kind-specific tombstones prevent
a missing posting-maintenance or quantized value from falling back to an older
checkpoint value. Any failure before source commit cancels capture; any failure
after source commit leaves the source journal authoritative, and exact sequence
admission rejects the incomplete sidecar until replay repairs it.

A first three-sample qualification applied 1,000 direct existing-ID reinserts
in ten batches after checkpointing. Each isolated sample produced 499 WAL
records and an 8.52 MB tail. Median total mutation time was 733.35 ms:
673.00 ms in the authoritative HBC apply and 58.56 ms capturing/appending the
derived WAL. Full posting snapshots therefore added about 8.7% over the apply
time and made the tail four times larger than the 2.10 MB checkpoint. This
validated transaction capture but rejected full snapshots as the final
steady-state encoding.

The derived WAL now encodes replacement payloads as checksummed copy/literal
deltas against the checkpoint or preceding replacement. Eight-byte anchors at
a four-byte stride find unchanged packed-array runs even when insertion shifts
the remainder of the payload. Explicit coverage records advance the source
sequence when a committed transaction touches no posting payload, and
kind-specific tombstones prevent fallback to stale values. The sidecar is
admitted only at the exact durable source sequence. A short, corrupt, or
ahead-of-source tail is rejected and rebuilt from the authoritative source
journal. The first policy checkpointed at a 1 MiB tail, at 64 MiB
unconditionally, or when the tail reached half the checkpoint size; the
large-corpus results below supersede that deliberately aggressive prototype.

The direct-reinsert workload is supported internally but is not the public
table replacement contract. The public path atomically deletes the old
assignment and inserts the replacement. That exposed two independent fast-path
problems. Treating transaction-local deletes as proof that all writes were new
let grouped insertion route the complete replacement batch against one
pre-insert topology. Restricting grouped routing to callers that knew the ids
were absent before the transaction preserves the existence-lookup saving
without selecting the topology-sensitive grouped algorithm. Replacement also
rewrote quantized payloads repeatedly as individual delete/insert steps changed
the same postings. The replacement transaction now queues touched postings and
rebuilds their quantized payload once at commit.

Query latency is part of the same qualification, not a separate microbenchmark.
Freshly reopened LSM and freshly reopened segment-plus-WAL reads produced the
same result digest and recall in every sample:

| Reopened query path | Cold first query (median) | Warm p50 / p95 / p99 (median) | Warm QPS (median) | Recall@10 |
| --- | ---: | ---: | ---: | ---: |
| Authoritative HBC LSM | 14.15 ms | 0.268 / 0.683 / 1.141 ms | -- | 0.710 |
| Immutable segment + 4.54 MB raw stress tail | 3.03 ms | 0.267 / 0.692 / 1.221 ms | -- | 0.710 |

The sidecar preserved the exact ranked-result digest—not merely recall within
a tolerance—in every paired LSM/sidecar run. Warm latency remained neutral and
the cold query improved by about 79%. The 51.1 ms activation above deliberately
reopened an oversized raw tail; production would have checkpointed it once it
exceeded half of the 2.36 MB segment.

With one deferred quantized rebuild per replacement transaction, ten
100-vector replacement batches took 266.7 ms in authoritative HBC apply and
125.7 ms in derived capture on the diagnostic 5K x 1,536-dimensional workload.
Default boundary-rerank recall reached 0.710. The same final corpus built fresh
reached 0.675, while forcing a global quantized rebuild cost another 104.8 ms
and regressed recall to 0.628. The updated implementation therefore retains the
existing rerank-policy boundary and avoids a global refresh or a wider
candidate window: neither is justified by quality or latency.

The efficient product design is therefore a packed immutable checkpoint plus
a shallow, buffered derived WAL, not another general-purpose LSM. That design
is now wired through normal HBC lifecycle and replay behind
`ANTFLY_HBC_POSTING_SIDECAR=1`. The primary source journal remains the
durability authority. Only postings touched by a successfully committed source
transaction are captured, and the applied watermark is not published until
derived capture completes. Derived failures never fail a committed source
write: they durably invalidate `CURRENT`, after which ordinary replay repairs
the acceleration. Each query transaction leases one immutable posting
generation. Covered writes leave the last committed generation available,
then atomically publish a delta overlay before their applied watermark becomes
visible; in-flight queries finish on their old generation and new queries use
the new one. The LSM remains authoritative for source rows, vectors, metadata,
and recovery, but normal posting queries after the first checkpoint do not
fall through to the LSM.

### Public API qualification

One ReleaseFast lifecycle for each official VectorDBBench case exercised the
standalone public server, `/db/v1` table API, batch size 100 with four load
workers, visibility catch-up, process restart, cold and warm serial queries,
and Circus physical-footprint sampling. The adapter removed Antfly's default
full-text index before adding the external-vector index, so these results do
not charge unrelated asynchronous text work to HBC. They are diagnostic single
runs, not the three-run publication sample required by the memory methodology.

| Case | Insert | Catch-up | Total ready | Live recall | Live serial p50 / p95 / p99 | Live QPS at 1 / 4 / 16 | Reopened cold p50 / p95 / p99 | Reopened warm p50 / p95 / p99 | Demand / RSS peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| OpenAI 1,536D 50K | 35.63 s | 9.04 s | 44.67 s | 0.9849 | 2.9 / 5.6 / 13.8 ms | 44.58 / 399.82 / 670.94 | 5.3 / 10.3 / 423.7 ms | 4.6 / 6.1 / 422.9 ms | 734 MB / 1.80 GB |
| OpenAI 1,536D 50K, live immutable generations | 34.11 s | 11.19 s | 45.31 s | 0.9846 | 2.6 / 4.5 / 13.2 ms | 44.66 / 482.70 / 683.75 | 4.8 / 9.9 / 412.2 ms | 4.0 / 4.7 / 411.7 ms | 719 MB / 1.87 GB |
| Cohere 768D 1M | 1,579.41 s | 32.21 s | 1,611.63 s | 0.9863 | 50.6 / 617.3 / 664.6 ms | 5.08 / 17.67 / 37.89 | 43.0 / 467.1 / 495.1 ms | 9.0 / 15.6 / 436.3 ms | 1.40 GB / 4.99 GB |

The 50K result recovers the established 30--40 second insert range while
keeping demand bounded. Its reopened recall was 0.9742 in both sidecar and a
same-data sidecar-disabled LSM control, so the 1.07-point live-to-reopened
change belongs to HBC persistence rather than the segment reader. At 1M,
live/cold/warm recall was 0.9863/0.9863/0.9864. The internal paired harness
also produced identical ranked-result digests between reopened LSM and
segment-plus-WAL reads.

The 1M load is about 46% faster than the reported 3,000-second run and finished
with all vectors visible, 16 pressure compactions, and zero hard-limit debt.
It does not meet the hoped-for 13-minute target. Its sawtooth throughput tracks
aggregate LSM pressure: successful compactions repeatedly reduced L0 from near
the hard limit, while the sidecar stopped publishing new generations during
the long steady-state portion. Cumulative mutable snapshot copies reached
1.89 GB, far below the earlier multi-gigabyte cloning observed before 28K rows,
but still identify the remaining general optimization surface.

The original 1M artifact also exposed a lifecycle limitation. Incremental
sidecar maintenance safely invalidated after an HBC write outside a captured
source window; queries fell back to the authoritative LSM, and restart rebuilt
a 175 MB generation before the warm pass. The follow-up implementation gives
every derived batch an order independent from its source watermark, so
maintenance may append multiple batches at the same covered source sequence.
It also classifies posting mutations exactly: projection metadata and raw
vector writes do not invalidate `CURRENT`, while an uncovered packed-posting
write still fails closed before it can commit.

The live-generation 50K follow-up exercised that lifecycle through the public
API without a posting publication or invalidation warning. A delayed sequence
278 persistence callback arrived after a newer sidecar generation in the first
diagnostic attempt; treating that callback as out-of-order had needlessly
invalidated the sidecar. The corrected path records its captured mutations as
another derived batch at the already-covered source epoch and never regresses
source coverage. The clean run kept `CURRENT` through sequence 501, and restart
admitted the same 16 MB segment at exact sequence 501. Focused tests pin an old
generation across publication and prove that a new transaction sees the new
posting bytes while the old transaction remains unchanged.

### Full derived-state segment follow-up

The posting sidecar now contains the complete query-facing derived HBC state,
not only packed postings and quantized payloads. Packed nodes already contain
centroids and child/member topology. The segment adds node split ranges and a
compact vector directory for vector-to-leaf assignments and result metadata.
The checkpoint also retains the stable index configuration needed to reject an
incompatible generation during recovery. Projection watermarks remain in the
authoritative catalog because they have a different mutation lifetime.

The vector directory is an ordered immutable block rather than one generic
segment object per vector. It uses a contiguous value area, a fixed-width
binary-search index, an index checksum, and per-value checksums. This avoids
millions of allocator objects at 1M scale. Raw embeddings deliberately remain
source-owned when HBC has an external vector loader; copying the 1M corpus into
one posting checkpoint would be duplicate storage and a poor mmap/RSS boundary.

Native segment admission now retains a read-only mmap. Checkpoint v2 verifies
the segment header, footer, and complete index without faulting all payload
pages; posting payloads and vector-directory values are verified on first
access. Legacy v1 checkpoints retain whole-file checksum admission. The nested
vector directory does not pay a redundant outer payload scan: its own index is
checked eagerly and the requested leaf/metadata value is checked lazily.
Memory and object-storage implementations safely fall back to owned bytes.

Every read transaction leases one immutable generation. Batch reads for split
ranges, leaf assignments, vectors, and result metadata use that same lease.
Leased queries bypass the process-global metadata cache, which prevents an old
query from observing metadata admitted by a newer generation and lets mmap
pages replace duplicate heap residency. A test pins an old transaction across
publication and verifies that only a new transaction observes the replacement
topology and metadata.

The first public 50K run exposed avoidable capture overhead: tiny leaf and
metadata values performed scalar LSM reads and walked the immutable generation
chain to attempt replacement patches. It inserted in 44.79 seconds and became
ready in 51.60 seconds. Batch-reading all touched vector values once and only
patching large packed/quantized posting families recovered the expected load
rate:

| Public 50K full-derived checkpoint | Insert | Catch-up | Total ready | Snapshot copies | Sidecar | Physical footprint / RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before capture fast path | 44.79 s | 6.81 s | 51.60 s | 133 MB | 19 MB | 829 MB / 1.60 GB during load |
| Batched tiny-value capture | 36.19 s | 13.41 s | 49.61 s | 116 MB | 17 MB + 5.9 MB WAL | 533 MB / 1.80 GB |
| Immutable flatten, half-segment policy | 45.05 s | 9.00 s | 54.05 s | 192 MB | 19 MB | 905 MB / 1.41 GB |
| Immutable flatten, bounded full-segment policy | 41.37 s | 9.01 s | 50.38 s | 164 MB | 19 MB | 576 MB / 1.54 GB |

The capture-only optimized insert reached the 30--40 second target range.
Total-ready time varies with asynchronous catch-up/checkpoint scheduling, so
insert and catch-up are reported separately. All runs reached sequence 501
with all 50,000 vectors query-visible. These are single diagnostic samples;
RSS is cache-inclusive and its high-water need not coincide with the native
physical-footprint ledger high-water.

At 1M, the half-segment policy exposed a repeat-checkpoint scalability issue.
The diagnostic was stopped at 451,500 query-visible rows after about 757
seconds: a fresh authoritative LSM scan during checkpointing temporarily put
replay about 120 source batches behind and pushed the sampler above its memory
target. Subsequent checkpoints now merge the complete leased immutable
directory and WAL overlays, so only generation 1 scans the authoritative LSM.
The first widened policy waited for at least 4 MiB and a tail equal to the
current segment size, with the existing unconditional 64 MiB cap. On 50K this
reduced checkpoint publications from ten to six and recovered 3.67 seconds
versus the half-segment flatten run while ending with an empty WAL. At 1M it
still published 18 full checkpoints before 575K and remained throughput-bound
on synchronous rewrites despite keeping L0 pressure and RSS bounded. A fixed
32 MiB policy reduced publication frequency but still made each full segment
construction and durable write part of replay's foreground critical path.

The current design separates three different bounds instead of treating them
as one compaction threshold:

- Every 32 MiB of WAL growth, live per-batch generations collapse to one
  overlay above the mmap root. If no query or builder leases the chain, owned
  payloads move into the newest map without copying; otherwise a replacement
  immutable overlay is copied and atomically installed, preserving the old
  transaction's view.
- At 128 MiB, one retained immutable generation is flattened and durably
  staged by a background worker. Source replay keeps appending to the current
  WAL while that work runs.
- Publication carries forward the exact committed byte suffix after the
  builder's boundary into a new WAL generation. This is byte-based rather
  than sequence-based because more than one ordered maintenance batch may
  legitimately commit at the same source sequence. Only a 256 MiB emergency
  ceiling can force replay to join a slow builder.

The crash order remains segment, next-generation WAL, then `CURRENT`; a crash
before `CURRENT` can leave only an unreferenced staged segment. The foreground
verifies the committed prefix and suffix before publication, and restart
checks the segment and WAL normally. A persistent background build or storage
failure propagates to the existing fail-closed invalidation path instead of
starting a new full build on every replay callback.

Moving construction to a worker without moving the durable segment write was
not enough: the first public 50K attempt regressed to 53.78 seconds ready. With
the segment staged by the worker, the same gate improved to 46.98 seconds
(33.64 insert plus 13.34 catch-up), 2.01 GB RSS, and 580.5 MB physical
footprint. Separating cheap overlay collapse from durable compaction improved a
load-only sample to 44.58 seconds (36.19 plus 8.39) with 1.98 GB RSS and
556.7 MB physical footprint.

The complete 50K lifecycle took 51.01 seconds (41.90 insert plus 9.11
catch-up) on the contended host. It published generation 1 and then retained a
52 MiB WAL without another full rewrite. Live recall@100 was 0.9837 and serial
p50/p95/p99 was 2.5/3.3/6.6 ms; after restart recall was 0.9839 and latency was
2.4/2.9/6.4 ms. Live QPS at concurrency 1/5/10/20/30/40/60/80 was
104/662/559/631/639/717/879/864; reopened QPS was
138/685/610/689/694/767/1,042/1,006. Restart became write-ready about 1.3
seconds after the public API was reachable. The load-only memory sample is the
cleaner comparison; the full lifecycle's query cache raised the combined
physical-footprint ledger to 1.40 GB while peak RSS was 1.92 GB.

The corresponding 1M lifecycle proves the handoff invariant but rejects this
dual-write implementation as the final ingest design. It took 2,160.94 seconds
to insert and 11.84 seconds to catch up, or 2,172.78 seconds ready. Thirteen
generations published without a forced builder join or a primary write-pressure
event, and every generation after the first was constructed from a retained
immutable generation rather than an LSM scan. The final sidecar was a 237 MiB
segment plus a 37 MiB WAL. Nevertheless, post-commit capture still opened a
primary snapshot to read final values back from the general LSM, producing
2.32 GB of cumulative mutable copying, and the same derived values were still
written to both storage engines. Load-only peak RSS was 4.12 GB and the native
physical-footprint ledger peak was 1.70 GB. These are bounded, but the duplicate
path is slower than the earlier 1,603-second LSM control and therefore rejected.

Live recall@100 was 0.9861. Serial p50/p95/p99 was
30.3/628.4/656.1 ms and QPS at concurrency 1/5/10/20/30/40/60/80 was
5.6/53.8/91.2/99.5/95.6/93.6/80.0/47.8. After restart, recall was 0.9862,
serial p50/p95/p99 was 27.6/519.3/538.2 ms, and the same concurrency curve was
7.1/74.8/119.7/104.8/108.0/128.5/119.6/92.8 QPS. Recall parity holds, but 1M
tail latency remains a separate query-path optimization target; the WAL-backed
mutation work must not trade recall for an attractive load number.

The full-derived query run preserved the existing boundary-rerank policy and
measured recall 0.9842 with serial p50/p95/p99 of 2.5/3.0/6.8 ms. Throughput
was 100 QPS at concurrency 1, 625 at 5, 698 at 20, and 774 at 40. Higher
concurrency crossed public-server admission and produced HTTP 429 responses,
so its nominal 905/985 QPS at 60/80 is not an accepted saturation result.
Restart admitted the mmap generation at exact sequence 501 and served 110 QPS
at concurrency 1 and 695 at concurrency 5 before the external harness was
interrupted. Recall remained within the prior public runs' normal spread.

### WAL-authoritative mutation-store follow-up

The next implementation removes normal query-facing HBC mutation persistence
from the general LSM after one complete immutable generation exists. This is a
storage ownership change, not a benchmark-only omission. The source table and
its replay journal remain authoritative for documents and raw embeddings. The
posting segment plus its ordered WAL become authoritative for the derived HBC
query state: packed nodes, posting-maintenance state, quantized checkpoints,
node split ranges, vector-to-leaf assignments, result metadata, and mutable
topology/count metadata.

The transition is explicit and crash-safe. A new or legacy index first builds
a complete checkpoint from the LSM. The next covered mutation pins that
immutable generation, owns each final value in a transaction-local map, and
stages a sticky authority marker in HBC metadata. Query-facing derived puts and
deletes then bypass the LSM. The source transaction/replay window establishes
its backend durability boundary before one checksummed posting-WAL batch is
fsynced; only after that append succeeds may the source applied watermark
advance. A crash after the posting append but before the applied-watermark
write is a prepared-ahead generation: startup admits it at or above the older
source watermark and idempotent source replay closes the gap.

Read-your-writes no longer requires a post-commit LSM snapshot. Write
transactions resolve the owned mutation map first and their pinned immutable
base second. Committed queries retain one immutable generation lease; an
overlay is installed only after its WAL commit, so old queries finish on the
old view and new queries observe the complete new view. Captured allocations
move into the immutable overlay rather than being copied again. Raw vectors
remain source-owned and use the external loader, avoiding a duplicate
1,536-dimensional or 768-dimensional corpus in the posting format.

Failure behavior changes once the marker commits. An optional pre-transition
sidecar may still invalidate and use the complete LSM. A WAL-authoritative
index may not: missing/corrupt startup state, a mutation outside capture, a
missing live generation, or failure to reinstall a durably appended generation
fails closed and leaves the source watermark unchanged. Ambiguous append
errors reopen and parse the durable prefix: an already committed intended
batch is acknowledged exactly once, an absent batch is retried once on the
repaired tail, and a different batch id is a single-writer violation. The
authority bit is atomic for concurrent query admission and remains effective
after restart even when the rollout environment flag is removed.

Maintenance uses the same store without fabricating source progress. A bounded
posting repair opens its own capture only when no source capture exists and
appends an independently ordered batch at the current covered source sequence.
If it runs inside source replay, it joins that source capture. Compaction is not
part of the foreground commit after a WAL batch is durable: overlays collapse
at 32 MiB, an immutable background checkpoint starts at 128 MiB, and only the
256 MiB recovery-debt ceiling may join the builder. Segment, next-generation
WAL, then `CURRENT` remains the publication order.

Legacy derived LSM rows are intentionally not deleted on the transition. They
are unreachable after the authority marker and removing them eagerly would
manufacture compaction debt during rollout. A rebuild can always regenerate a
complete generation from the source journal; physical legacy-key reclamation
belongs in a separately budgeted migration/compaction pass.

The final public 50K qualification of this WAL-authoritative path used the
standalone `/db/v1` API, batch size 100, four load workers, no default full-text
index, asynchronous replay, the normal boundary rerank policy, live search,
and process restart:

| Public 50K WAL-authoritative | Insert | Catch-up | Total ready | Recall / NDCG | Serial p50 / p95 / p99 | Valid QPS at 1 / 5 / 10 / 20 / 30 | Load demand / RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Live | 38.53 s | 2.27 s | 40.80 s | 0.9860 / 0.9882 | 2.8 / 3.9 / 15.9 ms | 99 / 558 / 382 / 435 / 571 | 879 MB / 1.49 GB |
| Restart at sequence 501 | -- | -- | -- | 0.9863 / 0.9885 | 2.5 / 3.2 / 6.7 ms | 104 / 608 / 350 / 435 / 509 | -- |

All 50,000 vectors were query-visible. The durable posting state was a 629 KB
bootstrap segment plus a 70.3 MB WAL; no derived leaf row for post-transition
vectors existed in the HBC LSM. Concurrency 40 and above crossed public-server
admission and returned HTTP 429, so VectorDBBench's larger nominal QPS values
are rejected-attempt artifacts and are not saturation results.

The first 1M attempt was stopped at about 843,400 visible rows rather than
accepted after `PostingWalMutationOutsideCapture`. Bounded posting maintenance
had just completed, and the next replay window inherited an already-open HBC
streaming session. A guard from the old snapshot/readback design silently
refused to start a new capture whenever LSM session batching was active. The
WAL backend does not need an LSM snapshot, so capture ownership is now
independent of LSM batching while publication remains forbidden until that
session establishes durability.

A second fresh run exposed a separate ownership race at about 562,400 visible
rows. Catch-up startup saw that posting maintenance already owned a capture and
treated `capture already active` as if the new source window owned it. The
maintenance transaction then published and closed its capture before the first
source mutation. Capture plus streaming-session acquisition is now atomic under
the per-index apply mutex, posting maintenance uses the same mutex through WAL
publication, and the only legal nested-capture case requires an already-open
streaming-session lease. An active capture without that lease is an explicit
`PostingWalCaptureOwnershipConflict`, never a silent join.

Focused HBC and index-manager regressions exercise streaming-session-first
ordering, independent-maintenance ownership rejection, legal source-window
nesting, sticky post-restart authority, WAL read-your-writes, abort without
publication, missing-generation fail-closed behavior, and restart recovery
without derived LSM persistence. Both stopped 1M samples are diagnostic only;
a fresh public qualification is required below.

The final fresh public 1M lifecycle completed without a posting-WAL, runtime,
or request failure. It crossed both former failure points and, more
importantly, exercised the contested transition directly: posting maintenance
repaired 73 steps at about 691K, 75 at about 834K, and 76 later in the load;
catch-up continued and published new immutable generations after every event.

| Public 1M WAL-authoritative | Insert | Catch-up | Total ready | Recall / NDCG | Serial p50 / p95 / p99 | Valid QPS at 1 / 5 / 10 / 20 / 30 | Demand / RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Live | 1,767.39 s | 6.72 s | 1,774.12 s | 0.9848 / 0.9869 | 35 / 638 / 660 ms | 3 / 28 / 66 / 86 / 68 | 2.91 GB / 2.59 GB ingestion |
| Restart at sequence 10001 | -- | -- | -- | 0.9851 / 0.9871 | 36 / 562 / 589 ms | 3 / 39 / 77 / 97 / 93 | 2.60 GB / 1.77 GB query |

All 1,000,000 vectors were query-visible. Recall changed by 0.03 percentage
points across restart. The final durable posting state was a 242.5 MB mapped
segment plus a 108.1 MB WAL; the entire table directory was 3.6 GB. The
ingestion physical-footprint ledger peak was 1.90 GB. Concurrency 40 and above
returned HTTP 429 and is excluded. The live query immediately followed primary
LSM compaction; restart improved c5--c30 throughput and tail latency while the
borrowed-WAL loader kept restart RSS well below ingestion RSS.

This is 41% faster than the reported 3,000-second load, but it does **not**
meet the hypothesized 13-minute target and is slower than the earlier 1,612
second full-segment diagnostic. The remaining ingestion cost is now visibly in
the source/embedding LSM rather than duplicated HBC posting persistence: the
final source LSM reported 2.52 GB of cumulative mutable snapshot copies and
3.24 GB of read-snapshot rotations despite only one pressure compaction. That
is the next general Antfly bottleneck; it should not be hidden by weakening
posting-WAL durability or rerank policy.

The follow-up sampler had one incomplete `vmmap` sample while the original
server process was exiting; its RSS fallback made the synthesized
`demand_peak_bytes` invalid. The table therefore reports the complete
physical-footprint ledger high-water (718.7 MB) as demand, plus the independent
cache-inclusive RSS peak (1.87 GB). Reopened recall was 0.9759 in both cold and
warm passes; as with the earlier control, the live-to-reopened change is not
specific to the segment reader.

## Whole-tree native HBC generation

The posting-only phase proved the WAL, recovery, query-generation lease, and
atomic publication protocol, but it was not the intended final experiment.
Keeping topology, node ranges, quantized payloads, result metadata, and the
vector-to-leaf directory in the generic HBC LSM retained its mutable-snapshot
copies and compaction work. The current format therefore extends the same
checksummed generation and WAL transaction across every query-facing HBC
namespace:

- packed tree nodes and topology;
- quantized and non-quantized leaf payloads;
- postings and node ranges;
- vector-to-leaf mappings and result metadata; and
- index metadata including `covered_source_sequence`.

An immutable generation is mmap-backed. Committed WAL transactions form an
in-memory delta over that base and publish as a single leased query generation.
A background checkpoint folds a pinned base plus a bounded WAL prefix into a
new segment, fsyncs it, establishes the next WAL, and atomically replaces
`CURRENT`. The sticky `AUTHORITY` marker makes recovery fail closed: after the
first complete native generation, Antfly never reopens the legacy HBC LSM or
silently treats its older rows as authoritative. The LSM backend and its native
storage owner are detached after activation; only the shared filesystem lease
needed by the segment/WAL store remains.

Source documents and their exact embeddings deliberately remain in the
primary LSM in this experiment. Approximate tree traversal is entirely native,
while the existing boundary-rerank policy still loads exact source embeddings
for its final candidates. Moving or sharing those source artifacts is a
separate ownership/migration decision and was explicitly deferred.

The first whole-tree public 50K lifecycle completed in 46.98 seconds (44.75
seconds insert plus 2.24 seconds catch-up). Live recall/NDCG was
0.9867/0.9888, with serial p50/p95/p99 of 2.7/5.4/12.1 ms; after graceful
restart it was 0.9870/0.9891 and 2.7/3.7/12.7 ms. Peak load RSS was 1.45 GB and
the complete HBC generation was about 20 MB. No dedicated HBC LSM remained.

The first whole-tree public 1M lifecycle completed in 1,859.94 seconds
(1,831.22 seconds insert plus 28.71 seconds catch-up). All rows were visible.
Live recall/NDCG was 0.9868/0.9888 and restart was 0.9869/0.9889. Peak valid
throughput was 87.3 QPS live and 68.4 QPS after restart at concurrency 20.
Serial live p50/p95/p99 was 36.1/653.2/682.9 ms; restart was
39.3/578.8/608.1 ms. Load RSS peaked at 2.78 GB. Native HBC durability occupied
about 236 MB of segment and 48 MB of WAL. Startup selected native authority at
sequence 10001 without replaying an HBC LSM; public write readiness was about
9.6 seconds.

The 1M query profile explains the remaining cold tail: native approximate HBC
traversal is active, but exact boundary rerank reads source embeddings from the
primary LSM, where block decode and `pread` dominate. The ingestion profile
also moved cleanly across the ownership boundary: primary dense-vector loading,
source point-run publication, and primary compaction dominate; generic HBC LSM
scans do not. This is useful separation rather than a claim that the primary
LSM is already optimal.

Large deferred leaf splits initially reloaded exact source vectors even though
the current replay transaction already owned them. The optimized path keeps a
resource-charged rolling append delta, merges it with the immutable leaf's
native non-quantized payload when exact reconstruction is possible, and retains
entries until bounded ring eviction. Updates/replacements conservatively clear
the append-only proof and fall back to the source loader. RaBit payloads are not
exact-reconstructable, so an older leaf prefix still comes from the primary
source while the rolling delta prevents newly appended members from being read
again. The ring is capped at 256 MB. Its constant-cardinality hash index uses
pre-reserved replacement windows and periodic in-place rebuilds so tombstones
cannot cause unbounded capacity growth. Deterministic churn tests require map
cardinality, slot ownership, fallback bounds, and resource charges to stay
exact. This optimization changes neither tree construction nor recall policy.

After bounding hash replacement churn and fixing deferred-node publication so
the bulk boundary cannot re-stage and discard its own writes, a fresh public
50K qualification completed in 42.16 seconds (39.92 seconds insert plus 2.24
seconds catch-up). Live recall/NDCG was 0.9860/0.9882 with serial
p50/p95/p99 of 2.8/5.3/13.2 ms and 1,018 QPS peak. Restart selected native
authority at sequence 501; recall/NDCG was 0.9866/0.9887, serial latency was
2.8/5.1/15.0 ms, and peak throughput was 1,114 QPS. Load-only peak RSS/demand
was 1.63/1.25 GB. The complete live-plus-restart sweep peaked at 2.04 GB RSS
during high-concurrency queries, not ingestion. The final primary LSM reported
253 MB of cumulative mutable snapshot copying; the detached HBC store reported
no generic LSM scan or compaction work.

A fresh whole-tree 1M qualification on the same corrected implementation
completed in 1,711.26 seconds (1,687.23 seconds insert plus 24.03 seconds
catch-up), versus the coworker-reported approximately 3,000 seconds. All
1,000,000 rows were query-visible at sequence 10001 and the process crossed the
previously failing approximately 843K boundary without a capture, publication,
or recovery error. Live recall/NDCG was 0.9854/0.9874 with serial
p50/p95/p99 of 35.2/654.3/691.0 ms and 77.9 peak QPS. Graceful restart selected
native authority without rebuilding; recall/NDCG was 0.9855/0.9875, serial
latency was 44.0/608.4/663.5 ms, and peak throughput was 89.6 QPS. Load RSS
peaked at 3.18 GB and the complete live-plus-restart sweep did not exceed that
RSS. The final immutable HBC generation was approximately 240 MB with a small
active WAL; the primary source LSM occupied approximately 3.51 GB.

The corrected 1M counters make the remaining boundary explicit. Native HBC
cache usage was only approximately 71 MB at load completion and no HBC LSM
existed, while the primary document/exact-embedding LSM accumulated 2.30 GB of
mutable snapshot copying. Applied-sequence publication spent 274.18 seconds in
native HBC WAL/generation publication across 223 flushes; increasingly large
background generation checkpoints remained crash-safe and bounded, but live
tree mutation/publication is now a material fraction of the remaining load
time. Query traversal stays native, while exact boundary rerank still performs
random primary-LSM reads; this preserves recall parity but explains the 1M
cold tail and the sharp throughput optimum near concurrency 20--30.

Profile-guided primary-LSM changes cache the decoded current entry in each
persisted compaction cursor and make adaptive Snappy back off after four
consecutive blocks fail its 12.5% savings floor, periodically reprobe, and
immediately re-enable after a successful probe. Prefix compression remains
enabled for every block and the on-disk codec is unchanged. A public 50K A/B
completed in 43.93 seconds (29.80 seconds insert plus 14.13 seconds catch-up),
with 0.9853/0.9877 live recall/NDCG and 1.30 GB peak RSS. A second diagnostic
run completed in 44.67 seconds (42.19 plus 2.48), demonstrating that foreground
insert versus HBC catch-up attribution is scheduler-sensitive even when total
load is stable. The faster-insert run left 118 primary L0 runs, so compaction
overlapped the immediate live query sweep; restart recovered serial
p50/p95/p99 to 3.1/5.0/36.1 ms with 0.9856/0.9880 recall/NDCG. These runs support
the CPU optimization but do not justify claiming an end-to-end 50K speedup.

Removing the redundant whole-payload CRC from staged-generation publication
while retaining an eager index/footer admission checksum and lazy per-entry
CRCs produced the fastest exact 50K load so far: 36.06 seconds (24.38 seconds
insert plus 11.68 seconds catch-up), with live/restart recall of 0.9851/0.9858.
The immediate query sweep was noisy (live serial p50/p95/p99
5.2/8.7/609.9 ms), so this is a load result rather than a query-latency win.
The corresponding public 1M lifecycle completed in 1,571.56 seconds
(1,551.96 plus 19.60), 8.16% faster than the prior 1,711.26-second run. Live
and restart recall were both 0.9900; live serial p50/p95/p99 was
33.8/687.9/713.0 ms at 80.0 peak QPS, and restart was 32.2/637.1/658.4 ms at
94.9 peak QPS. Load RSS/demand peaked at approximately 4.07/2.04 GB.

The admission checksum did not remove the main native publication cost:
`posting_publish_ns` was still 278.87 seconds across 248 flush calls. The run
wrote complete HBC generations growing from approximately 37 MB to 251 MB
about fourteen times. This identifies repeated O(live-tree) materialization,
not staging verification, as the remaining HBC-native load bottleneck. A
RaBit-only structural split reconstruction experiment was rejected despite a
38.62-second 50K load because recall collapsed to 0.6325; RaBit codes remain
appropriate for distance bounds, not exact topology construction.

The next native-format revision replaces repeated complete checkpoints with
one mmap base plus at most six ordered immutable replacement deltas. `CURRENT`
names the complete chain and covered source sequence in one checksummed 184-byte
record. Each delta has per-derived-family tombstones, publication carries the
exact concurrent WAL suffix into the next generation, queries lease the whole
chain, and the seventh rotation compacts it into a new complete base before
unlinking obsolete files. Normal delta construction includes only changes
above the immutable root; full compaction also enumerates delta-only vector
mappings and metadata. Focused restart and full-compaction regressions verify
that same-sequence WAL tails and mappings introduced only in deltas survive.

The first public 1M qualification of that bounded chain completed in 1,443.92
seconds (1,418.38 seconds insert plus 25.54 seconds catch-up), 127.64 seconds or
8.1% faster than the admission-checksum full-generation run. It crossed the
former approximately 843K mutation-boundary failure cleanly and reached exact
visibility at source sequence 10001. The run published six deltas, compacted
them into one 248 MB base, and finished with a small active WAL; restart chose
that native authority without an HBC LSM rebuild. Live recall/NDCG was
0.9865/0.9883 with serial p50/p95/p99 of 33.8/719.3/739.2 ms. Restart was
0.9866/0.9883 at 32.8/647.6/673.3 ms. The valid pre-HTTP-429 throughput peaks
were 93.1 and 101.2 QPS respectively.

The delta result exposes two separate remaining costs. Attributable load demand
fell to 1.34 GB and the native physical-footprint ledger peaked at 731 MB, but
cache-inclusive RSS briefly reached 4.93 GB while full compaction faulted the
six mapped deltas and the segment writer duplicated their live payloads.
`posting_publish_ns` was still 294.12 seconds across 227 flushes: background
delta publication removed repeated full-generation work but did not eliminate
foreground mutation encoding. Checkpoint writers now borrow values from their
pinned immutable generation until the final contiguous segment is built, and
the WAL encoder fast-paths a single insertion/deletion as direct prefix,
literal, and suffix operations before falling back to sparse shifted-run
matching. A first public 50K sample before sparse-anchor tuning remained
scheduler-noisy at 46.38 seconds ready, but publication CPU fell from 4.06
seconds over 23 calls to 3.55 seconds over 21 calls; load-only RSS/demand fell
from approximately 1.60 GB/929 MB to 1.51 GB/734 MB. Live recall/NDCG remained
0.9854/0.9878 with serial p50/p95/p99 of 2.9/3.2/12.6 ms. Restart selected
native authority at sequence 501 without rebuilding and returned
0.9853/0.9878 recall/NDCG at 2.9/3.3/12.3 ms and 952 QPS peak.
Sampling general replacement anchors every 16 rather than four base bytes
reduced the next 50K sample to 3.24 seconds of publication over 20 flushes and
a 56.8 MB WAL, without changing its approximately 46-second noisy total.
Live/restart recall was 0.9849/0.9854, serial p50 remained 2.9 ms, and both p99
values were approximately 12 ms. Load-only RSS/demand was 1.49 GB/731 MB.

The corresponding sparse-anchor 1M qualification completed in 1,397.78
seconds (1,372.32 seconds insert plus 25.46 seconds catch-up). This is another
46.14 seconds faster than the first delta run, 173.78 seconds or 11.1% faster
than the full-generation admission baseline, and less than half of the
coworker-reported approximately 3,000 seconds. Publication fell to 255.94
seconds over 221 flushes, a 13.0% CPU/wall reduction from the first delta run.
It crossed the approximately 843K boundary cleanly and reached exact visibility
at sequence 10001. Live recall/NDCG was 0.9861/0.9880, serial p50/p95/p99 was
34.8/710.0/728.1 ms, and the valid pre-429 peak was 94.6 QPS.
Restart selected native authority at sequence 10001 without rebuilding;
recall/NDCG was 0.9863/0.9882, serial p50/p95/p99 was 33.6/641.2/664.3 ms,
and the valid throughput peak was 101.2 QPS.

Borrowing pinned mmap payloads reduced the first six-delta full-compaction RSS
peak from approximately 3.89 GB to 3.31 GB. Load-cutoff RSS/demand was
4.12/1.62 GB, versus 4.93/1.34 GB for the first delta implementation; the
native physical-footprint ledger remained approximately 713 MB. An immediately
scheduled post-readiness full compaction raised cache-inclusive RSS to 4.70 GB
without raising attributable demand. An initial attempt to defer only the idle
checkpoint hook was insufficient: the final source mutation had already
crossed the ordinary 128 MiB threshold and started generation 15 as a full
rewrite at sequence 10001. That run still completed in 1,386.52 seconds
(1,369.07 seconds insert plus 17.45 seconds catch-up), but its query sweep used
the resulting single approximately 252 MB generation, not the six-delta chain.
Live recall/NDCG was 0.9867/0.9884 with p50/p95/p99 of
34.4/713.5/739.5 ms and 98.8 peak QPS; restart returned 0.9868/0.9884 at
35.4/637.8/661.8 ms and 104.5 peak QPS. Publication consumed 249.39 seconds
over 218 flushes. Load-only RSS/demand/physical-footprint-ledger peaks were
3.85/1.30/0.69 GB.

The corrected policy is cost-aware rather than benchmark-aware. Incremental
deltas still start at 128 MiB, while a max-chain full merge receives the
existing 256 MiB hard recovery budget because it faults and rewrites the whole
visible generation. A fresh public 1M qualification started its only full merge
at sequence 6838 with a 269.56 MB capture (the small excess is one atomic source
batch), produced a 172.28 MB base, then finished with six deltas and no active
WAL at exact source sequence 10001. It completed in 1,392.62 seconds
(1,369.04 seconds insert plus 23.58 seconds catch-up), 178.94 seconds or 11.4%
faster than the 1,571.56-second complete-generation admission baseline and less
than half of the coworker-reported approximately 3,000 seconds. Publication
fell to 241.59 seconds across 224 flushes. Load-only RSS/demand/physical-ledger
peaks were 4.18/1.65/0.88 GB; the complete live process peaked at 4.18 GB RSS
and 2.70 GB attributable demand during query cache warming.

This qualification genuinely queried the retained base plus six mmap deltas:
no generation 15 build occurred before, during, or after the live sweep. Live
recall/NDCG was 0.9862/0.9881, serial p50/p95/p99 was
35.9/731.2/756.2 ms, and peak throughput was 98.6 QPS. Restart selected the
same native authority directly while the compatibility mirror remained at
sequence 126; recall/NDCG was 0.9863/0.9882, serial latency was
37.0/659.3/683.2 ms, and peak throughput was 104.8 QPS. The bounded chain
therefore preserved recall and throughput, with only a small plausible serial
fan-out cost relative to the immediately preceding compacted sample.

The remaining format tradeoff is disk and mapped-address amplification, not
generic HBC LSM work. The final 172.28 MB base plus six 131--170 MB deltas used
1.08 GB (about 4.3 times one compacted generation), although pages remain
reclaimable and queries fault only what they visit. The next high-upside format
experiment should retain replacement patches or copy-on-write chunks in indexed
immutable deltas and materialize/cache a value on first access. More frequent
full rewrites would reduce disk but give back the load-time and RSS gains this
experiment was designed to obtain.

## Whole-HBC native query qualification and competitor target

The complete native HBC format now owns topology, quantized payloads, postings,
vector-to-leaf mappings, metadata, WAL replay, generation publication, and the
covered source sequence. A compacted 1M generation is approximately 251 MB and
restart mounts it directly; dedicated HBC LSM runs are no longer required. The
primary document/artifact LSM remains authoritative for exact source embeddings.
Its settled 1M layout had 16 runs (three L0 and thirteen lower-level runs), so
the former 825-L0-run compaction-debt failure is not present in this result.

The first complete-format 1M load was not a performance win: 1,897.67 seconds
of insertion plus 17.50 seconds of catch-up, 1,915.17 seconds total. The native
checkpoint itself is cheap; foreground source insertion and derived mutation
publication now dominate. The data directory occupied approximately 3.5 GiB,
including about 3.07 GB of irreducible float32 payload for one million 768-D
vectors. A final design cannot beat the roughly 3.15 GB Elasticsearch disk
result by adding another per-index exact-vector plane. Exact artifacts need one
shared table-level vector-block authority, or an exact reconstructible encoding,
before the duplicate primary artifact rows can be removed safely.

At search effort 0.45, the original policy produced 0.9628 recall and 0.9668
NDCG. A representative serial run was 24.6/53.8/65.4 ms p50/p95/p99 and peaked
at 349.5 QPS, but host contention makes absolute comparisons provisional. The
deterministic work profile is the more important result: the resolved width was
1,097 leaves, with about 130,000 quantized vectors scored per typical query and
hundreds of exact boundary candidates. Primary exact-artifact reads dominated
cold and tail latency.

Moving exact rerank batches from broad snapshot transactions to current-tip
point probes removes their mutable-state clone/rotation path. Shared-cache batch
admission is epoch guarded so a concurrent update cannot resurrect stale vector
bytes, and stable run-backed probe values remain pinned rather than being copied
a second time. The first query-only memory sample peaked at 2.25 GB attributable
demand and 1.54 GB RSS. After a full serial plus concurrent cache warmup, the
same process peaked at 2.56 GB demand and 1.24 GB RSS; approximately 716 MB of
that warm state was retained exact vectors in the HBC heap cache. This is bounded
but is not the desired steady-state architecture: a shared mmap vector-block
store should make those pages reclaimable and eliminate the second heap copy.

Pruning calibration exposed a sharp but useful quality knee on the full 1,000
query ground-truth set:

| epsilon | recall | NDCG | serial p50 | serial p95 |
| ---: | ---: | ---: | ---: | ---: |
| 0.19 | 0.3882 | 0.4101 | 3.2 ms | 86.1 ms |
| 0.70 | 0.9253 | 0.9319 | 56.2 ms | 146.6 ms |
| 0.90 | 0.9527 | 0.9574 | 46.3 ms | 103.2 ms |
| 0.95 | 0.9548 | 0.9593 | 39.2 ms | 86.8 ms |
| 0.97 | 0.9556 | 0.9602 | 40.7 ms | 97.9 ms |

Only the quality numbers are comparable across this sweep; unrelated builds
changed CPU and IO availability between runs. Epsilon 0.97 clears Circus's
0.955 calibration target, but a 100-query work sample still visited a mean of
1,014 leaves (p50 and p95 both at the 1,097 hard cap) and scored 119,734
quantized vectors on average. Lowering epsilon alone therefore gives away recall
without removing enough hard-query work and must not become a benchmark-specific
default. The production improvement is confidence-aware routing backed by stored
cluster bounds and better cluster quality, with the width retained only as a
safety cap.

The current Circus 1M comparison target is deliberately Pareto-oriented. At
recall at or above 0.955, first beat pgvector's 731.8 QPS, keep p95 below the
low-teens Chroma/Weaviate range, reduce attributable demand toward Elasticsearch's
2.06 GB, and reduce disk toward its 3.15 GB. Elasticsearch and Milvus publish
3,104.7 and 3,813.0 QPS respectively, so they remain the throughput stretch
target after the shared exact-vector read path and routing work are complete.

The public batch-100 qualification is an online incremental build, not a
recursive offline build. The VectorDBBench adapter creates the external dense
index before loading, then sends 10,000 ordinary public batch requests for the
1M case. Antfly's empty-index bulk path does select the balanced recursive
builder when a single derived bulk-ingest window contains at least 1,024
vectors, and production backfill/import can reach that path. That does not make
it valid to label the existing online run recursive: its lifecycle must remain
the apples-to-apples Circus score. A recursive backfill/import result should be
reported separately, including its build time, recall, restart behavior, and
steady-state footprint.

A subsequent public-API qualification removed per-candidate shared-cache lock
and refcount traffic by scoring a complete rerank batch under one shared cache
lease. The exact cosine and invalidation/epoch invariants remain unchanged. On
the preserved 1M generation, the default public request resolved to width 2,048
and epsilon 1.45; a warmed 100-query diagnostic measured 13.9 ms mean, 10.5 ms
p50, and 35.2 ms p95 while scoring approximately 240,000 quantized vectors and
450 exact vectors per query. Cache-hit distance work was only 0.2--0.3 ms even
on the slow tail, so routing and cache misses—not lease bookkeeping—now dominate.

Under the current host load, the full public harness scaled from 107.1 QPS at
C5 to 264.6 QPS at C10 and 265.0 QPS at C20, then fell to 227.2 QPS at C30.
These are diagnostic rather than publication numbers because another Zig build
was active. C40 exceeded the server's 32-request admission envelope and returned
429, so the run was stopped rather than recording invalid higher-concurrency
points. A live C30 sample used roughly 780% CPU and a 307 MB HBC cache: 141 MB
quantized topology plus 130 MB for 41,273 exact vectors. This validates the next
A/B: scan the small flat block-quantized leaf-centroid directory before selected
posting reads, then replace cold primary artifact point reads with a shared,
versioned mmap vector plane.

The flat RaBitQ centroid directory did not beat the hierarchy and is rejected as
the production default. It reached 0.9600 recall on the 200-query calibration
set at effort 0.47, but the full 1,000-query run scored roughly 167,625 centroid
candidates and 433 exact candidates per query for 0.95626 recall, 84.27 ms mean,
and 178.2 ms p95 under the then-current host load. The hierarchical directory
reached comparable quality with materially less deterministic work. Keeping the
flat selector is useful only as an explicit experimental A/B surface.

The adaptive primary exact-read path avoids decoding and pinning an entire
prefix-compressed table block when a sorted rerank batch touches only one large
artifact in that block. A second key in the same block promotes it to the
decoded-block path, preserving adjacent-batch amortization. Stable probes own
directly reconstructed values and pin decoded blocks and the run generation, so
this optimization changes copy amplification rather than visibility semantics.

With that path and the original boundary-rerank policy, the lowest public
`search_effort` that cleared both Circus gates on this generation was 0.437:
0.95625 recall on the 200-query calibration set and 0.95021 on the disjoint
800-query held-out set. The held-out sequential diagnostic averaged 6.37 ms with
8.09 ms p95 while visiting about 927 leaves, scoring 109,618 quantized vectors,
and reranking 431 exact vectors per query. Effort 0.436 cleared calibration but
fell to 0.94934 held-out recall and is therefore invalid.

At effort 0.45, the normal C1/C5/C10/C20/C30 sequence produced
148.3/602.9/505.7/460.9/420.5 QPS, with C1 and C5 p95 of 7.78 and 9.53 ms. A
heavily warmed effort-0.437 C5 diagnostic reached 670.7 QPS at 8.64 ms p95.
These results beat the published Antfly and Chroma throughput figures while
holding recall, but they do not yet beat pgvector's 731.8 QPS and are not
publication numbers: other Zig builds were active, and the 0.437 C5 run began
with a larger exact-vector cache than a fresh Circus lifecycle.

Two follow-up routing shortcuts were rejected. Reducing the approximate
candidate window from nine to eight times `k` only barely held the recall floor
and did not improve C5 throughput. A metric-ball A/B using each leaf's exact
centroid radius pruned no meaningful payload work because these online HBC
partitions overlap too broadly. The radius code was removed rather than leaving
an extra query-to-centroid pass. Faster routing therefore requires better
partition geometry, controlled boundary replication, or a tighter second-level
posting directory—not another benchmark-specific effort constant.

An exact-vector cache-ownership A/B also rejected unconditional transient
primary reads for public search. On the preserved 50K generation, serving the
first 100 query batches without admitting source physical blocks held those
block inserts essentially flat (126 to 127) and reduced the shared LSM cache
from the 313.6 MB control to 26.4 MB while the governed HBC cache retained
115.7 MB. Recall remained 0.985, but cold client latency regressed from the
53.6 ms control mean to 200.3 ms, with p50/p95 rising from 38.8/124.1 ms to
184.3/351.8 ms. The decoded cache removed artifact IO on the repeat pass, but
discarding every source block lost useful spatial reuse while that cache was
being populated. Public search must therefore retain source blocks under the
shared resource envelope; transient admission remains appropriate only for
paths that materialize and retain the complete useful representation. Run
qualifications with an explicit process memory envelope to compare memory and
latency on equal terms instead of inheriting an 8 GiB LSM cache ceiling from a
large development host.

The follow-up retained-cache run under an explicit 1 GiB process envelope
preserved that reuse without preserving the large-host footprint. After the
same first 100 queries, the ResourceManager held LSM residency at 234.9 MB
(224 MiB soft, 256 MiB hard), retained 115.7 MB of decoded HBC vectors, and the
process RSS was 454.8 MB. Low-priority block eviction made progress while every
run-table index stayed resident. Recall remained 0.985; cold mean/p50/p95 were
44.8/35.5/91.9 ms and the identical warm repeat reached 5.64/4.57/10.85 ms
with no artifact reads. On this contended host those absolute times are
diagnostic, but the controlled result supports normal cache admission plus an
explicit resource envelope over benchmark-specific transient reads.

The shared HBC cache then exposed two general foreground-tail bugs. Reusing an
evicted CLOCK slot scanned from the beginning of the full slot array, and a
second-chance miss could charge a complete CLOCK revolution to one insertion.
Keeping the slot array compact makes removal/reuse constant-time; bounding one
victim search to 64 entries distributes the second-chance sweep while advancing
the hand and preserving namespace fairness. Focused churn, recency, and bounded
scan tests cover the map/slot invariants. On the same persisted 50K generation,
the official warmed VectorDBBench p99 fell from 440.5 ms to 12.8 ms at 0.9867
recall after these changes.

Continuous macOS `vmmap --summary` sampling was the remaining regular tail, not
Antfly. It produced approximately 450--520 ms stalls every 75 queries and even
charged some pauses inside server search timers because `vmmap` inspects the
live target. The qualification runner now invokes the Circus sampler once after
each timed phase. The kernel footprint ledger preserves the process high-water,
so this retains the same primary memory yardstick without perturbing load or
query latency. With sampling moved out of the timed window, the checked-in
public profiler measured 3.63/4.26/4.74 ms p50/p95/p99, 14.87 ms maximum, and
0.98671 recall across 1,000 queries. The post-phase sample reported a 659.2 MB
physical-footprint ledger peak, 729.1 MB current RSS, and 700.7 MB conservative
demand under the explicit 1 GiB process envelope.

The first post-sampling 1M qualification under a 2 GiB envelope is invalid as
a timing result but exposed a general rollback amplification bug. At 568,800
query-visible documents, public inserts began hitting their exact 120-second
request timeout. A process sample caught derived replay inside
`commitDenseVectorMappingsWithRollback`: a primary mapping-store commit failure
started maintained inverse HBC deletes even though the active authoritative
source capture already owned the complete pre-mutation native generation.
Those deletes recomputed centroids and, with the governed HBC cache retaining
only one external vector, reloaded posting members from the primary LSM. The
correct rollback is to propagate the error and let the outer capture cancel
restore topology, metadata, search publication, and caches in constant time.
Pre-authority captures retain inverse rollback because they do not yet own a
complete restorable generation. Tests cover both sides of that authority
boundary.

The same partial run confirmed that exact rerank at 1M is now dominated by
sparse primary-artifact block reads rather than HBC routing. Sorted point
batches now overlap independent path-backed run-block reads with a bounded
four-slot pipeline. Each key still resolves mutable state and candidate runs in
strict newest-to-oldest order, so tombstone and overwrite precedence is
unchanged; only different keys overlap IO. The pipeline uses existing cache,
checksum, allocation, and read-stat paths and caps concurrent buffers at the
configured point-read ceiling. Its latency effect must be measured against the
preserved complete 1M generation before another fresh load qualification.

That preserved-generation A/B kept recall exactly 0.98535, exact rerank work
exactly 447.478 vectors per query, and approximate work exactly 237,596 vectors
per query across the same 1,000 public API queries. The bounded four-read
pipeline reduced client mean latency from 85.71 to 54.21 ms and p95 from
153.46 to 95.07 ms. Server-attributed artifact reads fell from 72.01 to 41.77
ms mean and from 138.21 to 81.38 ms p95. This is a 36.8% end-to-end mean
improvement and 42.0% artifact-read improvement without changing search work
or quality. Post-query demand was 1.12 GB versus 1.30 GB in the earlier run;
RSS was higher at 1.74 GB versus 1.46 GB because the merged cache governor
retained more reclaimable node metadata, so that one-sample cache-inclusive
difference is not attributed to the read pipeline.

After merging current `origin/main`, the stale sibling VectorDBBench checkout
failed before search because it POSTed the removed legacy
`/api/v1/tables/{name}` contract and received HTTP 405. The canonical runner
preserves that failed result, supports an explicitly labeled diagnostic-profile
mode, and accepts an explicit VectorDBBench checkout so the API-compatible
adapter can run the official lifecycle. Diagnostic profiles are not
publication-equivalent substitutes for the official client lifecycle.

The API-compatible checkout then completed the official reopened 1M serial
lifecycle. Cold recall was 0.9854 with 54.9/114.1/163.6 ms p50/p95/p99; the
immediate warm repeat held 0.9854 recall at 53.3/109.8/150.2 ms. The earlier
same-generation official run before sparse pipelining measured
140.7/466.7/731.4 ms cold and 87.4/214.8/319.3 ms warm. A detailed 1,000-query
profile after the official warm pass measured 48.52 ms mean, 86.08 ms p95,
102.86 ms p99, and 36.03 ms mean artifact-read time. Its post-phase sample was
1.22 GB demand, 1.20 GB physical-footprint ledger peak, and 1.57 GB RSS under
the same explicit 2 GiB process envelope.

Widening only the primary-store point pipeline from four to eight was rejected.
Although eight 32 KiB buffers look inexpensive in isolation, the preserved 1M
public profile made no bounded progress and one request exceeded the profiler's
120-second timeout. The server eventually unwound the in-flight request and
exited cleanly, but this is a hard tail failure, not a noisy benchmark sample.
Per-query read fanout composes with concurrent public queries, shared cache
admission, and the bounded service I/O lane; a local buffer calculation alone
is therefore not a safe concurrency policy. Keep four as the default until a
global admission controller can allocate read slots across queries.

The first fresh 50K qualification after the four-wide change recovered the
load target (31.20 seconds ready) but exposed an aggregate-cache deadlock at
five concurrent queries. An LSM cache admission held its accounting lock while
waiting for the resource-manager reclaimer gate, whose HBC callback waited for
the HBC cache owner. Making only the HBC callback nonblocking exposed the
complementary cycle on the next run: an HBC admission held the callback gate
while its LSM callback waited for LSM accounting. The production invariant is
therefore symmetric: resource-manager cache callbacks are opportunistic and
must never wait for an owner lock. HBC now try-locks its cache; LSM try-locks
accounting and individual shards, evicts only unpinned entries, and publishes
the exact released bytes. A contended callback returns zero, causing the
requesting cache allocation to use its bounded transient path without relaxing
the aggregate hard limit.

The fresh post-fix 50K public-API gate completed the full load/query/restart
lifecycle. It inserted in 29.17 seconds and was fully ready in 31.19 seconds;
the five-client 10-second phase completed at 214.01 aggregate QPS with 23.29 ms
mean, 37.04 ms p95, and 46.05 ms p99 concurrent latency. Live serial recall was
0.9853, and cold/warm reopened recall was 0.9841. Warm restart serial latency
was 3.0 ms p50, 3.6 ms p95, and 15.1 ms p99. A separate 100-query profiled pass
measured 3.63 ms mean, 4.09 ms p95, 4.58 ms p99, and 0.9826 recall (the smaller
sample explains the recall variance). Primary mutable snapshot copies totaled
106.27 MB, HBC cache demand was 16.42 MB, the data root occupied 409 MiB, live
demand/RSS peaked at 508.6 MB/1.25 GB, and reopened demand/RSS at 597.8 MB/
931.3 MB under the explicit 1 GiB envelope.

The subsequent full 50K ladder reproduced the load result at 28.07 seconds
insert and 30.10 seconds ready. Live recall was 0.9863. Aggregate QPS at
1/5/10/20/30 clients was 50.32/214.92/278.06/159.02/152.00; latency p95 was
34.18/36.78/62.17/212.59/334.71 ms. The throughput knee after ten clients is
CPU/global admission saturation rather than the prior deadlock. A 1,000-query
public profile measured recall 0.98541, 3.88 ms mean, 4.64 ms p95, and 8.33 ms
p99, with 557.96 exact vectors and 24,263 approximate vectors per query.

The fresh pre-resource-governor 1M qualification completed without the former
~568K rollback stall or ~843K posting-capture violation, but it remains too
slow. It inserted in 1,817.41 seconds and was ready in 1,821.51 seconds: better
than the reported 3,000 seconds, but 2.3x the 13-minute target. Live/reopened
recall was 0.9861/0.9858. Aggregate QPS at 1/5/10/20/30 clients was
5.51/23.22/37.52/46.21/44.10, with p95 latency
317.23/424.66/537.10/989.46/1,522.94 ms. The 1,000-query public profile measured
66.72 ms mean, 136.31 ms p95, and 183.22 ms p99. Artifact reads alone averaged
47.66 ms and exact rerank expanded to 2,068.97 vectors/query, versus about 447
on the earlier preserved generation at equivalent recall; query selection or
rerank-boundary behavior therefore regressed independently of sparse LSM I/O.

The completed 1M root occupied 3.76 GiB, primary mutable snapshot copies
totaled 1.46 GB, and HBC cache demand finished at 181.76 MB. Late ingest built
150 pressure events and 156 pressure compactions and still finished with 72 L0
runs. Direct observation caught about 3.27 GB RSS during ingest; the existing
post-phase one-sample footprint (1.01 GB RSS, 2.00 GB demand) did not capture
that peak and must not be presented as load peak memory. This is the clean
pre-PR-540 baseline for the lifetime-fenced resource governor.

The post-PR-540 integration preserves both sides of the cache callback
contract. ResourceManager invokes reclaimers outside its registry and
accounting locks and holds a fixed-slot in-flight lease so unregister can fence
callback context destruction. HBC and LSM callbacks remain opportunistic and
never wait for their cache owner locks. Managed query sessions reserve decoded
vector residency as one request-owned unit, switch coherently to retained LSM
residency before an overrun, and avoid loading metadata for already decoded
rerank hits. The resource-budget suite passed 73 tests, the LSM suite passed
323 with one intentional skip, the DB query suite passed 191 with two skips,
and the standalone vector-index suite passed all 38 tests.

The fresh post-governor 50K public-API gate inserted in 20.48 seconds and was
ready in 24.53 seconds. At five clients it reached 374.62 QPS with 28.66 ms
p95 and 0.9859 recall. The standardized full run was scheduler-noisier at
29.89 seconds insert and 31.93 seconds ready, but query throughput changed
materially: C1/C5/C10/C20/C30 reached
171.92/1330.34/1507.15/1308.57/1165.07 QPS, versus
50.32/214.92/278.06/159.02/152.00 before the governor follow-ups. Corresponding
p95 latency was 12.61/5.49/9.29/22.49/40.48 ms instead of
34.18/36.78/62.17/212.59/334.71 ms. Live recall was 0.9845 and warm reopened
recall was 0.9810. The warm 1,000-query profile measured 4.17 ms mean, 4.80 ms
p95, and 5.10 ms p99; governed decoded residency eliminated artifact read and
decode time in that warm phase.

The checked-in runner now complements the post-phase Circus footprint sample
with a lightweight 200 ms `ps` RSS timeline for the entire live and reopened
lifecycle. The full 50K run peaked at 1.55 GB RSS and 1.20 GB attributable
demand under the explicit 2 GiB envelope; the reopened process peaked at
456 MB RSS. This closes the methodology gap that hid the earlier 1M load peak
without reintroducing intrusive `vmmap` sampling into timed traffic.

The fresh post-governor 1M qualification completed without the former rollback
stall, posting-WAL capture violation, retry storm, or visibility loss. It
inserted in 1,499.99 seconds and was ready in 1,502.04 seconds: 17.5% faster
than the 1,821.51-second baseline, but still 1.9x the 13-minute target. Catch-up
is now only 2.05 seconds, so remaining load time belongs almost entirely to the
primary document/artifact LSM. Live recall was 0.9852. C1/C5/C10/C20/C30
throughput was 9.98/54.88/93.55/97.15/46.58 QPS, versus
5.51/23.22/37.52/46.21/44.10 before the governor follow-ups. The useful knee is
twenty clients; thirty clients saturated this host and raised p95 to 1.87
seconds. Warm reopened recall was 0.9851, and the 1,000-query profile improved
from 66.72 to 36.33 ms mean and from 136.31 to 77.35 ms p95. Exact rerank work
remained high at 2,087.31 vectors/query, so residency ownership accelerated the
same broad rerank boundary rather than hiding a recall reduction.

Continuous 1M RSS peaked at 2.50 GB live and 2.09 GB after restart; the
post-phase footprint ledger reported 2.13 GB demand. This is materially below
the prior 3.27--3.89 GB observations but does not meet a strict 2 GiB process
envelope. ResourceManager's managed-host peak was 1.10 GB, while dense apply
alone peaked at 222.42 MB against an 89.48 MB slice hard limit and two aggregate
accounting errors were recorded. The gap between managed charges, footprint,
and RSS needs an ownership audit; it must not be dismissed as harmless mapped
residency.

The final disk and write counters isolate the next general bottleneck. The
3.7 GiB root contained 3.3 GiB of primary LSM runs and only 357 MiB of the
native vector index. Primary ingest accepted five million logical entries, but
only 360,000 entries (7.2%) reached direct sorted ingest. The remainder caused
4,564 flushes, 16,595 flush output runs, 4.06 GB of flush output, and 4,381
manifest publications. It then required 147 pressure events and 151 pressure
compactions, finishing with 88 L0 and 13 lower-level runs. Mutable snapshot
copies totaled 1.51 GB and current-scan rotations 342.85 MB. The native HBC
generation/WAL design is no longer the dominant load or disk cost; making the
primary append-heavy document/artifact path publish larger sorted runs with
far fewer manifests is the highest-leverage next experiment.

## Primary geometric-merge experiment

The next 50K experiments changed only the primary LSM publication and
compaction policy. They retained the public `/db/v1` server path, batch size
100, twenty load workers, native HBC, the 2 GiB resource envelope, exact
rerank, and the same recall/query matrix. The implementation publishes sorted
bulk state at 8 MiB, assigns all partition files in one publication a durable
logical L0 sequence, and performs same-level streaming carries. Manifest v10
persists that chronology and remains able to read v9 manifests.

An exact four-way carry recovered fast load (21.66--23.10 seconds total) but
eventually fell through to generic L0-to-L1 compaction. That rewrote about
710 MiB and left live RSS around 1.86--1.92 GB. Raising the optional
foreground-query compaction pause from 2 to 25 milliseconds was rejected: it
stretched 662 MiB of compaction from roughly 4.7 to 14.3 seconds, raised RSS to
2.01 GB, and did not improve the first query lane.

Giving the tier lane ownership of run-count-only soft pressure reduced
compaction input to 75.4 MiB and load to 22.49 seconds, but retained 96 L0
files. Live RSS reached 2.13 GB and restart memory/latency regressed. A
two-to-four-way carry was also rejected earlier because it rewrote 1.02 GB in
thirteen jobs. The accepted refinement is a universal geometric carry: an
uneven contiguous generation window is eligible only when the output is at
least four times every input generation. This preserves chronological
overwrite/tombstone precedence and log-base-four write amplification while a
hard L0 run/byte limit remains an authoritative leveled-promotion escape.

Two fresh geometric-carry 50K runs were ready in 28.72 and 23.98 seconds. The
repeat ended with 24 L0 files, no lower-level files, 323.3 MiB compaction input,
321.4 MiB output, 2.38 seconds cumulative compaction, 134.9 MiB snapshot
copying, no write-pressure events, and 402.6 MB total disk. Its live RSS peak
was 1.54 GB and recall was 0.9854. Query results remained host/tree-sensitive:
the repeat reached 1,161 QPS but had 50.8/14.5/14.2/30.4/46.9 ms p95 at
concurrency 1/5/10/20/30, so the 1M gate must report query and restart
latencies rather than treating the improved storage counters as sufficient.

The first geometric 1M gate was correct and improved load from 1,502.04 to
1,267.79 seconds (1,263.74 insert plus 4.05 catch-up), but did not preserve its
early ~13-minute trajectory. Direct publication stopped after 1.087M of 5M
logical entries because transient bulk sessions no longer accumulated to the
8 MiB floor as the lower-level base slowed request overlap. The tail produced
3,869 flushes, 14,104 flush-output files, and 3,931 manifests. More
importantly, the hard-pressure lane bypassed geometric carries and performed
165 compactions with 92.74 GB input and 92.10 GB output for a 3.52 GB primary
run set. This is write amplification, not necessary dataset size.

Recall remained 0.9849. C1/C5/C10/C20/C30 throughput improved to
11.79/74.46/122.10/124.09/122.57 QPS, eliminating the former C30 collapse.
The warm 1,000-query profile was 35.26 ms mean and 77.89 ms p95 at 0.98452
recall, essentially the post-governor latency baseline. Demand improved to
1.70 GB live and 1.30 GB after restart, while cache-inclusive live RSS peaked
at 3.38 GB during compaction overlap. The next gate lowers the byte-based
direct-publication floor to 4 MiB and makes hard run-count pressure attempt a
geometric carry before generic promotion; byte-hard pressure remains directly
leveled because a same-level merge cannot reduce its byte debt.

The 4 MiB direct floor plus foreground geometric hard-pressure assist recovered
the intended load envelope. Its 50K gate finished in 21.74 seconds (19.71
insert plus 2.03 catch-up) at 0.9867 live recall, used 377 MiB on disk, cloned
76.7 MB, and performed seven geometric compactions over 326.8 MB. C1/C5/C10/
C20/C30 throughput was 59.44/1,253.64/1,580.69/1,276.12/1,285.59 QPS with
42.3/7.0/8.9/21.0/32.5 ms p95. No hard-pressure event occurred. Live RSS
peaked at 1.97 GB, so the speed result passed while memory retained noticeable
host/cache variance.

The matched 1M gate finished in 759.03 seconds (754.97 insert plus 4.06
catch-up), recovering the ~13-minute target while preserving 0.9859 recall.
Direct publication reached 3.9635M of 5M logical entries instead of freezing
at 1.087M. All 653 pressure events made progress with zero overloads. One
byte-hard promotion established a 2.09 GB lower-level base; geometric carries
then avoided rewriting it. Total compaction input fell from 92.74 GB to
14.55 GB, compaction time was 101.0 seconds, and final disk was 3.86 GB.

This load win is not yet the balanced endpoint. Live/restart demand was
3.15/2.18 GB and cache-inclusive RSS peaked at 3.94/2.19 GB. Warm recall was
0.9857, but warm p50/p95/p99 regressed to 41.1/90.0/108.3 ms. The public
profile attributes 29.88 ms mean and 77.45 ms p95 to rerank artifact reads.
After maintenance the primary still had 74 L0 runs containing 1.43 GB above a
2.09 GB base, while the LSM and HBC caches held about 470 and 408 MB. The next
experiment should retain geometric carries but permit one justified base-growth
merge when accumulated L0 is a substantial fraction of the lower base. That
is bounded size-ratio leveling, not a return to repeated low-growth rewrites.

A 50% base-growth promotion tested that hypothesis. The 50K gate remained
healthy at 22.41 seconds ready, 0.9861 recall, and 1.96 GB peak RSS. At 1M the
second promotion fired near 897K rows, reducing the query-visible primary from
1.43 GB in L0 over a 2.09 GB base to 415 MB in L0 over 3.09 GB in lower
levels. Live/restart demand improved from 3.15/2.18 GB to 1.97/1.52 GB, and
cache-inclusive peaks improved from 3.94/2.19 GB to 3.33/2.06 GB.

The leveled form is not the optimal endpoint. Load regressed from 759.03 to
788.81 seconds, compaction input rose from 14.55 to 18.40 GB, and compaction
time rose from 101.0 to 131.9 seconds. Warm p95 improved from 90.0 to 83.7 ms,
but the public profile improved only from 89.92 to 88.04 ms and C30 fell from
94.26 to 78.58 QPS. The 2.92 GB maximum job shows the ratio promotion merged
the new delta with an existing L1 fragment. The next variant should use the
same meaningful-growth threshold to seal all current L0 generations into one
large L0 generation instead. That keeps the existing lower base untouched,
removes fragmented L0 fan-out with roughly 1 GB of streaming input, and makes
the geometric growth rule prevent repeated rewrites of the sealed generation.

The first same-level implementation deliberately required the complete L0
output to be at least twice its largest input generation. Its 1M control never
sealed: the final manifest contained a 1.036 GB anchor plus 384.6 MB spread
over 17 newer generations, so the complete 1.42 GB L0 could not satisfy 2x
growth. It nevertheless provided a matched control at 818.28 seconds ready,
0.9852 live recall, 3.36 GB peak live RSS, 2.35 GB demand, and 42.73/91.82 ms
detailed mean/p95. Final L0 was 43 physical runs across 18 generations above
a 2.095 GB lower base. This corrected the policy: the established anchor must
not be part of a newer-delta seal.

The anchor-preserving prefix seal passed the next 1M gate. Once total L0
reached half the lower base, it compacted a newer prefix only after that prefix
was at least twice its largest component. The authoritative manifest retained
the original 991.9 MB anchor and 2.077 GB lower base, published one 381.9 MB
prefix generation, and finished with 28 L0 runs across 12 generations. Ready
time improved to 804.56 seconds, compaction time fell from 118.70 to 105.24
seconds, pressure steps fell from 654 to 615, and snapshot cloning fell from
917.8 to 792.8 MB. Total compaction input remained essentially flat at
15.22 versus 15.16 GB because the prefix seal replaced geometric carries
rather than adding a base rewrite.

Memory and warm query latency also improved: peak live RSS fell from 3.36 to
3.06 GB, demand from 2.35 to 2.28 GB, warm p95 from 88.6 to 79.8 ms, and the
detailed profile from 42.73/91.82 to 37.21/81.37 ms mean/p95. Detailed recall
was 0.98354 versus 0.98481, a 0.127 percentage-point delta. The live
concurrency curve regressed under a noisier host interval, however, and disk
rose from 3,773,788 to 3,857,468 KiB, mostly in native posting segments plus a
smaller primary-boundary difference. Treat the policy as a checkpoint, not the
final result.

A recursive geometric-stack extension was tested and rejected. It could
theoretically seal a newer prefix above each successively dominant anchor while
retaining the same 2x proof, but no recursive seal was legally eligible in its
full 1M run: the two closest prefix ratios finished at approximately 400/241
MB and 159/84 MB. The planner correctly left 44 L0 runs across 16 generations
instead of forcing either low-growth rewrite. This run recovered the load
baseline at 753.16 seconds ready with 0.9845 live recall, 14.18 GB compaction
input, 94.85 seconds compaction time, zero overloads, and 2.02 GB demand.
However, it was only another no-seal variance sample: live RSS was 3.21 GB and
the warm detailed profile regressed to 52.15/104.59 ms mean/p95. The recursive
planner and test were reverted; the validated anchor-preserving checkpoint is
the retained policy. Do not weaken the 2x bound merely to force a prettier
final manifest.

Public-profile boundary diagnostics also ruled out tree shape as the cause of
the earlier 1M query-latency spread. Replaying the same 1,000 public API
queries against the anchor-preserving and no-seal trees produced 443.69 versus
438.91 rerank candidates/query, 0.00789 versus 0.00786 mean boundary-tail
error, and materially identical interval gaps and recall. Their prior
37.21/81.37 versus 52.15/104.59 ms mean/p95 profiles were therefore cache and
host-state variance, not a quality regression caused by the L0 layout.

A promising-candidate-first progressive rerank was implemented and rejected.
It kept candidate selection unchanged and sorted vector ids within each
128-vector storage batch, but activated the existing interval stop much more
often. At 1M it skipped 75.17 of 444.88 candidates/query and improved a noisy
cold replay from 96.92/144.85 to 60.80/109.33 ms mean/p95, while recall moved
from 0.98392 to 0.98072. At 50K it skipped 106.11 of 254.31 candidates/query
but recall fell from 0.98396 to 0.95400, outside the parity envelope. The
quantization intervals are not a sufficient hard membership proof for this
reordering. The execution change was reverted; retain vector-id ordering and
boundary rerank until a stronger conservative bound is available. Also do not
compare the 50K diagnostic-only cold replay's 14.16 ms mean against the 3.67
ms post-warm profile: diagnostic-only resume intentionally skips the two
official serial warm-up stages.

Metric-correct angular covering radii were also implemented and rejected for
the Cohere cosine workload. A fresh 50K public-API qualification preserved the
load and quality envelope at 22.37 seconds ready and 0.9838 detailed recall,
and 208 of 214 frontier nodes resolved a durable exact-centroid bound. However,
all 1,000 queries still explored all 208 leaves and no bound stop fired. In
1,536 dimensions the exact leaf-enclosing angular spheres are nearly
hemispherical, so subtracting their radii from the query-to-centroid angle
collapses the triangle-inequality lower bound to zero. Do not pay write/format
cost for cosine enclosing spheres on this design; useful cosine stopping needs
a tighter proof object than a single centroid sphere. The experiment was
reverted. The profiler retains the traversal counters, and progressive rerank
now explicitly retires any candidates skipped by its existing proven stop so
they cannot re-enter the final approximate ordering.

The primary WAL checkpoint floor and immutable publication window must be
treated as two separate bounds. Raising both to 256 MiB reduced flush topology
but was rejected: the 1M lifecycle regressed to 735.01 seconds ready, cloned
8.57 GB cumulatively, and spent 144.63 seconds publishing native HBC posting
generations. Rotating the mutable epoch at 128 MiB while continuing to stream
two epochs into one 256 MiB immutable publication recovered the load baseline.
The fresh 1M public-API lifecycle completed in 623.16 seconds (613.58 insert +
9.59 catch-up), preserved 0.9845--0.9849 recall, peaked at 2.91 GB RSS with
1.40 GB attributable demand, and reduced manifests from the prior 598 to 167.
All nine primary pressure events completed with zero overload or hard debt;
cumulative snapshot copying was 3.24 GB. The corresponding 50K gate completed
in 22.43 seconds with 0.9849--0.9862 recall, 1.22 GB peak RSS, and 322.13 MB of
snapshot copies. Retain the nested 128/256 MiB bounds: a single large mutable
epoch couples source-scan cloning to HBC publication cadence even when the
eventual persisted flush topology looks cleaner.

Making the existing anchor-preserving L0 delta seal eligible under soft
maintenance was also tested and rejected. On a clone of the completed 1M
generation it reduced the active primary topology from 40 runs / 33 L0 runs to
20 / 13 without changing the seven-run lower base or recall. That did not make
reranking faster. The original detailed public profile was 36.19/82.12 ms
mean/p95 with 24.12 ms mean in exact-artifact reads; the immediate post-seal
profile was 46.89/93.02 ms with 34.77 ms artifact reads, and a second clean
reopen remained worse at 50.12/108.78 ms with 37.15 ms artifact reads. Fewer
generic LSM runs destroyed favorable artifact block locality rather than
improving it. The soft scheduling change was reverted; the hard-pressure seal
remains available for admission safety.

The half-window run therefore sharpens the next format boundary. Native HBC
tree traversal itself averaged about 10.2 ms once exact-vector loading is
subtracted, while primary artifact reads consumed roughly two thirds of warm
query time. A per-index float32 plane would duplicate about 3.07 GB at 1M and
is not an acceptable endpoint. The next high-upside design is one table-level,
versioned vector-block store shared by every index using an embedding artifact,
with source sequence and artifact revision in its WAL/delta publication and
generation leases matching HBC topology to the exact vector revision.

Before adding that format, a matched 1M read-path experiment showed that much
of the cold artifact cost was avoidable serialization. On the same repaired
native generation, raising sparse point-block overlap from four to sixteen cut
server mean/p95 from 86.51/166.44 to 51.72/93.73 ms and artifact-read mean from
72.33 to 39.65 ms, with identical 0.98477 recall, candidate counts, and cache
residency. Fixed sixteen was not the production endpoint: at 30 public clients
it peaked at 132.83 QPS with 630/1341 ms p95/p99 because every query could
independently fan out to sixteen reads.

The retained policy gives each batch up to sixteen reads but divides a
64-read backend-wide target across simultaneously active sparse batches. It is
non-blocking: later arrivals narrow themselves, and a share below two falls
back to the scalar precedence-preserving path. Against fixed sixteen, the
matched warmed 1/5/10/20/30-client curve changed QPS from
30.12/80.89/136.51/138.99/132.83 to
30.98/86.57/117.48/153.54/160.48. At 30 clients p95/p99 fell from
630/1341 to 475/645 ms. The post-curve detailed profile preserved 0.98477
recall and measured 35.02 ms mean, 71.16 ms p95, and 23.45 ms mean artifact
reads; the prior published half-window profile was 36.19/82.12 ms with
24.12 ms artifact reads. Peak sampled RSS was 1.865 GB and the process
physical-footprint ledger was 1.60 GB under the explicit 2 GiB envelope.

This is a general sparse LSM read improvement, not an HBC-only shortcut, and
it does not change key precedence, tombstone handling, block-cache ownership,
or recall policy. It improves the current primary-artifact miss path while the
shared table-level vector-block store remains the architectural endpoint.

The native posting/WAL and shared exact-vector block formats moved the next
query bottleneck into topology routing. A clean vector-block 1M generation
visited about 2,022 tree leaves, scored 235K approximate vectors, reranked 1,639
exact vectors, and measured 27.83 ms public p50 / 25.98 ms HBC p50 at 0.98437
detailed recall. Recursive binary and one-shot global k-means topology rebuilds
did not improve the leaf-recall curve. At 50K they increased topology and/or
workspace cost, retained roughly the same 208-leaf query budget, and produced
no latency win. They remain useful negative experiments, not production
defaults.

A flat full-precision centroid directory did improve the routing curve. At the
normal 0.5 effort, the same 1M generation reached 0.99095 recall with 15.34 ms
public p50 and 14.10 ms HBC p50. At the lowest measured parity boundary (0.47),
it reached 0.98495 recall with 13.40 ms public p50, visited 1,408 leaves, scored
165K approximate vectors, and reranked 443 exact vectors. The official public
API cold/warm runs at that boundary measured 12.2/12.2 ms p50 and 0.9829
recall, versus 28.6/30.6 ms and 0.9842 for the tree. The matched
1/5/10/20/30-client curve improved QPS from
60.83/215.15/219.67/199.38/183.97 to
81.04/322.14/332.81/320.96/291.32; concurrency-20 p95 fell from 146.71 to
79.11 ms. Attributable restart demand stayed 423--468 MB. Cache-inclusive RSS
rose when queries touched more mmap pages, so it must continue to be reported
separately from reclaimable demand.

The production policy is adaptive rather than a benchmark-wide override. Small
indexes keep tree routing (exact flat routing was about 3% slower at 50K), and
indexes estimated to have at least 1,024 postings use the exact directory at
the caller's unchanged effort. Full checkpoints now encode that directory as a
versioned, block-columnar entry in the same immutable native segment. Clean
generations borrow its aligned vectors directly under the query generation
lease; a post-checkpoint packed-node mutation refuses the stale entry and uses
the topology fallback until a matching generation is published. This removes
the restart topology scan and the roughly 54 MB exact-centroid heap copy at 1M
without introducing another store or publication boundary.

The complete persisted-centroid-delta lifecycle then passed a fresh 1M public
API qualification under a 2 GiB process budget. It inserted in 592.39 seconds
and reached full query visibility in 634.64 seconds, versus the earlier
619.96-second vector-block insert baseline. Live serial p50 was 13.7 ms at
0.9900 recall and the concurrency curve peaked at 274.54 QPS, improving the
earlier roughly 220-QPS peak. The final shared vector base encoded one million
768-dimensional vectors in 3.164 GB in 33.25 seconds; the native HBC generation
was 278.54 MB. All eight primary hard-pressure events completed with no
overload or remaining hard debt.

The same run separated reclaimable mmap residency from attributable demand.
Peak live demand was 1.812 GB and ResourceManager's managed peak was 1.040 GB
with zero release-accounting errors, but sampled RSS reached 6.128 GB live and
6.251 GB after restart. Disk was 6.5 GB total: 2.9 GB in shared vector blocks
and 270 MB in the native HBC index. Reopened detailed queries measured 27.28 ms
public p50 / 25.95 ms server p50 at 0.99055 recall, scoring 2,048 leaves and
239.6K approximate vectors; exact-vector loading accounted for 3.37 ms. The
literal first server search after a clean reopen was 199.57 ms, down from the
earlier roughly 650-ms activation sample, while the Python client's 1.76-second
wall time was dominated by loading its Parquet fixture.

A same-generation public `search_effort` sweep confirmed that candidate work,
not routing lookup or rerank storage, is the remaining steady-query lever.
Effort 0.40 visited 588 leaves and reached 11.18 ms server p50 but only 0.95255
recall. Effort 0.45 visited 1,097 leaves and reached 16.43 ms at 0.97895 recall.
Effort 0.46 visited 1,243 leaves, scored 145.8K approximate vectors, and reached
17.96 ms at 0.98195 recall, 0.86 percentage points below the default. This is a
useful explicit latency/quality point, not enough cross-dataset evidence to
change the public 0.5 default.

The restart RSS diagnosis exposed a physical-layout issue in the first shared
vector-block writer. Although vector payload checksums were already lazy, the
file interleaved each compact key with its roughly 3 KiB vector. Correct startup
validation of every key's checksum, shard, and total ordering consequently
faulted nearly every mmap page. New blocks now place all keys before an aligned
vector arena and retain the index/footer at the end. This preserves the same
reader, revision, checksum, and lazy-payload contracts—including compatibility
with existing interleaved blocks—but lets admission touch only compact key and
index pages. A fresh columnar-block lifecycle is required to quantify the RSS
and restart improvement; an old generation cannot gain it without rebuilding
its base.

The first fresh columnar-block 50K lifecycle passed the public API gate. It
inserted in 31.23 seconds and became ready in 33.34 seconds with 0.9869 live
recall, 7.7/11.5/14.5 ms serial p50/p95/p99, and a 699.65-QPS peak. The shared
base encoded 307.2 MB of float vectors into a 311.65 MB generation in 6.44
seconds. Most importantly, cache-inclusive restart RSS fell from the prior
interleaved-block sample's 609 MB to 383 MB, and live RSS fell from roughly
1.22 GB to 624 MB. Reopened recall remained 0.9851. Query latency in that
restart interval was host-contended (21.6 ms warm p50 versus the prior roughly
12 ms), and the system wired baseline moved enough to make the one-shot demand
delta non-comparable; neither is used to claim a search-kernel change. The RSS
direction is nevertheless the expected direct result of leaving untouched
vector pages out of the process mapping residency.

The interrupted first columnar 1M lifecycle exposed two readiness bugs rather
than a search failure. Background vector maintenance enumerated only live
write-cache databases, and the no-replay startup path returned before native
projection maintenance. Even after a later build, the public embeddings view
could erase the DB's pending state once external coverage converged. Native
projection absence is now broad startup debt, is executed at the stable source
tip, and is represented explicitly as `dense_vector_projection_pending` all
the way through shard aggregation and the public response. The checked-in
qualification runner refuses to query while that bit, backfill, or dense
publication is pending. LSM soft pressure still defers generic LSM work but no
longer starves this separately governed vector publication lane.

The 1M migration then built a complete 128-shard columnar float32 base in
41.98 seconds. Its actual ResourceManager builder/overlap peak was about
120.8 MB and the dedicated slice stayed within its 128 MiB hard limit with no
pressure event or rejection. A clean public restart measured 0.99061 recall,
28.41 ms client p50, 27.13 ms server p50, and 16.74 ms p50 in exact-vector
payload reads. It scored the same 2,048 leaves, 242.4K approximate vectors,
and 446.7 exact vectors as the prior generation, with zero primary-LSM or
vector-block fallback. Cache-inclusive RSS was 3.38 GB, but attributable
demand was only 437 MB; most of the difference was reclaimable mmap residency.

Ordering exact reads by `(shard, full_hash, key)` fixed a latent physical-sort
bug: ordering by full hash alone alternated among all low-bit-selected shard
files. A byte-bounded external rerank unit now coalesces sets that fit within
2 MiB (up to 512 vectors) and retains 128-entry progress checkpoints for long
tails. On the matched 1M corpus it preserved 0.99061 recall and improved server
p95/p99 from 80.07/128.42 to 75.94/108.72 ms, but did not improve p50. This
ruled out per-batch lease/sort overhead as the median bottleneck and isolated
the float32 exact plane's random payload reads.

A versioned float16 query projection produced the first large cross-scale
read-path gain without discarding the authoritative float32 artifact. The
source document/embedding store and mutation WAL remain float32; only the
immutable mmap base is encoded as float16, and source-sequence/revision misses
retain the primary fallback. On the matched 1M generation, recall changed only
from 0.99061 to 0.99047. Server p50/p95/p99 fell from
27.41/75.94/108.72 to 18.30/43.97/71.94 ms, client p50/p95/p99 fell from
28.64/77.39/109.91 to 19.65/45.25/73.19 ms, and exact-payload read p50 fell
from 17.12 to 7.79 ms. Cache-inclusive RSS fell from 3.38 to 2.28 GB while
attributable demand stayed about 434 MB. The base shrank from 3.162 to
1.626 GB, published in 40.02 seconds, used 57.4 MB at the governed builder
slice peak, and had zero misses, fallbacks, pressure events, or rejections.

The matched 50K comparison generalized the result. Float32 versus float16
recall was 0.98514 versus 0.98482, a 0.032-percentage-point change. Server
p50/p95/p99 improved from 15.69/29.81/53.68 to 9.37/12.61/13.97 ms and
client p50/p95/p99 from 17.45/32.19/55.28 to 11.37/14.72/16.71 ms. Restart
RSS fell from 457 to 380 MB and the base from roughly 312 to 158 MB. Its
coverage-only-WAL, 64-to-128-shard migration published in 3.43 seconds with a
48.2 MB builder-slice peak and no fallback or resource pressure.

That 50K migration deliberately found two publication invariants that the
already-128-shard 1M base did not exercise. First, a complete base replacement
was incorrectly forbidden from changing shard count. Second, the validator
treated a WAL containing only a coverage certificate as mutation debt. The
fixed contract permits an all-shard replacement to re-shard only when replay
contains no committed upsert or tombstone; coverage-only frames are subsumed by
the pinned snapshot. Sparse deltas and any mutation-bearing WAL retain the old
shard identity. Tests cover both the allowed coverage-only 4-to-8 replacement
and rejection of the same change after a committed vector mutation.

The current format version adds a per-vector float32 scale to float16 blocks.
Ordinary embeddings retain scale one; larger finite vectors are divided into
the safe float16 domain and rescaled during decode, while non-finite inputs
fail before mutating writer state. Readers remain compatible with v1 float32
and v2 unscaled float16 bases. Float16 is now the native projection default,
with an explicit float32 qualification/rollout override; this does not change
the precision of authoritative storage or recovery.

The first fresh scaled-float16 (`AFVBLK` v3) public lifecycle established the
current 50K end state. The official VectorDBBench load inserted in 22.16
seconds and reached full index/vector-projection readiness in 22.20 seconds,
with 0.9857 recall. The detailed 1,000-query public profile measured
7.75/9.75/11.21 ms client p50/p95/p99 and 5.97/7.88/9.08 ms server latency;
QPS at concurrency 1/10/30 was 95.5/966.5/1,083.8. Live/restart RSS was
926/368 MB and restart demand was 120 MB. The v3 block encoded 50,000 source
vectors into 158.3 MB in 3.06 seconds. The table had only the external vector
index during load; no full-text consumer participated.

The matching fresh 1M public lifecycle inserted in 751.66 seconds and became
fully ready in 755.71 seconds (12.60 minutes), correcting the reported
roughly-3,000-second batch-100 result while preserving 0.9901 official recall.
The final vector generation encoded 1,000,000 768D source vectors into 1.630 GB
in 30.82 seconds. Total disk was 5.54 GB: approximately 3.50 GB authoritative
primary document/artifact LSM, 1.59 GB shared exact-vector projection, 448 MB
native posting generations/WAL, and only 324 KiB in the compatibility HBC LSM.
Live RSS peaked at 3.41 GB; a clean restart used 3.01 GB RSS and 663 MB
attributable demand. Four concurrent public loaders were slower than the
earlier roughly-592-second serial result, so this meets the expected 13-minute
gate but does not close the remaining primary-store contention work.

That run also exposed a benchmark lifecycle contamination. VectorDBBench wrote
`key:__circus_write_probe__` before every skip-load search phase. On the first
1M reopen those non-vector source revisions overlapped bounded posting repair,
grew the posting WAL from source sequence 10001 to 10003, and made the supposed
cold measurement describe a mutated system. The adapter now has an explicit
read-only-reuse contract: query-only qualification verifies the existing table
and vector index but cannot issue the sentinel, remove full text, or create an
index. The repo runner also gates restart queries on zero posting backlog and
uses this contract for cold, warm, and concurrent reuse phases.

A controlled read-only rerun proved that boundary. The WAL mtime and size
remained exactly unchanged at 64,571,893 bytes, source applied/target sequence
remained 10003, and the reopened process reported zero dirty postings and zero
maintenance mutations. Genuine cold 1M serial p50/p95/p99 was
20.2/57.2/94.0 ms; the immediately warm official run was
14.2/16.9/18.9 ms at 0.9901 recall. A separate 100-query public profile measured
14.38/16.83/17.77 ms and 0.9895 recall. The stable 1,000-query auto-directory
control remains 14.50/17.47/21.54 ms at 0.99005 recall. `flat_rabitq` was
slightly faster but fell to 0.97423 recall, so the exact persisted directory
remains the production default.

Finally, `DB.runUntilIdle` had applied only one bounded 64-posting repair page.
That was safe for ordinary background maintenance but wrong for an explicit
lifecycle fence: large loads could return and reopen with later pages still
pending. The idle path now repeats bounded, query-cooperative pages until the
posting backlog is clean, without busy-waiting on asynchronous checkpoints or
the vector-block debounce. A regression dirties more than 64 leaves and proves
that one `runUntilIdle` call drains every page. Foreground/background write
paths remain bounded and may defer to active queries.

The fresh post-drain 50K gate showed that the honest fence does not regress the
small-corpus load target. Insert took 22.0018 seconds and complete readiness
22.0255 seconds, with 0.9860 official recall. Live serial p50/p95/p99 was
5.1/6.8/8.8 ms and QPS at concurrency 1/10/30 was
148.6/1,105.0/1,097.6. Read-only cold and warm restart p50 were both 6.2 ms.
The 1,000-query public profile measured 6.67/8.59/9.68 ms client and
4.97/6.85/7.70 ms server p50/p95/p99 at 0.98582 recall. Restart RSS was 374 MB
and its attributable footprint ledger was 120 MB. This supersedes the earlier
22.20-second v3 gate for current-code latency and lifecycle behavior.

The matching post-drain 1M load inserted in 737.35 seconds and the legacy
VectorDBBench optimize predicate returned at 745.53 seconds (12.43 minutes),
with 0.9899 recall. Steady live serial p50/p95/p99 was
14.2/17.0/18.2 ms and concurrency 10/20/30 reached
226.2/232.8/204.6 QPS. Live RSS peaked at 2.82 GB and the post-load process
footprint ledger at 1.90 GB. Disk was 5.61 GB: 3.53 GB primary LSM, 1.63 GB
float16 vector blocks, and 446 MB native HBC state.

That run also isolated a remaining readiness cliff without the old write
sentinel: concurrency one issued a single request that took 40.96 seconds.
The native vector generation was still encoding 1.63 GB from 3.10 GB of
authoritative artifacts and published in 40.78 seconds; the first request
arrived after the legacy adapter accepted `rebuilding=false` but before that
projection and the final 73 centroid repairs had completed. The adapter now
requires Antfly's authoritative readiness state, current replay watermarks,
zero dense publication/projection debt, and zero dirty postings. `runUntilIdle`
also preserves the ordinary bounded maintenance side effects, drains every
posting page, publishes the vector generation, and rechecks posting debt before
returning. This is a lifecycle fence, not a benchmark warm-up.

Primary ingest remained slower than the earlier 592.39-second four-worker
control even though both used batch 100 and the same client concurrency. The
post-drain run rotated 3.08 GB of mutable state through 51 current-scan epochs,
versus 1.26 GB through 22 epochs in that control. The 128 MiB WAL checkpoint
floor had coupled mutable-generation size to the 256 MiB immutable merge
window, recreating the large scan/snapshot surface that the window was meant
to remove. The next controlled A/B restores the independent 32 MiB mutable
floor while retaining WAL-backed epochs, the 256 MiB streaming linear merge,
four-way L0 tiers, and ratio sealing.

## 2026-08-26 end-state qualification

The linear-merge/WAL follow-up found that a logical immutable merge window must
not be forced out by every size rotation. Idle and WAL-pressure rotations still
publish partial windows, while ordinary size rotations accumulate to the
configured window. The WAL checkpoint floor also has a two-segment lower bound
for windowed publication so a segment-straddling retained tail cannot recreate
tiny flush cascades. Raising the resident mutable cap to 768 MiB was rejected:
it increased 1M readiness to 858.29 seconds and peak RSS to 3.17 GB without a
query benefit. The production cap remains 256 MiB.

Native posting checkpoints had another independent write-amplification bug.
The file format already supports eight immutable deltas, but managed policy
flattened after two. Using the format limit reduced the 1M load from five full
posting rewrites to two. On the final public batch-100/four-worker load, insert
completed in 715.28 seconds. Primary storage performed six pressure
compactions with zero overloads, flushed 3.576 GB in 116 flushes, accumulated
3.565 GB of mutable snapshot copies plus 1.936 GB of read-snapshot rotations,
and retained seven lower-level runs. Live RSS peaked at 3.187 GB. This is a
real public API load with the default full-text index removed through the API;
it is not a direct-store or batch-size shortcut.

Exact-vector publication exposed two production issues. First,
`BudgetedAllocator` did not retry incremental growth after aggregate cache
reclamation, unlike ordinary ResourceManager admission. A 1M builder could
scan and spool the full corpus, fail with `ResourceBudgetExceeded`, delete its
work, and immediately repeat. Incremental growth now reclaims governed HBC and
LSM cache before denying the allocation. Second, asynchronous status
invalidation left a race in which clients could observe the preceding ready
snapshot during a long base build. The maintenance owner now publishes pending
from its leased writer before the build and publishes the terminal state on
success or failure. Projection encoding and physical shard geometry are both
part of readiness, so a policy migration atomically replaces `CURRENT` rather
than silently retaining a suboptimal generation.

The builder now uses 64 KiB partition buffers. A controlled same-corpus A/B
rejected 256 vector shards even though they built in 34.40 seconds: cold
p50/p95/p99 was 28.1/66.2/101.5 ms, warm was 22.0/28.9/31.1 ms, maximum QPS
was 213.4, and restart RSS peaked at 2.605 GB. The final 128-shard generation
built under the same 2 GiB resource envelope in 43.29 seconds, removed every
spool after atomic publication, and reduced cold latency to
19.0/41.5/59.5 ms. Warm p50/p95/p99 was 12.3/15.2/19.3 ms at 0.9903 recall;
QPS at concurrency 1/10/20/30 was 78.1/267.2/225.9/214.6. The 1,000-query
public profile measured 12.86/15.70/18.96 ms client and
11.51/14.27/16.12 ms server p50/p95/p99 at 0.99027 recall. Restart RSS peaked
at 2.527 GB and attributable demand at 625 MB. The final vector directory is
1.630 GB across 128 float16 blocks; the complete data tree is 5.455 GB.

The measured final insert plus same-corpus 128-shard build is 758.57 seconds
(12.64 minutes), inside the 13-minute goal. A fresh single-lifecycle repeat is
still required before treating that sum as publication-grade readiness rather
than qualified component timing. The small-corpus end-to-end gate did complete
in one lifecycle: 50K inserted in 20.02 seconds and was fully ready in
20.05 seconds, with 0.9862 recall, live serial p50/p95/p99 4.8/6.8/9.1 ms,
1,090 peak QPS, 1.017 GB peak live RSS, and 325 MB peak restart RSS.

## Native shadow certification and restart end state

A capture-free dense repair candidate exposed a cross-generation publication
hole. The replacement could reach its source tip and publish its ready marker
without ever creating a native posting generation. Vector blocks correctly
refused to bind without a matching durable posting sequence, but activation
could still select the candidate's compatibility HBC LSM. The resulting index
was complete and queryable yet restarted with roughly 70 MB of obsolete HBC
LSM state, 1.50 GB RSS, and 36--79 ms query latency instead of the native read
path.

Shadow readiness now certifies a complete native posting generation before its
first ready marker, validates and flattens the converged candidate outside the
short activation fence, and verifies only the final WAL tail while source apply
is paused. Capture-free builders bootstrap one complete checkpoint directly
from their private compatibility projection; only after that checkpoint is
durable may they publish `AUTHORITY`, detach the live LSM, and bind a vector
generation at the identical `covered_source_sequence`. A native-first reopen
then removes only the obsolete `runs`, `wal`, manifest, and lock artifacts,
while preserving posting segments, generation identity, and active query
leases.

The repaired 50K generation demonstrated the intended restart state. Its
native-only HBC directory was 22 MB instead of 70 MB, the complete data tree
fell from 545 MB to 496 MB after safe legacy cleanup, restart RSS was 309 MB,
and a 1,000-query public profile measured 5.48/6.95/7.93 ms client and
3.80/5.16/6.16 ms server p50/p95/p99 at 0.98457 recall. Warm concurrency
1/10/20/30 reached 208/1,198/1,408/1,241 QPS. This was a migration of an
existing generation, so a fresh lifecycle remained the correctness gate.

The fresh public-API gate then inserted 50,000 1536D vectors in 21.11 seconds
and reached authoritative readiness in 21.13 seconds. The vector base encoded
in 2.60 seconds, live peak RSS was 1.147 GB, and restart peak RSS was 394 MB.
Cold and warm VectorDBBench recall were both 0.9862 with 6.3 ms p95 and
7.2--7.3 ms p99. The separate 1,000-query public profile measured
5.52/7.13/7.93 ms client and 3.85/5.26/6.18 ms server p50/p95/p99 at 0.98617
recall. Full text was removed through the public table API before load; the run
used batch 100, four public writers, native HBC WAL/segments, float16 vector
blocks, and read-only restart phases.

That fresh run also caught an empty-index lifecycle bug before the final gate.
An empty root intentionally has no quantized payload, but stable validation
treated that canonical absence as corruption and repeatedly repaired it back
to absence while holding structural admission. Missing payload is now valid
only for an empty posting; non-empty absence remains corrupt. Stable validation
also retries one fresh-lease no-progress observation, requires a clean
verification pass, and fails after bounded repeated mutations. This prevents
both premature native readiness and an infinite WAL rewrite if some future
payload cannot converge.

The next fresh gate found that a time-only quiescence test could still mistake
an LSM backpressure pause for the end of a burst. At sequence 204, with only
about 20,300 source rows, the primary still held 19 immutable memtables,
266.8 MB of immutable state, and a maintenance score of 111,276; nevertheless
the optional vector publisher built a 64 MB intermediate base. Opportunistic
publication now requires a complete quiet interval after the primary has zero
immutable state, WAL checkpoint/pressure debt, active compaction jobs, and
maintenance score. Caller-owned stable lifecycle fences remain immediate.

With that gate, the fresh public-API 50K run inserted in 20.87 seconds and was
ready in 20.90 seconds. It published no non-empty vector base before the final
source sequence 501. Recall was 0.9851 before and after restart; live p95/p99
was 6.1/7.0 ms, cold and warm p95/p99 was 6.4/7.8 ms, and peak QPS was 1,316.
The 1,000-query public profile measured 5.43/7.27/9.17 ms client and
3.70/5.11/5.98 ms server p50/p95/p99. Restart RSS was 321 MB. The 490 MB data
tree consisted primarily of the source document/artifact LSM, 151 MB of
float16 exact-vector blocks, and a 22 MB full native posting segment.

The first uninterrupted 1M lifecycle with the same online topology completed
insert-to-ready in 1,078.48 seconds (17.97 minutes). This is a large correction
from the reported 3,000 seconds, but it remains above the roughly 13-minute
target. The only non-empty exact-vector base published at final sequence 10,001
and encoded 3.072 GB of source vectors into 1.630 GB of float16 blocks in
36.1 seconds. Final posting validation flattened to a 279 MB native segment.
Thus, vector publication itself accounts for less than a minute; primary LSM
pressure and compaction set the remaining load curve.

The primary finished with 25 runs (18 L0), zero active maintenance job and zero
maintenance score, after 13 compactions read 7.754 GB and wrote 7.677 GB. Six
foreground pressure events all completed a pressure compaction, but ingestion
followed a reactive sawtooth near the 128-run hard limit. Direct sorted ingest
succeeded for 4.504M of 5.000M physical entries; 0.497M fell back while the
backend was pending. Cumulative mutable snapshot copies were 3.707 GB and read
snapshot rotations were 3.535 GB. The complete tree used 5.1 GiB: approximately
3.52 GB of source document/artifact runs, 1.63 GB of vector blocks, and 279 MB
of native HBC postings.

Recall was 0.9900 live, cold, and warm. Live queries overlapped the tail of
post-ingest storage work and were not publication-quality: serial p95/p99 was
47.5/68.5 ms, peak QPS was 87.6, and one concurrency-30 wave stalled for about
40.9 seconds. Reopened results isolate the native query path: cold p95/p99 was
15.7/21.8 ms and warm was 15.4/16.9 ms. The separate 1,000-query public profile
measured 12.97/15.42/16.48 ms client and 11.49/13.94/14.76 ms server
p50/p95/p99. It traversed 2,048 leaves and scored about 238K approximate
vectors/query before boundary rerank. Live/restart RSS peaked at 2.77/2.36 GB;
attributable restart demand was 409 MB.

A bounded recursive topology rebuild was then tested as an end-state
experiment. At 50K it consumed a measured 312.65 MB workspace and 4.6 seconds,
but reduced mean approximate candidates only from 24,189 to 23,927 and
regressed server p95/p99 from 5.11/5.98 ms to 6.17/7.38 ms. It is therefore not
a default-quality win. More importantly, that run exposed that the durable
tree was paired with a process-local "already rebuilt" sequence. Reopen reset
the marker, and coverage-only source advances from 501 to 503 rebuilt the same
tree twice more, consuming 4.4 seconds and 312.65 MB each time and changing
recall as randomized replacement trees published.

Topology rebuild identity is now durable in the same captured HBC metadata
transaction as the replacement root. Its epoch is the vector base generation,
the latest actual vector-WAL mutation sequence, and the algorithm. Generic
coverage commits do not change it. WAL replay reconstructs the same vector
epoch, while a real vector mutation, new base, or algorithm change admits one
new rebuild. This makes restart idempotent and prevents an optional optimizer
from silently becoming startup work; recursive topology remains opt-in until
it demonstrates recall/latency value. A fresh native-authority qualification
confirmed exactly one rebuild in the initial lifecycle and none after reopen:
root 441 and node count 1,315 were identical before and after coverage advanced
from sequence 501 to 503, recall remained 0.9831 live/cold/warm, and restart RSS
fell from the faulty run's 980 MB to 404 MB. Recursive still regressed ready
time to 22.39 seconds and server p95/p99 to 6.32/7.57 ms, so the 20.90-second
online topology run remains the qualified default.

### Exact-vector publication is part of readiness, not restart work

The first proactive-primary 1M run appeared to improve insert-to-ready from
1,078.48 to 740.27 seconds, but it was not a valid result. The public index
became ready while its exact-vector generation still covered the empty source
sequence. Reopen then built all 1M float16 vectors in 65.15 seconds. Query
visibility and recall happened to remain correct because the live process
could fall back to the primary LSM, but that made readiness, live latency, and
restart cost depend on an implementation fallback rather than the published
index generation.

Readiness now verifies source sequence, encoding, shard count, and exact vector
cardinality under one immutable generation lease. A vector mutation marks the
projection dirty and invalidates the old generation while holding the shared
publication mutex; a later vector-neutral transaction cannot advance the old
empty generation's coverage. The public status projects this writer-owned
pending fact as active finalization, so a client cannot observe ready between
derived catch-up and exact-vector publication. This ordering is crash-safe:
`CURRENT` remains on the last complete generation until the replacement base
is fully written and verified.

Waiting for the primary LSM to become completely idle before this optional
publication was correct but unnecessarily slow. A fresh 50K run inserted in
23.57 seconds, then waited 29 seconds for a small immutable tail to reach its
age-based flush threshold; the vector build itself took only 3.8 seconds.
Opportunistic publication now admits a stable primary tail only when there is
no WAL checkpoint or pressure block, hard L0 run/byte debt, or active
compaction, and the tail is bounded to 32 immutable memtables and 256 MiB.
The outer source-idle lease, two-second source debounce, and resource-manager
builder reservation remain mandatory. This is a general bounded-overlap rule,
not a VectorDBBench shortcut.

The resulting public-API 50K qualification inserted in 20.70 seconds and was
fully ready in 26.75 seconds. Generation 2 encoded all 50K vectors at source
sequence 501 in 2.79 seconds before readiness, with no restart rebuild. Recall
was 0.9849. Public client p50/p95/p99 was 5.45/6.99/8.06 ms and server
p50/p95/p99 was 3.74/5.15/6.20 ms. Live concurrency 1/10/20/30 reached
183/860/909/945 QPS. Live/restart peak RSS was 732/318 MB; attributable live
demand was 342 MB. The primary still had one safe 7.5 MB immutable tail at
publication, demonstrating that the removed wait—not weaker vector work—was
the speedup.

### Qualified proactive-tiering 1M end state

The uninterrupted 1M public-API qualification inserted in 663.75 seconds and
was fully ready in 717.10 seconds (11.95 minutes), beating the approximately
13-minute target. The exact-vector generation encoded all 1M source vectors
into 1.630 GB of float16 blocks at sequence 10,001 in 37.07 seconds before
readiness. The native posting store then flattened its seven-delta chain into
a 279 MB full generation. Reopen performed neither vector nor posting rebuild.

VectorDBBench measured 0.9902 recall and 0.9918 NDCG, with serial p95/p99 of
15.7/17.7 ms and peak throughput of 218.5 QPS on the contended development
host. Cold/warm reopen retained 0.9902 recall and measured 15.9/24.8 ms and
15.4/17.1 ms p95/p99 respectively. The separate 1,000-query public profile
measured 12.95/18.17/28.47 ms client and 11.48/14.95/22.05 ms server
p50/p95/p99. Live/restart peak RSS was 2.72/2.41 GB, while attributable demand
was 1.70/0.40 GB under the 2 GB process budget; mapped and reclaimable file
pages account for much of the RSS gap.

Primary proactive tiering limited the run to one completed pressure event.
Final primary state had 33 L0 runs, seven lower-level runs, no immutable tail,
and zero maintenance score. Cumulative primary mutable snapshot copies were
2.413 GB versus 3.707 GB in the 1,078-second baseline. The next load-side
opportunity is reducing current-scan copying and compaction write
amplification without returning to the reactive hard-limit sawtooth.

The dominant 1M query cost is now the exact flat centroid/quantized scan:
queries visit 2,048 leaves and score about 240K approximate vectors, consuming
11.93 ms mean HBC time. Exact boundary rerank loads about 448 vectors and costs
2.40 ms, of which artifact reads are 2.10 ms. Query work should therefore
prioritize recall-preserving routing/scoring layout and SIMD before changing
the rerank boundary. Packing immutable exact-vector shards into fewer indexed
container files is still worthwhile for metadata, restart, and artifact-read
tails, but it cannot by itself remove the larger approximate-scan cost.

## Native first-load transaction and linear consolidation (r55)

The first production bulk build must participate in the same exact-vector
capture as incremental `batchApply`. The initial implementation gated capture
on a nonzero HBC cardinality, and the recursive bulk-build wrappers bypassed
capture entirely. That made the empty sequence-one vector base stale on the
first replay window and forced a later primary-LSM reconstruction. Both gates
are now removed: ordinary and prepared-input bulk builders publish their
coalesced exact mutations into the HBC-native vector WAL transaction.

At a stable source tip, maintenance now treats a sequence-aligned native
WAL/delta generation as self-contained. It force-checkpoints the vector WAL to
the target encoding, merges base plus deltas one hash shard at a time, omits
latest tombstones from the complete replacement base, atomically publishes
`CURRENT`, and then validates/flattens the posting generation under the same
readiness fence. Only a missing generation or a base-only cardinality mismatch
uses the guarded pinned primary scan. This prevents an upload lull from causing
generic LSM snapshot cloning and prevents readiness from exposing a compact
vector base beside a large query-time posting overlay.

The public API batch-100 50K r55 qualification measured:

- 19.9995 s insert + 6.0391 s optimize = 26.0385 s ready;
- 0.9842 official recall and 0.9866 NDCG;
- 1,366.16 peak QPS; live serial p95/p99 6.7/8.9 ms;
- reopened detailed public p50/p95/p99 5.62/7.73/10.07 ms at 0.98421 recall;
- 1.484 GB cache-inclusive live peak RSS, 392 MB restart peak RSS, and
  115 MB attributable restart demand;
- one 154,800-KiB float16 vector base, one 23-MiB posting segment, and an
  empty current posting WAL.

The necessary A/B was r54. Publishing only the vector base left a 58-MiB
posting WAL instead of the 23-MiB immutable segment. Exact rerank candidates
rose from roughly 272 to 1,178, mean leaf scoring rose from 1.37 ms to 8.42 ms,
reopened public p95 rose to 15.75 ms, and live RSS peaked at 2.34 GB during the
query phase. Joining posting flattening to the stable-tip publication restored
latency and reduced the live peak by about 856 MB without changing the rerank
policy boundary.

## Native 1M qualification and bootstrap-linear checkpointing (r56)

The same public API lifecycle at 1M completed without retry, index
reactivation, capture-boundary failure, or primary-LSM vector reconstruction:

- 599.7847 s insert + 20.4622 s optimize = 620.2469 s ready, versus the r44
  843.5063 s insert + 81.7756 s optimize = 925.2819 s ready baseline;
- 0.9899 recall and 0.9916 NDCG, preserving the existing rerank boundary;
- 426.90 peak QPS, versus 235.58 in r44, with live serial p95/p99 of
  14.2/15.5 ms;
- restarted 1,000-query p50/p95/p99 of 12.10/15.07/22.66 ms at 0.98994
  recall, versus 14.58/20.66/23.95 ms in r44;
- 1.30 GB attributable live demand, versus 1.70 GB in r44;
- 4.773 GB cache-inclusive live RSS and 2.894 GB restart RSS, versus
  2.720/1.948 GB in r44;
- one 1,629,901,750-byte float16 vector base, one 280,684,301-byte posting
  segment, and empty mutation WALs. Total allocated data was 5,326,904 KiB,
  essentially unchanged from r44's 5,317,360 KiB because primary embedding
  ownership remains deliberately out of scope.

The RSS high-water occurred about 449 seconds into the load, not during final
publication. The vector manifest produced roughly 40 immutable generations.
Its former flat eight-delta limit repeatedly coalesced all accumulated
first-load vectors even though the bootstrap base was empty and the batches
were predominantly disjoint. This kept demand bounded but touched an
ever-growing set of clean mmaps and rewrote the same f16 projection repeatedly.

The r59 A/B allowed an empty base to append up to 64 immutable delta
generations, while an established base retained the eight-generation online
lookup limit. It measured:

- 503.9492 s insert + 24.4342 s optimize = 528.3834 s ready, 91.86 s (14.8%)
  faster than r56;
- 0.9903 recall, 536.55 peak QPS, and live serial p95/p99 of 13.5/13.9 ms;
- restarted detailed p50/p95/p99 of 11.45/13.94/20.89 ms at 0.99033 recall;
- 4.954 GB cache-inclusive live peak RSS and 1.70 GB sampled attributable
  demand, versus 4.773/1.30 GB in r56.

The 64-run policy therefore proves that repeated first-load rewrites cost
about 92 seconds, but it is too permissive as the final residency policy. The
production candidate checkpoints an empty bootstrap chain at 24 generations,
which should cause one mid-load coalescence on this corpus. The durable format
continues to admit 64 generations so tightening policy never makes a
previously valid `CURRENT` unreadable during restart or rolling upgrade.
Stable-tip maintenance still performs the complete shard-local merge,
atomically publishes `CURRENT`, and flattens postings under the same readiness
fence. This is a production first-load policy, not a batch-size exception:
crash recovery and queries continue to see every committed WAL/delta
generation, and ordinary online update fan-out is unchanged.

The 24-run r60 public qualification performed exactly one bootstrap
coalescence near 650K rows and measured 537.6565 s insert + 23.1201 s optimize
= 560.7766 s ready. This retains 59.47 seconds of the r59 speedup while the
4.814 GB cache-inclusive peak is effectively tied with r56. Recall was 0.9900;
the restarted detailed p50/p95/p99 was 11.79/14.43/23.55 ms. The format/policy
split therefore gives a better default balance than either eight or 64 runs.

r62 then released clean mmap residency after each input shard was durably
staged during delta and complete-base compaction. It preserved load throughput
(539.2666 s insert + 25.4540 s optimize = 564.7206 s ready), 0.9902 recall,
and live p95/p99 of 14.2/16.3 ms. It lowered RSS by approximately 135 MB at the
mid-load merge and reclaimed roughly 1.8 GB promptly after final publication,
but did not lower the historical high-water: validation had already touched
every new block's sorted index and key boundaries.

An r64 follow-up tested releasing those validation-touched pages immediately
after admission while retaining the immutable mapping. Reject this policy. It
made the 1M insert 601.0115 s (61.74 s slower than r62), raised live peak RSS
from 4.851 GB to 5.164 GB, and raised restart RSS from 2.624 GB to 2.891 GB.
Recall remained 0.9900 and detailed p50/p95/p99 was
11.60/14.19/21.87 ms, so there was no compensating query benefit. RSS briefly
fell to 1.10 GB after the mid-load merge, but continued mutation and final
publication refaulted the same pages and produced a higher high-water. Keep
post-shard maintenance reclaim from r62; do not evict a newly admitted
generation before its normal workload establishes actual residency.

The next query experiment kept the search effort and rerank boundary fixed and
changed only the AArch64 RaBitQ weighted-popcount reduction. Zig's prior
`@Vector(4, u64)` horizontal reduction lowered to repeated widening and scalar
extract sequences on NEON. Reducing 128-bit byte popcounts instead lowers to
`CNT` plus `UADDLV`; every other architecture retains the previous kernel. A
240K-candidate, 768-dimensional warm microbenchmark improved by 8.1%, with
identical integer results. Differential tests cover widths 0 through 32.

The r66 50K public lifecycle confirmed that the kernel win survives traversal
and heap admission: mean leaf scoring fell from 1.288 ms in r61 to 1.189 ms
(7.7%), mean HBC search fell from 3.817 to 3.706 ms, and peak QPS reached
1,649.6. Insert plus catch-up was 23.3697 + 6.0372 = 29.4069 s; recall was
0.9811, and restarted cold/warm p95 was 6.0/5.9 ms. The bit kernel is exact, so
the 0.21-percentage-point recall difference from r61 is concurrently-built tree
variance rather than approximate-math drift.

The r67 1M lifecycle then measured 556.4232 s insert + 35.1253 s catch-up =
591.5485 s ready, 0.9900 recall, and live serial p95/p99 of 13.8/14.9 ms. Mean
leaf scoring was 6.727 ms versus 7.223 ms in r62 (6.9% lower). Cache-inclusive
live peak RSS was 4.457 GB versus 4.851 GB in r62; restart RSS remained
effectively unchanged at 2.570 GB. The 2.20 GB physical-footprint sample is
within the run-to-run/host noise of r62's 2.27 GB and must not be claimed as a
demand reduction. The next query costs are the remaining 6.73 ms leaf scan and
2.02 ms exact-vector artifact read, not tree expansion (0.83 ms) or exact
distance arithmetic (0.05 ms).

The native exact-vector follow-up scores aligned little-endian float16 block
payloads directly instead of expanding them into request-sized float32 scratch
and then reading that scratch again. Conversion, query dot product, candidate
norm, and distance now share one SIMD pass. Payload CRC verification, immutable
generation leases, exact source-sequence equality, primary fallback, and the
rerank boundary are unchanged. Float32, WAL, unaligned, and big-endian values
retain the decoded path. Metric-parity tests cover L2, inner product, and
cosine including scalar tails.

r68 at 50K lowered mean native artifact read from 1.977 to 1.864 ms (5.7%) and
exact-vector load from 2.083 to 1.976 ms (5.1%) versus r66. Mean HBC search was
3.647 ms, peak QPS was 1,928.4, recall was 0.9820, and the complete public
lifecycle was 20.2534 s insert + 8.0855 s catch-up = 28.3389 s ready. r69 at
1M lowered artifact read from 2.021 to 1.875 ms (7.2%) and vector load from
2.331 to 2.185 ms (6.2%) versus r67. Mean HBC search was 10.788 ms, peak QPS
was 472.6, and recall was 0.9897. The lifecycle measured 569.5528 s insert +
2.0423 s catch-up = 571.5951 s ready. Its 4.852 GB live peak matched r62,
restart RSS was 2.563 GB, and sampled demand was 1.90 GB; do not attribute the
memory movement to this allocation-free query kernel. The concurrency-one
sample contained one 15.7-second cold/host stall and is not a steady-state
latency result.

Reader admission already validates the complete immutable index, every entry
range/flag/scale, the index checksum, every key checksum, and total hash/key/
source ordering. A further native lookup fast path reuses the artifact hash and
trusts those admitted index/key regions for the mmap lease lifetime instead of
re-parsing invariants and recomputing key CRCs at every binary-search step.
Per-vector payload CRC remains lazy and mandatory; checked admission and
compaction iteration are unchanged.

r70 at 50K lowered artifact read from 1.864 to 1.802 ms (3.3%), vector load
from 1.976 to 1.905 ms (3.6%), and mean HBC search from 3.647 to 3.532 ms
versus r68. Recall/restart was clean at 0.9840. A query-only reopen of r69's
identical 1M durable generation proved upgrade/restart compatibility and
preserved recall exactly at 0.98967. The first cold profile faulted mmap pages
and is intentionally not a steady-state comparison. An immediate warm repeat
lowered artifact read mean/p50 from 1.875/1.702 to 1.835/1.650 ms, vector load
from 2.185 to 2.122 ms, and mean HBC search from 10.788 to 10.401 ms; server
p95 fell from 13.02 to 12.21 ms on the same topology and source generation.

## Memory methodology

Use Circus's native `footprint_sampler.py` against the Antfly server process
tree and capture the wired-memory baseline immediately before server start.
Datasets must already be cached. A valid publication number requires three
fresh lifecycles and reports mean plus range.

For native macOS runs, the primary demand number is the process tree's
`phys_footprint` ledger high-water. System-wide wired growth is reported as a
separate conservative diagnostic because unrelated host activity cannot be
attributed to Antfly. RSS remains the cache-inclusive point-in-time view. Do not
poll native `vmmap` during a timed phase: invoke the sampler once immediately
afterward and use the kernel-maintained footprint high-water for the phase peak.
The qualification runner captures live and restarted processes separately.
Historical scripts invoked `vmmap` every 200--300 ms and materially contaminated
both load throughput and query tails; those timings are not publication data.

The first partial 1M sample is diagnostic only: dataset download occurred after
the wired baseline, contaminating the system-wide wired delta. Its
cache-inclusive process-tree peak was about 1.18 GiB and its physical-footprint
ledger peak was about 785 MiB, but its wired-demand headline must not be
published.

## Next checks

1. Narrow broad persisted L0 source ranges with adaptive, workload-independent
   range slices, then pair them with a lower soft compaction-input budget.
   Merely splitting leveled output files does not split the fixed-point overlap
   closure. Preserve the oversized progress escape for a minimum indivisible
   closure and verify that the extra L0 runs do not amplify write pressure.
2. Design bounded or incrementally publishable HBC leaf normalization inside a
   single replay transaction. Smaller replay windows and naïve internal apply
   chunks both regressed 50K throughput, so preserve source-write ordering,
   rollback semantics, and replay amortization.
3. Focus remaining snapshot-copy work on current scans, which accounted for
   127.6 MB of the 150.7 MB live aggregate in the external-admission run;
   bound-read copies were 23.1 MB. Do not replace multi-operation snapshots
   with unsafe live probes merely to improve a cumulative counter.
4. Repeat the final 128-shard/64-KiB-buffer float16 50K and 1M lifecycle three
   times through the post-phase-sampled, read-only-restart harness on a
   controlled host and publish mean plus range. In particular, measure insert
   and base publication in one uninterrupted 1M lifecycle; the current
   758.57-second readiness figure is a qualified sum from the same corpus.
5. Add deterministic fault injection at every posting-WAL append, fsync,
   checkpoint staging, `CURRENT` replacement, overlay allocation, and applied
   watermark boundary. The production ordering and fail-closed recovery paths
   now exist; exhaustive crash-matrix automation remains the release gate.
6. Move the WAL-authoritative store from rollout environment flags to an
   explicit catalog capability once mixed-version upgrade/downgrade policy is
   defined. Keep the persisted authority marker sticky and require an explicit
   source-journal rebuild to move back to the general LSM.
7. Reduce the remaining primary document/artifact contention. The final 1M
   load still accumulated 3.565 GB of mutable snapshot copies and 1.936 GB of
   read-snapshot rotations. Attribute those copies by reader class and compare
   four public workers against a current serial control before changing the
   benchmark's concurrency.
8. Reduce the native posting chain's 448 MB disk footprint and query-time mmap
   residency without weakening `covered_source_sequence`, atomic CURRENT
   publication, generation leases, or boundary rerank. Prefer indexed
   patch-native/chunked-copy-on-write deltas and measure recovery time as well
   as bytes.
9. Optimize the exact flat centroid scan itself—SIMD/block layout, cache
   residency, and bounded parallel scoring—while keeping the demonstrated
   0.990 recall. RaBitQ routing's 1.55-percentage-point loss is outside the
   parity budget and must not become the default merely for latency.
10. Compare the qualified 128-shard curve (0.9903 recall, 267 peak QPS) against
    current Circus competitors using identical corpus, payload, recall, and
    concurrency semantics. Report cold and warm separately; never mix source
    mutation or maintenance into the first concurrency sample.
11. Audit the remaining gap between 625 MB attributable restart demand and
    roughly 2.53 GB cache-inclusive RSS. Classify mmap residency, allocator
    arenas, primary run/index pages, cache leases, and runtime stacks before
    changing budgets; reclaimable file cache must not be mislabeled as demand.
