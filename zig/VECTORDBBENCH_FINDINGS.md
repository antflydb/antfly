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

## Memory methodology

Use Circus's native `footprint_sampler.py` against the Antfly server process
tree and capture the wired-memory baseline immediately before server start.
Datasets must already be cached. A valid publication number requires three
fresh lifecycles and reports mean plus range.

For native macOS runs, the primary demand number is the process tree's
`phys_footprint` ledger high-water. System-wide wired growth is reported as a
separate conservative diagnostic because unrelated host activity cannot be
attributed to Antfly. RSS remains the cache-inclusive view. The sampler still
polls RSS at the requested cadence, but rate-limits expensive `vmmap` ledger
reads to once per second per PID; the ledger itself preserves peaks between
reads. Earlier private scripts invoked `vmmap` every 200 ms and materially
contaminated load throughput.

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
4. Repeat the 1M lifecycle three times through Circus on a controlled host and
   publish its mean plus range. Three current-main 50K lifecycles now average
   41.65 seconds with a 38.72--43.14 second range, but the single follow-up 1M
   lifecycle remains diagnostic because compaction scheduling produced
   materially different curves on the same host.
5. Add deterministic fault injection at every posting-WAL append, fsync,
   checkpoint staging, `CURRENT` replacement, overlay allocation, and applied
   watermark boundary. The production ordering and fail-closed recovery paths
   now exist; exhaustive crash-matrix automation remains the release gate.
6. Move the WAL-authoritative store from rollout environment flags to an
   explicit catalog capability once mixed-version upgrade/downgrade policy is
   defined. Keep the persisted authority marker sticky and require an explicit
   source-journal rebuild to move back to the general LSM.
7. Profile primary document/embedding LSM snapshot rotation and post-load
   compaction at 1M. The posting store removed normal derived HBC writes, but
   2.52 GB of mutable clones, 3.24 GB of read-snapshot rotations, and immediate
   post-load compaction still dominate readiness, demand, and query tails.
