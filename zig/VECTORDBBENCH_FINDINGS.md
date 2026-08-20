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

## Measurements

Unless a row explicitly reports a mean and range, the times below are
one-machine diagnostics rather than publication-grade means.

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
| 1M Cohere, batch 100, four workers, original public path | incomplete | projected about 45–50 min | Throughput fell from about 30.8K docs/min in minute one to about 21.1K docs/min in minute four |
| 1M, bounded clones + fair coalescer control | incomplete at 783,201 rows / 1,867 s | -- | Two exact 120 s timeouts from primary L0 pressure; 3.83 GB clones, 1.40 GB demand, 3.76 GB peak RSS; 200 ms `vmmap` and overlapping compilers contaminate speed |
| 1M, rate-limited sampler + maintenance fair turn | incomplete at 592,001 rows / about 725 s | -- | No timeout, but live dense bulk mode still reached 537 aggregate L0 runs; 1.59 GB clones, 888 MB demand, 3.02 GB peak RSS |
| 1M, primary + dense hard pressure before relocation | 1,536.04 s | 1,644.37 s | 825 final L0 runs / 582 debt; 16 compactions from 26 pressure events, 1.60 GB demand, 3.89 GB RSS |
| 1M, relocated compaction publication | 1,099.85 s | 1,110.66 s | Full E2E completion; 10.81 s catch-up, 111 final L0 runs / zero debt, 1.20 GB demand, 4.21 GB RSS |
| 1M, instrumented replay-lane snapshot | 1,605.87 s | 1,617.08 s | Full E2E completion; 1.65 GB clones, 1.30 GB physical footprint, 3.19 GB RSS; zero debt; 2.57 GB / 52.6 s largest compaction |
| 1M, 128 MiB preferred primary runs (rejected) | 1,751.71 s | 1,753.77 s | 2.07 GB clones, 1.50 GB physical footprint, 3.72 GB RSS; zero debt; 2.562 GB / 55.15 s largest compaction |

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
