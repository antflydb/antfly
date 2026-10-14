# VectorDBBench Findings

> Paths under `.benchmark-results/` refer to local benchmark output that is not tracked in git.

This file records what the 50K and 1M Antfly VectorDBBench investigation
established: the benchmark contract, the product rules it produced, the current
reference numbers, the experiment flags still in the code, and the open work.
Keep benchmark-harness changes separate from product fixes. A vector-only
control should not do full-text work, but normal Antfly users who combine
full-text and vector indexes must still get bounded memory and stable ingest
throughput.

Status: native HBC posting/WAL generations and shared exact-vector blocks are
the production dense path. Fresh local single-shard standalone tables default to
`vector_store` source ownership ([VECTOR_STORE.md](VECTOR_STORE.md#scoped-default-promotion)).
New posting builds omit the duplicate posting-local projection plane. Every other
`ANTFLY_EXPERIMENT_*` treatment listed in
[Experiment flags](#experiment-and-benchmark-flags) is off by default unless the
table says otherwise.

> **Relocated:** The dated investigation log (August to mid-September 2026) is
> preserved verbatim, split by period:
> [source-store qualification narrative](../docs/design/vector-store/history/vectordbbench-source-store-2026-09.md),
> [confirmed findings and native HBC measurements through r126](../docs/design/dense-indexing/history/vectordbbench-native-hbc-2026-08.md),
> [bounded publication, r129 and V2 directory experiments (09-03 to 09-05)](../docs/design/dense-indexing/history/vectordbbench-2026-09-03-05.md),
> [prepared publication, recovery and subgroup routing (09-05 to 09-07)](../docs/design/dense-indexing/history/vectordbbench-2026-09-05-07.md), and
> [no-copy default, posting-row experiments and ownership comparison (09-07 to 09-14)](../docs/design/dense-indexing/history/vectordbbench-2026-09-07-14.md).
> Native posting/WAL format invariants now live in [HBC.md](HBC.md#native-posting-and-wal-generations).
> The V1-mirror/native-v2 authority rules now live in
> [DENSE_INDEXING_LIFECYCLE.md](DENSE_INDEXING_LIFECYCLE.md#physical-index-versioning-and-rolling-upgrades).

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

Qualification rules that apply to every comparison:

- Datasets: OpenAI 1536D for 50K and Cohere 768D for 1M; cosine, k=100,
  default effort, 30-second concurrency phases at 1/10/20/30. Absolute QPS at
  the two sizes is not a size-only scaling result.
- Run arms sequentially on the same host with the same binary, in alternating
  (AB/BA) order. Record the binary hash and source-state identity; a recorded
  HEAD alone does not identify the measured executable.
- An arm with request errors or client retries fails qualification even if the
  client exits zero. Do not stitch cells from different runs (for example a
  restarted retry's c30 point) into one curve.
- Report readiness as insert plus catch-up, and report live, cold-restart and
  warm-restart query results separately. Mixed phases run for equal time, so
  report completed write counts; fixed-duration runs with different write
  volumes are not equal memory workloads.
- RSS is cache-inclusive. Report attributable demand and the physical-footprint
  ledger separately and do not call reclaimable file cache demand. A one-sample
  post-phase demand value is not a high-water mark.
- Do not compare clone-byte or compaction counters as RSS: they are cumulative
  allocation/copy work.
- Before comparing ANN neighbor lists across an operation, wait for a clean
  posting-refresh certificate (`hbc_posting.refresh_pending` false, see
  [DENSE_INDEXING_LIFECYCLE.md](DENSE_INDEXING_LIFECYCLE.md#status-and-metrics)).
  Dirty postings fall back to exact member scoring, so an idle repair can change
  approximate neighbors for the same corpus.

Harness and controllers live in the repository-root `scripts/` directory (`run_vdbbench_qualification.sh`,
`validate_vdbbench_result.py`, `run_vector_store_ab.py`,
`run_posting_locality_ab.py`, `native_preparation_experiments.py`,
`preserve_vdbbench_baseline.py`). Frozen comparison catalogs are in
[benchmark-baselines/](benchmark-baselines/README.md); their archives under
`.benchmark-assets/baselines/` are local and Git-ignored.

## Confirmed findings

Each rule below is a product decision, not a benchmark switch, unless it says
otherwise. The supporting measurements are in the history files listed above.

### Benchmark isolation from the default full-text index

`{"num_shards":1}` table creation adds `full_text_index_v0`, whose replay worker
runs during a vector load even though `sync_level=write` does not wait for it.
The Antfly VectorDBBench adapter deletes that index through the public API
before creating the external dense index. Set
`ANTFLY_VDBBENCH_KEEP_DEFAULT_FULL_TEXT=1` for a mixed-index diagnostic. This is
benchmark isolation, not the mixed-workload fix; the replay-lane rule below is.

### Storage adapters must preserve point probes and multi-get

A snapshot read clones the mutable generation; a point probe reads the current
tip without cloning. Generic adapters (`DocStore.backendStore()` and the
namespace-erased store used by HBC) silently fell back to snapshots when they
did not advertise `beginProbe`, `beginCurrentScan`, and sorted `getMany`.
Fixing that cut a 5,000-row primary-only control from 1.10 GB of clone bytes to
27.6 MB. Transaction intent resolution now uses a durable, sorted intent-key
manifest committed with the intents and resolves those keys by current-tip point
reads. An in-flight transaction from an older binary falls back to one stable
prefix scan, and its next intent write publishes a complete manifest. Resolution
keeps an explicit four-byte empty manifest until terminal metadata cleanup so
recovery does not mistake a resolved record for a pre-manifest one. A
current-tip linear cursor was rejected: holding the backend mutex across the
walk serialized writers (69.95 s versus about 40 s at 50K).

### Replay readers snapshot only their append-only lane

A replay worker's wake previously cloned the complete mutable primary state.
The runtime LSM now snapshots only the requested replay lane and pins the
immutable/run generation at the same backend-lock point. With default full-text
and dense indexing, 50K copying fell from 3.51 GB to 46.3 MB. Exact replay
visibility compares the returned sequence; it never infers visibility from
sequence gaps. Stores without replay lanes keep the current-scan fallback.

### Primary checkpoint floor and compaction budget

- The adaptive WAL checkpoint floor stays at 32 MiB. A 1 MiB floor produced
  147 versus 59 final L0 runs and a slower 50K load (41.75 s versus 38.35 s).
- `max_compaction_input_bytes` defaults to 2 GiB. Foreground hard-pressure
  compaction honors the same input budget as background selection, keeping its
  scheduler-independent progress lane and the oversized minimum-closure escape.
  A 768 MiB budget increased 1M rewrite volume without bounding the largest
  closure (2.75 GB), so it was rejected.
- A 128 MiB preferred leveled file size was rejected. Smaller output files do not
  split an overlap-closed input closure, and the 1M load slowed from 1,617 s to
  1,754 s. Bounding the largest job (2.5–3.0 GB, 50–64 s at 1M) needs narrower
  persisted L0 source ranges (see [Open work](#open-work)).
- The LSM reports completed-job count, input/output byte totals, and the largest
  completed input, output and duration. Aggregation preserves maxima across
  primary and derived backends.
- Persisted compaction publication relocates its exact immutable input IDs and
  recomputes the target overlap closure against the current run version. A pure
  positional shift from concurrent flushes is accepted; a vanished input or a new
  overlapping target run rejects the output.

### Replay and compaction use separate admission lanes

Resource admission distinguishes replay-priority work from soft background
compaction. The derived backlog tracker bounds sequence-only debt at 200
sequences (resume at 100) and emits an asynchronous 16-sequence urgency window
(`derived_backlog_high_sequences`, `derived_backlog_resume_sequences`,
`derived_backlog_throttle_window_sequences` in `resource_manager.zig`). The
dense replay profile keeps bulk coalescing but still enforces hard L0 limits,
because a public replay stream is not a finite offline build.

### Dense replay sizing and batching

- Replay admission estimates use the index's configured dimension; the old
  384-D fallback applies only to callers without dimension metadata.
- Identity, vector-id, split-range metadata and transformed vectors load in
  sorted batches; a split reuses the matrix it already materialized.
- Live replay keeps quantized rebuild and leaf splitting inside bounded publish
  windows. The offline outer-finish mode is explicit and is not the
  public-write default.
- Smaller replay windows (4,096 items: 92 s at 50K) and splitting insert-only
  HBC applies into 4,096-item chunks (+10% load time) were both rejected. Bound
  leaf normalization inside one replay transaction instead.

### External indexes use an external-coverage admission fence

An `external: true` dense or sparse index cannot be populated from unrelated
source documents. Managed admission installs it at an activation fence after a
streaming key scan proves no matching caller-owned artifact predates the
catalog entry, then lets ordinary replay populate new artifacts. If matching
artifacts already exist, the durable rebuild path is retained. This removed
three spurious repair attempts (50K: 77.9 s to 42.1 s).

### Public write coalescer fairness

The elected drainer hands ownership to a waiting request as soon as its own
entry commits. Previously it could stay in the drain loop until the client's
120-second timeout under a continuously replenished queue.

### Compile reservations

ReleaseFast builds stay parallel. Scheduler reservations cover observed macOS
peaks: API 11 GiB, storage/distributed runtime 18 GiB, inference runtime
16 GiB, CLI 3 GiB, the broad data runtime test 12 GiB, and the metadata public
simulation 14 GiB. These are scheduling claims, not process limits; do not fall
back to `-j1`.

### Exact-vector publication is part of readiness

Readiness verifies source sequence, encoding, shard count and exact vector
cardinality under one immutable generation lease. A vector mutation marks the
projection dirty under the publication mutex, and status reports active
finalization until the replacement is published, so a client cannot observe
ready between catch-up and exact-vector publication. `CURRENT` stays on the last
complete generation until the replacement is written and verified.

Opportunistic (non-lifecycle) exact-vector publication requires a settled
primary write plane: no mutable rows, immutable memtables or active bulk batch,
no WAL checkpoint or pressure block, no L0 run or byte debt, and no active
compaction, followed by a 2-second quiescence debounce
(`primaryWritePlaneSafeForProjection`, `vector_block_quiescence_ns` in
`index_manager.zig`). The first base of at most 4,096 vectors publishes
immediately. Caller-owned lifecycle fences bypass the opportunistic policy and
build from a pinned snapshot.

### Serving-path decisions

- Writes route by exact child-centroid distance ([HBC.md](HBC.md#write-routing)).
- The exact persisted (`auto`) routing directory is the default. `auto` uses the
  recursive tree below 1,024 completed postings (`flat_exact_min_postings`). Flat RaBitQ and quantized
  centroid routing lost about 1.5 percentage points of recall and stay
  experiments. Lowering epsilon or effort to buy latency is not a default
  change: the public effort default stays 0.5.
- Positional I/O is the serving default. Mapped serving with or without
  `MADV_WILLNEED`, and positional readahead, raised RSS without a replicated
  latency win.
- Extra dense read helpers have a node-wide limit that defaults to twice the
  logical CPU count, independent of memory/bandwidth admission. Optional helper
  admission never waits; the admitted caller keeps draining its own queue.
- Native posting patch-cache entries are leased per request and accounted until
  their last reference, with a 64 MiB per-root retention ceiling and
  resource-manager eviction.
- Recursive topology rebuild stays opt-in. Its rebuild identity (vector base
  generation, latest vector-WAL mutation sequence, algorithm) is durable with
  the replacement root, so reopen and coverage-only advances never rebuild again.
- RaBitQ codes are for distance bounds. Structural split reconstruction from
  RaBitQ alone collapsed recall to 0.63 and was rejected.

### No-copy posting projections are the build default

New posting builds omit the duplicate posting-local projection plane. This
accepts the measured 50K throughput/restart cost for single-copy storage; it was
not an all-metric win. `ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS=1` retains
the locality-on plane. Existing planes stay readable and are not eagerly
rewritten. Subgroup training can borrow projections without retaining the plane.
Centralized projection reads, authoritative exact completion, source ownership,
effort, coverage and generation leases are unchanged.

The preserved same-binary references are
[`pr593-locality-matched-20260906.json`](benchmark-baselines/pr593-locality-matched-20260906.json)
(primary 50K/1M locality-on versus no-copy) and
[`pr593-locality-fast-50k-20260906.json`](benchmark-baselines/pr593-locality-fast-50k-20260906.json).
The release gates against them: recover locality-on-class 50K throughput and
tails while keeping no-copy disk, and improve the qualified no-copy 1M curve
without losing recall, RSS/footprint, mixed-write responsiveness, restart
behavior or durability. The ownership A/B pins locality on explicitly, so a
default change cannot silently alter that experiment.

## Current reference numbers

### Source ownership at 1M (2026-09-14)

Pinned binary `5a90e99d6e`; fresh single-shard standalone tables; float32
serving encoding; identical explicit ANN settings, durability, batch size,
concurrency and recall policy; all eight 50K/1M arms passed workload, strict
request-error, restart and reclamation gates. Receipts are in
`.benchmark-results/vector-store-owner-admission-20260914/RESULTS.md`.

| 1M pair/order | Ownership | Readiness s | Peak QPS | Mixed QPS | Mixed write rows/s | Mixed query p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 1 / AB | primary_lsm | 273.25 | 1144.2 | 211.29 | 4031.5 | 104.30 |
| 1 / AB | vector_store | 247.44 | 1210.3 | 232.80 | 3977.6 | 95.38 |
| 2 / BA | vector_store | 265.38 | 1225.6 | 225.02 | 3948.8 | 100.64 |
| 2 / BA | primary_lsm | 264.22 | 1141.9 | 189.00 | 3674.9 | 113.90 |

- `vector_store` improves peak QPS 5.8%/7.3%, mixed query QPS 10.2%/19.1% and
  mixed p99 8.6%/11.6%; readiness improves 9.4% in pair 1 and is equal in pair 2.
  Recall is 0.9901–0.9904 in all arms.
- Total allocated disk after restart, including journals, falls 43.4% in both
  pairs (6.74–6.77 GiB to 3.82–3.84 GiB). Maximum sampled RSS falls about 28%
  (9.35/10.06 GiB to 6.71/7.25 GiB).
- Tradeoffs: fixed-count update/delete churn is 4.4%/15.5% slower; reclamation
  settles in 149/155 s versus about 33 s (each collection reads 3.07 GB and
  writes 3.19 GB); updated-embedding readiness is 1.12/1.90 s versus 1.00/1.04 s.
- At 50K, `vector_store` uses 47–49% less disk, but peak QPS (+5.8%/−22.5%) and
  mixed QPS (−4.1%/−1.3%) are inconsistent. Post-mixed profiles resolve 0.225
  versus 45.9 metadata entries per query between the two vector-store runs.

### Audited historical baselines (2026-09-03)

Common contract: public HTTP, one shard, cosine, k=100, batch 100, four load
workers, native HBC plus float16 vector blocks, default effort, no full-text
index, 30-second concurrency phases. RSS is decimal GB, cache-inclusive, from
0.2-second samples. None of these runs included mixed ingest/query
qualification, and no single run wins every metric. Do not combine their best
cells into one baseline.

| Candidate | Ready s (insert + catch-up) | Recall | QPS c1/c10/c20/c30 | p95 ms c1/c10/c20/c30 | Live/restart RSS GB |
| --- | --- | --- | --- | --- | --- |
| 50K scratchpool | 30.36 (18.99 + 11.37) | .9885 | 293.8/1079.8/1192.0/1319.3 | 3.88/17.03/49.38/71.27 | 1.52/.34 |
| 50K row-block B | 24.38 (15.30 + 9.08) | .9857 | 283.6/1084.2/1203.2/1245.4 | 3.99/15.32/32.87/58.69 | 2.08/.34 |
| 50K admission + cold publication | 30.49 (20.44 + 10.05) | .9862 | 273.6/1109.2/1198.4/1229.0 | 4.19/14.76/34.46/59.56 | 1.88/.35 |
| 1M regression-recovery | 617.37 (548.91 + 68.46) | .9925 | 78.1/634.0/794.1/828.0 | 12.90/24.72/55.77/94.32 | 7.05/2.39 |
| 1M admission + cold publication | 724.25 (634.62 + 89.63) | .9924 | 70.7/626.8/701.6/649.6 | 13.85/24.68/66.73/87.48 | 6.26/2.37 |
| 1M kind-separated V2 | 716.46 (630.01 + 86.45) | .9927 | 58.5/568.4/596.2/585.0 | 15.05/27.96/78.99/105.61 | 5.73/2.37 |

Scratchpool is the 50K throughput/RSS reference and row-block B the faster-load,
lower-tail reference. Regression-recovery is the 1M speed reference;
admission + cold publication is the balanced c30-tail reference; kind-separated
V2 is a lower-memory alternative, not equal-speed proof. The r125 1M result
(775.94 s, 3.860 GB RSS) remains a lower-memory target. The newer mixed
references are `vdbbench-searchview-final-50k-c-20260904` (121.8 query QPS,
619 rows/s, 3.55 GB mixed RSS) and `vdbbench-searchview-final-1m-20260904`
(63.4 query QPS, 330 rows/s, 5.40 GB mixed RSS). Raw bundles and the r126
correction are recorded in the
[2026-09-03 to 09-05 history](../docs/design/dense-indexing/history/vectordbbench-2026-09-03-05.md).

## Experiment and benchmark flags

Process environment flags read at startup. Unless a row says otherwise, a flag
is enabled only by the exact value `1` (`dense_perf_experiments.zig`). Flags
marked *experiment* are qualification-only treatments with no promoted default.
Most are resolved into `ResourceManager` fields in `resource_manager.zig`.

| Flag | What it does | Default | Status |
| --- | --- | --- | --- |
| `ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS` | Retain the duplicate posting-local float16 projection plane in new posting builds | off (no-copy) | kept default |
| `ANTFLY_EXPERIMENT_EXACT_MAPPED` | Generation-leased exact views for float32 block requests; `0` disables | on | kept default |
| `ANTFLY_EXPERIMENT_VECTOR_READ_SINGLE_HELPER` | Allow one helper per positional payload batch; `0` disables | on | kept default |
| `ANTFLY_EXPERIMENT_QUERY_PACKING` | RaBitQ query bit-plane packing: `reduce`, `lanes` (prior path) or `mask` | `reduce` | kept default |
| `ANTFLY_EXPERIMENT_VECTOR_READ_INLINE` | Drain positional payload batches in the caller (takes precedence over the helper flag) | off | experiment |
| `ANTFLY_EXPERIMENT_VECTOR_READ_ADAPTIVE` | Probe-then-widen vector-read worker fan-out per batch | off | experiment |
| `ANTFLY_EXPERIMENT_VECTOR_READ_PROFILE` | Export exact-read batch, helper and timing counters | off | diagnostic |
| `ANTFLY_EXPERIMENT_UNIFIED_VECTOR_FETCH` | Resolve member bindings and missing metadata in one bounded rerank wave before one native payload batch | off | experiment |
| `ANTFLY_EXPERIMENT_QUERY_SNAPSHOT` | Capture catalog, identity, primary metadata, ANN and source generations at one sequence, then release the apply fence | off | experiment |
| `ANTFLY_EXPERIMENT_QUERY_SNAPSHOT_NATIVE_ONLY` | With the snapshot flag, skip the full primary metadata snapshot and use a short sequence probe | off | experiment |
| `ANTFLY_EXPERIMENT_CONFIGURED_BULK_BUILD` | Let empty-index bulk ingestion honor the persisted `bulk_build_algo` | off | experiment |
| `ANTFLY_EXPERIMENT_AGGREGATE_ADMISSION` | CPU-count aggregate slot pool shared by query drivers and optional read helpers | off | experiment |
| `ANTFLY_EXPERIMENT_PHASE_ADMISSION` | Release scan bandwidth after candidate scanning; acquire a FIFO rerank lane | off | experiment |
| `ANTFLY_EXPERIMENT_SCAN_PREDICTION` | Per-generation EWMA of selected work for the first tree reservation | off | experiment |
| `ANTFLY_EXPERIMENT_FUSED_NO_COPY` | Reuse immutable member IDs and stream ordered RaBitQ scores into the eight-candidate gate without a local float16 plane | off | experiment |
| `ANTFLY_EXPERIMENT_ANGULAR_BOUNDS` | Tighten cosine posting bounds with spherical-cap geometry and the persisted chord radius | off | experiment |
| `ANTFLY_EXPERIMENT_QUANTIZED_ROUTING` | Convert native exact centroid blocks to RaBitQ for routing | off | experiment (recall loss) |
| `ANTFLY_EXPERIMENT_CENTERED_ROUTING` | With quantized routing, quantize centroid residuals about the block mean | off | experiment |
| `ANTFLY_EXPERIMENT_SUBGROUPS_4` / `_8` / `_16` | Persist a balanced subgroup layout of that size in new posting builds (choose one) | off | experiment |
| `ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING` | Rank all frontier subgroup representatives together and scan a prefix covering ≥75% of rows | off | experiment |
| `ANTFLY_EXPERIMENT_COMPACT_SUBGROUP_ROUTING` | Lazily built, generation-owned int8 representative hints for global routing | off | experiment |
| `ANTFLY_EXPERIMENT_CERTIFIED_SUBGROUPS` | Conservative source-space balls on subgroup layouts; prune only on strict bound comparison | off | experiment |
| `ANTFLY_EXPERIMENT_PROJECTION_PAGES` | Group up to 256 projection requests by file/page via a node-wide 16 KiB clean-page cache | off | experiment |
| `ANTFLY_EXPERIMENT_PROJECTION_BORROW` | Borrow checksum-validated views from the clean-page cache under query leases | off | experiment |
| `ANTFLY_EXPERIMENT_GROUPED_FALLBACKS` | Queue oversized/cross-page projection reads with ordinary page groups | off | experiment |
| `ANTFLY_EXPERIMENT_PROJECTION_TRACE` | Record bounded projection-request traces for locality replay | off | diagnostic |
| `ANTFLY_EXPERIMENT_PROJECTION_CLUSTERING` | Reorder projection bytes inside streaming pages by a sparse-hyperplane key | off | experiment |
| `ANTFLY_EXPERIMENT_VECTOR_WRITE_CACHE` | Vector-block write cache intent: `all` (normal caching) or `wal` (WAL output only) | cold sequential | experiment |
| `ANTFLY_EXPERIMENT_INLINE_NATIVE_RESIDUAL_READS` | Skip helper scheduling for native residual completion | off | experiment |
| `ANTFLY_EXPERIMENT_DEFER_SOURCE_CAPTURE` | Acquire replay source capture after window collection, before apply | off | experiment |
| `ANTFLY_EXPERIMENT_COALESCE_REPLAY_DELETES` | One ordered, deduplicated union of replay deletes and replacements per capture | off | experiment |
| `ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS` | Load each surviving leaf's transformed vectors once during eager batch deletion | off | experiment |
| `ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS` | Keep surviving rows' codes and scoring origin on delete instead of re-encoding | off | experiment |
| `ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS` | Native posting row-delta mutation path (native-authoritative indexes only) | off | experiment |
| `ANTFLY_EXPERIMENT_REUSE_POSTING_ROWS` | Forward an authenticated unchanged no-copy base leaf row into a rebuilt segment | off | experiment |
| `ANTFLY_EXPERIMENT_COMPACT_POSTING_DELTAS` | Let chain-limit/dead-row maintenance fold the delta suffix against the retained base | off | experiment |
| `ANTFLY_EXPERIMENT_STAGE_POSTING_READERS` | Prepare immutable readers in the checkpoint worker before signaling completion | off | experiment |
| `ANTFLY_EXPERIMENT_STAGE_POSTING_REBASE` | Also prepare most of the live-generation rebase in a second `std.Io` task | off | experiment |
| `ANTFLY_EXPERIMENT_REPLAY_FINALIZE` | Coalesced dense catch-up policy plus finalize-stage timing logs | off | experiment |
| `ANTFLY_EXPERIMENT_CAPTURE_STAGES` | Log capture/patch/WAL/publication stage timings | off | diagnostic |
| `ANTFLY_HBC_POSTING_SIDECAR` | Enable the posting store for an LSM-authoritative (v1 mirror) index; implied while `ANTFLY_HBC_POSTING_WAL_STORE` is on | off (implied on) | legacy |
| `ANTFLY_VDBBENCH_KEEP_DEFAULT_FULL_TEXT` | VectorDBBench adapter: keep `full_text_index_v0` instead of deleting it | `0` in qualification scripts | benchmark |
| `ANTFLY_VDBBENCH_SYNC_LEVEL` | VectorDBBench adapter write sync level | `write` in A/B scripts | benchmark |
| `ANTFLY_VDBBENCH_DENSE_EMBEDDINGS` | Table source ownership requested by the patched adapter | set per arm | benchmark |

Source-ownership read-path defaults (`ANTFLY_SOURCE_VECTOR_*`,
`ANTFLY_HBC_VECTOR_BLOCK_ENCODING`) are listed in
[VECTOR_STORE.md](VECTOR_STORE.md#scoped-default-promotion).
`ANTFLY_HBC_POSTING_WAL_STORE` (default on) is an emergency pre-authority
rollout brake and never downgrades an index whose `AUTHORITY` marker exists.

## Open work

1. **Source-ownership default gaps.** Find why vector-store small-table queries
   sometimes miss the resolved-metadata fast path (0.225 versus 45.9 entries per
   query), keeping exact generation/identity validation. Isolate GC copy/mark
   work and scheduling delay before choosing a reclamation policy (149–155 s
   versus about 33 s at 1M).
2. **Coalesce stable-tip replay** into larger bounded HBC WAL transactions while
   preserving source ordering, capture identity and the backlog reservation.
   Never put this work on `sync_level=write`.
3. **Separate allocation-byte admission from bandwidth task admission.** Add an
   independently measured scratch permit and tune task concurrency from observed
   service time and queue pressure.
4. **Reject unsupported checkpoint work before staging it**, so retries do not
   consume load-path bandwidth.
5. **Narrow persisted L0 source ranges** with adaptive, workload-independent
   slices, paired with a lower soft compaction-input budget. Keep the oversized
   escape for an indivisible closure.
6. **Incrementally publishable HBC leaf normalization** inside one replay
   transaction, preserving ordering, rollback and amortization.
7. **Snapshot-copy attribution.** Remaining copies are mostly current scans; do not
   replace multi-operation snapshots with unsafe live probes to improve a
   counter. Attribute the 1M load's mutable snapshot copies by reader class.
8. **Crash-matrix automation.** Deterministic fault injection at every
   posting-WAL append, fsync, checkpoint staging, `CURRENT` replacement, overlay
   allocation and applied-watermark boundary is the release gate.
9. **Shared source artifacts.** Public catalog/API lifecycle for promoting
   index-managed immutable source artifacts to shared artifacts, with
   generation-fenced references and deletion-safe reference accounting. Never
   share index-specific topology/posting generations.
10. **Native posting disk and residency.** Reduce the posting chain's disk and
    mmap residency with indexed patch-native or chunked copy-on-write deltas,
    measuring recovery time as well as bytes.
11. **Approximate-scan cost.** At 1M the flat centroid/quantized scan dominates
    (about 240K approximate vectors/query). Improve SIMD/block layout, cache
    residency and bounded parallel scoring at 0.990 recall. Routing savings must
    survive total-cost accounting (the latest global-subgroup diagnostic spent
    0.228 ms to save 0.244 ms).
12. **Single-copy projection locality.** Prove a material physical-read
    reduction (prior within-page clustering saved 0.67% at 1M) before changing
    the format. Then keep one projection per authoritative vector revision with
    generation-bound posting references and bounded immutable relocation.
13. **Warm direct access versus cold I/O.** Evaluate generation-leased views over
    single-copy projection bytes under an explicit residency budget, with
    bounded positional reads for cold pages. Not unbounded mmap warmup.
14. **Candidate-page admission** under an explicit resource-manager budget,
    prefetching only the routed shell and evicting whole pages by generation.
15. **Mixed-workload publication cost.** Reusable immutable chunks and
    incremental shared-vector consolidation for catch-up, peak memory and
    rewrite amplification; compare equal offered write rates.
16. **RSS versus demand audit.** Classify mmap residency, allocator arenas,
    primary run/index pages, cache leases and stacks before changing budgets.
17. **Bound online HBC tree height** under adversarially ordered vectors. The
    Debug fixture `flat traversal does not treat a full candidate heap as a
    pruning proof` (128 collinear inner-product vectors in increasing order)
    entered deep recursive split recomputation using about 1.5 GiB. Keep that
    coverage; add a height/balanced-split invariant or rebuild an over-deep
    subtree with the bulk builder.
18. **Competitor comparison.** Compare against current Circus competitors with
    identical corpus, payload, recall and concurrency semantics; report cold and
    warm separately and keep maintenance out of the first concurrency sample.
