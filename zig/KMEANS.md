# K-Way K-Means Bulk Build Plan

**Status: partially implemented.** Phases 1-4 below (CPU K-way bulk build, clustered parent levels, Metal FlashAssign backend, sort/segment centroid updates) are implemented and covered by tests. Phase 5 (benchmarking across bulk-build algorithms and gating production rollout) is not complete: `BulkBuildAlgo.kmeans` is an opt-in mode selectable via `HBCConfig.bulk_build_algo`, but the default remains `.hilbert_seeded` — k-means has not been made the primary full-rebuild path. See "Current Status" and "Open work" below for what remains.

## Goal

Use K-way k-means as the primary full-rebuild path for HBC dense indexes, then accelerate the expensive Lloyd assignment/update loop with a Metal Flash-KMeans-style backend.

The target rebuild shape is:

1. Transform all vectors once.
2. Choose `K ~= ceil(vector_count / leaf_size)` leaf clusters.
3. Run K-way k-means over transformed vectors.
4. Pack bounded HBC leaves from the resulting clusters.
5. Build upper HBC levels from leaf centroids.

## Phases

### Phase 1: CPU K-Way Bulk Build

- Add a `BulkBuildAlgo.kmeans` mode.
- Implement a correctness-first CPU K-way Lloyd loop.
- Seed centroids with deterministic farthest-first initialization.
- Assign each vector to its closest centroid without materializing an `N x K` distance matrix.
- Pack each cluster into one or more leaves capped by `leaf_size`.
- Reuse the existing parent-level builder for the first milestone.
- Add tests proving the new bulk-build mode is searchable and respects active-count/node-count invariants.

### Phase 2: Clustered Parent Levels

- Replace sequential parent grouping with K-way grouping over child centroids.
- At each internal level, choose `K ~= ceil(child_count / branching_factor)`.
- Pack parent nodes from centroid clusters, capped by `branching_factor`.
- Keep the existing sequential parent builder as the fallback path.

### Phase 3: Metal FlashAssign Backend

- Add a macOS-only Metal backend behind an availability check and size threshold.
- Keep CPU seeding and tree construction.
- Offload repeated Lloyd assignment/update work:
  - fused distance plus online argmin per vector,
  - per-threadgroup partial sums and counts,
  - second-stage reduction into centroids.
- Support `l2_squared`, cosine, and inner product assignment; centroid updates remain CPU for now.

### Phase 4: Sort/Segment Update

- For large `K`, replace atomic-style centroid updates with sorted or segmented reductions.
- Use the paper's sort-inverse idea when the update stage becomes the bottleneck.
- Keep the simpler reduction backend for smaller `K` or small rebuilds.
- Keep Lloyd assignment/update in `zig/lib/vectorindex/src/kmeans.zig` so the HBC builder only owns tree packing and persistence.

### Phase 5: Benchmarks and Rollout

- Add `storage_bench hbc-write` coverage for `.kmeans`.
- Compare `.hilbert_seeded`, `.doc_key_seeded`, `.recursive`, and `.kmeans`.
- Track:
  - bulk tree build time,
  - leaf size distribution,
  - search recall/latency,
  - rebuild memory use.
- Gate production rollout behind config/env once recall and rebuild time are better than current defaults.

## Current Status

- Phase 1 is implemented as a CPU path.
- Phase 2 is implemented for the `.kmeans` bulk-build path: parent levels are clustered by child centroids and packed by `branching_factor`.
- K-means assignment/update code is factored into `zig/lib/vectorindex/src/kmeans.zig`.
- Phase 3 has an initial macOS Metal FlashAssign backend for assignment in `zig/lib/vectorindex/src/kmeans_metal.{zig,m}`. It supports `l2_squared`, cosine, and inner product distances. CPU still handles seeding, centroid updates, sorting, and HBC tree construction. `auto` uses Metal only for large jobs when a Metal device is available; `metal` forces the backend. A K-means run now creates one Metal context and reuses the uploaded point buffer plus assignment/distance buffers across Lloyd iterations.
- Phase 4 has a CPU segmented-update path and an initial unit-weight Metal centroid-update path behind `HBCConfig.kmeans_update_strategy`: `auto`, `scatter`, `segmented`, or `metal`. `auto` stays conservative and uses the CPU update strategies; `metal` requires a Metal assignment context unless `kmeans_backend` is explicitly `cpu`, then uses a two-stage Metal partial-sum/finalize update path for leaf-level unit-weight K-means while weighted parent levels fall back to CPU update strategies. Metal update only runs in iterations where assignment also ran on Metal, so auto-mode assignment fallback cannot feed stale GPU assignment buffers into the update step.
- HBC bench CLIs accept `--kmeans-backend auto|cpu|metal` and `--kmeans-update-strategy auto|scatter|segmented|metal` for comparing backends and update strategies. `storage_bench hbc-write` also reports K-means assignment/update call counts, point totals, and CPU/Metal nanoseconds.
- `zig/lib/vectorindex/src/kmeans.zig` has focused tests for CPU scatter stats, explicit CPU fallback with `update_strategy = .metal`, required Metal context failures under test builds, and forced-Metal dense-vector validation.
- `HBCConfig.bulk_build_algo` still defaults to `.hilbert_seeded`; `.kmeans` must be selected explicitly.

## Open work

- Phase 5 (benchmarks and rollout) is not finished: recall/latency/rebuild-memory comparisons across `.hilbert_seeded`, `.doc_key_seeded`, `.recursive`, and `.kmeans` have not been published, and there is no recorded decision to gate or flip the default `bulk_build_algo` to `.kmeans`.
- Weighted parent-level Metal centroid updates are not implemented; those levels always fall back to CPU update strategies (see Phase 4/Current Status above).

## Experimental e-means leaf builder

`BulkBuildAlgo.emeans` is a library/benchmark-only experiment for squared L2.
It does not change the default builder or add a public API configuration value.
Leaf clustering uses five shuffled CPU passes with the batch-size and step-size
laws from [Bessa et al., e-means (2026)](https://arxiv.org/abs/2611.09501), EMA
counts/means, cosine step decay, mass-weighted donor splitting every five
batches (disabled at dimension <= 2), and final-epoch closing means. Unlike the
reference's returned training labels, HBC obtains exact nearest-final-centroid
labels with one additional assignment pass before packing. Leaves remain
bounded by `leaf_size`; weighted parent levels still use the existing Lloyd
builder and its configured backend. Forced Metal leaf execution and non-L2
metrics return explicit errors. `kmeans_max_iter` still controls parents;
e-means leaf training uses a fixed five epochs.

The additional e-means scratch is one `usize` permutation per vector, one FP32
mass and one `usize` closing count per centroid, and one FP32 closing sum per
centroid coordinate: `N*sizeof(usize) + K*(sizeof(f32)+sizeof(usize)+D*sizeof(f32))`.
This is in addition to the existing Lloyd/bulk-builder buffers. No `N x K`
distance matrix is allocated. This CPU prototype does not use the paper's
CUDA kernels, so its performance must be measured independently.

Build and compare from `zig/`:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build hbc-read-bench -Doptimize=ReleaseFast
python3 tools/bench_emeans.py --vectors 10000 --dims 128 --samples 3
```

The script compares Hilbert, Lloyd and e-means on identical seeded synthetic
vectors using squared L2, CPU assignment, and memory storage. It saves raw
benchmark output and median build time, warm query time, recall@10, and leaf
occupancy. On macOS it also records peak process RSS via `/usr/bin/time -l`;
that includes the fixture, index, storage, and queries, and is not isolated
clustering workspace. The read benchmark reopens the index before its warm
query measurements. Use different `--seed`, `--vectors`, and `--dims` values
for additional comparisons, and vary `--search-width` (default 16 in the
script) to measure recall/latency tradeoffs. L2 fixture vectors and queries
retain their original magnitudes; cosine fixtures are normalized. These synthetic results alone are insufficient to
choose a production default; embedding fixtures and matched-recall latency
curves are still required.

Individual benchmark commands accept `--bulk-build-emeans --metric l2_squared`.
Both `hbc-read` and `hbc-write` default to cosine unless `--metric` is supplied.

### Initial local measurements (2026-11-09)

See [`emeans_baseline.json`](bench/vectors/emeans_baseline.json) for the complete
summary. ReleaseFast, CPU, memory storage, 10,000 vectors x 128 dimensions,
16 synthetic clusters, seed 42, median of three samples, 100 queries, leaf size
128, branching factor 128, search width 16:

| Builder | Build ms | Warm query µs | Recall@10 | Leaves | Leaf members min/max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hilbert | 225.8 | 192.3 | 0.243 | 79 | 126 / 127 |
| Lloyd | 328.2 | 392.0 | 0.929 | 114 | 30 / 128 |
| e-means | 132.6 | 388.4 | 0.920 | 115 | 16 / 128 |

E-means built about 2.5x faster than Lloyd on this fixture, with recall lower
by 0.9 percentage points. At search width 32, recall remained 0.920 versus
0.929, and query times were 452.4 versus 428.2 µs. This does not demonstrate a
query-speed improvement or equal quality. Peak whole-process RSS across both
comparisons ranged from about 80 to 101 MiB; these measurements do not isolate
the small additional clustering buffers. Every build retained all 10,000
members and bounded leaves by 128, and reopened-index queries completed.

Other builds/services were active during measurement. Samples reuse one fixed
seeded dataset; real embedding datasets, additional seeds, and matched-recall
comparisons remain necessary before considering any default change.

### Incremental insertion: 100,000 vectors

[`emeans_incremental_100k.json`](bench/vectors/emeans_incremental_100k.json)
records three runs per setting of 100,000 x 128-dimensional L2 vectors, inserted
from empty through 100 coalesced batches of 1,000. The timer covers the insert
loop, including index maintenance; fixture generation, index open, embeddings,
and queries are excluded. Storage is in memory, with CPU clustering.

| Bulk algorithm setting | Median incremental insert seconds | Vectors/second |
| --- | ---: | ---: |
| Hilbert | 7.479 | 13,372 |
| Lloyd | 7.609 | 13,142 |
| e-means | 8.140 | 12,285 |

The bulk algorithm selector is not consulted on this path: every setting uses
the same incremental routing and Lloyd-based splits. Every run recorded
100,000 insert calls, 1,133 leaf splits, 26,278 node saves, and 1,134 final
leaves holding all 100,000 members (maximum 128 per leaf). Reopened-index query
result digests were identical. Timing differences across these identical code
paths reflect run-to-run conditions rather than an e-means benefit. This
bulk-only comparison therefore demonstrates no incremental insertion speedup.
The independently enabled leaf-split experiment below evaluates e-means on the
actual incremental partitioning path.

Reproduce with the comparison script's incremental mode:

```sh
python3 tools/bench_emeans.py --build online_coalesced --vectors 100000 --dims 128 --batch-size 1000 --samples 3 --queries 1 --output .zig-cache/emeans-incremental-100k
```

One query is used only to exercise reopen/search, not to estimate recall.

### Experimental e-means for incremental binary leaf splits

`HBCConfig.experimental_emeans_leaf_splits` (default false) selects the existing
five-epoch CPU e-means optimizer for ordinary two-way leaf splits. This is
independent of `bulk_build_algo`. Internal-node splits and oversized-leaf bulk
rebuild paths retain their existing algorithms. This library/benchmark option
requires squared L2, the k-means split policy, and a CPU-compatible backend; it
is not exposed through the public index API.

The splitter enforces the existing minimum occupancy percentage (at least one
member per side), deterministically reassigning points by centroid-distance
difference when needed. It then recomputes both centroids from the final member
groups before persistence and quantization. Incremental scratch is accounted
through the existing apply-workspace hooks. The benchmark prints actual e-means
split-call and rebalance counts plus leaf partition time; it also checks active
count and tree links outside the timed insertion loop.

Reproduce the Lloyd/e-means split comparison from `zig/`:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build hbc-read-bench -Doptimize=ReleaseFast
python3 tools/bench_emeans.py --build online_coalesced --compare-leaf-splits --vectors 100000 --dims 128 --batch-size 1000 --samples 3 --queries 100 --output .zig-cache/emeans-leaf-splits-100k
```

Both settings retain the same Hilbert bulk configuration; incremental inserts
start empty and do not call the bulk builder. Search measurements use 100
queries after reopen, with search width 16 and the existing boundary rerank.

Measured results are in
[`emeans_leaf_splits_100k.json`](bench/vectors/emeans_leaf_splits_100k.json):

| Leaf split optimizer | Median insert seconds | Leaf partition ms | Recall@10 | Warm query µs | Final leaves |
| --- | ---: | ---: | ---: | ---: | ---: |
| Lloyd | 7.714 | 48.827 | 0.419 | 1,033.45 | 1,134 |
| e-means | 7.733 | 122.447 | 0.456 | 873.29 | 1,170 |

There is no meaningful insertion-speed improvement: total times differ by
about 0.25%, while e-means partitioning itself is about 2.5x slower for these
small two-cluster problems. Lloyd partitioning accounts for only about 0.6%
of total insertion time, limiting the upside from optimizing it alone.
E-means ran on all 1,169 binary leaf splits and required 670 occupancy
corrections; the 15 internal splits retained the original policy. All tree
checks passed, all 100,000 members remained present, and no leaf exceeded 128.

Fixed-budget recall and warm query time improved on this fixture, but absolute
recall is low, these are not matched-recall measurements, and they reuse one
synthetic seed. This result does not support enabling e-means splits by default.

### Full-corpus Cohere Parquet benchmark

The benchmark accepts the VectorDBBench Cohere `train.parquet`, `test.parquet`,
and `neighbors.parquet` files. The preparation script decodes and L2-normalizes
the embeddings in batches. Both CPU builders use squared L2 on those unit
vectors and the same held-out cosine neighbor IDs.

From `zig/`, with NumPy and PyArrow installed:

```sh
python3 tools/prepare_cohere_bulk_bench.py "$cohere_parquet_dir" .benchmark-results/cohere-prepared
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build bench-build-quality -Doptimize=ReleaseFast
for algo in emeans kmeans; do
  zig-out/bin/bench_build_quality \
    .benchmark-results/cohere-prepared/train.f32 \
    .benchmark-results/cohere-prepared/queries.f32 \
    .benchmark-results/cohere-prepared/truth.u64 \
    768 "$algo" full ".benchmark-results/cohere-prepared/index-$algo" 4096
done
```

Use fresh output directories for the preparation and each index build. The
`full` mode passes all prepared vectors to one clustering run, stages storage
commits with a 64 MiB target, and publishes a native posting generation on
local storage. The prepared float32 file supplies exact vectors. The benchmark
reopens and activates the published generation, verifies tree links and row
count, and measures recall@100 across a probe sweep.

Timings cover Parquet decode, normalization and local vector-file writing
(`prepare.json`), then fixture loading, HBC construction, leaf clustering
within construction, and native posting publication (`full_build_phase`).
The million-vector run uses a shared 4,096-vector leaf capacity to limit the
CPU cost of flat, all-centroid assignment. The 3.072 GB vector matrix is
held in memory during training. This is a local analogue of lake posting construction
and excludes remote reads, object-store upload, and lake manifest publication.

One measured build per optimizer on Cohere 1M, 768 dimensions, CPU, normalized
L2, 4,096-vector leaves and 100 held-out queries:

| Stage | Lloyd | e-means |
| --- | ---: | ---: |
| Shared Parquet decode, normalization, and float32 write | 17.42 s | 17.42 s |
| Leaf clustering | 711.77 s | 468.49 s |
| Other HBC construction | 99.40 s | 105.45 s |
| Native posting publication | 1.63 s | 1.88 s |
| Parquet preparation through reopened sidecar, summed phases | 831.47 s | 594.68 s |
| Recall@100 at 64 probes | 95.59% | 95.77% |
| Mean query time at 64 probes | 7.45 ms | 7.49 ms |

E-means reduced measured construction and posting publication from 13m33s to
9m36s. Both published generations reopened and verified all 1,000,000 rows.
The full sweep and phase data are in
[`emeans_cohere_1m.json`](bench/vectors/emeans_cohere_1m.json). The workstation
was shared and memory pressure varied during these single runs. A build-cache
eviction removed the completed e-means output and Lloyd's scratch directories.
Lloyd continued with its original in-memory input after the empty scratch
directories were recreated. The timing and validation logs survived.

### Sampled e-means leaf training

The optional `bulk_build_training_sample_size` setting selects a uniform
reservoir across the prepared input rows. E-means fits the same number of
centroids as a full-corpus build, using only the sample during training.
A single exact assignment pass routes every vector to the trained centroids.
The existing packer splits large groups into leaves within the configured
capacity and recomputes their leaf centroids.

To reproduce the sampled arm on the prepared Cohere 1M fixture, append the
sample size after the leaf capacity:

```sh
zig-out/bin/bench_build_quality \
  .benchmark-results/cohere-prepared/train.f32 \
  .benchmark-results/cohere-prepared/queries.f32 \
  .benchmark-results/cohere-prepared/truth.u64 \
  768 emeans full .benchmark-results/cohere-prepared/index-sampled 4096 50000
```

The comparison uses 245 trained centroids for both arms and a leaf cap of
4,096. The reservoir contains 50,000 rows, and the input remains the same
normalized 3.072 GB vector matrix. The probe reports training time, full
corpus routing time, leaf occupancy, and held-out recall and query timing.

Measured on the same prepared Cohere input in two consecutive runs:

| Metric | Full training | 50K sample |
| --- | ---: | ---: |
| Leaf training and final assignment | 189.28 s | 34.23 s |
| HBC build through native sidecar reopen | 273.51 s | 87.59 s |
| Local Parquet-to-reopen phase sum | 290.25 s | 104.39 s |
| Final leaves | 342 | 367 |
| Leaf members, min / mean / max | 1,220 / 2,924 / 4,091 | 800 / 2,725 / 4,095 |
| Whole-process peak RSS | 3.22 GiB | 3.70 GiB |
| Recall@100 at 64 probes | 95.77% | 95.16% |
| First tested probe count at or above 95.5% recall | 64 | 72 |
| Recall@100 and mean query time at that point | 95.77%, 9.59 ms | 95.53%, 6.27 ms |

The sampled arm spent 8.11 seconds selecting and training on the sample and
26.12 seconds routing the corpus and sorting its assignments. Both runs
verified the reopened tree and all one million members. The local phase sum
includes the same 16.27-second Parquet preparation in each column. The sample
reduced the measured local build time by about 2.8x. Both arms held the full
vector matrix, and sampled training did not reduce observed peak RSS. The
probe sweep compares a recall threshold on one fixture. Its query timings are
single-run observations from a shared workstation.

See [`emeans_sampled_cohere_1m.json`](bench/vectors/emeans_sampled_cohere_1m.json)
for the complete phases and recall/latency sweep.

### Bounded offline native lake builds

The offline builder trains small hierarchical routers, partitions the complete
corpus through checksummed disk spools, and finishes admitted terminal regions
in memory. Lloyd and e-means share the same sampling, routing, native leaf
encoding, and publication path. Fresh lake configurations select this path
with `lake_bulk_build`. Its managed allocation allowance is 256 MiB.

Cohere 1M measurements use normalized squared-L2 vectors, 768 dimensions,
production leaf capacity 100, two seeds, and 100 held-out queries.

| Builder | Build and publication s | First tested probes reaching 95.5% recall |
| --- | ---: | ---: |
| Full Hilbert | 196.9–200.7 | 8,192 |
| Sampled hierarchical Lloyd | 40.0–41.7 | 2,048 |
| Sampled hierarchical e-means | 31.7–31.8 | 2,048 |

The spooled builds admitted less than 178 MiB of allocations. Whole-process
RSS also includes native mappings and file residency. These serial observations
come from a shared workstation and cover local normalized inputs through native
posting publication. Parquet preparation, exact-vector LSM construction, remote
reads, and lake upload are separate stages. All six completed generations were
reopened and structurally checked after the final main merge, with identical
recall across the repeated sweeps.

Run the e-means arm with the prepared fixture.

```sh
zig build bench-build-quality -Doptimize=ReleaseFast
zig-out/bin/bench_build_quality \
  .benchmark-results/cohere-1m/prepared/train.f32 \
  .benchmark-results/cohere-1m/prepared/queries.f32 \
  .benchmark-results/cohere-1m/prepared/truth.u64 \
  768 spool_emeans full .benchmark-results/cohere-1m/spooled-index 100 0 42
```

Use `spool_lloyd` for the paired sampled Lloyd run. Use `reopen` in place of
`full` to repeat the extended recall sweep over an existing generation.

The implementation, resource limits, configuration example, and regression
commands are documented in the
[offline builder design](../docs/design/vector-store/bulk-builder.md).
The complete measurements and provenance are in
[the streamed Cohere result](bench/vectors/emeans_streamed_cohere_1m.json).
