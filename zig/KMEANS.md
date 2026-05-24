# K-Way K-Means Bulk Build Plan

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
- Keep Lloyd assignment/update in `go/pkg/antfly/lib/vectorindex/go/pkg/antfly/src/kmeans.zig` so the HBC builder only owns tree packing and persistence.

### Phase 5: Benchmarks and Rollout

- Add `hbc-write-bench` coverage for `.kmeans`.
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
- K-means assignment/update code is factored into `go/pkg/antfly/lib/vectorindex/go/pkg/antfly/src/kmeans.zig`.
- Phase 3 has an initial macOS Metal FlashAssign backend for assignment in `go/pkg/antfly/lib/vectorindex/go/pkg/antfly/src/kmeans_metal.{zig,m}`. It supports `l2_squared`, cosine, and inner product distances. CPU still handles seeding, centroid updates, sorting, and HBC tree construction. `auto` uses Metal only for large jobs when a Metal device is available; `metal` forces the backend. A K-means run now creates one Metal context and reuses the uploaded point buffer plus assignment/distance buffers across Lloyd iterations.
- Phase 4 has a CPU segmented-update path and an initial unit-weight Metal centroid-update path behind `HBCConfig.kmeans_update_strategy`: `auto`, `scatter`, `segmented`, or `metal`. `auto` stays conservative and uses the CPU update strategies; `metal` requires a Metal assignment context unless `kmeans_backend` is explicitly `cpu`, then uses a two-stage Metal partial-sum/finalize update path for leaf-level unit-weight K-means while weighted parent levels fall back to CPU update strategies. Metal update only runs in iterations where assignment also ran on Metal, so auto-mode assignment fallback cannot feed stale GPU assignment buffers into the update step.
- HBC bench CLIs accept `--kmeans-backend auto|cpu|metal` and `--kmeans-update-strategy auto|scatter|segmented|metal` for comparing backends and update strategies. `hbc-write-bench` also reports K-means assignment/update call counts, point totals, and CPU/Metal nanoseconds.
- `go/pkg/antfly/lib/vectorindex/go/pkg/antfly/src/kmeans.zig` has focused tests for CPU scatter stats, explicit CPU fallback with `update_strategy = .metal`, required Metal context failures under test builds, and forced-Metal dense-vector validation.
