# HBC Dense Indexing

This file is the canonical HBC note for Antfly dense indexing. It covers DB
integration, search/rerank behavior, vector ownership, storage-engine bulk
build, dense split child rebuild, and tree construction strategy.

## Overview

HBC owns dense vector indexing and approximate nearest-neighbor search. It does
not own public document storage, derived artifact identity, or the broader DB
query contract. Those are DB-layer concerns described in [DB.md](DB.md).

The important split is:

- writes need one deterministic destination leaf
- search may use approximate payloads, error bounds, and rerank machinery
- batch ingest and split rebuild should avoid online insert work where possible

## Write Routing

HBC write routing chooses one destination leaf for each inserted or updated
vector. That is a different problem from approximate search.

For writes, the production rule is:

- route by exact child-centroid distance at each internal node
- do not use quantized error-bound competitive sets on the write path
- keep grouped batch routing and coalesced leaf mutation
- defer expensive split/quantized maintenance to bounded publish windows

Quantized error bounds are search machinery. They are useful when a query needs
a candidate set whose approximate scores may overlap, especially at leaf/member
scoring and final rerank boundaries. They are not useful when a write only needs
one deterministic destination child. Applying search-style competitive selection
to inserts adds extra quantized estimation, candidate bookkeeping, and exact
rescoring without improving write semantics.

## Search And Rerank

Search may still use quantized payloads and error bounds:

- internal-node traversal can use approximate payloads where it is a search
  quality/performance win
- leaf/member scoring can use approximate distances plus error bounds
- boundary rerank uses those bounds to decide which candidates require exact
  vector scoring

Final rerank must use borrowed/external vector loading where possible. Search
scratch belongs in `dense.search_working_set`; retained HBC nodes and quantized
payloads remain in `hbc.node_metadata_cache`.

## Multi-bit RaBitQ codes

`quantization_bits` (dense index config, 1 to 8, default 1) adds extended
RaBitQ codes ([Gao et al., SIGMOD 2025](https://arxiv.org/abs/2409.09913),
`antfly_vector.rabitq_ex`) next to the 1-bit codes in every quantized set.
They describe the same residual against the same centroid with B bits per
dimension, so their distance estimates and error bounds are much tighter.

- **Why:** leaf scoring admits the best `k * rerank_factor` 1-bit estimates
  into the candidate heap before any f16 or exact scoring. With 1-bit codes,
  true neighbors often miss that cut (see the measurements below), and the
  loose bounds make boundary rerank score most of the heap exactly.
- **`quantization_refine: near_cutoff` (default):** score with the 1-bit
  kernel, then rescore with extended codes only candidates whose 1-bit
  estimate minus `quantization_refine_margin` (default 2) bounds could still
  enter the heap. `all` rescores every scanned vector.
- **Storage:** `ex_bits`/`ex_codes`/`ex_factors` fields on
  `RaBitQuantizedVectorSet` (protobuf fields 9 to 11; older readers skip
  them), an ex-codes flag on AFQD leaf entries and AFRC row-delta chunks
  (older builds reject such entries instead of misreading them), and
  `quantization_bits` in `IndexMetadata` (encoded only above 1). The stored
  value wins on reopen.
- **Rotation:** extended codes assume randomly rotated vectors, so bits above
  1 turn on `use_random_ortho_trans`; opening such an index without rotation
  fails.
- **Cost:** about `dims * bits / 8 + 8` extra bytes per vector, longer
  builds (about 2x at 4 bits), and the extended scorer's CPU for refined
  candidates (byte-parallel SIMD for 2, 4 and 8 bits; 3, 5, 6, 7 use a
  scalar path). Per-leaf query preparation uses a stack buffer.
- **Not covered:** segment sections (`RBIQ`) and serverless vector segments
  keep 1-bit codes; their quantizers have no config path yet.

Measured on the real index with the opt-in `hbc_adapter.zig` test "hbc
multi-bit quantization measurement on a dataset" (`ANTFLY_HBC_BENCH_DATA`).
Setup: NYTimes-256 cosine, 100k vectors, 200 queries, leaf 168, branching
168, epsilon 7, boundary rerank, rotation on. ReleaseFast on an Apple M4 Max,
single-threaded queries, warm caches. Search width 1008 (the index manager
default) scans all 100k vectors here:

| Codes | Heap (k * rerank_factor) | Recall@10 | Rescored/q | Exact reranks/q | ms/query |
|---|---:|---:|---:|---:|---:|
| 1-bit (default) | 20 | 0.559 | 0 | 19 | 1.52 |
| 1-bit | 80 | 0.719 | 0 | 76 | 2.29 |
| 1-bit | 320 | 0.834 | 0 | 274 | 4.13 |
| 2-bit, margin 2 | 80 | 0.937 | 37,533 | 64 | 3.58 |
| 3-bit, margin 2 | 80 | 0.996 | 24,544 | 46 | 7.20 |
| 4-bit, margin 1 | 80 | 0.975 | 3,883 | 21 | 1.57 |
| 4-bit, margin 2 | 20 | 0.987 | 10,204 | 13 | 1.99 |
| 4-bit, margin 2 | 80 | 0.999 | 19,137 | 22 | 2.21 |
| 4-bit, all | 80 | 0.999 | 100,000 | 23 | 4.05 |
| 8-bit, margin 2 | 80 | 0.996 | 8,393 | 1.5 | 2.06 |

- 4 bits with the default margin reaches 0.999 recall in the same time 1-bit
  needs for 0.719. Quadrupling the 1-bit heap only reaches 0.834 at almost
  twice the time.
- With 1-bit codes the loss is at admission: `rerank_policy: always` gives
  the same recall as boundary rerank (measured at 20k: 0.620 both ways).
- 3 bits uses the scalar scorer and is slower than 4. 8 bits makes rerank
  nearly free but builds take 105 s against 9.5 s for 4 bits and 5.2 s for
  1 bit, because code search sweeps 127 levels per dimension.
- At search width 128 (21k vectors scanned) every multi-bit setting reaches
  the 0.379 that routing allows, against 0.335 for 1-bit at heap 80.

`bench/vectors/rabitq_hbc_sim.zig` reproduces the admission effect outside
HBC.

## Dense Artifact Vectors

Public writes and replay store dense vectors as primary-store artifacts. HBC
apply should borrow those artifact vector bytes when they are already aligned
packed `f32` payloads.

The hot path must not decode every artifact into a separate heap allocation
before calling HBC. The correct ownership shape is:

- read artifact values in sorted batches
- borrow `denseEmbeddingVectorView()` when possible
- use one bounded batch scratch slab only for unaligned fallback decodes
- keep the scratch alive for the HBC apply call
- account scratch and borrowed batch payload pressure under
  `dense.apply_working_set`

This keeps the HBC cache and resource-manager accounting honest: retained HBC
state is tracked separately from transient dense apply work.

## Bulk Build

Dense/vector batch ingest and split child rebuild use a true bulk builder
instead of many online inserts in one transaction. Online tree maintenance —
route to leaf, mutate leaf, split leaves/internal nodes incrementally, persist
updated nodes repeatedly — remains the right model for genuinely incremental
writes, but it is not the right model for workloads that are already
batch-shaped.

Principles:

1. Build final nodes once whenever possible.
2. Quantize once per finished node, not once per inserted vector.
3. Persist vectors and metadata once before tree construction.
4. Keep the online insert path for truly incremental writes.
5. Use the bulk builder where Antfly is already batch-oriented: dense split
   child rebuild and large batch ingest.

### Empty-Index Bulk Builder

`bulkBuildWithMetadata(...)` builds a brand-new HBC tree from a batch in one
write transaction. It persists all raw vectors and metadata once, precomputes
transformed vectors once, recursively partitions the batch into final leaves,
builds parent nodes upward from finished children, and quantizes each final
node exactly once as it is written. There is no per-item online insert loop in
the bulk-build path; search works correctly on the built index, and
`active_count` and metadata are correct after reopen.

### Split Child Rebuild

Dense split child rebuild feeds `BatchInsertItem`s into the bulk builder
instead of looping `batchInsertWithMetadata...`. This is what moved the split
handoff bottleneck: dense split child handoff is now dominated by HBC insertion
itself, not by split bookkeeping or quantized maintenance overhead, since those
costs have already been cut by routing through the bulk builder.

### Bulk Partitioning Strategies

Both a recursive bulk build and a Hilbert-seeded bulk build exist as
partitioning strategies, along with an experimental doc-key-seeded path. An
informal bench comparison found recursive bulk build fastest overall, with
Hilbert-seeded producing a marginally smaller and cheaper-to-search tree
without beating recursive on build time, and doc-key-seeded winning only on
synthetic split locality while regressing the actual dense-split-handoff and
empty-index-ingest probes.

Recursive bulk build is the product default. Doc-key-seeded stays experimental
until it wins on the product-shaped probes, not just the synthetic HBC bench.
The best strategy is chosen from measured build time and search quality, not
assumptions.

## Native posting and WAL generations

Native storage is the production format for new and migrating HBC indexes
(`zig/pkg/antfly-embedded/src/storage/posting_segment_store.zig`,
`zig/lib/vectorindex/src/posting_wal.zig`). One checksummed generation plus a
committed WAL tail holds every query-facing HBC namespace: packed tree nodes
and topology, quantized and non-quantized leaf payloads, postings and node
ranges, vector-to-leaf mappings, result metadata, and index metadata including
`covered_source_sequence`. Source documents and exact embeddings are not in
this store; they belong to the table's source owner (primary LSM or
[vector store](VECTOR_STORE.md)) and the shared exact-vector blocks.

### Files and publication order

- `segment-<generation>.afps` files are immutable base or delta segments.
  `wal-<generation>.afpw` files are the appendable mutation WAL (frame magic
  `AFPW`, version 5; replacement patches use `AFPD` version 2).
- `CURRENT` is a fixed-size checkpoint record (`AFPC`, version 4) that names the
  base segment and its checksums, up to eight ordered delta segments, up to
  eight sealed WAL extents, the active WAL generation and committed byte
  boundary, and `covered_source_sequence`.
- Publication order is segment, then an empty next-generation WAL, then
  `CURRENT`. A crash before `CURRENT` leaves only unreferenced files; a crash
  after it leaves a complete recoverable generation. Obsolete files are removed
  only after `CURRENT` is durably replaced, and cleanup runs after publication
  exclusion is released. An ambiguous `CURRENT` publication fences stale
  in-process authority and keeps every possibly published file.
- Prepared publication and native compaction stage the replacement segment and
  readers outside the vector mutation mutex, carry the exact concurrently
  committed WAL suffix into the next generation, recheck the live generation and
  source proof, and leave old query leases valid.

### Authority

- The sticky `AUTHORITY` marker (`antfly-hbc-native-v1`) is written only after
  the first complete native checkpoint is durable. After that, recovery fails
  closed: Antfly never reopens the legacy HBC LSM or treats its rows as
  authoritative. The LSM backend is detached; a native-first reopen removes only
  the obsolete `runs`, `wal`, manifest and lock files and keeps posting segments,
  generation identity and active query leases.
- Capture-free builders bootstrap one complete checkpoint from their private
  projection first; only then may they publish `AUTHORITY`, detach the LSM, and
  bind an exact-vector generation at the identical `covered_source_sequence`.
- `ANTFLY_HBC_POSTING_WAL_STORE` (default on) is only a pre-authority rollout
  brake. It never downgrades an index whose marker exists. The v1-mirror versus
  native-v2 authority rules are in
  [DENSE_INDEXING_LIFECYCLE.md](DENSE_INDEXING_LIFECYCLE.md#physical-index-versioning-and-rolling-upgrades).

### Coverage and readiness

- `covered_source_sequence` is monotonic. A WAL record below the checkpoint's
  coverage is rejected (`PostingWalOverlapsCheckpoint`); a commit cannot cover a
  record newer than its declared sequence (`InvalidPostingWalCommit`). A window
  that becomes mutation-empty after filtering still advances coverage with an
  empty (coverage-only) WAL transaction.
- Only postings touched by a committed source transaction are captured, and the
  applied watermark is published only after derived capture completes. A
  derived failure never fails a committed source write; it invalidates the
  native generation and ordinary replay repairs it.
- Replay retains the capture lease and window sequence, then, under one
  per-index apply fence, retires the streaming session, validates lease
  ownership and publishes the capture at that sequence. Borrowers may append
  within an owner's transaction but cannot publish or cancel it; stale epochs and
  index incarnations fail closed.
- Readiness requires a certified complete native posting generation before the
  first ready marker, and an exact-vector generation whose source sequence,
  encoding, shard count and vector cardinality match under one immutable lease.
  A vector mutation marks the projection dirty so status reports finalization,
  not ready, until the replacement is published. Exact-vector blocks are
  rebuildable table-wide acceleration: a missing or corrupt shared generation is
  discarded without losing native posting authority or primary-vector fallback.
- An empty root canonically has no quantized payload; absence is corrupt only for
  a non-empty posting. Stable validation retries one fresh-lease no-progress
  observation, requires a clean pass, and fails after bounded repeated mutations.

### Checkpoint policy

Queries lease the whole base-plus-delta chain plus the in-memory WAL overlay.
Thresholds (`hbc_adapter.zig`): overlay collapse at 32 MiB or 16 live overlay
versions; idle checkpoint at 64 MiB of WAL; incremental delta checkpoint at
128 MiB; a max-chain full merge at 192 MiB; mandatory progress priority at the
256 MiB hard recovery budget. A full WAL is capped at 512 MiB and a segment at
4 GiB. At an idle or readiness boundary a max-depth chain is flattened. Dead
serving rows are compacted only when they are at least 64 MiB and at least one
third of the chain. Checkpoint writers borrow values from their pinned
immutable generation, and temporary allocations use the shared compaction
memory budget. Base, reshard and sparse-checkpoint outputs flush bounded 1 MiB
payload pages.

Backups (`hbc-native-v1` authority in the native backup manifest) decode each
authoritative `CURRENT` under the snapshot fence, hardlink immutable segments
and vector blocks into a private pin generation, copy appendable WALs only
through their committed byte boundary, and record the exact covered source
sequence.

## Open work

- Whether Hilbert-seeded or doc-key-seeded bulk build should replace recursive
  bulk build as the default depends on further product-shaped benchmarking,
  not just the synthetic HBC bench comparison recorded above.
- Resource-derived query capacity should feed public admission. Admission is a
  fixed request count (`max_concurrent_requests`, default 32), so an index-sized
  dense-search workspace whose estimator or configuration is wrong can still
  surface an internal `ResourceBudgetExceeded` 500 instead of being rejected at
  admission ([history](../docs/design/dense-indexing/history/vectordbbench-native-hbc-2026-08.md)).
- Runtime status should distinguish `temporarily_unavailable` from a literal
  empty index; as of the 2026-09-03–05 log, a busy managed writer briefly
  produced a zero-count status placeholder
  ([history](../docs/design/dense-indexing/history/vectordbbench-2026-09-03-05.md)).
- As of the 2026-09-08 log, the default-off stable scoring origins experiment
  (`ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS`) showed a mixed query p99
  regression (70.941→90.486 ms and 67.605→74.486 ms at 50K) that still needs
  attribution
  ([history](../docs/design/dense-indexing/history/vectordbbench-2026-09-07-14.md)).
- As of the 2026-09-07–14 log, the Debug test `db dense auto bulk finish wakes
  weak-sync replay and publishes visibility after catch-up` failed its
  `publish_blocking_checkpoint_clean` assertion with the capture flag both on
  and off; the cause was not established
  ([history](../docs/design/dense-indexing/history/vectordbbench-2026-09-07-14.md)).
