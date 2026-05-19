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

## Bulk Build Roadmap

Make dense/vector batch ingest and split child rebuild use a true bulk builder
instead of "many online inserts in one transaction".

The current measurements say:

- dense split child handoff is now dominated by HBC insertion itself
- split bookkeeping and quantized maintenance overhead have already been cut
- the remaining hot path is still online tree maintenance:
  - route to leaf
  - mutate leaf
  - split leaves/internal nodes incrementally
  - persist updated nodes repeatedly

So the next dense optimization should be structural, not another round of
micro-optimizing online insert.

### Principles

1. Build final nodes once whenever possible.
2. Quantize once per finished node, not once per inserted vector.
3. Persist vectors and metadata once before tree construction.
4. Keep the online insert path for truly incremental writes.
5. Use the bulk builder first where Antfly is already batch-oriented:
   - dense split child rebuild
   - large batch ingest

### Phase 1: Empty-Index Bulk Builder

Objective:
- build a brand-new HBC tree from a batch in one write transaction

Plan:
1. Add an empty-index-only `bulkBuildWithMetadata(...)` API.
2. Persist all raw vectors and metadata once.
3. Precompute transformed vectors once.
4. Recursively partition the batch into final leaves.
5. Build parent nodes upward from finished children.
6. Quantize each final node exactly once as it is written.

Acceptance:
- no per-item online insert loop in the bulk-build path
- search works correctly on the built index
- `active_count` and metadata are correct after reopen

### Phase 2: Bench Comparison

Objective:
- test whether bulk build actually beats the current batch path

Plan:
- compare:
  - current online single insert
  - current online batch insert
  - new bulk builder
- measure:
  - build time
  - search latency
  - resulting tree shape

Acceptance:
- bulk build is measurably faster than online batch insert on realistic batch
  sizes without unacceptable search regressions

### Phase 3: Split Child Rebuild Integration

Objective:
- replace dense split child rebuild's online batch insert loop

Plan:
- wire dense split child rebuild to feed `BatchInsertItem`s into the bulk
  builder instead of `batchInsertWithMetadata...`
- compare child split handoff timing before and after

Acceptance:
- `dense_handoff` in the split prepare probe drops materially

### Phase 4: Better Bulk Partitioning

Objective:
- improve resulting tree quality and split locality once the first bulk builder
  is in place

Candidates:
- recursive builder using the current HBC split algorithm
- Hilbert-seeded leaf construction
- more split-local leaf grouping by document ownership

Current status:
- both recursive and Hilbert-seeded bulk-build paths now exist
- doc-key-seeded bulk build also now exists as an experimental path
- first HBC bench comparison on `256` docs / `64` dims / `4` queries:
  - recursive bulk build: about `11.0ms`
  - Hilbert-seeded bulk build: about `11.9ms`
  - doc-key-seeded bulk build: about `17.1ms`
- Hilbert-seeded build produced a slightly smaller tree and slightly cheaper
  search on this workload, but it did not beat recursive bulk build on total
  build time yet
- doc-key-seeded build produced the best split locality on the synthetic HBC
  bench:
  - `frontier_right=1`
  - `mixed_right_members=0`
  but it regressed the actual dense child split handoff path when used there,
  and also did not improve the DB split prepare probe when used for source-side
  empty-index ingest
- recursive bulk build should remain the product default for now
- doc-key-seeded should stay experimental until it wins on the product-shaped
  probes, not just the synthetic HBC bench

Acceptance:
- keep the best strategy based on measured build time and search quality, not
  assumptions
