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
partitioning strategies, along with an experimental doc-key-seeded path. On a
first HBC bench comparison (`256` docs / `64` dims / `4` queries):

- recursive bulk build: about `11.0ms`
- Hilbert-seeded bulk build: about `11.9ms`
- doc-key-seeded bulk build: about `17.1ms`

Hilbert-seeded build produces a slightly smaller tree and slightly cheaper
search on this workload, but it does not beat recursive bulk build on total
build time. Doc-key-seeded build produces the best split locality on the
synthetic HBC bench (`frontier_right=1`, `mixed_right_members=0`), but it
regresses the actual dense child split handoff path when used there, and does
not improve the DB split prepare probe when used for source-side empty-index
ingest.

Recursive bulk build is the product default. Doc-key-seeded stays experimental
until it wins on the product-shaped probes, not just the synthetic HBC bench.
The best strategy is chosen from measured build time and search quality, not
assumptions.

## Open work

- Whether Hilbert-seeded or doc-key-seeded bulk build should replace recursive
  bulk build as the default depends on further product-shaped benchmarking,
  not just the synthetic HBC bench comparison recorded above.
