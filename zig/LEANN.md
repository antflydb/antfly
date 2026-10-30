# LEANN Dense Indexing

## Status

`lib/vectorindex/src/leann.zig` implements LEANN
([arXiv:2506.08276](https://arxiv.org/abs/2506.08276)) as a standalone index
in `antfly_vector_index` (exported as `vectorindex.leann`). It has unit tests
and a benchmark. It is **not yet selectable** as a table index: HBC remains
the only dense `embeddings` implementation. [Integration](#integration-plan)
lists the work needed to change that.

## Overview

HBC keeps full-precision vectors for rerank. LEANN keeps none. It stores:

- a pruned proximity graph (CSR, 4 bytes per edge),
- one PQ code per vector (`pq_subspaces` bytes, 8-bit codebooks),
- one flag byte per vector (hub, deleted),
- a fixed PQ codebook (`256 * dims` floats).

At query time the graph is walked with PQ distances, and only the most
promising candidates are re-embedded through an `EmbeddingSource` callback to
get exact distances. The trade is storage for query-time embedding compute,
which suits large, cold, or edge (Lite) corpora where the source text is
already stored and the embedder is local.

| Per vector, 768-d | Bytes | vs. f32 |
|---|---:|---:|
| Raw f32 embedding | 3072 | 100% |
| LEANN: 96 PQ bytes + ~9 edges + offset + flag | ~137 | ~4.5% |
| LEANN with `pq_subspaces = 32` | ~73 | ~2.4% |

## API

```zig
const leann = @import("antfly_vector_index").leann;

var index = try leann.Index.build(alloc, .{ .dims = 768, .metric = .cosine }, vectors);
defer index.deinit();

// Recompute embeddings for node ids (normally: load text, call the embedder).
const source = leann.EmbeddingSource{ .ctx = ctx, .recompute_fn = recompute };

var resp = try index.search(alloc, source, query, .{ .k = 10, .ef = 64 });
defer resp.deinit(alloc);
// resp.results: []Result{ id, distance }; resp.stats: recomputed, batches, ...

const id = try index.insert(source, new_vector); // dense ids, insertion order
try index.markDeleted(id);                       // O(1) soft delete
const bytes = try index.serialize(alloc);        // CRC-checked, no embeddings
```

Ids are dense `u32` node numbers; the caller maps them to document keys. Writes
are single-threaded; concurrent searches are safe between writes.

## Algorithm

**Build.** Vectors are normalized for cosine, PQ codebooks are trained per
subspace with Lloyd k-means on a sample, and every vector is encoded. A
single-layer HNSW-style graph is built incrementally from the medoid (the
entry point) with the HNSW neighbor heuristic and degree cap `max_degree` (M).
The vectors are then discarded.

**High-degree preserving pruning (paper Algorithm 3).** The top
`hub_fraction` (β, default 2%) of nodes by in-degree are hubs. Every other node
re-selects `low_degree` (m, default M/5) neighbors with the heuristic from a
beam search over the base graph plus its base edges. Reverse links may grow any
list to M. A final NSG-style pass links every unreachable node from its nearest
reachable node.

**Two-level search (Algorithm 2).** Expanding a node pushes its unvisited
neighbors into an approximate queue keyed by PQ distance. After each
expansion the top `rerank_ratio` (α, default 0.1) of that queue is promoted
for exact recomputation; exact results drive traversal and the result beam
(`ef`). With `batch_size > 0`, promoted candidates accumulate across steps
until the batch is full (the paper's dynamic batching). `two_level = false`
recomputes every neighbor (plain Algorithm 1) for comparison.

**Updates (Appendix B).** `insert` runs a two-level search with
`ef_construction`, links the closest `low_degree` live candidates, and adds
reverse links. A full reverse list drops its farthest member by PQ distance,
so inserts recompute only the embeddings the search itself needs. Deletes set
a flag; deleted nodes stay traversable and are filtered from results. The
paper suggests rebuilding once `deletedFraction()` passes about 5%.

### Differences from the paper

- **Single-layer graph.** The paper prunes a hierarchical HNSW whose upper
  layers carry long-range links. Here the base graph is one layer, so those
  links live on early, high in-degree nodes. Pruning these hubs away split
  the graph into per-cluster islands in testing (1% recall at 20k vectors).
  Hubs therefore keep their base edges verbatim, non-hubs see their base
  edges as extra candidates, and a connectivity pass guarantees reachability.
- **Insert shrinking** uses PQ distances instead of the paper's distance cache.
- **Not implemented:** sharded builds under a storage budget, the batched
  insert buffer, and the hot-embedding cache (an `EmbeddingSource` can wrap
  one).

## Benchmark

```sh
cd zig
zig run -OReleaseFast lib/vectorindex/src/leann_bench.zig -- \
  --data PREFIX --n 100000 --queries 200 --metric cosine
```

`PREFIX.train.f32` / `PREFIX.test.f32` hold a u32 row count, u32 dimension,
then row-major f32s. Without `--data` it uses Gaussian clusters.

GloVe-25 (ann-benchmarks `glove-25-angular`), first 100k vectors, 200
queries, cosine, M=30, efC=128, single thread, recall@10 against brute force:

| Graph | Avg degree | Build | ef | Mode | Recall@10 | Recomputed/q | Batches/q |
|---|---:|---:|---:|---|---:|---:|---:|
| Pruned (LEANN) | 9.1 | 38 s | 64 | plain | 0.904 | 565 | 74 |
| | | | 64 | two-level α=0.1 | 0.901 | 510 | 74 |
| | | | 64 | two-level + batch 64 | 0.922 | 750 | 15 |
| | | | 128 | plain | 0.957 | 955 | 135 |
| | | | 128 | two-level + batch 64 | 0.962 | 1109 | 20 |
| Unpruned | 22.4 | 19 s | 16 | plain | 0.868 | 443 | 23 |
| | | | 16 | two-level α=0.1 | 0.870 | 298 | 23 |
| | | | 32 | plain | 0.938 | 675 | 38 |

Takeaways, consistent with the paper:

- The pruned graph has 40% of the edges but needs about the same number of
  recomputations for the same recall (around 0.90 at ~560).
- Two-level search cuts recomputations by 10–33% at equal recall.
- Dynamic batching cuts embedder calls 5–7× for ~15–30% more recomputations,
  a good trade when the embedder runs on a GPU or a remote service.
- At 25 dimensions the index is 47% of the raw vectors because graph edges
  dominate; savings grow with dimension (see the table above).

Recompute cost dominates real latency: ~500 embeddings per query is roughly
the paper's operating point (it reports ~2.5 s retrieval on an RTX 4090 with
Contriever). LEANN is a storage-first option, not a latency-first one.

## Integration plan

1. **API.** Add `algorithm: hbc | leann` (default `hbc`) to
   `EmbeddingsIndexConfig` in `specs/openapi/antfly/indexes.yaml`, plus an
   optional `leann` block (`max_degree`, `pq_subspaces`, `rerank_ratio`,
   `batch_size`). Reject `leann` with `external: true` or `sources`, since the
   server must be able to re-embed a document from its field or template.
   Run `make generate`.
2. **EmbeddingSource.** Implement recompute as: node id → document key →
   render the index's field/template → embedder batch call. Put a bounded
   hot-embedding cache in front, and give it a per-query deadline and
   cancellation, since a slow embedder now sits on the read path.
3. **Storage.** Persist `serialize()` output as an index generation file next
   to HBC's, with the node-id → key map in the index namespace. Inserts and
   deletes go through the existing derived-replay path; the graph is
   rewritten at checkpoint.
4. **Build and ownership.** Build from the enrichment stream, then drop the
   vectors. Only the Raft leader builds or rebuilds (on the delete-fraction
   threshold), then publishes the generation; replicas load it rather than
   rebuilding, so results stay identical across replicas.
5. **Query path.** Route `embeddings` searches on a `leann` index to
   `Index.search`, map ids to keys, and feed the existing fusion and filter
   code. Pre-filters need a filter hook in the traversal (not yet in the
   module).
6. **Splits.** A shard split rebuilds both children from re-embedded
   documents, or recomputes the PQ codes and graph from the parent's text.
