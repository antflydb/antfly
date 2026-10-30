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

- an HNSW-style layered graph: a pruned bottom layer (CSR, 4 bytes per edge)
  and unpruned upper layers holding about 1 in M nodes,
- one PQ code per vector (`pq_subspaces` bytes, 8-bit codebooks),
- one flag byte and one level byte per vector,
- a fixed PQ codebook (`256 * dims` floats).

At query time the graph is walked with PQ distances, and only the most
promising candidates are re-embedded through an `EmbeddingSource` callback to
get exact distances. The trade is storage for query-time embedding compute,
which suits large, cold, or edge (Lite) corpora where the source text is
already stored and the embedder is local.

| Per vector, 768-d | Bytes | vs. f32 |
|---|---:|---:|
| Raw f32 embedding | 3072 | 100% |
| LEANN: 96 PQ bytes + ~9 edges + offset + flag + level + upper layers | ~143 | ~4.7% |
| LEANN with `pq_subspaces = 32` | ~79 | ~2.6% |

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
standard HNSW graph is built with exact distances, the HNSW neighbor
heuristic, and degree cap `max_degree` (M) on every layer. Node levels are
`floor(-ln(U) / ln(M))` with U derived from a hash of `(seed, id)`, so builds
and inserts assign the same levels on every replica. The entry point is a node
on the top layer. The vectors are then discarded.

**High-degree preserving pruning (paper Algorithm 3).** Only layer 0 is
pruned; the upper layers stay as built and carry the long-range links. The
top `hub_fraction` (β, default 2%) of nodes by layer-0 in-degree are hubs and
re-select up to M neighbors; every other node re-selects `low_degree` (m,
default M/5). Candidates come from a descent plus an `ef_construction` beam
search over the original graph. Reverse links may grow any list to M. A final
NSG-style pass links any layer-0 node unreachable from the entry point to its
nearest reachable node. It is a safety net: it linked 0 nodes on GloVe and
86 of 20,000 on the synthetic clusters (`Index.repaired`).

**Search.** The query descends the upper layers greedily with PQ distances, so
the descent costs no recomputation. `exact_descent = true` recomputes
instead, at roughly levels × M extra embeddings per query; it helps when PQ is
too coarse to find the right region (synthetic data) and adds cost otherwise
(GloVe).

**Two-level search on layer 0 (Algorithm 2).** Expanding a node pushes its
unvisited neighbors into an approximate queue keyed by PQ distance. After each
expansion the top `rerank_ratio` (α, default 0.1) of that queue is promoted
for exact recomputation; exact results drive traversal and the result beam
(`ef`). With `batch_size > 0`, promoted candidates accumulate across steps
until the batch is full (the paper's dynamic batching). `two_level = false`
recomputes every neighbor (plain Algorithm 1) for comparison.

**Updates (Appendix B).** `insert` assigns a level, descends and links the
upper layers with PQ distances (closest M per layer, no recomputation), then
runs a two-level search on layer 0 and links the closest `low_degree` live
candidates. A full reverse list drops its farthest member by PQ distance. A
node above the current top level becomes the new entry point. Deletes set a
flag; deleted nodes stay traversable and are filtered from results. The paper
suggests rebuilding once `deletedFraction()` passes about 5%.

### Differences from the paper

- **Insert shrinking** uses PQ distances instead of the paper's distance cache.
- **Connectivity safety net** after pruning (see above); the paper has none.
- **Not implemented:** sharded builds under a storage budget, the batched
  insert buffer, and the hot-embedding cache (an `EmbeddingSource` can wrap
  one).

An earlier single-layer version pruned the only layer and lost the long-range
links, splitting the graph into per-cluster islands (1% recall at 20k
vectors). It needed hubs to keep all their edges to recover. The multi-layer
version needs no such special case and does better on real data (below).

## Benchmark

```sh
cd zig
zig run -OReleaseFast lib/vectorindex/src/leann_bench.zig -- \
  --data PREFIX --n 100000 --queries 200 --metric cosine
```

`PREFIX.train.f32` / `PREFIX.test.f32` hold a u32 row count, u32 dimension,
then row-major f32s. Without `--data` it uses Gaussian clusters.

GloVe-25 (ann-benchmarks `glove-25-angular`), first 100k vectors, 200
queries, cosine, M=30, efC=128, single thread, recall@10 against brute force.
Three levels, 3,295 nodes above layer 0, 0 nodes repaired.

| Graph | Avg L0 degree | Build | ef | Mode | Recall@10 | Recomputed/q | Batches/q |
|---|---:|---:|---:|---|---:|---:|---:|
| Pruned (LEANN) | 9.4 | 41 s | 16 | plain | 0.724 | 201 | 23 |
| | | | 64 | plain | 0.913 | 548 | 69 |
| | | | 64 | two-level α=0.1 | 0.915 | 491 | 69 |
| | | | 64 | two-level + batch 64 | 0.929 | 631 | 13 |
| | | | 128 | plain | 0.964 | 961 | 131 |
| | | | 256 | plain | 0.989 | 1718 | 256 |
| Unpruned | 22.4 | 21 s | 16 | plain | 0.862 | 375 | 20 |
| | | | 16 | two-level α=0.1 | 0.859 | 245 | 21 |
| | | | 32 | plain | 0.942 | 614 | 36 |

Multi-layer vs. the earlier single-layer version (pruned, plain search):

| ef | Single-layer | Multi-layer |
|---:|---|---|
| 16 | 0.696 @ 233 | 0.724 @ 201 |
| 64 | 0.904 @ 565 | 0.913 @ 548 |
| 256 | 0.985 @ 1678 | 0.989 @ 1718 |

Takeaways, consistent with the paper:

- The pruned graph has 42% of the layer-0 edges but needs about the same
  number of recomputations for the same recall (around 0.91 at ~550).
- Two-level search cuts recomputations by 10–35% at equal recall.
- Dynamic batching cuts embedder calls 5–7× for ~15–60% more recomputations,
  a good trade when the embedder runs on a GPU or a remote service.
- At 25 dimensions the index is 52% of the raw vectors because graph edges
  dominate; savings grow with dimension (see the table above).

On the synthetic set (20k Gaussian clusters, 64-d, isotropic noise), the
pruned graph tops out at 0.90 recall with PQ descent and 0.97 with
`exact_descent`, versus 1.0 unpruned: when the descent lands in the wrong
cluster, the sparse layer 0 has few links out. Real embeddings have far lower
intrinsic dimension than this set, and GloVe shows no such ceiling.

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
