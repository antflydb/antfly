# LEANN Dense Indexing

## Status

Dense `embeddings` indexes accept `algorithm: "leann"` (default `"hbc"`)
with optional `leann` tuning (`max_degree`, `quantizer`, `search_ef`,
`bound_scale`, `batch_size`). The algorithm is in
`lib/vectorindex/src/leann.zig`; the table integration is described in
[Table integration](#table-integration). Not yet supported for LEANN indexes:
shard splits, remote dense storage, native index backups (backups use the
logical path), and re-embedding from text instead of stored embeddings
([Remaining work](#remaining-work)).

## Overview

HBC keeps full-precision vectors for rerank. LEANN keeps none. It stores:

- an HNSW-style layered graph: a pruned bottom layer (CSR, 4 bytes per edge)
  and unpruned upper layers holding about 1 in M nodes,
- one compressed code per vector, either
  - **PQ** (`quantizer = .pq`, the paper's choice): `pq_subspaces` bytes plus a
    fixed `256 * dims`-float codebook, or
  - **RaBitQ** (`quantizer = .rabitq`): 1 bit per dimension plus 16 bytes of
    per-vector factors, using the same RaBitQ kernel as HBC,
- one flag byte and one level byte per vector.

At query time the graph is walked with approximate distances, and only
candidates that can still matter are re-embedded through an `EmbeddingSource`
callback to get exact distances. PQ follows the paper and re-embeds a fixed
fraction of candidates. RaBitQ also gives an error bound per estimate, so it
re-embeds only candidates whose bound overlaps the top-k cutoff, about 10x
fewer on 256-d data (see [Benchmark](#benchmark)). The trade is storage for query-time embedding compute,
which suits large, cold, or edge (Lite) corpora where the source text is
already stored and the embedder is local.

| Per vector, 768-d | Bytes | vs. f32 |
|---|---:|---:|
| Raw f32 embedding | 3072 | 100% |
| LEANN: 96 PQ bytes + ~9 edges + offset + flag + level + upper layers | ~143 | ~4.7% |
| LEANN with `pq_subspaces = 32` | ~79 | ~2.6% |
| LEANN with RaBitQ: 96 code bytes + 16 factor bytes + graph | ~159 | ~5.2% |

## API

```zig
const leann = @import("antfly_vector_index").leann;

var index = try leann.Index.build(alloc, .{ .dims = 768, .metric = .cosine, .quantizer = .rabitq }, vectors);
defer index.deinit();

// Recompute embeddings for node ids (normally: load text, call the embedder).
const source = leann.EmbeddingSource{ .ctx = ctx, .recompute_fn = recompute };

// RaBitQ defaults to bound-driven reranking (ef 512); PQ to the paper's
// two-level search (ef 64).
var resp = try index.search(alloc, source, query, .{ .k = 10, .batch_size = 32 });
defer resp.deinit(alloc);
// resp.results: []Result{ id, distance }; resp.stats: recomputed, batches, ...

const id = try index.insert(source, new_vector); // dense ids, insertion order
try index.markDeleted(id);                       // O(1) soft delete
const bytes = try index.serialize(alloc);        // CRC-checked, no embeddings
```

Ids are dense `u32` node numbers; the caller maps them to document keys. Writes
are single-threaded; concurrent searches are safe between writes.

## Algorithm

**Build.** Vectors are normalized for cosine and encoded. PQ trains one Lloyd
k-means codebook per subspace on a sample. RaBitQ rotates every vector with a
seeded random orthogonal (Givens) transform, which RaBitQ's error analysis
assumes, and encodes the residual from the mean of the build set. Rotation
preserves distances, so the graph is built on the rotated vectors. A
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

**Search.** The query descends the upper layers greedily with approximate
distances, so the descent costs no recomputation. `exact_descent = true` recomputes
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

**Bound-driven search on layer 0 (RaBitQ, `rerank = .bounds`).** Phase 1
walks layer 0 with a beam of `ef` using RaBitQ estimates only, keeping every
scored candidate with its lower bound, `estimate - bound_scale * bound`. The
kernel's bound is about one standard error of the estimator. Phase 2 sorts
candidates by lower bound and re-embeds them in batches of `batch_size`
(default k), stopping when the next lower bound is at least the k-th exact
distance found so far. The walk costs no embeddings, so a wide beam
(default 512) is cheap; recall is limited by the walk, and `bound_scale`
trades recomputations for safety.

**Updates (Appendix B).** `insert` assigns a level, descends and links the
upper layers with approximate distances (closest M per layer, no
recomputation), then links the closest `low_degree` live candidates on layer
0. With PQ these come from a two-level search; with RaBitQ from an
estimate-only beam, so RaBitQ inserts re-embed nothing. A full reverse list
drops its farthest member by approximate distance, but never a node's last
inbound layer-0 edge (in-degrees are tracked in memory and rebuilt on load). If
every reverse list rejects the new node, it is attached to its closest
neighbor over the cap. A node above the current top level becomes the new
entry point. Deletes set a
flag; deleted nodes stay traversable and are filtered from results. The paper
suggests rebuilding once `deletedFraction()` passes about 5%.

### Differences from the paper

- **RaBitQ and bound-driven reranking** replace the paper's PQ and fixed
  rerank ratio when `quantizer = .rabitq`; PQ mode follows the paper.
- **Insert shrinking** uses approximate distances instead of the paper's
  distance cache, and protects each node's last inbound edge.
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
zig build-exe -OReleaseFast -lc -femit-bin=zig-out/bin/leann-bench \
  --dep antfly_vector -Mroot=lib/vectorindex/src/leann_bench.zig \
  --dep protobuf -Mantfly_vector=lib/vector/src/mod.zig \
  -Mprotobuf=lib/protobuf/src/root.zig
zig-out/bin/leann-bench --data PREFIX --n 100000 --queries 200 \
  --metric cosine [--quantizer pq|rabitq] [--unpruned 1] [--cost FILE]
```

`PREFIX.train.f32` / `PREFIX.test.f32` hold a u32 row count, u32 dimension,
then row-major f32s. Without `--data` it uses Gaussian clusters. `--cost`
takes an embedder cost curve from `scripts/leann_embed_cost.py` and adds
modeled embedding time per query.

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

### PQ vs. RaBitQ bounds (NYTimes-256)

ann-benchmarks `nytimes-256-angular`, first 100k vectors, 200 queries,
cosine, pruned graph (avg degree 9.75), M=30, efC=128. NYTimes is a hard set
for graph indexes. Index size: PQ 8.2% of raw f32, RaBitQ 9.5%.

| Quantizer | Mode | ef | Recall@10 | Recomputed/q | Batches/q |
|---|---|---:|---:|---:|---:|
| PQ | two-level α=0.1 | 128 | 0.801 | 1222 | 148 |
| PQ | two-level α=0.1 | 256 | 0.851 | 2234 | 274 |
| PQ | two-level + batch 64 | 256 | 0.851 | 2421 | 39 |
| RaBitQ | two-level α=0.1 | 256 | 0.847 | 2225 | 273 |
| RaBitQ | bounds ×1 | 256 | 0.815 | 120 | 12 |
| RaBitQ | bounds ×1 | 1024 | 0.892 | 207 | 21 |
| RaBitQ | bounds ×1, batch 32 | 1024 | 0.893 | 210 | 7 |
| RaBitQ | bounds ×1.5 | 1024 | 0.915 | 424 | 43 |
| RaBitQ | bounds ×2 | 1024 | 0.918 | 795 | 80 |

- Bound-driven reranking reaches higher recall than the paper's two-level
  search with about a tenth of the recomputations (0.892 at 207 vs. 0.851 at
  2,234).
- RaBitQ with the paper's ratio rule performs like PQ; the gain comes from
  using the bounds, not the codes.
- A wide estimate-only beam is cheap (ef=1024 adds ~2.6 ms CPU per query) and
  is where recall comes from; `bound_scale` above 1.5 buys little.
- On 64-d synthetic data, 1-bit bounds are too loose to help much (~250
  recomputations per query): the bound shrinks as 1/sqrt(dims), so bounds
  suit typical 384–1024-d text embeddings, and PQ remains the better choice
  for very low dimensions.

### Embedder cost and batching

Measured with `scripts/leann_embed_cost.py` against Antfly's inference server
(`zig build http-server -Doptimize=ReleaseFast -Dstrip=true -Dcpu=native`)
running `BAAI/bge-small-en-v1.5` on a 4-vCPU AVX-512 sandbox, with ~200-token
passages. ONNX Runtime (CPU provider) on the same machine and model for reference.

| Embedder | Batch 1 | Batch 4 | Batch 16 | Batch 64 |
|---|---:|---:|---:|---:|
| Antfly, one request per call (ms/call) | 249 | 1,241 | 6,297 | 36,818 |
| Antfly, call split into 4 parallel requests (ms/call) | 304 | 294 | 1,286 | 6,467 |
| ONNX Runtime, 1 thread (ms/passage) | 141 | 131 | 140 | 155 |
| ONNX Runtime, 4 threads (ms/passage) | 50 | 46 | 42 | 42 |

- On CPU, batching inside one request does not help: Antfly's per-passage cost
  rises from 249 ms to 575 ms at batch 64, and ONNX Runtime gains at most
  ~17%. Each Antfly request runs on about one core.
- Splitting a call into parallel requests fills the cores: 1–4 passages cost
  the same ~300 ms, then cost grows linearly (~80 ms per passage).
- Antfly's runtime is ~1.8x slower per thread than ONNX Runtime on this model
  and CPU, and does not parallelize within a request.

Modeled embedding time per query on NYTimes-256 (the bench's `--cost` replays
each recompute call against the curve):

| Search | Recall@10 | Recomputed/q | One request per call | 4-way fan-out |
|---|---:|---:|---:|---:|
| PQ two-level, ef 256 | 0.851 | 2,234 | 813 s | 175 s |
| PQ two-level + batch 64, ef 256 | 0.851 | 2,421 | 1,395 s | 246 s |
| RaBitQ bounds, ef 512 | 0.851 | 157 | 58 s | 12 s |
| RaBitQ bounds, ef 1024 | 0.892 | 207 | 77 s | 16 s |
| RaBitQ bounds + batch 32, ef 1024 | 0.893 | 210 | 105 s | 18 s |

Guidance:

- Reduce the number of embeddings first (RaBitQ bounds: ~14x here), then
  make each call use the hardware. An `EmbeddingSource` on CPU should fan a
  call out across cores or replicas; `batch_size` should be about the
  embedder's parallelism. Larger batches only add recomputations on CPU.
- Large batches pay off when per-call overhead dominates: GPUs (the paper's
  1.8x at batch 64) and remote APIs with 50–200 ms round trips. Measure with
  `leann_embed_cost.py` and replay with `--cost` before choosing.
- Even the best CPU configuration here spends 12–16 s per query embedding,
  so interactive LEANN search needs a GPU or a much faster CPU embedder; on
  CPU it suits batch or offline retrieval. The paper reports ~2.5 s on an
  RTX 4090 with Contriever.

## Table integration

```json
{"type": "embeddings", "field": "body", "embedder": {...},
 "algorithm": "leann", "leann": {"quantizer": "rabitq", "max_degree": 30}}
```

- **Config.** `specs/openapi/antfly/indexes.yaml` adds `DenseIndexAlgorithm`
  and `LeannIndexConfig`. `public_index_contract.zig` admits the fields, and
  `translateEmbeddingsIndexConfigJsonWithOptions` (managed_embedder.zig)
  validates them and writes normalized `"algorithm":"leann","leann":{...}`
  into the internal config, so the config hash covers every knob. A `leann`
  block without `algorithm: "leann"`, or LEANN on a sparse index, is rejected.
- **Where it plugs in.** A LEANN index is still a `dense_vector` index: the
  producers, derived replay, vector-id mapping, query planning, post-filters,
  hydration and paging are shared with HBC. The index manager opens the HBC
  adapter as usual and attaches a `LeannStore`
  (`pkg/antfly-embedded/src/storage/leann_store.zig`). The adapter then
  routes every write, delete, search, metadata lookup, `stats` and `sync`
  to LEANN, and the HBC tree underneath stays empty. HBC's native posting
  sidecar and WAL are not enabled for LEANN indexes.
- **LeannStore.** Maps the manager's u64 vector ids and metadata keys to
  LEANN node ids. The first 256 vectors are served by exact search, then the
  graph is built from them and the buffered vectors are dropped. Upserts and
  deletes are idempotent, so replay after a crash is safe. Searches answer
  HBC's `SearchRequest` with HBC's `SearchResults`, applying `filter_ids`,
  `exclude_ids`, the key predicate, the metadata prefix and distance limits
  to an over-fetched candidate list. Exact distances come from HBC's
  external vector loader (the stored embedding artifact, or the document
  field for direct-field indexes). Deleted or missing candidates are pushed
  to the far end of the metric rather than failing a search.
- **Persistence.** `sync` (called from `IndexManager.syncAll`, before the
  applied sequence is recorded) writes `leann.store` atomically with a CRC:
  the key map, the exact buffer, and the serialized graph. When at least
  20% of graph nodes are deleted, `sync` first rebuilds the graph from the
  live nodes' stored vectors. Rebuilds are deterministic (seeded levels,
  node order), so replicas that apply the same writes converge.
- **Guards.** `pruneDenseSplitRange` and `handoffDenseFrom` fail with
  `error.UnsupportedLeannSplit` before moving any state.
  `nativeBackupSupportsImmutableCheckpoint` returns false, so backups take
  the logical path and restore rebuilds from stored embeddings. Remote dense
  storage fails with `error.UnsupportedLeannStorage`.
- **Tests.** `leann_store.zig` (buffer, build, filters, idempotent writes,
  persistence, rebuild, corrupt file) and the index-manager test
  `leann dense index applies writes, searches, deletes, and reopens`.

### Storage today

LEANN replaces HBC's index files (quantized tree, posting lists, and the
full-precision rerank vectors HBC keeps) with a graph plus codes. The
embedding artifacts Antfly persists for every dense index remain, and LEANN
reads them for exact distances. So a LEANN index cuts index storage, not yet
the artifact storage. Dropping artifacts needs re-embedding from text,
below.

## Remaining work

1. **Re-embed instead of reading artifacts.** Plumb the table's dense
   embedder into the index manager and implement the vector loader as:
   vector id → document → render the index's field/template → embedder
   batch call, with a bounded hot-embedding cache, a per-query deadline and
   cancellation. Then stop persisting artifacts for LEANN indexes. On CPU,
   the measured cost (below) makes this practical only for batch retrieval
   or with a GPU embedder.
2. **Splits.** Rebuild both children from the parent's stored vectors (or
   re-embedded text) during handoff.
3. **Remote storage and native backups.** Store `leann.store` through the
   dense storage abstraction and include it in native checkpoints.
4. **Filter-aware traversal.** Filters are applied after an over-fetched
   search today; very selective filters would be better served by passing
   them into the graph walk.
5. **Leader-built rebuilds.** Rebuilds are replica-local and deterministic
   today. Large rebuilds could move to the Raft leader and ship the result,
   as `CLAUDE.md` requires for background mutation work.
6. **E2E coverage.** Add a Python e2e test that creates an
   `algorithm: "leann"` index over HTTP and queries it.
