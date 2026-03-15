# Sparse Vector (SPLADE) Support for Antfly

## Context

Antfly's hybrid search combines BM25 full-text (`full_text_v0`) with dense vector similarity (`aknn_v0`) via RRF/RSF fusion. Sparse vectors (SPLADE) bridge the gap between lexical and semantic search by producing learned term weights with neural expansion -- a document about "car" also activates "vehicle" and "automobile". Rather than modifying Bleve or the HBC vector index, this adds a new Pebble-backed chunked inverted index (`sparse_v0`) alongside them, with a new SPLADE inference pipeline in Termite.

## Workstreams

Workstreams 1 and 2 are independent and can run in parallel. Workstream 3 depends on both. Workstreams 4 and 5 depend on 3.

```
WS1 (Termite SPLADE) ✓    WS2 (Sparse Index Library)
                                     |
                                     v
                           WS3 (sparse_v0 index type)
                                     |
                           +---------+---------+
                           |                   |
                           v                   v
                   WS4 (Hybrid fusion)  WS5 (Import support)
```

---

## WS1: Termite SPLADE Pipeline -- DONE

Implemented: `SparseEmbeddingPipeline`, `/sparse_embed` endpoint, Termite client `SparseEmbed()` method, SPLADE activation via go-highway SIMD (`ExpVec`/`LogVec`/`ReLU`/`Max`), model export support.

---

## WS2: Sparse Vector Index Library

### go-highway dependencies -- DONE

The following go-highway packages are already implemented with `hwygen`-generated SIMD code (AVX2, AVX-512, NEON):

**`hwy/contrib/quantize/`** -- uint8 ↔ float32 quantization:
```go
quantize.DequantizeUint8(input []uint8, output []float32, min, scale float32)
quantize.QuantizeFloat32(input []float32, output []uint8, min, scale float32)
```

**`hwy/contrib/algo/`** -- delta encoding/decoding (in-place, SIMD prefix sum):
```go
algo.DeltaDecode[T hwy.Integers](data []T, base T)  // SIMD prefix sum
algo.DeltaEncode[T hwy.Integers](data []T, base T)  // scalar reverse-order
```

### Sparse index library

New package `lib/sparseindex/` -- a Pebble-backed chunked inverted index.

### Pebble key schema

```
fwd:<doc_id>              -> [doc_num uint64] [encoded sparse vector: N × (term_id int32, weight float32)]
inv:<term_id>:chunk<N>    -> chunked posting list (struct-of-arrays, see encoding format)
inv:<term_id>:meta        -> term metadata: max_weight (float32), chunk_count (uint32)
meta:doc_count            -> uint64 sequential doc number counter
```

Chunk size: 1024 docs per chunk (matching zapx default). Chunk assignment: `doc_num / chunk_size`.

Each chunk header includes the chunk's max weight (`float32`). The `inv:<term_id>:meta` key stores the global max weight across all chunks for that term. These are used by Block-Max WAND to skip chunks/terms that can't contribute enough score to enter the top-k.

### Chunk encoding format

```
[format_version uint8]          // v1 = uint8 quantized (format_version reserved for future formats)
[num_entries    uint32]
[max_weight     float32]        // block-max for BMW pruning (always float32)
[min_weight     float32]        // quantization range lower bound
[doc_id_deltas... N × uint32]   // sorted, delta-encoded (go-highway delta encode)
[weights...      N × uint8]     // quantized, q = round((w - min) / (max - min) * 255)
```

Per-chunk quantization: each chunk has its own min/max range, so inserting new docs never requires requantizing existing chunks. Decoder always returns `[]float32` via `quantize.DequantizeUint8()`.

Doc IDs sorted within each chunk, stored as fixed-width uint32 deltas. Weights stored as uint8 quantized. Struct-of-arrays layout (separate arrays rather than interleaved) enables SIMD delta decode of all doc IDs in one pass and SIMD dequantize of all weights in one pass. Forward index uses: `[num_pairs uint32] [term_ids... N × int32] [weights... N × float32]` sorted by term_id.

### SIMD acceleration via go-highway

Use `github.com/ajroetker/go-highway` for hot paths:

**Chunk decoding**:
- `algo.DeltaDecode[uint32]()` (SIMD prefix sum) reconstructs absolute doc IDs in one pass
- `quantize.DequantizeUint8()` for SIMD uint8 → float32 weight dequantization

**Document scoring** (`hwy/contrib/vec/`):
- Collect query weights + doc weights for matching terms into contiguous `[]float32`
- `vec.Dot()` computes the sparse dot product via SIMD

**Top-k extraction**: Standard heap (SIMD not beneficial here due to random-access pattern)

### Files to create

**`lib/sparseindex/sparseindex.proto`** -- protobuf definition for Raft/Pebble wire format:
```protobuf
edition = "2023";
package antfly.lib.sparseindex;
option go_package = "github.com/antflydb/antfly/lib/sparseindex";

message SparseVector {
  repeated int32 indices = 1;   // vocabulary term IDs, sorted
  repeated float values = 2;    // corresponding weights
}
```

Run `make generate` to produce `sparseindex.pb.go`. The generated `SparseVector` replaces the hand-written struct.

**`lib/sparseindex/sparse.go`**
```go
// SparseVector is generated from sparseindex.proto

type SparseIndex struct {
    db        *pebble.DB
    chunkSize int
    prefix    []byte
}

type Config struct {
    ChunkSize int    // default 1024
    Prefix    []byte
}

func New(db *pebble.DB, cfg Config) *SparseIndex
func (si *SparseIndex) Batch(inserts []BatchInsert, deletes [][]byte) error  // coalesced chunk writes
func (si *SparseIndex) Search(query SparseVector, k int, filterIDs []string) (*SearchResult, error)  // SearchResult compatible with vectorindex.SearchResult for fusion
func (si *SparseIndex) Stats() map[string]any
func (si *SparseIndex) Close() error

type BatchInsert struct {
    DocID []byte
    Vec   SparseVector
}
```

**`lib/sparseindex/encoding.go`** -- encode/decode sparse vectors and posting lists
- Encode: sort doc IDs, `algo.DeltaEncode[uint32]()` for deltas, compute chunk min/max, `quantize.QuantizeFloat32()` for weights, write header + `[]uint32` deltas + `[]uint8` weights
- Decode: `algo.DeltaDecode[uint32]()` (SIMD prefix sum) for doc IDs + `quantize.DequantizeUint8()` for weights, always returns `([]uint64, []float32)`
- Search code always receives `[]float32` weights, never raw bytes

**`lib/sparseindex/search.go`** -- Block-Max WAND search with pivot selection:
1. Load term metadata (`inv:<term_id>:meta`) for all query terms to get per-term max weights
2. Initialize cursor iterators for each query term's posting list, positioned at first chunk
3. **Pivot selection**: at each step, sort terms by current cursor doc ID. Accumulate `query_weight × term_max_weight` from the lowest doc ID upward until the sum exceeds the k-th best score threshold. The term where the sum crosses the threshold is the **pivot**. All terms below the pivot can't contribute enough -- advance their cursors past the pivot doc ID (skip chunks via BMW block-max pruning)
4. **Chunk-level pruning (BMW)**: when advancing a cursor, check chunk `max_weight` from header; if `query_weight × chunk_max_weight` can't contribute enough to reach threshold, skip the entire chunk
5. When the pivot doc ID is found: collect query weights and doc weights for all matching terms into contiguous `[]float32` slices, score via `vec.Dot()`. Push to min-heap if score beats threshold
6. Top-k via min-heap, threshold tightens as heap fills -- later pivots skip more aggressively

**`lib/sparseindex/batch.go`** -- batch insert/delete with chunk coalescing:

Batch strategy (minimizes Pebble I/O):
1. Collect all chunk modifications across all documents in the batch
2. Group by chunk key (`inv:<term_id>:chunk<N>`) -- many documents share terms, so modifications coalesce
3. Single read-modify-write per unique chunk key per Pebble batch
4. Update `inv:<term_id>:meta` max weights and chunk counts atomically

Doc number assignment: sequential counter from `meta:doc_count`. Forward index stores: `fwd:<doc_id> → [doc_num uint64][encoded_sparse_vector]`. Chunk assignment: `doc_num / chunk_size`.

Chunk overflow: when inserting into a full chunk (1024 entries), split into two chunks and update `inv:<term_id>:meta` chunk_count. Chunk keys use the lowest doc_num in the chunk as `<N>` to maintain sorted order.

Update path (re-enrichment): read old forward index → diff old vs new term sets → only mutate chunks for added/removed/changed terms. Reuse same doc_num.

Delete path: read forward index → remove entries from all affected chunks → delete forward key → update term metadata max weights (recompute from remaining chunk maxes if deleted entry was the max).

**`lib/sparseindex/sparse_test.go`**
- Encoding/decoding round-trip (uint8 quantized)
- Quantization accuracy: encode float32 → uint8 → decode, verify error within step size
- Single insert + search
- Multi-doc search with top-k ordering
- Delete removes from results
- Chunk boundary behavior (>1024 docs for same term)
- BMW pruning correctness: verify skipped chunks don't contain top-k results
- Pivot selection: verify terms below pivot are correctly skipped
- Filter IDs
- Empty/edge cases

**`lib/sparseindex/benchmark_test.go`**
- Search latency at 1K, 10K, 100K docs
- Insert throughput
- Chunk size impact

---

## WS3: `sparse_v0` Index Type

Register `sparse_v0` as a new index type implementing `Index` + `EnrichableIndex`. Follows the `aknn_v0.go` pattern exactly.

### Files to modify

**`src/store/db/indexes/openapi.yaml`**
1. Add `sparse_v0` to `IndexType` enum
2. Add `SparseIndexConfig` schema (field, template, embedder ref, top_k, min_weight, chunk_size)
3. Add `SparseIndexV0Stats` schema (error, total_indexed, disk_usage, total_terms)
4. Add to `IndexConfig.oneOf` and `IndexStats.oneOf`

Run `make generate` to regenerate `openapi.gen.go`.

**`src/store/db/indexmgr.go`**
- In the `SyncLevelAknn` handler (~line 171-174): add `IndexTypeSparseV0` alongside `IndexTypeAknnV0` and `IndexTypeFullTextV0`
- No proto changes -- reuse existing `SyncLevelAknn` for all semantic indexes

### Files to create

**`src/store/db/indexes/sparse_v0.go`** -- main index implementation (~800-1000 lines)

Struct mirrors `EmbeddingIndex` in `aknn_v0.go`:
- Separate `indexDB *pebble.DB` for sparse index data (same pattern as aknn_v0 line 464-525)
- `sparseIdx *sparseindex.SparseIndex` from WS2
- Key suffix `:i:<name>:sp` for sparse embedding storage in main shard Pebble
- Stored value at `<doc_key>:i:<name>:sp`: `[hashID uint64][proto.Marshal(sparseindex.SparseVector)]`
- Uses protobuf `SparseVector` from `lib/sparseindex/sparseindex.proto` (see WS2)
- Flows through Raft as `BatchOp.Write{key, value}` bytes -- no changes to `src/store/db/ops.proto`
- Metadata node converts user-facing `map[int32]float32` JSON into protobuf `SparseVector` before Raft proposal; storage nodes only see the structured format

Key methods:
- `init()` -- `RegisterIndex(IndexTypeSparseV0, NewSparseIndex)`
- `Open()` -- create indexDB Pebble instance, initialize `sparseindex.SparseIndex`
- `Batch(ctx, writes, deletes, sync)` -- detect enricher suffix keys, decode sparse vectors and insert into sparse index, enqueue non-enriched docs for async processing
- `Search(ctx, query)` -- accept `*SparseSearchRequest`, call `embedder.(SparseEmbedder).SparseEmbed()` if query needs embedding, call `sparseIdx.Search()`, return `*vectorindex.SearchResult` for fusion compatibility
- `NeedsEnricher() bool` -- return true
- `LeaderFactory(ctx, persistFunc)` -- start sparse enricher on Raft leader (follows aknn_v0 lines 1086-1213)

**`src/store/db/indexes/sparseenricher.go`** -- async enrichment (~400 lines)

Follows `embeddingenricher.go` pattern:
- WALBuffer for durable async queue
- Dequeue loop + backfill loop (scans for docs missing `:i:<name>:sp` keys)
- Rate limiting via `rate.Limiter`
- Calls `embedder.(SparseEmbedder).SparseEmbed()` for batch embedding
- PersistFunc callback submits writes through Raft

**`lib/embeddings/plugin.go`** -- add `SparseEmbedder` interface:
```go
type SparseEmbedder interface {
    SparseEmbed(ctx context.Context, texts []string) ([]sparseindex.SparseVector, error)
}
```

**`lib/embeddings/capabilities.go`** -- add `SupportsSparse bool` to `EmbedderCapabilities`

**`lib/embeddings/termite.go`** -- add `SparseEmbed()` method to existing `TermiteClient` (calls `/sparse_embed`). The Termite client now implements both `Embedder` and `SparseEmbedder`. No new struct needed.

sparse_v0 index config uses the same `EmbedderConfig` reference as aknn_v0 (same `embedder` field, same provider registry). At enricher creation: `embedder.(SparseEmbedder)` type assertion.

### Reference files
- `src/store/db/indexes/aknn_v0.go` -- primary structural reference
- `src/store/db/indexes/embeddingenricher.go` -- enricher reference
- `src/store/db/indexes/indexes.go:82-103` -- registration pattern
- `src/store/db/indexmgr.go:637-676` -- index creation in manager

---

## WS4: Hybrid Search Integration

Sparse results feed into the existing RRF/RSF fusion alongside BM25 and dense vector results. No new API fields -- the existing semantic search query routes to all semantic-capable indexes (both aknn_v0 and sparse_v0).

### Files to modify

**`src/store/db/db.go`**
- In the existing `VectorSearches` dispatch loop (line ~3025-3063): when iterating indexes for the semantic search query, include `sparse_v0` indexes alongside `aknn_v0`. For sparse indexes, create `SparseSearchRequest` with query text, limit, filter prefix. Store result in `res.VectorSearchResult[indexName]`
- `res.RRFResults()` / `res.RSFResults()` at line 3090-3093 already iterate `VectorSearchResult` -- sparse results participate automatically

**`src/store/db/indexes/remoteindex.go`**
- Sparse search results stored in existing `VectorSearchResult` map (reuse `vectorindex.SearchResult` type)
- No new fields on `RemoteIndexSearchRequest` -- the existing semantic search query text routes to both dense and sparse indexes

---

## WS5: Pre-computed Sparse Embedding Import

Reuse the existing `_embeddings` field. The import handler distinguishes dense from sparse by value type: an array is a dense vector (→ aknn_v0), an object/map is a sparse vector (→ sparse_v0).

### Document format
```json
{
  "title": "My Document",
  "content": "...",
  "_embeddings": {
    "my_dense_index": [0.1, 0.2, 0.3],
    "my_sparse_index": {"42": 0.8, "1337": 1.2, "9001": 0.3}  // keys parse as int32, values as float32
  }
}
```

Sparse vectors are `map[int32]float32` where keys are vocabulary term IDs (integers) and values are weights. JSON wire format is `{"42": 0.8, ...}` (JSON keys are always strings, but Go's `encoding/json` unmarshals string keys into `map[int32]float32`). This enforces term IDs are valid integers and matches the internal `SparseVector.Indices []int32`.

### Files to modify

**`src/store/db/db.go`**
- In the existing `_embeddings` handler in `ExtractEnrichments()` (~line 1569): after detecting the index by name, check the value type:
  - `[]any` (array) → existing dense vector path (aknn_v0)
  - `map[string]any` (object) → new sparse vector path: parse keys as int32 term IDs and values as float32 weights, encode as `<key>:i:<name>:sp` write
- No new special field names, no new fast-path checks, no new field selection logic

**`src/store/db/indexes/sparse_v0.go`** -- implement `EmbeddingsPreProcessor` interface (same as aknn_v0 does) so the existing `_embeddings` handler can detect sparse_v0 indexes and route accordingly

---

## Verification

### Unit tests
- `lib/sparseindex/sparse_test.go` -- index operations, encoding, search accuracy
- `termite/pkg/termite/lib/pipelines/sparse_embedding_test.go` -- SPLADE activation, sparsification
- `src/store/db/indexes/sparse_v0_test.go` -- index type lifecycle, batch, search

### Integration tests
Run with `make e2e E2E_TEST=TestSparse`:
- `e2e/sparse_test.go`:
  - Write docs → SPLADE enrichment → sparse search returns results
  - Three-way hybrid: sparse + dense + BM25 with RRF
  - Import via `_embeddings` field with `map[int32]float32` sparse format
  - Multi-shard scatter-gather

### Build verification
```bash
make generate              # After OpenAPI changes
GOEXPERIMENT=simd go build ./...  # Full build
GOEXPERIMENT=simd go test ./lib/sparseindex/...  # Library tests
GOEXPERIMENT=simd go test ./src/store/db/indexes/... -run Sparse  # Index type tests
make e2e E2E_TEST=TestSparse  # E2E
```

