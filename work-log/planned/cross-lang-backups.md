# AFB: Antfly Portable Backup Format

## Context

Antfly has two backend implementations: Go (Pebble storage) and Zig (LMDB/LSM storage). Current backup formats are physical snapshots tied to each storage engine -- Go produces tar.zst of Pebble checkpoints, Zig produces `store.bin` + `derived-log.bin` dumps. Neither can be restored by the other backend.

The goal is a **logical backup format** (AFB -- Antfly Format for Backups) that captures data at the semantic level (documents, vectors, edges, enrichments) rather than the physical storage level. Both Go and Zig implement readers/writers independently. This is an **optional** format alongside the existing per-engine "native" snapshots.

Key trade-off: indexes (Bleve, HNSW, sparse posting lists) are **not** exported -- only source data and enrichment artifacts. Restore rebuilds indexes. This makes the format truly portable at the cost of slower restore (acceptable since native snapshots remain available for same-backend fast restore).

## Format Specification: AFB v1

### File Structure

```
[File Header]           -- 64 bytes, magic + version + backup ID
[Cluster Manifest]      -- JSON block: backup metadata, table list
[Table Manifest]*       -- JSON block per table: schema, indexes, shard layout
  [Shard Header]*       -- binary: shard ID, key range
  [Document Batch]*     -- binary: doc key + JSON value batches (~4MB each)
  [Embedding Batch]*    -- binary: packed LE float32 vectors per index
  [Sparse Batch]*       -- binary: sparse vectors (indices + values) per index
  [Summary Batch]*      -- binary: summary text per index
  [Chunk Batch]*        -- binary: chunk artifacts per index
  [Edge Batch]*         -- binary: outgoing graph edges per index
  [Transaction Batch]*  -- binary: in-flight txn records (optional)
  [Shard Footer]*       -- counts for verification
[File Footer]           -- total counts, file size
```

### Block Envelope (every block after file header)

```
[block_type: u8] [flags: u8] [payload_length: u32 LE] [payload: N bytes] [crc32: u32]
```

- `flags` bit 0: zstd-compressed payload
- CRC32 covers `[block_type..payload]` (everything except the CRC itself)
- CRC32 chosen because both Go (`hash/crc32`) and Zig (`std.hash.Crc32`) have it in stdlib

### File Header (64 bytes)

```
[magic: "ANTFLYB\n" 8 bytes] [format_version: u32 LE = 1] [flags: u32 LE]
[created_at_ns: i64 LE] [backup_id: 16 bytes UUID] [table_count: u32 LE]
[shard_count: u32 LE] [header_crc: u32] [reserved: 12 bytes]
```

### Block Types

| Byte | Name | Payload |
|------|------|---------|
| 0x01 | CLUSTER_MANIFEST | JSON (backup ID, timestamp, source backend, table list) |
| 0x02 | TABLE_MANIFEST | JSON (schema, indexes, shard layout -- reuses existing OpenAPI types) |
| 0x03 | SHARD_HEADER | Binary (table name, shard ID, key range) |
| 0x10 | DOCUMENT_BATCH | Binary (count + entries: key + value_flags + value + timestamp) |
| 0x11 | EMBEDDING_BATCH | Binary (index name + dimension + entries: doc_key + hash_id + float32[]) |
| 0x12 | SPARSE_BATCH | Binary (index name + entries: doc_key + hash_id + nnz + u32[] + f32[]) |
| 0x13 | SUMMARY_BATCH | Binary (index name + entries: doc_key + hash_id + text) |
| 0x14 | CHUNK_BATCH | Binary (index name + entries: doc_key + chunk_type + chunk_id + payload) |
| 0x15 | EDGE_BATCH | Binary (index name + entries: source + target + edge_type + value) |
| 0x16 | TRANSACTION_BATCH | Binary (entries: txn_id + status + intents) |
| 0xF0 | SHARD_FOOTER | Binary (shard ID + counts) |
| 0xFF | FILE_FOOTER | Binary (total counts + file size) |

### Key Design Decisions

- **User-facing keys only**: Document keys in the format are raw user IDs (no `:\x00` Pebble suffix, no `0x01..0x00 0x00 0x10` Zig encoding). Each backend re-encodes on restore.
- **LE float32 for vectors**: Explicit little-endian, not platform-native SIMD encoding. Both backends can trivially read/write.
- **Document values as-is**: Preserve zstd-compressed or plain JSON as stored. A `value_flags` byte indicates compression state to avoid double-compression at block level.
- **~4MB batch target**: Large enough for efficient I/O, small enough for bounded memory.
- **Outgoing edges only**: Reverse (incoming) edges are derived data, rebuilt on restore.

## Implementation Plan

### Phase 1: Format Codec

**Go** -- new file `src/common/backup_codec.go`:
- Block writer: streaming encoder with per-block CRC32 and optional zstd
- Block reader: streaming decoder with CRC32 validation, block-type dispatch
- Types: block type constants, header struct, batch record structs

**Zig** -- new file `src/storage/backup_codec.zig`:
- Block writer: streaming encoder
- Block reader: streaming decoder
- Types: block type constants and structs

Both are pure sequential I/O with no external deps beyond CRC32 and optional zstd.

### Phase 2: Go Export/Import

**Export** -- new method on `DB` interface (`src/store/db/db.go:266`):
```go
ExportPortable(ctx context.Context, w io.Writer) error
```

Implementation in `DBImpl`:
1. Iterate Pebble using existing `ExportRangeChunk` pattern (`db.go:4986`) -- scan with range bounds
2. Classify each key using `storeutils` helpers: `MakeEmbeddingKey`, `MakeSparseKey`, `MakeSummaryKey`, `IsEdgeKey`, `MakeChunkKey`
3. Strip backend-specific key encoding, emit portable records via `backup_codec.go` writer
4. Batch into ~4MB blocks

**Import** -- corresponding `ImportPortable(ctx context.Context, r io.Reader) error`:
1. Read blocks via `backup_codec.go` reader, re-encode keys with Pebble suffixes
2. Write via Pebble batch operations
3. Queue index rebuilds through existing enrichment pipeline

**Critical Go files**:
- `src/common/backup_codec.go` -- New: AFB format codec (reader/writer/types)
- `src/store/db/db.go:266` -- DB interface, add ExportPortable/ImportPortable
- `src/store/storeutils/storeutils.go` -- Key helpers for classification
- `src/metadata/api_backup.go` -- API handler, add format routing
- `src/common/archive.go` -- Reuse magic-byte detection pattern for format auto-detect

### Phase 3: Zig Export/Import

**Export** -- new functions in `src/storage/backup_codec.zig` plus integration in `core.zig`:
```zig
pub fn exportPortable(alloc: Allocator, store: *DocStore, index_manager: *IndexManager, writer: anytype) !void
```

Implementation:
1. Use `store.scanRange(alloc, "", "")` (same scan as `writeStoreSnapshot` in `core.zig:1088`)
2. Classify keys using `internal_keys.isPrimaryDocumentKey`, artifact kind bytes
3. Decode internal keys via `decodePrimaryDocumentKeyAlloc` to user-visible keys
4. Emit portable records via `backup_codec.zig` writer

**Import** -- `importPortable` in the same module:
1. Read blocks via `backup_codec.zig` reader, encode keys via `internal_keys.documentKeyAlloc` and `artifactNamedPrefixAlloc`
2. Write via `store.putBatch()`
3. Rebuild derived indexes (graph reverse edges, text indexes, etc.)

**Critical Zig files**:
- `src/storage/backup_codec.zig` -- New: AFB format codec (reader/writer/types)
- `src/storage/db/core.zig` -- Parallels existing `writeSnapshot`/`importStoreSnapshot`
- `src/storage/internal_keys.zig` -- Key encoding/decoding for translation
- `src/api/backups.zig` -- Backup manifest types, BackupLocation abstraction
- `src/api/table_writes.zig` -- Where backup/restore calls DB snapshot

### Phase 4: API Integration

Add `format` field to backup endpoints in `openapi.yaml`:

```yaml
format:
  type: string
  enum: [native, portable]
  default: native
```

- `native`: existing physical snapshot (fast, same-backend)
- `portable`: new AFB format (cross-backend compatible)

Restore auto-detects format by reading magic bytes (`ANTFLYB\n` vs tar.zst magic `0x28B52FFD`).

Both backends add format parameter handling in their backup API handlers.

### Phase 5: Cross-Backend E2E Tests

1. **Golden file tests**: Go writes AFB, commit as test fixture; Zig reads and verifies (and vice versa)
2. **Python E2E** (extending `e2e/test_backup_restore.py`):
   - Go instance: create table with docs + embeddings + graph edges, backup with `format: portable`
   - Zig instance: restore from same location, verify document lookups + search + graph queries
   - Reverse: Zig backup -> Go restore
3. **Test matrix**: plain docs, dense embeddings, sparse embeddings, full-text chunks, graph edges, multi-shard tables

## Verification

- Run Go unit tests: `GOEXPERIMENT=simd go test ./src/common/ -run TestBackupCodec`
- Run Zig unit tests: `zig build unit-test` (backup_codec module tests)
- Run E2E: `make e2e E2E_TEST=TestPortableBackup`
- Cross-backend E2E: Python test that starts both Go and Zig instances

## Size Estimate (1M docs, 384-dim embeddings)

- Documents: ~1 GB
- Embeddings: ~1.5 GB (1M x 1.5KB per vector)
- With per-block zstd: ~1.5-2 GB total (documents compress well, vectors ~10-20%)
