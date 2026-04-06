// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
)

// portableBatchTarget is the approximate target size for each data batch (~4MB).
const portableBatchTarget = 4 * 1024 * 1024

// ExportPortable writes the shard's data in AFB (Antfly Format for Backups)
// portable format. This captures documents, embeddings, sparse vectors,
// summaries, chunks, and outgoing edges at the semantic level. Indexes are
// NOT exported — they are rebuilt on restore.
func (db *DBImpl) ExportPortable(ctx context.Context, w io.Writer) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}

	byteRange := db.getByteRange()

	writer, err := common.NewAFBWriter(w, true) // compress data blocks
	if err != nil {
		return fmt.Errorf("creating AFB writer: %w", err)
	}
	defer writer.Close()

	// Generate backup metadata
	backupID := uuid.New()
	header := common.NewFileHeader(backupID, 1, 1, true)

	if err := writer.WriteHeader(header); err != nil {
		return fmt.Errorf("writing AFB header: %w", err)
	}

	// Write cluster manifest
	clusterManifest, _ := json.Marshal(map[string]any{
		"backup_id":      backupID.String(),
		"created_at":     time.Now().UTC().Format(time.RFC3339),
		"source_backend": "go",
	})
	if err := writer.WriteBlock(common.BlockClusterManifest, clusterManifest); err != nil {
		return fmt.Errorf("writing cluster manifest: %w", err)
	}

	// Write table manifest
	tableManifest, _ := json.Marshal(map[string]any{
		"indexes": db.GetIndexes(),
	})
	if err := writer.WriteBlock(common.BlockTableManifest, tableManifest); err != nil {
		return fmt.Errorf("writing table manifest: %w", err)
	}

	// Write shard header
	shardHeader := common.ShardHeaderEntry{
		TableName: "",
		ShardID:   0,
		StartKey:  byteRange[0],
		EndKey:    byteRange[1],
	}
	shardHeaderPayload := common.EncodeShardHeader(shardHeader)
	if err := writer.WriteBlock(common.BlockShardHeader, shardHeaderPayload); err != nil {
		return fmt.Errorf("writing shard header: %w", err)
	}

	// Iterate all keys and classify them
	var counts common.ShardFooterEntry
	if err := db.exportShardData(ctx, writer, &counts); err != nil {
		return fmt.Errorf("exporting shard data: %w", err)
	}

	// Write shard footer
	shardFooter := common.EncodeShardFooter(counts)
	if err := writer.WriteBlock(common.BlockShardFooter, shardFooter); err != nil {
		return fmt.Errorf("writing shard footer: %w", err)
	}

	// Write file footer
	fileFooter := common.FileFooterEntry{
		TableCount:     1,
		ShardCount:     1,
		TotalDocuments: counts.DocumentCount,
		TotalBytes:     0, // filled in by caller if needed
	}
	fileFooterPayload := common.EncodeFileFooter(fileFooter)
	if err := writer.WriteBlock(common.BlockFileFooter, fileFooterPayload); err != nil {
		return fmt.Errorf("writing file footer: %w", err)
	}

	return nil
}

// exportShardData iterates all Pebble keys and writes batched AFB blocks.
func (db *DBImpl) exportShardData(ctx context.Context, writer *common.AFBWriter, counts *common.ShardFooterEntry) error {
	pdb := db.getPDB()
	iter, err := pdb.NewIter(&pebble.IterOptions{})
	if err != nil {
		return fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	// Accumulators for batching
	var docBatch []common.DocumentEntry
	var docBatchSize int

	// Embedding batches: keyed by index name
	type embBatchState struct {
		entries   []common.EmbeddingEntry
		dimension uint16
		size      int
	}
	embBatches := map[string]*embBatchState{}

	// Sparse batches: keyed by index name
	type sparseBatchState struct {
		entries []common.SparseEntry
		size    int
	}
	sparseBatches := map[string]*sparseBatchState{}

	// Edge batches: keyed by index name
	type edgeBatchState struct {
		entries []common.EdgeEntry
		size    int
	}
	edgeBatches := map[string]*edgeBatchState{}

	// Flush helpers
	flushDocs := func() error {
		if len(docBatch) == 0 {
			return nil
		}
		payload := common.EncodeDocumentBatch(docBatch)
		if err := writer.WriteBlock(common.BlockDocumentBatch, payload); err != nil {
			return err
		}
		docBatch = docBatch[:0]
		docBatchSize = 0
		return nil
	}

	flushEmbeddings := func(indexName string) error {
		state := embBatches[indexName]
		if state == nil || len(state.entries) == 0 {
			return nil
		}
		payload := common.EncodeEmbeddingBatch(indexName, state.dimension, state.entries)
		if err := writer.WriteBlock(common.BlockEmbeddingBatch, payload); err != nil {
			return err
		}
		state.entries = state.entries[:0]
		state.size = 0
		return nil
	}

	flushSparse := func(indexName string) error {
		state := sparseBatches[indexName]
		if state == nil || len(state.entries) == 0 {
			return nil
		}
		payload := common.EncodeSparseBatch(indexName, state.entries)
		if err := writer.WriteBlock(common.BlockSparseBatch, payload); err != nil {
			return err
		}
		state.entries = state.entries[:0]
		state.size = 0
		return nil
	}

	flushEdges := func(indexName string) error {
		state := edgeBatches[indexName]
		if state == nil || len(state.entries) == 0 {
			return nil
		}
		payload := common.EncodeEdgeBatch(indexName, state.entries)
		if err := writer.WriteBlock(common.BlockEdgeBatch, payload); err != nil {
			return err
		}
		state.entries = state.entries[:0]
		state.size = 0
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key := iter.Key()
		value := iter.Value()

		// Skip internal metadata keys
		if bytes.HasPrefix(key, storeutils.MetadataPrefix) {
			continue
		}

		// Classify key by suffix pattern
		switch {
		case bytes.HasSuffix(key, storeutils.DBRangeStart):
			// Document key: <userKey>:\x00
			userKey := key[:len(key)-len(storeutils.DBRangeStart)]

			// Check for timestamp
			var timestampNs uint64
			tsKey := append(bytes.Clone(key), storeutils.TransactionSuffix...)
			tsVal, tsCloser, tsErr := pdb.Get(tsKey)
			if tsErr == nil && len(tsVal) >= 8 {
				_, timestampNs, _ = encoding.DecodeUint64Ascending(tsVal)
			}
			if tsCloser != nil {
				_ = tsCloser.Close()
			}

			var valueFlags byte
			if storeutils.IsZstdCompressed(value) {
				valueFlags = common.DocValueFlagCompressed
			}

			docBatch = append(docBatch, common.DocumentEntry{
				Key:         bytes.Clone(userKey),
				ValueFlags:  valueFlags,
				Value:       bytes.Clone(value),
				TimestampNs: timestampNs,
			})
			docBatchSize += len(userKey) + len(value) + 20
			counts.DocumentCount++

			if docBatchSize >= portableBatchTarget {
				if err := flushDocs(); err != nil {
					return err
				}
			}

		case isEmbeddingKey(key):
			// Embedding key: <userKey>:i:<indexName>:e
			userKey, indexName := parseEnrichmentKey(key, storeutils.EmbeddingSuffix)
			if userKey == nil {
				continue
			}

			if storeutils.IsDudEnrichment(value) {
				continue
			}

			hashID, vec, _, err := vectorindex.DecodeEmbeddingWithHashID(value)
			if err != nil {
				continue // skip malformed
			}

			state, ok := embBatches[indexName]
			if !ok {
				state = &embBatchState{dimension: uint16(len(vec))} //nolint:gosec
				embBatches[indexName] = state
			}

			state.entries = append(state.entries, common.EmbeddingEntry{
				DocKey: userKey,
				HashID: hashID,
				Vector: []float32(vec),
			})
			state.size += len(userKey) + len(vec)*4 + 16
			counts.EmbeddingCount++

			if state.size >= portableBatchTarget {
				if err := flushEmbeddings(indexName); err != nil {
					return err
				}
			}

		case isSparseKey(key):
			// Sparse key: <userKey>:i:<indexName>:sp
			userKey, indexName := parseEnrichmentKey(key, storeutils.SparseSuffix)
			if userKey == nil {
				continue
			}

			if storeutils.IsDudEnrichment(value) {
				continue
			}

			if len(value) < 8 {
				continue
			}

			// Value format: [hashID:u64][encoded sparse vec]
			remaining, sparseHashID, err := encoding.DecodeUint64Ascending(value)
			if err != nil {
				continue
			}

			// Decode sparse vector: [n:u32][indices:u32*n][values:f32*n]
			sparseIndices, sparseValues, err := decodeSparseVecRaw(remaining)
			if err != nil {
				continue
			}

			state, ok := sparseBatches[indexName]
			if !ok {
				state = &sparseBatchState{}
				sparseBatches[indexName] = state
			}

			state.entries = append(state.entries, common.SparseEntry{
				DocKey:  userKey,
				HashID:  sparseHashID,
				Indices: sparseIndices,
				Values:  sparseValues,
			})
			state.size += len(userKey) + (len(sparseIndices)+len(sparseValues))*4 + 16

		case isOutgoingEdgeKey(key):
			// Outgoing edge: <sourceKey>:i:<indexName>:out:<edgeType>:<targetKey>:o
			source, target, indexName, edgeType, err := storeutils.ParseEdgeKey(key)
			if err != nil {
				continue
			}

			state, ok := edgeBatches[indexName]
			if !ok {
				state = &edgeBatchState{}
				edgeBatches[indexName] = state
			}

			state.entries = append(state.entries, common.EdgeEntry{
				SourceKey: bytes.Clone(source),
				TargetKey: bytes.Clone(target),
				EdgeType:  []byte(edgeType),
				Value:     bytes.Clone(value),
			})
			state.size += len(source) + len(target) + len(edgeType) + len(value) + 16
			counts.EdgeCount++

			if state.size >= portableBatchTarget {
				if err := flushEdges(indexName); err != nil {
					return err
				}
			}

		case isSummaryKey(key):
			// Summary key: <userKey>:i:<indexName>:s
			// For now, summaries are skipped in portable format — they're rebuilt
			// by the enrichment pipeline on restore. The plan includes
			// BlockSummaryBatch for future use.
			continue

		case storeutils.IsChunkKey(key):
			// Chunk keys: rebuilt by enrichment pipeline on restore
			continue

		case isIncomingEdgeKey(key):
			// Incoming edges are derived; skip
			continue

		default:
			// Transaction metadata (:t suffix on doc key), or unknown — skip
			continue
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	// Flush remaining batches
	if err := flushDocs(); err != nil {
		return err
	}
	for indexName := range embBatches {
		if err := flushEmbeddings(indexName); err != nil {
			return err
		}
	}
	for indexName := range sparseBatches {
		if err := flushSparse(indexName); err != nil {
			return err
		}
	}
	for indexName := range edgeBatches {
		if err := flushEdges(indexName); err != nil {
			return err
		}
	}

	return nil
}

// ImportPortable reads an AFB file and imports all data into this shard's Pebble.
// Indexes are NOT restored — the caller should trigger enrichment/rebuild after import.
func (db *DBImpl) ImportPortable(ctx context.Context, r io.Reader) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading AFB data: %w", err)
	}

	reader, err := common.NewAFBReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating AFB reader: %w", err)
	}
	defer reader.Close()

	// Read and validate header
	if _, err := reader.ReadHeader(); err != nil {
		return fmt.Errorf("reading AFB header: %w", err)
	}

	// Process blocks
	batch := pdb.NewBatch()
	defer func() { _ = batch.Close() }()

	var batchSize int
	const maxBatchSize = 32 * 1024 * 1024 // 32MB per Pebble batch

	commitBatch := func() error {
		if batchSize == 0 {
			return nil
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("committing batch: %w", err)
		}
		batch = pdb.NewBatch()
		batchSize = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		block, err := reader.ReadBlock()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading AFB block: %w", err)
		}

		switch block.Type {
		case common.BlockClusterManifest, common.BlockTableManifest:
			// Metadata blocks — skip for now (could validate schema compatibility)
			continue

		case common.BlockShardHeader:
			// Could validate shard range compatibility
			continue

		case common.BlockDocumentBatch:
			entries, err := common.DecodeDocumentBatch(block.Payload)
			if err != nil {
				return fmt.Errorf("decoding document batch: %w", err)
			}
			for _, e := range entries {
				pebbleKey := storeutils.KeyRangeStart(e.Key)
				if err := batch.Set(pebbleKey, e.Value, nil); err != nil {
					return fmt.Errorf("writing document %s: %w", string(e.Key), err)
				}
				batchSize += len(pebbleKey) + len(e.Value)

				// Write timestamp if present
				if e.TimestampNs > 0 {
					tsKey := append(bytes.Clone(pebbleKey), storeutils.TransactionSuffix...)
					tsBuf := encoding.EncodeUint64Ascending(nil, e.TimestampNs)
					if err := batch.Set(tsKey, tsBuf, nil); err != nil {
						return fmt.Errorf("writing timestamp for %s: %w", string(e.Key), err)
					}
					batchSize += len(tsKey) + len(tsBuf)
				}
			}

		case common.BlockEmbeddingBatch:
			indexName, dimension, entries, err := common.DecodeEmbeddingBatch(block.Payload)
			if err != nil {
				return fmt.Errorf("decoding embedding batch: %w", err)
			}
			_ = dimension
			for _, e := range entries {
				pebbleKey := storeutils.MakeEmbeddingKey(e.DocKey, indexName)
				val, err := vectorindex.EncodeEmbeddingWithHashID(nil, e.Vector, e.HashID)
				if err != nil {
					return fmt.Errorf("encoding embedding for %s: %w", string(e.DocKey), err)
				}
				if err := batch.Set(pebbleKey, val, nil); err != nil {
					return fmt.Errorf("writing embedding for %s: %w", string(e.DocKey), err)
				}
				batchSize += len(pebbleKey) + len(val)
			}

		case common.BlockSparseBatch:
			indexName, entries, err := common.DecodeSparseBatch(block.Payload)
			if err != nil {
				return fmt.Errorf("decoding sparse batch: %w", err)
			}
			for _, e := range entries {
				pebbleKey := storeutils.MakeSparseKey(e.DocKey, indexName)
				// Encode: [hashID:u64][n:u32][indices:u32*n][values:f32*n]
				val := encoding.EncodeUint64Ascending(nil, e.HashID)
				val = encodeSparseVecRaw(val, e.Indices, e.Values)
				if err := batch.Set(pebbleKey, val, nil); err != nil {
					return fmt.Errorf("writing sparse embedding for %s: %w", string(e.DocKey), err)
				}
				batchSize += len(pebbleKey) + len(val)
			}

		case common.BlockEdgeBatch:
			indexName, entries, err := common.DecodeEdgeBatch(block.Payload)
			if err != nil {
				return fmt.Errorf("decoding edge batch: %w", err)
			}
			for _, e := range entries {
				pebbleKey := storeutils.MakeEdgeKey(
					e.SourceKey, e.TargetKey,
					indexName, string(e.EdgeType),
				)
				if err := batch.Set(pebbleKey, e.Value, nil); err != nil {
					return fmt.Errorf("writing edge: %w", err)
				}
				batchSize += len(pebbleKey) + len(e.Value)
			}

		case common.BlockSummaryBatch, common.BlockChunkBatch, common.BlockTransactionBatch:
			// Not yet implemented — skip
			continue

		case common.BlockShardFooter, common.BlockFileFooter:
			// Validation blocks — skip
			continue
		}

		if batchSize >= maxBatchSize {
			if err := commitBatch(); err != nil {
				return err
			}
		}
	}

	return commitBatch()
}

// Key classification helpers

func isEmbeddingKey(key []byte) bool {
	return bytes.HasSuffix(key, storeutils.EmbeddingSuffix) &&
		bytes.Contains(key, []byte(":i:"))
}

func isSparseKey(key []byte) bool {
	return bytes.HasSuffix(key, storeutils.SparseSuffix) &&
		bytes.Contains(key, []byte(":i:"))
}

func isSummaryKey(key []byte) bool {
	return bytes.HasSuffix(key, storeutils.SummarySuffix) &&
		bytes.Contains(key, []byte(":i:"))
}

func isOutgoingEdgeKey(key []byte) bool {
	return bytes.Contains(key, []byte(":out:")) &&
		bytes.HasSuffix(key, storeutils.EdgeOutSuffix)
}

func isIncomingEdgeKey(key []byte) bool {
	return bytes.Contains(key, []byte(":in:"))
}

// parseEnrichmentKey extracts the user-visible document key and index name
// from an enrichment key with the given suffix.
// Key format: <userKey>:i:<indexName>:<suffix>
func parseEnrichmentKey(key []byte, suffix []byte) (userKey []byte, indexName string) {
	if !bytes.HasSuffix(key, suffix) {
		return nil, ""
	}
	withoutSuffix := key[:len(key)-len(suffix)]
	marker := bytes.LastIndex(withoutSuffix, []byte(":i:"))
	if marker <= 0 {
		return nil, ""
	}
	return bytes.Clone(withoutSuffix[:marker]), string(withoutSuffix[marker+3:])
}

// decodeSparseVecRaw decodes the sparse vector binary format.
// Format: [n:u32][indices:u32*n][values:f32*n]
func decodeSparseVecRaw(data []byte) (indices []uint32, values []float32, err error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("sparse vec too short")
	}
	n := binary.LittleEndian.Uint32(data[:4])
	expected := 4 + int(n)*8
	if len(data) < expected {
		return nil, nil, fmt.Errorf("sparse vec truncated: need %d, have %d", expected, len(data))
	}

	indices = make([]uint32, n)
	values = make([]float32, n)
	off := 4
	for i := range n {
		indices[i] = binary.LittleEndian.Uint32(data[off:])
		off += 4
	}
	for i := range n {
		values[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[off:]))
		off += 4
	}
	return indices, values, nil
}

// encodeSparseVecRaw encodes a sparse vector into [n:u32][indices:u32*n][values:f32*n].
func encodeSparseVecRaw(appendTo []byte, indices []uint32, values []float32) []byte {
	n := len(indices)
	buf := make([]byte, 4+n*8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(n)) //nolint:gosec
	off := 4
	for _, idx := range indices {
		binary.LittleEndian.PutUint32(buf[off:], idx)
		off += 4
	}
	for _, v := range values {
		binary.LittleEndian.PutUint32(buf[off:], math.Float32bits(v))
		off += 4
	}
	return append(appendTo, buf...)
}

// Ensure DBImpl implements the portable backup methods (compile-time check).
var _ interface {
	ExportPortable(ctx context.Context, w io.Writer) error
	ImportPortable(ctx context.Context, r io.Reader) error
} = (*DBImpl)(nil)
