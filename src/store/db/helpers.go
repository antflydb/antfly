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
	"fmt"
	"time"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// compressAndSet compresses value with zstd using the buffer pool and writes it to the batch.
// The encoder is reset and reused across calls within the same Batch() invocation.
// Returns an error if compression or batch write fails.
func (db *DBImpl) compressAndSet(batch *pebble.Batch, encoder *zstd.Encoder, key, value []byte) error {
	compressedVal := db.bufferPool.Get().(*bytes.Buffer)
	compressedVal.Reset()
	encoder.Reset(compressedVal)
	if _, err := encoder.Write(value); err != nil {
		db.bufferPool.Put(compressedVal)
		return fmt.Errorf("compressing value: %w", err)
	}
	if err := encoder.Close(); err != nil {
		db.bufferPool.Put(compressedVal)
		return fmt.Errorf("closing zstd encoder: %w", err)
	}
	if err := batch.Set(key, compressedVal.Bytes(), nil); err != nil {
		db.bufferPool.Put(compressedVal)
		return fmt.Errorf("writing key: %w", err)
	}
	db.bufferPool.Put(compressedVal)
	return nil
}

// specialFieldBatchResult holds the results of extracting and writing special fields to a batch.
type specialFieldBatchResult struct {
	// IndexWrites are the writes that should be forwarded to the IndexManager.
	IndexWrites [][2][]byte
	// EdgeDeletes are edge keys that should be deleted from the IndexManager.
	EdgeDeletes [][]byte
	// NumWrites is the number of individual writes added to the batch.
	NumWrites int
	// CleanedJSON is the document JSON with special fields removed.
	CleanedJSON []byte
	// HasNonSpecialFields is true if the document has fields other than _embeddings, _summaries, _edges.
	HasNonSpecialFields bool
}

// extractAndWriteSpecialFields checks a document for special fields (_embeddings, _summaries, _edges),
// extracts them, writes the extracted data to the batch, and returns the results.
// If the document has no special fields, it returns nil (no work done).
// The timestamp parameter is used for edge timestamp writes; pass 0 to skip timestamps.
func (db *DBImpl) extractAndWriteSpecialFields(
	batch *pebble.Batch,
	docJSON []byte,
	docKey []byte,
	timestamp uint64,
) (*specialFieldBatchResult, error) {
	// Fast-path check for special fields
	hasSpecialFields := bytes.Contains(docJSON, []byte(`"_embeddings"`)) ||
		bytes.Contains(docJSON, []byte(`"_summaries"`)) ||
		bytes.Contains(docJSON, []byte(`"_edges"`))

	if !hasSpecialFields {
		return nil, nil
	}

	embWrites, sumWrites, edgeWrites, edgeDeletes, cleanedJSON, hasNonSpecialFields, err := db.extractSpecialFields(docJSON, docKey)
	if err != nil {
		return nil, err
	}

	result := &specialFieldBatchResult{
		IndexWrites:         make([][2][]byte, 0, len(embWrites)+len(sumWrites)+len(edgeWrites)),
		EdgeDeletes:         make([][]byte, 0, len(edgeDeletes)),
		CleanedJSON:         cleanedJSON,
		HasNonSpecialFields: hasNonSpecialFields,
	}

	// Write extracted embeddings
	for _, w := range embWrites {
		if err := batch.Set(w[0], w[1], nil); err != nil {
			db.logger.Error("could not set embedding for write",
				zap.String("key", types.FormatKey(w[0])),
				zap.Error(err))
			continue
		}
		result.NumWrites++
		result.IndexWrites = append(result.IndexWrites, w)
	}

	// Write extracted summaries
	for _, w := range sumWrites {
		if err := batch.Set(w[0], w[1], nil); err != nil {
			db.logger.Error("could not set summary for write",
				zap.String("key", types.FormatKey(w[0])),
				zap.Error(err))
			continue
		}
		result.NumWrites++
		result.IndexWrites = append(result.IndexWrites, w)
	}

	// Write extracted edges
	for _, w := range edgeWrites {
		if err := batch.Set(w[0], w[1], nil); err != nil {
			db.logger.Error("could not set edge for write",
				zap.String("key", types.FormatKey(w[0])),
				zap.Error(err))
			continue
		}
		result.NumWrites++

		// Write timestamp for edge if timestamp is available
		if timestamp > 0 && shouldWriteTimestamp(w[0]) {
			txnKey := append(bytes.Clone(w[0]), storeutils.TransactionSuffix...)
			timestampBytes := encoding.EncodeUint64Ascending(nil, timestamp)
			if err := batch.Set(txnKey, timestampBytes, nil); err != nil {
				db.logger.Warn("could not write timestamp for edge",
					zap.ByteString("edgeKey", w[0]),
					zap.Error(err))
			}
		}

		result.IndexWrites = append(result.IndexWrites, w)
	}

	// Delete edges that aren't in _edges (reconciliation)
	for _, d := range edgeDeletes {
		if err := batch.Delete(d, nil); err != nil {
			db.logger.Error("could not delete edge",
				zap.ByteString("edgeKey", d),
				zap.Error(err))
			continue
		}
		result.EdgeDeletes = append(result.EdgeDeletes, d)
	}

	return result, nil
}

// finalizeTransaction updates a transaction record to the given status (1=Committed, 2=Aborted).
func (db *DBImpl) finalizeTransaction(txnID []byte, status int32, label string) error {
	start := time.Now()
	defer func() {
		transactionDurationSeconds.WithLabelValues(label).Observe(time.Since(start).Seconds())
	}()
	defer activeTransactionsGauge.Dec()

	txnKey := makeTxnKey(txnID)

	// Load existing record
	data, closer, err := db.pdb.Get(txnKey)
	if err != nil {
		transactionOpsTotal.WithLabelValues(label, "error").Inc()
		return fmt.Errorf("loading transaction record: %w", err)
	}
	defer func() { _ = closer.Close() }()

	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		transactionOpsTotal.WithLabelValues(label, "error").Inc()
		return fmt.Errorf("unmarshaling transaction record: %w", err)
	}

	// Update status
	record["status"] = status
	record["committed_at"] = time.Now().Unix()

	// Save back
	updatedData, err := json.Marshal(record)
	if err != nil {
		transactionOpsTotal.WithLabelValues(label, "error").Inc()
		return fmt.Errorf("marshaling transaction record: %w", err)
	}

	if err := db.pdb.Set(txnKey, updatedData, pebble.Sync); err != nil {
		transactionOpsTotal.WithLabelValues(label, "error").Inc()
		return fmt.Errorf("writing %s transaction record: %w", label, err)
	}

	transactionOpsTotal.WithLabelValues(label, "success").Inc()

	db.logger.Info("Finalized transaction",
		zap.Binary("txnID", txnID),
		zap.String("status", label))

	return nil
}
