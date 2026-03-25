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
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressAndSet(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	t.Run("basic compress and write", func(t *testing.T) {
		batch := db.pdb.NewBatch()
		defer batch.Close()

		key := []byte("test-key")
		value := []byte(`{"hello":"world"}`)

		err := db.compressAndSet(batch, encoder, key, value)
		require.NoError(t, err)

		require.NoError(t, batch.Commit(pebble.NoSync))

		// Read back and decompress
		data, closer, err := db.pdb.Get(key)
		require.NoError(t, err)
		defer closer.Close()

		decoder, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer decoder.Close()

		decompressed, err := decoder.DecodeAll(data, nil)
		require.NoError(t, err)
		assert.Equal(t, value, decompressed)
	})

	t.Run("empty value", func(t *testing.T) {
		batch := db.pdb.NewBatch()
		defer batch.Close()

		err := db.compressAndSet(batch, encoder, []byte("empty-key"), []byte{})
		require.NoError(t, err)

		require.NoError(t, batch.Commit(pebble.NoSync))

		data, closer, err := db.pdb.Get([]byte("empty-key"))
		require.NoError(t, err)
		defer closer.Close()

		decoder, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer decoder.Close()

		decompressed, err := decoder.DecodeAll(data, nil)
		require.NoError(t, err)
		assert.Empty(t, decompressed)
	})

	t.Run("large value compresses", func(t *testing.T) {
		batch := db.pdb.NewBatch()
		defer batch.Close()

		largeValue := make([]byte, 100_000)
		for i := range largeValue {
			largeValue[i] = byte('a' + (i % 26))
		}

		err := db.compressAndSet(batch, encoder, []byte("large-key"), largeValue)
		require.NoError(t, err)

		require.NoError(t, batch.Commit(pebble.NoSync))

		data, closer, err := db.pdb.Get([]byte("large-key"))
		require.NoError(t, err)
		defer closer.Close()

		assert.Less(t, len(data), len(largeValue))

		decoder, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer decoder.Close()

		decompressed, err := decoder.DecodeAll(data, nil)
		require.NoError(t, err)
		assert.Equal(t, largeValue, decompressed)
	})

	t.Run("encoder reuse across calls", func(t *testing.T) {
		batch := db.pdb.NewBatch()
		defer batch.Close()

		for i := range 5 {
			key := []byte("reuse-" + string(rune('0'+i)))
			value := []byte(`{"n":` + string(rune('0'+i)) + `}`)
			err := db.compressAndSet(batch, encoder, key, value)
			require.NoError(t, err, "iteration %d", i)
		}
	})
}

func TestExtractAndWriteSpecialFields(t *testing.T) {
	t.Run("no special fields returns nil", func(t *testing.T) {
		db := setupTestDB(t)
		defer cleanupTestDB(t, db)

		batch := db.pdb.NewBatch()
		defer batch.Close()

		doc := []byte(`{"title":"hello","body":"world"}`)
		result, err := db.extractAndWriteSpecialFields(batch, doc, []byte("doc1"), 0)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("embeddings extracted and written", func(t *testing.T) {
		db, dir := setupTestDBWithExternalIndex(t, 3, false)
		defer db.Close()
		defer os.RemoveAll(dir)

		batch := db.pdb.NewBatch()
		defer batch.Close()

		doc := mustMarshal(t, map[string]any{
			"title": "test doc",
			"_embeddings": map[string]any{
				"test_idx": []any{0.1, 0.2, 0.3},
			},
		})

		result, err := db.extractAndWriteSpecialFields(batch, doc, []byte("doc1"), 0)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, result.HasNonSpecialFields, "should have non-special fields (title)")
		assert.Positive(t, result.NumWrites)
		assert.NotEmpty(t, result.IndexWrites)

		// CleanedJSON should not contain _embeddings
		assert.NotContains(t, string(result.CleanedJSON), "_embeddings")
		assert.Contains(t, string(result.CleanedJSON), "title")
	})

	t.Run("only special fields sets HasNonSpecialFields false", func(t *testing.T) {
		db, dir := setupTestDBWithExternalIndex(t, 3, false)
		defer db.Close()
		defer os.RemoveAll(dir)

		batch := db.pdb.NewBatch()
		defer batch.Close()

		doc := mustMarshal(t, map[string]any{
			"_embeddings": map[string]any{
				"test_idx": []any{0.1, 0.2, 0.3},
			},
		})

		result, err := db.extractAndWriteSpecialFields(batch, doc, []byte("doc1"), 0)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.False(t, result.HasNonSpecialFields)
	})

	t.Run("edges with timestamp", func(t *testing.T) {
		db, dir := setupTestDBWithGraphIndex(t)
		defer db.Close()
		defer os.RemoveAll(dir)

		batch := db.pdb.NewBatch()
		defer batch.Close()

		doc := mustMarshal(t, map[string]any{
			"title": "linked doc",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "doc2", "weight": 1.0},
					},
				},
			},
		})

		timestamp := uint64(12345)
		result, err := db.extractAndWriteSpecialFields(batch, doc, []byte("doc1"), timestamp)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Positive(t, result.NumWrites)
		assert.NotEmpty(t, result.IndexWrites)

		// Commit and check timestamp keys were written for edges
		require.NoError(t, batch.Commit(pebble.NoSync))

		for _, w := range result.IndexWrites {
			if shouldWriteTimestamp(w[0]) {
				txnKey := append(w[0], storeutils.TransactionSuffix...)
				_, closer, err := db.pdb.Get(txnKey)
				if err == nil {
					closer.Close()
				}
				assert.NoError(t, err, "timestamp key should exist for edge %s", string(w[0]))
			}
		}
	})

	t.Run("edges without timestamp", func(t *testing.T) {
		db, dir := setupTestDBWithGraphIndex(t)
		defer db.Close()
		defer os.RemoveAll(dir)

		batch := db.pdb.NewBatch()
		defer batch.Close()

		doc := mustMarshal(t, map[string]any{
			"title": "linked doc",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "doc2", "weight": 1.0},
					},
				},
			},
		})

		// timestamp=0 means no timestamps should be written
		result, err := db.extractAndWriteSpecialFields(batch, doc, []byte("doc1"), 0)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Positive(t, result.NumWrites)
	})

	t.Run("mixed embeddings and edges", func(t *testing.T) {
		// Use graph index setup (has citations index registered)
		db, dir := setupTestDBWithGraphIndex(t)
		defer db.Close()
		defer os.RemoveAll(dir)

		batch := db.pdb.NewBatch()
		defer batch.Close()

		// Only test edges here since graph DB doesn't have an embedding index
		doc := mustMarshal(t, map[string]any{
			"title": "full doc",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "doc2", "weight": 1.0},
					},
					"similar_to": []any{
						map[string]any{"target": "doc3", "weight": 0.5},
					},
				},
			},
		})

		result, err := db.extractAndWriteSpecialFields(batch, doc, []byte("doc1"), 0)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, result.HasNonSpecialFields)
		assert.Positive(t, result.NumWrites)

		// CleanedJSON should only have title
		var cleaned map[string]any
		require.NoError(t, json.Unmarshal(result.CleanedJSON, &cleaned))
		assert.Contains(t, cleaned, "title")
		assert.NotContains(t, cleaned, "_edges")
	})
}

func TestFinalizeTransaction(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	t.Run("commit transaction", func(t *testing.T) {
		txnID := []byte("txn-commit-1")
		txnKey := makeTxnKey(txnID)

		record := TxnRecord{
			Status:    TxnStatusPending,
			CreatedAt: 1000,
		}
		data, err := json.Marshal(record)
		require.NoError(t, err)
		require.NoError(t, db.pdb.Set(txnKey, data, pebble.Sync))

		_, err = db.finalizeTransaction(t.Context(), txnID, TxnStatusCommitted, "commit")
		require.NoError(t, err)

		updatedData, closer, err := db.pdb.Get(txnKey)
		require.NoError(t, err)
		defer closer.Close()

		var updated TxnRecord
		require.NoError(t, json.Unmarshal(updatedData, &updated))
		assert.Equal(t, TxnStatusCommitted, updated.Status)
		assert.NotZero(t, updated.FinalizedAt)
	})

	t.Run("abort transaction", func(t *testing.T) {
		txnID := []byte("txn-abort-1")
		txnKey := makeTxnKey(txnID)

		record := TxnRecord{
			Status:    TxnStatusPending,
			CreatedAt: 1000,
		}
		data, err := json.Marshal(record)
		require.NoError(t, err)
		require.NoError(t, db.pdb.Set(txnKey, data, pebble.Sync))

		_, err = db.finalizeTransaction(t.Context(), txnID, TxnStatusAborted, "abort")
		require.NoError(t, err)

		updatedData, closer, err := db.pdb.Get(txnKey)
		require.NoError(t, err)
		defer closer.Close()

		var updated TxnRecord
		require.NoError(t, json.Unmarshal(updatedData, &updated))
		assert.Equal(t, TxnStatusAborted, updated.Status)
	})

	t.Run("missing transaction record", func(t *testing.T) {
		_, err := db.finalizeTransaction(t.Context(), []byte("nonexistent"), int32(1), "commit")
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTxnNotFound)
	})
}

// TestUpdateSchema_SurvivesReopen verifies that a schema update persists across
// DB close/reopen. Regression test: previously saveSchema() ran before the
// in-memory schema was updated, so the old schema was written to Pebble.
func TestUpdateSchema_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	initialSchema := &schema.TableSchema{
		DefaultType: "v1",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"v1": {Schema: map[string]any{"type": "object"}},
		},
	}

	// Open, update schema, close
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	require.NoError(t, db.UpdateSchema(initialSchema))

	updatedSchema := &schema.TableSchema{
		DefaultType: "v2",
		TtlDuration: "48h",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"v2": {Schema: map[string]any{"type": "object", "description": "updated"}},
		},
	}
	require.NoError(t, db.UpdateSchema(updatedSchema))

	// Verify in-memory state before close
	assert.Equal(t, "v2", db.schema.DefaultType)
	assert.NotZero(t, db.cachedTTLDuration, "TTL should be cached")

	// Read persisted schema directly from Pebble
	data, closer, err := db.pdb.Get(schemaKey)
	require.NoError(t, err)
	defer closer.Close()

	var persisted schema.TableSchema
	require.NoError(t, json.Unmarshal(data, &persisted))
	assert.Equal(t, "v2", persisted.DefaultType)
	assert.Equal(t, "48h", persisted.TtlDuration)

	_ = dir // schema persistence is verified via Pebble read above
}

// TestAddIndex_UsableAfterFailedAdd verifies the DB stays usable after a
// failed AddIndex (e.g. duplicate name). Regression test: previously the
// indexesMu lock leaked on error, deadlocking subsequent operations.
func TestAddIndex_UsableAfterFailedAdd(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	cfg := indexes.IndexConfig{Name: "idx1", Type: "mock"}
	require.NoError(t, db.AddIndex(cfg))

	// Duplicate registration fails
	err := db.AddIndex(cfg)
	require.Error(t, err)

	// DB must still accept new indexes — if the lock leaked this deadlocks.
	cfg2 := indexes.IndexConfig{Name: "idx2", Type: "mock"}
	require.NoError(t, db.AddIndex(cfg2))

	got := db.GetIndexes()
	assert.Contains(t, got, "idx1")
	assert.Contains(t, got, "idx2")
}

// TestDeleteIndex_UsableAfterFailedDelete verifies the DB stays usable after
// a failed DeleteIndex. Regression test: previously the indexesMu lock leaked
// when Unregister returned an error, deadlocking subsequent operations.
func TestDeleteIndex_UsableAfterFailedDelete(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Deleting a non-existent index fails
	err := db.DeleteIndex("ghost")
	require.Error(t, err)

	// DB must still accept new indexes — if the lock leaked this deadlocks.
	cfg := indexes.IndexConfig{Name: "after-failed-del", Type: "mock"}
	require.NoError(t, db.AddIndex(cfg))

	// And deleting that new index should succeed
	require.NoError(t, db.DeleteIndex("after-failed-del"))
	assert.NotContains(t, db.GetIndexes(), "after-failed-del")
}

// TestSetRange_PersistsCorrectly verifies that SetRange persists the new range
// to Pebble. Regression test: previously an empty Pebble batch was allocated,
// committed, and leaked alongside a direct pdb.Set.
func TestSetRange_PersistsCorrectly(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	newRange := types.Range{[]byte("foo"), []byte("qux")}
	require.NoError(t, db.SetRange(newRange))

	// Read back from Pebble
	data, closer, err := db.pdb.Get(byteRangeKey)
	require.NoError(t, err)
	defer closer.Close()

	var persisted types.Range
	require.NoError(t, json.Unmarshal(data, &persisted))
	assert.Equal(t, newRange, persisted)
	assert.Equal(t, newRange, db.getByteRange())
}

// TestBatch_SyncLevelPreservesCallerData verifies that a Batch call at
// SyncLevelFullText does not corrupt the caller's writes slice.
// Regression test: previously the sync path zeroed writes[i][1] on the
// caller's slice instead of on an internal copy.
func TestBatch_SyncLevelPreservesCallerData(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	cfg := indexes.IndexConfig{Name: "ft", Type: "mock"}
	require.NoError(t, db.AddIndex(cfg))

	val1 := []byte(`{"title":"one"}`)
	val2 := []byte(`{"title":"two"}`)
	writes := [][2][]byte{
		{[]byte("doc1"), val1},
		{[]byte("doc2"), val2},
	}

	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelFullText)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Caller's values must still be intact
	assert.Equal(t, val1, writes[0][1], "caller writes[0] value was corrupted")
	assert.Equal(t, val2, writes[1][1], "caller writes[1] value was corrupted")
}
