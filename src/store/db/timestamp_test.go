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
	"context"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/encoding"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimestampWriting verifies that :t timestamp keys are written for documents and edges
func TestTimestampWriting(t *testing.T) {
	testDB := setupTestDB(t)
	t.Cleanup(func() { testDB.Close() })

	ctx := context.Background()
	timestamp := uint64(time.Now().UnixNano())

	// Add timestamp to context (simulating metadata layer behavior)
	ctx = storeutils.WithTimestamp(ctx, timestamp)

	t.Run("document timestamp is written", func(t *testing.T) {
		docKey := []byte("test_doc")
		docValue := []byte(`{"name": "test"}`)

		// Write document
		err := testDB.Batch(ctx, [][2][]byte{{docKey, docValue}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify document was written
		actualKey := storeutils.KeyRangeStart(docKey)
		val, closer, err := testDB.pdb.Get(actualKey)
		require.NoError(t, err)
		require.NotNil(t, val)
		closer.Close()

		// Verify timestamp key was written
		timestampKey := append(storeutils.KeyRangeStart(docKey), storeutils.TransactionSuffix...)
		tsVal, closer, err := testDB.pdb.Get(timestampKey)
		require.NoError(t, err, "timestamp key should exist")
		defer closer.Close()

		// Decode and verify timestamp
		_, decodedTS, err := encoding.DecodeUint64Ascending(tsVal)
		require.NoError(t, err)
		assert.Equal(t, timestamp, decodedTS, "timestamp should match context timestamp")
	})

	t.Run("edge timestamp is written", func(t *testing.T) {
		// Create a graph index first
		var graphIndex indexes.IndexConfig
		err := json.Unmarshal([]byte(`{
			"name": "test_graph",
			"type": "graph_v0"
		}`), &graphIndex)
		require.NoError(t, err)
		err = testDB.AddIndex(graphIndex)
		require.NoError(t, err)

		// First create a document with edges
		docKey := []byte("source_doc")
		docValue := []byte(`{
			"name": "source",
			"_edges": {
				"test_graph": {
					"links_to": [
						{"target": "target_doc", "weight": 1.0}
					]
				}
			}
		}`)

		// Write document with edges
		err = testDB.Batch(ctx, [][2][]byte{{docKey, docValue}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Find the edge key
		// Pattern: source_doc:o:i:test_graph:out:links_to:target_doc:o
		prefix := append(storeutils.KeyRangeStart(docKey), []byte(":i:test_graph:out:")...)
		iter, err := testDB.pdb.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
		})
		require.NoError(t, err)
		defer iter.Close()

		// Find edge key
		var edgeKey []byte
		for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
			key := iter.Key()
			if storeutils.IsEdgeKey(key) && !storeutils.HasSuffix(key, storeutils.TransactionSuffix) {
				edgeKey = append([]byte(nil), key...)
				break
			}
		}
		require.NotNil(t, edgeKey, "edge key should exist")

		// Verify edge timestamp key was written
		timestampKey := append(append([]byte(nil), edgeKey...), storeutils.TransactionSuffix...)
		tsVal, closer, err := testDB.pdb.Get(timestampKey)
		require.NoError(t, err, "edge timestamp key should exist")
		defer closer.Close()

		// Decode and verify timestamp
		_, decodedTS, err := encoding.DecodeUint64Ascending(tsVal)
		require.NoError(t, err)
		assert.Equal(t, timestamp, decodedTS, "edge timestamp should match context timestamp")
	})

	t.Run("embedding timestamp is NOT written", func(t *testing.T) {
		// Embeddings should not get timestamp keys (they're derived data)
		embKey := append([]byte("test_doc"), []byte(":i:test_idx:e")...)
		embValue := make([]byte, 128) // Fake embedding data

		err := testDB.Batch(ctx, [][2][]byte{{embKey, embValue}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify embedding was written
		val, closer, err := testDB.pdb.Get(embKey)
		require.NoError(t, err)
		require.NotNil(t, val)
		closer.Close()

		// Verify timestamp key was NOT written
		timestampKey := append(embKey, storeutils.TransactionSuffix...)
		_, _, err = testDB.pdb.Get(timestampKey)
		assert.ErrorIs(t, err, pebble.ErrNotFound, "embedding should not have timestamp key")
	})

	t.Run("summary timestamp is NOT written", func(t *testing.T) {
		// Summaries should not get timestamp keys (they're derived data)
		sumKey := append([]byte("test_doc"), []byte(":i:test_idx:s")...)
		sumValue := []byte("test summary")

		err := testDB.Batch(ctx, [][2][]byte{{sumKey, sumValue}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify summary was written
		val, closer, err := testDB.pdb.Get(sumKey)
		require.NoError(t, err)
		require.NotNil(t, val)
		closer.Close()

		// Verify timestamp key was NOT written
		timestampKey := append(sumKey, storeutils.TransactionSuffix...)
		_, _, err = testDB.pdb.Get(timestampKey)
		assert.ErrorIs(t, err, pebble.ErrNotFound, "summary should not have timestamp key")
	})

	t.Run("no timestamp written when context has no timestamp", func(t *testing.T) {
		// Context without timestamp (e.g., direct DB writes, tests)
		ctxNoTS := context.Background()

		docKey := []byte("no_ts_doc")
		docValue := []byte(`{"name": "no timestamp"}`)

		err := testDB.Batch(ctxNoTS, [][2][]byte{{docKey, docValue}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify document was written
		actualKey := storeutils.KeyRangeStart(docKey)
		val, closer, err := testDB.pdb.Get(actualKey)
		require.NoError(t, err)
		require.NotNil(t, val)
		closer.Close()

		// Verify timestamp key was NOT written (timestamp was 0)
		timestampKey := append(actualKey, storeutils.TransactionSuffix...)
		_, _, err = testDB.pdb.Get(timestampKey)
		assert.ErrorIs(t, err, pebble.ErrNotFound, "no timestamp should be written when context timestamp is 0")
	})
}

func TestStoreDBApplyOpBatch_NoTimestampWhenZero(t *testing.T) {
	testDB := setupTestDB(t)
	t.Cleanup(func() { testDB.Close() })

	storeDB := &StoreDB{coreDB: testDB}
	docKey := []byte("no_batch_ts_doc")
	docValue := []byte(`{"name":"no timestamp"}`)

	err := storeDB.applyOpBatch(
		context.Background(),
		[][2][]byte{{docKey, docValue}},
		nil,
		Op_SyncLevelWrite,
		0,
	)
	require.NoError(t, err)

	timestampKey := append(storeutils.KeyRangeStart(docKey), storeutils.TransactionSuffix...)
	_, _, err = testDB.pdb.Get(timestampKey)
	assert.ErrorIs(t, err, pebble.ErrNotFound, "no timestamp should be written when batch timestamp is 0")
}

func TestStoreDBApplyOpBatch_WritesTimestampFromBatch(t *testing.T) {
	testDB := setupTestDB(t)
	t.Cleanup(func() { testDB.Close() })

	storeDB := &StoreDB{coreDB: testDB}
	timestamp := uint64(time.Now().UnixNano())
	docKey := []byte("batch_ts_doc")
	docValue := []byte(`{"name":"batch timestamp"}`)

	err := storeDB.applyOpBatch(
		context.Background(),
		[][2][]byte{{docKey, docValue}},
		nil,
		Op_SyncLevelWrite,
		timestamp,
	)
	require.NoError(t, err)

	timestampKey := append(storeutils.KeyRangeStart(docKey), storeutils.TransactionSuffix...)
	tsVal, closer, err := testDB.pdb.Get(timestampKey)
	require.NoError(t, err, "timestamp key should exist")
	defer closer.Close()

	_, decodedTS, err := encoding.DecodeUint64Ascending(tsVal)
	require.NoError(t, err)
	assert.Equal(t, timestamp, decodedTS, "timestamp should match batch timestamp")
}
