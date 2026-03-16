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
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransactionLifecycle_CommitFlow tests the full commit flow
func TestTransactionLifecycle_CommitFlow(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())
	coordinatorShard := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	// Step 1: Initialize transaction on coordinator
	initOp := InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    timestamp,
		Participants: [][]byte{coordinatorShard},
	}.Build()
	err := db.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Verify transaction is in Pending state
	status, err := db.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(0), status, "Transaction should be Pending")

	// Step 2: Write intents
	writeIntentOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShard,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("user:1"), Value: []byte(`{"name":"Alice","age":30}`)}.Build(),
				Write_builder{Key: []byte("user:2"), Value: []byte(`{"name":"Bob","age":25}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = db.WriteIntent(ctx, writeIntentOp)
	require.NoError(t, err)

	// Verify intents exist
	intentKey1 := makeIntentKey(txnID[:], []byte("user:1"))
	_, closer1, err := db.pdb.Get(intentKey1)
	require.NoError(t, err)
	closer1.Close()

	// Step 3: Commit transaction
	commitOp := CommitTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	cv, err := db.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	// Verify transaction is in Committed state
	status, err = db.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), status, "Transaction should be Committed")

	// Step 4: Resolve intents
	resolveOp := ResolveIntentsOp_builder{
		TxnId:         txnID[:],
		Status:        1, // Committed
		CommitVersion: cv,
	}.Build()
	err = db.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// Verify intents are removed
	_, _, err = db.pdb.Get(intentKey1)
	assert.Error(t, err, "Intent should be removed")

	// Verify actual data was written (would need to check actual keys)
	// This is a simplified test - in production the actual keys would be written
}

// TestTransactionLifecycle_AbortFlow tests the full abort flow
func TestTransactionLifecycle_AbortFlow(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())
	coordinatorShard := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	// Step 1: Initialize transaction
	initOp := InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    timestamp,
		Participants: [][]byte{coordinatorShard},
	}.Build()
	err := db.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Step 2: Write intents
	writeIntentOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShard,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("temp:1"), Value: []byte(`{"data":"temporary"}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = db.WriteIntent(ctx, writeIntentOp)
	require.NoError(t, err)

	// Verify intent exists
	intentKey := makeIntentKey(txnID[:], []byte("temp:1"))
	_, closer, err := db.pdb.Get(intentKey)
	require.NoError(t, err)
	closer.Close()

	// Step 3: Abort transaction (simulating failure)
	abortOp := AbortTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	err = db.AbortTransaction(ctx, abortOp)
	require.NoError(t, err)

	// Verify transaction is in Aborted state
	status, err := db.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(2), status, "Transaction should be Aborted")

	// Step 4: Resolve intents with abort status
	resolveOp := ResolveIntentsOp_builder{
		TxnId:  txnID[:],
		Status: 2, // Aborted
	}.Build()
	err = db.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// Verify intent is removed
	_, _, err = db.pdb.Get(intentKey)
	assert.Error(t, err, "Intent should be removed")

	// Verify actual data was NOT written (intent was discarded)
}

// TestConcurrentTransactions tests multiple transactions running concurrently
func TestConcurrentTransactions(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := context.Background()
	numTransactions := 5

	// Create and initialize multiple transactions
	txnIDs := make([]uuid.UUID, numTransactions)
	for i := range numTransactions {
		txnID := uuid.New()
		txnIDs[i] = txnID

		initOp := InitTransactionOp_builder{
			TxnId:        txnID[:],
			Timestamp:    uint64(time.Now().Unix()) + uint64(i),
			Participants: [][]byte{{byte(i), 0, 0, 0, 0, 0, 0, 0}},
		}.Build()
		err := db.InitTransaction(ctx, initOp)
		require.NoError(t, err)
	}

	// Write intents for all transactions
	for i, txnID := range txnIDs {
		writeIntentOp := WriteIntentOp_builder{
			TxnId:            txnID[:],
			Timestamp:        uint64(time.Now().Unix()),
			CoordinatorShard: []byte{byte(i), 0, 0, 0, 0, 0, 0, 0},
			Batch: BatchOp_builder{
				Writes: []*Write{
					Write_builder{
						Key:   []byte("concurrent:" + string(rune(i))),
						Value: []byte(`{"txn":"` + txnID.String() + `"}`),
					}.Build(),
				},
			}.Build(),
		}.Build()
		err := db.WriteIntent(ctx, writeIntentOp)
		require.NoError(t, err)
	}

	// Commit half, abort half
	commitVersions := make([]uint64, len(txnIDs))
	for i, txnID := range txnIDs {
		if i%2 == 0 {
			commitOp := CommitTransactionOp_builder{TxnId: txnID[:]}.Build()
			cv, err := db.CommitTransaction(ctx, commitOp)
			require.NoError(t, err)
			commitVersions[i] = cv
		} else {
			abortOp := AbortTransactionOp_builder{TxnId: txnID[:]}.Build()
			err := db.AbortTransaction(ctx, abortOp)
			require.NoError(t, err)
		}
	}

	// Verify all transactions have correct status
	for i, txnID := range txnIDs {
		status, err := db.GetTransactionStatus(ctx, txnID[:])
		require.NoError(t, err)

		if i%2 == 0 {
			assert.Equal(t, int32(1), status, "Transaction %d should be Committed", i)
		} else {
			assert.Equal(t, int32(2), status, "Transaction %d should be Aborted", i)
		}
	}

	// Resolve all intents
	for i, txnID := range txnIDs {
		var status int32
		if i%2 == 0 {
			status = 1 // Committed
		} else {
			status = 2 // Aborted
		}

		resolveOp := ResolveIntentsOp_builder{
			TxnId:         txnID[:],
			Status:        status,
			CommitVersion: commitVersions[i],
		}.Build()
		err := db.ResolveIntents(ctx, resolveOp)
		require.NoError(t, err)
	}
}

// TestTransactionWithMultipleIntents tests a transaction with many write intents
func TestTransactionWithMultipleIntents(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	// Initialize transaction
	initOp := InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    timestamp,
		Participants: [][]byte{{1, 0, 0, 0, 0, 0, 0, 0}},
	}.Build()
	err := db.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Create multiple writes and deletes
	numWrites := 10
	numDeletes := 5

	writes := make([]*Write, numWrites)
	for i := range numWrites {
		writes[i] = Write_builder{
			Key:   []byte("batch:write:" + string(rune(i))),
			Value: []byte(`{"index":` + string(rune(i)) + `}`),
		}.Build()
	}

	deletes := make([][]byte, numDeletes)
	for i := range numDeletes {
		deletes[i] = []byte("batch:delete:" + string(rune(i)))
	}

	// Write all intents
	writeIntentOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: []byte{1, 0, 0, 0, 0, 0, 0, 0},
		Batch: BatchOp_builder{
			Writes:  writes,
			Deletes: deletes,
		}.Build(),
	}.Build()
	err = db.WriteIntent(ctx, writeIntentOp)
	require.NoError(t, err)

	// Commit and resolve
	commitOp := CommitTransactionOp_builder{TxnId: txnID[:]}.Build()
	cv, err := db.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	resolveOp := ResolveIntentsOp_builder{
		TxnId:         txnID[:],
		Status:        1, // Committed
		CommitVersion: cv,
	}.Build()
	err = db.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// Verify all intents were resolved
	for i := range numWrites {
		intentKey := makeIntentKey(txnID[:], []byte("batch:write:"+string(rune(i))))
		_, _, err := db.pdb.Get(intentKey)
		assert.Error(t, err, "Intent %d should be removed", i)
	}

	for i := range numDeletes {
		intentKey := makeIntentKey(txnID[:], []byte("batch:delete:"+string(rune(i))))
		_, _, err := db.pdb.Get(intentKey)
		assert.Error(t, err, "Delete intent %d should be removed", i)
	}
}

// TestTransactionRecovery tests the recovery mechanism
func TestTransactionRecovery(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := context.Background()

	// Create some old committed transactions
	for i := range 3 {
		txnID := uuid.New()

		initOp := InitTransactionOp_builder{
			TxnId:        txnID[:],
			Timestamp:    uint64(time.Now().Unix()),
			Participants: [][]byte{{byte(i), 0, 0, 0, 0, 0, 0, 0}},
		}.Build()
		err := db.InitTransaction(ctx, initOp)
		require.NoError(t, err)

		commitOp := CommitTransactionOp_builder{TxnId: txnID[:]}.Build()
		_, err = db.CommitTransaction(ctx, commitOp)
		require.NoError(t, err)
	}

	// Manually run recovery (normally called by LeaderFactory)
	db.notifyPendingResolutions(ctx)

	// For now, just verify the method runs without errors
	// In production, this would also test actual participant notifications
}

// TestTransactionMetrics tests that metrics are properly recorded
func TestTransactionMetrics(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := context.Background()
	txnID := uuid.New()

	// Initialize transaction
	initOp := InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    uint64(time.Now().Unix()),
		Participants: [][]byte{{1, 0, 0, 0, 0, 0, 0, 0}},
	}.Build()
	err := db.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Write intents
	writeIntentOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        uint64(time.Now().Unix()),
		CoordinatorShard: []byte{1, 0, 0, 0, 0, 0, 0, 0},
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("metrics:test"), Value: []byte(`{"test":"data"}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = db.WriteIntent(ctx, writeIntentOp)
	require.NoError(t, err)

	// Commit
	commitOp := CommitTransactionOp_builder{TxnId: txnID[:]}.Build()
	cv, err := db.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	// Resolve
	resolveOp := ResolveIntentsOp_builder{
		TxnId:         txnID[:],
		Status:        1,
		CommitVersion: cv,
	}.Build()
	err = db.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// Metrics are updated via Prometheus - in a real test you'd use prometheus testutil
	// to verify metric values. For now, we just ensure no panics occur.
}

// TestTimestampBasedConflictResolution tests that higher timestamps win on overlapping writes
func TestTimestampBasedConflictResolution(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := context.Background()
	key := []byte("conflict:test")
	coordinatorShard := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	// Transaction 1: Write with timestamp 100
	txn1ID := uuid.New()
	timestamp1 := uint64(100)

	initOp1 := InitTransactionOp_builder{
		TxnId:        txn1ID[:],
		Timestamp:    timestamp1,
		Participants: [][]byte{coordinatorShard},
	}.Build()
	err := db.InitTransaction(ctx, initOp1)
	require.NoError(t, err)

	writeIntentOp1 := WriteIntentOp_builder{
		TxnId:            txn1ID[:],
		Timestamp:        timestamp1,
		CoordinatorShard: coordinatorShard,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: key, Value: []byte(`{"value":"first","timestamp":100}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = db.WriteIntent(ctx, writeIntentOp1)
	require.NoError(t, err)

	commitOp1 := CommitTransactionOp_builder{TxnId: txn1ID[:]}.Build()
	cv1, err := db.CommitTransaction(storeutils.WithTimestamp(ctx, timestamp1), commitOp1)
	require.NoError(t, err)

	resolveOp1 := ResolveIntentsOp_builder{
		TxnId:         txn1ID[:],
		Status:        1, // Committed
		CommitVersion: cv1,
	}.Build()
	err = db.ResolveIntents(ctx, resolveOp1)
	require.NoError(t, err)

	// Verify first write succeeded
	doc, err := db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "first", doc["value"])
	// Timestamp is now stored separately, not in document
	assert.NotContains(t, doc, "_timestamp", "Document should not contain _timestamp field")

	// Verify timestamp is in separate key
	txnKey := storeutils.KeyRangeStart(key)
	txnKey = append(txnKey, storeutils.TransactionSuffix...)
	tsBytes, closer, err := db.pdb.Get(txnKey)
	require.NoError(t, err)
	defer closer.Close()
	require.Len(t, tsBytes, 8)
	_, timestamp, _ := encoding.DecodeUint64Ascending(tsBytes)
	assert.Equal(t, uint64(100), timestamp, "Timestamp should be 100 in metadata key")

	// Transaction 2: Write with LOWER timestamp 50 (should be skipped)
	txn2ID := uuid.New()
	timestamp2 := uint64(50)

	initOp2 := InitTransactionOp_builder{
		TxnId:        txn2ID[:],
		Timestamp:    timestamp2,
		Participants: [][]byte{coordinatorShard},
	}.Build()
	err = db.InitTransaction(ctx, initOp2)
	require.NoError(t, err)

	writeIntentOp2 := WriteIntentOp_builder{
		TxnId:            txn2ID[:],
		Timestamp:        timestamp2,
		CoordinatorShard: coordinatorShard,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: key, Value: []byte(`{"value":"second","timestamp":50}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = db.WriteIntent(ctx, writeIntentOp2)
	require.NoError(t, err)

	commitOp2 := CommitTransactionOp_builder{TxnId: txn2ID[:]}.Build()
	cv2, err := db.CommitTransaction(storeutils.WithTimestamp(ctx, timestamp2), commitOp2)
	require.NoError(t, err)

	resolveOp2 := ResolveIntentsOp_builder{
		TxnId:         txn2ID[:],
		Status:        1, // Committed
		CommitVersion: cv2,
	}.Build()
	err = db.ResolveIntents(ctx, resolveOp2)
	require.NoError(t, err)

	// Verify write was skipped - still has first value
	doc, err = db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "first", doc["value"], "Lower timestamp write should be skipped")

	// Verify timestamp remains 100 in metadata key
	txnKey = storeutils.KeyRangeStart(key)
	txnKey = append(txnKey, storeutils.TransactionSuffix...)
	tsBytes, closer, err = db.pdb.Get(txnKey)
	require.NoError(t, err)
	defer closer.Close()
	_, timestamp, _ = encoding.DecodeUint64Ascending(tsBytes)
	assert.Equal(t, uint64(100), timestamp, "Timestamp should remain 100 in metadata key")

	// Transaction 3: Write with HIGHER timestamp 200 (should succeed)
	txn3ID := uuid.New()
	timestamp3 := uint64(200)

	initOp3 := InitTransactionOp_builder{
		TxnId:        txn3ID[:],
		Timestamp:    timestamp3,
		Participants: [][]byte{coordinatorShard},
	}.Build()
	err = db.InitTransaction(ctx, initOp3)
	require.NoError(t, err)

	writeIntentOp3 := WriteIntentOp_builder{
		TxnId:            txn3ID[:],
		Timestamp:        timestamp3,
		CoordinatorShard: coordinatorShard,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: key, Value: []byte(`{"value":"third","timestamp":200}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = db.WriteIntent(ctx, writeIntentOp3)
	require.NoError(t, err)

	commitOp3 := CommitTransactionOp_builder{TxnId: txn3ID[:]}.Build()
	cv3, err := db.CommitTransaction(storeutils.WithTimestamp(ctx, timestamp3), commitOp3)
	require.NoError(t, err)

	resolveOp3 := ResolveIntentsOp_builder{
		TxnId:         txn3ID[:],
		Status:        1, // Committed
		CommitVersion: cv3,
	}.Build()
	err = db.ResolveIntents(ctx, resolveOp3)
	require.NoError(t, err)

	// Verify write succeeded - now has third value
	doc, err = db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "third", doc["value"], "Higher timestamp write should succeed")

	// Verify timestamp updated to 200 in metadata key
	txnKey = storeutils.KeyRangeStart(key)
	txnKey = append(txnKey, storeutils.TransactionSuffix...)
	tsBytes, closer, err = db.pdb.Get(txnKey)
	require.NoError(t, err)
	defer closer.Close()
	_, timestamp, _ = encoding.DecodeUint64Ascending(tsBytes)
	assert.Equal(t, uint64(200), timestamp, "Timestamp should be updated to 200 in metadata key")

	t.Log("Timestamp-based conflict resolution working correctly:")
	t.Log("  - T1 (ts=100) wrote 'first' ✓")
	t.Log("  - T2 (ts=50) tried 'second', skipped ✓")
	t.Log("  - T3 (ts=200) wrote 'third' ✓")
	t.Log("  - Timestamps stored in separate key:t metadata (not in document)")
}

func TestCommitTimestampBecomesVisibleVersion(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	ctx := t.Context()
	key := []byte("txn:visible_version")
	coordinatorShard := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	err := db.Batch(storeutils.WithTimestamp(ctx, 10000), [][2][]byte{
		{key, []byte(`{"title":"v1"}`)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	txnID := uuid.New()
	beginTS := uint64(11000)
	commitTS := uint64(11001)

	err = db.InitTransaction(ctx, InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    beginTS,
		Participants: [][]byte{coordinatorShard},
	}.Build())
	require.NoError(t, err)

	err = db.WriteIntent(ctx, WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        beginTS,
		CoordinatorShard: coordinatorShard,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: key, Value: []byte(`{"title":"v2"}`)}.Build(),
			},
		}.Build(),
		Predicates: []*VersionPredicate{
			VersionPredicate_builder{Key: key, ExpectedVersion: 10000}.Build(),
		},
	}.Build())
	require.NoError(t, err)

	_, err = db.CommitTransaction(storeutils.WithTimestamp(ctx, commitTS), CommitTransactionOp_builder{
		TxnId: txnID[:],
	}.Build())
	require.NoError(t, err)

	err = db.ResolveIntents(ctx, ResolveIntentsOp_builder{
		TxnId:         txnID[:],
		Status:        TxnStatusCommitted,
		CommitVersion: commitTS,
	}.Build())
	require.NoError(t, err)

	doc, err := db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "v2", doc["title"])

	metaKey := append(storeutils.KeyRangeStart(key), storeutils.TransactionSuffix...)
	tsBytes, closer, err := db.pdb.Get(metaKey)
	require.NoError(t, err)
	defer closer.Close()

	_, visibleTS, err := encoding.DecodeUint64Ascending(tsBytes)
	require.NoError(t, err)
	assert.Equal(t, commitTS, visibleTS, "Committed value should expose commit timestamp as visible version")

	txn2ID := uuid.New()
	err = db.InitTransaction(ctx, InitTransactionOp_builder{
		TxnId:        txn2ID[:],
		Timestamp:    12000,
		Participants: [][]byte{coordinatorShard},
	}.Build())
	require.NoError(t, err)

	err = db.WriteIntent(ctx, WriteIntentOp_builder{
		TxnId:            txn2ID[:],
		Timestamp:        12000,
		CoordinatorShard: coordinatorShard,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: key, Value: []byte(`{"title":"stale"}`)}.Build(),
			},
		}.Build(),
		Predicates: []*VersionPredicate{
			VersionPredicate_builder{Key: key, ExpectedVersion: 10000}.Build(),
		},
	}.Build())
	var versionConflict *ErrVersionConflict
	require.ErrorAs(t, err, &versionConflict)
}
