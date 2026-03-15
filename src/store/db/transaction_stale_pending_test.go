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
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStalePendingTransaction_AutoAbort verifies that the recovery loop
// auto-aborts Pending transaction records whose created_at is older than
// the 5-minute cutoff. Before the fix, Pending (status=0) records were
// ignored forever by notifyPendingResolutions.
func TestStalePendingTransaction_AutoAbort(t *testing.T) {
	dir := t.TempDir()
	coordinatorDB := createTestDB(t, dir)
	defer coordinatorDB.Close()

	// Wire proposeAbortTransactionFunc to call AbortTransaction directly (bypass Raft)
	coordinatorDB.SetProposeAbortTransactionFunc(func(ctx context.Context, op *AbortTransactionOp) error {
		return coordinatorDB.AbortTransaction(ctx, op)
	})

	// Use a failing notifier (we don't need participant resolution for this test)
	coordinatorDB.shardNotifier = &failingShardNotifier{}

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	participantShardID := []byte{2, 0, 0, 0, 0, 0, 0, 0}

	// Initialize a Pending transaction
	initOp := InitTransactionOp_builder{
		TxnId:     txnID[:],
		Timestamp: timestamp,
		Participants: [][]byte{
			coordinatorShardID,
			participantShardID,
		},
	}.Build()
	err := coordinatorDB.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Verify it's Pending (status=0)
	status, err := coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(0), status, "Transaction should be Pending")

	// Backdate created_at to 10 minutes ago (past the 5-minute cutoff)
	backdateTxnRecord(t, coordinatorDB, txnID[:], "created_at", time.Now().Add(-10*time.Minute).Unix())

	// Run recovery loop
	coordinatorDB.notifyPendingResolutions(ctx)

	// After fix: txn should be auto-aborted (status=2)
	status, err = coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(2), status, "Stale Pending transaction should be auto-aborted by recovery loop")
}

// TestStalePendingTransaction_NotAbortedIfRecent verifies that the recovery
// loop does NOT abort Pending transactions that are recent (created within
// the 5-minute cutoff). This prevents aborting active in-flight transactions.
func TestStalePendingTransaction_NotAbortedIfRecent(t *testing.T) {
	dir := t.TempDir()
	coordinatorDB := createTestDB(t, dir)
	defer coordinatorDB.Close()

	// Wire proposeAbortTransactionFunc
	coordinatorDB.SetProposeAbortTransactionFunc(func(ctx context.Context, op *AbortTransactionOp) error {
		return coordinatorDB.AbortTransaction(ctx, op)
	})

	coordinatorDB.shardNotifier = &failingShardNotifier{}

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	participantShardID := []byte{2, 0, 0, 0, 0, 0, 0, 0}

	// Initialize a Pending transaction (created_at is "now" by default)
	initOp := InitTransactionOp_builder{
		TxnId:     txnID[:],
		Timestamp: timestamp,
		Participants: [][]byte{
			coordinatorShardID,
			participantShardID,
		},
	}.Build()
	err := coordinatorDB.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Backdate created_at to only 1 minute ago (within the 5-minute cutoff)
	backdateTxnRecord(t, coordinatorDB, txnID[:], "created_at", time.Now().Add(-1*time.Minute).Unix())

	// Run recovery loop
	coordinatorDB.notifyPendingResolutions(ctx)

	// Transaction should still be Pending -- recovery must not abort active transactions
	status, err := coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(0), status, "Recent Pending transaction should NOT be aborted")
}

// TestStalePendingTransaction_IntentConflictCleared verifies the full flow:
// a stuck Pending txn with intents blocks other transactions via
// hasConflictingIntentForKey, and after recovery auto-aborts and resolves
// the intents, the conflict is cleared.
func TestStalePendingTransaction_IntentConflictCleared(t *testing.T) {
	dir := t.TempDir()
	coordinatorDB := createTestDB(t, dir)
	defer coordinatorDB.Close()

	// Wire proposeAbortTransactionFunc
	coordinatorDB.SetProposeAbortTransactionFunc(func(ctx context.Context, op *AbortTransactionOp) error {
		return coordinatorDB.AbortTransaction(ctx, op)
	})

	// Wire proposeResolveIntentsFunc to call ResolveIntents directly
	coordinatorDB.SetProposeResolveIntentsFunc(func(ctx context.Context, op *ResolveIntentsOp) error {
		return coordinatorDB.ResolveIntents(ctx, op)
	})

	coordinatorDB.shardNotifier = &failingShardNotifier{}

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	// Initialize Pending transaction (coordinator-only, no external participants)
	initOp := InitTransactionOp_builder{
		TxnId:     txnID[:],
		Timestamp: timestamp,
		Participants: [][]byte{
			coordinatorShardID,
		},
	}.Build()
	err := coordinatorDB.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Write an intent on key "k"
	writeOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShardID,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("k"), Value: []byte(`{"data":"test"}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = coordinatorDB.WriteIntent(ctx, writeOp)
	require.NoError(t, err)

	// Verify the intent creates a conflict for other transactions
	conflictTxnID, err := coordinatorDB.hasConflictingIntentForKey([]byte("k"), []byte("other-txn-id"))
	require.NoError(t, err)
	assert.NotNil(t, conflictTxnID, "Should have conflicting intent from stuck Pending txn")

	// Backdate created_at to trigger auto-abort
	backdateTxnRecord(t, coordinatorDB, txnID[:], "created_at", time.Now().Add(-10*time.Minute).Unix())

	// First recovery pass: auto-aborts the Pending txn
	coordinatorDB.notifyPendingResolutions(ctx)

	status, err := coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(2), status, "Transaction should be aborted")

	// Backdate committed_at so the aborted record is eligible for resolution
	backdateTxnRecord(t, coordinatorDB, txnID[:], "committed_at", time.Now().Add(-10*time.Minute).Unix())

	// Second recovery pass: resolves intents for the now-aborted txn
	coordinatorDB.notifyPendingResolutions(ctx)

	// Verify the intent conflict is cleared
	conflictTxnID, err = coordinatorDB.hasConflictingIntentForKey([]byte("k"), []byte("other-txn-id"))
	require.NoError(t, err)
	assert.Nil(t, conflictTxnID, "Conflict should be cleared after recovery resolved intents")
}

// backdateTxnRecord updates a field in the transaction record stored in Pebble.
func backdateTxnRecord(t *testing.T, db *DBImpl, txnID []byte, field string, value any) {
	t.Helper()

	txnKey := makeTxnKey(txnID)
	data, closer, err := db.pdb.Get(txnKey)
	require.NoError(t, err)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	closer.Close()

	var record map[string]any
	err = json.Unmarshal(dataCopy, &record)
	require.NoError(t, err)

	record[field] = value

	updatedData, err := json.Marshal(record)
	require.NoError(t, err)
	err = db.pdb.Set(txnKey, updatedData, pebble.Sync)
	require.NoError(t, err)
}
