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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOrphanedIntents_CleanupPrevented verifies that the cleanup logic does NOT delete
// transaction records until all participants have resolved their intents.
//
// With the fix: Transaction records are preserved when participants haven't resolved,
// preventing orphaned intents even if cleanup time has passed.
//
// This test verifies the fix by:
// 1. Creating a committed transaction with unresolved participant intents
// 2. Simulating time passing beyond the cleanup threshold
// 3. Running the recovery/cleanup loop
// 4. Verifying the transaction record is PRESERVED (not deleted)
func TestOrphanedIntents_CleanupPrevented(t *testing.T) {
	// Setup: Create coordinator DB
	coordinatorDir := t.TempDir()
	coordinatorDB := createTestDB(t, coordinatorDir)
	defer coordinatorDB.Close()

	// Use a mock notifier that always fails to simulate network partition
	failingNotifier := &failingShardNotifier{}
	coordinatorDB.shardNotifier = failingNotifier

	ctx := context.Background()
	txnID := uuid.New()
	// Use a timestamp from 10 minutes ago to ensure it's past the cleanup cutoff
	timestamp := uint64(time.Now().Add(-10 * time.Minute).Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	participantShardID := []byte{2, 0, 0, 0, 0, 0, 0, 0}

	// === Phase 1: Initialize transaction with participants ===
	t.Log("Phase 1: Initialize transaction on coordinator")
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

	// === Phase 2: Commit transaction ===
	t.Log("Phase 2: Commit transaction on coordinator")
	commitOp := CommitTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	_, err = coordinatorDB.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	// Modify the committed_at time to be in the past (simulating old transaction)
	txnKey := makeTxnKey(txnID[:])
	txnData, closer, err := coordinatorDB.pdb.Get(txnKey)
	require.NoError(t, err)
	txnDataCopy := make([]byte, len(txnData))
	copy(txnDataCopy, txnData)
	closer.Close()

	var record TxnRecord
	err = json.Unmarshal(txnDataCopy, &record)
	require.NoError(t, err)

	// Set committed_at to 10 minutes ago (past the 5 minute cleanup cutoff)
	record.FinalizedAt = time.Now().Add(-10 * time.Minute).Unix()

	updatedData, err := json.Marshal(record)
	require.NoError(t, err)
	err = coordinatorDB.pdb.Set(txnKey, updatedData, pebble.Sync)
	require.NoError(t, err)

	// Verify transaction still exists before cleanup attempt
	status, err := coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), status, "Transaction should be Committed")

	// === Phase 3: Run cleanup (simulating recovery loop) ===
	t.Log("Phase 3: Running recovery loop / cleanup")
	// The notifier will fail, so participants won't be marked as resolved
	coordinatorDB.notifyPendingResolutions(ctx)

	// === Phase 4: Verify transaction record is PRESERVED ===
	t.Log("Phase 4: Verifying transaction record is preserved")

	// With the fix, the record should NOT be deleted because participants
	// haven't been marked as resolved (notifications failed)
	status, err = coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	if err != nil {
		t.Fatal("BUG: Transaction record was deleted before all participants resolved - orphaned intents possible!")
	}

	assert.Equal(t, int32(1), status, "Transaction should still be Committed")
	t.Log("FIX VERIFIED: Transaction record preserved - cleanup prevented until all participants resolve")
}

// TestOrphanedIntents_RecoveryLoopRace tests a more realistic scenario
// where the recovery loop runs but notifications fail, leading to eventual
// cleanup before resolution.
func TestOrphanedIntents_RecoveryLoopRace(t *testing.T) {
	coordinatorDir := t.TempDir()
	coordinatorDB := createTestDB(t, coordinatorDir)
	defer coordinatorDB.Close()

	// Notifier that fails (simulates persistent network partition)
	failingNotifier := &failingShardNotifier{}
	coordinatorDB.shardNotifier = failingNotifier

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	participantShardID := []byte{2, 0, 0, 0, 0, 0, 0, 0}

	// Initialize and commit transaction
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

	// Write coordinator's own intent
	writeOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShardID,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("coord:key1"), Value: []byte(`{"data":"coordinator"}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = coordinatorDB.WriteIntent(ctx, writeOp)
	require.NoError(t, err)

	commitOp := CommitTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	_, err = coordinatorDB.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	// Trigger recovery loop - this will try to notify participants
	// but our failing notifier will prevent delivery
	t.Log("Running recovery loop (notifications will fail)")
	coordinatorDB.notifyPendingResolutions(ctx)

	// Wait a bit for background goroutines
	time.Sleep(200 * time.Millisecond)

	// Verify notifications were attempted but none succeeded
	assert.Empty(t, failingNotifier.successfulNotifications(),
		"No notifications should succeed with failing notifier")
	assert.Positive(t, failingNotifier.attemptCount(),
		"At least one notification attempt should have been made")

	// Transaction record should still exist (not yet cleaned up)
	status, err := coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), status, "Transaction should still be Committed")

	t.Log("Test passed - transaction record preserved while notifications failing")
	t.Log("In production, after 5 minutes cleanup would orphan unresolved intents")
}

// failingShardNotifier is a mock that simulates network partition
type failingShardNotifier struct {
	mu       sync.Mutex
	attempts int
}

func (f *failingShardNotifier) NotifyResolveIntent(ctx context.Context, shardID []byte, txnID []byte, status int32, commitVersion uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	// Simulate network failure
	return context.DeadlineExceeded
}

func (f *failingShardNotifier) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func (f *failingShardNotifier) successfulNotifications() []mockNotification {
	return nil // Always fails
}
