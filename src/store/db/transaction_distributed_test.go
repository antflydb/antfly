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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockShardNotifier captures notifications for testing
type mockShardNotifier struct {
	mu            sync.Mutex
	notifications []mockNotification
}

type mockNotification struct {
	shardID []byte
	txnID   []byte
	status  int32
}

func (m *mockShardNotifier) NotifyResolveIntent(ctx context.Context, shardID []byte, txnID []byte, status int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifications = append(m.notifications, mockNotification{
		shardID: shardID,
		txnID:   txnID,
		status:  status,
	})
	return nil
}

func (m *mockShardNotifier) getNotifications() []mockNotification {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]mockNotification, len(m.notifications))
	copy(result, m.notifications)
	return result
}

func (m *mockShardNotifier) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifications = nil
}

// TestDistributedTransaction_TwoPhaseCommit tests a distributed transaction across two shards
func TestDistributedTransaction_TwoPhaseCommit(t *testing.T) {
	// Setup: Create coordinator and participant shards
	coordinatorDir := t.TempDir()
	participantDir := t.TempDir()

	coordinatorDB := createTestDB(t, coordinatorDir)
	defer coordinatorDB.Close()

	participantDB := createTestDB(t, participantDir)
	defer participantDB.Close()

	// Setup mock notifier to capture cross-shard notifications
	mockNotifier := &mockShardNotifier{}
	coordinatorDB.shardNotifier = mockNotifier

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	participantShardID := []byte{2, 0, 0, 0, 0, 0, 0, 0}

	// === Phase 1: Prepare Phase ===

	t.Log("Phase 1: Initialize transaction on coordinator with participant list")
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

	// Verify transaction is in Pending state
	status, err := coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(0), status, "Transaction should be Pending")

	t.Log("Phase 1: Write intents on coordinator shard")
	coordinatorWriteOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShardID,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("account:alice"), Value: []byte(`{"balance":100}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = coordinatorDB.WriteIntent(ctx, coordinatorWriteOp)
	require.NoError(t, err)

	t.Log("Phase 1: Write intents on participant shard")
	participantWriteOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShardID,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("account:bob"), Value: []byte(`{"balance":50}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = participantDB.WriteIntent(ctx, participantWriteOp)
	require.NoError(t, err)

	// Verify intents exist on both shards
	coordinatorIntentKey := makeIntentKey(txnID[:], []byte("account:alice"))
	_, closer1, err := coordinatorDB.pdb.Get(coordinatorIntentKey)
	require.NoError(t, err)
	closer1.Close()

	participantIntentKey := makeIntentKey(txnID[:], []byte("account:bob"))
	_, closer2, err := participantDB.pdb.Get(participantIntentKey)
	require.NoError(t, err)
	closer2.Close()

	// === Phase 2: Commit Phase ===

	t.Log("Phase 2: Commit transaction on coordinator")
	commitOp := CommitTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	err = coordinatorDB.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	// Verify transaction is in Committed state
	status, err = coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), status, "Transaction should be Committed")

	// === Phase 3: Resolution Phase ===

	t.Log("Phase 3: Trigger recovery loop to notify participants")
	// Manually trigger the notification process (simulating what the recovery loop does)
	coordinatorDB.notifyPendingResolutions(ctx)

	// Wait for background goroutines to complete
	time.Sleep(100 * time.Millisecond)

	// Verify that notifications were sent to all participants (coordinator + participant)
	notifications := mockNotifier.getNotifications()
	require.Len(t, notifications, 2, "Should send notifications to both coordinator and participant shards")

	// Verify participant shard notification
	var participantNotification *mockNotification
	for i := range notifications {
		if string(notifications[i].shardID) == string(participantShardID) {
			participantNotification = &notifications[i]
			break
		}
	}
	require.NotNil(t, participantNotification, "Should have notification for participant shard")
	assert.Equal(t, txnID[:], participantNotification.txnID, "Notification should contain transaction ID")
	assert.Equal(t, int32(1), participantNotification.status, "Notification should indicate Committed status")

	t.Log("Phase 3: Participant receives notification and resolves intents")
	resolveOp := ResolveIntentsOp_builder{
		TxnId:  txnID[:],
		Status: 1, // Committed
	}.Build()
	err = participantDB.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	t.Log("Phase 3: Coordinator resolves its own intents")
	err = coordinatorDB.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// === Verification ===

	t.Log("Verification: Intents should be removed from both shards")
	_, _, err = coordinatorDB.pdb.Get(coordinatorIntentKey)
	assert.Error(t, err, "Coordinator intent should be removed")

	_, _, err = participantDB.pdb.Get(participantIntentKey)
	assert.Error(t, err, "Participant intent should be removed")

	// Verify actual data was written (use Get method which handles decompression)
	val1, err := coordinatorDB.Get(ctx, []byte("account:alice"))
	require.NoError(t, err)
	assert.Contains(t, val1, "balance", "Data should be committed on coordinator")

	val2, err := participantDB.Get(ctx, []byte("account:bob"))
	require.NoError(t, err)
	assert.Contains(t, val2, "balance", "Data should be committed on participant")

	t.Log("SUCCESS: Distributed transaction committed atomically across both shards")
}

// TestDistributedTransaction_TwoPhaseAbort tests distributed transaction abort
func TestDistributedTransaction_TwoPhaseAbort(t *testing.T) {
	// Setup: Create coordinator and participant shards
	coordinatorDir := t.TempDir()
	participantDir := t.TempDir()

	coordinatorDB := createTestDB(t, coordinatorDir)
	defer coordinatorDB.Close()

	participantDB := createTestDB(t, participantDir)
	defer participantDB.Close()

	// Setup mock notifier
	mockNotifier := &mockShardNotifier{}
	coordinatorDB.shardNotifier = mockNotifier

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	participantShardID := []byte{2, 0, 0, 0, 0, 0, 0, 0}

	// === Phase 1: Prepare Phase ===

	t.Log("Phase 1: Initialize transaction with participants")
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

	t.Log("Phase 1: Write intents on both shards")
	coordinatorWriteOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShardID,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("temp:1"), Value: []byte(`{"temporary":"data1"}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = coordinatorDB.WriteIntent(ctx, coordinatorWriteOp)
	require.NoError(t, err)

	participantWriteOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShardID,
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("temp:2"), Value: []byte(`{"temporary":"data2"}`)}.Build(),
			},
		}.Build(),
	}.Build()
	err = participantDB.WriteIntent(ctx, participantWriteOp)
	require.NoError(t, err)

	// === Phase 2: Abort Phase (simulating conflict or error) ===

	t.Log("Phase 2: Abort transaction due to conflict")
	abortOp := AbortTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	err = coordinatorDB.AbortTransaction(ctx, abortOp)
	require.NoError(t, err)

	// Verify transaction is in Aborted state
	status, err := coordinatorDB.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(2), status, "Transaction should be Aborted")

	// === Phase 3: Resolution Phase ===

	t.Log("Phase 3: Trigger recovery to notify participants of abort")
	coordinatorDB.notifyPendingResolutions(ctx)

	// Wait for background goroutines to complete
	time.Sleep(100 * time.Millisecond)

	// Verify notifications were sent to all participants
	notifications := mockNotifier.getNotifications()
	require.Len(t, notifications, 2, "Should send abort notifications to both shards")

	// Verify participant shard got abort notification
	var participantNotification *mockNotification
	for i := range notifications {
		if string(notifications[i].shardID) == string(participantShardID) {
			participantNotification = &notifications[i]
			break
		}
	}
	require.NotNil(t, participantNotification, "Should have notification for participant shard")
	assert.Equal(t, int32(2), participantNotification.status, "Notification should indicate Aborted status")

	t.Log("Phase 3: Resolve intents with abort status")
	resolveOp := ResolveIntentsOp_builder{
		TxnId:  txnID[:],
		Status: 2, // Aborted
	}.Build()
	err = participantDB.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	err = coordinatorDB.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// === Verification ===

	t.Log("Verification: Intents should be removed and no data should be committed")
	_, _, err = coordinatorDB.pdb.Get([]byte("temp:1"))
	assert.Error(t, err, "Aborted data should not be committed on coordinator")

	_, _, err = participantDB.pdb.Get([]byte("temp:2"))
	assert.Error(t, err, "Aborted data should not be committed on participant")

	t.Log("SUCCESS: Distributed transaction aborted correctly, no data committed")
}

// TestTransactionRecovery_AutomaticNotification tests automatic participant notification
func TestTransactionRecovery_AutomaticNotification(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	mockNotifier := &mockShardNotifier{}
	db.shardNotifier = mockNotifier

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())

	coordinatorShardID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	participantShardID := []byte{2, 0, 0, 0, 0, 0, 0, 0}

	// Initialize and commit a transaction
	initOp := InitTransactionOp_builder{
		TxnId:     txnID[:],
		Timestamp: timestamp,
		Participants: [][]byte{
			coordinatorShardID,
			participantShardID,
		},
	}.Build()
	err := db.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	commitOp := CommitTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	err = db.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	t.Log("Simulating recovery loop running (normally every 30 seconds)")
	// The recovery loop should detect the committed transaction and notify participants
	db.notifyPendingResolutions(ctx)

	// Wait for background goroutines to complete
	time.Sleep(100 * time.Millisecond)

	// Verify notifications were sent to all participants
	notifications := mockNotifier.getNotifications()
	require.Len(t, notifications, 2, "Recovery should trigger notifications to both shards")

	// Verify participant shard notification
	var participantNotification *mockNotification
	for i := range notifications {
		if string(notifications[i].shardID) == string(participantShardID) {
			participantNotification = &notifications[i]
			break
		}
	}
	require.NotNil(t, participantNotification, "Should have notification for participant shard")
	assert.Equal(t, int32(1), participantNotification.status, "Should notify with Committed status")

	t.Log("Running recovery again should not re-notify (participants already resolved)")
	mockNotifier.reset()
	db.notifyPendingResolutions(ctx)

	// Wait for background goroutines to complete
	time.Sleep(100 * time.Millisecond)

	// Should NOT notify again - participants are already tracked as resolved
	// This is the fix for orphaned intents: we track successful resolutions
	// and don't re-notify participants that have already acknowledged
	notifications = mockNotifier.getNotifications()
	assert.Empty(t, notifications, "Should not re-notify participants that already resolved")

	t.Log("SUCCESS: Automatic recovery notification works correctly")
}
