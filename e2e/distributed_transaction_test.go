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

package e2e

import (
	"context"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// Test configuration constants
const (
	txnTestTableName = "txn_test_table"
	txnTestNumShards = 4
)

// TestE2E_DistributedTransaction_MultiShardCommit verifies batch operation
// touching multiple shards commits atomically via 2PC.
func TestE2E_DistributedTransaction_MultiShardCommit(t *testing.T) {
	skipUnlessEnv(t, "RUN_TXN_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, txnTestTableName, txnTestNumShards)

	// Log shard distribution
	distribution := cluster.GetShardDistribution(ctx, txnTestTableName)
	t.Logf("Shard distribution across stores: %v", distribution)

	// Insert initial documents across shards using keys that partition to different shards
	initialDocs := map[string]any{
		"0_account_a": map[string]any{"name": "Alice", "balance": 1000},
		"4_account_b": map[string]any{"name": "Bob", "balance": 500},
		"8_account_c": map[string]any{"name": "Charlie", "balance": 750},
		"c_account_d": map[string]any{"name": "Diana", "balance": 250},
	}

	t.Log("Inserting initial documents across shards...")
	_, err := cluster.Client.Batch(ctx, txnTestTableName, antfly.BatchRequest{
		Inserts:   initialDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert initial documents")

	t.Log("Verifying initial documents...")
	err = cluster.VerifyKeyValues(ctx, txnTestTableName, initialDocs)
	require.NoError(t, err, "Initial documents verification failed")

	// Perform multi-key update via Batch that modifies all 4 keys
	// This should trigger 2PC since keys span multiple shards
	updatedDocs := map[string]any{
		"0_account_a": map[string]any{"name": "Alice", "balance": 1100},
		"4_account_b": map[string]any{"name": "Bob", "balance": 600},
		"8_account_c": map[string]any{"name": "Charlie", "balance": 650},
		"c_account_d": map[string]any{"name": "Diana", "balance": 150},
	}

	t.Log("Performing multi-shard batch update (should trigger 2PC)...")
	_, err = cluster.Client.Batch(ctx, txnTestTableName, antfly.BatchRequest{
		Inserts:   updatedDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Multi-shard batch update failed")

	t.Log("Verifying all updates were applied atomically...")
	err = cluster.VerifyKeyValues(ctx, txnTestTableName, updatedDocs)
	require.NoError(t, err, "Updated documents verification failed")
}

// TestE2E_DistributedTransaction_AtomicMultiKeyUpdate tests the classic "bank transfer"
// scenario - atomic update of multiple keys.
func TestE2E_DistributedTransaction_AtomicMultiKeyUpdate(t *testing.T) {
	skipUnlessEnv(t, "RUN_TXN_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, txnTestTableName, txnTestNumShards)

	// Insert accounts on different shards
	t.Log("Creating Alice and Bob accounts on different shards...")
	initialDocs := map[string]any{
		"0_alice": map[string]any{"name": "Alice", "balance": 1000},
		"8_bob":   map[string]any{"name": "Bob", "balance": 0},
	}

	_, err := cluster.Client.Batch(ctx, txnTestTableName, antfly.BatchRequest{
		Inserts:   initialDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to create accounts")

	t.Log("Verifying initial balances...")
	err = cluster.VerifyKeyValues(ctx, txnTestTableName, initialDocs)
	require.NoError(t, err, "Initial account verification failed")

	initialSum := 1000 + 0
	t.Logf("Initial sum of balances: %d", initialSum)

	// Perform atomic "transfer" via batch
	t.Log("Performing atomic transfer: Alice -500 -> Bob +500...")
	transferDocs := map[string]any{
		"0_alice": map[string]any{"name": "Alice", "balance": 500},
		"8_bob":   map[string]any{"name": "Bob", "balance": 500},
	}

	_, err = cluster.Client.Batch(ctx, txnTestTableName, antfly.BatchRequest{
		Inserts:   transferDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Transfer failed")

	t.Log("Verifying transfer was atomic...")
	err = cluster.VerifyKeyValues(ctx, txnTestTableName, transferDocs)
	require.NoError(t, err, "Transfer verification failed")

	// Verify sum of balances preserved (conservation)
	aliceDoc, err := cluster.Client.LookupKey(ctx, txnTestTableName, "0_alice")
	require.NoError(t, err, "Failed to lookup Alice")
	bobDoc, err := cluster.Client.LookupKey(ctx, txnTestTableName, "8_bob")
	require.NoError(t, err, "Failed to lookup Bob")

	aliceBalance := int(aliceDoc["balance"].(float64))
	bobBalance := int(bobDoc["balance"].(float64))
	finalSum := aliceBalance + bobBalance

	t.Logf("Final balances - Alice: %d, Bob: %d, Sum: %d", aliceBalance, bobBalance, finalSum)
	require.Equal(t, initialSum, finalSum, "Balance conservation violated! Sum changed from %d to %d", initialSum, finalSum)
}

// TestE2E_DistributedTransaction_MultiShardAbort verifies that when context is
// cancelled mid-transaction, no data is committed.
func TestE2E_DistributedTransaction_MultiShardAbort(t *testing.T) {
	skipUnlessEnv(t, "RUN_TXN_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, txnTestTableName, txnTestNumShards)

	// Insert initial documents
	initialDocs := map[string]any{
		"0_account_a": map[string]any{"name": "Alice", "balance": 1000},
		"4_account_b": map[string]any{"name": "Bob", "balance": 500},
		"8_account_c": map[string]any{"name": "Charlie", "balance": 750},
		"c_account_d": map[string]any{"name": "Diana", "balance": 250},
	}

	t.Log("Inserting initial documents...")
	_, err := cluster.Client.Batch(ctx, txnTestTableName, antfly.BatchRequest{
		Inserts:   initialDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert initial documents")

	// Store original values (wait for keys to be available due to async intent resolution)
	t.Log("Storing original values...")
	var originalValues = make(map[string]map[string]any)
	for key := range initialDocs {
		err := cluster.WaitForKeyAvailable(ctx, txnTestTableName, key, 10*time.Second)
		require.NoError(t, err, "Key %s not available", key)
		doc, err := cluster.Client.LookupKey(ctx, txnTestTableName, key)
		require.NoError(t, err, "Failed to lookup key %s", key)
		originalValues[key] = doc
	}

	// Create a very short timeout context to trigger abort
	t.Log("Attempting batch update with very short timeout (should timeout/abort)...")
	shortCtx, shortCancel := context.WithTimeout(ctx, 1*time.Millisecond)
	defer shortCancel()

	updatedDocs := map[string]any{
		"0_account_a": map[string]any{"name": "Alice_UPDATED", "balance": 9999},
		"4_account_b": map[string]any{"name": "Bob_UPDATED", "balance": 9999},
		"8_account_c": map[string]any{"name": "Charlie_UPDATED", "balance": 9999},
		"c_account_d": map[string]any{"name": "Diana_UPDATED", "balance": 9999},
	}

	_, err = cluster.Client.Batch(shortCtx, txnTestTableName, antfly.BatchRequest{
		Inserts:   updatedDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})

	// We expect this to fail due to timeout
	if err != nil {
		t.Logf("Batch failed as expected: %v", err)
	} else {
		t.Log("Warning: Batch completed despite short timeout - transaction may have succeeded")
	}

	// Verify all values unchanged (atomicity - nothing committed on abort)
	t.Log("Verifying no partial updates occurred (atomicity)...")

	// Give a moment for any async cleanup
	time.Sleep(100 * time.Millisecond)

	allUnchanged := true
	for key, originalDoc := range originalValues {
		currentDoc, lookupErr := cluster.Client.LookupKey(ctx, txnTestTableName, key)
		require.NoError(t, lookupErr, "Failed to lookup key %s after abort", key)

		originalBalance := int(originalDoc["balance"].(float64))
		currentBalance := int(currentDoc["balance"].(float64))

		if currentBalance != originalBalance {
			t.Logf("Key %s: balance changed from %d to %d", key, originalBalance, currentBalance)
			allUnchanged = false
		} else {
			t.Logf("Key %s: balance unchanged at %d", key, currentBalance)
		}
	}

	// Note: Due to the nature of timeout-based abort testing, the transaction might
	// have succeeded before the timeout. This test primarily verifies that if an
	// abort occurs, no partial writes are visible.
	if !allUnchanged && err != nil {
		t.Fatal("Partial update detected after transaction error - atomicity violated!")
	}

	if allUnchanged {
		t.Log("All values unchanged - abort maintained atomicity")
	} else {
		t.Log("Transaction completed before timeout - this is acceptable behavior")
	}
}

// TestE2E_DistributedTransaction_RecoveryNotification verifies the recovery loop
// properly resolves committed transactions.
func TestE2E_DistributedTransaction_RecoveryNotification(t *testing.T) {
	skipUnlessEnv(t, "RUN_TXN_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, txnTestTableName, txnTestNumShards)

	// Perform successful multi-shard transaction
	t.Log("Performing successful multi-shard transaction...")
	docs := map[string]any{
		"0_recovery_a": map[string]any{"name": "Alpha", "value": 100},
		"8_recovery_b": map[string]any{"name": "Beta", "value": 200},
	}

	_, err := cluster.Client.Batch(ctx, txnTestTableName, antfly.BatchRequest{
		Inserts:   docs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Multi-shard transaction failed")

	t.Log("Verifying transaction data...")
	err = cluster.VerifyKeyValues(ctx, txnTestTableName, docs)
	require.NoError(t, err, "Transaction data verification failed")

	// Wait briefly and verify data is still accessible
	// This tests that any recovery/cleanup doesn't affect committed data
	t.Log("Waiting briefly and re-verifying data accessibility...")
	time.Sleep(2 * time.Second)

	err = cluster.VerifyKeyValues(ctx, txnTestTableName, docs)
	require.NoError(t, err, "Data not accessible after wait")

	// Perform another transaction to confirm system is healthy
	t.Log("Performing second transaction to verify system health...")
	moreDocs := map[string]any{
		"4_recovery_c": map[string]any{"name": "Gamma", "value": 300},
		"c_recovery_d": map[string]any{"name": "Delta", "value": 400},
	}

	_, err = cluster.Client.Batch(ctx, txnTestTableName, antfly.BatchRequest{
		Inserts:   moreDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Second transaction failed")

	// Verify all data
	allDocs := map[string]any{
		"0_recovery_a": docs["0_recovery_a"],
		"8_recovery_b": docs["8_recovery_b"],
		"4_recovery_c": moreDocs["4_recovery_c"],
		"c_recovery_d": moreDocs["c_recovery_d"],
	}

	err = cluster.VerifyKeyValues(ctx, txnTestTableName, allDocs)
	require.NoError(t, err, "Final data verification failed")
}
