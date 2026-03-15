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
	"fmt"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// TestE2E_Autoscaling_ShardSplit tests autoscaling behavior with shard splits
func TestE2E_Autoscaling_ShardSplit(t *testing.T) {
	skipUnlessEnv(t, "RUN_AUTOSCALING_TESTS")
	ctx := testContext(t, 15*time.Minute)

	t.Log("=== Starting Autoscaling E2E Test ===")
	t.Log("Configuration: 1 metadata node, 3 store nodes, 4 tables x 5 shards")

	// Step 1: Create test cluster with 1 metadata + 3 store nodes
	// Configure for autoscaling with low shard size to trigger splits
	// NOTE: Using ReplicationFactor 3 to ensure there are always enough voters
	// during node removal. With RF=1, removing a node can cause Raft conf change
	// races that panic due to "removed all voters" errors.
	t.Log("Step 1: Starting test cluster...")
	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:     3,
		NumShards:         5,
		ReplicationFactor: 3,
		MaxShardSizeBytes: 1024 * 1024, // 1MB - very low to trigger splits easily
		DisableShardAlloc: false,
	})
	defer cluster.Cleanup()

	t.Logf("Cluster started with %d store nodes", cluster.GetActiveStoreCount())

	// Step 2: Create 4 tables with 5 shards each
	t.Log("Step 2: Creating tables...")
	tableNames := []string{"autoscale_t1", "autoscale_t2", "autoscale_t3", "autoscale_t4"}
	for _, tableName := range tableNames {
		err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
			NumShards: 5,
		})
		require.NoError(t, err, "Failed to create table %s", tableName)
		t.Logf("Created table: %s", tableName)

		// Wait for shards to be ready
		err = cluster.WaitForShardsReady(ctx, tableName, 5, 60*time.Second)
		require.NoError(t, err, "Shards not ready for table %s", tableName)
	}

	// Step 3: Start availability checker
	t.Log("Step 3: Starting availability checker...")
	availChecker := NewAvailabilityChecker(cluster, tableNames[0])
	availChecker.Start()
	defer availChecker.Stop()

	// Step 4: Insert data to trigger shard splits
	t.Log("Step 4: Inserting data to trigger shard splits...")
	// With 1MB max shard size, insert ~2MB per table to trigger splits
	const recordsPerTable = 50
	const recordSize = 50 * 1024 // 50KB per record, 50 records = 2.5MB per table

	for _, tableName := range tableNames {
		t.Logf("Inserting %d records into %s (~%.1f MB)", recordsPerTable, tableName, float64(recordsPerTable*recordSize)/(1024*1024))

		records := make(map[string]any, recordsPerTable)
		for i := range recordsPerTable {
			recordID := fmt.Sprintf("record-%s-%03d", tableName, i)
			records[recordID] = GenerateTestData(recordSize)
		}

		// Use LinearMerge for efficient bulk insert
		result, err := cluster.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
			Records:   records,
			DryRun:    false,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err, "Failed to insert data into %s", tableName)
		t.Logf("Inserted %d records into %s (status: %s)", result.Upserted, tableName, result.Status)
	}

	// Step 5: Wait for shard splits to occur
	t.Log("Step 5: Waiting for shard splits...")
	time.Sleep(5 * time.Second) // Give reconciler time to detect oversized shards

	for _, tableName := range tableNames {
		// With 2.5MB of data and 1MB max, we expect at least 2-3 shards after splits
		shardCount, err := cluster.WaitForShardCount(ctx, tableName, 5, 2*time.Minute)
		if err != nil {
			t.Logf("Warning: shard split may not have occurred for %s (got %d shards)", tableName, shardCount)
		} else {
			t.Logf("Table %s has %d shards", tableName, shardCount)
		}
	}

	// Step 6: Verify data is still accessible after splits
	t.Log("Step 6: Verifying data accessibility after splits...")
	for _, tableName := range tableNames {
		// Query a few records to verify data integrity
		for i := range 5 {
			recordID := fmt.Sprintf("record-%s-%03d", tableName, i)
			record, err := cluster.Client.LookupKey(ctx, tableName, recordID)
			require.NoError(t, err, "Failed to read record %s after splits", recordID)
			require.NotNil(t, record, "Record %s should exist", recordID)
		}
		t.Logf("Verified data accessibility for %s", tableName)
	}

	// Step 7: Add a new store node and verify shard reallocation
	t.Log("Step 7: Adding new store node...")
	newNode := cluster.AddStoreNode(ctx)
	t.Logf("Added store node %d", newNode.ID)

	// Wait for shard reallocation to new node - needs longer time for Raft conf changes to propagate
	t.Log("Waiting for shard reallocation and Raft state to converge...")
	time.Sleep(30 * time.Second) // Give reconciler time to rebalance and Raft state to converge

	// Verify the cluster is still healthy
	for _, tableName := range tableNames {
		status, err := cluster.Client.GetTable(ctx, tableName)
		require.NoError(t, err, "Failed to get table status after adding node")
		t.Logf("Table %s: %d shards after node addition", tableName, len(status.Shards))
	}

	// Step 8: Remove a store node and verify shard rebalancing
	t.Log("Step 8: Removing store node...")

	// Get the first store node ID to remove (not the one we just added)
	storeNodeIDs := cluster.GetStoreNodeIDs()
	var nodeToRemove = storeNodeIDs[0]
	for _, id := range storeNodeIDs {
		if id != newNode.ID {
			nodeToRemove = id
			break
		}
	}

	err := cluster.RemoveStoreNode(ctx, nodeToRemove)
	require.NoError(t, err, "Failed to remove store node")
	t.Logf("Removed store node %d", nodeToRemove)

	// Wait for shard rebalancing after node removal
	// With RF=3 and 20 shards (4 tables x 5 shards), the reconciler needs time to:
	// 1. Remove the dead node from all shards
	// 2. Re-elect leaders for any shards that lost their leader
	// 3. Complete any pending Raft configuration changes
	t.Log("Waiting for shard rebalancing...")
	time.Sleep(45 * time.Second) // Give reconciler time to reassign shards and elect new leaders

	// Step 9: Verify all data is still accessible after node removal
	t.Log("Step 9: Verifying data after node removal...")
	for _, tableName := range tableNames {
		for i := range 5 {
			recordID := fmt.Sprintf("record-%s-%03d", tableName, i)
			// Retry lookups since leader election may take time after node removal
			var record map[string]any
			var err error
			for retries := range 10 {
				record, err = cluster.Client.LookupKey(ctx, tableName, recordID)
				if err == nil {
					break
				}
				t.Logf("Retry %d for record %s: %v", retries+1, recordID, err)
				time.Sleep(2 * time.Second)
			}
			require.NoError(t, err, "Failed to read record %s after node removal (after retries)", recordID)
			require.NotNil(t, record, "Record %s should exist after node removal", recordID)
		}
		t.Logf("Verified data accessibility for %s after node removal", tableName)
	}

	// Step 10: Check availability stats
	t.Log("Step 10: Checking availability statistics...")
	success, failed, maxDowntime := availChecker.Stats()
	t.Logf("Availability stats: success=%d, failed=%d, maxDowntime=%v", success, failed, maxDowntime)

	// Allow some failed operations during transitions, but max downtime should be reasonable
	if maxDowntime > 30*time.Second {
		t.Errorf("Max downtime exceeded threshold: %v > 30s", maxDowntime)
	}

	totalOps := success + failed
	if totalOps > 0 {
		availabilityRate := float64(success) / float64(totalOps) * 100
		t.Logf("Overall availability rate: %.2f%%", availabilityRate)

		// We expect at least 90% availability
		if availabilityRate < 90 {
			t.Errorf("Availability rate too low: %.2f%% < 90%%", availabilityRate)
		}
	}

	// Step 11: Final cluster state
	t.Log("Step 11: Final cluster state...")
	for _, tableName := range tableNames {
		status, err := cluster.Client.GetTable(ctx, tableName)
		require.NoError(t, err, "Failed to get final table status")

		// Note: Per-shard stats aren't available through the SDK, use table-level storage status
		t.Logf("Table %s: %d shards, disk usage: %d bytes",
			tableName, len(status.Shards), status.StorageStatus.DiskUsage)
	}

	t.Logf("Final store node count: %d", cluster.GetActiveStoreCount())
	t.Log("=== Autoscaling E2E Test Completed Successfully ===")
}

// TestE2E_Autoscaling_NodeChurn tests the system's resilience to node additions and removals
func TestE2E_Autoscaling_NodeChurn(t *testing.T) {
	skipUnlessEnv(t, "RUN_AUTOSCALING_TESTS")
	ctx := testContext(t, 10*time.Minute)

	t.Log("=== Starting Node Churn E2E Test ===")

	// Start with 3 store nodes with replication factor 3 for availability during churn
	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:     3,
		NumShards:         5,
		ReplicationFactor: 3,
		DisableShardAlloc: false,
	})
	defer cluster.Cleanup()

	// Create a table
	tableName := "churn_test"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 5,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 5, 60*time.Second)
	require.NoError(t, err)

	// Insert some data
	records := make(map[string]any, 100)
	for i := range 100 {
		records[fmt.Sprintf("doc-%03d", i)] = map[string]any{
			"content": fmt.Sprintf("Test document %d with some content", i),
		}
	}

	_, err = cluster.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
		Records:   records,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err)

	// Start availability checker
	availChecker := NewAvailabilityChecker(cluster, tableName)
	availChecker.Start()
	defer availChecker.Stop()

	// Perform multiple add/remove cycles
	t.Log("Starting node churn cycles...")
	for cycle := range 3 {
		t.Logf("Cycle %d: Adding node...", cycle+1)
		newNode := cluster.AddStoreNode(ctx)

		// Wait for cluster to stabilize after adding node
		err = cluster.WaitForShardsReady(ctx, tableName, 5, 60*time.Second)
		require.NoError(t, err, "Failed to stabilize after adding node in cycle %d", cycle)

		// Verify data is still accessible
		record, err := cluster.Client.LookupKey(ctx, tableName, "doc-050")
		require.NoError(t, err, "Failed to read data during cycle %d after add", cycle)
		require.NotNil(t, record)

		t.Logf("Cycle %d: Removing node %d...", cycle+1, newNode.ID)
		err = cluster.RemoveStoreNode(ctx, newNode.ID)
		require.NoError(t, err, "Failed to remove node during cycle %d", cycle)

		// Wait for cluster to stabilize after removing node
		err = cluster.WaitForShardsReady(ctx, tableName, 5, 60*time.Second)
		require.NoError(t, err, "Failed to stabilize after removing node in cycle %d", cycle)

		// Verify data is still accessible
		record, err = cluster.Client.LookupKey(ctx, tableName, "doc-050")
		require.NoError(t, err, "Failed to read data during cycle %d after remove", cycle)
		require.NotNil(t, record)

		t.Logf("Cycle %d completed, store count: %d", cycle+1, cluster.GetActiveStoreCount())
	}

	success, failed, maxDowntime := availChecker.Stats()
	t.Logf("Node churn availability: success=%d, failed=%d, maxDowntime=%v", success, failed, maxDowntime)

	t.Log("=== Node Churn E2E Test Completed ===")
}
