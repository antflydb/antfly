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
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

const (
	crossTableA    = "cross_table_a"
	crossTableB    = "cross_table_b"
	crossNumShards = 4
)

// setupTwoTableCluster creates a cluster with two tables for cross-table tests.
func setupTwoTableCluster(t *testing.T) *TestCluster {
	t.Helper()
	ctx := testContext(t, 5*time.Minute)

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:     2,
		NumShards:         crossNumShards,
		ReplicationFactor: 1,
		DisableShardAlloc: false,
	})
	t.Cleanup(cluster.Cleanup)

	err := cluster.Client.CreateTable(ctx, crossTableA, antfly.CreateTableRequest{
		NumShards: crossNumShards,
	})
	require.NoError(t, err, "Failed to create table A")
	err = cluster.WaitForShardsReady(ctx, crossTableA, crossNumShards, 60*time.Second)
	require.NoError(t, err, "Table A shards not ready")

	err = cluster.Client.CreateTable(ctx, crossTableB, antfly.CreateTableRequest{
		NumShards: crossNumShards,
	})
	require.NoError(t, err, "Failed to create table B")
	err = cluster.WaitForShardsReady(ctx, crossTableB, crossNumShards, 60*time.Second)
	require.NoError(t, err, "Table B shards not ready")

	return cluster
}

// TestE2E_CrossTableTransaction_Commit verifies that MultiBatch atomically
// writes to two tables in a single transaction.
func TestE2E_CrossTableTransaction_Commit(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 5*time.Minute)
	cluster := setupTwoTableCluster(t)

	t.Log("Performing cross-table MultiBatch insert...")
	result, err := cluster.Client.MultiBatch(ctx, antfly.MultiBatchRequest{
		Tables: map[string]antfly.BatchRequest{
			crossTableA: {
				Inserts: map[string]any{
					"user:1": map[string]any{"name": "Alice", "email": "alice@example.com"},
					"user:2": map[string]any{"name": "Bob", "email": "bob@example.com"},
				},
				SyncLevel: antfly.SyncLevelWrite,
			},
			crossTableB: {
				Inserts: map[string]any{
					"order:1": map[string]any{"user": "user:1", "item": "widget", "qty": 5},
					"order:2": map[string]any{"user": "user:2", "item": "gadget", "qty": 3},
				},
				SyncLevel: antfly.SyncLevelWrite,
			},
		},
	})
	require.NoError(t, err, "MultiBatch failed")
	require.NotNil(t, result)

	t.Log("Verifying table A documents...")
	doc, err := cluster.Client.LookupKey(ctx, crossTableA, "user:1")
	require.NoError(t, err)
	require.Equal(t, "Alice", doc["name"])

	doc, err = cluster.Client.LookupKey(ctx, crossTableA, "user:2")
	require.NoError(t, err)
	require.Equal(t, "Bob", doc["name"])

	t.Log("Verifying table B documents...")
	doc, err = cluster.Client.LookupKey(ctx, crossTableB, "order:1")
	require.NoError(t, err)
	require.Equal(t, "widget", doc["item"])

	doc, err = cluster.Client.LookupKey(ctx, crossTableB, "order:2")
	require.NoError(t, err)
	require.Equal(t, "gadget", doc["item"])
}

// TestE2E_CrossTableTransaction_MixedOps verifies atomicity of mixed insert+delete
// across two tables.
func TestE2E_CrossTableTransaction_MixedOps(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 5*time.Minute)
	cluster := setupTwoTableCluster(t)

	// Pre-populate a document to delete
	t.Log("Pre-populating document in table B...")
	_, err := cluster.Client.Batch(ctx, crossTableB, antfly.BatchRequest{
		Inserts: map[string]any{
			"order:old": map[string]any{"user": "user:0", "item": "legacy", "qty": 1},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err)

	t.Log("Performing cross-table MultiBatch with insert (A) + delete (B)...")
	_, err = cluster.Client.MultiBatch(ctx, antfly.MultiBatchRequest{
		Tables: map[string]antfly.BatchRequest{
			crossTableA: {
				Inserts: map[string]any{
					"user:3": map[string]any{"name": "Charlie"},
				},
				SyncLevel: antfly.SyncLevelWrite,
			},
			crossTableB: {
				Deletes:   []string{"order:old"},
				SyncLevel: antfly.SyncLevelWrite,
			},
		},
	})
	require.NoError(t, err, "MultiBatch mixed ops failed")

	t.Log("Verifying insert applied in table A...")
	doc, err := cluster.Client.LookupKey(ctx, crossTableA, "user:3")
	require.NoError(t, err)
	require.Equal(t, "Charlie", doc["name"])

	t.Log("Verifying delete applied in table B...")
	_, err = cluster.Client.LookupKey(ctx, crossTableB, "order:old")
	require.Error(t, err, "Expected order:old to be deleted")
}

// TestE2E_CrossTableTransaction_AbortOnInvalid verifies that an invalid
// document in one table causes the entire cross-table batch to fail,
// leaving both tables unmodified.
func TestE2E_CrossTableTransaction_AbortOnInvalid(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 5*time.Minute)
	cluster := setupTwoTableCluster(t)

	t.Log("Attempting MultiBatch with nonexistent table (should fail)...")
	_, err := cluster.Client.MultiBatch(ctx, antfly.MultiBatchRequest{
		Tables: map[string]antfly.BatchRequest{
			crossTableA: {
				Inserts: map[string]any{
					"user:phantom": map[string]any{"name": "Phantom"},
				},
				SyncLevel: antfly.SyncLevelWrite,
			},
			"nonexistent_table": {
				Inserts: map[string]any{
					"key:1": map[string]any{"data": "should fail"},
				},
				SyncLevel: antfly.SyncLevelWrite,
			},
		},
	})
	require.Error(t, err, "MultiBatch should fail with nonexistent table")

	t.Log("Verifying no writes landed in table A...")
	_, err = cluster.Client.LookupKey(ctx, crossTableA, "user:phantom")
	require.Error(t, err, "Expected user:phantom to not exist after failed transaction")
}
