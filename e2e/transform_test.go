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
	"sync"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// Test configuration constants
const (
	transformTestTableName = "transform_test_table"
	transformTestNumShards = 4
)

// TestE2E_Transform_MaxKeepsLatestValue tests that using $max operator
// ensures the latest (highest) version value is always kept, regardless
// of operation order.
func TestE2E_Transform_MaxKeepsLatestValue(t *testing.T) {
	skipUnlessEnv(t, "RUN_TRANSFORM_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, transformTestTableName, transformTestNumShards)

	// Insert initial document with version 5
	t.Log("Inserting initial document with version 5...")
	initialDoc := map[string]any{
		"name":    "test-item",
		"version": 5,
		"data":    "initial data",
	}
	_, err := cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Inserts:   map[string]any{"item-1": initialDoc},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert initial document")

	err = cluster.WaitForKeyAvailable(ctx, transformTestTableName, "item-1", 10*time.Second)
	require.NoError(t, err, "Key not available")

	doc, err := cluster.Client.LookupKey(ctx, transformTestTableName, "item-1")
	require.NoError(t, err, "Failed to lookup initial document")
	require.Equal(t, float64(5), doc["version"], "Initial version should be 5")

	// Use $max to try updating with lower version (should be ignored)
	t.Log("Attempting to update with lower version 3 using $max (should be ignored)...")
	_, err = cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Transforms: []antfly.Transform{
			{
				Key: "item-1",
				Operations: []antfly.TransformOp{
					{Op: antfly.TransformOpTypeMax, Path: "version", Value: 3},
					{Op: antfly.TransformOpTypeSet, Path: "data", Value: "updated with v3"},
				},
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Transform failed")

	doc, err = cluster.Client.LookupKey(ctx, transformTestTableName, "item-1")
	require.NoError(t, err, "Failed to lookup document after low version update")
	require.Equal(t, float64(5), doc["version"], "Version should still be 5 after $max with 3")
	// Data should have been updated even though version wasn't (operations are independent)
	require.Equal(t, "updated with v3", doc["data"], "Data should be updated")

	// Use $max to update with higher version (should be applied)
	t.Log("Updating with higher version 10 using $max (should be applied)...")
	_, err = cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Transforms: []antfly.Transform{
			{
				Key: "item-1",
				Operations: []antfly.TransformOp{
					{Op: antfly.TransformOpTypeMax, Path: "version", Value: 10},
					{Op: antfly.TransformOpTypeSet, Path: "data", Value: "updated with v10"},
				},
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Transform failed")

	doc, err = cluster.Client.LookupKey(ctx, transformTestTableName, "item-1")
	require.NoError(t, err, "Failed to lookup document after high version update")
	require.Equal(t, float64(10), doc["version"], "Version should be 10 after $max with 10")
	require.Equal(t, "updated with v10", doc["data"], "Data should be updated")
}

// TestE2E_Transform_ConcurrentMaxUpdates tests that concurrent updates
// using $max all converge to the highest version value.
func TestE2E_Transform_ConcurrentMaxUpdates(t *testing.T) {
	skipUnlessEnv(t, "RUN_TRANSFORM_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, transformTestTableName, transformTestNumShards)

	// Insert initial document with version 0
	t.Log("Inserting initial document with version 0...")
	_, err := cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Inserts: map[string]any{
			"concurrent-item": map[string]any{
				"name":    "concurrent-test",
				"version": 0,
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert initial document")

	err = cluster.WaitForKeyAvailable(ctx, transformTestTableName, "concurrent-item", 10*time.Second)
	require.NoError(t, err, "Key not available")

	// Launch concurrent updates with different versions
	t.Log("Launching concurrent $max updates with versions 1-20...")
	numUpdates := 20
	var wg sync.WaitGroup

	for version := 1; version <= numUpdates; version++ {
		wg.Go(func() {
			_, err := cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
				Transforms: []antfly.Transform{
					{
						Key: "concurrent-item",
						Operations: []antfly.TransformOp{
							{Op: antfly.TransformOpTypeMax, Path: "version", Value: version},
						},
					},
				},
				SyncLevel: antfly.SyncLevelWrite,
			})
			if err != nil {
				t.Logf("Update with version %d failed: %v", version, err)
			}
		})
	}

	wg.Wait()

	// Verify the final version is the maximum (20)
	t.Log("Verifying final version is 20...")
	doc, err := cluster.Client.LookupKey(ctx, transformTestTableName, "concurrent-item")
	require.NoError(t, err, "Failed to lookup document after concurrent updates")
	require.Equal(t, float64(numUpdates), doc["version"],
		"Version should be %d after concurrent $max updates", numUpdates)
}

// TestE2E_Transform_UpsertWithMax tests that $max works correctly with upsert
// to atomically create a document with a version if it doesn't exist,
// or update the version if the incoming value is higher.
func TestE2E_Transform_UpsertWithMax(t *testing.T) {
	skipUnlessEnv(t, "RUN_TRANSFORM_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, transformTestTableName, transformTestNumShards)

	// Use upsert to create a new document with version
	t.Log("Creating new document using upsert with $max...")
	_, err := cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Transforms: []antfly.Transform{
			{
				Key:    "upsert-item",
				Upsert: true,
				Operations: []antfly.TransformOp{
					{Op: antfly.TransformOpTypeMax, Path: "version", Value: 5},
					{Op: antfly.TransformOpTypeSet, Path: "name", Value: "upserted-item"},
				},
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Upsert transform failed")

	err = cluster.WaitForKeyAvailable(ctx, transformTestTableName, "upsert-item", 10*time.Second)
	require.NoError(t, err, "Key not available after upsert")

	doc, err := cluster.Client.LookupKey(ctx, transformTestTableName, "upsert-item")
	require.NoError(t, err, "Failed to lookup upserted document")
	require.Equal(t, float64(5), doc["version"], "Version should be 5")
	require.Equal(t, "upserted-item", doc["name"], "Name should be set")

	// Upsert again with lower version (should not change version)
	t.Log("Upserting with lower version 3 (should not change version)...")
	_, err = cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Transforms: []antfly.Transform{
			{
				Key:    "upsert-item",
				Upsert: true,
				Operations: []antfly.TransformOp{
					{Op: antfly.TransformOpTypeMax, Path: "version", Value: 3},
					{Op: antfly.TransformOpTypeSet, Path: "status", Value: "updated"},
				},
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Second upsert failed")

	doc, err = cluster.Client.LookupKey(ctx, transformTestTableName, "upsert-item")
	require.NoError(t, err, "Failed to lookup document after second upsert")
	require.Equal(t, float64(5), doc["version"], "Version should still be 5")
	require.Equal(t, "updated", doc["status"], "Status should be updated")

	// Upsert with higher version (should update version)
	t.Log("Upserting with higher version 10 (should update version)...")
	_, err = cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Transforms: []antfly.Transform{
			{
				Key:    "upsert-item",
				Upsert: true,
				Operations: []antfly.TransformOp{
					{Op: antfly.TransformOpTypeMax, Path: "version", Value: 10},
				},
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Third upsert failed")

	doc, err = cluster.Client.LookupKey(ctx, transformTestTableName, "upsert-item")
	require.NoError(t, err, "Failed to lookup document after third upsert")
	require.Equal(t, float64(10), doc["version"], "Version should be 10")
}

// TestE2E_Transform_IncAtomicCounter tests that $inc operator provides
// atomic counter increments without race conditions.
func TestE2E_Transform_IncAtomicCounter(t *testing.T) {
	skipUnlessEnv(t, "RUN_TRANSFORM_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, transformTestTableName, transformTestNumShards)

	// Create document with counter at 0
	t.Log("Creating document with counter at 0...")
	_, err := cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Inserts: map[string]any{
			"counter-item": map[string]any{
				"name":    "atomic-counter",
				"counter": 0,
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to create counter document")

	err = cluster.WaitForKeyAvailable(ctx, transformTestTableName, "counter-item", 10*time.Second)
	require.NoError(t, err, "Key not available")

	// Launch concurrent increments
	t.Log("Launching 50 concurrent $inc operations...")
	numIncrements := 50
	var wg sync.WaitGroup

	for range numIncrements {
		wg.Go(func() {

			_, err := cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
				Transforms: []antfly.Transform{
					{
						Key: "counter-item",
						Operations: []antfly.TransformOp{
							{Op: antfly.TransformOpTypeInc, Path: "counter", Value: 1},
						},
					},
				},
				SyncLevel: antfly.SyncLevelWrite,
			})
			if err != nil {
				t.Logf("Increment failed: %v", err)
			}
		})
	}

	wg.Wait()

	// Verify final counter value equals number of increments
	t.Log("Verifying final counter value...")
	doc, err := cluster.Client.LookupKey(ctx, transformTestTableName, "counter-item")
	require.NoError(t, err, "Failed to lookup counter document")
	require.Equal(t, float64(numIncrements), doc["counter"],
		"Counter should be %d after %d concurrent increments", numIncrements, numIncrements)
}

// TestE2E_Transform_MultipleOperators tests combining multiple transform
// operators in a single batch to perform complex atomic updates.
func TestE2E_Transform_MultipleOperators(t *testing.T) {
	skipUnlessEnv(t, "RUN_TRANSFORM_TESTS")
	ctx := testContext(t, 3*time.Minute)

	cluster := setupClusterWithTable(t, ctx, transformTestTableName, transformTestNumShards)

	// Create initial document
	t.Log("Creating initial document...")
	_, err := cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Inserts: map[string]any{
			"multi-op-item": map[string]any{
				"name":     "multi-operator-test",
				"version":  1,
				"views":    0,
				"tags":     []string{"initial"},
				"metadata": map[string]any{"created": true},
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to create document")

	err = cluster.WaitForKeyAvailable(ctx, transformTestTableName, "multi-op-item", 10*time.Second)
	require.NoError(t, err, "Key not available")

	// Apply multiple operators atomically
	t.Log("Applying multiple operators atomically...")
	_, err = cluster.Client.Batch(ctx, transformTestTableName, antfly.BatchRequest{
		Transforms: []antfly.Transform{
			{
				Key: "multi-op-item",
				Operations: []antfly.TransformOp{
					{Op: antfly.TransformOpTypeMax, Path: "version", Value: 5},
					{Op: antfly.TransformOpTypeInc, Path: "views", Value: 1},
					{Op: antfly.TransformOpTypeAddToSet, Path: "tags", Value: "updated"},
					{Op: antfly.TransformOpTypeSet, Path: "metadata.lastUpdated", Value: "2025-01-26"},
				},
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Multi-operator transform failed")

	// Verify all operators applied correctly
	t.Log("Verifying all operations applied...")
	doc, err := cluster.Client.LookupKey(ctx, transformTestTableName, "multi-op-item")
	require.NoError(t, err, "Failed to lookup document")

	require.Equal(t, float64(5), doc["version"], "Version should be 5 ($max)")
	require.Equal(t, float64(1), doc["views"], "Views should be 1 ($inc)")

	tags, ok := doc["tags"].([]any)
	require.True(t, ok, "Tags should be an array")
	require.Len(t, tags, 2, "Should have 2 tags")
	require.Contains(t, tags, "initial", "Should contain 'initial' tag")
	require.Contains(t, tags, "updated", "Should contain 'updated' tag ($addToSet)")

	metadata, ok := doc["metadata"].(map[string]any)
	require.True(t, ok, "Metadata should be a map")
	require.Equal(t, true, metadata["created"], "Should preserve existing metadata")
	require.Equal(t, "2025-01-26", metadata["lastUpdated"], "Should have lastUpdated ($set)")
}
