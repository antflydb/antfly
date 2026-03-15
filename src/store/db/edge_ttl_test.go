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

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEdgeTTLCleanup verifies that expired edges are automatically deleted
func TestEdgeTTLCleanup(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()

	// Create a graph index with 1-second TTL for testing
	// We create the config by marshaling JSON directly since GraphIndexConfig
	// doesn't have ttl_duration in the generated code yet
	var graphIndexConfig indexes.IndexConfig
	graphIndexConfigJSON := []byte(`{
		"name": "test_ttl_graph",
		"type": "graph_v0",
		"ttl_duration": "1s"
	}`)
	err := json.Unmarshal(graphIndexConfigJSON, &graphIndexConfig)
	require.NoError(t, err)

	err = db.AddIndex(graphIndexConfig)
	require.NoError(t, err)

	t.Run("edges expire after TTL duration", func(t *testing.T) {
		// Create edge with timestamp
		now := uint64(time.Now().UnixNano())
		ctx := storeutils.WithTimestamp(ctx, now)

		docKey := []byte("ttl_test_doc")
		docValue := []byte(`{
			"name": "ttl test",
			"_edges": {
				"test_ttl_graph": {
					"links_to": [
						{"target": "target1", "weight": 1.0},
						{"target": "target2", "weight": 2.0}
					]
				}
			}
		}`)

		// Write document with edges
		err := db.Batch(ctx, [][2][]byte{{docKey, docValue}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify edges exist
		edges, err := db.GetEdges(ctx, "test_ttl_graph", docKey, "links_to", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 2, "should have 2 edges initially")

		// Create cleaner and run cleanup immediately (edges should NOT be deleted yet)
		cleaner := NewEdgeTTLCleaner(db)

		// Run cleanup (should not delete - within grace period)
		// Use nil persistFunc to test direct deletion path
		deleted, err := cleaner.cleanupExpiredEdges(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, deleted, "no edges should be deleted yet")

		// Verify edges still exist
		edges, err = db.GetEdges(ctx, "test_ttl_graph", docKey, "links_to", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 2, "edges should still exist")

		// Wait for TTL + grace period to expire
		time.Sleep(1*time.Second + TTLGracePeriod + 100*time.Millisecond)

		// Run cleanup again (should delete now)
		// Use nil persistFunc to test direct deletion path
		deleted, err = cleaner.cleanupExpiredEdges(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, deleted, "both edges should be deleted after expiration")

		// Verify edges are gone
		edges, err = db.GetEdges(ctx, "test_ttl_graph", docKey, "links_to", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Empty(t, edges, "edges should be deleted")
	})

	t.Run("only expired edges are deleted", func(t *testing.T) {
		// Create two edges with different timestamps using different source documents
		// Old edge is > TTL + grace period (1s + 5s = 6s) ago
		// Using 10 seconds to have a safe margin
		oldTimestamp := uint64(time.Now().Add(-10 * time.Second).UnixNano())
		newTimestamp := uint64(time.Now().UnixNano())

		// Create old edge
		ctxOld := storeutils.WithTimestamp(ctx, oldTimestamp)
		oldDocKey := []byte("old_doc")
		docValueOld := []byte(`{
			"name": "old doc",
			"_edges": {
				"test_ttl_graph": {
					"old_link": [
						{"target": "target1", "weight": 1.0}
					]
				}
			}
		}`)
		err := db.Batch(ctxOld, [][2][]byte{{oldDocKey, docValueOld}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Create new edge
		ctxNew := storeutils.WithTimestamp(ctx, newTimestamp)
		newDocKey := []byte("new_doc")
		docValueNew := []byte(`{
			"name": "new doc",
			"_edges": {
				"test_ttl_graph": {
					"new_link": [
						{"target": "target2", "weight": 2.0}
					]
				}
			}
		}`)
		err = db.Batch(ctxNew, [][2][]byte{{newDocKey, docValueNew}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify both edges exist
		oldEdges, _ := db.GetEdges(ctx, "test_ttl_graph", oldDocKey, "old_link", indexes.EdgeDirectionOut)
		newEdges, _ := db.GetEdges(ctx, "test_ttl_graph", newDocKey, "new_link", indexes.EdgeDirectionOut)
		assert.Len(t, oldEdges, 1, "old edge should exist")
		assert.Len(t, newEdges, 1, "new edge should exist")

		// Run cleanup
		cleaner := NewEdgeTTLCleaner(db)

		// Use nil persistFunc to test direct deletion path
		deleted, err := cleaner.cleanupExpiredEdges(ctx, nil)
		require.NoError(t, err)
		assert.Positive(t, deleted, "old edge should be deleted")

		// Verify old edge is gone, new edge remains
		oldEdges, _ = db.GetEdges(ctx, "test_ttl_graph", oldDocKey, "old_link", indexes.EdgeDirectionOut)
		newEdges, _ = db.GetEdges(ctx, "test_ttl_graph", newDocKey, "new_link", indexes.EdgeDirectionOut)
		assert.Empty(t, oldEdges, "old edge should be deleted")
		assert.Len(t, newEdges, 1, "new edge should still exist")
	})
}

// TestGetEdgeTTLConfigs verifies TTL configuration extraction
func TestGetEdgeTTLConfigs(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })

	t.Run("extracts valid TTL configurations", func(t *testing.T) {
		// Add graph index with TTL
		var graphWithTTL indexes.IndexConfig
		err := json.Unmarshal([]byte(`{
			"name": "graph_with_ttl",
			"type": "graph_v0",
			"ttl_duration": "24h"
		}`), &graphWithTTL)
		require.NoError(t, err)
		err = db.AddIndex(graphWithTTL)
		require.NoError(t, err)

		// Add graph index without TTL
		var graphNoTTL indexes.IndexConfig
		err = json.Unmarshal([]byte(`{
			"name": "graph_no_ttl",
			"type": "graph_v0"
		}`), &graphNoTTL)
		require.NoError(t, err)
		err = db.AddIndex(graphNoTTL)
		require.NoError(t, err)

		// Add non-graph index (should be ignored)
		var bleveIndex indexes.IndexConfig
		err = json.Unmarshal([]byte(`{
			"name": "bleve_index",
			"type": "full_text_v0",
			"ttl_duration": "1h"
		}`), &bleveIndex)
		require.NoError(t, err)
		err = db.AddIndex(bleveIndex)
		require.NoError(t, err)

		// Get TTL configs
		configs := db.getEdgeTTLConfigs()

		// Verify only graph_with_ttl is included
		assert.Len(t, configs, 1, "should have 1 graph index with TTL")
		assert.Contains(t, configs, "graph_with_ttl")
		assert.Equal(t, 24*time.Hour, configs["graph_with_ttl"])
		assert.NotContains(t, configs, "graph_no_ttl", "graph without TTL should not be included")
		assert.NotContains(t, configs, "bleve_index", "non-graph index should not be included")
	})

	t.Run("handles invalid TTL durations", func(t *testing.T) {
		// Add graph index with invalid TTL
		var graphInvalidTTL indexes.IndexConfig
		err := json.Unmarshal([]byte(`{
			"name": "graph_invalid_ttl",
			"type": "graph_v0",
			"ttl_duration": "invalid"
		}`), &graphInvalidTTL)
		require.NoError(t, err)
		err = db.AddIndex(graphInvalidTTL)
		require.NoError(t, err)

		// Get TTL configs
		configs := db.getEdgeTTLConfigs()

		// Verify invalid config is not included
		assert.NotContains(t, configs, "graph_invalid_ttl", "invalid TTL should be ignored")
	})
}

// TestEdgeTTLCleanerStats verifies cleanup statistics
func TestEdgeTTLCleanerStats(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })

	cleaner := NewEdgeTTLCleaner(db)

	stats := cleaner.Stats()
	assert.Contains(t, stats, "total_edges_expired")
	assert.Contains(t, stats, "last_cleanup_duration_ms")
	assert.Contains(t, stats, "indexes_with_ttl")
	assert.Equal(t, int64(0), stats["total_edges_expired"])
}
