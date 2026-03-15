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
	"os"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// setupTestDBWithGraphIndex creates a test DB with a graph index configured
func setupTestDBWithGraphIndex(t *testing.T) (*DBImpl, string) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger: lg,
	}

	// Open the database
	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))

	// Create schema
	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title":   map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Create graph index config
	edgeTypes := []indexes.EdgeTypeConfig{
		{
			Name:             "cites",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: &[]string{},
		},
		{
			Name:             "similar_to",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   true,
			RequiredMetadata: &[]string{},
		},
		{
			Name:             "links",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: &[]string{},
		},
		{
			Name:             "authored_by",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: &[]string{},
		},
	}
	graphConfig := indexes.GraphIndexConfig{
		EdgeTypes:           &edgeTypes,
		MaxEdgesPerDocument: 100,
	}

	idxCfg, err := indexes.NewIndexConfig("citations", graphConfig)
	require.NoError(t, err)

	// Initialize index manager
	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	// Initialize db.indexes map before adding indexes
	db.indexes = make(map[string]indexes.IndexConfig)
	db.indexesMu.Lock()
	db.indexes["citations"] = *idxCfg
	db.indexesMu.Unlock()

	// Register the index with index manager and start it
	require.NoError(t, indexManager.Register("citations", false, *idxCfg))
	require.NoError(t, indexManager.Start(false))

	return db, dir
}

func TestGraphEdges_DeclarativeManagement(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Test 1: Create document with edges
	t.Run("create document with edges", func(t *testing.T) {
		doc := map[string]any{
			"title": "Paper A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{
							"target": "paper_b",
							"weight": 1.0,
						},
						map[string]any{
							"target": "paper_c",
							"weight": 0.8,
						},
					},
				},
			},
		}

		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Wait for async index processing
		time.Sleep(500 * time.Millisecond)

		// Verify edges were created
		edges, err := db.GetEdges(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 2)

		// Check edge details
		targets := make(map[string]float64)
		for _, edge := range edges {
			targets[string(edge.Target)] = edge.Weight
		}
		assert.Equal(t, 1.0, targets["paper_b"])
		assert.Equal(t, 0.8, targets["paper_c"])
	})

	// Test 2: Update edges (add new, keep existing)
	t.Run("update edges adding new edge", func(t *testing.T) {
		doc := map[string]any{
			"title": "Paper A Updated",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{
							"target": "paper_b",
							"weight": 1.0,
						},
						map[string]any{
							"target": "paper_c",
							"weight": 0.8,
						},
						map[string]any{
							"target": "paper_d",
							"weight": 0.9,
						},
					},
				},
			},
		}

		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify edges
		edges, err := db.GetEdges(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 3)
	})

	// Test 3: Edge reconciliation (remove edges)
	t.Run("remove edges via reconciliation", func(t *testing.T) {
		doc := map[string]any{
			"title": "Paper A Final",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{
							"target": "paper_b",
							"weight": 1.0,
						},
					},
				},
			},
		}

		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify only paper_b edge remains
		edges, err := db.GetEdges(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 1)
		assert.Equal(t, "paper_b", string(edges[0].Target))
	})

	// Test 4: Delete all edges
	t.Run("delete all edges", func(t *testing.T) {
		doc := map[string]any{
			"title": "Paper A No Edges",
			"_edges": map[string]any{
				"citations": map[string]any{},
			},
		}

		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify no edges
		edges, err := db.GetEdges(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Empty(t, edges)
	})
}

func TestGraphEdges_IncomingEdges(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create document paper_a with edge to paper_b
	docA := map[string]any{
		"title": "Paper A",
		"_edges": map[string]any{
			"citations": map[string]any{
				"cites": []any{
					map[string]any{
						"target": "paper_b",
						"weight": 1.0,
					},
				},
			},
		},
	}

	docAJSON, err := json.Marshal(docA)
	require.NoError(t, err)

	err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docAJSON}}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Wait for async index processing with polling
	// The edge needs to go through: write -> WAL buffer -> index manager background worker -> graph index batch -> indexDB
	// Note: indexmgr has a 30s timer, so we need to wait at least 35s for processing
	var incomingEdges []indexes.Edge
	maxWait := 45 * time.Second // Must be > 30s due to indexmgr defaultWait timer
	pollInterval := 200 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		incomingEdges, err = db.GetEdges(ctx, "citations", []byte("paper_b"), "", indexes.EdgeDirectionIn)
		require.NoError(t, err)
		if len(incomingEdges) > 0 {
			break
		}
		time.Sleep(pollInterval)
	}

	require.Len(t, incomingEdges, 1, "incoming edge should be indexed within %v", maxWait)
	assert.Equal(t, "paper_a", string(incomingEdges[0].Source))
	assert.Equal(t, "paper_b", string(incomingEdges[0].Target))
	assert.Equal(t, "cites", incomingEdges[0].Type)
}

func TestGraphTraversal_BFS(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create a citation graph:
	// paper_a -> paper_b -> paper_c
	//         -> paper_d
	docs := map[string]map[string]any{
		"paper_a": {
			"title": "Paper A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper_b", "weight": 1.0},
						map[string]any{"target": "paper_d", "weight": 0.8},
					},
				},
			},
		},
		"paper_b": {
			"title": "Paper B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper_c", "weight": 1.0},
					},
				},
			},
		},
		"paper_c": {
			"title": "Paper C",
		},
		"paper_d": {
			"title": "Paper D",
		},
	}

	// Write all documents
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	err := db.Batch(ctx, batch, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Wait for async index processing
	time.Sleep(1 * time.Second)

	// Test 1: 1-hop traversal
	t.Run("1-hop traversal", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"cites"},
			Direction:  indexes.EdgeDirectionOut,
			MaxDepth:   1,
			MaxResults: 100,
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("paper_a"), rules)
		require.NoError(t, err)
		assert.Len(t, results, 2)

		// Check we got paper_b and paper_d
		keys := make(map[string]bool)
		for _, r := range results {
			keys[string(r.Key)] = true
		}
		assert.True(t, keys["paper_b"])
		assert.True(t, keys["paper_d"])
	})

	// Test 2: 2-hop traversal
	t.Run("2-hop traversal", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"cites"},
			Direction:  indexes.EdgeDirectionOut,
			MaxDepth:   2,
			MaxResults: 100,
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("paper_a"), rules)
		require.NoError(t, err)
		assert.Len(t, results, 3) // paper_b, paper_d, paper_c

		// Check depths
		depthMap := make(map[string]int)
		for _, r := range results {
			depthMap[string(r.Key)] = r.Depth
		}
		assert.Equal(t, 1, depthMap["paper_b"])
		assert.Equal(t, 1, depthMap["paper_d"])
		assert.Equal(t, 2, depthMap["paper_c"])
	})

	// Test 3: Max results limit
	t.Run("max results limit", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"cites"},
			Direction:  indexes.EdgeDirectionOut,
			MaxDepth:   2,
			MaxResults: 2,
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("paper_a"), rules)
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})
}

func TestGraphNeighbors(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create a simple graph
	doc := map[string]any{
		"title": "Paper A",
		"_edges": map[string]any{
			"citations": map[string]any{
				"cites": []any{
					map[string]any{"target": "paper_b", "weight": 1.0},
					map[string]any{"target": "paper_c", "weight": 0.8},
				},
				"similar_to": []any{
					map[string]any{"target": "paper_d", "weight": 0.9},
				},
			},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Test 1: Get all neighbors
	t.Run("all neighbors", func(t *testing.T) {
		neighbors, err := db.GetNeighbors(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, neighbors, 3) // paper_b, paper_c, paper_d
	})

	// Test 2: Get neighbors by edge type
	t.Run("neighbors filtered by edge type", func(t *testing.T) {
		neighbors, err := db.GetNeighbors(ctx, "citations", []byte("paper_a"), "cites", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, neighbors, 2) // paper_b, paper_c

		keys := make(map[string]bool)
		for _, n := range neighbors {
			keys[string(n.Key)] = true
		}
		assert.True(t, keys["paper_b"])
		assert.True(t, keys["paper_c"])
	})
}

func TestGraphEdges_SpecialFieldsOptimization(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Test: Update only edges (should skip document write)
	t.Run("update only edges", func(t *testing.T) {
		// First create a document
		initialDoc := map[string]any{
			"title": "Paper A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper_b", "weight": 1.0},
					},
				},
			},
		}

		docJSON, err := json.Marshal(initialDoc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Now update only edges (no title change)
		edgesOnlyDoc := map[string]any{
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper_c", "weight": 0.9},
					},
				},
			},
		}

		edgesOnlyJSON, err := json.Marshal(edgesOnlyDoc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), edgesOnlyJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		// Verify edges were updated
		edges, err := db.GetEdges(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 1)
		assert.Equal(t, "paper_c", string(edges[0].Target))

		// Verify original document is still intact
		retrievedDoc, err := db.Get(ctx, []byte("paper_a"))
		require.NoError(t, err)
		assert.Equal(t, "Paper A", retrievedDoc["title"])
	})
}

// TestGraphEdges_ErrHandling tests error handling in graph operations
func TestGraphEdges_ErrHandling(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	t.Run("non-existent index", func(t *testing.T) {
		_, err := db.GetEdges(ctx, "nonexistent_index", []byte("key"), "", indexes.EdgeDirectionOut)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "index not found")
	})

	t.Run("invalid direction value", func(t *testing.T) {
		// Should still work as the function accepts string EdgeDirection
		edges, err := db.GetEdges(ctx, "citations", []byte("key"), "", indexes.EdgeDirection("invalid"))
		require.NoError(t, err) // Won't error, just won't match anything
		assert.Empty(t, edges)
	})

	t.Run("empty key", func(t *testing.T) {
		edges, err := db.GetEdges(ctx, "citations", []byte(""), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Empty(t, edges)
	})

	t.Run("nil key", func(t *testing.T) {
		edges, err := db.GetEdges(ctx, "citations", nil, "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Empty(t, edges)
	})
}

// TestGraphEdges_WeightAndMetadata tests edge weight validation and metadata storage
func TestGraphEdges_WeightAndMetadata(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	t.Run("store and retrieve edge weights", func(t *testing.T) {
		doc := map[string]any{
			"title": "Paper A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper_b", "weight": 1.0},
						map[string]any{"target": "paper_c", "weight": 0.5},
						map[string]any{"target": "paper_d", "weight": 0.1},
					},
				},
			},
		}

		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		edges, err := db.GetEdges(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 3)

		// Verify weights
		weights := make(map[string]float64)
		for _, edge := range edges {
			weights[string(edge.Target)] = edge.Weight
		}
		assert.Equal(t, 1.0, weights["paper_b"])
		assert.Equal(t, 0.5, weights["paper_c"])
		assert.Equal(t, 0.1, weights["paper_d"])
	})

	t.Run("store and retrieve edge metadata", func(t *testing.T) {
		doc := map[string]any{
			"title": "Paper A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{
							"target": "paper_x",
							"weight": 1.0,
							"metadata": map[string]any{
								"year":    2023,
								"context": "introduction",
								"page":    5,
							},
						},
					},
				},
			},
		}

		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_a"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		edges, err := db.GetEdges(ctx, "citations", []byte("paper_a"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		require.Len(t, edges, 1)

		edge := edges[0]
		assert.Equal(t, "paper_x", string(edge.Target))
		assert.NotNil(t, edge.Metadata)
		assert.Equal(t, float64(2023), edge.Metadata["year"])
		assert.Equal(t, "introduction", edge.Metadata["context"])
		assert.Equal(t, float64(5), edge.Metadata["page"])
	})

	t.Run("default weight when not specified", func(t *testing.T) {
		doc := map[string]any{
			"title": "Paper B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper_y"}, // No weight specified
					},
				},
			},
		}

		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte("paper_b"), docJSON}}, nil, Op_SyncLevelWrite)
		require.NoError(t, err)

		edges, err := db.GetEdges(ctx, "citations", []byte("paper_b"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		require.Len(t, edges, 1)

		// Default weight should be 1.0
		assert.Equal(t, 1.0, edges[0].Weight)
	})
}

// TestGraphTraversal_Advanced tests advanced traversal features
func TestGraphTraversal_Advanced(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create a more complex graph with weights
	docs := map[string]map[string]any{
		"a": {
			"title": "Node A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "b", "weight": 0.9},
						map[string]any{"target": "c", "weight": 0.3},
					},
				},
			},
		},
		"b": {
			"title": "Node B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "d", "weight": 0.8},
					},
				},
			},
		},
		"c": {
			"title": "Node C",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "d", "weight": 0.2},
					},
				},
			},
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	err := db.Batch(ctx, batch, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Wait for async index processing
	time.Sleep(1 * time.Second)

	t.Run("weight-based filtering", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"links"},
			Direction:  indexes.EdgeDirectionOut,
			MaxDepth:   2,
			MinWeight:  0.5, // Only edges with weight >= 0.5
			MaxWeight:  1.0,
			MaxResults: 100,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("a"), rules)
		require.NoError(t, err)

		// Should get: b (weight 0.9) and d (weight 0.8 via b)
		// Should NOT get: c (weight 0.3 < 0.5)
		keys := make(map[string]bool)
		for _, r := range results {
			keys[string(r.Key)] = true
		}
		assert.True(t, keys["b"], "Should include b (high weight)")
		assert.True(t, keys["d"], "Should include d (via high weight path)")
		assert.False(t, keys["c"], "Should not include c (low weight)")
	})

	t.Run("path inclusion", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:    []string{"links"},
			Direction:    indexes.EdgeDirectionOut,
			MaxDepth:     2,
			MaxResults:   100,
			MinWeight:    0.0,
			MaxWeight:    1.0,
			IncludePaths: true,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("a"), rules)
		require.NoError(t, err)

		// Find node d and check its path
		var nodeD *indexes.TraversalResult
		for _, r := range results {
			if string(r.Key) == "d" {
				nodeD = r
				break
			}
		}

		require.NotNil(t, nodeD, "Should find node d")
		assert.Equal(t, 2, nodeD.Depth, "Node d should be at depth 2")

		if nodeD.Path != nil {
			assert.NotEmpty(t, nodeD.Path, "Path should be included")
		}
	})

	t.Run("deduplication enabled", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:        []string{"links"},
			Direction:        indexes.EdgeDirectionOut,
			MaxDepth:         2,
			MaxResults:       100,
			MinWeight:        0.0,
			MaxWeight:        1.0,
			DeduplicateNodes: true,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("a"), rules)
		require.NoError(t, err)

		// Count occurrences of node d (reachable via both b and c)
		count := 0
		for _, r := range results {
			if string(r.Key) == "d" {
				count++
			}
		}

		assert.Equal(t, 1, count, "Node d should appear only once (deduplicated)")
	})

	t.Run("max results limiting at various depths", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"links"},
			Direction:  indexes.EdgeDirectionOut,
			MaxDepth:   2,
			MaxResults: 2, // Limit to 2 results
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("a"), rules)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results), 2, "Should respect max_results limit")
	})
}

// TestGraphTraversal_Cycles tests cycle detection and handling
func TestGraphTraversal_Cycles(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create a graph with cycles: a -> b -> c -> a
	docs := map[string]map[string]any{
		"a": {
			"title": "Node A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "b", "weight": 1.0},
					},
				},
			},
		},
		"b": {
			"title": "Node B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"c": {
			"title": "Node C",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "a", "weight": 1.0}, // Cycle back to a
					},
				},
			},
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	err := db.Batch(ctx, batch, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Wait for async index processing
	time.Sleep(1 * time.Second)

	t.Run("cycle handling with deduplication", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:        []string{"links"},
			Direction:        indexes.EdgeDirectionOut,
			MaxDepth:         5, // Deep enough to hit the cycle
			MaxResults:       100,
			MinWeight:        0.0,
			MaxWeight:        1.0,
			DeduplicateNodes: true,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("a"), rules)
		require.NoError(t, err)

		// With deduplication, each node should appear only once
		keys := make(map[string]int)
		for _, r := range results {
			keys[string(r.Key)]++
		}

		assert.Equal(t, 1, keys["b"], "Node b should appear once")
		assert.Equal(t, 1, keys["c"], "Node c should appear once")
		// Node a is the start node, so it won't appear in results
	})

	t.Run("cycle without deduplication", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:        []string{"links"},
			Direction:        indexes.EdgeDirectionOut,
			MaxDepth:         3,
			MaxResults:       100,
			MinWeight:        0.0,
			MaxWeight:        1.0,
			DeduplicateNodes: false,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("a"), rules)
		require.NoError(t, err)

		// Without deduplication, nodes may appear multiple times
		// At depth 3: a -> b -> c -> a
		assert.NotEmpty(t, results, "Should have results")
	})
}

// TestGraphEdges_MultipleEdgeTypes tests handling of multiple edge types
func TestGraphEdges_MultipleEdgeTypes(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	doc := map[string]any{
		"title": "Multi-edge Node",
		"_edges": map[string]any{
			"citations": map[string]any{
				"cites": []any{
					map[string]any{"target": "paper_a", "weight": 1.0},
					map[string]any{"target": "paper_b", "weight": 0.9},
				},
				"similar_to": []any{
					map[string]any{"target": "paper_c", "weight": 0.8},
					map[string]any{"target": "paper_d", "weight": 0.7},
				},
				"authored_by": []any{
					map[string]any{"target": "author_x", "weight": 1.0},
				},
			},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	err = db.Batch(ctx, [][2][]byte{{[]byte("main"), docJSON}}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Wait for async index processing
	time.Sleep(1 * time.Second)

	t.Run("get all edge types", func(t *testing.T) {
		edges, err := db.GetEdges(ctx, "citations", []byte("main"), "", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 5, "Should get all edges regardless of type")
	})

	t.Run("filter by specific edge type", func(t *testing.T) {
		edges, err := db.GetEdges(ctx, "citations", []byte("main"), "cites", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		assert.Len(t, edges, 2, "Should only get 'cites' edges")

		for _, edge := range edges {
			assert.Equal(t, "cites", edge.Type)
		}
	})

	t.Run("traverse specific edge types", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"cites", "similar_to"}, // Only these two types
			Direction:  indexes.EdgeDirectionOut,
			MaxDepth:   1,
			MaxResults: 100,
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("main"), rules)
		require.NoError(t, err)
		assert.Len(t, results, 4, "Should get 4 nodes (2 cites + 2 similar_to)")
	})
}

// TestGraphEdges_BidirectionalTraversal tests bidirectional graph traversal
func TestGraphEdges_BidirectionalTraversal(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create a bidirectional graph structure
	docs := map[string]map[string]any{
		"center": {
			"title": "Center Node",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "out1", "weight": 1.0},
						map[string]any{"target": "out2", "weight": 1.0},
					},
				},
			},
		},
		"in1": {
			"title": "Incoming 1",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "center", "weight": 1.0},
					},
				},
			},
		},
		"in2": {
			"title": "Incoming 2",
			"_edges": map[string]any{
				"citations": map[string]any{
					"links": []any{
						map[string]any{"target": "center", "weight": 1.0},
					},
				},
			},
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	err := db.Batch(ctx, batch, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Wait for async index processing with polling
	// Incoming edges need to go through: write -> WAL buffer -> index manager background worker -> graph index batch -> indexDB
	// Note: indexmgr has a 30s timer, so we need to wait at least 35s for processing
	var incomingEdges []indexes.Edge
	maxWait := 45 * time.Second // Must be > 30s due to indexmgr defaultWait timer
	pollInterval := 200 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		incomingEdges, err = db.GetEdges(ctx, "citations", []byte("center"), "links", indexes.EdgeDirectionIn)
		require.NoError(t, err)
		// We expect 2 incoming edges (from in1 and in2)
		if len(incomingEdges) >= 2 {
			break
		}
		time.Sleep(pollInterval)
	}

	require.Len(t, incomingEdges, 2, "incoming edges should be indexed within %v", maxWait)

	t.Run("outgoing only", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"links"},
			Direction:  indexes.EdgeDirectionOut,
			MaxDepth:   1,
			MaxResults: 100,
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("center"), rules)
		require.NoError(t, err)
		assert.Len(t, results, 2, "Should get 2 outgoing nodes")

		keys := make(map[string]bool)
		for _, r := range results {
			keys[string(r.Key)] = true
		}
		assert.True(t, keys["out1"])
		assert.True(t, keys["out2"])
	})

	t.Run("incoming only", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"links"},
			Direction:  indexes.EdgeDirectionIn,
			MaxDepth:   1,
			MaxResults: 100,
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("center"), rules)
		require.NoError(t, err)
		assert.Len(t, results, 2, "Should get 2 incoming nodes")

		keys := make(map[string]bool)
		for _, r := range results {
			keys[string(r.Key)] = true
		}
		assert.True(t, keys["in1"])
		assert.True(t, keys["in2"])
	})

	t.Run("both directions", func(t *testing.T) {
		rules := indexes.TraversalRules{
			EdgeTypes:  []string{"links"},
			Direction:  indexes.EdgeDirectionBoth,
			MaxDepth:   1,
			MaxResults: 100,
			MinWeight:  0.0,
			MaxWeight:  1.0,
		}

		results, err := db.TraverseEdges(ctx, "citations", []byte("center"), rules)
		require.NoError(t, err)
		assert.Len(t, results, 4, "Should get 4 nodes (2 outgoing + 2 incoming)")

		keys := make(map[string]bool)
		for _, r := range results {
			keys[string(r.Key)] = true
		}
		assert.True(t, keys["out1"])
		assert.True(t, keys["out2"])
		assert.True(t, keys["in1"])
		assert.True(t, keys["in2"])
	})
}
