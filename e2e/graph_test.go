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
	"encoding/base64"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/oapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test configuration constants for graph pattern matching
const (
	graphTestTableName = "graph_pattern_test"
	graphTestNumShards = 4
)

// setupGraphTestCluster creates a cluster with a graph-indexed table.
// Cleanup is registered via t.Cleanup.
func setupGraphTestCluster(t *testing.T, ctx context.Context) *TestCluster {
	t.Helper()

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:     2,
		NumShards:         graphTestNumShards,
		ReplicationFactor: 1,
		DisableShardAlloc: false,
	})
	t.Cleanup(cluster.Cleanup)

	// Create table with graph index
	graphConfig := oapi.IndexConfig{
		Name: "social",
		Type: oapi.IndexTypeGraph,
	}
	graphConfig.FromGraphIndexConfig(oapi.GraphIndexConfig{
		EdgeTypes: []oapi.EdgeTypeConfig{
			{Name: "KNOWS"},
			{Name: "FOLLOWS"},
			{Name: "MANAGES"},
		},
	})

	err := cluster.Client.CreateTable(ctx, graphTestTableName, antfly.CreateTableRequest{
		NumShards: graphTestNumShards,
		Indexes: map[string]oapi.IndexConfig{
			"social": graphConfig,
		},
	})
	require.NoError(t, err, "Failed to create graph table")

	err = cluster.WaitForShardsReady(ctx, graphTestTableName, graphTestNumShards, 60*time.Second)
	require.NoError(t, err, "Failed waiting for shard allocation")

	return cluster
}

// TestE2E_GraphPattern_TwoHopPattern tests a 2-hop pattern query:
// (a)-[KNOWS]->(b)-[KNOWS]->(c)
func TestE2E_GraphPattern_TwoHopPattern(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 3*time.Minute)

	cluster := setupGraphTestCluster(t, ctx)

	// Insert test data: Alice -> Bob -> Charlie
	testDocs := map[string]any{
		"0_alice": map[string]any{
			"name": "Alice",
			"_edges": map[string]any{
				"social": map[string]any{
					"KNOWS": []any{
						map[string]any{"target": "4_bob", "weight": 1.0},
					},
				},
			},
		},
		"4_bob": map[string]any{
			"name": "Bob",
			"_edges": map[string]any{
				"social": map[string]any{
					"KNOWS": []any{
						map[string]any{"target": "8_charlie", "weight": 1.0},
					},
				},
			},
		},
		"8_charlie": map[string]any{
			"name": "Charlie",
		},
	}

	t.Log("Inserting test documents with edges...")
	_, err := cluster.Client.Batch(ctx, graphTestTableName, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert test documents")

	time.Sleep(1 * time.Second)

	// Execute 2-hop pattern query: (a)-[KNOWS]->(b)-[KNOWS]->(c)
	t.Log("Executing 2-hop pattern query...")
	pattern := []oapi.PatternStep{
		{Alias: "a"},
		{
			Alias: "b",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"KNOWS"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   1,
			},
		},
		{
			Alias: "c",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"KNOWS"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   1,
			},
		},
	}

	graphQueries := map[string]oapi.GraphQuery{
		"two_hop": {
			Type:      oapi.GraphQueryTypePattern,
			IndexName: "social",
			StartNodes: oapi.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("0_alice"))},
			},
			Pattern:          pattern,
			IncludeDocuments: true,
		},
	}

	result, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:         graphTestTableName,
		GraphSearches: graphQueries,
	})
	require.NoError(t, err, "Pattern query failed")
	require.Len(t, result.Responses, 1, "Expected one query response")

	graphResult, ok := result.Responses[0].GraphResults["two_hop"]
	require.True(t, ok, "Expected two_hop graph result")
	assert.GreaterOrEqual(t, len(graphResult.Matches), 1, "Expected at least one pattern match")

	if len(graphResult.Matches) > 0 {
		match := graphResult.Matches[0]
		assert.Contains(t, match.Bindings, "a", "Expected binding for alias 'a'")
		assert.Contains(t, match.Bindings, "b", "Expected binding for alias 'b'")
		assert.Contains(t, match.Bindings, "c", "Expected binding for alias 'c'")
		t.Logf("Found match: a=%s, b=%s, c=%s",
			match.Bindings["a"].Key,
			match.Bindings["b"].Key,
			match.Bindings["c"].Key)
	}
}

// TestE2E_GraphPattern_VariableLengthPath tests variable-length path pattern:
// (start)-[*1..3]->(end)
func TestE2E_GraphPattern_VariableLengthPath(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 3*time.Minute)

	cluster := setupGraphTestCluster(t, ctx)

	// Create a chain: A -> B -> C -> D
	testDocs := map[string]any{
		"0_a": map[string]any{
			"name": "A",
			"_edges": map[string]any{
				"social": map[string]any{
					"FOLLOWS": []any{
						map[string]any{"target": "4_b", "weight": 1.0},
					},
				},
			},
		},
		"4_b": map[string]any{
			"name": "B",
			"_edges": map[string]any{
				"social": map[string]any{
					"FOLLOWS": []any{
						map[string]any{"target": "8_c", "weight": 1.0},
					},
				},
			},
		},
		"8_c": map[string]any{
			"name": "C",
			"_edges": map[string]any{
				"social": map[string]any{
					"FOLLOWS": []any{
						map[string]any{"target": "c_d", "weight": 1.0},
					},
				},
			},
		},
		"c_d": map[string]any{
			"name": "D",
		},
	}

	t.Log("Inserting test chain A -> B -> C -> D...")
	_, err := cluster.Client.Batch(ctx, graphTestTableName, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert test documents")

	time.Sleep(1 * time.Second)

	// Pattern: (start)-[*1..3]->(end)
	// Should find B (1 hop), C (2 hops), D (3 hops)
	t.Log("Executing variable-length pattern query [*1..3]...")
	pattern := []oapi.PatternStep{
		{Alias: "start"},
		{
			Alias: "end",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"FOLLOWS"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   3,
			},
		},
	}

	graphQueries := map[string]oapi.GraphQuery{
		"var_length": {
			Type:      oapi.GraphQueryTypePattern,
			IndexName: "social",
			StartNodes: oapi.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("0_a"))},
			},
			Pattern: pattern,
		},
	}

	result, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:         graphTestTableName,
		GraphSearches: graphQueries,
	})
	require.NoError(t, err, "Pattern query failed")
	require.Len(t, result.Responses, 1, "Expected one query response")

	graphResult, ok := result.Responses[0].GraphResults["var_length"]
	require.True(t, ok, "Expected var_length graph result")

	// Should find 3 matches: A->B, A->B->C, A->B->C->D
	assert.GreaterOrEqual(t, len(graphResult.Matches), 3, "Expected at least 3 matches for variable-length path")

	t.Logf("Found %d matches for variable-length pattern", len(graphResult.Matches))
}

// TestE2E_GraphPattern_CycleDetection tests cycle detection:
// (x)-[*1..5]->(x) where x is reused alias
func TestE2E_GraphPattern_CycleDetection(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 3*time.Minute)

	cluster := setupGraphTestCluster(t, ctx)

	// Create a triangle: A -> B -> C -> A
	testDocs := map[string]any{
		"0_a": map[string]any{
			"name": "A",
			"_edges": map[string]any{
				"social": map[string]any{
					"KNOWS": []any{
						map[string]any{"target": "4_b", "weight": 1.0},
					},
				},
			},
		},
		"4_b": map[string]any{
			"name": "B",
			"_edges": map[string]any{
				"social": map[string]any{
					"KNOWS": []any{
						map[string]any{"target": "8_c", "weight": 1.0},
					},
				},
			},
		},
		"8_c": map[string]any{
			"name": "C",
			"_edges": map[string]any{
				"social": map[string]any{
					"KNOWS": []any{
						map[string]any{"target": "0_a", "weight": 1.0}, // Completes the cycle
					},
				},
			},
		},
	}

	t.Log("Inserting test triangle A -> B -> C -> A...")
	_, err := cluster.Client.Batch(ctx, graphTestTableName, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert test documents")

	time.Sleep(1 * time.Second)

	// Pattern: (x)-[*1..5]->(x) - find cycles back to 'x'
	t.Log("Executing cycle detection pattern query...")
	pattern := []oapi.PatternStep{
		{Alias: "x"},
		{
			Alias: "x", // Same alias = cycle detection
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"KNOWS"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   5,
			},
		},
	}

	graphQueries := map[string]oapi.GraphQuery{
		"cycle": {
			Type:      oapi.GraphQueryTypePattern,
			IndexName: "social",
			StartNodes: oapi.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("0_a"))},
			},
			Pattern: pattern,
		},
	}

	result, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:         graphTestTableName,
		GraphSearches: graphQueries,
	})
	require.NoError(t, err, "Pattern query failed")
	require.Len(t, result.Responses, 1, "Expected one query response")

	graphResult, ok := result.Responses[0].GraphResults["cycle"]
	require.True(t, ok, "Expected cycle graph result")

	// Should find the cycle A -> B -> C -> A
	assert.GreaterOrEqual(t, len(graphResult.Matches), 1, "Expected at least 1 cycle match")

	if len(graphResult.Matches) > 0 {
		match := graphResult.Matches[0]
		t.Logf("Found cycle: %s", match.Bindings["x"].Key)
	}
}

// TestE2E_GraphPattern_DiamondPattern tests diamond pattern matching:
// A -> B -> D and A -> C -> D (multiple paths to same destination)
func TestE2E_GraphPattern_DiamondPattern(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 3*time.Minute)

	cluster := setupGraphTestCluster(t, ctx)

	// Create diamond: A -> B -> D and A -> C -> D
	testDocs := map[string]any{
		"0_a": map[string]any{
			"name": "A",
			"_edges": map[string]any{
				"social": map[string]any{
					"MANAGES": []any{
						map[string]any{"target": "4_b", "weight": 1.0},
						map[string]any{"target": "8_c", "weight": 1.0},
					},
				},
			},
		},
		"4_b": map[string]any{
			"name": "B",
			"_edges": map[string]any{
				"social": map[string]any{
					"MANAGES": []any{
						map[string]any{"target": "c_d", "weight": 1.0},
					},
				},
			},
		},
		"8_c": map[string]any{
			"name": "C",
			"_edges": map[string]any{
				"social": map[string]any{
					"MANAGES": []any{
						map[string]any{"target": "c_d", "weight": 1.0},
					},
				},
			},
		},
		"c_d": map[string]any{
			"name": "D",
		},
	}

	t.Log("Inserting diamond pattern: A -> B -> D, A -> C -> D...")
	_, err := cluster.Client.Batch(ctx, graphTestTableName, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert test documents")

	time.Sleep(1 * time.Second)

	// Pattern: (a)-[]->(middle)-[]->(d)
	// Should find two matches: via B and via C
	t.Log("Executing diamond pattern query...")
	pattern := []oapi.PatternStep{
		{Alias: "a"},
		{
			Alias: "middle",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"MANAGES"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   1,
			},
		},
		{
			Alias: "d",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"MANAGES"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   1,
			},
		},
	}

	graphQueries := map[string]oapi.GraphQuery{
		"diamond": {
			Type:      oapi.GraphQueryTypePattern,
			IndexName: "social",
			StartNodes: oapi.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("0_a"))},
			},
			Pattern:          pattern,
			IncludeDocuments: true,
		},
	}

	result, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:         graphTestTableName,
		GraphSearches: graphQueries,
	})
	require.NoError(t, err, "Pattern query failed")
	require.Len(t, result.Responses, 1, "Expected one query response")

	graphResult, ok := result.Responses[0].GraphResults["diamond"]
	require.True(t, ok, "Expected diamond graph result")

	// Should find two paths: A->B->D and A->C->D
	assert.GreaterOrEqual(t, len(graphResult.Matches), 2, "Expected at least 2 matches for diamond pattern")

	for i, match := range graphResult.Matches {
		t.Logf("Match %d: a=%s, middle=%s, d=%s",
			i+1,
			match.Bindings["a"].Key,
			match.Bindings["middle"].Key,
			match.Bindings["d"].Key)
	}
}

// TestE2E_GraphPattern_EdgeTypeFilter tests filtering by edge type
func TestE2E_GraphPattern_EdgeTypeFilter(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 3*time.Minute)

	cluster := setupGraphTestCluster(t, ctx)

	// A -[KNOWS]-> B -[FOLLOWS]-> C
	// Pattern looking for KNOWS->KNOWS should NOT match
	testDocs := map[string]any{
		"0_a": map[string]any{
			"name": "A",
			"_edges": map[string]any{
				"social": map[string]any{
					"KNOWS": []any{
						map[string]any{"target": "4_b", "weight": 1.0},
					},
				},
			},
		},
		"4_b": map[string]any{
			"name": "B",
			"_edges": map[string]any{
				"social": map[string]any{
					"FOLLOWS": []any{ // Different edge type
						map[string]any{"target": "8_c", "weight": 1.0},
					},
				},
			},
		},
		"8_c": map[string]any{
			"name": "C",
		},
	}

	t.Log("Inserting test data: A -[KNOWS]-> B -[FOLLOWS]-> C...")
	_, err := cluster.Client.Batch(ctx, graphTestTableName, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert test documents")

	time.Sleep(1 * time.Second)

	// Pattern: (a)-[KNOWS]->(b)-[KNOWS]->(c) - should NOT match
	t.Log("Executing pattern with edge type filter (KNOWS->KNOWS)...")
	pattern := []oapi.PatternStep{
		{Alias: "a"},
		{
			Alias: "b",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"KNOWS"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   1,
			},
		},
		{
			Alias: "c",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"KNOWS"}, // Second edge is FOLLOWS, not KNOWS
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   1,
			},
		},
	}

	graphQueries := map[string]oapi.GraphQuery{
		"edge_filter": {
			Type:      oapi.GraphQueryTypePattern,
			IndexName: "social",
			StartNodes: oapi.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("0_a"))},
			},
			Pattern: pattern,
		},
	}

	result, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:         graphTestTableName,
		GraphSearches: graphQueries,
	})
	require.NoError(t, err, "Pattern query failed")
	require.Len(t, result.Responses, 1, "Expected one query response")

	graphResult, ok := result.Responses[0].GraphResults["edge_filter"]
	require.True(t, ok, "Expected edge_filter graph result")

	// Should NOT find any matches because second edge is FOLLOWS not KNOWS
	assert.Empty(t, graphResult.Matches, "Expected no matches when edge type doesn't match")
}

// TestE2E_GraphPattern_MaxResultsLimit tests result limiting
func TestE2E_GraphPattern_MaxResultsLimit(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 3*time.Minute)

	cluster := setupGraphTestCluster(t, ctx)

	// Create star graph: center connected to many leaves
	testDocs := map[string]any{
		"center": map[string]any{
			"name": "Center",
			"_edges": map[string]any{
				"social": map[string]any{
					"KNOWS": []any{},
				},
			},
		},
	}

	// Add 20 leaf nodes
	edges := make([]any, 20)
	for i := range 20 {
		key := string(rune('a'+i/10)) + "_leaf_" + string(rune('0'+i%10))
		testDocs[key] = map[string]any{
			"name": "Leaf " + string(rune('0'+i)),
		}
		edges[i] = map[string]any{"target": key, "weight": 1.0}
	}
	testDocs["center"].(map[string]any)["_edges"].(map[string]any)["social"].(map[string]any)["KNOWS"] = edges

	t.Log("Inserting star graph with center and 20 leaves...")
	_, err := cluster.Client.Batch(ctx, graphTestTableName, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert test documents")

	time.Sleep(1 * time.Second)

	// Pattern with max_results=10
	t.Log("Executing pattern with max_results=10...")
	pattern := []oapi.PatternStep{
		{Alias: "center"},
		{
			Alias: "leaf",
			Edge: oapi.PatternEdgeStep{
				Types:     []string{"KNOWS"},
				Direction: oapi.EdgeDirectionOut,
				MinHops:   1,
				MaxHops:   1,
			},
		},
	}

	graphQueries := map[string]oapi.GraphQuery{
		"limited": {
			Type:      oapi.GraphQueryTypePattern,
			IndexName: "social",
			StartNodes: oapi.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("center"))},
			},
			Pattern: pattern,
			Params: oapi.GraphQueryParams{
				MaxResults: 10,
			},
		},
	}

	result, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:         graphTestTableName,
		GraphSearches: graphQueries,
	})
	require.NoError(t, err, "Pattern query failed")
	require.Len(t, result.Responses, 1, "Expected one query response")

	graphResult, ok := result.Responses[0].GraphResults["limited"]
	require.True(t, ok, "Expected limited graph result")

	// Should be limited to max_results
	assert.LessOrEqual(t, len(graphResult.Matches), 10, "Expected at most 10 matches due to max_results limit")
	t.Logf("Found %d matches (limited to 10)", len(graphResult.Matches))
}
