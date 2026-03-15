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
	"encoding/base64"
	"os"
	"testing"
	"time"

	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphQueryEngine_Execute_Traverse(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create test graph: a -> b -> c
	//                     a -> d
	docs := map[string]map[string]any{
		"a": {
			"title": "Node A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
						map[string]any{"target": "d", "weight": 0.8},
					},
				},
			},
		},
		"b": {
			"title": "Node B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "c", "weight": 0.9},
					},
				},
			},
		},
		"c": {"title": "Node C"},
		"d": {"title": "Node D"},
	}

	// Write documents
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond) // Wait for indexing

	engine := NewGraphQueryEngine(db, db.logger)

	t.Run("1-hop traversal", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "traverse",
			IndexName: "citations",
			Params: indexes.GraphQueryParams{
				MaxDepth:  1,
				Direction: "out",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 2, result.Total) // b and d
		assert.Len(t, result.Nodes, 2)
	})

	t.Run("2-hop traversal", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "traverse",
			IndexName: "citations",
			Params: indexes.GraphQueryParams{
				MaxDepth:  2,
				Direction: "out",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 3, result.Total) // b, c, d
		assert.Len(t, result.Nodes, 3)
	})

	t.Run("with include_documents", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:             "traverse",
			IndexName:        "citations",
			IncludeDocuments: true,
			Params: indexes.GraphQueryParams{
				MaxDepth:  1,
				Direction: "out",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 2, result.Total)

		// Check documents are included
		for _, node := range result.Nodes {
			assert.NotNil(t, node.Document)
			assert.NotEmpty(t, node.Document["title"])
		}
	})

	t.Run("with field projection", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:             "traverse",
			IndexName:        "citations",
			IncludeDocuments: true,
			Fields:           []string{"title"},
			Params: indexes.GraphQueryParams{
				MaxDepth:  1,
				Direction: "out",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)

		// Check only title field is included
		for _, node := range result.Nodes {
			assert.NotNil(t, node.Document)
			assert.Contains(t, node.Document, "title")
			assert.Len(t, node.Document, 1)
		}
	})

	t.Run("with include_edges", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:         "traverse",
			IndexName:    "citations",
			IncludeEdges: true,
			Params: indexes.GraphQueryParams{
				MaxDepth:  1,
				Direction: "out",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)

		// Check edges are included for each node
		foundEdges := false
		for _, node := range result.Nodes {
			if len(node.Edges) > 0 {
				foundEdges = true
				// Verify edge structure
				for _, edge := range node.Edges {
					assert.NotEmpty(t, edge.Type)
					assert.NotEmpty(t, edge.Source)
					assert.NotEmpty(t, edge.Target)
				}
			}
		}
		assert.True(t, foundEdges, "Expected at least one node to have edges")
	})
}

func TestGraphQueryEngine_Execute_Neighbors(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create test data
	doc := map[string]any{
		"title": "Central Node",
		"_edges": map[string]any{
			"citations": map[string]any{
				"cites": []any{
					map[string]any{"target": "neighbor1", "weight": 1.0},
					map[string]any{"target": "neighbor2", "weight": 0.8},
				},
				"similar_to": []any{
					map[string]any{"target": "neighbor3", "weight": 0.9},
				},
			},
		},
	}
	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("center"), docJSON}}, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	engine := NewGraphQueryEngine(db, db.logger)

	t.Run("all neighbors", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "neighbors",
			IndexName: "citations",
			Params: indexes.GraphQueryParams{
				Direction: "out",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("center")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 3, result.Total) // All 3 neighbors
	})

	t.Run("filter by edge type", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "neighbors",
			IndexName: "citations",
			Params: indexes.GraphQueryParams{
				EdgeTypes: []string{"cites"},
				Direction: "out",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("center")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 2, result.Total) // Only cites neighbors
	})
}

func TestGraphQueryEngine_Execute_ShortestPath(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create linear path: a -> b -> c -> d
	docs := map[string]map[string]any{
		"a": {"title": "Node A"},
		"b": {"title": "Node B"},
		"c": {"title": "Node C"},
		"d": {"title": "Node D"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))

	// Add edges explicitly instead of relying on enricher
	require.NoError(t, db.AddEdge(ctx, "citations", []byte("a"), []byte("b"), "cites", 1.0, nil))
	require.NoError(t, db.AddEdge(ctx, "citations", []byte("b"), []byte("c"), "cites", 1.0, nil))
	require.NoError(t, db.AddEdge(ctx, "citations", []byte("c"), []byte("d"), "cites", 1.0, nil))

	// Wait for edge indexing to complete
	time.Sleep(200 * time.Millisecond)

	// Verify edges exist before running test
	edges, err := db.GetEdges(ctx, "citations", []byte("a"), "", indexes.EdgeDirectionOut)
	require.NoError(t, err)
	require.NotEmpty(t, edges, "edges from 'a' should exist")

	engine := NewGraphQueryEngine(db, db.logger)

	t.Run("find path", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "shortest_path",
			IndexName: "citations",
			TargetNodes: indexes.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
			},
			Params: indexes.GraphQueryParams{
				Direction:  "out",
				MaxDepth:   5,
				WeightMode: "min_hops",
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 1, result.Total) // One path
		assert.Len(t, result.Paths, 1)

		// Verify path has correct nodes
		path := result.Paths[0]
		assert.Len(t, path.Nodes, 4) // a, b, c, d
	})

	t.Run("missing target nodes", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "shortest_path",
			IndexName: "citations",
			// Missing TargetNodes
			Params: indexes.GraphQueryParams{
				Direction: "out",
			},
		}

		_, _, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "target_nodes")
	})
}

func TestGraphQueryEngine_Execute_KShortestPaths(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create diamond graph for k shortest paths testing:
	//       b
	//      / \
	//     1   2
	//    /     \
	//   a       d
	//    \     /
	//     3   4
	//      \ /
	//       c
	// Paths from a to d: a->b->d (weight 3), a->c->d (weight 7)
	docs := map[string]map[string]any{
		"a": {"title": "Node A"},
		"b": {"title": "Node B"},
		"c": {"title": "Node C"},
		"d": {"title": "Node D"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))

	// Add edges explicitly
	require.NoError(t, db.AddEdge(ctx, "citations", []byte("a"), []byte("b"), "cites", 1.0, nil))
	require.NoError(t, db.AddEdge(ctx, "citations", []byte("b"), []byte("d"), "cites", 2.0, nil))
	require.NoError(t, db.AddEdge(ctx, "citations", []byte("a"), []byte("c"), "cites", 3.0, nil))
	require.NoError(t, db.AddEdge(ctx, "citations", []byte("c"), []byte("d"), "cites", 4.0, nil))

	// Wait for edge indexing
	time.Sleep(200 * time.Millisecond)

	engine := NewGraphQueryEngine(db, db.logger)

	t.Run("find k=2 shortest paths", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "k_shortest_paths",
			IndexName: "citations",
			TargetNodes: indexes.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
			},
			Params: indexes.GraphQueryParams{
				Direction:  "out",
				MaxDepth:   5,
				WeightMode: "min_hops",
				K:          2,
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 2, result.Total) // Two paths
		assert.Len(t, result.Paths, 2)

		// Both paths should have 3 nodes (a, intermediate, d)
		for _, path := range result.Paths {
			assert.Len(t, path.Nodes, 3)
		}
	})

	t.Run("k=1 returns single shortest path", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "k_shortest_paths",
			IndexName: "citations",
			TargetNodes: indexes.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
			},
			Params: indexes.GraphQueryParams{
				Direction:  "out",
				MaxDepth:   5,
				WeightMode: "min_hops",
				K:          1,
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 1, result.Total)
		assert.Len(t, result.Paths, 1)
	})

	t.Run("min_weight mode considers edge weights", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "k_shortest_paths",
			IndexName: "citations",
			TargetNodes: indexes.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
			},
			Params: indexes.GraphQueryParams{
				Direction:  "out",
				MaxDepth:   5,
				WeightMode: "min_weight",
				K:          2,
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Len(t, result.Paths, 2)

		// First path should be a->b->d (weight 3), second a->c->d (weight 7)
		if len(result.Paths) >= 2 {
			assert.LessOrEqual(t, result.Paths[0].TotalWeight, result.Paths[1].TotalWeight,
				"Paths should be ordered by weight: %.2f <= %.2f",
				result.Paths[0].TotalWeight, result.Paths[1].TotalWeight)
		}
	})

	t.Run("k larger than available paths", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "k_shortest_paths",
			IndexName: "citations",
			TargetNodes: indexes.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
			},
			Params: indexes.GraphQueryParams{
				Direction:  "out",
				MaxDepth:   5,
				WeightMode: "min_hops",
				K:          10, // Only 2 paths exist
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		// Should return only available paths, not 10
		assert.LessOrEqual(t, result.Total, 10)
		assert.Equal(t, 2, result.Total) // Only 2 paths exist
	})

	t.Run("no path exists", func(t *testing.T) {
		// Create isolated node
		isolatedDoc := map[string]any{"title": "Isolated Node"}
		isolatedJSON, err := json.Marshal(isolatedDoc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("isolated"), isolatedJSON}}, nil, Op_SyncLevelWrite))
		time.Sleep(100 * time.Millisecond)

		query := &indexes.GraphQuery{
			Type:      "k_shortest_paths",
			IndexName: "citations",
			TargetNodes: indexes.GraphNodeSelector{
				Keys: []string{base64.StdEncoding.EncodeToString([]byte("isolated"))},
			},
			Params: indexes.GraphQueryParams{
				Direction:  "out",
				MaxDepth:   5,
				WeightMode: "min_hops",
				K:          3,
			},
		}

		result, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		require.NoError(t, err)
		assert.True(t, status.Success)
		assert.Equal(t, 0, result.Total) // No paths exist
	})
}

func TestGraphQueryEngine_ResolveNodeSelector(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()
	engine := NewGraphQueryEngine(db, db.logger)

	t.Run("explicit keys", func(t *testing.T) {
		selector := &indexes.GraphNodeSelector{
			Keys: []string{
				base64.StdEncoding.EncodeToString([]byte("key1")),
				base64.StdEncoding.EncodeToString([]byte("key2")),
			},
		}

		keys, err := engine.resolveNodeSelector(ctx, selector, nil)
		require.NoError(t, err)
		assert.Len(t, keys, 2)
		assert.Equal(t, []byte("key1"), keys[0])
		assert.Equal(t, []byte("key2"), keys[1])
	})

	t.Run("invalid base64", func(t *testing.T) {
		selector := &indexes.GraphNodeSelector{
			Keys: []string{"not-valid-base64!!!"},
		}

		_, err := engine.resolveNodeSelector(ctx, selector, nil)
		assert.Error(t, err)
	})

	t.Run("result ref without search results", func(t *testing.T) {
		selector := &indexes.GraphNodeSelector{
			ResultRef: "$full_text_results",
		}

		_, err := engine.resolveNodeSelector(ctx, selector, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "search results")
	})

	t.Run("empty selector", func(t *testing.T) {
		selector := &indexes.GraphNodeSelector{}

		_, err := engine.resolveNodeSelector(ctx, selector, nil)
		assert.Error(t, err)
	})
}

func TestGraphQueryEngine_ErrHandling(t *testing.T) {
	db, dir := setupTestDBWithGraphIndex(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()
	engine := NewGraphQueryEngine(db, db.logger)

	t.Run("no start nodes", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "traverse",
			IndexName: "citations",
			Params:    indexes.GraphQueryParams{},
		}

		_, status, err := engine.Execute(ctx, query, [][]byte{})
		assert.Error(t, err)
		assert.False(t, status.Success)
		assert.Contains(t, err.Error(), "no start nodes")
	})

	t.Run("unknown query type", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "invalid_type",
			IndexName: "citations",
			Params:    indexes.GraphQueryParams{},
		}

		_, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		assert.Error(t, err)
		assert.False(t, status.Success)
		assert.Contains(t, err.Error(), "unknown graph query type")
	})

	t.Run("k_shortest_paths requires target_nodes", func(t *testing.T) {
		query := &indexes.GraphQuery{
			Type:      "k_shortest_paths",
			IndexName: "citations",
			Params:    indexes.GraphQueryParams{},
		}

		_, status, err := engine.Execute(ctx, query, [][]byte{[]byte("a")})
		assert.Error(t, err)
		assert.False(t, status.Success)
		assert.Contains(t, err.Error(), "target_nodes")
	})
}

func TestGraphQueryEngine_ParseDirection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected indexes.EdgeDirection
	}{
		{"out", "out", indexes.EdgeDirectionOut},
		{"in", "in", indexes.EdgeDirectionIn},
		{"both", "both", indexes.EdgeDirectionBoth},
		{"default", "", indexes.EdgeDirectionOut},
		{"invalid", "invalid", indexes.EdgeDirectionOut},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseDirection(indexes.EdgeDirection(tt.input))
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSortGraphQueriesByDependencies(t *testing.T) {
	t.Run("independent queries", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"query1": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"a"},
				},
			},
			"query2": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"b"},
				},
			},
		}

		sorted, err := SortGraphQueriesByDependencies(queries)
		require.NoError(t, err)
		assert.Len(t, sorted, 2)
		// All queries should be included (order doesn't matter for independent queries)
		assert.Contains(t, sorted, "query1")
		assert.Contains(t, sorted, "query2")
	})

	t.Run("simple dependency chain", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"first": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"a"},
				},
			},
			"second": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.first",
				},
			},
		}

		sorted, err := SortGraphQueriesByDependencies(queries)
		require.NoError(t, err)
		assert.Len(t, sorted, 2)
		// First should come before second
		assert.Equal(t, "first", sorted[0])
		assert.Equal(t, "second", sorted[1])
	})

	t.Run("multi-level dependency chain", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"first": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"a"},
				},
			},
			"second": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.first",
				},
			},
			"third": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.second",
				},
			},
		}

		sorted, err := SortGraphQueriesByDependencies(queries)
		require.NoError(t, err)
		assert.Len(t, sorted, 3)
		// Verify correct ordering
		firstIdx := -1
		secondIdx := -1
		thirdIdx := -1
		for i, name := range sorted {
			switch name {
			case "first":
				firstIdx = i
			case "second":
				secondIdx = i
			case "third":
				thirdIdx = i
			}
		}
		assert.Less(t, firstIdx, secondIdx, "first should come before second")
		assert.Less(t, secondIdx, thirdIdx, "second should come before third")
	})

	t.Run("circular dependency", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"a": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.b",
				},
			},
			"b": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.a",
				},
			},
		}

		_, err := SortGraphQueriesByDependencies(queries)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "circular dependency")
	})

	t.Run("self-referencing query", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"self": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.self",
				},
			},
		}

		_, err := SortGraphQueriesByDependencies(queries)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "circular dependency")
	})

	t.Run("target_nodes dependency", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"a"},
				},
			},
			"path": {
				Type:      "shortest_path",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"start"},
				},
				TargetNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.neighbors",
				},
			},
		}

		sorted, err := SortGraphQueriesByDependencies(queries)
		require.NoError(t, err)
		assert.Len(t, sorted, 2)
		// neighbors should come before path
		assert.Equal(t, "neighbors", sorted[0])
		assert.Equal(t, "path", sorted[1])
	})

	t.Run("reference to non-existent query", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"query": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.nonexistent",
				},
			},
		}

		// Should not error - just treat as independent query
		sorted, err := SortGraphQueriesByDependencies(queries)
		require.NoError(t, err)
		assert.Len(t, sorted, 1)
		assert.Equal(t, "query", sorted[0])
	})

	t.Run("mixed dependencies and independence", func(t *testing.T) {
		queries := map[string]*indexes.GraphQuery{
			"independent1": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"a"},
				},
			},
			"independent2": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{"b"},
				},
			},
			"dependent": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.independent1",
				},
			},
		}

		sorted, err := SortGraphQueriesByDependencies(queries)
		require.NoError(t, err)
		assert.Len(t, sorted, 3)
		// independent1 must come before dependent
		independent1Idx := -1
		dependentIdx := -1
		for i, name := range sorted {
			switch name {
			case "independent1":
				independent1Idx = i
			case "dependent":
				dependentIdx = i
			}
		}
		assert.Less(t, independent1Idx, dependentIdx, "independent1 should come before dependent")
	})
}

func TestExtractGraphDependency(t *testing.T) {
	tests := []struct {
		name     string
		ref      string
		expected string
	}{
		{
			name:     "graph result reference",
			ref:      "$graph_results.first_hop",
			expected: "first_hop",
		},
		{
			name:     "full text reference",
			ref:      "$full_text_results",
			expected: "",
		},
		{
			name:     "vector reference",
			ref:      "$aknn_results.embeddings",
			expected: "",
		},
		{
			name:     "empty reference",
			ref:      "",
			expected: "",
		},
		{
			name:     "malformed reference",
			ref:      "$graph_results.",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractGraphDependency(tt.ref)
			assert.Equal(t, tt.expected, result)
		})
	}
}
