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
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// setupGraphPathTestDB creates a test database with a graph index for path-finding tests
func setupGraphPathTestDB(t *testing.T) (*DBImpl, func()) {
	t.Helper()

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
						"name": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Create graph index config
	edgeTypes := []indexes.EdgeTypeConfig{
		{
			Name:             "friend",
			MaxWeight:        1000.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: &[]string{},
		},
		{
			Name:             "follows",
			MaxWeight:        1000.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: &[]string{},
		},
	}
	graphConfig := indexes.GraphIndexConfig{
		EdgeTypes:           &edgeTypes,
		MaxEdgesPerDocument: 100,
	}

	idxCfg, err := indexes.NewIndexConfig("test_graph", graphConfig)
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
	db.indexes["test_graph"] = *idxCfg
	db.indexesMu.Unlock()

	// Register the index with index manager and start it
	require.NoError(t, indexManager.Register("test_graph", false, *idxCfg))
	require.NoError(t, indexManager.Start(false))

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

// createTestGraph creates a simple graph for testing:
//
//	A --1--> B --2--> C --3--> D
//	|        |        |
//	5        4        6
//	|        |        |
//	v        v        v
//	E --7--> F --8--> G
//
// Edge weights are shown as numbers
func createTestGraph(t *testing.T, db *DBImpl) {
	t.Helper()

	ctx := context.Background()

	// Create nodes
	nodes := map[string]map[string]any{
		"A": {"name": "Node A"},
		"B": {"name": "Node B"},
		"C": {"name": "Node C"},
		"D": {"name": "Node D"},
		"E": {"name": "Node E"},
		"F": {"name": "Node F"},
		"G": {"name": "Node G"},
	}

	writes := make([][2][]byte, 0, len(nodes))
	for key, doc := range nodes {
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docBytes})
	}

	err := db.Batch(ctx, writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Create edges with weights
	edges := []struct {
		source, target string
		edgeType       string
		weight         float64
	}{
		{"A", "B", "friend", 1.0},
		{"B", "C", "friend", 2.0},
		{"C", "D", "friend", 3.0},
		{"A", "E", "friend", 5.0},
		{"B", "F", "friend", 4.0},
		{"C", "G", "friend", 6.0},
		{"E", "F", "friend", 7.0},
		{"F", "G", "friend", 8.0},
	}

	for _, edge := range edges {
		err := db.AddEdge(ctx, "test_graph", []byte(edge.source), []byte(edge.target),
			edge.edgeType, edge.weight, nil)
		require.NoError(t, err)
	}
}

func TestFindShortestPath_MinHops_DirectPath(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Find path from A to C (should be A -> B -> C)
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("C"),
		nil, // all edge types
		indexes.EdgeDirectionOut,
		"min_hops",
		10,
		0.0,
		1000.0,
	)

	require.NoError(t, err)
	require.NotNil(t, path)
	assert.Equal(t, 2, path.Length)
	assert.Len(t, path.Nodes, 3)

	// Decode nodes
	expectedNodes := []string{"A", "B", "C"}
	for i, nodeB64 := range path.Nodes {
		nodeBytes, err := base64.StdEncoding.DecodeString(nodeB64)
		require.NoError(t, err)
		assert.Equal(t, expectedNodes[i], string(nodeBytes))
	}

	// Check edges
	assert.Len(t, path.Edges, 2)
	assert.Equal(t, 3.0, path.TotalWeight) // 1 + 2
}

func TestFindShortestPath_MinHops_LongerPath(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Find path from A to G
	// Min hops: Either A -> B -> C -> G or A -> B -> F -> G (both 3 hops)
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("G"),
		nil,
		indexes.EdgeDirectionOut,
		"min_hops",
		10,
		0.0,
		1000.0,
	)

	require.NoError(t, err)
	require.NotNil(t, path)
	assert.Equal(t, 3, path.Length)
	assert.Len(t, path.Nodes, 4)

	// Decode nodes - first and last must be A and G
	firstNode, err := base64.StdEncoding.DecodeString(path.Nodes[0])
	require.NoError(t, err)
	assert.Equal(t, "A", string(firstNode))

	lastNode, err := base64.StdEncoding.DecodeString(path.Nodes[3])
	require.NoError(t, err)
	assert.Equal(t, "G", string(lastNode))
}

func TestFindShortestPath_MinWeight(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Find path from A to G using min_weight
	// A -> B -> C -> G = 1 + 2 + 6 = 9
	// A -> E -> F -> G = 5 + 7 + 8 = 20
	// A -> B -> F -> G = 1 + 4 + 8 = 13
	// Should choose A -> B -> C -> G (weight 9)
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("G"),
		nil,
		indexes.EdgeDirectionOut,
		"min_weight",
		10,
		0.0,
		1000.0,
	)

	require.NoError(t, err)
	require.NotNil(t, path)
	assert.Equal(t, 9.0, path.TotalWeight)

	// Decode nodes
	expectedNodes := []string{"A", "B", "C", "G"}
	for i, nodeB64 := range path.Nodes {
		nodeBytes, err := base64.StdEncoding.DecodeString(nodeB64)
		require.NoError(t, err)
		assert.Equal(t, expectedNodes[i], string(nodeBytes))
	}
}

func TestFindShortestPath_MaxWeight(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	// Create a graph optimized for testing max weight (product)
	ctx := context.Background()

	// Create nodes
	nodes := map[string]map[string]any{
		"A": {"name": "Node A"},
		"B": {"name": "Node B"},
		"C": {"name": "Node C"},
	}

	writes := make([][2][]byte, 0, len(nodes))
	for key, doc := range nodes {
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docBytes})
	}

	err := db.Batch(ctx, writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Create edges with different weights
	// Path 1: A -> B -> C with weights 0.9 * 0.9 = 0.81
	// Path 2: A -> C with weight 0.8
	// max_weight should choose path 1 (0.81 > 0.8)
	edges := []struct {
		source, target string
		weight         float64
	}{
		{"A", "B", 0.9},
		{"B", "C", 0.9},
		{"A", "C", 0.8},
	}

	for _, edge := range edges {
		err := db.AddEdge(ctx, "test_graph", []byte(edge.source), []byte(edge.target),
			"friend", edge.weight, nil)
		require.NoError(t, err)
	}

	// Find path from A to C using max_weight
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("C"),
		nil,
		indexes.EdgeDirectionOut,
		"max_weight",
		10,
		0.0,
		1.0,
	)

	require.NoError(t, err)
	require.NotNil(t, path)

	// Should choose A -> B -> C path
	assert.Equal(t, 2, path.Length)
	expectedNodes := []string{"A", "B", "C"}
	for i, nodeB64 := range path.Nodes {
		nodeBytes, err := base64.StdEncoding.DecodeString(nodeB64)
		require.NoError(t, err)
		assert.Equal(t, expectedNodes[i], string(nodeBytes))
	}
}

func TestFindShortestPath_NoPath(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Try to find path from D to A (no reverse edges exist)
	_, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("D"),
		[]byte("A"),
		nil,
		indexes.EdgeDirectionOut,
		"min_hops",
		10,
		0.0,
		1000.0,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no path found")
}

func TestFindShortestPath_MaxDepthExceeded(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Try to find path from A to G with max depth too small
	_, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("G"),
		nil,
		indexes.EdgeDirectionOut,
		"min_hops",
		2, // Too small - need 3 hops
		0.0,
		1000.0,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no path found")
}

func TestFindShortestPath_EdgeTypeFiltering(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create nodes
	nodes := map[string]map[string]any{
		"A": {"name": "Node A"},
		"B": {"name": "Node B"},
		"C": {"name": "Node C"},
	}

	writes := make([][2][]byte, 0, len(nodes))
	for key, doc := range nodes {
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docBytes})
	}

	err := db.Batch(ctx, writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Create edges with different types
	err = db.AddEdge(ctx, "test_graph", []byte("A"), []byte("B"), "friend", 1.0, nil)
	require.NoError(t, err)
	err = db.AddEdge(ctx, "test_graph", []byte("B"), []byte("C"), "follows", 2.0, nil)
	require.NoError(t, err)
	err = db.AddEdge(ctx, "test_graph", []byte("A"), []byte("C"), "friend", 5.0, nil)
	require.NoError(t, err)

	// Find path from A to C using only "friend" edges
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("C"),
		[]string{"friend"}, // Only friend edges
		indexes.EdgeDirectionOut,
		"min_weight",
		10,
		0.0,
		1000.0,
	)

	require.NoError(t, err)
	require.NotNil(t, path)

	// Should take direct path A -> C (weight 5)
	// Cannot use A -> B -> C because B -> C is "follows" not "friend"
	assert.Equal(t, 1, path.Length)
	assert.Equal(t, 5.0, path.TotalWeight)
}

func TestFindShortestPath_WeightFiltering(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Find path from A to D, but exclude edges with weight > 2.5
	// This should allow A -> B (weight 1) and B -> C (weight 2)
	// but prevent C -> D (weight 3)
	// There should be no path to D with these constraints
	_, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("D"),
		nil,
		indexes.EdgeDirectionOut,
		"min_hops",
		10,
		0.0,
		2.5, // Max weight - excludes C -> D (weight 3)
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no path found")

	// Now try with higher max weight - should find path
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("D"),
		nil,
		indexes.EdgeDirectionOut,
		"min_hops",
		10,
		0.0,
		10.0, // Higher max weight
	)

	require.NoError(t, err)
	require.NotNil(t, path)
	assert.Equal(t, 3, path.Length)
}

func TestFindShortestPath_SameNode(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Find path from A to A (same node)
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("A"),
		nil,
		indexes.EdgeDirectionOut,
		"min_hops",
		10,
		0.0,
		1000.0,
	)

	require.NoError(t, err)
	require.NotNil(t, path)
	assert.Equal(t, 0, path.Length)
	assert.Len(t, path.Nodes, 1)
	assert.Equal(t, 0.0, path.TotalWeight)

	// Verify it's node A
	nodeBytes, err := base64.StdEncoding.DecodeString(path.Nodes[0])
	require.NoError(t, err)
	assert.Equal(t, "A", string(nodeBytes))
}

func TestFindShortestPath_InvalidWeightMode(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	createTestGraph(t, db)

	ctx := context.Background()

	// Try invalid weight mode
	_, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("A"),
		[]byte("B"),
		nil,
		indexes.EdgeDirectionOut,
		"invalid_mode",
		10,
		0.0,
		1000.0,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid weight_mode")
}

func TestFindShortestPath_BidirectionalEdges(t *testing.T) {
	db, cleanup := setupGraphPathTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create nodes
	nodes := map[string]map[string]any{
		"A": {"name": "Node A"},
		"B": {"name": "Node B"},
		"C": {"name": "Node C"},
	}

	writes := make([][2][]byte, 0, len(nodes))
	for key, doc := range nodes {
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docBytes})
	}

	err := db.Batch(ctx, writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Create bidirectional edges
	err = db.AddEdge(ctx, "test_graph", []byte("A"), []byte("B"), "friend", 1.0, nil)
	require.NoError(t, err)
	err = db.AddEdge(ctx, "test_graph", []byte("B"), []byte("A"), "friend", 1.0, nil)
	require.NoError(t, err)
	err = db.AddEdge(ctx, "test_graph", []byte("B"), []byte("C"), "friend", 2.0, nil)
	require.NoError(t, err)
	err = db.AddEdge(ctx, "test_graph", []byte("C"), []byte("B"), "friend", 2.0, nil)
	require.NoError(t, err)

	// Find path from C to A using "both" direction
	path, err := db.FindShortestPath(
		ctx,
		"test_graph",
		[]byte("C"),
		[]byte("A"),
		nil,
		indexes.EdgeDirectionBoth,
		"min_hops",
		10,
		0.0,
		1000.0,
	)

	require.NoError(t, err)
	require.NotNil(t, path)
	assert.Equal(t, 2, path.Length)

	expectedNodes := []string{"C", "B", "A"}
	for i, nodeB64 := range path.Nodes {
		nodeBytes, err := base64.StdEncoding.DecodeString(nodeB64)
		require.NoError(t, err)
		assert.Equal(t, expectedNodes[i], string(nodeBytes))
	}
}
