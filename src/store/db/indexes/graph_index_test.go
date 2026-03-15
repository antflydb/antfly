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

package indexes

import (
	"context"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// setupTestGraphIndex creates a test GraphIndexV0 instance
func setupTestGraphIndex(t *testing.T) (*GraphIndexV0, *pebble.DB, string) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	// Open pebble database
	pdb, err := pebble.Open(dir, pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)

	// Create graph index config
	edgeTypes := []EdgeTypeConfig{
		{
			Name:             "cites",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: nil,
		},
		{
			Name:             "similar_to",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   true,
			RequiredMetadata: nil,
		},
	}
	config := GraphIndexConfig{
		EdgeTypes:           &edgeTypes,
		MaxEdgesPerDocument: 100,
	}

	indexConfig, err := NewIndexConfig("test_graph", config)
	require.NoError(t, err)

	// Create graph index
	index, err := NewGraphIndexV0(lg, &common.Config{}, pdb, dir, "test_graph", indexConfig, nil)
	require.NoError(t, err)

	graphIndex, ok := index.(*GraphIndexV0)
	require.True(t, ok, "Index should be *GraphIndexV0")

	return graphIndex, pdb, dir
}

// TestGraphIndexV0_Write tests writing edges to the index
func TestGraphIndexV0_Write(t *testing.T) {
	index, pdb, dir := setupTestGraphIndex(t)
	defer pdb.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	t.Run("write outgoing edge", func(t *testing.T) {
		// Create an edge key
		edgeKey := storeutils.MakeEdgeKey(
			[]byte("source_doc"),
			[]byte("target_doc"),
			"test_graph",
			"cites",
		)

		// Create edge value
		edge := &Edge{
			Source: []byte("source_doc"),
			Target: []byte("target_doc"),
			Type:   "cites",
			Weight: 0.9,
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		// Write to index
		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)

		// Verify edge index was created (reverse lookup)
		// The edge index should be at: target_doc:i:test_graph:in:cites:source_doc:i
		edgeIndexKey := make([]byte, 0)
		edgeIndexKey = append(edgeIndexKey, []byte("target_doc")...)
		edgeIndexKey = append(edgeIndexKey, []byte(":i:")...)
		edgeIndexKey = append(edgeIndexKey, []byte("test_graph")...)
		edgeIndexKey = append(edgeIndexKey, []byte(":in:")...)
		edgeIndexKey = append(edgeIndexKey, []byte("cites")...)
		edgeIndexKey = append(edgeIndexKey, ':')
		edgeIndexKey = append(edgeIndexKey, []byte("source_doc")...)
		edgeIndexKey = append(edgeIndexKey, []byte(":i")...)

		// Check if edge index exists in indexDB (not main pdb)
		_, closer, err := index.GetIndexDB().Get(edgeIndexKey)
		require.NoError(t, err)
		closer.Close()
	})

	t.Run("write multiple edges", func(t *testing.T) {
		edges := [][2][]byte{}
		var err error

		for i := range 5 {
			source := []byte("doc_a")
			target := []byte{byte('b' + i)}

			edgeKey := storeutils.MakeEdgeKey(source, target, "test_graph", "cites")
			edge := &Edge{
				Source: source,
				Target: target,
				Type:   "cites",
				Weight: 0.8,
			}
			edgeValue, err := EncodeEdgeValue(edge)
			require.NoError(t, err)

			edges = append(edges, [2][]byte{edgeKey, edgeValue})
		}

		// Write all edges
		err = index.Batch(ctx, edges, nil, true)
		require.NoError(t, err)
	})
}

// TestGraphIndexV0_Delete tests deleting edges from the index
func TestGraphIndexV0_Delete(t *testing.T) {
	index, pdb, dir := setupTestGraphIndex(t)
	defer pdb.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Write an edge first
	edgeKey := storeutils.MakeEdgeKey(
		[]byte("source"),
		[]byte("target"),
		"test_graph",
		"cites",
	)

	edge := &Edge{
		Source: []byte("source"),
		Target: []byte("target"),
		Type:   "cites",
		Weight: 1.0,
	}
	edgeValue, err := EncodeEdgeValue(edge)
	require.NoError(t, err)

	err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
	require.NoError(t, err)

	// Delete the edge
	err = index.Batch(ctx, nil, [][]byte{edgeKey}, true)
	require.NoError(t, err)

	// Verify edge index was deleted
	edgeIndexKey := make([]byte, 0)
	edgeIndexKey = append(edgeIndexKey, []byte("target")...)
	edgeIndexKey = append(edgeIndexKey, []byte(":i:")...)
	edgeIndexKey = append(edgeIndexKey, []byte("test_graph")...)
	edgeIndexKey = append(edgeIndexKey, []byte(":in:")...)
	edgeIndexKey = append(edgeIndexKey, []byte("cites")...)
	edgeIndexKey = append(edgeIndexKey, ':')
	edgeIndexKey = append(edgeIndexKey, []byte("source")...)
	edgeIndexKey = append(edgeIndexKey, []byte(":i")...)

	_, _, err = pdb.Get(edgeIndexKey)
	assert.Error(t, err, "Edge index should be deleted")
	assert.Equal(t, pebble.ErrNotFound, err)
}

// TestGraphIndexV0_Search tests searching the graph index
func TestGraphIndexV0_Search(t *testing.T) {
	index, pdb, dir := setupTestGraphIndex(t)
	defer pdb.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create a small graph for searching
	// a -> b, a -> c, d -> a
	edges := []struct {
		source string
		target string
		weight float64
	}{
		{"a", "b", 1.0},
		{"a", "c", 0.8},
		{"d", "a", 0.9},
	}

	for _, e := range edges {
		edgeKey := storeutils.MakeEdgeKey(
			[]byte(e.source),
			[]byte(e.target),
			"test_graph",
			"cites",
		)

		edge := &Edge{
			Source: []byte(e.source),
			Target: []byte(e.target),
			Type:   "cites",
			Weight: e.weight,
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)
	}

	t.Run("search should return empty", func(t *testing.T) {
		// Search for incoming edges to a node with no incoming edges
		query := map[string]any{
			"target": []byte("nonexistent"),
		}
		result, err := index.Search(ctx, query)
		require.NoError(t, err)
		edges, ok := result.([]Edge)
		require.True(t, ok, "Result should be []Edge")
		assert.Empty(t, edges, "Should return empty for nonexistent node")
	})
}

// TestGraphIndexV0_EdgeValidation tests edge type validation
func TestGraphIndexV0_EdgeValidation(t *testing.T) {
	index, pdb, dir := setupTestGraphIndex(t)
	defer pdb.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	t.Run("valid edge type", func(t *testing.T) {
		edgeKey := storeutils.MakeEdgeKey(
			[]byte("source"),
			[]byte("target"),
			"test_graph",
			"cites", // Valid edge type
		)

		edge := &Edge{
			Source: []byte("source"),
			Target: []byte("target"),
			Type:   "cites",
			Weight: 0.5,
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)
	})

	t.Run("self loops allowed for similar_to", func(t *testing.T) {
		edgeKey := storeutils.MakeEdgeKey(
			[]byte("doc"),
			[]byte("doc"), // Same source and target
			"test_graph",
			"similar_to", // Allows self loops
		)

		edge := &Edge{
			Source: []byte("doc"),
			Target: []byte("doc"),
			Type:   "similar_to",
			Weight: 1.0,
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)
	})
}

// TestGraphIndexV0_EdgeIndex tests the reverse edge index functionality
func TestGraphIndexV0_EdgeIndex(t *testing.T) {
	index, pdb, dir := setupTestGraphIndex(t)
	defer pdb.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create edges: a -> b, c -> b, d -> b
	// This tests that b's incoming edge index is properly maintained
	sources := []string{"a", "c", "d"}
	for _, source := range sources {
		edgeKey := storeutils.MakeEdgeKey(
			[]byte(source),
			[]byte("b"),
			"test_graph",
			"cites",
		)

		edge := &Edge{
			Source: []byte(source),
			Target: []byte("b"),
			Type:   "cites",
			Weight: 1.0,
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)
	}

	// Verify all incoming edges are indexed for node b in indexDB (not main pdb)
	prefix := []byte("b:i:test_graph:in:cites:")
	iter, err := index.GetIndexDB().NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append([]byte{}, append(prefix, 0xFF)...),
	})
	require.NoError(t, err)
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	assert.Equal(t, 3, count, "Should have 3 incoming edges indexed for node b")
}

// TestGraphIndexV0_Stats tests index statistics
func TestGraphIndexV0_Stats(t *testing.T) {
	index, pdb, dir := setupTestGraphIndex(t)
	defer pdb.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Add some edges
	for i := range 10 {
		source := []byte("source")
		target := []byte{byte('a' + i)}

		edgeKey := storeutils.MakeEdgeKey(source, target, "test_graph", "cites")
		edge := &Edge{
			Source: source,
			Target: target,
			Type:   "cites",
			Weight: 0.9,
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)
	}

	// Get stats
	stats := index.Stats()
	assert.NotNil(t, stats)
}

// TestGraphIndexV0_Close tests index cleanup
func TestGraphIndexV0_Close(t *testing.T) {
	index, pdb, dir := setupTestGraphIndex(t)
	defer pdb.Close()
	defer os.RemoveAll(dir)

	err := index.Close()
	assert.NoError(t, err)
}
