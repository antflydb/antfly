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
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/pebbleutils"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// setupTestGraphIndexWithConfig creates a test GraphIndexV0 with the given config
func setupTestGraphIndexWithConfig(t *testing.T, conf GraphIndexConfig) (*GraphIndexV0, *pebble.DB, string) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	pdb, err := pebble.Open(dir, pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)

	indexConfig, err := NewIndexConfig("test_graph", conf)
	require.NoError(t, err)

	index, err := NewGraphIndexV0(lg, &common.Config{}, pdb, dir, "test_graph", indexConfig, nil)
	require.NoError(t, err)

	graphIndex, ok := index.(*GraphIndexV0)
	require.True(t, ok, "Index should be *GraphIndexV0")

	return graphIndex, pdb, dir
}

func TestGraphIndexV0_NeedsEnricher(t *testing.T) {
	t.Run("no enrichment configured", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "cites"},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes: &edgeTypes,
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.False(t, index.NeedsEnricher())
	})

	t.Run("field configured", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "child_of", Field: "parent_id"},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes: &edgeTypes,
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.True(t, index.NeedsEnricher())
	})

	t.Run("summarizer configured", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "cites"},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes:  &edgeTypes,
			Summarizer: &ai.GeneratorConfig{},
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.True(t, index.NeedsEnricher())
	})
}

func TestGraphIndexV0_IsNavigable(t *testing.T) {
	t.Run("not navigable - no summarizer", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "child_of", Topology: EdgeTypeConfigTopologyTree},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes: &edgeTypes,
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.False(t, index.IsNavigable())
	})

	t.Run("not navigable - no tree topology", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "related_to"},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes:  &edgeTypes,
			Summarizer: &ai.GeneratorConfig{},
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.False(t, index.IsNavigable())
	})

	t.Run("navigable - tree + summarizer", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "child_of", Topology: EdgeTypeConfigTopologyTree},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes:  &edgeTypes,
			Summarizer: &ai.GeneratorConfig{},
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.True(t, index.IsNavigable())
	})
}

func TestGraphIndexV0_GetTreeEdgeType(t *testing.T) {
	t.Run("no tree edge type", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "cites"},
			{Name: "related_to"},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes: &edgeTypes,
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.Empty(t, index.GetTreeEdgeType())
	})

	t.Run("has tree edge type", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "cites"},
			{Name: "child_of", Topology: EdgeTypeConfigTopologyTree},
		}
		index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes: &edgeTypes,
		})
		defer pdb.Close()
		defer os.RemoveAll(dir)
		defer index.Close()

		assert.Equal(t, "child_of", index.GetTreeEdgeType())
	})
}

func TestGraphIndexV0_FieldEdgeTypes(t *testing.T) {
	edgeTypes := []EdgeTypeConfig{
		{Name: "child_of", Field: "parent_id"},
		{Name: "cites"}, // no field
		{Name: "related_to", Field: "related_ids"},
	}
	index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
		EdgeTypes: &edgeTypes,
	})
	defer pdb.Close()
	defer os.RemoveAll(dir)
	defer index.Close()

	fieldTypes := index.fieldEdgeTypes()
	assert.Len(t, fieldTypes, 2)
	assert.Equal(t, "child_of", fieldTypes[0].Name)
	assert.Equal(t, "related_to", fieldTypes[1].Name)
}

func TestGraphIndexV0_TreeTopologyValidation(t *testing.T) {
	edgeTypes := []EdgeTypeConfig{
		{Name: "child_of", Topology: EdgeTypeConfigTopologyTree},
		{Name: "related_to"}, // graph topology (default)
	}
	index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
		EdgeTypes: &edgeTypes,
	})
	defer pdb.Close()
	defer os.RemoveAll(dir)
	defer index.Close()

	ctx := context.Background()

	t.Run("tree allows first parent", func(t *testing.T) {
		// child_of edges: source=child, target=parent (child points to parent)
		edgeKey := storeutils.MakeEdgeKey(
			[]byte("child_1"),
			[]byte("parent_a"),
			"test_graph",
			"child_of",
		)
		edge := &Edge{
			Source:    []byte("child_1"),
			Target:    []byte("parent_a"),
			Type:      "child_of",
			Weight:    1.0,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		// Write the forward edge to main DB so validateTreeTopology can find it
		require.NoError(t, pdb.Set(edgeKey, edgeValue, nil))

		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)
	})

	t.Run("tree rejects second parent", func(t *testing.T) {
		// Try to add a second parent for child_1 (child_1 already has parent_a in main DB)
		edgeKey := storeutils.MakeEdgeKey(
			[]byte("child_1"),
			[]byte("parent_b"),
			"test_graph",
			"child_of",
		)
		edge := &Edge{
			Source:    []byte("child_1"),
			Target:    []byte("parent_b"),
			Type:      "child_of",
			Weight:    1.0,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		// Don't write to pdb - validateTreeTopology will find child_1→parent_a from previous sub-test
		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tree topology violation")
	})

	t.Run("tree allows same parent re-write", func(t *testing.T) {
		// Re-writing the same edge should be fine
		edgeKey := storeutils.MakeEdgeKey(
			[]byte("child_1"),
			[]byte("parent_a"),
			"test_graph",
			"child_of",
		)
		edge := &Edge{
			Source:    []byte("child_1"),
			Target:    []byte("parent_a"),
			Type:      "child_of",
			Weight:    0.5, // different weight is ok
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		edgeValue, err := EncodeEdgeValue(edge)
		require.NoError(t, err)

		err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
		require.NoError(t, err)
	})

	t.Run("graph topology allows multiple parents", func(t *testing.T) {
		// related_to has graph topology - multiple incoming should work
		for _, source := range []string{"node_x", "node_y", "node_z"} {
			edgeKey := storeutils.MakeEdgeKey(
				[]byte(source),
				[]byte("target_node"),
				"test_graph",
				"related_to",
			)
			edge := &Edge{
				Source:    []byte(source),
				Target:    []byte("target_node"),
				Type:      "related_to",
				Weight:    1.0,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}
			edgeValue, err := EncodeEdgeValue(edge)
			require.NoError(t, err)

			err = index.Batch(ctx, [][2][]byte{{edgeKey, edgeValue}}, nil, true)
			require.NoError(t, err)
		}
	})
}

func TestGraphIndexV0_ToStringSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected []string
	}{
		{"string", "hello", []string{"hello"}},
		{"string slice", []string{"a", "b"}, []string{"a", "b"}},
		{"any slice", []any{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"any slice with non-strings", []any{"a", 42, "b"}, []string{"a", "b"}},
		{"nil", nil, nil},
		{"integer", 42, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toStringSlice(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGraphIndexV0_MakeFieldHashKey(t *testing.T) {
	key := makeFieldHashKey([]byte("doc1"), "my_graph", "child_of")
	assert.Equal(t, []byte("doc1:i:my_graph:child_of:fh"), key)
}

func TestGraphIndexV0_FieldEdgeReconciliation(t *testing.T) {
	edgeTypes := []EdgeTypeConfig{
		{Name: "child_of", Field: "parent_id", Topology: EdgeTypeConfigTopologyTree},
	}
	index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
		EdgeTypes: &edgeTypes,
	})
	defer pdb.Close()
	defer os.RemoveAll(dir)
	defer index.Close()

	ctx := context.Background()

	// Create a persistFunc that writes directly to the main DB (simulating Raft)
	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		batch := pdb.NewBatch()
		for _, kv := range writes {
			if kv[1] == nil {
				if err := batch.Delete(kv[0], nil); err != nil {
					batch.Close()
					return err
				}
			} else {
				if err := batch.Set(kv[0], kv[1], nil); err != nil {
					batch.Close()
					return err
				}
			}
		}
		err := batch.Commit(pebble.Sync)
		batch.Close()

		// After persisting, also update the reverse index via Batch()
		var edgeWrites [][2][]byte
		var edgeDeletes [][]byte
		for _, kv := range writes {
			if kv[1] == nil {
				edgeDeletes = append(edgeDeletes, kv[0])
			} else {
				edgeWrites = append(edgeWrites, kv)
			}
		}
		if len(edgeWrites) > 0 || len(edgeDeletes) > 0 {
			return index.Batch(ctx, edgeWrites, edgeDeletes, true)
		}
		return err
	}

	enricher := &graphEnricher{
		graph:       index,
		persistFunc: persistFunc,
	}

	t.Run("creates edges from field", func(t *testing.T) {
		doc := map[string]any{
			"parent_id": "parent_a",
			"title":     "Test Document",
		}

		err := enricher.reconcileFieldEdges(ctx, []byte("doc1"), doc)
		require.NoError(t, err)

		// Check that the edge was created
		edgeKey := storeutils.MakeEdgeKey([]byte("doc1"), []byte("parent_a"), "test_graph", "child_of")
		val, closer, err := pdb.Get(edgeKey)
		require.NoError(t, err)
		assert.NotEmpty(t, val)
		closer.Close()

		// Check that the hash was stored
		hashKey := makeFieldHashKey([]byte("doc1"), "test_graph", "child_of")
		_, hashCloser, err := pdb.Get(hashKey)
		require.NoError(t, err)
		hashCloser.Close()
	})

	t.Run("skips unchanged field", func(t *testing.T) {
		doc := map[string]any{
			"parent_id": "parent_a", // same as before
			"title":     "Test Document",
		}

		// Should be a no-op (hash matches)
		err := enricher.reconcileFieldEdges(ctx, []byte("doc1"), doc)
		require.NoError(t, err)
	})

	t.Run("updates edges when field changes", func(t *testing.T) {
		doc := map[string]any{
			"parent_id": "parent_b", // changed!
			"title":     "Test Document",
		}

		err := enricher.reconcileFieldEdges(ctx, []byte("doc1"), doc)
		require.NoError(t, err)

		// Old edge should be deleted
		oldEdgeKey := storeutils.MakeEdgeKey([]byte("doc1"), []byte("parent_a"), "test_graph", "child_of")
		_, _, err = pdb.Get(oldEdgeKey)
		assert.ErrorIs(t, err, pebble.ErrNotFound)

		// New edge should exist
		newEdgeKey := storeutils.MakeEdgeKey([]byte("doc1"), []byte("parent_b"), "test_graph", "child_of")
		val, closer, err := pdb.Get(newEdgeKey)
		require.NoError(t, err)
		assert.NotEmpty(t, val)
		closer.Close()
	})

	t.Run("handles array field", func(t *testing.T) {
		arrayEdgeTypes := []EdgeTypeConfig{
			{Name: "related_to", Field: "related_ids"},
		}
		arrayIndex, arrayPdb, arrayDir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
			EdgeTypes: &arrayEdgeTypes,
		})
		defer arrayPdb.Close()
		defer os.RemoveAll(arrayDir)
		defer arrayIndex.Close()

		arrayPersistFunc := func(ctx context.Context, writes [][2][]byte) error {
			batch := arrayPdb.NewBatch()
			for _, kv := range writes {
				if kv[1] == nil {
					if err := batch.Delete(kv[0], nil); err != nil {
						batch.Close()
						return err
					}
				} else {
					if err := batch.Set(kv[0], kv[1], nil); err != nil {
						batch.Close()
						return err
					}
				}
			}
			return batch.Commit(pebble.Sync)
		}

		arrayEnricher := &graphEnricher{
			graph:       arrayIndex,
			persistFunc: arrayPersistFunc,
		}

		doc := map[string]any{
			"related_ids": []any{"target_1", "target_2", "target_3"},
		}

		err := arrayEnricher.reconcileFieldEdges(ctx, []byte("doc_x"), doc)
		require.NoError(t, err)

		// Check all edges were created
		for _, target := range []string{"target_1", "target_2", "target_3"} {
			edgeKey := storeutils.MakeEdgeKey([]byte("doc_x"), []byte(target), "test_graph", "related_to")
			val, closer, err := arrayPdb.Get(edgeKey)
			require.NoError(t, err, "edge to %s should exist", target)
			assert.NotEmpty(t, val)
			closer.Close()
		}
	})

	t.Run("handles missing field gracefully", func(t *testing.T) {
		doc := map[string]any{
			"title": "No parent field",
		}

		err := enricher.reconcileFieldEdges(ctx, []byte("doc_no_parent"), doc)
		require.NoError(t, err)
	})
}

func TestGraphIndexV0_LeaderFactory_NoOp(t *testing.T) {
	// When no enrichment is configured, LeaderFactory should block until cancelled
	edgeTypes := []EdgeTypeConfig{
		{Name: "cites"}, // no field, no summarizer
	}
	index, pdb, dir := setupTestGraphIndexWithConfig(t, GraphIndexConfig{
		EdgeTypes: &edgeTypes,
	})
	defer pdb.Close()
	defer os.RemoveAll(dir)
	defer index.Close()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- index.LeaderFactory(ctx, func(ctx context.Context, writes [][2][]byte) error {
			return nil
		})
	}()

	// Should not return immediately
	select {
	case <-done:
		t.Fatal("LeaderFactory returned immediately, should block")
	case <-time.After(100 * time.Millisecond):
		// Good - it's blocking
	}

	// Cancel should cause it to return
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("LeaderFactory did not return after cancel")
	}
}

func TestGraphIndexV0_ParseDocument(t *testing.T) {
	doc := map[string]any{
		"title":     "Test",
		"parent_id": "parent_1",
	}
	data, err := json.Marshal(doc)
	require.NoError(t, err)

	parsed, err := parseDocument(data, nil)
	require.NoError(t, err)
	assert.Equal(t, "Test", parsed["title"])
	assert.Equal(t, "parent_1", parsed["parent_id"])
}

func TestGraphIndexV0_ConfigEquality(t *testing.T) {
	t.Run("equal with new fields", func(t *testing.T) {
		edgeTypes := []EdgeTypeConfig{
			{Name: "child_of", Field: "parent_id", Topology: EdgeTypeConfigTopologyTree},
		}
		a := GraphIndexConfig{
			EdgeTypes:  &edgeTypes,
			Template:   "{{title}}",
			Summarizer: &ai.GeneratorConfig{},
		}
		b := GraphIndexConfig{
			EdgeTypes:  &edgeTypes,
			Template:   "{{title}}",
			Summarizer: &ai.GeneratorConfig{},
		}
		assert.True(t, a.Equal(b))
	})

	t.Run("not equal - different template", func(t *testing.T) {
		a := GraphIndexConfig{Template: "{{title}}"}
		b := GraphIndexConfig{Template: "{{content}}"}
		assert.False(t, a.Equal(b))
	})

	t.Run("not equal - different summarizer", func(t *testing.T) {
		a := GraphIndexConfig{Summarizer: &ai.GeneratorConfig{}}
		b := GraphIndexConfig{Summarizer: nil}
		assert.False(t, a.Equal(b))
	})
}
