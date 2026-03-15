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

package metadata

import (
	"encoding/base64"
	"testing"

	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReconstructPath tests path reconstruction from parent tracking
func TestReconstructPath(t *testing.T) {
	t.Run("reconstructs path from parent tracking", func(t *testing.T) {
		ms := &MetadataStore{}

		// Build parent tracking for path: A -> B -> C
		parent := make(map[string]*pathNode)

		sourceKey := []byte("A")
		middleKey := []byte("B")
		targetKey := []byte("C")

		parent[string(sourceKey)] = &pathNode{
			key:      sourceKey,
			distance: 0,
			hops:     0,
			parent:   nil,
		}

		edgeAB := indexes.Edge{
			Source: sourceKey,
			Target: middleKey,
			Type:   "test",
			Weight: 1.5,
		}

		parent[string(middleKey)] = &pathNode{
			key:        middleKey,
			distance:   1.5,
			hops:       1,
			parent:     parent[string(sourceKey)],
			parentEdge: edgeAB,
		}

		edgeBC := indexes.Edge{
			Source: middleKey,
			Target: targetKey,
			Type:   "test",
			Weight: 2.5,
		}

		parent[string(targetKey)] = &pathNode{
			key:        targetKey,
			distance:   4.0,
			hops:       2,
			parent:     parent[string(middleKey)],
			parentEdge: edgeBC,
		}

		// Reconstruct path
		path := ms.reconstructPath(parent, targetKey)

		// Verify path structure
		require.NotNil(t, path)
		assert.Len(t, path.Nodes, 3, "Should have 3 nodes: A, B, C")
		assert.Len(t, path.Edges, 2, "Should have 2 edges: A->B, B->C")
		assert.Equal(t, 2, path.Length, "Path length should be 2")
		assert.Equal(t, 4.0, path.TotalWeight, "Total weight should be 1.5 + 2.5 = 4.0")

		// Verify node order (source -> target)
		assert.Equal(t, base64.StdEncoding.EncodeToString(sourceKey), path.Nodes[0])
		assert.Equal(t, base64.StdEncoding.EncodeToString(middleKey), path.Nodes[1])
		assert.Equal(t, base64.StdEncoding.EncodeToString(targetKey), path.Nodes[2])

		// Verify edge order
		assert.Equal(t, base64.StdEncoding.EncodeToString(sourceKey), path.Edges[0].Source)
		assert.Equal(t, base64.StdEncoding.EncodeToString(middleKey), path.Edges[0].Target)
		assert.Equal(t, 1.5, path.Edges[0].Weight)

		assert.Equal(t, base64.StdEncoding.EncodeToString(middleKey), path.Edges[1].Source)
		assert.Equal(t, base64.StdEncoding.EncodeToString(targetKey), path.Edges[1].Target)
		assert.Equal(t, 2.5, path.Edges[1].Weight)
	})

	t.Run("handles single node path", func(t *testing.T) {
		ms := &MetadataStore{}

		parent := make(map[string]*pathNode)
		sourceKey := []byte("A")

		parent[string(sourceKey)] = &pathNode{
			key:      sourceKey,
			distance: 0,
			hops:     0,
			parent:   nil,
		}

		path := ms.reconstructPath(parent, sourceKey)

		require.NotNil(t, path)
		assert.Len(t, path.Nodes, 1)
		assert.Empty(t, path.Edges)
		assert.Equal(t, 0, path.Length)
		assert.Equal(t, 0.0, path.TotalWeight)
	})
}

// NOTE: Shortest path functionality is tested at the store level in:
// - TestGraphQueryEngine_Execute_ShortestPath (src/store/graph_query_test.go)
// - TestGraphQueryEngine_Execute_KShortestPaths (src/store/graph_query_test.go)
//
// Multi-shard cross-shard pathfinding integration tests would require cluster setup infrastructure.
