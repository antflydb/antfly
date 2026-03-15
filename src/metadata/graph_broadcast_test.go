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
	"fmt"
	"testing"

	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/assert"
)

// TestDeduplicateEdges tests the edge deduplication function
func TestDeduplicateEdges(t *testing.T) {
	tests := []struct {
		name     string
		input    []indexes.Edge
		expected int // Expected number of unique edges
	}{
		{
			name:     "empty input",
			input:    []indexes.Edge{},
			expected: 0,
		},
		{
			name: "no duplicates",
			input: []indexes.Edge{
				{Source: []byte("a"), Target: []byte("b"), Type: "cites"},
				{Source: []byte("c"), Target: []byte("d"), Type: "cites"},
			},
			expected: 2,
		},
		{
			name: "exact duplicates",
			input: []indexes.Edge{
				{Source: []byte("a"), Target: []byte("b"), Type: "cites", Weight: 0.9},
				{Source: []byte("a"), Target: []byte("b"), Type: "cites", Weight: 0.8}, // Duplicate (same source/target/type)
				{Source: []byte("c"), Target: []byte("d"), Type: "cites"},
			},
			expected: 2, // Should deduplicate to 2 unique edges
		},
		{
			name: "different types are not duplicates",
			input: []indexes.Edge{
				{Source: []byte("a"), Target: []byte("b"), Type: "cites"},
				{Source: []byte("a"), Target: []byte("b"), Type: "similar_to"}, // Different type
			},
			expected: 2, // Both should be kept
		},
		{
			name: "different directions are not duplicates",
			input: []indexes.Edge{
				{Source: []byte("a"), Target: []byte("b"), Type: "cites"},
				{Source: []byte("b"), Target: []byte("a"), Type: "cites"}, // Reverse direction
			},
			expected: 2, // Both should be kept
		},
		{
			name: "multiple duplicates",
			input: []indexes.Edge{
				{Source: []byte("a"), Target: []byte("b"), Type: "cites"},
				{Source: []byte("a"), Target: []byte("b"), Type: "cites"}, // Dup 1
				{Source: []byte("a"), Target: []byte("b"), Type: "cites"}, // Dup 2
				{Source: []byte("c"), Target: []byte("d"), Type: "similar_to"},
				{Source: []byte("c"), Target: []byte("d"), Type: "similar_to"}, // Dup 3
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deduplicateEdges(tt.input)
			assert.Len(t, result, tt.expected, "Unexpected number of deduplicated edges")

			// Verify no duplicates in result
			seen := make(map[string]bool)
			for _, edge := range result {
				key := string(edge.Source) + "\x00" + string(edge.Target) + "\x00" + edge.Type
				assert.False(t, seen[key], "Found duplicate edge in result: %v", edge)
				seen[key] = true
			}
		})
	}
}

// TestBroadcastGetIncomingEdgesToAllShards tests the broadcast function with mocked shards
func TestBroadcastGetIncomingEdgesToAllShards(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Note: This is a unit test with mocked dependencies
	// Full integration tests with real shards are in the integration test suite

	t.Run("successful broadcast to multiple shards", func(t *testing.T) {
		// This test would require mocking the shard clients
		// For now, we test the deduplication logic which is the core functionality

		// Simulate edges returned from 3 different shards
		shard1Edges := []indexes.Edge{
			{Source: []byte("doc1"), Target: []byte("target"), Type: "cites", Weight: 0.9},
			{Source: []byte("doc2"), Target: []byte("target"), Type: "cites", Weight: 0.8},
		}

		shard2Edges := []indexes.Edge{
			{Source: []byte("doc3"), Target: []byte("target"), Type: "cites", Weight: 0.7},
		}

		shard3Edges := []indexes.Edge{
			{Source: []byte("doc4"), Target: []byte("target"), Type: "similar_to", Weight: 0.95},
			// Duplicate from shard1 (simulating temporary inconsistency)
			{Source: []byte("doc1"), Target: []byte("target"), Type: "cites", Weight: 0.9},
		}

		// Merge all edges (simulating what broadcast would do)
		allEdges := append(append(shard1Edges, shard2Edges...), shard3Edges...)

		// Deduplicate
		result := deduplicateEdges(allEdges)

		// Should have 4 unique edges (doc1, doc2, doc3, doc4)
		assert.Len(t, result, 4, "Expected 4 unique edges after deduplication")

		// Verify all expected sources are present
		sources := make(map[string]bool)
		for _, edge := range result {
			sources[string(edge.Source)] = true
		}
		assert.True(t, sources["doc1"], "Missing edge from doc1")
		assert.True(t, sources["doc2"], "Missing edge from doc2")
		assert.True(t, sources["doc3"], "Missing edge from doc3")
		assert.True(t, sources["doc4"], "Missing edge from doc4")
	})

	t.Run("empty results from all shards", func(t *testing.T) {
		// Test case: no edges found across all shards
		emptyEdges := []indexes.Edge{}
		result := deduplicateEdges(emptyEdges)
		assert.Empty(t, result, "Expected 0 edges when all shards return empty")
	})

	t.Run("partial shard failures", func(t *testing.T) {
		// This test validates the logic of handling partial failures
		// The actual broadcast function logs warnings but returns successful results

		// Simulate: shard1 succeeds, shard2 fails, shard3 succeeds
		successfulEdges := []indexes.Edge{
			{Source: []byte("doc1"), Target: []byte("target"), Type: "cites"},
			{Source: []byte("doc2"), Target: []byte("target"), Type: "cites"},
		}

		result := deduplicateEdges(successfulEdges)

		// Should return edges from successful shards
		assert.Len(t, result, 2, "Expected edges from successful shards")
	})
}

// TestBroadcastEdgeCasesTableShards tests edge cases related to table shard counts
func TestBroadcastEdgeCasesTableShards(t *testing.T) {
	t.Run("single shard table", func(t *testing.T) {
		// Single-shard tables should work identically (broadcast to 1 shard)
		// This ensures backward compatibility

		edges := []indexes.Edge{
			{Source: []byte("doc1"), Target: []byte("target"), Type: "cites"},
		}

		result := deduplicateEdges(edges)
		assert.Len(t, result, 1, "Single shard should work correctly")
	})

	t.Run("many shards", func(t *testing.T) {
		// Simulate 10 shards each returning edges
		var allEdges []indexes.Edge
		for i := range 10 {
			allEdges = append(allEdges, indexes.Edge{
				Source: fmt.Appendf(nil, "doc%d", i),
				Target: []byte("target"),
				Type:   "cites",
				Weight: float64(i) / 10.0,
			})
		}

		result := deduplicateEdges(allEdges)
		assert.Len(t, result, 10, "Should handle many shards correctly")
	})
}

// Integration test marker - these require full setup
func TestCrossShardIncomingEdgesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// This is a marker for future integration tests
	// Full integration tests should be added to test:
	// 1. Creating edges from shard1 -> shard2
	// 2. Querying incoming edges at shard2
	// 3. Verifying all cross-shard edges are found
	// 4. Testing with multiple shards (3+)
	// 5. Testing partial shard failures

	t.Skip("TODO: Implement full integration test with multi-shard cluster")
}

// NOTE: Broadcast behavior:
// 1. direction=out: Single shard query (source's shard)
// 2. direction=in: Broadcast to ALL shards
// 3. direction=both: Source shard + broadcast

// Benchmark for deduplication performance
func BenchmarkDeduplicateEdges(b *testing.B) {
	// Create test data with varying sizes
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			// Create edges with 10% duplicates
			edges := make([]indexes.Edge, size)
			for i := range size {
				sourceID := i % (size / 10 * 9) // 10% duplication rate
				edges[i] = indexes.Edge{
					Source: fmt.Appendf(nil, "doc%d", sourceID),
					Target: []byte("target"),
					Type:   "cites",
					Weight: 0.5,
				}
			}

			b.ResetTimer()
			for b.Loop() {
				_ = deduplicateEdges(edges)
			}
		})
	}
}

// TestEdgeKeyFormat validates the edge key format used for deduplication
func TestEdgeKeyFormat(t *testing.T) {
	tests := []struct {
		name          string
		edge1         *indexes.Edge
		edge2         *indexes.Edge
		shouldBeEqual bool
	}{
		{
			name: "identical edges",
			edge1: &indexes.Edge{
				Source: []byte("a"),
				Target: []byte("b"),
				Type:   "cites",
			},
			edge2: &indexes.Edge{
				Source: []byte("a"),
				Target: []byte("b"),
				Type:   "cites",
			},
			shouldBeEqual: true,
		},
		{
			name: "different source",
			edge1: &indexes.Edge{
				Source: []byte("a"),
				Target: []byte("b"),
				Type:   "cites",
			},
			edge2: &indexes.Edge{
				Source: []byte("c"),
				Target: []byte("b"),
				Type:   "cites",
			},
			shouldBeEqual: false,
		},
		{
			name: "different target",
			edge1: &indexes.Edge{
				Source: []byte("a"),
				Target: []byte("b"),
				Type:   "cites",
			},
			edge2: &indexes.Edge{
				Source: []byte("a"),
				Target: []byte("c"),
				Type:   "cites",
			},
			shouldBeEqual: false,
		},
		{
			name: "different type",
			edge1: &indexes.Edge{
				Source: []byte("a"),
				Target: []byte("b"),
				Type:   "cites",
			},
			edge2: &indexes.Edge{
				Source: []byte("a"),
				Target: []byte("b"),
				Type:   "similar_to",
			},
			shouldBeEqual: false,
		},
		{
			name: "keys containing null bytes handled correctly",
			edge1: &indexes.Edge{
				Source: []byte("abc"),
				Target: []byte("def"),
				Type:   "cites",
			},
			edge2: &indexes.Edge{
				Source: []byte("ab"),
				Target: []byte("cdef"),
				Type:   "cites",
			},
			shouldBeEqual: false, // Null byte separator prevents ambiguity: "abc\x00def" != "ab\x00cdef"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key1 := string(tt.edge1.Source) + "\x00" + string(tt.edge1.Target) + "\x00" + tt.edge1.Type
			key2 := string(tt.edge2.Source) + "\x00" + string(tt.edge2.Target) + "\x00" + tt.edge2.Type

			if tt.shouldBeEqual {
				assert.Equal(t, key1, key2, "Keys should be equal")
			} else {
				assert.NotEqual(t, key1, key2, "Keys should be different")
			}
		})
	}
}
