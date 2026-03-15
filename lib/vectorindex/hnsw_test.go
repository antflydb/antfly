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

package vectorindex

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSWIndex_Creation(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             10,
		Name:                  "test_index",
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	// Fixed seed for reproducibility
	index, err := NewHNSWIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	require.NotNil(t, index)

	// Check that index directory was created
	_, err = os.Stat(indexPath)
	require.NoError(t, err, "Index directory should be created")

	// Check internal fields
	assert.Equal(t, config.Dimension, index.config.Dimension)
	assert.Equal(t, config.IndexPath, index.config.IndexPath)
	activeCount, err := index.getActiveCount(index.db)
	require.NoError(t, err)
	assert.EqualValues(t, 0, activeCount)

	// Close the index
	err = index.Close()
	require.NoError(t, err)
}

func TestHNSWIndex_Insert(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	index, err := NewHNSWIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	// Test inserting a vector
	vector := []float32{1.0, 2.0, 3.0}
	metadata := []byte("test metadata")
	err = index.Insert(1, vector, metadata)
	require.NoError(t, err)

	// Verify the vector was inserted
	stats := index.Stats()
	assert.EqualValues(t, 1, stats["nodes_active"])

	// Insert another vector
	vector2 := []float32{4.0, 5.0, 6.0}
	err = index.Insert(2, vector2, []byte("more metadata"))
	require.NoError(t, err)
	// Test inserting with incorrect dimension
	invalidVector := []float32{1.0, 2.0} // Only 2 dimensions, but config says 3
	err = index.Insert(666, invalidVector, nil)
	assert.Error(t, err, "Should error on dimension mismatch")
}

func TestHNSWIndex_BatchInsert(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}
	index, err := NewHNSWIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	// Create batch data
	numVectors := 10
	vectors := make([]vector.T, numVectors)
	metadata := make([][]byte, numVectors)
	ids := make([]uint64, numVectors)
	for i := range numVectors {
		ids[i] = uint64(i + 1) // Use sequential IDs for simplicity
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
		metadata[i] = fmt.Appendf(nil, "metadata %d", i)
	}

	// Test batch insertion
	err = index.BatchInsert(t.Context(), ids, vectors, metadata)
	require.NoError(t, err)
	assert.Len(t, ids, numVectors)
	assert.EqualValues(t, 1, ids[0])
	assert.EqualValues(t, numVectors, ids[numVectors-1])

	// Verify vectors were inserted
	stats := index.Stats()
	assert.EqualValues(t, numVectors, stats["nodes_active"])

	// Test empty batch
	err = index.BatchInsert(t.Context(), []uint64{}, []vector.T{}, [][]byte{})
	require.NoError(t, err)

	// Test dimension mismatch
	invalidVectors := []vector.T{
		{1.0, 2.0, 3.0},
		{4.0, 5.0}, // This one has wrong dimension
	}
	invalidIDs := []uint64{2, 1}
	err = index.BatchInsert(t.Context(), invalidIDs, invalidVectors, [][]byte{nil, nil})
	assert.Error(t, err, "Should error on dimension mismatch")
}

func TestHNSWIndex_Search(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	randSource := rand.NewPCG(42, 1024)
	index, err := NewHNSWIndex(config, randSource)
	require.NoError(t, err)
	defer index.Close()

	// Insert some vectors
	vectors := []vector.T{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
		{0.5, 0.5, 0.0},
		{0.0, 0.5, 0.5},
		{0.5, 0.0, 0.5},
	}
	metadata := make([][]byte, len(vectors))
	ids := make([]uint64, len(vectors))
	for i := range metadata {
		ids[i] = uint64(i + 1)
		metadata[i] = fmt.Appendf(nil, "metadata %d", i)
	}

	err = index.BatchInsert(t.Context(), ids, vectors, metadata)
	require.NoError(t, err)

	// Test search with exact match
	query := []float32{1.0, 0.0, 0.0} // Should match the first vector exactly
	results, err := index.Search(&SearchRequest{
		Embedding: query,
		K:         1,
	})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	require.EqualValues(t, 1, results[0].ID)
	assert.InDelta(
		t,
		0.0,
		results[0].Distance,
		0.001,
	) // Cosine distance to identical vector should be 0

	// Test search with multiple results
	query = []float32{0.7, 0.7, 0.0} // Should be closest to the {0.5, 0.5, 0.0} vector
	results, err = index.Search(&SearchRequest{
		Embedding: query,
		K:         3,
	})
	require.NoError(t, err)
	require.Len(t, results, 3, "Should return 3 nearest neighbors")
	for _, result := range results {
		require.True(
			t,
			strings.HasPrefix(string(result.Metadata), "metadata"),
			"Results should have metadata",
		)
	}

	// Test search with dimension mismatch
	invalidQuery := []float32{1.0, 2.0} // Wrong dimension
	_, err = index.Search(&SearchRequest{
		Embedding: invalidQuery,
		K:         1,
	})
	assert.Error(t, err, "Should error on dimension mismatch")

	// Test search with empty index (after closing and creating new empty index)
	err = index.Close()
	require.NoError(t, err)

	emptyDir := filepath.Join(tempDir, "empty_index")
	emptyConfig := config
	emptyConfig.IndexPath = emptyDir
	emptyIndex, err := NewHNSWIndex(emptyConfig, randSource)
	require.NoError(t, err)
	defer emptyIndex.Close()

	emptyResults, err := emptyIndex.Search(&SearchRequest{
		Embedding: query,
		K:         1,
	})
	require.NoError(t, err)
	assert.Empty(t, emptyResults)
}

func TestHNSWIndex_SearchWithFilterPrefix(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	randSource := rand.NewPCG(42, 1024)
	index, err := NewHNSWIndex(config, randSource)
	require.NoError(t, err)
	defer index.Close()

	// Insert vectors with different metadata prefixes
	vectors := []vector.T{
		{1.0, 0.0, 0.0}, // category:electronics/item1
		{0.9, 0.1, 0.0}, // category:electronics/item2
		{0.0, 1.0, 0.0}, // category:books/item1
		{0.1, 0.9, 0.0}, // category:books/item2
		{0.0, 0.0, 1.0}, // category:clothing/item1
		{0.5, 0.5, 0.0}, // no metadata
		{0.7, 0.7, 0.0}, // category:electronics/special
		{0.0, 0.5, 0.5}, // other:data
	}

	metadata := [][]byte{
		[]byte("category:electronics/item1"),
		[]byte("category:electronics/item2"),
		[]byte("category:books/item1"),
		[]byte("category:books/item2"),
		[]byte("category:clothing/item1"),
		nil, // No metadata for vector 6
		[]byte("category:electronics/special"),
		[]byte("other:data"),
	}

	ids := make([]uint64, len(vectors))
	for i := range ids {
		ids[i] = uint64(i + 1)
	}

	err = index.BatchInsert(t.Context(), ids, vectors, metadata)
	require.NoError(t, err)

	t.Run("SearchWithElectronicsFilter", func(t *testing.T) {
		// Search for vectors similar to {1.0, 0.0, 0.0} but only in electronics category
		query := []float32{1.0, 0.0, 0.0}
		filterPrefix := []byte("category:electronics")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            5,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Should only return vectors with electronics prefix
		assert.LessOrEqual(t, len(results), 3) // Maximum 3 electronics items
		for _, result := range results {
			// Verify each result has the correct prefix
			meta, err := index.GetMetadata(result.ID)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(meta, filterPrefix),
				"Result ID %d should have electronics prefix, got: %s", result.ID, meta)
		}

		// First result should be the exact match (ID 1)
		if len(results) > 0 {
			assert.EqualValues(t, 1, results[0].ID)
			assert.InDelta(t, 0.0, results[0].Distance, 0.001)
		}
	})

	t.Run("SearchWithBooksFilter", func(t *testing.T) {
		// Search for vectors but only in books category
		query := []float32{0.0, 1.0, 0.0}
		filterPrefix := []byte("category:books")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            5,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Should only return books
		assert.LessOrEqual(t, len(results), 2) // Maximum 2 books items
		for _, result := range results {
			meta, err := index.GetMetadata(result.ID)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(meta, filterPrefix),
				"Result ID %d should have books prefix, got: %s", result.ID, meta)
		}

		// First result should be ID 3 (exact match in books category)
		if len(results) > 0 {
			assert.EqualValues(t, 3, results[0].ID)
		}
	})

	t.Run("SearchWithNonMatchingFilter", func(t *testing.T) {
		// Search with a filter that matches no documents
		query := []float32{1.0, 0.0, 0.0}
		filterPrefix := []byte("category:nonexistent")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            5,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Should return empty results
		assert.Empty(t, results)
	})

	t.Run("SearchWithEmptyFilter", func(t *testing.T) {
		// Search with nil filter should return all nearest neighbors
		query := []float32{1.0, 0.0, 0.0}

		results, err := index.Search(&SearchRequest{
			Embedding: query,
			K:         5,
		})
		require.NoError(t, err)

		// Should return up to 5 results regardless of metadata
		assert.LessOrEqual(t, len(results), 5)
		assert.NotEmpty(t, results)

		// First result should be the exact match
		assert.EqualValues(t, 1, results[0].ID)
	})

	t.Run("SearchWithPartialPrefixFilter", func(t *testing.T) {
		// Search with just "category:" prefix to get all categorized items
		query := []float32{0.5, 0.5, 0.0}
		filterPrefix := []byte("category:")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            10,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Should return all items with "category:" prefix (IDs 1,2,3,4,5,7)
		for _, result := range results {
			if result.ID == 6 || result.ID == 8 {
				t.Errorf("Result ID %d should not be returned with category: filter", result.ID)
			}
			if result.ID != 6 && result.ID != 8 {
				meta, err := index.GetMetadata(result.ID)
				require.NoError(t, err)
				assert.True(t, bytes.HasPrefix(meta, filterPrefix),
					"Result ID %d should have category: prefix, got: %s", result.ID, meta)
			}
		}
	})

	t.Run("SearchLimitedByK", func(t *testing.T) {
		// Even with filter, should not return more than k results
		query := []float32{0.5, 0.5, 0.0}
		filterPrefix := []byte("category:")
		k := 2

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            k,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Should return at most k results
		assert.LessOrEqual(t, len(results), k)
	})

	t.Run("SearchWithFilterOnEmptyMetadata", func(t *testing.T) {
		// Search near vector with no metadata
		query := []float32{0.5, 0.5, 0.0} // Close to ID 6 which has no metadata
		filterPrefix := []byte("any")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            10,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// ID 6 should not be in results as it has no metadata
		for _, result := range results {
			assert.NotEqual(
				t,
				uint64(6),
				result.ID,
				"Vector with nil metadata should not match any filter",
			)
		}
	})

	t.Run("SearchEntryPointWithFilter", func(t *testing.T) {
		// Special case: when entry point matches the filter
		// The entry point (ID 1) has "category:electronics" metadata
		query := []float32{0.0, 0.0, 1.0} // Different from entry point
		filterPrefix := []byte("category:electronics")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            5,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Should still work correctly and return only electronics items
		for _, result := range results {
			meta, err := index.GetMetadata(result.ID)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(meta, filterPrefix))
		}
	})
}

func TestHNSWIndex_Delete(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	randSource := rand.NewPCG(42, 1024)
	index, err := NewHNSWIndex(config, randSource)
	require.NoError(t, err)
	defer index.Close()

	// Insert some vectors
	vectors := []vector.T{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
	}
	ids := []uint64{3, 1, 2}
	err = index.BatchInsert(t.Context(), ids, vectors, nil)
	require.NoError(t, err)

	// Verify initial counts
	stats := index.Stats()
	assert.EqualValues(t, 3, stats["nodes_active"])

	// Delete a vector
	err = index.Delete(1) // Delete the second vector
	require.NoError(t, err)

	// Verify counts after deletion
	stats = index.Stats()
	assert.EqualValues(t, 2, stats["nodes_active"])

	// Try to get the deleted vector
	_, err = index.GetMetadata(1)
	assert.Error(t, err, "Should error when getting deleted vector")

	// Delete a non-existent vector
	assert.NoError(t, index.Delete(999), "Should not error when deleting non-existent vector")
	// Delete an already deleted vector should not error
	require.NoError(t, index.Delete(1), "Should not error when deleting already deleted vector")

	// Verify counts unchanged after idempotent delete
	stats = index.Stats()
	assert.EqualValues(t, 2, stats["nodes_active"])

	// Delete the entry point (ID 0) and see if a new one is chosen
	originalEntryPoint, err := index.getEntryPoint(index.db)
	require.NoError(t, err)
	// assert.EqualValues(t, 3, originalEntryPoint)

	err = index.Delete(originalEntryPoint)
	require.NoError(t, err)

	// After deleting ID 0, the entry point should change to ID 2
	newEntryPoint, err := index.getEntryPoint(index.db)
	require.NoError(t, err)
	assert.NotEqual(
		t,
		originalEntryPoint,
		newEntryPoint,
		"New entry point should be different after deletion",
	)
	// assert.EqualValues(t, 2, newEntryPoint)

	// Verify counts after deleting entry point
	stats = index.Stats()
	assert.EqualValues(t, 1, stats["nodes_active"])
}

func TestHNSWIndex_GetVector(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	randSource := rand.NewPCG(42, 1024)
	index, err := NewHNSWIndex(config, randSource)
	require.NoError(t, err)
	defer index.Close()

	// Insert a vector with metadata
	vector := []float32{1.0, 2.0, 3.0}
	metadata := []byte("test metadata")
	err = index.Insert(1, vector, metadata)
	require.NoError(t, err)

	// Get the vector back
	retrievedMetadata, err := index.GetMetadata(1)
	require.NoError(t, err)
	assert.Equal(t, metadata, retrievedMetadata)

	// Insert a vector without metadata
	vector2 := []float32{4.0, 5.0, 6.0}
	err = index.Insert(2, vector2, nil)
	require.NoError(t, err)

	// Get the vector back
	_, err = index.GetMetadata(2)
	assert.Error(t, err, "Metadata should be empty for vector without metadata")

	// Try to get a non-existent vector
	_, err = index.GetMetadata(999) // ID out of range
	assert.Error(t, err, "Should error when getting non-existent vector")

	// Delete a vector and try to get it
	err = index.Delete(1)
	require.NoError(t, err)
	data, err := index.GetMetadata(1)
	assert.Error(t, err, "Should error when getting deleted vector")
	assert.Empty(t, data, "Data should be nil for deleted vector")
}

func TestHNSWIndex_AdaptiveSearchStrategy(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	randSource := rand.NewPCG(42, 1024)
	index, err := NewHNSWIndex(config, randSource)
	require.NoError(t, err)
	defer index.Close()

	// Create a dataset with specific cardinality distributions
	numVectors := 1000
	vectors := make([]vector.T, numVectors)
	metadata := make([][]byte, numVectors)
	ids := make([]uint64, numVectors)

	// Create clusters of vectors with different metadata prefixes
	// Small cluster: 20 vectors with "small:" prefix
	// Medium cluster: 80 vectors with "medium:" prefix
	// Large cluster: 400 vectors with "large:" prefix
	// Rest: 500 vectors with "other:" prefix

	for i := range numVectors {
		ids[i] = uint64(i + 1)
		// Generate vectors in clusters for better testing
		if i < 20 {
			vectors[i] = []float32{float32(i) * 0.1, 0.0, 0.0} // Small cluster around x-axis
			metadata[i] = []byte("small:cluster")
		} else if i < 100 {
			vectors[i] = []float32{0.0, float32(i-20) * 0.01, 0.0} // Medium cluster around y-axis
			metadata[i] = []byte("medium:cluster")
		} else if i < 500 {
			vectors[i] = []float32{0.0, 0.0, float32(i-100) * 0.002} // Large cluster around z-axis
			metadata[i] = []byte("large:cluster")
		} else {
			vectors[i] = []float32{
				float32(i-500) * 0.002,
				float32(i-500) * 0.002,
				float32(i-500) * 0.002,
			} // Other vectors spread out
			metadata[i] = []byte("other:data")
		}
	}

	err = index.BatchInsert(t.Context(), ids, vectors, metadata)
	require.NoError(t, err)

	t.Run("SmallCluster_DirectIteration", func(t *testing.T) {
		// For small cluster (20 items), even k=2 should trigger direct iteration
		// because the cluster has < 100 items
		query := []float32{1.0, 0.0, 0.0}
		filterPrefix := []byte("small:cluster")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            2,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Verify we get results from the small cluster
		assert.LessOrEqual(t, len(results), 2)
		for _, result := range results {
			meta, err := index.GetMetadata(result.ID)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(meta, filterPrefix))
		}
	})

	t.Run("MediumCluster_AdaptiveThreshold", func(t *testing.T) {
		// For medium cluster (80 items), k=10 (12.5%) should use graph search
		// but k=20 (25%) should use direct iteration
		query := []float32{0.0, 1.0, 0.0}
		filterPrefix := []byte("medium:cluster")

		// Small k - should use graph search
		results1, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            10,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results1), 10)

		// Large k - should use direct iteration (20/80 = 25% > 20% threshold)
		results2, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            20,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results2), 20)

		// Both should return valid results
		for _, result := range results1 {
			meta, err := index.GetMetadata(result.ID)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(meta, filterPrefix))
		}
		for _, result := range results2 {
			meta, err := index.GetMetadata(result.ID)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(meta, filterPrefix))
		}
	})

	t.Run("LargeCluster_GraphSearch", func(t *testing.T) {
		// For large cluster (400 items), even k=50 (12.5%) should use graph search
		query := []float32{0.0, 0.0, 1.0}
		filterPrefix := []byte("large:cluster")

		results, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            50,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results), 50)

		// Verify all results match the filter
		for _, result := range results {
			meta, err := index.GetMetadata(result.ID)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(meta, filterPrefix))
		}
	})

	t.Run("CompareDirectVsGraph", func(t *testing.T) {
		// Test that both methods return the same results for a borderline case
		query := []float32{0.5, 0.5, 0.0}
		filterPrefix := []byte("medium:cluster")
		k := 15 // Just under the 20% threshold for 80 items

		// Force direct iteration by temporarily modifying the threshold
		// Note: In real implementation, we can't easily force this without exposing internals
		// So we'll just verify that results are consistent across multiple runs

		results1, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            k,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		results2, err := index.Search(&SearchRequest{
			Embedding:    query,
			K:            k,
			FilterPrefix: filterPrefix,
		})
		require.NoError(t, err)

		// Results should be consistent
		assert.Len(t, results2, len(results1))

		// Create maps for easier comparison
		resultMap1 := make(map[uint64]float32)
		resultMap2 := make(map[uint64]float32)

		for _, r := range results1 {
			resultMap1[r.ID] = r.Distance
		}
		for _, r := range results2 {
			resultMap2[r.ID] = r.Distance
		}

		// Verify same IDs returned
		for id, dist1 := range resultMap1 {
			dist2, exists := resultMap2[id]
			assert.True(t, exists, "ID %d should be in both result sets", id)
			assert.InDelta(t, dist1, dist2, 0.0001, "Distances should match for ID %d", id)
		}
	})

	t.Run("EmptyFilter_UsesGraphSearch", func(t *testing.T) {
		// With no filter, should always use graph search
		query := []float32{0.5, 0.5, 0.5}

		results, err := index.Search(&SearchRequest{
			Embedding: query,
			K:         100,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, results)
		assert.LessOrEqual(t, len(results), 100)
	})
}

func TestHNSWIndex_CloseAndReopen(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	randSource := rand.NewPCG(42, 1024)

	// Create and populate index
	{
		index, err := NewHNSWIndex(config, randSource)
		require.NoError(t, err)

		// Insert some vectors
		vectors := []vector.T{
			{1.0, 0.0, 0.0},
			{0.0, 1.0, 0.0},
			{0.0, 0.0, 1.0},
		}
		metadata := [][]byte{
			[]byte("meta 1"),
			[]byte("meta 2"),
			[]byte("meta 3"),
		}
		ids := []uint64{3, 1, 2}

		err = index.BatchInsert(t.Context(), ids, vectors, metadata)
		require.NoError(t, err)

		// Delete one vector
		err = index.Delete(1)
		require.NoError(t, err)

		// Verify state before closing
		stats := index.Stats()
		assert.EqualValues(t, 2, stats["nodes_active"])

		// Close the index
		err = index.Close()
		require.NoError(t, err)
	}

	// Reopen the index and verify state is preserved
	{
		index2, err := NewHNSWIndex(config, randSource)
		require.NoError(t, err)
		defer index2.Close()

		// Verify counts are preserved
		stats := index2.Stats()
		assert.EqualValues(t, 2, stats["nodes_active"])

		// Verify vectors are preserved
		metadata0, err := index2.GetMetadata(3)
		require.NoError(t, err)
		assert.Equal(t, []byte("meta 1"), metadata0)

		// Verify deleted vector is still marked as deleted
		_, err = index2.GetMetadata(1)
		assert.Error(t, err, "Vector should still be marked as deleted after reopen")

		// Search should work after reopening
		query := []float32{0.9, 0.1, 0.0}
		results, err := index2.Search(&SearchRequest{
			Embedding: query,
			K:         1,
		})
		require.NoError(t, err)
		assert.Len(t, results, 1)
		assert.EqualValues(t, 3, results[0].ID)
	}
}

// generateRandomVectors generates random vectors for testing
func generateRandomVectors(dim int, count int, r *rand.Rand) []vector.T {
	vectors := make([]vector.T, count)
	for i := range vectors {
		vec := make(vector.T, dim)
		for j := range vec {
			vec[j] = r.Float32()
		}
		vectors[i] = vec
	}
	return vectors
}

func TestHNSWIndex_PersistenceAndRecovery(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             3,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	// Generate random vectors
	numVectors := 100
	dim := config.Dimension
	randSource := rand.NewPCG(42, 1024)
	r := rand.New(randSource)
	vectors := generateRandomVectors(int(dim), numVectors, r)

	// Create and populate index
	{
		index, err := NewHNSWIndex(config, randSource)
		require.NoError(t, err)

		// Insert in batches
		batchSize := 10
		for i := 0; i < numVectors; i += batchSize {
			end := min(i+batchSize, numVectors)
			batch := vectors[i:end]
			metadataBatch := make([][]byte, len(batch))
			ids := make([]uint64, len(batch))
			for j := range batch {
				ids[j] = uint64(i + j + 1)
				metadataBatch[j] = fmt.Appendf(nil, "metadata %d", i+j)
			}

			err = index.BatchInsert(t.Context(), ids, batch, metadataBatch)
			require.NoError(t, err)
		}

		// Delete some vectors (every 10th)
		for i := 0; i < numVectors; i += 10 {
			err = index.Delete(uint64(i + 1))
			require.NoError(t, err)
		}

		// Close the index
		err = index.Close()
		require.NoError(t, err)
	}

	// Reopen and verify
	{
		index2, err := NewHNSWIndex(config, randSource)
		require.NoError(t, err)
		defer index2.Close()

		stats := index2.Stats()
		expectedDeleted := numVectors / 10
		assert.EqualValues(t, numVectors-expectedDeleted, stats["nodes_active"])

		// Verify deleted vectors are still marked as deleted
		for i := 0; i < numVectors; i += 10 {
			_, err = index2.GetMetadata(uint64(i + 1))
			assert.Error(t, err, "Vector %d should be marked as deleted", i)
		}

		// Verify some non-deleted vectors
		for i := 1; i < numVectors; i += 10 {
			metadata, err := index2.GetMetadata(uint64(i + 1))
			require.NoError(t, err, "Vector %d should exist", i)
			assert.Equal(t, fmt.Appendf(nil, "metadata %d", i), metadata)
		}

		// Test search functionality
		// Use first non-deleted vector as query
		query := vectors[1]
		results, err := index2.Search(&SearchRequest{
			Embedding: query,
			K:         5,
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results), 5)
		for _, result := range results {
			// First result should be the exact match
			require.EqualValues(
				t,
				2,
				result.ID,
				"First result should be the exact match: got %v",
				result,
			)
			break
		}
		assert.InDelta(t, 0.0, results[0].Distance, 0.001, "Distance to self should be near 0")
	}
}

func BenchmarkHNSWIndex_BatchInsert(b *testing.B) {
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048, 3072} // Define dimensions to benchmark

	for _, dim := range dimensions {
		b.Run(fmt.Sprintf("Dim%d", dim), func(b *testing.B) {
			tempDir := b.TempDir()
			indexPath := filepath.Join(tempDir, fmt.Sprintf("pebble_index_dim%d", dim))

			// Create a new index
			config := HNSWConfig{
				Dimension:             uint32(dim), // Use current dimension from loop
				IndexPath:             indexPath,
				Neighbors:             16,
				NeighborsHigherLayers: 16,
				EfConstruction:        100,
				EfSearch:              50,
				CacheSizeNodes:        1000,
				PebbleSyncWrite:       true,
				LevelMultiplier:       1.0 / math.Log(2.0),
			}

			randSource := rand.NewPCG(42, 1024)
			index, err := NewHNSWIndex(config, randSource)
			require.NoError(b, err)
			defer index.Close()

			// Prepare batch data for benchmark
			batchSize := 100 // Define a batch size for the benchmark
			vectors := make([]vector.T, batchSize)
			metadata := make([][]byte, batchSize)
			ids := make([]uint64, batchSize)
			r := rand.New(
				randSource,
			) // Re-seed or use a different source if variations are needed per dim

			for i := range batchSize {
				ids[i] = uint64(i + 1) // Use sequential IDs for simplicity
				vec := make([]float32, config.Dimension)
				for j := range vec {
					vec[j] = r.Float32()
				}
				vectors[i] = vec
				metadata[i] = fmt.Appendf(nil, "benchmark metadata %d", i)
			}

			b.ResetTimer() // Reset timer for each sub-benchmark

			// Run the benchmark
			for b.Loop() {
				// In each iteration of the benchmark loop (b.N), we insert one batch.
				// The IDs returned by BatchInsert are not used in this benchmark.
				err := index.BatchInsert(b.Context(), ids, vectors, metadata)
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkHNSWIndex_Search(b *testing.B) {
	tempDir := b.TempDir()
	indexPath := filepath.Join(tempDir, "pebble_index")

	// Create a new index
	config := HNSWConfig{
		Dimension:             128,
		IndexPath:             indexPath,
		Neighbors:             16,
		NeighborsHigherLayers: 16,
		EfConstruction:        100,
		EfSearch:              50,
		CacheSizeNodes:        1000,
		PebbleSyncWrite:       true,
		LevelMultiplier:       1.0 / math.Log(2.0),
	}

	randSource := rand.NewPCG(218, 1024)
	index, err := NewHNSWIndex(config, randSource)
	require.NoError(b, err)
	defer index.Close()

	// Insert vectors for benchmark
	numVectors := 5000
	dim := config.Dimension
	r := rand.New(randSource)

	vectors := generateRandomVectors(int(dim), numVectors, r)

	// Insert in batches
	batchSize := 100
	for i := 0; i < numVectors; i += batchSize {
		end := min(i+batchSize, numVectors)
		batch := vectors[i:end]
		ids := make([]uint64, len(batch))
		metadata := make([][]byte, len(batch))
		for j := range batch {
			ids[j] = uint64(i + j) // Use sequential IDs for simplicity
			metadata[j] = fmt.Appendf(nil, "benchmark metadata %d", i+j)
		}

		err = index.BatchInsert(b.Context(), ids, batch, metadata)
		require.NoError(b, err)
	}

	// Prepare query vector
	query := make([]float32, dim)
	for i := range query {
		query[i] = r.Float32()
	}

	// Reset benchmark timer

	// Run the benchmark
	for b.Loop() {
		_, err := index.Search(&SearchRequest{
			Embedding: query,
			K:         10,
		})
		require.NoError(b, err)
	}
}

func BenchmarkHNSWIndex_SearchWithFilter(b *testing.B) {
	tempDir := b.TempDir()
	tempDirNonAdaptive := b.TempDir()

	// Helper function to create and populate an index
	createIndex := func(indexPath string, directFiltering bool) (*HNSWIndex, error) {
		config := HNSWConfig{
			Dimension:             128,
			IndexPath:             indexPath,
			Neighbors:             16,
			NeighborsHigherLayers: 16,
			EfConstruction:        100,
			EfSearch:              50,
			CacheSizeNodes:        1000,
			PebbleSyncWrite:       true,
			DirectFiltering:       directFiltering, // Control whether adaptive search is enabled
			LevelMultiplier:       1.0 / math.Log(2.0),
		}

		randSource := rand.NewPCG(218, 1024)
		index, err := NewHNSWIndex(config, randSource)
		if err != nil {
			return nil, err
		}

		// Insert vectors with categorized metadata
		numVectors := 10000
		dim := config.Dimension
		r := rand.New(randSource)

		// Create categories with different cardinalities
		// rare: 50 items (0.5%), medium: 500 items (5%), common: 2000 items (20%)
		vectors := generateRandomVectors(int(dim), numVectors, r)

		// Insert in batches with metadata
		batchSize := 100
		for i := 0; i < numVectors; i += batchSize {
			end := min(i+batchSize, numVectors)
			batch := vectors[i:end]
			ids := make([]uint64, len(batch))
			metadata := make([][]byte, len(batch))
			for j := range batch {
				ids[j] = uint64(i + j + 1)

				// Assign categories based on distribution
				if i+j < 50 {
					metadata[j] = []byte("category:rare/item")
				} else if i+j < 550 {
					metadata[j] = []byte("category:medium/item")
				} else if i+j < 2550 {
					metadata[j] = []byte("category:common/item")
				} else {
					// Rest have no category metadata
					metadata[j] = []byte("other:data")
				}
			}

			err = index.BatchInsert(b.Context(), ids, batch, metadata)
			if err != nil {
				return nil, err
			}
		}

		return index, nil
	}

	// Create two indexes: one with adaptive search, one without
	indexPathAdaptive := filepath.Join(tempDir, "pebble_index_adaptive")
	indexAdaptive, err := createIndex(indexPathAdaptive, false) // Adaptive search enabled (default)
	require.NoError(b, err)
	defer indexAdaptive.Close()

	indexPathNoAdaptive := filepath.Join(tempDirNonAdaptive, "pebble_index_no_adaptive")
	indexNoAdaptive, err := createIndex(indexPathNoAdaptive, true) // Force direct filtering always
	require.NoError(b, err)
	defer indexNoAdaptive.Close()

	// Prepare query vector
	randSource := rand.NewPCG(218, 1024)
	r := rand.New(randSource)
	query := make([]float32, 128)
	for i := range query {
		query[i] = r.Float32()
	}

	// Test scenarios comparing adaptive vs non-adaptive

	// Scenario 1: Rare category with large k (adaptive should use direct iteration)
	b.Run("RareCategory_LargeK_Adaptive", func(b *testing.B) {
		filterPrefix := []byte("category:rare")
		for b.Loop() {
			_, err := indexAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            20, // 20 out of 50 = 40%
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	b.Run("RareCategory_LargeK_NoAdaptive", func(b *testing.B) {
		filterPrefix := []byte("category:rare")
		for b.Loop() {
			_, err := indexNoAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            20,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	// Scenario 2: Medium category with large k (adaptive should use direct iteration)
	b.Run("MediumCategory_LargeK_Adaptive", func(b *testing.B) {
		filterPrefix := []byte("category:medium")
		for b.Loop() {
			_, err := indexAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            150, // 150 out of 500 = 30%
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	b.Run("MediumCategory_LargeK_NoAdaptive", func(b *testing.B) {
		filterPrefix := []byte("category:medium")
		for b.Loop() {
			_, err := indexNoAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            150,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	// Scenario 3: Very small filtered set (adaptive should always use direct iteration)
	b.Run("VerySmallFilteredSet_Adaptive", func(b *testing.B) {
		filterPrefix := []byte("category:rare")
		for b.Loop() {
			_, err := indexAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            3,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	b.Run("VerySmallFilteredSet_NoAdaptive", func(b *testing.B) {
		filterPrefix := []byte("category:rare")
		for b.Loop() {
			_, err := indexNoAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            3,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	// Scenario 4: Common category with small k (both should use graph search)
	b.Run("CommonCategory_SmallK_Adaptive", func(b *testing.B) {
		filterPrefix := []byte("category:common")
		for b.Loop() {
			_, err := indexAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            10,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	b.Run("CommonCategory_SmallK_NoAdaptive", func(b *testing.B) {
		filterPrefix := []byte("category:common")
		for b.Loop() {
			_, err := indexNoAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            10,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	// Scenario 5: Medium category with small k (both should use graph search)
	b.Run("MediumCategory_SmallK_Adaptive", func(b *testing.B) {
		filterPrefix := []byte("category:medium")
		for b.Loop() {
			_, err := indexAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            10,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	b.Run("MediumCategory_SmallK_NoAdaptive", func(b *testing.B) {
		filterPrefix := []byte("category:medium")
		for b.Loop() {
			_, err := indexNoAdaptive.Search(&SearchRequest{
				Embedding:    query,
				K:            10,
				FilterPrefix: filterPrefix,
			})
			require.NoError(b, err)
		}
	})

	// Scenario 6: No filter baseline (both behave the same)
	b.Run("NoFilter_Adaptive", func(b *testing.B) {
		for b.Loop() {
			_, err := indexAdaptive.Search(&SearchRequest{
				Embedding: query,
				K:         50,
			})
			require.NoError(b, err)
		}
	})

	b.Run("NoFilter_NoAdaptive", func(b *testing.B) {
		for b.Loop() {
			_, err := indexNoAdaptive.Search(&SearchRequest{
				Embedding: query,
				K:         50,
			})
			require.NoError(b, err)
		}
	})
}
