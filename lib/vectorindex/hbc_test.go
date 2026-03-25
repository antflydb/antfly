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
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/vector"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a pebble DB and populate it with vectors using vectorindex.Batch
// This simulates the production scenario where vectors are stored in the main pebble instance
// The metadata in the batch becomes the docID used to store vectors in pebble
func setupPebbleWithVectors(t testing.TB, indexName string, batch *Batch) *pebble.DB {
	t.Helper()

	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	// The suffix format matches what's used in production: ":i:<indexName>:e"
	suffix := fmt.Appendf(nil, ":i:%s:e", indexName)

	// Ensure MetadataList is initialized
	if batch.MetadataList == nil {
		batch.MetadataList = make([][]byte, len(batch.IDs))
	}

	// Write vectors to pebble with the expected key format
	pbatch := db.NewBatch()
	defer pbatch.Close()

	for i, id := range batch.IDs {
		// Use metadata as docID, or create default if not provided
		var docID []byte
		if batch.MetadataList[i] != nil {
			docID = batch.MetadataList[i]
		} else {
			docID = fmt.Appendf(nil, "doc_%d", id)
			batch.MetadataList[i] = docID
		}

		// Store the vector with suffix using the new format with hashID prefix
		vecKey := append(bytes.Clone(docID), suffix...)
		vecData := make([]byte, 0, 8+4*(len(batch.Vectors[i])+1))
		// Use a dummy hashID of 0 for tests
		vecData, err = EncodeEmbeddingWithHashID(vecData, batch.Vectors[i], 0)
		require.NoError(t, err)
		require.NoError(t, pbatch.Set(vecKey, vecData, nil))
	}
	require.NoError(t, pbatch.Commit(pebble.Sync))

	return db
}

func TestHBCIndex_Creation(t *testing.T) {
	// Create empty pebble DB
	db := setupPebbleWithVectors(t, "test_hbc", &Batch{})

	config := HBCConfig{
		Dimension:       10,
		Name:            "test_hbc",
		BranchingFactor: 4,
		LeafSize:        10,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	require.NotNil(t, index)

	// Check configuration
	assert.Equal(t, config.Dimension, index.config.Dimension)
	assert.Equal(t, config.BranchingFactor, index.config.BranchingFactor)
	assert.Equal(t, config.LeafSize, index.config.LeafSize)
	assert.EqualValues(t, 0, index.TotalVectors())

	err = index.Close()
	require.NoError(t, err)
}

func TestHBCIndex_InsertAndSearch(t *testing.T) {
	// Prepare vectors
	vectors := []vector.T{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
		{0.5, 0.5, 0.0},
		{0.0, 0.5, 0.5},
		{0.5, 0.0, 0.5},
	}

	ids := make([]uint64, len(vectors))
	metadata := make([][]byte, len(vectors))
	for i := range vectors {
		ids[i] = uint64(i + 1)
		metadata[i] = fmt.Appendf(nil, "meta_%d", i)
	}

	// Pre-populate pebble DB with vectors
	db := setupPebbleWithVectors(t, "test_hbc", &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})

	config := HBCConfig{
		Dimension:       3,
		Name:            "test_hbc",
		BranchingFactor: 4,
		LeafSize:        5,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	// Insert vectors into the index
	err = index.Batch(t.Context(), &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})
	require.NoError(t, err)
	assert.EqualValues(t, len(vectors), index.TotalVectors())

	// Search for exact match
	query := []float32{1.0, 0.0, 0.0}
	results, err := index.Search(&SearchRequest{
		Embedding: query,
		K:         3,
	})
	require.NoError(t, err)
	require.Len(t, results, 3)

	// First result should be exact match
	assert.EqualValues(t, 1, results[0].ID)
	assert.InDelta(t, 0.0, results[0].Distance, 0.001)
	assert.Equal(t, []byte("meta_0"), results[0].Metadata)
}

func TestHBCIndex_BulkBuildRecursive(t *testing.T) {
	vectors := []vector.T{
		{0.00, 0.00},
		{0.02, 0.01},
		{0.04, 0.00},
		{5.00, 5.00},
		{5.02, 5.01},
		{5.04, 5.00},
	}
	ids := []uint64{1, 2, 3, 4, 5, 6}
	metadata := [][]byte{
		[]byte("doc:1"),
		[]byte("doc:2"),
		[]byte("doc:3"),
		[]byte("doc:4"),
		[]byte("doc:5"),
		[]byte("doc:6"),
	}
	batch := &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	}

	db := setupPebbleWithVectors(t, "test_hbc_bulk", batch)
	config := HBCConfig{
		Dimension:       2,
		Name:            "test_hbc_bulk",
		BranchingFactor: 2,
		LeafSize:        2,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)

	require.NoError(t, index.BulkBuild(t.Context(), batch))
	assert.EqualValues(t, len(ids), index.TotalVectors())

	results, err := index.Search(&SearchRequest{
		Embedding: []float32{0.0, 0.0},
		K:         3,
	})
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.EqualValues(t, 1, results[0].ID)

	require.NoError(t, index.Close())

	reopened, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer reopened.Close()

	results, err = reopened.Search(&SearchRequest{
		Embedding: []float32{5.03, 5.01},
		K:         3,
	})
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.Contains(t, []uint64{4, 5, 6}, results[0].ID)
}

func TestHBCIndex_BulkBuildRejectsDuplicateIDs(t *testing.T) {
	batch := &Batch{
		IDs:          []uint64{1, 1},
		Vectors:      []vector.T{{0, 0}, {1, 1}},
		MetadataList: [][]byte{[]byte("doc:1"), []byte("doc:1b")},
	}
	db := setupPebbleWithVectors(t, "test_hbc_bulk_dupe", batch)
	config := HBCConfig{
		Dimension:       2,
		Name:            "test_hbc_bulk_dupe",
		BranchingFactor: 2,
		LeafSize:        2,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.BulkBuild(t.Context(), batch)
	require.ErrorIs(t, err, ErrDuplicateVectorID)
}

func TestHBCIndex_TreeSplitting(t *testing.T) {
	// Insert enough vectors to trigger splits
	numVectors := 20
	vectors := make([]vector.T, numVectors)
	ids := make([]uint64, numVectors)

	r := rand.New(rand.NewPCG(42, 1024))
	for i := range vectors {
		ids[i] = uint64(i + 1)
		vectors[i] = []float32{
			r.Float32(),
			r.Float32(),
			r.Float32(),
		}
	}

	// Pre-populate pebble DB with vectors
	batch := &Batch{
		IDs:     ids,
		Vectors: vectors,
	}
	db := setupPebbleWithVectors(t, "test_hbc", batch)

	config := HBCConfig{
		Dimension:       3,
		Name:            "test_hbc",
		BranchingFactor: 2,
		LeafSize:        3, // Small leaf size to trigger splits
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.Batch(t.Context(), batch)
	require.NoError(t, err)
	assert.EqualValues(t, numVectors, index.TotalVectors())

	// Verify first vector was stored correctly by reading it back from pebble
	docID := fmt.Appendf(nil, "doc_%d", ids[0])
	suffix := fmt.Appendf(nil, ":i:%s:e", "test_hbc")
	vecKey := append(bytes.Clone(docID), suffix...)
	vecData, closer, err := index.vectorDB.Get(vecKey)
	require.NoError(t, err)
	_, storedVec, _, err := DecodeEmbeddingWithHashID(vecData)
	closer.Close()
	require.NoError(t, err)
	assert.Equal(t, vectors[0], storedVec, "First vector should be stored correctly")

	// Verify tree structure by checking stats
	stats := index.Stats()
	nodeCount := stats["total_nodes"].(uint64)
	assert.Greater(t, nodeCount, uint64(1), "Tree should have split into multiple nodes")

	// Search should still work correctly
	results, err := index.Search(&SearchRequest{
		Embedding: vectors[0],
		K:         5,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, results)

	// Debug: print what we're getting
	t.Logf("Searching for vector[0]: %v", vectors[0])
	t.Logf("Expected ID: %d", ids[0])
	for i, result := range results {
		t.Logf("Result %d: ID=%d, Distance=%f", i, result.ID, result.Distance)
	}
	t.Logf("Found HBC Index with structure:\n%v", index.PrettyPrint(nil, nil))

	// First result should be the query vector itself (which has ID 1, not 0)
	// The exact match should have distance very close to 0
	assert.Equal(t, ids[0], results[0].ID)
	assert.InDelta(t, 0.0, results[0].Distance, 0.001, "Exact match should have near-zero distance")
}

func TestHBCIndex_CosineInternalSplitKeepsCentroidsUnitNormalized(t *testing.T) {
	const (
		numVectors = 32
		dims       = 32
	)

	r := rand.New(rand.NewPCG(42, 1024))
	vectors := make([]vector.T, numVectors)
	ids := make([]uint64, numVectors)
	for i := range vectors {
		ids[i] = uint64(i + 1)
		vectors[i] = make(vector.T, dims)
		for j := range vectors[i] {
			vectors[i][j] = r.Float32()*2 - 1
		}
		vec.NormalizeFloat32(vectors[i])
	}

	batch := &Batch{
		IDs:     ids,
		Vectors: vectors,
	}
	db := setupPebbleWithVectors(t, "test_hbc_cosine_split", batch)

	config := HBCConfig{
		Dimension:       dims,
		Name:            "test_hbc_cosine_split",
		BranchingFactor: 2,
		LeafSize:        2,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		DistanceMetric:  vector.DistanceMetric_Cosine,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	require.NotPanics(t, func() {
		require.NoError(t, index.Batch(t.Context(), batch))
	})

	stats := index.Stats()
	assert.Greater(t, stats["total_nodes"].(uint64), uint64(1))
}

func TestHBCIndex_SearchReturnsKAcrossLeaves(t *testing.T) {
	vectors := []vector.T{
		{1.0, 0.0},
		{0.9, 0.1},
		{0.0, 1.0},
		{0.1, 0.9},
		{5.0, 5.0},
		{5.2, 5.1},
	}
	ids := []uint64{1, 2, 3, 4, 5, 6}
	metadata := [][]byte{
		[]byte("doc:1"),
		[]byte("doc:2"),
		[]byte("doc:3"),
		[]byte("doc:4"),
		[]byte("doc:5"),
		[]byte("doc:6"),
	}

	db := setupPebbleWithVectors(t, "test_hbc", &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})

	config := HBCConfig{
		Dimension:       2,
		Name:            "test_hbc",
		BranchingFactor: 2,
		LeafSize:        2,
		SearchWidth:     4,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.Batch(t.Context(), &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})
	require.NoError(t, err)

	results, err := index.Search(&SearchRequest{
		Embedding: []float32{1.0, 0.0},
		K:         3,
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.EqualValues(t, 1, results[0].ID)
	assert.EqualValues(t, 2, results[1].ID)
	assert.EqualValues(t, 4, results[2].ID)
}

func TestHBCIndex_LeafCentroidsTrackInsertedMembers(t *testing.T) {
	vectors := []vector.T{
		{1.0, 0.0},
		{0.9, 0.1},
		{0.0, 1.0},
		{0.1, 0.9},
		{5.0, 5.0},
		{5.2, 5.1},
	}
	ids := []uint64{1, 2, 3, 4, 5, 6}
	metadata := make([][]byte, len(ids))
	for i, id := range ids {
		metadata[i] = fmt.Appendf(nil, "doc:%d", id)
	}

	batch := &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	}
	db := setupPebbleWithVectors(t, "test_hbc", batch)

	config := HBCConfig{
		Dimension:       2,
		Name:            "test_hbc",
		BranchingFactor: 2,
		LeafSize:        2,
		SearchWidth:     4,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.Batch(t.Context(), batch)
	require.NoError(t, err)

	meta, err := index.getMetadata(index.indexDB)
	require.NoError(t, err)

	queue := []uint64{meta.RootNode}
	seen := map[uint64]struct{}{}
	found := map[string][]float32{}

	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}

		node, err := index.loadNode(index.indexDB, nil, nodeID)
		require.NoError(t, err)
		if node.IsLeaf {
			members := slices.Clone(node.Members)
			slices.Sort(members)
			found[fmt.Sprint(members)] = slices.Clone(node.Centroid)
		} else {
			queue = append(queue, node.Children...)
		}
	}

	require.Contains(t, found, "[3 4]")
	require.Contains(t, found, "[5 6]")
	assert.InDeltaSlice(t, []float64{0.05, 0.95}, toFloat64s(found["[3 4]"]), 1e-4)
	assert.InDeltaSlice(t, []float64{5.1, 5.05}, toFloat64s(found["[5 6]"]), 1e-4)
}

func TestHBCIndex_DeleteRepairsUnderfullLeaf(t *testing.T) {
	vectors := []vector.T{
		{0.0, 0.0},
		{0.1, 0.0},
		{0.2, 0.0},
		{10.0, 10.0},
		{10.1, 10.0},
		{10.2, 10.0},
	}
	ids := []uint64{1, 2, 3, 4, 5, 6}
	metadata := make([][]byte, len(ids))
	for i, id := range ids {
		metadata[i] = fmt.Appendf(nil, "doc:%d", id)
	}
	batch := &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	}

	db := setupPebbleWithVectors(t, "test_hbc", batch)
	config := HBCConfig{
		Dimension:       2,
		Name:            "test_hbc",
		BranchingFactor: 2,
		LeafSize:        5,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}
	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	require.NoError(t, index.Batch(t.Context(), batch))
	require.NoError(t, index.Delete(1, 2))

	results, err := index.Search(&SearchRequest{
		Embedding: []float32{0.2, 0.0},
		K:         4,
	})
	require.NoError(t, err)
	require.Len(t, results, 4)
	assert.EqualValues(t, 3, results[0].ID)

	dump, err := index.DebugDump()
	require.NoError(t, err)
	assert.EqualValues(t, 4, dump.ActiveCount)
	assert.Equal(t, dump.RootNode, dump.Nodes[len(dump.Nodes)-1].ID)
	for _, node := range dump.Nodes {
		if node.IsLeaf {
			assert.NotEmpty(t, node.Members)
		}
	}
}

func toFloat64s(v []float32) []float64 {
	out := make([]float64, len(v))
	for i := range v {
		out[i] = float64(v[i])
	}
	return out
}

func TestHBCIndex_Delete(t *testing.T) {
	// Insert vectors
	vectors := []vector.T{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
	}

	ids := []uint64{1, 2, 3}
	metadata := [][]byte{[]byte("meta_1"), []byte("meta_2"), []byte("meta_3")}

	// Pre-populate pebble DB with vectors
	db := setupPebbleWithVectors(t, "test_hbc", &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})

	config := HBCConfig{
		Dimension:       3,
		Name:            "test_hbc",
		BranchingFactor: 4,
		LeafSize:        10,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.Batch(t.Context(), &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 3, index.TotalVectors())

	// Delete a vector
	err = index.Delete(2)
	require.NoError(t, err)
	assert.EqualValues(t, 2, index.TotalVectors())

	// Search should not return deleted vector
	query := []float32{0.0, 1.0, 0.0}
	results, err := index.Search(&SearchRequest{
		Embedding: query,
		K:         3,
	})
	require.NoError(t, err)

	for _, result := range results {
		assert.NotEqual(t, uint64(2), result.ID, "Deleted vector should not appear in results")
	}
}

func TestHBCIndex_FilteredSearch(t *testing.T) {
	// Insert vectors with different metadata prefixes
	vectors := []vector.T{
		{1.0, 0.0, 0.0},
		{0.9, 0.1, 0.0},
		{0.0, 1.0, 0.0},
		{0.1, 0.9, 0.0},
		{0.0, 0.0, 1.0},
	}

	metadata := [][]byte{
		[]byte("category:electronics"),
		[]byte("category:electronics"),
		[]byte("category:books"),
		[]byte("category:books"),
		[]byte("category:clothing"),
	}

	ids := make([]uint64, len(vectors))
	for i := range vectors {
		ids[i] = uint64(i + 1)
	}

	// Pre-populate pebble DB with vectors
	db := setupPebbleWithVectors(t, "test_hbc", &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})

	config := HBCConfig{
		Dimension:       3,
		Name:            "test_hbc",
		BranchingFactor: 4,
		LeafSize:        10,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.Batch(t.Context(), &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})
	require.NoError(t, err)

	// Search with filter
	query := []float32{1.0, 0.0, 0.0}
	filterPrefix := []byte("category:electronics")

	results, err := index.Search(&SearchRequest{
		Embedding:    query,
		K:            5,
		FilterPrefix: filterPrefix,
	})
	require.NoError(t, err)

	// Should only return electronics items
	assert.LessOrEqual(t, len(results), 2)
	for _, result := range results {
		meta, err := index.GetMetadata(result.ID)
		require.NoError(t, err)
		assert.Contains(t, string(meta), "electronics")
	}
}

func TestHBCIndex_Persistence(t *testing.T) {
	vectors := []vector.T{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
	}

	ids := []uint64{1, 2, 3}
	metadata := [][]byte{
		[]byte("meta_1"),
		[]byte("meta_2"),
		[]byte("meta_3"),
	}

	// Pre-populate pebble DB with vectors
	db := setupPebbleWithVectors(t, "test_hbc", &Batch{
		IDs:          ids,
		Vectors:      vectors,
		MetadataList: metadata,
	})

	config := HBCConfig{
		Dimension:       3,
		Name:            "test_hbc",
		BranchingFactor: 4,
		LeafSize:        10,
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	randSource := rand.NewPCG(42, 1024)

	// Create and populate index
	{
		index, err := NewHBCIndex(config, randSource)
		require.NoError(t, err)

		err = index.Batch(t.Context(), &Batch{
			IDs:          ids,
			Vectors:      vectors,
			MetadataList: metadata,
		})
		require.NoError(t, err)

		err = index.Close()
		require.NoError(t, err)
	}

	// Reopen and verify
	{
		index, err := NewHBCIndex(config, randSource)
		require.NoError(t, err)
		defer index.Close()

		assert.EqualValues(t, 3, index.TotalVectors())

		// Verify search works
		query := []float32{1.0, 0.0, 0.0}
		results, err := index.Search(&SearchRequest{
			Embedding: query,
			K:         1,
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.EqualValues(t, 1, results[0].ID)
		assert.Equal(t, []byte("meta_1"), results[0].Metadata)
	}
}

func TestHBCIndex_PrettyPrint(t *testing.T) {
	// Add some vectors
	vectors := []vector.T{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
		{0.5, 0.5, 0.0},
		{0.0, 0.5, 0.5},
		{0.5, 0.0, 0.5},
		{0.3, 0.3, 0.3},
	}

	ids := make([]uint64, len(vectors))
	for i := range vectors {
		ids[i] = uint64(i + 1)
	}

	// Pre-populate pebble DB with vectors
	batch := &Batch{
		IDs:     ids,
		Vectors: vectors,
	}
	db := setupPebbleWithVectors(t, "test_hbc_pretty", batch)

	config := HBCConfig{
		Dimension:       3,
		Name:            "test_hbc_pretty",
		BranchingFactor: 2,
		LeafSize:        3, // Small to trigger splits
		SearchWidth:     8,
		CacheSizeNodes:  100,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	// Print empty index
	t.Log("Empty index:")
	t.Log(index.PrettyPrint(nil, nil))

	err = index.Batch(t.Context(), batch)
	require.NoError(t, err)

	// Print populated index
	t.Log("\nPopulated index with tree structure:")
	prettyStr := index.PrettyPrint(nil, nil)
	t.Log(prettyStr)

	// Verify the pretty print contains expected elements
	assert.Contains(t, prettyStr, "HBC Index: test_hbc_pretty")
	assert.Contains(t, prettyStr, "Dimension: 3")
	assert.Contains(t, prettyStr, "Active Vectors: 7")
	assert.Contains(t, prettyStr, "Tree Structure:")
	assert.Contains(t, prettyStr, "Node")
}

func TestHBCIndex_ConcurrentOperations(t *testing.T) {
	// Pre-populate with some vectors
	numInitial := 100
	r := rand.New(rand.NewPCG(42, 1024))
	vectors := make([]vector.T, numInitial)
	ids := make([]uint64, numInitial)
	for i := range numInitial {
		ids[i] = uint64(i + 1)
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = r.Float32()
		}
		vectors[i] = vec
	}

	// Pre-populate pebble DB with vectors
	batch := &Batch{
		IDs:     ids,
		Vectors: vectors,
	}
	db := setupPebbleWithVectors(t, "test_hbc_concurrent", batch)

	config := HBCConfig{
		Dimension:       128,
		Name:            "test_hbc_concurrent",
		BranchingFactor: 4,
		LeafSize:        10,
		SearchWidth:     8,
		CacheSizeNodes:  1000,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.Batch(t.Context(), batch)
	require.NoError(t, err)

	// Run concurrent operations
	numGoroutines := 10
	opsPerGoroutine := 20
	errChan := make(chan error, numGoroutines+1)
	doneChan := make(chan bool, numGoroutines+1)

	// Concurrent inserts
	go func(goroutineID int) {
		defer func() { doneChan <- true }()

		r := rand.New(rand.NewPCG(uint64(goroutineID), 1024))
		for i := range opsPerGoroutine {
			vec := make([]float32, config.Dimension)
			for j := range vec {
				vec[j] = r.Float32()
			}
			id := uint64(numInitial + goroutineID*opsPerGoroutine + i + 1)
			metadata := fmt.Appendf(nil, "doc_%d", id)

			// Store vector in pebble DB
			suffix := fmt.Appendf(nil, ":i:%s:e", config.Name)
			vecKey := append(bytes.Clone(metadata), suffix...)
			vecData := make([]byte, 0, 8+4*(len(vec)+1))
			vecData, err := EncodeEmbeddingWithHashID(vecData, vec, 0)
			if err != nil {
				errChan <- fmt.Errorf("encode error in goroutine %d: %w", goroutineID, err)
				return
			}
			if err := db.Set(vecKey, vecData, nil); err != nil {
				errChan <- fmt.Errorf("pebble set error in goroutine %d: %w", goroutineID, err)
				return
			}

			if err := index.Batch(t.Context(), &Batch{IDs: []uint64{id}, Vectors: []vector.T{vec}, MetadataList: [][]byte{metadata}}); err != nil {
				errChan <- fmt.Errorf("insert error in goroutine %d: %w", goroutineID, err)
				return
			}
		}
	}(0)

	// Concurrent searches
	for g := range numGoroutines {
		go func(goroutineID int) {
			defer func() { doneChan <- true }()

			r := rand.New(rand.NewPCG(uint64(goroutineID+100), 1024))
			for range opsPerGoroutine * 2 {
				query := make([]float32, config.Dimension)
				for j := range query {
					query[j] = r.Float32()
				}
				results, err := index.Search(&SearchRequest{
					Embedding: query,
					K:         5,
				})
				if err != nil {
					errChan <- fmt.Errorf("search error in goroutine %d: %w", goroutineID, err)
					return
				}
				if len(results) == 0 && index.TotalVectors() > 0 {
					errChan <- fmt.Errorf("search returned no results in goroutine %d", goroutineID)
					return
				}
			}
		}(g)
	}

	// Wait for all operations to complete
	totalGoroutines := numGoroutines + 1
	for range totalGoroutines {
		select {
		case <-doneChan:
			// Operation completed
		case err := <-errChan:
			t.Fatal(err)
		case <-time.After(30 * time.Second):
			t.Fatal("Timeout waiting for concurrent operations")
		}
	}

	// Verify index is still consistent
	finalCount := index.TotalVectors()
	t.Logf("Final vector count: %d", finalCount)

	// Do a final search to ensure index is still functional
	query := make([]float32, config.Dimension)
	for i := range query {
		query[i] = r.Float32()
	}
	results, err := index.Search(&SearchRequest{
		Embedding: query,
		K:         10,
	})
	require.NoError(t, err)
	t.Logf("Final search returned %d results", len(results))
}

func TestHBCIndex_ConcurrentSearchOnly(t *testing.T) {
	// Pre-populate with vectors to create a deeper tree
	numVectors := 1000
	r := rand.New(rand.NewPCG(42, 1024))
	allVectors := make([]vector.T, numVectors)
	allIDs := make([]uint64, numVectors)

	for i := range numVectors {
		vec := make([]float32, 128)
		for k := range vec {
			vec[k] = r.Float32()
		}
		allVectors[i] = vec
		allIDs[i] = uint64(i + 1)
	}

	// Pre-populate pebble DB with vectors
	batch := &Batch{
		IDs:     allIDs,
		Vectors: allVectors,
	}
	db := setupPebbleWithVectors(t, "test_hbc_concurrent_search", batch)

	config := HBCConfig{
		Dimension:       128,
		Name:            "test_hbc_concurrent_search",
		BranchingFactor: 8,
		LeafSize:        50,
		SearchWidth:     16,
		CacheSizeNodes:  5000,
		CacheTTL:        10 * time.Minute,
		UseQuantization: true,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	batchSize := 100
	for i := 0; i < numVectors; i += batchSize {
		err := index.Batch(t.Context(), &Batch{
			IDs:          allIDs[i : i+batchSize],
			Vectors:      allVectors[i : i+batchSize],
			MetadataList: batch.MetadataList[i : i+batchSize],
		})
		require.NoError(t, err)
	}

	// Run many concurrent searches
	numSearchers := 20
	searchesPerGoroutine := 50
	errChan := make(chan error, numSearchers)
	var wg sync.WaitGroup

	start := time.Now()

	for g := range numSearchers {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			r := rand.New(rand.NewPCG(uint64(goroutineID+1000), 1024))
			for i := range searchesPerGoroutine {
				query := make([]float32, config.Dimension)
				for j := range query {
					query[j] = r.Float32()
				}

				results, err := index.Search(&SearchRequest{
					Embedding: query,
					K:         10,
				})
				if err != nil {
					errChan <- fmt.Errorf("search error in goroutine %d, iteration %d: %w", goroutineID, i, err)
					return
				}

				if len(results) == 0 {
					errChan <- fmt.Errorf("no results in goroutine %d, iteration %d", goroutineID, i)
					return
				}
			}
		}(g)
	}

	// Wait for all goroutines to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All searches completed successfully
	case err := <-errChan:
		t.Fatal(err)
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for concurrent searches")
	}

	elapsed := time.Since(start)
	totalSearches := numSearchers * searchesPerGoroutine
	t.Logf("Completed %d concurrent searches in %v (%.2f searches/sec)",
		totalSearches, elapsed, float64(totalSearches)/elapsed.Seconds())
}

func TestHBCIndex_ConcurrentInsertSearch(t *testing.T) {
	// Start with a small initial set
	r := rand.New(rand.NewPCG(42, 1024))
	vectors := make([]vector.T, 50)
	ids := make([]uint64, 50)
	for i := range 50 {
		ids[i] = uint64(i + 1)
		vec := make([]float32, 64)
		for j := range vec {
			vec[j] = r.Float32()
		}
		vectors[i] = vec
	}

	// Pre-populate pebble DB with vectors
	batch := &Batch{
		IDs:     ids,
		Vectors: vectors,
	}
	db := setupPebbleWithVectors(t, "test_hbc_concurrent_insert_search", batch)

	config := HBCConfig{
		Dimension:       64,
		Name:            "test_hbc_concurrent_insert_search",
		BranchingFactor: 4,
		LeafSize:        20,
		SearchWidth:     8,
		CacheSizeNodes:  2000,
		CacheTTL:        5 * time.Minute,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	err = index.Batch(t.Context(), batch)
	require.NoError(t, err)

	// Run concurrent inserts and searches
	numInserters := 1
	numSearchers := 10
	duration := 5 * time.Second

	stopChan := make(chan struct{})
	errChan := make(chan error, numInserters+numSearchers)
	doneChan := make(chan bool, numInserters+numSearchers)

	// Track operations
	var insertCount, searchCount atomic.Int64

	// Start inserters
	for g := range numInserters {
		go func(goroutineID int) {
			defer func() { doneChan <- true }()

			r := rand.New(rand.NewPCG(uint64(goroutineID+2000), 1024))
			id := uint64(1000 + goroutineID*1000)

			for {
				select {
				case <-stopChan:
					return
				default:
					vec := make([]float32, config.Dimension)
					for j := range vec {
						vec[j] = r.Float32()
					}

					metadata := fmt.Appendf(nil, "doc_%d", id)

					// Store vector in pebble DB
					suffix := fmt.Appendf(nil, ":i:%s:e", config.Name)
					vecKey := append(bytes.Clone(metadata), suffix...)
					vecData := make([]byte, 0, 8+4*(len(vec)+1))
					vecData, err := EncodeEmbeddingWithHashID(vecData, vec, 0)
					if err != nil {
						errChan <- fmt.Errorf("encode error in goroutine %d: %w", goroutineID, err)
						return
					}
					if err := db.Set(vecKey, vecData, nil); err != nil {
						errChan <- fmt.Errorf("pebble set error in goroutine %d: %w", goroutineID, err)
						return
					}

					if err := index.Batch(t.Context(), &Batch{IDs: []uint64{id}, Vectors: []vector.T{vec}, MetadataList: [][]byte{metadata}}); err != nil {
						errChan <- fmt.Errorf("insert error in goroutine %d: %w", goroutineID, err)
						return
					}

					id++
					insertCount.Add(1)
				}
			}
		}(g)
	}

	// Start searchers
	for g := range numSearchers {
		go func(goroutineID int) {
			defer func() { doneChan <- true }()

			r := rand.New(rand.NewPCG(uint64(goroutineID+3000), 1024))

			for {
				select {
				case <-stopChan:
					return
				default:
					query := make([]float32, config.Dimension)
					for j := range query {
						query[j] = r.Float32()
					}

					results, err := index.Search(&SearchRequest{
						Embedding: query,
						K:         5,
					})
					if err != nil {
						errChan <- fmt.Errorf("search error in goroutine %d: %w", goroutineID, err)
						return
					}

					if len(results) == 0 && index.TotalVectors() > 0 {
						errChan <- fmt.Errorf("no results in searcher %d", goroutineID)
						return
					}

					searchCount.Add(1)
				}
			}
		}(g)
	}

	// Let operations run for the specified duration
	time.Sleep(duration)
	close(stopChan)

	// Wait for all goroutines to finish
	for range numInserters + numSearchers {
		select {
		case <-doneChan:
			// Goroutine finished
		case err := <-errChan:
			t.Fatal(err)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for goroutines to finish")
		}
	}

	t.Logf(
		"Performed %d inserts and %d searches concurrently",
		insertCount.Load(),
		searchCount.Load(),
	)
	t.Logf("Final vector count: %d", index.TotalVectors())
}

// Benchmark to compare with PebbleANN
func BenchmarkHBCIndex_BatchInsert(b *testing.B) {
	// Prepare batch data
	batchSize := 100
	vectors := make([]vector.T, batchSize)
	ids := make([]uint64, batchSize)
	r := rand.New(rand.NewPCG(42, 1024))

	for i := range vectors {
		ids[i] = uint64(i + 1)
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = r.Float32()
		}
		vectors[i] = vec
	}

	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, db.Close())
	})

	config := HBCConfig{
		Dimension:       128,
		Name:            "bench_hbc",
		BranchingFactor: 16,
		LeafSize:        100,
		SearchWidth:     32,
		CacheSizeNodes:  10000,
		CacheTTL:        10 * time.Minute,
		DistanceMetric:  vector.DistanceMetric_Cosine,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(b, err)
	defer index.Close()

	b.ResetTimer()
	for b.Loop() {
		err := index.Batch(b.Context(), &Batch{IDs: ids, Vectors: vectors})
		require.NoError(b, err)
	}
}

func TestHBCIndex_ConcurrentStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Create empty pebble DB
	db := setupPebbleWithVectors(t, "test_hbc_stress", &Batch{})

	config := HBCConfig{
		Dimension:       256,
		Name:            "test_hbc_stress",
		BranchingFactor: 16,
		LeafSize:        100,
		SearchWidth:     32,
		CacheSizeNodes:  10000,
		CacheTTL:        10 * time.Minute,
		UseQuantization: true,
		DistanceMetric:  vector.DistanceMetric_L2Squared,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(t, err)
	defer index.Close()

	// Simulate real-world usage patterns
	duration := 10 * time.Second
	stopChan := make(chan struct{})
	errChan := make(chan error, 100)

	// Metrics
	var (
		totalInserts  atomic.Int64
		totalSearches atomic.Int64
		totalDeletes  atomic.Int64
		totalUpdates  atomic.Int64
	)

	// Start multiple writer goroutines (fewer writers)
	go func(writerID int) {
		r := rand.New(rand.NewPCG(uint64(writerID+10000), 1024))
		id := uint64(writerID * 100000)

		for {
			select {
			case <-stopChan:
				return
			default:
				// Randomly choose operation
				op := r.IntN(100)

				switch {
				case op < 70: // 70% inserts
					vec := make([]float32, config.Dimension)
					for i := range vec {
						vec[i] = r.Float32()
					}
					meta := fmt.Appendf(nil, "doc_%d", id)

					// Store vector in pebble DB
					suffix := fmt.Appendf(nil, ":i:%s:e", config.Name)
					vecKey := append(bytes.Clone(meta), suffix...)
					vecData := make([]byte, 0, 8+4*(len(vec)+1))
					vecData, err := EncodeEmbeddingWithHashID(vecData, vec, 0)
					if err != nil {
						select {
						case errChan <- fmt.Errorf("writer %d encode error: %w", writerID, err):
						default:
						}
						return
					}
					if err := db.Set(vecKey, vecData, nil); err != nil {
						select {
						case errChan <- fmt.Errorf("writer %d pebble set error: %w", writerID, err):
						default:
						}
						return
					}

					if err := index.Batch(t.Context(), &Batch{IDs: []uint64{id}, Vectors: []vector.T{vec}, MetadataList: [][]byte{meta}}); err != nil {
						select {
						case errChan <- fmt.Errorf("writer %d insert error: %w", writerID, err):
						default:
						}
						return
					}
					id++
					totalInserts.Add(1)

				case op < 85: // 15% updates
					if id > uint64(writerID*100000+10) {
						updateID := uint64(
							writerID*100000,
						) + uint64(
							r.IntN(int(id-uint64(writerID*100000))),
						)
						vec := make([]float32, config.Dimension)
						for i := range vec {
							vec[i] = r.Float32()
						}
						meta := fmt.Appendf(nil, "doc_%d_updated", updateID)

						// Store vector in pebble DB
						suffix := fmt.Appendf(nil, ":i:%s:e", config.Name)
						vecKey := append(bytes.Clone(meta), suffix...)
						vecData := make([]byte, 0, 8+4*(len(vec)+1))
						vecData, err := EncodeEmbeddingWithHashID(vecData, vec, 0)
						if err != nil {
							select {
							case errChan <- fmt.Errorf("writer %d encode error: %w", writerID, err):
							default:
							}
							return
						}
						if err := db.Set(vecKey, vecData, nil); err != nil {
							select {
							case errChan <- fmt.Errorf("writer %d pebble set error: %w", writerID, err):
							default:
							}
							return
						}

						// Update = delete + insert
						_ = index.Delete(updateID)
						if err := index.Batch(t.Context(), &Batch{IDs: []uint64{updateID}, Vectors: []vector.T{vec}, MetadataList: [][]byte{meta}}); err != nil {
							select {
							case errChan <- fmt.Errorf("writer %d update error: %w", writerID, err):
							default:
							}
							return
						}
						totalUpdates.Add(1)
					}

				case op < 100: // 15% deletes
					if id > uint64(writerID*100000+20) {
						deleteID := uint64(
							writerID*100000,
						) + uint64(
							r.IntN(int(id-uint64(writerID*100000))),
						)
						if err := index.Delete(deleteID); err != nil &&
							!errors.Is(err, ErrNotFound) {
							select {
							case errChan <- fmt.Errorf("writer %d delete error: %w", writerID, err):
							default:
							}
							return
						}
						totalDeletes.Add(1)
					}
				}
			}
		}
	}(1)

	// Start many reader goroutines
	numReaders := 20
	for r := range numReaders {
		go func(readerID int) {
			r := rand.New(rand.NewPCG(uint64(readerID+20000), 1024))
			for {
				select {
				case <-stopChan:
					return
				default:
					query := make([]float32, config.Dimension)
					for i := range query {
						query[i] = r.Float32()
					}

					// Randomly use filter or not
					var filter []byte
					if r.IntN(2) == 0 {
						filter = []byte("doc_")
					}

					results, err := index.Search(&SearchRequest{
						Embedding:    query,
						K:            10,
						FilterPrefix: filter,
					})
					if err != nil {
						select {
						case errChan <- fmt.Errorf("reader %d search error: %w", readerID, err):
						default:
						}
						return
					}

					// Verify results are valid
					for _, result := range results {
						if filter != nil {
							meta, err := index.GetMetadata(result.ID)
							if err != nil && !errors.Is(err, ErrNotFound) {
								select {
								case errChan <- fmt.Errorf("reader %d metadata error: %w", readerID, err):
								default:
								}
								return
							}
							if meta != nil && !bytes.HasPrefix(meta, filter) {
								select {
								case errChan <- fmt.Errorf("reader %d filter mismatch: %s", readerID, meta):
								default:
								}
								return
							}
						}
					}

					totalSearches.Add(1)
				}
			}
		}(r)
	}

	// Let the stress test run
	time.Sleep(duration)
	close(stopChan)

	// Check for errors
	select {
	case err := <-errChan:
		t.Fatal(err)
	default:
		// No errors
	}

	// Wait a bit for goroutines to finish
	time.Sleep(1 * time.Second)

	// Report metrics
	t.Logf("Stress test completed:")
	t.Logf("  Total inserts:  %d", totalInserts.Load())
	t.Logf("  Total searches: %d", totalSearches.Load())
	t.Logf("  Total updates:  %d", totalUpdates.Load())
	t.Logf("  Total deletes:  %d", totalDeletes.Load())
	t.Logf("  Final vectors:  %d", index.TotalVectors())

	stats := index.Stats()
	t.Logf("  Cache hits:     %d", stats["pebble_cache_hits"])
	t.Logf("  Cache misses:   %d", stats["pebble_cache_misses"])
	t.Logf("  Total nodes:    %d", stats["total_nodes"])
}

func BenchmarkHBCIndex_Search(b *testing.B) {
	// Populate index
	numVectors := 5000
	r := rand.New(rand.NewPCG(42, 1024))
	allVectors := make([]vector.T, numVectors)
	allIDs := make([]uint64, numVectors)

	for i := range numVectors {
		allIDs[i] = uint64(i + 1)
		vec := make([]float32, 128)
		for k := range vec {
			vec[k] = r.Float32()
		}
		allVectors[i] = vec
	}

	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, db.Close())
	})

	config := HBCConfig{
		Dimension:       128,
		Name:            "bench_hbc",
		BranchingFactor: 16,
		LeafSize:        100,
		SearchWidth:     32,
		CacheSizeNodes:  10000,
		CacheTTL:        10 * time.Minute,
		DistanceMetric:  vector.DistanceMetric_L2Squared,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(b, err)
	defer index.Close()

	for i := 0; i < numVectors; i += 100 {
		err = index.Batch(b.Context(), &Batch{
			IDs:     allIDs[i : i+100],
			Vectors: allVectors[i : i+100],
		})
		require.NoError(b, err)
	}

	// Prepare query
	query := make([]float32, config.Dimension)
	for i := range query {
		query[i] = r.Float32()
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := index.Search(&SearchRequest{
			Embedding: query,
			K:         10,
		})
		require.NoError(b, err)
	}
}

func BenchmarkHBCIndex_ConcurrentSearch(b *testing.B) {
	// Populate index
	numVectors := 10000
	r := rand.New(rand.NewPCG(42, 1024))
	allVectors := make([]vector.T, numVectors)
	allIDs := make([]uint64, numVectors)

	for i := range numVectors {
		allIDs[i] = uint64(i + 1)
		vec := make([]float32, 128)
		for k := range vec {
			vec[k] = r.Float32()
		}
		allVectors[i] = vec
	}

	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, db.Close())
	})

	config := HBCConfig{
		Dimension:       128,
		Name:            "bench_hbc_concurrent",
		BranchingFactor: 16,
		LeafSize:        100,
		SearchWidth:     32,
		CacheSizeNodes:  10000,
		CacheTTL:        10 * time.Minute,
		UseQuantization: true,
		DistanceMetric:  vector.DistanceMetric_L2Squared,
		VectorDB:        db,
		IndexDB:         db,
	}

	index, err := NewHBCIndex(config, rand.NewPCG(42, 1024))
	require.NoError(b, err)
	defer index.Close()

	for i := 0; i < numVectors; i += 100 {
		err = index.Batch(b.Context(), &Batch{
			IDs:     allIDs[i : i+100],
			Vectors: allVectors[i : i+100],
		})
		require.NoError(b, err)
	}

	// Prepare queries for each goroutine
	numGoroutines := 8
	queries := make([]vector.T, numGoroutines)
	for i := range numGoroutines {
		query := make([]float32, config.Dimension)
		for j := range query {
			query[j] = r.Float32()
		}
		queries[i] = query
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Get a unique query for this goroutine
		goroutineID := rand.IntN(numGoroutines)
		query := queries[goroutineID]

		for pb.Next() {
			_, err := index.Search(&SearchRequest{
				Embedding: query,
				K:         10,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
