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
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkDimension       = 512
	benchmarkDatasetSize     = 75_000 // For populating index before search benchmarks
	benchmarkQueryCount      = 1000   // Number of unique queries for search benchmark
	benchmarkK               = 100
	benchmarkEfSearch        = benchmarkEfConstruction
	benchmarkEfConstruction  = 100
	benchmarkM0              = 96
	benchmarkM               = 48
	benchmarkCacheSizeNodes  = 10_000
	benchmarkPebbleSyncWrite = true
	benchmarkBatchSize       = 1000 // For batch insert operations
	benchmarkNumBatches      = 1    // For batch insert operations
)

// generateRandomVectorsV2 creates a slice of random vectors for benchmarking.
func generateRandomVectorsV2(count, dim int, r *rand.Rand) []vector.T {
	vectors := make([]vector.T, count)
	for i := range count {
		vectors[i] = make([]float32, dim)
		for j := range dim {
			vectors[i][j] = r.Float32()
		}
	}
	return vectors
}

type indexFactoryFunc func(tb testing.TB, dir string, rSource rand.Source, db *pebble.DB) (VectorIndex, func())

// func setupPebbleANNBenchmark(tb testing.TB, dir string, rSource rand.Source) (VectorIndex, func()) {
// 	cfg := HNSWConfig{
// 		Dimension:             benchmarkDimension,
// 		Neighbors:             benchmarkM0,
// 		NeighborsHigherLayers: benchmarkM,
// 		EfConstruction:        benchmarkEfConstruction,
// 		EfSearch:              benchmarkEfSearch,
// 		CacheSizeNodes:        benchmarkCacheSizeNodes,
// 		PebbleSyncWrite:       benchmarkPebbleSyncWrite,
// 		DistanceMetric:        vector.DistanceMetric_L2Squared,
// 		LevelMultiplier:       1 / math.Log(benchmarkM),
// 	}
// 	idx, err := NewHNSWIndex(cfg, rSource)
// 	require.NoError(tb, err, "Failed to create PebbleANN index")
// 	require.NotNil(tb, idx, "PebbleANN index is nil")
//
// 	cleanup := func() {
// 		err := idx.Close()
// 		if err != nil {
// 			tb.Logf("Error closing PebbleANN index: %v", err)
// 		}
// 		os.RemoveAll(dir)
// 	}
// 	return idx, cleanup
// }

func setupHBCBenchmark_kmeans(
	tb testing.TB,
	dir string,
	rSource rand.Source,
	db *pebble.DB,
) (VectorIndex, func()) {
	cfg := HBCConfig{
		Dimension:           benchmarkDimension,
		Name:                "bench_hbc",
		SplitAlgo:           vector.ClustAlgorithm_Kmeans,
		QuantizerSeed:       42,
		UseQuantization:     true,
		UseRandomOrthoTrans: false,

		// recall@10
		// Episilon1: 10,
		// Episilon2: 7,
		// recall@1
		// Episilon1:       1,
		// Episilon2:       0.6,
		//
		Episilon2:       7,
		BranchingFactor: 7 * 24,
		LeafSize:        7 * 24,
		SearchWidth:     2 * 3 * 7 * 24,
		//
		// Episilon2:       1.6,
		// BranchingFactor: 7 * 24,
		// LeafSize:        7 * 24,
		// SearchWidth:     3 * 7 * 24,
		//
		CacheSizeNodes: benchmarkCacheSizeNodes,
		DistanceMetric: vector.DistanceMetric_L2Squared,
		VectorDB:       db,
		IndexDB:        db,
	}
	idx, err := NewHBCIndex(cfg, rSource)
	require.NoError(tb, err, "Failed to create HBC index")
	require.NotNil(tb, idx, "HBC index is nil")

	cleanup := func() {
		err := idx.Close()
		if err != nil {
			tb.Logf("Error closing HBC index: %v", err)
		}
		os.RemoveAll(dir)
	}
	return idx, cleanup
}

var indexFactories = map[string]indexFactoryFunc{
	// "PebbleANN_HNSW": setupPebbleANNBenchmark,
	"HBC_KMeans": setupHBCBenchmark_kmeans,
	// "HBC_Hilbert": setupHBCBenchmark_hilbert,
}

func BenchmarkComparison_BatchInsert(b *testing.B) {
	// debug.SetGCPercent(50) // More frequent GC
	// runtime.GOMAXPROCS(1)  // Single thread for initial debugging
	rGlobal := rand.New(rand.NewPCG(uint64(os.Getpid()), uint64(time.Now().UnixNano())))
	// Generate multiple batches of vectors
	allBatches := make([][]vector.T, benchmarkNumBatches)
	allMetadata := make([][][]byte, benchmarkNumBatches)
	allIDs := make([][]uint64, benchmarkNumBatches)

	for batchIdx := range benchmarkNumBatches {
		allBatches[batchIdx] = generateRandomVectorsV2(
			benchmarkBatchSize,
			benchmarkDimension,
			rGlobal,
		)
		allMetadata[batchIdx] = make([][]byte, benchmarkBatchSize)
		allIDs[batchIdx] = make([]uint64, benchmarkBatchSize)

		for i := range allMetadata[batchIdx] {
			// Ensure unique IDs across all batches
			allMetadata[batchIdx][i] = fmt.Appendf(nil, "meta_batch_%d_%d", batchIdx, i)
			allIDs[batchIdx][i] = xxhash.Sum64(allMetadata[batchIdx][i])
		}
	}

	for name, factory := range indexFactories {
		b.Run(name, func(b *testing.B) {
			tempDir := b.TempDir()
			rSource := rand.NewPCG(uint64(os.Getpid()), uint64(time.Now().UnixNano()))

			// Pre-populate pebble DB with vectors
			var allVectorsFlat []vector.T
			var allIDsFlat []uint64
			var allMetadataFlat [][]byte
			for batchIdx := range benchmarkNumBatches {
				allVectorsFlat = append(allVectorsFlat, allBatches[batchIdx]...)
				allIDsFlat = append(allIDsFlat, allIDs[batchIdx]...)
				allMetadataFlat = append(allMetadataFlat, allMetadata[batchIdx]...)
			}

			db := setupPebbleWithVectors(b, "bench_hbc", &Batch{
				IDs:          allIDsFlat,
				Vectors:      allVectorsFlat,
				MetadataList: allMetadataFlat,
			})

			index, cleanup := factory(b, tempDir, rSource, db)
			defer cleanup()

			b.ResetTimer()
			for b.Loop() {
				// Insert all batches
				for batchIdx := range benchmarkNumBatches {
					err := index.Batch(
						b.Context(),
						&Batch{
							IDs:          allIDs[batchIdx],
							Vectors:      allBatches[batchIdx],
							MetadataList: allMetadata[batchIdx],
						},
					)
					require.NoError(b, err, "Batch failed for batch %d", batchIdx)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkComparison_Delete(b *testing.B) {
	for name, factory := range indexFactories {
		b.Run(name, func(b *testing.B) {
			tempDir := b.TempDir()
			// Use a fixed seed for index population for consistent deletion benchmark runs for a given type
			rSourceIndexPopulate := rand.NewPCG(42, 1024)
			rIndexPopulate := rand.New(rSourceIndexPopulate)

			// Populate the index in batches
			populationVectors := generateRandomVectorsV2(
				benchmarkDatasetSize,
				benchmarkDimension,
				rIndexPopulate,
			)
			metadataForPopulation := make([][]byte, benchmarkDatasetSize)
			ids := make([]uint64, benchmarkDatasetSize)
			for i := range metadataForPopulation {
				ids[i] = uint64(i + 1) // IDs start from 1 to avoid zero ID issues
				metadataForPopulation[i] = fmt.Appendf(nil, "meta_delete_setup_%s_%d", name, i)
			}

			// Pre-populate pebble DB with vectors
			db := setupPebbleWithVectors(b, "bench_hbc", &Batch{
				IDs:          ids,
				Vectors:      populationVectors,
				MetadataList: metadataForPopulation,
			})

			index, cleanup := factory(b, tempDir, rSourceIndexPopulate, db)
			defer cleanup()

			// Insert in batches
			for i := 0; i < benchmarkDatasetSize; i += benchmarkBatchSize {
				end := min(i+benchmarkBatchSize, benchmarkDatasetSize)

				batchIDs := ids[i:end]
				batchVectors := populationVectors[i:end]
				batchMetadata := metadataForPopulation[i:end]

				err := index.Batch(
					b.Context(),
					&Batch{
						IDs:          batchIDs,
						Vectors:      batchVectors,
						MetadataList: batchMetadata,
					},
				)
				require.NoError(
					b,
					err,
					"Failed to populate index batch %d-%d for delete benchmark",
					i,
					end,
				)
			}

			if benchmarkDatasetSize == 0 { // Or if len(ids) == 0
				b.Skip("Skipping delete benchmark as no items were populated.")
				return
			}
			// b.Logf("[%s] Index populated with %d vectors for deletion.", name, len(ids))

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				idToDelete := ids[i%len(ids)] // Cycle through the initially populated IDs
				if err := index.Delete(idToDelete); err != nil {
					// Some indexes might return error if item already deleted, or not found.
					// For this benchmark, we'll treat errors as fatal.
					b.Fatalf("Delete failed for ID %d: %v", idToDelete, err)
				}
			}
			b.StopTimer()
		})
	}
}

// computeBruteForceTopK computes the true top-k nearest neighbors using brute force
func computeBruteForceTopK(
	queryVec vector.T,
	populationVectors []vector.T,
	ids []uint64,
	k int,
	distanceMetric vector.DistanceMetric,
) []uint64 {
	type distanceResult struct {
		id   uint64
		dist float32
	}

	results := make([]distanceResult, len(populationVectors))
	for i, vec := range populationVectors {
		results[i] = distanceResult{
			id:   ids[i],
			dist: vector.MeasureDistance(distanceMetric, queryVec, vec),
		}
	}

	// Sort by distance (ascending for distance metrics)
	for i := 0; i < k && i < len(results); i++ {
		minIdx := i
		for j := i + 1; j < len(results); j++ {
			if results[j].dist < results[minIdx].dist {
				minIdx = j
			}
		}
		results[i], results[minIdx] = results[minIdx], results[i]
	}

	// Extract top-k IDs
	topK := make([]uint64, 0, k)
	for i := 0; i < k && i < len(results); i++ {
		topK = append(topK, results[i].id)
	}
	return topK
}

// calcRecall computes the recall percentage between found results and true results
func calcRecall(foundIDs, trueIDs []uint64) float64 {
	trueSet := make(map[uint64]bool)
	for _, id := range trueIDs {
		trueSet[id] = true
	}

	matches := 0
	for _, id := range foundIDs {
		if trueSet[id] {
			matches++
		}
	}

	if len(trueIDs) == 0 {
		return 0.0
	}
	return float64(matches) / float64(len(trueIDs)) * 100.0
}

func BenchmarkComparison_Search(b *testing.B) {
	// rGlobal := rand.New(rand.NewPCG(uint64(os.Getpid()), uint64(time.Now().UnixNano())))
	rGlobal := rand.New(rand.NewPCG(69, 68))
	queryVectors := generateRandomVectorsV2(benchmarkQueryCount, benchmarkDimension, rGlobal)

	for name, factory := range indexFactories {
		b.Run(name, func(b *testing.B) {
			tempDir := b.TempDir()
			// Use a fixed seed for index population for consistent search benchmark runs for a given type
			rSourceIndexPopulate := rand.NewPCG(42, 1024)
			rIndexPopulate := rand.New(rSourceIndexPopulate)

			// Populate the index
			populationVectors := generateRandomVectorsV2(
				benchmarkDatasetSize,
				benchmarkDimension,
				rIndexPopulate,
			)
			metadataForPopulation := make([][]byte, benchmarkDatasetSize)
			ids := make([]uint64, benchmarkDatasetSize)
			for i := range metadataForPopulation {
				ids[i] = uint64(i + 1) // IDs start from 1 to avoid zero ID issues
				metadataForPopulation[i] = fmt.Appendf(nil, "meta_%s_%d", name, i)
			}

			// Pre-populate pebble DB with vectors
			db := setupPebbleWithVectors(b, "bench_hbc", &Batch{
				IDs:          ids,
				Vectors:      populationVectors,
				MetadataList: metadataForPopulation,
			})

			index, cleanup := factory(b, tempDir, rSourceIndexPopulate, db)
			defer cleanup()

			// Insert in batches
			for i := 0; i < benchmarkDatasetSize; i += benchmarkBatchSize {
				end := min(i+benchmarkBatchSize, benchmarkDatasetSize)

				batchIDs := ids[i:end]
				batchVectors := populationVectors[i:end]
				batchMetadata := metadataForPopulation[i:end]

				err := index.Batch(
					b.Context(),
					&Batch{
						IDs:          batchIDs,
						Vectors:      batchVectors,
						MetadataList: batchMetadata,
					},
				)
				require.NoError(
					b,
					err,
					"Failed to populate index batch %d-%d for search benchmark",
					i,
					end,
				)
			}
			// b.Logf("[%s] Index populated with %d vectors.", name, benchmarkDatasetSize)

			// Calculate recall for a sample of queries
			recallSampleSize := min(
				// Sample size for recall calculation
				10, benchmarkQueryCount)

			totalRecallAt100 := 0.0
			totalRecallAt10 := 0.0
			totalRecallAt1 := 0.0

			for i := range recallSampleSize {
				queryVec := queryVectors[i]

				// Get results from the index (get benchmarkK results for recall@100)
				results, err := index.Search(&SearchRequest{Embedding: queryVec, K: benchmarkK})
				if err != nil {
					b.Fatalf("Search failed during recall calculation: %v", err)
				}

				// Extract IDs from results
				foundIDs := make([]uint64, len(results))
				dists := make([]float32, len(results))
				for j, res := range results {
					foundIDs[j] = res.ID
					dists[j] = res.Distance
				}

				// Compute true top-k using brute force for different k values
				trueTop100 := computeBruteForceTopK(
					queryVec,
					populationVectors,
					ids,
					benchmarkK,
					vector.DistanceMetric_L2Squared,
				)
				trueTop10 := trueTop100[:min(10, len(trueTop100))]
				trueTop1 := trueTop100[:min(1, len(trueTop100))]

				// Calculate recall@100
				recall100 := calcRecall(foundIDs, trueTop100)
				totalRecallAt100 += recall100

				// Calculate recall@10
				foundTop10 := foundIDs[:min(10, len(foundIDs))]
				recall10 := calcRecall(foundTop10, trueTop10)
				totalRecallAt10 += recall10

				// Calculate recall@1
				foundTop1 := foundIDs[:min(1, len(foundIDs))]
				recall1 := calcRecall(foundTop1, trueTop1)
				totalRecallAt1 += recall1

				if recall100 <= 0 {
					fmt.Println(
						"Warning: Recall@100 is zero, check your index population and query vectors.",
						dists,
					)
				}
			}

			avgRecallAt100 := totalRecallAt100 / float64(recallSampleSize)
			avgRecallAt10 := totalRecallAt10 / float64(recallSampleSize)
			avgRecallAt1 := totalRecallAt1 / float64(recallSampleSize)

			b.Logf(
				"[%s] Average recall@1: %.2f%%, recall@10: %.2f%%, recall@100: %.2f%% (based on %d queries)",
				name,
				avgRecallAt1,
				avgRecallAt10,
				avgRecallAt100,
				recallSampleSize,
			)

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				queryVec := queryVectors[i%benchmarkQueryCount] // Cycle through queries
				_, err := index.Search(&SearchRequest{Embedding: queryVec, K: benchmarkK})
				if err != nil {
					b.Fatalf("Search failed: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}
