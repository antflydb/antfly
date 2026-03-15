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

// Package indexbench provides side-by-side comparison benchmarks for dense
// (HBC) and sparse (SPLADE inverted) embedding indexes.
package indexbench

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/sparseindex"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

const (
	benchK         = 10
	benchBatchSize = 100
	hbcIndexName   = "bench_dense"
)

// storeVectorsInPebble writes vectors to Pebble with the key format expected by
// HBC: <docID><suffix> where suffix is ":i:<indexName>:e". The HBC index reads
// vectors from Pebble during Batch operations.
func storeVectorsInPebble(tb testing.TB, db *pebble.DB, batch *vectorindex.Batch) {
	tb.Helper()
	suffix := fmt.Appendf(nil, ":i:%s:e", hbcIndexName)

	pbatch := db.NewBatch()
	defer pbatch.Close()

	for i, id := range batch.IDs {
		var docID []byte
		if batch.MetadataList[i] != nil {
			docID = batch.MetadataList[i]
		} else {
			docID = fmt.Appendf(nil, "doc_%d", id)
			batch.MetadataList[i] = docID
		}

		vecKey := append(bytes.Clone(docID), suffix...)
		vecData := make([]byte, 0, 8+4*(len(batch.Vectors[i])+1))
		var err error
		vecData, err = vectorindex.EncodeEmbeddingWithHashID(vecData, batch.Vectors[i], 0)
		require.NoError(tb, err)
		require.NoError(tb, pbatch.Set(vecKey, vecData, nil))
	}
	require.NoError(tb, pbatch.Commit(pebble.Sync))
}

// setupDenseHBC creates an in-memory HBC index and its backing Pebble DB.
func setupDenseHBC(tb testing.TB, dim int, db *pebble.DB) *vectorindex.HBCIndex {
	tb.Helper()

	cfg := vectorindex.HBCConfig{
		Dimension:       uint32(dim),
		Name:            hbcIndexName,
		SplitAlgo:       vector.ClustAlgorithm_Kmeans,
		QuantizerSeed:   42,
		UseQuantization: true,
		Episilon2:       7,
		BranchingFactor: 7 * 24,
		LeafSize:        7 * 24,
		SearchWidth:     2 * 3 * 7 * 24,
		CacheSizeNodes:  10_000,
		DistanceMetric:  vector.DistanceMetric_L2Squared,
		VectorDB:        db,
		IndexDB:         db,
	}

	rSource := rand.NewPCG(42, 0)
	idx, err := vectorindex.NewHBCIndex(cfg, rSource)
	require.NoError(tb, err)
	tb.Cleanup(func() { idx.Close() })

	return idx
}

// setupSparseIndex creates an in-memory sparse index for benchmarking.
func setupSparseIndex(tb testing.TB) *sparseindex.SparseIndex {
	tb.Helper()
	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(tb, err)
	tb.Cleanup(func() { db.Close() })

	return sparseindex.New(db, sparseindex.Config{ChunkSize: sparseindex.DefaultChunkSize})
}

// setupSparseMergeIndex creates an in-memory sparse index with the Pebble
// merge operator enabled, for benchmarking the merge-based insert path.
func setupSparseMergeIndex(tb testing.TB) *sparseindex.SparseIndex {
	tb.Helper()
	idx, _ := setupSparseMergeIndexWithDB(tb)
	return idx
}

// setupSparseMergeIndexWithDB is like setupSparseMergeIndex but also returns
// the underlying pebble.DB (e.g. for flushing before search benchmarks).
func setupSparseMergeIndexWithDB(tb testing.TB) (*sparseindex.SparseIndex, *pebble.DB) {
	tb.Helper()
	reg := pebbleutils.NewRegistry()
	sparseindex.RegisterChunkMerger(reg, nil)
	opts := pebbleutils.NewMemPebbleOpts()
	opts.Merger = reg.NewMerger("bench.merge.v1")
	db, err := pebble.Open("", opts)
	require.NoError(tb, err)
	tb.Cleanup(func() { db.Close() })

	idx := sparseindex.New(db, sparseindex.Config{
		ChunkSize: sparseindex.DefaultChunkSize,
		UseMerge:  true,
	})
	return idx, db
}

// generateRandomDenseVectors creates count random vectors of the given dimension.
func generateRandomDenseVectors(count, dim int, r *rand.Rand) []vector.T {
	vecs := make([]vector.T, count)
	for i := range count {
		vecs[i] = make([]float32, dim)
		for j := range dim {
			vecs[i][j] = r.Float32()
		}
	}
	return vecs
}

// generateRandomSparseVectors creates count random sparse vectors.
func generateRandomSparseVectors(count int, r *rand.Rand) []*vector.SparseVector {
	vecs := make([]*vector.SparseVector, count)
	for i := range count {
		nnz := 50 + r.IntN(100) // 50-150 non-zero terms
		indices := make([]uint32, nnz)
		values := make([]float32, nnz)
		used := make(map[uint32]bool, nnz)
		for j := range nnz {
			for {
				idx := r.Uint32N(30000)
				if !used[idx] {
					used[idx] = true
					indices[j] = idx
					break
				}
			}
			values[j] = r.Float32() * 5.0
		}
		vecs[i] = vector.NewSparseVector(indices, values)
	}
	return vecs
}

// prepareDenseBatch creates a Batch with docID metadata for HBC.
func prepareDenseBatch(vecs []vector.T, startID int) *vectorindex.Batch {
	count := len(vecs)
	ids := make([]uint64, count)
	metadata := make([][]byte, count)
	for i := range count {
		id := uint64(startID + i + 1)
		ids[i] = id
		metadata[i] = fmt.Appendf(nil, "doc_%d", id)
	}
	return &vectorindex.Batch{
		IDs:          ids,
		Vectors:      vecs,
		MetadataList: metadata,
	}
}

// BenchmarkComparison_Insert benchmarks insert performance for both index types.
func BenchmarkComparison_Insert(b *testing.B) {
	const numDocs = 1000
	const dim = 768

	b.Run("Dense_HBC", func(b *testing.B) {
		r := rand.New(rand.NewPCG(42, 0))
		denseVecs := generateRandomDenseVectors(numDocs, dim, r)

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
			require.NoError(b, err)

			idx := setupDenseHBC(b, dim, db)

			// Pre-store vectors in Pebble (required by HBC)
			for i := 0; i < numDocs; i += benchBatchSize {
				end := min(i+benchBatchSize, numDocs)
				batch := prepareDenseBatch(denseVecs[i:end], i)
				storeVectorsInPebble(b, db, batch)
			}
			b.StartTimer()

			for i := 0; i < numDocs; i += benchBatchSize {
				end := min(i+benchBatchSize, numDocs)
				batch := prepareDenseBatch(denseVecs[i:end], i)
				if err := idx.Batch(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			idx.Close()
			db.Close()
		}
	})

	b.Run("Sparse", func(b *testing.B) {
		r := rand.New(rand.NewPCG(42, 0))
		sparseVecs := generateRandomSparseVectors(numDocs, r)

		inserts := make([]sparseindex.BatchInsert, numDocs)
		for i := range numDocs {
			inserts[i] = sparseindex.BatchInsert{
				DocID: fmt.Appendf(nil, "doc%d", i),
				Vec:   sparseVecs[i],
			}
		}

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			idx := setupSparseIndex(b)
			b.StartTimer()

			for i := 0; i < numDocs; i += benchBatchSize {
				end := min(i+benchBatchSize, numDocs)
				if err := idx.Batch(inserts[i:end], nil); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("Sparse_Merge", func(b *testing.B) {
		r := rand.New(rand.NewPCG(42, 0))
		sparseVecs := generateRandomSparseVectors(numDocs, r)

		inserts := make([]sparseindex.BatchInsert, numDocs)
		for i := range numDocs {
			inserts[i] = sparseindex.BatchInsert{
				DocID: fmt.Appendf(nil, "doc%d", i),
				Vec:   sparseVecs[i],
			}
		}

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			idx := setupSparseMergeIndex(b)
			b.StartTimer()

			for i := 0; i < numDocs; i += benchBatchSize {
				end := min(i+benchBatchSize, numDocs)
				if err := idx.Batch(inserts[i:end], nil); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// BenchmarkComparison_Search benchmarks search performance for both index types
// after pre-populating each with the same number of documents.
func BenchmarkComparison_Search(b *testing.B) {
	const numDocs = 1000
	const dim = 768
	const numQueries = 100

	b.Run("Dense_HBC", func(b *testing.B) {
		r := rand.New(rand.NewPCG(42, 0))

		db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
		require.NoError(b, err)
		b.Cleanup(func() { db.Close() })

		idx := setupDenseHBC(b, dim, db)

		// Pre-populate
		denseVecs := generateRandomDenseVectors(numDocs, dim, r)
		for i := 0; i < numDocs; i += benchBatchSize {
			end := min(i+benchBatchSize, numDocs)
			batch := prepareDenseBatch(denseVecs[i:end], i)
			storeVectorsInPebble(b, db, batch)
			require.NoError(b, idx.Batch(context.Background(), batch))
		}

		// Generate query vectors
		queries := generateRandomDenseVectors(numQueries, dim, r)
		queryIdx := 0

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := idx.Search(&vectorindex.SearchRequest{
				Embedding: queries[queryIdx%numQueries],
				K:         benchK,
			})
			if err != nil {
				b.Fatal(err)
			}
			queryIdx++
		}
	})

	benchSparseSearch := func(b *testing.B, useMerge bool) {
		r := rand.New(rand.NewPCG(42, 0))
		var idx *sparseindex.SparseIndex
		var mergeDB *pebble.DB
		if useMerge {
			idx, mergeDB = setupSparseMergeIndexWithDB(b)
		} else {
			idx = setupSparseIndex(b)
		}

		// Pre-populate
		sparseVecs := generateRandomSparseVectors(numDocs, r)
		inserts := make([]sparseindex.BatchInsert, numDocs)
		for i := range numDocs {
			inserts[i] = sparseindex.BatchInsert{
				DocID: fmt.Appendf(nil, "doc%d", i),
				Vec:   sparseVecs[i],
			}
		}
		for i := 0; i < numDocs; i += benchBatchSize {
			end := min(i+benchBatchSize, numDocs)
			require.NoError(b, idx.Batch(inserts[i:end], nil))
		}

		// Force compaction so merge operands are resolved before searching.
		if mergeDB != nil {
			require.NoError(b, mergeDB.Flush())
			require.NoError(b, mergeDB.Compact(context.Background(), nil, []byte{0xff}, true))
		}

		// Generate query vectors (sparse queries typically have fewer non-zero terms)
		sparseQueries := make([]*vector.SparseVector, numQueries)
		for i := range numQueries {
			nnz := 10 + r.IntN(30) // 10-40 query terms
			indices := make([]uint32, nnz)
			values := make([]float32, nnz)
			used := make(map[uint32]bool, nnz)
			for j := range nnz {
				for {
					vi := r.Uint32N(30000)
					if !used[vi] {
						used[vi] = true
						indices[j] = vi
						break
					}
				}
				values[j] = r.Float32() * 3.0
			}
			sparseQueries[i] = vector.NewSparseVector(indices, values)
		}
		queryIdx := 0

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := idx.Search(sparseQueries[queryIdx%numQueries], benchK, nil)
			if err != nil {
				b.Fatal(err)
			}
			queryIdx++
		}
	}

	b.Run("Sparse", func(b *testing.B) {
		benchSparseSearch(b, false)
	})

	b.Run("Sparse_Merge", func(b *testing.B) {
		benchSparseSearch(b, true)
	})
}

// benchmarkSparseInsertFromDataset runs insert benchmark for a given sparse dataset.
// When useMerge is true, the merge-based insert path is used.
func benchmarkSparseInsertFromDataset(b *testing.B, datasetName string, useMerge bool) {
	sparseVecs := testutils.LoadSparseDataset(b, datasetName)
	count := len(sparseVecs)

	inserts := make([]sparseindex.BatchInsert, count)
	for i := range count {
		inserts[i] = sparseindex.BatchInsert{
			DocID: fmt.Appendf(nil, "doc%d", i),
			Vec:   sparseVecs[i],
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		var idx *sparseindex.SparseIndex
		if useMerge {
			idx = setupSparseMergeIndex(b)
		} else {
			idx = setupSparseIndex(b)
		}
		b.StartTimer()

		for i := 0; i < count; i += benchBatchSize {
			end := min(i+benchBatchSize, count)
			if err := idx.Batch(inserts[i:end], nil); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// benchmarkSparseSearchFromDataset runs search benchmark for a given sparse dataset.
// When useMerge is true, the index is populated via the merge-based insert path.
func benchmarkSparseSearchFromDataset(b *testing.B, datasetName string, useMerge bool) {
	const numQueries = 100

	sparseVecs := testutils.LoadSparseDataset(b, datasetName)
	count := len(sparseVecs)
	var idx *sparseindex.SparseIndex
	var mergeDB *pebble.DB
	if useMerge {
		idx, mergeDB = setupSparseMergeIndexWithDB(b)
	} else {
		idx = setupSparseIndex(b)
	}

	// Pre-populate
	inserts := make([]sparseindex.BatchInsert, count)
	for i := range count {
		inserts[i] = sparseindex.BatchInsert{
			DocID: fmt.Appendf(nil, "doc%d", i),
			Vec:   sparseVecs[i],
		}
	}
	for i := 0; i < count; i += benchBatchSize {
		end := min(i+benchBatchSize, count)
		require.NoError(b, idx.Batch(inserts[i:end], nil))
	}

	// Force compaction so merge operands are resolved before searching.
	// This simulates steady-state production behavior where Pebble's
	// background compaction has caught up.
	if mergeDB != nil {
		require.NoError(b, mergeDB.Flush())
		require.NoError(b, mergeDB.Compact(context.Background(), nil, []byte{0xff}, true))
	}

	// Use dataset vectors as queries (subsample to query-length)
	r := rand.New(rand.NewPCG(99, 0))
	queryCount := min(numQueries, count)
	sparseQueries := make([]*vector.SparseVector, queryCount)
	for i := range queryCount {
		src := sparseVecs[r.IntN(count)]
		srcIndices := src.GetIndices()
		srcValues := src.GetValues()
		queryNnz := min(20, len(srcIndices))
		sparseQueries[i] = vector.NewSparseVector(srcIndices[:queryNnz], srcValues[:queryNnz])
	}
	queryIdx := 0

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, err := idx.Search(sparseQueries[queryIdx%queryCount], benchK, nil)
		if err != nil {
			b.Fatal(err)
		}
		queryIdx++
	}
}

// BenchmarkComparison_InsertFromDataset benchmarks insert using pre-generated datasets.
func BenchmarkComparison_InsertFromDataset(b *testing.B) {
	const dim = 768

	b.Run("Dense_HBC_Wiki", func(b *testing.B) {
		denseSet := testutils.LoadDataset(b, testutils.WikiDataset)
		count := int(denseSet.GetCount())

		denseVecs := make([]vector.T, count)
		for i := range count {
			denseVecs[i] = denseSet.At(i)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
			require.NoError(b, err)

			idx := setupDenseHBC(b, dim, db)

			// Pre-store vectors in Pebble
			for i := 0; i < count; i += benchBatchSize {
				end := min(i+benchBatchSize, count)
				batch := prepareDenseBatch(denseVecs[i:end], i)
				storeVectorsInPebble(b, db, batch)
			}
			b.StartTimer()

			for i := 0; i < count; i += benchBatchSize {
				end := min(i+benchBatchSize, count)
				batch := prepareDenseBatch(denseVecs[i:end], i)
				if err := idx.Batch(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			idx.Close()
			db.Close()
		}
	})

	b.Run("Sparse_SPLADE_Wiki", func(b *testing.B) {
		benchmarkSparseInsertFromDataset(b, testutils.SparseWikiDataset, false)
	})

	b.Run("Sparse_Merge_SPLADE_Wiki", func(b *testing.B) {
		benchmarkSparseInsertFromDataset(b, testutils.SparseWikiDataset, true)
	})
}

// BenchmarkComparison_SearchFromDataset benchmarks search using pre-generated datasets.
func BenchmarkComparison_SearchFromDataset(b *testing.B) {
	const dim = 768
	const numQueries = 100

	b.Run("Dense_HBC_Wiki", func(b *testing.B) {
		denseSet := testutils.LoadDataset(b, testutils.WikiDataset)
		count := int(denseSet.GetCount())

		db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
		require.NoError(b, err)
		b.Cleanup(func() { db.Close() })

		idx := setupDenseHBC(b, dim, db)

		denseVecs := make([]vector.T, count)
		for i := range count {
			denseVecs[i] = denseSet.At(i)
		}

		// Pre-populate
		for i := 0; i < count; i += benchBatchSize {
			end := min(i+benchBatchSize, count)
			batch := prepareDenseBatch(denseVecs[i:end], i)
			storeVectorsInPebble(b, db, batch)
			require.NoError(b, idx.Batch(context.Background(), batch))
		}

		queryCount := min(numQueries, count)
		queryIdx := 0

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := idx.Search(&vectorindex.SearchRequest{
				Embedding: denseVecs[queryIdx%queryCount],
				K:         benchK,
			})
			if err != nil {
				b.Fatal(err)
			}
			queryIdx++
		}
	})

	b.Run("Sparse_SPLADE_Wiki", func(b *testing.B) {
		benchmarkSparseSearchFromDataset(b, testutils.SparseWikiDataset, false)
	})

	b.Run("Sparse_Merge_SPLADE_Wiki", func(b *testing.B) {
		benchmarkSparseSearchFromDataset(b, testutils.SparseWikiDataset, true)
	})
}
