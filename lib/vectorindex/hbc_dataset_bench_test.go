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
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

type hbcDatasetBenchmarkCase struct {
	name    string
	dataset string
}

func BenchmarkHBCBuildFromDataset(b *testing.B) {
	cases := []hbcDatasetBenchmarkCase{
		{name: "Wiki10k", dataset: testutils.WikiDataset},
		{name: "Images10k", dataset: testutils.ImagesDataset},
		{name: "Dbpedia1k", dataset: testutils.DbpediaDataset},
	}

	for _, tc := range cases {
		tc := tc
		dataset := testutils.LoadDataset(b, tc.dataset)
		dataCount := int(dataset.GetCount()) * 98 / 100
		queryCount := min(100, int(dataset.GetCount())-dataCount)
		if queryCount <= 0 {
			b.Fatalf("dataset %s does not have enough vectors for queries", tc.dataset)
		}

		dataVectors := dataset.Slice(0, dataCount)
		queryVectors := dataset.Slice(dataCount, queryCount)
		insertBatch := buildHBCTestBatch(dataVectors)
		dim := int(dataset.GetDims())

		b.Run(tc.name, func(b *testing.B) {
			b.ReportMetric(float64(dataCount), "vectors")
			b.ReportMetric(float64(dim), "dims")

			buildModes := []struct {
				name  string
				build func(context.Context, *HBCIndex, *Batch) error
			}{
				{
					name: "IncrementalBatch",
					build: func(ctx context.Context, idx *HBCIndex, batch *Batch) error {
						return idx.Batch(ctx, batch)
					},
				},
				{
					name: "RecursiveBulkBuild",
					build: func(ctx context.Context, idx *HBCIndex, batch *Batch) error {
						return idx.BulkBuild(ctx, batch)
					},
				},
			}

			for _, mode := range buildModes {
				mode := mode
				b.Run(mode.name, func(b *testing.B) {
					b.ReportAllocs()

					var recallSum float64
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						db := openHBCBenchmarkPebble(b)
						idx := newDatasetBenchmarkHBCIndex(b, db, dim)
						storeBenchmarkVectors(b, db, idx.Name(), insertBatch)

						b.StartTimer()
						err := mode.build(context.Background(), idx, insertBatch)
						b.StopTimer()
						require.NoError(b, err)

						recallSum += benchmarkHBCAverageRecall(
							b,
							idx,
							queryVectors,
							dataVectors,
							10,
						)

						require.NoError(b, idx.Close())
						require.NoError(b, db.Close())
					}

					if b.N > 0 {
						b.ReportMetric((recallSum/float64(b.N))*100, "recall_pct")
					}
				})
			}
		})
	}
}

func buildHBCTestBatch(dataVectors *vector.Set) *Batch {
	count := int(dataVectors.GetCount())
	ids := make([]uint64, count)
	metadata := make([][]byte, count)
	vecs := make([]vector.T, count)
	for i := range count {
		ids[i] = uint64(i + 1)
		metadata[i] = fmt.Appendf(nil, "data_%d", i+1)
		vecs[i] = slices.Clone(dataVectors.At(i))
	}
	return &Batch{
		IDs:          ids,
		Vectors:      vecs,
		MetadataList: metadata,
	}
}

func openHBCBenchmarkPebble(tb testing.TB) *pebble.DB {
	tb.Helper()

	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(tb, err)
	return db
}

func storeBenchmarkVectors(tb testing.TB, db *pebble.DB, indexName string, batch *Batch) {
	tb.Helper()

	suffix := fmt.Appendf(nil, ":i:%s:e", indexName)
	pbatch := db.NewBatch()
	defer func() { _ = pbatch.Close() }()

	for i, id := range batch.IDs {
		docID := batch.MetadataList[i]
		vecKey := append(bytes.Clone(docID), suffix...)
		vecData := make([]byte, 0, 8+4*(len(batch.Vectors[i])+1))
		var err error
		vecData, err = EncodeEmbeddingWithHashID(vecData, batch.Vectors[i], 0)
		require.NoError(tb, err)
		require.NoError(tb, pbatch.Set(vecKey, vecData, nil))
		require.NotZero(tb, id)
		require.NotEmpty(tb, docID)
	}

	require.NoError(tb, pbatch.Commit(pebble.Sync))
}

func newDatasetBenchmarkHBCIndex(tb testing.TB, db *pebble.DB, dim int) *HBCIndex {
	tb.Helper()

	cfg := HBCConfig{
		Dimension:           uint32(dim),
		Name:                "bench_hbc_dataset",
		SplitAlgo:           vector.ClustAlgorithm_Kmeans,
		QuantizerSeed:       42,
		UseQuantization:     true,
		UseRandomOrthoTrans: false,
		Episilon2:           1.6,
		BranchingFactor:     7 * 24,
		LeafSize:            7 * 24,
		SearchWidth:         3 * 7 * 24,
		CacheSizeNodes:      10_000,
		CacheTTL:            10 * time.Minute,
		DistanceMetric:      vector.DistanceMetric_L2Squared,
		VectorDB:            db,
		IndexDB:             db,
	}

	idx, err := NewHBCIndex(cfg, rand.NewPCG(42, 1024))
	require.NoError(tb, err)
	return idx
}

func benchmarkHBCAverageRecall(
	tb testing.TB,
	idx *HBCIndex,
	queryVectors *vector.Set,
	dataVectors *vector.Set,
	topK int,
) float64 {
	tb.Helper()

	dataKeys := make([]int, dataVectors.GetCount())
	for i := range dataVectors.GetCount() {
		dataKeys[i] = int(i)
	}

	var recallSum float64
	for i := range int(queryVectors.GetCount()) {
		query := queryVectors.At(i)
		results, err := idx.Search(&SearchRequest{
			Embedding: query,
			K:         topK,
		})
		require.NoError(tb, err)
		require.GreaterOrEqual(tb, len(results), topK)

		prediction := make([]int, topK)
		for j := range topK {
			prediction[j] = int(results[j].ID - 1)
		}

		truth := testutils.CalculateTruth(
			topK,
			vector.DistanceMetric_L2Squared,
			query,
			dataVectors,
			dataKeys,
		)
		recallSum += testutils.CalculateRecall(prediction, truth)
	}

	return recallSum / float64(queryVectors.GetCount())
}
