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
	"slices"
	"testing"
	"time"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recallTestCase struct {
	dataset   string
	randomize bool
	topK      int
	count     int
	expected  map[string]float64 // expected recall percentages per metric
	tolerance float64            // tolerance for floating point comparison
}

func TestCalculateRecall(t *testing.T) {
	testCases := []recallTestCase{
		{
			dataset: testutils.ImagesDataset,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    97.00,
				"InnerProduct": 91.50,
				"Cosine":       95.50,
			},
		},
		{
			dataset:   testutils.ImagesDataset,
			randomize: true,
			topK:      10,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    99.50,
				"InnerProduct": 99.50,
				"Cosine":       99.50,
			},
		},
		{
			dataset: testutils.RandomDataset20d,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    100.00,
				"InnerProduct": 99.50,
				"Cosine":       96.50,
			},
		},
		{
			dataset:   testutils.RandomDataset20d,
			randomize: true,
			topK:      10,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    99.00,
				"InnerProduct": 97.50,
				"Cosine":       97.50,
			},
		},
		{
			dataset: testutils.FashionMinstDataset1k,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    98.00,
				"InnerProduct": 98.50,
				"Cosine":       95.00,
			},
		},
		{
			dataset:   testutils.FashionMinstDataset1k,
			randomize: true,
			topK:      10,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    100.00,
				"InnerProduct": 100.00,
				"Cosine":       100.00,
			},
		},
		{
			dataset: testutils.FashionMinstDataset10k,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    93.00,
				"InnerProduct": 80.00,
				"Cosine":       95.50,
			},
		},
		{
			dataset:   testutils.FashionMinstDataset10k,
			randomize: true,
			topK:      10,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    99.50,
				"InnerProduct": 100.00,
				"Cosine":       100.00,
			},
		},
		// Laion image embeddings with 768 dimensions
		{
			dataset: testutils.LaionDatasetCLIP,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    95.50,
				"InnerProduct": 93.00,
				"Cosine":       93.00,
			},
		},
		{
			dataset:   testutils.LaionDatasetCLIP,
			randomize: true,
			topK:      10,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    99.00,
				"InnerProduct": 97.50,
				"Cosine":       98.50,
			},
		},
		{
			dataset: testutils.LaionDatasetGemini1k,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    95.00,
				"InnerProduct": 85.50,
				"Cosine":       86.50,
			},
		},
		{
			dataset:   testutils.LaionDatasetGemini1k,
			topK:      10,
			randomize: true,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    99.00,
				"InnerProduct": 97.00,
				"Cosine":       98.00,
			},
		},
		{
			dataset:   testutils.LaionDatasetGemini10k,
			topK:      10,
			count:     10_000,
			tolerance: 1.5, // Higher tolerance due to platform precision differences
			expected: map[string]float64{
				"Euclidean":    86.50,
				"InnerProduct": 69.00,
				"Cosine":       80.00,
			},
		},
		{
			dataset:   testutils.LaionDatasetGemini10k,
			topK:      10,
			randomize: true,
			count:     10_000,
			tolerance: 1.5, // Higher tolerance due to platform precision differences
			expected: map[string]float64{
				"Euclidean":    88.50,
				"InnerProduct": 83.00,
				"Cosine":       87.50,
			},
		},
		{
			dataset: testutils.WikiDataset,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    99.00,
				"InnerProduct": 98.50,
				"Cosine":       98.50,
			},
		},
		{
			dataset:   testutils.WikiDataset,
			randomize: true,
			topK:      10,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    100.00,
				"InnerProduct": 97.00,
				"Cosine":       97.00,
			},
		},
		{
			dataset: testutils.DbpediaDataset,
			topK:    10,
			count:   1000,
			expected: map[string]float64{
				"Euclidean":    100.00,
				"InnerProduct": 99.00,
				"Cosine":       100.00,
			},
		},
		{
			dataset:   testutils.DbpediaDataset,
			randomize: true,
			topK:      10,
			count:     1000,
			expected: map[string]float64{
				"Euclidean":    100.00,
				"InnerProduct": 99.50,
				"Cosine":       99.50,
			},
		},
	}

	for _, tc := range testCases {
		testName := fmt.Sprintf("%s-randomize=%t-topK=%d-count=%d",
			tc.dataset, tc.randomize, tc.topK, tc.count)
		t.Run(testName, func(t *testing.T) {
			results := calculateRecall(t, tc.dataset, tc.randomize, tc.topK, tc.count)

			// Verify results match expected values
			tolerance := 0.5001 // Allow small tolerance for floating point comparison
			if tc.tolerance > 0 {
				tolerance = tc.tolerance
			}
			metrics := []string{"Euclidean", "InnerProduct", "Cosine"}
			for _, metric := range metrics {
				assert.InDelta(t, tc.expected[metric], results[metric], tolerance,
					"%s: expected %.2f%% recall, got %.2f%%",
					metric, tc.expected[metric], results[metric])
			}
		})
	}
}

func calculateRecall(
	t *testing.T,
	datasetName string,
	randomize bool,
	topK int,
	count int,
) map[string]float64 {
	// Use the first 98% of the vectors as data vectors and the other 2% as query
	// vectors.
	dataset := testutils.LoadDataset(t, datasetName)
	dataVectors := dataset.Slice(0, count*98/100)
	queryVectors := dataset.Slice(int(dataVectors.GetCount()), count-int(dataVectors.GetCount()))
	dataKeys := make([]int, dataVectors.GetCount())
	for i := range dataVectors.GetCount() {
		dataKeys[i] = int(i)
	}

	calculateAvgRecall := func(metric vector.DistanceMetric) float64 {
		// Prepare vectors, IDs, and metadata
		vecs := make([]vector.T, dataVectors.GetCount())
		ids := make([]uint64, dataVectors.GetCount())
		metadata := make([][]byte, dataVectors.GetCount())
		for i := range dataVectors.GetCount() {
			ids[i] = uint64(i + 1)
			vecs[i] = slices.Clone(dataVectors.At(int(i)))
			metadata[i] = fmt.Appendf(nil, "data_%d", i+1)
		}
		if metric == vector.DistanceMetric_Cosine {
			for i := range vecs {
				vec.NormalizeFloat32(vecs[i])
			}
		}

		// Pre-populate pebble DB with vectors
		db := setupPebbleWithVectors(t, "calculate_recall", &Batch{
			IDs:          ids,
			Vectors:      vecs,
			MetadataList: metadata,
		})

		config := HBCConfig{
			Dimension: uint32(dataset.GetDims()),
			Name:      "calculate_recall",

			CacheSizeNodes: 10_000,
			CacheTTL:       10 * time.Minute,

			Episilon2:       1.6,
			BranchingFactor: 7 * 24,
			LeafSize:        7 * 24,
			// SearchWidth:     2 * 3 * 7 * 24, /* = 1008  */
			SearchWidth: 3 * 7 * 24, /* = 504  */

			UseQuantization:     true,
			UseRandomOrthoTrans: randomize,
			QuantizerSeed:       42,
			DistanceMetric:      metric,
			VectorDB:            db,
			IndexDB:             db,
		}

		randSource := rand.NewPCG(42, 1024)
		index, err := NewHBCIndex(config, randSource)
		require.NoError(t, err)
		defer func() {
			_ = index.Close()
		}()

		require.NoError(
			t,
			index.Batch(t.Context(), &Batch{IDs: ids, Vectors: vecs, MetadataList: metadata}),
		)

		// Build a vector set from vecs for truth calculation (may be normalized for Cosine)
		truthSet := vector.MakeSet(len(vecs[0]))
		for _, v := range vecs {
			truthSet.Add(v)
		}

		var recallSum float64
		for i := range queryVectors.GetCount() {
			query := slices.Clone(queryVectors.At(int(i)))
			if metric == vector.DistanceMetric_Cosine {
				vec.NormalizeFloat32(query)
			}
			results, err := index.Search(&SearchRequest{
				Embedding: query,
				K:         topK,
			})
			require.NoError(t, err)

			prediction := make([]int, len(results))
			for i := range prediction {
				prediction[i] = int(results[i].ID - 1)
			}
			prediction = prediction[:topK]
			truth := testutils.CalculateTruth(topK, metric, query, truthSet, dataKeys)
			recallSum += testutils.CalculateRecall(prediction, truth)
		}
		return recallSum / float64(queryVectors.GetCount())
	}

	results := make(map[string]float64)

	// Calculate Euclidean recall
	results["Euclidean"] = calculateAvgRecall(vector.DistanceMetric_L2Squared) * 100

	// Calculate InnerProduct recall
	results["InnerProduct"] = calculateAvgRecall(vector.DistanceMetric_InnerProduct) * 100

	// Calculate Cosine recall (normalization handled inside calculateAvgRecall)
	results["Cosine"] = calculateAvgRecall(vector.DistanceMetric_Cosine) * 100

	return results
}
