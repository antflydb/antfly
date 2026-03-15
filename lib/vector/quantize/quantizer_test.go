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

package quantize_test

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/antflydb/antfly/lib/vector/quantize"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/stretchr/testify/assert"
)

type recallTestCase struct {
	dataset   string
	randomize bool
	topK      int
	count     int
	expected  map[string]float64 // expected recall percentages per metric
	tolerance float64            // tolerance for recall percentage comparison
}

func TestCalculateRecall(t *testing.T) {
	for _, tc := range recallTestCases {
		testName := fmt.Sprintf("%s-randomize=%t-topK=%d-count=%d",
			tc.dataset, tc.randomize, tc.topK, tc.count)
		t.Run(testName, func(t *testing.T) {
			results := calculateRecallForQuantizer(t, tc.dataset, tc.randomize, tc.topK, tc.count)

			// Verify results match expected values
			for metric, expectedRecall := range tc.expected {
				actualRecall := results[metric]
				// Allow small tolerance for floating point comparison
				tolerance := 0.500001
				if tc.tolerance > 0 {
					tolerance = tc.tolerance
				}
				assert.InDelta(t, expectedRecall, actualRecall, tolerance,
					"%s: expected %.2f%% recall, got %.2f%%",
					metric, expectedRecall, actualRecall)
			}
		})
	}
}

func calculateRecallForQuantizer(
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

	if randomize {
		var transform vector.RandomOrthogonalTransformer
		transform.Init(vector.RotAlgorithm_Givens, int(dataset.GetDims()), 42)
		for i := range queryVectors.GetCount() {
			transform.Transform(queryVectors.At(int(i)), queryVectors.At(int(i)))
		}
		for i := range dataVectors.GetCount() {
			transform.Transform(dataVectors.At(int(i)), dataVectors.At(int(i)))
		}
	}

	calculateAvgRecall := func(metric vector.DistanceMetric) float64 {
		dataCentroid := make(vector.T, dataset.GetDims())
		dataCentroid = dataVectors.Centroid(dataCentroid)

		var recallSum float64
		var allocator allocator.StackAllocator
		rabitQ := quantize.NewRaBitQuantizer(int(dataset.GetDims()), 42, metric)
		for i := range queryVectors.GetCount() {
			query := queryVectors.At(int(i))
			rabitQSet := rabitQ.Quantize(&allocator, dataCentroid, dataVectors)
			estimated := make([]float32, rabitQSet.GetCount())
			errorBounds := make([]float32, rabitQSet.GetCount())
			rabitQ.EstimateDistances(
				&allocator, rabitQSet, query, estimated, errorBounds)

			prediction := make([]int, len(estimated))
			for i := range prediction {
				prediction[i] = i
			}
			slices.SortFunc(prediction, func(i, j int) int {
				return cmp.Compare(estimated[i], estimated[j])
			})
			prediction = prediction[:topK]
			truth := testutils.CalculateTruth(topK, metric, query, dataVectors, dataKeys)
			recallSum += testutils.CalculateRecall(prediction, truth)
		}
		return recallSum / float64(queryVectors.GetCount())
	}

	results := make(map[string]float64)

	// Calculate Euclidean recall
	results["Euclidean"] = calculateAvgRecall(vector.DistanceMetric_L2Squared) * 100

	// Calculate InnerProduct recall
	results["InnerProduct"] = calculateAvgRecall(vector.DistanceMetric_InnerProduct) * 100

	// For cosine distance, normalize the query and input vectors.
	for i := range queryVectors.GetCount() {
		vec.NormalizeFloat32(queryVectors.At(int(i)))
	}
	for i := range dataVectors.GetCount() {
		vec.NormalizeFloat32(dataVectors.At(int(i)))
	}

	// Calculate Cosine recall
	results["Cosine"] = calculateAvgRecall(vector.DistanceMetric_Cosine) * 100

	return results
}

func TestEstimateDistances(t *testing.T) {
	type memberCase struct {
		vec        vector.T
		exact      float32
		estimate   float32
		errorBound float32
	}

	type metricExpectations struct {
		centroid vector.T
		members  []memberCase
	}

	testCases := []struct {
		name     string
		query    vector.T
		vectors  []vector.T
		expected map[string]metricExpectations
	}{
		{
			name:    "orthogonal to data vectors",
			query:   vector.T{0, 2},
			vectors: []vector.T{{-2, 0}, {2, 0}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 8, estimate: 0, errorBound: 5.7},
						{vec: vector.T{2, 0}, exact: 8, estimate: 0, errorBound: 5.7},
					},
				},
				"InnerProduct": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 0, estimate: -4, errorBound: 2.8},
						{vec: vector.T{2, 0}, exact: 0, estimate: -4, errorBound: 2.8},
					},
				},
				"Cosine": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-1, 0}, exact: 1, estimate: 0, errorBound: 0.7071},
						{vec: vector.T{1, 0}, exact: 1, estimate: 0, errorBound: 0.7071},
					},
				},
			},
		},
		{
			name:    "translated centroid non-origin",
			query:   vector.T{2, 4},
			vectors: []vector.T{{0, 2}, {4, 2}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{0, 2}, exact: 8, estimate: 0, errorBound: 5.7},
						{vec: vector.T{4, 2}, exact: 8, estimate: 0, errorBound: 5.7},
					},
				},
				"InnerProduct": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{0, 2}, exact: -8, estimate: -12, errorBound: 2.8},
						{vec: vector.T{4, 2}, exact: -16, estimate: -20, errorBound: 2.8},
					},
				},
				"Cosine": {
					centroid: vector.T{0.4472, 0.7236},
					members: []memberCase{
						{vec: vector.T{0, 1}, exact: 0.106, estimate: 0.0875, errorBound: 0.0635},
						{
							vec:        vector.T{0.8944, 0.4472},
							exact:      0.2,
							estimate:   0.218,
							errorBound: 0.0635,
						},
					},
				},
			},
		},
		{
			name:    "query equals data vector",
			query:   vector.T{2, 0},
			vectors: []vector.T{{-2, 0}, {2, 0}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 16, estimate: 16, errorBound: 5.7},
						{vec: vector.T{2, 0}, exact: 0, estimate: 0, errorBound: 5.7},
					},
				},
				"InnerProduct": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 4, estimate: 4, errorBound: 2.8},
						{vec: vector.T{2, 0}, exact: -4, estimate: -4, errorBound: 2.8},
					},
				},
				"Cosine": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-1, 0}, exact: 2, estimate: 2, errorBound: 0.7071},
						{vec: vector.T{1, 0}, exact: 0, estimate: 0, errorBound: 0.7071},
					},
				},
			},
		},
		{
			name:    "query equals data vector translated",
			query:   vector.T{4, 2},
			vectors: []vector.T{{0, 2}, {4, 2}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{0, 2}, exact: 16, estimate: 16, errorBound: 5.7},
						{vec: vector.T{4, 2}, exact: 0, estimate: 0, errorBound: 5.7},
					},
				},
				"InnerProduct": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{0, 2}, exact: -4, estimate: -4, errorBound: 2.8},
						{vec: vector.T{4, 2}, exact: -20, estimate: -20, errorBound: 2.8},
					},
				},
				"Cosine": {
					centroid: vector.T{0.4472, 0.7236},
					members: []memberCase{
						{vec: vector.T{0, 1}, exact: 0.553, estimate: 0.5528, errorBound: 0.1954},
						{vec: vector.T{0.8944, 0.4472}, exact: 0, estimate: 0, errorBound: 0.1954},
					},
				},
			},
		},
		{
			name:    "query parallel but longer",
			query:   vector.T{4, 0},
			vectors: []vector.T{{-2, 0}, {2, 0}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 36, estimate: 36, errorBound: 11.3},
						{vec: vector.T{2, 0}, exact: 4, estimate: 4, errorBound: 11.3},
					},
				},
				"InnerProduct": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 8, estimate: 8, errorBound: 5.7},
						{vec: vector.T{2, 0}, exact: -8, estimate: -8, errorBound: 5.7},
					},
				},
				"Cosine": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-1, 0}, exact: 2, estimate: 2, errorBound: 0.7071},
						{vec: vector.T{1, 0}, exact: 0, estimate: 0, errorBound: 0.7071},
					},
				},
			},
		},
		{
			name:    "query equals centroid at origin",
			query:   vector.T{0, 0},
			vectors: []vector.T{{-2, 0}, {2, 0}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 4, estimate: 4, errorBound: 0},
						{vec: vector.T{2, 0}, exact: 4, estimate: 4, errorBound: 0},
					},
				},
				"InnerProduct": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-2, 0}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{2, 0}, exact: 0, estimate: 0, errorBound: 0},
					},
				},
				"Cosine": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{-1, 0}, exact: 1, estimate: 1, errorBound: 0},
						{vec: vector.T{1, 0}, exact: 1, estimate: 1, errorBound: 0},
					},
				},
			},
		},
		{
			name:    "query equals centroid non-origin",
			query:   vector.T{2, 2},
			vectors: []vector.T{{0, 2}, {4, 2}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{0, 2}, exact: 4, estimate: 4, errorBound: 0},
						{vec: vector.T{4, 2}, exact: 4, estimate: 4, errorBound: 0},
					},
				},
				"InnerProduct": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{0, 2}, exact: -4, estimate: -4, errorBound: 0},
						{vec: vector.T{4, 2}, exact: -12, estimate: -12, errorBound: 0},
					},
				},
				"Cosine": {
					centroid: vector.T{0.4472, 0.7236},
					members: []memberCase{
						{vec: vector.T{0, 1}, exact: 0.293, estimate: 0.2777, errorBound: 0.0968},
						{
							vec:        vector.T{0.8944, 0.4472},
							exact:      0.051,
							estimate:   0.0665,
							errorBound: 0.0968,
						},
					},
				},
			},
		},
		{
			name:    "all vectors same as query",
			query:   vector.T{2, 2},
			vectors: []vector.T{{2, 2}, {2, 2}, {2, 2}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{2, 2}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{2, 2}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{2, 2}, exact: 0, estimate: 0, errorBound: 0},
					},
				},
				"InnerProduct": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{2, 2}, exact: -8, estimate: -8, errorBound: 0},
						{vec: vector.T{2, 2}, exact: -8, estimate: -8, errorBound: 0},
						{vec: vector.T{2, 2}, exact: -8, estimate: -8, errorBound: 0},
					},
				},
				"Cosine": {
					centroid: vector.T{0.7071, 0.7071},
					members: []memberCase{
						{vec: vector.T{0.7071, 0.7071}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0.7071, 0.7071}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0.7071, 0.7071}, exact: 0, estimate: 0, errorBound: 0},
					},
				},
			},
		},
		{
			name:    "all vectors same query different",
			query:   vector.T{3, 4},
			vectors: []vector.T{{2, 2}, {2, 2}, {2, 2}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{2, 2}, exact: 5, estimate: 5, errorBound: 0},
						{vec: vector.T{2, 2}, exact: 5, estimate: 5, errorBound: 0},
						{vec: vector.T{2, 2}, exact: 5, estimate: 5, errorBound: 0},
					},
				},
				"InnerProduct": {
					centroid: vector.T{2, 2},
					members: []memberCase{
						{vec: vector.T{2, 2}, exact: -14, estimate: -14, errorBound: 0},
						{vec: vector.T{2, 2}, exact: -14, estimate: -14, errorBound: 0},
						{vec: vector.T{2, 2}, exact: -14, estimate: -14, errorBound: 0},
					},
				},
				"Cosine": {
					centroid: vector.T{0.7071, 0.7071},
					members: []memberCase{
						{
							vec:        vector.T{0.7071, 0.7071},
							exact:      0.01,
							estimate:   0.0101,
							errorBound: 0,
						},
						{
							vec:        vector.T{0.7071, 0.7071},
							exact:      0.01,
							estimate:   0.0101,
							errorBound: 0,
						},
						{
							vec:        vector.T{0.7071, 0.7071},
							exact:      0.01,
							estimate:   0.0101,
							errorBound: 0,
						},
					},
				},
			},
		},
		{
			name:    "all zeros",
			query:   vector.T{0, 0},
			vectors: []vector.T{{0, 0}, {0, 0}, {0, 0}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{0, 0}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0, 0}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0, 0}, exact: 0, estimate: 0, errorBound: 0},
					},
				},
				"InnerProduct": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{0, 0}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0, 0}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0, 0}, exact: 0, estimate: 0, errorBound: 0},
					},
				},
				"Cosine": {
					centroid: vector.T{0, 0},
					members: []memberCase{
						{vec: vector.T{0, 0}, exact: 1, estimate: 1, errorBound: 0},
						{vec: vector.T{0, 0}, exact: 1, estimate: 1, errorBound: 0},
						{vec: vector.T{0, 0}, exact: 1, estimate: 1, errorBound: 0},
					},
				},
			},
		},
		{
			name:    "colinear different scales",
			query:   vector.T{10, 0},
			vectors: []vector.T{{1, 0}, {4, 0}, {16, 0}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{7, 0},
					members: []memberCase{
						{vec: vector.T{1, 0}, exact: 81, estimate: 81, errorBound: 25.5},
						{vec: vector.T{4, 0}, exact: 36, estimate: 36, errorBound: 12.7},
						{vec: vector.T{16, 0}, exact: 36, estimate: 36, errorBound: 38.2},
					},
				},
				"InnerProduct": {
					centroid: vector.T{7, 0},
					members: []memberCase{
						{vec: vector.T{1, 0}, exact: -10, estimate: -10, errorBound: 12.7},
						{vec: vector.T{4, 0}, exact: -40, estimate: -40, errorBound: 6.4},
						{vec: vector.T{16, 0}, exact: -160, estimate: -160, errorBound: 19.1},
					},
				},
				"Cosine": {
					centroid: vector.T{1, 0},
					members: []memberCase{
						{vec: vector.T{1, 0}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{1, 0}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{1, 0}, exact: 0, estimate: 0, errorBound: 0},
					},
				},
			},
		},
		{
			name:    "cloud of locations",
			query:   vector.T{3, 4},
			vectors: []vector.T{{5, -1}, {2, 2}, {3, 4}, {4, 3}, {1, 8}, {12, 5}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{4.5, 3.5},
					members: []memberCase{
						{vec: vector.T{5, -1}, exact: 29, estimate: 39.4, errorBound: 10.1},
						{vec: vector.T{2, 2}, exact: 5, estimate: 6.8, errorBound: 6.5},
						{vec: vector.T{3, 4}, exact: 0, estimate: 0, errorBound: 3.5},
						{vec: vector.T{4, 3}, exact: 2, estimate: 2, errorBound: 1.6},
						{vec: vector.T{1, 8}, exact: 20, estimate: 18.7, errorBound: 12.7},
						{vec: vector.T{12, 5}, exact: 82, estimate: 74, errorBound: 17.1},
					},
				},
				"InnerProduct": {
					centroid: vector.T{4.5, 3.5},
					members: []memberCase{
						{vec: vector.T{5, -1}, exact: -11, estimate: -5.8, errorBound: 5.1},
						{vec: vector.T{2, 2}, exact: -14, estimate: -13.1, errorBound: 3.3},
						{vec: vector.T{3, 4}, exact: -25, estimate: -25, errorBound: 1.8},
						{vec: vector.T{4, 3}, exact: -24, estimate: -24, errorBound: 0.8},
						{vec: vector.T{1, 8}, exact: -35, estimate: -35.6, errorBound: 6.4},
						{vec: vector.T{12, 5}, exact: -56, estimate: -60, errorBound: 8.6},
					},
				},
				"Cosine": {
					centroid: vector.T{0.6891, 0.548},
					members: []memberCase{
						{
							vec:        vector.T{0.9806, -0.1961},
							exact:      0.569,
							estimate:   0.5654,
							errorBound: 0.1511,
						},
						{
							vec:        vector.T{0.7071, 0.7071},
							exact:      0.01,
							estimate:   0.025,
							errorBound: 0.0303,
						},
						{vec: vector.T{0.6, 0.8}, exact: 0, estimate: 0, errorBound: 0.0505},
						{
							vec:        vector.T{0.8, 0.6},
							exact:      0.04,
							estimate:   0.0282,
							errorBound: 0.0231,
						},
						{
							vec:        vector.T{0.124, 0.9923},
							exact:      0.132,
							estimate:   0.1195,
							errorBound: 0.1359,
						},
						{
							vec:        vector.T{0.9231, 0.3846},
							exact:      0.138,
							estimate:   0.1463,
							errorBound: 0.0539,
						},
					},
				},
			},
		},
		{
			name:    "query far outside data cloud",
			query:   vector.T{100, 100},
			vectors: []vector.T{{5, -1}, {2, 2}, {3, 4}, {4, 3}, {1, 8}, {12, 6}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{4.5, 3.6667},
					members: []memberCase{
						{vec: vector.T{5, -1}, exact: 19226, estimate: 18429.5, errorBound: 900.4},
						{vec: vector.T{2, 2}, exact: 19208, estimate: 19240.7, errorBound: 576.4},
						{vec: vector.T{3, 4}, exact: 18625, estimate: 18400.6, errorBound: 294.8},
						{vec: vector.T{4, 3}, exact: 18625, estimate: 18629.4, errorBound: 159.9},
						{vec: vector.T{1, 8}, exact: 18265, estimate: 18424.8, errorBound: 1068.6},
						{vec: vector.T{12, 6}, exact: 16580, estimate: 16054.9, errorBound: 1506.8},
					},
				},
				"InnerProduct": {
					centroid: vector.T{4.5, 3.6667},
					members: []memberCase{
						{vec: vector.T{5, -1}, exact: -400, estimate: -798.3, errorBound: 450.2},
						{vec: vector.T{2, 2}, exact: -400, estimate: -383.7, errorBound: 288.2},
						{vec: vector.T{3, 4}, exact: -700, estimate: -812.2, errorBound: 147.4},
						{vec: vector.T{4, 3}, exact: -700, estimate: -697.8, errorBound: 79.9},
						{vec: vector.T{1, 8}, exact: -900, estimate: -820.1, errorBound: 534.3},
						{vec: vector.T{12, 6}, exact: -1800, estimate: -2062.5, errorBound: 753.4},
					},
				},
				"Cosine": {
					centroid: vector.T{0.6844, 0.5584},
					members: []memberCase{
						{
							vec:        vector.T{0.9806, -0.1961},
							exact:      0.445,
							estimate:   0.4186,
							errorBound: 0.0862,
						},
						{vec: vector.T{0.7071, 0.7071}, exact: 0, estimate: 0, errorBound: 0.016},
						{
							vec:        vector.T{0.6, 0.8},
							exact:      0.01,
							estimate:   0.0188,
							errorBound: 0.0272,
						},
						{
							vec:        vector.T{0.8, 0.6},
							exact:      0.01,
							estimate:   0.0024,
							errorBound: 0.0131,
						},
						{
							vec:        vector.T{0.124, 0.9923},
							exact:      0.211,
							estimate:   0.1988,
							errorBound: 0.0754,
						},
						{
							vec:        vector.T{0.8944, 0.4472},
							exact:      0.051,
							estimate:   0.0617,
							errorBound: 0.0253,
						},
					},
				},
			},
		},
		{
			name:  "data cloud far from origin",
			query: vector.T{108, 108},
			vectors: []vector.T{
				{105, 99},
				{102, 102},
				{103, 104},
				{104, 103},
				{101, 108},
				{112, 105},
			},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{104.5, 103.5},
					members: []memberCase{
						{vec: vector.T{105, 99}, exact: 90, estimate: 61.2, errorBound: 36.5},
						{vec: vector.T{102, 102}, exact: 72, estimate: 75, errorBound: 23.5},
						{vec: vector.T{103, 104}, exact: 41, estimate: 32.5, errorBound: 12.7},
						{vec: vector.T{104, 103}, exact: 41, estimate: 41, errorBound: 5.7},
						{vec: vector.T{101, 108}, exact: 49, estimate: 56.9, errorBound: 46},
						{vec: vector.T{112, 105}, exact: 25, estimate: 0, errorBound: 48.7},
					},
				},
				"InnerProduct": {
					centroid: vector.T{104.5, 103.5},
					members: []memberCase{
						{
							vec:        vector.T{105, 99},
							exact:      -22032,
							estimate:   -22046.4,
							errorBound: 18.3,
						},
						{
							vec:        vector.T{102, 102},
							exact:      -22032,
							estimate:   -22030.5,
							errorBound: 11.8,
						},
						{
							vec:        vector.T{103, 104},
							exact:      -22356,
							estimate:   -22360.2,
							errorBound: 6.4,
						},
						{vec: vector.T{104, 103}, exact: -22356, estimate: -22356, errorBound: 2.9},
						{
							vec:        vector.T{101, 108},
							exact:      -22572,
							estimate:   -22568.1,
							errorBound: 23,
						},
						{
							vec:        vector.T{112, 105},
							exact:      -23436,
							estimate:   -23455,
							errorBound: 30.8,
						},
					},
				},
				"Cosine": {
					centroid: vector.T{0.7102, 0.7036},
					members: []memberCase{
						{
							vec:        vector.T{0.7276, 0.686},
							exact:      0,
							estimate:   0.0004,
							errorBound: 0.0001,
						},
						{vec: vector.T{0.7071, 0.7071}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0.7037, 0.7105}, exact: 0, estimate: 0, errorBound: 0},
						{vec: vector.T{0.7105, 0.7037}, exact: 0, estimate: 0, errorBound: 0},
						{
							vec:        vector.T{0.683, 0.7304},
							exact:      0.001,
							estimate:   0.0006,
							errorBound: 0.0001,
						},
						{
							vec:        vector.T{0.7295, 0.6839},
							exact:      0.001,
							estimate:   0.0005,
							errorBound: 0.0001,
						},
					},
				},
			},
		},
		{
			name:  "more dimensions",
			query: vector.T{4, 3, 7, 8},
			vectors: []vector.T{
				{5, -1, 3, 10},
				{2, 2, -5, 4},
				{3, 4, 8, 7},
				{4, 3, 7, 8},
				{1, 8, 10, 12},
				{12, 5, 6, -4},
			},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{4.5, 3.5, 4.8333, 6.1667},
					members: []memberCase{
						{vec: vector.T{5, -1, 3, 10}, exact: 37, estimate: 49.7, errorBound: 18.2},
						{vec: vector.T{2, 2, -5, 4}, exact: 165, estimate: 159.3, errorBound: 30.7},
						{vec: vector.T{3, 4, 8, 7}, exact: 4, estimate: 4.2, errorBound: 10.6},
						{vec: vector.T{4, 3, 7, 8}, exact: 0, estimate: 0.1, errorBound: 8.6},
						{vec: vector.T{1, 8, 10, 12}, exact: 59, estimate: 62.7, errorBound: 28.2},
						{
							vec:        vector.T{12, 5, 6, -4},
							exact:      213,
							estimate:   182.1,
							errorBound: 37.4,
						},
					},
				},
				"InnerProduct": {
					centroid: vector.T{4.5, 3.5, 4.8333, 6.1667},
					members: []memberCase{
						{
							vec:        vector.T{5, -1, 3, 10},
							exact:      -118,
							estimate:   -111.7,
							errorBound: 9.1,
						},
						{vec: vector.T{2, 2, -5, 4}, exact: -11, estimate: -13.8, errorBound: 15.3},
						{vec: vector.T{3, 4, 8, 7}, exact: -136, estimate: -135.9, errorBound: 5.3},
						{vec: vector.T{4, 3, 7, 8}, exact: -138, estimate: -138, errorBound: 4.3},
						{
							vec:        vector.T{1, 8, 10, 12},
							exact:      -194,
							estimate:   -192.1,
							errorBound: 14.1,
						},
						{
							vec:        vector.T{12, 5, 6, -4},
							exact:      -73,
							estimate:   -88.4,
							errorBound: 18.7,
						},
					},
				},
				"Cosine": {
					centroid: vector.T{0.3627, 0.2645, 0.2989, 0.5204},
					members: []memberCase{
						{
							vec:        vector.T{0.4303, -0.0861, 0.2582, 0.8607},
							exact:      0.135,
							estimate:   0.2254,
							errorBound: 0.0838,
						},
						{
							vec:        vector.T{0.2857, 0.2857, -0.7143, 0.5714},
							exact:      0.866,
							estimate:   0.6698,
							errorBound: 0.1722,
						},
						{
							vec:        vector.T{0.2554, 0.3405, 0.681, 0.5959},
							exact:      0.014,
							estimate:   0.0132,
							errorBound: 0.0696,
						},
						{
							vec:        vector.T{0.3405, 0.2554, 0.5959, 0.681},
							exact:      0,
							estimate:   0,
							errorBound: 0.0572,
						},
						{
							vec:        vector.T{0.0569, 0.4551, 0.5689, 0.6827},
							exact:      0.061,
							estimate:   0.0515,
							errorBound: 0.081,
						},
						{
							vec:        vector.T{0.8072, 0.3363, 0.4036, -0.2691},
							exact:      0.582,
							estimate:   0.4137,
							errorBound: 0.1548,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var allocator allocator.StackAllocator
			dims := len(tc.query)

			// Test each metric
			for metricName, expected := range tc.expected {
				t.Run(metricName, func(t *testing.T) {
					var metric vector.DistanceMetric
					var queryVec vector.T
					var vectors *vector.Set

					// Make copies to avoid modifying original test data
					queryVec = make(vector.T, len(tc.query))
					copy(queryVec, tc.query)

					vectors = vector.MakeSet(dims)
					for _, v := range tc.vectors {
						vec := make(vector.T, len(v))
						copy(vec, v)
						vectors.Add(vec)
					}

					switch metricName {
					case "L2Squared":
						metric = vector.DistanceMetric_L2Squared
					case "InnerProduct":
						metric = vector.DistanceMetric_InnerProduct
					case "Cosine":
						metric = vector.DistanceMetric_Cosine
						// Normalize vectors for cosine
						vec.NormalizeFloat32(queryVec)
						for i := range vectors.GetCount() {
							vec.NormalizeFloat32(vectors.At(int(i)))
						}
					}

					centroid := make(vector.T, dims)
					centroid = vectors.Centroid(centroid)

					// Test NonQuantizer
					nonQuantizer := quantize.NewNonQuantizer(dims, metric)
					nonQuantizedSet := nonQuantizer.Quantize(&allocator, centroid, vectors)
					exactDistances := make([]float32, nonQuantizedSet.GetCount())
					exactErrorBounds := make([]float32, nonQuantizedSet.GetCount())
					nonQuantizer.EstimateDistances(
						&allocator,
						nonQuantizedSet,
						queryVec,
						exactDistances,
						exactErrorBounds,
					)

					// Test RaBitQuantizer
					rabitQ := quantize.NewRaBitQuantizer(dims, 42, metric)
					rabitQSet := rabitQ.Quantize(&allocator, centroid, vectors)
					estimatedDistances := make([]float32, rabitQSet.GetCount())
					errorBounds := make([]float32, rabitQSet.GetCount())
					rabitQ.EstimateDistances(
						&allocator,
						rabitQSet,
						queryVec,
						estimatedDistances,
						errorBounds,
					)

					// Verify centroid
					qsCentroid := rabitQSet.(*quantize.RaBitQuantizedVectorSet).GetCentroid()
					for i, val := range expected.centroid {
						assert.InDelta(t, val, qsCentroid[i], 0.01,
							"centroid[%d]: expected %v, got %v", i, val, qsCentroid[i])
					}

					// Verify distances and error bounds
					for i, member := range expected.members {
						// Verify exact distance
						assert.InDelta(
							t,
							member.exact,
							exactDistances[i],
							0.1,
							"exact distance[%d]: expected %v, got %v",
							i,
							member.exact,
							exactDistances[i],
						)

						// Verify estimated distance
						assert.InDelta(
							t,
							member.estimate,
							estimatedDistances[i],
							0.1,
							"estimated distance[%d]: expected %v, got %v",
							i,
							member.estimate,
							estimatedDistances[i],
						)

						// Verify error bound
						assert.InDelta(
							t,
							member.errorBound,
							errorBounds[i],
							0.1,
							"error bound[%d]: expected %v, got %v",
							i,
							member.errorBound,
							errorBounds[i],
						)
					}
				})
			}
		})
	}
}

func TestGetCentroidDistances(t *testing.T) {
	type distanceCase struct {
		vec                       vector.T
		meanCentroidDistance      float32
		sphericalCentroidDistance float32
	}

	type metricExpectations struct {
		centroid  vector.T
		distances []distanceCase
	}

	testCases := []struct {
		name     string
		dims     int
		vectors  []vector.T
		expected map[string]metricExpectations
	}{
		{
			name:    "centroid at origin",
			dims:    2,
			vectors: []vector.T{{-2, 0}, {2, 0}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{0, 0},
					distances: []distanceCase{
						{
							vec:                       vector.T{-2, 0},
							meanCentroidDistance:      4,
							sphericalCentroidDistance: 4,
						},
						{
							vec:                       vector.T{2, 0},
							meanCentroidDistance:      4,
							sphericalCentroidDistance: 4,
						},
					},
				},
				"InnerProduct": {
					centroid: vector.T{0, 0},
					distances: []distanceCase{
						{
							vec:                       vector.T{-2, 0},
							meanCentroidDistance:      0,
							sphericalCentroidDistance: 0,
						},
						{
							vec:                       vector.T{2, 0},
							meanCentroidDistance:      0,
							sphericalCentroidDistance: 0,
						},
					},
				},
				"Cosine": {
					centroid: vector.T{0, 0},
					distances: []distanceCase{
						{
							vec:                       vector.T{-1, 0},
							meanCentroidDistance:      1,
							sphericalCentroidDistance: 1,
						},
						{
							vec:                       vector.T{1, 0},
							meanCentroidDistance:      1,
							sphericalCentroidDistance: 1,
						},
					},
				},
			},
		},
		{
			name:    "centroid at [0,2]",
			dims:    2,
			vectors: []vector.T{{-2, 2}, {2, 2}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{0, 2},
					distances: []distanceCase{
						{
							vec:                       vector.T{-2, 2},
							meanCentroidDistance:      4,
							sphericalCentroidDistance: 4,
						},
						{
							vec:                       vector.T{2, 2},
							meanCentroidDistance:      4,
							sphericalCentroidDistance: 4,
						},
					},
				},
				"InnerProduct": {
					centroid: vector.T{0, 2},
					distances: []distanceCase{
						{
							vec:                       vector.T{-2, 2},
							meanCentroidDistance:      -4,
							sphericalCentroidDistance: -2,
						},
						{
							vec:                       vector.T{2, 2},
							meanCentroidDistance:      -4,
							sphericalCentroidDistance: -2,
						},
					},
				},
				"Cosine": {
					centroid: vector.T{0, 0.7071},
					distances: []distanceCase{
						{
							vec:                       vector.T{-0.7071, 0.7071},
							meanCentroidDistance:      0.2929,
							sphericalCentroidDistance: 0.2929,
						},
						{
							vec:                       vector.T{0.7071, 0.7071},
							meanCentroidDistance:      0.2929,
							sphericalCentroidDistance: 0.2929,
						},
					},
				},
			},
		},
		{
			name:    "centroid at [3,4]",
			dims:    2,
			vectors: []vector.T{{2, 6}, {4, 3}, {3, 3}},
			expected: map[string]metricExpectations{
				"L2Squared": {
					centroid: vector.T{3, 4},
					distances: []distanceCase{
						{
							vec:                       vector.T{2, 6},
							meanCentroidDistance:      5,
							sphericalCentroidDistance: 5,
						},
						{
							vec:                       vector.T{4, 3},
							meanCentroidDistance:      2,
							sphericalCentroidDistance: 2,
						},
						{
							vec:                       vector.T{3, 3},
							meanCentroidDistance:      1,
							sphericalCentroidDistance: 1,
						},
					},
				},
				"InnerProduct": {
					centroid: vector.T{3, 4},
					distances: []distanceCase{
						{
							vec:                       vector.T{2, 6},
							meanCentroidDistance:      -30,
							sphericalCentroidDistance: -6,
						},
						{
							vec:                       vector.T{4, 3},
							meanCentroidDistance:      -24,
							sphericalCentroidDistance: -4.8,
						},
						{
							vec:                       vector.T{3, 3},
							meanCentroidDistance:      -21,
							sphericalCentroidDistance: -4.2,
						},
					},
				},
				"Cosine": {
					centroid: vector.T{0.6078, 0.7519},
					distances: []distanceCase{
						{
							vec:                       vector.T{0.3162, 0.9487},
							meanCentroidDistance:      0.0634,
							sphericalCentroidDistance: 0.0634,
						},
						{
							vec:                       vector.T{0.8, 0.6},
							meanCentroidDistance:      0.0305,
							sphericalCentroidDistance: 0.0305,
						},
						{
							vec:                       vector.T{0.7071, 0.7071},
							meanCentroidDistance:      0.0056,
							sphericalCentroidDistance: 0.0056,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var workspace allocator.StackAllocator

			// Test each metric
			for metricName, expected := range tc.expected {
				t.Run(metricName, func(t *testing.T) {
					var metric vector.DistanceMetric
					var vectors = vector.MakeSet(tc.dims)
					for _, v := range tc.vectors {
						vec := make(vector.T, len(v))
						copy(vec, v)
						vectors.Add(vec)
					}

					switch metricName {
					case "L2Squared":
						metric = vector.DistanceMetric_L2Squared
					case "InnerProduct":
						metric = vector.DistanceMetric_InnerProduct
					case "Cosine":
						metric = vector.DistanceMetric_Cosine
						// Normalize vectors for cosine
						for i := range vectors.GetCount() {
							vec.NormalizeFloat32(vectors.At(int(i)))
						}
					}

					centroid := make(vector.T, vectors.GetDims())
					centroid = vectors.Centroid(centroid)

					// Test RaBitQuantizer
					rabitQ := quantize.NewRaBitQuantizer(tc.dims, 42, metric)
					quantizedSet := rabitQ.Quantize(&workspace, centroid, vectors).(*quantize.RaBitQuantizedVectorSet)

					// Verify centroid
					qsCentroid := quantizedSet.GetCentroid()
					for i, val := range expected.centroid {
						assert.InDelta(t, val, centroid[i], 0.01,
							"centroid[%d]: expected %v, got %v", i, val, qsCentroid[i])
					}

					// Test mean centroid distances
					meanDistances := make([]float32, quantizedSet.GetCount())
					rabitQ.(*quantize.RaBitQuantizer).CalcCentroidDistances(
						quantizedSet,
						meanDistances,
						false, /* spherical */
					)

					// Test spherical centroid distances
					sphericalDistances := make([]float32, quantizedSet.GetCount())
					rabitQ.(*quantize.RaBitQuantizer).CalcCentroidDistances(
						quantizedSet,
						sphericalDistances,
						true, /* spherical */
					)

					// Verify distances
					for i, distCase := range expected.distances {
						assert.InDelta(t, distCase.meanCentroidDistance, meanDistances[i], 0.01,
							"mean centroid distance[%d]: expected %v, got %v",
							i, distCase.meanCentroidDistance, meanDistances[i])

						assert.InDelta(
							t,
							distCase.sphericalCentroidDistance,
							sphericalDistances[i],
							0.01,
							"spherical centroid distance[%d]: expected %v, got %v",
							i,
							distCase.sphericalCentroidDistance,
							sphericalDistances[i],
						)
					}
				})
			}
		})
	}
}

var recallTestCases = []recallTestCase{
	{
		dataset: testutils.ImagesDataset,
		topK:    10,
		count:   1000,
		expected: map[string]float64{
			"Euclidean":    70.00,
			"InnerProduct": 70.00,
			"Cosine":       69.50,
		},
	},
	{
		dataset:   testutils.ImagesDataset,
		randomize: true,
		topK:      10,
		count:     1000,
		expected: map[string]float64{
			"Euclidean":    85.00,
			"InnerProduct": 85.00,
			"Cosine":       85.00,
		},
	},
	{
		dataset: testutils.RandomDataset20d,
		topK:    10,
		count:   1000,
		expected: map[string]float64{
			"Euclidean":    88.00,
			"InnerProduct": 93.50,
			"Cosine":       89.00,
		},
	},
	{
		dataset:   testutils.RandomDataset20d,
		randomize: true,
		topK:      10,
		count:     1000,
		expected: map[string]float64{
			"Euclidean":    88.50,
			"InnerProduct": 89.00,
			"Cosine":       88.50,
		},
	},
	{
		dataset: testutils.FashionMinstDataset1k,
		topK:    10,
		count:   1000,
		expected: map[string]float64{
			"Euclidean":    76.00,
			"InnerProduct": 75.00,
			"Cosine":       70.50,
		},
	},
	{
		dataset:   testutils.FashionMinstDataset1k,
		randomize: true,
		topK:      10,
		count:     1000,
		expected: map[string]float64{
			"Euclidean":    87.50,
			"InnerProduct": 87.00,
			"Cosine":       85.50,
		},
	},
	{
		dataset: testutils.FashionMinstDataset10k,
		topK:    10,
		count:   1000,
		expected: map[string]float64{
			"Euclidean":    67.50,
			"InnerProduct": 83.00,
			"Cosine":       66.50,
		},
	},
	{
		dataset:   testutils.FashionMinstDataset10k,
		randomize: true,
		topK:      10,
		count:     1000,
		expected: map[string]float64{
			"Euclidean":    80.50,
			"InnerProduct": 90.00,
			"Cosine":       83.00,
		},
	},
	{
		dataset: testutils.LaionDatasetCLIP,
		topK:    10,
		count:   1000,
		expected: map[string]float64{
			"Euclidean":    70.50,
			"InnerProduct": 71.50,
			"Cosine":       70.50,
		},
	},
	{
		dataset:   testutils.LaionDatasetCLIP,
		randomize: true,
		topK:      10,
		count:     1000,
		expected: map[string]float64{
			"Euclidean":    81.50,
			"InnerProduct": 80.50,
			"Cosine":       81.00,
		},
	},
	{
		dataset:   testutils.LaionDatasetGemini1k,
		topK:      10,
		count:     1000,
		tolerance: 1.50001,
		expected: map[string]float64{
			"Euclidean":    66.00,
			"InnerProduct": 66.00,
			"Cosine":       66.00,
		},
	},
	{
		dataset:   testutils.LaionDatasetGemini1k,
		topK:      10,
		randomize: true,
		count:     1000,
		tolerance: 1.50001,
		expected: map[string]float64{
			"Euclidean":    79.50,
			"InnerProduct": 79.00,
			"Cosine":       79.00,
		},
	},
	{
		dataset:   testutils.LaionDatasetGemini10k,
		topK:      10,
		count:     1000,
		tolerance: 1.50001,
		expected: map[string]float64{
			"Euclidean":    70.00,
			"InnerProduct": 70.00,
			"Cosine":       70.00,
		},
	},
	{
		dataset:   testutils.LaionDatasetGemini10k,
		topK:      10,
		randomize: true,
		count:     1000,
		tolerance: 1.50001,
		expected: map[string]float64{
			"Euclidean":    72.50,
			"InnerProduct": 72.00,
			"Cosine":       72.00,
		},
	},
	{
		dataset: testutils.DbpediaDataset,
		topK:    10,
		count:   1000,
		expected: map[string]float64{
			"Euclidean":    81.50,
			"InnerProduct": 81.50,
			"Cosine":       81.50,
		},
	},
	{
		dataset:   testutils.DbpediaDataset,
		randomize: true,
		topK:      10,
		count:     1000,
		expected: map[string]float64{
			"Euclidean":    85.00,
			"InnerProduct": 85.00,
			"Cosine":       85.00,
		},
	},
	// 2048-dimensional dataset to test high-dimensional performance
	{
		dataset:   testutils.RandomDataset2048d,
		topK:      10,
		count:     1000,
		tolerance: 1.0,
		expected: map[string]float64{
			"Euclidean":    49.50,
			"InnerProduct": 46.50,
			"Cosine":       47.50,
		},
	},
	{
		dataset:   testutils.RandomDataset2048d,
		topK:      10,
		randomize: true,
		count:     1000,
		tolerance: 1.0,
		expected: map[string]float64{
			"Euclidean":    35.50,
			"InnerProduct": 30.50,
			"Cosine":       30.50,
		},
	},
	// 4096-dimensional dataset to test SME crossover point
	{
		dataset:   testutils.RandomDataset4096d,
		topK:      10,
		count:     1000,
		tolerance: 1.0,
		expected: map[string]float64{
			"Euclidean":    42.50,
			"InnerProduct": 39.00,
			"Cosine":       38.00,
		},
	},
	{
		dataset:   testutils.RandomDataset4096d,
		topK:      10,
		randomize: true,
		count:     1000,
		tolerance: 1.0,
		expected: map[string]float64{
			"Euclidean":    37.00,
			"InnerProduct": 34.00,
			"Cosine":       33.50,
		},
	},
}
