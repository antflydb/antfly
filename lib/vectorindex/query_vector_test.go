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
	"testing"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/stretchr/testify/require"
)

func TestQueryVector(t *testing.T) {
	testCases := []struct {
		name        string
		metric      vector.DistanceMetric
		isLeaf      bool
		queryVector vector.T
		candidates  []*Result
		expected    []float32
	}{
		// L2Squared tests.
		{
			name:        "L2Squared leaf level - query vector at origin",
			metric:      vector.DistanceMetric_L2Squared,
			isLeaf:      true,
			queryVector: vector.T{0, 0},
			candidates: []*Result{
				{Vector: vector.T{3, 4}},  // (0-3)² + (0-4)² = 25
				{Vector: vector.T{0, 0}},  // (0-0)² + (0-0)² = 0
				{Vector: vector.T{-3, 4}}, // (0+3)² + (0-4)² = 25
			},
			expected: []float32{25, 0, 25},
		},
		{
			name:        "L2Squared leaf level - query vector not at origin",
			metric:      vector.DistanceMetric_L2Squared,
			isLeaf:      true,
			queryVector: vector.T{3, 4},
			candidates: []*Result{
				{Vector: vector.T{3, 4}}, // (3-3)² + (4-4)² = 0
				{Vector: vector.T{2, 1}}, // (3-2)² + (4-1)² = 10
				{Vector: vector.T{0, 0}}, // (3-0)² + (4-0)² = 25
			},
			expected: []float32{0, 10, 25},
		},
		{
			name:        "L2Squared interior level - same as leaf",
			metric:      vector.DistanceMetric_L2Squared,
			isLeaf:      false,
			queryVector: vector.T{3, 4},
			candidates: []*Result{
				{Vector: vector.T{3, 4}}, // (3-3)² + (4-4)² = 0
				{Vector: vector.T{2, 1}}, // (3-2)² + (4-1)² = 10
				{Vector: vector.T{0, 0}}, // (3-0)² + (4-0)² = 25
			},
			expected: []float32{0, 10, 25},
		},

		// InnerProduct tests.
		{
			name:        "InnerProduct leaf level",
			metric:      vector.DistanceMetric_InnerProduct,
			isLeaf:      true,
			queryVector: vector.T{3, 4},
			candidates: []*Result{
				{Vector: vector.T{6, 5}}, // -(3*6 + 4*5) = -38
				{Vector: vector.T{0, 0}}, // -(3*0 + 4*0) = 0
			},
			expected: []float32{-38, 0},
		},
		{
			name:        "InnerProduct interior level - normalized centroids",
			metric:      vector.DistanceMetric_InnerProduct,
			isLeaf:      false,
			queryVector: vector.T{3, 4},
			candidates: []*Result{
				{Vector: vector.T{0, 0}},  // normalized = {0, 0}
				{Vector: vector.T{6, 8}},  // normalized = {0.6, 0.8}
				{Vector: vector.T{4, -3}}, // normalized = {0.8, -0.6}
				{Vector: vector.T{-6, 8}}, // normalized = {-0.6, 0.8}
			},
			expected: []float32{0, -5, 0, -1.4},
		},
		{
			name:        "InnerProduct interior level - zero query vector",
			metric:      vector.DistanceMetric_InnerProduct,
			isLeaf:      false,
			queryVector: vector.T{0, 0},
			candidates: []*Result{
				{Vector: vector.T{0, 0}}, // normalized = {0, 0}
				{Vector: vector.T{6, 8}}, // normalized = {0.6, 0.8}
			},
			expected: []float32{0, 0}, // Should not be NaN
		},

		// Cosine tests.
		{
			name:        "Cosine leaf level",
			metric:      vector.DistanceMetric_Cosine,
			isLeaf:      true,
			queryVector: vector.T{4, 3}, // normalized = {0.8, 0.6}
			candidates: []*Result{
				{Vector: vector.T{0, 0}},   // normalized = {0, 0}
				{Vector: vector.T{-3, 4}},  // normalized = {-0.6, 0.8}
				{Vector: vector.T{10, 0}},  // normalized = {1, 0}
				{Vector: vector.T{-3, -4}}, // normalized = {-0.6, -0.8}
			},
			expected: []float32{1, 1, 0.2, 1.96},
		},
		{
			name:        "Cosine leaf level - zero query vector",
			metric:      vector.DistanceMetric_Cosine,
			isLeaf:      true,
			queryVector: vector.T{0, 0}, // normalized = {0, 0}
			candidates: []*Result{
				{Vector: vector.T{0, 0}},  // normalized = {0, 0}
				{Vector: vector.T{-3, 4}}, // normalized = {-0.6, 0.8}
				{Vector: vector.T{10, 0}}, // normalized = {1, 0}
			},
			expected: []float32{1, 1, 1},
		},
		{
			name:        "Cosine interior level",
			metric:      vector.DistanceMetric_Cosine,
			isLeaf:      false,
			queryVector: vector.T{4, 3}, // normalized = {0.8, 0.6}
			candidates: []*Result{
				{Vector: vector.T{0, 0}},
				{Vector: vector.T{1, 0}},
				{Vector: vector.T{0.8, 0.6}},
			},
			expected: []float32{1, 0.2, 0},
		},
		{
			name:        "Cosine interior level - zero query vector",
			metric:      vector.DistanceMetric_Cosine,
			isLeaf:      false,
			queryVector: vector.T{0, 0},
			candidates: []*Result{
				{Vector: vector.T{0, 0}},
				{Vector: vector.T{1, 0}},
				{Vector: vector.T{0.8, 0.6}},
			},
			expected: []float32{1, 1, 1}, // Should not be NaN
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var rot vector.RandomOrthogonalTransformer
			rot.Init(vector.RotAlgorithm_Givens, len(tc.queryVector), 42)

			var comparer queryVector
			comparer.InitOriginal(tc.metric, tc.queryVector, &rot)

			// Make a copy of candidates.
			candidates := make([]*Result, len(tc.candidates))
			copy(candidates, tc.candidates)

			// Randomize candidates from an interior level.
			if !tc.isLeaf {
				for i := range tc.candidates {
					rot.Transform(tc.candidates[i].Vector, candidates[i].Vector)
				}
			}

			// Test ComputeExactDistances.
			comparer.ComputeExactDistances(tc.isLeaf, candidates)
			require.Len(t, candidates, len(tc.expected), "number of candidates should be preserved")

			for i, expected := range tc.expected {
				require.InDelta(t, expected, candidates[i].Distance, 1e-5,
					"distance mismatch for candidate %d", i)

				// Error bound should always be 0 for exact distances.
				require.Equal(t, float32(0), candidates[i].ErrorBound,
					"error bound should be 0 for exact distances")
			}

			// Test InitRandomized for interior levels.
			if tc.isLeaf {
				return
			}

			// Transform the query vector.
			queryVector := make(vector.T, len(tc.queryVector))
			rot.Transform(tc.queryVector, queryVector)
			if tc.metric == vector.DistanceMetric_Cosine {
				vec.NormalizeFloat32(queryVector)
			}
			comparer.InitTransformed(tc.metric, queryVector, &rot)

			// Test the main method.
			comparer.ComputeExactDistances(tc.isLeaf, candidates)
			require.Len(t, candidates, len(tc.expected), "number of candidates should be preserved")

			for i, expected := range tc.expected {
				require.InDelta(t, expected, candidates[i].Distance, 1e-5,
					"distance mismatch for candidate %d", i)

				// Error bound should always be 0 for exact distances.
				require.Equal(t, float32(0), candidates[i].ErrorBound,
					"error bound should be 0 for exact distances")
			}
		})
	}
}
