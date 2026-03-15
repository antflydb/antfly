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

package cluster

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/stretchr/testify/require"
)

func TestBalancedBinaryKMeans(t *testing.T) {
	calcMeanDistance := func(
		distanceMetric vector.DistanceMetric,
		vectors *vector.Set,
		centroid vector.T,
		assignments []uint64,
		assignVal uint64,
	) float32 {
		if distanceMetric == vector.DistanceMetric_Cosine ||
			distanceMetric == vector.DistanceMetric_InnerProduct {
			centroid = slices.Clone(centroid)
			vec.NormalizeFloat32(centroid)
		}
		var distanceSum float32
		var count int
		for i, val := range assignments {
			if val != assignVal {
				continue
			}
			distance := vector.MeasureDistance(distanceMetric, vectors.At(i), centroid)
			distanceSum += distance
			count++
		}
		return distanceSum / float32(count)
	}

	workspace := &allocator.StackAllocator{}
	images := testutils.LoadDataset(t, testutils.ImagesDataset)
	fashion := testutils.LoadDataset(t, testutils.FashionMinstDataset1k)

	testCases := []struct {
		desc           string
		distanceMetric vector.DistanceMetric
		vectors        *vector.Set
		assignments    []uint64
		leftCentroid   vector.T
		rightCentroid  vector.T
		skipPinTest    bool
	}{
		{
			desc:           "partition vector set with only 2 elements",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors:        vector.MakeSetFromRawData([]float32{1, 2}, 1),
			assignments:    []uint64{0, 1},
			leftCentroid:   []float32{1},
			rightCentroid:  []float32{2},
		},
		{
			desc:           "partition vector set with duplicates values",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 1,
				1, 1,
				1, 1,
				1, 1,
				1, 1,
			}, 2),
			assignments:   []uint64{0, 0, 1, 1, 1},
			leftCentroid:  []float32{1, 1},
			rightCentroid: []float32{1, 1},
		},
		{
			desc:           "partition 5x3 set of vectors",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				2, 5, 10,
				4, 6, 1,
				0, 0, 0,
				10, 15, 20,
				4, 7, 2,
			}, 3),
			assignments:   []uint64{0, 1, 0, 0, 1, 0},
			leftCentroid:  []float32{2.25, 3.75, 1.5},
			rightCentroid: []float32{6, 10, 15},
		},
		{
			// Unbalanced vector set, with 4 vectors close together and 1 far.
			// One of the close vectors will be grouped with the far vector due
			// to the balancing constraint.
			desc:           "unbalanced vector set",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				3, 0,
				2, 1,
				1, 2,
				4, 2,
				20, 30,
			}, 2),
			assignments:   []uint64{0, 0, 0, 1, 1},
			leftCentroid:  []float32{2, 1},
			rightCentroid: []float32{12, 16},
		},
		{
			desc:           "very small values close to one another",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				1.23e-10, 2.58e-10,
				1.25e-10, 2.60e-10,
				1.26e-10, 2.61e-10,
				1.24e-10, 2.59e-10,
			}, 2),
			assignments:   []uint64{1, 0, 0, 1},
			leftCentroid:  vector.T{1.255e-10, 2.605e-10},
			rightCentroid: vector.T{1.235e-10, 2.585e-10},
		},
		{
			desc:           "inner product distance",
			distanceMetric: vector.DistanceMetric_InnerProduct,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				2, 5, -10,
				-4, 6, 1,
				0, 0, 0,
				9, -14, 20,
				5, 9, 4,
			}, 3),
			assignments: []uint64{0, 1, 1, 1, 0, 0},
			skipPinTest: true,
		},
		{
			// Co-linear vectors are an edge case. The spherical centroids are the
			// same, so the vectors are the same distance to both. Therefore, they are
			// arbitrarily assigned to the left or right partition.
			desc:           "inner product distance, co-linear vectors",
			distanceMetric: vector.DistanceMetric_InnerProduct,
			vectors: vector.MakeSetFromRawData([]float32{
				0, 1,
				0, 10,
				0, 100,
				0, 1000,
			}, 2),
			assignments: []uint64{0, 0, 1, 1},
			skipPinTest: true,
		},
		{
			desc:           "cosine distance",
			distanceMetric: vector.DistanceMetric_Cosine,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 0, 0,
				0.57735, 0.57735, 0.57735,
				0, 0, 1,
				0, 0, 0,
				0, 1, 0,
				0.95672, -0.06355, -0.28399,
			}, 3),
			assignments: []uint64{0, 1, 1, 1, 1, 0},
		},
		{
			desc:           "high-dimensional unit vectors, Euclidean distance",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors:        images.Slice(0, 100),
			// It's challenging to test pinLeftCentroid for this case, due to the
			// inherent randomness of the K-means++ algorithm. The other test cases
			// should be sufficient to test that, however.
			skipPinTest: true,
		},
		{
			desc:           "high-dimensional unit vectors, InnerProduct distance",
			distanceMetric: vector.DistanceMetric_InnerProduct,
			vectors:        images.Slice(0, 100),
			skipPinTest:    true,
		},
		{
			desc:           "high-dimensional unit vectors, Cosine distance",
			distanceMetric: vector.DistanceMetric_Cosine,
			vectors:        images.Slice(0, 100),
			skipPinTest:    true,
		},
		{
			desc:           "high-dimensional non-unit vectors, InnerProduct distance",
			distanceMetric: vector.DistanceMetric_InnerProduct,
			vectors:        fashion.Slice(0, 100),
			skipPinTest:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Re-initialize rng for each iteration so that order of test cases
			// doesn't matter.
			kmeans := BalancedBinaryKmeans{
				Allocator:      workspace,
				Rand:           rand.New(rand.NewPCG(42, 1048)),
				DistanceMetric: tc.distanceMetric,
			}

			// Compute centroids for the vectors.
			leftCentroid := make(vector.T, tc.vectors.GetDims())
			rightCentroid := make(vector.T, tc.vectors.GetDims())
			kmeans.ComputeCentroids(
				tc.vectors, leftCentroid, rightCentroid, false /* pinLeftCentroid */)

			// Assign vectors to closest centroid.
			assignments := make([]uint64, tc.vectors.GetCount())
			leftCount := kmeans.AssignPartitions(
				tc.vectors,
				leftCentroid,
				rightCentroid,
				assignments,
			)
			if tc.assignments != nil {
				var count int
				for _, val := range assignments {
					if val == 0 {
						count++
					}
				}
				require.Equal(t, count, leftCount)
				require.Equal(t, tc.assignments, assignments)
			}
			if tc.leftCentroid != nil {
				require.Equal(t, tc.leftCentroid, leftCentroid)
			} else {
				// Fallback on calculation.
				expected := make(vector.T, tc.vectors.GetDims())
				calcPartitionCentroid(tc.vectors, assignments, 0, expected)
				require.InDeltaSlice(t, expected, leftCentroid, 1e-6)
			}
			if tc.rightCentroid != nil {
				require.Equal(t, tc.rightCentroid, rightCentroid)
			} else {
				// Fallback on calculation.
				expected := make(vector.T, tc.vectors.GetDims())
				calcPartitionCentroid(tc.vectors, assignments, 1, expected)
				require.InDeltaSlice(t, expected, rightCentroid, 1e-6)
			}
			ratio := float64(leftCount) / float64(int(tc.vectors.GetCount())-leftCount)
			require.GreaterOrEqual(t, ratio, 0.45)
			require.LessOrEqual(t, ratio, 2.05)

			// Ensure that left vectors are closer to the left partition than to
			// the right partition.
			leftMean := calcMeanDistance(
				tc.distanceMetric,
				tc.vectors,
				leftCentroid,
				assignments,
				0,
			)
			rightMean := calcMeanDistance(
				tc.distanceMetric,
				tc.vectors,
				rightCentroid,
				assignments,
				0,
			)
			require.LessOrEqual(t, leftMean, rightMean)

			// Ensure that right vectors are closer to the right partition than to
			// the left partition.
			leftMean = calcMeanDistance(tc.distanceMetric, tc.vectors, leftCentroid, assignments, 1)
			rightMean = calcMeanDistance(
				tc.distanceMetric,
				tc.vectors,
				rightCentroid,
				assignments,
				1,
			)
			require.GreaterOrEqual(t, leftMean, rightMean)

			if !tc.skipPinTest {
				// Check that pinning the left centroid returns the same right centroid.
				newLeftCentroid := slices.Clone(leftCentroid)
				newRightCentroid := make(vector.T, len(rightCentroid))
				kmeans.ComputeCentroids(
					tc.vectors, newLeftCentroid, newRightCentroid, true /* pinLeftCentroid */)
				require.Equal(t, leftCentroid, newLeftCentroid)
				require.Equal(t, rightCentroid, newRightCentroid)
			}
		})
	}

	t.Run("assign zero vectors", func(t *testing.T) {
		kmeans := BalancedBinaryKmeans{Allocator: workspace}
		vectors := vector.MakeSetFromRawData([]float32{}, 2)
		leftCentroid := vector.T{1, 2}
		rightCentroid := vector.T{3, 4}
		assignments := make([]uint64, 0)
		leftCount := kmeans.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments)
		require.Equal(t, 0, leftCount)
		require.Equal(t, []uint64{}, assignments)
	})

	t.Run("assign one vector", func(t *testing.T) {
		kmeans := BalancedBinaryKmeans{Allocator: workspace}
		vectors := vector.MakeSetFromRawData([]float32{0, 0}, 2)
		leftCentroid := vector.T{1, 2}
		rightCentroid := vector.T{3, 4}
		assignments := make([]uint64, 1)
		leftCount := kmeans.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments)
		require.Equal(t, 0, leftCount)
		require.Equal(t, []uint64{1}, assignments)
	})

	t.Run("imbalanced partition assignment", func(t *testing.T) {
		kmeans := BalancedBinaryKmeans{Allocator: workspace}
		vectors := vector.MakeSetFromRawData([]float32{3, 4, 5, 6, 7, 8, 1, 2, 9, 10}, 2)
		leftCentroid := vector.T{1, 2}
		rightCentroid := vector.T{3, 4}
		assignments := make([]uint64, 5)

		leftCount := kmeans.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments)
		require.Equal(t, 2, leftCount)
		require.Equal(t, []uint64{0, 1, 1, 0, 1}, assignments)

		leftCentroid = vector.T{7, 8}
		rightCentroid = vector.T{9, 10}
		leftCount = kmeans.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments)
		require.Equal(t, 3, leftCount)
		require.Equal(t, []uint64{0, 0, 1, 0, 1}, assignments)
	})

	t.Run("use global random number generator", func(t *testing.T) {
		kmeans := BalancedBinaryKmeans{Allocator: workspace}
		vectors := vector.MakeSetFromRawData([]float32{1, 2, 3, 4}, 2)
		leftCentroid := make(vector.T, 2)
		rightCentroid := make(vector.T, 2)
		kmeans.ComputeCentroids(
			vectors, leftCentroid, rightCentroid, false /* pinLeftCentroid */)
	})
}
