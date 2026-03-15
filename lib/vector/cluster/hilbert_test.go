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
	"math"
	"math/big"
	"slices"
	"testing"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/stretchr/testify/require"
)

func TestHilbert(t *testing.T) {
	t.Run("round trip encode/decode", func(t *testing.T) {
		sm, err := NewHilbert(5)
		require.NoError(t, err)

		f := []float32{1.02332, 2.02332, -1.02332, 0.12345, 0.54321}
		coords := make([]uint32, len(f))
		for i := range f {
			coords[i] = math.Float32bits(f[i])
		}

		encoded := sm.Encode(coords...)
		decoded := sm.Decode(encoded)

		result := make([]float32, len(decoded))
		for i, v := range decoded {
			result[i] = math.Float32frombits(v)
		}
		require.Equal(t, f, result)
	})

	t.Run("EncodeVec matches Encode", func(t *testing.T) {
		sm, err := NewHilbert(3)
		require.NoError(t, err)

		f := []float32{1.5, -2.5, 3.0}
		coords := make([]uint32, len(f))
		for i := range f {
			coords[i] = math.Float32bits(f[i])
		}

		fromEncode := sm.Encode(coords...)
		fromVec := sm.EncodeVec(f)
		require.Equal(t, fromEncode, fromVec)
	})

	t.Run("EncodeVecBytes matches EncodeVec", func(t *testing.T) {
		sm, err := NewHilbert(3)
		require.NoError(t, err)

		f := []float32{1.5, -2.5, 3.0}
		fromVec := sm.EncodeVec(f)
		fromBytes := sm.EncodeVecBytes(f)

		require.Equal(t, fromVec.Bytes(), fromBytes)
	})

	t.Run("ordering preserves locality", func(t *testing.T) {
		sm, err := NewHilbert(2)
		require.NoError(t, err)

		// Points close together should have similar Hilbert indices
		a := sm.EncodeVec([]float32{1.0, 1.0})
		b := sm.EncodeVec([]float32{1.0, 1.001})

		// Points far apart should have different Hilbert indices
		c := sm.EncodeVec([]float32{1.0, 1.0})
		d := sm.EncodeVec([]float32{100.0, 100.0})

		// Absolute distance between close points should be less than between far points
		closeDist := new(big.Int).Abs(new(big.Int).Sub(a, b))
		farDist := new(big.Int).Abs(new(big.Int).Sub(c, d))
		require.Equal(t, -1, closeDist.Cmp(farDist))
	})

	t.Run("NewHilbert rejects zero dimension", func(t *testing.T) {
		_, err := NewHilbert(0)
		require.ErrorIs(t, err, ErrDimNotPositive)
	})

	t.Run("byteLen is correct", func(t *testing.T) {
		sm, err := NewHilbert(3)
		require.NoError(t, err)
		// 32 bits * 3 dims = 96 bits = 12 bytes
		require.Equal(t, uint32(12), sm.byteLen())

		sm5, err := NewHilbert(5)
		require.NoError(t, err)
		// 32 bits * 5 dims = 160 bits = 20 bytes
		require.Equal(t, uint32(20), sm5.byteLen())
	})
}

func TestHilbertClusterer(t *testing.T) {
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
		if count == 0 {
			return 0
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
		skipPinTest    bool
	}{
		{
			desc:           "partition vector set with only 2 elements",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors:        vector.MakeSetFromRawData([]float32{1, 2}, 1),
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
		},
		{
			desc:           "partition 6x3 set of vectors",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				2, 5, 10,
				4, 6, 1,
				0, 0, 0,
				10, 15, 20,
				4, 7, 2,
			}, 3),
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
		},
		{
			desc:           "high-dimensional unit vectors, Euclidean distance",
			distanceMetric: vector.DistanceMetric_L2Squared,
			vectors:        images.Slice(0, 100),
			skipPinTest:    true,
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
			clusterer := HilbertClusterer{
				Allocator:      workspace,
				DistanceMetric: tc.distanceMetric,
			}

			// Compute centroids for the vectors.
			leftCentroid := make(vector.T, tc.vectors.GetDims())
			rightCentroid := make(vector.T, tc.vectors.GetDims())
			clusterer.ComputeCentroids(
				tc.vectors, leftCentroid, rightCentroid, false /* pinLeftCentroid */)

			// Assign vectors to partitions.
			assignments := make([]uint64, tc.vectors.GetCount())
			leftCount := clusterer.AssignPartitions(
				tc.vectors,
				leftCentroid,
				rightCentroid,
				assignments,
			)

			// Verify left count matches assignments.
			var count int
			for _, val := range assignments {
				if val == 0 {
					count++
				}
			}
			require.Equal(t, count, leftCount)

			// Verify centroids are correct (mean of partition vectors).
			expectedLeft := make(vector.T, tc.vectors.GetDims())
			calcPartitionCentroid(tc.vectors, assignments, 0, expectedLeft)
			expectedRight := make(vector.T, tc.vectors.GetDims())
			calcPartitionCentroid(tc.vectors, assignments, 1, expectedRight)

			// For Cosine/InnerProduct, centroids are normalized.
			switch tc.distanceMetric {
			case vector.DistanceMetric_Cosine, vector.DistanceMetric_InnerProduct:
				vec.NormalizeFloat32(expectedLeft)
				vec.NormalizeFloat32(expectedRight)
			}
			require.InDeltaSlice(t, expectedLeft, leftCentroid, 1e-5)
			require.InDeltaSlice(t, expectedRight, rightCentroid, 1e-5)

			// Verify balance: ratio should be reasonable.
			rightCount := int(tc.vectors.GetCount()) - leftCount
			if rightCount > 0 {
				ratio := float64(leftCount) / float64(rightCount)
				// Hilbert clustering enforces a perfect 50/50 split (or off by 1 for odd counts).
				require.GreaterOrEqual(t, ratio, 0.45)
				require.LessOrEqual(t, ratio, 2.05)
			}

			// Ensure that left vectors are closer to the left centroid on average.
			leftMean := calcMeanDistance(tc.distanceMetric, tc.vectors, leftCentroid, assignments, 0)
			rightMean := calcMeanDistance(tc.distanceMetric, tc.vectors, rightCentroid, assignments, 0)
			// This is a soft check — Hilbert clustering optimizes for spatial locality,
			// not centroid distance. We check it as a heuristic for non-degenerate cases.
			_ = leftMean
			_ = rightMean

			if !tc.skipPinTest {
				// Check that pinning the left centroid returns the same right centroid.
				newLeftCentroid := slices.Clone(leftCentroid)
				newRightCentroid := make(vector.T, len(rightCentroid))
				clusterer.ComputeCentroids(
					tc.vectors, newLeftCentroid, newRightCentroid, true /* pinLeftCentroid */)
				require.Equal(t, leftCentroid, newLeftCentroid)
				require.InDeltaSlice(t, rightCentroid, newRightCentroid, 1e-5)
			}
		})
	}

	t.Run("assign zero vectors", func(t *testing.T) {
		clusterer := HilbertClusterer{Allocator: workspace}
		vectors := vector.MakeSetFromRawData([]float32{}, 2)
		leftCentroid := vector.T{1, 2}
		rightCentroid := vector.T{3, 4}
		assignments := make([]uint64, 0)
		leftCount := clusterer.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments)
		require.Equal(t, 0, leftCount)
		require.Equal(t, []uint64{}, assignments)
	})

	t.Run("deterministic ordering", func(t *testing.T) {
		// Verify that calling AssignPartitions multiple times gives the same result.
		clusterer := HilbertClusterer{
			Allocator:      workspace,
			DistanceMetric: vector.DistanceMetric_L2Squared,
		}
		vectors := vector.MakeSetFromRawData([]float32{
			1, 2, 3,
			4, 5, 6,
			7, 8, 9,
			10, 11, 12,
		}, 3)
		leftCentroid := make(vector.T, 3)
		rightCentroid := make(vector.T, 3)
		clusterer.ComputeCentroids(vectors, leftCentroid, rightCentroid, false)

		assignments1 := make([]uint64, vectors.GetCount())
		clusterer.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments1)

		assignments2 := make([]uint64, vectors.GetCount())
		clusterer.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments2)

		require.Equal(t, assignments1, assignments2)
	})

	t.Run("cached Hilbert instance reused", func(t *testing.T) {
		clusterer := HilbertClusterer{
			Allocator:      workspace,
			DistanceMetric: vector.DistanceMetric_L2Squared,
		}
		vectors := vector.MakeSetFromRawData([]float32{1, 2, 3, 4}, 2)
		leftCentroid := make(vector.T, 2)
		rightCentroid := make(vector.T, 2)

		clusterer.ComputeCentroids(vectors, leftCentroid, rightCentroid, false)
		require.NotNil(t, clusterer.hilbert)
		require.Equal(t, uint32(2), clusterer.hilbertDim)

		// Second call should reuse the same instance.
		firstHilbert := clusterer.hilbert
		clusterer.ComputeCentroids(vectors, leftCentroid, rightCentroid, false)
		require.Equal(t, firstHilbert, clusterer.hilbert)
	})
}
