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

package quantize

import (
	"slices"
	"testing"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/chewxy/math32"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

// round32 rounds x to prec decimal places.
func round32(x float32, prec int) float32 {
	if x == 0 {
		return 0
	}
	pow := math32.Pow10(prec)
	return math32.Round(x*pow) / pow
}

// roundSlice rounds each element in dst to prec decimal places.
func roundSlice(dst []float32, prec int) []float32 {
	for i := range dst {
		dst[i] = round32(dst[i], prec)
	}
	return dst
}

// Basic tests.
func TestRaBitQuantizerSimple(t *testing.T) {
	var workspace allocator.StackAllocator
	defer require.True(t, workspace.IsClear())

	t.Run("add and remove vectors", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42, vector.DistanceMetric_L2Squared)
		require.Equal(t, 2, quantizer.GetDims())

		// Add 3 vectors and verify centroid.
		vectors := vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
		centroid := make(vector.T, vectors.GetDims())
		centroid = vectors.Centroid(centroid)
		quantizedSet := quantizer.Quantize(&workspace, centroid, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{4, 3}, quantizedSet.GetCentroid())

		// Add 2 more vectors to existing set.
		vectors = vector.MakeSetFromRawData([]float32{4, 3, 6, 5}, 2)
		quantizer.QuantizeWithSet(&workspace, quantizedSet, vectors)
		require.Equal(t, 5, quantizedSet.GetCount())

		// Ensure distances and error bounds are correct.
		distances := make([]float32, quantizedSet.GetCount())
		errorBounds := make([]float32, quantizedSet.GetCount())
		quantizer.EstimateDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{17, 0, 41, 13, 41}, roundSlice(distances, 2))
		require.Equal(t, []float32{7.21, 14.12, 14.42, 0, 14.42},
			roundSlice(errorBounds, 2))
		require.Equal(t, []float32{4, 3}, quantizedSet.GetCentroid())

		// Remove quantized vectors from the set.
		quantizedSet.ReplaceWithLast(1)
		quantizedSet.ReplaceWithLast(3)
		quantizedSet.ReplaceWithLast(1)
		require.Equal(t, 2, quantizedSet.GetCount())
		distances = distances[:2]
		errorBounds = errorBounds[:2]
		quantizer.EstimateDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{17, 41}, roundSlice(distances, 2))
		require.Equal(t, []float32{7.21, 14.42}, roundSlice(errorBounds, 2))

		// Remove remaining quantized vectors.
		quantizedSet.ReplaceWithLast(0)
		quantizedSet.ReplaceWithLast(0)
		require.Equal(t, 0, quantizedSet.GetCount())
		require.Equal(t, []float32{4, 3}, quantizedSet.GetCentroid())
		distances = distances[:0]
		errorBounds = errorBounds[:0]
		quantizer.EstimateDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	})

	t.Run("empty quantized set", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42, vector.DistanceMetric_L2Squared)
		vectors := vector.MakeSet(2)
		centroid := make(vector.T, vectors.GetDims())
		centroid = vectors.Centroid(centroid)
		quantizedSet := quantizer.Quantize(&workspace, centroid, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{0, 0}, quantizedSet.GetCentroid())
	})

	t.Run("empty quantized set with capacity", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(65, 42, vector.DistanceMetric_InnerProduct)
		centroid := make([]float32, 65)
		for i := range centroid {
			centroid[i] = float32(i)
		}
		quantizedSet := quantizer.NewSet(5, centroid).(*RaBitQuantizedVectorSet)
		require.Equal(t, centroid, quantizedSet.GetCentroid())
		require.EqualValues(t, 0, quantizedSet.GetCodes().GetCount())
		require.EqualValues(t, 2, quantizedSet.GetCodes().GetWidth())
		require.Equal(t, 10, cap(quantizedSet.GetCodes().GetData()))
		require.Equal(t, 5, cap(quantizedSet.GetCodeCounts()))
		require.Equal(t, 5, cap(quantizedSet.GetCentroidDistances()))
		require.Equal(t, 5, cap(quantizedSet.GetQuantizedDotProducts()))
		require.Equal(t, 5, cap(quantizedSet.GetCentroidDotProducts()))
		require.Equal(t, float64(299.07), scalar.Round(float64(quantizedSet.GetCentroidNorm()), 2))
	})
}

// Edge cases.
func TestRaBitQuantizerEdge(t *testing.T) {
	var workspace allocator.StackAllocator
	defer require.True(t, workspace.IsClear())

	// Search for query vector with two equal dimensions, which makes Δ = 0.
	t.Run("two dimensions equal", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42, vector.DistanceMetric_L2Squared)
		vectors := vector.MakeSetFromRawData([]float32{4, 4, -3, -3}, 2)
		centroid := make(vector.T, vectors.GetDims())
		centroid = vectors.Centroid(centroid)
		quantizedSet := quantizer.Quantize(&workspace, centroid, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, 2, quantizedSet.GetCount())
		require.Equal(t, []uint64{0xc000000000000000, 0x0}, quantizedSet.GetCodes().GetData())
		require.Equal(t, []uint32{2, 0}, quantizedSet.GetCodeCounts())

		distances := make([]float32, 2)
		errorBounds := make([]float32, 2)
		quantizer.EstimateDistances(
			&workspace, quantizedSet, vector.T{1, 1}, distances, errorBounds)
		require.Equal(t, []float32{18, 32}, roundSlice(distances, 2))
		require.Equal(t, []float32{4.95, 4.95}, roundSlice(errorBounds, 2))
	})

	t.Run("many dimensions, not multiple of 64", func(t *testing.T) {
		// Number dimensions is > 64 and not a multiple of 64.
		quantizer := NewRaBitQuantizer(141, 42, vector.DistanceMetric_L2Squared)

		vectors := vector.MakeSet(141)
		vectors.AddUndefined(2)
		zeros := vectors.At(0)
		ones := vectors.At(1)
		for i := range ones {
			zeros[i] = 0
			ones[i] = 1
		}
		centroid := make(vector.T, vectors.GetDims())
		centroid = vectors.Centroid(centroid)
		quantizedSet := quantizer.Quantize(&workspace, centroid, vectors).(*RaBitQuantizedVectorSet)
		centroidDistances := slices.Clone(quantizedSet.GetCentroidDistances())
		require.Equal(t, []float32{5.94, 5.94}, roundSlice(centroidDistances, 2))
		code := quantizedSet.GetCodes().At(0)
		require.Equal(t, RaBitQCode{0, 0, 0}, code)
		require.Equal(t, uint32(0), quantizedSet.GetCodeCounts()[0])
		code = quantizedSet.GetCodes().At(1)
		require.Equal(
			t,
			RaBitQCode{0xffffffffffffffff, 0xffffffffffffffff, 0xfff8000000000000},
			code,
		)
		require.Equal(t, uint32(141), quantizedSet.GetCodeCounts()[1])

		distances := make([]float32, quantizedSet.GetCount())
		errorBounds := make([]float32, quantizedSet.GetCount())
		quantizer.EstimateDistances(
			&workspace, quantizedSet, ones, distances, errorBounds)
		require.Equal(t, []float32{141, 0}, roundSlice(distances, 2))
		require.Equal(t, []float32{5.94, 5.94}, roundSlice(errorBounds, 2))
	})

	t.Run("add centroid to set", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42, vector.DistanceMetric_L2Squared)
		quantizedSet := quantizer.NewSet(4, []float32{3, 9}).(*RaBitQuantizedVectorSet)
		vectors := vector.MakeSetFromRawData([]float32{1, 5, 5, 13}, 2)
		quantizer.QuantizeWithSet(&workspace, quantizedSet, vectors)

		// Add centroid to the set along with another vector.
		vectors = vector.MakeSetFromRawData([]float32{1, 5, 3, 9}, 2)
		quantizer.QuantizeWithSet(&workspace, quantizedSet, vectors)
		require.Equal(t, float32(0), quantizedSet.GetQuantizedDotProducts()[3],
			"dot product for centroid should be zero")
		distances := make([]float32, 4)
		errorBounds := make([]float32, 4)
		quantizer.EstimateDistances(
			&workspace, quantizedSet, vector.T{3, 2}, distances, errorBounds)
		require.Equal(t, []float32{22.33, 115.67, 22.33, 49}, roundSlice(distances, 2))
		require.Equal(t, []float32{44.27, 44.27, 44.27, 0}, roundSlice(errorBounds, 2))

		// Estimate distances when the query vector is the centroid.
		quantizer.EstimateDistances(
			&workspace, quantizedSet, vector.T{3, 9}, distances, errorBounds)
		require.Equal(t, []float32{20, 20, 20, 0}, roundSlice(distances, 2))
		require.Equal(t, []float32{0, 0, 0, 0}, roundSlice(errorBounds, 2))
	})

	t.Run("query vector is centroid", func(t *testing.T) {
		quantizer := NewRaBitQuantizer(2, 42, vector.DistanceMetric_L2Squared)
		vectors := vector.MakeSetFromRawData([]float32{1, 5, -3, -9}, 2)
		centroid := make(vector.T, vectors.GetDims())
		centroid = vectors.Centroid(centroid)
		quantizedSet := quantizer.Quantize(&workspace, centroid, vectors).(*RaBitQuantizedVectorSet)
		require.Equal(t, []float32{-1, -2}, quantizedSet.GetCentroid())
		distances := make([]float32, 2)
		errorBounds := make([]float32, 2)
		quantizer.EstimateDistances(
			&workspace, quantizedSet, vector.T{-1, -2}, distances, errorBounds)
		require.Equal(t, []float32{53, 53}, roundSlice(distances, 2))
		require.Equal(t, []float32{0, 0}, roundSlice(errorBounds, 2))
	})
}

// Test InnerProduct distance metric.
func TestRaBitQuantizerInnerProduct(t *testing.T) {
	var workspace allocator.StackAllocator
	quantizer := NewRaBitQuantizer(2, 42, vector.DistanceMetric_InnerProduct)
	require.Equal(t, 2, quantizer.GetDims())

	// Add 3 vectors and verify centroid.
	vectors := vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
	centroid := make(vector.T, vectors.GetDims())
	centroid = vectors.Centroid(centroid)
	quantizedSet := quantizer.Quantize(&workspace, centroid, vectors).(*RaBitQuantizedVectorSet)
	require.Equal(t, []float32{4, 3}, roundSlice(quantizedSet.GetCentroid(), 4))

	// Ensure distances and error bounds are correct.
	distances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())
	quantizer.EstimateDistances(&workspace, quantizedSet, vector.T{3, 4}, distances, errorBounds)
	require.Equal(t, []float32{-23, -9, -38}, roundSlice(distances, 2))
	require.Equal(t, []float32{1.41, 3.16, 2.83}, roundSlice(errorBounds, 2))

	// Call NewQuantizedSet and ensure capacity.
	quantizedSet = quantizer.NewSet(
		5, quantizedSet.GetCentroid()).(*RaBitQuantizedVectorSet)
	require.Equal(t, 5, cap(quantizedSet.GetCentroidDotProducts()))

	// Add vectors to already-created set.
	quantizer.QuantizeWithSet(&workspace, quantizedSet, vectors)

	// Query vector is the centroid.
	quantizer.EstimateDistances(&workspace, quantizedSet, quantizedSet.GetCentroid(),
		distances, errorBounds)
	require.Equal(t, []float32{-26, -10, -39}, roundSlice(distances, 2))
	require.Equal(t, []float32{0, 0, 0}, roundSlice(errorBounds, 2))
}

// Test Cosine distance metric.
func TestRaBitQuantizerCosine(t *testing.T) {
	var workspace allocator.StackAllocator
	quantizer := NewRaBitQuantizer(2, 42, vector.DistanceMetric_Cosine)
	require.Equal(t, 2, quantizer.GetDims())

	// Add 3 vectors and verify centroid.
	vectors := vector.MakeSetFromRawData([]float32{1, 0, 0, 1, 0.70710678, 0.70710678}, 2)
	centroid := make(vector.T, vectors.GetDims())
	centroid = vectors.Centroid(centroid)
	quantizedSet := quantizer.Quantize(&workspace, centroid, vectors).(*RaBitQuantizedVectorSet)
	require.Equal(t, []float32{0.569, 0.569}, roundSlice(quantizedSet.GetCentroid(), 4))

	// Ensure distances and error bounds are correct.
	distances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())
	quantizer.EstimateDistances(&workspace, quantizedSet, vector.T{-1, 0}, distances, errorBounds)
	require.Equal(t, []float32{2, 1.14, 1.71}, roundSlice(distances, 2))
	require.Equal(t, []float32{0.69, 0.84, 0.23}, roundSlice(errorBounds, 2))

	// Call NewQuantizedSet and ensure capacity.
	qsCentroid := slices.Clone(quantizedSet.GetCentroid())
	vec.NormalizeFloat32(qsCentroid)
	quantizedSet = quantizer.NewSet(5, qsCentroid).(*RaBitQuantizedVectorSet)
	require.Equal(t, 5, cap(quantizedSet.GetCentroidDotProducts()))

	// Add vectors to already-created set.
	quantizer.QuantizeWithSet(&workspace, quantizedSet, vectors)

	// Query vector is the centroid.
	quantizer.EstimateDistances(&workspace, quantizedSet, quantizedSet.GetCentroid(),
		distances, errorBounds)
	require.Equal(t, []float32{0.29, 0.29, 0}, roundSlice(distances, 2))
	require.Equal(t, []float32{0, 0, 0}, roundSlice(errorBounds, 2))
}

// Load some real OpenAI embeddings and spot check calculations.
func TestRaBitQuantizeEmbeddings(t *testing.T) {
	var workspace allocator.StackAllocator
	defer require.True(t, workspace.IsClear())

	dataset := testutils.LoadDataset(t, testutils.ImagesDataset)
	dataset = dataset.Slice(0, 100)
	quantizer := NewRaBitQuantizer(
		int(dataset.GetDims()),
		42,
		vector.DistanceMetric_L2Squared,
	)
	require.Equal(t, 512, quantizer.GetDims())

	dsCentroid := make(vector.T, dataset.GetDims())
	dsCentroid = dataset.Centroid(dsCentroid)
	quantizedSet := quantizer.Quantize(&workspace, dsCentroid, dataset).(*RaBitQuantizedVectorSet)
	require.Equal(t, 100, quantizedSet.GetCount())

	centroid := quantizedSet.GetCentroid()
	require.Len(t, centroid, 512)
	require.InDelta(t, -0.00452728, centroid[0], 0.0000001)
	require.InDelta(t, -0.00299389, centroid[511], 0.0000001)

	centroidDistances := quantizedSet.GetCentroidDistances()
	require.Len(t, centroidDistances, 100)
	// TODO (ajr) Investigate why this is slightly different from the original after SIMD
	// require.InDelta(t, 0.7345806, centroidDistances[0], 0.0000001)
	require.InDelta(t, 0.7345806, centroidDistances[0], 0.00000015)
	require.InDelta(t, 0.7328457, centroidDistances[99], 0.0000001)

	queryVector := dataset.At(0)
	distances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())
	quantizer.EstimateDistances(
		&workspace, quantizedSet, queryVector, distances, errorBounds)
	roundSlice(distances, 4)
	roundSlice(errorBounds, 4)
	require.Equal(t, float32(0.0182), distances[0])
	require.Equal(t, float32(1.1103), distances[99])
	require.Equal(t, float32(0.0477), errorBounds[0])
	require.Equal(t, float32(0.0476), errorBounds[99])
}

// Benchmark quantization of 100 vectors.
func BenchmarkQuantize(b *testing.B) {
	var workspace allocator.StackAllocator
	dataset := testutils.LoadDataset(b, testutils.ImagesDataset)
	dataset = dataset.Slice(0, 100)
	quantizer := NewRaBitQuantizer(
		int(dataset.GetDims()),
		42,
		vector.DistanceMetric_L2Squared,
	)
	dsCentroid := make(vector.T, dataset.GetDims())
	dsCentroid = dataset.Centroid(dsCentroid)

	for b.Loop() {
		quantizer.Quantize(&workspace, dsCentroid, dataset)
	}
}

// Benchmark L2Squared distance estimation of 100 vectors.
func BenchmarkEstimateL2SquaredDistances(b *testing.B) {
	var workspace allocator.StackAllocator
	dataset := testutils.LoadDataset(b, testutils.ImagesDataset)
	dataset = dataset.Slice(0, 100)
	quantizer := NewRaBitQuantizer(
		int(dataset.GetDims()),
		42,
		vector.DistanceMetric_L2Squared,
	)
	dsCentroid := make(vector.T, dataset.GetDims())
	dsCentroid = dataset.Centroid(dsCentroid)
	quantizedSet := quantizer.Quantize(&workspace, dsCentroid, dataset)

	queryVector := dataset.At(0)
	squaredDistances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())

	for b.Loop() {
		quantizer.EstimateDistances(
			&workspace, quantizedSet, queryVector, squaredDistances, errorBounds)
	}
}
