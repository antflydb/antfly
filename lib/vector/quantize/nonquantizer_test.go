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
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/stretchr/testify/require"
)

func TestNonQuantizerSimple(t *testing.T) {
	var allocator allocator.StackAllocator
	quantizer := NewNonQuantizer(2, vector.DistanceMetric_L2Squared)
	require.Equal(t, 2, quantizer.GetDims())

	// Quantize empty set
	vectors := vector.MakeSet(2)
	centroid := make(vector.T, vectors.GetDims())
	centroid = vectors.Centroid(centroid)
	quantizedSet := quantizer.Quantize(&allocator, centroid, vectors)
	require.Equal(t, 0, quantizedSet.GetCount())

	// Add 3 vectors and verify centroid and centroid distances
	vectors = vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
	centroid = make(vector.T, vectors.GetDims())
	centroid = vectors.Centroid(centroid)
	quantizedSet = quantizer.Quantize(&allocator, centroid, vectors)

	// Add 2 more vectors to existing set
	vectors = vector.MakeSetFromRawData([]float32{4, 3, 6, 5}, 2)
	quantizer.QuantizeWithSet(&allocator, quantizedSet, vectors)
	require.Equal(t, 5, quantizedSet.GetCount())

	// Ensure distances and error bounds are correct
	distances := make([]float32, quantizedSet.GetCount())
	errorBounds := make([]float32, quantizedSet.GetCount())
	quantizer.EstimateDistances(
		&allocator, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{17, 1, 41, 13, 41}, distances)
	require.Equal(t, []float32{0, 0, 0, 0, 0}, errorBounds)

	// Query vector is centroid
	quantizer.EstimateDistances(
		&allocator, quantizedSet, vector.T{0, 0}, distances, errorBounds)
	require.Equal(t, []float32{29, 5, 61, 25, 61}, distances, "%+v", 2)
	require.Equal(t, []float32{0, 0, 0, 0, 0}, errorBounds, "%+v", 2)

	// Remove quantized vectors
	quantizedSet.ReplaceWithLast(1)
	quantizedSet.ReplaceWithLast(3)
	quantizedSet.ReplaceWithLast(1)
	require.Equal(t, 2, quantizedSet.GetCount())
	distances = distances[:2]
	errorBounds = errorBounds[:2]
	quantizer.EstimateDistances(
		&allocator, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{17, 41}, distances)
	require.Equal(t, []float32{0, 0}, errorBounds)

	// Remove remaining quantized vectors
	quantizedSet.ReplaceWithLast(0)
	quantizedSet.ReplaceWithLast(0)
	require.Equal(t, 0, quantizedSet.GetCount())
	distances = distances[:0]
	errorBounds = errorBounds[:0]
	quantizer.EstimateDistances(
		&allocator, quantizedSet, vector.T{1, 1}, distances, errorBounds)

	// Empty quantized set
	vectors = vector.MakeSet(2)
	centroid = make(vector.T, vectors.GetDims())
	centroid = vectors.Centroid(centroid)
	quantizedSet = quantizer.Quantize(&allocator, centroid, vectors)

	// Add single vector to quantized set
	vectors = vector.T{4, 4}.AsSet()
	quantizer.QuantizeWithSet(&allocator, quantizedSet, vectors)
	require.Equal(t, 1, quantizedSet.GetCount())
	distances = distances[:1]
	errorBounds = errorBounds[:1]
	quantizer.EstimateDistances(
		&allocator, quantizedSet, vector.T{1, 1}, distances, errorBounds)
	require.Equal(t, []float32{18}, distances, "%+v", 2)
	require.Equal(t, []float32{0}, errorBounds, "%+v", 2)

	// InnerProduct distance metric
	quantizer = NewNonQuantizer(2, vector.DistanceMetric_InnerProduct)
	vectors = vector.MakeSetFromRawData([]float32{5, 2, 1, 2, 6, 5}, 2)
	centroid = make(vector.T, vectors.GetDims())
	centroid = vectors.Centroid(centroid)
	quantizedSet = quantizer.Quantize(&allocator, centroid, vectors)

	distances = distances[:3]
	errorBounds = errorBounds[:3]
	quantizer.EstimateDistances(
		&allocator, quantizedSet, vector.T{3, 2}, distances, errorBounds)
	require.Equal(t, []float32{-19, -7, -28}, distances)
	require.Equal(t, []float32{0, 0, 0}, errorBounds)

	// Cosine distance metric
	quantizer = NewNonQuantizer(2, vector.DistanceMetric_Cosine)
	vectors = vector.MakeSetFromRawData([]float32{-1, 0, 0, 1, 0.70710678, 0.70710678}, 2)
	centroid = make(vector.T, vectors.GetDims())
	centroid = vectors.Centroid(centroid)
	quantizedSet = quantizer.Quantize(&allocator, centroid, vectors)

	distances = distances[:3]
	errorBounds = errorBounds[:3]
	quantizer.EstimateDistances(
		&allocator, quantizedSet, vector.T{1, 0}, distances, errorBounds)
	require.Equal(t, []float32{2, 1, 0.29289323}, distances)
	require.Equal(t, []float32{0, 0, 0}, errorBounds)
}
