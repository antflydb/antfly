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
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
)

var _ Quantizer = (*NonQuantizer)(nil)

// NonQuantizer implements the Quantizer interface, storing the
// original vectors unmodified (useful for the root node of our vector index).
//
// All methods in NonQuantizer are thread-safe.
type NonQuantizer struct {
	// dims is the dimensionality of vectors expected by the NonQuantizer.
	dims int
	// distanceMetric determines which distance function to use.
	distanceMetric vector.DistanceMetric
}

// NewNonQuantizer returns a new instance of the NonQuantizer that stores vectors
// with the given number of dimensions and distance metric.
func NewNonQuantizer(dims int, distanceMetric vector.DistanceMetric) Quantizer {
	return &NonQuantizer{dims: dims, distanceMetric: distanceMetric}
}

// GetDims implements the Quantizer interface.
func (q *NonQuantizer) GetDims() int {
	return q.dims
}

// GetDistanceMetric implements the Quantizer interface.
func (q *NonQuantizer) GetDistanceMetric() vector.DistanceMetric {
	return q.distanceMetric
}

// Quantize implements the Quantizer interface.
func (q *NonQuantizer) Quantize(
	w allocator.Allocator,
	centroid vector.T,
	vectors *vector.Set,
) QuantizedVectorSet {
	nonquantizedSet := &NonQuantizedVectorSet{}
	nonquantizedSet.SetVectors(vector.MakeSet(q.dims))
	q.QuantizeWithSet(w, nonquantizedSet, vectors)
	return nonquantizedSet
}

// QuantizeWithSet implements the Quantizer interface.
func (q *NonQuantizer) QuantizeWithSet(
	w allocator.Allocator, quantizedSet QuantizedVectorSet, vectors *vector.Set,
) {
	if q.distanceMetric == vector.DistanceMetric_Cosine {
		vector.ValidateUnitVectorSet(vectors)
	}
	nonquantizedSet := quantizedSet.(*NonQuantizedVectorSet)
	nonquantizedSet.AddSet(vectors)
}

// NewSet implements the Quantizer interface
func (q *NonQuantizer) NewSet(capacity int, centroid vector.T) QuantizedVectorSet {
	return NonQuantizedVectorSet_builder{
		Vectors: vector.MakeSetFromRawData(
			make([]float32, 0, capacity*q.GetDims()),
			q.GetDims(),
		),
	}.Build()
}

// EstimateDistances implements the Quantizer interface.
func (q *NonQuantizer) EstimateDistances(
	w allocator.Allocator,
	quantizedSet QuantizedVectorSet,
	queryVector vector.T,
	distances []float32,
	errorBounds []float32,
) {
	if q.distanceMetric == vector.DistanceMetric_Cosine {
		vector.ValidateUnitVector(queryVector)
	}

	nonquantizedSet := quantizedSet.(*NonQuantizedVectorSet)

	for i := range nonquantizedSet.GetVectors().GetCount() {
		dataVector := nonquantizedSet.GetVectors().At(int(i))
		distances[i] = vector.MeasureDistance(q.distanceMetric, queryVector, dataVector)
	}

	// Distances are exact, so error bounds are always zero.
	clear(errorBounds)
}
