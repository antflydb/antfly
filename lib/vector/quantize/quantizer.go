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

//go:generate protoc -I=../../.. -I=. --go_out=. --go_opt=paths=source_relative quantize.proto
package quantize

import (
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
)

// Quantizer quantizes a set of vectors to an equal-dimensionality set of
// representative quantized vectors, a compressed version of the original.
// While quantization is lossy and loses information about the original vector,
// the quantized form can be used to estimate the distance between the original vector
// and a query vector.
//
// Quantizer implementations must be thread-safe. There should typically be only
// one Quantizer instance in the process for each index.
//
// NOTE: Because of thread-safety requirements, the Quantizer interface satisfies
// the requirements for the StackAllocator
type Quantizer interface {
	// GetDims specifies the number of dimensions of the vectors that will be
	// quantized.
	GetDims() int

	// GetDistanceMetric specifies the method by which vector similarity is
	// determined, e.g. Euclidean (L2Squared), InnerProduct, or Cosine.
	GetDistanceMetric() vector.DistanceMetric

	// Quantize quantizes a set of input vectors using the given "centroid" and returns the quantized vector set.
	Quantize(a allocator.Allocator, centroid vector.T, vectors *vector.Set) QuantizedVectorSet

	// QuantizeWithSet quantizes a set of input vectors within an existing quantized vector set.
	//
	// NOTE: The set's centroid is not recalculated to reflect the newly added vectors.
	QuantizeWithSet(a allocator.Allocator, quantizedSet QuantizedVectorSet, vectors *vector.Set)

	// NewSet returns a new empty vector set preallocated to "capacity" number of vectors.
	NewSet(capacity int, centroid vector.T) QuantizedVectorSet

	// EstimateDistances returns the estimated distances of the query vector from
	// each data vector represented in the given quantized vector set, as well as
	// the error bounds for those distances. The quantizer has already been
	// initialized with the correct distance function to use for the calculation.
	//
	// NOTE: The caller is responsible for allocating the "distances" and "errorBounds"
	// slices with length equal to the number of quantized vectors in
	// "quantizedSet". EstimateDistances will update the slices with distances and
	// distance error bounds.
	EstimateDistances(
		a allocator.Allocator,
		quantizedSet QuantizedVectorSet,
		queryVector vector.T,
		distances []float32,
		errorBounds []float32,
	)
}

// QuantizedVectorSet is the compressed form of an original set of full-size
// vectors. It also stores a full-size centroid vector for the set, as well as
// the exact distances of the original full-size vectors from that centroid.
type QuantizedVectorSet interface {
	// GetCount returns the number of quantized vectors in the set.
	GetCount() int

	// ReplaceWithLast removes the quantized vector at the given offset from the
	// set, replacing it with the last quantized vector in the set. The modified
	// set has one less element and the last quantized vector's position changes.
	ReplaceWithLast(offset int)

	// Clone makes a deep copy of this quantized vector set. Changes to either
	// the original or clone will not affect the other.
	//
	// NOTE: This can be an expensive operation depending on the size of the set.
	Clone() QuantizedVectorSet

	// Clear removes all the elements of the vector set so that it may be reused.
	// The new centroid replaces the existing centroid.
	//
	// NOTE: Centroids are immutable, so implementations should replace the centroid
	// rather than writing the existing's memory.
	Clear(centroid vector.T)
}
