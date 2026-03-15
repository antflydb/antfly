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
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
)

// HilbertClusterer implements the Clusterer interface using Hilbert space-filling
// curves to partition vectors. This approach maps multi-dimensional vectors to a
// one-dimensional Hilbert curve, preserving spatial locality, then splits the
// ordered vectors into two balanced partitions.
type HilbertClusterer struct {
	// Allocator is used to allocate memory (satisfies requirements for using StackAllocator).
	Allocator allocator.Allocator
	// DistanceMetric specifies which distance function to use when clustering
	// vectors. Lower distances indicate greater similarity.
	DistanceMetric vector.DistanceMetric

	// Cached Hilbert curve mapper, reused across calls with the same dimension.
	hilbert    *Hilbert
	hilbertDim uint32
}

// Compile-time check that HilbertClusterer implements Clusterer
var _ Clusterer = (*HilbertClusterer)(nil)

// getHilbert returns a Hilbert curve mapper for the given dimension, caching
// the instance for reuse across calls.
func (hc *HilbertClusterer) getHilbert(dim uint32) *Hilbert {
	if hc.hilbert != nil && hc.hilbertDim == dim {
		return hc.hilbert
	}
	sm, err := NewHilbert(dim)
	if err != nil {
		panic(fmt.Errorf("creating Hilbert curve: %w", err))
	}
	hc.hilbert = sm
	hc.hilbertDim = dim
	return sm
}

// validateVectors ensures the input is valid for clustering.
func (hc *HilbertClusterer) validateVectors(vectors *vector.Set) {
	if vectors.GetCount() < 2 {
		panic(errors.New("Hilbert clustering requires at least 2 vectors"))
	}

	switch hc.DistanceMetric {
	case vector.DistanceMetric_L2Squared, vector.DistanceMetric_InnerProduct:

	case vector.DistanceMetric_Cosine:
		vector.ValidateUnitVectorSet(vectors)

	default:
		panic(fmt.Errorf("%s distance metric is not supported", hc.DistanceMetric))
	}
}

// hilbertAssign computes Hilbert embeddings for all vectors, sorts them by
// Hilbert curve order, and writes partition assignments (0=left, 1=right)
// into the assignments slice. Returns the number of vectors assigned to left.
func (hc *HilbertClusterer) hilbertAssign(
	vectors *vector.Set,
	assignments []uint64,
) int {
	count := vectors.GetCount()
	sm := hc.getHilbert(uint32(vectors.GetDims())) //nolint:gosec // G115: bounded value

	type vectorWithHilbert struct {
		index     int
		embedding []byte
	}

	embeddings := make([]vectorWithHilbert, count)
	for i := range int(count) {
		embeddings[i] = vectorWithHilbert{
			index:     i,
			embedding: HilbertEmbeddingBytes(sm, vectors.At(i)),
		}
	}

	slices.SortFunc(embeddings, func(a, b vectorWithHilbert) int {
		return bytes.Compare(a.embedding, b.embedding)
	})

	splitPoint := count / 2
	leftCount := 0
	for i, vh := range embeddings {
		if int64(i) < splitPoint {
			assignments[vh.index] = 0 // Left partition
			leftCount++
		} else {
			assignments[vh.index] = 1 // Right partition
		}
	}

	return leftCount
}

// ComputeCentroids separates the given set of input vectors into a left and
// right partition using Hilbert curve ordering. It sets the leftCentroid and
// rightCentroid inputs to the centroids of those partitions, respectively.
// If pinLeftCentroid is true, then keep the input value of leftCentroid and only
// compute the value of rightCentroid.
//
// NOTE: The caller is responsible for allocating the input centroids with
// dimensions equal to the dimensions of the input vector set.
func (hc *HilbertClusterer) ComputeCentroids(
	vectors *vector.Set,
	leftCentroid, rightCentroid vector.T,
	pinLeftCentroid bool,
) {
	hc.validateVectors(vectors)

	count := vectors.GetCount()
	tempAssignments := hc.Allocator.AllocUint64s(int(count))
	defer hc.Allocator.FreeUint64s(tempAssignments)

	hc.hilbertAssign(vectors, tempAssignments)

	// Calculate centroids for each partition
	if pinLeftCentroid {
		calcPartitionCentroid(vectors, tempAssignments, 1, rightCentroid)
	} else {
		calcPartitionCentroids(vectors, tempAssignments, leftCentroid, rightCentroid)
	}

	// Normalize centroids for Cosine and InnerProduct distance metrics
	switch hc.DistanceMetric {
	case vector.DistanceMetric_Cosine, vector.DistanceMetric_InnerProduct:
		if !pinLeftCentroid {
			vec.NormalizeFloat32(leftCentroid)
		}
		vec.NormalizeFloat32(rightCentroid)
	}
}

// AssignPartitions assigns the input vectors into either the left or right
// partition based on Hilbert curve ordering. Vectors are mapped to a 1D
// Hilbert curve index, sorted, and split in half to ensure balanced partitions
// while preserving spatial locality.
//
// Each assignment will be set to 0 if the vector gets assigned to the left
// partition, or 1 if assigned to the right partition.
// It returns the number of vectors assigned to the left partition.
func (hc *HilbertClusterer) AssignPartitions(
	vectors *vector.Set,
	leftCentroid, rightCentroid vector.T,
	assignments []uint64,
) int {
	count := vectors.GetCount()
	if len(assignments) != int(count) {
		panic(fmt.Errorf("assignments slice must have length %d, got %d", count, len(assignments)))
	}

	return hc.hilbertAssign(vectors, assignments)
}
