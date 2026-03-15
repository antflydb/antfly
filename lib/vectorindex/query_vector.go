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
	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
)

// queryVector manages a query vector, applying randomization and normalization
// as needed, and efficiently calculating exact distances to data vectors.
// Randomization distributes skew more evenly across dimensions, enabling the
// index to work consistently across diverse data sets. Normalization is applied
// when using Cosine distance, which is magnitude-agnostic.
type queryVector struct {
	// distanceMetric specifies the vector similarity function: L2Squared,
	// InnerProduct, or Cosine.
	distanceMetric vector.DistanceMetric
	// original is the original query vector passed to the top-level Index method.
	original vector.T
	// transformed is the query vector after random orthogonal transformation and
	// normalization (for Cosine distance).
	transformed vector.T
}

// InitOriginal sets the original query vector and prepares the comparer for use.
func (c *queryVector) InitOriginal(
	distanceMetric vector.DistanceMetric,
	original vector.T,
	rot *vector.RandomOrthogonalTransformer,
) {
	c.distanceMetric = distanceMetric
	c.original = original

	// Randomize the original query vector.
	c.transformed = make(vector.T, len(original))
	c.transformed = rot.Transform(original, c.transformed)

	// If using cosine distance, also normalize the query vector.
	if c.distanceMetric == vector.DistanceMetric_Cosine {
		vec.NormalizeFloat32(c.transformed)
	}
}

// InitRandomized sets the transformed query vector in cases where the original
// query vector is not available, such as when the vector is an interior
// partition centroid. It is expected to already be randomized and normalized.
func (c *queryVector) InitTransformed(
	distanceMetric vector.DistanceMetric,
	transformed vector.T,
	rot *vector.RandomOrthogonalTransformer,
) {
	c.distanceMetric = distanceMetric
	c.original = nil
	c.transformed = transformed
}

// Transformed returns the query vector after it has been randomized and
// normalized as needed.
func (c *queryVector) Transformed() vector.T {
	return c.transformed
}

// ComputeExactDistances calculates exact distances between the query vector and
// the given search candidates using the configured distance metric. The method
// modifies the candidates slice in-place, setting QueryDistance to the computed
// distance and ErrorBound to 0 (since these are exact calculations). The level
// parameter affects distance computation for certain metrics: InnerProduct
// normalizes vectors only in interior (non-leaf) levels, Cosine applies
// normalization unconditionally to all levels, and L2Squared never applies
// normalization.
//
// NOTE: The Vector field must be populated in each candidate before calling
// this method.
func (c *queryVector) ComputeExactDistances(isLeaf bool, candidates []*Result) {
	normalize := false
	queryVector := c.transformed
	queryNorm := float32(1)
	if isLeaf {
		// Leaf vectors have not been randomized, so compare with the original
		// vector rather than the randomized vector.
		queryVector = c.original

		// If using Cosine distance, then ensure that data vectors are normalized.
		// Also, normalize the original query vector.
		if c.distanceMetric == vector.DistanceMetric_Cosine {
			normalize = true
			queryNorm = vec.NormFloat32(queryVector)
		}
	} else {
		// Interior centroids are already randomized, so compare with the randomized
		// (and normalized) query vector. If using Cosine or InnerProduct distance,
		// then the centroids need to be normalized.
		// NOTE: For InnerProduct, only the data vectors are normalized; the query
		// vector is not normalized (queryNorm = 1). For Cosine, the randomized
		// query vector has already been normalized.
		switch c.distanceMetric {
		case vector.DistanceMetric_Cosine, vector.DistanceMetric_InnerProduct:
			normalize = true
		}
	}

	for i := range candidates {
		candidate := candidates[i]
		if normalize {
			// Compute inner product distance and perform needed normalization.
			candidate.Distance = vector.MeasureDistance(
				vector.DistanceMetric_InnerProduct,
				candidate.Vector,
				queryVector,
			)
			product := queryNorm * vec.NormFloat32(candidate.Vector)
			if product != 0 {
				candidate.Distance /= product
			}
			if c.distanceMetric == vector.DistanceMetric_Cosine {
				// Cosine distance for normalized vectors is 1 - (query ⋅ data).
				// We've computed the negative inner product, so just add one.
				candidate.Distance++
			}
		} else {
			candidate.Distance = vector.MeasureDistance(c.distanceMetric, candidate.Vector, queryVector)
		}
		candidate.ErrorBound = 0
	}
}
