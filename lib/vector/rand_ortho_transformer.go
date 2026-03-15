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

package vector

import (
	"math"
	"math/rand/v2"

	"github.com/chewxy/math32"
)

// givensRotation represents a 2D Givens rotation to be applied to a pair of
// vector elements. The rotation mixes the elements at offset1 and offset2 using
// the provided cosine and sine values, corresponding to a rotation by some
// angle θ in the (offset1, offset2) plane.
//
// A Givens rotation is an orthogonal transformation, so applying this rotation
// (or more generally, a sequence of rotations) to two vectors preserves the
// angles, inner product, and Euclidean distances between them.
type givensRotation struct {
	// offset1 is the index of the first element to rotate.
	offset1 int
	// offset2 is the index of the second element to rotate.
	offset2 int
	// cos is the cosine of the rotation angle.
	cos float32
	// sin is the sine of the rotation angle.
	sin float32
}

// RandomOrthogonalTransformer applies a random orthogonal transformation (ROT) to
// vectors in order to reduce the impact of skewed data distributions:
//
//  1. Set-level skew: some dimensions may have much higher variance than others
//     across the dataset (e.g., one dimension is nearly constant while another
//     varies widely).
//  2. Vector-level skew: individual vectors may have a few disproportionately
//     large coordinates that dominate the variance.
//
// This transformation redistributes both types of skew across all dimensions.
// This leads to more uniform quantization error, as no single coordinate dominates
// information loss, e.g. the RaBitQ algorithm depends on the statistical properties
// from the transform.
//
// Importantly the orthogonality means they preserve Euclidean distances, dot products,
// and angles — so distance-based comparisons remain valid across transformation.
//
// See the testutils package for example datasets that benefit from ROT.
type RandomOrthogonalTransformer struct {
	// algo is the algorithm used for the orthogonal transformation.
	algo RotAlgorithm
	// dims is the dimensionality of vectors that will be transformed.
	dims int
	// seed is used for pseudo-random number generation, ensuring reproducibility.
	seed uint64
	// rotations is the sequence of Givens rotations to apply when using RotAlgorithm_Givens.
	// Each rotation mixes a pair of coordinates.
	rotations []givensRotation
}

// Init initializes the transformer for the specified algorithm, operating on
// vectors with the given number of dimensions. The same seed must always be
// used for a given vector index, in order to generate the same transforms.
func (t *RandomOrthogonalTransformer) Init(algo RotAlgorithm, dims int, seed uint64) {
	*t = RandomOrthogonalTransformer{
		algo: algo,
		dims: dims,
		seed: seed,
	}

	if algo == RotAlgorithm_None {
		// Nothing to prepare if no rotations will be applied.
		return
	}

	rng := rand.New(rand.NewPCG(seed, 1048)) //nolint:gosec // G404: non-security randomness for ML/jitter

	switch algo {
	case RotAlgorithm_Givens:
		// Prepare NlogN Givens rotations, where each rotation multiplies a random
		// pair of vector coordinates (x and y) by a 2x2 matrix containing sines
		// and cosines of a random angle θ:
		//
		//  |  cosθ  sinθ |   | x |
		//  | -sinθ  cosθ | * | y |
		//
		// Precompute the random angle and sin/cosine values for each of the
		// NlogN Givens rotations that need to be applied to vectors.
		numRotations := int(math.Ceil(float64(dims) * math.Log2(float64(dims))))
		t.rotations = make([]givensRotation, numRotations)
		for rot := range numRotations {
			offset1 := rng.IntN(dims)
			offset2 := rng.IntN(dims - 1)
			if offset2 >= offset1 {
				offset2++
			}
			theta := rng.Float32() * 2 * math.Pi
			cos, sin := math32.Cos(theta), math32.Sin(theta)
			t.rotations[rot] = givensRotation{
				offset1: offset1, offset2: offset2, cos: cos, sin: sin,
			}
		}
	}
}

// Transform performs the random orthogonal transformation (ROT) on the
// "original" vector and writes the result to "transformed". The caller is
// responsible for allocating the transformed vector with length equal to the
// original vector.
func (t *RandomOrthogonalTransformer) Transform(original T, transformed T) T {
	switch t.algo {
	case RotAlgorithm_None:
		// Just copy the original, unchanged vector.
		copy(transformed, original)

	case RotAlgorithm_Givens:
		// Apply NlogN precomputed Givens rotations to the vector.
		copy(transformed, original)
		for i := range t.rotations {
			rot := &t.rotations[i]
			leftVal := transformed[rot.offset1]
			rightVal := transformed[rot.offset2]
			transformed[rot.offset1] = rot.cos*leftVal + rot.sin*rightVal
			transformed[rot.offset2] = -rot.sin*leftVal + rot.cos*rightVal
		}
	}

	return transformed
}

// UnTransformVector inverts the random orthogonal transformation performed by
// Transform, recovering the original vector from its transformed form. The
// caller is responsible for allocating the original vector with length equal
// to the transformed vector.
func (t *RandomOrthogonalTransformer) UnTransformVector(
	transformed T, original T,
) T {
	switch t.algo {
	case RotAlgorithm_None:
		// The transformed vector is the original vector, so simply copy it.
		copy(original, transformed)

	case RotAlgorithm_Givens:
		// Reverse previously applied Givens rotations by flipping the sign of
		// the sinθ and applying the rotations in reverse order.
		//
		// Forward rotation:
		//  |  cosθ  sinθ |
		//  | -sinθ  cosθ |
		//
		// Reverse rotation:
		//  | cosθ  -sinθ |
		//  | sinθ   cosθ |
		copy(original, transformed)
		for i := len(t.rotations) - 1; i >= 0; i-- {
			rot := &t.rotations[i]
			leftVal := original[rot.offset1]
			rightVal := original[rot.offset2]
			original[rot.offset1] = rot.cos*leftVal - rot.sin*rightVal
			original[rot.offset2] = rot.sin*leftVal + rot.cos*rightVal
		}
	}

	return original
}
