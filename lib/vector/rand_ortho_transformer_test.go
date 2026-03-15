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
	"math/rand/v2"
	"testing"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/stat"
)

func TestRandomOrthoTransformer(t *testing.T) {
	type testCase struct {
		name  string
		vec   T
		other T
	}

	testCases := []testCase{
		{
			name:  "simple 1d",
			vec:   T{1},
			other: T{2},
		},
		{
			name:  "simple 2d",
			vec:   T{1, 2},
			other: T{2, 1},
		},
		{
			name:  "negatives 2d",
			vec:   T{-1, 2},
			other: T{2, -1},
		},
		{
			name:  "unit vectors",
			vec:   T{1, 0},
			other: T{0, 1},
		},
		{
			name:  "simple 8d",
			vec:   T{1, 2, 3, 4, 5, 6, 7, 8},
			other: T{8, 7, 6, 5, 4, 3, 2, 1},
		},
	}

	algos := []struct {
		algo RotAlgorithm
		name string
	}{
		{RotAlgorithm_None, "None"},
		{RotAlgorithm_Givens, "Givens"},
	}

	const seed = 42

	for _, tc := range testCases {
		for _, a := range algos {
			t.Run(tc.name+"_"+a.name, func(t *testing.T) {
				dims := len(tc.vec)
				var rot RandomOrthogonalTransformer
				rot.Init(a.algo, dims, seed)

				// Transform the vectors.
				randomized := make(T, dims)
				rot.Transform(tc.vec, randomized)
				randomizedOther := make(T, dims)
				rot.Transform(tc.other, randomizedOther)

				// Norms should be preserved.
				origNorm := vec.NormFloat32(tc.vec)
				randomizedNorm := vec.NormFloat32(randomized)
				require.InDelta(t, origNorm, randomizedNorm, 1e-4, "norm not preserved")

				// Pairwise distance should be preserved.
				origDist := vec.L2SquaredDistanceFloat32(tc.vec, tc.other)
				randomizedDist := vec.L2SquaredDistanceFloat32(randomized, randomizedOther)
				require.InDelta(t, origDist, randomizedDist, 1e-4, "distance not preserved")

				// rotNone should be a no-op.
				if a.algo == RotAlgorithm_None {
					require.Equal(t, tc.vec, randomized, "rotNone should not change the vector")
				}

				// UnRandomizeVector should recover the original vector.
				orig := make(T, dims)
				rot.UnTransformVector(randomized, orig)
				require.InDeltaSlice(t, tc.vec, orig, 1e-4, "inverse did not recover original")
			})
		}
	}
}

func TestRandomOrthoTransformer_SkewedVectors(t *testing.T) {
	const dims = 100
	const seed = 42
	const seed2 = 42
	const numVecs = 1000

	// Use a seeded RNG for reproducibility.
	rng := rand.New(rand.NewPCG(seed, seed2))

	// Generate heavily skewed vectors: all dims random in [-1,1], but first dim
	// is scaled by 1000.
	vectors := make([]T, numVecs)
	for i := range vectors {
		vec := make(T, dims)
		for j := range dims {
			vec[j] = 2*rng.Float32() - 1 // Uniform in [-1, 1]
		}
		vec[0] *= 1000
		vectors[i] = vec
	}

	// Calculate the coefficient of variation (CV) of the variance of each
	// dimension in the data vectors. This is a good measure of how well the
	// random orthogonal transformation spreads the input skew across all
	// dimensions.
	calculateCV := func(algo RotAlgorithm) float64 {
		var rot RandomOrthogonalTransformer
		rot.Init(algo, dims, seed)

		// Transform all vectors.
		transformed := make([]T, numVecs)
		for i, original := range vectors {
			randomized := make(T, dims)
			rot.Transform(original, randomized)
			transformed[i] = randomized
		}

		// Compute variance across each dimension.
		variances := make([]float64, dims)
		scratch := make([]float64, numVecs)
		for i := range variances {
			for j := range transformed {
				scratch[j] = float64(transformed[j][i])
			}
			variances[i] = stat.Variance(scratch, nil /* weights */)
		}

		// Compute the CV, which is stddev divided by mean.
		meanVar := stat.Mean(variances, nil)
		stddevVar := stat.StdDev(variances, nil)

		return stddevVar / meanVar
	}

	// With no rotation, almost all variance is in the first dimension, so CV is
	// very high.
	require.InDelta(t, float32(9.99895), calculateCV(RotAlgorithm_None), 0.0001)

	// With Givens rotations, the variance is spread fairly well, but not as
	// uniformly as with a full matrix. This is a "good enough" reduction at a
	// much lower computational cost (NlogN rather than N^2).
	require.InDelta(t, float32(1.95991), calculateCV(RotAlgorithm_Givens), 0.0001)
}
