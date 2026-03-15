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

package stats

import (
	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
)

// MeanOfVariances computes the mean of variances of the input vectors across each dimension.
// Calculated using the same algorithm as the gonum stats package:
//
// "Algorithms for computing the sample variance: Analysis and recommendations",
// by Chan, Tony F., Gene H. Golub, and Randall J. LeVeque.
// URL: https://engineering.yale.edu/application/files/3817/3714/7395/tr222.pdf
//
// See formula 1.7:
//
//	for i = 1 to N:
//	 S += (x[i] - mean(x))**2  // First term (2-pass variance from figure 1.1a)
//	 V += x[i] - mean(x)
//	S -= 1/N * V**2 // Second term (error correction for floating-point precision loss)
func MeanOfVariances(allocator allocator.Allocator, vectors *vector.Set) float32 {
	tempVectorSet := allocator.AllocVectorSet(4, int(vectors.GetDims()))
	defer allocator.FreeVectorSet(tempVectorSet)

	// Start with the mean of the vectors.
	tempMean := tempVectorSet.At(0)
	vectors.Centroid(tempMean)

	tempVariance := tempVectorSet.At(1)
	clear(tempVariance)

	tempDiff := tempVectorSet.At(2)
	tempCompensation := tempVectorSet.At(3)
	clear(tempCompensation)

	// Compute the first term and part of second term.
	for i := range vectors.GetCount() {
		// Shared: x[i]
		vector := vectors.At(int(i))
		// Shared: x[i] - mean(x)
		vec.SubToFloat32(tempDiff, vector, tempMean)
		// Second: V += x[i] - mean(x)
		vec.AddFloat32(tempCompensation, tempDiff)
		// First: (x[i] - mean(x))**2
		vec.MulFloat32(tempDiff, tempDiff)
		// First: S += (x[i] - mean(x))**2
		vec.AddFloat32(tempVariance, tempDiff)
	}

	// Finish variance computation.
	// Second: V**2
	vec.MulFloat32(tempCompensation, tempCompensation)
	// Second: 1/N * V**2
	vec.ScaleFloat32(1/float32(vectors.GetCount()), tempCompensation)
	// S = First - Second
	vec.SubFloat32(tempVariance, tempCompensation)

	// Variance = S / (N-1)
	vec.ScaleFloat32(1/float32(vectors.GetCount()-1), tempVariance)

	// Calculate the mean of the variance elements.
	return vec.SumFloat32(tempVariance) / float32(vectors.GetDims())
}
