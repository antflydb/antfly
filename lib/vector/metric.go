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
	"fmt"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
)

// MeasureDistance between two vectors.
//
// For example, for the vectors [1,2] and [4,3]:
//
//	L2Squared: (3-1)^2 + (3-2)^2 = 10
//	InnerProduct: -(1*4 + 2*3) = -10
//
// For Cosine the vectors must already be normalized:
//
//	Cosine: 1 - (1/sqrt(5) * 4/5 + 2/sqrt(5) * 3/5) = 0.1056
func MeasureDistance(metric DistanceMetric, vec1, vec2 T) float32 {
	switch metric {
	case DistanceMetric_L2Squared:
		return vec.L2SquaredDistanceFloat32(vec1, vec2)

	case DistanceMetric_InnerProduct:
		// Calculate the distance by negating the inner product, thus more
		// similar vectors have lower distance.
		//
		// NOTE: inner product "distance" can be negative.
		return -vec.DotFloat32(vec1, vec2)

	case DistanceMetric_Cosine:
		// Assuming normalized inputs so that cosine similarity is equal
		// to the inner product, and cosine distance = 1 - cosine similarity.
		//
		// NOTE: Both vectors must be normalized for this to work correctly.
		// Otherwise, the result will be undefined.
		return 1 - vec.DotFloat32(vec1, vec2)
	}

	panic(fmt.Errorf("unknown distance function %d", metric))
}
