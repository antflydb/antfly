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
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/stat"
)

func TestMeanOfVariances(t *testing.T) {
	testCases := []struct {
		name     string
		vectors  *vector.Set
		expected float32
	}{
		{
			name: "zero variance",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 1, 1,
				1, 1, 1,
				1, 1, 1,
			}, 3),
			expected: 0,
		},
		{
			name: "simple values",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				4, 5, 6,
				7, 8, 9,
			}, 3),
			expected: 9,
		},
		{
			name: "larger set of floating-point values",
			vectors: vector.MakeSetFromRawData([]float32{
				4.2, 5.4, -6.3,
				10.3, -11.0, 12.9,
				1.5, 2.5, 3.5,
				-13.7, 14.8, 15.9,
				-7.9, -8.1, -9.4,
			}, 3),
			expected: 109.3903,
		},
		{
			name: "one-dimensional vectors",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3, 4, 5, 6,
				2, 3, 4, 5, 6, 7,
				3, 4, 5, 6, 7, 8,
			}, 1),
			expected: 3.7941,
		},
		{
			name: "large numbers with small variance",
			vectors: vector.MakeSetFromRawData([]float32{
				1e7 + 1, 1e7 + 2, 1e7 + 3, 1e7 + 4,
			}, 1),
			expected: 1.6667,
		},
	}
	var allocator allocator.StackAllocator
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := MeanOfVariances(&allocator, tc.vectors)
			defer require.True(t, allocator.IsClear())
			require.InDelta(
				t,
				tc.expected,
				result,
				0.0001,
				"Mean of variances does not match expected value",
			)

			// Compare result against calculation performed using gonum.
			variances := make([]float64, tc.vectors.GetDims())
			for dimIdx := range int(tc.vectors.GetDims()) {
				values := make([]float64, tc.vectors.GetCount())
				for vecIdx := range tc.vectors.GetCount() {
					values[vecIdx] = float64(tc.vectors.At(int(vecIdx))[dimIdx])
				}
				_, variances[dimIdx] = stat.MeanVariance(values, nil)
			}

			mean := stat.Mean(variances, nil)
			require.InDelta(
				t,
				mean,
				result,
				0.0001,
				"Mean of variances does not match gonum calculation",
			)
		})
	}
}
