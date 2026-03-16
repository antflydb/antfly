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
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/antflydb/antfly/lib/vector/testutils"
)

func BenchmarkAssignPartitions(b *testing.B) {
	workspace := &allocator.StackAllocator{}

	// Load datasets of different dimensionalities.
	images := testutils.LoadDataset(b, testutils.ImagesDataset)           // 512d, 10k
	fashion := testutils.LoadDataset(b, testutils.FashionMinstDataset10k) // 784d, 10k

	type benchCase struct {
		name    string
		metric  vector.DistanceMetric
		vectors *vector.Set
	}

	cases := []benchCase{
		{"L2/512d", vector.DistanceMetric_L2Squared, images},
		{"L2/784d", vector.DistanceMetric_L2Squared, fashion},
		{"IP/512d", vector.DistanceMetric_InnerProduct, images},
		{"Cosine/512d", vector.DistanceMetric_Cosine, images},
	}

	counts := []int{100, 1000, 10000}

	for _, tc := range cases {
		for _, n := range counts {
			if int64(n) > tc.vectors.GetCount() {
				continue
			}
			vecs := tc.vectors.Slice(0, n)
			b.Run(fmt.Sprintf("%s/n=%d", tc.name, n), func(b *testing.B) {
				kmeans := BalancedBinaryKmeans{
					Allocator:      workspace,
					Rand:           rand.New(rand.NewPCG(42, 1048)),
					DistanceMetric: tc.metric,
				}

				// Pre-compute centroids so we only benchmark assignment.
				dims := int(vecs.GetDims())
				leftCentroid := make(vector.T, dims)
				rightCentroid := make(vector.T, dims)
				kmeans.ComputeCentroids(vecs, leftCentroid, rightCentroid, false)

				assignments := make([]uint64, n)

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					kmeans.AssignPartitions(vecs, leftCentroid, rightCentroid, assignments)
				}
			})
		}
	}
}

func BenchmarkComputeCentroids(b *testing.B) {
	workspace := &allocator.StackAllocator{}

	images := testutils.LoadDataset(b, testutils.ImagesDataset) // 512d, 10k

	counts := []int{100, 1000, 10000}

	for _, n := range counts {
		if int64(n) > images.GetCount() {
			continue
		}
		vecs := images.Slice(0, n)
		b.Run(fmt.Sprintf("L2/512d/n=%d", n), func(b *testing.B) {
			kmeans := BalancedBinaryKmeans{
				Allocator:      workspace,
				Rand:           rand.New(rand.NewPCG(42, 1048)),
				DistanceMetric: vector.DistanceMetric_L2Squared,
			}

			dims := int(vecs.GetDims())
			leftCentroid := make(vector.T, dims)
			rightCentroid := make(vector.T, dims)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				kmeans.ComputeCentroids(vecs, leftCentroid, rightCentroid, false)
			}
		})
	}
}
