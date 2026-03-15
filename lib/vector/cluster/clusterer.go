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
	"github.com/antflydb/antfly/lib/vector"
)

// Clusterer defines the interface for clustering algorithms that can partition
// a set of vectors into two groups (left and right partitions).
type Clusterer interface {
	// ComputeCentroids separates the given set of input vectors into a left and
	// right partition using the clustering algorithm. It sets the leftCentroid and
	// rightCentroid inputs to the centroids of those partitions, respectively.
	// If pinLeftCentroid is true, then keep the input value of leftCentroid and only
	// compute the value of rightCentroid.
	//
	// NOTE: The caller is responsible for allocating the input centroids with
	// dimensions equal to the dimensions of the input vector set.
	ComputeCentroids(
		vectors *vector.Set,
		leftCentroid, rightCentroid vector.T,
		pinLeftCentroid bool,
	)

	// AssignPartitions assigns the input vectors into either the left or right
	// partition, based on which centroid they're closer to. It may also enforce
	// constraints on partition sizes.
	//
	// Each assignment will be set to 0 if the vector gets assigned to the left
	// partition, or 1 if assigned to the right partition.
	// It returns the number of vectors assigned to the left partition.
	AssignPartitions(
		vectors *vector.Set,
		leftCentroid, rightCentroid vector.T,
		assignments []uint64,
	) int
}
