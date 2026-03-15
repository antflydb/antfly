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
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/antflydb/antfly/lib/vector/stats"
	"github.com/chewxy/math32"
)

// A more general K-means algorithm can partition a set of N items into k
// clusters, where k <= N. However, splitting a K-means tree partition only
// requires k = 2, we only handle that case here since the code is simpler and
// faster. This algorithm can be extended using this paper:
//
// "Fast Partitioning with Flexible Balance Constraints" by Hongfu Liu, Ziming
// Huang, et. al.
// URL: https://ieeexplore.ieee.org/document/8621917
//
// Compile-time check that BalancedBinaryKmeans implements Clusterer
var _ Clusterer = (*BalancedBinaryKmeans)(nil)

// tolerableImbalancePercentage constrains how vectors will be assigned
// to partitions by the balanced K-means algorithm. If there are 100 vectors,
// then at least this number of vectors will be assigned to a side.
const tolerableImbalancePercentage = 33

// BalancedBinaryKmeans implements a balanced binary K-Means++ algorithm
// that separates d-dimensional vectors into two partitions. Vectors in each of the
// resulting partition are more similar to their own partition than they are to
// the other partition (i.e. closer to the centroid of each partition). The size
// of each partition is guaranteed to have no less than 1/3rd of the vectors to
// be partitioned.
//
// We seed the initial centroids using the K-means++ algorithm, which selects
// initial centroids that are far apart from each other, according to:
//
// "k-means++: The Advantages of Careful Seeding", by David Arthur and Sergei
// Vassilvitskii
// URL: http://ilpubs.stanford.edu:8090/778/1/2006-13.pdf
//
// For L2Squared distance, vectors are grouped by their distance from mean
// centroids (simple averaging of each dimension). For InnerProduct and Cosine
// distance metrics, vectors are grouped by their distance from spherical
// centroids (mean centroid normalized to unit length). This prevents
// high-magnitude centroids from disproportionately attracting vectors and
// continually growing in magnitude as centroids of centroids are computed.
// FAISS normalizes centroids when using InnerProduct distance for this reason.
//
// NOTE: ComputeCentroids always returns mean centroids, even for InnerProduct
// and Cosine metrics. Spherical centroids can be derived from mean centroids
// by normalization, but the reverse is not possible.
type BalancedBinaryKmeans struct {
	// MaxIterations specifies the maximum number of retries that the algorithm will
	// attempt in order to find locally optimal partitions.
	MaxIterations int
	// Allocator is used to allocate memory (satisfies requirements for using StackAllocator).
	Allocator allocator.Allocator
	// Rand is used to generate random numbers. If this is nil, then the global
	// random number generator is used instead. This is primarily used determinism in testing.
	Rand *rand.Rand
	// DistanceMetric specifies which distance function to use when clustering
	// vectors. Lower distances indicate greater similarity.
	DistanceMetric vector.DistanceMetric
}

// ComputeCentroids separates the given set of input vectors into a left and
// right partition using the K-means++ algorithm. It sets the leftCentroid and
// rightCentroid inputs to the centroids of those partitions, respectively. If
// pinLeftCentroid is true, then keep the input value of leftCentroid and only
// compute the value of rightCentroid.
//
// NOTE: The caller is responsible for allocating the input centroids with
// dimensions equal to the dimensions of the input vector set.
//
// NOTE: For InnerProduct and Cosine distance metrics, clustering uses spherical
// centroids (normalized to unit length), but ComputeCentroids still always
// returns mean centroids. See BalancedBinaryKmeans comment for details.
func (km *BalancedBinaryKmeans) ComputeCentroids(
	vectors *vector.Set, leftCentroid, rightCentroid vector.T, pinLeftCentroid bool,
) {
	km.validateVectors(vectors)

	tempAssignments := km.Allocator.AllocUint64s(int(vectors.GetCount()))
	defer km.Allocator.FreeUint64s(tempAssignments)

	tolerance := km.calculateTolerance(vectors)

	// Pick 2 centroids to start, using the K-means++ algorithm from scikit-learn:
	//
	// https://github.com/scikit-learn/scikit-learn/blob/c5497b7f7/sklearn/cluster/_kmeans.py#L68
	// https://github.com/scikit-learn/scikit-learn/blob/c5497b7f7/sklearn/cluster/_kmeans.py#L225
	//
	// TODO (ajr) FAISS library's implementation of K-means has special cases for empty clusters.
	// We could also consider an outer loop that retries with new random centroids if more that
	// 2/3 of the vectors get assigned to one side, which could have just been unlucky.
	tempLeftCentroid := km.Allocator.AllocVector(int(vectors.GetDims()))
	defer km.Allocator.FreeVector(tempLeftCentroid)
	newLeftCentroid := tempLeftCentroid

	tempRightCentroid := km.Allocator.AllocVector(int(vectors.GetDims()))
	defer km.Allocator.FreeVector(tempRightCentroid)
	newRightCentroid := tempRightCentroid

	if !pinLeftCentroid {
		km.initLeftCentroid(vectors, leftCentroid)
	} else {
		// Point newLeftCentroid at leftCentroid so the convergence check
		// below always computes zero shift for the pinned centroid (it
		// compares leftCentroid to itself).
		newLeftCentroid = leftCentroid
	}
	km.initRightCentroid(vectors, leftCentroid, rightCentroid)

	maxIterations := km.MaxIterations
	if maxIterations == 0 {
		maxIterations = 16
	}

	// Save references to the caller-supplied output buffers so that the
	// final centroids can be copied back after the loop (the pointer-swap
	// scheme may leave the results in temp buffers).
	origLeftCentroid := leftCentroid
	origRightCentroid := rightCentroid

	for range maxIterations {
		// Assign vectors to one of the partitions.
		km.AssignPartitions(vectors, leftCentroid, rightCentroid, tempAssignments)

		// Calculate new centroids.
		if pinLeftCentroid {
			calcPartitionCentroid(vectors, tempAssignments, 1, newRightCentroid)
		} else {
			calcPartitionCentroids(vectors, tempAssignments, newLeftCentroid, newRightCentroid)
		}

		// Swap old and new centroids so that leftCentroid/rightCentroid
		// always hold the most recently computed values after this point.
		newLeftCentroid, leftCentroid = leftCentroid, newLeftCentroid
		newRightCentroid, rightCentroid = rightCentroid, newRightCentroid

		// Check for convergence using the scikit-learn algorithm. After the
		// swap, newLeft/newRight hold the previous iteration's centroids.
		leftCentroidShift := vec.L2SquaredDistanceFloat32(leftCentroid, newLeftCentroid)
		rightCentroidShift := vec.L2SquaredDistanceFloat32(rightCentroid, newRightCentroid)
		if leftCentroidShift+rightCentroidShift <= tolerance {
			break
		}
	}

	// The pointer-swap scheme may leave the final centroids in temp buffers
	// rather than the caller-supplied output buffers. Copy back if needed.
	if !pinLeftCentroid {
		copy(origLeftCentroid, leftCentroid)
	}
	copy(origRightCentroid, rightCentroid)
}

// AssignPartitions assigns the input vectors into either the left or right
// partition, based on which centroid they're closer to. Also enforces a constraint
// that one partition will never be more than 2x as large as the other.
//
// Each assignment will be set to 0 if the vector gets assigned
// to the left partition, or 1 if assigned to the right partition.
// It returns the number of vectors assigned to the left partition for testing.
//
// NOTE: For InnerProduct and Cosine distance metrics, AssignPartitions groups
// vectors by their distance from spherical centroids (i.e. unit centroids). See
// BalancedBinaryKmeans comment.
func (km *BalancedBinaryKmeans) AssignPartitions(
	vectors *vector.Set, leftCentroid, rightCentroid vector.T, assignments []uint64,
) int {
	count := vectors.GetCount()
	if len(assignments) != int(count) {
		panic(fmt.Errorf("assignments slice must have length %d, got %d", count, len(assignments)))
	}

	tempDistances := km.Allocator.AllocFloat32s(int(count))
	defer km.Allocator.FreeFloat32s(tempDistances)

	// For Cosine and InnerProduct distances, compute the norms (magnitudes) of
	// the left and right centroids. Invert the magnitude to avoid division in
	// the loop, as well as to take care of the division-by-zero case up front.
	spherical := km.DistanceMetric == vector.DistanceMetric_Cosine ||
		km.DistanceMetric == vector.DistanceMetric_InnerProduct
	var invLeftNorm, invRightNorm float32
	if spherical {
		invLeftNorm = vec.NormFloat32(leftCentroid)
		if invLeftNorm != 0 {
			invLeftNorm = 1 / invLeftNorm
		}
		invRightNorm = vec.NormFloat32(rightCentroid)
		if invRightNorm != 0 {
			invRightNorm = 1 / invRightNorm
		}
	}

	// Calculate difference between distance of each vector to the left and right
	// centroids.
	var leftCount int
	for i := range count {
		var leftDistance, rightDistance float32
		if spherical {
			// Compute the distance between the input vector and the spherical
			// centroids. Because input vectors are expected to be normalized, Cosine
			// distance reduces to be InnerProduct distance. InnerProduct distance
			// is calculated like this:
			//
			//   sphericalCentroid = centroid / ||centroid||
			//   -(inputVector · sphericalCentroid)
			//
			// That is, we convert each mean centroid to a spherical centroid by
			// normalizing it (dividing by its norm). Then we compute the negative
			// dot product of the spherical centroid with the input vector. However,
			// we can use algebraic equivalencies to change the order of operations
			// to be more efficient:
			//
			//   -(inputVector · centroid) / ||centroid||
			leftDistance = -vec.DotFloat32(vectors.At(int(i)), leftCentroid) * invLeftNorm
			rightDistance = -vec.DotFloat32(vectors.At(int(i)), rightCentroid) * invRightNorm
		} else {
			// For L2Squared, compute Euclidean distance to the mean centroids.
			leftDistance = vec.L2SquaredDistanceFloat32(vectors.At(int(i)), leftCentroid)
			rightDistance = vec.L2SquaredDistanceFloat32(vectors.At(int(i)), rightCentroid)
		}
		tempDistances[i] = leftDistance - rightDistance
		if tempDistances[i] < 0 {
			leftCount++
		}
	}

	// Check imbalance limit, so that at least (tolerableImbalancePercentage / 100)% of the
	// vectors go to each side.
	minCount := (count*tolerableImbalancePercentage + 99) / 100
	if leftCount >= int(minCount) && (count-int64(leftCount)) >= minCount {
		// Set assignments slice.
		for i := range count {
			if tempDistances[i] < 0 {
				assignments[i] = 0 //nolint:gosec // G602: pre-allocated slice with known bounds
			} else {
				assignments[i] = 1 //nolint:gosec // G602: pre-allocated slice with known bounds
			}
		}
		return leftCount
	}

	// Not enough vectors on left or right side, so rebalance them.
	tempOffsets := km.Allocator.AllocUint64s(int(count))
	defer km.Allocator.FreeUint64s(tempOffsets)

	// Arg sort by the distance differences in order of increasing distance to
	// the left centroid, relative to the right centroid. Use a stable sort to
	// ensure that tests are deterministic.
	for i := range count {
		tempOffsets[i] = uint64(i)
	}
	slices.SortStableFunc(tempOffsets, func(i, j uint64) int {
		return cmp.Compare(tempDistances[i], tempDistances[j])
	})

	if leftCount < int(minCount) {
		leftCount = int(minCount)
	} else if int(count)-leftCount < int(minCount) {
		leftCount = int(count - minCount)
	}

	// Set assignments slice.
	for i := range count {
		if i < int64(leftCount) {
			assignments[tempOffsets[i]] = 0
		} else {
			assignments[tempOffsets[i]] = 1
		}
	}

	return leftCount
}

// calculateTolerance computes a threshold distance value used to determine
// when K-means has converged (i.e. once new centroids are less than this
// distance from the old centroids, K-means can terminate).
func (km *BalancedBinaryKmeans) calculateTolerance(vectors *vector.Set) float32 {
	// Use tolerance algorithm from scikit-learn:
	//   tolerance = mean(variances(vectors, axis=0)) * 1e-4
	return stats.MeanOfVariances(km.Allocator, vectors) * 1e-4
}

// initLeftCentroid selects the left centroid randomly from the input
// vector set according to K-means++ and copies the vector into "leftCentroid".
func (km *BalancedBinaryKmeans) initLeftCentroid(vectors *vector.Set, leftCentroid vector.T) {
	// Randomly select the left centroid from the vector set.
	var leftOffset int
	if km.Rand != nil {
		leftOffset = km.Rand.IntN(int(vectors.GetCount()))
	} else {
		leftOffset = rand.IntN(int(vectors.GetCount())) //nolint:gosec // G404: non-security randomness for ML/jitter
	}
	copy(leftCentroid, vectors.At(leftOffset))
}

// initRightCentroid continues the K-means++ seeding started in
// initLeftCentroid by randomly selecting one of the remaining vectors
// with probability proportional to their distance from the left
// centroid. See the K-means++ paper for details:
//
// "k-means++: The Advantages of Careful Seeding", by David Arthur and Sergei
// Vassilvitskii
// URL: http://ilpubs.stanford.edu:8090/778/1/2006-13.pdf
//
// Copies the vector into "rightCentroid".
func (km *BalancedBinaryKmeans) initRightCentroid(
	vectors *vector.Set, leftCentroid, rightCentroid vector.T,
) {
	count := int(vectors.GetCount())
	tempDistances := km.Allocator.AllocFloat32s(count)
	defer km.Allocator.FreeFloat32s(tempDistances)

	// Calculate distance of each vector in the set from the left centroid. Keep
	// track of min distance and sum of distances for calculating probabilities.
	var distanceSum float32
	minDistance := float32(math32.MaxFloat32)
	for i := range count {
		distance := vector.MeasureDistance(km.DistanceMetric, vectors.At(i), leftCentroid)
		if km.DistanceMetric == vector.DistanceMetric_InnerProduct {
			// For inner product, rank vectors by their angular distance from the
			// left centroid, ignoring their magnitudes.
			//
			// NOTE: We don't need to normalize the left centroid because scaling
			// its magnitude just scales distances by the same proportion -
			// probabilities won't change.
			//
			// NOTE: Cosine vectors are assumed to be unit vectors (i.e. they have norm of one),
			// so no need to perform this calculation.
			norm := vec.NormFloat32(vectors.At(i))
			if norm != 0 {
				distance /= norm
			}
		}
		tempDistances[i] = distance
		distanceSum += distance
		minDistance = min(distance, minDistance)
	}
	// Adjust the sum of distances to handle the case where the min distance is
	// not zero (e.g. with Inner Product). For example, if the min distance is
	// -10, then all distances need to be adjusted by +10 so that the min distance
	// becomes 0.
	distanceSum += float32(count) * -minDistance
	if minDistance != 0 {
		vec.AddConstFloat32(-minDistance, tempDistances)
	}

	// Calculate probability of each vector becoming the right centroid, equal
	// to its distance from the left centroid. Further vectors have a higher
	// probability. For Euclidean or Cosine distance, the left centroid has zero
	// distance from itself, and so will never be selected (unless there are
	// duplicates). However there are edge cases where InnerProduct can select
	// the left centroid.
	if distanceSum != 0 {
		vec.ScaleFloat32(1/distanceSum, tempDistances)
	}
	// This is similar to the cumulative probability we use in layer selection in HNSW.
	var cum, rnd float32
	if km.Rand != nil {
		rnd = km.Rand.Float32()
	} else {
		rnd = rand.Float32() //nolint:gosec // G404: non-security randomness for ML/jitter
	}
	rightOffset := len(tempDistances) - 1
	for i := range len(tempDistances) {
		cum += tempDistances[i]
		if rnd < cum {
			rightOffset = i
			break
		}
	}
	copy(rightCentroid, vectors.At(rightOffset))
}

// validateVectors ensures that if the Cosine distance metric is being used,
// that the vectors are unit vectors.
func (km *BalancedBinaryKmeans) validateVectors(vectors *vector.Set) {
	if vectors.GetCount() < 2 {
		panic(errors.New("k-means requires at least 2 vectors"))
	}

	switch km.DistanceMetric {
	case vector.DistanceMetric_L2Squared, vector.DistanceMetric_InnerProduct:

	case vector.DistanceMetric_Cosine:
		vector.ValidateUnitVectorSet(vectors)

	default:
		panic(fmt.Errorf("%s distance metric is not supported", km.DistanceMetric))
	}
}

// calcPartitionCentroids calculates the mean centroids of a set of
// vectors, partitioned by the corresponding assignment values (either 0 or 1).
// For example, if "assignments" is [0, 1, 0, 0, 1], then vectors at positions
// 0, 2, and 3 are averaged into "centroid0" and 1 and 4 into "centroid1". The
// result is written to the provided centroid vector, which the caller is expected to allocate.
func calcPartitionCentroids(
	vectors *vector.Set, assignments []uint64, centroid0 vector.T, centroid1 vector.T,
) {
	var n0, n1 int
	clear(centroid0)
	clear(centroid1)
	for i, val := range assignments {
		if val == 0 {
			vec.AddFloat32(centroid0, vectors.At(i))
			n0++
		} else {
			vec.AddFloat32(centroid1, vectors.At(i))
			n1++
		}
	}

	// Compute the mean vector by scaling the centroid by the inverse of N,
	// where N is the number of input vectors. Skip empty partitions to avoid
	// division by zero (the centroid is already zeroed from the clear above).
	if n0 > 0 {
		vec.ScaleFloat32(1/float32(n0), centroid0)
	}
	if n1 > 0 {
		vec.ScaleFloat32(1/float32(n1), centroid1)
	}
}

// calcPartitionCentroid calculates the mean centroid of a subset of the given
// vectors, which represents the "average" of those vectors. The subset consists
// of vectors with a corresponding assignment value equal to "assignVal". For
// example, if "assignments" is [0, 1, 0, 0, 1] and "assignVal" is 0, then
// vectors at positions 0, 2, and 3 are in the subset. The result is written to
// the provided centroid vector, which the caller is expected to allocate.
func calcPartitionCentroid(
	vectors *vector.Set, assignments []uint64, assignVal uint64, centroid vector.T,
) {
	var n int
	clear(centroid)
	for i, val := range assignments {
		if val != assignVal {
			continue
		}
		vec.AddFloat32(centroid, vectors.At(i))
		n++
	}

	// Compute the mean vector by scaling the centroid by the inverse of N,
	// where N is the number of input vectors. Skip empty partitions to avoid
	// division by zero (the centroid is already zeroed from the clear above).
	if n > 0 {
		vec.ScaleFloat32(1/float32(n), centroid)
	}
}
