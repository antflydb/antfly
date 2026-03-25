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

package quantize

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/ajroetker/go-highway/hwy/contrib/rabitq"
	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/chewxy/math32"
)

func allocRaBitQCodes(w allocator.Allocator, count, width int) *RaBitQCodeSet {
	tempUints := w.AllocUint64s(count * width)
	return MakeRaBitQCodeSetFromRawData(tempUints, width)
}

func freeRaBitQCodes(w allocator.Allocator, codeSet *RaBitQCodeSet) {
	w.FreeUint64s(codeSet.GetData())
}

// RaBitQuantizer quantizes vectors into only 1 bit per dimension according to the
// algorithm described here:
//
//	"RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound
//	for Approximate Nearest Neighbor Search" by Jianyang Gao & Cheng Long.
//	URL: https://arxiv.org/pdf/2405.12497
//
// For a human explanation of the algorithm see the ElasticSearch blog post:
//
// https://www.elastic.co/search-labs/blog/rabitq-explainer-101
//
// The RaBitQ quantization method provides good accuracy, compact representation,
// reasonable error bounds, is easy to implement, and can be
// broken up logically to accelerate with SIMD.
//
// All methods in RaBitQuantizer are thread-safe, meaning it can be cached and
// reused across threads quantizing vectors of the same dimensionality.
type RaBitQuantizer struct {
	// dims is the dimensionality of vectors that can be quantized.
	dims int
	// sqrtDims is the precomputed square root of the "dims" field.
	sqrtDims float32
	// sqrtDimsInv precomputes "1 / sqrtDims".
	sqrtDimsInv float32
	// unbias is a precomputed slice of "dims" random values in the [0, 1)
	// interval that's used to remove bias when quantizing query vectors.
	unbias []float32
	// distanceMetric determines which distance function to use.
	distanceMetric vector.DistanceMetric
}

// raBitQuantizedVector adds extra storage space for the special case where the
// vector set has at most one vector. In that case, the vector set slices point
// to the statically-allocated arrays in this struct.
type raBitQuantizedVector struct {
	RaBitQuantizedVectorSet

	codeCountStorage           [1]uint32
	centroidDistanceStorage    [1]float32
	quantizedDotProductStorage [1]float32
	centroidDotProductStorage  [1]float32
}

var _ Quantizer = (*RaBitQuantizer)(nil)

// NewRaBitQuantizer returns a new RaBitQ quantizer that quantizes vectors with
// "dims" dimensions. The "seed" is used to generate the pseudo-random values
// used by the algorithm.
//
// NOTE: Quantizers must be recreated with the same seed to be used
// to search or update any existing previously quantized vector sets.
func NewRaBitQuantizer(dims int, seed uint64, distanceMetric vector.DistanceMetric) Quantizer {
	if dims <= 0 {
		panic(fmt.Errorf("dimensions are not positive: %d", dims))
	}

	rng := rand.New(rand.NewPCG(seed, 1048)) //nolint:gosec // G404: non-security randomness for ML/jitter

	// Create random offsets in range [0, 1) to remove bias when quantizing
	// query vectors.
	unbias := make([]float32, dims)
	for i := range len(unbias) {
		unbias[i] = rng.Float32()
	}

	sqrtDims := math32.Sqrt(float32(dims))
	return &RaBitQuantizer{
		dims:           dims,
		sqrtDims:       sqrtDims,
		sqrtDimsInv:    1 / sqrtDims,
		unbias:         unbias,
		distanceMetric: distanceMetric,
	}
}

// GetDims implements the Quantizer interface.
func (q *RaBitQuantizer) GetDims() int {
	return q.dims
}

// GetDistanceMetric implements the Quantizer interface.
func (q *RaBitQuantizer) GetDistanceMetric() vector.DistanceMetric {
	return q.distanceMetric
}

// Quantize implements the Quantizer interface.
func (q *RaBitQuantizer) Quantize(
	a allocator.Allocator,
	centroid vector.T,
	vectors *vector.Set,
) QuantizedVectorSet {
	quantizedSet := q.NewSet(int(vectors.GetCount()), slices.Clone(centroid))
	q.QuantizeWithSet(a, quantizedSet, vectors)
	return quantizedSet
}

// QuantizeWithSet implements the Quantizer interface.
func (q *RaBitQuantizer) QuantizeWithSet(
	a allocator.Allocator, quantizedSet QuantizedVectorSet, vectors *vector.Set,
) {
	qs := quantizedSet.(*RaBitQuantizedVectorSet)
	if q.distanceMetric == vector.DistanceMetric_Cosine {
		vector.ValidateUnitVectorSet(vectors)
	}
	// Extend any existing slices in the vector set.
	count := vectors.GetCount()
	oldCount := qs.GetCount()
	qs.AddUndefined(int(count))

	// L2SquaredDistance doesn't use centroidDotProducts, so don't store it.
	if q.distanceMetric != vector.DistanceMetric_L2Squared {
		centroidDotProducts := qs.GetCentroidDotProducts()[oldCount:]
		for i := range count {
			centroidDotProducts[i] = vec.DotFloat32(vectors.At(int(i)), qs.GetCentroid())
		}
	}

	// Allocate temp space for vector calculations.
	tempVectors := a.AllocVectorSet(int(count), q.dims)
	defer a.FreeVectorSet(tempVectors)

	// Step 1 (Section 3.1.1): Normalize input vectors relative to the centroid.
	// Paper: o = (o_raw - c) / ||o_raw - c||
	//
	// First compute the difference: o_raw - c
	tempDiffs := tempVectors
	for i := range count {
		vec.SubToFloat32(tempDiffs.At(int(i)), vectors.At(int(i)), qs.GetCentroid())
	}

	// Step 4 (partial): Pre-compute ||o_raw - c|| (centroid distances).
	centroidDistances := qs.GetCentroidDistances()[oldCount:]
	for i := range len(centroidDistances) {
		centroidDistances[i] = vec.NormFloat32(tempDiffs.At(i))
	}

	// Then normalize to get unit vectors: o = (o_raw - c) / ||o_raw - c||
	tempUnitVectors := tempDiffs
	for i := range len(centroidDistances) {
		// If distance to the centroid is zero, then the diff is zero. The unit
		// vector should be zero as well, so no need to do anything in that case.
		centroidDistance := centroidDistances[i]
		if centroidDistance != 0 {
			vec.ScaleFloat32(1.0/centroidDistance, tempUnitVectors.At(i))
		}
	}

	// Step 2 (Section 3.1.2): The paper applies a random orthogonal matrix P
	// to the unit vectors here. In this implementation, the caller applies the
	// transformation to input vectors before quantization, which eliminates P
	// from the formulas and allows different rotation algorithms to be used.

	// Step 3 (Section 3.1.3): Compute quantization codes x_bar_bits for each
	// vector, and step 4 (remainder): pre-compute <o_bar, o> (stored inverted
	// as 1/<o_bar, o> to avoid division during distance estimation).
	dotProducts := qs.GetQuantizedDotProducts()[oldCount:]
	codeCounts := qs.GetCodeCounts()[oldCount:]

	// Use SIMD implementation if available
	codeWidth := int(qs.GetCodes().GetWidth())

	// Prepare flattened unit vectors and codes for SIMD processing
	unitVectorsFlat := tempUnitVectors.GetData()
	codesFlat := qs.GetCodes().GetData()[oldCount*codeWidth:]

	rabitq.QuantizeVectors(
		unitVectorsFlat,
		codesFlat,
		dotProducts,
		codeCounts,
		q.sqrtDimsInv,
		int(count),
		q.dims,
		codeWidth,
	)
}

// NewSet implements the Quantizer interface
func (q *RaBitQuantizer) NewSet(capacity int, centroid vector.T) QuantizedVectorSet {
	var vs *RaBitQuantizedVectorSet

	if capacity <= 1 {
		// Use in-line storage for capacity of zero or one.
		quantized := &raBitQuantizedVector{}
		quantized.SetCodeCounts(quantized.codeCountStorage[:0])
		quantized.SetCentroidDistances(quantized.centroidDistanceStorage[:0])
		quantized.SetQuantizedDotProducts(quantized.quantizedDotProductStorage[:0])

		// L2Squared doesn't use this.
		if q.distanceMetric != vector.DistanceMetric_L2Squared {
			quantized.SetCentroidDotProducts(quantized.centroidDotProductStorage[:0])
		}
		vs = &quantized.RaBitQuantizedVectorSet
	} else {
		vs = RaBitQuantizedVectorSet_builder{
			CodeCounts:           make([]uint32, 0, capacity),
			CentroidDistances:    make([]float32, 0, capacity),
			QuantizedDotProducts: make([]float32, 0, capacity),
		}.Build()
		// L2Squared doesn't use this.
		if q.distanceMetric != vector.DistanceMetric_L2Squared {
			vs.SetCentroidDotProducts(make([]float32, 0, capacity))
		}
	}

	vs.SetMetric(q.distanceMetric)
	vs.SetCentroid(centroid)
	codeWidth := RaBitQCodeSetWidth(q.GetDims())
	dataBuffer := make([]uint64, 0, capacity*codeWidth)
	codes := MakeRaBitQCodeSetFromRawData(dataBuffer, codeWidth)
	vs.SetCodes(codes)
	if q.distanceMetric != vector.DistanceMetric_L2Squared {
		vs.SetCentroidNorm(vec.NormFloat32(centroid))
	}

	return vs
}

// EstimateDistances implements the Quantizer interface.
func (q *RaBitQuantizer) EstimateDistances(
	w allocator.Allocator,
	quantizedSet QuantizedVectorSet,
	queryVector vector.T,
	distances []float32,
	errorBounds []float32,
) {
	if q.distanceMetric == vector.DistanceMetric_Cosine {
		vector.ValidateUnitVector(queryVector)
	}

	raBitSet := quantizedSet.(*RaBitQuantizedVectorSet)
	if raBitSet.GetCount() == 0 {
		return
	}

	// Allocate temp space for calculations.
	tempCodes := allocRaBitQCodes(w, 4, int(raBitSet.GetCodes().GetWidth()))
	defer freeRaBitQCodes(w, tempCodes)
	tempVectors := w.AllocVectorSet(1, q.dims)
	defer w.FreeVectorSet(tempVectors)

	// Normalize the query vector to a unit vector relative to the centroid.
	// Paper (Section 3.3): q = (q_raw - c) / ||q_raw - c||
	tempQueryDiff := tempVectors.At(0)
	vec.SubToFloat32(tempQueryDiff, queryVector, raBitSet.GetCentroid())
	queryCentroidDistance := vec.NormFloat32(tempQueryDiff)

	if queryCentroidDistance == 0 {
		// The query vector is the centroid.
		q.CalcCentroidDistances(quantizedSet, distances, false /* spherical */)
		clear(errorBounds)
		return
	}

	// L2Squared doesn't use these values, so don't compute them in its case.
	var squaredCentroidNorm, queryCentroidDotProduct float32
	if q.distanceMetric != vector.DistanceMetric_L2Squared {
		queryCentroidDotProduct = vec.DotFloat32(queryVector, raBitSet.GetCentroid())
		squaredCentroidNorm = raBitSet.GetCentroidNorm() * raBitSet.GetCentroidNorm()
	}

	tempQueryUnitVector := tempQueryDiff
	vec.ScaleFloat32(1.0/queryCentroidDistance, tempQueryUnitVector)

	// Find min and max values within the vector.
	// Paper: v_left and v_right
	minVal, maxVal := vec.MinMaxFloat32(tempQueryUnitVector)

	// Quantize the query unit vector to 4-bit unsigned ints in [0, 15]
	// (Section 3.3.1, B_q = 4):
	//   delta = (v_right - v_left) / (2^B_q - 1)
	//   q_bar_u[i] = floor((q'[i] - v_left) / delta + u[i])
	const quantizedRange = 15
	delta := (maxVal - minVal) / quantizedRange

	// The full quantized query code is separated into 4 sub-codes.
	// Sub-code j contains the j-th bit of each dimension's quantized value.
	// This enables more efficient computation of the dot product between the
	// quantized query and the quantized data vectors.
	var quantized1, quantized2, quantized3, quantized4 uint64
	var quantizedSum uint64
	tempQueryQuantized1 := tempCodes.At(0)
	tempQueryQuantized2 := tempCodes.At(1)
	tempQueryQuantized3 := tempCodes.At(2)
	tempQueryQuantized4 := tempCodes.At(3)
	for i := range len(tempQueryUnitVector) {
		// If delta == 0, then quantized sub-codes will be set to zero. This
		// only happens when every dimension in the query has the same value.
		if delta != 0 {
			quantized := uint64(
				math32.Floor((tempQueryUnitVector[i]-minVal)/delta + q.unbias[i]),
			)
			// Clamp to valid 4-bit range to prevent bit corruption from
			// floating-point rounding pushing values past the [0, 15] boundary.
			quantized = min(quantized, quantizedRange)
			quantizedSum += quantized
			quantized1 = (quantized1 << 1) | (quantized & 1)
			quantized2 = (quantized2 << 1) | ((quantized & 2) >> 1)
			quantized3 = (quantized3 << 1) | ((quantized & 4) >> 2)
			quantized4 = (quantized4 << 1) | ((quantized & 8) >> 3)
		}

		// Store accumulated sub-codes into the code buffer after every 64
		// dimensions (each uint64 holds 64 bits, one per dimension).
		if (i+1)%64 == 0 {
			offset := i / 64
			tempQueryQuantized1[offset] = quantized1
			tempQueryQuantized2[offset] = quantized2
			tempQueryQuantized3[offset] = quantized3
			tempQueryQuantized4[offset] = quantized4
		}
	}

	// Set any leftover bits.
	if (len(tempQueryUnitVector) % 64) != 0 {
		offset := len(tempQueryUnitVector) / 64
		shift := 64 - (len(tempQueryUnitVector) % 64)
		tempQueryQuantized1[offset] = quantized1 << shift
		tempQueryQuantized2[offset] = quantized2 << shift
		tempQueryQuantized3[offset] = quantized3 << shift
		tempQueryQuantized4[offset] = quantized4 << shift
	}

	count := raBitSet.GetCount()
	for i := range count {
		code := raBitSet.GetCodes().At(i)

		// Compute the multi-bit dot product between data code and query sub-codes:
		//   <x_bar_bits, q_bar_u> = sum_{j=0}^{B_q-1} 2^j * <x_bar_bits, q_bar_u_j>
		bitProduct := rabitq.BitProduct(code,
			tempQueryQuantized1, tempQueryQuantized2, tempQueryQuantized3, tempQueryQuantized4)

		// Compute the estimator (Section 3.3.2).
		//
		// First, estimate <x_bar, q_bar> from the bit-level dot product:
		//   term1 = 2*delta/sqrt(D) * <x_bar_bits, q_bar_u>
		//   term2 = 2*v_left/sqrt(D) * popcount(x_bar_bits)
		//   term3 = delta/sqrt(D) * sum(q_bar_u)
		//   term4 = sqrt(D) * v_left
		//   <x_bar, q_bar> = term1 + term2 - term3 - term4
		//
		// Then approximate <o, q> via the quantized estimator:
		//   <o, q> ~ <o_bar, q> / <o_bar, o>
		//
		// QuantizedDotProducts stores 1/<o_bar, o> (precomputed inverse) to
		// avoid division here and to handle the zero case.
		term1 := 2 * delta * q.sqrtDimsInv * float32(bitProduct)
		term2 := 2 * minVal * q.sqrtDimsInv * float32(raBitSet.GetCodeCounts()[i])
		term3 := delta * q.sqrtDimsInv * float32(quantizedSum)
		term4 := q.sqrtDims * minVal
		estimator := (term1 + term2 - term3 - term4) * raBitSet.GetQuantizedDotProducts()[i]
		dataCentroidDistance := raBitSet.GetCentroidDistances()[i]

		// Compute estimated distances between the raw query and raw data vector.
		// L2Squared uses Equation (2); InnerProduct/Cosine use Equation (8).
		switch q.distanceMetric {
		case vector.DistanceMetric_L2Squared:
			// Paper: ||o_raw - q_raw||^2 = ||o_raw - c||^2 +
			//        ||q_raw - c||^2 - 2 * ||o_raw - c|| * ||q_raw - c|| * <q,o>
			// The formula comes from equation 2 in the paper.
			distance := dataCentroidDistance * dataCentroidDistance
			distance += queryCentroidDistance * queryCentroidDistance
			multiplier := 2 * dataCentroidDistance * queryCentroidDistance
			distance -= multiplier * estimator

			// The RaBitQ estimator has theoretical error bounds of +/- 1/sqrt(D)
			// (Theorem 3.2). Scale by the multiplier to get the error bound for
			// the full distance. Clamp distance >= 0, reducing the error bound
			// by the amount clamped.
			errorBound := multiplier / q.sqrtDims
			if distance < 0 {
				errorBound = max(errorBound+distance, 0)
				distance = 0
			}

			distances[i] = distance
			errorBounds[i] = errorBound

		case vector.DistanceMetric_InnerProduct, vector.DistanceMetric_Cosine:
			// NOTE: Cosine similarity is equal to the inner product of unit vectors
			// (which the caller must guarantee).
			//
			// Equation (8): recover the raw inner product from the unit-vector
			// estimate <o, q> using the identity:
			//   <o_raw, q_raw> = ||o_raw - c|| * ||q_raw - c|| * <o, q>
			//                  + <o_raw, c> + <q_raw, c> - ||c||^2
			multiplier := dataCentroidDistance * queryCentroidDistance
			innerProduct := multiplier*estimator +
				raBitSet.GetCentroidDotProducts()[i] + queryCentroidDotProduct - squaredCentroidNorm

			// Same 1/sqrt(D) error bound as L2Squared, scaled by multiplier.
			errorBound := multiplier / q.sqrtDims

			var distance float32
			if q.distanceMetric == vector.DistanceMetric_InnerProduct {
				// Negate the inner product so that the more similar the vectors,
				// the lower the distance.
				distance = -innerProduct
			} else {
				// Cosine distance is 1 - cosine similarity (which is the inner
				// product for unit vectors). Cap the distance between 0 and 2,
				// adjusting the error bound accordingly.
				distance = 1 - innerProduct
				if distance < 0 {
					errorBound = max(errorBound+distance, 0)
					distance = 0
				} else if distance > 2 {
					errorBound = max(min(errorBound-(distance-2), 2), 0)
					distance = 2
				}
			}

			distances[i] = distance
			errorBounds[i] = errorBound

		default:
			panic(fmt.Errorf(
				"RaBitQuantizer does not support distance metric %s", q.distanceMetric))
		}
	}
}

// CalcCentroidDistances returns the exact distance of each vector in
// "quantizedSet" from that set's centroid, according to the quantizer's
// distance metric (e.g. L2Squared or Cosine). By default, it returns
// distances to the mean centroid. However, if "spherical" is true and the
// distance metric is Cosine or InnerProduct, then it returns distances to
// the spherical centroid instead.
//
// NOTE: The caller is responsible for allocating the "distances" slice with length
// equal to the number of quantized vectors in "quantizedSet". The centroid
// distances will be copied into that slice.
func (q *RaBitQuantizer) CalcCentroidDistances(
	quantizedSet QuantizedVectorSet, distances []float32, spherical bool,
) {
	raBitSet := quantizedSet.(*RaBitQuantizedVectorSet)

	switch q.distanceMetric {
	case vector.DistanceMetric_L2Squared:
		// The distance from the query to the data vectors are just the centroid
		// distances that have already been calculated, but just need to be
		// squared.
		vec.MulToFloat32(distances, raBitSet.GetCentroidDistances(), raBitSet.GetCentroidDistances())

	case vector.DistanceMetric_InnerProduct:
		// Need to negate precomputed centroid dot products to compute inner
		// product distance.
		multiplier := float32(-1)
		if spherical && raBitSet.GetCentroidNorm() != 0 {
			// Convert the mean centroid dot product into a spherical centroid
			// dot product by dividing by the centroid's norm.
			multiplier /= raBitSet.GetCentroidNorm()
		}
		vec.ScaleToFloat32(distances, multiplier, raBitSet.GetCentroidDotProducts())

	case vector.DistanceMetric_Cosine:
		// Cosine distance = 1 - dot product when vectors are normalized. The
		// precomputed centroid dot products were computed with normalized data
		// vectors, but the centroid was not normalized. Do that now by dividing
		// the dot products by the centroid's norm. Also negate the result.
		multiplier := float32(-1)
		if raBitSet.GetCentroidNorm() != 0 {
			multiplier /= raBitSet.GetCentroidNorm()
		}
		vec.ScaleToFloat32(distances, multiplier, raBitSet.GetCentroidDotProducts())
		vec.AddConstFloat32(1, distances)

	default:
		panic(fmt.Errorf(
			"RaBitQuantizer does not support distance metric %s", q.distanceMetric))
	}
}
