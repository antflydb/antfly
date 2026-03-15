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
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
)

// RaBitQCode is a quantization code that partially encodes a quantized vector.
// It has 1 bit per dimension of the quantized vector it represents. For
// example, if the quantized vector has 512 dimensions, then its code will have
// 512 bits that are packed into uint64 values using big-endian ordering (i.e.
// a width of 64 bytes). If the dimensions are not evenly divisible by 64, the
// trailing bits of the code are set to zero.
type RaBitQCode []uint64

// RaBitQCodeSetWidth returns the number of uint64 values needed to store 1 bit
// per dimension for a RaBitQ code.
func RaBitQCodeSetWidth(dims int) int {
	return (dims + 63) / 64
}

// MakeRaBitQCodeSet returns an empty set of quantization codes, where each code
// in the set represents a quantized vector with the given number of dimensions.
func MakeRaBitQCodeSet(dims int) *RaBitQCodeSet {
	return RaBitQCodeSet_builder{
		Count: 0,
		Width: int64(RaBitQCodeSetWidth(dims)),
	}.Build()
}

// MakeRaBitQCodeSetFromRawData constructs a set of quantization codes from a
// raw slice of codes. The raw codes are packed contiguously in memory and
// represent quantized vectors having the given number of dimensions.
//
// NOTE: The data slice is directly used rather than copied; do not use it outside
// the context of this code set after this point.
func MakeRaBitQCodeSetFromRawData(data []uint64, width int) *RaBitQCodeSet {
	if len(data)%width != 0 {
		panic(fmt.Errorf("data length %d is not a multiple of the width %d", len(data), width))
	}
	return RaBitQCodeSet_builder{
		Count: int64(len(data) / width),
		Width: int64(width),
		Data:  data,
	}.Build()
}

// Clone makes a deep copy of the code set. Changes to either the original or
// clone will not affect the other.
func (cs *RaBitQCodeSet) Clone() *RaBitQCodeSet {
	return RaBitQCodeSet_builder{
		Count: cs.GetCount(),
		Width: cs.GetWidth(),
		Data:  slices.Clone(cs.GetData()),
	}.Build()
}

// Clear resets the code set so that it can be reused.
func (cs *RaBitQCodeSet) Clear() {
	cs.scribble(0, int(cs.GetCount()))
	cs.SetCount(0)
	cs.SetData(cs.GetData()[:0])
}

// At returns the code at the given position in the set as a slice of uint64
// values that can be read or written by the caller.
func (cs *RaBitQCodeSet) At(offset int) RaBitQCode {
	start := offset * int(cs.GetWidth())
	return cs.GetData()[start : start+int(cs.GetWidth())]
}

// Add appends the given code to this set.
func (cs *RaBitQCodeSet) Add(code RaBitQCode) {
	if len(code) != int(cs.GetWidth()) {
		panic(
			fmt.Errorf(
				"cannot add code with %d width to set with width %d",
				len(code),
				cs.GetWidth(),
			),
		)
	}
	cs.SetData(append(cs.GetData(), code...))
	cs.SetCount(cs.GetCount() + 1)
}

func (cs *RaBitQCodeSet) scribble(startOffset, endOffset int) {
	if utils.AfdbTestBuild {
		start := startOffset * int(cs.GetWidth())
		end := endOffset * int(cs.GetWidth())
		for i := start; i < end; i++ {
			cs.GetData()[i] = 0xBADF00D
		}
	}
}

// AddUndefined adds the given number of codes to this set. The codes should be
// set to defined values before use.
func (cs *RaBitQCodeSet) AddUndefined(count int) {
	preCount := int(cs.GetCount())
	cs.SetData(slices.Grow(cs.GetData(), count*int(cs.GetWidth())))
	cs.SetCount(cs.GetCount() + int64(count))
	cs.SetData(cs.GetData()[:cs.GetCount()*cs.GetWidth()])
	cs.scribble(preCount, int(cs.GetCount()))
}

// ReplaceWithLast removes the code at the given offset from the set, replacing
// it with the last code in the set. The modified set has one less element and
// the last code's position changes.
func (cs *RaBitQCodeSet) ReplaceWithLast(offset int) {
	width := int(cs.GetWidth())
	targetStart := offset * width
	sourceEnd := len(cs.GetData())
	copy(cs.GetData()[targetStart:targetStart+width], cs.GetData()[sourceEnd-width:sourceEnd])
	cs.SetData(cs.GetData()[:sourceEnd-width])

	cs.SetCount(cs.GetCount() - 1)
}

// GetCount implements the QuantizedVectorSet interface.
func (vs *RaBitQuantizedVectorSet) GetCount() int {
	return len(vs.GetCodeCounts())
}

// ReplaceWithLast implements the QuantizedVectorSet interface.
func (vs *RaBitQuantizedVectorSet) ReplaceWithLast(offset int) {
	vs.GetCodes().ReplaceWithLast(offset)
	vs.SetCodeCounts(utils.ReplaceWithLast(vs.GetCodeCounts(), offset))
	vs.SetCentroidDistances(utils.ReplaceWithLast(vs.GetCentroidDistances(), offset))
	vs.SetQuantizedDotProducts(utils.ReplaceWithLast(vs.GetQuantizedDotProducts(), offset))
	if vs.GetCentroidDotProducts() != nil {
		// This is nil for the L2Squared distance metric.
		vs.SetCentroidDotProducts(utils.ReplaceWithLast(vs.GetCentroidDotProducts(), offset))
	}
}

// Clone implements the QuantizedVectorSet interface.
func (vs *RaBitQuantizedVectorSet) Clone() QuantizedVectorSet {
	codes := vs.GetCodes().Clone()
	return RaBitQuantizedVectorSet_builder{
		Metric:               vs.GetMetric(),
		Centroid:             vs.GetCentroid(), // Centroid is immutable
		Codes:                codes,
		CodeCounts:           slices.Clone(vs.GetCodeCounts()),
		CentroidDistances:    slices.Clone(vs.GetCentroidDistances()),
		QuantizedDotProducts: slices.Clone(vs.GetQuantizedDotProducts()),
		CentroidDotProducts:  slices.Clone(vs.GetCentroidDotProducts()),
		CentroidNorm:         vs.GetCentroidNorm(),
	}.Build()
}

// Clear implements the QuantizedVectorSet interface
func (vs *RaBitQuantizedVectorSet) Clear(centroid vector.T) {
	if utils.AfdbTestBuild {
		if vs.GetCentroid() == nil {
			panic(errors.New("Clear cannot be called on an uninitialized vector set"))
		}
		vs.scribble(0, len(vs.GetCodeCounts()))
	}
	// Recompute the centroid norm for Cosine and InnerProduct metrics, but only
	// if a new centroid is provided.
	if vs.GetMetric() != vector.DistanceMetric_L2Squared {
		if centroid.Compare(vs.GetCentroid()) != 0 {
			vs.SetCentroidNorm(vec.NormFloat32(centroid))
		}
	}

	// vs.Centroid is immutable, so do not try to reuse its memory.
	vs.SetCentroid(centroid)
	vs.GetCodes().Clear()
	vs.SetCodeCounts(vs.GetCodeCounts()[:0])
	vs.SetCentroidDistances(vs.GetCentroidDistances()[:0])
	vs.SetQuantizedDotProducts(vs.GetQuantizedDotProducts()[:0])
	vs.SetCentroidDotProducts(vs.GetCentroidDotProducts()[:0])
}

// AddUndefined adds the given number of quantized vectors to this set. The new
// quantized vector information should be set to defined values before use.
func (vs *RaBitQuantizedVectorSet) AddUndefined(count int) {
	newCount := len(vs.GetCodeCounts()) + count
	vs.GetCodes().AddUndefined(count)
	vs.SetCodeCounts(slices.Grow(vs.GetCodeCounts(), count))
	vs.SetCodeCounts(vs.GetCodeCounts()[:newCount])
	vs.SetCentroidDistances(slices.Grow(vs.GetCentroidDistances(), count))
	vs.SetCentroidDistances(vs.GetCentroidDistances()[:newCount])
	vs.SetQuantizedDotProducts(slices.Grow(vs.GetQuantizedDotProducts(), count))
	vs.SetQuantizedDotProducts(vs.GetQuantizedDotProducts()[:newCount])
	if vs.GetMetric() != vector.DistanceMetric_L2Squared {
		// L2Squared doesn't need this.
		vs.SetCentroidDotProducts(slices.Grow(vs.GetCentroidDotProducts(), count))
		vs.SetCentroidDotProducts(vs.GetCentroidDotProducts()[:newCount])
	}
}

// scribble writes garbage values to undefined vector set values.
// This is used in test builds to make detecting bugs easier.
func (vs *RaBitQuantizedVectorSet) scribble(start, end int) {
	if utils.AfdbTestBuild {
		for i := start; i < end; i++ {
			vs.GetCodeCounts()[i] = 0xBADF00D
		}
		for i := start; i < end; i++ {
			vs.GetCentroidDistances()[i] = math.Pi
		}
		for i := start; i < end; i++ {
			vs.GetQuantizedDotProducts()[i] = math.Pi
		}
		if vs.GetMetric() != vector.DistanceMetric_L2Squared {
			for i := start; i < end; i++ {
				vs.GetCentroidDotProducts()[i] = math.Pi
			}
		}
		// RaBitQCodeSet Clear and AddUndefined methods take care of scribbling
		// memory for vs.Codes.
	}
}
