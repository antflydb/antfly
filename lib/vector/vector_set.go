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

//go:generate protoc --go_out=. --go_opt=paths=source_relative vector.proto
package vector

import (
	"fmt"
	"slices"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/utils"
)

// MakeSet constructs a new empty vector set with the given number of
// dimensions. New vectors can be added via the Add or AddSet methods.
func MakeSet(dims int) *Set {
	return Set_builder{Dims: int64(dims)}.Build()
}

// MakeSetFromRawData constructs a new vector set from a raw slice of vectors.
// The vectors in the slice have the given number of dimensions and are laid out
// contiguously in row-wise order.
//
// NOTE: The data slice is used directly rather than copied; do not use it outside
// the context of this vector set after this point.
func MakeSetFromRawData(data []float32, dims int) *Set {
	if len(data)%dims != 0 {
		panic(fmt.Errorf(
			"data length %d is not a multiple of %d dimensions", len(data), dims))
	}
	return Set_builder{
		Dims:  int64(dims),
		Count: int64(len(data) / dims),
		Data:  data,
	}.Build()
}

// Clone performs a deep copy of the vector set. Changes to the original or
// clone will not affect the other.
func (vs *Set) Clone() *Set {
	return Set_builder{
		Dims:  vs.GetDims(),
		Count: vs.GetCount(),
		Data:  slices.Clone(vs.GetData()),
	}.Build()
}

// At returns the vector at the given offset in the set. The returned vector is
// intended for transient use, since mutations to the vector set can invalidate
// the reference.
func (vs *Set) At(offset int) T {
	start := offset * int(vs.GetDims())
	end := start + int(vs.GetDims())
	return vs.GetData()[start:end:end]
}

// Slice returns a vector set that contains a subset of "count" vectors,
// starting at the given offset.
//
// NOTE: Slice returns a set that references the same memory as this set.
// Modifications to one set may be visible in the other set, so callers should
// typically ensure that both sets are immutable after calling Slice.
func (vs *Set) Slice(offset, count int) *Set {
	if offset > int(vs.GetCount()) {
		panic(fmt.Errorf(
			"slice start %d cannot be greater than set size %d", offset, vs.GetCount()))
	}
	if offset+count > int(vs.GetCount()) {
		panic(fmt.Errorf(
			"slice end %d cannot be greater than set size %d", offset+count, vs.GetCount()))
	}

	start := offset * int(vs.GetDims())
	end := (offset + count) * int(vs.GetDims())
	return Set_builder{
		Dims:  vs.GetDims(),
		Count: int64(count),
		Data:  vs.GetData()[start:end:end],
	}.Build()
}

func checkDims(vs *Set, dims int) {
	if vs.GetDims() != int64(dims) {
		panic(fmt.Errorf(
			"cannot add vectors with %d dimensions to a set with %d dimensions",
			dims,
			vs.GetDims(),
		))
	}
}

// Add appends a new vector to the set.
func (vs *Set) Add(v T) {
	checkDims(vs, len(v))
	vs.SetData(append(vs.GetData(), v...))
	vs.SetCount(vs.GetCount() + 1)
}

// AddSet appends all vectors from the given set to this set.
func (vs *Set) AddSet(vectors *Set) {
	checkDims(vs, int(vectors.GetDims()))
	vs.SetData(append(vs.GetData(), vectors.GetData()...))
	vs.SetCount(vs.GetCount() + vectors.GetCount())
}

// AddUndefined adds the given number of vectors to this set. The vectors should
// be set to defined values before use.
func (vs *Set) AddUndefined(count int) {
	preCount := int(vs.GetCount())
	vs.SetData(slices.Grow(vs.GetData(), count*int(vs.GetDims())))
	vs.SetCount(vs.GetCount() + int64(count))
	vs.SetData(vs.GetData()[:vs.GetCount()*vs.GetDims()])
	vs.scribble(preCount, int(vs.GetCount()))
}

// Clear empties the set so that it has zero vectors.
func (vs *Set) Clear() {
	vs.scribble(0, int(vs.GetCount()))
	vs.SetData(vs.GetData()[:0])
	vs.SetCount(0)
}

func (vs *Set) scribble(startOffset, endOffset int) {
	if utils.AfdbTestBuild {
		start := startOffset * int(vs.GetDims())
		end := endOffset * int(vs.GetDims())
		for i := start; i < end; i++ {
			vs.GetData()[i] = 0xBADF00D
		}
	}
}

// ReplaceWithLast removes the vector at the given offset from the set,
// replacing it with the last vector in the set. The modified set has one less
// element and the last vector's position changes.
func (vs *Set) ReplaceWithLast(offset int) {
	targetStart := offset * int(vs.GetDims())
	sourceEnd := len(vs.GetData())
	copy(
		vs.GetData()[targetStart:targetStart+int(vs.GetDims())],
		vs.GetData()[sourceEnd-int(vs.GetDims()):sourceEnd],
	)
	if utils.AfdbTestBuild {
		count := int(vs.GetCount())
		vs.scribble(count-1, count)
	}
	vs.SetData(vs.GetData()[:sourceEnd-int(vs.GetDims())])
	vs.SetCount(vs.GetCount() - 1)
}

// EnsureCapacity grows the underlying data slice if needed to ensure the
// requested capacity. This is useful to prevent unnecessary resizing when it's
// known up-front how big the vector set will need to get.
func (vs *Set) EnsureCapacity(capacity int) {
	if vs.GetCount() < int64(capacity) {
		vs.SetData(slices.Grow(vs.GetData(), int((int64(capacity)-vs.GetCount())*vs.GetDims())))
	}
}

// Centroid calculates the mean of each dimension of vectors in the set.
// Results are written the provided `centroid` slice.
//
// NOTE: The slice must be pre-allocated by the caller, with length equal to the
// dimensions of the vectors in the set.
func (vs *Set) Centroid(centroid T) T {
	if int64(len(centroid)) != vs.GetDims() {
		panic(fmt.Errorf(
			"centroid dims %d cannot differ from vector set dims %d", len(centroid), vs.GetDims()))
	}

	if vs.GetCount() == 0 {
		// Return vector of zeros.
		clear(centroid)
		return centroid
	}

	data := vs.GetData()
	copy(centroid, data)
	data = data[vs.GetDims():]
	for len(data) > 0 {
		vec.AddFloat32(centroid, data[:vs.GetDims()])
		data = data[vs.GetDims():]
	}
	vec.ScaleFloat32(1/float32(vs.GetCount()), centroid)
	return centroid
}

// Equal returns true if this set is equal to the other set. Two sets are equal
// if they have the same number of dimensions, the same number of vectors, and
// the same values in the same order.
func (vs *Set) Equal(other *Set) bool {
	if vs.GetDims() != other.GetDims() || vs.GetCount() != other.GetCount() {
		return false
	}
	return slices.Equal(vs.GetData(), other.GetData())
}
