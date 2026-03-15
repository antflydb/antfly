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
	"errors"
	"math"
	"math/big"
	"sync"

	"github.com/antflydb/antfly/lib/vector"
)

// HilbertEmbeddingBytes returns the Hilbert curve index of a vector as a byte slice.
func HilbertEmbeddingBytes(sm *Hilbert, a []float32) []byte {
	return sm.EncodeVecBytes(a)
}

var ErrDimNotPositive = errors.New("Dimension must be greater than zero")

// Hilbert implements the Hilbert space-filling curve algorithm.
// This algorithm is derived from work done by John Skilling and published
// in "Programming the Hilbert curve". (c) 2004 American Institute of Physics.
// https://doi.org/10.1063/1.1751381
type Hilbert struct {
	bits, dimension, length uint32

	// Buffer pools for performance
	coordsPool sync.Pool // Pool for uint32 slices used in EncodeVec
	bytesPool  sync.Pool // Pool for byte slices used in untranspose
}

func NewHilbert(n uint32) (*Hilbert, error) {
	if n == 0 {
		return nil, ErrDimNotPositive
	}

	b := uint32(32)
	h := &Hilbert{
		bits:      b,
		dimension: n,
		length:    b * n,
	}

	byteLen := (h.length + 7) / 8

	// Initialize buffer pools
	h.coordsPool = sync.Pool{
		New: func() any {
			return make([]uint32, n)
		},
	}

	h.bytesPool = sync.Pool{
		New: func() any {
			return make([]byte, byteLen)
		},
	}

	return h, nil
}

// Dimension returns the number of dimensions.
func (s *Hilbert) Dimension() uint32 {
	return s.dimension
}

// Bits returns the number of bits per dimension.
func (s *Hilbert) Bits() uint32 {
	return s.bits
}

// Len returns the total number of bits (bits * dimension).
func (s *Hilbert) Len() uint32 {
	return s.length
}

// byteLen returns the number of bytes needed to represent the Hilbert index.
func (s *Hilbert) byteLen() uint32 {
	return (s.length + 7) / 8
}

// Encode converts points to its Hilbert curve index.
func (s *Hilbert) Encode(x ...uint32) *big.Int {
	return s.untranspose(s.axesToTranspose(x...))
}

// getCoords retrieves a coords buffer from the pool and fills it with
// the IEEE 754 binary representations of the vector components.
func (s *Hilbert) getCoords(vec vector.T) []uint32 {
	coords := s.coordsPool.Get().([]uint32) //nolint:staticcheck // SA6002
	for i := range vec {
		coords[i] = math.Float32bits(vec[i])
	}
	return coords
}

// putCoords clears and returns a coords buffer to the pool.
func (s *Hilbert) putCoords(coords []uint32) {
	for i := range coords {
		coords[i] = 0
	}
	s.coordsPool.Put(coords) //nolint:staticcheck // SA6002
}

// EncodeVec converts a float32 vector to its Hilbert curve index as a big.Int.
func (s *Hilbert) EncodeVec(vec vector.T) *big.Int {
	coords := s.getCoords(vec)
	defer s.putCoords(coords)
	return s.untranspose(s.axesToTranspose(coords...))
}

// EncodeVecBytes converts a float32 vector to its Hilbert curve index as a byte slice.
func (s *Hilbert) EncodeVecBytes(vec vector.T) []byte {
	coords := s.getCoords(vec)
	defer s.putCoords(coords)
	return s.untransposeBytes(s.axesToTranspose(coords...))
}

// Decode converts an index (distance along the Hilbert Curve from 0)
// to a point of dimensions defined.
func (s *Hilbert) Decode(index *big.Int) []uint32 {
	return s.transposedToAxes(s.transpose(index))
}

// untransposeInto packs the transposed Hilbert index x into the byte buffer t.
// The high-order bit from the last number in x becomes the high-order bit of
// the last byte in t. The low-order bit of the first number becomes the low
// order bit of the first byte in t.
func (s *Hilbert) untransposeInto(x []uint32, t []byte) {
	bIndex := s.length - 1
	mask := uint32(1 << (s.bits - 1))

	for range int(s.bits) {
		for j := range x {
			if (x[j] & mask) != 0 {
				t[s.byteLen()-1-bIndex/8] |= 1 << (bIndex % 8)
			}
			bIndex--
		}
		mask >>= 1
	}
}

// untransposeBytes packs the transposed index into a newly allocated byte slice.
// The caller owns the returned slice.
func (s *Hilbert) untransposeBytes(x []uint32) []byte {
	t := make([]byte, s.byteLen())
	s.untransposeInto(x, t)
	return t
}

// untranspose packs the transposed index into a big.Int using a pooled buffer.
func (s *Hilbert) untranspose(x []uint32) *big.Int {
	t := s.bytesPool.Get().([]byte) //nolint:staticcheck // SA6002
	defer func() {
		for i := range t {
			t[i] = 0
		}
		s.bytesPool.Put(t) //nolint:staticcheck // SA6002
	}()
	s.untransposeInto(x, t)
	return new(big.Int).SetBytes(t)
}

// transpose returns the transposed representation of the Hilbert curve index.
// The Hilbert index is expressed internally as an array of transposed bits.
// Example: 5 bits for each of n=3 coordinates.
//
//	15-bit Hilbert integer = A B C D E F G H I J K L M N O is stored
//	as its Transpose                        ^
//	X[0] = A D G J M                    X[2]|  7
//	X[1] = B E H K N        <------->       | /X[1]
//	X[2] = C F I L O                   axes |/
//	       high low                         0------> X[0]
func (s *Hilbert) transpose(index *big.Int) []uint32 {
	x := make([]uint32, s.dimension)
	b := index.Bytes()

	for idx := range 8 * len(b) {
		if (b[len(b)-1-idx/8] & (1 << (uint32(idx) % 8))) != 0 {
			dim := (s.length - uint32(idx) - 1) % s.dimension
			shift := (uint32(idx) / s.dimension) % s.bits
			x[dim] |= 1 << shift
		}
	}

	return x
}

// transposedToAxes converts the Hilbert transposed index into an N-dimensional
// point expressed as a vector of uint32.
func (s *Hilbert) transposedToAxes(x []uint32) []uint32 {
	N := uint32(2 << (s.bits - 1))
	// Note that x is mutated by this method (as a performance improvement
	// to avoid allocation)
	n := len(x)

	// Gray decode by H ^ (H/2)
	t := x[n-1] >> 1
	// Corrected error in Skilling's paper on the following line. The
	// appendix had i >= 0 leading to negative array index.
	for i := n - 1; i > 0; i-- {
		x[i] ^= x[i-1]
	}

	x[0] ^= t
	// Undo excess work
	for q := uint32(2); q != N; q <<= 1 {
		p := q - 1
		for i := n - 1; i >= 0; i-- {
			if (x[i] & q) != 0 {
				x[0] ^= p // invert
			} else {
				t = (x[0] ^ x[i]) & p
				x[0] ^= t
				x[i] ^= t
			}
		}
	} // exchange
	return x
}

// axesToTranspose converts axes (coordinates) of a point in N-Dimensional space
// to the transposed Hilbert curve index.
func (s *Hilbert) axesToTranspose(x ...uint32) []uint32 {
	M := uint32(1 << (s.bits - 1))
	n := len(x)

	var t uint32
	for q := M; q > 1; q >>= 1 {
		p := q - 1
		for i := range n {
			if (x[i] & q) != 0 {
				x[0] ^= p // invert
			} else {
				t = (x[0] ^ x[i]) & p
				x[0] ^= t
				x[i] ^= t
			}
		}
	}
	// Gray encode
	for i := 1; i < n; i++ {
		x[i] ^= x[i-1]
	}
	t = 0
	for q := M; q > 1; q >>= 1 {
		if (x[n-1] & q) != 0 {
			t ^= q - 1
		}
	}
	for i := range n {
		x[i] ^= t
	}

	return x
}
