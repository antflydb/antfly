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
	"cmp"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/chewxy/math32"
)

// MaxDim is the maximum number of dimensions a vector can have.
const MaxDim = 16000

var ErrMaxDimExceeded = fmt.Errorf("vector cannot have more than %d dimensions", MaxDim)

type T []float32

// MarshalJSON encodes the vector as a base64 string of raw little-endian float32 bytes.
// This is ~4x more compact than the default JSON array of decimal numbers.
func (v T) MarshalJSON() ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	if len(v) == 0 {
		return []byte(`""`), nil
	}
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(v))), len(v)*4)
	if !isLittleEndian() {
		raw = make([]byte, len(v)*4)
		for i, f := range v {
			binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(f))
		}
	}
	// Single allocation: quote + base64 + quote. Encode directly into buffer.
	n := base64.StdEncoding.EncodedLen(len(raw))
	buf := make([]byte, n+2)
	buf[0] = '"'
	base64.StdEncoding.Encode(buf[1:1+n], raw)
	buf[n+1] = '"'
	return buf, nil
}

// UnmarshalJSON decodes a vector from either a base64 string (binary format)
// or a JSON array of numbers (legacy format) for backward compatibility.
func (v *T) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*v = nil
		return nil
	}
	// Base64 string format: "aGVsbG8..."
	if data[0] == '"' {
		if len(data) < 2 || data[len(data)-1] != '"' {
			return errors.New("invalid JSON string for vector")
		}
		src := data[1 : len(data)-1]
		maxLen := base64.StdEncoding.DecodedLen(len(src))
		// Allocate float32 slice at max possible size, decode directly into its
		// backing bytes, then trim. This avoids an intermediate []byte allocation
		// on little-endian systems.
		maxFloats := (maxLen + 3) / 4 // round up to cover DecodedLen overestimate
		result := make(T, maxFloats)
		dst := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(result))), maxFloats*4)
		n, err := base64.StdEncoding.Decode(dst, src)
		if err != nil {
			return fmt.Errorf("decoding base64 vector: %w", err)
		}
		if n%4 != 0 {
			return fmt.Errorf("invalid vector byte length: %d (must be multiple of 4)", n)
		}
		result = result[:n/4]
		if !isLittleEndian() {
			for i := range result {
				result[i] = math.Float32frombits(binary.LittleEndian.Uint32(dst[i*4:]))
			}
		}
		*v = result
		return nil
	}
	// Legacy JSON array format: [0.1, 0.2, 0.3]
	if data[0] == '[' {
		// Manually parse to avoid infinite recursion
		s := strings.TrimSpace(string(data))
		s = s[1 : len(s)-1] // strip brackets
		if strings.TrimSpace(s) == "" {
			*v = T{}
			return nil
		}
		parts := strings.Split(s, ",")
		result := make(T, len(parts))
		for i, p := range parts {
			f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
			if err != nil {
				return fmt.Errorf("parsing vector element %d: %w", i, err)
			}
			result[i] = float32(f)
		}
		*v = result
		return nil
	}
	return fmt.Errorf("unexpected JSON type for vector: %c", data[0])
}

func isLittleEndian() bool {
	var x uint16 = 0x0102
	return *(*byte)(unsafe.Pointer(&x)) == 0x02
}

// FromString parses the string representation of a vector.
//
// This should be the same format as a pgvector.
func FromString(input string) (T, error) {
	input = strings.TrimSpace(input)
	if !strings.HasPrefix(input, "[") || !strings.HasSuffix(input, "]") {
		return T{}, errors.New(
			"malformed vector literal: Vector contents must start with \"[\" and" +
				" end with \"]\"",
		)
	}

	input = strings.TrimPrefix(input, "[")
	input = strings.TrimSuffix(input, "]")
	parts := strings.Split(input, ",")

	if len(parts) > MaxDim {
		return T{}, ErrMaxDimExceeded
	}

	vector := make(T, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return T{}, errors.New("invalid input syntax for type vector: empty string")
		}

		val, err := strconv.ParseFloat(part, 32)
		if err != nil {
			return T{}, fmt.Errorf("invalid input syntax for type vector: %s", part)
		}

		if math.IsInf(val, 0) {
			return T{}, errors.New("infinite value not allowed in vector")
		}
		if math.IsNaN(val) {
			return T{}, errors.New("NaN not allowed in vector")
		}
		vector[i] = float32(val)
	}

	return vector, nil
}

// AsSet returns this vector a set of one vector.
func (v T) AsSet() *Set {
	return Set_builder{
		Dims:  int64(len(v)),
		Count: 1,
		Data:  slices.Clip(v),
	}.Build()
}

// String implements the fmt.Stringer interface.
func (v T) String() string {
	var sb strings.Builder
	// Pre-grow by a reasonable amount to avoid multiple allocations.
	sb.Grow(len(v)*8 + 2)
	sb.WriteString("[")
	for i, v := range v {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
	}
	sb.WriteString("]")
	return sb.String()
}

// Size returns the size of the vector in bytes.
func (v T) Size() uintptr {
	return 24 + uintptr(cap(v))*4
}

// Compare returns -1 if v < v2, 1 if v > v2, and 0 if v == v2.
func (v T) Compare(v2 T) int {
	n := min(len(v), len(v2))
	for i := range n {
		if c := cmp.Compare(v[i], v2[i]); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(v), len(v2))
}

// InDelta checks if the two vectors are equal within the given delta for each dimension.
func (v T) InDelta(v2 T, delta float32) error {
	if len(v) != len(v2) {
		return fmt.Errorf("vector length mismatch: %d vs %d", len(v), len(v2))
	}
	for i := range v {
		vf := v[i]
		v2f := v2[i]
		if math32.IsNaN(vf) && math32.IsNaN(v2f) {
			continue
		} else if math32.IsNaN(vf) || math32.IsNaN(v2f) {
			return fmt.Errorf("vector value mismatch at index %d: %v vs %v", i, vf, v2f)
		}
		if math32.Abs(vf-v2f) > delta {
			return fmt.Errorf("vector value mismatch at index %d: %v vs %v", i, vf, v2f)
		}
	}
	return nil
}

// Encode encodes the vector as a byte array suitable for storing in pebble.
func Encode(appendTo []byte, t T) ([]byte, error) {
	appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(t))) //nolint:gosec // G115: bounded value, cannot overflow in practice
	encoded := make([]byte, len(t)*4)
	vec.EncodeFloat32s(encoded, t)
	appendTo = append(appendTo, encoded...)
	return appendTo, nil
}

// Decode decodes the byte array into a vector and returns any remaining bytes.
func Decode(b []byte) (remaining []byte, ret T, err error) {
	var n uint32
	b, n, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, ret, err
	}

	// Validate dimension is reasonable (max 100k dimensions to catch corruption)
	const maxDimension = 100000
	if n > maxDimension {
		return nil, ret, fmt.Errorf("invalid vector dimension %d (max %d): possible data corruption", n, maxDimension)
	}

	// Validate buffer has enough bytes for the vector data
	requiredBytes := n * 4
	if uint32(len(b)) < requiredBytes { //nolint:gosec // G115: bounded value, cannot overflow in practice
		return nil, ret, fmt.Errorf("insufficient bytes for vector: need %d bytes for %d dimensions, got %d bytes", requiredBytes, n, len(b))
	}

	ret = make(T, n)
	vec.DecodeFloat32s(ret, b)
	b = b[n*4:] // Adjust the buffer to skip the float32 values.
	return b, ret, nil
}

// Decode decodes the byte array into a supplied vector and returns any remaining bytes.
func DecodeTo(b []byte, dst T) (remaining []byte, err error) {
	var n uint32
	b, n, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, err
	}

	// Validate dimension matches destination vector size
	if int(n) != len(dst) {
		return nil, fmt.Errorf("dimension mismatch: encoded dimension %d != destination size %d", n, len(dst))
	}

	// Validate buffer has enough bytes for the vector data
	requiredBytes := n * 4
	if uint32(len(b)) < requiredBytes { //nolint:gosec // G115: bounded value, cannot overflow in practice
		return nil, fmt.Errorf("insufficient bytes for vector: need %d bytes for %d dimensions, got %d bytes", requiredBytes, n, len(b))
	}

	vec.DecodeFloat32s(dst, b)
	b = b[n*4:] // Adjust the buffer to skip the float32 values.
	return b, nil
}

// Random returns a random vector with the number of dimensions in [1, maxDim]
// range.
func Random(rng *rand.Rand, maxDim int) T {
	n := 1 + rng.IntN(maxDim)
	v := make(T, n)
	for i := range v {
		for {
			v[i] = float32(rng.NormFloat64())
			if math32.IsNaN(v[i]) || math32.IsInf(v[i], 0) {
				continue
			}
			break
		}
	}
	return v
}

// ValidateUnitVector panics if the given vector is not a unit vector or the
// zero vector (for degenerate case where norm=0).
func ValidateUnitVector(v T) {
	if utils.AfdbTestBuild {
		norm := vec.SquaredNormFloat32(v)
		// Check if norm is approximately 1 (within 0.01 tolerance for 2 decimal places)
		if norm != 0 && math32.Abs(norm-1) > 0.01 {
			panic(fmt.Errorf("vector is not a unit vector: %s", v))
		}
	}
}

// ValidateUnitVectorSet panics if the given vectors are not unit vectors or zero
// vectors (for degenerate case where norm=0).
func ValidateUnitVectorSet(vectors *Set) {
	if utils.AfdbTestBuild {
		for i := range vectors.GetCount() {
			ValidateUnitVector(vectors.At(int(i)))
		}
	}
}

// BatchL2SquaredDistance computes L2 squared distances from a single query vector to multiple data vectors.
// For each data vector data[i*dims:(i+1)*dims], it computes the L2 squared distance to query.
// Results are stored in distances[i].
//
// This function automatically chooses the optimal implementation based on dimensionality:
//   - For dims >= 3000: Uses SME batch processing (faster for high-dimensional vectors)
//   - For dims < 3000: Uses sequential processing (faster for typical embeddings)
//
// Most production embeddings (OpenAI, Cohere, etc.) use 512-1536 dimensions and will
// automatically use the faster sequential implementation.
//
// Parameters:
//   - query: query vector (dims elements)
//   - data: flattened array of data vectors (count*dims elements)
//   - distances: output buffer (count elements)
//   - count: number of data vectors
//   - dims: dimensionality of vectors
func BatchL2SquaredDistance(query, data []float32, distances []float32, count, dims int) {
	vec.BatchL2SquaredDistanceFloat32(query, data, distances, count, dims)
}

// BatchDot computes dot products of a single query vector with multiple data vectors.
// For each data vector data[i*dims:(i+1)*dims], it computes the dot product with query.
// Results are stored in dots[i].
//
// This function automatically chooses the optimal implementation based on dimensionality:
//   - For dims >= 3000: Uses SME batch processing (faster for high-dimensional vectors)
//   - For dims < 3000: Uses sequential processing (faster for typical embeddings)
//
// Most production embeddings (OpenAI, Cohere, etc.) use 512-1536 dimensions and will
// automatically use the faster sequential implementation.
//
// Parameters:
//   - query: query vector (dims elements)
//   - data: flattened array of data vectors (count*dims elements)
//   - dots: output buffer (count elements)
//   - count: number of data vectors
//   - dims: dimensionality of vectors
func BatchDot(query, data []float32, dots []float32, count, dims int) {
	vec.BatchDotFloat32(query, data, dots, count, dims)
}
