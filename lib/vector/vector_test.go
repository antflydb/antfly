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
	"encoding/json"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/chewxy/math32"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	NaN32 float32 = math32.NaN()
	Inf32 float32 = math32.Inf(1)
)

func TestFromString(t *testing.T) {
	testCases := []struct {
		input    string
		expected T
		hasError bool
	}{
		{input: "[1,2,3]", expected: T{1, 2, 3}, hasError: false},
		{input: "[1.0, 2.0, 3.0]", expected: T{1.0, 2.0, 3.0}, hasError: false},
		{input: "[1.0, 2.0, 3.0", expected: T{}, hasError: true},
		{input: "1.0, 2.0, 3.0]", expected: T{}, hasError: true},
		{input: "[1.0, 2.0, [3.0]]", expected: T{}, hasError: true},
		{input: "1.0, 2.0, 3.0]", expected: T{}, hasError: true},
		{input: "1.0, , 3.0]", expected: T{}, hasError: true},
		{input: "", expected: T{}, hasError: true},
		{input: "[]", expected: T{}, hasError: true},
		{input: "1.0, 2.0, 3.0", expected: T{}, hasError: true},
	}

	for _, tc := range testCases {
		result, err := FromString(tc.input)

		if tc.hasError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
			// Test roundtripping through String().
			s := result.String()
			result, err = FromString(s)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	}

	// Test the maxdims error case.
	var sb strings.Builder
	sb.WriteString("[")
	for range MaxDim {
		sb.WriteString("1,")
	}
	sb.WriteString("1]")
	_, err := FromString(sb.String())
	assert.Errorf(t, err, "vector cannot have more than %d dimensions", MaxDim)
}

// TODO (ajr) Consider making our own randutils package
var randLetters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandBytes returns a byte slice of the given length with random
// data.
func RandBytes(r *rand.Rand, size int) []byte {
	if size <= 0 {
		return nil
	}

	arr := make([]byte, size)
	for i := range arr {
		arr[i] = randLetters[r.IntN(len(randLetters))]
	}
	return arr
}

func TestJSONRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		vec  T
	}{
		{"nil", nil},
		{"empty", T{}},
		{"small", T{1.0, 2.0, 3.0}},
		{"negative", T{-1.5, 0.0, 1.5}},
		{"large", func() T {
			v := make(T, 384)
			for i := range v {
				v[i] = float32(i) * 0.001
			}
			return v
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.vec)
			require.NoError(t, err)

			var got T
			err = json.Unmarshal(data, &got)
			require.NoError(t, err)

			if tt.vec == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.vec, got)
			}
		})
	}
}

func TestJSONUnmarshalLegacyArray(t *testing.T) {
	// Legacy format: JSON array of numbers
	data := []byte(`[1.0, 2.0, 3.0]`)
	var v T
	err := json.Unmarshal(data, &v)
	require.NoError(t, err)
	assert.Equal(t, T{1.0, 2.0, 3.0}, v)
}

func TestJSONRoundtripInMap(t *testing.T) {
	// Simulate RemoteIndexSearchRequest.VectorSearches
	type request struct {
		VectorSearches map[string]T `json:"vector_searches"`
	}
	original := request{
		VectorSearches: map[string]T{
			"index_a": {0.1, 0.2, 0.3},
			"index_b": {0.4, 0.5, 0.6},
		},
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// Verify it's base64 strings, not arrays
	assert.NotContains(t, string(data), "[0.1")

	var got request
	err = json.Unmarshal(data, &got)
	require.NoError(t, err)
	assert.InDeltaSlice(t, []float32(original.VectorSearches["index_a"]), []float32(got.VectorSearches["index_a"]), 1e-6)
	assert.InDeltaSlice(t, []float32(original.VectorSearches["index_b"]), []float32(got.VectorSearches["index_b"]), 1e-6)
}

func TestJSONRoundtripRandomVector(t *testing.T) {
	r := rand.New(rand.NewPCG(99, 1))
	for range 100 {
		v := Random(r, 1024)
		data, err := json.Marshal(v)
		require.NoError(t, err)

		var got T
		err = json.Unmarshal(data, &got)
		require.NoError(t, err)
		assert.Equal(t, v, got)
	}
}

func BenchmarkJSONMarshal(b *testing.B) {
	v := make(T, 384) // typical embedding dimension
	for i := range v {
		v[i] = float32(i) * 0.001
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = json.Marshal(v)
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
	v := make(T, 384)
	for i := range v {
		v[i] = float32(i) * 0.001
	}
	data, _ := json.Marshal(v)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var out T
		_ = json.Unmarshal(data, &out)
	}
}

func TestRoundtripRandomVector(t *testing.T) {
	r := rand.New(rand.NewPCG(42, 1048))
	extra := RandBytes(r, 10)
	for range 1000 {
		v := Random(r, 1000 /* maxDim */)
		encoded, err := Encode(nil, v)
		assert.NoError(t, err)
		encoded = append(encoded, extra...)
		require.Len(t, encoded, len(v)*4+4+len(extra), "encoded vector length mismatch")
		remaining, roundtripped, err := Decode(encoded)
		assert.NoError(t, err)
		require.Equal(t, v.String(), roundtripped.String())
		assert.Equal(t, extra, remaining)
		reEncoded, err := Encode(nil, roundtripped)
		assert.NoError(t, err)
		assert.Equal(t, encoded, append(reEncoded, extra...))
	}
}
