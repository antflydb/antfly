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
