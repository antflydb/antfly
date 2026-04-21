// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package termite

import (
	"bytes"
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeDeserializeFloatArrays(t *testing.T) {
	tests := []struct {
		name string
		data [][]float32
	}{
		{
			name: "empty array",
			data: [][]float32{},
		},
		{
			name: "single vector",
			data: [][]float32{{1.0, 2.0, 3.0}},
		},
		{
			name: "multiple vectors",
			data: [][]float32{
				{1.0, 2.0, 3.0},
				{4.0, 5.0, 6.0},
				{7.0, 8.0, 9.0},
			},
		},
		{
			name: "large dimension vectors",
			data: [][]float32{
				{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.10},
				{11.11, 12.12, 13.13, 14.14, 15.15, 16.16, 17.17, 18.18, 19.19, 20.20},
			},
		},
		{
			name: "negative and zero values",
			data: [][]float32{
				{-1.0, 0.0, 1.0},
				{-2.5, 0.0, 2.5},
			},
		},
		{
			name: "very small and large values",
			data: [][]float32{
				{1e-10, 1e10, -1e-10, -1e10},
				{3.14159, 2.71828, 1.41421, 1.61803},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			var buf bytes.Buffer
			err := SerializeFloatArrays(&buf, tt.data)
			require.NoError(t, err)

			// Deserialize
			result, err := DeserializeFloatArrays(&buf)
			require.NoError(t, err)

			// Compare
			assert.Equal(t, tt.data, result)
		})
	}
}

func TestSerializeFloatArrays_WriteErr(t *testing.T) {
	// Test with a writer that fails after a certain number of writes
	data := [][]float32{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}

	// This writer will fail on the 3rd write
	w := &failingWriter{failAfter: 3}
	err := SerializeFloatArrays(w, data)
	assert.Error(t, err)
}

func TestDeserializeFloatArrays_ReadErr(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "empty reader",
			data:    []byte{},
			wantErr: "reading number of vectors",
		},
		{
			name:    "incomplete header",
			data:    []byte{1, 0, 0, 0, 0, 0, 0}, // only 7 bytes instead of 8
			wantErr: "reading number of vectors",
		},
		{
			name:    "missing dimension",
			data:    []byte{1, 0, 0, 0, 0, 0, 0, 0}, // numVectors=1, but no dimension
			wantErr: "reading number of vectors",
		},
		{
			name: "incomplete data",
			data: []byte{
				1, 0, 0, 0, 0, 0, 0, 0, // numVectors=1
				2, 0, 0, 0, 0, 0, 0, 0, // dimension=2
				0, 0, 128, 63, // first float (1.0)
				// missing second float
			},
			wantErr: "EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			_, err := DeserializeFloatArrays(r)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestSerializeDeserializeFloatArrays_Consistency(t *testing.T) {
	// Test multiple serialize/deserialize cycles
	original := [][]float32{
		{1.1, 2.2, 3.3},
		{4.4, 5.5, 6.6},
		{7.7, 8.8, 9.9},
	}

	for range 3 {
		var buf bytes.Buffer
		err := SerializeFloatArrays(&buf, original)
		require.NoError(t, err)

		result, err := DeserializeFloatArrays(&buf)
		require.NoError(t, err)
		assert.Equal(t, original, result)

		// Use result as input for next iteration
		original = result
	}
}

func BenchmarkSerializeFloatArrays(b *testing.B) {
	data := make([][]float32, 100)
	for i := range data {
		data[i] = make([]float32, 128) // typical embedding dimension
		for j := range data[i] {
			data[i][j] = float32(i*128 + j)
		}
	}

	for b.Loop() {
		var buf bytes.Buffer
		_ = SerializeFloatArrays(&buf, data)
	}
}

func BenchmarkDeserializeFloatArrays(b *testing.B) {
	// Prepare serialized data
	data := make([][]float32, 100)
	for i := range data {
		data[i] = make([]float32, 128)
		for j := range data[i] {
			data[i][j] = float32(i*128 + j)
		}
	}

	var buf bytes.Buffer
	_ = SerializeFloatArrays(&buf, data)
	serialized := buf.Bytes()

	for b.Loop() {
		r := bytes.NewReader(serialized)
		_, _ = DeserializeFloatArrays(r)
	}
}

func TestSerializeDeserializeSparseVectors(t *testing.T) {
	tests := []struct {
		name string
		data []embeddings.SparseVector
	}{
		{
			name: "empty array",
			data: []embeddings.SparseVector{},
		},
		{
			name: "single vector",
			data: []embeddings.SparseVector{
				{Indices: []uint32{0, 5, 100}, Values: []float32{0.5, 1.2, 0.8}},
			},
		},
		{
			name: "multiple vectors",
			data: []embeddings.SparseVector{
				{Indices: []uint32{1, 3}, Values: []float32{0.1, 0.9}},
				{Indices: []uint32{0, 2, 4, 6}, Values: []float32{0.3, 0.7, 0.2, 0.5}},
				{Indices: []uint32{10}, Values: []float32{2.5}},
			},
		},
		{
			name: "empty sparse vector (zero nnz)",
			data: []embeddings.SparseVector{
				{Indices: []uint32{}, Values: []float32{}},
			},
		},
		{
			name: "mixed empty and non-empty",
			data: []embeddings.SparseVector{
				{Indices: []uint32{5, 10}, Values: []float32{1.0, 2.0}},
				{Indices: []uint32{}, Values: []float32{}},
				{Indices: []uint32{100}, Values: []float32{0.01}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := SerializeSparseVectors(&buf, tt.data)
			require.NoError(t, err)

			result, err := DeserializeSparseVectors(&buf)
			require.NoError(t, err)

			assert.Equal(t, tt.data, result)
		})
	}
}

func TestDeserializeSparseVectors_ReadErr(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "empty reader",
			data:    []byte{},
			wantErr: "reading num vectors",
		},
		{
			name:    "missing nnz",
			data:    []byte{1, 0, 0, 0, 0, 0, 0, 0}, // numVectors=1, but no nnz
			wantErr: "reading nnz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			_, err := DeserializeSparseVectors(r)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func BenchmarkSerializeSparseVectors(b *testing.B) {
	data := make([]embeddings.SparseVector, 100)
	for i := range data {
		nnz := 256
		indices := make([]uint32, nnz)
		values := make([]float32, nnz)
		for j := range nnz {
			indices[j] = uint32(j * 100)
			values[j] = float32(j) * 0.01
		}
		data[i] = embeddings.SparseVector{Indices: indices, Values: values}
	}

	for b.Loop() {
		var buf bytes.Buffer
		_ = SerializeSparseVectors(&buf, data)
	}
}

func BenchmarkDeserializeSparseVectors(b *testing.B) {
	data := make([]embeddings.SparseVector, 100)
	for i := range data {
		nnz := 256
		indices := make([]uint32, nnz)
		values := make([]float32, nnz)
		for j := range nnz {
			indices[j] = uint32(j * 100)
			values[j] = float32(j) * 0.01
		}
		data[i] = embeddings.SparseVector{Indices: indices, Values: values}
	}

	var buf bytes.Buffer
	_ = SerializeSparseVectors(&buf, data)
	serialized := buf.Bytes()

	for b.Loop() {
		r := bytes.NewReader(serialized)
		_, _ = DeserializeSparseVectors(r)
	}
}

// failingWriter is a test helper that fails after a certain number of writes
type failingWriter struct {
	writes    int
	failAfter int
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	w.writes++
	if w.writes > w.failAfter {
		return 0, assert.AnError
	}
	return len(p), nil
}
