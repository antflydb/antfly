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

package pipelines

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindONNXFile_ExactMatch(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "model.onnx"), []byte("test"), 0644))

	result := FindONNXFile(dir, []string{"model.onnx"})
	assert.Equal(t, filepath.Join(dir, "model.onnx"), result)
}

func TestFindONNXFile_VariantFallback_I8(t *testing.T) {
	dir := t.TempDir()
	// Only i8 variant exists (user pulled --variants i8 without f32)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "model_i8.onnx"), []byte("test"), 0644))

	result := FindONNXFile(dir, []string{"model.onnx"})
	assert.Equal(t, filepath.Join(dir, "model_i8.onnx"), result)
}

func TestFindONNXFile_VariantFallback_F16(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "model_f16.onnx"), []byte("test"), 0644))

	result := FindONNXFile(dir, []string{"model.onnx"})
	assert.Equal(t, filepath.Join(dir, "model_f16.onnx"), result)
}

func TestFindONNXFile_ExactPreferredOverVariant(t *testing.T) {
	dir := t.TempDir()
	// Both base and variant exist — base should be preferred
	require.NoError(t, os.WriteFile(filepath.Join(dir, "model.onnx"), []byte("base"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "model_i8.onnx"), []byte("i8"), 0644))

	result := FindONNXFile(dir, []string{"model.onnx"})
	assert.Equal(t, filepath.Join(dir, "model.onnx"), result)
}

func TestFindONNXFile_MultimodalVariant(t *testing.T) {
	dir := t.TempDir()
	// CLIP-style model with only i8 variants
	require.NoError(t, os.WriteFile(filepath.Join(dir, "text_model_i8.onnx"), []byte("text"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "visual_model_i8.onnx"), []byte("visual"), 0644))

	textResult := FindONNXFile(dir, []string{"text_model.onnx", "model.onnx"})
	assert.Equal(t, filepath.Join(dir, "text_model_i8.onnx"), textResult)

	visualResult := FindONNXFile(dir, []string{"visual_model.onnx"})
	assert.Equal(t, filepath.Join(dir, "visual_model_i8.onnx"), visualResult)
}

func TestFindONNXFile_OnnxSubdirVariant(t *testing.T) {
	dir := t.TempDir()
	onnxDir := filepath.Join(dir, "onnx")
	require.NoError(t, os.MkdirAll(onnxDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(onnxDir, "model_i8.onnx"), []byte("test"), 0644))

	result := FindONNXFile(dir, []string{"model.onnx"})
	assert.Equal(t, filepath.Join(onnxDir, "model_i8.onnx"), result)
}

func TestFindONNXFile_NoMatch(t *testing.T) {
	dir := t.TempDir()

	result := FindONNXFile(dir, []string{"model.onnx"})
	assert.Empty(t, result)
}

func TestFindONNXFile_CandidateOrderPreserved(t *testing.T) {
	dir := t.TempDir()
	// First candidate's variant should be found before second candidate's variant
	require.NoError(t, os.WriteFile(filepath.Join(dir, "text_model_i8.onnx"), []byte("text"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "model_i8.onnx"), []byte("generic"), 0644))

	result := FindONNXFile(dir, []string{"text_model.onnx", "model.onnx"})
	assert.Equal(t, filepath.Join(dir, "text_model_i8.onnx"), result)
}

func TestParseTokenID(t *testing.T) {
	assert.Equal(t, int32(1), ParseTokenID(float64(1)))
	assert.Equal(t, int32(50256), ParseTokenID(float64(50256)))
	assert.Equal(t, int32(2), ParseTokenID([]any{float64(2), float64(3)}))
	assert.Equal(t, int32(0), ParseTokenID(nil))
	assert.Equal(t, int32(0), ParseTokenID("invalid"))
	assert.Equal(t, int32(0), ParseTokenID([]any{}))
}

func TestFindONNXFile_NonOnnxCandidateSkipped(t *testing.T) {
	dir := t.TempDir()
	// Non-.onnx candidates should be skipped in variant expansion
	result := FindONNXFile(dir, []string{"tokenizer.json"})
	assert.Empty(t, result)
}
