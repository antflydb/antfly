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

package modelregistry

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectMultimodalCapabilities_Standard(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "visual_model.onnx"), []byte("v"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "text_model.onnx"), []byte("t"), 0644))

	caps := DetectMultimodalCapabilities(dir)
	assert.True(t, caps.HasImage)
	assert.False(t, caps.HasImageQuantized)
	assert.False(t, caps.HasAudio)
}

func TestDetectMultimodalCapabilities_I8Variant(t *testing.T) {
	dir := t.TempDir()
	// CLIP pulled with --variants i8: only variant files exist
	require.NoError(t, os.WriteFile(filepath.Join(dir, "visual_model_i8.onnx"), []byte("v"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "text_model_i8.onnx"), []byte("t"), 0644))

	caps := DetectMultimodalCapabilities(dir)
	assert.True(t, caps.HasImage, "should detect visual_model_i8.onnx as image capability")
	assert.False(t, caps.HasImageQuantized)
}

func TestDetectMultimodalCapabilities_F16Variant(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "visual_model_f16.onnx"), []byte("v"), 0644))

	caps := DetectMultimodalCapabilities(dir)
	assert.True(t, caps.HasImage, "should detect visual_model_f16.onnx as image capability")
}

func TestDetectMultimodalCapabilities_AudioVariant(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "audio_model_i8.onnx"), []byte("a"), 0644))

	caps := DetectMultimodalCapabilities(dir)
	assert.True(t, caps.HasAudio, "should detect audio_model_i8.onnx as audio capability")
}

func TestDetectMultimodalCapabilities_Empty(t *testing.T) {
	dir := t.TempDir()

	caps := DetectMultimodalCapabilities(dir)
	assert.False(t, caps.HasImage)
	assert.False(t, caps.HasAudio)
	assert.False(t, caps.HasImageQuantized)
	assert.False(t, caps.HasAudioQuantized)
}

func TestFileExistsStemAny(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "visual_model_i8.onnx"), []byte("v"), 0644))

	assert.True(t, fileExistsStemAny(dir, "visual_model"))
	assert.False(t, fileExistsStemAny(dir, "audio_model"))
	assert.False(t, fileExistsStemAny(dir, "nonexistent"))
}
