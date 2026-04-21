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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDecoderOnlyVLMModel_ValidStructure(t *testing.T) {
	// Create temp directory with decoder-only VLM structure
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "my-vlm-model")
	require.NoError(t, os.MkdirAll(modelDir, 0755))

	// Create the required ONNX files: vision_encoder + embed_tokens + decoder
	for _, f := range []string{"vision_encoder.onnx", "embed_tokens.onnx", "decoder_model_merged.onnx"} {
		require.NoError(t, os.WriteFile(filepath.Join(modelDir, f), []byte("dummy"), 0644))
	}

	assert.True(t, IsDecoderOnlyVLMModel(modelDir))
}

func TestIsDecoderOnlyVLMModel_MissingEmbedTokens(t *testing.T) {
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "incomplete-model")
	require.NoError(t, os.MkdirAll(modelDir, 0755))

	// Missing embed_tokens.onnx
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "vision_encoder.onnx"), []byte("dummy"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "decoder_model_merged.onnx"), []byte("dummy"), 0644))

	assert.False(t, IsDecoderOnlyVLMModel(modelDir))
}

func TestIsDecoderOnlyVLMModel_EncoderDecoderVLMStructure(t *testing.T) {
	// Encoder-decoder VLM has encoder_model.onnx — should not be detected as decoder-only
	tmpDir := t.TempDir()
	florenceDir := filepath.Join(tmpDir, "florence-2")
	require.NoError(t, os.MkdirAll(florenceDir, 0755))

	// Encoder-decoder VLM files (has encoder_model.onnx)
	for _, f := range []string{"vision_encoder.onnx", "embed_tokens.onnx", "encoder_model.onnx", "decoder_model_merged.onnx"} {
		require.NoError(t, os.WriteFile(filepath.Join(florenceDir, f), []byte("dummy"), 0644))
	}

	assert.False(t, IsDecoderOnlyVLMModel(florenceDir))
}

func TestIsDecoderOnlyVLMModel_AnyModelName(t *testing.T) {
	// Structural detection should work regardless of model name in path
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "completely-unrelated-name")
	require.NoError(t, os.MkdirAll(modelDir, 0755))

	for _, f := range []string{"vision_encoder.onnx", "embed_tokens.onnx", "decoder_model_merged.onnx"} {
		require.NoError(t, os.WriteFile(filepath.Join(modelDir, f), []byte("dummy"), 0644))
	}

	assert.True(t, IsDecoderOnlyVLMModel(modelDir))
}

func TestLoadVision2SeqModelConfig_DecoderOnlyVLM(t *testing.T) {
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "decoder-only-vlm")
	require.NoError(t, os.MkdirAll(modelDir, 0755))

	// Create ONNX files
	for _, f := range []string{"vision_encoder.onnx", "embed_tokens.onnx", "decoder_model_merged.onnx"} {
		require.NoError(t, os.WriteFile(filepath.Join(modelDir, f), []byte("dummy"), 0644))
	}

	// Create config.json matching real Moondream2 format (nested phi_config)
	config := map[string]any{
		"model_type":        "moondream1",
		"image_token_index": -200,
		"phi_config": map[string]any{
			"hidden_size":             2048,
			"num_hidden_layers":       24,
			"num_attention_heads":     32,
			"num_key_value_heads":     32,
			"vocab_size":              51200,
			"bos_token_id":            50256,
			"eos_token_id":            50256,
			"max_position_embeddings": 2048,
		},
	}
	configBytes, _ := json.Marshal(config)
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "config.json"), configBytes, 0644))

	cfg, err := LoadVision2SeqModelConfig(modelDir)
	require.NoError(t, err)

	assert.Equal(t, 2048, cfg.HiddenSize)
	assert.Equal(t, 24, cfg.NumLayers)
	assert.Equal(t, 32, cfg.NumHeads)
	assert.Equal(t, 64, cfg.HeadDim) // 2048 / 32

	assert.NotNil(t, cfg.DecoderConfig)
	assert.Equal(t, 51200, cfg.DecoderConfig.VocabSize)
	assert.Equal(t, int32(50256), cfg.DecoderConfig.EOSTokenID)
	// DecoderStartTokenID should fall back to BOSTokenID
	assert.Equal(t, int32(50256), cfg.DecoderConfig.DecoderStartTokenID)

	assert.NotNil(t, cfg.ImageConfig)
	// Image dimensions come from preprocessor_config.json (not config.json)
	// so they default to 224 without a preprocessor config file
	assert.Equal(t, 224, cfg.ImageConfig.Width)
	assert.Equal(t, 224, cfg.ImageConfig.Height)

	// EmbedTokensPath should be found
	assert.NotEmpty(t, cfg.EmbedTokensPath)
	assert.Contains(t, cfg.EmbedTokensPath, "embed_tokens.onnx")
}

func TestLoadVision2SeqModelConfig_DecoderOnlyVLMDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "decoder-only-vlm")
	require.NoError(t, os.MkdirAll(modelDir, 0755))

	// Create ONNX files
	for _, f := range []string{"vision_encoder.onnx", "embed_tokens.onnx", "decoder_model_merged.onnx"} {
		require.NoError(t, os.WriteFile(filepath.Join(modelDir, f), []byte("dummy"), 0644))
	}

	// Create minimal config.json
	config := map[string]any{
		"model_type": "moondream1",
	}
	configBytes, _ := json.Marshal(config)
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "config.json"), configBytes, 0644))

	cfg, err := LoadVision2SeqModelConfig(modelDir)
	require.NoError(t, err)

	// Should use defaults from the fallback path
	assert.Equal(t, 6, cfg.NumLayers)    // default when no layers field present
	assert.Equal(t, 8, cfg.NumHeads)     // default when no heads field present
	assert.Equal(t, 768, cfg.HiddenSize) // default when no hidden_size present
}

func TestLoadVision2SeqModelConfig_DecoderOnlyVLMWithPreprocessor(t *testing.T) {
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "decoder-only-vlm")
	require.NoError(t, os.MkdirAll(modelDir, 0755))

	// Create ONNX files
	for _, f := range []string{"vision_encoder.onnx", "embed_tokens.onnx", "decoder_model_merged.onnx"} {
		require.NoError(t, os.WriteFile(filepath.Join(modelDir, f), []byte("dummy"), 0644))
	}

	// Create config.json
	config := map[string]any{"model_type": "moondream1"}
	configBytes, _ := json.Marshal(config)
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "config.json"), configBytes, 0644))

	// Create preprocessor_config.json with SigLIP normalization values
	preproc := map[string]any{
		"size":       map[string]any{"height": float64(384), "width": float64(384)},
		"image_mean": []float64{0.5, 0.5, 0.5},
		"image_std":  []float64{0.5, 0.5, 0.5},
	}
	preprocBytes, _ := json.Marshal(preproc)
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "preprocessor_config.json"), preprocBytes, 0644))

	cfg, err := LoadVision2SeqModelConfig(modelDir)
	require.NoError(t, err)

	// Should use preprocessor values
	assert.Equal(t, 384, cfg.ImageConfig.Width)
	assert.Equal(t, 384, cfg.ImageConfig.Height)
	assert.InDelta(t, 0.5, cfg.ImageConfig.Mean[0], 0.01)
	assert.InDelta(t, 0.5, cfg.ImageConfig.Std[0], 0.01)
}

func TestLoadVision2SeqModelConfig_DecoderOnlyVLMMissingConfig(t *testing.T) {
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "decoder-only-vlm")
	require.NoError(t, os.MkdirAll(modelDir, 0755))

	// Create ONNX files but no config.json
	for _, f := range []string{"vision_encoder.onnx", "embed_tokens.onnx", "decoder_model_merged.onnx"} {
		require.NoError(t, os.WriteFile(filepath.Join(modelDir, f), []byte("dummy"), 0644))
	}

	_, err := LoadVision2SeqModelConfig(modelDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config.json")
}
