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

// ============================================================================
// Embedding: MaxPositionEmbeddings extraction
// ============================================================================

func TestLoadEmbeddingModelConfig_MaxPositionEmbeddings_RootLevel(t *testing.T) {
	// BERT-style models have max_position_embeddings at the root level of config.json.
	tmpDir := t.TempDir()

	// Create model.onnx so that a text encoder is detected.
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{"model_type": "bert", "hidden_size": 384, "max_position_embeddings": 256}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadEmbeddingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 256, cfg.MaxTextLength, "should read max_position_embeddings from root level")
}

func TestLoadEmbeddingModelConfig_MaxPositionEmbeddings_Nested(t *testing.T) {
	// CLIP-style models have max_position_embeddings nested under text_config.
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{
		"model_type": "clip",
		"projection_dim": 512,
		"text_config": {
			"hidden_size": 512,
			"max_position_embeddings": 77
		},
		"vision_config": {
			"image_size": 224
		}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadEmbeddingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 77, cfg.MaxTextLength, "should read max_position_embeddings from nested text_config")
}

func TestLoadEmbeddingModelConfig_MaxPositionEmbeddings_RootTakesPrecedence(t *testing.T) {
	// When both root-level and nested text_config have max_position_embeddings,
	// root level should take precedence (FirstNonZero picks first non-zero).
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{
		"model_type": "bert",
		"hidden_size": 768,
		"max_position_embeddings": 512,
		"text_config": {
			"hidden_size": 768,
			"max_position_embeddings": 128
		}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadEmbeddingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 512, cfg.MaxTextLength, "root-level max_position_embeddings should take precedence over nested")
}

func TestLoadEmbeddingModelConfig_MaxPositionEmbeddings_Missing(t *testing.T) {
	// When max_position_embeddings is not present anywhere, MaxTextLength should be 0.
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{"model_type": "bert", "hidden_size": 384}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadEmbeddingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.MaxTextLength, "should be 0 when max_position_embeddings is not present")
}

// ============================================================================
// Reranking: MaxPositionEmbeddings extraction
// ============================================================================

func TestLoadRerankingModelConfig_MaxPositionEmbeddings(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{"model_type": "bert", "num_labels": 1, "hidden_size": 384, "max_position_embeddings": 512}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadRerankingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 512, cfg.MaxTextLength, "should read max_position_embeddings")
}

func TestLoadRerankingModelConfig_MaxPositionEmbeddings_Missing(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{"model_type": "bert", "num_labels": 1, "hidden_size": 384}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadRerankingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.MaxTextLength, "should be 0 when max_position_embeddings is not present")
}

func TestLoadRerankingModelConfig_MaxPositionEmbeddings_LargeValue(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{"model_type": "deberta", "num_labels": 1, "max_position_embeddings": 24528}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadRerankingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 24528, cfg.MaxTextLength, "should handle large max_position_embeddings values")
}

// ============================================================================
// Classification: MaxPositionEmbeddings extraction
// ============================================================================

func TestLoadClassificationModelConfig_MaxPositionEmbeddings(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{"model_type": "bert", "num_labels": 3, "hidden_size": 768, "max_position_embeddings": 512}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadClassificationModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 512, cfg.MaxTextLength, "should read max_position_embeddings")
}

func TestLoadClassificationModelConfig_MaxPositionEmbeddings_Missing(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{"model_type": "bert", "num_labels": 2, "hidden_size": 768}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadClassificationModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.MaxTextLength, "should be 0 when max_position_embeddings is not present")
}

// ============================================================================
// NER: MaxPositionEmbeddings extraction
// ============================================================================

func TestLoadNERModelConfig_MaxPositionEmbeddings(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{
		"model_type": "bert",
		"max_position_embeddings": 512,
		"id2label": {"0": "O", "1": "B-PER", "2": "I-PER"}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadNERModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 512, cfg.MaxTextLength, "should read max_position_embeddings")
}

func TestLoadNERModelConfig_MaxPositionEmbeddings_Missing(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{
		"model_type": "bert",
		"id2label": {"0": "O", "1": "B-PER", "2": "I-PER"}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadNERModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.MaxTextLength, "should be 0 when max_position_embeddings is not present")
}

// ============================================================================
// Chunking: MaxPositionEmbeddings extraction
// ============================================================================

func TestLoadChunkingModelConfig_MaxPositionEmbeddings(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{
		"model_type": "bert",
		"num_labels": 3,
		"max_position_embeddings": 512,
		"id2label": {"0": "O", "1": "B-SEP", "2": "I-SEP"}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadChunkingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 512, cfg.MaxTextLength, "should read max_position_embeddings")
}

func TestLoadChunkingModelConfig_MaxPositionEmbeddings_Missing(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "model.onnx"), []byte("dummy"), 0644))

	config := `{
		"model_type": "bert",
		"num_labels": 3,
		"id2label": {"0": "O", "1": "B-SEP", "2": "I-SEP"}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.json"), []byte(config), 0644))

	cfg, err := LoadChunkingModelConfig(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.MaxTextLength, "should be 0 when max_position_embeddings is not present")
}

// ============================================================================
// FirstNonZero precedence in pipeline MaxLength
// ============================================================================

func TestFirstNonZero_MaxLengthPrecedence(t *testing.T) {
	// Simulates the precedence chain used in all pipeline loaders:
	// FirstNonZero(loaderCfg.maxLength, config.MaxTextLength, 512)

	tests := []struct {
		name            string
		loaderMaxLength int
		configMaxText   int
		fallback        int
		want            int
	}{
		{
			name:            "loader option takes precedence",
			loaderMaxLength: 256,
			configMaxText:   512,
			fallback:        512,
			want:            256,
		},
		{
			name:            "model config used when loader not set",
			loaderMaxLength: 0,
			configMaxText:   1024,
			fallback:        512,
			want:            1024,
		},
		{
			name:            "fallback used when both are zero",
			loaderMaxLength: 0,
			configMaxText:   0,
			fallback:        512,
			want:            512,
		},
		{
			name:            "loader option overrides even large model config",
			loaderMaxLength: 128,
			configMaxText:   8192,
			fallback:        512,
			want:            128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FirstNonZero(tt.loaderMaxLength, tt.configMaxText, tt.fallback)
			assert.Equal(t, tt.want, got)
		})
	}
}
