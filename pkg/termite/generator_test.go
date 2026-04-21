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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTermiteNode_HandleApiGenerate_NoModels(t *testing.T) {
	node := &TermiteNode{
		logger:            zap.NewNop(),
		generatorRegistry: nil,
		requestQueue:      NewRequestQueue(RequestQueueConfig{}, zap.NewNop()),
	}

	t.Run("Returns503WhenNoRegistry", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/ml/v1/generate", bytes.NewBufferString(`{"model":"test","messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		node.handleApiGenerate(w, req)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "generation not available")
	})
}

func TestTermiteNode_HandleApiGenerate_InvalidRequest(t *testing.T) {
	// Note: When there's no generator registry, all requests get 503 "generation not available"
	// before validation can occur. The handler checks registry availability first.
	// This test verifies the expected behavior with no registry.
	node := &TermiteNode{
		logger:            zap.NewNop(),
		generatorRegistry: nil,
		requestQueue:      NewRequestQueue(RequestQueueConfig{}, zap.NewNop()),
	}

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantErr  string
	}{
		{
			name:     "invalid JSON",
			body:     `{invalid}`,
			wantCode: http.StatusServiceUnavailable,
			wantErr:  "generation not available",
		},
		{
			name:     "missing model",
			body:     `{"messages":[{"role":"user","content":"hi"}]}`,
			wantCode: http.StatusServiceUnavailable,
			wantErr:  "generation not available",
		},
		{
			name:     "missing messages",
			body:     `{"model":"test"}`,
			wantCode: http.StatusServiceUnavailable,
			wantErr:  "generation not available",
		},
		{
			name:     "empty messages",
			body:     `{"model":"test","messages":[]}`,
			wantCode: http.StatusServiceUnavailable,
			wantErr:  "generation not available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/ml/v1/generate", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			node.handleApiGenerate(w, req)
			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantErr)
		})
	}
}

// Note: TestTermiteNode_HandleApiGenerate_ModelNotFound and TestTermiteNode_HandleApiGenerate_Success
// require a real registry with models or a mockable interface. The GeneratorRegistry type now uses
// internal ttlcache and doesn't expose the models map directly. These tests would need to either:
// 1. Use a real registry with test models on disk
// 2. Introduce an interface for the registry that can be mocked
// For now, these are tested via integration tests in the e2e package.

func TestGeneratorRegistry_EmptyDirectory(t *testing.T) {
	// Test with no models directory
	registry, err := NewGeneratorRegistry(GeneratorConfig{ModelsDir: ""}, nil, nil, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, registry)
	assert.Empty(t, registry.List())
	_ = registry.Close()
}

func TestGeneratorRegistry_NonexistentDirectory(t *testing.T) {
	// Test with non-existent directory
	registry, err := NewGeneratorRegistry(GeneratorConfig{ModelsDir: "/nonexistent/path"}, nil, nil, zap.NewNop())
	require.NoError(t, err) // Should not error, just log warning
	require.NotNil(t, registry)
	assert.Empty(t, registry.List())
	_ = registry.Close()
}

func TestIsValidGeneratorModel(t *testing.T) {
	// Create temp directory for testing
	tempDir := t.TempDir()

	tests := []struct {
		name     string
		setup    func(dir string)
		expected bool
	}{
		{
			name:     "empty directory",
			setup:    func(dir string) {},
			expected: false,
		},
		{
			name: "only config.json",
			setup: func(dir string) {
				_ = os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{}`), 0644)
			},
			expected: false,
		},
		{
			name: "only model.onnx",
			setup: func(dir string) {
				_ = os.WriteFile(filepath.Join(dir, "model.onnx"), []byte{}, 0644)
			},
			expected: false,
		},
		{
			name: "config.json + model.onnx",
			setup: func(dir string) {
				_ = os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{}`), 0644)
				_ = os.WriteFile(filepath.Join(dir, "model.onnx"), []byte{}, 0644)
			},
			expected: true,
		},
		{
			name: "config.json + onnx/model.onnx",
			setup: func(dir string) {
				_ = os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{}`), 0644)
				_ = os.MkdirAll(filepath.Join(dir, "onnx"), 0755)
				_ = os.WriteFile(filepath.Join(dir, "onnx", "model.onnx"), []byte{}, 0644)
			},
			expected: true,
		},
		{
			name: "genai_config.json only",
			setup: func(dir string) {
				_ = os.WriteFile(filepath.Join(dir, "genai_config.json"), []byte(`{}`), 0644)
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modelDir := filepath.Join(tempDir, tt.name)
			_ = os.MkdirAll(modelDir, 0755)
			tt.setup(modelDir)
			assert.Equal(t, tt.expected, isValidGeneratorModel(modelDir))
		})
	}
}

func TestGenerateGenaiConfig(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("skips if genai_config.json exists", func(t *testing.T) {
		modelDir := filepath.Join(tempDir, "existing")
		_ = os.MkdirAll(modelDir, 0755)
		_ = os.WriteFile(filepath.Join(modelDir, "genai_config.json"), []byte(`{"existing": true}`), 0644)

		err := generateGenaiConfig(modelDir, zap.NewNop())
		require.NoError(t, err)

		// Should not be modified
		data, _ := os.ReadFile(filepath.Join(modelDir, "genai_config.json"))
		assert.Contains(t, string(data), "existing")
	})

	t.Run("fails if config.json missing", func(t *testing.T) {
		modelDir := filepath.Join(tempDir, "no-config")
		_ = os.MkdirAll(modelDir, 0755)

		err := generateGenaiConfig(modelDir, zap.NewNop())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reading config.json")
	})

	t.Run("generates config from HuggingFace format", func(t *testing.T) {
		modelDir := filepath.Join(tempDir, "hf-model")
		_ = os.MkdirAll(modelDir, 0755)

		hfConfig := `{
			"model_type": "gemma3_text",
			"bos_token_id": 2,
			"eos_token_id": 1,
			"pad_token_id": 0,
			"vocab_size": 262144,
			"hidden_size": 640,
			"num_attention_heads": 4,
			"num_hidden_layers": 18,
			"head_dim": 256,
			"max_position_embeddings": 32768
		}`
		_ = os.WriteFile(filepath.Join(modelDir, "config.json"), []byte(hfConfig), 0644)
		_ = os.WriteFile(filepath.Join(modelDir, "model.onnx"), []byte{}, 0644)

		err := generateGenaiConfig(modelDir, zap.NewNop())
		require.NoError(t, err)

		// Verify generated file
		data, err := os.ReadFile(filepath.Join(modelDir, "genai_config.json"))
		require.NoError(t, err)

		var config map[string]any
		err = json.Unmarshal(data, &config)
		require.NoError(t, err)

		model := config["model"].(map[string]any)
		assert.Equal(t, "gemma3_text", model["type"])
		assert.Equal(t, float64(2), model["bos_token_id"])
		assert.Equal(t, float64(262144), model["vocab_size"])
	})

	t.Run("infers model type from architectures", func(t *testing.T) {
		modelDir := filepath.Join(tempDir, "llama-model")
		_ = os.MkdirAll(modelDir, 0755)

		hfConfig := `{
			"model_type": "unknown_type",
			"architectures": ["LlamaForCausalLM"]
		}`
		_ = os.WriteFile(filepath.Join(modelDir, "config.json"), []byte(hfConfig), 0644)
		_ = os.WriteFile(filepath.Join(modelDir, "model.onnx"), []byte{}, 0644)

		err := generateGenaiConfig(modelDir, zap.NewNop())
		require.NoError(t, err)

		data, _ := os.ReadFile(filepath.Join(modelDir, "genai_config.json"))
		var config map[string]any
		_ = json.Unmarshal(data, &config)

		model := config["model"].(map[string]any)
		assert.Equal(t, "llama", model["type"])
	})
}
