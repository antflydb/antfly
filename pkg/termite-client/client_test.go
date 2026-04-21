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

package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serializeFloatArrays writes embeddings in binary format matching the termite server.
// Format: uint64(numVectors) + uint64(dimension) + float32 values in little endian
func serializeFloatArrays(embeddings [][]float32) []byte {
	if len(embeddings) == 0 {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, 0)
		return buf
	}

	dimension := len(embeddings[0])
	// 8 bytes for numVectors + 8 bytes for dimension + 4 bytes per float
	totalSize := 8 + 8 + len(embeddings)*dimension*4
	buf := make([]byte, totalSize)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(embeddings)))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(dimension))

	offset := 16
	for _, vec := range embeddings {
		for _, val := range vec {
			binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32FromFloat32(val))
			offset += 4
		}
	}
	return buf
}

func uint32FromFloat32(f float32) uint32 {
	return math.Float32bits(f)
}

func TestClient_Embed_Binary(t *testing.T) {
	// Mock server that returns binary embeddings
	expectedEmbeddings := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.5, 0.6, 0.7, 0.8},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		assert.Equal(t, "/ml/v1/embed", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		// Parse request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req map[string]any
		err = json.Unmarshal(body, &req)
		require.NoError(t, err)
		assert.Equal(t, "test-model", req["model"])

		// Return binary response (default)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(serializeFloatArrays(expectedEmbeddings))
	}))
	defer server.Close()

	// Create client
	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	// Call Embed
	ctx := context.Background()
	embeddings, err := termiteClient.Embed(ctx, "test-model", []string{"hello", "world"})
	require.NoError(t, err)

	// Verify response
	require.Len(t, embeddings, 2)
	assert.InDeltaSlice(t, expectedEmbeddings[0], embeddings[0], 0.0001)
	assert.InDeltaSlice(t, expectedEmbeddings[1], embeddings[1], 0.0001)
}

func TestClient_Embed_JSON(t *testing.T) {
	// Mock server that returns JSON embeddings when Accept header is set
	expectedEmbeddings := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ml/v1/embed", r.URL.Path)

		// Check if JSON was requested
		acceptHeader := r.Header.Get("Accept")
		if strings.Contains(acceptHeader, "application/json") {
			// Return JSON response
			w.Header().Set("Content-Type", "application/json")
			resp := map[string]any{
				"model":      "test-model",
				"embeddings": expectedEmbeddings,
			}
			_ = json.NewEncoder(w).Encode(resp)
		} else {
			// Return binary response (default)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(serializeFloatArrays(expectedEmbeddings))
		}
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	resp, err := termiteClient.EmbedJSON(ctx, "test-model", []string{"hello", "world"})
	require.NoError(t, err)

	assert.Equal(t, "test-model", resp.Model)
	require.Len(t, resp.Embeddings, 2)
	assert.InDeltaSlice(t, expectedEmbeddings[0], resp.Embeddings[0], 0.0001)
	assert.InDeltaSlice(t, expectedEmbeddings[1], resp.Embeddings[1], 0.0001)
}

func TestClient_Embed_EmptyInput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return empty binary response
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(serializeFloatArrays([][]float32{}))
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	embeddings, err := termiteClient.Embed(ctx, "test-model", []string{})
	require.NoError(t, err)
	assert.Empty(t, embeddings)
}

func TestClient_Embed_ModelNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "model not found: unknown-model"})
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = termiteClient.Embed(ctx, "unknown-model", []string{"hello"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model not found")
}

func TestClient_Embed_BadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "input is required"})
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = termiteClient.Embed(ctx, "test-model", []string{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad request")
}

func TestClient_Chunk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ml/v1/chunk", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		// Parse request
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req map[string]any
		err = json.Unmarshal(body, &req)
		require.NoError(t, err)
		assert.Equal(t, "This is a test document.", req["input"])

		// Return chunks
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"chunks": []map[string]any{
				{"id": 0, "text": "This is a test", "start_char": 0, "end_char": 14},
				{"id": 1, "text": "test document.", "start_char": 10, "end_char": 24},
			},
			"model":     "fixed",
			"cache_hit": false,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	chunks, err := termiteClient.Chunk(ctx, "This is a test document.", ChunkConfig{
		Model:         "fixed",
		TargetTokens:  100,
		OverlapTokens: 10,
	})
	require.NoError(t, err)

	require.Len(t, chunks, 2)
	assert.Equal(t, "This is a test", chunks[0].GetText())
	assert.Equal(t, "test document.", chunks[1].GetText())
}

func TestClient_Chunk_ConfigMapping(t *testing.T) {
	// Verify that ChunkConfig fields are correctly mapped to the nested
	// oapi.ChunkConfig structure (text options under "text" sub-object).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ml/v1/chunk", r.URL.Path)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req map[string]any
		err = json.Unmarshal(body, &req)
		require.NoError(t, err)

		config, ok := req["config"].(map[string]any)
		require.True(t, ok, "config should be an object")

		// Top-level config fields
		assert.Equal(t, "fixed", config["model"])
		assert.EqualValues(t, 5, config["max_chunks"])
		assert.InDelta(t, 0.7, config["threshold"], 0.01)

		// Text-specific fields must be nested under "text"
		textConfig, ok := config["text"].(map[string]any)
		require.True(t, ok, "text config should be a nested object")
		assert.EqualValues(t, 200, textConfig["target_tokens"])
		assert.EqualValues(t, 20, textConfig["overlap_tokens"])
		assert.Equal(t, "\n\n", textConfig["separator"])

		// These fields should NOT appear at the top level
		assert.Nil(t, config["target_tokens"], "target_tokens should not be at top level")
		assert.Nil(t, config["overlap_tokens"], "overlap_tokens should not be at top level")
		assert.Nil(t, config["separator"], "separator should not be at top level")

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"chunks":    []map[string]any{{"id": 0, "text": "chunk one", "start_char": 0, "end_char": 9}},
			"model":     "fixed",
			"cache_hit": false,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	chunks, err := termiteClient.Chunk(context.Background(), "Some long document text.", ChunkConfig{
		Model:         "fixed",
		TargetTokens:  200,
		OverlapTokens: 20,
		Separator:     "\n\n",
		MaxChunks:     5,
		Threshold:     0.7,
	})
	require.NoError(t, err)
	require.Len(t, chunks, 1)
}

func TestClient_ChunkMedia(t *testing.T) {
	// Verify that MediaChunkConfig fields are correctly mapped to the nested
	// oapi.ChunkConfig structure (audio options under "audio" sub-object).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ml/v1/chunk", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req map[string]any
		err = json.Unmarshal(body, &req)
		require.NoError(t, err)

		config, ok := req["config"].(map[string]any)
		require.True(t, ok, "config should be an object")

		// Top-level config fields
		assert.Equal(t, "vad", config["model"])
		assert.EqualValues(t, 10, config["max_chunks"])
		assert.InDelta(t, 0.5, config["threshold"], 0.01)

		// Audio-specific fields must be nested under "audio"
		audioConfig, ok := config["audio"].(map[string]any)
		require.True(t, ok, "audio config should be a nested object")
		assert.EqualValues(t, 30000, audioConfig["window_duration_ms"])
		assert.EqualValues(t, 1000, audioConfig["overlap_duration_ms"])

		// These fields should NOT appear at the top level
		assert.Nil(t, config["window_duration_ms"], "window_duration_ms should not be at top level")
		assert.Nil(t, config["overlap_duration_ms"], "overlap_duration_ms should not be at top level")

		// Verify input contains media data
		input := req["input"]
		require.NotNil(t, input, "input should be present for media chunking")

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"chunks": []map[string]any{
				{"id": 0, "text": "audio segment 1", "start_char": 0, "end_char": 15},
				{"id": 1, "text": "audio segment 2", "start_char": 15, "end_char": 30},
			},
			"model":     "vad",
			"cache_hit": false,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	audioData := []byte("fake-audio-data")

	chunks, err := termiteClient.ChunkMedia(context.Background(), audioData, "audio/wav", MediaChunkConfig{
		Model:             "vad",
		MaxChunks:         10,
		WindowDurationMs:  30000,
		OverlapDurationMs: 1000,
		Threshold:         0.5,
	})
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	assert.Equal(t, "audio segment 1", chunks[0].GetText())
	assert.Equal(t, "audio segment 2", chunks[1].GetText())
}

func TestClient_ChunkMedia_BadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unsupported media type"})
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	_, err = termiteClient.ChunkMedia(context.Background(), []byte("data"), "video/mp4", MediaChunkConfig{
		Model: "vad",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad request")
}

func TestClient_Chunk_EmptyText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "text is required"})
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = termiteClient.Chunk(ctx, "", ChunkConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad request")
}

func TestClient_Rerank(t *testing.T) {
	expectedScores := []float32{0.95, 0.72, 0.45}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ml/v1/rerank", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		// Parse request
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req map[string]any
		err = json.Unmarshal(body, &req)
		require.NoError(t, err)
		assert.Equal(t, "test-reranker", req["model"])
		assert.Equal(t, "what is machine learning?", req["query"])

		prompts := req["prompts"].([]any)
		assert.Len(t, prompts, 3)

		// Return scores
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"model":  "test-reranker",
			"scores": expectedScores,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	scores, err := termiteClient.Rerank(ctx, "test-reranker", "what is machine learning?", []string{
		"Machine learning is a subset of AI...",
		"Deep learning uses neural networks...",
		"Data science involves statistics...",
	})
	require.NoError(t, err)

	require.Len(t, scores, 3)
	assert.InDelta(t, expectedScores[0], scores[0], 0.0001)
	assert.InDelta(t, expectedScores[1], scores[1], 0.0001)
	assert.InDelta(t, expectedScores[2], scores[2], 0.0001)
}

func TestClient_Rerank_ModelNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "model not found: unknown-reranker"})
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = termiteClient.Rerank(ctx, "unknown-reranker", "query", []string{"doc1", "doc2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model not found")
}

func TestClient_Rerank_ServiceUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "reranking not available"})
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = termiteClient.Rerank(ctx, "test-reranker", "query", []string{"doc1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service unavailable")
}

func TestClient_ListModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ml/v1/models", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"embedders":    map[string]any{"bge-small-en-v1.5": map[string]any{}, "clip-vit-base-patch32": map[string]any{"capabilities": []string{"image"}}},
			"chunkers":     map[string]any{"fixed": map[string]any{}, "chonky": map[string]any{}},
			"rerankers":    map[string]any{"bge-reranker-v2-m3": map[string]any{}},
			"generators":   map[string]any{},
			"recognizers":  map[string]any{},
			"extractors":   map[string]any{},
			"rewriters":    map[string]any{},
			"classifiers":  map[string]any{},
			"readers":      map[string]any{},
			"transcribers": map[string]any{},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	models, err := termiteClient.ListModels(ctx)
	require.NoError(t, err)

	assert.Len(t, models.Embedders, 2)
	assert.Contains(t, models.Embedders, "bge-small-en-v1.5")
	assert.Contains(t, models.Embedders, "clip-vit-base-patch32")
	assert.Len(t, models.Chunkers, 2)
	assert.Contains(t, models.Chunkers, "fixed")
	assert.Len(t, models.Rerankers, 1)
	assert.Contains(t, models.Rerankers, "bge-reranker-v2-m3")
}

func TestClient_GetVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ml/v1/version", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]string{
			"version":    "v1.2.3",
			"git_commit": "abc123def",
			"build_time": "2025-01-15T10:00:00Z",
			"go_version": "go1.23.0",
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	version, err := termiteClient.GetVersion(ctx)
	require.NoError(t, err)

	assert.Equal(t, "v1.2.3", version.Version)
	assert.Equal(t, "abc123def", version.GitCommit)
	assert.Equal(t, "2025-01-15T10:00:00Z", version.BuildTime)
	assert.Equal(t, "go1.23.0", version.GoVersion)
}

func TestClient_CustomHTTPClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"embedders":    map[string]any{},
			"chunkers":     map[string]any{"fixed": map[string]any{}},
			"rerankers":    map[string]any{},
			"generators":   map[string]any{},
			"recognizers":  map[string]any{},
			"extractors":   map[string]any{},
			"rewriters":    map[string]any{},
			"classifiers":  map[string]any{},
			"readers":      map[string]any{},
			"transcribers": map[string]any{},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create client with custom timeout
	customHTTPClient := &http.Client{Timeout: 5 * time.Second}
	termiteClient, err := NewTermiteClient(server.URL, customHTTPClient)
	require.NoError(t, err)

	ctx := context.Background()
	models, err := termiteClient.ListModels(ctx)
	require.NoError(t, err)
	assert.Len(t, models.Chunkers, 1)
	assert.Contains(t, models.Chunkers, "fixed")
}

func TestClient_ContextCancellation(t *testing.T) {
	// Server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = termiteClient.Embed(ctx, "test-model", []string{"hello"})
	require.Error(t, err)
	// Error should be context-related
	assert.True(t, strings.Contains(err.Error(), "context") ||
		strings.Contains(err.Error(), "deadline") ||
		strings.Contains(err.Error(), "cancel"))
}

func TestClient_ServerErr(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal server error"})
	}))
	defer server.Close()

	termiteClient, err := NewTermiteClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = termiteClient.Embed(ctx, "test-model", []string{"hello"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server error")
}

func TestClient_URLNormalization(t *testing.T) {
	// Test that trailing slash is handled correctly
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify no double slashes
		assert.NotContains(t, r.URL.Path, "//")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedders":    map[string]any{},
			"chunkers":     map[string]any{},
			"rerankers":    map[string]any{},
			"generators":   map[string]any{},
			"recognizers":  map[string]any{},
			"extractors":   map[string]any{},
			"rewriters":    map[string]any{},
			"classifiers":  map[string]any{},
			"readers":      map[string]any{},
			"transcribers": map[string]any{},
		})
	}))
	defer server.Close()

	// Test with trailing slash
	termiteClient, err := NewTermiteClient(server.URL+"/", nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = termiteClient.ListModels(ctx)
	require.NoError(t, err)
}
