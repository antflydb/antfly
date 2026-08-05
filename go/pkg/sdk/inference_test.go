// Copyright 2026 Antfly, Inc.
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

package sdk

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type inferenceRoundTripFunc func(*http.Request) (*http.Response, error)

func (f inferenceRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type infiniteZeroReader struct{}

func (infiniteZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

type closeTrackingReader struct {
	io.Reader
	closed bool
}

func (r *closeTrackingReader) Close() error {
	r.closed = true
	return nil
}

// serializeFloatArrays writes embeddings in binary format matching the inference server.
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
	// Legacy servers may still return binary embeddings.
	expectedEmbeddings := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.5, 0.6, 0.7, 0.8},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		assert.Equal(t, "/ai/v1/embed", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		// Parse request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req map[string]any
		err = json.Unmarshal(body, &req)
		require.NoError(t, err)
		assert.Equal(t, "test-model", req["model"])

		// Return the legacy dense binary response.
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(serializeFloatArrays(expectedEmbeddings))
	}))
	defer server.Close()

	// Create client
	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	// Call Embed
	ctx := context.Background()
	embeddings, err := inferenceClient.Embed(ctx, "test-model", []string{"hello", "world"})
	require.NoError(t, err)

	// Verify response
	require.Len(t, embeddings, 2)
	assert.InDeltaSlice(t, expectedEmbeddings[0], embeddings[0], 0.0001)
	assert.InDeltaSlice(t, expectedEmbeddings[1], embeddings[1], 0.0001)
}

func TestClient_Embed_CurrentJSONResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"model":  "test-model",
			"data": []map[string]any{
				{"object": "embedding", "index": 0, "embedding": []float32{0.1, 0.2}},
			},
			"usage": map[string]any{"prompt_tokens": 1, "total_tokens": 1},
		})
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	embeddings, err := client.Embed(context.Background(), "test-model", []string{"hello"})
	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	assert.InDeltaSlice(t, []float32{0.1, 0.2}, embeddings[0], 0.0001)
}

func TestClient_EmbedRejectsUnregisteredBinaryMediaType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-streamx")
		_, _ = w.Write(serializeFloatArrays([][]float32{{0.1}}))
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	_, err = client.Embed(context.Background(), "test-model", []string{"hello"})
	require.ErrorContains(t, err, `unexpected embedding response content type "application/octet-streamx"`)
}

func TestClient_Embed_JSON(t *testing.T) {
	// Mock server that returns JSON embeddings when Accept header is set
	expectedEmbeddings := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/embed", r.URL.Path)

		// Check if JSON was requested
		acceptHeader := r.Header.Get("Accept")
		if strings.Contains(acceptHeader, "application/json") {
			// Return JSON response
			w.Header().Set("Content-Type", "application/json")
			resp := map[string]any{
				"object": "list",
				"model":  "test-model",
				"data": []map[string]any{
					{"object": "embedding", "index": 0, "embedding": expectedEmbeddings[0]},
					{"object": "embedding", "index": 1, "embedding": expectedEmbeddings[1]},
				},
				"usage": map[string]any{"prompt_tokens": 2, "total_tokens": 2},
			}
			_ = json.NewEncoder(w).Encode(resp)
		} else {
			// Return binary response (default)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(serializeFloatArrays(expectedEmbeddings))
		}
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	resp, err := inferenceClient.EmbedJSON(ctx, "test-model", []string{"hello", "world"})
	require.NoError(t, err)

	assert.Equal(t, "test-model", resp.Model)
	embeddings, err := denseEmbeddings(resp)
	require.NoError(t, err)
	require.Len(t, embeddings, 2)
	assert.InDeltaSlice(t, expectedEmbeddings[0], embeddings[0], 0.0001)
	assert.InDeltaSlice(t, expectedEmbeddings[1], embeddings[1], 0.0001)
}

func TestClient_Embed_EmptyInput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return empty binary response
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(serializeFloatArrays([][]float32{}))
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	embeddings, err := inferenceClient.Embed(ctx, "test-model", []string{})
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = inferenceClient.Embed(ctx, "unknown-model", []string{"hello"})
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = inferenceClient.Embed(ctx, "test-model", []string{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad request")
}

func TestClient_Embed_TransientCapacityError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":          "MODEL_RESOURCE_BUSY",
			"message":        "insufficient inference capacity is currently available",
			"reason":         "inference_capacity",
			"retryable":      true,
			"retry_after_ms": 1000,
		})
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	_, err = inferenceClient.Embed(context.Background(), "busy-model", []string{"hello"})
	require.Error(t, err)
	var capacityErr *InferenceCapacityError
	require.True(t, errors.As(err, &capacityErr))
	assert.Equal(t, "MODEL_RESOURCE_BUSY", capacityErr.Code)
	assert.Equal(t, "inference_capacity", capacityErr.Reason)
	assert.Equal(t, time.Second, capacityErr.RetryAfter)
	assert.True(t, capacityErr.Temporary())
}

func TestClient_Embed_NonRetryableCapacityBodyIsNotTemporary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":          "MODEL_RESOURCE_BUSY",
			"message":        "request cannot be retried",
			"reason":         "inference_capacity",
			"retryable":      false,
			"retry_after_ms": 1000,
		})
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	_, err = inferenceClient.Embed(context.Background(), "busy-model", []string{"hello"})
	require.Error(t, err)
	var capacityErr *InferenceCapacityError
	assert.False(t, errors.As(err, &capacityErr))
	assert.Contains(t, err.Error(), "service unavailable")
}

func TestClient_Chunk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/chunk", r.URL.Path)
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	chunks, err := inferenceClient.Chunk(ctx, "This is a test document.", ChunkConfig{
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
	// inference chunk config structure (text options under "text" sub-object).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/chunk", r.URL.Path)

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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	chunks, err := inferenceClient.Chunk(context.Background(), "Some long document text.", ChunkConfig{
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
	// inference chunk config structure (audio options under "audio" sub-object).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/chunk", r.URL.Path)
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	audioData := []byte("fake-audio-data")

	chunks, err := inferenceClient.ChunkMedia(context.Background(), audioData, "audio/wav", MediaChunkConfig{
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	_, err = inferenceClient.ChunkMedia(context.Background(), []byte("data"), "video/mp4", MediaChunkConfig{
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = inferenceClient.Chunk(ctx, "", ChunkConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad request")
}

func TestClient_ExtractClassifications(t *testing.T) {
	var gotPath string
	var gotReq struct {
		Model  string `json:"model"`
		Inputs []struct {
			Content string `json:"content"`
		} `json:"inputs"`
		Schema struct {
			Classifications []struct {
				Name   string   `json:"name"`
				Labels []string `json:"labels"`
			} `json:"classifications"`
		} `json:"schema"`
		Options struct {
			IncludeConfidence bool `json:"include_confidence"`
		} `json:"options"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"object": "extraction",
			"model":  "intent-model",
			"data": []map[string]any{{
				"classifications": []map[string]any{{
					"name": "classification", "label": "visual", "score": 0.9,
				}},
			}},
		}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	got, err := inferenceClient.ExtractClassifications(
		context.Background(),
		"intent-model",
		[]string{"find images"},
		[]string{"filename", "visual"},
	)
	require.NoError(t, err)

	assert.Equal(t, "/ai/v1/extract", gotPath)
	assert.Equal(t, "intent-model", gotReq.Model)
	require.Len(t, gotReq.Inputs, 1)
	assert.Equal(t, "find images", gotReq.Inputs[0].Content)
	require.Len(t, gotReq.Schema.Classifications, 1)
	assert.Equal(t, "classification", gotReq.Schema.Classifications[0].Name)
	assert.Equal(t, []string{"filename", "visual"}, gotReq.Schema.Classifications[0].Labels)
	assert.True(t, gotReq.Options.IncludeConfidence)

	require.Len(t, got, 1)
	require.Len(t, got[0].Classifications, 1)
	assert.Equal(t, Classification{Name: "classification", Label: "visual", Score: 0.9}, got[0].Classifications[0])
}

func TestClient_ExtractEntities(t *testing.T) {
	var gotPath string
	var gotReq struct {
		Model  string `json:"model"`
		Schema struct {
			Entities []string `json:"entities"`
		} `json:"schema"`
		Options struct {
			FlatNer           bool `json:"flat_ner"`
			IncludeConfidence bool `json:"include_confidence"`
			IncludeSpans      bool `json:"include_spans"`
		} `json:"options"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"object": "extraction",
			"model":  "ner-model",
			"data": []map[string]any{{
				"entities": []map[string]any{{
					"text": "Antfly", "label": "company", "score": 0.95, "start": 0, "end": 6,
				}},
			}},
		}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	got, err := inferenceClient.ExtractEntities(
		context.Background(),
		"ner-model",
		[]string{"Antfly ships SearchAF."},
		[]string{"company"},
	)
	require.NoError(t, err)

	assert.Equal(t, "/ai/v1/extract", gotPath)
	assert.Equal(t, "ner-model", gotReq.Model)
	assert.Equal(t, []string{"company"}, gotReq.Schema.Entities)
	assert.True(t, gotReq.Options.FlatNer)
	assert.True(t, gotReq.Options.IncludeConfidence)
	assert.True(t, gotReq.Options.IncludeSpans)

	require.Len(t, got, 1)
	require.Len(t, got[0].Entities, 1)
	assert.Equal(t, ExtractedEntity{
		Text: "Antfly", Label: "company", Score: 0.95, Start: 0, End: 6,
	}, got[0].Entities[0])
}

func TestClient_Rerank(t *testing.T) {
	expectedScores := []float32{0.95, 0.72, 0.45}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/rerank", r.URL.Path)
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
			"object": "list",
			"model":  "test-reranker",
			"data": []map[string]any{
				{"object": "rerank.score", "index": 0, "score": expectedScores[0]},
				{"object": "rerank.score", "index": 1, "score": expectedScores[1]},
				{"object": "rerank.score", "index": 2, "score": expectedScores[2]},
			},
			"usage": map[string]any{"prompt_tokens": 3, "completion_tokens": 0, "total_tokens": 3},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	scores, err := inferenceClient.Rerank(ctx, "test-reranker", "what is machine learning?", []string{
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = inferenceClient.Rerank(ctx, "unknown-reranker", "query", []string{"doc1", "doc2"})
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = inferenceClient.Rerank(ctx, "test-reranker", "query", []string{"doc1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service unavailable")
}

func TestClient_ListModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/models", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"embedders":    map[string]any{"bge-small-en-v1.5": map[string]any{}, "clip-vit-base-patch32": map[string]any{"capabilities": []string{"image"}}},
			"chunkers":     map[string]any{"fixed": map[string]any{}, "chonky": map[string]any{}},
			"rerankers":    map[string]any{"bge-reranker-v2-m3": map[string]any{}},
			"generators":   map[string]any{},
			"extractors":   map[string]any{},
			"rewriters":    map[string]any{},
			"classifiers":  map[string]any{},
			"readers":      map[string]any{},
			"transcribers": map[string]any{},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	models, err := inferenceClient.ListModels(ctx)
	require.NoError(t, err)

	assert.Len(t, models.Embedders, 2)
	assert.Contains(t, models.Embedders, "bge-small-en-v1.5")
	assert.Contains(t, models.Embedders, "clip-vit-base-patch32")
	assert.Len(t, models.Chunkers, 2)
	assert.Contains(t, models.Chunkers, "fixed")
	assert.Len(t, models.Rerankers, 1)
	assert.Contains(t, models.Rerankers, "bge-reranker-v2-m3")
}

func TestClient_CustomHTTPClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"embedders":    map[string]any{},
			"chunkers":     map[string]any{"fixed": map[string]any{}},
			"rerankers":    map[string]any{},
			"generators":   map[string]any{},
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
	inferenceClient, err := NewInferenceClient(server.URL, customHTTPClient)
	require.NoError(t, err)

	ctx := context.Background()
	models, err := inferenceClient.ListModels(ctx)
	require.NoError(t, err)
	assert.Len(t, models.Chunkers, 1)
	assert.Contains(t, models.Chunkers, "fixed")
}

func TestInferenceResponseLimitsApplyBeforeGeneratedParsing(t *testing.T) {
	for _, path := range []string{
		"/ai/v1",
		"/ai/v1/generate",
		"/ml/v1/predict",
		"/cloud/v1/instance/ai/v1/generate",
		"/cloud/v1/instance/ml/v1/predict",
	} {
		assert.True(t, isInferenceAPIPath(path), path)
	}

	tests := []struct {
		name   string
		status int
		limit  int64
	}{
		{name: "error", status: http.StatusInternalServerError, limit: maxErrorResponseBytes},
		{name: "json success", status: http.StatusOK, limit: maxInferenceJSONResponseBytes},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			httpClient := &http.Client{Transport: inferenceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: test.status,
					Header:     http.Header{"Content-Type": {"application/json"}},
					Body:       io.NopCloser(io.LimitReader(infiniteZeroReader{}, test.limit+1)),
					Request:    req,
				}, nil
			})}
			client, err := NewInferenceClient("http://inference.test", httpClient)
			require.NoError(t, err)

			_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, nil)
			require.ErrorContains(t, err, fmt.Sprintf("inference response exceeded %d bytes", test.limit))
		})
	}

	jsonReq, err := http.NewRequest(http.MethodPost, "http://inference.test/ai/v1/embed", nil)
	require.NoError(t, err)
	for _, contentType := range []string{"application/octet-stream", "application/x-sparse-vectors"} {
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}}
		assert.Equal(t, maxInferenceBinaryResponseBytes, inferenceResponseLimit(jsonReq, resp), contentType)
	}
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}}}
	assert.Equal(t, maxInferenceJSONResponseBytes, inferenceResponseLimit(jsonReq, resp))
	sseReq := jsonReq.Clone(context.Background())
	sseReq.Header.Set("Accept", "application/json, text/event-stream; q=1")
	assert.Zero(t, inferenceResponseLimit(sseReq, resp))
	assert.Equal(t, "text/event-stream", inferenceMediaType(resp.Header.Get("Content-Type")))
	assert.Equal(t, "application/json", inferenceMediaType(`application/json; note="text/event-stream"`))
	assert.Empty(t, inferenceMediaType("application/json; note=text/event-stream"))
	assert.Empty(t, inferenceMediaType("not a media type"))

	for _, contentType := range []string{
		"application/octet-streamx",
		"application/json; note=text/event-stream",
		"not a media type",
		"",
	} {
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}}
		assert.Equal(t, maxInferenceJSONResponseBytes, inferenceResponseLimit(jsonReq, resp), contentType)
	}

	for _, path := range []string{"/db/v1/query", "/auth/v1/users", "/ai/v10/generate"} {
		originalBody := io.NopCloser(strings.NewReader("{}"))
		httpClient := &http.Client{Transport: inferenceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       originalBody,
				Request:    req,
			}, nil
		})}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://antfly.test"+path, nil)
		require.NoError(t, err)
		resp, err := (inferenceResponseLimitDoer{next: httpClient}).Do(req)
		require.NoError(t, err)
		_, limited := resp.Body.(*inferenceLimitedBody)
		assert.False(t, limited, path)
	}
}

func TestInferenceJSONRequestCapsSuccessfulSSEMislabeledBody(t *testing.T) {
	body := &closeTrackingReader{Reader: io.LimitReader(infiniteZeroReader{}, maxInferenceJSONResponseBytes+1)}
	httpClient := &http.Client{Transport: inferenceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		require.NotContains(t, req.Header.Get("Accept"), "text/event-stream")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"text/event-stream"}},
			Body:       body,
			Request:    req,
		}, nil
	})}
	client, err := NewInferenceClient("http://inference.test", httpClient)
	require.NoError(t, err)

	_, err = client.Embed(context.Background(), "target", []string{"hello"})
	require.ErrorContains(t, err, fmt.Sprintf("inference response exceeded %d bytes", maxInferenceJSONResponseBytes))
	assert.True(t, body.closed, "generated response parser must close the rejected body")
}

func TestInferenceBinaryResponseLimitRejectsOversizedBody(t *testing.T) {
	httpClient := &http.Client{Transport: inferenceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/octet-stream"}},
			Body:       io.NopCloser(io.LimitReader(infiniteZeroReader{}, maxInferenceBinaryResponseBytes+1)),
			Request:    req,
		}, nil
	})}
	client, err := NewInferenceClient("http://inference.test", httpClient)
	require.NoError(t, err)

	_, err = client.Embed(context.Background(), "target", []string{"hello"})
	require.ErrorContains(t, err, fmt.Sprintf("inference response exceeded %d bytes", maxInferenceBinaryResponseBytes))
}

func TestDeserializeFloatArraysRejectsForgedHeaders(t *testing.T) {
	for _, test := range []struct {
		name string
		body []byte
	}{
		{name: "missing count", body: make([]byte, 7)},
		{name: "trailing empty payload", body: append(make([]byte, 8), 0)},
		{name: "missing dimension", body: func() []byte {
			body := make([]byte, 8)
			binary.LittleEndian.PutUint64(body, 1)
			return body
		}()},
		{name: "multiplication overflow", body: func() []byte {
			body := make([]byte, 16)
			binary.LittleEndian.PutUint64(body[0:8], ^uint64(0))
			binary.LittleEndian.PutUint64(body[8:16], 2)
			return body
		}()},
		{name: "declared payload mismatch", body: func() []byte {
			body := make([]byte, 16)
			binary.LittleEndian.PutUint64(body[0:8], 1)
			binary.LittleEndian.PutUint64(body[8:16], 1)
			return body
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := deserializeFloatArrays(test.body)
			require.Error(t, err)
		})
	}
}

func TestDeserializeSparseVectorsValidatesBeforeAllocation(t *testing.T) {
	forgedCount := make([]byte, 8)
	binary.LittleEndian.PutUint64(forgedCount, ^uint64(0))
	_, err := deserializeSparseVectors(forgedCount)
	require.Error(t, err)

	forgedNNZ := make([]byte, 12)
	binary.LittleEndian.PutUint64(forgedNNZ[0:8], 1)
	binary.LittleEndian.PutUint32(forgedNNZ[8:12], ^uint32(0))
	_, err = deserializeSparseVectors(forgedNNZ)
	require.ErrorContains(t, err, "beyond payload")

	valid := make([]byte, 28)
	binary.LittleEndian.PutUint64(valid[0:8], 1)
	binary.LittleEndian.PutUint32(valid[8:12], 2)
	binary.LittleEndian.PutUint32(valid[12:16], uint32(3))
	binary.LittleEndian.PutUint32(valid[16:20], uint32(7))
	binary.LittleEndian.PutUint32(valid[20:24], math.Float32bits(0.25))
	binary.LittleEndian.PutUint32(valid[24:28], math.Float32bits(0.75))
	vectors, err := deserializeSparseVectors(valid)
	require.NoError(t, err)
	require.Len(t, vectors, 1)
	assert.Equal(t, []int32{3, 7}, vectors[0].Indices)
	assert.InDeltaSlice(t, []float32{0.25, 0.75}, vectors[0].Values, 0.0001)
}

func TestClient_SparseEmbed_LegacyBinaryMediaType(t *testing.T) {
	body := make([]byte, 20)
	binary.LittleEndian.PutUint64(body[0:8], 1)
	binary.LittleEndian.PutUint32(body[8:12], 1)
	binary.LittleEndian.PutUint32(body[12:16], 7)
	binary.LittleEndian.PutUint32(body[16:20], math.Float32bits(0.75))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-sparse-vectors")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	vectors, err := client.SparseEmbed(context.Background(), "sparse-model", []string{"hello"})
	require.NoError(t, err)
	require.Len(t, vectors, 1)
	assert.Equal(t, []int32{7}, vectors[0].Indices)
	assert.InDeltaSlice(t, []float32{0.75}, vectors[0].Values, 0.0001)
}

func TestClient_ContextCancellation(t *testing.T) {
	// Server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = inferenceClient.Embed(ctx, "test-model", []string{"hello"})
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

	inferenceClient, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = inferenceClient.Embed(ctx, "test-model", []string{"hello"})
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
			"extractors":   map[string]any{},
			"rewriters":    map[string]any{},
			"classifiers":  map[string]any{},
			"readers":      map[string]any{},
			"transcribers": map[string]any{},
		})
	}))
	defer server.Close()

	// Test with trailing slash
	inferenceClient, err := NewInferenceClient(server.URL+"/", nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = inferenceClient.ListModels(ctx)
	require.NoError(t, err)
}

func TestClient_Generate_Speculation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/generate", r.URL.Path)
		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "draft", req["draft_model"])
		assert.Equal(t, float64(3), req["speculative_k"])
		assert.Equal(t, "auto", req["speculation_policy"])
		assert.Equal(t, "probe", req["speculation_calibration"])

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-test", "object": "chat.completion", "created": 1, "model": "target",
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "stop",
				"message": map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage":       map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			"speculation": map[string]any{"policy": "auto", "calibration": "probe", "decision": "active"},
		})
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	resp, err := client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, &GenerateConfig{
		DraftModel:             "draft",
		SpeculativeK:           3,
		SpeculationPolicy:      oapi.InferenceGenerateRequestSpeculationPolicyAuto,
		SpeculationCalibration: oapi.InferenceGenerateRequestSpeculationCalibrationProbe,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Speculation)
	assert.Equal(t, "active", resp.Speculation.Decision)

	var withoutSpeculation oapi.InferenceGenerateResponse
	require.NoError(t, json.Unmarshal([]byte(`{"speculation":null}`), &withoutSpeculation))
	assert.Nil(t, withoutSpeculation.Speculation)
}

func TestInferenceGenerateRequest_OmitsSamplingAndSpeculationDefaults(t *testing.T) {
	encoded, err := json.Marshal(oapi.InferenceGenerateRequest{
		Model:    "target",
		Messages: []oapi.InferenceChatMessage{NewUserMessage("hello")},
	})
	require.NoError(t, err)

	var request map[string]any
	require.NoError(t, json.Unmarshal(encoded, &request))
	for _, field := range []string{"temperature", "top_p", "top_k", "speculative_k"} {
		assert.NotContains(t, request, field)
	}
}

func TestClient_Generate_PreservesExplicitThinkingFalse(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		requests = append(requests, request)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-test","object":"chat.completion","created":1,"model":"target","choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`)
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, nil)
	require.NoError(t, err)
	enableThinking := false
	_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, &GenerateConfig{
		EnableThinking: &enableThinking,
	})
	require.NoError(t, err)

	require.Len(t, requests, 2)
	assert.NotContains(t, requests[0], "chat_template_kwargs")
	assert.Equal(t, map[string]any{"enable_thinking": false}, requests[1]["chat_template_kwargs"])
}

func TestClient_Generate_ExplicitSamplingZeroOverrides(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		requests = append(requests, request)

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-test","object":"chat.completion","created":1,"model":"target","choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`)
	}))
	defer server.Close()

	zeroFloat := float32(0)
	zeroInt := 0
	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, &GenerateConfig{
		Temperature: 0.7,
		TopP:        0.9,
		TopK:        50,
	})
	require.NoError(t, err)
	_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, &GenerateConfig{
		Temperature:         0.7,
		TemperatureOverride: &zeroFloat,
		TopP:                0.9,
		TopPOverride:        &zeroFloat,
		TopK:                50,
		TopKOverride:        &zeroInt,
	})
	require.NoError(t, err)
	require.Len(t, requests, 2)
	assert.InDelta(t, 0.7, requests[0]["temperature"], 0.0001)
	assert.InDelta(t, 0.9, requests[0]["top_p"], 0.0001)
	assert.Equal(t, float64(50), requests[0]["top_k"])
	assert.Equal(t, float64(0), requests[1]["temperature"])
	assert.Equal(t, float64(0), requests[1]["top_p"])
	assert.Equal(t, float64(0), requests[1]["top_k"])
}

func TestClient_Generate_PreservesErrorCodeAndMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"INVALID_REQUEST","message":"temperature must be between 0 and 2","retryable":false}`)
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, nil)
	require.EqualError(t, err, "bad request: temperature must be between 0 and 2 (INVALID_REQUEST)")
}

func TestInferenceErrorDetail(t *testing.T) {
	tests := []struct {
		name string
		err  oapi.InferenceError
		want string
	}{
		{name: "code only", err: oapi.InferenceError{Error: "INVALID_REQUEST"}, want: "INVALID_REQUEST"},
		{name: "message only", err: oapi.InferenceError{Message: "invalid request"}, want: "invalid request"},
		{name: "same code and message", err: oapi.InferenceError{Error: "invalid request", Message: "invalid request"}, want: "invalid request"},
		{name: "code and detail", err: oapi.InferenceError{Error: "INVALID_REQUEST", Message: "invalid request"}, want: "invalid request (INVALID_REQUEST)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, inferenceErrorDetail(&tt.err))
		})
	}
}

func TestClient_Generate_MemoryBudgetExceeded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInsufficientStorage)
		_, _ = io.WriteString(w, `{"error":"MEMORY_BUDGET_EXCEEDED","message":"model needs 8 GiB","retryable":true}`)
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, nil)
	require.NoError(t, err)
	_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, nil)
	require.EqualError(t, err, "memory budget exceeded: model needs 8 GiB (MEMORY_BUDGET_EXCEEDED)")
	var apiErr *InferenceAPIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusInsufficientStorage, apiErr.StatusCode)
	assert.Equal(t, "MEMORY_BUDGET_EXCEEDED", apiErr.Code)
	assert.Equal(t, "model needs 8 GiB (MEMORY_BUDGET_EXCEEDED)", apiErr.Message)
	require.NotNil(t, apiErr.Retryable)
	assert.True(t, *apiErr.Retryable)
}

func TestInferenceResponseErrorPreservesUnknownRetryability(t *testing.T) {
	err := inferenceResponseError(http.StatusServiceUnavailable, []byte(`{"error":"not_ready"}`))
	var apiErr *InferenceAPIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "not_ready", apiErr.Code)
	assert.Nil(t, apiErr.Retryable)
}

func TestClient_Generate_PreservesUnifiedAuthAndRemoteContentErrors(t *testing.T) {
	tests := []struct {
		status int
		code   string
	}{
		{status: http.StatusUnauthorized, code: "UNAUTHORIZED"},
		{status: http.StatusBadRequest, code: "INVALID_CONTENT_URL"},
		{status: http.StatusForbidden, code: "CONTENT_NOT_ALLOWED"},
		{status: http.StatusRequestEntityTooLarge, code: "CONTENT_TOO_LARGE"},
		{status: http.StatusBadGateway, code: "CONTENT_FETCH_FAILED"},
		{status: http.StatusServiceUnavailable, code: "not_ready"},
	}

	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_, _ = fmt.Fprintf(w, `{"error":%q,"message":"request rejected","retryable":false}`, test.code)
			}))
			defer server.Close()

			client, err := NewInferenceClient(server.URL, nil)
			require.NoError(t, err)
			_, err = client.Generate(context.Background(), "target", []oapi.InferenceChatMessage{NewUserMessage("hello")}, nil)
			var apiErr *InferenceAPIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, test.status, apiErr.StatusCode)
			assert.Equal(t, test.code, apiErr.Code)
			require.NotNil(t, apiErr.Retryable)
			assert.False(t, *apiErr.Retryable)
		})
	}
}
