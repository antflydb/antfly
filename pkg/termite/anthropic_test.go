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
	"bufio"
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/pkg/termite/lib/generation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// --- Mock helpers ---

type mockAnthropicGenerator struct {
	generateFunc func(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (*generation.GenerateResult, error)
}

func (m *mockAnthropicGenerator) Generate(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (*generation.GenerateResult, error) {
	if m.generateFunc != nil {
		return m.generateFunc(ctx, messages, opts)
	}
	return &generation.GenerateResult{
		Text:         "Hello! How can I help you?",
		TokensUsed:   7,
		FinishReason: "stop",
	}, nil
}

func (m *mockAnthropicGenerator) Close() error { return nil }

type mockStreamingAnthropicGenerator struct {
	mockAnthropicGenerator
	streamFunc func(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (<-chan generation.TokenDelta, <-chan error, error)
}

func (m *mockStreamingAnthropicGenerator) GenerateStream(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (<-chan generation.TokenDelta, <-chan error, error) {
	if m.streamFunc != nil {
		return m.streamFunc(ctx, messages, opts)
	}
	tokenChan := make(chan generation.TokenDelta, 3)
	errChan := make(chan error, 1)
	go func() {
		defer close(tokenChan)
		tokens := []string{"Hello", " world", "!"}
		for i, t := range tokens {
			tokenChan <- generation.TokenDelta{Token: t, Index: i}
		}
	}()
	return tokenChan, errChan, nil
}

type mockAnthropicGeneratorRegistry struct {
	models map[string]generation.Generator
}

func (r *mockAnthropicGeneratorRegistry) Acquire(modelName string) (generation.Generator, error) {
	if gen, ok := r.models[modelName]; ok {
		return gen, nil
	}
	return nil, fmt.Errorf("generator model not found: %s", modelName)
}

func (r *mockAnthropicGeneratorRegistry) Release(modelName string) {}

func (r *mockAnthropicGeneratorRegistry) List() []string {
	names := make([]string, 0, len(r.models))
	for name := range r.models {
		names = append(names, name)
	}
	return names
}

func (r *mockAnthropicGeneratorRegistry) Close() error { return nil }

func newAnthropicTestNode(t *testing.T, gen generation.Generator) *TermiteNode {
	t.Helper()
	logger := zaptest.NewLogger(t)
	registry := &mockAnthropicGeneratorRegistry{
		models: map[string]generation.Generator{
			"test-model": gen,
		},
	}
	return &TermiteNode{
		logger:            logger,
		generatorRegistry: registry,
		requestQueue: NewRequestQueue(RequestQueueConfig{
			MaxConcurrentRequests: 10,
			MaxQueueSize:          100,
		}, logger.Named("queue")),
	}
}

// --- Request translation tests ---

func TestParseAnthropicSystem(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "string system",
			raw:  `"You are a helpful assistant."`,
			want: "You are a helpful assistant.",
		},
		{
			name: "array system",
			raw:  `[{"type":"text","text":"You are helpful."},{"type":"text","text":"Be concise."}]`,
			want: "You are helpful.\nBe concise.",
		},
		{
			name: "empty",
			raw:  "",
			want: "",
		},
		{
			name: "null",
			raw:  "null",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseAnthropicSystem(stdjson.RawMessage(tt.raw))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseAnthropicContent(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []anthropicContentBlock
		wantErr bool
	}{
		{
			name: "string content",
			raw:  `"Hello, world!"`,
			want: []anthropicContentBlock{{Type: "text", Text: "Hello, world!"}},
		},
		{
			name: "array with text block",
			raw:  `[{"type":"text","text":"Hello!"}]`,
			want: []anthropicContentBlock{{Type: "text", Text: "Hello!"}},
		},
		{
			name: "array with image block",
			raw:  `[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"abc123"}}]`,
			want: []anthropicContentBlock{{
				Type:   "image",
				Source: &anthropicImageSource{Type: "base64", MediaType: "image/png", Data: "abc123"},
			}},
		},
		{
			name: "empty",
			raw:  "",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAnthropicContent(stdjson.RawMessage(tt.raw))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestConvertAnthropicMessages(t *testing.T) {
	t.Run("simple text messages", func(t *testing.T) {
		req := anthropicRequest{
			Messages: []anthropicMessage{
				{Role: "user", Content: stdjson.RawMessage(`"Hello"`)},
			},
		}
		messages, err := convertAnthropicMessages(req)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		assert.Equal(t, "user", messages[0].Role)
		assert.Equal(t, "Hello", messages[0].Content)
	})

	t.Run("with system prompt", func(t *testing.T) {
		req := anthropicRequest{
			System: stdjson.RawMessage(`"Be helpful"`),
			Messages: []anthropicMessage{
				{Role: "user", Content: stdjson.RawMessage(`"Hello"`)},
			},
		}
		messages, err := convertAnthropicMessages(req)
		require.NoError(t, err)
		require.Len(t, messages, 2)
		assert.Equal(t, "system", messages[0].Role)
		assert.Equal(t, "Be helpful", messages[0].Content)
		assert.Equal(t, "user", messages[1].Role)
	})

	t.Run("multi-turn conversation", func(t *testing.T) {
		req := anthropicRequest{
			Messages: []anthropicMessage{
				{Role: "user", Content: stdjson.RawMessage(`"Hello"`)},
				{Role: "assistant", Content: stdjson.RawMessage(`"Hi there!"`)},
				{Role: "user", Content: stdjson.RawMessage(`"How are you?"`)},
			},
		}
		messages, err := convertAnthropicMessages(req)
		require.NoError(t, err)
		require.Len(t, messages, 3)
		assert.Equal(t, "user", messages[0].Role)
		assert.Equal(t, "assistant", messages[1].Role)
		assert.Equal(t, "Hi there!", messages[1].Content)
		assert.Equal(t, "user", messages[2].Role)
	})

	t.Run("image content", func(t *testing.T) {
		req := anthropicRequest{
			Messages: []anthropicMessage{
				{Role: "user", Content: stdjson.RawMessage(`[
					{"type":"text","text":"What is in this image?"},
					{"type":"image","source":{"type":"base64","media_type":"image/png","data":"abc123"}}
				]`)},
			},
		}
		messages, err := convertAnthropicMessages(req)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		assert.Equal(t, "What is in this image?", messages[0].Content)
		require.Len(t, messages[0].Parts, 2)
		assert.Equal(t, "text", messages[0].Parts[0].Type)
		assert.Equal(t, "image_url", messages[0].Parts[1].Type)
		assert.Equal(t, "data:image/png;base64,abc123", messages[0].Parts[1].ImageURL)
	})

	t.Run("tool result messages", func(t *testing.T) {
		req := anthropicRequest{
			Messages: []anthropicMessage{
				{Role: "user", Content: stdjson.RawMessage(`[
					{"type":"tool_result","tool_use_id":"call_123","content":"The weather is sunny."}
				]`)},
			},
		}
		messages, err := convertAnthropicMessages(req)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		assert.Equal(t, "tool", messages[0].Role)
		assert.Equal(t, "The weather is sunny.", messages[0].Content)
	})
}

func TestMapAnthropicStopReason(t *testing.T) {
	assert.Equal(t, "end_turn", mapAnthropicStopReason("stop", false))
	assert.Equal(t, "max_tokens", mapAnthropicStopReason("length", false))
	assert.Equal(t, "tool_use", mapAnthropicStopReason("stop", true))
	assert.Equal(t, "tool_use", mapAnthropicStopReason("length", true))
	assert.Equal(t, "end_turn", mapAnthropicStopReason("unknown", false))
}

// --- Handler tests ---

func TestHandleAnthropicMessages_Success(t *testing.T) {
	gen := &mockAnthropicGenerator{}
	node := newAnthropicTestNode(t, gen)

	body, _ := json.Marshal(anthropicRequest{
		Model:     "test-model",
		MaxTokens: 100,
		Messages:  []anthropicMessage{{Role: "user", Content: stdjson.RawMessage(`"Hello"`)}},
	})

	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp anthropicResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Equal(t, "message", resp.Type)
	assert.Equal(t, "assistant", resp.Role)
	assert.True(t, strings.HasPrefix(resp.ID, "msg_"))
	require.Len(t, resp.Content, 1)
	assert.Equal(t, "text", resp.Content[0].Type)
	assert.Equal(t, "Hello! How can I help you?", resp.Content[0].Text)
	require.NotNil(t, resp.StopReason)
	assert.Equal(t, "end_turn", *resp.StopReason)
	assert.Equal(t, "test-model", resp.Model)
	assert.Equal(t, 7, resp.Usage.OutputTokens)
}

func TestHandleAnthropicMessages_WithSystem(t *testing.T) {
	var capturedMessages []generation.Message
	gen := &mockAnthropicGenerator{
		generateFunc: func(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (*generation.GenerateResult, error) {
			capturedMessages = messages
			return &generation.GenerateResult{Text: "ok", TokensUsed: 1, FinishReason: "stop"}, nil
		},
	}
	node := newAnthropicTestNode(t, gen)

	body, _ := json.Marshal(anthropicRequest{
		Model:     "test-model",
		MaxTokens: 100,
		System:    stdjson.RawMessage(`"You are a pirate."`),
		Messages:  []anthropicMessage{{Role: "user", Content: stdjson.RawMessage(`"Hello"`)}},
	})

	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	require.Len(t, capturedMessages, 2)
	assert.Equal(t, "system", capturedMessages[0].Role)
	assert.Equal(t, "You are a pirate.", capturedMessages[0].Content)
	assert.Equal(t, "user", capturedMessages[1].Role)
}

func TestHandleAnthropicMessages_ParameterMapping(t *testing.T) {
	var capturedOpts generation.GenerateOptions
	gen := &mockAnthropicGenerator{
		generateFunc: func(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (*generation.GenerateResult, error) {
			capturedOpts = opts
			return &generation.GenerateResult{Text: "ok", TokensUsed: 1, FinishReason: "stop"}, nil
		},
	}
	node := newAnthropicTestNode(t, gen)

	body, _ := json.Marshal(anthropicRequest{
		Model:         "test-model",
		MaxTokens:     200,
		Messages:      []anthropicMessage{{Role: "user", Content: stdjson.RawMessage(`"test"`)}},
		Temperature:   0.5,
		TopP:          0.9,
		TopK:          40,
		StopSequences: []string{"END"},
	})

	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 200, capturedOpts.MaxTokens)
	assert.InDelta(t, 0.5, capturedOpts.Temperature, 0.01)
	assert.InDelta(t, 0.9, capturedOpts.TopP, 0.01)
	assert.Equal(t, 40, capturedOpts.TopK)
	assert.Equal(t, []string{"END"}, capturedOpts.StopTokens)
}

func TestHandleAnthropicMessages_Validation(t *testing.T) {
	gen := &mockAnthropicGenerator{}
	node := newAnthropicTestNode(t, gen)

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantError  string
	}{
		{
			name:       "invalid JSON",
			body:       "not json",
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid JSON",
		},
		{
			name:       "missing model",
			body:       `{"max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "model is required",
		},
		{
			name:       "missing messages",
			body:       `{"model":"test-model","max_tokens":100}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "messages is required",
		},
		{
			name:       "missing max_tokens",
			body:       `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "max_tokens is required",
		},
		{
			name:       "model not found",
			body:       `{"model":"nonexistent","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`,
			wantStatus: http.StatusNotFound,
			wantError:  "model not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader([]byte(tt.body)))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			node.handleAnthropicMessages(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantError)
		})
	}
}

func TestHandleAnthropicMessages_NoModels(t *testing.T) {
	logger := zaptest.NewLogger(t)
	node := &TermiteNode{
		logger:            logger,
		generatorRegistry: nil,
		requestQueue: NewRequestQueue(RequestQueueConfig{
			MaxConcurrentRequests: 10,
			MaxQueueSize:          100,
		}, logger.Named("queue")),
	}

	body := `{"model":"test","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "no models configured")
}

func TestHandleAnthropicMessages_GenerationError(t *testing.T) {
	gen := &mockAnthropicGenerator{
		generateFunc: func(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (*generation.GenerateResult, error) {
			return nil, fmt.Errorf("out of memory")
		},
	}
	node := newAnthropicTestNode(t, gen)

	body, _ := json.Marshal(anthropicRequest{
		Model:     "test-model",
		MaxTokens: 100,
		Messages:  []anthropicMessage{{Role: "user", Content: stdjson.RawMessage(`"Hello"`)}},
	})

	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "generation failed")
}

func TestHandleAnthropicMessages_FinishReasonMaxTokens(t *testing.T) {
	gen := &mockAnthropicGenerator{
		generateFunc: func(ctx context.Context, messages []generation.Message, opts generation.GenerateOptions) (*generation.GenerateResult, error) {
			return &generation.GenerateResult{
				Text:         "partial output",
				TokensUsed:   100,
				FinishReason: "length",
			}, nil
		},
	}
	node := newAnthropicTestNode(t, gen)

	body, _ := json.Marshal(anthropicRequest{
		Model:     "test-model",
		MaxTokens: 100,
		Messages:  []anthropicMessage{{Role: "user", Content: stdjson.RawMessage(`"Hello"`)}},
	})

	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp anthropicResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.NotNil(t, resp.StopReason)
	assert.Equal(t, "max_tokens", *resp.StopReason)
}

// --- Streaming tests ---

func TestHandleAnthropicMessages_Streaming(t *testing.T) {
	gen := &mockStreamingAnthropicGenerator{}
	node := newAnthropicTestNode(t, gen)

	body, _ := json.Marshal(anthropicRequest{
		Model:     "test-model",
		MaxTokens: 100,
		Messages:  []anthropicMessage{{Role: "user", Content: stdjson.RawMessage(`"Hello"`)}},
		Stream:    true,
	})

	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))

	// Parse SSE events
	events := parseSSEEvents(t, w.Body.String())

	// Verify event sequence
	require.GreaterOrEqual(t, len(events), 7, "expected at least 7 SSE events")

	assert.Equal(t, "message_start", events[0].eventType)
	assert.Equal(t, "ping", events[1].eventType)
	assert.Equal(t, "content_block_start", events[2].eventType)

	// Token deltas (3 tokens: "Hello", " world", "!")
	assert.Equal(t, "content_block_delta", events[3].eventType)
	assert.Equal(t, "content_block_delta", events[4].eventType)
	assert.Equal(t, "content_block_delta", events[5].eventType)

	assert.Equal(t, "content_block_stop", events[6].eventType)
	assert.Equal(t, "message_delta", events[7].eventType)
	assert.Equal(t, "message_stop", events[8].eventType)

	// Verify message_start content
	var msgStart anthropicMessageStartEvent
	require.NoError(t, stdjson.Unmarshal([]byte(events[0].data), &msgStart))
	assert.Equal(t, "message_start", msgStart.Type)
	assert.True(t, strings.HasPrefix(msgStart.Message.ID, "msg_"))
	assert.Equal(t, "assistant", msgStart.Message.Role)
	assert.Equal(t, "test-model", msgStart.Message.Model)

	// Verify a content_block_delta
	var delta anthropicContentBlockDeltaEvent
	require.NoError(t, stdjson.Unmarshal([]byte(events[3].data), &delta))
	assert.Equal(t, "text_delta", delta.Delta.Type)
	assert.Equal(t, "Hello", delta.Delta.Text)

	// Verify message_delta has stop_reason
	var msgDelta anthropicMessageDeltaEvent
	require.NoError(t, stdjson.Unmarshal([]byte(events[7].data), &msgDelta))
	require.NotNil(t, msgDelta.Delta.StopReason)
	assert.Equal(t, "end_turn", *msgDelta.Delta.StopReason)
	assert.Equal(t, 3, msgDelta.Usage.OutputTokens)
}

func TestHandleAnthropicMessages_StreamingNonStreamingGenerator(t *testing.T) {
	// Generator that doesn't implement StreamingGenerator
	gen := &mockAnthropicGenerator{}
	node := newAnthropicTestNode(t, gen)

	body, _ := json.Marshal(anthropicRequest{
		Model:     "test-model",
		MaxTokens: 100,
		Messages:  []anthropicMessage{{Role: "user", Content: stdjson.RawMessage(`"Hello"`)}},
		Stream:    true,
	})

	req := httptest.NewRequest("POST", "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	node.handleAnthropicMessages(w, req)

	// Should return an error SSE event
	events := parseSSEEvents(t, w.Body.String())
	require.GreaterOrEqual(t, len(events), 1)
	assert.Equal(t, "error", events[0].eventType)
	assert.Contains(t, events[0].data, "does not support streaming")
}

// --- SSE event parsing helper ---

type sseEvent struct {
	eventType string
	data      string
}

func parseSSEEvents(t *testing.T, body string) []sseEvent {
	t.Helper()
	var events []sseEvent
	scanner := bufio.NewScanner(strings.NewReader(body))

	var currentEvent sseEvent
	for scanner.Scan() {
		line := scanner.Text()
		if after, ok := strings.CutPrefix(line, "event: "); ok {
			currentEvent.eventType = after
		} else if after, ok := strings.CutPrefix(line, "data: "); ok {
			currentEvent.data = after
		} else if line == "" && currentEvent.eventType != "" {
			events = append(events, currentEvent)
			currentEvent = sseEvent{}
		}
	}
	return events
}
