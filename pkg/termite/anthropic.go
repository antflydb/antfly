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
	"crypto/rand"
	"encoding/hex"
	stdjson "encoding/json"
	"fmt"
	"net/http"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/pkg/termite/lib/generation"
	"go.uber.org/zap"
)

// Anthropic Messages API compatibility at /anthropic/v1/*
//
// This provides a compatibility layer that allows Anthropic SDKs to work
// with Termite. The endpoint mirrors Anthropic's Messages API:
//
//   - POST /anthropic/v1/messages - Create a message
//
// Usage with Anthropic SDK:
//
//	client := anthropic.NewClient(
//	    option.WithBaseURL("http://localhost:8080/anthropic"),
//	    option.WithAPIKey("unused"), // Termite doesn't require auth
//	)

// --- Request types ---

type anthropicRequest struct {
	Model         string             `json:"model"`
	MaxTokens     int                `json:"max_tokens"`
	Messages      []anthropicMessage `json:"messages"`
	System        stdjson.RawMessage `json:"system,omitempty"`
	Stream        bool               `json:"stream,omitempty"`
	Temperature   float32            `json:"temperature,omitempty"`
	TopP          float32            `json:"top_p,omitempty"`
	TopK          int                `json:"top_k,omitempty"`
	StopSequences []string           `json:"stop_sequences,omitempty"`
	Tools         []anthropicTool    `json:"tools,omitempty"`
}

type anthropicMessage struct {
	Role    string             `json:"role"` // "user" or "assistant"
	Content stdjson.RawMessage `json:"content"`
}

type anthropicContentBlock struct {
	Type string `json:"type"`

	// type=text
	Text string `json:"text,omitempty"`

	// type=image
	Source *anthropicImageSource `json:"source,omitempty"`

	// type=tool_use (in assistant responses)
	ID    string             `json:"id,omitempty"`
	Name  string             `json:"name,omitempty"`
	Input stdjson.RawMessage `json:"input,omitempty"`

	// type=tool_result (in user messages)
	ToolUseID string             `json:"tool_use_id,omitempty"`
	Content   stdjson.RawMessage `json:"content,omitempty"` // string or []block
}

type anthropicImageSource struct {
	Type      string `json:"type"`       // "base64"
	MediaType string `json:"media_type"` // e.g. "image/png"
	Data      string `json:"data"`       // base64-encoded
}

type anthropicTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"input_schema"`
}

type anthropicSystemBlock struct {
	Type string `json:"type"` // "text"
	Text string `json:"text"`
}

// --- Response types ---

type anthropicResponse struct {
	ID           string                   `json:"id"`
	Type         string                   `json:"type"` // "message"
	Role         string                   `json:"role"` // "assistant"
	Content      []anthropicResponseBlock `json:"content"`
	Model        string                   `json:"model"`
	StopReason   *string                  `json:"stop_reason"`
	StopSequence *string                  `json:"stop_sequence"`
	Usage        anthropicUsage           `json:"usage"`
}

type anthropicResponseBlock struct {
	Type string `json:"type"`

	// type=text
	Text string `json:"text,omitempty"`

	// type=tool_use
	ID    string             `json:"id,omitempty"`
	Name  string             `json:"name,omitempty"`
	Input stdjson.RawMessage `json:"input,omitempty"`
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// --- Streaming event types ---
// Anthropic SSE uses: event: <type>\ndata: <json>\n\n

type anthropicMessageStartEvent struct {
	Type    string                      `json:"type"` // "message_start"
	Message anthropicMessageStartDetail `json:"message"`
}

type anthropicMessageStartDetail struct {
	ID           string                   `json:"id"`
	Type         string                   `json:"type"` // "message"
	Role         string                   `json:"role"` // "assistant"
	Content      []anthropicResponseBlock `json:"content"`
	Model        string                   `json:"model"`
	StopReason   *string                  `json:"stop_reason"`
	StopSequence *string                  `json:"stop_sequence"`
	Usage        anthropicUsage           `json:"usage"`
}

type anthropicContentBlockStartEvent struct {
	Type         string                 `json:"type"` // "content_block_start"
	Index        int                    `json:"index"`
	ContentBlock anthropicResponseBlock `json:"content_block"`
}

type anthropicContentBlockDeltaEvent struct {
	Type  string               `json:"type"` // "content_block_delta"
	Index int                  `json:"index"`
	Delta anthropicStreamDelta `json:"delta"`
}

type anthropicStreamDelta struct {
	Type string `json:"type"` // "text_delta"
	Text string `json:"text,omitempty"`
}

type anthropicContentBlockStopEvent struct {
	Type  string `json:"type"` // "content_block_stop"
	Index int    `json:"index"`
}

type anthropicMessageDeltaEvent struct {
	Type  string                    `json:"type"` // "message_delta"
	Delta anthropicMessageDeltaData `json:"delta"`
	Usage anthropicDeltaUsage       `json:"usage"`
}

type anthropicMessageDeltaData struct {
	StopReason   *string `json:"stop_reason"`
	StopSequence *string `json:"stop_sequence"`
}

type anthropicDeltaUsage struct {
	OutputTokens int `json:"output_tokens"`
}

type anthropicMessageStopEvent struct {
	Type string `json:"type"` // "message_stop"
}

type anthropicPingEvent struct {
	Type string `json:"type"` // "ping"
}

type anthropicErrorEvent struct {
	Type  string              `json:"type"` // "error"
	Error anthropicErrorBlock `json:"error"`
}

type anthropicErrorBlock struct {
	Type    string `json:"type"` // "overloaded_error", "api_error", etc.
	Message string `json:"message"`
}

// --- Route registration ---

// RegisterAnthropicRoutes adds Anthropic-compatible endpoints to the given mux.
// These routes allow Anthropic SDKs to work with Termite by providing an endpoint
// at /anthropic/v1/messages that mirrors Anthropic's Messages API.
func (ln *TermiteNode) RegisterAnthropicRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /anthropic/v1/messages", ln.handleAnthropicMessages)
}

// --- Request translation ---

// parseAnthropicSystem extracts the system prompt from the request.
// The system field can be a string or an array of {type: "text", text: "..."} blocks.
func parseAnthropicSystem(raw stdjson.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// Try as string first
	var s string
	if err := stdjson.Unmarshal(raw, &s); err == nil {
		return s
	}
	// Try as array of system blocks
	var blocks []anthropicSystemBlock
	if err := stdjson.Unmarshal(raw, &blocks); err == nil {
		var result string
		for _, b := range blocks {
			if b.Type == "text" {
				if result != "" {
					result += "\n"
				}
				result += b.Text
			}
		}
		return result
	}
	return ""
}

// parseAnthropicContent parses the content field of an Anthropic message,
// which can be a plain string or an array of content blocks.
func parseAnthropicContent(raw stdjson.RawMessage) ([]anthropicContentBlock, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	// Try as string first
	var s string
	if err := stdjson.Unmarshal(raw, &s); err == nil {
		return []anthropicContentBlock{{Type: "text", Text: s}}, nil
	}
	// Try as array of content blocks
	var blocks []anthropicContentBlock
	if err := stdjson.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("content must be a string or array of content blocks: %w", err)
	}
	return blocks, nil
}

// convertAnthropicMessages translates Anthropic messages and system prompt
// into the internal generation.Message format.
func convertAnthropicMessages(req anthropicRequest) ([]generation.Message, error) {
	var messages []generation.Message

	// Prepend system prompt if present
	if system := parseAnthropicSystem(req.System); system != "" {
		messages = append(messages, generation.Message{
			Role:    "system",
			Content: system,
		})
	}

	for _, m := range req.Messages {
		blocks, err := parseAnthropicContent(m.Content)
		if err != nil {
			return nil, fmt.Errorf("message content: %w", err)
		}

		switch m.Role {
		case "user":
			msg := generation.Message{Role: "user"}
			for _, b := range blocks {
				switch b.Type {
				case "text":
					msg.Parts = append(msg.Parts, generation.TextPart(b.Text))
					if msg.Content == "" {
						msg.Content = b.Text
					}
				case "image":
					if b.Source != nil && b.Source.Type == "base64" {
						dataURI := "data:" + b.Source.MediaType + ";base64," + b.Source.Data
						msg.Parts = append(msg.Parts, generation.ImagePart(dataURI))
					}
				case "tool_result":
					// Tool results come as user messages in Anthropic format.
					// Convert the content to a string for the tool role message.
					toolContent := parseToolResultContent(b.Content)
					messages = append(messages, generation.Message{
						Role:    "tool",
						Content: toolContent,
					})
					continue // Don't add to the current user message
				}
			}
			// Only append if there's actual user content (not just tool_result blocks)
			if msg.Content != "" || len(msg.Parts) > 0 {
				messages = append(messages, msg)
			}

		case "assistant":
			msg := generation.Message{Role: "assistant"}
			for _, b := range blocks {
				switch b.Type {
				case "text":
					if msg.Content != "" {
						msg.Content += "\n"
					}
					msg.Content += b.Text
				case "tool_use":
					// In multi-turn with tool use, the assistant's tool_use blocks
					// represent previous tool calls. We include the text content
					// of the assistant message for context.
				}
			}
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// parseToolResultContent extracts text from a tool_result's content field,
// which can be a string or an array of content blocks.
func parseToolResultContent(raw stdjson.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := stdjson.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []anthropicContentBlock
	if err := stdjson.Unmarshal(raw, &blocks); err == nil {
		var result string
		for _, b := range blocks {
			if b.Type == "text" {
				if result != "" {
					result += "\n"
				}
				result += b.Text
			}
		}
		return result
	}
	return string(raw)
}

func generateAnthropicMessageID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "msg_" + hex.EncodeToString(b)
}

// mapAnthropicStopReason converts internal finish reasons to Anthropic stop reasons.
func mapAnthropicStopReason(finishReason string, hasToolCalls bool) string {
	if hasToolCalls {
		return "tool_use"
	}
	switch finishReason {
	case "length":
		return "max_tokens"
	default:
		return "end_turn"
	}
}

// writeAnthropicError writes an Anthropic-format error response.
func writeAnthropicError(w http.ResponseWriter, status int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = stdjson.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]string{
			"type":    errType,
			"message": message,
		},
	})
}

// --- Handler ---

// handleAnthropicMessages handles Anthropic Messages API requests.
func (ln *TermiteNode) handleAnthropicMessages(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	ln.logger.Info("Anthropic messages request received",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path))

	// Check if generation is available
	if ln.generatorRegistry == nil || len(ln.generatorRegistry.List()) == 0 {
		writeAnthropicError(w, http.StatusServiceUnavailable, "api_error", "generation not available: no models configured")
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			writeAnthropicError(w, http.StatusServiceUnavailable, "overloaded_error", "server is overloaded, please retry")
		case ErrRequestTimeout:
			writeAnthropicError(w, http.StatusGatewayTimeout, "api_error", "request timed out in queue")
		default:
			writeAnthropicError(w, http.StatusRequestTimeout, "api_error", "request cancelled")
		}
		return
	}
	defer release()

	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request
	var req anthropicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	// Validate required fields
	if req.Model == "" {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}
	if len(req.Messages) == 0 {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "messages is required")
		return
	}
	if req.MaxTokens <= 0 {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "max_tokens is required and must be > 0")
		return
	}

	// Acquire generator
	generator, err := ln.generatorRegistry.Acquire(req.Model)
	if err != nil {
		writeAnthropicError(w, http.StatusNotFound, "not_found_error", fmt.Sprintf("model not found: %s: %v", req.Model, err))
		return
	}
	defer ln.generatorRegistry.Release(req.Model)

	// Check for tool support
	var toolParser generation.ToolParser
	if len(req.Tools) > 0 {
		ts, ok := generator.(generation.ToolSupporter)
		if !ok || !ts.SupportsTools() {
			writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error",
				fmt.Sprintf("model %s does not support tool calling", req.Model))
			return
		}
		toolParser = ts.ToolParser()
	}

	// Convert Anthropic messages to internal format
	messages, err := convertAnthropicMessages(req)
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", fmt.Sprintf("invalid messages: %v", err))
		return
	}

	// Set generation options
	opts := generation.GenerateOptions{
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		TopK:        req.TopK,
		StopTokens:  req.StopSequences,
	}
	// Apply defaults for zero values
	if opts.Temperature == 0 {
		opts.Temperature = 1.0
	}
	if opts.TopP == 0 {
		opts.TopP = 1.0
	}

	// Inject tool declarations into system prompt if tools are provided
	if toolParser != nil && len(req.Tools) > 0 {
		tools := make([]generation.ToolDefinition, len(req.Tools))
		for i, t := range req.Tools {
			tools[i] = generation.ToolDefinition{
				Type: "function",
				Function: generation.FunctionDefinition{
					Name:        t.Name,
					Description: t.Description,
					Parameters:  t.InputSchema,
				},
			}
		}

		toolsPrompt := toolParser.FormatToolsPrompt(tools)

		// Prepend to system message or create new one
		if len(messages) > 0 && messages[0].Role == "system" {
			messages[0].Content = toolsPrompt + "\n\n" + messages[0].Content
		} else {
			systemMsg := generation.Message{
				Role:    "system",
				Content: toolsPrompt,
			}
			messages = append([]generation.Message{systemMsg}, messages...)
		}
	}

	messageID := generateAnthropicMessageID()

	// Route to streaming or non-streaming handler
	if req.Stream {
		ln.handleAnthropicStreaming(w, r, req, generator, messages, opts, toolParser, messageID)
		return
	}

	// Non-streaming: generate
	result, err := generator.Generate(r.Context(), messages, opts)
	if err != nil {
		ln.logger.Error("Anthropic generation failed",
			zap.String("model", req.Model),
			zap.Error(err))
		writeAnthropicError(w, http.StatusInternalServerError, "api_error", fmt.Sprintf("generation failed: %v", err))
		return
	}

	RecordGeneratorRequest(req.Model)
	RecordTokenGeneration(req.Model, result.TokensUsed)

	// Parse tool calls if tools were requested
	var toolCalls []generation.ToolCall
	var responseText string
	if toolParser != nil && len(req.Tools) > 0 {
		toolParser.Reset()
		toolParser.Feed(result.Text)
		toolCalls, responseText = toolParser.Finish()
	} else {
		responseText = result.Text
	}

	// Estimate prompt tokens
	promptTokens := 0
	for _, m := range messages {
		promptTokens += len(m.GetTextContent()) / 4
	}

	// Build Anthropic response
	var content []anthropicResponseBlock
	if responseText != "" {
		content = append(content, anthropicResponseBlock{
			Type: "text",
			Text: responseText,
		})
	}
	for _, tc := range toolCalls {
		content = append(content, anthropicResponseBlock{
			Type:  "tool_use",
			ID:    tc.ID,
			Name:  tc.Function.Name,
			Input: stdjson.RawMessage(tc.Function.Arguments),
		})
	}
	// Ensure content is never null
	if content == nil {
		content = []anthropicResponseBlock{}
	}

	stopReason := mapAnthropicStopReason(result.FinishReason, len(toolCalls) > 0)
	resp := anthropicResponse{
		ID:         messageID,
		Type:       "message",
		Role:       "assistant",
		Content:    content,
		Model:      req.Model,
		StopReason: &stopReason,
		Usage: anthropicUsage{
			InputTokens:  promptTokens,
			OutputTokens: result.TokensUsed,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding Anthropic response", zap.Error(err))
	}
}

// --- Streaming handler ---

func (ln *TermiteNode) handleAnthropicStreaming(
	w http.ResponseWriter,
	r *http.Request,
	req anthropicRequest,
	generator generation.Generator,
	messages []generation.Message,
	opts generation.GenerateOptions,
	toolParser generation.ToolParser,
	messageID string,
) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAnthropicError(w, http.StatusInternalServerError, "api_error", "streaming not supported")
		return
	}

	// Assert streaming support
	streamingGen, ok := generator.(generation.StreamingGenerator)
	if !ok {
		sendAnthropicSSE(w, flusher, "error", anthropicErrorEvent{
			Type:  "error",
			Error: anthropicErrorBlock{Type: "api_error", Message: "model does not support streaming"},
		})
		return
	}

	// Start streaming
	tokenChan, errChan, err := streamingGen.GenerateStream(r.Context(), messages, opts)
	if err != nil {
		sendAnthropicSSE(w, flusher, "error", anthropicErrorEvent{
			Type:  "error",
			Error: anthropicErrorBlock{Type: "api_error", Message: fmt.Sprintf("failed to start streaming: %v", err)},
		})
		return
	}

	// Estimate prompt tokens
	promptTokens := 0
	for _, m := range messages {
		promptTokens += len(m.GetTextContent()) / 4
	}

	// 1. message_start
	sendAnthropicSSE(w, flusher, "message_start", anthropicMessageStartEvent{
		Type: "message_start",
		Message: anthropicMessageStartDetail{
			ID:      messageID,
			Type:    "message",
			Role:    "assistant",
			Content: []anthropicResponseBlock{},
			Model:   req.Model,
			Usage:   anthropicUsage{InputTokens: promptTokens, OutputTokens: 0},
		},
	})

	// 2. ping
	sendAnthropicSSE(w, flusher, "ping", anthropicPingEvent{Type: "ping"})

	// 3. content_block_start (text block at index 0)
	sendAnthropicSSE(w, flusher, "content_block_start", anthropicContentBlockStartEvent{
		Type:         "content_block_start",
		Index:        0,
		ContentBlock: anthropicResponseBlock{Type: "text", Text: ""},
	})

	// 4. Stream content_block_delta events for each token
	var tokenCount int
	for token := range tokenChan {
		tokenCount++
		sendAnthropicSSE(w, flusher, "content_block_delta", anthropicContentBlockDeltaEvent{
			Type:  "content_block_delta",
			Index: 0,
			Delta: anthropicStreamDelta{Type: "text_delta", Text: token.Token},
		})
	}

	// Check for errors
	select {
	case err := <-errChan:
		if err != nil {
			ln.logger.Error("Anthropic streaming generation error",
				zap.String("model", req.Model),
				zap.Error(err))
			sendAnthropicSSE(w, flusher, "error", anthropicErrorEvent{
				Type:  "error",
				Error: anthropicErrorBlock{Type: "api_error", Message: err.Error()},
			})
			return
		}
	default:
	}

	RecordGeneratorRequest(req.Model)
	RecordTokenGeneration(req.Model, tokenCount)

	// 5. content_block_stop
	sendAnthropicSSE(w, flusher, "content_block_stop", anthropicContentBlockStopEvent{
		Type:  "content_block_stop",
		Index: 0,
	})

	// 6. message_delta with stop reason
	stopReason := "end_turn"
	sendAnthropicSSE(w, flusher, "message_delta", anthropicMessageDeltaEvent{
		Type:  "message_delta",
		Delta: anthropicMessageDeltaData{StopReason: &stopReason},
		Usage: anthropicDeltaUsage{OutputTokens: tokenCount},
	})

	// 7. message_stop
	sendAnthropicSSE(w, flusher, "message_stop", anthropicMessageStopEvent{
		Type: "message_stop",
	})

	ln.logger.Info("Anthropic streaming generation completed",
		zap.String("model", req.Model),
		zap.Int("tokens_generated", tokenCount))
}

// sendAnthropicSSE writes a single Anthropic SSE event: event: <type>\ndata: <json>\n\n
func sendAnthropicSSE(w http.ResponseWriter, flusher http.Flusher, eventType string, data any) {
	payload, _ := json.Marshal(data)
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, payload)
	flusher.Flush()
}
