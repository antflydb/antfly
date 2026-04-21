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

package generation

import (
	"context"
	"strings"
)

// Generator is the interface for text generation models.
type Generator interface {
	Generate(ctx context.Context, messages []Message, opts GenerateOptions) (*GenerateResult, error)
	Close() error
}

// StreamingGenerator extends Generator with streaming capabilities.
type StreamingGenerator interface {
	Generator
	GenerateStream(ctx context.Context, messages []Message, opts GenerateOptions) (<-chan TokenDelta, <-chan error, error)
}

// ToolSupporter indicates a generator that supports tool calling.
type ToolSupporter interface {
	SupportsTools() bool
	ToolParser() ToolParser
	ToolCallFormat() string
}

// toolSupport provides a reusable ToolSupporter implementation.
// Embed this in generator structs to satisfy the ToolSupporter interface.
type toolSupport struct {
	toolParser     ToolParser
	toolCallFormat string
}

func (t *toolSupport) SupportsTools() bool    { return t.toolParser != nil }
func (t *toolSupport) ToolParser() ToolParser { return t.toolParser }
func (t *toolSupport) ToolCallFormat() string { return t.toolCallFormat }

// Message represents a chat message with optional multimodal content.
type Message struct {
	Role    string        `json:"role"`
	Content string        `json:"content,omitempty"`
	Parts   []ContentPart `json:"parts,omitempty"`
}

// GetTextContent returns the text content of the message.
// If Parts is set, it concatenates all text parts.
// Otherwise, it returns the Content field.
func (m Message) GetTextContent() string {
	if len(m.Parts) == 0 {
		return m.Content
	}
	var text strings.Builder
	for _, part := range m.Parts {
		if part.Type == "text" {
			text.WriteString(part.Text)
		}
	}
	return text.String()
}

// HasImages returns true if this message contains any image parts.
func (m Message) HasImages() bool {
	for _, part := range m.Parts {
		if part.Type == "image_url" && part.ImageURL != "" {
			return true
		}
	}
	return false
}

// ContentPart represents a part of multimodal content.
type ContentPart struct {
	Type     string `json:"type"`                // "text" or "image_url"
	Text     string `json:"text,omitempty"`      // For type="text"
	ImageURL string `json:"image_url,omitempty"` // For type="image_url"
}

// GenerateOptions holds parameters for text generation.
type GenerateOptions struct {
	MaxTokens          int                  `json:"max_tokens,omitempty"`
	Temperature        float32              `json:"temperature,omitempty"`
	TopP               float32              `json:"top_p,omitempty"`
	TopK               int                  `json:"top_k,omitempty"`
	StopTokens         []string             `json:"stop_tokens,omitempty"`
	Tools              []FunctionDefinition `json:"tools,omitempty"`
	ToolChoice         string               `json:"tool_choice,omitempty"`          // "auto", "none", "required"
	ForcedFunctionName string               `json:"forced_function_name,omitempty"` // Force a specific function
}

// DefaultGenerateOptions returns sensible defaults for generation.
// Zero values for Temperature/TopP/TopK mean "use pipeline default".
func DefaultGenerateOptions() GenerateOptions {
	return GenerateOptions{
		MaxTokens: 2048,
	}
}

// FunctionDefinition describes a function that can be called by the model.
type FunctionDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"` // JSON Schema
	Strict      bool           `json:"strict,omitempty"`     // Whether to enforce strict parameter validation
}

// GenerateResult holds the output of text generation.
type GenerateResult struct {
	Text         string     `json:"text"`
	TokensUsed   int        `json:"tokens_used"`
	FinishReason string     `json:"finish_reason"` // "stop", "length", "tool_calls"
	ToolCalls    []ToolCall `json:"tool_calls,omitempty"`
}

// TokenDelta represents a single token in streaming output.
type TokenDelta struct {
	Token string `json:"token"`
	Index int    `json:"index"`
}

// ToolDefinition describes a tool that the model can call.
type ToolDefinition struct {
	Type     string             `json:"type"` // "function"
	Function FunctionDefinition `json:"function"`
}

// ToolCall represents a tool call made by the model.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"` // "function"
	Function ToolCallFunction `json:"function"`
}

// ToolCallFunction contains the function name and arguments for a tool call.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON string
}

// TextPart creates a text content part for multimodal messages.
func TextPart(text string) ContentPart {
	return ContentPart{
		Type: "text",
		Text: text,
	}
}

// ImagePart creates an image URL content part for multimodal messages.
func ImagePart(imageURL string) ContentPart {
	return ContentPart{
		Type:     "image_url",
		ImageURL: imageURL,
	}
}
