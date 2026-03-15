// Copyright 2025 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package openrouter

import (
	"context"
	"os"
	"testing"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/revrost/go-openrouter"
)

func TestConvertMessages(t *testing.T) {
	tests := []struct {
		name     string
		messages []*ai.Message
		wantLen  int
		wantErr  bool
	}{
		{
			name: "simple text message",
			messages: []*ai.Message{
				{
					Role:    ai.RoleUser,
					Content: []*ai.Part{ai.NewTextPart("Hello")},
				},
			},
			wantLen: 1,
			wantErr: false,
		},
		{
			name: "system and user messages",
			messages: []*ai.Message{
				{
					Role:    ai.RoleSystem,
					Content: []*ai.Part{ai.NewTextPart("You are helpful")},
				},
				{
					Role:    ai.RoleUser,
					Content: []*ai.Part{ai.NewTextPart("Hello")},
				},
			},
			wantLen: 2,
			wantErr: false,
		},
		{
			name: "tool response",
			messages: []*ai.Message{
				{
					Role: ai.RoleTool,
					Content: []*ai.Part{
						ai.NewToolResponsePart(&ai.ToolResponse{
							Ref:    "call_123",
							Name:   "get_weather",
							Output: map[string]any{"temp": 72},
						}),
					},
				},
			},
			wantLen: 1,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertMessages(tt.messages)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != tt.wantLen {
				t.Errorf("convertMessages() got %d messages, want %d", len(got), tt.wantLen)
			}
		})
	}
}

func TestConvertMessageRoles(t *testing.T) {
	tests := []struct {
		name     string
		role     ai.Role
		wantRole string
	}{
		{"user role", ai.RoleUser, openrouter.ChatMessageRoleUser},
		{"model role", ai.RoleModel, openrouter.ChatMessageRoleAssistant},
		{"system role", ai.RoleSystem, openrouter.ChatMessageRoleSystem},
		{"tool role", ai.RoleTool, openrouter.ChatMessageRoleTool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &ai.Message{
				Role:    tt.role,
				Content: []*ai.Part{ai.NewTextPart("test")},
			}

			// Skip tool role since it uses a different path
			if tt.role == ai.RoleTool {
				msg.Content = []*ai.Part{
					ai.NewToolResponsePart(&ai.ToolResponse{
						Ref:    "test",
						Name:   "test",
						Output: "test",
					}),
				}
			}

			result, err := convertMessage(msg)
			if err != nil {
				t.Fatalf("convertMessage() error = %v", err)
			}

			if len(result) == 0 {
				t.Fatal("expected at least one message")
			}

			if result[0].Role != tt.wantRole {
				t.Errorf("got role %q, want %q", result[0].Role, tt.wantRole)
			}
		})
	}
}

func TestConvertTools(t *testing.T) {
	tools := []*ai.ToolDefinition{
		{
			Name:        "get_weather",
			Description: "Get the weather for a location",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"location": map[string]any{
						"type":        "string",
						"description": "The city and state",
					},
				},
				"required": []string{"location"},
			},
		},
	}

	got := convertTools(tools)

	if len(got) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(got))
	}

	if got[0].Type != openrouter.ToolTypeFunction {
		t.Errorf("expected type %q, got %q", openrouter.ToolTypeFunction, got[0].Type)
	}

	if got[0].Function.Name != "get_weather" {
		t.Errorf("expected name 'get_weather', got %s", got[0].Function.Name)
	}

	if got[0].Function.Description != "Get the weather for a location" {
		t.Errorf("expected description 'Get the weather for a location', got %s", got[0].Function.Description)
	}
}

func TestConvertToolRequest(t *testing.T) {
	msg := &ai.Message{
		Role: ai.RoleModel,
		Content: []*ai.Part{
			ai.NewToolRequestPart(&ai.ToolRequest{
				Ref:   "call_123",
				Name:  "get_weather",
				Input: map[string]any{"location": "San Francisco"},
			}),
		},
	}

	result, err := convertMessage(msg)
	if err != nil {
		t.Fatalf("convertMessage() error = %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result))
	}

	if len(result[0].ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(result[0].ToolCalls))
	}

	tc := result[0].ToolCalls[0]
	if tc.ID != "call_123" {
		t.Errorf("expected ID 'call_123', got %s", tc.ID)
	}
	if tc.Type != "function" {
		t.Errorf("expected type 'function', got %s", tc.Type)
	}
	if tc.Function.Name != "get_weather" {
		t.Errorf("expected function name 'get_weather', got %s", tc.Function.Name)
	}
}

func TestTranslateResponse(t *testing.T) {
	resp := &openrouter.ChatCompletionResponse{
		ID:    "test-id",
		Model: "openai/gpt-4",
		Choices: []openrouter.ChatCompletionChoice{
			{
				Index: 0,
				Message: openrouter.ChatCompletionMessage{
					Role: "assistant",
					Content: openrouter.Content{
						Text: "Hello! How can I help?",
					},
				},
				FinishReason: openrouter.FinishReasonStop,
			},
		},
		Usage: &openrouter.Usage{
			PromptTokens:     10,
			CompletionTokens: 8,
			TotalTokens:      18,
		},
	}

	input := &ai.ModelRequest{}
	result, err := translateResponse(resp, input)
	if err != nil {
		t.Fatalf("translateResponse() error = %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil response")
	}

	if len(result.Message.Content) == 0 {
		t.Fatal("expected content in response")
	}

	if result.Message.Content[0].Text != "Hello! How can I help?" {
		t.Errorf("expected 'Hello! How can I help?', got %s", result.Message.Content[0].Text)
	}

	if result.Usage == nil {
		t.Fatal("expected usage in response")
	}

	if result.Usage.InputTokens != 10 {
		t.Errorf("expected 10 input tokens, got %d", result.Usage.InputTokens)
	}

	if result.Usage.OutputTokens != 8 {
		t.Errorf("expected 8 output tokens, got %d", result.Usage.OutputTokens)
	}
}

func TestTranslateResponseWithToolCalls(t *testing.T) {
	resp := &openrouter.ChatCompletionResponse{
		ID:    "test-id",
		Model: "openai/gpt-4",
		Choices: []openrouter.ChatCompletionChoice{
			{
				Index: 0,
				Message: openrouter.ChatCompletionMessage{
					Role: "assistant",
					ToolCalls: []openrouter.ToolCall{
						{
							ID:   "call_123",
							Type: openrouter.ToolTypeFunction,
							Function: openrouter.FunctionCall{
								Name:      "get_weather",
								Arguments: `{"location":"San Francisco"}`,
							},
						},
					},
				},
				FinishReason: openrouter.FinishReasonToolCalls,
			},
		},
	}

	input := &ai.ModelRequest{}
	result, err := translateResponse(resp, input)
	if err != nil {
		t.Fatalf("translateResponse() error = %v", err)
	}

	if len(result.Message.Content) == 0 {
		t.Fatal("expected content in response")
	}

	part := result.Message.Content[0]
	if !part.IsToolRequest() {
		t.Fatal("expected tool request part")
	}

	if part.ToolRequest.Name != "get_weather" {
		t.Errorf("expected tool name 'get_weather', got %s", part.ToolRequest.Name)
	}

	if part.ToolRequest.Ref != "call_123" {
		t.Errorf("expected ref 'call_123', got %s", part.ToolRequest.Ref)
	}
}

func TestModelLookupWithName(t *testing.T) {
	ctx := context.Background()
	plugin := &OpenRouter{
		APIKey: "test-api-key",
	}

	g := genkit.Init(ctx, genkit.WithPlugins(plugin))

	// Model should not exist before defining
	if IsDefinedModel(g, "fast") {
		t.Error("model should not be defined before DefineModel")
	}

	// Define model with custom name - registers as "openrouter/fast"
	plugin.DefineModel(g, ModelDefinition{
		Name:   "fast",
		Models: []string{"anthropic/claude-3-haiku", "openai/gpt-4o-mini"},
	}, nil)

	// Model should exist after defining
	if !IsDefinedModel(g, "fast") {
		t.Error("model should be defined after DefineModel")
	}

	// Lookup should return the model
	model := Model(g, "fast")
	if model == nil {
		t.Error("Model() should return non-nil after DefineModel")
	}
}

func TestModelLookupWithoutName(t *testing.T) {
	ctx := context.Background()
	plugin := &OpenRouter{
		APIKey: "test-api-key",
	}

	g := genkit.Init(ctx, genkit.WithPlugins(plugin))

	// Define model without custom name - registers as "openrouter/anthropic/claude-3-opus"
	plugin.DefineModel(g, ModelDefinition{
		Models: []string{"anthropic/claude-3-opus"},
	}, nil)

	// Model should exist using the first model ID as the name
	if !IsDefinedModel(g, "anthropic/claude-3-opus") {
		t.Error("model should be defined using first model ID as name")
	}

	model := Model(g, "anthropic/claude-3-opus")
	if model == nil {
		t.Error("Model() should return non-nil after DefineModel")
	}
}

func TestModelDefinitionWithModels(t *testing.T) {
	// Test with custom name and multiple models (fallbacks)
	md := ModelDefinition{
		Name:   "expensive",
		Models: []string{"anthropic/claude-3-opus", "anthropic/claude-3-sonnet", "openai/gpt-4"},
		Label:  "Expensive Models with Fallbacks",
	}

	if md.Name != "expensive" {
		t.Errorf("expected Name 'expensive', got %s", md.Name)
	}

	if len(md.Models) != 3 {
		t.Errorf("expected 3 models, got %d", len(md.Models))
	}

	if md.Models[0] != "anthropic/claude-3-opus" {
		t.Errorf("expected first model 'anthropic/claude-3-opus', got %s", md.Models[0])
	}

	if md.Label != "Expensive Models with Fallbacks" {
		t.Errorf("expected Label 'Expensive Models with Fallbacks', got %s", md.Label)
	}
}

func TestModelDefinitionWithoutName(t *testing.T) {
	// Test without custom name - should use first model as registration name
	md := ModelDefinition{
		Models: []string{"openai/gpt-4o"},
		Label:  "GPT-4o",
	}

	if md.Name != "" {
		t.Errorf("expected empty Name, got %s", md.Name)
	}

	if len(md.Models) != 1 {
		t.Errorf("expected 1 model, got %d", len(md.Models))
	}

	if md.Models[0] != "openai/gpt-4o" {
		t.Errorf("expected model 'openai/gpt-4o', got %s", md.Models[0])
	}
}

func TestApplyConfig(t *testing.T) {
	req := &openrouter.ChatCompletionRequest{}

	// Test with pointer to GenerationCommonConfig
	config := &ai.GenerationCommonConfig{
		Temperature:     0.7,
		TopP:            0.9,
		MaxOutputTokens: 1000,
		StopSequences:   []string{"END"},
	}

	applyConfig(req, config)

	if req.Temperature != 0.7 {
		t.Errorf("expected Temperature 0.7, got %f", req.Temperature)
	}
	if req.TopP != 0.9 {
		t.Errorf("expected TopP 0.9, got %f", req.TopP)
	}
	if req.MaxTokens != 1000 {
		t.Errorf("expected MaxTokens 1000, got %d", req.MaxTokens)
	}
	if len(req.Stop) != 1 || req.Stop[0] != "END" {
		t.Errorf("expected Stop [\"END\"], got %v", req.Stop)
	}
}

func TestApplyConfigFromMap(t *testing.T) {
	req := &openrouter.ChatCompletionRequest{}

	config := map[string]any{
		"temperature":     0.8,
		"topP":            0.95,
		"maxOutputTokens": 2000.0,
	}

	applyConfig(req, config)

	if req.Temperature != 0.8 {
		t.Errorf("expected Temperature 0.8, got %f", req.Temperature)
	}
	if req.TopP != 0.95 {
		t.Errorf("expected TopP 0.95, got %f", req.TopP)
	}
	if req.MaxTokens != 2000 {
		t.Errorf("expected MaxTokens 2000, got %d", req.MaxTokens)
	}
}

// Integration test - only runs when OPENROUTER_API_KEY is set
func TestIntegration(t *testing.T) {
	apiKey := os.Getenv("OPENROUTER_API_KEY")
	if apiKey == "" {
		t.Skip("Skipping integration test: OPENROUTER_API_KEY not set")
	}

	ctx := context.Background()
	plugin := &OpenRouter{
		APIKey: apiKey,
	}

	g := genkit.Init(ctx, genkit.WithPlugins(plugin))

	// Use a free model for testing - registers as "openrouter/free"
	model := plugin.DefineModel(g, ModelDefinition{
		Name:   "free",
		Models: []string{"google/gemini-2.0-flash-exp:free"},
		Label:  "Gemini Flash (Free)",
	}, nil)

	resp, err := model.Generate(ctx, &ai.ModelRequest{
		Messages: []*ai.Message{
			{
				Role:    ai.RoleUser,
				Content: []*ai.Part{ai.NewTextPart("Say 'Hello' and nothing else.")},
			},
		},
		Config: &ai.GenerationCommonConfig{
			MaxOutputTokens: 10,
		},
	}, nil)

	if err != nil {
		t.Fatalf("failed to generate: %v", err)
	}

	if resp == nil {
		t.Fatal("response is nil")
	}

	if len(resp.Message.Content) == 0 {
		t.Fatal("response has no content")
	}

	t.Logf("Response: %s", resp.Message.Content[0].Text)
}
