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
//
// SPDX-License-Identifier: Apache-2.0

// Package openrouter provides a Genkit plugin for OpenRouter.
// OpenRouter provides a unified API to access various LLM providers.
package openrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
	"github.com/revrost/go-openrouter"
)

const provider = "openrouter"

var roleMapping = map[ai.Role]string{
	ai.RoleUser:   openrouter.ChatMessageRoleUser,
	ai.RoleModel:  openrouter.ChatMessageRoleAssistant,
	ai.RoleSystem: openrouter.ChatMessageRoleSystem,
	ai.RoleTool:   openrouter.ChatMessageRoleTool,
}

// OpenRouter provides configuration options for the plugin.
type OpenRouter struct {
	// APIKey is the OpenRouter API key. If empty, reads from OPENROUTER_API_KEY env var.
	APIKey string
	// SiteName is an optional site name for OpenRouter analytics.
	SiteName string
	// SiteURL is an optional site URL for OpenRouter analytics.
	SiteURL string

	client  *openrouter.Client
	mu      sync.Mutex
	initted bool
}

// Name returns the provider name.
func (o *OpenRouter) Name() string {
	return provider
}

// Init initializes the plugin.
func (o *OpenRouter) Init(ctx context.Context) []api.Action {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.initted {
		panic("openrouter.Init already called")
	}
	if o.APIKey == "" {
		o.APIKey = os.Getenv("OPENROUTER_API_KEY")
	}
	if o.APIKey == "" {
		panic("openrouter: need APIKey or OPENROUTER_API_KEY environment variable")
	}

	// Build client options
	opts := []openrouter.Option{}
	if o.SiteName != "" {
		opts = append(opts, openrouter.WithXTitle(o.SiteName))
	}
	if o.SiteURL != "" {
		opts = append(opts, openrouter.WithHTTPReferer(o.SiteURL))
	}

	o.client = openrouter.NewClient(o.APIKey, opts...)
	o.initted = true
	return []api.Action{}
}

// ModelDefinition represents a model configuration.
type ModelDefinition struct {
	// Name is an optional label for the model (e.g., "fast", "expensive").
	// If provided, the model registers as "openrouter/{Name}" (e.g., "openrouter/fast").
	// If empty, the first model in Models is used as the name (e.g., "openrouter/anthropic/claude-3-opus").
	Name string
	// Models is the list of OpenRouter model IDs to use, in order of preference.
	// If multiple models are specified, OpenRouter will try them in order as fallbacks.
	// At least one model must be specified.
	Models []string
	// Label is an optional human-readable label for display purposes.
	Label string
}

// DefineModel registers a model with Genkit.
func (o *OpenRouter) DefineModel(g *genkit.Genkit, model ModelDefinition, opts *ai.ModelOptions) ai.Model {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.initted {
		panic("openrouter.Init not called")
	}

	if len(model.Models) == 0 {
		panic("openrouter: ModelDefinition.Models must have at least one model")
	}

	// Determine the registration name:
	// - If Name is provided, use it (e.g., "fast" -> "openrouter/fast")
	// - Otherwise, use the first model ID (e.g., "anthropic/claude-3-opus" -> "openrouter/anthropic/claude-3-opus")
	registrationName := model.Name
	if registrationName == "" {
		registrationName = model.Models[0]
	}

	var modelOpts ai.ModelOptions
	if opts != nil {
		modelOpts = *opts
	} else {
		modelOpts = ai.ModelOptions{
			Label: model.Label,
			Supports: &ai.ModelSupports{
				Multiturn:  true,
				SystemRole: true,
				Media:      true,
				Tools:      true,
			},
			Versions: []string{},
		}
	}
	if modelOpts.Label == "" {
		modelOpts.Label = "OpenRouter - " + registrationName
	}

	meta := &ai.ModelOptions{
		Label:    modelOpts.Label,
		Supports: modelOpts.Supports,
		Versions: modelOpts.Versions,
	}

	gen := &generator{
		model:  model,
		client: o.client,
	}

	return genkit.DefineModel(g, api.NewName(provider, registrationName), meta, gen.generate)
}

// IsDefinedModel reports whether a model is defined.
func IsDefinedModel(g *genkit.Genkit, name string) bool {
	return genkit.LookupModel(g, api.NewName(provider, name)) != nil
}

// Model returns the [ai.Model] with the given name.
func Model(g *genkit.Genkit, name string) ai.Model {
	return genkit.LookupModel(g, api.NewName(provider, name))
}

// generator handles API requests.
type generator struct {
	model  ModelDefinition
	client *openrouter.Client
}

// generate handles the chat completion request.
func (g *generator) generate(ctx context.Context, input *ai.ModelRequest, cb func(context.Context, *ai.ModelResponseChunk) error) (*ai.ModelResponse, error) {
	stream := cb != nil

	// Convert messages
	messages, err := convertMessages(input.Messages)
	if err != nil {
		return nil, fmt.Errorf("failed to convert messages: %w", err)
	}

	// Build request
	req := openrouter.ChatCompletionRequest{
		Messages: messages,
		Stream:   stream,
	}

	// Use "models" array if multiple models specified (for fallbacks), otherwise use single "model"
	if len(g.model.Models) > 1 {
		req.Models = g.model.Models
	} else {
		req.Model = g.model.Models[0]
	}

	// Add generation config
	if input.Config != nil {
		applyConfig(&req, input.Config)
	}

	// Add tools
	if len(input.Tools) > 0 {
		req.Tools = convertTools(input.Tools)
	}

	if stream {
		return g.handleStreamingResponse(ctx, req, input, cb)
	}
	return g.handleNonStreamingResponse(ctx, req, input)
}

func applyConfig(req *openrouter.ChatCompletionRequest, config any) {
	if cfg, ok := config.(*ai.GenerationCommonConfig); ok && cfg != nil {
		if cfg.Temperature != 0 {
			req.Temperature = float32(cfg.Temperature)
		}
		if cfg.TopP != 0 {
			req.TopP = float32(cfg.TopP)
		}
		if cfg.MaxOutputTokens != 0 {
			req.MaxTokens = cfg.MaxOutputTokens
		}
		if len(cfg.StopSequences) > 0 {
			req.Stop = cfg.StopSequences
		}
	} else if cfg, ok := config.(ai.GenerationCommonConfig); ok {
		if cfg.Temperature != 0 {
			req.Temperature = float32(cfg.Temperature)
		}
		if cfg.TopP != 0 {
			req.TopP = float32(cfg.TopP)
		}
		if cfg.MaxOutputTokens != 0 {
			req.MaxTokens = cfg.MaxOutputTokens
		}
		if len(cfg.StopSequences) > 0 {
			req.Stop = cfg.StopSequences
		}
	} else if cfgMap, ok := config.(map[string]any); ok {
		if temp, ok := cfgMap["temperature"].(float64); ok && temp != 0 {
			req.Temperature = float32(temp)
		}
		if topP, ok := cfgMap["topP"].(float64); ok && topP != 0 {
			req.TopP = float32(topP)
		}
		if maxTokens, ok := cfgMap["maxOutputTokens"].(float64); ok && maxTokens != 0 {
			req.MaxTokens = int(maxTokens)
		}
		if stop, ok := cfgMap["stopSequences"].([]string); ok && len(stop) > 0 {
			req.Stop = stop
		}
	}
}

func (g *generator) handleNonStreamingResponse(ctx context.Context, req openrouter.ChatCompletionRequest, input *ai.ModelRequest) (*ai.ModelResponse, error) {
	resp, err := g.client.CreateChatCompletion(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("OpenRouter API error: %w", err)
	}

	return translateResponse(&resp, input)
}

func (g *generator) handleStreamingResponse(ctx context.Context, req openrouter.ChatCompletionRequest, input *ai.ModelRequest, cb func(context.Context, *ai.ModelResponseChunk) error) (*ai.ModelResponse, error) {
	stream, err := g.client.CreateChatCompletionStream(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("OpenRouter API error: %w", err)
	}
	defer stream.Close()

	var chunks []*ai.ModelResponseChunk

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("stream error: %w", err)
		}

		chunk := translateStreamChunk(&resp)
		if chunk != nil && len(chunk.Content) > 0 {
			chunks = append(chunks, chunk)
			if err := cb(ctx, chunk); err != nil {
				return nil, err
			}
		}
	}

	// Build final response
	finalResponse := &ai.ModelResponse{
		Request:      input,
		FinishReason: ai.FinishReason("stop"),
		Message: &ai.Message{
			Role: ai.RoleModel,
		},
	}
	for _, chunk := range chunks {
		finalResponse.Message.Content = append(finalResponse.Message.Content, chunk.Content...)
	}

	return finalResponse, nil
}

func convertMessages(messages []*ai.Message) ([]openrouter.ChatCompletionMessage, error) {
	result := make([]openrouter.ChatCompletionMessage, 0, len(messages))

	for _, msg := range messages {
		converted, err := convertMessage(msg)
		if err != nil {
			return nil, err
		}
		result = append(result, converted...)
	}

	return result, nil
}

func convertMessage(msg *ai.Message) ([]openrouter.ChatCompletionMessage, error) {
	role := roleMapping[msg.Role]
	if role == "" {
		role = openrouter.ChatMessageRoleUser
	}

	// Check if this is a tool response
	if msg.Role == ai.RoleTool {
		var messages []openrouter.ChatCompletionMessage
		for _, part := range msg.Content {
			if part.IsToolResponse() {
				outputJSON, err := json.Marshal(part.ToolResponse.Output)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal tool response: %w", err)
				}
				messages = append(messages, openrouter.ChatCompletionMessage{
					Role: openrouter.ChatMessageRoleTool,
					Content: openrouter.Content{
						Text: string(outputJSON),
					},
					ToolCallID: part.ToolResponse.Ref,
				})
			}
		}
		return messages, nil
	}

	// Check for tool calls (from model)
	var toolCalls []openrouter.ToolCall
	var contentParts []openrouter.ChatMessagePart
	var hasMultiPart bool

	for _, part := range msg.Content {
		if part.IsToolRequest() {
			args, err := json.Marshal(part.ToolRequest.Input)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal tool request args: %w", err)
			}
			toolCalls = append(toolCalls, openrouter.ToolCall{
				ID:   part.ToolRequest.Ref,
				Type: "function",
				Function: openrouter.FunctionCall{
					Name:      part.ToolRequest.Name,
					Arguments: string(args),
				},
			})
		} else if part.IsText() {
			contentParts = append(contentParts, openrouter.ChatMessagePart{
				Type: openrouter.ChatMessagePartTypeText,
				Text: part.Text,
			})
		} else if part.IsMedia() {
			hasMultiPart = true
			mediaURL := part.Text
			if mediaURL == "" && part.ContentType != "" {
				mediaURL = part.ContentType
			}
			contentParts = append(contentParts, openrouter.ChatMessagePart{
				Type: openrouter.ChatMessagePartTypeImageURL,
				ImageURL: &openrouter.ChatMessageImageURL{
					URL: mediaURL,
				},
			})
		}
	}

	chatMsg := openrouter.ChatCompletionMessage{
		Role: role,
	}

	if len(toolCalls) > 0 {
		chatMsg.ToolCalls = toolCalls
	}

	// Set content - use multipart if we have images, otherwise use simple string
	if hasMultiPart || len(contentParts) > 1 {
		chatMsg.Content = openrouter.Content{
			Multi: contentParts,
		}
	} else if len(contentParts) == 1 && contentParts[0].Type == openrouter.ChatMessagePartTypeText {
		chatMsg.Content = openrouter.Content{
			Text: contentParts[0].Text,
		}
	}

	return []openrouter.ChatCompletionMessage{chatMsg}, nil
}

func convertTools(tools []*ai.ToolDefinition) []openrouter.Tool {
	result := make([]openrouter.Tool, 0, len(tools))
	for _, tool := range tools {
		result = append(result, openrouter.Tool{
			Type: openrouter.ToolTypeFunction,
			Function: &openrouter.FunctionDefinition{
				Name:        tool.Name,
				Description: tool.Description,
				Parameters:  tool.InputSchema,
			},
		})
	}
	return result
}

func translateResponse(resp *openrouter.ChatCompletionResponse, input *ai.ModelRequest) (*ai.ModelResponse, error) {
	if len(resp.Choices) == 0 {
		return nil, errors.New("no choices in response")
	}

	choice := resp.Choices[0]
	modelResp := &ai.ModelResponse{
		Request:      input,
		FinishReason: ai.FinishReason(choice.FinishReason),
		Message: &ai.Message{
			Role: ai.RoleModel,
		},
	}

	// Handle tool calls
	if len(choice.Message.ToolCalls) > 0 {
		for _, tc := range choice.Message.ToolCalls {
			var args any
			if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
				args = tc.Function.Arguments
			}
			toolReq := &ai.ToolRequest{
				Ref:   tc.ID,
				Name:  tc.Function.Name,
				Input: args,
			}
			modelResp.Message.Content = append(modelResp.Message.Content, ai.NewToolRequestPart(toolReq))
		}
	}

	// Handle text content
	if choice.Message.Content.Text != "" {
		modelResp.Message.Content = append(modelResp.Message.Content, ai.NewTextPart(choice.Message.Content.Text))
	}

	// Add usage info
	if resp.Usage != nil {
		modelResp.Usage = &ai.GenerationUsage{
			InputTokens:  resp.Usage.PromptTokens,
			OutputTokens: resp.Usage.CompletionTokens,
		}
	}

	return modelResp, nil
}

func translateStreamChunk(resp *openrouter.ChatCompletionStreamResponse) *ai.ModelResponseChunk {
	if len(resp.Choices) == 0 {
		return nil
	}

	choice := resp.Choices[0]
	result := &ai.ModelResponseChunk{}

	// Handle tool calls in stream
	if len(choice.Delta.ToolCalls) > 0 {
		for _, tc := range choice.Delta.ToolCalls {
			var args any
			if tc.Function.Arguments != "" {
				if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
					args = tc.Function.Arguments
				}
			}
			toolReq := &ai.ToolRequest{
				Ref:   tc.ID,
				Name:  tc.Function.Name,
				Input: args,
			}
			result.Content = append(result.Content, ai.NewToolRequestPart(toolReq))
		}
	}

	// Handle text content
	if choice.Delta.Content != "" {
		result.Content = append(result.Content, ai.NewTextPart(choice.Delta.Content))
	}

	if len(result.Content) == 0 {
		return nil
	}

	return result
}
