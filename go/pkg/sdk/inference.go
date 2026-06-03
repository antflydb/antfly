/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	chunking "github.com/antflydb/antfly/go/pkg/libaf/chunking"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// NewChatMessage creates a ChatMessage with string content.
// This is a convenience helper for the common case of text-only messages.
func NewChatMessage(role oapi.InferenceRole, content string) oapi.InferenceChatMessage {
	msg := oapi.InferenceChatMessage{Role: role}
	raw, _ := json.Marshal(content)
	msg.Content = oapi.ChatMessageContent(raw)
	return msg
}

// NewUserMessage creates a user ChatMessage with string content.
func NewUserMessage(content string) oapi.InferenceChatMessage {
	return NewChatMessage(oapi.ChatMessageRoleUser, content)
}

// NewSystemMessage creates a system ChatMessage with string content.
func NewSystemMessage(content string) oapi.InferenceChatMessage {
	return NewChatMessage(oapi.ChatMessageRoleSystem, content)
}

// NewAssistantMessage creates an assistant ChatMessage with string content.
func NewAssistantMessage(content string) oapi.InferenceChatMessage {
	return NewChatMessage(oapi.ChatMessageRoleAssistant, content)
}

// NewMultimodalUserMessage creates a user ChatMessage with text and image content.
// The imageDataURI should be a base64 data URI like "data:image/png;base64,...".
func NewMultimodalUserMessage(text string, imageDataURIs ...string) (oapi.InferenceChatMessage, error) {
	var parts []oapi.ContentPart

	// Add text part if provided
	if text != "" {
		var textPart oapi.ContentPart
		if err := textPart.FromTextContentPart(oapi.TextContentPart{
			Type: oapi.TextContentPartTypeText,
			Text: text,
		}); err != nil {
			return oapi.InferenceChatMessage{}, fmt.Errorf("creating text part: %w", err)
		}
		parts = append(parts, textPart)
	}

	// Add image parts
	for _, dataURI := range imageDataURIs {
		var imagePart oapi.ContentPart
		if err := imagePart.FromImageURLContentPart(oapi.ImageURLContentPart{
			Type: oapi.ImageURLContentPartTypeImageUrl,
			ImageUrl: oapi.ImageURL{
				Url: dataURI,
			},
		}); err != nil {
			return oapi.InferenceChatMessage{}, fmt.Errorf("creating image part: %w", err)
		}
		parts = append(parts, imagePart)
	}

	msg := oapi.InferenceChatMessage{Role: oapi.ChatMessageRoleUser}
	raw, err := json.Marshal(parts)
	if err != nil {
		return oapi.InferenceChatMessage{}, fmt.Errorf("setting content parts: %w", err)
	}
	msg.Content = oapi.ChatMessageContent(raw)
	return msg, nil
}

// InferenceClient is a client for interacting with the Antfly inference API.
type InferenceClient struct {
	client  *oapi.ClientWithResponses
	baseURL string
}

// NewInferenceClient creates a new inference client.
// The baseURL should be the server address (e.g., "http://localhost:8080").
// Legacy base URLs ending in /ai/v1 are accepted and normalized.
func NewInferenceClient(baseURL string, httpClient *http.Client) (*InferenceClient, error) {
	apiURL := normalizeInferenceBaseURL(baseURL)

	var opts []oapi.ClientOption
	if httpClient != nil {
		opts = append(opts, oapi.WithHTTPClient(httpClient))
	}

	client, err := oapi.NewClientWithResponses(apiURL, opts...)
	if err != nil {
		return nil, err
	}
	return &InferenceClient{
		client:  client,
		baseURL: apiURL,
	}, nil
}

// Client returns the underlying oapi-codegen client for direct API access.
func (c *InferenceClient) Client() *oapi.ClientWithResponses {
	return c.client
}

func normalizeInferenceBaseURL(baseURL string) string {
	return strings.TrimSuffix(strings.TrimRight(baseURL, "/"), "/ai/v1")
}

// Embed generates embeddings for the given text strings.
// Returns embeddings in binary format (most efficient).
func (c *InferenceClient) Embed(ctx context.Context, model string, input []string) ([][]float32, error) {
	// Build the input union type
	var inputUnion oapi.InferenceEmbedRequest_Input
	if err := inputUnion.FromInferenceEmbedRequestInput1(input); err != nil {
		return nil, fmt.Errorf("building input: %w", err)
	}

	req := oapi.InferenceEmbedRequest{
		Model: model,
		Input: inputUnion,
	}

	// Make request - server defaults to binary response (most efficient)
	resp, err := c.client.GenerateEmbeddingsWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	// Check for error responses
	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}

	// Check content type to determine response format
	contentType := resp.HTTPResponse.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		// JSON response
		if resp.JSON200 != nil {
			return denseEmbeddings(resp.JSON200)
		}
		return nil, fmt.Errorf("unexpected JSON response: %s", string(resp.Body))
	}

	// Binary response (default)
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	embeddings, err := deserializeFloatArrays(bytes.NewReader(resp.Body))
	if err != nil {
		return nil, fmt.Errorf("deserializing embeddings: %w", err)
	}

	return embeddings, nil
}

// EmbedMultimodal generates embeddings for multimodal content parts (text, images, audio).
// Each ContentPart can be a TextContentPart or ImageURLContentPart (with URL or data URI).
// Returns embeddings in binary format (most efficient).
func (c *InferenceClient) EmbedMultimodal(ctx context.Context, model string, input []oapi.ContentPart) ([][]float32, error) {
	var inputUnion oapi.InferenceEmbedRequest_Input
	if err := inputUnion.FromInferenceEmbedRequestInput2(input); err != nil {
		return nil, fmt.Errorf("building input: %w", err)
	}

	req := oapi.InferenceEmbedRequest{
		Model: model,
		Input: inputUnion,
	}

	resp, err := c.client.GenerateEmbeddingsWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}

	contentType := resp.HTTPResponse.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		if resp.JSON200 != nil {
			return denseEmbeddings(resp.JSON200)
		}
		return nil, fmt.Errorf("unexpected JSON response: %s", string(resp.Body))
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	embeddings, err := deserializeFloatArrays(bytes.NewReader(resp.Body))
	if err != nil {
		return nil, fmt.Errorf("deserializing embeddings: %w", err)
	}

	return embeddings, nil
}

// EmbedJSON generates embeddings and returns JSON response (includes model name).
func (c *InferenceClient) EmbedJSON(ctx context.Context, model string, input []string) (*oapi.InferenceEmbedResponse, error) {
	var inputUnion oapi.InferenceEmbedRequest_Input
	if err := inputUnion.FromInferenceEmbedRequestInput1(input); err != nil {
		return nil, fmt.Errorf("building input: %w", err)
	}

	req := oapi.InferenceEmbedRequest{
		Model: model,
		Input: inputUnion,
	}

	resp, err := c.client.GenerateEmbeddingsWithResponse(ctx, req, func(ctx context.Context, req *http.Request) error {
		req.Header.Set("Accept", "application/json")
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200, nil
}

// ChunkConfig contains configuration for text chunking.
type ChunkConfig struct {
	Model         string
	TargetTokens  int
	OverlapTokens int
	Separator     string
	MaxChunks     int
	Threshold     float32
}

// Chunk splits text into smaller segments using semantic or fixed-size chunking.
func (c *InferenceClient) Chunk(ctx context.Context, text string, config ChunkConfig) ([]chunking.Chunk, error) {
	var input oapi.InferenceChunkRequest_Input
	if err := input.FromInferenceChunkRequestInput0(text); err != nil {
		return nil, fmt.Errorf("building chunk request input: %w", err)
	}

	req := oapi.InferenceChunkRequest{
		Input: input,
		Config: oapi.InferenceChunkConfig{
			Model:     config.Model,
			MaxChunks: config.MaxChunks,
			Threshold: config.Threshold,
			Text: oapi.InferenceTextChunkOptions{
				TargetTokens:  config.TargetTokens,
				OverlapTokens: config.OverlapTokens,
				Separator:     config.Separator,
			},
		},
	}

	resp, err := c.client.ChunkTextWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return chunksFromBody(resp.Body)
}

// MediaChunkConfig contains configuration for media chunking.
type MediaChunkConfig struct {
	Model             string
	MaxChunks         int
	WindowDurationMs  int
	OverlapDurationMs int
	Threshold         float32
}

// ChunkMedia splits binary media content (audio/wav, image/gif) into chunks.
func (c *InferenceClient) ChunkMedia(ctx context.Context, data []byte, mimeType string, config MediaChunkConfig) ([]chunking.Chunk, error) {
	// Build MediaContentPart
	var part oapi.ContentPart
	if err := part.FromMediaContentPart(oapi.MediaContentPart{
		Type:     oapi.MediaContentPartTypeMedia,
		Data:     data,
		MimeType: mimeType,
	}); err != nil {
		return nil, fmt.Errorf("building media content part: %w", err)
	}

	var input oapi.InferenceChunkRequest_Input
	if err := input.FromContentPart(part); err != nil {
		return nil, fmt.Errorf("building chunk request input: %w", err)
	}

	req := oapi.InferenceChunkRequest{
		Input: input,
		Config: oapi.InferenceChunkConfig{
			Model:     config.Model,
			MaxChunks: config.MaxChunks,
			Threshold: config.Threshold,
			Audio: oapi.InferenceAudioChunkConfig{
				WindowDurationMs:  config.WindowDurationMs,
				OverlapDurationMs: config.OverlapDurationMs,
			},
		},
	}

	resp, err := c.client.ChunkTextWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return chunksFromBody(resp.Body)
}

// Rerank re-scores pre-rendered text prompts based on relevance to a query.
func (c *InferenceClient) Rerank(ctx context.Context, model string, query string, prompts []string) ([]float32, error) {
	req := oapi.InferenceRerankRequest{
		Model:   model,
		Query:   query,
		Prompts: prompts,
	}

	resp, err := c.client.RerankPromptsWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON503 != nil {
		return nil, fmt.Errorf("service unavailable: %s", resp.JSON503.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return rerankScores(resp.JSON200), nil
}

// ListModels returns available models for embedding, chunking, and reranking.
func (c *InferenceClient) ListModels(ctx context.Context) (*oapi.InferenceModelsResponse, error) {
	resp, err := c.client.ListModelsWithResponse(ctx)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200, nil
}

// Extract runs schema-driven extraction through the canonical Antfly inference
// extraction endpoint.
func (c *InferenceClient) Extract(ctx context.Context, req oapi.ExtractionRequest) (*oapi.ExtractionResponse, error) {
	resp, err := c.client.ExtractWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON503 != nil {
		return nil, fmt.Errorf("service unavailable: %s", resp.JSON503.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200, nil
}

// Classification is one label score returned by ExtractClassifications.
type Classification struct {
	Name  string
	Label string
	Score float32
}

// ClassificationResult contains classification results for one input text.
type ClassificationResult struct {
	Index           int
	Classifications []Classification
}

// ExtractedEntity is one named entity returned by ExtractEntities.
type ExtractedEntity struct {
	Text  string
	Label string
	Score float32
	Start int
	End   int
}

// EntityExtractionResult contains named entities for one input text.
type EntityExtractionResult struct {
	Index    int
	Entities []ExtractedEntity
}

// ExtractClassifications performs zero-shot classification through the
// canonical extraction endpoint.
func (c *InferenceClient) ExtractClassifications(ctx context.Context, model string, texts []string, labels []string) ([]ClassificationResult, error) {
	inputs, err := newTextExtractionInputs(texts)
	if err != nil {
		return nil, err
	}
	extracted, err := c.Extract(ctx, oapi.ExtractionRequest{
		Model:  model,
		Inputs: inputs,
		Schema: oapi.ExtractionSchema{
			Classifications: []oapi.ExtractionClassificationSchema{{
				Name:   "classification",
				Labels: labels,
			}},
		},
		Options: oapi.ExtractionOptions{
			IncludeConfidence: true,
		},
	})
	if err != nil {
		return nil, err
	}

	out := make([]ClassificationResult, len(extracted.Data))
	for i, item := range extracted.Data {
		classifications := make([]Classification, 0, len(item.Classifications))
		for _, classification := range item.Classifications {
			classifications = append(classifications, Classification{
				Name:  classification.Name,
				Label: classification.Label,
				Score: classification.Score,
			})
		}
		out[i] = ClassificationResult{
			Index:           i,
			Classifications: classifications,
		}
	}
	return out, nil
}

// ExtractEntities extracts named entities through the canonical extraction
// endpoint.
func (c *InferenceClient) ExtractEntities(ctx context.Context, model string, texts []string, labels []string) ([]EntityExtractionResult, error) {
	inputs, err := newTextExtractionInputs(texts)
	if err != nil {
		return nil, err
	}
	extracted, err := c.Extract(ctx, oapi.ExtractionRequest{
		Model:  model,
		Inputs: inputs,
		Schema: oapi.ExtractionSchema{
			Entities: labels,
		},
		Options: oapi.ExtractionOptions{
			FlatNer:           true,
			IncludeConfidence: true,
			IncludeSpans:      true,
		},
	})
	if err != nil {
		return nil, err
	}

	out := make([]EntityExtractionResult, len(extracted.Data))
	for i, item := range extracted.Data {
		entities := make([]ExtractedEntity, 0, len(item.Entities))
		for _, entity := range item.Entities {
			entities = append(entities, ExtractedEntity{
				Text:  entity.Text,
				Label: entity.Label,
				Score: entity.Score,
				Start: entity.Start,
				End:   entity.End,
			})
		}
		out[i] = EntityExtractionResult{
			Index:    i,
			Entities: entities,
		}
	}
	return out, nil
}

func newTextExtractionInputs(texts []string) ([]oapi.ExtractionInput, error) {
	inputs := make([]oapi.ExtractionInput, len(texts))
	for i, text := range texts {
		raw, err := json.Marshal(text)
		if err != nil {
			return nil, err
		}
		inputs[i] = oapi.ExtractionInput{
			Content: oapi.ChatMessageContent(raw),
		}
	}
	return inputs, nil
}

// RewriteText rewrites input texts using a Seq2Seq rewriter model.
func (c *InferenceClient) RewriteText(ctx context.Context, model string, inputs []string) (*oapi.InferenceRewriteResponse, error) {
	req := oapi.InferenceRewriteRequest{
		Model:  model,
		Inputs: inputs,
	}

	resp, err := c.client.RewriteTextWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON503 != nil {
		return nil, fmt.Errorf("service unavailable: %s", resp.JSON503.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200, nil
}

// Transcribe transcribes audio to text using a speech-to-text model.
// The audio should be base64-encoded audio data (WAV, MP3, FLAC, etc.).
// Model is optional - if empty, uses the default transcriber model.
// Language is optional - if empty, the model will auto-detect.
func (c *InferenceClient) Transcribe(ctx context.Context, model string, audio []byte, language string) (*oapi.InferenceTranscribeResponse, error) {
	req := oapi.InferenceTranscribeRequest{
		Model:    model,
		Audio:    audio,
		Language: language,
	}

	resp, err := c.client.TranscribeAudioWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON503 != nil {
		return nil, fmt.Errorf("service unavailable: %s", resp.JSON503.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200, nil
}

// GenerateConfig contains configuration for text generation.
type GenerateConfig struct {
	MaxTokens   int
	Temperature float32
	TopP        float32
	TopK        int
	Tools       []oapi.InferenceTool
	ToolChoice  oapi.InferenceToolChoice
}

// ToolChoiceAuto returns a ToolChoice that lets the model decide whether to call a tool.
func ToolChoiceAuto() oapi.InferenceToolChoice {
	var tc oapi.InferenceToolChoice
	_ = tc.FromInferenceToolChoice0(oapi.InferenceToolChoice0Auto)
	return tc
}

// ToolChoiceNone returns a ToolChoice that prevents the model from calling any tools.
func ToolChoiceNone() oapi.InferenceToolChoice {
	var tc oapi.InferenceToolChoice
	_ = tc.FromInferenceToolChoice0(oapi.InferenceToolChoice0None)
	return tc
}

// ToolChoiceRequired returns a ToolChoice that forces the model to call at least one tool.
func ToolChoiceRequired() oapi.InferenceToolChoice {
	var tc oapi.InferenceToolChoice
	_ = tc.FromInferenceToolChoice0(oapi.InferenceToolChoice0Required)
	return tc
}

// ToolChoiceFunction returns a ToolChoice that forces the model to call a specific function.
func ToolChoiceFunction(name string) oapi.InferenceToolChoice {
	var tc oapi.InferenceToolChoice
	_ = tc.FromInferenceToolChoice1(oapi.InferenceToolChoice1{
		Type: oapi.InferenceToolChoice1TypeFunction,
		Function: struct {
			Name string `json:"name"`
		}{Name: name},
	})
	return tc
}

// Generate generates text using an LLM model (non-streaming).
func (c *InferenceClient) Generate(ctx context.Context, model string, messages []oapi.InferenceChatMessage, config *GenerateConfig) (*oapi.InferenceGenerateResponse, error) {
	req := oapi.InferenceGenerateRequest{
		Model:    model,
		Messages: messages,
	}

	if config != nil {
		if config.MaxTokens > 0 {
			req.MaxTokens = config.MaxTokens
		}
		if config.Temperature > 0 {
			req.Temperature = config.Temperature
		}
		if config.TopP > 0 {
			req.TopP = config.TopP
		}
		if config.TopK > 0 {
			req.TopK = config.TopK
		}
		if len(config.Tools) > 0 {
			req.Tools = config.Tools
		}
		req.ToolChoice = config.ToolChoice
	}

	resp, err := c.client.GenerateContentWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON503 != nil {
		return nil, fmt.Errorf("service unavailable: %s", resp.JSON503.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200, nil
}

// SparseVector represents a sparse embedding vector with parallel index/value arrays.
type SparseVector struct {
	Indices []int32   `json:"indices"`
	Values  []float32 `json:"values"`
}

// SparseEmbed generates sparse embeddings for the given text strings.
// Returns sparse vectors in binary format (most efficient).
// Only valid for models with the "sparse" capability.
func (c *InferenceClient) SparseEmbed(ctx context.Context, model string, input []string) ([]SparseVector, error) {
	// Build the input union type
	var inputUnion oapi.InferenceEmbedRequest_Input
	if err := inputUnion.FromInferenceEmbedRequestInput1(input); err != nil {
		return nil, fmt.Errorf("building input: %w", err)
	}

	req := oapi.InferenceEmbedRequest{
		Model: model,
		Input: inputUnion,
	}

	// Make request - server returns sparse binary format for sparse models
	resp, err := c.client.GenerateEmbeddingsWithResponse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}

	contentType := resp.HTTPResponse.Header.Get("Content-Type")

	if strings.Contains(contentType, "application/json") {
		// JSON response.
		if resp.JSON200 != nil {
			return sparseEmbeddings(resp.JSON200)
		}
		return nil, fmt.Errorf("unexpected JSON response: %s", string(resp.Body))
	}

	// Binary response (application/x-sparse-vectors)
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return deserializeSparseVectors(bytes.NewReader(resp.Body))
}

// SparseEmbedJSON generates sparse embeddings and returns JSON response.
func (c *InferenceClient) SparseEmbedJSON(ctx context.Context, model string, input []string) (*oapi.InferenceEmbedResponse, error) {
	var inputUnion oapi.InferenceEmbedRequest_Input
	if err := inputUnion.FromInferenceEmbedRequestInput1(input); err != nil {
		return nil, fmt.Errorf("building input: %w", err)
	}

	req := oapi.InferenceEmbedRequest{
		Model: model,
		Input: inputUnion,
	}

	resp, err := c.client.GenerateEmbeddingsWithResponse(ctx, req, func(ctx context.Context, req *http.Request) error {
		req.Header.Set("Accept", "application/json")
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	if resp.JSON400 != nil {
		return nil, fmt.Errorf("bad request: %s", resp.JSON400.Error)
	}
	if resp.JSON404 != nil {
		return nil, fmt.Errorf("model not found: %s", resp.JSON404.Error)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("server error: %s", resp.JSON500.Error)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}

	return resp.JSON200, nil
}

func denseEmbeddings(resp *oapi.InferenceEmbedResponse) ([][]float32, error) {
	embeddings := make([][]float32, len(resp.Data))
	for i, item := range resp.Data {
		embedding, err := item.Embedding.AsInferenceEmbeddingObjectEmbedding0()
		if err != nil {
			return nil, fmt.Errorf("decoding dense embedding %d: %w", i, err)
		}
		embeddings[i] = embedding
	}
	return embeddings, nil
}

func sparseEmbeddings(resp *oapi.InferenceEmbedResponse) ([]SparseVector, error) {
	embeddings := make([]SparseVector, len(resp.Data))
	for i, item := range resp.Data {
		embedding, err := item.Embedding.AsInferenceSparseVector()
		if err != nil {
			return nil, fmt.Errorf("decoding sparse embedding %d: %w", i, err)
		}
		embeddings[i] = SparseVector{
			Indices: embedding.Indices,
			Values:  embedding.Values,
		}
	}
	return embeddings, nil
}

func chunksFromBody(body []byte) ([]chunking.Chunk, error) {
	var decoded struct {
		Data   []chunking.Chunk `json:"data"`
		Chunks []chunking.Chunk `json:"chunks"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("decoding chunks: %w", err)
	}
	if decoded.Data != nil {
		return decoded.Data, nil
	}
	return decoded.Chunks, nil
}

func rerankScores(resp *oapi.InferenceRerankResponse) []float32 {
	scores := make([]float32, len(resp.Data))
	for i, item := range resp.Data {
		scores[i] = item.Score
	}
	return scores
}

// deserializeSparseVectors reads sparse vectors from binary format.
// Format: [uint64 num_vectors] per vector: [uint32 nnz] [int32*nnz indices] [float32*nnz values]
func deserializeSparseVectors(r io.Reader) ([]SparseVector, error) {
	var numVectors uint64
	if err := binary.Read(r, binary.LittleEndian, &numVectors); err != nil {
		return nil, fmt.Errorf("reading num vectors: %w", err)
	}
	if numVectors == 0 {
		return []SparseVector{}, nil
	}
	result := make([]SparseVector, numVectors)
	for i := range numVectors {
		var nnz uint32
		if err := binary.Read(r, binary.LittleEndian, &nnz); err != nil {
			return nil, fmt.Errorf("reading nnz for vector %d: %w", i, err)
		}
		indices := make([]int32, nnz)
		for j := range nnz {
			if err := binary.Read(r, binary.LittleEndian, &indices[j]); err != nil {
				return nil, fmt.Errorf("reading index %d for vector %d: %w", j, i, err)
			}
		}
		values := make([]float32, nnz)
		for j := range nnz {
			if err := binary.Read(r, binary.LittleEndian, &values[j]); err != nil {
				return nil, fmt.Errorf("reading value %d for vector %d: %w", j, i, err)
			}
		}
		result[i] = SparseVector{
			Indices: indices,
			Values:  values,
		}
	}
	return result, nil
}

// deserializeFloatArrays reconstructs a 2D float32 array from binary format.
// Format: uint64(numVectors) + uint64(dimension) + float32 values in little endian
func deserializeFloatArrays(r io.Reader) ([][]float32, error) {
	var numVectors uint64
	if err := binary.Read(r, binary.LittleEndian, &numVectors); err != nil {
		return nil, fmt.Errorf("reading number of vectors: %w", err)
	}
	if numVectors == 0 {
		return [][]float32{}, nil
	}
	var dimension uint64
	if err := binary.Read(r, binary.LittleEndian, &dimension); err != nil {
		return nil, fmt.Errorf("reading dimension: %w", err)
	}
	result := make([][]float32, numVectors)
	for i := range numVectors {
		result[i] = make([]float32, dimension)
		for j := range dimension {
			if err := binary.Read(r, binary.LittleEndian, &result[i][j]); err != nil {
				return nil, fmt.Errorf("reading vector %d, dimension %d: %w", i, j, err)
			}
		}
	}
	return result, nil
}
