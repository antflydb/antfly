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

//go:build go1.22

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package termite

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/pkg/libaf/s3"
	"github.com/antflydb/antfly/pkg/libaf/scraping"
	"github.com/antflydb/antfly/pkg/termite/lib/utils"
	termchunking "github.com/antflydb/antfly/pkg/termite/lib/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/classification"
	"github.com/antflydb/antfly/pkg/termite/lib/generation"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/antflydb/antfly/pkg/termite/lib/ner"
	"github.com/antflydb/antfly/pkg/termite/lib/transcribing"
	"go.uber.org/zap"
	_ "golang.org/x/image/webp"
)

// NOTE: SerializeFloatArrays is in codec.go in this package

// TermiteAPI implements the generated ServerInterface
type TermiteAPI struct {
	logger *zap.Logger
	node   *TermiteNode
}

// NewTermiteAPI creates a new HTTP handler for the Termite API using generated code
func NewTermiteAPI(logger *zap.Logger, node *TermiteNode) http.Handler {
	api := &TermiteAPI{
		logger: logger,
		node:   node,
	}
	return HandlerWithOptions(api, StdHTTPServerOptions{
		BaseURL:    "/ml/v1",
		BaseRouter: http.NewServeMux(),
	})
}

// GenerateEmbeddings implements ServerInterface
func (t *TermiteAPI) GenerateEmbeddings(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiEmbed(w, r)
}

// ChunkText implements ServerInterface
func (t *TermiteAPI) ChunkText(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiChunk(w, r)
}

// RerankPrompts implements ServerInterface
func (t *TermiteAPI) RerankPrompts(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiRerank(w, r)
}

// GenerateContent implements ServerInterface
func (t *TermiteAPI) GenerateContent(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiGenerate(w, r)
}

// RecognizeEntities implements ServerInterface
func (t *TermiteAPI) RecognizeEntities(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiRecognize(w, r)
}

// RewriteText implements ServerInterface
func (t *TermiteAPI) RewriteText(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiRewrite(w, r)
}

// ClassifyText implements ServerInterface
func (t *TermiteAPI) ClassifyText(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiClassify(w, r)
}

// ReadImages implements ServerInterface
func (t *TermiteAPI) ReadImages(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiRead(w, r)
}

// TranscribeAudio implements ServerInterface
func (t *TermiteAPI) TranscribeAudio(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiTranscribe(w, r)
}

// ExtractJSON implements ServerInterface
func (t *TermiteAPI) ExtractJSON(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiExtract(w, r)
}

// stringsToModelInfoMap converts a flat list of model names to a map with empty ModelInfo.
func stringsToModelInfoMap(names []string) map[string]ModelInfo {
	m := make(map[string]ModelInfo, len(names))
	for _, name := range names {
		m[name] = ModelInfo{}
	}
	return m
}

// capsMapToModelInfoMap converts a map of model name to capabilities to a ModelInfo map.
// writeModelAcquireError writes an appropriate HTTP error for model acquire failures,
// distinguishing "not found" from load/infrastructure errors.
func writeModelAcquireError(w http.ResponseWriter, logger *zap.Logger, model string, err error) {
	if strings.Contains(err.Error(), "not found") {
		http.Error(w, fmt.Sprintf("model not found: %s", model), http.StatusNotFound)
	} else {
		logger.Error("Failed to load model", zap.String("model", model), zap.Error(err))
		http.Error(w, fmt.Sprintf("failed to load model %s: %v", model, err), http.StatusServiceUnavailable)
	}
}

func capsMapToModelInfoMap(caps map[string][]string) map[string]ModelInfo {
	m := make(map[string]ModelInfo, len(caps))
	for name, c := range caps {
		m[name] = ModelInfo{Capabilities: c}
	}
	return m
}

// ListModels implements ServerInterface
func (t *TermiteAPI) ListModels(w http.ResponseWriter, r *http.Request) {
	resp := ModelsResponse{
		Chunkers:     map[string]ModelInfo{},
		Rerankers:    map[string]ModelInfo{},
		Embedders:    map[string]ModelInfo{},
		Generators:   map[string]ModelInfo{},
		Recognizers:  map[string]ModelInfo{},
		Extractors:   map[string]ModelInfo{},
		Rewriters:    map[string]ModelInfo{},
		Classifiers:  map[string]ModelInfo{},
		Readers:      map[string]ModelInfo{},
		Transcribers: map[string]ModelInfo{},
	}

	if t.node.chunker != nil {
		// Built-in models (always available, no capabilities)
		resp.Chunkers[termchunking.ModelFixedBert] = ModelInfo{}
		resp.Chunkers[termchunking.ModelFixedBPE] = ModelInfo{}
		// Registry models (text + media chunkers with capabilities)
		for name, caps := range t.node.chunker.ListWithCapabilities() {
			resp.Chunkers[name] = ModelInfo{Capabilities: caps}
		}
	}

	if t.node.embedderRegistry != nil {
		resp.Embedders = capsMapToModelInfoMap(t.node.embedderRegistry.ListWithCapabilities())
	}

	if t.node.rerankerRegistry != nil {
		resp.Rerankers = stringsToModelInfoMap(t.node.rerankerRegistry.List())
	}

	if t.node.generatorRegistry != nil {
		resp.Generators = stringsToModelInfoMap(t.node.generatorRegistry.List())
	}

	if t.node.nerRegistry != nil {
		resp.Recognizers = capsMapToModelInfoMap(t.node.nerRegistry.List())

		// Populate extractors: NER models with extraction capability
		for name, caps := range t.node.nerRegistry.List() {
			if slices.Contains(caps, string(modelregistry.CapabilityExtraction)) {
				resp.Extractors[name] = ModelInfo{Capabilities: caps}
			}
		}
	}

	if t.node.seq2seqRegistry != nil {
		resp.Rewriters = stringsToModelInfoMap(t.node.seq2seqRegistry.List())
	}

	if t.node.classifierRegistry != nil {
		resp.Classifiers = stringsToModelInfoMap(t.node.classifierRegistry.List())
	}

	if t.node.readerRegistry != nil {
		resp.Readers = capsMapToModelInfoMap(t.node.readerRegistry.ListWithCapabilities())
	}

	if t.node.transcriberRegistry != nil {
		resp.Transcribers = stringsToModelInfoMap(t.node.transcriberRegistry.List())
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// GetVersion implements ServerInterface
func (t *TermiteAPI) GetVersion(w http.ResponseWriter, r *http.Request) {
	resp := VersionResponse{
		Version:        Version,
		GitCommit:      GitCommit,
		BuildTime:      BuildTime,
		GoVersion:      runtime.Version(),
		AllowDownloads: t.node.allowDownloads,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleApiEmbed handles embedding generation requests using Ollama-compatible API
// with OpenAI-compatible multimodal extension for CLIP models.
func (ln *TermiteNode) handleApiEmbed(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if embedder provider is available
	if ln.embedderRegistry == nil {
		http.Error(w, "embedding not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			// Context cancelled
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode the request using generated types
	var req EmbedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate model
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}

	// Check if this is a sparse model — use a different code path
	if ln.embedderRegistry.HasCapability(req.Model, modelregistry.CapabilitySparse) {
		ln.handleSparseEmbed(w, r, req)
		return
	}

	// Acquire embedder (increments ref count to prevent eviction during request)
	embedder, err := ln.embedderRegistry.Acquire(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf("model not found: %s", req.Model), http.StatusNotFound)
		return
	}
	defer ln.embedderRegistry.Release(req.Model)

	// Parse input - supports text strings, arrays, and multimodal content parts
	// Uses scraping package for URL downloads with security config and S3 credentials
	contents, err := parseEmbedInput(r.Context(), req.Input, ln.contentSecurityConfig, ln.s3Credentials)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid input: %v", err), http.StatusBadRequest)
		return
	}

	if len(contents) == 0 {
		http.Error(w, "input is required", http.StatusBadRequest)
		return
	}

	// Validate MIME types against embedder capabilities
	if err := validateContentTypes(contents, embedder.Capabilities()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Wrap embedder with caching for deduplicated requests
	cachedEmbedder := NewCachedEmbedder(embedder, req.Model, ln.embeddingCache, ln.logger.Named(req.Model))

	// Generate embeddings (with caching and singleflight deduplication)
	embeds, err := cachedEmbedder.Embed(r.Context(), contents)
	if err != nil {
		ln.logger.Error("failed to generate embeddings",
			zap.String("model", req.Model),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("generating embeddings: %v", err), http.StatusInternalServerError)
		return
	}

	// Determine response format based on Accept header
	acceptHeader := r.Header.Get("Accept")

	switch acceptHeader {
	case "application/json":
		// JSON response using Ollama-compatible format
		resp := EmbedResponse{
			Model:      req.Model,
			Embeddings: embeds,
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			ln.logger.Error("encoding JSON response", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	default:
		// Default: binary serialization (application/octet-stream)
		w.Header().Set("Content-Type", "application/octet-stream")
		if err := SerializeFloatArrays(w, embeds); err != nil {
			ln.logger.Error("serializing embeddings", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// handleSparseEmbed handles the sparse embedding code path for models with the "sparse" capability.
func (ln *TermiteNode) handleSparseEmbed(w http.ResponseWriter, r *http.Request, req EmbedRequest) {
	// Parse text-only input (sparse models don't support multimodal)
	texts, err := parseTextOnlyInput(req.Input)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid input: %v", err), http.StatusBadRequest)
		return
	}

	if len(texts) == 0 {
		http.Error(w, "input is required", http.StatusBadRequest)
		return
	}

	// Acquire sparse embedder
	sparseEmbedder, err := ln.embedderRegistry.AcquireSparse(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf("model not found: %s", req.Model), http.StatusNotFound)
		return
	}
	defer ln.embedderRegistry.Release(req.Model)

	// Wrap with caching
	cachedSparse := NewCachedSparseEmbedder(sparseEmbedder, req.Model, ln.sparseEmbeddingCache, ln.logger.Named(req.Model))

	// Generate sparse embeddings
	sparseVecs, err := cachedSparse.SparseEmbed(r.Context(), texts)
	if err != nil {
		ln.logger.Error("failed to generate sparse embeddings",
			zap.String("model", req.Model),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("generating sparse embeddings: %v", err), http.StatusInternalServerError)
		return
	}

	// Determine response format based on Accept header
	acceptHeader := r.Header.Get("Accept")

	switch acceptHeader {
	case "application/json":
		// JSON response with sparse_embeddings field
		resp := EmbedResponse{
			Model:            req.Model,
			SparseEmbeddings: make([]SparseVector, len(sparseVecs)),
		}
		for i, sv := range sparseVecs {
			// Convert from uint32 indices to generated int32 for JSON wire format
			indices := make([]int32, len(sv.Indices))
			for j, idx := range sv.Indices {
				indices[j] = int32(idx)
			}
			resp.SparseEmbeddings[i] = SparseVector{
				Indices: indices,
				Values:  sv.Values,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			ln.logger.Error("encoding JSON response", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	default:
		// Binary serialization with sparse content type
		w.Header().Set("Content-Type", SparseVectorsContentType)
		if err := SerializeSparseVectors(w, sparseVecs); err != nil {
			ln.logger.Error("serializing sparse embeddings", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// parseTextOnlyInput extracts text strings from the embed request input.
// Returns an error if the input contains multimodal content (images, audio).
func parseTextOnlyInput(input EmbedRequest_Input) ([]string, error) {
	// Try array of strings first (most common case)
	if arr, err := input.AsEmbedRequestInput1(); err == nil && len(arr) > 0 {
		return arr, nil
	}

	// Try single string
	if str, err := input.AsEmbedRequestInput0(); err == nil && str != "" {
		return []string{str}, nil
	}

	return nil, errors.New("sparse models only support text input (string or array of strings)")
}

// parseEmbedInput parses the EmbedRequest input which can be:
// - A single text string
// - An array of text strings (Ollama-compatible)
// - An array of ContentPart objects (OpenAI-compatible multimodal)
//
// For image_url content, supports:
// - Data URIs: data:image/png;base64,...
// - HTTP/HTTPS URLs: https://example.com/image.png
// - Local files: file:///path/to/image.png
// - S3 URLs: s3://endpoint/bucket/key
func parseEmbedInput(
	ctx context.Context,
	input EmbedRequest_Input,
	securityConfig *scraping.ContentSecurityConfig,
	s3Creds *s3.Credentials,
) ([][]ai.ContentPart, error) {
	// Try array of strings first (most common case)
	if arr, err := input.AsEmbedRequestInput1(); err == nil && len(arr) > 0 {
		contents := make([][]ai.ContentPart, len(arr))
		for i, t := range arr {
			contents[i] = []ai.ContentPart{ai.TextContent{Text: t}}
		}
		return contents, nil
	}

	// Try single string
	if str, err := input.AsEmbedRequestInput0(); err == nil && str != "" {
		return [][]ai.ContentPart{{ai.TextContent{Text: str}}}, nil
	}

	// Try multimodal content parts (OpenAI-compatible)
	if parts, err := input.AsEmbedRequestInput2(); err == nil && len(parts) > 0 {
		contents := make([][]ai.ContentPart, len(parts))
		for i, part := range parts {
			// Try image URL content first - check Type field since both AsTextContentPart
			// and AsImageURLContentPart will succeed on any JSON (Go unmarshal doesn't fail on extra fields)
			if imgPart, err := part.AsImageURLContentPart(); err == nil && imgPart.Type == ImageURLContentPartTypeImageUrl {
				// Use scraping package - handles data:, http://, https://, file://, s3://
				mimeType, data, err := scraping.DownloadContent(ctx, imgPart.ImageUrl.Url, securityConfig, s3Creds)
				if err != nil {
					return nil, fmt.Errorf("downloading image at index %d: %w", i, err)
				}
				contents[i] = []ai.ContentPart{ai.BinaryContent{
					MIMEType: mimeType,
					Data:     data,
				}}
				continue
			}

			// Try text content - check Type field
			if textPart, err := part.AsTextContentPart(); err == nil && textPart.Type == TextContentPartTypeText {
				contents[i] = []ai.ContentPart{ai.TextContent{Text: textPart.Text}}
				continue
			}

			// Try inline media content - check Type field
			if mediaPart, err := part.AsMediaContentPart(); err == nil && mediaPart.Type == MediaContentPartTypeMedia {
				contents[i] = []ai.ContentPart{ai.BinaryContent{
					MIMEType: mediaPart.MimeType,
					Data:     mediaPart.Data,
				}}
				continue
			}

			return nil, fmt.Errorf("unknown content type at index %d", i)
		}
		return contents, nil
	}

	return nil, errors.New("input must be a string, array of strings, or array of content parts")
}

// validateContentTypes checks that all content types in the input are supported
// by the embedder's capabilities.
func validateContentTypes(contents [][]ai.ContentPart, caps embeddings.EmbedderCapabilities) error {
	// Build set of supported MIME types (exact and wildcard prefixes)
	supported := make(map[string]bool)
	var wildcards []string
	for _, m := range caps.SupportedMIMETypes {
		supported[m.MIMEType] = true
		// Collect wildcard types like "image/*" or "audio/*"
		if strings.HasSuffix(m.MIMEType, "/*") {
			wildcards = append(wildcards, strings.TrimSuffix(m.MIMEType, "*"))
		}
	}

	// Check each content part
	for i, parts := range contents {
		for _, part := range parts {
			switch p := part.(type) {
			case ai.TextContent:
				// Text is always supported via text/plain
				if !supported["text/plain"] {
					return fmt.Errorf("model does not support text input")
				}
			case ai.BinaryContent:
				if !isMIMESupported(p.MIMEType, supported, wildcards) {
					return fmt.Errorf("unsupported MIME type at index %d: %s (model supports: %v)",
						i, p.MIMEType, getMIMETypeList(caps))
				}
			}
		}
	}

	return nil
}

// isMIMESupported checks if a MIME type is supported by exact match or wildcard.
func isMIMESupported(mimeType string, supported map[string]bool, wildcards []string) bool {
	if supported[mimeType] {
		return true
	}
	for _, prefix := range wildcards {
		if strings.HasPrefix(mimeType, prefix) {
			return true
		}
	}
	return false
}

// getMIMETypeList returns a list of supported MIME types for error messages.
func getMIMETypeList(caps embeddings.EmbedderCapabilities) []string {
	types := make([]string, len(caps.SupportedMIMETypes))
	for i, m := range caps.SupportedMIMETypes {
		types[i] = m.MIMEType
	}
	return types
}

// handleApiChunk handles text and media chunking requests.
// Supports both the new 'input' field (string or ContentPart) and the
// deprecated 'text' field for backward compatibility.
func (ln *TermiteNode) handleApiChunk(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	var req ChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Convert ChunkConfig to internal chunkConfig type
	internalConfig := chunkConfig{
		Model:         req.Config.Model,
		TargetTokens:  req.Config.Text.TargetTokens,
		OverlapTokens: req.Config.Text.OverlapTokens,
		Separator:     req.Config.Text.Separator,
		MaxChunks:     req.Config.MaxChunks,
		Threshold:     req.Config.Threshold,
	}

	mediaOpts := chunking.ChunkOptions{
		MaxChunks: req.Config.MaxChunks,
		Threshold: req.Config.Threshold,
		Audio: chunking.AudioChunkOptions{
			WindowDurationMs:  req.Config.Audio.WindowDurationMs,
			OverlapDurationMs: req.Config.Audio.OverlapDurationMs,
		},
	}

	// Inject VAD config into context for the VAD audio chunker.
	// NOTE: This condition must include all fields of VADOptions. The gate prevents
	// a zero-valued VADConfig from being injected when no overrides are specified.
	// The chunker's apply block (vad_audio_chunker.go ChunkPCM) uses individual
	// > 0 guards to skip zero fields within an injected config.
	ctx := r.Context()
	vadCfg := req.Config.Audio.Vad
	if vadCfg.MinSilenceDurationMs > 0 || vadCfg.MinSpeechDurationMs > 0 || vadCfg.SpeechPadMs > 0 || vadCfg.MaxSegmentDurationMs > 0 {
		ctx = termchunking.WithVADConfig(ctx, termchunking.VADConfig{
			MinSilenceDurationMs: vadCfg.MinSilenceDurationMs,
			MinSpeechDurationMs:  vadCfg.MinSpeechDurationMs,
			SpeechPadMs:          vadCfg.SpeechPadMs,
			MaxSegmentDurationMs: vadCfg.MaxSegmentDurationMs,
		})
	}

	var chunks []chunking.Chunk
	var cacheHit bool

	// Determine input type: try 'input' first, fall back to deprecated 'text'
	inputHandled := false

	if req.Input.union != nil {
		// Try as string (text)
		if text, err := req.Input.AsChunkRequestInput0(); err == nil && text != "" {
			chunks, cacheHit, err = ln.chunker.Chunk(ctx, text, internalConfig)
			if err != nil {
				ln.logger.Error("chunking failed", zap.Error(err))
				http.Error(w, fmt.Sprintf("chunking text: %v", err), http.StatusInternalServerError)
				return
			}
			inputHandled = true
		}

		// Try as ContentPart
		if !inputHandled {
			if part, err := req.Input.AsContentPart(); err == nil {
				// MediaContentPart — inline binary
				if mediaPart, err := part.AsMediaContentPart(); err == nil && mediaPart.Type == MediaContentPartTypeMedia {
					chunks, err = ln.chunkMedia(ctx, mediaPart.Data, mediaPart.MimeType, internalConfig.Model, mediaOpts)
					if err != nil {
						ln.logger.Error("media chunking failed", zap.Error(err))
						http.Error(w, fmt.Sprintf("chunking media: %v", err), http.StatusInternalServerError)
						return
					}
					inputHandled = true
				}

				// TextContentPart
				if !inputHandled {
					if textPart, err := part.AsTextContentPart(); err == nil && textPart.Type == TextContentPartTypeText {
						chunks, cacheHit, err = ln.chunker.Chunk(ctx, textPart.Text, internalConfig)
						if err != nil {
							ln.logger.Error("chunking failed", zap.Error(err))
							http.Error(w, fmt.Sprintf("chunking text: %v", err), http.StatusInternalServerError)
							return
						}
						inputHandled = true
					}
				}

				// ImageURLContentPart — download then dispatch to media chunker
				if !inputHandled {
					if imgPart, err := part.AsImageURLContentPart(); err == nil && imgPart.Type == ImageURLContentPartTypeImageUrl {
						mimeType, data, err := scraping.DownloadContent(ctx, imgPart.ImageUrl.Url, ln.contentSecurityConfig, ln.s3Credentials)
						if err != nil {
							http.Error(w, fmt.Sprintf("downloading content: %v", err), http.StatusBadRequest)
							return
						}
						chunks, err = ln.chunkMedia(ctx, data, mimeType, internalConfig.Model, mediaOpts)
						if err != nil {
							ln.logger.Error("media chunking failed", zap.Error(err))
							http.Error(w, fmt.Sprintf("chunking media: %v", err), http.StatusInternalServerError)
							return
						}
						inputHandled = true
					}
				}
			}
		}
	}

	// Backward compat: fall back to deprecated 'text' field
	if !inputHandled {
		if req.Text == "" {
			http.Error(w, "input or text is required", http.StatusBadRequest)
			return
		}
		chunks, cacheHit, err = ln.chunker.Chunk(ctx, req.Text, internalConfig)
		if err != nil {
			ln.logger.Error("chunking failed", zap.Error(err))
			http.Error(w, fmt.Sprintf("chunking text: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Record metrics
	modelUsed := internalConfig.Model
	if modelUsed == "" {
		modelUsed = "default"
	}
	RecordChunkerRequest(modelUsed)
	RecordChunkCreation(modelUsed, len(chunks))

	// Build response
	resp := ChunkResponse{
		Chunks:   chunks,
		Model:    internalConfig.Model,
		CacheHit: cacheHit,
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// chunkMedia delegates media chunking to the unified chunker, which handles
// model-based and algorithmic fallback internally.
func (ln *TermiteNode) chunkMedia(ctx context.Context, data []byte, mimeType, model string, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	return ln.chunker.ChunkMedia(ctx, data, mimeType, model, opts)
}

// handleApiRerank handles reranking requests
func (ln *TermiteNode) handleApiRerank(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if reranking is available
	if ln.rerankerRegistry == nil || len(ln.rerankerRegistry.List()) == 0 {
		http.Error(w, "reranking not available", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request
	var req struct {
		Model   string   `json:"model"`   // Model name to use (required)
		Query   string   `json:"query"`   // Query text
		Prompts []string `json:"prompts"` // Pre-rendered document texts to rerank
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if req.Query == "" {
		http.Error(w, "query is required", http.StatusBadRequest)
		return
	}
	if len(req.Prompts) == 0 {
		http.Error(w, "prompts are required", http.StatusBadRequest)
		return
	}

	// Acquire model from registry
	reranker, err := ln.rerankerRegistry.Acquire(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf("model not found: %s", req.Model), http.StatusNotFound)
		return
	}
	defer ln.rerankerRegistry.Release(req.Model)

	// Wrap reranker with caching for deduplicated requests
	cachedReranker := NewCachedReranker(reranker, req.Model, ln.rerankingCache, ln.logger.Named(req.Model))

	// Rerank prompts (with caching and singleflight deduplication)
	scores, err := cachedReranker.Rerank(r.Context(), req.Query, req.Prompts)
	if err != nil {
		ln.logger.Error("reranking failed",
			zap.String("model", req.Model),
			zap.String("query", req.Query),
			zap.Int("num_prompts", len(req.Prompts)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("reranking failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Record metrics
	RecordRerankerRequest(req.Model)
	RecordRerankingCreation(req.Model, len(req.Prompts))

	// Validate response
	if len(scores) != len(req.Prompts) {
		http.Error(w,
			fmt.Sprintf("expected %d scores, got %d", len(req.Prompts), len(scores)),
			http.StatusInternalServerError)
		return
	}

	ln.logger.Info("reranking request completed",
		zap.String("model", req.Model),
		zap.String("query", req.Query),
		zap.Int("num_prompts", len(req.Prompts)),
		zap.Int("num_scores", len(scores)))

	// Send response
	resp := struct {
		Model  string    `json:"model"`
		Scores []float32 `json:"scores"`
	}{
		Model:  req.Model,
		Scores: scores,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleApiRecognize handles NER (Named Entity Recognition) requests
func (ln *TermiteNode) handleApiRecognize(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if NER is available
	if ln.nerRegistry == nil || len(ln.nerRegistry.List()) == 0 {
		http.Error(w, "NER not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request
	var req struct {
		Model          string          `json:"model"`           // Model name to use (required)
		Texts          []string        `json:"texts"`           // Texts to extract entities from
		Labels         []string        `json:"labels"`          // Custom labels for zero-shot models (optional)
		RelationLabels []string        `json:"relation_labels"` // Relation types to extract (optional, for models with relations capability)
		Resolver       *ResolverConfig `json:"resolver"`        // Entity resolution config (optional)
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if len(req.Texts) == 0 {
		http.Error(w, "texts are required", http.StatusBadRequest)
		return
	}

	// Acquire model from registry
	model, err := ln.nerRegistry.Acquire(req.Model)
	if err != nil {
		writeModelAcquireError(w, ln.logger, req.Model, err)
		return
	}
	defer ln.nerRegistry.Release(req.Model)

	var entities [][]ner.Entity
	var relations [][]ner.Relation

	// Only extract relations when the client explicitly requests them
	wantRelations := len(req.RelationLabels) > 0

	// Try relation extraction first (type assertion replaces capability check)
	if wantRelations {
		if relExtractor, ok := model.(ner.RelationExtractor); ok {
			entities, relations, err = relExtractor.ExtractRelations(r.Context(), req.Texts, req.Labels, req.RelationLabels)
			if err != nil {
				ln.logger.Error("Relation extraction failed",
					zap.String("model", req.Model),
					zap.Strings("labels", req.Labels),
					zap.Strings("relation_labels", req.RelationLabels),
					zap.Int("num_texts", len(req.Texts)),
					zap.Error(err))
				http.Error(w, fmt.Sprintf("Relation extraction failed: %v", err), http.StatusInternalServerError)
				return
			}
		} else {
			http.Error(w, fmt.Sprintf("model %s does not support relation extraction", req.Model), http.StatusBadRequest)
			return
		}
	} else if recognizer, ok := model.(ner.Recognizer); ok {
		// Zero-shot NER with custom or default labels
		labels := req.Labels
		if len(labels) == 0 {
			labels = recognizer.Labels()
		}
		entities, err = recognizer.RecognizeWithLabels(r.Context(), req.Texts, labels)
		if err != nil {
			ln.logger.Error("Recognition failed",
				zap.String("model", req.Model),
				zap.Strings("labels", labels),
				zap.Int("num_texts", len(req.Texts)),
				zap.Error(err))
			http.Error(w, fmt.Sprintf("Recognition failed: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		// Standard NER model - wrap with caching for deduplicated requests
		cachedModel := NewCachedNER(model, req.Model, ln.nerCache, ln.logger.Named(req.Model))

		// Recognize entities (with caching and singleflight deduplication)
		entities, err = cachedModel.Recognize(r.Context(), req.Texts)
		if err != nil {
			ln.logger.Error("NER failed",
				zap.String("model", req.Model),
				zap.Int("num_texts", len(req.Texts)),
				zap.Error(err))
			http.Error(w, fmt.Sprintf("NER failed: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Record metrics
	RecordNERRequest(req.Model)
	totalEntities := utils.CountNested(entities)
	RecordNERCreation(req.Model, totalEntities)

	ln.logger.Info("NER request completed",
		zap.String("model", req.Model),
		zap.Int("num_texts", len(req.Texts)),
		zap.Int("total_entities", totalEntities),
		zap.Int("total_relations", utils.CountNested(relations)))

	// Convert internal Entity type to API response type
	apiEntities := make([][]RecognizeEntity, len(entities))
	for i, textEntities := range entities {
		apiEntities[i] = make([]RecognizeEntity, len(textEntities))
		for j, e := range textEntities {
			apiEntities[i][j] = RecognizeEntity{
				Text:  e.Text,
				Label: e.Label,
				Start: e.Start,
				End:   e.End,
				Score: e.Score,
			}
		}
	}

	// Convert internal Relation type to API response type
	var apiRelations [][]Relation
	if len(relations) > 0 {
		apiRelations = make([][]Relation, len(relations))
		for i, textRelations := range relations {
			apiRelations[i] = make([]Relation, len(textRelations))
			for j, rel := range textRelations {
				apiRelations[i][j] = Relation{
					Head: RecognizeEntity{
						Text:  rel.HeadEntity.Text,
						Label: rel.HeadEntity.Label,
						Start: rel.HeadEntity.Start,
						End:   rel.HeadEntity.End,
						Score: rel.HeadEntity.Score,
					},
					Tail: RecognizeEntity{
						Text:  rel.TailEntity.Text,
						Label: rel.TailEntity.Label,
						Start: rel.TailEntity.Start,
						End:   rel.TailEntity.End,
						Score: rel.TailEntity.Score,
					},
					Label: rel.Label,
					Score: rel.Score,
				}
			}
		}
	}

	// If resolver config is present, run entity resolution to deduplicate.
	if req.Resolver != nil {
		cfg := ner.ResolverConfig{
			SimilarityThreshold:   float64(req.Resolver.SimilarityThreshold),
			TypeMustMatch:         req.Resolver.TypeMustMatch,
			MinEntityConfidence:   req.Resolver.MinEntityConfidence,
			MinRelationConfidence: req.Resolver.MinRelationConfidence,
			DeduplicateRelations:  req.Resolver.DeduplicateRelations,
			TrackProvenance:       req.Resolver.TrackProvenance,
		}
		// Apply defaults for zero-valued threshold (omitzero sends 0 for unset floats).
		if cfg.SimilarityThreshold == 0 {
			cfg.SimilarityThreshold = 0.85
		}

		kg := ner.BuildKnowledgeGraph(entities, relations, cfg)

		// Build entity ID -> resolved entity lookup for relation mapping.
		entityByID := make(map[string]*ner.ResolvedEntity, len(kg.Entities))
		for i := range kg.Entities {
			entityByID[kg.Entities[i].ID] = &kg.Entities[i]
		}

		// Flatten resolved entities into a single array (no per-text grouping).
		resolvedEntities := make([]RecognizeEntity, len(kg.Entities))
		for i, re := range kg.Entities {
			resolvedEntities[i] = RecognizeEntity{
				Text:  re.CanonicalName,
				Label: re.Label,
				Score: re.Score,
			}
		}
		apiEntities = [][]RecognizeEntity{resolvedEntities}

		// Flatten resolved relations into a single array.
		if len(kg.Relations) > 0 {
			resolvedRelations := make([]Relation, len(kg.Relations))
			for i, rr := range kg.Relations {
				head := entityByID[rr.HeadID]
				tail := entityByID[rr.TailID]
				resolvedRelations[i] = Relation{
					Head: RecognizeEntity{
						Text:  head.CanonicalName,
						Label: head.Label,
						Score: head.Score,
					},
					Tail: RecognizeEntity{
						Text:  tail.CanonicalName,
						Label: tail.Label,
						Score: tail.Score,
					},
					Label: rr.Label,
					Score: rr.Score,
				}
			}
			apiRelations = [][]Relation{resolvedRelations}
		}
	}

	// Send response
	nerResp := RecognizeResponse{
		Model:     req.Model,
		Entities:  apiEntities,
		Relations: apiRelations,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(nerResp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleApiExtract handles structured JSON extraction requests
func (ln *TermiteNode) handleApiExtract(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if NER is available
	if ln.nerRegistry == nil || len(ln.nerRegistry.List()) == 0 {
		http.Error(w, "JSON extraction not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request manually instead of using the generated ExtractRequest type because
	// the generated type uses `bool` with `omitzero`, making it impossible to distinguish
	// an unset flat_ner (should default to true) from an explicitly set false value.
	// Using *bool here lets us detect the difference.
	var req struct {
		Model             string              `json:"model"`
		Texts             []string            `json:"texts"`
		Schema            map[string][]string `json:"schema"`
		Threshold         float32             `json:"threshold"`
		FlatNER           *bool               `json:"flat_ner"`
		IncludeConfidence bool                `json:"include_confidence"`
		IncludeSpans      bool                `json:"include_spans"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if len(req.Texts) == 0 {
		http.Error(w, "texts are required", http.StatusBadRequest)
		return
	}
	if len(req.Schema) == 0 {
		http.Error(w, "schema is required", http.StatusBadRequest)
		return
	}

	// Parse schema
	schemas, err := ner.ParseSchemaString(req.Schema)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid schema: %v", err), http.StatusBadRequest)
		return
	}

	// Build extraction config
	config := ner.DefaultExtractionConfig()
	if req.Threshold > 0 {
		config.Threshold = req.Threshold
	}
	if req.FlatNER != nil {
		config.FlatNER = *req.FlatNER
	}
	config.IncludeConfidence = req.IncludeConfidence
	config.IncludeSpans = req.IncludeSpans

	// Acquire model and check extraction support
	model, err := ln.nerRegistry.Acquire(req.Model)
	if err != nil {
		writeModelAcquireError(w, ln.logger, req.Model, err)
		return
	}
	defer ln.nerRegistry.Release(req.Model)

	extractor, ok := model.(ner.Extractor)
	if !ok {
		http.Error(w, fmt.Sprintf("model %s does not support extraction", req.Model), http.StatusBadRequest)
		return
	}

	// Perform extraction
	results, err := extractor.Extract(r.Context(), req.Texts, schemas, config)
	if err != nil {
		ln.logger.Error("extraction failed",
			zap.String("model", req.Model),
			zap.Int("num_texts", len(req.Texts)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("extraction failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Record metrics
	RecordExtractionRequest(req.Model)
	totalFields := 0
	for _, result := range results {
		for _, instances := range result {
			for _, instance := range instances {
				totalFields += len(instance)
			}
		}
	}
	RecordExtractionFields(req.Model, totalFields)

	ln.logger.Info("extraction request completed",
		zap.String("model", req.Model),
		zap.Int("num_texts", len(req.Texts)),
		zap.Int("total_fields", totalFields))

	// Convert internal ExtractionResult to API response format
	apiResults := make([]map[string][]map[string]any, len(results))
	for i, result := range results {
		apiResult := make(map[string][]map[string]any)
		for structName, instances := range result {
			apiInstances := make([]map[string]any, len(instances))
			for j, instance := range instances {
				apiInstance := make(map[string]any)
				for fieldName, fieldValue := range instance {
					switch v := fieldValue.(type) {
					case ner.ExtractedFieldValue:
						apiInstance[fieldName] = convertFieldValue(v)
					case []ner.ExtractedFieldValue:
						apiValues := make([]extractFieldValueJSON, len(v))
						for k, fv := range v {
							apiValues[k] = convertFieldValue(fv)
						}
						apiInstance[fieldName] = apiValues
					default:
						apiInstance[fieldName] = fieldValue
					}
				}
				apiInstances[j] = apiInstance
			}
			apiResult[structName] = apiInstances
		}
		apiResults[i] = apiResult
	}

	resp := ExtractResponse{
		Model:   req.Model,
		Results: apiResults,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// extractFieldValueJSON is a response-only type for JSON serialization.
// We cannot use the generated ExtractFieldValue because it declares Start/End as
// `int` with `omitzero`, which silently drops offset 0 during marshalling.
// Using *int pointers here ensures offset 0 is serialized correctly.
type extractFieldValueJSON struct {
	Value string  `json:"value"`
	Score float32 `json:"score,omitempty"`
	Start *int    `json:"start,omitempty"`
	End   *int    `json:"end,omitempty"`
}

// convertFieldValue converts an internal ExtractedFieldValue to the response type.
func convertFieldValue(v ner.ExtractedFieldValue) extractFieldValueJSON {
	return extractFieldValueJSON{
		Value: v.Value,
		Score: v.Score,
		Start: v.Start,
		End:   v.End,
	}
}


// generateCompletionID generates a unique ID like OpenAI's "chatcmpl-xxx" format
func generateCompletionID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "chatcmpl-" + hex.EncodeToString(b)
}

// convertChatMessage converts an API ChatMessage to a generation.Message.
// Supports both simple string content and OpenAI-format array of content parts.
func convertChatMessage(msg ChatMessage) generation.Message {
	result := generation.Message{
		Role: string(msg.Role),
	}

	// Try as simple string first (most common case)
	if str, err := msg.Content.AsChatMessageContent0(); err == nil && str != "" {
		result.Content = str
		return result
	}

	// Try as array of content parts (OpenAI multimodal format)
	if parts, err := msg.Content.AsChatMessageContent1(); err == nil {
		for _, part := range parts {
			// Try as text content part
			if textPart, err := part.AsTextContentPart(); err == nil {
				result.Parts = append(result.Parts, generation.TextPart(textPart.Text))
				// Also set Content for backward compatibility with text-only generators
				if result.Content == "" {
					result.Content = textPart.Text
				}
			}
			// Try as image content part
			if imgPart, err := part.AsImageURLContentPart(); err == nil {
				result.Parts = append(result.Parts, generation.ImagePart(imgPart.ImageUrl.Url))
			}
		}
	}

	return result
}

// handleApiGenerate handles text generation requests using LLM models (OpenAI-compatible)
func (ln *TermiteNode) handleApiGenerate(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	ln.logger.Info("Generate request received",
		zap.String("path", r.URL.Path),
		zap.String("content-type", r.Header.Get("Content-Type")))

	// Check if generation is available
	if ln.generatorRegistry == nil || len(ln.generatorRegistry.List()) == 0 {
		ln.logger.Warn("Generation not available: no models configured")
		http.Error(w, "generation not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request
	var req GenerateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ln.logger.Error("Failed to decode generate request",
			zap.Error(err))
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}

	ln.logger.Info("Generate request decoded",
		zap.String("model", req.Model),
		zap.Int("num_messages", len(req.Messages)))

	// Validate request
	if req.Model == "" {
		ln.logger.Warn("Generate request missing model")
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if len(req.Messages) == 0 {
		ln.logger.Warn("Generate request missing messages")
		http.Error(w, "messages are required", http.StatusBadRequest)
		return
	}

	// Acquire generator from registry (increments ref count to prevent cache eviction during use)
	generator, err := ln.generatorRegistry.Acquire(req.Model)
	if err != nil {
		ln.logger.Error("Failed to get generator",
			zap.String("model", req.Model),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("model not found: %s: %v", req.Model, err), http.StatusNotFound)
		return
	}
	defer ln.generatorRegistry.Release(req.Model)

	// Check for tool support if tools are requested
	var toolParser generation.ToolParser
	if len(req.Tools) > 0 {
		ts, ok := generator.(generation.ToolSupporter)
		if !ok || !ts.SupportsTools() {
			http.Error(w, fmt.Sprintf("model %s does not support tool calling", req.Model), http.StatusBadRequest)
			return
		}
		toolParser = ts.ToolParser()
	}

	// Convert messages to internal format
	messages := make([]generation.Message, len(req.Messages))
	for i, m := range req.Messages {
		messages[i] = convertChatMessage(m)
	}

	// Set options from request, using defaults for zero values.
	opts := generation.DefaultGenerateOptions()
	if req.MaxTokens > 0 {
		opts.MaxTokens = req.MaxTokens
	}
	if req.Temperature > 0 {
		opts.Temperature = req.Temperature
	}
	if req.TopP > 0 {
		opts.TopP = req.TopP
	}
	if req.TopK > 0 {
		opts.TopK = req.TopK
	}
	// Try to extract tool choice from union type
	// First try string variant (auto, none, required)
	if tc, err := req.ToolChoice.AsToolChoice0(); err == nil && tc != "" {
		opts.ToolChoice = string(tc)
	} else if tc, err := req.ToolChoice.AsToolChoice1(); err == nil && tc.Function.Name != "" {
		// Function-specific variant: force calling a specific function
		opts.ToolChoice = "required"
		opts.ForcedFunctionName = tc.Function.Name
	}

	// If tools are provided, format tool declarations and prepend to system message
	if toolParser != nil && len(req.Tools) > 0 {
		// Convert API tools to internal format
		tools := make([]generation.ToolDefinition, len(req.Tools))
		for i, t := range req.Tools {
			tools[i] = generation.ToolDefinition{
				Type: string(t.Type),
				Function: generation.FunctionDefinition{
					Name:        t.Function.Name,
					Description: t.Function.Description,
					Parameters:  t.Function.Parameters,
					Strict:      t.Function.Strict,
				},
			}
		}

		// If a specific function is forced, filter tools to only that function
		if opts.ForcedFunctionName != "" {
			filteredTools := make([]generation.ToolDefinition, 0, 1)
			for _, tool := range tools {
				if tool.Function.Name == opts.ForcedFunctionName {
					filteredTools = append(filteredTools, tool)
					break
				}
			}
			if len(filteredTools) == 0 {
				http.Error(w, fmt.Sprintf("forced function %q not found in tools", opts.ForcedFunctionName), http.StatusBadRequest)
				return
			}
			tools = filteredTools
		}

		// Format tools prompt
		toolsPrompt := toolParser.FormatToolsPrompt(tools)

		// If a specific function is forced, add a directive to call it
		if opts.ForcedFunctionName != "" {
			toolsPrompt += fmt.Sprintf("\nYou MUST call the %s function. Do not respond with text, only call the function.\n", opts.ForcedFunctionName)
		}

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

	// Generate completion ID and timestamp
	completionID := generateCompletionID()
	created := int(time.Now().Unix())

	// Handle streaming vs non-streaming
	if req.Stream {
		ln.handleStreamingGenerate(w, r, req, generator, messages, opts, completionID, created)
		return
	}

	// Non-streaming: Generate text
	result, err := generator.Generate(r.Context(), messages, opts)
	if err != nil {
		ln.logger.Error("generation failed",
			zap.String("model", req.Model),
			zap.Int("num_messages", len(req.Messages)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("generation failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Record metrics
	RecordGeneratorRequest(req.Model)
	RecordTokenGeneration(req.Model, result.TokensUsed)

	// Parse tool calls from output if tools were requested
	var toolCalls []generation.ToolCall
	var responseText string
	if toolParser != nil && len(req.Tools) > 0 {
		// Feed the entire response to the parser
		toolParser.Reset()
		toolParser.Feed(result.Text)
		toolCalls, responseText = toolParser.Finish()

		ln.logger.Info("tool call parsing completed",
			zap.String("model", req.Model),
			zap.Int("tool_calls", len(toolCalls)),
			zap.Int("remaining_text_len", len(responseText)))
	} else {
		responseText = result.Text
	}

	ln.logger.Info("generation request completed",
		zap.String("model", req.Model),
		zap.Int("num_messages", len(req.Messages)),
		zap.Int("tokens_generated", result.TokensUsed),
		zap.Int("tool_calls", len(toolCalls)))

	// Map finish reason
	var finishReason FinishReason
	switch {
	case len(toolCalls) > 0:
		finishReason = FinishReasonToolCalls
	case result.FinishReason == "length":
		finishReason = FinishReasonLength
	default:
		finishReason = FinishReasonStop
	}

	// Estimate prompt tokens (rough estimate based on message content length)
	// TODO: Use actual tokenizer for accurate count
	promptTokens := 0
	for _, m := range messages {
		promptTokens += len(m.GetTextContent()) / 4 // Rough estimate: ~4 chars per token
	}

	// Build OpenAI-compatible response
	respMessage := GenerateMessage{
		Role: RoleAssistant,
	}

	// Set content or nil based on whether there are tool calls
	if len(toolCalls) > 0 {
		// When tool calls are present, content can be null or empty
		if responseText != "" {
			respMessage.Content = responseText
		}
		// Convert internal tool calls to API format
		apiToolCalls := make([]ToolCall, len(toolCalls))
		for i, tc := range toolCalls {
			apiToolCalls[i] = ToolCall{
				Id:   tc.ID,
				Type: ToolCallType(tc.Type),
				Function: ToolCallFunction{
					Name:      tc.Function.Name,
					Arguments: tc.Function.Arguments,
				},
			}
		}
		respMessage.ToolCalls = apiToolCalls
	} else {
		respMessage.Content = responseText
	}

	resp := GenerateResponse{
		Id:      completionID,
		Object:  GenerateResponseObjectChatCompletion,
		Created: created,
		Model:   req.Model,
		Choices: []GenerateChoice{
			{
				Index:        0,
				Message:      respMessage,
				FinishReason: finishReason,
				Logprobs:     nil,
			},
		},
		Usage: GenerateUsage{
			PromptTokens:     promptTokens,
			CompletionTokens: result.TokensUsed,
			TotalTokens:      promptTokens + result.TokensUsed,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleStreamingGenerate handles streaming generation with SSE
func (ln *TermiteNode) handleStreamingGenerate(
	w http.ResponseWriter,
	r *http.Request,
	req GenerateRequest,
	generator generation.Generator,
	messages []generation.Message,
	opts generation.GenerateOptions,
	completionID string,
	created int,
) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// Type-assert to StreamingGenerator for true token-by-token streaming
	streamingGen, ok := generator.(generation.StreamingGenerator)
	if !ok {
		ln.logger.Error("generator does not support streaming",
			zap.String("model", req.Model))
		_, _ = fmt.Fprintf(w, "data: {\"error\": \"generator does not support streaming\"}\n\n")
		flusher.Flush()
		return
	}

	// Start streaming generation
	tokenChan, errChan, err := streamingGen.GenerateStream(r.Context(), messages, opts)
	if err != nil {
		ln.logger.Error("failed to start streaming generation",
			zap.String("model", req.Model),
			zap.Error(err))
		errData, _ := json.Marshal(map[string]string{"error": err.Error()})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", errData)
		flusher.Flush()
		return
	}

	// Send first chunk with role
	firstChunk := GenerateChunk{
		Id:      completionID,
		Object:  GenerateChunkObjectChatCompletionChunk,
		Created: created,
		Model:   req.Model,
		Choices: []GenerateChunkChoice{
			{
				Index: 0,
				Delta: GenerateDelta{
					Role: RoleAssistant,
				},
			},
		},
	}
	data, _ := json.Marshal(firstChunk)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()

	// Stream tokens as they arrive
	var tokenCount int
	for token := range tokenChan {
		tokenCount++

		chunk := GenerateChunk{
			Id:      completionID,
			Object:  GenerateChunkObjectChatCompletionChunk,
			Created: created,
			Model:   req.Model,
			Choices: []GenerateChunkChoice{
				{
					Index: 0,
					Delta: GenerateDelta{
						Content: token.Token,
					},
				},
			},
		}
		data, _ := json.Marshal(chunk)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Check for errors from the error channel
	select {
	case err := <-errChan:
		if err != nil {
			ln.logger.Error("streaming generation error",
				zap.String("model", req.Model),
				zap.Error(err))
			errMsg, _ := json.Marshal(err.Error())
			_, _ = fmt.Fprintf(w, "data: {\"error\": %s}\n\n", errMsg)
			flusher.Flush()
			return
		}
	default:
	}

	// Record metrics
	RecordGeneratorRequest(req.Model)
	RecordTokenGeneration(req.Model, tokenCount)

	// Send final chunk with finish_reason
	finalChunk := GenerateChunk{
		Id:      completionID,
		Object:  GenerateChunkObjectChatCompletionChunk,
		Created: created,
		Model:   req.Model,
		Choices: []GenerateChunkChoice{
			{
				Index:        0,
				Delta:        GenerateDelta{},
				FinishReason: FinishReasonStop,
			},
		},
	}
	data, _ = json.Marshal(finalChunk)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()

	// Send [DONE] signal
	_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()

	ln.logger.Info("streaming generation completed",
		zap.String("model", req.Model),
		zap.Int("tokens_generated", tokenCount))
}

// handleApiRewrite handles Seq2Seq text rewriting requests
func (ln *TermiteNode) handleApiRewrite(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if rewriting is available
	if ln.seq2seqRegistry == nil || len(ln.seq2seqRegistry.List()) == 0 {
		http.Error(w, "rewriting not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request
	var req struct {
		Model  string   `json:"model"`  // Model name to use (required)
		Inputs []string `json:"inputs"` // Input texts to rewrite
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if len(req.Inputs) == 0 {
		http.Error(w, "inputs are required", http.StatusBadRequest)
		return
	}

	// Acquire model from registry
	model, err := ln.seq2seqRegistry.Acquire(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf("model not found: %s", req.Model), http.StatusNotFound)
		return
	}
	defer ln.seq2seqRegistry.Release(req.Model)

	// Generate text
	output, err := model.Generate(r.Context(), req.Inputs)
	if err != nil {
		ln.logger.Error("rewriting failed",
			zap.String("model", req.Model),
			zap.Int("num_inputs", len(req.Inputs)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("rewriting failed: %v", err), http.StatusInternalServerError)
		return
	}

	ln.logger.Info("rewrite request completed",
		zap.String("model", req.Model),
		zap.Int("num_inputs", len(req.Inputs)),
		zap.Int("num_outputs", len(output.Texts)))

	// Send response
	resp := RewriteResponse{
		Model: req.Model,
		Texts: output.Texts,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleApiClassify handles zero-shot text classification requests
func (ln *TermiteNode) handleApiClassify(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if classification is available (from either classifier registry or NER registry with multi-task models)
	hasClassifiers := ln.classifierRegistry != nil && len(ln.classifierRegistry.List()) > 0
	hasNERClassifiers := false
	if ln.nerRegistry != nil {
		for _, caps := range ln.nerRegistry.List() {
			if slices.Contains(caps, string(modelregistry.CapabilityClassification)) {
				hasNERClassifiers = true
			}
			if hasNERClassifiers {
				break
			}
		}
	}
	if !hasClassifiers && !hasNERClassifiers {
		http.Error(w, "classification not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request
	var req ClassifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if len(req.Texts) == 0 {
		http.Error(w, "texts are required", http.StatusBadRequest)
		return
	}
	if len(req.Labels) == 0 {
		http.Error(w, "labels are required for zero-shot classification", http.StatusBadRequest)
		return
	}

	var results [][]ClassifyResult

	// First, try to get the model from the classifier registry
	if ln.classifierRegistry != nil {
		classifier, err := ln.classifierRegistry.Acquire(req.Model)
		if err == nil {
			defer ln.classifierRegistry.Release(req.Model)

			// Found in classifier registry - use NLI-based classification
			var classifyResults [][]classification.Classification
			var classifyErr error

			// Use custom hypothesis template if provided, otherwise use default
			if req.HypothesisTemplate != "" {
				classifyResults, classifyErr = classifier.ClassifyWithHypothesis(r.Context(), req.Texts, req.Labels, req.HypothesisTemplate)
			} else if req.MultiLabel {
				classifyResults, classifyErr = classifier.MultiLabelClassify(r.Context(), req.Texts, req.Labels)
			} else {
				classifyResults, classifyErr = classifier.Classify(r.Context(), req.Texts, req.Labels)
			}

			if classifyErr != nil {
				ln.logger.Error("classification failed",
					zap.String("model", req.Model),
					zap.Int("num_texts", len(req.Texts)),
					zap.Strings("labels", req.Labels),
					zap.Error(classifyErr))
				http.Error(w, fmt.Sprintf("classification failed: %v", classifyErr), http.StatusInternalServerError)
				return
			}

			// Convert to API response format
			results = make([][]ClassifyResult, len(classifyResults))
			for i, textResults := range classifyResults {
				results[i] = make([]ClassifyResult, len(textResults))
				for j, c := range textResults {
					results[i][j] = ClassifyResult{
						Label: c.Label,
						Score: c.Score,
					}
				}
			}

			ln.logger.Info("classify request completed (NLI classifier)",
				zap.String("model", req.Model),
				zap.Int("num_texts", len(req.Texts)),
				zap.Int("num_labels", len(req.Labels)))

			// Send response
			resp := ClassifyResponse{
				Model:           req.Model,
				Classifications: results,
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				ln.logger.Error("encoding response", zap.Error(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		// Model not found in classifier registry, continue to check NER registry
	}

	// Try to get a classifier from NER registry
	if ln.nerRegistry != nil {
		model, err := ln.nerRegistry.Acquire(req.Model)
		if err == nil {
			defer ln.nerRegistry.Release(req.Model)

			// Check if model supports classification
			classifier, ok := model.(ner.Classifier)
			if ok {
				// Found model with classification support
				config := &ner.ClassificationConfig{
					MultiLabel: req.MultiLabel,
					Threshold:  0.0, // Return all scores, let caller filter
				}

				classifyResults, classifyErr := classifier.ClassifyText(r.Context(), req.Texts, req.Labels, config)
				if classifyErr != nil {
					ln.logger.Error("classification failed",
						zap.String("model", req.Model),
						zap.Int("num_texts", len(req.Texts)),
						zap.Strings("labels", req.Labels),
						zap.Error(classifyErr))
					http.Error(w, fmt.Sprintf("classification failed: %v", classifyErr), http.StatusInternalServerError)
					return
				}

				// Convert ner.Classification to API response format
				results = make([][]ClassifyResult, len(classifyResults))
				for i, textResults := range classifyResults {
					results[i] = make([]ClassifyResult, len(textResults))
					for j, c := range textResults {
						results[i][j] = ClassifyResult{
							Label: c.Label,
							Score: c.Score,
						}
					}
				}

				ln.logger.Info("classify request completed",
					zap.String("model", req.Model),
					zap.Int("num_texts", len(req.Texts)),
					zap.Int("num_labels", len(req.Labels)))

				// Send response
				resp := ClassifyResponse{
					Model:           req.Model,
					Classifications: results,
				}

				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(resp); err != nil {
					ln.logger.Error("encoding response", zap.Error(err))
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
		}
	}

	// Model not found in either registry
	ln.logger.Error("failed to get classifier model",
		zap.String("model", req.Model))
	http.Error(w, fmt.Sprintf("model not found: %s", req.Model), http.StatusNotFound)
}

// handleApiRead handles reading/OCR requests using Vision2Seq models
func (ln *TermiteNode) handleApiRead(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if reading is available
	if ln.readerRegistry == nil || len(ln.readerRegistry.List()) == 0 {
		http.Error(w, "reading not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request using generated types
	var req ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if len(req.Images) == 0 {
		http.Error(w, "images are required", http.StatusBadRequest)
		return
	}

	// Acquire reader model from registry
	reader, err := ln.readerRegistry.Acquire(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf("model not found: %s", req.Model), http.StatusNotFound)
		return
	}
	defer ln.readerRegistry.Release(req.Model)

	// Download and decode images
	images, err := downloadAndDecodeImages(r.Context(), req.Images, ln.contentSecurityConfig, ln.s3Credentials)
	if err != nil {
		ln.logger.Error("failed to download images",
			zap.String("model", req.Model),
			zap.Int("num_images", len(req.Images)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("failed to download images: %v", err), http.StatusBadRequest)
		return
	}

	// Get optional parameters
	prompt := req.Prompt // empty string if not provided
	maxTokens := req.MaxTokens
	if maxTokens == 0 {
		maxTokens = 256 // default
	}

	// Wrap reader with caching for deduplicated requests
	cachedReader := NewCachedReader(reader, req.Model, ln.readingCache, ln.logger.Named(req.Model))

	// Read images (with caching and singleflight deduplication)
	results, err := cachedReader.Read(r.Context(), images, prompt, maxTokens)
	if err != nil {
		ln.logger.Error("reading failed",
			zap.String("model", req.Model),
			zap.Int("num_images", len(images)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("reading failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Record metrics
	RecordReaderRequest(req.Model)

	ln.logger.Info("read request completed",
		zap.String("model", req.Model),
		zap.Int("num_images", len(images)),
		zap.Int("num_results", len(results)))

	// Convert to API response format
	apiResults := make([]ReadResult, len(results))
	for i, r := range results {
		apiResults[i] = ReadResult{
			Text:   r.Text,
			Fields: r.Fields,
		}

		// Populate regions from multi-stage OCR models
		if len(r.Regions) > 0 {
			apiRegions := make([]TextRegion, len(r.Regions))
			for j, region := range r.Regions {
				apiRegions[j] = TextRegion{
					Text:       region.Text,
					Bbox:       []float32{float32(region.BBox[0]), float32(region.BBox[1]), float32(region.BBox[2]), float32(region.BBox[3])},
					Confidence: float32(region.Confidence),
					Label:      region.Label,
				}
			}
			apiResults[i].Regions = apiRegions
		}
	}

	// Send response
	resp := ReadResponse{
		Model:   req.Model,
		Results: apiResults,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// downloadAndDecodeImages downloads images from URLs and decodes them to image.Image
func downloadAndDecodeImages(ctx context.Context, imageURLs []ImageURL, secConfig *scraping.ContentSecurityConfig, s3Creds *s3.Credentials) ([]image.Image, error) {
	images := make([]image.Image, 0, len(imageURLs))

	for _, imgURL := range imageURLs {
		// Download image data - returns (mimeType, data []byte, error)
		// scraping.DownloadContent handles data:, http://, https://, file://, s3:// URLs
		// and returns already-decoded bytes (base64 decoding for data: URLs is handled internally)
		_, imageData, err := scraping.DownloadContent(ctx, imgURL.Url, secConfig, s3Creds)
		if err != nil {
			return nil, fmt.Errorf("downloading image %s: %w", imgURL.Url, err)
		}

		// Decode image from bytes
		img, _, err := image.Decode(bytes.NewReader(imageData))
		if err != nil {
			return nil, fmt.Errorf("decoding image %s: %w", imgURL.Url, err)
		}

		images = append(images, img)
	}

	return images, nil
}

// handleApiTranscribe handles speech-to-text transcription requests
func (ln *TermiteNode) handleApiTranscribe(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	// Check if transcription is available
	if ln.transcriberRegistry == nil || len(ln.transcriberRegistry.List()) == 0 {
		http.Error(w, "transcription not available: no models configured", http.StatusServiceUnavailable)
		return
	}

	// Apply backpressure via request queue
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		switch err {
		case ErrQueueFull:
			RecordQueueRejection()
			WriteQueueFullResponse(w, 5*time.Second)
		case ErrRequestTimeout:
			RecordQueueTimeout()
			WriteTimeoutResponse(w)
		default:
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
		return
	}
	defer release()

	// Update queue metrics
	UpdateQueueMetrics(ln.requestQueue.Stats())

	// Decode request using generated types
	var req TranscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	if len(req.Audio) == 0 {
		http.Error(w, "audio is required", http.StatusBadRequest)
		return
	}

	// Acquire transcriber model from registry
	transcriber, err := ln.transcriberRegistry.Acquire(req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf("model not found: %s", req.Model), http.StatusNotFound)
		return
	}
	defer ln.transcriberRegistry.Release(req.Model)

	// req.Audio is []byte — Go's JSON unmarshaler already base64-decoded it
	audioData := req.Audio

	// Build transcription options
	opts := transcribing.TranscribeOptions{}
	if req.Language != "" {
		opts.Language = req.Language
	}

	// Wrap transcriber with caching for deduplicated requests
	cachedTranscriber := NewCachedTranscriber(transcriber, req.Model, ln.transcriptionCache, ln.logger.Named(req.Model))

	// Transcribe audio (with caching and singleflight deduplication)
	result, err := cachedTranscriber.TranscribeWithOptions(r.Context(), audioData, opts)
	if err != nil {
		ln.logger.Error("transcription failed",
			zap.String("model", req.Model),
			zap.Int("audio_bytes", len(audioData)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("transcription failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Record metrics
	RecordTranscriberRequest(req.Model)

	ln.logger.Info("transcribe request completed",
		zap.String("model", req.Model),
		zap.Int("audio_bytes", len(audioData)),
		zap.Int("text_length", len(result.Text)))

	// Send response
	resp := TranscribeResponse{
		Model:    req.Model,
		Text:     result.Text,
		Language: result.Language,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
