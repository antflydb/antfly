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

//go:build go1.22

//go:generate go tool oapi-codegen --config=cfg.yaml ../../../specs/openapi/inference/config.yaml
package termite

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"net/http"
	"slices"
	"strings"
	"time"

	generatingtypes "github.com/antflydb/antfly/go/pkg/generating"
	extractingtypes "github.com/antflydb/antfly/go/pkg/generating/extracting"
	"github.com/antflydb/antfly/go/pkg/libaf/ai"
	"github.com/antflydb/antfly/go/pkg/libaf/chunking"
	"github.com/antflydb/antfly/go/pkg/libaf/embeddings"
	json "github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/antflydb/antfly/go/pkg/libaf/s3"
	"github.com/antflydb/antfly/go/pkg/libaf/scraping"
	"github.com/antflydb/antfly/go/pkg/termite/lib/backends"
	termchunking "github.com/antflydb/antfly/go/pkg/termite/lib/chunking"
	"github.com/antflydb/antfly/go/pkg/termite/lib/classification"
	"github.com/antflydb/antfly/go/pkg/termite/lib/generation"
	"github.com/antflydb/antfly/go/pkg/termite/lib/modelregistry"
	"github.com/antflydb/antfly/go/pkg/termite/lib/ner"
	"github.com/antflydb/antfly/go/pkg/termite/lib/transcribing"
	"github.com/antflydb/antfly/go/pkg/termite/lib/utils"
	"go.uber.org/zap"
	_ "golang.org/x/image/webp"
)

const (
	maxReadBatchImages = 64
	maxReadBatchBytes  = int64(256 * 1024 * 1024)
)

type entityExtractionRequest struct {
	Model             string
	Texts             []string
	IDs               []string
	Labels            []string
	RelationLabels    []string
	Resolver          *extractingtypes.ExtractionResolverOptions
	IncludeConfidence bool
	IncludeSpans      bool
	Threshold         float32
	FlatNER           bool
}

type extractedEntity struct {
	Text  string
	Label string
	Start int
	End   int
	Score float32
}

type extractedRelation struct {
	Head  extractedEntity
	Tail  extractedEntity
	Label string
	Score float32
}

var errReadBatchTooLarge = errors.New("read batch too large")

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
		BaseURL:    "/ai/v1",
		BaseRouter: http.NewServeMux(),
	})
}

type captureResponseWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newCaptureResponseWriter() *captureResponseWriter {
	return &captureResponseWriter{header: make(http.Header)}
}

func (w *captureResponseWriter) Header() http.Header {
	return w.header
}

func (w *captureResponseWriter) WriteHeader(statusCode int) {
	if w.status != 0 {
		return
	}
	w.status = statusCode
}

func (w *captureResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *captureResponseWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
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

// ClassifyText implements ServerInterface
func (t *TermiteAPI) ClassifyText(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiClassify(w, r)
}

// GenerateContent implements ServerInterface
func (t *TermiteAPI) GenerateContent(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiGenerate(w, r)
}

// GenerateBatchContent implements ServerInterface
func (t *TermiteAPI) GenerateBatchContent(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiGenerateBatch(w, r)
}

// RewriteText implements ServerInterface
func (t *TermiteAPI) RewriteText(w http.ResponseWriter, r *http.Request) {
	t.node.handleApiRewrite(w, r)
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
	defer func() { _ = r.Body.Close() }()
	var req extractingtypes.ExtractionRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxReadBatchBytes)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	texts, err := extractionTexts(req.Inputs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	hasEntities := req.Schema.Entities != nil && len(*req.Schema.Entities) > 0
	hasRelations := req.Schema.Relations != nil && len(*req.Schema.Relations) > 0
	hasStructures := req.Schema.Structures != nil && len(*req.Schema.Structures) > 0
	hasClassifications := req.Schema.Classifications != nil && len(*req.Schema.Classifications) > 0
	operations := 0
	if hasEntities || hasRelations {
		operations++
	}
	if hasStructures {
		operations++
	}
	if hasClassifications {
		operations++
	}
	if operations != 1 {
		http.Error(w, "schema must request exactly one supported extraction operation", http.StatusBadRequest)
		return
	}
	if hasClassifications {
		t.node.handleApiExtractionClassifications(w, r, req, texts)
		return
	}
	if hasEntities || hasRelations {
		ids := make([]string, len(req.Inputs))
		for i := range req.Inputs {
			if req.Inputs[i].Id != nil {
				ids[i] = *req.Inputs[i].Id
			}
		}
		entityReq := entityExtractionRequest{
			Model:   req.Model,
			Texts:   texts,
			IDs:     ids,
			FlatNER: true,
		}
		if hasEntities {
			entityReq.Labels = *req.Schema.Entities
		}
		if hasRelations {
			labels := make([]string, 0, len(*req.Schema.Relations))
			for _, relation := range *req.Schema.Relations {
				label := relation.Type
				if relation.Source != nil {
					label = *relation.Source + "::" + label
				}
				if relation.Target != nil {
					label += "::" + *relation.Target
				}
				labels = append(labels, label)
			}
			entityReq.RelationLabels = labels
		}
		if req.Options != nil && req.Options.Resolver != nil {
			entityReq.Resolver = req.Options.Resolver
		}
		if req.Options != nil {
			entityReq.IncludeConfidence = valueOr(req.Options.IncludeConfidence, false)
			entityReq.IncludeSpans = valueOr(req.Options.IncludeSpans, false)
			entityReq.Threshold = valueOr(req.Options.Threshold, float32(0))
			entityReq.FlatNER = valueOr(req.Options.FlatNer, true)
		}
		t.node.handleEntityExtraction(w, r, entityReq)
		return
	}
	schemas, err := canonicalStructureSchemas(*req.Schema.Structures)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ids := make([]string, len(req.Inputs))
	for i := range req.Inputs {
		if req.Inputs[i].Id != nil {
			ids[i] = *req.Inputs[i].Id
		}
	}
	t.node.handleStructuredExtract(w, r, req.Model, texts, ids, schemas, req.Options)
}

func (ln *TermiteNode) handleApiExtractionClassifications(w http.ResponseWriter, r *http.Request, req extractingtypes.ExtractionRequest, texts []string) {
	release, err := ln.requestQueue.Acquire(r.Context())
	if err != nil {
		http.Error(w, "extraction capacity unavailable", http.StatusServiceUnavailable)
		return
	}
	defer release()
	if ln.nerRegistry == nil {
		http.Error(w, "model not found", http.StatusNotFound)
		return
	}
	model, err := ln.nerRegistry.Acquire(req.Model)
	if err != nil {
		writeModelAcquireError(w, ln.logger, req.Model, err)
		return
	}
	defer ln.nerRegistry.Release(req.Model)
	classifier, ok := model.(ner.Classifier)
	if !ok {
		http.Error(w, "model does not support classification extraction", http.StatusBadRequest)
		return
	}

	data := make([]extractingtypes.ExtractionObject, len(texts))
	for i := range req.Inputs {
		if req.Inputs[i].Id != nil {
			data[i].Id = req.Inputs[i].Id
		}
	}
	includeConfidence := req.Options != nil && valueOr(req.Options.IncludeConfidence, false)
	for _, schema := range *req.Schema.Classifications {
		if schema.Name == "" || len(schema.Labels) == 0 {
			http.Error(w, "classification name and labels are required", http.StatusBadRequest)
			return
		}
		config := ner.DefaultClassificationConfig()
		config.MultiLabel = valueOr(schema.MultiLabel, false)
		if config.MultiLabel {
			config.TopK = 0
		}
		if req.Options != nil && req.Options.Threshold != nil {
			config.Threshold = *req.Options.Threshold
		}
		results, classifyErr := classifier.ClassifyText(r.Context(), texts, schema.Labels, &config)
		if classifyErr != nil {
			http.Error(w, fmt.Sprintf("classification extraction failed: %v", classifyErr), http.StatusInternalServerError)
			return
		}
		for i, items := range results {
			if data[i].Classifications == nil {
				empty := make([]extractingtypes.ExtractionClassification, 0)
				data[i].Classifications = &empty
			}
			for _, item := range items {
				classification := extractingtypes.ExtractionClassification{Name: schema.Name, Label: item.Label}
				if includeConfidence {
					classification.Score = &item.Score
				}
				*data[i].Classifications = append(*data[i].Classifications, classification)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(extractingtypes.ExtractionResponse{Object: extractingtypes.Extraction, Model: req.Model, Data: data}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func valueOr[T any](value *T, fallback T) T {
	if value != nil {
		return *value
	}
	return fallback
}

func extractionTexts(inputs []extractingtypes.ExtractionInput) ([]string, error) {
	texts := make([]string, len(inputs))
	for i, input := range inputs {
		var text string
		if err := json.Unmarshal(input.Content, &text); err == nil {
			texts[i] = text
			continue
		}
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(input.Content, &parts); err != nil {
			return nil, fmt.Errorf("input %d must contain text", i)
		}
		var builder strings.Builder
		for _, part := range parts {
			if part.Type == "text" && part.Text != "" {
				if builder.Len() > 0 {
					builder.WriteByte('\n')
				}
				builder.WriteString(part.Text)
			}
		}
		if builder.Len() == 0 {
			return nil, fmt.Errorf("input %d must contain text", i)
		}
		texts[i] = builder.String()
	}
	return texts, nil
}

func canonicalStructureSchemas(structures map[string]extractingtypes.ExtractionStructureSchema) ([]ner.ExtractionSchema, error) {
	structureNames := make([]string, 0, len(structures))
	for name := range structures {
		structureNames = append(structureNames, name)
	}
	slices.Sort(structureNames)
	out := make([]ner.ExtractionSchema, 0, len(structureNames))
	for _, name := range structureNames {
		if name == "" {
			return nil, errors.New("structure name is required")
		}
		structure := structures[name]
		if len(structure.Fields) == 0 {
			return nil, fmt.Errorf("structure %q has no fields", name)
		}
		fieldNames := make([]string, 0, len(structure.Fields))
		for fieldName := range structure.Fields {
			fieldNames = append(fieldNames, fieldName)
		}
		slices.Sort(fieldNames)
		fields := make([]ner.SchemaField, 0, len(fieldNames))
		for _, fieldName := range fieldNames {
			if fieldName == "" {
				return nil, fmt.Errorf("structure %q has an empty field name", name)
			}
			descriptor := structure.Fields[fieldName]
			fieldType, choices, err := canonicalStructureField(descriptor)
			if err != nil {
				return nil, fmt.Errorf("structure %q field %q: %w", name, fieldName, err)
			}
			fields = append(fields, ner.SchemaField{Name: fieldName, Type: fieldType, Choices: choices})
		}
		out = append(out, ner.ExtractionSchema{Name: name, Fields: fields})
	}
	return out, nil
}

func canonicalStructureField(descriptor extractingtypes.ExtractionStructureField) (ner.FieldType, []string, error) {
	if value, err := descriptor.AsExtractionStructureField0(); err == nil {
		fieldType, err := canonicalStructureFieldType(value)
		return fieldType, nil, err
	}
	value, err := descriptor.AsExtractionStructureField1()
	if err != nil {
		return 0, nil, errors.New("descriptor must be a string or object")
	}
	fieldType := ner.FieldTypeStr
	if value.Type != nil {
		fieldType, err = canonicalStructureFieldType(string(*value.Type))
		if err != nil {
			return 0, nil, err
		}
	}
	var choices []string
	if value.Enum != nil {
		if len(*value.Enum) < 2 {
			return 0, nil, errors.New("enum must contain at least two strings")
		}
		choices = make([]string, len(*value.Enum))
		for i, choice := range *value.Enum {
			if choice == "" {
				return 0, nil, errors.New("enum values must be non-empty strings")
			}
			choices[i] = choice
		}
	}
	return fieldType, choices, nil
}

func canonicalStructureFieldType(value string) (ner.FieldType, error) {
	switch strings.ToLower(value) {
	case "str", "string":
		return ner.FieldTypeStr, nil
	case "list", "array":
		return ner.FieldTypeList, nil
	default:
		return 0, fmt.Errorf("unsupported type %q", value)
	}
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

func backendRuntimesFromAvailable() BackendRuntimes {
	var runtimes BackendRuntimes
	for _, backend := range backends.ListAvailable() {
		switch backend.Type() {
		case backends.BackendONNX:
			runtimes.Onnx = true
		case backends.BackendXLA:
			runtimes.Xla = true
		case backends.BackendCoreML:
			runtimes.Metal = true
		}
	}
	return runtimes
}

func appendOpenAIModelData(data []map[string]interface{}, names map[string]ModelInfo, seen map[string]struct{}) []map[string]interface{} {
	created := time.Now().Unix()
	for name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		data = append(data, map[string]interface{}{
			"id":       name,
			"object":   "model",
			"created":  created,
			"owned_by": "antfly-inference",
		})
	}
	return data
}

// ListModels implements ServerInterface
func (t *TermiteAPI) ListModels(w http.ResponseWriter, r *http.Request) {
	resp := ModelsResponse{
		Object:         ModelsResponseObjectList,
		Data:           []map[string]interface{}{},
		AllowDownloads: t.node.allowDownloads,
		Backends:       backendRuntimesFromAvailable(),
		Chunkers:       map[string]ModelInfo{},
		Rerankers:      map[string]ModelInfo{},
		Embedders:      map[string]ModelInfo{},
		Generators:     map[string]ModelInfo{},
		Extractors:     map[string]ModelInfo{},
		Rewriters:      map[string]ModelInfo{},
		Classifiers:    map[string]ModelInfo{},
		Readers:        map[string]ModelInfo{},
		Transcribers:   map[string]ModelInfo{},
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
		// Entity, relation, and structured extraction share one public model
		// collection. Individual capabilities describe which modes each model
		// can execute.
		resp.Extractors = capsMapToModelInfoMap(t.node.nerRegistry.List())
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

	seenOpenAIModels := map[string]struct{}{}
	resp.Data = appendOpenAIModelData(resp.Data, resp.Embedders, seenOpenAIModels)
	resp.Data = appendOpenAIModelData(resp.Data, resp.Generators, seenOpenAIModels)

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
// Supports an input field containing either a string or a ContentPart.
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

	var req struct {
		Input  json.RawMessage `json:"input"`
		Config ChunkConfig     `json:"config,omitempty"`
	}
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

	input := bytes.TrimSpace(req.Input)
	if len(input) == 0 || bytes.Equal(input, []byte("null")) {
		http.Error(w, "input is required", http.StatusBadRequest)
		return
	}
	switch input[0] {
	case '"':
		var text string
		if err := json.Unmarshal(input, &text); err != nil {
			http.Error(w, "input text must be a string", http.StatusBadRequest)
			return
		}
		if text == "" {
			http.Error(w, "input is required", http.StatusBadRequest)
			return
		}
		chunks, cacheHit, err = ln.chunker.Chunk(ctx, text, internalConfig)
		if err != nil {
			ln.logger.Error("chunking failed", zap.Error(err))
			http.Error(w, fmt.Sprintf("chunking text: %v", err), http.StatusInternalServerError)
			return
		}
	case '{':
		var discriminator struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(input, &discriminator); err != nil {
			http.Error(w, "input content part must be an object", http.StatusBadRequest)
			return
		}
		switch discriminator.Type {
		case string(generatingtypes.MediaContentPartTypeMedia):
			var mediaPart generatingtypes.MediaContentPart
			if err := json.Unmarshal(input, &mediaPart); err != nil {
				http.Error(w, "media content part is invalid", http.StatusBadRequest)
				return
			}
			if len(mediaPart.Data) == 0 {
				http.Error(w, "media content part missing 'data' field", http.StatusBadRequest)
				return
			}
			if strings.TrimSpace(mediaPart.MimeType) == "" {
				http.Error(w, "media content part missing 'mime_type' field", http.StatusBadRequest)
				return
			}
			chunks, err = ln.chunkMedia(ctx, mediaPart.Data, mediaPart.MimeType, internalConfig.Model, mediaOpts)
			if err != nil {
				ln.logger.Error("media chunking failed", zap.Error(err))
				http.Error(w, fmt.Sprintf("chunking media: %v", err), http.StatusInternalServerError)
				return
			}
		case string(generatingtypes.TextContentPartTypeText):
			var textPart generatingtypes.TextContentPart
			if err := json.Unmarshal(input, &textPart); err != nil {
				http.Error(w, "text content part is invalid", http.StatusBadRequest)
				return
			}
			if textPart.Text == "" {
				http.Error(w, "text content part missing 'text' field", http.StatusBadRequest)
				return
			}
			chunks, cacheHit, err = ln.chunker.Chunk(ctx, textPart.Text, internalConfig)
			if err != nil {
				ln.logger.Error("chunking failed", zap.Error(err))
				http.Error(w, fmt.Sprintf("chunking text: %v", err), http.StatusInternalServerError)
				return
			}
		default:
			http.Error(w, "input content part type must be 'text' or 'media'", http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "input must be a non-empty string or content part object", http.StatusBadRequest)
		return
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

	data := make([]struct {
		Object string  `json:"object"`
		Index  int     `json:"index"`
		Score  float32 `json:"score"`
	}, len(scores))
	for i, score := range scores {
		data[i] = struct {
			Object string  `json:"object"`
			Index  int     `json:"index"`
			Score  float32 `json:"score"`
		}{
			Object: "rerank.score",
			Index:  i,
			Score:  score,
		}
	}

	// Send both response shapes while generated clients converge on the
	// OpenAI-compatible data[] contract.
	resp := struct {
		Object string `json:"object"`
		Model  string `json:"model"`
		Data   []struct {
			Object string  `json:"object"`
			Index  int     `json:"index"`
			Score  float32 `json:"score"`
		} `json:"data"`
		Scores []float32 `json:"scores"`
	}{
		Object: "list",
		Model:  req.Model,
		Data:   data,
		Scores: scores,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleEntityExtraction executes the canonical entity/relation extraction request.
func (ln *TermiteNode) handleEntityExtraction(w http.ResponseWriter, r *http.Request, req entityExtractionRequest) {
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
	apiEntities := make([][]extractedEntity, len(entities))
	for i, textEntities := range entities {
		apiEntities[i] = make([]extractedEntity, len(textEntities))
		for j, e := range textEntities {
			apiEntities[i][j] = extractedEntity{
				Text:  e.Text,
				Label: e.Label,
				Start: e.Start,
				End:   e.End,
				Score: e.Score,
			}
		}
	}

	// Convert internal relation type to the unified extraction response type.
	var apiRelations [][]extractedRelation
	if len(relations) > 0 {
		apiRelations = make([][]extractedRelation, len(relations))
		for i, textRelations := range relations {
			apiRelations[i] = make([]extractedRelation, len(textRelations))
			for j, rel := range textRelations {
				apiRelations[i][j] = extractedRelation{
					Head: extractedEntity{
						Text:  rel.HeadEntity.Text,
						Label: rel.HeadEntity.Label,
						Start: rel.HeadEntity.Start,
						End:   rel.HeadEntity.End,
						Score: rel.HeadEntity.Score,
					},
					Tail: extractedEntity{
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
			SimilarityThreshold:   float64(valueOr(req.Resolver.SimilarityThreshold, float32(0.85))),
			TypeMustMatch:         valueOr(req.Resolver.TypeMustMatch, true),
			MinEntityConfidence:   valueOr(req.Resolver.MinEntityConfidence, float32(0)),
			MinRelationConfidence: valueOr(req.Resolver.MinRelationConfidence, float32(0)),
			DeduplicateRelations:  valueOr(req.Resolver.DeduplicateRelations, true),
			TrackProvenance:       valueOr(req.Resolver.TrackProvenance, true),
		}

		kg := ner.BuildKnowledgeGraph(entities, relations, cfg)

		// Build entity ID -> resolved entity lookup for relation mapping.
		entityByID := make(map[string]*ner.ResolvedEntity, len(kg.Entities))
		for i := range kg.Entities {
			entityByID[kg.Entities[i].ID] = &kg.Entities[i]
		}

		apiEntities = make([][]extractedEntity, len(req.Texts))
		for _, re := range kg.Entities {
			resolvedEntity := extractedEntity{
				Text:  re.CanonicalName,
				Label: re.Label,
				Score: re.Score,
			}
			for _, textIndex := range re.TextIndices {
				if textIndex >= 0 && textIndex < len(apiEntities) {
					apiEntities[textIndex] = append(apiEntities[textIndex], resolvedEntity)
				}
			}
		}

		apiRelations = make([][]extractedRelation, len(req.Texts))
		for _, rr := range kg.Relations {
			head := entityByID[rr.HeadID]
			tail := entityByID[rr.TailID]
			if head == nil || tail == nil {
				continue
			}
			resolvedRelation := extractedRelation{
				Head: extractedEntity{
					Text:  head.CanonicalName,
					Label: head.Label,
					Score: head.Score,
				},
				Tail: extractedEntity{
					Text:  tail.CanonicalName,
					Label: tail.Label,
					Score: tail.Score,
				},
				Label: rr.Label,
				Score: rr.Score,
			}
			for _, textIndex := range rr.TextIndices {
				if textIndex >= 0 && textIndex < len(apiRelations) {
					apiRelations[textIndex] = append(apiRelations[textIndex], resolvedRelation)
				}
			}
		}
	}

	data := make([]extractingtypes.ExtractionObject, len(apiEntities))
	for i := range apiEntities {
		if i < len(req.IDs) && req.IDs[i] != "" {
			data[i].Id = &req.IDs[i]
		}
		entities := make([]extractingtypes.ExtractionEntity, len(apiEntities[i]))
		for j, entity := range apiEntities[i] {
			entities[j] = extractingtypes.ExtractionEntity{Text: entity.Text, Label: entity.Label}
			if req.IncludeSpans {
				entities[j].Start, entities[j].End = &entity.Start, &entity.End
			}
			if req.IncludeConfidence {
				entities[j].Score = &entity.Score
			}
		}
		data[i].Entities = &entities
		if i < len(apiRelations) {
			relations := make([]extractingtypes.ExtractionRelation, 0, len(apiRelations[i]))
			for _, relation := range apiRelations[i] {
				head, headOK := extractedEntityIndex(apiEntities[i], relation.Head)
				tail, tailOK := extractedEntityIndex(apiEntities[i], relation.Tail)
				if !headOK || !tailOK {
					ln.logger.Error("relation references an entity missing from extraction output", zap.Int("input_index", i), zap.String("relation", relation.Label))
					http.Error(w, "invalid relation extraction result", http.StatusInternalServerError)
					return
				}
				apiRelation := extractingtypes.ExtractionRelation{Type: relation.Label, Source: &extractingtypes.ExtractionRelationEndpoint{EntityIndex: &head}, Target: &extractingtypes.ExtractionRelationEndpoint{EntityIndex: &tail}}
				if req.IncludeConfidence {
					apiRelation.Score = &relation.Score
				}
				relations = append(relations, apiRelation)
			}
			data[i].Relations = &relations
		}
	}
	response := extractingtypes.ExtractionResponse{
		Object: extractingtypes.Extraction,
		Data:   data,
		Model:  req.Model,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func extractedEntityIndex(entities []extractedEntity, needle extractedEntity) (int, bool) {
	for i, entity := range entities {
		if entity.Text == needle.Text && entity.Label == needle.Label && entity.Start == needle.Start && entity.End == needle.End {
			return i, true
		}
	}
	return 0, false
}

// handleStructuredExtract executes a canonical structured extraction request.
func (ln *TermiteNode) handleStructuredExtract(
	w http.ResponseWriter,
	r *http.Request,
	modelName string,
	texts []string,
	ids []string,
	schemas []ner.ExtractionSchema,
	options *extractingtypes.ExtractionOptions,
) {
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

	// Build extraction config
	config := ner.DefaultExtractionConfig()
	if options != nil {
		if options.Threshold != nil {
			config.Threshold = *options.Threshold
		}
		if options.FlatNer != nil {
			config.FlatNER = *options.FlatNer
		}
		config.IncludeConfidence = valueOr(options.IncludeConfidence, false)
		config.IncludeSpans = valueOr(options.IncludeSpans, false)
	}

	// Acquire model and check extraction support
	model, err := ln.nerRegistry.Acquire(modelName)
	if err != nil {
		writeModelAcquireError(w, ln.logger, modelName, err)
		return
	}
	defer ln.nerRegistry.Release(modelName)

	extractor, ok := model.(ner.Extractor)
	if !ok {
		http.Error(w, fmt.Sprintf("model %s does not support extraction", modelName), http.StatusBadRequest)
		return
	}

	// Perform extraction
	results, err := extractor.Extract(r.Context(), texts, schemas, config)
	if err != nil {
		ln.logger.Error("extraction failed",
			zap.String("model", modelName),
			zap.Int("num_texts", len(texts)),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("extraction failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Record metrics
	RecordExtractionRequest(modelName)
	totalFields := 0
	for _, result := range results {
		for _, instances := range result {
			for _, instance := range instances {
				totalFields += len(instance)
			}
		}
	}
	RecordExtractionFields(modelName, totalFields)

	ln.logger.Info("extraction request completed",
		zap.String("model", modelName),
		zap.Int("num_texts", len(texts)),
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

	data := make([]extractingtypes.ExtractionObject, len(apiResults))
	for i := range apiResults {
		if i < len(ids) && ids[i] != "" {
			data[i].Id = &ids[i]
		}
		structures := make(map[string]interface{}, len(apiResults[i]))
		for name, value := range apiResults[i] {
			structures[name] = value
		}
		data[i].Structures = &structures
	}
	response := extractingtypes.ExtractionResponse{
		Object: extractingtypes.Extraction,
		Data:   data,
		Model:  modelName,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
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
			if textPart, err := part.AsTextContentPart(); err == nil && textPart.Type == TextContentPartTypeText {
				result.Parts = append(result.Parts, generation.TextPart(textPart.Text))
				// Also set Content for backward compatibility with text-only generators
				if result.Content == "" {
					result.Content = textPart.Text
				}
			}
			// Try as image content part
			if imgPart, err := part.AsImageURLContentPart(); err == nil && imgPart.Type == ImageURLContentPartTypeImageUrl {
				result.Parts = append(result.Parts, generation.ImagePart(imgPart.ImageUrl.Url))
			}
			if mediaPart, err := part.AsMediaContentPart(); err == nil && mediaPart.Type == MediaContentPartTypeMedia {
				if imageURL := mediaPartImageURL(mediaPart); imageURL != "" {
					result.Parts = append(result.Parts, generation.ImagePart(imageURL))
				}
			}
		}
	}

	return result
}

func mediaPartImageURL(part MediaContentPart) string {
	if !strings.HasPrefix(part.MimeType, "image/") {
		return ""
	}
	if part.Url != "" {
		return part.Url
	}
	if len(part.Data) == 0 {
		return ""
	}
	return "data:" + part.MimeType + ";base64," + base64.StdEncoding.EncodeToString(part.Data)
}

func generateBatchErrorFromHTTPStatus(status int, message string) GenerateBatchError {
	code := "GENERATION_FAILED"
	retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
	switch status {
	case http.StatusBadRequest:
		code = "INVALID_REQUEST"
	case http.StatusNotFound:
		code = "MODEL_NOT_FOUND"
	case http.StatusRequestTimeout:
		code = "REQUEST_TIMEOUT"
	case http.StatusTooManyRequests:
		code = "QUEUE_FULL"
	case http.StatusServiceUnavailable:
		code = "SERVICE_UNAVAILABLE"
	}
	return GenerateBatchError{
		Code:      code,
		Message:   strings.TrimSpace(message),
		Retryable: retryable,
	}
}

// handleApiGenerateBatch handles synchronous batch generation requests.
//
// The Go Termite runtime delegates each item through the existing single-request
// generator path. The Zig inference server implements the native batched KV
// scheduler for compatible GGUF-backed generation requests.
func (ln *TermiteNode) handleApiGenerateBatch(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var req GenerateBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}
	if req.Mode != "" && req.Mode != GenerateBatchModeSync {
		http.Error(w, fmt.Sprintf("unsupported batch mode: %s", req.Mode), http.StatusBadRequest)
		return
	}
	if len(req.Requests) == 0 {
		http.Error(w, "requests are required", http.StatusBadRequest)
		return
	}
	if len(req.Requests) > 128 {
		http.Error(w, "requests must contain at most 128 items", http.StatusRequestEntityTooLarge)
		return
	}

	resp := GenerateBatchResponse{
		Object: GenerateBatchResponseObjectGenerateBatch,
		Data:   make([]GenerateBatchResultItem, len(req.Requests)),
		Summary: GenerateBatchSummary{
			Total: len(req.Requests),
		},
	}

	for i, item := range req.Requests {
		result := GenerateBatchResultItem{
			CustomId: item.CustomId,
			Index:    i,
		}

		if item.Body.Stream {
			result.Error = GenerateBatchError{
				Code:    "UNSUPPORTED_STREAM",
				Message: "streaming is not supported for synchronous generate batches",
			}
			resp.Summary.Failed++
			resp.Data[i] = result
			continue
		}

		body, err := json.Marshal(item.Body)
		if err != nil {
			result.Error = GenerateBatchError{
				Code:    "INVALID_REQUEST",
				Message: fmt.Sprintf("encoding request item: %v", err),
			}
			resp.Summary.Failed++
			resp.Data[i] = result
			continue
		}

		itemReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, "/ai/v1/generate", bytes.NewReader(body))
		if err != nil {
			result.Error = GenerateBatchError{
				Code:      "INTERNAL_ERROR",
				Message:   fmt.Sprintf("creating request item: %v", err),
				Retryable: true,
			}
			resp.Summary.Failed++
			resp.Data[i] = result
			continue
		}
		itemReq.Header = r.Header.Clone()
		itemReq.Header.Set("Content-Type", "application/json")

		itemWriter := newCaptureResponseWriter()
		ln.handleApiGenerate(itemWriter, itemReq)
		status := itemWriter.Status()
		if status < http.StatusOK || status >= http.StatusMultipleChoices {
			result.Error = generateBatchErrorFromHTTPStatus(status, itemWriter.body.String())
			resp.Summary.Failed++
			resp.Data[i] = result
			continue
		}

		if err := json.Unmarshal(itemWriter.body.Bytes(), &result.Response); err != nil {
			result.Error = GenerateBatchError{
				Code:      "INVALID_RESPONSE",
				Message:   fmt.Sprintf("decoding generation response: %v", err),
				Retryable: true,
			}
			resp.Summary.Failed++
			resp.Data[i] = result
			continue
		}

		resp.Summary.Succeeded++
		resp.Data[i] = result
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
		apiToolCalls := make([]generatingtypes.ToolCall, len(toolCalls))
		for i, tc := range toolCalls {
			apiToolCalls[i] = generatingtypes.ToolCall{
				Id:   tc.ID,
				Type: generatingtypes.ToolCallType(tc.Type),
				Function: generatingtypes.ToolCallFunction{
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
	if len(req.Images) > maxReadBatchImages {
		http.Error(w, fmt.Sprintf("images must contain at most %d items", maxReadBatchImages), http.StatusRequestEntityTooLarge)
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
		if errors.Is(err, errReadBatchTooLarge) {
			http.Error(w, fmt.Sprintf("total downloaded image bytes must be at most %d", maxReadBatchBytes), http.StatusRequestEntityTooLarge)
			return
		}
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
	var downloadedBytes int64

	for _, imgURL := range imageURLs {
		if downloadedBytes >= maxReadBatchBytes {
			return nil, errReadBatchTooLarge
		}
		remainingBytes := maxReadBatchBytes - downloadedBytes
		boundedSecurity := boundedReadContentSecurity(secConfig, remainingBytes)
		// Download image data - returns (mimeType, data []byte, error)
		// scraping.DownloadContent handles data:, http://, https://, file://, s3:// URLs
		// and returns already-decoded bytes (base64 decoding for data: URLs is handled internally)
		contentType, imageData, err := scraping.DownloadContent(ctx, imgURL.Url, boundedSecurity, s3Creds)
		if err != nil {
			if errors.Is(err, scraping.ErrDownloadTooLarge) {
				return nil, errReadBatchTooLarge
			}
			return nil, fmt.Errorf("downloading image %s: %w", imgURL.Url, err)
		}
		downloadedBytes += int64(len(imageData)) + int64(len(contentType))
		if downloadedBytes > maxReadBatchBytes {
			return nil, errReadBatchTooLarge
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

func boundedReadContentSecurity(secConfig *scraping.ContentSecurityConfig, remainingBytes int64) *scraping.ContentSecurityConfig {
	if remainingBytes < 1 {
		remainingBytes = 1
	}
	downloadLimit := remainingBytes + 1
	if secConfig == nil {
		return &scraping.ContentSecurityConfig{MaxDownloadSizeBytes: downloadLimit}
	}
	bounded := *secConfig
	if bounded.MaxDownloadSizeBytes <= 0 || bounded.MaxDownloadSizeBytes > downloadLimit {
		bounded.MaxDownloadSizeBytes = downloadLimit
	}
	return &bounded
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
