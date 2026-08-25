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
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func NewEmbedderConfig(config any) (*EmbedderConfig, error) {
	var provider EmbedderProvider
	modelConfig := &EmbedderConfig{}
	switch v := config.(type) {
	case OllamaEmbedderConfig:
		provider = EmbedderProviderOllama
		if err := modelConfig.FromOllamaEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("from ollama embedder config: %w", err)
		}
	case OpenAIEmbedderConfig:
		provider = EmbedderProviderOpenai
		if err := modelConfig.FromOpenAIEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("from openai embedder config: %w", err)
		}
	case GoogleEmbedderConfig:
		provider = EmbedderProviderGemini
		if err := modelConfig.FromGoogleEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("from google embedder config: %w", err)
		}
	case BedrockEmbedderConfig:
		provider = EmbedderProviderBedrock
		if err := modelConfig.FromBedrockEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("from bedrock embedder config: %w", err)
		}
	case VertexEmbedderConfig:
		provider = EmbedderProviderVertex
		if err := modelConfig.FromVertexEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("from vertex embedder config: %w", err)
		}
	case AntflyEmbedderConfig:
		provider = EmbedderProviderAntfly
		if err := modelConfig.FromAntflyEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("from antfly embedder config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown model config type: %T", v)
	}

	modelConfig.Provider = provider
	return modelConfig, nil
}

func NewGeneratorConfig(config any) (*GeneratorConfig, error) {
	var provider GeneratorProvider
	modelConfig := &GeneratorConfig{}
	switch v := config.(type) {
	case OllamaGeneratorConfig:
		provider = GeneratorProviderOllama
		if err := modelConfig.FromOllamaGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from ollama generator config: %w", err)
		}
	case OpenAIGeneratorConfig:
		provider = GeneratorProviderOpenai
		if err := modelConfig.FromOpenAIGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from openai generator config: %w", err)
		}
	case GoogleGeneratorConfig:
		provider = GeneratorProviderGemini
		if err := modelConfig.FromGoogleGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from google generator config: %w", err)
		}
	case BedrockGeneratorConfig:
		provider = GeneratorProviderBedrock
		if err := modelConfig.FromBedrockGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from bedrock generator config: %w", err)
		}
	case VertexGeneratorConfig:
		provider = GeneratorProviderVertex
		if err := modelConfig.FromVertexGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from vertex generator config: %w", err)
		}
	case AnthropicGeneratorConfig:
		provider = GeneratorProviderAnthropic
		if err := modelConfig.FromAnthropicGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from anthropic generator config: %w", err)
		}
	case AntflyGeneratorConfig:
		provider = GeneratorProviderAntfly
		if err := modelConfig.FromAntflyGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from antfly generator config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown model config type: %T", v)
	}

	modelConfig.Provider = provider
	return modelConfig, nil
}

func NewRerankerConfig(config any) (*RerankerConfig, error) {
	var provider RerankerProvider
	rerankerConfig := &RerankerConfig{}
	switch v := config.(type) {
	case OllamaRerankerConfig:
		provider = RerankerProviderOllama
		if err := rerankerConfig.FromOllamaRerankerConfig(v); err != nil {
			return nil, fmt.Errorf("from ollama reranker config: %w", err)
		}
	case AntflyRerankerConfig:
		provider = RerankerProviderAntfly
		if err := rerankerConfig.FromAntflyRerankerConfig(v); err != nil {
			return nil, fmt.Errorf("from antfly reranker config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown reranker config type: %T", v)
	}

	rerankerConfig.Provider = provider
	return rerankerConfig, nil
}

func NewIndexConfig(name string, config any) (*IndexConfig, error) {
	var t IndexType
	idxConfig := &IndexConfig{
		Name: name,
	}
	switch v := config.(type) {
	case EmbeddingsIndexConfig:
		t = IndexTypeEmbeddings
		if err := idxConfig.FromEmbeddingsIndexConfig(v); err != nil {
			return nil, fmt.Errorf("from embeddings index config: %w", err)
		}
	case FullTextIndexConfig:
		t = IndexTypeFullText
		if err := idxConfig.FromFullTextIndexConfig(v); err != nil {
			return nil, fmt.Errorf("from full text index config: %w", err)
		}
	case GraphIndexConfig:
		t = IndexTypeGraph
		if err := idxConfig.FromGraphIndexConfig(v); err != nil {
			return nil, fmt.Errorf("from graph index config: %w", err)
		}
	case AlgebraicIndexConfig:
		t = IndexTypeAlgebraic
		if err := idxConfig.FromAlgebraicIndexConfig(v); err != nil {
			return nil, fmt.Errorf("from algebraic index config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported index config type: %T", config)
	}
	idxConfig.Type = t

	return idxConfig, nil
}

// NewCreateIndexRequest builds the path-identified request body for CreateIndex.
// The index name is deliberately absent so it cannot disagree with the URL.
func NewCreateIndexRequest(config any) (*CreateIndexRequest, error) {
	request := &CreateIndexRequest{}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("marshal index config: %w", err)
	}
	switch typed := config.(type) {
	case IndexConfig:
		return newCreateIndexRequestFromIndexConfig(data, typed.Type)
	case *IndexConfig:
		if typed == nil {
			return nil, fmt.Errorf("index config must not be nil")
		}
		return newCreateIndexRequestFromIndexConfig(data, typed.Type)
	case EmbeddingsIndexConfig, CreateEmbeddingsIndexRequest:
		var variant oapi.CreateEmbeddingsIndexRequest
		if err := json.Unmarshal(data, &variant); err != nil {
			return nil, fmt.Errorf("build embeddings create index request: %w", err)
		}
		variant.Type = oapi.CreateEmbeddingsIndexRequestTypeEmbeddings
		if err := request.FromCreateEmbeddingsIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set embeddings create index request: %w", err)
		}
	case *CreateEmbeddingsIndexRequest:
		if typed == nil {
			return nil, fmt.Errorf("embeddings create index request must not be nil")
		}
		variant := *typed
		variant.Type = oapi.CreateEmbeddingsIndexRequestTypeEmbeddings
		if err := request.FromCreateEmbeddingsIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set embeddings create index request: %w", err)
		}
	case FullTextIndexConfig, CreateFullTextIndexRequest:
		var variant oapi.CreateFullTextIndexRequest
		if err := json.Unmarshal(data, &variant); err != nil {
			return nil, fmt.Errorf("build full-text create index request: %w", err)
		}
		variant.Type = oapi.CreateFullTextIndexRequestTypeFullText
		if err := request.FromCreateFullTextIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set full-text create index request: %w", err)
		}
	case *CreateFullTextIndexRequest:
		if typed == nil {
			return nil, fmt.Errorf("full-text create index request must not be nil")
		}
		variant := *typed
		variant.Type = oapi.CreateFullTextIndexRequestTypeFullText
		if err := request.FromCreateFullTextIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set full-text create index request: %w", err)
		}
	case GraphIndexConfig, CreateGraphIndexRequest:
		var variant oapi.CreateGraphIndexRequest
		if err := json.Unmarshal(data, &variant); err != nil {
			return nil, fmt.Errorf("build graph create index request: %w", err)
		}
		variant.Type = oapi.CreateGraphIndexRequestTypeGraph
		if err := request.FromCreateGraphIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set graph create index request: %w", err)
		}
	case *CreateGraphIndexRequest:
		if typed == nil {
			return nil, fmt.Errorf("graph create index request must not be nil")
		}
		variant := *typed
		variant.Type = oapi.CreateGraphIndexRequestTypeGraph
		if err := request.FromCreateGraphIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set graph create index request: %w", err)
		}
	case AlgebraicIndexConfig, CreateAlgebraicIndexRequest:
		var variant oapi.CreateAlgebraicIndexRequest
		if err := json.Unmarshal(data, &variant); err != nil {
			return nil, fmt.Errorf("build algebraic create index request: %w", err)
		}
		variant.Type = oapi.CreateAlgebraicIndexRequestTypeAlgebraic
		if err := request.FromCreateAlgebraicIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set algebraic create index request: %w", err)
		}
	case *CreateAlgebraicIndexRequest:
		if typed == nil {
			return nil, fmt.Errorf("algebraic create index request must not be nil")
		}
		variant := *typed
		variant.Type = oapi.CreateAlgebraicIndexRequestTypeAlgebraic
		if err := request.FromCreateAlgebraicIndexRequest(variant); err != nil {
			return nil, fmt.Errorf("set algebraic create index request: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported index config type: %T", config)
	}
	return request, nil
}

func newCreateIndexRequestFromIndexConfig(data []byte, indexType IndexType) (*CreateIndexRequest, error) {
	switch indexType {
	case IndexTypeEmbeddings, IndexTypeFullText, IndexTypeGraph, IndexTypeAlgebraic:
	default:
		return nil, fmt.Errorf("unsupported index config type: %q", indexType)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, fmt.Errorf("decode index config: %w", err)
	}
	delete(body, "name")

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal create index request: %w", err)
	}
	request := &CreateIndexRequest{}
	if err := json.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("build create index request: %w", err)
	}
	return request, nil
}

const maxArtifactSources = 64

// NewArtifactIndexSources builds the shared artifact-only source shape used by
// full-text and embeddings indexes.
func NewArtifactIndexSources(artifacts ...string) ([]ArtifactIndexSource, error) {
	if len(artifacts) == 0 {
		return nil, fmt.Errorf("at least one artifact source is required")
	}
	if len(artifacts) > maxArtifactSources {
		return nil, fmt.Errorf("at most %d artifact sources are allowed", maxArtifactSources)
	}
	sources := make([]ArtifactIndexSource, 0, len(artifacts))
	seen := make(map[string]struct{}, len(artifacts))
	for i, artifact := range artifacts {
		if artifact == "" {
			return nil, fmt.Errorf("artifacts[%d] is required", i)
		}
		if _, ok := seen[artifact]; ok {
			return nil, fmt.Errorf("duplicate artifact source %q", artifact)
		}
		seen[artifact] = struct{}{}
		sources = append(sources, ArtifactIndexSource{Artifact: artifact})
	}
	return sources, nil
}

// ArtifactFullTextIndexOptions controls optional projection behavior for an
// artifact-backed full-text index.
type ArtifactFullTextIndexOptions struct {
	Field   string
	MemOnly bool
}

// NewArtifactFullTextIndexConfig builds a full-text index over one or more
// generated chunk or textual asset streams using whole-artifact projection.
func NewArtifactFullTextIndexConfig(name string, artifacts ...string) (*IndexConfig, error) {
	return NewArtifactFullTextIndexConfigWithOptions(name, ArtifactFullTextIndexOptions{}, artifacts...)
}

// NewArtifactFullTextIndexConfigWithOptions builds an artifact-backed
// full-text index with an optional content field shared by every source.
func NewArtifactFullTextIndexConfigWithOptions(name string, options ArtifactFullTextIndexOptions, artifacts ...string) (*IndexConfig, error) {
	if name == "" {
		return nil, fmt.Errorf("index name is required")
	}
	sources, err := NewArtifactIndexSources(artifacts...)
	if err != nil {
		return nil, err
	}
	field := strings.TrimSpace(options.Field)
	if options.Field != "" && field == "" {
		return nil, fmt.Errorf("field must not be empty")
	}
	return NewIndexConfig(name, FullTextIndexConfig{
		Sources: sources,
		Field:   field,
		MemOnly: options.MemOnly,
	})
}

// NewGraphTemplateValue creates a string literal or Handlebars template value.
func NewGraphTemplateValue(value string) (GraphTemplateValue, error) {
	var result GraphTemplateValue
	if err := result.FromGraphTemplateValue0(value); err != nil {
		return GraphTemplateValue{}, fmt.Errorf("encode graph template value: %w", err)
	}
	return result, nil
}

// NewGraphNumericValue creates a finite numeric graph mapping value.
func NewGraphNumericValue(value float64) (GraphTemplateValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return GraphTemplateValue{}, fmt.Errorf("graph numeric value must be finite")
	}
	var result GraphTemplateValue
	if err := result.FromGraphTemplateValue1(value); err != nil {
		return GraphTemplateValue{}, fmt.Errorf("encode graph numeric value: %w", err)
	}
	return result, nil
}

// NewGraphIndexSources validates and defensively copies graph artifact sources.
func NewGraphIndexSources(sources ...GraphArtifactSourceConfig) ([]GraphArtifactSourceConfig, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("at least one graph artifact source is required")
	}
	if len(sources) > maxArtifactSources {
		return nil, fmt.Errorf("at most %d graph artifact sources are allowed", maxArtifactSources)
	}
	result := make([]GraphArtifactSourceConfig, len(sources))
	seen := make(map[string]struct{}, len(sources))
	for i, source := range sources {
		if source.Artifact == "" {
			return nil, fmt.Errorf("sources[%d].artifact is required", i)
		}
		if _, exists := seen[source.Artifact]; exists {
			return nil, fmt.Errorf("duplicate graph artifact source %q", source.Artifact)
		}
		if source.Format != "" && source.Format != GraphArtifactSourceConfigFormatExtractionRelation && source.Format != GraphArtifactSourceConfigFormatExtractionGraph {
			return nil, fmt.Errorf("sources[%d].format is invalid", i)
		}
		if source.Nodes.Model != "" && source.Nodes.Model != GraphArtifactNodeMappingConfigModelDocument && source.Nodes.Model != GraphArtifactNodeMappingConfigModelExternal {
			return nil, fmt.Errorf("sources[%d].nodes.model is invalid", i)
		}
		fields := append([]string(nil), source.Context.DocFields...)
		fieldSet := make(map[string]struct{}, len(fields))
		for j, field := range fields {
			if field == "" {
				return nil, fmt.Errorf("sources[%d].context.doc_fields[%d] is required", i, j)
			}
			if _, exists := fieldSet[field]; exists {
				return nil, fmt.Errorf("sources[%d].context.doc_fields contains duplicate %q", i, field)
			}
			fieldSet[field] = struct{}{}
		}
		result[i] = source
		result[i].Context.DocFields = fields
		if source.Edge.Metadata != nil {
			encoded, err := json.Marshal(source.Edge.Metadata)
			if err != nil {
				return nil, fmt.Errorf("sources[%d].edge.metadata must contain JSON values: %w", i, err)
			}
			result[i].Edge.Metadata = nil
			if err := json.Unmarshal(encoded, &result[i].Edge.Metadata); err != nil {
				return nil, fmt.Errorf("copy sources[%d].edge.metadata: %w", i, err)
			}
		}
		seen[source.Artifact] = struct{}{}
	}
	return result, nil
}

// ArtifactEmbeddingSource describes one generated embedding artifact stream
// and the enrichment that produces it.
type ArtifactEmbeddingSource struct {
	// ArtifactName is the stable generated embedding artifact name.
	ArtifactName string
	// SourceArtifactName is an upstream artifact stream such as
	// "document_chunks_v1". Leave it empty to read a document field directly.
	SourceArtifactName string
	// SourceField defaults to "text" when SourceTemplate is empty.
	SourceField string
	// SourceTemplate optionally renders the embedding input.
	SourceTemplate string
}

// ArtifactEmbeddingIndexConfig describes a managed vector index whose vectors
// are generated from one or more document- or artifact-backed streams.
type ArtifactEmbeddingIndexConfig struct {
	// Sources contribute independent vector members to the index.
	Sources []ArtifactEmbeddingSource
	// ExpectedDims is optional when the embedder can be probed by the server.
	ExpectedDims int
	// Sparse creates a sparse token-space index. ExpectedDims must be zero.
	Sparse   bool
	Embedder EmbedderConfig
	// DistanceMetric defaults on the server when left empty.
	DistanceMetric DistanceMetric
}

func NewArtifactEmbeddingIndexConfig(name string, config ArtifactEmbeddingIndexConfig) (*IndexConfig, error) {
	if name == "" {
		return nil, fmt.Errorf("index name is required")
	}
	if config.Embedder.Provider == "" {
		return nil, fmt.Errorf("embedder provider is required")
	}
	if len(config.Sources) == 0 {
		return nil, fmt.Errorf("at least one artifact embedding source is required")
	}
	if len(config.Sources) > maxArtifactSources {
		return nil, fmt.Errorf("at most %d artifact embedding sources are allowed", maxArtifactSources)
	}
	if config.ExpectedDims < 0 {
		return nil, fmt.Errorf("expected dimensions cannot be negative")
	}
	if config.Sparse && config.ExpectedDims != 0 {
		return nil, fmt.Errorf("expected dimensions must be zero for sparse embedding indexes")
	}
	if config.Sparse && config.DistanceMetric != "" {
		return nil, fmt.Errorf("distance metric must be empty for sparse embedding indexes")
	}
	if config.DistanceMetric != "" && config.DistanceMetric != DistanceMetricL2Squared && config.DistanceMetric != DistanceMetricInnerProduct && config.DistanceMetric != DistanceMetricCosine {
		return nil, fmt.Errorf("distance metric is invalid")
	}

	artifactNames := make([]string, 0, len(config.Sources))
	enrichments := make([]EnrichmentConfig, 0, len(config.Sources))
	for i, source := range config.Sources {
		if source.ArtifactName == "" {
			return nil, fmt.Errorf("sources[%d].artifact name is required", i)
		}
		sourceField := source.SourceField
		if sourceField == "" && source.SourceTemplate == "" {
			sourceField = "text"
		}
		artifactNames = append(artifactNames, source.ArtifactName)
		enrichments = append(enrichments, EnrichmentConfig{
			Name:               source.ArtifactName,
			Kind:               EnrichmentKindEmbedding,
			Field:              sourceField,
			Template:           source.SourceTemplate,
			SourceArtifactName: source.SourceArtifactName,
			ExpectedDims:       config.ExpectedDims,
		})
	}
	sources, err := NewArtifactIndexSources(artifactNames...)
	if err != nil {
		return nil, err
	}
	idx, err := NewIndexConfig(name, EmbeddingsIndexConfig{
		Sources:        sources,
		Dimension:      config.ExpectedDims,
		Sparse:         config.Sparse,
		Embedder:       config.Embedder,
		DistanceMetric: config.DistanceMetric,
	})
	if err != nil {
		return nil, err
	}
	idx.Enrichments = enrichments
	return idx, nil
}
