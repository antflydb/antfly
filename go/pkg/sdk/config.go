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
	"fmt"
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

// NewArtifactIndexSources builds the shared artifact-only source shape used by
// full-text and embeddings indexes. Graph indexes use GraphIndexSource because
// their path and format are source-specific.
func NewArtifactIndexSources(artifacts ...string) ([]ArtifactIndexSource, error) {
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
	if len(sources) == 0 {
		return nil, fmt.Errorf("at least one artifact source is required")
	}
	return sources, nil
}

// ArtifactEmbeddingSource describes one generated embedding artifact stream and
// the enrichment that produces it.
type ArtifactEmbeddingSource struct {
	// ArtifactName is the stable generated embedding artifact name.
	ArtifactName string
	// SourceArtifactName is the artifact stream to embed, for example
	// "document_chunks_v1".
	SourceArtifactName string
	// SourceField is the text field inside each source artifact payload. It
	// defaults to "text".
	SourceField string
}

// ArtifactEmbeddingIndexConfig describes a managed vector index whose vectors
// are generated from one or more existing generated artifact streams.
type ArtifactEmbeddingIndexConfig struct {
	// Sources are the embedding artifact streams indexed together. Each source
	// contributes independent vector members to the index.
	Sources []ArtifactEmbeddingSource
	// ExpectedDims is optional when the embedder can be probed by the server.
	ExpectedDims int
	Embedder     EmbedderConfig
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

	sources := config.Sources
	if len(sources) == 0 {
		return nil, fmt.Errorf("at least one artifact embedding source is required")
	}

	seen := make(map[string]struct{}, len(sources))
	publicSources := make([]ArtifactIndexSource, 0, len(sources))
	enrichments := make([]EnrichmentConfig, 0, len(sources))
	for i, source := range sources {
		if source.ArtifactName == "" {
			return nil, fmt.Errorf("sources[%d].artifact name is required", i)
		}
		if source.SourceArtifactName == "" {
			return nil, fmt.Errorf("sources[%d].source artifact name is required", i)
		}
		if _, ok := seen[source.ArtifactName]; ok {
			return nil, fmt.Errorf("duplicate embedding artifact source %q", source.ArtifactName)
		}
		seen[source.ArtifactName] = struct{}{}
		sourceField := source.SourceField
		if sourceField == "" {
			sourceField = "text"
		}
		publicSources = append(publicSources, ArtifactIndexSource{Artifact: source.ArtifactName})
		enrichments = append(enrichments, EnrichmentConfig{
			Name:               source.ArtifactName,
			Kind:               EnrichmentKindEmbedding,
			Field:              sourceField,
			SourceArtifactName: source.SourceArtifactName,
			ExpectedDims:       config.ExpectedDims,
		})
	}

	idx, err := NewIndexConfig(name, EmbeddingsIndexConfig{
		Sources:        publicSources,
		Dimension:      config.ExpectedDims,
		Embedder:       config.Embedder,
		DistanceMetric: config.DistanceMetric,
	})
	if err != nil {
		return nil, err
	}
	idx.Enrichments = enrichments
	return idx, nil
}
