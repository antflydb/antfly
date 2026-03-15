/*
Copyright 2025 The Antfly Contributors

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

package client

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
	case TermiteEmbedderConfig:
		provider = EmbedderProviderTermite
		if err := modelConfig.FromTermiteEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("from termite embedder config: %w", err)
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
	case TermiteGeneratorConfig:
		provider = GeneratorProviderTermite
		if err := modelConfig.FromTermiteGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from termite generator config: %w", err)
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
	case TermiteRerankerConfig:
		provider = RerankerProviderTermite
		if err := rerankerConfig.FromTermiteRerankerConfig(v); err != nil {
			return nil, fmt.Errorf("from termite reranker config: %w", err)
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
	default:
		return nil, fmt.Errorf("unsupported index config type: %T", config)
	}
	idxConfig.Type = t

	return idxConfig, nil
}
