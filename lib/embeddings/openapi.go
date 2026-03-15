// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package embeddings

import "fmt"

func NewModelConfig(config any) (*EmbedderConfig, error) {
	var provider EmbedderProvider
	modelConfig := &EmbedderConfig{}
	switch v := config.(type) {
	case OllamaEmbedderConfig:
		provider = EmbedderProviderOllama
		if err := modelConfig.FromOllamaEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Ollama embedder config: %w", err)
		}
	case OpenAIEmbedderConfig:
		provider = EmbedderProviderOpenai
		if err := modelConfig.FromOpenAIEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert OpenAI embedder config: %w", err)
		}
	case GoogleEmbedderConfig:
		provider = EmbedderProviderGemini
		if err := modelConfig.FromGoogleEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Google embedder config: %w", err)
		}
	case BedrockEmbedderConfig:
		provider = EmbedderProviderBedrock
		if err := modelConfig.FromBedrockEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Bedrock embedder config: %w", err)
		}
	case AntflyEmbedderConfig:
		provider = EmbedderProviderAntfly
		if err := modelConfig.FromAntflyEmbedderConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Antfly embedder config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown model config type: %T", v)
	}

	modelConfig.Provider = provider
	return modelConfig, nil
}
