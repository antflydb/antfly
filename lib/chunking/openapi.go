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

package chunking

import "fmt"

// NewChunkerConfig creates a unified ChunkerConfig from a provider-specific config.
// Similar to embeddings.NewModelConfig, this helper simplifies creating ChunkerConfig
// instances from specific provider configurations.
func NewChunkerConfig(config any) (*ChunkerConfig, error) {
	var provider ChunkerProvider
	chunkerConfig := &ChunkerConfig{}

	switch v := config.(type) {
	case TermiteChunkerConfig:
		provider = ChunkerProviderTermite
		if err := chunkerConfig.FromTermiteChunkerConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Termite chunker config: %w", err)
		}
	case AntflyChunkerConfig:
		provider = ChunkerProviderAntfly
		if err := chunkerConfig.FromAntflyChunkerConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Antfly chunker config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown chunker config type: %T", v)
	}

	chunkerConfig.Provider = provider
	return chunkerConfig, nil
}

// GetProviderConfig extracts the provider-specific config from a unified ChunkerConfig.
// This is the inverse of NewChunkerConfig - it takes a unified config and returns
// the provider-specific configuration.
func GetProviderConfig(config ChunkerConfig) (any, error) {
	switch config.Provider {
	case ChunkerProviderTermite:
		return config.AsTermiteChunkerConfig()
	case ChunkerProviderAntfly:
		return config.AsAntflyChunkerConfig()
	case ChunkerProviderMock:
		// Mock provider doesn't have additional config
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown chunker provider: %s", config.Provider)
	}
}

// GetFullTextIndex extracts the FullTextIndex configuration from a ChunkerConfig.
// Returns nil if FullTextIndex is not configured.
func GetFullTextIndex(config ChunkerConfig) map[string]any {
	return config.FullTextIndex
}
