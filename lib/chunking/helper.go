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

import (
	"context"
	"fmt"

	libtermite "github.com/antflydb/antfly/lib/termite"
	"go.uber.org/zap"
)

// ChunkingHelper helps with document chunking operations
type ChunkingHelper struct {
	Name    string
	chunker Chunker
	logger  *zap.Logger
}

// detectChunkerProvider attempts to detect the provider from the union data
// when the Provider field is not set (e.g., SDK's FromAntflyChunkerConfig).
// It tries to unmarshal as each provider type and uses heuristics to determine the provider.
func detectChunkerProvider(config ChunkerConfig) ChunkerProvider {
	// Try Antfly first - if it has TargetTokens or other Antfly-specific fields, it's Antfly
	if antflyConfig, err := config.AsAntflyChunkerConfig(); err == nil {
		// AntflyChunkerConfig uses Text.TargetTokens (primary field for fixed-size chunking)
		if antflyConfig.Text.TargetTokens > 0 || antflyConfig.Text.OverlapTokens > 0 || antflyConfig.Text.Separator != "" {
			return ChunkerProviderAntfly
		}
	}

	// Try Termite - if it has Model or ApiUrl, it's likely Termite
	if termiteConfig, err := config.AsTermiteChunkerConfig(); err == nil {
		// TermiteChunkerConfig uses Model (required field for semantic chunking)
		if termiteConfig.Model != "" || termiteConfig.ApiUrl != "" {
			return ChunkerProviderTermite
		}
	}

	// Could not detect provider
	return ""
}

// NewChunkingHelper creates a new chunking helper from a ChunkerConfig.
// This function properly respects the Provider field in the config:
//   - For "antfly" provider: uses local chunking directly, ignoring Termite URL
//   - For "termite" provider: uses Termite service for chunking
func NewChunkingHelper(name string, config ChunkerConfig, logger *zap.Logger) (*ChunkingHelper, error) {
	// If Provider is empty (e.g., SDK's FromAntflyChunkerConfig doesn't set it),
	// try to detect the provider from the union data
	provider := config.Provider
	if provider == "" {
		provider = detectChunkerProvider(config)
		if provider != "" {
			logger.Debug("Detected chunker provider from union data",
				zap.String("name", name),
				zap.String("provider", string(provider)))
			config.Provider = provider
		}
	}

	switch provider {
	case ChunkerProviderAntfly:
		// Use local Antfly chunking directly - ignore Termite URL even if configured
		logger.Info("Using local Antfly chunking (provider=antfly)",
			zap.String("name", name))

		// Create chunker using the registry (config already has correct provider)
		chunker, err := NewChunker(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create antfly chunker: %w", err)
		}

		return &ChunkingHelper{
			Name:    name,
			chunker: chunker,
			logger:  logger,
		}, nil

	case ChunkerProviderTermite:
		// Use Termite for chunking
		termiteConfig, err := config.AsTermiteChunkerConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to extract termite chunker config: %w", err)
		}

		// Get Termite URL from config, env var, or global default
		termiteURL := libtermite.ResolveURL(termiteConfig.ApiUrl)
		if termiteURL == "" {
			return nil, fmt.Errorf("termite chunker configured but no Termite URL available")
		}

		logger.Info("Using Termite for chunking (provider=termite)",
			zap.String("name", name),
			zap.String("termite_url", termiteURL),
			zap.String("model", termiteConfig.Model))

		return NewTermiteChunkingHelper(name, termiteURL, termiteConfig, logger)

	default:
		return nil, fmt.Errorf("unknown chunker provider: %s", config.Provider)
	}
}

// NewTermiteChunkingHelper creates a chunking helper that uses Termite for chunking
func NewTermiteChunkingHelper(name string, termiteURL string, config TermiteChunkerConfig, logger *zap.Logger) (*ChunkingHelper, error) {
	// Set API URL if not already set
	if config.ApiUrl == "" {
		config.ApiUrl = termiteURL
	}

	// Create chunker config using helper (sets provider automatically)
	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunker config: %w", err)
	}

	// Create chunker using the registry
	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunker: %w", err)
	}

	return &ChunkingHelper{
		Name:    name,
		chunker: chunker,
		logger:  logger,
	}, nil
}

// ChunkDocument chunks a document and returns the chunks
func (h *ChunkingHelper) ChunkDocument(text string) ([]Chunk, error) {
	if text == "" {
		h.logger.Debug("ChunkDocument called with empty text")
		return nil, nil
	}

	// Safety check - this should never happen but prevents panic
	if h.chunker == nil {
		h.logger.Error("ChunkDocument called but chunker not initialized")
		return nil, fmt.Errorf("chunker not initialized")
	}

	h.logger.Debug("Starting document chunking",
		zap.String("chunker", h.Name),
		zap.Int("text_length", len(text)),
		zap.String("text_preview", text[:min(100, len(text))]))

	// Perform chunking (lib chunker requires context)
	chunks, err := h.chunker.Chunk(context.Background(), text)
	if err != nil {
		h.logger.Error("Chunking failed",
			zap.String("chunker", h.Name),
			zap.Error(err))
		return nil, fmt.Errorf("chunking failed: %w", err)
	}

	h.logger.Debug("Document chunked successfully",
		zap.String("chunker", h.Name),
		zap.Int("num_chunks", len(chunks)),
		zap.Int("text_length", len(text)))

	return chunks, nil
}
