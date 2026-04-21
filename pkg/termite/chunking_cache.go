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

package termite

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	termchunking "github.com/antflydb/antfly/pkg/termite/lib/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/cespare/xxhash/v2"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// CachedChunker provides in-memory caching for chunking operations with model registry.
// It handles both text chunking and media chunking (audio, etc.) through a unified interface.
type CachedChunker struct {
	registry          *ChunkerRegistry
	fixedChunker      chunking.Chunker
	fixedMediaChunker *termchunking.FixedMediaChunker // Algorithmic fallback for media chunking
	cache             *ResultCache[ChunkResult]
	logger            *zap.Logger
}

// ChunkResult stores chunking results with metadata
type ChunkResult struct {
	Chunks   []chunking.Chunk `json:"chunks"`
	Model    string           `json:"model"`
	CachedAt time.Time        `json:"cached_at"`
}

// NewCachedChunker creates a new cached chunker with model registry support.
// If sessionManager is provided, it will be used to obtain sessions for model loading (required for ONNX Runtime).
// mediaKeepAlive controls the TTL for media chunker models (audio, etc.); text chunkers use eager loading.
// maxLoadedModels limits how many models can be loaded simultaneously (0 = unlimited).
func NewCachedChunker(
	modelsDir string,
	sessionManager *backends.SessionManager,
	poolSize int,
	mediaKeepAlive time.Duration,
	maxLoadedModels uint64,
	budget *ModelBudget,
	logger *zap.Logger,
) (*CachedChunker, error) {
	cache := NewResultCache[ChunkResult]("Chunking", 2*time.Minute, logger.Named("cache"))

	// Create fixed chunker (always available as fallback)
	fixedChunker, err := termchunking.NewFixedChunker(termchunking.DefaultFixedChunkerConfig())
	if err != nil {
		cache.Close()
		return nil, fmt.Errorf("failed to create fixed chunker: %w", err)
	}

	// Create model registry with session manager
	// Text chunkers use eager loading (KeepAlive=0) since they're always needed.
	// Media chunkers use the provided keepAlive for lazy loading with TTL.
	registry, err := NewChunkerRegistry(
		ChunkerConfig{
			ModelsDir:       modelsDir,
			KeepAlive:       0,              // Eager loading for text chunkers
			MediaKeepAlive:  mediaKeepAlive, // Lazy loading for media chunkers
			MaxLoadedModels: maxLoadedModels,
			PoolSize:        poolSize, // Number of concurrent pipelines per model
		},
		sessionManager,
		budget,
		logger.Named("registry"),
	)
	if err != nil {
		cache.Close()
		_ = fixedChunker.Close()
		return nil, fmt.Errorf("failed to create chunker registry: %w", err)
	}

	cc := &CachedChunker{
		registry:          registry,
		fixedChunker:      fixedChunker,
		fixedMediaChunker: termchunking.NewFixedMediaChunker(),
		cache:             cache,
		logger:            logger,
	}

	// Log available models
	models := registry.List()
	if len(models) > 0 {
		logger.Info("Loaded ONNX chunker models", zap.Strings("models", models))
	} else {
		logger.Info("No ONNX models loaded, using built-in fixed-bert-tokenizer model only")
	}

	return cc, nil
}

// chunkConfig is the internal config format for the public API
type chunkConfig struct {
	Model         string  `json:"model"`
	TargetTokens  int     `json:"target_tokens"`
	OverlapTokens int     `json:"overlap_tokens"`
	Separator     string  `json:"separator"`
	MaxChunks     int     `json:"max_chunks"`
	Threshold     float32 `json:"threshold"`
}

// Chunk performs chunking with caching
func (cc *CachedChunker) Chunk(ctx context.Context, text string, config chunkConfig) ([]chunking.Chunk, bool, error) {
	if text == "" {
		return nil, false, nil
	}

	// Compute cache key based on config and text hash
	cacheKey := cc.computeCacheKey(text, config)

	// Check memory cache
	if item := cc.cache.Cache().Get(cacheKey); item != nil {
		cc.logger.Debug("Chunk cache hit (memory)",
			zap.String("cache_key", cacheKey),
			zap.String("model", item.Value().Model),
			zap.Int("num_chunks", len(item.Value().Chunks)))
		return item.Value().Chunks, true, nil
	}

	// Cache miss: Use singleflight to deduplicate concurrent identical requests
	cc.logger.Debug("Chunk cache miss, performing chunking",
		zap.String("cache_key", cacheKey),
		zap.Int("text_length", len(text)),
		zap.String("model", config.Model))

	v, err, shared := cc.cache.SFGroup().Do(cacheKey, func() (any, error) {
		// Double-check cache (another goroutine might have populated it)
		if item := cc.cache.Cache().Get(cacheKey); item != nil {
			cc.logger.Debug("Chunk found in cache during singleflight")
			return item.Value(), nil
		}

		// Perform actual chunking
		chunks, model, err := cc.performChunking(ctx, text, config)
		if err != nil {
			return nil, err
		}

		result := ChunkResult{
			Chunks:   chunks,
			Model:    model,
			CachedAt: time.Now(),
		}

		// Store in memory cache
		cc.cache.Cache().Set(cacheKey, result, ttlcache.DefaultTTL)

		cc.logger.Info("Chunking completed and cached",
			zap.String("cache_key", cacheKey),
			zap.String("model", model),
			zap.Int("num_chunks", len(chunks)),
			zap.Int("text_length", len(text)))

		return result, nil
	})

	if shared {
		cc.logger.Debug("Singleflight deduplication hit")
	}

	if err != nil {
		return nil, false, err
	}

	result := v.(ChunkResult)
	return result.Chunks, false, nil
}

// performChunking executes the actual chunking logic based on model
func (cc *CachedChunker) performChunking(ctx context.Context, text string, config chunkConfig) (chunks []chunking.Chunk, model string, err error) {
	model = config.Model

	// Build per-request options from config
	opts := cc.buildChunkOptions(config)

	// Check if it's a built-in fixed model
	isFixedModel := model == termchunking.ModelFixedBert || model == termchunking.ModelFixedBPE

	// Try to get ONNX model from registry first (if not a built-in fixed model)
	if !isFixedModel {
		if chunker, err := cc.registry.Acquire(model); err == nil {
			cc.logger.Debug("Using ONNX model from registry",
				zap.String("model", model))

			chunks, err = chunker.Chunk(ctx, text, opts)
			cc.registry.Release(model)
			if err != nil {
				cc.logger.Warn("ONNX model failed, falling back to fixed-bert-tokenizer",
					zap.String("model", model),
					zap.Error(err))
				// Fall through to fixed chunker
			} else {
				return chunks, model, nil
			}
		} else {
			cc.logger.Debug("Model not found in registry, falling back to fixed-bert-tokenizer",
				zap.String("requested", model),
				zap.Error(err))
		}
	}

	// Use fixed chunker as fallback
	cc.logger.Debug("Using fixed chunker")
	chunks, err = cc.fixedChunker.Chunk(ctx, text, opts)
	model = termchunking.ModelFixedBert

	if err != nil {
		return nil, "", fmt.Errorf("chunking failed with model %s: %w", model, err)
	}

	return chunks, model, nil
}

// buildChunkOptions converts internal chunkConfig to the chunking.ChunkOptions type.
// Only sets non-zero values to allow chunker defaults to apply for unset options.
func (cc *CachedChunker) buildChunkOptions(config chunkConfig) chunking.ChunkOptions {
	var opts chunking.ChunkOptions
	if config.MaxChunks > 0 {
		opts.MaxChunks = config.MaxChunks
	}
	if config.Threshold > 0 {
		opts.Threshold = config.Threshold
	}
	if config.TargetTokens > 0 {
		opts.Text.TargetTokens = config.TargetTokens
	}
	return opts
}

// computeCacheKey generates a cache key from text and config
func (cc *CachedChunker) computeCacheKey(text string, config chunkConfig) string {
	// Create a deterministic key from config
	configStr := fmt.Sprintf("%s:%d:%d:%s:%d:%.3f",
		config.Model,
		config.TargetTokens,
		config.OverlapTokens,
		config.Separator,
		config.MaxChunks,
		config.Threshold)

	// Hash text separately (for large texts)
	textHash := sha256.Sum256([]byte(text))

	// Combine config and text hash
	combined := configStr + string(textHash[:])
	return strconv.FormatUint(xxhash.Sum64String(combined), 16)
}

// ListModels returns all available chunker models and strategies
func (cc *CachedChunker) ListModels() []string {
	models := cc.registry.List()
	// Add built-in strategies
	all := append([]string{termchunking.ModelFixedBert, termchunking.ModelFixedBPE}, models...)
	return all
}

// ChunkMedia routes media chunking to a model-based media chunker from the registry
// (if a model name is specified and available), or falls back to the fixed-duration
// algorithmic media chunker.
func (cc *CachedChunker) ChunkMedia(ctx context.Context, data []byte, mimeType, model string, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	// Try model-based media chunker from registry
	if model != "" {
		chunker, err := cc.registry.AcquireMedia(model)
		if err == nil {
			defer cc.registry.Release(model)
			return chunker.ChunkMedia(ctx, data, mimeType, opts)
		}
		cc.logger.Debug("Model-based media chunker not available, falling back to fixed",
			zap.String("model", model),
			zap.Error(err))
	}

	// Fall back to algorithmic media chunker
	return cc.fixedMediaChunker.ChunkMedia(ctx, data, mimeType, opts)
}

// ListWithCapabilities returns a map of all model names to their capabilities,
// including both text and media chunker models from the registry.
func (cc *CachedChunker) ListWithCapabilities() map[string][]string {
	return cc.registry.ListWithCapabilities()
}

// HasCapability checks if a model has a specific capability (e.g., audio).
func (cc *CachedChunker) HasCapability(modelName string, capability modelregistry.Capability) bool {
	return cc.registry.HasCapability(modelName, capability)
}

// Close releases resources
func (cc *CachedChunker) Close() error {
	cc.cache.Close()

	if cc.registry != nil {
		if err := cc.registry.Close(); err != nil {
			cc.logger.Warn("Error closing chunker registry", zap.Error(err))
		}
	}

	if cc.fixedChunker != nil {
		if err := cc.fixedChunker.Close(); err != nil {
			cc.logger.Warn("Error closing fixed chunker", zap.Error(err))
		}
	}

	return nil
}
