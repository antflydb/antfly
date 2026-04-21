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

package chunking

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"go.uber.org/zap"
)

// Ensure PooledChunker implements the Chunker interface
var _ chunking.Chunker = (*PooledChunker)(nil)

// ChunkerConfig contains configuration for the chunker.
type ChunkerConfig struct {
	// MaxChunks is the maximum number of chunks to generate per document.
	MaxChunks int

	// Threshold is the minimum confidence threshold for separator detection.
	Threshold float32

	// TargetTokens is the target number of tokens per chunk.
	TargetTokens int
}

// DefaultChunkerConfig returns sensible defaults for the chunker.
func DefaultChunkerConfig() ChunkerConfig {
	return ChunkerConfig{
		MaxChunks:    50,
		Threshold:    0.5,
		TargetTokens: 500,
	}
}

// PooledChunkerConfig holds configuration for creating a PooledChunker.
type PooledChunkerConfig struct {
	// ModelPath is the path to the model directory
	ModelPath string

	// PoolSize determines how many concurrent requests can be processed (0 = 1)
	PoolSize int

	// ChunkerConfig contains chunking-specific settings
	ChunkerConfig ChunkerConfig

	// ModelBackends specifies which backends this model supports (nil = all backends)
	ModelBackends []string

	// Logger for logging (nil = no logging)
	Logger *zap.Logger
}

// PooledChunker manages multiple ChunkingPipeline instances for concurrent chunking.
// Uses the new backends package (go-huggingface + gomlx/onnxruntime).
type PooledChunker struct {
	pool        *pool.LazyPool[*pipelines.ChunkingPipeline]
	config      ChunkerConfig
	logger      *zap.Logger
	backendType backends.BackendType
}

// NewPooledChunker creates a new ChunkingPipeline-based pooled chunker.
// This is the new implementation using go-huggingface tokenizers and the backends package.
func NewPooledChunker(
	cfg PooledChunkerConfig,
	sessionManager *backends.SessionManager,
) (*PooledChunker, backends.BackendType, error) {
	if cfg.ModelPath == "" {
		return nil, "", fmt.Errorf("model path is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// Apply defaults for zero values
	chunkerCfg := cfg.ChunkerConfig
	if chunkerCfg.MaxChunks <= 0 {
		chunkerCfg.MaxChunks = 50
	}
	if chunkerCfg.Threshold <= 0 {
		chunkerCfg.Threshold = 0.5
	}
	if chunkerCfg.TargetTokens <= 0 {
		chunkerCfg.TargetTokens = 500
	}

	// Default pool size to 1 if not specified
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	logger.Info("Initializing pooled chunker",
		zap.String("modelPath", cfg.ModelPath),
		zap.Int("poolSize", poolSize),
		zap.Int("maxChunks", chunkerCfg.MaxChunks),
		zap.Float32("threshold", chunkerCfg.Threshold),
		zap.Int("targetTokens", chunkerCfg.TargetTokens))

	// Capture backendUsed from the first factory call
	var backendUsed backends.BackendType

	lazyPool, _, err := pool.New(pool.Config[*pipelines.ChunkingPipeline]{
		Size: poolSize,
		Factory: func() (*pipelines.ChunkingPipeline, error) {
			pipeline, bt, err := pipelines.LoadChunkingPipeline(
				cfg.ModelPath,
				sessionManager,
				cfg.ModelBackends,
				pipelines.WithChunkingThreshold(chunkerCfg.Threshold),
				pipelines.WithChunkingTargetTokens(chunkerCfg.TargetTokens),
			)
			if err != nil {
				return nil, err
			}
			backendUsed = bt
			return pipeline, nil
		},
		Close: func(p *pipelines.ChunkingPipeline) error {
			if p != nil {
				return p.Close()
			}
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		logger.Error("Failed to create chunking pipeline pool", zap.Error(err))
		return nil, "", fmt.Errorf("creating chunking pipeline pool: %w", err)
	}

	logger.Info("Successfully created pooled chunker pipelines",
		zap.Int("count", poolSize),
		zap.String("backend", string(backendUsed)))

	return &PooledChunker{
		pool:        lazyPool,
		config:      chunkerCfg,
		logger:      logger,
		backendType: backendUsed,
	}, backendUsed, nil
}

// BackendType returns the backend type used by this chunker
func (p *PooledChunker) BackendType() backends.BackendType {
	return p.backendType
}

// Chunk splits text using neural token classification.
// Thread-safe: uses pool to limit concurrent pipeline access.
// Note: per-request options (opts) are currently ignored; pipeline uses config from creation time.
func (p *PooledChunker) Chunk(ctx context.Context, text string, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	if text == "" {
		p.logger.Debug("Chunk called with empty text")
		return nil, nil
	}

	// Acquire a pipeline from the pool (blocks if all pipelines busy)
	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring pipeline slot: %w", err)
	}
	defer p.pool.Release()

	textLen := len(text)
	textPreview := text
	if textLen > 100 {
		textPreview = text[:100] + "..."
	}

	p.logger.Debug("Starting chunking",
		zap.Int("pipelineIndex", idx),
		zap.Int("text_length", textLen),
		zap.String("text_preview", textPreview))

	// Delegate to ChunkingPipeline.Chunk
	pipelineChunks, err := pipeline.Chunk(ctx, text)
	if err != nil {
		p.logger.Error("Chunking failed",
			zap.Int("pipelineIndex", idx),
			zap.Error(err))
		return nil, fmt.Errorf("chunking text: %w", err)
	}

	// Convert pipelines.Chunk to chunking.Chunk
	result := make([]chunking.Chunk, len(pipelineChunks))
	for i, c := range pipelineChunks {
		result[i] = chunking.NewTextChunk(
			uint32(c.Index),
			c.Text,
			c.Start,
			c.End,
		)
	}

	// Enforce max chunks limit if configured
	if p.config.MaxChunks > 0 && len(result) > p.config.MaxChunks {
		p.logger.Debug("Limiting chunks",
			zap.Int("from", len(result)),
			zap.Int("to", p.config.MaxChunks))
		result = result[:p.config.MaxChunks]
	}

	p.logger.Info("Chunking completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_chunks", len(result)),
		zap.Int("text_length", textLen))

	return result, nil
}

// Close releases resources.
func (p *PooledChunker) Close() error {
	return p.pool.Close()
}
