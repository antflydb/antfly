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

package pipelines

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sort"

	"github.com/ajroetker/go-highway/hwy/contrib/activation"
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"
)

// SparseEmbeddingPipelineConfig holds configuration for a SparseEmbeddingPipeline.
type SparseEmbeddingPipelineConfig struct {
	// MaxLength is the maximum sequence length for tokenization.
	MaxLength int

	// TopK is the maximum number of non-zero entries per sparse vector.
	TopK int

	// MinWeight is the minimum weight threshold for sparse entries.
	MinWeight float32
}

// DefaultSparseEmbeddingPipelineConfig returns sensible defaults for SPLADE-style sparse embedding.
func DefaultSparseEmbeddingPipelineConfig() *SparseEmbeddingPipelineConfig {
	return &SparseEmbeddingPipelineConfig{
		MaxLength: 512,
		TopK:      256,
		MinWeight: 0.0,
	}
}

// SparseEmbeddingPipeline generates sparse (SPLADE-style) embeddings.
// It shares tokenization and model inference with EmbeddingPipeline but
// applies SPLADE-specific post-processing: softplus activation followed
// by sparsification (top-k selection with minimum weight threshold).
type SparseEmbeddingPipeline struct {
	Model     backends.Model
	Tokenizer tokenizers.Tokenizer
	Config    *SparseEmbeddingPipelineConfig
}

// NewSparseEmbeddingPipeline creates a new SparseEmbeddingPipeline.
func NewSparseEmbeddingPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	config *SparseEmbeddingPipelineConfig,
) *SparseEmbeddingPipeline {
	if config == nil {
		config = DefaultSparseEmbeddingPipelineConfig()
	}
	return &SparseEmbeddingPipeline{
		Model:     model,
		Tokenizer: tokenizer,
		Config:    config,
	}
}

// Embed generates sparse embeddings for a batch of text strings.
func (p *SparseEmbeddingPipeline) Embed(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	inputs := TokenizeTexts(p.Tokenizer, texts, p.Config.MaxLength)
	return p.EmbedBatch(ctx, inputs)
}

// EmbedBatch generates sparse embeddings from pre-tokenized inputs.
// SPLADE post-processing:
//  1. Forward pass to get logits or hidden states
//  2. If output.Logits → use directly [batch, vocab]
//     Else if output.LastHiddenState → max-pool over sequence dim [batch, seq, vocab] → [batch, vocab]
//  3. Apply softplus activation: log(1+exp(x))
//  4. Threshold at MinWeight, take top-k by value
func (p *SparseEmbeddingPipeline) EmbedBatch(ctx context.Context, inputs *backends.ModelInputs) ([]embeddings.SparseVector, error) {
	output, err := p.Model.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("forward pass: %w", err)
	}

	// Get [batch, vocab] scores
	var scores [][]float32

	if len(output.Logits) > 0 {
		// Model directly outputs logits [batch, vocab] (e.g., SPLADE with MLM head)
		scores = output.Logits
	} else if len(output.LastHiddenState) > 0 {
		// Max-pool over sequence dimension, respecting attention mask
		scores = maxPoolOverSequence(output.LastHiddenState, inputs.AttentionMask)
	} else {
		return nil, fmt.Errorf("model output contains neither logits nor hidden states")
	}

	// Apply softplus and sparsify
	results := make([]embeddings.SparseVector, len(scores))
	for i, row := range scores {
		results[i] = applySoftplusAndSparsify(row, p.Config.TopK, p.Config.MinWeight)
	}

	return results, nil
}

// Close releases resources held by the pipeline.
func (p *SparseEmbeddingPipeline) Close() error {
	return p.Model.Close()
}

// maxPoolOverSequence computes element-wise max across the sequence dimension,
// masking out padding positions. Input shape: [batch, seq, hidden], output: [batch, hidden].
func maxPoolOverSequence(hiddenStates [][][]float32, attentionMask [][]int32) [][]float32 {
	batchSize := len(hiddenStates)
	results := make([][]float32, batchSize)

	for b := range batchSize {
		seqLen := len(hiddenStates[b])
		if seqLen == 0 {
			results[b] = nil
			continue
		}
		hiddenDim := len(hiddenStates[b][0])
		pooled := make([]float32, hiddenDim)

		// Initialize to -inf
		for d := range pooled {
			pooled[d] = float32(math.Inf(-1))
		}

		for s := range seqLen {
			// Skip masked (padding) positions
			if attentionMask != nil && b < len(attentionMask) && s < len(attentionMask[b]) && attentionMask[b][s] == 0 {
				continue
			}
			for d := range hiddenDim {
				if hiddenStates[b][s][d] > pooled[d] {
					pooled[d] = hiddenStates[b][s][d]
				}
			}
		}

		// Replace -inf with 0 for positions where no unmasked token existed
		for d := range pooled {
			if math.IsInf(float64(pooled[d]), -1) {
				pooled[d] = 0
			}
		}

		results[b] = pooled
	}

	return results
}

// applySoftplusAndSparsify applies SPLADE activation and extracts top-k sparse entries.
// Softplus: log(1 + exp(x)) is always >= 0, so no separate ReLU needed.
func applySoftplusAndSparsify(row []float32, topK int, minWeight float32) embeddings.SparseVector {
	if len(row) == 0 {
		return embeddings.SparseVector{}
	}

	// Apply softplus in-place using SIMD-accelerated implementation
	activated := make([]float32, len(row))
	activation.Softplus(row, activated)

	// Collect entries above minimum weight
	type entry struct {
		index uint32
		value float32
	}
	var entries []entry
	for i, v := range activated {
		if v > minWeight {
			entries = append(entries, entry{index: uint32(i), value: v})
		}
	}

	// Sort by value descending for top-k selection
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].value > entries[j].value
	})

	// Take top-k
	if len(entries) > topK {
		entries = entries[:topK]
	}

	if len(entries) == 0 {
		return embeddings.SparseVector{}
	}

	// Sort by index ascending for the final output
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].index < entries[j].index
	})

	// Build sparse vector
	indices := make([]uint32, len(entries))
	values := make([]float32, len(entries))
	for i, e := range entries {
		indices[i] = e.index
		values[i] = e.value
	}

	return embeddings.SparseVector{
		Indices: indices,
		Values:  values,
	}
}

// ============================================================================
// Loader Functions
// ============================================================================

// SparseEmbeddingLoaderOption configures sparse embedding pipeline loading.
type SparseEmbeddingLoaderOption func(*sparseEmbeddingLoaderConfig)

type sparseEmbeddingLoaderConfig struct {
	maxLength int
	topK      int
	minWeight float32
}

// WithSparseMaxLength sets the maximum sequence length.
func WithSparseMaxLength(maxLength int) SparseEmbeddingLoaderOption {
	return func(c *sparseEmbeddingLoaderConfig) {
		c.maxLength = maxLength
	}
}

// WithSparseTopK sets the maximum number of non-zero entries.
func WithSparseTopK(topK int) SparseEmbeddingLoaderOption {
	return func(c *sparseEmbeddingLoaderConfig) {
		c.topK = topK
	}
}

// WithSparseMinWeight sets the minimum weight threshold.
func WithSparseMinWeight(minWeight float32) SparseEmbeddingLoaderOption {
	return func(c *sparseEmbeddingLoaderConfig) {
		c.minWeight = minWeight
	}
}

// LoadSparseEmbeddingPipeline loads a sparse embedding pipeline from a model directory.
// The model should be a SPLADE-style model with MLM head output.
func LoadSparseEmbeddingPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...SparseEmbeddingLoaderOption,
) (*SparseEmbeddingPipeline, backends.BackendType, error) {
	loaderCfg := &sparseEmbeddingLoaderConfig{}
	for _, opt := range opts {
		opt(loaderCfg)
	}

	// Load model configuration (reuse embedding model config loader)
	config, err := LoadEmbeddingModelConfig(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading model config: %w", err)
	}

	if !config.HasTextEncoder() {
		return nil, "", fmt.Errorf("sparse embedding model at %s does not have a text encoder", modelPath)
	}

	// Get a loader for the model
	loader, backendType, err := sessionManager.GetLoaderForModel(modelBackends)
	if err != nil {
		return nil, "", fmt.Errorf("getting model loader: %w", err)
	}

	// Load tokenizer
	tokenizer, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading tokenizer: %w", err)
	}

	// Load model
	onnxRelPath, err := filepath.Rel(modelPath, config.TextEncoderFile)
	if err != nil {
		onnxRelPath = filepath.Base(config.TextEncoderFile)
	}
	model, err := loader.Load(modelPath, backends.WithONNXFile(onnxRelPath))
	if err != nil {
		return nil, "", fmt.Errorf("loading model: %w", err)
	}

	// Build pipeline config
	pipelineConfig := &SparseEmbeddingPipelineConfig{
		MaxLength: FirstNonZero(loaderCfg.maxLength, config.MaxTextLength, 512),
		TopK:      loaderCfg.topK,
		MinWeight: loaderCfg.minWeight,
	}
	if pipelineConfig.TopK <= 0 {
		pipelineConfig.TopK = 256
	}

	pipeline := NewSparseEmbeddingPipeline(model, tokenizer, pipelineConfig)
	return pipeline, backendType, nil
}
