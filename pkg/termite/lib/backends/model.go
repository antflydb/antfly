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

package backends

import (
	"context"
	"fmt"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/gomlx/gomlx/pkg/core/tensors/bucketing"
)

// Model represents an inference model that can process inputs.
// All model types (encoder-only, generative, seq2seq, vision2seq) implement this interface.
// Different model types use different subsets of ModelInputs fields.
type Model interface {
	// Forward runs inference on the given inputs and returns the model outputs.
	// The context can be used for cancellation and timeout.
	//
	// For encoder-only models: uses InputIDs, AttentionMask
	// For generative models: uses InputIDs, PastKeyValues
	// For seq2seq encoder: uses InputIDs, AttentionMask (returns EncoderOutput)
	// For seq2seq/vision2seq decoder: uses InputIDs, EncoderOutput, PastKeyValues
	// For vision encoder: uses ImagePixels and dimensions (returns EncoderOutput)
	Forward(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error)

	// Close releases resources associated with the model.
	Close() error

	// Name returns the model name for logging and debugging.
	Name() string

	// Backend returns the backend type this model uses.
	Backend() BackendType
}

// DecoderConfigProvider is implemented by models that support decoding (generative, seq2seq, vision2seq).
// Use type assertion to access decoder configuration from a Model:
//
//	if provider, ok := model.(DecoderConfigProvider); ok {
//	    config := provider.DecoderConfig()
//	}
type DecoderConfigProvider interface {
	DecoderConfig() *DecoderConfig
}

// ImageConfigProvider is implemented by models that process images (vision2seq, vision encoders).
// Use type assertion to access image preprocessing configuration from a Model:
//
//	if provider, ok := model.(ImageConfigProvider); ok {
//	    config := provider.ImageConfig()
//	}
type ImageConfigProvider interface {
	ImageConfig() *ImageConfig
}

// InputInfoProvider is implemented by models that can report their input tensor shapes.
// Dimensions use -1 for dynamic axes and positive values for fixed axes.
// Use type assertion to inspect input shapes at runtime:
//
//	if provider, ok := model.(InputInfoProvider); ok {
//	    info := provider.InputInfo()
//	    // check dimensions to determine batching support, etc.
//	}
type InputInfoProvider interface {
	InputInfo() []InputOutputInfo
}

// FeatureExtractionModel is a model optimized for generating embeddings.
type FeatureExtractionModel interface {
	Model

	// EmbedBatch generates embeddings for a batch of tokenized inputs.
	// Returns embeddings with shape [batch, hidden].
	EmbedBatch(ctx context.Context, inputs *ModelInputs) ([][]float32, error)
}

// TokenClassificationModel is a model for token-level classification tasks (NER, chunking).
type TokenClassificationModel interface {
	Model

	// ClassifyTokens returns per-token predictions.
	// Returns logits with shape [batch, seq, num_labels].
	ClassifyTokens(ctx context.Context, inputs *ModelInputs) ([][][]float32, error)
}

// SequenceClassificationModel is a model for sequence classification tasks.
type SequenceClassificationModel interface {
	Model

	// ClassifySequence returns sequence-level predictions.
	// Returns logits with shape [batch, num_labels].
	ClassifySequence(ctx context.Context, inputs *ModelInputs) ([][]float32, error)
}

// CrossEncoderModel is a model for computing similarity scores between text pairs.
type CrossEncoderModel interface {
	Model

	// Score computes similarity scores for text pairs.
	// Inputs should contain concatenated query-document pairs.
	// Returns scores with shape [batch].
	Score(ctx context.Context, inputs *ModelInputs) ([]float32, error)
}

// VisionModel is a model that can process image inputs.
type VisionModel interface {
	Model

	// EmbedImage generates embeddings for images.
	// imageData should be preprocessed image tensors.
	EmbedImage(ctx context.Context, imageData []float32, width, height int) ([]float32, error)
}

// MultimodalModel is a model that can process both text and images.
type MultimodalModel interface {
	FeatureExtractionModel
	VisionModel
}

// ModelLoader loads models for a specific backend.
type ModelLoader interface {
	// Load loads a model from the given path with the specified options.
	// The returned Model can be type-asserted to specific interfaces
	// (FeatureExtractionModel, CrossEncoderModel, etc.) based on capabilities.
	Load(path string, opts ...LoadOption) (Model, error)

	// SupportsModel returns true if this loader can handle the model at the given path.
	SupportsModel(path string) bool

	// Backend returns the backend type this loader uses.
	Backend() BackendType
}

// LoadAs is a type-safe helper that loads a model and asserts it to the requested type.
// This provides compile-time type safety when loading models for specific purposes.
//
// Example:
//
//	embedder, err := backends.LoadAs[backends.FeatureExtractionModel](loader, modelPath)
//	if err != nil {
//	    return err
//	}
//	embeddings, err := embedder.EmbedBatch(ctx, inputs)
func LoadAs[T Model](loader ModelLoader, path string, opts ...LoadOption) (T, error) {
	var zero T
	model, err := loader.Load(path, opts...)
	if err != nil {
		return zero, err
	}
	typed, ok := model.(T)
	if !ok {
		return zero, fmt.Errorf("model at %s does not implement %T", path, zero)
	}
	return typed, nil
}

// ModelFormat specifies the format of model files.
type ModelFormat string

const (
	// ModelFormatAuto auto-detects the model format
	ModelFormatAuto ModelFormat = ""
	// ModelFormatONNX indicates ONNX model file format
	ModelFormatONNX ModelFormat = "onnx"
)

// GoMLXBackendType specifies which GoMLX backend to use for inference.
type GoMLXBackendType string

const (
	// GoMLXBackendAuto auto-detects the best available backend (xla if available, else simplego)
	GoMLXBackendAuto GoMLXBackendType = ""
	// GoMLXBackendSimpleGo uses pure Go inference (slow but always available)
	GoMLXBackendSimpleGo GoMLXBackendType = "go"
	// GoMLXBackendXLA uses XLA/PJRT for hardware acceleration (CUDA, TPU, optimized CPU)
	GoMLXBackendXLA GoMLXBackendType = "xla"
)

// LoadConfig holds configuration for model loading.
// Created via LoadOption functions.
type LoadConfig struct {
	// ONNXFilename specifies which ONNX file to load (e.g., "model.onnx")
	ONNXFilename string

	// ModelFormat specifies the model format (auto-detected if empty)
	ModelFormat ModelFormat

	// GoMLXBackend specifies which GoMLX backend to use (auto-detected if empty)
	GoMLXBackend GoMLXBackendType

	// MaxLength is the maximum sequence length for tokenization
	MaxLength int

	// Normalize indicates whether to L2-normalize embeddings
	Normalize bool

	// Pooling specifies the pooling strategy ("mean", "cls", "max", "none")
	Pooling string

	// TruncationStrategy specifies how to handle sequences longer than MaxLength
	TruncationStrategy string

	// PaddingStrategy specifies how to handle padding ("max_length", "longest", "none")
	PaddingStrategy string

	// GPUMode controls GPU acceleration
	GPUMode GPUMode

	// BatchSize is the inference batch size (0 = auto)
	BatchSize int

	// NumThreads is the number of inference threads (0 = auto)
	NumThreads int

	// BatchBucketing specifies the bucketing strategy for the batch dimension.
	// Input batch sizes are rounded up via this strategy to reduce JIT recompilation.
	// Only used by backends with JIT compilation (XLA, CoreML).
	// Nil uses backend defaults (Exponential(1.4) for JIT backends, None for Go).
	BatchBucketing bucketing.Strategy

	// SeqBucketing specifies the bucketing strategy for the sequence length dimension.
	// Input sequence lengths are rounded up via this strategy to reduce JIT recompilation.
	// Only used by backends with JIT compilation (XLA, CoreML).
	// Nil uses backend defaults (Exponential(1.4) for JIT backends, None for Go).
	SeqBucketing bucketing.Strategy

	// MaxCacheSize limits the number of cached compiled graphs.
	// 0 = use GoMLX default (32), -1 = unlimited.
	MaxCacheSize int

	// PreCompileMaxBatch is the maximum batch size for eager pre-compilation of
	// bucket shapes at model load time. 0 = use BatchSize if set, otherwise skip.
	// Only used by JIT backends (CoreML, XLA) with bucketing enabled.
	PreCompileMaxBatch int

	// PreCompileMaxSeq is the maximum sequence length for eager pre-compilation of
	// bucket shapes at model load time. 0 = use MaxLength.
	// Only used by JIT backends (CoreML, XLA) with bucketing enabled.
	PreCompileMaxSeq int
}

// DefaultLoadConfig returns a LoadConfig with sensible defaults.
func DefaultLoadConfig() *LoadConfig {
	return &LoadConfig{
		ONNXFilename:       "model.onnx",
		MaxLength:          512,
		Normalize:          true,
		Pooling:            "mean",
		TruncationStrategy: "longest_first",
		PaddingStrategy:    "longest",
		GPUMode:            GPUModeAuto,
	}
}

// LoadOption is a functional option for configuring model loading.
type LoadOption func(*LoadConfig)

// WithONNXFile sets the ONNX filename to load.
func WithONNXFile(filename string) LoadOption {
	return func(c *LoadConfig) {
		c.ONNXFilename = filename
	}
}

// WithMaxLength sets the maximum sequence length.
func WithMaxLength(length int) LoadOption {
	return func(c *LoadConfig) {
		c.MaxLength = length
	}
}

// WithNormalization enables or disables L2 normalization of embeddings.
func WithNormalization(normalize bool) LoadOption {
	return func(c *LoadConfig) {
		c.Normalize = normalize
	}
}

// WithPooling sets the pooling strategy.
func WithPooling(strategy string) LoadOption {
	return func(c *LoadConfig) {
		c.Pooling = strategy
	}
}

// WithTruncation sets the truncation strategy.
func WithTruncation(strategy string) LoadOption {
	return func(c *LoadConfig) {
		c.TruncationStrategy = strategy
	}
}

// WithPadding sets the padding strategy.
func WithPadding(strategy string) LoadOption {
	return func(c *LoadConfig) {
		c.PaddingStrategy = strategy
	}
}

// WithGPUMode sets the GPU acceleration mode.
func WithGPUMode(mode GPUMode) LoadOption {
	return func(c *LoadConfig) {
		c.GPUMode = mode
	}
}

// WithBatchSize sets the inference batch size.
func WithBatchSize(size int) LoadOption {
	return func(c *LoadConfig) {
		c.BatchSize = size
	}
}

// WithNumThreads sets the number of inference threads.
func WithNumThreads(threads int) LoadOption {
	return func(c *LoadConfig) {
		c.NumThreads = threads
	}
}

// WithModelFormat sets the model format explicitly.
// If not set, the format is auto-detected from the model files.
func WithModelFormat(format ModelFormat) LoadOption {
	return func(c *LoadConfig) {
		c.ModelFormat = format
	}
}

// WithGoMLXBackend sets the GoMLX backend to use.
// If not set, the best available backend is auto-detected.
func WithGoMLXBackend(backend GoMLXBackendType) LoadOption {
	return func(c *LoadConfig) {
		c.GoMLXBackend = backend
	}
}

// WithBatchBucketing sets the bucketing strategy for the batch dimension.
// Common strategies: bucketing.Exponential(1.4), bucketing.Pow2(), bucketing.Linear(8).
func WithBatchBucketing(strategy bucketing.Strategy) LoadOption {
	return func(c *LoadConfig) {
		c.BatchBucketing = strategy
	}
}

// WithSeqBucketing sets the bucketing strategy for the sequence length dimension.
// Common strategies: bucketing.Exponential(1.4), bucketing.Pow2(), bucketing.Linear(64).
func WithSeqBucketing(strategy bucketing.Strategy) LoadOption {
	return func(c *LoadConfig) {
		c.SeqBucketing = strategy
	}
}

// WithMaxCacheSize sets the maximum number of cached compiled graphs.
// 0 = use GoMLX default (32), -1 = unlimited.
func WithMaxCacheSize(size int) LoadOption {
	return func(c *LoadConfig) {
		c.MaxCacheSize = size
	}
}

// WithPreCompileBuckets configures eager pre-compilation of bucket shapes at
// model load time. maxBatch and maxSeq define the range of dimensions to
// enumerate. All unique (batchBucket, seqBucket) combinations within these
// bounds are pre-compiled. Use 0 to skip a dimension.
func WithPreCompileBuckets(maxBatch, maxSeq int) LoadOption {
	return func(c *LoadConfig) {
		c.PreCompileMaxBatch = maxBatch
		c.PreCompileMaxSeq = maxSeq
	}
}

// ApplyOptions applies LoadOptions to a LoadConfig.
func ApplyOptions(opts ...LoadOption) *LoadConfig {
	config := DefaultLoadConfig()
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// PoolHiddenStates applies pooling to convert [batch, seq, hidden] to [batch, hidden].
func PoolHiddenStates(hiddenStates [][][]float32, attentionMask [][]int32, pooling PoolingStrategy) [][]float32 {
	batchSize := len(hiddenStates)
	if batchSize == 0 {
		return nil
	}

	hiddenSize := len(hiddenStates[0][0])
	embeddings := make([][]float32, batchSize)

	switch pooling {
	case PoolingCLS:
		// Use [CLS] token (first token)
		for i := range batchSize {
			embeddings[i] = make([]float32, hiddenSize)
			copy(embeddings[i], hiddenStates[i][0])
		}

	case PoolingMax:
		// Max pooling over sequence
		for i := range batchSize {
			embeddings[i] = make([]float32, hiddenSize)
			for h := range hiddenSize {
				maxVal := float32(-1e9)
				for j := 0; j < len(hiddenStates[i]); j++ {
					if attentionMask[i][j] > 0 && hiddenStates[i][j][h] > maxVal {
						maxVal = hiddenStates[i][j][h]
					}
				}
				embeddings[i][h] = maxVal
			}
		}

	case PoolingMean, "":
		// Mean pooling (default)
		for i := range batchSize {
			embeddings[i] = make([]float32, hiddenSize)
			count := float32(0)
			for j := 0; j < len(hiddenStates[i]); j++ {
				if attentionMask[i][j] > 0 {
					for h := range hiddenSize {
						embeddings[i][h] += hiddenStates[i][j][h]
					}
					count++
				}
			}
			if count > 0 {
				for h := range hiddenSize {
					embeddings[i][h] /= count
				}
			}
		}

	default:
		// No pooling - return first token
		for i := range batchSize {
			embeddings[i] = make([]float32, hiddenSize)
			copy(embeddings[i], hiddenStates[i][0])
		}
	}

	return embeddings
}

// NormalizeL2 performs L2 normalization on a vector in-place using SIMD acceleration.
func NormalizeL2(v []float32) []float32 {
	vec.Normalize(v)
	return v
}

// NormalizeEmbeddings applies L2 normalization to all embeddings in a batch.
// Uses in-place SIMD normalization to avoid allocations.
func NormalizeEmbeddings(embeddings [][]float32) {
	for i := range embeddings {
		vec.Normalize(embeddings[i])
	}
}
