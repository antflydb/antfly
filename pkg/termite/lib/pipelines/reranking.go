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
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// ============================================================================
// Config Types and Loading
// ============================================================================

// RerankingModelConfig holds parsed configuration for a reranking (cross-encoder) model.
type RerankingModelConfig struct {
	// Path to the model directory
	ModelPath string

	// ModelFile is the ONNX file for the cross-encoder model
	ModelFile string

	// NumLabels is the number of output classes (1 for regression, 2 for binary classification)
	NumLabels int

	// ModelType is the model architecture type (bert, roberta, deberta, etc.)
	ModelType string

	// MaxTextLength is the maximum text sequence length from max_position_embeddings
	MaxTextLength int
}

// LoadRerankingModelConfig loads and parses configuration for a reranking model.
func LoadRerankingModelConfig(modelPath string) (*RerankingModelConfig, error) {
	config := &RerankingModelConfig{
		ModelPath: modelPath,
	}

	// Detect model file
	config.ModelFile = FindONNXFile(modelPath, []string{
		"model.onnx",
		"model_quantized.onnx",
		"cross_encoder.onnx",
		"reranker.onnx",
	})

	if config.ModelFile == "" {
		return nil, fmt.Errorf("no ONNX model file found in %s", modelPath)
	}

	// Load configuration from config.json
	rawConfig, err := loadRawRerankingConfig(modelPath)
	if err != nil {
		// Config is optional for some models
		rawConfig = &rawRerankingConfig{
			NumLabels: 1, // Default for cross-encoder scoring
		}
	}

	// Determine model type
	config.ModelType = rawConfig.ModelType
	if config.ModelType == "" {
		config.ModelType = detectRerankingModelType(modelPath)
	}

	// Extract number of labels
	config.NumLabels = FirstNonZero(rawConfig.NumLabels, 1)

	// Extract max text sequence length
	config.MaxTextLength = rawConfig.MaxPositionEmbeddings

	return config, nil
}

// IsRerankingModel checks if a model path contains a reranking/cross-encoder model.
// Returns true if the model appears to be a cross-encoder based on config and files.
func IsRerankingModel(path string) bool {
	// Check for reranker-specific indicators in path
	lowerPath := strings.ToLower(path)
	if strings.Contains(lowerPath, "rerank") || strings.Contains(lowerPath, "cross-encoder") {
		return true
	}

	// Check config.json for classification head
	rawConfig, err := loadRawRerankingConfig(path)
	if err != nil {
		return false
	}

	// Cross-encoders typically have 1 or 2 labels
	if rawConfig.NumLabels == 1 || rawConfig.NumLabels == 2 {
		// Also check if it's not a typical embedding model
		if rawConfig.ModelType != "clip" && rawConfig.ModelType != "siglip" {
			return true
		}
	}

	return false
}

// rawRerankingConfig represents config.json for reranking models.
type rawRerankingConfig struct {
	ModelType             string `json:"model_type"`
	NumLabels             int    `json:"num_labels"`
	HiddenSize            int    `json:"hidden_size"`
	MaxPositionEmbeddings int    `json:"max_position_embeddings"`
}

// loadRawRerankingConfig loads config.json for reranking models.
func loadRawRerankingConfig(modelPath string) (*rawRerankingConfig, error) {
	configPath := filepath.Join(modelPath, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config.json: %w", err)
	}

	var config rawRerankingConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config.json: %w", err)
	}

	return &config, nil
}

// detectRerankingModelType attempts to detect the model type from the path.
func detectRerankingModelType(modelPath string) string {
	lowerPath := strings.ToLower(modelPath)
	switch {
	case strings.Contains(lowerPath, "deberta"):
		return "deberta"
	case strings.Contains(lowerPath, "roberta"):
		return "roberta"
	case strings.Contains(lowerPath, "bert"):
		return "bert"
	case strings.Contains(lowerPath, "xlm"):
		return "xlm-roberta"
	case strings.Contains(lowerPath, "minilm"):
		return "minilm"
	default:
		return "cross_encoder"
	}
}

// ============================================================================
// Pipeline Struct and Methods
// ============================================================================

// Ensure RerankingPipeline implements backends.CrossEncoderModel
var _ backends.CrossEncoderModel = (*RerankingPipeline)(nil)

// RerankingPipeline wraps a cross-encoder model for computing relevance scores
// between query-document pairs.
type RerankingPipeline struct {
	// Model performs inference on tokenized inputs.
	Model backends.Model

	// Tokenizer handles text-to-token conversion.
	Tokenizer tokenizers.Tokenizer

	// BasePipeline handles tokenization with pair encoding.
	BasePipeline *Pipeline

	// Config holds pipeline configuration.
	Config *RerankingPipelineConfig
}

// RerankingPipelineConfig holds configuration for a RerankingPipeline.
type RerankingPipelineConfig struct {
	// MaxLength is the maximum sequence length.
	MaxLength int

	// AddSpecialTokens controls whether to add [CLS], [SEP], etc.
	AddSpecialTokens bool

	// NumLabels is the number of output classes.
	// 1 = regression (score output directly)
	// 2 = binary classification (use softmax and take positive class)
	NumLabels int
}

// DefaultRerankingPipelineConfig returns sensible defaults for reranking.
func DefaultRerankingPipelineConfig() *RerankingPipelineConfig {
	return &RerankingPipelineConfig{
		MaxLength:        512,
		AddSpecialTokens: true,
		NumLabels:        1, // Most cross-encoders output a single score
	}
}

// NewRerankingPipeline creates a new RerankingPipeline.
func NewRerankingPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	config *RerankingPipelineConfig,
) *RerankingPipeline {
	if config == nil {
		config = DefaultRerankingPipelineConfig()
	}

	// Create base pipeline for pair encoding
	basePipeline := New(tokenizer, model,
		WithMaxLength(config.MaxLength),
		WithAddSpecialTokens(config.AddSpecialTokens),
		WithTruncation(TruncationLongestFirst),
		WithPadding(PaddingLongest),
	)

	return &RerankingPipeline{
		Model:        model,
		Tokenizer:    tokenizer,
		BasePipeline: basePipeline,
		Config:       config,
	}
}

// Rerank computes relevance scores for a query against multiple documents.
// Returns one score per document, higher scores indicate more relevance.
func (p *RerankingPipeline) Rerank(ctx context.Context, query string, documents []string) ([]float32, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	// Build pairs
	pairs := make([][2]string, len(documents))
	for i, doc := range documents {
		pairs[i] = [2]string{query, doc}
	}

	return p.RerankPairs(ctx, pairs)
}

// RerankPairs computes relevance scores for batches of query-document pairs.
// Returns one score per pair, higher scores indicate more relevance.
func (p *RerankingPipeline) RerankPairs(ctx context.Context, pairs [][2]string) ([]float32, error) {
	if len(pairs) == 0 {
		return nil, nil
	}

	// Encode pairs using the base pipeline
	encoded, err := p.BasePipeline.EncodePairs(pairs)
	if err != nil {
		return nil, fmt.Errorf("encoding pairs: %w", err)
	}

	// Create model inputs
	inputs := &backends.ModelInputs{
		InputIDs:      encoded.InputIDs,
		AttentionMask: encoded.AttentionMask,
		TokenTypeIDs:  encoded.TokenTypeIDs,
	}

	// Run model forward pass
	output, err := p.Model.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("forward pass: %w", err)
	}

	// Extract scores from logits
	scores, err := p.extractScores(output.Logits)
	if err != nil {
		return nil, fmt.Errorf("extracting scores: %w", err)
	}

	return scores, nil
}

// Score computes similarity scores for text pairs (implements backends.CrossEncoderModel).
// Inputs should contain concatenated query-document pairs.
// Returns scores with shape [batch].
func (p *RerankingPipeline) Score(ctx context.Context, inputs *backends.ModelInputs) ([]float32, error) {
	// Run model forward pass
	output, err := p.Model.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("forward pass: %w", err)
	}

	// Extract scores from logits
	return p.extractScores(output.Logits)
}

// extractScores converts model logits to relevance scores.
// For [batch, 1]: use the logit directly as score
// For [batch, 2]: use softmax and take the positive class probability
func (p *RerankingPipeline) extractScores(logits [][]float32) ([]float32, error) {
	if len(logits) == 0 {
		return nil, nil
	}

	batchSize := len(logits)
	numLabels := len(logits[0])

	scores := make([]float32, batchSize)

	switch numLabels {
	case 1:
		// Single logit output - use directly as score
		for i := range logits {
			scores[i] = logits[i][0]
		}

	case 2:
		// Binary classification - apply softmax and take positive class (index 1)
		for i := range logits {
			scores[i] = softmaxScore(logits[i])
		}

	default:
		// For other cases, take the first logit (or could sum/average)
		for i := range logits {
			if len(logits[i]) > 0 {
				scores[i] = logits[i][0]
			}
		}
	}

	return scores, nil
}

// softmaxScore computes softmax and returns the probability of the positive class (index 1).
func softmaxScore(logits []float32) float32 {
	if len(logits) < 2 {
		if len(logits) == 1 {
			return logits[0]
		}
		return 0
	}

	// Compute softmax with numerical stability (subtract max)
	maxLogit := logits[0]
	for _, l := range logits[1:] {
		if l > maxLogit {
			maxLogit = l
		}
	}

	var sum float64
	for _, l := range logits {
		sum += math.Exp(float64(l - maxLogit))
	}

	// Return probability of positive class (index 1)
	return float32(math.Exp(float64(logits[1]-maxLogit)) / sum)
}

// Forward runs inference on the given inputs and returns the model outputs.
// Implements backends.Model.
func (p *RerankingPipeline) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	return p.Model.Forward(ctx, inputs)
}

// Close releases resources held by the pipeline.
// Implements backends.Model.
func (p *RerankingPipeline) Close() error {
	return p.Model.Close()
}

// Name returns the model name for logging and debugging.
// Implements backends.Model.
func (p *RerankingPipeline) Name() string {
	return p.Model.Name()
}

// Backend returns the backend type this model uses.
// Implements backends.Model.
func (p *RerankingPipeline) Backend() backends.BackendType {
	return p.Model.Backend()
}

// ============================================================================
// Loader Functions
// ============================================================================

// RerankingLoaderOption configures reranking pipeline loading.
type RerankingLoaderOption func(*rerankingLoaderConfig)

type rerankingLoaderConfig struct {
	maxLength int
	numLabels int
	quantized bool
}

// WithRerankingMaxLength sets the maximum sequence length for reranking.
func WithRerankingMaxLength(maxLength int) RerankingLoaderOption {
	return func(c *rerankingLoaderConfig) {
		c.maxLength = maxLength
	}
}

// WithRerankingNumLabels sets the number of output labels.
func WithRerankingNumLabels(numLabels int) RerankingLoaderOption {
	return func(c *rerankingLoaderConfig) {
		c.numLabels = numLabels
	}
}

// WithRerankingQuantized uses quantized model files if available.
func WithRerankingQuantized(quantized bool) RerankingLoaderOption {
	return func(c *rerankingLoaderConfig) {
		c.quantized = quantized
	}
}

// LoadRerankingPipeline loads a reranking pipeline from a model directory.
// Returns the pipeline and the backend type that was used.
func LoadRerankingPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...RerankingLoaderOption,
) (*RerankingPipeline, backends.BackendType, error) {
	// Apply options
	loaderCfg := &rerankingLoaderConfig{}
	for _, opt := range opts {
		opt(loaderCfg)
	}

	// Load model configuration
	config, err := LoadRerankingModelConfig(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading reranking config: %w", err)
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
	model, err := loader.Load(modelPath, backends.WithONNXFile(filepath.Base(config.ModelFile)))
	if err != nil {
		return nil, "", fmt.Errorf("loading model: %w", err)
	}

	// Build pipeline config
	pipelineConfig := &RerankingPipelineConfig{
		MaxLength:        FirstNonZero(loaderCfg.maxLength, config.MaxTextLength, 512),
		AddSpecialTokens: true,
		NumLabels:        FirstNonZero(loaderCfg.numLabels, config.NumLabels, 1),
	}

	pipeline := NewRerankingPipeline(model, tokenizer, pipelineConfig)

	return pipeline, backendType, nil
}
