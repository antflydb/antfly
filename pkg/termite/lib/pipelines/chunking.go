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
	"sort"
	"strconv"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// ============================================================================
// Config Types and Loading
// ============================================================================

// ChunkingModelConfig holds parsed configuration for a chunking model.
// Chunking models are typically token classification models that identify
// semantic boundaries (sentence boundaries, paragraph breaks, topic shifts).
type ChunkingModelConfig struct {
	// Path to the model directory
	ModelPath string

	// ModelFile is the ONNX file for the token classification model
	ModelFile string

	// NumLabels is the number of output classes (e.g., 3 for O/B-SEP/I-SEP)
	NumLabels int

	// Labels maps label indices to names (e.g., {0: "O", 1: "B-SEP", 2: "I-SEP"})
	Labels map[int]string

	// ModelType is the model architecture type (bert, roberta, deberta, etc.)
	ModelType string

	// MaxTextLength is the maximum text sequence length from max_position_embeddings
	MaxTextLength int
}

// LoadChunkingModelConfig loads and parses configuration for a chunking model.
func LoadChunkingModelConfig(modelPath string) (*ChunkingModelConfig, error) {
	config := &ChunkingModelConfig{
		ModelPath: modelPath,
	}

	// Detect model file
	config.ModelFile = FindONNXFile(modelPath, []string{
		"model.onnx",
		"model_quantized.onnx",
		"chunker.onnx",
		"sentence_boundary.onnx",
	})

	if config.ModelFile == "" {
		return nil, fmt.Errorf("no ONNX model file found in %s", modelPath)
	}

	// Load configuration from config.json
	rawConfig, err := loadRawChunkingConfig(modelPath)
	if err != nil {
		// Config is optional for some models - use defaults
		rawConfig = &rawChunkingConfig{
			NumLabels: 3, // Default for O/B-SEP/I-SEP
			ID2Label: map[string]string{
				"0": "O",
				"1": "B-SEP",
				"2": "I-SEP",
			},
		}
	}

	// Determine model type
	config.ModelType = rawConfig.ModelType
	if config.ModelType == "" {
		config.ModelType = detectChunkingModelType(modelPath)
	}

	// Extract number of labels
	config.NumLabels = FirstNonZero(rawConfig.NumLabels, 3)

	// Extract max text sequence length
	config.MaxTextLength = rawConfig.MaxPositionEmbeddings

	// Build labels map
	config.Labels = make(map[int]string)
	for idStr, label := range rawConfig.ID2Label {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		config.Labels[id] = label
	}

	// Ensure we have default labels if none were loaded
	if len(config.Labels) == 0 {
		config.Labels = map[int]string{
			0: "O",
			1: "B-SEP",
			2: "I-SEP",
		}
	}

	return config, nil
}

// IsChunkingModel checks if a model path contains a chunking/sentence boundary model.
// Returns true if the model appears to be a token classifier for chunking.
func IsChunkingModel(path string) bool {
	// Check for chunker-specific indicators in path
	lowerPath := strings.ToLower(path)
	if strings.Contains(lowerPath, "chunk") ||
		strings.Contains(lowerPath, "sentence-boundary") ||
		strings.Contains(lowerPath, "segmentation") {
		return true
	}

	// Check config.json for token classification with separator labels
	rawConfig, err := loadRawChunkingConfig(path)
	if err != nil {
		return false
	}

	// Look for separator-related labels
	for _, label := range rawConfig.ID2Label {
		labelLower := strings.ToLower(label)
		if strings.Contains(labelLower, "sep") ||
			strings.Contains(labelLower, "separator") ||
			strings.Contains(labelLower, "boundary") {
			return true
		}
	}

	return false
}

// rawChunkingConfig represents config.json for chunking models.
type rawChunkingConfig struct {
	ModelType             string            `json:"model_type"`
	NumLabels             int               `json:"num_labels"`
	HiddenSize            int               `json:"hidden_size"`
	MaxPositionEmbeddings int               `json:"max_position_embeddings"`
	ID2Label              map[string]string `json:"id2label"`
	Label2ID              map[string]int    `json:"label2id"`
}

// loadRawChunkingConfig loads config.json for chunking models.
func loadRawChunkingConfig(modelPath string) (*rawChunkingConfig, error) {
	configPath := filepath.Join(modelPath, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config.json: %w", err)
	}

	var config rawChunkingConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config.json: %w", err)
	}

	return &config, nil
}

// detectChunkingModelType attempts to detect the model type from the path.
func detectChunkingModelType(modelPath string) string {
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
		return "token_classifier"
	}
}

// ============================================================================
// Chunk Result Type
// ============================================================================

// Chunk represents a segment of text identified by the chunking pipeline.
type Chunk struct {
	// Text is the content of this chunk.
	Text string

	// Start is the byte offset of the chunk start in the original text.
	Start int

	// End is the byte offset of the chunk end in the original text.
	End int

	// Index is the zero-based index of this chunk in the sequence.
	Index int
}

// ============================================================================
// Pipeline Struct and Methods
// ============================================================================

// Ensure ChunkingPipeline implements backends.TokenClassificationModel
var _ backends.TokenClassificationModel = (*ChunkingPipeline)(nil)

// ChunkingPipeline wraps a token classification model for identifying semantic
// boundaries in text. It can use sentence boundary detection models or
// semantic similarity-based approaches to split text into coherent chunks.
type ChunkingPipeline struct {
	// Model performs inference on tokenized inputs.
	Model backends.Model

	// Tokenizer handles text-to-token conversion.
	Tokenizer tokenizers.Tokenizer

	// BasePipeline handles tokenization with offsets.
	BasePipeline *Pipeline

	// Config holds pipeline configuration.
	Config *ChunkingPipelineConfig

	// Labels maps label indices to names.
	Labels map[int]string
}

// ChunkingPipelineConfig holds configuration for a ChunkingPipeline.
type ChunkingPipelineConfig struct {
	// MaxLength is the maximum sequence length.
	MaxLength int

	// MinChunkSize is the minimum number of characters per chunk.
	// Chunks smaller than this will be merged with adjacent chunks.
	MinChunkSize int

	// MaxChunkSize is the maximum number of characters per chunk.
	// Chunks larger than this will be split at natural boundaries.
	MaxChunkSize int

	// Overlap is the number of characters to overlap between chunks.
	Overlap int

	// Threshold is the minimum confidence for separator detection.
	// Predictions below this threshold are ignored.
	Threshold float32

	// TargetTokens is the target number of tokens per chunk for aggregation.
	// If > 0, small chunks will be combined until reaching this target.
	TargetTokens int

	// AddSpecialTokens controls whether to add [CLS], [SEP], etc.
	AddSpecialTokens bool
}

// DefaultChunkingPipelineConfig returns sensible defaults for chunking.
func DefaultChunkingPipelineConfig() *ChunkingPipelineConfig {
	return &ChunkingPipelineConfig{
		MaxLength:        512,
		MinChunkSize:     50,
		MaxChunkSize:     2000,
		Overlap:          0,
		Threshold:        0.5,
		TargetTokens:     0, // No aggregation by default
		AddSpecialTokens: true,
	}
}

// NewChunkingPipeline creates a new ChunkingPipeline.
func NewChunkingPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	labels map[int]string,
	config *ChunkingPipelineConfig,
) *ChunkingPipeline {
	if config == nil {
		config = DefaultChunkingPipelineConfig()
	}

	// Create base pipeline for tokenization with offsets
	basePipeline := New(tokenizer, model,
		WithMaxLength(config.MaxLength),
		WithAddSpecialTokens(config.AddSpecialTokens),
		WithTruncation(TruncationLongestFirst),
		WithPadding(PaddingLongest),
	)

	return &ChunkingPipeline{
		Model:        model,
		Tokenizer:    tokenizer,
		BasePipeline: basePipeline,
		Config:       config,
		Labels:       labels,
	}
}

// Chunk splits a single text into semantic chunks.
func (p *ChunkingPipeline) Chunk(ctx context.Context, text string) ([]Chunk, error) {
	results, err := p.ChunkBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

// ChunkBatch splits multiple texts into semantic chunks.
func (p *ChunkingPipeline) ChunkBatch(ctx context.Context, texts []string) ([][]Chunk, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	// Encode texts with character offsets
	encoded, err := p.BasePipeline.EncodeWithSpans(texts)
	if err != nil {
		return nil, fmt.Errorf("encoding texts: %w", err)
	}

	// Create model inputs
	inputs := &backends.ModelInputs{
		InputIDs:      encoded.InputIDs,
		AttentionMask: encoded.AttentionMask,
		TokenTypeIDs:  encoded.TokenTypeIDs,
	}

	// Run token classification
	logits, err := p.ClassifyTokens(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("classifying tokens: %w", err)
	}

	// Convert logits to chunks for each text
	results := make([][]Chunk, len(texts))
	for i, text := range texts {
		if len(logits) <= i || len(logits[i]) == 0 {
			// No predictions - return whole text as single chunk
			results[i] = []Chunk{{
				Text:  text,
				Start: 0,
				End:   len(text),
				Index: 0,
			}}
			continue
		}

		// Get offsets for this text (may be nil if tokenizer doesn't support it)
		var offsets []TokenSpan
		if encoded.Spans != nil && len(encoded.Spans) > i {
			offsets = encoded.Spans[i]
		}

		chunks := p.parseChunks(text, logits[i], offsets)
		results[i] = chunks
	}

	return results, nil
}

// ClassifyTokens returns per-token predictions.
// Implements backends.TokenClassificationModel.
func (p *ChunkingPipeline) ClassifyTokens(ctx context.Context, inputs *backends.ModelInputs) ([][][]float32, error) {
	// Try to use TokenClassificationModel interface if the underlying model supports it
	if tcModel, ok := p.Model.(backends.TokenClassificationModel); ok {
		return tcModel.ClassifyTokens(ctx, inputs)
	}

	// Fall back to Forward and extract logits
	output, err := p.Model.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("forward pass: %w", err)
	}

	// For token classification, output should be in LastHiddenState with shape [batch, seq, num_labels]
	// or we need to interpret the model output appropriately
	if len(output.LastHiddenState) == 0 {
		return nil, fmt.Errorf("model did not return token-level outputs")
	}

	return output.LastHiddenState, nil
}

// parseChunks converts token classification results into chunks.
func (p *ChunkingPipeline) parseChunks(text string, logits [][]float32, offsets []TokenSpan) []Chunk {
	if len(logits) == 0 || len(text) == 0 {
		return []Chunk{{
			Text:  text,
			Start: 0,
			End:   len(text),
			Index: 0,
		}}
	}

	// Find separator positions based on predicted labels
	separatorPositions := p.findSeparatorPositions(logits, offsets, len(text))

	// If no separators found, return whole text as single chunk
	if len(separatorPositions) == 0 {
		return []Chunk{{
			Text:  strings.TrimSpace(text),
			Start: 0,
			End:   len(text),
			Index: 0,
		}}
	}

	// Sort separator positions
	sort.Ints(separatorPositions)

	// Create chunks by splitting at separator positions
	chunks := make([]Chunk, 0, len(separatorPositions)+1)
	startPos := 0

	for _, sepPos := range separatorPositions {
		if sepPos > startPos && sepPos <= len(text) {
			chunkText := strings.TrimSpace(text[startPos:sepPos])
			if len(chunkText) >= p.Config.MinChunkSize {
				chunks = append(chunks, Chunk{
					Text:  chunkText,
					Start: startPos,
					End:   sepPos,
					Index: len(chunks),
				})
				startPos = sepPos
			}
		}
	}

	// Add final chunk
	if startPos < len(text) {
		chunkText := strings.TrimSpace(text[startPos:])
		if chunkText != "" {
			// Merge with previous if too small
			if len(chunkText) < p.Config.MinChunkSize && len(chunks) > 0 {
				// Merge with last chunk
				lastIdx := len(chunks) - 1
				chunks[lastIdx].Text = strings.TrimSpace(text[chunks[lastIdx].Start:])
				chunks[lastIdx].End = len(text)
			} else {
				chunks = append(chunks, Chunk{
					Text:  chunkText,
					Start: startPos,
					End:   len(text),
					Index: len(chunks),
				})
			}
		}
	}

	// Ensure we have at least one chunk
	if len(chunks) == 0 {
		chunks = append(chunks, Chunk{
			Text:  strings.TrimSpace(text),
			Start: 0,
			End:   len(text),
			Index: 0,
		})
	}

	// Apply target tokens aggregation if configured
	if p.Config.TargetTokens > 0 && len(chunks) > 1 {
		chunks = p.aggregateByTargetTokens(text, chunks)
	}

	return chunks
}

// findSeparatorPositions identifies character positions where separators occur.
func (p *ChunkingPipeline) findSeparatorPositions(logits [][]float32, offsets []TokenSpan, textLen int) []int {
	var positions []int

	for tokenIdx, tokenLogits := range logits {
		// Get predicted label
		labelIdx := argmaxFloat32(tokenLogits)
		if labelIdx >= len(p.Labels) {
			continue
		}

		label := p.Labels[labelIdx]

		// Check if this is a separator label
		isSeparator := p.isSeparatorLabel(label)
		if !isSeparator {
			continue
		}

		// Apply confidence threshold using softmax probability
		prob := softmaxProb(tokenLogits, labelIdx)
		if prob < p.Config.Threshold {
			continue
		}

		// Determine character position
		var charPos int
		if offsets != nil && tokenIdx < len(offsets) {
			charPos = offsets[tokenIdx].End
		} else {
			// Estimate position if offsets not available
			// This is a rough approximation
			charPos = (tokenIdx * textLen) / len(logits)
		}

		if charPos > 0 && charPos < textLen {
			positions = append(positions, charPos)
		}
	}

	return positions
}

// isSeparatorLabel checks if a label indicates a separator/boundary.
func (p *ChunkingPipeline) isSeparatorLabel(label string) bool {
	labelLower := strings.ToLower(label)
	return strings.Contains(labelLower, "sep") ||
		strings.Contains(labelLower, "separator") ||
		strings.Contains(labelLower, "boundary") ||
		strings.HasPrefix(labelLower, "b-") || // BIO format: B-SEP
		strings.HasPrefix(labelLower, "i-") // BIO format: I-SEP (continuation)
}

// aggregateByTargetTokens combines chunks until they reach the target token count.
func (p *ChunkingPipeline) aggregateByTargetTokens(originalText string, chunks []Chunk) []Chunk {
	if len(chunks) == 0 {
		return chunks
	}

	aggregated := make([]Chunk, 0)
	currentTexts := make([]string, 0)
	currentTokens := 0
	currentStartPos := chunks[0].Start
	var lastEndPos int

	for i, chunk := range chunks {
		chunkTokens := p.estimateTokens(chunk.Text)
		lastEndPos = chunk.End

		// If adding this chunk exceeds target, finalize current
		if currentTokens > 0 && currentTokens+chunkTokens > p.Config.TargetTokens {
			combinedText := strings.Join(currentTexts, "\n\n")
			aggregated = append(aggregated, Chunk{
				Text:  combinedText,
				Start: currentStartPos,
				End:   chunks[i-1].End,
				Index: len(aggregated),
			})

			currentTexts = []string{chunk.Text}
			currentTokens = chunkTokens
			currentStartPos = chunk.Start
		} else {
			currentTexts = append(currentTexts, chunk.Text)
			currentTokens += chunkTokens
		}
	}

	// Add remaining
	if len(currentTexts) > 0 {
		combinedText := strings.Join(currentTexts, "\n\n")
		aggregated = append(aggregated, Chunk{
			Text:  combinedText,
			Start: currentStartPos,
			End:   lastEndPos,
			Index: len(aggregated),
		})
	}

	return aggregated
}

// estimateTokens returns an approximate token count for text.
// Uses a rough heuristic of ~4 characters per token.
func (p *ChunkingPipeline) estimateTokens(text string) int {
	return len(text) / 4
}

// Forward runs inference on the given inputs and returns the model outputs.
// Implements backends.Model.
func (p *ChunkingPipeline) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	return p.Model.Forward(ctx, inputs)
}

// Close releases resources held by the pipeline.
// Implements backends.Model.
func (p *ChunkingPipeline) Close() error {
	return p.Model.Close()
}

// Name returns the model name for logging and debugging.
// Implements backends.Model.
func (p *ChunkingPipeline) Name() string {
	return p.Model.Name()
}

// Backend returns the backend type this model uses.
// Implements backends.Model.
func (p *ChunkingPipeline) Backend() backends.BackendType {
	return p.Model.Backend()
}

// ============================================================================
// Loader Functions
// ============================================================================

// ChunkingLoaderOption configures chunking pipeline loading.
type ChunkingLoaderOption func(*chunkingLoaderConfig)

type chunkingLoaderConfig struct {
	maxLength    int
	minChunkSize int
	maxChunkSize int
	overlap      int
	threshold    float32
	targetTokens int
	quantized    bool
}

// WithChunkingMaxLength sets the maximum sequence length for chunking.
func WithChunkingMaxLength(maxLength int) ChunkingLoaderOption {
	return func(c *chunkingLoaderConfig) {
		c.maxLength = maxLength
	}
}

// WithChunkingMinChunkSize sets the minimum chunk size.
func WithChunkingMinChunkSize(size int) ChunkingLoaderOption {
	return func(c *chunkingLoaderConfig) {
		c.minChunkSize = size
	}
}

// WithChunkingMaxChunkSize sets the maximum chunk size.
func WithChunkingMaxChunkSize(size int) ChunkingLoaderOption {
	return func(c *chunkingLoaderConfig) {
		c.maxChunkSize = size
	}
}

// WithChunkingOverlap sets the overlap between chunks.
func WithChunkingOverlap(overlap int) ChunkingLoaderOption {
	return func(c *chunkingLoaderConfig) {
		c.overlap = overlap
	}
}

// WithChunkingThreshold sets the confidence threshold for separator detection.
func WithChunkingThreshold(threshold float32) ChunkingLoaderOption {
	return func(c *chunkingLoaderConfig) {
		c.threshold = threshold
	}
}

// WithChunkingTargetTokens sets the target tokens for chunk aggregation.
func WithChunkingTargetTokens(tokens int) ChunkingLoaderOption {
	return func(c *chunkingLoaderConfig) {
		c.targetTokens = tokens
	}
}

// WithChunkingQuantized uses quantized model files if available.
func WithChunkingQuantized(quantized bool) ChunkingLoaderOption {
	return func(c *chunkingLoaderConfig) {
		c.quantized = quantized
	}
}

// LoadChunkingPipeline loads a chunking pipeline from a model directory.
// Returns the pipeline and the backend type that was used.
func LoadChunkingPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...ChunkingLoaderOption,
) (*ChunkingPipeline, backends.BackendType, error) {
	// Apply options
	loaderCfg := &chunkingLoaderConfig{}
	for _, opt := range opts {
		opt(loaderCfg)
	}

	// Load model configuration
	config, err := LoadChunkingModelConfig(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading chunking config: %w", err)
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
	pipelineConfig := &ChunkingPipelineConfig{
		MaxLength:        FirstNonZero(loaderCfg.maxLength, config.MaxTextLength, 512),
		MinChunkSize:     FirstNonZero(loaderCfg.minChunkSize, 50),
		MaxChunkSize:     FirstNonZero(loaderCfg.maxChunkSize, 2000),
		Overlap:          loaderCfg.overlap,
		Threshold:        loaderCfg.threshold,
		TargetTokens:     loaderCfg.targetTokens,
		AddSpecialTokens: true,
	}

	// Apply default threshold if not set
	if pipelineConfig.Threshold == 0 {
		pipelineConfig.Threshold = 0.5
	}

	pipeline := NewChunkingPipeline(model, tokenizer, config.Labels, pipelineConfig)

	return pipeline, backendType, nil
}

// ============================================================================
// Helper Functions
// ============================================================================

// argmaxFloat32 returns the index of the maximum value in the slice.
func argmaxFloat32(values []float32) int {
	if len(values) == 0 {
		return 0
	}
	maxIdx := 0
	maxVal := values[0]
	for i, v := range values[1:] {
		if v > maxVal {
			maxVal = v
			maxIdx = i + 1
		}
	}
	return maxIdx
}

// softmaxProb computes the softmax probability for a specific index.
func softmaxProb(logits []float32, idx int) float32 {
	if len(logits) == 0 || idx >= len(logits) {
		return 0
	}

	// Find max for numerical stability
	maxVal := logits[0]
	for _, v := range logits[1:] {
		if v > maxVal {
			maxVal = v
		}
	}

	// Compute softmax
	var sum float64
	for _, v := range logits {
		sum += math.Exp(float64(v - maxVal))
	}

	return float32(math.Exp(float64(logits[idx]-maxVal)) / sum)
}
