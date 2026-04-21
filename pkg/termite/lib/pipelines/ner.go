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
	"strconv"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// ============================================================================
// Config Types and Loading
// ============================================================================

// NERModelConfig holds parsed configuration for a Named Entity Recognition model.
type NERModelConfig struct {
	// Path to the model directory
	ModelPath string

	// ModelFile is the ONNX file for the token classification model
	ModelFile string

	// ID2Label maps label indices to label strings (e.g., {0: "O", 1: "B-PER", ...})
	ID2Label map[int]string

	// Label2ID maps label strings to indices
	Label2ID map[string]int

	// NumLabels is the number of entity labels
	NumLabels int

	// ModelType is the model architecture type (bert, roberta, etc.)
	ModelType string

	// MaxTextLength is the maximum text sequence length from max_position_embeddings
	MaxTextLength int
}

// LoadNERModelConfig loads and parses configuration for an NER model.
func LoadNERModelConfig(modelPath string) (*NERModelConfig, error) {
	config := &NERModelConfig{
		ModelPath: modelPath,
	}

	// Detect model file
	config.ModelFile = FindONNXFile(modelPath, []string{
		"model.onnx",
		"model_quantized.onnx",
		"ner.onnx",
		"token_classification.onnx",
	})

	if config.ModelFile == "" {
		return nil, fmt.Errorf("no ONNX model file found in %s", modelPath)
	}

	// Load configuration from config.json
	rawConfig, err := loadRawNERConfig(modelPath)
	if err != nil {
		// Config is optional - use defaults
		rawConfig = &rawNERConfig{
			ID2Label: map[string]string{
				"0": "O",
				"1": "B-PER",
				"2": "I-PER",
				"3": "B-ORG",
				"4": "I-ORG",
				"5": "B-LOC",
				"6": "I-LOC",
				"7": "B-MISC",
				"8": "I-MISC",
			},
		}
	}

	// Convert string keys to int keys for ID2Label
	config.ID2Label = make(map[int]string, len(rawConfig.ID2Label))
	config.Label2ID = make(map[string]int, len(rawConfig.ID2Label))
	for idStr, label := range rawConfig.ID2Label {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		config.ID2Label[id] = label
		config.Label2ID[label] = id
	}

	config.NumLabels = len(config.ID2Label)

	// Determine model type
	config.ModelType = rawConfig.ModelType
	if config.ModelType == "" {
		config.ModelType = detectNERModelType(modelPath)
	}

	// Extract max text sequence length
	config.MaxTextLength = rawConfig.MaxPositionEmbeddings

	return config, nil
}

// IsNERModel checks if a model path contains an NER/token classification model.
// Returns true if the model appears to be an NER model based on config and files.
func IsNERModel(path string) bool {
	// Check for NER-specific indicators in path
	lowerPath := strings.ToLower(path)
	if strings.Contains(lowerPath, "ner") || strings.Contains(lowerPath, "token-classification") {
		return true
	}

	// Check config.json for id2label with BIO labels
	rawConfig, err := loadRawNERConfig(path)
	if err != nil {
		return false
	}

	// Check if id2label contains BIO-style labels
	for _, label := range rawConfig.ID2Label {
		if strings.HasPrefix(label, "B-") || strings.HasPrefix(label, "I-") ||
			strings.HasPrefix(label, "E-") || strings.HasPrefix(label, "S-") {
			return true
		}
	}

	return false
}

// rawNERConfig represents config.json for NER models.
type rawNERConfig struct {
	ModelType             string            `json:"model_type"`
	MaxPositionEmbeddings int               `json:"max_position_embeddings"`
	ID2Label              map[string]string `json:"id2label"`
	Label2ID              map[string]int    `json:"label2id"`
}

// loadRawNERConfig loads config.json for NER models.
func loadRawNERConfig(modelPath string) (*rawNERConfig, error) {
	configPath := filepath.Join(modelPath, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config.json: %w", err)
	}

	var config rawNERConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config.json: %w", err)
	}

	if len(config.ID2Label) == 0 {
		return nil, fmt.Errorf("no id2label found in config.json")
	}

	return &config, nil
}

// detectNERModelType attempts to detect the model type from the path.
func detectNERModelType(modelPath string) string {
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
	case strings.Contains(lowerPath, "electra"):
		return "electra"
	default:
		return "token_classifier"
	}
}

// ============================================================================
// Pipeline Struct and Methods
// ============================================================================

// Ensure NERPipeline implements backends.TokenClassificationModel
var _ backends.TokenClassificationModel = (*NERPipeline)(nil)

// Entity is an alias for NEREntity so both pipeline types share a single entity representation.
type Entity = NEREntity

// AggregationStrategy defines how to aggregate tokens into entities.
type AggregationStrategy string

const (
	// AggregationNone returns one entity per token (no aggregation).
	AggregationNone AggregationStrategy = "none"
	// AggregationSimple merges adjacent tokens with the same entity type.
	AggregationSimple AggregationStrategy = "simple"
	// AggregationFirst uses the label of the first token for merged entities.
	AggregationFirst AggregationStrategy = "first"
	// AggregationAverage averages scores of merged tokens.
	AggregationAverage AggregationStrategy = "average"
	// AggregationMax uses the maximum score among merged tokens.
	AggregationMax AggregationStrategy = "max"
)

// NERPipelineConfig holds configuration for an NERPipeline.
type NERPipelineConfig struct {
	// MaxLength is the maximum sequence length.
	MaxLength int

	// AddSpecialTokens controls whether to add [CLS], [SEP], etc.
	AddSpecialTokens bool

	// AggregationStrategy defines how to merge tokens into entities.
	AggregationStrategy AggregationStrategy

	// IgnoreLabels is a set of labels to ignore (typically "O" for outside).
	IgnoreLabels map[string]bool
}

// DefaultNERPipelineConfig returns sensible defaults for NER.
func DefaultNERPipelineConfig() *NERPipelineConfig {
	return &NERPipelineConfig{
		MaxLength:           512,
		AddSpecialTokens:    true,
		AggregationStrategy: AggregationSimple,
		IgnoreLabels:        map[string]bool{"O": true, "": true},
	}
}

// NERPipeline wraps a token classification model for Named Entity Recognition.
type NERPipeline struct {
	// Model performs inference on tokenized inputs.
	Model backends.Model

	// Tokenizer handles text-to-token conversion.
	Tokenizer tokenizers.Tokenizer

	// BasePipeline handles tokenization with offset tracking.
	BasePipeline *Pipeline

	// Config holds pipeline configuration.
	Config *NERPipelineConfig

	// ModelConfig holds the model's label configuration.
	ModelConfig *NERModelConfig
}

// NewNERPipeline creates a new NERPipeline.
func NewNERPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	modelConfig *NERModelConfig,
	config *NERPipelineConfig,
) *NERPipeline {
	if config == nil {
		config = DefaultNERPipelineConfig()
	}

	// Create base pipeline for tokenization with offsets
	basePipeline := New(tokenizer, model,
		WithMaxLength(config.MaxLength),
		WithAddSpecialTokens(config.AddSpecialTokens),
		WithTruncation(TruncationLongestFirst),
		WithPadding(PaddingLongest),
	)

	return &NERPipeline{
		Model:        model,
		Tokenizer:    tokenizer,
		BasePipeline: basePipeline,
		Config:       config,
		ModelConfig:  modelConfig,
	}
}

// ExtractEntities extracts named entities from the given texts.
// Returns a slice of entities for each input text.
func (p *NERPipeline) ExtractEntities(ctx context.Context, texts []string) ([][]Entity, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	// Encode texts with offsets for accurate character position mapping
	batch, err := p.BasePipeline.EncodeWithSpans(texts)
	if err != nil {
		return nil, fmt.Errorf("encoding texts: %w", err)
	}

	// Create model inputs
	inputs := &backends.ModelInputs{
		InputIDs:      batch.InputIDs,
		AttentionMask: batch.AttentionMask,
		TokenTypeIDs:  batch.TokenTypeIDs,
	}

	// Get token-level predictions
	tokenLogits, err := p.ClassifyTokens(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("classifying tokens: %w", err)
	}

	// Convert predictions to entities for each text
	results := make([][]Entity, len(texts))
	for i := range texts {
		if i >= len(tokenLogits) {
			results[i] = nil
			continue
		}

		var offsets []TokenSpan
		if batch.Spans != nil && i < len(batch.Spans) {
			offsets = batch.Spans[i]
		}

		entities, err := p.parseEntities(texts[i], tokenLogits[i], offsets, batch.AttentionMask[i])
		if err != nil {
			return nil, fmt.Errorf("parsing entities for text %d: %w", i, err)
		}
		results[i] = entities
	}

	return results, nil
}

// ClassifyTokens returns per-token predictions (implements backends.TokenClassificationModel).
// Returns logits with shape [batch, seq, num_labels].
func (p *NERPipeline) ClassifyTokens(ctx context.Context, inputs *backends.ModelInputs) ([][][]float32, error) {
	// Check if underlying model implements TokenClassificationModel
	if tcModel, ok := p.Model.(backends.TokenClassificationModel); ok {
		return tcModel.ClassifyTokens(ctx, inputs)
	}

	// Fall back to Forward and interpret output
	output, err := p.Model.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("forward pass: %w", err)
	}

	// Check if we have LastHiddenState that could be token logits
	if len(output.LastHiddenState) > 0 {
		// LastHiddenState has shape [batch, seq, hidden]
		// For NER, we need logits with shape [batch, seq, num_labels]
		// This typically requires a classification head
		return nil, fmt.Errorf("model returns hidden states but does not implement TokenClassificationModel interface")
	}

	return nil, fmt.Errorf("model does not support token classification")
}

// parseEntities converts token-level logits to aggregated entities.
func (p *NERPipeline) parseEntities(
	text string,
	logits [][]float32,
	offsets []TokenSpan,
	attentionMask []int32,
) ([]Entity, error) {
	if len(logits) == 0 {
		return nil, nil
	}

	// Build list of token predictions
	preds := make([]tokenPred, 0, len(logits))

	for i, tokenLogits := range logits {
		// Skip padding tokens
		if i < len(attentionMask) && attentionMask[i] == 0 {
			continue
		}

		// Skip tokens without offset info
		if i >= len(offsets) {
			continue
		}

		// Skip special tokens (zero-length offsets at position > 0, or [CLS]/[SEP])
		offset := offsets[i]
		if offset.Start == 0 && offset.End == 0 && i > 0 {
			continue
		}

		// Get predicted label
		labelIdx := argmaxNER(tokenLogits)
		label := p.getLabel(labelIdx)
		score := softmaxScoreNER(tokenLogits, labelIdx)

		preds = append(preds, tokenPred{
			labelIdx: labelIdx,
			label:    label,
			score:    score,
			offset:   offset,
			valid:    true,
		})
	}

	// Aggregate tokens into entities based on strategy
	return p.aggregateTokens(text, preds)
}

// aggregateTokens merges token predictions into entities based on the aggregation strategy.
func (p *NERPipeline) aggregateTokens(text string, preds []tokenPred) ([]Entity, error) {
	if len(preds) == 0 {
		return nil, nil
	}

	entities := make([]Entity, 0)

	switch p.Config.AggregationStrategy {
	case AggregationNone:
		// Return one entity per non-O token
		for _, pred := range preds {
			if p.Config.IgnoreLabels[pred.label] {
				continue
			}
			entityText := safeSubstring(text, pred.offset.Start, pred.offset.End)
			entities = append(entities, Entity{
				Text:  entityText,
				Label: normalizeLabel(pred.label),
				Start: pred.offset.Start,
				End:   pred.offset.End,
				Score: pred.score,
			})
		}

	case AggregationSimple, AggregationFirst, AggregationAverage, AggregationMax, "":
		// Merge adjacent tokens with same entity type
		var current *Entity
		var currentScores []float32

		for _, pred := range preds {
			// Check if this is an "O" (outside) label
			if p.Config.IgnoreLabels[pred.label] {
				// Finish current entity
				if current != nil {
					current.Score = p.aggregateScores(currentScores)
					current.Text = safeSubstring(text, current.Start, current.End)
					entities = append(entities, *current)
					current = nil
					currentScores = nil
				}
				continue
			}

			entityType := getLabelType(pred.label)
			isBegin := isBIOBegin(pred.label)

			if current == nil || isBegin || getLabelType(current.Label) != entityType {
				// Start new entity
				if current != nil {
					current.Score = p.aggregateScores(currentScores)
					current.Text = safeSubstring(text, current.Start, current.End)
					entities = append(entities, *current)
				}
				current = &Entity{
					Label: normalizeLabel(pred.label),
					Start: pred.offset.Start,
					End:   pred.offset.End,
				}
				currentScores = []float32{pred.score}
			} else {
				// Continue current entity
				current.End = pred.offset.End
				currentScores = append(currentScores, pred.score)
			}
		}

		// Don't forget the last entity
		if current != nil {
			current.Score = p.aggregateScores(currentScores)
			current.Text = safeSubstring(text, current.Start, current.End)
			entities = append(entities, *current)
		}

	default:
		return nil, fmt.Errorf("unknown aggregation strategy: %s", p.Config.AggregationStrategy)
	}

	return entities, nil
}

// aggregateScores combines multiple scores based on the aggregation strategy.
func (p *NERPipeline) aggregateScores(scores []float32) float32 {
	if len(scores) == 0 {
		return 0
	}
	if len(scores) == 1 {
		return scores[0]
	}

	switch p.Config.AggregationStrategy {
	case AggregationFirst:
		return scores[0]
	case AggregationMax:
		maxScore := scores[0]
		for _, s := range scores[1:] {
			if s > maxScore {
				maxScore = s
			}
		}
		return maxScore
	case AggregationAverage, AggregationSimple, "":
		var sum float32
		for _, s := range scores {
			sum += s
		}
		return sum / float32(len(scores))
	default:
		// Default to average
		var sum float32
		for _, s := range scores {
			sum += s
		}
		return sum / float32(len(scores))
	}
}

// getLabel returns the label string for a given index.
func (p *NERPipeline) getLabel(idx int) string {
	if p.ModelConfig != nil && p.ModelConfig.ID2Label != nil {
		if label, ok := p.ModelConfig.ID2Label[idx]; ok {
			return label
		}
	}
	return fmt.Sprintf("LABEL_%d", idx)
}

// Forward runs inference on the given inputs and returns the model outputs.
// Implements backends.Model.
func (p *NERPipeline) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	return p.Model.Forward(ctx, inputs)
}

// Close releases resources held by the pipeline.
// Implements backends.Model.
func (p *NERPipeline) Close() error {
	return p.Model.Close()
}

// Name returns the model name for logging and debugging.
// Implements backends.Model.
func (p *NERPipeline) Name() string {
	return p.Model.Name()
}

// Backend returns the backend type this model uses.
// Implements backends.Model.
func (p *NERPipeline) Backend() backends.BackendType {
	return p.Model.Backend()
}

// ============================================================================
// Loader Functions
// ============================================================================

// NERLoaderOption configures NER pipeline loading.
type NERLoaderOption func(*nerLoaderConfig)

type nerLoaderConfig struct {
	maxLength           int
	aggregationStrategy AggregationStrategy
	ignoreLabels        []string
}

// WithNERMaxLength sets the maximum sequence length for NER.
func WithNERMaxLength(maxLength int) NERLoaderOption {
	return func(c *nerLoaderConfig) {
		c.maxLength = maxLength
	}
}

// WithNERAggregation sets the aggregation strategy for NER.
func WithNERAggregation(strategy AggregationStrategy) NERLoaderOption {
	return func(c *nerLoaderConfig) {
		c.aggregationStrategy = strategy
	}
}

// WithNERIgnoreLabels sets the labels to ignore (e.g., "O" for outside).
func WithNERIgnoreLabels(labels ...string) NERLoaderOption {
	return func(c *nerLoaderConfig) {
		c.ignoreLabels = labels
	}
}

// LoadNERPipeline loads an NER pipeline from a model directory.
// Returns the pipeline and the backend type that was used.
func LoadNERPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...NERLoaderOption,
) (*NERPipeline, backends.BackendType, error) {
	// Apply options
	loaderCfg := &nerLoaderConfig{
		ignoreLabels: []string{"O", ""},
	}
	for _, opt := range opts {
		opt(loaderCfg)
	}

	// Load model configuration
	modelConfig, err := LoadNERModelConfig(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading NER config: %w", err)
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
	model, err := loader.Load(modelPath, backends.WithONNXFile(filepath.Base(modelConfig.ModelFile)))
	if err != nil {
		return nil, "", fmt.Errorf("loading model: %w", err)
	}

	// Build pipeline config
	pipelineConfig := &NERPipelineConfig{
		MaxLength:           FirstNonZero(loaderCfg.maxLength, modelConfig.MaxTextLength, 512),
		AddSpecialTokens:    true,
		AggregationStrategy: loaderCfg.aggregationStrategy,
		IgnoreLabels:        make(map[string]bool),
	}
	if pipelineConfig.AggregationStrategy == "" {
		pipelineConfig.AggregationStrategy = AggregationSimple
	}
	for _, label := range loaderCfg.ignoreLabels {
		pipelineConfig.IgnoreLabels[label] = true
	}

	pipeline := NewNERPipeline(model, tokenizer, modelConfig, pipelineConfig)

	return pipeline, backendType, nil
}

// ============================================================================
// Helper Functions
// ============================================================================

// tokenPred holds a token prediction.
type tokenPred struct {
	labelIdx int
	label    string
	score    float32
	offset   TokenSpan
	valid    bool
}

// argmaxNER returns the index of the maximum value in the slice.
func argmaxNER(values []float32) int {
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

// softmaxScoreNER computes the softmax probability for a specific index.
func softmaxScoreNER(logits []float32, idx int) float32 {
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

// normalizeLabel normalizes BIO/BIOES labels to a standard form.
// Removes the B-/I-/E-/S- prefix and returns the entity type.
func normalizeLabel(label string) string {
	if label == "O" || label == "" {
		return ""
	}

	// Remove BIO prefix if present (B-, I-, E-, S-)
	if len(label) >= 2 && label[1] == '-' {
		return label[2:]
	}

	return label
}

// getLabelType extracts the entity type from a BIO label.
// Returns empty string for O labels.
func getLabelType(label string) string {
	if label == "O" || label == "" {
		return ""
	}
	if len(label) >= 2 && label[1] == '-' {
		return label[2:]
	}
	return label
}

// isBIOBegin checks if a label is a beginning token (B-).
func isBIOBegin(label string) bool {
	return len(label) >= 2 && label[0] == 'B' && label[1] == '-'
}

// safeSubstring returns a substring of text, handling bounds safely.
func safeSubstring(text string, start, end int) string {
	if start < 0 {
		start = 0
	}
	if end > len(text) {
		end = len(text)
	}
	if start >= end {
		return ""
	}
	return text[start:end]
}
