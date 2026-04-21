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
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ajroetker/go-highway/hwy/contrib/algo"
	"github.com/ajroetker/go-highway/hwy/contrib/nn"
	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// ============================================================================
// Config Types and Loading
// ============================================================================

// ClassificationModelConfig holds parsed configuration for a text classification model.
type ClassificationModelConfig struct {
	// Path to the model directory
	ModelPath string

	// ModelFile is the ONNX file for the classification model
	ModelFile string

	// NumLabels is the number of output classes
	NumLabels int

	// ID2Label maps label IDs to human-readable labels
	ID2Label map[int]string

	// Label2ID maps label names to IDs
	Label2ID map[string]int

	// ModelType is the model architecture type (bert, roberta, deberta, bart, etc.)
	ModelType string

	// ProblemType indicates the classification task type (single_label, multi_label, zero_shot_nli)
	ProblemType string

	// MaxTextLength is the maximum text sequence length from max_position_embeddings
	MaxTextLength int
}

// LoadClassificationModelConfig loads and parses configuration for a classification model.
func LoadClassificationModelConfig(modelPath string) (*ClassificationModelConfig, error) {
	config := &ClassificationModelConfig{
		ModelPath: modelPath,
	}

	// Detect model file
	config.ModelFile = FindONNXFile(modelPath, []string{
		"model.onnx",
		"model_quantized.onnx",
		"classifier.onnx",
	})

	if config.ModelFile == "" {
		return nil, fmt.Errorf("no ONNX model file found in %s", modelPath)
	}

	// Load configuration from config.json
	rawConfig, err := loadRawClassificationConfig(modelPath)
	if err != nil {
		// Config is optional for some models
		rawConfig = &rawClassificationConfig{
			NumLabels: 2, // Default for binary classification
		}
	}

	// Determine model type
	config.ModelType = rawConfig.ModelType
	if config.ModelType == "" {
		config.ModelType = detectClassificationModelType(modelPath)
	}

	// Extract number of labels
	config.NumLabels = FirstNonZero(rawConfig.NumLabels, 2)

	// Extract label mappings
	config.ID2Label = rawConfig.ID2Label
	config.Label2ID = rawConfig.Label2ID

	// Extract max text sequence length
	config.MaxTextLength = rawConfig.MaxPositionEmbeddings

	// Determine problem type
	config.ProblemType = rawConfig.ProblemType
	if config.ProblemType == "" {
		config.ProblemType = detectProblemType(config)
	}

	return config, nil
}

// IsClassificationModel checks if a model path contains a text classification model.
// Returns true if the model appears to be a classifier based on config and files.
func IsClassificationModel(path string) bool {
	// Check for classifier-specific indicators in path
	lowerPath := strings.ToLower(path)
	if strings.Contains(lowerPath, "classifier") ||
		strings.Contains(lowerPath, "classification") ||
		strings.Contains(lowerPath, "sentiment") ||
		strings.Contains(lowerPath, "mnli") ||
		strings.Contains(lowerPath, "xnli") ||
		strings.Contains(lowerPath, "nli") {
		return true
	}

	// Check config.json for classification indicators
	rawConfig, err := loadRawClassificationConfig(path)
	if err != nil {
		return false
	}

	// Classification models typically have num_labels >= 2
	if rawConfig.NumLabels >= 2 {
		// Also check if it's not a typical embedding model
		if rawConfig.ModelType != "clip" && rawConfig.ModelType != "siglip" {
			return true
		}
	}

	return false
}

// rawClassificationConfig represents config.json for classification models.
type rawClassificationConfig struct {
	ModelType             string         `json:"model_type"`
	NumLabels             int            `json:"num_labels"`
	HiddenSize            int            `json:"hidden_size"`
	MaxPositionEmbeddings int            `json:"max_position_embeddings"`
	ID2Label              map[int]string `json:"id2label"`
	Label2ID              map[string]int `json:"label2id"`
	ProblemType           string         `json:"problem_type"`
}

// loadRawClassificationConfig loads config.json for classification models.
func loadRawClassificationConfig(modelPath string) (*rawClassificationConfig, error) {
	configPath := filepath.Join(modelPath, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config.json: %w", err)
	}

	var config rawClassificationConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config.json: %w", err)
	}

	return &config, nil
}

// detectClassificationModelType attempts to detect the model type from the path.
func detectClassificationModelType(modelPath string) string {
	lowerPath := strings.ToLower(modelPath)
	switch {
	case strings.Contains(lowerPath, "bart"):
		return "bart"
	case strings.Contains(lowerPath, "deberta"):
		return "deberta"
	case strings.Contains(lowerPath, "roberta"):
		return "roberta"
	case strings.Contains(lowerPath, "bert"):
		return "bert"
	case strings.Contains(lowerPath, "xlm"):
		return "xlm-roberta"
	case strings.Contains(lowerPath, "distilbert"):
		return "distilbert"
	default:
		return "classifier"
	}
}

// detectProblemType determines the problem type based on model configuration.
func detectProblemType(config *ClassificationModelConfig) string {
	// Check if it's an NLI model (typically 3 labels: contradiction, neutral, entailment)
	if config.NumLabels == 3 {
		if config.ID2Label != nil {
			// Check for NLI-specific labels
			for _, label := range config.ID2Label {
				lowerLabel := strings.ToLower(label)
				if strings.Contains(lowerLabel, "entailment") ||
					strings.Contains(lowerLabel, "contradiction") ||
					strings.Contains(lowerLabel, "neutral") {
					return "zero_shot_nli"
				}
			}
		}
	}

	// Default to single-label classification
	return "single_label"
}

// ============================================================================
// Result Types
// ============================================================================

// ClassificationResult holds the result of classifying a single text.
type ClassificationResult struct {
	// Label is the predicted class label (highest scoring)
	Label string `json:"label"`

	// Score is the confidence score for the predicted label (0.0 to 1.0)
	Score float32 `json:"score"`

	// Scores contains probabilities for all labels
	Scores map[string]float32 `json:"scores,omitempty"`
}

// ============================================================================
// Pipeline Struct and Methods
// ============================================================================

// Ensure ClassificationPipeline implements backends.SequenceClassificationModel
var _ backends.SequenceClassificationModel = (*ClassificationPipeline)(nil)

// ClassificationPipeline wraps a model for text classification tasks.
// Supports single-label classification, multi-label classification, and zero-shot classification.
type ClassificationPipeline struct {
	// Model performs inference on tokenized inputs.
	Model backends.Model

	// Tokenizer handles text-to-token conversion.
	Tokenizer tokenizers.Tokenizer

	// BasePipeline handles tokenization with pair encoding.
	BasePipeline *Pipeline

	// Config holds pipeline configuration.
	Config *ClassificationPipelineConfig

	// ModelConfig holds model-specific configuration.
	ModelConfig *ClassificationModelConfig

	// entailmentIndex is the index of the "entailment" label in NLI model outputs.
	// Different models may use different orderings (e.g., 0=entailment vs 2=entailment).
	entailmentIndex int
}

// ClassificationPipelineConfig holds configuration for a ClassificationPipeline.
type ClassificationPipelineConfig struct {
	// MaxLength is the maximum sequence length.
	MaxLength int

	// AddSpecialTokens controls whether to add [CLS], [SEP], etc.
	AddSpecialTokens bool

	// MultiLabel enables multi-label classification mode.
	// When true, sigmoid is applied per-label independently.
	// When false, softmax is applied across all labels.
	MultiLabel bool

	// HypothesisTemplate is the template for zero-shot classification.
	// The "{}" placeholder is replaced with candidate labels.
	// Default: "This text is about {}.".
	HypothesisTemplate string
}

// DefaultClassificationPipelineConfig returns sensible defaults for classification.
func DefaultClassificationPipelineConfig() *ClassificationPipelineConfig {
	return &ClassificationPipelineConfig{
		MaxLength:          512,
		AddSpecialTokens:   true,
		MultiLabel:         false,
		HypothesisTemplate: "This example is {}.",
	}
}

// NewClassificationPipeline creates a new ClassificationPipeline.
func NewClassificationPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	modelConfig *ClassificationModelConfig,
	pipelineConfig *ClassificationPipelineConfig,
) *ClassificationPipeline {
	if pipelineConfig == nil {
		pipelineConfig = DefaultClassificationPipelineConfig()
	}

	// Create base pipeline for tokenization
	basePipeline := New(tokenizer, model,
		WithMaxLength(pipelineConfig.MaxLength),
		WithAddSpecialTokens(pipelineConfig.AddSpecialTokens),
		WithTruncation(TruncationLongestFirst),
		WithPadding(PaddingLongest),
	)

	// Determine entailment index from model config.
	// NLI models have different label orderings; we need to find "entailment".
	entailmentIndex := findEntailmentIndex(modelConfig)

	return &ClassificationPipeline{
		Model:           model,
		Tokenizer:       tokenizer,
		BasePipeline:    basePipeline,
		Config:          pipelineConfig,
		ModelConfig:     modelConfig,
		entailmentIndex: entailmentIndex,
	}
}

// findEntailmentIndex determines the index of the "entailment" label in the model output.
// Different NLI models use different label orderings:
// - Some: [contradiction, neutral, entailment] -> entailment at index 2
// - Others: [entailment, neutral, contradiction] -> entailment at index 0
func findEntailmentIndex(modelConfig *ClassificationModelConfig) int {
	if modelConfig == nil || modelConfig.ID2Label == nil {
		// Default assumption: [contradiction, neutral, entailment]
		return 2
	}

	// Search for "entailment" in id2label mapping
	for idx, label := range modelConfig.ID2Label {
		lowerLabel := strings.ToLower(label)
		if strings.Contains(lowerLabel, "entailment") {
			return idx
		}
	}

	// Fallback to index 2 if entailment not found
	return 2
}

// Classify classifies a batch of texts and returns the predicted label with scores.
// For multi-class models, returns the label with highest probability.
func (p *ClassificationPipeline) Classify(ctx context.Context, texts []string) ([]ClassificationResult, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	// Encode texts
	batch, err := p.BasePipeline.Encode(texts)
	if err != nil {
		return nil, fmt.Errorf("encoding texts: %w", err)
	}

	// Create model inputs
	inputs := &backends.ModelInputs{
		InputIDs:      batch.InputIDs,
		AttentionMask: batch.AttentionMask,
	}

	// Run model forward pass
	logits, err := p.ClassifySequence(ctx, inputs)
	if err != nil {
		return nil, err
	}

	// Convert logits to results
	return p.logitsToResults(logits), nil
}

// ClassifyWithLabels performs zero-shot classification using NLI.
// The model takes (text, candidate_label) pairs and predicts entailment.
// This is similar to how cross-encoders work.
func (p *ClassificationPipeline) ClassifyWithLabels(ctx context.Context, texts []string, candidateLabels []string) ([]ClassificationResult, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	if len(candidateLabels) == 0 {
		return nil, fmt.Errorf("at least one candidate label is required")
	}

	results := make([]ClassificationResult, len(texts))

	for textIdx, text := range texts {
		// Build text-hypothesis pairs for this text
		pairs := make([][2]string, len(candidateLabels))
		for labelIdx, label := range candidateLabels {
			hypothesis := strings.Replace(p.Config.HypothesisTemplate, "{}", label, 1)
			pairs[labelIdx] = [2]string{text, hypothesis}
		}

		// Encode all pairs
		batch, err := p.BasePipeline.EncodePairs(pairs)
		if err != nil {
			return nil, fmt.Errorf("encoding pairs for text %d: %w", textIdx, err)
		}

		// Create model inputs
		inputs := &backends.ModelInputs{
			InputIDs:      batch.InputIDs,
			AttentionMask: batch.AttentionMask,
			TokenTypeIDs:  batch.TokenTypeIDs,
		}

		// Run model forward pass
		output, err := p.Model.Forward(ctx, inputs)
		if err != nil {
			return nil, fmt.Errorf("forward pass for text %d: %w", textIdx, err)
		}

		if len(output.Logits) == 0 {
			return nil, fmt.Errorf("model does not return logits")
		}

		// Extract entailment scores for each label
		// NLI models have varying label orderings, use entailmentIndex from config.
		entailmentScores := make([]float32, len(candidateLabels))
		for i, logit := range output.Logits {
			if len(logit) >= 3 {
				// Standard NLI model: use entailment score at configured index
				entailmentScores[i] = logit[p.entailmentIndex]
			} else if len(logit) == 2 {
				// Binary model: use positive class
				entailmentScores[i] = logit[1]
			} else if len(logit) >= 1 {
				// Single output: use directly
				entailmentScores[i] = logit[0]
			}
		}

		// Convert to probabilities
		var probabilities []float32
		if p.Config.MultiLabel {
			// Multi-label: sigmoid for each label independently
			probabilities = applySigmoid(entailmentScores)
		} else {
			// Single-label: softmax across all labels
			probabilities = applySoftmax(entailmentScores)
		}

		// Build result with scores for all labels
		scores := make(map[string]float32, len(candidateLabels))
		bestIdx := 0
		bestScore := probabilities[0]
		for i, label := range candidateLabels {
			scores[label] = probabilities[i]
			if probabilities[i] > bestScore {
				bestScore = probabilities[i]
				bestIdx = i
			}
		}

		results[textIdx] = ClassificationResult{
			Label:  candidateLabels[bestIdx],
			Score:  bestScore,
			Scores: scores,
		}
	}

	return results, nil
}

// ClassifySequence returns sequence-level predictions.
// Returns logits with shape [batch, num_labels].
// Implements backends.SequenceClassificationModel.
func (p *ClassificationPipeline) ClassifySequence(ctx context.Context, inputs *backends.ModelInputs) ([][]float32, error) {
	// Run model forward pass
	output, err := p.Model.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("forward pass: %w", err)
	}

	// Return logits
	if len(output.Logits) == 0 {
		return nil, fmt.Errorf("model does not return logits for sequence classification")
	}

	return output.Logits, nil
}

// logitsToResults converts model logits to classification results.
func (p *ClassificationPipeline) logitsToResults(logits [][]float32) []ClassificationResult {
	results := make([]ClassificationResult, len(logits))

	for i, logit := range logits {
		// Apply softmax or sigmoid based on config
		var probs []float32
		if p.Config.MultiLabel {
			probs = applySigmoid(logit)
		} else {
			probs = applySoftmax(logit)
		}

		// Find best label
		bestIdx := 0
		bestScore := probs[0]
		for j, prob := range probs {
			if prob > bestScore {
				bestScore = prob
				bestIdx = j
			}
		}

		// Get label name from config or use index
		label := fmt.Sprintf("LABEL_%d", bestIdx)
		if p.ModelConfig != nil && p.ModelConfig.ID2Label != nil {
			if name, ok := p.ModelConfig.ID2Label[bestIdx]; ok {
				label = name
			}
		}

		// Build scores map
		scores := make(map[string]float32, len(probs))
		for j, prob := range probs {
			labelName := fmt.Sprintf("LABEL_%d", j)
			if p.ModelConfig != nil && p.ModelConfig.ID2Label != nil {
				if name, ok := p.ModelConfig.ID2Label[j]; ok {
					labelName = name
				}
			}
			scores[labelName] = prob
		}

		results[i] = ClassificationResult{
			Label:  label,
			Score:  bestScore,
			Scores: scores,
		}
	}

	return results
}

// applySoftmax applies softmax to convert logits to probabilities in-place using SIMD acceleration.
// The input slice is modified and returned.
func applySoftmax(logits []float32) []float32 {
	if len(logits) == 0 {
		return nil
	}
	nn.SoftmaxInPlace(logits)
	return logits
}

// applySigmoid applies sigmoid to convert logits to probabilities (for multi-label) in-place using SIMD acceleration.
// The input slice is modified and returned.
func applySigmoid(logits []float32) []float32 {
	if len(logits) == 0 {
		return nil
	}
	algo.SigmoidTransform(logits, logits)
	return logits
}

// Forward runs inference on the given inputs and returns the model outputs.
// Implements backends.Model.
func (p *ClassificationPipeline) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	return p.Model.Forward(ctx, inputs)
}

// Close releases resources held by the pipeline.
// Implements backends.Model.
func (p *ClassificationPipeline) Close() error {
	return p.Model.Close()
}

// Name returns the model name for logging and debugging.
// Implements backends.Model.
func (p *ClassificationPipeline) Name() string {
	return p.Model.Name()
}

// Backend returns the backend type this model uses.
// Implements backends.Model.
func (p *ClassificationPipeline) Backend() backends.BackendType {
	return p.Model.Backend()
}

// GetLabels returns the available labels for this classifier.
func (p *ClassificationPipeline) GetLabels() []string {
	if p.ModelConfig == nil {
		return []string{"LABEL_0", "LABEL_1"} // Default binary
	}

	if p.ModelConfig.ID2Label == nil {
		labels := make([]string, p.ModelConfig.NumLabels)
		for i := range labels {
			labels[i] = fmt.Sprintf("LABEL_%d", i)
		}
		return labels
	}

	// Sort by ID to maintain consistent ordering
	type labelEntry struct {
		id    int
		label string
	}
	entries := make([]labelEntry, 0, len(p.ModelConfig.ID2Label))
	for id, label := range p.ModelConfig.ID2Label {
		entries = append(entries, labelEntry{id, label})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].id < entries[j].id
	})

	labels := make([]string, len(entries))
	for i, e := range entries {
		labels[i] = e.label
	}
	return labels
}

// ============================================================================
// Loader Functions
// ============================================================================

// ClassificationLoaderOption configures classification pipeline loading.
type ClassificationLoaderOption func(*classificationLoaderConfig)

type classificationLoaderConfig struct {
	maxLength          int
	multiLabel         bool
	hypothesisTemplate string
	quantized          bool
}

// WithClassificationMaxLength sets the maximum sequence length for classification.
func WithClassificationMaxLength(maxLength int) ClassificationLoaderOption {
	return func(c *classificationLoaderConfig) {
		c.maxLength = maxLength
	}
}

// WithClassificationMultiLabel enables multi-label classification mode.
func WithClassificationMultiLabel(multiLabel bool) ClassificationLoaderOption {
	return func(c *classificationLoaderConfig) {
		c.multiLabel = multiLabel
	}
}

// WithHypothesisTemplate sets the hypothesis template for zero-shot classification.
func WithHypothesisTemplate(template string) ClassificationLoaderOption {
	return func(c *classificationLoaderConfig) {
		c.hypothesisTemplate = template
	}
}

// WithClassificationQuantized uses quantized model files if available.
func WithClassificationQuantized(quantized bool) ClassificationLoaderOption {
	return func(c *classificationLoaderConfig) {
		c.quantized = quantized
	}
}

// LoadClassificationPipeline loads a classification pipeline from a model directory.
// Returns the pipeline and the backend type that was used.
func LoadClassificationPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...ClassificationLoaderOption,
) (*ClassificationPipeline, backends.BackendType, error) {
	// Apply options
	loaderCfg := &classificationLoaderConfig{
		hypothesisTemplate: "This text is about {}.",
	}
	for _, opt := range opts {
		opt(loaderCfg)
	}

	// Load model configuration
	modelConfig, err := LoadClassificationModelConfig(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading classification config: %w", err)
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
	pipelineConfig := &ClassificationPipelineConfig{
		MaxLength:          FirstNonZero(loaderCfg.maxLength, modelConfig.MaxTextLength, 512),
		AddSpecialTokens:   true,
		MultiLabel:         loaderCfg.multiLabel,
		HypothesisTemplate: loaderCfg.hypothesisTemplate,
	}

	pipeline := NewClassificationPipeline(model, tokenizer, modelConfig, pipelineConfig)

	return pipeline, backendType, nil
}
