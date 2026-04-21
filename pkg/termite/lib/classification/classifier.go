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

// Package classification provides text classification functionality using NLI models.
// Supports zero-shot classification, allowing text to be classified into arbitrary
// categories without requiring training data for those specific categories.
package classification

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"sort"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"go.uber.org/zap"
)

// Classification represents a single classification prediction.
type Classification struct {
	// Label is the predicted category/class
	Label string `json:"label"`
	// Score is the confidence score (0.0 to 1.0)
	Score float32 `json:"score"`
}

// Classifier defines the interface for Zero-Shot Classification models.
// These models use Natural Language Inference (NLI) to classify text into
// arbitrary categories without requiring category-specific training.
type Classifier interface {
	// Classify classifies the given texts using the specified candidate labels.
	// Returns a slice of classification results for each input text.
	// Each result is a slice of Classifications, one per label, sorted by score descending.
	Classify(ctx context.Context, texts []string, labels []string) ([][]Classification, error)

	// ClassifyWithHypothesis classifies texts using a custom hypothesis template.
	// The template should contain "{}" which will be replaced with each label.
	// Example: "This text is about {}." or "Este texto trata sobre {}."
	ClassifyWithHypothesis(ctx context.Context, texts []string, labels []string, hypothesisTemplate string) ([][]Classification, error)

	// MultiLabelClassify classifies texts allowing multiple labels per text.
	// Unlike Classify which normalizes scores across labels (single-label),
	// this treats each label independently (multi-label classification).
	MultiLabelClassify(ctx context.Context, texts []string, labels []string) ([][]Classification, error)

	// Close releases any resources held by the classifier.
	Close() error
}

// DefaultHypothesisTemplate is the default template used for NLI-based zero-shot classification.
// The "{}" will be replaced with each candidate label.
const DefaultHypothesisTemplate = "This example is {}."

// Config holds configuration for zero-shot classification models.
type Config struct {
	// HypothesisTemplate is the template for constructing NLI hypotheses.
	// Use "{}" as placeholder for the label.
	// Default: "This example is {}."
	HypothesisTemplate string `json:"hypothesis_template,omitempty"`

	// MultiLabel enables multi-label classification mode.
	// When true, each label is evaluated independently.
	// When false (default), scores are normalized across all labels.
	MultiLabel bool `json:"multi_label,omitempty"`

	// Threshold is the minimum score for a classification to be included (0.0-1.0).
	// Only used in multi-label mode.
	Threshold float32 `json:"threshold,omitempty"`

	// ModelID is the original HuggingFace model identifier.
	ModelID string `json:"model_id,omitempty"`
}

// Ensure PooledClassifier implements Classifier
var _ Classifier = (*PooledClassifier)(nil)

// PooledClassifierConfig holds configuration for creating a PooledClassifier.
type PooledClassifierConfig struct {
	// ModelPath is the path to the model directory
	ModelPath string

	// PoolSize determines how many concurrent requests can be processed (0 = auto-detect from CPU count)
	PoolSize int

	// ModelBackends specifies which backends this model supports (nil = all backends)
	ModelBackends []string

	// Logger for logging (nil = no logging)
	Logger *zap.Logger
}

// PooledClassifier manages multiple ClassificationPipeline instances for concurrent zero-shot classification.
// Uses the new backends package (go-huggingface + gomlx/onnxruntime).
type PooledClassifier struct {
	pool        *pool.LazyPool[*pipelines.ClassificationPipeline]
	config      Config
	logger      *zap.Logger
	backendType backends.BackendType
}

// NewPooledClassifier creates a new ClassificationPipeline-based pooled zero-shot classifier.
// This is the new implementation using go-huggingface tokenizers and the backends package.
func NewPooledClassifier(
	cfg PooledClassifierConfig,
	sessionManager *backends.SessionManager,
) (*PooledClassifier, backends.BackendType, error) {
	if cfg.ModelPath == "" {
		return nil, "", fmt.Errorf("model path is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// Default pool size
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	// Load ZSC configuration (hypothesis template, multi-label settings)
	zscConfig, err := LoadZSCConfig(cfg.ModelPath)
	if err != nil {
		logger.Warn("Failed to load ZSC config, using defaults",
			zap.String("modelPath", cfg.ModelPath),
			zap.Error(err))
		zscConfig = &Config{
			HypothesisTemplate: DefaultHypothesisTemplate,
			MultiLabel:         false,
			Threshold:          0.0,
		}
	}

	logger.Info("Initializing pooled zero-shot classifier",
		zap.String("modelPath", cfg.ModelPath),
		zap.Int("poolSize", poolSize),
		zap.String("hypothesisTemplate", zscConfig.HypothesisTemplate))

	var backendUsed backends.BackendType

	lazyPool, first, err := pool.New(pool.Config[*pipelines.ClassificationPipeline]{
		Size: poolSize,
		Factory: func() (*pipelines.ClassificationPipeline, error) {
			pipeline, bt, err := pipelines.LoadClassificationPipeline(
				cfg.ModelPath,
				sessionManager,
				cfg.ModelBackends,
				pipelines.WithHypothesisTemplate(zscConfig.HypothesisTemplate),
				pipelines.WithClassificationMultiLabel(zscConfig.MultiLabel),
			)
			if err != nil {
				return nil, fmt.Errorf("creating classification pipeline: %w", err)
			}
			backendUsed = bt
			return pipeline, nil
		},
		Close: func(p *pipelines.ClassificationPipeline) error {
			if p != nil {
				return p.Close()
			}
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		return nil, "", err
	}

	// backendUsed is set from the eagerly-created first item
	_ = first

	logger.Info("Successfully created pooled ZSC pipelines",
		zap.Int("count", poolSize),
		zap.String("backend", string(backendUsed)))

	return &PooledClassifier{
		pool:        lazyPool,
		config:      *zscConfig,
		logger:      logger,
		backendType: backendUsed,
	}, backendUsed, nil
}

// BackendType returns the backend type used by this classifier
func (p *PooledClassifier) BackendType() backends.BackendType {
	return p.backendType
}

// Classify classifies texts using the specified candidate labels.
// Thread-safe: uses semaphore to limit concurrent pipeline access.
func (p *PooledClassifier) Classify(ctx context.Context, texts []string, labels []string) ([][]Classification, error) {
	return p.ClassifyWithHypothesis(ctx, texts, labels, p.config.HypothesisTemplate)
}

// ClassifyWithHypothesis classifies texts using a custom hypothesis template.
// Note: The hypothesis template is currently ignored; pipeline uses template from creation time.
func (p *PooledClassifier) ClassifyWithHypothesis(ctx context.Context, texts []string, labels []string, hypothesisTemplate string) ([][]Classification, error) {
	if len(texts) == 0 {
		return [][]Classification{}, nil
	}

	if len(labels) == 0 {
		return nil, fmt.Errorf("at least one label is required")
	}

	// Acquire a pipeline from the pool (blocks if all pipelines busy)
	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring pipeline slot: %w", err)
	}
	defer p.pool.Release()

	p.logger.Debug("Using pipeline for ZSC",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Int("num_labels", len(labels)))

	// Delegate to ClassificationPipeline.ClassifyWithLabels
	pipelineResults, err := pipeline.ClassifyWithLabels(ctx, texts, labels)
	if err != nil {
		p.logger.Error("ZSC failed",
			zap.Int("pipelineIndex", idx),
			zap.Error(err))
		return nil, fmt.Errorf("classifying texts: %w", err)
	}

	// Convert pipelines.ClassificationResult to classification.Classification
	results := make([][]Classification, len(pipelineResults))
	for i, result := range pipelineResults {
		// Build sorted list of (label, score) pairs from Scores map
		classifications := make([]Classification, 0, len(result.Scores))
		for label, score := range result.Scores {
			classifications = append(classifications, Classification{
				Label: label,
				Score: score,
			})
		}
		// Sort by score descending
		sort.Slice(classifications, func(a, b int) bool {
			return classifications[a].Score > classifications[b].Score
		})
		results[i] = classifications
	}

	p.logger.Debug("ZSC completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Int("num_results", len(results)))

	return results, nil
}

// MultiLabelClassify classifies texts allowing multiple labels per text.
// Note: Multi-label mode must be configured at pipeline creation time.
func (p *PooledClassifier) MultiLabelClassify(ctx context.Context, texts []string, labels []string) ([][]Classification, error) {
	// For multi-label, we use the same ClassifyWithLabels method
	// The pipeline handles multi-label mode based on its configuration
	results, err := p.ClassifyWithHypothesis(ctx, texts, labels, p.config.HypothesisTemplate)
	if err != nil {
		return nil, err
	}

	// Apply threshold if configured
	if p.config.Threshold > 0 {
		for i := range results {
			filtered := make([]Classification, 0, len(results[i]))
			for _, cls := range results[i] {
				if cls.Score >= p.config.Threshold {
					filtered = append(filtered, cls)
				}
			}
			results[i] = filtered
		}
	}

	return results, nil
}

// Close releases resources.
func (p *PooledClassifier) Close() error {
	return p.pool.Close()
}

// Config returns the classifier configuration.
func (p *PooledClassifier) Config() Config {
	return p.config
}

// IsClassifierModel checks if the given path contains a valid classifier model.
// It looks for zsc_config.json (zero-shot classification config) or checks
// if the model config indicates an NLI-based model that can be used for ZSC.
func IsClassifierModel(modelPath string) bool {
	// Check for explicit ZSC config
	configPath := filepath.Join(modelPath, "zsc_config.json")
	if _, err := os.Stat(configPath); err == nil {
		return true
	}

	// Check for model_type in config.json that indicates NLI capability
	modelConfigPath := filepath.Join(modelPath, "config.json")
	data, err := os.ReadFile(modelConfigPath)
	if err != nil {
		return false
	}

	var modelConfig struct {
		ModelType string            `json:"model_type"`
		Finetuned string            `json:"finetuning_task"`
		ID2Label  map[string]string `json:"id2label"`
	}
	if err := json.Unmarshal(data, &modelConfig); err != nil {
		return false
	}

	// Check for NLI output labels (entailment, neutral, contradiction)
	// which indicates the model can be used for zero-shot classification
	hasEntailment := false
	hasContradiction := false
	for _, label := range modelConfig.ID2Label {
		switch label {
		case "entailment", "ENTAILMENT":
			hasEntailment = true
		case "contradiction", "CONTRADICTION":
			hasContradiction = true
		}
	}

	return hasEntailment && hasContradiction
}

// LoadZSCConfig loads the zero-shot classification configuration from the model directory.
func LoadZSCConfig(modelPath string) (*Config, error) {
	configPath := filepath.Join(modelPath, "zsc_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading zsc_config.json: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing zsc_config.json: %w", err)
	}

	// Set defaults for empty values
	if config.HypothesisTemplate == "" {
		config.HypothesisTemplate = DefaultHypothesisTemplate
	}

	return &config, nil
}
