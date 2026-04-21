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

package ner

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/pkg/termite/lib/utils"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"go.uber.org/zap"
)

// Ensure PooledGLiNER implements the core NER interfaces.
// Classifier and Extractor are conditionally supported based on model capabilities;
// callers should type-assert to check support rather than relying on compile-time guarantees.
var (
	_ Model             = (*PooledGLiNER)(nil)
	_ Recognizer        = (*PooledGLiNER)(nil)
	_ RelationExtractor = (*PooledGLiNER)(nil)
	_ AnswerExtractor   = (*PooledGLiNER)(nil)
)

// =============================================================================
// Pooled GLiNER Implementation
// =============================================================================

// PooledGLiNERConfig holds configuration for creating a PooledGLiNER.
type PooledGLiNERConfig struct {
	// ModelPath is the path to the model directory.
	ModelPath string

	// PoolSize determines how many concurrent requests can be processed (0 = default 1).
	PoolSize int

	// Quantized if true, use quantized model (model_quantized.onnx).
	Quantized bool

	// ModelBackends specifies which backends this model supports (nil = all backends).
	ModelBackends []string

	// Logger for logging (nil = no logging).
	Logger *zap.Logger
}

// PooledGLiNER manages multiple GLiNER pipelines for concurrent zero-shot NER.
// Uses the pipelines.GLiNERPipeline for inference.
type PooledGLiNER struct {
	pool           *pool.LazyPool[*pipelines.GLiNERPipeline]
	logger         *zap.Logger
	backendType    backends.BackendType
	labels         []string // Default labels from config
	relationLabels []string // Default relation labels (if multitask model)
	capabilities   []string // Capabilities from gliner_config.json
}

// NewPooledGLiNER creates a new pooled GLiNER model with session management.
func NewPooledGLiNER(
	cfg PooledGLiNERConfig,
	sessionManager *backends.SessionManager,
) (*PooledGLiNER, backends.BackendType, error) {
	if cfg.ModelPath == "" {
		return nil, "", fmt.Errorf("model path is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	logger.Info("Initializing pooled GLiNER",
		zap.String("modelPath", cfg.ModelPath),
		zap.Int("poolSize", poolSize),
		zap.Bool("quantized", cfg.Quantized))

	// Build loader options — LoadGLiNERPipeline reads gliner_config.json
	// internally, so we only need to forward the quantized preference.
	loaderOpts := []pipelines.GLiNERLoaderOption{
		pipelines.WithGLiNERQuantized(cfg.Quantized),
	}

	// Capture backendType and model config from the first pipeline creation
	var (
		backendType backends.BackendType
		modelConfig *pipelines.GLiNERModelConfig
	)

	lazyPool, first, err := pool.New(pool.Config[*pipelines.GLiNERPipeline]{
		Size: poolSize,
		Factory: func() (*pipelines.GLiNERPipeline, error) {
			pipeline, bt, err := pipelines.LoadGLiNERPipeline(
				cfg.ModelPath,
				sessionManager,
				cfg.ModelBackends,
				loaderOpts...,
			)
			if err != nil {
				return nil, err
			}
			backendType = bt
			if pipeline.Config != nil {
				modelConfig = pipeline.Config
			}
			return pipeline, nil
		},
		Close: func(p *pipelines.GLiNERPipeline) error {
			if p != nil {
				return p.Close()
			}
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		logger.Error("Failed to create GLiNER pipeline pool", zap.Error(err))
		return nil, "", fmt.Errorf("creating GLiNER pipeline pool: %w", err)
	}

	// Extract config values from the pipeline's parsed model config.
	var (
		labels         []string
		relationLabels []string
	)
	if modelConfig != nil {
		labels = modelConfig.DefaultLabels
		relationLabels = modelConfig.RelationLabels
	}
	// Also read capabilities from the pipeline's config.
	var caps []string
	if first.Config != nil {
		caps = first.Config.Capabilities
	}

	logger.Info("Successfully created pooled GLiNER pipelines",
		zap.Int("count", poolSize),
		zap.String("backend", string(backendType)),
		zap.Strings("default_labels", labels))

	return &PooledGLiNER{
		pool:           lazyPool,
		logger:         logger,
		backendType:    backendType,
		labels:         labels,
		relationLabels: relationLabels,
		capabilities:   caps,
	}, backendType, nil
}

// BackendType returns the backend type used by this GLiNER model.
func (p *PooledGLiNER) BackendType() backends.BackendType {
	return p.backendType
}

// =============================================================================
// Model Interface Implementation
// =============================================================================

// Recognize extracts named entities using default labels.
func (p *PooledGLiNER) Recognize(ctx context.Context, texts []string) ([][]Entity, error) {
	return p.RecognizeWithLabels(ctx, texts, p.labels)
}

// Close releases resources.
func (p *PooledGLiNER) Close() error {
	p.logger.Info("Closing PooledGLiNER")
	return p.pool.Close()
}

// =============================================================================
// Recognizer Interface Implementation
// =============================================================================

// RecognizeWithLabels extracts entities of the specified types (zero-shot NER).
func (p *PooledGLiNER) RecognizeWithLabels(ctx context.Context, texts []string, labels []string) ([][]Entity, error) {
	if len(texts) == 0 {
		return [][]Entity{}, nil
	}

	if len(labels) == 0 {
		labels = p.labels
	}

	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Release()

	p.logger.Debug("Starting GLiNER recognition",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Strings("labels", labels))

	output, err := pipeline.RecognizeWithLabels(ctx, texts, labels)
	if err != nil {
		p.logger.Error("GLiNER recognition failed",
			zap.Int("pipelineIndex", idx),
			zap.Error(err))
		return nil, fmt.Errorf("running GLiNER pipeline: %w", err)
	}

	// Entity is a type alias for NEREntity, so no conversion needed.
	results := output.Entities

	p.logger.Debug("GLiNER recognition completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Int("total_entities", utils.CountNested(results)))

	return results, nil
}

// Labels returns the default entity labels this model uses.
func (p *PooledGLiNER) Labels() []string {
	return p.labels
}

// ExtractRelations extracts both entities and relationships between them.
// Returns ErrNotSupported if the model doesn't support relation extraction.
func (p *PooledGLiNER) ExtractRelations(ctx context.Context, texts []string, entityLabels []string, relationLabels []string) ([][]Entity, [][]Relation, error) {
	if len(texts) == 0 {
		return [][]Entity{}, [][]Relation{}, nil
	}

	if !p.SupportsRelationExtraction() {
		return nil, nil, ErrNotSupported
	}

	if len(entityLabels) == 0 {
		entityLabels = p.labels
	}
	if len(relationLabels) == 0 {
		relationLabels = p.relationLabels
	}

	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer p.pool.Release()

	p.logger.Debug("Starting GLiNER relation extraction",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Strings("entity_labels", entityLabels),
		zap.Strings("relation_labels", relationLabels))

	output, err := pipeline.ExtractRelations(ctx, texts, entityLabels, relationLabels)
	if err != nil {
		p.logger.Error("GLiNER relation extraction failed",
			zap.Int("pipelineIndex", idx),
			zap.Error(err))
		return nil, nil, fmt.Errorf("extracting relations: %w", err)
	}

	// Entity and Relation are type aliases, so no conversion needed.
	entities := output.Entities
	relations := output.Relations

	p.logger.Debug("GLiNER relation extraction completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Int("total_entities", utils.CountNested(entities)),
		zap.Int("total_relations", utils.CountNested(relations)))

	return entities, relations, nil
}

// ExtractAnswers performs extractive question answering.
// Returns ErrNotSupported if the model doesn't support QA.
func (p *PooledGLiNER) ExtractAnswers(ctx context.Context, questions []string, contexts []string) ([]Answer, error) {
	if len(questions) == 0 || len(contexts) == 0 {
		return []Answer{}, nil
	}

	if len(questions) != len(contexts) {
		return nil, fmt.Errorf("questions and contexts must have the same length: got %d questions and %d contexts", len(questions), len(contexts))
	}

	if !p.SupportsQA() {
		return nil, ErrNotSupported
	}

	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Release()

	p.logger.Debug("Starting GLiNER question answering",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_questions", len(questions)))

	answers := make([]Answer, len(questions))
	for i, question := range questions {
		context := contexts[i]

		output, err := pipeline.RecognizeWithLabels(ctx, []string{context}, []string{question})
		if err != nil {
			p.logger.Error("GLiNER QA failed",
				zap.Int("index", i),
				zap.String("question", question),
				zap.Error(err))
			return nil, fmt.Errorf("running GLiNER QA for question %d: %w", i, err)
		}

		if len(output.Entities) > 0 && len(output.Entities[0]) > 0 {
			best := output.Entities[0][0]
			for _, e := range output.Entities[0][1:] {
				if e.Score > best.Score {
					best = e
				}
			}
			answers[i] = Answer{
				Text:  best.Text,
				Start: best.Start,
				End:   best.End,
				Score: best.Score,
			}
		}
	}

	p.logger.Debug("GLiNER QA completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_answers", len(answers)))

	return answers, nil
}

// RelationLabels returns the default relation labels this model uses.
func (p *PooledGLiNER) RelationLabels() []string {
	return p.relationLabels
}

// =============================================================================
// Capability Methods — all delegate to pipeline for consistency
// =============================================================================

// SupportsRelationExtraction returns true if the model supports relation extraction.
func (p *PooledGLiNER) SupportsRelationExtraction() bool {
	return p.pool.First().SupportsRelationExtraction()
}

// SupportsQA returns true if the model supports question answering.
func (p *PooledGLiNER) SupportsQA() bool {
	return p.pool.First().SupportsQA()
}

// SupportsClassification returns true if the model supports text classification.
func (p *PooledGLiNER) SupportsClassification() bool {
	return p.pool.First().SupportsClassification()
}

// IsGLiNER2 returns true if this is a GLiNER2 model.
func (p *PooledGLiNER) IsGLiNER2() bool {
	return p.pool.First().IsGLiNER2()
}

// Capabilities returns the capabilities from the GLiNER config.
func (p *PooledGLiNER) Capabilities() []string {
	return p.capabilities
}

// =============================================================================
// Classification Methods (GLiNER2 only)
// =============================================================================

// ClassifyText performs zero-shot text classification using GLiNER2.
func (p *PooledGLiNER) ClassifyText(ctx context.Context, texts []string, labels []string, config *ClassificationConfig) ([][]Classification, error) {
	if len(texts) == 0 {
		return [][]Classification{}, nil
	}

	if !p.SupportsClassification() {
		return nil, ErrNotSupported
	}

	if len(labels) == 0 {
		return nil, fmt.Errorf("classification labels are required")
	}

	if config == nil {
		defaultConfig := DefaultClassificationConfig()
		config = &defaultConfig
	}

	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Release()

	p.logger.Debug("Starting GLiNER2 classification",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Strings("labels", labels),
		zap.Bool("multi_label", config.MultiLabel))

	// ClassificationConfig is a type alias for NERClassificationConfig,
	// so we can pass it directly.
	output, err := pipeline.ClassifyText(ctx, texts, labels, config)
	if err != nil {
		p.logger.Error("GLiNER2 classification failed",
			zap.Int("pipelineIndex", idx),
			zap.Error(err))
		return nil, fmt.Errorf("classifying text: %w", err)
	}

	// Classification is a type alias for NERClassification, so no conversion needed.
	results := output

	p.logger.Debug("GLiNER2 classification completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Int("total_classifications", utils.CountNested(results)))

	return results, nil
}

// =============================================================================
// JSON Extraction Methods (GLiNER2 only)
// =============================================================================

// SupportsExtraction returns true if the model supports structured schema-based extraction.
func (p *PooledGLiNER) SupportsExtraction() bool {
	return p.pool.First().SupportsExtraction()
}

// Extract extracts structured data from texts based on the given schemas.
func (p *PooledGLiNER) Extract(ctx context.Context, texts []string, schemas []ExtractionSchema, config ExtractionConfig) ([]ExtractionResult, error) {
	if len(texts) == 0 {
		return []ExtractionResult{}, nil
	}

	if !p.SupportsExtraction() {
		return nil, ErrNotSupported
	}

	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Release()

	p.logger.Debug("Starting GLiNER2 extraction",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Int("num_schemas", len(schemas)))

	results := make([]ExtractionResult, len(texts))
	for i, text := range texts {
		result, err := extractFromText(ctx, pipeline, text, schemas, config, p.logger)
		if err != nil {
			p.logger.Error("GLiNER2 extraction failed",
				zap.Int("pipelineIndex", idx),
				zap.Int("textIndex", i),
				zap.Error(err))
			return nil, fmt.Errorf("extracting from text %d: %w", i, err)
		}
		results[i] = result
	}

	p.logger.Debug("GLiNER2 extraction completed",
		zap.Int("num_texts", len(texts)))

	return results, nil
}

// =============================================================================
// BiEncoder Label Caching
// =============================================================================

// IsBiEncoder returns true if this is a BiEncoder model that supports label caching.
func (p *PooledGLiNER) IsBiEncoder() bool {
	return p.pool.First().IsBiEncoder()
}

// PrecomputeLabelEmbeddings precomputes and caches embeddings for the given labels.
func (p *PooledGLiNER) PrecomputeLabelEmbeddings(labels []string) error {
	if err := p.pool.InitAll(); err != nil {
		return fmt.Errorf("initializing all pipelines for label precomputation: %w", err)
	}

	var precomputeErr error
	p.pool.ForEachInitialized(func(pipeline *pipelines.GLiNERPipeline) {
		if precomputeErr != nil {
			return
		}
		if err := pipeline.PrecomputeLabelEmbeddings(labels); err != nil {
			p.logger.Error("Failed to precompute label embeddings", zap.Error(err))
			precomputeErr = fmt.Errorf("precomputing label embeddings: %w", err)
		}
	})
	if precomputeErr != nil {
		return precomputeErr
	}

	p.logger.Debug("Precomputed label embeddings",
		zap.Int("num_labels", len(labels)),
		zap.Strings("labels", labels))

	return nil
}

// HasCachedLabelEmbeddings returns true if label embeddings are currently cached.
func (p *PooledGLiNER) HasCachedLabelEmbeddings() bool {
	return p.pool.First().HasCachedLabelEmbeddings()
}

// CachedLabels returns the list of labels that are currently cached.
func (p *PooledGLiNER) CachedLabels() []string {
	return p.pool.First().CachedLabels()
}

// ClearLabelEmbeddingCache clears all cached label embeddings across all pooled pipelines.
func (p *PooledGLiNER) ClearLabelEmbeddingCache() {
	p.pool.ForEachInitialized(func(pipeline *pipelines.GLiNERPipeline) {
		pipeline.ClearLabelEmbeddingCache()
	})

	p.logger.Debug("Cleared label embedding cache")
}

