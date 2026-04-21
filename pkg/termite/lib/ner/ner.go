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
	"errors"
	"fmt"

	"github.com/antflydb/antfly/pkg/termite/lib/utils"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"go.uber.org/zap"
)

// ErrNotSupported is returned when a model doesn't support a particular operation.
// Check capabilities before calling methods to avoid this error.
var ErrNotSupported = errors.New("operation not supported by this model")

// Entity is a named entity extracted from text.
// It is a type alias for pipelines.NEREntity to avoid conversion boilerplate.
type Entity = pipelines.NEREntity

// Model defines the interface for Named Entity Recognition models.
type Model interface {
	// Recognize extracts named entities from the given texts.
	// Returns a slice of entities for each input text.
	Recognize(ctx context.Context, texts []string) ([][]Entity, error)

	// Close releases any resources held by the model.
	Close() error
}

// Relation is a relationship between two entities.
// It is a type alias for pipelines.NERRelation to avoid conversion boilerplate.
type Relation = pipelines.NERRelation

// Answer represents an extracted answer span from question answering.
type Answer struct {
	// Text is the answer text extracted from the context
	Text string `json:"text"`
	// Start is the character offset where the answer begins in the context
	Start int `json:"start"`
	// End is the character offset where the answer ends (exclusive)
	End int `json:"end"`
	// Score is the model's confidence in this answer (0.0-1.0)
	Score float32 `json:"score"`
}

// Recognizer extends Model with zero-shot NER capabilities.
// Models implementing this interface can recognize any entity types specified
// at inference time without requiring model retraining (e.g., GLiNER, REBEL).
//
// For additional capabilities, type-assert to:
//   - RelationExtractor: relation extraction between entities
//   - AnswerExtractor: extractive question answering
//   - Classifier: zero-shot text classification
//   - Extractor: structured schema-based extraction
type Recognizer interface {
	Model

	// RecognizeWithLabels extracts entities of the specified types.
	// For zero-shot models like GLiNER, labels can be arbitrary entity types.
	// For traditional NER models, labels should match the trained entity types.
	RecognizeWithLabels(ctx context.Context, texts []string, labels []string) ([][]Entity, error)

	// Labels returns the default entity labels this model uses.
	Labels() []string
}

// RelationExtractor extracts relationships between entities.
// Type-assert from Model or Recognizer to check support.
type RelationExtractor interface {
	// ExtractRelations extracts both entities and relationships between them.
	ExtractRelations(ctx context.Context, texts []string, entityLabels []string, relationLabels []string) ([][]Entity, [][]Relation, error)

	// RelationLabels returns the default relation labels this model uses.
	RelationLabels() []string
}

// AnswerExtractor performs extractive question answering.
// Type-assert from Model or Recognizer to check support.
type AnswerExtractor interface {
	// ExtractAnswers extracts answer spans from context given questions.
	ExtractAnswers(ctx context.Context, questions []string, contexts []string) ([]Answer, error)
}

// Classifier defines the interface for text classification models.
// Models implementing this interface can perform zero-shot text classification
// where labels are specified at inference time (e.g., GLiNER2).
//
// Type-assert from Model or Recognizer to check support.
type Classifier interface {
	// ClassifyText performs zero-shot text classification.
	// Returns classification results for each input text.
	ClassifyText(ctx context.Context, texts []string, labels []string, config *ClassificationConfig) ([][]Classification, error)
}

// Classification is a text classification result.
// It is a type alias for pipelines.NERClassification to avoid conversion boilerplate.
type Classification = pipelines.NERClassification

// ClassificationConfig holds configuration for classification.
// It is a type alias for pipelines.NERClassificationConfig to avoid conversion boilerplate.
type ClassificationConfig = pipelines.NERClassificationConfig

// DefaultClassificationConfig returns sensible defaults for classification.
func DefaultClassificationConfig() ClassificationConfig {
	return ClassificationConfig{
		Threshold:  0.5,
		MultiLabel: false,
		TopK:       1,
	}
}

// Ensure PooledNER implements the Model interface
var _ Model = (*PooledNER)(nil)

// PooledNERConfig holds configuration for creating a PooledNER.
type PooledNERConfig struct {
	// ModelPath is the path to the model directory
	ModelPath string

	// PoolSize determines how many concurrent requests can be processed (0 = 1)
	PoolSize int

	// ModelBackends specifies which backends this model supports (nil = all backends)
	ModelBackends []string

	// Logger for logging (nil = no logging)
	Logger *zap.Logger
}

// PooledNER manages multiple NERPipeline instances for concurrent NER.
// Uses the new backends package (go-huggingface + gomlx/onnxruntime).
type PooledNER struct {
	pool        *pool.LazyPool[*pipelines.NERPipeline]
	logger      *zap.Logger
	backendType backends.BackendType
}

// NewPooledNER creates a new NERPipeline-based pooled NER model.
// This is the new implementation using go-huggingface tokenizers and the backends package.
func NewPooledNER(
	cfg PooledNERConfig,
	sessionManager *backends.SessionManager,
) (*PooledNER, backends.BackendType, error) {
	if cfg.ModelPath == "" {
		return nil, "", fmt.Errorf("model path is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// Default pool size to 1 if not specified
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	logger.Info("Initializing pooled NER",
		zap.String("modelPath", cfg.ModelPath),
		zap.Int("poolSize", poolSize))

	var backendUsed backends.BackendType

	p, _, err := pool.New(pool.Config[*pipelines.NERPipeline]{
		Size: poolSize,
		Factory: func() (*pipelines.NERPipeline, error) {
			pipeline, bt, err := pipelines.LoadNERPipeline(
				cfg.ModelPath,
				sessionManager,
				cfg.ModelBackends,
			)
			if err != nil {
				return nil, fmt.Errorf("creating NER pipeline: %w", err)
			}
			backendUsed = bt
			return pipeline, nil
		},
		Close: func(pipeline *pipelines.NERPipeline) error {
			if pipeline != nil {
				return pipeline.Close()
			}
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		logger.Error("Failed to create NER pipeline pool", zap.Error(err))
		return nil, "", err
	}

	logger.Info("Successfully created pooled NER pipeline pool",
		zap.Int("size", poolSize),
		zap.String("backend", string(backendUsed)))

	return &PooledNER{
		pool:        p,
		logger:      logger,
		backendType: backendUsed,
	}, backendUsed, nil
}

// BackendType returns the backend type used by this NER model
func (p *PooledNER) BackendType() backends.BackendType {
	return p.backendType
}

// Recognize extracts named entities from the given texts.
// Thread-safe: uses pool semaphore to limit concurrent pipeline access.
func (p *PooledNER) Recognize(ctx context.Context, texts []string) ([][]Entity, error) {
	if len(texts) == 0 {
		return [][]Entity{}, nil
	}

	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring pipeline slot: %w", err)
	}
	defer p.pool.Release()

	p.logger.Debug("Using pipeline for NER",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)))

	// pipelines.Entity is an alias for NEREntity (same as ner.Entity),
	// so no conversion is needed.
	results, err := pipeline.ExtractEntities(ctx, texts)
	if err != nil {
		p.logger.Error("NER failed",
			zap.Int("pipelineIndex", idx),
			zap.Error(err))
		return nil, fmt.Errorf("extracting entities: %w", err)
	}

	p.logger.Debug("NER completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_texts", len(texts)),
		zap.Int("total_entities", utils.CountNested(results)))

	return results, nil
}

// Close releases resources.
func (p *PooledNER) Close() error {
	return p.pool.Close()
}

