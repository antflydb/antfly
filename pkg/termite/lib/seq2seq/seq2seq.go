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

package seq2seq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"go.uber.org/zap"
)

// Model is the interface for seq2seq text generation models.
type Model interface {
	Generate(ctx context.Context, inputs []string) (*GeneratedOutput, error)
	Config() Config
	Close() error
}

// QuestionGenerator is the interface for models that generate questions from context.
type QuestionGenerator interface {
	GenerateQuestions(ctx context.Context, pairs []AnswerContextPair) (*GeneratedOutput, error)
}

// Paraphraser is the interface for models that generate paraphrases.
type Paraphraser interface {
	Paraphrase(ctx context.Context, texts []string) (*GeneratedOutput, error)
}

// Config holds seq2seq model configuration.
type Config struct {
	ModelID            string  `json:"model_id,omitempty"`
	Task               string  `json:"task,omitempty"`
	MaxLength          int     `json:"max_length,omitempty"`
	NumBeams           int     `json:"num_beams,omitempty"`
	NumReturnSequences int     `json:"num_return_sequences,omitempty"`
	DoSample           bool    `json:"do_sample,omitempty"`
	TopP               float32 `json:"top_p,omitempty"`
	Temperature        float32 `json:"temperature,omitempty"`
	InputFormat        string  `json:"input_format,omitempty"`
}

// GeneratedOutput holds the output from seq2seq generation.
type GeneratedOutput struct {
	// Texts[i][j] is the j-th generated sequence for the i-th input
	Texts [][]string
	// Tokens[i][j] is the token IDs for Texts[i][j]
	Tokens [][][]uint32
}

// AnswerContextPair represents an answer-context pair for question generation.
type AnswerContextPair struct {
	Answer  string
	Context string
}

// FormatLMQGInput formats an answer-context pair for LMQG-style models.
func FormatLMQGInput(answer, context string) string {
	return fmt.Sprintf("generate question: <hl> %s <hl> %s", answer, context)
}

// FormatLMQGInputBatch formats multiple answer-context pairs for LMQG-style models.
func FormatLMQGInputBatch(pairs []AnswerContextPair) []string {
	inputs := make([]string, len(pairs))
	for i, pair := range pairs {
		inputs[i] = FormatLMQGInput(pair.Answer, pair.Context)
	}
	return inputs
}

// IsParaphraseModel checks if the model path indicates a paraphrase model.
func IsParaphraseModel(modelPath string) bool {
	// Check config for task type
	configPath := filepath.Join(modelPath, "seq2seq_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		// Check model name for hints
		modelName := strings.ToLower(filepath.Base(modelPath))
		return strings.Contains(modelName, "paraphrase") || strings.Contains(modelName, "pegasus")
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return false
	}

	return config.Task == "paraphrase"
}

// containsAny checks if s contains any of the substrings (case-insensitive).
func containsAny(s string, substrings ...string) bool {
	sLower := strings.ToLower(filepath.Base(s))
	for _, sub := range substrings {
		if len(sub) > 0 && strings.Contains(sLower, sub) {
			return true
		}
	}
	return false
}

// Ensure PooledSeq2Seq implements the Model, QuestionGenerator, and Paraphraser interfaces
var _ Model = (*PooledSeq2Seq)(nil)
var _ QuestionGenerator = (*PooledSeq2Seq)(nil)
var _ Paraphraser = (*PooledSeq2Seq)(nil)

// PooledSeq2SeqConfig holds configuration for creating a PooledSeq2Seq.
type PooledSeq2SeqConfig struct {
	// ModelPath is the path to the model directory
	ModelPath string

	// PoolSize determines how many concurrent requests can be processed (0 = auto-detect from CPU count)
	PoolSize int

	// ModelBackends specifies which backends this model supports (nil = all backends)
	ModelBackends []string

	// Logger for logging (nil = no logging)
	Logger *zap.Logger
}

// PooledSeq2Seq manages multiple Seq2SeqPipeline instances for concurrent seq2seq generation.
type PooledSeq2Seq struct {
	pool        *pool.LazyPool[*pipelines.Seq2SeqPipeline]
	config      Config
	logger      *zap.Logger
	backendType backends.BackendType
}

// NewPooledSeq2Seq creates a new pooled seq2seq model.
func NewPooledSeq2Seq(
	cfg PooledSeq2SeqConfig,
	sessionManager *backends.SessionManager,
) (*PooledSeq2Seq, backends.BackendType, error) {
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

	// Load seq2seq configuration
	seq2seqConfig, err := LoadSeq2SeqConfig(cfg.ModelPath)
	if err != nil {
		logger.Warn("Failed to load seq2seq config, using defaults",
			zap.String("modelPath", cfg.ModelPath),
			zap.Error(err))
		seq2seqConfig = &Config{
			MaxLength:          64,
			NumBeams:           1,
			NumReturnSequences: 1,
			DoSample:           false,
			TopP:               0.9,
			Temperature:        1.0,
		}
	}

	logger.Info("Initializing pooled seq2seq model",
		zap.String("modelPath", cfg.ModelPath),
		zap.Int("poolSize", poolSize),
		zap.String("task", seq2seqConfig.Task),
		zap.Int("maxLength", seq2seqConfig.MaxLength))

	// Create generation config
	genConfig := &backends.GenerationConfig{
		MaxNewTokens:      seq2seqConfig.MaxLength,
		MinLength:         1,
		Temperature:       seq2seqConfig.Temperature,
		TopP:              seq2seqConfig.TopP,
		TopK:              50,
		DoSample:          seq2seqConfig.DoSample,
		RepetitionPenalty: 1.0,
	}

	// Track the backend type from the factory
	var backendUsed backends.BackendType

	lazyPool, firstPipeline, err := pool.New(pool.Config[*pipelines.Seq2SeqPipeline]{
		Size: poolSize,
		Factory: func() (*pipelines.Seq2SeqPipeline, error) {
			pipeline, bt, err := pipelines.LoadSeq2SeqPipeline(
				cfg.ModelPath,
				sessionManager,
				cfg.ModelBackends,
				pipelines.WithSeq2SeqGenerationConfig(genConfig),
			)
			if err != nil {
				return nil, err
			}
			backendUsed = bt
			return pipeline, nil
		},
		Close: func(p *pipelines.Seq2SeqPipeline) error {
			if p != nil {
				return p.Close()
			}
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		return nil, "", fmt.Errorf("creating seq2seq pool: %w", err)
	}

	_ = firstPipeline // backend type captured via closure

	logger.Info("Successfully created pooled seq2seq pipelines",
		zap.Int("count", poolSize),
		zap.String("backend", string(backendUsed)))

	return &PooledSeq2Seq{
		pool:        lazyPool,
		config:      *seq2seqConfig,
		logger:      logger,
		backendType: backendUsed,
	}, backendUsed, nil
}

// BackendType returns the backend type used by this model
func (p *PooledSeq2Seq) BackendType() backends.BackendType {
	return p.backendType
}

// Generate runs the seq2seq model on the given inputs.
// Thread-safe: uses pool semaphore to limit concurrent pipeline access.
func (p *PooledSeq2Seq) Generate(ctx context.Context, inputs []string) (*GeneratedOutput, error) {
	if len(inputs) == 0 {
		return &GeneratedOutput{
			Texts:  [][]string{},
			Tokens: [][][]uint32{},
		}, nil
	}

	// Acquire a pipeline from the pool (blocks if all pipelines busy)
	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring pipeline slot: %w", err)
	}
	defer p.pool.Release()

	p.logger.Debug("Using pipeline for seq2seq generation",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_inputs", len(inputs)))

	// Generate for each input using the pipeline
	results := &GeneratedOutput{
		Texts:  make([][]string, len(inputs)),
		Tokens: make([][][]uint32, len(inputs)),
	}

	for i, input := range inputs {
		result, err := pipeline.Generate(ctx, input)
		if err != nil {
			p.logger.Error("Generation failed for input",
				zap.Int("index", i),
				zap.Error(err))
			return nil, fmt.Errorf("generating for input %d: %w", i, err)
		}

		// Convert result to output format
		tokens := make([]uint32, len(result.TokenIDs))
		for j, id := range result.TokenIDs {
			tokens[j] = uint32(id)
		}
		results.Texts[i] = []string{result.Text}
		results.Tokens[i] = [][]uint32{tokens}
	}

	p.logger.Debug("Seq2seq generation completed",
		zap.Int("pipelineIndex", idx),
		zap.Int("num_inputs", len(inputs)))

	return results, nil
}

// GenerateQuestions generates questions given answer-context pairs.
// This is a convenience method for LMQG-style backends.
func (p *PooledSeq2Seq) GenerateQuestions(ctx context.Context, pairs []AnswerContextPair) (*GeneratedOutput, error) {
	if len(pairs) == 0 {
		return &GeneratedOutput{
			Texts:  [][]string{},
			Tokens: [][][]uint32{},
		}, nil
	}

	// Format inputs for LMQG models
	inputs := FormatLMQGInputBatch(pairs)

	return p.Generate(ctx, inputs)
}

// Paraphrase generates paraphrases of the input texts.
// This is a convenience method for paraphrase models like PEGASUS.
func (p *PooledSeq2Seq) Paraphrase(ctx context.Context, texts []string) (*GeneratedOutput, error) {
	if len(texts) == 0 {
		return &GeneratedOutput{
			Texts:  [][]string{},
			Tokens: [][][]uint32{},
		}, nil
	}

	// Paraphrase models take raw text as input
	return p.Generate(ctx, texts)
}

// Config returns the model configuration.
func (p *PooledSeq2Seq) Config() Config {
	return p.config
}

// Close releases resources.
func (p *PooledSeq2Seq) Close() error {
	return p.pool.Close()
}

// LoadSeq2SeqConfig loads the seq2seq configuration from the model directory.
func LoadSeq2SeqConfig(modelPath string) (*Config, error) {
	configPath := filepath.Join(modelPath, "seq2seq_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading seq2seq_config.json: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing seq2seq_config.json: %w", err)
	}

	// Set defaults for empty values
	if config.MaxLength == 0 {
		config.MaxLength = 64
	}
	if config.NumBeams == 0 {
		config.NumBeams = 1
	}
	if config.NumReturnSequences == 0 {
		config.NumReturnSequences = 1
	}
	if config.Temperature == 0 {
		config.Temperature = 1.0
	}
	if config.TopP == 0 {
		config.TopP = 0.9
	}

	return &config, nil
}

// IsSeq2SeqModel checks if the model path contains a seq2seq model
// by looking for required ONNX files (encoder.onnx, decoder-init.onnx, decoder.onnx).
func IsSeq2SeqModel(modelPath string) bool {
	requiredFiles := []string{"encoder.onnx", "decoder-init.onnx", "decoder.onnx"}
	for _, file := range requiredFiles {
		filePath := filepath.Join(modelPath, file)
		if _, err := os.Stat(filePath); err != nil {
			return false
		}
	}
	return true
}

// IsQuestionGenerationModel checks if the model is configured for question generation.
func IsQuestionGenerationModel(modelPath string) bool {
	if !IsSeq2SeqModel(modelPath) {
		return false
	}

	// Check config for task type
	configPath := filepath.Join(modelPath, "seq2seq_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		// Check model name for hints
		modelName := strings.ToLower(filepath.Base(modelPath))
		return strings.Contains(modelName, "qg") ||
			strings.Contains(modelName, "question") ||
			strings.Contains(modelName, "squad")
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return false
	}

	return config.Task == "question_generation"
}
