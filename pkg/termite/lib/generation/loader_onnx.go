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

//go:build onnx && ORT

package generation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"github.com/knights-analytics/ortgenai"
	"go.uber.org/zap"
)

// LoadGenerator loads a text generation model using the available backends.
// With ONNX support, this tries the pipeline-based approach first,
// then falls back to the GenerativeSessionFactory for generative models.
//
// Models with genai_config.json (ortgenai-native format, like FunctionGemma)
// always use GenerativeSessionFactory directly, as it provides features like
// tool calling that the pipeline approach doesn't support.
func LoadGenerator(
	modelPath string,
	poolSize int,
	logger *zap.Logger,
	sessionManager *backends.SessionManager,
	modelBackends []string,
) (Generator, backends.BackendType, error) {
	// Check if model has genai_config.json (ortgenai-native format).
	// These models should always use GenerativeSessionFactory for full feature support
	// (e.g., tool calling, proper chat templates, etc.).
	hasGenaiConfig := false
	if _, statErr := os.Stat(filepath.Join(modelPath, "genai_config.json")); statErr == nil {
		hasGenaiConfig = true
	}

	if hasGenaiConfig {
		logger.Debug("Model has genai_config.json, using GenerativeSessionFactory directly",
			zap.String("modelPath", modelPath))

		genFactory, bt, factoryErr := sessionManager.GetGenerativeSessionFactoryForModel(modelBackends)
		if factoryErr != nil {
			return nil, "", fmt.Errorf("getting generative session factory: %w", factoryErr)
		}

		// Try Engine API for continuous batching (available in ORT GenAI >= 0.9.1).
		// The factory is passed so a fallback session can handle multimodal requests.
		if ortgenai.IsEngineApiAvailable() {
			logger.Debug("Engine API available, trying EngineGenerator",
				zap.String("modelPath", modelPath))

			engineGen, engineErr := NewEngineGenerator(modelPath, genFactory, logger)
			if engineErr == nil {
				logger.Info("Loaded generator using Engine (continuous batching)",
					zap.String("modelPath", modelPath))
				return engineGen, backends.BackendONNX, nil
			}
			logger.Warn("Engine creation failed, falling back to session pool",
				zap.Error(engineErr))
		}

		genGenerator, genErr := NewPooledGenerativeSessionGenerator(modelPath, poolSize, genFactory, logger)
		if genErr != nil {
			return nil, "", fmt.Errorf("creating generative session generator: %w", genErr)
		}

		logger.Info("Loaded generator using GenerativeSessionFactory",
			zap.String("modelPath", modelPath),
			zap.String("backend", string(bt)))

		return genGenerator, bt, nil
	}

	// Try the pipeline-based approach for models without genai_config.json
	cfg := &PooledPipelineGeneratorConfig{
		ModelPath: modelPath,
		PoolSize:  poolSize,
		Logger:    logger,
	}
	generator, backendType, err := NewPooledPipelineGenerator(cfg, sessionManager, modelBackends)
	if err == nil {
		return generator, backendType, nil
	}

	// Check if we should fall back to GenerativeSessionFactory for generative models.
	// Fall back when session factory not supported (encoder model factory can't handle generative models)
	if strings.Contains(err.Error(), "session factory") {
		logger.Debug("Pipeline approach failed, falling back to GenerativeSessionFactory",
			zap.String("modelPath", modelPath),
			zap.Error(err))

		genFactory, bt, factoryErr := sessionManager.GetGenerativeSessionFactoryForModel(modelBackends)
		if factoryErr != nil {
			// Return the original error since it's likely more informative
			return nil, "", err
		}

		genGenerator, genErr := NewPooledGenerativeSessionGenerator(modelPath, poolSize, genFactory, logger)
		if genErr != nil {
			// Return the original error
			return nil, "", err
		}

		logger.Info("Loaded generator using GenerativeSessionFactory",
			zap.String("modelPath", modelPath),
			zap.String("backend", string(bt)))

		return genGenerator, bt, nil
	}

	// Return the original error for other failure cases
	return nil, "", err
}

// Ensure PooledGenerativeSessionGenerator implements the interfaces
var _ Generator = (*PooledGenerativeSessionGenerator)(nil)
var _ StreamingGenerator = (*PooledGenerativeSessionGenerator)(nil)

// PooledGenerativeSessionGenerator wraps multiple GenerativeSessions for concurrent generation.
// It adapts the backends.GenerativeSession interface to the generation.Generator interface.
type PooledGenerativeSessionGenerator struct {
	pool      *pool.LazyPool[backends.GenerativeSession]
	logger    *zap.Logger
	modelPath string
	toolSupport
}

// NewPooledGenerativeSessionGenerator creates a new pooled generator using GenerativeSessionFactory.
func NewPooledGenerativeSessionGenerator(
	modelPath string,
	poolSize int,
	factory backends.GenerativeSessionFactory,
	logger *zap.Logger,
) (*PooledGenerativeSessionGenerator, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	if poolSize <= 0 {
		poolSize = 1
	}

	logger.Info("Initializing pooled generative session generator",
		zap.String("modelPath", modelPath),
		zap.Int("poolSize", poolSize))

	p, _, err := pool.New(pool.Config[backends.GenerativeSession]{
		Size: poolSize,
		Factory: func() (backends.GenerativeSession, error) {
			return factory.CreateGenerativeSession(modelPath)
		},
		Close: func(s backends.GenerativeSession) error {
			s.Close()
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}

	logger.Info("Successfully created pooled generative sessions", zap.Int("count", poolSize))

	toolParser, toolCallFormat := loadToolParserFromConfig(modelPath, logger)

	return &PooledGenerativeSessionGenerator{
		pool:      p,
		logger:    logger,
		modelPath: modelPath,
		toolSupport: toolSupport{
			toolParser:     toolParser,
			toolCallFormat: toolCallFormat,
		},
	}, nil
}

// Generate produces text from the given messages.
func (p *PooledGenerativeSessionGenerator) Generate(ctx context.Context, messages []Message, opts GenerateOptions) (*GenerateResult, error) {
	// Acquire a session from the pool
	session, _, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Release()

	// Convert messages to backend format
	backendMsgs := toBackendMessages(messages)
	backendOpts := toBackendOptions(opts)

	// Generate
	result, err := session.Generate(ctx, backendMsgs, backendOpts)
	if err != nil {
		return nil, err
	}

	return &GenerateResult{
		Text:         result.Text,
		TokensUsed:   result.TokensUsed,
		FinishReason: result.FinishReason,
	}, nil
}

// GenerateStream produces tokens one at a time via channels.
func (p *PooledGenerativeSessionGenerator) GenerateStream(ctx context.Context, messages []Message, opts GenerateOptions) (<-chan TokenDelta, <-chan error, error) {
	// Acquire a session from the pool
	session, _, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Convert messages to backend format
	backendMsgs := toBackendMessages(messages)
	backendOpts := toBackendOptions(opts)

	// Start streaming
	backendTokenChan, backendErrChan, err := session.GenerateStream(ctx, backendMsgs, backendOpts)
	if err != nil {
		p.pool.Release()
		return nil, nil, err
	}

	tokenChan, errChan := adaptBackendStream(ctx, backendTokenChan, backendErrChan, func() {
		p.pool.Release()
	})
	return tokenChan, errChan, nil
}

// Close releases resources.
func (p *PooledGenerativeSessionGenerator) Close() error {
	p.logger.Info("Closing pooled generative session generator")
	return p.pool.Close()
}

// toBackendMessages converts generation.Message to backends.GenerativeMessage.
func toBackendMessages(messages []Message) []backends.GenerativeMessage {
	result := make([]backends.GenerativeMessage, len(messages))
	for i, m := range messages {
		// Extract text content
		content := m.Content
		if len(m.Parts) > 0 {
			var textParts []string
			for _, part := range m.Parts {
				if part.Type == "text" && part.Text != "" {
					textParts = append(textParts, part.Text)
				}
			}
			content = strings.Join(textParts, "")
		}

		// Extract image URLs
		var imageURLs []string
		for _, part := range m.Parts {
			if part.Type == "image_url" && part.ImageURL != "" {
				imageURLs = append(imageURLs, part.ImageURL)
			}
		}

		result[i] = backends.GenerativeMessage{
			Role:      m.Role,
			Content:   content,
			ImageURLs: imageURLs,
		}
	}
	return result
}

// toBackendOptions converts generation.GenerateOptions to backends.GenerativeOptions.
func toBackendOptions(opts GenerateOptions) *backends.GenerativeOptions {
	return &backends.GenerativeOptions{
		MaxTokens:   opts.MaxTokens,
		Temperature: opts.Temperature,
		TopP:        opts.TopP,
		TopK:        opts.TopK,
		StopTokens:  opts.StopTokens,
	}
}

// adaptBackendStream bridges backend streaming channels to generation channels.
// The optional onDone callback runs when the goroutine finishes (e.g., semaphore release).
func adaptBackendStream(
	ctx context.Context,
	backendTokenChan <-chan backends.GenerativeToken,
	backendErrChan <-chan error,
	onDone func(),
) (<-chan TokenDelta, <-chan error) {
	tokenChan := make(chan TokenDelta)
	errChan := make(chan error, 1)

	go func() {
		if onDone != nil {
			defer onDone()
		}
		defer close(tokenChan)
		defer close(errChan)

		for token := range backendTokenChan {
			select {
			case <-ctx.Done():
				return
			case tokenChan <- TokenDelta{Token: token.Token, Index: token.Index}:
			}
		}

		for err := range backendErrChan {
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}
	}()

	return tokenChan, errChan
}

// loadToolParserFromConfig attempts to load a tool parser from genai_config.json.
// Returns nil parser and empty format if no tool_call_format is configured.
func loadToolParserFromConfig(modelPath string, logger *zap.Logger) (ToolParser, string) {
	format := readToolCallFormat(modelPath)
	if format == "" {
		return nil, ""
	}
	parser, err := GetToolParser(format, modelPath)
	if err != nil {
		logger.Warn("Failed to load tool parser",
			zap.String("format", format),
			zap.Error(err))
		return nil, ""
	}
	if parser != nil {
		logger.Info("Loaded tool parser from model config",
			zap.String("format", format))
	}
	return parser, format
}

// genaiConfigToolFormat holds the tool_call_format from genai_config.json.
type genaiConfigToolFormat struct {
	ToolCallFormat string `json:"tool_call_format"`
}

// readToolCallFormat reads the tool_call_format from genai_config.json.
// Returns empty string if the file doesn't exist or doesn't have tool_call_format.
func readToolCallFormat(modelPath string) string {
	configPath := filepath.Join(modelPath, "genai_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return ""
	}

	var config genaiConfigToolFormat
	if err := json.Unmarshal(data, &config); err != nil {
		return ""
	}

	return config.ToolCallFormat
}
