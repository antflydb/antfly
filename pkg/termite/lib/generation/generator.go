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

package generation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"go.uber.org/zap"
)

// Ensure PooledPipelineGenerator implements the Generator and StreamingGenerator interfaces
var _ Generator = (*PooledPipelineGenerator)(nil)
var _ StreamingGenerator = (*PooledPipelineGenerator)(nil)

// PooledPipelineGenerator manages multiple TextGenerationPipelines for concurrent text generation.
// Each request acquires a pipeline slot via the pool, enabling true parallelism.
type PooledPipelineGenerator struct {
	pool         *pool.LazyPool[*pipelines.TextGenerationPipeline]
	logger       *zap.Logger
	modelPath    string
	chatTemplate *ChatTemplate
	toolSupport
}

// PooledPipelineGeneratorConfig holds configuration for creating a PooledPipelineGenerator.
type PooledPipelineGeneratorConfig struct {
	// ModelPath is the path to the model directory.
	ModelPath string

	// PoolSize is the number of concurrent pipelines (0 = default of 1).
	PoolSize int

	// GenerationConfig holds text generation parameters. If nil, uses defaults.
	GenerationConfig *backends.GenerationConfig

	// Logger for logging. If nil, uses a no-op logger.
	Logger *zap.Logger
}

// NewPooledPipelineGenerator creates a new pooled generator using the session-based pipeline architecture.
func NewPooledPipelineGenerator(
	cfg *PooledPipelineGeneratorConfig,
	sessionManager *backends.SessionManager,
	modelBackends []string,
) (*PooledPipelineGenerator, backends.BackendType, error) {
	if cfg == nil {
		return nil, "", errors.New("config is required")
	}
	if cfg.ModelPath == "" {
		return nil, "", errors.New("model path is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	logger.Info("Initializing pooled pipeline generator",
		zap.String("modelPath", cfg.ModelPath),
		zap.Int("poolSize", poolSize))

	// Build pipeline options with tool parser factory
	var opts []pipelines.TextGenerationPipelineOption
	if cfg.GenerationConfig != nil {
		opts = append(opts, pipelines.WithTextGenerationConfig(cfg.GenerationConfig))
	}

	// Pass the tool parser factory to the pipeline loader
	// This enables the pipeline to automatically load tool parsers from genai_config.json
	opts = append(opts, pipelines.WithToolParserFactory(func(modelPath string) (pipelines.ToolParser, error) {
		parser, err := GetToolParser("", modelPath)
		if err != nil {
			return nil, err
		}
		if parser == nil {
			return nil, nil
		}
		return &toolParserAdapter{parser}, nil
	}))

	// Track the backend type from the first pipeline creation
	var backendType backends.BackendType

	// Create lazy pool of pipelines
	p, first, err := pool.New(pool.Config[*pipelines.TextGenerationPipeline]{
		Size: poolSize,
		Factory: func() (*pipelines.TextGenerationPipeline, error) {
			pipeline, bt, err := pipelines.LoadTextGenerationPipeline(
				cfg.ModelPath,
				sessionManager,
				modelBackends,
				opts...,
			)
			if err != nil {
				return nil, fmt.Errorf("loading text generation pipeline: %w", err)
			}
			backendType = bt
			return pipeline, nil
		},
		Close: func(pipeline *pipelines.TextGenerationPipeline) error {
			if pipeline != nil {
				return pipeline.Close()
			}
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		return nil, "", err
	}

	// Get tool parser info from first pipeline
	var toolParser ToolParser
	var toolCallFormat string
	if first.SupportsTools() {
		toolCallFormat = first.ToolCallFormat()
		if pipelineParser := first.GetToolParser(); pipelineParser != nil {
			// Unwrap the adapter to get our ToolParser
			if adapter, ok := pipelineParser.(*toolParserAdapter); ok {
				toolParser = adapter.inner
			}
		}
		// If we have a format but no parser from pipeline, try to load it directly
		if toolParser == nil && toolCallFormat != "" {
			var err error
			toolParser, err = GetToolParser(toolCallFormat, cfg.ModelPath)
			if err != nil {
				logger.Warn("Failed to load tool parser",
					zap.String("format", toolCallFormat),
					zap.Error(err))
			} else {
				logger.Info("Loaded tool parser from model config",
					zap.String("format", toolCallFormat))
			}
		}
	}

	// Load chat template (optional — falls back to simple prompt format)
	chatTemplate, err := LoadChatTemplate(cfg.ModelPath)
	if err != nil {
		logger.Warn("Failed to load chat template, using simple prompt format",
			zap.String("modelPath", cfg.ModelPath),
			zap.Error(err))
	} else if chatTemplate != nil {
		logger.Info("Loaded chat template from model",
			zap.String("modelPath", cfg.ModelPath))
	} else {
		logger.Info("No chat template found, using simple prompt format",
			zap.String("modelPath", cfg.ModelPath))
	}

	logger.Info("Created pooled pipeline generator",
		zap.Int("poolSize", poolSize),
		zap.String("backend", string(backendType)))

	return &PooledPipelineGenerator{
		pool:         p,
		logger:       logger,
		modelPath:    cfg.ModelPath,
		chatTemplate: chatTemplate,
		toolSupport: toolSupport{
			toolParser:     toolParser,
			toolCallFormat: toolCallFormat,
		},
	}, backendType, nil
}

// toolParserAdapter adapts our ToolParser to pipelines.ToolParser
type toolParserAdapter struct {
	inner ToolParser
}

func (a *toolParserAdapter) Name() string {
	return a.inner.Name()
}

func (a *toolParserAdapter) FormatToolsPrompt(tools []pipelines.ToolDefinition) string {
	// Convert pipelines.ToolDefinition to generation.ToolDefinition
	genTools := make([]ToolDefinition, len(tools))
	for i, t := range tools {
		genTools[i] = ToolDefinition{
			Type: t.Type,
			Function: FunctionDefinition{
				Name:        t.Function.Name,
				Description: t.Function.Description,
				Parameters:  t.Function.Parameters,
				Strict:      t.Function.Strict,
			},
		}
	}
	return a.inner.FormatToolsPrompt(genTools)
}

func (a *toolParserAdapter) Feed(token string) []pipelines.ToolCall {
	calls := a.inner.Feed(token)
	// Convert generation.ToolCall to pipelines.ToolCall
	result := make([]pipelines.ToolCall, len(calls))
	for i, c := range calls {
		result[i] = pipelines.ToolCall{
			ID:   c.ID,
			Type: c.Type,
			Function: pipelines.ToolCallFunction{
				Name:      c.Function.Name,
				Arguments: c.Function.Arguments,
			},
		}
	}
	return result
}

func (a *toolParserAdapter) Finish() ([]pipelines.ToolCall, string) {
	calls, text := a.inner.Finish()
	result := make([]pipelines.ToolCall, len(calls))
	for i, c := range calls {
		result[i] = pipelines.ToolCall{
			ID:   c.ID,
			Type: c.Type,
			Function: pipelines.ToolCallFunction{
				Name:      c.Function.Name,
				Arguments: c.Function.Arguments,
			},
		}
	}
	return result, text
}

func (a *toolParserAdapter) Reset() {
	a.inner.Reset()
}

// Generate produces text from the given messages.
// Thread-safe: uses pool to limit concurrent pipeline access.
func (p *PooledPipelineGenerator) Generate(ctx context.Context, messages []Message, opts GenerateOptions) (*GenerateResult, error) {
	if len(messages) == 0 {
		return nil, errors.New("messages are required")
	}

	// Acquire pool slot (blocks if all pipelines busy)
	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Release()

	p.logger.Debug("Using pipeline for generation",
		zap.Int("pipelineIndex", idx),
		zap.Int("numMessages", len(messages)))

	// Convert messages to prompt string
	prompt, err := p.formatPrompt(messages)
	if err != nil {
		return nil, fmt.Errorf("formatting prompt: %w", err)
	}

	// Save original config and restore after request to avoid mutating shared state.
	// Without this, the second request sees stale values from the first request.
	originalConfig := *pipeline.Generator.Config
	defer func() { *pipeline.Generator.Config = originalConfig }()

	// Default to greedy decoding (like Ollama). The model's generation_config.json
	// may set do_sample=true, but for API serving we want deterministic output
	// unless the caller explicitly requests sampling via temperature/top_p/top_k.
	pipeline.Generator.Config.DoSample = false
	if opts.MaxTokens > 0 {
		pipeline.Generator.Config.MaxNewTokens = opts.MaxTokens
	}
	if opts.Temperature > 0 {
		pipeline.Generator.Config.Temperature = opts.Temperature
		pipeline.Generator.Config.DoSample = true
	}
	if opts.TopP > 0 && opts.TopP < 1.0 {
		pipeline.Generator.Config.TopP = opts.TopP
		pipeline.Generator.Config.DoSample = true
	}
	if opts.TopK > 0 {
		pipeline.Generator.Config.TopK = opts.TopK
		pipeline.Generator.Config.DoSample = true
	}

	p.logger.Debug("Generation config",
		zap.Int("maxNewTokens", pipeline.Generator.Config.MaxNewTokens),
		zap.Bool("doSample", pipeline.Generator.Config.DoSample),
		zap.Float32("temperature", pipeline.Generator.Config.Temperature),
		zap.Int("topK", pipeline.Generator.Config.TopK),
		zap.Float32("topP", pipeline.Generator.Config.TopP),
		zap.Int("promptLen", len(prompt)))

	// Run pipeline
	result, err := pipeline.Generate(ctx, prompt)
	if err != nil {
		p.logger.Error("Pipeline generation failed",
			zap.Int("pipelineIndex", idx),
			zap.Error(err))
		return nil, fmt.Errorf("running text generation: %w", err)
	}

	finishReason := "stop"
	if !result.StoppedAtEOS {
		finishReason = "length"
	}

	genResult := &GenerateResult{
		Text:         result.Text,
		TokensUsed:   result.TokenCount,
		FinishReason: finishReason,
	}

	p.logger.Debug("Generation complete",
		zap.Int("pipelineIndex", idx),
		zap.Int("responseLength", len(result.Text)),
		zap.Int("tokensGenerated", result.TokenCount),
		zap.String("finishReason", genResult.FinishReason))

	return genResult, nil
}

// GenerateStream produces tokens one at a time via channels.
// Thread-safe: uses pool to limit concurrent pipeline access.
func (p *PooledPipelineGenerator) GenerateStream(ctx context.Context, messages []Message, opts GenerateOptions) (<-chan TokenDelta, <-chan error, error) {
	if len(messages) == 0 {
		return nil, nil, errors.New("messages are required")
	}

	// Acquire pool slot (blocks if all pipelines busy)
	pipeline, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}

	p.logger.Debug("Using pipeline for streaming generation",
		zap.Int("pipelineIndex", idx),
		zap.Int("numMessages", len(messages)))

	// Convert messages to prompt string
	prompt, err := p.formatPrompt(messages)
	if err != nil {
		p.pool.Release()
		return nil, nil, fmt.Errorf("formatting prompt: %w", err)
	}

	// Save original config and restore after request to avoid mutating shared state.
	originalConfig := *pipeline.Generator.Config

	// Default to greedy decoding (like Ollama). Only enable sampling when
	// the caller explicitly requests it via temperature/top_p/top_k.
	pipeline.Generator.Config.DoSample = false
	if opts.MaxTokens > 0 {
		pipeline.Generator.Config.MaxNewTokens = opts.MaxTokens
	}
	if opts.Temperature > 0 {
		pipeline.Generator.Config.Temperature = opts.Temperature
		pipeline.Generator.Config.DoSample = true
	}
	if opts.TopP > 0 && opts.TopP < 1.0 {
		pipeline.Generator.Config.TopP = opts.TopP
		pipeline.Generator.Config.DoSample = true
	}
	if opts.TopK > 0 {
		pipeline.Generator.Config.TopK = opts.TopK
		pipeline.Generator.Config.DoSample = true
	}

	p.logger.Debug("Streaming generation config",
		zap.Int("maxNewTokens", pipeline.Generator.Config.MaxNewTokens),
		zap.Bool("doSample", pipeline.Generator.Config.DoSample),
		zap.Float32("temperature", pipeline.Generator.Config.Temperature),
		zap.Int("topK", pipeline.Generator.Config.TopK),
		zap.Float32("topP", pipeline.Generator.Config.TopP),
		zap.Int("promptLen", len(prompt)))

	// Create output channels
	tokenChan := make(chan TokenDelta)
	errChan := make(chan error, 1)

	go func() {
		// Defers run LIFO: close channels first, then restore config, then release pool slot.
		defer p.pool.Release()
		defer func() { *pipeline.Generator.Config = originalConfig }()
		defer close(tokenChan)
		defer close(errChan)

		// Streaming callback
		callback := func(token int32, text string) bool {
			select {
			case <-ctx.Done():
				return false
			case tokenChan <- TokenDelta{Token: text, Index: 0}:
				return true
			}
		}

		// Run with streaming
		_, err := pipeline.GenerateWithStreaming(ctx, prompt, callback)
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
		}

		p.logger.Debug("Streaming generation complete", zap.Int("pipelineIndex", idx))
	}()

	return tokenChan, errChan, nil
}

// Close releases resources.
func (p *PooledPipelineGenerator) Close() error {
	p.logger.Info("Closing pooled pipeline generator")
	return p.pool.Close()
}

// formatPrompt converts messages to a prompt string using the chat template
// if available, otherwise falling back to a simple format.
func (p *PooledPipelineGenerator) formatPrompt(messages []Message) (string, error) {
	if p.chatTemplate != nil {
		prompt, err := p.chatTemplate.Apply(messages, true)
		if err != nil {
			p.logger.Warn("Chat template failed, falling back to simple format",
				zap.Error(err))
			return messagesToPrompt(messages), nil
		}
		p.logger.Debug("Formatted prompt with chat template",
			zap.Int("promptLength", len(prompt)))
		return prompt, nil
	}
	p.logger.Debug("No chat template, using simple prompt format")
	return messagesToPrompt(messages), nil
}

// messagesToPrompt converts messages to a simple prompt string.
func messagesToPrompt(messages []Message) string {
	var prompt strings.Builder
	for _, msg := range messages {
		switch msg.Role {
		case "system":
			fmt.Fprintf(&prompt, "System: %s\n\n", msg.GetTextContent())
		case "user":
			fmt.Fprintf(&prompt, "User: %s\n\n", msg.GetTextContent())
		case "assistant":
			fmt.Fprintf(&prompt, "Assistant: %s\n\n", msg.GetTextContent())
		default:
			prompt.WriteString(msg.GetTextContent() + "\n\n")
		}
	}
	prompt.WriteString("Assistant: ")
	return prompt.String()
}
