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
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/knights-analytics/ortgenai"
	"go.uber.org/zap"
)

// Ensure EngineGenerator implements the interfaces.
var _ Generator = (*EngineGenerator)(nil)
var _ StreamingGenerator = (*EngineGenerator)(nil)

// EngineGenerator wraps an ortgenai.Engine for continuous batching.
// Unlike PooledGenerativeSessionGenerator which creates N session copies,
// EngineGenerator uses a single model copy with continuous batching for
// efficient concurrent inference.
//
// For multimodal (image) requests, it falls back to a single GenerativeSession
// because the Engine C API only supports token-based inputs (OgaRequestAddTokens),
// not named tensors (OgaGenerator_SetInputs) needed for processed image data.
type EngineGenerator struct {
	engine        *ortgenai.Engine
	logger        *zap.Logger
	modelPath     string
	contextLength int
	toolSupport

	// Lazy-initialized fallback session for multimodal (image) requests.
	fallbackMu      sync.Mutex
	fallbackOnce    sync.Once
	fallbackSession backends.GenerativeSession
	fallbackFactory backends.GenerativeSessionFactory
	fallbackErr     error
}

// NewEngineGenerator creates an EngineGenerator from the given model path.
// A fallback GenerativeSession is created for multimodal requests since the
// Engine API only supports text (token) inputs.
func NewEngineGenerator(modelPath string, factory backends.GenerativeSessionFactory, logger *zap.Logger) (*EngineGenerator, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	engine, err := backends.CreateOrtgenaiEngine(modelPath)
	if err != nil {
		return nil, fmt.Errorf("creating ortgenai engine: %w", err)
	}

	contextLength := backends.ReadContextLength(modelPath)

	toolParser, toolCallFormat := loadToolParserFromConfig(modelPath, logger)

	logger.Info("Created EngineGenerator with continuous batching",
		zap.String("modelPath", modelPath),
		zap.Int("contextLength", contextLength))

	return &EngineGenerator{
		engine:          engine,
		fallbackFactory: factory,
		logger:          logger,
		modelPath:       modelPath,
		contextLength:   contextLength,
		toolSupport: toolSupport{
			toolParser:     toolParser,
			toolCallFormat: toolCallFormat,
		},
	}, nil
}

// Generate produces text from the given messages.
func (g *EngineGenerator) Generate(ctx context.Context, messages []Message, opts GenerateOptions) (*GenerateResult, error) {
	// Multimodal requests fall back to Session — Engine API only supports tokens.
	for _, m := range messages {
		if m.HasImages() {
			g.logger.Debug("Multimodal request, using fallback session")
			return g.generateWithFallback(ctx, messages, opts)
		}
	}

	// Convert messages: generation.Message → backends.GenerativeMessage → ortgenai.Message
	ortMessages := toOrtgenaiMessagesFromGen(messages)

	maxLength := g.contextLength
	if maxLength <= 0 {
		maxLength = 8192
	}

	genOpts := &ortgenai.GenerationOptions{
		MaxLength: maxLength,
		BatchSize: 1,
	}

	// Use a cancellable context to enforce MaxTokens output limit.
	genCtx, genCancel := context.WithCancel(ctx)
	defer genCancel()

	outputChan, errChan, err := g.engine.Submit(genCtx, ortMessages, genOpts)
	if err != nil {
		return nil, fmt.Errorf("engine submit: %w", err)
	}

	// Collect tokens, enforcing MaxTokens.
	maxOutputTokens := opts.MaxTokens
	var generatedText strings.Builder
	var tokenCount int
	for delta := range outputChan {
		if delta.EOSReached {
			break
		}
		generatedText.WriteString(delta.Token)
		tokenCount++
		if maxOutputTokens > 0 && tokenCount >= maxOutputTokens {
			genCancel()
			break
		}
	}

	// Determine finish reason.
	finishReason := "stop"
	if maxOutputTokens > 0 && tokenCount >= maxOutputTokens {
		finishReason = "length"
	}

	// Drain remaining tokens after cancel.
	for range outputChan {
	}

	// Check for errors (ignore context.Canceled since we may have cancelled intentionally).
	for err := range errChan {
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, fmt.Errorf("generation error: %w", err)
		}
	}

	return &GenerateResult{
		Text:         generatedText.String(),
		TokensUsed:   tokenCount,
		FinishReason: finishReason,
	}, nil
}

// GenerateStream produces tokens one at a time via channels.
func (g *EngineGenerator) GenerateStream(ctx context.Context, messages []Message, opts GenerateOptions) (<-chan TokenDelta, <-chan error, error) {
	// Multimodal requests fall back to Session — Engine API only supports tokens.
	for _, m := range messages {
		if m.HasImages() {
			g.logger.Debug("Multimodal streaming request, using fallback session")
			return g.generateStreamWithFallback(ctx, messages, opts)
		}
	}

	// Convert messages.
	ortMessages := toOrtgenaiMessagesFromGen(messages)

	maxLength := g.contextLength
	if maxLength <= 0 {
		maxLength = 8192
	}

	genOpts := &ortgenai.GenerationOptions{
		MaxLength: maxLength,
		BatchSize: 1,
	}

	genCtx, genCancel := context.WithCancel(ctx)

	outputChan, ortErrChan, err := g.engine.Submit(genCtx, ortMessages, genOpts)
	if err != nil {
		genCancel()
		return nil, nil, fmt.Errorf("engine submit: %w", err)
	}

	// Adapt ortgenai channels to generation channels, enforcing MaxTokens.
	maxOutputTokens := opts.MaxTokens
	tokenChan := make(chan TokenDelta)
	errChan := make(chan error, 1)

	go func() {
		defer close(tokenChan)
		defer close(errChan)
		defer genCancel()

		var tokenCount int
		for delta := range outputChan {
			if delta.EOSReached {
				break
			}
			select {
			case <-ctx.Done():
				return
			case tokenChan <- TokenDelta{Token: delta.Token, Index: delta.Sequence}:
			}
			tokenCount++
			if maxOutputTokens > 0 && tokenCount >= maxOutputTokens {
				genCancel()
				break
			}
		}

		// Drain remaining tokens after cancel.
		for range outputChan {
		}

		for err := range ortErrChan {
			if err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errChan <- err:
				default:
				}
			}
		}
	}()

	return tokenChan, errChan, nil
}

// Close releases resources.
func (g *EngineGenerator) Close() error {
	g.logger.Info("Closing EngineGenerator")
	if g.engine != nil {
		g.engine.Destroy()
		g.engine = nil
	}
	g.fallbackMu.Lock()
	defer g.fallbackMu.Unlock()
	if g.fallbackSession != nil {
		g.fallbackSession.Close()
		g.fallbackSession = nil
	}
	return nil
}

// getFallbackSessionLocked lazily creates and returns the fallback GenerativeSession.
// The caller must hold fallbackMu.
func (g *EngineGenerator) getFallbackSessionLocked() (backends.GenerativeSession, error) {
	g.fallbackOnce.Do(func() {
		g.logger.Info("Lazily creating fallback session for multimodal requests")
		g.fallbackSession, g.fallbackErr = g.fallbackFactory.CreateGenerativeSession(g.modelPath)
	})
	return g.fallbackSession, g.fallbackErr
}

// generateWithFallback delegates a multimodal Generate call to the fallback session.
// Holds fallbackMu for the full duration to prevent Close() from destroying the session mid-call.
func (g *EngineGenerator) generateWithFallback(ctx context.Context, messages []Message, opts GenerateOptions) (*GenerateResult, error) {
	g.fallbackMu.Lock()
	defer g.fallbackMu.Unlock()

	session, err := g.getFallbackSessionLocked()
	if err != nil {
		return nil, fmt.Errorf("creating fallback session: %w", err)
	}

	backendMsgs := toBackendMessages(messages)
	backendOpts := toBackendOptions(opts)

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

// generateStreamWithFallback delegates a multimodal GenerateStream call to the fallback session.
// Holds fallbackMu until the stream completes to prevent Close() from destroying the session.
func (g *EngineGenerator) generateStreamWithFallback(ctx context.Context, messages []Message, opts GenerateOptions) (<-chan TokenDelta, <-chan error, error) {
	g.fallbackMu.Lock()

	session, err := g.getFallbackSessionLocked()
	if err != nil {
		g.fallbackMu.Unlock()
		return nil, nil, fmt.Errorf("creating fallback session: %w", err)
	}

	backendMsgs := toBackendMessages(messages)
	backendOpts := toBackendOptions(opts)

	backendTokenChan, backendErrChan, err := session.GenerateStream(ctx, backendMsgs, backendOpts)
	if err != nil {
		g.fallbackMu.Unlock()
		return nil, nil, err
	}

	// Release fallbackMu when the stream goroutine completes.
	tokenChan, errChan := adaptBackendStream(ctx, backendTokenChan, backendErrChan, g.fallbackMu.Unlock)
	return tokenChan, errChan, nil
}

// toOrtgenaiMessagesFromGen converts generation.Message to ortgenai.Message directly.
func toOrtgenaiMessagesFromGen(messages []Message) []ortgenai.Message {
	result := make([]ortgenai.Message, len(messages))
	for i, m := range messages {
		result[i] = ortgenai.Message{
			Role:    m.Role,
			Content: m.GetTextContent(),
		}
	}
	return result
}
