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
	"fmt"
	"math"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// EncoderDecoderResult holds the result of encoder-decoder generation.
// This is a unified result type for both Vision2Seq and Seq2Seq pipelines.
type EncoderDecoderResult struct {
	// Text is the generated text.
	Text string

	// TokenIDs are the generated token IDs.
	TokenIDs []int32

	// TokenCount is the number of tokens generated.
	TokenCount int

	// StoppedAtEOS indicates whether generation stopped due to EOS token.
	StoppedAtEOS bool

	// Score is the generation confidence score (probability).
	// Computed as exp(LogProb / TokenCount) to get average per-token probability.
	Score float32

	// LogProb is the cumulative log probability of the generated sequence.
	LogProb float64
}

// EncoderDecoderPipeline provides shared functionality for encoder-decoder models.
// It is embedded by Vision2SeqPipeline and Seq2SeqPipeline.
type EncoderDecoderPipeline struct {
	// Model is the encoder-decoder model (implements backends.Model).
	Model backends.Model

	// Tokenizer handles text encoding/decoding.
	Tokenizer tokenizers.Tokenizer

	// Generator handles the autoregressive generation loop.
	Generator *Generator

	// GenerationConfig holds generation parameters.
	GenerationConfig *backends.GenerationConfig

	// DecoderConfig holds decoder configuration.
	DecoderConfig *backends.DecoderConfig
}

// NewEncoderDecoderPipeline creates a new EncoderDecoderPipeline with resolved configs.
func NewEncoderDecoderPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	genConfig *backends.GenerationConfig,
) *EncoderDecoderPipeline {
	genConfig = ResolveGenerationConfig(genConfig)
	decoderConfig := ResolveDecoderConfig(model)
	generator := NewGeneratorFromDecoderConfig(genConfig, decoderConfig)

	return &EncoderDecoderPipeline{
		Model:            model,
		Tokenizer:        tokenizer,
		Generator:        generator,
		GenerationConfig: genConfig,
		DecoderConfig:    decoderConfig,
	}
}

// GenerateFromEncoderOutput performs text generation given encoder output.
// This is the core generation method shared by Vision2Seq and Seq2Seq pipelines.
func (p *EncoderDecoderPipeline) GenerateFromEncoderOutput(
	ctx context.Context,
	encoderOutput *backends.EncoderOutput,
	startTokens []int32,
) (*EncoderDecoderResult, error) {
	// Create decoder step function using encoder output
	stepFn := p.makeDecoderStepFunc(encoderOutput)

	// Generate using the shared generator
	result, err := p.Generator.Generate(ctx, startTokens, stepFn, true)
	if err != nil {
		return nil, fmt.Errorf("generating text: %w", err)
	}

	// Decode tokens to text
	text := p.Tokenizer.Decode(Int32ToInt(result.TokenIDs))

	// Compute score as average per-token probability
	// Score = exp(totalLogProb / numTokens)
	var score float32
	tokenCount := len(result.TokenIDs)
	if tokenCount > 0 {
		avgLogProb := result.LogProb / float64(tokenCount)
		score = float32(math.Exp(avgLogProb))
	}

	return &EncoderDecoderResult{
		Text:         text,
		TokenIDs:     result.TokenIDs,
		TokenCount:   tokenCount,
		StoppedAtEOS: result.StoppedAtEOS,
		Score:        score,
		LogProb:      result.LogProb,
	}, nil
}

// GenerateFromEncoderOutputStreaming performs streaming text generation.
// The callback is called for each generated token. Return false to stop generation.
func (p *EncoderDecoderPipeline) GenerateFromEncoderOutputStreaming(
	ctx context.Context,
	encoderOutput *backends.EncoderOutput,
	startTokens []int32,
	callback func(token int32, text string) bool,
) (*EncoderDecoderResult, error) {
	// Create decoder step function using encoder output
	stepFn := p.makeDecoderStepFunc(encoderOutput)

	// Wrap callback to decode token to text
	wrappedCallback := func(token int32) bool {
		text := p.Tokenizer.Decode([]int{int(token)})
		return callback(token, text)
	}

	// Generate with streaming
	result, err := p.Generator.GenerateStreaming(ctx, startTokens, stepFn, true, wrappedCallback)
	if err != nil {
		return nil, fmt.Errorf("generating text: %w", err)
	}

	// Decode all tokens to final text
	text := p.Tokenizer.Decode(Int32ToInt(result.TokenIDs))

	return &EncoderDecoderResult{
		Text:         text,
		TokenIDs:     result.TokenIDs,
		TokenCount:   len(result.TokenIDs),
		StoppedAtEOS: result.StoppedAtEOS,
	}, nil
}

// makeDecoderStepFunc creates a DecoderStepFunc that uses the encoder output.
func (p *EncoderDecoderPipeline) makeDecoderStepFunc(encoderOutput *backends.EncoderOutput) DecoderStepFunc {
	return func(ctx context.Context, state *DecoderState) ([]float32, *backends.KVCache, error) {
		output, err := p.Model.Forward(ctx, &backends.ModelInputs{
			EncoderOutput: encoderOutput,
			InputIDs:      [][]int32{state.InputIDs},
			PastKeyValues: state.KVCache,
		})
		if err != nil {
			return nil, nil, err
		}
		return output.Logits[0], output.PastKeyValues, nil
	}
}

// GetStartTokens returns the start tokens for decoding.
// If a prompt is provided, it tokenizes the prompt.
// Otherwise, it returns the decoder start token.
func (p *EncoderDecoderPipeline) GetStartTokens(prompt string) []int32 {
	if prompt != "" {
		return IntToInt32(p.Tokenizer.Encode(prompt))
	}
	return []int32{p.DecoderConfig.DecoderStartTokenID}
}

// Close releases resources held by the pipeline.
func (p *EncoderDecoderPipeline) Close() error {
	return p.Model.Close()
}

// SetDebug enables or disables debug output during generation.
func (p *EncoderDecoderPipeline) SetDebug(debug bool) {
	p.Generator.Debug = debug
}
