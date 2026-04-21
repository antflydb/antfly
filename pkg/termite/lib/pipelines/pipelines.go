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

// Package pipelines provides the Pipeline type that pairs a tokenizer with a model
// for end-to-end inference. Pipelines handle text encoding, padding, truncation,
// and model inference.
package pipelines

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"
)

// PaddingStrategy specifies how to pad sequences.
type PaddingStrategy string

const (
	// PaddingNone disables padding (sequences must be same length).
	PaddingNone PaddingStrategy = "none"
	// PaddingLongest pads to the longest sequence in the batch.
	PaddingLongest PaddingStrategy = "longest"
	// PaddingMaxLength pads to the configured max length.
	PaddingMaxLength PaddingStrategy = "max_length"
)

// TruncationStrategy specifies how to truncate sequences.
type TruncationStrategy string

const (
	// TruncationNone disables truncation (will error if too long).
	TruncationNone TruncationStrategy = "none"
	// TruncationLongestFirst truncates the longest sequence first (for pairs).
	TruncationLongestFirst TruncationStrategy = "longest_first"
	// TruncationOnlyFirst truncates only the first sequence.
	TruncationOnlyFirst TruncationStrategy = "only_first"
	// TruncationOnlySecond truncates only the second sequence.
	TruncationOnlySecond TruncationStrategy = "only_second"
)

// PipelineConfig holds configuration for a Pipeline.
type PipelineConfig struct {
	// MaxLength is the maximum sequence length.
	MaxLength int

	// Padding specifies the padding strategy.
	Padding PaddingStrategy

	// Truncation specifies the truncation strategy.
	Truncation TruncationStrategy

	// PadTokenID is the token ID used for padding.
	// If zero, attempts to get from tokenizer.
	PadTokenID int32

	// AddSpecialTokens controls whether to add [CLS], [SEP], etc.
	AddSpecialTokens bool
}

// DefaultPipelineConfig returns a PipelineConfig with sensible defaults.
func DefaultPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		MaxLength:        512,
		Padding:          PaddingLongest,
		Truncation:       TruncationLongestFirst,
		AddSpecialTokens: true,
	}
}

// PipelineOption is a functional option for configuring a Pipeline.
type PipelineOption func(*PipelineConfig)

// WithMaxLength sets the maximum sequence length.
func WithMaxLength(length int) PipelineOption {
	return func(c *PipelineConfig) {
		c.MaxLength = length
	}
}

// WithPadding sets the padding strategy.
func WithPadding(strategy PaddingStrategy) PipelineOption {
	return func(c *PipelineConfig) {
		c.Padding = strategy
	}
}

// WithTruncation sets the truncation strategy.
func WithTruncation(strategy TruncationStrategy) PipelineOption {
	return func(c *PipelineConfig) {
		c.Truncation = strategy
	}
}

// WithPadTokenID sets the padding token ID.
func WithPadTokenID(id int32) PipelineOption {
	return func(c *PipelineConfig) {
		c.PadTokenID = id
	}
}

// WithAddSpecialTokens controls whether to add special tokens.
func WithAddSpecialTokens(add bool) PipelineOption {
	return func(c *PipelineConfig) {
		c.AddSpecialTokens = add
	}
}

// Pipeline pairs a tokenizer with a model for end-to-end inference.
// It handles text encoding, padding, truncation, and model execution.
type Pipeline struct {
	// Tokenizer handles text-to-token conversion.
	Tokenizer tokenizers.Tokenizer

	// Model performs inference on tokenized inputs.
	Model backends.Model

	// Config holds pipeline configuration.
	Config *PipelineConfig
}

// New creates a new Pipeline with the given tokenizer and model.
func New(tokenizer tokenizers.Tokenizer, model backends.Model, opts ...PipelineOption) *Pipeline {
	config := DefaultPipelineConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Try to get pad token from tokenizer if not set
	if config.PadTokenID == 0 {
		if padID, err := tokenizer.SpecialTokenID(tokenizers.TokPad); err == nil {
			config.PadTokenID = int32(padID)
		}
	}

	return &Pipeline{
		Tokenizer: tokenizer,
		Model:     model,
		Config:    config,
	}
}

// TokenSpan represents the byte span of a token in the original text.
type TokenSpan = tokenizers.TokenSpan

// EncodedBatch holds the result of encoding a batch of texts.
type EncodedBatch struct {
	// InputIDs contains token IDs [batch, seq].
	InputIDs [][]int32

	// AttentionMask contains attention mask [batch, seq].
	AttentionMask [][]int32

	// TokenTypeIDs contains token type IDs [batch, seq] (optional).
	TokenTypeIDs [][]int32

	// OriginalLengths contains the original (pre-padding) length of each sequence.
	OriginalLengths []int

	// Spans contains byte spans for each token [batch, seq] (optional).
	// Only populated when using EncodeWithSpans.
	Spans [][]TokenSpan
}

// Encode tokenizes and encodes a batch of texts.
// The result is padded and truncated according to the pipeline config.
func (p *Pipeline) Encode(texts []string) (*EncodedBatch, error) {
	if len(texts) == 0 {
		return &EncodedBatch{}, nil
	}

	// Tokenize all texts
	var allTokens [][]int
	maxLen := 0
	for _, text := range texts {
		tokens := p.Tokenizer.Encode(text)
		allTokens = append(allTokens, tokens)
		if len(tokens) > maxLen {
			maxLen = len(tokens)
		}
	}

	// Determine target length
	targetLen := maxLen
	switch p.Config.Padding {
	case PaddingMaxLength:
		targetLen = p.Config.MaxLength
	case PaddingLongest:
		// Keep maxLen
	case PaddingNone:
		// No padding; all sequences must be same length
		for _, tokens := range allTokens {
			if len(tokens) != maxLen {
				return nil, fmt.Errorf("padding=none but sequences have different lengths")
			}
		}
	}

	// Apply truncation
	if p.Config.Truncation != TruncationNone && targetLen > p.Config.MaxLength {
		targetLen = p.Config.MaxLength
	}

	// Build output arrays
	batch := &EncodedBatch{
		InputIDs:        make([][]int32, len(texts)),
		AttentionMask:   make([][]int32, len(texts)),
		OriginalLengths: make([]int, len(texts)),
	}

	for i, tokens := range allTokens {
		batch.OriginalLengths[i] = len(tokens)

		// Truncate if needed
		if len(tokens) > targetLen {
			tokens = tokens[:targetLen]
		}

		// Convert to int32 and pad
		inputIDs := make([]int32, targetLen)
		attentionMask := make([]int32, targetLen)

		for j, tok := range tokens {
			inputIDs[j] = int32(tok)
			attentionMask[j] = 1
		}

		// Pad remaining positions
		for j := len(tokens); j < targetLen; j++ {
			inputIDs[j] = p.Config.PadTokenID
			attentionMask[j] = 0
		}

		batch.InputIDs[i] = inputIDs
		batch.AttentionMask[i] = attentionMask
	}

	return batch, nil
}

// EncodeWithSpans tokenizes texts and returns token IDs with byte spans.
// This is useful for token classification tasks where you need to map predictions
// back to character positions in the original text.
func (p *Pipeline) EncodeWithSpans(texts []string) (*EncodedBatch, error) {
	if len(texts) == 0 {
		return &EncodedBatch{}, nil
	}

	// Check if tokenizer supports spans
	tokWithSpans, hasSpans := p.Tokenizer.(tokenizers.TokenizerWithSpans)

	// Tokenize all texts
	type tokenResult struct {
		ids   []int
		spans []TokenSpan
	}
	var allResults []tokenResult
	maxLen := 0

	for _, text := range texts {
		var result tokenResult
		if hasSpans {
			encoded := tokWithSpans.EncodeWithSpans(text)
			result.ids = encoded.IDs
			result.spans = encoded.Spans
		} else {
			result.ids = p.Tokenizer.Encode(text)
			// No spans available - leave nil
		}
		allResults = append(allResults, result)
		if len(result.ids) > maxLen {
			maxLen = len(result.ids)
		}
	}

	// Determine target length
	targetLen := maxLen
	switch p.Config.Padding {
	case PaddingMaxLength:
		targetLen = p.Config.MaxLength
	case PaddingLongest:
		// Keep maxLen
	case PaddingNone:
		for _, result := range allResults {
			if len(result.ids) != maxLen {
				return nil, fmt.Errorf("padding=none but sequences have different lengths")
			}
		}
	}

	// Apply truncation
	if p.Config.Truncation != TruncationNone && targetLen > p.Config.MaxLength {
		targetLen = p.Config.MaxLength
	}

	// Build output arrays
	batch := &EncodedBatch{
		InputIDs:        make([][]int32, len(texts)),
		AttentionMask:   make([][]int32, len(texts)),
		OriginalLengths: make([]int, len(texts)),
	}
	if hasSpans {
		batch.Spans = make([][]TokenSpan, len(texts))
	}

	for i, result := range allResults {
		batch.OriginalLengths[i] = len(result.ids)

		tokens := result.ids
		spans := result.spans

		// Truncate if needed
		if len(tokens) > targetLen {
			tokens = tokens[:targetLen]
			if spans != nil {
				spans = spans[:targetLen]
			}
		}

		// Convert to int32 and pad
		inputIDs := make([]int32, targetLen)
		attentionMask := make([]int32, targetLen)

		for j, tok := range tokens {
			inputIDs[j] = int32(tok)
			attentionMask[j] = 1
		}

		// Pad remaining positions
		for j := len(tokens); j < targetLen; j++ {
			inputIDs[j] = p.Config.PadTokenID
			attentionMask[j] = 0
		}

		batch.InputIDs[i] = inputIDs
		batch.AttentionMask[i] = attentionMask

		// Handle spans
		if hasSpans && spans != nil {
			paddedSpans := make([]TokenSpan, targetLen)
			copy(paddedSpans, spans)
			// Padding tokens get zero spans (already zeroed by make)
			batch.Spans[i] = paddedSpans
		}
	}

	return batch, nil
}

// Forward runs model inference on raw text inputs.
// This is a convenience method that combines Encode and model.Forward.
func (p *Pipeline) Forward(ctx context.Context, texts []string) (*backends.ModelOutput, error) {
	batch, err := p.Encode(texts)
	if err != nil {
		return nil, fmt.Errorf("encoding texts: %w", err)
	}

	inputs := &backends.ModelInputs{
		InputIDs:      batch.InputIDs,
		AttentionMask: batch.AttentionMask,
		TokenTypeIDs:  batch.TokenTypeIDs,
	}

	return p.Model.Forward(ctx, inputs)
}

// TokenCount returns the number of tokens in a text.
// Useful for estimating costs or checking length constraints.
func (p *Pipeline) TokenCount(text string) int {
	return len(p.Tokenizer.Encode(text))
}

// Close releases resources held by the pipeline.
func (p *Pipeline) Close() error {
	return p.Model.Close()
}

// EncodePair tokenizes a pair of texts (e.g., query and document for reranking).
// The texts are concatenated with appropriate separators.
func (p *Pipeline) EncodePair(text1, text2 string) (*EncodedBatch, error) {
	// Get special token IDs
	// TokEndOfSentence is used for [SEP] in BERT-style models
	sepID, err := p.Tokenizer.SpecialTokenID(tokenizers.TokEndOfSentence)
	if err != nil {
		return nil, fmt.Errorf("getting SEP token: %w", err)
	}
	// TokClassification is used for [CLS] in BERT-style models
	clsID, err := p.Tokenizer.SpecialTokenID(tokenizers.TokClassification)
	if err != nil {
		return nil, fmt.Errorf("getting CLS token: %w", err)
	}

	// Tokenize both texts without post-processing — we add [CLS]/[SEP] manually below.
	tokens1 := p.Tokenizer.EncodeWithOptions(text1, false)
	tokens2 := p.Tokenizer.EncodeWithOptions(text2, false)

	// Combine: [CLS] tokens1 [SEP] tokens2 [SEP]
	combined := make([]int, 0, len(tokens1)+len(tokens2)+3)
	if p.Config.AddSpecialTokens {
		combined = append(combined, clsID)
	}
	combined = append(combined, tokens1...)
	if p.Config.AddSpecialTokens {
		combined = append(combined, sepID)
	}
	combined = append(combined, tokens2...)
	if p.Config.AddSpecialTokens {
		combined = append(combined, sepID)
	}

	// Truncate if needed
	if len(combined) > p.Config.MaxLength {
		combined = combined[:p.Config.MaxLength]
	}

	// Build token type IDs: 0 for first segment, 1 for second
	tokenTypeIDs := make([]int32, len(combined))
	inSecond := false
	sepCount := 0
	for i := range combined {
		if combined[i] == sepID {
			sepCount++
			if sepCount == 1 {
				inSecond = true
			}
		}
		if inSecond {
			tokenTypeIDs[i] = 1
		}
	}

	// Convert to int32
	inputIDs := make([]int32, len(combined))
	attentionMask := make([]int32, len(combined))
	for i, tok := range combined {
		inputIDs[i] = int32(tok)
		attentionMask[i] = 1
	}

	return &EncodedBatch{
		InputIDs:        [][]int32{inputIDs},
		AttentionMask:   [][]int32{attentionMask},
		TokenTypeIDs:    [][]int32{tokenTypeIDs},
		OriginalLengths: []int{len(combined)},
	}, nil
}

// EncodePairs tokenizes multiple text pairs (batch of query-document pairs).
func (p *Pipeline) EncodePairs(pairs [][2]string) (*EncodedBatch, error) {
	if len(pairs) == 0 {
		return &EncodedBatch{}, nil
	}

	// Encode each pair
	var batches []*EncodedBatch
	maxLen := 0
	for _, pair := range pairs {
		batch, err := p.EncodePair(pair[0], pair[1])
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
		if len(batch.InputIDs[0]) > maxLen {
			maxLen = len(batch.InputIDs[0])
		}
	}

	// Combine and pad to same length
	result := &EncodedBatch{
		InputIDs:        make([][]int32, len(pairs)),
		AttentionMask:   make([][]int32, len(pairs)),
		TokenTypeIDs:    make([][]int32, len(pairs)),
		OriginalLengths: make([]int, len(pairs)),
	}

	for i, batch := range batches {
		seqLen := len(batch.InputIDs[0])
		result.OriginalLengths[i] = seqLen

		// Pad to maxLen
		inputIDs := make([]int32, maxLen)
		attentionMask := make([]int32, maxLen)
		tokenTypeIDs := make([]int32, maxLen)

		copy(inputIDs, batch.InputIDs[0])
		copy(attentionMask, batch.AttentionMask[0])
		copy(tokenTypeIDs, batch.TokenTypeIDs[0])

		// Pad remaining
		for j := seqLen; j < maxLen; j++ {
			inputIDs[j] = p.Config.PadTokenID
			// attentionMask and tokenTypeIDs remain 0
		}

		result.InputIDs[i] = inputIDs
		result.AttentionMask[i] = attentionMask
		result.TokenTypeIDs[i] = tokenTypeIDs
	}

	return result, nil
}
