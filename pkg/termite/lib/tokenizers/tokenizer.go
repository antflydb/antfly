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

// Package tokenizer provides token counting and tokenization for text processing.
//
// It consolidates all tokenizer functionality: simple token counting (for chunking/pruning)
// and full tokenization (Encode/Decode/SpecialTokenID) for ML pipelines.
//
// The package re-exports key types from go-huggingface/tokenizers so that callers
// don't need to import the upstream library directly.
package tokenizers

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	esentencepiece "github.com/eliben/go-sentencepiece"
	"github.com/gomlx/go-huggingface/tokenizers"
	"github.com/gomlx/go-huggingface/tokenizers/api"
	"github.com/gomlx/go-huggingface/tokenizers/hftokenizer"
)

// Re-export key types from go-huggingface/tokenizers so pipeline code
// can import this package instead of the upstream library directly.
type (
	// Tokenizer is the full tokenizer interface with Encode/Decode/SpecialTokenID.
	Tokenizer = tokenizers.Tokenizer

	// TokenizerWithSpans extends Tokenizer with span tracking capability.
	TokenizerWithSpans = tokenizers.TokenizerWithSpans

	// TokenSpan represents the byte span of a token in the original text.
	TokenSpan = api.TokenSpan

	// EncodingResult contains tokens with their spans.
	EncodingResult = api.EncodingResult

	// Config holds HuggingFace's tokenizer_config.json contents.
	Config = api.Config

	// SpecialToken is an enum of commonly used special tokens.
	SpecialToken = api.SpecialToken
)

// Re-export special token constants.
const (
	TokBeginningOfSentence = api.TokBeginningOfSentence
	TokEndOfSentence       = api.TokEndOfSentence
	TokUnknown             = api.TokUnknown
	TokPad                 = api.TokPad
	TokMask                = api.TokMask
	TokClassification      = api.TokClassification
)

// Re-export config parsing functions.
var (
	ParseConfigContent = api.ParseConfigContent
	ParseConfigFile    = api.ParseConfigFile
)

//go:embed tokenizer.json
var defaultTokenizerJSON []byte

// TokenCounter provides simple token counting for text chunking and pruning.
type TokenCounter interface {
	// CountTokens returns the number of tokens in the text.
	CountTokens(text string) int
}

// hfTokenCounter uses the go-huggingface pure Go tokenizer with embedded BERT base uncased tokenizer.json.
type hfTokenCounter struct {
	tok *hftokenizer.Tokenizer
}

// NewTokenCounter creates a TokenCounter using the embedded BERT base uncased tokenizer.
// When built with onnx+ORT tags, uses the fast Rust tokenizer; otherwise falls back to pure Go.
func NewTokenCounter() (TokenCounter, error) {
	// Try Rust tokenizer first (much faster when available)
	if tc, err := newRustTokenCounter(); tc != nil && err == nil {
		return tc, nil
	}

	// Fall back to pure Go tokenizer
	tok, err := hftokenizer.NewFromContent(nil, defaultTokenizerJSON)
	if err != nil {
		return nil, fmt.Errorf("loading embedded tokenizer: %w", err)
	}
	return &hfTokenCounter{tok: tok}, nil
}

// CountTokens returns the number of tokens in the text.
func (t *hfTokenCounter) CountTokens(text string) int {
	if text == "" {
		return 0
	}
	return len(t.tok.Encode(text))
}

// LoadTokenizer loads a tokenizer from a local model directory.
// It auto-detects the tokenizer type (HuggingFace tokenizer.json or SentencePiece tokenizer.model).
// When built with ONNX/ORT tags, it uses the fast Rust tokenizer; otherwise falls back to pure Go.
//
// Both the Rust and Go tokenizers recognize special/added tokens (like <bos>,
// <start_of_turn>) in input text by default — no special options are needed.
func LoadTokenizer(modelPath string) (Tokenizer, error) {
	// First, try to load tokenizer_config.json for class information
	var config *api.Config
	configPath := filepath.Join(modelPath, "tokenizer_config.json")
	if _, err := os.Stat(configPath); err == nil {
		// Normalize the config to handle HuggingFace AddedToken objects
		normalizedContent, err := normalizeTokenizerConfig(configPath)
		if err != nil {
			return nil, fmt.Errorf("normalizing tokenizer config: %w", err)
		}
		config, err = api.ParseConfigContent(normalizedContent)
		if err != nil {
			return nil, fmt.Errorf("parsing tokenizer config: %w", err)
		}
		config.ConfigFile = configPath
	}

	// Try tokenizer.json (HuggingFace Tokenizers format - BPE, WordPiece, etc.)
	tokenizerJSONPath := filepath.Join(modelPath, "tokenizer.json")
	if _, err := os.Stat(tokenizerJSONPath); err == nil {
		// Try Rust tokenizer first (much faster, available with ORT/XLA builds)
		if rustTokenizerAvailable() {
			if tok, err := loadRustTokenizer(modelPath, config); err == nil && tok != nil {
				return tok, nil
			}
			// Fall through to Go tokenizer if Rust fails
		}

		// Fall back to pure Go tokenizer
		tok, err := hftokenizer.NewFromFile(config, tokenizerJSONPath)
		if err != nil {
			return nil, fmt.Errorf("loading tokenizer.json: %w", err)
		}
		return tok, nil
	}

	// Try tokenizer.model (SentencePiece format)
	spModelPath := filepath.Join(modelPath, "tokenizer.model")
	if _, err := os.Stat(spModelPath); err == nil {
		proc, err := esentencepiece.NewProcessorFromPath(spModelPath)
		if err != nil {
			return nil, fmt.Errorf("loading tokenizer.model: %w", err)
		}
		return &sentencepieceTokenizer{
			Processor: proc,
			Info:      proc.ModelInfo(),
		}, nil
	}

	return nil, fmt.Errorf("no tokenizer found in %s (expected tokenizer.json or tokenizer.model)", modelPath)
}

// MustLoadTokenizer loads a tokenizer and panics on error.
// Useful for tests and initialization code.
func MustLoadTokenizer(modelPath string) Tokenizer {
	tok, err := LoadTokenizer(modelPath)
	if err != nil {
		panic(fmt.Sprintf("failed to load tokenizer: %v", err))
	}
	return tok
}

// sentencepieceTokenizer wraps esentencepiece.Processor to implement Tokenizer.
type sentencepieceTokenizer struct {
	*esentencepiece.Processor
	Info *esentencepiece.ModelInfo
}

// Ensure sentencepieceTokenizer implements Tokenizer
var _ Tokenizer = (*sentencepieceTokenizer)(nil)

// Encode returns the text encoded into a sequence of token IDs.
func (t *sentencepieceTokenizer) Encode(text string) []int {
	return t.EncodeWithOptions(text, true)
}

// EncodeWithOptions returns the text encoded into a sequence of token IDs.
// The addSpecialTokens parameter is accepted for interface compatibility but
// has no effect — SentencePiece models do not have a post-processor.
func (t *sentencepieceTokenizer) EncodeWithOptions(text string, _ bool) []int {
	tokens := t.Processor.Encode(text)
	result := make([]int, len(tokens))
	for i, tok := range tokens {
		result[i] = tok.ID
	}
	return result
}

// Decode returns the text from a sequence of token IDs.
func (t *sentencepieceTokenizer) Decode(ids []int) string {
	return t.Processor.Decode(ids)
}

// SpecialTokenID returns the ID for the given special token, or an error if unknown.
func (t *sentencepieceTokenizer) SpecialTokenID(token api.SpecialToken) (int, error) {
	switch token {
	case api.TokUnknown:
		return t.Info.UnknownID, nil
	case api.TokPad:
		return t.Info.PadID, nil
	case api.TokBeginningOfSentence:
		return t.Info.BeginningOfSentenceID, nil
	case api.TokEndOfSentence:
		return t.Info.EndOfSentenceID, nil
	default:
		return 0, fmt.Errorf("unknown special token: %s (%d)", token, int(token))
	}
}

// normalizeTokenizerConfig reads a tokenizer_config.json file and normalizes
// HuggingFace AddedToken objects to plain strings.
// Some HuggingFace models use {"__type": "AddedToken", "content": "<s>"} format
// instead of plain strings for special tokens.
func normalizeTokenizerConfig(configPath string) ([]byte, error) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	// Parse as generic JSON
	var raw map[string]any
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil, fmt.Errorf("parsing config JSON: %w", err)
	}

	// Token fields that might be AddedToken objects
	tokenFields := []string{
		"bos_token", "eos_token", "pad_token", "unk_token",
		"cls_token", "sep_token", "mask_token",
	}

	// Normalize each token field
	for _, field := range tokenFields {
		if val, ok := raw[field]; ok {
			raw[field] = extractTokenContent(val)
		}
	}

	// Re-serialize to JSON
	return json.Marshal(raw)
}

// extractTokenContent extracts the token string from either a plain string
// or a HuggingFace AddedToken object.
func extractTokenContent(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case map[string]any:
		// HuggingFace AddedToken format: {"__type": "AddedToken", "content": "<s>", ...}
		if content, ok := val["content"].(string); ok {
			return content
		}
	}
	return ""
}
