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

package chunking

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"
)

// Fixed chunker model names
const (
	// ModelFixedBert uses BERT's WordPiece tokenization (~30k vocab).
	// Good for general-purpose text and multilingual content.
	ModelFixedBert = "fixed-bert-tokenizer"

	// ModelFixedBPE uses OpenAI's tiktoken BPE tokenization (cl100k_base, ~100k vocab).
	// Good for GPT-style models and code.
	ModelFixedBPE = "fixed-bpe-tokenizer"
)

// Ensure FixedChunker implements the Chunker interface
var _ chunking.Chunker = (*FixedChunker)(nil)

// positionedSection tracks a text section along with its byte offset in the
// original input text. Threading offsets through the splitting pipeline ensures
// that chunk StartChar/EndChar values always satisfy:
//
//	originalText[StartChar:EndChar] == chunk.Text
type positionedSection struct {
	text   string
	start  int // byte offset in original text
	tokens int // cached token count (0 = not yet computed)
}

// FixedChunkerConfig contains configuration for the fixed chunker.
type FixedChunkerConfig struct {
	// Model specifies which tokenizer to use (ModelFixedBert or ModelFixedBPE)
	Model string

	// TargetTokens is the target number of tokens per chunk
	TargetTokens int

	// OverlapTokens is the number of overlapping tokens between chunks
	OverlapTokens int

	// Separator is the text separator for splitting
	Separator string

	// MaxChunks is the maximum number of chunks to generate
	MaxChunks int
}

// DefaultFixedChunkerConfig returns sensible defaults for the fixed chunker.
func DefaultFixedChunkerConfig() FixedChunkerConfig {
	return FixedChunkerConfig{
		Model:         ModelFixedBert,
		TargetTokens:  500,
		OverlapTokens: 50,
		Separator:     "\n\n",
		MaxChunks:     50,
	}
}

// FixedChunker splits text into fixed-size chunks
// while respecting token count targets
type FixedChunker struct {
	config    FixedChunkerConfig
	tokenizer tokenizers.TokenCounter
}

// NewFixedChunker creates a chunker that splits text into fixed-size chunks.
// Supported models:
// - "fixed-bert-tokenizer": BERT WordPiece tokenization (~30k vocab)
// - "fixed-bpe-tokenizer": OpenAI tiktoken BPE tokenization (cl100k_base, ~100k vocab)
func NewFixedChunker(config FixedChunkerConfig) (*FixedChunker, error) {
	// Apply defaults for zero values
	if config.Model == "" {
		config.Model = ModelFixedBert
	}
	if config.TargetTokens <= 0 {
		config.TargetTokens = 500
	}
	if config.OverlapTokens < 0 {
		config.OverlapTokens = 50
	}
	if config.Separator == "" {
		config.Separator = "\n\n"
	}
	if config.MaxChunks <= 0 {
		config.MaxChunks = 50
	}

	// Validate config
	if config.OverlapTokens >= config.TargetTokens {
		return nil, errors.New("overlap_tokens must be less than target_tokens")
	}

	// Create tokenizer
	tk, err := tokenizers.NewTokenCounter()
	if err != nil {
		return nil, fmt.Errorf("failed to create token counter: %w", err)
	}

	return &FixedChunker{
		config:    config,
		tokenizer: tk,
	}, nil
}

// splitWithPositions splits text by sep, returning positionedSections whose
// start fields are byte offsets relative to baseOffset in the original text.
// For ". " splitting, the period is kept with the preceding section.
func splitWithPositions(text string, sep string, baseOffset int) []positionedSection {
	if text == "" {
		return nil
	}

	isSentenceSplit := sep == ". "

	parts := strings.Split(text, sep)
	sections := make([]positionedSection, 0, len(parts))
	pos := baseOffset

	for i, part := range parts {
		sectionText := part
		if isSentenceSplit && i < len(parts)-1 {
			sectionText = part + "."
		}

		sections = append(sections, positionedSection{
			text:  sectionText,
			start: pos,
		})

		// Advance pos past this part + separator
		pos += len(part)
		if i < len(parts)-1 {
			pos += len(sep)
		}
	}

	return sections
}

// countTokens returns the token count for a section, using the cached value
// if available.
func (s *FixedChunker) countTokens(sec *positionedSection) int {
	if sec.tokens == 0 && sec.text != "" {
		sec.tokens = s.tokenizer.CountTokens(sec.text)
	}
	return sec.tokens
}

// Chunk splits text into chunks with per-request config overrides.
func (s *FixedChunker) Chunk(ctx context.Context, text string, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	if text == "" {
		return nil, nil
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Resolve effective config by applying overrides (zero values mean "use default")
	effectiveConfig := s.config
	if opts.MaxChunks != 0 {
		effectiveConfig.MaxChunks = opts.MaxChunks
	}
	if opts.Text.TargetTokens != 0 {
		effectiveConfig.TargetTokens = opts.Text.TargetTokens
	}
	if opts.Text.OverlapTokens != 0 {
		effectiveConfig.OverlapTokens = opts.Text.OverlapTokens
	}
	if opts.Text.Separator != "" {
		effectiveConfig.Separator = opts.Text.Separator
	}
	// Note: Threshold is not applicable to FixedChunker (only used by ONNX models)

	// Split on separator to get candidate sections
	sections := splitWithPositions(text, effectiveConfig.Separator, 0)

	// If splitting on the separator didn't produce multiple sections,
	// try progressively less strict separators.
	// Skip "\n" fallback when the primary separator is already "\n".
	if len(sections) <= 1 {
		if effectiveConfig.Separator != "\n" {
			sections = splitWithPositions(text, "\n", 0)
		}
		if len(sections) <= 1 {
			sections = splitWithPositions(text, ". ", 0)
		}
	}

	if len(sections) == 0 {
		return nil, nil
	}

	// Flatten oversized sections: if any single section exceeds target tokens,
	// split it further using progressively finer separators
	sections = s.flattenOversizedSections(sections, effectiveConfig.TargetTokens)

	// Filter empty sections after trimming and compute token counts
	filtered := make([]positionedSection, 0, len(sections))
	for i := range sections {
		trimmed := strings.TrimSpace(sections[i].text)
		if trimmed == "" {
			continue
		}
		// Adjust start offset to account for leading whitespace trimmed
		leadingTrimmed := len(sections[i].text) - len(strings.TrimLeft(sections[i].text, " \t\n\r"))
		sections[i].text = trimmed
		sections[i].start += leadingTrimmed
		sections[i].tokens = 0 // reset cached count since text changed
		s.countTokens(&sections[i])
		filtered = append(filtered, sections[i])
	}
	sections = filtered

	chunks := make([]chunking.Chunk, 0)
	// Accumulate sections for the current chunk
	var currentSections []positionedSection
	currentTokens := 0
	previousChunkText := "" // For overlap

	for _, section := range sections {
		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		sectionTokens := section.tokens

		// Check if adding this section would exceed target
		if currentTokens > 0 && currentTokens+sectionTokens > effectiveConfig.TargetTokens {
			// Finalize current chunk from accumulated sections
			if len(currentSections) > 0 {
				startChar := currentSections[0].start
				lastSec := currentSections[len(currentSections)-1]
				endChar := lastSec.start + len(lastSec.text)
				chunkText := strings.TrimSpace(text[startChar:endChar])

				// Adjust startChar if trimming removed leading whitespace
				actualStart := strings.Index(text[startChar:endChar], chunkText)
				if actualStart > 0 {
					startChar += actualStart
				}
				endChar = startChar + len(chunkText)

				chunks = append(chunks, chunking.NewTextChunk(
					uint32(len(chunks)),
					chunkText,
					startChar,
					endChar,
				))

				// Check max chunks limit
				if len(chunks) >= effectiveConfig.MaxChunks {
					break
				}

				previousChunkText = chunkText
			}

			// Start new chunk
			currentSections = nil
			currentTokens = 0

			// Add overlap from previous chunk if configured
			if effectiveConfig.OverlapTokens > 0 && previousChunkText != "" {
				overlapText := s.extractOverlap(previousChunkText, effectiveConfig.OverlapTokens)
				if overlapText != "" {
					currentTokens = s.tokenizer.CountTokens(overlapText)
				}
			}
		}

		currentSections = append(currentSections, section)
		currentTokens += sectionTokens
	}

	// Add final chunk
	if len(currentSections) > 0 && len(chunks) < effectiveConfig.MaxChunks {
		startChar := currentSections[0].start
		lastSec := currentSections[len(currentSections)-1]
		endChar := lastSec.start + len(lastSec.text)
		chunkText := strings.TrimSpace(text[startChar:endChar])

		actualStart := strings.Index(text[startChar:endChar], chunkText)
		if actualStart > 0 {
			startChar += actualStart
		}
		endChar = startChar + len(chunkText)

		chunks = append(chunks, chunking.NewTextChunk(
			uint32(len(chunks)),
			chunkText,
			startChar,
			endChar,
		))
	}

	// If no chunks were created (text too short), return single chunk
	if len(chunks) == 0 {
		trimmed := strings.TrimSpace(text)
		startChar := strings.Index(text, trimmed)
		chunks = append(chunks, chunking.NewTextChunk(
			0,
			trimmed,
			startChar,
			startChar+len(trimmed),
		))
	}

	return chunks, nil
}

// flattenOversizedSections splits any sections that exceed targetTokens into
// smaller pieces using progressively finer separators ("\n", ". "), and as a
// last resort, force-splits on word boundaries by token count.
func (s *FixedChunker) flattenOversizedSections(sections []positionedSection, targetTokens int) []positionedSection {
	result := make([]positionedSection, 0, len(sections))
	for i := range sections {
		if s.countTokens(&sections[i]) <= targetTokens {
			result = append(result, sections[i])
			continue
		}

		// Try splitting on single newlines
		subs := splitWithPositions(sections[i].text, "\n", sections[i].start)
		if len(subs) <= 1 {
			// Try splitting on sentences
			subs = splitWithPositions(sections[i].text, ". ", sections[i].start)
		} else {
			// Newline split produced multiple subs — check if any are still
			// oversized and try sentence splitting on those (cascade fix).
			var expanded []positionedSection
			for j := range subs {
				if s.countTokens(&subs[j]) <= targetTokens {
					expanded = append(expanded, subs[j])
				} else {
					sentSubs := splitWithPositions(subs[j].text, ". ", subs[j].start)
					if len(sentSubs) > 1 {
						expanded = append(expanded, sentSubs...)
					} else {
						expanded = append(expanded, subs[j])
					}
				}
			}
			subs = expanded
		}

		for j := range subs {
			trimmed := strings.TrimSpace(subs[j].text)
			if trimmed == "" {
				continue
			}
			subs[j].text = trimmed
			subs[j].tokens = 0

			if s.countTokens(&subs[j]) <= targetTokens {
				result = append(result, subs[j])
			} else {
				// Force-split on word boundaries
				result = append(result, s.splitByTokenCount(subs[j], targetTokens)...)
			}
		}
	}
	return result
}

// splitByTokenCount splits a section on word boundaries into pieces that each
// fit within targetTokens. Each returned piece preserves its byte offset in the
// original text.
func (s *FixedChunker) splitByTokenCount(sec positionedSection, targetTokens int) []positionedSection {
	text := sec.text
	if text == "" {
		return nil
	}

	var result []positionedSection
	// Scan through the text finding word boundaries
	i := 0
	pieceStart := 0
	currentTokens := 0

	for i < len(text) {
		// Skip whitespace
		for i < len(text) && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n' || text[i] == '\r') {
			i++
		}
		if i >= len(text) {
			break
		}

		// Find end of word
		wordStart := i
		for i < len(text) && text[i] != ' ' && text[i] != '\t' && text[i] != '\n' && text[i] != '\r' {
			i++
		}
		word := text[wordStart:i]
		wordTokens := s.tokenizer.CountTokens(word)

		if currentTokens > 0 && currentTokens+wordTokens > targetTokens {
			// Emit current piece
			pieceText := strings.TrimSpace(text[pieceStart:wordStart])
			if pieceText != "" {
				result = append(result, positionedSection{
					text:   pieceText,
					start:  sec.start + pieceStart,
					tokens: currentTokens,
				})
			}
			pieceStart = wordStart
			currentTokens = 0
		}
		currentTokens += wordTokens
	}

	// Emit last piece
	pieceText := strings.TrimSpace(text[pieceStart:])
	if pieceText != "" {
		result = append(result, positionedSection{
			text:   pieceText,
			start:  sec.start + pieceStart,
			tokens: currentTokens,
		})
	}

	return result
}

// extractOverlap extracts the last N tokens from text for overlap
func (s *FixedChunker) extractOverlap(text string, targetTokens int) string {
	if text == "" || targetTokens <= 0 {
		return ""
	}

	// Fallback: take last ~targetTokens*4 characters
	overlapChars := targetTokens * 4
	if len(text) <= overlapChars {
		return text
	}
	cutPoint := len(text) - overlapChars
	// Advance to the next valid UTF-8 rune boundary
	for cutPoint < len(text) && !utf8.RuneStart(text[cutPoint]) {
		cutPoint++
	}
	return text[cutPoint:]
}

// Close releases tokenizer resources
func (s *FixedChunker) Close() error {
	// Tokenizer doesn't need explicit closing
	return nil
}
