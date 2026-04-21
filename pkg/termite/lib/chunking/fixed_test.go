package chunking

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
)

func newTestChunker(t *testing.T, config FixedChunkerConfig) *FixedChunker {
	t.Helper()
	fc, err := NewFixedChunker(config)
	if err != nil {
		t.Fatalf("NewFixedChunker: %v", err)
	}
	return fc
}

func chunkTexts(chunks []chunking.Chunk) []string {
	texts := make([]string, len(chunks))
	for i, c := range chunks {
		texts[i] = c.GetText()
	}
	return texts
}

// sectionTexts extracts the text field from positioned sections.
func sectionTexts(sections []positionedSection) []string {
	texts := make([]string, len(sections))
	for i, s := range sections {
		texts[i] = s.text
	}
	return texts
}

// toPositionedSections converts plain strings into positionedSections with
// sequential offsets (no separator gaps). Useful for testing internal functions
// that now accept positionedSection slices.
func toPositionedSections(strs []string) []positionedSection {
	sections := make([]positionedSection, len(strs))
	offset := 0
	for i, s := range strs {
		sections[i] = positionedSection{text: s, start: offset}
		offset += len(s)
	}
	return sections
}

func TestFixedChunker_NewDefaults(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{})
	if fc.config.TargetTokens != 500 {
		t.Errorf("TargetTokens = %d, want 500", fc.config.TargetTokens)
	}
	// OverlapTokens=0 is valid (no overlap), default only applies for negative values
	if fc.config.OverlapTokens != 0 {
		t.Errorf("OverlapTokens = %d, want 0 (zero-value passes through)", fc.config.OverlapTokens)
	}
	if fc.config.MaxChunks != 50 {
		t.Errorf("MaxChunks = %d, want 50", fc.config.MaxChunks)
	}
	if fc.config.Separator != "\n\n" {
		t.Errorf("Separator = %q, want %q", fc.config.Separator, "\n\n")
	}
	if fc.config.Model != ModelFixedBert {
		t.Errorf("Model = %q, want %q", fc.config.Model, ModelFixedBert)
	}
}

func TestFixedChunker_InvalidConfig(t *testing.T) {
	_, err := NewFixedChunker(FixedChunkerConfig{
		TargetTokens:  10,
		OverlapTokens: 10, // must be less than target
	})
	if err == nil {
		t.Fatal("expected error for overlap >= target, got nil")
	}
}

func TestFixedChunker_EmptyText(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{})
	chunks, err := fc.Chunk(context.Background(), "", chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if chunks != nil {
		t.Errorf("expected nil chunks for empty text, got %d", len(chunks))
	}
}

func TestFixedChunker_ShortText(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})
	chunks, err := fc.Chunk(context.Background(), "Hello world.", chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1", len(chunks))
	}
	if text := chunks[0].GetText(); text != "Hello world." {
		t.Errorf("chunk text = %q, want %q", text, "Hello world.")
	}
}

func TestFixedChunker_ParagraphSplitting(t *testing.T) {
	// Two paragraphs, each small enough for target, but too large together.
	para1 := strings.Repeat("word ", 20) // ~20 tokens
	para2 := strings.Repeat("other ", 20)
	text := para1 + "\n\n" + para2

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  25,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("got %d chunks, want at least 2", len(chunks))
	}
}

func TestFixedChunker_MaxChunksLimit(t *testing.T) {
	// Build text that would produce many chunks.
	var sections []string
	for range 20 {
		sections = append(sections, strings.Repeat("token ", 10))
	}
	text := strings.Join(sections, "\n\n")

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  15,
		OverlapTokens: 0,
		MaxChunks:     3,
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) > 3 {
		t.Errorf("got %d chunks, want <= 3", len(chunks))
	}
}

func TestFixedChunker_ContextCancellation(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 10, OverlapTokens: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := fc.Chunk(ctx, "some text", chunking.ChunkOptions{})
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}

func TestFixedChunker_PerRequestOverrides(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  500,
		OverlapTokens: 0,
		MaxChunks:     50,
	})

	// Build text that fits in 500 tokens but not in 15.
	var sections []string
	for range 10 {
		sections = append(sections, strings.Repeat("word ", 10))
	}
	text := strings.Join(sections, "\n\n")

	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{
		MaxChunks: 2,
		Text:      chunking.TextChunkOptions{TargetTokens: 15},
	})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) > 2 {
		t.Errorf("per-request MaxChunks not respected: got %d chunks, want <= 2", len(chunks))
	}
}

// --- Tests for flattenOversizedSections fix ---

func TestFlattenOversizedSections_SmallSections(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})
	sections := toPositionedSections([]string{"short section one", "short section two"})
	result := fc.flattenOversizedSections(sections, 100)

	if len(result) != 2 {
		t.Fatalf("got %d sections, want 2", len(result))
	}
	if result[0].text != "short section one" || result[1].text != "short section two" {
		t.Errorf("sections altered unexpectedly: %v", sectionTexts(result))
	}
}

func TestFlattenOversizedSections_SplitsOnNewlines(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})

	// Build a section that exceeds 10 tokens, containing newlines.
	line1 := strings.Repeat("alpha ", 8) // ~8 tokens
	line2 := strings.Repeat("beta ", 8)
	oversized := line1 + "\n" + line2 // combined > 10 tokens

	result := fc.flattenOversizedSections(toPositionedSections([]string{oversized}), 10)

	if len(result) < 2 {
		t.Fatalf("expected section to be split on newlines, got %d pieces", len(result))
	}
	for _, s := range result {
		tokens := fc.tokenizer.CountTokens(s.text)
		if tokens > 10 {
			t.Errorf("sub-section has %d tokens (> 10): %q", tokens, s.text)
		}
	}
}

func TestFlattenOversizedSections_SplitsOnSentences(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})

	// Build a section with no newlines but sentence boundaries.
	// Use 7 repeats so each sentence half fits within 10 BPE tokens
	// (7 "alpha" + "." = 10 tokens; 8 would be 11 and trigger force-split).
	sent1 := strings.Repeat("alpha ", 7)
	sent2 := strings.Repeat("beta ", 7)
	oversized := strings.TrimSpace(sent1) + ". " + strings.TrimSpace(sent2) // no newlines

	result := fc.flattenOversizedSections(toPositionedSections([]string{oversized}), 10)

	if len(result) < 2 {
		t.Fatalf("expected section to be split on sentences, got %d pieces", len(result))
	}
	// First section should end with a period (re-added by the sentence split logic)
	if !strings.HasSuffix(strings.TrimSpace(result[0].text), ".") {
		t.Errorf("first sub-section should end with period, got %q", result[0].text)
	}
}

func TestFlattenOversizedSections_ForceSplitOnWords(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})

	// Build a long section with no newlines or periods — only spaces between words.
	oversized := strings.Repeat("word ", 50) // ~50 tokens, no sentence/newline breaks

	result := fc.flattenOversizedSections(toPositionedSections([]string{oversized}), 10)

	if len(result) < 2 {
		t.Fatalf("expected force-split into multiple pieces, got %d", len(result))
	}
	for i, s := range result {
		tokens := fc.tokenizer.CountTokens(s.text)
		if tokens > 10 {
			t.Errorf("piece %d has %d tokens (> 10): %q", i, tokens, s.text)
		}
	}
}

func TestFlattenOversizedSections_EmptySections(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})
	result := fc.flattenOversizedSections([]positionedSection{}, 10)
	if len(result) != 0 {
		t.Errorf("expected 0 sections, got %d", len(result))
	}
}

func TestFlattenOversizedSections_MixedSizes(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})

	small := "tiny"
	oversized := strings.Repeat("word ", 50) // no newlines or periods

	result := fc.flattenOversizedSections(toPositionedSections([]string{small, oversized}), 10)

	// First should be unchanged.
	if result[0].text != "tiny" {
		t.Errorf("first section changed: got %q, want %q", result[0].text, "tiny")
	}
	// Remaining should all fit within target.
	for i := 1; i < len(result); i++ {
		tokens := fc.tokenizer.CountTokens(result[i].text)
		if tokens > 10 {
			t.Errorf("piece %d has %d tokens (> 10): %q", i, tokens, result[i].text)
		}
	}
}

// --- Tests for splitByTokenCount ---

func TestSplitByTokenCount_FitsInOne(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})
	result := fc.splitByTokenCount(positionedSection{text: "hello world", start: 0}, 100)
	if len(result) != 1 {
		t.Fatalf("got %d pieces, want 1", len(result))
	}
	if result[0].text != "hello world" {
		t.Errorf("got %q, want %q", result[0].text, "hello world")
	}
}

func TestSplitByTokenCount_SplitsEvenly(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})

	// 30 words, target 10 tokens — should split into ~3+ pieces.
	text := strings.TrimSpace(strings.Repeat("word ", 30))
	result := fc.splitByTokenCount(positionedSection{text: text, start: 0}, 10)

	if len(result) < 2 {
		t.Fatalf("expected multiple pieces, got %d", len(result))
	}
	for i, piece := range result {
		tokens := fc.tokenizer.CountTokens(piece.text)
		if tokens > 10 {
			t.Errorf("piece %d has %d tokens (> 10): %q", i, tokens, piece.text)
		}
	}
	// Reconstructing should yield the same words.
	texts := sectionTexts(result)
	reconstructed := strings.Join(texts, " ")
	if reconstructed != text {
		t.Errorf("reconstruction mismatch:\ngot:  %q\nwant: %q", reconstructed, text)
	}
}

func TestSplitByTokenCount_EmptyText(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})
	result := fc.splitByTokenCount(positionedSection{text: "", start: 0}, 10)
	if len(result) != 0 {
		t.Errorf("expected 0 pieces for empty text, got %d", len(result))
	}
}

// --- End-to-end tests for oversized section handling ---

func TestFixedChunker_OversizedSingleSection(t *testing.T) {
	// A single block of text with no paragraph separators that exceeds target.
	text := strings.TrimSpace(strings.Repeat("word ", 50))

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  10,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected oversized text to produce multiple chunks, got %d", len(chunks))
	}
	// Every chunk's text should be non-empty.
	for i, c := range chunks {
		if c.GetText() == "" {
			t.Errorf("chunk %d has empty text", i)
		}
	}
}

func TestFixedChunker_OversizedSectionWithNewlines(t *testing.T) {
	// Two paragraph-separated blocks, each larger than target, containing newlines.
	block := strings.TrimSpace(strings.Repeat("line\n", 20)) // newlines inside
	text := block + "\n\n" + block

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  10,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) < 3 {
		t.Fatalf("expected multiple chunks from oversized blocks, got %d", len(chunks))
	}
}

func TestFixedChunker_OversizedSectionWithSentences(t *testing.T) {
	// A single block with sentence boundaries but no newlines.
	var sentences []string
	for range 10 {
		sentences = append(sentences, strings.Repeat("alpha ", 5))
	}
	text := strings.Join(sentences, ". ") + "."

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  10,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected sentence-level splitting to produce multiple chunks, got %d", len(chunks))
	}
}

func TestFixedChunker_OverlapWithOversizedSections(t *testing.T) {
	// Verify overlap still works correctly after the flattening fix.
	var sections []string
	for range 6 {
		sections = append(sections, strings.Repeat("overlap ", 10))
	}
	text := strings.Join(sections, "\n\n")

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  15,
		OverlapTokens: 3,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	// With overlap, chunks after the first should contain some text from the previous chunk.
	texts := chunkTexts(chunks)
	for i := 1; i < len(texts); i++ {
		if texts[i] == "" {
			t.Errorf("chunk %d is empty", i)
		}
	}
}

func TestFixedChunker_Close(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{})
	if err := fc.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- New tests: offset invariant, UTF-8, cascade, separator ---

// verifyOffsetInvariant checks that text[StartChar:EndChar] == chunk.Text for
// every chunk.
func verifyOffsetInvariant(t *testing.T, text string, chunks []chunking.Chunk) {
	t.Helper()
	for i, chunk := range chunks {
		tc, err := chunk.AsTextContent()
		if err != nil {
			t.Errorf("chunk %d: AsTextContent: %v", i, err)
			continue
		}
		if tc.StartChar < 0 || tc.EndChar > len(text) || tc.StartChar > tc.EndChar {
			t.Errorf("chunk %d: invalid offsets [%d:%d] for text len %d", i, tc.StartChar, tc.EndChar, len(text))
			continue
		}
		expected := text[tc.StartChar:tc.EndChar]
		if expected != tc.Text {
			t.Errorf("chunk %d: text[%d:%d] = %q, but chunk.Text = %q", i, tc.StartChar, tc.EndChar, expected, tc.Text)
		}
	}
}

func TestFixedChunker_OffsetInvariant_Paragraphs(t *testing.T) {
	para1 := "The quick brown fox jumps over the lazy dog."
	para2 := "Pack my box with five dozen liquor jugs."
	para3 := "How vexingly quick daft zebras jump."
	text := para1 + "\n\n" + para2 + "\n\n" + para3

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  15,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	verifyOffsetInvariant(t, text, chunks)
}

func TestFixedChunker_OffsetInvariant_NewlineFallback(t *testing.T) {
	// No double-newlines, but single newlines present.
	lines := []string{
		"First line of text here.",
		"Second line of text here.",
		"Third line of text here.",
		"Fourth line of text here.",
	}
	text := strings.Join(lines, "\n")

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  10,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	verifyOffsetInvariant(t, text, chunks)
}

func TestFixedChunker_OffsetInvariant_SentenceFallback(t *testing.T) {
	// No newlines at all, but sentence boundaries.
	text := "First sentence here. Second sentence here. Third sentence here. Fourth sentence here."

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  8,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	verifyOffsetInvariant(t, text, chunks)
}

func TestFixedChunker_OffsetInvariant_ForceSplit(t *testing.T) {
	// No newlines, no periods — just words.
	text := strings.TrimSpace(strings.Repeat("word ", 40))

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  8,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	verifyOffsetInvariant(t, text, chunks)
}

func TestFixedChunker_OffsetInvariant_Mixed(t *testing.T) {
	// Mix of paragraphs, sentences, and plain words.
	text := "Paragraph one sentence one. Paragraph one sentence two.\n\nParagraph two has words " +
		strings.Repeat("word ", 30) + "\n\nShort para."

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  10,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	verifyOffsetInvariant(t, text, chunks)
}

func TestFixedChunker_UTF8_OverlapSafety(t *testing.T) {
	// Multi-byte characters: ensure overlap doesn't split mid-rune.
	text := strings.Repeat("日本語テスト ", 20)

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  5,
		OverlapTokens: 2,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	for i, c := range chunks {
		if !utf8.ValidString(c.GetText()) {
			t.Errorf("chunk %d is not valid UTF-8: %q", i, c.GetText())
		}
	}
}

func TestFixedChunker_UTF8_Chunking(t *testing.T) {
	// Multi-byte text with paragraph separators, verify offset invariant.
	text := "日本語のテスト文。\n\nこれは二番目の段落です。\n\n三番目の段落もあります。"

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  5,
		OverlapTokens: 0,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	verifyOffsetInvariant(t, text, chunks)
}

func TestFixedChunker_SeparatorNewline(t *testing.T) {
	// When separator is "\n", the fallback should not redundantly try "\n" again.
	lines := []string{
		"First line of text.",
		"Second line of text.",
		"Third line of text.",
		"Fourth line of text.",
	}
	text := strings.Join(lines, "\n")

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  10,
		OverlapTokens: 0,
		Separator:     "\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	verifyOffsetInvariant(t, text, chunks)
}

func TestFixedChunker_CascadeSplitting(t *testing.T) {
	// Oversized newline-split subs with sentence boundaries should get
	// sentence-split, not force-split.
	sent1 := strings.Repeat("alpha ", 8) // ~8 tokens
	sent2 := strings.Repeat("beta ", 8)
	// A section containing newlines where each line is still oversized but
	// has sentence boundaries.
	line1 := strings.TrimSpace(sent1) + ". " + strings.TrimSpace(sent2)
	line2 := strings.TrimSpace(sent2) + ". " + strings.TrimSpace(sent1)
	section := line1 + "\n" + line2

	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})
	result := fc.flattenOversizedSections(toPositionedSections([]string{section}), 10)

	// Should have at least 4 pieces (2 sentences per line × 2 lines)
	if len(result) < 4 {
		t.Fatalf("expected cascade splitting to produce >= 4 pieces, got %d: %v", len(result), sectionTexts(result))
	}
	for i, s := range result {
		tokens := fc.tokenizer.CountTokens(s.text)
		if tokens > 10 {
			t.Errorf("piece %d has %d tokens (> 10): %q", i, tokens, s.text)
		}
	}
}

func TestFixedChunker_OverlapOffsetValidity(t *testing.T) {
	// Even with overlap enabled, chunk offsets should be valid positions in
	// the original text (offsets point to non-overlap content).
	var sections []string
	for range 6 {
		sections = append(sections, "Section number "+strings.Repeat("word ", 10))
	}
	text := strings.Join(sections, "\n\n")

	fc := newTestChunker(t, FixedChunkerConfig{
		TargetTokens:  15,
		OverlapTokens: 3,
		Separator:     "\n\n",
	})
	chunks, err := fc.Chunk(context.Background(), text, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	// Verify all offsets are within bounds
	for i, chunk := range chunks {
		tc, err := chunk.AsTextContent()
		if err != nil {
			t.Errorf("chunk %d: AsTextContent: %v", i, err)
			continue
		}
		if tc.StartChar < 0 || tc.EndChar > len(text) || tc.StartChar > tc.EndChar {
			t.Errorf("chunk %d: invalid offsets [%d:%d] for text len %d", i, tc.StartChar, tc.EndChar, len(text))
		}
	}
}

func TestExtractOverlap_UTF8Safety(t *testing.T) {
	fc := newTestChunker(t, FixedChunkerConfig{TargetTokens: 100, OverlapTokens: 0})

	// Build text where the naive cut point would land mid-rune.
	// Each CJK char is 3 bytes. With targetTokens=1, overlapChars=4,
	// we want a text longer than 4 bytes.
	text := "日本語テスト" // 6 chars × 3 bytes = 18 bytes

	result := fc.extractOverlap(text, 1)
	if !utf8.ValidString(result) {
		t.Errorf("extractOverlap produced invalid UTF-8: %q", result)
	}
}
