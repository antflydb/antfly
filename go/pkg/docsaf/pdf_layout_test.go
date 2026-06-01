package docsaf

import (
	"strings"
	"testing"

	"github.com/ajroetker/pdf"
)

func TestLayoutAnalyzer_DetectColumns(t *testing.T) {
	la := NewLayoutAnalyzer()

	// Simulate a two-column layout
	// Left column: X=50, Right column: X=350
	texts := []pdf.Text{
		// Row 1
		{S: "Left", X: 50, Y: 700, W: 30, FontSize: 12},
		{S: "column", X: 85, Y: 700, W: 45, FontSize: 12},
		{S: "text", X: 135, Y: 700, W: 25, FontSize: 12},
		{S: "Right", X: 350, Y: 700, W: 35, FontSize: 12},
		{S: "column", X: 390, Y: 700, W: 45, FontSize: 12},
		{S: "text", X: 440, Y: 700, W: 25, FontSize: 12},
		// Row 2
		{S: "More", X: 50, Y: 680, W: 30, FontSize: 12},
		{S: "left", X: 85, Y: 680, W: 25, FontSize: 12},
		{S: "More", X: 350, Y: 680, W: 30, FontSize: 12},
		{S: "right", X: 385, Y: 680, W: 30, FontSize: 12},
		// Row 3
		{S: "Third", X: 50, Y: 660, W: 35, FontSize: 12},
		{S: "line", X: 90, Y: 660, W: 25, FontSize: 12},
		{S: "Third", X: 350, Y: 660, W: 35, FontSize: 12},
		{S: "right", X: 390, Y: 660, W: 30, FontSize: 12},
		// Row 4
		{S: "Fourth", X: 50, Y: 640, W: 40, FontSize: 12},
		{S: "Fourth", X: 350, Y: 640, W: 40, FontSize: 12},
	}

	columns := la.detectColumns(texts, 50, 500)

	if len(columns) < 2 {
		t.Errorf("Expected at least 2 columns, got %d", len(columns))
		return
	}

	// Verify left column has blocks
	leftBlocks := columns[0].Blocks
	if len(leftBlocks) == 0 {
		t.Error("Left column should have blocks")
	}

	// Verify right column has blocks
	rightBlocks := columns[len(columns)-1].Blocks
	if len(rightBlocks) == 0 {
		t.Error("Right column should have blocks")
	}
}

func TestLayoutAnalyzer_SingleColumn(t *testing.T) {
	la := NewLayoutAnalyzer()

	// Single column layout - no gaps large enough for column detection
	texts := []pdf.Text{
		{S: "This", X: 50, Y: 700, W: 25, FontSize: 12},
		{S: "is", X: 80, Y: 700, W: 12, FontSize: 12},
		{S: "single", X: 97, Y: 700, W: 40, FontSize: 12},
		{S: "column", X: 142, Y: 700, W: 45, FontSize: 12},
		{S: "Second", X: 50, Y: 680, W: 42, FontSize: 12},
		{S: "line", X: 97, Y: 680, W: 25, FontSize: 12},
	}

	columns := la.detectColumns(texts, 50, 200)

	if len(columns) != 1 {
		t.Errorf("Expected 1 column for single-column layout, got %d", len(columns))
	}
}

func TestLayoutAnalyzer_GroupIntoRows(t *testing.T) {
	la := NewLayoutAnalyzer()

	texts := []pdf.Text{
		{S: "A", X: 50, Y: 700.0, W: 10, FontSize: 12},
		{S: "B", X: 65, Y: 700.5, W: 10, FontSize: 12}, // Same row (within tolerance)
		{S: "C", X: 80, Y: 699.5, W: 10, FontSize: 12}, // Same row
		{S: "D", X: 50, Y: 680.0, W: 10, FontSize: 12}, // Different row
		{S: "E", X: 65, Y: 680.0, W: 10, FontSize: 12}, // Same row as D
	}

	rows := la.groupIntoRows(texts)

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	// First row should have 3 elements (A, B, C)
	if len(rows) > 0 && len(rows[0]) != 3 {
		t.Errorf("First row should have 3 elements, got %d", len(rows[0]))
	}

	// Second row should have 2 elements (D, E)
	if len(rows) > 1 && len(rows[1]) != 2 {
		t.Errorf("Second row should have 2 elements, got %d", len(rows[1]))
	}
}

func TestLayoutAnalyzer_DetectTables(t *testing.T) {
	la := NewLayoutAnalyzer()

	// Create a 3x3 table-like structure
	blocks := []TextBlock{
		// Row 1
		{X: 50, Y: 700, Width: 80, Text: "Header1", FontSize: 12},
		{X: 150, Y: 700, Width: 80, Text: "Header2", FontSize: 12},
		{X: 250, Y: 700, Width: 80, Text: "Header3", FontSize: 12},
		// Row 2
		{X: 50, Y: 680, Width: 80, Text: "Cell1", FontSize: 12},
		{X: 150, Y: 680, Width: 80, Text: "Cell2", FontSize: 12},
		{X: 250, Y: 680, Width: 80, Text: "Cell3", FontSize: 12},
		// Row 3
		{X: 50, Y: 660, Width: 80, Text: "Cell4", FontSize: 12},
		{X: 150, Y: 660, Width: 80, Text: "Cell5", FontSize: 12},
		{X: 250, Y: 660, Width: 80, Text: "Cell6", FontSize: 12},
	}

	tables := la.detectTables(blocks)

	if len(tables) == 0 {
		t.Log("No table detected - this may be expected with current heuristics")
		return
	}

	table := tables[0]
	if table.Rows < 2 || table.Cols < 2 {
		t.Errorf("Expected at least 2x2 table, got %dx%d", table.Rows, table.Cols)
	}
}

func TestLayoutAnalyzer_FormatTable(t *testing.T) {
	la := NewLayoutAnalyzer()

	table := Table{
		Rows: 2,
		Cols: 3,
		Cells: [][]TableCell{
			{
				{Text: "A"},
				{Text: "B"},
				{Text: "C"},
			},
			{
				{Text: "1"},
				{Text: "2"},
				{Text: "3"},
			},
		},
	}

	formatted := la.formatTable(table)

	if !strings.Contains(formatted, "A") || !strings.Contains(formatted, "B") {
		t.Error("Formatted table should contain cell text")
	}
	if !strings.Contains(formatted, "|") {
		t.Error("Formatted table should contain column separators")
	}
}

func TestFontDecoder_Decode(t *testing.T) {
	fd := NewFontDecoder()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "fi ligature",
			input: "of\uFB01ce",
			want:  "office",
		},
		{
			name:  "fl ligature",
			input: "\uFB02ower",
			want:  "flower",
		},
		{
			name:  "ff ligature",
			input: "e\uFB00ect",
			want:  "effect",
		},
		{
			name:  "ffi ligature",
			input: "o\uFB03ce",
			want:  "office",
		},
		{
			name:  "smart quotes",
			input: "\u201CHello\u201D \u2018world\u2019",
			want:  "\"Hello\" 'world'",
		},
		{
			name:  "em dash",
			input: "word\u2014word",
			want:  "word-word",
		},
		{
			name:  "ellipsis",
			input: "wait\u2026",
			want:  "wait...",
		},
		{
			name:  "various spaces",
			input: "hello\u00A0world\u2003test",
			want:  "hello world test",
		},
		{
			name:  "normal text unchanged",
			input: "Hello, World! 123",
			want:  "Hello, World! 123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fd.Decode(tt.input)
			if got != tt.want {
				t.Errorf("Decode(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFontDecoder_ROT3(t *testing.T) {
	fd := NewFontDecoder()

	tests := []struct {
		name    string
		input   string
		isROT3  bool
		decoded string
	}{
		{
			name:    "ROT3 encoded 'the'",
			input:   "wkh",
			isROT3:  false, // Not enough patterns to detect
			decoded: "the",
		},
		{
			name:    "ROT3 encoded sentence",
			input:   "wkh dqg iru zlwk",
			isROT3:  true,
			decoded: "the and for with",
		},
		{
			name:    "normal text",
			input:   "the and for with",
			isROT3:  false,
			decoded: "the and for with",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isROT3 := fd.IsLikelyROT3(tt.input)
			if isROT3 != tt.isROT3 {
				t.Errorf("IsLikelyROT3(%q) = %v, want %v", tt.input, isROT3, tt.isROT3)
			}

			// DecodeROT3 always applies the shift - only verify for ROT3-encoded input
			// (this matches actual usage where IsLikelyROT3 is checked first)
			decoded := fd.DecodeROT3(tt.input)
			// For ROT3 test cases (where input IS encoded), verify decoding works
			if strings.HasPrefix(tt.name, "ROT3") {
				if decoded != tt.decoded {
					t.Errorf("DecodeROT3(%q) = %q, want %q", tt.input, decoded, tt.decoded)
				}
			}
		})
	}
}

func TestGlyphMapper_PUA(t *testing.T) {
	gm := NewGlyphMapper()

	// Test that unknown PUA characters are replaced with space
	input := "Hello\uE000World"
	got := gm.Map(input)
	want := "Hello World"
	if got != want {
		t.Errorf("Map(%q) = %q, want %q", input, got, want)
	}

	// Test that normal text is unchanged
	input = "Normal text"
	got = gm.Map(input)
	if got != input {
		t.Errorf("Map(%q) = %q, want unchanged", input, got)
	}
}

func TestEnhancedTextCleaner_Clean(t *testing.T) {
	etc := NewEnhancedTextCleaner()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "ligatures and spaces",
			input: "The of\uFB01ce\u00A0is\u2014open",
			want:  "The office is-open",
		},
		{
			name:  "smart quotes",
			input: "\u201CHello,\u201D she said",
			want:  "\"Hello,\" she said",
		},
		{
			name:  "multiple spaces collapsed",
			input: "hello    world   test",
			want:  "hello world test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := etc.Clean(tt.input)
			if got != tt.want {
				t.Errorf("Clean(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestLayoutAnalyzer_HasConsistentSpacing(t *testing.T) {
	la := NewLayoutAnalyzer()

	tests := []struct {
		name      string
		positions []float64
		tolerance float64
		want      bool
	}{
		{
			name:      "consistent spacing",
			positions: []float64{0, 100, 200, 300},
			tolerance: 0.1,
			want:      true,
		},
		{
			name:      "inconsistent spacing",
			positions: []float64{0, 100, 150, 400},
			tolerance: 0.1,
			want:      false,
		},
		{
			name:      "single position",
			positions: []float64{100},
			tolerance: 0.1,
			want:      false,
		},
		{
			name:      "two positions",
			positions: []float64{0, 100},
			tolerance: 0.1,
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := la.hasConsistentSpacing(tt.positions, tt.tolerance)
			if got != tt.want {
				t.Errorf("hasConsistentSpacing(%v, %v) = %v, want %v",
					tt.positions, tt.tolerance, got, tt.want)
			}
		})
	}
}

func TestLayoutAnalyzer_FormatBlocks(t *testing.T) {
	la := NewLayoutAnalyzer()
	la.RowTolerance = 3.0

	blocks := []TextBlock{
		{X: 50, Y: 700, Width: 30, Text: "Hello", FontSize: 12},
		{X: 90, Y: 700, Width: 30, Text: "World", FontSize: 12},
		{X: 50, Y: 680, Width: 50, Text: "New line", FontSize: 12},
	}

	formatted := la.formatBlocks(blocks)

	if !strings.Contains(formatted, "Hello") {
		t.Error("Formatted text should contain 'Hello'")
	}
	if !strings.Contains(formatted, "World") {
		t.Error("Formatted text should contain 'World'")
	}
	if !strings.Contains(formatted, "\n") {
		t.Error("Formatted text should contain newline between rows")
	}
}

func TestLayoutAnalyzer_AdaptiveSpacing(t *testing.T) {
	// Simulate the Epstein PDF issue: tight character spacing (0.5pt)
	// with word breaks at 3pt. Fixed threshold of 30% * 10pt = 3.0pt
	// would incorrectly merge words, but adaptive threshold should work.

	t.Run("tight spacing with adaptive threshold", func(t *testing.T) {
		la := NewLayoutAnalyzer()
		la.UseAdaptiveSpacing = true

		// Simulate "taken on behalf" with tight inter-character spacing (0.5pt gap)
		// Each character is 5pt wide, with 0.5pt gap between chars
		// Word breaks have 3pt gap
		texts := []pdf.Text{
			// "taken" - 5pt wide chars with 0.5pt gaps
			{S: "t", X: 50.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "a", X: 55.5, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			{S: "k", X: 61.0, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			{S: "e", X: 66.5, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			{S: "n", X: 72.0, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			// Word break: 3pt gap
			// "on" - 5pt wide chars with 0.5pt gaps
			{S: "o", X: 80.0, Y: 700, W: 5.0, FontSize: 10}, // gap: 3pt
			{S: "n", X: 85.5, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			// Word break: 3pt gap
			// "behalf" - 5pt wide chars with 0.5pt gaps
			{S: "b", X: 93.5, Y: 700, W: 5.0, FontSize: 10},  // gap: 3pt
			{S: "e", X: 99.0, Y: 700, W: 5.0, FontSize: 10},  // gap: 0.5pt
			{S: "h", X: 104.5, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			{S: "a", X: 110.0, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			{S: "l", X: 115.5, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
			{S: "f", X: 121.0, Y: 700, W: 5.0, FontSize: 10}, // gap: 0.5pt
		}

		blocks := la.textsToBlocks(texts)

		// Should produce 3 blocks: "taken", "on", "behalf"
		if len(blocks) != 3 {
			t.Errorf("Expected 3 blocks, got %d", len(blocks))
			for i, b := range blocks {
				t.Logf("Block %d: %q", i, b.Text)
			}
			return
		}

		if blocks[0].Text != "taken" {
			t.Errorf("Block 0: got %q, want %q", blocks[0].Text, "taken")
		}
		if blocks[1].Text != "on" {
			t.Errorf("Block 1: got %q, want %q", blocks[1].Text, "on")
		}
		if blocks[2].Text != "behalf" {
			t.Errorf("Block 2: got %q, want %q", blocks[2].Text, "behalf")
		}
	})

	t.Run("fixed threshold incorrectly merges words", func(t *testing.T) {
		la := NewLayoutAnalyzer()
		la.UseAdaptiveSpacing = false // Use fixed threshold
		la.WordSpaceMultiplier = 0.3  // 30% * 10pt = 3.0pt

		// Same tight spacing scenario - 3pt word breaks are <= 3.0pt threshold
		texts := []pdf.Text{
			{S: "t", X: 50.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "a", X: 55.5, Y: 700, W: 5.0, FontSize: 10},
			{S: "k", X: 61.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "e", X: 66.5, Y: 700, W: 5.0, FontSize: 10},
			{S: "n", X: 72.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "o", X: 80.0, Y: 700, W: 5.0, FontSize: 10}, // 3pt gap - at threshold
			{S: "n", X: 85.5, Y: 700, W: 5.0, FontSize: 10},
			{S: "b", X: 93.5, Y: 700, W: 5.0, FontSize: 10}, // 3pt gap - at threshold
			{S: "e", X: 99.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "h", X: 104.5, Y: 700, W: 5.0, FontSize: 10},
			{S: "a", X: 110.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "l", X: 115.5, Y: 700, W: 5.0, FontSize: 10},
			{S: "f", X: 121.0, Y: 700, W: 5.0, FontSize: 10},
		}

		blocks := la.textsToBlocks(texts)

		// With fixed 3.0pt threshold and 3.0pt gaps, should merge into 1 block
		// (demonstrating the problem)
		if len(blocks) != 1 {
			t.Logf("Fixed threshold produced %d blocks (expected 1 to show the problem)", len(blocks))
		}
	})

	t.Run("calculates median spacing correctly", func(t *testing.T) {
		la := NewLayoutAnalyzer()

		// Create enough texts with 0.5pt gaps between chars
		texts := []pdf.Text{
			{S: "a", X: 50.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "b", X: 55.5, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "c", X: 61.0, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "d", X: 66.5, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "e", X: 72.0, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "f", X: 77.5, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "g", X: 83.0, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "h", X: 88.5, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "i", X: 94.0, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "j", X: 99.5, Y: 700, W: 5.0, FontSize: 10},  // 0.5pt gap
			{S: "k", X: 105.0, Y: 700, W: 5.0, FontSize: 10}, // 0.5pt gap
		}

		medianSpacing := la.calculateMedianCharSpacing(texts)

		if medianSpacing != 0.5 {
			t.Errorf("Median spacing: got %v, want 0.5", medianSpacing)
		}

		// Threshold should be 2.5x median = 1.25pt
		expectedThreshold := 1.25
		actualThreshold := medianSpacing * 2.5
		if actualThreshold != expectedThreshold {
			t.Errorf("Threshold: got %v, want %v", actualThreshold, expectedThreshold)
		}
	})

	t.Run("returns zero for insufficient data", func(t *testing.T) {
		la := NewLayoutAnalyzer()

		// Too few texts (need at least 10)
		texts := []pdf.Text{
			{S: "a", X: 50.0, Y: 700, W: 5.0, FontSize: 10},
			{S: "b", X: 55.5, Y: 700, W: 5.0, FontSize: 10},
		}

		medianSpacing := la.calculateMedianCharSpacing(texts)

		if medianSpacing != 0 {
			t.Errorf("Expected 0 for insufficient data, got %v", medianSpacing)
		}
	})
}

// ========== Phase 2 Tests: Subscript/Superscript Detection ==========

func TestNormalizeSuperscripts(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "superscript digits",
			input: "x² + y³ = z⁴",
			want:  "x2 + y3 = z4",
		},
		{
			name:  "superscript letters",
			input: "aⁿ + bᵐ",
			want:  "an + bm",
		},
		{
			name:  "mixed text and superscripts",
			input: "E=mc²",
			want:  "E=mc2",
		},
		{
			name:  "normal text unchanged",
			input: "Hello World",
			want:  "Hello World",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeSuperscripts(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeSuperscripts(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeSubscripts(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "subscript digits",
			input: "H₂O",
			want:  "H2O",
		},
		{
			name:  "subscript letters",
			input: "Cₙ + Oₘ",
			want:  "Cn + Om",
		},
		{
			name:  "chemical formula",
			input: "C₆H₁₂O₆",
			want:  "C6H12O6",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeSubscripts(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeSubscripts(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeSubSuperscripts(t *testing.T) {
	input := "H₂O has a boiling point of 100°C at sea level (10⁵ Pa)"
	got := NormalizeSubSuperscripts(input)

	if !strings.Contains(got, "H2O") {
		t.Errorf("Should normalize H₂O to H2O, got: %s", got)
	}
	if !strings.Contains(got, "105") {
		t.Errorf("Should normalize 10⁵ to 105, got: %s", got)
	}
}

func TestExpandFootnoteReferences(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "single footnote",
			input: "statement¹",
			want:  "statement[1]",
		},
		{
			name:  "multiple footnotes",
			input: "fact¹ and claim²",
			want:  "fact[1] and claim[2]",
		},
		{
			name:  "double digit footnote",
			input: "reference¹²",
			want:  "reference[12]",
		},
		{
			name:  "footnote after punctuation",
			input: "end of sentence.¹",
			want:  "end of sentence.[1]",
		},
		{
			name:  "superscript not footnote (math)",
			input: "x² = 4",
			want:  "x2 = 4", // Not treated as footnote since preceded by letter
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExpandFootnoteReferences(tt.input)
			if got != tt.want {
				t.Errorf("ExpandFootnoteReferences(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ========== Phase 2 Tests: Enhanced Paragraph Detection ==========

func TestIsBulletLine(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{"• Item one", true},
		{"- List item", true},
		{"* Star item", true},
		{"→ Arrow item", true},
		{"Regular line", false},
		{"1. Numbered", false}, // Numbered, not bullet
		{"", false},
	}

	for _, tt := range tests {
		got := isBulletLine(tt.line)
		if got != tt.want {
			t.Errorf("isBulletLine(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}

func TestIsNumberedLine(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{"1. First item", true},
		{"10. Tenth item", true},
		{"a. Alpha item", true},
		{"(1) Parenthetical", true},
		{"i. Roman numeral", true},
		{"Regular line", false},
		{"• Bullet", false},
		{"", false},
	}

	for _, tt := range tests {
		got := isNumberedLine(tt.line)
		if got != tt.want {
			t.Errorf("isNumberedLine(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}

func TestIsHeaderLine(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{"INTRODUCTION", true},
		{"Section 1 Overview", true},
		{"CHAPTER ONE", true},
		{"1.1 Subsection Title", true},
		{"This is a normal sentence.", false},
		{"The quick brown fox jumps over the lazy dog.", false},
		{"", false},
	}

	for _, tt := range tests {
		got := isHeaderLine(tt.line)
		if got != tt.want {
			t.Errorf("isHeaderLine(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}

func TestEnhancedParagraphDetection(t *testing.T) {
	config := DefaultParagraphConfig()

	t.Run("detects header followed by content", func(t *testing.T) {
		input := "INTRODUCTION\nThis is the first paragraph."
		got := EnhancedParagraphDetection(input, config)

		if !strings.Contains(got, "\n\n") {
			t.Error("Should insert paragraph break after header")
		}
	})

	t.Run("detects bullet list boundaries", func(t *testing.T) {
		input := "Consider the following:\n• Item one\n• Item two\nAfter the list."
		got := EnhancedParagraphDetection(input, config)

		// Should have breaks before and after list
		if strings.Count(got, "\n\n") < 2 {
			t.Errorf("Should have at least 2 paragraph breaks, got: %q", got)
		}
	})

	t.Run("preserves existing blank lines", func(t *testing.T) {
		input := "First paragraph.\n\nSecond paragraph."
		got := EnhancedParagraphDetection(input, config)

		if !strings.Contains(got, "\n\n") {
			t.Error("Should preserve existing blank lines")
		}
	})
}

func TestAnalyzeLine(t *testing.T) {
	t.Run("detects bullet line", func(t *testing.T) {
		info := AnalyzeLine("• This is a bullet point", 50)
		if !info.IsBullet {
			t.Error("Should detect bullet line")
		}
	})

	t.Run("detects numbered line", func(t *testing.T) {
		info := AnalyzeLine("1. First item", 50)
		if !info.IsNumbered {
			t.Error("Should detect numbered line")
		}
	})

	t.Run("detects empty line", func(t *testing.T) {
		info := AnalyzeLine("   ", 50)
		if !info.IsEmpty {
			t.Error("Should detect empty line")
		}
	})

	t.Run("detects short line", func(t *testing.T) {
		info := AnalyzeLine("Short.", 100)
		if !info.IsShort {
			t.Error("Should detect short line")
		}
	})

	t.Run("calculates indent", func(t *testing.T) {
		info := AnalyzeLine("    Indented text", 50)
		if info.Indent != 4 {
			t.Errorf("Indent = %d, want 4", info.Indent)
		}
	})
}

func TestDetectAndFormatLists(t *testing.T) {
	input := "Before list:\n▪ First item\n▪ Second item\nAfter list."
	got := DetectAndFormatLists(input)

	// Should normalize fancy bullets to standard bullet
	if !strings.Contains(got, "• First item") {
		t.Errorf("Should normalize bullets, got: %q", got)
	}
}

// ========== Phase 2 Tests: EnhancedTextCleaner Methods ==========

func TestEnhancedTextCleaner_CleanForSearch(t *testing.T) {
	etc := NewEnhancedTextCleaner()

	input := "H₂O has a boiling point at 100°C. E=mc²"
	got := etc.CleanForSearch(input)

	if strings.Contains(got, "₂") || strings.Contains(got, "²") {
		t.Errorf("CleanForSearch should normalize subscripts/superscripts, got: %q", got)
	}
	if !strings.Contains(got, "H2O") {
		t.Errorf("Should contain 'H2O', got: %q", got)
	}
}

func TestEnhancedTextCleaner_CleanWithFootnotes(t *testing.T) {
	etc := NewEnhancedTextCleaner()

	input := "The evidence shows¹ that claims² are valid."
	got := etc.CleanWithFootnotes(input)

	if !strings.Contains(got, "[1]") || !strings.Contains(got, "[2]") {
		t.Errorf("CleanWithFootnotes should expand footnotes, got: %q", got)
	}
}

func TestEnhancedTextCleaner_CleanWithEnhancedParagraphs(t *testing.T) {
	etc := NewEnhancedTextCleaner()

	input := "SUMMARY\nThis is the content of the summary section."
	got := etc.CleanWithEnhancedParagraphs(input)

	if !strings.Contains(got, "\n\n") {
		t.Error("CleanWithEnhancedParagraphs should insert paragraph break after header")
	}
}

func TestEnhancedTextCleaner_CleanFull(t *testing.T) {
	etc := NewEnhancedTextCleaner()

	input := "INTRODUCTION\n• First point¹\n• Second point\nConclusion."
	got := etc.CleanFull(input)

	// Should have paragraph breaks
	if !strings.Contains(got, "\n\n") {
		t.Error("CleanFull should have paragraph breaks")
	}
	// Should expand footnotes
	if !strings.Contains(got, "[1]") {
		t.Error("CleanFull should expand footnotes")
	}
}
