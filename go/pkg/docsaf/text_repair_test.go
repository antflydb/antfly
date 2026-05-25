package docsaf

import (
	"slices"
	"strings"
	"testing"
)

func TestTextRepair_DetectEncodingShift(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name           string
		text           string
		wantShift      int
		wantConfidence float64 // minimum confidence
	}{
		{
			name:           "normal english text",
			text:           "The quick brown fox jumps over the lazy dog. This is a sample text with normal English.",
			wantShift:      0,
			wantConfidence: 0.0,
		},
		{
			name:           "ROT3 encoded text",
			text:           "Wkh txlfn eurzq ira mxpsv ryhu wkh odcb grj. Wklv lv d vdpsoh whaw.",
			wantShift:      3,
			wantConfidence: 0.3,
		},
		{
			// ROT13 with longer text for better detection
			name:           "ROT13 encoded text",
			text:           "Gur dhvpx oebja sbk whzcf bire gur ynml qbt. Guvf vf n fnzcyr grkg jvgu ybgf bs jbeqf sbe orggre qrgrpgvba.",
			wantShift:      13,
			wantConfidence: 0.3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shift, confidence := tr.DetectEncodingShift(tt.text)
			if shift != tt.wantShift {
				t.Errorf("DetectEncodingShift() shift = %d, want %d", shift, tt.wantShift)
			}
			if tt.wantConfidence > 0 && confidence < tt.wantConfidence {
				t.Errorf("DetectEncodingShift() confidence = %f, want >= %f", confidence, tt.wantConfidence)
			}
		})
	}
}

func TestTextRepair_DecodeShiftedText(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name  string
		text  string
		shift int
		want  string
	}{
		{
			name:  "ROT3 decode",
			text:  "Wkh txlfn eurzq ira",
			shift: 3,
			want:  "The quick brown fox",
		},
		{
			name:  "ROT13 decode",
			text:  "Gur dhvpx oebja sbk",
			shift: 13,
			want:  "The quick brown fox",
		},
		{
			name:  "no shift",
			text:  "Hello World",
			shift: 0,
			want:  "Hello World",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tr.DecodeShiftedText(tt.text, tt.shift)
			if got != tt.want {
				t.Errorf("DecodeShiftedText() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTextRepair_DetectSymbolSubstitution(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name           string
		text           string
		wantDetected   bool
		wantConfidence float64
	}{
		{
			name:         "normal text",
			text:         "Hello World this is normal text with proper words and sentences",
			wantDetected: false,
		},
		{
			// Symbol substitution with enough symbols to trigger detection
			// This simulates text where A-Z are replaced with symbols
			name:         "symbol encoded text",
			text:         "$ % & ' ( ) * + , - . / 0 1 2 3 4 5 $ % & ' ( ) * + , - . / 0 1 2 3 4 5 6 7 8",
			wantDetected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			substMap, confidence := tr.DetectSymbolSubstitution(tt.text)
			detected := substMap != nil && confidence > 0.3
			if detected != tt.wantDetected {
				t.Errorf("DetectSymbolSubstitution() detected = %v, want %v (confidence=%f)",
					detected, tt.wantDetected, confidence)
			}
		})
	}
}

func TestTextRepair_HeaderFooterDetection(t *testing.T) {
	tr := NewTextRepair()

	// Simulate multiple pages with same header/footer
	pages := []string{
		"COURT REPORTING INC\nThis is page 1 content.\nPage 1",
		"COURT REPORTING INC\nThis is page 2 content.\nPage 2",
		"COURT REPORTING INC\nThis is page 3 content.\nPage 3",
		"COURT REPORTING INC\nThis is page 4 content.\nPage 4",
	}

	for _, page := range pages {
		tr.RecordPageContent(page)
	}

	headers := tr.GetDetectedHeaders()
	footers := tr.GetDetectedFooters()

	// Should detect "COURT REPORTING INC" as header
	if len(headers) == 0 {
		t.Error("Expected to detect at least one header pattern")
	} else {
		found := slices.Contains(headers, "COURT REPORTING INC")
		if !found {
			t.Errorf("Expected header 'COURT REPORTING INC', got %v", headers)
		}
	}

	// Should detect page number pattern as footer (but normalized away)
	// The actual footer detection may vary based on normalization
	t.Logf("Detected footers: %v", footers)
}

func TestTextRepair_RemoveHeadersFooters(t *testing.T) {
	tr := NewTextRepair()

	headers := []string{"COURT REPORTING INC"}
	footers := []string{"Page 1"}

	text := `COURT REPORTING INC
This is the main content.
More content here.
Page 1`

	result := tr.RemoveHeadersFooters(text, headers, footers)

	// Should not contain header
	if containsLine(result, "COURT REPORTING INC") {
		t.Error("Header should have been removed")
	}

	// Should contain main content
	if !containsLine(result, "This is the main content.") {
		t.Error("Main content should be preserved")
	}
}

func containsLine(text, line string) bool {
	return slices.Contains(splitLines(text), line)
}

func splitLines(text string) []string {
	var lines []string
	start := 0
	for i, c := range text {
		if c == '\n' {
			lines = append(lines, text[start:i])
			start = i + 1
		}
	}
	if start < len(text) {
		lines = append(lines, text[start:])
	}
	return lines
}

func TestTextRepair_LevenshteinDistance(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "", 3},
		{"", "abc", 3},
		{"abc", "abc", 0},
		{"abc", "abd", 1},
		{"abc", "adc", 1},
		{"abc", "xyz", 3},
		{"kitten", "sitting", 3},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			got := tr.levenshteinDistance(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("levenshteinDistance(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestLayoutAnalyzer_WithDepositionMode(t *testing.T) {
	la := NewLayoutAnalyzer()

	// Default values
	if la.ColumnGapThreshold != 30.0 {
		t.Errorf("Default ColumnGapThreshold = %f, want 30.0", la.ColumnGapThreshold)
	}
	if la.FilterLineNumbers {
		t.Error("Default FilterLineNumbers should be false")
	}

	// Apply deposition mode
	la.WithDepositionMode()

	if la.ColumnGapThreshold != 12.0 {
		t.Errorf("Deposition ColumnGapThreshold = %f, want 12.0", la.ColumnGapThreshold)
	}
	if !la.FilterLineNumbers {
		t.Error("Deposition FilterLineNumbers should be true")
	}
	if la.MinRowsForColumnPct != 75 {
		t.Errorf("Deposition MinRowsForColumnPct = %d, want 75", la.MinRowsForColumnPct)
	}
}

func TestNewLayoutAnalyzerWithConfig(t *testing.T) {
	cfg := LayoutConfig{
		ColumnGapThreshold:  15.0,
		RowTolerance:        2.5,
		MinRowsForColumnPct: 50,
		FilterLineNumbers:   true,
	}

	la := NewLayoutAnalyzerWithConfig(cfg)

	if la.ColumnGapThreshold != 15.0 {
		t.Errorf("ColumnGapThreshold = %f, want 15.0", la.ColumnGapThreshold)
	}
	if la.RowTolerance != 2.5 {
		t.Errorf("RowTolerance = %f, want 2.5", la.RowTolerance)
	}
	if la.MinRowsForColumnPct != 50 {
		t.Errorf("MinRowsForColumnPct = %d, want 50", la.MinRowsForColumnPct)
	}
	if !la.FilterLineNumbers {
		t.Error("FilterLineNumbers should be true")
	}
	if la.AutoDetectLayout {
		t.Error("AutoDetectLayout should be false when using explicit config")
	}
}

func TestTextRepair_DetectMirroredText(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name          string
		text          string
		wantMirrored  bool
		minConfidence float64
	}{
		{
			name:         "normal English text",
			text:         "The quick brown fox jumps over the lazy dog. This is a sample of normal English text with proper word order.",
			wantMirrored: false,
		},
		{
			name:          "reversed English text",
			text:          "ehT kciuq nworb xof spmuj revo eht yzal god. sihT si a elpmas fo desrever txet.",
			wantMirrored:  true,
			minConfidence: 0.3,
		},
		{
			name:          "mixed reversed words",
			text:          "eht dna rof era tub ton uoy lla nac dah reh saw eno ruo tuo yad teg sah mih sih",
			wantMirrored:  true,
			minConfidence: 0.3,
		},
		{
			name:         "short text",
			text:         "Hello world",
			wantMirrored: false, // Too short to analyze
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			confidence := tr.DetectMirroredText(tt.text)
			isMirrored := confidence >= 0.3

			if isMirrored != tt.wantMirrored {
				t.Errorf("DetectMirroredText() mirrored = %v (confidence=%.2f), want mirrored = %v",
					isMirrored, confidence, tt.wantMirrored)
			}

			if tt.wantMirrored && tt.minConfidence > 0 && confidence < tt.minConfidence {
				t.Errorf("DetectMirroredText() confidence = %.2f, want >= %.2f",
					confidence, tt.minConfidence)
			}
		})
	}
}

func TestTextRepair_RepairMirroredText(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name      string
		text      string
		wantFixed bool
	}{
		{
			name:      "word-reversed text",
			text:      "ehT kciuq nworb xof spmuj revo eht yzal god",
			wantFixed: true,
		},
		{
			name:      "normal text unchanged",
			text:      "The quick brown fox jumps over the lazy dog",
			wantFixed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.RepairMirroredText(tt.text)
			wasFixed := result != tt.text

			if wasFixed != tt.wantFixed {
				t.Errorf("RepairMirroredText() fixed = %v, want %v\nInput:  %s\nOutput: %s",
					wasFixed, tt.wantFixed, tt.text, result)
			}

			if tt.wantFixed {
				// Verify the repaired text looks more like English
				origScore := tr.DetectMirroredText(tt.text)
				repairedScore := tr.DetectMirroredText(result)
				if repairedScore >= origScore {
					t.Errorf("Repaired text should have lower mirrored score: orig=%.2f, repaired=%.2f",
						origScore, repairedScore)
				}
				t.Logf("Repaired: %s (score: %.2f -> %.2f)", result, origScore, repairedScore)
			}
		})
	}
}

func TestTextRepair_BigramExtraction(t *testing.T) {
	tr := NewTextRepair()

	text := "hello world"
	bigrams := tr.extractBigrams(text)

	// Check expected bigrams
	expectedBigrams := []string{"he", "el", "ll", "lo", "wo", "or", "rl", "ld"}
	for _, expected := range expectedBigrams {
		if _, ok := bigrams[expected]; !ok {
			t.Errorf("Expected bigram %q not found", expected)
		}
	}

	// Check that non-adjacent letters don't form bigrams
	if _, ok := bigrams["ow"]; ok {
		t.Error("Bigram 'ow' should not exist (space between 'o' and 'w')")
	}
}

// -------------------------------------------------------------------------
// Tests for Word Segmentation
// -------------------------------------------------------------------------

func TestTextRepair_SegmentWords(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "merged uppercase words",
			input:    "UNITEDSTATESDISTRICTCOURT",
			expected: "UNITED STATES DISTRICT COURT",
		},
		{
			name:     "merged lowercase words",
			input:    "theplaintiffwashere",
			expected: "the plaintiff was here",
		},
		{
			name:     "already segmented text",
			input:    "This is already segmented text.",
			expected: "This is already segmented text.",
		},
		{
			name:     "mixed case merged",
			input:    "ThePlaintiffIsHere",
			expected: "The Plaintiff Is Here",
		},
		{
			name:     "short word unchanged",
			input:    "hello",
			expected: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.SegmentWords(tt.input)
			if result != tt.expected {
				t.Errorf("SegmentWords() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestTextRepair_segmentWord(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name     string
		input    string
		minParts int // Minimum number of parts expected
	}{
		{
			name:     "legal phrase",
			input:    "unitedstates",
			minParts: 2,
		},
		{
			name:     "court phrase",
			input:    "districtcourt",
			minParts: 2,
		},
		{
			name:     "simple word",
			input:    "the",
			minParts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts := tr.segmentWord(tt.input)
			if len(parts) < tt.minParts {
				t.Errorf("segmentWord(%q) returned %d parts, want at least %d: %v",
					tt.input, len(parts), tt.minParts, parts)
			}
			t.Logf("Segmented %q into: %v", tt.input, parts)
		})
	}
}

func TestTextRepair_scoreWord(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name         string
		word         string
		wantScoreMin float64
		wantScoreMax float64
	}{
		{
			name:         "common word",
			word:         "the",
			wantScoreMin: 10.0,
			wantScoreMax: 20.0,
		},
		{
			name:         "uncommon word",
			word:         "xyz",
			wantScoreMin: -10.0,
			wantScoreMax: -3.0,
		},
		{
			name:         "single char",
			word:         "a",
			wantScoreMin: -10.0,
			wantScoreMax: -5.0,
		},
		{
			name:         "empty string",
			word:         "",
			wantScoreMin: -100.0,
			wantScoreMax: -99.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := tr.scoreWord(tt.word)
			if score < tt.wantScoreMin || score > tt.wantScoreMax {
				t.Errorf("scoreWord(%q) = %f, want between %f and %f", tt.word, score, tt.wantScoreMin, tt.wantScoreMax)
			}
		})
	}
}

// -------------------------------------------------------------------------
// Tests for Dictionary-Based Word Repair
// -------------------------------------------------------------------------

func TestTextRepair_RepairMisspelledWords(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "correct text unchanged",
			input:    "The court was in session.",
			expected: "The court was in session.",
		},
		{
			name:     "OCR error comma-v",
			input:    "im,olving the case",
			expected: "involving the case",
		},
		{
			name:     "OCR error m-n",
			input:    "plaimitiff filed a motion",
			expected: "plaintiff filed a motion",
		},
		{
			name:     "short words unchanged",
			input:    "ab cd ef",
			expected: "ab cd ef", // Too short to correct
		},
		{
			name:     "preserves punctuation",
			input:    "The plaimitiff, im,olving the case.",
			expected: "The plaintiff, involving the case.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.RepairMisspelledWords(tt.input)
			if result != tt.expected {
				t.Errorf("RepairMisspelledWords() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestTextRepair_findBestCorrection(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name           string
		word           string
		wantCorrection string
	}{
		{
			name:           "OCR comma instead of v",
			word:           "in,ol,ing",
			wantCorrection: "involving",
		},
		{
			name:           "correct word unchanged",
			word:           "court",
			wantCorrection: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.findBestCorrection(tt.word)
			if tt.wantCorrection != "" && result != tt.wantCorrection {
				t.Errorf("findBestCorrection(%q) = %q, want %q", tt.word, result, tt.wantCorrection)
			}
			if tt.wantCorrection == "" && result != "" && result != tt.word {
				t.Logf("findBestCorrection(%q) = %q (expected no correction)", tt.word, result)
			}
		})
	}
}

func TestTextRepair_generateOCRVariants(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name            string
		word            string
		expectedVariant string
	}{
		{
			name:            "comma to v",
			word:            "in,olving",
			expectedVariant: "involving",
		},
		{
			name:            "l to i",
			word:            "helllo",
			expectedVariant: "heillo", // One variant
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variants := tr.generateOCRVariants(tt.word)
			found := slices.Contains(variants, tt.expectedVariant)
			if !found {
				t.Errorf("generateOCRVariants(%q) did not include expected variant %q. Got: %v",
					tt.word, tt.expectedVariant, variants)
			}
		})
	}
}

func TestTextRepair_applyCasing(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name       string
		original   string
		cleaned    string
		correction string
		expected   string
	}{
		{
			name:       "uppercase preserved",
			original:   "PLAIMITIFF",
			cleaned:    "PLAIMITIFF",
			correction: "plaintiff",
			expected:   "PLAINTIFF",
		},
		{
			name:       "title case preserved",
			original:   "Plaimitiff",
			cleaned:    "Plaimitiff",
			correction: "plaintiff",
			expected:   "Plaintiff",
		},
		{
			name:       "lowercase preserved",
			original:   "plaimitiff",
			cleaned:    "plaimitiff",
			correction: "plaintiff",
			expected:   "plaintiff",
		},
		{
			name:       "punctuation preserved",
			original:   "Plaimitiff,",
			cleaned:    "Plaimitiff",
			correction: "plaintiff",
			expected:   "Plaintiff,",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.applyCasing(tt.original, tt.cleaned, tt.correction)
			if result != tt.expected {
				t.Errorf("applyCasing(%q, %q, %q) = %q, want %q",
					tt.original, tt.cleaned, tt.correction, result, tt.expected)
			}
		})
	}
}

// -------------------------------------------------------------------------
// Tests for Entropy-Based Noise Detection
// -------------------------------------------------------------------------

func TestTextRepair_CalculateLineEntropy(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name          string
		line          string
		expectedRange [2]float64 // min, max expected entropy
	}{
		{
			name:          "normal English text",
			line:          "The quick brown fox jumps over the lazy dog.",
			expectedRange: [2]float64{3.0, 4.5},
		},
		{
			name:          "repeated character",
			line:          "aaaaaaaaaa",
			expectedRange: [2]float64{0.0, 0.5},
		},
		{
			name:          "random characters",
			line:          "xk3#@fj&*2lq9z%",
			expectedRange: [2]float64{3.5, 5.5},
		},
		{
			name:          "empty line",
			line:          "",
			expectedRange: [2]float64{0.0, 0.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entropy := tr.CalculateLineEntropy(tt.line)
			if entropy < tt.expectedRange[0] || entropy > tt.expectedRange[1] {
				t.Errorf("CalculateLineEntropy(%q) = %f, want between %f and %f",
					tt.line, entropy, tt.expectedRange[0], tt.expectedRange[1])
			}
			t.Logf("Entropy of %q: %f", tt.line, entropy)
		})
	}
}

func TestTextRepair_IsNoiseLine(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name      string
		line      string
		wantNoise bool
	}{
		{
			name:      "normal English text",
			line:      "The court heard the case on Monday.",
			wantNoise: false,
		},
		{
			name:      "garbled random chars",
			line:      "xK3#@fJq&*2LqZ%mN8!pR",
			wantNoise: true,
		},
		{
			name:      "mostly symbols",
			line:      "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./",
			wantNoise: true,
		},
		{
			name:      "empty line",
			line:      "",
			wantNoise: false,
		},
		{
			name:      "short line",
			line:      "Test",
			wantNoise: false,
		},
		{
			name:      "high entropy gibberish",
			line:      "aZ9$bY8#cX7@dW6!eV5%fU4^gT3&hS2*",
			wantNoise: true,
		},
		{
			name:      "Unicode replacement chars",
			line:      "Some text \ufffd\ufffd\ufffd with replacement chars",
			wantNoise: false, // Not enough to trigger (< 30% unusual)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.IsNoiseLine(tt.line)
			if result != tt.wantNoise {
				t.Errorf("IsNoiseLine(%q) = %v, want %v (entropy: %f)",
					tt.line, result, tt.wantNoise, tr.CalculateLineEntropy(tt.line))
			}
		})
	}
}

func TestTextRepair_FilterNoiseLines(t *testing.T) {
	tr := NewTextRepair()

	input := `Normal text line
Some more normal text
xK3#@fJq&*2LqZ%mN8!pR
Another good line
!@#$%^&*()_+-={}[]|\\:;
Final normal line`

	result := tr.FilterNoiseLines(input)

	// Should remove the garbled lines
	if strings.Contains(result, "xK3#@fJq") {
		t.Error("FilterNoiseLines should have removed garbled line")
	}
	if strings.Contains(result, "!@#$%^&*()") {
		t.Error("FilterNoiseLines should have removed symbol-only line")
	}

	// Should keep normal lines
	if !strings.Contains(result, "Normal text line") {
		t.Error("FilterNoiseLines should have kept normal text")
	}
	if !strings.Contains(result, "Final normal line") {
		t.Error("FilterNoiseLines should have kept final normal line")
	}

	t.Logf("Filtered result:\n%s", result)
}

// -------------------------------------------------------------------------
// Tests for Font Encoding Detection
// -------------------------------------------------------------------------

func TestTextRepair_DetectFontEncodingCorruption(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name          string
		text          string
		wantCorrupted bool
		minScore      float64
	}{
		{
			name:          "normal English text",
			text:          "The quick brown fox jumps over the lazy dog. This is a sample of normal English.",
			wantCorrupted: false,
		},
		{
			name:          "encoded case number pattern",
			text:          "NWNRJcvJMTQPPJLAP",
			wantCorrupted: true,
			minScore:      0.5,
		},
		{
			name:          "random consonant clusters",
			text:          "XKJRLMWQ NPTQRST BCDFGHJK without vowels",
			wantCorrupted: true,
			minScore:      0.3,
		},
		{
			name:          "short text",
			text:          "Hello",
			wantCorrupted: false, // Too short
		},
		{
			name:          "legal text normal",
			text:          "UNITED STATES DISTRICT COURT for the Southern District of New York",
			wantCorrupted: false,
		},
		{
			name:          "corrupted header",
			text:          "VJLWHG VWDWHV GLVWULFW FRXUW",
			wantCorrupted: true,
			minScore:      0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := tr.DetectFontEncodingCorruption(tt.text)
			isCorrupted := score > 0.5

			if isCorrupted != tt.wantCorrupted {
				t.Errorf("DetectFontEncodingCorruption(%q) = %.2f, corrupted=%v, want corrupted=%v",
					tt.text, score, isCorrupted, tt.wantCorrupted)
			}

			if tt.wantCorrupted && tt.minScore > 0 && score < tt.minScore {
				t.Errorf("DetectFontEncodingCorruption() score = %.2f, want >= %.2f",
					score, tt.minScore)
			}
		})
	}
}

func TestTextRepair_IsFontEncodingCorrupted(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name          string
		text          string
		wantCorrupted bool
	}{
		{
			name:          "normal text",
			text:          "The court was in session today for the trial.",
			wantCorrupted: false,
		},
		{
			name:          "encoded text",
			text:          "NWNRJcvJMTQPPJLAP", // Looks like 1:15-cv-07433-LAP encoded
			wantCorrupted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.IsFontEncodingCorrupted(tt.text)
			if result != tt.wantCorrupted {
				t.Errorf("IsFontEncodingCorrupted(%q) = %v, want %v",
					tt.text, result, tt.wantCorrupted)
			}
		})
	}
}

func TestTextRepair_DetectEncodedPattern(t *testing.T) {
	tr := NewTextRepair()

	tests := []struct {
		name            string
		text            string
		wantPatternType string
	}{
		{
			name:            "encoded case number with cv",
			text:            "NWNRJcvJMTQPPJLAP",
			wantPatternType: "case_number",
		},
		{
			name:            "case number with cv-",
			text:            "15-cv-07433-LAP",
			wantPatternType: "case_number",
		},
		{
			name:            "normal text",
			text:            "The quick brown fox",
			wantPatternType: "",
		},
		{
			name:            "too short",
			text:            "1:15-cv",
			wantPatternType: "", // Too short
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patternType, _ := tr.DetectEncodedPattern(tt.text)
			if patternType != tt.wantPatternType {
				t.Errorf("DetectEncodedPattern(%q) patternType = %q, want %q",
					tt.text, patternType, tt.wantPatternType)
			}
		})
	}
}

func TestTextRepair_FilterFontEncodingCorruption(t *testing.T) {
	tr := NewTextRepair()

	input := `Normal text line
VJLWHG VWDWHV GLVWULFW FRXUW
Another good line
NWNRJcvJMTQPPJLAP
Final normal line`

	result := tr.FilterFontEncodingCorruption(input)

	// Should remove severely corrupted lines
	if strings.Contains(result, "VJLWHG VWDWHV") {
		t.Error("FilterFontEncodingCorruption should have removed corrupted line")
	}

	// Should keep normal lines
	if !strings.Contains(result, "Normal text line") {
		t.Error("FilterFontEncodingCorruption should have kept normal text")
	}
	if !strings.Contains(result, "Final normal line") {
		t.Error("FilterFontEncodingCorruption should have kept final normal line")
	}

	// Case number pattern should be marked (if detected)
	t.Logf("Filtered result:\n%s", result)
}
