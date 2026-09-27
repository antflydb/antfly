// Copyright 2026 Antfly, Inc.
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

package docsaf

import (
	"strings"
)

// DefaultOCRMinContent is the default minimum character count before OCR fallback triggers.
const DefaultOCRMinContent = 50

// DefaultOCRRenderDPI is the default DPI for rendering PDF pages to images for OCR.
const DefaultOCRRenderDPI = 150.0

// NeedsOCRFallback checks whether extracted text is too short, garbled, or
// corrupted and should be replaced by OCR output.
func NeedsOCRFallback(text string, minContentLen int) bool {
	text = strings.TrimSpace(text)

	if len(text) < minContentLen {
		return true
	}

	if HasGarbledPatterns(text) {
		return true
	}

	if HasFontEncodingCorruption(text) {
		return true
	}

	if ReplacementCharRatio(text) > 0.05 {
		return true
	}

	return false
}

// HasGarbledPatterns detects text where many of the first 50 words are
// single characters, suggesting garbled header text from PDF extraction.
func HasGarbledPatterns(text string) bool {
	words := strings.Fields(text)
	if len(words) < 20 {
		return false
	}

	sampleSize := min(50, len(words))
	singleCharWords := 0
	for _, w := range words[:sampleSize] {
		if len(w) == 1 {
			r := rune(w[0])
			// Exclude common standalone characters in formatted text.
			if r != '.' && r != '-' && r != 'X' && r != 'x' && r != 'v' && r != ':' {
				singleCharWords++
			}
		}
	}

	return float64(singleCharWords)/float64(sampleSize) > 0.4
}

// HasFontEncodingCorruption checks whether more than 20% of substantial
// lines (≥15 chars) show font-encoding corruption patterns.
func HasFontEncodingCorruption(text string) bool {
	tr := NewTextRepair()

	lines := strings.Split(text, "\n")
	corruptedLines := 0
	totalLines := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) < 15 {
			continue
		}
		totalLines++

		if tr.IsFontEncodingCorrupted(trimmed) {
			corruptedLines++
		}
	}

	if totalLines == 0 {
		return false
	}
	return float64(corruptedLines)/float64(totalLines) > 0.2
}

// ReplacementCharRatio returns the fraction of characters in text that are
// Unicode replacement characters (U+FFFD), indicating encoding failures.
func ReplacementCharRatio(text string) float64 {
	if len(text) == 0 {
		return 0
	}

	count := 0
	total := 0
	for _, r := range text {
		total++
		if r == '\uFFFD' {
			count++
		}
	}

	return float64(count) / float64(total)
}
