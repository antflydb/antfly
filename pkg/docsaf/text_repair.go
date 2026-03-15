package docsaf

import (
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/ajroetker/pdf"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// TextRepair provides utilities for detecting and fixing common PDF text extraction issues.
type TextRepair struct {
	// Configuration
	HeaderFooterMargin float64 // Percentage of page height to consider as header/footer region (0.1 = 10%)
	MinPagesSeen       int     // Minimum pages to analyze before detecting headers/footers

	// State for page-association detection
	pageFirstLines []string
	pageLastLines  []string
	pageCount      int
}

// NewTextRepair creates a new TextRepair with sensible defaults.
func NewTextRepair() *TextRepair {
	return &TextRepair{
		HeaderFooterMargin: 0.1, // 10% of page height
		MinPagesSeen:       3,   // Need at least 3 pages to detect patterns
		pageFirstLines:     make([]string, 0),
		pageLastLines:      make([]string, 0),
	}
}

// -------------------------------------------------------------------------
// Encoding Detection and Repair (ROT-N, Symbol Substitution)
// -------------------------------------------------------------------------

// EnglishLetterFrequency contains standard English letter frequencies.
var EnglishLetterFrequency = map[rune]float64{
	'e': 0.127, 't': 0.091, 'a': 0.082, 'o': 0.075, 'i': 0.070,
	'n': 0.067, 's': 0.063, 'h': 0.061, 'r': 0.060, 'd': 0.043,
	'l': 0.040, 'c': 0.028, 'u': 0.028, 'm': 0.024, 'w': 0.024,
	'f': 0.022, 'g': 0.020, 'y': 0.020, 'p': 0.019, 'b': 0.015,
	'v': 0.010, 'k': 0.008, 'j': 0.002, 'x': 0.002, 'q': 0.001,
	'z': 0.001,
}

// CommonSymbolSubstitutions maps common symbol substitutions in encoded PDFs.
// Key: encoded symbol, Value: decoded character
var CommonSymbolSubstitutions = map[rune]rune{
	'$': 'A', '%': 'B', '&': 'C', '\'': 'D', '(': 'E', ')': 'F',
	'*': 'G', '+': 'H', ',': 'I', '-': 'J', '.': 'K', '/': 'L',
	'0': 'M', '1': 'N', '2': 'O', '3': 'P', '4': 'Q', '5': 'R',
	'6': 'S', '7': 'T', '8': 'U', '9': 'V', ':': 'W', ';': 'X',
	'<': 'Y', '=': 'Z',
}

// DetectEncodingShift analyzes text to detect if it uses a shifted alphabet encoding.
// Returns the detected shift (0-25) and a confidence score (0.0-1.0).
// A shift of 0 means no encoding detected or text is normal.
func (tr *TextRepair) DetectEncodingShift(text string) (shift int, confidence float64) {
	// Skip if text is too short
	if len(text) < 50 {
		return 0, 0.0
	}

	// Calculate letter frequency of the text
	letterCount := make(map[rune]int)
	totalLetters := 0

	for _, r := range strings.ToLower(text) {
		if r >= 'a' && r <= 'z' {
			letterCount[r]++
			totalLetters++
		}
	}

	if totalLetters < 30 {
		return 0, 0.0 // Not enough letters to analyze
	}

	// Convert to frequency distribution
	textFreq := make(map[rune]float64)
	for r, count := range letterCount {
		textFreq[r] = float64(count) / float64(totalLetters)
	}

	// Test each possible shift (0-25)
	bestShift := 0
	bestScore := 0.0

	for testShift := range 26 {
		// Apply shift to text frequency and compare with English
		score := tr.calculateFrequencyMatchScore(textFreq, testShift)
		if score > bestScore {
			bestScore = score
			bestShift = testShift
		}
	}

	// Calculate confidence based on how much better the best shift is vs shift=0
	noShiftScore := tr.calculateFrequencyMatchScore(textFreq, 0)

	// If best shift is 0, no encoding detected
	if bestShift == 0 {
		return 0, bestScore
	}

	// Confidence is based on improvement over no-shift
	confidence = (bestScore - noShiftScore) / (1.0 - noShiftScore + 0.001)
	if confidence > 1.0 {
		confidence = 1.0
	}
	if confidence < 0 {
		confidence = 0
	}

	return bestShift, confidence
}

// calculateFrequencyMatchScore calculates how well the text frequency matches English
// when shifted by the given amount.
func (tr *TextRepair) calculateFrequencyMatchScore(textFreq map[rune]float64, shift int) float64 {
	// Calculate dot product of shifted text frequency with English frequency
	dotProduct := 0.0
	textMag := 0.0
	engMag := 0.0

	for r := 'a'; r <= 'z'; r++ {
		// Shift the character back
		shiftedR := 'a' + (r-'a'+rune(26-shift))%26 //nolint:gosec // shift is 0-25, no overflow

		textF := textFreq[r]
		engF := EnglishLetterFrequency[shiftedR]

		dotProduct += textF * engF
		textMag += textF * textF
		engMag += engF * engF
	}

	// Cosine similarity
	if textMag == 0 || engMag == 0 {
		return 0.0
	}
	return dotProduct / (math.Sqrt(textMag) * math.Sqrt(engMag))
}

// DecodeShiftedText decodes text that has been shifted by the specified amount.
func (tr *TextRepair) DecodeShiftedText(text string, shift int) string {
	if shift == 0 {
		return text
	}

	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if r >= 'A' && r <= 'Z' {
			// Shift uppercase back
			shifted := 'A' + (r-'A'+rune(26-shift))%26 //nolint:gosec // shift is 0-25, no overflow
			result.WriteRune(shifted)
		} else if r >= 'a' && r <= 'z' {
			// Shift lowercase back
			shifted := 'a' + (r-'a'+rune(26-shift))%26 //nolint:gosec // shift is 0-25, no overflow
			result.WriteRune(shifted)
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// DetectSymbolSubstitution checks if text uses symbol-to-letter substitution.
// Returns the substitution map and confidence score.
func (tr *TextRepair) DetectSymbolSubstitution(text string) (map[rune]rune, float64) {
	// Count symbols that could be substituted letters
	symbolCount := 0
	letterCount := 0

	for _, r := range text {
		if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' {
			letterCount++
		} else if _, isSubst := CommonSymbolSubstitutions[r]; isSubst {
			symbolCount++
		}
	}

	// If high symbol ratio compared to letters, likely substitution
	total := symbolCount + letterCount
	if total < 20 {
		return nil, 0.0
	}

	symbolRatio := float64(symbolCount) / float64(total)

	// If >30% symbols that could be letters, likely encoded
	if symbolRatio > 0.3 {
		return CommonSymbolSubstitutions, symbolRatio
	}

	return nil, 0.0
}

// DecodeSymbolSubstitution applies symbol substitution decoding to text.
func (tr *TextRepair) DecodeSymbolSubstitution(text string, substMap map[rune]rune) string {
	if substMap == nil {
		return text
	}

	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if sub, ok := substMap[r]; ok {
			result.WriteRune(sub)
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// AutoDecodeText automatically detects and decodes text with encoding issues.
// Returns the decoded text and a description of what was fixed.
func (tr *TextRepair) AutoDecodeText(text string) (decoded string, fixed string) {
	// First, check if text already looks like valid English.
	// If so, don't apply transformations that could corrupt it.
	if tr.looksLikeValidEnglish(text) {
		return text, ""
	}

	// Try symbol substitution first
	substMap, substConf := tr.DetectSymbolSubstitution(text)
	if substConf > 0.3 {
		substituted := tr.DecodeSymbolSubstitution(text, substMap)
		// Only accept the substitution if it actually improves the text
		if !tr.looksLikeValidEnglish(substituted) {
			// Substitution didn't help, keep original
		} else {
			decoded = substituted
			// Now check if the result needs shift decoding
			shift, shiftConf := tr.DetectEncodingShift(decoded)
			if shiftConf > 0.5 && shift != 0 {
				decoded = tr.DecodeShiftedText(decoded, shift)
				return decoded, "symbol_substitution+shift"
			}
			return decoded, "symbol_substitution"
		}
	}

	// Try shift encoding
	shift, shiftConf := tr.DetectEncodingShift(text)
	if shiftConf > 0.5 && shift != 0 {
		decoded = tr.DecodeShiftedText(text, shift)
		return decoded, "shift"
	}

	return text, ""
}

// looksLikeValidEnglish checks if text appears to be readable English.
// Returns true if the text has recognizable English words and normal structure.
func (tr *TextRepair) looksLikeValidEnglish(text string) bool {
	words := strings.Fields(text)
	if len(words) < 3 {
		return false
	}

	recognizedCount := 0
	totalChecked := 0

	for _, word := range words {
		// Clean punctuation for checking
		cleaned := strings.Trim(word, ".,!?;:\"'()[]{}$%")
		lower := strings.ToLower(cleaned)

		// Skip very short words and numbers/case numbers
		if len(cleaned) <= 2 {
			continue
		}
		// Skip words that look like identifiers (contain digits mixed with letters)
		if containsDigit(cleaned) {
			continue
		}

		totalChecked++
		if CommonEnglishWords[lower] {
			recognizedCount++
		}
	}

	// If we checked at least 3 words and >40% are recognized, text is valid
	if totalChecked >= 3 && float64(recognizedCount)/float64(totalChecked) > 0.4 {
		return true
	}

	return false
}

// containsDigit returns true if the string contains any digit.
func containsDigit(s string) bool {
	for _, r := range s {
		if unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------
// Private Use Area (PUA) Decoding for Unmapped Font Bytes
// -------------------------------------------------------------------------

// IsPUAChar returns true if the rune is in the Private Use Area range
// used to preserve unmapped font bytes (U+E000-U+E0FF).
func IsPUAChar(r rune) bool {
	return r >= 0xE000 && r <= 0xE0FF
}

// HasPUAChars returns true if the text contains any PUA characters.
func HasPUAChars(text string) bool {
	for _, r := range text {
		if IsPUAChar(r) {
			return true
		}
	}
	return false
}

// CountPUAChars returns the count and ratio of PUA characters in text.
func CountPUAChars(text string) (count int, ratio float64) {
	total := 0
	for _, r := range text {
		total++
		if IsPUAChar(r) {
			count++
		}
	}
	if total == 0 {
		return 0, 0.0
	}
	return count, float64(count) / float64(total)
}

// DecodePUAWithShift decodes PUA-preserved bytes by applying a character shift.
// This handles custom font encodings where characters are shifted by a fixed offset.
// For example, a shift of 29 would decode PUA byte 65 as 65+29=94 ('n' in some encodings).
func DecodePUAWithShift(text string, shift int) string {
	if !HasPUAChars(text) {
		return text
	}

	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if IsPUAChar(r) {
			// Extract original byte from PUA encoding
			originalByte := int(r) - 0xE000

			// Apply shift (with wraparound for printable ASCII, preserving space)
			decoded := originalByte + shift
			if decoded > 126 {
				decoded = 32 + (decoded - 127)
			}
			if decoded < 32 {
				decoded = 126 - (32 - decoded) + 1
			}

			result.WriteRune(rune(decoded)) //nolint:gosec // decoded is bounded to printable ASCII range 32-126
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// DetectPUAShift analyzes text with PUA characters to detect the encoding shift.
// Returns the best shift value and confidence score based on resulting English frequency.
func (tr *TextRepair) DetectPUAShift(text string) (shift int, confidence float64) {
	if !HasPUAChars(text) {
		return 0, 0.0
	}

	bestShift := 0
	bestScore := 0.0

	// Test shifts from 1 to 127 (full byte range)
	for testShift := 1; testShift <= 127; testShift++ {
		decoded := DecodePUAWithShift(text, testShift)

		// Calculate how English-like the result is
		score := tr.calculateEnglishScore(decoded)
		if score > bestScore {
			bestScore = score
			bestShift = testShift
		}
	}

	// Confidence is based on how English-like the best result is
	confidence = bestScore

	return bestShift, confidence
}

// calculateEnglishScore returns a score (0.0-1.0) indicating how English-like text is.
func (tr *TextRepair) calculateEnglishScore(text string) float64 {
	// Count common English patterns
	lower := strings.ToLower(text)
	words := strings.Fields(lower)

	if len(words) < 2 {
		return 0.0
	}

	// Check for common English words
	commonWords := []string{"the", "and", "that", "have", "for", "not", "with", "you", "this", "but", "from", "they", "was", "are", "been"}
	foundCommon := 0
	for _, w := range words {
		if slices.Contains(commonWords, w) {
			foundCommon++
		}
	}

	// Calculate vowel ratio (English typically has 35-45% vowels)
	vowels := 0
	letters := 0
	for _, r := range lower {
		if r >= 'a' && r <= 'z' {
			letters++
			if r == 'a' || r == 'e' || r == 'i' || r == 'o' || r == 'u' {
				vowels++
			}
		}
	}

	vowelRatio := 0.0
	if letters > 0 {
		vowelRatio = float64(vowels) / float64(letters)
	}

	// Score components
	wordScore := float64(foundCommon) / float64(len(words))
	vowelScore := 1.0 - math.Abs(vowelRatio-0.40)*2.5 // Optimal around 40%
	if vowelScore < 0 {
		vowelScore = 0
	}

	// Combined score
	return wordScore*0.7 + vowelScore*0.3
}

// AutoDecodePUA attempts to automatically decode PUA-preserved bytes.
// Returns the decoded text and a description of the decoding applied.
func (tr *TextRepair) AutoDecodePUA(text string) (decoded string, description string) {
	if !HasPUAChars(text) {
		return text, ""
	}

	shift, confidence := tr.DetectPUAShift(text)
	if confidence > 0.2 && shift != 0 {
		decoded = DecodePUAWithShift(text, shift)
		return decoded, "pua_shift_" + string(rune('0'+shift/100)) + string(rune('0'+(shift/10)%10)) + string(rune('0'+shift%10)) //nolint:gosec // shift is small PUA offset, fits in rune
	}

	return text, ""
}

// -------------------------------------------------------------------------
// Mirrored/Reversed Text Detection and Repair (Bigram Analysis)
// -------------------------------------------------------------------------

// EnglishBigramFrequency contains the top English bigram frequencies.
// These are used to detect reversed text by comparing bigram distributions.
var EnglishBigramFrequency = map[string]float64{
	"th": 0.0356, "he": 0.0307, "in": 0.0243, "er": 0.0205, "an": 0.0199,
	"re": 0.0185, "on": 0.0176, "at": 0.0149, "en": 0.0145, "nd": 0.0135,
	"ti": 0.0134, "es": 0.0134, "or": 0.0128, "te": 0.0120, "of": 0.0117,
	"ed": 0.0117, "is": 0.0113, "it": 0.0112, "al": 0.0109, "ar": 0.0107,
	"st": 0.0105, "to": 0.0104, "nt": 0.0104, "ng": 0.0095, "se": 0.0093,
	"ha": 0.0093, "as": 0.0087, "ou": 0.0087, "io": 0.0083, "le": 0.0083,
	"ve": 0.0083, "co": 0.0079, "me": 0.0079, "de": 0.0076, "hi": 0.0076,
	"ri": 0.0073, "ro": 0.0073, "ic": 0.0070, "ne": 0.0069, "ea": 0.0069,
	"ra": 0.0069, "ce": 0.0065, "li": 0.0062, "ch": 0.0060, "ll": 0.0058,
	"be": 0.0058, "ma": 0.0057, "si": 0.0055, "om": 0.0055, "ur": 0.0054,
}

// CommonReversedWords contains common English words and their reversed forms.
// Used as a secondary check for mirrored text.
var CommonReversedWords = map[string]string{
	"eht": "the", "dna": "and", "rof": "for", "era": "are", "tub": "but",
	"ton": "not", "uoy": "you", "lla": "all", "nac": "can", "dah": "had",
	"reh": "her", "saw": "was", "eno": "one", "ruo": "our", "tuo": "out",
	"yad": "day", "teg": "get", "sah": "has", "mih": "him", "sih": "his",
	"woh": "how", "nam": "man", "wen": "new", "won": "now", "dlo": "old",
	"ees": "see", "emit": "time", "yrev": "very", "nehw": "when", "ohw": "who",
	"yob": "boy", "did": "did", "sti": "its", "tel": "let", "tup": "put",
	"yas": "say", "ehs": "she", "owt": "two", "yaw": "way", "that": "that",
	"siht": "this", "htiw": "with", "evah": "have", "morf": "from", "yeht": "they",
	"neeb": "been", "evol": "love", "ekam": "make", "erom": "more", "ylno": "only",
	"revo": "over", "hcus": "such", "ekat": "take", "naht": "than", "meht": "them",
	"neht": "then", "eseht": "these", "gniht": "thing", "kniht": "think",
	"erehw": "where", "hcihw": "which", "dlrow": "world", "dluow": "would",
	"tuoba": "about", "retfa": "after", "niaga": "again", "tsniaga": "against",
}

// DetectMirroredText analyzes text to detect if it appears to be reversed/mirrored.
// Returns a confidence score (0.0-1.0) where higher values indicate more likely mirroring.
func (tr *TextRepair) DetectMirroredText(text string) float64 {
	text = strings.ToLower(text)

	// Skip if text is too short
	if len(text) < 30 {
		return 0.0
	}

	// Method 1: Check for reversed common words
	wordScore := tr.detectReversedWords(text)

	// Method 2: Bigram frequency analysis
	bigramScore := tr.detectReversedBigrams(text)

	// Combine scores - if either is high, text is likely mirrored
	// Weight bigram analysis more heavily as it's more robust
	combinedScore := wordScore*0.4 + bigramScore*0.6

	return combinedScore
}

// detectReversedWords checks for known reversed English words in the text.
func (tr *TextRepair) detectReversedWords(text string) float64 {
	words := strings.Fields(text)
	if len(words) < 5 {
		return 0.0
	}

	reversedCount := 0
	checkedCount := 0

	for _, word := range words {
		// Clean the word
		word = strings.Trim(word, ".,!?;:\"'()[]{}$%")
		if len(word) < 3 || len(word) > 10 {
			continue
		}

		checkedCount++
		if _, isReversed := CommonReversedWords[word]; isReversed {
			reversedCount++
		}
	}

	if checkedCount < 5 {
		return 0.0
	}

	// Return ratio of reversed words found
	return float64(reversedCount) / float64(checkedCount) * 3.0 // Scale up since we won't find all
}

// detectReversedBigrams uses bigram frequency analysis to detect mirrored text.
// Compares bigram frequencies of original text vs reversed text against English baseline.
func (tr *TextRepair) detectReversedBigrams(text string) float64 {
	// Extract bigrams from original text
	originalBigrams := tr.extractBigrams(text)
	if len(originalBigrams) < 10 {
		return 0.0
	}

	// Calculate score for original text
	originalScore := tr.calculateBigramScore(originalBigrams)

	// Calculate score for reversed text
	reversedText := tr.reverseString(text)
	reversedBigrams := tr.extractBigrams(reversedText)
	reversedScore := tr.calculateBigramScore(reversedBigrams)

	// If reversed text scores significantly higher, original is mirrored
	if reversedScore > originalScore*1.2 {
		// Confidence based on how much better reversed is
		confidence := (reversedScore - originalScore) / (reversedScore + 0.001)
		if confidence > 1.0 {
			confidence = 1.0
		}
		return confidence
	}

	return 0.0
}

// extractBigrams extracts all letter bigrams from text.
func (tr *TextRepair) extractBigrams(text string) map[string]int {
	bigrams := make(map[string]int)
	text = strings.ToLower(text)

	prevLetter := rune(0)
	for _, r := range text {
		if r >= 'a' && r <= 'z' {
			if prevLetter != 0 {
				bigram := string([]rune{prevLetter, r})
				bigrams[bigram]++
			}
			prevLetter = r
		} else {
			prevLetter = 0 // Reset on non-letter
		}
	}

	return bigrams
}

// calculateBigramScore calculates how well a bigram distribution matches English.
func (tr *TextRepair) calculateBigramScore(bigrams map[string]int) float64 {
	if len(bigrams) == 0 {
		return 0.0
	}

	// Count total bigrams
	total := 0
	for _, count := range bigrams {
		total += count
	}

	// Calculate weighted sum based on English bigram frequencies
	score := 0.0
	for bigram, count := range bigrams {
		if englishFreq, ok := EnglishBigramFrequency[bigram]; ok {
			// Weight by how common this bigram is in English
			score += englishFreq * float64(count) / float64(total)
		}
	}

	return score
}

// reverseString reverses a string, preserving Unicode characters.
func (tr *TextRepair) reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// RepairMirroredText reverses text that has been detected as mirrored.
// It can operate at word level or full text level.
func (tr *TextRepair) RepairMirroredText(text string) string {
	// First, check if the whole text should be reversed
	confidence := tr.DetectMirroredText(text)
	if confidence < 0.3 {
		return text // Not mirrored
	}

	// Try word-by-word reversal first (handles some PDF extraction patterns)
	wordReversed := tr.reverseWords(text)
	wordReversedScore := tr.DetectMirroredText(wordReversed)

	// Try full text reversal
	fullReversed := tr.reverseString(text)
	fullReversedScore := tr.DetectMirroredText(fullReversed)

	// Return the version that scores lowest (least mirrored)
	if wordReversedScore < fullReversedScore && wordReversedScore < confidence {
		return wordReversed
	}
	if fullReversedScore < confidence {
		return fullReversed
	}

	return text // Return original if neither reversal helps
}

// reverseWords reverses each word in the text individually.
func (tr *TextRepair) reverseWords(text string) string {
	words := strings.Fields(text)
	for i, word := range words {
		words[i] = tr.reverseString(word)
	}
	return strings.Join(words, " ")
}

// AutoRepairMirroredText detects and repairs mirrored text if confidence is high enough.
// Returns the repaired text and whether repair was applied.
func (tr *TextRepair) AutoRepairMirroredText(text string) (string, bool) {
	confidence := tr.DetectMirroredText(text)
	if confidence >= 0.4 {
		repaired := tr.RepairMirroredText(text)
		// Verify repair actually improved things
		newConfidence := tr.DetectMirroredText(repaired)
		if newConfidence < confidence*0.5 {
			return repaired, true
		}
	}
	return text, false
}

// -------------------------------------------------------------------------
// Header/Footer Detection via Page-Association
// -------------------------------------------------------------------------

// RecordPageContent records the first and last lines of a page for pattern detection.
func (tr *TextRepair) RecordPageContent(pageText string) {
	lines := strings.Split(strings.TrimSpace(pageText), "\n")
	if len(lines) == 0 {
		return
	}

	tr.pageCount++

	// Record first line (header candidate)
	firstLine := strings.TrimSpace(lines[0])
	if len(firstLine) > 0 {
		tr.pageFirstLines = append(tr.pageFirstLines, firstLine)
	}

	// Record last line (footer candidate)
	lastLine := strings.TrimSpace(lines[len(lines)-1])
	if len(lastLine) > 0 {
		tr.pageLastLines = append(tr.pageLastLines, lastLine)
	}
}

// GetDetectedHeaders returns headers detected across multiple pages.
// A line is considered a header if it appears (with edit distance tolerance) on most pages.
func (tr *TextRepair) GetDetectedHeaders() []string {
	return tr.findRepeatingPatterns(tr.pageFirstLines, 0.6)
}

// GetDetectedFooters returns footers detected across multiple pages.
func (tr *TextRepair) GetDetectedFooters() []string {
	return tr.findRepeatingPatterns(tr.pageLastLines, 0.6)
}

// findRepeatingPatterns finds lines that appear on at least threshold fraction of pages.
func (tr *TextRepair) findRepeatingPatterns(lines []string, threshold float64) []string {
	if len(lines) < tr.MinPagesSeen {
		return nil
	}

	// Group similar lines using edit distance
	type lineGroup struct {
		representative string
		count          int
	}

	var groups []lineGroup

	for _, line := range lines {
		found := false
		for i := range groups {
			if tr.isSimilar(line, groups[i].representative) {
				groups[i].count++
				found = true
				break
			}
		}
		if !found {
			groups = append(groups, lineGroup{representative: line, count: 1})
		}
	}

	// Find groups that appear on enough pages
	minCount := int(float64(len(lines)) * threshold)
	var patterns []string

	for _, g := range groups {
		if g.count >= minCount {
			patterns = append(patterns, g.representative)
		}
	}

	return patterns
}

// isSimilar checks if two lines are similar using Levenshtein-like distance.
// Returns true if the normalized edit distance is < 0.3
func (tr *TextRepair) isSimilar(a, b string) bool {
	// Normalize: lowercase, remove page numbers
	aNorm := tr.normalizeForComparison(a)
	bNorm := tr.normalizeForComparison(b)

	if aNorm == bNorm {
		return true
	}

	// Calculate simple edit distance for short strings
	maxLen := max(len(bNorm), len(aNorm))
	if maxLen == 0 {
		return true
	}

	dist := tr.levenshteinDistance(aNorm, bNorm)
	normalizedDist := float64(dist) / float64(maxLen)

	return normalizedDist < 0.3
}

// normalizeForComparison normalizes text for header/footer comparison.
// Removes page numbers, dates, and other variable content.
func (tr *TextRepair) normalizeForComparison(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))

	// Remove page numbers like "Page 1", "1", "- 1 -", etc.
	pageNumPattern := regexp.MustCompile(`(?i)(page\s*)?\d+(\s*of\s*\d+)?|^\s*-?\s*\d+\s*-?\s*$`)
	s = pageNumPattern.ReplaceAllString(s, "")

	// Remove common date patterns
	datePattern := regexp.MustCompile(`\d{1,2}[/-]\d{1,2}[/-]\d{2,4}`)
	s = datePattern.ReplaceAllString(s, "")

	return strings.TrimSpace(s)
}

// levenshteinDistance calculates the Levenshtein edit distance between two strings.
func (tr *TextRepair) levenshteinDistance(a, b string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	// Limit comparison length for performance
	if len(a) > 100 {
		a = a[:100]
	}
	if len(b) > 100 {
		b = b[:100]
	}

	aRunes := []rune(a)
	bRunes := []rune(b)

	// Use two-row optimization
	prev := make([]int, len(bRunes)+1)
	curr := make([]int, len(bRunes)+1)

	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(aRunes); i++ {
		curr[0] = i
		for j := 1; j <= len(bRunes); j++ {
			cost := 1
			if aRunes[i-1] == bRunes[j-1] {
				cost = 0
			}
			curr[j] = min(min(prev[j]+1, curr[j-1]+1), prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}

	return prev[len(bRunes)]
}

// RemoveHeadersFooters removes detected headers and footers from page text.
func (tr *TextRepair) RemoveHeadersFooters(pageText string, headers, footers []string) string {
	lines := strings.Split(pageText, "\n")
	if len(lines) == 0 {
		return pageText
	}

	var result []string

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			result = append(result, line)
			continue
		}

		// Check if this line matches a header (first few lines)
		if i < 3 {
			isHeader := false
			for _, h := range headers {
				if tr.isSimilar(trimmed, h) {
					isHeader = true
					break
				}
			}
			if isHeader {
				continue // Skip this line
			}
		}

		// Check if this line matches a footer (last few lines)
		if i >= len(lines)-3 {
			isFooter := false
			for _, f := range footers {
				if tr.isSimilar(trimmed, f) {
					isFooter = true
					break
				}
			}
			if isFooter {
				continue // Skip this line
			}
		}

		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

// -------------------------------------------------------------------------
// Interleaved Character Removal
// -------------------------------------------------------------------------

// RemoveInterleavedReplacements removes U+FFFD replacement characters that appear
// to be interleaved with real text (pattern: char, FFFD, char, FFFD, ...).
// This fixes PDFs where font encoding issues produce "C·O·N·F·I·D·E·N·T·I·A·L"
// patterns where · is U+FFFD.
func (tr *TextRepair) RemoveInterleavedReplacements(text string) string {
	// Quick check - if no replacement chars, return as-is
	if !strings.ContainsRune(text, unicode.ReplacementChar) {
		return text
	}

	runes := []rune(text)
	if len(runes) < 3 {
		return text
	}

	// Check for interleaved pattern: real char followed by replacement char
	interleavedCount := 0
	for i := 0; i < len(runes)-1; i++ {
		if runes[i] != unicode.ReplacementChar && runes[i+1] == unicode.ReplacementChar {
			interleavedCount++
		}
	}

	// If we see significant interleaving (>20% of chars), remove replacements
	replacementCount := strings.Count(text, string(unicode.ReplacementChar))
	if replacementCount > 0 && float64(interleavedCount)/float64(replacementCount) > 0.3 {
		var result strings.Builder
		result.Grow(len(text))
		for _, r := range runes {
			if r != unicode.ReplacementChar {
				result.WriteRune(r)
			}
		}
		return result.String()
	}

	return text
}

// -------------------------------------------------------------------------
// Deposition Transcript Detection
// -------------------------------------------------------------------------

// DepositionLayoutConfig provides optimized settings for deposition transcripts.
var DepositionLayoutConfig = LayoutConfig{
	ColumnGapThreshold:  12.0, // Narrow gap for line number column
	RowTolerance:        2.0,  // Tight row grouping
	MinRowsForColumnPct: 75,   // Line numbers appear on 75%+ of rows
	FilterLineNumbers:   true, // Don't include line numbers in output
}

// LayoutConfig allows customization of layout analysis parameters.
type LayoutConfig struct {
	ColumnGapThreshold  float64 // Minimum gap for column detection
	RowTolerance        float64 // Y tolerance for row grouping
	MinRowsForColumnPct int     // % of rows that must have gap for column detection
	FilterLineNumbers   bool    // Whether to filter out line number columns
}

// DefaultLayoutConfig returns standard layout configuration.
func DefaultLayoutConfig() LayoutConfig {
	return LayoutConfig{
		ColumnGapThreshold:  30.0,
		RowTolerance:        3.0,
		MinRowsForColumnPct: 25,
		FilterLineNumbers:   false,
	}
}

// DetectDepositionLayout checks if a page appears to be a deposition transcript.
// Deposition transcripts have:
// - Line numbers 1-25 (or similar) in a narrow left column
// - Consistent line spacing
// - Q: and A: question/answer format
func (tr *TextRepair) DetectDepositionLayout(texts []pdf.Text) bool {
	if len(texts) < 50 {
		return false
	}

	// Find the leftmost X position (potential line number column)
	minX := texts[0].X
	for _, t := range texts {
		if t.X < minX {
			minX = t.X
		}
	}

	// Check for line numbers (1-25) in left column
	lineNumbersFound := 0
	lineNumberPattern := regexp.MustCompile(`^[1-9]$|^1[0-9]$|^2[0-5]$`)

	for _, t := range texts {
		// Only check leftmost column (within 30pt of min X)
		if t.X-minX < 30 {
			if lineNumberPattern.MatchString(strings.TrimSpace(t.S)) {
				lineNumbersFound++
			}
		}
	}

	// If we found 20+ line numbers, likely a deposition
	if lineNumbersFound >= 20 {
		return true
	}

	// Also check for Q: and A: patterns anywhere
	qaCount := 0
	for _, t := range texts {
		s := strings.TrimSpace(t.S)
		if s == "Q" || s == "A" || strings.HasPrefix(s, "Q:") || strings.HasPrefix(s, "A:") ||
			strings.HasPrefix(s, "Q.") || strings.HasPrefix(s, "A.") {
			qaCount++
		}
	}

	// If we found Q/A patterns with some line numbers, likely deposition
	return qaCount >= 5 && lineNumbersFound >= 10
}

// FilterLineNumberColumn removes the leftmost column if it contains only line numbers.
// It identifies line numbers (1-25) in the left margin and filters them out along with
// any other content in that narrow column, preserving the main text content.
func (tr *TextRepair) FilterLineNumberColumn(texts []pdf.Text) []pdf.Text {
	if len(texts) == 0 {
		return texts
	}

	// Find min X
	minX := texts[0].X
	for _, t := range texts {
		if t.X < minX {
			minX = t.X
		}
	}

	// First, identify which texts are actually line numbers (1-25)
	lineNumberPattern := regexp.MustCompile(`^[1-9]$|^1[0-9]$|^2[0-5]$`)
	var lineNumberXPositions []float64

	for _, t := range texts {
		s := strings.TrimSpace(t.S)
		if lineNumberPattern.MatchString(s) {
			// Only count if it's in the left portion (within 50pt of min X)
			if t.X-minX < 50 {
				lineNumberXPositions = append(lineNumberXPositions, t.X)
			}
		}
	}

	// Need a significant number of line numbers to justify filtering
	if len(lineNumberXPositions) < 10 {
		return texts // Not enough line numbers found, don't filter
	}

	// Find the max X position of line numbers - this defines the column boundary
	maxLineNumX := lineNumberXPositions[0]
	for _, x := range lineNumberXPositions {
		if x > maxLineNumX {
			maxLineNumX = x
		}
	}

	// Find the gap: look for significant space between line numbers and main text
	// Sort all X positions and find the first big gap after the line number column
	type textWithX struct {
		t pdf.Text
		x float64
	}
	var allTexts []textWithX
	for _, t := range texts {
		allTexts = append(allTexts, textWithX{t: t, x: t.X})
	}
	sort.Slice(allTexts, func(i, j int) bool {
		return allTexts[i].x < allTexts[j].x
	})

	// Look for a gap > 15pt after the line number column
	lineNumColumnEnd := maxLineNumX + 5 // Small buffer after last line number
	for i := 0; i < len(allTexts)-1; i++ {
		if allTexts[i].x <= maxLineNumX+10 && allTexts[i+1].x > maxLineNumX {
			gap := allTexts[i+1].x - allTexts[i].x
			if gap >= 15 {
				// Found a clear gap - use the midpoint as boundary
				lineNumColumnEnd = (allTexts[i].x + allTexts[i+1].x) / 2
				break
			}
		}
	}

	// Filter out texts in the line number column only if they're before the boundary
	// and within a reasonable distance from minX (line numbers are typically within 40pt)
	var filtered []pdf.Text
	for _, t := range texts {
		// Keep text if:
		// 1. It's beyond the line number column boundary, OR
		// 2. It's not in the left margin region (> 50pt from minX)
		if t.X > lineNumColumnEnd || t.X-minX > 50 {
			filtered = append(filtered, t)
		}
	}

	return filtered
}

// -------------------------------------------------------------------------
// Word Segmentation (for zero-gap PDFs)
// -------------------------------------------------------------------------

// CommonEnglishWords contains top 10000 common English words for word segmentation.
// This is a subset - in production, you'd want the full list.
var CommonEnglishWords = map[string]bool{
	// Top 100 most common words
	"the": true, "be": true, "to": true, "of": true, "and": true,
	"a": true, "in": true, "that": true, "have": true, "i": true,
	"it": true, "for": true, "not": true, "on": true, "with": true,
	"he": true, "as": true, "you": true, "do": true, "at": true,
	"this": true, "but": true, "his": true, "by": true, "from": true,
	"they": true, "we": true, "say": true, "her": true, "she": true,
	"or": true, "an": true, "will": true, "my": true, "one": true,
	"all": true, "would": true, "there": true, "their": true, "what": true,
	"so": true, "up": true, "out": true, "if": true, "about": true,
	"who": true, "get": true, "which": true, "go": true, "me": true,
	"when": true, "make": true, "can": true, "like": true, "time": true,
	"no": true, "just": true, "him": true, "know": true, "take": true,
	"people": true, "into": true, "year": true, "your": true, "good": true,
	"some": true, "could": true, "them": true, "see": true, "other": true,
	"than": true, "then": true, "now": true, "look": true, "only": true,
	"come": true, "its": true, "over": true, "think": true, "also": true,
	"back": true, "after": true, "use": true, "two": true, "how": true,
	"our": true, "work": true, "first": true, "well": true, "way": true,
	"even": true, "new": true, "want": true, "because": true, "any": true,
	"these": true, "give": true, "day": true, "most": true, "us": true,
	// Common legal/court words
	"court": true, "case": true, "law": true, "state": true, "united": true,
	"district": true, "plaintiff": true, "defendant": true, "attorney": true,
	"judge": true, "versus": true, "order": true, "motion": true, "evidence": true,
	"witness": true, "testimony": true, "trial": true, "jury": true, "verdict": true,
	"appeal": true, "hearing": true, "counsel": true, "party": true, "section": true,
	"statute": true, "claim": true, "damages": true, "contract": true, "agreement": true,
	// Add more common words as needed
	"was": true, "were": true, "been": true, "being": true, "am": true, "are": true, "is": true,
	"has": true, "had": true, "having": true, "did": true, "does": true, "done": true, "doing": true,
	"said": true, "says": true, "saying": true, "made": true, "makes": true, "making": true,
	"here": true, "where": true, "why": true,
	"each": true, "such": true, "much": true, "many": true, "few": true, "several": true,
	"between": true, "during": true, "before": true, "above": true, "below": true,
	"through": true, "under": true, "against": true, "within": true, "without": true,
	"including": true, "regarding": true, "according": true, "concerning": true,
	"shall": true, "may": true, "must": true, "should": true,
	"might": true, "ought": true, "need": true, "dare": true,
	// More common words for segmentation
	"room": true, "involving": true, "states": true, "session": true,
	// Email and document terms (common in legal/business documents)
	"email": true, "chain": true, "document": true, "documents": true, "draft": true,
	"redraft": true, "legal": true, "relating": true, "litigation": true, "product": true,
	"withheld": true, "interest": true, "common": true, "defense": true, "joint": true,
	"client": true, "privilege": true, "privileged": true, "confidential": true,
	"conveying": true, "information": true, "potential": true, "action": true,
	"sent": true, "date": true, "address": true, "subject": true,
	"matter": true, "type": true, "count": true, "page": true, "log": true,
	// Additional common words
	"office": true, "company": true, "meeting": true, "report": true, "project": true,
	"review": true, "response": true, "request": true, "notice": true, "letter": true,
	"memo": true, "file": true, "record": true, "number": true, "reference": true,
	// Common verb forms (to prevent OCR overcorrection)
	"filed": true, "claimed": true, "stated": true, "noted": true, "provided": true,
	"received": true, "issued": true, "signed": true, "dated": true, "submitted": true,
	// Additional domain-specific words
	"form": true, "forms": true, "media": true, "nature": true, "natural": true,
	"southern": true, "northern": true, "eastern": true, "western": true,
	"florida": true, "california": true, "texas": true,
	"deposition": true, "depositions": true, "interrogatory": true, "interrogatories": true,
	"presence": true, "present": true, "scheduled": true, "schedule": true,
	"household": true, "visited": true, "visitor": true, "visitors": true,
	"associated": true, "observation": true, "observations": true, "observed": true,
	"involvement": true, "anticipated": true, "testify": true,
	"massage": true, "massages": true, "female": true, "females": true,
	"underage": true, "activities": true, "activity": true, "sexual": true,
	"place": true, "places": true, "mansion": true, "closely": true,
	"minor": true, "minors": true, "girls": true, "girl": true,
	"respect": true, "prior": true,
	// Legal specific terms
	"objection": true, "objections": true, "sustained": true, "overruled": true,
	"depose": true, "deposed": true, "sworn": true, "affidavit": true,
	"subpoena": true, "summons": true, "complaint": true,
	"discovery": true, "exhibit": true, "exhibits": true, "stipulation": true,
	"allegation": true, "allegations": true, "liability": true, "negligence": true,
	"breach": true, "violations": true,
}

// SegmentWords uses dynamic programming to find optimal word boundaries in merged text.
// This handles PDFs with zero-gap character positioning that result in merged words
// like "UNITEDSTATESDISTRICTCOURT" -> "UNITED STATES DISTRICT COURT"
func (tr *TextRepair) SegmentWords(text string) string {
	if len(text) == 0 {
		return text
	}

	// Process line by line to preserve formatting
	lines := strings.Split(text, "\n")
	var result []string

	for _, line := range lines {
		if len(strings.TrimSpace(line)) == 0 {
			result = append(result, line)
			continue
		}

		// Segment words in this line
		segmented := tr.segmentLine(line)
		result = append(result, segmented)
	}

	return strings.Join(result, "\n")
}

// segmentLine segments a single line of merged text into words.
func (tr *TextRepair) segmentLine(line string) string {
	words := strings.Fields(line)
	var segmented []string

	for _, word := range words {
		// Skip email addresses and URLs - don't segment these
		if strings.Contains(word, "@") || strings.HasPrefix(strings.ToLower(word), "http") {
			segmented = append(segmented, word)
			continue
		}

		// Only segment words that look merged (long words not in dictionary)
		if len(word) > 8 && !tr.isLikelyProperWord(word) {
			parts := tr.segmentWord(strings.ToLower(word))
			if len(parts) > 1 {
				// Preserve original casing
				if tr.isAllUpper(word) {
					for i := range parts {
						parts[i] = strings.ToUpper(parts[i])
					}
				} else if tr.isTitleCase(word) {
					for i := range parts {
						parts[i] = cases.Title(language.English).String(parts[i])
					}
				}
				segmented = append(segmented, strings.Join(parts, " "))
				continue
			}
		}
		segmented = append(segmented, word)
	}

	return strings.Join(segmented, " ")
}

// segmentWord uses dynamic programming to find optimal word segmentation.
func (tr *TextRepair) segmentWord(word string) []string {
	n := len(word)
	if n == 0 {
		return []string{}
	}

	// dp[i] = best score for segmenting word[0:i]
	// parent[i] = index of start of last word in best segmentation
	dp := make([]float64, n+1)
	parent := make([]int, n+1)

	// Initialize: empty string has score 0
	dp[0] = 0
	for i := 1; i <= n; i++ {
		parent[i] = -1
		dp[i] = -1e9 // Very negative score
	}

	// Fill DP table
	for i := 1; i <= n; i++ {
		// Try all possible last words ending at position i
		for j := 0; j < i; j++ {
			candidate := word[j:i]
			wordScore := tr.scoreWord(candidate)

			// Score of segmentation ending with this word
			score := dp[j] + wordScore

			if score > dp[i] {
				dp[i] = score
				parent[i] = j
			}
		}
	}

	// Reconstruct segmentation by following parent pointers
	var parts []string
	pos := n
	for pos > 0 {
		start := parent[pos]
		if start == -1 {
			// Couldn't find valid segmentation
			return []string{word}
		}
		parts = append([]string{word[start:pos]}, parts...)
		pos = start
	}

	return parts
}

// scoreWord returns a score for how likely this is a valid English word.
// Higher scores are better.
func (tr *TextRepair) scoreWord(word string) float64 {
	// Penalize very short or very long words
	if len(word) == 0 {
		return -100
	}
	if len(word) == 1 {
		return -10 // Single letters get low score unless common
	}
	if len(word) > 20 {
		return -50 // Very long words are unlikely
	}

	// Check if it's a known word
	if CommonEnglishWords[word] {
		return 10.0 + float64(len(word)) // Bonus for known words, prefer longer
	}

	// Heuristic scoring for unknown words
	// Prefer reasonable length (3-8 chars)
	lengthScore := 0.0
	if len(word) >= 3 && len(word) <= 8 {
		lengthScore = 2.0
	} else if len(word) >= 2 && len(word) <= 12 {
		lengthScore = 1.0
	}

	// Small penalty for unknown words
	return lengthScore - 5.0
}

// isLikelyProperWord returns true if word looks like a proper word (not merged).
func (tr *TextRepair) isLikelyProperWord(word string) bool {
	// Check if it's a known word
	if CommonEnglishWords[strings.ToLower(word)] {
		return true
	}

	// Long words with mixed case or all caps are suspicious
	if len(word) > 15 {
		return false
	}

	return true
}

// isAllUpper returns true if all letters in s are uppercase.
func (tr *TextRepair) isAllUpper(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) && !unicode.IsUpper(r) {
			return false
		}
	}
	return true
}

// isTitleCase returns true if s looks like title case (first letter caps).
func (tr *TextRepair) isTitleCase(s string) bool {
	runes := []rune(s)
	if len(runes) == 0 {
		return false
	}
	return unicode.IsUpper(runes[0])
}

// -------------------------------------------------------------------------
// Dictionary-Based Word Repair (OCR Error Correction)
// -------------------------------------------------------------------------

// RepairMisspelledWords uses edit distance to correct likely OCR errors.
// Conservative approach: only fixes words with small edit distance to known words.
func (tr *TextRepair) RepairMisspelledWords(text string) string {
	words := strings.Fields(text)
	var repaired []string

	for _, word := range words {
		// Clean punctuation for checking
		cleaned := strings.Trim(word, ".,!?;:\"'()[]{}$%")
		lower := strings.ToLower(cleaned)

		// Skip if it's a known word
		if CommonEnglishWords[lower] {
			repaired = append(repaired, word)
			continue
		}

		// Skip very short words (likely abbreviations)
		if len(cleaned) <= 2 {
			repaired = append(repaired, word)
			continue
		}

		// Try to find correction
		correction := tr.findBestCorrection(lower)
		if correction != "" && correction != lower {
			// Apply the same case/punctuation as original
			corrected := tr.applyCasing(word, cleaned, correction)
			repaired = append(repaired, corrected)
		} else {
			repaired = append(repaired, word)
		}
	}

	return strings.Join(repaired, " ")
}

// findBestCorrection finds the best correction for a misspelled word.
// Returns empty string if no good correction found.
func (tr *TextRepair) findBestCorrection(word string) string {
	if len(word) == 0 {
		return ""
	}

	bestMatch := ""
	bestDistance := 3 // Only correct if edit distance <= 2

	// Common OCR error patterns: similar looking characters
	ocrVariants := tr.generateOCRVariants(word)

	// Check variants first (fast path for common OCR errors)
	for _, variant := range ocrVariants {
		if CommonEnglishWords[variant] {
			return variant
		}
	}

	// Check edit distance to known words (limited search for performance)
	// Only check words of similar length (±2 chars)
	for knownWord := range CommonEnglishWords {
		lenDiff := len(knownWord) - len(word)
		if lenDiff < -2 || lenDiff > 2 {
			continue
		}

		dist := tr.editDistance(word, knownWord)
		if dist < bestDistance {
			bestDistance = dist
			bestMatch = knownWord
		}
	}

	// Only return if we found a close match (edit distance 1-2)
	if bestDistance <= 2 && bestMatch != "" {
		return bestMatch
	}

	return ""
}

// generateOCRVariants generates common OCR error variants for a word.
// OCR commonly confuses similar-looking characters: l/I/1, O/0, m/n, etc.
func (tr *TextRepair) generateOCRVariants(word string) []string {
	ocrSubstitutions := map[rune][]rune{
		'l': {'i', '1', 'I'},
		'i': {'l', '1', 'I'},
		'I': {'l', '1', 'i'},
		'1': {'l', 'i', 'I'},
		'o': {'0', 'O'},
		'O': {'0', 'o'},
		'0': {'o', 'O'},
		',': {'v'}, // Common in "im,olvng" -> "involving"
		'v': {','}, // Reverse mapping
		'm': {'n'}, // m/n confusion
		'n': {'m'},
		's': {'5'},
		'5': {'s'},
		'z': {'2'},
		'2': {'z'},
	}

	variants := []string{}
	runes := []rune(word)

	// Try single-character substitutions
	for i, r := range runes {
		if subs, ok := ocrSubstitutions[r]; ok {
			for _, sub := range subs {
				variant := make([]rune, len(runes))
				copy(variant, runes)
				variant[i] = sub
				variants = append(variants, string(variant))
			}
		}
	}

	return variants
}

// editDistance calculates edit distance between two strings (optimized version).
func (tr *TextRepair) editDistance(a, b string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	// Use same implementation as levenshteinDistance but optimized
	return tr.levenshteinDistance(a, b)
}

// applyCasing applies the casing pattern from original word to correction.
func (tr *TextRepair) applyCasing(original, cleaned, correction string) string {
	// Find prefix/suffix punctuation
	prefix := ""
	suffix := ""

	for i, r := range original {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			prefix = original[:i]
			break
		}
	}

	for i := len(original) - 1; i >= 0; i-- {
		r := rune(original[i])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			suffix = original[i+1:]
			break
		}
	}

	// Apply casing
	result := correction
	if tr.isAllUpper(cleaned) {
		result = strings.ToUpper(correction)
	} else if tr.isTitleCase(cleaned) {
		result = cases.Title(language.English).String(correction)
	}

	return prefix + result + suffix
}

// -------------------------------------------------------------------------
// Entropy-Based Noise Detection
// -------------------------------------------------------------------------

// CalculateLineEntropy calculates Shannon entropy of a line of text.
// Higher entropy indicates more randomness (potential garbled content).
func (tr *TextRepair) CalculateLineEntropy(line string) float64 {
	if len(line) == 0 {
		return 0.0
	}

	// Count character frequencies
	freq := make(map[rune]int)
	for _, r := range line {
		freq[r]++
	}

	// Calculate entropy
	entropy := 0.0
	total := float64(len(line))

	for _, count := range freq {
		p := float64(count) / total
		if p > 0 {
			entropy -= p * math.Log2(p)
		}
	}

	return entropy
}

// IsNoiseLine detects if a line is likely garbled/corrupted content.
// Uses entropy and character pattern analysis.
func (tr *TextRepair) IsNoiseLine(line string) bool {
	line = strings.TrimSpace(line)

	// Empty lines are not noise
	if len(line) == 0 {
		return false
	}

	// Very short lines are hard to classify
	if len(line) < 10 {
		return false
	}

	// Calculate entropy
	entropy := tr.CalculateLineEntropy(line)

	// High entropy threshold (English text typically < 4.5 bits/char)
	// Random/garbled text has entropy > 4.8
	if entropy > 4.8 {
		// Additional check: high entropy with mixed case, numbers, and symbols
		letterCount := 0
		digitCount := 0
		symbolCount := 0
		for _, r := range line {
			if unicode.IsLetter(r) {
				letterCount++
			} else if unicode.IsDigit(r) {
				digitCount++
			} else if unicode.IsPunct(r) || unicode.IsSymbol(r) {
				symbolCount++
			}
		}

		// If high entropy and highly mixed character types, likely noise
		if digitCount > 0 && symbolCount > 0 && letterCount < len(line)/2 {
			return true
		}
	}

	// Check for high ratio of non-printable or unusual characters
	unusualCount := 0
	for _, r := range line {
		// Count non-ASCII, control chars, or replacement chars as unusual
		if r > 127 || r < 32 || r == unicode.ReplacementChar {
			unusualCount++
		}
	}

	unusualRatio := float64(unusualCount) / float64(len(line))
	if unusualRatio > 0.3 {
		return true
	}

	// Check for very high ratio of punctuation/symbols
	symbolCount := 0
	letterCount := 0
	digitCount := 0
	for _, r := range line {
		if unicode.IsLetter(r) {
			letterCount++
		} else if unicode.IsDigit(r) {
			digitCount++
		} else if unicode.IsPunct(r) || unicode.IsSymbol(r) {
			symbolCount++
		}
	}

	// If more symbols than letters, likely noise
	if symbolCount > letterCount && letterCount < len(line)/4 {
		return true
	}

	// Check for alternating letter/digit/symbol pattern (gibberish)
	// Example: "xK3#@fJq&*2LqZ%mN8!pR" has lots of transitions
	if len(line) > 15 {
		transitions := 0
		prevType := 0 // 0=other, 1=letter, 2=digit, 3=symbol
		for _, r := range line {
			currentType := 0
			if unicode.IsLetter(r) {
				currentType = 1
			} else if unicode.IsDigit(r) {
				currentType = 2
			} else if unicode.IsPunct(r) || unicode.IsSymbol(r) {
				currentType = 3
			}

			if prevType != 0 && currentType != 0 && prevType != currentType {
				transitions++
			}
			prevType = currentType
		}

		// High transition rate suggests random garbage
		transitionRate := float64(transitions) / float64(len(line))
		if transitionRate > 0.5 && symbolCount > len(line)/6 {
			return true
		}
	}

	return false
}

// FilterNoiseLines removes lines detected as garbled/corrupted content.
func (tr *TextRepair) FilterNoiseLines(text string) string {
	lines := strings.Split(text, "\n")
	var filtered []string

	for _, line := range lines {
		if !tr.IsNoiseLine(line) {
			filtered = append(filtered, line)
		}
	}

	return strings.Join(filtered, "\n")
}

// -------------------------------------------------------------------------
// Font Encoding Detection and Repair
// -------------------------------------------------------------------------

// DetectFontEncodingCorruption checks if text appears to be using a corrupted
// or non-standard font encoding. This happens when PDF fonts have custom glyph
// mappings that don't match standard character codes.
//
// Characteristics of font-encoding corruption:
// - Text looks like random letters but has structure (same length as expected)
// - Unusual mix of uppercase/lowercase in patterns that don't match English
// - High proportion of consonant clusters that are phonetically impossible
// - No recognizable words despite looking like text
func (tr *TextRepair) DetectFontEncodingCorruption(text string) float64 {
	text = strings.TrimSpace(text)
	if len(text) < 10 {
		return 0.0
	}

	// Count various characteristics
	letterCount := 0
	upperCount := 0
	lowerCount := 0
	consonantRuns := 0
	currentConsonantRun := 0
	vowelCount := 0

	vowels := map[rune]bool{'a': true, 'e': true, 'i': true, 'o': true, 'u': true,
		'A': true, 'E': true, 'I': true, 'O': true, 'U': true}

	for _, r := range text {
		if unicode.IsLetter(r) {
			letterCount++
			if unicode.IsUpper(r) {
				upperCount++
			} else {
				lowerCount++
			}
			if vowels[r] {
				vowelCount++
				if currentConsonantRun >= 4 {
					consonantRuns++
				}
				currentConsonantRun = 0
			} else {
				currentConsonantRun++
			}
		}
	}
	if currentConsonantRun >= 4 {
		consonantRuns++
	}

	if letterCount == 0 {
		return 0.0
	}

	// Calculate corruption indicators
	score := 0.0

	// 1. Unusual case mixing (e.g., "NWNRJcvJMTQPPJLAP")
	// Normal text has either mostly lowercase, mostly uppercase, or title case
	if upperCount > 0 && lowerCount > 0 {
		upperRatio := float64(upperCount) / float64(letterCount)
		// Random-looking case mixing scores high
		if upperRatio > 0.3 && upperRatio < 0.7 {
			score += 0.3
		}
	}

	// 2. Impossible consonant clusters (e.g., "NWNRJ", "MTQPP")
	if consonantRuns > 0 {
		consonantRunRatio := float64(consonantRuns) / float64(len(strings.Fields(text))+1)
		if consonantRunRatio > 0.3 {
			score += 0.3
		}
	}

	// 3. Very low vowel ratio (English typically 35-40% vowels)
	if letterCount > 0 {
		vowelRatio := float64(vowelCount) / float64(letterCount)
		if vowelRatio < 0.15 {
			score += 0.3
		}
	}

	// 4. Check if words are recognizable
	words := strings.Fields(text)
	unrecognizedCount := 0
	for _, word := range words {
		cleaned := strings.Trim(strings.ToLower(word), ".,!?;:\"'()[]{}$%")
		if len(cleaned) > 3 && !CommonEnglishWords[cleaned] {
			// Check if it's pronounceable (has vowels distributed reasonably)
			hasVowel := false
			for _, r := range cleaned {
				if vowels[r] {
					hasVowel = true
					break
				}
			}
			if !hasVowel && len(cleaned) > 4 {
				unrecognizedCount++
			}
		}
	}
	if len(words) > 0 {
		unrecognizedRatio := float64(unrecognizedCount) / float64(len(words))
		if unrecognizedRatio > 0.5 {
			score += 0.2
		}
	}

	if score > 1.0 {
		score = 1.0
	}
	return score
}

// IsFontEncodingCorrupted returns true if text appears to have font encoding issues.
func (tr *TextRepair) IsFontEncodingCorrupted(text string) bool {
	return tr.DetectFontEncodingCorruption(text) > 0.5
}

// DetectEncodedPattern checks if a string matches a pattern that suggests
// it's an encoded version of a known format (like case numbers).
// Returns the detected pattern type and a description.
func (tr *TextRepair) DetectEncodedPattern(text string) (patternType string, description string) {
	text = strings.TrimSpace(text)

	// Case number pattern: 1:15-cv-07433-LAP
	// Encoded might look like: NWNRJcvJMTQPPJLAP
	// - Has the "cv" or "cv-" preserved (common in font encoding issues)
	// - Has uppercase letters at the end (judge initials)
	// - Length similar to case numbers (15-20 chars)

	if len(text) >= 12 && len(text) <= 25 {
		// Check for preserved "cv" which often survives encoding
		if strings.Contains(strings.ToLower(text), "cv") {
			// Count structure: digits-like before cv, letters after
			parts := strings.SplitN(strings.ToLower(text), "cv", 2)
			if len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0 {
				return "case_number", "Possible encoded case number"
			}
		}
	}

	return "", ""
}

// MarkFontEncodingIssues wraps text that appears to have font encoding issues
// with markers for downstream processing or flagging.
func (tr *TextRepair) MarkFontEncodingIssues(text string) string {
	lines := strings.Split(text, "\n")
	var result []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) > 10 && tr.IsFontEncodingCorrupted(trimmed) {
			// Mark the line as having encoding issues
			result = append(result, "[ENCODING_ISSUE] "+line)
		} else {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

// FilterFontEncodingCorruption removes or replaces lines with severe font encoding issues.
// Less severe issues are left in place but could be marked.
func (tr *TextRepair) FilterFontEncodingCorruption(text string) string {
	lines := strings.Split(text, "\n")
	var result []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines
		if len(trimmed) == 0 {
			result = append(result, line)
			continue
		}

		// Check for severe corruption
		corruptionScore := tr.DetectFontEncodingCorruption(trimmed)

		if corruptionScore > 0.7 {
			// Severely corrupted - skip this line
			continue
		} else if corruptionScore > 0.5 {
			// Moderately corrupted - check if it's a recognizable pattern
			patternType, _ := tr.DetectEncodedPattern(trimmed)
			if patternType != "" {
				// It's a recognizable pattern, keep it with a marker
				result = append(result, "["+patternType+"] "+line)
			}
			// Otherwise skip it
		} else {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}
