// Package reading provides OCR and document understanding capabilities
// using Vision2Seq models like TrOCR, Donut, and Florence-2.
package reading

import (
	"fmt"
	"maps"
	"regexp"
	"strings"
)

// Donut-specific regex patterns for output parsing
var (
	// Matches opening task tokens like <s_cord-v2>, <s_docvqa>
	openingTaskPattern = regexp.MustCompile(`^<s_[a-zA-Z0-9_-]+>`)
	// Matches closing task tokens like </s_cord-v2>, </s_docvqa>
	closingTaskPattern = regexp.MustCompile(`</s_[a-zA-Z0-9_-]+>$`)
	// Matches field opening tags like <s_company>, <s_menu>
	fieldOpenPattern = regexp.MustCompile(`<s_([a-zA-Z_][a-zA-Z0-9_]*)>`)
)

// DonutCleanOutput removes Donut-style outer task tokens from the output.
// Donut outputs like: <s_cord-v2>{"menu": {...}}</s_cord-v2>
// This only removes the outermost task wrapper, preserving inner field tokens.
func DonutCleanOutput(text string) string {
	text = strings.TrimSpace(text)

	// Remove leading outer task token like <s_cord-v2>
	if loc := openingTaskPattern.FindStringIndex(text); loc != nil && loc[0] == 0 {
		text = strings.TrimSpace(text[loc[1]:])
	}

	// Remove trailing outer task token like </s_cord-v2>
	if loc := closingTaskPattern.FindStringIndex(text); loc != nil && loc[1] == len(text) {
		text = strings.TrimSpace(text[:loc[0]])
	}

	return text
}

// DonutParseFields extracts field values from Donut's XML-like output format.
// Donut outputs fields like: <s_company>ACME Corp</s_company><s_total>$123.45</s_total>
// Nested fields are flattened with dot notation: menu.nm, menu.price
//
// Example:
//
//	input := "<s_menu><s_nm>Coffee</s_nm><s_price>$3.50</s_price></s_menu>"
//	fields := DonutParseFields(input)
//	// fields = {"menu.nm": "Coffee", "menu.price": "$3.50"}
func DonutParseFields(text string) map[string]string {
	return donutParseFieldsWithPrefix(text, "")
}

func donutParseFieldsWithPrefix(text string, prefix string) map[string]string {
	result := make(map[string]string)

	pos := 0
	for pos < len(text) {
		match := fieldOpenPattern.FindStringSubmatchIndex(text[pos:])
		if match == nil {
			break
		}

		tagEnd := pos + match[1]
		fieldName := text[pos+match[2] : pos+match[3]]
		closeTag := "</s_" + fieldName + ">"

		closeTagPos := strings.Index(text[tagEnd:], closeTag)
		if closeTagPos < 0 {
			pos = tagEnd
			continue
		}

		fieldValue := strings.TrimSpace(text[tagEnd : tagEnd+closeTagPos])
		fullKey := fieldName
		if prefix != "" {
			fullKey = prefix + "." + fieldName
		}

		if strings.Contains(fieldValue, "<s_") {
			nested := donutParseFieldsWithPrefix(fieldValue, fullKey)
			maps.Copy(result, nested)
		} else {
			result[fullKey] = fieldValue
		}

		pos = tagEnd + closeTagPos + len(closeTag)
	}

	return result
}

// DonutDocVQAPrompt builds a DocVQA task prompt for visual question answering.
// Example: DonutDocVQAPrompt("What is the total?") returns:
// "<s_docvqa><s_question>What is the total?</s_question><s_answer>"
func DonutDocVQAPrompt(question string) string {
	return fmt.Sprintf("<s_docvqa><s_question>%s</s_question><s_answer>", question)
}

// DonutCORDPrompt returns the CORD v2 receipt parsing task prompt.
func DonutCORDPrompt() string {
	return "<s_cord-v2>"
}

// DonutRVLCDIPPrompt returns the RVL-CDIP document classification task prompt.
func DonutRVLCDIPPrompt() string {
	return "<s_rvlcdip>"
}

// DonutOutputParser parses Donut model output into a structured Result.
// Handles both structured field extraction (CORD) and DocVQA answers.
func DonutOutputParser(text, prompt string) Result {
	result := Result{
		Text:   DonutCleanOutput(text),
		Fields: DonutParseFields(text),
	}
	if strings.Contains(prompt, "<s_docvqa>") {
		result.Text = DonutParseDocVQAAnswer(text)
	}
	return result
}

// DonutParseDocVQAAnswer extracts the answer from a DocVQA response.
// Input: "The total is $123.45</s_answer></s_docvqa>"
// Returns: "The total is $123.45"
func DonutParseDocVQAAnswer(text string) string {
	text = strings.TrimSpace(text)
	if idx := strings.Index(text, "</s_answer>"); idx >= 0 {
		text = strings.TrimSpace(text[:idx])
	}
	return text
}
