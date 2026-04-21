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

// Package reading provides OCR and document understanding capabilities
// using Vision2Seq models like TrOCR, Donut, Florence-2, and Moondream.
package reading

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Moondream prompt templates

// MoondreamSingleImagePrompt is the template for describing a single image.
const MoondreamSingleImagePrompt = `Describe this image in detail.

Respond ONLY with valid JSON in this exact format:
{
  "description": "A detailed description of the image",
  "mood": "The emotional tone (e.g., happy, sad, funny, exciting)",
  "possible_source": "Where this might be from (e.g., photo, artwork, screenshot)",
  "tags": ["tag1", "tag2", "tag3"]
}

%s`

// MoondreamDefaultPrompt is used when no user prompt is provided.
const MoondreamDefaultPrompt = "Describe this image."

// MoondreamDescriptionPrompt builds a description task prompt for Moondream.
// If userPrompt is empty, a default prompt is used.
func MoondreamDescriptionPrompt(userPrompt string) string {
	if userPrompt == "" {
		userPrompt = MoondreamDefaultPrompt
	}
	return fmt.Sprintf(MoondreamSingleImagePrompt, userPrompt)
}

// Moondream output parsing

// jsonBlockRegex matches JSON code blocks in markdown (```json ... ``` or ``` ... ```)
var moondreamJSONBlockRegex = regexp.MustCompile("(?s)```(?:json)?\\s*\\n?(\\{.*?\\})\\s*```")

// jsonObjectRegex matches a JSON object anywhere in text
var moondreamJSONObjectRegex = regexp.MustCompile(`(?s)(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})`)

// MoondreamOutputParser parses Moondream model output into a Result.
// Extracts JSON fields if present, falls back to raw text.
func MoondreamOutputParser(text, prompt string) Result {
	description, fields := MoondreamParseFields(text)
	return Result{Text: description, Fields: fields}
}

// MoondreamParseFields parses Moondream's output into a description and structured fields.
// It handles multiple formats:
//   - Raw JSON objects
//   - Markdown code blocks (```json ... ```)
//   - Partial/malformed JSON (falls back to raw text)
//
// Returns:
//   - description: The main text description (from "description" field or raw text)
//   - fields: Structured fields (mood, possible_source, tags as comma-separated)
func MoondreamParseFields(text string) (description string, fields map[string]string) {
	fields = make(map[string]string)

	// Try to extract JSON from the output
	jsonStr := moondreamExtractJSON(text)
	if jsonStr == "" {
		// No JSON found, use raw text as description fallback
		return strings.TrimSpace(text), fields
	}

	// Try to parse the JSON into our expected structure
	var parsed struct {
		Description    string   `json:"description"`
		Mood           string   `json:"mood"`
		PossibleSource string   `json:"possible_source"`
		TemporalFlow   string   `json:"temporal_flow"`
		Tags           []string `json:"tags"`
	}

	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		// JSON parsing failed, use raw text
		return strings.TrimSpace(text), fields
	}

	// Populate the result from parsed JSON
	description = parsed.Description
	if description == "" {
		description = strings.TrimSpace(text)
	}

	// Store structured fields
	if parsed.Mood != "" {
		fields["mood"] = parsed.Mood
	}
	if parsed.PossibleSource != "" {
		fields["possible_source"] = parsed.PossibleSource
	}
	if parsed.TemporalFlow != "" {
		fields["temporal_flow"] = parsed.TemporalFlow
	}
	if len(parsed.Tags) > 0 {
		fields["tags"] = strings.Join(parsed.Tags, ",")
	}

	return description, fields
}

// moondreamExtractJSON attempts to extract a JSON object from text.
// It tries multiple strategies in order:
//  1. Markdown code blocks (```json ... ```)
//  2. Raw JSON object detection
//  3. First { to last } extraction
func moondreamExtractJSON(text string) string {
	text = strings.TrimSpace(text)

	// Strategy 1: Look for markdown code blocks
	if matches := moondreamJSONBlockRegex.FindStringSubmatch(text); len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}

	// Strategy 2: If the text starts with {, try to parse it directly
	if strings.HasPrefix(text, "{") {
		// Find the matching closing brace
		if extracted := moondreamExtractBalancedJSON(text); extracted != "" {
			return extracted
		}
	}

	// Strategy 3: Look for JSON object anywhere in text
	if matches := moondreamJSONObjectRegex.FindStringSubmatch(text); len(matches) > 1 {
		candidate := strings.TrimSpace(matches[1])
		// Validate it's actually valid JSON
		if json.Valid([]byte(candidate)) {
			return candidate
		}
	}

	// Strategy 4: Find first { and last } and try to extract
	firstBrace := strings.Index(text, "{")
	lastBrace := strings.LastIndex(text, "}")
	if firstBrace != -1 && lastBrace > firstBrace {
		candidate := text[firstBrace : lastBrace+1]
		if json.Valid([]byte(candidate)) {
			return candidate
		}
	}

	return ""
}

// moondreamExtractBalancedJSON extracts a balanced JSON object from the start of text.
func moondreamExtractBalancedJSON(text string) string {
	if len(text) == 0 || text[0] != '{' {
		return ""
	}

	depth := 0
	inString := false
	escaped := false

	for i, ch := range text {
		if escaped {
			escaped = false
			continue
		}

		if ch == '\\' && inString {
			escaped = true
			continue
		}

		if ch == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				candidate := text[:i+1]
				if json.Valid([]byte(candidate)) {
					return candidate
				}
				return ""
			}
		}
	}

	return ""
}
