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

package ner

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// FieldType represents the type of an extraction field.
type FieldType int

const (
	// FieldTypeStr keeps only the top-scoring span for a field.
	FieldTypeStr FieldType = iota
	// FieldTypeList keeps all extracted spans for a field.
	FieldTypeList
)

// SchemaField represents a single field in an extraction schema.
type SchemaField struct {
	// Name is the field name (used as the NER label).
	Name string
	// Type is the field type (str or list).
	Type FieldType
	// Choices contains valid options for choice fields (nil for non-choice fields).
	Choices []string
}

// ExtractionSchema represents a named structure to extract from text.
type ExtractionSchema struct {
	// Name is the structure name (e.g., "person").
	Name string
	// Fields are the fields to extract within this structure.
	Fields []SchemaField
}

// ExtractionConfig holds configuration for JSON extraction.
type ExtractionConfig struct {
	// Threshold is the score threshold for span extraction (0.0-1.0).
	Threshold float32
	// FlatNER if true, don't allow nested/overlapping entities.
	FlatNER bool
	// IncludeConfidence if true, include confidence scores in output.
	IncludeConfidence bool
	// IncludeSpans if true, include span offsets in output.
	IncludeSpans bool
	// ClusterGap overrides the adaptive clustering gap threshold (in characters).
	// Zero means adaptive (default): uses min(100, textLength/10) as a floor.
	ClusterGap int
}

// DefaultExtractionConfig returns sensible defaults for extraction.
func DefaultExtractionConfig() ExtractionConfig {
	return ExtractionConfig{
		Threshold: 0.3,
		FlatNER:   true,
	}
}

// ExtractedFieldValue represents a single extracted value for a field.
type ExtractedFieldValue struct {
	// Value is the extracted text.
	Value string `json:"value"`
	// Score is the confidence score (included when IncludeConfidence is true).
	Score float32 `json:"score,omitempty"`
	// Start is the character offset (included when IncludeSpans is true).
	// Pointer type so offset 0 is not silently dropped.
	Start *int `json:"start,omitempty"`
	// End is the character offset (included when IncludeSpans is true).
	// Pointer type so offset 0 is not silently dropped.
	End *int `json:"end,omitempty"`
}

// ExtractedInstance represents a single extracted instance of a structure.
// Keys are field names, values are either a single ExtractedFieldValue (for ::str)
// or a slice of ExtractedFieldValue (for ::list).
type ExtractedInstance map[string]any

// ExtractionResult holds the extraction results for a single text.
// Keys are structure names, values are slices of ExtractedInstance.
type ExtractionResult map[string][]ExtractedInstance

// Extractor defines the interface for models that support structured schema-based extraction.
//
// Type-assert from Model or Recognizer to check support.
type Extractor interface {
	// Extract extracts structured data from text based on the given schemas.
	Extract(ctx context.Context, texts []string, schemas []ExtractionSchema, config ExtractionConfig) ([]ExtractionResult, error)
}

// ParseSchemaString parses a schema map (e.g., {"person": ["name::str", "age::str", "skills::list"]})
// into ExtractionSchema values.
//
// Field syntax:
//   - "name::str"                 -> FieldTypeStr, no choices
//   - "skills::list"              -> FieldTypeList, no choices
//   - "role::[engineer|manager]"  -> FieldTypeStr with choices
//   - "name"                      -> FieldTypeStr (default if no :: separator)
func ParseSchemaString(schema map[string][]string) ([]ExtractionSchema, error) {
	schemas := make([]ExtractionSchema, 0, len(schema))

	for structName, fieldDefs := range schema {
		if structName == "" {
			return nil, fmt.Errorf("empty structure name")
		}
		if len(fieldDefs) == 0 {
			return nil, fmt.Errorf("structure %q has no fields", structName)
		}

		fields := make([]SchemaField, 0, len(fieldDefs))
		for _, fieldDef := range fieldDefs {
			field, err := parseFieldDef(fieldDef)
			if err != nil {
				return nil, fmt.Errorf("structure %q: %w", structName, err)
			}
			fields = append(fields, field)
		}

		schemas = append(schemas, ExtractionSchema{
			Name:   structName,
			Fields: fields,
		})
	}

	return schemas, nil
}

// isTypeSpecifier returns true if s is a recognised type keyword.
func isTypeSpecifier(s string) bool {
	switch strings.ToLower(s) {
	case "str", "string", "list", "array":
		return true
	}
	return false
}

// isChoiceSpecifier returns true if s looks like "[opt1|opt2|...]".
func isChoiceSpecifier(s string) bool {
	return strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")
}

// parseFieldDef parses a single field definition string.
// Parsing proceeds right-to-left so that field names containing "::" are handled
// correctly. The rightmost segments that are recognised type specifiers or choice
// brackets are consumed; everything else is the field name.
//
// Examples:
//
//	"person::name::str"  → name="person::name", type=str
//	"role::[a|b]"        → name="role", choices=[a,b]
//	"a::b::[x|y]"        → name="a::b", choices=[x,y]
//	"name"               → name="name", type=str (default)
func parseFieldDef(def string) (SchemaField, error) {
	def = strings.TrimSpace(def)
	if def == "" {
		return SchemaField{}, fmt.Errorf("empty field definition")
	}

	// Split on all "::" separators
	parts := strings.Split(def, "::")

	field := SchemaField{
		Type: FieldTypeStr, // default
	}

	// Walk backward from the last segment, consuming specifiers.
	// Once we hit a segment that is neither a type nor a choice, stop —
	// the remaining (leftward) segments form the field name.
	nameEnd := len(parts) // exclusive upper bound of name parts
	for i := len(parts) - 1; i >= 1; i-- {
		part := strings.TrimSpace(parts[i])

		if isChoiceSpecifier(part) {
			choicesStr := part[1 : len(part)-1]
			choices := strings.Split(choicesStr, "|")
			for j, c := range choices {
				choices[j] = strings.TrimSpace(c)
			}
			if len(choices) < 2 {
				return SchemaField{}, fmt.Errorf("choice field must have at least 2 options in %q", def)
			}
			if slices.Contains(choices, "") {
				return SchemaField{}, fmt.Errorf("choice field has empty option in %q", def)
			}
			field.Choices = choices
			nameEnd = i
		} else if isTypeSpecifier(part) {
			switch strings.ToLower(part) {
			case "str", "string":
				field.Type = FieldTypeStr
			case "list", "array":
				field.Type = FieldTypeList
			}
			nameEnd = i
		} else {
			// Not a specifier — stop consuming from the right
			break
		}
	}

	// Everything from parts[0..nameEnd) is the field name, joined back with "::".
	nameParts := make([]string, nameEnd)
	for i := 0; i < nameEnd; i++ {
		nameParts[i] = strings.TrimSpace(parts[i])
	}
	field.Name = strings.Join(nameParts, "::")

	if field.Name == "" {
		return SchemaField{}, fmt.Errorf("empty field name in %q", def)
	}

	return field, nil
}
