// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package query

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/antflydb/antfly/lib/evaluator"
)

// FieldInfo describes a searchable field for LLM context.
type FieldInfo struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Type        string      `json:"type"`               // text, keyword, numeric, datetime, boolean, geopoint
	Searchable  bool        `json:"searchable"`         // Whether the field is indexed
	Types       []string    `json:"types,omitempty"`    // x-antfly-types (e.g., ["text", "keyword"])
	Nested      bool        `json:"nested,omitempty"`   // Whether this is a nested object field
	ArrayOf     string      `json:"array_of,omitempty"` // If array, the type of items
	Children    []FieldInfo `json:"children,omitempty"` // Child fields for nested objects

	// Enhanced LLM guidance fields
	ExampleValues []string    `json:"example_values,omitempty"` // Example values for this field (e.g., ["draft", "published"])
	ValueRange    *ValueRange `json:"value_range,omitempty"`    // Min/max for numeric fields
	Format        string      `json:"format,omitempty"`         // Expected format (e.g., "email", "date-time", "uri")
	Nullable      bool        `json:"nullable,omitempty"`       // Whether the field can be null
	CommonQueries []string    `json:"common_queries,omitempty"` // Example queries for this field
}

// ValueRange describes the expected range for numeric fields.
type ValueRange struct {
	Min         *float64 `json:"min,omitempty"`
	Max         *float64 `json:"max,omitempty"`
	Description string   `json:"description,omitempty"` // e.g., "Price in USD cents"
}

// SchemaDescription provides a summary of a table schema for LLM context.
type SchemaDescription struct {
	Description string      `json:"description,omitempty"`
	Fields      []FieldInfo `json:"fields"`
}

// ExtractSchemaDescription extracts a simplified schema description from a JSON Schema.
// This is designed to be included in LLM prompts for query generation.
func ExtractSchemaDescription(jsonSchema map[string]any) SchemaDescription {
	desc := SchemaDescription{
		Fields: []FieldInfo{},
	}

	if description, ok := jsonSchema["description"].(string); ok {
		desc.Description = description
	}

	properties, ok := jsonSchema["properties"].(map[string]any)
	if !ok {
		return desc
	}

	// Sort field names for consistent output
	fieldNames := make([]string, 0, len(properties))
	for name := range properties {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	for _, fieldName := range fieldNames {
		fieldSchemaI := properties[fieldName]
		fieldSchema, ok := fieldSchemaI.(map[string]any)
		if !ok {
			continue
		}

		// Skip internal fields unless they're useful for search
		if strings.HasPrefix(fieldName, "_") && fieldName != "_type" && fieldName != "_timestamp" {
			continue
		}

		field := extractFieldInfo(fieldName, fieldSchema)
		if field.Type != "" {
			desc.Fields = append(desc.Fields, field)
		}
	}

	return desc
}

// extractFieldInfo extracts field information from a JSON Schema property.
func extractFieldInfo(name string, schema map[string]any) FieldInfo {
	field := FieldInfo{
		Name:       name,
		Searchable: true,
	}

	// Get description
	if desc, ok := schema["description"].(string); ok {
		field.Description = desc
	}

	// Check if indexing is disabled
	if indexVal, ok := schema["x-antfly-index"].(bool); ok && !indexVal {
		field.Searchable = false
	}

	// Get x-antfly-types
	if typesI, ok := schema["x-antfly-types"]; ok {
		switch v := typesI.(type) {
		case []any:
			for _, t := range v {
				if typeStr, ok := t.(string); ok {
					field.Types = append(field.Types, typeStr)
				}
			}
		case []string:
			field.Types = v
		}
	}

	// Get format (e.g., "email", "date-time", "uri")
	if format, ok := schema["format"].(string); ok {
		field.Format = format
	}

	// Check for nullable
	if nullable, ok := schema["nullable"].(bool); ok {
		field.Nullable = nullable
	}

	// Extract enum values as example values
	if enumI, ok := schema["enum"]; ok {
		switch v := enumI.(type) {
		case []any:
			for _, e := range v {
				if str, ok := e.(string); ok {
					field.ExampleValues = append(field.ExampleValues, str)
				}
			}
		case []string:
			field.ExampleValues = v
		}
	}

	// Extract x-antfly-examples if present
	if examplesI, ok := schema["x-antfly-examples"]; ok {
		switch v := examplesI.(type) {
		case []any:
			for _, e := range v {
				if str, ok := e.(string); ok {
					field.ExampleValues = append(field.ExampleValues, str)
				}
			}
		case []string:
			field.ExampleValues = append(field.ExampleValues, v...)
		}
	}

	// Extract numeric range from minimum/maximum
	jsonType, _ := schema["type"].(string)
	if jsonType == "number" || jsonType == "integer" {
		var vr *ValueRange
		if min, ok := schema["minimum"]; ok {
			vr = &ValueRange{}
			if f, ok := evaluator.ToFloat64(min); ok {
				vr.Min = &f
			}
		}
		if max, ok := schema["maximum"]; ok {
			if vr == nil {
				vr = &ValueRange{}
			}
			if f, ok := evaluator.ToFloat64(max); ok {
				vr.Max = &f
			}
		}
		if vr != nil {
			field.ValueRange = vr
		}
	}

	// Get JSON Schema type
	jsonType, _ = schema["type"].(string)

	// Handle array type
	if jsonType == "array" {
		items, ok := schema["items"].(map[string]any)
		if ok {
			itemType, _ := items["type"].(string)
			if itemType == "object" {
				field.Nested = true
				field.ArrayOf = "object"
				field.Type = "array"

				// Extract child fields
				if itemProps, ok := items["properties"].(map[string]any); ok {
					childNames := make([]string, 0, len(itemProps))
					for name := range itemProps {
						childNames = append(childNames, name)
					}
					sort.Strings(childNames)

					for _, childName := range childNames {
						childSchemaI := itemProps[childName]
						if childSchema, ok := childSchemaI.(map[string]any); ok {
							child := extractFieldInfo(childName, childSchema)
							if child.Type != "" {
								field.Children = append(field.Children, child)
							}
						}
					}
				}
				return field
			}
			field.ArrayOf = mapJSONTypeToFieldType(itemType, nil)
		}
	}

	// Handle object type
	if jsonType == "object" {
		field.Nested = true
		field.Type = "object"

		// Check for additionalProperties (dynamic mapping)
		if additionalProps, ok := schema["additionalProperties"].(map[string]any); ok {
			addType, _ := additionalProps["type"].(string)
			field.ArrayOf = mapJSONTypeToFieldType(addType, nil)
		}

		// Extract child fields from properties
		if props, ok := schema["properties"].(map[string]any); ok {
			childNames := make([]string, 0, len(props))
			for name := range props {
				childNames = append(childNames, name)
			}
			sort.Strings(childNames)

			for _, childName := range childNames {
				childSchemaI := props[childName]
				if childSchema, ok := childSchemaI.(map[string]any); ok {
					child := extractFieldInfo(childName, childSchema)
					if child.Type != "" {
						field.Children = append(field.Children, child)
					}
				}
			}
		}
		return field
	}

	// Map to field type
	field.Type = mapJSONTypeToFieldType(jsonType, field.Types)

	return field
}

// mapJSONTypeToFieldType maps JSON Schema type + x-antfly-types to a simplified field type.
func mapJSONTypeToFieldType(jsonType string, antflyTypes []string) string {
	// If we have x-antfly-types, use the most relevant one
	if len(antflyTypes) > 0 {
		for _, t := range antflyTypes {
			switch t {
			case "text", "html":
				return "text"
			case "keyword", "link":
				return "keyword"
			case "numeric":
				return "numeric"
			case "datetime":
				return "datetime"
			case "boolean":
				return "boolean"
			case "geopoint":
				return "geopoint"
			case "geoshape":
				return "geoshape"
			case "embedding":
				return "embedding"
			case "search_as_you_type":
				return "text" // search_as_you_type is used with text fields
			}
		}
	}

	// Fall back to JSON Schema type
	switch jsonType {
	case "string":
		return "text"
	case "number", "integer":
		return "numeric"
	case "boolean":
		return "boolean"
	default:
		return jsonType
	}
}

// FormatSchemaForLLM formats a SchemaDescription as a string suitable for LLM prompts.
func FormatSchemaForLLM(schema SchemaDescription) string {
	var sb strings.Builder

	if schema.Description != "" {
		sb.WriteString(schema.Description)
		sb.WriteString("\n\n")
	}

	sb.WriteString("Available fields:\n")
	for _, field := range schema.Fields {
		formatFieldForLLM(&sb, field, 0)
	}

	return sb.String()
}

// FormatSchemaForLLMDetailed formats a SchemaDescription with query examples.
// This provides more context for LLMs to generate correct queries.
func FormatSchemaForLLMDetailed(schema SchemaDescription) string {
	var sb strings.Builder

	if schema.Description != "" {
		sb.WriteString(schema.Description)
		sb.WriteString("\n\n")
	}

	sb.WriteString("Available fields:\n")
	for _, field := range schema.Fields {
		formatFieldForLLMDetailed(&sb, field, 0)
	}

	return sb.String()
}

// formatFieldForLLM formats a single field for LLM output.
func formatFieldForLLM(sb *strings.Builder, field FieldInfo, indent int) {
	prefix := strings.Repeat("  ", indent)

	// Field name and type
	fmt.Fprintf(sb, "%s- %s (%s)", prefix, field.Name, field.Type)

	// Add array info if present
	if field.ArrayOf != "" {
		fmt.Fprintf(sb, " [array of %s]", field.ArrayOf)
	}

	// Add searchability note
	if !field.Searchable {
		sb.WriteString(" [not indexed]")
	}

	// Add specific types if different from the main type
	if len(field.Types) > 0 {
		hasNonStandard := false
		for _, t := range field.Types {
			if t != field.Type && t != "text" && t != "keyword" {
				hasNonStandard = true
				break
			}
		}
		if hasNonStandard || len(field.Types) > 1 {
			fmt.Fprintf(sb, " (indexed as: %s)", strings.Join(field.Types, ", "))
		}
	}

	sb.WriteString("\n")

	// Add description if present
	if field.Description != "" {
		fmt.Fprintf(sb, "%s  Description: %s\n", prefix, field.Description)
	}

	// Add nested children
	for _, child := range field.Children {
		formatFieldForLLM(sb, child, indent+1)
	}
}

// formatFieldForLLMDetailed formats a field with additional query guidance.
func formatFieldForLLMDetailed(sb *strings.Builder, field FieldInfo, indent int) {
	prefix := strings.Repeat("  ", indent)

	// Field name and type
	fmt.Fprintf(sb, "%s- %s (%s)", prefix, field.Name, field.Type)

	// Add format if present
	if field.Format != "" {
		fmt.Fprintf(sb, " [format: %s]", field.Format)
	}

	// Add array info if present
	if field.ArrayOf != "" {
		fmt.Fprintf(sb, " [array of %s]", field.ArrayOf)
	}

	// Add searchability note
	if !field.Searchable {
		sb.WriteString(" [not indexed]")
	}

	sb.WriteString("\n")

	// Add description if present
	if field.Description != "" {
		fmt.Fprintf(sb, "%s  Description: %s\n", prefix, field.Description)
	}

	// Add example values if present
	if len(field.ExampleValues) > 0 {
		if len(field.ExampleValues) <= 5 {
			fmt.Fprintf(sb, "%s  Values: %s\n", prefix, strings.Join(field.ExampleValues, ", "))
		} else {
			fmt.Fprintf(sb, "%s  Values: %s, ... (%d total)\n", prefix,
				strings.Join(field.ExampleValues[:5], ", "), len(field.ExampleValues))
		}
	}

	// Add value range if present
	if field.ValueRange != nil {
		rangeStr := ""
		if field.ValueRange.Min != nil && field.ValueRange.Max != nil {
			rangeStr = fmt.Sprintf("%.0f to %.0f", *field.ValueRange.Min, *field.ValueRange.Max)
		} else if field.ValueRange.Min != nil {
			rangeStr = fmt.Sprintf(">= %.0f", *field.ValueRange.Min)
		} else if field.ValueRange.Max != nil {
			rangeStr = fmt.Sprintf("<= %.0f", *field.ValueRange.Max)
		}
		if rangeStr != "" {
			fmt.Fprintf(sb, "%s  Range: %s\n", prefix, rangeStr)
		}
	}

	// Add query suggestion based on field type
	querySuggestion := getQuerySuggestion(field)
	if querySuggestion != "" {
		fmt.Fprintf(sb, "%s  Query: %s\n", prefix, querySuggestion)
	}

	// Add nested children
	for _, child := range field.Children {
		formatFieldForLLMDetailed(sb, child, indent+1)
	}
}

// getQuerySuggestion returns a query example based on field type.
func getQuerySuggestion(field FieldInfo) string {
	switch field.Type {
	case "text":
		return fmt.Sprintf(`{"match": "search text", "field": "%s"}`, field.Name)
	case "keyword":
		if len(field.ExampleValues) > 0 {
			return fmt.Sprintf(`{"term": "%s", "field": "%s"}`, field.ExampleValues[0], field.Name)
		}
		return fmt.Sprintf(`{"term": "value", "field": "%s"}`, field.Name)
	case "numeric":
		if field.ValueRange != nil && field.ValueRange.Min != nil {
			return fmt.Sprintf(`{"range": "%s", "gte": %.0f}`, field.Name, *field.ValueRange.Min)
		}
		return fmt.Sprintf(`{"range": "%s", "gte": 0}`, field.Name)
	case "datetime":
		return fmt.Sprintf(`{"range": "%s", "gte": "now-7d"}`, field.Name)
	case "boolean":
		return fmt.Sprintf(`{"bool": true, "field": "%s"}`, field.Name)
	case "geopoint":
		return fmt.Sprintf(`{"geo_distance": {"field": "%s", "point": [lng, lat], "distance": "10km"}}`, field.Name)
	default:
		return ""
	}
}

// SchemaToJSON converts a SchemaDescription to JSON for embedding in prompts.
func SchemaToJSON(schema SchemaDescription) (string, error) {
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling schema: %w", err)
	}
	return string(data), nil
}

// ExtractFieldNames returns a flat list of searchable field names from a schema.
func ExtractFieldNames(schema SchemaDescription) []string {
	var names []string
	for _, field := range schema.Fields {
		if field.Searchable {
			names = append(names, field.Name)
			// Add nested field names with dot notation
			for _, child := range field.Children {
				if child.Searchable {
					names = append(names, field.Name+"."+child.Name)
				}
			}
		}
	}
	return names
}

// QueryTypeRecommendation provides guidance on which query type to use for a field.
type QueryTypeRecommendation struct {
	FieldName       string
	FieldType       string
	RecommendedType string
	Reason          string
}

// RecommendQueryTypes provides query type recommendations based on field types.
func RecommendQueryTypes(schema SchemaDescription) []QueryTypeRecommendation {
	var recommendations []QueryTypeRecommendation

	for _, field := range schema.Fields {
		if !field.Searchable {
			continue
		}

		rec := QueryTypeRecommendation{
			FieldName: field.Name,
			FieldType: field.Type,
		}

		switch field.Type {
		case "text":
			rec.RecommendedType = "match"
			rec.Reason = "Use 'match' for full-text search with tokenization and stemming"
		case "keyword":
			rec.RecommendedType = "term"
			rec.Reason = "Use 'term' for exact matching on keywords, IDs, or enums"
		case "numeric":
			rec.RecommendedType = "range"
			rec.Reason = "Use 'range' with gt/gte/lt/lte for numeric comparisons"
		case "datetime":
			rec.RecommendedType = "range"
			rec.Reason = "Use 'range' with gt/gte/lt/lte for date comparisons (ISO 8601 format)"
		case "boolean":
			rec.RecommendedType = "bool"
			rec.Reason = "Use 'bool' query with true/false"
		case "geopoint":
			rec.RecommendedType = "geo_distance"
			rec.Reason = "Use 'geo_distance' or 'geo_bbox' for location queries"
		default:
			continue // Skip unknown types
		}

		recommendations = append(recommendations, rec)
	}

	return recommendations
}
