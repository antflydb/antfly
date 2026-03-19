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

package schema

import (
	"fmt"
	"slices"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

// applyDynamicTemplates translates antfly DynamicTemplate objects to bleve DynamicTemplate
// objects and adds them to the provided document mapping.
func applyDynamicTemplates(docMapping *mapping.DocumentMapping, templates []DynamicTemplate) {
	for _, t := range templates {
		bleveTemplate := mapping.NewDynamicTemplate(t.Name)

		if t.Match != "" {
			bleveTemplate.MatchField(t.Match)
		}
		if t.Unmatch != "" {
			bleveTemplate.UnmatchField(t.Unmatch)
		}
		if t.PathMatch != "" {
			bleveTemplate.MatchPath(t.PathMatch)
		}
		if t.PathUnmatch != "" {
			bleveTemplate.UnmatchPath(t.PathUnmatch)
		}
		if t.MatchMappingType != "" {
			bleveTemplate.MatchType(string(t.MatchMappingType))
		}

		// Create bleve field mapping from antfly template mapping
		fieldMapping := translateTemplateFieldMapping(t.Mapping)
		bleveTemplate.WithMapping(fieldMapping)

		docMapping.AddDynamicTemplate(bleveTemplate)
	}
}

// translateTemplateFieldMapping converts an antfly TemplateFieldMapping to a bleve FieldMapping.
func translateTemplateFieldMapping(m TemplateFieldMapping) *mapping.FieldMapping {
	fieldMapping := mapping.NewTextFieldMapping()

	// Set antfly defaults (different from bleve defaults)
	// Antfly defaults: Index=true, Store=false, IncludeInAll=false, DocValues=false
	fieldMapping.Index = true
	fieldMapping.Store = false
	fieldMapping.IncludeInAll = false
	fieldMapping.DocValues = false

	// Apply type based on AntflyType
	switch m.Type {
	case AntflyTypeText:
		fieldMapping.Type = "text"
	case AntflyTypeHtml:
		fieldMapping.Type = "text"
		fieldMapping.Analyzer = HTMLAnalyzer
	case AntflyTypeKeyword, AntflyTypeLink:
		fieldMapping.Type = "text"
		fieldMapping.Analyzer = keyword.Name
	case AntflyTypeNumeric:
		fieldMapping.Type = "number"
	case AntflyTypeBoolean:
		fieldMapping.Type = "boolean"
	case AntflyTypeDatetime:
		fieldMapping.Type = "datetime"
	case AntflyTypeGeopoint:
		fieldMapping.Type = "geopoint"
	case AntflyTypeGeoshape:
		fieldMapping.Type = "geoshape"
	case AntflyTypeSearchAsYouType:
		fieldMapping.Type = "text"
		fieldMapping.Analyzer = SearchAsYouTypeAnalyzer
	case AntflyTypeEmbedding, AntflyTypeBlob:
		// These types should not be indexed in full-text search
		fieldMapping.Index = false
	}

	// Apply analyzer (overrides type-based default)
	if m.Analyzer != "" {
		fieldMapping.Analyzer = m.Analyzer
	}

	// Apply boolean settings from template
	// With omitzero, false is the zero value meaning "not set"
	// We only override our defaults when value is true
	// Note: Index defaults to true (set above), so we only set false for embedding/blob types
	if m.Store {
		fieldMapping.Store = true
	}
	if m.IncludeInAll {
		fieldMapping.IncludeInAll = true
	}
	if m.DocValues {
		fieldMapping.DocValues = true
	}

	return fieldMapping
}

const (
	XAntflyTypes                 = "x-antfly-types"
	XAntflyIndex                 = "x-antfly-index"
	XAntflyIncludeInAll          = "x-antfly-include-in-all"
	XAntflyAnalyzer              = "x-antfly-analyzer"
	XAntflyAnalyzers             = "x-antfly-analyzers"
	XAntflyCharFilters           = "x-antfly-char-filters"
	XAntflyTokenFilters          = "x-antfly-token-filters"
	XAntflyTokenizers            = "x-antfly-tokenizers"
	XAntflyDateTimeParser        = "x-antfly-date-time-parser"
	XAntflyDateTimeParsers       = "x-antfly-date-time-parsers"
	XAntflyDefaultDateTimeParser = "x-antfly-default-date-time-parser"
)

const (
	SearchAsYouTypeAnalyzer = "search_as_you_type_analyzer"
	HTMLAnalyzer            = "html_analyzer"
	EdgeNgramTokenFilter    = "edge_ngram_filter" //nolint:gosec // G101: env var name mapping, not credentials
)

func buildMappingFromJSONSchema(
	schema map[string]any,
	includeInAll []string,
) (*mapping.DocumentMapping, bool, bool, bool) {
	docMapping := bleve.NewDocumentMapping()
	analyzerNeeded := false
	htmlAnalyzerNeeded := false
	allFieldUsed := false

	// Enable dynamic indexing if additionalProperties is true or is a schema object
	if additionalProps, ok := schema["additionalProperties"]; ok {
		switch v := additionalProps.(type) {
		case bool:
			docMapping.Dynamic = v
		case map[string]any:
			// additionalProperties is a schema, enable dynamic indexing
			docMapping.Dynamic = true
		}
	} else {
		// No additionalProperties specified, disable dynamic indexing
		docMapping.Dynamic = false
	}

	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		return docMapping, false, false, false
	}

	for fieldName, fieldSchemaI := range properties {
		fieldSchema, ok := fieldSchemaI.(map[string]any)
		if !ok {
			continue
		}

		// Check if indexing is disabled for this field
		if indexVal, ok := fieldSchema[XAntflyIndex].(bool); ok && !indexVal {
			disabledFieldMapping := mapping.NewTextFieldMapping()
			disabledFieldMapping.Index = false
			docMapping.AddFieldMappingsAt(fieldName, disabledFieldMapping)
			continue
		}

		var antflyTypes []AntflyType
		if typesI, ok := fieldSchema[XAntflyTypes]; ok {
			// Handle both []any and []string (JSON unmarshaling can produce either)
			switch v := typesI.(type) {
			case []any:
				for _, t := range v {
					if typeStr, ok := t.(string); ok {
						antflyTypes = append(antflyTypes, AntflyType(typeStr))
					}
				}
			case []string:
				for _, typeStr := range v {
					antflyTypes = append(antflyTypes, AntflyType(typeStr))
				}
			}
		}

		jsonType, _ := fieldSchema["type"].(string)

		// Handle object type recursively
		if jsonType == "object" {
			// Nested objects don't inherit includeInAll - pass empty slice
			subMapping, subAnalyzerNeeded, subHtmlAnalyzerNeeded, _ := buildMappingFromJSONSchema(
				fieldSchema,
				nil,
			)
			analyzerNeeded = analyzerNeeded || subAnalyzerNeeded
			htmlAnalyzerNeeded = htmlAnalyzerNeeded || subHtmlAnalyzerNeeded
			docMapping.AddSubDocumentMapping(fieldName, subMapping)
			continue
		}

		// Handle array type
		if jsonType == "array" {
			items, ok := fieldSchema["items"].(map[string]any)
			if !ok {
				continue
			}
			itemsType, _ := items["type"].(string)
			if itemsType == "object" {
				// Nested objects don't inherit includeInAll - pass empty slice
				subMapping, subAnalyzerNeeded, subHtmlAnalyzerNeeded, _ := buildMappingFromJSONSchema(
					items,
					nil,
				)
				analyzerNeeded = analyzerNeeded || subAnalyzerNeeded
				htmlAnalyzerNeeded = htmlAnalyzerNeeded || subHtmlAnalyzerNeeded
				docMapping.AddSubDocumentMapping(fieldName, subMapping)
			} else {
				// Array of primitives: use parent's antfly-types or infer from item type
				if len(antflyTypes) == 0 {
					antflyTypes = append(antflyTypes, inferAntflyType(itemsType))
				}
			}
		}

		// Infer type if not specified
		if len(antflyTypes) == 0 {
			antflyTypes = append(antflyTypes, inferAntflyType(jsonType))
		}

		// Validate antfly types
		if len(antflyTypes) > 1 {
			hasText := slices.Contains(antflyTypes, AntflyTypeText)
			hasHtml := slices.Contains(antflyTypes, AntflyTypeHtml)
			hasSearchAsYouType := slices.Contains(antflyTypes, AntflyTypeSearchAsYouType)
			hasKeyword := slices.Contains(antflyTypes, AntflyTypeKeyword)

			// html and text are mutually exclusive
			if hasText && hasHtml {
				continue
			}

			// Count primary types (text or html)
			hasPrimaryType := hasText || hasHtml

			// Allow combinations:
			// 1. primary (text or html) + variants (keyword, search_as_you_type)
			// 2. search_as_you_type + keyword (no primary)
			if !hasPrimaryType && (!hasSearchAsYouType || !hasKeyword || len(antflyTypes) != 2) {
				continue
			}

			// Validate that only keyword and search_as_you_type can be combined with primary types
			if hasPrimaryType && slices.ContainsFunc(antflyTypes, func(antflyType AntflyType) bool {
				switch antflyType {
				case AntflyTypeText, AntflyTypeHtml, AntflyTypeKeyword, AntflyTypeSearchAsYouType:
					return false
				}
				return true
			}) {
				continue
			}
		}

		// Handle special cases: search_as_you_type should always include a primary type for proper field naming
		hasSearchAsYouType := slices.Contains(antflyTypes, AntflyTypeSearchAsYouType)
		hasText := slices.Contains(antflyTypes, AntflyTypeText)
		hasHtml := slices.Contains(antflyTypes, AntflyTypeHtml)
		hasPrimaryType := hasText || hasHtml

		// If search_as_you_type is present without a primary type, add text as default
		if hasSearchAsYouType && !hasPrimaryType {
			antflyTypes = append(antflyTypes, AntflyTypeText)
			hasText = true
		}

		// Check if this field should be included in _all
		shouldIncludeInAll := slices.Contains(includeInAll, fieldName)

		// Sort to ensure primary types (text or html) come first
		slices.SortFunc(antflyTypes, func(a, b AntflyType) int {
			// Primary types (text, html) should come first
			aPrimary := (a == AntflyTypeText || a == AntflyTypeHtml)
			bPrimary := (b == AntflyTypeText || b == AntflyTypeHtml)

			if aPrimary && !bPrimary {
				return -1
			}
			if !aPrimary && bPrimary {
				return 1
			}
			// If both are primary or both are variants, prefer text over html
			if a == AntflyTypeText {
				return -1
			}
			return 0
		})

		// Track if we've added the primary text field for this field name
		isPrimaryTextField := true

		for _, antflyTypeStr := range antflyTypes {
			antflyType := AntflyType(antflyTypeStr)
			fieldMapping := mapping.NewTextFieldMapping()
			fieldMapping.Store = false
			fieldMapping.IncludeInAll = false
			fieldMapping.DocValues = false
			// fieldMapping.Name = fieldName // Default name, may be overridden below

			// Only include in _all if:
			// 1. Field is in includeInAll list
			// 2. Field type is text-based (text, html, keyword, search_as_you_type, link)
			// 3. This is the primary text field (not __keyword or __2gram variants)
			if shouldIncludeInAll && isPrimaryTextField {
				switch antflyType {
				case AntflyTypeText,
					AntflyTypeHtml,
					AntflyTypeKeyword,
					AntflyTypeSearchAsYouType,
					AntflyTypeLink:
					fieldMapping.IncludeInAll = true
					allFieldUsed = true
				}
			}

			switch antflyType {
			case AntflyTypeSearchAsYouType:
				fieldMapping.Analyzer = SearchAsYouTypeAnalyzer
				analyzerNeeded = true
				fieldMapping.Name = fieldName + "__2gram"
			case AntflyTypeHtml:
				fieldMapping.Analyzer = HTMLAnalyzer
				htmlAnalyzerNeeded = true
				// html is a primary type, no suffix needed
			case AntflyTypeText:
				// Regular text field, no special naming
				// if hasText {
				// 	fieldMapping.Name = fmt.Sprintf("%s__text", fieldName)
				// }
			case AntflyTypeKeyword, AntflyTypeLink:
				fieldMapping.Analyzer = keyword.Name
				fieldMapping.DocValues = true // Enable for aggregations/faceting
				// Add suffix if there's a primary type (text or html)
				if hasText || hasHtml {
					fieldMapping.Name = fieldName + "__keyword"
				}
			case AntflyTypeNumeric:
				fieldMapping.Type = "number"
				fieldMapping.DocValues = true // Enable for numeric aggregations (sum, avg, histogram, etc.)
			case AntflyTypeBoolean:
				fieldMapping.Type = "boolean"
			case AntflyTypeDatetime:
				fieldMapping.Type = "datetime"
				fieldMapping.DocValues = true // Enable for date aggregations (date_range, date_histogram)
				if parserName, ok := fieldSchema[XAntflyDateTimeParser].(string); ok && parserName != "" {
					fieldMapping.DateFormat = parserName
				}
			case AntflyTypeGeopoint:
				fieldMapping.Type = "geopoint"
				fieldMapping.DocValues = true // Enable for geo aggregations (geohash_grid, geo_distance)
			case AntflyTypeGeoshape:
				fieldMapping.Type = "geoshape"
			case AntflyTypeEmbedding, AntflyTypeBlob:
				fieldMapping.Index = false
			default:
				continue // Skip unknown types
			}
			if analyzerName, ok := fieldSchema[XAntflyAnalyzer].(string); ok && analyzerName != "" {
				fieldMapping.Analyzer = analyzerName
			}
			docMapping.AddFieldMappingsAt(fieldName, fieldMapping)

			// After adding the first field mapping, subsequent ones are variants (__keyword, __2gram)
			isPrimaryTextField = false
		}
	}
	return docMapping, analyzerNeeded, htmlAnalyzerNeeded, allFieldUsed
}

func inferAntflyType(jsonType string) AntflyType {
	switch jsonType {
	case "string":
		return AntflyTypeText
	case "number", "integer":
		return AntflyTypeNumeric
	case "boolean":
		return AntflyTypeBoolean
	default:
		return ""
	}
}

// NewIndexMapFromSchema creates a Bleve index mapping from a TableSchema based on JSON Schema documents.
func NewIndexMapFromSchema(schema *TableSchema) mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.ScoringModel = index.BM25Scoring
	indexMapping.StoreDynamic = false
	// DocValuesDynamic=false is fine because we explicitly enable DocValues per field type
	// in buildMappingFromJSONSchema() for types that need aggregations (keyword, numeric, datetime, geopoint)
	indexMapping.DocValuesDynamic = false

	var anyAllFieldUsed bool
	if schema != nil && len(schema.DocumentSchemas) > 0 {
		schema.EnsureAnalysisConfig()
		if schema.EnforceTypes {
			indexMapping.IndexDynamic = false
		}
		// New JSON Schema-based logic
		for typeName, docSchema := range schema.DocumentSchemas {
			// Add default fields to the schema if not already present
			properties, ok := docSchema.Schema["properties"].(map[string]any)
			if !ok {
				properties = make(map[string]any)
				docSchema.Schema["properties"] = properties
			}

			// Add default _timestamp field if not already present
			if _, hasTimestamp := properties["_timestamp"]; !hasTimestamp {
				properties["_timestamp"] = map[string]any{
					"type":           "string",
					"format":         "date-time",
					"x-antfly-types": []string{"datetime"},
				}
			}

			// Add default _summaries field if not already present
			// _summaries is a map[string]string where keys are index names and values are summary text
			if _, hasSummaries := properties["_summaries"]; !hasSummaries {
				properties["_summaries"] = map[string]any{
					"type": "object",
					"additionalProperties": map[string]any{
						"type":           "string",
						"x-antfly-types": []string{"text"},
					},
				}
			}

			// Add default _chunks field if not already present
			// Structure: _chunks: { <indexName>: [ {chunk}, {chunk}, ... ], ... }
			if _, hasChunks := properties["_chunks"]; !hasChunks {
				properties["_chunks"] = map[string]any{
					"type": "object",
					"additionalProperties": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"_id":         map[string]any{"type": "string", "x-antfly-types": []string{"keyword"}},
								"_start_char": map[string]any{"type": "integer", "x-antfly-types": []string{"numeric"}},
								"_end_char":   map[string]any{"type": "integer", "x-antfly-types": []string{"numeric"}},
								"_content":    map[string]any{"type": "string", "x-antfly-types": []string{"text"}},
							},
						},
					},
				}
			}

			// Extract x-antfly-include-in-all array from schema
			var includeInAll []string
			if includeInAllI, ok := docSchema.Schema[XAntflyIncludeInAll]; ok {
				switch v := includeInAllI.(type) {
				case []any:
					for _, fieldName := range v {
						if fieldStr, ok := fieldName.(string); ok {
							includeInAll = append(includeInAll, fieldStr)
						}
					}
				case []string:
					includeInAll = v
				}
			}

			// TODO (ajr): Validate docSchema is valid JSON Schema
			// https://github.com/kaptinlin/jsonschema (more active)
			// or
			// https://github.com/invopop/jsonschema (genkit)
			docMapping, analyzerNeeded, htmlNeeded, allFieldUsed := buildMappingFromJSONSchema(
				docSchema.Schema,
				includeInAll,
			)
			_ = analyzerNeeded
			_ = htmlNeeded
			anyAllFieldUsed = anyAllFieldUsed || allFieldUsed

			if schema.DefaultType == typeName {
				indexMapping.DefaultMapping = docMapping
			}
			indexMapping.AddDocumentMapping(typeName, docMapping)
		}
		if err := applyAnalysisConfig(indexMapping, schema.AnalysisConfig); err != nil {
			panic(fmt.Errorf("failed to apply analysis config: %w", err))
		}
	}

	// Apply dynamic templates to the default mapping
	// Templates are evaluated in order when a dynamic field is encountered
	if schema != nil && len(schema.DynamicTemplates) > 0 {
		applyDynamicTemplates(indexMapping.DefaultMapping, schema.DynamicTemplates)
	}

	indexMapping.TypeField = "_type"

	allDocumentMapping := bleve.NewDocumentMapping()
	allDocumentMapping.Enabled = anyAllFieldUsed
	indexMapping.AddDocumentMapping("_all", allDocumentMapping)

	return indexMapping
}

func analysisComponentConfigMap(component AnalysisComponentConfig) map[string]any {
	if nested, ok := component.Config["config"].(map[string]any); ok && len(component.Config) == 1 {
		config := make(map[string]any, len(nested)+1)
		config["type"] = component.Type
		for k, v := range nested {
			config[k] = canonicalizeAnalysisValue(v)
		}
		return config
	}
	config := make(map[string]any, len(component.Config)+1)
	config["type"] = component.Type
	for k, v := range component.Config {
		config[k] = canonicalizeAnalysisValue(v)
	}
	return config
}

func canonicalizeAnalysisValue(value any) any {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = canonicalizeAnalysisValue(item)
		}
		return out
	case []string:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = item
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = canonicalizeAnalysisValue(item)
		}
		return out
	default:
		return value
	}
}

func applyAnalysisConfig(indexMapping *mapping.IndexMappingImpl, cfg *AnalysisConfig) error {
	if cfg == nil {
		return nil
	}
	if cfg.DefaultDateTimeParser != "" {
		indexMapping.DefaultDateTimeParser = cfg.DefaultDateTimeParser
	}
	for name, component := range cfg.TokenFilters {
		if err := indexMapping.AddCustomTokenFilter(name, analysisComponentConfigMap(component)); err != nil {
			return err
		}
	}
	for name, component := range cfg.CharFilters {
		if err := indexMapping.AddCustomCharFilter(name, analysisComponentConfigMap(component)); err != nil {
			return err
		}
	}
	for name, component := range cfg.Tokenizers {
		if err := indexMapping.AddCustomTokenizer(name, analysisComponentConfigMap(component)); err != nil {
			return err
		}
	}
	for name, component := range cfg.Analyzers {
		if err := indexMapping.AddCustomAnalyzer(name, analysisComponentConfigMap(component)); err != nil {
			return err
		}
	}
	for name, component := range cfg.DateTimeParsers {
		if err := indexMapping.AddCustomDateTimeParser(name, analysisComponentConfigMap(component)); err != nil {
			return err
		}
	}
	return nil
}
