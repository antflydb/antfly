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
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/mapping"
)

func TestTranslateTemplateFieldMapping(t *testing.T) {
	tests := []struct {
		name           string
		input          TemplateFieldMapping
		wantType       string
		wantAnalyzer   string
		wantIndex      bool
		wantStore      bool
		wantIncludeAll bool
		wantDocValues  bool
	}{
		{
			name: "text type",
			input: TemplateFieldMapping{
				Type: AntflyTypeText,
			},
			wantType:     "text",
			wantAnalyzer: "",
			wantIndex:    true,
		},
		{
			name: "keyword type",
			input: TemplateFieldMapping{
				Type: AntflyTypeKeyword,
			},
			wantType:     "text",
			wantAnalyzer: keyword.Name,
			wantIndex:    true,
		},
		{
			name: "numeric type",
			input: TemplateFieldMapping{
				Type: AntflyTypeNumeric,
			},
			wantType:  "number",
			wantIndex: true,
		},
		{
			name: "boolean type",
			input: TemplateFieldMapping{
				Type: AntflyTypeBoolean,
			},
			wantType:  "boolean",
			wantIndex: true,
		},
		{
			name: "datetime type",
			input: TemplateFieldMapping{
				Type: AntflyTypeDatetime,
			},
			wantType:  "datetime",
			wantIndex: true,
		},
		{
			name: "geopoint type",
			input: TemplateFieldMapping{
				Type: AntflyTypeGeopoint,
			},
			wantType:  "geopoint",
			wantIndex: true,
		},
		{
			name: "geoshape type",
			input: TemplateFieldMapping{
				Type: AntflyTypeGeoshape,
			},
			wantType:  "geoshape",
			wantIndex: true,
		},
		{
			name: "html type with analyzer",
			input: TemplateFieldMapping{
				Type: AntflyTypeHtml,
			},
			wantType:     "text",
			wantAnalyzer: HTMLAnalyzer,
			wantIndex:    true,
		},
		{
			name: "search_as_you_type",
			input: TemplateFieldMapping{
				Type: AntflyTypeSearchAsYouType,
			},
			wantType:     "text",
			wantAnalyzer: SearchAsYouTypeAnalyzer,
			wantIndex:    true,
		},
		{
			name: "embedding type (not indexed)",
			input: TemplateFieldMapping{
				Type: AntflyTypeEmbedding,
			},
			wantType:  "text",
			wantIndex: false,
		},
		{
			name: "custom analyzer override",
			input: TemplateFieldMapping{
				Type:     AntflyTypeText,
				Analyzer: "custom_analyzer",
			},
			wantType:     "text",
			wantAnalyzer: "custom_analyzer",
			wantIndex:    true,
		},
		{
			name: "store and doc_values enabled",
			input: TemplateFieldMapping{
				Type:      AntflyTypeText,
				Store:     true,
				DocValues: true,
			},
			wantType:      "text",
			wantIndex:     true,
			wantStore:     true,
			wantDocValues: true,
		},
		{
			name: "include_in_all enabled",
			input: TemplateFieldMapping{
				Type:         AntflyTypeText,
				IncludeInAll: true,
			},
			wantType:       "text",
			wantIndex:      true,
			wantIncludeAll: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translateTemplateFieldMapping(tt.input)

			if got.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", got.Type, tt.wantType)
			}
			if got.Analyzer != tt.wantAnalyzer {
				t.Errorf("Analyzer = %v, want %v", got.Analyzer, tt.wantAnalyzer)
			}
			if got.Index != tt.wantIndex {
				t.Errorf("Index = %v, want %v", got.Index, tt.wantIndex)
			}
			if got.Store != tt.wantStore {
				t.Errorf("Store = %v, want %v", got.Store, tt.wantStore)
			}
			if got.IncludeInAll != tt.wantIncludeAll {
				t.Errorf("IncludeInAll = %v, want %v", got.IncludeInAll, tt.wantIncludeAll)
			}
			if got.DocValues != tt.wantDocValues {
				t.Errorf("DocValues = %v, want %v", got.DocValues, tt.wantDocValues)
			}
		})
	}
}

func TestApplyDynamicTemplates(t *testing.T) {
	tests := []struct {
		name      string
		templates []DynamicTemplate
		wantCount int
	}{
		{
			name:      "empty templates",
			templates: nil,
			wantCount: 0,
		},
		{
			name: "single template",
			templates: []DynamicTemplate{
				{
					Name:  "text_fields",
					Match: "*_text",
					Mapping: TemplateFieldMapping{
						Type: AntflyTypeText,
					},
				},
			},
			wantCount: 1,
		},
		{
			name: "multiple templates",
			templates: []DynamicTemplate{
				{
					Name:  "text_fields",
					Match: "*_text",
					Mapping: TemplateFieldMapping{
						Type: AntflyTypeText,
					},
				},
				{
					Name:  "keyword_fields",
					Match: "*_keyword",
					Mapping: TemplateFieldMapping{
						Type: AntflyTypeKeyword,
					},
				},
				{
					Name:      "skip_internal",
					PathMatch: "_internal.**",
					Mapping: TemplateFieldMapping{
						Type: AntflyTypeText,
					},
				},
			},
			wantCount: 3,
		},
		{
			name: "template with all matching options",
			templates: []DynamicTemplate{
				{
					Name:             "full_template",
					Match:            "*_field",
					Unmatch:          "skip_*",
					PathMatch:        "data.**",
					PathUnmatch:      "data.internal.**",
					MatchMappingType: DynamicTemplateMatchMappingTypeString,
					Mapping: TemplateFieldMapping{
						Type:     AntflyTypeText,
						Analyzer: "standard",
					},
				},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fresh document mapping for each test
			schema := &TableSchema{
				DynamicTemplates: tt.templates,
			}
			indexMapping := NewIndexMapFromSchema(schema)

			// Type assert to get access to DefaultMapping
			impl, ok := indexMapping.(*mapping.IndexMappingImpl)
			if !ok {
				t.Fatal("expected *mapping.IndexMappingImpl")
			}

			// Check that templates were applied
			if got := len(impl.DefaultMapping.DynamicTemplates); got != tt.wantCount {
				t.Errorf("template count = %v, want %v", got, tt.wantCount)
			}
		})
	}
}

func TestNewIndexMapFromSchemaWithTemplates(t *testing.T) {
	// Test that templates are properly integrated into the index mapping
	schema := &TableSchema{
		DocumentSchemas: map[string]DocumentSchema{
			"article": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title": map[string]any{
							"type":           "string",
							"x-antfly-types": []string{"text"},
						},
					},
					"additionalProperties": true,
				},
			},
		},
		DefaultType: "article",
		DynamicTemplates: []DynamicTemplate{
			{
				Name:  "text_suffix",
				Match: "*_text",
				Mapping: TemplateFieldMapping{
					Type:     AntflyTypeText,
					Analyzer: "standard",
				},
			},
			{
				Name:  "id_suffix",
				Match: "*_id",
				Mapping: TemplateFieldMapping{
					Type: AntflyTypeKeyword,
				},
			},
		},
	}

	indexMapping := NewIndexMapFromSchema(schema)

	// Type assert to get access to DefaultMapping
	impl, ok := indexMapping.(*mapping.IndexMappingImpl)
	if !ok {
		t.Fatal("expected *mapping.IndexMappingImpl")
	}

	// Verify templates were added to default mapping
	if len(impl.DefaultMapping.DynamicTemplates) != 2 {
		t.Errorf("expected 2 templates, got %d", len(impl.DefaultMapping.DynamicTemplates))
	}

	// Verify first template
	if impl.DefaultMapping.DynamicTemplates[0].Name != "text_suffix" {
		t.Errorf("expected first template name 'text_suffix', got %s",
			impl.DefaultMapping.DynamicTemplates[0].Name)
	}
	if impl.DefaultMapping.DynamicTemplates[0].Match != "*_text" {
		t.Errorf("expected first template match '*_text', got %s",
			impl.DefaultMapping.DynamicTemplates[0].Match)
	}
}
