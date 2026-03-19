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
	"encoding/json"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/stretchr/testify/require"
)

func TestNewIndexMapFromSchema_JSONSchema(t *testing.T) {
	t.Run("search_as_you_type and keyword", func(t *testing.T) {
		schemaJSON := `{
			"schema": {
				"type": "object",
				"properties": {
					"id": {
						"type": "string",
						"x-antfly-types": ["keyword"]
					},
					"title": {
						"type": "string",
						"x-antfly-types": ["search_as_you_type", "keyword"]
					}
				}
			}
		}`
		var docSchema DocumentSchema
		err := json.Unmarshal([]byte(schemaJSON), &docSchema)
		require.NoError(t, err)

		tableSchema := &TableSchema{
			DefaultType: "post",
			DocumentSchemas: map[string]DocumentSchema{
				"post": docSchema,
			},
		}

		indexMapping := NewIndexMapFromSchema(tableSchema).(*mapping.IndexMappingImpl)

		// Check for custom analyzer
		_, ok := indexMapping.CustomAnalysis.Analyzers[SearchAsYouTypeAnalyzer]
		require.True(t, ok, "search_as_you_type analyzer should be registered")

		// Check field mapping
		postMapping := indexMapping.TypeMapping["post"]
		require.NotNil(t, postMapping)

		titleMapping := postMapping.Properties["title"]
		require.NotNil(t, titleMapping)
		require.Len(t, titleMapping.Fields, 3)

		hasSearchAsYouType := false
		hasKeyword := false
		for _, f := range titleMapping.Fields {
			if f.Analyzer == SearchAsYouTypeAnalyzer {
				hasSearchAsYouType = true
			}
			if f.Analyzer == keyword.Name {
				hasKeyword = true
			}
		}
		require.True(t, hasSearchAsYouType, "should have search_as_you_type mapping")
		require.True(t, hasKeyword, "should have keyword mapping")
	})

	t.Run("nested object", func(t *testing.T) {
		schemaJSON := `{
		"schema": {
			"type": "object",
			"properties": {
				"author": {
					"type": "object",
					"properties": {
						"name": {
							"type": "string",
							"x-antfly-types": ["keyword"]
						}
					}
				}
			}
		}
		}`
		var docSchema DocumentSchema
		err := json.Unmarshal([]byte(schemaJSON), &docSchema)
		require.NoError(t, err)

		tableSchema := &TableSchema{
			DocumentSchemas: map[string]DocumentSchema{"doc": docSchema},
		}

		indexMapping := NewIndexMapFromSchema(tableSchema).(*mapping.IndexMappingImpl)
		docMapping := indexMapping.TypeMapping["doc"]
		require.NotNil(t, docMapping)

		authorMapping := docMapping.Properties["author"]
		require.NotNil(t, authorMapping)
		require.True(t, authorMapping.Enabled)

		nameMapping := authorMapping.Properties["name"]
		require.NotNil(t, nameMapping)
		require.Len(t, nameMapping.Fields, 1)
		require.Equal(t, keyword.Name, nameMapping.Fields[0].Analyzer)
	})

	t.Run("field indexing disabled", func(t *testing.T) {
		schemaJSON := `{
		"schema": {
			"type": "object",
			"properties": {
				"title": {
					"type": "string"
				},
				"raw_data": {
					"type": "string",
					"x-antfly-index": false
				}
			}
		}
		}`
		var docSchema DocumentSchema
		err := json.Unmarshal([]byte(schemaJSON), &docSchema)
		require.NoError(t, err)

		tableSchema := &TableSchema{
			DocumentSchemas: map[string]DocumentSchema{"doc": docSchema},
		}

		indexMapping := NewIndexMapFromSchema(tableSchema).(*mapping.IndexMappingImpl)
		docMapping := indexMapping.TypeMapping["doc"]
		require.NotNil(t, docMapping)
		require.NotNil(t, docMapping.Properties["title"])

		rawMapping := docMapping.Properties["raw_data"]
		require.NotNil(t, rawMapping)
		require.Len(t, rawMapping.Fields, 1)
		require.False(t, rawMapping.Fields[0].Index)
	})

	t.Run("type inference", func(t *testing.T) {
		schemaJSON := `{
		"schema": {
			"type": "object",
			"properties": {
				"description": { "type": "string" },
				"age": { "type": "number" },
				"is_active": { "type": "boolean" }
			}
		}
		}`
		var docSchema DocumentSchema
		err := json.Unmarshal([]byte(schemaJSON), &docSchema)
		require.NoError(t, err)

		tableSchema := &TableSchema{
			DocumentSchemas: map[string]DocumentSchema{"doc": docSchema},
		}

		indexMapping := NewIndexMapFromSchema(tableSchema).(*mapping.IndexMappingImpl)
		docMapping := indexMapping.TypeMapping["doc"]
		require.NotNil(t, docMapping)

		descMapping := docMapping.Properties["description"].Fields[0]
		require.Equal(t, "text", descMapping.Type)

		ageMapping := docMapping.Properties["age"].Fields[0]
		require.Equal(t, "number", ageMapping.Type)

		activeMapping := docMapping.Properties["is_active"].Fields[0]
		require.Equal(t, "boolean", activeMapping.Type)
	})
}

func TestTableSchema_Validate(t *testing.T) {
	validDocSchema := DocumentSchema{
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":   map[string]any{"type": "string"},
				"name": map[string]any{"type": "string"},
			},
		},
	}

	tests := []struct {
		name      string
		schema    *TableSchema
		expectErr bool
	}{
		{
			name: "valid schema",
			schema: &TableSchema{
				DefaultType: "doc",
				DocumentSchemas: map[string]DocumentSchema{
					"doc": validDocSchema,
				},
			},
			expectErr: false,
		},
		{
			name:      "nil schema",
			schema:    nil,
			expectErr: false,
		},
		{
			name:      "empty schema",
			schema:    &TableSchema{},
			expectErr: false,
		},
		// FIXME (ajr) - re-enable when we have full JSON schema validation
		// {
		// 	name: "invalid json schema",
		// 	schema: &TableSchema{
		// 		DocumentSchemas: map[string]DocumentSchema{
		// 			"doc": {"type": "invalid"},
		// 		},
		// 	},
		// 	expectErr: true,
		// },
		{
			name: "default type not in schemas",
			schema: &TableSchema{
				DefaultType: "nonexistent",
				DocumentSchemas: map[string]DocumentSchema{
					"doc": validDocSchema,
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.schema.Validate()
			if tt.expectErr {
				require.Error(t, err, "expected error but got none: %v", tt.schema)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTableSchema_EnsureAnalysisConfig(t *testing.T) {
	schemaJSON := `{
		"schema": {
			"type": "object",
			"properties": {
				"title": {
					"type": "string",
					"x-antfly-types": ["search_as_you_type"]
				},
				"body": {
					"type": "string",
					"x-antfly-types": ["html"]
				}
			}
		}
	}`
	var docSchema DocumentSchema
	err := json.Unmarshal([]byte(schemaJSON), &docSchema)
	require.NoError(t, err)

	tableSchema := &TableSchema{
		DefaultType: "doc",
		DocumentSchemas: map[string]DocumentSchema{
			"doc": docSchema,
		},
	}

	tableSchema.EnsureAnalysisConfig()
	require.NotNil(t, tableSchema.AnalysisConfig)
	require.Equal(t, "dateTimeOptional", tableSchema.AnalysisConfig.DefaultDateTimeParser)
	require.Contains(t, tableSchema.AnalysisConfig.TokenFilters, EdgeNgramTokenFilter)
	require.Contains(t, tableSchema.AnalysisConfig.Analyzers, SearchAsYouTypeAnalyzer)
	require.Contains(t, tableSchema.AnalysisConfig.Analyzers, HTMLAnalyzer)
	require.Equal(t, SearchAsYouTypeAnalyzer, tableSchema.AnalysisConfig.FieldAnalyzers["title"])
	require.Equal(t, HTMLAnalyzer, tableSchema.AnalysisConfig.FieldAnalyzers["body"])
}

func TestTableSchema_EnsureAnalysisConfig_CustomDateTimeParsers(t *testing.T) {
	schemaJSON := `{
		"schema": {
			"type": "object",
			"x-antfly-default-date-time-parser": "queryDT",
			"x-antfly-date-time-parsers": {
				"queryDT": {
					"type": "sanitizedgo",
					"layouts": ["02/01/2006 3:04PM"]
				}
			},
			"properties": {
				"published_at": {
					"type": "string",
					"x-antfly-types": ["datetime"],
					"x-antfly-date-time-parser": "queryDT"
				}
			}
		}
	}`
	var docSchema DocumentSchema
	err := json.Unmarshal([]byte(schemaJSON), &docSchema)
	require.NoError(t, err)

	tableSchema := &TableSchema{
		DefaultType: "doc",
		DocumentSchemas: map[string]DocumentSchema{
			"doc": docSchema,
		},
	}

	tableSchema.EnsureAnalysisConfig()
	require.NotNil(t, tableSchema.AnalysisConfig)
	require.Equal(t, "queryDT", tableSchema.AnalysisConfig.DefaultDateTimeParser)
	require.Contains(t, tableSchema.AnalysisConfig.DateTimeParsers, "queryDT")
	require.Equal(t, "queryDT", tableSchema.AnalysisConfig.FieldDateTimeParsers["published_at"])

	indexMapping := NewIndexMapFromSchema(tableSchema).(*mapping.IndexMappingImpl)
	docMapping := indexMapping.TypeMapping["doc"]
	require.NotNil(t, docMapping)
	require.Contains(t, docMapping.Properties, "published_at")
	require.Len(t, docMapping.Properties["published_at"].Fields, 1)
	require.Equal(t, "queryDT", docMapping.Properties["published_at"].Fields[0].DateFormat)
}

func TestTableSchema_EnsureAnalysisConfig_CustomAnalyzers(t *testing.T) {
	schemaJSON := `{
		"schema": {
			"type": "object",
			"x-antfly-token-filters": {
				"tri_edge": {
					"type": "edge_ngram",
					"min": 3,
					"max": 5
				}
			},
			"x-antfly-analyzers": {
				"tri_edge_analyzer": {
					"type": "custom",
					"tokenizer": "unicode",
					"token_filters": ["to_lower", "tri_edge"]
				}
			},
			"properties": {
				"title": {
					"type": "string",
					"x-antfly-types": ["text"],
					"x-antfly-analyzer": "tri_edge_analyzer"
				}
			}
		}
	}`
	var docSchema DocumentSchema
	err := json.Unmarshal([]byte(schemaJSON), &docSchema)
	require.NoError(t, err)

	tableSchema := &TableSchema{
		DefaultType: "doc",
		DocumentSchemas: map[string]DocumentSchema{
			"doc": docSchema,
		},
	}

	tableSchema.EnsureAnalysisConfig()
	require.NotNil(t, tableSchema.AnalysisConfig)
	require.Contains(t, tableSchema.AnalysisConfig.TokenFilters, "tri_edge")
	require.Contains(t, tableSchema.AnalysisConfig.Analyzers, "tri_edge_analyzer")
	require.Equal(t, "tri_edge_analyzer", tableSchema.AnalysisConfig.FieldAnalyzers["title"])

	indexMapping := NewIndexMapFromSchema(tableSchema).(*mapping.IndexMappingImpl)
	docMapping := indexMapping.TypeMapping["doc"]
	require.NotNil(t, docMapping)
	require.Contains(t, docMapping.Properties, "title")
	require.Len(t, docMapping.Properties["title"].Fields, 1)
	require.Equal(t, "tri_edge_analyzer", docMapping.Properties["title"].Fields[0].Analyzer)
}

func TestTableSchema_EnsureAnalysisConfig_CustomTokenizersAndCharFilters(t *testing.T) {
	schemaJSON := `{
		"schema": {
			"type": "object",
			"x-antfly-char-filters": {
				"strip_html_alias": {
					"type": "html"
				}
			},
			"x-antfly-token-filters": {
				"tri_gram_filter": {
					"type": "ngram",
					"min": 3,
					"max": 3
				}
			},
			"x-antfly-tokenizers": {
				"whitespace_alias": {
					"type": "whitespace"
				}
			},
			"x-antfly-analyzers": {
				"tri_html_analyzer": {
					"type": "custom",
					"tokenizer": "whitespace_alias",
					"char_filters": ["strip_html_alias"],
					"token_filters": ["to_lower", "tri_gram_filter"]
				}
			},
			"properties": {
				"title": {
					"type": "string",
					"x-antfly-types": ["text"],
					"x-antfly-analyzer": "tri_html_analyzer"
				}
			}
		}
	}`
	var docSchema DocumentSchema
	err := json.Unmarshal([]byte(schemaJSON), &docSchema)
	require.NoError(t, err)

	tableSchema := &TableSchema{
		DefaultType: "doc",
		DocumentSchemas: map[string]DocumentSchema{
			"doc": docSchema,
		},
	}

	tableSchema.EnsureAnalysisConfig()
	require.NotNil(t, tableSchema.AnalysisConfig)
	require.Contains(t, tableSchema.AnalysisConfig.CharFilters, "strip_html_alias")
	require.Contains(t, tableSchema.AnalysisConfig.TokenFilters, "tri_gram_filter")
	require.Contains(t, tableSchema.AnalysisConfig.Tokenizers, "whitespace_alias")
	require.Contains(t, tableSchema.AnalysisConfig.Analyzers, "tri_html_analyzer")
	require.Equal(t, "tri_html_analyzer", tableSchema.AnalysisConfig.FieldAnalyzers["title"])

	indexMapping := NewIndexMapFromSchema(tableSchema).(*mapping.IndexMappingImpl)
	docMapping := indexMapping.TypeMapping["doc"]
	require.NotNil(t, docMapping)
	require.Contains(t, docMapping.Properties, "title")
	require.Len(t, docMapping.Properties["title"].Fields, 1)
	require.Equal(t, "tri_html_analyzer", docMapping.Properties["title"].Fields[0].Analyzer)
}
