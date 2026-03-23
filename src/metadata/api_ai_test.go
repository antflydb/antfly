package metadata

import (
	"testing"

	"github.com/antflydb/antfly/lib/query"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSchemaDescriptionUsesRichSchemaMetadata(t *testing.T) {
	api := &TableApi{}
	tableSchema := &schema.TableSchema{
		DefaultType: "article",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"article": {
				Description: "Article documents",
				Schema: map[string]any{
					"description": "Article documents",
					"properties": map[string]any{
						"status": map[string]any{
							"type":           "string",
							"description":    "Publication status",
							"enum":           []any{"draft", "published"},
							"x-antfly-types": []any{"keyword"},
						},
						"rating": map[string]any{
							"type":        "integer",
							"minimum":     1,
							"maximum":     5,
							"description": "Editorial score",
						},
						"author": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"name": map[string]any{
									"type":        "string",
									"description": "Author display name",
								},
							},
						},
					},
				},
			},
		},
	}

	desc := api.buildSchemaDescription(tableSchema, []string{"status", "rating", "author"})

	require.Equal(t, "Article documents", desc.Description)
	require.Len(t, desc.Fields, 3)

	fields := make(map[string]query.FieldInfo, len(desc.Fields))
	for _, field := range desc.Fields {
		fields[field.Name] = field
	}

	assert.Equal(t, "keyword", fields["status"].Type)
	assert.ElementsMatch(t, []string{"draft", "published"}, fields["status"].ExampleValues)

	require.NotNil(t, fields["rating"].ValueRange)
	require.NotNil(t, fields["rating"].ValueRange.Min)
	require.NotNil(t, fields["rating"].ValueRange.Max)
	assert.Equal(t, 1.0, *fields["rating"].ValueRange.Min)
	assert.Equal(t, 5.0, *fields["rating"].ValueRange.Max)

	assert.True(t, fields["author"].Nested)
	require.Len(t, fields["author"].Children, 1)
	assert.Equal(t, "name", fields["author"].Children[0].Name)
}

func TestBuildSchemaDescriptionWithoutSchemaFallsBackToFlatFields(t *testing.T) {
	api := &TableApi{}

	desc := api.buildSchemaDescription(nil, []string{"title", "content"})

	require.Len(t, desc.Fields, 2)
	assert.Equal(t, "title", desc.Fields[0].Name)
	assert.Equal(t, "text", desc.Fields[0].Type)
	assert.Equal(t, "content", desc.Fields[1].Name)
	assert.Equal(t, "text", desc.Fields[1].Type)
}

func TestFilterQueryBuilderExampleDocument(t *testing.T) {
	doc := map[string]any{
		"title":   "Vector Search",
		"status":  "published",
		"ignored": true,
	}

	filtered := filterQueryBuilderExampleDocument(doc, []string{"title", "status"})

	assert.Equal(t, map[string]any{
		"title":  "Vector Search",
		"status": "published",
	}, filtered)
}
