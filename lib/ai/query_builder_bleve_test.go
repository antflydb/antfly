package ai

import (
	"testing"

	querylib "github.com/antflydb/antfly/lib/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildBleveQueryBuilderSystemPromptIncludesDetailedSchemaAndExampleDocuments(t *testing.T) {
	min := 1.0
	max := 5.0
	schema := querylib.SchemaDescription{
		Description: "Article search fields",
		Fields: []querylib.FieldInfo{
			{
				Name:          "status",
				Type:          "keyword",
				Searchable:    true,
				ExampleValues: []string{"draft", "published"},
			},
			{
				Name:       "rating",
				Type:       "numeric",
				Searchable: true,
				ValueRange: &querylib.ValueRange{
					Min: &min,
					Max: &max,
				},
			},
		},
	}

	prompt := buildBleveQueryBuilderSystemPrompt(schema, false, []string{"status: published"})

	require.NotEmpty(t, prompt)
	assert.Contains(t, prompt, "## Available Fields")
	assert.Contains(t, prompt, "Values: draft, published")
	assert.Contains(t, prompt, "Range: 1 to 5")
	assert.Contains(t, prompt, "## Example Documents")
	assert.Contains(t, prompt, "status: published")
	assert.Contains(t, prompt, `"term": "draft", "field": "status"`)
}
