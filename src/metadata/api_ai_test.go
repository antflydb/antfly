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

func TestNormalizeQueryBuilderSessionRequest(t *testing.T) {
	req := &QueryBuilderRequest{Intent: "find published posts"}

	normalizeQueryBuilderSessionRequest(req)

	require.NotEmpty(t, req.SessionId)
	assert.Contains(t, req.SessionId, "qbs_")
}

func TestBuildEffectiveQueryBuilderIntentIncludesDecisions(t *testing.T) {
	req := &QueryBuilderRequest{
		Intent: "find recent published posts",
		Decisions: []AgentDecision{
			{QuestionId: "time_window", Answer: "last 30 days"},
			{QuestionId: "status_scope", Approved: true},
		},
	}

	intent := buildEffectiveQueryBuilderIntent(req)

	assert.Contains(t, intent, "find recent published posts")
	assert.Contains(t, intent, "Resolved user decisions:")
	assert.Contains(t, intent, "time_window: last 30 days")
	assert.Contains(t, intent, "status_scope: approved")
}

func TestFinalizeQueryBuilderSession(t *testing.T) {
	req := &QueryBuilderRequest{
		SessionId:             "qbs_test",
		MaxInternalIterations: 3,
		MaxUserClarifications: 2,
		Decisions:             []AgentDecision{{QuestionId: "time_window", Answer: "last 30 days"}},
	}

	result := finalizeQueryBuilderSession(req, &QueryBuilderResult{})

	assert.Equal(t, "qbs_test", result.SessionId)
	assert.Equal(t, AgentStatusCompleted, result.Status)
	assert.Equal(t, 1, result.Iteration)
	assert.Equal(t, 1, result.ClarificationCount)
	assert.Equal(t, 2, result.RemainingInternalIterations)
	assert.Equal(t, 1, result.RemainingUserClarifications)
	if assert.Len(t, result.Steps, 1) {
		assert.Equal(t, AgentStepKindGeneration, result.Steps[0].Kind)
		assert.Equal(t, "query_builder", result.Steps[0].Name)
		assert.Equal(t, AgentStepStatusSuccess, result.Steps[0].Status)
	}
}
