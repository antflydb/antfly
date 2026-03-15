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

package ai

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/lib/query"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"go.uber.org/zap"
)

// QueryBuilderResult contains the result of query building
type QueryBuilderResult struct {
	// Query is the generated query in native Bleve format
	Query map[string]any `json:"query"`
	// Explanation describes what the query does and why it was structured this way
	Explanation string `json:"explanation,omitempty"`
	// Confidence is the model's confidence in the generated query (0.0-1.0)
	Confidence float64 `json:"confidence"`
	// Warnings contains any issues or limitations with the generated query
	Warnings []string `json:"warnings,omitempty"`
}

// QueryBuilderOption is a functional option for customizing query building
type QueryBuilderOption interface {
	applyQueryBuilderOption(*queryBuilderOptions)
}

type queryBuilderOptions struct {
	systemPrompt  string
	withExamples  bool
	maxComplexity int // Maximum query nesting depth
}

type queryBuilderSystemPromptOption struct {
	prompt string
}

func (o queryBuilderSystemPromptOption) applyQueryBuilderOption(opts *queryBuilderOptions) {
	opts.systemPrompt = o.prompt
}

// WithQueryBuilderSystemPrompt sets a custom system prompt for query building
func WithQueryBuilderSystemPrompt(prompt string) QueryBuilderOption {
	return queryBuilderSystemPromptOption{prompt: prompt}
}

type queryBuilderExamplesOption struct {
	enabled bool
}

func (o queryBuilderExamplesOption) applyQueryBuilderOption(opts *queryBuilderOptions) {
	opts.withExamples = o.enabled
}

// WithQueryBuilderExamples enables/disables example queries in the prompt
func WithQueryBuilderExamples(enabled bool) QueryBuilderOption {
	return queryBuilderExamplesOption{enabled: enabled}
}

type queryBuilderMaxComplexityOption struct {
	maxDepth int
}

func (o queryBuilderMaxComplexityOption) applyQueryBuilderOption(opts *queryBuilderOptions) {
	opts.maxComplexity = o.maxDepth
}

// WithQueryBuilderMaxComplexity sets the maximum nesting depth for queries
func WithQueryBuilderMaxComplexity(maxDepth int) QueryBuilderOption {
	return queryBuilderMaxComplexityOption{maxDepth: maxDepth}
}

// parseQueryBuilderResponse parses the LLM response for query building
func parseQueryBuilderResponse(responseText string) (*QueryBuilderResult, error) {
	// Try to extract JSON from the response
	jsonStart := strings.Index(responseText, "{")
	jsonEnd := strings.LastIndex(responseText, "}")

	if jsonStart == -1 || jsonEnd == -1 || jsonEnd <= jsonStart {
		return nil, fmt.Errorf("no valid JSON object found in response")
	}

	jsonText := responseText[jsonStart : jsonEnd+1]

	// Parse the JSON
	var result QueryBuilderResult
	if err := json.Unmarshal([]byte(jsonText), &result); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Validate required fields
	if result.Query == nil {
		return nil, fmt.Errorf("response missing 'query' field")
	}
	if result.Confidence < 0 || result.Confidence > 1 {
		result.Confidence = 0.5
	}

	return &result, nil
}

// BuildQueryBleve generates a query in native Bleve format from natural language.
// This is the raw format that Bleve's query parser expects.
func (g *GenKitModelImpl) BuildQueryBleve(
	ctx context.Context,
	intent string,
	schemaDescription query.SchemaDescription,
	opts ...QueryBuilderOption,
) (*QueryBuilderResult, error) {
	if intent == "" {
		return nil, errors.New("intent cannot be empty")
	}

	// Apply options
	options := &queryBuilderOptions{
		withExamples:  true,
		maxComplexity: 3,
	}
	for _, opt := range opts {
		opt.applyQueryBuilderOption(options)
	}

	// Build system prompt for Bleve format
	systemPrompt := options.systemPrompt
	if systemPrompt == "" {
		systemPrompt = buildBleveQueryBuilderSystemPrompt(schemaDescription, options.withExamples)
	}

	// Build the user prompt
	promptTemplate := `User's search intent: "{{intent}}"

Generate a search query in NATIVE BLEVE FORMAT that fulfills this intent.
Return your response in JSON format:
{
  "query": { ... your native Bleve query ... },
  "explanation": "brief explanation of the query",
  "confidence": 0.0 to 1.0,
  "warnings": ["optional warnings or limitations"]
}

Return ONLY the JSON object, no additional text.`

	// Generate a unique prompt name
	schemaJSON, _ := query.SchemaToJSON(schemaDescription)
	hash := sha256.Sum256([]byte(systemPrompt + schemaJSON + "bleve"))
	promptName := fmt.Sprintf("query-builder-bleve-%x", hash[:8])

	// Check if prompt already exists, if not define it
	queryPrompt := genkit.LookupPrompt(g.Genkit, promptName)
	if queryPrompt == nil {
		queryPrompt = genkit.DefinePrompt(g.Genkit, promptName,
			ai.WithDescription("Build native Bleve search query from natural language"),
			ai.WithModel(g.Model),
			ai.WithSystem(systemPrompt),
			ai.WithPrompt(promptTemplate),
		)
	}

	// Prepare input
	input := map[string]any{
		"intent": intent,
	}

	// Execute prompt
	resp, err := queryPrompt.Execute(ctx, ai.WithInput(input))
	if err != nil {
		g.logger.Warn("Bleve query builder LLM call failed",
			zap.Error(err),
			zap.String("intent", intent),
		)
		return nil, fmt.Errorf("LLM query generation failed: %w", err)
	}

	// Extract and parse the response
	responseText := strings.TrimSpace(resp.Text())

	// Parse the JSON response
	result, parseErr := parseQueryBuilderResponse(responseText)
	if parseErr != nil {
		g.logger.Warn("Failed to parse Bleve query builder response",
			zap.Error(parseErr),
			zap.String("intent", intent),
			zap.String("response_text", responseText),
		)
		return nil, fmt.Errorf("failed to parse query builder response: %w", parseErr)
	}

	return result, nil
}

// buildBleveQueryBuilderSystemPrompt constructs the system prompt for Bleve query building
func buildBleveQueryBuilderSystemPrompt(schema query.SchemaDescription, withExamples bool) string {
	var sb strings.Builder

	sb.WriteString(`You are an expert search query builder. Your task is to translate natural language search intents into NATIVE BLEVE search queries.

## Native Bleve Query Format

Use the raw Bleve query format directly:

### Text Queries
- **match**: Full-text search
  {"match": "search terms", "field": "content"}
  Optional: "operator": "and" or "or"
  Optional: "boost": 2.0 (increase relevance weight)
  Optional: "analyzer": "en" (specify text analyzer)

- **match_phrase**: Exact phrase match (words must appear consecutively)
  {"match_phrase": "machine learning", "field": "title"}
  Optional: "boost": 2.0

- **term**: Exact term match (no analysis applied)
  {"term": "exact_value", "field": "status"}
  Optional: "boost": 1.5

- **prefix**: Prefix matching
  {"prefix": "micro", "field": "title"}

- **wildcard**: Pattern matching
  {"wildcard": "pro*", "field": "name"}

- **regexp**: Regular expression matching
  {"regexp": "user-[0-9]+", "field": "author_id"}

### Boolean/Compound Queries
- **conjuncts**: All conditions must match (AND)
  {"conjuncts": [query1, query2, ...]}
  Optional: "boost": 1.0

- **disjuncts**: Any condition can match (OR)
  {"disjuncts": [query1, query2, ...]}
  Optional: "min": 1 (minimum matches required)
  Optional: "boost": 1.0

- **must_not**: Exclude matching documents (NOT)
  {"must_not": {"disjuncts": [query_to_exclude]}}

- **boolean**: Full boolean query with must/should/must_not
  {
    "must": {"conjuncts": [required_queries]},
    "should": {"disjuncts": [optional_queries]},
    "must_not": {"disjuncts": [excluded_queries]}
  }

### Range Queries
- **Numeric range**:
  {"field": "price", "min": 100, "max": 500, "inclusive_min": true, "inclusive_max": true}

- **Date range**:
  {"field": "created_at", "start": "2024-01-01T00:00:00Z", "end": "2024-12-31T23:59:59Z", "inclusive_start": true, "inclusive_end": true}

- **Term range** (alphabetical):
  {"field": "name", "min": "a", "max": "m", "inclusive_min": true, "inclusive_max": false}

### Geo Queries
- **geo_distance**:
  {"field": "location", "location": [-122.4194, 37.7749], "distance": "10km"}

- **geo_bounding_box**:
  {"field": "location", "top_left": [-122.5, 37.9], "bottom_right": [-122.3, 37.7]}

### Special Queries
- **match_all**: Match all documents
  {"match_all": {}}

- **match_none**: Match no documents
  {"match_none": {}}

- **docids**: Match specific document IDs
  {"ids": ["doc-1", "doc-2"]}

## Guidelines

1. **Use native Bleve terminology:**
   - "conjuncts" for AND (not "and")
   - "disjuncts" for OR (not "or")
   - "must_not" for NOT (wrapped in disjuncts)
   - "match_phrase" for phrase queries (not "phrase")
   - "min"/"max" with "inclusive_min"/"inclusive_max" for ranges

2. **Boosting:** Use "boost" to weight query importance

3. **Range queries:** Always specify inclusivity explicitly

4. **Complex negation:** Wrap negated query in must_not with disjuncts array

5. **Keep queries simple when possible:** Start with the most important condition
`)

	// Add schema context
	if len(schema.Fields) > 0 {
		sb.WriteString("\n## Available Fields\n\n")
		sb.WriteString(query.FormatSchemaForLLM(schema))
		sb.WriteString("\n")
	}

	// Add examples if enabled
	if withExamples {
		sb.WriteString(`
## Examples

Intent: "Find all published articles about machine learning"
Query:
{
  "conjuncts": [
    {"match": "machine learning", "field": "content"},
    {"term": "published", "field": "status"}
  ]
}

Intent: "Find documents with exact phrase 'Getting Started'"
Query:
{
  "match_phrase": "Getting Started",
  "field": "title"
}

Intent: "Search for products between $50 and $100"
Query:
{
  "field": "price",
  "min": 50,
  "max": 100,
  "inclusive_min": true,
  "inclusive_max": true
}

Intent: "Find restaurants within 5km of downtown"
Query:
{
  "field": "location",
  "location": [-122.4194, 37.7749],
  "distance": "5km"
}

Intent: "Documents about Python but not tutorials"
Query:
{
  "conjuncts": [
    {"match": "Python", "field": "content"},
    {"must_not": {"disjuncts": [{"term": "tutorial", "field": "category"}]}}
  ]
}

Intent: "Find items in draft or pending status"
Query:
{
  "disjuncts": [
    {"term": "draft", "field": "status"},
    {"term": "pending", "field": "status"}
  ]
}

Intent: "Articles created after January 2024"
Query:
{
  "field": "created_at",
  "start": "2024-01-01T00:00:00Z",
  "inclusive_start": true
}

Intent: "Exclude archived items from search"
Query:
{
  "must_not": {
    "disjuncts": [{"term": "archived", "field": "status"}]
  }
}
`)
	}

	return sb.String()
}
