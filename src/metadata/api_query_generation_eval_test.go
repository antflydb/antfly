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

package metadata

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/query"
	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/stretchr/testify/require"
)

// TestQueryGeneration_StructuralQuickEval runs fast structural checks on generated queries.
// Tests that queries are valid and use correct query types.
func TestQueryGeneration_StructuralQuickEval(t *testing.T) {
	skipIfOllamaUnavailable(t)

	// Load dataset
	dataset := loadDataset(t, "query_generation_structural",
		"testdata/query_generation_dataset.json")

	// Use structural evaluators (no LLM needed)
	evaluators := []eval.Evaluator{
		NewQueryValidityEvaluator("validity"),
		NewQueryStructureEvaluator("structure"),
	}

	// Create target function
	target := createQueryGenerationTargetFunc(t)

	// Configure runner
	config := *eval.DefaultConfig()
	config.Execution.Parallel = true
	config.Execution.MaxConcurrency = 3

	// Run evaluation
	runner := eval.NewRunner(config, evaluators)
	report, err := runner.RunWithTarget(t.Context(), dataset, target)
	require.NoError(t, err)

	// Log summary
	t.Logf("Query Generation Structural Evaluation Results:")
	t.Logf("  Pass Rate: %.2f%%", report.Summary.PassRate*100)
	t.Logf("  Average Score: %.2f", report.Summary.AverageScore)
	t.Logf("  Total Examples: %d", report.Summary.TotalExamples)
	t.Logf("  Passed: %d", report.Summary.PassedExamples)
	t.Logf("  Failed: %d", report.Summary.FailedExamples)

	// Log per-evaluator stats
	for name, stats := range report.Summary.EvaluatorStats {
		t.Logf("  %s: Pass Rate %.2f%%, Avg Score %.2f",
			name, stats.PassRate*100, stats.AverageScore)
	}

	// Assert minimum pass rates
	assertPassRate(t, report, 0.80, "Query generation structural")

	// Save report
	saveReportToTestdata(t, report, "query_generation_structural_eval_results.json")
}

// TestQueryGeneration_SemanticComprehensiveEval runs comprehensive evaluation with LLM-as-judge.
func TestQueryGeneration_SemanticComprehensiveEval(t *testing.T) {
	skipIfEvalDisabled(t)

	// Load dataset
	dataset := loadDataset(t, "query_generation_comprehensive",
		"testdata/query_generation_dataset.json")

	// Initialize Genkit for LLM-as-judge
	g := initGenkitForEval(t)

	// Use structural + semantic evaluators
	evaluators := []eval.Evaluator{
		NewQueryValidityEvaluator("validity"),
		NewQueryStructureEvaluator("structure"),
		NewQuerySemanticEvaluator(g, "ollama/gemma3:4b", "semantic"),
	}

	// Create target function
	target := createQueryGenerationTargetFunc(t)

	// Configure runner (sequential for LLM calls)
	config := *eval.DefaultConfig()
	config.Execution.Parallel = false
	config.Execution.MaxConcurrency = 1

	// Run evaluation
	runner := eval.NewRunner(config, evaluators)
	report, err := runner.RunWithTarget(t.Context(), dataset, target)
	require.NoError(t, err)

	// Log detailed summary
	t.Logf("Comprehensive Query Generation Evaluation Results:")
	t.Logf("  Pass Rate: %.2f%%", report.Summary.PassRate*100)
	t.Logf("  Average Score: %.2f", report.Summary.AverageScore)
	t.Logf("  Total Examples: %d", report.Summary.TotalExamples)

	// Log per-evaluator stats
	t.Logf("\nPer-Evaluator Statistics:")
	for name, stats := range report.Summary.EvaluatorStats {
		t.Logf("  %s:", name)
		t.Logf("    Pass Rate: %.2f%%", stats.PassRate*100)
		t.Logf("    Average Score: %.2f", stats.AverageScore)
	}

	// Assert minimum pass rates (more lenient for comprehensive)
	assertPassRate(t, report, 0.70, "Query generation comprehensive")

	// Save detailed report
	saveReportToTestdata(t, report, "query_generation_comprehensive_eval_results.json")
}

// createQueryGenerationTargetFunc creates an evalaf target function for query generation.
func createQueryGenerationTargetFunc(t *testing.T) eval.TargetFunc {
	t.Helper()

	// Create a GenKit generator once for all evaluations
	config := getDefaultOllamaConfig(t)
	var generator *ai.GenKitModelImpl

	return func(ctx context.Context, example eval.Example) (any, error) {
		// Get input intent
		intent, ok := example.Input.(string)
		if !ok {
			t.Fatalf("Expected input to be string, got %T", example.Input)
		}

		// Get schema fields from metadata
		var schemaFields []string
		if fields, ok := example.Metadata["schema_fields"].([]any); ok {
			for _, f := range fields {
				if s, ok := f.(string); ok {
					schemaFields = append(schemaFields, s)
				}
			}
		}

		// Lazy initialize generator (avoids creating multiple instances)
		if generator == nil {
			var err error
			generator, err = ai.NewGenKitGenerator(ctx, *config)
			if err != nil {
				return nil, err
			}
		}

		// Build schema description from field names
		schemaDesc := query.SchemaDescription{
			Fields: make([]query.FieldInfo, 0, len(schemaFields)),
		}
		for _, name := range schemaFields {
			schemaDesc.Fields = append(schemaDesc.Fields, query.FieldInfo{
				Name:       name,
				Type:       "text", // Default assumption
				Searchable: true,
			})
		}

		// Generate query using the Bleve query builder
		result, err := generator.BuildQueryBleve(ctx, intent, schemaDesc)
		if err != nil {
			return nil, err
		}

		// Return structured output
		return map[string]any{
			"query":         result.Query,
			"intent":        intent,
			"schema_fields": schemaFields,
			"explanation":   result.Explanation,
			"confidence":    result.Confidence,
			"warnings":      result.Warnings,
		}, nil
	}
}

// --- Query Generation Evaluators ---

// QueryValidityEvaluator checks if a generated query is valid JSON and can be parsed.
type QueryValidityEvaluator struct {
	name string
}

func NewQueryValidityEvaluator(name string) *QueryValidityEvaluator {
	return &QueryValidityEvaluator{name: name}
}

func (e *QueryValidityEvaluator) Name() string {
	return e.name
}

func (e *QueryValidityEvaluator) SupportsStreaming() bool {
	return false
}

func (e *QueryValidityEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	outputMap, ok := input.Output.(map[string]any)
	if !ok {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "output is not a map",
		}, nil
	}

	queryData, ok := outputMap["query"]
	if !ok {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "output missing 'query' field",
		}, nil
	}

	// Try to marshal and validate as a native Bleve query (valid JSON object)
	queryJSON, err := json.Marshal(queryData)
	if err != nil {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "failed to marshal query: " + err.Error(),
		}, nil
	}

	var nativeQuery map[string]any
	if err := json.Unmarshal(queryJSON, &nativeQuery); err != nil {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "invalid query JSON: " + err.Error(),
		}, nil
	}

	if len(nativeQuery) == 0 {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "empty query object",
		}, nil
	}

	return &eval.EvalResult{
		Pass:   true,
		Score:  1.0,
		Reason: "valid native bleve query",
	}, nil
}

// QueryStructureEvaluator checks if the generated query has the expected structure.
type QueryStructureEvaluator struct {
	name string
}

func NewQueryStructureEvaluator(name string) *QueryStructureEvaluator {
	return &QueryStructureEvaluator{name: name}
}

func (e *QueryStructureEvaluator) Name() string {
	return e.name
}

func (e *QueryStructureEvaluator) SupportsStreaming() bool {
	return false
}

func (e *QueryStructureEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	outputMap, ok := input.Output.(map[string]any)
	if !ok {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "output is not a map",
		}, nil
	}

	generatedQuery, ok := outputMap["query"].(map[string]any)
	if !ok {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "query is not a map",
		}, nil
	}

	// Get reference query
	referenceQuery, ok := input.Reference.(map[string]any)
	if !ok {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "reference is not a map",
		}, nil
	}

	// Check if query types match (top-level keys)
	generatedKeys := getTopLevelKeys(generatedQuery)
	referenceKeys := getTopLevelKeys(referenceQuery)

	// Calculate structural similarity
	commonKeys := intersection(generatedKeys, referenceKeys)
	if len(referenceKeys) == 0 {
		return &eval.EvalResult{
			Pass:   true,
			Score:  1.0,
			Reason: "empty reference",
		}, nil
	}

	similarity := float64(len(commonKeys)) / float64(len(referenceKeys))

	// Check if main discriminator matches
	discriminators := []string{"match", "term", "prefix", "wildcard", "regexp", "and", "or", "not", "field", "geo_distance", "geo_bbox", "match_all", "match_none", "ids", "bool"}
	var generatedDiscriminator, referenceDiscriminator string
	for _, d := range discriminators {
		if _, ok := generatedQuery[d]; ok {
			generatedDiscriminator = d
		}
		if _, ok := referenceQuery[d]; ok {
			referenceDiscriminator = d
		}
	}

	if generatedDiscriminator == referenceDiscriminator && generatedDiscriminator != "" {
		similarity = (similarity + 1.0) / 2.0 // Boost score for matching discriminator
	}

	pass := similarity >= 0.5
	reason := "structural similarity"
	if generatedDiscriminator != referenceDiscriminator {
		reason = "discriminator mismatch: generated " + generatedDiscriminator + " vs expected " + referenceDiscriminator
		pass = false
	}

	return &eval.EvalResult{
		Pass:   pass,
		Score:  similarity,
		Reason: reason,
	}, nil
}

func getTopLevelKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func intersection(a, b []string) []string {
	set := make(map[string]bool)
	for _, v := range a {
		set[v] = true
	}
	var result []string
	for _, v := range b {
		if set[v] {
			result = append(result, v)
		}
	}
	return result
}

// QuerySemanticEvaluator uses LLM-as-judge to evaluate semantic equivalence.
type QuerySemanticEvaluator struct {
	g         any // *genkit.Genkit
	modelName string
	name      string
}

func NewQuerySemanticEvaluator(g any, modelName, name string) *QuerySemanticEvaluator {
	return &QuerySemanticEvaluator{
		g:         g,
		modelName: modelName,
		name:      name,
	}
}

func (e *QuerySemanticEvaluator) Name() string {
	return e.name
}

func (e *QuerySemanticEvaluator) SupportsStreaming() bool {
	return false
}

func (e *QuerySemanticEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	outputMap, ok := input.Output.(map[string]any)
	if !ok {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "output is not a map",
		}, nil
	}

	generatedQuery := outputMap["query"]
	intent := outputMap["intent"]
	referenceQuery := input.Reference

	// Compare queries directly first
	generatedJSON, _ := json.Marshal(generatedQuery)
	referenceJSON, _ := json.Marshal(referenceQuery)

	if reflect.DeepEqual(generatedQuery, referenceQuery) {
		return &eval.EvalResult{
			Pass:   true,
			Score:  1.0,
			Reason: "exact match",
		}, nil
	}

	// For now, use structural comparison as fallback
	// TODO: Implement actual LLM-as-judge when query builder agent is complete
	generatedMap, ok1 := generatedQuery.(map[string]any)
	referenceMap, ok2 := referenceQuery.(map[string]any)

	if !ok1 || !ok2 {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0,
			Reason: "queries are not comparable maps",
		}, nil
	}

	// Check if they would produce similar results
	// This is a simplified heuristic - real evaluation would use LLM
	similarity := calculateQuerySimilarity(generatedMap, referenceMap)

	return &eval.EvalResult{
		Pass:   similarity >= 0.6,
		Score:  similarity,
		Reason: "semantic similarity based on structure (generated: " + string(generatedJSON) + ", reference: " + string(referenceJSON) + ", intent: " + intent.(string) + ")",
	}, nil
}

func calculateQuerySimilarity(a, b map[string]any) float64 {
	// Simple structural similarity
	aKeys := getTopLevelKeys(a)
	bKeys := getTopLevelKeys(b)

	common := intersection(aKeys, bKeys)
	total := len(aKeys) + len(bKeys) - len(common)
	if total == 0 {
		return 1.0
	}

	return float64(len(common)) / float64(total)
}
