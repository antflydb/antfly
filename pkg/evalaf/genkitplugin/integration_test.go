package genkitplugin

import (
	"errors"
	"testing"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
)

func TestPluginRegistration(t *testing.T) {
	// Initialize Genkit
	g := genkit.Init(t.Context())

	// Create plugin with metrics
	plugin := &EvalafPlugin{
		Metrics: []MetricConfig{
			{
				MetricType: MetricExactMatch,
				Name:       "exact_match",
			},
			{
				MetricType: MetricCitation,
				Name:       "citation",
			},
		},
	}

	// Register evaluators
	err := plugin.RegisterEvaluators(t.Context(), g)
	if err != nil {
		t.Fatalf("Failed to register evaluators: %v", err)
	}

	// Verify evaluators are registered
	exactMatchEval := genkit.LookupEvaluator(g, "exact_match")
	if exactMatchEval == nil {
		t.Error("exact_match evaluator not registered")
	}

	citationEval := genkit.LookupEvaluator(g, "citation")
	if citationEval == nil {
		t.Error("citation evaluator not registered")
	}
}

func TestToEvalafInput(t *testing.T) {
	example := ai.Example{
		TestCaseId: "test_001",
		Input:      "What is ML?",
		Output:     "Machine learning is...",
		Context:    []any{"doc1", "doc2"},
		Reference:  "expected answer",
		TraceIds:   []string{"trace1"},
	}

	result := ToEvalafInput(example)

	if result.Input != "What is ML?" {
		t.Errorf("Input mismatch: got %v, want 'What is ML?'", result.Input)
	}
	if result.Output != "Machine learning is..." {
		t.Errorf("Output mismatch: got %v, want 'Machine learning is...'", result.Output)
	}
	if result.Reference != "expected answer" {
		t.Errorf("Reference mismatch: got %v, want 'expected answer'", result.Reference)
	}
	if len(result.Context) != 2 {
		t.Errorf("Context length mismatch: got %d, want 2", len(result.Context))
	}
	if result.Metadata["test_case_id"] != "test_001" {
		t.Errorf("TestCaseId mismatch: got %v, want 'test_001'", result.Metadata["test_case_id"])
	}
}

func TestFromEvalafResult(t *testing.T) {
	evalResult := &eval.EvalResult{
		Pass:   true,
		Score:  0.95,
		Reason: "All checks passed",
	}

	score := FromEvalafResult(evalResult, "test_evaluator")

	if score.Id != "test_evaluator" {
		t.Errorf("Id mismatch: got %s, want 'test_evaluator'", score.Id)
	}
	if score.Score != 0.95 {
		t.Errorf("Score mismatch: got %v, want 0.95", score.Score)
	}
	if score.Status != "PASS" {
		t.Errorf("Status mismatch: got %s, want 'PASS'", score.Status)
	}
	if score.Details["reason"] != "All checks passed" {
		t.Errorf("Reason mismatch: got %v, want 'All checks passed'", score.Details["reason"])
	}
}

func TestFromEvalafResultWithErr(t *testing.T) {
	evalResult := &eval.EvalResult{
		Pass:   false,
		Score:  0.3,
		Reason: "Citation missing",
		Error:  errors.New("validation failed"),
	}

	score := FromEvalafResult(evalResult, "test_evaluator")

	if score.Status != "FAIL" {
		t.Errorf("Status mismatch: got %s, want 'FAIL'", score.Status)
	}
	if score.Error != "validation failed" {
		t.Errorf("Error mismatch: got %s, want 'validation failed'", score.Error)
	}
}

func TestConvertDatasets(t *testing.T) {
	// Test Genkit -> evalaf
	genkitExamples := []ai.Example{
		{
			TestCaseId: "test_001",
			Input:      "query1",
			Reference:  "ref1",
			Context:    []any{"doc1"},
		},
	}

	evalExamples := ConvertGenkitDataset(genkitExamples)

	if len(evalExamples) != 1 {
		t.Fatalf("Expected 1 example, got %d", len(evalExamples))
	}
	if evalExamples[0].Input != "query1" {
		t.Errorf("Input mismatch: got %v, want 'query1'", evalExamples[0].Input)
	}

	// Test evalaf -> Genkit
	converted := ConvertEvalafDataset(evalExamples)

	if len(converted) != 1 {
		t.Fatalf("Expected 1 example, got %d", len(converted))
	}
	if converted[0].TestCaseId != "test_001" {
		t.Errorf("TestCaseId mismatch: got %s, want 'test_001'", converted[0].TestCaseId)
	}
}

func TestWrapEvaluator(t *testing.T) {
	// Create a simple evalaf evaluator
	exactMatch := eval.NewExactMatchEvaluator("test_exact")

	// Wrap it for Genkit
	wrapped := wrapEvaluator("test_exact", exactMatch)

	// Test with matching output
	req := &ai.EvaluatorCallbackRequest{
		Input: ai.Example{
			TestCaseId: "test_001",
			Input:      "query",
			Output:     "expected",
			Reference:  "expected",
		},
	}

	result, err := wrapped(t.Context(), req)
	if err != nil {
		t.Fatalf("Wrapped evaluator returned error: %v", err)
	}

	if len(result.Evaluation) != 1 {
		t.Fatalf("Expected 1 score, got %d", len(result.Evaluation))
	}

	score := result.Evaluation[0]
	if score.Status != "PASS" {
		t.Errorf("Expected PASS, got %s", score.Status)
	}
	if score.Id != "test_exact" {
		t.Errorf("Expected id 'test_exact', got %s", score.Id)
	}

	// Test with non-matching output
	req.Input.Output = "different"
	result, err = wrapped(t.Context(), req)
	if err != nil {
		t.Fatalf("Wrapped evaluator returned error: %v", err)
	}

	score = result.Evaluation[0]
	if score.Status != "FAIL" {
		t.Errorf("Expected FAIL, got %s", score.Status)
	}
}
