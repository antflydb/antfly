package eval

import (
	"context"
	"strings"
	"testing"
)

func TestJSONDataset(t *testing.T) {
	examples := []Example{
		{
			Input:     "test input",
			Reference: "test reference",
		},
	}

	dataset := NewJSONDatasetFromExamples("test", examples)

	if dataset.Name() != "test" {
		t.Errorf("expected name 'test', got %s", dataset.Name())
	}

	if dataset.Len() != 1 {
		t.Errorf("expected 1 example, got %d", dataset.Len())
	}

	loaded, err := dataset.Load(t.Context())
	if err != nil {
		t.Fatalf("failed to load dataset: %v", err)
	}

	if len(loaded) != 1 {
		t.Errorf("expected 1 example, got %d", len(loaded))
	}

	if loaded[0].Input != "test input" {
		t.Errorf("expected 'test input', got %v", loaded[0].Input)
	}
}

func TestExactMatchEvaluator(t *testing.T) {
	evaluator := NewExactMatchEvaluator("exact")

	tests := []struct {
		name      string
		output    any
		reference any
		wantPass  bool
		wantScore float64
	}{
		{
			name:      "exact match",
			output:    "hello",
			reference: "hello",
			wantPass:  true,
			wantScore: 1.0,
		},
		{
			name:      "no match",
			output:    "hello",
			reference: "world",
			wantPass:  false,
			wantScore: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(t.Context(), EvalInput{
				Output:    tt.output,
				Reference: tt.reference,
			})

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Pass != tt.wantPass {
				t.Errorf("expected pass=%v, got %v", tt.wantPass, result.Pass)
			}

			if result.Score != tt.wantScore {
				t.Errorf("expected score=%v, got %v", tt.wantScore, result.Score)
			}
		})
	}
}

func TestRegexEvaluator(t *testing.T) {
	evaluator, err := NewRegexEvaluator("regex", EvaluatorConfig{
		Pattern: `\d+`,
	})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}

	tests := []struct {
		name     string
		output   string
		wantPass bool
	}{
		{
			name:     "contains number",
			output:   "hello 123",
			wantPass: true,
		},
		{
			name:     "no number",
			output:   "hello world",
			wantPass: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(t.Context(), EvalInput{
				Output: tt.output,
			})

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Pass != tt.wantPass {
				t.Errorf("expected pass=%v, got %v (reason: %s)", tt.wantPass, result.Pass, result.Reason)
			}
		})
	}
}

func TestContainsEvaluator(t *testing.T) {
	evaluator := NewContainsEvaluator("contains", false)

	tests := []struct {
		name      string
		output    string
		reference string
		wantPass  bool
	}{
		{
			name:      "contains substring",
			output:    "hello world",
			reference: "world",
			wantPass:  true,
		},
		{
			name:      "does not contain",
			output:    "hello world",
			reference: "goodbye",
			wantPass:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(t.Context(), EvalInput{
				Output:    tt.output,
				Reference: tt.reference,
			})

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Pass != tt.wantPass {
				t.Errorf("expected pass=%v, got %v", tt.wantPass, result.Pass)
			}
		})
	}
}

func TestFuzzyMatchEvaluator(t *testing.T) {
	evaluator := NewFuzzyMatchEvaluator("fuzzy", 0.8)

	tests := []struct {
		name      string
		output    string
		reference string
		wantPass  bool
	}{
		{
			name:      "exact match",
			output:    "hello",
			reference: "hello",
			wantPass:  true,
		},
		{
			name:      "close match",
			output:    "hello",
			reference: "helo",
			wantPass:  true,
		},
		{
			name:      "very different",
			output:    "hello",
			reference: "goodbye",
			wantPass:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(t.Context(), EvalInput{
				Output:    tt.output,
				Reference: tt.reference,
			})

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Pass != tt.wantPass {
				t.Errorf("expected pass=%v, got %v (score: %.2f, reason: %s)",
					tt.wantPass, result.Pass, result.Score, result.Reason)
			}
		})
	}
}

func TestRunner(t *testing.T) {
	dataset := NewJSONDatasetFromExamples("test", []Example{
		{
			Input:     "test",
			Reference: "test",
			Metadata: map[string]any{
				"output": "test",
			},
		},
	})

	evaluator := NewExactMatchEvaluator("exact")

	config := *DefaultConfig()
	runner := NewRunner(config, []Evaluator{evaluator})

	report, err := runner.Run(t.Context(), dataset)

	if err != nil {
		t.Fatalf("evaluation failed: %v", err)
	}

	if report.Summary.TotalExamples != 1 {
		t.Errorf("expected 1 example, got %d", report.Summary.TotalExamples)
	}

	if report.Summary.PassedExamples != 1 {
		t.Errorf("expected 1 passed, got %d", report.Summary.PassedExamples)
	}

	if report.Summary.PassRate != 1.0 {
		t.Errorf("expected pass rate 1.0, got %.2f", report.Summary.PassRate)
	}
}

func TestRunnerWithTarget(t *testing.T) {
	dataset := NewJSONDatasetFromExamples("test", []Example{
		{
			Input:     "uppercase",
			Reference: "UPPERCASE",
		},
	})

	evaluator := NewExactMatchEvaluator("exact")

	config := *DefaultConfig()
	runner := NewRunner(config, []Evaluator{evaluator})

	target := func(ctx context.Context, example Example) (any, error) {
		// Simulate a system that uppercases the input
		if input, ok := example.Input.(string); ok {
			// Simple uppercase implementation
			var result strings.Builder
			for _, r := range input {
				if r >= 'a' && r <= 'z' {
					result.WriteString(string(r - 32))
				} else {
					result.WriteString(string(r))
				}
			}
			return result.String(), nil
		}
		return example.Input, nil
	}

	report, err := runner.RunWithTarget(t.Context(), dataset, target)

	if err != nil {
		t.Fatalf("evaluation failed: %v", err)
	}

	if report.Summary.TotalExamples != 1 {
		t.Errorf("expected 1 example, got %d", report.Summary.TotalExamples)
	}

	if report.Summary.PassedExamples != 1 {
		t.Errorf("expected 1 passed, got %d", report.Summary.PassedExamples)
	}
}

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		s1   string
		s2   string
		want int
	}{
		{"", "", 0},
		{"hello", "hello", 0},
		{"hello", "helo", 1},
		{"hello", "world", 4},
		{"kitten", "sitting", 3},
	}

	for _, tt := range tests {
		t.Run(tt.s1+"_"+tt.s2, func(t *testing.T) {
			got := LevenshteinDistance(tt.s1, tt.s2)
			if got != tt.want {
				t.Errorf("LevenshteinDistance(%q, %q) = %d, want %d",
					tt.s1, tt.s2, got, tt.want)
			}
		})
	}
}
