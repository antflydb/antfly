package main

import (
	"context"
	"fmt"
	"log"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
)

func main() {
	ctx := context.Background()

	// Create a simple dataset with examples
	dataset := eval.NewJSONDatasetFromExamples("simple_qa", []eval.Example{
		{
			Input:     "What is 2+2?",
			Reference: "4",
			Metadata: map[string]any{
				"domain": "math",
			},
		},
		{
			Input:     "What is the capital of France?",
			Reference: "Paris",
			Metadata: map[string]any{
				"domain": "geography",
			},
		},
		{
			Input:     "What color is the sky?",
			Reference: "(?i)blue",
			Metadata: map[string]any{
				"domain": "general",
			},
		},
	})

	// Create evaluators
	regexEval, err := eval.NewRegexEvaluator("regex_match", eval.EvaluatorConfig{Pattern: ".*"})
	if err != nil {
		log.Fatalf("Failed to create regex evaluator: %v", err)
	}

	evaluators := []eval.Evaluator{
		eval.NewExactMatchEvaluator("exact_match"),
		regexEval,
		eval.NewContainsEvaluator("contains", false),
		eval.NewFuzzyMatchEvaluator("fuzzy_match", 0.8),
	}

	// Create a simple target function that simulates an LLM
	target := func(ctx context.Context, example eval.Example) (any, error) {
		// In real usage, this would call your LLM, RAG system, agent, etc.
		// For this demo, we'll just return some simple answers

		input, ok := example.Input.(string)
		if !ok {
			return nil, fmt.Errorf("input is not a string")
		}

		switch input {
		case "What is 2+2?":
			return "4", nil
		case "What is the capital of France?":
			return "Paris", nil
		case "What color is the sky?":
			return "Blue", nil
		default:
			return "I don't know", nil
		}
	}

	// Create runner with default config
	config := *eval.DefaultConfig()
	config.Execution.Parallel = true
	config.Execution.MaxConcurrency = 5

	runner := eval.NewRunner(config, evaluators)

	// Run evaluation
	fmt.Println("Running evaluation...")
	report, err := runner.RunWithTarget(ctx, dataset, target)
	if err != nil {
		log.Fatalf("Evaluation failed: %v", err)
	}

	// Print results to console
	fmt.Println()
	report.Print()

	// Save results to JSON
	if err := report.SaveToFile("simple_results.json", "json", true); err != nil {
		log.Printf("Failed to save JSON report: %v", err)
	} else {
		fmt.Println("\nResults saved to simple_results.json")
	}

	// Save results to Markdown
	if err := report.SaveToFile("simple_results.md", "markdown", false); err != nil {
		log.Printf("Failed to save Markdown report: %v", err)
	} else {
		fmt.Println("Results saved to simple_results.md")
	}
}
