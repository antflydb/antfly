package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/antflydb/antfly/pkg/evalaf/genkitplugin"
	"github.com/antflydb/antfly/pkg/evalaf/rag"
	"github.com/firebase/genkit/go/genkit"
)

func main() {
	ctx := context.Background()

	fmt.Println("Genkit Plugin Integration - WORKING EXAMPLE!")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println()

	// Example 1: Register evalaf evaluators with Genkit
	fmt.Println("Example 1: Register Evalaf Evaluators with Genkit")
	fmt.Println(strings.Repeat("=", 50))
	runGenkitIntegrationExample(ctx)

	fmt.Println("\n" + strings.Repeat("=", 80) + "\n")

	// Example 2: Standalone evalaf (still works)
	fmt.Println("Example 2: Standalone Evalaf Usage")
	fmt.Println(strings.Repeat("=", 50))
	runStandaloneExample(ctx)

	fmt.Println("\n" + strings.Repeat("=", 80) + "\n")

	// Example 3: Format conversion
	fmt.Println("Example 3: Format Conversion Utilities")
	fmt.Println(strings.Repeat("=", 50))
	demonstrateFormatConversion()
}

func runGenkitIntegrationExample(ctx context.Context) {
	// Initialize Genkit
	g := genkit.Init(ctx)

	// Create plugin with evalaf metrics
	plugin := &genkitplugin.EvalafPlugin{
		Metrics: []genkitplugin.MetricConfig{
			{
				MetricType: genkitplugin.MetricCitation,
				Name:       "citation_check",
			},
			{
				MetricType: genkitplugin.MetricNDCG,
				Name:       "ndcg@10",
				Config:     map[string]any{"k": 10},
			},
			{
				MetricType: genkitplugin.MetricClassification,
				Name:       "classification",
				Config:     map[string]any{"classes": []any{"question", "search"}},
			},
		},
	}

	// Register all evalaf evaluators with Genkit
	err := plugin.RegisterEvaluators(ctx, g)
	if err != nil {
		fmt.Printf("Failed to register evaluators: %v\n", err)
		return
	}

	fmt.Println("✅ Successfully registered 3 evalaf evaluators with Genkit:")
	fmt.Println("   - citation_check")
	fmt.Println("   - ndcg@10")
	fmt.Println("   - classification")

	// Verify evaluators are registered
	citationEval := genkit.LookupEvaluator(g, "citation_check")
	if citationEval != nil {
		fmt.Println("✅ citation_check evaluator is accessible via genkit.LookupEvaluator")
	}

	ndcgEval := genkit.LookupEvaluator(g, "ndcg@10")
	if ndcgEval != nil {
		fmt.Println("✅ ndcg@10 evaluator is accessible via genkit.LookupEvaluator")
	}

	classificationEval := genkit.LookupEvaluator(g, "classification")
	if classificationEval != nil {
		fmt.Println("✅ classification evaluator is accessible via genkit.LookupEvaluator")
	}

	fmt.Println("\nNow you can use these evaluators with:")
	fmt.Println("  - Genkit Dev UI at http://localhost:4000")
	fmt.Println("  - Genkit CLI: genkit eval:flow <flow_name> --evaluators=citation_check,ndcg@10")
	fmt.Println("  - genkit.Evaluate() API in your code")
}

func runStandaloneExample(ctx context.Context) {
	// Create dataset
	dataset := eval.NewJSONDatasetFromExamples("rag_test", []eval.Example{
		{
			Input: "What is machine learning?",
			Context: []any{
				"Machine learning is a subset of AI.",
				"It involves training models on data.",
			},
		},
	})

	// Create evaluators
	evaluators := []eval.Evaluator{
		rag.NewCitationEvaluator("citations"),
		rag.NewCitationCoverageEvaluator("coverage"),
	}

	// Create target function
	target := func(ctx context.Context, example eval.Example) (any, error) {
		query := example.Input.(string)
		if strings.Contains(query, "machine learning") {
			return "Machine learning is a subset of AI [resource_id 0]. It trains models on data [resource_id 1].", nil
		}
		return "Response with [resource_id 0] citation.", nil
	}

	// Run evaluation
	config := *eval.DefaultConfig()
	config.Execution.Parallel = false

	runner := eval.NewRunner(config, evaluators)
	report, err := runner.RunWithTarget(ctx, dataset, target)
	if err != nil {
		fmt.Printf("Evaluation failed: %v\n", err)
		return
	}

	// Print results
	fmt.Println("Evaluation Results:")
	fmt.Printf("  Total: %d examples\n", report.Summary.TotalExamples)
	fmt.Printf("  Passed: %d\n", report.Summary.PassedExamples)
	fmt.Printf("  Failed: %d\n", report.Summary.FailedExamples)
	fmt.Printf("  Pass Rate: %.1f%%\n", report.Summary.PassRate*100)

	fmt.Println("\nBoth approaches work - choose based on your use case:")
	fmt.Println("  - Genkit integration: For Dev UI and CLI tools")
	fmt.Println("  - Standalone evalaf: For CI/CD and programmatic usage")
}

func demonstrateFormatConversion() {
	// Show format conversion between Genkit and evalaf
	fmt.Println("Format conversion allows interop between Genkit and evalaf:")

	evalExamples := []eval.Example{
		{
			Input:     "What is ML?",
			Reference: "Machine learning definition",
			Context:   []any{"doc1", "doc2"},
		},
	}

	// Convert to Genkit format
	genkitExamples := genkitplugin.ConvertEvalafDataset(evalExamples)
	fmt.Printf("✅ Converted %d evalaf examples to Genkit format\n", len(genkitExamples))

	// Convert back to evalaf format
	backToEvalaf := genkitplugin.ConvertGenkitDataset(genkitExamples)
	fmt.Printf("✅ Converted %d Genkit examples to evalaf format\n", len(backToEvalaf))

	fmt.Println("\nThis enables:")
	fmt.Println("  - Using Genkit Dev UI to create datasets")
	fmt.Println("  - Exporting evalaf datasets to Genkit")
	fmt.Println("  - Sharing evaluation results between systems")
}
