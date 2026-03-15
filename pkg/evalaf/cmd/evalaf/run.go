package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/spf13/cobra"
)

var (
	configPath      string
	datasetPath     string
	outputPath      string
	outputFormat    string
	evaluatorFilter string
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run evaluation with a configuration file",
	Long: `Run evaluation using a configuration file.

Examples:
  # Run with config file
  evalaf run --config evalaf.yaml

  # Run with specific dataset
  evalaf run --config evalaf.yaml --dataset testdata/my_dataset.json

  # Run with specific evaluators only
  evalaf run --config evalaf.yaml --evaluators faithfulness,relevance

  # Custom output
  evalaf run --config evalaf.yaml --output results.json --format json
`,
	RunE: runEvaluation,
}

func init() {
	runCmd.Flags().StringVarP(&configPath, "config", "c", "evalaf.yaml", "Path to configuration file")
	runCmd.Flags().StringVarP(&datasetPath, "dataset", "d", "", "Path to dataset file (overrides config)")
	runCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output file path (overrides config)")
	runCmd.Flags().StringVarP(&outputFormat, "format", "f", "", "Output format: json, yaml, markdown (overrides config)")
	runCmd.Flags().StringVarP(&evaluatorFilter, "evaluators", "e", "", "Comma-separated list of evaluators to run")
}

func runEvaluation(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Load configuration
	config, err := eval.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Override config with flags
	if outputPath != "" {
		config.Output.Path = outputPath
	}
	if outputFormat != "" {
		config.Output.Format = outputFormat
	}

	// Load datasets
	var datasets []eval.Dataset
	if datasetPath != "" {
		// Use dataset from flag
		dataset, err := eval.NewJSONDataset("cli_dataset", datasetPath)
		if err != nil {
			return fmt.Errorf("failed to load dataset: %w", err)
		}
		datasets = []eval.Dataset{dataset}
	} else {
		// Use datasets from config
		datasets, err = eval.LoadDatasets(config)
		if err != nil {
			return fmt.Errorf("failed to load datasets from config: %w", err)
		}
	}

	if len(datasets) == 0 {
		return fmt.Errorf("no datasets specified")
	}

	// Create evaluators
	evaluators, err := createEvaluatorsFromConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create evaluators: %w", err)
	}

	// Filter evaluators if specified
	if evaluatorFilter != "" {
		filterNames := strings.Split(evaluatorFilter, ",")
		evaluators = filterEvaluators(evaluators, filterNames)
	}

	if len(evaluators) == 0 {
		return fmt.Errorf("no evaluators specified")
	}

	fmt.Printf("Running evaluation with %d evaluator(s) on %d dataset(s)...\n", len(evaluators), len(datasets))

	// Run evaluation for each dataset
	for _, dataset := range datasets {
		fmt.Printf("\nDataset: %s (%d examples)\n", dataset.Name(), dataset.Len())

		runner := eval.NewRunner(*config, evaluators)

		// Run evaluation (assumes pre-collected outputs in dataset metadata)
		report, err := runner.Run(ctx, dataset)
		if err != nil {
			return fmt.Errorf("evaluation failed for dataset %s: %w", dataset.Name(), err)
		}

		// Print results to console
		fmt.Println()
		report.Print()

		// Save results
		if config.Output.Path != "" {
			if err := report.Save(); err != nil {
				return fmt.Errorf("failed to save report: %w", err)
			}
			fmt.Printf("\nResults saved to: %s\n", config.Output.Path)
		}
	}

	return nil
}

// createEvaluatorsFromConfig creates evaluators from the configuration.
func createEvaluatorsFromConfig(config *eval.Config) ([]eval.Evaluator, error) {
	evaluators := []eval.Evaluator{}

	for name, evalConfig := range config.Evaluators {
		var evaluator eval.Evaluator
		var err error

		switch evalConfig.Type {
		case "exact_match":
			evaluator = eval.NewExactMatchEvaluator(name)

		case "regex":
			evaluator, err = eval.NewRegexEvaluator(name, evalConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create regex evaluator %s: %w", name, err)
			}

		case "contains":
			caseInsensitive := false
			if evalConfig.Custom != nil {
				if ci, ok := evalConfig.Custom["case_insensitive"].(bool); ok {
					caseInsensitive = ci
				}
			}
			evaluator = eval.NewContainsEvaluator(name, caseInsensitive)

		case "fuzzy_match":
			threshold := 0.8
			if evalConfig.Custom != nil {
				if t, ok := evalConfig.Custom["threshold"].(float64); ok {
					threshold = t
				}
			}
			evaluator = eval.NewFuzzyMatchEvaluator(name, threshold)

		default:
			fmt.Fprintf(os.Stderr, "Warning: unknown evaluator type %s for %s (skipping)\n", evalConfig.Type, name)
			continue
		}

		if evaluator != nil {
			evaluators = append(evaluators, evaluator)
		}
	}

	return evaluators, nil
}

// filterEvaluators filters evaluators by name.
func filterEvaluators(evaluators []eval.Evaluator, names []string) []eval.Evaluator {
	nameSet := make(map[string]bool)
	for _, name := range names {
		nameSet[strings.TrimSpace(name)] = true
	}

	filtered := []eval.Evaluator{}
	for _, ev := range evaluators {
		if nameSet[ev.Name()] {
			filtered = append(filtered, ev)
		}
	}

	return filtered
}
