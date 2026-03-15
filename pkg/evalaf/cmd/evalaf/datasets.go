package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/spf13/cobra"
)

var datasetsCmd = &cobra.Command{
	Use:   "datasets",
	Short: "Manage evaluation datasets",
	Long:  "Commands for managing and validating evaluation datasets.",
}

var listDatasetsCmd = &cobra.Command{
	Use:   "list",
	Short: "List datasets from config",
	RunE:  listDatasets,
}

var validateDatasetCmd = &cobra.Command{
	Use:   "validate <dataset-file>",
	Short: "Validate a dataset file",
	Args:  cobra.ExactArgs(1),
	RunE:  validateDataset,
}

var inspectDatasetCmd = &cobra.Command{
	Use:   "inspect <dataset-file>",
	Short: "Inspect a dataset file",
	Args:  cobra.ExactArgs(1),
	RunE:  inspectDataset,
}

func init() {
	datasetsCmd.AddCommand(listDatasetsCmd)
	datasetsCmd.AddCommand(validateDatasetCmd)
	datasetsCmd.AddCommand(inspectDatasetCmd)

	listDatasetsCmd.Flags().StringVarP(&configPath, "config", "c", "evalaf.yaml", "Path to configuration file")
}

func listDatasets(cmd *cobra.Command, args []string) error {
	config, err := eval.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if len(config.Datasets) == 0 {
		fmt.Println("No datasets configured")
		return nil
	}

	fmt.Printf("Datasets (%d):\n\n", len(config.Datasets))

	for i, datasetConfig := range config.Datasets {
		fmt.Printf("%d. %s\n", i+1, datasetConfig.Name)
		fmt.Printf("   Path: %s\n", datasetConfig.Path)
		fmt.Printf("   Type: %s\n", datasetConfig.Type)

		// Try to load and show size
		dataset, err := eval.LoadDataset(datasetConfig)
		if err != nil {
			fmt.Printf("   Error: %v\n", err)
		} else {
			fmt.Printf("   Examples: %d\n", dataset.Len())
		}
		fmt.Println()
	}

	return nil
}

func validateDataset(cmd *cobra.Command, args []string) error {
	datasetPath := args[0]

	dataset, err := eval.NewJSONDataset("validation", datasetPath)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	ctx := context.Background()
	examples, err := dataset.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load examples: %w", err)
	}

	fmt.Printf("✓ Dataset is valid\n")
	fmt.Printf("  Path: %s\n", datasetPath)
	fmt.Printf("  Examples: %d\n", len(examples))

	// Check for common issues
	issues := []string{}

	hasInput := 0
	hasReference := 0
	hasContext := 0
	hasMetadata := 0

	for i, example := range examples {
		if example.Input != nil {
			hasInput++
		}
		if example.Reference != nil {
			hasReference++
		}
		if len(example.Context) > 0 {
			hasContext++
		}
		if len(example.Metadata) > 0 {
			hasMetadata++
		}

		// Check for empty input
		if example.Input == nil || example.Input == "" {
			issues = append(issues, fmt.Sprintf("Example %d has empty input", i))
		}
	}

	fmt.Printf("\nDataset statistics:\n")
	fmt.Printf("  Examples with input: %d/%d (%.1f%%)\n", hasInput, len(examples), float64(hasInput)/float64(len(examples))*100)
	fmt.Printf("  Examples with reference: %d/%d (%.1f%%)\n", hasReference, len(examples), float64(hasReference)/float64(len(examples))*100)
	fmt.Printf("  Examples with context: %d/%d (%.1f%%)\n", hasContext, len(examples), float64(hasContext)/float64(len(examples))*100)
	fmt.Printf("  Examples with metadata: %d/%d (%.1f%%)\n", hasMetadata, len(examples), float64(hasMetadata)/float64(len(examples))*100)

	if len(issues) > 0 {
		fmt.Printf("\n⚠ Issues found:\n")
		for _, issue := range issues {
			fmt.Printf("  - %s\n", issue)
		}
	}

	return nil
}

func inspectDataset(cmd *cobra.Command, args []string) error {
	datasetPath := args[0]

	dataset, err := eval.NewJSONDataset("inspection", datasetPath)
	if err != nil {
		return fmt.Errorf("inspection failed: %w", err)
	}

	ctx := context.Background()
	examples, err := dataset.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load examples: %w", err)
	}

	fmt.Printf("Dataset: %s\n", datasetPath)
	fmt.Printf("Examples: %d\n\n", len(examples))

	// Show first few examples
	showCount := min(len(examples), 3)

	for i := range showCount {
		fmt.Printf("Example %d:\n", i+1)

		exampleJSON, err := json.MarshalIndent(examples[i], "  ", "  ")
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			fmt.Printf("  %s\n", string(exampleJSON))
		}
		fmt.Println()
	}

	if len(examples) > showCount {
		fmt.Printf("... and %d more examples\n", len(examples)-showCount)
	}

	return nil
}
