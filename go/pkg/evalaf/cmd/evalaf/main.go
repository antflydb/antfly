package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "0.1.0"

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "evalaf",
	Short: "Evalaf - Embeddable LLM/RAG/Agent Evaluation Framework",
	Long: `Evalaf is a comprehensive evaluation framework for LLM applications.

It supports evaluating:
- RAG systems (faithfulness, relevance, retrieval quality, citations)
- Agents (classification, tool selection, reasoning)
- General LLM outputs (correctness, safety, quality)

Use evalaf to run evaluations, manage datasets, and monitor LLM quality.`,
	Version: version,
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(datasetsCmd)
	rootCmd.AddCommand(metricsCmd)
}
