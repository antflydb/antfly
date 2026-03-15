package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "List available evaluation metrics",
	Long:  "Display information about available evaluation metrics and evaluators.",
}

var listMetricsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all available metrics",
	RunE:  listMetrics,
}

var showMetricCmd = &cobra.Command{
	Use:   "show <metric-name>",
	Short: "Show details about a specific metric",
	Args:  cobra.ExactArgs(1),
	RunE:  showMetric,
}

func init() {
	metricsCmd.AddCommand(listMetricsCmd)
	metricsCmd.AddCommand(showMetricCmd)
}

func listMetrics(cmd *cobra.Command, args []string) error {
	fmt.Println("Available Evaluation Metrics")
	fmt.Println("============================")
	fmt.Println()

	fmt.Println("📊 Built-in Evaluators (eval package)")
	fmt.Println("--------------------------------------")
	fmt.Println("  exact_match      - Exact string matching")
	fmt.Println("  regex            - Regular expression matching")
	fmt.Println("  contains         - Substring matching")
	fmt.Println("  fuzzy_match      - Levenshtein distance-based matching")
	fmt.Println()

	fmt.Println("🧠 LLM-as-Judge Evaluators (genkit package)")
	fmt.Println("-------------------------------------------")
	fmt.Println("  faithfulness     - Answer grounded in context")
	fmt.Println("  relevance        - Answer addresses question")
	fmt.Println("  correctness      - Factually correct answer")
	fmt.Println("  coherence        - Logically coherent answer")
	fmt.Println("  safety           - Safe and non-harmful content")
	fmt.Println("  completeness     - Comprehensive answer")
	fmt.Println("  helpfulness      - Useful and actionable")
	fmt.Println("  tone             - Appropriate tone")
	fmt.Println("  citation_quality - Accurate citations")
	fmt.Println()

	fmt.Println("📝 RAG Evaluators (rag package)")
	fmt.Println("-------------------------------")
	fmt.Println("  rag_citation     - Citation accuracy validation")
	fmt.Println("  rag_coverage     - Citation coverage check")
	fmt.Println("  ndcg             - Normalized Discounted Cumulative Gain")
	fmt.Println("  mrr              - Mean Reciprocal Rank")
	fmt.Println("  precision        - Precision@k")
	fmt.Println("  recall           - Recall@k")
	fmt.Println("  map              - Mean Average Precision")
	fmt.Println()

	fmt.Println("🤖 Agent Evaluators (agent package)")
	fmt.Println("------------------------------------")
	fmt.Println("  classification   - Query classification accuracy")
	fmt.Println("  confidence       - Confidence score validation")
	fmt.Println("  tool_selection   - Tool selection correctness")
	fmt.Println("  tool_sequence    - Tool call sequence validation")
	fmt.Println()

	fmt.Println("Use 'evalaf metrics show <metric-name>' for details")

	return nil
}

func showMetric(cmd *cobra.Command, args []string) error {
	metricName := args[0]

	metrics := map[string]struct {
		description string
		category    string
		inputReq    string
		config      string
	}{
		"exact_match": {
			description: "Checks if output exactly matches the reference",
			category:    "Built-in",
			inputReq:    "Reference value required",
			config: `evaluators:
  exact_match:
    type: exact_match`,
		},
		"regex": {
			description: "Checks if output matches a regex pattern",
			category:    "Built-in",
			inputReq:    "Pattern in config or reference",
			config: `evaluators:
  regex_check:
    type: regex
    pattern: "(?i)expected_pattern"`,
		},
		"contains": {
			description: "Checks if output contains a substring",
			category:    "Built-in",
			inputReq:    "Reference substring required",
			config: `evaluators:
  contains_check:
    type: contains
    custom:
      case_insensitive: true`,
		},
		"fuzzy_match": {
			description: "Checks similarity using Levenshtein distance",
			category:    "Built-in",
			inputReq:    "Reference value required",
			config: `evaluators:
  fuzzy:
    type: fuzzy_match
    custom:
      threshold: 0.8  # 80% similarity`,
		},
		"faithfulness": {
			description: "LLM evaluates if answer is grounded in context",
			category:    "LLM-as-Judge",
			inputReq:    "Context documents required",
			config: `evaluators:
  faithfulness:
    type: genkit_llm_judge
    model: ollama/mistral
    temperature: 0.0
    prompt: "[faithfulness prompt]"`,
		},
		"relevance": {
			description: "LLM evaluates if answer addresses the question",
			category:    "LLM-as-Judge",
			inputReq:    "Input query required",
			config: `evaluators:
  relevance:
    type: genkit_llm_judge
    model: ollama/mistral
    temperature: 0.0
    prompt: "[relevance prompt]"`,
		},
		"rag_citation": {
			description: "Validates citation accuracy (e.g., [resource_id 0])",
			category:    "RAG",
			inputReq:    "Context documents required",
			config: `evaluators:
  citations:
    type: rag_citation
    pattern: '\[resource_id\s+([^\]]+)\]'`,
		},
		"ndcg": {
			description: "Normalized Discounted Cumulative Gain for ranking",
			category:    "RAG Retrieval",
			inputReq:    "Metadata: retrieved_ids, relevant_ids",
			config: `evaluators:
  ndcg@10:
    type: rag_retrieval_metric
    metric: ndcg
    k: 10`,
		},
		"classification": {
			description: "Checks classification accuracy",
			category:    "Agent",
			inputReq:    "Reference classification required",
			config: `evaluators:
  classification:
    type: agent_classification
    classes: ["question", "search"]`,
		},
		"tool_selection": {
			description: "Checks if correct tools were selected",
			category:    "Agent",
			inputReq:    "Reference tool(s) required",
			config: `evaluators:
  tool_selection:
    type: agent_tool_selection`,
		},
	}

	info, ok := metrics[metricName]
	if !ok {
		return fmt.Errorf("unknown metric: %s\n\nUse 'evalaf metrics list' to see available metrics", metricName)
	}

	fmt.Printf("Metric: %s\n", metricName)
	fmt.Printf("====================%s\n\n", "====================")
	fmt.Printf("Category: %s\n", info.category)
	fmt.Printf("Description: %s\n", info.description)
	fmt.Printf("Input Requirements: %s\n\n", info.inputReq)
	fmt.Printf("Configuration Example:\n")
	fmt.Printf("----------------------\n")
	fmt.Printf("%s\n", info.config)

	return nil
}
