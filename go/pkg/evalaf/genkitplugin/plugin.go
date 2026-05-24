package genkitplugin

import (
	"context"
	"fmt"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
)

// EvalafPlugin integrates evalaf evaluators with Genkit's evaluation framework
type EvalafPlugin struct {
	Metrics []MetricConfig
}

// MetricConfig defines configuration for a single metric/evaluator
type MetricConfig struct {
	MetricType MetricType
	Name       string
	Config     map[string]any
}

// MetricType identifies the type of evaluator to create
type MetricType string

const (
	// Built-in evaluators from eval package
	MetricExactMatch MetricType = "exact_match"
	MetricRegex      MetricType = "regex"
	MetricContains   MetricType = "contains"
	MetricFuzzyMatch MetricType = "fuzzy_match"

	// RAG evaluators from rag package
	MetricCitation         MetricType = "citation"
	MetricCitationCoverage MetricType = "citation_coverage"
	MetricNDCG             MetricType = "ndcg"
	MetricMRR              MetricType = "mrr"
	MetricPrecision        MetricType = "precision"
	MetricRecall           MetricType = "recall"
	MetricMAP              MetricType = "map"

	// Agent evaluators from agent package
	MetricClassification MetricType = "classification"
	MetricConfidence     MetricType = "confidence"
	MetricToolSelection  MetricType = "tool_selection"
	MetricToolSequence   MetricType = "tool_sequence"

	// LLM-as-judge evaluators (require Genkit instance)
	MetricFaithfulness MetricType = "faithfulness"
	MetricRelevance    MetricType = "relevance"
	MetricCorrectness  MetricType = "correctness"
	MetricCoherence    MetricType = "coherence"
	MetricSafety       MetricType = "safety"
	MetricCompleteness MetricType = "completeness"
	MetricHelpfulness  MetricType = "helpfulness"
)

// RegisterEvaluators registers all configured evalaf evaluators with Genkit
// This should be called after Genkit initialization
func (p *EvalafPlugin) RegisterEvaluators(ctx context.Context, g *genkit.Genkit) error {
	if len(p.Metrics) == 0 {
		return fmt.Errorf("no metrics configured in EvalafPlugin")
	}

	for _, metricConfig := range p.Metrics {
		evaluatorFunc, err := p.createEvaluatorFunc(ctx, g, metricConfig)
		if err != nil {
			return fmt.Errorf("failed to create evaluator %s: %w", metricConfig.Name, err)
		}

		// Register evaluator with Genkit using DefineEvaluator
		genkit.DefineEvaluator(g, metricConfig.Name, nil, evaluatorFunc)
	}

	return nil
}

// createEvaluatorFunc creates an evalaf evaluator and wraps it for Genkit
func (p *EvalafPlugin) createEvaluatorFunc(ctx context.Context, g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	switch config.MetricType {
	// Built-in evaluators
	case MetricExactMatch:
		return createExactMatchEvaluator(config)
	case MetricRegex:
		return createRegexEvaluator(config)
	case MetricContains:
		return createContainsEvaluator(config)
	case MetricFuzzyMatch:
		return createFuzzyMatchEvaluator(config)

	// RAG evaluators
	case MetricCitation:
		return createCitationEvaluator(config)
	case MetricCitationCoverage:
		return createCitationCoverageEvaluator(config)
	case MetricNDCG:
		return createNDCGEvaluator(config)
	case MetricMRR:
		return createMRREvaluator(config)
	case MetricPrecision:
		return createPrecisionEvaluator(config)
	case MetricRecall:
		return createRecallEvaluator(config)
	case MetricMAP:
		return createMAPEvaluator(config)

	// Agent evaluators
	case MetricClassification:
		return createClassificationEvaluator(config)
	case MetricConfidence:
		return createConfidenceEvaluator(config)
	case MetricToolSelection:
		return createToolSelectionEvaluator(config)
	case MetricToolSequence:
		return createToolSequenceEvaluator(config)

	// LLM-as-judge evaluators
	case MetricFaithfulness:
		return createFaithfulnessEvaluator(g, config)
	case MetricRelevance:
		return createRelevanceEvaluator(g, config)
	case MetricCorrectness:
		return createCorrectnessEvaluator(g, config)
	case MetricCoherence:
		return createCoherenceEvaluator(g, config)
	case MetricSafety:
		return createSafetyEvaluator(g, config)
	case MetricCompleteness:
		return createCompletenessEvaluator(g, config)
	case MetricHelpfulness:
		return createHelpfulnessEvaluator(g, config)

	default:
		return nil, fmt.Errorf("unknown metric type: %s", config.MetricType)
	}
}

// Helper functions for creating evaluators from each package
// These are defined in wrappers.go
