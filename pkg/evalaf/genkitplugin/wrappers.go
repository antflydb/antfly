package genkitplugin

import (
	"context"

	"github.com/antflydb/antfly/pkg/evalaf/agent"
	"github.com/antflydb/antfly/pkg/evalaf/eval"
	evalafgenkit "github.com/antflydb/antfly/pkg/evalaf/genkit"
	"github.com/antflydb/antfly/pkg/evalaf/rag"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
)

// wrapEvaluator wraps an evalaf evaluator for use with Genkit
// Returns a function matching ai.EvaluatorFunc signature
func wrapEvaluator(evaluatorName string, evalafEval eval.Evaluator) ai.EvaluatorFunc {
	return func(ctx context.Context, req *ai.EvaluatorCallbackRequest) (*ai.EvaluatorCallbackResponse, error) {
		// Convert Genkit input to evalaf format
		evalInput := ToEvalafInput(req.Input)

		// Run evalaf evaluator
		evalResult, err := evalafEval.Evaluate(ctx, evalInput)
		if err != nil {
			// Return error in the response, not as Go error
			score := ai.Score{
				Id:     evaluatorName,
				Status: "FAIL",
				Error:  err.Error(),
			}
			return &ai.EvaluationResult{
				TestCaseId: req.Input.TestCaseId,
				Evaluation: []ai.Score{score},
			}, nil
		}

		// Convert evalaf result to Genkit format
		score := FromEvalafResult(evalResult, evaluatorName)

		return &ai.EvaluationResult{
			TestCaseId: req.Input.TestCaseId,
			Evaluation: []ai.Score{score},
		}, nil
	}
}

// Built-in evaluator creators

func createExactMatchEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	evaluator := eval.NewExactMatchEvaluator(config.Name)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createRegexEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	pattern, ok := config.Config["pattern"].(string)
	if !ok {
		pattern = ".*"
	}

	evalConfig := eval.EvaluatorConfig{Pattern: pattern}
	regexEval, err := eval.NewRegexEvaluator(config.Name, evalConfig)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, regexEval), nil
}

func createContainsEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	caseInsensitive := false
	if ci, ok := config.Config["case_insensitive"].(bool); ok {
		caseInsensitive = ci
	}

	evaluator := eval.NewContainsEvaluator(config.Name, caseInsensitive)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createFuzzyMatchEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	threshold := 0.8
	if t, ok := config.Config["threshold"].(float64); ok {
		threshold = t
	}

	evaluator := eval.NewFuzzyMatchEvaluator(config.Name, threshold)
	return wrapEvaluator(config.Name, evaluator), nil
}

// RAG evaluator creators

func createCitationEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	evaluator := rag.NewCitationEvaluator(config.Name)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createCitationCoverageEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	evaluator := rag.NewCitationCoverageEvaluator(config.Name)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createNDCGEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	k := 10
	if kVal, ok := config.Config["k"].(int); ok {
		k = kVal
	}

	evaluator := rag.NewRetrievalEvaluator(config.Name, rag.MetricNDCG, k)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createMRREvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	evaluator := rag.NewRetrievalEvaluator(config.Name, rag.MetricMRR, 0)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createPrecisionEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	k := 5
	if kVal, ok := config.Config["k"].(int); ok {
		k = kVal
	}

	evaluator := rag.NewRetrievalEvaluator(config.Name, rag.MetricPrecision, k)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createRecallEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	k := 10
	if kVal, ok := config.Config["k"].(int); ok {
		k = kVal
	}

	evaluator := rag.NewRetrievalEvaluator(config.Name, rag.MetricRecall, k)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createMAPEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	evaluator := rag.NewRetrievalEvaluator(config.Name, rag.MetricMAP, 0)
	return wrapEvaluator(config.Name, evaluator), nil
}

// Agent evaluator creators

func createClassificationEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	classes := []string{"question", "search"}
	if c, ok := config.Config["classes"].([]any); ok {
		classes = make([]string, len(c))
		for i, class := range c {
			if classStr, ok := class.(string); ok {
				classes[i] = classStr
			}
		}
	}

	evaluator := agent.NewClassificationEvaluator(config.Name, classes)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createConfidenceEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	threshold := 0.7
	if t, ok := config.Config["threshold"].(float64); ok {
		threshold = t
	}

	evaluator := agent.NewConfidenceEvaluator(config.Name, threshold)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createToolSelectionEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	evaluator := agent.NewToolSelectionEvaluator(config.Name)
	return wrapEvaluator(config.Name, evaluator), nil
}

func createToolSequenceEvaluator(config MetricConfig) (ai.EvaluatorFunc, error) {
	evaluator := agent.NewToolCallSequenceEvaluator(config.Name)
	return wrapEvaluator(config.Name, evaluator), nil
}

// LLM-as-judge evaluator creators
// These require a Genkit instance to create LLM-based evaluators

func createFaithfulnessEvaluator(g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	modelName := "ollama/mistral"
	if model, ok := config.Config["model"].(string); ok {
		modelName = model
	}

	faithfulness, err := evalafgenkit.NewFaithfulnessEvaluator(g, modelName)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, faithfulness), nil
}

func createRelevanceEvaluator(g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	modelName := "ollama/mistral"
	if model, ok := config.Config["model"].(string); ok {
		modelName = model
	}

	relevance, err := evalafgenkit.NewRelevanceEvaluator(g, modelName)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, relevance), nil
}

func createCorrectnessEvaluator(g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	modelName := "ollama/mistral"
	if model, ok := config.Config["model"].(string); ok {
		modelName = model
	}

	correctness, err := evalafgenkit.NewCorrectnessEvaluator(g, modelName)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, correctness), nil
}

func createCoherenceEvaluator(g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	modelName := "ollama/mistral"
	if model, ok := config.Config["model"].(string); ok {
		modelName = model
	}

	coherence, err := evalafgenkit.NewCoherenceEvaluator(g, modelName)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, coherence), nil
}

func createSafetyEvaluator(g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	modelName := "ollama/mistral"
	if model, ok := config.Config["model"].(string); ok {
		modelName = model
	}

	safety, err := evalafgenkit.NewSafetyEvaluator(g, modelName)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, safety), nil
}

func createCompletenessEvaluator(g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	modelName := "ollama/mistral"
	if model, ok := config.Config["model"].(string); ok {
		modelName = model
	}

	completeness, err := evalafgenkit.NewCompletenessEvaluator(g, modelName)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, completeness), nil
}

func createHelpfulnessEvaluator(g *genkit.Genkit, config MetricConfig) (ai.EvaluatorFunc, error) {
	modelName := "ollama/mistral"
	if model, ok := config.Config["model"].(string); ok {
		modelName = model
	}

	helpfulness, err := evalafgenkit.NewHelpfulnessEvaluator(g, modelName)
	if err != nil {
		return nil, err
	}

	return wrapEvaluator(config.Name, helpfulness), nil
}
