package genkit

import (
	"github.com/firebase/genkit/go/genkit"
)

// NewFaithfulnessEvaluator creates a faithfulness evaluator using code-defined Handlebars prompt.
func NewFaithfulnessEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "faithfulness", modelName, FaithfulnessPrompt, 0.0)
}

// NewRelevanceEvaluator creates a relevance evaluator using code-defined Handlebars prompt.
func NewRelevanceEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "relevance", modelName, RelevancePrompt, 0.0)
}

// NewCorrectnessEvaluator creates a correctness evaluator using code-defined Handlebars prompt.
func NewCorrectnessEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "correctness", modelName, CorrectnessPrompt, 0.0)
}

// NewCoherenceEvaluator creates a coherence evaluator using code-defined Handlebars prompt.
func NewCoherenceEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "coherence", modelName, CoherencePrompt, 0.0)
}

// NewSafetyEvaluator creates a safety evaluator using code-defined Handlebars prompt.
func NewSafetyEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "safety", modelName, SafetyPrompt, 0.0)
}

// NewCompletenessEvaluator creates a completeness evaluator using code-defined Handlebars prompt.
func NewCompletenessEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "completeness", modelName, CompletenessPrompt, 0.0)
}

// NewHelpfulnessEvaluator creates a helpfulness evaluator using code-defined Handlebars prompt.
func NewHelpfulnessEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "helpfulness", modelName, HelpfulnessPrompt, 0.0)
}

// NewToneEvaluator creates a tone evaluator using code-defined Handlebars prompt.
func NewToneEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "tone", modelName, TonePrompt, 0.0)
}

// NewCitationQualityEvaluator creates a citation quality evaluator using code-defined Handlebars prompt.
func NewCitationQualityEvaluator(g *genkit.Genkit, modelName string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, "citation_quality", modelName, CitationQualityPrompt, 0.0)
}

// NewCustomEvaluator creates a custom LLM judge evaluator with a custom Handlebars prompt.
func NewCustomEvaluator(g *genkit.Genkit, name, modelName, promptTemplate string) (*LLMJudgeEvaluator, error) {
	return NewLLMJudge(g, name, modelName, promptTemplate, 0.0)
}
