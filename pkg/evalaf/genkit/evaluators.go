package genkit

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
)

// LLMJudgeEvaluator uses an LLM to evaluate outputs using Genkit prompts defined in code.
type LLMJudgeEvaluator struct {
	name   string
	prompt ai.Prompt
}

// EvalInput represents the input structure for evaluation prompts.
type EvalInput struct {
	Input     any            `json:"Input"`
	Output    any            `json:"Output"`
	Reference any            `json:"Reference,omitempty"`
	Context   []any          `json:"Context,omitempty"`
	Metadata  map[string]any `json:"Metadata,omitempty"`
}

// LLMJudgeResponse represents the structured response from an LLM judge.
type LLMJudgeResponse struct {
	Pass       bool    `json:"pass"`
	Score      float64 `json:"score"`
	Reason     string  `json:"reason"`
	Confidence float64 `json:"confidence,omitempty"`
}

// GranularCompletenessResponse extends LLMJudgeResponse with detailed tracking.
type GranularCompletenessResponse struct {
	Pass             bool     `json:"pass"`
	Score            float64  `json:"score"`
	Reason           string   `json:"reason"`
	Confidence       float64  `json:"confidence,omitempty"`
	KeywordsCovered  []string `json:"keywords_covered,omitempty"`
	KeywordsMissing  []string `json:"keywords_missing,omitempty"`
	AspectsAddressed []string `json:"aspects_addressed,omitempty"`
	Gaps             []string `json:"gaps,omitempty"`
}

// NewLLMJudge creates a new LLM judge evaluator with a code-defined prompt using Handlebars syntax.
func NewLLMJudge(g *genkit.Genkit, name, modelName, promptText string, temperature float64) (*LLMJudgeEvaluator, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if modelName == "" {
		return nil, fmt.Errorf("model name is required")
	}
	if promptText == "" {
		return nil, fmt.Errorf("prompt text is required")
	}

	// Define the prompt using Genkit's DefinePrompt with Handlebars syntax
	prompt := genkit.DefinePrompt(
		g, name,
		ai.WithModelName(modelName),
		ai.WithPrompt(promptText),
		ai.WithConfig(map[string]any{"temperature": temperature}),
		ai.WithInputType(EvalInput{}),
		ai.WithOutputType(LLMJudgeResponse{}),
	)

	return &LLMJudgeEvaluator{
		name:   name,
		prompt: prompt,
	}, nil
}

// Name returns the evaluator name.
func (e *LLMJudgeEvaluator) Name() string {
	return e.name
}

// Evaluate performs LLM-based evaluation using the code-defined prompt.
func (e *LLMJudgeEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	// Prepare input for the prompt
	promptInput := EvalInput{
		Input:     input.Input,
		Output:    input.Output,
		Reference: input.Reference,
		Context:   input.Context,
		Metadata:  input.Metadata,
	}

	// Execute the prompt
	response, err := e.prompt.Execute(ctx, ai.WithInput(promptInput))
	if err != nil {
		return nil, fmt.Errorf("prompt execution failed: %w", err)
	}

	// Parse structured output
	var judgeResponse LLMJudgeResponse
	if err := response.Output(&judgeResponse); err != nil {
		return nil, fmt.Errorf("failed to parse output: %w", err)
	}

	return &eval.EvalResult{
		Pass:   judgeResponse.Pass,
		Score:  judgeResponse.Score,
		Reason: judgeResponse.Reason,
		Metadata: map[string]any{
			"confidence":   judgeResponse.Confidence,
			"raw_response": response.Text(),
		},
	}, nil
}

// SupportsStreaming returns false (not implemented yet).
func (e *LLMJudgeEvaluator) SupportsStreaming() bool {
	return false
}

// GranularCompletenessEvaluator uses an LLM to evaluate completeness with detailed tracking.
type GranularCompletenessEvaluator struct {
	name   string
	prompt ai.Prompt
}

// NewGranularCompletenessEvaluator creates a new granular completeness evaluator.
func NewGranularCompletenessEvaluator(g *genkit.Genkit, name, modelName string) (*GranularCompletenessEvaluator, error) {
	if name == "" {
		name = "granular_completeness"
	}
	if modelName == "" {
		return nil, fmt.Errorf("model name is required")
	}

	// Define the prompt with granular response type
	prompt := genkit.DefinePrompt(
		g, name,
		ai.WithModelName(modelName),
		ai.WithPrompt(GranularCompletenessPrompt),
		ai.WithConfig(map[string]any{"temperature": 0.0}),
		ai.WithInputType(EvalInput{}),
		ai.WithOutputType(GranularCompletenessResponse{}),
	)

	return &GranularCompletenessEvaluator{
		name:   name,
		prompt: prompt,
	}, nil
}

// Name returns the evaluator name.
func (e *GranularCompletenessEvaluator) Name() string {
	return e.name
}

// Evaluate performs granular completeness evaluation.
func (e *GranularCompletenessEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	// Prepare input for the prompt
	promptInput := EvalInput{
		Input:     input.Input,
		Output:    input.Output,
		Reference: input.Reference,
		Context:   input.Context,
		Metadata:  input.Metadata,
	}

	// Execute the prompt
	response, err := e.prompt.Execute(ctx, ai.WithInput(promptInput))
	if err != nil {
		return nil, fmt.Errorf("prompt execution failed: %w", err)
	}

	// Parse structured output
	var granularResponse GranularCompletenessResponse
	if err := response.Output(&granularResponse); err != nil {
		return nil, fmt.Errorf("failed to parse output: %w", err)
	}

	return &eval.EvalResult{
		Pass:   granularResponse.Pass,
		Score:  granularResponse.Score,
		Reason: granularResponse.Reason,
		Metadata: map[string]any{
			"confidence":        granularResponse.Confidence,
			"keywords_covered":  granularResponse.KeywordsCovered,
			"keywords_missing":  granularResponse.KeywordsMissing,
			"aspects_addressed": granularResponse.AspectsAddressed,
			"gaps":              granularResponse.Gaps,
			"raw_response":      response.Text(),
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *GranularCompletenessEvaluator) SupportsStreaming() bool {
	return false
}
