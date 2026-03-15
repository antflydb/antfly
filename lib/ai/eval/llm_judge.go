// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package eval

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/antflydb/antfly/lib/ai"
	libtermite "github.com/antflydb/antfly/lib/termite"
	evalafEval "github.com/antflydb/antfly/pkg/evalaf/eval"
	evalafGenkit "github.com/antflydb/antfly/pkg/evalaf/genkit"
	genkitAI "github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/compat_oai/anthropic"
	"github.com/firebase/genkit/go/plugins/compat_oai/openai"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/firebase/genkit/go/plugins/ollama"
	"github.com/openai/openai-go/option"
)

// llmJudgeEvaluator wraps an evalaf LLM judge evaluator.
type llmJudgeEvaluator struct {
	name     EvaluatorName
	category EvalCategory
	judge    *evalafGenkit.LLMJudgeEvaluator
}

// Name returns the evaluator name.
func (e *llmJudgeEvaluator) Name() string {
	return string(e.name)
}

// Category returns the evaluator category.
func (e *llmJudgeEvaluator) Category() EvalCategory {
	return e.category
}

// Evaluate runs the LLM judge evaluation.
func (e *llmJudgeEvaluator) Evaluate(ctx context.Context, input InternalEvalInput) (*EvaluatorScore, error) {
	// Convert internal input to evalaf input format
	evalInput := convertToEvalafInput(input)

	// Run the evaluation
	result, err := e.judge.Evaluate(ctx, evalInput)
	if err != nil {
		return nil, fmt.Errorf("llm judge evaluation failed: %w", err)
	}

	// Convert result
	metadata := result.Metadata
	if metadata == nil {
		metadata = make(map[string]any)
	}

	return &EvaluatorScore{
		Score:    float32(result.Score),
		Pass:     result.Pass,
		Reason:   result.Reason,
		Metadata: metadata,
	}, nil
}

// convertToEvalafInput converts our internal input format to evalaf's EvalInput.
func convertToEvalafInput(input InternalEvalInput) evalafEval.EvalInput {
	evalInput := evalafEval.EvalInput{
		Input:    input.Query,
		Output:   input.Output,
		Context:  input.Context,
		Metadata: make(map[string]any),
	}

	// Set expectations as reference for correctness evaluator
	if input.Expectations != "" {
		evalInput.Reference = input.Expectations
	}

	return evalInput
}

// InitGenkit initializes a genkit instance from a GeneratorConfig.
func InitGenkit(ctx context.Context, config *ai.GeneratorConfig) (*genkit.Genkit, string, error) {
	if config == nil {
		return nil, "", fmt.Errorf("generator config required for LLM evaluators")
	}

	var g *genkit.Genkit
	var modelName string

	switch config.Provider {
	case ai.GeneratorProviderOllama:
		c, err := config.AsOllamaGeneratorConfig()
		if err != nil {
			return nil, "", fmt.Errorf("parsing ollama config: %w", err)
		}

		serverAddr := "http://localhost:11434"
		if c.Url != nil && *c.Url != "" {
			serverAddr = *c.Url
		}

		ollamaPlugin := &ollama.Ollama{ServerAddress: serverAddr}
		g = genkit.Init(ctx, genkit.WithPlugins(ollamaPlugin))

		// Define the model
		var modelOpts *genkitAI.ModelOptions
		mediaSupportedModels := []string{"gemma3:4b", "gemma3:12b", "gemma3:27b"}
		if slices.Contains(mediaSupportedModels, c.Model) {
			modelOpts = &genkitAI.ModelOptions{
				Label: c.Model,
				Supports: &genkitAI.ModelSupports{
					Multiturn:  true,
					SystemRole: true,
					Media:      true,
					Tools:      false,
				},
			}
		}
		ollamaPlugin.DefineModel(g, ollama.ModelDefinition{
			Name: c.Model,
			Type: "chat",
		}, modelOpts)

		modelName = fmt.Sprintf("ollama/%s", c.Model)

	case ai.GeneratorProviderGemini:
		c, err := config.AsGoogleGeneratorConfig()
		if err != nil {
			return nil, "", fmt.Errorf("parsing google config: %w", err)
		}

		model := c.Model
		if model == "" {
			model = "gemini-2.5-flash"
		}

		googlePlugin := &googlegenai.GoogleAI{}
		if c.ApiKey != nil && *c.ApiKey != "" {
			googlePlugin.APIKey = *c.ApiKey
		}

		g = genkit.Init(ctx, genkit.WithPlugins(googlePlugin))
		modelName = fmt.Sprintf("googleai/%s", model)

	case ai.GeneratorProviderOpenai:
		c, err := config.AsOpenAIGeneratorConfig()
		if err != nil {
			return nil, "", fmt.Errorf("parsing openai config: %w", err)
		}

		openaiPlugin := &openai.OpenAI{}
		if c.ApiKey != nil && *c.ApiKey != "" {
			openaiPlugin.APIKey = *c.ApiKey
		}

		var opts []option.RequestOption
		if c.Url != nil && *c.Url != "" {
			opts = append(opts, option.WithBaseURL(*c.Url))
		}
		if len(opts) > 0 {
			openaiPlugin.Opts = opts
		}

		g = genkit.Init(ctx, genkit.WithPlugins(openaiPlugin))

		// Define the model
		openaiPlugin.DefineModel(c.Model, genkitAI.ModelOptions{})
		modelName = fmt.Sprintf("openai/%s", c.Model)

	case ai.GeneratorProviderAnthropic:
		c, err := config.AsAnthropicGeneratorConfig()
		if err != nil {
			return nil, "", fmt.Errorf("parsing anthropic config: %w", err)
		}

		model := c.Model
		if model == "" {
			model = "claude-3-7-sonnet-20250219"
		}

		var opts []option.RequestOption
		apiKey := ""
		if c.ApiKey != nil && *c.ApiKey != "" {
			apiKey = *c.ApiKey
		} else if envKey := os.Getenv("ANTHROPIC_API_KEY"); envKey != "" {
			apiKey = envKey
		}
		if apiKey != "" {
			opts = append(opts, option.WithAPIKey(apiKey))
		}
		if c.Url != nil && *c.Url != "" {
			opts = append(opts, option.WithBaseURL(*c.Url))
		}

		anthropicPlugin := &anthropic.Anthropic{Opts: opts}
		g = genkit.Init(ctx, genkit.WithPlugins(anthropicPlugin))

		anthropicPlugin.DefineModel(model, genkitAI.ModelOptions{})
		modelName = fmt.Sprintf("anthropic/%s", model)

	case ai.GeneratorProviderTermite:
		c, err := config.AsTermiteGeneratorConfig()
		if err != nil {
			return nil, "", fmt.Errorf("parsing termite config: %w", err)
		}

		configURL := ""
		if c.ApiUrl != nil {
			configURL = *c.ApiUrl
		}
		apiURL := libtermite.ResolveURL(configURL)
		if apiURL == "" {
			return nil, "", fmt.Errorf("termite: api_url is required (set via config, ANTFLY_TERMITE_URL env var, or termite.api_url in config file)")
		}

		// Termite's /generate endpoint is OpenAI-compatible, so we use the OpenAI plugin
		// with a custom base URL pointing to Termite
		termiteURL := apiURL + "/openai/v1"
		opts := []option.RequestOption{
			option.WithBaseURL(termiteURL),
		}

		openaiPlugin := &openai.OpenAI{
			// Termite doesn't require an API key, but the OpenAI plugin requires one
			APIKey: "termite-local",
			Opts:   opts,
		}

		g = genkit.Init(ctx, genkit.WithPlugins(openaiPlugin))

		// Define the model
		openaiPlugin.DefineModel(c.Model, genkitAI.ModelOptions{})
		modelName = fmt.Sprintf("openai/%s", c.Model)

	default:
		return nil, "", fmt.Errorf("unsupported provider for LLM evaluation: %s", config.Provider)
	}

	return g, modelName, nil
}

// Factory functions for LLM judge evaluators

// providerToGenkitPrefix maps our provider names to genkit plugin prefixes
func providerToGenkitPrefix(provider ai.GeneratorProvider) string {
	switch provider {
	case ai.GeneratorProviderGemini:
		return "googleai"
	case ai.GeneratorProviderOllama:
		return "ollama"
	case ai.GeneratorProviderOpenai:
		return "openai"
	case ai.GeneratorProviderAnthropic:
		return "anthropic"
	case ai.GeneratorProviderTermite:
		// Termite uses the OpenAI-compatible API
		return "openai"
	default:
		return string(provider)
	}
}

func newRelevanceFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for relevance evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	judge, err := evalafGenkit.NewRelevanceEvaluator(g, fullModelName)
	if err != nil {
		return nil, fmt.Errorf("creating relevance evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameRelevance,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}

func newFaithfulnessFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for faithfulness evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	judge, err := evalafGenkit.NewFaithfulnessEvaluator(g, fullModelName)
	if err != nil {
		return nil, fmt.Errorf("creating faithfulness evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameFaithfulness,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}

func newCompletenessFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for completeness evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	judge, err := evalafGenkit.NewCompletenessEvaluator(g, fullModelName)
	if err != nil {
		return nil, fmt.Errorf("creating completeness evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameCompleteness,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}

func newCoherenceFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for coherence evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	judge, err := evalafGenkit.NewCoherenceEvaluator(g, fullModelName)
	if err != nil {
		return nil, fmt.Errorf("creating coherence evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameCoherence,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}

func newSafetyFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for safety evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	judge, err := evalafGenkit.NewSafetyEvaluator(g, fullModelName)
	if err != nil {
		return nil, fmt.Errorf("creating safety evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameSafety,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}

func newHelpfulnessFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for helpfulness evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	judge, err := evalafGenkit.NewHelpfulnessEvaluator(g, fullModelName)
	if err != nil {
		return nil, fmt.Errorf("creating helpfulness evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameHelpfulness,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}

func newCorrectnessFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for correctness evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	// Use custom prompt that references expectations instead of expected answer
	correctnessWithExpectationsPrompt := `You are an expert evaluator. Your task is to determine if the answer meets the expected criteria.

**Question:** {{Input}}

**Expectations:** {{Reference}}

**Actual Answer:** {{#if Output.answer}}{{Output.answer}}{{else}}{{Output}}{{/if}}

Evaluate if the actual answer meets the expectations. The answer should address the key points mentioned in the expectations.

Respond in JSON format:
{
  "pass": true/false,
  "score": 0.0-1.0,
  "reason": "Brief explanation",
  "confidence": 0.0-1.0
}

**Evaluation:**`

	judge, err := evalafGenkit.NewLLMJudge(g, "correctness", fullModelName, correctnessWithExpectationsPrompt, 0.0)
	if err != nil {
		return nil, fmt.Errorf("creating correctness evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameCorrectness,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}

func newCitationQualityFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	if g == nil || judgeConfig == nil {
		return nil, fmt.Errorf("genkit instance and judge config required for citation_quality evaluator")
	}

	modelName, err := judgeConfig.GetModel()
	if err != nil {
		return nil, err
	}
	fullModelName := fmt.Sprintf("%s/%s", providerToGenkitPrefix(judgeConfig.Provider), modelName)

	judge, err := evalafGenkit.NewCitationQualityEvaluator(g, fullModelName)
	if err != nil {
		return nil, fmt.Errorf("creating citation_quality evaluator: %w", err)
	}

	return &llmJudgeEvaluator{
		name:     EvaluatorNameCitationQuality,
		category: CategoryGeneration,
		judge:    judge,
	}, nil
}
