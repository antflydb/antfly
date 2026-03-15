package redteam

import (
	"fmt"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/firebase/genkit/go/genkit"
)

// PresetConfig represents different red-team evaluation presets.
type PresetConfig struct {
	Name        string
	Description string
	Evaluators  []eval.Evaluator
}

// QuickRedTeamPreset returns fast pattern-based red-team evaluators.
// No LLM calls - suitable for CI/CD and quick checks.
func QuickRedTeamPreset() []eval.Evaluator {
	return []eval.Evaluator{
		NewPromptInjectionEvaluator("prompt_injection", false),
		NewJailbreakEvaluator("jailbreak"),
		NewPIILeakageEvaluator("pii_leakage", nil),
	}
}

// SafetyRedTeamPreset returns evaluators focused on harmful content detection.
func SafetyRedTeamPreset() []eval.Evaluator {
	return []eval.Evaluator{
		NewHarmfulContentEvaluator("harmful_content", nil),
		NewRefusalEvaluator("refusal_check"),
		NewJailbreakEvaluator("jailbreak"),
	}
}

// PrivacyRedTeamPreset returns evaluators focused on privacy and data leakage.
func PrivacyRedTeamPreset() []eval.Evaluator {
	return []eval.Evaluator{
		NewPIILeakageEvaluator("pii_leakage", nil),
		NewUnauthorizedAccessEvaluator("unauthorized_access", nil, true),
	}
}

// SecurityRedTeamPreset returns evaluators focused on security concerns.
func SecurityRedTeamPreset() []eval.Evaluator {
	return []eval.Evaluator{
		NewPromptInjectionEvaluator("prompt_injection", true), // Strict mode
		NewJailbreakEvaluator("jailbreak"),
		NewUnauthorizedAccessEvaluator("unauthorized_access", nil, true),
	}
}

// ComprehensiveRedTeamPreset returns all pattern-based red-team evaluators.
func ComprehensiveRedTeamPreset() []eval.Evaluator {
	evaluators := []eval.Evaluator{
		// Security
		NewPromptInjectionEvaluator("prompt_injection", false),
		NewJailbreakEvaluator("jailbreak"),

		// Safety
		NewHarmfulContentEvaluator("harmful_content", nil),
		NewRefusalEvaluator("refusal_check"),

		// Privacy
		NewPIILeakageEvaluator("pii_leakage", nil),
		NewUnauthorizedAccessEvaluator("unauthorized_access", nil, true),
	}

	return evaluators
}

// LLMJudgeRedTeamPreset returns LLM-based red-team evaluators.
// Requires a Genkit instance and model specification.
// modelSpec example: "ollama/mistral", "openai/gpt-4", "gemini/gemini-pro"
func LLMJudgeRedTeamPreset(g *genkit.Genkit, modelSpec string) ([]eval.Evaluator, error) {
	evaluators := make([]eval.Evaluator, 0, 5)

	// Create LLM-based evaluators for each category
	categories := []string{
		CategorySafety,
		CategoryPromptInjection,
		CategoryPIILeakage,
		CategoryBias,
		CategoryOverconfidence,
	}

	for _, category := range categories {
		evaluator, err := NewLLMJudgeEvaluator(g, "", modelSpec, category)
		if err != nil {
			return nil, err
		}
		evaluators = append(evaluators, evaluator)
	}

	return evaluators, nil
}

// HybridRedTeamPreset returns a mix of pattern-based and LLM-based evaluators.
// Fast pattern checks first, then LLM-based for sophisticated analysis.
func HybridRedTeamPreset(g *genkit.Genkit, modelSpec string) ([]eval.Evaluator, error) {
	evaluators := make([]eval.Evaluator, 0)

	// Add pattern-based evaluators
	evaluators = append(evaluators, QuickRedTeamPreset()...)

	// Add LLM-based evaluators
	llmEvaluators, err := LLMJudgeRedTeamPreset(g, modelSpec)
	if err != nil {
		return nil, err
	}
	evaluators = append(evaluators, llmEvaluators...)

	return evaluators, nil
}

// CustomRedTeamPreset allows building a custom preset from components.
type CustomRedTeamPresetBuilder struct {
	evaluators []eval.Evaluator
}

// NewCustomRedTeamPreset creates a new custom preset builder.
func NewCustomRedTeamPreset() *CustomRedTeamPresetBuilder {
	return &CustomRedTeamPresetBuilder{
		evaluators: make([]eval.Evaluator, 0),
	}
}

// WithPromptInjection adds prompt injection detection.
func (b *CustomRedTeamPresetBuilder) WithPromptInjection(strict bool) *CustomRedTeamPresetBuilder {
	b.evaluators = append(b.evaluators, NewPromptInjectionEvaluator("", strict))
	return b
}

// WithJailbreak adds jailbreak detection.
func (b *CustomRedTeamPresetBuilder) WithJailbreak() *CustomRedTeamPresetBuilder {
	b.evaluators = append(b.evaluators, NewJailbreakEvaluator(""))
	return b
}

// WithHarmfulContent adds harmful content detection.
func (b *CustomRedTeamPresetBuilder) WithHarmfulContent(categories []HarmCategory) *CustomRedTeamPresetBuilder {
	b.evaluators = append(b.evaluators, NewHarmfulContentEvaluator("", categories))
	return b
}

// WithPIILeakage adds PII leakage detection.
func (b *CustomRedTeamPresetBuilder) WithPIILeakage(types []PIIType) *CustomRedTeamPresetBuilder {
	b.evaluators = append(b.evaluators, NewPIILeakageEvaluator("", types))
	return b
}

// WithRefusal adds refusal detection.
func (b *CustomRedTeamPresetBuilder) WithRefusal() *CustomRedTeamPresetBuilder {
	b.evaluators = append(b.evaluators, NewRefusalEvaluator(""))
	return b
}

// WithUnauthorizedAccess adds unauthorized access detection.
func (b *CustomRedTeamPresetBuilder) WithUnauthorizedAccess(keywords []string, requireContext bool) *CustomRedTeamPresetBuilder {
	b.evaluators = append(b.evaluators, NewUnauthorizedAccessEvaluator("", keywords, requireContext))
	return b
}

// WithLLMJudge adds an LLM-based evaluator.
func (b *CustomRedTeamPresetBuilder) WithLLMJudge(g *genkit.Genkit, model, category string) *CustomRedTeamPresetBuilder {
	if evaluator, err := NewLLMJudgeEvaluator(g, "", model, category); err == nil {
		b.evaluators = append(b.evaluators, evaluator)
	}
	return b
}

// WithCustomEvaluator adds a custom evaluator.
func (b *CustomRedTeamPresetBuilder) WithCustomEvaluator(evaluator eval.Evaluator) *CustomRedTeamPresetBuilder {
	b.evaluators = append(b.evaluators, evaluator)
	return b
}

// Build returns the configured evaluators.
func (b *CustomRedTeamPresetBuilder) Build() []eval.Evaluator {
	return b.evaluators
}

// GetPreset returns a named preset configuration.
func GetPreset(name string, g *genkit.Genkit, modelSpec string) ([]eval.Evaluator, error) {
	switch name {
	case "quick":
		return QuickRedTeamPreset(), nil
	case "safety":
		return SafetyRedTeamPreset(), nil
	case "privacy":
		return PrivacyRedTeamPreset(), nil
	case "security":
		return SecurityRedTeamPreset(), nil
	case "comprehensive":
		return ComprehensiveRedTeamPreset(), nil
	case "llm_judge":
		if g == nil {
			return nil, fmt.Errorf("genkit instance required for llm_judge preset")
		}
		return LLMJudgeRedTeamPreset(g, modelSpec)
	case "hybrid":
		if g == nil {
			return nil, fmt.Errorf("genkit instance required for hybrid preset")
		}
		return HybridRedTeamPreset(g, modelSpec)
	default:
		return nil, fmt.Errorf("unknown preset: %s", name)
	}
}

// ListPresets returns information about available presets.
func ListPresets() []PresetConfig {
	return []PresetConfig{
		{
			Name:        "quick",
			Description: "Fast pattern-based checks (prompt injection, jailbreak, PII) - no LLM calls",
		},
		{
			Name:        "safety",
			Description: "Harmful content detection (violence, hate, illegal, toxicity)",
		},
		{
			Name:        "privacy",
			Description: "Privacy and data leakage detection (PII, unauthorized access)",
		},
		{
			Name:        "security",
			Description: "Security-focused checks (strict prompt injection, jailbreak, access control)",
		},
		{
			Name:        "comprehensive",
			Description: "All pattern-based evaluators (security + safety + privacy)",
		},
		{
			Name:        "llm_judge",
			Description: "LLM-based evaluators (safety, injection, PII, bias, overconfidence) - requires Genkit",
		},
		{
			Name:        "hybrid",
			Description: "Combination of pattern-based and LLM-based evaluators - requires Genkit",
		},
	}
}
