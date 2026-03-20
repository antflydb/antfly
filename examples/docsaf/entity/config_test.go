package entity

import (
	"strings"
	"testing"

	docsafentity "github.com/antflydb/antfly/pkg/docsaf/entity"
)

func TestConfigValidateRejectsPromptTextVariable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Model = "functiongemma"
	cfg.Kind = ExtractorKindGenerator
	cfg.PromptTemplate = "Read {{Text}} and extract entities."

	err := cfg.Validate()
	if err == nil {
		t.Fatalf("expected validation error for Text prompt variable")
	}
	if !strings.Contains(err.Error(), "source text is sent separately") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGeneratorRenderPromptUsesInstructionContextOnly(t *testing.T) {
	extractor := termiteGeneratorExtractor{
		model:          "functiongemma",
		promptTemplate: "Labels: {{#each EntityLabels}}{{this}} {{/each}}",
	}

	prompt, err := extractor.renderPrompt(docsafentity.ExtractOptions{
		EntityLabels: []string{"technology", "concept"},
	})
	if err != nil {
		t.Fatalf("renderPrompt returned error: %v", err)
	}
	if strings.Contains(prompt, "user-provided text") {
		t.Fatalf("custom template should replace default prompt, got %q", prompt)
	}
	if !strings.Contains(prompt, "technology") || !strings.Contains(prompt, "concept") {
		t.Fatalf("prompt did not contain rendered labels: %q", prompt)
	}
}

func TestGeneratorRenderPromptDefaultIncludesFunctionInstruction(t *testing.T) {
	extractor := termiteGeneratorExtractor{model: "functiongemma"}

	prompt, err := extractor.renderPrompt(docsafentity.ExtractOptions{
		EntityLabels: []string{"technology"},
	})
	if err != nil {
		t.Fatalf("renderPrompt returned error: %v", err)
	}
	if !strings.Contains(prompt, "Call the extract_entities function exactly once.") {
		t.Fatalf("default prompt missing function instruction: %q", prompt)
	}
	if !strings.Contains(prompt, "technology") {
		t.Fatalf("default prompt missing entity label guidance: %q", prompt)
	}
}
