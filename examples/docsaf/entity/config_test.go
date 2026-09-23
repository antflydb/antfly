// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entity

import (
	"strings"
	"testing"

	docsafentity "github.com/antflydb/antfly/go/pkg/docsaf/entity"
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
	extractor := inferenceGeneratorExtractor{
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
	extractor := inferenceGeneratorExtractor{model: "functiongemma"}

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
