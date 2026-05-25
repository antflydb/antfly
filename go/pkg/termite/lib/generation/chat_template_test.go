package generation

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeMultilineStrings(t *testing.T) {
	// Simulate the problematic pattern from Gemma chat template:
	// string literal with actual newline inside a {{ }} tag
	input := "{{ '<start_of_turn>' + role + '\n' + extra }}"
	got := normalizeMultilineStrings(input)
	want := `{{ '<start_of_turn>' + role + '\n' + extra }}`
	if got != want {
		t.Errorf("normalizeMultilineStrings:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestNormalizeMultilineStringsInSetTag(t *testing.T) {
	input := "{%- set x = y + '\n\n' -%}"
	got := normalizeMultilineStrings(input)
	want := `{%- set x = y + '\n\n' -%}`
	if got != want {
		t.Errorf("normalizeMultilineStrings:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestNormalizePreservesTextOutsideTags(t *testing.T) {
	// Newlines outside tags should NOT be modified
	input := "hello\nworld\n{{ x }}"
	got := normalizeMultilineStrings(input)
	if got != input {
		t.Errorf("normalizeMultilineStrings modified text outside tags:\n  got:  %q\n  want: %q", got, input)
	}
}

func TestStripRaiseException(t *testing.T) {
	input := `before{%- if x != y -%}{{ raise_exception("bad") }}{%- endif -%}after`
	got := stripRaiseException(input)
	want := "beforeafter"
	if got != want {
		t.Errorf("stripRaiseException:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestStripRaiseExceptionElseBranch(t *testing.T) {
	input := `{%- if a -%}ok{%- elif b -%}also{%- else -%}{{ raise_exception("nope") }}{%- endif -%}done`
	got := stripRaiseException(input)
	want := `{%- if a -%}ok{%- elif b -%}also{%- endif -%}done`
	if got != want {
		t.Errorf("stripRaiseException else branch:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestRewriteInlineIf(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "basic inline if",
			input: `{{ (x if flag else y) }}`,
			want:  `{% if flag %}{{ x }}{% else %}{{ y }}{% endif %}`,
		},
		{
			name:  "inline if with concatenation",
			input: `{{ 'hello' + (name if has_name else "world") }}`,
			want:  `{% if has_name %}{{ 'hello' + name }}{% else %}{{ 'hello' + "world" }}{% endif %}`,
		},
		{
			name:  "no inline if",
			input: `{{ x + y }}`,
			want:  `{{ x + y }}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rewriteInlineIf(tt.input)
			if got != tt.want {
				t.Errorf("rewriteInlineIf:\n  got:  %q\n  want: %q", got, tt.want)
			}
		})
	}
}

func TestLoadChatTemplateFromGemma(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "chat_template.jinja")); err != nil { //nolint:gosec // test-only path
		t.Skip("Gemma model not downloaded, skipping")
	}

	ct, err := LoadChatTemplate(modelPath)
	if err != nil {
		t.Fatalf("LoadChatTemplate: %v", err)
	}
	if ct == nil {
		t.Fatal("LoadChatTemplate returned nil")
	}

	messages := []Message{
		{Role: "user", Content: "The capital of France is"},
	}

	out, err := ct.Apply(messages, true)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	expected := "<bos><start_of_turn>user\nThe capital of France is<end_of_turn>\n<start_of_turn>model\n"
	if out != expected {
		t.Errorf("Apply output mismatch:\n  got:  %q\n  want: %q", out, expected)
	}
}

func TestLoadChatTemplateMultiTurn(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "chat_template.jinja")); err != nil { //nolint:gosec // test-only path
		t.Skip("Gemma model not downloaded, skipping")
	}

	ct, err := LoadChatTemplate(modelPath)
	if err != nil {
		t.Fatalf("LoadChatTemplate: %v", err)
	}

	messages := []Message{
		{Role: "user", Content: "Hello"},
		{Role: "assistant", Content: "Hi there!"},
		{Role: "user", Content: "What is 2+2?"},
	}

	out, err := ct.Apply(messages, true)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	expected := "<bos><start_of_turn>user\nHello<end_of_turn>\n<start_of_turn>model\nHi there!<end_of_turn>\n<start_of_turn>user\nWhat is 2+2?<end_of_turn>\n<start_of_turn>model\n"
	if out != expected {
		t.Errorf("Apply output mismatch:\n  got:  %q\n  want: %q", out, expected)
	}
}

func TestLoadChatTemplateWithSystem(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "chat_template.jinja")); err != nil { //nolint:gosec // test-only path
		t.Skip("Gemma model not downloaded, skipping")
	}

	ct, err := LoadChatTemplate(modelPath)
	if err != nil {
		t.Fatalf("LoadChatTemplate: %v", err)
	}

	messages := []Message{
		{Role: "system", Content: "You are a helpful assistant."},
		{Role: "user", Content: "Hello"},
	}

	out, err := ct.Apply(messages, true)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// System message should be prepended to first user message
	expected := "<bos><start_of_turn>user\nYou are a helpful assistant.\n\nHello<end_of_turn>\n<start_of_turn>model\n"
	if out != expected {
		t.Errorf("Apply output mismatch:\n  got:  %q\n  want: %q", out, expected)
	}
}
