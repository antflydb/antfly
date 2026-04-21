// Copyright 2025 Antfly, Inc.
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

package generation

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
)

// ChatTemplate renders HuggingFace Jinja2 chat templates.
type ChatTemplate struct {
	template *exec.Template
	// Special tokens from tokenizer_config.json
	bosToken string
	eosToken string
	unkToken string
	padToken string
}

// LoadChatTemplate loads a chat template from a model directory.
// It reads chat_template.jinja and tokenizer_config.json for special tokens.
// Returns nil if no chat template is found.
func LoadChatTemplate(modelPath string) (*ChatTemplate, error) {
	// Read the Jinja template
	templatePath := filepath.Join(modelPath, "chat_template.jinja")
	templateData, err := os.ReadFile(templatePath) //nolint:gosec // modelPath is a trusted internal path, not user input
	if err != nil {
		return nil, nil // No template found — not an error
	}

	templateStr := string(templateData)
	if strings.TrimSpace(templateStr) == "" {
		return nil, nil
	}

	// Preprocess to fix multiline string literals inside tags,
	// which gonja's parser cannot handle.
	templateStr = normalizeMultilineStrings(templateStr)

	// Strip the raise_exception calls — gonja doesn't have this function,
	// and they're just input validation we don't need.
	templateStr = stripRaiseException(templateStr)

	// Rewrite inline if expressions (x if cond else y) into
	// gonja-compatible {% if %}{% endif %} blocks.
	templateStr = rewriteInlineIf(templateStr)

	// Parse template
	tpl, err := gonja.FromString(templateStr)
	if err != nil {
		return nil, fmt.Errorf("parsing chat template: %w", err)
	}

	// Load special tokens from tokenizer_config.json
	bosToken, eosToken, unkToken, padToken := loadSpecialTokens(modelPath)

	return &ChatTemplate{
		template: tpl,
		bosToken: bosToken,
		eosToken: eosToken,
		unkToken: unkToken,
		padToken: padToken,
	}, nil
}

// Apply renders the chat template with the given messages.
// Messages are converted to the format expected by HuggingFace templates:
// [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}, ...]
func (ct *ChatTemplate) Apply(messages []Message, addGenerationPrompt bool) (string, error) {
	// Convert messages to the map format expected by Jinja templates
	msgMaps := make([]map[string]any, len(messages))
	for i, m := range messages {
		msgMaps[i] = map[string]any{
			"role":    m.Role,
			"content": m.GetTextContent(),
		}
	}

	ctx := exec.NewContext(map[string]any{
		"messages":              msgMaps,
		"add_generation_prompt": addGenerationPrompt,
		"bos_token":             ct.bosToken,
		"eos_token":             ct.eosToken,
		"unk_token":             ct.unkToken,
		"pad_token":             ct.padToken,
	})

	out, err := ct.template.ExecuteToString(ctx)
	if err != nil {
		return "", fmt.Errorf("executing chat template: %w", err)
	}

	return out, nil
}

// normalizeMultilineStrings fixes string literals that contain actual newline
// characters inside Jinja2 tags ({{ }} and {% %}). gonja's parser splits tokens
// on newlines, so a string like '<start_of_turn>' + role + '\n' that spans two
// physical lines causes parse errors. This function replaces actual newlines
// inside quoted strings within tags with the literal characters \n.
func normalizeMultilineStrings(template string) string {
	var result strings.Builder
	result.Grow(len(template))

	i := 0
	for i < len(template) {
		// Look for tag openings: {{ or {%
		if i+1 < len(template) && template[i] == '{' && (template[i+1] == '{' || template[i+1] == '%') {
			closer := "}}"
			if template[i+1] == '%' {
				closer = "%}"
			}

			// Find the end of this tag, respecting string literals
			tagStart := i
			i += 2 // skip opening {{ or {%

			for i < len(template) {
				// Check for tag close
				if i+1 < len(template) && template[i:i+2] == closer {
					i += 2
					break
				}

				// Handle string literals — replace newlines inside them
				if template[i] == '\'' || template[i] == '"' {
					quote := template[i]
					result.WriteString(template[tagStart:i])
					i++ // skip opening quote
					result.WriteByte(quote)

					// Scan to end of string, replacing newlines
					for i < len(template) && template[i] != quote {
						if template[i] == '\\' && i+1 < len(template) {
							// Escaped character — keep as-is
							result.WriteByte(template[i])
							result.WriteByte(template[i+1])
							i += 2
							continue
						}
						switch template[i] {
						case '\n':
							result.WriteString(`\n`)
						case '\r':
							result.WriteString(`\r`)
						default:
							result.WriteByte(template[i])
						}
						i++
					}
					if i < len(template) {
						result.WriteByte(quote) // closing quote
						i++
					}
					tagStart = i
					continue
				}

				i++
			}
			result.WriteString(template[tagStart:i])
		} else {
			result.WriteByte(template[i])
			i++
		}
	}

	return result.String()
}

// stripRaiseException removes if/raise_exception/endif blocks from HuggingFace
// templates. These are input validation checks that gonja can't execute.
//
// It handles two patterns:
//  1. Standalone: {%- if COND -%}{{ raise_exception(...) }}{%- endif -%}
//  2. Else branch: {%- else -%}{{ raise_exception(...) }}{%- endif -%}
//     In this case only the else+raise+endif is removed, preserving the outer if.
func stripRaiseException(template string) string {
	for {
		idx := strings.Index(template, "raise_exception")
		if idx < 0 {
			break
		}

		// Find the {{ }} expression tag containing raise_exception
		exprStart := strings.LastIndex(template[:idx], "{{")
		if exprStart < 0 {
			break
		}
		exprEnd := strings.Index(template[idx:], "}}")
		if exprEnd < 0 {
			break
		}
		exprEnd = idx + exprEnd + 2

		// Find the endif after the raise_exception expression
		endifIdx := strings.Index(template[exprEnd:], "endif")
		if endifIdx < 0 {
			break
		}
		endifAbsolute := exprEnd + endifIdx
		// Find the {% containing this endif
		endifTagStart := strings.LastIndex(template[:endifAbsolute], "{%")
		if endifTagStart < 0 {
			break
		}
		// Find the %} closing this endif tag
		endifTagEnd := strings.Index(template[endifAbsolute:], "%}")
		if endifTagEnd < 0 {
			break
		}
		endPos := endifAbsolute + endifTagEnd + 2

		// Find the tag just before the {{ raise_exception }} expression.
		// This should be either {%- if ... -%} or {%- else -%}.
		prevTagEnd := strings.LastIndex(template[:exprStart], "%}")
		if prevTagEnd < 0 {
			break
		}
		prevTagEnd += 2
		prevTagStart := strings.LastIndex(template[:prevTagEnd], "{%")
		if prevTagStart < 0 {
			break
		}

		// Extract tag content to determine if it's "if" or "else"
		tagContent := template[prevTagStart+2 : prevTagEnd-2]
		tagContent = strings.TrimSpace(tagContent)
		tagContent = strings.TrimPrefix(tagContent, "-")
		tagContent = strings.TrimSuffix(tagContent, "-")
		tagContent = strings.TrimSpace(tagContent)

		if strings.HasPrefix(tagContent, "if ") || strings.HasPrefix(tagContent, "if(") {
			// Pattern 1: standalone {%- if COND -%}{{ raise_exception(...) }}{%- endif -%}
			template = template[:prevTagStart] + template[endPos:]
		} else if tagContent == "else" {
			// Pattern 2: {%- else -%}{{ raise_exception(...) }}{%- endif -%}
			// Remove else+raise, keep endif to close the outer if/elif chain.
			template = template[:prevTagStart] + template[endifTagStart:]
		} else {
			// Unknown pattern — skip past this raise_exception to avoid infinite loop
			template = template[:idx] + template[idx+len("raise_exception"):]
		}
	}

	return template
}

// rewriteInlineIf transforms Jinja2 inline if expressions into gonja-compatible
// constructs. gonja doesn't support (X if COND else Y) syntax.
//
// It finds patterns like:
//
//	{{ expr + (VAR if COND else ALT) }}
//
// and rewrites them as:
//
//	{% if COND %}{{ expr + VAR }}{% else %}{{ expr + ALT }}{% endif %}
func rewriteInlineIf(template string) string {
	// Process expression tags {{ ... }} that contain inline if
	var result strings.Builder
	result.Grow(len(template))

	i := 0
	for i < len(template) {
		// Look for {{ opening
		if i+1 < len(template) && template[i] == '{' && template[i+1] == '{' {
			// Find matching }}
			tagStart := i
			tagEnd := findTagEnd(template, i+2, "}}")
			if tagEnd < 0 {
				result.WriteByte(template[i])
				i++
				continue
			}
			tagEnd += 2 // include }}

			tagContent := template[tagStart:tagEnd]
			if strings.Contains(tagContent, " if ") && !strings.Contains(tagContent, "raise_exception") {
				rewritten := rewriteExpressionTag(tagContent)
				result.WriteString(rewritten)
			} else {
				result.WriteString(tagContent)
			}
			i = tagEnd
			continue
		}

		result.WriteByte(template[i])
		i++
	}

	return result.String()
}

// rewriteExpressionTag rewrites a single {{ ... }} tag containing an inline if.
// Input:  {{ '<start_of_turn>' + role + '\n' + (first_user_prefix if loop.first else "") }}
// Output: {% if loop.first %}{{ '<start_of_turn>' + role + '\n' + first_user_prefix }}{% else %}{{ '<start_of_turn>' + role + '\n' + "" }}{% endif %}
func rewriteExpressionTag(tag string) string {
	// Extract the inner content between {{ and }}
	inner := tag
	prefix := "{{"
	suffix := "}}"

	// Handle whitespace control: {{- and -}}
	if strings.HasPrefix(inner, "{{-") {
		prefix = "{{-"
		inner = inner[3:]
	} else {
		inner = inner[2:]
	}
	if strings.HasSuffix(inner, "-}}") {
		suffix = "-}}"
		inner = inner[:len(inner)-3]
	} else {
		inner = inner[:len(inner)-2]
	}
	inner = strings.TrimSpace(inner)

	// Find the inline if pattern: (EXPR if COND else ALT)
	// Look for the outermost parenthesized inline if
	parenStart := -1
	for j := 0; j < len(inner); j++ {
		if inner[j] == '(' {
			// Check if this paren contains " if " and " else "
			closeIdx := findMatchingParen(inner, j)
			if closeIdx < 0 {
				continue
			}
			parenContent := inner[j+1 : closeIdx]
			if containsInlineIf(parenContent) {
				parenStart = j
				break
			}
		}
	}

	if parenStart < 0 {
		return tag // no inline if found
	}

	parenEnd := findMatchingParen(inner, parenStart)
	if parenEnd < 0 {
		return tag
	}

	parenContent := inner[parenStart+1 : parenEnd]
	beforeParen := inner[:parenStart]
	afterParen := inner[parenEnd+1:]

	// Parse: EXPR if COND else ALT
	trueExpr, condition, falseExpr := parseInlineIf(parenContent)
	if condition == "" {
		return tag
	}

	// Build: {% if COND %}{{ before + trueExpr + after }}{% else %}{{ before + falseExpr + after }}{% endif %}
	var result strings.Builder
	result.WriteString("{% if ")
	result.WriteString(condition)
	result.WriteString(" %}")
	result.WriteString(prefix)
	result.WriteString(" ")
	result.WriteString(beforeParen)
	result.WriteString(trueExpr)
	result.WriteString(afterParen)
	result.WriteString(" ")
	result.WriteString(suffix)
	result.WriteString("{% else %}")
	result.WriteString(prefix)
	result.WriteString(" ")
	result.WriteString(beforeParen)
	result.WriteString(falseExpr)
	result.WriteString(afterParen)
	result.WriteString(" ")
	result.WriteString(suffix)
	result.WriteString("{% endif %}")

	return result.String()
}

// containsInlineIf checks if text contains an inline if pattern (not inside nested parens).
func containsInlineIf(text string) bool {
	_, cond, _ := parseInlineIf(text)
	return cond != ""
}

// parseInlineIf parses "EXPR if COND else ALT" returning the three parts.
// Returns empty condition if the pattern doesn't match.
func parseInlineIf(text string) (trueExpr, condition, falseExpr string) {
	// Find " if " that's not inside quotes or parens
	depth := 0
	inQuote := byte(0)

	for i := 0; i < len(text); i++ {
		ch := text[i]
		if inQuote != 0 {
			if ch == '\\' && i+1 < len(text) {
				i++ // skip escaped char
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}

		if ch == '\'' || ch == '"' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			continue
		}

		if depth == 0 && i+4 <= len(text) && text[i:i+4] == " if " {
			// Found " if ", now find " else "
			trueExpr = strings.TrimSpace(text[:i])
			remaining := text[i+4:]

			// Find " else " in remaining, respecting quotes and parens
			elseIdx := findElse(remaining)
			if elseIdx < 0 {
				continue // not a valid inline if, keep looking
			}
			condition = strings.TrimSpace(remaining[:elseIdx])
			falseExpr = strings.TrimSpace(remaining[elseIdx+6:]) // len(" else ") == 6
			return
		}
	}
	return "", "", ""
}

// findElse finds the index of " else " in text, not inside quotes or parens.
func findElse(text string) int {
	depth := 0
	inQuote := byte(0)

	for i := 0; i < len(text); i++ {
		ch := text[i]
		if inQuote != 0 {
			if ch == '\\' && i+1 < len(text) {
				i++
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			continue
		}
		if depth == 0 && i+6 <= len(text) && text[i:i+6] == " else " {
			return i
		}
	}
	return -1
}

// findMatchingParen finds the closing ) for the ( at position start.
func findMatchingParen(text string, start int) int {
	depth := 0
	inQuote := byte(0)

	for i := start; i < len(text); i++ {
		ch := text[i]
		if inQuote != 0 {
			if ch == '\\' && i+1 < len(text) {
				i++
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// findTagEnd finds the position of closer (}} or %}) starting from pos,
// respecting string literals.
func findTagEnd(template string, pos int, closer string) int {
	inQuote := byte(0)
	for i := pos; i < len(template)-1; i++ {
		ch := template[i]
		if inQuote != 0 {
			if ch == '\\' && i+1 < len(template) {
				i++
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inQuote = ch
			continue
		}
		if template[i:i+2] == closer {
			return i
		}
	}
	return -1
}

// loadSpecialTokens reads special tokens from tokenizer_config.json.
func loadSpecialTokens(modelPath string) (bos, eos, unk, pad string) {
	configPath := filepath.Join(modelPath, "tokenizer_config.json")
	data, err := os.ReadFile(configPath) //nolint:gosec // modelPath is a trusted internal path, not user input
	if err != nil {
		return "", "", "", ""
	}

	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		return "", "", "", ""
	}

	bos = extractToken(config, "bos_token")
	eos = extractToken(config, "eos_token")
	unk = extractToken(config, "unk_token")
	pad = extractToken(config, "pad_token")
	return
}

// extractToken extracts a token string from config. Tokens can be strings
// or objects with a "content" field.
func extractToken(config map[string]any, key string) string {
	v, ok := config[key]
	if !ok {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case map[string]any:
		if content, ok := val["content"].(string); ok {
			return content
		}
	}
	return ""
}
