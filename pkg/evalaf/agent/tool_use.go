package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
)

// ToolSelectionEvaluator evaluates if the agent selected the correct tool(s).
type ToolSelectionEvaluator struct {
	name string
}

// NewToolSelectionEvaluator creates a new tool selection evaluator.
func NewToolSelectionEvaluator(name string) *ToolSelectionEvaluator {
	if name == "" {
		name = "tool_selection"
	}

	return &ToolSelectionEvaluator{
		name: name,
	}
}

// Name returns the evaluator name.
func (e *ToolSelectionEvaluator) Name() string {
	return e.name
}

// Evaluate checks if the correct tool(s) were selected.
// Expects:
// - input.Reference: expected tool name(s) (string or []string)
// - input.Output: actual tool selection (string, []string, or map with "tool" or "tools" key)
func (e *ToolSelectionEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	if input.Reference == nil {
		return nil, fmt.Errorf("reference tool selection is required")
	}

	// Extract expected tools
	expectedTools := normalizeTools(input.Reference)
	if len(expectedTools) == 0 {
		return nil, fmt.Errorf("no expected tools specified")
	}

	// Extract actual tools
	actualTools := normalizeTools(input.Output)
	if len(actualTools) == 0 {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0.0,
			Reason: "No tools selected",
			Metadata: map[string]any{
				"expected_tools": expectedTools,
				"actual_tools":   actualTools,
			},
		}, nil
	}

	// Check for exact match
	exactMatch := setsEqual(expectedTools, actualTools)

	// Calculate overlap score
	overlap := setIntersection(expectedTools, actualTools)
	score := float64(len(overlap)) / float64(len(expectedTools))

	pass := exactMatch

	reason := fmt.Sprintf("Expected: %v, Got: %v", expectedTools, actualTools)
	if exactMatch {
		reason += " ✓"
	} else if len(overlap) > 0 {
		reason += fmt.Sprintf(" (partial match: %v)", overlap)
	} else {
		reason += " ✗"
	}

	return &eval.EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
		Metadata: map[string]any{
			"expected_tools": expectedTools,
			"actual_tools":   actualTools,
			"overlap":        overlap,
			"exact_match":    exactMatch,
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *ToolSelectionEvaluator) SupportsStreaming() bool {
	return false
}

// ToolCallSequenceEvaluator evaluates if tools were called in the correct sequence.
type ToolCallSequenceEvaluator struct {
	name string
}

// NewToolCallSequenceEvaluator creates a tool call sequence evaluator.
func NewToolCallSequenceEvaluator(name string) *ToolCallSequenceEvaluator {
	if name == "" {
		name = "tool_sequence"
	}

	return &ToolCallSequenceEvaluator{
		name: name,
	}
}

// Name returns the evaluator name.
func (e *ToolCallSequenceEvaluator) Name() string {
	return e.name
}

// Evaluate checks if tools were called in the correct sequence.
// Expects:
// - input.Reference: expected sequence ([]string)
// - input.Output: actual sequence ([]string or map with "tool_sequence" key)
func (e *ToolCallSequenceEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	expectedSeq, err := extractSequence(input.Reference)
	if err != nil {
		return nil, fmt.Errorf("invalid reference sequence: %w", err)
	}

	actualSeq, err := extractSequence(input.Output)
	if err != nil {
		return nil, fmt.Errorf("invalid output sequence: %w", err)
	}

	// Check for exact sequence match
	exactMatch := sequencesEqual(expectedSeq, actualSeq)

	// Calculate sequence similarity (longest common subsequence)
	lcs := longestCommonSubsequence(expectedSeq, actualSeq)
	var score float64
	if len(expectedSeq) > 0 {
		score = float64(len(lcs)) / float64(len(expectedSeq))
	}

	pass := exactMatch

	reason := fmt.Sprintf("Expected sequence: %v, Got: %v", expectedSeq, actualSeq)
	if exactMatch {
		reason += " ✓"
	} else {
		reason += fmt.Sprintf(" (similarity: %.2f)", score)
	}

	return &eval.EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
		Metadata: map[string]any{
			"expected_sequence": expectedSeq,
			"actual_sequence":   actualSeq,
			"lcs":               lcs,
			"exact_match":       exactMatch,
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *ToolCallSequenceEvaluator) SupportsStreaming() bool {
	return false
}

// Helper functions

// normalizeTools converts various formats to a normalized string slice.
func normalizeTools(value any) []string {
	switch v := value.(type) {
	case string:
		return []string{strings.TrimSpace(v)}

	case []string:
		result := make([]string, 0, len(v))
		for _, tool := range v {
			if t := strings.TrimSpace(tool); t != "" {
				result = append(result, t)
			}
		}
		return result

	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				if t := strings.TrimSpace(s); t != "" {
					result = append(result, t)
				}
			}
		}
		return result

	case map[string]any:
		// Try to extract from common keys
		if tool, ok := v["tool"].(string); ok {
			return []string{strings.TrimSpace(tool)}
		}
		if tools := normalizeTools(v["tools"]); len(tools) > 0 {
			return tools
		}
		if tools := normalizeTools(v["selected_tools"]); len(tools) > 0 {
			return tools
		}
	}

	return []string{}
}

// extractSequence extracts a sequence from various formats.
func extractSequence(value any) ([]string, error) {
	switch v := value.(type) {
	case []string:
		return v, nil

	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			} else {
				return nil, fmt.Errorf("sequence contains non-string element: %T", item)
			}
		}
		return result, nil

	case map[string]any:
		if seq, ok := v["tool_sequence"]; ok {
			return extractSequence(seq)
		}
		if seq, ok := v["sequence"]; ok {
			return extractSequence(seq)
		}
		return nil, fmt.Errorf("map does not contain 'tool_sequence' or 'sequence' key")

	default:
		return nil, fmt.Errorf("value must be a sequence ([]string or []any), got %T", value)
	}
}

// setsEqual checks if two string slices contain the same elements (order-independent).
func setsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	setA := make(map[string]bool, len(a))
	for _, item := range a {
		setA[item] = true
	}

	for _, item := range b {
		if !setA[item] {
			return false
		}
	}

	return true
}

// setIntersection returns the intersection of two string slices.
func setIntersection(a, b []string) []string {
	setA := make(map[string]bool, len(a))
	for _, item := range a {
		setA[item] = true
	}

	result := []string{}
	seen := make(map[string]bool)
	for _, item := range b {
		if setA[item] && !seen[item] {
			result = append(result, item)
			seen[item] = true
		}
	}

	return result
}

// sequencesEqual checks if two sequences are exactly equal.
func sequencesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// longestCommonSubsequence finds the longest common subsequence of two sequences.
func longestCommonSubsequence(a, b []string) []string {
	m, n := len(a), len(b)
	if m == 0 || n == 0 {
		return []string{}
	}

	// Create DP table
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	// Fill DP table
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				dp[i][j] = max(dp[i-1][j], dp[i][j-1])
			}
		}
	}

	// Reconstruct LCS
	lcs := []string{}
	i, j := m, n
	for i > 0 && j > 0 {
		if a[i-1] == b[j-1] {
			lcs = append([]string{a[i-1]}, lcs...)
			i--
			j--
		} else if dp[i-1][j] > dp[i][j-1] {
			i--
		} else {
			j--
		}
	}

	return lcs
}
