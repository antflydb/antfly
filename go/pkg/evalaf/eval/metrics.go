package eval

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// ExactMatchEvaluator checks if output exactly matches the reference.
type ExactMatchEvaluator struct {
	name string
}

// NewExactMatchEvaluator creates a new exact match evaluator.
func NewExactMatchEvaluator(name string) *ExactMatchEvaluator {
	if name == "" {
		name = "exact_match"
	}
	return &ExactMatchEvaluator{name: name}
}

// Name returns the evaluator name.
func (e *ExactMatchEvaluator) Name() string {
	return e.name
}

// Evaluate performs exact match evaluation.
func (e *ExactMatchEvaluator) Evaluate(ctx context.Context, input EvalInput) (*EvalResult, error) {
	if input.Reference == nil {
		return nil, fmt.Errorf("reference is required for exact match evaluation")
	}

	// Convert both to strings for comparison
	outputStr := fmt.Sprintf("%v", input.Output)
	refStr := fmt.Sprintf("%v", input.Reference)

	pass := outputStr == refStr
	score := 0.0
	if pass {
		score = 1.0
	}

	reason := "Output matches reference exactly"
	if !pass {
		reason = fmt.Sprintf("Output does not match reference (got %q, expected %q)", outputStr, refStr)
	}

	return &EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
	}, nil
}

// SupportsStreaming returns false.
func (e *ExactMatchEvaluator) SupportsStreaming() bool {
	return false
}

// RegexEvaluator checks if output matches a regex pattern in the reference.
type RegexEvaluator struct {
	name    string
	pattern string
	regex   *regexp.Regexp
}

// NewRegexEvaluator creates a new regex evaluator.
func NewRegexEvaluator(name string, config EvaluatorConfig) (*RegexEvaluator, error) {
	if name == "" {
		name = "regex"
	}

	pattern := config.Pattern
	if pattern == "" && config.Custom != nil {
		if p, ok := config.Custom["pattern"].(string); ok {
			pattern = p
		}
	}

	if pattern == "" {
		return nil, fmt.Errorf("pattern is required for regex evaluator")
	}

	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	return &RegexEvaluator{
		name:    name,
		pattern: pattern,
		regex:   regex,
	}, nil
}

// Name returns the evaluator name.
func (e *RegexEvaluator) Name() string {
	return e.name
}

// Evaluate performs regex matching evaluation.
func (e *RegexEvaluator) Evaluate(ctx context.Context, input EvalInput) (*EvalResult, error) {
	// Use reference as pattern if provided, otherwise use configured pattern
	pattern := e.pattern
	if input.Reference != nil {
		if refStr, ok := input.Reference.(string); ok && refStr != "" {
			// Recompile with reference pattern
			regex, err := regexp.Compile(refStr)
			if err != nil {
				return &EvalResult{
					Pass:  false,
					Score: 0.0,
					Error: fmt.Errorf("invalid regex pattern in reference: %w", err),
				}, nil
			}
			pattern = refStr
			outputStr := fmt.Sprintf("%v", input.Output)
			pass := regex.MatchString(outputStr)
			score := 0.0
			if pass {
				score = 1.0
			}

			reason := fmt.Sprintf("Output matches pattern %q", pattern)
			if !pass {
				reason = fmt.Sprintf("Output does not match pattern %q", pattern)
			}

			return &EvalResult{
				Pass:   pass,
				Score:  score,
				Reason: reason,
			}, nil
		}
	}

	// Use configured pattern
	outputStr := fmt.Sprintf("%v", input.Output)
	pass := e.regex.MatchString(outputStr)
	score := 0.0
	if pass {
		score = 1.0
	}

	reason := fmt.Sprintf("Output matches pattern %q", pattern)
	if !pass {
		reason = fmt.Sprintf("Output does not match pattern %q", pattern)
	}

	return &EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
	}, nil
}

// SupportsStreaming returns false.
func (e *RegexEvaluator) SupportsStreaming() bool {
	return false
}

// ContainsEvaluator checks if output contains the reference substring.
type ContainsEvaluator struct {
	name            string
	caseInsensitive bool
}

// NewContainsEvaluator creates a new contains evaluator.
func NewContainsEvaluator(name string, caseInsensitive bool) *ContainsEvaluator {
	if name == "" {
		name = "contains"
	}
	return &ContainsEvaluator{
		name:            name,
		caseInsensitive: caseInsensitive,
	}
}

// Name returns the evaluator name.
func (e *ContainsEvaluator) Name() string {
	return e.name
}

// Evaluate performs contains evaluation.
func (e *ContainsEvaluator) Evaluate(ctx context.Context, input EvalInput) (*EvalResult, error) {
	if input.Reference == nil {
		return nil, fmt.Errorf("reference is required for contains evaluation")
	}

	outputStr := fmt.Sprintf("%v", input.Output)
	refStr := fmt.Sprintf("%v", input.Reference)

	if e.caseInsensitive {
		outputStr = strings.ToLower(outputStr)
		refStr = strings.ToLower(refStr)
	}

	pass := strings.Contains(outputStr, refStr)
	score := 0.0
	if pass {
		score = 1.0
	}

	reason := fmt.Sprintf("Output contains %q", input.Reference)
	if !pass {
		reason = fmt.Sprintf("Output does not contain %q", input.Reference)
	}

	return &EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
	}, nil
}

// SupportsStreaming returns false.
func (e *ContainsEvaluator) SupportsStreaming() bool {
	return false
}

// LevenshteinDistance computes the Levenshtein distance between two strings.
func LevenshteinDistance(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// Create distance matrix
	matrix := make([][]int, len(s1)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(s2)+1)
		matrix[i][0] = i
	}
	for j := 0; j <= len(s2); j++ {
		matrix[0][j] = j
	}

	// Compute distance
	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}

			matrix[i][j] = min(
				matrix[i-1][j]+1,      // deletion
				matrix[i][j-1]+1,      // insertion
				matrix[i-1][j-1]+cost, // substitution
			)
		}
	}

	return matrix[len(s1)][len(s2)]
}

func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// FuzzyMatchEvaluator checks if output approximately matches reference using Levenshtein distance.
type FuzzyMatchEvaluator struct {
	name      string
	threshold float64 // similarity threshold (0-1), where 1 is exact match
}

// NewFuzzyMatchEvaluator creates a new fuzzy match evaluator.
func NewFuzzyMatchEvaluator(name string, threshold float64) *FuzzyMatchEvaluator {
	if name == "" {
		name = "fuzzy_match"
	}
	if threshold <= 0 || threshold > 1 {
		threshold = 0.8 // default 80% similarity
	}
	return &FuzzyMatchEvaluator{
		name:      name,
		threshold: threshold,
	}
}

// Name returns the evaluator name.
func (e *FuzzyMatchEvaluator) Name() string {
	return e.name
}

// Evaluate performs fuzzy matching evaluation.
func (e *FuzzyMatchEvaluator) Evaluate(ctx context.Context, input EvalInput) (*EvalResult, error) {
	if input.Reference == nil {
		return nil, fmt.Errorf("reference is required for fuzzy match evaluation")
	}

	outputStr := fmt.Sprintf("%v", input.Output)
	refStr := fmt.Sprintf("%v", input.Reference)

	distance := LevenshteinDistance(outputStr, refStr)
	maxLen := max(len(refStr), len(outputStr))

	var similarity float64
	if maxLen == 0 {
		similarity = 1.0
	} else {
		similarity = 1.0 - float64(distance)/float64(maxLen)
	}

	pass := similarity >= e.threshold
	score := similarity

	reason := fmt.Sprintf("Similarity: %.2f%% (threshold: %.2f%%)", similarity*100, e.threshold*100)

	return &EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
		Metadata: map[string]any{
			"similarity":           similarity,
			"levenshtein_distance": distance,
			"threshold":            e.threshold,
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *FuzzyMatchEvaluator) SupportsStreaming() bool {
	return false
}
