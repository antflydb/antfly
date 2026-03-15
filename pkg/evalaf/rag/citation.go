package rag

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
)

// CitationEvaluator validates citation accuracy in RAG outputs.
// It checks if citations like [resource_id 0] or [resource_id 0, 1] are valid.
type CitationEvaluator struct {
	name    string
	pattern *regexp.Regexp
}

// NewCitationEvaluator creates a new citation evaluator.
// The default pattern matches citations like [resource_id X] or [resource_id X, Y, Z].
func NewCitationEvaluator(name string) *CitationEvaluator {
	if name == "" {
		name = "citation_accuracy"
	}

	// Default pattern for Antfly-style citations
	pattern := regexp.MustCompile(`\[resource_id\s+([^\]]+)\]`)

	return &CitationEvaluator{
		name:    name,
		pattern: pattern,
	}
}

// NewCitationEvaluatorWithPattern creates a citation evaluator with a custom pattern.
func NewCitationEvaluatorWithPattern(name, patternStr string) (*CitationEvaluator, error) {
	if name == "" {
		name = "citation_accuracy"
	}

	pattern, err := regexp.Compile(patternStr)
	if err != nil {
		return nil, fmt.Errorf("invalid citation pattern: %w", err)
	}

	return &CitationEvaluator{
		name:    name,
		pattern: pattern,
	}, nil
}

// Name returns the evaluator name.
func (e *CitationEvaluator) Name() string {
	return e.name
}

// Evaluate checks citation accuracy.
func (e *CitationEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	outputStr := fmt.Sprintf("%v", input.Output)

	// Find all citations
	matches := e.pattern.FindAllStringSubmatch(outputStr, -1)

	if len(matches) == 0 {
		// No citations found - this might be okay or a failure depending on context
		return &eval.EvalResult{
			Pass:   true, // Pass by default if no citations needed
			Score:  1.0,
			Reason: "No citations found in output",
			Metadata: map[string]any{
				"citation_count": 0,
			},
		}, nil
	}

	// Extract cited document IDs
	citedIDs := make(map[int]bool)
	invalidCitations := []string{}

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		// Parse the citation content (e.g., "0" or "0, 1, 2")
		citationContent := strings.TrimSpace(match[1])
		parts := strings.SplitSeq(citationContent, ",")

		for part := range parts {
			part = strings.TrimSpace(part)
			id, err := strconv.Atoi(part)
			if err != nil {
				invalidCitations = append(invalidCitations, match[0])
				continue
			}
			citedIDs[id] = true
		}
	}

	// Check if cited IDs are valid (within context range)
	contextLen := len(input.Context)
	invalidIDs := []int{}

	for id := range citedIDs {
		if id < 0 || id >= contextLen {
			invalidIDs = append(invalidIDs, id)
		}
	}

	// Calculate score
	totalCitations := len(citedIDs)
	validCitations := totalCitations - len(invalidIDs)

	var score float64
	if totalCitations > 0 {
		score = float64(validCitations) / float64(totalCitations)
	} else {
		score = 1.0
	}

	pass := len(invalidCitations) == 0 && len(invalidIDs) == 0

	reason := fmt.Sprintf("Found %d citation(s)", len(matches))
	if len(invalidCitations) > 0 {
		reason += fmt.Sprintf(", %d invalid format", len(invalidCitations))
	}
	if len(invalidIDs) > 0 {
		reason += fmt.Sprintf(", %d out of range (context has %d documents)", len(invalidIDs), contextLen)
	}
	if pass {
		reason += " - all valid"
	}

	return &eval.EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
		Metadata: map[string]any{
			"citation_count":    len(matches),
			"cited_doc_ids":     getKeys(citedIDs),
			"invalid_citations": invalidCitations,
			"out_of_range_ids":  invalidIDs,
			"context_doc_count": contextLen,
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *CitationEvaluator) SupportsStreaming() bool {
	return false
}

// getKeys returns the keys of a map as a slice.
func getKeys(m map[int]bool) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// CitationCoverageEvaluator checks if all context documents are cited.
type CitationCoverageEvaluator struct {
	name    string
	pattern *regexp.Regexp
}

// NewCitationCoverageEvaluator creates an evaluator that checks citation coverage.
// It verifies that all provided context documents are cited in the output.
func NewCitationCoverageEvaluator(name string) *CitationCoverageEvaluator {
	if name == "" {
		name = "citation_coverage"
	}

	pattern := regexp.MustCompile(`\[resource_id\s+([^\]]+)\]`)

	return &CitationCoverageEvaluator{
		name:    name,
		pattern: pattern,
	}
}

// NewCitationCoverageEvaluatorWithPattern creates a coverage evaluator with a custom pattern.
func NewCitationCoverageEvaluatorWithPattern(name, patternStr string) (*CitationCoverageEvaluator, error) {
	if name == "" {
		name = "citation_coverage"
	}

	pattern, err := regexp.Compile(patternStr)
	if err != nil {
		return nil, fmt.Errorf("invalid citation pattern: %w", err)
	}

	return &CitationCoverageEvaluator{
		name:    name,
		pattern: pattern,
	}, nil
}

// Name returns the evaluator name.
func (e *CitationCoverageEvaluator) Name() string {
	return e.name
}

// Evaluate checks citation coverage.
func (e *CitationCoverageEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	outputStr := fmt.Sprintf("%v", input.Output)
	contextLen := len(input.Context)

	if contextLen == 0 {
		return &eval.EvalResult{
			Pass:   true,
			Score:  1.0,
			Reason: "No context documents to cite",
		}, nil
	}

	// Find all citations
	matches := e.pattern.FindAllStringSubmatch(outputStr, -1)

	// Extract cited document IDs
	citedIDs := make(map[int]bool)

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		citationContent := strings.TrimSpace(match[1])
		for part := range strings.SplitSeq(citationContent, ",") {
			part = strings.TrimSpace(part)
			id, err := strconv.Atoi(part)
			if err != nil {
				continue
			}
			if id >= 0 && id < contextLen {
				citedIDs[id] = true
			}
		}
	}

	// Calculate coverage
	coverage := float64(len(citedIDs)) / float64(contextLen)
	pass := coverage >= 0.8 // At least 80% of documents should be cited

	uncitedIDs := []int{}
	for i := range contextLen {
		if !citedIDs[i] {
			uncitedIDs = append(uncitedIDs, i)
		}
	}

	reason := fmt.Sprintf("Cited %d/%d documents (%.1f%% coverage)", len(citedIDs), contextLen, coverage*100)
	if len(uncitedIDs) > 0 {
		reason += fmt.Sprintf(", uncited: %v", uncitedIDs)
	}

	return &eval.EvalResult{
		Pass:   pass,
		Score:  coverage,
		Reason: reason,
		Metadata: map[string]any{
			"cited_count":   len(citedIDs),
			"context_count": contextLen,
			"coverage":      coverage,
			"cited_ids":     getKeys(citedIDs),
			"uncited_ids":   uncitedIDs,
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *CitationCoverageEvaluator) SupportsStreaming() bool {
	return false
}
