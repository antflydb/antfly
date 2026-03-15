package antflyevalaf

import (
	"github.com/antflydb/antfly/pkg/evalaf/rag"
)

// NewCitationEvaluator creates a citation evaluator configured for Antfly's format.
// Antfly uses the format: [resource_id 0] or [resource_id 0, 1, 2]
func NewCitationEvaluator(name string) *rag.CitationEvaluator {
	if name == "" {
		name = "antfly_citation"
	}
	return rag.NewCitationEvaluator(name)
}

// NewCitationCoverageEvaluator creates a citation coverage evaluator for Antfly.
func NewCitationCoverageEvaluator(name string) *rag.CitationCoverageEvaluator {
	if name == "" {
		name = "antfly_citation_coverage"
	}
	return rag.NewCitationCoverageEvaluator(name)
}
