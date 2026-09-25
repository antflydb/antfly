package antflyevalaf

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/antflydb/antfly/go/pkg/evalaf/eval"
	antfly "github.com/antflydb/antfly/go/pkg/sdk"
)

// researchResult extracts an *antfly.ResearchAgentResult from an evaluator's
// output, accepting either the struct or a pointer to it so target functions
// can return either shape.
func researchResult(output any) (*antfly.ResearchAgentResult, bool) {
	switch v := output.(type) {
	case *antfly.ResearchAgentResult:
		return v, v != nil
	case antfly.ResearchAgentResult:
		return &v, true
	default:
		return nil, false
	}
}

// ResearchCitationCoverageEvaluator measures the share of report sections that
// carry at least one resolvable citation. It trusts the server's own
// verification pass (verification.uncited_sections) when the run enabled
// steps.verify, and otherwise derives coverage from the citations list.
type ResearchCitationCoverageEvaluator struct {
	name        string
	minCoverage float64
}

// NewResearchCitationCoverageEvaluator creates a citation coverage evaluator for
// the research agent. minCoverage defaults to 0.8 (at least 80% of report
// sections must carry a resolvable citation).
func NewResearchCitationCoverageEvaluator(name string, minCoverage float64) *ResearchCitationCoverageEvaluator {
	if name == "" {
		name = "research_citation_coverage"
	}
	if minCoverage <= 0 {
		minCoverage = 0.8
	}
	return &ResearchCitationCoverageEvaluator{name: name, minCoverage: minCoverage}
}

// Name returns the evaluator name.
func (e *ResearchCitationCoverageEvaluator) Name() string { return e.name }

// SupportsStreaming reports that this evaluator only runs on complete outputs.
func (e *ResearchCitationCoverageEvaluator) SupportsStreaming() bool { return false }

// Evaluate checks report section citation coverage.
func (e *ResearchCitationCoverageEvaluator) Evaluate(_ context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	result, ok := researchResult(input.Output)
	if !ok {
		return nil, fmt.Errorf("research citation coverage: output must be *antfly.ResearchAgentResult, got %T", input.Output)
	}

	totalSections := len(result.Report.Sections)
	if totalSections == 0 {
		return &eval.EvalResult{Pass: true, Score: 1.0, Reason: "no report sections to cite"}, nil
	}

	verification := result.Verification
	var uncited int
	switch {
	case verification.CheckedSections > 0:
		// The server's verify step already flagged sections with no
		// resolvable citation; trust it and use its section count.
		totalSections = verification.CheckedSections
		uncited = len(verification.UncitedSections)
	default:
		// No verify step ran: derive coverage directly from the citations list.
		cited := make(map[int]bool, len(result.Citations))
		for _, c := range result.Citations {
			if c.SectionIndex >= 0 {
				cited[c.SectionIndex] = true
			}
		}
		for i := range totalSections {
			if !cited[i] {
				uncited++
			}
		}
	}

	citedSections := totalSections - uncited
	coverage := float64(citedSections) / float64(totalSections)
	reason := fmt.Sprintf("%d/%d sections cited (%.1f%% coverage)", citedSections, totalSections, coverage*100)
	if len(verification.UnresolvedMarkers) > 0 {
		reason += fmt.Sprintf("; %d unresolved citation markers removed", len(verification.UnresolvedMarkers))
	}

	return &eval.EvalResult{
		Pass:   coverage >= e.minCoverage,
		Score:  coverage,
		Reason: reason,
		Metadata: map[string]any{
			"total_sections":     totalSections,
			"uncited_sections":   uncited,
			"unresolved_markers": len(verification.UnresolvedMarkers),
		},
	}, nil
}

// ResearchCitationPrecisionEvaluator measures how well the report's citations
// are actually supported by the evidence they point to. It prefers the
// server's own verify step (verification.supported_ratio); when verification
// did not run, it falls back to the citation resolution rate implied by
// markers the server had to strip because they never resolved to evidence.
type ResearchCitationPrecisionEvaluator struct {
	name         string
	minPrecision float64
}

// NewResearchCitationPrecisionEvaluator creates a citation precision evaluator
// for the research agent. minPrecision defaults to 0.7.
func NewResearchCitationPrecisionEvaluator(name string, minPrecision float64) *ResearchCitationPrecisionEvaluator {
	if name == "" {
		name = "research_citation_precision"
	}
	if minPrecision <= 0 {
		minPrecision = 0.7
	}
	return &ResearchCitationPrecisionEvaluator{name: name, minPrecision: minPrecision}
}

// Name returns the evaluator name.
func (e *ResearchCitationPrecisionEvaluator) Name() string { return e.name }

// SupportsStreaming reports that this evaluator only runs on complete outputs.
func (e *ResearchCitationPrecisionEvaluator) SupportsStreaming() bool { return false }

// Evaluate checks citation precision.
func (e *ResearchCitationPrecisionEvaluator) Evaluate(_ context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	result, ok := researchResult(input.Output)
	if !ok {
		return nil, fmt.Errorf("research citation precision: output must be *antfly.ResearchAgentResult, got %T", input.Output)
	}

	verification := result.Verification

	var (
		precision float64
		source    string
	)
	if verification.CheckedSections > 0 {
		precision = float64(verification.SupportedRatio)
		source = "verification.supported_ratio"
	} else {
		resolved := len(result.Citations)
		unresolved := len(verification.UnresolvedMarkers)
		total := resolved + unresolved
		if total == 0 {
			return &eval.EvalResult{Pass: true, Score: 1.0, Reason: "no citations to evaluate"}, nil
		}
		precision = float64(resolved) / float64(total)
		source = "citation resolution rate (no verify step)"
	}

	return &eval.EvalResult{
		Pass:   precision >= e.minPrecision,
		Score:  precision,
		Reason: fmt.Sprintf("citation precision %.1f%% via %s", precision*100, source),
		Metadata: map[string]any{
			"unsupported_claims": len(verification.Unsupported),
		},
	}, nil
}

// ResearchSubQuestionCoverageEvaluator measures how many of the planner's
// sub-questions were actually researched, as opposed to left pending, failed,
// or skipped because a budget limit was hit first.
type ResearchSubQuestionCoverageEvaluator struct {
	name        string
	minCoverage float64
}

// NewResearchSubQuestionCoverageEvaluator creates a sub-question coverage
// evaluator for the research agent. minCoverage defaults to 0.8.
func NewResearchSubQuestionCoverageEvaluator(name string, minCoverage float64) *ResearchSubQuestionCoverageEvaluator {
	if name == "" {
		name = "research_sub_question_coverage"
	}
	if minCoverage <= 0 {
		minCoverage = 0.8
	}
	return &ResearchSubQuestionCoverageEvaluator{name: name, minCoverage: minCoverage}
}

// Name returns the evaluator name.
func (e *ResearchSubQuestionCoverageEvaluator) Name() string { return e.name }

// SupportsStreaming reports that this evaluator only runs on complete outputs.
func (e *ResearchSubQuestionCoverageEvaluator) SupportsStreaming() bool { return false }

// Evaluate checks planned-vs-researched sub-question coverage.
func (e *ResearchSubQuestionCoverageEvaluator) Evaluate(_ context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	result, ok := researchResult(input.Output)
	if !ok {
		return nil, fmt.Errorf("research sub-question coverage: output must be *antfly.ResearchAgentResult, got %T", input.Output)
	}

	planned := result.Plan.SubQuestions
	if len(planned) == 0 {
		return &eval.EvalResult{Pass: true, Score: 1.0, Reason: "no planned sub-questions"}, nil
	}

	var researched, failed, skipped, pending int
	for _, sq := range planned {
		switch sq.Status {
		case "researched":
			researched++
		case "failed":
			failed++
		case "skipped":
			skipped++
		default:
			pending++
		}
	}

	coverage := float64(researched) / float64(len(planned))
	return &eval.EvalResult{
		Pass:  coverage >= e.minCoverage,
		Score: coverage,
		Reason: fmt.Sprintf("%d/%d sub-questions researched (%d failed, %d skipped, %d pending)",
			researched, len(planned), failed, skipped, pending),
		Metadata: map[string]any{
			"planned":    len(planned),
			"researched": researched,
			"failed":     failed,
			"skipped":    skipped,
			"pending":    pending,
			"findings":   len(result.Findings),
		},
	}, nil
}

// ResearchEvidenceDiversityEvaluator measures how varied the deduplicated
// evidence registry is: distinct table/doc_id pairs for table evidence and
// distinct domains for web/fetch evidence, as a share of total evidence
// retained. Low diversity often means every sub-question converged on the
// same handful of sources instead of triangulating independent ones.
type ResearchEvidenceDiversityEvaluator struct {
	name         string
	minDiversity float64
}

// NewResearchEvidenceDiversityEvaluator creates an evidence diversity
// evaluator for the research agent. minDiversity defaults to 0.5.
func NewResearchEvidenceDiversityEvaluator(name string, minDiversity float64) *ResearchEvidenceDiversityEvaluator {
	if name == "" {
		name = "research_evidence_diversity"
	}
	if minDiversity <= 0 {
		minDiversity = 0.5
	}
	return &ResearchEvidenceDiversityEvaluator{name: name, minDiversity: minDiversity}
}

// Name returns the evaluator name.
func (e *ResearchEvidenceDiversityEvaluator) Name() string { return e.name }

// SupportsStreaming reports that this evaluator only runs on complete outputs.
func (e *ResearchEvidenceDiversityEvaluator) SupportsStreaming() bool { return false }

// Evaluate checks evidence source diversity.
func (e *ResearchEvidenceDiversityEvaluator) Evaluate(_ context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	result, ok := researchResult(input.Output)
	if !ok {
		return nil, fmt.Errorf("research evidence diversity: output must be *antfly.ResearchAgentResult, got %T", input.Output)
	}

	if len(result.Evidence) == 0 {
		return &eval.EvalResult{Pass: false, Score: 0, Reason: "no evidence retrieved"}, nil
	}

	distinctDocs := make(map[string]bool, len(result.Evidence))
	distinctDomains := make(map[string]bool, len(result.Evidence))
	for _, ev := range result.Evidence {
		switch ev.Source {
		case "table":
			distinctDocs[ev.Table+"/"+ev.DocId] = true
		case "web", "fetch":
			if domain := evidenceDomain(ev.Url); domain != "" {
				distinctDomains[domain] = true
			} else {
				distinctDocs[ev.Url] = true
			}
		default:
			distinctDocs[ev.Id] = true
		}
	}

	distinct := len(distinctDocs) + len(distinctDomains)
	diversity := float64(distinct) / float64(len(result.Evidence))

	return &eval.EvalResult{
		Pass:  diversity >= e.minDiversity,
		Score: diversity,
		Reason: fmt.Sprintf("%d distinct sources across %d evidence items (%d tables/docs, %d domains)",
			distinct, len(result.Evidence), len(distinctDocs), len(distinctDomains)),
		Metadata: map[string]any{
			"evidence_count":   len(result.Evidence),
			"distinct_docs":    len(distinctDocs),
			"distinct_domains": len(distinctDomains),
		},
	}, nil
}

// evidenceDomain extracts a lowercased hostname from a URL for grouping web
// and fetched evidence, returning "" when the URL is empty or unparsable.
func evidenceDomain(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Hostname())
}
