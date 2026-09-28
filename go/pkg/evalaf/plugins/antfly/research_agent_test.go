package antflyevalaf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/evalaf/eval"
	antfly "github.com/antflydb/antfly/go/pkg/sdk"
)

func TestCallResearchAgentForcesNonStreaming(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/db/v1/agents/research" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		var req antfly.ResearchAgentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Stream {
			t.Error("expected stream to be forced false")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(antfly.ResearchAgentResult{
			Status: "completed",
			Report: antfly.ResearchReport{Markdown: "# Report"},
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	result, err := client.CallResearchAgent(context.Background(), antfly.ResearchAgentRequest{
		Query:  "how does antfly rerank?",
		Stream: true, // should be overridden to false
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Report.Markdown != "# Report" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestCreateResearchAgentTargetFunc(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(antfly.ResearchAgentResult{Status: "completed"})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	fn := client.CreateResearchAgentTargetFunc([]string{"docs"})
	if fn == nil {
		t.Fatal("expected non-nil target func")
	}

	out, err := fn(context.Background(), eval.Example{Input: "how does antfly rerank?"})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := out.(*antfly.ResearchAgentResult)
	if !ok {
		t.Fatalf("expected *antfly.ResearchAgentResult, got %T", out)
	}
	if result.Status != "completed" {
		t.Errorf("unexpected result: %+v", result)
	}

	if _, err := fn(context.Background(), eval.Example{Input: 42}); err == nil {
		t.Fatal("expected error for non-string input")
	}
}

func reportWithSections(n int) antfly.ResearchReport {
	sections := make([]antfly.ResearchReportSection, n)
	for i := range sections {
		sections[i] = antfly.ResearchReportSection{Heading: "Section", Markdown: "body [E1]"}
	}
	return antfly.ResearchReport{Markdown: "# Report", Sections: sections}
}

func TestResearchCitationCoverageEvaluator(t *testing.T) {
	e := NewResearchCitationCoverageEvaluator("", 0)
	if e.Name() != "research_citation_coverage" {
		t.Errorf("unexpected name %q", e.Name())
	}
	if e.SupportsStreaming() {
		t.Error("expected SupportsStreaming to be false")
	}

	t.Run("no sections", func(t *testing.T) {
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: &antfly.ResearchAgentResult{}})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Pass || result.Score != 1.0 {
			t.Errorf("unexpected result: %+v", result)
		}
	})

	t.Run("uses verification when present", func(t *testing.T) {
		res := &antfly.ResearchAgentResult{
			Report: reportWithSections(4),
			Verification: antfly.ResearchVerification{
				CheckedSections: 4,
				UncitedSections: []int{2},
			},
		}
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: res})
		if err != nil {
			t.Fatal(err)
		}
		if result.Score != 0.75 {
			t.Errorf("expected score 0.75, got %v", result.Score)
		}
		if result.Pass {
			t.Errorf("expected fail below 0.8 threshold, got pass with score %v", result.Score)
		}
	})

	t.Run("falls back to citations without verification", func(t *testing.T) {
		res := &antfly.ResearchAgentResult{
			Report: reportWithSections(2),
			Citations: []antfly.ResearchCitation{
				{Marker: "[E1]", EvidenceId: "E1", SectionIndex: 0},
			},
		}
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: res})
		if err != nil {
			t.Fatal(err)
		}
		if result.Score != 0.5 {
			t.Errorf("expected score 0.5, got %v", result.Score)
		}
	})

	t.Run("wrong output type", func(t *testing.T) {
		if _, err := e.Evaluate(context.Background(), eval.EvalInput{Output: "not a result"}); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestResearchCitationPrecisionEvaluator(t *testing.T) {
	e := NewResearchCitationPrecisionEvaluator("", 0)

	t.Run("uses supported_ratio when verification ran", func(t *testing.T) {
		res := &antfly.ResearchAgentResult{
			Verification: antfly.ResearchVerification{
				CheckedSections: 3,
				SupportedRatio:  0.9,
			},
		}
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: res})
		if err != nil {
			t.Fatal(err)
		}
		if result.Score != float64(float32(0.9)) {
			t.Errorf("expected score ~0.9, got %v", result.Score)
		}
		if !result.Pass {
			t.Errorf("expected pass, got %+v", result)
		}
	})

	t.Run("falls back to resolution rate without verification", func(t *testing.T) {
		res := &antfly.ResearchAgentResult{
			Citations: []antfly.ResearchCitation{{Marker: "[E1]", EvidenceId: "E1"}},
			Verification: antfly.ResearchVerification{
				UnresolvedMarkers: []string{"[E2]"},
			},
		}
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: res})
		if err != nil {
			t.Fatal(err)
		}
		if result.Score != 0.5 {
			t.Errorf("expected score 0.5, got %v", result.Score)
		}
	})

	t.Run("no citations at all", func(t *testing.T) {
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: &antfly.ResearchAgentResult{}})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Pass || result.Score != 1.0 {
			t.Errorf("unexpected result: %+v", result)
		}
	})
}

func TestResearchSubQuestionCoverageEvaluator(t *testing.T) {
	e := NewResearchSubQuestionCoverageEvaluator("", 0)

	t.Run("mixed statuses", func(t *testing.T) {
		res := &antfly.ResearchAgentResult{
			Plan: antfly.ResearchPlan{
				SubQuestions: []antfly.ResearchSubQuestion{
					{Id: "q1", Question: "a", Status: "researched"},
					{Id: "q2", Question: "b", Status: "researched"},
					{Id: "q3", Question: "c", Status: "failed"},
					{Id: "q4", Question: "d", Status: "pending"},
				},
			},
		}
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: res})
		if err != nil {
			t.Fatal(err)
		}
		if result.Score != 0.5 {
			t.Errorf("expected score 0.5, got %v", result.Score)
		}
		if result.Pass {
			t.Errorf("expected fail below 0.8 threshold")
		}
	})

	t.Run("no planned sub-questions", func(t *testing.T) {
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: &antfly.ResearchAgentResult{}})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Pass || result.Score != 1.0 {
			t.Errorf("unexpected result: %+v", result)
		}
	})
}

func TestResearchEvidenceDiversityEvaluator(t *testing.T) {
	e := NewResearchEvidenceDiversityEvaluator("", 0)

	t.Run("mixed table and web evidence", func(t *testing.T) {
		res := &antfly.ResearchAgentResult{
			Evidence: []antfly.ResearchEvidence{
				{Id: "E1", Source: "table", Table: "docs", DocId: "1"},
				{Id: "E2", Source: "table", Table: "docs", DocId: "1"}, // duplicate doc
				{Id: "E3", Source: "web", Url: "https://example.com/a"},
				{Id: "E4", Source: "web", Url: "https://example.com/b"}, // same domain
				{Id: "E5", Source: "web", Url: "https://other.example/"},
			},
		}
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: res})
		if err != nil {
			t.Fatal(err)
		}
		// distinct: docs/1 (1), example.com (1), other.example (1) = 3 distinct / 5 total
		if result.Score != 0.6 {
			t.Errorf("expected score 0.6, got %v", result.Score)
		}
	})

	t.Run("no evidence", func(t *testing.T) {
		result, err := e.Evaluate(context.Background(), eval.EvalInput{Output: &antfly.ResearchAgentResult{}})
		if err != nil {
			t.Fatal(err)
		}
		if result.Pass || result.Score != 0 {
			t.Errorf("unexpected result: %+v", result)
		}
	})
}

func TestResearchAgentEvaluatorPreset(t *testing.T) {
	evaluators := ResearchAgentEvaluatorPreset()
	if len(evaluators) != 4 {
		t.Fatalf("expected 4 evaluators, got %d", len(evaluators))
	}
	for _, ev := range evaluators {
		if ev.Name() == "" {
			t.Error("expected non-empty evaluator name")
		}
	}
}
