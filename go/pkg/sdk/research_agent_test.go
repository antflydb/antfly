// Copyright 2026 The Antfly Contributors
// SPDX-License-Identifier: Apache-2.0

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResearchAgentJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/db/v1/agents/research" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("expected json accept header, got %q", r.Header.Get("Accept"))
		}
		var req ResearchAgentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Query != "how does antfly rerank?" {
			t.Errorf("unexpected query %q", req.Query)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ResearchAgentResult{
			Status:        "completed",
			Phase:         "done",
			ResearchState: ResearchState{Phase: "done"},
			Report:        ResearchReport{Markdown: "# Report\n\n[E1]"},
		})
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	result, err := client.ResearchAgent(context.Background(), ResearchAgentRequest{
		Query:   "how does antfly rerank?",
		Queries: []RetrievalQueryRequest{{Table: "docs"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Report.Markdown != "# Report\n\n[E1]" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestResearchAgentJSONError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.ResearchAgent(context.Background(), ResearchAgentRequest{Query: "x"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestResearchAgentStreaming(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Errorf("expected sse accept header, got %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		write := func(event string, payload any) {
			data, _ := json.Marshal(payload)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, string(data))
			if flusher != nil {
				flusher.Flush()
			}
		}
		write("step_started", SSEStepStarted{Id: "step_1", Name: "research", Action: "planning the research brief"})
		write("step_progress", map[string]any{
			"name": "research", "phase": "plan",
			"brief":            "Explain reranking trade-offs",
			"sub_questions":    []map[string]any{{"id": "q1", "question": "What models are supported?"}},
			"success_criteria": []string{"covers latency"},
		})
		write("step_progress", map[string]any{
			"name": "research", "phase": "sub_question_started",
			"sub_question_id": "q1", "question": "What models are supported?", "round": 1,
		})
		write("step_progress", map[string]any{
			"name": "research", "phase": "finding",
			"sub_question_id": "q1", "summary": "Cross-encoders are supported.",
			"evidence_ids": []string{"E1"},
		})
		write("step_progress", map[string]any{
			"name": "research", "phase": "reflection",
			"round": 1, "done": true,
		})
		write("step_progress", map[string]any{
			"name": "research", "phase": "section",
			"index": 0, "heading": "Overview",
		})
		write("generation", "# Overview\n")
		write("step_progress", map[string]any{
			"name": "research", "phase": "verification",
			"checked_sections": 1, "supported_ratio": 1.0,
		})
		write("step_completed", AgentStep{Id: "step_1", Name: "research", Kind: "planning", Status: "success"})
		write("done", ResearchAgentResult{
			Status:        "completed",
			Phase:         "done",
			ResearchState: ResearchState{Phase: "done"},
		})
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	var (
		gotStepStarted   *SSEStepStarted
		gotStepCompleted *AgentStep
		gotPlan          *ResearchPlan
		gotSubQuestion   *ResearchSubQuestionStarted
		gotFinding       *ResearchFinding
		gotReflection    *ResearchReflection
		gotSection       *ResearchSectionProgress
		gotVerification  *ResearchVerification
		gotGeneration    string
	)

	result, err := client.ResearchAgent(context.Background(), ResearchAgentRequest{
		Query:   "how does antfly rerank?",
		Queries: []RetrievalQueryRequest{{Table: "docs"}},
		Stream:  true,
	}, ResearchAgentOptions{
		OnStepStarted:   func(s *SSEStepStarted) error { gotStepStarted = s; return nil },
		OnStepCompleted: func(s *AgentStep) error { gotStepCompleted = s; return nil },
		OnPlan:          func(p *ResearchPlan) error { gotPlan = p; return nil },
		OnSubQuestionStarted: func(sq *ResearchSubQuestionStarted) error {
			gotSubQuestion = sq
			return nil
		},
		OnFinding:      func(f *ResearchFinding) error { gotFinding = f; return nil },
		OnReflection:   func(r *ResearchReflection) error { gotReflection = r; return nil },
		OnSection:      func(s *ResearchSectionProgress) error { gotSection = s; return nil },
		OnVerification: func(v *ResearchVerification) error { gotVerification = v; return nil },
		OnGeneration:   func(chunk string) error { gotGeneration += chunk; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "completed" || result.Phase != "done" {
		t.Errorf("unexpected result: %+v", result)
	}
	if gotStepStarted == nil || gotStepStarted.Id != "step_1" {
		t.Errorf("missing step_started callback: %+v", gotStepStarted)
	}
	if gotStepCompleted == nil || gotStepCompleted.Id != "step_1" {
		t.Errorf("missing step_completed callback: %+v", gotStepCompleted)
	}
	if gotPlan == nil || gotPlan.Brief != "Explain reranking trade-offs" || len(gotPlan.SubQuestions) != 1 {
		t.Errorf("missing/incorrect plan callback: %+v", gotPlan)
	}
	if gotSubQuestion == nil || gotSubQuestion.SubQuestionID != "q1" || gotSubQuestion.Round != 1 {
		t.Errorf("missing/incorrect sub_question_started callback: %+v", gotSubQuestion)
	}
	if gotFinding == nil || gotFinding.SubQuestionId != "q1" || gotFinding.Summary != "Cross-encoders are supported." {
		t.Errorf("missing/incorrect finding callback: %+v", gotFinding)
	}
	if gotReflection == nil || !gotReflection.Done || gotReflection.Round != 1 {
		t.Errorf("missing/incorrect reflection callback: %+v", gotReflection)
	}
	if gotSection == nil || gotSection.Heading != "Overview" {
		t.Errorf("missing/incorrect section callback: %+v", gotSection)
	}
	if gotVerification == nil || gotVerification.CheckedSections != 1 {
		t.Errorf("missing/incorrect verification callback: %+v", gotVerification)
	}
	if gotGeneration != "# Overview\n" {
		t.Errorf("unexpected generation text: %q", gotGeneration)
	}
}

func TestResearchAgentStreamingError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: error\ndata: {\"error\":\"incomplete\",\"reason\":\"deadline\"}\n\n")
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	var gotErr *ResearchAgentError
	_, err = client.ResearchAgent(context.Background(), ResearchAgentRequest{
		Query:  "x",
		Stream: true,
	}, ResearchAgentOptions{
		OnError: func(e *ResearchAgentError) error { gotErr = e; return nil },
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if gotErr == nil || gotErr.Error != "incomplete" || gotErr.Reason != "deadline" {
		t.Errorf("unexpected callback error: %+v", gotErr)
	}
}

func TestResearchJobLifecycle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/db/v1/agents/research/jobs":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(ResearchJob{JobId: "job_1", State: "queued", Phase: "plan"})
		case r.Method == http.MethodGet && r.URL.Path == "/db/v1/agents/research/jobs/job_1":
			_ = json.NewEncoder(w).Encode(ResearchJob{JobId: "job_1", State: "running", Phase: "research"})
		case r.Method == http.MethodPost && r.URL.Path == "/db/v1/agents/research/jobs/job_1/advance":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(ResearchJob{JobId: "job_1", State: "succeeded", Phase: "done"})
		case r.Method == http.MethodPost && r.URL.Path == "/db/v1/agents/research/jobs/job_1/cancel":
			_ = json.NewEncoder(w).Encode(ResearchJob{JobId: "job_1", State: "cancelled", Phase: "research"})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	job, err := client.StartResearchJob(ctx, ResearchJobStartRequest{Request: ResearchAgentRequest{Query: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if job.JobId != "job_1" || job.State != "queued" {
		t.Errorf("unexpected start result: %+v", job)
	}

	job, err = client.GetResearchJob(ctx, "job_1")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != "running" {
		t.Errorf("unexpected get result: %+v", job)
	}

	job, err = client.AdvanceResearchJob(ctx, "job_1", ResearchJobAdvanceRequest{MaxPhases: 2})
	if err != nil {
		t.Fatal(err)
	}
	if job.State != "succeeded" {
		t.Errorf("unexpected advance result: %+v", job)
	}

	job, err = client.CancelResearchJob(ctx, "job_1")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != "cancelled" {
		t.Errorf("unexpected cancel result: %+v", job)
	}
}

func TestAdvanceResearchJobConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "advance_in_progress", "status": http.StatusConflict})
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.AdvanceResearchJob(context.Background(), "job_1", ResearchJobAdvanceRequest{})
	if !errors.Is(err, ErrResearchJobAdvanceConflict) {
		t.Fatalf("expected ErrResearchJobAdvanceConflict, got %v", err)
	}
}

// TestRunResearchJob exercises the advance loop across multiple phases,
// including a 409 conflict that must be resolved by waiting and re-GETting
// the job rather than failing.
func TestRunResearchJob(t *testing.T) {
	var advanceCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/db/v1/agents/research/jobs":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(ResearchJob{JobId: "job_1", State: "queued", Phase: "plan"})
		case r.Method == http.MethodPost && r.URL.Path == "/db/v1/agents/research/jobs/job_1/advance":
			advanceCalls++
			switch advanceCalls {
			case 1:
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(ResearchJob{JobId: "job_1", State: "running", Phase: "research"})
			case 2:
				// Simulate a concurrent advance in flight.
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "advance_in_progress"})
			default:
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(ResearchJob{
					JobId: "job_1", State: "succeeded", Phase: "done",
					Result: ResearchAgentResult{Status: "completed"},
				})
			}
		case r.Method == http.MethodGet && r.URL.Path == "/db/v1/agents/research/jobs/job_1":
			// Re-GET after the conflict observes the job still running.
			_ = json.NewEncoder(w).Encode(ResearchJob{JobId: "job_1", State: "running", Phase: "research"})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	var updates []ResearchJobState
	job, err := client.RunResearchJob(context.Background(), ResearchAgentRequest{Query: "x"}, RunResearchJobOptions{
		PollInterval: 1, // avoid slowing down the test
		OnUpdate: func(j *ResearchJob) error {
			updates = append(updates, j.State)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.State != "succeeded" || job.Result.Status != "completed" {
		t.Errorf("unexpected final job: %+v", job)
	}
	if len(updates) == 0 || updates[len(updates)-1] != "succeeded" {
		t.Errorf("unexpected update sequence: %+v", updates)
	}
}

func TestResearchAgentStreamingRequiresValidDone(t *testing.T) {
	cases := map[string]string{
		"truncated":      "event: step_progress\ndata: {\"name\":\"research\",\"phase\":\"plan\",\"brief\":\"b\",\"sub_questions\":[]}\n\n",
		"malformed":      "event: done\ndata: {not json\n\n",
		"missing status": "event: done\ndata: {\"research_state\":{\"phase\":\"done\"}}\n\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			client, err := NewAntflyClient(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.ResearchAgent(context.Background(), ResearchAgentRequest{Query: "x", Stream: true})
			if err == nil {
				t.Fatalf("expected error, got result %+v", result)
			}
		})
	}
}

func TestResearchAgentStreamingIncompleteDoneIsAResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: done\ndata: {\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"deadline\"},\"research_state\":{\"phase\":\"research\"}}\n\n")
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ResearchAgent(context.Background(), ResearchAgentRequest{Query: "x", Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "incomplete" || result.ResearchState.Phase != "research" {
		t.Fatalf("expected a resumable incomplete result, got %+v", result)
	}
}
