// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	a2afacade "github.com/antflydb/antfly/src/a2a"
)

func TestBuildRetrievalRequestBasic(t *testing.T) {
	adapter := &a2aAdapter{}

	req := &a2afacade.RetrievalRequest{
		Query:         "test query",
		Table:         "docs",
		MaxIterations: 3,
	}

	antflyReq := adapter.buildRetrievalRequest(req)

	if antflyReq.Query != "test query" {
		t.Errorf("expected query 'test query', got %q", antflyReq.Query)
	}
	if antflyReq.MaxIterations != 3 {
		t.Errorf("expected max_iterations 3, got %d", antflyReq.MaxIterations)
	}
	if len(antflyReq.Queries) != 1 {
		t.Fatalf("expected 1 query config, got %d", len(antflyReq.Queries))
	}
	if antflyReq.Queries[0].Table != "docs" {
		t.Errorf("expected table 'docs', got %q", antflyReq.Queries[0].Table)
	}
	if antflyReq.Queries[0].SemanticSearch != "test query" {
		t.Errorf("expected semantic search to match query, got %q", antflyReq.Queries[0].SemanticSearch)
	}
}

func TestBuildRetrievalRequestNoTable(t *testing.T) {
	adapter := &a2aAdapter{}

	req := &a2afacade.RetrievalRequest{
		Query: "test query",
	}

	antflyReq := adapter.buildRetrievalRequest(req)

	if len(antflyReq.Queries) != 0 {
		t.Errorf("expected 0 query configs when no table, got %d", len(antflyReq.Queries))
	}
}

func TestBuildRetrievalRequestWithGeneration(t *testing.T) {
	adapter := &a2aAdapter{}

	req := &a2afacade.RetrievalRequest{
		Query:            "test query",
		EnableGeneration: true,
	}

	antflyReq := adapter.buildRetrievalRequest(req)

	if antflyReq.Steps.Generation.Enabled == nil {
		t.Fatal("expected generation.enabled to be set")
	}
	if !*antflyReq.Steps.Generation.Enabled {
		t.Error("expected generation.enabled to be true")
	}
}

func TestBuildRetrievalRequestGenerationNotSet(t *testing.T) {
	adapter := &a2aAdapter{}

	req := &a2afacade.RetrievalRequest{
		Query: "test query",
	}

	antflyReq := adapter.buildRetrievalRequest(req)

	if antflyReq.Steps.Generation.Enabled != nil {
		t.Error("expected generation.enabled to be nil when not requested")
	}
}

func TestBuildRetrievalRequestWithMessages(t *testing.T) {
	adapter := &a2aAdapter{}

	req := &a2afacade.RetrievalRequest{
		Query: "follow up",
		Messages: []ai.ChatMessage{
			{Role: ai.ChatMessageRoleUser, Content: "first question"},
			{Role: ai.ChatMessageRoleAssistant, Content: "first answer"},
		},
	}

	antflyReq := adapter.buildRetrievalRequest(req)

	if len(antflyReq.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(antflyReq.Messages))
	}
}

func TestMapRetrievalResultComplete(t *testing.T) {
	adapter := &a2aAdapter{}

	result := &RetrievalAgentResult{
		Generation:           "The answer is 42",
		GenerationConfidence: 0.95,
		FollowupQuestions:    []string{"Why 42?"},
		Status:               "complete",
		StrategyUsed:         "hybrid",
		ToolCallsMade:        5,
		Hits: []QueryHit{
			{ID: "doc1", Score: 0.9},
			{ID: "doc2", Score: 0.8},
		},
	}

	mapped := adapter.mapRetrievalResult(result)

	if mapped.Generation != "The answer is 42" {
		t.Errorf("expected generation text, got %q", mapped.Generation)
	}
	if mapped.GenerationConfidence != 0.95 {
		t.Errorf("expected confidence 0.95, got %f", mapped.GenerationConfidence)
	}
	if mapped.State != "complete" {
		t.Errorf("expected state 'complete', got %q", mapped.State)
	}
	if mapped.StrategyUsed != "hybrid" {
		t.Errorf("expected strategy 'hybrid', got %q", mapped.StrategyUsed)
	}
	if mapped.ToolCallsMade != 5 {
		t.Errorf("expected 5 tool calls, got %d", mapped.ToolCallsMade)
	}
	if len(mapped.Hits) != 2 {
		t.Errorf("expected 2 hits, got %d", len(mapped.Hits))
	}
	if len(mapped.FollowupQuestions) != 1 {
		t.Errorf("expected 1 followup question, got %d", len(mapped.FollowupQuestions))
	}
	if mapped.FollowupQuestions[0] != "Why 42?" {
		t.Errorf("expected followup 'Why 42?', got %q", mapped.FollowupQuestions[0])
	}
}

func TestMapRetrievalResultClarification(t *testing.T) {
	adapter := &a2aAdapter{}

	result := &RetrievalAgentResult{
		Status: "awaiting_clarification",
		ClarificationRequest: ClarificationRequest{
			Question: "Which table?",
			Options:  []string{"users", "docs"},
		},
	}

	mapped := adapter.mapRetrievalResult(result)

	if mapped.State != "awaiting_clarification" {
		t.Errorf("expected state 'awaiting_clarification', got %q", mapped.State)
	}
	if mapped.ClarificationQuestion != "Which table?" {
		t.Errorf("expected clarification question, got %q", mapped.ClarificationQuestion)
	}
}

func TestMapRetrievalResultEmpty(t *testing.T) {
	adapter := &a2aAdapter{}

	result := &RetrievalAgentResult{
		Status: "complete",
	}

	mapped := adapter.mapRetrievalResult(result)

	if mapped.State != "complete" {
		t.Errorf("expected state 'complete', got %q", mapped.State)
	}
	if len(mapped.Hits) != 0 {
		t.Errorf("expected 0 hits, got %d", len(mapped.Hits))
	}
	if mapped.ClarificationQuestion != "" {
		t.Errorf("expected empty clarification question, got %q", mapped.ClarificationQuestion)
	}
}

func TestMapRetrievalResultNoClarificationQuestion(t *testing.T) {
	adapter := &a2aAdapter{}

	// ClarificationRequest with empty question should not set ClarificationQuestion
	result := &RetrievalAgentResult{
		Status:               "complete",
		ClarificationRequest: ClarificationRequest{},
	}

	mapped := adapter.mapRetrievalResult(result)

	if mapped.ClarificationQuestion != "" {
		t.Errorf("expected empty clarification question for empty request, got %q", mapped.ClarificationQuestion)
	}
}
