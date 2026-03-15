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

package a2a

import (
	"context"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
	"go.uber.org/zap"
)

// mockRetrievalExecutor records calls and returns configured results.
type mockRetrievalExecutor struct {
	pipelineReq *RetrievalRequest
	agenticReq  *RetrievalRequest
	result      *RetrievalResult
	err         error
}

func (m *mockRetrievalExecutor) ExecuteRetrievalPipeline(_ context.Context, req *RetrievalRequest, _ StreamCallback) (*RetrievalResult, error) {
	m.pipelineReq = req
	return m.result, m.err
}

func (m *mockRetrievalExecutor) ExecuteRetrievalAgentic(_ context.Context, req *RetrievalRequest, _ StreamCallback) (*RetrievalResult, error) {
	m.agenticReq = req
	return m.result, m.err
}

// createQueuePair creates a writer and reader queue for the same task.
// The in-memory queue does not deliver events to the same queue that wrote them,
// so tests need a separate reader queue.
func createQueuePair(t *testing.T) (writer eventqueue.Queue, reader eventqueue.Queue, taskID a2a.TaskID) {
	t.Helper()
	mgr := eventqueue.NewInMemoryManager()
	taskID = a2a.NewTaskID()
	var err error
	writer, err = mgr.GetOrCreate(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	reader, err = mgr.GetOrCreate(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	return writer, reader, taskID
}

func TestRetrievalHandlerTextOnly(t *testing.T) {
	mock := &mockRetrievalExecutor{
		result: &RetrievalResult{
			State:      "complete",
			Generation: "Here is the answer.",
			Hits:       []any{"hit1"},
		},
	}
	handler := NewRetrievalAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "How do I configure OAuth?"}},
		},
	}

	// Run in goroutine since Execute writes to queue and reader must consume
	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

	// Read events until we get a completed status
	var events []a2a.Event
	for {
		evt, _, readErr := readerQueue.Read(context.Background())
		if readErr != nil {
			t.Fatalf("queue read error: %v", readErr)
		}
		events = append(events, evt)
		if statusEvt, ok := evt.(*a2a.TaskStatusUpdateEvent); ok {
			if statusEvt.Status.State.Terminal() {
				break
			}
		}
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify pipeline mode was called (MaxIterations=0 is default)
	if mock.pipelineReq == nil {
		t.Fatal("expected pipeline to be called")
	}
	if mock.pipelineReq.Query != "How do I configure OAuth?" {
		t.Errorf("unexpected query: %s", mock.pipelineReq.Query)
	}

	// Verify we got working + artifact + completed events
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
}

func TestRetrievalHandlerWithDataPart(t *testing.T) {
	mock := &mockRetrievalExecutor{
		result: &RetrievalResult{
			State: "complete",
			Hits:  []any{"hit1"},
		},
	}
	handler := NewRetrievalAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role: a2a.MessageRoleUser,
			Parts: a2a.ContentParts{
				&a2a.TextPart{Text: "search query"},
				&a2a.DataPart{Data: map[string]any{
					"table":          "docs",
					"max_iterations": float64(5),
				}},
			},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

	// Drain events
	for {
		evt, _, readErr := readerQueue.Read(context.Background())
		if readErr != nil {
			t.Fatalf("queue read error: %v", readErr)
		}
		if statusEvt, ok := evt.(*a2a.TaskStatusUpdateEvent); ok {
			if statusEvt.Status.State.Terminal() {
				break
			}
		}
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify agentic mode was called (max_iterations=5)
	if mock.agenticReq == nil {
		t.Fatal("expected agentic mode to be called")
	}
	if mock.agenticReq.Table != "docs" {
		t.Errorf("expected table 'docs', got %q", mock.agenticReq.Table)
	}
	if mock.agenticReq.MaxIterations != 5 {
		t.Errorf("expected max_iterations=5, got %d", mock.agenticReq.MaxIterations)
	}
}

func TestRetrievalHandlerEmptyQuery(t *testing.T) {
	mock := &mockRetrievalExecutor{
		result: &RetrievalResult{State: "complete"},
	}
	handler := NewRetrievalAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	// Empty message — no text parts
	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.DataPart{Data: map[string]any{"table": "docs"}}},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

	// Read the failed event
	evt, _, readErr := readerQueue.Read(context.Background())
	if readErr != nil {
		t.Fatalf("queue read error: %v", readErr)
	}

	statusEvt, ok := evt.(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatalf("expected TaskStatusUpdateEvent, got %T", evt)
	}
	if statusEvt.Status.State != a2a.TaskStateFailed {
		t.Errorf("expected failed state, got %s", statusEvt.Status.State)
	}
}

func TestRetrievalHandlerAwaitingClarification(t *testing.T) {
	mock := &mockRetrievalExecutor{
		result: &RetrievalResult{
			State:                 "awaiting_clarification",
			ClarificationQuestion: "Which table should I search?",
		},
	}
	handler := NewRetrievalAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "search something"}},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

	// Drain events until terminal
	var lastStatus *a2a.TaskStatusUpdateEvent
	for {
		evt, _, readErr := readerQueue.Read(context.Background())
		if readErr != nil {
			t.Fatalf("queue read error: %v", readErr)
		}
		if statusEvt, ok := evt.(*a2a.TaskStatusUpdateEvent); ok {
			lastStatus = statusEvt
			if statusEvt.Status.State.Terminal() || statusEvt.Status.State == a2a.TaskStateInputRequired {
				break
			}
		}
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if lastStatus == nil {
		t.Fatal("expected a status event")
	}
	if lastStatus.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("expected input_required state, got %s", lastStatus.Status.State)
	}
}

func TestRetrievalSkillDescriptor(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	skill := handler.Skill()

	if skill.ID != "retrieval" {
		t.Errorf("expected skill ID 'retrieval', got %q", skill.ID)
	}
	if skill.Name != "Retrieval" {
		t.Errorf("expected skill name 'Retrieval', got %q", skill.Name)
	}
}

// --- Stream bridge tests ---

func TestStreamBridgeHitEvent(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{TaskID: taskID}
	hitsID := a2a.NewArtifactID()
	genID := a2a.NewArtifactID()
	cb := handler.newStreamBridge(context.Background(), reqCtx, writerQueue, hitsID, genID)

	if err := cb(context.Background(), "hit", map[string]any{"id": "doc1"}); err != nil {
		t.Fatalf("stream callback error: %v", err)
	}

	evt, _, err := readerQueue.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	artEvt, ok := evt.(*a2a.TaskArtifactUpdateEvent)
	if !ok {
		t.Fatalf("expected TaskArtifactUpdateEvent, got %T", evt)
	}
	if artEvt.Artifact.ID != hitsID {
		t.Errorf("expected hits artifact ID, got %v", artEvt.Artifact.ID)
	}
	if artEvt.Artifact.Name != "hits" {
		t.Errorf("expected artifact name 'hits', got %q", artEvt.Artifact.Name)
	}
	if !artEvt.Append {
		t.Error("expected Append=true for hit event")
	}
	dp, ok := artEvt.Artifact.Parts[0].(*a2a.DataPart)
	if !ok {
		t.Fatalf("expected DataPart, got %T", artEvt.Artifact.Parts[0])
	}
	hitData, ok := dp.Data["hit"].(map[string]any)
	if !ok {
		t.Fatalf("expected hit data map, got %T", dp.Data["hit"])
	}
	if hitData["id"] != "doc1" {
		t.Errorf("expected hit id 'doc1', got %v", hitData["id"])
	}
}

func TestStreamBridgeTokenEvent(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{TaskID: taskID}
	hitsID := a2a.NewArtifactID()
	genID := a2a.NewArtifactID()
	cb := handler.newStreamBridge(context.Background(), reqCtx, writerQueue, hitsID, genID)

	if err := cb(context.Background(), "token", "Hello "); err != nil {
		t.Fatalf("stream callback error: %v", err)
	}

	evt, _, err := readerQueue.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	artEvt, ok := evt.(*a2a.TaskArtifactUpdateEvent)
	if !ok {
		t.Fatalf("expected TaskArtifactUpdateEvent, got %T", evt)
	}
	if artEvt.Artifact.ID != genID {
		t.Errorf("expected generation artifact ID, got %v", artEvt.Artifact.ID)
	}
	if artEvt.Artifact.Name != "generation" {
		t.Errorf("expected artifact name 'generation', got %q", artEvt.Artifact.Name)
	}
	if !artEvt.Append {
		t.Error("expected Append=true for token event")
	}
	tp, ok := artEvt.Artifact.Parts[0].(*a2a.TextPart)
	if !ok {
		t.Fatalf("expected TextPart, got %T", artEvt.Artifact.Parts[0])
	}
	if tp.Text != "Hello " {
		t.Errorf("expected text 'Hello ', got %q", tp.Text)
	}
}

func TestStreamBridgeErrorEvent(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{TaskID: taskID}
	cb := handler.newStreamBridge(context.Background(), reqCtx, writerQueue, a2a.NewArtifactID(), a2a.NewArtifactID())

	if err := cb(context.Background(), "error", "something went wrong"); err != nil {
		t.Fatalf("stream callback error: %v", err)
	}

	evt, _, err := readerQueue.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	statusEvt, ok := evt.(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatalf("expected TaskStatusUpdateEvent, got %T", evt)
	}
	if statusEvt.Status.State != a2a.TaskStateFailed {
		t.Errorf("expected failed state, got %s", statusEvt.Status.State)
	}
}

func TestStreamBridgeUnknownEvent(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	writerQueue, _, taskID := createQueuePair(t)
	defer writerQueue.Close()

	reqCtx := &a2asrv.RequestContext{TaskID: taskID}
	cb := handler.newStreamBridge(context.Background(), reqCtx, writerQueue, a2a.NewArtifactID(), a2a.NewArtifactID())

	// Unknown events should return nil without writing
	err := cb(context.Background(), "unknown_event_type", "data")
	if err != nil {
		t.Errorf("expected nil error for unknown event, got %v", err)
	}
}

func TestStreamBridgeMetadataEventTypes(t *testing.T) {
	// Verify all metadata event types (classification, reasoning, followup, eval)
	// produce DataPart artifacts with the event type as key
	eventTypes := []string{"classification", "reasoning", "followup", "eval"}

	for _, eventType := range eventTypes {
		t.Run(eventType, func(t *testing.T) {
			handler := NewRetrievalAgentHandler(nil, zap.NewNop())
			writerQueue, readerQueue, taskID := createQueuePair(t)
			defer writerQueue.Close()
			defer readerQueue.Close()

			reqCtx := &a2asrv.RequestContext{TaskID: taskID}
			cb := handler.newStreamBridge(context.Background(), reqCtx, writerQueue, a2a.NewArtifactID(), a2a.NewArtifactID())

			if err := cb(context.Background(), eventType, "test-data"); err != nil {
				t.Fatalf("stream callback error for %s: %v", eventType, err)
			}

			evt, _, err := readerQueue.Read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			artEvt, ok := evt.(*a2a.TaskArtifactUpdateEvent)
			if !ok {
				t.Fatalf("expected TaskArtifactUpdateEvent for %s, got %T", eventType, evt)
			}
			if artEvt.Artifact.Name != eventType {
				t.Errorf("expected artifact name %q, got %q", eventType, artEvt.Artifact.Name)
			}
			dp, ok := artEvt.Artifact.Parts[0].(*a2a.DataPart)
			if !ok {
				t.Fatalf("expected DataPart for %s, got %T", eventType, artEvt.Artifact.Parts[0])
			}
			if _, ok := dp.Data[eventType]; !ok {
				t.Errorf("expected %q key in data part", eventType)
			}
		})
	}
}

// --- buildResultParts tests ---

func TestBuildResultPartsWithGeneration(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	result := &RetrievalResult{
		Generation:        "The answer is 42.",
		State:             "complete",
		Hits:              []any{"hit1", "hit2"},
		StrategyUsed:      "hybrid",
		ToolCallsMade:     3,
		FollowupQuestions: []string{"What about X?"},
	}

	parts := handler.buildResultParts(result)

	// Should have TextPart (generation) + DataPart (metadata)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}

	tp, ok := parts[0].(*a2a.TextPart)
	if !ok {
		t.Fatalf("expected first part to be TextPart, got %T", parts[0])
	}
	if tp.Text != "The answer is 42." {
		t.Errorf("expected generation text, got %q", tp.Text)
	}

	dp, ok := parts[1].(*a2a.DataPart)
	if !ok {
		t.Fatalf("expected second part to be DataPart, got %T", parts[1])
	}
	if dp.Data["state"] != "complete" {
		t.Errorf("expected state 'complete', got %v", dp.Data["state"])
	}
	if dp.Data["hit_count"] != 2 {
		t.Errorf("expected hit_count 2, got %v", dp.Data["hit_count"])
	}
	if dp.Data["strategy"] != "hybrid" {
		t.Errorf("expected strategy 'hybrid', got %v", dp.Data["strategy"])
	}
	if dp.Data["tool_calls"] != 3 {
		t.Errorf("expected tool_calls 3, got %v", dp.Data["tool_calls"])
	}
	if _, ok := dp.Data["followup_questions"]; !ok {
		t.Error("expected followup_questions in data part")
	}
}

func TestBuildResultPartsNoGeneration(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	result := &RetrievalResult{
		State: "complete",
		Hits:  []any{"hit1"},
	}

	parts := handler.buildResultParts(result)

	// Should have only DataPart (no generation text)
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}

	_, ok := parts[0].(*a2a.DataPart)
	if !ok {
		t.Fatalf("expected DataPart, got %T", parts[0])
	}
}

func TestBuildResultPartsNoFollowups(t *testing.T) {
	handler := NewRetrievalAgentHandler(nil, zap.NewNop())
	result := &RetrievalResult{
		State: "complete",
	}

	parts := handler.buildResultParts(result)
	dp, ok := parts[0].(*a2a.DataPart)
	if !ok {
		t.Fatalf("expected DataPart, got %T", parts[0])
	}

	if _, ok := dp.Data["followup_questions"]; ok {
		t.Error("expected no followup_questions key when empty")
	}
}

// --- Streaming executor integration test ---

// streamingMockExecutor invokes the stream callback during execution.
type streamingMockExecutor struct {
	result     *RetrievalResult
	streamEvts []struct {
		eventType string
		data      any
	}
}

func (m *streamingMockExecutor) ExecuteRetrievalPipeline(ctx context.Context, _ *RetrievalRequest, streamCb StreamCallback) (*RetrievalResult, error) {
	for _, evt := range m.streamEvts {
		if err := streamCb(ctx, evt.eventType, evt.data); err != nil {
			return nil, err
		}
	}
	return m.result, nil
}

func (m *streamingMockExecutor) ExecuteRetrievalAgentic(_ context.Context, _ *RetrievalRequest, _ StreamCallback) (*RetrievalResult, error) {
	return m.result, nil
}

func TestRetrievalHandlerWithStreaming(t *testing.T) {
	mock := &streamingMockExecutor{
		result: &RetrievalResult{
			State:      "complete",
			Generation: "Answer text",
			Hits:       []any{"hit1"},
		},
		streamEvts: []struct {
			eventType string
			data      any
		}{
			{"hit", map[string]any{"id": "doc1"}},
			{"hit", map[string]any{"id": "doc2"}},
			{"token", "Answer "},
			{"token", "text"},
		},
	}
	handler := NewRetrievalAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "test query"}},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

	var statusEvents []*a2a.TaskStatusUpdateEvent
	var artifactEvents []*a2a.TaskArtifactUpdateEvent
	for {
		evt, _, readErr := readerQueue.Read(context.Background())
		if readErr != nil {
			t.Fatalf("queue read error: %v", readErr)
		}
		switch e := evt.(type) {
		case *a2a.TaskStatusUpdateEvent:
			statusEvents = append(statusEvents, e)
			if e.Status.State.Terminal() {
				goto done
			}
		case *a2a.TaskArtifactUpdateEvent:
			artifactEvents = append(artifactEvents, e)
		}
	}
done:

	if err := <-errCh; err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// working + completed = 2 status events
	if len(statusEvents) < 2 {
		t.Errorf("expected at least 2 status events, got %d", len(statusEvents))
	}
	if statusEvents[0].Status.State != a2a.TaskStateWorking {
		t.Errorf("expected first status to be working, got %s", statusEvents[0].Status.State)
	}
	if statusEvents[len(statusEvents)-1].Status.State != a2a.TaskStateCompleted {
		t.Errorf("expected last status to be completed, got %s", statusEvents[len(statusEvents)-1].Status.State)
	}

	// 4 streaming events + 1 final result artifact = 5 artifacts
	// (2 hits + 2 tokens + 1 final result)
	if len(artifactEvents) < 5 {
		t.Errorf("expected at least 5 artifact events, got %d", len(artifactEvents))
	}
}
