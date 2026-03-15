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
	"fmt"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"go.uber.org/zap"
)

// mockQueryBuilderExecutor records calls and returns configured results.
type mockQueryBuilderExecutor struct {
	req    *QueryBuilderRequest
	result *QueryBuilderResult
	err    error
}

func (m *mockQueryBuilderExecutor) ExecuteQueryBuilderA2A(_ context.Context, req *QueryBuilderRequest) (*QueryBuilderResult, error) {
	m.req = req
	return m.result, m.err
}

func TestQueryBuilderHandlerBasic(t *testing.T) {
	mock := &mockQueryBuilderExecutor{
		result: &QueryBuilderResult{
			Query:       map[string]any{"match": "test"},
			Explanation: "Simple match query",
			Confidence:  0.95,
		},
	}
	handler := NewQueryBuilderAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "Find error logs with status 500"}},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

	// Drain events until terminal
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

	// Verify request
	if mock.req == nil {
		t.Fatal("expected executor to be called")
	}
	if mock.req.Intent != "Find error logs with status 500" {
		t.Errorf("unexpected intent: %s", mock.req.Intent)
	}

	// Expect: working + artifact + completed = at least 3 events
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}

	// Last event should be completed
	lastEvt := events[len(events)-1]
	statusEvt, ok := lastEvt.(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatalf("expected last event to be TaskStatusUpdateEvent, got %T", lastEvt)
	}
	if statusEvt.Status.State != a2a.TaskStateCompleted {
		t.Errorf("expected completed state, got %s", statusEvt.Status.State)
	}
}

func TestQueryBuilderHandlerWithDataPart(t *testing.T) {
	mock := &mockQueryBuilderExecutor{
		result: &QueryBuilderResult{
			Query:       map[string]any{"match": "test"},
			Explanation: "Match query",
			Confidence:  0.9,
		},
	}
	handler := NewQueryBuilderAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role: a2a.MessageRoleUser,
			Parts: a2a.ContentParts{
				&a2a.TextPart{Text: "search intent"},
				&a2a.DataPart{Data: map[string]any{
					"table":         "products",
					"schema_fields": []any{"title", "description", "price"},
				}},
			},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

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

	if mock.req == nil {
		t.Fatal("expected executor to be called")
	}
	if mock.req.Table != "products" {
		t.Errorf("expected table 'products', got %q", mock.req.Table)
	}
	if len(mock.req.SchemaFields) != 3 {
		t.Fatalf("expected 3 schema fields, got %d", len(mock.req.SchemaFields))
	}
	if mock.req.SchemaFields[0] != "title" || mock.req.SchemaFields[1] != "description" || mock.req.SchemaFields[2] != "price" {
		t.Errorf("unexpected schema fields: %v", mock.req.SchemaFields)
	}
}

func TestQueryBuilderHandlerEmptyIntent(t *testing.T) {
	mock := &mockQueryBuilderExecutor{
		result: &QueryBuilderResult{},
	}
	handler := NewQueryBuilderAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	// No text part — only data
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

func TestQueryBuilderHandlerExecutorError(t *testing.T) {
	mock := &mockQueryBuilderExecutor{
		err: fmt.Errorf("generator not configured"),
	}
	handler := NewQueryBuilderAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "some intent"}},
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
			if statusEvt.Status.State.Terminal() {
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
	if lastStatus.Status.State != a2a.TaskStateFailed {
		t.Errorf("expected failed state, got %s", lastStatus.Status.State)
	}
}

func TestQueryBuilderHandlerWarnings(t *testing.T) {
	mock := &mockQueryBuilderExecutor{
		result: &QueryBuilderResult{
			Query:       map[string]any{"match_all": map[string]any{}},
			Explanation: "Broad match query",
			Confidence:  0.5,
			Warnings:    []string{"Low confidence", "Consider adding filters"},
		},
	}
	handler := NewQueryBuilderAgentHandler(mock, zap.NewNop())

	writerQueue, readerQueue, taskID := createQueuePair(t)
	defer writerQueue.Close()
	defer readerQueue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "find everything"}},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.Execute(context.Background(), reqCtx, writerQueue)
	}()

	// Collect all events
	var artifacts []*a2a.TaskArtifactUpdateEvent
	for {
		evt, _, readErr := readerQueue.Read(context.Background())
		if readErr != nil {
			t.Fatalf("queue read error: %v", readErr)
		}
		if artEvt, ok := evt.(*a2a.TaskArtifactUpdateEvent); ok {
			artifacts = append(artifacts, artEvt)
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

	// Verify artifact has warnings
	if len(artifacts) == 0 {
		t.Fatal("expected at least one artifact")
	}
	art := artifacts[0]
	if len(art.Artifact.Parts) == 0 {
		t.Fatal("expected artifact parts")
	}
	dataPart, ok := art.Artifact.Parts[0].(*a2a.DataPart)
	if !ok {
		t.Fatalf("expected DataPart, got %T", art.Artifact.Parts[0])
	}
	warnings, ok := dataPart.Data["warnings"]
	if !ok {
		t.Fatal("expected warnings in result data")
	}
	warningsList, ok := warnings.([]string)
	if !ok {
		t.Fatalf("expected warnings to be []string, got %T", warnings)
	}
	if len(warningsList) != 2 {
		t.Errorf("expected 2 warnings, got %d", len(warningsList))
	}
}

func TestQueryBuilderSkillDescriptor(t *testing.T) {
	handler := NewQueryBuilderAgentHandler(nil, zap.NewNop())
	skill := handler.Skill()

	if skill.ID != "query-builder" {
		t.Errorf("expected skill ID 'query-builder', got %q", skill.ID)
	}
	if skill.Name != "Query Builder" {
		t.Errorf("expected skill name 'Query Builder', got %q", skill.Name)
	}
}
