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

// stubHandler is a minimal AgentHandler for testing dispatch.
type stubHandler struct {
	id       string
	executed bool
}

func (s *stubHandler) SkillID() string       { return s.id }
func (s *stubHandler) Skill() a2a.AgentSkill { return a2a.AgentSkill{ID: s.id, Name: s.id} }
func (s *stubHandler) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue) error {
	s.executed = true
	return queue.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCompleted, nil))
}
func (s *stubHandler) Cancel(_ context.Context, _ *a2asrv.RequestContext, _ eventqueue.Queue) error {
	return nil
}

func TestDispatcherRoutesExplicitSkill(t *testing.T) {
	logger := zap.NewNop()
	d := NewDispatcher(logger)

	h1 := &stubHandler{id: "retrieval"}
	h2 := &stubHandler{id: "query-builder"}
	d.Register(h1)
	d.Register(h2)

	mgr := eventqueue.NewInMemoryManager()
	taskID := a2a.NewTaskID()
	queue, err := mgr.GetOrCreate(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:     a2a.MessageRoleUser,
			Parts:    a2a.ContentParts{&a2a.TextPart{Text: "test"}},
			Metadata: map[string]any{"skill": "retrieval"},
		},
	}

	if err := d.Execute(context.Background(), reqCtx, queue); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !h1.executed {
		t.Error("expected retrieval handler to be executed")
	}
	if h2.executed {
		t.Error("did not expect query-builder handler to be executed")
	}
}

func TestDispatcherSingleHandlerFallback(t *testing.T) {
	logger := zap.NewNop()
	d := NewDispatcher(logger)

	h := &stubHandler{id: "retrieval"}
	d.Register(h)

	mgr := eventqueue.NewInMemoryManager()
	taskID := a2a.NewTaskID()
	queue, err := mgr.GetOrCreate(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	// No skill in metadata — should fall back to the single handler
	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "test"}},
		},
	}

	if err := d.Execute(context.Background(), reqCtx, queue); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !h.executed {
		t.Error("expected handler to be executed via fallback")
	}
}

func TestDispatcherUnknownSkillError(t *testing.T) {
	logger := zap.NewNop()
	d := NewDispatcher(logger)
	d.Register(&stubHandler{id: "retrieval"})

	mgr := eventqueue.NewInMemoryManager()
	taskID := a2a.NewTaskID()
	queue, err := mgr.GetOrCreate(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:     a2a.MessageRoleUser,
			Parts:    a2a.ContentParts{&a2a.TextPart{Text: "test"}},
			Metadata: map[string]any{"skill": "nonexistent"},
		},
	}

	err = d.Execute(context.Background(), reqCtx, queue)
	if err == nil {
		t.Fatal("expected error for unknown skill")
	}
}

func TestDispatcherMultipleHandlersNoSkillError(t *testing.T) {
	logger := zap.NewNop()
	d := NewDispatcher(logger)
	d.Register(&stubHandler{id: "retrieval"})
	d.Register(&stubHandler{id: "query-builder"})

	mgr := eventqueue.NewInMemoryManager()
	taskID := a2a.NewTaskID()
	queue, err := mgr.GetOrCreate(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	// No skill specified with multiple handlers
	reqCtx := &a2asrv.RequestContext{
		TaskID: taskID,
		Message: &a2a.Message{
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{&a2a.TextPart{Text: "test"}},
		},
	}

	err = d.Execute(context.Background(), reqCtx, queue)
	if err == nil {
		t.Fatal("expected error when skill not specified with multiple handlers")
	}
}

func TestDispatcherSkills(t *testing.T) {
	logger := zap.NewNop()
	d := NewDispatcher(logger)
	d.Register(&stubHandler{id: "retrieval"})
	d.Register(&stubHandler{id: "query-builder"})

	skills := d.Skills()
	if len(skills) != 2 {
		t.Fatalf("expected 2 skills, got %d", len(skills))
	}

	ids := map[string]bool{}
	for _, s := range skills {
		ids[s.ID] = true
	}
	if !ids["retrieval"] || !ids["query-builder"] {
		t.Errorf("expected retrieval and query-builder skills, got %v", ids)
	}
}
