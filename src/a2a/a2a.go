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

// Package a2a provides an A2A (Agent-to-Agent) protocol facade for Antfly agents.
//
// It translates A2A Messages into Antfly agent requests, calls the existing agent
// logic, and maps results back to A2A Tasks/Artifacts/Events. The REST API stays
// untouched — this is a protocol adapter, not a replacement.
package a2a

import (
	"context"
	"fmt"
	"strings"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
	"go.uber.org/zap"
)

// AgentHandler is the interface each Antfly agent implements for A2A.
// Each handler maps a single A2A "skill" to an existing Antfly agent.
type AgentHandler interface {
	// SkillID returns the unique skill identifier (e.g. "retrieval", "query-builder").
	SkillID() string

	// Skill returns the A2A skill descriptor for the agent card.
	Skill() a2a.AgentSkill

	// Execute runs the agent logic, writing A2A events to the queue.
	Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue) error

	// Cancel requests cancellation of an ongoing task.
	Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue) error
}

// Dispatcher implements a2asrv.AgentExecutor by routing to registered AgentHandlers.
// It dispatches based on the "skill" key in the incoming message's metadata.
type Dispatcher struct {
	handlers map[string]AgentHandler
	logger   *zap.Logger
}

// NewDispatcher creates a new Dispatcher.
func NewDispatcher(logger *zap.Logger) *Dispatcher {
	return &Dispatcher{
		handlers: make(map[string]AgentHandler),
		logger:   logger,
	}
}

// Register adds an AgentHandler to the dispatcher.
func (d *Dispatcher) Register(h AgentHandler) {
	d.handlers[h.SkillID()] = h
	d.logger.Info("Registered A2A agent handler", zap.String("skill", h.SkillID()))
}

// Skills returns all registered skill descriptors (for building the Agent Card).
func (d *Dispatcher) Skills() []a2a.AgentSkill {
	skills := make([]a2a.AgentSkill, 0, len(d.handlers))
	for _, h := range d.handlers {
		skills = append(skills, h.Skill())
	}
	return skills
}

// Execute implements a2asrv.AgentExecutor.
func (d *Dispatcher) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue) error {
	h, err := d.resolve(reqCtx)
	if err != nil {
		return err
	}
	d.logger.Debug("Dispatching A2A execute", zap.String("skill", h.SkillID()), zap.String("taskID", string(reqCtx.TaskID)))
	return h.Execute(ctx, reqCtx, queue)
}

// Cancel implements a2asrv.AgentExecutor.
func (d *Dispatcher) Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue) error {
	h, err := d.resolve(reqCtx)
	if err != nil {
		return err
	}
	return h.Cancel(ctx, reqCtx, queue)
}

// resolve determines which handler should process the request.
// Priority: 1) explicit metadata["skill"], 2) single-handler fallback, 3) error.
func (d *Dispatcher) resolve(reqCtx *a2asrv.RequestContext) (AgentHandler, error) {
	// Check explicit skill in message metadata
	if reqCtx.Message != nil {
		if skillVal, ok := reqCtx.Message.Metadata["skill"]; ok {
			if skillID, ok := skillVal.(string); ok {
				if h, exists := d.handlers[skillID]; exists {
					return h, nil
				}
				return nil, fmt.Errorf("unknown skill %q, available: %s", skillID, d.availableSkills())
			}
		}
	}

	// Check request-level metadata
	if reqCtx.Metadata != nil {
		if skillVal, ok := reqCtx.Metadata["skill"]; ok {
			if skillID, ok := skillVal.(string); ok {
				if h, exists := d.handlers[skillID]; exists {
					return h, nil
				}
				return nil, fmt.Errorf("unknown skill %q, available: %s", skillID, d.availableSkills())
			}
		}
	}

	// Single-handler fallback
	if len(d.handlers) == 1 {
		for _, h := range d.handlers {
			return h, nil
		}
	}

	return nil, fmt.Errorf("skill must be specified in message metadata when multiple skills are available: %s", d.availableSkills())
}

func (d *Dispatcher) availableSkills() string {
	ids := make([]string, 0, len(d.handlers))
	for id := range d.handlers {
		ids = append(ids, id)
	}
	return strings.Join(ids, ", ")
}
