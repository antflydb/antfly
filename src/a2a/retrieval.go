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

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
	"github.com/antflydb/antfly/lib/ai"
	"go.uber.org/zap"
)

// RetrievalResult holds the fields from a retrieval agent result that the A2A
// facade needs. This avoids importing the metadata package.
type RetrievalResult struct {
	Hits                  []any
	Generation            string
	GenerationConfidence  float32
	FollowupQuestions     []string
	State                 string // "complete", "awaiting_clarification", "tool_calling"
	StrategyUsed          string
	ToolCallsMade         int
	ClarificationQuestion string
}

// RetrievalRequest holds the fields needed to invoke the retrieval agent.
type RetrievalRequest struct {
	Query            string
	Table            string
	MaxIterations    int
	Messages         []ai.ChatMessage
	EnableGeneration bool
}

// StreamCallback is the streaming callback signature used by Antfly agents.
type StreamCallback = func(ctx context.Context, eventType string, data any) error

// RetrievalExecutor abstracts the retrieval agent methods on TableApi.
type RetrievalExecutor interface {
	// ExecuteRetrievalPipeline runs pipeline-mode retrieval.
	ExecuteRetrievalPipeline(ctx context.Context, req *RetrievalRequest, streamCb StreamCallback) (*RetrievalResult, error)
	// ExecuteRetrievalAgentic runs agentic-mode retrieval.
	ExecuteRetrievalAgentic(ctx context.Context, req *RetrievalRequest, streamCb StreamCallback) (*RetrievalResult, error)
}

// RetrievalAgentHandler adapts the Antfly retrieval agent to the A2A protocol.
type RetrievalAgentHandler struct {
	executor RetrievalExecutor
	logger   *zap.Logger
}

// NewRetrievalAgentHandler creates a new RetrievalAgentHandler.
func NewRetrievalAgentHandler(executor RetrievalExecutor, logger *zap.Logger) *RetrievalAgentHandler {
	return &RetrievalAgentHandler{
		executor: executor,
		logger:   logger.Named("a2a.retrieval"),
	}
}

func (h *RetrievalAgentHandler) SkillID() string { return "retrieval" }

func (h *RetrievalAgentHandler) Skill() a2a.AgentSkill {
	return a2a.AgentSkill{
		ID:          "retrieval",
		Name:        "Retrieval",
		Description: "Hybrid search and RAG agent. Retrieves documents using semantic, full-text, and metadata search, optionally generating an LLM answer.",
		Tags:        []string{"search", "rag", "retrieval"},
		Examples: []string{
			"How do I configure OAuth?",
			"Find documents about vector search",
		},
		InputModes:  []string{"text", "data"},
		OutputModes: []string{"text", "data"},
	}
}

func (h *RetrievalAgentHandler) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue) error {
	// Extract query from message text
	query := extractTextFromMessage(reqCtx.Message)
	if query == "" {
		return writeFailedStatus(ctx, reqCtx, queue, "message must contain a text part with the search query")
	}

	// Extract optional config from DataPart
	data := extractDataFromMessage(reqCtx.Message)

	// Build the request
	req := &RetrievalRequest{
		Query:         query,
		Table:         stringFromMap(data, "table", ""),
		MaxIterations: intFromMap(data, "max_iterations", 0),
	}

	// Extract steps config
	if stepsData, ok := data["steps"]; ok {
		if stepsMap, ok := stepsData.(map[string]any); ok {
			if _, hasGen := stepsMap["generation"]; hasGen {
				req.EnableGeneration = true
			}
		}
	}

	// Multi-turn: extract conversation history from stored task
	if reqCtx.StoredTask != nil {
		req.Messages = extractConversationHistory(reqCtx.StoredTask)
	}

	// Signal working state
	if err := queue.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return fmt.Errorf("writing working status: %w", err)
	}

	// Create streaming bridge
	hitsArtifactID := a2a.NewArtifactID()
	genArtifactID := a2a.NewArtifactID()
	streamCb := h.newStreamBridge(ctx, reqCtx, queue, hitsArtifactID, genArtifactID)

	// Execute the appropriate mode
	var result *RetrievalResult
	var err error
	if req.MaxIterations == 0 {
		result, err = h.executor.ExecuteRetrievalPipeline(ctx, req, streamCb)
	} else {
		result, err = h.executor.ExecuteRetrievalAgentic(ctx, req, streamCb)
	}
	if err != nil {
		return writeFailedStatus(ctx, reqCtx, queue, fmt.Sprintf("retrieval failed: %v", err))
	}

	// Write final result as artifact
	resultParts := h.buildResultParts(result)
	finalArtifact := &a2a.TaskArtifactUpdateEvent{
		TaskID:    reqCtx.TaskID,
		ContextID: reqCtx.ContextID,
		Artifact: &a2a.Artifact{
			ID:          a2a.NewArtifactID(),
			Name:        "result",
			Description: "Complete retrieval result",
			Parts:       resultParts,
		},
		LastChunk: true,
	}
	if err := queue.Write(ctx, finalArtifact); err != nil {
		return fmt.Errorf("writing final artifact: %w", err)
	}

	// Map terminal state
	state := a2a.TaskStateCompleted
	var statusMsg *a2a.Message
	if result.State == "awaiting_clarification" {
		state = a2a.TaskStateInputRequired
		clarText := result.ClarificationQuestion
		if clarText == "" {
			clarText = "Please provide more details to continue."
		}
		statusMsg = a2a.NewMessage(a2a.MessageRoleAgent, &a2a.TextPart{Text: clarText})
	} else if result.Generation != "" {
		statusMsg = a2a.NewMessage(a2a.MessageRoleAgent, &a2a.TextPart{Text: result.Generation})
	}

	return queue.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, state, statusMsg))
}

func (h *RetrievalAgentHandler) Cancel(_ context.Context, _ *a2asrv.RequestContext, _ eventqueue.Queue) error {
	return nil
}

// newStreamBridge returns a streamCallback that translates Antfly events to A2A queue writes.
func (h *RetrievalAgentHandler) newStreamBridge(
	ctx context.Context,
	reqCtx *a2asrv.RequestContext,
	queue eventqueue.Queue,
	hitsArtifactID, genArtifactID a2a.ArtifactID,
) StreamCallback {
	return func(_ context.Context, eventType string, data any) error {
		switch eventType {
		case "hit":
			evt := &a2a.TaskArtifactUpdateEvent{
				TaskID:    reqCtx.TaskID,
				ContextID: reqCtx.ContextID,
				Append:    true,
				Artifact: &a2a.Artifact{
					ID:   hitsArtifactID,
					Name: "hits",
					Parts: a2a.ContentParts{
						&a2a.DataPart{Data: map[string]any{"hit": data}},
					},
				},
			}
			return queue.Write(ctx, evt)

		case "token":
			text, _ := data.(string)
			evt := &a2a.TaskArtifactUpdateEvent{
				TaskID:    reqCtx.TaskID,
				ContextID: reqCtx.ContextID,
				Append:    true,
				Artifact: &a2a.Artifact{
					ID:   genArtifactID,
					Name: "generation",
					Parts: a2a.ContentParts{
						&a2a.TextPart{Text: text},
					},
				},
			}
			return queue.Write(ctx, evt)

		case "classification", "reasoning", "followup", "eval":
			evt := &a2a.TaskArtifactUpdateEvent{
				TaskID:    reqCtx.TaskID,
				ContextID: reqCtx.ContextID,
				Append:    true,
				Artifact: &a2a.Artifact{
					ID:   a2a.NewArtifactID(),
					Name: eventType,
					Parts: a2a.ContentParts{
						&a2a.DataPart{Data: map[string]any{eventType: data}},
					},
				},
			}
			return queue.Write(ctx, evt)

		case "error":
			return writeFailedStatus(ctx, reqCtx, queue, fmt.Sprintf("%v", data))

		default:
			h.logger.Debug("Passing through stream event", zap.String("type", eventType))
			return nil
		}
	}
}

// buildResultParts converts a RetrievalResult into A2A content parts.
func (h *RetrievalAgentHandler) buildResultParts(result *RetrievalResult) a2a.ContentParts {
	var parts a2a.ContentParts

	if result.Generation != "" {
		parts = append(parts, &a2a.TextPart{Text: result.Generation})
	}

	resultData := map[string]any{
		"state":      result.State,
		"hit_count":  len(result.Hits),
		"strategy":   result.StrategyUsed,
		"tool_calls": result.ToolCallsMade,
	}
	if len(result.FollowupQuestions) > 0 {
		resultData["followup_questions"] = result.FollowupQuestions
	}
	parts = append(parts, &a2a.DataPart{Data: resultData})

	return parts
}

// writeFailedStatus writes a failed TaskStatusUpdateEvent to the queue.
func writeFailedStatus(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue, errMsg string) error {
	msg := a2a.NewMessage(a2a.MessageRoleAgent, &a2a.TextPart{Text: errMsg})
	return queue.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateFailed, msg))
}
