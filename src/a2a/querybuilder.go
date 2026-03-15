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
	"go.uber.org/zap"
)

// QueryBuilderRequest holds the fields needed to invoke the query builder.
type QueryBuilderRequest struct {
	Intent       string
	Table        string
	SchemaFields []string
}

// QueryBuilderResult holds the result of a query builder invocation.
type QueryBuilderResult struct {
	Query       map[string]any
	Explanation string
	Confidence  float64
	Warnings    []string
}

// QueryBuilderExecutor abstracts the query builder methods on TableApi.
type QueryBuilderExecutor interface {
	ExecuteQueryBuilderA2A(ctx context.Context, req *QueryBuilderRequest) (*QueryBuilderResult, error)
}

// QueryBuilderAgentHandler adapts the Antfly query builder agent to the A2A protocol.
type QueryBuilderAgentHandler struct {
	executor QueryBuilderExecutor
	logger   *zap.Logger
}

// NewQueryBuilderAgentHandler creates a new QueryBuilderAgentHandler.
func NewQueryBuilderAgentHandler(executor QueryBuilderExecutor, logger *zap.Logger) *QueryBuilderAgentHandler {
	return &QueryBuilderAgentHandler{
		executor: executor,
		logger:   logger.Named("a2a.query-builder"),
	}
}

func (h *QueryBuilderAgentHandler) SkillID() string { return "query-builder" }

func (h *QueryBuilderAgentHandler) Skill() a2a.AgentSkill {
	return a2a.AgentSkill{
		ID:          "query-builder",
		Name:        "Query Builder",
		Description: "Translates natural language intent into structured Bleve search queries using an LLM.",
		Tags:        []string{"query", "nlp", "bleve"},
		Examples: []string{
			"Find documents about machine learning published after 2023",
			"Search for error logs with status code 500",
		},
		InputModes:  []string{"text", "data"},
		OutputModes: []string{"data"},
	}
}

func (h *QueryBuilderAgentHandler) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, queue eventqueue.Queue) error {
	// Extract intent from message text
	intent := extractTextFromMessage(reqCtx.Message)
	if intent == "" {
		return writeFailedStatus(ctx, reqCtx, queue, "message must contain a text part with the query intent")
	}

	// Extract optional config from DataPart
	data := extractDataFromMessage(reqCtx.Message)

	req := &QueryBuilderRequest{
		Intent: intent,
		Table:  stringFromMap(data, "table", ""),
	}
	if fields, ok := data["schema_fields"]; ok {
		if fieldsList, ok := fields.([]any); ok {
			for _, f := range fieldsList {
				if s, ok := f.(string); ok {
					req.SchemaFields = append(req.SchemaFields, s)
				}
			}
		}
	}

	// Signal working state
	if err := queue.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return fmt.Errorf("writing working status: %w", err)
	}

	// Execute query builder
	result, err := h.executor.ExecuteQueryBuilderA2A(ctx, req)
	if err != nil {
		return writeFailedStatus(ctx, reqCtx, queue, fmt.Sprintf("query builder failed: %v", err))
	}

	// Write result as DataPart artifact
	resultData := map[string]any{
		"query":       result.Query,
		"explanation": result.Explanation,
		"confidence":  result.Confidence,
	}
	if len(result.Warnings) > 0 {
		resultData["warnings"] = result.Warnings
	}

	artifact := &a2a.TaskArtifactUpdateEvent{
		TaskID:    reqCtx.TaskID,
		ContextID: reqCtx.ContextID,
		Artifact: &a2a.Artifact{
			ID:          a2a.NewArtifactID(),
			Name:        "query",
			Description: "Generated Bleve search query",
			Parts: a2a.ContentParts{
				&a2a.DataPart{Data: resultData},
			},
		},
		LastChunk: true,
	}
	if err := queue.Write(ctx, artifact); err != nil {
		return fmt.Errorf("writing artifact: %w", err)
	}

	statusMsg := a2a.NewMessage(a2a.MessageRoleAgent, &a2a.TextPart{Text: result.Explanation})
	return queue.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCompleted, statusMsg))
}

func (h *QueryBuilderAgentHandler) Cancel(_ context.Context, _ *a2asrv.RequestContext, _ eventqueue.Queue) error {
	return nil
}
