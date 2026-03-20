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
	"context"
	"fmt"
	"net/http"

	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/antflydb/antfly/lib/ai"
	generating "github.com/antflydb/antfly/pkg/generating"
	a2afacade "github.com/antflydb/antfly/src/a2a"
	"github.com/antflydb/antfly/src/tablemgr"
	"go.uber.org/zap"
)

// a2aAdapter implements the A2A facade interfaces (RetrievalExecutor and
// QueryBuilderExecutor) by delegating to the existing TableApi methods.
type a2aAdapter struct {
	t *TableApi
}

// newA2AAdapter creates an adapter that bridges the A2A facade interfaces
// to the concrete TableApi methods.
func newA2AAdapter(t *TableApi) *a2aAdapter {
	return &a2aAdapter{t: t}
}

// wrapA2AStreamCallback adapts an a2a StreamCallback (string event types) to
// the metadata package's SSEEvent-typed callback.
func wrapA2AStreamCallback(cb a2afacade.StreamCallback) func(context.Context, SSEEvent, any) error {
	if cb == nil {
		return nil
	}
	return func(ctx context.Context, eventType SSEEvent, data any) error {
		return cb(ctx, string(eventType), data)
	}
}

// ExecuteRetrievalPipeline implements a2afacade.RetrievalExecutor.
func (a *a2aAdapter) ExecuteRetrievalPipeline(ctx context.Context, req *a2afacade.RetrievalRequest, streamCb a2afacade.StreamCallback) (*a2afacade.RetrievalResult, error) {
	antflyReq := a.buildRetrievalRequest(req)
	generator, _ := a.resolveGenerator(ctx, antflyReq)

	result, err := a.t.ExecutePipeline(ctx, antflyReq, generator, wrapA2AStreamCallback(streamCb))
	if err != nil {
		return nil, err
	}
	return a.mapRetrievalResult(result), nil
}

// ExecuteRetrievalAgentic implements a2afacade.RetrievalExecutor.
func (a *a2aAdapter) ExecuteRetrievalAgentic(ctx context.Context, req *a2afacade.RetrievalRequest, streamCb a2afacade.StreamCallback) (*a2afacade.RetrievalResult, error) {
	antflyReq := a.buildRetrievalRequest(req)
	generator, err := a.resolveGenerator(ctx, antflyReq)
	if err != nil {
		return nil, err
	}
	if generator == nil {
		return nil, fmt.Errorf("generator is required for agentic mode")
	}

	result := a.t.RunAgenticRetrieval(ctx, antflyReq, generator, wrapA2AStreamCallback(streamCb))
	return a.mapRetrievalResult(result), nil
}

// ExecuteQueryBuilderA2A implements a2afacade.QueryBuilderExecutor.
func (a *a2aAdapter) ExecuteQueryBuilderA2A(ctx context.Context, req *a2afacade.QueryBuilderRequest) (*a2afacade.QueryBuilderResult, error) {
	antflyReq := &QueryBuilderRequest{
		Intent:       req.Intent,
		Table:        req.Table,
		SchemaFields: req.SchemaFields,
	}

	result, err := a.t.ExecuteQueryBuilder(ctx, antflyReq)
	if err != nil {
		return nil, err
	}

	return &a2afacade.QueryBuilderResult{
		Query:       result.Query,
		Explanation: result.Explanation,
		Confidence:  result.Confidence,
		Warnings:    result.Warnings,
	}, nil
}

func (a *a2aAdapter) buildRetrievalRequest(req *a2afacade.RetrievalRequest) *RetrievalAgentRequest {
	antflyReq := &RetrievalAgentRequest{
		Query:         req.Query,
		MaxIterations: req.MaxIterations,
		Messages:      req.Messages,
	}

	if req.Table != "" {
		antflyReq.Queries = []RetrievalQueryRequest{{
			Table:          req.Table,
			SemanticSearch: req.Query,
		}}
	}

	if req.EnableGeneration {
		enabled := true
		antflyReq.Steps.Generation.Enabled = &enabled
	}

	return antflyReq
}

func (a *a2aAdapter) resolveGenerator(ctx context.Context, req *RetrievalAgentRequest) (*ai.GenKitModelImpl, error) {
	chain := generating.ResolveGeneratorOrChain(req.Generator, req.Chain)
	if len(chain) == 0 {
		chain = generating.GetDefaultChain()
	}
	if len(chain) == 0 {
		return nil, nil
	}
	return ai.NewGenKitGenerator(ctx, chain[0].Generator)
}

// a2aRouteHandlers holds the HTTP handlers for the A2A facade.
type a2aRouteHandlers struct {
	jsonrpcHandler http.Handler
	cardHandler    http.Handler
}

// mountA2ARoutes creates and returns the A2A route handlers.
func mountA2ARoutes(logger *zap.Logger, ln *MetadataStore, tm *tablemgr.TableManager, apiURL string) a2aRouteHandlers {
	tableApi := NewTableApi(logger, ln, tm)
	adapter := newA2AAdapter(tableApi)

	dispatcher := a2afacade.NewDispatcher(logger)
	dispatcher.Register(a2afacade.NewRetrievalAgentHandler(adapter, logger))
	dispatcher.Register(a2afacade.NewQueryBuilderAgentHandler(adapter, logger))

	handler := a2asrv.NewHandler(dispatcher)

	return a2aRouteHandlers{
		jsonrpcHandler: a2asrv.NewJSONRPCHandler(handler),
		cardHandler:    a2asrv.NewAgentCardHandler(a2afacade.NewCardProducer(dispatcher, apiURL)),
	}
}

func (a *a2aAdapter) mapRetrievalResult(result *RetrievalAgentResult) *a2afacade.RetrievalResult {
	r := &a2afacade.RetrievalResult{
		Generation:           result.Generation,
		GenerationConfidence: result.GenerationConfidence,
		FollowupQuestions:    result.FollowupQuestions,
		State:                string(result.Status),
		StrategyUsed:         string(result.StrategyUsed),
		ToolCallsMade:        result.ToolCallsMade,
	}

	// Map hits as []any
	for _, hit := range result.Hits {
		r.Hits = append(r.Hits, hit)
	}

	if result.ClarificationRequest.Question != "" {
		r.ClarificationQuestion = result.ClarificationRequest.Question
	}

	return r
}
