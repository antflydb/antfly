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
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/ai/eval"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/websearch"
	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/usermgr"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// retrievalToolExecutor implements ai.RetrievalToolExecutor for the retrieval agent.
// It handles executing search tools against table indexes.
type retrievalToolExecutor struct {
	tableApi          *TableApi
	defaultTable      string // fallback table when not specified per-call
	websearchProvider websearch.SearchProvider
	fetcher           *websearch.Fetcher
	logger            *zap.Logger

	// Accumulated state
	collectedDocuments   []QueryHit
	collectedIDs         map[string]bool // dedup guard for collectedDocuments
	reasoningChain       []RetrievalReasoningStep
	pendingClarification *ai.ClarificationRequest
	appliedFilters       []ai.FilterSpec
	queryResults         map[string][]QueryHit

	// Streaming
	streamCallback func(ctx context.Context, eventType SSEEvent, data any) error
}

// collectHits appends hits to the executor's collected documents (deduplicating by ID)
// and streams them, returning a summary slice suitable for returning to the LLM.
func (e *retrievalToolExecutor) collectHits(ctx context.Context, hits []QueryHit) []map[string]any {
	if e.collectedIDs == nil {
		e.collectedIDs = make(map[string]bool)
	}
	results := make([]map[string]any, 0, len(hits))
	for _, hit := range hits {
		if e.collectedIDs[hit.ID] {
			continue
		}
		e.collectedIDs[hit.ID] = true
		e.collectedDocuments = append(e.collectedDocuments, hit)
		if e.streamCallback != nil {
			_ = e.streamCallback(ctx, SSEEventHit, hit)
		}
		results = append(results, map[string]any{
			"id":     hit.ID,
			"source": hit.Source,
			"score":  hit.Score,
		})
	}
	return results
}

// emitStep wraps a tool execution with step_started/step_completed lifecycle events.
// The fn callback performs the actual work and returns details for the completed step.
func (e *retrievalToolExecutor) emitStep(ctx context.Context, stepName, action string, fn func() (map[string]any, error)) (RetrievalReasoningStep, error) {
	stepID := "step_" + xid.New().String()
	if e.streamCallback != nil {
		if err := e.streamCallback(ctx, SSEEventStepStarted, map[string]any{
			"id": stepID, "step": stepName, "action": action,
		}); err != nil {
			e.logger.Debug("Failed to stream step_started", zap.String("step", stepName), zap.Error(err))
		}
	}
	start := time.Now()
	details, err := fn()
	status := RetrievalReasoningStepStatusSuccess
	var errMsg string
	if err != nil {
		status = RetrievalReasoningStepStatusError
		errMsg = err.Error()
	}
	step := RetrievalReasoningStep{
		Id:           stepID,
		Step:         stepName,
		Action:       action,
		Status:       status,
		ErrorMessage: errMsg,
		DurationMs:   int(time.Since(start).Milliseconds()),
		Details:      details,
	}
	e.reasoningChain = append(e.reasoningChain, step)
	if e.streamCallback != nil {
		if err := e.streamCallback(ctx, SSEEventStepCompleted, step); err != nil {
			e.logger.Debug("Failed to stream step_completed", zap.String("step", stepName), zap.Error(err))
		}
	}
	return step, err
}

// applyGenerationOutput copies generation fields from a GenKit output into the retrieval result.
func applyGenerationOutput(result *RetrievalAgentResult, out *ai.GenerationOutput) {
	result.Generation = out.Generation
	result.GenerationConfidence = out.GenerationConfidence
	result.ContextRelevance = out.ContextRelevance
	result.FollowupQuestions = out.FollowupQuestions
	accumulateUsage(&result.Usage, out.Usage)
}

// accumulateUsage merges token usage from one LLM call into the running totals.
func accumulateUsage(usage *RetrievalAgentUsage, gen *ai.GenerationUsage) {
	if gen == nil {
		return
	}
	usage.InputTokens += gen.InputTokens
	usage.OutputTokens += gen.OutputTokens
	usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	usage.CachedInputTokens += gen.CachedTokens
	usage.LlmCalls++
}

// ExecuteSearch runs a semantic search (legacy ToolExecutor interface)
func (e *retrievalToolExecutor) ExecuteSearch(ctx context.Context, query string, filters []ai.FilterSpec) ([]map[string]any, error) {
	return e.ExecuteSemanticSearch(ctx, e.defaultTable, query, "", 10, filters)
}

// ExecuteSemanticSearch runs a vector/semantic search against the specified table and index
func (e *retrievalToolExecutor) ExecuteSemanticSearch(ctx context.Context, table string, query string, index string, limit int, filters []ai.FilterSpec) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 10
	}

	var results []map[string]any
	_, err := e.emitStep(ctx, string(ai.ToolNameSemanticSearch), fmt.Sprintf("Searching for '%s'", query), func() (map[string]any, error) {
		queryReq := &QueryRequest{
			Table:          table,
			SemanticSearch: query,
			Limit:          limit,
		}
		if index != "" {
			queryReq.Indexes = []string{index}
		}
		applyFiltersToQuery(queryReq, append(e.appliedFilters, filters...), e.logger)

		result := e.tableApi.runQuery(ctx, queryReq)
		if result.Status != 200 {
			return nil, fmt.Errorf("semantic search failed: %s", result.Error)
		}

		results = e.collectHits(ctx, result.Hits.Hits)
		return map[string]any{"index": index, "limit": limit, "results": len(results)}, nil
	})
	return results, err
}

// ExecuteFullTextSearch runs a BM25 full-text search against the specified table and index
func (e *retrievalToolExecutor) ExecuteFullTextSearch(ctx context.Context, table string, query string, index string, limit int, fields []string, filters []ai.FilterSpec) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 10
	}

	var results []map[string]any
	_, err := e.emitStep(ctx, string(ai.ToolNameFullTextSearch), fmt.Sprintf("Full-text search for '%s'", query), func() (map[string]any, error) {
		queryReq := &QueryRequest{
			Table:          table,
			FullTextSearch: json.RawMessage(fmt.Sprintf(`{"query": %q}`, query)),
			Limit:          limit,
		}
		// Don't set Indexes for full-text search: the validation requires
		// semantic_search when indexes are specified, and full-text search
		// uses the _all field by default without needing explicit indexes.
		if len(fields) > 0 {
			queryReq.Fields = fields
		}
		applyFiltersToQuery(queryReq, append(e.appliedFilters, filters...), e.logger)

		result := e.tableApi.runQuery(ctx, queryReq)
		if result.Status != 200 {
			return nil, fmt.Errorf("full text search failed: %s", result.Error)
		}

		results = e.collectHits(ctx, result.Hits.Hits)
		return map[string]any{"index": index, "limit": limit, "fields": fields, "results": len(results)}, nil
	})
	return results, err
}

// ExecuteTreeSearch runs a tree search with beam search navigation on the specified table
func (e *retrievalToolExecutor) ExecuteTreeSearch(ctx context.Context, table string, index string, startNodes string, query string, maxDepth int, beamWidth int) ([]map[string]any, error) {
	if maxDepth <= 0 {
		maxDepth = 3
	}
	if beamWidth <= 0 {
		beamWidth = 3
	}
	if startNodes == "" {
		startNodes = "$roots"
	}

	var results []map[string]any
	_, err := e.emitStep(ctx, string(ai.ToolNameTreeSearch), fmt.Sprintf("Tree search in '%s'", index), func() (map[string]any, error) {
		treeConfig := &TreeSearchConfig{
			Index:      index,
			StartNodes: startNodes,
			MaxDepth:   maxDepth,
			BeamWidth:  beamWidth,
		}

		hits := e.executeTreeSearchInternal(ctx, treeConfig, query, e.queryResults)

		// executeTreeSearchInternal already streams individual hits,
		// so skip streaming here but still dedup into collectedDocuments.
		if e.collectedIDs == nil {
			e.collectedIDs = make(map[string]bool)
		}
		for _, hit := range hits {
			if !e.collectedIDs[hit.ID] {
				e.collectedIDs[hit.ID] = true
				e.collectedDocuments = append(e.collectedDocuments, hit)
			}
			results = append(results, map[string]any{
				"id":     hit.ID,
				"source": hit.Source,
				"score":  hit.Score,
			})
		}
		return map[string]any{
			"index": index, "start_nodes": startNodes,
			"max_depth": maxDepth, "beam_width": beamWidth, "results": len(results),
		}, nil
	})
	return results, err
}

// ExecuteGraphSearch runs a graph traversal search on the specified table
func (e *retrievalToolExecutor) ExecuteGraphSearch(ctx context.Context, tableName string, index string, startNode string, edgeType string, direction string, depth int) ([]map[string]any, error) {
	if depth <= 0 {
		depth = 1
	}
	if direction == "" {
		direction = "outgoing"
	}

	var results []map[string]any
	_, err := e.emitStep(ctx, string(ai.ToolNameGraphSearch), fmt.Sprintf("Graph traversal from '%s'", startNode), func() (map[string]any, error) {
		// Get table
		table, _, err := e.tableApi.tm.GetTableWithShardStatuses(tableName)
		if err != nil {
			return nil, fmt.Errorf("get table: %w", err)
		}

		// Convert direction
		edgeDir := indexes.EdgeDirectionOut
		switch direction {
		case "incoming":
			edgeDir = indexes.EdgeDirectionIn
		case "both":
			edgeDir = indexes.EdgeDirectionBoth
		}

		// Traverse edges with parallel edge lookups and batched document fetches
		currentNodes := []string{startNode}
		visited := make(map[string]bool)
		visited[startNode] = true

		for d := 0; d < depth && len(currentNodes) > 0; d++ {
			// Parallel edge lookups for all nodes at this depth level
			rg, _ := workerpool.NewResultGroup[[]indexes.Edge](ctx, e.tableApi.pool)
			for _, nodeKey := range currentNodes {
				rg.Go(func(ctx context.Context) ([]indexes.Edge, error) {
					edges, err := e.tableApi.ln.getEdgesAcrossShards(ctx, table, index, nodeKey, edgeType, edgeDir)
					if err != nil {
						e.logger.Debug("Failed to get edges", zap.String("node", nodeKey), zap.Error(err))
						return nil, nil
					}
					return edges, nil
				})
			}
			edgeResults, _ := rg.Wait()

			// Collect unique target keys and their edge metadata
			type targetInfo struct {
				key      string
				edgeType string
			}
			var targets []targetInfo
			for _, edges := range edgeResults {
				for _, edge := range edges {
					targetKey := string(edge.Target)
					if edgeDir == indexes.EdgeDirectionIn {
						targetKey = string(edge.Source)
					}
					if !visited[targetKey] {
						visited[targetKey] = true
						targets = append(targets, targetInfo{key: targetKey, edgeType: edge.Type})
					}
				}
			}

			if len(targets) == 0 {
				break
			}

			// Batch document lookup for all discovered targets
			targetKeys := make([]string, len(targets))
			for i, t := range targets {
				targetKeys[i] = t.key
			}
			docs, err := e.tableApi.batchLookupDocuments(ctx, e.defaultTable, targetKeys)
			if err != nil {
				e.logger.Debug("Batch document lookup failed", zap.Error(err))
			}

			var nextNodes []string
			for _, t := range targets {
				nextNodes = append(nextNodes, t.key)
				doc, ok := docs[t.key]
				if !ok {
					continue
				}

				hit := QueryHit{
					ID:     t.key,
					Source: doc,
					Score:  1.0,
				}
				e.collectHits(ctx, []QueryHit{hit})

				results = append(results, map[string]any{
					"id":        t.key,
					"source":    doc,
					"edge_type": t.edgeType,
					"depth":     d + 1,
				})
			}
			currentNodes = nextNodes
		}
		return map[string]any{
			"index": index, "start_node": startNode, "edge_type": edgeType,
			"direction": direction, "depth": depth, "results": len(results),
		}, nil
	})
	return results, err
}

// ExecuteWebSearch runs a web search
func (e *retrievalToolExecutor) ExecuteWebSearch(ctx context.Context, query string, numResults int) ([]map[string]any, error) {
	if e.websearchProvider == nil {
		return []map[string]any{
			{
				"error":   "Web search provider not configured",
				"message": "Web search requires a configured search provider. Set websearch_config in steps.tools.",
			},
		}, nil
	}

	if numResults <= 0 {
		numResults = 5
	}

	var results []map[string]any
	_, err := e.emitStep(ctx, string(ai.ToolNameWebSearch), fmt.Sprintf("Web search for '%s'", query), func() (map[string]any, error) {
		response, err := e.websearchProvider.Search(ctx, query, websearch.SearchOptions{
			MaxResults: numResults,
		})
		if err != nil {
			e.logger.Error("Web search failed", zap.Error(err), zap.String("query", query))
			return nil, fmt.Errorf("web search failed: %w", err)
		}

		for _, r := range response.Results {
			results = append(results, map[string]any{
				"title":   r.Title,
				"url":     r.Url,
				"snippet": r.Snippet,
				"source":  r.Source,
			})
		}

		if response.Answer != "" {
			results = append([]map[string]any{{
				"type": "answer", "answer": response.Answer,
			}}, results...)
		}
		return map[string]any{"results": len(results)}, nil
	})
	if err != nil {
		// Return error as result instead of failing the tool call
		return []map[string]any{
			{"error": "Web search failed", "message": err.Error()},
		}, nil
	}
	return results, nil
}

// ExecuteFetch fetches content from a URL
func (e *retrievalToolExecutor) ExecuteFetch(ctx context.Context, url string) (map[string]any, error) {
	if e.fetcher == nil {
		return map[string]any{
			"error":   "Fetch not configured",
			"message": "URL fetching requires security configuration. Set fetch_config in steps.tools.",
		}, nil
	}

	var fetchResult map[string]any
	_, err := e.emitStep(ctx, string(ai.ToolNameFetch), fmt.Sprintf("Fetching %s", url), func() (map[string]any, error) {
		result, err := e.fetcher.Fetch(ctx, url)
		if err != nil {
			e.logger.Error("URL fetch failed", zap.Error(err), zap.String("url", url))
			return nil, fmt.Errorf("fetch failed: %w", err)
		}

		fetchResult = map[string]any{
			"url":           result.Url,
			"title":         result.Title,
			"content":       result.Content,
			"content_type":  result.ContentType,
			"truncated":     result.Truncated,
			"fetch_time_ms": result.FetchTimeMs,
		}
		return map[string]any{"url": url, "content_type": result.ContentType}, nil
	})
	if err != nil {
		return map[string]any{
			"error": "Fetch failed", "message": err.Error(), "url": url,
		}, nil
	}
	return fetchResult, nil
}

// SetClarification captures a clarification request.
func (e *retrievalToolExecutor) SetClarification(_ context.Context, clarification ai.ClarificationRequest) {
	e.pendingClarification = &clarification
}

// collectExecutorResults copies accumulated results from the executor into the agent result.
func collectExecutorResults(executor *retrievalToolExecutor, result *RetrievalAgentResult) {
	result.Hits = executor.collectedDocuments
	result.ReasoningChain = append(result.ReasoningChain, executor.reasoningChain...)
	result.AppliedFilters = executor.appliedFilters
	result.ToolCallsMade = len(executor.reasoningChain)
}

// applyPendingClarification sets the result status to incomplete with clarification details if the executor
// has a pending clarification request.
func applyPendingClarification(executor *retrievalToolExecutor, result *RetrievalAgentResult) {
	if executor.pendingClarification == nil {
		return
	}
	result.Status = RetrievalAgentStatusIncomplete
	result.IncompleteDetails = IncompleteDetails{Reason: IncompleteDetailsReasonClarificationNeeded}
	cr := executor.pendingClarification
	result.ClarificationRequest = ClarificationRequest{
		Question: cr.Question,
	}
	if cr.Options != nil {
		result.ClarificationRequest.Options = *cr.Options
	}
}

// AddFilter adds a filter to the accumulated filters.
func (e *retrievalToolExecutor) AddFilter(ctx context.Context, filter ai.FilterSpec) {
	e.appliedFilters = append(e.appliedFilters, filter)
	e.emitStep(ctx, string(ai.ToolNameFilter), fmt.Sprintf("Added filter: %s %s", filter.Field, filter.Operator), func() (map[string]any, error) { //nolint:errcheck,gosec // fire-and-forget
		return map[string]any{"field": filter.Field, "operator": string(filter.Operator), "value": filter.Value}, nil
	})
}

// buildRetrievalClassificationOptions builds classification step options from RetrievalAgentSteps.
func buildRetrievalClassificationOptions(steps RetrievalAgentSteps, agentKnowledge string) []ai.GenerationOption {
	var opts []ai.GenerationOption
	if steps.Classification.Enabled != nil && *steps.Classification.Enabled {
		if steps.Classification.WithReasoning != nil && *steps.Classification.WithReasoning {
			opts = append(opts, ai.WithClassificationReasoning(true))
		}
	}
	if agentKnowledge != "" {
		opts = append(opts, ai.WithAgentKnowledge(agentKnowledge))
	}
	return opts
}

// buildRetrievalGenerationOptions builds generation options from RetrievalAgentSteps and request.
func buildRetrievalGenerationOptions(steps RetrievalAgentSteps, req *RetrievalAgentRequest, reasoning string) []ai.GenerationOption {
	var opts []ai.GenerationOption

	if steps.Generation.Enabled != nil && *steps.Generation.Enabled {
		if steps.Generation.SystemPrompt != nil && *steps.Generation.SystemPrompt != "" {
			opts = append(opts, ai.WithGenerationSystemPrompt(*steps.Generation.SystemPrompt))
		}
		if steps.Generation.GenerationContext != nil && *steps.Generation.GenerationContext != "" {
			opts = append(opts, ai.WithGenerationContext(*steps.Generation.GenerationContext))
		}
	}

	if steps.Confidence != (ai.ConfidenceStepConfig{}) {
		if steps.Confidence.Enabled != nil && *steps.Confidence.Enabled {
			opts = append(opts, ai.WithConfidence(true))
		}
		if steps.Confidence.Context != nil && *steps.Confidence.Context != "" {
			opts = append(opts, ai.WithConfidenceContext(*steps.Confidence.Context))
		}
	}

	if steps.Followup != (ai.FollowupStepConfig{}) {
		if steps.Followup.Enabled != nil && *steps.Followup.Enabled {
			opts = append(opts, ai.WithFollowup(true))
		}
		if steps.Followup.Context != nil && *steps.Followup.Context != "" {
			opts = append(opts, ai.WithFollowupContext(*steps.Followup.Context))
		}
	}

	if reasoning != "" {
		opts = append(opts, ai.WithReasoning(reasoning))
	}
	if req.AgentKnowledge != "" {
		opts = append(opts, ai.WithAgentKnowledge(req.AgentKnowledge))
	}

	return opts
}

// runClassificationStep runs the classification pre-step if configured.
// Returns the classification result (nil if not configured).
func (t *TableApi) runClassificationStep(
	ctx context.Context,
	req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
	streamCallback func(ctx context.Context, eventType SSEEvent, data any) error,
) (*ai.ClassificationTransformationResult, error) {
	if req.Steps.Classification.Enabled == nil || !*req.Steps.Classification.Enabled {
		return nil, nil
	}

	classOpts := buildRetrievalClassificationOptions(req.Steps, req.AgentKnowledge)

	if streamCallback != nil {
		classOpts = append(classOpts, ai.WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
			if SSEEvent(eventType) == SSEEventReasoning {
				return streamCallback(ctx, SSEEventReasoning, data)
			}
			return nil
		}))
	}

	enhanced, err := generator.ClassifyImproveAndTransformQuery(ctx, req.Query, classOpts...)
	if err != nil {
		return nil, fmt.Errorf("classification failed: %w", err)
	}

	// Stream classification event (clear reasoning since it was streamed separately)
	if streamCallback != nil {
		classificationEvent := *enhanced
		classificationEvent.Reasoning = ""
		_ = streamCallback(ctx, SSEEventClassification, &classificationEvent)
	}

	return enhanced, nil
}

// runGenerationStep runs the generation post-step if configured.
// Populates result with answer, confidence, followup, and eval fields.
func (t *TableApi) runGenerationStep(
	ctx context.Context,
	req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
	result *RetrievalAgentResult,
	classification *ai.ClassificationTransformationResult,
	streamCallback func(ctx context.Context, eventType SSEEvent, data any) error,
) {
	if req.Steps.Generation.Enabled == nil || !*req.Steps.Generation.Enabled {
		return
	}

	// Convert retrieved documents to schema.Document
	docs := convertQueryHitsToDocuments(result.Hits)

	// Apply token pruning if max_context_tokens is set
	prunedDocs, pruneStats := t.applyTokenPruning(
		docs,
		req.MaxContextTokens,
		req.ReserveTokens,
		req.DocumentRenderer,
		t.logger,
	)
	if pruneStats != nil {
		result.Usage.PruneStats = PruneStats{
			ResourcesKept:   pruneStats.ResourcesKept,
			ResourcesPruned: pruneStats.ResourcesPruned,
			TokensKept:      pruneStats.TokensKept,
			TokensPruned:    pruneStats.TokensPruned,
		}
	}

	// Provide default classification if none was run
	if classification == nil {
		classification = &ai.ClassificationTransformationResult{
			RouteType: ai.RouteTypeSearch,
		}
	}

	reasoning := classification.Reasoning

	generationOpts := buildRetrievalGenerationOptions(req.Steps, req, reasoning)

	if streamCallback != nil {
		// Use errgroup for async streaming of generation chunks
		llmEventChan := make(chan sseEvent, 100)
		eg, egCtx := errgroup.WithContext(ctx)

		var generationOutput *ai.GenerationOutput

		eg.Go(func() error {
			defer close(llmEventChan)

			streamOpt := ai.WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				select {
				case llmEventChan <- sseEvent{Type: SSEEvent(eventType), Data: data}:
					return nil
				case <-egCtx.Done():
					return egCtx.Err()
				}
			})

			var err error
			generationOutput, err = generator.GenerateQueryResponse(
				egCtx, req.Query, prunedDocs, classification,
				append(generationOpts, streamOpt)...)
			return err
		})

		// Stream LLM events
		for event := range llmEventChan {
			_ = streamCallback(ctx, event.Type, event.Data)
		}

		if err := eg.Wait(); err != nil {
			if !errors.Is(err, context.Canceled) {
				t.logger.Error("Generation step failed", zap.Error(err))
			}
			return
		}

		if generationOutput != nil {
			applyGenerationOutput(result, generationOutput)
			// Note: followup questions are already streamed by Generate() via streamCallback
		}

		// Run eval if configured
		t.runEvalStep(ctx, req, prunedDocs, generationOutput, result, streamCallback)

	} else {
		// JSON mode: synchronous generation
		generationOutput, err := generator.GenerateQueryResponse(
			ctx, req.Query, prunedDocs, classification, generationOpts...)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.logger.Error("Generation step failed", zap.Error(err))
			}
			return
		}

		if generationOutput != nil {
			applyGenerationOutput(result, generationOutput)
		}

		// Run eval if configured
		t.runEvalStep(ctx, req, prunedDocs, generationOutput, result, nil)
	}

}

// runEvalStep runs inline evaluation if configured in steps.eval.
func (t *TableApi) runEvalStep(
	ctx context.Context,
	req *RetrievalAgentRequest,
	prunedDocs []schema.Document,
	generationOutput *ai.GenerationOutput,
	result *RetrievalAgentResult,
	streamCallback func(ctx context.Context, eventType SSEEvent, data any) error,
) {
	if len(req.Steps.Eval.Evaluators) == 0 {
		return
	}

	var answer string
	if generationOutput != nil {
		answer = generationOutput.Generation
	}

	contextAny := make([]any, len(prunedDocs))
	for i, doc := range prunedDocs {
		contextAny[i] = doc
	}

	retrievedIDs := make([]string, len(prunedDocs))
	for i, doc := range prunedDocs {
		retrievedIDs[i] = doc.ID
	}

	evalInput := eval.EvaluateInput{
		Config:       req.Steps.Eval,
		Query:        req.Query,
		Output:       answer,
		RetrievedIDs: retrievedIDs,
		Context:      contextAny,
	}

	evalResult, err := eval.NewOrchestrator().Evaluate(ctx, evalInput)
	if err != nil {
		t.logger.Warn("Eval step failed", zap.Error(err))
		return
	}

	if evalResult != nil {
		result.EvalResult = *evalResult
		if streamCallback != nil {
			_ = streamCallback(ctx, SSEEventEval, evalResult)
		}
	}
}

// RetrievalAgent implements the document retrieval endpoint
func (t *TableApi) RetrievalAgent(w http.ResponseWriter, r *http.Request) {
	// Auth check
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeRead) {
		return
	}

	// Decode request
	var req RetrievalAgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	t.logger.Debug("RetrievalAgent request received",
		zap.String("query", req.Query),
		zap.Int("num_queries", len(req.Queries)),
		zap.Int("max_iterations", req.MaxIterations),
		zap.Bool("stream", req.Stream),
		zap.String("generator.provider", string(req.Generator.Provider)),
		zap.Int("chain_len", len(req.Chain)),
	)

	// Validate request
	if req.Query == "" {
		errorResponse(w, "query cannot be empty", http.StatusBadRequest)
		return
	}
	if len(req.Queries) == 0 {
		errorResponse(w, "queries cannot be empty", http.StatusBadRequest)
		return
	}

	// Collect unique tables from queries
	tables := collectTablesFromQueries(req.Queries)
	if len(tables) == 0 {
		errorResponse(w, "at least one query must specify a table", http.StatusBadRequest)
		return
	}

	// Resolve generator chain (needed for agentic mode, when a generator is
	// explicitly provided, or when classification/generation steps are configured)
	needsGenerator := req.MaxIterations > 0 ||
		req.Generator.Provider != "" ||
		len(req.Chain) > 0 ||
		(req.Steps.Classification.Enabled != nil && *req.Steps.Classification.Enabled) ||
		(req.Steps.Generation.Enabled != nil && *req.Steps.Generation.Enabled)
	var generator *ai.GenKitModelImpl

	if needsGenerator {
		chain := ai.ResolveGeneratorOrChain(req.Generator, req.Chain)
		if len(chain) == 0 {
			defaultChain := ai.GetDefaultChain()
			if len(defaultChain) == 0 {
				if req.MaxIterations != 0 {
					errorResponse(w, "either 'generator' or 'chain' must be provided (no default chain configured)", http.StatusBadRequest)
					return
				}
				// Pipeline mode without generator steps — proceed without LLM
			} else {
				chain = defaultChain
			}
		}

		if len(chain) > 0 {
			var err error
			generator, err = ai.NewGenKitGenerator(r.Context(), chain[0].Generator)
			if err != nil {
				errorResponse(w, fmt.Sprintf("Failed to create generator: %v", err), http.StatusBadRequest)
				return
			}
		}
	}

	// Pipeline mode: max_iterations=0 (default)
	if req.MaxIterations == 0 {
		if req.Stream {
			t.streamRetrievalPipeline(w, r, &req, generator)
		} else {
			t.jsonRetrievalPipeline(w, r, &req, generator)
		}
		return
	}

	// Agentic mode: LLM-driven tool calling (max_iterations > 0)

	if generator == nil {
		errorResponse(w, "generator is required for agentic mode", http.StatusBadRequest)
		return
	}

	if req.Stream {
		t.streamRetrievalAgentic(w, r, &req, generator)
	} else {
		t.jsonRetrievalAgentic(w, r, &req, generator)
	}
}

// streamRetrievalPipeline executes a query pipeline and streams results
func (t *TableApi) streamRetrievalPipeline(
	w http.ResponseWriter, r *http.Request, req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	rc := http.NewResponseController(w)
	ctx := r.Context()

	streamCb := func(ctx context.Context, eventType SSEEvent, data any) error {
		return streamEvent(w, rc, eventType, data)
	}

	result, err := t.ExecutePipeline(ctx, req, generator, streamCb)
	if err != nil {
		_ = streamEvent(w, rc, SSEEventError, map[string]string{"error": err.Error()})
		return
	}

	if err := streamEvent(w, rc, SSEEventDone, result); err != nil {
		if !errors.Is(err, context.Canceled) {
			t.logger.Error("Failed to stream done event", zap.Error(err))
		}
	}
}

// jsonRetrievalPipeline executes a query pipeline and returns JSON
func (t *TableApi) jsonRetrievalPipeline(
	w http.ResponseWriter, r *http.Request, req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
) {
	result, err := t.ExecutePipeline(r.Context(), req, generator, nil)
	if err != nil {
		errorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		t.logger.Error("Failed to encode response", zap.Error(err))
	}
}

// collectTablesFromQueries returns unique table names from the queries.
func collectTablesFromQueries(queries []RetrievalQueryRequest) []string {
	seen := make(map[string]bool)
	var tables []string
	for _, q := range queries {
		if q.Table != "" && !seen[q.Table] {
			seen[q.Table] = true
			tables = append(tables, q.Table)
		}
	}
	return tables
}

// queryRequestFromRetrieval converts a RetrievalQueryRequest to a QueryRequest
// for execution, copying the relevant search fields.
func queryRequestFromRetrieval(qr *RetrievalQueryRequest) *QueryRequest {
	return &QueryRequest{
		Table:             qr.Table,
		SemanticSearch:    qr.SemanticSearch,
		FullTextSearch:    qr.FullTextSearch,
		Indexes:           qr.Indexes,
		Fields:            qr.Fields,
		Limit:             qr.Limit,
		Offset:            qr.Offset,
		FilterPrefix:      qr.FilterPrefix,
		FilterQuery:       qr.FilterQuery,
		ExclusionQuery:    qr.ExclusionQuery,
		Reranker:          qr.Reranker,
		Pruner:            qr.Pruner,
		OrderBy:           qr.OrderBy,
		Aggregations:      qr.Aggregations,
		GraphSearches:     qr.GraphSearches,
		Join:              qr.Join,
		MergeConfig:       qr.MergeConfig,
		Embeddings:        qr.Embeddings,
		EmbeddingTemplate: qr.EmbeddingTemplate,
		DocumentRenderer:  qr.DocumentRenderer,
		DistanceUnder:     qr.DistanceUnder,
		DistanceOver:      qr.DistanceOver,
		ExpandStrategy:    QueryRequestExpandStrategy(qr.ExpandStrategy),
		Count:             qr.Count,
		Analyses:          qr.Analyses,
	}
}

// ExecutePipeline runs queries in order (pipeline mode: max_iterations=0).
// It executes classification, retrieval queries, and generation steps sequentially.
func (t *TableApi) ExecutePipeline(
	ctx context.Context,
	req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
	streamCallback func(ctx context.Context, eventType SSEEvent, data any) error,
) (*RetrievalAgentResult, error) {
	result := &RetrievalAgentResult{
		Id:             "ragr_" + xid.New().String(),
		CreatedAt:      time.Now().Unix(),
		Hits:           []QueryHit{},
		ReasoningChain: []RetrievalReasoningStep{},
		Status:         RetrievalAgentStatusCompleted,
	}
	if generator != nil {
		result.Model = generator.Model.Name()
	}

	// Classification pre-step (optional, requires generator)
	var classification *ai.ClassificationTransformationResult
	if generator != nil && req.Steps.Classification.Enabled != nil && *req.Steps.Classification.Enabled {
		var err error
		classification, err = t.runClassificationStep(ctx, req, generator, streamCallback)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.logger.Error("Classification step failed in pipeline", zap.Error(err))
			}
			// Continue without classification
		}
		if classification != nil {
			result.Classification = *classification
		}
	}

	// Run all search queries in parallel
	var queryReqs []QueryRequest
	queryIdxMap := make([]int, 0, len(req.Queries)) // maps queryReqs index → req.Queries index
	for i, qr := range req.Queries {
		hasSearchFields := qr.SemanticSearch != "" || len(qr.FullTextSearch) > 0 || len(qr.FilterQuery) > 0
		if hasSearchFields {
			queryReqs = append(queryReqs, *queryRequestFromRetrieval(&qr))
			queryIdxMap = append(queryIdxMap, i)
		}
	}
	queryResults := t.runQueries(ctx, queryReqs)

	// Index query results by their original req.Queries position
	queryHitsPerQuery := make(map[int][]QueryHit, len(queryResults))
	for ri, qResult := range queryResults {
		origIdx := queryIdxMap[ri]
		if qResult.Status != 200 {
			return nil, fmt.Errorf("query failed (table=%s): %s", req.Queries[origIdx].Table, qResult.Error)
		}
		queryHitsPerQuery[origIdx] = qResult.Hits.Hits
	}

	// Stream hits and run tree searches in order
	allHits := []QueryHit{}
	for i, qr := range req.Queries {
		queryHits := queryHitsPerQuery[i]
		if streamCallback != nil {
			for _, hit := range queryHits {
				_ = streamCallback(ctx, SSEEventHit, hit)
			}
		}
		allHits = append(allHits, queryHits...)

		// Execute tree search (if configured)
		if qr.TreeSearch.Index != "" {
			treeConfig := qr.TreeSearch
			if len(queryHits) > 0 && treeConfig.StartNodes == "" {
				var ids []string
				for _, hit := range queryHits {
					ids = append(ids, hit.ID)
				}
				treeConfig.StartNodes = strings.Join(ids, ",")
			}
			executor := &retrievalToolExecutor{
				tableApi:       t,
				defaultTable:   qr.Table,
				logger:         t.logger,
				streamCallback: streamCallback,
			}
			_, _ = executor.emitStep(ctx, "tree_search", fmt.Sprintf("Tree search in '%s'", treeConfig.Index), func() (map[string]any, error) {
				hits := executor.executeTreeSearchInternal(ctx, &treeConfig, req.Query, nil)
				allHits = append(allHits, hits...)
				return map[string]any{"collected": len(hits)}, nil
			})
			result.ReasoningChain = append(result.ReasoningChain, executor.reasoningChain...)
		}
	}

	result.Hits = allHits
	result.ReasoningChain = append(result.ReasoningChain, RetrievalReasoningStep{
		Step:   "pipeline",
		Action: fmt.Sprintf("Pipeline executed: %d queries, %d documents", len(req.Queries), len(allHits)),
	})

	// Generation post-step (optional, requires generator)
	if generator != nil {
		t.runGenerationStep(ctx, req, generator, result, classification, streamCallback)
	}

	return result, nil
}

// streamRetrievalAgentic runs the agentic tool-calling loop and streams results
func (t *TableApi) streamRetrievalAgentic(
	w http.ResponseWriter, r *http.Request,
	req *RetrievalAgentRequest, generator *ai.GenKitModelImpl,
) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	rc := http.NewResponseController(w)
	ctx := r.Context()

	streamCb := func(ctx context.Context, eventType SSEEvent, data any) error {
		return streamEvent(w, rc, eventType, data)
	}

	result := t.RunAgenticRetrieval(ctx, req, generator, streamCb)

	if err := streamEvent(w, rc, SSEEventDone, result); err != nil {
		if !errors.Is(err, context.Canceled) {
			t.logger.Error("Failed to stream done event", zap.Error(err))
		}
	}
}

// jsonRetrievalAgentic runs the agentic tool-calling loop and returns JSON
func (t *TableApi) jsonRetrievalAgentic(
	w http.ResponseWriter, r *http.Request,
	req *RetrievalAgentRequest, generator *ai.GenKitModelImpl,
) {
	result := t.RunAgenticRetrieval(r.Context(), req, generator, nil)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		t.logger.Error("Failed to encode response", zap.Error(err))
	}
}

// RunAgenticRetrieval executes the LLM-driven tool-calling loop (agentic mode: max_iterations > 0).
// It uses tool-calling to iteratively search and refine results.
func (t *TableApi) RunAgenticRetrieval(
	ctx context.Context,
	req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
	streamCallback func(ctx context.Context, eventType SSEEvent, data any) error,
) *RetrievalAgentResult {
	result := &RetrievalAgentResult{
		Id:             "ragr_" + xid.New().String(),
		CreatedAt:      time.Now().Unix(),
		Model:          generator.Model.Name(),
		Hits:           []QueryHit{},
		ReasoningChain: []RetrievalReasoningStep{},
		Status:         RetrievalAgentStatusCompleted,
	}

	// Classification pre-step (optional)
	classification, err := t.runClassificationStep(ctx, req, generator, streamCallback)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			t.logger.Error("Classification step failed", zap.Error(err))
		}
		if streamCallback != nil {
			_ = streamCallback(ctx, SSEEventError, map[string]string{"error": err.Error()})
		}
		// Continue without classification
	}
	if classification != nil {
		result.Classification = *classification
	}

	// Introspect indexes from all tables referenced in queries
	tables := collectTablesFromQueries(req.Queries)
	var availableIndexes []ai.IndexInfo
	for _, tableName := range tables {
		availableIndexes = append(availableIndexes, t.getTableIndexes(tableName)...)
	}

	// Resolve tool configuration
	var toolsConfig ai.ChatToolsConfig
	if req.Steps.Tools.EnabledTools != nil {
		toolsConfig = req.Steps.Tools
	}

	// Create the tool executor
	executor := &retrievalToolExecutor{
		tableApi:       t,
		defaultTable:   tables[0],
		logger:         t.logger,
		queryResults:   make(map[string][]QueryHit),
		streamCallback: streamCallback,
	}

	// Apply any pre-accumulated filters
	if len(req.AccumulatedFilters) > 0 {
		executor.appliedFilters = append(executor.appliedFilters, req.AccumulatedFilters...)
	}

	// Initialize websearch provider if configured
	if toolsConfig.WebsearchConfig != nil {
		provider, err := websearch.NewSearchProvider(*toolsConfig.WebsearchConfig)
		if err != nil {
			t.logger.Warn("Failed to create websearch provider", zap.Error(err))
		} else {
			executor.websearchProvider = provider
		}
	}

	// Initialize URL fetcher if configured
	if toolsConfig.FetchConfig != nil {
		executor.fetcher = websearch.NewFetcher(*toolsConfig.FetchConfig)
	}

	// Check provider capabilities
	caps := ai.GetProviderCapabilities(ai.GeneratorProvider(req.Generator.Provider))

	if caps.SupportsTools {
		t.runAgenticWithTools(ctx, req, generator, executor, availableIndexes, toolsConfig, result)
	} else {
		t.runAgenticWithStructuredOutput(ctx, req, generator, executor, availableIndexes, result)
	}

	// Generation post-step (optional)
	t.runGenerationStep(ctx, req, generator, result, classification, streamCallback)

	// Set resources_retrieved regardless of whether generation ran
	result.Usage.ResourcesRetrieved = len(result.Hits)

	// Append assistant message with generation output for multi-turn context
	if result.Generation != "" {
		result.Messages = append(result.Messages, ai.ChatMessage{
			Role:    ai.ChatMessageRoleAssistant,
			Content: result.Generation,
		})
	}

	return result
}

// runAgenticWithTools runs the agentic loop using native tool calling
func (t *TableApi) runAgenticWithTools(
	ctx context.Context,
	req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
	executor *retrievalToolExecutor,
	availableIndexes []ai.IndexInfo,
	toolsConfig ai.ChatToolsConfig,
	result *RetrievalAgentResult,
) {
	// Create native genkit tools from available indexes
	tools := ai.CreateRetrievalTools(generator.Genkit, toolsConfig, executor, availableIndexes)

	if len(tools) == 0 {
		t.logger.Warn("No tools created for retrieval agent")
		result.Status = RetrievalAgentStatusIncomplete
		result.IncompleteDetails = IncompleteDetails{Reason: IncompleteDetailsReasonNoTools}
		return
	}

	// Stream tool mode event
	if executor.streamCallback != nil {
		_ = executor.streamCallback(ctx, SSEEventToolMode, map[string]any{
			"mode":        "native",
			"tools_count": len(tools),
		})
	}

	// Build system prompt
	var toolDefs []string
	for _, tool := range tools {
		toolDefs = append(toolDefs, fmt.Sprintf("- **%s**", tool.Name()))
	}
	systemPrompt := ai.RetrievalAgentSystemPrompt(availableIndexes, toolDefs, "", req.AgentKnowledge)

	// Build conversation messages
	messages := req.Messages
	if len(messages) == 0 {
		messages = req.Context // backward compat
	}

	// Generate with tools
	generationOpts := []ai.GenerationOption{
		ai.WithGenerationSystemPrompt(systemPrompt),
		ai.WithTools(tools),
		ai.WithMaxToolIterations(req.MaxIterations),
	}

	// Pass conversation history for multi-turn chat
	if len(messages) > 0 {
		generationOpts = append(generationOpts, ai.WithMessages(ai.ChatMessagesToGenKit(messages)))
	}

	if executor.streamCallback != nil {
		generationOpts = append(generationOpts, ai.WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
			return executor.streamCallback(ctx, SSEEvent(eventType), data)
		}))
	}

	// Provide default classification to avoid nil error
	defaultClassification := &ai.ClassificationTransformationResult{
		RouteType: ai.RouteTypeSearch,
	}
	genOutput, err := generator.GenerateQueryResponse(ctx, req.Query, nil, defaultClassification, generationOpts...)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			t.logger.Error("Agentic retrieval failed", zap.Error(err))
		}
		result.Status = RetrievalAgentStatusFailed
		result.ReasoningChain = append(result.ReasoningChain, RetrievalReasoningStep{
			Step:   "error",
			Action: fmt.Sprintf("Agentic retrieval failed: %v", err),
		})
	}
	if genOutput != nil {
		accumulateUsage(&result.Usage, genOutput.Usage)
	}

	// Collect results from executor
	collectExecutorResults(executor, result)
	applyPendingClarification(executor, result)

	// Populate response messages for multi-turn chat.
	// Include the input messages plus a new user message with the current query
	// and an assistant message placeholder (generation content is added in runGenerationStep).
	result.Messages = append(result.Messages, messages...)
	result.Messages = append(result.Messages, ai.ChatMessage{
		Role:    ai.ChatMessageRoleUser,
		Content: req.Query,
	})
}

// runAgenticWithStructuredOutput runs the agentic loop using XML-tagged structured output
// (for providers like Ollama that don't support native tool calling)
func (t *TableApi) runAgenticWithStructuredOutput(
	ctx context.Context,
	req *RetrievalAgentRequest,
	generator *ai.GenKitModelImpl,
	executor *retrievalToolExecutor,
	availableIndexes []ai.IndexInfo,
	result *RetrievalAgentResult,
) {
	// Stream tool mode event
	if executor.streamCallback != nil {
		_ = executor.streamCallback(ctx, SSEEventToolMode, map[string]any{
			"mode": "structured_output",
		})
	}

	// Build system prompt for structured output
	systemPrompt := ai.RetrievalAgentSystemPromptWithoutTools(availableIndexes, "", req.AgentKnowledge)

	// Run iterative structured output loop
	hitMaxIterations := true
	for iteration := 0; iteration < req.MaxIterations; iteration++ {
		// Call LLM
		response, usage, err := generator.RAG(ctx, nil,
			ai.WithPromptTemplate(req.Query),
			ai.WithSystemPrompt(systemPrompt),
		)
		accumulateUsage(&result.Usage, usage)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.logger.Error("Structured output LLM call failed", zap.Error(err))
			}
			result.Status = RetrievalAgentStatusFailed
			hitMaxIterations = false
			break
		}

		// Parse tool actions from response
		actions, _, err := ai.ParseStructuredOutput(response)
		if err != nil || len(actions) == 0 {
			// No more actions — LLM is done
			hitMaxIterations = false
			break
		}

		// Execute each action
		for _, action := range actions {
			actionTable, _ := action.Arguments["table"].(string)
			if actionTable == "" {
				actionTable = executor.defaultTable
			}
			switch action.ToolName {
			case ai.ToolNameSemanticSearch:
				query, _ := action.Arguments["query"].(string)
				index, _ := action.Arguments["index"].(string)
				_, _ = executor.ExecuteSemanticSearch(ctx, actionTable, query, index, 10, nil)

			case ai.ToolNameFullTextSearch:
				query, _ := action.Arguments["query"].(string)
				index, _ := action.Arguments["index"].(string)
				_, _ = executor.ExecuteFullTextSearch(ctx, actionTable, query, index, 10, nil, nil)

			case ai.ToolNameTreeSearch:
				query, _ := action.Arguments["query"].(string)
				index, _ := action.Arguments["index"].(string)
				startNodes, _ := action.Arguments["start_nodes"].(string)
				_, _ = executor.ExecuteTreeSearch(ctx, actionTable, index, startNodes, query, 3, 3)

			case ai.ToolNameGraphSearch:
				startNode, _ := action.Arguments["start_node"].(string)
				index, _ := action.Arguments["index"].(string)
				edgeType, _ := action.Arguments["edge_type"].(string)
				direction, _ := action.Arguments["direction"].(string)
				_, _ = executor.ExecuteGraphSearch(ctx, actionTable, index, startNode, edgeType, direction, 1)

			case ai.ToolNameFilter:
				field, _ := action.Arguments["field"].(string)
				operator, _ := action.Arguments["operator"].(string)
				value := action.Arguments["value"]
				executor.AddFilter(ctx, ai.FilterSpec{
					Field:    field,
					Operator: ai.FilterSpecOperator(operator),
					Value:    value,
				})

			case ai.ToolNameClarification:
				question, _ := action.Arguments["question"].(string)
				var options []string
				if opts, ok := action.Arguments["options"].([]any); ok {
					for _, o := range opts {
						if s, ok := o.(string); ok {
							options = append(options, s)
						}
					}
				}
				executor.SetClarification(ctx, ai.ClarificationRequest{
					Question: question,
					Options:  &options,
				})
			}
		}

		// If clarification was requested, stop
		if executor.pendingClarification != nil {
			hitMaxIterations = false
			break
		}
	}

	// Collect results
	collectExecutorResults(executor, result)
	applyPendingClarification(executor, result)
	if executor.pendingClarification == nil && hitMaxIterations {
		result.Status = RetrievalAgentStatusIncomplete
		result.IncompleteDetails = IncompleteDetails{Reason: IncompleteDetailsReasonMaxIterations}
	}
}

// getTableIndexes introspects a table to get its available indexes
func (t *TableApi) getTableIndexes(tableName string) []ai.IndexInfo {
	table, err := t.tm.GetTable(tableName)
	if err != nil {
		t.logger.Warn("Failed to get table for index introspection", zap.Error(err), zap.String("table", tableName))
		return nil
	}

	var indexInfos []ai.IndexInfo
	for name, cfg := range table.Indexes {
		info := ai.IndexInfo{
			Table: tableName,
			Name:  name,
			Type:  string(indexes.NormalizeIndexType(cfg.Type)),
		}
		if cfg.Description != nil {
			info.Description = *cfg.Description
		}
		indexInfos = append(indexInfos, info)
	}

	return indexInfos
}

// =========================================
// Tree Search Internals (preserved from DFA)
// =========================================

// TreeSearchNode represents a node being considered in tree search
type TreeSearchNode struct {
	Key      string
	Summary  string
	Score    float64
	Depth    int
	IsLeaf   bool
	Document *QueryHit
}

// TreeSearchBranchSelection represents LLM's selection of which branches to follow
type TreeSearchBranchSelection struct {
	Selected []string `json:"selected"` // Keys to follow
	Reason   string   `json:"reason"`
}

// TreeSearchSufficiencyResult represents LLM's sufficiency assessment
type TreeSearchSufficiencyResult struct {
	Sufficient bool   `json:"sufficient"`
	Reason     string `json:"reason"`
}

// executeTreeSearchInternal is the internal implementation of tree search
func (e *retrievalToolExecutor) executeTreeSearchInternal(
	ctx context.Context,
	treeConfig *TreeSearchConfig,
	query string,
	queryResults map[string][]QueryHit,
) []QueryHit {
	e.logger.Debug("Starting tree search",
		zap.String("index", treeConfig.Index),
		zap.String("start_nodes", treeConfig.StartNodes),
		zap.Int("max_depth", treeConfig.MaxDepth),
		zap.Int("beam_width", treeConfig.BeamWidth),
	)

	// Set defaults
	maxDepth := treeConfig.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 5
	}
	beamWidth := treeConfig.BeamWidth
	if beamWidth <= 0 {
		beamWidth = 3
	}

	// Stream tree search progress
	if e.streamCallback != nil {
		_ = e.streamCallback(ctx, SSEEventStepProgress, map[string]any{
			"step":       "tree_search",
			"index":      treeConfig.Index,
			"max_depth":  maxDepth,
			"beam_width": beamWidth,
		})
	}

	// Step 1: Resolve start nodes
	startNodes, err := e.resolveTreeStartNodesWithPipeline(ctx, treeConfig, queryResults)
	if err != nil {
		e.logger.Error("Failed to resolve start nodes", zap.Error(err))
		e.reasoningChain = append(e.reasoningChain, RetrievalReasoningStep{
			Step:    "tree_search",
			Action:  "Failed to resolve start nodes, falling back to semantic",
			Details: map[string]any{"error": err.Error()},
		})
		return e.fallbackToSemanticSearch(ctx, query)
	}

	if len(startNodes) == 0 {
		e.logger.Warn("No start nodes found for tree search")
		e.reasoningChain = append(e.reasoningChain, RetrievalReasoningStep{
			Step:   "tree_search",
			Action: "No start nodes found, falling back to semantic",
		})
		return e.fallbackToSemanticSearch(ctx, query)
	}

	e.logger.Debug("Resolved start nodes", zap.Int("count", len(startNodes)))

	// Step 2: Navigate tree using beam search
	collected := []QueryHit{}
	stack := [][]string{startNodes}
	backtrackCandidates := make(map[int][]string)
	visited := make(map[string]bool)
	depth := 0

	for len(stack) > 0 && depth < maxDepth {
		currentNodes := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if e.streamCallback != nil {
			_ = e.streamCallback(ctx, SSEEventStepProgress, map[string]any{
				"step":      "tree_search",
				"depth":     depth,
				"num_nodes": len(currentNodes),
			})
		}

		nodesWithSummaries, err := e.getNodeSummaries(ctx, treeConfig.Index, currentNodes)
		if err != nil {
			e.logger.Warn("Failed to get node summaries", zap.Error(err))
			continue
		}

		// Filter out visited nodes
		var unvisitedNodes []TreeSearchNode
		for _, node := range nodesWithSummaries {
			if !visited[node.Key] {
				visited[node.Key] = true
				unvisitedNodes = append(unvisitedNodes, node)
			}
		}

		if len(unvisitedNodes) == 0 {
			continue
		}

		// Collect leaf nodes as documents
		var nonLeafNodes []TreeSearchNode
		for _, node := range unvisitedNodes {
			if node.IsLeaf && node.Document != nil {
				collected = append(collected, *node.Document)
				if e.streamCallback != nil {
					_ = e.streamCallback(ctx, SSEEventHit, node.Document)
				}
			} else {
				nonLeafNodes = append(nonLeafNodes, node)
			}
		}

		// Check sufficiency
		if len(collected) > 0 {
			sufficient, reason := e.checkTreeSearchSufficiency(query, collected)
			if e.streamCallback != nil {
				_ = e.streamCallback(ctx, SSEEventStepProgress, map[string]any{
					"step":       "tree_search",
					"sufficient": sufficient,
					"reason":     reason,
					"collected":  len(collected),
				})
			}
			if sufficient {
				e.reasoningChain = append(e.reasoningChain, RetrievalReasoningStep{
					Step:   "tree_search",
					Action: "Sufficiency reached",
					Details: map[string]any{
						"depth": depth, "collected": len(collected), "reason": reason,
					},
				})
				break
			}
		}

		// If no non-leaf nodes to explore, try backtracking
		if len(nonLeafNodes) == 0 {
			for d := depth - 1; d >= 0; d-- {
				if candidates, ok := backtrackCandidates[d]; ok && len(candidates) > 0 {
					stack = append(stack, candidates)
					delete(backtrackCandidates, d)
					break
				}
			}
			continue
		}

		// Select which branches to follow
		selected, unselected := e.selectTreeBranches(query, nonLeafNodes, beamWidth)
		if len(unselected) > 0 {
			backtrackCandidates[depth] = unselected
		}

		// Get children for selected branches
		if len(selected) > 0 {
			children, err := e.getTreeChildren(ctx, treeConfig.Index, selected)
			if err != nil {
				e.logger.Warn("Failed to get children", zap.Error(err))
			} else if len(children) > 0 {
				stack = append(stack, children)
			}
		}

		depth++
	}

	// Stream completion
	if e.streamCallback != nil {
		_ = e.streamCallback(ctx, SSEEventStepProgress, map[string]any{
			"step":      "tree_search",
			"collected": len(collected),
			"depth":     depth,
			"complete":  true,
		})
	}

	return collected
}

// resolveTreeStartNodesWithPipeline resolves start nodes with access to pipeline results
func (e *retrievalToolExecutor) resolveTreeStartNodesWithPipeline(
	ctx context.Context,
	treeConfig *TreeSearchConfig,
	queryResults map[string][]QueryHit,
) ([]string, error) {
	startNodesRef := treeConfig.StartNodes

	if startNodesRef == "$roots" {
		return e.tableApi.getGraphRoots(ctx, treeConfig.Index, e.defaultTable)
	}

	// Use collected documents from prior queries as start nodes
	if startNodesRef == "" {
		if len(e.collectedDocuments) > 0 {
			var keys []string
			for _, doc := range e.collectedDocuments {
				keys = append(keys, doc.ID)
			}
			return keys, nil
		}
		return nil, fmt.Errorf("no start_nodes specified and no prior query results available")
	}

	// Explicit comma-separated node IDs
	keys := strings.Split(startNodesRef, ",")
	for i := range keys {
		keys[i] = strings.TrimSpace(keys[i])
	}
	return keys, nil
}

// getGraphRoots gets root nodes from the graph index by querying for documents
// that have no parent_id. Uses a server-side exclusion query to avoid fetching
// non-root documents.
func (t *TableApi) getGraphRoots(
	ctx context.Context,
	indexName string,
	tableName string,
) ([]string, error) {
	// Use an exclusion query to filter out documents where parent_id has a value.
	// This keeps only documents where parent_id is missing or empty.
	queryReq := &QueryRequest{
		Table:          tableName,
		Fields:         []string{"parent_id"},
		Limit:          200,
		ExclusionQuery: json.RawMessage(`{"field": "parent_id", "regexp": ".+"}`),
	}

	result := t.runQuery(ctx, queryReq)
	if result.Status != 200 {
		// Fall back to unfiltered query if exclusion isn't supported
		queryReq = &QueryRequest{
			Table:  tableName,
			Fields: []string{"parent_id"},
			Limit:  200,
		}
		result = t.runQuery(ctx, queryReq)
		if result.Status != 200 {
			return nil, fmt.Errorf("query failed: %s", result.Error)
		}
	}

	var roots []string
	for _, hit := range result.Hits.Hits {
		parentID, hasParent := hit.Source["parent_id"]
		isRoot := !hasParent || parentID == nil || parentID == ""
		if isRoot {
			roots = append(roots, hit.ID)
		}
	}

	if len(roots) == 0 {
		for _, hit := range result.Hits.Hits {
			roots = append(roots, hit.ID)
			if len(roots) >= 10 {
				break
			}
		}
	}

	return roots, nil
}

// getNodeSummaries fetches summaries for nodes from the table using batched
// shard lookups to avoid N+1 RPC calls.
func (e *retrievalToolExecutor) getNodeSummaries(
	ctx context.Context,
	indexName string,
	nodeKeys []string,
) ([]TreeSearchNode, error) {
	// Batch-lookup all keys, grouped by shard
	docs, err := e.tableApi.batchLookupDocuments(ctx, e.defaultTable, nodeKeys)
	if err != nil {
		return nil, err
	}

	nodes := make([]TreeSearchNode, 0, len(docs))
	for _, key := range nodeKeys {
		doc, ok := docs[key]
		if !ok {
			continue
		}

		node := TreeSearchNode{
			Key:   key,
			Depth: 0,
		}

		if summaries, ok := doc["_summaries"].(map[string]any); ok {
			if summary, ok := summaries[indexName].(string); ok {
				node.Summary = summary
			}
		}

		if node.Summary == "" {
			if content, ok := doc["content"].(string); ok {
				if len(content) > 500 {
					node.Summary = content[:500] + "..."
				} else {
					node.Summary = content
				}
			}
		}

		if content, ok := doc["content"].(string); ok && len(content) > 100 {
			node.IsLeaf = true
			hit := QueryHit{
				ID:     key,
				Source: doc,
				Score:  1.0,
			}
			node.Document = &hit
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

// batchLookupDocuments looks up multiple documents in a single batched operation,
// grouping keys by shard to minimize RPC calls.
func (t *TableApi) batchLookupDocuments(
	ctx context.Context,
	tableName string,
	keys []string,
) (map[string]map[string]any, error) {
	table, err := t.tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}

	shardKeys, _ := table.PartitionKeysByShard(keys)

	var mu sync.Mutex
	docs := make(map[string]map[string]any, len(keys))

	g, _ := workerpool.NewGroup(ctx, t.pool)
	for shardID, skeys := range shardKeys {
		g.Go(func(ctx context.Context) error {
			results, err := t.ln.forwardLookupToShard(ctx, shardID, skeys)
			if err != nil {
				t.logger.Debug("Batch lookup failed for shard", zap.Uint64("shard", uint64(shardID)), zap.Error(err))
				return nil // non-fatal
			}
			mu.Lock()
			for key, raw := range results {
				var doc map[string]any
				if err := json.Unmarshal(raw, &doc); err != nil {
					t.logger.Debug("Failed to unmarshal document", zap.String("key", key), zap.Error(err))
					continue
				}
				docs[key] = doc
			}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	return docs, nil
}

// selectTreeBranches selects which branches to follow using heuristics
func (e *retrievalToolExecutor) selectTreeBranches(
	query string,
	nodes []TreeSearchNode,
	beamWidth int,
) (selected []string, unselected []string) {
	if len(nodes) <= beamWidth {
		for _, node := range nodes {
			selected = append(selected, node.Key)
		}
		return selected, nil
	}

	queryTerms := strings.Fields(strings.ToLower(query))
	type scoredNode struct {
		key   string
		score int
	}
	var scoredNodes []scoredNode

	for _, node := range nodes {
		score := 0
		summaryLower := strings.ToLower(node.Summary)
		for _, term := range queryTerms {
			if strings.Contains(summaryLower, term) {
				score++
			}
		}
		scoredNodes = append(scoredNodes, scoredNode{key: node.Key, score: score})
	}

	// Sort by score descending
	slices.SortFunc(scoredNodes, func(a, b scoredNode) int {
		return b.score - a.score
	})

	for i, sn := range scoredNodes {
		if i < beamWidth {
			selected = append(selected, sn.key)
		} else {
			unselected = append(unselected, sn.key)
		}
	}

	return selected, unselected
}

// getTreeChildren gets child nodes for the given parent keys, fetching edges
// in parallel across parents.
func (e *retrievalToolExecutor) getTreeChildren(
	ctx context.Context,
	indexName string,
	parentKeys []string,
) ([]string, error) {
	table, _, err := e.tableApi.tm.GetTableWithShardStatuses(e.defaultTable)
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}

	rg, _ := workerpool.NewResultGroup[[]indexes.Edge](ctx, e.tableApi.pool)
	for _, parentKey := range parentKeys {
		rg.Go(func(ctx context.Context) ([]indexes.Edge, error) {
			edges, err := e.tableApi.ln.getEdgesAcrossShards(
				ctx, table, indexName, parentKey, "", indexes.EdgeDirectionIn,
			)
			if err != nil {
				e.logger.Debug("Failed to get edges for parent",
					zap.String("parent", parentKey), zap.Error(err))
				return nil, nil // non-fatal: skip this parent
			}
			return edges, nil
		})
	}
	edgeResults, _ := rg.Wait()

	seen := make(map[string]bool)
	var allChildren []string
	for _, edges := range edgeResults {
		for _, edge := range edges {
			childKey := string(edge.Source)
			if !seen[childKey] {
				seen[childKey] = true
				allChildren = append(allChildren, childKey)
			}
		}
	}

	return allChildren, nil
}

// checkTreeSearchSufficiency checks if we have collected sufficient documents
func (e *retrievalToolExecutor) checkTreeSearchSufficiency(
	query string, collected []QueryHit,
) (bool, string) {
	if len(collected) >= 5 {
		return true, "Collected sufficient documents (5+)"
	}

	if len(collected) >= 3 {
		queryTerms := strings.Fields(strings.ToLower(query))
		relevantCount := 0

		for _, doc := range collected {
			content := ""
			if c, ok := doc.Source["content"].(string); ok {
				content = strings.ToLower(c)
			}

			matchCount := 0
			for _, term := range queryTerms {
				if strings.Contains(content, term) {
					matchCount++
				}
			}

			if matchCount*2 >= len(queryTerms) {
				relevantCount++
			}
		}

		if relevantCount >= 2 {
			return true, fmt.Sprintf("Collected %d relevant documents", relevantCount)
		}
	}

	return false, fmt.Sprintf("Need more documents (currently have %d)", len(collected))
}

// fallbackToSemanticSearch falls back to semantic search when tree search fails
func (e *retrievalToolExecutor) fallbackToSemanticSearch(
	ctx context.Context, query string,
) []QueryHit {
	queryReq := &QueryRequest{
		Table:          e.defaultTable,
		SemanticSearch: query,
		Limit:          10,
	}

	result := e.tableApi.runQuery(ctx, queryReq)
	if result.Status == 200 {
		e.collectHits(ctx, result.Hits.Hits)
		return result.Hits.Hits
	}

	return []QueryHit{}
}

// convertQueryHitsToDocuments converts QueryHit slice to schema.Document slice
func convertQueryHitsToDocuments(hits []QueryHit) []schema.Document {
	docs := make([]schema.Document, 0, len(hits))
	for _, hit := range hits {
		doc := schema.Document{
			ID:     hit.ID,
			Fields: hit.Source,
		}
		docs = append(docs, doc)
	}
	return docs
}
