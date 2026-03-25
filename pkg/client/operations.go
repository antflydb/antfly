/*
Copyright 2025 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strconv"
	"strings"

	"github.com/antflydb/antfly/pkg/client/oapi"
	"github.com/antflydb/antfly/pkg/libaf/json"
)

// readSSEEvents reads SSE events from a reader and yields (eventType, data) pairs.
// Events are parsed from "event: <type>" and "data: <content>" lines.
func readSSEEvents(r io.Reader) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		buf := make([]byte, 4096)
		var partial string // buffer for incomplete lines across reads
		var currentEvent string
		for {
			n, err := r.Read(buf)
			if n > 0 {
				chunk := partial + string(buf[:n])
				lines := strings.Split(chunk, "\n")
				// Last element may be incomplete; save for next read
				partial = lines[len(lines)-1]
				for _, line := range lines[:len(lines)-1] {
					if after, ok := strings.CutPrefix(line, "event: "); ok {
						currentEvent = strings.TrimSpace(after)
					} else if after, ok := strings.CutPrefix(line, "data: "); ok {
						if !yield(currentEvent, after) {
							return
						}
					}
				}
			}
			if err != nil {
				return
			}
		}
	}
}

// Query executes queries against a table
func (c *AntflyClient) Query(ctx context.Context, opts ...QueryRequest) (*QueryResponses, error) {
	request := bytes.NewBuffer(nil)
	e := json.NewEncoder(request)
	for _, opt := range opts {
		// Validate options
		if len(opt.Indexes) > 0 && opt.SemanticSearch == "" {
			return nil, errors.New("semantic_search required when indexes are specified")
		}
		if len(opt.Indexes) > 0 && opt.Offset > 0 {
			return nil, errors.New("offset not available when indexes are specified")
		}

		// MarshalJSON now handles the conversion to oapi.QueryRequest automatically
		if err := e.Encode(opt); err != nil {
			return nil, fmt.Errorf("marshalling query: %w", err)
		}
	}

	resp, err := c.client.GlobalQueryWithBody(ctx, "application/json", request)
	if err != nil {
		return nil, fmt.Errorf("sending query request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query failed: %w", readErrorResponse(resp))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var result QueryResponses
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parsing result: %w", err)
	}

	return &result, nil
}

// Batch performs a batch operation on a table
func (c *AntflyClient) Batch(ctx context.Context, tableName string, request BatchRequest) (*BatchResult, error) {
	batchBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshalling batch request: %w", err)
	}

	resp, err := c.client.BatchWriteWithBody(ctx, tableName, "application/json", bytes.NewBuffer(batchBody))
	if err != nil {
		return nil, fmt.Errorf("batch operation failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("batch failed: %w", readErrorResponse(resp))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var result BatchResult
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			// If unmarshaling fails, return a basic result
			result = BatchResult{
				Inserted: len(request.Inserts),
				Deleted:  len(request.Deletes),
			}
		}
	} else {
		// No response body, return counts from request
		result = BatchResult{
			Inserted: len(request.Inserts),
			Deleted:  len(request.Deletes),
		}
	}

	return &result, nil
}

// LinearMerge performs a stateless linear merge of sorted records from an external source.
// Records are upserted, and any Antfly records in the key range that are absent from the
// input are deleted. Supports progressive pagination for large datasets.
//
// WARNING: Not safe for concurrent merge operations with overlapping ranges.
// Designed as a sync/import API for single-client use.
func (c *AntflyClient) LinearMerge(ctx context.Context, tableName string, request LinearMergeRequest) (*LinearMergeResult, error) {
	resp, err := c.client.LinearMerge(ctx, tableName, request)
	if err != nil {
		return nil, fmt.Errorf("linear merge operation failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("linear merge failed: %w", readErrorResponse(resp))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var result LinearMergeResult
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, fmt.Errorf("parsing linear merge result: %w", err)
		}
	}

	return &result, nil
}

// LookupKey looks up a document by its key.
// Use LookupKeyWithFields if you need to specify which fields to return.
func (c *AntflyClient) LookupKey(ctx context.Context, tableName, key string) (map[string]any, error) {
	return c.LookupKeyWithFields(ctx, tableName, key, "")
}

// LookupKeyWithFields looks up a document by its key with optional field projection.
// The fields parameter is a comma-separated list of fields to include in the response.
// If empty, returns the full document. Supports:
// - Simple fields: "title,author"
// - Nested paths: "user.address.city"
// - Wildcards: "_chunks.*"
// - Exclusions: "-_chunks.*._embedding"
// - Special fields: "_embeddings,_summaries,_chunks"
func (c *AntflyClient) LookupKeyWithFields(ctx context.Context, tableName, key, fields string) (map[string]any, error) {
	var params *oapi.LookupKeyParams
	if fields != "" {
		params = &oapi.LookupKeyParams{Fields: fields}
	}
	resp, err := c.client.LookupKey(ctx, tableName, key, params)
	if err != nil {
		return nil, fmt.Errorf("looking up key: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("looking up key: %w", readErrorResponse(resp))
	}

	// Parse the response
	var document map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return document, nil
}

// ScanKeys scans keys in a table within an optional key range.
// Returns keys and optionally document data based on the request parameters.
func (c *AntflyClient) ScanKeys(ctx context.Context, tableName string, request ScanKeysRequest) ([]map[string]any, error) {
	resp, err := c.client.ScanKeys(ctx, tableName, oapi.ScanKeysRequest(request))
	if err != nil {
		return nil, fmt.Errorf("scanning keys: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("scanning keys: %w", readErrorResponse(resp))
	}

	// Parse the response as array of documents
	var documents []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&documents); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return documents, nil
}

// RetrievalAgentOptions configures streaming callbacks for the retrieval agent.
// Callbacks are invoked as SSE events arrive during a streaming request.
type RetrievalAgentOptions struct {
	OnStepStarted    func(step *SSEStepStarted) error
	OnStepProgress   func(data map[string]any) error
	OnStepCompleted  func(step *AgentStep) error
	OnClassification func(classification *ClassificationTransformationResult) error
	OnReasoning      func(chunk string) error
	OnGeneration     func(chunk string) error
	OnFollowup       func(question string) error
	OnHit            func(hit *Hit) error
	OnToolMode       func(mode string, toolsCount int) error
	OnEval           func(data map[string]any) error
	OnError          func(err *RetrievalAgentError) error
}

// QueryBuilder generates a structured Bleve query from a natural language intent.
func (c *AntflyClient) QueryBuilder(ctx context.Context, req QueryBuilderRequest) (*QueryBuilderResult, error) {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshalling query builder request: %w", err)
	}

	resp, err := c.client.QueryBuilderAgentWithBody(ctx, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("sending query builder request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query builder request failed: %w", readErrorResponse(resp))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var result QueryBuilderResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parsing query builder result: %w", err)
	}
	return &result, nil
}

// RetrievalAgentError represents an error from the retrieval agent
type RetrievalAgentError struct {
	Error string `json:"error"`
}

// RetrievalAgent performs agentic document retrieval with strategy selection and query refinement.
// Supports streaming responses with callbacks for step lifecycle, hits, and generation progress.
func (c *AntflyClient) RetrievalAgent(ctx context.Context, req RetrievalAgentRequest, opts ...RetrievalAgentOptions) (*RetrievalAgentResult, error) {
	// Merge options
	var opt RetrievalAgentOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	// Marshal request
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshalling retrieval agent request: %w", err)
	}

	// Set Accept header based on streaming mode
	acceptHeader := func(_ context.Context, httpReq *http.Request) error {
		if req.Stream {
			httpReq.Header.Set("Accept", "text/event-stream")
		} else {
			httpReq.Header.Set("Accept", "application/json")
		}
		return nil
	}

	resp, err := c.client.RetrievalAgentWithBody(ctx, "application/json", bytes.NewBuffer(reqBody), acceptHeader)
	if err != nil {
		return nil, fmt.Errorf("sending retrieval agent request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("retrieval agent request failed: %w", readErrorResponse(resp))
	}

	// If streaming is disabled, read JSON response directly
	if !req.Stream {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}
		var result RetrievalAgentResult
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, fmt.Errorf("parsing retrieval agent result: %w", err)
		}
		return &result, nil
	}

	// Build result from streaming events
	result := &RetrievalAgentResult{}

	for eventType, data := range readSSEEvents(resp.Body) {
		switch oapi.SSEEvent(eventType) {
		case oapi.SSEEventStepStarted:
			if opt.OnStepStarted != nil {
				var d SSEStepStarted
				if json.UnmarshalString(data, &d) == nil {
					if err := opt.OnStepStarted(&d); err != nil {
						return nil, fmt.Errorf("step_started callback: %w", err)
					}
				}
			}
		case oapi.SSEEventStepProgress:
			if opt.OnStepProgress != nil {
				var d map[string]any
				if json.UnmarshalString(data, &d) == nil {
					if err := opt.OnStepProgress(d); err != nil {
						return nil, fmt.Errorf("step_progress callback: %w", err)
					}
				}
			}
		case oapi.SSEEventStepCompleted:
			if opt.OnStepCompleted != nil {
				var step AgentStep
				if json.UnmarshalString(data, &step) == nil {
					if err := opt.OnStepCompleted(&step); err != nil {
						return nil, fmt.Errorf("step_completed callback: %w", err)
					}
				}
			}
		case oapi.SSEEventClassification:
			if opt.OnClassification != nil {
				var d ClassificationTransformationResult
				if json.UnmarshalString(data, &d) == nil {
					if err := opt.OnClassification(&d); err != nil {
						return nil, fmt.Errorf("classification callback: %w", err)
					}
				}
			}
		case oapi.SSEEventReasoning:
			if opt.OnReasoning != nil {
				var chunk string
				if json.UnmarshalString(data, &chunk) == nil {
					if err := opt.OnReasoning(chunk); err != nil {
						return nil, fmt.Errorf("reasoning callback: %w", err)
					}
				}
			}
		case oapi.SSEEventGeneration:
			if opt.OnGeneration != nil {
				var chunk string
				if json.UnmarshalString(data, &chunk) == nil {
					if err := opt.OnGeneration(chunk); err != nil {
						return nil, fmt.Errorf("generation callback: %w", err)
					}
				}
			}
		case oapi.SSEEventFollowup:
			if opt.OnFollowup != nil {
				var question string
				if json.UnmarshalString(data, &question) == nil {
					if err := opt.OnFollowup(question); err != nil {
						return nil, fmt.Errorf("followup callback: %w", err)
					}
				}
			}
		case oapi.SSEEventHit:
			if opt.OnHit != nil {
				var hitData Hit
				if json.UnmarshalString(data, &hitData) == nil {
					if err := opt.OnHit(&hitData); err != nil {
						return nil, fmt.Errorf("hit callback: %w", err)
					}
				}
			}
		case oapi.SSEEventToolMode:
			if opt.OnToolMode != nil {
				var d struct {
					Mode       string `json:"mode"`
					ToolsCount int    `json:"tools_count"`
				}
				if json.UnmarshalString(data, &d) == nil {
					if err := opt.OnToolMode(d.Mode, d.ToolsCount); err != nil {
						return nil, fmt.Errorf("tool_mode callback: %w", err)
					}
				}
			}
		case oapi.SSEEventEval:
			if opt.OnEval != nil {
				var d map[string]any
				if json.UnmarshalString(data, &d) == nil {
					if err := opt.OnEval(d); err != nil {
						return nil, fmt.Errorf("eval callback: %w", err)
					}
				}
			}
		case oapi.SSEEventDone:
			_ = json.UnmarshalString(data, result)
		case oapi.SSEEventError:
			var agentErr RetrievalAgentError
			if json.UnmarshalString(data, &agentErr) != nil {
				agentErr = RetrievalAgentError{Error: data}
			}
			if opt.OnError != nil {
				if callbackErr := opt.OnError(&agentErr); callbackErr != nil {
					return nil, callbackErr
				}
			}
			return nil, fmt.Errorf("retrieval agent: %s", agentErr.Error)
		}
	}

	return result, nil
}

// MultiBatch performs a cross-table batch operation atomically.
func (c *AntflyClient) MultiBatch(ctx context.Context, request MultiBatchRequest) (*MultiBatchResult, error) {
	batchBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshalling multi-batch request: %w", err)
	}

	resp, err := c.client.MultiBatchWriteWithBody(ctx, "application/json", bytes.NewBuffer(batchBody))
	if err != nil {
		return nil, fmt.Errorf("multi-batch operation failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("multi-batch failed: %w", readErrorResponse(resp))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var result MultiBatchResult
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, fmt.Errorf("parsing multi-batch result: %w", err)
		}
	}

	return &result, nil
}

// LookupKeyWithVersion looks up a document by key and returns its version token.
// The version can be used with Transaction.Read for OCC transactions.
func (c *AntflyClient) LookupKeyWithVersion(ctx context.Context, tableName, key string) (map[string]any, uint64, error) {
	resp, err := c.client.LookupKey(ctx, tableName, key, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("looking up key: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, 0, fmt.Errorf("looking up key: %w", readErrorResponse(resp))
	}

	var version uint64
	if v := resp.Header.Get("X-Antfly-Version"); v != "" {
		version, _ = strconv.ParseUint(v, 10, 64)
	}

	var document map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		return nil, 0, fmt.Errorf("parsing response: %w", err)
	}

	return document, version, nil
}

// Transaction represents a stateless OCC transaction.
// Use NewTransaction to create one, Read to capture versions, and Commit to execute.
type Transaction struct {
	client  *AntflyClient
	readSet []oapi.TransactionReadItem
}

// NewTransaction creates a new OCC transaction builder.
func (c *AntflyClient) NewTransaction() *Transaction {
	return &Transaction{client: c}
}

// Read reads a document and captures its version for conflict detection at commit time.
func (tx *Transaction) Read(ctx context.Context, table, key string) (map[string]any, error) {
	doc, version, err := tx.client.LookupKeyWithVersion(ctx, table, key)
	if err != nil {
		return nil, err
	}

	tx.readSet = append(tx.readSet, oapi.TransactionReadItem{
		Table:   table,
		Key:     key,
		Version: strconv.FormatUint(version, 10),
	})

	return doc, nil
}

// Commit submits the transaction's read set and writes to the server for atomic commit.
// Returns a TransactionCommitResult with status "committed" or "aborted".
// An error is returned only for transport/server failures, not for version conflicts.
func (tx *Transaction) Commit(ctx context.Context, writes map[string]BatchRequest) (*TransactionCommitResult, error) {
	// Convert SDK BatchRequest to oapi types
	oapiTables := make(map[string]oapi.BatchRequest, len(writes))
	for tableName, br := range writes {
		// Convert map[string]any to map[string]map[string]interface{} for oapi compat
		var oapiInserts map[string]map[string]any
		if len(br.Inserts) > 0 {
			oapiInserts = make(map[string]map[string]any, len(br.Inserts))
			for k, v := range br.Inserts {
				switch doc := v.(type) {
				case map[string]any:
					oapiInserts[k] = doc
				default:
					// Marshal and re-unmarshal for struct types
					b, err := json.Marshal(v)
					if err != nil {
						return nil, fmt.Errorf("marshalling insert for key %s: %w", k, err)
					}
					var m map[string]any
					if err := json.Unmarshal(b, &m); err != nil {
						return nil, fmt.Errorf("converting insert for key %s: %w", k, err)
					}
					oapiInserts[k] = m
				}
			}
		}
		oapiTables[tableName] = oapi.BatchRequest{
			Inserts:    oapiInserts,
			Deletes:    br.Deletes,
			Transforms: br.Transforms,
			SyncLevel:  br.SyncLevel,
		}
	}

	reqBody := oapi.TransactionCommitRequest{
		ReadSet: tx.readSet,
		Tables:  oapiTables,
	}

	resp, err := tx.client.client.CommitTransaction(ctx, reqBody)
	if err != nil {
		return nil, fmt.Errorf("commit transaction failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	// Both 200 (committed) and 409 (conflict/aborted) return TransactionCommitResponse
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict {
		var result TransactionCommitResult
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, fmt.Errorf("parsing commit result: %w", err)
		}
		return &result, nil
	}

	return nil, fmt.Errorf("commit transaction failed (%d): %s", resp.StatusCode, string(respBody))
}
