/*
Copyright 2026 The Antfly Contributors

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

package sdk

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"github.com/antflydb/antfly/go/pkg/sdk/query"
)

// BatchRequest represents a batch operation request with flexible insert types.
// Unlike the oapi.BatchRequest, this version allows Inserts to accept any type
// (including structs) which will be automatically marshaled.
type BatchRequest struct {
	// Deletes List of keys to delete.
	Deletes []string `json:"deletes,omitempty"`

	// Inserts Map of key to document. Documents can be any type (map, struct, etc.)
	// and will be automatically marshaled to JSON.
	Inserts map[string]any `json:"inserts,omitempty"`

	// Transforms Array of transform operations for in-place document updates using MongoDB-style operators.
	// Transform operations allow you to modify documents without read-modify-write races:
	// - Operations are applied atomically on the server
	// - Multiple operations per document are applied in sequence
	// - Supports $set, $setOnInsert, $unset, $inc, $addToSet, and $max
	Transforms []Transform `json:"transforms,omitempty"`

	// SyncLevel Synchronization level for the batch operation:
	// - "propose": Wait for Raft proposal acceptance (fastest, default)
	// - "write": Wait for Pebble KV write
	// - "query": Wait until affected documents are visible to query paths such as full-text search
	// - "enrichments": Pre-compute enrichments before Raft proposal
	// - "full_index": Wait for all index writes to complete, including vector indexes
	SyncLevel SyncLevel `json:"sync_level,omitempty"`
}

// BatchResult represents the result of a batch operation with detailed failure information
type BatchResult struct {
	// Status is "committed" or "committed_pending". A pending result has a
	// durable commit decision but has not yet reached the requested visibility
	// or participant-recovery barrier.
	Status string `json:"status,omitempty"`

	// Deleted Number of documents successfully deleted
	Deleted int `json:"deleted,omitempty"`

	// Inserted Number of documents successfully inserted
	Inserted int `json:"inserted,omitempty"`

	// Transformed Number of documents successfully transformed
	Transformed int `json:"transformed,omitempty"`

	// Failed List of failed operations with error details
	Failed []struct {
		// Error message for this failure
		Error string `json:"error,omitempty"`

		// Id The document ID that failed
		Id string `json:"id,omitempty"`
	} `json:"failed,omitempty"`
}

// QueryRequest represents a query request with strongly-typed query fields.
// This is the SDK-friendly version of oapi.QueryRequest with Query types instead of json.RawMessage.
type QueryRequest struct {
	// Table name to query
	Table string `json:"table,omitempty"`

	// Analyses specifies analysis operations to perform
	Analyses *oapi.Analyses `json:"analyses,omitempty"`

	// Count whether to return only the count of matching documents.
	// Count-only requests do not support OrderBy, SearchAfter, or SearchBefore.
	Count bool `json:"count,omitempty"`

	// DistanceOver minimum distance for semantic similarity search
	DistanceOver *float32 `json:"distance_over,omitempty"`

	// DistanceUnder maximum distance for semantic similarity search
	DistanceUnder *float32 `json:"distance_under,omitempty"`

	// Embeddings raw embeddings to use for semantic searches (the keys are the indexes to use for the queries).
	// Supports both dense ([]float32 via Embedding0) and sparse ({Indices, Values} via Embedding1) embeddings.
	Embeddings map[string]Embedding `json:"embeddings,omitempty"`

	// ExclusionQuery strongly-typed Bleve search query for exclusions
	ExclusionQuery *query.Query `json:"-"`

	// Aggregations to compute
	Aggregations map[string]AggregationRequest `json:"aggregations,omitempty"`

	// Fields list of fields to include in the results
	Fields []string `json:"fields,omitempty"`

	// FilterPrefix for filtering by key prefix
	FilterPrefix []byte `json:"filter_prefix,omitempty"`

	// FilterQuery strongly-typed Bleve search query for filtering
	FilterQuery *query.Query `json:"-"`

	// FullTextSearch strongly-typed Bleve search query for full-text search
	FullTextSearch *query.Query `json:"-"`

	// Indexes to search (required for semantic search)
	Indexes []string `json:"indexes,omitempty"`

	// Limit maximum number of results to return or topk for semantic_search
	Limit int `json:"limit,omitempty"`

	// MergeConfig for combining results from semantic_search and full_text_search
	MergeConfig MergeConfig `json:"merge_config"`

	// Offset number of results to skip for pagination.
	// Supported for text-backed, match_all, and filter-only requests; not semantic search.
	Offset int `json:"offset,omitempty"`

	// OrderBy specifies exact stored-field sort order.
	// Supported for text-backed, match_all, and filter-only requests; not semantic search or count-only requests.
	OrderBy []oapi.SortField `json:"order_by,omitempty"`

	// SearchAfter cursor for forward pagination. Pass typed _sort values from the last hit.
	// Requires OrderBy and is not supported for semantic search or count-only requests.
	SearchAfter []any `json:"search_after,omitempty"`

	// SearchBefore cursor for backward pagination. Pass typed _sort values from the first hit.
	// Requires OrderBy and is not supported for semantic search or count-only requests.
	SearchBefore []any `json:"search_before,omitempty"`

	// Reranker configuration for reranking results
	Reranker *RerankerConfig `json:"reranker,omitempty"`

	// Pruner configuration for pruning search results based on score quality
	Pruner Pruner `json:"pruner,omitzero"`

	// SemanticSearch text to use for semantic similarity search
	SemanticSearch string `json:"semantic_search,omitempty"`

	// DocumentRenderer optional Go template string for rendering document content to the prompt
	DocumentRenderer string `json:"document_renderer,omitempty"`

	// GraphSearches declarative graph queries to execute after full-text/vector searches.
	// Results can reference search results using node selectors like $full_text_results.
	GraphSearches map[string]GraphQuery `json:"graph_searches,omitempty"`

	// Join configuration for joining data from another table.
	// Supports inner, left, and right joins with automatic strategy selection.
	Join JoinClause `json:"join"`

	// ForeignSources maps table names to foreign data source configurations for
	// query-time federated access. When a table name referenced in a query or join
	// appears in this map, the query is routed to the external data source instead
	// of Antfly storage.
	ForeignSources map[string]ForeignSource `json:"foreign_sources,omitempty"`
}

// MarshalJSON implements custom JSON marshalling for QueryRequest.
// It converts the strongly-typed *query.Query fields to json.RawMessage
// for compatibility with the OAPI layer.
func (q QueryRequest) MarshalJSON() ([]byte, error) {
	// Convert SDK QueryRequest to oapi.QueryRequest
	oapiReq := oapi.QueryRequest{
		Table:            q.Table,
		Analyses:         q.Analyses,
		Count:            q.Count,
		DistanceOver:     q.DistanceOver,
		DistanceUnder:    q.DistanceUnder,
		Embeddings:       q.Embeddings,
		Aggregations:     q.Aggregations,
		Fields:           q.Fields,
		FilterPrefix:     q.FilterPrefix,
		Indexes:          q.Indexes,
		Limit:            q.Limit,
		MergeConfig:      q.MergeConfig,
		Offset:           q.Offset,
		OrderBy:          q.OrderBy,
		SearchAfter:      q.SearchAfter,
		SearchBefore:     q.SearchBefore,
		Reranker:         q.Reranker,
		Pruner:           q.Pruner,
		SemanticSearch:   q.SemanticSearch,
		DocumentRenderer: q.DocumentRenderer,
		GraphSearches:    q.GraphSearches,
		ForeignSources:   q.ForeignSources,
	}
	if !reflect.ValueOf(q.Join).IsZero() {
		oapiReq.Join = q.Join
	}

	// Marshal query fields to json.RawMessage
	var err error
	if q.FilterQuery != nil {
		oapiReq.FilterQuery, err = json.Marshal(q.FilterQuery)
		if err != nil {
			return nil, fmt.Errorf("marshalling filter_query: %w", err)
		}
	}
	if q.FullTextSearch != nil {
		oapiReq.FullTextSearch, err = json.Marshal(q.FullTextSearch)
		if err != nil {
			return nil, fmt.Errorf("marshalling full_text_search: %w", err)
		}
	}
	if q.ExclusionQuery != nil {
		oapiReq.ExclusionQuery, err = json.Marshal(q.ExclusionQuery)
		if err != nil {
			return nil, fmt.Errorf("marshalling exclusion_query: %w", err)
		}
	}

	data, err := json.Marshal(oapiReq)
	if err != nil {
		return nil, err
	}
	if reflect.ValueOf(q.Join).IsZero() {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return nil, err
		}
		delete(fields, "join")
		return json.Marshal(fields)
	}
	return data, nil
}

// UnmarshalJSON implements custom JSON unmarshalling for QueryRequest.
// It converts json.RawMessage fields back to strongly-typed *query.Query.
func (q *QueryRequest) UnmarshalJSON(data []byte) error {
	// Unmarshal into oapi.QueryRequest
	var oapiReq oapi.QueryRequest
	if err := json.Unmarshal(data, &oapiReq); err != nil {
		return err
	}

	// Copy simple fields
	q.Table = oapiReq.Table
	q.Analyses = oapiReq.Analyses
	q.Count = oapiReq.Count
	q.DistanceOver = oapiReq.DistanceOver
	q.DistanceUnder = oapiReq.DistanceUnder
	q.Embeddings = oapiReq.Embeddings
	q.Aggregations = oapiReq.Aggregations
	q.Fields = oapiReq.Fields
	q.FilterPrefix = oapiReq.FilterPrefix
	q.Indexes = oapiReq.Indexes
	q.Limit = oapiReq.Limit
	q.MergeConfig = oapiReq.MergeConfig
	q.Offset = oapiReq.Offset
	q.OrderBy = oapiReq.OrderBy
	q.SearchAfter = oapiReq.SearchAfter
	q.SearchBefore = oapiReq.SearchBefore
	q.Reranker = oapiReq.Reranker
	q.Pruner = oapiReq.Pruner
	q.SemanticSearch = oapiReq.SemanticSearch
	q.DocumentRenderer = oapiReq.DocumentRenderer
	q.GraphSearches = oapiReq.GraphSearches
	q.Join = oapiReq.Join
	q.ForeignSources = oapiReq.ForeignSources

	// Unmarshal query fields (only if not null and not empty)
	if len(oapiReq.FilterQuery) > 0 && !bytes.Equal(oapiReq.FilterQuery, []byte("null")) {
		q.FilterQuery = new(query.Query)
		if err := json.Unmarshal(oapiReq.FilterQuery, q.FilterQuery); err != nil {
			return fmt.Errorf("unmarshalling filter_query: %w", err)
		}
	}
	if len(oapiReq.FullTextSearch) > 0 && !bytes.Equal(oapiReq.FullTextSearch, []byte("null")) {
		q.FullTextSearch = new(query.Query)
		if err := json.Unmarshal(oapiReq.FullTextSearch, q.FullTextSearch); err != nil {
			return fmt.Errorf("unmarshalling full_text_search: %w", err)
		}
	}
	if len(oapiReq.ExclusionQuery) > 0 && !bytes.Equal(oapiReq.ExclusionQuery, []byte("null")) {
		q.ExclusionQuery = new(query.Query)
		if err := json.Unmarshal(oapiReq.ExclusionQuery, q.ExclusionQuery); err != nil {
			return fmt.Errorf("unmarshalling exclusion_query: %w", err)
		}
	}

	return nil
}

// MultiBatchRequest represents a cross-table batch operation.
type MultiBatchRequest struct {
	// Tables maps table names to their batch operations.
	Tables map[string]BatchRequest `json:"tables"`

	// SyncLevel Synchronization level for the batch operation.
	SyncLevel SyncLevel `json:"sync_level,omitempty"`
}

// MultiBatchResult represents the result of a cross-table batch operation.
type MultiBatchResult struct {
	// Status is "committed", "committed_visibility_pending",
	// "committed_recovery_pending", or "aborted". "committed_pending" is
	// used as a generic fallback when an older server returns HTTP 202 without
	// a status field.
	Status string `json:"status,omitempty"`

	// Conflict describes why the atomic batch was aborted. It is present only
	// when Status is "aborted".
	Conflict *TransactionConflict `json:"conflict,omitempty"`

	// Tables maps table names to their batch results.
	Tables map[string]BatchResult `json:"tables,omitempty"`
}

// TransactionCommitResult represents the result of an OCC transaction commit.
type TransactionCommitResult struct {
	// Status is "committed", "committed_visibility_pending",
	// "committed_recovery_pending", or "aborted".
	Status string `json:"status"`

	// Conflict details (only present when status is "aborted").
	Conflict *TransactionConflict `json:"conflict,omitempty"`

	// Tables maps table names to their batch results (only present when committed).
	Tables map[string]BatchResult `json:"tables,omitempty"`
}

// TransactionConflictKind is the stable machine-readable transaction conflict classification.
type TransactionConflictKind string

const (
	TransactionConflictVersionConflict        TransactionConflictKind = "version_conflict"
	TransactionConflictIntentConflict         TransactionConflictKind = "intent_conflict"
	TransactionConflictTopologyChanged        TransactionConflictKind = "topology_changed"
	TransactionConflictParticipantUnavailable TransactionConflictKind = "participant_unavailable"
	TransactionConflictDocIdentityUnavailable TransactionConflictKind = "doc_identity_unavailable"
	TransactionConflictSessionLeaseLost       TransactionConflictKind = "session_lease_lost"
	TransactionConflictTransactionConflict    TransactionConflictKind = "transaction_conflict"
	TransactionConflictTornState              TransactionConflictKind = "torn_state"
)

// TransactionConflictPhase identifies the 2PC participant phase that reported a conflict.
type TransactionConflictPhase string

const (
	TransactionConflictPhaseBegin   TransactionConflictPhase = "begin"
	TransactionConflictPhasePrepare TransactionConflictPhase = "prepare"
	TransactionConflictPhaseResolve TransactionConflictPhase = "resolve"
)

// TransactionConflictRetryScope identifies the component that should be refreshed before retrying.
type TransactionConflictRetryScope string

const (
	TransactionConflictRetryScopeTopology    TransactionConflictRetryScope = "topology"
	TransactionConflictRetryScopeParticipant TransactionConflictRetryScope = "participant"
	TransactionConflictRetryScopeDocIdentity TransactionConflictRetryScope = "doc_identity"
	TransactionConflictRetryScopeSession     TransactionConflictRetryScope = "session"
)

// TransactionConflictParticipant identifies where a distributed transaction conflict occurred.
type TransactionConflictParticipant struct {
	GroupID *uint64                  `json:"group_id,omitempty"`
	Phase   TransactionConflictPhase `json:"phase,omitempty"`
}

// TransactionConflict describes the conflict that caused a transaction abort and whether it is safe to retry.
type TransactionConflict struct {
	Table           string                          `json:"table,omitempty"`
	Key             string                          `json:"key,omitempty"`
	Message         string                          `json:"message,omitempty"`
	Kind            TransactionConflictKind         `json:"kind,omitempty"`
	Retryable       bool                            `json:"retryable"`
	RetryAfterMS    *uint32                         `json:"retry_after_ms,omitempty"`
	RetryScope      TransactionConflictRetryScope   `json:"retry_scope,omitempty"`
	ExpectedVersion *uint64                         `json:"expected_version,omitempty"`
	CurrentVersion  *uint64                         `json:"current_version,omitempty"`
	Participant     *TransactionConflictParticipant `json:"participant,omitempty"`
}
