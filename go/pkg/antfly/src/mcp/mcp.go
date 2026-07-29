// Copyright 2026 Antfly, Inc.
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

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db/indexes"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// AntflyHandler abstracts Antfly operations for MCP tools.
// Implemented by the adapter in the metadata package.
type AntflyHandler interface {
	CreateTable(ctx context.Context, name string, numShards int, schemaJSON string) error
	DropTable(ctx context.Context, name string) error
	ListTables(ctx context.Context) ([]TableInfo, error)
	CreateIndex(ctx context.Context, tableName, indexName, field, template string, dimension int, embedderJSON, summarizerJSON string) error
	DropIndex(ctx context.Context, tableName, indexName string) error
	ListIndexes(ctx context.Context, tableName string) ([]IndexInfo, error)
	Query(ctx context.Context, req QueryRequest) (*QueryResult, error)
	Batch(ctx context.Context, tableName string, inserts map[string]any, deletes []string) (*BatchResult, error)
	Backup(ctx context.Context, tableName, backupID, connection, location string) error
	Restore(ctx context.Context, tableName, backupID, connection, location string) error
}

// TableInfo describes a table for MCP display and structured output.
type TableInfo struct {
	Name   string         `json:"name"`
	Status map[string]any `json:"status,omitempty"`
}

// IndexInfo describes an index for MCP display and structured output.
type IndexInfo struct {
	Name   string         `json:"name"`
	Status map[string]any `json:"status,omitempty"`
}

// QueryRequest holds query parameters passed from MCP tools.
type QueryRequest struct {
	TableName           string
	RawQueryRequest     map[string]any
	FullTextSearch      any
	FullTextSearchField string
	SemanticSearch      string
	Fields              []string
	Limit               int
	OrderBy             []indexes.SortField
	Indexes             []string
	FilterPrefix        string
}

// QueryResult holds query results returned to MCP tools.
type QueryResult struct {
	HitCount   int            `json:"hit_count"`
	Structured map[string]any `json:"structured,omitempty"`
}

// BatchResult holds batch operation results returned to MCP tools.
type BatchResult struct {
	Inserted int `json:"inserted"`
	Deleted  int `json:"deleted"`
	Failed   any `json:"failed,omitempty"`
}

// CreateTableArgs defines the create table tool parameters.
type CreateTableArgs struct {
	TableName string `json:"tableName"           mcp:"name of the table to create"`
	NumShards int    `json:"numShards,omitempty" mcp:"number of shards (default: 3)"`
	Key       string `json:"key,omitempty"       mcp:"document id/key field name for the schema"`
	Fields    string `json:"fields,omitempty"    mcp:"JSON object defining field types"`
}

// DropTableArgs defines the drop table tool parameters.
type DropTableArgs struct {
	TableName string `json:"tableName" mcp:"name of the table to drop"`
}

// CreateIndexArgs defines the create index tool parameters.
type CreateIndexArgs struct {
	TableName  string `json:"tableName"          mcp:"name of the table"`
	IndexName  string `json:"indexName"          mcp:"name of the index"`
	Field      string `json:"field,omitempty"    mcp:"field to index (mutually exclusive with template)"`
	Template   string `json:"template,omitempty" mcp:"template for generating index values (mutually exclusive with field)"`
	Dimension  int    `json:"dimension"          mcp:"vector dimension"`
	Embedder   string `json:"embedder"           mcp:"JSON embedder configuration"`
	Summarizer string `json:"summarizer"         mcp:"JSON summarizer LLM configuration"`
}

// DropIndexArgs defines the drop index tool parameters.
type DropIndexArgs struct {
	TableName string `json:"tableName" mcp:"name of the table"`
	IndexName string `json:"indexName" mcp:"name of the index to drop"`
}

// QueryArgs defines the query tool parameters.
type QueryArgs struct {
	TableName           string              `json:"tableName"                     mcp:"name of the table to query"`
	QueryRequest        map[string]any      `json:"queryRequest,omitempty"        mcp:"raw Antfly QueryRequest body for POST /tables/{tableName}/query; mutually exclusive with query shorthand arguments"`
	FullTextSearch      any                 `json:"fullTextSearch,omitempty"      mcp:"full text search query string shorthand, or the generic full_text_search query object accepted by the REST API"`
	FullTextSearchRaw   map[string]any      `json:"full_text_search,omitempty"    mcp:"generic REST-shaped full_text_search query object"`
	FullTextSearchField string              `json:"fullTextSearchField,omitempty" mcp:"field to search when fullTextSearch is a string shorthand, for example content"`
	Fields              []string            `json:"fields,omitempty"              mcp:"fields to return"`
	Limit               int                 `json:"limit,omitempty"               mcp:"maximum number of results (default: 10)"`
	OrderBy             []indexes.SortField `json:"orderBy,omitempty"             mcp:"sort fields with direction (desc: true for descending)"`
	SemanticSearch      string              `json:"semanticSearch,omitempty"      mcp:"semantic search query"`
	Indexes             []string            `json:"indexes,omitempty"             mcp:"index names to use for semantic search"`
	FilterPrefix        string              `json:"filterPrefix,omitempty"        mcp:"filter results by document id/key prefix"`
}

// BackupArgs defines the backup tool parameters.
type BackupArgs struct {
	TableName  string `json:"tableName"  mcp:"name of the table to backup"`
	BackupID   string `json:"backupId"   mcp:"unique identifier for the backup"`
	Connection string `json:"connection" mcp:"external_io connection authorized for backup.write"`
	Location   string `json:"location"   mcp:"backup location (e.g., file:///path/to/backup)"`
}

// RestoreArgs defines the restore tool parameters.
type RestoreArgs struct {
	TableName  string `json:"tableName"  mcp:"name of the table to restore into"`
	BackupID   string `json:"backupId"   mcp:"backup identifier to restore from"`
	Connection string `json:"connection" mcp:"external_io connection authorized for restore.read"`
	Location   string `json:"location"   mcp:"backup location"`
}

// BatchArgs defines the batch tool parameters.
type BatchArgs struct {
	TableName string         `json:"tableName" mcp:"name of the table to upload batch to"`
	Writes    map[string]any `json:"writes"    mcp:"array of documents to insert (document id -> fields)"`
	Deletes   []string       `json:"deletes"   mcp:"array of document ids to deletes"`
}

// ListIndexesArgs defines the list indexes tool parameters.
type ListIndexesArgs struct {
	TableName string `json:"tableName" mcp:"name of the table"`
}

// ListTablesResult wraps the list of tables in a struct for MCP output schema compatibility.
type ListTablesResult struct {
	Tables []TableInfo `json:"tables"`
}

// ListIndexesResult wraps the list of indexes in a struct for MCP output schema compatibility.
type ListIndexesResult struct {
	Indexes []IndexInfo `json:"indexes"`
}

// QueryRequestDescription returns compact schema guidance for the raw queryRequest argument.
func QueryRequestDescription() map[string]any {
	return map[string]any{
		"openapi_schema":   "specs/openapi/antfly/metadata.yaml#/components/schemas/QueryRequest",
		"query_ast_schema": "specs/openapi/antfly/query.yaml#/components/schemas/Query",
		"mcp_usage": map[string]any{
			"tool":                   "query",
			"path_table_argument":    "tableName",
			"raw_body_argument":      "queryRequest",
			"raw_mode_exclusive":     true,
			"rejects_query_table":    true,
			"shorthand_arguments":    []string{"fullTextSearch", "fullTextSearchField", "semanticSearch", "fields", "limit", "orderBy", "indexes", "filterPrefix"},
			"forwarded_request_path": "POST /tables/{tableName}/query",
		},
		"top_level_fields": []string{
			"query", "full_text_search", "semantic_search", "embedding_template", "indexes",
			"filter_prefix", "filter_query", "exclusion_query", "aggregations", "embeddings",
			"search_effort", "fields", "limit", "offset", "order_by", "search_after",
			"search_before", "distance_under", "distance_over", "merge_config", "count",
			"profile", "reranker", "analyses", "graph_searches", "expand_strategy",
			"document_renderer", "pruner", "join", "foreign_sources",
		},
		"examples": map[string]any{
			"fielded_full_text": map[string]any{
				"full_text_search": map[string]any{"match": "hello", "field": "body"},
				"fields":           []string{"title", "body"},
				"limit":            5,
			},
			"hybrid": map[string]any{
				"full_text_search": map[string]any{"match": "raft", "field": "body"},
				"semantic_search":  "raft snapshot architecture",
				"indexes":          []string{"body_embedding"},
				"merge_config":     map[string]any{"strategy": "rrf"},
				"fields":           []string{"title", "body"},
				"limit":            20,
				"profile":          true,
			},
		},
	}
}

// MCPCapabilitiesDescription returns compact capability discovery metadata.
func MCPCapabilitiesDescription() map[string]any {
	return map[string]any{
		"protocol":         "mcp",
		"protocol_version": "2025-06-18",
		"transport":        "streamable_http",
		"sessions": map[string]any{
			"initialize_returns_session_id":  true,
			"delete_closes_session":          true,
			"last_event_id_cursor_supported": true,
			"historical_replay":              false,
		},
		"query_builder": "Use the A2A query-builder skill for agentic natural-language query planning. MCP stays focused on deterministic database tools and raw QueryRequest execution.",
		"tools": map[string]any{
			"deterministic":   []string{"list_tables", "list_indexes", "query", "describe_query_request"},
			"write":           []string{"create_table", "drop_table", "create_index", "drop_index", "batch", "backup", "restore"},
			"schema_helpers":  []string{"describe_query_request", "describe_mcp_capabilities"},
			"zig_extra_tools": []string{"describe_table", "describe_indexes", "get_document", "sample_documents"},
		},
		"query": map[string]any{
			"raw_query_request":          true,
			"raw_query_request_argument": "queryRequest",
			"shorthand_arguments":        []string{"fullTextSearch", "fullTextSearchField", "semanticSearch", "fields", "limit", "orderBy", "indexes", "filterPrefix"},
			"raw_mode_exclusive":         true,
		},
	}
}

// AntflyMCPServer wraps an AntflyHandler for MCP operations.
type AntflyMCPServer struct {
	handler AntflyHandler
}

// CreateTable creates a new table in Antfly
func (s *AntflyMCPServer) CreateTable(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args CreateTableArgs,
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult

	numShards := args.NumShards
	if numShards == 0 {
		numShards = 3 // default
	}

	// Validate schema JSON if provided
	if args.Fields != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(args.Fields), &m); err != nil {
			res.Content = []mcp.Content{
				&mcp.TextContent{Text: "Failed to parse schema fields JSON: " + err.Error()},
			}
			return &res, nil, nil
		}
	}

	err := s.handler.CreateTable(ctx, args.TableName, numShards, args.Fields)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to create table: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{Text: "Table created successfully"},
	}
	structuredContent := map[string]any{
		"success": true,
		"table":   args.TableName,
		"shards":  numShards,
	}
	return &res, structuredContent, nil
}

// DropTable drops a table from Antfly
func (s *AntflyMCPServer) DropTable(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args DropTableArgs,
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult

	err := s.handler.DropTable(ctx, args.TableName)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to drop table: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{Text: "Table dropped successfully"},
	}
	structuredContent := map[string]any{
		"success": true,
		"table":   args.TableName,
	}
	return &res, structuredContent, nil
}

// ListTables lists all tables in Antfly
func (s *AntflyMCPServer) ListTables(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args struct{},
) (*mcp.CallToolResult, ListTablesResult, error) {
	var res mcp.CallToolResult

	tables, err := s.handler.ListTables(ctx)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to list tables: " + err.Error()},
		}
		return &res, ListTablesResult{}, nil
	}

	// Format table names for display
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{
			Text: fmt.Sprintf("Found %d tables: %s", len(tables), strings.Join(tableNames, ", ")),
		},
	}
	return &res, ListTablesResult{Tables: tables}, nil
}

// CreateIndex creates an index on a table
func (s *AntflyMCPServer) CreateIndex(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args CreateIndexArgs,
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult

	err := s.handler.CreateIndex(ctx, args.TableName, args.IndexName, args.Field, args.Template, args.Dimension, args.Embedder, args.Summarizer)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to create index: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{Text: "Index created successfully"},
	}
	structuredContent := map[string]any{
		"success": true,
		"table":   args.TableName,
		"index":   args.IndexName,
	}
	return &res, structuredContent, nil
}

// DropIndex drops an index from a table
func (s *AntflyMCPServer) DropIndex(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args DropIndexArgs,
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult

	err := s.handler.DropIndex(ctx, args.TableName, args.IndexName)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to drop index: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{Text: "Index dropped successfully"},
	}
	structuredContent := map[string]any{
		"success": true,
		"table":   args.TableName,
		"index":   args.IndexName,
	}
	return &res, structuredContent, nil
}

// ListIndexes lists all indexes for a table
func (s *AntflyMCPServer) ListIndexes(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args ListIndexesArgs,
) (*mcp.CallToolResult, ListIndexesResult, error) {
	var res mcp.CallToolResult

	indexes, err := s.handler.ListIndexes(ctx, args.TableName)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to list indexes: " + err.Error()},
		}
		return &res, ListIndexesResult{}, nil
	}

	// Format index names for display
	indexNames := make([]string, len(indexes))
	for i, idx := range indexes {
		indexNames[i] = idx.Name
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{
			Text: fmt.Sprintf("Found %d indexes: %s", len(indexes), strings.Join(indexNames, ", ")),
		},
	}
	return &res, ListIndexesResult{Indexes: indexes}, nil
}

// Query performs a query on a table.
// The structured output uses map[string]any to avoid the MCP SDK's JSON
// schema generator hitting the recursive AggregationResult → AggregationBucket
// → SubAggregations cycle.
func (s *AntflyMCPServer) Query(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args QueryArgs,
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult

	if args.QueryRequest != nil {
		if _, ok := args.QueryRequest["table"]; ok {
			res.Content = []mcp.Content{
				&mcp.TextContent{Text: "queryRequest.table is not allowed; pass the table as tableName"},
			}
			return &res, nil, nil
		}
		if queryRequestHasShorthandArgs(args) {
			res.Content = []mcp.Content{
				&mcp.TextContent{Text: "queryRequest cannot be combined with query shorthand arguments"},
			}
			return &res, nil, nil
		}
		qr := QueryRequest{
			TableName:       args.TableName,
			RawQueryRequest: args.QueryRequest,
		}
		return s.runQuery(ctx, qr)
	}

	qr := QueryRequest{
		TableName:           args.TableName,
		FullTextSearch:      args.FullTextSearch,
		FullTextSearchField: args.FullTextSearchField,
		SemanticSearch:      args.SemanticSearch,
		Fields:              args.Fields,
		Limit:               args.Limit,
		OrderBy:             args.OrderBy,
		Indexes:             args.Indexes,
		FilterPrefix:        args.FilterPrefix,
	}
	if qr.Limit == 0 {
		qr.Limit = 10 // default
	}
	if args.FullTextSearchRaw != nil {
		qr.FullTextSearch = args.FullTextSearchRaw
	}

	return s.runQuery(ctx, qr)
}

func queryRequestHasShorthandArgs(args QueryArgs) bool {
	return args.FullTextSearch != nil ||
		args.FullTextSearchRaw != nil ||
		args.FullTextSearchField != "" ||
		args.SemanticSearch != "" ||
		len(args.Fields) != 0 ||
		args.Limit != 0 ||
		len(args.OrderBy) != 0 ||
		len(args.Indexes) != 0 ||
		args.FilterPrefix != ""
}

func (s *AntflyMCPServer) runQuery(ctx context.Context, qr QueryRequest) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult
	result, err := s.handler.Query(ctx, qr)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to execute query: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{
			Text: fmt.Sprintf("Query returned %d results", result.HitCount),
		},
	}
	return &res, result.Structured, nil
}

// DescribeQueryRequest returns compact guidance for query.queryRequest.
func (s *AntflyMCPServer) DescribeQueryRequest(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args struct{},
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult
	res.Content = []mcp.Content{
		&mcp.TextContent{Text: "Antfly QueryRequest schema summary for query.queryRequest"},
	}
	return &res, QueryRequestDescription(), nil
}

// DescribeMCPCapabilities returns compact capability discovery metadata.
func (s *AntflyMCPServer) DescribeMCPCapabilities(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args struct{},
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult
	res.Content = []mcp.Content{
		&mcp.TextContent{Text: "Antfly MCP capabilities"},
	}
	return &res, MCPCapabilitiesDescription(), nil
}

// Backup creates a backup of a table
func (s *AntflyMCPServer) Backup(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args BackupArgs,
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult

	err := s.handler.Backup(ctx, args.TableName, args.BackupID, args.Connection, args.Location)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to create backup: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{Text: fmt.Sprintf("Backup %s initiated successfully", args.BackupID)},
	}
	structuredContent := map[string]any{
		"success":   true,
		"table":     args.TableName,
		"backup_id": args.BackupID,
		"location":  args.Location,
	}
	return &res, structuredContent, nil
}

// Restore restores a table from backup
func (s *AntflyMCPServer) Restore(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args RestoreArgs,
) (*mcp.CallToolResult, map[string]any, error) {
	var res mcp.CallToolResult

	err := s.handler.Restore(ctx, args.TableName, args.BackupID, args.Connection, args.Location)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to restore from backup: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{
			Text: fmt.Sprintf("Restore from backup %s initiated successfully", args.BackupID),
		},
	}
	structuredContent := map[string]any{
		"success":   true,
		"table":     args.TableName,
		"backup_id": args.BackupID,
		"location":  args.Location,
	}
	return &res, structuredContent, nil
}

// Batch loads and/or deletes data in a table
func (s *AntflyMCPServer) Batch(
	ctx context.Context,
	req *mcp.CallToolRequest,
	args BatchArgs,
) (*mcp.CallToolResult, *BatchResult, error) {
	var res mcp.CallToolResult

	if len(args.Writes) == 0 && len(args.Deletes) == 0 {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "No data provided for batch"},
		}
		return &res, nil, nil
	}

	// Validate that write values are objects
	inserts := make(map[string]any, len(args.Writes))
	for k, v := range args.Writes {
		if m, ok := v.(map[string]any); ok {
			inserts[k] = m
		} else {
			res.Content = []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Invalid document format for key %s: expected object", k)},
			}
			return &res, nil, nil
		}
	}

	result, err := s.handler.Batch(ctx, args.TableName, inserts, args.Deletes)
	if err != nil {
		res.Content = []mcp.Content{
			&mcp.TextContent{Text: "Failed to batch data: " + err.Error()},
		}
		return &res, nil, nil
	}

	res.Content = []mcp.Content{
		&mcp.TextContent{
			Text: fmt.Sprintf(
				"Batch upload completed: %d inserted, %d deleted, %v failed",
				result.Inserted,
				result.Deleted,
				result.Failed,
			),
		},
	}
	return &res, result, nil
}

// NewMCPServer creates an MCP server backed by the given handler.
func NewMCPServer(handler AntflyHandler) *mcp.Server {
	s := &AntflyMCPServer{handler: handler}

	server := mcp.NewServer(&mcp.Implementation{Name: "antfly"}, nil)

	// Table operations
	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_table",
		Description: "Create a new table in Antfly",
	}, s.CreateTable)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "drop_table",
		Description: "Drop an existing table from Antfly",
	}, s.DropTable)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_tables",
		Description: "List all tables in Antfly",
	}, s.ListTables)

	// Index operations
	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_index",
		Description: "Create an index on a table",
	}, s.CreateIndex)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "drop_index",
		Description: "Drop an index from a table",
	}, s.DropIndex)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_indexes",
		Description: "List all indexes for a table",
	}, s.ListIndexes)

	// Query operations
	mcp.AddTool(server, &mcp.Tool{
		Name:        "query",
		Description: "Query data from a table with full-text and semantic search",
	}, s.Query)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "describe_query_request",
		Description: "Return compact guidance for the raw Antfly QueryRequest accepted by query.queryRequest",
	}, s.DescribeQueryRequest)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "describe_mcp_capabilities",
		Description: "Describe Antfly MCP capabilities, deterministic tools, and A2A query-builder handoff guidance",
	}, s.DescribeMCPCapabilities)

	// Backup and restore
	mcp.AddTool(server, &mcp.Tool{
		Name:        "backup",
		Description: "Create a backup of a table",
	}, s.Backup)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "restore",
		Description: "Restore a table from a backup",
	}, s.Restore)

	// Batch operations
	mcp.AddTool(server, &mcp.Tool{
		Name:        "batch",
		Description: "Load and delete multiple documents in a table",
	}, s.Batch)

	return server
}

// NewMCPHandler wraps an MCP server in a streamable HTTP handler.
func NewMCPHandler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return server
	}, nil)
}
