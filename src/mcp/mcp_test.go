// Copyright 2025 The Antfly Authors
// Licensed under the Apache License, Version 2.0

package mcp

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHandler implements AntflyHandler for testing. Each method delegates to
// an optional func field; if nil the method returns a zero/nil result.
type mockHandler struct {
	createTableFn func(ctx context.Context, name string, numShards int, schemaJSON string) error
	dropTableFn   func(ctx context.Context, name string) error
	listTablesFn  func(ctx context.Context) ([]TableInfo, error)
	createIndexFn func(ctx context.Context, tableName, indexName, field, template string, dimension int, embedderJSON, summarizerJSON string) error
	dropIndexFn   func(ctx context.Context, tableName, indexName string) error
	listIndexesFn func(ctx context.Context, tableName string) ([]IndexInfo, error)
	queryFn       func(ctx context.Context, req QueryRequest) (*QueryResult, error)
	batchFn       func(ctx context.Context, tableName string, inserts map[string]any, deletes []string) (*BatchResult, error)
	backupFn      func(ctx context.Context, tableName, backupID, location string) error
	restoreFn     func(ctx context.Context, tableName, backupID, location string) error
}

func (m *mockHandler) CreateTable(ctx context.Context, name string, numShards int, schemaJSON string) error {
	if m.createTableFn != nil {
		return m.createTableFn(ctx, name, numShards, schemaJSON)
	}
	return nil
}

func (m *mockHandler) DropTable(ctx context.Context, name string) error {
	if m.dropTableFn != nil {
		return m.dropTableFn(ctx, name)
	}
	return nil
}

func (m *mockHandler) ListTables(ctx context.Context) ([]TableInfo, error) {
	if m.listTablesFn != nil {
		return m.listTablesFn(ctx)
	}
	return nil, nil
}

func (m *mockHandler) CreateIndex(ctx context.Context, tableName, indexName, field, template string, dimension int, embedderJSON, summarizerJSON string) error {
	if m.createIndexFn != nil {
		return m.createIndexFn(ctx, tableName, indexName, field, template, dimension, embedderJSON, summarizerJSON)
	}
	return nil
}

func (m *mockHandler) DropIndex(ctx context.Context, tableName, indexName string) error {
	if m.dropIndexFn != nil {
		return m.dropIndexFn(ctx, tableName, indexName)
	}
	return nil
}

func (m *mockHandler) ListIndexes(ctx context.Context, tableName string) ([]IndexInfo, error) {
	if m.listIndexesFn != nil {
		return m.listIndexesFn(ctx, tableName)
	}
	return nil, nil
}

func (m *mockHandler) Query(ctx context.Context, req QueryRequest) (*QueryResult, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, req)
	}
	return &QueryResult{}, nil
}

func (m *mockHandler) Batch(ctx context.Context, tableName string, inserts map[string]any, deletes []string) (*BatchResult, error) {
	if m.batchFn != nil {
		return m.batchFn(ctx, tableName, inserts, deletes)
	}
	return &BatchResult{}, nil
}

func (m *mockHandler) Backup(ctx context.Context, tableName, backupID, location string) error {
	if m.backupFn != nil {
		return m.backupFn(ctx, tableName, backupID, location)
	}
	return nil
}

func (m *mockHandler) Restore(ctx context.Context, tableName, backupID, location string) error {
	if m.restoreFn != nil {
		return m.restoreFn(ctx, tableName, backupID, location)
	}
	return nil
}

// setupMCPTest creates an MCP client session backed by a mock handler.
func setupMCPTest(t *testing.T, handler AntflyHandler) *mcp.ClientSession {
	t.Helper()

	server := NewMCPServer(handler)
	ctx := context.Background()

	t1, t2 := mcp.NewInMemoryTransports()

	_, err := server.Connect(ctx, t1, nil)
	require.NoError(t, err)

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, nil)
	session, err := client.Connect(ctx, t2, nil)
	require.NoError(t, err)
	t.Cleanup(func() { session.Close() })

	return session
}

func TestListTools(t *testing.T) {
	session := setupMCPTest(t, &mockHandler{})

	result, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)

	got := make([]string, len(result.Tools))
	for i, tool := range result.Tools {
		got[i] = tool.Name
	}
	sort.Strings(got)

	want := []string{
		"backup",
		"batch",
		"create_index",
		"create_table",
		"drop_index",
		"drop_table",
		"list_indexes",
		"list_tables",
		"query",
		"restore",
	}
	assert.Equal(t, want, got)
}

func TestListTables(t *testing.T) {
	handler := &mockHandler{
		listTablesFn: func(ctx context.Context) ([]TableInfo, error) {
			return []TableInfo{
				{Name: "docs"},
				{Name: "users"},
			}, nil
		},
	}
	session := setupMCPTest(t, handler)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "list_tables",
	})
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.NotEmpty(t, result.Content)

	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, tc.Text, "2 tables")
	assert.Contains(t, tc.Text, "docs")
	assert.Contains(t, tc.Text, "users")

	// Verify structured content contains the tables wrapper
	if result.StructuredContent != nil {
		sc, ok := result.StructuredContent.(map[string]any)
		require.True(t, ok)
		tables, ok := sc["tables"]
		require.True(t, ok)
		tableList, ok := tables.([]any)
		require.True(t, ok)
		assert.Len(t, tableList, 2)
	}
}

func TestListTablesError(t *testing.T) {
	handler := &mockHandler{
		listTablesFn: func(ctx context.Context) ([]TableInfo, error) {
			return nil, fmt.Errorf("internal error")
		},
	}
	session := setupMCPTest(t, handler)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "list_tables",
	})
	require.NoError(t, err, "protocol error should not occur; business errors go in Content")
	require.NotEmpty(t, result.Content)

	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, tc.Text, "Failed to list tables")
}

func TestCreateTable(t *testing.T) {
	var gotName string
	var gotShards int
	handler := &mockHandler{
		createTableFn: func(ctx context.Context, name string, numShards int, schemaJSON string) error {
			gotName = name
			gotShards = numShards
			return nil
		},
	}
	session := setupMCPTest(t, handler)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "create_table",
		Arguments: map[string]any{
			"tableName": "my_table",
			"numShards": 3,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, tc.Text, "Table created successfully")
	assert.Equal(t, "my_table", gotName)
	assert.Equal(t, 3, gotShards)
}

func TestDropTable(t *testing.T) {
	var gotName string
	handler := &mockHandler{
		dropTableFn: func(ctx context.Context, name string) error {
			gotName = name
			return nil
		},
	}
	session := setupMCPTest(t, handler)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "drop_table",
		Arguments: map[string]any{
			"tableName": "old_table",
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, tc.Text, "Table dropped successfully")
	assert.Equal(t, "old_table", gotName)
}

func TestQuery(t *testing.T) {
	handler := &mockHandler{
		queryFn: func(ctx context.Context, req QueryRequest) (*QueryResult, error) {
			return &QueryResult{
				HitCount: 2,
				Structured: map[string]any{
					"hits": map[string]any{
						"hits": []any{
							map[string]any{"id": "doc1", "score": 1.5, "fields": map[string]any{"title": "First"}},
							map[string]any{"id": "doc2", "score": 1.2, "fields": map[string]any{"title": "Second"}},
						},
						"total": 2,
					},
				},
			}, nil
		},
	}
	session := setupMCPTest(t, handler)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"tableName":      "docs",
			"fullTextSearch": "test",
			"limit":          10,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, tc.Text, "2 results")

	// Verify structured content is a map (round-tripped query result)
	if result.StructuredContent != nil {
		sc, ok := result.StructuredContent.(map[string]any)
		require.True(t, ok)
		hits, ok := sc["hits"].(map[string]any)
		require.True(t, ok)
		hitList, ok := hits["hits"].([]any)
		require.True(t, ok)
		assert.Len(t, hitList, 2)
	}
}

func TestBatch(t *testing.T) {
	handler := &mockHandler{
		batchFn: func(ctx context.Context, tableName string, inserts map[string]any, deletes []string) (*BatchResult, error) {
			return &BatchResult{
				Inserted: len(inserts),
				Deleted:  len(deletes),
			}, nil
		},
	}
	session := setupMCPTest(t, handler)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "batch",
		Arguments: map[string]any{
			"tableName": "docs",
			"writes": map[string]any{
				"doc1": map[string]any{"title": "First"},
				"doc2": map[string]any{"title": "Second"},
			},
			"deletes": []string{"doc3"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, tc.Text, "2 inserted")
	assert.Contains(t, tc.Text, "1 deleted")
}

func TestNewMCPHandler(t *testing.T) {
	server := NewMCPServer(&mockHandler{})
	handler := NewMCPHandler(server)
	assert.NotNil(t, handler)
}
