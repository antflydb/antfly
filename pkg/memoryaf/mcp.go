package memoryaf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// UserContextFunc extracts a UserContext from the request context.
// Callers provide an implementation matching their auth model
// (e.g. JWT middleware, a fixed local user, etc.).
type UserContextFunc func(ctx context.Context) (UserContext, error)

// MCPHandler wraps a Handler in an MCP streamable HTTP handler.
type MCPHandler struct {
	handler       *Handler
	userContextFn UserContextFunc
	mcpServer     *mcp.Server
	httpHandler   http.Handler
}

// NewMCPHandler creates a new MCP handler backed by a memory Handler.
// userContextFn is called on every tool invocation to extract identity.
func NewMCPHandler(handler *Handler, userContextFn UserContextFunc) *MCPHandler {
	m := &MCPHandler{
		handler:       handler,
		userContextFn: userContextFn,
	}
	m.mcpServer = m.buildMCPServer()
	m.httpHandler = mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return m.mcpServer
	}, nil)
	return m
}

// ServeHTTP implements http.Handler.
func (m *MCPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.httpHandler.ServeHTTP(w, r)
}

func (m *MCPHandler) buildMCPServer() *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "memoryaf"}, nil)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "store_memory",
		Description: "Store a memory for later retrieval. Supports episodic, semantic, and procedural memories. Entities are auto-extracted for graph linking. Set ephemeral=true for session-scoped memories, and use source_backend/source_id/source_path/source_url/section_path to point at external docs.",
	}, m.storeMemory)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "search_memories",
		Description: "Search memories using hybrid semantic + full-text search with optional graph expansion. Use scope shortcuts ('session', 'agent', 'device') to narrow results. Set ephemeral=true to search session memories.",
	}, m.searchMemories)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_memories",
		Description: "List recent memories with optional filters. Supports session_id, agent_id, device_id filters. Set ephemeral=true to list session memories.",
	}, m.listMemories)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_memory",
		Description: "Get a single memory by ID, including its extracted entities.",
	}, m.getMemory)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "update_memory",
		Description: "Update an existing memory. Re-extracts entities if content changes.",
	}, m.updateMemory)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "delete_memory",
		Description: "Delete a memory by ID.",
	}, m.deleteMemory)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "find_related",
		Description: "Find memories related to a given memory via the entity graph. Set ephemeral=true to traverse the ephemeral session graph instead of persistent memory.",
	}, m.findRelated)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_entities",
		Description: "List known entities extracted from memories, sorted by mention count. Set ephemeral=true to inspect session-only entities.",
	}, m.listEntities)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "entity_memories",
		Description: "Get all memories that mention a specific entity. Set ephemeral=true to search the ephemeral session graph instead of persistent memory.",
	}, m.entityMemories)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "memory_stats",
		Description: "Get aggregated memory statistics for the namespace: counts by type, project, tag, visibility, agent, and session.",
	}, m.memoryStats)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "end_session",
		Description: "End a session by deleting all ephemeral memories for the given session_id. Use this when a conversation or task is complete.",
	}, m.endSession)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_sessions",
		Description: "List active sessions with their memory counts. Optionally filter by agent_id.",
	}, m.listSessions)

	return server
}

// --- Tool implementations ---

func (m *MCPHandler) storeMemory(ctx context.Context, req *mcp.CallToolRequest, args StoreMemoryArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	memory, err := m.handler.StoreMemory(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(memory), memory, nil
}

func (m *MCPHandler) searchMemories(ctx context.Context, req *mcp.CallToolRequest, args SearchMemoriesArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	results, err := m.handler.SearchMemories(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(results), results, nil
}

func (m *MCPHandler) listMemories(ctx context.Context, req *mcp.CallToolRequest, args ListMemoriesArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	memories, err := m.handler.ListMemories(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(memories), memories, nil
}

type getMemoryArgs struct {
	ID string `json:"id" mcp:"The memory ID"`
}

func (m *MCPHandler) getMemory(ctx context.Context, req *mcp.CallToolRequest, args getMemoryArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	memory, err := m.handler.GetMemory(ctx, args.ID, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(memory), memory, nil
}

func (m *MCPHandler) updateMemory(ctx context.Context, req *mcp.CallToolRequest, args UpdateMemoryArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	memory, err := m.handler.UpdateMemory(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(memory), memory, nil
}

type deleteMemoryArgs struct {
	ID string `json:"id" mcp:"The memory ID to delete"`
}

func (m *MCPHandler) deleteMemory(ctx context.Context, req *mcp.CallToolRequest, args deleteMemoryArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	if err := m.handler.DeleteMemory(ctx, args.ID, uctx); err != nil {
		return errorResult(err), nil, nil
	}
	result := map[string]any{"id": args.ID, "status": "deleted"}
	return jsonResult(result), result, nil
}

func (m *MCPHandler) findRelated(ctx context.Context, req *mcp.CallToolRequest, args FindRelatedArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	results, err := m.handler.FindRelated(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(results), results, nil
}

func (m *MCPHandler) listEntities(ctx context.Context, req *mcp.CallToolRequest, args ListEntitiesArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	entities, err := m.handler.ListEntities(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(entities), entities, nil
}

func (m *MCPHandler) entityMemories(ctx context.Context, req *mcp.CallToolRequest, args EntityMemoriesArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	results, err := m.handler.GetEntityMemories(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(results), results, nil
}

func (m *MCPHandler) memoryStats(ctx context.Context, req *mcp.CallToolRequest, args MemoryStatsArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	stats, err := m.handler.GetStats(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(stats), stats, nil
}

func (m *MCPHandler) endSession(ctx context.Context, req *mcp.CallToolRequest, args EndSessionArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	if err := m.handler.EndSession(ctx, args, uctx); err != nil {
		return errorResult(err), nil, nil
	}
	result := map[string]any{"session_id": args.SessionID, "status": "ended"}
	return jsonResult(result), result, nil
}

func (m *MCPHandler) listSessions(ctx context.Context, req *mcp.CallToolRequest, args ListSessionsArgs) (*mcp.CallToolResult, any, error) {
	uctx, err := m.userContextFn(ctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	sessions, err := m.handler.ListSessions(ctx, args, uctx)
	if err != nil {
		return errorResult(err), nil, nil
	}
	return jsonResult(sessions), sessions, nil
}

// --- Result helpers ---

func errorResult(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{
			&mcp.TextContent{Text: err.Error()},
		},
	}
}

func jsonResult(v any) *mcp.CallToolResult {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return errorResult(fmt.Errorf("marshalling result: %w", err))
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(data)},
		},
	}
}
