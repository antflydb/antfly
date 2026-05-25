# Zig MCP And A2A Support

## Current State

This repo now has reusable protocol cores under `go/pkg/antfly/lib/mcp` and `go/pkg/antfly/lib/a2a`, plus Antfly-specific HTTP adapters in
`pkg/antfly/src/api/protocol_adapters.zig`.

The implementation intentionally keeps the protocol libraries independent of Antfly OpenAPI/generated types. Antfly
tools and skills are registered at the product layer and delegate back through existing HTTP/API paths so auth,
permission checks, request validation, table/query behavior, backup/restore behavior, and agent behavior stay
centralized.

## Go Parity Context

The Go implementation uses mature protocol SDKs:

- MCP is mounted with `github.com/modelcontextprotocol/go-sdk/mcp.NewStreamableHTTPHandler` in `go/pkg/antfly/src/mcp/mcp.go`. That
  SDK provides streamable HTTP sessions, `Mcp-Session-Id`, DELETE close, SSE reconnect behavior, and `Last-Event-ID`
  resumability. The Antfly Go product code exposes streamable HTTP; the SDK also supports stdio, but there is no
  Antfly-specific stdio server command wired in the Go tree.
- A2A is mounted with `github.com/a2aproject/a2a-go/a2asrv.NewJSONRPCHandler` in `go/pkg/antfly/src/metadata/a2a_adapters.go`.
  `message/stream` is a real SSE stream backed by the SDK event queue, and retrieval writes callback events into that
  queue as work progresses.
- A2A `tasks/get` and `tasks/cancel` are provided by the SDK's default in-memory task store. Antfly Go does not appear
  to configure a durable task store for this surface.
- MCP tool schemas in Go are derived by the MCP SDK from typed argument structs and `json`/`mcp` tags in
  `go/pkg/antfly/src/mcp/mcp.go`, not handwritten JSON strings.

## Implemented

- `go/pkg/antfly/lib/mcp`
  - JSON-RPC 2.0 request/response handling.
  - MCP `initialize`, `notifications/initialized`, `tools/list`, and `tools/call`.
  - Tool registry API with `Server`, `Tool`, `ToolHandler`, and `CallToolResult`.
  - Text content, structured JSON results, and tool error results.
  - Transport-shaped streamable HTTP helpers for POST responses and GET endpoint events.
  - In-memory MCP session store interface/implementation and initialize-time `Mcp-Session-Id` response headers.
  - Session-scoped SSE event IDs and `Last-Event-ID` cursor handling for streamable HTTP GET.
  - Line-oriented stdio dispatch helper for newline-framed JSON-RPC messages.
  - Standalone tests via `zig build lib-mcp-test`.
- `go/pkg/antfly/lib/a2a`
  - Agent skill registration and metadata skill routing.
  - Agent card production.
  - JSON-RPC methods for `agent/getAuthenticatedExtendedCard`, `message/send`, `message/stream`, `tasks/get`, and
    `tasks/cancel`.
  - In-memory task store interface and implementation.
  - Event sink plumbing for `message/stream` so queue events can be consumed as they are emitted.
  - Text/data part helpers and event queue helpers.
  - Standalone tests via `zig build lib-a2a-test`.
- Antfly HTTP routes
  - `GET /mcp/v1`
  - `POST /mcp/v1`
  - `POST /a2a`
  - `GET /.well-known/agent.json`
  - `GET /.well-known/agent-card.json`
  - The built-in `StdHttpListener` has an optional streaming executor hook, and the Antfly data server wires it for
    `POST /a2a` `message/stream` so A2A queue events are framed onto the HTTP response as chunked SSE frames when the
    product listener is used.
  - The retrieval agent exposes an event-sink execution path for `classification`, `step_progress`, `hit`,
    `generation`, `followup`, `eval`, and `done` milestones. The A2A retrieval skill uses that path directly so
    retrieval progress is forwarded into the A2A queue as it is produced.
- Antfly MCP tools
  - `create_table`
  - `drop_table`
  - `list_tables`
  - `create_index`
  - `drop_index`
  - `list_indexes`
  - `query`
  - `backup`
  - `restore`
  - `batch`
- Antfly A2A skills
  - `query-builder`
  - `retrieval`

## Verification

- `zig build lib-mcp-test`
- `zig build lib-a2a-test`
- `zig build raft-transport-test`
- `zig build lib-api-auth-test`
- `zig build public-api-parity-test -- --test-filter "retrieval agent event sink receives live milestones" "api http server serves retrieval agent event stream"`

The API auth test bucket includes an HTTP-level coverage test for MCP initialize, the A2A well-known agent card, and
the A2A card JSON-RPC method. It also covers MCP session response headers, MCP GET event-stream endpoint framing, A2A
`message/stream` SSE framing, and A2A `tasks/get`/`tasks/cancel`. The raft transport test bucket covers the optional
chunked streaming response path in `StdHttpListener`.

The standalone protocol tests also cover parse errors, invalid params, unknown MCP tools, unknown A2A skills, and
missing A2A tasks.

## Known Gaps

- MCP now creates server-side streamable HTTP sessions, returns `Mcp-Session-Id`/`Mcp-Protocol-Version` headers on
  initialize responses, validates inbound `Mcp-Session-Id` headers for streamable HTTP requests, and closes sessions
  via `DELETE /mcp/v1`. GET streams emit event IDs and honor `Last-Event-ID`, but historical event replay is not
  implemented yet.
- MCP has a line-oriented stdio JSON-RPC dispatcher in `go/pkg/antfly/lib/mcp`; the product CLI does not yet expose a long-running
  stdio server mode. This is also not exposed by Antfly's Go product code, even though the Go SDK supports it.
- A2A task storage is in-memory only. That matches the current Antfly Go mount; durable storage is still out of scope
  until product requirements demand it.
- The Antfly adapters now live in `protocol_adapters.zig`. They can be split further into dedicated MCP and A2A modules
  if either surface grows.
- Protocol structs are intentionally minimal. Dynamic `std.json.Value` remains the extension path for evolving MCP/A2A
  fields and tool payloads.
- MCP schemas are generated from Antfly MCP tool descriptors and cover the current Go-parity tool arguments. They are
  not yet derived from generated OpenAPI or Zig request structs.

## Long-Term Direction

The current shape is the right foundation: protocol cores stay reusable, while Antfly-specific tools and skills are
registered outside the libraries.

The next durability improvements should be:

1. Add MCP historical event replay if clients need more than cursor-aware stream continuation.
2. Expose the `go/pkg/antfly/lib/mcp` stdio dispatcher through a product CLI/server mode if local agent hosts need it.
3. Add durable task store plumbing if A2A task state needs to survive process restart.
4. Broaden adapter failure mapping and tool schema stability tests.
5. Consider deriving MCP tool schemas from generated OpenAPI or Zig request structs if the tool surface continues to
   expand.

For Go product parity, the only remaining behavior difference worth tracking is MCP historical replay after
`Last-Event-ID`. The other items above are product extensions or maintainability improvements, not missing behavior in
the current Antfly Go MCP/A2A mount.
