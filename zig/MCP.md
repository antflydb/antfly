# Zig MCP Support

## Current State

This repo now has a reusable MCP protocol core under `go/pkg/antfly/lib/mcp`, plus Antfly-specific HTTP adapters in
`pkg/antfly/src/api/protocol_adapters.zig`.

The implementation intentionally keeps the protocol library independent of Antfly OpenAPI/generated types. Antfly tools
are registered at the product layer and delegate back through existing HTTP/API paths so auth, permission checks,
request validation, table/query behavior, backup/restore behavior, and agent behavior stay centralized.

A2A and native bounded-agent behavior are documented in `A2A.md`. This file keeps only the MCP surface and the explicit
handoff points where MCP clients should call native agents or A2A skills.

## Go Parity Context

The Go implementation uses mature protocol SDKs:

- MCP is mounted with `github.com/modelcontextprotocol/go-sdk/mcp.NewStreamableHTTPHandler` in `go/pkg/antfly/src/mcp/mcp.go`. That
  SDK provides streamable HTTP sessions, `Mcp-Session-Id`, DELETE close, SSE reconnect behavior, and `Last-Event-ID`
  resumability. The Antfly Go product code exposes streamable HTTP; the SDK also supports stdio, but there is no
  Antfly-specific stdio server command wired in the Go tree.
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
- Antfly HTTP routes
  - `GET /mcp/v1`
  - `POST /mcp/v1`
- Trusted-principal auth
  - Antfly can accept `X-Antfly-Trusted-Principal: <token>` from a trusted upstream proxy when
    `ANTFLY_TRUSTED_PRINCIPAL_SECRET` is configured.
  - `ANTFLY_TRUSTED_PRINCIPAL_ISSUER` optionally constrains the token issuer. Issuer names are provider-owned; Antfly
    only verifies the configured trust boundary and maps the token's permissions, row filters, and metadata into the
    normal authenticated identity model.
- Antfly MCP tools
  - `create_table`
  - `drop_table`
  - `list_tables`
  - `describe_table`
  - `create_index`
  - `drop_index`
  - `list_indexes`
  - `describe_indexes`
  - `get_document`
  - `sample_documents`
  - `query`
  - `describe_query_request`
  - `describe_mcp_capabilities`
  - `backup`
  - `restore`
  - `batch`

## MCP Query Request Design

MCP is the deterministic database control and retrieval surface. It should expose exact table/index/document/query
operations, schema/capability discovery, and raw request pass-through. It should not duplicate the agentic query
planner. Natural-language query planning belongs in native `/agents/query-builder` or the A2A `query-builder` skill,
which can inspect context, propose a query plan, and then call MCP `query` with either shorthand args or a raw
`queryRequest`.

The MCP `query` tool has two modes:

1. Shorthand mode, for common agent calls. This keeps the existing MCP-friendly arguments such as `fullTextSearch`,
   `fullTextSearchField`, `semanticSearch`, `fields`, `limit`, `orderBy`, `indexes`, and `filterPrefix`.
2. Raw QueryRequest mode, for the full Antfly query contract. Callers pass `queryRequest`, which is forwarded as the
   JSON body for `POST /tables/{tableName}/query`.

Example raw QueryRequest call:

```json
{
  "tableName": "montessori_copilot_ft",
  "queryRequest": {
    "full_text_search": {
      "match": "individual freedom personality development",
      "field": "content"
    },
    "fields": ["document_title", "page", "content"],
    "limit": 5
  }
}
```

The raw `queryRequest` path is intentionally generic. It can carry the OpenAPI `QueryRequest` fields such as `query`,
`full_text_search`, `filter_query`, `exclusion_query`, `semantic_search`, `embedding_template`, `indexes`,
`embeddings`, `fields`, `hierarchy`, `limit`, `offset`, `order_by`, `search_after`, `search_before`, `distance_under`,
`distance_over`, `search_effort`, `merge_config`, `count`, `profile`, `reranker`, `aggregations`, `graph_searches`,
`expand_strategy`, `document_renderer`, `pruner`, `join`, and `foreign_sources`.

Raw mode rules:

- `tableName` remains the MCP path/table selector.
- `queryRequest` must be a JSON object.
- `queryRequest` is mutually exclusive with shorthand query arguments. The adapter does not merge the two modes because
  precedence rules would otherwise drift from the REST/OpenAPI contract.
- `queryRequest.table` is rejected on table-scoped MCP calls. Use `tableName` instead.
- The raw object is validated by the same `/tables/{tableName}/query` path as direct REST calls.

For hierarchical document retrieval, `limit` controls top-level hits; it does not limit child values expanded from a
source document. Prefer direct matches when the records selected by an index are the evidence an agent needs:

```json
{
  "tableName": "document_search_import",
  "queryRequest": {
    "semantic_search": "How does Antfly index documents?",
    "indexes": ["document_vectors"],
    "hierarchy": {
      "ancestors": {
        "source": {"fields": ["title", "url"]}
      }
    },
    "fields": ["text"],
    "limit": 5
  }
}
```

When a caller needs source groups with matching descendants attached, use the independently bounded `group_by.matches`
projection:

```json
"hierarchy": {
  "group_by": {
    "level": "source",
    "matches": {"limit": 3, "fields": ["text"]}
  }
}
```

The presence of `hierarchy` selects the canonical contract. Without `group_by`, including for `hierarchy: {}`, Antfly
returns direct index matches; `ancestors` only adds projected context and never changes result cardinality.
`group_by.level` currently supports `source`; its nested `matches` retain their actual `hierarchy.level`, so the response
is not tied to chunks. Every match or ancestor projection must specify `fields`; use an empty array when only hierarchy
identity is needed. Omitting `group_by.matches.limit` defaults it to three, and the server rejects values above 100 before
executing the query. Nested matches follow the effective query order and each group carries its best match score.

For source-level groups, select source fields with the top-level `fields` projection. A source ancestor projection would
duplicate the grouping-level document and is rejected. A unit ancestor projection may be combined with source grouping
when nested matches need intermediate unit context.

The legacy hierarchy controls remain accepted for existing callers, but they cannot be mixed with `group_by` or
`ancestors`. They are intentionally omitted from MCP discovery so new integrations see only the canonical controls.
MCP callers should not request
`_chunks.*`, which expands the stored child array and can create an oversized response. Antfly returns an actionable tool error
instead of sending a serialized tool result larger than its MCP compatibility budget. The server default is 96 KiB,
including TextContent and structuredContent; deployments with known client limits can change
`mcp.max_tool_result_bytes` in the Antfly configuration, or set it to zero to disable the guard.

The full OpenAPI schema is not inlined into every `tools/list` response because the schema is large and references
recursive query/reranker/graph/join definitions. The `query.queryRequest` input schema stays permissive with compact
top-level field guidance. Clients that need schema help can call `describe_query_request`, which returns a compact,
structured summary with examples and pointers to:

- `specs/openapi/antfly/metadata.yaml#/components/schemas/QueryRequest`
- `specs/openapi/antfly/query.yaml#/components/schemas/Query`

## MCP Introspection Tools

The MCP tool surface includes deterministic helpers so clients can discover table shape and build precise requests
without needing to scrape broad list responses:

- `describe_table` calls the existing table status route for one table, including schema, indexes, ranges, and storage
  status when available.
- `describe_indexes` calls the existing table index listing route for one table. It is intentionally an alias-shaped
  companion to `list_indexes` for clients that distinguish list and describe workflows.
- `sample_documents` calls the existing lookup/scan route with bounded `limit` and optional `from`, `to`,
  `inclusiveFrom`, and `fields` arguments. It is for schema/content inspection, not agentic retrieval planning.
- `describe_mcp_capabilities` returns MCP transport/session capabilities, deterministic tool categories, raw
  `queryRequest` support, shorthand query args, and native-agent query-builder handoff guidance.
- `backup` and `restore` require the same `connection` as their REST request contracts; `backup` also exposes the
  optional `format` selector.
- `batch` exposes the REST request capabilities as `inserts`, `deletes`, `transforms`, and `syncLevel`. The older
  `writes` spelling remains a compatibility alias for `inserts`, but the two cannot be combined.

These tools route through the same HTTP handlers as REST calls where possible. That keeps auth, permission checks,
validation, and error behavior aligned with the product API.

## Verification

- `zig build lib-mcp-test`
- `zig build raft-transport-test`
- `zig build lib-api-auth-test`
- `zig build root-test -- --test-filter "api http server serves fielded full-text search through mcp tools"`

The API auth test bucket includes HTTP-level coverage for MCP initialize. It also covers MCP session response headers
and MCP GET event-stream endpoint framing.

The standalone protocol tests also cover parse errors, invalid params, unknown MCP tools, and oversized tool results.

## Known Gaps

- MCP now creates server-side streamable HTTP sessions, returns `Mcp-Session-Id`/`Mcp-Protocol-Version` headers on
  initialize responses, validates inbound `Mcp-Session-Id` headers for streamable HTTP requests, and closes sessions
  via `DELETE /mcp/v1`. GET streams emit event IDs and honor `Last-Event-ID`, but historical event replay is not
  implemented yet.
- MCP has a line-oriented stdio JSON-RPC dispatcher in `go/pkg/antfly/lib/mcp`; the product CLI does not yet expose a long-running
  stdio server mode. This is also not exposed by Antfly's Go product code, even though the Go SDK supports it.
- The Antfly adapters now live in `protocol_adapters.zig`. MCP-specific adapter code can move to a dedicated module if
  the surface grows.
- Protocol structs are intentionally minimal. Dynamic `std.json.Value` remains the extension path for evolving MCP
  fields and tool payloads.
- Simple MCP schemas are generated from Antfly MCP tool descriptors. Compact schemas for hierarchy, backup, restore,
  and batch are generated from their canonical OpenAPI components by `scripts/generate_mcp_schema_fragments.py`.
  The generator resolves local references, removes verbose API-only annotations from complete tool inputs, applies
  the MCP camelCase argument overlay, and emits self-contained JSON Schema for Zig to embed at compile time.
- The raw `queryRequest` field deliberately uses a permissive top-level schema plus the `describe_query_request`
  helper to avoid inlining the full recursive OpenAPI query schema into every MCP `tools/list` response.

## Long-Term Direction

The current shape is the right foundation: protocol cores stay reusable, while Antfly-specific tools and skills are
registered outside the libraries.

The next durability improvements should be:

1. Add MCP historical event replay if clients need more than cursor-aware stream continuation.
2. Expose the `go/pkg/antfly/lib/mcp` stdio dispatcher through a product CLI/server mode if local agent hosts need it.
3. Broaden adapter failure mapping and tool schema stability tests.
4. Extend OpenAPI-derived compact schema fragments selectively when another MCP tool has a substantial structured
   request; keep simple scalar tools and runtime permission filtering in the Zig registry.
