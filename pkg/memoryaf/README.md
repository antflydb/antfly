# memoryaf

Shared team memory for AI agents. Open source. Backed by [Antfly](https://antfly.io).

memoryaf gives your AI agents persistent, searchable long-term memory — across sessions, teammates, and projects. It exposes an **MCP server** (for Claude Code, Cursor, and other MCP clients), a lightweight **HTTP API**, and an embedded **dashboard UI** backed by Antfly's hybrid search and graph indexes.

This Go package (`pkg/memoryaf`) is the core library.

`memoryaf` now supports both persistent memories and ephemeral session memories. Memory records can also carry stable references back to external documents, which is the main bridge for interoperating with markdown-centric memory systems and `docsaf`.

## Usage

```go
import (
    "github.com/antflydb/antfly/pkg/memoryaf"
    "go.uber.org/zap"
)

// Create a handler with an Antfly client and optional entity extractor.
handler := memoryaf.NewHandler(antflyClient, extractor, logger)

// Wrap as an MCP HTTP handler.
mcpHandler := memoryaf.NewMCPHandler(handler, userContextFn)
http.Handle("/mcp", mcpHandler)

// Or serve the JSON API + embedded dashboard UI.
httpHandler := memoryaf.NewHTTPHandler(handler, nil)
http.Handle("/", httpHandler)
```

When `NewHTTPHandler` is created with `nil`, it reads identity from request headers:

- `X-User-ID` default: `dashboard`
- `X-Namespace` default: `default`
- `X-Role` default: `member`
- `X-Agent-ID`, `X-Device-ID`, `X-Session-ID`: optional

That keeps the embedded dashboard safe by default while still allowing admin operations when you intentionally opt into them.

### Entity Extraction

memoryaf defines a pluggable `Extractor` interface for named entity recognition:

```go
type Extractor interface {
    Extract(ctx context.Context, texts []string, opts ExtractOptions) ([]Extraction, error)
}
```

The built-in `NERClient` implements this using [Termite](https://antfly.io/termite) with the GLiNER2 model. You can also provide your own implementation (e.g. tool-calling LLM, spaCy, etc.).

```go
// Use the built-in Termite/GLiNER2 extractor.
extractor, err := memoryaf.DefaultNERClient(logger)

// Or pass nil to disable entity extraction entirely.
handler := memoryaf.NewHandler(client, nil, logger)
```

Extracted entities are linked via Antfly [graph indexes](https://antfly.io/docs/api/index-management#graph-indexes-and-edge-ttl), powering `find_related`, `entity_memories`, and graph-expanded search. If no extractor is configured, everything except entity features works normally.

### Handler Options

```go
handler := memoryaf.NewHandler(client, extractor, logger,
    memoryaf.WithEntityLabels([]string{"person", "technology", "service"}),
    memoryaf.WithEntityThreshold(0.7),
)
```

## Memory Types

- **Episodic** — *what happened*. Chronological events: incidents, debugging sessions, decisions made in context.
- **Semantic** — *what we know*. Factual knowledge: architecture decisions, conventions, preferences.
- **Procedural** — *how to do things*. Workflow templates: runbooks, checklists, standard procedures.

## MCP Tools

The MCP server exposes 12 tools:

| Tool | Description |
|------|-------------|
| `store_memory` | Store a memory with auto entity extraction, optional ephemeral TTL, and optional external source references |
| `search_memories` | Hybrid semantic + full-text search with optional graph expansion and session/agent/device scoping |
| `list_memories` | List recent memories with filters, including ephemeral session memories |
| `get_memory` | Get a single memory by ID |
| `update_memory` | Update an existing memory |
| `delete_memory` | Delete a memory by ID |
| `find_related` | Find related memories via entity graph traversal in persistent or ephemeral memory |
| `list_entities` | List extracted entities by mention count from persistent or ephemeral memory |
| `entity_memories` | Get all memories mentioning a specific entity from persistent or ephemeral memory |
| `memory_stats` | Aggregated stats by type, project, tag, visibility, agent, and session |
| `end_session` | Delete all ephemeral memories owned by the caller for a session, or all session memories for admins |
| `list_sessions` | List active ephemeral sessions and their memory counts |

## Team Mode

Each namespace gets its own Antfly table for full data isolation. Memories default to **team** visibility. Use `"visibility": "private"` to keep memories to yourself.

## Session Memory

- Set `ephemeral=true` to store session-scoped memories in a separate TTL-backed table.
- `scope: "session"`, `scope: "agent"`, and `scope: "device"` narrow `search_memories` using caller identity.
- `scope: "session"` uses `session_id` from the request or `UserContext`; if neither is available, the call fails instead of widening unexpectedly.
- `get_memory`, `update_memory`, and `delete_memory` now resolve IDs across both persistent and ephemeral storage.

## External Source References

Use these fields when a memory is derived from documentation, a markdown note, or another external system instead of being authored directly in MCP:

- `source_backend`: where the canonical document lives, such as `filesystem`, `git`, `s3`, `google_drive`, or `web`
- `source_id`: stable external identifier such as a Drive file ID, `s3://bucket/key`, or `repo@ref:path`
- `source_path`: backend-relative path or object key
- `source_url`: canonical human-openable URL for the document or section
- `source_version`: optional version marker such as a commit SHA, ETag, or modified timestamp
- `section_path`: heading hierarchy inside the source document

The older `source` field still exists, but it is best treated as free-form origin context, not a canonical external reference.

## docsaf Interop

`docsaf` already produces structured document sections with file path, URL, section hierarchy, and source metadata. The clean integration pattern is:

1. Use `docsaf` to ingest filesystem, GitHub/git, S3, Google Drive, or web content into `DocumentSection`s.
2. Map each section into a `store_memory` call, typically as a semantic or procedural memory.
3. Preserve the canonical document identity in `source_backend`, `source_id`, `source_path`, `source_url`, `source_version`, and `section_path`.
4. Keep the markdown file or external object as the source of truth; use `memoryaf` as the searchable memory/index layer.

See [docsaf-integration.md](./docsaf-integration.md) for the backend mapping conventions.

## Dashboard

The embedded dashboard is served by `NewHTTPHandler`. It includes:

- an overview page with stats, recent memories, and health/info panels
- memory search, create, edit, delete, related-memory inspection, and sibling-section lookup for docsaf-backed memories
- entity browsing with linked-memory lookup
- settings for browser-stored identity, namespace initialization, and ephemeral session management

The HTTP API behind the dashboard exposes:

- `GET /health`
- `GET /api/v1/info`
- `GET/POST /api/v1/memories`
- `POST /api/v1/memories/search`
- `GET/PUT/DELETE /api/v1/memories/{id}`
- `GET /api/v1/memories/{id}/related`
- `GET /api/v1/entities`
- `GET /api/v1/entities/{label}:{text}/memories`
- `GET /api/v1/stats`
- `GET /api/v1/sessions`
- `POST /api/v1/sessions/{id}/end`
- `POST /api/v1/namespaces/{namespace}/init`

Unlike the older standalone TS dashboard, this UI understands:

- `ephemeral` session memories
- `session_id`, `agent_id`, and `device_id`
- `source_backend`, `source_id`, `source_path`, `source_url`, `source_version`
- `section_path` for docsaf-derived content

`find_related` and `entity_memories` use graph merge strategy `union`, matching the broader recall behavior used elsewhere in memory search.

Source-reference fields are treated as immutable after creation. If an external document identity changes, the recommended flow is to create a replacement memory rather than mutate those identifiers in place.

## Configuration

Key defaults used by the built-in `NERClient`:

| Setting | Default | Description |
|---------|---------|-------------|
| Termite URL | `http://localhost:11433` (or `TERMITE_URL` env) | Termite API URL |
| NER model | `fastino/gliner2-base-v1` | GLiNER2 model for entity recognition |
| NER labels | person, organization, project, technology, service, tool, framework, pattern | Entity types to extract |
| Entity threshold | `0.5` | Minimum entity confidence score |
| Embedding dimension | `384` | Vector dimension for the embedding index |
| Embedding provider | `antfly` | Managed embedder for semantic search |

## License

MIT
