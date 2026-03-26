# memoryaf

Shared team memory for AI agents. Open source. Backed by [Antfly](https://antfly.io).

memoryaf gives your AI agents persistent, searchable long-term memory — across sessions, teammates, and projects. It exposes an **MCP server** (for Claude Code, Cursor, and other MCP clients) backed by Antfly's hybrid search and graph indexes.

This Go package (`pkg/memoryaf`) is the core library.

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
```

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

The MCP server exposes 10 tools:

| Tool | Description |
|------|-------------|
| `store_memory` | Store a memory with auto entity extraction |
| `search_memories` | Hybrid semantic + full-text search with optional graph expansion |
| `list_memories` | List recent memories with filters |
| `get_memory` | Get a single memory by ID |
| `update_memory` | Update an existing memory |
| `delete_memory` | Delete a memory by ID |
| `find_related` | Find related memories via entity graph traversal |
| `list_entities` | List extracted entities by mention count |
| `entity_memories` | Get all memories mentioning a specific entity |
| `memory_stats` | Aggregated stats by type, project, tag, visibility |

## Team Mode

Each namespace gets its own Antfly table for full data isolation. Memories default to **team** visibility. Use `"visibility": "private"` to keep memories to yourself.

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
