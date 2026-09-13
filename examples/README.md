# Antfly Examples

This directory contains complete examples demonstrating key Antfly features.
Each example is its own Go module; build and run it from inside its directory
with `GOWORK=off`.

## Ingestion Examples

### [docsaf](./docsaf/)

Ingest documentation from local files, Git repositories, S3, Google Drive, and
web sources into Antfly with `docsaf`, including entity extraction.

**Use cases:**
- Documentation sync from Git repositories
- Knowledge base import
- Content management

**Quick start:**
```bash
cd examples/docsaf
./run-demo.sh
```

### [Postgres Real-time Sync](./postgres-sync/)

Real-time synchronization from Postgres JSONB columns to Antfly using LISTEN/NOTIFY.

**Use cases:**
- Keep Antfly in sync with existing Postgres databases
- Real-time search for Postgres data
- Hybrid SQL + vector search

**Features:**
- Real-time updates via LISTEN/NOTIFY
- Efficient batching (1-second window)
- Periodic full sync for consistency
- Production-ready daemon with metrics

**Quick start:**
```bash
# Start Postgres
docker run --name postgres-demo -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:16

# Set up schema
psql postgresql://postgres:postgres@localhost:5432/postgres -f examples/postgres-sync/schema.sql

# Build and run
cd examples/postgres-sync
GOWORK=off go build -o postgres-sync .
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/postgres"
./postgres-sync --create-table
```

For a change-data-capture path that needs no daemon, see the
[Stream PostgreSQL into Antfly](../docs/guides/cdc-replication.mdx) guide, which
uses logical replication managed by Antfly itself.

### [Pinecone Migration](./pinecone-migration/)

Migrate vector embeddings from Pinecone into Antfly.

## Search Examples

### [Image Search](./image-search/)

Index images with native multimodal embeddings and search them with text or
image queries.

### [Epstein](./epstein/)

Document corpus ingestion with entity extraction and a graph visualization.

### [Screenshots](./screenshots-shots-shots/)

Index screenshots and search them semantically.

## Embedded and Memory Examples

### [Antfly Lite Go](./antfly-lite-go/)

Embed Antfly Lite directly in a Go process with a live `.aflite` database and
export a portable `.afb` backup for restore or promotion.

**Use cases:**
- Local-first desktop and edge applications
- Embedded search in Go services
- Tests and demos that should not start a server

**Features:**
- Creates a native `.aflite` database on first run, then reopens it through the
  Go Lite binding
- Writes and reads JSON documents without a server process
- Exports a portable `.afb` backup

**Quick start:**
```bash
cd zig
zig build capi
cd ../examples/antfly-lite-go
GOWORK=off go run . --reset
```

### [Antfly Lite Retrieval Template](./antfly-lite-retrieval-go/)

Build a local-first retrieval app on Antfly Lite with a native `.aflite`
database, caller-supplied embeddings, full-text search, dense vector search, and
hybrid search.

**Use cases:**
- Embedded retrieval in desktop, edge, or single-user apps
- Local demos that should not require an inference service
- Seeded retrieval fixtures that can later promote into normal Antfly

**Features:**
- Creates a native `.aflite` database on first run, then reopens it through the
  Go Lite binding
- Initializes schema, full-text, and dense vector indexes in one `.aflite` file
- Writes documents with caller-supplied embeddings
- Runs full-text, dense, and hybrid search locally
- Exports a portable `.afb` backup for restore, promotion, or archival use

**Quick start:**
```bash
cd zig
zig build capi
cd ../examples/antfly-lite-retrieval-go
GOWORK=off go run . --reset
```

### [memoryaf + docsaf](./memoryaf/)

Turn documentation into `memoryaf` records from local files, Git, S3, Google Drive, or web sources, with local watch mode for filesystem sync.

**Use cases:**
- Searchable long-term memory built from docs
- Markdown knowledge bases with stable source references
- Live local-doc sync during authoring

**Features:**
- `docsaf` section extraction for Markdown, MDX, OpenAPI, and more
- `memoryaf` source references (`source_id`, `source_path`, `section_path`, etc.)
- One-shot sync across multiple `docsaf` backends plus `fsnotify` watch mode for local directories
- Create/update/delete reconciliation against managed memories

**Quick start:**
```bash
cd examples/memoryaf
GOWORK=off go run . watch --dir ../../docs --project antfly-docs
```

## Comparison

| Feature | docsaf | Postgres Sync | Antfly Lite |
|---------|--------|---------------|-------------|
| **Data Source** | Files, Git, S3, Drive, web | Postgres JSONB | In-process documents |
| **Sync Type** | On-demand / watch | Real-time + periodic | N/A (embedded) |
| **Latency** | Seconds (watch mode) | <100ms (LISTEN/NOTIFY) | In-process |
| **Use Case** | Documentation, static content | Live databases | Desktop, edge, tests |
| **Dependencies** | None | Postgres with triggers | `libantfly` from `zig build capi` |

## Key Concepts

### Linear Merge API

The Linear Merge API enables efficient, stateless synchronization from external
data sources to Antfly with automatic change detection and deletion handling.
It provides:

1. **Stateless Sync**: No server-side session tracking
2. **Content Hashing**: Automatically skips unchanged documents
3. **Auto-deletion**: Removes documents not in source
4. **Shard-aware**: Handles shard boundaries with cursors
5. **Idempotent**: Safe to re-run

### How It Works

```
External Source              Antfly
     │                         │
     │  1. Read data           │
     ├──────────────┐          │
     │              │          │
     │  2. Convert to records  │
     │     {id: doc, ...}      │
     │              │          │
     │  3. Linear Merge API    │
     │              └─────────▶│
     │                         │
     │  4. For each batch:     │
     │     - Scan storage      │
     │     - Compare hashes    │
     │     - Upsert changed    │
     │     - Delete missing    │
     │                         │
     │  5. Return stats        │
     │◀────────────────────────│
```

### Typical Workflow

The Go SDK lives at `github.com/antflydb/antfly/go/pkg/sdk` (package `sdk`).

```go
import "github.com/antflydb/antfly/go/pkg/sdk"

client, _ := sdk.NewAntflyClient("http://localhost:8080", http.DefaultClient)

// 1. Fetch data from external source
records := fetchFromSource()

// 2. Convert to Antfly format
antflyRecords := make(map[string]interface{})
for _, record := range records {
    antflyRecords[record.ID] = record.ToDocument()
}

// 3. Sync with Linear Merge
result, _ := client.LinearMerge(ctx, "my_table", sdk.LinearMergeRequest{
    Records:      antflyRecords,
    LastMergedId: cursor,
})

// 4. Check results
fmt.Printf("Upserted: %d, Skipped: %d, Deleted: %d\n",
    result.Upserted, result.Skipped, result.Deleted)

// 5. Handle pagination if needed
if result.NextCursor != "" {
    cursor = result.NextCursor
    // Continue with next batch
}
```

`ExecuteLinearMerge` wraps this loop: pass an iterator of record pages and it
drives the cursor, dry-run, sync level, and write options for you.

## Building Your Own Sync Tool

Use these examples as templates:

### 1. Identify Your Data Source

- **Files**: Use the docsaf example as a template
- **Database**: Use the Postgres example as a template
- **API**: Fetch, convert, and merge, as in the workflow above

### 2. Implement Data Fetching

```go
func fetchRecords(source string) (map[string]interface{}, error) {
    // Your custom logic here
    // Return: map[id]document
}
```

### 3. Add Change Detection (Optional)

- **File-based**: Compare modification times or content hashes
- **Database**: Use triggers (like the Postgres example)
- **API**: Use webhooks or polling

### 4. Sync with Linear Merge

```go
result, err := client.LinearMerge(ctx, tableName, sdk.LinearMergeRequest{
    Records:      records,
    LastMergedId: cursor,
    DryRun:       false,
})
```

### 5. Handle Errors and Pagination

```go
if result.NextCursor != "" {
    // Continue from result.NextCursor
} else if len(result.Failed) > 0 {
    // Handle individual failures
}
```

## Testing

```bash
# Test the docs ingestion example
(cd examples/docsaf && GOWORK=off go test ./...)

# Test Postgres sync (requires Postgres)
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/postgres"
(cd examples/postgres-sync && GOWORK=off go test ./...)
```

## Documentation

- [Antfly docs](https://antfly.io/docs)
- OpenAPI specs: `specs/openapi/antfly/`

## Contributing

To add a new example:

1. Create a new directory: `examples/my-example/`
2. Add `main.go`, `README.md`, and optional `demo.sh`
3. Include a test suite alongside the example
4. Update this README
5. Submit PR

## License

Same as Antfly (check root LICENSE file)
