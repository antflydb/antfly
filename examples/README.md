# Antfly Examples

This directory contains complete, production-ready examples demonstrating key Antfly features.

## Linear Merge API Examples

The **Linear Merge API** enables efficient, stateless synchronization from external data sources to Antfly with automatic change detection and deletion handling.

### [Linear Merge Markdown](./linear-merge-markdown/)

Chunks markdown documentation into sections and syncs to Antfly.

**Use cases:**
- Documentation sync from Git repositories
- Knowledge base import
- Content management

**Features:**
- Markdown chunking by headers
- Content hash optimization (skips unchanged sections)
- Dry-run preview mode
- Standalone CLI tool + test suite

**Quick start:**
```bash
go build -o linear-merge-markdown ./examples/linear-merge-markdown
./linear-merge-markdown --docs ./README.md --table docs --create-table
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
go build -o postgres-sync ./examples/postgres-sync
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/postgres"
./postgres-sync --create-table
```

## Memory Examples

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

| Feature | Markdown Sync | Postgres Sync |
|---------|---------------|---------------|
| **Data Source** | Markdown files | Postgres JSONB |
| **Sync Type** | On-demand / periodic | Real-time + periodic |
| **Latency** | N/A (manual trigger) | <100ms (LISTEN/NOTIFY) |
| **Use Case** | Documentation, static content | Live databases |
| **Complexity** | Low | Medium |
| **Dependencies** | None (just files) | Postgres with triggers |

## Key Concepts

### Linear Merge API

The Linear Merge API provides:

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

```go
// 1. Fetch data from external source
records := fetchFromSource()

// 2. Convert to Antfly format
antflyRecords := make(map[string]interface{})
for _, record := range records {
    antflyRecords[record.ID] = record.ToDocument()
}

// 3. Sync with Linear Merge
result, _ := client.LinearMerge(ctx, "my_table", antfly.LinearMergeRequest{
    Records:      antflyRecords,
    LastMergedId: cursor,
})

// 4. Check results
fmt.Printf("Upserted: %d, Skipped: %d, Deleted: %d\n",
    result.Upserted, result.Skipped, result.Deleted)

// 5. Handle pagination if needed
if result.Status == antfly.LinearMergePageStatusPartial {
    cursor = result.NextCursor
    // Continue with next batch
}
```

## Building Your Own Sync Tool

Use these examples as templates:

### 1. Identify Your Data Source

- **Files**: Use markdown example as template
- **Database**: Use Postgres example as template
- **API**: Similar to markdown example (fetch + sync)

### 2. Implement Data Fetching

```go
func fetchRecords(source string) (map[string]interface{}, error) {
    // Your custom logic here
    // Return: map[id]document
}
```

### 3. Add Change Detection (Optional)

- **File-based**: Compare modification times or content hashes
- **Database**: Use triggers (like Postgres example)
- **API**: Use webhooks or polling

### 4. Sync with Linear Merge

```go
result, err := client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
    Records:      records,
    LastMergedId: cursor,
    DryRun:       false,
})
```

### 5. Handle Errors and Pagination

```go
if result.Status == antfly.LinearMergePageStatusPartial {
    // Continue from cursor
} else if len(result.Failed) > 0 {
    // Handle individual failures
}
```

## Additional Examples

More examples coming soon:

- [ ] MySQL/MariaDB sync
- [ ] MongoDB sync
- [ ] S3/Cloud Storage sync
- [ ] REST API sync
- [ ] CSV/Excel import
- [ ] Git repository sync
- [ ] Confluence/Wiki sync

## Testing

Both examples include comprehensive test suites:

```bash
# Test markdown sync
go test -v ./src/metadata -run TestLinearMergeMarkdownDemo

# Test Postgres sync (requires Postgres)
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/postgres"
go test -v ./src/metadata -run TestLinearMergePostgresDemo
```

## Documentation

- [Linear Merge API Implementation Summary](../work-log/006-create-linear-merge-api/IMPLEMENTATION_SUMMARY.md)
- [Linear Merge API Plan](../work-log/006-create-linear-merge-api/plan.md)
- OpenAPI spec: `src/metadata/api.yaml` (lines 578-1081)

## Contributing

To add a new example:

1. Create a new directory: `examples/my-example/`
2. Add `main.go`, `README.md`, and optional `demo.sh`
3. Include test suite in `src/metadata/`
4. Update this README
5. Submit PR

## License

Same as Antfly (check root LICENSE file)
