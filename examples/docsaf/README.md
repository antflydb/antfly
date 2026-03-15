# docsaf - Documentation Sync to Antfly

**docsaf** is a tool that syncs documentation files (Markdown, MDX, and OpenAPI specs) to Antfly using the Linear Merge API, with automatic change detection and type separation.

## What is Linear Merge?

The Linear Merge API is a stateless, progressive synchronization API that:

- **Efficiently syncs** external data sources to Antfly tables
- **Detects changes** using content hashing (skips unchanged documents)
- **Auto-deletes** documents removed from the source
- **Handles shard boundaries** with cursor-based pagination
- **Supports dry-run** to preview changes before applying

Perfect for syncing documentation, configuration files, or any external data source.

## Features

1. **Multi-format support**: Processes Markdown (.md), MDX (.mdx), and OpenAPI (.yaml, .yml, .json) files
2. **Smart chunking**: Uses [goldmark](https://github.com/yuin/goldmark) to parse Markdown/MDX into sections by headings
3. **Frontmatter parsing**: Extracts YAML frontmatter (title, description, etc.) from MDX files
4. **Wildcard filtering**: Include/exclude files using glob patterns with `**` support
5. **OpenAPI parsing**: Uses [libopenapi](https://github.com/pb33f/libopenapi) to extract paths, schemas, and info from OpenAPI specs
6. **Type separation**: Different document types are stored with distinct `_type` values for targeted querying
7. **Content hashing**: Automatically skips unchanged sections on re-sync
8. **Incremental updates**: Only updates modified sections
9. **Deletion detection**: Removes sections that no longer exist in source
10. **Dry run mode**: Preview what will change before committing

## Document Types

docsaf creates documents with the following `_type` values:

| Type | Description | Source |
|------|-------------|--------|
| `markdown_section` | Sections from `.md` files | Chunked by headings using goldmark |
| `mdx_section` | Sections from `.mdx` files | Chunked by headings using goldmark |
| `openapi_info` | API information | Info object from OpenAPI specs |
| `openapi_path` | API path operations | GET /users, POST /orders, etc. |
| `openapi_schema` | Data schemas | Component schemas from OpenAPI specs |

Each type has its own schema with specific metadata fields. See `schemas.yaml` for details.

## Prerequisites

1. **Antfly running locally**:
   ```bash
   cd /path/to/antfly
   go run ./cmd/antfly swarm
   ```

2. **Build docsaf** (from antfly root):
   ```bash
   go build -o docsaf ./examples/docsaf
   ```

## Usage

docsaf has three subcommands:

1. **`prepare`** - Process files and create sorted JSON data
2. **`load`** - Load prepared JSON data into Antfly
3. **`sync`** - Full pipeline (prepare + load in one step)

### Subcommand: prepare

Process documentation files and save to JSON:

```bash
./docsaf prepare \
  --dir /path/to/your/docs \
  --output docs.json
```

**Flags:**
- `--dir` *(required)* - Path to directory containing documentation files
- `--output` - Output JSON file path (default: `docs.json`)
- `--base-url` - Base URL for generating document links (e.g., `https://docs.example.com`)
- `--include` - Include pattern (can be repeated, supports `**` wildcards)
- `--exclude` - Exclude pattern (can be repeated, supports `**` wildcards)

### Subcommand: load

Load prepared JSON data into Antfly:

```bash
./docsaf load \
  --input docs.json \
  --table docs \
  --create-table
```

Dry run to preview changes:

```bash
./docsaf load \
  --input docs.json \
  --table docs \
  --dry-run
```

**Flags:**
- `--input` - Input JSON file path (default: `docs.json`)
- `--url` - Antfly API URL (default: `http://localhost:8080/api/v1`)
- `--table` - Table name to merge into (default: `docs`)
- `--create-table` - Create table if it doesn't exist (default: `false`)
- `--num-shards` - Number of shards for new table (default: `1`)
- `--batch-size` - Linear merge batch size (default: `10`)
- `--embedding-model` - Embedding model to use (default: `embeddinggemma`)
- `--chunker-strategy` - Chunker strategy: `hugot`, `semantic`, or `fixed` (default: `hugot`)
- `--target-tokens` - Target tokens for chunking (default: `512`)
- `--overlap-tokens` - Overlap tokens for chunking (default: `50`)
- `--dry-run` - Preview changes without applying them (default: `false`)

### Subcommand: sync

Full pipeline - process files and load directly (original behavior):

```bash
./docsaf sync \
  --dir /path/to/your/docs \
  --table docs \
  --create-table
```

**Flags:**
- `--dir` *(required)* - Path to directory containing documentation files
- `--url` - Antfly API URL (default: `http://localhost:8080/api/v1`)
- `--table` - Table name to merge into (default: `docs`)
- `--base-url` - Base URL for generating document links (e.g., `https://docs.example.com`)
- `--create-table` - Create table if it doesn't exist (default: `false`)
- `--num-shards` - Number of shards for new table (default: `1`)
- `--batch-size` - Linear merge batch size (default: `10`)
- `--embedding-model` - Embedding model to use (default: `embeddinggemma`)
- `--chunker-strategy` - Chunker strategy: `hugot`, `semantic`, or `fixed` (default: `hugot`)
- `--target-tokens` - Target tokens for chunking (default: `512`)
- `--overlap-tokens` - Overlap tokens for chunking (default: `50`)
- `--dry-run` - Preview changes without applying them (default: `false`)
- `--include` - Include pattern (can be repeated, supports `**` wildcards)
- `--exclude` - Exclude pattern (can be repeated, supports `**` wildcards)

## Example Workflows

### 1. Two-Step Workflow (Prepare + Load)

**Step 1: Prepare the data**

```bash
# Process files and create JSON
./docsaf prepare \
  --dir ./my-docs \
  --output my-docs.json

# Output:
# === docsaf prepare - Process Documentation Files ===
# ✓ Found 127 document sections
#
# Document types found:
#   - markdown_section: 45
#   - openapi_path: 52
#   - openapi_schema: 24
#   - openapi_info: 6
#
# ✓ Prepared data written to my-docs.json
```

**Step 2: Load into Antfly**

```bash
# Load the prepared JSON data
./docsaf load \
  --input my-docs.json \
  --table docs \
  --create-table

# Output:
# === docsaf load - Load Data to Antfly ===
# ✓ Loaded 127 records
# Upserted: 127
# Skipped: 0
# Deleted: 0
```

**Benefits of two-step workflow:**
- Separate data processing from loading
- Can version control the JSON file
- Can manually inspect/modify the JSON before loading
- Can load the same data to multiple tables/clusters

### 2. One-Step Workflow (Sync)

```bash
# Process and load in one command
./docsaf sync \
  --dir ./my-docs \
  --table docs \
  --create-table

# Output:
# === docsaf sync - Full Pipeline ===
# ✓ Found 127 document sections
# Upserted: 127
# Skipped: 0
# Deleted: 0
```

### 3. Re-sync (No Changes)

```bash
# Second run - nothing changed
./docsaf sync --dir ./my-docs --table docs

# Output:
# ✓ Found 127 document sections
# Upserted: 0
# Skipped: 127 (unchanged)
# Deleted: 0
```

The content hash optimization means unchanged documents are **skipped entirely** - no expensive writes!

### 4. Incremental Update

After editing a file:

```bash
./docsaf sync --dir ./my-docs --table docs

# Output:
# ✓ Found 127 document sections
# Upserted: 3 (updated sections)
# Skipped: 124 (unchanged)
# Deleted: 0
```

Only the **modified sections** are updated!

### 5. Sync Antfly's Own Documentation

```bash
# From the antfly repository root
./docsaf sync \
  --dir . \
  --create-table \
  --table antfly_docs
```

This will process:
- All markdown files (README.md, work-log/*.md, etc.)
- MDX documentation
- OpenAPI specs in `src/metadata/api.yaml` and `src/usermgr/api.yaml`

### 6. Sync www/ Website Documentation (with Wildcards)

```bash
# From the antfly repository root
# Using exclude patterns to skip build artifacts and code
./docsaf sync \
  --dir www \
  --exclude "**/node_modules/**" \
  --exclude "**/.next/**" \
  --exclude "**/out/**" \
  --exclude "**/work-log/**" \
  --exclude "**/scripts/**" \
  --exclude "**/config/**" \
  --exclude "**/components/**" \
  --exclude "**/app/**" \
  --exclude "**/public/**" \
  --table antfly_website_docs \
  --create-table
```

Or, more simply using include patterns:

```bash
# Only index files in the content directory
./docsaf sync \
  --dir www \
  --include "**/content/**" \
  --table antfly_website_docs \
  --create-table
```

This will:
- Extract frontmatter (title, description) from MDX files
- Process 22 MDX documentation files in `www/content/docs/`
- Skip auto-generated navigation and build artifacts
- Create sections with frontmatter metadata

## How It Works

### 1. File Discovery

The `RepositoryTraverser` walks the directory tree and identifies files by extension:
- `.md`, `.mdx` → `MarkdownProcessor`
- `.yaml`, `.yml`, `.json` → `OpenAPIProcessor` (if valid OpenAPI)

### 2. Markdown/MDX Processing (goldmark)

Using goldmark's AST parser:

```markdown
# Main Title           → Section 1 (markdown_section)
Content here...

## Subsection A        → Section 2 (markdown_section)
More content...

## Subsection B        → Section 3 (markdown_section)
Even more content...
```

1. Parse the document into an Abstract Syntax Tree
2. Walk the AST and identify heading nodes
3. Split content into sections at each heading
4. Create a `DocumentSection` for each section with:
   - Unique ID (hash of file path + heading)
   - Title (heading text)
   - Content (section text with markdown formatting)
   - Metadata (heading level, is_mdx flag)
   - Type: `markdown_section` or `mdx_section`

### 3. OpenAPI Processing (libopenapi)

Using libopenapi to parse OpenAPI v3 specifications:

**Input**: `api.yaml` with paths, schemas, info

**Output**:
1. **Info document** (`openapi_info`): API title, version, description
2. **Path documents** (`openapi_path`): One per operation (GET /users, POST /orders)
3. **Schema documents** (`openapi_schema`): One per component schema (User, Order)

Each document includes rich metadata (HTTP method, tags, operation ID, etc.)

### 4. Linear Merge Process

The core sync logic uses batched Linear Merge with cursor-based pagination:

<!-- include: main.go#batched_linear_merge -->

```
┌─────────────────────┐
│ Documentation Files │
│ (.md, .mdx, .yaml)  │
└──────────┬──────────┘
           │ 1. Discover & process files
           ▼
┌─────────────────────┐
│ Document Sections   │ {id_1: doc_1, id_2: doc_2, ...}
│ (with _type)        │
└──────────┬──────────┘
           │ 2. Linear Merge API
           ▼
┌─────────────────────┐
│ For each range:     │
│  - Scan storage     │
│  - Compare hash     │
│  - Upsert/Skip      │
│  - Delete old       │
└──────────┬──────────┘
           │ 3. Result
           ▼
┌─────────────────────┐
│ Antfly Table        │ Synced with source!
│ (typed documents)   │
└─────────────────────┘
```

### 5. Content Hash Optimization

Each document's content is hashed:

```go
hash := xxhash(canonical_json(document))
```

If the hash matches what's already in storage → **SKIP** (no write needed!)

### 6. Auto-Deletion

Documents in the table but **not in the merge request** are automatically deleted:

```
Storage:  [doc_1, doc_2, doc_3, doc_4]
Request:  [doc_1, doc_2, doc_5]        ← doc_5 is new

Result:
  - doc_1: SKIP (unchanged)
  - doc_2: SKIP (unchanged)
  - doc_3: DELETE (not in request)
  - doc_4: DELETE (not in request)
  - doc_5: UPSERT (new)
```

## Querying the Data

Once ingested, you can query by document type:

### Search all markdown sections

```bash
curl -X POST http://localhost:8080/api/v1/tables/docs/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "_type": "markdown_section"
      }
    },
    "full_text_query": "authentication"
  }'
```

### Search OpenAPI paths

```bash
curl -X POST http://localhost:8080/api/v1/tables/docs/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "_type": "openapi_path"
      }
    },
    "full_text_query": "users"
  }'
```

### Filter by HTTP method

```bash
curl -X POST http://localhost:8080/api/v1/tables/docs/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "bool": {
        "must": [
          {"match": {"_type": "openapi_path"}},
          {"match": {"metadata.http_method": "get"}}
        ]
      }
    }
  }'
```

### Get OpenAPI schemas

```bash
curl -X POST http://localhost:8080/api/v1/tables/docs/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "_type": "openapi_schema"
      }
    }
  }'
```

## Document Schemas

The `schemas.yaml` file contains example schema definitions for each document type with Antfly's custom annotations:

- **`x-antfly-types`**: Specify field types (`text`, `keyword`, `embedding`, etc.)
- **`x-antfly-index`**: Enable/disable indexing for a field
- **`x-antfly-include-in-all`**: Include fields in the Bleve `_all` field for full-text search

Example for `markdown_section`:

```yaml
markdown_section:
  type: object
  x-antfly-include-in-all:
    - title
    - content
  properties:
    title:
      type: string
      x-antfly-types:
        - text
        - keyword
    content:
      type: string
      x-antfly-types:
        - text
    _type:
      type: string
      x-antfly-types:
        - keyword
```

## Advanced Features

### Hybrid Search with Embeddings

Add embedding enricher to enable semantic search. Here's how docsaf creates embedding indexes:

<!-- include: main.go#create_embedding_index -->

You can also manually configure indexes:

```go
// Add indexes to the table
client.UpdateTable(ctx, "docs", antfly.UpdateTableRequest{
    Indexes: []antfly.IndexConfig{
        {
            Type: "full_text",
            Name: "bleve",
        },
        {
            Type: "embeddings",
            Name: "embeddings",
            Config: map[string]any{
                "embedding_field": "content_embedding",
            },
        },
        {
            Type: "embeddingenricher",
            Name: "enricher",
            Config: map[string]any{
                "text_fields": []string{"content"},
                "output_field": "content_embedding",
                "model": "text-embedding-3-small",
            },
        },
    },
})
```

### Incremental Updates (Cron)

Run docsaf periodically to sync changes:

```bash
# Cron job: sync docs every hour
0 * * * * cd /path/to/antfly && ./docsaf sync --dir /path/to/docs --table docs
```

Or use the two-step workflow to separate data processing from loading:

```bash
# Cron job: prepare data every hour, load separately
0 * * * * cd /path/to/antfly && ./docsaf prepare --dir /path/to/docs --output /tmp/docs.json
15 * * * * cd /path/to/antfly && ./docsaf load --input /tmp/docs.json --table docs
```

Only new or modified files will be updated.

### Filter by File Type

Query specific document types:

```bash
# Only MDX files
curl ... -d '{"query": {"match": {"_type": "mdx_section"}}}'

# Only OpenAPI paths with tag "users"
curl ... -d '{
  "query": {
    "bool": {
      "must": [
        {"match": {"_type": "openapi_path"}},
        {"match": {"metadata.tags": "users"}}
      ]
    }
  }
}'
```

## Implementation Details

### ID Generation

Document IDs are generated using SHA-256 hashes:
```
doc_<hash(file_path + identifier)[:16]>
```

This ensures:
- Stable IDs across runs (same file → same ID)
- Uniqueness within the repository
- Efficient lookups

### Error Handling

- Invalid OpenAPI files are logged and skipped (doesn't fail the entire run)
- File read errors are logged and skipped
- Parse errors are logged with file path for debugging

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Initial import | O(n) writes | All sections inserted |
| Re-sync unchanged | O(n) reads, **0 writes** | Hash comparison only |
| Incremental update | O(n) reads, O(k) writes | k = changed sections |
| Deletion scan | O(n) reads | Identifies removed sections |

**Batch size recommendations**:
- Small directories: 100-500 sections/request
- Medium directories: 1,000-5,000 sections/request
- Large directories: 10,000+ sections/request (may span multiple batches)

## Troubleshooting

**Table doesn't exist**:
```bash
# Add --create-table flag
./docsaf --dir . --create-table
```

**Connection refused**:
```bash
# Make sure Antfly is running
go run ./cmd/antfly swarm
```

**No documents found**:
- Ensure the directory contains `.md`, `.mdx`, or valid OpenAPI spec files
- Check file extensions (case-insensitive)
- OpenAPI files must be valid v3 specifications

**OpenAPI parsing errors**:
- Verify the file is a valid OpenAPI 3.x specification
- Use a validator like [swagger-editor](https://editor.swagger.io/)
- Check for YAML/JSON syntax errors
- Note: Invalid OpenAPI files are logged and skipped

## Real-World Use Cases

1. **Documentation Sync**: Keep Antfly in sync with documentation directories (Markdown, MDX, OpenAPI)
2. **API Documentation**: Ingest OpenAPI specs for searchable API reference
3. **Knowledge Base**: Import and maintain wiki/docs from external sources
4. **Multi-Format Search**: Search across markdown docs and API specs simultaneously
5. **Developer Portal**: Build searchable developer documentation from multiple sources

## Next Steps

- Add embedding generation to enable semantic search across all document types
- Implement Git integration to track document versions and authors
- Add support for more file formats (reStructuredText, AsciiDoc, etc.)
- Create webhooks for automatic re-sync on file changes
- Build a UI for browsing documents by type

## API Reference

See the full Linear Merge API documentation:
- OpenAPI spec: `src/metadata/api.yaml` (search for `LinearMerge`)
- [goldmark documentation](https://github.com/yuin/goldmark)
- [libopenapi documentation](https://github.com/pb33f/libopenapi)
- [OpenAPI Specification](https://spec.openapis.org/oas/v3.1.0)

## Project Files

<!-- files: main.go, schemas.yaml, run-demo.sh -->
