# Epstein Documents Search

A complete example demonstrating how to use Antfly to index and search the publicly released Jeffrey Epstein court documents and DOJ files.

## Overview

This tool downloads, processes, and indexes PDF documents from:

1. **January 2024 Court Unsealing** - 943 pages from Giuffre v. Maxwell (Case 1:15-cv-07433-LAP)
2. **DOJ December 2025 Release** - 4,055+ documents across 8 datasets released via the Epstein Files Transparency Act (EFTA)
3. **DOJ January 2026 Release** - 3.5 million+ pages across datasets 10-12 (dataset 9 excluded due to incomplete release)

The documents are processed page-by-page, chunked for semantic search, and made searchable through both BM25 (full-text) and vector similarity search.

## Quick Start

### Prerequisites

- Go 1.21+
- Running Antfly instance with Termite (for embeddings)

### 1. Start Antfly

```bash
# From the antfly root directory
go run ./cmd/antfly swarm
```

This starts a single-node Antfly cluster with:
- Metadata server on port 8080
- Store server on port 8081
- Termite ML service on port 8082

### 2. Build the Tool

```bash
cd examples/epstein
go build -o epstein .
```

### 3. Download Documents

Choose a dataset based on your needs:

```bash
# Option A: January 2024 Court Documents (~23MB, 943 pages)
# Good for testing and smaller deployments
./epstein download --dataset court-2024

# Option B: DOJ December 2025 Release (~4.8GB, 8 datasets)
./epstein download --dataset doj-complete

# Option C: DOJ January 2026 Release (~104GB, datasets 10-12)
# These are ZIP archives that are automatically extracted after download
./epstein download --dataset doj-jan2026

# Option D: Everything
./epstein download --dataset all
```

### 4. Index and Load

```bash
# Process PDFs and load into Antfly
./epstein sync --create-table

# Or do it in steps:
./epstein prepare                    # Extract text from PDFs
./epstein load --create-table        # Load into Antfly
```

### 5. Start Search Interface

```bash
./epstein serve
```

Open http://localhost:3000 in your browser.

## Commands

### `download`

Downloads documents from Internet Archive.

```bash
./epstein download [flags]

Flags:
  --output    Output directory (default: ./epstein-docs)
  --dataset   Dataset to download: court-2024, doj-complete, doj-jan2026, all
```

### `prepare`

Processes PDF files and creates JSON data for loading.

```bash
./epstein prepare [flags]

Flags:
  --dir       Path to PDF directory (default: ./epstein-docs)
  --output    Output JSON file (default: epstein-docs.json)
  --base-url  Base URL for document links
```

### `load`

Loads prepared JSON data into Antfly.

```bash
./epstein load [flags]

Flags:
  --url             Antfly API URL (default: http://localhost:8080/api/v1)
  --table           Table name (default: epstein_docs)
  --input           Input JSON file (default: epstein-docs.json)
  --create-table    Create table if it doesn't exist
  --dry-run         Preview changes without applying
  --num-shards      Number of shards (default: 1)
  --batch-size      Batch size for linear merge (default: 25)
  --embedding-model Embedding model (default: embeddinggemma)
  --chunker-model   Chunker model (default: fixed-bert-tokenizer)
  --target-tokens   Target tokens per chunk (default: 512)
  --overlap-tokens  Overlap between chunks (default: 50)
```

### `sync`

Full pipeline: process PDFs and load directly.

```bash
./epstein sync [flags]

# Combines prepare + load flags
```

### `serve`

Starts a web server with search interface.

```bash
./epstein serve [flags]

Flags:
  --url     Antfly API URL (default: http://localhost:8080/api/v1)
  --table   Table name to search (default: epstein_docs)
  --listen  Listen address (default: :3000)
```

## Architecture

### Document Processing

1. **PDF Extraction**: Uses `ledongthuc/pdf` to extract text page-by-page
2. **Document Sections**: Each page becomes a `DocumentSection` with:
   - Unique ID (hash of file path + page number)
   - Title (document title + page number)
   - Content (extracted text)
   - Metadata (page number, total pages, PDF metadata)

### Indexing

Documents are indexed with:

1. **BM25 Full-Text Index** (automatic)
   - Keyword search
   - Exact phrase matching

2. **Embedding Index** (aknn_v0)
   - Semantic similarity search
   - Powered by Termite + embedding model
   - Chunked with configurable overlap

### Search

The web interface performs hybrid search:
- Queries both `full_text_index` and `embeddings` indexes
- Results are ranked by combined relevance score
- Supports natural language queries

## Datasets

### January 2024 Court Unsealing

- **Source**: [Internet Archive](https://archive.org/details/final-epstein-documents)
- **Size**: ~23MB (PDF), 943 pages
- **Content**: Unsealed documents from Giuffre v. Maxwell civil case
- **Released**: January 3, 2024

### DOJ Complete Release

- **Source**: [Internet Archive](https://archive.org/details/combined-all-epstein-files)
- **Size**: ~4.8GB (8 consolidated PDFs)
- **Content**: 4,055+ documents released under EFTA
- **Released**: December 19, 2025
- **Datasets**:
  - DataSet 1: 1.2GB
  - DataSet 2: 629MB
  - DataSet 3: 598MB
  - DataSet 4: 356MB
  - DataSet 5: 61MB
  - DataSet 6: 53MB
  - DataSet 7: 98MB
  - DataSet 8: 1.8GB

## Performance

Expected processing times (approximate):

| Dataset | Size | Prepare | Load | Embeddings |
|---------|------|---------|------|------------|
| court-2024 | ~23MB | 30s | 2min | 20-30min |
| doj-complete | ~4.8GB | 10min | 30min | 4-8hrs |

Embedding generation is the slowest step as each chunk needs to be processed by the ML model.

## API Usage

You can also query the Antfly API directly:

```bash
# Search via curl
curl "http://localhost:8080/api/v1/tables/epstein_docs/query" \
  -H "Content-Type: application/json" \
  -d '{
    "semantic_search": "flight logs to Little St James",
    "indexes": ["full_text_index", "embeddings"],
    "limit": 10
  }'
```

Or use the Go SDK:

```go
client, _ := antfly.NewAntflyClient("http://localhost:8080/api/v1", http.DefaultClient)

resp, _ := client.Query(ctx, "epstein_docs", antfly.QueryRequest{
    SemanticSearch: "flight logs",
    Indexes:        []string{"full_text_index", "embeddings"},
    Limit:          10,
})

for _, hit := range resp.Hits {
    fmt.Printf("Score: %.2f - %s\n", hit.Score, hit.Document["title"])
}
```

## Troubleshooting

### "No PDF files found"

Make sure you've run `download` first and the PDFs are in the expected directory.

### "Failed to create table (may already exist)"

The table already exists. This is fine - the sync will update existing documents.

### Slow embedding generation

Embedding generation runs asynchronously. You can monitor progress:

```bash
curl "http://localhost:8080/api/v1/tables/epstein_docs/indexes/embeddings" | jq '.status.total_indexed'
```

### Out of memory

For large datasets, increase the Go garbage collector threshold:

```bash
GOGC=50 ./epstein sync --create-table
```

Or use multiple shards:

```bash
./epstein sync --create-table --num-shards 4
```

## Legal Notice

These documents are publicly available through official government channels and public archives. This tool is provided for research, journalism, and educational purposes. The creators of this tool do not endorse or condone any illegal activity.

## References

- [DOJ Epstein Library](https://www.justice.gov/epstein)
- [Internet Archive - Epstein Documents](https://archive.org/details/combined-all-epstein-files)
- [PDF Association Analysis](https://pdfa.org/a-case-study-in-pdf-forensics-the-epstein-pdfs/)
