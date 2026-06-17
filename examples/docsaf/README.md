# docsaf - Source Document Sync to Antfly

`docsaf` syncs source document rows into Antfly. It does not locally split files
into section rows, chunks, vectors, or graph evidence. Antfly owns extraction and
derived artifact lifecycle from the source row.

## Model

`docsaf prepare` and `docsaf sync` emit rows shaped like:

```json
{
  "id": "guide.md",
  "url": "s3://docs-bucket/guide.md",
  "filename": "guide.md",
  "mime_type": "text/markdown",
  "sha256": "...",
  "source_path": "guide.md",
  "_type": "source_document"
}
```

When `--create-table` is used, the example creates:

- `document_units`: a graph index backed by the `document_units_v1` extraction
  artifact.
- `document_text`: a full-text index over `document_chunks_v1`.
- `document_vectors`: a managed vector index over `document_chunk_dense_v1`,
  generated from `document_chunks_v1`.

PDFs and slide decks are partitioned by Antfly's `document_extraction` producer
before chunking. PDFs produce page units; slide decks produce slide units; chunks
are derived from those units.

## Build

From the repository root:

```bash
(cd go/pkg/docsaf && go build -o ../../../examples/docsaf/docsaf ./cmd/docsaf)
```

## Commands

Prepare source rows into JSON:

```bash
./docsaf prepare \
  --dir ./docs \
  --base-url s3://docs-bucket \
  --output docs.json
```

Load prepared rows:

```bash
./docsaf load \
  --input docs.json \
  --table docs \
  --create-table
```

Traverse and load in one command:

```bash
./docsaf sync \
  --dir ./docs \
  --base-url s3://docs-bucket \
  --table docs \
  --create-table
```

For local smoke tests, inline file bytes as `data:` URLs:

```bash
./docsaf sync \
  --dir ./docs \
  --inline-content \
  --table docs \
  --create-table
```

## Flags

Source flags:

- `--dir`: directory containing source documents.
- `--base-url`: fetchable URL prefix for source documents.
- `--inline-content`: encode source bytes into `data:` URLs for local smoke
  tests.
- `--id-prefix`: optional stable prefix for source document IDs.
- `--include`: include pattern; repeatable and supports `**`.
- `--exclude`: exclude pattern; repeatable and supports `**`.

Load/sync flags:

- `--url`: Antfly API URL, default `http://localhost:8080/db/v1`.
- `--table`: table name, default `docs`.
- `--create-table`: create the table with derived hierarchy indexes.
- `--num-shards`: number of shards for a new table.
- `--batch-size`: linear merge batch size.
- `--chunk-size`: target size for unit-derived chunks.
- `--chunk-overlap`: overlap for unit-derived chunks.
- `--embedding-model`: Ollama embedding model for managed vector search.
- `--embedding-dims`: expected embedding dimension; `0` lets Antfly probe.
- `--dry-run`: preview linear merge changes without applying them.

## Notes

Production sync should use URLs Antfly can fetch directly, such as S3 or HTTPS.
Inline content is useful for small local tests only.

The source-row design is documented in
[`go/pkg/docsaf/DOCSAF.md`](../../go/pkg/docsaf/DOCSAF.md).
