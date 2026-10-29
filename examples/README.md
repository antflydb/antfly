# Antfly Examples

Complete, runnable programs built on Antfly. Each example has its own README
with prerequisites and run commands. Go examples are separate modules: build and
test them from inside their directory with `GOWORK=off GOEXPERIMENT=simd go`.

Web application examples live in [`ts/examples`](../ts/examples/).

## Ingestion and sync

| Example | Language | What it shows |
|---|---|---|
| [docsaf](docsaf/) | Go | Sync documents from local files, Git, S3, Google Drive, and the web; Antfly does extraction, chunking, and embedding |
| [postgres-sync](postgres-sync/) | Go | Real-time Postgres JSONB sync with LISTEN/NOTIFY and the Linear Merge API |
| [pinecone-migration](pinecone-migration/) | Python | Move existing vectors from Pinecone into Antfly |

For change data capture without a daemon, see
[Stream PostgreSQL into Antfly](../docs/guides/cdc-replication.mdx), which uses
logical replication managed by Antfly.

## Search applications

| Example | Language | What it shows |
|---|---|---|
| [image-search](image-search/) | Go | CLIP image search by text over sample images, or over your own screenshots, images, and PDFs with `-dir` |
| [grepstein](grepstein/) | Go | Public-records showcase: PDF reading and OCR, full-text, semantic, and graph enrichment, and grep over a GCS-backed corpus on Antfly Cloud |
| [hackernews](hackernews/) | Python | Historical Hacker News search over GCS Parquet with Antfly index artifacts, including deployment tooling; qualification records are in [`docs/operations/hackernews/history`](../docs/operations/hackernews/history/) |

## Embedded (Antfly Lite)

| Example | Language | What it shows |
|---|---|---|
| [antfly-lite-go](antfly-lite-go/) | Go | Open a `.aflite` database in-process, read and write documents, and export a portable `.afb` backup |
| [antfly-lite-retrieval-go](antfly-lite-retrieval-go/) | Go | Local retrieval template: schema, full-text, dense, and hybrid search with caller-supplied embeddings |
| [dogfood](dogfood/) | Go | Ingest Antfly's own docs into Lite and build a knowledge graph with GLiNER extraction |
| [native-memory](native-memory/) | Python | The `/memory/v1` evidence and temporal policy over HTTP or the embedded SDK, with no model |

The Lite examples need `libantfly`: run `zig build capi` in `zig/` first.

## Testing

```bash
(cd examples/docsaf && GOWORK=off GOEXPERIMENT=simd go test ./...)

# Requires Postgres
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/postgres"
(cd examples/postgres-sync && GOWORK=off GOEXPERIMENT=simd go test ./...)
```

## Adding an example

Create `examples/<name>/` with a README, the program, and tests, and add it to
the tables above. Examples teach one use of Antfly; put dated benchmark or
qualification records under the matching `docs/**/history/` directory instead.
