# Antfly Examples

Complete, runnable programs built on Antfly. Each example has its own README
with prerequisites and run commands. Go examples are separate modules: build and
test them from inside their directory with `GOWORK=off GOEXPERIMENT=simd go`.

Web application examples live in [`ts/examples`](../ts/examples/).

## Ingestion and sync

| Example | Language | What it shows |
|---|---|---|
| [antfly-docs-knowledge-base](antfly-docs-knowledge-base/) | Go | Ingest Antfly's docs with docsaf into Lite or a server; chunking, embeddings, a GLiNER knowledge graph, hybrid search, graph traversal, and backups |
| [postgres-sync](postgres-sync/) | Go | Real-time Postgres JSONB sync with LISTEN/NOTIFY and the Linear Merge API |
| [pinecone-migration](pinecone-migration/) | Python | Move existing vectors from Pinecone into Antfly |

To sync documents from Git, S3, Google Drive, or the web without writing code,
use the [docsaf CLI](../go/pkg/docsaf/README.md).

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
| [antfly-docs-knowledge-base](antfly-docs-knowledge-base/) | Go | The same knowledge base on an embedded `.aflite` file, plus a model-free `smoke` command for the embedded API |

The Lite examples need `libantfly`: run `zig build capi` in `zig/` first. The
[Lite guide](../docs/guides/lite.mdx) has a minimal Go program.

## Agent memory

For the `/memory/v1` native memory API, follow
[Give an Agent Long-Term Memory](../docs/guides/agent-memory.mdx), which runs
over HTTP and embedded. [Codex on Antfly](https://github.com/antflydb/codex/blob/antfly/integration/ANTFLY_INTEGRATION.md)
is the full integration: a fork of the Codex CLI that keeps its knowledge,
decisions, and session history in Antfly.

## Testing

```bash
# Requires libantfly (zig build capi)
(cd examples/antfly-docs-knowledge-base && GOWORK=off GOEXPERIMENT=simd go test ./...)

# Requires Postgres
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/postgres"
(cd examples/postgres-sync && GOWORK=off GOEXPERIMENT=simd go test ./...)
```

## Adding an example

Create `examples/<name>/` with a README, the program, and tests, and add it to
the tables above. Examples teach one use of Antfly; put dated benchmark or
qualification records under the matching `docs/**/history/` directory instead.
