# antfly-docs-knowledge-base

A searchable knowledge base over Antfly's own documentation. It walks the docs
with [docsaf](../../go/pkg/docsaf/), writes one document per markdown section,
and lets Antfly do the rest:

- **Chunking and embedding.** Each section is split into ~400-token chunks with
  40 tokens of overlap and embedded with `Qwen/Qwen3-Embedding-0.6B-GGUF`.
- **Knowledge graph.** `fastino/gliner2.5-base-v1` extracts entities
  (`component`, `subsystem`, `file`, `decision`, `model`, `protocol`, and
  others) and relations (`depends_on`, `owns`, `implements`, `supersedes`,
  `tested_by`, `documented_in`) into a graph index named `knowledge`. A
  resolver gives every mention a canonical `entity/<slug>` key, so mentions of
  one name in different sections become one node, and each section gets
  `mentions` edges to the entities it references.
- **Hybrid search.** Full-text search merged by reciprocal rank fusion with
  semantic search over the chunks, then a two-hop walk of the knowledge graph
  from the top hits.

It runs on embedded Antfly Lite (a single `.aflite` file) by default, or
against a running Antfly server with `-url` or `ANTFLY_URL`.

The [knowledge graph guide](../../docs/guides/knowledge-graph.mdx) walks
through the same index configuration with `curl` against a server.

Sections are tagged by path: `guide` (docs/guides and top-level docs pages),
`reference`, `design` (subsystem docs), `plan` (docs/plans), and `work-log`
(dated history).

## Prerequisites

1. Build Antfly and `libantfly` from the repository's `zig/` directory:

   ```sh
   cd zig && zig build
   ```

   The Go bindings find `libantfly` through pkg-config; see
   [go/pkg/embedded](../../go/pkg/embedded/README.md).

2. Pull the models:

   ```sh
   antfly inference pull Qwen/Qwen3-Embedding-0.6B-GGUF
   antfly inference pull fastino/gliner2.5-base-v1
   ```

3. Lite runs inference in-process. Model execution happens in a sandboxed
   worker process, found through `ANTFLY_INFERENCE_WORKER`, an `antfly` binary
   next to `libantfly`, or `antfly` on `PATH`. From a source tree:

   ```sh
   export ANTFLY_INFERENCE_WORKER=$PWD/../../zig/zig-out/bin/antfly
   ```

   To use a separate inference server instead, start `antfly inference run`
   and pass `-inference-url http://127.0.0.1:8090` to `ingest`, `query`, and
   `status`.

## Run on Antfly Lite

```sh
cd examples/antfly-docs-knowledge-base
GOWORK=off go run . ingest -repo ../.. -reset
GOWORK=off go run . query "how does Raft elect a leader"
GOWORK=off go run . entity raft
GOWORK=off go run . status
GOWORK=off go run . backup -out kb.afb
```

- `ingest` creates `kb.aflite` (or reopens it), adds the indexes, writes the
  sections, and waits for chunking, embedding, and extraction to finish.
  Re-running against an existing file checks that the index settings still
  match the flags and fails with a `-reset` hint if they don't.
- `query` runs the hybrid search and prints the hits, then the entities and
  edges reached from them.
- `entity` prints the edges of one entity node, for example `entity raft`.
- `status` prints the inference mode, indexes, enrichments, and pending work.
- `backup` writes a portable `.afb` backup that a server can restore, and
  checks the `.aflite` file.

`-db` selects the database file for every command.

## Run against a server

```sh
export ANTFLY_URL=http://localhost:8080/db/v1
GOWORK=off go run . ingest -repo ../.. -reset
GOWORK=off go run . query "how does Raft elect a leader"
```

Server mode creates an `antfly_docs` table (`-table` to rename it) and an
`entities` table for the resolver, with the same schema and index configs as
Lite, and writes the sections through the batch API. The server builds
embeddings and the graph in the background. Set `ANTFLY_TOKEN` if the server
requires authentication. Explore the graph with `graph_queries`
([graph navigation guide](../../docs/guides/graph-navigation.mdx)) or in
Antfarm; the `entity` command and the post-search graph walk use Lite's
embedded graph API.

## Smoke check without models

```sh
GOWORK=off go run . smoke -reset
```

`smoke` exercises the embedded API without any model: it writes two notes with
caller-supplied vectors, runs full-text, dense, and hybrid search, reads a
document back, and exports a backup. The `zig build lite-go-example` step runs
it against a freshly built `libantfly`.

## Other sources

The example reads the docs from a local checkout with docsaf's
`FilesystemSource`. docsaf can also read Git repositories, websites, S3, and
Google Drive; see the [docsaf package](../../go/pkg/docsaf/README.md). To sync
any of those into a server table without writing code, use the docsaf CLI:

```sh
(cd ../../go/pkg/docsaf && go build -o docsaf ./cmd/docsaf)
../../go/pkg/docsaf/docsaf sync --dir ./docs --base-url s3://docs-bucket --table docs --create-table
../../go/pkg/docsaf/docsaf sync --source google-drive --drive-folder <folder-url> --inline-content --table docs --create-table
```

`docsaf sync --help` lists the source, authentication, and index flags.

## Index configuration

Lite receives each index through `AddIndexJSON`; the server receives the same
configs in its create-table request. See `index_config.go`.

1. `chunk_vectors`, an `embeddings` index over the `doc_chunk_dense_v1`
   artifact (1024 dimensions, cosine). Two enrichments travel with it:
   `doc_chunks_v1` chunks each section body with the token-aware `fixed`
   chunker, and `doc_chunk_dense_v1` embeds those chunks. Semantic hits carry
   `hierarchy.parent_doc_key`, which `query` uses to map a chunk back to its
   section.
2. `knowledge`, a `graph` index fed by an `extractor` producer over each
   section body, with windowed long-document extraction, an `entities`
   resolver (`entity/{{ slug _entity.text }}`, minimum confidence 0.5),
   `mentions` edges, and a `pagerank` metric.
3. `full_text_index_v0`, the default full-text index every table gets.

## Files

- `main.go`: commands, flags, opening Lite, inference preflight checks, and
  index setup.
- `index_config.go`: the schema and index configs.
- `ingest.go`: the corpus walk and batched writes.
- `query.go`: hybrid search, graph traversal, and the `entity` command.
- `server.go`: server mode over the HTTP API.
- `smoke.go`: the model-free smoke check.
- `verify.go`: detects changed settings against an existing database.
- `smoke_test.go`, `server_test.go`: index acceptance against a real
  `libantfly`, drift detection, corpus classification, and server config
  shape.
