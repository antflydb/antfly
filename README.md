# Antfly

Antfly is a search-and-inference database written in Zig. One engine carries full-text (BM25), dense, sparse, and late-interaction vector indexes plus graph traversal over the same table, and the models that chunk, embed, rerank, transcribe, OCR, and extract run inside the process. Embeddings, chunks, entities, and graph edges are generated automatically as you write data, and built-in RAG agents tie it together. The same engine runs as a single `.aflite` file, a single node with a hot standby, a multi-Raft cluster, or serverless over object storage.

![Quickstart](https://cdn.antfly.io/quickstart.gif)

## Quick Start

```bash
# Install the CLI (macOS and Linux), then start a single node with built-in ML inference
curl -fsSL https://releases.antfly.io/antfly/latest/install.sh | sh
antfly standalone

# Or with Homebrew (macOS or Linux)
brew install antflydb/taps/antfly

# Or build from source
make build && ./antfly standalone

# Or run with Docker
docker run -p 127.0.0.1:8080:8080 ghcr.io/antflydb/antfly:latest standalone --host 0.0.0.0
```

These quickstarts run without authentication and are intended for local development. Keep Docker ports bound to loopback. Before allowing remote access, [enable authentication and provision a unique admin password](docs/auth.md#secure-deployment), then configure TLS at a reverse proxy.

That gives you the [Antfarm dashboard](ts/apps/antfarm) at `http://localhost:8080` — playgrounds for search, RAG, knowledge graphs, embeddings, reranking, and more.

See the [quickstart guide](https://antfly.io/docs/guides/quickstart) for a full walkthrough.

### Lite, inference, or a database server

| Product | How it runs | License |
|---------|-------------|---------|
| [Antfly Lite](docs/guides/lite.mdx) | An embedded `.aflite` database through `libantfly`, Zig, Go, Python, Rust, TypeScript, or browser WASM; local file commands through `antfly-lite lite` | [Apache 2.0](LICENSES/Apache-2.0.txt) |
| [Antfly inference](zig/pkg/inference) | In-process inference, browser WASM (wasm32/wasm64), or the independent `antfly-inference` executable and inference APIs | [Apache 2.0](LICENSES/Apache-2.0.txt) |
| Antfly database server | `antfly standalone`, distributed clusters, or serverless deployment; database HTTP APIs and the dashboard | [ELv2](LICENSES/Elastic-2.0.txt) |

The Apache `antfly-embedded` release archive bundles `antfly-lite`,
`antfly-inference`, `libantfly`, its C header, and the private inference worker,
with runtime files and license notices. Python/npm embedded packages use the same
library without installing public commands. Go/Rust consumers
link the library from the archive or a local build using its relocatable
`lib/pkgconfig/libantfly.pc` metadata.

Build the Apache Lite CLI and native library from source:

```bash
cd zig
zig build lite -j1
./zig-out/bin/antfly-lite lite init app.aflite
```

The Lite build installs its CLI, `libantfly`, and the C header. To serve that file over HTTP, use the full database server executable:

```bash
antfly standalone --storage-engine lite --storage-path app.aflite
```

`lite serve` has been removed. The standalone server uses the same Apache engine under ELv2. See [LICENSING.md](LICENSING.md) for the source boundaries and third-party licenses.

## Features

- **Hybrid search** — full-text (BM25), dense vectors ([RaBitQ](https://arxiv.org/abs/2405.12497)-compressed with [SPFresh](https://arxiv.org/abs/2410.14452)-style updates), sparse vectors ([SPLADE](https://arxiv.org/abs/2107.05720)), and [late interaction](https://arxiv.org/abs/2004.12832) (ColQwen2), fused with [reciprocal rank](https://cormack.uwaterloo.ca/cormacksigir09-rrf.pdf) or relative score fusion in one query
- **Full-text search** — Lucene-style segments with [highlighting](zig/pkg/antfly-embedded/src/local/search/highlight.zig), geo, regex, wildcard, and fuzzy queries, plus English and ten [Snowball](https://snowballstem.org/) stemmer languages
- **RAG agents** — built-in [retrieval-augmented generation](zig/pkg/antfly/src/api/retrieval_agent.zig) with streaming, multi-turn chat, tool calling (graph traversal, plus web search through an Exa connection), confidence scoring, and [TOON](docs/reference/toon-format.md) document rendering to cut prompt tokens
- **Query-builder agent** — turns a natural-language question into a structured Antfly query, with [evaluation metrics](go/pkg/evalaf) to measure retrieval quality
- **Graph indexes** — automatic relationship extraction and [graph traversal](zig/pkg/antfly-embedded/src/local/graph) over your data
- **Multimodal** — index and search [images, audio, and video](docs/guides/multimodal.mdx) with CLIP, CLAP, and vision-language models
- **Reranking** — cross-encoder reranking with score-based pruning to cut the noise
- **Aggregations** — stats, terms facets, histogram, date histogram, range, and geo-distance [aggregations](zig/pkg/antfly-embedded/src/local/search/aggregation.zig) for analytics
- **Transactions** — ACID transactions at the shard level with distributed coordination
- **Document TTL** — automatic [document expiration](docs/guides/ttl-example.md) so you don't have to clean up yourself
- **PostgreSQL CDC** — [mirror a Postgres table](docs/guides/cdc-replication.mdx) into Antfly over logical replication, every insert, update, and delete included
- **CLI** — one `antfly` binary for the server, tables, queries, backups, auth, and the [model registry](docs/guides/inference.mdx) (`antfly inference pull owner/model`)
- **Secrets** — reference credentials as [`${secret:...}` keystore entries or env vars](docs/reference/secrets.md) instead of putting them in config
- **Object storage** — the [serverless engine](docs/reference/s3-storage.md) keeps artifacts, manifests, WAL, and catalog state in S3-compatible storage (S3, MinIO, R2) or Google Cloud Storage
- **CPU, Metal, and CUDA** — native kernels for [inference](zig/pkg/inference) and vector search: SIMD on x86 and ARM, Metal on Apple silicon, and [CUDA](zig/pkg/inference/CUDA.md) with a kernel JIT
- **Distributed** — multi-Raft consensus, key-range sharding and replication, online shard splits (automatic size-based split and merge is opt-in), cross-shard transactions, horizontal scaling
- **Runs anywhere** — [Antfly Lite](docs/guides/lite.mdx) as a single `.aflite` file, a single node with a [hot standby](zig/pkg/antfly/src/storage/hot_standby), a Raft cluster, or [serverless](zig/pkg/antfly/src/serverless) over object storage
- **Embeddable** — a [C API](zig/pkg/antfly-embedded/src/local/capi) (`libantfly`), [embedded bindings](docs/reference/sdks.mdx#embedded-bindings) for [Go](go/pkg/embedded), [Python](py/packages/embedded), [Rust](rs/crates/embedded), and [TypeScript](ts/packages/embedded), and an in-browser [WASM build](zig/pkg/antfly-embedded/WASM.md) so the engine runs in-process, in unit tests, or on the edge
- **Extensions** — run your own code inside the engine with the [Wasmtime extension runtime](zig/pkg/antfly/src/extensions)
- **Enrichment pipelines** — [configurable pipelines](zig/pkg/antfly-embedded/src/local/storage/db/enrichment) per index for embeddings, summaries, graph edges, and custom computed fields
- **Bring your own models** — Ollama, OpenAI, OpenRouter, Cohere, Bedrock, Gemini, Vertex AI, or run models locally with Antfly inference (GGUF, safetensors, and ONNX)
- **Fine-tuning** — [LoRA, QLoRA, SFT, DPO, GRPO and more](zig/pkg/inference/src/finetune) with recipes for Gemma 4, GLiNER2, ColQwen2, LayoutLMv3, rerankers, and chunkers
- **Auth** — built-in [user management](zig/pkg/antfly/src/usermgr) with API keys, basic auth, and bearer tokens
- **Backup & restore** — portable backups to local disk, S3, or Google Cloud Storage
- **Kubernetes operator** — deploy and manage clusters with the [operator](go/pkg/operator) ([docs](go/pkg/operator/docs))
- **MCP and A2A protocols** — [protocol adapters](zig/pkg/antfly/src/api/protocol_adapters.zig) let agents and LLMs use Antfly directly, and an [n8n guide](docs/guides/n8n.mdx) wires it into workflows
- **Relational tables** — `storage_mode: relational` tables with typed packed rows, `relational` indexes, row query and mutate APIs, databases, a system catalog, and tablespaces
- **Antfarm** — [web dashboard](ts/apps/antfarm) with playgrounds for search, RAG, chat, knowledge graphs, embeddings, reranking, chunking, extraction, OCR, transcription, and evals

### In progress

- **SQL and the Postgres wire protocol** — SQL lowered to native typed plans, a `psql`-compatible server, and lake tables over Iceberg and Parquet. Tracked in [#145](https://github.com/antflydb/antfly/pull/145)

## Documentation

[antfly.io/docs](https://antfly.io/docs), or the source under [`docs/`](docs). Good starting points:

- [Quickstart](docs/guides/quickstart.mdx), [Document Engine](docs/guides/document-engine.mdx), and [Hybrid Search](docs/guides/hybrid-search.mdx)
- [Multimodal](docs/guides/multimodal.mdx), [Artifact Indexes](docs/guides/artifact-indexes.mdx), and [Inference](docs/guides/inference.mdx) with the [supported models](docs/guides/supported-models.mdx)
- [Documentation index](docs/README.md)
- [Antfly Lite](docs/guides/lite.mdx), [Architecture](docs/architecture.mdx), and [Object storage](docs/reference/s3-storage.md)
- End-to-end guides: [support answer agent](docs/guides/support-answer-agent.mdx), [site search and answers](docs/guides/site-search-and-answers.mdx), [ticket routing](docs/guides/ticket-routing.mdx), [coding copilot retrieval](docs/guides/coding-copilot-retrieval.mdx)
- Runnable [examples](examples): Lite in Go, image search, memoryaf, Pinecone migration, Postgres sync

Prefer not to run it yourself? [Antfly Cloud](https://antfly.io/cloud) is the hosted option.

## SDKs & Client Libraries

| Language | Package | Source |
|----------|---------|--------|
| Go | `github.com/antflydb/antfly/go/pkg/sdk` | [`go/pkg/sdk`](go/pkg/sdk) |
| TypeScript | `@antfly/sdk` | [`ts/packages/sdk`](ts/packages/sdk) |
| Python | `antfly-sdk` (import `antfly`) | [`py/packages/sdk`](py/packages/sdk) |
| Rust | `antfly-sdk` | [`rs/crates/sdk`](rs/crates/sdk) |
| React | `@antfly/components` | [`ts/packages/components`](ts/packages/components) |
| PostgreSQL | `antfly-postgres` (SQL: `antfly_postgres`) | [`rs/crates/postgres`](rs/crates/postgres) |

### antfly-postgres — PostgreSQL Extension

[antfly-postgres](rs/crates/postgres) brings Antfly search into Postgres. The Cargo package is `antfly-postgres`; the PostgreSQL extension and query-builder schema are `antfly_postgres`. Enable the extension, create an index, and use the `@@@` operator:

```sql
CREATE EXTENSION antfly_postgres;

CREATE INDEX idx_content ON docs USING antfly (content)
  WITH (url = 'http://localhost:8080/db/v1/', collection = 'my_docs');

SELECT * FROM docs WHERE content @@@ 'fix my computer';
```

### React Components

[`@antfly/components`](ts/packages/components) gives you drop-in React components for search UIs — `QueryBox`, `Autosuggest`, `Facet`, `ActiveFilters`, `Results`, `Pagination`, `AnswerResults`, `AnswerFeedback`, `ChatBar`, and `ChatMessages`, plus streaming hooks like `useAnswerStream`, `useChatStream`, `useCitations`, and `useSearchHistory`.

### Inference Runtime

Antfly inference handles the ML side: embeddings, chunking, reranking, classification, NER, OCR, transcription, generation, and more. The Apache runtime can run in-process or independently as [`antfly-inference`](zig/pkg/inference). The full database server also exposes `antfly inference` commands and includes inference in standalone mode.

## Libraries & Tools

| Package | What it does | Source |
|---------|--------------|--------|
| docsaf | Ingest content from the filesystem, web crawls and sitemaps, git repos, S3, and Google Drive | [`go/pkg/docsaf`](go/pkg/docsaf) |
| evalaf | LLM/RAG/agent evaluation ("promptfoo for Go") | [`go/pkg/evalaf`](go/pkg/evalaf) |
| Genkit plugin | Firebase Genkit integration for retrieval and docstore | [`go/pkg/genkit/antfly`](go/pkg/genkit/antfly) |
| memoryaf | Shared long-term memory for AI agents over MCP and HTTP | [`go/pkg/memoryaf`](go/pkg/memoryaf) |
| embedded | Go, Python, Rust, and TypeScript bindings for local databases and inference over the C ABI | [`go/pkg/embedded`](go/pkg/embedded), [`py/packages/embedded`](py/packages/embedded), [`rs/crates/embedded`](rs/crates/embedded), [`ts/packages/embedded`](ts/packages/embedded) |

## Architecture

The distributed database server uses a multi-[Raft](https://raft.github.io/raft.pdf) design with separate consensus groups:

- **Metadata raft** — table schemas, shard assignments, cluster topology
- **Storage rafts** — one per shard, handling data, indexes, and queries

The runtime includes our own [Raft](zig/pkg/antfly/src/raft), [LSM](zig/pkg/antfly-embedded/src/local/storage/lsm), WAL, [full-text search](zig/pkg/antfly-embedded/src/local/search), HTTP/2 and HTTP/3, and inference implementations. The repository also maintains a standalone Apache-licensed [Zig LMDB-compatible B+tree](zig/lib/lmdb/src) and a vendored [C LMDB oracle](zig/deps/lmdb) for compatibility testing; neither is a production storage backend. Bundled and adapted third-party code retains its original licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Our [Snowball fork](zig/deps/snowball) generates the checked-in stemmer tables. The engine runs under a deterministic [VOPR](zig/pkg/antfly/src/vopr) simulation harness that injects storage, network, concurrency, and clock faults, in the style of [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md).

End-to-end [chaos tests](zig/e2e/antfly) — inspired by [Jepsen](https://jepsen.io/) — cover node crashes, leader failures, shard splits under load, and cluster scaling. These tests run real multi-node clusters and inject faults to verify that Raft consensus, transactions, and replication behave correctly under failure.

Critical distributed protocols are formally specified, model-checked, and trace-validated with [TLA+](https://lamport.azurewebsites.net/tla/tla.html) under [`zig/specs/tla`](zig/specs/tla):

- [AntflyTransaction](zig/specs/tla/AntflyTransaction.tla) — distributed transaction protocol
- [occ-2pc](zig/specs/tla/occ-2pc.tla) — optimistic concurrency control with two-phase commit
- [AntflySnapshotTransfer](zig/specs/tla/AntflySnapshotTransfer.tla) — Raft snapshot transfer
- [AntflyShardSplit](zig/specs/tla/AntflyShardSplit.tla) — shard split coordination
- [AntflyLsmLifecycle](zig/specs/tla/AntflyLsmLifecycle.tla) — LSM ownership handoff under allocation failure
- [etcdraft](zig/specs/tla/etcdraft.tla) — Raft, trace-validated against the Zig implementation

## Community

Join the [Discord](https://discord.gg/zrdjguy84P) for support, discussion, and updates.

Interested in contributing? See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

The shared embedded engine, Antfly Lite CLI, native `libantfly`, [Zig embedding package](zig/pkg/antfly-embedded), embedded bindings, SDKs, and inference runtime and executable are [Apache 2.0](LICENSES/Apache-2.0.txt).

The standalone database server, database HTTP serving, distributed control, and serverless orchestration are [Elastic License 2.0 (ELv2)](LICENSES/Elastic-2.0.txt). Third-party components retain their original licenses. See [LICENSING.md](LICENSING.md) for the exact scope and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for upstream notices.
