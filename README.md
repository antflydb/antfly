# Antfly

Antfly is a search-and-inference database written in Zig with zero dependencies. One engine carries full-text (BM25), dense, sparse, and late-interaction vector indexes plus graph traversal over the same table, and the models that chunk, embed, rerank, transcribe, OCR, and extract run inside the process. Embeddings, chunks, entities, and graph edges are generated automatically as you write data, and built-in RAG agents tie it together. The same engine runs as a single `.aflite` file, a single node with a hot standby, a multi-Raft cluster, or serverless over object storage.

![Quickstart](https://cdn.antfly.io/quickstart.gif)

## Quick Start

```bash
# Install the CLI (macOS and Linux), then start a single node with built-in ML inference
curl -fsSL https://releases.antfly.io/antfly/latest/install.sh | sh
antfly standalone

# Or with Homebrew
brew install antflydb/taps/antfly

# Or build from source
make build && ./antfly standalone

# Or run with Docker
docker run -p 8080:8080 ghcr.io/antflydb/antfly:latest
```

That gives you the [Antfarm dashboard](ts/apps/antfarm) at `http://localhost:8080` — playgrounds for search, RAG, knowledge graphs, embeddings, reranking, and more.

See the [quickstart guide](https://antfly.io/docs/guides/quickstart) for a full walkthrough.

## Features

- **Hybrid search** — full-text (BM25), dense vectors ([RaBitQ](https://arxiv.org/abs/2405.12497)-compressed with [SPFresh](https://arxiv.org/abs/2410.14452)-style updates), sparse vectors ([SPLADE](https://arxiv.org/abs/2107.05720)), and [late interaction](https://arxiv.org/abs/2004.12832) (ColQwen2), fused with [reciprocal rank](https://cormack.uwaterloo.ca/cormacksigir09-rrf.pdf) or relative score fusion in one query
- **Full-text search** — Lucene-style segments with [highlighting](zig/pkg/antfly/src/search/highlight.zig), geo, regex, wildcard, and fuzzy queries, plus English and ten [Snowball](https://snowballstem.org/) stemmer languages
- **RAG agents** — built-in [retrieval-augmented generation](zig/pkg/antfly/src/api/retrieval_agent.zig) with streaming, multi-turn chat, tool calling (graph traversal, plus web search through an Exa connection), confidence scoring, and [TOON](docs/toon-format.md) document rendering to cut prompt tokens
- **Query-builder agent** — turns a natural-language question into a structured Antfly query, with [evaluation metrics](go/pkg/evalaf) to measure retrieval quality
- **Graph indexes** — automatic relationship extraction and [graph traversal](zig/pkg/antfly/src/graph) over your data
- **Multimodal** — index and search [images, audio, and video](docs/guides/multimodal.mdx) with CLIP, CLAP, and vision-language models
- **Reranking** — cross-encoder reranking with score-based pruning to cut the noise
- **Aggregations** — stats, terms facets, histogram, date histogram, range, and geo-distance [aggregations](zig/pkg/antfly/src/search/aggregation.zig) for analytics
- **Transactions** — ACID transactions at the shard level with distributed coordination
- **Document TTL** — automatic [document expiration](docs/ttl-example.md) so you don't have to clean up yourself
- **PostgreSQL CDC** — [mirror a Postgres table](docs/guides/cdc-replication.mdx) into Antfly over logical replication, every insert, update, and delete included
- **CLI** — one `antfly` binary for the server, tables, queries, backups, auth, and the [model registry](docs/guides/inference.mdx) (`antfly inference pull owner/model`)
- **Secrets** — reference credentials as [`${secret:...}` keystore entries or env vars](docs/secrets.md) instead of putting them in config
- **S3 storage** — store data in [S3/MinIO/R2](docs/s3-storage.md) for big cost savings and way faster shard splits
- **CPU, Metal, and CUDA** — native kernels for [inference](zig/pkg/inference) and vector search: SIMD on x86 and ARM, Metal on Apple silicon, and [CUDA](zig/pkg/inference/CUDA.md) with a kernel JIT
- **Distributed** — multi-Raft consensus, automatic sharding and replication, online shard splits, cross-shard transactions, horizontal scaling
- **Runs anywhere** — [Antfly Lite](docs/guides/lite.mdx) as a single `.aflite` file, a single node with a [hot standby](zig/pkg/antfly/src/storage/ha), a Raft cluster, or [serverless](zig/pkg/antfly/src/serverless) over object storage
- **Embeddable** — a [C API](zig/pkg/antfly/src/capi) (`libantfly`), a [Go binding](go/pkg/antflylite), and an in-browser [WASM build](zig/pkg/antfly-embedded/WASM.md) so the engine runs in-process, in unit tests, or on the edge
- **Extensions** — run your own code inside the engine with the [Wasmtime extension runtime](zig/pkg/antfly/src/extensions)
- **Enrichment pipelines** — [configurable pipelines](zig/pkg/antfly/src/storage/db/enrichment) per index for embeddings, summaries, graph edges, and custom computed fields
- **Bring your own models** — Ollama, OpenAI, Bedrock, Google, or run models locally with Antfly inference (GGUF, safetensors, and ONNX)
- **Fine-tuning** — [LoRA, QLoRA, SFT, DPO, GRPO and more](zig/pkg/inference/src/finetune) with recipes for Gemma 4, GLiNER2, ColQwen2, LayoutLMv3, rerankers, and chunkers
- **Auth** — built-in [user management](zig/pkg/antfly/src/usermgr) with API keys, basic auth, and bearer tokens
- **Backup & restore** — to local disk or S3
- **Kubernetes operator** — deploy and manage clusters with the [operator](go/pkg/operator) ([docs](go/pkg/operator/docs))
- **MCP and A2A protocols** — [protocol adapters](zig/pkg/antfly/src/api/protocol_adapters.zig) let agents and LLMs use Antfly directly, and an [n8n guide](docs/guides/n8n.mdx) wires it into workflows
- **Antfarm** — [web dashboard](ts/apps/antfarm) with playgrounds for search, RAG, chat, knowledge graphs, embeddings, reranking, chunking, extraction, OCR, transcription, and evals

### In progress

- **Relational tables, SQL, and the Postgres wire protocol** — closed schemas with typed packed rows, SQL lowered to native typed plans, a `psql`-compatible server, and lake tables over Iceberg and Parquet. Tracked in [#502](https://github.com/antflydb/antfly/pull/502) (relational storage), [#691](https://github.com/antflydb/antfly/pull/691) (system catalog and tablespaces), and [#145](https://github.com/antflydb/antfly/pull/145) (SQL, pgwire, and lake query mode)

## Documentation

[antfly.io/docs](https://antfly.io/docs), or the source under [`docs/`](docs). Good starting points:

- [Quickstart](docs/guides/quickstart.mdx), [Document Engine](docs/guides/document-engine.mdx), and [Hybrid Search](docs/guides/hybrid-search.mdx)
- [Multimodal](docs/guides/multimodal.mdx), [Artifact Indexes](docs/guides/artifact-indexes.mdx), and [Inference](docs/guides/inference.mdx) with the [supported models](docs/guides/supported-models.mdx)
- [Antfly Lite](docs/guides/lite.mdx), [Architecture](docs/architecture.mdx), and [Object storage](docs/s3-storage.md)
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
| PostgreSQL | `pgaf` extension | [`rs/crates/pgaf`](rs/crates/pgaf) |

### pgaf — PostgreSQL Extension

[pgaf](rs/crates/pgaf) brings Antfly search into Postgres. Create an index, use the `@@@` operator, and you're done:

```sql
CREATE INDEX idx_content ON docs USING antfly (content)
  WITH (url = 'http://localhost:8080/db/v1/', collection = 'my_docs');

SELECT * FROM docs WHERE content @@@ 'fix my computer';
```

### React Components

[`@antfly/components`](ts/packages/components) gives you drop-in React components for search UIs — `QueryBox`, `Autosuggest`, `Facet`, `ActiveFilters`, `Results`, `Pagination`, `AnswerResults`, `AnswerFeedback`, `ChatBar`, and `ChatMessages`, plus streaming hooks like `useAnswerStream`, `useChatStream`, `useCitations`, and `useSearchHistory`.

### Inference Runtime

Antfly inference handles the ML side: embeddings, chunking, reranking, classification, NER, OCR, transcription, generation, and more. It runs under the `antfly inference` CLI and starts automatically in standalone mode, so you don't need to set it up separately.

## Libraries & Tools

| Package | What it does | Source |
|---------|--------------|--------|
| docsaf | Ingest content from the filesystem, web crawls and sitemaps, git repos, S3, and Google Drive | [`go/pkg/docsaf`](go/pkg/docsaf) |
| evalaf | LLM/RAG/agent evaluation ("promptfoo for Go") | [`go/pkg/evalaf`](go/pkg/evalaf) |
| Genkit plugin | Firebase Genkit integration for retrieval and docstore | [`go/pkg/genkit/antfly`](go/pkg/genkit/antfly) |
| memoryaf | Shared long-term memory for AI agents over MCP and HTTP | [`go/pkg/memoryaf`](go/pkg/memoryaf) |
| antflylite | Go binding for embedded `.aflite` databases over the C ABI | [`go/pkg/antflylite`](go/pkg/antflylite) |

## Architecture

Antfly uses a multi-[Raft](https://raft.github.io/raft.pdf) design with separate consensus groups:

- **Metadata raft** — table schemas, shard assignments, cluster topology
- **Storage rafts** — one per shard, handling data, indexes, and queries

Every dependency is our own: [Raft](zig/pkg/antfly/src/raft), the [LSM](zig/pkg/antfly/src/storage/lsm), an [LMDB-compatible B+tree](zig/pkg/antfly/src/lmdb), the WAL, [full-text search](zig/pkg/antfly/src/search), HTTP/2 and HTTP/3, and the inference runtime. The one vendored input is our [Snowball fork](zig/deps/snowball), used to generate the stemmer tables that are checked in. Because the engine owns the whole process, each of these runs under a deterministic [VOPR](zig/pkg/antfly/src/vopr) simulation harness that injects storage, network, concurrency, and clock faults, in the style of [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md).

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

The core server is [Elastic License 2.0 (ELv2)](LICENSE). That means you can use it, modify it, self-host it, and build products on top of it — you just can't offer Antfly itself as a managed service. The in-process bindings that link the core — [`antfly-embedded`](zig/pkg/antfly-embedded) and the [Go Lite binding](go/pkg/antflylite) — are ELv2 as well. Everything else — the [SDKs](go/pkg/sdk) for Go, TypeScript, Python, and Rust, [React components](ts/packages/components), the [inference runtime](zig/pkg/inference), [pgaf](rs/crates/pgaf), [docsaf](go/pkg/docsaf), [evalaf](go/pkg/evalaf) — is Apache 2.0. We tried to keep as much as possible under a permissive license.
