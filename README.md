# Antfly

Antfly is a distributed search engine built on [etcd's raft library](https://github.com/etcd-io/raft). It combines full-text search (BM25), vector similarity, and graph traversal over multimodal data — text, images, audio, and video. Embeddings, chunking, and graph edges are generated automatically as you write data. Built-in RAG agents tie it all together with retrieval-augmented generation.

![Quickstart](https://cdn.antfly.io/quickstart.gif)

## Quick Start

```bash
# Build and start a single-node cluster with built-in ML inference
make build
./antfly standalone

# Or run with Docker
docker run -p 8080:8080 ghcr.io/antflydb/antfly:latest
```

That gives you the [Antfarm dashboard](ts/apps/antfarm) at `http://localhost:8080` — playgrounds for search, RAG, knowledge graphs, embeddings, reranking, and more.

See the [quickstart guide](https://antfly.io/docs/guides/quickstart) for a full walkthrough.

## Features

- **Hybrid search** — full-text (BM25), dense vectors, and [sparse vectors (SPLADE)](https://huggingface.co/naver/splade-cocondenser-ensembledistil), all in one query
- **RAG agents** — built-in [retrieval-augmented generation](zig/pkg/antfly/src/api/retrieval_agent.zig) with streaming, multi-turn chat, tool calling (web search, graph traversal), and confidence scoring
- **Graph indexes** — automatic relationship extraction and [graph traversal](zig/pkg/antfly/src/graph) over your data
- **Multimodal** — index and search [images, audio, and video](docs/guides/multimodal.mdx) with CLIP, CLAP, and vision-language models
- **Reranking** — cross-encoder reranking with score-based pruning to cut the noise
- **Aggregations** — stats (sum/min/max/avg) and terms facets for analytics
- **Transactions** — ACID transactions at the shard level with distributed coordination
- **Document TTL** — automatic [document expiration](docs/ttl-example.md) so you don't have to clean up yourself
- **S3 storage** — store data in [S3/MinIO/R2](docs/s3-storage.md) for big cost savings and way faster shard splits
- **SIMD / SME acceleration** — vector operations use hardware intrinsics via [go-highway](https://github.com/ajroetker/go-highway) on x86 and ARM
- **Distributed** — Raft consensus, automatic sharding and replication, horizontal scaling
- **Enrichment pipelines** — [configurable pipelines](zig/pkg/antfly/src/storage/db/enrichment) per index for embeddings, summaries, graph edges, and custom computed fields
- **Bring your own models** — Ollama, OpenAI, Bedrock, Google, or run models locally with Antfly inference
- **Auth** — built-in [user management](zig/pkg/antfly/src/usermgr) with API keys, basic auth, and bearer tokens
- **Backup & restore** — to local disk or S3
- **Kubernetes operator** — deploy and manage clusters with the [operator](go/pkg/operator)
- **MCP and A2A protocols** — [protocol adapters](zig/pkg/antfly/src/api/protocol_adapters.zig) let agents and LLMs use Antfly directly
- **Antfarm** — [web dashboard](ts/apps/antfarm) with playgrounds for search, RAG, knowledge graphs, embeddings, reranking, chunking, NER, OCR, and transcription

## Documentation

[antfly.io/docs](https://antfly.io/docs)

## SDKs & Client Libraries

| Language | Package | Source |
|----------|---------|--------|
| Go | `github.com/antflydb/antfly/go/pkg/sdk` | [`go/pkg/sdk`](go/pkg/sdk) |
| TypeScript | `@antfly/sdk` | [`ts/packages/sdk`](ts/packages/sdk) |
| Python | `antfly` | [`py/packages/sdk`](py/packages/sdk) |
| React | `@antfly/components` | [`ts/packages/components`](ts/packages/components) |
| PostgreSQL | `pgaf` extension | [`rs/pgaf`](rs/pgaf) |

### pgaf — PostgreSQL Extension

[pgaf](rs/pgaf) brings Antfly search into Postgres. Create an index, use the `@@@` operator, and you're done:

```sql
CREATE INDEX idx_content ON docs USING antfly (content)
  WITH (url = 'http://localhost:8080/db/v1/', collection = 'my_docs');

SELECT * FROM docs WHERE content @@@ 'fix my computer';
```

### React Components

[`@antfly/components`](ts/packages/components) gives you drop-in React components for search UIs — `SearchBox`, `Autosuggest`, `Facet`, `Results`, `RAGBox`, `AnswerBox`, plus streaming hooks like `useAnswerStream` and `useCitations`.

### Inference Runtime

Antfly inference handles the ML side: embeddings, chunking, reranking, classification, NER, OCR, transcription, generation, and more. It runs under the `antfly inference` CLI and starts automatically in standalone mode, so you don't need to set it up separately.

## Libraries & Tools

| Package | What it does | Source |
|---------|--------------|--------|
| docsaf | Ingest content from filesystem, web crawl, git repos, and S3 | [`go/pkg/docsaf`](go/pkg/docsaf) |
| evalaf | LLM/RAG/agent evaluation ("promptfoo for Go") | [`go/pkg/evalaf`](go/pkg/evalaf) |
| Genkit plugin | Firebase Genkit integration for retrieval and docstore | [`go/pkg/genkit/antfly`](go/pkg/genkit/antfly) |

## Architecture

Antfly uses a multi-raft design with separate consensus groups:

- **Metadata raft** — table schemas, shard assignments, cluster topology
- **Storage rafts** — one per shard, handling data, indexes, and queries

End-to-end [chaos tests](zig/e2e/antfly) — inspired by [Jepsen](https://jepsen.io/) — cover node crashes, leader failures, shard splits under load, and cluster scaling. These tests run real multi-node clusters and inject faults to verify that Raft consensus, transactions, and replication behave correctly under failure.

Critical distributed protocols are formally specified, model-checked, and trace-validated with [TLA+](zig/specs/tla):

- [AntflyTransaction](zig/specs/tla/AntflyTransaction.tla) — distributed transaction protocol
- [occ-2pc](zig/specs/tla/occ-2pc.tla) — optimistic concurrency control with two-phase commit
- [AntflySnapshotTransfer](zig/specs/tla/AntflySnapshotTransfer.tla) — Raft snapshot transfer
- [AntflyShardSplit](zig/specs/tla/AntflyShardSplit.tla) — shard split coordination

## Community

Join the [Discord](https://discord.gg/zrdjguy84P) for support, discussion, and updates.

Interested in contributing? See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

The core server is [Elastic License 2.0 (ELv2)](LICENSE). That means you can use it, modify it, self-host it, and build products on top of it — you just can't offer Antfly itself as a managed service. Everything else — the [SDKs](go/pkg/sdk), [React components](ts/packages/components), inference runtime, [pgaf](rs/pgaf), [docsaf](go/pkg/docsaf), [evalaf](go/pkg/evalaf) — is Apache 2.0. We tried to keep as much as possible under a permissive license.
