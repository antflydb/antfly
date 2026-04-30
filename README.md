# Antfly

Antfly is a distributed search engine built on [etcd's raft library](https://github.com/etcd-io/raft). It combines full-text search (BM25), vector similarity, and graph traversal over multimodal data — text, images, audio, and video. Embeddings, chunking, and graph edges are generated automatically as you write data. Built-in RAG agents tie it all together with retrieval-augmented generation.

![Quickstart](https://cdn.antfly.io/quickstart.gif)

## Quick Start

```bash
# Start a single-node cluster with built-in ML inference
go run ./cmd/antfly swarm

# Or run with Docker
docker run -p 8080:8080 ghcr.io/antflydb/antfly:omni
```

That gives you the [Antfarm dashboard](ts/apps/antfarm) at `http://localhost:8080` — playgrounds for search, RAG, knowledge graphs, embeddings, reranking, and more.

See the [quickstart guide](https://antfly.io/docs/guides/quickstart) for a full walkthrough.

## Features

- **Hybrid search** — full-text (BM25), dense vectors, and [sparse vectors (SPLADE)](https://huggingface.co/naver/splade-cocondenser-ensembledistil), all in one query
- **RAG agents** — built-in [retrieval-augmented generation](src/metadata/retrieval_agent.go) with streaming, multi-turn chat, tool calling (web search, graph traversal), and confidence scoring
- **Graph indexes** — automatic [relationship extraction](src/store/db/indexes/graph_index.go) and graph traversal queries over your data
- **Multimodal** — index and search [images, audio, and video](docs/guides/multimodal.mdx) with CLIP, CLAP, and vision-language models
- **Reranking** — cross-encoder reranking with score-based pruning to cut the noise
- **Aggregations** — stats (sum/min/max/avg) and terms facets for analytics
- **Transactions** — ACID transactions at the shard level with distributed coordination
- **Document TTL** — automatic [document expiration](docs/ttl-example.md) so you don't have to clean up yourself
- **S3 storage** — store data in [S3/MinIO/R2](docs/s3-storage.md) for big cost savings and way faster shard splits
- **SIMD / SME acceleration** — vector operations use hardware intrinsics via [go-highway](https://github.com/ajroetker/go-highway) on x86 and ARM
- **Distributed** — Raft consensus, automatic sharding and replication, horizontal scaling
- **Enrichment pipelines** — [configurable pipelines](src/store/db/indexes/walenricher.go) per index for embeddings, summaries, graph edges, and custom computed fields
- **Bring your own models** — Ollama, OpenAI, Bedrock, Google, or run models locally with [Termite](pkg/termite)
- **Auth** — built-in [user management](src/usermgr) with API keys, basic auth, and bearer tokens
- **Backup & restore** — to local disk or S3
- **Kubernetes operator** — deploy and manage clusters with the [operator](pkg/operator)
- **MCP server** — [Model Context Protocol](src/mcp) so LLMs can use Antfly as a tool
- **A2A protocol** — [Agent-to-Agent](src/a2a) support for Google's A2A standard
- **Antfarm** — [web dashboard](ts/apps/antfarm) with playgrounds for search, RAG, knowledge graphs, embeddings, reranking, chunking, NER, OCR, and transcription

## Documentation

[antfly.io/docs](https://antfly.io/docs)

## SDKs & Client Libraries

| Language | Package | Source |
|----------|---------|--------|
| Go | `github.com/antflydb/antfly/pkg/client` | [`pkg/client`](pkg/client) |
| TypeScript | `@antfly/sdk` | [`ts/packages/sdk`](ts/packages/sdk) |
| Python | `antfly` | [`py/`](py/) |
| React | `@antfly/components` | [`ts/packages/components`](ts/packages/components) |
| PostgreSQL | `pgaf` extension | [`rs/pgaf`](rs/pgaf) |

### pgaf — PostgreSQL Extension

[pgaf](rs/pgaf) brings Antfly search into Postgres. Create an index, use the `@@@` operator, and you're done:

```sql
CREATE INDEX idx_content ON docs USING antfly (content)
  WITH (url = 'http://localhost:8080/api/v1/', collection = 'my_docs');

SELECT * FROM docs WHERE content @@@ 'fix my computer';
```

### React Components

[`@antfly/components`](ts/packages/components) gives you drop-in React components for search UIs — `SearchBox`, `Autosuggest`, `Facet`, `Results`, `RAGBox`, `AnswerBox`, plus streaming hooks like `useAnswerStream` and `useCitations`.

### Termite — ML Inference

[Termite](pkg/termite) handles the ML side: embeddings, chunking, reranking, classification, NER, OCR, transcription, generation, and more. It ships as an in-tree module (`pkg/termite`) and runs automatically in swarm mode — you don't need to set it up separately.

## Libraries & Tools

| Package | What it does | Source |
|---------|--------------|--------|
| docsaf | Ingest content from filesystem, web crawl, git repos, and S3 | [`pkg/docsaf`](pkg/docsaf) |
| evalaf | LLM/RAG/agent evaluation ("promptfoo for Go") | [`pkg/evalaf`](pkg/evalaf) |
| Genkit plugin | Firebase Genkit integration for retrieval and docstore | [`pkg/genkit/antfly`](pkg/genkit/antfly) |

## Architecture

Antfly uses a multi-raft design with separate consensus groups:

- **Metadata raft** — table schemas, shard assignments, cluster topology
- **Storage rafts** — one per shard, handling data, indexes, and queries

End-to-end [chaos tests](e2e/) — inspired by [Jepsen](https://jepsen.io/) — cover node crashes, leader failures, shard splits under load, and cluster scaling. These tests run real multi-node clusters and inject faults to verify that Raft consensus, transactions, and replication behave correctly under failure.

Critical distributed protocols are formally specified and model-checked with [TLA+](specs/tla):

- [AntflyTransaction](specs/tla/AntflyTransaction.tla) — distributed transaction protocol
- [occ-2pc](specs/tla/occ-2pc.tla) — optimistic concurrency control with two-phase commit
- [AntflySnapshotTransfer](specs/tla/AntflySnapshotTransfer.tla) — Raft snapshot transfer
- [AntflyShardSplit](specs/tla/AntflyShardSplit.tla) — shard split coordination

## Community

Join the [Discord](https://discord.gg/zrdjguy84P) for support, discussion, and updates.

Interested in contributing? See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

The core server is [Elastic License 2.0 (ELv2)](LICENSE). That means you can use it, modify it, self-host it, and build products on top of it — you just can't offer Antfly itself as a managed service. Everything else — the [SDKs](pkg/client), [React components](ts/packages/components), [Termite](termite/), [pgaf](rs/pgaf), [docsaf](pkg/docsaf), [evalaf](pkg/evalaf) — is Apache 2.0. We tried to keep as much as possible under a permissive license.
