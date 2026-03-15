# Antfly Work Log

This directory tracks major features and architectural changes in Antfly. Each document preserves design decisions and implementation context for future reference.

## Completed Features

### Core Infrastructure

| Feature | Document | Summary |
|---------|----------|---------|
| Metadata Port Consolidation | [metadata-architecture.md](completed/metadata-architecture.md) | 3 servers → 2, `/_internal/v1/` prefix |
| Indexes in Snapshots | [indexes-in-snapshots.md](completed/indexes-in-snapshots.md) | Pause/Resume interface for all index types during snapshots |
| Versioned Snapshots | [versioned-snapshots.md](completed/versioned-snapshots.md) | Compression auto-detection, format v3 with metadata |
| Remote Content Config v2 | [remote-content-configs-v2.md](completed/remote-content-configs-v2.md) | S3/HTTP credential resolution with per-bucket matching |

### Distributed Systems & Transactions

| Feature | Document | Summary |
|---------|----------|---------|
| Distributed Write Transactions | [distributed-write-transactions/](completed/distributed-write-transactions/) | 2PC with coordinator recovery, cross-shard atomic writes |
| Cross-Table Transactions & OCC RMW | [rmw-multi-table-transactions.md](completed/rmw-multi-table-transactions.md) | Version tokens, optimistic concurrency, 409 conflict |
| TLA+ Stuck-Pending Fix | [tla-stuck-pending-fix.md](completed/tla-stuck-pending-fix.md) | Formal model + auto-abort for stale pending transactions |
| E2E 2PC Tests | [e2e-2pc-test-plan.md](completed/e2e-2pc-test-plan.md) | Multi-node commit, abort, and recovery test suite |
| Online Shard Splits | [online-shard-splits/](completed/online-shard-splits/) | Initial shard splitting implementation |
| Zero-Downtime Shard Splits | [online-shard-splits-v2/](completed/online-shard-splits-v2/) | Shadow IndexManager, dual-write routing, archive inclusion |
| External Table Replication | [external-table-replication.md](completed/external-table-replication.md) | PostgreSQL logical replication (CDC) via pglogrepl |

### Search & Indexing

| Feature | Document | Summary |
|---------|----------|---------|
| Aggregations | [aggregations.md](completed/aggregations.md) | Sum, avg, min, max, count, terms, histogram, date_histogram |
| Bleve _all Field | [bleve-all-field.md](completed/bleve-all-field.md) | `x-antfly-include-in-all` schema configuration |
| Dynamic Templates | [dynamic-templates.md](completed/dynamic-templates.md) | Pattern-based field mapping with Bleve translation |
| Full-Text Chunks | [full-text-chunks.md](completed/full-text-chunks.md) | Full-text indexing for document chunks |
| Sparse Embeddings (SPLADE) | [sparse-embeddings.md](completed/sparse-embeddings.md) | Sparse vector index, Termite SPLADE pipeline, hybrid fusion |
| Graph Index Enricher | [pageindex-infra.md](completed/pageindex-infra.md) | Field-based edges, topology, summarizer with LeaderFactory |
| Graph Database | [graphdb/](completed/graphdb/) | Citation networks, knowledge graphs, edge types, traversal |
| Linear Merge API | [linear-merge-api.md](completed/linear-merge-api.md) | Stateless progressive sync from external sources |
| Foreign Tables | [foreign-tables.md](completed/foreign-tables.md) | PostgreSQL as federated data source with SQL translation |

### RAG & Agents

| Feature | Document | Summary |
|---------|----------|---------|
| RAG Streaming Evolution | [rag-streaming-evolution.md](completed/rag-streaming-evolution.md) | Token-by-token streaming, multi-table, parallel LLM |
| Retrieval/Generation Agents | [retrieval-generation-agents.md](completed/retrieval-generation-agents.md) | DFA-based `/agents/retrieval` with tool calling |
| Generation Mode | [generation-mode-feature.md](completed/generation-mode-feature.md) | `without_generation` flag for retrieval-only mode |
| Link Processing | [link-processing.md](completed/link-processing.md) | Schema-aware link download: HTML, PDF, images |

### ML & Termite

| Feature | Document | Summary |
|---------|----------|---------|
| HuggingFace Direct Pull | [huggingface-direct.md](completed/huggingface-direct.md) | `hf:` prefix support with variant auto-detection |
| Termite Operator GKE Autopilot | [termite-operator-autopilot.md](completed/termite-operator-autopilot.md) | GKE Autopilot compute classes and spot scheduling |
| Termite Operator TPUs | [termite-operator-tpus.md](completed/termite-operator-tpus.md) | GKE TPU node pool integration |
| Reader Interface (OCR/Vision) | [reader-integration.md](completed/reader-integration.md) | TrOCR, Donut, Florence-2, multi-stage readers |
| INT8 Fixups | [int8-fixups.md](completed/int8-fixups.md) | Worker pool and SIMD optimization for quantized inference |
| Termite Enhancements | [../termite-enhancements.md](termite-enhancements.md) | Ollama parity: lazy loading, queuing, metrics, caching |

### Document Processing & API

| Feature | Document | Summary |
|---------|----------|---------|
| DOCX/PPTX Support | [ppt-docx.md](completed/ppt-docx.md) | Heading-aware chunking, slide extraction, OOXML metadata |
| JPEG2000 Encoder | [jpeg2000-encoder.md](completed/jpeg2000-encoder.md) | Implemented in go-jpeg2000 |
| API Key/Bearer Auth | [auth-types.md](completed/auth-types.md) | Salted SHA-256 API keys, bearer tokens, permission scoping |
| Website Termite Refactor | [website-termite-refactor.md](completed/website-termite-refactor.md) | Product switcher, Termite docs section, models page |

## Planned Features

### Partially Implemented

| Feature | Document | Status |
|---------|----------|--------|
| Answer Agent Enhancements | [answer-agent-enhancements.md](planned/answer-agent-enhancements.md) | Core agent done; semantic caching, citation verification, feedback loop, etc. not built |
| Chat Agent Frontend | [chat-agent-frontend-spec.md](planned/chat-agent-frontend-spec.md) | SDK types generated; React components not built |
| Audio TTS/STT | [audio.md](planned/audio.md) | STT done (OpenAI, Vertex, Termite); TTS partial (ElevenLabs only) |
| Separate Termite Packages | [separate-termite-packages.md](planned/separate-termite-packages.md) | Operator/proxy/client separated; core module still at root |
| Admission Webhooks | [admission-webhooks.md](planned/admission-webhooks.md) | Validation logic exists; webhook infrastructure not wired |

### Not Yet Started

| Feature | Document | Summary |
|---------|----------|---------|
| Query Samplers | [query-samplers/plan.md](planned/query-samplers/plan.md) | Capture query embeddings + results for ML training |
| Pipelined Query API | [pipelined-query-api/plan.md](planned/pipelined-query-api/plan.md) | Multi-stage query pipelines, delete-by-query |

## Quick Links

- **Completed Features**: [completed/](completed/)
- **Planned Features**: [planned/](planned/)
- **Termite Enhancements**: [termite-enhancements.md](termite-enhancements.md)
- **Main Documentation**: [../CLAUDE.md](../CLAUDE.md)
- **API Specification**: [../src/metadata/api.yaml](../src/metadata/api.yaml)
