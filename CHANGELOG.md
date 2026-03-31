# Changelog

All notable changes to Antfly will be documented in this file.

## Roadmap

### Upcoming Features

- **Multimodal Search Enhancements**
  - Support for [ImageBind](https://github.com/facebookresearch/ImageBind) as a multimodal embedder
  - Image-to-image similarity search (search with images as queries)
  - Extended audio file support (MP3, WAV, FLAC)
  - Video file processing and frame extraction
  - Automatic scene detection and indexing for video content

- **Advanced AI Capabilities**
  - Custom prompt templates for different content types
  - Model performance comparison tools
  - Automatic model selection based on content type
  - Fine-tuning support for domain-specific embeddings

## Releases

### [0.1.1] - 2026-03-31

#### Highlights

- **Faster Swarm Mode** — local shard bypass eliminates network hops and JSON serialization for single-node deployments, significantly reducing query latency
- **Smarter Vector Search** — automatic reranking and size-aware search effort tuning deliver better recall out of the box
- **Friendlier Errors** — LLM generation failures now return clear, actionable error messages with appropriate HTTP status codes
- **Automatic Shard Management** — Antfly now automatically splits large shards and merges underutilized ones, keeping cluster performance balanced without manual intervention
- **memoryaf** — a new MCP-compatible memory service with built-in HTTP API and dashboard for memory-augmented search

#### Features

- Automatic shard split policy keeps individual shards from growing too large
- Online shard merges consolidate underutilized shards without downtime
- Local shard bypass skips HTTP and serialization overhead in swarm mode
- Single-shard fast path and index caching speed up common query patterns
- Automatic reranking for HBC vector search improves recall without configuration
- Size-aware search effort defaults adapt to index size automatically
- Hybrid full-text fallback when embedding indexes are unavailable
- Packed dense and sparse embedding format accepted in the query API
- Unified bounded agent APIs across all generated clients
- memoryaf: MCP-compatible memory service with HTTP API and embedded dashboard
- Richer query builder schema context for better autocomplete
- NER entity extraction and knowledge graph support in docsaf
- LLM generation error classification with user-friendly messages @dovinmu

#### Bug Fixes

- Fix cosine distance centroid normalization in HBC indexes @dovinmu
- Fix RaBit centroid clone aliasing causing quantization regressions @dovinmu
- Fix nil-interface panic in enricher stats during health checks — thanks @montanaflynn!
- Fix raft serveChannels shutdown hang — thanks @montanaflynn!
- Fix shard orphaning in StopRaftGroup when shard is initializing @esniff
- Fix Pebble close races during split finalize and node shutdown
- Fix split write routing during handoff, cutover, and readiness transitions
- Fix Store.Close goroutine leak after shard shutdown
- Fix index cache concurrency with proper LoadOrStore semantics
- Fix SSE error events not being sent when RAG generation fails @dovinmu

#### Performance

- Eliminate JSON serialization on the local shard search path
- Optimize inter-node vector search serialization

[Full changelog](https://github.com/antflydb/antfly/compare/v0.1.0...v0.1.1)

---

### [0.1.0] - 2026-03-17

#### Highlights

- **First official public release of Antfly**

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.19...v0.1.0)

---

### [0.0.19] - 2026-03-17

#### Highlights

- **Pruning Fix** — handle negative scores in pruning
- **Build Version** — add build version ldflags

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.18...v0.0.19)

---

### [0.0.18] - 2026-03-16

#### Highlights

- **Initial Public Release** — Antfly is now open source
- **Transaction Safety** — multiple fixes for nil pointer panics during shutdown and transaction recovery
- **Schema Migration** — table migration state exposed via API for schema version cutover
- **SQL Join Fix** — LEFT JOIN no longer returns INNER JOIN results due to plan cache key collision

#### Features

- Initial public release of Antfly
- Add table migration state to API for schema version cutover
- Replace `map[string]any` transaction records with typed `TxnRecord` struct
- Thread `commit_version` through `ResolveIntentsOp` proto and clean up transaction DB code
- Fix HBC delete to repair underfull leaves and update recall expectations
- Fix race in Group causing ResultGroup to drop results from queued tasks
- Fix nil pointer panic in transaction recovery during shutdown
- Guard transaction Pebble accesses with `pdbMu` to prevent nil dereference on shutdown
- Fix LEFT JOIN returning INNER JOIN results due to plan cache key collision

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.17...v0.0.18)

---

### [0.0.17] - 2026-03-15

#### Features

- CLIPCLAP multimodal embedder capabilities (#421)
- `search_after`/`search_before` cursor pagination (#413)
- Enrichment pipeline error handling for broken remote resources

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.16...v0.0.17)

---

### [0.0.15] - 2026-03-10

#### Highlights

- **Foreign Table Joins** — query across foreign tables with automatic filter pushdown and SQL aggregations
- **Routed PG Replication** — route PostgreSQL CDC streams to specific tables with a new evaluator package for custom replication logic
- **Distance Metric Configuration** — choose between cosine, euclidean, and dot-product distance per embedding index
- **Index & Enricher Observability** — new stats endpoints exposing document counts, enrichment progress, and per-index health

#### Features

- **pgaf** — PostgreSQL extension providing a custom index access method (`CREATE INDEX ... USING antfly`), `@@@` operator for full-text/semantic/hybrid search, query builder functions, sync triggers, and `antfly_search()` for native Postgres integration
- Foreign table join support with filter pushdown and SQL aggregations (#400)
- Routed PostgreSQL replication with evaluator package extraction (#403)
- Configurable distance metrics for embedding indexes (#406)
- Comprehensive index and enricher stats API (#405)
- CLIP image search improvements for multi-image queries (#406)

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.14...v0.0.15)

---

### [0.0.13] - 2026-03-03

#### Highlights

- **Sparse Vector Search (SPLADE)** — hybrid search combining dense and sparse vectors with weighted fusion for better relevance
- **PostgreSQL CDC Replication** — automatically sync data from PostgreSQL into Antfly via logical replication
- **Operator Improvements** — PVC lifecycle management, availability zone topology, admission webhooks, and storage resilience
- **Faster Sparse Indexing** — up to 3.6x faster sparse index inserts with multi-tier caching

#### Features

- **Sparse Vector (SPLADE) Search** — hybrid dense+sparse fusion with configurable per-index merge weights
- **PostgreSQL CDC Replication** — logical replication with automatic change capture from PostgreSQL tables
- **Chunking and Summarization** support in sparse embeddings index
- **Operator: PVC Lifecycle** management, availability zone topology, and storage resilience
- **Operator: Admission Webhooks** for AntflyCluster resource validation

#### Performance

- Up to 3.6x faster sparse index inserts
- Multi-tier caching and batched writes for sparse indexes
- Configurable sync level for embeddings indexes

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.12...v0.0.13)

---

### [0.0.9] - 2026-02-22

#### Highlights

- **Secrets Management** — new API and dashboard page for managing secrets
- **API Key & Bearer Token Auth** — authenticate with API keys or bearer tokens
- **AI Provider Timeouts** — configurable timeout for AI provider calls
- **PDF Enrichment Pipeline** — zip-direct reading, parallel extraction, vision-based categorization, and Florence 2 re-OCR for low-quality pages
- **Omni Edition** — renamed install edition with streamlined macOS support

#### Features

- **Secrets Management** API and Antfarm dashboard page
- **API Key & Bearer Token Authentication**
- **AI Provider Timeout** configuration
- **PDF Enrichment** — direct zip reading, parallel page extraction, page-type categorization with vision support, and Florence 2 re-OCR for low-quality pages
- **Dashboard** improvements with reverse proxy support and sidebar redesign

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.8...v0.0.9)

---

### [0.0.8] - 2026-02-18

#### Highlights

- **Cross-Table Transactions** — optimistic concurrency control (OCC) with read-modify-write support across tables
- **Built-in Embedder & Reranker** — bundled INT8 quantized all-MiniLM-L6-v2 embedder and reranker, no external service required
- **Shared Pebble Block Cache** — single block cache shared across all DB instances per process for better memory utilization
- **Operator Scheduling Constraints** — tolerations, nodeSelector, affinity, and topologySpreadConstraints in AntflyCluster CRD
- **Cluster Hibernation** — scale operator replicas to zero while retaining PVCs for cost savings

#### Features

- **Cross-Table Transactions** with OCC read-modify-write support
- **Built-in Embedder** — INT8 quantized all-MiniLM-L6-v2 bundled with Antfly
- **Built-in Reranker** — INT8 quantized reranker model bundled with Antfly
- **Shared Pebble Block Cache** across all DB instances per process
- **Operator Scheduling Constraints** — tolerations, nodeSelector, affinity, and topologySpreadConstraints added to AntflyCluster CRD
- **Cluster Hibernation** — allow scaling metadata and data node replicas to zero

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.7...v0.0.8)

---

### [0.0.7] - 2026-02-13

#### Highlights

- **Retrieval & Generation Agents** — new agentic architecture for retrieval-augmented generation
- **MCP & A2A Protocol Support** — connect Antfly to AI agents via MCP (`/mcp/v1`) and Agent-to-Agent protocol
- **Foreign Tables** — federated queries against external PostgreSQL databases
- **Named Provider Registry** — configure embedders, generators, rerankers, and chunkers by name
- **Audio Transcription** — speech-to-text support via Termite
- **Ephemeral Chunks** — transient chunk storage with the `store_chunks` config option

#### Features

- **Retrieval Agents** — tool-use agentic loop for retrieval and generation, replacing the previous answer endpoint (deprecated `/agents/answer` still available for backward compatibility)
- **MCP Server** at `/mcp/v1` for AI agent integration
- **A2A Protocol** facade for retrieval and query-builder agents
- **Foreign Tables** for federated PostgreSQL queries
- **Named Provider Registry** for embedders, generators, chains, rerankers, and chunkers
- **Audio/STT** with Termite as speech-to-text provider and media chunking support
- **Ephemeral Chunks** mode (`store_chunks` config option) for transient chunk storage
- **Graph Index** — field-based edges, topology constraints, and summarizer
- **Remote Content** configuration system for web scraping
- **CLAP & CLIPCLAP** model support for audio embeddings
- **Antfly Operator** now included in the main repository with docs and install manifests

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.3...v0.0.7)

---

### [0.0.2] - 2026-01-10

#### Highlights

- **Cross-table join support** for queries spanning multiple tables
- **Zero-downtime shard splitting** with two-phase split
- **TTS/STT audio library** with OpenAI and Google Cloud providers
- **CLIP model support** for multimodal image indexing

#### Features

- **Cross-Table Joins** — query across multiple tables with shard-aware routing
- **Zero-Downtime Shard Splitting** — two-phase split for high availability
- **Audio Library** — TTS and STT support with OpenAI and Google Cloud providers
- **Dynamic Templates** — flexible field mapping with automatic schema inference
- **Aggregations API** — range and term aggregations (renamed from Facets)
- **Chat Agent** — tool execution, clarification handling, confidence scoring, and multi-turn query builder mode
- **Indexes in Raft Snapshots** — faster recovery with pause/resume for index operations
- **ONNX Runtime GenAI** bundled for local LLM generation

[Full changelog](https://github.com/antflydb/antfly/compare/v0.0.1...v0.0.2)

---

### [0.0.1] - 2025-12-20

First official release of Antfly.

#### Features

- **Unified ONNX + XLA build** for cross-platform ML inference
- **Termite** downloads page with Homebrew support
- **antflycli** included in container images
- **Document TTL** support
