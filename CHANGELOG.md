# Changelog

All notable changes to Antfly will be documented in this file.

## Roadmap

### 0.3.x

- **SQL and the Postgres wire protocol** — DDL and DML over Antfly tables, a `psql`-compatible server, and a REPL
- **Relational storage** — closed schemas with foreign keys and PostgreSQL-compatible types, with full-text, vector, sparse, graph, and algebraic indexes derived over the same rows
- **Lake tables** — Iceberg and Parquet queried in place over object storage
- **Graph analytics** — PageRank and eigenvector centrality in graph indexes
- **Build system** — shorter compile times and lower build memory

## Releases

### [Unreleased]

- Relational packed base-row storage lands behind the relational table profile
- HBC vector LSM replaced with native WAL-backed generations
- Self-contained deterministic VOPR testing, with production HA promotion and scaling composed in simulation against retained corpora
- Bounded multimodal document processing in inference
- Homebrew version resolution and macOS dylib relocation made reliable
- Docs for the v0.2 launch rebuilt on one scaffold with facts re-verified against `main`

---

### [0.2.1] - 2026-09-08

Not just a patch release. v0.2.1 fills in parts of the v0.2.0 release that shipped rough or missing, on top of a long list of bug fixes found in the first weeks of the Zig runtime in production.

#### Highlights

- **Hot-standby HA** — standalone mode now has a Postgres-style hot standby with fenced promotion, a lower-cost alternative to Raft replication. Supported end to end by the operator and CLI, and chaos tested for weeks @bpopadiuk
- **Generated GPU kernels** — a Triton-style quant-kernel JIT now generates our CUDA and Metal kernels, accelerating Gemma 4, BGE-M3, and GLiNER2 while cutting the number of hand-maintained quantization types @timkaye11
- **Qwen3 across the board** — Metal and CUDA support for Qwen3-Embedding and Qwen3-VL; Qwen3 is now the suggested embedder, reranker, and multimodal reranker, with a q8_0 bundle in the quickstart sized for a laptop @timkaye11
- **PDFs, properly** — native rendering, OCR handoff, whitespace reconstruction, JBIG2 and JPEG 2000 decoding, and ParseBench-guided parser fixes, validated against benchmark corpora @dovinmu
- **Benchmarked** — graph, full-text, and vector index behavior benchmarked extensively, and many of the metrics you care about improved as a result. A release "model card" with our evaluation suite is coming @dovinmu
- **Structured graph query DSL** — a Cypher-like DSL covering a large subset of Cypher functionality, with much faster exact pattern queries
- **Multi-source vector indexes** — index multiple fields with the same embedding type into a single index

#### Features

- Structured graph query DSL, plus unit hierarchy grouping and traversal in queries
- Multi-source artifact indexes restored to production grade
- Managed embeddings published progressively, so partial indexes stay queryable during enrichment retries
- Authoritative cross-runtime index readiness, recoverable index generation repair, and a clearer `index wait`
- Catalog routing as a first-class capability
- Algebraic indexes gain dynamic projections and adaptive HyperLogLog cardinality
- `$min` document transforms
- Object storage checksum provenance; embedding activity and backup bundle architecture exposed
- Lake-native Parquet and Iceberg serving foundations, and a yacc-generated SQL parser foundation
- Configurable query, write, and inference admission; adaptive vector cache governance; unified inference resource ownership with enforced process envelopes
- Gemma 4 26B-A4B on Metal and CUDA; faster Gemma 4 E2B/E4B Metal decode with long-context correctness fixes @timkaye11
- Metal support for ModernBERT, NomicBERT, BGE-M3, Florence-2, and GLiNER2 and DeBERTa rerankers; quantized Mixedbread ONNX rerankers @timkaye11
- Inference execution is cancellable and observable, with HTTP cancellation and worker recovery tested end to end
- Operator: custom Kubernetes cluster domains, stable data pod routes, and unsafe metadata replica changes rejected
- Standard Linux containers built with glibc; CLI packages published from hosted runners; Python 3.11 through 3.14 supported

#### Bug Fixes

- Raft: retry-safe data apply replay, progress rounds and exact metadata proposal receipts, isolated read-index heartbeat contexts, bounded replication pipelines, and preserved metadata cadence
- Restore: fenced restore attempts recover, CLI restore polling tolerates transient responses, generated indexes survive native backups, and backup create admission is enforced
- Metadata: exact proposal application, mutation leader discovery, control-round authority churn, and fenced snapshot reads
- Index lifecycle: false DDL failures after status fencing, deferred lifecycle retries, stale incarnation status handoff, and repair handoff through fenced status publication
- Enrichment retry wedges across runtime boundaries; artifact-backed embedding indexes executable; artifact indexes preserved during embedding publication; materialized chunk embedding writes after list growth @dovinmu
- Artifact full-text merge scaling past 1M chunks
- Auth: request authorization boundaries hardened, durable destination authority bound, internal RPC authenticated across upgrades, transaction leaders rediscovered securely, direct browser query access secured
- HTTP: 413 returned before admitting oversized HTTP/1 bodies
- Inference: Bedrock request construction, preload registry variants, POSIX file advice gated to Linux, weight lifetimes bounded and Metal request contexts reclaimed, embedding CLI results written to stdout, standalone workers isolated
- PDF: compressed remote content decoded before extraction @dovinmu, stencil colors and sparse pattern transparency honored, remaining pdf-hard corpus gaps closed
- Autosplit: lost forced scans, and incomplete split provisioning snapshots retried
- Quickstart: index readiness, search, chunking, and retry contracts; Gemma multimodal inference and streaming completion
- Antfarm: table creation defaults and errors, artifact query retrieval
- Chunk overlap bounded and generation capacity errors preserved
- HBC recall under cache governance; overlapping vector cache fills bounded

#### Performance

- Exact graph pattern queries optimized
- Persistent ordered memory snapshots; public vector ingest memory and compaction optimized; HBC vector cache reads sharded
- Linear merge uses ordered hash scans; catalog routing no longer takes the Raft runtime lock
- Bounded runtime graph and parallel memory scheduling ported to the Zig runtime

#### Removed

- The end-of-life Go server implementations. The Go SDK, operator, docsaf, evalaf, genkit plugin, and memoryaf remain

#### Packaging

- Go, Python, and TypeScript SDKs v0.2.0 released to coincide with the v0.2.x series
- Operator v0.1.0 supports the full v0.2.x feature set and CLI interface
- Antfly installs with pip, npm, or Homebrew, and inference on macOS is much friendlier

Thanks to @markhayden, James, and Drew for finding and helping tackle things that were dropped from the Go port or were plain bugs in the new code, and to batz.prime for finding restore bugs on Raft deployments and the testing gaps behind them.

[Full changelog](https://github.com/antflydb/antfly/compare/v0.2.0...v0.2.1)

---

### [0.2.0] - 2026-08-10

Antfly v0.2.0 is a ground-up rewrite of the engine in Zig with zero dependencies outside the repository. It is the default release from this version on, uses less memory and CPU than the Go engine with a smaller RSS footprint, and adds a great deal of surface area that the Go engine never had.

#### Highlights

- **Zig runtime** — lower memory and CPU usage, a smaller RSS footprint, zero third-party dependencies, and WASM compatibility
- **Storage backends for every workload** — single-file (Antfly Lite, `.aflite`, SQLite-shaped), standalone LSM, distributed multi-Raft, and serverless over object storage, with portable AFB backups to move data between them
- **Inference in the database** — run GGUF, ONNX, and safetensors models, or fine-tune them, directly in Antfly, across many LLM and small-model architectures with Metal and CUDA acceleration
- **Artifacts API** — declarative JSON pipelines that run OCR, graph relation extraction, embeddings, LLM enrichment, and classification inside the database, the work you would otherwise build as a downstream queue or data engineering pipeline
- **Query language** — a simpler Bleve-style DSL that covers roughly 99% of Elasticsearch query capabilities, plus an improved foreign table planner
- **Index management** — Postgres-style visibility and control over indexes: rebuilds, cursors, and health status
- **Wider content types** — native PDF to PNG rendering for embedding generation, more audio formats, and hardened remote content handling
- **Testing** — deterministic simulation (VOPR-style) and TLA+ trace validation across Raft, transactions, snapshots, and shard splits

#### Upgrade compatibility

Antfly 0.2.0 is not an in-place data-directory upgrade from 0.1.x. Durable
formats changed without a 0.1.x compatibility path. Replace or wipe 0.1.x pods
and create fresh 0.2.0 data directories; export and restore any data that must
be retained.

The Python SDK now exposes transform operators with stable semantic enum names
(`SET`, `SET_ON_INSERT`, `UNSET`, `INC`, `ADD_TO_SET`, and `MAX`). Positional
`VALUE_n` names are not part of the 0.2 API.

Dense-query `_score` values now follow the same relevance convention as text,
sparse, hybrid, and Elasticsearch-style search results: higher values rank
first. Code that previously treated dense `_score` as a lower-is-better vector
distance must use the new optional `_distance` field instead. `_distance`
retains the raw metric-specific distance for direct dense hits. On a source
group ranked by dense descendants it is the distance of the best descendant
that supplied the group score. It is omitted for non-dense and fused results.

#### Features

- Antfly Lite: the engine as a single embedded file
- Hot-standby HA design and runtime surface (productionized in 0.2.1)
- Extension system v1, with extension MCP tools filtered by permission
- DOCID document identity and query APIs; derived document hierarchy in Antfly and docsaf
- Artifacts API, artifact CLI, and enrichment listing; enrichment assets routed into full text
- `GET /connections` listing connected providers, models, and stores; cluster topology status endpoint
- Data-plane auth CLI commands; trusted-principal MCP auth; authentication required on the public MCP endpoint; A2A and internal retrieval routes admin-only @dovinmu
- Retrieval agent tools configurable per request step; fielded and raw MCP query requests
- Native Gemini and Bedrock multimodal embeddings, including Titan @excenter; Jina v5 direct Metal embeddings; unified managed embedders
- GLiNER2: relation extraction, ONNX exports and symbolic Metal fallbacks, split GGUF bundles, and resident fine-tuning on Metal; LoRA policy alignment across fine-tuning recipes @timkaye11
- Gemma 4 E2B, E4B, and 12B on Metal and CUDA with production tool calling, MTP, QAT, and unified image and audio inference @timkaye11
- Florence-2 on Metal and CUDA; Tensor Core CUDA dispatch for CLIP/CLAP, mxbai rerankers, and GLiNER2 @timkaye11
- Native traditional ML predictors with `antfly inference pull` and `/ml/v1/predict` @timkaye11
- Inference CLI consolidated under `antfly inference`; SDK model pull helpers; inference defaults
- Antfarm: design system rebuild @stinkbugaf, Graph Index Explorer @timkaye11, connections view, simplified navigation, hosted mode
- Operator: Antfly and Termite operators consolidated, autoscaling with storage scale-down safety, single-node swarm mode @timkaye11, serverless operator and proxy, reduced RBAC @dovinmu
- docsaf: Google Drive auth, derived document hierarchy
- Entity-resolution design and comparison-levels scorer; tenant-scoped ARD discovery surfaces
- pip and npm CLI packaging; Antfly Cloud CLI shim @dovinmu; index query latency metrics
- Zig runtime, proxy, operator, Termite, and SDKs consolidated into the monorepo; Go fallback artifacts no longer built during release

#### Bug Fixes

- Composed vector query correctness, with faster hybrid filtering
- Raft: apply deadlock behind nightly e2e hangs, HTTP snapshot catch-up, data leader readiness publication, metadata bootstrap churn and no-op persistence, startup catch-up busy loop
- LSM: root and WAL locking hardened, recovered temp files cleaned before WAL replay, bloom filters stored only in SSTables
- Backup: restore writer conflict, backup forwarding, backup job URL configuration, read-index barrier for metadata backups, Go portable AFB backups restorable in Zig @dovinmu
- Dense replay catch-up convergence and progress
- Writes outside the byte range rejected during split transition; large linear merge requests handled
- TTL metadata for local document writes; query aggregation label lifetime; bool must+filter clause parsing @dovinmu
- Zig OOM retirement lifecycle cleanup @dovinmu; partial config defaults
- ONNX and Metal: dependency-free ONNX loading, symbolic projection embeddings and runtime shapes, quantized GGUF embedding lookup shapes @timkaye11, CLIP preprocessing contract and Metal parity, clipclap ONNX correctness, Metal SDPA layout, macOS Metal builds without MPSGraph @timkaye11
- PDF: malformed content recovery, extraction failure recovery, bounded extraction memory, unsupported OCR backend paths @stinkbugaf
- Corrupt indexes dropped without blocking shard startup; full-text enrichment index provisioning; artifact-backed chunk embeddings
- Remote content credential resolution and security config; store backup path handling @dovinmu; SearchAF Zig runtime response contracts
- TypeScript SDK: openapi-fetch bundled to fix a CJS interop crash @markhayden; vulnerable TypeScript and Go dependencies bumped @dovinmu

#### Performance

- Read-path scalability, quarantine self-heal, and HBC link repair
- Live-doc visibility, search effort policy, and stale payload repair @dovinmu
- LSM online write and bulk ingest tuning; load memory pressure reduced; metadata Raft memory retention bounded
- Gemma 4 E2B Metal reuse and CUDA inference speedups @timkaye11

#### Removed

- MLX backend

[Full changelog](https://github.com/antflydb/antfly/compare/v0.1.1...v0.2.0)

---

### [0.1.1] - 2026-03-31

#### Highlights

- **Faster Standalone Mode** — local shard bypass eliminates network hops and JSON serialization for single-node deployments, significantly reducing query latency
- **Smarter Vector Search** — automatic reranking and size-aware search effort tuning deliver better recall out of the box
- **Friendlier Errors** — LLM generation failures now return clear, actionable error messages with appropriate HTTP status codes
- **Automatic Shard Management** — Antfly now automatically splits large shards and merges underutilized ones, keeping cluster performance balanced without manual intervention
- **memoryaf** — a new MCP-compatible memory service with built-in HTTP API and dashboard for memory-augmented search

#### Features

- Automatic shard split policy keeps individual shards from growing too large
- Online shard merges consolidate underutilized shards without downtime
- Local shard bypass skips HTTP and serialization overhead in standalone mode
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
