# Project Roadmap

This file is the top-level execution map for `antfly-zig`. Use it to answer:

- what the major project lanes are
- what order they should move in
- which detailed subsystem plan to open next

Use [TODO.md](TODO.md)
for the live bug list and remaining parity gaps. Use
[README.md](README.md)
for repository layout, build commands, and day-to-day development notes.
Use [Design documents](#design-documents) below for the full index of
subsystem design docs in this directory.

Detailed execution belongs in subsystem docs:

- [TODO.md](TODO.md)
- [BACKUPS.md](BACKUPS.md)
- [SERVERLESS.md](SERVERLESS.md)
- [STARTUP.md](STARTUP.md)
- [DB.md](DB.md)
- [BATCH.md](BATCH.md)
- [FULL_TEXT.md](FULL_TEXT.md)
- [QUERY_STRING.md](QUERY_STRING.md)
- [HBC.md](HBC.md)
- [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md)
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md)
- [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md)
- [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md)
- [pkg/inference/ROADMAP.md](pkg/inference/ROADMAP.md)
- [lib/raft/ROADMAP.md](lib/raft/ROADMAP.md)

## Test Targets

Use these build targets for the current test split:

- `make unit-test`
  - focused fast/unit-style buckets, storage, auth, serverless, and other
    non-chaos lanes
- `zig build vopr-test`
  - deterministic VOPR engine, metadata, Raft, HA, storage, application-domain,
    and production public-HTTP suites
- `zig build antfly-chaos-test`
  - delayed transport, restart, partition, and long-running metadata chaos
    coverage
- `make test`
  - umbrella target that runs all of the above

## Current Shape

The project is past basic substrate bring-up. The main work now is product
correctness, public-contract convergence, and making the stateful and serverless
execution modes feel like one database product. See [Shipped](#shipped) below
for what already exists, and [Major Lanes](#major-lanes) for what is still
converging.

The live gaps are tracked in [TODO.md](TODO.md):

- current E2E failures and CI coverage gaps
- serverless table architecture and publication parity
- stateful/control-plane follow-up
- query, search, retrieval, API, config, and protocol parity

## Shipped

These pieces are implemented and in active use, not planning targets. Each
line links to the design doc with the durable detail (contracts, invariants,
formats):

- hosted Raft/runtime substrate with split/merge coordination —
  [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md),
  [GROUPS.md](GROUPS.md), [RELOCATIONS.md](RELOCATIONS.md)
- metadata service/server, desired topology, placement, reconciliation, and
  status surfaces —
  [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md),
  [STATUS.md](STATUS.md), [STATUS_API.md](STATUS_API.md)
- DB-backed shard transitions, durable replica state, LMDB/WAL paths, and LSM
  backend work —
  [DB.md](DB.md), [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md)
- table/index lifecycle, routed reads/writes, graph/query/retrieval surfaces,
  and OpenAPI-shaped API contracts —
  [SCHEMA.md](SCHEMA.md), [GRAPH.md](GRAPH.md), [OPENAPI.md](OPENAPI.md)
- reusable full-text, vector, graph, JSON, regex, image, audio, and Antfly
  inference library modules —
  [FULL_TEXT.md](FULL_TEXT.md), [HBC.md](HBC.md),
  [lib/json/JSON.md](lib/json/JSON.md), [lib/regex/REGEX.md](lib/regex/REGEX.md),
  [lib/image/IMAGE.md](lib/image/IMAGE.md), [lib/audio/AUDIO.md](lib/audio/AUDIO.md),
  [pkg/inference/ROADMAP.md](pkg/inference/ROADMAP.md)

Serverless manifest/artifact/publication work is deliberately not listed here:
it has a table-first public contract but is still under active convergence —
see [Serverless Table Product](#4-serverless-table-product) below.

## Major Lanes

### 1. Product Correctness And CI

Primary reference:
- [TODO.md](TODO.md)

Near-term goals:
- fix current Antfly Python E2E failures before expanding public surface area
- make readiness/status bugs diagnosable from preserved roots and server logs
- move high-signal E2E coverage into regular CI once it is stable enough
- keep `zig build openapi-root-check` as the safe contract drift check until
  all source specs are local

### 2. Stateful Metadata And Runtime

Primary references:
- [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md)
- [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md)

Near-term goals:
- keep metadata/data-node orchestration stable across split, merge, recovery,
  and remote status reporting
- strengthen replica/bootstrap descriptors and disappearing group/store
  handling
- keep product policy out of raft-core where the metadata layer can own it

### 3. Public API, Query, Search, And Retrieval

Primary references:
- [TODO.md](TODO.md)
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md)
- [../openapi.yaml](../openapi.yaml) (joined public spec at the repo root)

Near-term goals:
- fix status/readiness gaps before broadening API behavior
- add parity coverage before new public query/search shapes
- deepen hybrid, foreign source, join, graph, and retrieval behavior against
  Go/OpenAPI expectations
- keep handwritten behavior and generated contract surfaces aligned

Principle:
- internal control-plane/runtime seams stay as Zig modules
- external user/operator APIs should converge on the OpenAPI contract
- both the stateful and serverless paths should converge on the table-centric
  product contract wherever the capability makes sense

### 4. Serverless Table Product

Primary references:
- [SERVERLESS.md](SERVERLESS.md)

Near-term goals:
- keep `/tables/...` as the public serverless product surface
- keep provider/runtime controls under `/_internal/...`
- finish canonical table metadata, publication state, and build-status
  alignment
- make index/schema changes publish through concrete per-family and per-index
  artifact actions
- make published/latest/exact-read freshness semantics explicit

Principle:
- serverless should be the same product with a different execution model, not a
  namespace-only database model
- reuse engine code from search, vector, graph, indexing, and segment machinery
- do not make serverless depend on hosted-Raft lifecycle or replica placement
  as first-order architecture

### 5. Storage Engine And Durability

Primary reference:
- [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md)

Near-term goals:
- keep LMDB/WAL durability and crash confidence improving
- keep LSM/HBC/vector write guardrails aligned with production-shaped ingest
- support metadata/data workflows without storage regressions
- keep reopen/recovery and simulation matrices strong

### 6. Antfly inference And Shared Libraries

Primary references:
- [pkg/inference/ROADMAP.md](pkg/inference/ROADMAP.md)
- [lib/json/JSON.md](lib/json/JSON.md)
- [lib/regex/REGEX.md](lib/regex/REGEX.md)
- [lib/image/IMAGE.md](lib/image/IMAGE.md)
- [lib/audio/AUDIO.md](lib/audio/AUDIO.md)

Near-term goals:
- keep reusable libraries documented where their implementation lives
- keep Antfly inference API/model work separate from Antfly product API planning unless
  the integration surface requires it
- use library-level docs for design details and root docs for repository
  orientation

## Immediate Project Order

1. Stabilize the active Antfly E2E failures in `TODO.md`, especially status and
   readiness bugs that obscure actual data-path health.
2. Tighten CI around the stable parts of the current verification matrix.
3. Continue serverless table/publication convergence:
   - canonical table metadata
   - index/schema publication execution
   - per-family artifact reuse
   - explicit freshness/read semantics
4. Deepen public query/search/retrieval parity only with matching coverage.
5. Continue stateful metadata/runtime hardening around split, merge, recovery,
   backup/restore, and remote status propagation.
6. Keep shared library docs and implementation colocated under `lib/` as those
   modules become stable user-facing design surfaces.

## Planning Rules

- Put project-wide sequencing here.
- Put current bugs and parity task detail in `TODO.md`.
- Put subsystem implementation detail in the subsystem roadmap/plan.
- If a task is mostly about one directory, update that subsystem plan first.
- If a task changes project priorities or ordering, update this file too.

## Design documents

Every design doc in `zig/` (`ls zig/*.md`), grouped by area. Each line is the
file's own title and a one-line description taken from its first paragraph.

### Storage

- [DB.md](DB.md) — DB Contract: the DB-layer landing page for handle/runtime
  ownership, write and derived-artifact contracts, and storage backend
  boundaries.
- [LITE.md](LITE.md) — Antfly Lite: Antfly's single-file storage engine and
  `.aflite` format, usable embedded or via the standalone server.
- [STORAGE.md](STORAGE.md) — Antfly deployment and storage engines: deployment
  topology and persistence are modeled as independent choices.
- [DATA_DIR.md](DATA_DIR.md) — Zig Data Directory Layout: how `--data-dir`
  roots all durable local Antfly state.
- [BACKUPS.md](BACKUPS.md) — Backup and Restore: the public contract, manifest/
  blob storage model, and table/cluster backup and restore behavior.
- [RELOCATIONS.md](RELOCATIONS.md) — Relocations: the production relocation
  design for moving hot or draining shard placements without empty-copy reads.
- [SCALING.md](SCALING.md) — Antfly Scaling and Node Shutdown: the
  control-plane contract for adding/removing data capacity and safe scale-down.
- [GROUPS.md](GROUPS.md) — Group IDs: the shared `u64` Raft group ID space
  across metadata and data groups.
- [HOT_STANDBY.md](HOT_STANDBY.md) — Hot Standby WAL Replication: a Postgres-style
  single-primary replication mode for read replicas, DR, and online upgrades.
- [CDC.md](CDC.md) — CDC: the Postgres replication-source / CDC subsystem.
- [SCHEMA.md](SCHEMA.md) — Schema: the current split in how schema is defined
  and enforced.
- [STANDALONE.md](STANDALONE.md) — Standalone Runtime, Providers, and Shard DB
  Access: node-level design for metadata, data, local providers, and DB
  runtime ownership.

### Indexing

- [FULL_TEXT.md](FULL_TEXT.md) — Full-Text Indexing: visibility semantics kept
  aligned with the LSM path.
- [FULL_TEXT_PERFORMANCE.md](FULL_TEXT_PERFORMANCE.md) — Full-Text Performance
  and Benchmark Plan: how to make full-text performance work measurable,
  comparable, and implementable.
- [SORT.md](SORT.md) — Sort And Search Design: converging `order_by`/
  `search_after` on a native, segment-aware execution model.
- [DOCID.md](DOCID.md) — Document IDs and Posting IDs: the document-ID/
  posting-ID key layout, ordering, and cross-index planning contract.
- [HBC.md](HBC.md) — HBC Dense Indexing: the canonical dense-indexing note
  covering DB integration, search/rerank, and storage-engine bulk build.
- [KMEANS.md](KMEANS.md) — K-Way K-Means Bulk Build Plan: partially
  implemented k-means bulk-build path for dense indexing (see file for what
  remains).
- [SPFRESH.md](SPFRESH.md) — SPFresh-Style HBC Refactor Plan: evaluating a
  mutable AKNN index without prematurely replacing the current HBC
  implementation.
- [VECTOR_STORE.md](VECTOR_STORE.md) — Separate Vector Store: Intended Design:
  an experimental separate vector store with reference-based source payload
  handling.
- [VECTOR_STORE_REVIEW.md](VECTOR_STORE_REVIEW.md) — Vector store correctness
  and design review — 2026-09-08: dated investigation log for vector-store
  memory/recovery correctness work.
- [VECTORDBBENCH_FINDINGS.md](VECTORDBBENCH_FINDINGS.md) — VectorDBBench
  Findings: the working evidence log for 50K/1M VectorDBBench investigation.
- [DENSE_INDEXING_LIFECYCLE.md](DENSE_INDEXING_LIFECYCLE.md) — Dense Indexing
  Lifecycle: core implementation complete, with remaining rollout-gate
  qualification runs tracked in the file.
- [GRAPH.md](GRAPH.md) — Graph Indexing Design: graph indexes consuming
  enrichment artifacts through the managed-index replay path.
- [RESOLUTION.md](RESOLUTION.md) — Entity Resolution Design (Resolver,
  Promoter, Fusion): turning per-document extraction artifacts into canonical
  entities and an entity graph.
- [ALGEBRAIC.md](ALGEBRAIC.md) — Algebraic Sparse-Token Database Theory: a
  database design sketch representing records as sparse symbolic tokens.
- [VISIBILITY.md](VISIBILITY.md) — Visibility Masks: how query execution
  removes deleted, not-yet-visible, expired, and stale rows.
- [ENRICHMENTS.md](ENRICHMENTS.md) — Enrichments: the canonical enrichment
  architecture note covering artifact identity, storage-side enrichment, and
  serverless publication.
- [INFLIGHT.md](INFLIGHT.md) — Inflight Batching Plan: reducing dense derived
  apply overhead by cutting expensive apply/commit boundaries.
- [BATCH.md](BATCH.md) — Batch Coalescing: collapsing duplicate per-key work
  before storage, replay, and indexing fan out.

### Query

- [QUERY_STRING.md](QUERY_STRING.md) — Query String Language: the
  Lucene-style query string syntax.
- [QUERY_BUILDER.md](QUERY_BUILDER.md) — Query Builder Agent: porting the Go
  query-builder agent to build general Antfly queries, not only Bleve
  fragments.
- [JOINS.md](JOINS.md) — Joins: how joins work in `antfly-zig` and why the
  distributed path is more complex than the Go service-layer flow.
- [RELATIONAL.md](RELATIONAL.md) — Relational Mode: tables are document-first
  by default, with schema optional and soft.
- [LAKES.md](LAKES.md) — Lake Query Mode: querying user-owned object-storage
  files (Parquet, Iceberg, later Lance) through the relational row-plan API.
- [EXTRACT.md](EXTRACT.md) — Extraction API: the one public extraction
  endpoint Antfly exposes.

### Inference

- [MANAGERS.md](MANAGERS.md) — Resource and model manager design: why Antfly
  has three cooperating process services.
- [INFERENCE_CACHING.md](INFERENCE_CACHING.md) — Inference caching: the two
  materially different inference caches Antfly uses.
- [PDF.md](PDF.md) — Bounded document preparation and multimodal inference:
  the shared PDF window scheduler and precommit/replay document-preparation
  pipeline.

### Runtime, ops, and product surface

- [STD_IO_HTTP.md](STD_IO_HTTP.md) — Structured `std.Io` HTTP and API Runtime
  Design: HTTP transport, listener concurrency, and runtime supervision across
  runtimes.
- [STARTUP.md](STARTUP.md) — Startup Status And Provisioning: keeping
  startup/health/status paths cheap even for large local shards.
- [STATUS.md](STATUS.md) — Antfly Status Subsystem: where the runtime status
  subsystem is today and the production shape it is converging on.
- [STATUS_API.md](STATUS_API.md) — Status API: the public `/db/v1/status` and
  `/db/v1/cluster` endpoints.
- [METRICS.md](METRICS.md) — Antfly Metrics and Profiling: current runtime
  metrics, benchmark-only profiling logs, and status-count semantics.
- [SECRETS.md](SECRETS.md) — Antfly Zig Secrets Store: the local secrets store
  backing `/secrets` and `${secret:key}` resolution.
- [AUTH.md](AUTH.md) — Auth: the current auth model and the planned
  document-level security shape for row filters.
- [CONNECTIONS.md](CONNECTIONS.md) — Connections: the long-run `/connections`
  interface and its first Zig implementation target.
- [CAPI.md](CAPI.md) — Antfly C API: `libantfly`, the stable embedded C ABI
  boundary shared across storage layouts.
- [MCP.md](MCP.md) — Zig MCP Support: the reusable MCP protocol core and
  Antfly-specific HTTP adapters.
- [A2A.md](A2A.md) — Antfly Native Agents And A2A Integration: the two native
  bounded-agent APIs in the Zig implementation.
- [ARD.md](ARD.md) — Antfly Agentic Resource Discovery Design: support for
  Google's Agentic Resource Discovery (ARD) specification.
- [WEBSEARCH.md](WEBSEARCH.md) — Web Search: the long-run web-search provider
  model for Antfly agents and connection configuration.
- [ARTIFACTS.md](ARTIFACTS.md) — Artifacts And Enrichments API: the intended
  public API and storage boundary for artifacts/enrichments.
- [SERVERLESS.md](SERVERLESS.md) — Serverless Plan: building a serverless
  architecture from `antfly-zig` code (see [Serverless Table
  Product](#4-serverless-table-product) above for current priorities).
- [EXTENSIONS.md](EXTENSIONS.md) — Postgres-Style Extension System: written as
  a design proposal, but a real subset (catalog, install/update/drop
  lifecycle, WASM runtime, `/extensions/v1/*` API) has since shipped in
  `zig/pkg/antfly/src/extensions/`; the doc has not been reconciled to match.
- [OPENAPI.md](OPENAPI.md) — OpenAPI: how generated Zig OpenAPI code is kept
  checked in and imported.

### Testing

- [TESTING.md](TESTING.md) — Testing: the default test aggregate plus
  package-scoped and special-purpose test tiers.
- [VOPR.md](VOPR.md) — VOPR: Deterministic Autonomous Testing for Antfly: the
  common VOPR engine and deterministic `std.Io` runtime integration.
- [FLAKES.md](FLAKES.md) — Zig runtime flakes: recorded flake evidence,
  reproduction conditions, and regression status.
- [BENCHMARKS.md](BENCHMARKS.md) — Benchmarks and build tools: commands for
  building and running benchmarks and tools from `zig/`.

### Meta and trackers

- [README.md](README.md) — antfly-zig: the Zig monorepo overview for AntflyDB
  and the inference runtime.
- [TODO.md](TODO.md) — TODO: the single live tracker for current bugs, active
  work, and remaining Go-parity gaps.
- [ROADMAP.md](ROADMAP.md) — Project Roadmap: this document.
