# Serverless Plan

This document outlines a concrete plan for building a serverless architecture
from `antfly-zig` code.

The goal is not "run the current Antfly architecture without servers." The goal
is to build a different storage and control-plane architecture that reuses the
portable search/indexing parts of this repository while avoiding the current
replica-placement and local-shard assumptions.

The product goal is still cheap hosted Antfly, not a second unrelated database
product. The public data model should remain Antfly-shaped: tables, schemas,
indexes, enrichments, and explicit consistency semantics. Serverless
namespaces, manifests, and published versions should be treated as the serving
model underneath that product surface, not as the only user-facing abstraction.

## Terminology

The preferred internal/product terms going forward are:

- `TableDefinition`
  - canonical table metadata definition
- `TableMigrationState`
  - schema migration state, especially target schema vs `read_schema`
- `TableIndexCatalog`
  - canonical table-owned index definitions
- `TablePublicationState`
  - serverless publication/build/head/retention state for a table
- `PublishedGeneration`
  - one immutable published serving generation for a table

Older terms such as `namespace` and `manifest` may still exist in the current
implementation, but they should be treated as transitional implementation names
for `TablePublicationState` and `PublishedGeneration`, not as separate product
objects.

## Definition

For this document, "serverless" means:

- stateless query and indexing workers
- durable state in object storage plus an append-only write log
- local disk used only for cache, scratch, and build staging
- no permanent shard-to-node ownership
- no requirement that a given namespace always lives on one fixed machine

This is different from the current `antfly-zig` architecture, which is built
around:

- metadata-driven placement
- hosted replicas
- local durable stores
- split / merge orchestration
- a hosted multi-Raft runtime

## Product Contract

The serverless path should preserve Antfly's product identity while changing its
runtime shape.

That means:

- tables remain the primary user-facing abstraction
- schema, index, and enrichment configuration remain table-scoped concepts
- serverless namespaces / generations may exist internally as publication units
- cheap hosted reads come from immutable published artifacts plus cache
- stronger write semantics stay explicit instead of being implied by the read
  plane

In practice, the clean split is:

- write / control plane
  - owns table metadata, write admission, schema and index definitions, and any
    stronger transactional semantics
- published read plane
  - owns object-store artifacts, manifest heads, cache-driven query workers, and
    bounded freshness modes such as published-head and WAL-overlay reads

The serverless path may still use namespace-oriented internal APIs while the
implementation is being proven, but that should be treated as an implementation
detail of the serving plane rather than the final Antfly product contract.

## Sync Level Contract

`sync_level` is part of the shared public write contract, so serverless should
accept the same public enum as stateful even when the internals differ.

Current serverless mapping:

- `propose`
  - return after the write is admitted to the canonical WAL stream
- `write`
  - same public effect as `propose` today for serverless; durability comes from
    the canonical WAL rather than replica-local mutable state
- `full_text`
  - ingest the write, synchronously publish a new `PublishedGeneration`, and
    return only after the published head covers the admitted WAL range
- `aknn` / `full_index`
  - use the same synchronous publish path when vector/index visibility is
    satisfiable from the current table definition
  - reject explicitly when the table is configured for background
    materialization or enrichment that cannot be completed inline
- `enrichments`
  - only succeed when the requested visibility is already satisfiable without
    background materialization
  - otherwise reject explicitly instead of pretending serverless provides the
    same inline maintenance semantics as stateful

That means serverless preserves the public request/response shape while being
honest about where its execution model differs: strong write visibility is
supported when it can be satisfied by synchronous publication, and explicitly
rejected when it depends on asynchronous enrichment/materialization stages.

## Table Model vs Serving Model

The clean product/provider split is:

- user-facing model
  - tables
  - schema
  - `read_schema` migration state
  - indexes
  - enrichments
  - writes and queries
- provider-facing serverless model
  - table -> serving namespace binding
  - WAL / change stream
  - immutable artifacts
  - published manifest generations
  - retention and cutover

That means `namespace` is not the primary user object. It is the internal
serving unit for a table.

Stateful and serverless should both consume the same canonical table metadata.
The difference is how that metadata is realized:

- stateful
  - mutable replicated table state with online backfills
- serverless
  - immutable published serving generations built from table metadata plus WAL

The public API should keep talking about tables even when the provider is
really operating on an internal serving namespace underneath.

## Canonical Metadata Ownership

Table metadata should remain the source of truth for both engines:

- schema
- `read_schema`
- index definitions
- enrichment definitions
- replication source definitions
- backup metadata

Serverless namespace metadata should be limited to serving/runtime concerns:

- default query view
- retention
- compaction policy
- enrichment scheduling/runtime policy
- publication head / progress

Do not move schema or index ownership into namespace policy. That would create a
separate serverless product model instead of a cheaper hosted execution mode of
Antfly.

## Lifecycle Mapping

### Table Lifecycle

For users, table lifecycle should mean the same thing across both engines.

- `create table`
  - stateful: create table metadata, ranges, and replica placement
  - serverless: create table metadata, create a table -> serving namespace
    binding, and initialize empty WAL/publication state
- `drop table`
  - stateful: remove table metadata, topology, and local storage
  - serverless: remove table metadata and serving binding, then asynchronously
    garbage-collect WAL, manifests, and artifacts

### Schema Lifecycle

Schema remains table-scoped in both engines.

- stateful
  - `schema_json` is the target schema
  - `read_schema_json` preserves old read/index behavior during migration
  - shard-local rebuild/backfill completes
  - `read_schema_json` is cleared
- serverless
  - table metadata still owns `schema_json` and `read_schema`
  - the old published generation remains queryable while affected serving
    artifacts rebuild
  - a new generation is published when the rebuild completes

So `read_schema` should remain a table/control-plane concept. Serverless uses
it to decide what needs to be rebuilt and when a new published generation can
replace the old one.

### Index Lifecycle

Index definitions should also remain table-scoped in both engines.

- `create index`
  - stateful: mutate table metadata and run online shard-local backfill
  - serverless: mutate table metadata, build the new index artifact family in
    the background, then publish a new generation
- `drop index`
  - stateful: mutate table metadata and remove local index state
  - serverless: mutate table metadata, omit that artifact family from the next
    generation, and let retention remove old generations later

The serverless provider should therefore think in terms of "table-owned index
definitions that produce published artifact families", not "namespace-owned
indexes".

For chunk-aware full-text, the ownership should follow the Go contract:

- the user-facing switch is `chunker.full_text_index`
- not a direct full-text index `chunk_name` field
- serverless should interpret that as "chunk-derived text also feeds the
  table's full-text publication", while keeping the underlying chunk/artifact
  routing internal

## Incremental Rebuild Principle

Schema changes should not imply "rebuild the whole table" unless the change
actually affects every artifact family.

The better serverless target is:

- build generations from reusable artifact families
- reuse unchanged artifacts from the previous generation
- rebuild only affected artifact families
- publish a new manifest that references both reused and rebuilt artifacts

Examples:

- full-text field/schema change
  - rebuild only the affected full-text artifacts and any required stored-field
    projections
  - do not force a vector, sparse, or graph rebuild unless those inputs changed
- vector index config change
  - rebuild only that vector artifact family
- graph edge extraction change
  - rebuild only graph artifacts and any dependent derived outputs

This is the serverless analogue of stateful online backfill with `read_schema`:
keep serving the old generation while the new affected index artifacts are
prepared, then cut over atomically.

Current implementation status:

- WAL-time publication now reuses unaffected per-index full-text, dense,
  sparse, and graph artifacts when touched-document projections are unchanged
- metadata-only republish can reuse unaffected artifacts and republish heads
  without new WAL when the current head already satisfies the requested table
  metadata
- compaction now reuses unaffected named artifacts instead of rebuilding every
  search lane blindly

## Per-Index Artifact Target

The serverless build/publish path should move toward per-index/per-family
artifact ownership.

Target artifact families:

- document / stored-field artifacts
- full-text artifact family per full-text index
- dense vector artifact family per dense index
- sparse artifact family per sparse index
- graph artifact family per graph index
- derived-output artifacts per chunking / rerank / enrichment stage

Generations then become an assembly problem rather than a forced full rebuild:

- reuse prior artifacts when still valid
- rebuild only impacted families
- publish one new table generation / manifest head

### Full-Text Segment Sizing

The stateful full-text path now follows the Lucene / Tantivy shape more
closely: mutable indexing work is flushed into bounded immutable segments, and
merge output may atomically replace a set of old segment ids with multiple new
segment ids. That avoids depending on one giant segment value and keeps the
index format scalable as document count and stored-field payload size grow.

The serverless full-text path is separate. Its current
`serverless/text_segment` artifact codec still represents one logical
full-text artifact as one encoded payload with `u32` counts and lengths. A
proper serverless fix should not just widen those fields or stream one larger
blob. It should make the manifest and query path understand a per-index
full-text artifact family made of multiple bounded immutable text artifacts,
with publication cutover replacing the family atomically. That keeps the
serving model aligned with Lucene / Tantivy and with the stateful segment
architecture, and avoids rebuilding or loading a single oversized serverless
text artifact.

## Index Management Stance

The intended product model is that users can create and manage indexes ad hoc on
tables in both stateful and serverless modes.

That means:

- the public table API owns index lifecycle
- stateful executes that lifecycle through online mutable backfill
- serverless executes that lifecycle through background artifact builds and
  published cutover

Current implementation note:

- stateful already supports public ad hoc index lifecycle
- serverless now exposes public table-shaped `create index` / `drop index`
  lifecycle and persists those definitions as canonical table-owned metadata
- serverless publication planning/execution is now per-index for:
  - full-text
  - dense vector
  - sparse vector
  - graph
  with reuse/rebuild/drop actions visible in build-status and query
  publication views
- compaction follows the same named-artifact model instead of collapsing back
  to one anonymous artifact per family

Remaining work is narrower now:

- derived-output/materialization paths are still coarser than the main artifact
  families in some publish/republish cases
- schema / `read_schema` migration cutover still needs fuller table-centric
  execution and visibility
- joins now have a first published-head public slice on `/tables/{table}/query`
  for inner/left/right equality joins and nested joins, and direct
  `foreign_sources` table reads now work on that same public route when the
  path table name resolves to a registry-backed foreign source; direct foreign
  aggregations now also work there for the current Go-shaped subset
  (`count`, `sum`, `avg`, `min`, `max`, `stats`, `terms`), but remaining work
  still includes:
  - richer foreign-source planning/runtime polish beyond the current
    statistics/runtime seam
  - broader right-side filter parity
  - any distributed/shuffle-style serverless execution
  - broader foreign-source parity beyond the current slice; valid
    `foreign_sources` maps now normalize through the public query path, resolve
    `${secret:...}` DSNs, direct foreign-table reads and foreign RHS execution
    work including nested foreign leaf joins, direct foreign-table
    aggregations now flow through the same runtime seam, the serverless
    bootstrap can install the shared Postgres runtime by default when `libpq`
    is available, and the executor-backed Postgres seam now supports a real
    dynamic `libpq` transport plus the Go-shaped parameterized
    `filter_query_json` subset
  - live PostgreSQL E2E coverage now exists for the main Go foreign-table
    slice on the serverless lane too:
    - direct foreign-table query
    - filtered foreign-table query
    - unsupported foreign aggregation rejection
    - Antfly-to-Postgres join
  - remaining foreign-source work is now mostly broader behavior:
    CDC-backed foreign join coverage, richer foreign query routing beyond the
    current table/join subset, broader right-side filter parity, and any
    distributed/shuffle-style execution if serverless ever grows that far
- keep pushing operator/public docs toward the table contract rather than
  namespace-centric language

## API Stance

The public serverless HTTP surface should be table-oriented from the start.

- public routes live under `/tables/...`
- namespace-oriented routes live under `/internal/v1/namespaces/...`
- internal namespace routes exist for serving/debug/build plumbing, not as the
  main product API
- no new end-user capability should be introduced under `/internal/v1/namespaces`

Public routes should cover the product behaviors users reason about:

- table lifecycle
- ingest
- query, published/latest reads, and search
- table-scoped joins when the published head can satisfy them locally
- graph reads when graph is part of the product surface

Serverless-only table controls should stay behind `/internal/v1/tables/...`:

- build and build-status
- serverless table policy/runtime knobs

The remaining `/internal/v1/namespaces/...` routes should stay internal-only:

- manifest head inspection and head publication
- explicit published-version reads
- artifact inspection
- internal namespace lifecycle and debug operations

If product code needs any of those behaviors later, expose them as table-shaped
debug or admin APIs rather than leaking raw serving namespace ids.

## Shared Public Table API Backend

The public `/tables/...` surface should be shared across stateful and
serverless implementations wherever the route semantics are the same. Public
HTTP handlers should own request parsing, validation, error/status mapping, and
response encoding; engine-specific code should sit behind small backend
interfaces.

The shared public layer is organized around two capability surfaces:

- `TableApi`
  - public table route families such as `batch`, `POST /tables/{table}/query`,
    `GET /tables/{table}/query`, and table backup/restore
- `ClusterApi`
  - cluster-level backup routes such as `/backup`, `/restore`, and `/backups`

Implementation-specific logic stays behind those interfaces:

- stateful metadata, raft, table reads, and table writes
- serverless catalog, build, query, and publication runtime

Internal and admin surfaces remain separate. In particular:

- do not force internal serverless namespace/debug routes through `TableApi`
- do not force serverless table build/policy controls through the shared public
  interface
- do not force metadata or raft admin routes through the shared public interface
- do not force cluster-level backup routes through `TableApi`; keep them under
  `ClusterApi`

Current extraction status:

- [x] Shared backend for public `batch`
- [x] Shared `/tables/...` interface introduced as `TableApi`
- [x] Shared public handlers for `batch`, `POST /tables/{table}/query`, and
  `GET /tables/{table}/query`
- [x] Stateful backend wired to shared public query handlers
- [x] Serverless backend wired to shared public query handlers
- [x] Serverless-only table policy/build/build-status moved behind
  `/internal/v1/tables/...`
- [x] Shared public handlers for table backup/restore
- [x] Shared public handlers for cluster backup/list/restore
- [ ] Separate public and internal serverless HTTP surfaces more aggressively
- [ ] Separate public and internal stateful HTTP surfaces more aggressively

Prefer extracting one public route family at a time and keeping parity/e2e
coverage green after each slice.

## Current Execution Priorities

The bootstrap/objectstore/control-plane seam is in decent shape. The next
priority is no longer basic end-to-end publication scaffolding. The next
priority is making the published read path and runtime model look like a real
serverless retrieval system.

The public surface should continue to converge on `table` while `namespace`
remains an internal serving/publication unit until there is a better replacement
name.

Table-facing progress:

- [x] document that serverless is a cheap hosted deployment mode of Antfly
- [x] add a table-facing HTTP/client public surface
- [x] persist a table-to-serving mapping in the serverless catalog
- [x] route all table reads/writes/policy operations through that mapping
- [x] add table-specific response types anywhere namespace fields still leak
- [x] move namespace-oriented serving routes behind an explicit internal-only
  prefix
- [x] confirm the remaining namespace routes are ops/debug-only and not missing
  product APIs
- [ ] evaluate renaming internal `namespace` to `serving_namespace`,
  `publication`, or `generation_family` after the public surface is stable

What is already in decent shape:

- object-backed artifacts / manifests / WAL / catalog / progress
- bootstrap validation
- `GET /health`, `GET /status`, and `GET /metrics`
- URI-driven backend config for `file://`, `s3://`, and `gs://`

Execute the next tranche in this order:

1. finish real indexed-reader execution and the query request model
2. finish local query cache under `pkg/antfly/src/serverless/query/`
3. finish compaction that produces optimized published artifacts
4. harden publish/prune concurrency and crash/retry behavior
5. split query runtime from maintenance runtime roles
6. only then package with operator/proxy deployment patterns

After that, keep retrieval as the first-class product until the published
text/sparse/dense path is operationally credible. Then finish graph as a
read-mostly immutable published artifact family under
`pkg/antfly/src/serverless/*`, and only add narrow conditional write semantics
if the product needs them.

## Current Reusable Pieces

These parts are good candidates to keep and build on.

### Query and Index Execution

- `pkg/antfly/src/index.zig`
- `pkg/antfly/src/search/*`
- `lib/vector/src/*`
- `lib/vectorindex/src/*`
- `pkg/antfly/src/section/*`
- `pkg/antfly/src/segment.zig`
- `pkg/antfly/src/columnar.zig`

These contain the core retrieval and indexing logic that can still be useful in
an immutable-artifact and cached-query-worker design.

### Vector Search and Quantization

- `pkg/antfly/src/storage/hbc_adapter.zig`

This is valuable as algorithmic code even if the surrounding persistence model
changes.

### Backend Abstraction Work

- `pkg/antfly/src/storage/backend_types.zig`
- `pkg/antfly/src/storage/backend_adapter.zig`
- `pkg/antfly/src/storage/backend_erased.zig`
- `pkg/antfly/src/storage/backend_conformance_test.zig`
- `pkg/antfly/src/storage/lsm_backend.zig`
- `pkg/antfly/src/storage/lsm/*`

This work is a good base for local cache and build-time storage abstraction.
It is not yet the full abstraction needed for a serverless architecture, but it
is the right direction.

## Current Pieces To Freeze For The Serverless Path

These modules are important for the existing stateful distributed system, but
they should not be the foundation of the serverless path.

### Replica and Placement Stack

- `pkg/antfly/src/raft/RAFT.md`
- `pkg/antfly/src/raft/*`
- `pkg/antfly/src/metadata/*`

This stack assumes:

- persistent hosted replicas
- node-aware placement
- local recovery of replica state
- split / merge execution against owned ranges

That is not the right center of gravity for a stateless query/index worker
model.

### Local Ownership and Local Transaction Layers

- `pkg/antfly/src/storage/docstore.zig`
- `pkg/antfly/src/storage/shard.zig`
- `pkg/antfly/src/storage/transactions.zig`
- `pkg/antfly/src/storage/db/*`

These modules are still useful references, but they reflect:

- local durable ownership
- local MVCC and intent resolution
- range split and rollback semantics
- local background maintenance

Those assumptions should not define the serverless architecture.
They may still remain part of the canonical write/control plane if the cheap
hosted offering keeps stronger table-level semantics above a serverless serving
layer.

## Architectural Target

The target system should be built from four major subsystems.

### 1. Control Plane

Responsibilities:

- table and namespace lifecycle mapping
- schema / index / enrichment definitions
- auth, quotas, limits
- manifest head publication
- namespace progress tracking
- opportunistic worker coordination
- background job tracking

This control plane should track table-serving metadata and published-generation
state, not replica placement.

### 2. Ingest Frontends

Responsibilities:

- accept writes against Antfly tables
- validate and normalize write batches
- append them to a serverless change log / WAL used to build published serving
  generations
- return an acknowledged sequence number / version

This layer should be simple and narrow at first. One sequencer per published
serving unit is acceptable for the first implementation, even if the public
product surface remains table-centric.

### 3. Build / Compaction Workers

Responsibilities:

- read WAL entries
- build immutable text, vector, and metadata artifacts
- compact old artifacts into new versions
- publish new serving manifests atomically

These workers should be stateless aside from temporary local build state.

### 4. Query Workers

Responsibilities:

- load namespace manifests
- fetch required artifacts from object storage
- cache them locally
- execute search over cached artifacts

Any query worker should be able to serve any namespace after cache warmup.

## Consistency Model

The cheapest hosted path should make freshness and correctness explicit.

The read plane should support at least:

- `published`
  - reads from the latest published serving generation only
- `latest`
  - reads from the latest published generation plus a bounded WAL tail overlay
- exact point / transactional reads
  - served by the canonical write plane rather than by mutating immutable
    published artifacts in place

This keeps "cheap search serving" separate from "strong write semantics" without
forcing users to think about replica placement or internal namespace ownership.

## Runtime URIs And Environment

The runnable serverless entrypoint in `pkg/antfly/src/serverless_main.zig` is
configured by five required storage URIs:

- `ANTFLY_SERVERLESS_ARTIFACTS_URI`
- `ANTFLY_SERVERLESS_MANIFESTS_URI`
- `ANTFLY_SERVERLESS_WAL_URI`
- `ANTFLY_SERVERLESS_PROGRESS_URI`
- `ANTFLY_SERVERLESS_CATALOG_URI`

Each URI may currently use one of these schemes:

- `file://...`
- `s3://bucket/prefix`
- `gs://bucket/prefix`

The listener is configured with:

- `ANTFLY_SERVERLESS_BIND_HOST`
- `ANTFLY_SERVERLESS_BIND_PORT`
- `ANTFLY_SERVERLESS_TICK_INTERVAL_MS`

## Image And CI Path

The Zig runtime image is owned by `antfly-zig`, not by the Go control plane
repo.

Current binary/image split:

- top-level binary: `antfly`
- serverless API command: `antfly serverless api`
- serverless query command: `antfly serverless query`
- serverless maintenance command: `antfly serverless maintenance`
- serverless all-in-one command: `antfly serverless swarm`
- runtime image Dockerfile: `Dockerfile.serverless`
- published image: `ghcr.io/antflydb/antfly:zig`

Build graph steps:

- `zig build install-antfly`
- `zig build serverless-test`

GitHub Actions:

- `.github/workflows/zig-tests.yml`
  - PR / push coverage for `serverless-test`
  - smoke checks for `antfly --help` and the nested
    `antfly serverless <role> --help` commands
  - nightly scheduled `zig build test` + `zig build serverless-test`
- `.github/workflows/container-smoke.yml`
  - PR / push smoke build for `Dockerfile.serverless`
  - builds `install-antfly`
  - smoke-checks the nested `antfly serverless <role>` commands
  - smoke-builds the shared Zig runtime container before tag-time publish
- `.github/workflows/container.yml`
  - publishes `ghcr.io/antflydb/antfly:zig`
  - also publishes versioned tags like `ghcr.io/antflydb/antfly:zig-v0.1.0`
  - release tag convention: `zig/v<version>`

The Go operator/proxy repo should consume that single image and set container
args per role rather than carrying separate query and maintenance image names.

Model-backed enrichment is configured with:

- `ANTFLY_SERVERLESS_EMBEDDING_INDEXES_JSON`
- `ANTFLY_SERVERLESS_SPARSE_EMBEDDING_INDEX_NAME`
- `ANTFLY_SERVERLESS_CHUNK_EMBEDDING_INDEX_NAME`
- `ANTFLY_SERVERLESS_CHUNK_EMBEDDING_DIMS`

These are optional. If `ANTFLY_SERVERLESS_EMBEDDING_INDEXES_JSON` is absent,
the enrichment worker stays on deterministic fallback behavior.

### Backend-Specific Environment

For `file://...`, no extra auth configuration is required.

For `s3://...`, the runtime uses the S3-compatible objectstore client and reads:

- `AWS_ENDPOINT_URL`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- optional `AWS_SESSION_TOKEN`
- optional `AWS_REGION`

This is suitable for MinIO, AWS S3, Cloudflare R2, and other S3-compatible
endpoints.

For `gs://...`, the runtime uses the GCS JSON API client and resolves auth in
this order:

1. `GCS_BEARER_TOKEN` or `GOOGLE_OAUTH_ACCESS_TOKEN`
2. `GOOGLE_SERVICE_ACCOUNT_JSON`
3. `GOOGLE_APPLICATION_CREDENTIALS`

Optional GCS settings:

- `GOOGLE_CLOUD_PROJECT` or `GCLOUD_PROJECT`
- `GCS_OAUTH_SCOPE`
- `GCS_JSON_API_ENDPOINT`
- `GCS_JSON_API_UPLOAD_ENDPOINT`

The first fully automated path is service-account auth. That now lives in
`lib/objectstore/src/google_auth.zig` and is specific to the GCS objectstore
integration rather than a generic repo-wide OAuth framework.

### Runtime Examples

Local filesystem:

```bash
ANTFLY_SERVERLESS_ARTIFACTS_URI=file:///tmp/antfly-artifacts \
ANTFLY_SERVERLESS_MANIFESTS_URI=file:///tmp/antfly-manifests \
ANTFLY_SERVERLESS_WAL_URI=file:///tmp/antfly-wal \
ANTFLY_SERVERLESS_PROGRESS_URI=file:///tmp/antfly-progress \
ANTFLY_SERVERLESS_CATALOG_URI=file:///tmp/antfly-catalog \
zig build serverless
```

S3-compatible backends, including MinIO, AWS S3, and Cloudflare R2, all use
the same `s3://bucket/prefix` runtime URIs plus the AWS-compatible env vars:

```bash
export ANTFLY_SERVERLESS_ARTIFACTS_URI=s3://antfly/artifacts/dev
export ANTFLY_SERVERLESS_MANIFESTS_URI=s3://antfly/manifests/dev
export ANTFLY_SERVERLESS_WAL_URI=s3://antfly/wal/dev
export ANTFLY_SERVERLESS_PROGRESS_URI=s3://antfly/progress/dev
export ANTFLY_SERVERLESS_CATALOG_URI=s3://antfly/catalog/dev
export AWS_ENDPOINT_URL=...
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1

zig build serverless
```

GCS uses `gs://bucket/prefix` runtime URIs plus bearer-token or service-account
auth:

```bash
export ANTFLY_SERVERLESS_ARTIFACTS_URI=gs://antfly/artifacts/dev
export ANTFLY_SERVERLESS_MANIFESTS_URI=gs://antfly/manifests/dev
export ANTFLY_SERVERLESS_WAL_URI=gs://antfly/wal/dev
export ANTFLY_SERVERLESS_PROGRESS_URI=gs://antfly/progress/dev
export ANTFLY_SERVERLESS_CATALOG_URI=gs://antfly/catalog/dev
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

zig build serverless
```

After the process starts:

- `GET /health` returns a compact liveness/readiness payload
- `GET /metrics` returns machine-readable runtime and namespace counters
- `GET /status` returns the validated runtime view of the configured lanes

`/status` includes:

- role and maintenance feature flags
- parsed storage backends for artifacts/manifests/WAL/progress/catalog
- whether the runtime was validated successfully

`/metrics` and namespace build status now also expose enrichment execution
state more explicitly:

- `next_publish_reason`
  - `head_republish`
  - `wal_artifact_update`
  - `wal_enrichment`
- document publish-mode visibility:
  - `head_document_publish_mode`
    - `append_mutation_tail`
    - `inline_rebase`
    - `head_republish`
  - `next_document_publish_mode`
    - `append_mutation_tail`
    - `inline_rebase`
    - `head_republish`
  - `mutation_tail_resolution`
    - `none`
    - `background_compaction`
    - `next_publish_inline_rebase`
- aggregate publication action summaries on query/build views:
  - `head_artifact_actions`
  - `artifact_actions`
  - `head_derived_output_actions`
  - `derived_output_actions`
- aggregate derived-output resolution summaries on query/build/metrics views:
  - `derived_output_resolutions`
    - `disabled`
    - `ready`
    - `head_republish_reuse`
    - `pending_materialization`
    - `drop_on_republish`
- named per-index publication action arrays on query/build views:
  - `head_full_text_index_actions`
  - `full_text_index_actions`
  - `head_vector_index_actions`
  - `vector_index_actions`
  - `head_sparse_index_actions`
  - `sparse_index_actions`
  - `head_graph_index_actions`
  - `graph_index_actions`
- `pending_materialization_families`
  - `full_text`
  - `dense_vector`
  - `sparse_vector`
  - `chunk_preview`
  - `chunk_embeddings`
  - `rerank_terms`
- `enrichment_stage_source`
  - `current_head`
  - `pending_wal`
- `enrichment_stage_state`
  - `executing`
  - `awaiting_execution`
  - `deferred_for_publish_threshold`
  - `ready_for_publish`

Public table index status now also exposes per-index materialization blockers:

- `materialization_blocked`
- `materialization_blocker`
  - `chunk_preview`
  - `chunk_embeddings`
  - `lexical_sparse`
  - `full_text`
  - `dense_vector`

Use those fields to distinguish:

- metadata-only head republishes from WAL-driven publishes
- the last published head shape from the next predicted publish shape
- whether mutation-tail compaction debt still needs background compaction or
  will be cleared by the next inline-rebase publish
- WAL-driven artifact updates from WAL-driven enrichment publishes
- derived-output policy changes that can reuse current-head materialization from
  derived-output policy changes that still need new materialization work
- which concrete publication families are blocked on unfinished enrichment
- current-head enrichment that is actively running
- current-head enrichment that is queued but not yet executing
- pending-WAL enrichment that would run on the next publish
- pending-WAL enrichment that is currently deferred only because publish
  thresholds have not been met
- whether a specific public index is waiting on chunk preview generation,
  chunk embeddings, lexical sparse enrichment, or a direct artifact rebuild

`/metrics` also aggregates those blockers across namespaces, so operators can
see whether the fleet is mostly waiting on:

- full-text rebuild inputs
- dense or sparse vector materialization
- chunk preview or chunk embeddings
- rerank-term generation

## Enrichment Policy Knobs

Namespace policy now has explicit model/fallback controls for enrichment.

Current operator-facing fields are:

- `enrichment_enabled`
- `enrichment_batch_size`
- `enrichment_failure_policy`
  - `skip_document`
  - `fail_stage`
- `lexical_sparse_model_preference`
  - `deterministic_only`
  - `prefer_model`
  - `require_model`
- `chunk_preview_enabled`
- `chunk_embeddings_enabled`
- `chunk_embeddings_model_preference`
  - `deterministic_only`
  - `prefer_model`
  - `require_model`
- `rerank_terms_enabled`

The intended meaning is:

- `deterministic_only`
  - never call a model for that stage
- `prefer_model`
  - try the configured model, but fall back if it is unavailable or errors
- `require_model`
  - treat model absence or model failure as a stage error
- `skip_document`
  - a failed document is skipped and the stage continues
- `fail_stage`
  - a failed document stops the active stage for that runtime tick

### Policy Example

This policy enables all current stages, prefers a sparse model, requires a
chunk embedding model, and fails the stage if chunk embeddings cannot be
produced:

```json
{
  "default_query_view": "published",
  "compaction_enabled": true,
  "vector_distance_metric": "cosine",
  "enrichment_enabled": true,
  "enrichment_batch_size": 64,
  "enrichment_failure_policy": "fail_stage",
  "lexical_sparse_model_preference": "prefer_model",
  "chunk_preview_enabled": true,
  "chunk_embeddings_enabled": true,
  "chunk_embeddings_model_preference": "require_model",
  "rerank_terms_enabled": true
}
```

### Runtime Example With Models

```bash
export ANTFLY_SERVERLESS_ARTIFACTS_URI=s3://antfly/artifacts/dev
export ANTFLY_SERVERLESS_MANIFESTS_URI=s3://antfly/manifests/dev
export ANTFLY_SERVERLESS_WAL_URI=s3://antfly/wal/dev
export ANTFLY_SERVERLESS_PROGRESS_URI=s3://antfly/progress/dev
export ANTFLY_SERVERLESS_CATALOG_URI=s3://antfly/catalog/dev

export ANTFLY_SERVERLESS_EMBEDDING_INDEXES_JSON=/path/to/indexes.json
export ANTFLY_SERVERLESS_SPARSE_EMBEDDING_INDEX_NAME=serverless_sparse
export ANTFLY_SERVERLESS_CHUNK_EMBEDDING_INDEX_NAME=serverless_chunk
export ANTFLY_SERVERLESS_CHUNK_EMBEDDING_DIMS=768

zig build install-antfly
./zig-out/bin/antfly serverless maintenance
```

The maintenance runtime will then:

- use the configured sparse embedder for lexical sparse enrichment when policy
  allows it
- use the configured dense embedder for `chunk_embeddings` when policy allows
  it
- report model-backed, fallback, and failure counts through `GET /metrics`

For deployment verification, `/status` and startup logs expose backend
summaries with:

- lane name
- original URI
- parsed backend kind: `file`, `s3`, or `gs`
- parsed path or bucket/prefix

## Operator And Deployment Packaging

Kubernetes is a control substrate. Object storage is the durable data substrate.
The operator should manage fleets and config, not data ownership.

A good first self-hosted/serverless-shaped deployment is:

- edge proxy / ingress
- query deployment
- maintenance deployment
- object storage
- small control-plane metadata store if needed
- operator-managed config and rollouts

This is closer to turbopuffer's public shape than to a hosted replica-placement
system.

The best near-term architecture to emulate is turbopuffer first: stateless
query/index workers over object storage, immutable manifests/artifacts, and
local cache plus object-store durability. Larger serverless search systems can
be useful references later for separate search/indexing fleets, stronger cache
hierarchies, advanced compaction, and richer operational isolation.

The operator should manage:

- image versions and rollout strategy
- worker fleet sizing
- secrets for `s3://` and `gs://` backends
- bootstrap URI config
- autoscaling policy
- readiness/liveness wiring
- storage class choices for local cache volumes
- service and ingress wiring

It should not manage:

- shard placement
- sticky namespace ownership
- replica repair workflows
- split / merge orchestration

Before operator work becomes serious, the runtime should separate into
first-class roles:

- query pods
  - serve read traffic
  - keep local SSD/NVMe cache
  - do not run maintenance loops
- maintenance pods
  - publish new versions
  - compact artifacts
  - prune WAL/history
  - rely on CAS/progress coordination rather than ownership
- optional ingest/API pods
  - accept writes
  - append WAL
  - expose table APIs and internal namespace APIs

The current in-process maintenance loop is acceptable for development, but it
is not the long-term operator packaging model.

Use a thin proxy. Good proxy responsibilities are TLS, auth, rate limiting,
tenant routing headers, request logging, ingress policy, and possibly
cache-locality hints for query workers. Do not make the proxy responsible for
namespace state, publication decisions, or retention logic.

### Deployment Modes

Development:

- one combined pod
- `file://` backends
- no operator required

Self-hosted small:

- one API deployment
- one query deployment
- one maintenance deployment
- MinIO / S3-compatible object storage
- optional Postgres-like metadata store if needed later

Cloud object store:

- `s3://...` for S3, MinIO, or Cloudflare R2 through the S3-compatible layer
- `gs://...` for GCS JSON API

### Suggested CRDs

If an operator is introduced, a reasonable first shape is:

- `ServerlessProject`
  - top-level deployment object
- `ServerlessNamespacePolicy`
  - optional namespace defaults / quotas / retention

`ServerlessProject` would define:

- artifact/manifests/WAL/progress/catalog URIs
- backend credentials secret refs
- optional embedding index config source
- query replica counts
- maintenance replica counts
- ingest/API replica counts
- cache volume configuration
- autoscaling settings

`ServerlessNamespacePolicy` should also carry enrichment-stage controls, not
just retention and freshness. It should project directly into the serverless
namespace policy surface instead of inventing a second enrichment-control model.

Example namespace policy:

```yaml
apiVersion: antflydb.io/v1alpha1
kind: ServerlessNamespacePolicy
metadata:
  name: docs
spec:
  defaultQueryView: published
  enrichmentEnabled: true
  enrichmentBatchSize: 64
  enrichmentFailurePolicy: fail_stage
  lexicalSparseModelPreference: prefer_model
  chunkPreviewEnabled: true
  chunkEmbeddingsEnabled: true
  chunkEmbeddingsModelPreference: require_model
  rerankTermsEnabled: true
```

### Maintenance Deployment Env

The maintenance runtime is where model-backed enrichment belongs. A typical
deployment env block should include:

```yaml
env:
  - name: ANTFLY_SERVERLESS_ROLE
    value: maintenance_only
  - name: ANTFLY_SERVERLESS_ARTIFACTS_URI
    value: s3://antfly/artifacts/prod
  - name: ANTFLY_SERVERLESS_MANIFESTS_URI
    value: s3://antfly/manifests/prod
  - name: ANTFLY_SERVERLESS_WAL_URI
    value: s3://antfly/wal/prod
  - name: ANTFLY_SERVERLESS_PROGRESS_URI
    value: s3://antfly/progress/prod
  - name: ANTFLY_SERVERLESS_CATALOG_URI
    value: s3://antfly/catalog/prod
  - name: ANTFLY_SERVERLESS_EMBEDDING_INDEXES_JSON
    valueFrom:
      secretKeyRef:
        name: antfly-embedding-indexes
        key: indexes.json
  - name: ANTFLY_SERVERLESS_SPARSE_EMBEDDING_INDEX_NAME
    value: serverless_sparse
  - name: ANTFLY_SERVERLESS_CHUNK_EMBEDDING_INDEX_NAME
    value: serverless_chunk
  - name: ANTFLY_SERVERLESS_CHUNK_EMBEDDING_DIMS
    value: "768"
```

### Operator Signals

The operator/proxy story should lean on the existing serverless surfaces:

- `GET /health`
  - compact liveness/readiness
- `GET /status`
  - bootstrap/runtime backend view
- `GET /metrics`
  - machine-readable runtime and namespace counters

Those endpoints are enough to build first-pass readiness probes, liveness
probes, operator reconciliation checks, and basic dashboards. The most useful
operator signals are the publish reason, document publish-mode visibility,
derived-output action/resolution summaries, pending materialization families,
enrichment stage source/state, and per-index materialization blockers described
above.

Recommended packaging order:

1. finish indexed query/cache/compaction work inside
   `pkg/antfly/src/serverless/`
2. split runtime roles into query vs maintenance
3. keep coordination ownerless and CAS-driven
4. then package with an operator and thin proxy

## Missing Abstractions

The main gap is that the current abstraction work is still centered on "KV
backend with transactions and snapshots."

A serverless system needs new top-level interfaces above that layer.

## New Core Interfaces

Introduce these as first-class interfaces under
`pkg/antfly/src/serverless/`.

### `ArtifactStore`

Purpose:

- store immutable blobs in object storage
- fetch whole objects or byte ranges

Suggested responsibilities:

- `put(blob) -> artifact_id`
- `get(artifact_id)`
- `getRange(artifact_id, offset, len)`
- integrity metadata

The transport seam beneath this should now be a dedicated
`lib/objectstore/` package rather than ad hoc `file://` handling. That package
needs to cover the MinIO/S3-compatible feature surface already used by the Go
repository:

- bucket existence and creation
- object put / get / stat / delete / list
- file upload and download helpers
- ranged reads
- multipart/object-attribute inspection for large downloads
- endpoint parsing and environment credential fallback
- `s3://bucket/key` and `s3://endpoint/bucket/key` parsing

### `WalStore`

Purpose:

- append write batches for a namespace
- stream batches from a known LSN / offset

Suggested responsibilities:

- `append(namespace, batch) -> lsn`
- `readFrom(namespace, lsn)`
- truncate / retention hooks later

### `ManifestStore`

Purpose:

- store immutable versioned manifests

Suggested responsibilities:

- `putManifest(version, manifest)`
- `getManifest(version)`

### `NamespaceProgressStore`

Purpose:

- track shared namespace publication and retention progress

Suggested responsibilities:

- `getHead(namespace) -> version`
- `compareAndSwapHead(namespace, expected, next)`
- `getGcWatermark(namespace) -> ?lsn`
- `compareAndSwapGcWatermark(namespace, expected, next)`

This is the critical coordination seam for new builds and safe WAL retention.
Workers do not permanently own a namespace. They race to publish or prune, and
shared compare-and-swap state decides which result becomes visible.

## Current Object-Storage Implementation Direction

The current serverless path should use objectstore-backed adapters at the
remote-storage boundary:

- `pkg/antfly/src/serverless/artifacts/object_store.zig`
- `pkg/antfly/src/serverless/wal/object_store.zig`
- `pkg/antfly/src/serverless/manifest/object_store.zig`
- `pkg/antfly/src/serverless/catalog/object_progress_store.zig`

`file://` remains the local/shared-filesystem stand-in, but it should flow
through the same objectstore contract as future S3-compatible backends.

## Current Namespace Policy Knobs

Serverless namespaces now need explicit policy, not just default query mode.

- `keep_latest_versions`
  - retention target for manifest history
- `max_pending_records`
  - backpressure threshold for unpublished WAL tail
- `compaction_trigger_version_count`
  - threshold where retained history should be treated as needing compaction

These knobs should remain namespace-scoped and query/build status should expose:

- current unpublished WAL depth
- whether new ingest is admitted
- retained version count
- retained artifact count
- whether compaction is recommended

### `QueryCache`

Purpose:

- manage local on-disk / memory caching of fetched artifacts

Suggested responsibilities:

- `openArtifact(artifact_id)`
- pin / unpin
- eviction
- local materialization of read-friendly views

## Recommended Repository Layout

Add a parallel path instead of mutating the current stateful path in place.

Suggested new top-level area:

- `pkg/antfly/src/serverless/artifacts/`
- `pkg/antfly/src/serverless/wal/`
- `pkg/antfly/src/serverless/manifest/`
- `pkg/antfly/src/serverless/catalog/`
- `pkg/antfly/src/serverless/build/`
- `pkg/antfly/src/serverless/query/`
- `pkg/antfly/src/serverless/api/`

The existing stateful path remains:

- `pkg/antfly/src/raft/*`
- `pkg/antfly/src/metadata/*`
- `pkg/antfly/src/storage/db/*`

The shared engine pieces remain reusable:

- `pkg/antfly/src/search/*`
- `lib/vector/src/*`
- `lib/vectorindex/src/*`
- `pkg/antfly/src/index.zig`
- `pkg/antfly/src/segment.zig`
- `pkg/antfly/src/section/*`
- selected parts of `pkg/antfly/src/storage/lsm/*`

## What To Reuse Directly

### Keep As Engine Code

- query execution
- text retrieval
- vector search and quantization
- fusion logic
- segment-level artifact reading and searching

### Reuse As Supporting Local Runtime

- backend abstraction modules
- LSM implementation for local cache and staging
- conformance-style tests as local cache backend tests

### Treat As Reference, Not Foundation

- DocStore layout
- split / merge state machine
- local transaction manager
- hosted multi-Raft runtime

If the product keeps stronger table semantics, those systems may still remain
the source of truth for writes. The serverless path should avoid dragging their
runtime assumptions directly into the published read plane.

## What Not To Force Into The New Architecture

Do not force object storage to behave like LMDB.

Do not start with:

- cross-range distributed transactions
- split / merge workflows
- persistent query-node ownership
- replica repair and rebalance logic
- graph distribution semantics

These belong to the current stateful architecture, not the first serverless
version.

That is an implementation constraint for the first serverless tranches. It does
not mean the final cheap hosted Antfly offering must expose a completely
different user model or give up table-level metadata and lifecycle semantics.

## Migration Order

Build the serverless path in phases.

### Phase 1: Read-Only Immutable Namespace

Goal:

- serve reads from immutable artifacts and a manifest

Work:

- define manifest schema
- define artifact storage schema
- create a builder that emits one namespace version
- create a query path that loads one manifest and answers search requests

No WAL yet.
No async ingest yet.
No control plane beyond static config.

### Phase 2: Object-Storage-Backed Query Path

Goal:

- query workers fetch artifacts from object storage and cache them locally

Work:

- implement `ArtifactStore`
- implement local cache manager
- add artifact fetch, validation, and eviction
- support lazy manifest-driven loading

At this point the read path becomes meaningfully stateless.

### Phase 3: Append-Only Ingest

Goal:

- decouple write acknowledgment from index publication

Work:

- implement `WalStore`
- implement ingest API
- acknowledge writes by LSN or version token
- add a background builder that consumes WAL and produces the next manifest

This is the first real serverless write path.

### Phase 4: Publication and Freshness Semantics

Goal:

- define what "read after write" means

Start with two read modes:

- `indexed`
  - reads the latest published manifest only
- `latest`
  - reads the latest manifest plus a small WAL tail overlay

This keeps correctness and product semantics explicit.
If stronger transactional or point-read semantics are needed, keep them on the
canonical write plane rather than teaching immutable published artifacts to act
like a general-purpose transactional store.

### Phase 5: Real Control Plane

Goal:

- productionize namespace management and build orchestration

Work:

- internal namespace lifecycle APIs
- auth and quota enforcement
- manifest head publication
- namespace progress tracking
- opportunistic worker scheduling
- job tracking and retries

This replaces shard placement with namespace/version management.
If serverless becomes a public deployment mode of Antfly, this layer should also
own the mapping between user-facing tables and internal published serving units.

### Phase 5.5: Object-Store Remote Adapters

Goal:

- move the serverless remote boundary onto the shared objectstore contract

Work:

- keep objectstore-backed adapters for artifacts, WAL, manifests, and progress
  behind the shared objectstore seam
- keep `file://` as the initial shared-filesystem backend
- make serverless remote adapters depend on `lib/objectstore/`
- preserve the current serverless store interfaces above that seam

Acceptance:

- remote serverless tests pass through objectstore-backed adapters
- no remote serverless path directly depends on raw filesystem layout
- objectstore covers the MinIO/S3-compatible features the Go repo already uses,
  including R2 through the same compatibility path

### Phase 5.6: Namespace Admission and Compaction Policy

Goal:

- keep explicit namespace-level operational controls in place before scaling
  runtime loops

Work:

- enforce `max_pending_records`
- expose `compaction_trigger_version_count`
- report retained versions, retained artifacts, unpublished WAL depth, and
  whether compaction is recommended

Acceptance:

- ingest can reject backpressured namespaces
- build status reports retained history and admission state
- compaction recommendation is visible without reading internal logs

### Phase 6: Advanced Features

Only after the above is stable, consider:

- hybrid dense + sparse optimization
- serverless graph over published snapshots
- incremental reranking support
- richer write semantics

The first serverless graph phase should be immutable and published-version
oriented:

1. use `graph_segment` artifacts in
   `pkg/antfly/src/serverless/graph_segment/`
2. publish graph artifacts from WAL-materialized document state
3. harden pinned-version graph reads in `pkg/antfly/src/serverless/query/`
4. start with neighbor lookup, edge-type filtering, and published-head or
   explicit-version reads
5. only then consider traversal/path features over immutable artifacts

Do not port the stateful graph engine directly. Reuse graph semantics where
they fit, but keep the storage format immutable and object-store-friendly.

Do not plan for full replica-coupled stateful transaction parity inside the
serverless published read plane. If the product needs stronger write semantics
later, start with idempotent ingest, expected-head / compare-and-swap writes,
and small namespace-local conditional updates. If the product keeps
transactional table semantics, prefer keeping those on an upstream write/control
plane and feeding the serverless read plane from committed table-originated
state.

## First End-to-End Milestone

The first meaningful milestone should be:

1. `PUT /tables/{table}/ingest-batch`
   - appends to the table's internal serving WAL
2. background builder
   - consumes WAL
   - emits immutable artifacts
   - publishes manifest `vN`
3. `POST /tables/{table}/query/search`
   - loads manifest `vN`
   - fetches needed artifacts
   - answers from local cache

The corresponding internal serving/debug routes may remain namespace-oriented
under `/internal/v1/namespaces/...`, but they should not define the public product
surface.

Do not include:

- Raft
- shard split / merge
- distributed transactions
- graph distribution
- weighted global topology handling

One namespace, one sequencer, many stateless readers is enough for the first
proof.

## Implementation Backlog

### Tranche 1: Documents and Manifest Model

- keep `pkg/antfly/src/serverless/manifest/types.zig` as the manifest contract
  and harden:
  - namespace id
  - manifest version
  - artifact references
  - doc count / term stats / vector stats
  - build metadata
- keep encoding / decoding tests deterministic

### Tranche 2: Artifact Storage Abstraction

- keep `pkg/antfly/src/serverless/artifacts/store.zig` as the shared interface
  for:
  - filesystem-backed implementation for tests
  - object-store-shaped interface from day one
- continue hardening real cloud implementations behind the same interface

### Tranche 3: Query Runtime

- keep `pkg/antfly/src/serverless/query/runtime.zig` focused on:
  - manifest loader
  - local artifact cache
  - snapshot-like query session pinned to one manifest version
- make the query runtime reuse:
  - `pkg/antfly/src/index.zig`
  - `pkg/antfly/src/search/*`
  - `lib/vector/src/*`
  - `lib/vectorindex/src/*`

### Tranche 4: WAL and Builder

- keep `pkg/antfly/src/serverless/wal/store.zig` and
  `pkg/antfly/src/serverless/build/builder.zig` centered on append-only write
  batches and full rebuild or coarse-grained build
- do not optimize for tiny incremental updates first

### Tranche 5: Minimal API Surface

- keep `pkg/antfly/src/serverless/api/http_routes.zig` focused on:
  - table ingest
  - table query/search
  - table build status
  - internal namespace head/debug routes as needed

Keep `/tables/...` as the public API. Route internal publication, artifact, and
manifest inspection through `/internal/v1/namespaces/...`.

### Tranche 6: Control Plane Skeleton

- keep `pkg/antfly/src/serverless/catalog/service.zig` tracking:
  - namespace records
  - namespace progress state
  - build jobs
  - retention policy

### Tranche 7: Progress-Based Coordination

- keep `pkg/antfly/src/serverless/catalog/progress_store.zig` focused on:
  - filesystem-backed head CAS
  - filesystem-backed GC watermark CAS
- route publish and prune through shared progress state
- allow duplicate build work when only one CAS winner becomes visible

### Current Next-Tranche Detail

The current query path is still centered on manifest-pinned artifact fetch and
document-state materialization. That is correctness scaffolding, not the final
steady state. The next query work should finish the existing request/indexed
reader seam and add the missing search module:

- `pkg/antfly/src/serverless/query/request.zig`
- `pkg/antfly/src/serverless/query/indexed_reader.zig`
- missing: `pkg/antfly/src/serverless/query/search.zig`

The goal is to read from published searchable artifacts, preserve
pinned-manifest semantics, and return ranked hits or bounded document matches
without reconstructing full document state for every request.

Text-only indexed reads are not enough. The serverless path also needs the
existing dense and sparse artifact families to become production query inputs
before operator work starts:

- `pkg/antfly/src/serverless/vector_segment/mod.zig`
- `pkg/antfly/src/serverless/vector_segment/types.zig`
- `pkg/antfly/src/serverless/vector_segment/codec.zig`
- `pkg/antfly/src/serverless/sparse_segment/mod.zig`
- `pkg/antfly/src/serverless/sparse_segment/types.zig`
- `pkg/antfly/src/serverless/sparse_segment/codec.zig`
- `pkg/antfly/src/serverless/document_projection.zig`

Do not copy the mutable HBC storage format directly into serverless. Reuse HBC
and RaBitQ lessons, but publish immutable `vector_segment` and `sparse_segment`
artifact families that are object-storage and cache friendly. Start with exact
or lightly optimized retrieval over immutable published vectors and weighted
sparse postings, while keeping the format evolvable toward centroid / quantized
ANN layouts.

The local query cache should keep becoming explicit and testable:

- `pkg/antfly/src/serverless/query/cache.zig`
- optional missing: `pkg/antfly/src/serverless/query/cache_fs.zig`

Cache behavior should include SSD/NVMe-oriented artifact storage, query-session
pin/unpin behavior, bounded eviction, and checksum-aware validation.

Compaction should keep moving beyond retention:

- `pkg/antfly/src/serverless/build/compactor.zig`
- optional missing: `pkg/antfly/src/serverless/build/segment_plan.zig`

The compactor should rewrite many small published deltas into fewer optimized
artifacts, reduce repeated full snapshot publication, and control artifact
growth without depending only on deletion.

Before multiple maintenance workers are expected to run in anger, harden:

- `pkg/antfly/src/serverless/catalog/progress_store.zig`
- `pkg/antfly/src/serverless/catalog/object_progress_store.zig`
- `pkg/antfly/src/serverless/runtime/manager.zig`

Add concurrent publish/prune tests, crash/retry tests around manifest write plus
head publish, and idempotency checks for prune GC watermark advancement.

Before operator packaging, `pkg/antfly/src/serverless/runtime/manager.zig`
should expose first-class query-only and maintenance-only modes so query pods
can run without maintenance loops and maintenance pods can run without serving
queries.

## Testing Strategy

Do not rely only on low-level backend conformance tests.

Add new tests at these levels:

### Manifest Correctness

- version pinning
- head publication CAS
- artifact-list integrity

### Query Correctness

- same manifest, same results on cold and warm cache
- stale cache invalidation when manifest changes
- pinned query session stays on one manifest version

### Build Correctness

- WAL replay produces deterministic artifacts
- republishing does not expose partial versions
- manifest head changes are atomic
- retention only advances after GC watermark CAS

### End-to-End Freshness

- acknowledged write visible in WAL
- eventually visible in indexed reads
- optional WAL-tail overlay visible in `latest` reads

## Important Tradeoffs

### Preserve Engine Code, Not Stateful Deployment Assumptions

The main reusable value in `antfly-zig` is its search and indexing code. The
main non-reusable part for this effort is the replica-oriented runtime model.

### The Backend Abstraction Is Necessary But Not Sufficient

The current backend abstraction is still too low-level to define the new
architecture by itself. The serverless architecture should be defined in terms
of:

- manifests
- immutable artifacts
- WAL
- publication progress
- cache

not just:

- `get`
- `put`
- `delete`
- `beginRead`
- `beginWrite`

### Avoid Pulling The Whole DB Upward

The current `pkg/antfly/src/storage/db/*` path bundles:

- local ownership
- local maintenance runtimes
- local transactions
- local enrichment execution

That is too opinionated to be the top-level serverless façade.

## Recommended Rule Of Thumb

When deciding whether a current module belongs in the serverless path, ask:

"Is this module fundamentally about search/index execution, or fundamentally
about local durable ownership and replica orchestration?"

If the answer is:

- search/index execution: likely reusable
- local ownership / replica orchestration: likely not foundational

## Initial Scope Limits

To keep the project tractable, the initial serverless version should explicitly
exclude:

- distributed graph search across dynamic ownership
- general-purpose distributed transactions
- split / merge orchestration
- replica placement and repair
- rich topology-aware retry semantics

The initial success condition is much smaller:

- write to WAL
- build immutable artifacts
- publish a manifest
- serve stateless queries from cached artifacts

That is enough to prove the architecture.

## Coordination Rule Of Thumb

Do not introduce long-lived namespace ownership into the serverless path.

Prefer:

- immutable candidate artifacts
- manifest writes before visibility changes
- head publication by compare-and-swap
- GC watermark advancement by compare-and-swap
- opportunistic workers that can safely lose races

Duplicate build work is acceptable. Conflicting visible state is not.
