# Backup and Restore Plan

This file is the execution plan for backup and restore in `antfly-zig`.

Use it to answer:

- what public backup and restore contract Zig should converge on
- which parts already exist in storage vs what is missing in the public API
- what order implementation should move in
- which slices are stateful-only first and which can later be shared with
  serverless

## Contract Source

The public contract target is the finished Go implementation:

- [../antfly/openapi.yaml](/Users/ajroetker/go/src/github.com/antflydb/antfly/openapi.yaml)
  - `/backup`
  - `/restore`
  - `/backups`
  - `/tables/{tableName}/backup`
  - `/tables/{tableName}/restore`
- [../antfly/go/e2e/backup_restore_test.go](/Users/ajroetker/go/src/github.com/antflydb/antfly/go/e2e/backup_restore_test.go)

## Current State

### Already Exists

Low-level DB snapshot and restore primitives already exist in
[db.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/src/storage/db/db.zig):

- `DB.snapshot(id)`
- `DB.restoreSnapshotTo(snapshot_root, path, opts)`

Those primitives already cover:

- logical store export
- derived log export
- durable LSM reopen / restore tests
- text, sparse, and graph index rehydration on restore

### Current Gap

The public stateful API now exposes:

- table-scoped backup and restore routes
- cluster `/backup`, `/restore`, and `/backups` routes
- manifest handling for table and cluster backup artifacts over `file://`,
  `s3://`, and `gs://` locations
- `gcs://` accepted as a compatibility alias for `gs://`

So the remaining gap is no longer basic route presence. The remaining gap is
full Go-parity verification, broader operational coverage, and backend depth
such as store/operator flows beyond the public API.

## Design Rules

- The Go OpenAPI remains the public contract source.
- Zig must use the same public request and response structure as the Go
  implementation.
- Shape parity comes first; backend/support parity can arrive in stages.
- If a Go-shaped request uses a backend or mode Zig does not support yet, return
  a clear error instead of inventing a Zig-only request shape.
- Start table-scoped before cluster-scoped.
- Start local-filesystem first before object-store backends.
- Keep the first-cut synchronous implementation testable, but make the
  production contract asynchronous: restore requests validate and record durable
  restore intent quickly, while shard bootstrap and derived-index catch-up run
  outside the public HTTP request.
- Use the existing DB snapshot/restore primitives instead of inventing a second
  storage format.
- Keep stateful Raft/control-plane as the canonical owner first.
- Only bring backup/restore into serverless once the stateful public contract is
  stable.

## First-Cut Scope

The first useful tranche should be table-scoped and local-filesystem only.

### In Scope

- `POST /tables/{tableName}/backup`
- `POST /tables/{tableName}/restore`
- `file://...` locations only
- metadata + shard snapshot export for one table
- restore into a target table name
- restore modes needed for a safe first cut:
  - `fail_if_exists`
  - `overwrite`

### Explicitly Out of Scope For First Cut

- S3/object-store locations
- background async restore orchestration
- serverless backup/restore
- backup of live cross-table transactional consistency across many tables in one
  operation

## Architecture Shape

### Table Backup

Table backup should be a thin public API over:

1. metadata snapshot for the table
2. per-range/per-group local DB snapshot export
3. a table backup manifest written to the target location

The table backup artifact should contain:

- table metadata
  - schema
  - `read_schema` / migration metadata
  - index definitions
  - shard/range layout needed for restore
- one snapshot payload per participating shard/group
- a table-level manifest that maps table metadata to shard snapshot locations

### Table Restore

Table restore should eventually be an asynchronous table lifecycle operation.
The public request should validate and accept restore work, then return the
Go-shaped `202` response without waiting for full-text, vector, graph, or
generated-enrichment indexes to drain.

The production restore flow is:

1. parse the request body and validate the backup location
2. enforce table restore error precedence before manifest I/O:
   - if the target table already exists, return `400 restore target already exists`
   - only then read and validate the backup manifest
3. create durable restore intent in metadata, including:
   - target table name and table id
   - `backup_id`
   - backup location
   - per-range `snapshot_path`
   - restore phase/progress records
4. return `202 {"restore":"triggered"}` once metadata has accepted the intent
5. let placement/bootstrap workers restore shard snapshots into replica storage
6. write a local per-shard `.restore-state` marker after primary data restore
   completes, then reopen restored DBs and rebuild/replay derived state in
   background
7. clear restore intent and mark the table ready only after required shard
   restore progress and managed-index readiness have been observed

The local shard marker format is intentionally versioned and breaking while the
restore API is not yet used in production. `restore_state_v2` records:

- `backup_id`
- `location`
- `snapshot_path`
- `group_id`
- `phase`
- `primary_restored`
- `runtime_repair_complete`
- `last_error`

`primary_restored=true` means the logical store contents have been copied into
place. It does not mean the table is query-ready. Metadata restore progress must
keep the table restore intent active until every required placement reports
`runtime_repair_complete=true`.

Snapshot import also has a crash-recovery marker. `restore_import_v1` must be
written before primary-store import begins and must include `snapshot_root`,
`backup_id`, `location`, `snapshot_path`, and `group_id`. If the process dies
before `.restore-state` is written, startup recovery replays the primary-store
import from `snapshot_root` and then writes `restore_state_v2` with the same
identity. Restore code must not synthesize an empty restore identity during this
path, because metadata progress matching depends on the backup/table identity.

Runtime repair is resumable by phase. The current phase sequence is:

- `runtime_repair` / `reset_watermarks`
- `rebuild_graph`
- `rebuild_artifacts`
- `replay_enrichments`
- `drain_async`
- `sync_indexes`
- `complete`

The repair worker must not rely on a wall-clock timeout around the whole repair.
It should advance one durable phase at a time, yield between attempts, and allow
shutdown/deinit to observe bounded progress instead of blocking on a single
untracked restore operation.

#### Restore Status and Readiness

Restore state must be visible through the table status surface, not only through
the response to the initial restore request. Operators and clients need a stable
place to poll after the `202` response returns.

The table lifecycle should distinguish at least:

- `creating`
- `ready`
- `restoring`
- `deleting`
- `failed`

While a table is `restoring`, status should include restore-specific details:

- `backup_id`
- restore phase, for example:
  - `accepted`
  - `runtime_repair`
  - `rebuild_graph`
  - `rebuild_artifacts`
  - `replay_enrichments`
  - `drain_async`
  - `sync_indexes`
  - `ready`
  - `failed`
- started/completed timestamps where available
- last error string when failed
- per-shard progress from restore progress records
- per-index readiness/catch-up state from managed-index runtime status

Read behavior should be explicit while restore is in progress:

- primary-key lookup and scan may become available once the primary store is
  restored
- full-text, vector, sparse, graph, and generated-enrichment queries should
  either return a clear restoring/not-ready response or report degraded index
  readiness until their required indexes catch up
- when all required shards and indexes are ready, the table lifecycle becomes
  `ready`

Cluster restore should continue to return per-table trigger/skip/failure
statuses, but those statuses are an admission result. The durable source of
truth after admission is the per-table lifecycle and restore status.

The current metadata model already has restore intent fields on table/range
records, projected restore progress records, and restore-pending readiness
overlay. The remaining production hardening is to make public table restore use
that durable asynchronous path consistently and to remove synchronous
`runUntilIdle`/derived-index drain from the HTTP critical path.

### Bootstrap Model

Backup restore should not be modeled as ordinary Raft peer snapshot transfer.

Zig now needs two distinct bootstrap source kinds:

- `raft_snapshot_fetch`
  - used for real Raft replica catch-up from another node
  - carries a Raft snapshot locator and transport source node
- `backup_db_snapshot_restore`
  - used for backup/restore provisioning from backup artifacts
  - carries:
    - `backup_id`
    - backup `location`
    - per-range `snapshot_path`

Production replica/bootstrap metadata should carry that source explicitly
instead of only a mode bit. Metadata restore intent already carries the
range-scoped backup source, and both raw and managed hosts now consume
`backup_db_snapshot_restore` directly before replica startup, including catalog
replay on restart, instead of pretending backup restore is a Raft peer snapshot
fetch. The remaining gap is not bootstrap routing anymore; it is deeper runtime
coverage and operational hardening around the explicit backup bootstrap source.

Metadata/operator status now also reports how many projected placement intents
are waiting on Raft snapshot bootstrap vs backup restore bootstrap, so the
bootstrap mix is visible without inspecting every placement record manually.

Host/runtime status now also exposes per-group backup bootstrap progress for
the explicit backup path:

- kind
- phase:
  - `preparing`
  - `succeeded`
  - `failed`
- attempt count
- last update time
- last error string when bootstrap fails
- source fields for backup restore:
  - `backup_id`
  - `snapshot_path`

That status is owned by `raft.Host` and is surfaced through `HttpHost`,
`ManagedHost`, and `ManagedHttpHost`, so the explicit backup source handling is
shared across raw, HTTP, managed, and managed-HTTP host entrypoints instead of
being reimplemented in each wrapper. The metadata admin snapshot now also
includes those local bootstrap status records, so operators can see concrete
per-group restore/bootstrap state next to placement intents and restore
progress.

## Stages

### Stage 0: Contract and Format Definition

Goal:
- define the Zig table backup manifest and public request/response types

Deliverables:

- request/response Zig types matching Go table backup/restore shapes
- local filesystem URI parsing rules for `file://...`
- table backup manifest format doc
- explicit mapping from Go contract fields to Zig implementation fields

Notes:

- do not start with cluster manifests
- do not start with S3

### Stage 1: Table Backup API

Goal:
- implement `POST /tables/{tableName}/backup` for stateful Zig

Deliverables:

- route match in `src/api/http_routes.zig`
- handler in `src/api/http_server.zig`
- backup service module, likely under `src/api` or `src/metadata`
- metadata lookup for the target table
- per-shard snapshot export using existing `DB.snapshot(...)`
- manifest + metadata file output under a `file://...` location
- public success and not-found/error responses

Suggested first behavior:

- synchronous request that completes after files are written
- `201` response with a small Go-shaped body

### Stage 2: Table Restore API

Goal:
- implement `POST /tables/{tableName}/restore` for stateful Zig

Deliverables:

- route match in `src/api/http_routes.zig`
- handler in `src/api/http_server.zig`
- restore service module
- manifest load/validation
- metadata creation or replacement for the target table
- local DB restore via `DB.restoreSnapshotTo(...)`
- shard readiness/reopen verification

Suggested first behavior:

- synchronous restore for local filesystem input
- `202` Go-shaped response if we want to preserve the future async shape
- actual implementation may still complete inline in the first cut

Status:

- shipped for `file://`, `s3://`, and `gs://` single-range tables
- restore remains table-scoped and fail-if-exists, matching the Go table API

### Stage 3: Cluster Backup and Restore

Goal:
- implement `/backup`, `/restore`, and `/backups`

Deliverables:

- cluster backup manifest format
- table backup aggregation
- restore-mode handling across many tables:
  - `fail_if_exists`
  - `skip_if_exists`
  - `overwrite`
- backup listing over a location

Notes:

- this is a metadata/control-plane feature first
- first cut may report per-table failure for unsupported multi-range tables

Status:

- `/backup`, `/restore`, and `/backups` are implemented for stateful backups
  over `file://`, `s3://`, and `gs://`
- cluster backup writes a cluster manifest plus per-table manifests under the same location
- restore modes follow the Go cluster contract
- unsupported table layouts still fail per-table instead of restoring

### Stage 4: Stateful E2E and Parity Hardening

Goal:
- prove the table API against the Go contract

Deliverables:

- `e2e/test_backup_restore.py`
- local-filesystem backup/restore round-trip
- cluster backup/list/restore round-trip
- restore into missing target table
- overwrite behavior
- index/search validation after restore
- graph/text/sparse reopen checks as applicable

Recommended first E2E order:

1. simple table with documents only
2. table with full-text index
3. cluster backup/list/restore
4. table with graph/text/sparse state as restore confidence grows

Status:

- public Zig parity coverage now includes:
  - table backup/restore route coverage
  - cluster backup/list/restore round-trip
  - cluster restore modes `fail_if_exists`, `skip_if_exists`, and `overwrite`
  - cluster partial success reporting for mixed valid and invalid table sets
  - cluster partial success reporting for unsupported multi-range tables
  - table backup rejection while schema migration is still rebuilding
  - table restore rejection for backup manifests that still carry migration-state metadata
  - table restore rejection when the target already exists
  - table restore rejection for manifest/table-name mismatches
  - public request validation for malformed backup/restore bodies and unsupported locations
  - `/backups` validation for missing or unsupported locations
  - cluster restore rejection for invalid `restore_mode`
  - managed embeddings backup/restore with index status and semantic query checks
  - managed sparse embeddings backup/restore with index status and sparse query checks
  - chunked managed embeddings backup/restore with chunk artifact checks
  - graph backup/restore with graph index status and query checks
- the remaining gap is broader Go-contract coverage breadth, not basic restore viability

### Stage 5: Object Store Backends

Goal:
- support S3/object-store backup locations

Deliverables:

- backend-neutral backup IO seam
- `file://...` implementation stays the reference path
- object-store URI parsing
- manifest + shard payload upload/download paths

Notes:

- keep this separate from the core backup format work
- the first implementation should remain testable entirely with local files

### Stage 6: Serverless Integration

Goal:
- define how serverless interacts with backup/restore once the stateful API is
  real

Likely shape:

- serverless reuses the same public table contract
- actual backup source of truth comes from canonical table metadata and backing
  storage
- published generations/manifests are not the first restore primitive

Do not start this before the stateful API is stable.

## Recommended Execution Order

1. Stage 0: request/response + manifest definition
2. Stage 1: stateful table backup
3. Stage 2: stateful table restore
4. Stage 3: E2E parity coverage
5. Stage 4: cluster backup/restore
6. Stage 5: object-store backends
7. Stage 6: serverless integration

## Concrete Next Checklist

- [x] Define table backup/restore Zig request and response types from Go OpenAPI.
- [x] Add route placeholders for table backup and restore.
- [x] Define a local-filesystem `file://...` location parser and validator.
- [x] Define the table manifest format and where it lives on disk.
- [x] Identify how table metadata should be serialized for restore.
- [x] Implement synchronous table backup for one table.
- [x] Implement synchronous table restore for one table.
- [x] Add a standalone public backup/restore parity test outside the current Zig in-tree e2e suite.
- [ ] Deepen cluster backup/list/restore parity coverage against the Go e2e contract.

Current external-suite note:
- request/validation parity now runs in `e2e/test_backup_restore.py`
- split `metadata` + `data` round-trip coverage now runs there as a real passing gate, including table backup/restore, cluster backup/restore, restore modes, and partial-status reporting

## Main Risks

- metadata restore may drift from actual DB/index snapshot contents if the table
  manifest is underspecified
- cluster backup before table backup is stable will multiply control-plane and
  async complexity
- object-store support too early will blur storage-format problems with
  transport problems
- serverless-first backup design will likely conflict with the canonical
  mutable-table contract

## Non-Goals For The First Cut

- full Go parity in one step
- background restore orchestration
- provider-backed semantic embedding rebuild during restore
- perfect shared implementation across stateful and serverless on day one
