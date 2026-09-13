# Backup and Restore

This document describes the backup and restore design in `antfly-zig`: the
public contract, the manifest/blob storage model, and how table and cluster
backup/restore work today.

Use it to answer:

- what public backup and restore contract Zig implements
- how the manifest/blob storage model works
- which slices are stateful-only today and which remain to extend to
  serverless

## Contract Source

The public contract target is the finished Go implementation:

- [../antfly/openapi.yaml](../openapi.yaml)
  - `/backup`
  - `/restore`
  - `/backups`
  - `/tables/{tableName}/backup`
  - `/tables/{tableName}/restore`
- `go/e2e/backup_restore_test.go` (removed with the Go server; see [zig/e2e/antfly](e2e/antfly))

## Implementation State

### Storage Primitives

Low-level DB snapshot and restore primitives already exist in
[db.zig](pkg/antfly/src/storage/db/db.zig):

- `DB.snapshot(id)`
- `DB.restoreSnapshotTo(snapshot_root, path, opts)`

Those primitives already cover:

- logical store export
- derived log export
- durable LSM reopen / restore tests
- text, sparse, and graph index rehydration on restore

### Public API Surface

The public stateful API exposes:

- table-scoped backup and restore routes
- cluster `/backup`, `/restore`, and `/backups` routes
- manifest handling for table and cluster backup artifacts over `file://`,
  `s3://`, and `gs://` locations
- `gcs://` accepted as a compatibility alias for `gs://`

The remaining gap is not basic route presence; it is full Go-parity
verification, broader operational coverage, and backend depth such as
store/operator flows beyond the public API (see [Open work](#open-work)).

## Design Rules

### Canonical repository and bundle layers

The canonical backup is not a tar/ZIP tree. It is a manifest DAG over immutable
content-addressed blobs:

```text
refs/<backup-id>          -> root manifest SHA-256
manifests/<sha256>        -> immutable canonical snapshot manifest
seals/<manifest-sha256>    -> immutable receipt-complete proof
blobs/sha256/<storage-sha256> -> immutable artifact bytes
```

Every manifest declares table/catalog identity, an explicit `portable` or
`native` representation, shard ranges, capture/checkpoint revisions,
compatibility requirements, compression/encryption metadata, and two complete
inventories:

- `objects`, sorted by logical path, map each restorable path and semantic role
  to a blob digest. Shards list object paths, not bare digests.
- `blobs`, sorted by content digest, carry separate content and stored-byte
  digests and describe each unique stored representation once.
  Several objects may reference one blob without losing either path.

That split is required for native restore: a digest-only inventory can prove
bytes exist but cannot reconstruct their filenames. Manifest validation rejects
unsafe/duplicate paths, dangling object references, size disagreement, missing
catalog identity, non-canonical ordering, and shard membership outside the
complete object inventory.

A delta uploads only blobs absent from its parent, but its manifest still lists
the complete current object and blob inventories. The parent digest is therefore
lineage, accounting, and reachability information—not a chain that ordinary
repository restore must replay. A repository restore resolves its ref once,
pins that immutable manifest digest, downloads each unique blob once into a
private staging cache, rehashes it, materializes all logical objects, and only
then permits the owner to publish the generation.

Refs are published with compare-and-swap semantics through a fenced publication
session. The session first advances the repository epoch and installs a
durable, renewable lease naming the candidate digest and any exact incremental
base. Each newly required blob upload returns
a backend-issued receipt bound to the session fence and to the verified object
generation. Finalization accepts the complete receipt set, writes an immutable
completion seal, consumes the manifest-bound lease, and conditionally publishes
the ref while holding the repository coordinator.
It does not issue one remote existence request per blob.

Local garbage collection marks manifests, seals, and blobs reachable from refs
and active leases at one repository epoch, then applies a grace period before
deletion.
Every lease/ref transition advances that epoch and every deletion rechecks it
under the same coordinator. A publication that races an old mark therefore
forces that sweep to restart instead of deleting its candidate artifacts.
Failed writers cannot expose partial snapshots, stale writers are fenced, and
concurrent writers cannot silently replace one another. Parent links are
informational lineage, not retention edges: complete child inventories directly
retain every stored blob they need, avoiding unbounded ancestor retention.
Remote coordinator owners refresh through an ETag compare-and-swap around
control mutations, and epoch updates are themselves conditional writes. Remote
immutable storage is deliberately append-only: a renewable time lease cannot
fence an owner paused for an arbitrary duration between refresh and delete, and
a content-addressed object may be republished with the same ETag. Remote sweep
requires a future transactional catalog that first tombstones an exact storage
generation, lets publishers pin or replace it, and then deletes only that
version. Until then, remote cleanup may retain unreachable bytes but can never
delete bytes reachable by a backup.

Publication order is part of the durability contract:

1. validate a full capture or a typed committed base whose digest is recomputed
   from its canonical manifest and whose immutable completion seal matches;
2. advance the epoch, activate the fenced lease, and then write the candidate
   manifest immutably (a lease whose manifest is temporarily missing makes GC
   abort and retry);
3. stream only newly required `blobs/sha256/<storage-sha256>` objects with
   create-if-absent semantics. Each digest-sorted blob entry names one
   path-sorted representative logical object with the same digest and size,
   making source lookup bounded (`O(log objects)` per blob) while the source
   file generation remains pinned and the stored object generation is
   post-verified;
4. validate the backend receipts, write the completion seal, consume the lease,
   and conditionally update `refs/<backup-id>` with the expected prior digest
   and generation.

A crash before step 4 leaves unreachable immutable content but no visible
partial backup. Competing writers cannot silently replace one another. GC marks
from live refs and unexpired restore/export leases, retains an active delta's
exact base proof while publication is in flight, and deletes only unmarked
objects older than a grace cutoff from the same stable repository epoch.
Stable epochs are even. A backend writes the next odd epoch before changing a
lease or ref and the following even epoch after the new root set is durable.
Mark and sweep take the repository coordinator and reject odd epochs; if a
writer crashed, the next coordinator owner advances the abandoned odd value to
an even value before GC can enumerate roots. This prevents an epoch-first lease
activation or renewal from briefly looking like a complete namespace.

`.afb` is the transport layer over that model:

- AFB1 remains readable as the released v0.2.0 portable format.
- AFB2 has a representation-neutral root manifest with the same logical-object
  and unique-blob split. Each included digest has exactly one physical blob
  record, even when several paths or portable records share its bytes. Payloads
  use bounded chunks, the footer maps each digest to its byte offset, and a
  fixed-size checksummed trailer locates that footer without scanning payloads.
- AFB2 `full` is self-contained and is the normal offline/superquickstart
  artifact.
- AFB2 `delta` accepts one typed exact base containing the immutable canonical
  base manifest and its digest; callers cannot independently assemble a parent
  digest and a blob inventory. Writers recompute that identity before omitting
  bytes. Both native and portable readers require the same exact matching base,
  rehash every supplied base blob, and reject missing or mismatched bases.
  Delta manifests remain complete inventories; only their physical payload is
  partial.
- Native and portable are manifest values, never inferred from file extension
  or CLI flags during restore.
- The AFB2 root manifest is sealed source identity. Import validates it before
  copying any artifact and never rewrites it for a renamed restore. Native
  staging instead derives a target-scoped restore envelope: the target table
  name changes, while backup id, generation, object inventory, digests, and
  compatibility facts remain identical. The ordinary table-restore path then
  consumes that envelope, so restoring `source_docs` as `restored_docs` has the
  same atomic publication and identity checks as a repository restore.
- Compression and encryption are declared capabilities, not hints. Current
  writers emit uncompressed, unencrypted payloads and current restore paths
  fail closed on any other declaration until the corresponding streaming
  codec/key-provider implementation is installed.

Remote repositories stay unpacked for deduplication, range access, and
incremental capture. The repository backend contract has streamed file upload
and materialization operations so native artifacts do not become whole-object
heap allocations. Exporting one snapshot to a file packs its reachable manifest
and blobs into AFB2; importing verifies the same hashes before publishing them
into a repository or native restore staging generation. Portable replay uses
the footer index for bounded-memory positional reads rather than caching the
whole archive.

- The Go OpenAPI remains the public contract source.
- Zig uses the same public request and response structure as the Go
  implementation.
- If a Go-shaped request uses a backend or mode Zig does not support, it
  returns a clear error instead of inventing a Zig-only request shape.
- The production restore contract is asynchronous: restore requests validate
  and record durable restore intent quickly, while shard bootstrap and
  derived-index catch-up run outside the public HTTP request.
- Backup and restore use the existing DB snapshot/restore primitives. AFB2
  packages their portable logical stream or native physical generation; it
  does not invent another DB storage format.
- Stateful Raft/control-plane is the canonical owner of backup/restore.
  Backup/restore extends into serverless only once that stateful contract's
  remaining gaps (see [Open work](#open-work)) are closed.

### Durable Restore Jobs

- Restore creation returns a durable `RestoreJob`; `job_id` is an opaque decimal
  string so every SDK can round-trip it without numeric precision loss.
- `expires_at_ms` is absent while work is queued or running and is assigned only
  when the job reaches a terminal phase.
- Explicit idempotency keys are scoped by authenticated principal and restore
  resource. Reusing a key for the same scoped request returns the original job;
  unrelated users and tables cannot collide.
- Explicit table lists are bounded at 256 entries to keep request and initial job
  records bounded. Cluster-wide backup and restore support up to 4096 tables and
  reject larger clusters before backup artifacts are created.
- Publication and completion checkpoints are stored as canonical ordinal ranges.
  Sequential cluster restores therefore retain constant-sized progress state;
  even maximally fragmented progress remains below the durable 64 KiB job-record
  limit.

## Architecture Shape

### Table Backup Flow

Table backup is a thin public API over:

1. metadata snapshot for the table
2. per-range/per-group local DB snapshot export
3. a table backup manifest written to the target location

The table backup artifact contains:

- table metadata
  - schema
  - `read_schema` / migration metadata
  - index definitions
  - shard/range layout needed for restore
- one snapshot payload per participating shard/group
- a table-level manifest that maps table metadata to shard snapshot locations

`POST /tables/{tableName}/backup` implements this: route match in
`zig/pkg/antfly/src/api/http_routes.zig`, handler in
`zig/pkg/antfly/src/api/http_server.zig`, a backup service module in the
API/metadata layer, metadata lookup for the target table, per-shard snapshot
export via `DB.snapshot(...)`, and manifest + metadata file output under a
`file://...`, `s3://`, or `gs://` location. The request is synchronous today:
it completes after files are written and returns a `201` response with a
Go-shaped body.

### Table Restore Flow

`POST /tables/{tableName}/restore` (route match in
`zig/pkg/antfly/src/api/http_routes.zig`, handler in
`zig/pkg/antfly/src/api/http_server.zig`) is shipped for `file://`, `s3://`,
and `gs://` single-range tables, and remains table-scoped and fail-if-exists,
matching the Go table API.

Table restore is an asynchronous table lifecycle operation. The public
request validates and accepts restore work, then returns the Go-shaped `202`
response without waiting for full-text, vector, graph, or
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
4. return `202 RestoreJob` once the durable job store has accepted the intent
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

The table lifecycle distinguishes at least:

- `creating`
- `ready`
- `restoring`
- `deleting`
- `failed`

While a table is `restoring`, status includes restore-specific details:

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

Read behavior is explicit while restore is in progress:

- primary-key lookup and scan may become available once the primary store is
  restored
- full-text, vector, sparse, graph, and generated-enrichment queries either
  return a clear restoring/not-ready response or report degraded index
  readiness until their required indexes catch up
- when all required shards and indexes are ready, the table lifecycle becomes
  `ready`

Cluster restore returns per-table trigger/skip/failure statuses, but those
statuses are an admission result. The durable source of truth after admission
is the per-table lifecycle and restore status.

The metadata model has restore intent fields on table/range records, projected
restore progress records, and a restore-pending readiness overlay (see
[Open work](#open-work) for the remaining hardening to make public table
restore use this path consistently on every route).

### Bootstrap Model

Backup restore should not be modeled as ordinary Raft peer snapshot transfer.

Zig has two distinct bootstrap source kinds:

- `raft_snapshot_fetch`
  - used for real Raft replica catch-up from another node
  - carries a Raft snapshot locator and transport source node
- `backup_db_snapshot_restore`
  - used for backup/restore provisioning from backup artifacts
  - carries:
    - `backup_id`
    - backup `location`
    - per-range `snapshot_path`

Production replica/bootstrap metadata carries that source explicitly instead
of only a mode bit. Metadata restore intent carries the range-scoped backup
source, and both raw and managed hosts consume `backup_db_snapshot_restore`
directly before replica startup, including catalog replay on restart, instead
of treating backup restore as a Raft peer snapshot fetch. The remaining gap is
not bootstrap routing; it is deeper runtime coverage and operational hardening
around the explicit backup bootstrap source (see [Open work](#open-work)).

Metadata/operator status reports how many projected placement intents are
waiting on Raft snapshot bootstrap vs backup restore bootstrap, so the
bootstrap mix is visible without inspecting every placement record manually.

Host/runtime status exposes per-group backup bootstrap progress for the
explicit backup path:

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

## Cluster Backup and Restore

`/backup`, `/restore`, and `/backups` are implemented for stateful backups
over `file://`, `s3://`, and `gs://`:

- cluster backup writes a cluster manifest plus per-table manifests under the
  same location
- backup IDs are immutable publication keys: generation-scoped payloads are
  committed by a conditional public manifest, and reuse returns `409`
- restore modes (`fail_if_exists`, `skip_if_exists`, `overwrite`) follow the
  Go cluster contract
- unsupported table layouts fail per-table instead of restoring

## Object Store Backends

Object-store backup locations are supported alongside `file://`, through a
backend-neutral backup IO seam; `file://` remains the reference path. `s3://`
and `gs://` (with `gcs://` accepted as a compatibility alias) are implemented
for both table and cluster backup/restore.

## E2E Parity Coverage

Public Zig parity coverage includes:

- table backup/restore route coverage
- cluster backup/list/restore round-trip
- cluster restore modes `fail_if_exists` and `skip_if_exists`; destructive
  overwrite fails closed
- cluster partial success reporting for mixed valid and invalid table sets
- cluster partial success reporting for unsupported multi-range tables
- table backup rejection while schema migration is still rebuilding
- table restore rejection for backup manifests that still carry
  migration-state metadata
- table restore rejection when the target already exists
- table restore rejection for manifest/table-name mismatches
- public request validation for malformed backup/restore bodies and
  unsupported locations
- `/backups` validation for missing or unsupported locations
- cluster restore rejection for invalid `restore_mode`
- managed embeddings backup/restore with index status and semantic query
  checks
- managed sparse embeddings backup/restore with index status and sparse query
  checks
- chunked managed embeddings backup/restore with chunk artifact checks
- graph backup/restore with graph index status and query checks

Request/validation parity runs in `e2e/antfly/test_backup_restore.py`, including a
split `metadata` + `data` round-trip gate that covers table backup/restore,
cluster backup/restore, restore modes, and partial-status reporting. The
remaining gap is broader Go-contract coverage breadth, not basic restore
viability (see [Open work](#open-work)).

## Risks

- metadata restore may drift from actual DB/index snapshot contents if the
  table manifest is underspecified
- object-store backend depth can blur storage-format problems with transport
  problems if changed alongside the core backup format
- a serverless-first backup design would likely conflict with the canonical
  mutable-table contract, which is why serverless integration follows the
  stateful contract rather than leading it (see [Open work](#open-work))

## Open work

- **Serverless integration**: serverless does not yet interact with
  backup/restore. The intended shape is for serverless to reuse the same
  public table contract, with the backup source of truth coming from
  canonical table metadata and backing storage rather than from published
  generations/manifests directly. This should not start before the stateful
  API is stable.
- **Broader Go-contract parity depth**: deepen cluster backup/list/restore
  parity coverage against the Go e2e contract beyond what is listed under
  [E2E Parity Coverage](#e2e-parity-coverage).
- **Backup bootstrap operational hardening**: deeper runtime coverage and
  operational hardening around the explicit `backup_db_snapshot_restore`
  bootstrap source (routing itself is done; see [Bootstrap
  Model](#bootstrap-model)).
- **Restore status path consistency**: make public table restore consistently
  use the durable asynchronous restore path on every route, and remove
  synchronous `runUntilIdle`/derived-index drain from the HTTP critical path
  where it still exists.
- **Provider-backed semantic embedding rebuild during restore**: not
  confirmed to exist; unverified whether embedding regeneration (as opposed
  to restoring already-computed embeddings) is supported during restore.
