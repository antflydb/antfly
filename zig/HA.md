# Antfly HA: Hot Standby WAL Replication

This document explores a Postgres-style hot-standby HA mode for the supported
Zig implementation of Antfly. The goal is not to replace every use of Raft. The
goal is to define a simpler, efficient single-primary replication mode for read
replicas, disaster recovery, online upgrades, and deployments that prefer
Postgres-like operational semantics over quorum consensus.

## Summary

Antfly can support an efficient hot-standby design by combining:

- a consistent base backup of table/shard storage,
- continuous ordered WAL streaming,
- replication slots for WAL retention,
- read-only standby apply,
- explicit promotion with fencing and timeline changes.

This is a good alternative HA design when the product requirement is
single-primary availability with configurable RPO/RTO. It is not equivalent to
Raft unless Antfly also provides a strongly correct failover authority. Raft
bundles leader election, quorum durability, log agreement, and split-brain
avoidance. Hot standby shifts those responsibilities into leases, fencing,
operator policy, or an external control plane.

Recommended position:

- Keep Raft for multi-node consensus and automatic quorum-protected write
  ownership.
- Add hot standby as a separate `single-primary + standby` mode.
- Allow async and synchronous standby durability policies.
- Require fencing for automatic promotion.

The closest design model is Postgres physical standby operation: base backup,
WAL streaming, replication slots, timelines, synchronous commit modes, and
rewind/reseed after failover. CockroachDB is still useful as a source of design
discipline around explicit ownership, lease/fencing checks, protected retention,
and read freshness, but its core HA mechanism is Raft-per-range and should not be
copied wholesale for this non-Raft mode.

## Current Building Blocks

The Zig tree already has several primitives that fit this design.

The generic storage WAL in `pkg/antfly/src/storage/wal.zig` is append-only,
LSN-ordered, CRC-protected, truncatable, and replayable. It intentionally stores
opaque byte entries, so it can back storage persistence, consensus logs, or a
replication stream.

The LSM backend already persists mutable state through its own WAL path in
`pkg/antfly/src/storage/lsm_backend.zig`. `appendWalForMutable` writes state
records, and `replayWalIntoMutable` replays them at open time.

The DB layer also has sequence-ordered derived/change journal machinery under
`pkg/antfly/src/storage/db/derived`. That journal is useful for index/enrichment
maintenance and may inform the HA stream shape, but HA should replicate committed
database effects, not rely on each standby independently discovering or
recomputing all derived work.

The CDC design in `zig/CDC.md` already has an important precedent: checkpointed
snapshot plus streaming apply into the normal Antfly write path. Hot standby is
similar structurally, but the source is another Antfly primary and the stream is
an Antfly-native commit/WAL stream rather than Postgres logical decoding.

## Design Influences

### Postgres

Antfly should borrow these pieces directly:

- base backup plus WAL catch-up,
- replication slots for retention,
- timeline changes on promotion,
- explicit `remote_write` and `remote_apply` synchronous commit semantics,
- operator-visible lag and replay progress,
- rewind or reseed for a former primary after failover.

### CockroachDB

Antfly should borrow these principles, not Cockroach's Raft implementation:

- ownership must be explicit and machine-checkable,
- stale reads need an explicit freshness boundary,
- retention protection must be tied to consumers that need history,
- a node that loses ownership must be fenced before another node writes.

### Antfly

The HA stream should be Antfly-native. The stable contract should be a versioned
logical/effects commit stream, not the incidental byte layout of the current LSM
recovery WAL. The LSM WAL can remain an implementation detail underneath the
replication stream.

## Design Goals

1. Preserve the write-path efficiency of a single primary.
2. Keep standby catch-up sequential and cheap.
3. Make reads available from standbys when staleness is acceptable.
4. Support explicit durability modes:
   - async replication,
   - remote WAL write,
   - remote apply.
5. Avoid recomputing expensive derived state during normal standby apply.
6. Make promotion safe through fencing and epochs.
7. Keep the wire format versioned and independent of incidental in-memory
   layouts.

## Non-Goals

This mode should not initially provide:

- multi-primary writes,
- quorum reads/writes,
- automatic split-brain-safe failover without a fencing authority,
- transparent replacement for shard/metadata Raft groups,
- arbitrary standby writes.

Those features either belong to Raft or require a separate consensus/control
plane.

## Replication Model

Each replicated unit should be a table shard or another explicit storage owner
with one primary and zero or more standbys.

The primary:

- accepts writes,
- assigns monotonically increasing replication LSNs or sequences,
- persists the local commit/WAL record,
- streams records to standbys,
- tracks standby acknowledgements,
- retains WAL required by configured replication slots.

The standby:

- starts from a base backup,
- receives WAL records from the primary,
- durably stores received records before apply,
- applies records in order,
- exposes read-only state at an applied LSN,
- reports write/apply progress to the primary.

Clients write only to the primary. Standby write APIs must reject writes unless
the node is explicitly promoted.

## Base Backup

Standby creation starts with a base backup:

1. Create or reserve a replication slot for the standby before the backup starts.
2. Emit a `backup_start` record with `cluster_id`, `timeline_id`, `epoch`,
   `backup_lsn`, and `manifest_id`.
3. Publish a manifest that can be copied safely.
4. Pin every file referenced by that manifest, including SSTables, artifact
   objects, metadata files, and any local WAL tail required by the checkpoint.
5. Copy files to the standby, or materialize object-store references when shared
   storage is used.
6. Keep streaming WAL from `backup_lsn` while the copy is running.
7. Emit a `backup_end` record after the copied file list and checksums are
   durable.
8. Standby validates file sizes/checksums, opens the copied data in standby
   mode, replays WAL from `backup_lsn`, and reaches `backup_end`.
9. Release backup pins only after the standby confirms the copied files and has
   advanced past `backup_end`, or after the slot is explicitly dropped.

This should be compatible with local filesystem storage and object-backed LSM
layouts. For local files, copy SSTables, manifests, metadata, and any needed WAL
tail. For object-backed storage, copy or reference immutable objects and transfer
only local metadata plus WAL.

The key invariant is the same as Postgres base backup plus LSM manifest pinning:
the manifest must never reference a file that compaction or GC can delete before
the standby has validated and replayed through the backup boundary. The primary
must retain WAL from `backup_lsn` until the standby catches up or the operator
accepts reseeding.

## WAL Stream Shape

Do not expose the raw current LSM state record as the permanent HA wire format.
The LSM WAL is an internal recovery mechanism and may evolve with storage
internals. The HA stream should be an Antfly replication envelope with an
explicit version.

The initial contract should be a logical/effects stream:

- user document mutations,
- metadata/catalog mutations,
- derived artifact writes,
- full-text/vector/sparse/graph/algebraic index effects that must survive
  failover,
- checkpoint, manifest, retention, and timeline records.

The standby applies the effects in primary commit order. It does not run
mutating derived workers while in standby mode. That avoids recomputing
embeddings or independently scheduling background index work and gives the
standby the same committed state the primary exposed.

Proposed envelope:

```text
ReplicationRecord {
  magic
  version
  cluster_id
  shard_id
  table_id
  timeline_id
  epoch
  lsn
  previous_lsn
  commit_timestamp
  record_kind
  payload_codec
  payload_len
  payload_crc
  payload
}
```

Initial `record_kind` values:

- `batch_mutation`: committed document/artifact/index mutation batch.
- `metadata_mutation`: committed metadata/catalog mutation.
- `derived_effect`: committed enrichment, index, graph, or artifact effect.
- `backup_start`: base-backup boundary and pinned manifest id.
- `backup_end`: copied file list/checksum boundary.
- `checkpoint`: base-backup or manifest checkpoint marker.
- `manifest`: storage manifest publication marker when needed.
- `truncate`: WAL-retention/truncation boundary.
- `timeline_switch`: promotion marker for failover.

The payload can use the existing batch/derived encodings where appropriate, but
the replication envelope should be stable and self-describing.

## Apply Semantics

Standby apply must be deterministic and idempotent across restart. A standby
should persist received records and its applied LSN separately:

- `received_lsn`: highest WAL record durably stored locally.
- `applied_lsn`: highest WAL record applied to visible storage.
- `safe_read_lsn`: highest LSN available to read snapshots.

On restart:

1. Open local storage.
2. Replay locally persisted received WAL from `applied_lsn + 1`.
3. Resume streaming from `received_lsn + 1`.
4. Reject records from the wrong cluster, shard, epoch, or timeline.

The standby apply path should avoid expensive user-level recomputation. For v1,
the replication stream should carry committed effects rather than asking the
standby to rediscover them from documents. Rebuild-from-log can still exist as a
repair path, but it should not be the normal HA apply path.

Derived workers may still run on the primary. Standbys should generally keep
leader-only or owner-only background mutation jobs disabled until promotion.

## Durability Modes

The primary should expose a per-shard or per-table durability policy.

Policy should be explicit rather than hidden in a boolean. Example shapes:

```text
async
remote_write ANY 1 (standby-a, standby-b)
remote_write ALL (standby-a, standby-b)
remote_apply FIRST 1 (standby-a, standby-b)
remote_apply ALL (standby-a, standby-b)
```

`ANY 1` means any named standby can satisfy the acknowledgement. `FIRST 1` means
the first healthy standby in priority order must satisfy it. `ALL` means every
named synchronous standby must satisfy it. If no named standby is available, the
configured failure policy decides whether writes block, fail, or degrade.

Failure policies:

- `block`: preserve the synchronous guarantee by waiting until a standby returns.
- `fail_closed`: reject writes while the synchronous guarantee cannot be met.
- `degrade_to_async`: continue accepting writes and surface degraded RPO status.

`degrade_to_async` should be opt-in because it changes the durability contract
for acknowledged writes.

### Async

The primary commits once local durability succeeds. Standbys receive WAL later.

Benefits:

- lowest write latency,
- useful for read replicas and cross-region DR.

Tradeoff:

- acknowledged writes can be lost if the primary fails before streaming them.

### Remote Write

The primary commits after one or more synchronous standbys durably receive the
WAL record.

Benefits:

- protects against primary disk/node loss after acknowledgement,
- lower latency than waiting for full standby apply.

Tradeoff:

- standby may need replay time before serving latest reads or promotion.

### Remote Apply

The primary commits after one or more synchronous standbys apply the record.

Benefits:

- strongest standby freshness,
- simpler zero-data-loss promotion expectations.

Tradeoff:

- highest write latency,
- sensitive to standby apply stalls.

## Replication Slots and Retention

Primary WAL retention should be slot-based:

- each standby has a durable slot id,
- each slot tracks `restart_lsn`, `received_lsn`, and `applied_lsn`,
- primary keeps WAL from the oldest required `restart_lsn`,
- operators can cap retained bytes/time and mark a standby as needing reseed.

This mirrors Postgres operational behavior. A dead standby must not retain WAL
forever without an explicit operator choice.

Expose status:

- current primary LSN,
- per-standby received/apply lag,
- retained WAL bytes,
- oldest retained LSN,
- slot health,
- reseed recommended flag,
- last replication error.

## Promotion and Fencing

Promotion is the hard part. Without Raft, Antfly must not pretend promotion is
automatically safe.

A safe promotion requires:

1. A fencing authority declares the old primary unable to accept writes.
2. The selected standby verifies it has the required LSN for the chosen RPO.
3. The standby writes a `timeline_switch` record with a new timeline id.
4. The standby enables write ownership and leader-only background jobs.
5. Other standbys follow the new timeline or are reseeded if they diverged.

Possible fencing authorities:

- Kubernetes Lease plus storage-level fencing,
- cloud load balancer/control-plane fencing,
- a metadata Raft group that only manages ownership,
- an external operator that performs manual failover,
- a witness service.

If there is no fencing authority, promotion should be manual and clearly marked
as potentially lossy. Antfly should require an explicit force flag when the
chosen standby has not received all acknowledged synchronous WAL.

## Timeline Handling

Promotion creates a new timeline. WAL records include `timeline_id` and `epoch`.

Rules:

- A standby must reject records from an unexpected timeline.
- A promoted standby must never append to the old timeline.
- A former primary rejoining after failover must be fenced, demoted, and either
  rewound to the new timeline or fully reseeded.
- Replication slots are scoped to timelines.

This is the Postgres timeline idea adapted to Antfly storage.

## Metadata and Shards

There are two separate concerns:

1. Data shard replication.
2. Metadata/catalog ownership.

For a first hot-standby mode, keep the scope narrow:

- replicate a full standalone Antfly instance or explicit shard set,
- use one primary metadata owner,
- keep standbys read-only,
- promote the whole instance together.

For v1 whole-instance standby, metadata and data should share one ordered
instance replication stream. Schema/table/shard records must be applied before
dependent data records with higher LSNs become visible. A standby should reject
or wait on reads when the metadata applied LSN is behind the data LSN required by
the read snapshot.

Shard-granular promotion is possible later, but it reintroduces distributed
ownership and routing complexity. At that point, a small metadata consensus
layer may still be needed even if data replication is WAL-based.

## Read Behavior

Standbys can serve reads at their applied LSN.

Expose consistency options:

- `stale_ok`: read current standby state.
- `at_least_lsn`: wait until the standby applies a required LSN.
- `primary`: route to primary for read-after-write.

The API should surface standby lag so clients and routers can make informed
choices.

## API and CLI Surface

The HA control plane should be API-first. The stable automation contract should
be a typed, versioned `/admin/v1/ha` API specified in
`specs/openapi/antfly/admin.yaml`, with Zig admin API routing and helpers under
`zig/pkg/antfly/src/admin/`. The CLI should remain as an ergonomic human and
break-glass interface, but long-term operator automation should not depend on
shelling out to a command as the primary protocol.

`specs/openapi/antfly/admin.yaml` is the source of truth for this surface and
should be treated as a new, dedicated admin OpenAPI spec, not an extension point
inside the existing public DB specs. New HA administration methods must not be
added first to the existing public DB OpenAPI specs, to
`specs/openapi/antfly/internal.yaml`, or directly to ad hoc Zig HTTP handlers.
The committed starting point for this contract is `specs/openapi/antfly/admin.yaml`;
it is generated as `antfly_admin_openapi` and surfaced through
`zig/pkg/antfly/src/admin/mod.zig` and `zig/pkg/antfly/src/admin/routes.zig`.
The implementation path is:

1. define the operation, request schema, response schema, and error response in
   `specs/openapi/antfly/admin.yaml`;
2. regenerate the Zig admin OpenAPI bindings;
3. re-export shared request/response types and route constants from the Zig
   admin package rooted at `zig/pkg/antfly/src/admin/`;
4. implement node-local behavior by consuming those admin package types and
   route constants from the HA storage adapter. The admin package owns the
   HTTP contract, generated request parsing helpers, and shared route/type
   surface; storage HA modules own execution against local WAL, slots, fences,
   promotion state, and rejoin state; and
5. generate the same admin OpenAPI contract into `go/pkg/sdk/admin`, keep a
   small hand-written Go wrapper around the generated client, and have the CLI
   and `go/pkg/operator` call that typed `/admin/v1/ha` wrapper.

The generated Zig module for this spec should remain the admin contract module
(`antfly_admin_openapi`) and should be surfaced through
`zig/pkg/antfly/src/admin/mod.zig` plus route constants in
`zig/pkg/antfly/src/admin/routes.zig`. Runtime replication handlers may import
admin types when they need to produce the same receipt/status shape, but they
must not define new HA administration paths under `zig/pkg/antfly/src/internal/`
or `specs/openapi/antfly/internal.yaml`. The internal OpenAPI spec is reserved
for node-to-node replication RPCs such as identify-system, start-replication,
and standby-status-update.

Recommended split:

- `/admin/v1/ha`: human and operator control-plane actions. This API owns
  replication slot lifecycle, base-backup orchestration, HA status, fencing
  receipts, promotion, former-primary rejoin, rewind, and reseed workflows. It
  should return typed responses with action ids, LSNs, timelines, fence tokens,
  receipts, and idempotency state. New HA admin endpoints and schemas should be
  added to the dedicated `specs/openapi/antfly/admin.yaml` spec first, generated
  into Zig admin types, and implemented through `zig/pkg/antfly/src/admin/`
  routing/helpers rather than mixed into the public DB API or runtime-internal
  API.
- `/internal/v1`: runtime-to-runtime traffic inside a trusted deployment. This
  is where WAL streaming, replication pulls, standby status updates, identity
  probes, and other node-to-node mechanisms belong. It should not be the
  operator policy or human operations surface.
- CLI: a thin client over `/admin/v1/ha` for remote operations, plus local
  offline helpers where useful. CLI output should be derived from the same typed
  responses the admin API returns.
- Go SDK: generated client/types under `go/pkg/sdk/admin/oapi`, with a
  hand-written `go/pkg/sdk/admin` wrapper that normalizes the admin base URL,
  installs auth/request editors, exposes stable HA methods, returns typed
  responses plus raw response bodies where receipts must be audited, and maps
  non-2xx responses into operation-aware errors. The operator should import
  this package for executable admin operations instead of duplicating an HTTP
  client, hard-coding paths, or parsing CLI output.

The admin API is node-local even though it is typed and operator-facing. The
operator must choose the target node deliberately:

- primary-scoped actions such as slot create/drop/pause/resume, retention
  inspection, standby seed scheduling, and reseed marking target the current
  primary's admin URL;
- standby-scoped actions such as bootstrap-seed, promotion readiness checks, and
  promotion target the selected standby's admin URL;
- former-primary rewind targets the former primary's admin URL because it needs
  that node's local WAL/storage state;
- former-primary reseed coordination targets the current primary when it marks
  a slot or publishes a new seed, and uses a pod-local CLI helper only for the
  actual local data replacement step on the former primary.

This targeting rule is part of the production contract. A successful HTTP call
to the wrong node is not enough evidence for failover automation; typed
responses must include the acted-on node id, timeline, epoch, LSNs, fence token
or receipt, and idempotency state so the operator can prove the intended node
performed the intended step. The Kubernetes operator should publish the expected
node-local executor as `status.haStatus.plannedActions[].adminNodeID` and reject
typed action receipts whose `action.node_id` does not match it.

Kubernetes Jobs that run `antfly ha ...` are acceptable as a bootstrap mechanism
for workflows that need pod-local volume mounts or shared backup files. They
should not become the only production automation path. The operator should move
toward typed `/admin/v1/ha` calls for idempotent actions and reserve CLI Jobs
for explicitly local file-transfer or recovery steps.

## Failure Cases

### Primary crash, async standby behind

The standby may not have acknowledged writes. Promotion can proceed with data
loss only if the operator or failover policy accepts that RPO.

### Primary crash, remote-write standby current

The standby has durable WAL. Promotion should replay through the required LSN
before becoming writable.

### Standby crash

The standby recovers from local received WAL, reports its progress, and resumes
from its slot. If the primary has already discarded required WAL, the standby
must be reseeded.

### Network partition

This is where fencing matters. A standby must not self-promote just because it
cannot reach the primary. Some authority must decide which side may write.

### Former primary returns after promotion

The former primary must not accept writes. It must discover the newer timeline
and either rewind or reseed.

## Implementation Plan

### Phase 1: Local Replication Format

- Define `ReplicationRecord` envelope and binary codec.
- Add tests for CRC, versioning, ordering, and corrupt-tail behavior.
- Build an in-process primary/standby simulation that appends records and
  applies them to a standby store.

### Phase 2: Snapshot Plus WAL Catch-Up

- Add base-backup checkpoint creation.
- Add copy/restore flow for local LSM storage.
- Add `received_lsn` and `applied_lsn` metadata.
- Prove standby restart and catch-up from copied storage plus WAL.

### Phase 3: Streaming Transport

- Add a pull or bidirectional internal replication API under `/internal/v1`:
  - `IDENTIFY_SYSTEM`
  - `CREATE_REPLICATION_SLOT`
  - `START_REPLICATION from_lsn`
  - `STANDBY_STATUS_UPDATE`
- Implement backpressure and batching.
- Add lag/status surfaces.

### Phase 4: Async Durability and Ack Plumbing

- Add async commit mode.
- Track standby acknowledgements without gating primary commit.
- Persist per-standby `received_lsn`, `applied_lsn`, and slot status.
- Surface degraded, lagging, and reseed-needed status.

### Phase 5: Promotion

- Add standby promotion command.
- Add timeline switch records.
- Add forced promotion guardrails.
- Add former-primary rejoin handling.
- Integrate with a concrete fencing mechanism before enabling automatic
  failover.

### Phase 6: Production Hardening

- Add chaos tests:
  - crash during base backup,
  - crash during WAL receive,
  - crash during apply,
  - network partition,
  - standby lag and reseed,
  - former primary return.
- Add metrics and admin status.
- Add compatibility tests across replication format versions.

### Phase 7: Synchronous Failover

- After async standby works under crash tests, add `remote_write` and
  `remote_apply` commit modes.
- Implement `ANY`, `FIRST`, and `ALL` synchronous standby policies.
- Implement `block`, `fail_closed`, and `degrade_to_async` failure policies.
- Add fenced automatic promotion using a concrete ownership authority.

### Phase 8: CLI and Admin API

- Define `/admin/v1/ha` as the stable typed control-plane API in
  the dedicated `specs/openapi/antfly/admin.yaml` OpenAPI spec, separate from
  public DB and `/internal/v1` specs.
- Generate and re-export Zig admin request/response types from that spec, and
  keep generated request parsing helpers and shared route/type constants in
  `zig/pkg/antfly/src/admin/`.
- Keep `specs/openapi/antfly/admin.yaml` and `zig/pkg/antfly/src/admin/` as the
  only source locations for HA admin HTTP contract definitions. Public DB specs
  and `/internal/v1` specs may reference HA concepts only as clients of the
  contract, not as owners of HA operator actions.
- Reject implementations that add HA operator actions first to
  `specs/openapi/antfly/internal.yaml`, public OpenAPI specs, or ad hoc Zig HTTP
  handlers; the new admin spec and `zig/pkg/antfly/src/admin/` package must land
  before the route is consumed by the CLI or operator.
- Implement node-local admin behavior in the HA runtime by importing
  `zig/pkg/antfly/src/admin/` types and routes, not by hard-coding new
  `/admin/v1/ha` paths or request/response schemas in storage modules.
- Add a CI or unit-test guard that fails when a documented `/admin/v1/ha`
  route is implemented without a matching `operationId` in
  `specs/openapi/antfly/admin.yaml`.
- Add admin API endpoints to create, drop, pause, resume, and list replication
  slots.
- Add admin API endpoints to seed a standby from a base backup and report
  resumable action state.
- Add admin API endpoints to show primary LSN, standby received/apply LSN, lag,
  slot retention, degraded sync status, and reseed recommendations.
- Add admin API promotion endpoints with explicit safe, forced, and lossy modes.
- Add admin API endpoints to validate timeline/LSN compatibility before
  promotion or rejoin.
- Add former-primary API workflows for rewind when possible and reseed when
  rewind is unsafe.
- Keep the CLI as a thin client over `/admin/v1/ha` for remote operations, with
  local/offline helpers only where direct filesystem access is required.
- Keep CLI table and JSON output aligned with admin API response schemas so
  humans, tests, and the operator observe the same fields.
- Generate Go admin client/types from `specs/openapi/antfly/admin.yaml` into
  `go/pkg/sdk/admin/oapi`, and keep a small `go/pkg/sdk/admin` wrapper for HA
  operations following the style of the other SDK APIs.
- Make both the CLI and operator consume the `go/pkg/sdk/admin` HA wrapper for
  remote admin operations. Any CLI-only code path must be limited to local
  filesystem recovery, pod-local volume manipulation, or explicit break-glass
  workflows.

### Phase 9: Operator Integration

The Kubernetes operator integration lives in `go/pkg/operator`. The Zig HA
planner should remain a portable policy engine, but CRD fields, status
conditions, admin-job targeting, service updates, and promotion automation must
be validated against that operator package.

- Add CRD fields for HA mode, standby topology, sync policy, failure policy,
  retention caps, and automatic-failover policy.
- Bootstrap standby pods from base backup and attach them to replication slots.
- Manage slot lifecycle and WAL retention pressure.
- Prefer typed `/admin/v1/ha` calls for idempotent operator actions.
- Treat `specs/openapi/antfly/admin.yaml` plus `zig/pkg/antfly/src/admin/` as
  the operator-facing contract source for admin HTTP method/path, request, and
  response fields.
- Generate the Go admin client/types from that admin OpenAPI contract into
  `go/pkg/sdk/admin/oapi`, wrap them in `go/pkg/sdk/admin`, and have
  `go/pkg/operator` import that wrapper for executable `/admin/v1/ha` calls.
  The operator may keep path constants only for status display and plan
  summaries; live calls and request/response decoding should go through the
  SDK wrapper.
- Publish each executable planned action with its typed admin HTTP method/path
  and target admin URL, while keeping CLI argv as a compatibility and
  break-glass execution hint.
- Target former-primary rewind at the former primary's admin URL, not the
  current primary. Target reseed scheduling/slot marking at the current primary,
  then run any data-replacement step through a pod-local helper on the node being
  reseeded.
- Use CLI-backed Kubernetes Jobs only for workflows that need pod-local mounted
  files, shared backup volumes, or explicit break-glass execution.
- Publish lag, degraded, unhealthy, and reseed-required conditions.
- Coordinate fenced failover through Kubernetes Lease, storage fencing, or
  another configured ownership authority.
- Update Services, routes, and client-facing primary endpoints after promotion.
- Automate former-primary demotion, rewind, or reseed after failover.
- Keep automatic promotion disabled unless Phase 7 fencing requirements are
  satisfied by the configured environment.

## Recommendation

Hot standby is worth building for Antfly. It fits the Zig storage architecture,
can be efficient, and gives users a familiar Postgres-style HA story.

The product line should be explicit:

- Hot standby is the simple, efficient HA/read-replica/DR path.
- Raft remains the correct path for consensus-backed distributed write
  ownership.

The first version should prioritize correctness over automatic failover:

1. single primary,
2. async standby,
3. base backup plus WAL catch-up,
4. read-only standby,
5. manual promotion with timeline switch.
