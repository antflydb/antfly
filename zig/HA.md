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

Latest review decisions:

- Keep HA string validation shared only at the missing/padded classification
  layer. Replace `paddedHAString` with
  `HAStringValidation = enum { ok, missing, padded }` and
  `classifyHAString(value: ?[]const u8)`, but keep field-specific errors and
  type-specific validation for paths, node ids, slot names, token environment
  variables, and URLs. Do not replace this with one catch-all
  `validateHAString`.
- Type-specific validation is part of the HA contract: paths must be absolute,
  normalized, and bounded to the allowed storage root where appropriate; node
  ids and slot names must use a restricted charset and bounded length; token
  environment variables must use the existing environment-variable-name rules;
  and admin/replication URLs must parse as URLs while rejecting hidden
  whitespace.
- Add `test_standby.py` as a real-process Zig e2e once the admin API and
  runtime wiring are usable. The test should cover primary startup, slot
  creation, standby seed/startup, primary writes, standby catch-up, read-only
  behavior, standby restart/replay resume, and later fenced promotion plus
  old-primary write rejection.
- Integrate HA with Zig simulation tests before treating the mode as safe.
  Simulation coverage should exercise receive/apply crash windows, sync-ack
  crashes, duplicate/gap/out-of-order WAL, fenced and unfenced promotion,
  old-primary rejoin/rewind/reseed, WAL expiry, and timeline propagation.
- Do not call the design production grade, or claim bulk Postgres-style HA
  parity, until runtime wiring, generated `/admin/v1/ha` Zig and Go clients,
  `go/pkg/operator` integration through the Go SDK wrapper, real base backup,
  sync commit, fencing, promotion/timeline/former-primary repair, standby
  freshness, WAL retention/reseed, auth, audit, metrics, runbooks,
  compatibility tests, crash/e2e/operator coverage, and optional Postgres-like
  archive/PITR or relay-replica decisions are explicit. The bulk parity gaps
  after basic streaming are former-primary repair similar to `pg_rewind`,
  synchronous commit policy depth, WAL archive/PITR options, robust
  observability, cascading or relay replicas if desired, and operator
  ergonomics.

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
   small hand-written Go wrapper around the generated client, and have
   `go/pkg/operator` and other Go automation call that typed `/admin/v1/ha`
   wrapper. The supported Zig CLI should use the Zig admin bindings generated
   from the same spec rather than importing or shelling through the Go SDK.

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
  hand-written `go/pkg/sdk/admin` wrapper that follows the style of the other
  Go SDK APIs: it normalizes the admin base URL, installs auth/request editors,
  exposes stable HA methods, returns typed responses plus raw response bodies
  where receipts must be audited, and maps non-2xx responses into
  operation-aware errors. This should be the only generated Go client for the
  admin spec; do not generate a separate operator-local client. The Kubernetes
  operator should import this wrapper for executable admin operations instead
  of duplicating an HTTP client, hard-coding paths, importing generated `oapi`
  internals directly, or parsing CLI output. The wrapper is the compatibility
  boundary for Go control-plane code; generated `oapi` symbols are a transport
  detail hidden inside the SDK package. This keeps operator behavior, SDK
  consumers, and OpenAPI compatibility checks on one reviewed contract instead
  of creating a second admin API surface inside `go/pkg/operator`.

The Go SDK wrapper should enforce the same HA identifier policy as the Zig
runtime and operator admission layer before it builds operation metadata or
executes requests. Generated OpenAPI path helpers prove method/path compatibility
but they do not prove semantic validity: a replication slot name or node id is
not acceptable merely because `url.PathEscape` can encode it into a path
segment. Wrapper helpers for slot paths, node-scoped actions, promotion targets,
and former-primary repair should reject missing, padded, overlong, or
out-of-charset identifiers and should not silently trim operator input. This
keeps local CLI use, Go SDK consumers, and `go/pkg/operator` automation aligned
with the durable HA identity rules.

Runtime HA validation should be shared but still field-aware. Helpers such as
`paddedHAString` should evolve into a small classifier, for example

```zig
const HAStringValidation = enum { ok, missing, padded };

fn classifyHAString(value: ?[]const u8) HAStringValidation
```

Role validation can reuse the same whitespace and missing-value rules while
preserving field-specific errors such as `HAPrimaryLogInvalid`,
`HAStandbySlotMissing`, or `HAAdminTokenEnvInvalid`. Do not collapse validation
into one generic `validateHAString` function that decides every field's type
rules, silently trims operator input, or returns generic errors. HA runtime
identity and path fields should fail closed when they contain leading or
trailing whitespace, because those values become durable node identity, WAL
path, fence, slot, URL, or token-env configuration. Field-specific validators
should translate `ok`, `missing`, or `padded` into the right field-specific HA
error, then layer type checks on top of the shared classifier:

- paths must pass path-specific safety rules: absolute, normalized, and bounded
  to an allowed storage root where appropriate. Do not accept paths whose raw
  value changes after normalization, escapes through `..`, or points outside
  the configured HA data/backup root;
- node ids and slot names must have restricted character sets and bounded
  lengths;
- token environment names must pass environment-variable-name validation;
- admin and replication URLs must be parsed as URLs and reject hidden
  whitespace instead of relying on implicit trimming.

Admin authentication should be explicit but operationally simple. The Antfly
runtime may be started with `--ha-admin-token-env <name>`; when set, the Zig
process reads a bearer token from that environment variable at startup and
requires `Authorization: Bearer <token>` on typed `/admin/v1/ha` routes. Health
checks and node-to-node `/internal/v1` replication traffic are separate from
this control-plane auth path. The operator should read its outbound bearer token
from `spec.highAvailability.admin.tokenEnvVar`, defaulting to
`ANTFLY_HA_ADMIN_TOKEN`, and the Antfly pods should receive the same token
through `spec.highAvailability.runtime.adminTokenEnvVar`, with pod injection
from `spec.highAvailability.runtime.adminTokenSecretRef` or `spec.standalone.envFrom`.
When `adminTokenSecretRef` is used, the referenced Secret key should be required
(`optional: false`) so pods do not start without the admin token. Kubernetes
should inject both process environments from Secrets; the operator should not
need direct Secret read permissions merely to call the HA admin API.
For human or break-glass operations, `antfly ha --ha-url <url>` with
`--ha-token-env ANTFLY_HA_ADMIN_TOKEN` should resolve the token from the
operator/admin environment and send the same bearer header to typed admin
routes. Do not add a raw token CLI flag; tokens should not be exposed through
process argv.
If the operator ever uses a CLI-backed HA admin Job for compatibility or
pod-local workflows against an authenticated admin endpoint, it should pass
`--ha-token-env` only when `spec.highAvailability.admin.tokenEnvVar` is
explicitly configured and should inject that variable into the Job with
`spec.highAvailability.admin.envFrom`. Direct operator SDK calls may continue to
default to `ANTFLY_HA_ADMIN_TOKEN` from the operator process environment.

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

### Implementation Guardrails

#### Review Decisions

The review outcome is to keep HA validation, e2e coverage, simulation coverage,
and the production bar explicit in the design. The implementation should not
treat these as nice-to-have cleanup after the storage path streams records.

Treat these review points as acceptance gates, not follow-up polish:

- `classifyHAString` can land before the full HA runtime, but it must preserve
  field-specific errors and never silently trim durable HA identity, WAL path,
  fence, slot, URL, or token-env configuration.
- The shared string validator should be a classifier, not a catch-all
  `validateHAString` that decides every field's type rules. Each caller should
  translate `ok`, `missing`, or `padded` into the right field-specific HA error,
  then run type-specific validation for paths, node ids, slot names, token env
  vars, and URLs.
- `test_standby.py` must prove a real primary/standby process path, not just
  argument validation.
- Zig simulation coverage must own crash, replay, partition, promotion, rejoin,
  rewind, reseed, retention-expiry, and timeline-switch correctness.
- Production readiness requires the failure cases to be first-class before
  automatic promotion or synchronous commit is advertised as supported.

The concrete follow-up decisions are:

- Replace `paddedHAString` with a small shared `HAStringValidation` classifier
  rather than a catch-all `validateHAString`. Keep durable HA string handling
  generic only for `ok`, `missing`, and `padded`, then have each caller map that
  result to field-specific errors and type-specific validation.
- Add `test_standby.py` as a real-process e2e, not just a CLI or validation
  test. It should cover primary startup, slot creation, standby seed, standby
  startup, primary writes, standby catch-up, read-only standby behavior,
  standby restart/replay, and later fenced promotion plus old-primary write
  rejection.
- Integrate HA with Zig simulation tests before treating the mode as safe.
  Sim coverage should exercise receive/apply crashes, primary crash before and
  after sync ack, duplicate/gap/out-of-order WAL, promotion with and without
  fences, old-primary rejoin, rewind, reseed, retained-WAL expiry, and timeline
  switch propagation.
- Do not advertise production-grade Postgres-style HA parity until the runtime
  wiring, generated `/admin/v1/ha` Zig and Go clients, `go/pkg/operator`
  integration, real base backup, synchronous commit, fencing, promotion
  receipts, standby freshness, retention/reseed handling, auth, audit, metrics,
  runbooks, compatibility tests, crash/e2e/operator coverage, and former-primary
  repair paths are all implemented and tested.

#### Acceptance Checklist

Before the hot-standby design is treated as more than an experimental async
replication path, the implementation should satisfy this checklist:

- shared HA string handling uses `classifyHAString` for missing/padded
  detection, while every caller preserves field-specific errors and applies
  type-specific validation for paths, node ids, slot names, token env vars, and
  URLs;
- `test_standby.py` exists as a real-process Zig e2e covering primary startup,
  slot creation, standby seeding, standby startup, primary writes, standby
  catch-up, read-only standby behavior, standby restart, and replay resume;
- Zig simulation tests cover receive/apply crash points, primary crash before
  and after synchronous acknowledgement, duplicate/gap/out-of-order WAL,
  promotion with and without fence evidence, old-primary rejoin, rewind, reseed,
  WAL-retention expiry, and timeline switch propagation;
- production-grade scope includes the generated `/admin/v1/ha` Zig and Go
  clients, `go/pkg/operator` integration through the Go SDK wrapper, real
  base-backup/reseed workflows, synchronous commit modes, fencing, promotion
  receipts, former-primary repair, standby freshness controls, WAL retention
  policy, auth, auditability, metrics, runbooks, compatibility tests, crash
  tests, black-box e2e, and operator e2e;
- bulk Postgres-style parity is not claimed until the former-primary repair,
  synchronous commit, observability, operator workflow, and optional
  WAL-archive/PITR or relay-replica gaps are explicit product decisions rather
  than implicit omissions.

#### Open Review Question Answers

The direct answers to the HA review questions are:

- `paddedHAString` should become a shared classifier, but not a catch-all
  `validateHAString` that owns every HA input policy.
- `test_standby.py` should be added as a black-box Zig e2e once the real admin
  API and runtime path are usable.
- HA must be integrated with Zig simulation tests because the sim layer is
  where crash, replay, promotion, fencing, and retention correctness should be
  explored exhaustively.
- The mode is not production grade, or close to bulk Postgres HA parity, until
  runtime wiring, generated admin clients, operator integration, real
  base-backup/reseed, sync commit, fencing, former-primary repair, observability,
  compatibility, e2e, operator e2e, and crash/sim coverage are all in place.

These answers imply a concrete implementation boundary: Antfly should share the
cheap string classification logic, not hide all HA validation behind one generic
validator. A generic `validateHAString` name is too broad because paths, node
ids, slot names, URLs, and token environment variables have different safety
rules and different user-facing error types. The shared helper should only
answer whether a value is present and whether its exact bytes include forbidden
leading or trailing whitespace; the caller must still apply the field-specific
policy. That keeps the code reusable without making operator-visible failures
generic or silently normalizing durable HA identity.

The HA string helper should be generic only at the whitespace/presence
classification layer:

```zig
const HAStringValidation = enum { ok, missing, padded };

fn classifyHAString(value: ?[]const u8) HAStringValidation
```

Callers should translate `missing` and `padded` into field-specific errors such
as `HAPrimaryLogInvalid`, `HAStandbySlotMissing`, or the matching token-env,
slot, URL, path, or node-id error. They should then run type-specific
validation instead of putting all HA input policy into one generic
`validateHAString` function:

- paths must be absolute, normalized, and under the allowed storage root;
- node ids and replication slot names must have restricted charset and length;
- admin token environment variable names must use the existing env-var-name
  validation rules;
- admin and replication URLs must parse as URLs and reject hidden whitespace.

`test_standby.py` belongs in the e2e suite once the admin API and runtime wiring
are usable as real black-box process surfaces. Its first product-path version
should start a primary, create a slot, seed a standby, start the standby, write
to the primary, verify standby catch-up and read-only/stale-read status, restart
the standby, and verify replay resumes. Later versions should add fenced
promotion, forced-promotion receipts, old-primary write rejection, and
rewind-or-reseed behavior.

Zig simulation coverage is mandatory before this mode is treated as safe. It
should cover crash after receive before apply, crash after apply before ack,
primary crash before and after sync ack, duplicate/gap/out-of-order WAL records,
promotion with and without fence evidence, old-primary rejoin, rewind, reseed,
retained-WAL expiry forcing reseed, and timeline switch propagation.

Production-grade Postgres-style parity requires more than streaming records. The
bulk feature-parity bar includes end-to-end primary/standby runtime wiring,
generated `/admin/v1/ha` Zig and Go SDK clients, `go/pkg/operator` integration,
real base-backup and seed workflows, `remote_write` and `remote_apply`
synchronous commit policies, hard-to-misuse fencing, promotion/timeline
switch/former-primary repair, standby freshness controls, WAL retention and
reseed status, auth, auditability, metrics, runbooks, format compatibility
tests, black-box e2e, operator e2e, and crash/simulation coverage.

## Test Strategy

HA needs both black-box e2e coverage and deterministic simulation coverage. The
Python e2e suite should add a Zig-backed standby test, for example
`test_standby.py`, once the runtime and admin API are usable as real process
surfaces. That e2e test should not exist only to assert whitespace or argument
validation. It should launch real Antfly processes and cover the user-visible
Postgres-style flow:

1. start a primary;
2. create a replication slot and seed a standby from the primary;
3. start the standby against the seeded data and replication stream;
4. write data to the primary;
5. wait for standby catch-up and verify read-only standby visibility;
6. restart the standby and verify local received-WAL replay plus stream resume;
7. later, fence and promote the standby, then verify the old primary rejects
   writes or must rejoin through rewind/reseed.

The first version of `test_standby.py` should stop at the black-box async path
if promotion is not wired yet, but it should still be a real process test:
primary process, standby process, durable directories, admin API or CLI setup,
client writes, standby catch-up observation, standby restart, and read-only
verification. Whitespace or argument-validation coverage belongs in unit tests;
it is not enough evidence for HA.

Treat the e2e work as a set of concrete product-path gates:

- the initial gate proves primary startup, slot creation, base-backup seed,
  standby startup, primary writes, standby catch-up, read-only enforcement, and
  standby restart/replay using real processes and durable files;
- the admin-auth gate proves typed `/admin/v1/ha` calls require bearer auth when
  `--ha-admin-token-env` is configured, while health checks and replication
  traffic remain separate from that control-plane auth;
- the freshness gate proves standby reads report stale, `at_least_lsn`, and
  primary-only routing decisions instead of serving ambiguous read-after-write
  behavior;
- the synchronous-commit gate proves `remote_write` and `remote_apply` decisions
  against real standby progress, including fail-closed behavior when no standby
  can satisfy the requested durability;
- the retention gate proves lagging or abandoned slots become
  `reseed_required` and do not pin WAL forever without an operator-visible
  status;
- the promotion gate proves a standby can be fenced, promoted, and made current
  only with machine-checkable receipt evidence, and that the old primary rejects
  writes once it has observed the fence;
- the former-primary gate proves an old primary cannot rejoin without a fence,
  can rewind only when the retained WAL/fork record is sufficient, and otherwise
  is explicitly marked for reseed.

The Zig simulation tests should carry most of the correctness burden because
they can explore interleavings that are expensive or flaky in process e2e. Add
model or harness coverage for:

- crash after WAL receive before apply;
- crash after apply before acknowledgement, including the window where the
  standby has durable applied progress but the primary has not received the
  status update yet;
- primary crash before and after synchronous acknowledgement;
- duplicate, missing, out-of-order, or divergent WAL records;
- delayed status updates and stale slot progress;
- promotion with a valid fence, without a fence, and with stale fence evidence;
- old-primary return after promotion;
- rewind versus reseed decisions;
- retention expiry forcing reseed;
- timeline switch propagation to remaining standbys.

The expected split is: e2e proves the supported CLI/admin/operator path works
with real processes and files, while Zig simulation proves the state machine is
correct under crash, restart, partition, and replay ordering stress. Production
readiness should depend on the simulation matrix because it can cover failure
windows, duplicate delivery, reordered delivery, and promotion races that would
be too slow or nondeterministic to rely on in process e2e alone.

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
  - crash after receive before apply,
  - crash after apply before acknowledgement, proving the primary does not
    treat `remote_apply` as satisfied until the resumed standby reports durable
    applied progress,
  - primary crash before and after synchronous acknowledgement,
  - duplicate, missing, divergent, or out-of-order WAL records,
  - network partition,
  - promotion with and without valid fence evidence,
  - standby lag and reseed,
  - retained WAL expiry forcing reseed,
  - former primary return,
  - timeline switch propagation.
- Add metrics and admin status.
- Add compatibility tests across replication format versions.
- Add the Python `test_standby.py` e2e path for real primary/standby process
  startup, seed, catch-up, standby restart, and read-only standby verification.
- Add Zig simulation coverage for the HA state machine before depending on
  black-box e2e for correctness.

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
- Wire the supported Zig `antfly standalone` runtime so a primary can be started with
  durable HA replication log, slot store, promotion fence WAL, optional
  former-primary rewind log, optional admin bearer-token env var, node id, and
  identity flags. That runtime path should attach the same `/admin/v1/ha`
  executor, durable fence store, former-primary log handle, admin auth
  enforcement, and `/internal/v1/ha/replication` executor used by tests and the
  CLI, rather than requiring a bespoke harness to expose primary-side HA
  operations or rejoin/rewind workflows.
- Wire the supported Zig `antfly standalone` runtime so a standby can also be started
  with a durable received-WAL log, progress WAL, promotion fence WAL, optional
  former-primary rewind log, optional admin bearer-token env var, node id, and
  identity flags. The standby runtime path should expose `/admin/v1/ha` status,
  read/write gate, bootstrap, and promotion operations against the real standby
  handle, guarded by the same admin auth policy as primary nodes. Continuous
  pull/apply should then plug into the
  DataServer-managed standby DB open path so applied LSN only advances after
  replicated records are applied to storage. Every provisioned writer DB opened
  by that DataServer path must carry the same HA write gate as the node's admin
  role, so standby processes reject client/local-owner writes and suppress
  primary-only background mutation loops while still permitting replicated apply.
- Generate Go admin client/types from `specs/openapi/antfly/admin.yaml` into
  `go/pkg/sdk/admin/oapi`, and keep a small `go/pkg/sdk/admin` wrapper for HA
  operations following the style of the other SDK APIs. This SDK package is the
  single Go generation target for the admin contract; operator code must not
  own another generated admin client.
- Make the Go SDK HA wrapper validate slot names, node ids, and other durable
  HA identifiers before constructing operation metadata or issuing generated
  requests. Path escaping is not validation; padded, whitespace-containing,
  overlong, or out-of-charset identifiers should return a typed local error or
  `ok=false` from metadata helpers instead of being normalized into a different
  path.
- Make `go/pkg/operator` consume the `go/pkg/sdk/admin` HA wrapper for remote
  admin operations. The operator should not import generated `oapi` internals
  directly except in wrapper tests, and it should not maintain separate method
  paths, request structs, response structs, retry classification, or auth header
  plumbing for `/admin/v1/ha`.
- Make the supported Zig CLI consume `zig/pkg/antfly/src/admin/` bindings and
  route constants from the same `specs/openapi/antfly/admin.yaml` contract.
  Any CLI-only code path must be limited to local filesystem recovery,
  pod-local volume manipulation, or explicit break-glass workflows.

### Phase 9: Operator Integration

The Kubernetes operator integration lives in `go/pkg/operator`. The Zig HA
planner should remain a portable policy engine, but CRD fields, status
conditions, admin-job targeting, service updates, and promotion automation must
be validated against that operator package.

- Add CRD fields for HA mode, standby topology, sync policy, failure policy,
  retention caps, durable runtime WAL/fence paths, and automatic-failover
  policy.
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
  summaries; live calls, auth header installation, retry/error classification,
  and request/response decoding should go through the SDK wrapper. This keeps
  the operator on the same API compatibility path as other Go consumers and
  avoids drift between operator automation and the public Go SDK.
- Support authenticated admin endpoints by letting the operator read a bearer
  token from a configured process environment variable, defaulting to
  `ANTFLY_HA_ADMIN_TOKEN`. Kubernetes should inject that variable into the
  operator pod from a Secret; the operator should not require broad direct
  Secret read permissions just to make HA admin API calls.
- Support runtime-side admin auth by passing `--ha-admin-token-env` from
  `spec.highAvailability.runtime.adminTokenEnvVar`. Antfly pods should receive
  the same token through `spec.standalone.envFrom` or the explicit
  `spec.highAvailability.runtime.adminTokenSecretRef` secret-key injection.
  Admission should reject `adminTokenSecretRef.optional=true`, and the process
  should fail closed if the configured env var is missing or empty.
- Scope `spec.highAvailability.runtime` to operator Standalone mode until the
  split metadata/data topology has first-class HA process wiring. Admission
  should reject runtime fields outside Standalone mode instead of accepting settings
  that are never passed to the Zig process.
- Publish each executable planned action with its typed admin HTTP method/path
  and target admin URL, while keeping CLI argv as a compatibility and
  break-glass execution hint.
- Target former-primary rewind at the former primary's admin URL, not the
  current primary. Target reseed scheduling/slot marking at the current primary,
  then run any data-replacement step through a pod-local helper on the node being
  reseeded.
- Expose a `highAvailability.runtime.formerPrimaryLogPath` operator field and
  pass it to `antfly standalone --ha-former-primary-log` on nodes that may need
  rewind/rejoin. For the original primary, this should usually be the same
  durable file as `highAvailability.runtime.primary.logPath`; after failover it
  becomes the former primary's local evidence for timeline divergence checks and
  rewind decisions.
- Use CLI-backed Kubernetes Jobs only for workflows that need pod-local mounted
  files, shared backup volumes, or explicit break-glass execution.
- Publish lag, degraded, unhealthy, and reseed-required conditions.
- Coordinate fenced failover through Kubernetes Lease, storage fencing, or
  another configured ownership authority.
- When Kubernetes Lease fencing is used, scope the Lease to the exact HA
  identity and promotion boundary it protects. The operator should write and
  validate machine-readable Lease annotations for `cluster_id`, `shard_id`,
  `table_id`, current primary id, timeline, epoch, and primary LSN before
  treating the Lease as a ready fence. A stale Lease from an older timeline,
  epoch, primary, or observed LSN must block automatic promotion even if its
  holder and renewal timestamp are otherwise valid.
- Update Services, routes, and client-facing primary endpoints after promotion.
- Automate former-primary demotion, rewind, or reseed after failover.
- Keep automatic promotion disabled unless Phase 7 fencing requirements are
  satisfied by the configured environment.

## Operator Runbooks

Hot-standby HA should ship with boring operator runbooks before it is called
production grade. The runbooks should cover the Kubernetes operator path in
`go/pkg/operator`, the typed `/admin/v1/ha` path generated from
`specs/openapi/antfly/admin.yaml`, and the supported Zig CLI path. They should
make the split explicit:

- typed `/admin/v1/ha` calls are the preferred operator and SDK automation
  surface;
- CLI commands are human and break-glass helpers, or pod-local helpers for
  workflows that need mounted data paths;
- `/internal/v1` remains runtime-to-runtime replication plumbing, not an
  operator control surface.

The runbook prerequisites should require `HotStandby` mode, an explicit HA
identity (`clusterID`, timeline, epoch, and current primary), standby topology,
admin URLs, durable runtime paths, and matching admin bearer-token environment
injection for the operator and Antfly pods. Automatic failover additionally
requires `executePlannedActions`, a supported fencing authority, route selectors
for the primary endpoint, and admin URLs for the primary, standbys, and any
former-primary repair target.

Daily checks should teach operators to inspect `status.haStatus` and the HA
conditions, especially `HAAvailable`, `HADegraded`, `HAUnhealthy`, `HALagging`,
`HARetentionPressure`, `HAReseedRequired`, and
`HAAutomaticFailoverReady`. The minimum status fields to check are primary
admin reachability, primary LSN, standby received/applied/safe-read LSNs,
retention pressure, sync-policy satisfaction, primary route state, former
primary state, and `plannedActions`.

Bootstrap and reseed runbooks should follow the typed planned actions instead
of ad hoc shelling out. A standby seed should be visible as actions such as
slot creation, seed scheduling, seed bootstrap, and seed completion. A reseed
should be explicit when retained WAL is no longer sufficient. Operators should
verify the action target, admin method/path, admin URL, admin node id, action
receipt, backup manifest, checkpoint, and safe-read progress before marking a
standby healthy. A lagging standby must not pin WAL forever without either a
catch-up path or an operator-visible reseed decision.

Promotion runbooks should require a machine-checkable fence before automatic
promotion. With Kubernetes Lease fencing, the Lease must be scoped to the exact
cluster, shard/table identity, current primary, timeline, epoch, and observed
primary LSN. A stale Lease from an older identity, timeline, epoch, primary, or
LSN blocks automatic promotion. The operator should record planned actions for
fence acquisition, promotion assessment, standby promotion, primary-route
update, and former-primary demotion, rewind, or reseed. Forced promotion should
produce a distinct lossy receipt and should never look identical to a safe
promotion.

Former-primary runbooks should be first-class. A returning old primary cannot
resume writes merely because it restarted. It must observe the newer timeline
and either demote, rewind using retained WAL and fork evidence, or reseed from
the current primary. Rewind targets the former primary's admin URL because it
uses that node's local state. Reseed scheduling targets the current primary for
slot/seed coordination and uses a pod-local helper only for the data replacement
step on the former primary. The status and receipt must prove which node was
acted on and whether rewind or reseed was required.

Alert guidance should cover admin 401/403 responses, missing or unreachable
admin URLs, missing typed result evidence, unhealthy or lagging standbys,
retention pressure, reseed requirements, degraded synchronous commit, stale
fences, unsafe promotion requests, and old-primary write attempts after
promotion. The useful evidence is not just log text: preserve `plannedActions`,
typed admin receipts, fence token/generation, timeline/epoch/LSN boundaries,
admin action ids, target node ids, and route-update status so an operator can
explain exactly why the system promoted, refused to promote, rewound, or
required a reseed.

## Production Readiness and Postgres-Parity Gaps

Antfly should not call hot standby production grade merely because records can
stream from one process to another. The production bar is that ordinary and
adverse operational workflows are typed, observable, restartable, and fenced.
The remaining work before this mode has the bulk of Postgres-style HA parity is:

- real `antfly standalone` primary and standby runtime wiring, including durable
  replication logs, received-WAL logs, slot stores, progress WALs, fence WALs,
  former-primary logs, read/write gates, admin auth, and background-job gating;
- a stable `/admin/v1/ha` OpenAPI contract generated into Zig admin bindings
  and the Go SDK admin wrapper, with the Kubernetes operator using that wrapper
  instead of shelling out or duplicating HTTP code;
- base-backup creation, manifest pinning, file/object copy, checksum
  validation, catch-up, and resumable seed workflows against real filesystem
  and object-store layouts;
- asynchronous replication with explicit received/apply progress and durable
  slots;
- synchronous commit policies matching the intended Postgres semantics:
  `remote_write`, `remote_apply`, `ANY`, `FIRST`, `ALL`, and clear `block`,
  `fail_closed`, or `degrade_to_async` failure behavior;
- promotion with durable timeline switch records, machine-checkable fence
  receipts, forced-promotion receipts, and old-primary write rejection;
- `pg_rewind`-style former-primary repair where retained WAL is sufficient,
  plus explicit reseed when rewind is unsafe or retention has expired;
- standby read routing and freshness controls such as stale reads,
  `at_least_lsn`, and primary-only read-after-write routing;
- WAL retention pressure handling, slot expiration, reseed-required status, and
  operator policies that prevent dead standbys from pinning WAL forever;
- versioned replication record compatibility tests and upgrade/downgrade
  behavior for mixed-version rolling deployments;
- metrics, logs, audit events, status conditions, action receipts, and runbooks
  for slot lag, retained WAL, degraded synchronous commit, promotion readiness,
  replay failure, and reseed requirements;
- crash, partition, and replay simulation coverage plus real process e2e and
  operator e2e coverage.

This list is intentionally larger than "stream a WAL record to another process".
The production gate should require the main failure paths to be implemented,
tested, observable, and documented before Antfly advertises bulk Postgres-style
HA parity. The minimum feature set is async streaming plus deterministic replay,
base backup and reseed, slots and WAL retention accounting, explicit read-only
standby behavior, fenced promotion, former-primary repair or reseed, generated
admin clients, operator workflows, crash/simulation coverage, and real-process
e2e coverage.

For bulk Postgres-style HA parity beyond the first production target, the large
remaining areas are `pg_rewind`-style former-primary repair depth, richer
synchronous commit policy support, WAL archive or PITR-like recovery options,
robust observability, optional cascading or relay replication, cross-region
latency policy, richer read-replica routing, and operator workflows that make
the common cases boring. Those features can follow the core HA path, but they
should not be confused with the minimum safe production surface.

The minimum production-grade target is a boring single-primary system: the
primary streams ordered records, standbys recover and apply deterministically,
promotion requires a fence and creates a new timeline, the former primary cannot
silently continue, and the operator can explain every action it took.

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
