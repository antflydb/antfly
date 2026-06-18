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

Runtime HA validation should be shared but still field-aware. Helpers such as
`paddedHAString` should evolve into a small classifier, for example
`classifyHAString(value) -> ok | missing | padded`, so role validation can reuse
the same whitespace and missing-value rules while preserving field-specific
errors such as `HAPrimaryLogInvalid`, `HAStandbySlotMissing`, or
`HAAdminTokenEnvInvalid`. Do not collapse validation into one generic
string-cleaning function that silently trims operator input. HA runtime identity
and path fields should fail closed when they contain leading or trailing
whitespace, because those values become durable node identity, WAL path, fence,
slot, URL, or token-env configuration. Field-specific validators should layer
type checks on top of the shared classifier:

- paths must pass path-specific safety rules such as absolute or storage-root
  bounded paths where appropriate;
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
from `spec.highAvailability.runtime.adminTokenSecretRef` or `spec.swarm.envFrom`.
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

The review outcome is to keep the HA helpers reusable without hiding
configuration mistakes. `paddedHAString` should not become a broad
`validateHAString` function that tries to validate every HA string in one place.
Use a shared missing/padded classifier, then let field-specific validators
return precise errors and layer type-specific rules for paths, node ids, slot
names, token environment variables, and URLs. Validators must reject padded
input; they must not trim and continue.

Add `test_standby.py` as a black-box Zig e2e test once the admin API and
runtime wiring are usable through real processes. The test should exercise the
supported Postgres-style path: primary start, slot creation, seed/bootstrap,
standby start, primary writes, standby catch-up and read-only behavior, standby
restart with replay resume, and later fenced promotion with old-primary write
rejection.

Add Zig simulation coverage for the correctness cases that process e2e cannot
explore exhaustively: receive-before-apply crashes, apply-before-ack crashes,
primary crashes before and after synchronous acknowledgement, duplicate/gap or
out-of-order WAL, promotion with and without valid fencing, old-primary
rejoin/rewind/reseed, retained-WAL expiry, and timeline switch propagation.

Treat production readiness as Postgres-style operational parity, not merely
successful streaming. Before this mode is production grade, Antfly needs runtime
primary/standby wiring, stable generated admin clients, operator integration
through the Go SDK wrapper, base backup, sync commit, fencing, promotion and
timeline repair, standby freshness controls, WAL retention pressure handling,
auth/audit/metrics/runbooks, format compatibility, crash/e2e/operator tests,
and the remaining parity work such as `pg_rewind`-style repair, synchronous
commit policy depth, WAL archive or PITR-style recovery, observability, optional
cascading or relay replication, and ergonomic operator workflows.

The first runtime implementation should keep validation reusable without making
it vague. `paddedHAString` should become a shared classifier, not a single
generic validator that tries to do every check. The intended shape is:

```zig
const HAStringValidation = enum { ok, missing, padded };

fn classifyHAString(value: ?[]const u8) HAStringValidation
```

Field-specific validators should translate that classifier into field-specific
errors and then add type-specific checks. A primary log path should still return
an error such as `HAPrimaryLogInvalid`, a standby slot should still return
`HAStandbySlotMissing`, and an admin token env var should still return
`HAAdminTokenEnvInvalid`. The shared helper should classify missing or padded
input; the caller should decide which path, node-id, slot-name, env-var, or URL
rule applies and which precise error belongs in the API or CLI response. It
should never trim and continue, because doing so can hide operator mistakes in
durable HA identity, WAL path, fence, slot, URL, or token-env configuration.

The type-specific validation layer should include:

- paths: absolute or normalized as required by the runtime, and bounded to the
  configured storage root when a field refers to local storage;
- node ids and slot names: restricted character sets and bounded lengths;
- environment variable names: the same validation used for process env names;
- URLs: parsed as URLs and rejected when they contain hidden leading/trailing
  whitespace.

The first `test_standby.py` should be a real process e2e once the runtime and
admin surfaces are usable. It should start a primary and standby, create a slot
or seed workflow, write data through the primary, wait for standby catch-up,
verify read-only standby behavior, restart the standby, and verify replay
resumes. After fence and promotion support is usable, the same e2e should
promote the standby and verify the old primary rejects writes or must rejoin
through rewind/reseed. Argument-validation coverage belongs in unit tests; this
e2e should prove the supported Postgres-style user path with real Antfly
processes, files, admin calls, and client-visible reads/writes.

The Zig simulation layer should carry the deeper correctness burden. It should
model crash, restart, partition, duplicate/gap/out-of-order WAL, promotion,
old-primary rejoin, rewind/reseed, retained-WAL expiry, and timeline switch
interleavings before the project relies on black-box e2e as evidence that the
state machine is correct.

The production-grade bar is feature and failure-case parity, not merely "records
stream." Before Antfly treats this mode as production ready, the implementation
needs runtime primary/standby wiring, generated Zig and Go admin clients, operator
integration through the Go SDK wrapper, base backup, sync commit, fencing,
promotion, timelines, former-primary repair, standby read freshness, retention
pressure handling, auth, metrics, runbooks, format compatibility, crash tests,
real process e2e, and operator e2e. For bulk Postgres-style HA parity, the
remaining gaps after basic streaming are former-primary repair comparable to
`pg_rewind`, deeper synchronous commit policy support, WAL archive or PITR-like
recovery options, robust observability, optional cascading or relay replication,
and operator workflows that make the common cases boring.

## Test Strategy

HA needs both black-box e2e coverage and deterministic simulation coverage. The
Python e2e suite should add a Zig-backed standby test, for example
`test_standby.py`, once the runtime and admin API are usable as real process
surfaces. That e2e test should not exist only to assert argument validation. It
should launch real Antfly processes and cover the user-visible Postgres-style
flow:

1. start a primary;
2. create or reserve a replication slot;
3. seed and start a standby;
4. write data to the primary;
5. wait for standby catch-up and verify read-only standby visibility;
6. restart the standby and verify local received-WAL replay plus stream resume;
7. later, fence and promote the standby, then verify the old primary rejects
   writes or must rejoin through rewind/reseed.

The Zig simulation tests should carry most of the correctness burden because
they can explore interleavings that are expensive or flaky in process e2e. Add
model or harness coverage for:

- crash after WAL receive before apply;
- crash after apply before acknowledgement;
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
correct under crash, restart, partition, and replay ordering stress.

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
- Wire the supported Zig `antfly swarm` runtime so a primary can be started with
  durable HA replication log, slot store, promotion fence WAL, optional
  former-primary rewind log, optional admin bearer-token env var, node id, and
  identity flags. That runtime path should attach the same `/admin/v1/ha`
  executor, durable fence store, former-primary log handle, admin auth
  enforcement, and `/internal/v1/ha/replication` executor used by tests and the
  CLI, rather than requiring a bespoke harness to expose primary-side HA
  operations or rejoin/rewind workflows.
- Wire the supported Zig `antfly swarm` runtime so a standby can also be started
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
  the same token through `spec.swarm.envFrom` or the explicit
  `spec.highAvailability.runtime.adminTokenSecretRef` secret-key injection.
  Admission should reject `adminTokenSecretRef.optional=true`, and the process
  should fail closed if the configured env var is missing or empty.
- Scope `spec.highAvailability.runtime` to operator Swarm mode until the
  split metadata/data topology has first-class HA process wiring. Admission
  should reject runtime fields outside Swarm mode instead of accepting settings
  that are never passed to the Zig process.
- Publish each executable planned action with its typed admin HTTP method/path
  and target admin URL, while keeping CLI argv as a compatibility and
  break-glass execution hint.
- Target former-primary rewind at the former primary's admin URL, not the
  current primary. Target reseed scheduling/slot marking at the current primary,
  then run any data-replacement step through a pod-local helper on the node being
  reseeded.
- Expose a `highAvailability.runtime.formerPrimaryLogPath` operator field and
  pass it to `antfly swarm --ha-former-primary-log` on nodes that may need
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

## Production Readiness and Postgres-Parity Gaps

Antfly should not call hot standby production grade merely because records can
stream from one process to another. The production bar is that ordinary and
adverse operational workflows are typed, observable, restartable, and fenced.
The remaining work before this mode has the bulk of Postgres-style HA parity is:

- real `antfly swarm` primary and standby runtime wiring, including durable
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
- metrics, logs, status conditions, action receipts, and runbooks for slot lag,
  retained WAL, degraded synchronous commit, promotion readiness, replay
  failure, and reseed requirements;
- crash, partition, and replay simulation coverage plus real process e2e and
  operator e2e coverage.

Features such as WAL archive/PITR recovery, cascading or relay replication,
cross-region latency policy, and richer read-replica routing can follow the
core HA path, but they should not be confused with the minimum safe production
surface. The minimum production-grade target is a boring single-primary system:
the primary streams ordered records, standbys recover and apply deterministically,
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
