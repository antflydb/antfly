# Raft Integration

This file is the consolidated architecture and status note for the Antfly-side
Raft integration. Use the top-level [ROADMAP.md](../../../../ROADMAP.md)
for project-wide sequencing and
[pkg/antfly/src/metadata/METADATA.md](../metadata/METADATA.md)
for the active metadata/control-plane status and follow-up work.

## Goal

Use the external `raft` repo as:

- deterministic single-group Raft core
- generic multi-Raft host/runtime
- generic transport, codec, snapshot, storage, and reconciliation seams

Use `antfly-zig` as:

- concrete network transport implementation
- metadata/control-plane integration layer
- state machine and local storage integration
- operational policy layer for real deployment

This keeps consensus/runtime reusable while leaving Antfly-specific behavior in
the Antfly repo.

## Repository Boundary

The external `raft` repo should own:

- `core/`
  - single-group Raft semantics
  - parity with etcd behavior
- `runtime/`
  - multi-Raft host
  - scheduling
  - batching
  - apply/persist queues
  - transport and snapshot interfaces
  - replica lifecycle seams
  - generic metadata-driven reconciliation seam

The external `raft` repo should not own:

- HTTP handlers
- gRPC services
- QUIC/HTTP3 setup
- Antfly metadata watches
- Antfly endpoint resolution
- Antfly auth/TLS policy
- Antfly snapshot REST layout

`antfly-zig` should own:

- concrete network transport
- peer endpoint resolution from metadata
- local replica bootstrap policy
- mapping metadata placement to `ensureReplica` / `removeReplica`
- snapshot upload/download endpoints
- state machine integration for shards
- operational policy and rollout behavior

## Runtime Layers

`ensureReplica` and `ConfChangeV2` are different layers.

- `ConfChangeV2`
  - replicated membership change inside one Raft group
  - proposed and committed through the Raft log
  - changes voters, learners, and joint config
- `ensureReplica`
  - node-local runtime operation
  - makes sure this node is actually hosting a local replica for a group
  - can start from empty local state, persisted local state, or fetched
    snapshot/bootstrap state

In practice, Antfly needs both:

1. metadata decides a node should host a replica
2. local host runs `ensureReplica`
3. group leader proposes `ConfChangeV2`
4. catch-up, snapshot, and promotion happen

The transport split should stay generic in the external `raft` runtime:

- `Transport`
  - host-facing delivery and peer lifecycle seam
- `MessageCodec`
  - frame encoding/decoding
- `FrameDriver`
  - low-level send boundary
- `SnapshotTransport`
  - large snapshot transfer

Antfly's concrete transport currently lives under
[transport](transport).
Plain HTTP remains the right first concrete protocol because it is simple to
debug, covers snapshot upload/download naturally, and does not force QUIC,
HTTP/2, or gRPC choices into the generic runtime seams.

## Module Shape

The Antfly-side raft integration lives under
[pkg/antfly/src/raft](.).

- [mod.zig](mod.zig)
  - Antfly-facing Raft integration entrypoint
- [host.zig](host.zig)
  - wraps `runtime.MultiRaft`
  - owns host config and runtime wiring
- [managed_host.zig](managed_host.zig)
  - binds `MetadataView`, the generic host, durable providers, and the
    reconciler into one control-loop surface
- [metadata_apply.zig](metadata_apply.zig)
  - converts committed metadata changes into `MetadataUpdate` values
- [metadata_view.zig](metadata_view.zig)
  - applies committed metadata updates into placement intent plus peer-route
    state
- [reconciler.zig](reconciler.zig)
  - turns desired local placements into `ensureReplica`, `removeReplica`, and
    peer-route refresh work
- [service.zig](service.zig)
  - queue-driven runtime service around `ManagedHost` / `ManagedHttpHost`
- [runtime_loop.zig](runtime_loop.zig)
  - deterministic node-local runtime driver
- [sim_harness.zig](sim_harness.zig)
  - deterministic host and HTTP-cluster simulation harnesses
- [storage](storage)
  - local replica catalog, persisted raft state, WAL provider, snapshot store,
    and per-replica path layout
- [state_machine](state_machine)
  - routed metadata/data apply and snapshot-builder seams
- [transition_runtime.zig](transition_runtime.zig)
  - raft-side split/merge runtime adapters
- [transition_service.zig](transition_service.zig)
  - service-owned split/merge transition queue
- [transition_checker.zig](transition_checker.zig)
  - stepped invariant checks for split/merge and enrichment ownership
- [enrichment_runtime.zig](enrichment_runtime.zig)
  - leader/readiness-gated enrichment runtime
- [read_gate.zig](read_gate.zig)
  - feature-facing readable-lease request seam

Metadata-specific state lives under
[pkg/antfly/src/metadata](../metadata).
Data-replica apply and shard ownership state lives under
[pkg/antfly/src/data/storage](../data/storage).

## Metadata-Driven Hosting

The metadata layer is the source of truth for:

- which groups exist
- which replicas should live on this node
- peer endpoint addresses
- rebalancing and relocation intent
- leader-transfer or lease-placement intent

The local raft host reconciles to committed metadata. It is not managed as an
independent source of truth.

The intended control flow is:

1. metadata Raft commits desired placement, endpoint, or transition changes
2. metadata apply code projects those changes into the local service boundary
3. local reconciler diffs desired vs actual hosted replicas
4. local reconciler calls `ensureReplica`, `removeReplica`, and peer refresh
   APIs
5. raft-side services run bounded host rounds and transition steps
6. nodes report observed status back through metadata-managed status

For deterministic tests, the simulation harnesses wrap the metadata applier,
update queue, runtime loop, HTTP transport, and transition runtime so add,
remove, restart, rejoin, split, merge, and lease-read sequences can be stepped
without hidden timing.

## Current Status

The raft substrate is substantially in place.

Completed or substantially in place:

- external `raft` runtime is imported and wrapped by `host.zig`
- metadata-facing adapters exist: `metadata_apply.zig`, `metadata_view.zig`,
  `reconciler.zig`, `managed_host.zig`, and `service.zig`
- deterministic runtime stepping exists in `runtime_loop.zig`
- concrete HTTP transport exists under `transport`
- deterministic host and HTTP-cluster simulation harnesses exist
- durable local-replica storage exists with both file-image and WAL-backed raft
  state providers
- managed-host startup can restore hosted replicas from a file-backed catalog
- metadata and data apply paths are split and routed through domain-shaped
  adapters
- metadata and data default adapters persist through the real DocStore / LMDB
  stack
- range-transition storage has split and merge coordinator support with
  restart-safe state
- raft-side transition queue supports split/merge upsert, removal, rollback,
  retry, restart recovery, and concurrent transition isolation
- transition checker coverage catches asymmetric merge state and impossible
  enrichment ownership during split/merge
- leader-scoped enrichment runtime supports simulated, threaded, evented, and
  DB-backed executor paths
- readable-lease hooks reach C API, direct dense search, compat runner, and
  feature-facing `lookup`, `scan`, and `search` paths
- metadata service/server and control-loop surfaces now own most active
  control-plane work

The active follow-up is no longer basic raft bring-up. Most new sequencing
belongs in
[pkg/antfly/src/metadata/METADATA.md](../metadata/METADATA.md),
where the remaining work is metadata topology, placement/rebalance policy,
admin workflows, and Go-contract parity.

## WAL State Policy

The WAL-backed raft state path moved from full-image writes toward append-only
delta records plus checkpoints.

Current shape:

- `persist_ready` appends delta records instead of rewriting the full retained
  replica image
- applied watermark is persisted separately so restart can replay only the
  unapplied suffix
- shutdown flush writes a final checkpoint and applied watermark
- checkpoint triggering is based on replay-debt thresholds rather than eager
  per-batch checkpointing
- `managed-host-wal-bench` exposes checkpoint thresholds, repeated runs, and
  crash-like restart mode

The current measured tradeoff is:

- default thresholds keep the hot path on cheap delta appends and accept bounded
  crash-replay debt
- aggressive checkpointing eliminates replay debt but gives back most of the
  write-latency win
- the production default should stay debt-tolerant until broader workloads
  justify changing the fixed thresholds or adding a smarter adaptive policy

Do not weaken the applied-state visibility boundary:

- proposed-but-uncommitted document versions must not be visible to reads
- reads must not be served from speculative leader-local state
- MVCC is not the next performance lever for this raft durability path

## Remaining Raft-Side Work

Keep remaining raft-side work narrow:

- decide whether managed-host durable defaults should switch from `file_image`
  to `wal` after more operational coverage
- keep snapshot/rejoin behavior covered across partial local state loss and
  transport failure cases
- keep runtime/service hardening focused on listener lifecycle, shutdown/drain
  sequencing, metrics, and tracing
- keep product policy in metadata modules rather than pushing it into the
  generic raft runtime or the Antfly raft hosting substrate

## What Multi-Raft Buys Antfly

A real node-local multi-Raft host gives Antfly more than shared transport:

- per-peer batching across groups
- centralized scheduling
- fairness between hot and cold groups
- host-wide backpressure
- grouped persistence/apply handling
- snapshot throttling across all groups
- simpler control-plane driven lifecycle

This is a better fit for autoscaling than many mostly-independent per-shard
loops.
