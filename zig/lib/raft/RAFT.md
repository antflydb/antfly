# antflydb/raft

Experimental Zig 0.16 Raft module for Antfly.

The module is intended to become a reusable, production-oriented consensus
library for Antfly's metadata/control plane and shard-local replication. The
shape is:

- an `etcd/raft`-style deterministic single-group core
- a Dragonboat-inspired node-local multi-Raft runtime
- transport, storage engines, snapshots, leases, and Antfly product behavior
  kept outside the core

The target is not a toy Raft library and not a mechanical port of any one
implementation.

## Core Boundary

`antflydb/raft` is split into two layers:

- `src/core/`: deterministic Raft state machine logic
- `src/runtime/`: multi-group hosting and IO coordination

The core:

- does not perform network IO
- does not perform disk IO
- does not spawn threads
- does not know about Antfly-specific concepts
- remains deterministic given messages, ticks, storage state, and injected
  randomness

The primary public core interface is:

- `RawNode`
- `Ready`
- `Storage`
- `Message`

Callers drive the core by feeding ticks and messages into `RawNode`, persisting
and sending the outputs from `Ready`, then calling `advance()` after those
outputs are handled.

The core API is intentionally close to this shape:

```zig
pub const RawNode = struct {
    pub fn init(alloc: Allocator, cfg: Config, storage: Storage) !RawNode;
    pub fn tick(self: *RawNode) void;
    pub fn step(self: *RawNode, msg: Message) !void;
    pub fn propose(self: *RawNode, data: []const u8) !void;
    pub fn proposeConfChange(self: *RawNode, cc: ConfChange) !void;
    pub fn ready(self: *RawNode) Ready;
    pub fn hasReady(self: *RawNode) bool;
    pub fn advance(self: *RawNode, ready: Ready) void;
    pub fn status(self: *RawNode) Status;
};
```

## Runtime Boundary

The runtime exists because a real Antfly node needs to host many Raft groups
efficiently. Multi-Raft is not transport.

Transport answers:

- how messages and snapshots move between nodes

Multi-Raft answers:

- how one node runs many Raft groups coherently

The runtime owns:

- group lifecycle and registry
- tick scheduling
- `Ready` polling and draining
- local persistence ordering
- apply ordering
- outbound message dispatch
- inbound message demux
- snapshot throttling
- host-level fairness and backpressure

The host should be the only place that ticks groups, routes inbound messages by
`group_id`, collects ready work, persists ready state, applies committed work,
and sends outbound messages.

## Runtime Structure

The runtime layer is organized around these files:

- `src/runtime/group.zig`
  - wraps one `core.RawNode`
  - owns one group's local consensus runtime state
  - exposes `tick`, `step`, `ready`, `advance`, and `status`
- `src/runtime/multi_raft.zig`
  - node-local host for many groups
  - owns registry, scheduler, ready draining, and hooks
- `src/runtime/scheduler.zig`
  - deterministic, quiescence-aware scheduling
  - priority boosts for groups with ready work or recent activity
- `src/runtime/transport_iface.zig`
  - outbound message seam by `group_id`
- `src/runtime/storage_iface.zig`
  - raft-log durability seam and state-machine apply seam
- `src/runtime/snapshot_iface.zig`
  - host-level snapshot throttling seam
- `src/runtime/snapshot_transport_iface.zig`
  - large snapshot transfer and fetch/install seam

Each hosted group carries at least:

- `group_id`
- `local_node_id`
- `core.RawNode`
- async-storage-write mode

The host carries:

- group registry
- scheduler
- transport hook
- storage hook
- state-machine hook
- snapshot throttling hook
- bounded outbound/apply queues
- metrics and backpressure state

The current host API is centered on explicit group lifecycle, inbound message
routing, and host rounds:

```zig
pub const MultiRaft = struct {
    pub fn init(alloc: Allocator, cfg: RuntimeConfig, hooks: RuntimeHooks) MultiRaft;
    pub fn addGroup(self: *MultiRaft, cfg: GroupConfig) !void;
    pub fn removeGroup(self: *MultiRaft, group_id: GroupId) bool;
    pub fn ensureReplica(self: *MultiRaft, desc: ReplicaDescriptor) !EnsureReplicaResult;
    pub fn removeReplica(self: *MultiRaft, group_id: GroupId) !void;
    pub fn step(self: *MultiRaft, group_id: GroupId, msg: Message) !void;
    pub fn tickGroup(self: *MultiRaft, group_id: GroupId) !void;
    pub fn tickBatch(self: *MultiRaft, alloc: Allocator) ![]GroupId;
    pub fn tickAll(self: *MultiRaft) void;
    pub fn runRound(self: *MultiRaft, max_tick_groups: usize, max_ready_groups: usize) !HostRound;
};
```

## Transport Layering

`MultiRaft` remains the execution engine. Transport stays below the host.

The transport stack is split into:

1. `src/runtime/transport_iface.zig`
   - host-facing message transport
   - peer lifecycle
   - group serving lifecycle
   - inbound routing callback surface
2. `src/runtime/codec_iface.zig`
   - message framing and decoding
   - codec/version/compression boundary
   - protobuf, custom binary, or other encodings
3. `src/runtime/snapshot_transport_iface.zig`
   - large snapshot transfer
   - direct send/install
   - remote fetch/download by locator
   - cancellation hooks
4. `src/runtime/frame_driver_iface.zig`
   - low-level frame send seam below codec-aware transport hosting

Raft messages and snapshot blobs are operationally different: messages are
small and latency-sensitive, while snapshots are large and throughput-sensitive.
They should not be forced through one abstraction.

These remain host-level rather than transport-level:

- per-peer batching across groups
- host rounds
- quiescing
- host-level metrics
- persistence/apply batching

## Storage And Snapshots

The core depends on an abstract read interface supporting:

- initial state load
- term lookup
- entries range read
- last index
- first index
- snapshot load

The runtime depends on a write path supporting:

- persist `HardState`
- append entries
- persist snapshot metadata
- compact log
- install snapshot

The module should not bind itself to LMDB, Pebble, RocksDB, or the Antfly Zig
DB.

Snapshots have two layers:

- logical snapshot metadata in the Raft core
- external snapshot materialization handled by runtime or integration code

A snapshot should contain membership state, applied/committed index markers,
and caller-owned opaque state payload or reference. The module should not assume
a specific snapshot file format.

## Metadata Boundary

For Antfly-style systems:

- the metadata cluster is the control plane
- the multi-Raft host is the execution engine

The metadata/control layer decides:

- which groups should exist on the node
- replica placement
- leader-transfer intent
- split, merge, and relocation intent
- lease ownership metadata above the shard-local Raft layer

The host executes:

- add local group
- remove local group
- ensure local replica from placement intent
- route Raft message
- tick local groups
- drain ready work

This keeps product policy out of the Raft core and out of the generic runtime.

## Antfly Fit

Antfly should continue to use a multi-Raft shape:

- one small metadata/config Raft group for shard map, placement, split/merge
  state, and cluster-wide coordination
- one Raft group per data shard for replicated writes and shard-local state
- leases granted by the shard leader for expensive enrichment work

This gives:

- isolation between shards
- independent replication and recovery per shard
- natural shard-level leadership
- direct mapping between ownership and enrichment scheduling

The Raft module should not implement a distributed lease service. Instead, it
should expose primitives needed for caller-managed, leader-issued leases:

- term-bound validation helpers
- monotonic epoch helpers
- optional clock-skew-safe deadline utilities

The recommended Antfly pattern is that a lease is a replicated record in shard
state, only the current leader may grant or renew it, and workers present the
lease epoch/term on state-changing writes.

## Non-Goals

This module does not embed:

- transport servers
- a storage engine
- lease orchestration
- shard splitting workflows
- enrichment orchestration logic
- Antfly document, index, or query semantics
- admin UI or operational CLI

Those belong in higher integration layers.

## References

Use `go.etcd.io/raft/v3` as the semantic reference for:

- state machine semantics
- election and leader transitions
- log replication behavior
- pre-vote
- `ReadIndex`
- learners and non-voters
- membership changes
- message handling model
- `Ready`/`advance` interfaces

Use Dragonboat as the reference for:

- multi-group scheduling
- batching and coalescing
- transport/runtime organization
- snapshot and log compaction orchestration
- operational patterns for large numbers of groups

Do not transliterate `etcd/raft` line by line. Do not port Dragonboat wholesale.

## More Detail

See [ROADMAP.md](ROADMAP.md) for current implementation state, parity status,
remaining gaps, and next work.
