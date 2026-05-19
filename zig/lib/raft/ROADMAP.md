# Raft Roadmap

This file tracks current state, parity, gaps, and next work for
`antflydb/raft`. Stable architecture belongs in [RAFT.md](RAFT.md).

## Current State

The repo now has a broad single-group core and the first real multi-Raft runtime
pass. The implementation is past skeleton bring-up.

The single-group Zig core covers:

- leader election
- pre-vote
- check-quorum
- leader transfer
- `ReadIndex`
- lease-based reads
- snapshots, retry, abort, and restore
- learners
- joint consensus / `ConfChangeV2`
- restart, replay, and compaction paths
- `AsyncStorageWrites`
- proposal forwarding and `DisableProposalForwarding`
- `MaxSizePerMsg`
- `MaxInflightMsgs`
- `MaxInflightBytes`
- `MaxUncommittedEntriesSize`
- `MaxCommittedSizePerReady`
- `Applied`
- `ForgetLeader`
- `StepDownOnRemoval`
- `DisableConfChangeValidation`
- public randomness API for simulation-style testing

The runtime has:

- real `Group`
- real `MultiRaft` host owning groups
- round-robin and priority-aware scheduler
- quiescence-aware scheduling and ready draining
- explicit transport, storage, state-machine, snapshot, and backpressure
  interfaces
- async-aware `processReady` handling for local storage/apply messages
- host-round execution via `runRound`
- host-side disk batcher seam
- concrete in-memory disk batcher
- host-side apply queue seam
- concrete queued apply worker
- host-owned bounded apply backlog with per-round draining
- host-owned bounded outbound queue with per-round transport draining
- concrete limit-based backpressure policy
- retry/defer behavior through bounded outbound/apply host queues
- lightweight host metrics snapshot
- control-plane command surface in `src/runtime/control_plane.zig`
- replica catalog and factory seams for restart-safe local hosting
- concrete in-memory replica catalog and in-memory replica factory
- concrete file-backed replica catalog
- host-driven replica restart scan through the control-plane API
- metadata-driven reconciliation layer
- placement provider seam and in-memory placement provider
- replica reconciler driving `ensureReplica`, `removeReplica`, and peer refresh

The transport layer has:

- in-memory transport host
- group serving lifecycle
- peer lifecycle tracking
- inbound delivery for tests
- debug/test peer-batch codec
- binary peer-batch codec intended as the first production-oriented framing
  layer
- codec-backed transport host
- per-group peer route table
- peer upsert/refresh
- bounded retry/backoff across host rounds
- inbound frame decode back into served groups
- local-file snapshot transport implementation
- outbound snapshot routing through the snapshot transport seam
- host-driven snapshot fetch/install through the snapshot transport seam
- transport metrics for lifecycle, flush activity, peer-batch flushes, and
  snapshot sends

## Parity Status

The project is aiming for strong single-group behavioral parity with
`go.etcd.io/raft/v3`, not just API shape parity.

The validation stack includes:

- direct Zig core tests
- direct Zig cluster/harness tests
- checked-in differential traces compared against etcd
- seeded stable/stress trace generation
- explicit random-seed support for reproducible election scheduling

Recent matrix expansion has covered:

- `AsyncStorageWrites` combined with lease-based reads
- async lease-based overlap traces for transfer, joint config, restart,
  reelection, expiry/replacement, and pre-vote automatic reelection
- promoted async stress traces for general async churn, async lease churn,
  async restart/snapshot-style churn, and async lease restart/snapshot-style
  churn

Randomness is a public dependency:

- `Config.random_source`
- `Config.random_seed`
- `core.RandomSource`
- `core.SplitMix64`

This boundary exists so simulation and differential replay can stay
reproducible. The core should remain deterministic given input messages, ticks,
storage state, and injected randomness.

## Remaining Gaps

This is not yet perfect etcd parity.

The main remaining single-group gaps are:

- broader randomized differential parity
- better control of etcd's randomized election schedule in the comparator
- longer restart, partition, reconfiguration, and transfer simulations
- more promoted stress findings turned into named fixtures
- additional native etcd behavior ports, especially timing-heavy overlap cases
- more async overlap promotion, especially longer async stress runs and more
  restart/snapshot-heavy async promotions

The main runtime and transport gaps are:

- no concrete production protocol driver yet
- no threaded disk batcher yet
- no threaded apply-worker model yet
- no advanced fairness heuristics beyond quiesce-aware boosted scheduling
- no direct Antfly metadata watcher yet
- endpoint discovery and retry policy are not yet integrated with the real
  control plane
- no production storage engine binding in the generic runtime

## Next Work

Near-term runtime work:

1. add threaded disk-batch and apply-worker implementations on top of the
   existing seams
2. add richer quiescence heuristics beyond simple activity-based resume
3. add real protocol drivers on top of `codec_transport.zig` and
   `binary_codec.zig`
4. wire metadata-driven ensure-replica flows from Antfly proper
5. expand queue policy into stronger starvation and cost-aware fairness
6. add endpoint discovery and retry policy integration with the real control
   plane

Near-term parity work:

1. prefer fixing the core over shaping traces
2. promote reproducible stress findings into named fixtures
3. keep the stable seeded sweep clean and deterministic
4. keep the stress profile exploratory
5. keep timing-heavy but unstable cases as direct Zig tests until the comparator
   can control the full timeout schedule

## Validation Bar

Before Antfly depends on this module in production:

- port behavioral tests from `etcd/raft` where possible
- keep adding exact differential traces when behavior is deterministic
- run deterministic simulation with seeded failure schedules
- add storage fault injection
- add transport fault injection
- add long-running randomized multi-group simulation
- compare emitted messages, state, commit index, and leadership transitions
  against Go `etcd/raft` for identical input traces

Single-group parity is done enough when:

- remaining unstable cases are mostly comparator/runtime-control issues, not
  missing core behavior
- seeded simulations produce reproducible failures
- the important overlap matrix is covered by either exact differential traces or
  strong native tests

The runtime is done enough for Antfly integration when:

- one node can host many independent Raft groups safely
- persistence, apply, transport, snapshot, and backpressure boundaries are
  explicit and exercised
- fairness and starvation behavior are covered by deterministic tests
- restart-safe local hosting works through the replica catalog/factory seams
- metadata-driven reconciliation can ensure and remove replicas without
  embedding Antfly product policy in the core

## Adoption Path

Recommended Antfly adoption order:

1. Keep Go `etcd/raft` in production.
2. Build the Zig module independently with strong simulation coverage.
3. Prototype one replicated metadata group against the Zig module.
4. Prototype one replicated data shard.
5. Add lease-driven enrichment execution on top.
6. Only then consider replacing the Go consensus path broadly.

If shipping replicated auto-sharding soon is the priority, do not block on this
module. Use the existing Go consensus system and move the DB state machine into
Zig first.

## Bottom Line

Build:

- an `etcd/raft`-style core for correctness
- a Dragonboat-inspired runtime for multi-group scalability

Treat both references as design input, and make correctness validation a
first-class deliverable.
