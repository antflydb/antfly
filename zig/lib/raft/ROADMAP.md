# Raft Roadmap

This file tracks parity gaps and next work for `antflydb/raft`. Stable
architecture and shipped capabilities belong in [RAFT.md](RAFT.md).

## Parity Status

The project is aiming for strong single-group behavioral parity with
`go.etcd.io/raft/v3`, not just API shape parity. RAFT.md describes the
validation stack (direct core/cluster tests, differential traces, seeded
stress generation) that backs this effort.

Recent matrix expansion has covered:

- `AsyncStorageWrites` combined with lease-based reads
- async lease-based overlap traces for transfer, joint config, restart,
  reelection, expiry/replacement, and pre-vote automatic reelection
- promoted async stress traces for general async churn, async lease churn,
  async restart/snapshot-style churn, and async lease restart/snapshot-style
  churn

This is not yet perfect etcd parity. See Remaining Gaps below.

## Remaining Gaps

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
