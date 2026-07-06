# Relocations

This note captures the production relocation design for Antfly table group
placements. The goal is to move hot or draining shards without making reads
depend on an empty replacement copy.

## Problem

A placement move is currently planned as a desired placement-set change:

1. the planner emits a new placement for the target node,
2. the reconciler removes placements that no longer appear in the desired set,
3. hosted read routing follows the visible placement set.

That is too direct for a data-bearing shard. A placement can disappear from
metadata before the replacement has copied a snapshot, replayed deltas, and
become safe for reads. In an RF=1 deployment this creates an availability gap:
metadata may say the drained node no longer owns the group, while the new owner
is still empty or behind.

The fix is not to block hot-shard movement. Hot shards still need to move. The
fix is to split relocation into phases and make the cutover explicit.

## Industry Pattern

Mature distributed storage systems separate "start moving data" from "serve
from the new copy":

- CockroachDB sends snapshots during rebalancing and then replays Raft log
  entries since the snapshot before the replica is current. It also applies
  admission control to snapshot ingestion so relocation work does not starve
  foreground traffic.
- TiKV/TiDB scheduling is expressed as add replica, remove replica, and
  transfer leader operators. PD balances hot regions, but limits scheduling
  speed and treats an offline store as an intermediate state while regions move
  away.
- etcd/Raft learners join as non-voting members, receive data from the leader,
  and can only be promoted after their log has caught up. Learners do not serve
  normal client reads/writes.
- Cassandra bootstraps/replaces nodes by streaming token ranges first. Cleanup
  of old range data is delayed until the operator is satisfied the new copy is
  working.

The common rule is:

```text
add target copy -> copy/replay/catch up -> mark target serving -> remove source
```

## Desired Antfly Semantics

Relocation should be a phased operation:

```text
serving_source
  -> target_planned
  -> target_bootstrapping
  -> target_replaying
  -> target_cutover_ready
  -> target_serving
  -> source_draining
  -> source_removed
```

For an existing, data-bearing group:

- New target placements must start non-serving.
- The source placement remains serving until replacement readiness is durable.
- Reads route only to serving placements.
- Strong reads route to the current serving leader/leaseholder.
- Stale/fallback reads may route to serving followers, but not to
  bootstrapping or replaying targets.
- Removal of the source placement is gated on target readiness, not merely on
  desired-set membership.

For an empty new group:

- The first placement may become serving immediately after the local hosted
  replica is active, because there is no source data to preserve.

## Readiness Gates

A target placement is `target_cutover_ready` only when all of these are true:

- The target has installed its snapshot or otherwise copied the durable base
  state for the group.
- The target has replayed all required deltas through a durable applied index.
- The target reports `replay_required == false` or `replay_caught_up == true`.
- The target is not reporting active bootstrap/backfill work for this group.
- The target's visible primary/doc count is not below the source or committed
  relocation watermark for the group.
- The target's table identity namespace matches the range/table identity.
- The target has a valid local replica status and can acquire/serve the
  required read consistency class.
- Derived artifacts required for normal reads are either caught up or visibly
  degraded with bounded errors.

The readiness proof should be durable enough that metadata restart cannot
forget that the target was or was not ready.

## Hot-Shard Movement

Relocation must continue to move hot shards. The safety gate applies to cutover,
not to starting the move.

Use throttles instead of blocking:

- max concurrent relocations per source node;
- max concurrent relocations per target node;
- max concurrent relocations per table;
- per-node snapshot/backfill byte budgets;
- per-node replay work budgets;
- cooldown/hysteresis before moving the same group again;
- higher priority for node drain and repair than pure rebalancing;
- lower priority or slower budgets for hot shards when the target is already
  saturated.

The scheduler should be able to keep a hot relocation in progress while
continuing to serve from the source.

## Metadata Model

Placement metadata needs an explicit serving state rather than inferring
readability from placement existence.

One possible model:

```zig
pub const PlacementServingState = enum {
    planned,
    bootstrapping,
    replaying,
    cutover_ready,
    serving,
    draining,
};
```

Required persisted fields:

- `serving_state`;
- relocation id or generation;
- source node/store id when this placement is a relocation target;
- base snapshot sequence or source applied sequence;
- target applied sequence;
- committed vector/doc count watermark where applicable;
- last transition reason/error for operator visibility.

The exact wire/storage shape can differ, but the state must be explicit and
durable.

## Planner and Reconciler Rules

The planner may choose a new desired target immediately when a node is draining,
overloaded, or a better placement exists. For an existing data-bearing group,
that target is a relocation target, not an immediately serving replacement.

The reconciler should:

- upsert the target placement before scheduling source removal;
- keep the source placement serving while the target is not ready;
- emit bootstrap/replay work for the target;
- promote the target to serving only after readiness gates pass;
- transfer leadership/lease where required;
- remove or mark the source non-serving only after target promotion is durable;
- make drain completion depend on source removal after cutover, not just on the
  planner no longer wanting the source.

The current direct-removal pattern in
`zig/pkg/antfly/src/metadata/reconciler.zig` should become a cutover-aware
removal. Current desired placement calculation in
`zig/pkg/antfly/src/metadata/placement_planner.zig` should not mark a new
replacement for an existing group as fully serving just because a previous group
placement exists.

## Routing Rules

Hosted routing should distinguish placement existence from serving eligibility:

- `serving`: eligible for reads according to consistency policy.
- `cutover_ready`: eligible only during controlled promotion, not general reads.
- `replaying` / `bootstrapping` / `planned`: not eligible for client reads.
- `draining`: eligible as source until replacement promotion is durable; after
  promotion it is not eligible.

Lookup fallback should skip non-serving placements and should not treat an empty
target response as proof that a key is absent while a serving source still
exists.

## Tests

Required regressions:

- RF=1 node drain keeps reads available: insert rows, request node shutdown,
  add replacement placement, verify source remains serving until replacement
  reports caught-up, then kill source and verify reads still succeed.
- Target starts empty: verify metadata shows relocation in progress and reads
  route to source, not target.
- Target catches up: verify promotion to serving and then source removal.
- Hot shard movement: verify overloaded source starts relocation but does not
  exceed configured migration budgets.
- Restart during relocation: verify metadata reopens with the same serving
  source and non-serving target state.
- Restart after cutover: verify metadata reopens with target serving and source
  removable/removed.
- Route fallback: verify lookup does not return `found=false` from a
  bootstrapping target while a serving source has the row.
- Node shutdown completion: verify shutdown is not `safe_to_terminate` until all
  data-bearing groups have cut over.

## Non-Goals

- Do not solve this by disabling hot-shard movement.
- Do not make readers probe every replica indefinitely.
- Do not rely on process liveness alone as data readiness.
- Do not treat placement-intent absence as proof that a source is safe to kill.
