# Automatic Shard Splits

## Context

Antfly already has the mechanics for shard splitting:
- split eligibility and orchestration in the reconciler
- split execution and cutover in store/table manager code
- simulator coverage for the split lifecycle

What it does not yet have is a clear policy for turning automatic splitting back on in a way that avoids split storms, repeated churn, and source-node overload.

This note captures the intended policy and the reasoning behind it before re-enabling the background allocator.

## Goals

- Re-enable automatic shard splitting without making the system eager or unstable.
- Keep split selection cluster-aware while preventing too many concurrent background splits.
- Avoid immediate re-splitting of fresh child shards.
- Keep configuration and defaults simple enough to reason about in production.

## Non-Goals

- This does not try to make shard sizing mirror Elasticsearch exactly.
- This does not add load-based splitting in this change set.
- This does not change the zero-downtime split state machine itself.

## Design

### 1. Planner chooses candidates cluster-wide

Automatic splits should continue to be chosen in the reconciler from cluster state, not from per-node local heuristics.

Reasoning:
- the shard is the unit that becomes too large
- metadata already has the global view needed to reject unsafe candidates
- a pure per-node limiter would block the wrong thing; the overloaded shard may live on one node while the scheduler budget on another node is irrelevant

### 2. Rate limiting is layered

Automatic split throttling should use several small controls instead of one large quota:

- shard-level cooldown
  - already exists and should remain the first line of defense
- per-table planning cap
  - allow at most 1 automatic split per table per reconciliation cycle
- cluster-wide in-flight budget
  - allow at most 1 new automatic split to be scheduled while another split is already active
- node-local execution cap
  - treat this as a secondary brake, not the primary planning rule
  - useful later if source-node archive or snapshot work proves too bursty

Reasoning:
- per-table fairness prevents one large table from consuming the whole allocator
- a cluster-wide budget prevents split storms across many tables
- shard cooldown prevents immediate re-selection of the same shard
- node-local caps are still useful, but they should protect execution resources rather than drive global planning

### 3. Use a target band, not a single hard threshold

We should think in terms of a target shard size and hysteresis band:

- `target_shard_size_bytes`
- `split_when_above = 2 * target_shard_size_bytes`
- children should usually land near the target size

Reasoning:
- a single threshold makes the system oscillate at the boundary
- a target band gives room for fresh children to grow before they are reconsidered
- it keeps the policy legible even before hot-range or write-rate heuristics are introduced

This change set does not add a separate target-size config. For now the existing `max_shard_size_bytes` remains the split trigger. The planner throttles new work so the current threshold behaves more like a conservative high-water mark.

### 4. Do not mirror Elasticsearch literally

Elastic's shard-sizing guidance is useful as a sanity check, but Antfly shards are not just Lucene shards. They are also:

- a Raft replication unit
- a split archive / snapshot unit
- a replay and cutover unit
- a recovery and failover unit

So the right sizing inputs are:
- split duration
- snapshot transfer time
- replay lag
- recovery time after node failure
- write amplification during split

Elastic-style shard-size bands are a good mental model, but the actual size should be tuned around Antfly's split and recovery behavior, not borrowed directly from Elasticsearch.

## Concrete Policy

### Candidate eligibility

Only consider shards that:
- are not in shard cooldown
- have a full voter set
- have a leader
- are not in a transitioning shard state
- do not have an active `SplitState`
- are not split children still replaying parent deltas
- have fresh shard stats
- exceed the split threshold

These checks already exist and should remain the basis for auto-split selection.

### Automatic planning limits

When choosing new automatic splits:

- schedule at most 1 new split per table per reconciliation cycle
- schedule at most 1 new split cluster-wide if any split is already active

Active split means a shard with an active split state or split-transition state already in progress.

### Execution limits

This change set does not yet add a node-local execution semaphore.

Reasoning:
- the planner limits above are enough to make automatic splitting conservative
- node-local execution caps can be added later if real clusters show archive or snapshot resource contention

## Rollout Plan

1. Keep automatic shard allocation disabled by default.
2. Land planner throttling, finalize-grace plumbing, and config cleanup first.
3. Re-enable auto-splits only in a canary environment.
4. Watch:
   - split duration
   - finalize delay
   - replay catch-up time
   - rollback count
   - repeated splitting of the same table
5. Only after canary success should auto-splits be enabled more broadly.

## Initial Defaults

Recommended conservative starting point:

- `disable_shard_alloc = true` by default
- `max_shard_size_bytes` set high enough to make splits uncommon at first
- `split_finalize_grace_period = 15s`
- planner caps:
  - 1 split per table per cycle
  - 1 cluster-wide in-flight split budget

## Implementation in This Change Set

- add this design note
- fix config parsing to use the actual `disable_shard_alloc` key
- plumb `split_finalize_grace_period` through runtime config
- throttle automatic split planning to:
  - 1 split per table per cycle
  - 0 new automatic splits when another split is already active

## Follow-Ups

- add explicit config knobs for automatic split planning limits
- add load-based split scoring
- add node-local execution budgeting if split-source work proves too expensive
- add metrics for:
  - active splits
  - planned automatic splits
  - split rollbacks
  - split duration
  - replay catch-up duration
