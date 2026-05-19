# Joins

This file describes how joins work in `antfly-zig`, why the distributed path is
more complex than the basic Go service-layer join flow, and what the intended
long-term split is.

## Short Answer

Joins do **not** need to go through Raft for query correctness.

The metadata-backed lease path exists for **distributed shuffle job
coordination**, not for read consistency:
- picking a finalizer owner
- converging multiple API servers on the same worker/finalizer choice
- bounded retry/handoff
- replay/resume of durable distributed shuffle jobs

For common joins, the preferred path is still a fast non-Raft path.

## Current Execution Modes

`antfly-zig` now has two distinct distributed shuffle modes on the stateful
public query path:

1. `distributed_transient`
- used for smaller/common distributed shuffles
- uses worker/finalizer RPCs directly
- does **not** persist a durable join job
- does **not** require metadata lease ownership
- best for low-latency execution when replay/recovery is not worth the cost

2. `distributed_durable`
- used when the API server has a durable join-job store and shared metadata
  lease support
- persists finalizer-local job state and completed results
- persists partial partition progress too, so a restarted finalizer can resume
  incomplete work from the next partition instead of always replaying from
  partition zero
- can import a prior owner's persisted partial state over internal worker RPC
  during lease handoff, so a new finalizer can continue instead of always
  recomputing from scratch
- can also import a prior owner's completed durable result over the same RPC,
  so a new coordinator can reuse that cached result instead of always
  redispatching a finalizer
- uses metadata-backed shuffle lease ownership to converge API servers on the
  same finalizer
- clears shared lease ownership again when a durable job fails or expires
- meant for heavier shuffle work where recovery/ownership matters more than the
  extra coordination cost

Fast/local paths also still exist:
- local join
- index lookup join
- small broadcast join
- shard-targeted coordinator execution

Those do not need metadata-backed coordination.

## What The Metadata Lease Is For

The shared shuffle lease is **not** a transactional correctness primitive.

It does **not**:
- serialize joins through the metadata leader
- make join results more read-consistent than the underlying query path
- replace normal read-index/read-consistency behavior

It **does**:
- provide shared ownership for durable distributed shuffle jobs
- reduce split-brain finalizer selection across API servers
- let different API servers converge on the same owner for the same job id
- get cleared on durable job failure/expiry so stale owners do not linger
- allow the designated finalizer to resume incomplete durable jobs from
  persisted partial progress after restart
- allow a new finalizer to pull the previous owner's persisted durable job
  snapshot over internal RPC during handoff
- allow a coordinator to pull a previous owner's completed durable result over
  internal RPC and reuse it directly

The cached result payload still belongs to the finalizer-local durable job
store, not metadata, and the partial-progress snapshot is also finalizer-local.

## How This Differs From Go

Go’s top-level join flow in
[api_join.go](/Users/ajroetker/go/src/github.com/antflydb/antfly/src/metadata/api_join.go)
is simpler:

1. run the primary query
2. convert hits to rows
3. build a join plan
4. execute via the join planner/executor package

That is mostly a service-layer design. It does planning and supports multiple
strategies, but it does not currently use a metadata-backed lease/handoff path
like Zig’s durable distributed shuffle path.

So:
- Go is simpler at the service boundary
- Zig is now more explicit about distributed orchestration and failure handling

## Does This Scale?

Parts of the current system scale reasonably:
- index lookup
- shard-targeted broadcast
- worker-dispatched shuffle
- stable finalizer selection
- bounded retry/fallback
- completed-job replay
- persisted partial-progress replay for incomplete durable jobs on finalizer
  restart
- prior-owner handoff of persisted partial progress to a new finalizer
- prior-owner import of completed cached durable results
- finalizer-owned unmatched-right completion for distributed `right` joins

Parts are still not the final scalable design:
- no shared cluster-wide persisted result store
- incomplete-job recovery is now stronger on finalizer restart, but still does
  not provide a true shared cluster-wide result store; it still relies on
  finalizer-local durable state plus metadata lease owner selection and
  point-to-point handoff/result-import RPC
- distributed `right` joins still materialize unmatched-right completion on the
  designated finalizer worker rather than a richer multi-stage worker service
- very large joins can still bottleneck on coordinator/finalizer memory and RPC
  fanout

So the current implementation is a real distributed first cut, not the final
large-scale shuffle service.

## Long-Term Direction

The intended architecture is:

1. Keep the fast path simple.
- local
- index lookup
- small broadcast
- coordinator-only distributed read/merge

2. Reserve metadata-backed ownership for durable distributed shuffle.
- only the heavier shuffle lane should pay that coordination cost

3. Grow the durable shuffle path into a clearer service boundary.
- worker ownership
- partition ownership
- retry/steal semantics
- durable cleanup/retention
- richer recovery for incomplete jobs

## How Zig Can Improve On Go

Potential advantages for Zig:
- clearer split between fast joins and durable distributed joins
- stronger operator visibility for distributed execution state
- more explicit shard-local pushdown and routing
- more honest execution-mode reporting in query profiles
- explicit profile visibility for imported owner state:
  - `imported_partial_state`
  - `imported_cached_result`
  - `imported_owner_group_id`

The key is to avoid forcing all joins through the heavy path.

## Current Remaining Gaps

- `foreign_sources` still need broader parity work
- serverless still has only local join execution; it does not have distributed
  shuffle execution or a durable join-job lifecycle
- no shared cluster-wide persisted result store exists for shuffle jobs
- incomplete-job recovery across arbitrary API-server loss is still limited to
  finalizer-local durable state plus metadata-backed lease ownership and
  point-to-point state/result import
- very large distributed `right` joins still rely on finalizer-local unmatched
  row materialization rather than a deeper multi-stage worker service
- joined-response mutation is more centralized now, but it still operates on
  dynamic hit/doc payloads rather than a fully typed internal response model
- arbitrary document/hit inspection remains intentionally dynamic in a few
  places because the stored payloads are open-ended

## File Boundaries

- [json_helpers.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/antfly/src/api/json_helpers.zig)
  is the generic JSON utility layer used by join code:
  - path extraction
  - clone/deinit
  - JSON equality
  - owned parse/object/path helpers
  - stringify/scalar conversion
- [join_model.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/antfly/src/api/join_model.zig)
  is the join-specific shared model/helper layer:
  - owned join result shells
  - response/profile shaping
  - joined-hit/source mutation
  - unmatched-right append/build logic
  - simple planner fallback logic
  - shared planner cost/threshold helpers
- [distributed_join.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/antfly/src/api/distributed_join.zig)
  owns the genuinely stateful layer:
  - shuffle selection
  - planned execution routing across index-lookup/shuffle/broadcast
  - distributed group/snapshot loading for lookup/broadcast/shuffle lanes
  - a local `StatefulDistributedShuffleEngine` that owns the finalized and
    partitioned shuffle entrypoints, including local finalizer-worker
    execution
  - worker partition dispatch/progress flow under that engine
  - remote finalizer attempt flow and coordinator fallback under that engine
  - a local `StatefulShuffleJobLifecycle` for cached-result/resume/import/start
    and success/failure bookkeeping in the finalizer worker path
  - `StatefulShufflePartitionState` for partition progress
  - `StatefulShuffleFinalizerState` for finalizer selection/handoff

This is intentionally not collapsed into `join.zig` yet because there is not
yet a single shared join engine/planner module to center that name around.

## Next Work

The next meaningful join work is no longer more helper extraction. The highest
value is deeper runtime/engine work in
[distributed_join.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/antfly/src/api/distributed_join.zig),
ordered by operational payoff:

1. Strengthen the local stateful shuffle engine boundary.
- Keep making the stateful flow read like one engine:
  - plan
  - acquire or resume
  - dispatch partitions
  - finalize
  - persist or expire
- The point is not style. The benefit is making lease-handoff, restart, and
  retry behavior easier to reason about and test.

2. Reduce finalizer-local bottlenecks for large distributed `right` joins.
- The biggest remaining scalability issue is still finalizer-local
  unmatched-right materialization.
- The next serious improvement is a more staged or streamed unmatched-right
  completion flow so very large `right` joins do not rely so heavily on one
  finalizer worker's memory.

3. Improve durable shuffle recovery semantics.
- Today the system has:
  - finalizer-local durable state
  - metadata-backed owner lease
  - point-to-point partial-state import
  - point-to-point cached-result import
- The next payoff is making this feel more like a real durable execution
  service:
  - clearer lifecycle/state abstraction
  - cleaner retention/expiry semantics
  - better recovery behavior after arbitrary API-server loss
- A shared cluster-wide persisted result store would belong here if and when
  the current owner-local design stops being sufficient.

4. Keep tests focused on engine behavior, not just route coverage.
- Add direct tests around:
  - durable lifecycle transitions
  - lease/handoff behavior
  - retry/fallback paths
  - imported partial-state and cached-result paths
- The main remaining risk is durable orchestration behavior, not JSON response
  formatting.

5. Keep the lane boundaries honest.
- Prefer moving planner/result-shaping logic into
  [join_model.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/antfly/src/api/join_model.zig)
  only when the behavior is genuinely shared across lanes.
- Keep
  [distributed_join.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/antfly/src/api/distributed_join.zig)
  focused on truly stateful concerns:
  - distributed shuffle orchestration
  - durable lifecycle
  - lease/handoff/retry behavior
- Do not force arbitrary stored documents into fake static types. Keep that
  dynamic work behind
  [json_helpers.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/antfly/src/api/json_helpers.zig).

### Explicit Non-Goals

The following are low-value next steps and should not drive the roadmap:

- more one-off JSON helper extraction without changing durable behavior
- trying to type arbitrary hit `_source` payloads as if they were stable
  contracts
- collapsing the join files into one module before there is a real shared join
  engine spanning both lanes

## Execution Rule

Only do low-risk extractions unless they clearly improve one of the current
boundaries above.

Good work:
- extracting truly generic dynamic JSON helpers
- introducing internal join result structs
- removing duplicated mutation/build logic

Bad churn:
- renaming helpers without changing the architecture
- forcing static typing onto intentionally open-ended document payloads
- widening stateful/serverless differences while “cleaning up” one side
