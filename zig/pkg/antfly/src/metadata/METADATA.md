# Metadata Control Plane

This file is the current-state note for Antfly's metadata/control-plane layer.

Use [ROADMAP.md](../../../../ROADMAP.md)
for project-wide sequencing and
[RAFT.md](../raft/RAFT.md)
for hosted Raft/runtime integration. The metadata layer owns product policy
above that substrate.

## Ownership

The metadata layer owns:

- desired table and range topology
- canonical mutable table semantics required for Go-contract parity
- placement and replica intent policy
- split and merge administration
- reconciliation between desired metadata state and observed replica state
- store membership, liveness, health, and capacity signals
- metadata service/server and admin/status surfaces

The Raft layer stays focused on:

- hosted group lifecycle
- transition execution/runtime status
- leader/read readiness
- durable replica state and recovery

## Current Shape

Completed or substantially in place:

- hosted metadata service/runtime boundary
- metadata transition records and transition driver
- real split/merge coordinator-backed execution
- service-owned transition queue and restart recovery
- leader-scoped enrichment runtime with readable-lease gating
- `MetadataService` and HTTP/admin server surfaces
- table/range topology state in
  [state.zig](state.zig)
- desired table/range workflow in
  [table_manager.zig](table_manager.zig)
- pure reconcile planning in
  [reconciler.zig](reconciler.zig)
- placement planning in
  [placement_planner.zig](placement_planner.zig)
- store status observation and liveness/capacity ingestion
- metadata admin/status snapshots for topology, stores, placement, repair, and
  rebalance inspection
- metadata HTTP/admin endpoints in
  [http_server.zig](http_server.zig)
- stepped multi-node metadata VOPR tests in
  [vopr_harness.zig](vopr_harness.zig)

The projected raft apply store is still part of the implementation, but the
control plane is no longer just transition-focused scaffolding. Desired topology,
placement, store observation, and reconcile status are first-class surfaces.

## Listener Topology

Raft peer traffic keeps its own dedicated listener, separate from the metadata
HTTP/admin surface. The public API and the internal (`/internal/v1`) API,
however, share one HTTP listener rather than each getting a dedicated port.
Internal routes are protected by request-level authentication rather than by
being reachable only from a private port, so there is exactly one port to
expose and firewall for API traffic, plus the raft-only listener for peer
communication. Gating internal routes by auth instead of by port keeps the
deployment surface small without weakening the internal endpoints, since a
request has to pass the same authentication check regardless of which network
path reached the listener.

## Module Map

- [service.zig](service.zig)
  - metadata raft-group service boundary
  - admin proposal helpers
  - projected metadata state access
- [server.zig](server.zig)
  - hosted metadata service entrypoint
- [http_server.zig](http_server.zig)
  - metadata HTTP/admin API
- [state.zig](state.zig)
  - metadata table, range, store, status, and projection model
- [table_manager.zig](table_manager.zig)
  - desired table/range topology
  - split/merge admin intent creation
  - table/range validation
- [reconciler.zig](reconciler.zig)
  - pure planning from desired state, committed transition records, observed
    runtime state, and store/candidate state
- [placement_planner.zig](placement_planner.zig)
  - placement and rebalance candidate selection
- [store_observer.zig](store_observer.zig)
  - heartbeat/load/lease-pressure signal merge outside the service boundary
- [control_loop.zig](control_loop.zig)
  - control-loop execution around reconcile/service actions
- [vopr_harness.zig](vopr_harness.zig)
  - multi-node metadata VOPR and HTTP integration convergence coverage

## Control-Plane Shape

The control plane follows the Go-style split:

- planning is pure
- execution is explicit
- desired state lives in metadata
- observed state comes from hosted runtime and status updates

Flow:

1. `TableManager` owns desired topology and admin intents.
2. `Reconciler` computes transition upserts/removals, placement upserts/removals,
   repair/rebalance summaries, and runtime-driving steps.
3. `MetadataService` proposes metadata changes into the metadata raft group.
4. The hosted transition queue executes runtime work from committed transition
   records.
5. Status/admin surfaces expose projected topology and reconcile state for
   operators and tests.

## Covered Workflows

Current tests and simulations cover:

- table creation and persisted range descriptors
- split and merge intent flows through the real control loop
- metadata leader restart during placement reconcile
- placement convergence after candidate churn
- committed node/store membership as placement-candidate sources
- store status heartbeat/liveness/capacity ingestion
- repair vs rebalance classification
- adding a data node and assigning placements to it
- draining/stopping/finalizing data nodes after replacement
- multi-range placement spread
- placement role constraints under churn
- automatic split/merge planning from runtime status

## Automatic Split And Merge Policy

Automatic range transitions are governed by a size band rather than a single
threshold: shards are split once they exceed a configured maximum size, and
merged back together only once they fall under a configured minimum size,
which defaults to one quarter of the maximum when not set explicitly. That gap
between the two thresholds keeps a shard sitting near either boundary from
being split and then immediately re-merged (or the reverse) as its size
fluctuates slightly.

Planning also throttles how much transition work can be in flight at once:
each table has its own cap on the number of concurrent automatic split/merge
transitions it can have running, and the whole cluster shares a further cap
across all tables, so one table's growth cannot monopolize the reconciler or
trigger a burst of simultaneous transitions. Each shard that finishes a split
or merge (or has one rolled back) also enters a cooldown window before it can
be selected for another automatic transition, which keeps a shard from being
repeatedly re-split or re-merged while its size stabilizes.

An automatic merge only fires between shards that are healthy, adjacent, and
not already mid-transition. Because a merge folds one shard's data into its
neighbor rather than just widening a byte range, the donor's data has to be
moved into the receiver before the donor range is retired from the table's
topology, not the other way around.

## Active Follow-Up

Remaining work is not basic bring-up. It is policy depth and operator/product
hardening:

- placement/rebalance policy maturity
- replica intent repair and disappearance handling
- table/index lifecycle workflows that should live in metadata rather than API
  ad hoc state
- broader operator-facing admin/status parity
- OpenAPI-shaped metadata admin/status surface decisions after in-tree routes
  stabilize
- stronger remote status propagation and diagnostics under split, merge,
  recovery, and placement churn
