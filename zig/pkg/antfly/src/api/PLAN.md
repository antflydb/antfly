# API Plan

This file tracks the remaining public/API-layer work for Antfly's stateful
surface.

Use [TODO.md](../../../../TODO.md)
for live public contract gaps and active E2E failures. Use
[DB.md](../../../../DB.md)
for DB-layer storage/query execution boundaries.

## Current Shape

The API layer has:

- public `/api/v1/...` request routing and OpenAPI-shaped contracts
- internal `/internal/v1/...` forwarding for hosted group reads, writes, and
  transaction participant work
- metadata-backed table and index lifecycle
- multi-node routed E2E coverage for CRUD, split, merge, embeddings, graph, and
  transactions
- cross-range graph v1 in
  [distributed_graph.zig](distributed_graph.zig)
- distributed transaction coordinator and participant protocol in
  [distributed_txn.zig](distributed_txn.zig)
- public transaction parity and extended session endpoints in
  [transactions.zig](transactions.zig)

Public API behavior should continue to converge on the OpenAPI contract, while
runtime behavior stays handwritten in Zig modules.

Boundary rules:

- public product APIs stay under `/api/v1/...`
- internal shard/node routing stays under `/internal/v1/...`
- generated OpenAPI types shape requests and responses
- metadata owns topology snapshots, leader routing, and placement visibility
- shard-local execution belongs in DB/graph/search engines

## Implemented: Parallel Fanout

The original `PARALLEL.md` plan is mostly complete and is folded here as status.

Implemented:

- `std.Io` ownership/capability on provisioned and hosted table read sources
- parallel distributed text-stats fanout
- parallel shard search fanout
- parallel preflight fanout
- parallel independent cross-range graph expand/hydrate group batches
- runtime/query metrics for parallel fanout timing and fallback
- read-side `FanoutPlan` for search, text-stats, and preflight
- bounded fanout width instead of launching all groups at once
- long-lived `std.Io.Threaded` query runtime on the provisioned `DataServer`
  path with explicit `async_limit`
- local DB `ApplyRwLock` with safe pure-read paths moved to shared locking
- mixed local read/write measurement via
  [rw_lock_bench.zig](../../../../bench/storage/rw_lock_bench.zig)
- local DB `ExecutionContext` threaded through `search`, `searchComposed`,
  planning, and preflight as the hook for future execution controls

Remaining question:

- if writer/read blocking still dominates after RW-locking, introduce a
  snapshot/publish model for hot local read paths so reads can run outside the
  publish critical section and local composed search can later use bounded
  `std.Io` fanout safely

## Remaining: True Distributed Graph Protocol

The current cross-range graph v1 is intentionally narrow.

Supported today:

- `neighbors`
- `traverse`
- `shortest_path`
- `include_paths`
- explicit `start_nodes.keys`
- `result_ref` starts from fused/base hits and prior graph results

Current constraints:

- `direction = out`
- `weight_mode = min_hops`
- `deduplicate = true`

Still remaining:

- weighted distributed shortest path
- distributed `k_shortest_paths`
- topology-aware retries/restarts across churn
- explicit worker API instead of generic internal table query reuse
- coordinator-owned path-state storage instead of copied path frontier state

### Protocol Shape

The true protocol should make cross-range graph queries behave like one logical
graph query over a partitioned graph, not a merge of unrelated local traversals.

Ownership model:

- outgoing edges are owned by the shard that owns the source key
- a worker is authoritative for expanding edges out of a frontier key it owns
- the coordinator resolves target-key ownership using one pinned metadata
  snapshot
- cross-range traversal is frontier handoff, not graph duplication

Query snapshot:

- pin committed range map and group routing at query start
- use one topology epoch/version for the query
- fail as retriable on `UnknownGroup`, topology mismatch, or route/leadership
  conflict
- restart the whole query with a fresh snapshot rather than remapping partial
  frontier state in the first true-protocol version

Recommended worker endpoint:

- `POST /internal/v1/groups/{group_id}/graph-expand`
- optional follow-up:
  - `POST /internal/v1/groups/{group_id}/graph-hydrate`

Workers return local edge expansions. The coordinator owns the search algorithm.

### Coordinator Algorithms

Distributed `neighbors`:

1. Partition start frontier by owning group.
2. Ask each owning group for one-hop expansions.
3. Deduplicate and merge globally.
4. Hydrate final node documents if needed.

Distributed `traverse`:

1. Keep a global frontier queue.
2. Expand one hop at a time by group.
3. Maintain global visited/dedup state.
4. Track parent/path state in the coordinator.
5. Stop on empty frontier, `max_depth`, `max_results`, or timeout.

Distributed `shortest_path`:

- `weight_mode = min_hops` uses global BFS
- weighted modes use global Dijkstra-style search
- workers return outgoing edge expansions only
- the coordinator owns global frontier ordering and node settlement

Distributed `k_shortest_paths`:

- use Yen's algorithm at the coordinator
- reuse the weighted shortest-path primitive
- add excluded node/edge sets to `graph-expand`
- reconstruct accepted paths from coordinator path state

Path state should move toward:

- `PathStateId`
- current key
- cumulative score
- cumulative hops
- parent state id
- incoming edge

Only reconstruct final graph result nodes, final graph paths, and optional
debug/profile output.

### Execution Order

1. Replace copied-path frontier state with coordinator-owned parent/path state
   ids.
2. Add explicit internal `graph-expand` RPC.
3. Re-implement distributed `neighbors` and `traverse` on top of that worker
   API.
4. Implement true distributed weighted shortest path.
5. Implement distributed `k_shortest_paths` via Yen on top of the weighted
   shortest-path primitive.
6. Add result hydration.
7. Add retries/restarts across topology churn once the protocol is stable.

### Testing Matrix

Add API-owned and local coordinator tests for:

- cross-range `include_paths`
- min-hop distributed shortest path
- weighted distributed shortest path
- `k_shortest_paths` with split ownership
- exclusion handling for Yen spur paths
- topology-epoch failure and retriable restart
- leader change during query returning retriable failure

## Remaining: Public Contract Convergence

Near-term public API work should stay coverage-driven:

- fix status/readiness and E2E failures tracked in `TODO.md`
- add parity tests before widening public query/search shapes
- keep generated OpenAPI types and handwritten behavior aligned
- keep stateful and serverless public table behavior converging where the
  execution model supports the same capability
