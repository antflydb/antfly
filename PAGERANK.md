# PageRank Graph Metric Design

## Goal

Add PageRank-style graph centrality as a materialized graph index metric. The
metric should be eventually fresh, observable, safe to query during rebuilds,
and cheap on the write path.

This should be implemented as a generic graph metric framework with PageRank as
the first supported metric. Eigenvector centrality, HITS authorities, and HITS
hubs can reuse the same storage, job, and query surfaces later.

## User Model

Users opt in through graph index configuration. A graph metric is owned by the
graph index because it is derived index state: the graph index tracks dirtiness,
runs maintenance, stores scores, and exposes query access. Table schema may
validate or surface the config, but it should not own the materialization.

Writes to graph edges mark the metric stale. Background work computes a new
score generation. Queries read the last completely published generation by
default.

The core promise is:

- Writes do not wait for PageRank recomputation.
- Queries do not read partial scores.
- Failed rebuilds leave the prior published generation usable.
- Freshness and progress are visible through status APIs.

## Index Configuration

Graph metrics should live under graph index configuration:

```json
{
  "name": "knowledge_graph",
  "type": "graph",
  "metrics": {
    "pagerank": {
      "enabled": true,
      "damping": 0.85,
      "max_iterations": 50,
      "tolerance": 0.000001,
      "refresh": "background",
      "edge_filter": {
        "mode": "all"
      }
    }
  }
}
```

The internal representation should keep the surface generic:

```zig
pub const GraphMetricKind = enum {
    pagerank,
    degree,
    eigenvector,
    hits_authority,
    hits_hub,
};

pub const GraphMetricRefreshMode = enum {
    background,
    manual,
};

pub const GraphMetricConfig = struct {
    name: []const u8,
    kind: GraphMetricKind,
    damping: f64 = 0.85,
    tolerance: f64 = 0.000001,
    max_iterations: u32 = 50,
    refresh: GraphMetricRefreshMode = .background,
    edge_filter: GraphMetricEdgeFilter = .{ .mode = .all },
};
```

PageRank should ship first. Additional metric kinds should stay opt-in and must
reuse the same graph-index-owned materialization, status, and query framework.

The resolved config should always record the edge scope. The ergonomic default
is all edges in the graph index, but users need a first-class way to restrict
the metric to specific edge families because PageRank over mixed semantic edges
can be misleading:

For v1, support `mode: "all"` and typed edge include lists:

```json
{
  "edge_filter": {
    "types": ["mentions", "cites"]
  }
}
```

Typed include lists should be implemented only against the graph index's
existing edge type or edge family metadata. V1 should not add a separate
predicate engine for graph metric filtering.

Long-term, edge scope can grow into labels and field predicates:

```json
{
  "edge_filter": {
    "types": ["mentions", "cites"],
    "labels": ["references"],
    "where": [
      { "field": "confidence", "op": ">=", "value": 0.8 }
    ]
  }
}
```

Arbitrary predicates should wait until the core materialization and generation
contract is stable.

## SQL DDL

If SQL DDL is exposed for graph metrics, it should lower into the same graph
index metric config:

```sql
CREATE GRAPH METRIC pagerank
ON knowledge_graph
WITH (
  damping = 0.85,
  tolerance = 0.000001,
  max_iterations = 50,
  refresh = 'background'
);
```

This is optional for the first implementation. The JSON/schema path is enough to
prove the storage and query contract.

## Query API

Graph traversal and graph search APIs should be able to return and order by a
published graph metric:

```json
{
  "graph": {
    "index": "knowledge_graph",
    "start": { "label": "Person", "id": "alice" },
    "traverse": { "max_depth": 2 },
    "order_by": [
      { "metric": "pagerank", "direction": "desc" }
    ],
    "return": ["id", "label", "pagerank"]
  }
}
```

Direct top-k metric reads should also be supported:

```json
{
  "graph_metric": {
    "index": "knowledge_graph",
    "metric": "pagerank",
    "top_k": 100
  }
}
```

Direct metric endpoints should always include metric status in their responses.
Graph traversal and graph search responses should include metric status only
when requested:

```json
{
  "graph": {
    "index": "knowledge_graph",
    "traverse": { "max_depth": 2 },
    "return": ["id", "pagerank"],
    "include_metric_status": true
  }
}
```

Metric status is a map keyed by metric name. That keeps responses easy to join
against requested metric fields when a query includes multiple metrics.

Response shape:

```json
{
  "metric_status": {
    "pagerank": {
      "state": "stale",
      "published_generation": 42,
      "edge_generation": 43,
      "converged": true,
      "iterations_completed": 31,
      "computed_at_ms": 1780000000000
    },
    "authority": {
      "state": "stale",
      "published_generation": 8,
      "edge_generation": 11,
      "converged": false,
      "iterations_completed": 50,
      "computed_at_ms": 1779999900000
    }
  }
}
```

By default, queries read the last published complete generation. If a metric has
never been published, behavior depends on how the metric is used:

- Projecting a metric field returns `null`.
- Ordering by a metric fails with `MetricNotReady`.
- Filtering by a metric fails with `MetricNotReady`.
- Direct top-k metric reads fail with `MetricNotReady`.

Projection can tolerate missing derived data; ranking and filtering cannot
because they imply meaningful score semantics.

Useful query freshness modes:

```json
{
  "metric_freshness": "published"
}
```

Initial modes:

- `published`: read the last complete generation, even if stale.
- `fresh`: require the published generation to match the current edge
  generation; fail with `MetricNotReady` or `MetricStale` otherwise.

Use two distinct freshness errors:

- `MetricNotReady`: no published generation exists.
- `MetricStale`: a published generation exists, but the caller requested
  `fresh` and it does not match the current edge generation.

Avoid blocking query execution for a rebuild in the first implementation.

## Status API

Expose metric progress and freshness:

```json
{
  "index": "knowledge_graph",
  "metric": "pagerank",
  "state": "stale",
  "published_generation": 42,
  "building_generation": 43,
  "edge_generation": 43,
  "progress": 0.61,
  "iterations_completed": 18,
  "last_error": null
}
```

Suggested states:

- `disabled`
- `not_ready`
- `fresh`
- `stale`
- `building`
- `failed`

The status API should distinguish "queryable but stale" from "not queryable".

## Storage Layout

Use generationed materialization so publishing is atomic:

```text
graph_metric_dirty:<index>:<metric> -> edge_generation
graph_metric_published:<index>:<metric> -> score_generation
graph_metric_build:<index>:<metric>:<job_id> -> build metadata
graph_metric_score:<index>:<metric>:<generation>:<node_id> -> f64
graph_metric_meta:<index>:<metric>:<generation> -> stats/convergence metadata
```

Queries only resolve scores through `graph_metric_published`. Build jobs write
to a private `building_generation` and flip the published pointer only after the
generation converges or reaches the configured iteration cap.

Score generations are private storage epochs, not edge generations. A manual
refresh or config-only rebuild may target the same edge snapshot more than once,
so every attempt gets a new durable score namespace. Status and API responses
continue to expose the edge snapshot as `published_generation`; the private
score epoch is only used to locate immutable materialized rows.

Publication enqueues the superseded score epoch for bounded background cleanup.
Cleanup uses durable phase/cursor state and small transactions, which avoids
unbounded write batches and remains safe for readers holding an older storage
snapshot. Failed builds enqueue only their unpublished output and never remove
the last good published generation. Operator deletion similarly tombstones the
metric immediately, then removes scores, ranks, metadata, and job state in
bounded maintenance pages.

The durable cleanup state is internal:

```text
graph_metric_retired:<index>:<metric> -> score_generation
graph_metric_cleanup_phase:<index>:<metric> -> scores | ranks | metadata
graph_metric_cleanup_cursor:<index>:<metric> -> last_key
```

The queue is deliberately bounded. If maintenance cannot retire generations as
fast as new materializations are requested, control requests apply backpressure
instead of accumulating unbounded disk usage.

Future versions can add retention controls for debugging or rollback:

```json
{
  "retention": {
    "mode": "count",
    "retained_generations": 2
  }
}
```

Do not expose retention as a first-version user-facing option. Keep v1
latest-only. If this becomes useful later, prefer a retention object with modes:

- `latest`: keep only the latest published generation.
- `count`: keep the last N published generations.
- `duration`: keep generations for a time window.

## Write Path

Graph edge writes should only mark metric dirtiness:

```text
edge write commits
  -> edge_generation advances
  -> graph_metric_dirty:<index>:pagerank = edge_generation
  -> background job is scheduled best-effort
```

The write path must not compute PageRank, scan the graph, or wait for a metric
job. If scheduling fails, the dirty marker remains durable and a later
maintenance round can recover.

## Local Job Execution

For a single local graph index, follow the algebraic HLL maintenance pattern:

- Dirty marker is persisted.
- A durable maintenance lane runs the rebuild off the write path.
- Redundant jobs collapse by re-checking dirty state under the index write lock.
- A failed maintenance attempt leaves the dirty marker intact.

This is the simplest first implementation and keeps the first PageRank version
small.

## Distributed Job Execution

For distributed graph indexes, use the relational job pattern:

- durable job id
- worker lease
- phase
- cursor/progress key
- requeue support
- progress endpoint
- idempotent page writes

PageRank is iterative, so the distributed job is a multi-phase job rather than a
single range repair pass.

Suggested phases:

```text
prepare_generation
scan_edges_and_out_degree
initialize_ranks
iterate_contributions
reduce_ranks
check_convergence
publish_generation
cleanup_old_generations
```

The important distributed invariant is that partial contributions and partial
scores are never visible through the query path. Only the final publish step
changes the generation pointer that queries read.

## PageRank Algorithm

Initial algorithm:

```text
rank_next(node) =
  (1 - damping) / node_count
  + damping * sum(rank_prev(src) / out_degree(src))
  + damping * sink_mass / node_count
```

Each iteration computes:

- contribution records from source nodes to destination nodes
- sink mass from zero-out-degree nodes
- reduced next-rank values per destination node
- convergence delta, for example L1 norm

Stop when either:

- `delta <= tolerance`
- `iterations_completed == max_iterations`

If the iteration cap is reached without convergence, publish the bounded result
by default and mark the metadata as approximate:

```json
{
  "converged": false,
  "iterations_completed": 50,
  "delta": 0.000034
}
```

PageRank is commonly used as a bounded iterative approximation, so a valid
non-converged fixed-iteration result is still useful. The job should fail and
preserve the prior published generation only for invalid output or corrupt
state, such as NaN scores, infinities, missing graph metadata, or incomplete
iteration output.

## Query Integration

At query time:

1. Resolve the graph metric config.
2. Resolve the published metric generation.
3. Read scores for returned candidate nodes.
4. Apply metric ordering if requested.
5. Include freshness metadata when requested.

Internal graph query types likely need fields similar to:

```zig
pub const GraphMetricRead = struct {
    name: []const u8,
    freshness: GraphMetricFreshnessMode = .published,
};

pub const GraphOrder = union(enum) {
    field: GraphFieldOrder,
    metric: GraphMetricOrder,
};

pub const GraphMetricOrder = struct {
    name: []const u8,
    direction: OrderDirection = .desc,
};
```

Metric values should be returned as nullable floats. Missing scores can occur
for nodes introduced after the last publish. Ordering should treat missing
scores explicitly, with the default being `nulls_last`.

Before the first publish, direct metric ranking paths should not try to rank all
nodes as missing. They should fail with `MetricNotReady`.

## Progress and Recovery

Jobs should persist enough state to recover after restart:

- job id
- metric name
- target edge generation
- building score generation
- current phase
- iteration number
- page cursor
- accumulated convergence delta
- worker lease owner and expiration
- last error

Recovery should be idempotent:

- Re-running a contribution page overwrites the same generation/iteration output.
- Re-running a reduce page overwrites the same next-rank output.
- Re-running publish checks that the target generation is complete before
  flipping the pointer.

If graph writes happen during a build, the active build can still publish its
target generation. The dirty marker remains newer than the published generation,
which schedules a later rebuild.

## Why PageRank First

PageRank is the best first centrality metric because it behaves well on directed
graphs, disconnected graphs, and sink-heavy graphs. The damping factor gives
stable results even when the graph does not have the structural properties raw
eigenvector centrality expects.

Eigenvector centrality can reuse the same materialization framework later, but
it needs more careful handling for disconnected components, reducibility, and
non-convergence.

## Implementation Plan

1. Add graph metric config and schema parsing for `pagerank`.
2. Add dirty and published generation metadata keys.
3. Add local PageRank maintenance using the durable background lane.
4. Store generationed scores and publish through a metadata pointer flip.
5. Add query support for returning metric scores.
6. Add direct top-k metric reads.
7. Add status reporting for freshness and build progress.
8. Add tests for write dirtiness, successful publish, failed rebuild preserving
   prior generation, stale reads, and ordering by score.
9. Add distributed phased jobs after the local implementation is stable.

## Long-Term Design Roadmap

The first PageRank implementation should prove the core generation contract:
graph-index-owned metric config, durable dirty state, complete-generation
publish, direct metric reads, and observable freshness. The roadmap below is the
production shape beyond that first slice.

This roadmap treats PageRank as the first product surface for a broader graph
metric subsystem. The long-term design goal is that future centrality metrics,
distributed execution, richer edge scopes, and retrieval composition extend the
same index-owned lifecycle instead of creating parallel APIs.

Long-term graph metrics should evolve as an index subsystem, not as one-off
query operators. Every new metric should reuse the same lifecycle:

```text
graph writes
  -> durable metric dirtiness
  -> local or distributed metric job
  -> generationed score writes
  -> atomic publish pointer flip
  -> snapshot-safe cleanup
  -> query/status/read APIs
```

The sequencing principle is to expand one axis at a time:

- First make one metric reliable on one node.
- Then make that metric compose with graph queries.
- Then distribute the same lifecycle.
- Then add more metrics through the same framework.
- Then add richer scopes and operational controls.

Avoid adding new metric-specific query surfaces after PageRank. Eigenvector,
HITS, degree, personalized PageRank, and future centrality metrics should all
look like named graph metrics to users and should differ only in config,
metadata, and documented convergence behavior.

Roadmap summary:

| Phase | User-facing outcome | Main implementation work |
| --- | --- | --- |
| 0. Framework baseline | PageRank is queryable as a named graph metric. | Local dirty tracking, generationed score storage, atomic publish, status, and cleanup. |
| 1. Query integration | Traversals/searches can project, order, and filter by metrics. | Metric lookup in graph execution, freshness checks, deterministic ordering, and OpenAPI/client updates. |
| 2. Distributed jobs | Large graphs rebuild metrics durably across workers. | Job records, leases, resumable phases, idempotent pages, progress, retries, and verified publish. |
| 3. Metric families | Degree, eigenvector, HITS, and later personalized PageRank reuse the same surface. | Shared algorithm runner contracts, metric-specific convergence metadata, paired HITS publish, and high-cardinality safeguards. |
| 4. Edge scope | Users can maintain scoped metrics for specific edge families. | Resolved edge-filter metadata, typed validation, materialization invalidation, and status/explain output. |
| 5. Operations | Admins can refresh, rebuild, pause, resume, delete, and observe metrics. | Idempotent controls, retention policy, event logs, queue visibility, and safe cleanup. |
| 6. Retrieval composition | Graph metrics can contribute to explicit reranking and planner features. | Score features, freshness-aware planning, cross-shard top-k merge, and explain/profile integration. |
| 7. Compatibility | Existing PageRank APIs keep working as the framework grows. | Metadata versioning, compatibility aliases, migrations, and deprecation windows. |

### Phase 0: Framework Baseline

The baseline is the minimum viable graph metric framework:

- graph index config owns `metrics`
- metric config resolves to a stable name, kind, refresh mode, and edge filter
- edge writes persist dirty state
- maintenance computes a complete score generation
- publishing is a metadata pointer flip
- queries only read the published generation
- failed builds preserve the prior published generation
- direct metric reads expose status

This phase should stay intentionally local. Its job is to prove the storage and
freshness contract, not to optimize for very large graphs.

Milestones:

- Persist dirty and published generation metadata.
- Implement local PageRank over all edges and typed edge include lists.
- Publish fixed-iteration non-converged PageRank with `converged: false`.
- Reject invalid score output such as NaN or infinity.
- Clean up old generations immediately when reads are snapshot-safe.
- Add public tests for first publish, stale publish, top-k reads, and status.

Exit criteria:

- A graph can accept writes while metric maintenance runs.
- A query never observes partially written metric scores.
- Restart preserves dirty state and the last published generation.
- The user can distinguish `not_ready`, `fresh`, `stale`, `building`, and
  `failed`.

Current implementation progress:

- Local PageRank dirty-state handling now has restart coverage: after a fresh
  publish, a later edge write persists stale/queued status across graph/store
  reopen, continues serving direct metric reads from the prior published
  generation, and rebuilds the later edge generation when maintenance runs.
- Local graph metric failure handling now has restart coverage for the critical
  publish invariant: after a successful PageRank publish, a later invalid
  rebuild records durable failed status, preserves the prior published
  generation across graph/store reopen, and continues serving direct top-k reads
  from the last complete generation until a later successful rebuild clears the
  failure.

### Phase 1: Complete Query Integration

Direct metric reads are useful for top-k centrality lists, but graph metrics
become more valuable when they compose with graph traversal, graph search, and
ordinary retrieval.

Add metric projection to graph query nodes:

```json
{
  "graph_searches": {
    "related": {
      "type": "traverse",
      "index_name": "knowledge_graph",
      "start_nodes": { "keys": ["doc:alice"] },
      "params": { "edge_types": ["mentions"], "max_depth": 2 },
      "metrics": ["pagerank"],
      "include_metric_status": true
    }
  }
}
```

Response nodes should expose metric values as nullable floats:

```json
{
  "key": "doc:bob",
  "depth": 1,
  "metrics": {
    "pagerank": 0.0182
  }
}
```

Projection semantics:

- Missing metric generation returns `null`.
- Missing node score in an existing generation returns `null`.
- `metric_freshness: "published"` allows stale projected scores.
- `metric_freshness: "fresh"` fails with `MetricNotReady` or `MetricStale`.

Add metric ordering for graph traversal/search results:

```json
{
  "order_by": [
    { "metric": "pagerank", "direction": "desc", "nulls": "last" }
  ]
}
```

Ordering semantics:

- Ordering by an unpublished metric fails with `MetricNotReady`.
- Ordering with `fresh` over stale scores fails with `MetricStale`.
- Missing scores sort with explicit `nulls_first` or `nulls_last`; default
  `nulls_last`.
- Ties break by the graph query's existing deterministic node order, then key.

Filtering by metric should come after ordering support:

```json
{
  "where_metric": [
    { "metric": "pagerank", "op": ">=", "value": 0.01 }
  ]
}
```

Filtering must fail closed if the metric is not published or if `fresh` is
requested and stale.

Implementation milestones:

- Add metric projection to graph traversal/search result nodes.
- Add optional `metric_status` to graph query responses.
- Add metric ordering with deterministic tie-breaking.
- Add metric filtering after ordering semantics are stable.
- Thread `metric_freshness` through local and remote table-read paths.
- Regenerate OpenAPI/client types for the public query surface.

Do not add planner-level score blending in this phase. Keep metric usage
explicit and local to graph query results until freshness and distributed merge
semantics are tested.

Current implementation progress:

- Direct DB graph metric reads now have focused freshness coverage for stale
  materializations: `metric_freshness: "published"` returns the last complete
  generation with stale status, while `metric_freshness: "fresh"` fails closed
  with `MetricStale` after a later graph write advances the edge generation.
  Active planned-rebuild coverage now also proves public direct metric top-k
  queries with `published` use the prior score generation while reporting
  building status, and `fresh` fails closed before the planned generation
  publishes. Failed planned-rebuild coverage now proves the same public direct
  top-k path continues to serve the prior published generation while reporting
  failed status, and `fresh` remains fail-closed. Public DB search coverage now
  also proves direct graph metric top-k returns `MetricNotReady` for both
  `published` and `fresh` reads before the first generation publishes.
- DB graph traversal/search metric reads now have matching stale/fresh coverage:
  published projection can return stale metric scores and status, while fresh
  projection, ordering, and filtering over stale graph metric materializations
  fail closed with `MetricStale`. The DB graph query conversion path also
  releases temporary graph metric status ownership after cloning it into the
  public search result. Active planned-rebuild coverage now also proves
  traversal projection with `published` reads the prior score generation while
  reporting building status, and `fresh` projection/order/filter fail closed
  while the planned generation is unpublished. Failed planned-rebuild coverage
  now proves the same traversal projection path keeps serving the prior
  generation while reporting failed status, and `fresh` projection/order/filter
  remain fail-closed. These traversal projection/order/filter not-ready, stale,
  building, failed, and fresh-failure DB cases now run in the fast root suite.
- DB search rerank now has active planned-rebuild freshness coverage:
  `metric_freshness: "published"` continues to rerank with the last complete
  score generation while reporting building status, and `fresh` fails closed
  while the planned generation is still unpublished. Failed planned-rebuild
  coverage now also proves published rerank preserves the prior generation while
  reporting failed status, and fresh rerank remains fail-closed. Public DB
  search coverage now also proves search rerank returns `MetricNotReady` for
  both `published` and `fresh` reads before the first generation publishes.
- DB graph traversal/search metric reads now also cover unpublished metrics:
  published projection returns a null metric score plus `not_ready` status,
  while fresh projection and published ranking/filtering fail closed with
  `MetricNotReady` before the first metric generation is published.
- Public HTTP API graph metric e2e coverage now has a focused build step. It
  creates a table, a graph index with PageRank, degree, eigenvector, and
  compatible HITS metrics, first writes typed edges with `sync_level: "write"`,
  verifies public status reports `not_ready` with no published generation, and
  proves direct metric reads plus search rerank fail closed while traversal
  projection can expose null scores with not-ready metric status. It then writes
  the same graph with `sync_level: "full_index"`, verifies public metric status
  metadata and edge filters, and exercises fresh direct graph metric top-k plus
  graph traversal projection, ordering, and filtering through the public query
  route. The same public e2e now follows with a later `sync_level: "write"`
  graph update and verifies `published` direct graph metric top-k and traversal
  projection keep serving the prior published generation with `stale` status,
  published search rerank exposes stale status in profile output and
  prior-generation score details, while `fresh` direct, traversal, and rerank
  reads fail closed. The broader `public-api-parity-test` also remains green with
  generated OpenAPI query bodies that include explicit `null` graph metric
  fields, so graph metric raw-body parsing now matches ordinary optional-field
  semantics.

### Phase 2: Distributed Metric Jobs

Local maintenance is enough to validate storage and API semantics. Production
large-graph metrics need the same durable distributed job shape used by
relational rebuild work.

Add a graph metric job table with:

- metric name and graph index name
- target edge generation
- building score generation
- phase and iteration number
- cursor/progress keys
- worker lease owner and expiration
- retry count and last error
- publish eligibility metadata

Recommended phases:

```text
prepare_generation
scan_edges_and_out_degree
initialize_ranks
iterate_contributions
reduce_ranks
check_convergence
publish_generation
cleanup_old_generations
```

Distributed invariants:

- Partial scores are never visible to queries.
- Every intermediate key includes metric name, target generation, and iteration.
- Contribution and reduce pages are idempotent overwrites.
- Publish verifies all required pages for the target generation are complete.
- Writes during a build do not cancel the build; they leave the dirty generation
  newer than the published generation and schedule a follow-up build.
- Worker loss only abandons a lease, not the generation.

The status API should expose distributed progress without leaking internal page
keys:

```json
{
  "state": "building",
  "phase": "iterate_contributions",
  "iteration": 12,
  "progress": 0.64,
  "target_edge_generation": 84,
  "published_generation": 72
}
```

Implementation milestones:

- Add durable graph metric job records and worker leases.
- Split PageRank into resumable phases with idempotent page writes.
- Track contribution, reduce, and convergence metadata per iteration.
- Make publish verify completion before flipping the pointer.
- Expose status by phase, iteration, target generation, and progress.
- Add restart/retry tests that kill work between phases.

The distributed implementation should not introduce a different public API.
Users should see better scale and progress visibility, not a new way to request
PageRank.

Current implementation progress:

- Local graph metric builds now write a durable per-metric build job record
  alongside the active lease. The record captures the build job id, target edge
  generation, building score generation, start/update timestamps, lease
  expiration, worker id, phase, iteration, opaque build cursor, completed/total
  work units, retry count, and last error. Active local builds now report the
  same long-term phase vocabulary planned for resumable workers, including
  `prepare_generation`, `scan_edges_and_out_degree`, `initialize_ranks`,
  `iterate_contributions`, `check_convergence`, and `publish_generation`. The
  job record is updated with the same phase/iteration progress used by active
  status, persists cursor/unit progress across graph-index reopen while a build
  lease is active, records failed build retry/error details durably, and is
  marked `complete` with no active lease expiration or failure details after a
  successful publish. The status and OpenAPI surfaces now expose cursor and
  work-unit fields for active builds so distributed workers can reuse the
  observable shape later. This is still a local job-table primitive for the
  future distributed/resumable worker implementation; it does not yet split
  PageRank into distributed pages or change the public metric API.
- Active local metric builds now also plan a durable per-job manifest under the
  graph metric control namespace. The first manifest version records the job id,
  target edge generation, building score generation, metric config fingerprint,
  planned edge/node counts, phase count, and page count. The planner writes one
  deterministic pending page per phase for the selected metric kind: degree uses
  the short prepare/scan/publish/cleanup phase list, PageRank and eigenvector
  use the iterative prepare/scan/initialize/contribute/reduce/check/publish/
  cleanup list, and HITS uses the paired
  prepare/scan/initialize/authority-contribute/authority-reduce/
  hub-contribute/hub-reduce/check/publish/cleanup list. Re-planning the same
  manifest is
  idempotent and does not reset an already completed page, and restart coverage
  now verifies that the manifest and page records survive graph-index reopen.
  Build pages now persist versioned range metadata: range kind, opaque lower
  bound, opaque upper bound, and output namespace prefix. Degree and PageRank
  now both plan deterministic key-range pages for scan and node phases, and
  PageRank later-iteration pages are appended by the convergence coordinator
  when another iteration is required. The PageRank manifest now increments its
  durable page count for newly appended later-iteration pages and keeps
  re-planning idempotent; applying the same dynamic accounting model to later
  iterative metrics remains future work. Phase barriers and iteration summaries
  are now covered by the durable coordinator primitives below.
- Durable build pages now have the first lease lifecycle primitives. A worker
  can claim a pending or failed page, same-worker claims renew the active lease,
  other workers are refused while the lease is active, expired leases can be
  reclaimed with the attempt counter incremented, and completed pages cannot be
  claimed again. Page completion is idempotent for the same output fingerprint
  and rejects conflicting fingerprints. Failed leased pages persist the worker,
  attempt count, and last error, and can be claimed again by a later worker.
  Leased pages can also persist progress cursor and completed/total unit
  updates without completing the page; active job status mirrors the current
  page cursor, same-worker lease renewal preserves it, and restart coverage
  verifies that page/job cursor progress survives graph-index reopen. Workers
  can also claim the next eligible page by scanning the durable page prefix for
  the current phase/iteration; the scheduler skips active leases, claims pending
  or failed pages, and reclaims expired leases through the same attempt-counting
  lifecycle until an internal bounded retry policy is exhausted. Exhausted pages
  now make the coordinator record a failed active build instead of leaving the
  planned scheduler idle on an unclaimable page. This establishes the
  recovery/retry/progress state machine needed by distributed page workers.
  PageRank now uses this path for scan/out-degree,
  initialize, contribution, reduce, convergence, publish, and cleanup pages;
  scan, contribution, reduce, and convergence pages can persist cursor progress,
  and contribution/reduce/convergence pages now have reopen-and-resume coverage
  on initial and dynamically planned later-iteration pages. Dynamically planned
  later-iteration contribution/reduce/convergence pages also have failed-page
  retry coverage. Cleanup prefix deletion now has durable cursor progress and
  same-worker renewal coverage across graph-index reopen. Reclaimed PageRank
  scan, initialize, contribution, and reduce pages now have coverage that
  expired partial output is recomputed without double-counting or preserving
  stale out-degree, node, contribution, or rank records, and reclaimed
  convergence pages now clear stale partial summaries before recompute.
  Remaining work is broader production expired/reclaimed coverage across all
  phases and later iterative metrics.
- Build jobs now have a durable phase summary and barrier primitive. The
  coordinator can summarize a job phase into expected/completed/failed page
  counts, completed/total units, output fingerprint, and reserved convergence
  fields (`max_delta`, `total_delta`, `rank_sum`, `converged`) by enumerating
  every durable page record for the requested phase and iteration. The summary
  is persisted under the job's phase namespace. Phase advancement is gated on
  the summary being complete: incomplete or failed pages keep the job and active
  lease on the current phase, while a complete summary advances both the build
  job and active lease to the next planned phase. This gives the future
  distributed PageRank executor a restart-safe multi-page barrier contract
  before iteration-specific contribution/reduce/check executors are added.
- Convergence metadata is now part of durable page and iteration state.
  `check_convergence` pages can persist `max_delta`, `total_delta`, `rank_sum`,
  `converged`, and an output fingerprint. The phase barrier aggregates those
  fields across completed check pages, and the coordinator can persist a
  per-iteration summary with expected/completed page counts, convergence
  metrics, convergence status, bounded fixed-iteration status, and output
  fingerprint. This establishes the durable iteration summary contract needed to
  decide whether a distributed iterative metric should publish, schedule another
  iteration, or publish bounded non-converged output at `max_iterations`.
- Verified planned metric publish now flips the public generation pointer and
  advances the active job to `cleanup_old_generations` without deleting the job
  namespace in the publish transaction. The new generation is queryable while
  cleanup is still pending. The cleanup page then deletes the per-job manifest,
  page, phase-summary, iteration-summary, and partial keys under the durable
  control namespace, clears the active build lease, and preserves the public
  score generation plus current `build_job` status record. Degree, PageRank,
  eigenvector, and HITS cleanup-resume coverage now also proves abandoned
  attempt-scoped scan, contribution, and hub-raw output remains present during
  non-final cleanup pages and is removed by the final job-namespace cleanup
  page. Direct cleanup is still refused for an active job unless the cleanup
  worker is executing the planned cleanup phase. Failed-job retention remains
  future work.
- Planned metric builds now have the first verified publish readiness primitive.
  The verifier requires the active build job to be at `publish_generation`,
  checks that the durable manifest still matches the active job and current
  metric config fingerprint, recomputes prerequisite phase summaries from page
  records, requires every pre-publish phase/page to be complete, and records
  iterative convergence readiness before allowing publish. This is not yet the
  final distributed publish transaction and does not write score pages itself;
  it is the metadata gate that future degree/PageRank page executors will call
  before flipping the published generation pointer.
- Planned PageRank failure coverage now verifies that a failed rebuild for a
  newer edge generation keeps the prior published generation queryable, removes
  the abandoned planned-job namespace, deletes any unpublished score generation,
  and preserves compact failed-job diagnostics for status.
- Failed planned builds now also append bounded retained failure diagnostics
  under the metric control namespace. Status exposes the recent failure records
  with sequence, job id, target/building generations, phase, iteration, retry
  count, and last error, records matching failed metric events, and prunes both
  failure records and event records beyond the fixed recent
  retention cap. Fast root coverage now verifies failed planned builds remove
  abandoned score generations and job namespaces, retain bounded diagnostics,
  and prune old failure and event records. This keeps failure inspection useful
  without letting failed job metadata grow without bound.
- `degree` now has the first executable planned-build path. The opt-in planned
  runner claims durable pages, completes them with deterministic output
  fingerprints, advances through the phase barrier, verifies publish readiness,
  and flips the published generation pointer while marking the job complete.
  Focused coverage compares planned degree output against the existing local
  runner and verifies that an active planned rebuild for a newer edge generation
  continues serving direct top-k reads from the prior published generation until
  the verified publish completes. The scan page now reports a deterministic
  page cursor before completion and executes through a dedicated reverse-edge
  cursor scan page, rather than calling the local whole-metric collection
  helper. Planned degree coverage now includes local-output parity, prior
  generation reads while active, and typed edge-filter parity. This is still a
  single-worker, one-page-per-phase executor, but it now runs through a
  worker-step loop that claims the next eligible durable page for the active
  phase, executes the phase-specific page handler, advances barriers, and
  publishes from persisted score state. Its scan page reloads durable page
  metadata and honors reverse-edge lower/upper bounds when executing. Planned
  degree now writes scan output as job-scoped partials and materializes final
  score generation entries in `reduce_ranks`. Coverage verifies that multiple
  scan pages can contribute partial counts for the same node and reduce to the
  correct final degree. The degree planner now creates deterministic
  reverse-edge key-range scan pages and node-range reduce pages, and the worker
  loop drains both before publish. Degree scan pages can now persist an opaque
  reverse-edge cursor, resume from it on same-worker renewal, and merge resumed
  partials into the same page output. Expired degree scan page leases can also
  be reclaimed by another worker and safely recompute from the page range start
  without double-counting abandoned partial output. Cleanup partitions exist for
  degree job namespaces, and planned PageRank plus planned HITS now have local
  alternating-worker coverage over partitioned pages. Degree, PageRank,
  eigenvector, and HITS now also have threaded concurrent-handle coverage
  through the public worker/coordinator boundary, including benign lost-lease
  retry behavior for racing page workers. The same planned build boundary is now
  exposed through DB/index-manager named calls for build ensure, worker page
  step, coordinator step, failure, and drain; DB-level coverage proves a planned
  degree generation can publish through index-name/metric-name worker and
  coordinator calls without passing metric configs to workers. The
  DB/index-manager layer now also has
  bounded coordinator and worker sweeps: coordinator sweeps can start queued
  background planned builds and advance active build barriers/publish steps,
  while worker sweeps claim only active durable page leases by worker id. DB
  coverage proves active planned degree, PageRank, eigenvector, and HITS builds
  can finish through those sweeps without per-metric drain calls, including
  PageRank/eigenvector iterative phase advancement, paired HITS publish, and
  cleanup. Reopened DB-handle coverage now proves a PageRank build can be
  started, worked, coordinated, published, cleaned up, and read through fresh DB
  handles that share only durable job/page state. A bounded planned-maintenance
  primitive now composes background-build startup, worker page sweeps, and
  coordinator sweeps into one scheduler-facing call; DB coverage proves it can
  drain background PageRank, degree, eigenvector, and paired HITS builds without
  the local whole-metric runner. Budget exhaustion is now an observable result
  flag rather than a scheduler error, and repeated tiny-budget ticks can resume
  and finish the same background PageRank build. Pending-work stats now also
  expose graph-metric planned-work hints for queued builds, active builds,
  capped active/failed page summaries, paused metrics, and truncated page
  status, giving the future idle scheduler a cheap way to decide whether the
  planned path still has work. DB coverage now also proves paused metrics are
  counted as paused but not queued or active work, and explicit planned
  maintenance ticks do not start or advance them while the pause is in effect.
  Runtime coverage proves the same pause boundary holds for the graph-metric
  maintenance runtime: a paused background metric produces an idle runtime tick
  with no build started, and an already-started durable planned build is not
  claimed, advanced, or published by split worker/coordinator runtime ticks
  while paused. Both paths resume and publish through the same runtime once
  unpaused. `runUntilIdle` can now exercise planned graph
  metric maintenance behind an internal DB-open gate with explicit worker,
  round, metric, and page budgets for small bounded background PageRank builds;
  budget exhaustion through that path is fail-fast, leaves active planned work
  visible in pending stats, and can resume to completion when the budget is
  raised. The internal `auto` gate now chooses the planned path for already-active
  planned work plus queued degree, PageRank, eigenvector, and compatible HITS
  authority/hub pairs under bounded scheduler caps. HITS hub work is counted as
  part of the authority-owned pair lifecycle, not as a second independent
  metric. Incompatible HITS pairs and explicit operator caps remain outside
  planned startup and can fall back to local maintenance. Coverage now proves
  queued degree, PageRank, eigenvector, compatible HITS, and multi-metric graph
  indexes enter planned maintenance by default, tiny budgets leave resumable
  active work, and per-index caps defer extra queued work until capacity returns.
  The default idle path uses the same bounded `auto` gate, so scheduler-style
  work goes through the planned primitive without adding graph-metric-specific
  CI or Make targets. A first internal graph-metric
  maintenance runtime now wraps the planned-maintenance primitive with the same
  start/stop/notify shape as the other DB maintenance runtimes and can drain a
  background PageRank build through repeated bounded `runOnce` ticks. DB-open
  coverage now also proves an enabled graph-metric runtime starts automatically,
  receives derived-apply notifications, drains a background degree build without
  manual runtime ticks, publishes a fresh generation, and reports durable
  progress plus owner identity through runtime stats. Separate started
  coordinator-role and worker-role runtime loops can also publish a background
  degree build through durable job/page state without manual tick calls, and a
  started worker-pool runtime can do the same with two configured worker IDs
  while a separate coordinator loop owns barriers and publish. The worker-pool
  automatic-loop proof now also covers background PageRank, eigenvector, and
  paired HITS builds, so the iterative reference metric, reusable single-vector
  metric, and paired-vector metric exercise the same split coordinator plus
  worker-pool ownership shape. This
  proves the first automatic split-owner and multi-worker orchestration boundary
  before real remote processes are introduced. The
  planned-maintenance primitive and runtime can also cycle an internal set of
  worker IDs within one bounded round; coverage proves two runtime worker IDs
  complete separate planned scan pages for the same background graph metric,
  including reopened-handle runtime paths where each worker-pool, coordinator,
  and reader tick comes from a fresh DB handle for partitioned degree,
  iterative PageRank, eigenvector, and paired HITS builds. The
  runtime now also exposes split coordinator-only and worker-only ticks, and
  coverage proves a worker tick can complete a page without advancing the build
  phase until a coordinator tick runs. The runtime can now be configured as a
  combined coordinator/worker loop, coordinator-only loop, worker-only loop, or
  worker-pool loop, so automatic maintenance ticks can be assigned the same
  roles as the explicit split entrypoints. Runtime worker/coordinator sweeps now
  carry the runtime clock into durable page claiming and exhausted-page checks,
  and graph-level coverage proves injected time controls whether a leased page
  is still owned by the prior worker or can be reclaimed and completed by a new
  worker. Reopened-handle runtime coverage now
  proves a background PageRank build can be started, worked, coordinated,
  published, cleaned up, and queried through separate fresh DB handles that
  instantiate the runtime per role and communicate through durable graph-index
  job/page state. The runtime can now also opt into durable runtime-owner
  leases, using the same ownership helper as other DB maintenance loops.
  Coordinator and combined owners are role-scoped, while worker and worker-pool
  owners are scoped by configured worker identity so unrelated workers do not
  serialize each other. Worker-pool identity uses an order-independent worker-id
  set fingerprint, with unit coverage proving reordered worker-id sets produce
  the same runtime owner lease key while different sets do not, and duplicate
  worker identities in one pool are rejected by planned-maintenance, runtime
  initialization, and command parsing. Manual worker ticks on
  lease-owned runtimes are bound to the configured worker identity or worker-pool
  set, coordinator-role owners cannot execute worker pages, and worker-role
  owners cannot execute coordinator ticks. Coverage proves a second owner for
  the same coordinator role, same worker identity, or same worker-pool identity
  set is refused while the lease is active, can take over after expiry, and the
  former owner observes lease loss without doing graph-metric work, while
  independent coordinator and worker owners plus independent worker identities
  can hold separate runtime leases and continue the same build together. Split
  runtime coverage now also proves distinct lease-owned worker runtimes can
  complete separate active pages for one build without serializing on the worker
  role, while live duplicate owners for the same worker identity or worker-pool
  identity set are fenced before they can claim or complete durable pages.
  Coverage also proves a replacement owner for the same worker identity can
  take over after the runtime lease expires during an active build, complete a
  durable page through the same worker-identity scope, and make the former owner
  observe lease loss instead of doing more graph-metric work. The
  runtime now also records internal tick telemetry for role, separate runtime
  and owner identity hashes, hashed runtime lease scope, configured worker
  identity hash/count, runtime lease
  ownership/acquisition/loss counters, started/completed ticks,
  durable-progress ticks, idle ticks, error ticks, last error name, cumulative
  planned-sweep totals, and last planned sweep result, with coverage proving
  progress, idle, recovered-error, role, owner accounting, durable lease
  takeover, and cumulative coordinator/worker/page/publish counters. That
  telemetry is now also surfaced through internal DB stats/runtime-status
  snapshots so operations can observe scheduler health and distinguish
  coordinator/worker owners without exposing public job resources. Direct
  runtime-owner lease cleanup coverage now also verifies that clean runtime
  shutdown removes the durable lease record, while stale-owner shutdown after
  takeover preserves the replacement owner's lease record. PageRank-style scan
  pages now use the same attempt-scoped output adoption pattern as contribution
  pages and degree scan pages: partial out-degree/node output is written under
  the current page attempt, is invisible to phase aggregation until that attempt
  completes, and stale workers cannot adopt or mutate abandoned scan output
  after another worker reclaims the lease. Reclaimed PageRank, eigenvector, and
  HITS scan-page tests prove abandoned scan-attempt output remains isolated
  until the replacement attempt completes and publishes its own scan partials.
  Broader remote production multi-worker orchestration remains future work.

### Phase 3: More Graph Metrics

Once the framework can reliably materialize PageRank, add more metrics through
the same config, status, storage, and query paths.

Candidate metrics:

- `eigenvector`: useful for undirected or strongly connected influence graphs,
  but needs careful handling for disconnected and reducible graphs.
- `hits_authority`: useful for citation/search-link authority scoring.
- `hits_hub`: useful for identifying connector or curator nodes.
- `personalized_pagerank`: useful for tenant-, user-, or seed-specific ranking,
  but it changes storage cardinality and should not be the second metric.
- `degree`: cheap baseline metric, useful for testing and fallback ordering.

Current implementation progress:

- `degree` uses the shared graph metric config, dirty state, generationed score
  storage, top-k reads, status, projection, ordering, filtering, and edge
  filtering paths.
- `eigenvector` uses the same local generationed materialization and query
  framework with fixed-iteration power iteration over the resolved edge scope.
  It publishes bounded non-converged output with `converged: false`, matching
  the framework contract.
- `hits_authority` and `hits_hub` use the same local generationed
  materialization and query framework with fixed-iteration HITS updates over the
  resolved edge scope. When a compatible authority/hub pair is configured with
  the same scope and iteration settings, local maintenance computes once and
  publishes both generations atomically.
- Distributed jobs and personalized PageRank remain future work.

Do not add a metric until it has:

- documented graph assumptions
- convergence behavior
- failure behavior for disconnected/sink-heavy graphs
- storage key namespace
- status metadata
- direct top-k tests
- traversal projection/order tests

For Eigenvector centrality specifically:

- support fixed-iteration publish with `converged: false`
- expose normalization mode
- document behavior on disconnected components
- consider returning per-component scores or using the largest component only
  only if the API names that behavior explicitly

Recommended order:

1. `degree`
2. `eigenvector`
3. `hits_authority` and `hits_hub`
4. `personalized_pagerank`

`degree` is the right second metric because it is cheap, deterministic, and
does not require iterative convergence. It exercises the generic storage,
status, top-k, projection, ordering, and edge-filter paths without adding
algorithmic complexity.

Eigenvector centrality should come after the framework has a second non-PageRank
metric. It needs explicit documentation for disconnected graphs, reducible
graphs, normalization, and non-convergence. It should reuse the PageRank
iteration/job machinery where possible, but its API must not imply PageRank
damping semantics.

HITS should be added as a paired metric family. Authority and hub scores are
separate named metrics, but they should be computed by one job when they share
the same edge scope. The publish step should either publish both scores for a
generation or neither.

Personalized PageRank is intentionally later because it multiplies score
cardinality by seed, tenant, user, or profile. It likely needs an additional
scope key:

```text
graph_metric_score:<index>:<metric>:<scope>:<generation>:<node_id> -> f64
```

Do not add personalized metrics until retention, deletion, and quota controls
exist for high-cardinality metric families.

### Phase 4: Edge Scope and Metric Families

The first edge filter supports all edges or typed include lists. Long-term,
metric scope should become richer but still compile to explicit graph-index
state, not arbitrary runtime scanning.

Add scoped metric families:

```json
{
  "metrics": {
    "pagerank_all": {
      "kind": "pagerank",
      "edge_filter": { "mode": "all" }
    },
    "pagerank_citations": {
      "kind": "pagerank",
      "edge_filter": { "types": ["cites", "references"] }
    }
  }
}
```

Future edge filters may include:

- labels
- edge metadata equality/range predicates
- source/target label filters
- tenant or namespace filters

Constraints:

- Filter config must be stable and serialized in index metadata.
- Filter changes create a new metric materialization generation.
- Filter predicates should be validated against graph-index edge metadata
  definitions where possible.
- Arbitrary predicate execution should wait until graph edge metadata has a
  typed expression/predicate contract.

Implementation milestones:

- Store resolved edge scope in metric metadata for each generation.
- Validate metric filter config against graph edge metadata.
- Support named metric families with shared kind and different edge scopes.
- Treat edge-filter changes as materialization changes requiring rebuild.
- Add explain/status output that shows the resolved edge scope.

Metric families should be named explicitly by users. Avoid implicit metric names
such as `pagerank_mentions_cites` generated from filters; stable names are
important for query APIs, status, retention, and migrations.

Current implementation progress:

- Published graph metric generations now persist their resolved edge filter as
  generation metadata. Status prefers the published generation's stored edge
  scope when available and uses the current config only when stored scope is not
  present. This keeps status/explainable scope tied to the scores that were
  actually published, even if a future config change marks the metric stale or
  requires rebuild.
- When a metric's current edge filter no longer matches the published
  generation's stored edge filter, status reports the metric as `stale` and
  queued for rebuild while still showing the scope of the currently published
  scores.
- Published generations also persist a materialization config fingerprint for
  algorithm-affecting settings such as metric kind, damping, tolerance,
  max-iterations, and edge scope. When the current config fingerprint differs
  from the published generation, status marks the metric `stale` even if the
  graph edge generation itself has not changed.
- Graph metric edge-scope validation now rejects empty typed scopes, blank edge
  type names, duplicate edge types, and `all` scopes that also carry explicit
  types. Unknown edge types are rejected when graph edge type metadata is
  available.

### Phase 5: Operational Controls

Add operational controls only after the default maintenance loop is reliable.

Useful controls:

- manual refresh endpoint
- pause/resume metric maintenance
- per-metric max build concurrency
- retention controls for debug/admin users
- rebuild-from-scratch endpoint
- metric deletion with generation cleanup
- progress/event log for failed builds

Retention should remain latest-only by default. Optional future retention:

```json
{
  "retention": {
    "mode": "count",
    "retained_generations": 2
  }
}
```

Operational safety rules:

- A manual refresh should not block writes.
- A rebuild should not delete the prior published generation until publish.
- Metric deletion must remove dirty, published, metadata, score, and job keys.
- Failed builds preserve the last published generation and keep status
  queryable.

Implementation milestones:

- Add manual refresh and rebuild endpoints.
- Add pause/resume for background maintenance.
- Add metric deletion with full key cleanup.
- Add admin-only retention controls.
- Add per-metric build concurrency and queue visibility.
- Add structured event logs for build failures and publish decisions.

Operational APIs should be idempotent. Repeating a manual refresh, delete, or
pause request should be safe and should return the current metric state.

Expose retention only as an admin/debug feature. Latest-only remains the default
because most users need current scores, not historical centrality snapshots.

Current implementation progress:

- Local graph indexes expose a metric materialization deletion primitive that
  removes dirty, published, metadata, and score keys for a configured metric.
  A durable disabled marker prevents background maintenance from immediately
  recreating operator-deleted data; refresh, rebuild, or resume explicitly
  re-enables the configured metric and rebuilds it from durable graph edges.
- DB and index-manager layers expose local manual refresh and rebuild
  primitives for a named graph metric. Manual refresh runs the configured metric
  regardless of background refresh mode; rebuild clears materialized state first
  and then recomputes from durable graph edges.
- DB and index-manager layers expose local pause/resume primitives for
  background graph metric maintenance. Paused metrics remain queryable from the
  last published generation, status reports `maintenance_paused`, and manual
  refresh can still publish a fresh generation while background maintenance is
  paused.
- The public table HTTP layer exposes graph metric operational actions at
  `POST /tables/{table}/indexes/{index}/graph-metrics/{metric}:{action}` for
  `refresh`, `rebuild`, `delete`, `pause`, and `resume`. Each action is
  idempotent at the API contract level and returns the updated graph metric
  status. `delete` clears materialized metric state, reports `disabled`, and
  suppresses automatic maintenance while leaving the configured metric
  available for a later refresh, rebuild, or resume.
- The modular OpenAPI source specs and regenerated Zig public/client contract
  modules now describe graph query metric projection, ordering, filtering,
  freshness, result status metadata, and the graph metric operational action
  route with a typed status response.
- Graph metric status now includes forward-compatible build observability
  fields: `phase`, `target_edge_generation`, and `progress`. Local metric
  maintenance reports `complete` with `progress: 1.0` for the current published
  target and `idle` with `progress: 0.0` when a metric is not ready or has a
  pending target generation. These fields are part of the public JSON response,
  graph query metric status, and generated OpenAPI client/server contract.
- Graph metric materializations now record durable structured events for local
  publish, failed build, delete, pause, and resume decisions. Status exposes both
  `last_event` and a bounded newest-first `recent_events` history with monotonic
  sequence, kind, timestamp, target edge generation, published generation, and
  score count. Durable event keys are pruned to the same retained event window,
  keeping local operational history latest-only by default. These fields are
  included in public JSON and generated OpenAPI client/server types. This is
  the first local event-log primitive; future distributed jobs can extend it
  into a paginated history without changing the status shape.
- Failed local metric builds now record a `failed` event, surface `failed`
  status for the current target generation, persist consecutive `retry_count`
  and `last_error` details, and preserve the prior published generation and
  queryable scores. A later successful publish clears the failure details while
  retaining the event history.
- Local metric builds now acquire a durable build lease before running. Status
  exposes whether a build is queued, the queued generation, the active building
  generation, the durable build job id, the active build iteration, the
  persisted worker id, persisted phase, and the build lease expiration.
  Overlapping local builds fail fast with `GraphMetricBuildAlreadyRunning`,
  which gives the future distributed job system a compatible queue/lease status
  shape without changing the public status API later. The lease metadata remains
  backward-compatible with older local lease records that only stored generation
  and expiration, and with the first versioned local lease records that did not
  carry iteration or job id. Active status progress now derives from the
  persisted build phase and iteration: initial compute reports low progress,
  iterative compute advances by iteration/max-iterations, publishing reports
  near-complete progress, and complete reports `1.0`. Local PageRank,
  eigenvector, HITS, and degree maintenance update that persisted
  phase/iteration state while computing and before publishing, providing the
  same observable shape that distributed resumable workers will use. Active
  leases survive graph-index reopen with their job id, start timestamp, phase,
  worker, and iteration intact, while expired leases are ignored by status and
  can be reclaimed by a later build. The public OpenAPI graph metric status
  schema and hand-written JSON status encoder now include `build_job_id` and
  `build_started_at_ms`, and remote response parsing plus deterministic shard
  merge preserve matching active build job ids and start timestamps. DB graph
  query conversion now deep-copies active build worker ids when cloning graph
  metric status out of temporary graph query results, so active lease status
  remains valid after the temporary result is released.

### Phase 6: Planner and Retrieval Composition

After graph query integration is stable, allow graph metrics to participate in
broader retrieval planning.

Examples:

- blend semantic score and PageRank in reranking
- use PageRank as a tie-breaker for graph-expanded retrieval
- use authority scores as a feature in result pruning
- materialize graph metrics into explain/profile output

Planner constraints:

- Metric score usage must be explicit in the request or a documented retrieval
  profile.
- `fresh` metric requirements should affect sync targets and maintenance waits.
- Query profiles should show metric generation and freshness state.
- Cross-shard score ordering must either use a globally comparable score
  generation or fail closed.

Implementation milestones:

- Add graph metric score features to query explain/profile output.
- Add explicit rerank expressions that can combine lexical, vector, relational,
  and graph metric scores.
- Add freshness-aware planning for requests that require `fresh` metrics.
- Add deterministic cross-shard metric top-k merge behavior.
- Add public tests for metric-assisted retrieval with stale and fresh modes.

This phase should not make PageRank a hidden default ranking factor. Any metric
use that changes result ordering must be visible in the request, index profile,
or explain output.

Current implementation progress:

- Public query profiles now include `graph_metrics` entries for direct graph
  metric top-k reads and graph-query metric status when graph queries request
  metric status. Each profile entry records the query name, source, graph index,
  metric name, effective freshness mode, and the observed graph metric status,
  including published generation, target edge generation, state, and progress.
- The distributed merge layer contains a deterministic, fail-closed contract
  for a future globally coordinated metric materialization. It groups direct
  results by requested query name, validates index, metric, configuration
  fingerprint, edge scope, and publication metadata, sorts scores by score
  descending with node-key tie-breaking, and applies `top_k` only after merge.
  `metric_freshness: "fresh"` is enforced again at fan-in. Focused merge coverage
  also
  verifies paired HITS authority/hub results preserve the last compatible
  published generation when a shard reports a failed rebuild, while `fresh`
  fails closed. Hosted cross-range coverage now proves compatible published
  shard generations merge through the table-read source and that unpublished or
  incompatible shard generations are rejected before ranking. The hosted
  table-read gate now also includes a nonuniform eight-shard degree/PageRank/
  eigenvector layout so every shard contributes a published metric score through
  the real cross-range reader. Four of those hosted shards are also advanced
  into a newer building generation with unpublished high-score targets;
  `published` fan-in still merges only the prior target scores and `fresh` fails
  closed for all three single-vector-style metrics. Separate hosted active-stale
  coverage still exercises direct top-k, traversal
  projection/order/filter, and search rerank while `fresh` fails closed. The
  hosted compatible HITS authority/hub fixture now also uses an eight-shard
  layout with four active/stale shards, covering direct, traversal
  projection/order/filter, and authority rerank merge behavior with prior-pair
  preservation, `fresh` rejection, remote HITS generation/metadata/edge-filter
  mismatch rejection, and missing-status rejection.
  This narrows the gap between focused merge tests and promotion-scale shard
  evidence, but it does not make independently normalized shard-local scores
  globally comparable.
- Until a coordinator builds and publishes one table-wide metric snapshot,
  production table reads fail closed before fan-out when a request uses direct
  metric top-k, metric-assisted reranking, or graph metric projection, ordering,
  or filtering across more than one shard. Traversal without metric scores
  remains supported. This gate prevents numerically compatible-looking local
  generations from silently producing incorrect global rankings. The internal
  merge contract remains tested for the future coordinated implementation.
- Ordinary search requests can now opt into explicit graph metric score
  blending with `graph_metric_rerank`. The first implementation validates that
  the requested graph metric has a published generation, honors
  `metric_freshness: "fresh"` with `MetricStale`, and computes
  `final_score = base_weight * existing_score + weight * metric_score`, where
  `missing_score` supplies the metric feature value for hits missing from the
  published metric generation. Hits are sorted by final score with document id
  tie-breaking. This is intentionally visible in the request rather than a
  hidden PageRank boost, and it provides the first metric-assisted retrieval
  test surface for stale, fresh, and missing-score modes. Profile output now
  includes a `source: "graph_metric_rerank"` entry with the observed metric
  status, and distributed fan-in fails closed if reranked shard scores do not
  report the same nonzero published metric generation, if any shard omits or
  reports an unpublished rerank metric status, or if a fresh rerank request
  receives a stale/building/failed shard status. Hits reranked by this path now
  include `_score_details.graph_metric_rerank` with the base score,
  base weight, metric score, metric weight, missing-score fallback flag, final
  score, and published metric generation used for that hit. Richer expression
  support remains future work. Search rerank coverage now also proves an active
  planned rebuild cannot leak building output: published rerank uses the prior
  generation and reports building status, while fresh rerank fails closed. The
  table-read shard serializer now carries representable single-metric
  `graph_metric` reads and `graph_metric_rerank` requests so hosted remote
  shards do not silently lose those public read surfaces; multi-metric HITS
  traversal fan-in now has focused hosted coverage, while HITS rerank remains a
  merge-contract gate until a public multi-metric rerank wire shape is
  introduced.

### Phase 7: Compatibility and Migration

Long-term graph metric APIs should stay compatible with the PageRank-first
surface:

- existing `graph_metric` top-k reads continue to work
- `metric_status` remains a map keyed by metric name
- `published` remains the default freshness mode
- `fresh` continues to use `MetricNotReady` and `MetricStale`
- fixed-iteration non-converged results continue to publish unless a metric
  explicitly opts out with documented behavior

If API names change, keep aliases for at least one compatibility window. For
example, if `metric_freshness` later becomes a nested object, continue accepting
the string form:

```json
{
  "metric_freshness": "fresh"
}
```

Implementation milestones:

- Version graph metric metadata schemas.
- Keep compatibility aliases for renamed API fields.
- Add migration code for metric config shape changes.
- Add validation that refuses ambiguous metric definitions.
- Document deprecation windows for generated clients.

Migration should never require recomputing scores synchronously during startup.
If a schema migration invalidates a metric generation, mark the metric stale or
not ready, preserve enough metadata for diagnosis, and let maintenance rebuild
asynchronously.

Current implementation progress:

- Published graph metric metadata now carries an explicit schema version.
  Newly written materializations encode version `3`. Public status responses
  expose `metadata_version` while generated clients treat the field as optional
  so shard responses remain parseable when a metric has not published yet.
- Graph metric config validation now rejects empty and duplicate metric names
  before an index opens. This keeps status maps, direct metric reads, and future
  migration code from having to resolve ambiguous metric definitions.

### Remaining Roadmap: Distributed and Resumable Metric Execution

The remaining graph-index work is mostly about replacing the local whole-metric
runner with a durable coordinator that can execute the same materialization
lifecycle across pages and workers. The public API should not change materially:
users still configure named metrics on a graph index, read published
generations, request `published` or `fresh` freshness, and inspect status.

The implementation should continue in small production-hardening slices, but
the center of gravity has moved from proving the planned runner to hardening it.
Degree, PageRank, eigenvector, and HITS now exercise the shared planned
lifecycle. The remaining work should make that lifecycle production-distributed:
real worker ownership, broader restart/failure coverage, attempt-scoped output
adoption where recompute is too expensive, paired-vector hardening for HITS,
and rollout gates that prove query freshness and cleanup behavior before the
planned runner becomes the default.

Design principle for the rest of the work: user interface stability comes first.
The graph metric lives with the graph index, the user names and configures the
metric there, and all job/page/attempt mechanics remain internal. The only
public effects of distributed execution should be better scalability, richer
status, explicit failures, and the same generation/freshness contract that local
PageRank already exposes.

The roadmap should now be read as a production-execution plan rather than a
PageRank-only feature plan. Degree is the cheap lifecycle proof, PageRank is the
dynamic-iteration proof, eigenvector is the reusable single-vector proof, and
HITS is the paired-vector proof. Each remaining milestone should preserve the
same external graph-index metric API while moving more work behind durable
coordinator/worker boundaries.

Roadmap north star:

```text
named graph-index metric config
  -> dirty marker and target edge generation
  -> coordinator-owned durable build
  -> deterministic manifest pages
  -> worker-owned page leases and cursors
  -> coordinator-owned phase/iteration barriers
  -> verified publish of one complete generation
  -> published generation pointer
  -> resumable cleanup
```

The rest of the work is organized into four release tracks:

| Track | Purpose | First promotion gate |
| --- | --- | --- |
| Default planned maintenance | Make scheduler-style graph metric work use the bounded planned-maintenance primitive without changing public query behavior. | `runUntilIdle` and background scheduler ticks can finish degree, PageRank, eigenvector, and compatible HITS under latency-safe budgets while unsupported workloads remain outside planned execution. |
| Production worker ownership | Move from in-process role-configured runtimes to real coordinator and worker owners that communicate only through durable graph-index state. | A remote worker can die after claiming a page, another worker can reclaim it after expiry, and duplicate coordinators cannot double-publish. |
| Failure, HITS, and cleanup hardening | Close restart/reclaim/publish/cleanup coverage for PageRank, eigenvector, and paired HITS, including explicit hub phases. | Failed or abandoned rebuilds preserve the prior published generation or compatible HITS pair, and cleanup resumes after restart without unbounded storage growth. |
| Public-read and per-family promotion | Promote the planned runner by metric family only after every score-bearing query path proves the same freshness contract. | Direct top-k, traversal, search rerank, explain/profile, and cross-shard fan-in either read compatible published generations or fail closed with `MetricNotReady`/`MetricStale`. |

Concrete ordering:

1. Use the bounded planned-maintenance `auto` gate as the default scheduler
   path for degree, PageRank, eigenvector, compatible HITS pairs, and
   multi-metric graph indexes. Keep incompatible HITS and explicit operator
   caps on the local fallback path, and use telemetry to tune production-safe
   idle budgets.
2. Treat the current role-configured graph-metric runtime as the compatibility
   harness for the remote-worker contract. Its `combined`, `coordinator`,
   `worker`, and `worker_pool` roles should map directly to future process
   owners, but the durable job/page records remain the only communication
   boundary.
3. Harden the restart/failure matrix while worker ownership is still easy to
   test locally. PageRank stays the baseline matrix; eigenvector catches up as
   the single-vector iterative case; HITS catches up by applying the same
   restart, reclaim, failure, and cleanup matrix to its explicit authority and
   hub phases.
4. Harden the explicit HITS hub contribution and hub reduce/norm phases before
   enabling remote HITS builds by default. Authority and hub stay separate named
   metric scores, but publish, failure, and freshness remain paired for
   compatible configs.
5. Keep v1 cleanup aggressive: latest published generation plus bounded failed
   diagnostics. Retention, manual retry, pause/resume, and priority scheduling
   are future admin/debug controls, not query semantics.
6. Promote per metric family. Degree and PageRank go first, eigenvector follows
   after restart/failure parity, and HITS promotes last after paired-vector
   partitioning and paired failure coverage.

#### Canonical Rest-of-Work Plan

The remaining work is not another public metric API. It is the productionization
of the graph-index-owned metric executor. The current implementation has enough
durable job, page, runtime, scheduler, and status machinery to use the planned
runner as the compatibility harness. The rest of the roadmap should move one
ownership boundary at a time until coordinator and worker processes can safely
operate through durable graph-index state only.

The user interface should stay fixed through that transition:

- Users configure named metrics on the graph index.
- Reads choose `published` or `fresh`.
- `MetricNotReady` means no complete generation exists.
- `MetricStale` means a complete generation exists, but not for the current edge
  generation requested by `fresh`.
- Fixed-iteration non-converged PageRank, eigenvector, and compatible HITS
  publish by default with `converged: false` and final iteration metadata.
- V1 cleanup keeps only the latest published generation plus bounded failure
  diagnostics; retention is a future bounded admin/debug option.
- Jobs, pages, attempts, leases, worker identity, retry limits, and backoff
  remain internal. Status may summarize them, but users should not manage them.

Design the rest of the implementation around five internal boundaries:

| Boundary | Owns | Must not own |
| --- | --- | --- |
| Graph index | Metric config, edge-scope fingerprint, dirty marker, published pointer, public score metadata, query freshness, and status assembly. | Worker scheduling policy or raw job/page controls. |
| Coordinator | Job start/resume, manifest validation, phase barriers, dynamic iteration planning, verified publish, active-build failure, cleanup scheduling, and duplicate-coordinator fencing. | Page execution or worker-local progress. |
| Worker | One leased page at a time: claim, renew, cursor, deterministic output, complete/fail, and stop. | Job creation, config resolution from callers, phase advancement, publish, or failure of the whole build. |
| Runtime/scheduler | Process role, owner identity, worker identity set, tick budgets, wakeups, telemetry, and bounded maintenance loops. | Metric semantics or public freshness behavior. |
| Cleanup runner | Resumable deletion of old score generations, abandoned attempts, completed job namespaces, manifests, pages, summaries, and bounded diagnostics. | Deleting the current published generation or changing query semantics. |

The production owner model should be explicit before remote workers are enabled:

- Coordinator ownership is scoped to the graph-metric coordinator role for a DB
  or graph-index maintenance domain. Duplicate coordinators are fenced until the
  durable lease expires, and a later coordinator can take over after expiry.
- Worker ownership must be scoped by worker identity, not only by the worker
  role. Two distinct workers should be able to hold independent runtime-owner
  leases and claim different page leases for the same build. A duplicate owner
  for the same worker identity should be fenced until expiry; after expiry, a
  replacement owner should be able to continue active page work through the same
  worker-identity scope while the old owner records lease loss.
- Worker-pool ownership should be keyed by the configured worker identity set in
  an order-independent way, or by one stable pool identity. The important
  invariant is that unrelated worker owners do not serialize each other, while
  duplicate owners for the same worker identity are detectable. Duplicate
  worker identities inside one configured pool are rejected at the command and
  runtime/scheduler validation boundaries instead of silently running the same
  worker twice. Coordinator and single-worker command roles also reject worker
  identity lists, and enabled runtime config validation enforces the same
  boundary so role argv and in-process owner config stay scoped to the owner
  type.
- Process owners must be launched with positive runtime lease TTL and positive
  maintenance budgets for rounds, metrics, and pages. A zero TTL or zero budget
  is rejected by command/supervisor parsing and by enabled runtime config
  validation; it is not a valid idle process state.
- Runtime role gates are independent from durable lease ownership. Disabling
  the lease fence can be useful for local compatibility paths, but it must not
  let a coordinator execute worker pages, a worker execute coordinator steps, or
  a worker role use an unconfigured worker identity.
- Page leases remain separate from runtime-owner leases. Runtime leases fence
  process ownership; page leases fence page execution and are reclaimed by
  expiry/attempt policy.

Current runtime coverage now exercises that process boundary through DB-open
configuration, not only through manually constructed runtimes:

- `OpenOptions.graph_metric_maintenance` can initialize a graph-metric runtime
  as `combined`, `coordinator`, `worker`, or `worker_pool`.
- `start_background_loop = false` lets tests and future process harnesses open a
  DB handle with the production runtime configuration but drive one explicit
  `runOnceDetailed` tick instead of starting an in-process background loop.
- `antfly graph-metric-maintenance` is now the first server-side process
  entrypoint for those roles. It opens one DB path with
  `OpenOptions.graph_metric_maintenance`, runs explicit bounded ticks as
  `combined`, `coordinator`, `worker`, or `worker_pool`, and emits JSON runtime
  stats so separate owners can run without passing metric configs to workers.
  The command supports bounded polling with `--tick-ms` and
  `--max-idle-ticks`, so supervised role processes can stay alive across idle
  rounds and still terminate deterministically in tests. It also has a first
  `supervise` mode that launches coordinator and worker-pool child role
  processes with shared DB path, owner identities, worker identity set, bounded
  tick policy, idle policy, and a bounded restart policy; child stdout is
  captured so the supervisor can parse per-role durable progress and emit one
  JSON summary with child exit/output sizes. Command-level coverage now also
  asserts that production-style launched child argv stays scoped to DB path,
  role, owner identity, worker identity, lease timing, tick budgets, idle
  policy, and page/metric budgets; it does not pass metric names, index names,
  target generations, job/page ids, metric configs, or the direct file-backed
  writer-lock guard used only by summary-file child launches. Command summary
  coverage also pins the stable operations telemetry emitted by role processes:
  role, runtime hash, owner hash, worker identity/count, lease-key hash,
  acquisition/takeover/lost-lease counters, tick progress, idle/error counts,
  and last error. The supervisor/launcher aggregate summary now preserves a
  compact version of that child telemetry for each role, and supervised-degree
  coverage asserts coordinator and worker-pool owner/worker/lease/tick/error
  fields survive aggregation after the build drains to fresh. The spawned
  process harness now also validates the same stable role, runtime/owner hash,
  worker hash/count, lease-key, tick, and error telemetry on every standalone
  coordinator, worker, and worker-pool role process it uses for restart,
  lease-fencing, publish, cleanup, and reclaim proofs. Those standalone role
  summaries must include durable-progress, idle, and error tick counters, and
  completed ticks must be explained by progress, idle, error, lease contention,
  or lease loss. They must also report either a runtime-owner lease acquisition
  or an explicit lease-acquisition failure, so a role summary cannot silently
  omit ownership accounting. The harness rejects any non-null `last_error_name` before
  accepting the typed summary. Its final release-gate summary emits required
  and observed coverage counts for every remote-owner category, including
  separate service multi-page coordinator-takeover and worker-pool-takeover
  counts plus explicit service worker-phase, coordinator-phase, and takeover
  phase-proof floors, before setting `remote_owner_release_gate: true`, so
  release tooling can audit the process gate from one JSON event. The same raw-field guard now
  runs over both
  standalone role summaries and aggregate supervisor/launcher summaries,
  recursively rejecting operational JSON fields such as metric/index names,
  target generations, job/page ids, attempt namespaces, storage paths/prefixes,
  metric configs, process ids, and local writer details. The same harness now preflights
  those standalone and killable role-owner argv vectors through a strict
  allow-list, with positive and negative harness checks rejecting metric names,
  index names, target generations, job/page ids, metric configs, summary files,
  unknown flags, missing values, and the local file-writer guard at that
  boundary.
- Fresh coordinator and worker-pool DB handles can publish partitioned degree,
  PageRank, eigenvector, and compatible HITS builds through durable graph-index
  state only, with no worker-side metric config and no manual runtime
  construction. PageRank is the reference proof that the same process-style
  boundary survives dynamic iteration, convergence, publish, and cleanup; HITS
  proves the same DB-open owner boundary can preserve paired authority/hub
  publication.
- This is still an initial process-orchestration proof, not full distributed
  failure coverage. The next step is to run supervised child roles under
  crash/restart tests that kill and replace active workers and coordinators
  while proving lease expiry, stale-attempt fencing, publish idempotence, and
  cleanup resume.

The remaining stages should land in this order:

| Stage | What changes | Design details | Exit gate |
| --- | --- | --- | --- |
| 1. Scheduler default readiness | Planned maintenance becomes the scheduler path for bounded graph-metric work. | Use the bounded `auto` gate as the single internal entrypoint for queued build startup, worker page sweeps, coordinator sweeps, pending-work stats, and budget exhaustion. Global/per-index caps keep multi-metric indexes fair, and compatible HITS counts as one paired lifecycle. | `runUntilIdle` and background ticks drain degree, PageRank, eigenvector, compatible HITS, multi-metric indexes, and already-active planned work without unbounded latency or partial visibility. Incompatible HITS and explicit caps remain fallback cases. |
| 2. Runtime ownership hardening | The in-process runtime matches the future remote ownership contract. | Add/keep durable owner leases for coordinator, combined, worker, and worker-pool roles. Worker leases must be worker-identity scoped. Runtime stats should expose role, owner hash, worker identity hash/count, lease acquisition/failure/loss, progress, idle, errors, and last sweep result. | Distinct worker identities can coexist; duplicate same-role coordinators and duplicate same-worker owners are fenced; clean runtime shutdown releases the current owner's durable lease without waiting for TTL; stale shutdown after takeover cannot clear the replacement owner's lease; same-worker replacement takeover after expiry is covered during active page work; split coordinator/worker runtimes publish through durable state only. |
| 3. Remote process orchestration | Real coordinator and worker processes use the same durable boundaries. | Coordinators tick only build startup/barriers/publish/failure/cleanup. Workers tick only page claims and execution by index/metric name. No worker receives metric config from a caller. Communication is durable graph-index job/page state plus leases. | Killing a worker abandons only its page/runtime lease; a replacement for the same worker identity takes over after expiry without duplicating page output; duplicate/racing coordinators cannot double-publish or append incompatible phases. |
| 4. Failure and restart matrix | PageRank becomes the reference distributed failure matrix; eigenvector and HITS catch up. | Cover reopen, expired lease, reclaimed output, same-worker cursor resume, failed page retry, exhausted attempts, publish verifier failure, cleanup resume, and prior-generation preservation across prepare, scan, initialize, contribution, reduce, convergence, publish, and cleanup. | Failed or abandoned rebuilds preserve the prior published generation or compatible HITS pair. Every output-writing phase either resumes from cursor, recomputes safely, or adopts attempt-scoped output only after completion. |
| 5. Attempt-output policy | Attempt namespaces are used only where correctness or cost needs them. | Deterministic recompute remains the default. Use attempt-scoped output for contribution-like phases and any reduce-like phase whose partial output can be consumed too early, cannot be atomically replaced cheaply, or is too expensive to recompute at production scale. | Later phases read only adopted output. Abandoned attempts are invisible to readers and removed by cleanup. Reduce-phase adoption is added only when tests prove stale-output or recompute risk. |
| 6. HITS paired-vector hardening | HITS becomes production-ready as the paired-vector proof. | Authority and hub stay separate named metric scores, but compatible configs share one target generation, convergence decision, failure decision, and atomic publish. Hub contribution and hub reduce/norm remain explicit retryable phases, and local lifecycle coverage now proves reclaim, failed-page/exhausted-attempt handling, publish failure, cleanup resume, larger-manifest restart across reopen boundaries, parity, and stale-read behavior preserve the previous compatible pair. | Remote HITS builds are not enabled by default until promotion-scale fan-in, deployment-scale owner evidence, operations evidence, and larger-graph latency all preserve the previous compatible pair. |
| 7. Public read and fan-in gates | Every score-bearing query path proves the same freshness contract. | Direct top-k, traversal projection/order/filter, graph search rerank, explain/profile, and cross-shard fan-in resolve scores only through complete published generations. Active job output, failed attempts, and abandoned generations stay unqueryable. | `published` reads use the latest complete generation during queued/building/failed rebuilds. `fresh` fails with `MetricNotReady` or `MetricStale`. Cross-shard direct metric and graph-search fan-in reject unsolicited or unrequested score/status surfaces, extra unrequested graph metric status names, missing, zero, stale, duplicate/ambiguous graph requests, projected metric requests, graph order metric requests, graph-search node/hit result ownership, or score-node ownership, malformed graph-search traversal/path-shape/node/hit metric payloads or internally inconsistent rerank score details, including mismatched request weights, missing-score semantics, or final-score formula, non-finite traversal distances, path weights, metric/rerank scores, or status numbers, out-of-range progress, mismatched index/metric/status identity, incompatible metadata version or edge filter, invalid state/generation combinations such as `fresh` status pointing at a newer current or target edge generation than the published scores, or incompatible published generations when comparability is required. |
| 8. Cleanup and operations | Cleanup becomes a production invariant, not best-effort housekeeping. | V1 aggressively removes completed job namespaces, abandoned attempts, unpublished scores, manifests, pages, summaries, and old generations when snapshot-safe. Pause/resume/manual retry/priority/retention remain future admin controls. | Cleanup resumes after restart, never hides the current published generation, bounds diagnostics, and prevents unbounded storage growth without requiring a public retention knob. |
| 9. Per-family promotion | Planned/distributed execution becomes default by metric family. | Keep local runners as deterministic CI/debug oracles. Promote degree and PageRank first, eigenvector after single-vector restart/failure parity, and HITS last after paired-vector production coverage. | Each family has local-vs-planned parity, remote-worker coverage, restart/failure coverage, cleanup coverage, public freshness coverage, cross-shard coverage, and operations docs before default promotion. |

The stages have a strict dependency shape:

- Scheduler default readiness can promote before real remote processes, but it
  must keep conservative gates and resumable budgets.
- Remote process orchestration depends on the runtime ownership model matching
  the future process model: coordinator ownership, worker identity ownership,
  and page leases must be independently fenced.
- The failure matrix depends on attempt-aware page execution. Progress,
  completion, failure, and any output write that becomes visible to a later
  phase must validate the durable page attempt, not just worker identity.
- Attempt-scoped output adoption is not mandatory for every phase. The
  mandatory invariant is that stale attempts cannot publish, adopt, or write
  visible output after the page lease has been reclaimed.
- Public read promotion depends on the failure matrix and cleanup guarantees,
  because readers must never need to understand building namespaces, abandoned
  attempts, or retained failed-job state.

The transaction rule for the rest of the implementation is: when a page writes
output that is already public or can be consumed by a later phase, the write
must be guarded by the same durable page-attempt validation that completes or
adopts the page output. If the phase cannot make that write-and-complete path
atomic, it should write to an attempt namespace and adopt only after completion.
This keeps same-worker replacement owners, expired lease recovery, and racing
remote processes from leaking stale page output.

Concrete next slices:

1. Promote the DB-open configured runtime path to the canonical integration
   test harness. New ownership tests should configure roles through
   `OpenOptions.graph_metric_maintenance`, usually with
   `start_background_loop = false`, and tick through the runtime attached to the
   DB handle. Direct `GraphMetricRuntime.init` tests should remain only for
   narrow runtime-unit behavior.
2. Use `antfly graph-metric-maintenance` as the first real process harness for
   one coordinator owner and two worker owners against the same DB path. The
   command passes only role, owner identity, worker identity set, tick budget,
   idle policy, poll interval, and DB path; metric config stays in the graph
   index. The first target is degree because it proves remote ownership without
   iterative math.
3. Add crash/restart tests around the process harness before broadening the
   metric families: worker killed while holding a page, replacement after
   runtime lease expiry, stale old worker trying to complete after reclaim,
   duplicate coordinator startup, and coordinator restart after workers finish
   pages but before publish.
4. Move PageRank onto the same remote harness after degree passes. The PageRank
   target is one dynamic later-iteration build where scan, initialize,
   contribution, reduce, convergence, publish, and cleanup can all survive at
   least one reopened owner boundary.
5. Extend the same harness to eigenvector once PageRank is stable. Eigenvector
   should not add new ownership mechanics; it should only prove the
   single-vector iterative executor can reuse the PageRank failure matrix.
6. Extend HITS last. HITS remote promotion requires explicit authority and hub
   contribution/reduce phase coverage, paired compatible publish, paired
   failure preservation, and cleanup restart evidence before default execution
   is enabled.
7. Only after remote ownership and public-read coverage pass should the
   conservative `auto` gate widen for larger PageRank/eigenvector workloads,
   default HITS, incompatible HITS, or multi-metric indexes.

This sequence intentionally leaves local runners in place until the planned path
has release-quality evidence. Degree proves the non-iterative lifecycle,
PageRank proves dynamic iteration and convergence, eigenvector proves the
single-vector substrate is generic, and HITS proves paired-vector publish and
failure. New graph metrics should be added only after those boundaries are
stable enough that a metric contributes math and metadata, not a new job system.

#### Remaining Product Contract

The user-facing model should stay small even as the implementation becomes
distributed:

- Metrics are configured on the graph index by stable user-provided names.
- `published` reads use the latest complete generation and may report stale,
  building, or failed status alongside those scores.
- `fresh` reads require the current edge generation and fail closed with
  `MetricNotReady` before the first publish or `MetricStale` after newer graph
  writes.
- Fixed-iteration non-converged PageRank, eigenvector, and HITS publish by
  default with `converged: false`, completed iteration counts, and final delta
  metadata.
- Old generations and intermediate job state are cleaned aggressively in v1;
  future retention is a bounded admin/debug option, not query semantics.
- Retry policy, lease timing, worker IDs, page IDs, and attempt namespaces stay
  internal, surfaced only through status and diagnostic events.

#### Roadmap for the Remaining Work

Current baseline: graph metrics already live on the graph index, use named
metric configs, publish complete generations, report explicit freshness errors,
and can run through durable planned maintenance. Degree, PageRank, eigenvector,
and HITS all exercise the shared planned lifecycle. The new in-process graph
metric runtime gives the maintenance loop a start/stop/notify boundary, can
start automatically from DB open, receives derived-apply notifications, can
cycle internal worker IDs, exposes split coordinator-only and worker-only ticks,
and can run automatic ticks in explicit `combined`, `coordinator`, `worker`, or
`worker_pool` roles. Separate started coordinator and worker runtime loops now
publish a background degree generation without manual tick calls, and a started
coordinator plus worker-pool runtime can publish a partitioned degree build
through two configured worker IDs. Reopened-handle worker-pool coverage also
proves fresh coordinator, worker-pool, and reader handles can advance the same
partitioned degree build through durable state. That is the right staging point
for separating ownership, but it is not the final distributed execution model.

The remaining roadmap should finish the system in this order. This table is the
authoritative completion plan; the longer sections below expand the same stages
with current implementation status and test coverage.

| Stage | User/API contract | Internal implementation | Promotion gate |
| --- | --- | --- | --- |
| 0. Keep metrics index-owned | Users configure named metrics inside the graph index. They choose `published` or `fresh`, read scores, and inspect metric status. They never create PageRank jobs, leases, attempts, pages, or cleanup tasks. | Keep job records, manifests, leases, page ids, attempts, retries, and cleanup namespaces internal to the graph index. Status/events may expose summarized progress and errors, but not raw key ranges or operational tuning. | No new public job API is added. OpenAPI/docs describe named metric config, freshness, convergence, failures, cleanup defaults, and status only. |
| 1. Promote planned maintenance safely | Existing reads and writes behave the same. `runUntilIdle` and background schedulers may make bounded graph-metric progress, but unfinished planned work is resumable maintenance, not a user-visible failure. | Route scheduler-style work through one bounded planned-maintenance primitive. Keep the conservative `auto` gate: planned mode is allowed for already-active planned work and safe small cases, while broader larger-graph and multi-metric workloads remain gated until latency is proven. | Degree, PageRank, eigenvector, and paired HITS can drain through planned maintenance with tiny-budget resume coverage; default promotion waits for latency-safe budgets and query/read latency evidence. |
| 2. Turn the runtime into production orchestration | Users still see graph-index metric status, not worker controls. Operational status can show role, owner, phase, page progress, cursor presence, and last error summaries. | Replace same-process runtime ownership with real coordinator and worker processes. Coordinators start/resume jobs, advance barriers, append dynamic iteration pages, publish, fail, and schedule cleanup. Workers only claim pages, renew leases, persist cursors, write page output, complete/fail pages, and stop. | Independent worker processes complete one generation through durable graph-index state only; killing a worker abandons only its lease; another worker reclaims after expiry; duplicate coordinators cannot double-publish or append incompatible phases. |
| 3. Close the restart/failure matrix | `published` keeps returning the latest complete generation during queued, building, or failed rebuilds. `fresh` fails closed with `MetricNotReady` before first publish and `MetricStale` after newer graph writes. | Treat PageRank as the baseline matrix, then bring eigenvector and HITS to the same standard across prepare, scan, initialize, contribution, reduce, convergence, publish, and cleanup. Cover reopen, lease expiry, reclaimed output, failed pages, exhausted attempts, publish failure, cleanup resume, and prior-generation preservation. | Failed or abandoned builds never hide the latest published generation or compatible HITS pair. Every durable-output phase either resumes from cursor, safely recomputes, or adopts attempt-scoped output only after page completion. |
| 4. Harden HITS paired-vector partitioning | Authority and hub remain separate named metric scores, but they publish, fail, and report compatibility as one pair for a target generation. There is no separate HITS job API. | HITS now has explicit retryable phases for authority contribution, authority reduce/norm, hub contribution, hub reduce/norm, paired convergence, paired publish, and paired cleanup. Lifecycle-gated local coverage proves reclaim, exhausted attempts, publish failure, cleanup resume, larger-manifest restart across reopen boundaries, paired failure preservation, paired publish idempotence, and local/planned parity. The remaining work is production remote-worker evidence, promotion-scale fan-in, operations evidence, and latency data before large remote HITS builds are enabled. | Authority and hub pages are independently retryable, but publish and failure stay atomic for the compatible pair. Either side failing preserves the previous pair. |
| 5. Harden every public read gate | Building output is invisible through direct metric top-k, traversal projection/order/filter, search rerank, explain/profile output, and cross-shard fan-in. | Resolve all score-bearing query paths through published generation pointers and compatibility checks. Failed attempt output, abandoned generations, and active job namespaces remain unqueryable. Cross-shard merge validates compatible nonzero published generations before combining scores. Unit-test lifecycle coverage now also runs the fast-root query/profile/fan-in checks for direct metric top-k, traversal status/order/filter, rerank details, failed status preservation, paired HITS failed-status preservation, malformed shard payload rejection, status-generation comparability, and profile generation reporting. Unit-test fan-in coverage combines those fast merge/profile checks with hosted cross-range graph metric fan-in coverage for compatible published shard merges, unpublished and incompatible shard rejection, a nonuniform eight-shard hosted degree/PageRank/eigenvector direct merge layout with four active/stale shards that keep unpublished high-score targets invisible and make `fresh` fail closed, active-stale hosted degree/PageRank/eigenvector traversal projection/order/filter and search rerank published merge plus fresh rejection, compatible HITS authority/hub hosted merge over an eight-shard layout with four active/stale shards and prior-pair preservation across direct, traversal projection/order/filter, and authority rerank surfaces plus fresh rejection, remote HITS generation/metadata/edge-filter mismatch rejection, and missing remote HITS status rejection. | Public e2e coverage proves `published`, `fresh`, `MetricNotReady`, and `MetricStale` behavior for direct reads, graph traversal/search integration, status, and distributed fan-in. Full promotion-scale shard layouts stay a separate release gate. |
| 6. Finish cleanup and operational defaults | V1 keeps aggressive cleanup semantics: latest published generation is retained; completed, failed, abandoned, and unpublished build state is removed when snapshot-safe. Future retention is a bounded admin/debug option, not query behavior. | Make cleanup a resumable phase for old score generations, completed job namespaces, abandoned attempts, manifests, pages, summaries, and bounded failed diagnostics. Keep lease timing, backoff, max attempts, pause/resume, priority, and manual retry as internal/admin concerns until production behavior is stable. | Cleanup resumes after restart, storage does not grow without bound, diagnostics remain bounded, and no retention knob is required for correctness. |
| 7. Promote by metric family | Public metric behavior stays stable while the executor changes behind internal gates. | Keep local runners as deterministic CI/debug oracles. Promote degree and PageRank first, eigenvector after single-vector restart/failure parity, and HITS last after paired partitioning and paired failure coverage. | Each family has local-vs-planned parity, restart, cleanup, freshness, cross-shard, operations, and remote-worker coverage before planned/distributed execution becomes the default. |

The intended implementation shape is:

```text
graph index metric config
  -> dirty marker and target edge generation
  -> coordinator-owned durable build job
  -> deterministic manifest pages
  -> worker-owned page leases and cursors
  -> attempt-scoped intermediate output where needed
  -> coordinator-owned phase and iteration barriers
  -> verified publish of one complete generation
  -> published generation pointer
  -> resumable cleanup
```

Operationally, the next big design transition is from "one DB process can tick
coordinator and worker work, optionally in explicit runtime roles" to "separate
process owners can safely tick those roles." The durable state model should
remain the communication boundary. Workers should not receive metric configs
from callers, should not mutate public generation pointers, and should not
decide that a build has failed. They should only execute bounded page work for a
metric name and persist enough progress for another worker or coordinator to
continue.

#### Current Completion Plan

The remaining work should be planned from the current implementation state, not
from the original PageRank-only design. The graph index already has durable
jobs, manifests, pages, page attempts, planned maintenance, split
worker/coordinator calls, role-configured in-process runtimes, conservative
`auto` scheduling, local-vs-planned parity for the first metric families, and
first coverage for duplicate coordinators, same-worker replacement, reclaimed
attempts, and public stale-read behavior. What remains is production
distribution and promotion.

The next roadmap should therefore optimize for these outcomes:

1. Convert in-process runtime roles into real remote owners.

   The current `combined`, `coordinator`, `worker`, and `worker_pool` runtime
   roles are the compatibility harness. The production version should preserve
   the same responsibilities, but the owners should be independently running
   processes that communicate only through durable graph-index state.

   Design requirements:

   - coordinators start or resume jobs, advance barriers, append dynamic
     iteration pages, publish, fail active builds, and schedule cleanup
   - workers claim page leases, renew, persist cursors, write attempt-fenced
     output, complete or fail pages, and stop
   - workers resolve work by index name and metric name, never by caller-passed
     metric config
   - page leases, runtime owner leases, and active build leases remain separate
     durability concerns
   - duplicate coordinators may observe completed work, but must not duplicate
     publish events or append incompatible pages

   Exit gate: a coordinator process and two worker processes can finish one
   degree or PageRank generation using only durable state; killing one worker
   abandons only its leased page; another worker reclaims after expiry; public
   reads continue to see either the old published generation or the verified new
   generation.

2. Promote planned maintenance with bounded fairness.

   The default idle path now uses the bounded `auto` gate for scheduler-style
   graph metric work. Keep local runners as oracles and fallback for
   incompatible HITS and explicit caps, but do not split the executor into
   one-off graph-metric release targets.

   Promotion sequence:

   - already-active planned builds
   - queued degree
   - queued PageRank
   - queued eigenvector
   - compatible HITS authority/hub pairs counted as one lifecycle
   - multi-metric indexes under global and per-index active-build caps
   - explicit iteration caps and incompatible HITS remain local fallback
   - broader production latency evidence before removing fallback/oracle usage

   Exit gate: `runUntilIdle` and background scheduler ticks return bounded,
   resumable progress for every promoted class; unsupported classes still fall
   back to local maintenance or remain explicitly gated without changing query
   semantics.

3. Finish the distributed failure matrix.

   PageRank remains the reference because it exercises scan, initialize,
   dynamic contribution/reduce/check iterations, convergence, fixed-iteration
   non-converged publish, verified publish, stale reads, and cleanup.
   Eigenvector should match the same single-vector matrix. HITS should match it
   for paired authority/hub phases.

   Required failure cases:

   - reopen during every phase and at a later dynamic iteration
   - expired page lease reclaimed by another worker
   - same-worker replacement after runtime lease expiry
   - stale prior attempt tries to write progress, output, completion, or failure
   - failed page retry and exhausted page attempts
   - publish verifier failure after durable score output exists
   - crash after publish before cleanup
   - cleanup cursor resume after restart
   - failed rebuild preserves the prior generation or compatible HITS pair

   Exit gate: every output-writing phase either resumes from a durable cursor,
   recomputes safely from the page range, or writes to an attempt namespace that
   is adopted only after attempt-validated page completion.

4. Keep attempt adoption targeted.

   Deterministic recompute should remain the default because it is easier to
   reason about and cheaper to operate for small phases. Attempt namespaces are
   an executor capability for phases whose partial output can be consumed by a
   later phase, whose output cannot be cheaply replaced atomically, or whose
   production-scale recompute cost is too high.

   Current policy:

   - contribution-like pages use attempt-scoped output and adoption
   - HITS hub raw contribution uses attempt-scoped output and adoption
   - scan/initialize/reduce/convergence writes validate the durable page
     attempt before writing job-visible output
   - future reduce-like phases should move to attempt adoption only when tests
     or workload data show stale-output or recompute risk

   Exit gate: later phases read only adopted output or attempt-validated durable
   output; abandoned attempts are invisible to readers and cleanup removes them.

5. Finish HITS as the paired-vector proof.

   HITS should remain two user-visible named metrics, authority and hub, but one
   compatible pair for target generation, convergence, publish, failure, and
   freshness. It should not introduce a separate HITS job API.

   Remaining HITS work:

   - broaden restart/reclaim/failure/cleanup coverage across explicit authority
     contribution, authority reduce/norm, hub contribution, hub reduce/norm,
     paired convergence, paired publish, and paired cleanup
   - prove remote workers can independently retry authority and hub pages while
     the coordinator keeps paired publish/failure atomic
   - keep active authority/hub job output invisible to direct top-k and graph
     reads until paired publish
   - preserve the prior compatible pair after either side fails

   Exit gate: remote HITS builds are enabled only after paired-vector
   partitioning, stale-read coverage, failure preservation, cleanup resume, and
   local-vs-planned parity all match PageRank-quality evidence.

6. Gate promotion on public reads, not only executor tests.

   The planned runner is not production-ready until every score-bearing query
   path proves the same generation contract. Building output, abandoned
   attempts, and failed unpublished generations must stay unqueryable.

   Required public gates:

   - direct graph metric top-k
   - traversal projection, ordering, and filtering
   - search rerank
   - explain/profile/status output
   - cross-shard direct metric fan-in
   - cross-shard rerank or score-bearing merge

   Exit gate: `published` reads use the latest complete generation during
   queued, building, cleanup, and failed rebuilds; `fresh` fails closed with
   `MetricNotReady` before first publish and `MetricStale` after newer graph
   writes; cross-shard paths prove compatible nonzero published generations or
   fail closed.

7. Make cleanup and diagnostics production invariants.

   V1 should keep aggressive cleanup: retain the latest published generation,
   delete completed job namespaces, delete unpublished score generations,
   delete abandoned attempts, and retain only bounded failed-job diagnostics.
   Retention, manual retry, priority, and extended history can be future
   bounded admin/debug features.

   Exit gate: cleanup resumes after restart, never deletes the current
   published generation, and prevents completed, failed, abandoned, and
   unpublished job state from growing without bound.

8. Promote by metric family.

   Keep local runners as deterministic CI/debug oracles until each family has
   enough planned/distributed release evidence.

   Promotion order:

   1. degree, because it proves the non-iterative lifecycle
   2. PageRank, because it proves dynamic iteration and convergence
   3. eigenvector, after single-vector restart/failure parity with PageRank
   4. HITS, after paired-vector partitioning and paired failure coverage

   Exit gate: each promoted family has local-vs-planned parity, remote-worker
   coverage, restart/failure coverage, cleanup coverage, public freshness
   coverage, cross-shard coverage, operations documentation, and generated
   client fields for status/freshness/convergence/failure metadata.

#### Completion Roadmap From Current State

The current checkpoint has enough durable machinery to treat the remaining work
as product hardening and distributed execution, not as a new metric feature.
The roadmap below is the intended path from the in-process planned runtime to a
production graph-metric subsystem.

User-facing design stays stable through every milestone:

- metrics live on the graph index as named metric configs
- users choose `published` or `fresh` freshness at read time
- `MetricNotReady` means no complete generation has ever published
- `MetricStale` means a `fresh` read cannot use the latest complete generation
- fixed-iteration non-converged PageRank/eigenvector/HITS output publishes by
  default with `converged: false`
- jobs, pages, attempts, leases, worker ids, retry policy, and cleanup queues
  stay internal, with only summarized status and diagnostics exposed

Implementation design should move one ownership boundary at a time:

| Milestone | User/API surface | Implementation work | Required proof |
| --- | --- | --- | --- |
| M1. Scheduler default readiness | No public API change. Background maintenance may take longer than local execution for some workloads, but visible reads keep the same published/fresh contract. | Promote the bounded planned-maintenance primitive behind latency-safe scheduler budgets. The `auto` gate starts degree, PageRank, eigenvector, compatible HITS, and multi-metric work within global/per-index caps; explicit caps and incompatible HITS stay on the local fallback path. | `runUntilIdle` and background ticks drain degree, PageRank, eigenvector, compatible HITS, and multi-metric indexes; tiny budgets return resumable `budget_exhausted`; deferred queued work starts when capacity returns. |
| M2. Remote ownership boundary | Users still see graph-index metric status, not job submission or worker controls. Operational status can show role, owner hash, phase, iteration, page counts, cursor presence, attempt, and last error. | Replace same-process runtime roles with independently owned coordinator and worker processes. Coordinators start builds, advance barriers, append iteration pages, publish, fail, and schedule cleanup. Workers claim page leases, renew, persist cursors, write deterministic output, complete/fail pages, and stop. | Killing a worker abandons only its page lease; another worker reclaims after expiry; duplicate coordinators cannot publish twice or append incompatible phase pages; no worker needs caller-supplied metric config. |
| M3. Failure and restart matrix | `published` reads keep returning the prior generation during queued, building, and failed rebuilds. `fresh` fails closed until the current edge generation has published. | Finish restart, lease-expiry, reclaimed-output, exhausted-attempt, publish-failure, cleanup-resume, and prior-generation-preservation tests across PageRank first, then eigenvector, then HITS. | Every output-writing phase either resumes from cursor, recomputes safely, or adopts attempt-scoped output only after page completion. Failed builds preserve the previous generation or compatible HITS pair. |
| M4. Attempt-scoped output policy | No user configuration. Attempt storage is an internal correctness and cost-control mechanism. | Keep deterministic recompute as the default. Use attempt namespaces only where partial output can be consumed by later phases, cannot be atomically replaced cheaply, or is too expensive to recompute at production scale. | Later phases read only adopted output; abandoned attempts are invisible to readers; cleanup removes adopted and abandoned attempt namespaces without growing storage unboundedly. |
| M5. HITS paired-vector hardening | Authority and hub remain separate named metrics, but compatible configs publish, fail, and report freshness as one pair. | HITS now has explicit retryable authority contribution, authority reduce/norm, hub contribution, hub reduce/norm, paired convergence, paired publish, and paired cleanup phases. Direct process coverage now also exhausts a killed hub-reduce page attempt sequence, fails the compatible pair once, and proves a duplicate coordinator cannot fail it again. Lifecycle-gated local coverage now covers active prior-pair visibility, paired publish idempotence, initialize/contribution/reduce/hub/convergence reclaim, cleanup resume after reopen, larger-manifest restart across reopen boundaries, failed-build preservation, publish-failure preservation, local/planned parity, and failed public-read preservation. Finish production remote-worker evidence, promotion-scale fan-in, operations evidence, and latency data before remote HITS is enabled by default. | Authority and hub pages can retry independently, while publish/failure remains atomic for the compatible pair and failed work preserves the previous pair. |
| M6. Public read hardening | Direct top-k, traversal projection/order/filter, search rerank, explain/profile, and cross-shard fan-in all expose the same generation/freshness semantics. | Resolve every score-bearing path through published generation pointers, score metadata, and compatibility checks. Keep active job namespaces, abandoned attempts, and unpublished generations unqueryable. Unit-test lifecycle coverage now includes fast-root query/profile/fan-in checks across direct metric, graph search, and rerank score surfaces, and unit-test fan-in coverage pairs them with hosted cross-range graph metric fan-in coverage. | Public e2e coverage proves `published`, `fresh`, `MetricNotReady`, `MetricStale`, building, failed, and cross-shard incompatible-generation behavior. Promotion-scale shard layouts remain separately qualified. |
| M7. Cleanup and operations | V1 keeps aggressive cleanup. Future retention, pause/resume, manual retry, and priority controls are bounded admin/debug features, not query semantics. | Make cleanup a resumable phase for old score generations, completed jobs, failed/abandoned jobs, attempts, manifests, pages, summaries, and bounded diagnostics. Keep retry, lease, and backoff defaults internal until operational behavior is stable. | Cleanup resumes after restart, never deletes the current published generation, and prevents unbounded growth without requiring a user-facing retention knob. |
| M8. Per-family promotion | Public metric behavior stays stable while the executor changes behind internal gates. | Promote degree and PageRank first, eigenvector after single-vector restart/failure parity, and HITS last after paired partitioning plus paired failure coverage. Keep local runners as CI/debug oracles until each family has release history. | Each promoted family has local-vs-planned parity, remote-worker, restart, cleanup, freshness, cross-shard, and operations coverage. |

The implementation layers should stay separated:

- **Graph index:** owns metric config resolution, dirty state, published
  generation pointers, metric status, score metadata, and cleanup eligibility.
- **Coordinator:** owns job creation, manifest validation, phase and iteration
  barriers, dynamic page planning, verified publish, failure, and cleanup
  scheduling.
- **Worker:** owns only one leased page at a time: claim, renew, cursor,
  deterministic output, complete/fail, and stop.
- **Runtime/scheduler:** owns when to tick coordinators and workers, budget
  limits, role/owner identity, and telemetry. It does not define metric
  semantics.
- **Query layer:** owns freshness checks and published-generation resolution.
  It must never inspect building output directly.

The practical promotion order is:

1. keep the in-process runtime as the compatibility harness
2. make planned maintenance latency-safe for default scheduler use
3. introduce real coordinator and worker process ownership for degree/PageRank
4. finish the PageRank failure matrix as the reference distributed matrix
5. bring eigenvector to the same single-vector matrix
6. finish phase-specific HITS restart, reclaim, failure, and cleanup coverage
   for the explicit authority and hub phases
7. harden public read paths and cross-shard fan-in against building, stale,
   failed, and incompatible generations
8. promote per family behind internal gates, with local runners retained as
   oracles until planned/distributed execution is mature

#### Rest-of-Work Roadmap

The remaining work should be delivered as an execution-system roadmap, not as a
set of separate PageRank, degree, eigenvector, and HITS projects. PageRank
remains the reference metric because it exercises dynamic iteration, convergence,
fixed-iteration publish, stale-read behavior, and cleanup. Degree remains the
cheap distributed-runner proof. Eigenvector proves the single-vector iterative
substrate is generic, and HITS proves paired-vector publish and failure.

The order should be:

| Step | Workstream | Design decision | Done when |
| --- | --- | --- | --- |
| 1 | Default planned maintenance readiness | Keep the public API unchanged and move scheduler-style background graph metric work through one bounded planned-maintenance primitive. `runUntilIdle` has an explicit planned mode and an internal `auto` gate that selects planned maintenance for queued degree, PageRank, eigenvector, compatible HITS authority/hub pairs, multi-metric graph indexes, and already-active planned builds under global and per-index active-build caps. Compatible HITS counts as one paired lifecycle; incompatible HITS pairs and explicit operator caps stay outside planned startup and fall back to local maintenance. | Budget exhaustion is resumable, pending-work stats expose queued/active/deferred graph metric work, and PageRank/degree/eigenvector/HITS complete through scheduler ticks without partial visibility or unbounded latency. |
| 2 | Production worker orchestration | Workers are lease executors only: they claim pages, persist cursors, write output, complete/fail pages, and stop. Coordinators own job start, phase barriers, dynamic iteration planning, publish, failure, and cleanup. The first internal DB runtime now gives planned graph-metric maintenance a start/stop/notify worker boundary, can cycle multiple internal worker IDs in one bounded round, exposes split coordinator-only and worker-only ticks, can run automatic ticks as `combined`, `coordinator`, `worker`, or `worker_pool`, has started coordinator plus worker-pool coverage that publishes a partitioned degree build through durable state, and now has a real process launcher proof for direct file-backed DB paths. The direct DB launcher serializes storage writes with an explicit local writer guard; production remote orchestration should replace that local storage boundary with the deployment/service boundary while preserving the same graph-index job/page ownership model. | A real worker process can be killed, restarted, or raced without duplicate publish; another worker can reclaim expired pages; status reports owner, attempt, cursor, phase, iteration, progress, and last error from durable records. |
| 3 | Restart and failure matrix | Treat PageRank, eigenvector, and HITS as one matrix over scan, initialize, contribution, reduce, convergence, publish, and cleanup. PageRank is the baseline; eigenvector and HITS must catch up before promotion. | Reopen/retry/reclaim tests cover every phase boundary and at least one later iterative boundary; failed builds preserve the prior published generation or compatible HITS pair. |
| 4 | Attempt-scoped output adoption | Deterministic recompute stays the default. Use attempt-scoped output only where partial output can be consumed early, where replacement is not atomic enough, or where recompute cost is too high. | Later phases read only adopted output; abandoned attempts are invisible and cleanup-owned; reduce-like phases adopt attempts only where there is a demonstrated need. |
| 5 | HITS paired-vector hardening | HITS already has explicit authority contribution, authority reduce/norm, hub contribution, hub reduce/norm, and paired convergence phases. Before enabling remote HITS builds by default, finish restart/reclaim/failure/cleanup coverage for those phases under paired publish semantics. | Authority and hub remain separate named metrics but share one compatible target generation, one convergence decision, one failure decision, and one atomic publish. |
| 6 | Public freshness and fan-in gates | Promotion is blocked until every public read path proves the same generation contract as direct metric top-k. | Direct top-k, traversal projection/order/filter, search rerank, explain/profile output, and cross-shard fan-in use only complete published generations or fail closed with `MetricNotReady`/`MetricStale`. |
| 7 | Cleanup and operations hardening | V1 cleanup remains aggressive. Completed job namespaces, abandoned attempts, unpublished score generations, pages, manifests, and summaries are removed as soon as snapshot safety allows. | Cleanup can resume after restart, failed diagnostics stay bounded, storage does not grow without bound, and retention remains a future bounded admin/debug option. |
| 8 | Per-family promotion | Promote planned execution by metric family behind internal gates, keeping local runners as CI/debug oracles until the planned path has release history. | Degree and PageRank promote first, eigenvector follows after restart/failure parity, and HITS promotes last after paired partitioning and paired failure coverage. |

The remaining implementation should land as roadmap milestones with evidence
profiles on the existing test harnesses, not as standalone graph-metric Make
targets or independent release gates:

1. Scheduler-ready planned maintenance.

   Keep the existing public metric API unchanged and make the bounded planned
   maintenance primitive the internal scheduler entrypoint. This slice owns
   budget semantics, pending-work hints, runtime stats, and `runUntilIdle`
   integration. The planned path can be selected explicitly and by the bounded
   `auto` gate. Default CI should run the promotion-shaped scheduler coverage
   through the existing full-default/unit integration profiles, while larger
   latency and deployment evidence stay in named harness profiles.

   Gate: scheduler ticks can drain degree, PageRank, eigenvector, and paired
   HITS through the shared primitive; tiny budgets return a resumable
   `budget_exhausted` result; queued and active graph-metric work is visible in
   DB stats/runtime status; old local whole-metric maintenance remains an
   oracle and fallback.

2. Production coordinator and worker processes.

   Use the existing metric-name worker/coordinator boundary as the contract for
   real process owners. A coordinator process starts or resumes builds, advances
   phase barriers, appends later-iteration pages, publishes, fails active
   builds, and schedules cleanup. Worker processes claim durable pages, renew
   leases, persist cursors, write output, complete or fail the page, and stop.
   Workers never receive metric configs from callers and never mutate the
   published-generation pointer.

   Gate: independently owned coordinator and worker DB handles can complete one
   generation using only durable graph-index job/page state; killing a worker
   abandons only its page lease; another worker reclaims it after expiry;
   duplicate or racing coordinators cannot double-publish or append
   incompatible phases.

3. Distributed failure matrix.

   PageRank is the reference matrix because it exercises dynamic iteration,
   convergence, fixed-iteration publish, stale reads, and cleanup. Bring
   eigenvector and HITS to the same matrix instead of adding metric-specific
   exceptions. Each metric family needs coverage for reopen, expired leases,
   reclaimed output, failed pages, exhausted attempts, publish failure,
   cleanup-resume, and prior-generation preservation.

   Gate: every phase that writes durable output either resumes from a cursor,
   safely recomputes from its page range, or writes through an attempt namespace
   that is adopted only on page completion. Failed or abandoned builds preserve
   the latest published score generation or the latest compatible HITS pair.

4. Attempt-scoped adoption policy.

   Keep deterministic overwrite/recompute as the default. Attempt namespaces
   are an executor capability for phases whose partial output can be consumed
   by later phases, whose output cannot be atomically replaced cheaply, or whose
   recompute cost is too high for production. Attempt adoption should remain
   internal job machinery, not user configuration.

   Gate: later phases read only adopted job-scoped output; abandoned attempts
   are invisible to readers and removed by cleanup; reduce-like phases adopt
   attempt output only when tests demonstrate stale partial output or excessive
   recompute risk.

5. HITS paired-vector hardening.

   Keep HITS as the paired-vector proof of the generic runner. Hidden global
   hub work has been split into explicit retryable phases: authority
   contribution, authority reduce/norm, hub contribution, hub reduce/norm,
   paired convergence, paired publish, and paired cleanup. Authority and hub
   remain separate named metrics, but a compatible pair has one target
   generation, one convergence decision, one failure decision, and one atomic
   publish decision.

   Gate: authority and hub output can be rebuilt by remote workers without
   hidden global materialization, either side failing preserves the previous
   compatible pair, and paired cleanup resumes after restart without hiding
   published scores.

6. Public read and fan-in hardening.

   Treat query behavior as part of the distributed execution design. Direct
   top-k, traversal projection, traversal ordering, traversal filtering, search
   rerank, explain/profile output, and cross-shard fan-in should all resolve
   graph metric scores only through complete published generations. Building
   output, failed attempt output, and abandoned generations stay invisible.

   Gate: `published` reads work against the last complete generation even while
   a newer build is queued, building, or failed; `fresh` fails with
   `MetricNotReady` before first publish and `MetricStale` after newer graph
   writes; cross-shard merge refuses incompatible or zero published
   generations.

7. Cleanup, diagnostics, and operational defaults.

   V1 cleanup stays aggressive. Keep the latest published generation, delete
   completed job namespaces when snapshot safety allows, remove unpublished
   score generations, remove abandoned attempt namespaces, and retain only
   bounded diagnostics for failed jobs. Pause, resume, priority scheduling,
   manual retry, and retention are future admin/debug controls, not v1 query
   semantics.

   Gate: cleanup can resume from durable cursors after restart, completed and
   failed builds do not grow storage without bound, failed diagnostics remain
   bounded, and no public retention knob is required for correctness.

8. Metric-family promotion.

   Promote the planned/distributed runner by metric family behind internal
   gates. Degree and PageRank should promote first because they prove the
   non-iterative and iterative baselines. Eigenvector follows after restart and
   failure parity with PageRank. HITS promotes last after paired partitioning is
   complete.

   Gate: CI compares local and planned output on deterministic graphs, restart
   and cleanup coverage exists for each family, public freshness tests cover
   building/stale/failed states, cross-shard score-bearing reads prove
   generation compatibility, and generated clients document status,
   convergence, freshness, and failure fields.

The system is not complete until large graph metric rebuilds can run through
remote workers, survive worker death and database reopen, publish exactly one
verified generation, keep previous generations visible on failure, clean their
temporary state, and expose only the stable graph-index metric API to users.

#### Remaining Architecture Roadmap

| Lane | Target design | What remains |
| --- | --- | --- |
| Scheduler entrypoint | One bounded graph-metric maintenance primitive starts queued builds, ticks workers, ticks coordinators, reports progress, returns budget exhaustion as a resumable result, contributes queued/active/deferred graph-metric hints to pending-work stats, and is now the default `runUntilIdle` path through the bounded `auto` gate for scheduler-style workloads. Explicit planned mode remains available for tests/operators, and incompatible HITS or explicitly capped cases still fall back to local maintenance. Worker-only sweeps now also report budget exhaustion when they consume their page-step budget and the durable build remains active, so split runtimes can expose resumable work without relying on the combined maintenance loop. Worker-pool rounds spend page budget on actual worker steps rather than idle worker IDs. The auto gate now has coverage for default queued degree, PageRank, eigenvector, compatible HITS, multi-metric graph indexes, already-active planned degree/PageRank/eigenvector/HITS builds, explicit iteration caps, incompatible HITS fallback, and per-index deferred queued work. Compatible HITS reports one eligible queued pair and one active lifecycle before planned execution. Controlled-cap tests prove the scheduler starts work only within global/per-index budgets, defers extra queued work, exhausts tiny budgets as resumable active work, and finishes after budget expansion. | Broaden deployment latency evidence and keep default promotion coverage in existing full-default/unit profiles instead of adding graph-metric-specific Make targets. |
| Remote workers | Workers execute page leases only. They do not pass metric configs, create jobs, advance phases, publish, fail builds, or clean completed jobs outside cleanup pages. Worker-page results now carry an explicit `completed_build` terminal signal so drain loops do not have to overload publish telemetry to detect final cleanup. An internal DB maintenance runtime now repeatedly ticks the same planned worker/coordinator sweeps under bounded budgets, starts automatically from DB open when enabled, wakes from derived-apply notifications, can cycle multiple internal worker IDs across page sweeps, exposes split coordinator/worker tick entrypoints, supports role-configured automatic loops for combined/coordinator/worker/worker-pool ownership, proves separate started coordinator-role and worker-role loops can publish a background degree generation without manual tick calls, proves a started coordinator-role plus worker-pool-role pair can publish a partitioned degree build through two configured worker IDs, keeps job/page semantics in durable index state, passes runtime clock time into page-lease claim/reclaim and exhausted-page decisions, can opt into role-scoped durable runtime-owner leases with acquisition/failure/loss/takeover telemetry, and records internal role/owner/tick/progress/error telemetry plus cumulative planned-sweep totals surfaced through DB stats/runtime-status snapshots for operations. Reopened-handle PageRank coverage now proves lease-owned runtime split ticks can be driven by separate fresh coordinator, worker, and reader DB handles, and reopened-handle degree coverage now proves a lease-owned worker-pool runtime role can do the same with two configured worker IDs. Runtime lease coverage now proves one owner blocks a duplicate same-role owner until lease expiry, a later same-role owner can take over through the durable lease, coordinator/worker owners can hold independent role leases for the same build, distinct lease-owned worker runtimes can complete separate active pages under different worker identities, live duplicate worker owners for the same worker identity or worker-pool identity set are fenced before page execution during active builds, and a replacement owner for the same worker identity can take over after lease expiry and continue active page work while the former owner observes lease loss. Worker-pool runtime identity now has a focused order-independence test proving reordered worker-id sets produce the same runtime owner hash/lease key while different sets do not. Duplicate worker IDs in one command-level worker-pool config are rejected before runtime launch, and coordinator/worker command roles plus enabled runtime config validation reject worker-id lists, matching the runtime/scheduler validation boundary and preventing one configured pool from silently running the same worker identity twice. Page execution now fences progress, completion, and failure by both worker identity and durable page attempt, so a stale process from the prior attempt cannot finish work after same-worker replacement takeover. The process command now has both a sequential `supervise` proof and a bounded `launch` proof that starts independently owned coordinator and worker-pool child processes, captures child summaries, uses a local DB writer guard only for direct file-backed launches, emits command-summary ownership telemetry that pins role, owner, worker, lease, progress, and error counters for operators, carries compact per-child telemetry into the aggregate supervisor/launcher summary, and the spawned-process harness now verifies the same role/owner/worker/lease-key/tick/error telemetry on every standalone coordinator, worker, and worker-pool role process used in restart and reclaim proofs. | Add production remote deployment orchestration and tests where independently owned workers communicate only through durable graph-index job/page state or a service boundary, without relying on direct local file-writer serialization. |
| Coordinator | The coordinator is the only owner of build startup, phase barriers, dynamic iteration planning, publish, active-build failure, and cleanup scheduling. Degree, PageRank, eigenvector, and paired HITS now have duplicate coordinator tick coverage around publish/cleanup proving repeated coordinator ticks do not append duplicate publish events or advance incompatible state. PageRank, eigenvector, and paired HITS also have reopened-coordinator publish-race coverage: one fresh coordinator handle publishes, a second fresh coordinator handle observes cleanup or completed paired state, and status still has one publish event for one visible generation or compatible authority/hub pair. The same no-duplicate-publish proof now runs through the DB/index-manager scheduler boundary and DB graph-metric runtime split-coordinator boundary with lease-owned coordinator roles for PageRank, eigenvector, and paired HITS. Scheduler publish telemetry is now coordinator-owned too: coordinator sweeps count publish when they advance `publish_generation`, while worker sweeps keep cleanup completion out of the publish counter. Those runtime tests now cover both live duplicate-owner fencing while the publishing coordinator still holds the runtime lease, and later post-release idempotence when another coordinator owner observes cleanup/completed state. The spawned-process harness now carries the same publish-boundary invariant through real coordinator role processes for degree, PageRank, eigenvector, and compatible HITS: a second coordinator after publish must not publish, fail, advance phase, or append another publish event. It also carries the failed-publish sibling invariant for PageRank, eigenvector, and compatible HITS: after a real coordinator records a publish-verifier failure, a second coordinator must not publish, fail again, advance phase, or append another failure event. | Harden duplicate/racing coordinator coverage under true remote scheduling and carry the invariant into deployment orchestration. |
| Page output | Cheap deterministic phases can recompute on reclaim; expensive or high-risk phases use attempt-scoped output that is adopted only after page completion. Iterative scan partial writes now validate the durable page attempt before writing job-visible out-degree and node partials, and PageRank/eigenvector/HITS initialize validate the durable page attempt before rewriting iteration-0 rank output. PageRank initialize also rewrites aggregate out-degree through the same validation path. PageRank partial convergence summaries now update cursor, unit progress, `max_delta`, `total_delta`, and `rank_sum` in one attempt-validated write path, so stale reclaimed check pages cannot revive abandoned convergence metadata. Degree reduce validates the durable page attempt in the same write path that materializes public score-generation rows and completes the reduce page, so a stale reclaimed attempt cannot write visible degree scores before completion is rejected. PageRank, eigenvector, and HITS authority/hub reduce now apply the same attempt-validated write path to next-iteration rank output consumed by convergence. HITS hub reduce also validates the durable attempt before writing or reusing hub raw summary state. | Extend attempt-scoped adoption to future reduce-like phases only where recompute cost or atomic replacement risk justifies it. |
| HITS paired vectors | Authority and hub share one compatible target generation, paired convergence metadata, one failure decision, and one atomic publish decision. Hub contribution and hub reduce/norm are explicit retryable phases with attempt-scoped hub raw contribution output. | Broaden phase-specific restart, reclaim, failed-page, publish-failure, cleanup, and remote-worker coverage before enabling remote HITS builds by default. |
| Public reads | Direct top-k, traversal, ordering, filtering, search rerank, status, and cross-shard fan-in read only complete published generations or fail closed. | Add public e2e coverage for building, stale, failed, and cross-shard distributed cases before promotion. |
| Cleanup | Cleanup is a resumable phase that removes old score generations, abandoned building output, attempt namespaces, manifests, pages, summaries, and bounded failed diagnostics without hiding the current published generation. | Broaden cleanup restart/reclaim tests under remote ownership and keep retention knobs out of v1. |

#### Remaining Delivery Plan

| Order | Milestone | Design | Ship gate |
| --- | --- | --- | --- |
| 1 | Default planned maintenance | Route background graph metric rebuilds through the bounded planned-maintenance primitive instead of the local whole-metric runner. The explicit `runUntilIdle` planned gate proves the path can publish a small background PageRank build through planned maintenance, tiny planned-idle budgets fail fast while preserving resumable active work, and the default bounded auto gate proves queued degree, PageRank, eigenvector, compatible HITS, multi-metric indexes, and already-active planned PageRank/degree/eigenvector/HITS can use planned maintenance. The auto gate exposes an internal decision summary for active, eligible queued, deferred queued, and ineligible queued work. Default and explicit auto tests assert that compatible HITS reports one eligible queued pair before planned execution, active paired HITS reports one active lifecycle, incompatible HITS and explicit iteration caps remain ineligible before fallback, and per-index caps defer extra queued work until the scheduler has capacity. The remaining work is broader deployment latency evidence and release-profile evidence, not another local execution path. The call must make bounded progress and return `budget_exhausted` instead of treating unfinished work as failure. | `runUntilIdle` and scheduler-style ticks finish PageRank, degree, eigenvector, and paired HITS without unbounded latency or partial visibility. |
| 2 | Remote worker ownership | Introduce production worker processes around the existing metric-name worker/coordinator boundary and the new role-configured runtime. Workers claim pages by index and metric name, persist cursors, complete/fail leases, and exit. Coordinators run as their own owners and never rely on worker-local state. | Killing or racing a worker reclaims only that page lease; active status shows owner, attempt, cursor, and error; coordinator remains the only publish authority. |
| 3 | Distributed failure matrix | Expand restart, expired-lease, reclaimed-output, publish-failure, and cleanup tests across PageRank, eigenvector, and HITS. PageRank remains the reference matrix; eigenvector and HITS must match it before promotion. | Failed or abandoned rebuilds preserve prior published scores or the prior compatible HITS pair across reopen. |
| 4 | HITS paired partitioning | Keep authority contribution, authority reduce/norm, hub contribution, hub reduce/norm, and paired convergence as explicit retryable phases. Finish the restart, reclaim, failure, cleanup, parity, and remote-worker matrix around those phases. | Authority and hub work are independently retryable pages, but publish and failure remain atomic for the compatible pair. |
| 5 | Public freshness and fan-in gates | Treat query freshness as part of the execution design. Promotion requires direct top-k, traversal projection/order/filter, search rerank, explain/profile status, and cross-shard fan-in coverage. | Building output is never visible through public reads; `fresh` fails closed; cross-shard score merges prove compatible nonzero published generations or fail closed. |
| 6 | Per-family promotion | Enable planned execution behind internal gates per metric family: degree and PageRank first, eigenvector next, HITS last after paired partitioning. Keep local runners as CI/debug oracles until planned output has release history. | CI has deterministic local-vs-planned parity, restart/failure/cleanup coverage, and public docs/clients describing status, freshness, convergence, and failures. |

Roadmap state:

| Area | Current state | Remaining work |
| --- | --- | --- |
| Job root | Durable per-metric build records exist, and the planned worker step now loads the active job before dispatching page work; planned degree, PageRank, eigenvector, and HITS now have active runners, planned PageRank has local alternating-worker coverage over partitioned pages, and planned HITS has local alternating-worker coverage over partitioned paired-vector pages. The planned build startup path now exposes a public `GraphIndex` ensure call that creates or returns the active durable job/manifest for one target generation, the worker path exposes public metric-name worker-only page and coordinator steps so remote runners do not pass metric configs around, and a public active-build failure call records coordinator-owned failure while requiring the active lease/job pair. The DB/index-manager layer now exposes the same named build ensure, worker page step, coordinator step, failure, and drain boundary by index name and metric name; DB-level coverage proves a planned degree generation can publish through those calls without worker-side config access, and the direct DB/index-manager worker/coordinator split-step boundary now also has injected-time `At` variants for deterministic lease/reclaim tests. The DB/index-manager layer also has bounded planned scheduler sweeps: coordinator sweeps start queued background builds and advance active barriers/publish steps, while worker sweeps claim active durable pages by worker id and now report resumable budget exhaustion when their page-step budget is consumed while the build remains active; DB coverage proves active planned degree, PageRank, eigenvector, and HITS builds can complete through these sweeps, with PageRank/eigenvector exercising iterative phase advancement, HITS preserving paired authority/hub publish, and cleanup completing from active durable jobs. Reopened DB-handle coverage now proves an iterative PageRank build can be started, worked, coordinated, published, cleaned up, and queried through fresh handles that communicate through persisted graph-index job/page state rather than in-memory state; PageRank, eigenvector, and paired HITS also have fresh-DB coordinator race coverage at `publish_generation` proving the DB/index-manager boundary records one publish event when a second coordinator handle ticks after publish. A reusable planned-drain primitive now composes those metric-name ensure, worker-page, and coordinator calls with named worker IDs, giving production scheduling a concrete internal loop without exposing metric configs to workers. A bounded planned-maintenance primitive now wraps background planned-build startup plus worker/coordinator sweeps for scheduler callers, DB coverage proves it can drain background PageRank, degree, eigenvector, and paired HITS generations without the local whole-metric runner, and budget exhaustion is now reported as a resumable result flag rather than an error. The first DB graph-metric runtime wraps those scheduler calls with start/stop/notify lifecycle, split tick entrypoints, role-configured automatic loops for combined/coordinator/worker/worker-pool ownership, and DB stats/runtime-status telemetry that includes role plus hashed runtime/worker ownership; runtime-level PageRank/eigenvector/HITS coverage now proves lease-owned split-coordinator runtime instances fence a live duplicate coordinator owner at publish and cannot duplicate publish after one runtime owner moves the job to cleanup or completed paired state. Cross-family public-boundary coverage now proves degree, PageRank, eigenvector, and HITS page workers do not advance phases or publish when driven through public build ensure plus metric-name worker/coordinator calls, and the same public failure boundary preserves the prior published degree, PageRank, eigenvector, or compatible HITS pair while retaining bounded diagnostics. Degree also has reopened-handle coverage proving independently opened coordinator/worker handles communicate through durable job and page state rather than in-memory state, tolerate duplicate coordinator barrier/publish ticks, and complete scan/reduce pages from concurrent reopened worker handles while the coordinator remains the only publish authority. PageRank, eigenvector, and HITS now have the same first concurrent reopened-worker proof across scan, initialize, contribution, reduce, and convergence pages, plus separate reopened-coordinator publish-race coverage proving a second coordinator owner cannot duplicate the publish event after another owner moved the job to cleanup or completed paired state. | Add true remote scheduling, production worker process orchestration, latency-safe default idle promotion to planned maintenance, broader concurrent/distributed failure coverage, and deepen HITS hub contribution/reduction phase partitioning. |
| Manifest | Durable manifests exist with versioned page range metadata; planned degree partitions reverse-edge scan work, reduce work, and cleanup work into deterministic pages, initial PageRank/eigenvector/HITS manifests partition scan, initialize, contribution, reduce, and convergence pages, non-final iterative convergence dynamically plans the next iteration's contribution/reduce/check pages, and dynamic page appends update manifest page counts idempotently. Planned HITS reduce now has multi-page coverage proving one claimed reduce page writes only authority/hub output for its planned node range, and HITS hub raw contribution state is now materialized as durable job-scoped output with fingerprinted summary metadata before rank writes. HITS reduce page fingerprints include the hub raw summary they depended on. Reusing a HITS hub raw summary validates the raw namespace count, norm, and raw fingerprint; stale summaries or raw values are rejected across reopen, and recompute replaces the iteration's raw namespace before writing replacement raw values. | Broaden paired-vector manifest coverage, including partitioned HITS hub contribution/reduction phases and restart/retry cases across dynamically appended HITS pages. |
| Page leases | Explicit page claim plus next-eligible scheduling, renew, reclaim, complete, fail, idempotent completion, progress, page range primitives, bounded retry exhaustion, and a metric/phase page-executor dispatch exist for planned degree; PageRank scan/out-degree, initialize, contribution, reduce, convergence, and cleanup pages now execute through the same path; eigenvector has a single-vector planned executor; and HITS has a first paired-vector planned executor over the same durable page lifecycle. Status now includes a capped active-page summary for the current build phase, including page id, state, range kind, worker id, attempt, lease expiry, cursor, unit progress, and last error, without exposing raw page range bounds; coverage verifies leased, failed, exhausted-attempt coordinator failure, reopened, capped multi-worker page summaries, and two active scan-page leases owned by independently reopened workers while the coordinator refuses to advance the incomplete barrier. Degree now has reopened-handle ownership coverage for a dead worker's active scan-page lease: status reports the abandoned owner and cursor, early cross-worker claim is refused, expiry allows another worker to reclaim with attempt increment and cursor reset, and the reclaimed page can recompute and finish the generation. Degree, PageRank, eigenvector, and HITS worker-only public steps now also tolerate a racing lost lease as no completed page, allowing another worker tick to retry instead of failing the build. Attempt-aware page execution now validates the durable page attempt before progress, completion, failure, or partial-output writes, so same-worker replacement after lease expiry fences stale prior-attempt work even when the worker id string matches. PageRank has restart coverage after dynamic iteration planning, contribution/reduce/convergence cursor resume after reopen on initial and later iterations, later-iteration failed-page retry across contribution/reduce/convergence phases, later-iteration exhausted contribution-page coordinator failure after graph-index reopen and now through spawned killed-worker processes with duplicate coordinator idempotence, same-worker next-page lease renewal, cleanup prefix cursor resume after reopen, reclaimed scan/initialize/contribution/reduce partial-output recompute coverage, reclaimed convergence-page summary reset before recompute, cleanup resume after reopen, public failed planned-build prior-generation preservation, and concurrent reopened-worker coverage across partitioned scan/initialize/contribution/reduce/convergence pages. Planned eigenvector contribution/reduce pages can now resume from durable cursors after reopen, reclaimed initialize/contribution/reduce pages overwrite stale partial output, later-iteration failed contribution/reduce/convergence pages retry through the generic worker step, later-iteration exhausted contribution-page coordinator failure after graph-index reopen and now through spawned killed-worker processes preserves the prior published generation with duplicate coordinator idempotence, cleanup resumes after reopen with the published scores visible, public failed planned rebuilds preserve the prior published generation, and concurrent reopened-worker coverage now proves the same partitioned scan/initialize/contribution/reduce/convergence ownership path. Planned HITS now has contribution/reduce cursor resume after reopen, multi-page reduce output-range isolation coverage, later-iteration failed-page retry coverage across contribution, reduce, explicit hub contribution, explicit hub reduce, and convergence phases through the generic worker step, later-iteration exhausted hub-reduce coordinator failure after graph-index reopen and spawned killed-worker process coverage that fails the compatible authority/hub pair once while preserving the prior published pair and fencing duplicate coordinator failure, reclaimed initialize/contribution/reduce output overwrite coverage for authority/hub job state, reclaimed convergence-summary reset coverage, cleanup cursor resume after reopen with the published pair visible, public failed planned HITS rebuilds fail the compatible authority/hub pair together while preserving the prior published pair, and concurrent reopened-worker coverage proves paired scan/initialize/contribution/reduce/convergence ownership while preserving coordinator-owned paired publish. | Add broader production failure coverage for expired/reclaimed partial output across all phases and add true remote distributed ownership tests. |
| Barriers | Durable phase summaries, multi-page phase barriers, convergence summaries, executable PageRank/eigenvector/HITS check pages, dynamic iterative advancement, resume-after-reopen coverage for a dynamically planned PageRank later iteration, and later-iteration HITS retry advancement into publish readiness exist. | Broaden failure tests around every iteration boundary and add the same restart matrix for eigenvector and paired HITS. |
| Publish | Local atomic generation publish plus planned-job publish verification exist; planned degree publish advances to a cleanup phase after flipping visibility, planned PageRank can publish converged or fixed-iteration output from durable rank state after dynamic iteration advancement, planned eigenvector publishes a verified single score vector from durable rank state, and planned HITS can atomically publish compatible authority/hub generations together. Planned PageRank, eigenvector, and paired HITS now have coordinator-owned publish-verifier failure coverage after graph-index reopen: a corrupted manifest at `publish_generation` fails the active build, removes abandoned build output, records bounded diagnostics, and keeps the prior published generation or compatible authority/hub pair queryable. Planned HITS active-rebuild coverage also proves newly materialized authority/hub job-namespace ranks stay invisible to direct top-k readers until paired publish, and failed planned HITS rebuilds record failure on both compatible metrics while leaving the previous authority/hub score generations queryable. | Add larger restart and cleanup coverage for paired HITS outputs plus true remote publish-race coverage. |
| Cleanup | Local score-generation cleanup exists; planned degree cleanup now uses separate durable prefix pages for degree partials and final job namespace removal; planned PageRank cleanup now uses separate durable pages for out-degree partials, node membership partials, and final job namespace removal, with restart coverage after a non-final cleanup page and durable cursor resume inside a large cleanup prefix. Planned HITS cleanup now has dedicated hub raw, hub raw summary, and HITS rank namespace cleanup pages before final job namespace cleanup, with cursor-resume-after-reopen coverage while the published authority/hub pair remains queryable. Planned degree scan attempts and planned iterative contribution attempts now write under the job namespace, so final cleanup removes abandoned attempt output after adoption. Planned eigenvector cleanup has cursor-resume-after-reopen coverage for final job namespace cleanup while the published scores remain queryable. Failed planned builds now delete unpublished score generations and the job namespace while preserving compact failed job status, public planned-failure coverage verifies that prior published degree, PageRank, eigenvector, and compatible HITS scores remain visible, and recent failed-build diagnostics are retained with a fixed bound. The public failure boundary now distinguishes an active lease/job pair from retained failed-job diagnostics, so a completed or already-failed build cannot be failed again as if it were active. | Extend attempt-scoped adoption to reduce phases only where deterministic recompute is insufficient, and add production distributed worker ownership. |
| Attempt adoption | Planned degree scan plus planned PageRank, eigenvector, and HITS contribution pages now have attempt-scoped partial output. The executor writes partial output into the current page attempt, same-worker resume accumulates in that attempt, page completion adopts the attempt output into the job-scoped namespace, and later reduce phases read only adopted output. Contribution and HITS hub-raw attempt writes now validate the durable page attempt before writing attempt-namespace partials, and adoption validates the durable page attempt in the same batch that copies attempt output into the job-visible namespace, so reclaimed stale attempts cannot write new partials or adopt abandoned output. PageRank scan still uses deterministic job-scoped page partials, but those writes are now fenced by the same durable page-attempt validation. | Apply the same capability to reduce pages if recompute cost or atomic replacement risk requires it. |
| Reduce-output fencing | Planned PageRank, eigenvector, and HITS authority/hub reduce pages now write next-iteration rank rows through durable-attempt validation paths that also record reduce progress or page completion. HITS hub raw summary creation/reuse is fenced by the same hub-reduce page attempt before rank normalization. Coverage proves reclaimed stale reduce claims cannot write additional rank rows or hub summary state after another worker owns the replacement attempt. | Apply the same write-and-complete fencing pattern to future reduce-like phases, or move those phases to attempt adoption if production-scale recompute/atomicity needs it. |
| Degree executor | Planned degree now runs through the generic metric/phase page-executor dispatch: scan pages write attempt-scoped partials, adopt them into job-scoped partials only on page completion, reduce pages materialize final scores from adopted partials, publish stays coordinator-owned, cleanup removes completed job and abandoned attempt keys through durable prefix pages, output matches the local runner, reopened coordinator/worker handles can start a build through public ensure, finish one generation through the public split-step boundary, and a dead worker's leased scan page can be observed, refused before expiry, reclaimed after expiry, recomputed, and completed through durable state. Degree reduce score writes are now guarded by durable page-attempt validation in the same batch that completes the page, and coverage proves a stale reduce claim cannot write the building score generation after another worker reclaims the page. Name-only public worker/coordinator steps now verify two workers can complete distinct scan and reduce pages while coordinator ticks refuse to advance until each phase barrier is complete. Threaded concurrent-handle coverage now proves independently opened workers can contend on scan/reduce pages, lose page ownership without failing the build, retry, and complete a generation without worker-side phase advancement or publish. Reopened-handle status coverage now proves two active scan-page owners are visible from durable page records before the coordinator advances. | Add remote worker orchestration and production ownership coverage beyond local threaded handles. |
| Rollout | Local PageRank, planned PageRank, degree, planned eigenvector, and HITS paths exist; planned degree, planned PageRank, planned eigenvector, and planned HITS have local-vs-planned parity coverage on deterministic graphs, including partitioned planned PageRank pages and partitioned paired HITS pages drained by alternating workers. Degree, PageRank, eigenvector, and HITS now also have coverage for the reusable planned-drain loop reaching a fresh generation through metric-name worker/coordinator calls and cleanup; the HITS case proves the same loop preserves paired authority/hub publish. Degree, PageRank, eigenvector, and HITS now have first threaded concurrent-handle worker ownership coverage, PageRank/eigenvector/HITS have reopened-coordinator no-duplicate-publish coverage, PageRank has first DB-level reopened-handle scheduler coverage through the bounded sweep interface, background PageRank/degree/eigenvector/HITS have explicit planned-maintenance coverage, default and explicit `runUntilIdle` auto mode now cover queued degree, PageRank, eigenvector, compatible HITS, multi-metric indexes, per-index deferred queued work, incompatible HITS fallback, explicit iteration caps, and active planned degree/PageRank/eigenvector/HITS coverage, and PageRank/eigenvector/HITS now have reopened-handle runtime split coverage through separate coordinator, worker, and reader handles plus automatic coordinator-only and worker-only runtime roles, including split-runtime coordinator publish-race proofs. | Broaden parity/restart coverage, add remote distributed worker coverage, collect deployment latency evidence, then gate and promote the distributed runner through existing profiles. |

Remaining delivery plan:

| Milestone | Design | Exit criteria |
| --- | --- | --- |
| R1. Production worker ownership | Replace local drain-style execution with a real worker ownership model. Workers claim only durable page leases; the coordinator owns build startup, phase barriers, dynamic iteration planning, publish, failure, and cleanup. The first split now exists as public `GraphIndex` build ensure, metric-name worker-only page, metric-name coordinator, active-build failure, and planned-drain steps: coordinator startup creates or returns the active durable job/manifest for a target generation, worker-only page execution leaves phases and publish untouched until a coordinator step runs, coordinator/worker callers do not need to pass metric configs, the planned-drain loop alternates named workers through the metric-name worker boundary and coordinator boundary, coordinator failure requires the active lease/job pair and preserves the prior published generation or compatible HITS pair, cross-family public-boundary coverage proves degree, PageRank, eigenvector, and HITS workers cannot advance barriers or publish, public failure coverage spans all four metric families, reopened degree coordinator/worker handles can complete a generation through durable state, DB-level PageRank scheduler coverage now reopens a fresh DB handle for every coordinator, worker, and reader tick while completing the same active durable build, and DB-level PageRank/eigenvector/HITS coverage proves two fresh coordinator handles cannot duplicate a publish event or paired publish event, planned-maintenance coverage proves scheduler callers can drain background PageRank, degree, eigenvector, and paired HITS builds through one bounded primitive, tiny-budget planned-maintenance ticks now report exhaustion and resume to completion without treating unfinished work as an error, pending-work stats report queued and active graph-metric planned work for scheduler callers, role-configured runtime loops can run as combined, coordinator-only, worker-only, or worker-pool owners with role/owner identity visible in internal stats, PageRank/eigenvector/HITS split-runtime coordinator race coverage now proves lease-owned runtime coordinator roles cannot duplicate a publish event or compatible paired publish event, scheduler publish counters are emitted from coordinator sweeps rather than worker cleanup sweeps, duplicate degree coordinator ticks are idempotent across barriers and publish, PageRank/eigenvector/HITS reopened-coordinator publish-race coverage proves two separate coordinator owners still leave one publish event and one published generation or compatible pair, active split-runtime coverage proves distinct worker identities can hold independent runtime leases and complete separate active pages, duplicate worker owners for the same worker identity or worker-pool identity set are fenced before claiming page work, same-worker replacement ownership can take over after runtime lease expiry during active page work, a dead worker's scan-page lease can be reclaimed after expiry without losing published visibility, racing degree workers can lose a page lease without failing the build and retry through another worker tick, exhausted page attempts now make the coordinator record a failed active build, and status reports capped active page owner/attempt/cursor/error summaries from durable page records. Lease timeouts, attempt limits, and backoff stay internal defaults until production behavior is stable. | Remote workers can complete one generation without duplicate publish; killing a worker only reclaims its leased page; status reports active distributed page ownership without exposing raw key bytes. |
| R2. Attempt-scoped output adoption | Keep deterministic overwrite as the default retry path, but add an executor capability for phases whose output is too expensive or risky to recompute. Degree scan and iterative contribution pages now use attempt namespaces: partial output remains invisible until page completion adopts it into the job namespace, and final cleanup removes abandoned attempt keys. Reduce phases should adopt the same capability as needed. | Page retries are safe for large contribution/reduce pages; abandoned attempts are cleaned; adoption does not expose partial output to readers; cleanup covers adopted and abandoned attempt namespaces. |
| R3. Iterative restart matrix | Treat PageRank, eigenvector, and HITS as one restart matrix over prepare, scan, initialize, contribution, reduce, convergence, publish, and cleanup. PageRank, eigenvector, and paired HITS now include later-iteration failed-page retry plus exhausted-attempt coordinator failure after graph-index reopen that preserves the prior published generation or compatible authority/hub pair. All three iterative families also have publish-verifier failure coverage after reopen, with the coordinator recording failed build diagnostics instead of leaking an error or publishing partial output. | Restart/reopen tests exist at every phase and iteration boundary; expired/reclaimed pages either resume from cursor or recompute without stale partial output; failed rebuilds preserve the prior published generation or authority/hub pair. |
| R4. Paired-vector completeness | Finish HITS as the paired-vector proof case. Authority and hub work share one target edge generation, compatible manifests, paired convergence metadata, and one atomic publish decision. Hub contribution and hub reduce/norm are now explicit retryable phases, hub raw contribution output is attempt-scoped, and active-rebuild authority/hub ranks stay invisible to direct top-k until paired publish. Lifecycle-gated local coverage now proves reclaim across authority/hub phases, paired publish failure, cleanup resume, larger-manifest restart after repeated reopen boundaries, failed-build preservation, exhausted hub-reduce pair failure, paired publish idempotence, and local/planned parity. The remaining work is true production remote-worker coverage before remote HITS builds are enabled. | HITS authority/hub output remains atomically visible; hub contribution/reduction work is partitioned into retryable pages; paired publish failure, cleanup resume, and stale-read behavior are covered. |
| R5. Query and freshness gates | Keep the public API stable: named graph metrics, `published` freshness for latest complete output, `fresh` freshness for current edge generation, `MetricNotReady` before first publish, and `MetricStale` for stale published generations. Distributed execution must not leak building output into projections, order-by, direct top-k, search rerank, or cross-shard fan-in. | Public e2e coverage proves direct top-k, traversal projection/order/filter, search rerank, status, and cross-shard fan-in behavior for stale, fresh, failed, and building metrics. |
| R6. Cleanup and retention hardening | Keep v1 cleanup aggressive: retain the latest published generation, remove completed job namespaces immediately when snapshot safety allows, delete failed/abandoned build output, and keep only bounded diagnostics. Future retention is an opt-in debug/admin feature, not part of query semantics. | Completed, failed, reclaimed, and abandoned jobs do not grow storage without bound; cleanup can resume after restart without hiding published output; future retention knobs remain bounded and disabled by default. |
| R7. Promotion | Promote the planned runner by metric family behind internal gates. Local runners remain the oracle until parity, restart, cleanup, and public freshness tests pass. Promotion should be per metric kind, with PageRank and degree before eigenvector/HITS remote defaults. | CI compares local and planned output on deterministic graphs; distributed worker tests run before enabling remote workers; public docs and generated clients describe status, freshness, convergence, and failure fields. |

Rest-of-work design roadmap:

The remaining implementation should be organized by production tracks rather
than by algorithm. Degree, PageRank, eigenvector, and HITS are different proofs
of the same graph-metric lifecycle, so each track should land as a narrow,
testable slice that keeps the current local runner as the compatibility oracle.

| Track | What to build | Dependencies | Exit criteria |
| --- | --- | --- | --- |
| T1. Lifecycle hardening | Close the remaining local planned-runner gaps: expired/reclaimed output tests, cleanup restart tests, failed-build preservation, and reduce-phase attempt adoption only where deterministic recompute is not enough. | Existing planned degree/PageRank/eigenvector/HITS executors and local parity tests. | Building output is invisible until publish; page retry either resumes or recomputes safely; completed and failed jobs clean up without hiding the prior published generation. |
| T2. Coordinator contract | Make the coordinator the only component that creates jobs, advances phase barriers, appends dynamic iteration pages, publishes, marks failure, and schedules cleanup. Worker-only page execution must remain unable to publish or advance phases. Planned build startup is now a public `GraphIndex` ensure call that creates or returns the active durable job/manifest for one target generation while keeping raw lease mutation internal, a public metric-name coordinator step resolves the config internally before ticking barriers or publish, and a public failure step records failed active builds only when the active lease matches the retained job. Public-boundary coverage proves degree, PageRank, eigenvector, and HITS workers cannot advance or publish without coordinator ticks, and public failure coverage preserves the prior published degree, PageRank, eigenvector, or compatible HITS pair. Degree also has direct coverage that reopened coordinators can use that public ensure boundary, duplicate coordinator ticks after barrier advancement become no-ops for the new phase until pages complete, coordinator publish moves the durable job to cleanup once, and a stale duplicate publish attempt fails closed rather than appending another publish event. PageRank, eigenvector, and HITS public-worker coverage now extends that publish idempotence proof across iterative and paired metrics: duplicate coordinator ticks after publish stay in cleanup, completion ticks preserve one publish event, and paired HITS keeps authority/hub publish events singular and compatible. | The worker/coordinator split and durable phase/page summaries. | Duplicate coordinator ticks are idempotent; concurrent coordinators cannot publish twice; status is derived from durable job, manifest, page, phase, iteration, failure, and cleanup records. |
| T3. Worker ownership | Replace local drain-style execution with production page ownership. Workers claim pages, renew leases, persist cursors, complete or fail pages, and stop. They do not create jobs, plan phases, pass metric configs, or publish generations. Worker-only and coordinator steps are now explicit metric-name `GraphIndex` calls and are also available through DB/index-manager calls by index name and metric name; bounded DB/index-manager coordinator and worker sweeps can drive active durable builds across graph indexes without per-metric drain calls; a reusable planned-drain primitive composes those calls over named worker IDs; degree, PageRank, eigenvector, and HITS drain coverage proves the primitive can reach a fresh generation and finish cleanup without worker-side publish, with HITS preserving paired authority/hub publish; degree/eigenvector/HITS split-step coverage starts through public build ensure and reaches publish through durable page and job records; DB-level degree coverage proves the same split boundary can publish through the future scheduler-facing layer, DB sweep coverage proves active degree plus iterative PageRank, eigenvector, and paired HITS work can complete through bounded production-style sweeps, DB-level PageRank coverage now proves those sweeps survive a fresh DB handle per worker/coordinator tick, and planned-maintenance coverage proves background PageRank, degree, eigenvector, and paired HITS can be started and drained through the scheduler-facing primitive; tiny-budget planned-maintenance coverage proves budget exhaustion is resumable and non-fatal, and pending-work stats now expose queued/active graph-metric work before, during, and after those budgeted ticks. The first DB graph-metric maintenance runtime now wraps that primitive in a start/stop/notify loop, focused coverage proves repeated bounded runtime ticks can publish a background PageRank build, multi-worker runtime coverage proves one bounded round can cycle two internal worker IDs across separate planned scan pages for one metric, split runtime coverage proves worker-only ticks complete pages without advancing phases until coordinator-only ticks run, automatic-role coverage proves coordinator-only and worker-only runtime loops preserve that same ownership split without manual split calls, started split-role loop coverage proves separate coordinator-role and worker-role background runtimes can publish a background degree generation without manual ticks, started worker-pool loop coverage proves a coordinator-role runtime plus worker-pool-role runtime can publish a partitioned degree generation through two configured worker IDs, reopened-handle runtime coverage proves background PageRank can publish through separate fresh coordinator, worker, and reader handles, reopened-handle worker-pool coverage proves fresh coordinator, worker-pool, and reader handles can publish a partitioned degree generation through two configured worker IDs, runtime clock plumbing now gives worker/coordinator sweeps deterministic lease-reclaim and exhausted-page time, and runtime stats coverage proves progress, idle, last-result, cumulative coordinator/worker/page/publish counters, recovered-error telemetry, and DB stats/runtime-status propagation. Degree has name-only public-step coverage proving separate workers finish distinct scan/reduce pages and cannot advance barriers without coordinator ticks; degree also has reopened expired-lease coverage proving a dead worker's active page remains visible, cannot be stolen early, and can be reclaimed after expiry with attempt/cursor reset; degree now has injected-time worker-step coverage proving a dead worker lease remains owned before expiry and is reclaimed/reset/completed after the injected time passes expiry; degree threaded concurrent-handle coverage proves racing worker handles can complete scan/reduce pages and retry benign lost-lease contention; degree status coverage now proves multiple active reopened-worker page leases are visible before barrier advancement; PageRank, eigenvector, and HITS threaded concurrent-handle coverage now prove the same public worker-only ownership shape across partitioned scan, initialize, contribution, reduce, and convergence phases; and active leased/failed pages are summarized in status from durable page records, including capped multi-worker active-page coverage. True remote worker scheduling, default idle promotion, and broader production failure tests remain. | Coordinator contract and bounded internal retry defaults. | Remote workers can finish one generation; killing a worker only reclaims its leased page; page ownership, attempt, cursor, phase, iteration, and last error are visible in status without exposing raw key ranges. |
| T4. Algorithm substrate | Keep PageRank as the iterative reference, eigenvector as the single-vector proof, and HITS as the paired-vector proof. Shared code owns planning, leases, barriers, convergence summaries, publish verification, status, and cleanup. Metric-specific code owns edge direction, transition math, normalization, convergence criteria, and score metadata. | Lifecycle hardening plus coordinator/worker contracts. | Adding a new centrality metric does not require a new job system, public query API, or status model. |
| T5. HITS paired-vector hardening | HITS now has explicit durable phases for authority contribute, authority reduce/norm, hub contribute, hub reduce/norm, and paired convergence. The next work is to finish phase-specific restart, reclaim, failure, and parity coverage before enabling large remote HITS builds. | Durable HITS paired publish, explicit hub phases, paired local-vs-planned parity, and active-rebuild stale-read coverage. | Authority and hub publish atomically; either side failing preserves the previous pair; hub and authority work have page-level retry, reclaim, progress, cleanup, and stale-output coverage. |
| T6. Public freshness gates | Prove that distributed execution cannot leak building output through direct top-k, traversal projection/order/filter, search rerank, explain/profile output, or cross-shard fan-in. Direct top-k, traversal projection/order/filter, and search rerank now have active planned-rebuild coverage proving published reads stick to the prior generation while reporting building status, and fresh reads fail closed. Direct top-k, traversal projection/order/filter, and search rerank now also have failed planned-rebuild coverage proving published reads preserve the prior generation while reporting failed status. | Verified publish and public query status plumbing. | `published` reads only complete generations; `fresh` fails closed with `MetricNotReady` or `MetricStale`; failed rebuilds preserve the latest published generation or compatible HITS pair. |
| T7. Operations and cleanup | Keep v1 operations conservative: immediate cleanup when snapshot-safe, deferred internal cleanup otherwise, bounded failed-job diagnostics, and internal retry/lease/backoff defaults. Public failure is an active-build operation, not a retained-diagnostics mutation: once failure clears the active lease, repeated failure attempts fail as not active. Pause, resume, retention, manual retry, and priority scheduling are future admin controls. | Cleanup cursors, failed-build diagnostics, and status. | Completed, failed, reclaimed, and abandoned jobs do not grow storage without bound; cleanup can resume after restart; no user-facing retention knob is required for v1 correctness. |
| T8. Promotion | Promote planned execution by metric family behind internal gates. Degree and PageRank should move first, eigenvector next, and HITS last after paired partitioning. | Parity, restart, cleanup, freshness, and multi-worker coverage for each family. | CI compares local and planned output on deterministic graphs; production distributed ownership tests pass before remote workers are enabled; docs and generated clients describe freshness, convergence, status, and failure fields. |

Recommended sequence:

1. Finish lifecycle hardening while execution is still local and deterministic.
   Add the missing restart/reclaim tests first because they define the durable
   invariants the production worker model must preserve.
2. Tighten the coordinator boundary. The coordinator should be an idempotent
   state machine over durable summaries, not a helper that trusts in-memory
   worker state.
3. Add production worker ownership with internal lease, attempt, and backoff
   defaults. Keep those defaults out of the public graph metric config until
   operational behavior is stable.
4. Finish the common iterative substrate. PageRank should remain the reference
   for dynamic iterations; eigenvector should reach the same restart matrix for
   single-vector normalization; HITS should finish the paired-vector phase
   split before large remote builds.
5. Gate every public read path against the same freshness semantics. Query
   behavior is part of the distributed design, not a later integration detail.
6. Promote per metric kind. Keep local runners as CI/debug oracles until the
   planned path has been stable across releases.

Long-term shape:

```text
configured graph index metric
  -> dirty marker for edge-scope/config fingerprint
  -> durable build job
  -> deterministic manifest pages
  -> worker-owned page leases
  -> phase and iteration barriers
  -> verified publish transaction
  -> published generation pointer
  -> resumable cleanup
```

The graph metric should continue to live with the graph index. Users should
configure named metrics on the index, not create a separate job resource for
each algorithm. Jobs, attempts, pages, and cleanup are implementation details
that surface only through status and events. `MetricNotReady` remains the
explicit pre-publish failure, `MetricStale` remains the explicit fresh-read
failure, fixed-iteration non-converged PageRank/eigenvector/HITS output should
publish by default with `converged: false`, and v1 should clean old generations
immediately unless snapshot safety requires the internal deferred cleanup queue.

Detailed remaining design:

1. Production coordinator and worker ownership.

   Keep phase ownership separate from page ownership. Workers only claim pages
   and persist progress; the coordinator owns active-job creation, phase
   barriers, dynamic iteration planning, publish, failure, and cleanup. Page
   leases should carry `worker_id`, `attempt`, `lease_expires_at_ms`,
   `cursor`, `completed_units`, and `last_error`. Coordinator ticks should be
   idempotent and should derive the next phase exclusively from durable page and
   phase summaries. A duplicate coordinator tick may observe that a transition
   already happened, but it must never publish twice or append incompatible
   pages.

   The first production scheduler should use internal defaults for lease
   timeout, max attempts, and backoff. These values are operational tuning, not
   graph metric API. Process/supervisor entrypoints and enabled runtime config
   validation should still reject zero runtime lease TTLs and zero
   round/metric/page budgets before starting an owner, so production
   misconfiguration fails closed instead of creating an idle role that cannot
   claim or renew work. Lower-level planned-maintenance calls may still model a
   zero-budget tick as resumable budget exhaustion for scheduler tests and
   callers. Status should expose enough to debug ownership and progress, but it
   should continue to hide raw key ranges.

2. Attempt-scoped output adoption.

   Deterministic overwrite remains the default retry strategy for small and
   cheap phases. A reclaimed page can delete or overwrite the output range for
   its own page and recompute from the range start. This is simpler and should
   stay the default for PageRank scan, reduce pages, and other phases whose
   page output can be atomically overwritten. Degree scan and iterative
   contribution pages use attempt-scoped adoption because their partial output
   can otherwise become visible to later phase aggregation before page
   completion.

   Keep attempt-scoped adoption as an executor capability for large reduce-like
   pages and future remote jobs. The page writes to:

   ```text
   metric_attempt/<metric>/<job_id>/<phase>/<iteration>/<page_id>/<attempt>/*
   ```

   Completion validates the attempt output, records its fingerprint, and then
   atomically adopts it into the job namespace or records an adopted-attempt
   pointer that later phases read. Failed or abandoned attempts are invisible to
   readers and are cleanup-owned. This keeps partial remote output from leaking
   into published scores while avoiding expensive destructive cleanup before
   every retry.

3. Harden HITS paired-vector partitioning.

   HITS should be the proof that the distributed runner can materialize
   compatible vector pairs without a new job system. The planned runner now
   explicitly separates authority and hub work instead of letting one reduce
   page materialize global hub raw state:

   ```text
   initialize_pair
   authority_contribute(iteration n)
   authority_reduce_and_norm(iteration n + 1)
   hub_contribute(iteration n + 1 authority)
   hub_reduce_and_norm(iteration n + 1)
   check_pair_convergence(iteration n, iteration n + 1)
   ```

   Authority contribution pages scan inbound edge ranges and write
   target-node authority partials. Authority reduce pages aggregate by target
   node range and write normalized authority ranks. Hub contribution pages then
   scan reverse-edge ranges, read the normalized authority rank for each target,
   and write attempt-scoped source-node hub raw partials that are adopted only
   after the page completes. Hub reduce pages aggregate by node range, repair or
   write the hub raw summary, and write normalized hub ranks. The convergence
   barrier compares authority and hub vectors together and either plans the next
   paired iteration or advances the compatible pair to publish.

   Publicly, authority and hub remain named graph metrics that live with the
   graph index. They publish together only when configured as a compatible pair,
   fail together on invalid output, and preserve the previous published pair on
   failure. There should not be separate HITS query APIs. The remaining work is
   to expand restart, lease-expiry, reclaimed-output, exhausted-attempt, publish
   failure, cleanup-resume, local-vs-planned parity, and true remote-worker
   coverage across the explicit HITS phases.

4. Restart and failure matrix.

   Treat PageRank, eigenvector, and HITS as one matrix over:

   ```text
   prepare -> scan -> initialize -> contribution -> reduce -> convergence
   -> publish -> cleanup
   ```

   PageRank is the coverage baseline. Eigenvector needs the same restart,
   failed-page retry, reclaimed-output, publish-failure, and cleanup coverage
   for its single-vector phases. HITS needs the same matrix for the paired
   authority/hub phases, with explicit tests that one side cannot publish
   without the other and that stale paired output is overwritten or ignored on
   retry.

   The required invariants are:

   - `published` reads never observe building output.
   - `fresh` reads return `MetricNotReady` before first publish and
     `MetricStale` when the published generation does not match the target edge
     generation.
   - failed rebuilds preserve the previous published generation or pair.
   - expired/reclaimed pages either resume from a durable cursor or recompute
     from the range start without accumulating stale partial output.
   - cleanup can resume after restart without hiding the published generation.

5. Query and freshness gates.

   The distributed runner is not complete until public query paths prove the
   same freshness semantics as direct top-k metric reads. Gate promotion on
   coverage for traversal projection, traversal ordering, filters that depend
   on metric score, search rerank, explain/profile output, and cross-shard
   fan-in. Cross-shard score-bearing results must prove they refer to compatible
   nonzero published generations before merging; otherwise they should fail
   closed.

6. Cleanup and retention.

   V1 cleanup should remain aggressive. Keep the latest published generation,
   delete completed job namespaces promptly, delete failed or abandoned building
   output, and retain only bounded recent diagnostics. Future retention can be
   a bounded debug/admin option, but it should not affect query semantics and
   should stay disabled by default.

7. Promotion sequence.

   Promote by metric family rather than flipping all graph metrics at once:

   1. keep local runners as the oracle
   2. run planned degree and PageRank by default in tests
   3. enable local multi-worker planned builds
   4. enable production worker ownership for degree/PageRank behind a gate
   5. finish eigenvector restart/failure parity
   6. finish HITS paired restart/failure parity across the explicit phases
   7. enable remote/distributed workers for large graph indexes
   8. demote local runners only after deterministic parity, restart, cleanup,
      freshness, and cross-shard tests pass

Recommended implementation order:

| Order | Slice | Design intent |
| --- | --- | --- |
| 1 | Planned `degree` reduce | Done: independent scan pages are safe because scan writes per-page partials and `reduce_ranks` materializes final scores. |
| 2 | Add real partition planning | In progress: degree scan, reduce, and cleanup pages are partitioned, initial PageRank/eigenvector/HITS phase pages are partitioned, HITS now has an explicit durable phase shape for authority contribution, authority reduce/norm, hub contribution, hub reduce/norm, paired convergence, publish, and cleanup, and hub raw output is produced through attempt-scoped hub-contribution pages before hub-reduce pages normalize it. Later iterative pages are planned dynamically, and dynamic page appends update manifest page counts idempotently. HITS unit-test local coverage now includes active prior-pair visibility, paired publish idempotence, initialize/contribution/reduce/hub/convergence reclaim, larger-manifest resume across repeated reopen boundaries, cleanup resume after reopen, failed-build preservation, publish-failure preservation, and exhausted hub-reduce pair failure. Remaining work is production distributed coverage. |
| 3 | Add resumable cursors | In progress: degree scan pages resume from same-worker cursors, reclaimed scan pages safely recompute, and degree scan now uses an attempt namespace whose output is adopted only when the page completes; PageRank scan, contribution, reduce, convergence, and cleanup pages persist cursor progress, with PageRank, eigenvector, and HITS contribution partials now also written to attempt-scoped keys and adopted only on page completion. Contribution/reduce/convergence resume is verified across reopen on initial pages and dynamically planned later-iteration pages, cleanup prefix deletion resume is verified across reopen, later-iteration failed pages retry through the coordinator, reclaimed scan/initialize/contribution/reduce pages recompute without stale partial output, and reclaimed convergence pages recompute without stale partial summaries. Planned eigenvector now verifies contribution/reduce cursor resume after reopen, reclaimed contribution/reduce stale-output overwrite, later-iteration failed contribution/reduce/convergence page retry, cleanup cursor resume after reopen, and prior-published preservation after a failed planned rebuild. Planned HITS now verifies contribution/reduce cursor resume after reopen, later-iteration failed contribution/reduce/hub-contribution/hub-reduce/convergence pages retry through the generic worker step and advance to publish readiness, later-iteration exhausted hub-reduce attempts fail the compatible pair while preserving the prior published pair, reclaimed contribution/reduce pages overwrite stale durable authority/hub job output, reclaimed convergence pages reset stale partial summaries before recompute, and cleanup cursor progress resumes after reopen with the published pair still visible. Remaining work is broader attempt-scoped adoption for reduce phases where needed, broader expired/reclaimed partial-output coverage across all phases, and production distributed ownership. |
| 4 | Generalize the page executor | In progress: degree, PageRank, eigenvector, and HITS use the generic metric/phase page-executor dispatch. The combined local worker helper now composes a worker-only page step with a coordinator step, the split worker/coordinator calls are exposed on `GraphIndex`, planned build startup has a public ensure call that keeps raw lease mutation internal, metric-name public worker/coordinator calls resolve configs inside the index, public failure records active-build diagnostics through the coordinator boundary, and a planned-drain primitive now composes metric-name ensure, worker-page, and coordinator calls over named worker IDs. PageRank and cross-family split-step coverage now run through public build ensure plus metric-name worker/coordinator calls, public active-build failure coverage now spans degree, PageRank, eigenvector, and HITS, degree has reopened-handle coverage across public build ensure plus worker/coordinator steps, name-only multi-worker page coverage, threaded concurrent-handle scan/reduce ownership coverage, concurrent active-page status coverage, expired-page reclaim, benign lost-lease retry behavior, and duplicate coordinator tick coverage, DB-level PageRank coverage now drives the bounded scheduler sweeps through a fresh DB handle per worker/coordinator/read tick, background PageRank, degree, eigenvector, and paired HITS can now be drained through the explicit planned-maintenance primitive, budget exhaustion is reported as a resumable result instead of an error, the graph-metric runtime can now run automatic ticks in combined/coordinator/worker/worker-pool roles, and status now summarizes active leased/failed pages from durable page records. Remaining work is true remote production worker orchestration, latency-safe default idle promotion, and broader production failure coverage. |
| 5 | Move PageRank onto durable pages | In progress: scan/out-degree pages write durable out-degree and node intermediates, initialize pages write aggregate out-degree plus iteration-0 rank state, contribution pages write attempt-scoped durable contribution partials and adopt them into the reduce-visible namespace only on page completion, reduce pages write iteration `n + 1` ranks, check pages write convergence summaries, non-final checks dynamically plan later iterations, planned publish materializes public score output, local-vs-planned parity is covered on deterministic graphs, dynamically planned later iterations can resume after reopen, contribution/reduce/convergence pages can resume from a durable cursor after reopen on initial and later iterations, failed later-iteration pages can be retried by another worker before publish readiness, and expired scan/initialize/contribution/reduce pages can be reclaimed without stale partial output; remaining work is broader expired/reclaimed coverage and larger cleanup/failure coverage. |
| 6 | Enable multiple workers | In progress: local alternating-worker coverage now proves planned PageRank pages and paired HITS pages can be claimed by independent workers while the coordinator remains the only phase/publish authority. Degree, PageRank, eigenvector, and HITS planned-drain coverage now proves a reusable metric-name worker/coordinator loop can complete a generation and cleanup with multiple worker IDs; HITS additionally proves paired authority/hub publish through that loop. Degree, PageRank, eigenvector, and HITS also have threaded concurrent-handle coverage proving partitioned pages can complete through independent reopened handles with benign lost-lease retries, PageRank now has DB-level fresh-handle scheduler coverage through the bounded sweep interface, background PageRank/degree/eigenvector/HITS have explicit planned-maintenance drain coverage, and tiny-budget planned-maintenance ticks resume to completion. Remaining work is remote distributed worker orchestration, default idle promotion, and failure coverage across all metric families. |
| 7 | Complete distributed cleanup | In progress: planned degree and PageRank now delete metric-specific partial prefixes before final job namespace removal without deleting published output; planned HITS now deletes hub raw, hub raw summary, and HITS rank intermediates through dedicated cleanup pages before final job namespace removal; planned degree final cleanup now also removes abandoned attempt-scoped scan output, and the shared job namespace cleanup removes abandoned iterative contribution attempts for PageRank, eigenvector, and HITS. Planned PageRank cleanup can resume after graph-index reopen with the published generation still queryable, including durable cursor resume within a large prefix cleanup page. Planned eigenvector and HITS cleanup can also resume after reopen while keeping published scores queryable. Failed planned builds now clean unpublished score output and job namespaces while keeping compact failure status and bounded recent failure diagnostics, and public failure coverage verifies that previous published degree, PageRank, eigenvector, and compatible HITS scores remain queryable. Remaining work is reduce-phase attempt adoption where needed and production distributed worker ownership. |
| 8 | Move eigenvector and HITS | In progress: eigenvector now reuses the single-vector iterative executor with local-vs-planned parity plus attempt-scoped contribution output, contribution/reduce cursor-resume, reclaim, later-iteration failed-page retry, later-iteration exhausted-attempt failure preservation, cleanup resume, public failed-rebuild prior-generation preservation coverage, reusable planned-drain coverage, threaded concurrent-handle partitioned worker coverage, and spawned-process coverage that publishes a fresh eigenvector generation through real coordinator/worker-pool child roles, reclaims killed scan, initialize, contribution, reduce, and convergence page owners through real worker role processes, publishes at the coordinator boundary after setup restart, completes cleanup through a separate worker process, and fails a corrupted publish manifest while preserving the prior generation. HITS now has a first paired-vector planned runner with atomic compatible authority/hub publish parity, attempt-scoped authority and hub contribution output, explicit hub contribution and hub reduce phases in the durable manifest, active-rebuild stale-read coverage for authority/hub top-k, alternating-worker paired-page drain coverage, reusable planned-drain coverage through metric-name worker/coordinator boundaries, threaded concurrent-handle paired-page ownership coverage, later-iteration failed-page retry coverage across authority and explicit hub phases, later-iteration exhausted-attempt paired failure preservation through hub reduce, reclaimed contribution/reduce output coverage, reclaimed convergence-summary reset coverage, cleanup resume coverage, and public paired failure preservation for prior published authority/hub scores. The focused unit-test aggregate is now wired into Zig CI and keeps degree, PageRank, eigenvector, and HITS local-vs-planned parity, partitioned PageRank/HITS worker-drain proofs, eigenvector scan/initialize/contribution/reduce/convergence reclaim, cleanup resume, failed-build preservation, publish-failure preservation, HITS active prior-pair visibility, paired publish idempotence, initialize/contribution/reduce/hub/convergence reclaim, cleanup resume, failed-build preservation, publish-failure preservation, exhausted hub-reduce pair failure, and repeated failed-build cleanup/storage-growth proofs executable without running the entire graph suite. Remaining work is production distributed coverage, promotion-scale fan-in, and latency/default-widening evidence. |
| 9 | Promote behind gates | Compare local and planned outputs, then make the distributed runner the default after restart and parity coverage. |

Current HITS restart coverage now includes explicit hub contribution and hub
reduce cursor resume across graph-index reopen, in addition to authority
contribution/reduce resume, hub-phase failed-page retry, and hub-reduce
exhausted-attempt preservation of the prior compatible pair.

Current runtime process-boundary coverage now proves
`OpenOptions.graph_metric_maintenance` can create lease-owned coordinator and
worker-pool runtimes with `start_background_loop = false`; fresh DB-open
handles tick those roles through durable graph-index state and publish degree,
PageRank, eigenvector, and compatible HITS without manual runtime construction
or worker-side metric config.

This order keeps every slice independently testable. Degree proves the storage,
lease, progress, publish, and cleanup contract without iterative math. PageRank
then proves iteration planning, convergence summaries, and intermediate
storage. Eigenvector and HITS should be last because they are mostly proof that
the executor abstraction is metric-family shaped rather than PageRank-shaped.

#### Remaining Graph Index Roadmap

The rest of the graph-index work should be treated as one execution-roadmap,
not as separate PageRank, degree, and eigenvector projects. The durable runner
should become the only way graph metrics materialize large generations, while
the existing local runner remains the compatibility oracle until planned output
matches it.

Current baseline:

- The user model is settled: graph metrics live on graph indexes, reads choose
  `published` or `fresh`, `MetricNotReady` means no complete generation exists,
  and `MetricStale` means a complete generation exists but not for the requested
  current edge generation.
- The executor model is settled: coordinators own job creation, barriers,
  publish, failure, and cleanup; workers own leased page execution only; runtime
  owners and page leases are separate durable fences.
- The implementation already has planned degree, PageRank, eigenvector, and
  compatible HITS coverage through durable graph-index state, plus a first
  role-configured runtime and `graph-metric-maintenance` command surface.
- What remains is production distribution, not a new public API: remote
  deployment orchestration, PageRank-style failure coverage for every promoted
  family, public-read fan-in gates, and operations/cleanup hardening.

Authoritative rest-of-work roadmap:

| Milestone | Design | Implementation slices | Exit gate |
| --- | --- | --- | --- |
| 1. Process supervisor boundary | Keep the graph metric API unchanged while moving the role-configured runtime into independently managed process owners. Processes receive DB path or service endpoint, runtime owner identity, worker identity, tick budgets, and idle policy only; metric config is resolved from the graph index. | Done for the first process proof: `graph-metric-maintenance supervise` and `launch` run coordinator and worker-pool child roles in bounded rounds, capture JSON summaries, preserve per-role stderr, enforce a narrow argv allow-list, and keep local DB writer-guard behavior confined to direct file-backed proof runs. The integration-test aggregate runs the spawned-process harness with the real `antfly` binary, drains degree, bounded PageRank, eigenvector, and compatible HITS through durable graph-index state, verifies owner/worker/lease telemetry, rejects stale page attempts, and proves duplicate coordinators cannot republish, refail, advance phase, or append duplicate terminal events after publish or publish-verifier failure. | Complete for process smoke: coordinator and worker processes can finish degree, PageRank, eigenvector, and compatible HITS work without worker-side metric config, and the local launch path can drain all four metric families through durable state. Remaining work moves to remote deployment orchestration and rollout readiness. |
| 2. Runtime ownership and lease hardening | Treat durable state, not process memory, as the coordination boundary. Runtime-owner leases fence processes; page leases fence page execution; page attempts fence output adoption. | The process harness now kills a coordinator owner after it has acquired its runtime lease, proves a distinct worker-pool process can continue under an independent runtime lease, proves a duplicate coordinator is fenced before the original lease expires, and proves a replacement coordinator can take over after expiry and advance durable work. It also kills a process that owns a degree scan page lease, proves a replacement worker is fenced before page-lease expiry, proves a real `antfly graph-metric-maintenance --role worker` process can reclaim and complete the page after expiry with an injected clock, and proves the stale old page attempt is rejected after replacement. Same-worker runtime fencing is now covered through real worker role processes: one owner acquires the worker runtime lease and completes a page, a duplicate owner with the same worker id is fenced before expiry, and a replacement owner with the same worker id takes over after expiry, reports runtime `takeover_count`, and completes more durable page work. Replacement coordinator takeover also reports `takeover_count`, giving operations an explicit cross-process replacement signal even when the killed owner cannot observe local `lost_leases`. PageRank now has the same process proof for first-iteration scan/out-degree, initialize, contribution, reduce, and convergence pages; dynamically planned later contribution, reduce, and convergence pages; publish through a real coordinator role process; cleanup through separate real worker role processes; cleanup-page owner death/reclaim with the published generation visible; publish-verifier failure through a real coordinator process while preserving the prior published generation; duplicate coordinator idempotence after that publish-verifier failure; and same-worker-id replacement after page-lease expiry with stale prior-attempt rejection. Remaining hardening is remote orchestration beyond a direct local DB path. | Worker loss abandons only reclaimable page work; stale owners cannot write, adopt, complete, publish, or fail the build after replacement; duplicate coordinators cannot double-publish, double-fail, or append incompatible pages/events. |
| 3. Crash/restart harness | Make failures reproducible before promoting distributed execution. Every phase must be restartable from fresh DB handles and process boundaries. | The current degree harness now covers runtime-owner kill/takeover, scan-page owner kill/reclaim, stale scan-page attempt rejection, publish-boundary restart through a real coordinator process, and cleanup restart through separate real worker processes. PageRank process coverage now covers first-iteration scan/out-degree, initialize, contribution, reduce, and convergence owner kill, dynamically planned later contribution/reduce/convergence owner kill, before-expiry fencing, post-expiry reclaim by a real worker role, stale attempt rejection, same-worker replacement attempt fencing, drain to fresh, publish-boundary restart through a real coordinator process, cleanup restart through separate real worker processes, cleanup-page owner death/reclaim, and publish-verifier failure preserving the prior generation. | Published reads see either the prior complete generation or the newly published complete generation. Active job output, abandoned attempts, and failed generations are never queryable. |
| 4. Distributed degree promotion | Use degree as the first production ownership proof because it exercises scan, reduce, publish, cleanup, leases, attempts, and public freshness without iterative math. | Degree now has a spawned-process restart proof for supervisor completion, coordinator runtime lease loss, worker page lease loss/reclaim, publish-boundary restart, cleanup restart, and stale attempt rejection. Remaining degree work is promotion hardening: keep local/planned parity, broaden public read-surface freshness coverage through the distributed gate, verify active/failed status page summaries from durable records, and decide the internal rollout gate. | Degree can be enabled behind an internal distributed gate with local-vs-planned parity, process crash coverage, cleanup resume coverage, and direct/traversal/search freshness coverage. |
| 5. Distributed PageRank matrix | PageRank is the reference iterative metric. It must prove dynamic iteration planning, convergence, fixed-iteration non-converged publish, publish verification, failure preservation, and cleanup across process boundaries. | The process harness now covers first-iteration and dynamically planned later-iteration page owner death/reclaim/stale-attempt rejection, publish through a real coordinator process, cleanup through real worker processes, cleanup-page owner death/reclaim, same-worker replacement, publish-verifier failure preserving the prior generation, duplicate coordinator idempotence after publish-verifier failure, fixed-iteration non-converged publish metadata through both the process-supervised path and the publish-boundary coordinator process path, a bounded PageRank build drained through independently launched coordinator and worker-pool owners, and public freshness while a spawned coordinator-owned rebuild is active: direct metric top-k, graph traversal projection, and search rerank serve the prior generation with `building` status, while direct `fresh` reads, traversal `fresh` projection/order/filter, and rerank `fresh` fail with `MetricStale`. The real HTTP service-owner path now also covers PageRank publish-verifier failure: a service-targeted coordinator observes a corrupted manifest, fails without publishing, preserves the prior generation, and a duplicate service coordinator cannot fail or publish again. The unit-test aggregate now also enforces PageRank local-vs-planned score parity, partitioned alternating-worker drain behavior, and repeated failed-build cleanup/storage-growth behavior. Remaining work is production remote orchestration and promotion-scale cross-shard public read coverage. | A dirty PageRank rebuild can finish through remote owners, match local output within tolerance, publish fixed-iteration non-converged results with `converged: false`, preserve the prior generation on failed rebuilds, and recover from restart at every phase boundary. |
| 6. Eigenvector parity | Eigenvector proves the single-vector iterative substrate is metric-family shaped rather than PageRank-specific. It must not add a new job system, status model, or query API. | The process harness now has eigenvector process-boundary coverage: the real `antfly graph-metric-maintenance supervise` path starts coordinator and worker-pool child roles, drains a bounded eigenvector build through durable graph-index state, and verifies the target generation becomes fresh; the independently launched coordinator/worker-pool path now drains a separate eigenvector build to fresh through the same graph-index state; separate killed-owner cases cover scan, initialize, contribution, reduce, and convergence page leases, before-expiry fencing, post-expiry reclaim by a real worker role, stale-attempt rejection, and final drain to fresh; publish/cleanup coverage prepares a build to `publish_generation`, publishes through a real coordinator role process, then completes cleanup through a separate worker role process; publish-verifier failure coverage corrupts the manifest at `publish_generation`, fails through a real coordinator process, preserves the prior generation with bounded diagnostics, and proves a second coordinator cannot fail the already-failed build again or append another failure event. The same publish-verifier failure invariant now also crosses the real HTTP service-owner path for eigenvector service coordinators. Fixed-iteration non-converged publish now also crosses the process-supervised path and verifies `converged: false`, `iterations_completed`, and positive finite delta metadata. Active-rebuild public freshness now also crosses a coordinator-owned process boundary: `published` eigenvector direct top-k, traversal projection/status, and search rerank serve the prior generation with `building` status, while `fresh` direct reads, traversal projection/order/filter, and search rerank fail closed with `MetricStale`. Graph-layer parity now also covers disconnected/reducible topology: local and planned eigenvector output match within tolerance, every score is finite, the vector remains L2-normalized, and reducible sink-chain nodes can decay to zero without invalid output. Remaining work is promotion-scale cross-shard fan-in and production rollout evidence. | Eigenvector reaches the same restart/failure/cleanup/public-read matrix as PageRank through the same graph-index metric API and distributed executor. |
| 7. HITS paired-vector production | HITS is the paired-vector proof. Authority and hub are separate named scores, but compatible configs share one target generation, convergence decision, publish decision, failure decision, and cleanup lifecycle. | The process harness now has a first HITS process-boundary proof: a compatible manual authority/hub pair is explicitly started as a planned build, the real supervisor launches coordinator and worker-pool child roles, and both authority and hub publish the same fresh target generation with queryable top-k output. The independently launched coordinator/worker-pool path now also drains a compatible HITS pair to fresh. It also kills process-owned authority contribution, authority reduce, convergence, `hits_hub_contributions`, and `hits_hub_reduce_ranks` page owners, proves replacement workers are fenced before lease expiry, proves real worker role processes reclaim and complete those pages after expiry, rejects stale attempts, and drains the compatible pair to fresh. Paired publish/cleanup now also crosses process boundaries: a real coordinator role publishes the authority/hub pair atomically, authority remains in cleanup while hub is fresh/complete with the same generation and publish event, and separate worker role processes resume cleanup until both metrics are fresh. Publish-verifier failure now also crosses a real coordinator role process: a corrupted HITS manifest fails without publishing, both authority and hub preserve the previous generation and retained publish-failure diagnostics, and a second coordinator cannot fail the already-failed compatible pair again or append another failure event. The same paired publish-verifier failure invariant now also crosses the real HTTP service-owner path: a service-targeted coordinator fails the compatible pair without publishing, and a duplicate service coordinator cannot add another failure or publish event. Failed HITS public reads now preserve that prior compatible pair across direct top-k, traversal projection/status, and search rerank score details, while `fresh` direct, traversal projection, and rerank reads fail closed with `MetricStale`. Active-rebuild public freshness now crosses a coordinator-owned process boundary: published authority/hub top-k reads serve the prior compatible pair with `building` or compatible stale status, traversal projection/status serves prior authority/hub scores, and search rerank over HITS authority uses prior-generation score details, while fresh authority/hub direct reads, traversal projection/order/filter, and search rerank fail closed with `MetricStale`. Exhausted-attempt failure now also crosses process boundaries: repeated killed owners reclaim the same later-iteration hub-reduce page across expired leases until a real coordinator fails the compatible pair with `GraphMetricBuildPageAttemptsExhausted`, preserving the previous pair and prior top-k/direct/traversal/rerank output. Public fan-in now rejects default authority/hub status pairs with mismatched published generations or incompatible stable metadata/edge filters before merging direct metric or graph traversal/search status surfaces. Hosted cross-range fan-in now also proves compatible HITS authority/hub pairs merge for both `published` and `fresh` reads across direct metric, traversal projection/order/filter/status, and HITS authority rerank surfaces, while unpublished or mixed-generation HITS shards fail closed before those score surfaces can mix results. Real HTTP service-owner publish/cleanup and multi-page proofs now also verify paired fixed-iteration metadata for authority and hub: both publish `converged: false`, matching `iterations_completed`, and positive finite deltas when the bounded iteration limit is reached. Remaining work is promotion-scale parity, larger-graph latency, remote deployment orchestration, and promotion-scale cross-shard fan-in coverage under remote owners. Keep hub and authority phase work page-partitioned and attempt-fenced where partial output can be consumed by later phases. | Compatible HITS authority/hub output publishes atomically, either side failing preserves the previous compatible pair, and remote HITS remains disabled by default until paired restart/failure coverage matches the PageRank matrix. |
| 8. Public read and fan-in gates | Distributed execution is not complete until every score-bearing read path uses only complete published generations and fails closed for freshness mismatches. | Current fast-root fan-in coverage now includes direct metric top-k merge, direct missing/unpublished shard rejection, direct failed-rebuild shard-status preservation with `fresh` fail-closed behavior, traversal projection/status generation checks, traversal failed-rebuild shard-status preservation with `fresh` fail-closed behavior, traversal order/filter generation checks, traversal failed-rebuild order/filter status preservation with `fresh` fail-closed behavior, duplicate graph order metric request rejection, duplicate graph-search node/hit ownership rejection, direct/traversal/rerank profile status output, failed-status profile output across direct metric top-k, traversal, and rerank surfaces, direct metric top-k merged profile status after shard fan-in, search rerank merged profile status after shard fan-in, search rerank incompatible-generation checks, rerank score details, rerank score-details arithmetic and missing-score consistency checks, rerank missing/unpublished shard-status rejection, rerank failed-rebuild shard-status preservation with `fresh` fail-closed behavior, HITS authority/hub pair compatibility rejection for direct graph metric and graph traversal/search status fan-in, and direct/traversal/rerank status-generation shape checks that reject `fresh` statuses whose current or target edge generation does not match the published score generation. DB search coverage now also proves direct graph metric top-k and search rerank return `MetricNotReady` for both `published` and `fresh` before first publish, while traversal projection/order/filter DB coverage proves not-ready, stale, building, failed, and fresh-failure behavior. Hosted cross-range coverage now also builds local shard DBs from catalog range metadata, proves compatible fresh published generations merge for `published` and `fresh` direct metric top-k, traversal projection/status/order/filter, and search rerank, proves direct graph metric fan-in and search rerank fail closed when one shard is unpublished, advances one shard to an incompatible published generation, and proves direct metric top-k, traversal projection/status/order/filter, and search rerank reject mixed-generation results. It now also includes paired HITS authority/hub in the hosted path: compatible shard generations merge for both `published` and `fresh` direct metric fan-in, traversal projection/order/filter/status fan-in, and HITS authority rerank, while unpublished or mixed-generation paired HITS shards fail closed before score mixing. It also proves cross-range traversal metric projection/status/order/filter now carries metric payloads through distributed expand/fan-in, reports graph-query metric status in query profile, and `fresh` projection fails closed on an unpublished shard. Focused public API graph metric e2e coverage now covers public HTTP status, not-ready status/direct/projection/rerank behavior before first publish, fresh direct top-k, traversal projection, ordering, filtering, stale `published` direct top-k/projection/search rerank after a `sync_level: "write"` graph update, prior-generation rerank score details, stale rerank profile status, active-build `published` direct/projection/rerank reads with `building` status, failed-build `published` direct/projection/rerank reads with `failed` status, and `fresh` fail-closed behavior across stale, building, and failed public direct/traversal/rerank reads. The broader `public-api-parity-test` covers generated public query bodies with explicit `null` graph metric fields. Those hosted fan-in and public HTTP freshness checks now run through the unit-test aggregate, so hosted fan-in and public HTTP freshness regressions are no longer local-only evidence. Remaining read-gate work is promotion-scale cross-shard generation compatibility for every read surface and, if a standalone explain API is added later, matching graph metric generation/freshness evidence there. Query profile is the current explainable read contract and already carries direct, traversal, and rerank metric status. | `published` reads use the latest complete generation; `fresh` returns `MetricNotReady` before first publish or `MetricStale` when published is behind; cross-shard fan-in rejects missing, zero, stale, or incompatible generations when comparability is required. |
| 9. Cleanup, retention, and operations | V1 cleanup remains aggressive. Retain the latest published generation and bounded diagnostics; remove completed, failed, abandoned, unpublished, and attempt-scoped job state as soon as snapshot safety allows. Retention, pause/resume, manual retry, priority, and lease tuning are future admin controls, not query semantics. | Make cleanup process-resumable for score generations, job namespaces, manifests, pages, phase summaries, iteration summaries, attempt namespaces, failure diagnostics, and runtime-owner records. Keep operations status high level: role, owner hash, worker identity count/hash, phase, iteration, page counts, cursor presence, attempts, progress, last error, and last sweep result. Fast root coverage now includes failed planned-build cleanup of abandoned score/job namespaces plus bounded retained failure diagnostics and metric events, including old-key pruning; runtime-status cache coverage preserves graph metric runtime ownership telemetry such as owner/worker hashes, worker count, lease state, takeover count, lost leases, tick progress, and page counters across sequence-only status refreshes; graph index status now exposes that telemetry as a typed `graph_metric_runtime` OpenAPI summary on aggregate and per-shard graph index status; command summary and real spawned-process harness coverage now preserve the same role/owner/worker/lease/tick/error telemetry in supervisor/launcher output; and direct runtime-owner lease-record assertions prove clean runtime shutdown deletes the current durable owner record while stale shutdown after takeover leaves the replacement record intact. The unit-test aggregate now pins cleanup-page resume for PageRank/eigenvector/HITS, active-job cleanup refusal, failed planned-build abandoned namespace cleanup, bounded retained diagnostics, repeated failed non-iterative/iterative/paired cleanup, and durable runtime-owner lease cleanup. The unit-test aggregate pins active build page status summaries, failed-page cursor/progress/error diagnostics, OpenAPI/runtime-status encoders, internal service maintenance boundary, command/supervisor argv contract, child runtime telemetry parsing, idle-exit behavior, and owner/worker summary fields. | Cleanup resumes after restart, never deletes the current published generation, bounds diagnostics, and prevents graph-metric control/output namespaces from growing without a public retention knob. Deployment-scale cleanup cost and storage-growth qualification remain separate release evidence. |
| 10. Per-family default promotion | Promote planned/distributed execution by metric family, not all graph metrics at once. Local runners stay as CI/debug oracles until distributed parity is boring. | Keep the conservative `auto` gate, collect latency evidence, then promote degree and PageRank first, eigenvector after single-vector parity, and HITS last after paired-vector coverage. The internal auto-gate decision summary now makes the rollout boundary inspectable in tests: active planned work, eligible queued work, and ineligible queued work are counted before default or explicit auto idle paths choose planned execution or local fallback. Queued degree, small PageRank, bounded eigenvector, and active planned PageRank/degree/eigenvector/HITS all assert the planned decision before execution; larger PageRank/eigenvector, multi-metric indexes, default HITS, and incompatible HITS assert fallback before local execution. HITS opt-in is inspectable through the same counters: default HITS and incompatible opt-in pairs remain ineligible, while a compatible opt-in authority/hub pair contributes one eligible queued build before planned execution. PageRank and eigenvector threshold widening are now covered too: raising the internal iteration caps makes previously gated larger single-vector rebuilds eligible for planned execution, proves tiny-budget exhaustion leaves resumable active work, and proves the same jobs complete when the budget is widened. Unit-test default-gate coverage pins those conservative auto-gate, widening, fallback, HITS opt-in, and active-planned-resume decisions in PR and full-default Zig CI. Update generated clients and public docs only around stable user-facing status/freshness fields. | Each promoted family has deterministic parity, process ownership, crash/restart, cleanup, public-read, cross-shard, status, operations, latency-budget, and rollback coverage before distributed execution becomes the default. |

The next PRs should be cut along these implementation slices:

1. **Production remote orchestration**: promote the process launcher beyond the
   direct local DB proof into deployment hooks for remote coordinator and worker
   owners. Workers should still receive only DB path or service endpoint,
   role/owner identity, worker identity, budgets, and idle policy; all metric
   config must remain graph-index owned. Direct file-backed launch keeps its
   explicit local DB writer guard because graph runtime leases are not a
   storage-engine multi-writer lock.
2. **Degree distributed gate**: finish promotion hardening for degree, including
   local/planned parity, process restart, cleanup, durable page status, public
   freshness, and the internal rollout flag.
3. **PageRank distributed gate**: add the remaining PageRank promotion evidence:
   local/planned parity under production budgets and cross-shard fan-in
   behavior. Fixed-iteration non-converged publish metadata plus direct metric
   top-k, graph traversal, and search-rerank active-rebuild freshness are now
   covered through the process harness.
4. **Algorithm-family rollout**: bring eigenvector to the PageRank
   single-vector matrix, then bring HITS to the stricter paired authority/hub
   matrix before enabling either by default.
5. **Read-surface gate**: prove direct reads, traversal, search/rerank,
   explain/profile, and cross-shard fan-in all honor the same published/fresh
   contract before enabling any distributed metric family by default.
6. **Operations gate**: document and test runtime status, bounded diagnostics,
   immediate v1 cleanup, and future bounded retention controls.

Rest-of-work design plan:

1. **Finish the production process contract before promoting more defaults.**
   The graph metric scheduler already has durable jobs, pages, attempts,
   runtime leases, and role-owned process execution. The remaining process work
   should prove production orchestration, not introduce another executor:
   production worker/coordinator launch, stale worker rejection before
   write/adopt/complete, stale coordinator rejection before publish/fail, and
   final drain to a fresh published generation. Runtime replacement telemetry is
   now covered by `takeover_count` on replacement owners after expired durable
   lease takeover. PageRank now covers the first-iteration scan/out-degree,
   initialize, contribution, reduce, convergence, publish, cleanup, dynamically
   planned later contribution/reduce/convergence process boundaries,
   publish-verifier failure, and same-worker replacement.
2. **Promote by metric family, not by executor feature.**
   Degree is the promotion canary because it exercises planning, scan/reduce,
   publish, cleanup, leases, and stale reads without iterative math. PageRank
   follows because it is the reference single-vector iterative metric.
   Eigenvector should reuse the PageRank runner after PageRank is stable. HITS
   should remain last because paired authority/hub visibility makes failure and
   publish semantics stricter than the single-vector metrics.
3. **Keep local execution as the oracle until each family is boring.**
   Every promoted metric family should keep deterministic local-vs-planned
   parity tests, restart tests, cleanup tests, public freshness tests, and
   failure-preservation tests. Local execution can remain a debug/CI oracle even
   after planned execution becomes the default for production scheduling.
4. **Treat public reads as a promotion gate, not a follow-up.**
   A distributed metric is not done when the worker publishes a score
   generation. It is done when direct metric top-k, traversal projection,
   traversal order/filter, search rerank, explain/profile output, and
   cross-shard fan-in all prove the same generation contract: `published` uses
   the latest complete generation, `fresh` fails with `MetricNotReady` or
   `MetricStale`, and active/failed/unpublished output is never visible.
   Focused public HTTP coverage now proves that contract for not-ready, stale,
   active-build, and failed-build states on direct top-k, traversal projection,
   and search rerank. Fast-root merge coverage now also proves merged direct
   metric top-k and merged rerank profile output carries the merged shard
   generation, status, freshness, and failure details. Hosted cross-range
   coverage now proves direct metric fan-in fails closed when one local shard has
   no published PageRank generation, and hosted cross-range search rerank now
   fails closed on that unpublished shard instead of reranking mixed-generation
   hits. The same hosted fixture now advances the right shard to a different
   published metric generation and proves direct metric top-k, traversal
   projection/status/order/filter, and search rerank all reject incompatible
   published generations. A compatible hosted two-shard fixture now proves
   direct metric top-k, traversal projection/status/order/filter, and search
   rerank merge for both `published` and `fresh` reads when both shards report
   the same nonzero fresh published generation.
   Cross-range traversal metric projection/status/order/filter now carries
   metric payloads through distributed expand and fan-in, reports graph-query
   metric status in query profile, and `fresh` projection fails closed on an
   unpublished shard. The remaining read-gate work is promotion-scale
   cross-shard compatibility for every read surface, plus matching generation
   and freshness evidence for any future standalone explain API. Query profile
   is the current explainable read contract and already reports direct,
   traversal, and rerank metric status.
5. **Make cleanup a durable phase for every family.**
   V1 should keep the aggressive cleanup default: retain the current published
   generation and bounded diagnostics, then delete completed, failed,
   abandoned, unpublished, attempt-scoped, and job-scoped state as soon as
   snapshot safety allows. Future retention can be opt-in and bounded, but it
   should not affect query freshness semantics.
6. **Expose operations state without exposing storage internals.**
   Status should summarize role, owner hash, worker identity count or hash,
   phase, iteration, page counts, cursor presence, attempts, progress, last
   error, lease expiry, and last cleanup sweep. It should not expose raw key
   prefixes or make runtime-owner identities part of the stable public API.

Concrete delivery order:

| Order | Slice | Primary files | Done when |
| --- | --- | --- | --- |
| 1 | Production remote orchestration | maintenance runtime, process launcher, deployment hooks | Production coordinator and worker processes communicate only through durable graph-index job/page state or the equivalent service boundary, drain degree/PageRank work with bounded restarts, and do not rely on shared local file-writer serialization except for the direct DB launch harness. |
| 2 | Degree distributed gate | DB/runtime config, maintenance runtime, tests | Degree can run through planned maintenance by default behind an internal gate with parity, restart, public freshness, durable page status, and cleanup coverage. |
| 3 | PageRank distributed gate | planner/executor/runtime tests | PageRank distributed execution matches local output within tolerance, preserves failed rebuilds, reports fixed-iteration non-converged publish metadata, and enforces public freshness. |
| 4 | Eigenvector backfill | shared iterative executor plus eigenvector math | Eigenvector reuses PageRank's single-vector lifecycle, convergence summaries, failure behavior, cleanup, and promotion gates. |
| 5 | HITS paired-vector hardening | shared iterative executor plus paired HITS publish | Compatible authority/hub output publishes atomically, fails atomically, and preserves the previous compatible pair under process restart. |
| 6 | Public read/fan-in gate | public API/query/search/e2e tests | Every read surface enforces published/fresh semantics and cross-shard comparability. |
| 7 | Operations and retention gate | status APIs, runtime stats, cleanup tests | Status is useful for operators, diagnostics are bounded, and cleanup prevents namespace growth. |
| 8 | Default promotion | internal gates, CI, docs, generated clients | Each family is promoted only after parity, process ownership, crash/restart, cleanup, public-read, fan-in, and operations coverage pass. |

Design boundaries for the rest of the work:

- The graph index owns metric config, target edge generation, build job state,
  page manifests, score generations, freshness, and cleanup.
- Coordinators own job creation, phase barriers, iteration decisions, publish,
  failure, and cleanup scheduling.
- Workers own page execution only. They receive page metadata from durable
  state and must not receive caller-supplied metric config.
- Runtime-owner leases fence long-lived process roles. Page leases and attempts
  fence page execution. Publish is fenced by coordinator ownership and verified
  durable summaries.
- Attempt-scoped output is required when later phases could consume partial
  output. Completed pages adopt output into the job namespace; cleanup removes
  abandoned attempts.
- Fixed-iteration non-converged PageRank and eigenvector output should publish
  by default when the config asked for a bounded run, but status and metadata
  must record `converged: false`, completed iterations, and final deltas.
- Old generations should be cleaned up immediately in v1 once snapshot safety
  allows. A future retention option may keep additional published generations
  or failed diagnostics, but it must be bounded and opt-in.

Completed process-proof slice:

- The top-level `antfly graph-metric-maintenance supervise` path builds under
  the full binary.
- Deterministic supervisor tests drive degree through supervisor-built
  coordinator and worker-pool child argv.
- The integration-test aggregate builds the real `antfly` executable, seeds a
  graph degree DB through the library, runs supervisor mode with spawned child
  role processes, and verifies the degree metric is fresh for the target edge
  generation after child completion.
- The same real spawned-process harness now parses supervisor and launcher
  aggregate summaries and asserts coordinator plus worker-pool child telemetry:
  runtime/owner hashes, worker identity count/hash, lease-key presence, lease
  ownership, acquisition count, tick progress, and no last error.
- The same spawned-process harness now also seeds a bounded eigenvector metric,
  runs it through real supervisor-launched coordinator and worker-pool roles,
  verifies the target eigenvector generation is fresh after cleanup, and covers
  killed scan, initialize, contribution, reduce, and convergence page owners
  being fenced before expiry, reclaimed after expiry by real worker role
  processes, and rejected as stale attempts after replacement. It also covers
  eigenvector publish through a real coordinator role process, cleanup through a
  separate worker role process, and publish-verifier failure preserving the
  prior published eigenvector generation. Eigenvector process coverage now also
  verifies fixed-iteration non-converged publish metadata after process
  execution: `converged: false`, the expected completed iteration count, and a
  positive finite delta.
- Eigenvector process coverage now also starts a dirty rebuild through a real
  coordinator role process and verifies public read freshness: `published`
  direct top-k, traversal, and search rerank serve the prior generation with
  `building` status, while `fresh` direct reads, traversal
  projection/order/filter, and search rerank fail closed with `MetricStale`.
- Eigenvector graph-layer parity now also covers a disconnected/reducible graph:
  local and planned output match within tolerance, scores remain finite and
  L2-normalized, and reducible sink-chain nodes can decay to zero without
  invalid output.
- The same spawned-process harness now also seeds a compatible HITS
  authority/hub pair, explicitly starts the planned authority-side build,
  drains it through real supervisor-launched coordinator and worker-pool child
  roles, verifies both named metrics publish the same fresh target generation,
  and verifies both authority and hub top-k reads return score output.
- HITS process coverage now also kills authority contribution, authority reduce,
  convergence, `hits_hub_contributions`, and `hits_hub_reduce_ranks`
  page-owner processes, verifies replacement workers are fenced before lease
  expiry, verifies real worker role processes reclaim and complete the expired
  pages, rejects stale prior attempts, and drains both authority and hub metrics
  to the same fresh generation.
- HITS process coverage now prepares a compatible authority/hub build to
  `publish_generation`, publishes the pair through a real coordinator role
  process, verifies authority stays in cleanup while hub is fresh/complete with
  the same generation and publish event, and finishes cleanup through separate
  worker role processes until both named metrics are fresh.
- HITS process coverage now corrupts a compatible authority/hub rebuild
  manifest at `publish_generation`, fails publish verification through a real
  coordinator role process, verifies both metrics preserve the previous
  published generation with retained publish-failure diagnostics, and verifies
  prior authority/hub top-k output remains visible.
- HITS process coverage now also starts a dirty compatible authority/hub rebuild
  through a real coordinator role process and verifies public direct metric
  reads: `published` authority and hub top-k queries serve the prior compatible
  generation with `building` status, while `fresh` authority/hub reads fail
  closed with `MetricStale`.
- HITS process coverage now also exhausts a later-iteration hub-reduce page by
  repeatedly killing process owners across expired page leases, then lets a real
  coordinator role fail the compatible pair with
  `GraphMetricBuildPageAttemptsExhausted` while preserving the previous
  authority/hub generation, failure diagnostics, and prior top-k visibility.
- The same process test kills a coordinator after it has acquired a runtime
  lease, verifies a worker-pool process can continue under a separate runtime
  lease, verifies a duplicate coordinator is fenced before lease expiry, and
  verifies a replacement coordinator can take over after expiry and advance
  durable work.
- The process test also kills a degree scan-page owner process after it claims
  and persists cursor progress, verifies a replacement worker process cannot
  reclaim before page-lease expiry, verifies a real worker role process reclaims
  and completes the page after expiry through an injected clock, verifies the
  stale old attempt is rejected, and then drains the metric to a fresh
  generation.
- The process test prepares a degree build to `publish_generation`, closes the
  setup DB handle, publishes through a real coordinator role process, verifies
  cleanup remains resumable with the new generation visible, completes cleanup
  through separate real worker role processes, and verifies the metric returns
  to fresh.
- The process test also seeds PageRank metrics and covers killed process-owned
  PageRank `scan_edges_and_out_degree`, `initialize_ranks`,
  `iterate_contributions`, `reduce_ranks`, and `check_convergence` pages. For
  each phase, it persists cursor progress, verifies a replacement worker is
  fenced before expiry, verifies a real worker role process reclaims and
  completes the page after expiry, verifies the stale old attempt is rejected,
  and drains the PageRank build to fresh.
- The process test now prepares a PageRank build to `publish_generation`,
  publishes through a real coordinator role process, verifies cleanup remains
  visible with the new generation published, completes PageRank cleanup through
  separate real worker role processes, and verifies the metric returns to
  fresh.
- The process test also kills a PageRank cleanup page owner after the new
  generation is published, verifies the killed cleanup page remains fenced
  before lease expiry even while another cleanup worker may complete a
  different page, verifies a real worker role process reclaims the expired
  cleanup page, rejects the stale old attempt, and drains cleanup to fresh.
- The process test now corrupts a PageRank rebuild manifest at
  `publish_generation`, runs a real coordinator role process, verifies publish
  verification fails without publishing, and verifies the prior published
  generation plus retained failure diagnostics remain visible.
- The process test now also kills a PageRank page owner and replaces it after
  lease expiry with a new process using the same worker id, proving the
  replacement attempt can complete while the stale prior attempt from the same
  worker id is rejected.
- The process test now also uses partitioned degree work to prove duplicate
  same-worker runtime fencing through real worker role processes: a duplicate
  owner with the same worker id is fenced before lease expiry, and a replacement
  owner with the same worker id acquires after expiry, reports
  `takeover_count`, and completes additional durable page work.
- The process test now also verifies replacement coordinator takeover reports
  `takeover_count` after acquiring the expired durable runtime lease left by a
  killed coordinator owner.
- The process test also runs PageRank with `max_iterations: 2`, advances through
  a non-final convergence barrier into dynamically planned iteration-1
  contribution, reduce, and convergence pages, kills each page owner, proves
  before-expiry fencing, proves after-expiry reclaim by a real worker role
  process, rejects the stale old attempt, and drains each build to fresh.
- The process test now also verifies fixed-iteration non-converged PageRank
  metadata after process execution: `converged: false`, the expected completed
  iteration count, and a positive finite final delta are preserved after both
  end-to-end process supervision and coordinator-process publish followed by
  worker-process cleanup.
- The process test now also verifies direct metric top-k freshness during a
  process-owned active PageRank rebuild: after a spawned coordinator starts the
  next generation, `published` reads continue to return prior-generation scores
  with `building` status and the active building generation, while `fresh` fails
  closed with `MetricStale`.
- The same process-owned active PageRank rebuild now also covers graph
  traversal projection/order/filter freshness: `published` traversal projection
  serves the prior PageRank score with `building` status, and `fresh`
  projection, ordering, and filtering fail closed with `MetricStale`.
- The same process-owned active PageRank rebuild now also covers search rerank
  freshness: `published` rerank uses prior-generation score details with
  `building` status, while `fresh` rerank fails closed with `MetricStale`.

The next unimplemented slices are:

1. Replace the local supervisor proof with production remote orchestration
   where independently launched coordinator and worker owners communicate only
   through durable graph-index state.
2. Finish degree promotion hardening, then put distributed degree behind an
   internal feature gate.
3. Finish PageRank promotion hardening: extend the new DB-level
   production-budget parity gate into deployment-scale parity and shard fan-in
   behavior.
4. Bring eigenvector to the PageRank single-vector matrix, then bring HITS to
   the paired authority/hub matrix.
5. Prove direct reads, traversal, search/rerank, explain/profile, and
   cross-shard fan-in all honor the same published/fresh contract before
   enabling any distributed metric family by default.
6. Document and test runtime status, bounded diagnostics, immediate v1 cleanup,
   and future bounded retention controls.

Plan the rest of the work around dependency order, not around metric names.
The current implementation has enough executor surface that the next changes
should be release gates:

| Phase | Why it comes next | Work to do | Promotion signal |
| --- | --- | --- | --- |
| A. Freeze the public contract | The UI/API is already the right shape: graph metrics live on graph indexes, users read `published` or `fresh`, and jobs remain internal. Churn here would multiply generated-client and query-path work. | Keep metric config on the graph index, keep freshness on reads, keep `MetricNotReady` and `MetricStale` as the explicit read failures, and document that fixed-iteration non-converged output can publish with `converged: false` metadata. Do not add job handles, worker controls, or per-query metric execution. | Public docs and generated clients expose only stable metric config, status, freshness, convergence, and failure fields. Internal job/lease/page fields stay out of user request bodies. |
| B. Make remote ownership production-shaped | The process harness proves the split role model locally; production still needs the same role contract through deployment-managed owners. | Move from local supervisor proof to deployment hooks for independently launched coordinator and worker processes. Owners receive endpoint or DB location, role, owner id, worker id, tick budget, lease policy, and idle policy. They resolve metric configs only from durable graph-index state. | A coordinator and multiple workers can be started, killed, replaced, and drained without shared memory or caller-supplied metric config. Duplicate owners are fenced by durable runtime leases and stale page attempts. |
| C. Close cross-shard read comparability | Distributed execution is incomplete if shard fan-in can merge incomparable score generations. | Add promotion-scale tests for direct metric top-k, traversal projection/order/filter, search rerank, explain/profile, and shard fan-in across `published`, `fresh`, not-ready, stale, active-build, failed-build, missing-generation, zero-generation, and incompatible-generation cases. | Every score-bearing fan-in either proves all shard scores come from compatible nonzero published generations or fails closed with `MetricNotReady`, `MetricStale`, or an internal unsupported-query error before mixing scores. |
| D. Promote degree first | Degree is the smallest production canary because it exercises scan, reduce, publish, cleanup, page leases, runtime leases, stale attempts, and public freshness without iterative convergence. | Put planned/distributed degree behind an internal gate. Keep local-vs-planned parity, process restart, cleanup resume, active/failed status, page-summary status, and direct/traversal/search freshness tests in CI. | Degree can run by default behind the internal gate with bounded latency, bounded diagnostics, no namespace growth, and a rollback path to the local oracle. |
| E. Promote PageRank as the reference iterative family | PageRank is the single-vector iterative standard that later centrality metrics should inherit. | Keep the DB-level production-budget local/planned parity gate, then finish deployment-scale parity, cross-shard fan-in tests, larger-graph latency evidence, and status/cleanup operations checks. Keep dynamic iteration planning, fixed-iteration non-converged publish metadata, publish verification, failed rebuild preservation, and every phase restart/reclaim test as required checks. | Distributed PageRank matches local output within tolerance, serves prior published generations during rebuilds, fails `fresh` reads while stale, and preserves the previous generation on failed rebuilds. |
| F. Backfill eigenvector through the PageRank substrate | Eigenvector should prove the executor is generic for single-vector centrality, not introduce another lifecycle. | Keep the completed process-boundary, fixed-iteration, active-freshness, disconnected/reducible parity coverage, and DB-level production-budget local/planned parity gate. Add the same cross-shard fan-in, deployment-scale parity, and operations evidence required for PageRank before default promotion. | Eigenvector can be promoted only when its restart, cleanup, failure, freshness, fan-in, and operations matrix is equivalent to PageRank's single-vector matrix. |
| G. Harden HITS last | HITS has stricter semantics because authority and hub are separate named scores sharing one compatible pair lifecycle. | Finish paired local-vs-planned parity, larger-graph latency, remote-owner deployment, public read-surface coverage, fan-in coverage, and pair-level failure/publish/cleanup operations evidence. Preserve attempt-scoped authority/hub output until page completion and publish/fail the pair atomically. | Remote HITS stays disabled until either side failing preserves the previous compatible pair, paired publish is atomic, cleanup is resumable, and paired-vector restart coverage matches the PageRank quality bar. |
| H. Promote defaults one family at a time | Defaulting the executor is a product decision, not just a test result. | Keep local runners as CI/debug oracles. Widen the conservative `auto` gate in this order: degree, PageRank, eigenvector, HITS. Require release history, latency budgets, operations docs, cleanup bounds, and generated-client stability before each widening. | Each family is defaulted only after parity, process ownership, crash/restart, cleanup, public-read, fan-in, status, operations, and latency evidence are routine. |

This roadmap intentionally keeps the user interface stable while the
implementation changes underneath it:

- users configure named metrics on the graph index
- users choose `published` or `fresh` at read time
- users inspect status, convergence, progress, and failure diagnostics
- coordinators and workers remain internal maintenance actors
- retention and retry policy stay internal in v1, with only future bounded
  admin controls considered after distributed execution is stable

The implementation dependency chain should stay strict: remote ownership before
default promotion, cross-shard comparability before any score-bearing fan-in
promotion, degree before PageRank defaults, PageRank before eigenvector
defaults, and HITS only after paired-vector failure behavior is routine.

The remaining roadmap should be delivered as four gates:

1. **Execution gate**: production coordinators and workers are durable-state
   clients. They start, tick, restart, and stop independently; they do not
   exchange metric configs or in-memory phase state. Runtime-owner leases fence
   process roles, page leases fence page execution, and page attempts fence
   output adoption.
2. **Metric-family gate**: each metric family earns promotion separately.
   Degree proves the non-iterative path, PageRank proves the reference
   single-vector iterative path, eigenvector proves the same substrate with
   normalization, and HITS proves paired authority/hub atomic publish.
3. **Read-surface gate**: every user-visible score path reads only published
   generations unless `fresh` is requested, in which case stale or absent
   generations fail closed with `MetricStale` or `MetricNotReady`.
4. **Operations gate**: status and cleanup are production features, not test
   helpers. Status reports progress, failures, ownership counters, and bounded
   diagnostics; cleanup removes completed, failed, abandoned, unpublished, and
   attempt-scoped state promptly while preserving the current published
   generation.

The steady-state architecture should look like this:

```text
graph index metric config
  -> dirty marker for target edge generation
  -> coordinator-owned durable build job
  -> deterministic manifest pages
  -> worker-owned page leases and cursors
  -> attempt-scoped intermediate output where needed
  -> coordinator-owned phase and iteration barriers
  -> verified publish of one complete generation
  -> published generation pointer
  -> resumable cleanup and bounded diagnostics
```

Roadmap from the current state:

| Step | Design target | Implementation plan | Exit gate |
| --- | --- | --- | --- |
| R1. Production execution contract | Keep jobs invisible to users. A user configures a named graph-index metric and chooses `published` or `fresh`; coordinators and workers are internal maintenance actors. | Freeze the process role contract around endpoint/DB location, role, owner identity, worker identity, budgets, clock, and idle policy. Move launch/orchestration behind deployment hooks while keeping metric config, target generation, manifests, pages, attempts, and publish decisions inside durable graph-index state. | Independently started coordinator and worker owners can drain degree and PageRank from dirty state to fresh publish without shared memory, caller-supplied metric config, or local test-only process coupling. |
| R2. Degree canary | Degree is the first default distributed family because it exercises scan, reduce, publish, cleanup, ownership, and freshness without iterative convergence. | Promote degree behind an internal distributed gate. Keep local/planned parity tests, active-build freshness tests, failed-build preservation, page-status summaries, cleanup resume, stale attempt rejection, and process replacement coverage. | Degree can be enabled for production maintenance with bounded budgets, durable status, direct/traversal/search freshness behavior, and cleanup that does not grow namespaces. |
| R3. PageRank promotion | PageRank is the reference single-vector iterative metric and the standard for later centrality families. Fixed-iteration non-converged output publishes when requested, with `converged: false` metadata. | Keep the DB-level production-budget parity proof, then finish cross-shard fan-in behavior, public read-surface coverage, deployment-scale parity, and latency evidence. Keep dynamic iteration planning, convergence summaries, publish verification, failed rebuild preservation, cleanup, and process restart coverage as required promotion checks. | PageRank distributed output matches local output within tolerance, serves prior published generations during rebuilds, fails `fresh` reads closed when stale, and preserves the prior generation on failed rebuilds. |
| R4. Eigenvector parity | Eigenvector proves the executor is generic for single-vector iterative metrics. It should reuse PageRank's lifecycle, errors, status, and cleanup rather than adding a metric-specific job system. | Keep the completed process-boundary coverage for page restart/reclaim, publish, cleanup, publish failure, fixed-iteration non-converged publish metadata, and active public freshness across direct top-k, traversal, and search rerank. Keep graph-layer local-vs-planned parity for normalization plus disconnected/reducible graphs. Backfill cross-shard fan-in and any production process evidence still needed for default promotion. | Eigenvector can be promoted only after its restart/failure/cleanup/public-read matrix is equivalent to PageRank's single-vector matrix. |
| R5. HITS paired-vector hardening | HITS remains two named metric scores, authority and hub, but one compatible pair for target generation, convergence, failure, publish, and cleanup. | Keep the completed process-owner coverage for explicit authority and hub contribution, reduce/norm, convergence, publish, cleanup, reclaim, exhausted-attempt, publish-failure, and stale-read behavior. Active and failed public freshness now cover direct top-k, traversal projection/order/filter, and search rerank across coordinator-owned process boundaries, fan-in rejects incompatible default authority/hub status pairs before merging public results, and hosted cross-range tests now prove compatible paired HITS `published`/`fresh` direct metric, traversal projection/order/filter/status, and rerank fan-in plus unpublished/mixed-generation failure. Finish local-vs-planned parity, larger graph latency, remote-worker deployment, and promotion-scale cross-shard fan-in coverage before promoting HITS. Keep authority/hub output attempt-scoped until page completion and publish the pair atomically. | Remote HITS stays disabled by default until either side failing preserves the previous compatible pair, paired publish is atomic, cleanup is resumable, and paired-vector restart coverage matches the PageRank quality bar. |
| R6. Public read and fan-in gate | A metric is not production-ready until every score-bearing read path enforces the same generation contract. | Keep the new hosted paired-HITS `published`/`fresh` cross-range evidence while continuing to cover direct metric top-k, traversal projection/order/filter, search rerank, query profile, any future standalone explain surface, and cross-shard fan-in for `published`, `fresh`, not-ready, stale, active-build, failed-build, incompatible-generation, and incompatible paired-HITS status cases. | `published` always uses the latest complete generation; `fresh` returns `MetricNotReady` before first publish and `MetricStale` when the published generation is behind; fan-in rejects missing, zero, stale, or incompatible generations when comparability is required. |
| R7. Operations and cleanup | V1 cleanup is immediate and aggressive; retention is a future bounded admin option, not part of query semantics. | Make cleanup resumable for score generations, manifests, pages, phase summaries, iteration summaries, attempt namespaces, failure diagnostics, and runtime-owner records. Expose summarized status: phase, iteration, page counts, attempts, cursor presence, progress, last error, owner hash, worker identity count/hash, lease expiry, and takeover counters. | Cleanup never deletes the current published generation, bounds diagnostics, resumes after restart, and prevents graph-metric control/output namespaces from growing without a retention knob. |
| R8. Default promotion | Promote by metric family, not by executor milestone. Local runners remain deterministic CI/debug oracles until distributed parity is routine. | Keep the conservative `auto` gate, collect latency evidence, document operations behavior, update public docs and generated clients only for stable user-facing fields, and widen defaults one family at a time: degree, PageRank, eigenvector, then HITS. | A family becomes default only after parity, process ownership, crash/restart, cleanup, public-read, fan-in, status, operations, and latency-budget evidence all pass. |

The user-facing interface should stay small for this entire roadmap:

```json
{
  "metrics": {
    "pagerank": {
      "kind": "pagerank",
      "refresh": "background",
      "edge_filter": { "types": ["cites"] },
      "max_iterations": 20,
      "tolerance": 0.000001
    }
  }
}
```

Reads should continue to express freshness as a read policy, not as a job
handle:

- `published`: return the latest complete generation and include status when
  requested, even if a newer rebuild is queued, building, or failed.
- `fresh`: require the current edge generation. Return `MetricNotReady` before
  first publish and `MetricStale` when a prior generation exists but is behind.

The internal interface should stay explicitly different from the public
interface. Coordinators create jobs, append phase pages, make iteration and
convergence decisions, verify publish preconditions, fail builds, and schedule
cleanup. Workers claim pages, renew leases, write cursor progress, write
attempt-scoped output where needed, and complete or fail pages. Workers do not
receive metric configs, create jobs, advance phases, publish generations, or
decide pair-level HITS failure.

The main non-negotiable invariant for the rest of the work is that workers
execute pages and coordinators publish generations. Workers never receive
caller-supplied metric configs, never create jobs, never advance phases, never
publish, and never decide that a whole build has failed. Any stale process that
lost its runtime lease or page attempt must fail closed before writing,
adopting, completing, or publishing output.

The planned PageRank runner has moved past the original single-worker
checkpoint. The remaining PageRank work should keep the local runner as an
oracle while hardening the planned lifecycle under restart, retry, cleanup, and
eventual distributed worker execution:

- `iterate_contributions` reads iteration `n` ranks and aggregate out-degree,
  scans the scoped reverse-edge range, and writes deterministic contribution
  partials keyed by metric, job id, iteration, target node, and page id. This
  executor now exists for first-iteration planned PageRank pages.
- `reduce_ranks` aggregates contribution partials for a target-node range,
  applies teleport/base mass and sink handling exactly like the local runner,
  writes iteration `n + 1` ranks, and records page-level rank summaries. This
  executor now exists for first-iteration planned PageRank pages.
- `check_convergence` compares iteration `n` and `n + 1` ranks for each node
  range, writes durable page summaries for `max_delta`, `total_delta`,
  `rank_sum`, `score_count`, and `converged`, and leaves iteration advancement
  to the coordinator. This executor now exists, and non-final check barriers now
  drive dynamic PageRank iteration advancement.
- Dynamic iteration planning creates the next contribution, reduce, and check
  pages only after the convergence barrier says another iteration is required.
  This now exists for planned PageRank.
- Verified publish copies or adopts the final rank generation into public score
  storage only after all PageRank pre-publish phases and the publishable
  iteration summary verify. Fixed-iteration planned PageRank publish now
  materializes durable rank state into public score output after dynamic
  iteration advancement.
- The active planned PageRank runner now drives prepare, scan, initialize,
  contribution, reduce, check, publish, and cleanup without test-only manual
  page completion, and deterministic local-vs-planned parity coverage exists.
- Planned PageRank can now resume after graph-index reopen once a non-final
  check barrier has dynamically planned the next iteration.
- Cleanup removes PageRank job namespaces for out-degree, node membership,
  rank, contribution, reduce, phase summary, iteration summary, manifest, page,
  and abandoned score records without deleting the current published
  generation. Completed planned PageRank jobs now run through the generic
  cleanup phase after publish with separate durable pages for out-degree
  partials, node membership partials, and final job namespace removal. Failed
  planned builds now remove unpublished score generations and job namespaces
  while preserving compact failed status and bounded recent failure diagnostics,
  and public PageRank failure coverage verifies that the prior published score
  generation remains visible after a failed rebuild.

That PageRank slice is complete when planned PageRank can rebuild from dirty
state under production worker ownership, survive restart at every phase
boundary and iteration boundary, publish the same scores as local PageRank
within tolerance, and preserve the previous published generation on failure.

The second checkpoint is making the planned runner a real distributed runner.
At that point the metric math should already be correct, so the work is mostly
scheduling and recovery:

- workers claim only durable pages, never phases
- the coordinator is the sole phase, iteration, publish, and failure authority
- expired leases are reclaimed with bounded attempts
- page progress, completion, and failure validate the durable attempt as well as
  the worker id so same-worker replacement owners fence stale prior attempts,
  including stale idempotent completion after the replacement attempt completes
- same-worker renewals resume from cursor when possible
- cross-worker recovery can recompute page output from the range start unless
  attempt-scoped adoption has been added for that phase
- status reports active pages, attempts, lease owners, cursors, phase progress,
  convergence, and last error through stable user-facing fields

Attempt-scoped output adoption is required for phases whose partial output can
be consumed by a later phase before the page is complete. It also becomes
worthwhile when recomputing a large page is too expensive or when a phase writes
many intermediate keys that are hard to replace atomically. Keep it as an
internal executor capability, not as a public API feature. Degree scan and
iterative contribution pages now implement this capability: same-worker
progress writes to the current attempt namespace, later phases read only
adopted job-scoped partials, page completion adopts the attempt output, and
final cleanup removes abandoned attempt keys with the rest of the job namespace.

The third checkpoint is promoting cleanup and retention from best-effort
housekeeping into a first-class graph metric phase. V1 should keep aggressive
cleanup as the default: one latest published generation, completed job
namespaces removed immediately when snapshot safety allows, and bounded failed
job diagnostics. A future retention option can be added for debugging, but it
should be opt-in, bounded, and independent from the metric query contract.

The fourth checkpoint is reusing the iterative runner for non-PageRank metrics.
Eigenvector centrality should be first because it exercises normalization and
non-convergence while still publishing one score vector. HITS should follow
because it requires paired authority/hub output and atomic paired publish.
Neither should add a new query surface; they should be named graph metrics with
metric-specific config, metadata, and convergence behavior.

The final checkpoint is product promotion:

- planned degree and planned PageRank become default behind an internal gate
- local-vs-planned parity runs in CI for small deterministic graphs
- multi-worker restart and failure coverage runs before enabling remote workers
- graph traversal/search projection, ordering, filtering, and rerank e2e tests
  cover `published`, `fresh`, `MetricNotReady`, and `MetricStale`
- cross-shard top-k and rerank paths verify comparable published generations
  or fail closed
- public docs describe graph metric status, freshness, convergence metadata,
  non-converged publishes, cleanup defaults, and failure preservation

After those checkpoints, the graph index metric system is complete enough to
support new centrality algorithms as metric plugins on the same lifecycle
instead of as custom rebuild code.

#### Remaining Architecture

The rest of the implementation should be organized around four explicit
contracts: planner, page executor, coordinator, and cleanup runner.

Planner responsibilities:

- resolve the current metric config and target edge generation
- create an idempotent manifest for one metric/job/generation
- create stable page ids and page ranges for every planned phase
- estimate `total_units` for status without requiring exact load balance
- refuse to reuse a manifest whose config fingerprint no longer matches
- plan future iteration pages only after convergence says another iteration is
  required

Page executor responsibilities:

- claim only leased pages assigned to the worker
- read page range and cursor metadata from durable page records
- write deterministic output keys scoped by metric, job, generation, phase, and
  iteration
- periodically persist cursor and completed unit progress
- complete pages with output fingerprints and page-level summaries
- make retries safe by overwriting the same output key range or by writing into
  an attempt namespace that is atomically adopted on completion

Coordinator responsibilities:

- acquire or resume the active job for each dirty metric
- decide the next phase only from durable summaries, not in-memory worker state
- summarize completed pages into phase and iteration summaries
- plan the next phase or next iteration after a successful barrier
- run verified publish in the same transaction as the published pointer flip
- mark failed jobs without hiding the previous published generation
- expose user-facing status from job, manifest, page, phase, and iteration
  records

Cleanup runner responsibilities:

- delete old published score generations after snapshot safety allows
- delete intermediate job namespaces for completed and failed jobs
- delete abandoned building generations that never published
- preserve bounded diagnostic records for recent failed jobs
- resume cleanup after restart without requiring the original build worker

The first production version should keep these contracts internal. The public
API should remain metric config, freshness selection, score reads, graph query
projection/order/rerank, and status. Operational controls such as explicit
pause, resume, retry policy, and retention can be added after the distributed
runner is proven.

#### Implementation Stages

**Stage A: Planned Degree Reduce**

`degree` is the non-iterative proof of the distributed execution model. Scan
pages no longer write final score generation entries directly. They write
deterministic partials under the job namespace, keyed by metric, job id, node,
and page id. A `reduce_ranks` page aggregates partials into the building score
generation.

This keeps retry behavior simple:

- retrying a scan page overwrites that page's partial keys
- retrying reduce recomputes final scores from completed partials
- verified publish only sees final score generation output
- cleanup can remove the whole completed job namespace after publish

Stage A exit criteria:

- planned degree includes `prepare_generation`, `scan_edges_and_out_degree`,
  `reduce_ranks`, `publish_generation`, and `cleanup_old_generations`
- one-page planned degree still matches local degree output
- two or more scan pages can contribute to the same node and reduce to the
  correct final degree
- scan-page retry does not double-count
- verified publish requires scan and reduce barriers before flipping the
  published generation

**Stage B: Range-Partitioned Manifests**

Replace one-page manifests with deterministic key-range pages. Degree scan
planning now uses contiguous ranges over encoded reverse-edge keys, degree
reduce planning now uses deterministic node ranges, degree cleanup uses durable
prefix pages, and initial PageRank manifests use the same reverse-edge and node
ranges for scan, initialize, contribution, reduce, and convergence phases. The
remaining partitioning work is dynamic iteration-specific PageRank pages after
each convergence barrier. Exact load balance is less important than stable page
ids and restart-safe replay.

Manifest/page schema already has the right shape and should be used as the
contract:

- opaque lower and upper range bounds
- range kind, such as `reverse_edges`, `nodes`, `scores`, `contributions`, or
  `job_control`
- planned phase and iteration
- output namespace prefix
- cursor, completed units, and total units

The planner should be idempotent for the same metric config fingerprint and
target edge generation. A changed config fingerprint should make the old plan
ineligible instead of mutating it in place.

Stage B exit criteria:

- reopening the index resumes the same manifest and page ranges
- planning the same metric/target generation twice is idempotent
- scan pages cover the intended reverse-edge keyspace without overlap gaps
- reduce pages cover the intended node/score keyspace without overlap gaps
- status reports page progress without exposing raw key bytes

**Stage C: Resumable Page Progress**

Long pages should persist cursors often enough that lease renewal can continue
from the last durable point. Degree scan pages now have the first implementation
of this contract: they persist an opaque reverse-edge key cursor, resume after
that key on same-worker renewal, and merge newly scanned partial counts into the
same page's job-scoped partial output. Cross-worker recovery for degree scans
recomputes from the page range start after lease reclaim and overwrites the same
page partial keys without double-counting. Attempt-scoped output adoption can
come later if recompute cost is too high.

Cursor rules:

- same-worker renewal resumes from the persisted cursor
- expired-lease reclaim increments attempt count
- a reclaimed scan or reduce page may recompute from the range start
- completed pages are immutable except for idempotent same-fingerprint
  completion
- exhausted attempts mark the page failed and make the phase barrier fail

Stage C exit criteria:

- killing a worker during degree scan or reduce either resumes or safely
  recomputes
- page progress is durable across graph-index reopen
- failed pages include metric, job id, phase, iteration, page id, worker id,
  attempt, and last error
- `published` reads continue to serve the previous generation while a new
  generation is building
- `fresh` reads fail with `MetricStale` until the new generation publishes

**Stage D: Generic Page Executor And Worker Loop**

Generalize the degree-specific worker-step loop into a registry keyed by metric
kind and phase. Workers should only claim pages and run executors. The
coordinator should remain the only authority for phase summaries, iteration
decisions, publish, failure, and cleanup.

The first version of this split now exists for planned degree. The degree API
still exposes the same planned runner, but internally it delegates to a generic
worker step that loads the durable job, routes executable phases through a
metric/phase page executor, and keeps `publish_generation` as coordinator work.
`prepare_generation`, `scan_edges_and_out_degree`, `reduce_ranks`, and
`cleanup_old_generations` return a shared page execution result shape so
PageRank can add its own phase executors without adding another worker loop.

Worker loop:

```text
claim eligible page
load executor for metric kind and phase
execute page range
persist progress cursor
complete page with fingerprint and summary
repeat
```

Executor result:

```text
PageExecutionResult {
  completed_units,
  total_units,
  output_fingerprint,
  optional max_delta,
  optional total_delta,
  optional rank_sum,
  optional score_count,
  optional next_cursor
}
```

The coordinator interprets summaries by phase. Page executors should not decide
that a job is publishable.

Stage D exit criteria:

- degree uses the generic worker loop without hardcoded page ids
- the retry policy is internal and bounded
- duplicate coordinator ticks cannot advance incompatible phase state
- status exposes phase, iteration, completed pages, total pages, active leases,
  attempts, cursor, and last error

**Stage E: Durable PageRank Executor**

Move PageRank onto the same runner using durable intermediate namespaces. This
is the first iterative metric on the planned executor and should preserve the
existing local PageRank semantics exactly.

The initial PageRank manifest now creates partitioned durable pages for the
first scan, initialize, contribution, reduce, and convergence phases. The first
scan/out-degree executor writes job-scoped out-degree partials and node
membership keys through the generic page lease path. The initialize executor
reloads durable page range metadata, derives the filtered node set from scan
partials, writes aggregate out-degree state, and writes iteration-0 ranks. The
contribution executor reads iteration ranks plus aggregate out-degree and
writes attempt-scoped durable edge contribution partials that become
reduce-visible only after page completion adopts the attempt output. Scan,
contribution, reduce, and convergence pages can persist cursor progress;
contribution pages now resume after graph-index reopen and merge same-worker
partial output into the current attempt on initial and later iterations. The
reduce executor applies the local PageRank base, sink, and contribution
formula, writes iteration `n + 1` ranks, and now resumes node-range progress
after reopen on initial and later iterations. The convergence executor compares
adjacent rank iterations,
writes durable `max_delta`, `total_delta`, `rank_sum`, and convergence
summaries, and now resumes partial convergence progress after reopen before
completing the phase barrier on initial and later iterations. Non-final checks
now plan later contribution/reduce/check pages; fixed-iteration planned publish
materializes final rank state into public score storage and runs generic job
namespace cleanup. Expired scan, contribution, and reduce pages now have reclaim
coverage proving stale partial out-degree, node membership, contribution, and
rank output is overwritten instead of accumulated. The remaining work is to
broaden parity, cleanup, and failure coverage for full planned PageRank
generations.

Required phases:

```text
prepare_generation
scan_edges_and_out_degree
initialize_ranks
iterate_contributions
reduce_ranks
check_convergence
publish_generation
cleanup_old_generations
```

Required intermediate namespaces:

```text
metric_out_degree/<metric>/<job_id>/<source_node>
metric_rank/<metric>/<job_id>/<iteration>/<node>
metric_contribution/<metric>/<job_id>/<iteration>/<target_range>/<source_node>
metric_reduce/<metric>/<job_id>/<iteration>/<target_node>
metric_score/<metric>/<score_generation>/<node>
```

Iteration control:

- `check_convergence` aggregates page summaries into a durable iteration
  summary
- if converged, the coordinator advances to publish
- if not converged and iterations remain, the coordinator plans the next
  contribution/reduce/check pages
- if `max_iterations` is reached, publish fixed-iteration output with
  `converged: false`

Stage E exit criteria:

- planned PageRank matches local PageRank within documented tolerance
- restart coverage exists for every phase and iteration boundary
- convergence summaries drive every iteration transition
- invalid score output fails the job and preserves prior published scores
- `MetricNotReady`, `MetricStale`, `published`, and `fresh` behavior is
  unchanged

**Stage F: Coordinator-Owned Publish And Cleanup**

Workers complete pages; the coordinator publishes. The publish transaction must
verify active job, manifest, summaries, page fingerprints, config fingerprint,
score metadata, and convergence metadata before flipping the published pointer.

Cleanup should be aggressive in v1:

- keep the latest published generation
- delete older generations when snapshot safety allows
- delete completed job manifests, pages, phase summaries, iteration summaries,
  contributions, reduces, ranks, out-degree state, and abandoned building score
  namespaces
- preserve only bounded diagnostic records for recent failed jobs

Planned PageRank now uses prefix-partitioned cleanup for the completed job path:
one page removes out-degree partials, one page removes node membership partials,
and the final page removes the remaining job namespace and clears the active
lease. Restart coverage now verifies that a build can stop after a non-final
cleanup page, reopen with the published generation still queryable, and finish
the remaining cleanup pages. Large cleanup prefixes now persist a cursor and can
resume after reopen before the cleanup page is marked complete. Failed planned
build cleanup now removes unpublished building score generations, deletes the
job namespace, clears the active lease, and preserves a compact failed job
record plus bounded recent failure diagnostics for status. Larger
attempt-scoped output namespaces still need the same treatment.

Stage F exit criteria:

- crash before publish leaves the old generation visible and can resume publish
- crash after publish but before cleanup leaves the new generation visible and
  cleanup resumes later
- cleanup never deletes the currently published generation
- failed builds preserve the prior published generation
- failed planned builds delete unpublished score output and active job keys
  while retaining compact failure diagnostics
- completed and failed job namespaces do not grow without bound

**Stage G: Multi-Worker Scheduling**

Enable production worker ownership only after the planned executor and
in-process split-runtime contract are stable. The current DB graph-metric
runtime already provides the bridge: it can tick the same durable
planned-maintenance primitive as `combined`, `coordinator`, `worker`, or
`worker_pool`, starts from DB open when enabled, wakes from derived-apply
notifications, reports role/owner/worker telemetry, can publish through
separate coordinator and worker loops without manual tick calls, and can also
be initialized from `OpenOptions.graph_metric_maintenance` with
`start_background_loop = false` so process-style tests can drive explicit
runtime ticks from fresh DB handles for degree, PageRank, eigenvector, and
compatible HITS. The new `antfly graph-metric-maintenance` command exposes that
same configured runtime path as an executable process role with bounded polling
and idle-exit controls, and its `supervise` mode drives coordinator and
worker-pool child role processes in bounded rounds, parses their durable
progress summaries, exits after global idle, and applies bounded restart
policy. The full binary command path now builds, and command-level tests drive
degree through supervisor-built coordinator and worker-pool child argv against
one shared DB path. The integration-test aggregate now runs the
real binary through supervisor mode with spawned child roles and verifies that
degree, PageRank, eigenvector, and compatible HITS publish fresh output through
durable DB state only. The same process harness now kills runtime and page
owners across coordinator, worker, and worker-pool roles; proves duplicate
owners are fenced before lease expiry; proves replacements take over after
expiry; rejects stale page attempts after replacement; verifies publish,
failure, cleanup, fixed-iteration, exhausted-attempt, and same-worker fencing
invariants; and emits a final `graph_metric_process_harness_summary` only after
all required owner-boundary coverage categories are observed. The
full-default workflow runs that process harness through the ordinary
`integration-test` aggregate. Release-sized public-read, fan-in,
retained-storage, scheduler, and latency evidence remains rollout readiness
work rather than another graph-only CI target.

Remote workers should claim independent pages from durable graph-index state by
index and metric name. They should persist cursors, complete or fail leases,
and stop. They should not receive metric configs from callers, create jobs,
advance barriers, fail active builds, publish generations, or clean completed
jobs outside cleanup pages. Coordinators should start and resume jobs, advance
phase and iteration barriers, append dynamic iteration pages, verify publish,
mark failed builds, and schedule cleanup based only on durable summaries.
Retry, lease, backoff, and attempt limits should remain internal defaults in
the first version.

Stage G exit criteria:

- independently owned coordinator and worker processes can finish one metric
  generation without duplicate publish attempts
- worker loss abandons only the leased page, not the build
- another worker can reclaim an expired page lease and either resume from
  cursor or recompute without stale partial output
- duplicate or racing coordinators cannot publish conflicting generations or
  append incompatible phase pages
- status and runtime telemetry show role, owner, phase, iteration, page
  progress, attempt, cursor presence, and last error without exposing raw key
  ranges

**Stage H: Eigenvector And HITS**

Eigenvector centrality and HITS should reuse the iterative executor rather than
adding another job system. Eigenvector proves the executor is not PageRank
specific. HITS proves paired vector materialization and atomic paired publish.

Current state: eigenvector has a planned single-vector runner with local parity,
durable contribution/reduce cursor resume after reopen, reclaimed
contribution/reduce stale-output overwrite coverage, and later-iteration failed
contribution/reduce/convergence page retry through the generic worker step.
Planned eigenvector cleanup can also resume after reopen while published scores
remain visible, and failed planned eigenvector rebuilds preserve the previous
published generation. HITS has a first planned paired-vector runner that
computes authority and hub ranks from durable job state, publishes compatible
authority/hub generations atomically, matches the local runner on deterministic
coverage, resumes authority and hub contribution/reduce cursor progress after
reopen, and retries
failed later-iteration contribution, reduce, hub contribution, hub reduce, and
convergence pages through the generic worker step. Later-iteration exhausted hub
reduce attempts fail the compatible pair while keeping the previous
authority/hub generations visible. Failed planned HITS rebuilds also mark the
compatible pair failed together, and reclaimed contribution/reduce pages
overwrite stale durable authority/hub job output while reclaimed convergence
pages reset stale partial summaries. The planned HITS phase model now also
separates hub contribution and hub reduce/norm from authority reduce/norm: hub
raw output is written through attempt-scoped hub contribution pages and adopted
only after page completion, then hub reduce pages normalize the adopted raw
namespace. The planned cleanup page can also resume after reopen with the
published pair still visible. The remaining work is not a new job system; it is
production hardening of the same runner shape, especially phase-specific HITS
restart/failure coverage under the new hub phases.

Eigenvector requirements:

- reuse scan, initialize, iterate, reduce, check, publish, and cleanup
- persist normalization metadata per iteration
- document behavior for disconnected and reducible graphs
- publish fixed-iteration non-converged output with `converged: false`

HITS requirements:

- compute authority and hub vectors over the same target generation and edge
  scope
- persist paired manifests or one manifest with paired output metadata
- publish authority and hub together when configured as a compatible pair
- fail the pair together if one side has invalid output, while preserving the
  previously published pair
- partition paired-vector work fully, including hub reduction work that is still
  broader than the authority contribution pages in the first planned runner
- broaden restart, lease-expiry, reclaimed-output, publish-failure, and cleanup
  coverage across HITS prepare, scan, initialize, contribution, reduce,
  convergence, publish, and cleanup
- keep reclaimed contribution/reduce coverage as the minimum proof for stale
  paired-vector output overwrite, and convergence-summary reset coverage as the
  minimum proof for stale convergence metadata, before adding broader publish
  and cleanup reclaim tests
- keep cleanup cursor-resume coverage as the minimum proof that partially
  cleaned HITS job namespaces can finish after restart without hiding the
  published pair

Stage H exit criteria:

- at least one non-PageRank iterative metric reuses the distributed executor
- HITS paired publish preserves atomic visibility
- local and distributed outputs match within documented tolerance
- paired-vector restart and retry tests preserve the previous published pair,
  including failed-page retry and planned-build failure cases
- no new public query/status API is required

**Stage I: Promotion**

Rollout should be controlled by internal gates and parity checks:

1. keep local runners as deterministic CI/debug oracles
2. keep explicit planned maintenance and the conservative `auto` gate for
   already-active planned work, queued degree, small queued PageRank, bounded
   queued eigenvector, and internally opted-in compatible HITS
3. measure and promote latency-safe idle budgets for broader default
   planned-maintenance scheduling
4. run local multi-worker and split-runtime coverage for every promoted metric
   family
5. enable real remote workers for degree and PageRank behind internal gates
6. broaden eigenvector restart/failure parity until it matches PageRank's
   single-vector matrix
7. finish HITS hub phase restart/failure coverage
8. enable remote/distributed workers for large graph indexes per metric family
9. demote local runners only after planned/distributed parity, restart,
   cleanup, freshness, operations, and cross-shard tests pass

Promotion exit criteria:

- public e2e tests cover direct metric top-k, graph projection, graph ordering,
  graph search rerank, status, and freshness
- failed builds preserve published scores across restart
- dirty markers survive restart and eventually rebuild
- cleanup prevents unbounded growth of failed/intermediate job keys
- cross-shard metric fan-in either proves comparable nonzero generations or
  fails closed
- generated clients and public docs describe metric status, freshness,
  convergence, fixed-iteration non-converged output, cleanup defaults, and
  failure preservation

#### 1. Verified Publish Gate

The metadata-only publish verifier now exists as the first half of this slice.
It does not yet need fully distributed score execution. Its job is to make the
final pointer flip depend on the manifest, phase summaries, page records,
iteration summaries, score metadata, and current metric config.

Before publishing a distributed build, verify:

- the active build job is at `publish_generation`
- the active build job and manifest agree on `job_id`, target edge generation,
  score generation, planned phase/page counts, and config fingerprint
- the manifest's target generation is still eligible to publish for the current
  resolved metric config
- every required pre-publish phase has a complete phase summary
- every planned page for those phases is complete and has a stable output
  fingerprint
- iterative metrics have a complete convergence summary for the publishable
  iteration
- fixed-iteration non-converged metrics record `converged: false`,
  `iterations_completed`, and the final delta values
- score metadata has valid counts and no NaN or infinity
- paired publishes, such as HITS authority/hub, have compatible manifests and
  publish in one transaction

The remaining work in this slice is to attach that verifier to the publish
transaction. The transaction should then write the same public metadata as the
local runner:

- score metadata
- edge-filter metadata
- config fingerprint metadata
- published generation pointer
- publish event
- cleanup eligibility records for old generations and intermediate job keys
- completed job status

Exit criteria:

- Query readers either see the old generation or the new generation, never a
  mix.
- A crash after score writes but before publish resumes verification and either
  publishes or leaves the old generation visible.
- A crash after publish but before cleanup leaves the new generation queryable
  and schedules cleanup on restart.
- Cross-shard top-k and rerank fan-in continue to fail closed unless shard
  responses report comparable nonzero published generations.

#### 2. Partition Model

The current manifest intentionally plans one page per phase. That is enough to
prove durable metadata, but not enough for scale. The next planner version
should create stable key-range pages over graph edge and node namespaces for a
target edge generation.

Initial partitioning should be simple and deterministic:

- edge scan pages are contiguous ranges over encoded edge keys
- node/rank pages are contiguous ranges over encoded node ids discovered during
  scan
- contribution pages are keyed by target node range and iteration
- reduce pages aggregate one target range at a time
- cleanup pages cover score, contribution, reduce, and job-control key ranges

The planner does not need perfect load balance in v1. It needs stable
boundaries, restart-safe page ids, and progress counters that can be explained
in status. Later versions can split hot pages or rebalance based on observed
unit counts.

Exit criteria:

- Reopening the graph index resumes the same manifest and page boundaries.
- Planning the same metric/target generation twice is idempotent.
- A config fingerprint change makes the old plan ineligible and schedules a new
  target generation.
- Status reports phase, iteration, page counts, cursor, completed units, total
  units, and lease owner without exposing raw internal key ranges.

#### 3. Executable Degree Job

`degree` is the first metric to run through the planned coordinator. It is
non-iterative, cheap to validate against the existing local runner, and
exercises the same publish and cleanup invariants without PageRank's iteration
loop.

The degree job should execute:

```text
prepare_generation
scan_edges_and_out_degree
reduce_ranks
publish_generation
cleanup_old_generations
```

The first implementation can still run with one local worker, but it should use
the same partial/reduce contract needed for many scan pages. Scan pages write
job-scoped partial degree counts. The reduce page aggregates completed partials
and writes final score entries into the building score generation. Retrying a
scan page overwrites that page's partial keys; retrying reduce recomputes final
scores from the completed partial namespace.

Exit criteria:

- Single-worker planned degree output matches local degree output.
- Multiple scan pages can contribute partial counts for the same node without
  double-counting.
- Killing the worker during scan or reduce retries the page and produces the
  same published generation.
- Publish verification requires both scan and reduce phase summaries.
- `published` reads continue to serve the old generation while the job is
  active.
- `fresh` reads fail with `MetricStale` until the new generation publishes.
- Cleanup removes old degree score generations, completed partials, and stale
  job keys safely.

#### 4. Executable PageRank Job

After degree proves the coordinator, PageRank should move onto the same page
executor path. PageRank needs durable intermediate storage because each
iteration reads the previous rank state and writes contribution/reduce output
before convergence can be checked.

Required intermediate namespaces:

```text
metric_score/<metric>/<score_generation>/<node>
metric_out_degree/<metric>/<job_id>/<source_node>
metric_rank/<metric>/<job_id>/<iteration>/<node>
metric_contribution/<metric>/<job_id>/<iteration>/<target_range>/<source_node>
metric_reduce/<metric>/<job_id>/<iteration>/<target_node>
```

Required phases:

```text
prepare_generation
scan_edges_and_out_degree
initialize_ranks
iterate_contributions
reduce_ranks
check_convergence
publish_generation
cleanup_old_generations
```

`check_convergence` reduces page-level summaries into the durable iteration
summary. If the summary is converged, the job moves to publish. If it is not
converged and another iteration is allowed, the coordinator plans the next
iteration's contribution/reduce/check pages. If `max_iterations` is reached, it
publishes bounded fixed-iteration output with `converged: false`, matching the
current local behavior.

Exit criteria:

- Single-worker planned PageRank output matches local PageRank within the
  documented floating-point tolerance.
- Restart during every phase either resumes or safely retries the current page.
- No phase can advance until every required page in that phase is complete.
- Invalid score output fails the job and leaves the prior published generation
  visible.
- Non-converged fixed-iteration output publishes with complete convergence
  metadata.

#### 5. Multi-Worker Coordinator

Once degree and PageRank work with one planned worker, the coordinator can allow
multiple workers to claim pages concurrently. Workers should operate on page
leases only; the coordinator owns phase summaries, iteration decisions, publish,
failure, and cleanup.

Worker loop:

```text
claim next eligible page
execute page with deterministic output keys
update cursor and completed units during long work
complete page with output fingerprint
coordinator summarizes phase
coordinator advances phase, plans next iteration, publishes, or fails
```

Recovery rules:

- expired leased pages can be reclaimed after lease timeout
- page attempt increments on explicit failure or expired-lease reclaim
- completed page output is idempotent by fingerprint
- repeated page failures mark the job failed after a bounded internal policy
- failed jobs preserve the previous published generation and expose page-level
  failure details

The first retry policy should stay internal:

```text
max_page_attempts = 3
lease_timeout_ms = existing local lease timeout initially
backoff = fixed small delay or coordinator tick interval
```

Do not expose retry policy as user configuration in the first distributed
version. It can become an internal tuning option after the behavior is stable.

Exit criteria:

- Multiple workers can finish one build without duplicate publish attempts.
- Worker loss only abandons a lease, not the generation.
- Build progress is monotonic within a phase except after lease recovery, where
  retry behavior is visible in status.
- Failure status includes phase, page id, attempt, worker id, and last error.

#### 6. Eigenvector and HITS on the Distributed Runner

Eigenvector centrality and HITS should not get separate job systems. They should
reuse the PageRank iteration machinery with metric-specific math and metadata.
The current planned HITS slice already uses that shared runner, proves atomic
paired authority/hub publish against the local implementation, verifies the same
metric-name planned-drain loop used by degree, PageRank, and eigenvector can
reach a fresh paired generation through worker/coordinator boundaries, and
verifies failed later-iteration contribution, reduce, hub contribution, hub
reduce, and convergence pages can retry through the generic worker step.
The same planned HITS coverage now resumes explicit hub contribution and hub
reduce cursor progress across graph-index reopen before completing the paired
iteration.
Later-iteration exhausted hub reduce attempts also fail the compatible metric
pair together while preserving the previous published pair, and reclaimed
contribution/reduce pages overwrite stale durable paired-vector output while
reclaimed convergence pages reset stale partial summaries. Planned cleanup can
resume after reopen with the published pair visible. The rest of this roadmap is
production completeness:
restart coverage, lease expiry, reclaimed-output behavior, paired-vector
partitioning, and distributed worker rollout.

Eigenvector requirements:

- use the same scan, initialize, iterate, reduce, check, publish, and cleanup
  shape as PageRank
- persist normalization metadata per iteration
- document behavior for disconnected and reducible graphs
- publish fixed-iteration non-converged output with `converged: false`

HITS requirements:

- compute authority and hub vectors over the same target generation and edge
  scope
- persist paired manifests or one manifest with paired output metadata
- publish authority/hub together when configured as a compatible pair
- fail the pair together if one side has invalid output
- partition both authority and hub work into retryable pages before enabling
  large remote builds by default
- preserve the previous published authority/hub pair after failed, expired, or
  reclaimed pages; planned-build failure is covered, while expiry and reclaimed
  output still need broader production coverage beyond contribution/reduce and
  convergence summaries

Exit criteria:

- At least one non-PageRank iterative metric reuses the distributed executor
  without new public query/status APIs.
- HITS paired publish preserves the atomic visibility invariant.
- Local and distributed outputs match within documented tolerance.
- Restart and retry coverage exists for paired-vector dynamic iterations,
  publish failure, and cleanup.

#### 7. Cleanup and Retention

V1 cleanup should remain aggressive: keep the latest published generation and
delete old generations as soon as snapshot safety allows. Distributed execution
adds intermediate keys that also need cleanup.

Cleanup should cover:

- previous score generations
- stale job manifests
- contribution pages
- reduce pages
- out-degree and rank state pages
- per-page lease/status records
- abandoned score generations that never published

Use the existing resolved default:

- immediate cleanup when graph metric reads are snapshot-safe
- deferred cleanup queue when snapshot safety cannot be proven
- no user-facing retention knob in the first distributed version

Future configuration may add retention for debug or audit workflows, but it
should be opt-in and bounded:

```json
{
  "metrics": [
    {
      "name": "pagerank",
      "kind": "pagerank",
      "retention": {
        "published_generations": 2,
        "failed_job_manifests": 5
      }
    }
  ]
}
```

Exit criteria:

- Cleanup never deletes the currently published generation.
- Cleanup can resume after restart.
- Failed or abandoned jobs do not grow storage without bound.
- Debug retention, if later added, is bounded and explicitly configured.

#### 8. Test Matrix and Rollout

Testing should be staged with the implementation. Do not wait for the whole
distributed runner before adding coverage.

Required tests by slice:

- verified publish rejects incomplete or mismatched manifests
- planner idempotence and manifest restart
- page lease claim, completion, expiry, and retry
- idempotent page overwrite
- restart between every phase
- restart after all score writes but before publish
- restart after publish but before cleanup
- stale reads while a distributed build is active
- fresh reads failing with `MetricStale` until publish
- failed distributed build preserving old top-k results
- non-converged fixed-iteration publish
- local-vs-distributed parity for degree, PageRank, eigenvector, and HITS
- cross-shard direct top-k generation guard
- cross-shard rerank generation guard

Rollout should use feature gates internally:

1. Keep the local runner as default.
2. Add verified publish and cleanup validation under local builds.
3. Run planned degree with one local worker and compare output to the local
   runner.
4. Run planned PageRank with one local worker and compare output to the local
   runner.
5. Enable multiple local workers in tests.
6. Enable remote/distributed workers for large graph indexes.
7. Broaden planned eigenvector and HITS restart/failure coverage, including
   paired-vector stale-read and previous-publish preservation tests.
8. Remove or demote the old local runner only after the distributed path has
   parity tests for degree, PageRank, eigenvector, and HITS.

Production-complete criteria for this roadmap:

- PageRank supports local and distributed materialization.
- Degree, eigenvector, and HITS reuse the same distributed executor model.
- Direct metric top-k, graph projection, graph ordering, graph search rerank,
  and status APIs have public e2e coverage.
- Failed builds preserve published scores across restart.
- Dirty markers survive restart and eventually rebuild.
- Cross-shard direct metric top-k has deterministic merge behavior, and
  retrieval/rerank metric merges either prove globally comparable generations or
  fail closed.
- OpenAPI, generated clients, and public docs describe freshness semantics,
  phase progress, convergence metadata, and failure status.

### Remaining Delivery Roadmap

The remaining work is a rollout and production-hardening roadmap, not a new
user-facing API. Users should continue to configure named graph metrics on graph
indexes and choose `published` or `fresh` on reads. Coordinators, workers, jobs,
leases, attempts, and cleanup remain internal maintenance machinery.

The rest of the design should keep three surfaces separate:

1. **User query surface**: graph metrics stay on graph indexes. Reads name the
   metric and choose `metric_freshness: "published"` or
   `metric_freshness: "fresh"`. `MetricNotReady` means no complete generation
   exists; `MetricStale` means a complete generation exists but does not match
   the current edge generation. Direct top-k, traversal projection/order/filter,
   search rerank, and query profile should all expose the same generation
   contract.
2. **Admin/operations surface**: v1 exposes enough status to answer "what is
   building, who owns it, how far did it get, and why did it fail?" without
   exposing raw storage prefixes. Pause, resume, priority, manual retry, lease
   tuning, and retention are future admin controls. They should not be required
   for query correctness or ordinary cleanup.
3. **Executor surface**: coordinators and workers communicate through durable
   graph-index state. Coordinators create jobs, advance barriers, publish, fail,
   and schedule cleanup. Workers lease and complete pages only. Runtime-owner
   leases fence process roles; page leases fence work units; attempt namespaces
   fence partial output adoption.

The near-term PR split should be:

| PR lane | Purpose | First useful deliverable |
| --- | --- | --- |
| Remote owner orchestration | Move from local process proofs to deployment-owned coordinator and worker roles. | Degree and PageRank can drain through independently managed owners using only durable graph-index state or the production service boundary. |
| Degree default gate | Use degree as the first low-risk distributed default. | Internal gate can route degree through planned maintenance with parity, cleanup, status, restart, and public freshness coverage. |
| PageRank promotion gate | Make PageRank the reference single-vector iterative metric. | Production-budget parity, larger graph evidence, fixed-iteration metadata, failed rebuild preservation, and cross-shard read compatibility all pass together. |
| Eigenvector parity | Prove the single-vector executor is generic. | Eigenvector uses the same lifecycle, errors, status, cleanup, freshness, and fan-in gates as PageRank. |
| HITS paired-vector gate | Prove paired metrics can share one lifecycle safely. | Authority and hub publish/fail atomically and preserve the previous compatible pair under restart, failure, and cleanup. |
| Operations contract | Make distributed metric work explainable and bounded. | Status exposes summarized role/owner/progress/failure/cleanup facts; cleanup and diagnostics remain bounded without a public retention knob. |
| Public fan-in contract | Prevent mixed-generation scoring from becoming a distributed correctness bug. | Every score-bearing read path either proves compatible nonzero published generations across shards or fails closed. |

The implementation should move in this order:

| Gate | Design target | Work remaining | Exit signal |
| --- | --- | --- | --- |
| 1. Production process ownership | Coordinators and workers are independently managed durable-state clients. | Move from local supervisor/process proof to deployment-managed owners. Owners receive endpoint or DB location, role, owner id, worker id, tick budgets, lease policy, and idle policy only. Metric config, target generation, manifests, page ranges, attempts, publish decisions, failure, and cleanup stay inside graph-index state. Command argv coverage now locks that boundary down for launched coordinator and worker-pool children: no metric/index names, target generations, job/page ids, metric configs, or direct file-backed writer-lock guard are present in production-style child argv. The spawned-process harness applies the same preflight to standalone coordinator/worker/worker-pool role processes before ordinary restart/reclaim runs and killable long-lived lease-loss owners are spawned. Command-summary coverage pins the stable operations telemetry emitted by those owners, including role, runtime/owner/worker hashes, lease-key hash, worker count, acquisition/takeover/lost-lease counters, tick progress, idle/error counters, and last error; aggregate supervisor/launcher summaries now preserve compact per-child telemetry for those same fields; standalone process-role coverage now asserts the same role/owner/worker/lease-key/tick/error telemetry for every coordinator, worker, and worker-pool process used in restart and reclaim proofs. | A coordinator and multiple workers can start, stop, crash, restart, and drain degree plus PageRank without shared memory, caller-supplied metric config, or local test-only writer serialization. Duplicate owners are fenced by runtime leases; clean shutdown releases the current owner lease immediately; stale page attempts cannot write, adopt, complete, publish, or fail. |
| 2. Degree canary | Degree is the first production-distributed metric because it proves scan, reduce, publish, cleanup, leases, attempts, and freshness without iterative convergence. | Promote planned/distributed degree behind an internal gate. Keep local-vs-planned parity, process replacement, stale attempt rejection, cleanup resume, active/failed status, page-status summaries, and public freshness checks in CI. | Degree can run through distributed maintenance with bounded latency, bounded diagnostics, no control-namespace growth, and rollback to the local oracle. |
| 3. PageRank promotion | PageRank is the reference single-vector iterative family. | Keep the DB-level production-budget parity proof, then finish larger-graph evidence, cross-shard fan-in checks, and public read-surface coverage. Keep dynamic iteration planning, convergence summaries, fixed-iteration non-converged publish metadata, publish verification, failed rebuild preservation, cleanup, and per-phase restart/reclaim tests as required promotion checks. | Distributed PageRank matches local output within tolerance, serves prior published generations during rebuilds, fails `fresh` reads while stale, preserves the prior generation on failed rebuilds, and recovers from restart at every phase boundary. |
| 4. Eigenvector parity | Eigenvector should reuse the PageRank single-vector substrate rather than adding a metric-specific lifecycle. | Keep the existing process-boundary, fixed-iteration, active-freshness, disconnected/reducible parity, publish-failure, cleanup coverage, and DB-level production-budget parity proof. Add the same cross-shard fan-in, deployment-scale parity, and operations evidence required for PageRank before default promotion. | Eigenvector is promoted only when its restart, failure, cleanup, freshness, fan-in, status, and operations matrix is equivalent to PageRank's single-vector matrix. |
| 5. HITS paired-vector hardening | HITS authority and hub are separate named scores but one compatible pair for target generation, convergence, failure, publish, and cleanup. | Finish paired local-vs-planned parity, larger-graph latency evidence, deployment-scale owner evidence, public read-surface coverage, and promotion-scale cross-shard fan-in coverage. Keep the default authority/hub pair compatibility guard in direct metric and graph traversal/search status fan-in plus the hosted paired-HITS `published`/`fresh` cross-range direct, traversal projection/order/filter/status, rerank evidence, and service-owner replacement proof. Keep authority/hub output attempt-scoped until page completion and publish/fail the pair atomically. | HITS stays disabled by default until either side failing preserves the previous compatible pair, paired publish is atomic, cleanup is resumable, and paired-vector restart coverage matches the PageRank quality bar. |
| 6. Public read and fan-in gate | Distributed execution is not complete until every score-bearing read path enforces the same generation contract. | Keep hosted paired-HITS `published`/`fresh` cross-range success/failure coverage while covering direct metric top-k, graph projection, graph ordering, graph filtering, graph search rerank, query profile, any future standalone explain surface, and shard fan-in for `published`, `fresh`, not-ready, stale, active-build, failed-build, missing-generation, zero-generation, incompatible-generation, and incompatible paired-HITS status cases. | `published` reads use the latest complete generation; `fresh` returns `MetricNotReady` before first publish and `MetricStale` when published is behind; fan-in rejects missing, zero, stale, or incompatible generations when comparability is required. |
| 7. Operations and cleanup | V1 cleanup is aggressive and bounded. Retention remains a future admin/debug option, not query semantics. | Make cleanup resumable for score generations, manifests, pages, phase summaries, iteration summaries, attempt namespaces, failure diagnostics, and runtime-owner records. Status should summarize phase, iteration, page counts, attempts, cursor presence, progress, last error, owner hash, worker identity count/hash, lease expiry, and takeover counters; active-page status coverage now requires capped building payloads to include leased page state, worker identity, lease expiry, attempt, cursor/error fields, progress units, and finite aggregate progress; cache-preservation coverage now verifies graph metric runtime ownership telemetry survives status refreshes that only carry synthetic or sequence-only table state, graph index status now exposes a typed `graph_metric_runtime` OpenAPI summary with role, owner/worker hashes, lease state, takeover/lost-lease counters, tick progress, and page counters, command-summary coverage proves standalone role processes emit the same stable ownership/progress/error fields for operators, and aggregate supervisor/launcher coverage carries that telemetry up to the orchestration summary. | Cleanup never deletes the current published generation, resumes after restart, bounds diagnostics, and prevents completed, failed, abandoned, unpublished, and attempt-scoped state from growing without a retention knob. |
| 8. Default promotion | Promote by metric family, not by executor milestone. Local runners remain deterministic CI/debug oracles until distributed parity is routine. | Widen the conservative `auto` gate one family at a time: degree, PageRank, eigenvector, then HITS. The PageRank and eigenvector auto thresholds now have explicit widen/rollback-shaped coverage: larger single-vector cases remain local under conservative caps, become planned when the caps are raised, and resume after budget exhaustion. Require release history, latency budgets, operations docs, cleanup bounds, generated-client stability, and rollback paths before each widening. | A family becomes default only after parity, process ownership, crash/restart, cleanup, public-read, fan-in, status, operations, and latency-budget evidence all pass. |

The steady-state architecture for every metric family should remain:

```text
graph index metric config
  -> dirty marker for target edge generation
  -> coordinator-owned durable build job
  -> deterministic manifest pages
  -> worker-owned page leases and cursors
  -> attempt-scoped intermediate output where later phases could consume partials
  -> coordinator-owned phase and iteration barriers
  -> verified publish of one complete generation
  -> published generation pointer
  -> resumable cleanup plus bounded diagnostics
```

The strict dependency chain is remote ownership before default promotion,
cross-shard comparability before score-bearing fan-in promotion, degree before
PageRank defaults, PageRank before eigenvector defaults, and HITS only after
paired-vector failure behavior is routine.

Status/API maturity should follow the same rule. Internal runtime telemetry can
be richer than the public contract, but generated clients should only gain
fields once they are stable summaries: metric state, freshness, published and
target generations, current phase/iteration, page progress, bounded failure
diagnostics, cleanup state, role, owner hash, worker count/hash, lease state,
takeover/lost-lease counters, tick progress, and page counters. Raw storage
keys, exact owner identifiers, attempt namespaces, and local-process details
should remain implementation details. The first stable runtime summary is now
`graph_metric_runtime` on graph index status; aggregate status merges shard
counters while per-shard status preserves each shard's runtime summary. If
independent shard runtimes report different roles, aggregate status keeps the
counter and identity-hash summary but omits `role` rather than implying a single
owner role. The shared plus joined Zig OpenAPI client contracts parse that
summary as generated typed status. The public generated Go, Python, and
TypeScript SDK surfaces also carry the typed runtime summary so operations code
can inspect the stable telemetry without depending on internal graph-index
records.

#### Rest-of-Roadmap Execution Plan

This subsection is the canonical roadmap from the current PR state. Older
roadmap notes below remain useful as implementation history, but this is the
plan to execute next.

The remaining work is not a new PageRank API. It is the path from the current
durable graph-index metric executor to a production distributed executor that
can be enabled by default one metric family at a time. Users should keep seeing
one graph-index metric model:

- metrics are configured on the graph index by stable names
- reads choose `published` or `fresh`
- `MetricNotReady` means no complete generation exists
- `MetricStale` means a complete generation exists but not for the requested
  current edge generation
- fixed-iteration non-converged PageRank, eigenvector, and compatible HITS
  publish by default with `converged: false`
- jobs, leases, pages, attempts, retries, owners, and cleanup policy remain
  internal, with only stable summaries exposed through status

The implementation should now move through these release bands:

| Band | Goal | Implementation design | Exit gate |
| --- | --- | --- | --- |
| 1. Owner restart/failure closeout | Make the service-backed owner path as strong as the direct DB harness. | Use the internal graph-metric-maintenance service boundary as the deployment-shaped path. Degree and bounded PageRank now have direct service-route and real HTTP service-targeted process coverage for abandoned coordinator and worker-pool leases, duplicate owners before TTL, takeover after TTL, stale release after takeover, and continued durable progress after replacement. Eigenvector now has the same service-owner replacement proof for its single-vector path. HITS now also has a service-owner replacement proof through `hits_authority`, followed by compatible authority/hub freshness verification. The spawned-process harness also accepts strict service-targeted owner argv in its role preflight while rejecting mixed or incomplete DB/service targets. Keep extending this same evidence to larger deployment and promotion-scale cases while keeping the direct DB writer guard as local harness-only behavior. | Degree, bounded PageRank, eigenvector, and the HITS compatible pair can drain through service-targeted owners; killed or abandoned owners lose only their runtime/page leases; replacements continue after expiry; duplicate coordinators and duplicate worker owners are fenced. |
| 2. Degree distributed canary | Promote the cheapest metric first to prove the mechanics. | Put degree behind the first internal distributed-maintenance gate. Require local/distributed parity, multi-worker page partitioning, stale-attempt rejection, publish idempotence, active/failed status, cleanup resume, public `published`/`fresh` checks, and bounded control-record growth. The first internal degree canary decision now admits exactly one queued or active degree build, blocks active or queued non-degree metric work, rejects failed/truncated page summaries, and applies a configurable control-record cap so rollout can fall back before namespace growth becomes unbounded. Queued degree work now contributes a planned build control-record estimate before the build starts, so an oversized queued build is rejected before it creates job/page state. Planned maintenance results now also report `rounds_executed`, and degree canary coverage proves a one-round budget reports resumable exhaustion before a later bounded budget drains to fresh. The DB idle maintenance mode now has an explicit internal `degree_canary` setting: eligible degree-only work runs through planned maintenance, queued mixed/non-degree work rolls back to the local graph-metric oracle, and already-active planned work outside guardrails fails fast instead of silently reporting idle. Active degree rebuilds now also have direct top-k, traversal projection, and search rerank freshness coverage in both canary-mode DB maintenance and service-route ownership: `published` reads keep serving the prior generation with building status, while `fresh` reads fail closed with `MetricStale`. | Degree can be enabled internally without duplicate publish, stale score visibility, unbounded latency, unbounded namespace growth, or loss of rollback to the local oracle. |
| 3. PageRank production gate | Make PageRank the reference iterative metric. | Run PageRank through the same owner harness with production budgets, larger graph evidence, dynamic iteration restart coverage, fixed-iteration non-converged metadata, publish verification, failed rebuild preservation, cleanup resume, and fan-in checks. | Distributed PageRank matches the local oracle within tolerance, preserves prior scores on failed rebuilds, serves prior `published` scores while rebuilding, fails `fresh` with `MetricStale`, and recovers from every phase boundary. |
| 4. Eigenvector single-vector parity | Prove the single-vector executor is generic. | Reuse the PageRank scan/initialize/contribution/reduce/convergence/publish/cleanup substrate. Eigenvector should add metric math, metadata, tolerance rules, and disconnected/reducible graph behavior, not a new job model. | Eigenvector passes the same parity, restart, failure, cleanup, fan-in, public freshness, fixed-iteration metadata, and operations matrix as PageRank. |
| 5. HITS paired-vector hardening | Prove paired metrics can share one lifecycle safely. | Keep authority and hub as separate named scores with one compatible target generation, convergence decision, publish decision, failure decision, and cleanup owner. Authority and hub pages may retry independently, but publish and failure stay atomic for the compatible pair. Service-owner replacement is now covered for the compatible pair through the authority metric path, direct process coverage exhausts hub-reduce attempts while preserving the previous compatible pair, and unit-test lifecycle coverage now covers local paired reclaim, publish failure, cleanup resume, failed-build preservation, publish idempotence, failed public-read preservation, and local/planned parity. | HITS remains off by default until promotion-scale fan-in under deployment-shaped owners, larger deployment-scale owner evidence, and larger-graph latency evidence all preserve the previous compatible pair. |
| 6. Public read and fan-in closeout | Treat every score-bearing read as distributed correctness. | Route direct top-k, traversal projection/order/filter, search rerank, query profile, explain/status, hosted fan-in, and cross-shard merge through published generation pointers and compatibility checks. Building, failed, abandoned, and attempt-scoped output stays invisible outside status. | `published` reads use the latest complete generation; `fresh` fails closed with `MetricNotReady` or `MetricStale`; fan-in rejects missing, zero, stale, malformed, incompatible, or incomparable generations before merging scores. |
| 7. Cleanup and operations | Make distributed metric maintenance safe to leave enabled. | Keep v1 cleanup latest-only and aggressive. Make cleanup resumable for old score generations, manifests, pages, phase/iteration summaries, attempts, failures, and runtime-owner records. Keep retention, manual retry, pause/resume, priority, and lease tuning as future bounded admin controls. | Cleanup never removes the current published generation, resumes after restart, bounds diagnostics, and prevents completed, failed, abandoned, unpublished, or attempt-scoped state from growing without bound. |
| 8. Default widening | Promote by metric family, not by executor feature. | Keep local runners as deterministic CI/debug oracles. Widen the conservative `auto` gate in order: degree, PageRank, eigenvector, then HITS. Require generated-client stability, docs, latency evidence, operations evidence, and rollback controls for each widening. | A family becomes distributed-by-default only after parity, remote ownership, crash/restart, cleanup, public-read, fan-in, status, operations, latency-budget, docs, generated-client, and rollback checks are all green for that family. |

The internal boundaries should stay fixed while those bands land:

| Boundary | Owns | Must not own |
| --- | --- | --- |
| Graph index | Metric config, edge-scope fingerprint, dirty marker, published pointer, score metadata, public freshness, and status assembly. | Worker scheduling, raw job/page controls, or lease tuning exposed to users. |
| Coordinator | Job start/resume, manifest validation, phase barriers, dynamic iteration planning, verified publish, failed-build recording, and cleanup scheduling. | Page execution or worker-local cursor progress. |
| Worker | Claiming one page lease, renewing it, persisting cursor progress, writing attempt-fenced output, completing/failing the page, and stopping. | Job creation, config resolution from callers, phase advancement, publish, or whole-build failure. |
| Runtime/scheduler | Role, owner identity, worker identity set, budgets, idle policy, lease policy, wakeups, and telemetry. | Metric semantics or public query freshness behavior. |
| Cleanup runner | Resumable deletion of old generations, abandoned attempts, completed job namespaces, manifests, pages, summaries, and bounded diagnostics. | Deleting the current published generation or changing query behavior. |

The next concrete PR sequence should be:

1. **Service-owner restart/failure PR**: extend the new degree and bounded
   PageRank direct service-route lease-loss/takeover proofs to real killed-owner
   process coverage for coordinator and worker-pool abandonment, duplicate-owner
   fencing before TTL, takeover after TTL, stale release fencing, and continued
   durable progress after replacement.
2. **Degree canary PR**: add the internal rollout gate, latency/storage
   guardrails, distributed public freshness coverage, cleanup-resume coverage,
   and rollback to local execution.
3. **PageRank promotion PR**: keep the DB-level production-budget parity gate,
   then finish larger-graph runs, fixed-iteration non-converged metadata checks, publish-verifier
   failure, failed rebuild preservation, cleanup resume, and cross-shard
   generation compatibility.
4. **Eigenvector parity PR**: reuse the PageRank single-vector executor and add
   the missing restart/failure/fan-in/operations evidence plus documented
   disconnected and reducible graph behavior.
5. **HITS paired-vector PR**: finish paired authority/hub remote-owner,
   restart, reclaim, failed-page, publish-failure, cleanup, fan-in, and latency
   evidence before enabling default HITS execution.
6. **Operations rollout PR**: document status fields, scheduler defaults,
   cleanup defaults, release qualification metrics, generated-client behavior,
   and rollback/default-widening controls.

The dependency that should not be skipped is owner correctness before default
promotion. A metric family must not become distributed-by-default while
duplicate coordinators can race publish/fail, duplicate workers can write stale
attempts, cleanup can grow without bound, or fan-in can merge incomparable
generations.

The remaining graph-index work should be tracked as a release train from
implementation proof to production default. The current PR should be treated as
the durable-executor baseline: graph metrics are index-owned, planned work is
durable and resumable, status exposes the stable runtime summary, and all metric
families have at least one shared lifecycle proof. The rest of the work should
avoid adding new user concepts unless a gate below proves the existing
graph-index metric surface is insufficient.

Target end state:

- Graph metric configs remain part of the graph index definition.
- The default maintenance path can run locally or through remote owners without
  changing query semantics.
- Degree, PageRank, eigenvector, and HITS all use the same durable
  coordinator/worker/job/page model.
- Every score-bearing read path enforces the same published/fresh generation
  contract.
- Operations can answer ownership, progress, freshness, failure, and cleanup
  questions from bounded status records.

The rest of the roadmap should be implemented in these shippable milestones:

| Milestone | Scope | User-visible result | Required implementation | Release gate |
| --- | --- | --- | --- | --- |
| A. Freeze the v1 public contract | Lock the naming, freshness, status, and error vocabulary before widening execution. | Users keep one graph-index metric API for degree, PageRank, eigenvector, and HITS. `MetricNotReady` and `MetricStale` stay the explicit read failures. | Audit OpenAPI, SDKs, docs, and public e2e tests so direct metric reads, traversal, search rerank, query profile, and status use the same fields and error names. | No remaining read path has metric-specific freshness behavior or ad hoc not-ready errors. |
| B. Production owner harness | Turn the current runtime/process proofs into the canonical distributed harness. | Operators can run coordinator and worker owners as independently managed processes or service roles. | Promote `antfly graph-metric-maintenance`/service-role configuration to the test harness for one coordinator plus multiple workers. Remove local test-only writer serialization from the success path. Keep role, owner, worker identity, budget, idle, and lease policy as the only runtime inputs. | Degree and PageRank drain through independent owners using only durable graph-index state or the service boundary. Killing and replacing a worker reclaims only its page after expiry. |
| C. Degree production canary | Use degree as the first default candidate because it is cheap and non-iterative. | Degree metrics become the canary for distributed maintenance with rollback to the local oracle. | Keep local-vs-planned parity, multi-worker page partitioning, restart, stale-attempt rejection, cleanup resume, status summaries, and public freshness coverage in CI. The internal degree canary decision now provides the first rollout guard: exactly one queued or active degree build is eligible, non-degree queued/active work blocks the degree-only rollout, failed/truncated page summaries block rollout, and a configurable control-record cap guards namespace growth. Queued degree builds now estimate their planned job/page control records before rollout, so work that would exceed the cap falls back before planned state is created. Planned-maintenance results now expose `rounds_executed`, and canary tests prove tight round budgets are reported as resumable rather than hidden unbounded work. DB idle maintenance can now select `degree_canary`, which uses planned maintenance only when that guard passes, falls back to local maintenance before starting blocked queued work, and fails fast when active planned work is already outside guardrails. Direct top-k, traversal projection, and search rerank freshness now cross both the DB canary path and the in-process service-route owner path for active degree rebuilds: prior `published` scores remain visible with building status and `fresh` fails closed. Unit-test coverage now includes those degree canary guardrails so those guardrails are release-gated. Remaining work is production-scale latency data. | Degree can be enabled internally by default without unbounded latency, duplicate publish, stale score visibility, or namespace growth. |
| D. PageRank distributed promotion | Make PageRank the reference iterative metric. | PageRank remains the primary user-facing centrality metric, but its default executor can become distributed when gates pass. | Keep the DB-level production-budget local/planned parity proof, then finish larger-graph parity, dynamic iteration restart coverage, fixed-iteration non-converged metadata, publish-verifier failure, cleanup resume, public read coverage, and cross-shard fan-in checks. | Distributed PageRank matches the local oracle within tolerance, preserves the previous generation on failed rebuilds, fails `fresh` while stale, and recovers from every phase boundary. |
| E. Eigenvector parity | Prove the single-vector executor is generic. | Eigenvector looks like another named graph metric, not a separate subsystem. | Reuse the PageRank scan/initialize/contribute/reduce/check/publish/cleanup substrate. Add the same restart, failure, cleanup, status, fan-in, and operations evidence as PageRank. Document reducible/disconnected graph behavior and fixed-iteration non-converged output. | Eigenvector has no metric-specific job model and passes the same single-vector promotion matrix as PageRank. |
| F. HITS paired-vector hardening | Prove paired metrics can be promoted safely. | Authority and hub scores remain separate named reads but share compatible lifecycle and failure semantics. | Keep authority/hub output attempt-scoped until page completion. Preserve the completed active/failed process-boundary public freshness coverage, hosted paired-HITS `published`/`fresh` cross-range fan-in coverage, the hosted active paired-HITS fan-in proof for direct top-k, traversal projection/order/filter/status, authority rerank, service-owner replacement proof, and default authority/hub pair compatibility rejection while finishing promotion-scale cross-shard fan-in, larger-graph latency, deployment-scale remote-owner evidence, and paired parity evidence. Require atomic compatible-pair publish and paired failure preservation. | HITS stays off by default until either side failing preserves the previous compatible pair and paired-vector restart coverage matches the PageRank bar. |
| G. Public fan-in and retrieval gate | Prevent distributed scoring from mixing incomparable generations. | Cross-shard top-k, traversal/search scoring, rerank, and profile either compare compatible published generations or fail closed. | Add/keep tests for missing metric status, zero generation, stale generation, incompatible generation, incompatible paired-HITS status, active build, failed build, and mixed shard runtime roles; hosted paired-HITS `published`/`fresh` success/failure coverage now exists for local shard DB fan-in. Explain/profile should report the generation/freshness basis used for scoring. | Every score-bearing fan-in path rejects missing, zero, stale, or incompatible generations when global comparability is required. |
| H. Operations, cleanup, and rollout | Make the executor safe to leave on. | Admins get bounded status and cleanup behavior without needing retention or lease tuning for correctness. | Finish resumable cleanup for published-generation supersets, manifests, pages, phase/iteration summaries, attempts, failures, and runtime-owner records. Add event/status docs, latency budgets, scheduler defaults, and rollback controls. | Cleanup never removes the current published generation, resumes after restart, bounds diagnostics, and gives operators enough telemetry to debug stuck work. |

Milestones should merge in order, but their tests should overlap. For example,
PageRank promotion should reuse the owner harness created for degree, while
eigenvector and HITS should start gaining public-read and cleanup coverage
before PageRank becomes default. The dependency that should not be skipped is
owner correctness before default promotion: a metric family must not become
distributed-by-default until duplicate coordinators, duplicate workers, stale
attempts, failed rebuilds, cleanup, and fan-in are already proven for that
family.

Design rules for the remaining implementation:

- Prefer adding metric families by plugging algorithm-specific phases into the
  shared executor rather than adding metric-specific job records.
- Keep local runners as deterministic CI/debug oracles until distributed output
  parity and operations evidence are routine.
- Keep worker callers name-based. A worker should never receive the resolved
  metric config from a caller; it should load durable graph-index state by
  index and metric name.
- Treat attempt-scoped output as mandatory when a later phase could consume
  partial output. Deterministic recompute is acceptable only when stale output
  cannot become visible and recompute cost is bounded.
- Keep v1 cleanup aggressive. Retention, manual retry, pause/resume, priority,
  and lease tuning can become bounded admin controls later, but they should not
  be needed for correct queries.
- Treat failed planned builds as terminal for automatic planned scheduler
  starts at the same target generation. Status can still expose failed work as
  needing attention, but duplicate/background coordinators must not immediately
  recreate the same failed generation and append another failure. A later dirty
  generation or an explicit future retry control can start new work. DB-level
  scheduler coverage now pins this policy: failed planned PageRank is not
  counted as auto-eligible planned work, a duplicate coordinator sweep does not
  start another build or append another failure event, and a later graph write
  makes the next generation eligible again. Runtime-owner coverage now pins the
  operations shape too: a runtime tick over only terminal failed planned work
  reports no durable progress through both runtime-local and stable DB runtime
  stats, increments idle rather than error/failure counters, leaves one failure
  event, and starts work again only after a newer dirty generation appears.
- Promote defaults by metric family, not by executor feature. Degree can ship
  before PageRank; PageRank before eigenvector; HITS last because paired-vector
  failure is the highest-risk contract.

The minimum dashboard/status vocabulary for the rest of the rollout is:

| Status area | Stable summary fields |
| --- | --- |
| Freshness | metric state, published generation, target edge generation, freshness result, dirty state |
| Build progress | phase, iteration, expected/completed/failed pages, units completed/total, convergence metadata |
| Ownership | runtime role, owner hash, worker count/hash, lease held, last acquired time, takeover count, lost-lease count |
| Failure | bounded recent failures with phase, iteration, page attempt, retry count, and last error |
| Cleanup | cleanup phase, remaining namespace/page counts when available, last cleanup error, diagnostics pruned count |
| Fan-in | per-shard generation/freshness compatibility and aggregate role omission when shard roles differ |

Default-promotion checklist for each metric family:

- Local-vs-distributed parity over typed and all-edge scopes.
- Restart coverage across every phase boundary.
- Expired page lease reclaim and stale-attempt rejection.
- Failed rebuild preserves prior published generation.
- Cleanup resumes after restart and bounds abandoned state.
- Public `published` and `fresh` reads behave consistently across direct,
  traversal, search rerank, profile, and shard fan-in paths.
- OpenAPI and generated Go, Python, TypeScript, and Zig client surfaces are
  stable.
- Scheduler/runtime budgets have latency evidence and a rollback path.

#### Detailed Roadmap Design From Here

The remaining work should be planned as a sequence of production-boundary
changes rather than more algorithm-specific experiments. The current codebase
already proves the durable graph-index executor shape locally: metric configs
belong to graph indexes, coordinators and workers communicate through persisted
job/page state, runtime and page leases fence owners, attempt namespaces fence
partial output, and public reads use only published generations. The unfinished
roadmap is to make that boundary production-shaped, prove it at promotion scale,
and then widen defaults one metric family at a time.

The production architecture should have these internal components:

| Component | Responsibility | Boundary rule |
| --- | --- | --- |
| Graph index metric config | Own metric names, algorithm parameters, edge filters, freshness semantics, and stable status. | Never copy resolved metric configs into worker process arguments or external job payloads. |
| Dirty-generation detector | Detect the target edge generation that needs a rebuild. | Creates or resumes durable planned work; it does not synchronously compute metric scores. |
| Coordinator owner | Ensure jobs, advance phase/iteration barriers, append dynamic pages, verify publish, fail builds, and schedule cleanup. | Exactly one active coordinator per role lease may mutate build lifecycle state; duplicate coordinators must be idempotent observers or fenced. |
| Worker owner | Claim page leases, renew leases, persist cursors, write attempt-scoped output, and complete or fail pages. | Workers do not start jobs, decide convergence, publish, fail whole builds, or receive caller-supplied metric configs. |
| Runtime lease registry | Fence independently managed coordinator and worker processes. | Runtime lease loss stops further page/build mutation; clean shutdown deletes only the current owner record. |
| Page/attempt store | Fence individual work units and partial output adoption. | Stale owners from older attempts cannot write, complete, adopt, publish, or fail after replacement. |
| Publish verifier | Validate manifest/config fingerprints, page summaries, iteration metadata, and score output before moving the generation pointer. | Failed verification preserves the previous published generation and records bounded diagnostics. |
| Cleanup runner | Delete old generations, manifests, pages, summaries, attempts, failures, and runtime records when safe. | Cleanup never removes the current published generation and is resumable after restart. |
| Public read gate | Enforce `published` and `fresh` semantics across direct, traversal, rerank, profile, and fan-in paths. | Building, failed, abandoned, or attempt-scoped output is never queryable as score data. |

The roadmap should be executed in four release bands:

| Release band | Goal | Primary PRs | Must prove before moving on |
| --- | --- | --- | --- |
| 1. Production owner boundary | Replace local-process proof assumptions with deployment-shaped coordinator and worker owners. | Remote owner harness, service-boundary adapter, runtime lease ownership, command/status telemetry. | Degree and PageRank drain from dirty state to fresh publish through independently managed owners without shared memory, worker-side metric config, or local test-only writer serialization. |
| 2. Canary and reference promotion | Promote the lowest-risk family first, then the reference iterative family. | Degree canary, PageRank production promotion, latency/storage budget collection, rollback wiring. | Degree and PageRank pass parity, restart, stale-attempt rejection, failed rebuild preservation, cleanup, public read, status, and fan-in gates under production budgets. |
| 3. Generalization and paired metrics | Prove the executor is not PageRank-specific and that paired metric families remain compatible. | Eigenvector parity, HITS paired-vector hardening, promotion-scale paired fan-in, larger-graph runs. | Eigenvector matches the PageRank single-vector matrix; HITS authority/hub publish and fail atomically while preserving the previous compatible pair. |
| 4. Default widening and operations | Make distributed maintenance safe to leave enabled. | Per-family default gate widening, ops docs, dashboard/status docs, cleanup and diagnostics bounds, generated-client stability. | Each family has latency evidence, rollback controls, bounded cleanup, stable status, and release history before becoming distributed-by-default. |

Current checkpoint for that roadmap:

| Area | Already true | Remaining design work |
| --- | --- | --- |
| Local owner proof | `graph-metric-maintenance` can launch independently owned coordinator and worker-pool roles against a shared local DB path, drain degree, PageRank, eigenvector, and compatible HITS, and prove lease/page/publish idempotence through spawned processes. | Keep this path as the compatibility harness and local oracle, but stop treating shared local DB access plus the writer guard as production orchestration. |
| Runtime boundary | The runtime owner loop now calls a `MaintenanceBoundary` for combined, coordinator, worker, and worker-pool ticks instead of directly embedding every `IndexManager` call. The boundary is now a small vtable with a reusable role-dispatch tick helper, so the same runtime tick contract can target direct DB state or another implementation. | Keep extending the service-backed boundary implementation until production supervisors use it by default and direct DB access is only the local oracle/harness. |
| Service API | `/internal/v1/groups/{group}/tables/{table}/graph-metric-maintenance` accepts the same role, runtime owner, worker, lease, tick budget, page budget, and clock-shaped inputs as the direct DB path, the API client has a matching method, and `antfly graph-metric-maintenance` can target either a local DB path or that service endpoint with group/table identity. The service path now acquires or renews the durable runtime-owner lease before running a tick, reports runtime ownership stats in the maintenance response, fences duplicate owners before TTL expiry, and reports takeover after expiry. The service-targeted command path now runs through the same `MaintenanceBoundary` role dispatch as the runtime, and worker-pool service ticks preserve the worker set as one owner-scoped request instead of expanding to local per-worker DB calls. The `supervise` and `launch` entrypoints now also accept the same service target and launch coordinator/worker-pool children through the service boundary without the local DB writer guard. The same endpoint now accepts `tick`, `status`, and `release` actions: status reports the current owner hash and expiration without exposing raw keys, release deletes only the current owner's lease, stale-owner release after takeover is fenced, and service-mode commands send a final release on clean shutdown. Focused in-process service-route proofs now drain background degree and bounded PageRank builds to fresh through service-targeted coordinator and worker-pool owners without opening the DB through the command path. Degree service-route coverage now also leaves a coordinator-started rebuild active and proves direct top-k, traversal projection, and search rerank use the prior published generation with building status while `fresh` fails closed. Degree and bounded PageRank now also have service-route owner restart/failure coverage: abandoned coordinator and worker-pool owners fence duplicate replacements before TTL, allow takeover after TTL, reject stale release after takeover, and continue to a fresh generation through replacement owners. The service command test hook now writes its ready marker and optional hold before clean release, so the spawned-process harness can kill a service-targeted owner after a real lease-bearing tick instead of after shutdown cleanup. Degree and bounded PageRank now have real HTTP service-targeted process proofs: the harness exposes each seeded graph DB through `ApiHttpServer`, kills service-targeted coordinator and worker-pool owner processes after ready, observes duplicate-owner fencing before TTL, observes replacement takeover after TTL, and drains the generation to fresh through replacement service-targeted processes. Degree, PageRank, eigenvector, and paired HITS now also have larger service-targeted multi-page proofs: real HTTP service coordinators start 130-source builds, a two-worker service worker-pool drains multiple degree scan/reduce pages, multiple PageRank/eigenvector scan/initialize/contribution/reduce/convergence pages, and paired HITS scan/initialize/authority-contribution/authority-reduce/hub-contribution/hub-reduce/convergence pages through bounded ticks, the coordinator publishes, and cleanup drains to fresh without the command opening the local DB. The degree, PageRank, eigenvector, and paired HITS multi-page service proofs now also kill the coordinator after start and kill the scan worker-pool after bounded page work, prove duplicate owners are fenced before TTL, prove replacements take over after TTL, and then continue the same 130-source builds to publish and cleanup. HITS now also has service-owner restart/replacement coverage through `hits_authority`, with a final compatible authority/hub freshness check proving the pair drains together. Degree, PageRank, eigenvector, and HITS now also have service-targeted publish-to-cleanup proofs: a real HTTP service coordinator publishes a prepared build, a second coordinator cannot duplicate publish, and separate worker-pool service processes resume cleanup to a fresh generation, including degree/PageRank/eigenvector/HITS multi-page cleanup. PageRank, eigenvector, and paired HITS now also have service-targeted publish-verifier failure proofs: a real HTTP service coordinator fails a corrupted manifest without publishing, preserves the prior generation or compatible pair, and a duplicate service coordinator cannot publish or fail again. The HITS service publish/cleanup and larger multi-page service proofs now also verify paired fixed-iteration metadata for authority and hub after service-owned publish and cleanup. Separate real HTTP service-targeted degree, PageRank, eigenvector, and paired-HITS public-read proofs now publish initial generations through service owners, start later rebuilds through the same service boundary, and verify direct top-k, traversal projection, and search rerank use the prior published generation while `fresh` fails closed. Degree, PageRank, and eigenvector report `building`; paired HITS preserves the compatible prior pair while authority/hub statuses may be `building` or `stale` depending on which side owns the active lifecycle state. | Owner-boundary closeout is covered for degree and bounded PageRank, HITS has compatible-pair service-owner replacement evidence, degree/PageRank/eigenvector/HITS have multi-page two-worker service-boundary evidence, degree/PageRank/eigenvector/paired HITS have first multi-page service killed-owner timing evidence, PageRank/eigenvector/paired HITS publish-verifier failure now crosses the service process boundary, HITS has paired fixed-iteration metadata evidence across service-owned publish/cleanup and multi-page service runs, degree/PageRank/eigenvector/HITS cleanup restart now crosses the service process boundary, and the degree/PageRank/eigenvector/HITS active public-read contracts now cross the real HTTP service owner path; remaining work is broader promotion-scale remote-owner evidence, public fan-in coverage, broader cleanup operations, promotion-scale failure timing, and per-family default gates. |
| Public reads | Direct metric, traversal, rerank, profile, hosted fan-in, not-ready, stale, active-build, and failed-build behavior are covered in focused tests for many surfaces. Hosted cross-range fan-in now also covers active degree, PageRank, eigenvector, and paired HITS rebuild shards with compatible prior published generations: direct metric top-k, traversal projection/order/filter/status, and search rerank merge the prior generation for `published`, while `fresh` fails closed. Degree/PageRank/eigenvector report `building`; paired HITS may report the authority side as `building` and the compatible hub side as `stale` while still preserving the prior pair. Degree, PageRank, eigenvector, and paired HITS also have direct/traversal/rerank active public-read proof through the real HTTP service owner path. Paired HITS now also has a direct DB failed-planned-rebuild proof: authority and hub `published` direct reads, traversal projection/order/filter/status, and authority rerank return the prior compatible pair with failed status, omit attempt output from the failed generation, and `fresh` fails with `MetricStale`. | Repeat the public-read matrix under production remote owners and promotion-scale shard layouts before widening defaults. |
| Cleanup and operations | Latest-only cleanup, bounded diagnostics, runtime-owner lease telemetry, command summaries, and graph-index runtime status are now visible enough for local/process proofs. Direct process proofs cover degree, PageRank, eigenvector, and HITS publish/cleanup restart, PageRank/eigenvector/HITS exhausted page-attempt failure, and all four families now have service-targeted publish/cleanup restart proofs that publish through the real HTTP service owner path and resume cleanup through replacement worker-pool service processes. Degree, PageRank, eigenvector, and paired HITS now also have graph-level repeated-failure storage-growth proofs: each failed planned build removes abandoned score/job namespaces immediately while recent failure/event diagnostics remain capped. | Promote those summaries into the operational contract for remote owners without exposing raw storage keys, page ids, attempt namespaces, or process-local writer details; broaden cleanup qualification across larger retained namespaces and promotion-scale storage growth before default widening. |

The implementation sequence from this checkpoint should be:

| Step | Slice | Design | Done when |
| --- | --- | --- | --- |
| 0 | Service-targeted owner command | Done for the first boundary slice: `antfly graph-metric-maintenance` accepts either `--db-path` or `--base-uri --group-id --table-name`, sends only role, runtime owner identity, worker identity or worker set, lease TTL, tick/page budgets, background-start policy, and optional test clock inputs to the internal group maintenance endpoint, rejects mixed/incomplete targets, and keeps metric names, index names, target generations, job ids, page ids, manifest paths, score prefixes, and metric configs out of the request body. The command accepts the service response envelope and merges server runtime ownership telemetry into its summary. | A coordinator, worker, worker-pool, or combined owner can run bounded ticks through the internal group maintenance endpoint and aggregate durable progress, idle, error, sweep-result, and runtime-owner lease summaries without opening the local group DB directly. |
| 1 | Service-backed `MaintenanceBoundary` | Done for the boundary contract slice: the internal service boundary now has the same request-shaped coordinator/worker operations and service-side runtime owner lease acquire/renew/takeover fencing for bounded ticks. The runtime boundary itself is implementation-neutral, and the service-targeted command path uses that boundary dispatch for combined, coordinator, worker, and worker-pool ticks. The supervisor and launcher can target either a local DB path or service endpoint, and service-targeted launched children omit the local DB writer guard. The endpoint now has explicit status/release actions, clean service command shutdown releases only the current owner lease, and stale release after takeover preserves the replacement owner. Treat the internal API as a deployment boundary, not as a public job API. | The runtime can execute the same owner loop through a direct DB boundary or service boundary, and tests prove the request/response contract preserves durable progress, idle, error, conflict, lease-acquisition, lease-loss, takeover, clean-release, and status semantics. |
| 2 | Remote owner harness | Replace local launch assumptions with deployment-shaped coordinator and worker processes using the service target by default in integration coverage. Degree and bounded PageRank now have in-process service-route drain proofs: service-targeted coordinator and worker-pool owners alternate bounded ticks through the internal group route, release their runtime-owner leases on shutdown, and publish fresh generations without the command path opening the local DB. They also prove abandoned service-route coordinator and worker-pool leases are fenced before TTL, taken over after TTL, and stale releases cannot clear the replacement owner before the build drains to fresh. The spawned-process preflight now treats service targets as first-class owner argv and rejects mixed or partial targets. Degree, PageRank, eigenvector, and paired HITS now extend that service proof to larger 130-source builds where a two-worker service worker-pool completes multiple partitioned pages before service coordinator publish and cleanup; PageRank and eigenvector cover scan, initialize, contribution, reduce, and convergence phases, while HITS covers both authority and hub contribution/reduce phases before compatible pair publish. Degree, PageRank, eigenvector, and paired HITS also combine those 130-source service proofs with killed coordinator and killed worker-pool timing: duplicates are fenced before TTL, replacements take over after TTL, and the same builds continue through remaining pages, publish, and cleanup. Keep the direct DB writer guard only for the local proof harness. | Killing service-targeted owners, expiring leases, replacing owners, and retrying stale attempts produce the same durable results as the direct DB harness; multi-page degree, PageRank, eigenvector, and paired HITS service builds prove the worker-pool request shape can carry real partitioned non-iterative, single-vector iterative, and paired-vector iterative work, not only one-page fixtures. Degree, PageRank, eigenvector, and paired HITS now have first multi-page service killed-owner timing proofs; the same timing matrix still needs to be broadened at promotion scale. |
| 3 | Degree canary | Use degree as the first distributed-by-default candidate because it exercises planning, page leases, publish, cleanup, status, and freshness without iterative math. The internal degree canary decision now separates degree-only rollout eligibility from the broader auto gate: one queued/active degree build can run planned, active/queued non-degree work forces fallback, failed/truncated page summaries force fallback, and a control-record cap bounds namespace growth. Queued degree builds now add an estimated planned control-record count to that cap check before startup. Planned sweep results now carry `rounds_executed`, giving the canary a direct bounded-round signal for latency-budget qualification. The `degree_canary` idle mode wires that decision into `runUntilIdle`, so canary-enabled DBs exercise planned degree maintenance while retaining rollback to the local oracle before blocked queued work starts and failing fast when already-active planned work is outside guardrails. Direct top-k, traversal projection, and search rerank freshness now have active-rebuild coverage for prior-generation `published` reads and `fresh` failure in canary-mode DB maintenance, in-process service-route ownership, the real HTTP service owner path, and focused hosted cross-range fan-in. | Degree passes local/distributed parity, owner restart, stale-attempt rejection, cleanup resume, active/failed status, public read freshness, latency budget, storage-growth budget, and rollback-to-local checks under the service harness. |
| 4 | PageRank production gate | Promote PageRank only after the reference iterative matrix passes with remote owners. Dynamic iteration planning, convergence metadata, fixed-iteration non-converged publish, publish verification, failed rebuild preservation, cleanup, and fan-in are all required. The real HTTP service owner harness now covers the active public-read part of that matrix: after an initial service-owned publish and a service-started rebuild, direct top-k, traversal projection, and search rerank serve the prior published generation while `fresh` fails closed. Hosted cross-range fan-in now carries the same active PageRank invariant across compatible shard generations for direct metric top-k, traversal projection/order/filter/status, and search rerank. | PageRank matches the local oracle within tolerance, preserves prior scores on failure, serves prior `published` scores while rebuilding, fails `fresh` with `MetricStale` when appropriate, and rejects incompatible shard fan-in at promotion scale. |
| 5 | Single-vector and paired-vector backfill | Bring eigenvector to the PageRank matrix through the same single-vector executor, then bring HITS through the stricter paired authority/hub lifecycle. Eigenvector now has real HTTP service-owner active public-read coverage for direct top-k, traversal projection, and search rerank, service-owner replacement coverage, hosted cross-range active fan-in coverage, two-worker service-boundary multi-page phase coverage, service-targeted publish/cleanup restart coverage, unit-test scan/initialize/contribution/reduce/convergence reclaim coverage, cleanup resume after reopen, failed planned-build preservation, coordinator publish-failure preservation after reopen, failed planned-rebuild public-read preservation, and DB-level production-budget parity through a 130-source one-page-budget planned drain matching the local oracle. Paired HITS now has real HTTP service-owner active public-read coverage, service-owner replacement proof through the authority path, two-worker service-boundary multi-page authority/hub phase coverage, first service killed-owner timing for coordinator and scan worker-pool replacement, scan-page reclaim coverage that rejects stale partial writes, failed planned rebuild public-read coverage that preserves the prior authority/hub pair for direct top-k, traversal projection/order/filter/status, and authority rerank while `fresh` fails with `MetricStale`, paired fixed-iteration metadata verification after service-owned publish/cleanup and multi-page service publish, focused hosted active fan-in coverage for authority/hub direct top-k, traversal projection/order/filter/status, authority rerank while `fresh` fails closed, service-targeted publish/cleanup restart coverage, and DB-level production-budget parity through a 130-source one-page-budget planned drain matching the local authority/hub oracle for the compatible pair. | Eigenvector adds no new job system or status model and still needs deployment-scale parity, promotion-scale fan-in, operations evidence, and latency data before promotion. HITS authority/hub publish and fail atomically, preserve the previous compatible pair, and stay disabled by default until promotion-scale paired fan-in, broader deployment-scale owner evidence, and larger-graph latency evidence are complete. |
| 6 | Operations and default widening | Widen the conservative `auto` gate one family at a time only after operational evidence is routine. | Status, generated clients, docs, dashboards, cleanup bounds, diagnostics bounds, rollback controls, latency budgets, and release qualification all pass before a family becomes distributed-by-default. |

The first release band should turn the current process harness into the
canonical production harness:

1. Define a narrow owner launch contract: endpoint or DB location, role, owner
   id, worker id or worker-id set, tick/page budgets, idle policy, lease TTL,
   and optional test clock inputs.
2. Keep metric name, index name, target generation, page id, manifest details,
   metric config, score prefixes, and publish decisions inside durable
   graph-index state or the service API that fronts it.
3. Introduce a production service-boundary adapter that has the same semantics
   as the direct DB harness: coordinator sweeps, worker page sweeps, lease
   acquire/release, status fetch, and cleanup ticks. The runtime now routes
   owner ticks through a `MaintenanceBoundary` instead of calling the
   `IndexManager` inline; the current boundary is direct/embedded, and the
   internal group service boundary now exposes the same budget-shaped
   coordinator, worker, worker-pool, and combined operations through
   `/internal/v1/groups/{group}/tables/{table}/graph-metric-maintenance` plus
   the matching API client method. The `graph-metric-maintenance` command can
   now call that service endpoint directly for bounded owner ticks through the
   same boundary dispatch helper used by the runtime, and the endpoint now
   acquires/renews durable runtime owner leases before executing
   service-targeted work. Worker-pool service ticks remain owner-scoped service
   requests with the worker set intact instead of being expanded into local
   direct-DB worker calls. The supervisor and launcher entrypoints now accept
   service targets too, and service-targeted launched children do not take the
   local DB writer guard. The service endpoint now also supports status and
   owner-scoped release actions, and clean service-mode command shutdown sends
   a final release while stale-owner release after takeover is fenced. The
   first service-route drain proofs now publish degree and bounded PageRank
   through alternating service-targeted coordinator and worker-pool owners
   without the command path opening the local group DB directly. Degree and
   bounded PageRank now also prove abandoned service owner leases are fenced
   before TTL, replacements can take over after TTL, stale releases after
   takeover are refused, and replacement owners can continue to fresh output.
   Service-mode owner processes now write their test-ready marker and hold
   before clean shutdown release, which makes "kill after ready" a true
   abandoned-lease scenario for the real service-targeted process harness.
   Degree and bounded PageRank now have that live HTTP proof: the
   spawned-process harness serves each seeded graph DB through `ApiHttpServer`,
   kills service-targeted coordinator and worker-pool owners after they acquire
   runtime leases, verifies duplicate replacements are fenced before TTL,
   verifies replacements take over after TTL, and drains to a fresh generation
   through service-targeted replacement processes. Eigenvector now has the same
   service-owner restart/replacement proof for its single-vector path, and HITS
   has service-owner restart/replacement coverage through `hits_authority`,
   followed by a compatible authority/hub freshness check. The remaining
   production work is promotion-scale remote-owner evidence, public read/fan-in
   coverage, cleanup operations, and per-family default gates.
4. Preserve the direct local DB launch path only as a proof harness, with its
   explicit file-backed writer guard documented as local-only storage
   serialization rather than a graph metric coordination primitive.
5. Add crash/restart tests that start real owners, kill a coordinator or worker,
   advance the clock past the lease, start replacements, and prove the same
   generation either publishes once or fails once while preserving prior scores.

The second release band should make degree and PageRank promotion decisions
explicit:

| Metric family | Why this order | Promotion evidence |
| --- | --- | --- |
| Degree | Exercises the distributed mechanics without iterative convergence. | Local/planned parity, page partitioning, process replacement, stale-attempt rejection, publish idempotence, cleanup resume, active/failed status, direct/traversal/rerank freshness, two-worker service-boundary multi-page scan/reduce evidence, latency budget, storage-growth bound, rollback to local. |
| PageRank | Reference single-vector iterative metric and primary user-facing centrality score. | DB-level production-budget local/planned parity now drains a 130-source multi-page PageRank build through one-page planned maintenance rounds and matches the local oracle. Unit-test lifecycle coverage now also includes later-iteration exhausted-attempt preservation for the prior published generation plus failed planned-rebuild public-read preservation for direct top-k, traversal projection/order/filter/status, and search rerank while `fresh` fails with `MetricStale`. Hosted fan-in now includes a nonuniform eight-shard PageRank direct merge layout with four active/stale shards that keep unpublished target scores invisible while `fresh` fails closed. Remaining promotion evidence is deployment-scale cleanup, broader hosted fan-in surfaces, and latency evidence. |
| Eigenvector | Proves PageRank's single-vector substrate is generic. | Same as PageRank plus documented disconnected/reducible graph behavior and fixed-iteration non-converged output. Active direct/traversal/rerank freshness now has real HTTP service-owner coverage, service-owner replacement coverage, two-worker service-boundary multi-page scan/initialize/contribution/reduce/convergence evidence, focused hosted cross-range fan-in coverage including a nonuniform eight-shard direct merge with active/stale shards, unit-test scan/initialize/contribution/reduce/convergence reclaim coverage, cleanup resume after reopen, failed planned-build preservation, coordinator publish-failure preservation after reopen, later-iteration exhausted-attempt preservation, direct DB failed planned-rebuild public-read preservation for direct top-k, traversal projection/order/filter/status, and search rerank while `fresh` fails with `MetricStale`, and DB-level production-budget local/planned parity through one-page planned maintenance rounds; deployment-scale parity, broader promotion-scale fan-in, and latency evidence remain required. |
| HITS | Highest-risk contract because two named scores share one compatible lifecycle. | Authority/hub paired parity, paired restart/reclaim/exhausted-attempt coverage, atomic compatible publish, paired failure preservation, paired cleanup, fixed-iteration metadata, public freshness, promotion-scale fan-in, larger-graph latency. Real HTTP service-owner active public-read coverage, service-owner replacement through `hits_authority`, two-worker service-boundary multi-page authority/hub contribution/reduce evidence, unit-test and direct-process killed-worker exhausted hub-reduce attempt coverage with duplicate coordinator idempotence, scan-page reclaim coverage that rejects stale partial writes, direct DB failed planned rebuild coverage for prior compatible authority/hub direct top-k, traversal projection/order/filter/status, and authority rerank with `fresh` `MetricStale`, paired service-owned fixed-iteration metadata, focused hosted fan-in, and DB-level production-budget local/planned parity now cover published/fresh, active, failed, and exhausted-attempt paired-HITS reads plus one-page-budget paired drains for direct top-k, traversal, and authority rerank. |

The public/API work should stay deliberately small. The graph metric user model
does not need a new job-submission API for this roadmap. The only stable public
surface that should grow is status: fields that summarize freshness, phase,
iteration, page progress, convergence, owner hash, worker count/hash, lease
state, takeover/lost-lease counters, bounded failures, and cleanup progress.
Exact owner ids, raw keys, page ids, attempt namespaces, process pids, and local
writer-guard details should remain internal.

The operations roadmap should be tracked with the same severity as algorithm
coverage:

- Define scheduler defaults for tick budget, page budget, idle budget, lease
  TTL, and retry attempts, plus rollback controls for every default widening.
- Add release-qualification runs that record latency, storage growth, page
  counts, cleanup duration, retry counts, and fan-in behavior for representative
  graph sizes.
- Document how to inspect stuck metrics: status, recent events, role ownership,
  active pages, failed pages, lease expiry, and cleanup state.
- Keep v1 retention latest-only. Historical generation retention, manual retry,
  pause/resume, priority, and lease tuning can be later admin controls, but none
  should be required for query correctness.
- Treat generated client changes as part of the release gate; status fields
  should not appear in OpenAPI/SDKs until their semantics are stable.

The work is complete only when a clean install can enable distributed graph
metric maintenance, restart any owner at any point, continue to serve correct
published scores, fail `fresh` reads when appropriate, reject incomparable shard
fan-in, clean abandoned state, and give operators enough bounded telemetry to
understand what happened.

### Remaining Implementation Backlog

The rest of the work should be cut as a sequence of narrow release gates. Each
gate should leave the public graph metric API unchanged and move one internal
boundary closer to production distributed execution.

| Order | Backlog item | Implementation work | Verification required |
| --- | --- | --- | --- |
| 1 | Remote owner harness | Promote the process/runtime path from local proof to the canonical coordinator/worker harness. Owners should accept only DB path or service endpoint, role, owner id, worker identity, tick budget, idle policy, and lease policy. Metric config, target generation, page manifests, attempts, publish, failure, and cleanup stay in graph-index state. Launched child argv tests now enforce that narrow owner/budget interface and keep metric/index/job/page details out of process arguments. Command-summary tests now pin the stable role/owner/worker/lease/progress/error telemetry emitted by standalone coordinator and worker-pool processes, and aggregate supervisor/launcher summaries now preserve the same compact per-child telemetry. Standalone role summaries are now treated as a stable operational contract too: they must expose durable-progress, idle, and error tick counters; explain completed ticks as progress, idle, error, lease contention, or lease loss; prove either runtime-owner lease acquisition or explicit acquisition failure; and report `last_error_name: null` for successful owner ticks. Standalone role summaries and aggregate supervisor/launcher summaries must both omit raw metric/index names, target generations, job/page ids, attempt namespaces, storage paths/prefixes, metric configs, process ids, and local writer details. The runtime owner loop now depends on a `MaintenanceBoundary` for combined/coordinator/worker/worker-pool ticks, the internal group write API now exposes the same narrow graph metric maintenance request over HTTP with a matching client method, the command can call that service target directly through the same boundary dispatch path, service-targeted ticks now acquire/renew/take over durable runtime-owner leases before mutating graph metric state, supervisors can launch service-targeted coordinator/worker-pool children without local writer serialization, clean service shutdown releases only the current owner lease while stale releases are fenced, and degree plus bounded PageRank now drain to fresh through an in-process internal service route using service-targeted owners. Degree and bounded PageRank service-route restart/failure coverage now proves abandoned coordinator and worker-pool owners are fenced before TTL, replaced after TTL, stale releases cannot clear replacement owners, and replacement owners continue to fresh output. The spawned-process harness now preflights service-targeted owner argv as a strict endpoint/group/table target and rejects mixed or incomplete local/service targets, real HTTP killed-owner process coverage now proves the same restart/fencing path for degree and bounded PageRank, eigenvector now has service-owner replacement proof, HITS now has service-owner replacement proof with compatible pair freshness verification, real HTTP service-targeted PageRank/eigenvector/paired-HITS active-read coverage now proves the service owner path preserves prior published direct/traversal/rerank results while a rebuild is active, PageRank/eigenvector/paired-HITS publish-verifier failure preserves prior output through service-targeted coordinators, degree/PageRank/eigenvector/HITS have service-targeted publish/cleanup restart proof across coordinator publish, duplicate coordinator idempotence, and worker-pool cleanup resume, degree/PageRank/eigenvector/paired-HITS now have 130-source two-worker service multi-page proofs for non-iterative, single-vector iterative, and paired-vector iterative phase families, and direct-process page-reclaim proofs now read the durable page record to require replacement-worker completion under a newer attempt before stale-owner completion is rejected. The process harness now enforces and emits a final `graph_metric_process_harness_summary` JSON event with `remote_owner_release_gate: true` only after launch, service-owner restart, service publish/cleanup, service publish-failure, service multi-page worker-pool, service active-read, direct publish/cleanup, direct publish-failure, direct active-read, page-reclaim, fixed-iteration, exhausted-attempt, and same-worker fencing coverage all reach their required category counts. That event now also gates explicit service multi-page phase floors: 27 worker phase proofs, 31 coordinator phase proofs, and 8 takeover phase proofs across degree, PageRank, eigenvector, and paired HITS. The remaining remote adapter work is promotion-scale deployment evidence and rollout hardening rather than another owner-boundary proof. | One coordinator and multiple workers drain degree, PageRank, eigenvector, and paired HITS through durable state only. Killing a worker abandons only its page/runtime lease; replacement after expiry completes the page; stale workers cannot complete reclaimed pages; duplicate coordinators cannot publish or fail twice; and active single-vector or paired-HITS service rebuilds do not expose attempt output through public reads. |
| 2 | Degree canary rollout | Put degree behind the first internal distributed-maintenance gate. Degree should exercise scan, reduce, publish, cleanup, page leases, runtime leases, status, and freshness without iterative convergence risk. The first gate is now implemented as an internal decision that admits only one queued/active degree build, blocks non-degree queued/active work, rejects failed/truncated page summaries, and applies a configurable control-record cap before planned rollout proceeds. Queued degree builds are now estimated against that cap before job/page records are created. Planned maintenance reports `rounds_executed`, so canary qualification can assert tight budgets exhaust resumably and expanded budgets finish within their configured round cap. That gate is now selectable through the internal `degree_canary` DB idle maintenance mode, which runs planned degree work when eligible, falls back to local maintenance when queued work is blocked before planned execution starts, and fails fast when already-active planned work is outside guardrails. Direct top-k, traversal projection, and search rerank `published`/`fresh` coverage now runs through the canary path, service-route owner path, real HTTP service owner path, and focused hosted cross-range fan-in while a planned rebuild is active. Repeated failed degree builds now also prove abandoned score/job namespaces are cleaned immediately and recent diagnostics stay bounded. The unit-test aggregate now includes the canary guardrails. | Local-vs-distributed parity, process restart, stale-attempt rejection, cleanup resume, active/failed status summaries, public `published`/`fresh` behavior, scheduler latency budget, storage-growth bounds, and rollback to the local oracle all pass in CI or release qualification. |
| 3 | PageRank production promotion | Promote PageRank after the single-vector iterative matrix is complete. Keep dynamic iteration planning, convergence summaries, fixed-iteration non-converged publish metadata, publish verification, prior-generation preservation, and cleanup as required checks. Process coverage now includes killed-worker exhausted-attempt failure for a later contribution page plus a duplicate coordinator tick that cannot fail the build twice, and service-targeted process coverage includes the active public-read contract for direct top-k, traversal projection, and search rerank. Hosted cross-range fan-in now includes active PageRank shards in both the focused two-shard public-read matrix and a nonuniform eight-shard direct merge layout; both keep the prior published generation mergeable while `fresh` fails closed. Graph-level repeated failed-build coverage now proves abandoned PageRank score/job namespaces are cleaned while diagnostics stay bounded, and unit-test lifecycle coverage now includes later-iteration exhausted-attempt preservation, failed planned-rebuild public-read preservation for direct top-k/traversal/rerank, and DB-level production-budget parity: a 130-source PageRank build exhausts and resumes through one-page planned maintenance rounds, then matches the local oracle status and top-k output. Promotion still needs deployment-scale cleanup, broader hosted fan-in surfaces, and latency evidence. | Distributed PageRank matches the local oracle within tolerance, preserves the previous generation on failed rebuilds, serves prior published scores while building, fails `fresh` with `MetricStale` when stale, recovers from every phase boundary, and passes promotion-scale cross-shard fan-in checks. |
| 4 | Eigenvector single-vector parity | Reuse the PageRank substrate for eigenvector rather than adding metric-specific job state. Eigenvector should contribute only metric math, metadata, and tolerance rules. Service-targeted process coverage now includes the active public-read contract for direct top-k, traversal projection, and search rerank, service-owner replacement coverage, two-worker service-boundary multi-page scan/initialize/contribution/reduce/convergence evidence, hosted cross-range fan-in for active eigenvector direct metric top-k/traversal/rerank while `fresh` fails closed plus nonuniform eight-shard active/stale direct merge coverage, publish/cleanup restart and publish-verifier failure through the service owner boundary, killed-worker exhausted-attempt failure for a later contribution page plus duplicate coordinator idempotence through the process harness, unit-test later-iteration exhausted-attempt preservation, unit-test failed planned-rebuild public-read preservation for direct top-k/traversal/rerank, graph-level repeated failed-build cleanup/storage-growth proof, unit-test scan/initialize/contribution/reduce/convergence reclaim coverage, cleanup resume after reopen, failed planned-build preservation, coordinator publish-failure preservation after reopen, and DB-level production-budget parity through a 130-source one-page-budget planned drain matching the local oracle. | The eigenvector matrix still needs deployment-scale parity, broader promotion-scale fan-in, and latency evidence before promotion. |
| 5 | HITS paired-vector promotion | Keep authority and hub as separate named metric scores with one compatible lifecycle. Authority and hub must share target generation, convergence decision, publish decision, failure decision, and cleanup ownership. Focused hosted fan-in now covers an eight-shard active/stale compatible authority/hub layout whose direct, traversal projection/order/filter, and authority-rerank merges preserve the prior pair and reject `fresh`, while real HTTP service-owner active reads, service-owner replacement coverage, two-worker service-boundary multi-page authority/hub phase coverage, first service killed-owner timing for coordinator and scan worker-pool replacement, killed-worker exhausted hub-reduce attempt coverage through the direct process harness and unit-test lifecycle coverage, unit-test active prior-pair visibility, paired publish idempotence, initialize/contribution/reduce/hub/convergence reclaim, cleanup resume after reopen, failed-build preservation, publish-failure preservation, scan-page reclaim coverage that rejects stale partial writes, failed planned rebuild public-read coverage for prior compatible authority/hub output, paired fixed-iteration metadata after service-owned publish and cleanup, service-targeted publish/cleanup restart, service-targeted publish-verifier failure preservation, graph-level repeated failed-build cleanup/storage-growth proof, and DB-level production-budget parity through a 130-source one-page-budget paired drain cover the paired-HITS `published`/`fresh` contract for direct top-k, traversal projection/order/filter/status, authority rerank, compatible pair freshness after replacement, compatible pair cleanup after publish, failed/exhausted-attempt prior-pair preservation, bounded failed-build diagnostics, and local-oracle parity under bounded scheduler budgets. | HITS remains disabled by default until promotion-scale fan-in, broader deployment-scale owner evidence, and larger-graph latency evidence all preserve the previous compatible pair. |
| 6 | Public read-surface closeout | Treat every score-bearing read path as part of distributed correctness. Building, failed, abandoned, and attempt-scoped output must remain invisible outside status. Unit-test lifecycle coverage now runs the fast-root query/profile/fan-in invariants for direct metric top-k, graph traversal/search metric status, order/filter generation checks, rerank score details, failed status preservation, paired HITS failed-status preservation, malformed shard payloads, and profile generation reporting. Unit-test fan-in coverage combines that fast-root artifact with hosted cross-range graph metric fan-in for compatible published generation merge, unpublished/incompatible generation rejection, nonuniform eight-shard hosted degree/PageRank/eigenvector direct merge coverage with four active/stale shards and invisible unpublished targets, active-stale hosted degree/PageRank/eigenvector traversal projection/order/filter and search rerank published merge plus fresh rejection, compatible HITS authority/hub hosted direct, traversal projection/order/filter, and authority-rerank merge coverage over an eight-shard layout with four active/stale shards plus fresh rejection, remote HITS generation/metadata/edge-filter mismatch rejection, missing remote HITS status rejection, and serializer coverage for representable single-metric shard reads/rerank. Unit-test public API graph metric coverage combines the public graph-query e2e with the public graph metric action route plus generated OpenAPI/client contracts for graph metric status, runtime ownership summaries, active build pages, and graph metric action responses. | Direct top-k, traversal projection/order/filter/status, search rerank, query profile, hosted fan-in, public action/status routes, generated clients, and any future standalone explain surface either read compatible complete published generations or fail closed with `MetricNotReady`/`MetricStale`. Mixed shard generations, missing statuses, zero generations, incompatible edge filters, incompatible HITS pairs, and malformed score/status payloads are rejected before merging. Full promotion-scale shard layouts remain the separate release gate. |
| 7 | Operations and cleanup release gate | Make the executor safe to leave enabled. V1 keeps latest-only retention, bounded diagnostics, immediate cleanup when snapshot-safe, and deferred internal cleanup only when needed for readers. Degree, PageRank, eigenvector, and HITS now have service-boundary cleanup restart evidence after publish plus graph-level repeated-failed-build cleanup/storage-growth evidence across non-iterative, iterative, and paired-vector families. The local promotion-budgeted release summary now also exposes configured/observed operations-floor evidence for active page probes, active leased/detailed status pages, terminal no-work status, untruncated work/status pagination, and finite progress. The release-qualification active-page probe now persists cursor progress after reclaim and rejects status that omits the reclaimed page cursor or completed/total progress units. Unit-test operations coverage now also runs the capped active-page status test that requires every reported active page to carry worker, lease, attempt, cursor/error, and progress-unit details. Release qualification still needs larger retained namespaces and promotion-scale storage-growth evidence under deployment-shaped owner load. | Cleanup resumes after restart for score generations, manifests, pages, phase/iteration summaries, attempts, failures, and runtime-owner records. Direct runtime-owner lease-record tests now prove clean shutdown deletes the current owner record and stale shutdown preserves a replacement owner. Status exposes stable summaries for freshness, phase/iteration progress, owner hashes, worker counts, lease state, takeover/lost-lease counters, page counters, bounded failures, and cleanup progress without raw storage keys or attempt namespaces. |
| 8 | Default widening | Widen the conservative `auto` gate one family at a time: degree, PageRank, eigenvector, then HITS. Keep local runners as CI/debug oracles until distributed parity and operations evidence are routine. | A metric family becomes distributed-by-default only after parity, remote ownership, crash/restart, cleanup, public-read, fan-in, status, operations, latency-budget, generated-client, docs, and rollback checks are all green for that family. |

The critical dependency is owner correctness before default promotion. A family
must not become distributed-by-default while duplicate coordinators can race
publish/fail, duplicate workers can write stale attempts, cleanup can grow
without bound, or fan-in can merge incomparable generations. Degree is the
canary because it proves the mechanics cheaply; PageRank proves iterative
single-vector execution; eigenvector proves the substrate is generic; HITS
proves paired-vector failure and publish semantics.

Recommended follow-up release cuts from the current checkpoint:

1. **Promotion-scale remote owner evidence**: keep the existing service-targeted
   owner boundary as the contract, then widen larger coordinator/worker-pool
   deployments against realistic page counts and failure timing. This PR should
   not add a new public job API. Degree, PageRank, eigenvector, and paired HITS
   now have first two-worker service-boundary multi-page proofs for
   non-iterative, single-vector iterative, and paired-vector iterative phase
   families. Degree, PageRank, eigenvector, and paired HITS now also have first
   multi-page service killed-owner timing proofs: killed coordinators and killed
   worker-pools fence duplicate owners before TTL, allow replacement takeover
   after TTL, and continue the same 130-source builds through publish and
   cleanup. The process-harness summary now makes those killed-owner timing
   proofs auditable as explicit required/observed service multi-page
   coordinator-takeover and worker-pool-takeover counters, plus a separate
   required/observed service cleanup-takeover counter for killed cleanup owners
   after publish. It also breaks `remote_owner_release_gate` into
   `service_remote_owner_release_gate`, `direct_remote_owner_release_gate`, and
   `failure_reclaim_release_gate` so promotion tooling can tell whether missing
   evidence is service-boundary, direct-boundary, or failure/reclaim coverage.
   Degree, PageRank, eigenvector, and paired HITS service-targeted cleanup now
   also have killed-owner timing after publish: a cleanup worker-pool can die
   mid-cleanup, duplicate cleanup ownership is fenced before TTL, and a
   replacement owner takes over after TTL and finishes to fresh scores,
   fixed-iteration metadata, or a fresh compatible HITS pair. The
   future rollout evidence should reopen
   the DB handle between every progressing maintenance tick to prove durable
   resume for degree, PageRank, eigenvector, and paired HITS. The remaining work
   is to extend killed-owner evidence to promotion-scale deployments and keep
   runtime summaries bounded, no-error, and useful. The process harness now
   proves reclaimed pages are completed by the replacement worker under a newer
   attempt before the stale owner is rejected, so stale workers cannot silently
   complete reclaimed direct-process pages. The process-harness summary now
   makes those direct abandoned-attempt checks auditable with separate
   required/observed reclaimed-attempt-completion and stale-attempt-rejection
   counters in addition to the generic direct page-reclaim phase count.
2. **PageRank production gate**: finish the iterative single-vector promotion
   matrix under remote owners. DB-level production-budget local/planned parity
   now covers a 130-source one-page-budget planned drain against the local
   oracle. Existing graph-level and process-level coverage also covers
   later-iteration restart/retry, fixed-iteration non-converged publish
   metadata, publish-verifier failure preserving prior output,
   exhausted-attempt failure under killed process owners with duplicate
   coordinator idempotence, failed planned-rebuild public-read preservation for
   direct top-k/traversal/rerank, cleanup resume, cleanup killed-owner
   replacement, and active public-read freshness through local, planned,
   service-owned, and hosted fan-in paths. The remaining work is promotion-scale remote-owner
   deployment cleanup, larger cross-shard fan-in, and latency data.
   PageRank promotes only when `published` and `fresh` reads behave the same
   under local, planned, service-owned, and hosted fan-in execution.
3. **Eigenvector parity gate**: reuse the PageRank single-vector executor
   instead of adding metric-specific job machinery. DB-level
   production-budget parity now covers a 130-source one-page-budget planned
   drain against the local oracle. Unit-test lifecycle coverage now covers scan,
   initialize, contribution, reduce, and convergence reclaim; cleanup resume
   after reopen; failed planned-build preservation; coordinator publish-failure
   preservation after reopen; and failed planned-rebuild public-read
   preservation for direct top-k/traversal/rerank while `fresh` fails closed.
   The remaining work includes deployment-scale parity, operations evidence,
   promotion-scale fan-in, and latency data.
4. **HITS paired-vector gate**: treat HITS as the last default-promotion family.
   Authority and hub stay separate named metrics, but compatible configs share
   one target generation, convergence decision, publish decision, failure
   decision, and cleanup owner. DB-level production-budget parity now covers a
   130-source one-page-budget planned drain against the local compatible-pair
   oracle. Scan-page reclaim now rejects stale partial writes before replacement
   completion. Failed planned rebuilds now preserve the prior compatible pair
   for authority/hub direct `published` reads, traversal projection/order/filter
   and status, and authority rerank; omit failed-generation attempt output; and
   make `fresh` reads fail with `MetricStale`. Direct process coverage now also
   exhausts a killed hub-reduce page attempt sequence, fails the compatible pair
   once, preserves the previous pair, and fences duplicate coordinator failure.
   This cut must still finish promotion-scale fan-in, broader deployment-scale
   owner evidence, operations evidence, and larger-graph latency
   before any default HITS execution is enabled.
5. **Cleanup and storage-growth gate**: broaden the current latest-only cleanup
   proof from graph-level repeated failed builds and service-boundary cleanup
   restart into deployment-scale qualification. Degree, PageRank, eigenvector,
   and paired HITS now add service-boundary killed-owner cleanup proofs after
   publish. Degree, PageRank, eigenvector, and HITS now prove repeated failed
   planned builds remove abandoned job namespaces and unpublished score
   generations while bounding recent diagnostics. Unit-test coverage now makes that evidence explicit in CI: cleanup pages resume after
   reopen, active jobs refuse direct cleanup, failed and repeated failed builds
   remove abandoned namespaces while bounding diagnostics, and runtime owner
   lease cleanup is fenced. Future rollout evidence should add aggregate
   storage-footprint evidence for degree, PageRank, eigenvector, and
   paired HITS: successful planned cleanup leaves zero durable job namespace
   records, zero attempt records, and exactly one retained metric-control
   record; repeated failed builds preserve the prior score-record count, remove
   abandoned failed job and attempt namespaces, verify retained metric-record
   counts against published score records plus fixed per-metric metadata and
   bounded failure diagnostics, emit retained
   score/metric/control/job/attempt/failure/event record counts, and keep
   retained control/failure/event record counts bounded under the configured
   ceiling. The harness now also records cleanup tick count and cleanup elapsed
  time from the planned-maintenance/status boundary; zero cleanup ticks is
  valid when a tiny workload finishes cleanup in the publish round, while
  larger iterative and paired runs expose separate cleanup cost. Promotion
  release qualification now requires a configured cleanup latency ceiling and
  fails the observed deployment-shaped gate when the max cleanup phase exceeds
  that ceiling. Promotion qualification also exposes failure-churn as its own
  configured/observed floor: the promotion profile must request the repeated
  failed-build floor and bounded diagnostics, every completed family must
  observe that retry volume, paired HITS must report compatible paired
  diagnostics, and failed cleanup must leave zero job and attempt namespaces
  with only bounded failure/event records retained. Rollout qualification should
  also close and reopen the planned DB between
  nonterminal maintenance ticks to prove durable scheduler state resumes after
  handle/owner restart boundaries. The remaining gate is larger retained
  namespaces, deployment-shaped killed-owner churn with abandoned in-flight
  attempts, and promotion-scale storage growth under longer-running owner
  churn. Larger retained namespaces can be a future admin/debug option, but v1
  correctness should not depend on a retention knob.
6. **Latency and default-widening gate**: widen `auto` one family at a time only
   after scheduler tick latency, public read latency, cleanup cost, and storage
   growth are measured. The order stays degree, PageRank, eigenvector, then
   HITS. Multi-metric indexes, larger single-vector workloads, default HITS,
   and incompatible HITS pairs remain gated until fairness and fan-in coverage
   are routine. The unit-test aggregate now makes the current
   conservative default boundary explicit in CI: safe degree/PageRank/eigenvector
   cases and compatible opt-in HITS can use planned maintenance, larger or
   incompatible cases fall back before local execution, and threshold widening
   remains an intentional internal decision rather than an accidental default.
   Rollout qualification should include latency budgets for local oracle
   publish, planned publish, cleanup, published reads, fail-closed fresh reads,
   and synthetic fan-in merge/fail-closed paths. Promotion evidence should
   include conservative public-read, fresh-failure, and fan-in latency ceilings
   so active/failed direct,
   traversal, rerank, paired HITS, and fan-in read paths become pass/fail
   evidence instead of log-only measurements. It also has disabled-by-default retained score/metric/control/failure/event
   storage, page-claim, cleanup-tick, executed-round, failure-retry, worker-step, and
   coordinator-step budget flags so promotion runs can turn observed storage and
   scheduler footprints into pass/fail gates after real baselines exist, without
   baking arbitrary timing, storage, page-count, retry-count, or role-step
   thresholds into PR CI.
7. **Operations and client surface gate**: stabilize the status fields exposed
   through OpenAPI, generated clients, docs, and dashboards. Operators should
   see freshness, phase/iteration progress, owner hashes, worker counts, lease
   state, takeover/lost-lease counters, page counters, bounded failures, cleanup
   progress, and last error summaries. They should not see raw storage keys,
   page ids, attempt namespaces, or process-local writer details as public API.
   Unit-test operations coverage now makes capped active-page status
   details, failed-page cursor/progress/error diagnostics, the
   runtime-summary OpenAPI/client shape, graph index encoders,
   internal service maintenance route, and command/supervisor telemetry contract
   explicit in PR and full-default Zig CI. Unit-test public API graph metric coverage now also
   pins the public action route and generated client-facing graph metric status
   types so client drift is caught with the public graph metric read surface.

The remaining promotion work should be tracked as rollout evidence rather than
PR unit coverage or graph-specific build targets. The local process harness is
the canonical rollout-summary producer: it accepts `--profile smoke` and
`--profile promotion`, emits one `graph_metric_process_harness_summary` row, and
reports top-level rollout, public-read, remote-owner, service, direct, and
failure/reclaim gates. Hosted or deployment-sized qualification should consume
that summary shape instead of adding standalone graph-metric Make targets.

- The rollout qualification path
  runs degree, PageRank, eigenvector, and paired HITS through the graph-index
  metric API, drains planned maintenance with configurable graph size, worker
  count, tick budget, metrics-per-round budget, page budget, iteration cap,
  deterministic graph fanout, and maintenance mode, compares planned top-k
  output against the local oracle, and checks that an active rebuild keeps
  `published` reads on the prior generation while `fresh` fails closed. During
  that active rebuild it also verifies that status reports the prior published
  generation, target building generation, active job id, finite progress in the
  `[0, 1]` range, and a bounded page-status payload. During that check the
  harness deliberately claims one durable build page with a stale probe worker
  id, verifies another worker cannot steal the live lease early, reclaims that
  same page after its stored lease expiry, and then requires active status to
  expose the reclaimed page's worker/attempt/lease summary while pending-work
  stats count an active page. The active rebuild can be preceded by multiple
  independent write/derive mutation cycles with
  `--active-mutation-writes`, and the harness requires the active target
  generation to advance by exactly that count. It emits both the resulting
  active target generation and generation delta so release runs can prove dirty
  graph generations coalesce behind one unpublished rebuild while `published`
  remains pinned to the prior complete generation. Active rebuilds also exercise
  graph traversal projection, metric ordering, metric filtering, and graph
  search rerank: `published` reads must use the prior complete generation and
  report the active state, while `fresh` traversal/rerank reads must fail with
  `MetricStale`. The same active and failed published-read windows now encode
  public query profile output and require explainable graph metric entries for
  the direct metric source, graph traversal source, and graph metric rerank
  source, all reporting the prior published generation and the observed
  building or failed status. The HITS run also issues paired authority/hub
  direct metric reads plus paired traversal projection and graph search rerank
  reads during active and failed rebuilds: `published` must return
  prior-generation results and paired traversal projection/profile output must
  include both authority and hub metric scores, while `fresh` reads must fail
  closed. The harness also injects
  repeated failed rebuilds after the active-build read check and verifies that
  direct, traversal, and graph search rerank `published` reads still serve the
  prior generation; direct, traversal, and rerank `fresh` reads still fail
  closed; the prior generation is preserved in failed status; and bounded failure
  diagnostics stay under a configurable retained-record ceiling across multiple
  failed target generations. The harness now also records aggregate metric
  storage footprints and verifies successful cleanup leaves no durable job
  namespace records, exactly one retained metric-control record remains, exactly
  one retained score generation remains for the current graph shape, repeated
  failed generations do not increase retained score records, abandoned failed
  job namespaces are removed, retained score/metric/control/failure/event
  record counts are emitted, retained metric records match the published score
  records plus fixed per-metric metadata overhead, and retained failure/event
  records match the configured bounded retention window exactly:
  `min(failure_repeats, max_failure_diagnostics)` failures and
  `min(failure_repeats + 1, max_failure_diagnostics)` events per metric, doubled
  for compatible HITS pairs. Failed retained metric records must equal the fresh
  retained metric-record count plus the bounded retained failure records.
  Retained control records must stay within the combined diagnostic ceiling. It
  also verifies that terminal fresh and terminal failed statuses do not expose
  active build pages, truncated page payloads, job ids, worker ids, or cursors.
  Cleanup tick and elapsed-time evidence must agree: runs with no cleanup ticks
  must report zero cleanup latency, while runs that execute cleanup ticks must
  report nonzero cleanup elapsed time.
  For paired HITS, the harness verifies that authority and hub retained event
  and failure diagnostics stay in sync and that paired retained diagnostic
  storage remains bounded. With `--reopen-between-ticks`, it also closes and reopens the
  planned DB after each progressing nonterminal tick, forcing PageRank,
  eigenvector, degree, and paired HITS to resume from durable graph-index state
  rather than in-memory scheduler state. The reopen count is checked as a
  correctness gate: enabled reopen runs must report one reopen for every
  nonterminal maintenance tick, while disabled reopen runs must report none. The
  smoke and promotion profiles enable reopen by default; focused control runs
  can pass `--no-reopen-between-ticks` to validate the zero-reopen branch. With
  `--maintenance-mode split`, the harness drives
  coordinator-before, worker-pool, and coordinator-after sweeps directly instead
  of the combined idle loop, giving release runs a cheap deployment-shaped
  preflight for the same durable coordinator/worker boundary.
  Split release runs must now prove that every worker-pool sweep is bracketed by
  exactly two coordinator sweeps and that worker-pool sweeps match executed
  scheduler rounds, while combined release runs must prove one combined sweep per
  tick with no split-role sweeps.
  The harness now also samples the DB pending-work graph metric stats: before
  draining it requires exactly one canonical queued build and no active pages,
  during the active rebuild it requires the scheduler to report active work
  rather than queued or failed work, after fresh publish/cleanup it requires no
  queued/active/failed page work, and after repeated failed rebuild cleanup it
  requires the failed terminal metric to leave no scheduler-visible work. This
  turns the operational pending-work surface into release evidence rather than
  only ad hoc status output. Before the first planned publish, the harness now
  also issues direct graph metric top-k reads, graph traversal reads, and graph
  search rerank reads. Direct top-k and rerank reads with both `published` and
  `fresh` freshness must return `MetricNotReady`. Traversal projection with
  `published` must return null metric scores plus `not_ready` metric status,
  while traversal fresh projection and traversal published ordering/filtering
  must fail closed with `MetricNotReady`. HITS runs the same pre-publish
  not-ready checks for the hub side, so a compatible pair cannot accidentally
  expose one side before either side has a complete generation. The same
  pre-publish probe checks graph metric status: primary metrics, and the HITS
  hub side, must report `not_ready`, no published or building generation, a
  queued target generation equal to the current edge generation, finite zero
  progress, and no active page payload.
  Each family run also builds a
  synthetic direct metric fan-in probe from the published top-k
  results: compatible shard payloads must merge to the same top-k ordering, an
  active/building shard with the same prior published generation must still
  merge for `published`, a mixed active shard set with both `building` and
  `failed` status must still merge the prior published score generation for
  `published` while reporting the higher-severity failed status, the same active
  fan-in must fail closed for `fresh`, and a deliberately incompatible
  published generation must fail closed through the normal query merge path.
  Deliberately incompatible metric metadata
  versions and edge filters must also fail closed before score mixing, proving
  release fan-in compares metric compatibility as well as generation. Shards
  that report mismatched index, metric, or status-name identity must fail closed
  before score mixing. A shard that omits the requested metric result must also
  fail closed before score mixing. A shard that duplicates a requested metric,
  returns an unrequested metric, carries a non-finite score, reports a
  non-finite status number, reports out-of-range progress, or reports an
  invalid published state such as `not_ready` or `disabled` must also fail
  closed before score mixing. The
  synthetic shard splitter is itself a correctness gate: shard ranges must
  exactly cover the published score set in order and stay balanced to within one
  score, so increasing `--synthetic-fan-in-shards` exercises a real merge layout
  instead of silently creating a malformed fixture. The active/stale fan-in
  breadth is controlled separately by `--synthetic-fan-in-active-shards`, so
  promotion runs can prove more than the minimum two active shard statuses while
  keeping the same merge layout.
  HITS runs an additional paired authority/hub synthetic fan-in probe where each
  shard must provide both requested metric results; compatible pairs merge
  together, active pairs preserve the prior published generation for
  `published`, mixed active pairs with failed and building shard statuses still
  preserve the prior compatible pair for `published`, `fresh` rejects active
  pairs, a missing authority/hub metric result rejects,
  duplicate/unrequested/non-finite score payloads reject,
  non-finite status numbers and out-of-range progress reject, mismatched
  index/metric/status-name identity rejects, invalid published states reject,
  and incompatible authority/hub generation, metadata-version, and edge-filter
  pairs reject before merge output is published.
- The rollout runner should emit structured JSONL for each run: graph size, edge count,
  metric family, maintenance mode, target generation, tick count, budget
  exhaustion, generated graph topology evidence, configured round/metric/page
  budgets, worker and coordinator step counts, combined/coordinator/worker-pool
  sweep counts, pre-drain metrics-scanned and queued-build counts, pre-publish
  not-ready direct-read, rerank, traversal projection, traversal fail-closed,
  and status counts, page counts,
  phase advances, publish/failure counts, local/planned publish latency, active page probe
  claim/reclaim flags, active status page count, leased-page count,
  detailed-page count, cursor-bearing page count, progress-bearing page count,
  active status truncation count, truncation flag, progress, cleanup tick count, cleanup elapsed time, DB reopen
  count, configured worker identity count, split worker identities that made
  tick progress, split worker identities that made page claim/completion
  progress, split worker min/max page-progress counts, configured active
  mutation write count, active target generation, active generation delta,
  HITS active paired published-read latency, published-result count,
  published-score count, fresh-failure latency, fresh-rejection count, HITS
  active paired rerank published-result count, HITS active paired rerank
  fresh-rejection count, HITS active paired traversal metric-result
  count, active published-read latency, active published-read result count,
  active fresh-failure latency, active fresh-rejection count, active
  direct top-k score count, active rerank published-read latency, active rerank
  published-result count, active rerank fresh-failure latency, active rerank
  fresh-rejection count, active traversal published-read latency, active
  traversal fresh-failure latency,
  active traversal published-check count, active traversal fresh-rejection
  count, active profile graph-metric entry count, failed-build published-read
  latency, failed-build published-read result count, failed-build fresh-failure
  latency, failed-build fresh-rejection count, failed-build rerank
  published-read latency, failed-build rerank published-result count,
  failed-build rerank fresh-failure latency, failed-build rerank
  fresh-rejection count, failed-build traversal published-read latency,
  failed-build traversal fresh-failure latency, failed-build traversal
  published-check count, failed-build traversal fresh-rejection count,
  failed-build direct top-k score count, failed-build profile graph-metric entry count,
  configured failure
  repeat count, final retry count, configured retained-diagnostic ceiling,
  bounded recent event/failure counts, retained expected-error failure-record
  counts, retained failed-event counts, paired HITS event/failure/diagnostic
  shape counts,
  pre-drain/fresh/active/failed paused-metric counts, expected score
  record count, synthetic fan-in shard count, min/max scores per synthetic
  shard, active synthetic shard count, terminal merged score count,
  active-shard published merged score count, mixed-active published merged score
  count, active-shard fresh rejection count,
  zero-generation, incompatible-generation, metadata-version, edge-filter,
  missing-metric, duplicate-metric, extra-metric, non-finite-score,
  non-finite-status, out-of-range-progress, identity-mismatch, and invalid-state rejection counts, terminal fan-in merge latency,
  active-shard published fan-in latency, mixed-active published fan-in latency,
  active-shard fresh fail-closed latency,
  zero-generation, incompatible-generation, metadata-version, edge-filter,
  missing-requested-metric, duplicate-metric, extra-metric, non-finite-score,
  non-finite-status, out-of-range-progress, identity-mismatch, and invalid-state fail-closed latencies,
  paired HITS fan-in metric-result count, paired HITS min/max scores per
  synthetic shard, paired active synthetic shard count, paired
  active-published metric-result count, paired mixed-active published
  metric-result count, paired
  fresh/zero-generation/generation/metadata-version/edge-filter/missing/duplicate/extra/non-finite/status-non-finite/progress/identity/state
  rejection counts, paired terminal/active/mixed-active/fresh-fail/
  zero-generation-fail/generation-fail/metadata-fail/edge-filter-fail/missing-fail/duplicate-fail/
  extra-fail/non-finite-fail/status-non-finite-fail/progress-fail/identity-fail/state-fail fan-in latencies,
  HITS failed paired published-read latency, published-result count,
  published-score count, fresh-failure latency, fresh-rejection count, HITS
  failed paired rerank published-result count, HITS failed paired rerank
  fresh-rejection count, HITS failed paired traversal metric-result
  count, active/fresh/failed pending-work summaries, fresh/failed status page
  counts, terminal status truncation flags, fresh convergence/non-convergence
  counts, iterations-completed value, positive-delta count, computed-at count,
  local/planned top-k and status parity check counts,
  fresh/failed score, metric, control, job-namespace, and attempt-record
  counts, failed failure/event record counts, and parity summary.
  Those footprint fields are verified, not merely logged: expected, fresh, and
  failed score-record counts must match the generated graph shape exactly,
  fresh and failed metric-record counts must match score records plus fixed
  retained metadata and bounded failed diagnostic records, fresh and failed job
  namespace counts must be zero, fresh and failed attempt-record counts must be
  zero, the fresh terminal state must retain exactly one metric-control record,
  failed failure/event records must match the bounded diagnostic window exactly,
  and failed control records must stay within that same diagnostic ceiling.
  Repeated-failure diagnostics
  are shape-checked too: the terminal failed status must expose a failed last
  event, the expected `InvalidGraphMetricScore` last error, retained failure
  records with nonzero job ids, target/score generations newer than the
  published generation, non-idle phases, retry counts, and the expected error,
  plus retained failed events that preserve the prior published generation and
  carry no score count. Compatible HITS pairs must satisfy the same diagnostic
  shape on both authority and hub.
  The emitted graph topology and active-generation fields are also verified as
  result-level evidence: node/edge totals, source/sink/authority counts,
  sink/cycle/bipartite/self-edge counts, max out-degree, expected score-record
  count, active mutation count, active target generation, and active generation
  delta must all match the selected family and workload knobs.
  Published metadata is a result-level gate too: degree must publish converged
  metadata with one completed iteration, zero positive deltas, and a computed
  timestamp; PageRank, eigenvector, and compatible HITS must publish computed
  timestamps, bounded iteration counts at or below `--max-iterations`, and if
  they publish non-converged output, every metric in the family must report
  `converged: false`, positive finite delta evidence, and
  `iterations_completed == --max-iterations`.
  Local-oracle parity is no longer score-only: each family metric must match the
  local oracle top-k ordering and scores, and its planned published status must
  match the local status for published generation, edge generation, complete
  state, metadata version, convergence flag, iteration count, and finite delta
  within tolerance. HITS must pass the same parity checks for both authority and
  hub.
  Optional budget flags can fail a run when local publish, planned publish,
  cleanup, published read, fail-closed fresh read, HITS paired direct
  published/fresh read, or synthetic fan-in latency exceeds the selected release
  baseline; the public-read gates are `--max-published-read-latency-ns` and
  `--max-fresh-fail-latency-ns`, and the fan-in gate is
  `--max-fan-in-latency-ns`. These threshold checks run after the
  family result JSONL is emitted, so a budget failure still leaves the measured
  evidence needed to set or adjust the baseline. Separate disabled-by-default storage gates
  `--max-storage-score-records`, `--max-storage-metric-records`,
  `--max-storage-control-records`, `--max-storage-attempt-records`,
  `--max-storage-failure-records`, and `--max-storage-event-records` fail a run
  when either the fresh terminal footprint or repeated-failure footprint exceeds
  the selected release baseline. Scheduler footprint gates
  `--max-page-claims` and `--max-cleanup-ticks` fail a run when page churn or
  cleanup scheduler work exceeds the selected release baseline.
  `--max-rounds-executed` and `--max-failure-retry-count` make bounded scheduler
  progress and bounded failed-build retry volume explicit release gates.
  `--max-worker-steps` and `--max-coordinator-steps` bound how much work each
  runtime role consumed before publish, cleanup, and failure verification.
  `--min-families-run` fails the final summary when a release run completes
  fewer metric families than the configured floor, making all-family promotion
  coverage an explicit gate instead of an inference from logs.
  `--min-split-worker-identities-with-progress` and
  `--min-split-worker-identities-with-page-progress` fail a split-mode run when
  too few configured worker identities make tick progress or actual page
  claim/completion progress. The promotion evidence profile should set both to the
  four-worker promotion floor, so promotion evidence cannot pass while silently
  serializing page ownership through fewer worker identities. The rollout
  runner should finish successful runs with a summary JSONL event that repeats the
  configured latency/storage/scheduler budgets, marks whether any budget was
  enabled, breaks that budget evidence into latency, storage, scheduler, and
  coverage-floor categories, records whether the promotion profile floor was
  enforced, and marks `deployment_shaped_release_gate` in the config row when
  the invocation requests the promotion profile with all-family execution, split
  ownership, reopen evidence, a four-family floor, split worker identity floors
  at least as strict as the configured worker count, public-read/fresh/fan-in
  latency ceilings, retained storage ceilings, and scheduler ceilings. The
  final summary row marks the same flag only after observing all four metric
  families, per-family split-worker progress/page-progress floors, and the
  promotion fan-in floor: the selected shard count must be exercised for every
  family, active/stale shard counts must reach the configured promotion floor,
  and the observed primary plus paired-HITS layouts must be nonuniform. Worker
  floors use the minimum observed family contribution rather than aggregate
  worker totals. That flag is
  release-tooling metadata for local evidence: it proves the run used and
  satisfied the deployment-shaped local gate with all local promotion budget
  categories enabled, but it does not replace hosted remote-owner, killed-owner,
  or cross-range promotion evidence. The config and summary rows should expose
  configured and observed deployment-shaped evidence flags so rollout tooling can distinguish
  a correctly shaped invocation from a completed promotion run that actually met
  the observed floors. The config and summary rows also emit the component audit booleans
  `all_family_execution`, `public_read_fan_in_latency_budgeted`,
  `cleanup_latency_budgeted`,
  `retained_storage_budgeted`, `promotion_scheduler_budgeted`,
  `split_worker_progress_floor_configured`, and
  `split_worker_page_progress_floor_configured`, and
  `promotion_fan_in_floor_configured`, and
  `promotion_failure_churn_floor_configured`, and
  `promotion_operations_floor_configured`; the summary adds
  `public_read_fan_in_latency_budget_observed`,
  `cleanup_latency_budget_observed`, `retained_storage_budget_observed`,
  `promotion_scheduler_budget_observed`, `split_worker_progress_floor_observed`, and
  `split_worker_page_progress_floor_observed`, plus
  `promotion_failure_churn_floor_observed` and
  `promotion_operations_floor_observed`. The latency observed flag is
  true only when published-read, fresh-fail, and fan-in latency caps are
  configured and the max observed surfaces stay within them. The promotion
  operations floor also emits `min_observed_active_status_pages` and
  `max_observed_active_status_pages`; the observed flag stays false unless each
  family produced at least one active status page and the largest observed
  active status page count stayed within `max_status_pages`. The cleanup
  latency observed flag is true only when a cleanup latency cap is configured
  and the max observed cleanup phase stays within it. The
  retained-storage observed flag is true only when configured score, metric,
  control, attempt, failure, and event caps are present, every completed family
  stayed under those caps, retained score/metric/control/failure/event record
  counts match the expected fresh and failed generations, and cleanup left zero
  job and attempt namespace records. The scheduler observed flag is true only
  when page-claim, cleanup, round, retry, worker-step, and coordinator-step caps
  are configured and the completed summary stayed within them while publishing
  exactly the completed families. The worker observed flags are true only when
  the matching floor was configured and every completed family met it, plus
  `promotion_fan_in_floor_observed`, which is true only after the final summary
  proves every completed family used the configured shard/active-shard shape and
  the expected nonuniform primary or paired-HITS fan-in layout, and
  `promotion_failure_churn_floor_observed`, which is true only after repeated
  failed-build retries, bounded diagnostics, paired-HITS diagnostic records, and
  zero retained failed job/attempt namespaces match the promotion floor. The
  operations observed flag is true only when every completed family exposes an
  active page probe, the reclaimed page is fenced from stale completion, the
  reclaimed probe page persists cursor-bearing progress, active status pages
  carry leased, detailed, cursor-bearing, and progress-bearing page counts,
  active status truncation is zero, terminal fresh/failed work is empty and
  untruncated, and active progress is finite and strictly between zero and one,
  proving the status sample is in-flight rather than empty or terminal. Rollout tooling
  can therefore tell which local promotion-gate category is absent without
  reverse-engineering every raw knob. When a deployment-shaped rollout
  requirement is set, the runner should require the completed summary to satisfy
  the observed deployment-shaped evidence flag;
  a correctly shaped invocation that fails to produce the observed family,
  worker, fan-in, failure-churn, public-read latency, cleanup latency, storage,
  or scheduler evidence fails the promotion run instead of only emitting a
  false summary flag.
  The summary repeats the selected workload shape for documents, fanout, top-k,
  synthetic fan-in shards,
  synthetic active fan-in shards, active mutation writes, workers, max
  ticks, rounds per tick, metrics per round, pages per round, max iterations,
  failed rebuild repeats, retained diagnostics, status pages, reopen mode, and
  tolerance, repeats the matching promotion floor values, records the number of
  metric families run, records explicit degree/PageRank/eigenvector/HITS family
  counters, and reports total and maximum observed latency across those families
  for local oracle publish, planned publish, published public reads,
  fail-closed fresh reads, and synthetic fan-in. When a release run requests a
  four-family summary floor, those counters are verified so a promotion summary
  cannot pass unless each family ran exactly once. The config JSONL row
  emits the same selected workload, floor, and budget-category fields before
  work starts, so release tooling can classify failed or interrupted runs and
  prove a successful promotion run used at least the required workload shape
  from either the starting config or final summary while still retaining the
  per-family records for diagnosis. The summary row now aggregates
  graph-shape and active-generation
  proof: total actual and expected graph nodes/edges, source/sink/authority
  nodes, sink/cycle/bipartite/authority-self edge components, maximum observed
  out-degree, total successful-generation repeats and delta, min/max
  per-family successful-generation repeat and delta counts, total active
  mutation writes, and total active generation delta.
  Those totals are verified against the configured document count, fanout,
  active mutation count, and degree/PageRank/eigenvector/HITS family mix, so
  promotion tooling can prove from the final row that the run exercised the
  intended workload shape before trusting latency, storage, or public-read
  evidence. The summary row now aggregates scheduler execution proof too:
  maximum allowed total ticks, observed ticks, budget-exhausted family count,
  combined/coordinator/worker-pool sweeps, maximum allowed drain scheduler
  scans, observed drain scheduler scans, pre-drain scan totals plus maximum
  allowed pre-drain scans, active-build observations, expected and observed
  build starts, worker steps, coordinator decision count, coordinator steps,
  maximum allowed page claims, page claims/completions, phase advances, expected
  and observed publishes, expected and observed failed builds, maximum allowed
  executed rounds, observed executed rounds, total configured worker identities,
  total split worker identities that made tick progress, and total split worker
  identities that made actual page claim/completion progress. These totals are
  verified against the selected combined or split maintenance mode and selected
  tick/round/page/metric-scan budgets for both drain and pre-drain scans, so a
  release summary cannot pass while a combined run silently uses split-role
  sweeps, a split run omits the coordinator-before/worker-pool/coordinator-after
  shape, a split run reports impossible worker ownership totals, the aggregate
  scheduler footprint exceeds the configured resumable bounds, or the run
  finishes without build start, active-build observation, worker page,
  coordinator, phase-advance, publish, zero-failure evidence, and
  budget-exhaustion evidence whenever a family needed multiple ticks to finish.
  Page-claim, executed-round, worker-step, and coordinator-step evidence now
  also emits min/max per-family bounds and requires nonzero minima, so the final
  row cannot hide a family that did no scheduler work behind aggregate totals.
  Page-completion totals must cover phase advances, and aggregate coordinator
  decisions must fit inside the observed coordinator step count, so the summary
  row cannot claim publish/phase/failure decisions that were not accounted for
  by scheduler execution.
  When worker-identity floors are configured, the final summary also
  verifies that each completed family contributed the required number of
  progressing and page-progressing worker identities by checking the minimum
  observed family counts, not only aggregate totals or per-family rows. The same summary row records total reopen count,
  cleanup ticks, and cleanup latency. Reopen totals are verified against
  `reopen_between_ticks`, so reopen-enabled release runs prove a DB-handle
  boundary after every nonterminal tick, while non-reopen runs cannot report
  synthetic reopen evidence. Cleanup tick and latency totals are verified as a
  pair, so release tooling can distinguish no separate cleanup work from
  measured cleanup work without relying on per-family rows. The summary latency
  totals are verified too: successful runs must report nonzero local/planned
  execution, public-read, fresh-failure, and fan-in latency evidence. The final
  row emits min/max per-family latency evidence for those required surfaces, and
  verification requires every minimum to be nonzero, every minimum to be no
  larger than its maximum, and every maximum to be bounded by the matching
  total. When latency,
  storage, page-claim, cleanup-tick, executed-round, failed-retry, worker-step,
  or coordinator-step ceilings are configured, the final summary re-checks
  those ceilings against the observed maxima, so promotion tooling can trust the
  final row without replaying every per-family record. Retained metric-record
  expected totals are emitted next to the observed totals, along with the metric
  slot count and fixed per-metric metadata overhead used to compute them. The
  same storage block emits the expected fresh control-record total and the
  failed control-record ceiling derived from retained diagnostics. Those
  expected totals are re-checked in the final row against the generated
  score-record count, the fixed per-metric metadata overhead, and retained
  failed diagnostic records, so storage-growth evidence cannot pass while
  hiding extra metric namespace entries or unbounded control records. The same
  block now emits min/max observed retained score, metric, control, attempt,
  failure, and event records; deployment-shaped retained-storage evidence stays
  false unless every family retains nonzero score/metric/control/diagnostic
  evidence, attempt namespaces are fully cleaned up, and the max bounds remain
  within the configured ceilings.
  Failed-build
  diagnostic totals are included as status-level evidence too: total retry
  count, recent failure/event counts, retained expected-error records, retained
  failed-event records, expected and observed storage failure/event records, and
  the paired-HITS equivalents. Those totals are verified against
  `failure_repeats`, `max_failure_diagnostics`, and the HITS paired-family
  count, so final-row release evidence proves bounded diagnostics
  were retained and shaped correctly, not only that the underlying storage
  record counts stayed bounded. The summary row also aggregates operational
  scheduler-state proof: total pre-drain queued builds, min observed pre-drain
  scheduler scans, min observed queued builds, total fresh-terminal pending work
  plus its active-build/page, failed-page, paused-metric, and truncation
  components, fresh terminal status page and truncation counts, total active
  builds, active pages plus min/max observed per-family active page bounds,
  active failed-page, paused-metric, and truncation components, active status
  pages, active page-probe claim and reclaim counts,
  active leased pages, active detailed pages, active cursor-bearing pages,
  active progress-bearing pages, active status truncation count, the observed active-progress range, total failed-terminal pending work plus its active-build/page,
  failed-page, paused-metric, and truncation components, and failed terminal
  status page and truncation counts. Those
  totals are verified against the families that actually ran, so release
  tooling can prove from the final row that every family started as exactly one
  queued build, every family had nonzero scheduler scan and active page
  evidence, every active rebuild exercised a claim plus post-expiry reclaim of a
  durable build page, active rebuilds exposed bounded leased page detail with
  finite in-flight progress, and fresh and failed terminal states left
  no scheduler-visible work, paused metrics, truncated page summaries, or active
  page payloads. The summary row
  also aggregates the public read-surface proof:
  expected and observed primary pre-publish `MetricNotReady` surfaces, expected
  and observed paired-HITS pre-publish `MetricNotReady` surfaces, expected and
  observed primary published-read surfaces, expected and observed primary fresh
  rejections, total graph metric profile entries, expected and observed
  paired-HITS published-read surfaces, expected and observed paired-HITS fresh
  rejections, total primary fan-in rejections, and total paired-HITS fan-in
  rejections.
  The same final row breaks active and failed public reads down by surface:
  direct metric reads, search rerank reads, traversal projection/order/filter
  checks, and paired-HITS direct/rerank/traversal reads. Those read-type
  counters are verified independently from the expected aggregate surface
  totals, so a release summary cannot pass by exercising only one public read
  path while reporting the expected total.
  Direct published-read score totals are included for active and failed
  rebuilds, including paired-HITS active and failed direct score totals, and
  are verified against top-k and family shape. That makes prior-generation
  preservation visible from the final row instead of requiring per-family
  result reconstruction.
  It also records successful fan-in merge evidence with expected and observed
  totals: total synthetic shards, min/max shard score counts, total merged
  primary scores, active-stale published primary scores, mixed failed/building
  published primary scores, primary nonuniform-layout count, paired-HITS merged
  metric results, paired active shard counts, paired active/mixed published
  metric results, and paired HITS nonuniform-layout count. Successful fan-in
  totals are verified against top-k, shard count, and the
  degree/PageRank/eigenvector/HITS family mix, including whether the selected
  workload should produce uniform or nonuniform primary and paired-HITS shard
  layouts. Promotion summaries now expose that as a single
  configured/observed fan-in floor so deployment-shaped runs cannot pass after
  silently falling back to a uniform or too-small fan-in fixture. Expected
  aggregate rejection totals are emitted next to observed
  totals, and rejection totals are broken down by category for both primary and
  paired-HITS fan-in: active `fresh`,
  zero-generation, incompatible generation, metadata-version, edge-filter,
  missing requested metric, duplicate metric, extra metric, non-finite score,
  non-finite status number, out-of-range progress, identity mismatch, and
  invalid published state. Those category totals are verified independently, so
  a final summary cannot satisfy the fan-in gate by producing the right
  aggregate rejection count while skipping one malformed or incompatible shard
  class.
  Those summary totals are verified against the families that actually ran, so
  promotion tooling can prove from the final row alone that first-publish
  direct reads, traversal reads, rerank reads, status checks, active and failed
  read paths, profile entries, paired-HITS read paths, compatible fan-in merges,
  active-stale fan-in preservation, mixed failed/building fan-in preservation,
  and fan-in fail-closed paths were exercised instead of only inferring that
  from per-family rows. The
  summary row also aggregates local-oracle parity and published-status metadata
  proof: expected and observed top-k parity checks, expected and observed
  status parity checks, minimum expected and observed converged status counts,
  non-converged published status counts, minimum expected and observed
  positive-delta metadata counts, expected and observed computed-at metadata
  counts, and the min/max completed-iteration counts seen across the families
  plus the configured maximum allowed iteration count. Those totals are
  verified against the family and metric counts, so a release summary cannot
  pass while omitting planned-vs-local parity, computed publish timestamps,
  bounded iteration metadata, or the non-converged positive-delta evidence
  required for fixed-iteration iterative metrics. The summary row also
  aggregates storage-footprint proof:
  total metric slots, fixed retained metadata overhead per metric, total
  expected score records, fresh and failed retained score records, expected and
  observed fresh/failed metric-record counts, fresh and failed control record
  counts, the expected fresh control count, the failed control-record ceiling,
  fresh and failed job namespace counts, fresh and failed attempt-record counts,
  and expected plus observed retained failed diagnostic failure/event records.
  Those totals
  are verified against the generated graph shape, the fixed retained
  metric-record overhead, the number of metric families, the compatible HITS
  pair multiplier, and the bounded diagnostic window, so release tooling can
  prove from the final row that fresh and failed terminal states retained only
  published score output plus bounded metadata, removed job and attempt
  namespaces, kept exactly one fresh control record per family, and bounded
  failed diagnostics across the whole run. The
  summary row also reports the minimum split worker identities with tick
  progress, the minimum split worker identities with page claim/completion
  progress, the minimum per-family active-worker page progress, and the maximum
  per-family active-worker page progress. Promotion tooling can therefore
  reject a run where one family silently serialized all page ownership through a
  single worker while another family supplied the maximum worker-step evidence.
  The rollout runner should stay out of the public Make/build target surface. Release
  recipes can pass `--profile smoke` or `--profile promotion` plus the selected
  latency, retained-storage, scheduler, worker-identity, and family-floor
  budgets as rollout-tooling inputs instead of adding feature-specific public
  build targets.
  The budgeted smoke recipe uses loose local/planned/cleanup latency runaway
  guards plus conservative public-read, fan-in, retained-storage, page-claim,
  cleanup-tick, executed-round, failed-retry, worker-step, coordinator-step,
  attempt-record, and four-family summary budgets, giving PR validation a fast
  pass/fail exercise of every release-budget category without claiming
  production latency baselines. The promotion recipe runs the larger
  release-profile floor with conservative pass/fail budgets for published-read
  latency, fresh-failure latency, fan-in latency, retained storage records,
  page claims, cleanup ticks, executed rounds, failed rebuild retries, worker
  steps, coordinator steps, and minimum split worker identity progress, plus a
  four-family summary floor. Retained storage budgets now
  include explicit attempt-record ceilings in addition to score, metric,
  control, failure, and event records, while the cleanup invariant still
  requires zero retained attempt records after fresh and failed cleanup. The
  promotion recipe should require deployment-shaped evidence before accepting a
  run as rollout-ready.
  The required shape now includes the promotion workload floor, all-family
  execution, split/reopen ownership evidence, four-family coverage,
  split-worker progress floors, promotion fan-in and failure-churn floors,
  promotion operations/status floors, public-read/fresh/fan-in latency
  ceilings, cleanup latency ceiling, retained-storage ceilings, and scheduler
  ceilings. That makes the local
  promotion gate summary a required shape rather than best-effort metadata
  before deployment rollout tooling consumes the larger hosted evidence. Both
  the starting config row and final summary row expose the component booleans
  for all-family execution, public-read/fan-in latency budgets, cleanup latency
  budget, retained-storage budgets, promotion scheduler budgets, promotion
  fan-in floors, promotion failure-churn floors, and promotion operations
  floors; the config row reports the
  requested shape, while the summary row reports the observed result. The
  promotion recipe intentionally leaves local/planned wall-clock
  latency thresholds disabled until deployment baselines exist, while cleanup
  latency is now part of the deployment-shaped promotion requirement. The
  smoke recipe keeps only loose runaway guards for local, planned, and cleanup
  phases. The runner accepts `--profile smoke` and `--profile promotion` for
  focused family or budget overrides. The process-owner summary emits top-level
  `rollout_qualification_gate`, `public_read_release_gate`, and
  `remote_owner_release_gate` booleans plus service, direct, and
  failure/reclaim component booleans. The service gate is split into
  `service_lifecycle_release_gate`, `service_multipage_release_gate`, and
  `service_active_read_release_gate`; the direct/failure side is split into
  `direct_publish_read_release_gate`, `direct_reclaim_release_gate`, and
  `direct_exhaustion_fencing_release_gate`. The same row carries
  required/observed service cleanup-takeover counts for killed degree,
  PageRank, eigenvector, and paired-HITS cleanup owners, required/observed
  service multi-page worker, coordinator, and takeover phase proof counts, and
  required/observed direct reclaimed-attempt-completion plus
  stale-attempt-rejection counts for abandoned direct page attempts; the
  rollout summary should emit configured and observed deployment-shaped
  local evidence booleans. Those JSON rows are useful evidence for rollout
  tooling, but they are not themselves product build targets. CI should keep
  graph metrics shaped like other features: focused tests for lifecycle,
  cleanup, fan-in, operations, public API behavior, and process ownership. Full
  promotion still requires hosted deployment-scale owner, fan-in, cleanup,
  storage-growth, and latency evidence before any distributed-by-default
  rollout.
  Smoke keeps `maintenance_mode: "combined"` for backward-compatible PR
  validation, while promotion starts with `maintenance_mode: "split"` so release
  runs exercise the coordinator/worker-pool boundary by default. Promotion is a
  floor, not just a label: a `promotion` run may raise workload knobs, but it
  cannot lower the split/reopen, document, fanout, top-k, shard, active-shard,
  mutation, worker, iteration, successful-generation-repeat, failure-repeat,
  diagnostic, or status-page evidence below the profile baseline while still
  emitting `profile: "promotion"`. Promotion
  also requires `top_k` not to divide evenly by the synthetic shard count, so
  the emitted shard min/max counters prove a nonuniform merge layout instead of
  a perfectly even fixture; `top_k` must also exceed the synthetic shard count,
  so every shard carries score evidence. Profiles set
  workload defaults; latency, storage, page-claim, cleanup-tick,
  executed-round, failure-retry, worker-step, and coordinator-step budgets
  remain disabled unless a release invocation provides explicit
  `--max-*-latency-ns`, `--max-storage-*-records`,
  `--max-page-claims`, `--max-cleanup-ticks`, `--max-rounds-executed`,
  `--max-failure-retry-count`, `--max-worker-steps`, or
  `--max-coordinator-steps` thresholds or uses the named budgeted promotion
  target. Synthetic
  direct-metric fan-in also accepts `--synthetic-fan-in-shards` and
  `--synthetic-fan-in-active-shards`; the smoke profile keeps the two-shard,
  two-active-shard invariant, the CLI rejects active-shard counts below two or
  above the total shard count, and the promotion profile starts with
  `top_k: 33`, eight synthetic shards, and four active/stale synthetic shards
  so release runs exercise a broader, nonuniform merge layout before hosted
  cross-range qualification. The
  CLI also rejects `--fanout` values larger than `--docs`, keeping generated
  cyclic source targets unique enough for graph-shape evidence to be
  meaningful. The promotion profile also starts with two successful generation
  repeats before the failed-build churn phase and three active mutation writes
  before the active rebuild, while smoke keeps a single mutation write for fast
  local validation.
  Independent of those optional thresholds, the harness treats scheduler summary
  shape and graph topology as correctness gates: before drain the graph index's
  persisted stats must match the generated node and edge counts, the edge
  generation must match the target generation, and JSONL must expose source,
  sink, authority, sink-edge, cyclic-edge, bipartite-edge, authority-self-edge,
  and max-out-degree components for the selected family. Those emitted topology
  components must match the configured family, document count, fanout, and
  expected score-record count exactly. The active generation fields are checked
  as part of the same release record: active mutation writes must equal the
  target-generation delta, and the active target generation must equal the
  first published target plus that delta. Before drain it must also see exactly
  one queued build for the family. Scheduler budget shape is checked before the
  optional release-threshold gates: observed ticks must not exceed
  `--max-ticks`, executed rounds must fit within `ticks *
  --max-rounds-per-tick`, page claims must fit within executed rounds times
  `--max-pages-per-round`, and pre-drain metric scans must fit inside
  `--max-metrics-per-round`. Maintenance-mode evidence is
  a correctness gate too: combined mode must use exactly one combined sweep per
  tick and no split-role sweeps, while split mode must use the
  coordinator-before/worker-pool/coordinator-after shape for each executed
  scheduler round. Split multi-worker runs must also prove that more than one
  configured worker identity made both tick progress and actual page
  claim/completion progress whenever the run performs multiple worker steps, and
  must emit a nonzero min/max page-progress range for active worker identities,
  so a release run cannot accidentally serialize all page ownership through one
  worker while claiming a worker-pool shape. Reopen evidence is
  checked alongside those role-shape
  counters: reopen-enabled runs must cross a DB-handle boundary after every
  nonterminal tick and disabled runs must not report synthetic reopen evidence.
  The fresh drain must start exactly one build, publish exactly once, record no
  build failure, claim and complete pages with completed pages not exceeding
  claims, and prove worker steps account for claimed/completed pages. Completed
  pages must cover phase advancement, and coordinator steps must cover build
  start, phase advancement, publish, and failure decisions. The drain must also
  advance at least one phase, execute at least one round, and record both worker
  and coordinator progress. Pre-publish direct reads are also correctness gates:
  the primary metric must reject both `published` and `fresh` direct reads and
  graph search rerank reads with `MetricNotReady`, HITS must additionally reject
  both hub direct and rerank reads with `MetricNotReady`, traversal published
  projection must expose exactly one null/not-ready metric status for non-HITS
  and both authority/hub null/not-ready statuses for HITS, traversal fresh
  projection plus published ordering and filtering must each fail with
  `MetricNotReady`, and non-HITS families must emit no paired pre-publish
  counters. Pre-publish status is a correctness gate too: queued metrics must be
  `not_ready`, expose no active build/page payload, and point their queued
  generation at the current edge generation.
  The synthetic fan-in evidence is also checked as a correctness gate: the
  observed shard count and min/max score counts must match the configured
  non-empty balanced layout, at least two synthetic shards must be marked
  active/building for the active-published and fresh-fail fan-in probes,
  compatible terminal and active-published merges must return the expected
  top-k score count, mixed-active published merges must return the expected
  top-k score count with failed status severity, fresh,
  zero-generation, incompatible-generation, incompatible-metadata,
  incompatible-edge-filter, missing-metric, duplicate-metric, extra-metric,
  non-finite-score, non-finite-status, and out-of-range-progress probes must
  each reject once, index/metric/status-name identity probes must reject three
  times, invalid-state probes must reject twice, and HITS must additionally prove
  paired authority/hub shard layout, active-shard, mixed-active, result, and rejection counts while
  non-HITS families emit no paired fan-in counters.
  Active/terminal work summaries are checked the same way: fresh and failed
  terminal states must expose no pending work or active/failed page status,
  active rebuilds must prove the page claim/reclaim probe, expose at least one
  bounded active page without exceeding `--max-status-pages`, expose finite
  progress, and report one active build with active pages but no failed pages.
  Every emitted active status page must be leased and operationally detailed:
  its phase/iteration must match the active job, it must include a worker
  identity, attempt number, lease expiry, bounded progress units, and no page
  error. Focused status coverage also proves failed active pages retain their
  worker identity, attempt number, cursor, completed/total progress units, and
  last error. The result-level leased-page and detailed-page counters must both
  match the bounded active status page count.
  Active and failed direct reads, graph
  traversal projection/order/filter reads, and graph search rerank reads must
  preserve the prior published generation for `published` and fail closed for
  `fresh`; direct top-k reads must also return exactly the requested
  query/index/metric identity, expected status state where stable, non-empty
  configured-`top_k` score payloads, non-empty node ids, and finite scores. Rerank
  published checks must attach per-hit score details with the requested
  index/metric identity, prior published generation, finite score components,
  and consistent missing-score markers. Traversal published checks must also
  return metric payloads and status entries for exactly the requested metric
  names. Each active and failed traversal gate must record three published
  checks and three fresh rejections.
  Query profile is checked as an explainable
  read-surface gate too: non-HITS families must emit three graph metric profile
  entries with exactly one direct metric source, one traversal source, and one
  rerank source, while HITS must emit four entries because traversal profile
  includes separate authority and hub statuses. Paired HITS active and failed
  reads must return both authority/hub published results, exactly
  configured-`top_k` score payloads for each side, measured paired direct
  published and fresh-failure latencies, one paired direct fresh rejection,
  separate authority/hub rerank fresh rejections, and paired
  traversal projection metric results, while non-HITS families must keep those
  paired counters at zero. Storage footprint evidence is a correctness gate:
  successful and failed terminal score-record counts must match the expected
  graph shape exactly, terminal job namespaces must be gone, successful publish
  must retain one control record, and repeated failed rebuilds must retain only
  the exact bounded failure/event diagnostics allowed for the family.
- Local promotion-profile evidence should cover every graph metric family under
  the CI-sized profile floor: `docs: 128`, `fanout: 4`,
  `top_k: 33`, eight synthetic fan-in shards, three active mutation writes,
  four split worker identities, reopen-between-ticks, eight maximum iterations,
  two successful generation repeats, five failed rebuild repeats, sixteen
  retained diagnostic/status slots, and a four-family budgeted promotion
  summary floor.
  Degree, PageRank, eigenvector, and paired HITS each prove local-oracle parity,
  split coordinator/worker-pool sweep shape, all configured worker identities
  making tick and page progress, active and failed `published`/`fresh` read
  behavior, repeated successful-generation storage growth, cleanup/storage
  footprint bounds, nonuniform synthetic fan-in layout, active-shard and
  mixed-active `published` merge plus `fresh` rejection, zero/stale/
  incompatible/malformed fan-in rejection, identity-mismatch rejection, and
  invalid published-state rejection. The PageRank promotion run also proves
  fixed-iteration non-converged metadata at the iteration cap, eigenvector
  proves the same single-vector substrate can converge under the promotion
  shape, and HITS proves paired authority/hub promotion-shape reads,
  diagnostics, fan-in, compatibility, and invalid-state rejection.
  The all-family promotion evidence profile should include published-read,
  fresh-failure, and fan-in latency capped at one second, retained storage
  ceilings of 2500 score, 2600 metric, 32 control, one attempt record, 16
  failure, and 20 event records, plus scheduler ceilings of 4000 page claims,
  600 cleanup ticks, 1000 executed rounds, five failed-build retries, 7000
  worker steps, 2000
  coordinator steps, a four-worker minimum for both split tick progress and
  split page claim/completion progress, and four active/stale synthetic fan-in
  shards across primary and paired-HITS probes. Those non-local/planned/cleanup
  latency thresholds are deliberately conservative local promotion evidence.
  Larger HITS graphs, including the previous 1000-source stress shape, remain
  separate performance and deployment-scale qualification rather than the
  default CI promotion gate.
- Promotion-scale rollout still needs deployment-sized graph indexes
  with one coordinator, multiple worker pools, and forced killed-owner churn at
  scan, contribution, reduce, publish, and cleanup boundaries. Reopen between
  ticks proves durable restart after each scheduler tick, but it is not a
  substitute for killing active owners while they hold leases or page attempts.
  The direct process gate now explicitly counts abandoned page attempts whose
  stale completion is rejected after a replacement worker completes a newer
  attempt. The service process gate now also counts 27 worker phase proofs, 31
  coordinator phase proofs, and eight takeover phase proofs across the existing
  two-worker 130-source degree/PageRank/eigenvector/HITS service runs; the
  remaining gap is exercising the same shape through deployment-sized service
  owner churn.
  The synthetic fan-in probe should prove the rollout runner sees normal merge
  compatibility behavior, and unit-test hosted fan-in coverage now covers a
  nonuniform eight-shard degree/PageRank/eigenvector direct merge with four
  active/stale shards whose unpublished high-score targets stay invisible,
  active-stale degree/PageRank/eigenvector traversal
  projection/order/filter/search rerank published merge plus fresh rejection,
  compatible HITS authority/hub direct, traversal projection/order/filter, and
  authority-rerank merges over an eight-shard layout with four active/stale
  shards and prior-pair preservation, remote HITS
  generation/metadata/edge-filter mismatch rejection, and missing remote HITS
  status rejection through the real
  cross-range table reader. Those checks are still not a substitute for hosted
  cross-range promotion layouts with larger shard counts, broader mixed
  active/stale shard states across traversal/rerank surfaces and paired HITS,
  and broader injected incompatible statuses.
  Increasing synthetic fan-in shards gives the local runner a cheap preflight
  for larger shard counts, and increasing active synthetic shard counts
  preflights broader active/stale shard
  mixes, but successful local synthetic fan-in still does not replace
  deployment-shaped hosted fan-in evidence.
- Keep pass/fail gates separate from raw benchmark numbers. Correctness gates
  should require local-oracle parity, no leaked attempt output, bounded retained
  namespaces, no unbounded status payloads, and successful cleanup after owner
  churn. The promotion evidence profile should enforce retained-storage,
  page-claim, cleanup-tick, executed-round, failure-retry, worker-step,
  coordinator-step, published-read latency, fresh-failure latency, and fan-in
  latency ceilings as a conservative release guard. Local/planned/cleanup
  wall-clock latency should still be
  recorded as release notes until real production baselines exist, then promoted
  to explicit rollout thresholds per metric family and profile.
- Promotion-scale fan-in should use shard layouts beyond the focused hosted
  unit coverage: larger mixed hot/cold shards, active/stale shard mixes for
  rerank surfaces and paired HITS, and more varied incompatible HITS
  authority/hub pair rejection. Unit-test hosted coverage now proves nonuniform
  eight-shard degree/PageRank/eigenvector direct fan-in with four active/stale
  shards, active-stale degree/PageRank/eigenvector traversal/rerank fan-in,
  compatible HITS direct, traversal projection/order/filter, and authority
  rerank fan-in over an eight-shard layout with four active/stale shards and
  prior-pair preservation, remote HITS
  generation/metadata/edge-filter mismatch rejection, and missing remote HITS
  status rejection, while
  the rollout runner should have fanout graph-shape, merge-layout, and
  active-staleness knobs so promotion runs can
  move beyond pure star topologies and two-shard synthetic fan-in, exercising
  denser PageRank/eigenvector cycles plus multi-authority HITS graphs while
  preserving the same local-oracle parity check. Release JSONL
  now records the generated topology components and verifies them against graph
  index stats before metric execution, so promotion notes can distinguish larger
  graph-shape evidence from merely larger metric budgets.
- The rollout runner should not introduce a public job API. It should drive only the
  same public index/metric config surface, public action/status routes, and
  internal service owner boundary used by the focused gates.

### Long-Term Non-Goals

These should stay out of the graph metric roadmap unless a later design changes
the core assumptions:

- Synchronous PageRank computation on the write path.
- Query-time full-graph centrality scans.
- Exposing partial distributed job output as queryable scores.
- Metric-specific query APIs for every new centrality algorithm.
- Retaining many historical generations by default.
- Hidden PageRank boosts that affect retrieval without explainable config.

### Roadmap Exit Criteria

The graph metric framework should be considered production-complete when:

- PageRank supports local and distributed materialization through the same
  graph-index-owned metric API.
- Degree, eigenvector, and HITS reuse the same distributed executor model
  without adding a second storage/query/status framework.
- Direct metric top-k, graph projection, graph ordering, graph filtering, graph
  search rerank, query profile, any future standalone explain surface, and
  status APIs are covered by public e2e tests.
- Remote coordinator and worker owners communicate only through durable
  graph-index job/page state; duplicate coordinators cannot double-publish, and
  worker loss abandons only reclaimable page leases.
- Failed, abandoned, or exhausted builds preserve published scores across
  restart, including compatible HITS authority/hub pairs.
- Dirty markers survive restart and eventually rebuild through bounded
  scheduler/runtime ticks.
- Cleanup resumes after restart, keeps diagnostics bounded, and prevents
  completed, failed, abandoned, and unpublished job state from growing without
  bound.
- Cross-shard direct metric top-k has deterministic merge behavior, and
  retrieval/rerank metric merges either prove globally comparable generations or
  fail closed.
- OpenAPI, generated clients, and public docs describe freshness semantics,
  convergence metadata, phase progress, runtime ownership summaries, cleanup
  behavior, and failure status.

## Resolved Design Defaults

- Graph metrics are owned by graph indexes.
- PageRank is the first v1 metric kind; later metric kinds remain explicit,
  opt-in graph metric configs.
- Edge scope defaults to all edges in the graph index.
- The resolved metric config always records `edge_filter`.
- V1 supports `mode: "all"` and typed edge include lists.
- Metric projections return `null` before first publish.
- Ordering, filtering, and top-k metric reads fail with `MetricNotReady` before
  first publish.
- `metric_freshness: "published"` reads the latest complete generation, stale or
  fresh.
- `metric_freshness: "fresh"` requires the published generation to match the
  current edge generation.
- `fresh` with no published generation fails with `MetricNotReady`.
- `fresh` with an older published generation fails with `MetricStale`.
- Fixed-iteration non-converged PageRank publishes by default with
  `converged: false`.
- Invalid score output fails the job and preserves the prior generation.
- V1 keeps only the latest published generation, subject to snapshot safety.
- Old generation cleanup happens immediately when graph metric reads are
  snapshot-safe.
- If reads are not snapshot-safe, old generation cleanup uses a deferred cleanup
  queue keyed by eligible cleanup time.
- The v1 deferred cleanup fallback uses an internal 60 second delay.
- Direct graph metric endpoints always return metric status.
- Graph traversal and graph search execution carries metric status for every
  projected, ordered, and filtered metric dependency so fan-in can prove
  generation/freshness compatibility. Clients should set `include_metric_status`
  when they need that status as an explicit response field.
- `metric_status` is a map keyed by metric name.
- Retention controls are future debug/admin options, not v1 user-facing config.
