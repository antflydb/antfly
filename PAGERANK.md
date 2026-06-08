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

For the first version, clean up old generations immediately after publishing a
new generation, while preserving snapshot safety. The implementation must not
delete a generation that an active query snapshot can still read.

If graph metric reads run inside a stable read transaction or storage snapshot,
delete old generation rows immediately after the publish pointer flips. Existing
readers keep their snapshot; new readers resolve the new published generation.

If graph metric reads are not snapshot-safe, use a small deferred cleanup queue
instead of adding a full reader-epoch system in v1:

```text
graph_metric_cleanup:<index>:<metric>:<generation> -> eligible_after_ms
```

Maintenance deletes queued generations after `eligible_after_ms`. Use an
internal v1 constant:

```text
default_deferred_cleanup_ms = 60_000
```

Do not expose the deferred cleanup delay as user-facing config in v1.

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
- DB graph traversal/search metric reads now have matching stale/fresh coverage:
  published projection can return stale metric scores and status, while fresh
  projection, ordering, and filtering over stale graph metric materializations
  fail closed with `MetricStale`. The DB graph query conversion path also
  releases temporary graph metric status ownership after cloning it into the
  public search result.
- DB graph traversal/search metric reads now also cover unpublished metrics:
  published projection returns a null metric score plus `not_ready` status,
  while fresh projection and published ranking/filtering fail closed with
  `MetricNotReady` before the first metric generation is published.

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
  `iterate_contributions`, `check_convergence`, and `publish_generation`, while
  still accepting legacy `computing` and `publishing` phase records from older
  local leases. The job record is updated with the same phase/iteration progress
  used by active status, persists cursor/unit progress across graph-index
  reopen while a build lease is active, records failed build retry/error details
  durably, and is marked `complete` with no active lease expiration or failure
  details after a successful publish. The status and OpenAPI surfaces now expose
  cursor and work-unit fields for active builds so distributed workers can reuse
  the observable shape later. This is still a local job-table primitive for the
  future distributed/resumable worker implementation; it does not yet split
  PageRank into distributed pages or change the public metric API.

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
  scope when available and falls back to current config only for legacy
  materializations. This keeps status/explainable scope tied to the scores that
  were actually published, even if a future config change marks the metric stale
  or requires rebuild.
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
  The metric remains configured and returns `MetricNotReady` until maintenance
  or a manual run rebuilds it from durable graph edges.
- DB and index-manager layers expose local manual refresh and rebuild
  primitives for a named graph metric. Manual refresh runs the configured metric
  regardless of background refresh mode; rebuild clears materialized state first
  and then recomputes from durable graph edges.
- DB and index-manager layers expose local pause/resume primitives for
  background graph metric maintenance. Paused metrics remain queryable from the
  last published generation, status reports `maintenance_paused`, and manual
  refresh can still publish a fresh generation while background maintenance is
  paused.
- The public table HTTP layer exposes local graph metric operational actions at
  `/tables/{table}/indexes/{index}/graph-metrics/{metric}:{action}` for
  `refresh`, `rebuild`, `delete`, `pause`, and `resume`. Each action is
  idempotent at the API contract level and returns the updated graph metric
  status. `delete` clears materialized metric state and maintenance controls
  while leaving the configured metric available for a later refresh or rebuild.
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
- Distributed public query fan-in now preserves direct `graph_metric_results`
  from remote shard responses. The API merge path groups results by requested
  query name, merges shard statuses conservatively, sorts scores by score
  descending with node-key tie-breaking, and applies the requested metric
  `top_k` after global merge. It also verifies that all score-bearing shard
  results for a named metric refer to the same index, metric, and published
  score generation before ranking scores together; incompatible generations
  fail closed instead of producing a blended top-k. This establishes the
  deterministic direct-metric merge contract; distributed metric jobs and
  globally comparable published generations remain future work.
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
  report the same nonzero published metric generation. Hits reranked by this
  path now include `_score_details.graph_metric_rerank` with the base score,
  base weight, metric score, metric weight, missing-score fallback flag, final
  score, and published metric generation used for that hit. Richer expression
  trees remain future work.

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
  Newly written materializations encode version `3`; legacy unversioned
  metadata decodes as version `0`. Public status responses expose
  `metadata_version` while generated clients treat the field as optional so
  mixed-version shard responses remain parseable during rolling upgrades.
- Graph metric config validation now rejects empty and duplicate metric names
  before an index opens. This keeps status maps, direct metric reads, and future
  migration code from having to resolve ambiguous metric definitions.

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

- PageRank supports local and distributed materialization.
- Direct metric top-k, graph projection, graph ordering, and status APIs are
  covered by public e2e tests.
- Failed builds preserve published scores across restart.
- Dirty markers survive restart and eventually rebuild.
- Cross-shard direct metric top-k has deterministic merge behavior, and
  retrieval/rerank metric merges either prove globally comparable generations or
  fail closed.
- At least one non-PageRank metric reuses the same framework without adding a
  second storage/query/status model.
- OpenAPI, generated clients, and public docs describe freshness semantics and
  status fields.

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
- Graph traversal and graph search return metric status only when
  `include_metric_status` is set.
- `metric_status` is a map keyed by metric name.
- Retention controls are future debug/admin options, not v1 user-facing config.
