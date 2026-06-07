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

Only `pagerank` should ship initially.

The resolved config should always record the edge scope. The ergonomic default
is all edges in the graph index, but users need a first-class way to restrict
the metric to specific edge families because PageRank over mixed semantic edges
can be misleading:

```json
{
  "edge_filter": {
    "types": ["mentions", "cites"]
  }
}
```

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

By default, queries read the last published complete generation. If a metric has
never been published, metric projections may return `null`, but ordering or
filtering by that metric must fail with `MetricNotReady`. This is more explicit
than silently producing unstable ordering.

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
delete a generation that an active query snapshot can still read. If storage
snapshots already make this safe, immediate cleanup is sufficient.

Future versions can add retention controls for debugging or rollback:

```json
{
  "retained_generations": 2
}
```

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

## Open Questions

- What exact schema shape should represent `edge_filter` for graph indexes that
  already have typed edge families, labels, or predicates?
- Should metric projections return `null` before first publish, or should every
  metric read path consistently fail with `MetricNotReady` until the first
  generation is available?
- What storage snapshot or reader-tracking primitive should guard immediate old
  generation cleanup?
- Should future versions expose `retained_generations` or `retention_ms`, and
  should that be user-facing or debug-only?
- Should `fresh` failure use `MetricNotReady`, `MetricStale`, or a shared
  freshness error with structured status details?
