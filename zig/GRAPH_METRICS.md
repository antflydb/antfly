> Current command and test organization is documented in
> [GRAPH_METRICS_EXECUTION.md](docs/GRAPH_METRICS_EXECUTION.md#test-ownership-and-fault-qualification).
> The process-command and promotion-matrix descriptions below are historical:
> operators now use `antfly index maintenance`; worker launch controls live in
> the native integration fixture, and replayable fault histories live in VOPR.

# Graph Metrics Design

## Goal

Add PageRank-style graph centrality as a materialized graph index metric. The
metric should be eventually fresh, observable, safe to query during rebuilds,
and cheap on the write path.

This should be implemented as a generic graph metric framework with PageRank as
the first supported metric. Eigenvector centrality, HITS authorities, and HITS
hubs can reuse the same storage, job, and query surfaces later.

## User Model

Service-targeted maintenance roles (including `supervise` and `launch`) require
`ANTFLY_INTERNAL_SERVICE_SECRET` (at least 32 bytes) and
`ANTFLY_INTERNAL_SERVICE_ISSUER`, matching the owning service's
`antfly.internal_service.secret` and `antfly.internal_service.issuer` credentials.
Supply these through the deployment's secret environment, never process arguments.
Roles validate credentials before requesting work; supervisors and launchers
validate them before spawning children. Children inherit the environment, and
the shared HTTP client signs only internal API requests with short-lived tokens.
The process harness exercises enforced authentication without an unsigned bypass.

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

`max_iterations` must be between 1 and 1,000. The bound applies at both the
configuration parser and graph runtime boundary so malformed internal config
cannot create unbounded foreground or background work. HITS authority and hub
metrics form a pair only when their iteration limit, tolerance, refresh mode,
and edge filter match; ambiguous aliases and mixed manual/background pairs are
rejected during index validation.

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

## Execution and Memory Architecture

Graph metric kernels consume an immutable ordinal topology whose adjacency
lanes are selected by capability: degree retains counts only, PageRank retains
incoming neighbors plus outgoing counts, eigenvector retains incoming
neighbors, and HITS retains both neighbor lanes. A compatible metric group
builds the union once. Endpoint ordinals are validated during construction so
reused topology is not rescanned for every metric.

Serverless compilation enforces the peak-memory limit with a live allocation
limiter after charging the decoded source graph. Admission therefore follows
observed vertex, edge, and distinct-edge-type cardinality instead of rejecting
low-cardinality graphs using a pessimistic per-edge string estimate. Encoders
borrow canonical node IDs, and paired HITS outputs are encoded and published
one at a time.

Large dense and adjacency passes use the caller's shared `std.Io` runtime.
Floating-point reductions use fixed logical partitions and merge them in a
stable order, so changing execution parallelism does not change score bits.
The embedded compatibility runner uses the same storage-independent kernels in
serial mode; it is an oracle and rollback path rather than a second algorithm.
Serverless operators can bound per-materialization CPU fanout with
`--graph-metric-max-parallelism` or
`ANTFLY_SERVERLESS_GRAPH_METRIC_MAX_PARALLELISM` (range 1-16, default 4).
Admission and execution share one algorithm-specific logical cost model:
PageRank, eigenvector, and HITS account separately for adjacency passes, dense
reductions, normalization/scaling, and vector setup. This keeps the configured
work ceiling meaningful for sparse graphs and prevents HITS from being admitted
using a cheaper single-vector estimate.

The durable planned runner treats attempt records and per-page contribution
records as recovery journals, not history. Iterative producers write immutable,
attempt-tagged ordinal shards directly in reducer order; the final checkpoint
and producer completion commit atomically. There is no contribution copy/delete
adoption pass. Every iterative build uses checkpointed summary leaves and a
scalar-only root, regardless of node count, because small graphs can still have
dense edge sets. Consumers select only completed producer attempts and retain
inputs until the entire consumer phase finishes. Bounded, cursor-based barrier
cleanup then retires all winning and abandoned shards. The working set does not
grow with the configured iteration count; final cleanup removes job state.

Scan-page adoption also maintains one target-owned out-degree total per node.
The retained per-page value is an idempotency ledger: a reclaimed attempt
replaces that page's value and adjusts the total by its delta. Initialization
therefore performs one point lookup per node instead of probing every scan
partition, while retry safety remains independent of worker identity.

Published native generations keep the complete node-keyed score vector for
point reads, but their score-ordered secondary index retains only the best
10,000 entries, matching the public `top_k` ceiling. Each publication page
merges its local top prefix with that bounded durable prefix, so top-k remains
`O(K)` to read without doubling persistent score storage. Native reranking and
graph projections resolve every dependency column through one stable read
snapshot and reuse retained cursor storage across maintenance pages. Graph filtering, ordering, and limiting
then stay columnar: clause names are resolved once, bounded result pages use
`O(N log K)` selection, and surviving rows are views into one contiguous
metric-value slab with one owned copy of each metric name. Native and serverless execution call the same
storage-independent selector, so ordering, null placement, filtering, limits,
and deterministic tie-breaking cannot drift between deployments.

Search reranking is a bounded two-stage retrieval operation. `candidate_count`
controls the first-stage window (default `offset + 4 * limit`, maximum 10,000),
the graph metric scores and sorts that window, and only then are `offset` and
`limit` applied. An explicit window must cover the requested page. This avoids
the misleading UX of reranking only an already-truncated page while keeping
score reads and latency predictable for operators.

Serverless point, projection, rerank, and direct top-k reads share one budget
ledger on the pinned request session. The ledger composes authenticated range
operations, transferred bytes, decoded blocks/work, and retained result-column
and status memory across every named operation and metric dependency. This closes the
per-metric-limit loophole where a valid request could multiply the allowed I/O
by its dependency count. Exhaustion fails before the next backend range read
and is returned as an actionable, non-retryable HTTP 422. The unreleased
serverless format has one accepted wire version: discarded pre-release layouts
are rejected rather than carried as a permanent compatibility surface.
Independent immutable score ranges use bounded eight-way `std.Io`
fanout, while every child view shares the same synchronized request ledger and
pinned manifest. Range payloads are decoded and released one fanout batch at a
time, so peak temporary memory is bounded by concurrency rather than the total
number of planned ranges. Parallel metric workers write into disjoint
request-owned result columns, avoiding a second full-column clone from the
thread-safe transient allocator. Point-score planning uses a sparse sorted
worklist, so the common path allocates in proportion to requested nodes and
touched blocks instead of the complete routing table. Public graph shaping
carries stable source-row
ordinals through filter and order stages, fetches later dependencies only for
surviving rows, and moves nodes once when the final projection is materialized.

Planned native maintenance pins the index catalog with a shared lifetime guard
for each bounded scheduler unit. It does not hold the database-wide apply fence:
graph storage transactions, page leases, attempt identities, and publication
generation fences are the concurrency boundary. A short cooperative pause after
durable progress protects foreground storage latency without serializing graph
writes behind a complete maintenance page.

Each computational native phase also maintains an order-independent progress
record in the same transaction as its page mutation. Claims advance a durable
round-robin cursor, idle coordinator ticks can decide incomplete/failed state
without a page scan, and exhausted-page scans are skipped unless a page has
reached the attempt ceiling. Cleanup instead uses its bounded page/job cursor,
so every retirement path remains a single job-namespace protocol. Once every
computational page is complete, the coordinator performs one page-key-ordered
floating-point reduction and caches the phase summary. This keeps publication
bit-deterministic without recomputing floating aggregates after every
completion.

Planned score workers likewise write only their disjoint node-keyed score
pages. They do not maintain the generation-wide score-ranked keyspace at every
checkpoint. After verification, the coordinator selects the exact supported
top-10k once from the immutable generation and installs that bounded secondary
index in the same transaction that flips the publication pointer (and both
indexes/pointers for paired HITS). Readers therefore retain O(K) top-K access
without turning every 4k-score checkpoint into another generation scan and
sort.

Public serverless query shaping treats the selector's returned parent indexes
as the row-lineage authority. Resident metric columns are transactionally
rebased with direct indexing after each filter/order transition, filter-only
and order-only columns are released before the next transition, and a failed
allocation leaves the entire cache on its prior lineage. Cross-column I/O also
preallocates every caller-owned result buffer before scheduling the first
worker, so no error path can release stack state or score storage still used by
an asynchronous child.

External serverless reconciliation accepts the same caller-owned compute
runtime as ordinary lake builds, keeping CPU fanout under one operator-visible
limit. PageRank accepts authenticated, ordinal-aligned seeds from
the last compatible publication. Both document-backed publication and lake
reconciliation use the same seed admission and mapping path. It linearly maps the prior
node-sorted vector onto the new projection, assigns zero to new nodes, skips
deleted nodes, and lets the kernel validate and normalize the result. A
disjoint, rejected, over-budget, incompatible, missing, or corrupt prior artifact
cold-starts. Unauthenticated seeds are never used; optional fetch failures do not
block publication from authoritative topology. Cancellation and allocation
failure still propagate. Seed admission covers both preparation
(including decoded routing memory) and execution alongside kernel/output memory.
Cold kernel work is admitted before optional seed I/O. A separate per-publication
seed budget caps input at 64 MiB and decode/mapping work at 67,108,864 units
(one prior byte plus one current node per unit). Every actual read is charged,
including repeated identities; no seed cache is implied. Zero allowance disables
warm starts. Exhausting this optional budget cold-starts without consuming later
metrics' cold execution allowance. Materializer epoch 10 fingerprints this policy
and the independently authenticated paged routing format.
An optional seed never turns an admissible cold build into a budget rejection.
Native PageRank pins its seed generation and configuration for the job and computes
the surviving seed mass through the bounded, checkpointed initialization summary.
Every worker uses that same scalar before writing ranks and source factors. Zero
surviving mass falls back to the uniform vector. Execution epoch 9 rejects older
in-flight jobs: rebuild them with upgraded workers. Their previously published
score generation remains readable; partially computed old seeds are not resumed.
The same job/configuration/epoch fence applies to workers, coordinator phase
transitions, and the final publication transaction, including paired HITS.
Continuing an unexpired owned lease does not consume a retry; the final attempt
may finish any number of bounded checkpoints, but cannot be reclaimed after expiry.
Warm starts preserve PageRank's probability normalization, but finite-iteration
results and tolerance-based stopping can depend on the seed.

All iterative native jobs assign stable, partition-local ordinals in
the initialization producers. The dictionary is immutable for the job and is
retired with job state. PageRank ranks/source factors and both spectral vectors
use 256-slot blocks with explicit presence bits; zero and missing output are
distinct. Sorted multi-get resolves dictionary slots, deduplicates block reads,
and preserves caller order. Attempt-fenced checkpoint writes update each block
once and retire obsolete iterations by block. Small graphs use the same bounded
pipeline, with one summary leaf, one scalar root, and one data partition.

Iterative native jobs compile immutable row-oriented adjacency during the first
contribution phase of each lane. HITS stores both orientations, so hub reduction
does not repeatedly scatter across target-sorted physical edges. Each compile
checkpoint scans at most 4,096 edges and emits tiles of at most 256 neighbor/output
ordinal pairs, grouped by output vector block. Later contribution phases are
metadata-only barriers. Reducers gather the current input vector directly; no
numeric contribution shuffle is written on any iteration. Adjacency identities
include the first-iteration producer attempt and checkpoint. Before the first
fold, each output chunk packs only winning producer fragments into dense 256-edge
tiles. Compaction checkpoints bound both physical records (512) and edges (4,096),
persisting the input cursor and partial tile atomically. A completion receipt
selects one immutable packed attempt; replacement attempts cannot mix abandoned
output, and missing or truncated tiles fail closed. Document IDs remain at the
input/output boundary; node-oriented reducers resolve ordinals by sorted multi-get.
A worker step may run one bounded packing pass followed by one bounded fold,
so small chunks do not wait another maintenance tick after receipt publication.
Receipt preflight prevents repeated numeric work while another chunk is packing.

All native reducers retain packed immutable adjacency until job cleanup.
Checkpointed output may be replayed safely after lease takeover. Once every
consumer finishes, the phase barrier deletes at most 512 temporary records per
transaction, persisting a last-deleted-key cursor atomically with each batch.
Restart seeks beyond that cursor instead of rescanning LSM tombstones. Completed
namespaces retain a durable sentinel until job cleanup. Retirement counts flow
through DB maintenance sweeps, idle detection and runtime status
(`total_retired_input_records` / `last_retired_input_records`). The barrier advances
only after cleanup finishes. Both HITS lanes retire producer fragments after
iteration zero and raw vectors each iteration.
Retained topology is proportional to edges and bounded producer attempts, not
the number of power iterations; temporary vectors remain iteration-bounded.

Large reduction passes reuse the immutable node quantiles: independently leased
producers fold at most 16 physical adjacency tiles (at most 4,096 edges) for a group of up to
256 nodes per checkpoint. A checkpoint-local vector-block cache deduplicates
neighbor reads across all its tiles without retaining state across transactions.
Their attempt-fenced cursor and compensated sums
survive restart; a replacement attempt recomputes without mixing old state.
Completed folds atomically publish raw vector blocks and partial spectral norms
or PageRank dangling mass. The root deterministically combines at most 256
completed scalar records. Data reducers stay behind that barrier and read only
raw vector blocks, avoiding a second adjacency traversal for normalization.
Raw vectors retire at the consumer barrier, bounding temporary state by an
iteration. Filtered-out ranges contribute empty leaves.

Serverless metric wire version 9 separates the ranked routing root, a sparse
directory, and 64-entry primary routing pages. Manifest version 18 authenticates
the root, which authenticates the directory; each directory entry authenticates
one routing page, whose entries authenticate score blocks. Point reads load
only selected pages, with global block ordinals preserving score-cache identity.
Indexes fitting in one routing page or a 64 KiB footer retain a single footer fetch and decoded
cache lease, avoiding extra network round trips for small metrics.
Only missing contiguous pages coalesce into authenticated ranges; independent runs
use the same bounded parallel executor as score ranges (per column: eight reads and 32 MiB
in flight, with each coalesced range capped at 8 MiB). A query prepares every
column's control and routing, consuming authenticated cached score blocks before
admitting score network reads. Contiguous misses form zero-overfetch runs capped
at 8 MiB, sharing the remaining request budget without per-column quotas.
When those runs exceed the request allowance, an exact bounded partition planner
minimizes bytes across all columns. It considers partial merges, splits within
miss runs, and ranges crossing former fixed-window boundaries. A monotone queue
keeps planning work O(missed blocks × remaining requests); scratch memory and work
are admitted against the shared query limits before allocation. Ranges omit unused
boundary blocks and bridge authenticated cached gaps only when required to fit
the request budget. They never bridge a discontinuity in authenticated extents;
partial cache warmth must not reduce admission feasibility.
The complete score request and byte totals are reserved atomically before
bounded parallel execution. An uneven query therefore does not fail merely because
one column needs more than an equal share. The shared budget remains authoritative
across all metric surfaces of the pinned query.
Top-K reads fetch only the root (at most 1,872 bytes) and requested ranked blocks,
never the point directory or pages. The root also binds primary vector extents
and the complete point-index digest for full-artifact/warm-start validation.
Decoders validate the cross-tier relationship. Unreleased older
graph-metric wire versions are rejected rather than migrated at query time.

Eigenvector and HITS always use canonical cold seeds in both runtimes. An old
spectral vector may have zero support on a newly dominant disconnected component;
normalization alone cannot make that a safe warm start. A future spectral restart
policy needs component/support guarantees and oracle tests for topology changes.

Serverless query caches retain authenticated decoded metric roots and directories under
an independent 16 MiB process-memory budget (configurable with
`max_graph_metric_routing_bytes`). Identity includes the immutable artifact,
checksums, footer extent, and wire version. Leases keep borrowed IDs alive;
eviction skips pinned entries and saturated caches bypass retention.
Misses reserve bounded per-identity fill ownership before fetching or decoding.
Waiters yield through caller-owned `std.Io`, check cancellation independently,
and can take over when a failed/canceled producer releases ownership. At most
64 distinct fills are admitted concurrently; cache saturation applies backpressure.
Registered waiters pin a shared result independently of LRU admission, including
when retention is disabled or existing entries are pinned. The producer may drop
its lease before waiters wake without losing the result. Cancellation releases
the waiter's registration; the last reference releases a bypassed result. This
does not create another unbounded cache: fill registrations have fixed capacity,
and every consuming query applies its retained-memory admission to the lease.
Routing pages use the authenticated block cache and are decoded only for selected
pages, so a large primary index cannot repeatedly bypass the decoded-index cache.
Routing pages and primary score blocks have canonical cache identities using the
immutable artifact, global block ordinal, and exact extent, independent of the
candidate set or transport batch. Readers probe those units before planning
transport, authenticate the entire coalesced response before publishing any unit,
and never retain candidate-specific combined routing or primary-score ranges.
Publication groups at most 32 canonical blocks (and 8 MiB) per batch, bounding
simultaneously open per-key leases. A cache-wide 32-publication admission limit
also bounds descriptor use across parallel queries; saturation bypasses optional
retention without waiting or evicting useful entries. Each batch
reserves capacity and performs eviction once, writes payloads outside global
maintenance/coordination locks, then commits independent canonical entries.
Small caches retain a fitting subset instead of bypassing the entire batch.
Existing per-block durable reservations, nonblocking publication ownership,
cross-process usage reconciliation, and abandoned-write recovery remain intact.
Point, routing-page, and ranked top-K reads also share a process-local canonical
block pool, bounded to 4,096 live blocks and 64 MiB of payload. Identity binds the
artifact, checksum, extent, and authenticated block digest, never K or a candidate
set. Missing block sets are claimed atomically before transport: a reader waits
without holding other fill claims, preventing overlapping-query deadlocks.
Registered waiters pin completed or failed producer state; cancellation does not
cancel another reader's producer, and producer failure permits takeover.
Failed pinned entries still consume both admission limits. Ready unpinned entries
are evictable, and warm point preparation can copy them without disk I/O or
re-authentication. Payload bytes, live entries, and waiters are reported in
query-cache statistics.
Only missing contiguous runs are downloaded. If newly shared interior hits would
require more requests than the already-admitted range budget allows, execution
keeps its original bounded range rather than failing a query due to cache warmth.
Ranked blocks use canonical disk identities too: changing top-K across a block
boundary fetches the new suffix, rather than retaining overlapping prefix ranges.
Fetch temporaries retire before contiguous result allocation, preserving the
per-query in-flight payload bound independently of the shared pool's fixed budget.
Overlapping candidate sets therefore reuse already-fetched blocks even when their
routing-page sets or coalescing boundaries differ. Cached scores are decoded into
the final result during preparation without retaining all cached payloads; they
consume decode/work budgets but no score network request/byte reservation. The
latest artifact's existing authenticated blocks define these units; no additional
wire format or legacy cache reader is needed.
Parallel range payloads use a thread-safe allocator and transfer ownership
explicitly, independently of the allocator owning returned scores or per-request
routing arrays.
PageRank's fixed logical reduction partitions account for edge work as well as
vertex count. Edge-heavy graphs below the vector-parallelism threshold therefore
use configured compute workers, while logical reduction order stays identical
across serial and parallel execution.
Stateful reverse-edge probes validate the catalog's index incarnation and config
fingerprint, plus the source shard's read generation, under the storage apply
lease that protects the reverse snapshot, including probes without hydration.
An index replacement cannot certify old negative answers under a new routing
cache identity. Index-incarnation mismatches retain their identity across native
and remote HTTP boundaries. Public queries release the failed snapshot and retry the
whole query at most once, respecting cancellation and deadlines; continued
reconciliation returns the existing retryable `index_rebuilding` response.
No storage or decode operation runs under the cache lock. Warm point
and top-k reads avoid footer I/O and full-index decoding. Decode-cache hits,
misses, and retained bytes are exposed in query-cache statistics.

Ordinal dictionaries are scoped to an exact edge-filter projection; graph-wide
ordinals are not interchangeable because filters change the active node set.
The compute lifecycle treats that projection as the family boundary and shares
its topology across every compatible column. Durable score artifacts remain
independently addressable by column: point reads are the primary serving shape,
so they must not fetch unrelated columns or couple cache eviction and rebuild
failure domains. Paired HITS shares one kernel execution while publishing its
two columns independently. A packed multi-column wire is therefore gated on a
scale benchmark that proves its storage savings exceed its extra random-read
and lifecycle costs; it is not assumed to be an unconditional optimization.
PageRank contributions remain target/page qualified so retry adoption and
reclaimed-attempt replacement stay deterministic. Serverless is unreleased and
accepts no compatibility readers for discarded graph-metric prototypes;
native in-flight attempt changes still require an explicit rolling-upgrade
bridge.

Native durable cross-job topology reuse remains a benchmark-gated follow-up, not
an implicit extension of the compute-family sharing above. Measure repeated
compatible metric jobs against the current job-scoped packed topology, including
cold/warm wall time, edge-scan bytes, topology bytes written, peak RSS, and cleanup
cost at representative graph sizes. A shared artifact must identify the exact
edge generation, filter, orientation, and ordinal/layout version; publish only
complete immutable data; and acquire durable job pins atomically with attachment.
Concurrent builders, failed publication, restart recovery, and last-pin garbage
collection need fault-injection coverage before replacing job-scoped ownership.
Sharing ordinal arrays by metric configuration alone is not safe. No durable
cross-job cache or measured speedup is claimed by this PR.

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

### Distributed Execution Roadmap

> **Relocated:** The detailed per-phase implementation logs and the eight successive restatements of the remaining distributed-execution roadmap that previously lived here (4811 lines) are preserved verbatim in [work-log/completed/graph-metrics/roadmap-restatements.md](../work-log/completed/graph-metrics/roadmap-restatements.md). Durable decisions from them are folded into the table below and into Resolved Design Defaults.

Standing product contract (holds across every phase below):

- Metrics are configured on the graph index by stable user-provided names; users never create jobs, leases, attempts, pages, or cleanup tasks directly.
- `published` reads return the latest complete generation and may report `stale`, `building`, or `failed` status alongside scores; `fresh` reads require the current edge generation and fail closed with `MetricNotReady` (no publish yet) or `MetricStale` (newer graph writes since publish).
- Fixed-iteration non-converged PageRank, eigenvector, and HITS publish by default with `converged: false`, completed iteration counts, and final delta metadata.
- Old generations and intermediate job state are cleaned aggressively in v1; retention beyond latest-only is a future bounded admin/debug option, not query semantics.
- Retry policy, lease timing, worker IDs, page IDs, and attempt namespaces stay internal, surfaced only through status/diagnostic events.
- Operational actions (`refresh`, `rebuild`, `delete`, `pause`, `resume` via `POST /tables/{table}/indexes/{index}/graph-metrics/{metric}:{action}`) are idempotent and return current status.

Target distributed shape:

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

Workers never receive metric configs, never mutate the published generation pointer, and never decide a build has failed — they execute bounded page work and persist enough progress for another worker or coordinator to continue. The critical dependency is owner correctness before default promotion: a family must not become distributed-by-default while duplicate coordinators can race publish/fail, duplicate workers can write stale attempts, cleanup can grow without bound, or fan-in can merge incomparable generations.

| Phase | User/API contract | Current capability | Promotion gate / verification required |
| --- | --- | --- | --- |
| 1. Remote owner harness | Job ids, leases, pages, attempts, and worker identities never appear in the public API — only summarized role/owner/lease/progress/error telemetry. | Coordinator/worker/worker-pool roles run with durable lease-scoped ownership (role-scoped for coordinator, worker-identity or order-independent worker-pool-set-scoped for workers); duplicate owners are fenced until lease expiry and replaced after. Service-targeted (HTTP) owners exist for degree and bounded PageRank, with growing eigenvector/HITS coverage. True independent remote processes remain the gap. | One coordinator and multiple independent worker processes drain degree, PageRank, eigenvector, and paired HITS through durable state only: killing a worker abandons only its page/lease, a replacement completes it after expiry, stale workers cannot complete reclaimed pages, duplicate coordinators cannot double-publish or double-fail, and active rebuilds never expose attempt-scoped output through public reads. |
| 2. Degree canary rollout | Same standing contract; no user-visible change. Rollout is gated by the internal `degree_canary` DB idle-maintenance mode. | Degree runs the full durable job/manifest/lease/phase-barrier/cleanup path locally and through planned maintenance. The internal `auto` scheduler gate admits degree (plus PageRank/eigenvector/compatible HITS) into planned execution under a configurable control-record cap and bounded per-index caps, with `rounds_executed` reported for canary qualification. | Local-vs-distributed parity, process restart, stale-attempt rejection, cleanup resume, active/failed status summaries, public `published`/`fresh` behavior, scheduler latency budget, storage-growth bounds, and rollback to the local oracle all pass in CI/release qualification. |
| 3. PageRank production promotion | Same standing contract; `published`/`fresh` semantics unchanged as execution distributes. | The durable job/manifest/page/lease/phase-barrier/convergence/publish/cleanup model is implemented and exercised locally, through planned maintenance, and through service-targeted owners, including killed-worker exhausted-attempt handling and duplicate-coordinator fencing. | Distributed PageRank matches the local oracle within tolerance, preserves the previous generation on failed rebuilds, serves prior published scores while building, fails `fresh` with `MetricStale` when stale, recovers from every phase boundary, and passes promotion-scale cross-shard fan-in checks. |
| 4. Eigenvector single-vector parity | Reuses the PageRank substrate; eigenvector contributes only metric math, metadata, and tolerance rules — no metric-specific job state. | Eigenvector reuses the PageRank executor end-to-end (scan/initialize/contribution/reduce/convergence), including service-owner replacement, publish/cleanup restart, and killed-worker exhausted-attempt coverage. | Same bar as PageRank (local-oracle parity, prior-generation preservation, fail-closed freshness); still needs deployment-scale parity, promotion-scale fan-in, and latency evidence. |
| 5. HITS paired-vector promotion | Authority and hub are separate named metrics but share one target generation, convergence decision, publish decision, failure decision, and cleanup owner; publish/fail are atomic for the pair — no separate HITS job API. | Paired authority/hub phases (contribute/reduce per side, paired convergence, paired publish, paired cleanup) are implemented with reclaim, exhausted-attempt, publish-failure, and cleanup-resume coverage, plus service-owner replacement with compatible-pair freshness verification. | HITS stays disabled by default until promotion-scale fan-in, broader deployment-scale owner evidence, and larger-graph latency evidence all preserve the previous compatible pair. |
| 6. Public read-surface closeout | Building, failed, abandoned, and attempt-scoped output are invisible through every score-bearing read path (direct top-k, traversal projection/order/filter, search rerank, profile/explain, cross-shard fan-in). `graph_metric_rerank` blending is always explicit in the request, never a hidden ranking boost. | Cross-shard fan-in validates compatible nonzero published generations and rejects mismatched index/metric/status identity, missing/duplicate/unrequested metrics, non-finite scores/progress, and invalid published states before merging. Until a coordinator publishes one table-wide snapshot, multi-shard requests using direct metric top-k, metric rerank, or metric projection/ordering/filtering fail closed before fan-out. | Direct top-k, traversal projection/order/filter/status, search rerank, query profile, hosted fan-in, and public action/status routes either read compatible complete published generations or fail closed with `MetricNotReady`/`MetricStale`. Full promotion-scale shard layouts remain a separate release gate. |
| 7. Operations and cleanup release gate | V1 keeps latest-only retention: completed, failed, abandoned, and unpublished build state is removed once snapshot-safe; richer retention is a future bounded admin/debug option, not query semantics. | Cleanup is a resumable phase covering score generations, job/page/manifest/phase-summary/iteration-summary/attempt records, and runtime-owner leases. Status exposes freshness, phase/iteration progress, owner hashes, worker counts, lease state, takeover/lost-lease counters, page counters, and bounded failure/event diagnostics (pruned to a fixed retention cap) without raw storage keys or attempt namespaces. | Cleanup resumes after restart for every durable record type; storage does not grow without bound; diagnostics stay bounded; no retention knob is required for correctness. |
| 8. Default widening | Public metric behavior stays stable while the executor changes behind internal gates; local runners remain the CI/debug oracle. | The conservative `auto` gate currently admits already-active planned work plus queued degree/PageRank/eigenvector/compatible-HITS pairs under bounded scheduler caps; incompatible HITS pairs and explicit operator caps fall back to local execution. | A metric family becomes distributed-by-default only after parity, remote ownership, crash/restart, cleanup, public-read, fan-in, status, operations, latency-budget, generated-client, docs, and rollback checks are all green for that family — widened one family at a time: degree, PageRank, eigenvector, then HITS. |

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
