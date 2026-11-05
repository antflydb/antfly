# Serverless multi-node SQL joins

Status: broadcast, bucket-compatible, and shuffle execution, adaptive partition
coalescing and skew tiling, durable recovery, and Cloudflare bindings are
implemented as opt-in host capabilities in
[`zig/pkg/antfly/src/serverless/sql`](../../zig/pkg/antfly/src/serverless/sql/README.md).
The [Cloudflare SQL profile](../../examples/cloudflare-serverless/SQL.md) documents
WASM, R2, Queue, and Durable Object deployment. The companion Colony operator
owns native query and maintenance pools.

The scale target, up to roughly a trillion rows, is not a qualified performance
claim. Implementation and local correctness tests do not establish provider
throughput or latency at that scale. The final section distinguishes remaining
work from the execution paths described below.

## Routing and supported SQL

Small queries retain the existing local execution path, including native grace
and skew joins when local spill is enabled. The opt-in bridge lowers supported
direct lake equijoins to distributed fragments. Local operators above that
relation retain filter, projection, aggregation, ordering, and limit semantics.
Unsupported lowerings remain on the ordinary local path.

Fragments contain authorized, retained input capabilities, column ordinals,
scalar programs, join semantics, and resource limits. They never contain process
pointers or caller-supplied storage credentials. The module README records the
supported lowering boundaries and embedding contracts.

## Strategy selection

The planner uses retained-size and row estimates, supported bucket layouts, and
query/task admission limits. Planning does not fetch Parquet footers.

### Local

Relations below the bridge's local threshold use the existing local operators.
This preserves small-query routing and avoids distributed coordination overhead.

### Co-partitioned joins

Compatible Iceberg `bucket[N]` layouts on aligned join keys can bypass shuffle.
The planner groups buckets using compatible divisors of the bucket counts,
subject to task budgets. Workers validate the captured layout and prune files
for their assigned bucket group.

The managed writer and compactor preserve supported bucketed data layouts.
Equality deletes remain global across partition-spec evolution and bucket moves;
retained WAL changes suppress replaced base rows before the join and are routed
to the appropriate bucket group.

### Broadcast

When the right build input fits the conservative task budget and join semantics
permit it, workers publish typed build fragments and probe independent left
splits. Right and full joins cannot use this replicated-build strategy because
they require global ownership of unmatched build rows. Large fragment counts
use a bounded fan-in consolidation tree.

Each probe worker constructs its dynamic filter from accepted build rows. It
installs pruning only where the join may discard unmatched probe rows. Input
filters and residual join conditions preserve SQL NULL and outer-join semantics.

### Shuffle

Map tasks apply the retained WAL overlay and input predicates before hashing
join keys. Each map publishes one immutable exchange object containing indexed
typed blocks for its virtual partitions. Reducers range-read their inputs;
bounded consolidation trees avoid reopening hundreds of small fragments in one
task.

Initial virtual partition counts are powers of two, chosen against retained
bytes, rows, and task limits. Native ordinary reducers reuse the disk grace/skew
kernel. Hosted tasks have no local spill and enforce their in-memory budgets.

### Adaptive execution

Accepted map indexes supply actual partition bytes and row counts. Before
reducing, the coordinator coalesces small adjacent partitions and isolates skewed
partitions into bounded map tiles. Tile-pair tasks can run on native and hosted
workers. Authenticated immutable tile pages hold large layouts; mutable
coordinator records retain bounded references and resumable progress.

For right/full joins, accepted match witnesses are combined before one owner
emits an unmatched build row. Left, semi, and anti joins similarly use global
probe ownership when input tiles are replicated. A task never makes an unmatched
row decision using only its own subset of a skewed partition.

Automatic switching from an admitted shuffle plan to broadcast after observing
map output is not implemented. Adaptive execution currently means partition
coalescing and skew tiling.

## Ownership, recovery, and consistency

A query is identified by its immutable plan digest and expiry. Its durable
admission sidecar fixes the task retry limit. Query and task records use
conditional writes, boot-specific owners, lease epochs, and expiry checks to
fence stale workers. Immutable content-addressed outputs become authoritative
only through the accepted task record. Duplicate delivery and lost responses
reuse accepted output.

Each input carries a pinned base snapshot and copied accepted WAL cut. Workers
validate tenant scope, artifact-store identity, schema, table incarnation, and
expiry. Inserts, replacements, and deletes are applied before input predicates,
dynamic filters, or exchange. Retries reopen the same retained inputs rather
than a newer source catalog.

Native query workers own bounded background coordinator lanes and a separate
bounded delivery pool. Query queues rotate fairly, deduplicate queued and active
tickets, and retain retry backoff without blocking metadata turns. Reserved
coordinator lanes renew leases independently of slow task RPCs. API gateways
export retained grants and poll accepted results; they do not execute tasks.
Maintenance pools collect expired state and exchange objects from expiry-first
roots, independently of discovery hints.

Submission durably schedules a query. Remote result polling is read-only and
sends no turn request on each poll. Explicit turn requests restore a missing
local hint but coalesce with queued or active work without shortening heartbeat
or failure delays. Discovery follows the same rule. Actual delivery progress and
durable cancellation still wake the coordinator immediately. Lost hints and
process failures recover through durable discovery. Inline embeddings without a
remote gateway continue to drive their own coordinator.

Cloudflare uses the same execution contracts through WASM and R2. A Durable
Object owns coordinator turns and arms recovery before execution or Queue
submission. Queue consumers acknowledge accepted terminal task records and wake
orchestration. Separate named discovery and collector objects persist bounded
alarm continuations; cron starts them independently as a repair backstop. The
collector drains each expiry lane independently, preserves failure backoff across
eviction, and stops once only live objects remain. These continuations repair
missed delivery and process eviction without making cron the cleanup throughput
limit. The checked-in Wrangler profile
binds the coordinator class, task Queue, source/execution R2 buckets, and cron.

## Resource admission

Planning rejects estimates that exceed scan, request, task-count, retry, or
exchange budgets. Runtime counters enforce actual per-task memory, scan, output,
and spill limits. Bounded result manifests and aggregate fan-in trees avoid
loading an entire query's intermediate state into one coordinator invocation.

Native query pods reserve a spill-capable task's complete byte envelope, capped
by the pod spill budget, and 64 file slots before acquiring its durable task
lease. Each admitted task owns an independent spill manager. The process file
envelope is 64 times worker concurrency; the spill byte budget remains shared
across admitted tasks. Map stages stream bounded blocks without reserving spill.
Capacity pressure returns HTTP 429 and permits peer failover without quarantining
a healthy endpoint or spending durable task attempts. Every exit releases the
reservation. Exceeding an admitted per-task quota remains a permanent failure.

Native deployment separates per-pod task concurrency from query fan-out. The
Colony operator configures memory budgets, spill volumes, role Deployments,
readiness probes, ready-pod DNS, optional HPA, disruption budgets, network policy,
and Secret-backed authentication. Source read/pin authority and execution-store
write authority are separate from table publication authority.

Hosted execution uses bounded in-memory tasks, request admission, a bounded WASM
host pool, and R2 exchange. Per-invocation operation, transfer, and memory limits
remain enforced. Large skew is divided into durable tasks rather than relying
on native filesystem spill in a WASM isolate.

## Validation and remaining work

The native suite exercises broadcast, bucket, shuffle, adaptive/skew execution,
NULL and duplicate semantics, inner/outer/semi/anti joins, retained WAL overlays,
aggregate partials, and parity with local SQL. It also covers fencing, lost
responses, retry admission, cancellation, bounded metadata work, endpoint
recovery, spill isolation, and coordinator takeover.

Hosted tests use native-generated retained Parquet/WAL fixtures with the WASM
kernel. Local workerd coverage executes the actual Durable Object, Queue, and R2
bindings across restart. These are implemented and tested paths, not proposals;
a provider deployment and throughput qualification are separate work.

Remaining extensions include automatic post-map shuffle-to-broadcast conversion,
broader fragment lowering, merge-join planning from sort orders, and further
source-pruning improvements. Scale qualification should report shuffled bytes,
object-store requests, task counts, peak per-task memory, and latency distributions
for large star schemas and skewed many-to-many joins. Until that evidence exists,
the trillion-row goal remains a target.
