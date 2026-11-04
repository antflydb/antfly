# Distributed lake SQL execution

This module implements the native execution service proposed in
[the distributed join design](../../../../../../docs/design/serverless-distributed-joins.md).
It is opt-in through host capabilities; it does not change small-query routing
or enable an unauthenticated worker API by default.

## Integration

1. Open scope-isolated shared object-store lanes for execution state and
   exchange. All pods participating in a scope use the same lanes. Keep these
   credentials separate from table-publication authority.
2. Construct `lake_source.Host` with a native artifact store, shared serving
   cache, object-store options, and an authorization resolver. The resolver
   re-establishes the authorized table incarnation and columns for the opaque
   principal/policy scope. It must reject unsupported row policies.
3. Construct `runtime.Service` for query/combined roles. `owner` must be unique
   to a process incarnation, not a reusable pod or deployment name. Supply the
   host's source and retention adapters.
4. Install `transport.Server` through
   `ServerlessHttpServerConfig.distributed_sql`, with the existing internal
   service signing configuration and the query pool's foreground admission.
   `runtime.Factory` resolves the authenticated scope to its service. Internal
   requests carry tickets, not arbitrary source URLs or credentials. The
   public API host can install the same factory through
   `ApiHttpServerConfig.distributed_sql`.
5. Supply a `transport.Client` dispatcher with trusted worker discovery URLs
   and the node HTTP executor. Its bounded fan-out uses concurrent requests
   only when the executor advertises that capability. Service discovery may
   change on retries; no task has permanent pod ownership.
6. Start `driver.Driver` through `ServerlessServerConfig.sql_driver`. It runs
   independently of maintenance, discovers durable query submissions, and
   resumes coordinator turns after failures. Query-only nodes do not run GC.
7. Install `collector.Collector` through
   `BootstrapConfig.sql_execution_gc` on maintenance/combined nodes. It walks
   expiry-first indices and reclaims bounded batches of task/query records and
   exchange artifacts. Existing retained-cut collection reclaims the copied WAL
   capabilities after their own expiry.
8. For ordinary SQL routing, install `bridge.Bridge.port()` through
   `ApiHttpServerConfig.distributed_sql_join` (or `sql.catalog.Backend` for an
   embedding). The bridge lowers direct lake equijoins above `local_bytes`.
   SQL's normal filter, projection, aggregate, order, and limit operators remain
   authoritative above the distributed relation. Captured statement cursors
   export durable read capabilities; workers never reopen a fresh read cut.

The factories and discovery ports are deployment-owned. There is no new
Kubernetes node role, per-task Kubernetes Job, operator CRD, or automatic CLI
configuration for these capabilities in this change.

## Durable protocol

Plans are versioned, pointer-free JSON; scalar fragments contain validated
instruction DAGs with bound parameter values. Process caches, settings captures,
external AI calls, and relation-node pointers are not serialized. A scope plus
plan content identifies a query. Deterministic stage/task IDs make dispatch
recoverable without relying on queue acknowledgements.

A task's owner, epoch, deadline, and accepted output share one conditional
record. Completion CAS therefore rejects an attempt superseded by another
worker. Lost completion responses resolve by exact accepted output identity.
Coordinator checkpoints and terminal transitions similarly require the current
claim. Expiry and the 30-second grace fence cleanup from in-flight attempts.

The native lake adapter copies the accepted WAL suffix into the retained-cut
capability and pins base snapshots through capability expiry. Every split
suppresses the complete changed-key set. Each recent image belongs to exactly
one deterministic split before exchange. Base scans consume typed column pages
directly; only changed-key checks and the bounded recent WAL images require
row objects. Filters execute after this overlay;
workers validate the capability, schema incarnation, and snapshot on reopen.

## Execution and limits

- Broadcast builds each right split once and publishes typed blocks. Probe
  tasks install safe dynamic filters before scanning. Left/anti joins retain
  unmatched probe rows; right/full joins use shuffle rather than replicating
  unmatched build decisions.
- Shuffle maps write one object containing contiguous partition runs. The
  accepted output authenticates its footer index; a reducer needs one footer
  range GET and one partition range GET per nonempty map input. Empty partitions
  need only the footer. Columnar v2 blocks preserve SQL NULL independently of
  JSON null, exact NUMERIC, arrays, and encoded payloads.
- Native reducers with a supplied spill manager reuse the existing grace/skew
  join kernel. Without local spill, oversized tasks fail their explicit budget.
  Semi/anti reducers use the bounded hash kernel.
- Plans submitted directly to the service can push grouped aggregate partials
  above joins. These use the existing aggregate interchange and a fan-in-32
  merge tree; only the last merge finalizes values. SQL bridge aggregation
  remains in the ordinary SQL engine.
- Admission estimates scan/exchange bytes, task count, retry output reservation,
  and exchange requests. Each task has an actual shared allocator ceiling,
  processed-row ceiling, output-byte ceiling, attempt bound, and I/O deadline.
  Request estimates are not a provider-level hard request meter.
- Object-store listing and cleanup are bounded. Driver discovery is round-robin;
  live early queries cannot hide later submissions. Accepted outputs, including
  empty results, are reused after coordinator takeover.

## Remaining strategy work

The service supplies native broadcast/shuffle execution and their common
recovery contracts. Bucket-compatible planning/native bucketed writes, adaptive
partition coalescing and skew replication, Cloudflare Queue/Durable Object
bindings and R2 recursive repartitioning remain subsequent strategy work.
There is no claim of trillion-row or cloud-provider performance qualification.
