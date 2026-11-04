# Distributed lake SQL execution

This module implements the native execution service proposed in
[the distributed join design](../../../../../../docs/design/serverless-distributed-joins.md).
It is opt-in through host capabilities; it does not change small-query routing
or enable an unauthenticated worker API by default.

## Native deployment

The API gateway and `serverless-query` / `serverless-maintenance` commands
construct the execution service when these environment variables are present:

```sh
ANTFLY_SQL_STATE_URI=gs://execution/cluster/state
ANTFLY_SQL_EXCHANGE_URI=gs://execution/cluster/exchange
ANTFLY_SQL_STATE_CONNECTION=execution
ANTFLY_SQL_EXCHANGE_CONNECTION=execution
ANTFLY_SQL_QUERY_WORKERS='["http://query-pool:12390"]'
ANTFLY_SQL_QUERY_DNS=query-pool.namespace.svc.cluster.local
ANTFLY_SQL_SERVICE_SECRET=<dedicated-secret-of-at-least-32-bytes>
ANTFLY_SQL_SERVICE_ISSUER=sql:cluster-incarnation
ANTFLY_SQL_TASK_MEMORY_BYTES=134217728
ANTFLY_SQL_CONCURRENCY=4
ANTFLY_SQL_SPILL_DIR=/var/lib/antfly/sql-spill
```

`QUERY_DNS` and spill are optional. DNS discovery snapshots ready pod addresses
for each dispatch; failed or busy workers fail over without changing task
identity. Initial worker URLs also support fixed native deployments. Ports in
DNS mode use the query listener's standard port 12390. State and exchange use
configured connection aliases and their existing credential policy; file URIs
without aliases support local development. All participants must use the same
state/exchange URIs and issuer. Scope identities include the trusted SQL read
principal. The dedicated SQL signing secret must differ from the principal
credential signing secret. Secret values belong in a keystore or Secret-backed
environment variables, never in a ConfigMap.

Gateways export read-only, scope-bound table grants with retained cuts. Query
workers validate the grant's scope, artifact-store identity, schema, table
incarnation and expiry and reopen the captured snapshot and WAL suffix. They
need source read access and snapshot-pin access, plus execution-store writes;
they do not need table publication authority. Query pools run recovery and
signed task/control RPCs. Maintenance pools collect expired execution records
and exchange objects. API gateways never execute worker tasks.

The companion Colony operator adds `AntflyCluster.spec.serverlessSQL` and
owns long-lived query and maintenance Deployments, ready-pod DNS discovery,
admission budgets, spill volumes, probes, optional HPA, disruption budgets,
network policy and Secret rotation. Operator code lives in the Colony repository.
Use an Antfly image containing this runtime with the companion operator change.

Embeddings can still supply `runtime.Factory`, `lake_source.Host`,
`transport.Client`, `driver.Driver`, `collector.Collector` and
`bridge.Bridge.port()` directly. Custom authorization resolvers override the
retained read grant and must enforce the same scope and unsupported-policy
checks. Ordinary SQL keeps local filter/projection/aggregate/order/limit
operators above the distributed relation and exports its existing statement cut.

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
  accepted output authenticates a version-2 footer with per-block hashes,
  offsets, row counts and retained-memory statistics. A reducer reads one
  footer and one bounded range per consumed block, without retaining the whole
  partition. Empty partitions need only the footer. Columnar v2 blocks preserve SQL NULL independently of
  JSON null, exact NUMERIC, arrays, and encoded payloads.
- Adaptive coalescing groups adjacent small partitions from accepted map
  statistics. Recovery checkpoints at most sixteen map directories per turn;
  immutable progress objects and the final boundary list are fenced by the
  query CAS. Workers never infer a layout from changing pod counts. Aggregation
  fan-in uses the resolved reducer count.
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
distributed skew replication, Cloudflare Queue/Durable Object
bindings and R2 recursive repartitioning remain subsequent strategy work.
There is no claim of trillion-row or cloud-provider performance qualification.
