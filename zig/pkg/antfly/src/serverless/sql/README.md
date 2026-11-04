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
ANTFLY_SQL_QUERY_CONCURRENCY=64
ANTFLY_SQL_QUERY_LIMITS='{"query_exchange_bytes":8589934592}'
ANTFLY_SQL_SPILL_DIR=/var/lib/antfly/sql-spill
```

`QUERY_DNS`, query-limit overrides and spill are optional. Per-pod task admission
and total per-query fan-out are separate budgets. SQL-specific startup/readiness
probes fail closed on images without the execution service. DNS discovery snapshots ready pod addresses
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
signed task/control RPCs. Terminal CAS operations retire discovery hints; expiry
GC uses the actual state/exchange roots independently. Recovery skips live foreign
leases, drains discovery pages promptly, and backs off empty sweeps to about
30 seconds with boot-specific jitter. Active owners retain fast recovery.
Submissions, cancellations, and foreground ownership acquisition signal an interruptible
local event after durable admission. An allocation-free, coalescing FIFO retains
up to 256 query hints, with at most 16 processed per turn. Hinted queries bypass
the discovery cursor and remain queued until complete. Internal progress never
self-signals; notification overflow and process loss fall back to durable
round-robin discovery. Embeddings wire `Driver.notification()` into their
`Service.notification` when they use the native recovery driver. Maintenance
pools collect expired execution records and exchange objects. API gateways never execute worker tasks.

Native coordinator turns enqueue task tickets without waiting for task RPCs.
A fixed 1,024-ticket queue includes in-flight tickets for deduplication and
rotates across up to 256 query queues. The process-wide delivery pool is bounded
by `ANTFLY_SQL_QUERY_CONCURRENCY`; worker task admission remains independently
bounded by `ANTFLY_SQL_CONCURRENCY`. Queue saturation never blocks coordinator
progress: persisted pending tasks replay omitted hints. Delivery completion wakes
the owning driver, and slow RPCs do not delay new admissions, cancellation, or
lease renewal. While delivery is pending, hints use ten-second heartbeats;
completion/cancellation notifications override that delay. Foreground polling
skips task enumeration until progress or the lease's twenty-second renewal
margin, avoiding repeated task GETs during slow RPCs. A process-local turn registry prevents foreground/recovery turns
from overlapping under the same owner/epoch. Shutdown joins delivery before
releasing the transport and stores. Transient delivery failures retain deduplication
identity and retry with jittered exponential backoff (250 ms through 5 seconds,
at most eight deliveries before returning to durable recovery). Embedders using native HTTP dispatch should
wire `Scheduler.dispatcher()` and `Turns.port()` as the deployment owner does.

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
plan content identifies a query. Version-2 query references carry that full digest
and expiry; state objects use an expiry-first namespace, independently of the
discovery marker, so late writes remain discoverable after cleanup. Deterministic stage/task IDs make dispatch
recoverable without relying on queue acknowledgements.

A task's owner, epoch, deadline, and accepted output share one conditional
record. Completion CAS therefore rejects an attempt superseded by another
worker. Lost completion responses resolve by exact accepted output identity.
Coordinator checkpoints and terminal transitions similarly require the current
claim. Scope-checked cancellation sets a durable flag without stealing that lease;
the owner stops dispatch, and an accepted cancellation wins a later result CAS.
Expiry and the 30-second grace fence cleanup from live attempts. Expired physical
names cannot be reused by a live query, so bounded cleanup works on R2 without
conditional DELETE.

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
  statistics. Recovery checkpoints at most eight map directories per turn;
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

## Hosted execution

The [Cloudflare SQL profile](../../../../../../examples/cloudflare-serverless/SQL.md)
wires this service to WASM, R2, Durable Object alarms and Queues. Its tests share
native-generated retained Parquet/WAL fixtures and run under real local workerd
bindings. No provider deployment or throughput qualification is claimed.

Bucket-compatible plans reuse proven native bucket layouts. Adaptive coalescing
and skew tiling run on native and hosted workers. Layouts use authenticated
immutable tile pages; coordinator CAS records retain only bounded references.
Pinned source scans divide canonical row ranges within files and row groups,
with page pruning and retained-size-aware task planning. Native reducers also
reuse the disk grace/skew kernel; hosted tasks enforce their in-memory budgets.

Query control requests select a stable worker by query identity, with bounded
failover. Statement deadlines and cancellation cover result exchange reads.
Both SQL roles expose capability readiness at `/readyz/sql` on the health
listener (port 4200 by default); maintenance does not need a query listener.


Result assembly checkpoints at most 64 accepted outputs per coordinator turn.
Immutable result pages link to the preceding page, keeping checkpoint and final
root size constant while writing each descriptor once. Results with at most
64 outputs retain the inline `pages` representation when assembly finishes in
one turn. Slow storage yields earlier, retaining time for publication and the
query CAS; native reads use private arenas in concurrent waves of eight.
Partially assembled and larger internal result
handles expose a single `manifests` head; follow each authenticated result
page's `previous` reference to stream its `pages`. Physical output order is
unspecified; the ordinary SQL engine still owns `ORDER BY`. The native gateway
keeps only one manifest page and one exchange decode block resident.

For outer/semi/anti skew joins, fan-ins above 32 use deterministic distributed
bitmap reduction trees. Each merge task consumes at most 32 accepted proofs;
a finalizer consumes the merged root. Small fan-ins read pair proofs directly.
The resolved plan reserves every tree task and output before dispatch. Tree
identities, accepted outputs and result checkpoints survive at-least-once
messages, lost acknowledgements and coordinator replacement.


Ordinary shuffle partitions and broadcast builds with more than 32 original
fragments use bounded fan-in-32 compaction stages. Each side stays separate;
reducers consume compacted roots instead of reopening every original map.
Adjacent-partition planning includes retained bytes from both sides so each
ordinary compaction output stays within task memory. Oversized partitions keep
their original map identities for skew tiles and match ownership. Admission
reserves compaction tasks together with join, proof and aggregate tasks.

Distributed cuts larger than 64 KiB of WAL images retain those images in
immutable pages, with a Bloom filter and sorted exact changed-key digest pages.
Every worker suppresses changed base rows against that full index, while it
reads only its disjoint contiguous interval of recent WAL images. A key-only
pass sorts candidate digests and checks each relevant key page once. Only keys
and a suppression bitmap share task memory. A second pinned pass streams
surviving payloads; fully suppressed splits skip that pass entirely. Both passes
use unfiltered ordinals to keep the bitmap aligned. Image pages are invocation
private. New writers use binary SHA-256 pages; readers also accept legacy ASCII
pages. All child references bind the parent's table/store domain and expiry.
Small cuts remain inline, and local cut readers can materialize paged cuts.

Paged cuts include WAL bytes/rows in planning and advertise minimum parallelism
that assigns at most 128 recent WAL images to each fragment. Committed base rows
retain their normal scan and byte budgets, independently of the image limit.
Workers enforce the image bound against the authenticated cut, rather than
trusting estimated base row counts. Key-page reads are bounded by the exact
index page count, rather than physical row order or cache misses. Object I/O
budgets remain bounded.
Bucket execution falls back to shuffle for these cuts. Image pages target 32 KiB;
individual larger rows remain subject to the existing task/query budgets.

Query records now retain the admitted tenant digest. Control operations validate
that compact scope before returning status/results or changing ownership. Jobs
written before the scope field was added validate their immutable plan instead;
new result polling reads only the query record and result manifest. Task records
also carry scope, and recovery can reopen new jobs without rereading a plan just
to discover their tenant.

Native delivery purges expired queued tickets before capacity admission or
selection. In-flight RPC deadlines are capped by query expiry, and expired
failures cannot enter retry backoff. SQL execution GC has a separate monotonic
schedule: empty or failed passes back off to thirty seconds with boot jitter;
nonempty pages continue promptly. SQL-only maintenance sleeps until that
schedule, while mixed-role maintenance retains its other work deadlines.
Collection errors retry on this schedule without stopping unrelated maintenance.

Native recovery separates bounded discovery from metadata execution. Sixteen
metadata lanes share a coalescing FIFO; two lanes are reserved for imminent lease
renewal. A due renewal performs only the scope check and lease CAS, preserving
its pending-task cursor. Ordinary turns have a ten-second I/O quantum, and task
status/admission waves overlap at most sixteen/eight native operations with
private response arenas. Admission checkpoints at most eight new tasks per
quantum and explicitly retains the refill signal until the query window is full.
Native control RPCs authorize and notify this scheduler rather than running a
second set of metadata turns. Transient failures and delivery saturation back
off without blocking other queries. `Driver.start()` enables these production
lanes; synchronous `tick()` remains a deterministic embedding/test hook.

Worker tasks prepare only the predicates used by their physical task kind.
Typed instructions remain immutable and borrowed; scalar constant/decimal plans,
a bounded regex session, and reusable row scratch belong to that task. Both VM
and regex work inherit the task's cancellation/deadline checkpoints. Map filters,
bucket/broadcast probes, ordinary and spill joins, and skew pairs all use this
same execution owner, avoiding per-row regex compilation and setup allocation.

Adaptive tile indexing makes one shared map pass for every hot partition. All
partition tails live in an authenticated packed buffer object capped at 4 MiB;
the coordinator CAS references it alongside the immutable page lists. Each turn
either reads eight map directories or flushes at most eight tile pages, keeping
storage operation counts linear in maps plus published pages. Older in-flight
per-partition checkpoints retain their recovery path during rolling upgrades.
