# System catalog benchmarks

Run these from `zig/`. Results describe a particular binary, machine, and workload;
they are not CI latency thresholds or production capacity claims.
See [recorded measurements](SYSTEM_CATALOG_RESULTS.md) for a reproducible local
before/after comparison and representative request latencies.

## Event enrichment across shards

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --binary /path/to/immutable/antfly --scenario catalog --deployment cluster \
  --table-counts 10 --catalog-shards 8 --schema-fields 32 --join-rows 1000 \
  --samples 20 --warmup 2 --ndjson-lines 20 --output enrichment.json
```

`--join-rows` adds hash-distributed events with repeated customer keys, then
measures lookup and broadcast enrichment separately. Every result is checked
for complete event identities, duplicates, and the correct customer payload.
Setup and cleanup use individual document mutations outside request timings,
so constructing the dataset does not require cross-shard transaction preparation.
The artifact records shard
owners and verifies remote right-hand shards. Compare immutable baseline and
candidate binaries serially; sweep shard and row counts to separate per-row
partitioning from transport fanout. Debug builds are useful for relative costs,
but these measurements do not establish production capacity.

The lookup partition component compares the prior per-group scan with the
production partitioner on 20,000 rows and 64 ranges, including bucket allocation
and destruction. It validates every bucket count, warms once, and records seven
paired samples without a timing assertion:

```sh
ANTFLY_CATALOG_JOIN_PARTITION_BENCH=1 python3 tools/run_bounded_zig_build.py \
  build public-api-parity-test -- --test-filter 'distributed join lookup partition benchmark'
```

## Reporting cadence and concurrent migrations

```sh
ANTFLY_CATALOG_REPORT_BENCH=1 python3 tools/run_bounded_zig_build.py build antfly-system-catalog-report-bench
ANTFLY_CATALOG_REPORT_BENCH=1 python3 tools/run_bounded_zig_build.py build antfly-system-catalog-finalization-test
ANTFLY_BIN=./zig-out/bin/antfly ANTFLY_E2E_PHASE_TIMINGS=1 uv run --project e2e/antfly pytest -q -s \
  e2e/antfly/test_catalog_resilience.py::test_concurrent_tenant_schema_migrations_preserve_documents
```

The runtime-cache component models a node with 1,000 or 10,000 groups and 32
indexes per group. It compares copying/freeing the former cache with retaining/
releasing acknowledged runtime leaves, including the ordered lease map. Both use
`c_allocator`, one warmup, and nine samples. It excludes report collection and
transport. Structural changes still require fresh inventory; clean Raft reporting
ticks now refresh live Raft facts without collecting index inventory.

The finalization component models 100 concurrent tenant migrations, 1,000 groups,
and 3,000 placements plus one unhosted table. It checks every result against the
former scan with one warmup and five paired samples. The new interval includes
schema parsing and readiness-index construction; the reference interval excludes
schema parsing. Index destruction is excluded. Neither includes document rebuilds,
HTTP, or Raft. These are Debug comparisons unless an optimization mode is supplied.

The cluster scenario complements those component measurements with three tables,
two shards per table, three replicas per shard, and existing documents. It updates schemas concurrently,
waits for all cutovers, and verifies reads. Its optional phase timing includes
migration and validation, excludes provisioning, and has no performance threshold.
Use repeated independent runs for latency comparisons. Node-drain E2Es separately
verify that live Raft apply progress still satisfies retirement fences.

For skewed tenants under foreground traffic, compare immutable binaries serially:

```sh
uv run --project e2e/antfly python tools/benchmark_schema_migrations.py \
  --binary zig-out/bin/antfly --baseline /path/to/baseline \
  --traffic-clients 4 --samples 3 --warmups 0 --output migration-comparison.json
```

Each fresh three-metadata/three-data cluster migrates a 10,000-document tenant
alongside five small tenants. Independent clients write, look up documents, and
search during the migrations. The scenario verifies every acknowledged write
and final full-text counts; failures remain in the artifact and ambiguous writes
are never replayed to make a run pass. Inspect completion times together with
request distributions because traffic is closed-loop.

## Catalog scale in process

```sh
zig build antfly-system-catalog-bench
zig build antfly-system-catalog-routing-bench
```

`antfly-system-catalog-bench` builds its own ReleaseFast executable. It reports five-sample medians
for indexed versus scanned name lookup and table-rename planning at 1,000,
10,000, and 100,000 tables. Tenant offboarding measures planning and applying a
database drop with 1,000 or 10,000 empty namespaces while retaining another
database's tables. Fixture construction is outside the timed region. Rename
includes construction of the planner's indexes; lookup reuses an owned index.
Tenant-management microbenchmarks additionally compare repeated related-record
scans with indexed projection, and per-command index rebuilding with a retained
reader. Those comparisons isolate algorithm costs, not HTTP or Raft latency.

The routing target compares cloning/rebuilding a compact routing generation for
each request with retaining its existing indexes at 10, 1,000, and 10,000 tables.
It uses ReleaseFast and `c_allocator`, 100 requests per sample, and three target
keys. These are component costs, not HTTP latency. The catalog target also
compares whole-state copy/index rebuilding with affected-record apply/undo.

## Live application workflows

```sh
zig build antfly
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario all --output /tmp/catalog-workloads.json
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario catalog --deployment cluster --table-counts 10 100 \
  --output /tmp/catalog-cluster.json
```

The harness starts disposable servers with reserved loopback ports, uses real
HTTP requests, validates results, and stops its servers on exit. It needs the
normal E2E Python dependencies and permission to open local sockets. Startup,
table creation, shard-readiness waits, warmup, and measured requests are kept
separate. Cluster setup waits for each new shard to report a healthy voter and
a known leader on every metadata node before starting the next table. Resolution
setup also waits for the entity and document shards before seeding candidates.
Resolution timeout diagnostics include the source key, expected destinations,
graph response, and index status. The output
records the binary SHA-256, platform, complete settings, sample counts, and
p50/p95/max latency. Supply `--binary` to compare separately built revisions;
use the same build mode and settings, and run them without competing workloads.
The normal development build includes debug overhead.

The catalog workload models an application serving tenant-scoped tables:

- Provision a database, namespace, and inherited placement policy; grow from
  10 to 100 tables by default, with one document in each measured target.
- Read a document, issue a qualified query and join to a second table, and run 20 NDJSON
  queries sharing a target. NDJSON timing is for the whole HTTP batch.
- Repeatedly replace one document with full-index visibility while unrelated
  table count grows. `--schema-fields 32` enables type enforcement and models
  schema-constrained event ingestion; the timed write includes validation,
  replication, storage, and indexing rather than isolating catalog CPU cost.
- List the namespace's tables, rename a table while checking stable identity,
  and run concurrent qualified lookups with eight independent client sessions.
  Concurrent output includes total throughput and individual request latency.

The resolution workload models document ingestion into an entity knowledge
graph on three metadata and three data nodes:

- Use three document shards and one entity shard, with 10 and 100 mentions per
  document. Keys alternate across the three initial document key ranges.
  Exact-key candidate search exercises cross-shard document reads
  with an explicit exact-name scoring policy and no inference service. Half the entities already exist; the other
  half must be created by atomic promotion. Each document uses new keys.
- Measure from source write to a graph containing every hydrated entity. This
  includes resolution, promotion, graph publication, and polling overhead;
  it is not an isolated write or catalog-binding latency. Seed writes are
  outside this interval. Output includes readiness poll counts.
- Measure steady graph traversal with and without document hydration separately.

Additional production-shaped cases:

```sh
# Wide schemas: avoid repeatedly copying unrelated table definitions.
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario catalog --schema-fields 200 --table-counts 10 100 \
  --output /tmp/catalog-wide.json
# Repeated names share label-prefix candidates within each document.
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario resolution --resolution-workload prefix \
  --output /tmp/catalog-prefix.json
# Every alias resolves through a curated survivor document.
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario resolution --resolution-workload redirects \
  --output /tmp/catalog-redirects.json
# Tenant discovery and DDL while unrelated catalog inventory grows.
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario management --deployment cluster --tenant-counts 10 100 1000 \
  --samples 10 --concurrency 4 --output /tmp/catalog-management.json
# Standalone tenant DDL alongside readers, plus durable restart verification.
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario management --deployment standalone --tenant-counts 10 100 1000 \
  --samples 10 --concurrency 4 --restart-after-ddl \
  --output /tmp/catalog-standalone-recovery.json
# Compare clustered keys with keys distributed across entity ranges.
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario resolution --entity-shards 8 --entity-key-layout spread \
  --output /tmp/catalog-resolution-sharded.json
```

The prefix workload repeats a pool of ten names (or fewer for small mention
counts), including across documents; warmup populates that pool before measured
writes. The redirects workload seeds distinct aliases and survivors for every
document. Both validate the complete set of hydrated destination keys. Reports
record mention count, unique entity count, and seeded entity-document count.
The storage regression suite separately verifies that legacy and missing lookups
fit a 4 KiB caller allocator with 1,000 unrelated databases, and that reopening
repairs derived name indexes. These are allocation/correctness checks rather
than elapsed-time thresholds.

The management workload measures tenant point reads, database listings,
namespace create/drop, identity-preserving rename round trips, and concurrent
readers alongside namespace DDL. Provisioning is reported separately. Resolution
accepts `--entity-shards` (for example 1, 8, or 32) and `--entity-key-layout`.
`clustered` keeps the label-prefixed keys; `spread` uses a declared key template
with hexadecimal-leading names distributed across initial ranges. Use identical
settings for each binary and report clustered and spread cases separately.

Useful controls include `--table-counts`, `--mentions`, `--documents`, `--samples`,
`--warmup`, `--concurrency`, and `--ndjson-lines`. A quick harness check can use
`--table-counts 3 10 --mentions 4 10 --documents 2 --samples 3 --warmup 1`.
Requests and derived-readiness polling are bounded; failures abort the scenario
instead of becoming successful latency samples. These workloads measure the
catalog integration and resolver/graph path, not vector-search quality, inference
throughput, large-document indexing, or multi-machine network capacity.

Management reads and DDL are measured through the public API. Catalog and
resolution setup observes visibility-pending create acknowledgements with GET
and then waits for published shard leaders. It never replays a create to obtain
its response, and it does not retry measured requests or ambiguous writes.

`--restart-after-ddl` is optional and requires standalone mode. At each management
checkpoint it restarts after the mixed workload, verifies every database name/ID,
and checks that deleted temporary namespaces remain absent. Restart/readiness and
validation duration are separate from request samples. Compare binaries with
identical flag settings; use a separate recovery run when comparing steady
latency without checkpoint restarts.


### Scoped discovery and application schema diversity

The `listing` scenario models a tenant dashboard with one selected table beside
an expanding namespace, a prefix search returning one table, and an inventory
view returning every table in the large namespace. It verifies row counts and
scope isolation. Provisioning and shard readiness are excluded from latency.
Use both shared application schemas and independently evolved schemas:

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario listing --schema-fields 200 --table-counts 10 100 \
  --samples 10 --warmup 2 --output /tmp/catalog-listing.json
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario listing --schema-fields 200 --listing-distinct-schemas \
  --table-counts 10 100 --samples 10 --warmup 2 \
  --output /tmp/catalog-listing-distinct.json
```

The second command adds a different declared property to each large-namespace
table. This prevents a shared-schema benchmark from hiding cache-capacity costs.
Returned JSON size still grows with selected tables and schema width. The
bounded cache can evict definitions beyond its entry or byte budget; compare
larger inventories separately when sizing an application workload.


The component target also reports retained child-array bytes after 10, 1,000,
and 10,000 tenant create/drop cycles. This counts array capacity in the parent
index, excluding hash-table capacity, row storage, allocator overhead, and RSS.
It complements the public management workload and the commit/rollback regression
without treating memory counters as elapsed-time assertions.


### Large inventory alongside application reads

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario listing --schema-fields 200 --listing-distinct-schemas \
  --table-counts 100 200 --listing-page-size 25 --listing-concurrent \
  --samples 10 --warmup 2 --output /tmp/catalog-capacity.json
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario listing --schema-fields 200 --table-counts 1 100 1000 \
  --samples 5 --warmup 2 --output /tmp/catalog-detail-scale.json
```

The first workload crosses the 64 MiB schema cache budget with independently
evolved wide schemas. It measures complete inventory, first-page latency and a
validated complete cursor walk separately. It also runs one inventory scanner
alongside detail readers that remain active until the scan finishes, reporting
both latency distributions and detail throughput.
Repeat it with `--listing-reader-rate 100` to cap each reader at a target of
100 requests per second (800 total with the default eight readers). These are
paced, closed-loop clients: slow responses reduce delivered traffic, and missed
deadlines do not accumulate an unbounded backlog. Compare both target and actual
throughput; equal target rates do not guarantee equal delivered load. Keep the
uncapped run as a separate saturation experiment.
The second models a schema browser opening one table while unrelated tenant
inventory grows. Both include a one-table prefix control and an empty default
namespace. Add `--deployment cluster` to exercise durable metadata projections
and per-group runtime report selection. Page and concurrent workloads are opt-in
so the same harness can measure an older binary that lacks pagination.

## Scoped relational application workloads

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --binary zig-out/bin/antfly --scenario catalog --storage-mode relational \
  --table-counts 10 --samples 10 --warmup 3 \
  --output /tmp/system-catalog-relational.json
```

This models tenant event tables backed by authoritative packed rows: scoped point
reads, search, customer joins, repeated-target NDJSON requests, concurrent reads,
inventory discovery and identity-preserving rename. The fixture uses a closed
schema with a required body and optional customer key. Add `--schema-fields 200`
for wide rows or `--deployment cluster` for replicated metadata and data routing.
The `listing` scenario also accepts `--storage-mode relational`; other scenarios
reject it because their fixtures have different schema requirements. Provisioning
and shard readiness stay outside steady-state read measurements. Each operation
checks its result, but latency is an observation rather than a test assertion.

## Many-range control-plane reports and heartbeat framing

```sh
ANTFLY_CATALOG_REPORT_BENCH=1 zig build antfly-system-catalog-report-bench -Doptimize=ReleaseFast
```

This opt-in storage workload uses 100, 1,000 and 10,000 groups per store. It
measures committed metadata apply for cached report payloads, fresh observation
clocks, one changed group, every group changed, and cached runtime references,
plus full-store hydration
and placement drain checks. It calls `SnapshotBuilder.applyBatch`, including
command decode, checkpoint persistence, projection and transaction commit. Wire
encoding happens before timing. The earlier projection-only measurements omitted
the full-batch checkpoint write; see the correction in the results history.
Each apply changes the header's available-capacity counter; report clocks only
advance in the fresh-clock case. Seven measured samples follow fixture creation. It uses `c_allocator`, matching
the libc-linked ReleaseFast executable; correctness tests retain leak checking.
Output includes local WAL bytes, full wire record size and transmitted command
size. The runtime-reference case omits runtime arrays and retains the already
committed observations, while sending current group facts. Placement drain
checks use 100 operations per sample and report the per-operation median of
those samples, including opening/closing each read transaction. This isolates
local apply/storage work: network receipt, Raft proposal encoding/replication,
and periodic status collection are outside the interval. Use the same allocator,
build mode and host load for comparisons. It is not a benchmark of distributed
heartbeat latency.

[Heartbeat bundling](HEARTBEAT_BUNDLING.md) documents production route-aware
batching, retry ownership, isolated codec and transport-host benchmarks, and
larger live-cluster capacity workloads.

The report target also exercises repair-heavy admission at 1,000/10,000 groups
with separately allocated equal report slices, and selected-store admission at
1/10/100 stores with 100 runtime groups each. The latter compares the former
whole-inventory clone/capability scan with retained single-store selection in the
same binary, including clone destruction. These component scenarios model a
repair backlog and steady reporting beside growing unrelated tenant inventory;
they exclude protocol negotiation, proposals and network I/O. Compile once with
the environment flag unset, then run with it set after other builds finish.

### Admission, hydration and node recovery

Use these component workloads for the costs paid by large multi-tenant clusters:

```sh
# From zig/: compile both report executables before timing, then run serially.
env ANTFLY_CATALOG_REPORT_BENCH=1 zig build antfly-system-catalog-report-bench -Doptimize=ReleaseFast -j1
# A node becomes reachable with a full retry backlog (100/1,000/4,096 frames).
(cd lib/raft && zig build retry-bench -Doptimize=ReleaseFast -j1)
# Drain 256/1,024/4,096 queued requests across 16 peers, excluding network/setup.
env ANTFLY_HTTP_SCHEDULER_BENCH=1 zig build antfly-http-scheduler-bench -Doptimize=ReleaseFast -j1
```

`ADMISSION_PLAN_BENCH` compares the previous clone/compare/apply preparation
with production admission using borrowed pinned records. It covers unchanged
repair inventories and header changes at 100/1,000/10,000 groups, records successful
allocations, and checks allocation/free balance. Incoming arrays are distinct
from the prior snapshot. The fake proposal sink excludes encoding, replication
and commit latency. `HTTP_SCHEDULER_BENCH` compares ready-peer scheduling against
a reproduced global array FIFO using the same frame ownership/freeing costs;
its reference omits the old in-flight hash operations, making that comparison
conservative. Neither queue benchmark measures network throughput or elections.

Run these after task-owned builds and tests finish. The report target includes
cached/fresh/sparse/full/reference updates, WAL bytes, full hydration and its caller-owned allocation count/bytes (counted outside the timing loop), repair
comparison, and selected-store preparation. Keep the full and reference paths
in the results, including regressions. Pair component results with the existing
live tenant-provisioning/restart, scoped discovery and relational-query scenarios;
small live runs validate application behavior without establishing cluster capacity.

The report target also emits `PUBLISHER_BENCH` (full HTTP encoding versus an
acknowledged sparse prepare/encode/commit), `SPARSE_REPORT_BENCH` (one changed
group through command decode, WAL/checkpoint and commit), `COLLECTION_BENCH`
(linear scans versus building and using captured-inventory indexes), and
`RECONCILE_VIEW_BENCH` (deep clones versus retaining immutable store leaves).
The paired component comparisons run in one binary; setup is excluded and
temporary allocation/free costs are included. The publisher case has no volatile
embedding samples, so its byte count does not establish telemetry-heavy traffic
cost. Sparse apply still visits compact membership rows; only affected payload
components are decoded and rewritten.

For a live application workload, add `--mixed-seconds 30` to the catalog scenario:

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario catalog --deployment cluster --table-counts 10 100 \
  --storage-mode relational --samples 9 --warmup 2 --mixed-seconds 30 \
  --output /tmp/catalog-mixed.json
```

At each provisioned table count, this runs three clients concurrently: batches of
100 upserts with full-index acknowledgement, qualified search, and scoped catalog
discovery. Every response is validated; a failure aborts the workload. It reports
per-operation latency distributions and completion rates over the requested
duration. Provisioning/readiness and the mixed interval remain separate. This is
a bounded working set with repeated updates, not a storage growth or maximum
cluster capacity claim.

### Concurrent reporting and tenant DDL

Use the report-ingestion workload to measure many independent owners alongside
namespace create/drop operations on a real three-member metadata Raft cluster:

```sh
uv run --project e2e/antfly python tools/benchmark_report_ingestion.py \
  --reporters 8 --groups 100 --samples 30 \
  --output /tmp/catalog-report-ingestion.json
```

The workload registers synthetic non-live stores outside data placement, waits for
registration to become visible, then measures single-group runtime changes and
activity-only reports separately. It records report throughput/latency and tenant
DDL latency. Setup is untimed. Every HTTP failure aborts the run; successful
responses are required. The client includes metadata-leader discovery overhead.
`--wire previous` supports comparison against the pre-change binary's telemetry
shape; production has one compact activity shape. `--groups 1000` or `10000`
exercises large per-owner inventories independently of reporter concurrency.

The report component target also prints `GROUP_CACHE_BENCH` for full versus sparse
immutable cache publication, and `ACTIVITY_BENCH` for whole-runtime versus compact
bounded telemetry encoding. The activity workload uses one embedding index per
group and delivers all 1,000/10,000 samples in batches of at most 512. It reports
both aggregate bytes and the maximum request size, rather than treating a smaller
single batch as the full workload. Component times exclude HTTP and Raft network
latency; the cluster workload includes them.


### Failover, large control views, and independent telemetry

Run `test_catalog_resilience.py` with the catalog E2Es to verify that an activated
three-voter cluster continues sparse reports and namespace DDL after its leader
stops. The same module registers a 10,000-group synthetic non-live store, reads
complete diagnostics through bounded pages, and performs public DDL over multiple
control rounds while asserting that every real data node remains alive.

From `zig/`, run the delivery workload with:

```sh
uv run --project e2e/antfly python tools/benchmark_catalog_resilience.py \
  --groups 10000 --samples 12 --warmup 2 --output /tmp/catalog-resilience.json
```

This uses three metadata replicas and three real data processes. The synthetic
store has one embedding index per group and does not participate in placement.
It compares complete multi-batch collections through durable admission and the
independent telemetry path on the same binary, alongside namespace create/drop
pairs. Two warmup collections and DDL pairs are discarded. Export timings are cold
observations, not steady-state latency comparisons. Every real data node must
remain alive; a successful HTTP benchmark with terminated data nodes is invalid.
These synthetic group IDs are outside the catalog, so the compact control view
contains no group facts for that store. Actual placements and active transition
groups remain in control views.

The component target also prints `GROUP_CACHE_FIRST_READER_BENCH`. Pair it with
`GROUP_CACHE_BENCH`: sparse publication now retains a persistent tree, while the
first flat-view consumer pays the explicit O(G) materialization cost. Subsequent
readers reuse that array. Do not attribute the publication speedup to whole-view
reads or durable persistence, which have separate measurements.


The resilience harness establishes the tenant before loading its large baseline,
then waits for stable metadata leadership and successful catalog reads. It records
startup, baseline readiness, setup, cold snapshot transfer, warmup, and measured
work separately. Setup reads may retry within a 30-second readiness budget;
measured telemetry and namespace requests never retry. Both workers start
together and perform the configured number of collections/pairs. Their elapsed
work windows can differ. Preserve failed setup attempts and mixed latency results
when recording a run; see the final compiled-storage results for an example.


### Node inventory recovery, restart bursts, and operator diagnostics

`tools/benchmark_catalog_control.py` runs three metadata voters and three real
data processes with a synthetic 10,000-group reporter (one index per group):

```sh
uv run --project e2e/antfly python tools/benchmark_catalog_control.py \
  --samples 5 --output /tmp/catalog-control.json
```

It exercises three operational scenarios: replacing a node's complete inventory,
32/33/128-group durable status bursts following a restart, and four concurrent
operator diagnostic captures alongside a control read. The synthetic groups are
outside actual placement; this measures inventory and reporting pressure, not
10,000 hosted Raft groups. Use `--baseline-mode full --binary /path/to/old/antfly`
for the preceding implementation's ordinary full-report endpoint.

The recovery phase records planning time, total delivery time, each request's
latency and endpoint attempts, maximum request size, and root-activation latency.
The burst phase records each publication's endpoint discovery attempts. Those
latencies include discovery; snapshot observations never retry. Successful
control latency summaries exclude rejected responses, whose counts and raw
samples remain in the artifact. Diagnostic saturation is an expected scenario;
compare admission failures and their latency alongside successful captures.

Each burst size has one warmup and the requested measured samples. Initial
registration, catalog readiness, and fixture acquisition remain separate from
measurements. Partial checkpoints preserve progress if the workload aborts.
Every real data process must survive. Five local Debug samples diagnose gross
algorithmic regressions; they do not establish production throughput or p95 SLOs.


Compare `--baseline-mode chunked` and `--baseline-mode batched` on the same binary
and host to isolate batch admission from unrelated revisions. Both modes record
logical chunks, fragment counts, actual HTTP requests and rejected discovery attempts.
The default is batched. For index-heavy tenants, use `--groups 8
--indexes-per-group 1200`; each group then needs multiple durable frames. This measures
large-group transport and activation independently of actual shard placement.
The harness drives the protocol directly; it does not impose the production
worker's two-second quantum or retry backoff. Delivery comparisons therefore
measure metadata ingestion, not end-to-end production worker recovery time.

The production sender has a separate regression scenario, `system catalog baseline
worker keeps control scheduling live and cancels transport on shutdown` in the data
runtime test target. It stalls the real worker's HTTP executor, runs 2,000 full/heartbeat
scheduling attempts without recollection or another HTTP request, verifies an unrelated
maintenance job completes, and checks transport cancellation and worker release. This
is a controlled scheduling regression, not a claim about hosted-group throughput.

`system catalog initial report collection is isolated fenced and cancellable`
also stalls the capacity collector before any update exists. It checks 2,000
scheduling calls, rejection after a concurrent ownership change, a subsequent
successful publication, and cancellation during a new first collection.

The placement-annotation regression has an opt-in component benchmark. Compile
once, wait for competing builds to finish, then run the cached target:

```sh
zig build antfly-data-runtime-test -- 'system catalog placement annotation'
env ANTFLY_CATALOG_REPORT_BENCH=1 zig build antfly-data-runtime-test -- 'system catalog placement annotation'
```

It compares the former nested scan with production indexed annotation using
10,000 groups and 20,001 intents, including unrelated stores and duplicate local
intents. Both implementations must produce identical results. The timed indexed
path includes building and freeing its map; setup and equality checks are outside
the interval. One warmup precedes five samples. This isolates annotation CPU cost
and excludes collection I/O, network, and hosted Raft processing.


## Wide heartbeat inventories and concurrent schema migrations

```sh
ANTFLY_CATALOG_REPORT_BENCH=1 zig build antfly-system-catalog-report-bench
ANTFLY_CATALOG_REPORT_BENCH=1 zig build antfly-system-catalog-progress-test
```

Run these sequentially without competing builds or server workloads. The reporting
target includes unchanged and single-group-change heartbeats at 1,000 and 10,000
groups with 32 indexes per group. It compares full-runtime preparation with retained
immutable runtime leaves in the same binary, using one warmup and nine samples.
The progress target models 100 migrating tenant tables across 2,000 hosted groups:
it checks indexed readiness against the former nested scan and verifies that
replicated acknowledgements suppress unchanged progress while missing or stale
acknowledgements are resent. It uses one warmup and five samples. Both are component
benchmarks; see the recorded results for allocator choices and timing boundaries.

The real-workflow correctness companion uses existing documents and checks schema
rebuild/cutover, plus bounded batch acknowledgement through a three-metadata,
three-data-node cluster:

```sh
ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/antfly pytest \
  e2e/antfly/test_schema_migration.py \
  e2e/antfly/test_catalog_resilience.py
```


## Skewed tenant migrations under foreground traffic

Run the production workload with one large tenant and several small tenants, all
with three replicas, while issuing document lookups, searches and writes to migrating tables:

```sh
uv run --project e2e/antfly python tools/benchmark_schema_migrations.py \
  --binary zig-out/bin/antfly --baseline /path/to/previous/antfly \
  --large-docs 10000 --small-tenants 5 --warmups 1 --samples 3 \
  --output /tmp/schema-migration-comparison.json
```

Runs alternate revisions sequentially and create fresh clusters. Results retain
binary hashes, warmups, unsuccessful runs, each tenant's cutover time, and raw
lookup/search/write latencies. Setup is excluded from workload timing. Traffic is closed
loop: assess completion times, request counts and latency distributions together;
these results do not establish a fixed-load production SLO. The E2E checks every
acknowledged foreground write after cutover. Storage regression tests separately
force page yields, reopen the DB and verify replay of updates/inserts/deletes on
both sides of the saved source cursor.

Use `--traffic-clients 4` to run four independent closed-loop clients against the
same cluster. Each client performs lookups, searches and uniquely keyed writes
against the large and last small tenant. Compare this separately from the default
single-client workload. Both binaries are staged under the canonical `antfly`
filename so fixture routing is identical even when the baseline has been renamed.

The completed-prefix scheduling component can also run without a server:

```sh
ANTFLY_CATALOG_REPORT_BENCH=1 zig test -lc pkg/antfly/src/data/schema_repair_schedule.zig
```

This compares the former repeated-prefix selection with the actual queue at 1,000
and 10,000 groups. It includes scheduling allocation and destruction, uses one
warmup and five samples, and verifies the number of owner inspections. Physical
owner calls and the former one-second scheduling interval are excluded; the CPU
comparison must not be presented as an end-to-end migration speedup.

For diagnosing concurrent lookup tails, `--diagnostics-dir /private/tmp/catalog-read-logs`
retains the catalog cluster's logs and writes its live endpoint/root to `server.json`.
The concurrent lookup result includes each client's request sequence, start offset,
and duration, so correlated stalls can be distinguished from steady per-request
cost. Server profiling should be recorded as an instrumented run, separately from
unprofiled timing comparisons.

Use `--catalog-ingress nonmember` with the catalog cluster scenario to choose and
record a coordinator that has no placement for the target table. This makes remote
endpoint discovery an explicit part of the workload instead of depending on where
the last-created shard happens to land. Provisioning and membership observation
remain outside the measured operations.

For sustained routing-cache qualification, add `--lookup-seconds 30` (or longer)
to the cluster catalog workload with `--catalog-ingress nonmember`. The concurrent
phase continues until both the minimum duration and per-client `--samples` count
are satisfied. This covers repeated one-second peer-view refreshes. Run the same
configuration against both binaries, retain per-request timing records, and keep
cluster provisioning outside measured work. Short sample-count-only runs do not
qualify sustained refresh tails.

To measure distributed query and join fanout, use `--catalog-shards 8` with
`--table-counts 10 --deployment cluster`. Keep `--catalog-ingress first` for this
case: shards may occupy every data node, so an entirely nonmember coordinator
may not exist. The qualified query and join scenarios validate the returned
results while exercising local and remote shard destinations.

The component workload `system catalog peer publication avoids schema copies
workload` can be run with `ANTFLY_CATALOG_PEER_REFRESH_BENCH=1`. It compares owned
snapshot results with retained peer results at publication, using 1,000 tables
with 8 KiB schema payloads. Incoming snapshot construction is excluded from both
measurements. This isolates publication/result-copy cost; it is not an end-to-end
network or throughput benchmark.

### Join planning acquisition and shard fanout

The retained-planning workload compares warm diagnostic-snapshot acquisition and
release with warm planning-generation acquisition, indexed table/key lookup, and
release. It uses the same Debug binary and checking allocator for both paths.
The opt-in size is 1,000 tables with one range each and 8 KiB schema definitions;
normal test runs use 16 tables. Initial control-generation publication is reported
separately, outside the warm samples.

```sh
ANTFLY_CATALOG_JOIN_PLANNING_BENCH=1 python3 tools/run_bounded_zig_build.py \
  build antfly-data-runtime-test -- \
  --test-filter 'system catalog join planning retained acquisition workload'
```

For the complete read path, run the catalog workload on fresh three-data-node
clusters, with eight single-replica shards per table and a coordinator that must
forward to remote right-hand shards. The harness verifies and records those
placements. It validates writes, lookups, search, joins, NDJSON, discovery, and
rename. Sequential join and NDJSON samples cross cache freshness intervals; this
is separate from the deliberately warm component measurement.

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --binary /path/to/antfly --scenario catalog --deployment cluster \
  --table-counts 10 --catalog-shards 8 \
  --schema-fields 32 --samples 20 --warmup 2 --ndjson-lines 20 \
  --output /tmp/catalog-join-fanout.json
```

Run baseline and candidate sequentially without task-owned compilation or other
workloads. Compare result correctness, p50/p95/max, and errors as well as warm
acquisition cost. A component speedup does not establish an HTTP tail-latency SLO.
