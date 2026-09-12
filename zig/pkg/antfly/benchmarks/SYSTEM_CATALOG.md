# System catalog benchmarks

Run these from `zig/`. Results describe a particular binary, machine, and workload;
they are not CI latency thresholds or production capacity claims.
See [recorded measurements](SYSTEM_CATALOG_RESULTS.md) for a reproducible local
before/after comparison and representative request latencies.

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
