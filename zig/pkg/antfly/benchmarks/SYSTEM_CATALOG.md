# System catalog benchmarks

Run these from `zig/`. Results describe a particular binary, machine, and workload;
they are not CI latency thresholds or production capacity claims.
See [recorded measurements](SYSTEM_CATALOG_RESULTS.md) for a reproducible local
before/after comparison and representative request latencies.

## Catalog scale in process

```sh
zig build antfly-system-catalog-bench
```

This target builds its own ReleaseFast executable. It reports five-sample medians
for indexed versus scanned name lookup and table-rename planning at 1,000,
10,000, and 100,000 tables. Tenant offboarding measures planning and applying a
database drop with 1,000 or 10,000 empty namespaces while retaining another
database's tables. Fixture construction is outside the timed region. Rename
includes construction of the planner's indexes; lookup reuses an owned index.

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
a known leader on every metadata node before starting the next table. The output
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

Useful controls include `--table-counts`, `--mentions`, `--documents`, `--samples`,
`--warmup`, `--concurrency`, and `--ndjson-lines`. A quick harness check can use
`--table-counts 3 10 --mentions 4 10 --documents 2 --samples 3 --warmup 1`.
Requests and derived-readiness polling are bounded; failures abort the scenario
instead of becoming successful latency samples. These workloads measure the
catalog integration and resolver/graph path, not vector-search quality, inference
throughput, large-document indexing, or multi-machine network capacity.
