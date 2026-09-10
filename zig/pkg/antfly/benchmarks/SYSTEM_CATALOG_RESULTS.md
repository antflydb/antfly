# System catalog measurements — 2026-09-10

Local macOS 26.3.1 ARM64 development builds (`zig build antfly`, Zig 0.16.0).
Resolution and distributed catalog runs used disposable three-metadata/three-data-node
clusters on one host; the standalone workload used one server.
The two resolution runs ran sequentially after builds and tests completed.
They are workload observations, not production capacity claims.

## Scored entity-resolution comparison

Both builds include `origin/main` at `8211fc92c`. The baseline is `55a6af895`,
before bulk candidate reads; the updated build is `5b1eed417`.
Both used the same explicit exact-name scorer, three document shards, one entity
shard, half existing and half new entities, two warmup documents, five measured
documents per size, and 20 ms readiness polling. Document keys alternate across
the three initial key ranges. Each document uses fresh entity keys.

The interval starts before the source write and ends when all entity documents
are visible through graph hydration. It includes resolution, atomic promotion,
graph publication, query latency, and polling.

| Mentions/document | Before p50 / p95 | After p50 / p95 | Median improvement |
| --- | --- | --- | --- |
| 10 | 1.682 s / 1.873 s | 0.490 s / 0.566 s | 3.4× |
| 100 | 12.835 s / 13.004 s | 0.498 s / 0.811 s | 25.8× |

Bulk exact-ID queries cap each request at 256 keys and retain the work unit’s
pinned physical destination. Candidate redirects are cached within that work
unit. Deterministic configurations without a scorer also skip unused candidate
and embedding calls; the measured comparison enables scoring on both builds.

Binary SHA-256 values:

- Before: `5527b023e590b5031431e4a3ee6e75c32f4255df717d91ee271feda84c7da7c3`
- After: `b385d8c71ca481268a4b92705e7173c3e46f7de2b53c20b64797a7cb7e215e9f`

## Steady entity-graph reads

Thirty requests after two warmups using the updated resolution binary. The
source document has already completed resolution and promotion.

| Operation | 10 mentions p50 / p95 | 100 mentions p50 / p95 |
| --- | --- | --- |
| Graph topology | 218 / 265 ms | 213 / 260 ms |
| Graph with hydrated documents | 260 / 312 ms | 278 / 347 ms |

## Standalone application operations

Thirty measured requests after two warmups at each catalog size. NDJSON latency
is for 20 queries in one HTTP request. The join enriches a row from a separate
table. Concurrent lookup uses eight independent sessions and 30 requests per
session. Listing returns every table in the measured namespace.

| Operation | 10 tables p50 / p95 | 100 tables p50 / p95 |
| --- | --- | --- |
| Qualified document lookup | 0.41 / 0.52 ms | 0.64 / 0.77 ms |
| Qualified query | 0.61 / 0.77 ms | 1.12 / 1.30 ms |
| Qualified join | 1.19 / 1.43 ms | 2.47 / 2.71 ms |
| NDJSON (20 queries) | 5.60 / 6.16 ms | 14.67 / 15.53 ms |
| Scoped listing | 1.94 / 2.12 ms | 14.23 / 15.68 ms |
| Table rename | 0.97 / 1.12 ms | 4.04 / 4.31 ms |
| Concurrent lookup | 1.97 / 3.26 ms | 1.50 / 7.08 ms |

Concurrent lookup throughput: 3791 requests/s at 10 tables and 3144 requests/s at 100 tables.

Server revision: `928571880`; binary SHA-256: `e35e00a0e903ab0e889f42bd34a7ac4430995ab67a932b49411ada49585a53f2`.

## Distributed application operations

The same 30-request workloads on the three-data-node cluster, using the
`928571880` server plus the harness's explicit shard-readiness setup. Each new
shard must report a known leader and healthy voter on every metadata node before
provisioning the next table. Setup waits are excluded from request latency.
No measured errors or ambiguous writes are retried by the harness.

| Operation | 10 tables p50 / p95 | 100 tables p50 / p95 |
| --- | --- | --- |
| Qualified document lookup | 27.1 / 54.6 ms | 29.9 / 368.3 ms |
| Qualified query | 49.8 / 57.2 ms | 363.1 / 993.5 ms |
| Qualified join | 53.3 / 77.5 ms | 189.0 / 1070.0 ms |
| NDJSON (20 queries) | 525.9 / 551.6 ms | 4749.3 / 7088.3 ms |
| Scoped listing | 28.2 / 44.5 ms | 116.2 / 901.8 ms |
| Table rename | 47.8 / 67.8 ms | 56.3 / 531.6 ms |
| Concurrent lookup | 52.2 / 76.2 ms | 126.1 / 236.2 ms |

Concurrent lookup throughput: 144 requests/s at 10 tables and 49 requests/s at
100 tables. The 100-table development cluster has substantial tail latency;
these figures include Raft, metadata reads, routing, and query execution. They
are not isolated catalog-lookup costs or evidence of production capacity.

The initial run exposed a read-only metadata failover gap. Catalog reads now
retry across elections under one absolute deadline and pin endpoint order per
pass. Regression tests cover changing affinity, generation conflicts, deadlines,
and cancellation. A separate setup run exposed an ambiguous seed write during
shard bootstrap; the harness now observes readiness before issuing the seed.

Binary SHA-256: `e35e00a0e903ab0e889f42bd34a7ac4430995ab67a932b49411ada49585a53f2`.

## Isolated catalog scale

ReleaseFast microbenchmark, five-sample medians, run after the live cluster
stopped. Lookup reports nanoseconds per key; each sample performs 1,000 lookups.
Rename includes building the planner's indexes.

| Tables | Scanned lookup | Indexed lookup | Rename planning |
| --- | --- | --- | --- |
| 1,000 | 750.8 ns | 29.6 ns | 0.094 ms |
| 10,000 | 7,430.7 ns | 27.8 ns | 0.647 ms |
| 100,000 | 61,732.3 ns | 126.6 ns | 8.348 ms |

Tenant offboarding (drop planning and apply) took 0.804 ms for 1,000 empty
namespaces with 1,000 unrelated tables, and 7.805 ms for 10,000 namespaces with
10,000 unrelated tables. This exercises the removal of nested scans during
large database drops.

## Reproduction

See [workloads and commands](SYSTEM_CATALOG.md). Run the resolution scenario
with `--binary` pointing to each separately built revision, using otherwise
identical arguments. Keep setup and warmup outside measured intervals and run
builds, tests, and other benchmark scenarios separately. Raw JSON output retains
all settings, binary hash, percentiles, startup time, and readiness poll counts.
