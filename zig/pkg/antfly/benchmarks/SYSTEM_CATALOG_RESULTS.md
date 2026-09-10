# System catalog measurements — 2026-09-10

Local macOS 26.3.1 ARM64 development builds (`zig build antfly`, Zig 0.16.0).
Resolution and distributed catalog runs used disposable three-metadata/three-data-node
clusters on one host; the standalone workload used one server.
The original exact-key comparison ran sequentially after builds and tests completed.
The follow-up section explicitly records concurrent compiler activity.
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

## Request projections and batched candidates: follow-up

Production sources: baseline `7d977f090`, updated `268b1ceb3`. Both are macOS
development builds. These are **provisional shared-host measurements**: unrelated
compiler processes were active at run boundaries. Use the results as workload
observations alongside the deterministic RPC-count and allocation regressions,
not as isolated capacity estimates. Complete settings, percentiles, hashes, and
host-load flags are in [the machine-readable results](system_catalog_workloads_2026_09_10.json).

The entity cases use three metadata and three data nodes, three document shards,
two warmup documents and five measured documents per size, plus 30 steady graph
queries per mode. Prefix documents repeat ten entity names; redirect documents
seed a distinct alias and curated survivor for every mention.

| Workload | Mentions | Before p50 / p95 | After p50 / p95 | Observed median ratio |
| --- | --- | --- | --- | --- |
| Prefix | 10 | 1.001 / 1.070 s | 0.572 / 0.629 s | 1.8× |
| Prefix | 100 | 5.870 / 5.903 s | 0.580 / 0.613 s | 10.1× |
| Redirects | 10 | 0.914 / 1.073 s | 0.461 / 0.590 s | 2.0× |
| Redirects | 100 | 5.969 / 6.415 s | 0.650 / 0.858 s | 9.2× |

Steady graph-read latencies remain in the same broad range; the large observed
gain is in completing resolution and promotion. Exact keys and prefixes are
deduplicated per work unit, redirects use a second bulk read, missing targets
are cached, and immutable candidate records are decoded once per distinct lookup.

The wide-schema workload declares a searchable body plus 200 additional string
fields per table. It uses 30 requests after two warmups; NDJSON contains 20
queries in one HTTP batch.

| Tables | Operation | Before p50 / p95 | After p50 / p95 |
| --- | --- | --- | --- |
| 10 | Query | 1.05 / 1.19 ms | 1.14 / 1.27 ms |
| 10 | Join | 2.06 / 2.21 ms | 2.21 / 2.62 ms |
| 10 | NDJSON ×20 | 14.27 / 14.80 ms | 13.30 / 13.80 ms |
| 100 | Query | 2.47 / 2.61 ms | 2.08 / 2.34 ms |
| 100 | Join | 5.88 / 6.91 ms | 5.06 / 5.50 ms |
| 100 | NDJSON ×20 | 40.59 / 42.22 ms | 31.43 / 33.20 ms |

Distributed catalog operations use the ordinary schema, one replica per shard
through an inherited tablespace policy, and ten requests after two warmups.
Shard-bootstrap waits are outside these intervals. Only the 10-table updated
run completed; the 100-table run returned HTTP 503
`storage_read_temporarily_unavailable` during NDJSON measurement. There is no
successful updated 100-table comparison. The baseline's completed 100-table
results are retained in the machine-readable artifact.

| Tables | Operation | Before p50 / p95 | After p50 / p95 |
| --- | --- | --- | --- |
| 10 | Lookup | 48.4 / 56.3 ms | 28.2 / 52.5 ms |
| 10 | Query | 53.0 / 59.2 ms | 51.8 / 61.1 ms |
| 10 | Join | 57.7 / 78.5 ms | 80.3 / 112.2 ms |
| 10 | NDJSON ×20 | 542.1 / 556.8 ms | 672.1 / 833.3 ms |

The distributed join and NDJSON medians regressed in those runs; host contention
prevents attributing that difference to the change. That revision still performed
an internal shard definition read because its wire request lacked prepared index
selection. The indexed-management and prepared-routing follow-up below implements
a versioned internal envelope with matching identity/fence validation and a legacy
fallback, then measures the distributed workload again.

One updated redirect run and one baseline rerun timed out while a document shard
held the source document but its graph remained empty. Those failures are not
successful latency samples. Explicit shard readiness was added after the first
timeout, but did not eliminate the baseline reproduction. The successful updated
run includes that extra untimed setup; the completed baseline predates it.
This intermittent readiness/replay problem remains a limitation of the live
scenario and should be investigated independently of the candidate batching
speedup. Together with the 100-table storage-read failure, this means the report
does not establish reliability under load.

Independent regression checks verify one prefix scan for 100 repeated mentions,
one bulk read for their shared missing redirect, identity retention across retries
and joins, one reused NDJSON definition with administrative snapshots disabled,
and legacy/missing lookups within a 4 KiB allocator with 1,000 unrelated databases.

## Indexed management, prepared routing, and owning-shard reads

Production baseline `c4971e9d5`; updated production sources `ddfb52448`. Both
include `origin/main` at `8211fc92c`. These are sequential Debug-build runs on
one shared macOS ARM64 host, with no own build or test workloads running during
measurement. They are not isolated capacity measurements. Complete settings,
binary hashes, all catalog sizes, steady graph reads, intermediate results, and
failed-run diagnostics are in [the machine-readable results](system_catalog_indexed_workloads_2026_09_10.json).
A subsequent ownership fix retains legacy standalone table names through mutation
publication; it does not change the measured distributed paths.

The implementation uses transaction-backed point reads and covering parent
indexes for management, reverse references for DDL dependencies, a versioned
prepared-query header checked against the catalog fence, and bounded candidate
batches partitioned by owning shard. Derived rows are written atomically and
rebuilt from primary records after reopen/snapshot installation.

### Multi-tenant management

Three metadata and three data nodes; 10, 100, and 1,000 tenant databases, each
with its default namespace. Ten samples after two warmups per sequential
operation. Four concurrent clients perform 20 reads and 20 namespace create/drop
cycles in total. Create/drop and rename timings are complete round trips; rename
includes a GET that verifies identity. Database provisioning is reported separately.

| Operation at 1,000 tenants | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| Named GET | 47.5 / 57.0 | 24.8 / 26.9 |
| List all databases | 52.4 / 56.9 | 28.2 / 44.3 |
| Namespace create/drop | 264.7 / 294.3 | 131.1 / 183.9 |
| Rename round trip | 264.3 / 311.9 | 129.4 / 167.3 |
| Concurrent named GET | 62.7 / 150.8 | 26.3 / 58.2 |
| Concurrent namespace create/drop | 421.9 / 630.5 | 184.2 / 258.9 |

An intermediate implementation fetched each listed record through a separate
primary seek: its 1,000-tenant list median regressed to 146.1 ms. The final
covering parent index reduced that to 28.2 ms. The intermediate run is retained
in the artifact, not used as the baseline. Small-catalog medians were mixed:
for example, 10-tenant named GET rose from 13.9 to 26.8 ms. The results support
better scale behavior, not a universal improvement at every size.

### Distributed application operations

Ten scoped tables; 20 samples after two warmups. NDJSON contains 20 queries;
concurrent lookup uses four clients with 20 requests each. All before/after
operations completed. The earlier 100-table failure above was not rerun in this
comparison, so these results do not establish reliability at that scale.

| Operation | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| Qualified lookup | 26.7 / 48.0 | 25.3 / 33.5 |
| Qualified query | 51.8 / 75.1 | 26.2 / 45.9 |
| Qualified join | 82.8 / 103.3 | 65.2 / 79.1 |
| NDJSON ×20 | 787.1 / 945.4 | 540.8 / 559.0 |
| Scoped table listing | 26.3 / 72.0 | 25.2 / 32.9 |
| Concurrent lookup | 26.6 / 31.8 | 38.7 / 54.8 |
| Table rename | 38.9 / 58.9 | 30.0 / 50.2 |

### Entity resolution across eight shards

Three document shards and eight entity shards. Redirect workloads seed a unique
alias and curated survivor for every mention. Each size uses two warmup documents
and five measured documents, followed by ten steady graph reads per mode. The
interval runs from source write to graph hydration and includes polling. Clustered
keys share an owner; spread keys use hexadecimal prefixes across the initial
ranges. Benchmark redirects keep each survivor near its alias. A separate E2E
regression covers redirects crossing owners and 100 repeated mentions.

| Key layout | Mentions | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- | --- |
| Clustered | 10 | 1115.2 / 1325.5 | 955.4 / 1006.2 |
| Clustered | 100 | 1595.2 / 1621.6 | 1232.4 / 1400.7 |
| Spread | 10 | 1527.0 / 1667.9 | 1413.3 / 1486.2 |
| Spread | 100 | 2403.8 / 2966.1 | 1289.1 / 1626.0 |

Two baseline attempts stopped during setup because the create acknowledgement
had no table projection yet. The harness now observes visibility with GET before
waiting for shard readiness, without replaying the create. A subsequent spread
baseline failed an entity seed write with HTTP 503 `write unavailable`; a fresh
cluster run completed. Those failures are recorded separately and are not latency
samples. No measured requests or ambiguous writes were retried. Both updated
layouts completed. This is not evidence that the previously observed intermittent
empty-graph or storage-read failures are resolved.

### Isolated algorithm scale

ReleaseFast microbenchmarks compare related-label lookup by repeated scans versus
an indexed projection, and rebuilding a planner index for each rename versus a
retained reader. At 1,000/10,000 tenants, listing projection took 0.992/145.478 ms
with scans and 0.009/0.082 ms with indexes. Rename planning with a rebuilt index
took 2.346/34.137 ms, versus 3.881/7.559 microseconds with the retained reader.
These are algorithm comparisons within the new harness, not measured DDL timings
from the previous server binary. Distributed DDL still includes Raft; standalone
publication still clones and checkpoints the complete catalog.

## Reproduction

See [workloads and commands](SYSTEM_CATALOG.md). Run the resolution scenario
with `--binary` pointing to each separately built revision, using otherwise
identical arguments. Keep setup and warmup outside measured intervals and run
builds, tests, and other benchmark scenarios separately. Raw JSON output retains
all settings, binary hash, percentiles, startup time, and readiness poll counts.
