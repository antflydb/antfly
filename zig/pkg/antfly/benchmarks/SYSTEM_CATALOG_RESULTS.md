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
publication; it does not change the measured distributed paths. The subsequent
merge of main at `1d6e3ac69` includes runtime/scheduler/inference changes. These
measurements predate that merge and are not measurements of its final binary.

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

## Retained routing generations and standalone row transactions (2026-09-10)

This comparison starts at `0182d591b`, after the previous round's final main
merge. Both baseline and updated revisions contain main `1d6e3ac69`. The local
baseline executable was preserved before editing; raw results retain its hash.
Routing/partitioning measurements use `d0fc594ee`. Final standalone measurements
use `bbbde23cb`, which additionally removes redundant fsyncs after fully durable
LSM commits. These are sequential Debug-server runs on the same shared host;
this task ran no compiler, test suite, or second benchmark during measurement.
They are small-sample observations, not production capacity guarantees.

Raw settings, binary hashes, all measurements, the intermediate diagnostic run,
and microbenchmark output are in
[the generation/transaction workload artifact](system_catalog_generation_workloads_2026_09_10.json).

### Tenant provisioning and DDL alongside readers

Standalone; 10, 100, then 1,000 tenants. The paired runs use 10 samples after two
warmups, four concurrent clients, and no restart between checkpoints. Point
operations target one tenant; create/drop and rename timings cover the complete
round trip. The concurrent scenario runs two readers alongside two DDL clients.

| Operation, 1,000 tenants | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| Named database GET | 0.426 / 0.642 | 0.352 / 0.370 |
| List databases | 9.281 / 9.535 | 8.686 / 9.226 |
| Namespace create/drop | 70.572 / 71.047 | 1.037 / 2.293 |
| Rename round trip | 70.585 / 71.384 | 1.364 / 1.440 |
| Concurrent reads | 35.807 / 175.851 | 0.976 / 2.934 |
| Concurrent namespace create/drop | 121.214 / 207.876 | 2.059 / 3.446 |

The rename median is about 52× lower, and namespace create/drop about 68× lower.
Final rename medians at 10/100/1,000 tenants are 1.416/1.305/1.364 ms: unrelated
logical inventory no longer drives mutation cost. Small point-read timings remain
mixed: at 10 tenants, named GET rose from 0.395 to 0.550 ms. Whole listings still
perform work proportional to their output.

An intermediate run exposed duplicate local WAL/index syncs after an already
fully durable commit. Its 1,000-tenant rename median was 4.201 ms. The final local
path relies on the LSM's synchronous WAL commit; borrowed stores, including Lite,
retain explicit sync. Recovery tests reopen without a graceful backend flush.
The intermediate run also restarted at each checkpoint, so it is retained as a
diagnostic, not substituted into the paired table above.

A separate final 1,000-tenant run performs mixed DDL and then restarts the server.
It verified all 1,001 database names/IDs and absence of deleted namespaces in
995.7 ms, including restart, readiness, listing, and validation. Recovery is
reported separately from steady request latency. This is graceful process
restart timing; the unit suite separately tests WAL recovery without a graceful
flush, failed publication rollback, ambiguous sync fencing, and legacy local/Lite
migration.

### Applications with wide schemas

Standalone; 10 then 100 tables, each with 200 extra schema fields. Ten samples,
two warmups, four concurrent readers, and 20 lines per NDJSON request. Fixture
creation/readiness is excluded. Both before/after runs completed successfully.

| Operation, 100 tables | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| Qualified document lookup | 1.450 / 1.606 | 0.481 / 0.624 |
| Qualified query | 2.479 / 2.589 | 1.075 / 1.338 |
| Qualified join | 5.773 / 6.088 | 3.268 / 5.154 |
| NDJSON ×20, repeated target | 34.980 / 35.629 | 12.185 / 13.007 |
| Concurrent document lookup | 2.986 / 6.515 | 1.116 / 1.459 |
| Scoped table listing | 535.201 / 537.323 | 509.441 / 538.604 |
| Table rename | 11.488 / 11.744 | 0.581 / 0.633 |

Routing reuse improves the selected-table read path. Scoped listings still
materialize schemas/status for every returned table, and their p95 did not
improve. This standalone success does not resolve the earlier clustered
100-table storage-read or intermittent empty-graph failures documented above.

### Distributed entity resolution

Three metadata and three data nodes, three document shards, eight entity shards,
spread keys, and curated redirects. Two warmup documents and five measured
documents per size; ten steady graph reads per mode. The write-to-graph interval
includes background resolution, publication, hydration, and polling.

| Mentions per document | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| 10 | 1674.170 / 2032.154 | 1424.014 / 1628.979 |
| 100 | 1757.840 / 1949.177 | 1476.476 / 1737.838 |

Both sizes completed. These medians are about 15–16% lower; steady graph-read
results were mixed and are retained in the artifact. A deterministic transport
regression independently forces parallel hosted fanout and verifies each shard's
wire body contains only its owning keys. For 100 keys spread across eight shards,
that changes 800 transmitted key entries to 100; it is not an eightfold latency
claim. The existing multi-node E2E covers redirects crossing shard owners.

### Isolated routing and mutation costs

Five-sample ReleaseFast medians; fixture/index construction is outside warm
measurements. Routing uses `c_allocator`, 100 requests per sample, one range per
table, and three keys in the target table. The rebuilt path clones compact rows
and rebuilds indexes for each request; the retained path acquires/releases the
published generation and performs the same key routing.

| Catalog size | Rebuilt routing request (µs) | Retained routing request (µs) |
| --- | --- | --- |
| 10 tables | 2.036 | 0.123 |
| 1,000 tables | 126.337 | 0.109 |
| 10,000 tables | 1305.845 | 0.094 |

The mutation harness uses its existing `page_allocator`. At 10,000 tenants,
copying the logical state and rebuilding indexes for a rename took 7276.250 µs;
applying and undoing an affected-record delta took 8.500 µs. These measurements
isolate allocation/index work. They exclude metadata RPCs, Raft, fsync, storage
reads, and HTTP serialization; they must not be presented as server speedups.

Validation of this implementation: 430 focused query/join/routing/sort tests
passed with zero leaks; 83 catalog API/standalone tests, 23 metadata durability
and transport tests, six remote-routing/cache tests, and all 31 selected E2E
cases passed. The standalone follow-up also passed all 42 tests after removing
redundant syncs. The full Antfly build, Python lint/formatting, and Zig formatting
passed. These counts describe overlapping focused targets, not a summed total
or a claim that every repository test was run.

## Coherent listings and retained schema memory (2026-09-10)

[Raw observations](system_catalog_listing_workloads_2026_09_10.json) compare
`69fe2d21b` with the final source committed as `e3c5f83a4`. The after binary was
built from that working tree before its source commit; the artifact records its
SHA-256. The after source includes main `444440574`, including its build refactor.
This is an end-to-end comparison, not an isolated attribution of main's changes.

Sequential Debug standalone runs on the same shared macOS host, with 200 extra
schema fields, 10 samples and two warmups. No other agent-started builds, tests,
or benchmarks overlapped measured requests. Table creation/readiness is excluded.
Distinct schemas add one unique declared field per table.

| Public listing workload | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| One table beside 100 unrelated tables | 7.624 / 7.844 | 1.789 / 1.939 |
| Prefix selecting one of 100 tables | 7.570 / 7.734 | 1.750 / 1.837 |
| 100 tables, shared schema | 509.523 / 533.364 | 93.832 / 96.772 |
| 100 tables, distinct schemas | 508.313 / 535.352 | 93.990 / 95.001 |

The 100-table medians improve about 5.4× in both schema workloads. Scoped reads
avoid unrelated definitions, and the API groups ranges once per response.
Only completed immutable schema projections enter the bounded cache. Index
incarnations, permissions, runtime field observations, and counters remain fresh.
The distinct-schema workload exposed retention of parser/aggregation scratch;
compacting the owned projection reduced one 200-field fixture from 825,584 to
460,156 retained bytes. An allocation-budget regression caps that fixture at
512 KiB, alongside eviction/lifetime and all-allocation-failure tests.

The cache retains at most 256 entries and 64 MiB; active response leases and
in-flight compiler scratch can temporarily consume additional memory. Larger
working sets can evict entries. Response serialization still scales with the
selected inventory and schema width. These small-sample Debug results are not
production latency promises or evidence about clustered listing throughput.

The checked-in component benchmark also reports tenant create/drop churn. After
10, 1,000, and 10,000 cycles, the fixed implementation retains two live resources,
two parent buckets, and 784 child-array bytes. The baseline retained 10,002 parent
buckets and 4,480,784 child-array bytes after 10,000 cycles. These byte counts
exclude hash-table capacity, rows, allocator overhead, and process RSS.

Correctness coverage includes concurrent private-table drop/listing, a projection
that rejects any attempt to join independent admin/binding snapshots, and scoped
metadata allocation budgets. Portable HA topology v4 preserves the logical
catalog and extension inventory alongside physical topology; v3 remains readable.
Tests cover restored names/IDs/tablespaces/next ID, invalid logical references,
literal and long restore names, and materialization publication crashes.

Merged validation: 118 catalog tests, six HA materialization/activation tests,
and two data-runtime seed-capture tests passed. All 32 selected catalog,
resolution, schema-migration, and exact-sort E2E tests passed. After the final
schema compaction, the 46-test catalog API suite and all 17 affected catalog,
schema-migration, and exact-sort E2E tests passed again. Full builds, both
relocated benchmark targets, Python lint/formatting, and Zig formatting passed.
Counts overlap; these are focused suites rather than the complete repository.

## Bounded inventory and shared schema compilation (2026-09-11)

[Raw observations and executable provenance](system_catalog_capacity_workloads_2026_09_11.json)
compare the preceding PR head `a05523e6b` with the implementations recorded per
run. Standalone capacity and pagination use `0ef9fe841`; the subsequent
metadata-only batching change is `1ff6ad950`. Both include main `c3bc00135`.
Runs used Debug binaries on the same shared macOS ARM64 host. No task-started
build, test, or other benchmark overlapped measured requests; unrelated host
activity was outside our control. Provisioning and readiness are excluded.
These are small-sample workload observations, not production capacity estimates.

### Wide inventory while applications read table details

Five measured inventories after two warmups, with 200 extra schema fields and a
unique declared field per table. The 200-table case exceeds the schema cache's
64 MiB retention budget. The mixed workload uses one inventory scanner and eight
detail readers; readers remain active until the scanner finishes.

| Workload | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| 100-table full inventory | 96.601 / 97.741 | 102.948 / 104.606 |
| 200-table full inventory | 1120.286 / 1132.184 | 496.549 / 498.701 |
| Detail beside 200 tables | 11.758 / 12.037 | 4.931 / 5.065 |
| 200-table inventory alongside readers | 1217.340 / 1221.480 | 558.464 / 585.840 |
| Detail alongside 200-table inventory | 12.360 / 13.460 | 6.057 / 8.275 |

At 200 tables, full inventory improved about 2.26× and concurrent detail
throughput rose from 614 to 1143 requests/s. The 100-table full inventory median
regressed 6.6%; this change does not make every cache-resident scan faster.
Frequency/size admission prevents sequential scans from replacing equally useful
resident definitions. Concurrent misses share compilation, and point details use
the same immutable cache. Runtime observations, permissions, and index
incarnations remain fresh. Nonresident definitions still require compilation;
active response leases and compiler scratch are outside the retention budget.

A separate 200-table run measured the first 25-row page at 26.291 / 26.376 ms
(p50 / p95). The validated complete cursor walk took 473.240 / 488.068 ms,
compared with 490.992 / 497.898 ms for the unpaged inventory in that run.
Pagination bounds each response; it does not eliminate full-inventory work.
The harness checks scope, counts, unique names, complete traversal, ordering, and
cursor progress. Optional pagination keeps the baseline comparison usable with
older binaries that do not implement cursors.

### Single-table details as unrelated inventory grows

Five requests after two warmups, shared 200-field schemas, standalone. The final
binary is `1ff6ad950`; this isolates the application workflow rather than a
synthetic schema-compilation loop.

| Unrelated tables | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| 1 | 11.331 / 11.921 | 4.083 / 4.262 |
| 100 | 11.131 / 11.372 | 4.657 / 4.677 |
| 1000 | 16.340 / 16.498 | 4.808 / 4.979 |

At 1,000 unrelated tables, the detail median improved about 3.40×. The final
point projection avoids copying the full catalog and shares immutable schema
compilation with inventory reads.

### Clustered projections and profiling

Twenty measured requests after five warmups, on three metadata and three data
nodes on one host. The final batched implementation is `1ff6ad950`.

| Workload | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| One table beside 30 tables | 27.232 / 190.852 | 24.609 / 43.253 |
| Prefix selecting one of 30 tables | 28.794 / 217.020 | 23.160 / 40.618 |
| 30-table full inventory | 78.485 / 277.536 | 103.602 / 485.014 |
| Detail beside 30 tables | 27.240 / 221.178 | 28.713 / 320.416 |
| Empty default namespace beside 30 tables | 26.139 / 235.148 | 22.882 / 51.666 |

No clustered full-inventory improvement is established. The matched 30-table
inventory median regressed 32%, and detail tail latency was worse in the final
run. Earlier five-sample baseline inventory measured 110 ms; repeated updated
runs measured 101–109 ms. At ten tables, empty-default-namespace latency rose
from 12.644 to 25.603 ms, while selective-prefix medians were similar. These
differences and broad tails remain visible in the artifact; local quorum timing,
metadata heartbeat/index maintenance, storage work, and host contention are all
included. Selected-key batching reduces repeated LSM work, but does not remove
the read barrier, derived-index write amplification, or full-response serialization.

The initial implementation encoded derived heartbeat reports as JSON and resolved
logical names before a separate status read. Its 30-table inventory/detail
medians were 258.137 / 55.855 ms. Those observations prompted two corrections:
derived rows reuse the primary binary codec, and HTTP/MCP status resolves names
and projects status behind one read barrier. Repeated full-inventory profiling
then identified selected report and definition point reads as avoidable work.
The final projection sorts selected keys and batches their storage reads, retaining
logical result order and excluding unrelated payloads.

A ten-second sample of the serving metadata node attributed 318 of 955 sampled
projection stacks to report point reads and 155 to definition point reads. These
are call-site stack observations, not end-to-end CPU percentages. The separate
unprofiled 100-request inventory run measured 101.449 / 353.457 ms before batching.
The artifact retains intermediate regressions and the profiling summary rather
than replacing them with successful results.

### Correctness and compatibility

The final batched implementation passed the full build and all 134 focused
catalog tests. The metadata regression stays within a 128 KiB caller allocator
with a 256 KiB unrelated definition/report and 2,000 group summaries; it covers
missing selected reports, lexical versus numeric key order, cursor continuation,
heartbeat removal, membership invalidation, and derived-index rebuild. Other tests
cover single compilation under concurrent cold reads, cache admission/eviction
leases, allocation failure cleanup, cursor validation, CORS, and one coherent
HTTP/MCP detail observation.

The 19 selected catalog, schema-migration, exact-sort, and exact-star grant E2E
cases passed on the fused-detail implementation. The subsequent batching change
is confined to metadata reads and covered by the focused storage regression and
clustered public-API workload. Generated OpenAPI checks, Go SDK pagination tests,
Python SDK generation checks, TypeScript SDK typechecking, and changed-file
formatting/lint passed. The global license scan still reports inherited failures;
new files were checked individually. These are focused, overlapping checks, not
a claim that the complete repository suite passed.

## Report storage, bounded standalone captures, and detail encoding — 2026-09-11

This round compares `f393d9cda` with normalized report storage at `c4fe1e215`
and the final standalone cache/ordered-range capture at `13829ab83`. The latter
binary SHA-256 is `0c488a127056aed42d5cf2199859769f407b8d05318e5a95c780b601da1be960`;
the baseline is `da72be79c6565d2cf8772595502a04b6a8ea289df83fe6376f97a1a764d1f3a4`.
The [observation artifact](system_catalog_report_workloads_2026_09_11.json)
contains settings, hashes, percentiles, and intermediate standalone runs.
Live workloads use Debug binaries, ten measured requests after three warmups,
and a shared macOS ARM64 host. This task's builds, tests and benchmark scenarios
ran sequentially during final measurements; other host activity is uncontrolled.
With ten live samples, the harness's nearest-rank p95 is the maximum observation.

### Metadata status application

**Correction from the checkpoint review:** the table below measured only the
inner projection transaction. It omitted `applyCommittedBatchInternal`, including
its full committed-entry watermark write and command decoding. The 184-byte WAL
figure is projection-only, not the complete local heartbeat apply. The checkpoint
follow-up below measures the full path and supersedes those broader claims.

A separate ReleaseFast component workload uses the production C allocator and
seven samples. Every store contains both group summaries and detailed runtime
observations. Every measured apply changes the compact header; a cached heartbeat
keeps all report observations unchanged. Fresh-clock updates advance all report
timestamps; sparse/all-group updates change report terms. Timing includes the inner projection transaction commit, not the outer committed
apply/checkpoint, command encoding, network transfer or Raft replication.
The baseline worktree adds only this benchmark and its build target plus a latent
write-stat accessor correction to the unoptimized production code.

| Groups | Apply workload | Before p50 (ms) | After p50 (ms) | Before / after WAL bytes per apply |
| --- | --- | --- | --- | --- |
| 1,000 | Cached heartbeat | 9.875 | 0.836 | 812,339 / 184 |
| 1,000 | Fresh clocks | 13.104 | 0.947 | 1,792,439 / 105,720 |
| 1,000 | One group changes | 9.222 | 0.938 | 813,319 / 138,643 |
| 1,000 | All groups change | 13.628 | 1.672 | 1,792,439 / 992,752 |
| 10,000 | Cached heartbeat | 95.250 | 9.086 | 8,120,339 / 184 |
| 10,000 | Fresh clocks | 118.539 | 15.447 | 17,929,539 / 1,053,249 |
| 10,000 | One group changes | 93.266 | 8.884 | 8,121,319 / 858,643 |
| 10,000 | All groups change | 118.055 | 29.195 | 17,929,539 / 9,923,563 |

The production layout uses stable slots in 64-group pages, a fixed directory for
selected-entry access, separate payload and clock pages, and structural digests.
It rewrites changed pages and compact membership, copies unchanged encoded page
entries, and reuses freed slots without renumbering unrelated observations.
Selected catalog reads enumerate actual reporters rather than probing every
store/range combination. A 64 MiB immutable block cache serves metadata reads.
Wire StoreRecord commands still contain 812,091 bytes at 1,000 groups and
8,120,091 at 10,000; incoming hashing remains proportional to report count.
These measurements do not establish lower Raft bandwidth or heartbeat RPC cost.

Full-store reconstruction is a measured tradeoff: 0.256 → 0.307 ms at 1,000 groups
and 2.618 → 3.639 ms at 10,000. The first one-row-per-group implementation regressed
these reads badly. With the same C allocator its uncached cursor version took
4.904 / 50.735 ms; cache plus batched point reads still took 2.679 / 31.021 ms.
That prompted stable pages before shipping. Earlier diagnostic runs used the
Zig testing allocator and are not mixed into the production-allocator comparison.
Pages bound group count, not arbitrary bytes in a single report. Selected reads
decode only their directory entries; broad snapshots still reconstruct all data.

### Application discovery workloads

Standalone keeps name/range indexes in the catalog transaction, captures selected
records into owned memory under the lock, and encodes the response after unlocking.
Its owned store has an 8 MiB immutable block cache. Broad inventory visits selected
range prefixes in storage order; narrow pages bound unrelated cursor skips.
Details construct typed enrichment summaries and redact producer configuration
before the final encode, avoiding a full-response JSON parse/redaction/re-encode.

| Workload | Before p50 (ms) | After p50 (ms) |
| --- | --- | --- |
| Detail beside 200 distinct, 200-field schemas | 5.086 | 1.649 |
| Full inventory of those 200 tables | 510.985 | 490.905 |
| First 25-row page of those 200 tables | 26.232 | 26.342 |
| Complete cursor walk of those 200 tables | 490.047 | 463.533 |
| Detail beside 1,000 narrow-schema tables | 0.897 | 0.695 |
| Full inventory of 1,000 narrow-schema tables | 113.707 | 117.298 |
| First 25-row page of 1,000 narrow-schema tables | 4.631 | 4.318 |
| Complete cursor walk of 1,000 narrow-schema tables | 191.693 | 193.240 |

The 100-table narrow-schema inventory regressed 12.072 → 14.031 ms; its first
page regressed 3.885 → 4.519 ms. The initial 1,000-table inventory took 141.590 ms;
the cache reduced it to 132.226 ms, then ordered range traversal to 117.298 ms,
still 3.2% above baseline. Do not interpret the detail win as a universal scan win.

With eight saturated detail readers beside the wide 200-table inventory, detail
throughput rose 981 → 1,588 requests/s, and detail p50/p95 fell 7.271/10.292 →
4.661/6.898 ms. Inventory p50/p95 worsened 670.158/1,099.720 → 864.253/1,248.471 ms.
The updated server completes more competing detail work; this is a saturation
comparison, not equal delivered traffic or a claim of improved scan fairness.

At a target cap of 100 requests/s for each of eight readers, achieved aggregate
rates were 632 → 517 requests/s. Detail p50/p95 improved 5.222/8.027 → 2.434/5.156 ms;
inventory p50/p95 measured 489.244/1,002.467 → 499.889/1,171.778 ms. These closed-loop
clients missed the target in both runs and delivered different traffic, so this
is not an equal-load comparison or evidence that scan fairness improved.

### Clustered public API

The matched 30-table, 200-field workload uses three metadata and three data nodes
on one host, with ten samples after three warmups. Setup and shard readiness are
outside the measured requests.

| Workload | Before p50 / p95 (ms) | After p50 / p95 (ms) |
| --- | --- | --- |
| One-table namespace | 27.411 / 184.113 | 24.716 / 192.628 |
| Prefix selecting one table | 27.123 / 177.296 | 25.087 / 33.098 |
| Full inventory | 98.808 / 244.103 | 91.278 / 488.188 |
| Single-table detail | 26.372 / 48.001 | 25.336 / 248.265 |
| Empty default namespace | 33.127 / 209.267 | 22.906 / 197.940 |
| First 25-row page | 73.354 / 430.162 | 68.201 / 501.894 |
| Complete cursor walk | 126.200 / 338.385 | 102.054 / 531.791 |

Medians improved modestly; several tails worsened. This does not establish a
cluster-wide latency improvement or resolve the earlier 100-table storage-read
failure. The local report-apply improvement does not remove read barriers,
per-group transport work, scheduling or serialization from clustered requests.

### Node-level heartbeat framing

The [heartbeat investigation](HEARTBEAT_BUNDLING.md) traces existing store-report
bundling and the Raft transport's per-group split. Using the existing codec, a
256-group cap reduces 1,000 one-group frames / 106,000 bytes to four frames /
88,072 bytes. At 10,000 groups it reduces 10,000 frames to 40. The isolated
ReleaseFast, page-allocator encoding run measured 20.817 → 0.200 ms at 10,000
groups. Allocator/frame overhead dominates this component comparison; it is not
HTTP throughput or production transport latency. The raw artifact retains all
caps and sizes. At that revision, production transport still sent isolated frames.
Route-aware retries, bounded queue ownership and failure/latency workloads are
specified before a separate live transport-bundling change.

### Final validation

The full Antfly build and 65 metadata-storage, 58 standalone and 57 API tests
passed. New regressions cover selected replication statuses/action hints,
legacy normalization, unchanged/clock-only/sparse report updates, slot reuse,
duplicate observations, reincarnation, snapshot wire compatibility and repair,
other-group preservation, reopen without cache, bounded selected allocation,
and standalone captured-data ownership across rename/drop/reopen. Existing typed
redaction assertions pass with the single-encode detail path.

All 21 selected E2E cases passed in 76.50 seconds on the final `13829ab83` production
sources: `test_system_catalog.py`, `test_schema_migration.py`, `test_exact_sort.py`,
and the exact-star/scoped-permission-and-row-filter cases in `test_auth.py`.
Changed-file Zig/Python formatting, Python lint/compile checks and diff whitespace
checks passed. No public generated contracts changed in this round; earlier SDK
and generated checks remain in the preceding history. These focused checks are
not a complete repository-suite pass.

## Complete apply checkpoints and placement reads — 2026-09-11

This follow-up corrects the preceding report benchmark's scope: that benchmark
timed the inner projection transaction and omitted the outer applied watermark,
which still stored the full committed batch. Its 184-byte cached-heartbeat WAL
result was not the complete local apply cost. The corrected harness times
`SnapshotBuilder.applyBatch`, including command decoding, checkpoint persistence,
projections and transaction commit. Wire encoding remains outside the interval.

The matched baseline is `9f828fc4d33cd062a906540c62a576cf7973d049` with only the
final benchmark harness substituted. Updated production sources are
`052c79c4a7d51becbf3dd78f04337be16e909cc3`, including main at `2bd96e33e`.
Both use ReleaseFast, the C allocator and seven samples on the same shared
macOS ARM64 host. Runs were sequential with no builds or tests from this task
overlapping measurement. The [raw observations](system_catalog_checkpoint_workloads_2026_09_11.json)
retain all sizes and scenarios, the discarded digest prototype, validation and
the final live workload's binary hash. The harness emits medians, not individual
sample timings.

### Durable progress without retained replay batches

Metadata now stores a versioned 26-byte checkpoint containing applied index,
input kind and input byte count in the same transaction as projected state.
The Raft log owns replay entries. The in-memory checkpoint map also retains only
this compact value. Legacy index-plus-batch rows remain readable and upgrade
on the next successful apply or snapshot installation. Logical snapshot wire
format is unchanged. Tests cover reopening, snapshot progress, legacy import,
format validation and failed-apply preservation of durable and cached progress.

| Groups | Scenario | Before p50 (ms) | After p50 (ms) | WAL bytes/apply before / after |
| --- | --- | --- | --- | --- |
| 1,000 | Cached heartbeat | 1.354 | 1.046 | 812,381 / 277 |
| 1,000 | Fresh clocks | 1.414 | 1.024 | 917,917 / 105,813 |
| 1,000 | One group changes | 1.382 | 0.988 | 950,840 / 138,736 |
| 1,000 | All groups change | 2.238 | 1.808 | 1,804,949 / 992,845 |
| 10,000 | Cached heartbeat | 15.000 | 11.226 | 8,120,381 / 277 |
| 10,000 | Fresh clocks | 24.547 | 16.674 | 9,173,446 / 1,053,342 |
| 10,000 | One group changes | 15.391 | 11.450 | 8,978,840 / 858,736 |
| 10,000 | All groups change | 41.055 | 31.197 | 18,043,760 / 9,923,656 |

An intermediate 58-byte checkpoint also hashed the complete input with SHA-256.
It achieved the write reduction but cached apply at 10,000 groups measured
16.276 ms, above the 15.000 ms baseline. Recovery did not consume the diagnostic
digest, so the final design removes that extra full-input pass. It retains the
normal storage integrity checks. This intermediate result is recorded separately
and is not the shipped implementation.

The command wire remains 812,091 bytes at 1,000 groups and 8,120,091 at 10,000.
Decoding and report comparison still scale with incoming reports. These local
measurements exclude Raft replication, network transport and registered service
callback fanout; they do not establish production throughput or reduced network
bandwidth. The shared host had low disk headroom and uncontrolled other activity.

### Placement checks without full report hydration

Placement compare-and-upsert reads the store header for node identity and drain
state in its existing transaction. It no longer reconstructs unrelated group
summaries and runtime reports. Seven samples each contain 100 independent read
transactions; reported per-operation medians include transaction open and close.

| Reported groups/store | Before p50 (ms) | After p50 (ms) |
| --- | --- | --- |
| 100 | 0.036910 | 0.003640 |
| 1,000 | 0.329340 | 0.005330 |
| 10,000 | 3.882020 | 0.031450 |

Full-store reads remain available where required, such as termination debt.
Their 10,000-group observation was 3.954 / 3.575 ms; this unchanged code path's
timing variation is not attributed to the checkpoint change.

### Contract and validation follow-up

Both listing routes now declare and return a JSON error for stale-cursor HTTP
409 responses. The TypeScript `tables.list()` contract is `Promise<TableStatus[]>`
and rejects bodyless error responses. Regressions exercise JSON/bodyless errors,
successful empty listings, generated Go 409 decoders and public HTTP pagination.

The final Antfly build and 134 focused tests passed: 66 metadata storage, 57 API
and 11 managed-host tests. All 21 selected catalog/schema-migration/exact-sort/
scoped-auth E2E cases passed in 78.27 seconds on the final Debug binary with SHA-256
`7dc98e2246ac2b83dbe12f2360a39d5f6e83ee70efd35e67bf60da5a685c240e`.
The first E2E attempt hit the fixture disk-headroom safeguard; removing obsolete
build artifacts allowed the final run without lowering that safeguard.

The TypeScript suite passed 282 tests with one skipped. SDK build/typecheck,
Antfarm typecheck, Go `oapi` tests, Python generated checks, public and Zig
OpenAPI generation/checks, and changed-file Zig/TypeScript/Python checks passed.
These are focused validations, not a complete repository-suite pass.

### Final clustered application observation

The final Debug binary also completed the discovery workload with three metadata
and three data nodes, 30 unrelated tables, 200-field schemas and 25-row pages.
Ten measured requests follow three warmups; setup and readiness are outside timing.

| Operation | p50 (ms) | p95 (ms) |
| --- | --- | --- |
| One-table namespace | 27.836 | 214.649 |
| Prefix selecting one table | 20.191 | 220.997 |
| Full inventory | 81.466 | 283.404 |
| Single-table detail | 27.035 | 197.666 |
| Empty default namespace | 12.786 | 23.545 |
| First 25-row page | 68.904 | 206.459 |
| Complete cursor walk | 102.777 | 312.533 |

This is a final application validation observation, not a paired speedup claim.
It does not establish improved cluster-wide latency or resolve the earlier
100-table storage-read failure. Settings and binary provenance are embedded in
the raw artifact. Run it from `zig/` with:

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario listing --deployment cluster --table-counts 30 --schema-fields 200 \
  --listing-page-size 25 --samples 10 --warmup 3 \
  --output /tmp/catalog-checkpoint-cluster.json
```

## Bounded manifests, runtime references and routed heartbeats — 2026-09-11

Production sources: baseline `6e5cc393a`, updated `a0b3dfe0a`, with `main` merged
through `6ae2ddb37`. The baseline report fixture was adjusted to the same reporter
incarnation (77) and status generation (1), and the production heartbeat harness
was copied unchanged to it. No baseline production code changed. Updated builds
used the sources subsequently committed as `a0b3dfe0a`.

[Raw component and clustered observations](system_catalog_bounded_reports_2026_09_11.json)
retain settings and all measured group counts. Component benchmarks use
ReleaseFast and seven-sample medians. Builds, tests and benchmark scenarios from
this task did not overlap the timed measurements; unrelated host activity was
uncontrolled. These are local development measurements.

### Report apply and bounded persistence

The apply interval includes command decoding, projection, checkpoint persistence
and transaction commit. It excludes proposal encoding, replication, network and
registered service callback fanout. Both fixtures change the capacity header on
every sample. Report allocation uses the C allocator.

| 10,000 groups per store | Before apply p50 (ms) | After apply p50 (ms) | Before WAL bytes | After WAL bytes |
| --- | --- | --- | --- | --- |
| Cached full report | 12.708 | 13.184 | 304 | 304 |
| Fresh observation clocks | 17.802 | 16.572 | 1,053,369 | 253,295 |
| One changed group | 12.174 | 13.108 | 858,763 | 61,841 |
| Every group changed | 33.241 | 33.084 | 9,923,683 | 9,616,373 |

Membership now lives in 64-group pages of 48-byte entries; a small root directory
changes only when live pages change. Sparse payload updates no longer rewrite
an 800 KB membership row, and observation clocks no longer have redundant hashes
in that row. The one-group case writes 13.9 times fewer WAL bytes. This is a
write-amplification improvement, not a universal CPU improvement: cached and
one-group apply medians increased modestly. Full-store hydration measured
4.185 → 3.535 ms; the unchanged header-only drain path measured 0.0333 → 0.0070 ms,
too small and noisy to attribute to a new optimization.

The additional runtime-reference scenario transmits a 1,230,124-byte Raft command
instead of 8,210,124 bytes, an 85% reduction. It retains committed runtime
observations and sends current group facts. Apply writes 304 WAL bytes but takes
21.206 ms, versus 13.184 ms for the updated full cached report. Reading and
reconstructing the retained observations costs CPU; removing an extra owned clone
did not eliminate that tradeoff. Changed runtime observations still use full
reports. No distributed heartbeat-latency improvement is claimed.

Projection-cache tests separately verify that header-only changes retain report
arrays, payload changes reload only their store, and unrelated stores retain
ownership. The bounded invalidation queue falls back to a full refresh after
overflow, snapshot replacement or failure. These allocation/ownership regressions
do not measure complete reconciliation latency; consumer snapshot cloning remains.

### Production heartbeat host

The counting driver measures production route lookup, grouping and encoding,
excluding HTTP and receiver work. Routes are installed outside timing; the
allocator is the page allocator in both runs.

| Ready groups sharing a route | Frames before → after | Host send p50 before → after (ms) |
| --- | --- | --- |
| 100 | 100 → 1 | 0.293 → 0.007 |
| 1,000 | 1,000 → 4 | 2.546 → 0.052 |
| 10,000 | 10,000 → 40 | 20.193 → 0.516 |

At 10,000 groups, encoded bytes fall from 1,060,000 to 880,720. Group/source/route
identity, terms and contexts are retained. Failures become per-group retries with
current route lookup and bounded retained bytes. Per-group ticks and consensus
processing remain. Deterministic tests cover mixed routes, source identity,
endpoint metadata, ordering, byte/group caps, read contexts and retry route removal.
This is not a measurement of network throughput, hot-group latency, elections or
10,000 simultaneously running ranges. The larger workload matrix in
[heartbeat bundling](HEARTBEAT_BUNDLING.md) remains production capacity validation.

### Final live discovery observation

A disposable three-metadata/three-data-node Debug cluster completed the 30-table,
200-field discovery workload, with ten samples after three warmups. Startup,
provisioning and readiness are outside timing. Binary SHA-256:
`6c8adc2ea7836472d12808f0bd961190f0ad0d2e768df1249275af2fc6c26100`.

| Operation | p50 (ms) | p95 (ms) |
| --- | --- | --- |
| One-table namespace | 24.491 | 172.589 |
| Prefix selecting one table | 15.761 | 24.273 |
| Full inventory | 78.945 | 83.925 |
| Single-table detail | 26.711 | 376.566 |
| Empty default namespace | 27.041 | 32.957 |
| First 25-row page | 65.592 | 383.748 |
| Complete cursor walk | 83.349 | 375.142 |

This is an unpaired final application observation. It establishes that the
workload completed, not a cluster-wide speedup or resolution of earlier
100-table storage-read failures. Large tails remain.

### Validation and compatibility boundary

The measured server sources built successfully with 68 metadata-storage, 58 API
and 102 metadata-service tests passing. All 395 Raft tests passed, including the
new failure and frame-boundary regressions. The 22 selected catalog, migration,
sorting and authorization E2E cases passed in 75.61 seconds on the recorded binary.
The subsequent compatibility clarification passed 59 standalone tests (including
main JSON/Lite checkpoints and current HA logical seeds) and all 101 catalog/store
tests after removing obsolete development-index cleanup.

TypeScript passed 282 tests with one skipped; SDK and Antfarm type checks passed.
Go `oapi`, ten Python response tests, Python/Zig generation checks and changed-file
formatting passed. The embedded Antfarm bundle was regenerated with the pinned
toolchain. These checks do not constitute a full repository-suite run.

New catalog errors use the required shared `error` field and stable `code`.
Seven resource-mutation operations now expose typed committed-but-not-yet-visible
HTTP 202 responses in generated clients. Compatibility remains for shipped
`main` records, watermarks and standalone checkpoints, plus the current logical
HA seed import contract. Intermediate catalog layouts from this unmerged PR are
not supported migration inputs.

## Reproduction

See [workloads and commands](SYSTEM_CATALOG.md). Run the resolution scenario
with `--binary` pointing to each separately built revision, using otherwise
identical arguments. Keep setup and warmup outside measured intervals and run
builds, tests, and other benchmark scenarios separately. Raw JSON output retains
all settings, binary hash, percentiles, startup time, and readiness poll counts.

## Report admission and asynchronous delivery follow-up

The matched baseline is `23335d336`; the updated source includes `origin/main`
at `5460d0490` via merge `72794f05a`. [Raw results and source hashes](system_catalog_admission_workloads_2026_09_11.json)
record both component runs and the live discovery workload. Both component
revisions were compiled before measurement; task-owned builds/tests did not
overlap the sequential benchmark runs. Unrelated host activity was uncontrolled.
Zig 0.16.0 ReleaseFast and `c_allocator` were used for components. Each apply and
repair comparison uses seven samples; selected-store capture uses nine.

| 10,000 groups per store | Before | After |
| --- | --- | --- |
| Repair-fact admission comparison p50 | 180.733 ms | 5.651 ms |
| Repair-free admission comparison p50 | 0.045 ms | 0.012 ms |
| Referenced-runtime committed apply p50 | 20.896 ms | 8.726 ms |
| Cached full-report committed apply p50 | 13.048 ms | 16.484 ms |
| Fresh-clock committed apply p50 | 16.056 ms | 20.889 ms |
| One-group-change committed apply p50 | 12.854 ms | 16.420 ms |
| All-group-change committed apply p50 | 33.347 ms | 22.304 ms |
| Full-store hydration p50 | 3.693 ms | 5.326 ms |
| One-group-change WAL bytes/apply | 61,841 | 14,591 |
| All-group-change WAL bytes/apply | 9,616,373 | 2,233,295 |
| Fresh-clock WAL bytes/apply | 253,295 | 347,228 |

Repair comparison builds temporary identity indexes over borrowed observations;
its fixture uses separate equal runtime slices and one full-text index per group.
It excludes cloning, service callbacks and proposal/replication. At 1,000 groups,
repair admission measured 0.633 → 0.582 ms; at 10,000 groups the quadratic prior
scan dominates. References also avoid the comparison entirely for the retained
runtime inventory: only group facts and the header are read and compared.

Runtime and group payloads now occupy separate primary pages, with independent
clock pages. Reference apply never decodes/hashes runtime payloads. The benefit
comes with additional page reads, directories and per-group encoding on full
reports: cached full apply increased about 26%, full hydration 44%, and fresh-clock
WAL bytes 37% in this run. These are material tradeoffs, not universal speedups.
The preferred unchanged-runtime heartbeat path improved 2.4×, while changing
all group facts no longer rewrites runtime payloads. Wire size is unchanged from
the previous iteration: 1,230,124 bytes for the 10,000-group reference command
versus 8,210,124 for a full command. Apply includes command decode, projection,
checkpoint persistence and commit, but excludes network, Raft replication and
service callback fanout.

A second component models one reporting store beside unrelated tenant inventory,
with 100 runtime groups per store. Both paths run in the updated binary. The
baseline reproduces the previous whole-inventory clone and capability scan;
selection retains one immutable store lease, reads aggregate capability counts,
and clones only that store. The interval includes clone destruction.

| Stores | Whole-inventory p50 | Selected-store p50 |
| --- | --- | --- |
| 1 | 0.019 ms | 0.016 ms |
| 10 | 0.167 ms | 0.015 ms |
| 100 | 1.514 ms | 0.016 ms |

This measures admission preparation, not HTTP end-to-end reporting or all
reconciliation consumers. The full-inventory reconciliation clone remains.

The disposable three-metadata/three-data-node discovery workload provisioned
30 tables with 200-field schemas. Ten samples followed three warmups: inventory
p50/p95 was 77.698/86.696 ms, first-page 75.243/476.368 ms, complete cursor walk
99.858/323.133 ms, and selected-table status 25.921/30.909 ms. These are unpaired
application observations, not evidence of capacity at 10,000 live ranges or
resolution of earlier 100-table/empty-graph failures. Server SHA-256:
`a1c4b965e60d10f9a1f6872b028cf38fbe35948a96ecbde92447d50d52e43420`.

Asynchronous HTTP now returns failed frame ownership to the codec transport for
route-aware retries. HTTP reservations include queued, in-flight and failed
completions, with counters for retained bytes/frames. Defaults admit the existing
32 MiB maximum request; the separate codec retry queue retains its 8 MiB cap.
Deterministic regressions verify blocked peers, invalidated unsent routes,
failed-completion accounting, current-route retry, removed groups, source/read
contexts and attempt exhaustion. These correctness checks do not measure
network throughput or queue-tail latency.

Validation: server build and focused suites passed (69 storage, 58 API,
117 metadata/observer, 46 HTTP transport tests), plus all 396 Raft library tests.
The 22 catalog/schema/sort/authorization E2E tests passed in 80.57 s. A distributed
status E2E also passed: with the data owner paused, an authenticated reference
heartbeat preserves runtime/index observations, updates group clocks, and rejects
stale generations and changed group inventory. Rust SDK generation and all
14 tests passed with the locked dependencies; heterogeneous catalog 202 responses
retain typed pending-visibility variants. These are focused checks, not a full
repository-suite pass.

## Relational main integration

Merged relational storage from `origin/main` at `aefe3bad4` into the implementation
above (`c4044610a`). Scoped create/drop now expose the shared committed mutation
outcomes; Rust decodes scoped HTTP 201 as a typed completed `TableStatus`, retaining
the status and ETag. The merge keeps one deadline/clock visibility type. Packed
rows continue to use immutable catalog destinations, without a new migration.

[Post-merge raw results and source hashes](system_catalog_relational_merge_2026_09_11.json)
record a fresh component run and two application workloads. These are post-merge
observations, not a new matched comparison. Compilation and other task-owned
tests finished before timing; all measured workloads ran sequentially. Unrelated
host activity was uncontrolled. The original comparison's raw `merged_main`
provenance has been corrected to `5460d0490`, matching merge `72794f05a`; its
measurements were taken before the relational merge.

At 10,000 groups, repair admission measured 5.994 ms p50, referenced-runtime
apply 8.837 ms, cached full apply 15.248 ms and full hydration 5.550 ms. WAL sizes
were unchanged from the prior implementation run. With 100 stores, selected
admission measured 0.016 ms versus 1.613 ms for the reproduced whole-inventory
preparation. The full-report and hydration tradeoffs described above remain.

The new standalone relational catalog scenario provisions ten closed-schema
tables and validates event/customer rows while measuring scoped operations.
Ten samples follow three warmups; concurrent lookup uses eight clients and
80 measured requests. This exercises catalog routing over packed rows, with
one selected event and one customer row, not bulk relational ingestion capacity.

| Scoped relational operation | p50 | p95 |
| --- | --- | --- |
| Point lookup | 0.441 ms | 0.554 ms |
| Query | 0.763 ms | 1.027 ms |
| Customer join | 1.438 ms | 1.639 ms |
| 20-line repeated-target NDJSON | 5.389 ms | 5.887 ms |
| Ten-table listing | 2.746 ms | 2.887 ms |
| Concurrent point lookup | 2.182 ms | 3.753 ms |
| Identity-preserving rename | 0.621 ms | 0.670 ms |

The repeated three-metadata/three-data-node, 30-wide-table discovery workload
measured inventory p50/p95 80.605/271.918 ms, first page 81.327/271.081 ms,
cursor walk 104.212/287.773 ms and selected status 27.288/403.440 ms. Tail latency
varied materially. These small unpaired runs establish neither a distributed
speedup nor resolution of the earlier larger-capacity failures. Both application
runs used the merged Debug server SHA-256
`2d6116422e8327ee386585792dde062e198172c9587a547e3826191c6933e691`.

Post-merge validation: server and focused storage/API/metadata/HTTP suites passed
(69/58/117/46 tests). The E2E selection passed 22 cases initially; after correcting
the new fixtures to omit server-managed schema version and await the committed
runtime baseline before pausing the owner, both remaining cases passed. The
relational regression checks scoped-versus-literal row isolation, logical query
labels, table/database rename, stable identity and restart persistence. Go SDK
packages, 15 Rust tests, 283 TypeScript tests (one skipped), SDK/Antfarm type
checks and the canonical Antfarm rebuild passed. The earlier 396-test Raft library
run remains recorded above; this merge did not change those library sources.

### Subsequent durability/readiness merge

The final fetch brought in `origin/main` at `2b57462b1` (#694, #713 and #714).
These changes reserve topology versions 5/6 and restore command tags 52/53.
The catalog now uses capability version 7 and command tags 54/55; published main
encodings and runtime error numbers remain stable. The routed catalog callbacks
also accept the shared mutation driver's expanded error contract. Intermediate
encodings from this unmerged PR remain outside the compatibility boundary.
The relational application measurements above precede this subsequent merge and
retain their original source and binary provenance.

Final merge validation passed: server build, 72 metadata-storage, 58 API,
121 metadata/observer/routing and 46 HTTP transport tests; all 408 Raft library
tests; Go SDK packages, 15 Rust tests and generated Zig API checks. The E2E
selection passed 24 cases on the first run. Its additional stalled-discovery
restore case passed in 19.83 s after replacing the old name-derived identity
assertion with a fresh destination ID after drop/restore. The regression retains
fresh Raft-group, full-replication, every-node row-read and restore-cleanup checks.
Python response tests (14), TypeScript tests (283 passed, one skipped) and
SDK/Antfarm type checks passed at the preceding relational merge. These remain
focused checks rather than a full repository-suite pass.

A final sequential component rerun, recorded under `final_main_integration` in
[the raw artifact](system_catalog_relational_merge_2026_09_11.json), measured
10,000-group repair admission at 6.558 ms p50 and reference apply at 9.524 ms.
Cached full apply was 17.611 ms and full hydration 6.188 ms; WAL sizes remained
unchanged. Selected-store preparation beside 100 stores was 0.015 ms versus
1.692 ms for the reproduced full-inventory path. These unpaired observations
retain the earlier full-report tradeoffs; they do not establish the cause of
run-to-run timing variation. Application workloads were not repeated after this
last main merge.

## Immutable mutation results and report delivery

Implementation `d79e0441a` returns the exact admitted resource projection only
following verified commit. Resource responses no longer perform a second lookup
by a reusable name. This fixes the reproduced create/drop/recreate identity race
and removes the associated read barrier. The graph integration in `45d63e1ff`
resolves metric actions through the catalog, excludes graph metric work from
plain document-lookup routing, and supports the generated colon-delimited action
route in the shared HTTP router. Published main error IDs retain their numbers.

[Raw results, binary hashes and workload scope](system_catalog_delivery_workloads_2026_09_12.json)
record source `44a94167b`, including graph main `014424f59` (merge `8d242c237`).
Zig 0.16 ReleaseFast component measurements use `c_allocator`. All task-owned
builds and tests finished before the final sequential measurements; unrelated
host activity, including another Antfly swarm, was uncontrolled. Queue/admission
comparisons below exclude network and proposal/replication work. They are not
claims about distributed throughput or election stability.

| Component, p50 | Before | After |
| --- | --- | --- |
| Reconnect retry drain, 4,096 frames | 9.168 ms | 0.064 ms |
| HTTP queue drain, 4,096 frames across 16 peers | 9.691 ms | 0.150 ms |
| Unchanged repair-bearing admission, 10,000 groups | 13.314 ms | 5.910 ms |
| Header-change admission, 10,000 groups | 14.464 ms | 8.442 ms |

Retry draining compares separately compiled production transports with the same
harness. Stable survivor compaction removes repeated tail movement while retaining
ordering, retry deadlines and route re-resolution. HTTP scheduling compares a
reproduced global FIFO with the production per-peer FIFOs/ready-peer queue in one
binary, using the same frame release costs. Its reference omits the old in-flight
hash operations. The scheduler waits on a condition predicate, with one request
in flight per peer and unchanged global/per-peer retention bounds. No new batching
timer or wire protocol is required.

Admission compares the former prior-clone/compare/owned-apply preparation against
one production admission pass with pinned borrowed prior records and a fake
proposal sink. Incoming report arrays are separately allocated. Unchanged-report
allocations fall from 70,147 to 75; header-change allocations fall from 140,149
to 70,078. The counter checks complete allocation/free balance. Duplicate reports
retain sequential reporter-generation fencing, and failed allocations leave the
borrowed prior and incoming report intact.

### Storage cost and remaining tradeoffs

A fresh rerun of the pristine `fd4745718` executable and the merged implementation
produced these observations. This cross-revision comparison also includes the
main graph integration, so timing differences are not isolated attribution to the
local codec. The full-record and command byte sizes match between revisions.
Each apply includes command decoding, projection, checkpoint persistence and
commit, excluding replication/network and callback fanout.

| 10,000 groups, p50 | Before | After |
| --- | --- | --- |
| Cached full report apply | 14.916 ms | 15.223 ms |
| Fresh observation clocks | 20.231 ms | 20.619 ms |
| One changed group | 16.879 ms | 16.702 ms |
| All groups changed | 21.674 ms | 20.737 ms |
| Referenced runtime apply | 8.412 ms | 8.076 ms |
| Full-store hydration | 5.190 ms | 3.345 ms |

The local component codec removes repeated StoreRecord envelopes, partitions
incoming groups into contiguous buffers and decodes directly into caller-owned
aggregate arrays. Full logical StoreRecord wire/snapshot codecs remain unchanged.
One-group WAL falls from 14,591 to 10,111 bytes and all-group WAL from 2,233,295
to 1,533,295 bytes. Cached/reference WAL stays at 304 bytes; fresh-clock WAL stays
at 347,228 bytes. Full apply time remains essentially unchanged, including small
regressions for cached/fresh-clock cases. This does not resolve the earlier
full-report apply tradeoff. Full hydration allocates 40,015 times / 12,609,697
bytes in caller-owned output and scratch; these counters are collected outside
the latency loop and have no matched baseline allocation count.

### Live application workloads and validation

A disposable Debug standalone server exercised ten relational tables with two
warmups and nine samples, checking returned rows and logical table labels.
Lookup/query/customer-join p50s were 0.456/0.606/1.130 ms. Twenty-line repeated-target
NDJSON was 4.796 ms, ten-table listing 2.364 ms and identity-preserving rename
0.574 ms. Eight-client point lookup measured 1.938 ms p50 / 3.366 ms p95 over
72 requests. This is a small packed-row catalog workload, not bulk ingestion.

A separate ten-tenant management/restart scenario measured point GET 0.545 ms,
namespace create/drop 1.247 ms and rename round trip 1.493 ms p50. Concurrent
namespace create/drop was 4.284 ms p50 / 8.809 ms p95. Restart validation preserved
all database IDs/names and confirmed temporary namespaces stayed deleted. Both
application runs are unpaired observations; raw artifacts retain the server hash,
settings, setup and recovery timing separately.

Validation passed: server; 73 storage, 26 catalog durability, 9 catalog transport,
123 metadata service, 61 standalone, 60 catalog API, 48 HTTP transport, 20 router,
15 observer, 54 graph fan-in and 14 graph remote-wire tests. The 26 selected E2Es
passed as 23 in the combined run and three graph/auth reruns after correcting the
shared router. These include scoped graph maintenance through rename, read-only
maintenance denial, relational rows, restore/restart, CLI, remote heartbeat status
and filtered PageRank. Go SDK packages, 16 Rust tests, 14 Python response tests,
286 TypeScript tests (one skipped), TypeScript typecheck and generated Zig API
checks passed. The checked-in scheduler benchmark retains module reachability
anchors so its workload actually executes.

At the benchmark revision, Raft Debug passed all 409 tests and ReleaseFast passed
403, with six joint-consensus trace failures also reproduced on a pristine
baseline. The follow-up below resolves those failures. These are focused checks,
not a full repository-suite pass.

### Raft simulation fixture resolution

Follow-up commit `c5a91d4af` fixes two interacting simulation harness bugs. Applying
a membership change freed the borrowed previous membership before routing cleanup
used it. The harness now owns that membership until cleanup finishes. It also
clones the current Ready message batch before applying membership changes, then
publishes it after cleanup, matching the etcd/raft reference replay. This preserves
the final commit notification to a removed peer while dropping older queued
traffic. Production consensus and transport code are unchanged.

A regression covers both configuration-change formats. Against the old harness,
Debug retains four messages and ReleaseFast retains one, where two are required.
With the fix, all 410 Raft tests pass in Debug, ReleaseSafe and ReleaseFast. All six
unchanged differential traces also match the etcd/raft v3.6.0 reference runner.
These harness-only changes do not alter the benchmark measurements above.

## Sparse reports and shared reconciliation views

Production source `5206bfe15` implements acknowledged sparse reports, retained
reconciliation inputs and indexed report collection. Benchmark follow-up
`1f1670980` makes both snapshot comparison paths consume retained runtime payloads.
Main `3ea6fcead` is included through merge `2a0e2ee85`.
[Raw results, settings and provenance](system_catalog_sparse_workloads_2026_09_12.json)
retain every component case and the application binary hash.

Components use Zig 0.16.0 ReleaseFast and `c_allocator`. Both component binaries
compiled before timing; task-owned builds, tests and preflight workloads had
finished. The recorded application workload ran afterward, separately, using
Debug. Unrelated host activity and thermal state were uncontrolled. The paired
comparisons below use the same binary and inputs; they compare the full-report,
linear-scan or deep-clone paths with the new paths, not whole-server throughput.

| Component, p50 | Reference path | New path |
| --- | --- | --- |
| HTTP prepare/encode/free, 10,000 groups, one changed group | 30.068 ms full JSON | 1.969 ms sparse prepare/encode/commit |
| Apply, 10,000 groups, one changed group's Raft facts | 20.752 ms full report | 5.019 ms sparse report |
| Collection lookups, 10,000 tables/ranges | 64.185 ms repeated scans | 0.164 ms build/use/free indexes |
| Snapshot capture/read/release, 100 stores × 100 groups | 1.830 ms deep clone | 0.001541 ms retained leaves |

The publisher retains acknowledged per-group leaves, compares against that map
directly and allocates only changed replacements. Unchanged clocks remain at
their last transmitted values, preserving periodic refresh. The HTTP fixture
shrinks from 36,127,753 to 4,262 bytes. It contains no volatile samples and excludes
network/admission/replication. Its full 10,000-group JSON exceeds the default
32 MiB HTTP body limit: this demonstrates encoding cost, not successful bootstrap
of that inventory through the default server configuration.

The applied command shrinks from 8,210,124 to 1,044 bytes. The new bounded binary
envelope reuses the StoreRecord codec and excludes HTTP-only embedding activity.
Admission reads affected groups through covering references; apply checks the
exact cursor again. This version incorrectly discarded cursors during snapshot installation;
the snapshot-plus-suffix divergence is corrected in the following section.
Ordinary reopen preserves the cursor and payload atomically. Empty deltas with
unchanged headers retain their cursor without generating a Raft entry. Exact
replays acknowledge the prior commit, while stale bases and incarnations cannot
overwrite it. These semantics are tested through the real HTTP/Raft path.

Sparse apply still scans compact membership records. A changed runtime component
also rewrites its containing 64-slot page: that case took 5.151 ms and wrote
59,417 WAL bytes. The group-facts-only sparse case wrote 10,232 bytes versus
10,111 for the full-report equivalent; the cursor adds 121 bytes. Full/reference
paths remain in the artifact: at 10,000 groups, cached/fresh-clock/all-group/full
hydration p50s were 18.695/24.864/25.475/4.618 ms, and reference apply was 10.448 ms.
This work does not make full refreshes or full hydration cheap.

Snapshot measurements include capture, one runtime payload read per store and
release. Shared views batch 1,000 iterations per timer reading to resolve small
durations; both paths validate the payload. These are snapshot costs, excluding
the reconciliation plan. Production local reconciliation pins catalog generations
and store leaves; transition readiness pins store leaves. Public owned snapshots
clone large payloads after releasing publication locks. Smaller workflow progress
collections still use owned copies.

### Sustained application traffic

The recorded disposable cluster has three metadata and three data nodes, 100
one-shard relational tables and one desired data replica per shard. Table creation
and shard readiness p50s were 148.216 and 711.545 ms, measured separately. Qualified
lookup/query/join p50s were 30.001/51.882/53.861 ms; scoped listing was 79.586 ms.

The new `--mixed-seconds 30` workload runs three clients concurrently and validates
every response. It completed 68 full-index batches of 100 upserts, 188 qualified
searches and 153 scoped inventory reads without request failures. Ingestion,
search and discovery p50/p95 latencies were 463.697/918.570,
53.716/721.463 and 78.191/719.527 ms. The 100 documents are repeatedly updated;
this is a bounded working set, not bulk storage growth. Tail latency remains
material. This unpaired run establishes neither a distributed speedup nor a
resolution of the earlier wide-schema/three-replica and empty-graph failure cases.

Validation passed: Debug server, 81 storage, 34 catalog, nine catalog transport,
126 metadata service, 60 catalog API and 20 benchmark test executions. Some
focused selections overlap. All 21 distributed status/system catalog E2Es passed;
the expanded telemetry-only distributed case then passed separately. Coverage
includes duplicate observations, explicit removals, stale bases/incarnations,
lost-response replay, snapshot/reopen recovery, allocation failures, malformed
binary/HTTP input and immutable-leaf lifetime. Ruff and whitespace checks passed.
These are focused checks, not a full repository-suite pass.
The additional repository-wide license-header check reports pre-existing header
drift (984 files before normalization, 982 remaining). The two short headers in
files touched by this follow-up were normalized; all follow-up files now match
their canonical headers. No runtime behavior changed in that cleanup.

## Group-local reports, bounded activity and concurrent admission

Implementation `dfaff83e8` fixes snapshot cursor loss, makes admitted header/cursor
preconditions mandatory in report commands, patches affected storage pages and
retains unchanged per-group cache payloads. It replaces whole-runtime telemetry
with bounded identity/counter batches and replaces the exclusive catalog report
lock with per-store lanes plus a shared gate released before apply waits.
Merge `cff4ef7be` includes main `a3515eb85` (owned physical-usage snapshots).

[Raw component and cluster results](system_catalog_group_local_workloads_2026_09_12.json)
include binary hashes, source provenance and limitations. The paired measurements
below precede the merge so both sides use main `3ea6fcead`. Components use Zig
0.16.0 ReleaseFast and `c_allocator`; the cluster binaries use Debug. Runs were
sequential after task-owned builds/tests finished. Unrelated host activity and
thermal state were uncontrolled. Numbers are observations, not timing assertions.

| Component at 10,000 groups, p50 | Reference path | New path |
| --- | ---: | ---: |
| Apply one group's Raft facts | 16.297 ms full report | 0.168 ms sparse report |
| Clone/publish/release runtime cache view | 5.230 ms full refresh | 2.107 ms sparse refresh |
| Encode all embedding activity samples | 25.757 ms whole runtime reports | 3.402 ms compact batches |
| Activity HTTP bytes, all samples | 39,048,908 | 2,944,534 |

Sparse apply was 0.131/0.152/0.168 ms at 100/1,000/10,000 groups. The previous
sparse implementation recorded 0.199/0.472/5.019 ms in the preceding experiment;
that historical comparison is separate from the paired full/sparse comparison
above. The new runtime-change case was 0.179 ms at 10,000 groups. It still rewrites
one 64-slot page (59,417 WAL bytes). Raft command size is 1,125 bytes, including
the mandatory admitted header and cursor. Allocation metadata is touched only
when group inventory changes; directory maintenance is limited to page liveness
changes. Full repair continues to walk the inventory.

The cache fixture has one embedding index per runtime group. Full refresh made
90,037 allocations versus 32 for sparse refresh. These measurements include input
cloning and lease construction/release, excluding storage reads. Sparse refresh
retains nested payloads, but its flat arrays and group maps remain proportional
to the store's inventory; the 2.107 ms result is not an O(1) publication claim.

The activity fixture includes every sample, with no durable changes. At 10,000
groups, all samples fit in 20 requests of at most 150,798 bytes. The reference
request exceeds the default 32 MiB limit; this is an encoding comparison, not a
successful oversized HTTP call. Full baseline JSON still has the ordinary HTTP
limit. The compact activity format bounds sample counts and identity strings;
telemetry validates against retained committed identities without loading whole
store reports or holding catalog/runtime locks during cache updates.

The application workload uses three real metadata replicas, three data nodes,
and eight synthetic non-live reporting stores with 100 groups each. Setup waits
for accepted registrations to apply. Each case sends 240 reports while another
client performs 30 namespace create/drop pairs. All requests succeeded. These
measurements include client HTTP and metadata-leader discovery overhead and do
not discard warmup samples.

| Cluster workload | Before | After |
| --- | ---: | ---: |
| Runtime-change reports/s | 23.57 | 72.96 |
| Runtime-change report p95 | 990.84 ms | 163.50 ms |
| Concurrent namespace create/drop p95 | 1,186.69 ms | 202.98 ms |
| Activity-only reports/s | 37.05 | 161.98 |
| Activity-only report p95 | 635.02 ms | 79.37 ms |
| Concurrent namespace create/drop p95 | 1,053.64 ms | 132.01 ms |

Snapshot coverage now applies an identical committed log suffix to uninterrupted
and snapshot-restored replicas, verifies their cursors and group state, then
accepts a successor delta on the recovered replica and compares logical snapshots.
Additional regressions cover sparse removal/reuse/reopen, stale full-repair
admission, retained payload ownership under allocation failure, bounded group
invalidation, multiple activity batches, oversized batches and unknown identities.

The exact merged binary also completed the same 480-report/60-DDL-pair
workload without failures. Its runtime-change and activity-only throughput
was 75.11/138.30 reports/s, with report p95 152.74/79.02 ms and concurrent DDL
p95 166.85/135.82 ms. The artifact retains its binary hash separately.

Merged validation: 84 storage, 37 catalog, nine catalog transport, 129 metadata
service and 60 catalog API test executions passed (selections overlap). The
server build and all 21 distributed-status/system-catalog E2Es passed. The
pre-merge component target passed all 22 test executions. Ruff, Zig formatting
and whitespace checks passed. Standalone E2Es initially stopped at the disk
headroom preflight; clearing obsolete local build artifacts restored headroom,
and the subsequent complete run passed without changing production disk guards.


## Bounded reporting and snapshot workloads — 2026-09-13

[Raw observations and provenance](system_catalog_production_workloads_2026_09_13.json)
include separate publication and first-reader measurements. These runs precede
integration with main's compiled storage boundary; they measure the reporting
algorithm and same-binary request modes, not final merged release capacity.

| Report cache operation | 100 groups | 1,000 groups | 10,000 groups |
| --- | --- | --- | --- |
| Sparse publication p50 | 0.002 ms | 0.002 ms | 0.002 ms |
| Sparse publication allocations | 36 | 36 | 36 |
| First flat reader p50 | 0.003 ms | 0.029 ms | 0.403 ms |
| Full refresh p50 | 0.047 ms | 0.484 ms | 5.664 ms |

At 10,000 groups, the previous sparse refresh measured 2.107 ms. The new
publication cost excludes flattening: readers that need every report pay the
separately measured first-reader cost once per published component. Full refresh
remains linear and has more allocations (100,708 versus the earlier 90,037).

A real three-metadata/three-data-node cluster received a synthetic inventory of
10,000 groups with one index each. Twelve measured collections followed two
warmups; namespace creation and deletion ran concurrently.

| Operation | Normal report admission p50 / p95 | Independent telemetry p50 / p95 |
| --- | --- | --- |
| Complete 10,000-index collection | 524 / 837 ms | 133 / 326 ms |
| Concurrent namespace create + drop | 313 / 876 ms | 134 / 607 ms |

All three data nodes remained alive in both modes. The compact control transfer
was 8,027 bytes; the diagnostic transfer was 46,148,026 bytes across 89 pages,
with no page above 512 KiB. Synthetic groups were outside actual table placement,
so the compact size is not a claim that 10,000 active shard placements fit in
8 KiB. Cold capture timings were 1,952 ms and 983 ms respectively and should not
be interpreted as steady-state latency improvements.


### Final compiled-storage integration

The final Debug server matches source `821b5007b`, with main `c1a39a3ae`
merged. Its binary SHA-256 and every sample are retained in the raw artifact.
The same three-metadata/three-data-node workload completed with 12 measured
collections after two warmups in each mode. No measured requests retried.

| Operation | Normal report admission p50 / p95 | Independent telemetry p50 / p95 |
| --- | --- | --- |
| Complete 10,000-index collection | 638.178 / 761.593 ms | 130.709 / 282.215 ms |
| Concurrent namespace create + drop | 122.499 / 509.424 ms | 147.054 / 862.871 ms |

Independent delivery improved median collection time by 4.88× in this run.
Concurrent DDL did **not** improve; these 12-sample tail estimates are noisy,
and p95 is the maximum observation. This result does not establish a DDL latency
win or a production throughput limit. Both modes issue the same number of
collections and DDL pairs, starting together; their completion times differ.

All three data nodes remained alive. The compact control view was 8,035 bytes
in one page; diagnostics were 46,148,032 bytes in 89 pages, at most 512 KiB each.
Cold capture timings were 701.828 ms and 1,766.752 ms respectively. The inventory
still consists of synthetic groups outside authoritative table placements.

Two earlier attempts aborted before measured samples with HTTP 503 `NotLeader`:
once during tenant creation after baseline upload, and once during the first
warmup DDL. Logs showed roughly two-second metadata rounds while materializing
the full inventory. The harness now creates the tenant before baseline upload
and explicitly waits for metadata leadership and successful catalog reads to
stabilize. It records readiness separately (1,814.743 ms in the successful run),
within 8,240.496 ms of total setup after server startup. Initial baseline apply
and full cache reads remain inventory-sized; this experiment does not claim to
remove their transient availability cost.

Final verification passed all 28 selected E2Es in 150.01 seconds, with no skips,
and all 69 server/storage-contract build steps. Focused catalog/API/transport/
report selections passed 207 executions; metadata service passed 131, data
catalog 14, compiled storage contracts 36, HA seed two, and BFS provenance one.
Selections overlap and were checked as the corresponding changes landed. The
storage inventory audit, generated-source checks, 17 Python audit tests, Ruff,
Zig formatting, and whitespace checks passed. The native restore, graph path,
diagnostic ownership, report recovery, and public-name regressions found while
integrating main were fixed without weakening shutdown or disk-space guards.


## Resumable inventory and control isolation — 2026-09-13

[Raw runs, retries, rejection counts, and provenance](system_catalog_control_workloads_2026_09_13.json)
retain the baseline, two completed intermediate runs, and the final run.
The baseline binary is from `821b5007b`; final source is `2faa02d71`. Both include
main `c1a39a3ae`. These are macOS ARM64 Debug builds with Zig 0.16.0, three
metadata voters, three real data nodes, and a synthetic 10,000-group inventory
with one embedding index per group. No Zig compilers were detected before the
final run, and no task builds or tests ran during it.

### Large inventory recovery

The 46,140,485-byte ordinary baseline request returned HTTP 413 on the previous
binary. Resumable generation publication succeeds on the final binary: 157 chunks
plus prepare and activation, each HTTP body at most 296,029 bytes. Client-side
planning took 2.235 seconds; delivery took 64.543 seconds, including discovery;
total recovery took 66.778 seconds. Activation took 442.199 ms, including Raft
commit scheduling. It publishes the root/header/cursor atomically rather than
rewriting the inventory. Timing is not constant: an intermediate run recorded
47.295 ms for activation.

The harness probes metadata endpoints in order for each baseline command. The
159 logical commands required 318 HTTP attempts: 159 follower rejections and
159 successful attempts. All attempts and response bodies are retained. Accepted
request bodies totaled 46,256,442 bytes; actual attempted request bodies totaled
92,512,884 bytes. Production endpoint affinity can avoid that repeated discovery;
these measurements do not isolate server apply time from client planning,
encoding, discovery, and Raft durability.

### Control traffic during diagnostics and report bursts

Five measured idle and five contended control captures use the same workload.
Each contended capture starts 150 ms after four simultaneous diagnostic requests.

| Control capture | Before p50 / maximum | Final p50 / maximum |
| --- | --- | --- |
| Idle | 245.307 / 1,622.921 ms | 52.360 / 53.252 ms |
| During diagnostics | 2,140.435 / 3,926.267 ms | 53.092 / 55.944 ms |

Median contended control latency improved 40.3× in this run. All ten captures
returned 200. All three real data nodes remained alive throughout the final
workload, and the post-baseline catalog readiness check succeeded.

Each burst size has one warmup and five measured publications. Control capture
immediately follows the publication; latencies are measured separately.

| Changed groups | Publish before p50 / p95 | Publish final p50 / p95 | Control before p50 / p95 | Control final p50 / p95 |
| --- | --- | --- | --- | --- |
| 32 | 65.862 / 78.249 ms | 102.850 / 509.459 ms | 293.339 / 2,335.109 ms | 92.719 / 96.276 ms |
| 33 | 116.424 / 1,723.856 ms | 378.368 / 479.920 ms | 1,558.274 / 2,211.989 ms | 92.738 / 93.168 ms |
| 128 | 363.893 / 1,824.210 ms | 552.722 / 964.074 ms | 1,637.942 / 2,333.331 ms | 186.897 / 608.967 ms |

All 15 final burst publications succeeded on their first endpoint attempt, and
all 15 following control captures returned 200. The 33-group first-reader cliff
is gone. Publication medians did **not** improve; this result supports control
isolation and bounded recovery, not a blanket write-latency improvement. With
five samples, p95 is the maximum and tail estimates are noisy.

### Admission capacity and limitations

The first separated control lane reserved only 32 MiB, enough for two maximum-size
captures. A completed run with that budget recorded one contended control 503 and
two control 503s after 33-group bursts. Their response bodies were not captured by
that harness version. The deterministic fan-in regression now covers seven
concurrent control reservations alongside a retained page and a full diagnostic
reservation. The final lane reserves 128 MiB, allowing up to eight maximum-size
captures, and the final benchmark has zero control failures across all 25 probes.

Diagnostics keep their independent 96 MiB budget and a 64 MiB maximum view.
The aggregate encoding/reservation cap is 224 MiB; buffers allocate their exact
encoded size rather than preallocating the reservation. Admission remains
conservative: 17 of 20 final diagnostic probes returned the explicit capacity
503 in 1.164–2.030 ms, while three completed in 712–725 ms. The previous binary
completed nine and rejected eleven diagnostic probes, with rejections taking
roughly 0.9–5.8 seconds. This trades diagnostic concurrency for bounded work and
prompt overload responses. It does not claim improved diagnostic throughput.

Synthetic groups are outside authoritative table placement, so the roughly 8 KiB
control view is not a size estimate for 10,000 active shard placements. These
small-sample, single-host development runs are not production capacity claims.

Validation passed 57 selected E2Es in 210.34 seconds, then six distributed
recovery/status/drain E2Es in 70.21 seconds after the admission adjustment.
The HA fixture always uses a cluster identity above the signed 64-bit range.
All 394 HA unit tests, 99 CLI tests, 35 graph-maintenance command tests, 33 OpenAPI
contract tests in both test roots, and four snapshot-transfer tests passed.
Catalog/store/API/transport, metadata-service, compiled-storage, transaction
allocation-failure, and production DataServer simulation regressions also passed.
OpenAPI generation, generated-file consistency, test-ownership audit, lint,
formatting, and diff checks passed. This is local validation; hosted CI is
reported independently on the PR.

## Batched recovery and initial collection isolation — 2026-09-14

[Raw measurements](system_catalog_batched_workloads_2026_09_14.json) record source
`d5af4ffd4`, including main `3554f8210` (standby rename, PR #721). All three live
scenarios used the same Debug binary on macOS arm64, three metadata nodes, and
three real data nodes, with no task-owned builds or tests running concurrently.
Each recovery ran once; steady scenarios used one warmup and five measurements.
An earlier interrupted run is retained separately and excluded from comparisons.

| 10,000-group recovery (46.1 MB) | Sequential chunks | Batched chunks |
| --- | ---: | ---: |
| Delivery time | 67.77 s | 40.99 s |
| Logical requests | 159 | 34 |
| HTTP attempts, including discovery | 318 | 68 |
| Maximum request bytes | 296,029 | 1,480,895 |
| Activation request | 45.59 ms | 468.76 ms |
| Contended control median | 50.92 ms | 51.43 ms |
| Successful control probes | 25/25 | 25/25 |
| Successful diagnostic probes | 2/20 | 2/20 |

Batching reduced recovery time by 39.5% (1.65×) and logical request count by
78.6%. Both uploads contained 157 logical chunks. Activation was slower in the
batched run; a single recovery per configuration cannot establish its distribution.
Python planning took 2.24/2.29 seconds, separately from delivery. Diagnostic
capacity rejected 18/20 competing captures in each run; this is not a diagnostic
throughput improvement. Durable burst publication medians for 32/33/128 changed
groups were 96.6/117.5/727.1 ms sequential and 88.6/199.4/596.8 ms batched. These
bursts use the same sparse path, so their mixed movements are not a batching win.

An index-heavy recovery with eight groups and 1,200 indexes each transported
9.76 MB through 24 durable fragments and 26 requests. It completed in 7.67 seconds
with a 699,999-byte maximum request; planning took 3.47 seconds. All real data
nodes survived, and every control probe succeeded. Subsequent eight-group
publication took 996 ms median. Fragment completion still materializes a whole
group once at admission and once at apply; its memory/CPU cost scales with group
size, bounded by the generation limit and serialized admission.

The placement component benchmark compares the old nested scan with production
indexed annotation for 10,000 groups and 20,001 intents. One warmup and five samples
gave medians of 1,695.53 ms and 5.681 ms (298.5×). The indexed measurement includes
building and freeing the map; both implementations must produce identical output.
This Debug CPU comparison excludes collection I/O, HTTP, and hosted Raft work.

The production worker now owns initial group/index collection, capacity observation,
preparation, and publication. Registration and placement reconciliation stay on the
control owner. Native regressions stall both collection and upload while 2,000
scheduling attempts remain responsive, reject a raced ownership generation, and
verify cancellation/join. The HTTP harness drives ingestion directly without the
production worker quantum or backoff. Synthetic groups are outside actual placement;
these measurements do not establish hosted-shard capacity or production SLOs.

Merged validation: 54 catalog/resolution/standby E2Es passed in 176.88 seconds;
423 standby storage tests, the standby command suite, 79 CLI tests, 127 standalone
tests, and both production DataServer simulations passed. The simulations cover
public writes, failover, restart, split, and merge. Worker regressions and generated
file checks passed. Hosted CI status is reported separately on the PR.


## Reporting worker and schema migration progress — 2026-09-14

Source `64a11bf6d44082967e3c9fc86671b0eae5de033e`, based on main
`3554f821010a7e4b9ac01b30378322ef79abb53f`; local macOS arm64, Debug.
[Raw samples, commands, executable hashes, and limitations](system_catalog_reporter_workloads_2026_09_14.json)
are retained. Benchmark commands ran sequentially after validation, with no
concurrent task-owned builds or E2Es.

| Component workload | Comparison median | New path median |
| --- | ---: | ---: |
| Unchanged heartbeat, 1,000 groups × 32 indexes | Full-runtime preparation: 4.552 ms | Retained: 0.922 ms |
| One changed group, 1,000 groups × 32 indexes | Full-runtime preparation: 4.393 ms | Retained: 0.877 ms |
| Unchanged heartbeat, 10,000 groups × 32 indexes | Full-runtime preparation: 47.066 ms | Retained: 10.431 ms |
| One changed group, 10,000 groups × 32 indexes | Full-runtime preparation: 47.542 ms | Retained: 8.294 ms |
| Migration readiness, 100 tables / 2,000 groups | Former nested scan: 1,610.391 ms | Indexed: 63.639 ms |

Heartbeat preparation uses one warmup and nine samples per case, with
`c_allocator`. It measures preparation only and checks the number of replacement
leaves. The comparison uses ordinary full-runtime preparation in the **new** binary,
including its activity scan; it is not an exact old/new retained-heartbeat comparison.
The fixture repeats 32 index descriptors per group. HTTP, Raft, destruction, and
fixture construction are excluded.

Schema readiness uses one warmup and five paired samples with
`std.testing.allocator`. The former scan is retained as a reference and every
result is checked for equality. The indexed collector is **25.3× faster** in this
case, including schema parsing and map construction. Result destruction, HTTP,
Raft, and document rebuilding are excluded. The 100 ready records require two
batches under the 64-record bound; after replicated acknowledgement, the delta is
empty and sends no request. These request counts are derived from the algorithm.

Functional validation on the rebuilt server passed seven cluster/migration E2Es
in 61.44 seconds, including rebuilding existing documents under a new schema,
64-record batch apply/replay, invalid batches, and the 16 KiB HTTP body limit.
Both production DataServer simulations and 11 reporting/snapshot regressions
passed without leaks. The catalog storage, API, and progress targets and generated
consistency checks passed. Functional durations are not production latency claims.

## Report cache and replica migration finalization — 2026-09-14

Source `0528208791beabb5ccc255bae0bfa27a45c44731`, including main
`2032ed7d871ee8bbb8bdaf31824677ec045df556`; macOS arm64, Debug, `c_allocator`.
[Raw samples, commands, executable hashes, and limitations](system_catalog_scheduling_workloads_2026_09_14.json)
are retained. Runs were sequential, without concurrent task-owned builds or E2Es.
Benchmark steps always execute fresh samples, including when a normal test run
previously populated Zig's run cache.

| Component workload | Former path median | New path median | Ratio |
| --- | ---: | ---: | ---: |
| Runtime cache, 1,000 groups × 32 indexes | Deep copy/free: 11.473 ms | Retain/release: 1.075 ms | 10.7× |
| Runtime cache, 10,000 groups × 32 indexes | Deep copy/free: 114.894 ms | Retain/release: 11.642 ms | 9.9× |
| Finalization, 100 migrations / 1,000 groups / 3,000 placements | Nested scans: 2,779.695 ms | Shared readiness index: 2.127 ms | 1,306.9× |

The cache comparison uses one warmup and nine samples, emitted in sorted order.
Capture and release are timed; fixture and publisher construction are excluded.
The publisher retains the acknowledged leaves throughout each measurement, so
final leaf destruction is outside the interval. The fixture repeats 32 index
descriptors per group. This measures cache ownership costs, not heartbeat latency.
Production clean Raft reporting ticks now use the cached heartbeat path; ownership
changes, dirty inventory, and the full-refresh interval still trigger collection.

Finalization uses one warmup and five paired samples. Each sample checks every
indexed answer against the former scan, including an additional unhosted table.
The new interval includes schema parsing, map construction, and equality checks;
the reference excludes schema parsing. New index destruction is excluded, while
the reference includes destruction of each temporary host list. These are local
component comparisons; HTTP, Raft, storage I/O, and document rebuilding are excluded.

The merged server passed all 10 catalog, migration, and drain E2Es in 96.90 seconds.
Three tables with two shards and three replicas each completed concurrent schema
cutover and document validation in 4.225 seconds, excluding provisioning. This is
one functional measurement, not a latency distribution. The scenario caught two
causes of stalled cutover: whole-table status probes discarded healthy local facts,
and leader-only repair left follower indexes rebuilding. Group-scoped observations
preserve retry debt; exact schema-index repair now runs on every hosting replica
with at most one attempt per startup-maintenance pass.

Catalog storage, API, progress, and finalization targets passed on merge
`98f043a7b31470333bb374867361a1bee776f496`, as did both production DataServer
simulations and 12 reporting/runtime regressions without leaks. Three targeted
repair-scheduler and sibling-isolation tests passed before the merge. Generated
checks passed after the merge. The subsequent timing-source commit changes only
benchmark step execution; server code is identical to the validated merge.

## Resumable fair schema reconstruction — 2026-09-14

Full-text repair now persists bounded snapshot pages and resumes its durable cursor
across reopen. Structural configuration downgrades its exclusive owner lease before
reconstruction, allowing foreground reads and Raft apply under the same storage
generation. A fair queue retains completed proofs and rotates only started work.
The 25 ms slice and 100 ms pass budgets are cooperative: storage I/O can exceed them.

The completed-prefix component benchmark uses the production queue and a boolean-array
reference, including allocation and destruction. One warmup and five samples used
Debug mode with `c_allocator` on macOS ARM64; no compiler jobs were observed at start.

| Groups | Prefix scan median | Queue median | Modeled owner inspections, before → after |
| --- | ---: | ---: | ---: |
| 1,000 | 0.584 ms | 1.803 ms | 500,500 → 1,000 |
| 10,000 | 57.412 ms | 17.878 ms | 50,005,000 → 10,000 |

The queue has higher CPU overhead at 1,000 groups in this cheap boolean reference.
At 10,000 groups its measured CPU cost is 3.21× lower. The modeled inspection counts
exclude actual owner I/O, HTTP, Raft, route discovery and the old one-second scheduling
interval; this is not an end-to-end migration speedup.
[Raw component samples](system_catalog_schema_queue_2026_09_14.json).

The real workload uses fresh three-metadata/three-data-node clusters, a 10,000-document
large tenant, five small tenants, and three replicas per shard. Schema changes run
alongside closed-loop lookup, full-text search and write traffic. It checks every
acknowledged write and the final search totals. Provisioning is outside the workload
interval; raw request latencies and all failed runs are retained. This measures
coexistence and cutover fairness, not a fixed-offered-load production SLO.

Investigation found two additional failure causes: dropped ReadIndex requests could
consume the entire read deadline, and concurrent control snapshot captures could
exhaust retained-transfer admission. The implementation retransmits one read identity
until its first quorum proof, coalesces control snapshot refreshes, retries capture
admission under the caller's budget, and releases tokens under a separate bounded
cleanup budget after cancellation. A capacity refusal during routed batch validation
retains its pre-proposal outcome instead of becoming an ambiguous write.
[Raw investigation runs](system_catalog_schema_investigation_2026_09_14.json) include
failed baseline and intermediate candidate runs. The first baseline attempt used an
incorrect executable basename and failed fixture routing during setup; it is excluded
from comparisons. Repeated JSON printed by pytest failure assertions is deduplicated
by exact canonical equality. None of these failed runs is a successful latency sample.

A four-client intermediate run also exposed that the ordinary repair queue could
select a schema-migration intent with its former 15-second quantum. Both queues now
use the same 25 ms reconstruction quantum. That run had a forwarded-write timeout;
a subsequent sample failed during setup with explicit `NoSpaceLeft` errors, after
which the remaining sample was stopped. These are retained as investigation history,
not final measurements. The harness now records free disk space and requires at least
8 GiB before each cluster sample; this does not reserve space against other processes.

The shared-quantum intermediate candidate had no foreground request failures in
three four-client samples, but two failed final full-text totals: 11,148 vs 10,172
and 10,801 vs 10,069. These overcounts were four and three 244-document pages.
A deterministic regression reproduced the crash boundary between durable page data
and its separate repair-intent cursor: three source documents left four live index
entries. Query result deduplication masked this in the original small test, so the
regression now checks physical live count and a one-hit query as well. Resumable
pages upsert into the private candidate before publication, and defer compaction to
the serving generation's normal merge scheduler. The original failed run remains
in the investigation artifact with the expected/actual count assertions.

### Four-client validation after page replay fix

All six page-replay candidate samples use source `5dcf4b323` and executable SHA-256
`67a0ccc697546f32e10a7c775ccc7137e7be8ac6b2f43a418a0b8df2d219a07d`.
Five passed all request, acknowledged-write, and final-total checks. One failed with
five forwarded-write transport timeouts on the large tenant; these retained their
ambiguous outcomes and were not retried by the harness. Three follow-up diagnostic
samples passed without reproducing that timeout. Their log watcher did not trigger
any stack capture. This remaining intermittent timeout prevents claiming a clean
four-client availability result; it is not evidence of data loss or a successful
latency sample.

| Sample | Result | Large cutover | Slowest small cutover |
| --- | --- | ---: | ---: |
| Primary 0 | Passed | 8.376 s | 8.031 s |
| Primary 1 | Failed: write timeouts | 15.058 s | 15.245 s |
| Primary 2 | Passed | 10.693 s | 7.431 s |
| Diagnostic 0 | Passed | 9.746 s | 6.844 s |
| Diagnostic 1 | Passed | 8.438 s | 31.462 s |
| Diagnostic 2 | Passed | 9.014 s | 6.906 s |

These are six fresh-cluster observations with no discarded warmups. The diagnostic
runs include log polling and are functional validation, not an isolated timing
comparison. The 31.462-second small-tenant observation also shows that queue fairness
alone is not an end-to-end cutover latency bound: metadata publication and replica
readiness remain on the completion path.
[All page-replay stress samples](system_catalog_schema_stress_2026_09_14.json).

### Paired comparison and native error transport

A subsequent one-client comparison used the same `5dcf4b323` executable, one warmup
and three measured runs per binary. The baseline passed two of three measured runs
and failed its warmup; the candidate passed two of three measured runs and its warmup.
Unrelated worktree compiler jobs ran concurrently, so this is not an isolated latency
comparison and does not establish an end-to-end speedup.

The failed candidate returned HTTP 500 because `CatalogRoutingSnapshotTimeout` was
not classified by the native error ABI. The public write handler already distinguishes
that admission failure from an unknown proposal outcome. The ABI now preserves the
exact timeout, unavailable and projection-refresh identities, using appended detail
values; it does not turn ambiguous writes into safe retries. A foreign-dispatch
regression covers all three admission failures and both ambiguous write outcomes.
This fixes error transport, not the availability of catalog routing under pressure.
[All paired observations before the error-transport fix](system_catalog_schema_comparison_2026_09_14.json).

### Post-merge qualification — 2026-09-15

After merging `origin/main` at `0fb01a4ad` (#728) and adding stable native catalog
routing error identities, the production source was `d9af5068f` (the subsequent
`851f959fc` changes tests only). Three fresh four-client samples all failed
availability assertions. No task-owned build or other workload ran concurrently;
the host was shared and was not an isolated benchmark machine.

| Sample | Failure |
| --- | --- |
| 0 | Two lookup requests exceeded their deadlines (HTTP 504). |
| 1 | A public write exceeded the client's 10-second transport timeout. |
| 2 | Two writes returned explicit retryable unavailability (HTTP 503). |

The failed cluster roots were retained. Sample 0's metadata leader logged a
linearizable-read timeout with equal commit and applied indexes. That observation
does not prove the quorum request or response path was healthy, and does not
establish a root cause. The current branch therefore has **no clean four-client
availability qualification**. The repair and native-boundary regressions demonstrate
their specific fixes; they do not resolve or excuse these remaining workload failures.
Neither the harness nor the implementation retries ambiguous writes to make a sample
pass. [All post-merge observations and binary identity](system_catalog_schema_post_merge_2026_09_15.json).

Post-merge functional verification passed: 30 catalog/resilience/backup/migration
E2Es (196.62 s), all 129 standalone runtime tests, eight compiled storage-owner
source tests, and three compiled write-boundary/full-text replay regressions.
Generated-file checks, regenerated Go SDK tests, Python lint/format, and patch
whitespace checks passed. These checks cover correctness and integration separately
from the failed four-client qualification above.

## Atomic rebuild pages and table-specific validation — 2026-09-15

The resumable full-text builder now publishes a page's segments and its cursor
in one candidate transaction. Restart uses this cursor even when the separate
repair-intent cursor lags or the scheduling policy changes. This removes the
page-by-page scan of every older document ID. Append publication also carries
forward unchanged field totals instead of reparsing older segment headers.

The first component comparison used Debug and `std.testing.allocator`, 256
rows/page, freshly created indexes, and one sample per size/path. Segment
construction and final exact-count verification are outside the timed region.
The production file-backed layout includes segment and metadata durability.
These initial measurements precede the incremental field-total optimization.

| Documents | Previous delete-then-publish | Atomic page | Reduction |
| --- | ---: | ---: | ---: |
| 16,384 | 3.414 s | 3.043 s | 10.9% |
| 65,536 | 16.904 s | 12.969 s | 23.3% |
| 131,072 | 38.249 s | 29.402 s | 23.1% |

An exploratory in-memory LSM run embedded the segment payloads in metadata and
showed only 4–11% reductions; its allocation/copying cost is not representative
of production file-backed publication. Both layouts and binary hashes are
retained in `system_catalog_rebuild_pages_2026_09_15.json`. Every exact live-count
assertion passed. These are component observations, not migration latency or
production throughput claims.

The final implementation was measured again with incremental append field totals
enabled on both paths. At 16,384 / 65,536 / 131,072 documents, delete-then-publish
took 2.805 / 14.604 / 37.920 s and atomic pages took 2.542 / 12.126 / 29.372 s
(9.4% / 17.0% / 22.5% lower). These remain single component samples; the small
differences between the initial and final atomic-page runs do not establish an
independent benefit from the field-total change. The final regression also
verifies field totals after reopen and another committed page.

Run the page regression and optional measurements with:

```sh
zig build persistent-rebuild-page-test
ANTFLY_BENCH_REBUILD_PAGE=1 zig build persistent-rebuild-page-test
```

The real catalog workload now also measures `qualified_batch_validation`: one
schema-constrained, full-index write held fixed while unrelated table count
grows. This complements the multi-tenant migration workload with concurrent
lookups, searches and writes. A representative command is:

```sh
uv run --project e2e/antfly python tools/benchmark_system_catalog.py \
  --scenario catalog --deployment cluster --table-counts 10 100 \
  --schema-fields 32 --samples 20 --warmup 2 --output catalog-validation.json
```

The final production binary passed all three measured four-client migration runs;
the pre-change binary passed two of three. The failed baseline reported two
unknown forwarded-write outcomes. Every candidate run verified acknowledged
writes and final index counts, without retrying ambiguous writes. Large-tenant
completion was 8.348 / 7.175 / 12.687 s for the candidate and 7.955 / 10.506 /
7.553 s for the baseline (the first baseline run failed availability checks).
The candidate's slowest small-tenant cutover was 6.739 / 6.392 / 12.874 s. These
variable closed-loop samples do **not** establish a migration latency speedup or
prove that every previously observed availability failure is eliminated. The
maximum candidate write latency across the three runs was 887 ms; the baseline
included two unknown outcomes and a successful run with a 6.319 s write.

The comparison used Debug binaries, 10,000 large-tenant documents, five 20-document
small tenants, four clients, and three metadata plus three data processes with
three replicas. Runs alternated serially with no task-owned compiler or other
benchmark running. No warmup runs were performed; all six measured runs were retained. Raw observations, failures, source
identity, binary hashes and free disk space are retained in
[the comparison artifact](system_catalog_schema_validation_2026_09_15.json).

Integration testing exposed and fixed two additional gaps: missing scoped schema
PUT/PATCH routes, and standalone's missing bounded table-descriptor projection.
The standalone migration then exposed an awake/native-monotonic clock mismatch on
Darwin; translating the remaining scheduler budget resolved it. The previously
240-second-timeout migration completed its test body in 1.25 s after that fix.
Thirty catalog/resilience API cases passed before the clock fix, and both the
standalone migration and scoped schema-validation regression passed afterward
(7.75 s combined, including setup). The final stress runs above exercise cluster
repair with the translated deadline.

Focused checks passed: nine storage-owner tests, 129 standalone runtime tests,
79 read/write contracts, two crash/resume lifecycle regressions, and the atomic
page regression plus optional scaling benchmark. Catalog projection/validation
suites passed earlier in this update. Production builds, generated-file checks,
Go SDK tests, and changed Python lint/format passed.

The schema-constrained catalog workload completed on the candidate at both 10
and 100 tables (32 extra string fields plus body/customer fields, 20 measured
requests per operation, two warmups). It checks full-index writes, lookups,
qualified queries/joins, NDJSON, listing, concurrent reads and rename identity.
The baseline completed 10 tables but failed during the 100-table measured write
phase with HTTP 409 / unknown write outcome; no successful baseline latency
comparison is available. Its failure is preserved explicitly in
[the baseline observation](system_catalog_validation_baseline_2026_09_15.json).
The harness now writes failure/interruption artifacts automatically.

| Candidate operation | 10 tables p50 / p95 | 100 tables p50 / p95 |
| --- | ---: | ---: |
| Schema-constrained full-index write | 239.11 / 267.32 ms | 259.26 / 631.91 ms |
| Sequential qualified lookup | 26.56 / 32.67 ms | 28.20 / 57.61 ms |
| Qualified lookup, 8 clients | 54.68 / 107.73 ms | 48.80 / 7,011.86 ms |

All operations completed, but the 100-table concurrent read phase had a 21.46 s
maximum and only 7.03 requests/s, versus 121.54 requests/s at 10 tables. This is
an unresolved read-tail performance concern, not a satisfactory scale-latency
qualification. The write-validation changes do not establish a fix for that
bottleneck; it requires a separate profile of routing/control admission and
storage under concurrent reads. These are observations from one fresh cluster
per binary, not production SLOs. [Candidate results and binary identity](system_catalog_validation_candidate_2026_09_15.json).


### Remote read routing and concurrent lookup tail (2026-09-15)

The prior 21.46 s tail was reproduced twice. Stack samples captured waiting
lookups in `dataReadRouterGroupLeaderNodeId → remoteAdminSnapshot →
fetchPagedSnapshot → waitBeforeMetadataMutationRetry`. Multiple readers spent
an entire three-second sample in diagnostic snapshot admission backoff. Resolving
one remote shard leader was independently capturing the full administrative
inventory for each cache miss.

The read router now retains a compact endpoint/placement/leader index from the
accepted control snapshot. It coalesces cold refreshes, pins one view for scalar
or fanout routing, retains the local-leader fast path, and uses readable peer IDs
for missing-document fallback. Destination topology and consistency checks remain
mandatory. No diagnostic snapshot is requested by these production routing paths.

A fresh before/after pair explicitly chose a nonmember coordinator at both 10 and
100 tables (`--catalog-ingress nonmember`). Each run used 32 extra schema fields,
20 sequential samples, two warmups, and eight clients making 20 lookups each.
The exact pre-fix binary (its hash matches the previous reproduced slow run)
ran before the final candidate. Both completed every operation and correctness
check. Obsolete task cache artifacts were removed between candidate checkpoints
during provisioning to preserve the production disk safety floor. No profiler, task-owned compiler, or other task-owned workload ran during
either measurement. Other worktree jobs were present; these are local Debug
observations from one fresh cluster per binary, not isolated production estimates.

| Tables | Concurrent lookup metric | Before | After |
| --- | --- | ---: | ---: |
| 10 | p50 / p95 | 44.20 / 57.18 ms | 47.65 / 83.28 ms |
| 10 | Maximum | 7,994.66 ms | 218.88 ms |
| 10 | Throughput | 18.19 requests/s | 150.86 requests/s |
| 100 | p50 / p95 | 50.46 / 546.74 ms | 48.21 / 85.76 ms |
| 100 | Maximum | 24,766.47 ms | 133.34 ms |
| 100 | Throughput | 6.22 requests/s | 155.91 requests/s |

The 100-table run's observed throughput increased 25.1× and its longest lookup
fell from 24.77 s to 133.34 ms. Median latency stayed similar; the 10-table p95
was higher. An earlier candidate run also removed the multi-second stalls but
retained a 924.74 ms maximum at 100 tables. The final candidate additionally starts
peer-view freshness at publication, so a slow control capture cannot publish an
already-expired routing view. The final concurrent phases lasted only about one
second each; this pair does not characterize repeated refresh cycles or establish
uniformly low latency under sustained load. It demonstrates removal of the
observed diagnostic-admission convoy, not the absence of other scale bottlenecks.

Raw records, coordinator/placement identities, settings, and binary hashes:
[`system_catalog_read_routing_baseline_2026_09_15.json`](system_catalog_read_routing_baseline_2026_09_15.json)
and [`system_catalog_read_routing_candidate_2026_09_15.json`](system_catalog_read_routing_candidate_2026_09_15.json).

### Request budgets, peer publication, and sustained routing (2026-09-15)

Read-peer refresh now shares the request's remaining deadline and cancellation
through cache admission, clock translation, metadata endpoint selection, and HTTP
transport. Query, preflight, text-statistics, and aggregation fanouts resolve all
destinations from one retained peer view per phase, including sequential dispatch.
Join forwarding deducts routing time from its original timeout. The shared control
snapshot refresh can return a retained peer index directly, avoiding the full
catalog/schema result clone; retired observations are freed outside the cache lock.

The Debug component workload compares both result modes in the same implementation:
1,000 tables, 8 KiB schema payloads, one warmup pair and seven measured pairs.
Median publication/result construction fell from 376.95 ms for an owned snapshot
to 200.90 ms for a retained peer view (46.7%). Incoming observation construction
and caller result destruction are excluded; retirement of the previous observation
is included. This uses the checking test allocator, so it isolates redundant work
rather than predicting production HTTP latency or throughput.
[Raw component samples](system_catalog_peer_publication_2026_09_15.json).

Validation passed with no skips: 81 table-read contracts, six focused peer-routing
and fanout contracts, the modeled three-DataServer merge/split/failover/restart
scenario, public API smoke, both dense-storage regressions, six standalone
transaction/catalog E2Es, and two three-data-node catalog E2Es. Fixture fixes use
physical table identities for activity waits, structured candidate-key assertions,
and the new write-validation protocol in the modeled metadata adapter. The dense
artifact rebuild functional test uses the existing completion-test budget rather
than a production scheduling quantum. The earlier dense catch-up checkpoint and
transaction-transform HTTP 503 failures did not reproduce locally; these passes
do not establish that their Linux CI failure modes are fixed.

The sustained cluster pair uses the previous `093d95d8f` binary as baseline and
`e399de8b3` as candidate. Both already contain the shared peer-routing cache;
this comparison measures the subsequent budget, publication, and fanout changes.
Each fresh three-data-node cluster uses 32 extra schema fields, 20 sequential
samples, two warmups, and eight concurrent clients reading for at least 30 seconds
at both 10 and 100 tables. The harness verifies a nonmember ingress node and every
lookup body, and also checks writes, queries, joins, NDJSON, listing, and rename.
Both binaries completed every operation without errors.

| Tables | Concurrent lookup metric | Baseline | Candidate |
| --- | --- | ---: | ---: |
| 10 | Completed lookups | 5,401 | 4,582 |
| 10 | p50 / p95 | 47.53 / 78.51 ms | 52.30 / 82.30 ms |
| 10 | Maximum | 300.51 ms | 444.61 ms |
| 10 | Throughput | 179.78 requests/s | 152.35 requests/s |
| 100 | Completed lookups | 3,185 | 3,497 |
| 100 | p50 / p95 | 52.68 / 129.76 ms | 52.20 / 107.39 ms |
| 100 | Maximum | 1,911.27 ms | 1,163.72 ms |
| 100 | Throughput | 105.96 requests/s | 116.45 requests/s |

The 100-table candidate improved observed p95, maximum, and throughput, while the
10-table candidate was slower. These samples therefore do not establish a uniform
end-to-end speedup. The candidate's 1.16-second maximum also leaves a sustained
read-tail qualification gap. No task-owned compiler, profiler, or second workload
ran during measurement; other worktrees had active tests/builds. These are local
Debug observations, not isolated production SLO measurements. The 30-second phases
cover many peer-cache freshness intervals, unlike the earlier one-second runs.
[Baseline records](system_catalog_sustained_routing_baseline_2026_09_15.json) and
[candidate records](system_catalog_sustained_routing_candidate_2026_09_15.json)
include per-request timings, exact binary hashes, and coordinator/placement IDs.

The eight-shard catalog workload uncovered an additional correctness gap. Join
coordinators called the provisioned source's local-only exact-group callbacks,
causing `UnknownGroup` when a right-hand shard belonged to another node. Remote
forwarding also needs a catalog fence selected for that exact table/group. The
final implementation routes coordinator calls through the hosted adapter and
binds the selected fence; fenced worker callbacks retain the existing resident
storage and admission owners. Caller deadlines are translated into the catalog
clock for selection and into the native clock for fence admission.

The Zig regression starts from an unbound catalog, checks that a remote typed
probe does not attempt local admission, and requires fence acknowledgement for
its HTTP fallback. The Python regression covers both lookup and broadcast joins,
qualified targets versus literal lookalike names, and one/eight shards. Single-
replica placement ensures the cluster cannot hide the bug behind local replicas.
Both standalone cases and all four cluster catalog cases passed; the 81 read
contracts and production build also passed after the complete fix.

The final `b66389f3a` binary completed the full ten-table/eight-shard workload,
including all 20 samples and two warmups for every operation. The baseline and
initial candidates failed during joins, so no successful before/after fanout
latency comparison is available.

| Final eight-shard operation | p50 | p95 | Maximum |
| --- | ---: | ---: | ---: |
| Qualified query | 79.84 ms | 107.90 ms | 112.86 ms |
| Qualified join | 277.72 ms | 416.92 ms | 8,129.14 ms |
| 20-line NDJSON query stream | 1,430.51 ms | 8,484.84 ms | 9,018.71 ms |

The join and NDJSON outliers remain a performance qualification gap despite all
requests completing correctly. This workload does not establish bounded low-tail
latency or a fanout speedup. It does establish that the new real-work scenario
caught a routing/fencing failure missed by single-shard and fully replicated tests.
No task-owned compiler or other task-owned workload ran during these measurements.
Shared-host Debug limitations remain the same as for the sustained pair above.

Raw observations: [baseline failure](system_catalog_fanout_baseline_2026_09_15.json),
[initial candidate failure](system_catalog_fanout_initial_candidate_2026_09_15.json),
[missing-fence failure](system_catalog_fanout_unfenced_candidate_2026_09_15.json), and
[complete candidate workload](system_catalog_fanout_candidate_2026_09_15.json).


## Retained join planning and clock-safe fence narrowing (2026-09-15)

The planner now acquires a compact immutable generation from the control-read
cache, retaining it once per public query. Indexed table statistics, shard IDs,
and sorted key ranges replace diagnostic snapshot copying and repeated full
inventory scans. Generations contain no schemas or index definitions. Cold
control refresh still transfers the existing control snapshot; these measurements
do not claim to eliminate that transfer. Inherited admission fences preserve
their clock when narrowed by a request using another clock.

The same Debug test binary compared warm diagnostic snapshot clone/release with
planning retain, indexed table/key lookup, and release. With 1,000 tables, one
range per table, and 8 KiB schemas, seven samples after one warmup measured:

| Component | Median | Minimum–maximum |
| --- | ---: | ---: |
| Copied diagnostic view | 372.521 ms | 371.160–376.837 ms |
| Retained planning view | 0.007 ms | 0.006–0.007 ms |

Initial control-generation publication took 32.575 ms, excluding construction of
the incoming snapshot. Cache freshness was reset outside timing for both paths.
The checking allocator amplifies allocation/free cost, and retained timings are
near timer resolution; this is evidence of work removed, not a production latency
ratio. No task-owned compiler or other workload ran concurrently.
[Raw component samples](system_catalog_join_planning_component_2026_09_15.json).

Validation after merging `fab41bb61` passed 45 join/planning tests, 81 read
contracts, 20 control-read/deadline tests, the catalog-store suite (including
main's retained-entry replay regressions), the production build, and the two
standalone one/eight-shard scoped/literal join E2Es. The merged paginated transfer
fixture preserves main's distinct-clock, cancellation, clone, and publication
checks; its per-page transport timeout is five seconds inside the overall
10-second read budget.

The modeled `production-cluster-join-split-vopr-test` remains failing. A detached
`3b4f803a9` baseline with only the same fixture compatibility repairs reproduces
`TextProjectionProvenanceMismatch`, the resulting `IndexUnavailable` join failure,
and exact replay divergence. The merged candidate reproduces these failures too.
Thus the modeled active-split witness is an unresolved pre-existing qualification
gap; passing unit and real-server workloads must not be described as passing that
witness. The provenance guard remains enabled, and the test is not skipped.

The high-level pair used fresh three-data-node clusters, ten tables with eight
single-replica shards each, 32 schema fields, 20 samples and two warmups per
operation, and 20-line NDJSON requests. Both runs verified five right-hand shards
remote from coordinator 101 and validated all operation results. Provisioning was
outside timing. The candidate binary is built from `c63cde8ab`, including main
through `1faa190bd` (LSM key-bound buffer reuse); its production build and all six
focused streaming/compaction regressions passed. The baseline is the saved
`3b4f803a9` binary. This pair therefore includes upstream integration changes in
addition to planning retention; the same-binary component isolates the latter.

| Operation | Baseline p50 / p95 / max (ms) | Candidate p50 / p95 / max (ms) |
| --- | ---: | ---: |
| Qualified batch | 172.80 / 266.70 / 295.62 | 224.00 / 307.19 / 330.75 |
| Qualified lookup | 28.74 / 56.49 / 61.44 | 26.48 / 52.44 / 57.09 |
| Qualified query | 87.32 / 121.30 / 121.40 | 80.81 / 107.69 / 163.26 |
| Qualified join | 263.68 / 294.74 / 299.14 | 287.08 / 338.61 / 395.99 |
| 20-line NDJSON | 1456.72 / 1696.11 / 1743.86 | 1348.36 / 1479.67 / 1537.87 |
| Scoped listing | 63.59 / 81.11 / 84.59 | 54.16 / 140.26 / 161.24 |
| Concurrent lookup (8 clients) | 52.13 / 98.73 / 184.72 | 52.37 / 81.80 / 102.39 |
| Rename | 32.36 / 102.39 / 140.35 | 31.24 / 115.36 / 136.98 |

NDJSON and concurrent-lookup p95 improved in this pair, while join and batch
latency increased. These observations do not establish an end-to-end join
speedup or a uniform latency improvement. The small catalog does not reproduce
the 1,000-table copying workload, and there is only one paired shared-host Debug
run. No task-owned compiler, profiler, or second workload ran during either
measurement. Tail qualification under sustained load remains separate work.

[Baseline workload](system_catalog_join_planning_baseline_2026_09_15.json) and
[candidate workload](system_catalog_join_planning_candidate_2026_09_15.json)
retain exact binary hashes, settings, placements, and per-request concurrent
lookup samples.

## Authoritative join topology and bounded fanout (2026-09-15)

This follow-up pins one authoritative routing generation for the complete join,
including left-side reads, right-side group selection, and worker fences. Cached
planning statistics cannot omit a newly split range. Lookup keys are partitioned
once, and independent shard reads run in batches of at most eight. Embedded
metadata retains routing and planning indexes across queries. Deadline and
cancellation scopes survive finalizer dispatch without escaping into durable
job-store state.

The split model exposed two fixture defects: direct table creation omitted the
public API's default schema, changing posting provenance when split admission
later installed it; and the recovery oracle permanently latched a transient
initial metadata election. The fixture now initializes the canonical schema and
observes recovered topology after workload completion. Production fixes also
align wire-visible local query timing and forwarded catalog admission budgets
with their executor clocks. Metadata service adapters translate deadlines at the
native service boundary. No provenance checks or replay assertions are disabled.

The control binary is built from `3928b1d2b`, including `origin/main` through
`4da9b1cc6`; the candidate adds only this follow-up to that same base. Both are
Debug builds. The many-to-one enrichment workload checks every event identity
and customer value, forces and verifies both index-lookup and broadcast joins,
and confirms distributed execution and remote right-hand shard placement.

The same-binary lookup partition component uses 20,000 rows and 64 ranges, one
warmup pair and seven measured pairs. Repeated per-group scanning took a median
847.856 ms; partitioning once took 15.796 ms, a 53.68× reduction. Both paths
validate identical bucket counts. This isolates key classification and allocation;
it excludes topology acquisition, storage, HTTP, and result encoding, and is not
an end-to-end speedup claim. [Raw paired samples](system_catalog_join_partition_2026_09_15.json).


The initial ten-table/eight-shard baseline completed: 1,001 events and 251
customers, 20 measured requests per strategy after two warmups. Index lookup
p50/p95/max was 369.07/772.52/880.81 ms; broadcast was
652.71/716.63/6,846.47 ms. Single-document dataset construction took 334.57 s and
is excluded from these timings. An earlier bulk seed failed in cross-shard
transaction preparation; the read harness now uses individual document mutations.
The matched candidate failed during ingestion with a missing metadata identity
proof. Its unclassified callback error was incorrectly surfaced as an invalid
path parameter; stable identity-error transport and catalog-unavailability
mapping now preserve that failure correctly. This is a failed observation, not
a valid end-to-end speedup comparison.

[Successful baseline](system_catalog_join_topology_baseline_2026_09_15.json) and
[failed candidate](system_catalog_join_topology_candidate_failed_2026_09_15.json)
retain the binary hashes, settings, and outcome. No task-owned compiler, model
runner, profiler, or second workload ran concurrently with their measured phases.
These are shared-host observations; unrelated worktree activity is not controlled.

Integration with `origin/main` at `46fb5ca95` adds the vector-storage migration
work. The merge preserves main's published ABI error ordinals and appends catalog
identities, regenerates clients from the combined API, and connects online and
offline migration to catalog identity and row persistence. Three migration E2Es
passed, covering online restart, offline lock/resume, and cancellation after
restoring stale catalog state.

The subsequent main merge is `271838a19` (empty hot standby instances and
replicated table creation). Table-create WAL records now include the logical
catalog delta and revision fence as well as physical tables/ranges; standby
apply journals both atomically. Existing-create acknowledgements retain the
RemoteApply frontier requirement. All four new bootstrap/restart/outage/promotion
E2Es passed in 74.53 s. The stale-catalog case restores the row store while the
fixture is stopped, and artifact identity comes from the fixture's actual u64
cluster ID.

Exact-replay diagnosis traced a one-byte disk-usage difference to persisted
index-status timestamps read from the host clock. Full-text, dense, sparse, and
graph status snapshots now read the owning index manager's I/O clock. This keeps
persisted bytes, compression, and resulting disk telemetry in the same clock
domain. A focused regression verifies persisted timestamps at two injected clock
values; it passed without leaks. The replay oracle still compares full payloads.

The first successful final eight-shard enrichment run used two tables, 32 schema
fields, 257 events, and 65 customers. Ten samples per strategy after two warmups
returned exact event/customer matches: index lookup p50/p95/max
155.90/186.15/186.15 ms; broadcast 159.14/178.65/178.65 ms. Setup took 74.19 s and
is excluded. Five of the eight right-table ranges were remote to ingress.
This smaller workload and newer main revision are not comparable to the earlier
ten-table baseline. [Raw observation before compact identity projection](system_catalog_join_topology_before_identity_projection_2026_09_16.json).

Final review found that HTTP catalog identity fencing called full diagnostic
status twice per read, allocating projected table/range/store inventories.
The endpoint now requires a dedicated group/incarnation capability before and
after its authoritative read. A regression makes diagnostic status fail if used
and checks stable identity, replacement, and missing capability. This removes
catalog-size work from identity fencing without weakening the read barrier.

The model fixture now advances metadata consensus independently of public
workload progress, as the production metadata servers do. Previously a request
waiting for catalog authority could prevent the next control round that would
elect a metadata leader. The complete join/split target passed all 16 tests,
including bounded cancellation and exact replay, with the original 11,000- and
420,000-tick budgets. This run includes the persisted-clock fix and independent
metadata driver; qualification of the subsequent compact identity callback is
reported separately below. Assertions and fault coverage are unchanged.

The main merge through `c4b4728fa` updates CI admission and documentation;
it does not change production code or the measured binary.

The compact-identity binary completed the same two-table/eight-shard workload
with all correctness checks passing. Ten samples followed two warmups for each
operation. The comparison below reports p50/p95 milliseconds:

| Operation | Before compact identity | Compact identity |
| --- | ---: | ---: |
| Event/customer index lookup | 155.90 / 186.15 | 127.94 / 184.99 |
| Event/customer broadcast | 159.14 / 178.65 | 143.33 / 181.48 |
| Qualified document lookup | 28.56 / 60.68 | 25.13 / 29.01 |
| Qualified query | 79.77 / 104.47 | 80.06 / 108.68 |
| Single-event qualified join | 128.21 / 156.71 | 143.57 / 163.37 |
| Ten-query NDJSON request | 674.63 / 713.01 | 608.50 / 675.21 |

Setup took 75.37 s outside the measurements. Six of eight right-table ranges
were remote, versus five in the preceding run. Placement and unrelated host work
are uncontrolled, and some operations regressed; this small comparison does not
establish a uniform or causal latency improvement. The deterministic gain is
removing two full diagnostic inventory reads from each catalog read. Concurrent
lookup completed 80 requests with eight readers in 0.427 s (p50/p95
33.58/56.59 ms); that short burst does not measure sustained throughput.
No task-owned build, model, profiler, or second workload overlapped measurement.
[Final raw observation](system_catalog_join_topology_final_2026_09_16.json)
records the settings, binary hash, placements, and concurrent request samples.

The compact-identity revision also passed all 16 join/split model tests, including
exact replay and the bounded cancellation history, in an 18-minute Debug run.
The subsequent merge of `6f7df8233` (#704) brings storage publication, transport,
and VOPR hardening from main. The measurements above predate that merge and are
not timing claims for the merged binary. Integration retains main's stable error
ordinals, the catalog's host-owned retry queue, and one retry policy for scheduled
and explicitly awaited store reports. A newly included virtual-HTTP fiber test
uses leak-checked allocation without native stack capture, matching the other
fiber fixtures and avoiding macOS unwinding across switched stacks.

Qualification after the `6f7df8233` merge passed: all 16 join/split model tests
with exact replay (11 minutes), 132 standalone tests, 84 transport tests, five
virtual-HTTP tests, 69 table-read contracts plus 12 write implementation tests,
and 17 failure/error ABI tests. Four standby and three distributed resolution
E2Es passed together; three migration E2Es then passed in 14.90 s after clearing
completed task build artifacts to restore the fixture's required disk headroom.
Thirty resolution helper tests also passed. Sandbox-blocked TLS listener tests
were rerun with loopback access; disk admission and test assertions remain enabled.

The later `04df69feb` main merge adds OpenRouter provider support. Go, Python,
TypeScript, and Zig clients and the bundled UI were regenerated from the combined
catalog/provider sources. This provider merge follows the timings above; it is
not part of their binary hashes.
The final production build and all 67 catalog HTTP tests passed, along with the
Go generated client tests, nine Python index-configuration tests, five OpenAPI
path checks, and the bundled UI build/typecheck.
