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
