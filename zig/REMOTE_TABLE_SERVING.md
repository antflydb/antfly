# Remote table indexes, caching, and materialized execution

This document specifies the intended serving contract. The implementation status
below distinguishes implemented paths from remaining integration work.

Parquet and Iceberg remain the authoritative row sources. Antfly supplies durable,
snapshot-bound indexes and materializations, a bounded local cache, and one native
query planner across HTTP rows, search, SQL, and PostgreSQL wire delivery. An index
configuration is desired state; only a verified published generation is query-ready.

## Implementation status

The serving path now connects the existing persistent range cache beneath shared
RAM reads for SQL and typed HTTP rows, including Parquet files referenced by
Iceberg. Cache initialization belongs to the API owner and happens after table
read authorization. Startup failures leave source reads available and are visible
in cache stats. Versioned range keys use length-prefixed object/version/codec/column
identities. RAM and disk reserve bounded metadata/sidecar capacity; restart recovery
restores eviction classification from bounded, filename-verified key provenance,
and payload checksum validation occurs before a cached read can serve bytes.

The `lake_cache` node configuration owns `enabled`, optional `root`,
`max_memory_bytes`, `max_disk_bytes`, `max_entries`, `max_write_queue_bytes`,
`max_write_queue_entries`, and `protected_bytes`. Defaults are 64 MiB raw-range RAM,
10 GiB disk, 16,384 disk entries, and a 32 MiB / 16-entry background write queue.
The disk protected pool is capped at one quarter of total capacity; its configured
maximum defaults to 256 MiB. Without an explicit root, use
`<storage.local.base_dir>/cache/lake-ranges`, or `cache/lake-ranges` next to a Lite
file. Object-only nodes require an explicit local root. Disabling the disk tier
preserves RAM caching. Cache storage can be discarded after stopping its owner.

API request stats expose RAM hits/misses/retained bytes/evictions, disk hits/bytes,
provider range reads/bytes, disk initialization failures, and persistent queue,
eviction, corruption, and recovery counters. Provider counters describe physical
range reads; snapshot discovery and authoritative metadata revalidation may still
contact the source. Real Parquet and Iceberg API tests close and restart the cache
owner, run SQL again, and assert disk hits with zero repeated provider range reads.
Separate tests cover credential/version isolation, unversioned-read rejection,
bounded priority eviction across restart, and unavailable local cache fallback.

The following paths remain required before claiming complete remote index serving:

| Path | Current implementation | Remaining integration |
| --- | --- | --- |
| Range caching | Public SQL/rows RAM → disk → source reads | Shared artifact-reader admission and per-statement explain accounting |
| Index construction | RowSource sidecar builders, scoped artifact uploads, rebuild planning | Declared remote index scheduling, durable generation publication and public status |
| Index selection | Snapshot/column/config bindings and candidate/hydration helpers | Catalog-bound SQL/rows/search selection, complete coverage proofs and explicit-index errors |
| Algebraic execution | Native exact typed reducers; separate legacy lake fold artifacts | Versioned shared state codec, bound semantic matcher and native SQL state import |
| Incremental refresh | Immutable file identities and invalidation foundations | Per-file contribution manifests, append merging and delete-aware correction |

This table is an acceptance gate. Helper tests or a configured index alone do not
make any unfinished path query-ready.

## Query and index UX

Use the existing table/index create, get, list, delete, and maintenance operations.
Remote tables support the same declared index types where their source columns and
operators are supported. Do not introduce a parallel remote-index catalog or require
users to copy lake rows into native document storage. Unsupported configurations
are rejected when declared, with the incompatible source column/operator identified.

Index status reports configured, building, ready, stale, failed, or deleting, plus
source snapshot, schema identity, indexed coverage, generation, and build progress.
Creating an index schedules bounded background work; ordinary queries remain
available while it builds. Requests that explicitly require an index report an
unavailable/stale index rather than pretending to use it. Automatic plans fall back
to the authoritative scan. Deleting an index prevents new selections immediately;
pinned readers may finish before unreferenced artifacts are reclaimed.

Each statement pins the authorized table definition and source snapshot once.
Planning chooses among a pruned scan, an index producing external row references,
a covering projection, and a compatible algebraic materialization. Candidate
hydration uses the same physical page readers, schema rules, Iceberg delete filters,
and native typed batches as an ordinary scan. A candidate index must prove complete
coverage for an exact SQL predicate; approximate search candidates cannot substitute
for an exact SQL filter. Residual filters are still evaluated. Index selection must
preserve requested ordering, LIMIT/OFFSET semantics, and stable continuation identity.

Explain reports the selected index/materialization, generation and snapshot,
coverage, residual work, expected remote reads, RAM/disk cache activity, and a concrete
fallback reason such as missing generation, snapshot mismatch, schema mismatch,
unsupported predicate, or insufficient coverage. Existing APIs expose this through
their normal explain/status contracts; PostgreSQL uses SQL EXPLAIN.

## Publication and refresh

Reuse the RowSource sidecar builders and content-addressed artifact store. A durable
publication binds table identity, index semantic configuration, source inventory,
schema and field mapping, snapshot/delete semantics, and artifact checksums.
Publication is conditional on the current authorized catalog definition and desired
index generation. Build cancellation, dropped/recreated indexes, and source changes
cannot publish into a superseding generation. Durable manifests are the discovery
boundary across API nodes and restarts; a node-local warm cache is never readiness
or freshness authority. Build attempts have distinct fenced namespaces, and orphan
cleanup occurs after publication/reachability checks.

Refresh can reuse per-file artifacts only when immutable file versions and semantic
interpretation match. Append-only aggregate coverage may combine unchanged file
states with newly built states. File rewrites rebuild affected contributions.
Iceberg deletes require a delete-aware generation or a correction proven valid for
the reducer; MIN/MAX and distinct state cannot be repaired by subtracting a scalar.
A changed snapshot defaults to scan until compatible coverage is proved.

## Persistent cache

Connect the existing PersistentObjectRangeCache to the server-owned lake reader.
The read sequence is RAM range lease, local persistent range, verified remote read.
Decoded columns remain a separate bounded RAM tier. Both scans and sidecar artifact
reads use the same version and authorization rules. Cache storage is disposable;
statement sort/join/group spill files remain private scratch and are not published
as reusable results.

Raw range identity includes storage/authorization scope, object identity, verified
object version, and byte range. It deliberately excludes the table snapshot so an
unchanged object can be reused across snapshots. Authorize and pin/revalidate source
identity before cache lookup. Unversioned objects bypass shared persistent caching.
Entries store unambiguous length-prefixed key provenance, length and checksum.
Private temporary files and atomic publication recover from truncation/corruption
as cache misses.

Operator configuration controls root directory, RAM/disk bounds, entry count,
background write queue bytes/concurrency, and protected metadata/index lanes.
Initialize one owner per configured root. A nonblocking filesystem lock enforces
ownership; new cache directories and payload files are private to the node user.
Cache misses never block on durable cache writes; queue saturation, disk pressure, and optional cache failures preserve a
successful source read. Shutdown drains accepted work after readers quiesce.
Restart recovery validates the inventory and removes incomplete writes. One-off
broad scans must not evict the working metadata/index set. Track per-tier hits,
misses, bytes, evictions, dropped writes, and source bytes avoided.

## Algebraic materializations

Algebraic indexes store reusable reducer/expression state, not arbitrary cached
HTTP responses. SQL derives a semantic request from bound expressions, predicates,
group keys, aggregate filters, types, NULL rules and collations. Matching requires
exact snapshot coverage and equivalent semantics. HAVING, ordering and final
projection still run through the native typed expression kernels.

Use versioned typed reducer interchange shared with native execution. Preserve
wide integer sums until final SQL narrowing; AVG merges sum/count states; floating
reducers retain compensation and mean state where required. Persist SQL NULL
separately from JSON null and preserve distinct/extrema/pattern state domains.
Legacy i64-only artifacts are selected only when their narrower contract is proved
compatible, otherwise rebuilt or skipped. Decode and validate a whole state batch
before importing it. Failed admission cannot expose a partially described state.

Remote group/expression materializations are the initial exact substitution shapes.
Multi-axis folds, join materializations, sketches and other reducers follow the same
semantic/coverage proof contract; no shape is advertised query-ready before its
builder, publication, matcher, executor, and invalidation tests exist.

## Verification

Exercise real independently written plain/dictionary Parquet files through public
index creation, readiness, explain, selective hydration, SQL materialized execution,
and cold restart. Include Iceberg snapshot advancement and deletes, changed object
versions, schema changes, authorization scopes, drop/recreate races, corrupt cache
entries, interrupted publication, resource pressure and scalar fallback parity.
Warm/restart tests assert provider reads avoided, not only correct output. Benchmarks
separate source bytes/network latency, decoding, execution, and result delivery.

DuckDB's core external-file cache is memory-limited; persistent disk caching is also
available through its community cache_httpfs extension. These are useful comparisons,
not dependencies of Antfly's native execution:

- [DuckDB external-file cache](https://duckdb.org/2025/05/21/announcing-duckdb-130#external-file-cache)
- [Performance guidance](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads)
- [cache_httpfs extension](https://duckdb.org/community_extensions/extensions/cache_httpfs)
