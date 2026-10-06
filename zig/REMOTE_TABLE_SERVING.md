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
| Range caching | Public SQL/rows ranges and authenticated native aggregate artifacts share bounded RAM → disk → source reads | Other artifact-reader admission and per-statement explain accounting |
| Index construction | RowSource sidecar builders, scoped artifact uploads; native catalog CAS leases, API creation/deletion, maintenance recovery and catalog status | Lease renewal during long builds, streaming builders beyond bounded replay |
| Index selection | Fresh per-execution catalog definitions, complete-coverage proofs and catalog-selected native SQL aggregates | Connect candidate/hydration readers to rows/search and exact SQL predicates |
| Algebraic execution | Native exact typed reducers, strict recipe matching, shared construction scans/blocks and catalog-selected sparse slot composition | Additional expression/predicate equivalence proofs |
| Incremental refresh | Immutable file identities and invalidation foundations | Per-file contribution manifests, append merging and delete-aware correction |

This table is an acceptance gate. Helper tests or a configured index alone do not
make any unfinished path query-ready.

Native metadata owns external index generations in an internal, versioned table
record extension. The query-definition projection carries that extension with
the schema and desired indexes; identity-only projections omit it. Empty legacy
records retain their original binary encoding and JSON shape. Publication requires
metadata decoder capability 22 on every coordinated replica.

Each build attempt records a monotonic generation, a unique token, a bounded
lease, and separate digests for desired definitions, resolved source coverage,
source credentials, and artifact-store identity. Renewing, completing, failing,
or clearing an attempt uses a full table-definition compare-and-set. Raft
application rejects stale generations and tokens, publication after a definition
change, and unconditional catalog writes through table reconciliation. A failed
or pending rebuild retains the preceding immutable publication; selection must
still prove that publication matches the current authorized source and desired
definition before reading it. Clearing retained roots advances the attempt
counter so a previously admitted worker cannot resurrect them.

Native API creation and deletion wake an idempotent maintenance reconciler. It
does not synthesize a default text index during Parquet/Iceberg attachment;
external indexes are explicitly declared at creation or added afterward. It
commits the attempt before uploading artifacts, caps construction allocations,
revalidates coverage, then publishes through the exact pending-record CAS.
Ambiguous metadata replies are re-read rather than replayed; expired attempts
can be replaced. The periodic supervisor rediscovers definitions and pending
generations after restart. Dropping the last index clears retained roots through
a fenced generation change. Public remote status follows this catalog instead
of empty native shard indexes. Uploaded generations remain non-queryable until
the serving paths consume the coverage and candidate proofs.

Configure shared immutable artifacts separately from source credentials:
`storage.artifacts` contains `connection`, `bucket`, and optional `prefix` (default
`native-lake-indexes`). The named S3/GCS connection requires `storage.primary`
capability and its bucket/prefix allowlist must contain the location. Distributed
nodes require shared artifact storage. Standalone/embedded deployments can use
`<resolved engine data directory>/artifacts` when no shared location is configured.
This durability directory is separate from the evictable read cache. Artifact
readers cannot provision buckets, upload, or delete objects. Storage identity
includes the actual location and resolved credential digest; credential material
is never persisted in the publication catalog.

The native builder input adapter borrows typed vectors and dictionary IDs from
one authorized source. It emits live contiguous runs after applying deletion
masks, including equality-delete columns absent from the index projection.
Coverage preparation requires real provider versions for every covered data and
delete object. Its canonical data-file digest excludes discovered footer details
and pins the delete-object versions used by the builder; publication must compare
the completed build with that original coverage proof. The native maintenance
worker uses this adapter and fencing protocol. Native aggregate cursors consume
the catalog selection proof before opening exact aggregate state. Search and row
candidate consumers remain open.

## Native execution and delivery

Parquet's ordinary INT32/INT64 and FLOAT/DOUBLE dictionary pages retain their
numeric dictionary values and row indices in the native scan path. Null slots do
not reference dictionary entries. Cached decoded pages retain the dictionary
lease; uncached pages borrow their cursor's dictionary. SQL kernels, Iceberg equality deletes,
public row reads, and sidecar builders accept these representations. Predicate
and dynamic-filter evaluation reuse dictionary results within each physical page.
Supported SQL expressions evaluate only referenced dictionary entries or repeated
tuples of dictionary inputs and retain an encoded intermediate. Projections over
the same inputs share the selected-tuple gather and typed expression graph. Tuple
identity compares physical IDs exactly, including the NULL lane, and abandons
memoization when the selected tuple cardinality exceeds half the batch. Unselected
dictionary entries are never evaluated. Direct projections, selected
scans, mapped batches, and retained stores export compact referenced dictionaries;
column stores remap each referenced entry once instead of hashing every row.
Dictionary import validates IDs before mutation, preserves prior NULL rows, and
returns to flat storage when cardinality grows; high-cardinality, lazy and external-function
expressions use the existing typed/scalar fallbacks. SQL scan predicates use the
same dictionary kernels. Retained integer and float columns sample cardinality
without allocating; repeated values use dictionaries and a later high-cardinality
suffix returns to flat storage. Representation IDs preserve exact integers, float
bit patterns and SQL NULL separately. Downstream expressions reuse these retained
dictionaries through selection and slicing. Unique numeric columns stay flat.
Logical timestamp conversion currently retains its expanded numeric path.

Spilled joins admit compact blocks into typed hash state and reuse bounded
candidate workspace. Probing borrows compact blocks through a forward reader,
retains at most 16 KiB of its reusable arena between blocks, and expands only the
current row into a reusable buffer. That row remains valid until all duplicate
matches and residual checks have drained. Group partitions consume compact blocks using reusable row
scratch and import exact partial states without expanding a whole block into a
`Datum` matrix. Singleton records and already-expanded replay spans borrow the
source's read arena; typed multi-row blocks own compact payloads until admission
finishes. Consumers copy retained state before advancing the source. This avoids
allocating a payload lease for each singleton. Unfiltered grouped expression cohorts retain encoded columns through
group hashing and aggregate updates. Dictionary keys memoize semantic hashes;
aggregate inputs preserve source lane order, including floating reductions. Partial
admission, replay, skew fallback, and legacy wide records share the sequential
reader's lifetime and retry contract. Serial partition builds receive their full
assigned workspace; sibling reservations apply only when parallel builds actually
run. The enclosing statement reserves a delivery lane before assigning partition
workspace, and partition costs estimate typed payloads, hash links, metadata, and
capacity growth rather than expanded `Datum` cells. Open partition files share a
bounded buffer allowance.

PostgreSQL delivery reads cells from retained execution columns directly. Result
views preserve ownership through portal slicing; scroll/hold cursors copy cells
at their spooling boundary. SQL NULL remains separate from JSON null, and datetime
conversion occurs at encoding. Retained pages must be released before their stream
closes. Stateless HTTP SELECT encodes these leased columns directly into its bounded,
atomic response envelope. NULL flags use one bit per cell during encoding;
integers retain exact decimal-string wire values. Blocking delivery leases decoded
sequential spill blocks and final sort-merge blocks. Primitive columns decode into
validated owned buffers, packed NULL flags, and compact position/text directories;
`Datum` cells are reconstructed at access boundaries. Heterogeneous JSON and
pattern columns keep the fallback decoder. Compact blocks preserve repeated
primitive/text columns with a private dictionary encoding
when its serialized size is smaller than the flat encoding. IDs are validated at
decode; exact integers, float bits, SQL NULL and JSON null remain distinct. Sort
merging reuses a scratch arena per lane for keys instead of allocating each head
into its payload lease. Sorted delivery requests compact
heads at final-merge initialization; switching from earlier scalar delivery
transfers already decoded heads safely. Pages gather row descriptors
and retain each distinct block once, without cloning payloads into another column
store. Sorted page admission accounts for decoded block capacity as well as logical
result bytes. In-memory sorted rows remain owned by the bounded sort after the
first leased page; scalar/batch mixing cannot reclaim rows held by earlier pages.
A page retains its cursor through terminal error cleanup. Active transactions,
mutations and declined streaming shapes retain their existing result path.
The SQL aggregate provider contract binds direct grouping columns and reducer
inputs by physical path, type, nullability and DISTINCT semantics. COUNT(*) and
COUNT(column) have different recipes. Aliases, HAVING, ordering and paging remain
in the native SQL consumer. Predicates, casts, expressions and filtered
aggregates fall back until a separate equivalence proof exists. Providers supply
bounded AGS1 partial pages only after proving current authority and complete
source coverage. SQL imports their exact states, including i128 integer sums and
compensated floating sums; restoring the first floating partial copies its
state without arithmetic. The optimized COUNT(*) path uses this same provider
and signature. Read failures after selection abort rather than mixing snapshots.
Public algebraic definitions accept `derive_from_schema: true` with optional
`aggregates` recipes containing `name`, `op`, `group_by` and `measure`. For example,
`{"type":"algebraic","derive_from_schema":true,"aggregates":[{"name":"total","op":"sum","measure":"amount"},{"name":"rows","op":"count"}]}`
requests SUM(amount) and COUNT(*) over the full snapshot. Fields, physical state,
laws and build policy remain engine-owned. COUNT(column) excludes SQL NULLs;
COUNT(*) includes them. Schema derivation rejects unknown/incompatible fields.

The native provider resolves the current catalog and authorized source before
opening a matching aggregate root. New roots use `native-sql-aggregate-v2`, with
metadata version 2; readers also accept exact version 1 roots. Each root
binds its materialization identity, exact recipe and group count to bounded,
checksum-authenticated NCB1 column blocks containing AGS1 state cells. Readers
retain one block, import borrowed state through the existing exact reducers and
verify the statement catalog fence while draining. Native construction uses the
same typed grouping state and disk partitions as SQL. Delete-free global COUNT(*)
uses footer counts; Iceberg deletes use the delete-aware input adapter. Every
requested aggregate slot must match a materialization from the same complete
publication. Composition imports only the selected typed slot and retains one
reader, avoiding a dense array of empty states for each input partial. Slot
maps and every state signature validate before group mutation. Existing dense
spill frames expand sparse slots only at the durable spill boundary. Group orders may differ after spilling;
the SQL reducer merges by semantic keys rather than zipping matching positions.
Incomplete slot coverage falls back before selection. Custom laws, joins, temporal
buckets and other unsupported recipes do not claim SQL substitution.

Construction groups compatible materializations by physical grouping paths,
types and nullability, across index definitions. Each bounded cohort (up to 64
reducers) scans the union of required columns once and shares typed grouping,
disk partitions, keys and immutable output blocks. Each version 2 root binds its
own reducer slot and the full state recipe, so selecting a slot cannot reinterpret
another reducer's bytes. Materialization identities hash length-framed public
index/recipe names to avoid punctuation collisions and exceed no catalog name
limit. Roots remain distinct even when their shared block references coincide.

Aggregate roots and blocks share the server's bounded RAM and persistent disk
cache. Keys bind artifact identity, expected length/checksum and resolved artifact
store credentials. Fresh coverage and authorization checks precede lookup;
cached bytes do not supply source authority. Concurrent misses share a load,
waiters retain their own cancellation, and disk admission uses the existing
sidecar priority lane. Damaged disk entries retry the verified provider; a
provider integrity failure after selection aborts the query.

Remote search/rows candidate selection and hydration remain open.

Native partial aggregate handoff uses the `AGS` version 1 binary state codec.
Its signature uses frozen explicit kind/type IDs, independent of enum declaration
order, and binds DISTINCT semantics. Native count, i128 sum, compensation and
mean fields avoid decimal parsing. Typed
variable payloads retain extrema, DISTINCT membership, pattern NULL and JSON
NULL separately. All signatures decode before group import. This codec does
not by itself publish a reusable remote materialization.

The ReleaseSafe dictionary-expression fixture evaluates 4,096 rows with 32
unique integers, repeated 256 times. Across three local samples, encoded
execution takes 13.4–14.2 ms versus 34.4–35.1 ms for expanded kernels, with
24,642 versus 442,644 peak workspace bytes. Both validate the same checksum.
Run `zig build sql-native-refinement-bench -Doptimize=ReleaseSafe` to reproduce;
these measurements describe this fixture, not end-to-end lake throughput.

Scan, join and group partition workers choose useful fan-out from shared scheduler
capacity and their total workspace allowance, up to eight concurrent lanes.
Ordered scan delivery, bounded credits, inline fallback and cancellation remain
part of the contract. Concurrent statements share admission; planned fan-out is
advisory and every submitted task still acquires its own lease. Spilled joins
measure partition costs during ingestion, start larger pending partitions first,
and reduce concurrency when a larger workspace avoids another spill. A child
uses its assigned allowance directly; it does not divide that allowance again.

The ReleaseSafe wide 100,000-by-100,000 spilled join benchmark produces the same
aggregate checksum with approximately 0.366 million backing allocations, compared
with 1.244–1.278 million at branch head `479e4af31` before these changes. The
new sample retains roughly 9.70 MB of peak statement workspace. Build concurrency
varied during timing samples, so allocation counts are the useful comparison.
These local timings are not a cross-machine throughput guarantee. Run `zig build sql-native-pipeline-bench
-Doptimize=ReleaseSafe` to reproduce allocation and peak-memory measurements.
The recorded [workspace and lease samples](bench/baselines/native-lake-workspace-and-leases.json)
include source hashes and the comparison against the reviewed branch. A separate
4,096-row, 32-integer-column decode fixture measures approximately 110 KB of live
compact decode workspace versus 1.14 MB for expanded cells, with identical output
checksums. This isolates spill decoding from file construction and other operators;
it is not a whole-statement memory or throughput claim. The
[compact state samples](bench/baselines/native-lake-compact-typed-state.json)
record the measured source hashes and all three comparisons. Sequential
leases reduce delivery work in the delivery fixture. Sorted timings overlap, and leases
retain more decoded memory than gathering into a small dictionary; decoded-capacity
admission keeps each page bounded. These are separate tradeoffs from join admission.

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
wide integer sums until final SQL narrowing; AVG retains its exact native mean/count state; floating
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
