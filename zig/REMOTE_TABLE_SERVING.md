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

The integrated serving paths are:

| Path | Implemented behavior |
| --- | --- |
| Range caching | SQL, HTTP rows, and native text/vector/row/aggregate artifacts share bounded RAM → disk → source reads and immutable metadata leases |
| Index construction | Scoped uploads, renewing catalog CAS leases, bounded shared replay, native builders, API maintenance recovery, and catalog status |
| Index selection | Fresh per-execution definitions and coverage proofs gate native rows, ordered indexes, text/dense/sparse search, and exact SQL materializations |
| Algebraic execution | Exact typed reducers, strict recipe matching, shared construction scans/blocks, and sparse slot composition; additional equivalence shapes follow the same proof contract |
| Incremental refresh | Per-file contribution manifests reuse unchanged physical artifacts and handle append, replacement, removal, and delete changes |
| Retention | Durable reader sessions and resumable fenced mark/sweep protect live generations, with bounded membership batches and filesystem upload inventories |

A configured index alone does not establish query readiness: the authorized
publication, source proof, and reader fence must all validate before consumption.

Native metadata owns external index generations in an internal, versioned table
record extension. The query-definition projection carries that extension with
the schema and desired indexes; identity-only projections omit it. Empty legacy
records retain their original binary encoding and JSON shape. Publication requires
metadata decoder capability 22 on every coordinated replica. Native reader
sessions and reader-safe collection require protocol 24 or later. Paged aggregate
contribution directories require metadata and reader protocol 25; admission waits
for that decoder capability on every coordinated replica. Protocol 24 publications
remain readable and retain their reader-safe retirement obligations.

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

### Native runtime and ordered scan reuse

Native dense and sparse queries borrow a runtime lane keyed by the artifact
identity, upload domain, logical index and current artifact-store scope. Admission
still verifies the current schema, publication, credential identity and complete
source coverage. Each lane serves one execution at a time, keeping kernel scratch
and lease-bearing callbacks isolated. Returning a lane clears its request context.
The server retains at most 64 lanes under a shared 512 MiB heap budget, charged to
the storage resource manager. Each runtime has a 256 MiB ceiling and retains at
most 8 MiB of authenticated native file blocks. Reader checks also run on warm
reads. Retirement or eviction closes indexes, releases block leases and frees the
runtime before the persistent read cache shuts down.

Ordered native indexes now accept signed datetime keys. The tuple encoding is
version 2 and the desired-publication fingerprint is version 5; a generation or
cursor built with the older encoding cannot be admitted under the new format.
The encoding keeps signed nanoseconds ordered without losing the existing local
unsigned timestamp domain.

SQL can automatically choose leading equality prefixes and a range on the next
index key. A scan's requested ordering is separate from the provider's ordering
proof. The executor skips sorting only after a pinned cursor attests every direct
column, direction and NULL placement. Compatible `ORDER BY ... LIMIT` scans stop
when residual matches and OFFSET are satisfied. Expression orders and collation
mismatches retain sorting. Physical hydration gathers projected columns into a
bounded typed window and exposes the index permutation; covering reads bypass
Parquet hydration.

### Explicit immutable data objects

The attachment setting `base_source.object_mutability` defaults to `mutable`.
That mode continues to request fresh provider-version evidence for every data
file. Set it to `immutable` only when an existing data-file URI is never replaced.
For example:

```json
{"kind":"external","table_id":"events","format":"iceberg",
 "uri":"s3://warehouse/events","object_mutability":"immutable"}
```

Under that contract, fresh listing versions and authenticated retained publication
inventories can prove unchanged data objects without one HEAD per file. Reuse is
limited to matching file identity, URI and size within the current credential and
artifact-store scope. Conflicting fresh evidence is preserved. New unresolved
objects are verified individually. Current metadata and deletion objects are
still verified, and the full coverage signature continues to fence index reuse.
There is no time-based authority cache.

### Shared native build input

When several native indexes require reconstruction, the publication planner
collects their projected source columns. One pinned, delete-aware Parquet scan
writes bounded typed column blocks to a private spill run. Independent readers
replay projections for text, dense, sparse and ordered builders. File boundaries
carry both logical and physical offsets, allowing per-file aggregate consumers
to seek directly rather than replaying the complete run for every file. Replay
uses the existing spill quotas, cancellation and cleanup lifecycle. Native values,
NULLs and physical row coordinates survive replay without serializing documents.


### Incremental native generations

Native producers authenticate a per-file content identity from the source and
schema identities, object URI and provider version, partition/sequence metadata,
and deletion evidence. This identity excludes the serving snapshot label. A new
snapshot can retain unchanged file contributions after fresh authorization and
complete coverage verification. Schema or producer configuration changes force
reconstruction. Position deletes contribute a sorted semantic fingerprint per data file. Equality
deletes contribute only the versions and applicability metadata of delete files
that can affect that data file. A global delete-object version is still checked
at admission and publication; localized changes no longer invalidate unrelated
native file state. Producer identity v3 and recipe v2 force a one-time rebuild
of indexes created with the previous global deletion identity.

Text roots retain segments per file and assemble one corpus with global BM25
statistics. Compatible cached corpora fork immutable reference-counted readers;
new snapshots decode only added segments and update field totals from changed
segments. Up to four cold loads share process-wide scheduler admission, and all
workers join before publication or failure cleanup. Dense and sparse roots retain authenticated per-file document lists;
rebuilds restore a private native checkpoint, delete removed/replaced file rows,
and ingest changed-file rows through the existing native kernels. Document-list
artifacts are part of the GC reference graph. Native text/vector root metadata is
version 2. Internal `lake2:` identities are stable across compatible snapshots;
public results keep current snapshot-bound `lake1:` IDs. Search projects identities
before result filtering, sorting, pagination and fusion, including public ID filters.

Ordered roots use metadata version 4. A second immutable tree indexes each tuple
key by physical file/row coordinate. Replacements and removals seek only the affected
file ranges. Authenticated reverse-tree counts estimate changed rows: deltas below
max(1,024 rows, one eighth of the prior row count) use bounded copy-on-write
mutations; larger changes use one sorted streaming merge. Small appends retain
untouched pages and covering blocks. Initial and legacy generations build both trees
from bounded sorted streams. GC traverses both roots. Removed file slots can be
reassigned after their old keys are removed, so file churn is bounded by concurrent
snapshot size rather than lifetime file count. Public coordinates bind to the current
snapshot. An unchanged generation retains both trees.

Dense and sparse checkpoint publication retains authenticated chunk references for
unchanged native runs and posting segments. Appended WALs reuse complete prefix
blocks and publish the changed tail. Manifest bytes are compared against the prior
chunk checksum before reuse. Dense generations retain small committed WAL deltas;
full flattening occurs at a 32 MiB WAL, eight-segment, or four-sealed-WAL boundary.
Rebuild candidates use a private writable overlay on the immutable checkpoint.
Unchanged runs remain remote and use bounded authenticated range reads. Append or
rename copies only the affected file; replacements write directly to the overlay,
and tombstones prevent removed paths from reappearing from the base. Storage leases
retain the overlay through native worker and checkpoint lifetimes. Publication and
cross-attempt references remain protected by the same catalog and GC fences.

Search hydration compiles positive output-field patterns into physical Parquet
projections. Full-document and exclusion-only requests retain full hydration.
Filter evaluation and sort consumers retain their own field requirements. Missing
optional columns become typed NULL vectors at the shared row-source boundary, so
single-index builds, shared replay and ordered key generation agree on schema
evolution without weakening required-column or type checks.

Algebraic consumers participate in the same changed-file plan using authenticated
recipe/file contributions. Mixing algebraic and search indexes therefore preserves
delta replay. Unsupported reducer laws or deletion layouts conservatively request
a complete replay.

For compatible unchanged declarations, the shared build replay scans the union of
changed files. New or unsupported producer recipes conservatively request a full
shared scan. All candidate checkpoints, spill files and readers close on failure;
publication still requires fresh coverage verification and the catalog CAS fence.

Search caches bounded immutable publication identity maps after fresh source,
credential, and coverage validation. Entries own strings and never retain request
contexts or reader authorization. Queries acquire a fresh reader lease even when
all payload and metadata reads hit caches. Vector-only tables support `match_none`
and delete-aware `match_all` scans without requiring a text index. Flat nullable
Parquet V1 and V2 pages share typed value kernels after version-specific level
framing and decompression; independent PyArrow fixtures exercise plain and
dictionary encodings through attachment, index publication, and public reads.


### Bounded publication reuse and collection progress

Verified inventories, their file-ID maps, declaration directories, and native
text/vector/ordered roots share the decoded-cache byte limit and reference-counted
leases. Cache identities include the metadata type, artifact identity, provider
scope, and wire version. Authorization, credential binding, source coverage,
and reader admission are checked for every query; a warm metadata hit grants no
permission and cannot bypass a deadline. Iceberg data and delete-file version
checks use the shared scheduler in bounded waves, hashing results in manifest
order regardless of network completion order. Under the explicit immutable-data
contract, a freshly resolved Iceberg plan can reuse a canonical data coverage
proof keyed by metadata content, credential/store scope, publication inventory,
and definition/source signatures. Delete-object versions still refresh on every
selection. Source owners retain independent inventory leases and resolve pinned
provider versions only for files that survive pruning. Mutable sources retain
complete revalidation; snapshot labels and TTLs never authorize proof reuse.

Text corpus admission reserves each compatible physical segment once across
active generations, plus each root. Sealed readers compose from all compatible
ready generations; compatible cold builders single-flight before reserving shared
segments. Failed builders drain and release their reservations before retry.
Idle entries can be evicted under pressure; active queries retain their roots and
segments. Unchanged file groups and physical segment ordinals use maps. Mapped
residency belongs to each shared segment, so overlapping generations do not
duplicate resource-manager charges; the final segment release removes the charge.

Initial reader-session admission preserves the caller's deadline and cancellation.
Shared session heartbeats use independent deadlines bounded by remaining lease
validity and five seconds, plus owner shutdown cancellation. Request pointers are
never retained by shared heartbeat owners.

Native remote readers pin immutable chunk leases while copying, with per-chunk
single-flight loading and no cache lock held during remote I/O. Independent chunks
can load concurrently. Reads spanning chunks may prefetch two requested successor
chunks through shared scheduling; all tasks join before the request owner closes.
Cache admission includes bytes reserved by in-flight loads, and pinned chunks are
never eviction victims.

Artifact GC checkpoints an immutable native page tree for its live set and a
separate paged work frontier through the collection's metadata CAS. Each pass
bounds jobs, payload reads, checkpoint reads/writes, and retained memory; these
limits bound a pass rather than the size of the live graph. A replacement collector
resumes the same retirement cut after lease takeover. Checkpoint updates compare
the previous checkpoint identity as well as the collection token, preventing a
stale worker from moving progress backwards. Physical retention and structured
root/page expansion use separate deduplication markers.

Sweeping batches sorted live-set membership through affected native page-tree
subtrees once, with an operation-local verified routing-page cache. Enumeration
remains restricted to the collection's upload range. Remote object stores resume
with the provider's exclusive start-after key. Filesystem artifacts, including the
standalone object-provider wrapper, append and sync fixed-size upload inventory
records before writing payloads. Offset continuations survive restart and deletion
without sorting or rescanning the remaining object namespace for every page.
Discovery metadata lives outside framed object files. Legacy flat attempts receive
one streaming backfill, atomically installed under an interprocess lock; incomplete
backfills and uploads cannot authorize reclamation or make a live object invisible.
After the durable completion receipt, GC removes discovery journals and directories
only for fenced attempts with no payloads. Live attempts retain their journals;
interrupted advisory cleanup retries during later collections, keeping enumeration
independent of the accumulated history of fully reclaimed attempts. A private
checkpoint attempt stays protected while its collection runs and is reclaimed
only after the metadata completion receipt. Interrupted checkpoint cleanup leaves
ordinary orphans for a later collection. Incomplete background passes reschedule
without waiting the normal maintenance interval; a pass has a 120-second deadline
to leave bounded time for checkpoint I/O. Dry-run collection remains read-only.


### Immutable scan plans and aggregate reduction trees

Cached Iceberg plans own the validated manifest, canonical public-row-ID file
ordering, inverse file ranks, file-ID lookup map and row/byte estimates. Parquet
and uncached Iceberg sources prepare the same structure once per source owner;
Parquet listings are still refreshed for each query. Query owners borrow those bytes through a decoded-cache lease. Selective hydration
maps and orders only candidate files rather than rebuilding a manifest-sized
lookup or sorting every file for each candidate window. Provider version evidence
lives in a sparse query-local overlay; full coverage checks and builds explicitly
materialize an owned inventory. Ordered split children borrow their parent's plan
and keep independent version overlays. Fresh metadata, credentials, coverage and
delete-object checks remain required. Mutable prefix listings are not immutable
snapshot plans.

The SQL statement cursor forwards ordering proof, exact-count callbacks, parallel
split callbacks and estimates. Split children close before their parent, which
retains the statement's source and reader leases until all child work drains.

Exact associative algebraic recipes use a compressed binary radix reduction tree
keyed by snapshot-independent file digests. Hash-indexed contribution lookup
reuses unchanged file partials and internal reductions. An append, replacement or
removal rebuilds only affected ancestors, merging authenticated child partials
through the existing bounded/spillable typed reducer. MIN/MAX use hierarchical
merging, with no subtraction or approximate numeric conversion. Floating-point,
distinct and unsupported recipes retain their ordinary exact rebuild path.

Current leaf and internal contributions are retained in authenticated pages of at
most 256 records and 1 MiB each, referenced by a version-2 declaration directory.
The old 16,384-record incremental fallback is removed; the explicit catalog safety
limit is 1,048,576 contributions, alongside build, artifact and spill budgets.
Serving reads only declarations; builders load contribution pages for indexed
reuse, and GC checkpoints page traversal before sweeping any payload. Metadata
planning and contribution-directory publication still scale with live file/state
count; reduction payload work scales with changed tree paths and group sizes.

Dry-run GC completes its census independently of the destructive deletion budget.
It does not hold the scheduler on the same table merely because eligible objects
exceed `max_deleted`. Destructive collections retain their durable continuation.

### Keyed contribution state, grouped partitions, and cold-load admission

Reader/topology protocol 28 adds counted contribution ownership to
`native-lake-index-directory-v3`. Protocol 27 introduced ownership edges;
protocol 26 introduced that directory and `native-sql-aggregate-v3`. Readers
retain compatibility with protocols 24/25/26/27 and aggregate versions 1/2.
Coordinated metadata admission prevents publishing new ownership semantics
while an older metadata voter is still active.

A v3 directory authenticates an immutable page-tree root keyed by file/reduction
identity, exact recipe, and materialization name. Builders look up inherited
contributions lazily, using a hash-indexed page cache bounded to 4096 pages and
128 MiB. Reduction nodes own two child contributions and up to 64 terminal range
aliases. The directory records the publication root set; each contribution
records its incoming ownership count. Refresh adds new roots before retiring
old roots, traversing descendants only when their count crosses zero. Shared
subtrees survive without loading their descendants. Changed records update the
immutable page tree, preserving old generations for pinned readers. A no-op
ownership update needs no contribution-page reads.

Builders probe unchanged reduction nodes before resolving file contributions.
For unchanged definitions without delete plans, an authenticated prior inventory
also supplies a shared file-membership set for replay admission, avoiding one
contribution lookup per file and recipe. Snapshot inventory planning and reduction
shape calculation still require linear file metadata/CPU work; refresh is not
constant time. Legacy uncounted generations perform a one-time ownership census
and prune before publishing counted state. Subsequent refreshes touch changed
ownership paths and newly live/dead subgraphs. Durable GC checkpoints contribution
pages and aggregate references as authenticated frontier jobs.

Exact incremental grouped reducers partition keys by their semantic hash into
64 immutable ranges. Each file contribution and reduction root authenticates
its range directory. Missing ranges are inherited without reading their state;
unchanged child-range pairs reuse their previous exact aggregate artifact.
Only affected ranges are reduced and republished. SUM/COUNT, booleans, and
supported MIN/MAX preserve the existing exact state laws and disk spilling;
removing a file recomposes surviving contributions, without assuming MIN/MAX
have inverses. Floating-point grouping/reduction, DISTINCT, and delete plans
retain their conservative paths. This reduces high-cardinality rewrite work
when a delta touches few ranges; broad or skewed deltas can still touch every
range. Partition readers validate recipe/slot provenance, range uniqueness,
counts, and authenticated child references before exposing their state.
Compatible partitioned cohorts fuse SUM/COUNT/MIN/MAX slots and decode each
shared block once. Fusion checks every child directory before changing slot
maps; a mismatch anywhere leaves the readers independent. Preflight retains
one directory pair at a time and bounded per-partition slot maps, then serving
retains one active child reader. Parsed authenticated aggregate directories share the bounded decoded
metadata cache and its single-flight loaders across fusion preflights and serving.
Recipe and slot compatibility is checked on every reader admission, including
cache hits.

Sort runs retain payloads and fallback keys in typed column stores and sort
compact position/ordinal references with normalized keys. In-memory results lease
those columns; payload rows are materialized only at owned output boundaries.
Spilled runs gather bounded column batches directly into the existing typed spill
codec. Partitioned joins likewise hash bounded input batches into partition
selections and write typed columns without cloning a row per input. Both paths
retain existing cancellation, memory, disk quota, checksum, and exact null/numeric
semantics. Compression and physical spill formats remain shared with ordinary
native execution.

Unordered parallel scans discover file footers and physical plans in bounded
waves through the shared native scheduler. Each planner owns version pins and
reader state; synchronized statement allocation and a join-before-transfer
boundary protect request arenas. Prepared delete membership stays immutable.
The coordinator publishes the largest-first row-group queue after all planners
join. Ordered multi-file scans keep lazy contiguous planning so a later file's
failure cannot move ahead of an earlier successful prefix.

Non-covering index hydration shares fully owned projected file-reader plans,
immutable footer metadata, parsed Parquet
page directories, dictionaries, and decoded vectors across candidate windows.
Page-directory keys include object versions, column interpretation, row count,
and allocation policy. Projected file plans also bind the source snapshot,
schema, selected columns, and physical predicates; delete preparation and
position offsets remain fresh request state. Once the dictionary prefix has been consumed, indexed
page seeks use binary search rather than walking every preceding page. Index
order is still restored after gathering physical candidates, and delete and
residual checks remain mandatory.

Decoded metadata, footer, parsed page-directory, dictionary and vector-page
misses use one in-flight owner per immutable key. Waiters retain their own deadlines/cancellation and share the
result even when it cannot enter resident cache. At most sixteen cold loaders
share a separate 64 MiB allocation admission budget. Reservations grow and
shrink with actual allocator capacity, allowing unrelated small metadata loads
to overlap even when each has a 64 MiB ceiling. A contending decoder releases
its partial state and retries in an exclusive lane; it never waits while
holding allocations that another decoder needs. Dependencies, such as a
hydration plan's footer, are resolved before decoder admission. Resident cache
saturation cannot silently create unbounded parallel decoding. A canceled leader permits another live
reader to retry, and an in-flight result stays pinned until every waiter leaves.

Filesystem scoped uploads append their content identity before creating a
`.pending-v2` staging file. A per-content interprocess lock serializes that
staging name. Fenced journal sweep batches remove staging alongside payload
census, using the same durable continuation and collection cutoff. Legacy
nonce staging cleanup and private checkpoint reclamation run after the durable
completion receipt; they cannot prevent a completed collection from advancing.

Native text corpus version 6 publishes 1 MiB immutable packs containing
independently authenticated 64 KiB ranges across the native segment. Readers use the shared native range codecs, retaining bounded
navigation and decoder scratch instead of allocating and zeroing a segment-sized
heap buffer. Term-frequency probes read dictionary addresses and posting headers;
Required WAND reads fetch the document chunks they visit, while bounded lookahead
may warm subsequent chunks. Scoped field views and both block-cache adapters
forward advisory hints, translating section-relative offsets at the field boundary. Position records are decoded when phrase consumers
request them. A block checksum may bring neighboring
bytes into cache, so the minimum read unit is 64 KiB for new publications
(256 KiB for version 3 packed directories). Each query binds its
current store capability, deadline, cancellation and reader lease to a private
native reader. Query binding borrows the already admitted field navigation,
collection statistics, and atomic page-validation states; it performs no metadata
reads. The physical segment pin outlives every borrowed view, while identity and
decoder caches remain query-local. Immutable composition normalizes query-bound
entries to their physical base reader, so it never copies query ownership or
retains another execution's authority. The shared corpus drops opening-request
authority after admission. Bounded artifact reads pin verified RAM blocks or disk
mappings instead of allocating and copying an entire block for each small read.
Pinned RAM remains charged and cannot be evicted until released; current deadline,
cancellation and reader-lease checks still run on warm hits. Large contiguous
segment leases keep their disk-first policy for clean-page reclamation. GC retains physical pack references rather than synthetic range cache identities,
and still understands retained version 1–5 roots and unpacked directories.
Reader/topology protocol 32 triggers automatic republication of
older corpora during rolling upgrades.

Native text readers hint the next document chunk using its authenticated native
metadata, including explicit offsets in layouts that interleave position records.
Up to four bounded ranges per query source warm through the shared process scheduler;
its worker and byte limits arbitrate with Parquet and other parallel consumers.
A full speculative queue yields, required reads recycle consumed slots, and close
cancels and joins every worker before releasing capabilities or physical owners.
Releasing a pinned query source quiesces its bound readers even when a caller has
retained the snapshot for immutable composition; later bound reads are canceled.
Speculative failures do not become query errors until a required read reaches the
failed block. Workers use independent scratch and current query cancellation.
Term-frequency and BM25 bound-table caches belong to the exact immutable snapshot:
query facades retain its cache owner, while cold frequency reads still use their
own bound capability. Publishing a changed corpus starts new scoring caches.

Automatic ordered-index access enumerates eligible definitions, proves partial
predicates and covering columns, and counts ranges using authenticated B+tree
subtree counts. Costing includes projected compressed column bytes and a
conservative estimate of touched row groups. An explicit SQL OFFSET + LIMIT is an
advisory row goal when the index proves the full ordering and every filter is
represented by its equality prefix and next-key lower/upper bounds. SQL binding
marks disjunctions, scalar predicates and other unbound residuals as incomplete.
Covered-column residuals and duplicate bounds with uncertain selectivity retain
full-range costing. The goal never limits scan pages or changes residual semantics.
Covering row-index windows preserve spill dictionary identities through their
batch callbacks and selected lanes. Column-major residual evaluation memoizes each
surviving dictionary identity per predicate, retaining separate SQL NULL and JSON
null identities and exact integer values; only result delivery expands rows.
Covering blocks use the shared bounded decoded-artifact cache: singleflight
decoding retains validated wire buffers, dictionaries and column views under a
lease. Candidate windows borrow these values across pagination and query reuse;
no query arena owns a cached payload. Scope and authenticated identity fence
reuse, while deadlines and cancellation remain query-local.
Unknown residual selectivity costs the entire candidate range. Actual page clustering
statistics remain a possible future refinement; explicit index requests retain
their required semantics.

Build replay writes bounded columnar spill blocks directly from native vectors,
without an intermediate row matrix or decimal coordinate strings. Consumer
cursors retain compact spill blocks and preserve numeric/string dictionary IDs
while borrowing payloads until the next batch. The spill codec exports typed
columns directly: strings and string dictionaries borrow decoded buffers, numeric
wire values decode into typed arrays, nulls come directly from packed flags, and
only dictionary IDs require widening for native scan consumers. It does not build
intermediate Datum dictionaries. Legacy scalar records retain the same typed
fallback and reject incompatible logical types. Independent per-file cursors seek
to physical record boundaries and share the same bounded spill owner.

The `iceberg_integration` E2E uses PyIceberg commits and PyArrow Parquet files,
including partitioned manifests, append snapshots, field-ID-preserving rename,
copy-on-write deletion, pinned history, HTTP SQL, pgwire, and cold restart.
`e2e-full` installs the lake and Iceberg writer extras and runs it; base E2E
excludes it like PostgreSQL integration. Missing full-suite writer dependencies
are a failure, not a silently skipped test.

Aggregate contribution construction runs bounded waves of independent file
reducers through the shared scheduler once replay capture is complete. Workers
borrow immutable per-file replay readers and own spill state. The coordinator
shares synchronized input-work admission across workers and joins them before
publishing artifacts or changing contribution ownership. Fully retained subtrees still bypass leaf
construction. Without replay, construction keeps the serial path rather than
sharing mutable source discovery state across workers. Recursive join spill
partitioning now selects typed blocks into child files, reusing key scratch and
preserving physical ordinals without materializing payload row matrices.

Remote native search attaches highlights through the same per-index analysis and
fragment helper as local search, after final hit selection and before response
encoding. Deferred projections retain original source until that final encoder,
avoiding a second hydration for highlight fields. Projected or omitted source is hydrated in bounded, delete-aware batches
from the same leased snapshot for highlighting, without widening returned source.
Named full-text clauses retain their own index analysis and selected-field
provenance. Highlight extraction shares the build projection path, including
explicit field indexes that override general table text mapping. Vector-only requests do
not synthesize text highlights. Real Parquet E2Es cover projected highlight fields
and repeat the request after a cold restart.

Small native disk blocks also share verified mapping owners under an independent
8 MiB/128-entry LRU bound. Active leases prevent eviction and keep the disk inode
pinned; saturated admission returns a private required-read lease. Mapping hits
avoid reopening, remapping and rehashing the same immutable cache inode. New
owners still validate the header, identity, length and complete payload digest;
eviction/restart requires verification again. This does not change large
contiguous segment reclamation. Mapping owners drain before the disk cache closes.

Remote search compiles physical hydration dependencies once before retrieval:
returned includes, field sorting and highlight source paths form one union.
Default highlighting follows each index's selected-field or schema provenance,
including document type discriminators and dynamic source prefixes. Deferred
wire projection does not force decoding unrelated columns. Omitted source loads
only highlight dependencies. Exclusion-only/full-source requests, schema-less
or unrestricted dynamic highlighting, and consumers without a finite dependency
contract (evaluation, reranking, hierarchy and residual filters) retain full
source. Final public projection still controls the returned document.


Native lookahead owns a rolling bounded task set. Shared scheduler completion
is observable independently of joining or admission release. Each new hint
reaps completed tasks, including ranges skipped by WAND, and remembers recently
warmed pages to avoid resubmitting work for every small decoder read. Failures
remain speculative until a required read, and close still cancels and joins
all outstanding work before releasing the query capability.

Disk-cache quota pressure can reclaim idle verified mapping owners, even when
the independent mapping LRU has spare capacity. The disk worker calls this
consumer hook without holding its inventory mutex. Only mappings with zero
active leases are retired; active query mappings remain pinned. Cache shutdown
flushes the worker before destroying the table borrowed by this hook.

Source-independent remote retrieval, including named text/vector fusion, now
hydrates the final hit page into shared immutable selected column pages. Physical hydration
returns bounded 4,096-row pages and applies the same snapshot/delete/lease checks;
key lookup uses a batch map rather than repeatedly scanning all requested keys.
Shared highlighting borrows one source row at a time and public field projection
operates directly on vectors for flat patterns. The public response is the first JSON encoding of
the document. Typed sources participate in hit cloning, release, and retained
memory accounting. Source omitted from the response may still be retained for
highlighting, without leaking dependency fields into `_source`.

An explicit shared dependency contract keeps encoded hydration for evaluation,
reranking, hierarchy, document-bound bindings, and other source-dependent
operators. Ordinary native identity/score ordering can use typed delivery;
field sorting retains its established execution path. Score/identity cursor
continuations use final projected column hydration after the snapshot-fenced
collector has applied their boundary.
This preserves response projection, exact integers, source omission, and
highlight behavior while removing document encode/parse cycles from the common
retrieval path.


Packed text directory version 4 authenticates each range checksum, length and pack
location through the corpus root. The provider returns only that range; the reader
checks its exact length and SHA-256 before admitting it to the immutable RAM/disk
cache. It never authenticates a sparse read by downloading the whole pack. Cache
keys identify verified range contents within the publication scope, while GC marks
the actual pack objects. Adjacent required and speculative ranges in the same
pack share a lazy bounded provider read. Up to four pack groups can warm in
parallel under shared worker/byte admission; each worker owns at most one pack.
A 1 MiB sequential read uses one range request instead of sixteen, while
publication uploads one pack instead of sixteen small objects. A single cold byte
fetches at most 64 KiB. Legacy directories retain their original geometry.
Each unit is checked before cache admission; a warm read does not fetch a pack.
Provider counters record physical coalesced requests and transferred bytes,
rather than counting each logical cache fill as a separate GET.
Denied cache admission serves required reads from the same bounded request.
This is a request-count improvement, not a claim of a measured wall-clock speedup.

Cold scoring gathers all requested term frequencies with one dictionary reader
per segment. Up to four disjoint segment lanes share process scheduler admission;
saturated lanes run inline. Caller scratch is synchronized across workers, every
worker is joined before scratch is released, and cancellation/deadline checks run
while waiting for an equivalent lookup and between segment reads. Bounded hashed
singleflight stripes coalesce equivalent term batches within the exact corpus
generation. Warm requests avoid flight admission; failed reads do not populate
scoring caches. Different term batches can proceed independently except for a
bounded stripe collision. Cache admission remains optional under memory pressure.

Native range sources forward the node resource manager into physical and
query-bound page caches. Optional slabs participate in pressure reclamation and
cache admission denial falls back to required uncached reads. Resource accounting
outlives bound readers; current query authority remains separate from that owner.

Final eligible search pages retain selected Parquet vectors through response
serialization, without constructing an owned JSON tree per hit. Reference-counted
immutable pages gather only selected slots and retain shared dictionary entries
once. Producers can also grant an immutable column-owner lease. Dense selections
retain decoded scalar vectors and dictionary payloads directly, copying only
column descriptors, names and selection ordinals. Hydration pulls up to 4,096
selected rows per decoded batch, so transport chunk size cannot make a dense
batch appear sparse. Individual hits share that page through 16 KiB wire writes.
Sparse selections, unavailable
ownership capabilities, and owners above the 4 MiB retention ceiling gather
compact independent slots. Retained cache owners survive cursor pulls, closure,
and eviction, including their dictionary dependencies. Hit cloning within the same allocator retains a page lease; a clone into
a different allocator gathers an independent row so it can outlive the original
request arena. Deinitialization releases the owned lease. Cursor
advancement and closure cannot invalidate a result. Scalar sources and flat include/exclude projections write directly
from vectors into the generated public response envelope. Complex projections and
JSON columns reuse scratch bounded to one hit, including with an arena backing
allocator; highlighting likewise borrows one
row at a time, including fields excluded from the public projection. Internal hit
codecs serialize source values rather than owner pointers. Exact integers, source
omission, nested inclusion/exclusion, and internal-field stripping retain their
existing behavior. Internal callers retain owned responses; eligible public calls use the prepared streaming delivery described below.

Boolean conjunctions check local required-term lookups before requesting global
scoring frequencies, retaining successful navigation for iterator construction.
An impossible conjunction therefore performs no corpus-wide scoring reads.


Physical text reads use pack-scoped in-flight coordination separate from unit
cache admission. A leader probes verified RAM/mapped/disk residency and reads
only contiguous cold runs; fifteen warm units plus one cold unit transfer only
64 KiB. Concurrent leaders for different units can share the verified physical
result even when RAM admission is denied. Flights carry immutable interval
coverage: readers join containing or partially overlapping intervals, while disjoint
intervals proceed independently under the same shared loader/byte admission.
A partial join completes before the reader registers its own flight. Its loader
pins the completed immutable inputs and fetches only uncovered cold runs, without
waiting on another flight while owning one. Composed payload dependencies survive
producer closure and denied residency; independently authenticated unit errors
remain isolated. Physical results carry
verified bytes or an error per unit. A corrupt unit in a broad speculative read
cannot fail a required reader of a healthy unit, and a failed provider run cannot
discard successes from other runs. Readers recheck residency before a missing-unit
fallback GET. Flight payloads are temporary leases,
with no second resident copy of a pack. The physical loader authenticates each
unit once and carries its digest proof with a retained slice owner. Cache admission
copies a unit once into independently evictable RAM residency; denied admission
returns the original physical slice without copying or hashing again. Each
consumer checks its own current authority. Waiting readers use their own
cancellation/deadline; a canceled leader does not cancel another reader.
Planners never join unit flights while owning a physical flight, avoiding
cycles between overlapping unit and pack leaders.

Required multi-pack reads use at most four lanes through the shared scheduler.
Each lane owns bounded scratch and a disjoint output slice; saturation runs
required work inline. All lanes join on success, failure, or cancellation before
query capabilities and output storage can be released. Speculative hints keep
their separate transient admission and may yield under pressure. A required
cache hit reaps only completed speculation: it does not wait for unrelated units
in an overlapping pack. Quiesce still cancels and joins all remaining tasks.

Final hydration builds one physical row selection for small ranked results,
including duplicate identity consumers. Above 65,536 identities, a 512 KiB native
external sort orders canonical physical identities, preserving original result
positions. Bounded windows of at most 65,536 references then visit files/groups/rows
in physical order and scatter results back into rank order. The selector window
is not a public result or pre-pagination residual-candidate limit; duplicate
consumers remain independent, including across window boundaries. Score/identity pagination retains this
path, with the same snapshot fence and sort tuple semantics. Native-proven filters
can rank identities without final-source hydration; unresolved predicates retain
the existing complete-source callback and fail-closed authorization behavior.
Temporary predicate sources are released before final projected column hydration,
so predicate/highlight dependencies never broaden the public projection.
A pinned cursor per physical window visits selected
files/row groups in physical order and returns bounded pages; an identity map
scatters each page into rank order. Crossing a 256-hit boundary no longer reopens
a cursor or revisits the same physical group solely because of rank batching.

Eligible single public remote searches deliver the existing JSON envelope
through a synchronous transport sink. A prepared wire plan encodes metadata
once and compiles projected source descriptors before committing headers. Scalar
strings borrow immutable column pages; numbers and complex JSON fragments are
canonicalized once. Exact JSON escaping and punctuation lengths enforce the
caller's response-size ceiling before transport starts. Transient JSON/projection
trees are discarded after each source is prepared, including escaped strings.

The plan emits its ordered metadata/source segments through a 16 KiB writer with
transport backpressure, without replaying envelope serialization or projection.
Small plans retain compact descriptors and borrow scalar strings. Public delivery
uses a 4 MiB preparation threshold; exceeding either planned metadata/fragment
storage or uniquely pinned source-page bytes switches to a private spill file.
Per-hit preparation scratch is discarded after its validated fragment is appended.
Previously prepared segments are serialized once before their arena is released.
The existing temporary-file manager supplies cancellation, shared I/O scheduling,
private permissions, immediate unlinking, cleanup on failure and a 1 GiB disk quota
(or the caller response ceiling, when smaller). The complete file is flushed and
its exact length checked before headers commit. Exhausting delivery resource
limits returns HTTP 413 before commitment. Spilled public delivery consumes source
leases as their fragments are prepared, so a slow client does not pin decoded
source pages. Replay uses a 16 KiB buffer and observes cancellation/backpressure.
Reusable sink callers opt into source consumption explicitly. Ranking metadata
and the hydration result still obey candidate/result admission; this is bounded
wire preparation, not a constant-memory query/result cursor. The query and
publication reader lease remain alive through the last write; unspilled plans
also retain their selected column pages through that boundary. A disconnect or
error after commitment terminates the stream rather than retrying execution or
sending a second response. Internal/group callers, composed dispatch consumers,
and NDJSON multi-query retain buffered delivery. Existing row/candidate limits
and caller response ceilings still apply. No wall-clock speedup is claimed.

The `x-antfly-response-streaming` OpenAPI operation flag exports optional
transport streaming for global, table, and namespace query routes through the
kernel manifest. It leaves the ordinary typed JSON response schema unchanged.
Adapters with no streaming transport use buffered delivery; internal group
queries do not inherit the public route capability. Route-policy and manifest
contracts exercise this boundary, and the real Parquet E2E checks chunked HTTP
framing as well as the decoded response.


Remote text metadata predicates use published relational tuple indexes. Exact
scalar `term` and bounded `range` predicates seek the same authenticated trees
as SQL. Conjunctions can use composite bounds or intersect separate indexes;
disjunctions require every branch to be exact. An indexed conjunct may narrow
a residual predicate, but an exclusion never discards rows using a superset.
Unsupported expressions retain the shared exact residual evaluator.

The consumer builds a Roaring bitmap in the pinned text snapshot's document
number space. Both selective and broad predicates seek the tuple bounds in an immutable
compressed row-set tree. Contiguous per-tuple/file/group physical blocks encode
as extents; scattered selections encode as Roaring sets. Broad categorical
predicates consume blocks rather than one forward record per matching row.
Initial publication aggregates the sorted tuple stream; incremental publication
replaces only changed-file blocks through bounded spill and copy-on-write pages.
The forward/reverse trees remain available for SQL seeks and file replacement;
predicate evaluation does not scan the unrelated physical reverse tree. Native text metadata version 8
publishes a delete-aware physical-row ordinal directory per file. Contiguous
2^20-row blocks need only an ordinal base and extent; blocks with holes use an
authenticated Roaring bitmap and rank. Group identifiers and the high bits of
64-bit row coordinates select blocks without truncation. Mapping an index result
needs neither formatted private IDs nor binary/sequential stored-ID decoding.
The bitmap is applied before top-k and exact counts, preserving corpus statistics.
Text metadata version 8 and ordered-row metadata version 5 rebuild automatically
under reader/topology protocol 33. Existing publication and spill quotas still
apply; this change does not claim a measured 50-million-row build capacity.

Planning consumes the shared compiled predicate IR. Shorthand terms, field/path
aliases, range aliases and boolean structure have one grammar. Standard numeric
ranges compare integers exactly, including mixed integer/float bounds, using the
same scalar order as SQL. A planner uses a tuple index only when its order proves
the shared predicate: RFC3339 ranges over string columns retain chronological
fallback evaluation, and integer bounds that cannot be represented exactly in a
floating-point index remain residual. Explicit datetime columns also retain the
shared evaluator until their unit/normalization contract is proven compatible.

Dense, sparse and mixed retrieval use compressed physical-row predicate sets,
without the legacy 100,000 matching-ID limit. Dense leaf admission reuses batched
native metadata reads before scoring; scalar/rerank paths enforce the same
membership callback and propagate errors. Sparse postings point-seek existing
native reverse identities and cache accepted/rejected doc numbers in compressed
bitmaps across terms. Membership is enforced before top-k, including when a lower
scoring eligible row would otherwise fall outside an unfiltered candidate page.
Candidate scoring still follows each native index's search/effort semantics.

Native field sorting can use the shared doc-value top-N heap for remote text.
It iterates compressed membership instead of materializing all candidate doc
numbers and projects public IDs before tie breaking or cursor comparison. Only
the selected page loads source. Public scores are evaluated for that page against
the original query and complete corpus statistics. A producer's private-ID sorted
segment attestation does not imply public-ID order; such plans use the native
comparator heap. This path requires complete native doc-value coverage.

Corpus construction assembles bounded projection batches (65,536 documents,
8 MiB source bytes or 24 MiB arena capacity, whichever comes first), then uses the
shared 8 MiB segment/32 MiB build-budget splitter. Resetting the projection arena
releases its capacity before the next batch. The old 1,024-document flush and
512 MiB total encoded-output limit no longer bound a corpus. Immutable per-file
manifests keep segment and ordinal references out of the bounded root; unchanged
files reuse their segments and ordinal maps. Manifest metadata, segment sizes,
snapshot doc-number capacity, cache admission and cancellation remain bounded.
GC follows manifests, ordinal hole bitmaps and native segment directories for
both current publications and retained reader roots, with durable continuation.

Public consumed delivery can hydrate 64 ranked hits at a time, grouping physical
reads inside each window, attach highlights from hidden dependencies and append
validated fragments directly to the private response spool. Source leases and
highlight payloads are released after each fragment. A complete hydrated source
array is no longer retained before spooling. Validation, exact length accounting
and resource failures still precede headers; replay retains the publication lease
and observes backpressure. Internal reusable results remain eager. Ranking/result
metadata is still admitted separately; this is not an unbounded result cursor.

When a scalar predicate has no published index, the same consumer can use a
bounded typed SQL scan. It pushes conditions into Iceberg partition, Parquet
row-group and page pruning, then merges physical selections into the native
bitmap. Indexed conjuncts take precedence over scanning an unindexed conjunct;
the remaining expression is evaluated only on the narrowed text candidates.
Eligible match-all counts use per-segment bitmap cardinalities and do not
allocate candidate hits for the whole matching population. Other query shapes
retain their existing candidate and response resource budgets.

Background native publication and native runtime/text cache heaps use the platform process allocator beneath their existing working-set and resource-manager budgets. Bulk sparse/dense and ordered builders retain ordinary small heap entries; using the page allocator here would create a mapping per entry and amplify resident memory and teardown costs. Budget admission and cancellation remain enforced by the build owner.
