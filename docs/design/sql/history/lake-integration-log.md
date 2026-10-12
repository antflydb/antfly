# SQL extraction: external lake SQL and native lake publication log

> Relocated verbatim from `docs/sql-extraction.md` (lines 168–731, 4363–4394, 5263–5292, 5902–5929) at commit 3ce5aff4ea on 2026-10-12. Durable decisions were folded into [SQL extraction](../extraction.md) before the move. This is a historical implementation log; it describes work at the time it was performed, and its counts and test totals are not current guarantees.

### External lake SQL integration

Relational table schemas can attach a read-only Parquet prefix or Iceberg table
with `base_source`. The attachment persists with the native schema and is
resolved through the catalog for public typed-row queries and SQL over HTTP or
pgwire. Table creation can infer `document_schemas` and `schema_fingerprint`
from lake metadata. For example, this table schema needs no column declaration:

```json
{
  "storage_mode": "relational",
  "base_source": {
    "kind": "external",
    "table_id": "events",
    "format": "parquet",
    "uri": "s3://analytics/events",
    "credentials": {"ref": "analytics-lake", "scope": "events"},
    "snapshot": {"mode": "current"},
    "write_policy": "read_only"
  }
}
```

Creation reads Parquet footers or the selected Iceberg schema, without decoding
rows. Parquet inference combines all listed files, marks absent/nullable fields
optional, and rejects incompatible types. Iceberg uses field IDs, types and
requiredness from metadata; a pinned snapshot uses its schema. Empty Iceberg
tables are supported. The discovered columns and fingerprint are persisted
before catalog publication, so SQL Describe and Execute use stable types.

Inference supports flat booleans, integers, floating-point numbers, UTF-8 strings,
timestamps and decimal strings that preserve precision. Decimal values use exact
integer-and-scale decoding up to precision 38, including 16-byte binary values,
and retain trailing fractional zeros. They have SQL string semantics; casts to
floating-point numbers are explicit. Timestamp filters compare exact signed
epoch nanoseconds, accepting timezone offsets and dates before 1970. Nested, binary and
unsupported logical types fail explicitly. Parquet inference needs a schema-bearing
file. Missing optional columns become SQL NULL; incompatible types and missing
required columns fail scans. New fields do not silently expand the catalog.
Explicit schemas/fingerprints remain supported; omitted fingerprints default to
`auto`. Schema discovery also runs through MCP and SQL catalog creation.

Credential references use configured `external_io` connections with `lake_read`
capability and enforce their bucket/prefix scope. Iceberg attachments use
`format: "iceberg"` and the snapshot schema fingerprint (for example,
`iceberg-schema:7`). A snapshot selector can instead pin an Iceberg snapshot ID
or a Parquet object-version digest. Filesystem sources use the existing
filesystem object-store layout under its `antfly` bucket.

Each SQL statement pins source inventories and object versions. Repeated table
aliases share the inventory, and mixed joins retain the native statement read.
SQL applies its existing projections, predicates, aggregates, joins, CTEs, set
operations and windows to lake cursors. Public row queries apply residual
predicates before limits and emit snapshot-derived `_id` values. Iceberg reads
apply the existing position/equality delete machinery. Provider reads receive
the request's cancellation and deadline.

Lake cursors pull typed column batches, retaining one file's footer/delete
metadata and one decoded page per projected column. Opening a pinned cursor does not read data
pages; the first page need not wait for later files. Aggregate execution consumes
selected typed cells directly, and row objects are created at the row API
boundary. Physical snapshot identities use opaque versioned `lake1:` IDs with
numeric row-group/row ordinals. File metadata is ordered by identity before
scanning, so pagination needs no full row sort. IDs are stable within a snapshot;
clients must preserve them verbatim and must not parse or synthesize them.

Simple integer, string and boolean comparisons prune disjoint row groups from
footer min/max statistics, and files can be skipped when their known row groups
are all disjoint. Iceberg partition pruning resolves each file's spec ID and source
field IDs, then conservatively projects identity, bucket, truncate and temporal
predicates. Unknown specs, transforms, values and comparisons remain residual. Missing statistics, annotated decimal/timestamp types and
unsupported comparisons stay residual; predicates and Iceberg position/equality
deletes are evaluated before page limits. Exact unfiltered `COUNT(*)` uses footer
row counts only when no deletes or cursor constraints apply. Other aggregates,
filtered counts and delete-bearing counts scan the selected rows normally.

API SQL reads share a server-owned 64 MiB range cache (at most 4,096 entries).
Keys bind the credential reference/scope, source endpoint, object version and
byte range. Unversioned reads bypass it; cache hits still check cancellation and
deadlines. Shared cached bytes are separate from each statement's memory budget.
The cursor prefetches exact upcoming data-page ranges, next-group header probes
(or the next file's footer) through the existing I/O runtime: at most four
concurrent reads and 32 MiB of requested ranges per lookahead batch. Worker buffers use
independent allocations outside the SQL arena; provider response copies can add
temporary memory. Closing or canceling a cursor cancels and joins every worker
before releasing source metadata. Prefetch failures stay speculative: required
reads still enforce pinned versions, deadlines and cancellation.

Iceberg admissions revalidate the metadata pointer and content, then lease an
immutable snapshot plan from the decoded cache. Each admission owns its mutable
file-version state. Partition and file-bound pruning happen before data-object
HEAD requests; surviving files are pinned once before reading their footers.
Prepared equality/position delete indexes also use cache-owned immutable leases,
keyed by plan/applicability, limits and provider versions. Unversioned objects
bypass index reuse. Request cancellation handles and footer-derived position
offsets remain outside cached membership state.

Each stream admits at most 100,000,000 examined rows. Parquet decoding aligns
independent column pages and decodes each column dictionary once. Up to four
column decoders overlap provider reads and page decoding; allocation admission
is serialized, and every worker joins before releasing cursor state. The 32 MiB
input/decoded budgets apply to the active page set, rather than the entire row group. Large
individual pages and dictionaries can still fail admission. SQL defaults admit
10,000,000 scanned rows, 65,536 scan pages and 256 MiB retained bytes per statement.
The embedded SQL JSON request limit is 64 MiB (67,108,864 bytes), including
statement text and parameters. Preparation and execution each have a 256 MiB
working-memory budget: decoded inputs, mutation staging and storage encodings
can coexist, so this is distinct from the wire limit. Embedded SQL sessions
admit 4,096 staged mutations and 256 MiB of staging memory. Native transaction
intents additionally use the configured admission limit (128 MiB by default). Oversized
requests return SQLSTATE `54000` with the numeric wire limit; execution remains
bounded by memory, result, syntax and work quotas rather than a 2 MiB value cap.
Nested pipelines reduce internal page sizes under smaller budgets.

Blocking sorts, grouped aggregates (including DISTINCT inputs), hash-join build
rows and window partitions spill through a shared statement owner. In-memory
sort heaps grow with admitted rows instead of eagerly allocating their maximum
logical cardinality; growth and row storage share the retained-byte budget.
Sorts merge
bounded runs and stream past OFFSET; the final merge reads up to eight run heads
directly without writing another complete run. Groups merge partial states one
key at a time. Spilled joins sequentially hash-partition both sides, retaining
one build partition at a time and preserving outer-join match markers. A
bounded runtime filter skips certainly absent probe keys when outer semantics
permit it. Oversized partitions recursively split on unused hash bits with bounded depth
and open files; inseparable duplicate keys retain the disk-chain fallback.
Snapshot-local row estimates, bounded derived-query LIMITs, and source byte
estimates choose the smaller build side when comparable estimates exist. Window passes retain partition
rows, peer/group directories and aggregate frame trees on disk, with small
tracked caches. Separate cell records store window outputs without rewriting
the original row payload for each function. Window sorts carry row indices instead
of wide row payloads. Sliding COUNT, integer SUM, BOOL_AND and BOOL_OR use exact
removable state with constant memory; other aggregates and exclusions retain
the frame tree. Ranking, navigation,
ROWS/RANGE/GROUPS and frame exclusions keep their existing semantics. Quantified pattern sets use external DISTINCT and a
reusable statement-owned file; matching retains one pattern at a time.

API SQL statements default to a shared 1 GiB live spill quota and at most 64
open spill files per statement. `sql.runtime.Limits` exposes `spill_bytes`
(`0` disables spilling) and `spill_root` (default `/tmp`). Temporary directories
and files are private; files are immediately unlinked while open, and handles
and directories are cleaned up on completion, cancellation and error. Spill
records preserve exact numeric tags, SQL/JSON null distinction and row order,
with length and checksum validation. Disk exhaustion and oversized records
still return errors. Spill I/O uses bounded read-ahead and double-buffered
asynchronous writes where concurrency is available. Repetitive large records
use the existing Snappy codec when it saves space; decoded lengths and checksums
remain validated before rows are accepted. Small join budgets reduce per-file
buffer sizes to leave room for the active partition.

Pgwire can deliver sorted, grouped and window results in bounded pages, beyond
the materialized response row cap. Execution pins one statement cut and
transfers ownership of the completed sort into the result cursor, avoiding a
second full result spool. Delivered in-memory rows are released immediately;
external sorts merge directly into continuation pages. Other blocking shapes
retain the spool fallback without rescanning source data. Explicit SQL LIMIT/OFFSET, ordering and NULL flags are
preserved. HTTP JSON response limits remain; blocking projections involving
external decision providers retain the existing bounded materialization path.

Numeric and boolean expression batches use bounded instruction-major kernels,
including four-lane exact integer and floating-point arithmetic/comparisons.
String comparisons and boolean unary operations also run by batch. Eligible
streaming filters/projections and aggregate input expressions consume selected
column pages directly, applying predicates before computing projections. Live
intermediate vectors reuse workspace slots; global COUNT, integer SUM and
boolean reductions avoid per-row grouping probes. Mixed numeric values retain
scalar conversion semantics. Lazy expressions (CASE, AND/OR) and
unsupported types/functions use the scalar evaluator.

Repeatable native microbenchmarks run with
`zig build sql-native-refinement-bench lake-native-refinement-bench -Doptimize=ReleaseFast`
from `zig/`. The measured baseline and refined paths validate equivalent outputs
and alternate execution order across three samples at each size. Raw samples,
fixture details and median timings are in
[`native-sql-refinements.json`](../../../../zig/bench/baselines/native-sql-refinements.json).
The sort and partitioned-join cases also report first-row latency, allocator
peaks, backing allocation counts, spill bytes, and physical I/O calls; their
single-sample results are in
[`native-lake-execution-refinements.json`](../../../../zig/bench/baselines/native-lake-execution-refinements.json).
Partitioning can increase writes and first-row latency while reducing random
reads, so those samples are not a general throughput guarantee. Merge-head
arenas retain bounded capacity between records to reduce allocation churn.
These measure CPU kernels and local temporary-file writes; remote lake latency
and end-to-end query throughput need separate measurement. Vector batching
uses more bounded workspace than row-at-a-time scalar evaluation, while reuse
lets long expressions run without retaining one vector per instruction.

The regression fixture scans 131,072 and 1,048,576 integer rows in 65,536-row
Parquet groups under a 32 MiB tracking allocator. Peak tracked allocations are
7,081,703 and 7,081,817 bytes respectively: input growth adds file metadata rather
than retaining all rows. This measures cursor memory for that fixture, not
process memory or query latency. Tests also verify lazy first-page I/O, warm
range reuse, pruning without decoded pages, changed-object rejection, exact
large integers, SQL/JSON null distinction and Iceberg deletes before limits.
Forced-spill SQL tests execute sorting with large OFFSET, grouping with DISTINCT,
and joins under a 256 KiB statement budget. Operator tests cover duplicate join
keys and unmatched rows; spill tests cover quota failures, cancellation, checksum
corruption and cleanup. Prefetch tests prove overlapping reads, warm reuse and
provider-token cancellation independent of the worker I/O runtime. Kernel tests
compare results and numeric errors with the scalar evaluator.

The independent-file integration fixtures are written by PyArrow, with Snappy,
SQL nulls, multiple row groups, and dictionary and plain encodings. Native tests
infer and decode every row. The production end-to-end test additionally creates
an attachment without an explicit schema, checks HTTP projections, aggregates,
joins (including a dynamically filtered probe) and compatible windows, streams all rows with psycopg, checks read-only rejection,
and repeats reads after a cold process restart. Run it from `zig/` with:

```sh
ANTFLY_BIN="$PWD/zig-out/bin/antfly" uv run --project e2e/antfly --extra lake pytest e2e/antfly/test_lake_sql.py -n 0
```

The filesystem fixture wraps the independently generated Parquet payload in
Antfly's existing object-store envelope; `file://` is that provider's namespace.
Iceberg rename tests separately verify equality deletes resolve field IDs in
both data and delete files rather than trusting physical column names.

The native batch interface is internal, not Apache Arrow. Retained join payloads,
join keys and grouping keys use typed primitive columns with packed SQL-null
validity. Rows are reconstructed at candidate/expression/result boundaries.
Complex JSON and heterogeneous columns retain native JSON values rather than
converting exact integers to doubles. Aggregate state uses one compact vector
per aggregate: counts, i128 sums, compensated floating sums/means, booleans and
typed extrema. DISTINCT, pattern sets and complex extrema retain their explicit
dynamic ownership. Legacy variable-width join inputs preserve their row widths.

Sort and top-K comparisons cache fixed 32-byte normalized prefixes. Signed
integers, finite floats, booleans and escaped byte strings use memcomparable
encodings; NULL placement and direction remain explicit. Exact type-layout
metadata prevents mixed integer/float shortcuts, signed zero is canonicalized,
and truncated/complex comparisons fall back to the scalar comparator. Original
keys remain available for exact ties, peer detection and spill interoperability.

Nonrecursive equijoins install immutable composite Bloom/range evidence in a
supported direct probe scan after the build completes and before its first
pull. Filters are skipped on preserved probe sides of outer joins and for
computed keys. Lake cursors apply Bloom evidence before producing selected
cells; range evidence also prunes files, row groups and Iceberg partitions
through the existing field-ID-aware planner. Unknown physical statistics remain
residual. Empty builds need no probe data decoding. The exact hash join and ON
residual still decide every returned match, and borrowed evidence outlives pulls.

Window requirements with compatible ORDER BY prefixes and identical partition
keys share the strongest permutation when their consumers are peer-invariant.
Each weaker requirement builds its own peer/group boundaries. Navigation,
ROWS frames, order-sensitive exclusions, floating sums and complex extrema
retain independent stable tie ordering. Both in-memory and spilled execution
use the same compatibility plan.

Expression stripes, Parquet decoding, speculative prefetch and spill writers
share one native admission scheduler: at most eight asynchronous tasks and
64 MiB of reserved staging/workspace across statements. Expression pages of at
least 1,024 selected rows split into at most four independent stripes. Required
work executes inline under saturation; speculative prefetch yields. Allocator
admission is serialized, and task leases release only after await/cancel joins
workers, including cancellation before start. Statement memory/disk budgets
remain separate from this global concurrency allowance. This uses the existing
I/O runtime rather than creating independent pools for each operator.

Window inputs spill as bounded column blocks with disk directories and a
four-block decode cache. Key sorting and frame arguments load only requested
columns; window outputs retain the shared cell sidecar. Spill I/O buffers and
execution chunks scale with the statement memory quota. Native execution chunks
have a 4,096-row upper bound, independent of response-page size.

Large eligible lake scans can split into ordered ranges whose workers run the
complete scan/filter/projection pipeline. Multi-file ranges discover footers
lazily and own independent mutable object versions. Bounded typed queues apply backpressure;
the consumer preserves source order and delivers valid rows before a later
worker error. Early closure and cancellation join workers before
releasing the parent snapshot. LIMIT/OFFSET, small scans/budgets, external decision
expressions and scheduler saturation use the existing serial pipeline.
Homogeneous hash keys compare retained primitive columns directly; batch hashing
avoids transposing Datum rows and probing interleaves independent bucket chains.

The local key-only window fixture measured about 20x lower elapsed time with
column blocks across three samples. Physical reads decreased by about 32% for
its repeated, compressed 8 KiB payloads. These numbers measure decoding and local
spill reads, not total query throughput. Raw samples and the existing join
comparison cases are recorded in
[`native-lake-column-pipeline-refinements.json`](../../../../zig/bench/baselines/native-lake-column-pipeline-refinements.json).

Further tuning includes richer statistics-driven costing, runtime filters
through derived/computed scan mappings, and more parallel aggregate/join
pipelines. Current scalar and spill fallbacks preserve supported SQL semantics.

`EXPLAIN` identifies `Lake Scan`; verbose plans include the source format and
configured snapshot selector without opening the source. Unsupported Parquet
encodings fail through the existing lake engine. Source attachments reject native write constraints, relational schema indexes, defaults,
generated columns and TTL. SQL mutations and public batches reject writes;
existing native tables cannot acquire or remove an attachment through a schema
update. External scans reject native serializable range-proof requests and
active row policies or row filters. Public secondary-index cursors and explicit
collations are unsupported. Lake sidecar and operational engine APIs remain the
existing standalone interfaces.

### Native lake aggregate publication and reuse

External tables accept opt-in indexes. Public aggregate recipes can be included
in the table's `indexes` at creation or added through the existing index endpoint:

```json
{
  "type": "algebraic",
  "derive_from_schema": true,
  "aggregates": [
    {"name": "rows", "op": "count"},
    {"name": "amount_total", "op": "sum", "measure": "amount"}
  ]
}
```

HTTP and MCP creation infer the schema first, then expand and persist the same
canonical index definition used by installation and publication. Public recipes
remain catalog provenance; the storage engine receives its strict runtime
configuration. Each index accepts at most 64 recipes, and external-table admission
checks the shared 4,096-declaration directory ceiling before committing definitions.

Native metadata protocol 23 stores a checksum, length and immutable artifact
reference for the publication directory, rather than embedding every declaration
in the 256 KiB catalog record. The directory is bounded to 16 MiB; legacy inline
catalogs remain readable. Directory and inventory bytes use the shared verified
memory/disk cache after fresh source, credential and store validation. Source
object checks run in batches of up to eight through the shared scheduler; cached
payloads do not replace fresh provider evidence.

Builds renew their five-minute ownership lease with a separate heartbeat. A lost
or ambiguous metadata CAS is terminal and is never replayed. Provider cancellation
callbacks perform no metadata writes. Upload completion takes an owned snapshot of
the latest fence and publishes through an exact table-definition CAS. The build
request has a separate 24-hour deadline, and closing joins the heartbeat and all
speculative reads before releasing their owners.

Compatible recipes share a scan and typed blocks. SQL readers authenticate each
logical root, then decode matching cohort blocks once and map their states into
requested reducer slots. Independently ordered materializations retain sparse
composition. Exact integer/count/boolean lake aggregation can also use row-group
workers; floating sums and averages retain their ordered reductions.

Unchanged aggregate roots survive definition-only updates when the complete
source/schema, credentials, store and recipe proofs match. Exact mergeable recipes
also persist per-file contributions inside the directory. A changed-source build
merges authenticated states for unchanged versioned files and scans only new or
changed files. File proofs include source/schema identity, URI, provider versions,
byte length and exact typed recipe. Contributions are retained only for live files
and desired recipes, with at most 16,384 entries. Deletion-bearing sources,
floating keys/extrema and non-associative floating reductions use the complete
scan; they do not inherit contributions under a weaker proof. The real PyArrow
E2E regression covers 320 inline recipes, append rebuilds, unchanged contribution
identities, HTTP/pgwire results, selected-artifact corruption and cold restart.

Published and queryable remain separate states. Native exact aggregates and
ordered relational indexes become queryable after current source proof and
reader-contract verification. Remote `CREATE INDEX` and the relational index
API use the schema definition as their durable build obligation; publication is
asynchronous and readiness is explicit. Remote tables remain read-only for row
mutations and unsupported constraints.

Ordered indexes use the native tuple encoder, expression evaluator, NULL
placement and partial-index membership rules. Shared delete-aware source scans
feed bounded native sort/spill runs into immutable authenticated B+tree pages.
A key has a physical file/group/row tie, so equal tuples retain distinct rows.
SQL equality prefixes can select these indexes automatically, with a scan
fallback only before an artifact is selected. Explicit row queries require a
ready publication, preserve index order across bounded hydration windows and
return exclusive cursors fenced by schema, index semantics and immutable root.
Integers do not cross floating point. Direct key columns and `INCLUDE` columns
are retained in native column blocks; fully covered projections and residuals
read those blocks without Parquet data-page hydration. Other queries prune
unselected files, row groups and pages and preserve Iceberg delete filtering.
Authenticated page/block reads share the scoped persistent lake cache, whose
contents never replace current source or authorization proof.

Cold lake serving uses a server-owned, single-worker `std.Io` persistence lane,
independent of request/provider executor capacity. Shutdown drains accepted
writes before releasing its executor and resource manager. Keep the configured
`lake_cache.root` on persistent local storage with a single process owner to
reuse authenticated ranges across restarts. Cache availability is optional:
missing local paths, ownership contention, startup failures and resource pressure
must not become source authority or query readiness failures.

A failed disk-tier initialization is retried opportunistically by a later
request after a 30-second backoff. No request sleeps during backoff, and a
successful owner is published exactly once with acquire/release synchronization
for readers already serving from RAM/source. Initialization attempt/failure
counters expose repeated recovery failures; a successful retry clears the
current unavailability reason. This permits recovery from temporary owner-lock
contention without requiring another server restart. It does not repair invalid
configuration or guarantee a hit for data read while persistence was unavailable.

The data-server metrics expose `antfly_lake_cache_disk_ready`, initialization
failure reasons, disk/mapping hits, provider reads/bytes, asynchronous write
errors, queue depth and policy/queue/memory/capacity/allocation/closing drops.
Source capture, publication loading, index acquisition, ranking, hydration,
highlighting and total query timings are reported separately. Phase times can
overlap (for example index acquisition inside ranking/highlighting); do not sum
them as exclusive elapsed time. Public lake `took_ms` includes cold setup rather
than starting only after publication/index preparation. Provider cache counters
describe payload reads, not all GCS HTTP attempts, metadata requests or retries.

Seekable text readers coalesce a fully requested, authenticated 1 MiB pack into
one GET instead of four 256 KiB GETs without increasing transferred bytes.
Partial cold reads retain 256 KiB units; a cached pack can satisfy those units
without a provider read, including after restart. Prefetch admits at most four
units/packs concurrently (at most 4 MiB of source bytes), charges scheduler
memory, shares singleflight keys with required reads and joins cancellation.
Both pack and constituent-unit digests remain enforced.

Native text artifact metadata version 9 records stored projection coverage when
an index opts in to source storage. Single-text-source typed result pages use that coverage
proof and native row identity to hydrate covered fields directly from the text
snapshot. Uncovered display/source fields still use the shared delete-aware
Parquet cursor. Mixed/composed sources without one proven text identity retain
physical hydration. This adds build/storage work proportional to the indexed
source projection, not the whole source document. Existing text publications
need a format refresh; reconciliation checks artifact format even when schema
and source signatures have not changed. Retained older roots remain understood
by garbage collection until their reader leases and retirement obligations end.

Local regression evidence: a complete 1 MiB pack needs one provider request,
an isolated cold read transfers 256 KiB, a persisted pack supplies a sparse
post-restart read with zero provider requests/bytes, and fully covered typed
hydration opens no Parquet cursor. These are deterministic local fixtures, not
live GCS latency measurements. To verify a deployment, run the same highlighted
query cold, wait for queue depth to reach zero (and check write errors/drops),
restart against the same cache root, then compare results, elapsed time, cache
metrics and actual GCS request/byte telemetry. Authorization and mutable source
metadata can still require remote validation on a warm cache.

Cold-query qualification separates persistent-byte reuse from decoded runtime
warmup. Server request-stat snapshots include `lake_range_cache` and
`lake_disk_cache`; data/standalone health endpoints expose the matching
`antfly_lake_cache_*`, `antfly_lake_disk_cache_*`, and
`antfly_lake_query_phase_nanoseconds_total` Prometheus metrics. Inspect
`disk_unavailable`, `disk_init_attempts`, and `disk_init_failures` before treating
an empty disk directory as a query performance problem. Optional disk startup
failures preserve RAM/source reads and retry after a 30-second awake-clock
cooldown. Concurrent requests do not wait behind cache inventory startup.
The disk counters include `write_errors`, `last_write_error`, `writes_dropped`,
`queued_entries`, and `writes_completed`, distinguishing a failed worker write
from bounded admission or a write that has not yet drained. A disabled cache or
an enabled cache without a resolvable root does not start a disk owner; configure
`lake_cache.root` explicitly when the node has no local storage base directory.

To qualify a deployment, issue the same highlighted query against one pinned
publication, wait for queued writes to drain, and record provider requests/bytes,
disk hits, and elapsed time. Restart using the same persistent cache volume and
credentials and repeat, then separately repeat with an empty cache directory.
Compare counter deltas on an otherwise idle node: shared cache traffic counters
are not per-request attribution. Completed indexed-search queries also accumulate
publication, search, hydration, delivery, and total nanoseconds without query-log
spam. Hydration is nested inside search/delivery, so these counters must not all
be summed. Decoded structures still rebuild after restart; persisted immutable
artifacts and Parquet ranges can be reused. This procedure is deployment
qualification, not a claim that a particular GCS startup failure is diagnosed.

Cold Parquet reads transfer verified object-response ownership without another
body copy and reuse a header probe when it already contains the complete page.
The projected-page restart regression asserts four cold provider requests and
zero additional requests after reopening the same disk cache with the provider
unavailable, while excluding a 512-KiB unselected column. This is deterministic
request-count evidence, not a latency benchmark.

Full-text indexes can opt in to duplicated source with
`{"type":"full_text","field":"body","store_source":true}`. The default is
false. Native seekable segments retain only the indexed source projection:
`field` limits it to that field, while a default-projection index retains its
projected source. This increases storage and build work, but eligible single
text queries can highlight directly from authenticated stored-source blocks
without adding the body to the Parquet display projection. Explicit highlight
fields outside that projection and named/mixed-query cases keep the existing
Parquet path. Fields still needed for display, ranking, or filtering are not
pruned. Source duplication participates in the immutable build recipe, so older
unattested artifacts fall back to Parquet until rebuilt. The highlight-source
regression removes the Parquet object after pinning/building and verifies native
source hydration, highlighting, and deadline propagation; it does not bypass
the API's current-source/publication authorization checks or claim that a fresh
API request can succeed with its required source proof unavailable.

Reader authority lives in native metadata independently of definition CAS.
One renewable publication lease per process/session amortizes concurrent reads,
protects an old generation across replacement and DROP, and fences every cache
read when renewal fails. Lifecycle rows and retained roots survive DROP and
native metadata snapshots. Retirement atomically closes new reader admission;
a bounded collector marks directories, inventories, contributions and all
aggregate, graph and ordered-index descendants before its scoped upload census.
It preserves pending upload attempts, supports a dry run and resumes the exact
retirement cut after interruption. A delete budget cannot prematurely release
retained roots. Deleting old attempt namespaces alone is unsafe.

Retained publications record a physical store locator and a named connection,
without copying credentials into metadata. Reopening resolves current credentials
and rejects a connection redirected to another endpoint, root, bucket or prefix.
A paginated native metadata work feed includes DROP tombstones. Protocol 24 and
25 publications declare their reader contract; collection preserves legacy roots
and all upload generations below the first leased attempt, including temporary
files. A protocol upgrade alone cannot prove old readers have drained.

The durable table incarnation has a random upload namespace. Collection cannot
cross into another incarnation sharing a store and table number. Store bindings
are recorded when an attempt is admitted, so failed builds and DROP before the
first publication still leave enumerable cleanup obligations. A successful sweep
forgets only its own unused binding; credential rotations retain the other stores.
Historical metadata recovery rechecks the durable publication directory and
rebuilds missing derived storage instead of treating a matching signature as
proof that collected artifacts remain available.

Table backup manifests preserve schema and index rebuild obligations rather
than publication authority. Restore creates an unpublished external table and
rebuilds against its authorized source. The shared native text executor accepts
an owned immutable corpus snapshot, with the same global BM25 statistics, typed
ordering and missing-row filtering as its local storage provider.

Native remote text publication uses the local field projector and segment
builder, including positions and typed values. A bounded process corpus cache
constructs global statistics once per immutable root. Warm snapshots hold
authenticated pinned disk mappings; eviction waits for their leases, while cache
damage is a miss. Publication leases and source/credential checks remain
query-owned authority. Corpus metadata, schema arenas, decoded statistics and
cold payloads share a 256 MiB process heap budget across entries; the encoded
corpus limit remains 512 MiB. Idle entries yield under memory pressure, and
active readers keep their pinned payloads.

Native sparse publication retains the existing sparse index’s exact LSM
manifest and immutable runs. Native dense publication retains HBC posting
authority, its exact committed WAL prefixes, and a separately pinned LSM plane
of canonical float32 vectors. Both reopen through the existing native storage
port over independently authenticated 256 KiB artifact blocks. Native ranking,
constraints, adaptive candidate refill, and named-source fusion remain in the
shared executors. Dense exact-vector loads use sorted batched LSM lookups and
caller-owned decode buffers; physical document delivery also batches rows.
Read-only HBC recovery neither creates directories nor repairs WAL tails, and
rejects mutation before writing. Request runtimes share process memory admission
and enforce a 256 MiB per-request native heap limit. GC marks every retained
file block, including the dense exact-vector source plane.

Artifact collection runs on a separate bounded cleanup scheduling owner, drains
before reader/cache teardown, and enumerates DROP tombstones through native
metadata. Configure `lake_indexes.artifact_gc` with `enabled`, `dry_run`,
`interval_ms`, `max_deleted`, `max_marked`, and `max_read_bytes`. Defaults enable
one pass every 30 seconds with at most 4096 deletes, 262144 marked artifacts and
512 MiB of root reads. Dry runs report eligible/deleted counts without changing
authority or objects. Legacy generations remain protected rather than inferring
an old-reader drain from a protocol upgrade.

Native remote text HTTP execution passes a real Parquet regression over several
native segments, scored offset pages, physical hydration, adjacent exact integers
above 2^53, and cold restart. Named text sources use the shared composition
executor. Ordered search pages carry `remote_snapshot` alongside the ordinary
sort tuple; replay must echo that token and a changed publication fails closed.
The token grants no access and does not retain artifacts. The Parquet HTTP
regression checks score-sort continuation across restart and rejection of
missing or stale snapshot tokens. The real Parquet fixture also verifies native
dense nearest neighbors, exact sparse dot-product scores, sparse residual filters
over integers above 2^53, combined text/dense/sparse fusion, physical hydration,
and vector search across restart. Public graph searches and search aggregations are
not routed through these adapters; SQL algebraic materializations retain their
existing native consumer path. Native dense generation metadata is reopened per
request within its heap budget; a shared detached generation cache and streamed
contiguous native-file mappings remain further optimization work.
Constant-time Iceberg coverage also requires
an explicit immutable-object proof, rather than trusting a snapshot label or a
TTL.

> Continued from the `Public typed-array column schemas` section (lines 4363–4394).

Remote publication metadata now reuses owned decoded-cache leases for inventories,
file maps, declaration directories, and native ordered/text/vector roots. Current
coverage, authorization, and reader fences still precede consumption. Remote native
chunk loading uses per-chunk single-flight admission and bounded successor prefetch;
cache mutexes protect bookkeeping rather than network calls. See
[remote serving ownership and GC](../../../../zig/REMOTE_TABLE_SERVING.md#bounded-publication-reuse-and-collection-progress)
for the durable collection frontier, replacement admission, and shared mapping
accounting contracts.


Variable-width MIN/MAX batch admission includes retained input payloads before
mutating group state, routing oversized cohorts through ordered spill admission.
Remote immutable snapshot coverage proofs reuse canonical data hashing while
refreshing delete evidence. Text corpus limits count unique physical segments
across active generations, with sealed reader composition across overlapping
roots. GC batches verified live-set probes and uses durable filesystem upload
journal offsets; remote providers retain exclusive lexical continuations.


Native lake scan and aggregate ownership refinements are specified in
[REMOTE_TABLE_SERVING.md](../../../../zig/REMOTE_TABLE_SERVING.md#immutable-scan-plans-and-aggregate-reduction-trees).
The maintained contracts include transparent statement cursor capabilities,
immutable snapshot plans with sparse version overlays, mixed algebraic/search
delta replay, exact radix reduction trees, protocol-25 paged contribution records,
and independently completing GC audit censuses. The regression tests enforce
unchanged-file read rejection during mixed-index appends and aggregate append/
removal, subtree reference reuse, immutable manifest retention and split lifetime.

Native lake refinements use reader/topology protocol 26 for keyed contribution
pages and group-key-partitioned aggregate roots. The source, recipe, lease,
spilling, GC, and compatibility contracts are documented in
[Remote table serving](../../../../zig/REMOTE_TABLE_SERVING.md#keyed-contribution-state-grouped-partitions-and-cold-load-admission).

### Cold lake search: restart composition and warm startup admission

The persistent-cache recovery, phase diagnostics, bounded range concurrency,
packed text reads and sidecar hydration implemented earlier are now covered by
a composed ranked-search/highlighting restart regression. It starts with an
empty cache, ranks a top hit, projects display/body fields from the immutable
text sidecar and runs the production highlighter. Shutdown drains accepted
writes. A fresh cache and index writer rebuild decoded navigation from disk
with all provider read methods disabled. Scores and highlights remain valid,
with zero provider requests/bytes and nonzero disk hits. No Parquet reader is
available to mask a source-hydration fallback. This exercises immutable query
payload reuse, not offline publication discovery or a complete HTTP request.

Warm cache preparation now checks the acquire-published owner before path
allocation or startup locking. Failed initialization still takes the bounded
retry path. The server executor/restart test also verifies warm preparation
returns while the startup mutex is held; recovery tests verify the readiness
transition only follows successful initialization.

For deployment verification, keep the same node-local directory across a
graceful restart and compare `antfly_lake_cache_provider_reads_total`,
`antfly_lake_cache_provider_bytes_total`, disk hits and per-phase query timings
for the same highlighted request. Check disk-ready, initialization failure
reasons, completed/queued/dropped writes and last write error before restart.
Counters restart with the process; compare per-run deltas rather than absolute
values. An abrupt crash can lose pending cache writes without losing source
data. Cache eviction, changed object versions or credentials can legitimately
require new provider reads. These local fixtures do not establish the cause of
the observed deployment's missing cache files or quantify live GCS latency.

### Cold lake reads: compatibility-grouped coalescing

The requested persistent-cache diagnostics/recovery, server-owned write worker,
shutdown draining, bounded concurrent prefetch, phase timings and sidecar-based
highlight hydration are already implemented on this branch. The restart
regressions cover immutable payload reuse with the provider disabled, not
offline publication discovery or live GCS latency. Deployment verification still
requires the same node-local cache directory and per-run provider request/byte
deltas; local tests cannot explain the previously observed missing cache files.

Physical range planning now groups the complete coalescing compatibility class
before sorting offsets. Previously, interleaved object versions, codecs or
decoded-column identities could separate otherwise mergeable ranges. The new
regression reduces eight such reads to four within the configured gap policy,
verifies each original has exactly one compatible covering range, and verifies
zero-gap policy keeps all eight exact reads without padding. This is a request
count fixture, not a measured production latency improvement. Existing response
size limits and interpretation/version boundaries remain enforced.

The allocation-fault sweep also covers final output ownership: scratch sorting
storage is now freed exactly once if conversion to an owned result fails.
Unrelated in-progress SQL modifier and HTTP discovery edits are preserved.

Validation passes the 35 focused cache/reader tests, 164 lake integration tests
and all 503 lake-native tests without failures or leaks. The composed restart
test preserves ranking/highlights with zero provider requests and bytes.
Formatting and whitespace checks pass. No live GCS latency claim is made.
