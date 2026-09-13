# Relational Mode

Antfly tables are document-first by default: a document is a single
zstd-compressed JSON blob, and every index (`full_text`, `embeddings`,
`graph`, `algebraic`) is *derived* from that blob. Schema is optional and
soft.

**Relational mode** is the second table profile on the same engine. This
document describes the contract and runtime integration. The current runtime
establishes schema validation, column planning, projection, an authoritative
packed-row codec, and the complete base-row lifecycle. It keeps
every piece of the existing machinery — shards, Raft, indexes, enrichers, the
join planner, and the algebraic fold runtime — but changes two things:

1. **Schema is required and closed.** Documents in a relational table must
   match a declared document type; unknown/unbounded fields are rejected
   rather than dynamically indexed.
2. **Typed columns are first-class.** Every declared scalar property maps to a
   typed column (`section/typed_doc_values.zig`) so predicates, sorts, and
   aggregations can be served columnar instead of reconstructed from JSON.

`json` is itself a column type: a `json` column stores an opaque subtree and is
indexed exactly the way documents are indexed today (path-fact projection plus
dynamic templates over that subtree). That gives relational tables typed
columns *and* the schemaless document behaviour where it is wanted.

Relational mode is **not** a separate engine. Internally it is represented by a
`storage_mode` on the parsed `TableSchema`, and relational rows occupy a
dedicated internal key kind so packed values can never be mistaken for JSON.
Schema mutations admit `storage_mode: "relational"`; document-mode tables keep
their existing key and value format.

## Why this fits

The substrate already exists:

- **Typed scalars** — `storage/db/algebraic/value.zig` (`Kind`:
  string/integer/number/boolean/datetime/bytes, canonical encodings).
- **Typed column store** — `section/typed_doc_values.zig`
  (`u64`/`i64`/`f64`/`bytes`/`bool`/`geo_point`, chunked, SIMD bulk reads, range
  scans).
- **Per-field columnar blob with projection pushdown and null backfill** —
  `columnar.zig`.
- **Schema → indexable-field analysis** —
  `storage/db/algebraic/schema_capability.zig` already walks a parsed schema and
  classifies bounded scalar fields vs. skipped dynamic/complex/unbounded ones.
- **Schema evolution detection** — `schema_capability.classifyChange`
  (added / removed / type-changed → `requires_rebuild`).
- **Joins** — relational join planner + distributed executor
  (`api/join_model.zig`, `api/distributed_join.zig`) for row-producing joins,
  and the algebraic fold planner (`algebraic/planner.zig`, `distributed.zig`)
  for distributive aggregations over joins.

Relational mode is therefore mostly *wiring and a required-schema contract*
over things that are already built, plus one genuinely new query operator
(the columnar table scan).

## The pivotal decision: schema-bound authoritative rows

The base store uses one `AROW v2` row per document, bound to an immutable schema
epoch. Declared scalars are stored by stable column ordinal in physical typed
representation; nullable absence and explicit null remain distinct; `json`
columns retain their canonical JSON subtree bytes. Paths and physical types
live once in the epoch's `PhysicalLayout`, not in every row. The legacy zstd
whole-document blob is not double-written.

Each row carries its schema version, a semantic content hash, and a physical
checksum. The semantic hash is computed from canonical typed logical values and
therefore survives equivalent storage-format migrations. The checksum covers
the encoded bytes and detects corruption. Each row deterministically uses the
smaller of two canonical bodies: dense presence/null bitmaps, fixed-width slots,
and a variable offset table for populated schemas; or a sorted ordinal/payload
directory whose ordinal word carries the null bit for wide sparse schemas. The
sparse representation has no schema-width section. Both support direct
projection without reconstructing the whole document.

Bodies of at least 64 KiB use the checksum-groups capability (bit 1): one
little-endian CRC32 per 4 KiB body group, followed by the body length (`u64`)
and a CRC32 of that directory including its length. The body still starts with
the ordinary schema-bound AROW header; group boundaries are physical, not
column boundaries. Smaller bodies retain their single trailing CRC32. This is
a deterministic encoding choice, with approximately 0.1% large-row overhead.
Finalization visits each body byte once, then checksums the small directory.
No legacy encoding of this unreleased feature is required during restore.

Point lookups pin their store probe until projection is complete, rather than
copying the full row. On LMDB, large-row views check the directory and metadata
groups before use and check each selected value's groups before decoding it.
The view retains that obligation across different projections; full reads,
semantic no-op verification, scrubbing, and restore check every group. Backend-
authenticated LSM values do not need redundant AROW checks. CRC32 detects
accidental corruption; it is not cryptographic tamper authentication. Unselected
group damage is detected when that group is read or during full verification,
not by an unrelated narrow projection.

Segment-level typed columns remain a derived acceleration structure. They can
be added for scans, predicates, sorts, and aggregations without changing point
lookup, transaction, backup, or recovery semantics because the packed base row
is already the durable authority.

## Contract rollout

Public create and update requests admit `relational` only after the schema has
passed the closed-row and physical-encoding checks below. Base-row writes,
reads, transactions, replay, recovery, TTL cleanup, split handoff, indexing,
and portable backup all preserve the packed-row contract end-to-end.

In `relational` mode the following are implied/enforced:

- `enforce_types = true` (documents must match a declared type).
- Exactly one non-empty document schema is required; its name must match
  `default_type` when a default is supplied.
- Each document type is treated as closed (`additionalProperties: false`)
  unless a field is explicitly typed `json`.
- Underscore-prefixed fields are not an implicit metadata escape hatch: any
  such value carried in the document must have a declared column. The document
  key remains out-of-band and must not be copied into the value as `_id`.
- `required_fields` requires column presence. A required column is `NOT NULL`
  only when its property schema also excludes `null`.
- A configured TTL field, including the default `_timestamp`, must be declared
  as a `datetime` column so authoritative-row reconstruction cannot drop it.
- Table-level `dynamic_templates` are rejected by the core contract. Scoped
  dynamic rules inside `json` columns require the later JSON-subdocument
  lifecycle integration.

The active public schema is compiled once per immutable write generation inside
the DB. Historical cache misses load only the immutable runtime decode layout;
they are single-flight and never compile a validator that cannot participate in
a write. Every storage entry point validates its final post-transform rows
against the active cached contract, including embedded batches, transaction
prepares, replicated callers, and recovery resolution. API validation remains
an early feedback optimization rather than the only integrity boundary.

Each durable epoch explicitly records whether it requires a public schema.
API-derived relational epochs require their matching immutable public contract;
missing metadata is rejected on open and restore. Runtime-only embedded schemas
instead declare that their physical column contract is complete. Both kinds can
coexist in a table's history and round-trip through portable backup without
inferring intent from absent metadata or dropping public constraints.

The validator's immutable execution plan includes hashed property dispatch for
wide objects (including nested and composed schemas) and deduplicated Thompson
regex programs. Small objects keep linear lookup to avoid hash-table overhead.
Pattern execution uses O(program size) request-local scratch and
O(input length × program size) work, including unanchored substring searches.
Compilation is limited to 4096 states, 16384 parse nodes/lowering steps, and 128 nesting levels;
over-complex patterns are rejected as invalid schema patterns. Concurrent
preparation and restore never mutate shared matcher state. Root constraints
and root members are dispatched separately so ordinary declared fields are
validated once; composition retains its branch-specific checks.

The raw JSON Schema type `json` is accepted for a relational property. It is
not a dynamic-template `AntflyType`: a `json` column is stored as a `bytes`
column and later indexed like a document subtree (path facts + dynamic
templates). It is the escape hatch for semi-structured data inside an
otherwise typed row.

Constraints in scope for v1: primary key is the existing document key; required
column presence via `required_fields`; and `NOT NULL` when a required property's
schema excludes `null`. **Out of scope for v1:** cross-document unique
constraints, multi-document transactions, foreign-key enforcement (use the
`graph` index / join planner for relationships).

## Runtime model

### Column plan

`schema_capability.relationalColumnPlanAlloc` compiles a closed `TableSchema`
into a `RelationalPlan`: one `RelationalColumn` per declared property, each
carrying

- `document_type`, `name`, dotted `path`
- `column_type` — `string` / `integer` / `number` / `boolean` / `datetime` /
  `geopoint` / `geoshape` / `json`
- `physical` — the `typed_doc_values` value type it lands in
  (`bytes_val` / `i64_val` / `u64_val` / `f64_val` / `bool_val` / `geo_point`)
- `nullable` — `true` when the field is optional or its schema explicitly
  permits `null`
- `indexed` — whether to maintain an inverted/typed index for the column
- `is_json` — nested objects, arrays, and `json`-typed fields collapse to a
  single `json` column at their path instead of recursing
- `json_kind` — retains whether a JSON-backed column was declared as an
  `object`, `array`, or unconstrained `json`, so projection can enforce the
  logical container type without reparsing the schema

This reuses the existing `schema_capability` traversal. Unlike the algebraic
`Plan` (which emits group/measure/time *fact* roles and may emit a field under
multiple roles), the relational plan emits exactly one physical column per
property — it is the column catalog.

First-cut physical mapping:

| `column_type` | `physical`  | notes                                 |
| ------------- | ----------- | ------------------------------------- |
| string        | `bytes_val` | keyword / link / text-as-keyword      |
| integer       | `i64_val`   | exact signed integer                  |
| number        | `f64_val`   |                                       |
| boolean       | `bool_val`  |                                       |
| datetime      | `u64_val`   | epoch nanoseconds                     |
| geopoint      | `geo_point` | packed lat/lon                        |
| geoshape      | `bytes_val` | encoded shape                         |
| json          | `bytes_val` | indexed as a document subtree         |

### Write path

Dense preparation gathers cells through recycled worker-local ordinal scratch,
then hashes and emits them in linear time, including rows with optional holes.
The scratch uses four bytes per schema column, with width at most twice the
number of present cells. Genuinely
sparse rows retain present-cell sorting without schema-width allocation.

Retained row regions, worker scratch, and prepared mutation effects charge the
shared `relational.preparation_working_set` resource slice before allocating.
The limit applies across requests (and provisioned groups sharing a manager),
not merely to each request's worker count. Admission denial releases the entire
attempt and returns retryable `ResourceBudgetExceeded`; no preparer waits for
memory while retaining a partial batch. Slice usage and limits are observable
through the standard resource metrics.

Direct transaction intents use this same admission ledger and validate before
the exclusive apply fence. The first prepare atomically persists a schema-epoch
lease with the intents and prepare vote. Later prepares, commit, and recovery
use that immutable epoch and its public validator, even after a newer schema
is published or the participant restarts. Historical write validators are
request-owned and budgeted; the normal read cache remains layout-only. The
lease is retired atomically with intent resolution. Choosing relational mode
on a previously schemaless table is fenced while document intents remain.
API preflight defers to the durable contract for prepared and terminal retries;
it must not reject an accepted transaction using the latest catalog schema.

Commit's intent snapshot and prepared rows share one admission ledger across
retries. Snapshot rows borrow their owned intent envelopes, avoiding a second
payload copy. Once the intent revision is checked under the apply fence,
relational commit retires only the known intent/lock keys without rereading
payloads. Schema and index generation checks still apply to ordinary writes;
index-plan checks also apply to transactions pinned to an older schema.

Prepare persists the canonical AROW alongside a sidecar containing only
API-only `_edges`/`_embeddings` fields in a tagged intent envelope. Ordinary
columns are stored once, not duplicated as JSON. Physical representability
(including finite f32 embeddings) is checked before the durable vote. Commit reuses that row instead
of repeating schema validation, semantic hashing, and physical encoding. The
commit root comes directly from typed cells; only JSON-typed columns and the
special-field sidecar require JSON parsing. Legacy index consumers receive a
logical rendering, while ordinal consumers use the typed view. Transaction
preparation uses bounded `std.Io` tasks and resettable worker scratch. Recovery
copies the verified AROW and
finalizes its timestamp without parsing JSON. Both paths own a single intent
snapshot through revision-fenced resolution and charge the same resource slice.
The recovery-context mutex uses `std.Io` and protects only a short context copy,
not intent loading, validation, or encoding.

Transaction admission is cumulative and durable. Each intent has a point-read
membership/credit record; a fixed 16-byte header stores count and total credits.
A prepare updates only touched members and the header in the same atomic batch
as the vote. Replacements subtract the previous credits; repeated prepares do
not double-charge. This removes whole-manifest rewrites across incremental
prepares. Released document transactions with the previous manifest pay one
conversion pass; there is no compatibility format for earlier PR-only rows.

Credits conservatively allow 64 bytes of working space per retained sidecar/AROW
byte, 16 per key byte, and 4096 per row. The logical ceiling is persisted in the
table catalog (128 MiB of credits by default), independent of each replica's
memory configuration. Replacements and duplicate keys are coalesced before
payload envelopes are allocated, and those allocations share the request's
tracked preparation budget. Oversized additions fail
before voting with `TransactionTooLarge` (HTTP 413); shrinking and identical
retries remain allowed after a limit reduction. Temporary contention still
returns retryable `ResourceBudgetExceeded`; it cannot change the replicated
transaction decision. Raft records `TransactionTooLarge` as a command result
and continues applying subsequent entries, including the coordinator's abort.
These are conservative admission
credits, not a guarantee against arbitrary generated-index expansion or a
subsequent reduction of the node memory limit. Larger transactions require a
larger execution envelope within the catalog's logical ceiling; spillable atomic
commit remains future work.

Field-backed dense indexes consume the prepared row's typed ordinal view.
Decimal vector elements round directly to f32 once, so foreground indexing,
row projection, semantic hashing, and index rebuilds use identical values.
JSON-backed vectors retain the document extraction path.

Every request pins one immutable `SchemaView`. JSON is parsed once into an owned
`PreparedRelationalWrite`; one schema-guided preparation fills ordinal logical
values for public-schema validation, special-field and index extraction, the
canonical semantic hash, TTL resolution, and `AROW v2` encoding. Ordinary rows
borrow the request body for derived consumers instead of copying it; rows with
reserved fields clone and stringify one stripped logical tree. Large batches
prepare on the bounded runtime worker pool into one ref-counted arena per
worker, so allocator synchronization occurs at page granularity without one
allocator/page chain per row. Parsed trees survive preparation only when a
synchronous base-document text consumer or split shadow needs them. Vector-only
and artifact-only `full_index` writes recycle parse scratch per row rather than
retaining all batch JSON trees. Store keys, timestamps, and derived write effects
are prepared from a ref-counted immutable
`WritePlanSnapshot`. Graph/vector extraction and generated-enrichment templates
are compiled once per durable catalog generation, so foreground work does not
hold a live catalog lease per row. Slow embedding and asset provider calls run
before the exclusive DB apply fence, allowing ordinary commits to continue. A
bounded optimistic retry rebuilds preparation if either pinned generation
changes; the exclusive apply section checks only the schema epoch and write-plan
generation before committing primary rows, identity metadata, catalog state,
indexes/outboxes, and transaction markers atomically.

Ordered transforms use the same prepared-row path. Their durable base values
and versions are captured under the shared apply fence, the effective rows are
coalesced and prepared without the exclusive fence, and commit revalidates the
pinned read set before applying. A changed base causes a bounded retry rather
than a stale transform. Per-row generated-enrichment plans borrow strings from
the pinned immutable write plan and allocate only their filtered consumer
vectors, avoiding configuration-sized allocation and copying per document.

The durable table catalog records storage mode, active schema version, format
capabilities, index build state, and whether user data exists. Exact cardinality
remains in transactional identity metadata, so ordinary writes update the
catalog only on meaningful state transitions. First-schema admission is O(1)
after a one-time streaming reconciliation of legacy stores.

Portable backup uses a manifest-first `AFB2` stream. Runtime and public schemas
are persisted by version before row blocks. Restore validates each row with its
declared immutable layout, recomputes its logical hash directly from typed
ordinal cells, canonical-checks JSON subcolumns, and invokes the matching
compiled public validator only for higher-order schema constraints. Per-row
scratch arenas are recycled, and block/footer checksums are verified while the
archive streams into an unpublished staging database. Each decoded AFB2 block
is validated and imported in the same pass; it is not parsed once for validation
and again for application. Only a completely validated stage is atomically
published, giving bounded memory, cancellation safety, and no partially visible
restore.

`schema_capability.projectRelationalRowJsonAlloc` parses and turns a document into one typed
cell per declared column (`RelationalRow` / `RelationalCell` / `ColumnValue`),
ready to hand to `section/typed_doc_values.zig` at segment-build time:

- a missing required column is rejected with `error.MissingRequiredColumn`;
- an explicit null that the property schema does not admit is rejected with
  `error.InvalidColumnValue` — together with required presence, this enforces
  `NOT NULL`;
- a value that does not match the declared column type is rejected
  (`error.InvalidColumnValue`) — relational columns are strict;
- nullable columns absent from a document produce no cell (the typed column is
  sparse, matching `typed_doc_values` doc-id semantics);
- an explicit null produces a cell with `is_null = true`; segment storage must
  preserve that state separately from a sparse/absent value;
- `json` columns are stringified to bytes and flagged `is_json` so the write
  path can additionally project the subtree via `pathfact` + dynamic templates.

Numeric physical encoding matches `typed_doc_values` and is order-preserving so
range scans work directly on the packed column:

- `number` → `f64` (native);
- `integer` → `i64` for exact signed round-tripping and native signed reads;
- `datetime` → `u64` epoch nanoseconds from a non-negative epoch integer,
  integer-string, or date-only string; timestamp strings use the RFC 3339
  profile representable by this encoding (at most nine fractional-second
  digits and no leap-second `:60` values), with numeric offsets normalized to
  UTC;
- `boolean` → `bool`, `geopoint` → packed lat/lon, `string`/`blob`/`geoshape`
  → `bytes`.

Numeric schema literals that govern a physical `number` column (bounds,
`multipleOf`, `const`, and `enum`) must round-trip through that same `f64`
representation without changing their mathematical value. Schemas containing
an unrepresentable literal are rejected up front. `multipleOf` is evaluated in
the canonical decimal domain without a floating-point tolerance.

Round-trip through the real `TypedDocValuesWriter`/`TypedDocValuesReader` is
covered by unit tests.

**Table-owned column accelerator:** `relational_columns.zig` streams a pinned
primary snapshot into hidden, schema-bound blocks of at most 256 rows or roughly
1 MiB of source rows (an individual large row remains subject to the request
budget). Per-column `TypedDocValuesWriter` instances consume AROW cells directly,
alongside presence and null bitmaps; no JSON projection or reparsing belongs in
this build path. ACB8 separates row metadata from independently addressed,
checksummed per-column metadata. The root contains a sorted sparse directory
of 64-ordinal presence pages (12 bytes per populated page); binary search
locates only the columns a predicate/projection requests, without allocating or
decoding every column's bounds and bitmaps. Missing declared metadata is
corruption, not a missing field. Existence predicates and null projections need neither payload I/O
nor value-stream decoding, even for large vector/JSON columns.

Payloads have independently checksummed row-group pages. The builder partitions
actual cell bytes at a 16 KiB value budget, with explicit exclusive row ends and
encoded byte sizes. Each 46-byte descriptor contains an exclusive destination
row end, encoded byte size, BLAKE3 payload identity, source-row offset, and
source-row count. Payload doc IDs are page-local, not block-local. Oversized values get singleton pages;
adjacent small/null/missing values never share their payload. Scalar columns
usually stay in one page. Directory validation requires strictly increasing
ends, exact row coverage, and agreement between payload sizes and presence/null
bitmaps, including zero-byte absent/null-only groups. Predicates load pages intersecting their surviving
candidate mask; projections load only the pages containing delivered rows.
Repeated predicates share decoded pages. The cost model charges only still-
unread pages containing surviving values, and `payload_pages_read` exposes the
actual I/O alongside bytes read. A block caches decoded payloads by identity,
so multiple mapped fragments do not reread or decompress the same object.

Payloads are immutable, content-addressed objects scoped to a generation.
Identity hashes canonical typed cells and page-local ordinals before compression;
the payload CRC independently checks physical integrity. Reads validate both.
Block metadata owns durable reference counts and encoded sizes in separate
checksummed small records, so
retaining a page never rewrites its payload. Staging atomically retains all
references with its column metadata; publication atomically removes old roots
from the directory and enqueues durable retirement intents. Bounded maintenance
releases retired metadata's references, deleting payloads only when their final
reference disappears.
Store MVCC preserves deleted payloads for already-pinned readers. Abandoned
staging GC releases references in the same transaction that deletes its
metadata. Old-generation GC can delete its entire namespace incrementally,
because references never cross generations. References point directly to
payloads, never to other blocks: repeated compaction cannot build lookup chains.
Point projection resolves a row's page with binary search only on a cache miss.
Cached typed cells are addressed directly; null/cell slots are initialized once
per column/block. `cell_slots_initialized` and `cell_cache_hits` expose this CPU
work independently of payload I/O. There is no per-projected-row bitmap/page
rescan, and no compatibility decoder for previous PR-only column formats.

Column read metadata is separate from decoded/materialized values. Metadata
views use a compact loaded-page bitmap and allocate no JSON slots. Logical
slots are allocated only on first materialization, sized to the actual block
row count; scalar and existence predicates never allocate them.
`column_view_bytes` and `logical_slots_initialized` expose these costs.
Logical column values borrow strings and exact JSON number tokens from their
pinned decoded payloads. Only containers and escaped tokens need new storage;
an iterative standard-tokenizer adapter validates JSON without recursive parser
frames. Durable prepared rows and values retained across primary-cursor advances
continue to use owned materialization. Borrowed trees never enter the payload
cache or escape block lifetime.

Each scan also owns a lazy, byte-budgeted cache of verified typed payloads,
shared across its block read scopes. Payloads own decompressed byte buffers;
scalar payloads discard their decompression buffers after decoding. Neither
backend-borrowed bytes nor schema-dependent cells/JSON enter this cache.
The cache is confined to one snapshot and manifest generation, so no global
lock, cross-query invalidation, or schema-lifetime coupling is needed.
Active blocks pin entries until their cells and visitor callbacks are finished.
Four-way set-associative lookup and 64 total slots bound lookup work and metadata;
LRU replacement skips pinned entries. Admission accounts for the slot table,
payload headers, typed arrays, and owned decompression buffers. The default
budget is 1 MiB per scan (`ScanOptions.columnar_decoded_cache_bytes`, internal
only); zero disables cross-block reuse. Oversized values, pinned pressure, and
cache-metadata allocation failure bypass admission without failing the scan.
This bounds retained cache memory, not the active block's decode workspace,
caller-retained output, or allocator bookkeeping. Memory is released on every
exit, including cancellation, visitor failure, and primary fallback.
`decoded_cache_{hits,misses,admissions,evictions,bypasses,peak_bytes}` report
reuse and pressure; payload-read counters count actual misses, not cache hits.
A process-wide cache is intentionally not enabled: cross-query reuse must
justify a separate global admission/memory policy with concurrency benchmarks.

The bound-scan benchmark (`--test-filter 'relational columnar bound scan benchmark'`)
compares forced primary scans with the hybrid column path on 768 rows with an
8 KiB unselected string, a scalar predicate, and a small nested JSON column.
Seven measured rounds follow a warmup, alternating execution order. Local
ReleaseFast arm64 macOS medians (milliseconds, primary → hybrid):

| Output / selected rows | LMDB | LSM |
| --- | ---: | ---: |
| Full document / 8 | 11.960 → 0.932 | 12.900 → 2.924 |
| Nested projection / 8 | 15.256 → 0.784 | 12.596 → 2.165 |
| Hyphenated field / 8 | 15.112 → 0.686 | 12.610 → 2.200 |
| Full document / 768 | 96.194 → 33.826 | 85.459 → 35.860 |
| Nested projection / 768 | 46.182 → 23.497 | 46.614 → 25.132 |

Selective full output reads eight primary rows; positive projections read none.
Each hybrid scan builds one schema plan across seven blocks. Dense full output
uses sequential primary ranges without reading column payloads. The hybrid path
trades bounded block workspace for less row decoding: selective nested scans
allocate 138/310 kB (LMDB/LSM), versus 27 kB for the streaming primary baseline;
dense full output allocates about 6.9/7.0 MB versus 35.8 MB. These are fixture
measurements, not universal latency guarantees or timing-based test gates.

The dense nested-predicate benchmark exercises 512 rows whose JSON column has
a matching scalar and an unselected 2,049-element array (~4 KiB per row).
Five measured rounds follow a warmup. On the same local ReleaseFast arm64 macOS
setup, compared with `b021b89de` before selection reuse/borrowed materialization:

| Backend | Median milliseconds, before → after | Cumulative allocated bytes, before → after |
| --- | ---: | ---: |
| LMDB | 81.230 → 29.733 | 263,149,212 → 82,092,620 |
| LSM | 78.278 → 31.151 | 275,850,442 → 102,463,108 |

The subsequent lazy-JSON read path indexes only visited containers' raw child
spans, caches requested scalar/subtree values, and shares navigation between
column predicates and projection. Numeric array lookup caches only the requested
position; whole selected JSON columns are emitted directly. Escaped object keys,
exact number lexemes, dotted array fanout, JSON pointer indices, missing/null
values, and ordered projection replacement retain their existing semantics.
Navigation still scans skipped bytes; it avoids their DOM/string allocations,
not the need to locate their boundaries. The same fixture measured 21.440/23.072
ms and approximately 3.1 MB allocated on LMDB/LSM, versus 29.733/31.151 ms and
82.1/102.5 MB immediately before this change. A deterministic allocation test
projects a leaf and the last index of a 131,073-element array using 16 KiB of
scratch, with no index allocation proportional to the array length.

`--test-filter 'relational point projection lease benchmark'` alternates seven
measured rounds after warmup on a 1 MiB row with one selected integer. It measures
64 warmed store probes with the same compiled projection (milliseconds):

| Backend | Copied + full verification | Leased + full verification | Leased + selected-group verification |
| --- | ---: | ---: | ---: |
| LMDB | 123.189 | 118.829 | 7.101 |
| LSM | 7.418 | 5.214 | 5.241 |

LSM already authenticates its values; its last two modes deliberately follow
the same path. The LSM baseline uses the ordinary owning probe, while the other
modes explicitly request a short value lease. A separate integrity-bypassing
**diagnostic only** measured 6.008 ms on LMDB, motivating grouped checks rather than accepting the remaining
full-row verification cost. Public `DB.lookup` request-allocator traffic is 209
bytes on both backends. This excludes backend/cache allocations and must not be
interpreted as total allocation for the operation; timing above isolates
store/projection work, not request/network cost.

### LSM ownership and physical amplification

Column maintenance owns payload reference counts through the existing
single-maintainer guard. Staging reuses its pinned build snapshot, with an owned
build-local digest registry for payloads committed after that snapshot. It does
not create a new mutable-memtable snapshot for each output block. Cleanup probes
its job key before opening a snapshot and shares one snapshot across up to eight
cleanup pages, retaining the per-page atomic commit and apply-lock release and
the 50 ms cooperative quantum.

Reference changes are prepared as aggregated digest deltas. Counts are fetched
in sorted batches of at most 128 distinct keys outside publication; each digest
produces one count update, regardless of how many descriptors share it. New
payload bytes, counts, and owning metadata still commit atomically. Namespace
and build-token validation prevent a prepared builder from publishing into a
different namespace. The single-maintainer guard is required from preparation
through commit: this is not a general multi-writer read/modify/write API.

Point projection explicitly opts into `getLeased`. Immutable-generation values
remain pinned until the probe aborts; SST values reuse the probe's owned buffer
or block-cache handle rather than making a second full-row copy. Uncached wide
values can transfer the decoded block allocation into the lease when allocator
ownership matches and the value occupies at least half the block. Small metadata
reads keep compact value-only buffers. Ordinary probes
still copy and release generation pins promptly. Mutable entries now use
independently reference-counted immutable allocations. Short point leases pin
one entry without copying its value; an overwrite copy-on-writes that entry
when another reader owns it. Unpinned same-sized updates may reuse their bytes.
An uncached SST read may still decompress/materialize the entire physical block.
Leasing is not independently addressable value chunks or zero-I/O projection.

Production primary LSM options use contiguous compaction domains. Each
generation's metadata before `:v:` stays together, its immutable payloads form a
separate domain, and metadata outside the column-generation interval cannot
jump across that interval. This holds even in a metadata-only flush with no
payload keys present. Flush, sorted ingestion, and streaming compaction honor
the same boundaries without changing persisted keys or backup formats.

Compaction selection projects each domain into the existing leveled planner,
preserving newest-first L0 precedence and complete target overlap closure within
that domain. A mapping selects noncontiguous global inputs without including
intervening cold families. Publication relocates exact input IDs and recomputes
the domain closure after an unlocked build. Changed closures reject stale work.
Global L0 pressure still drains small domains, input-byte admission still applies,
and a single nonoverlapping input without tombstones moves levels through the manifest without
rewriting its SST. Existing mixed SSTs use the complete global overlap closure
until reshaped; they are never hidden from a domain-local read or merge.
Payloads remain ordinary LSM values: payload-domain compaction can rewrite them,
and physical retention still depends on live readers and obsolete-file grace.

The original output-partitioning benchmark kept a reader pinned while performing
16 metadata commits beside 1 MiB of incompressible, unchanged payloads. It uses
the real SST/WAL encoders on memory-backed files, not logical value counters:

| SST partitioning | Additional SST bytes written | Retained file bytes |
| --- | ---: | ---: |
| First-byte only | 8,474,322 | 9,534,372 |
| Payload family | 1,066,714 | 2,127,336 |

Those historical measurements cover output splitting, not domain-aware input
selection, and measure write amplification rather than device latency.

### Shared LSM read versions and bounded column batches

Run membership and precomputed L0/lower-level lookup topology are owned by one
reference-counted read version. Point probes and read transactions pin that
version instead of cloning all run descriptors for each request. Every run-set
publication invalidates the backend's cached reference; existing readers retain
their old descriptors and file references. A version owns its metadata rather
than borrowing from an obsolete run list. Cache-index hints are mutex-protected;
shared descriptors never acquire unsynchronized lazy Bloom-filter ownership.
The topology is built lazily once per version under the backend mutex, not once
per key. SST I/O remains outside the writer lock.

Mutable snapshots pin a reference-counted, rank-indexed AVL root in O(1).
The live memtable owns only this ordered representation, not a second hash
index and entry vector. Transaction-local batches keep the hash index. Commit
builds a complete successor root after any WAL-pressure relief (which may
flush and unlock), performs final allocation-based admission, appends the WAL,
then publishes with a non-failing root swap. Allocation failure cannot expose
a partial batch. Writers copy shared paths and mutate private paths. Forward
merge cursors traverse tree edges in amortized O(1) per entry.

Rotation transfers the root without sorting, allocation or reclamation.
Retired snapshot and immutable-generation subtrees are reclaimed outside the
writer mutex, with lifecycle and accounting handles held until completion.
Values remain alive until their last owner releases them. Ordinary reads do
not rotate memtables; configured write, WAL, idle and retention limits apply.
An allocation account tracks nodes and shared payloads once, independently of
the number of epochs referencing them. Memory guards deduplicate accounts and
include spare capacity, rather than summing each snapshot's reachable tree.
The single AVL still costs one node per key and a bounded spare pool, so narrow
rows retain more indexing overhead than a densely packed immutable block.
`mutable_snapshot_clone_bytes_total` now counts copied index/owned bytes rather
than counting shared payload bytes as copies. `read_version_builds` and
`read_version_pins` distinguish topology publication from request pinning.

Domain membership, global level budgets and complete overlap components are
cached once per published run version. `domain_index_builds` exposes rebuilds.
For more than 64 runs, planner sorting and overlap-component construction run
outside the writer mutex against a pinned version; publication discards stale
plans. Capturing a previously unbuilt read version still copies metadata and
builds read topology under the mutex: this is not a fully incremental planner.
Selection retains normalized pressure scores across domains and uses the same
global level targets as maintenance debt accounting. A collection of individually
small domains can no longer strand global lower-level debt.

Manifest v10 records each run's tombstone count, oldest delete timestamp, and
stable L0 visibility identity. The existing main-branch v9
manifest remains readable with unknown counts until runs are rewritten; no
historical relational format is retained. Compaction drops tombstones
only with complete coverage of older persisted data. Below ordinary level thresholds,
maintenance schedules a complete overlapping SST component containing deletes,
including singleton runs. To avoid rewriting mostly live data after every small
delete, standalone GC defaults to at least 50% of the largest input's key count
in tombstones. It does not sum duplicate older versions into that denominator:
fully retired components still qualify, even with many historical copies.
Ordinary compaction elides covered deletes regardless of this threshold.
Sparse deletes also become eligible after one hour by default, so a key-count
threshold cannot indefinitely strand byte-heavy obsolete values. Ages survive
rewrites and reopen; unknown ages and clock rollback qualify immediately.
One in eight compaction maintenance turns gives aged GC first opportunity.

GC has a default 2 GiB input cap, further restricted by the normal compaction
cap, scheduler and IO grants; it never takes the oversized-job exception.
Selection prefers a fitting complete component. Larger components advance
through bounded next-level jobs. A wide source whose target closure cannot
fit is split first, preserving its L0 visibility identity independently of
new file IDs. Each manifest publication is a restart-safe progress checkpoint.
The input budget must accommodate an irreducible source/target window; an
individually oversized source or insufficient maintenance bandwidth can still
defer GC. Inadmissible aged work uses a timed retry instead of spinning on an
already expired age deadline.
This bounds individual work units, not an unconditional physical/live ratio.
Publication revalidates exact input IDs, levels and older-data coverage after
the unlocked streaming build. Empty output atomically retires every input.
Pinned readers retain their old files; actual deletion still waits for reader
release, manifest publication and the configured grace period. The
`tombstone_entries` maintenance counter exposes remaining known delete debt.

Development-host measurements for this redesign:

| Fixture | Before | After |
| --- | ---: | ---: |
| 40,000 single-row writes, 8-byte values, guarded, median | 1.722 s | 0.735 s |
| 8,192 narrow keys / 32 snapshot setups, median | 95.595 ms | 0.003 ms |
| Descriptor bytes copied by those snapshots | 16 MiB | 0 |
| 24 unique-key insert/delete generations after maintenance | Retained delete entries | 0 SSTs |

The write fixture uses ReleaseFast, three samples, memory-backed real WAL/SST
encoding, a 64 MiB byte guard and no intermediate row-count flush. Accounting
alone measured 0.666 s; the preceding shared-root implementation measured
0.691 s. Atomic successor preparation and allocation ownership add about 6%
over that implementation in this narrow-write fixture. The snapshot fixture
uses ReleaseSafe and five alternating samples;
it measures root pin/release and point lookup, not end-to-end request latency.
The churn regression keeps an old reader during GC, reopens the store repeatedly,
and verifies old/current visibility and physical reclamation (539 bytes peak
retained files for this tiny fixture). These are diagnostics, not timing gates
or a universal physical-to-live-byte bound.

The subsequent atomic-publication/allocation-accounting change measured
17,645,096 charged bytes for 17,645,096 allocated bytes in a 4,096-row, 4 KiB,
32-epoch diagnostic (previously 574,603,456 charged for 17,981,488 allocated).
For 100,000 narrow keys, allocated memory fell from 25,064,072 to 17,607,304
bytes. Root handoff performs zero allocations/frees and no tree traversal;
the previous rotation took about 45 ms in that isolated fixture. These measure
allocator-requested bytes, not process RSS or end-to-end request latency.
The final three write samples were 0.745, 0.735 and 0.735 s. Shared-host timing
is diagnostic, not a gate or a general throughput claim.
The production physical-churn fixture retained its previous write reduction:
about 17.28 MB SST output at the default density threshold versus 29.31 MB
with eager standalone GC; WAL output remained 10.09 MB in both cases.

The native production-shaped churn fixture compares eager standalone GC with
the 50% trigger using otherwise identical primary options (ReleaseFast, one
sample per policy, 16 overwrite batches followed by deleting half the rows):

| GC policy | SST bytes written | Peak SST+WAL | Settled SST+WAL |
| --- | ---: | ---: | ---: |
| Eager | 29,307,851 | 8,265,193 | 2,768,258 |
| 50% trigger | 17,283,948 | 8,529,796 | 3,055,160 |

The threshold avoids about 41% of eager-GC SST writes while retaining about
10% more settled bytes in this fixture. WAL writes are identical (10,093,136
bytes). Batch median/max times were 23.115/23.956 ms and 22.826/23.977 ms;
these single-run timings do not establish a latency improvement. Both policies
validate pinned-reader visibility, payload ownership and an integer projection
reading 1,050 payload bytes with zero primary-row reads.

Reproduce the narrow-write measurement with:

```sh
zig build lsm-write-bench -Doptimize=ReleaseFast -- \
  --samples 3 --keys 40000 --batch-size 1 --value-size 8 \
  --storage memory --mode default --workload-set ingest_compact \
  --flush-threshold 1000000 --flush-threshold-bytes 67108864
```

The storage test filters `ordered mutable snapshot setup benchmark`,
`lsm tombstone GC bounds unique key churn`, and
`production LSM physical churn benchmark` reproduce the other fixtures.

Column read scopes expose snapshot-consistent sorted multi-get with scope-local
result ownership. Predicate metadata is gathered in bounded batches; payload
requests retain predicate-stage and survivor-only projection selection. Physical
keys are sorted and deduplicated, cached decoded payloads are skipped, and a
batch is capped at 32 pages / 256 KiB of encoded payload (one indivisible
oversized value is allowed). Cancellation is checked between batches. No new
thread primitives, speculative all-column reads, or unbounded prefetch queues
are introduced.

New differential fixtures use diagnostic-only controls, not alternative
production formats or legacy compatibility modes. A ReleaseFast development
run before merging main's accelerated checksum implementation measured:

| Fixture | Baseline | Shared/domain/batched |
| --- | ---: | ---: |
| Two-sided metadata churn, additional SST bytes | 16,957,275 | 20,625 |
| 256 SSTs / 64 point reads, median milliseconds | 3.390 | 1.151 |
| Topology builds for those 64 point reads | 64 | 0 after warmup |
| 32-column projection, SST block loads | 78 | 52 |
| 32-column projection, SST block bytes | 461,515 | 259,199 |
| 32-column projection, median milliseconds | 6.204 | 4.376 |
| 1,024 × 4 KiB staging, copied snapshot bytes | 42,308,576 | 2,485,568 |

The two-sided fixture uses actual SST/WAL encoders on memory-backed files and
keeps an old reader pinned. The projection fixture uses native files with local
and shared decoded-block caches disabled; OS page-cache state is unspecified.
The staging fixture uses the production 32 MiB threshold and compares deep
versus shared snapshots with the same build-snapshot reuse in both modes. Its
100.337/95.703 ms sample shows that the large byte reduction is not an equivalent
end-to-end throughput multiplier. Timings are diagnostics, not regression gates.

The native-file churn fixture uses production primary options (only the
obsolete-file grace period is set to zero for deterministic reclamation), pins
an old reader, performs 16 batches of overwrites, releases the reader, deletes
half the rows, and validates column ownership and projected reads. It reports
actual active/obsolete SST file sizes plus retained WAL, cumulative SST/WAL
writes, and foreground batch median/max latency. It checkpoints at measurement
boundaries and is not a concurrent-load or device-cold benchmark. Comparing the
previous commit's output-only partitioning with domain selection measured:

| Native churn | Output-only | Domain selection |
| --- | ---: | ---: |
| Cumulative SST bytes written | 20,745,889 | 16,248,094 |
| Cumulative WAL bytes written | 10,093,136 | 10,093,136 |
| Peak SST+WAL bytes, reader pinned | 6,707,877 | 8,868,177 |
| Settled SST+WAL bytes | 3,095,189 | 3,097,601 |
| Foreground batch median / max, ms | 32.555 / 34.283 | 32.904 / 36.064 |

Both read 1,050 column payload bytes for the final integer projection with zero
primary-row reads. Domain isolation reduced total SST writes by about 22%, but
did not improve foreground write latency in this sample and increased pinned
peak disk usage by about 32%; settled usage was nearly unchanged. Timings were
collected on a development host with other compilation work and are not
isolated-machine latency claims. Independent domains change compaction geometry;
less rewriting is not a universal peak-footprint bound. Long readers still need
retention limits and operational disk headroom. No fixed physical-to-live-byte
ratio is inferred from the logical churn tests.

Reproduce with `zig build lib-storage-test -Doptimize=ReleaseFast --` and filters
`'relational columnar production LSM'`, `'lsm payload family isolation'`, and
`'lsm point leases'`, plus `'lsm shared read version'` and
`'lsm compaction domains'`. Timing is diagnostic; regression gates check ownership,
snapshot-copy work, and physical write reduction rather than wall-clock limits.

All 512 primary owners are still read and checked; the already-evaluated
predicate is reused, not run twice. Allocation totals are allocator traffic,
not peak RSS. Reproduce with `--test-filter 'relational columnar dense nested
predicate benchmark'`. The deterministic historical-plan churn benchmark
(`--test-filter 'column scan cursor historical churn'`) warms 32 resident epochs
then visits 32 consecutive rows from a rejected epoch: execution ownership cuts
plan builds from 32 to 1 and allocated bytes from 58,464 to 1,827 in its small
two-column fixture. Regression gates assert work/ownership bounds, not timings.

Reproducible focused benchmarks (Zig 0.16, ReleaseFast, arm64 macOS):

```sh
zig build lib-storage-test -Doptimize=ReleaseFast -- \
  --test-filter 'relational columnar wide metadata allocation benchmark' \
  --test-filter 'relational columnar decoded reuse benchmark'
```

The payload fixture scans 128 rows containing 64 KiB strings, with either one
shared value or 128 distinct values. Both variants use a nonmatching typed term
predicate, no document materialization, and a 512 KiB cache budget. Nine measured
rounds follow a warmup, alternating cache-off/on order. One local run measured:

| Backend / values | Median ms, off → on | Payload decodes, off → on |
| --- | ---: | ---: |
| LMDB / shared | 1.780 → 0.431 | 8 → 1 |
| LSM / shared | 3.136 → 1.604 | 8 → 1 |
| LMDB / distinct | 23.936 → 23.842 | 128 → 128 |
| LSM / distinct | 50.211 → 50.145 | 128 → 128 |

Peak retained cache bytes were 68,756 for shared values and 462,860 for distinct
values. The one-row, 1,024-column existence-filter fixture allocated 3,337,310
bytes/scan after lazy read state, versus 20,308,708 with the same fixture before
the change. Timings are workload-specific and sensitive to machine load; CI
asserts exact results, allocation budgets, reuse counts, and ownership cleanup,
not latency thresholds. These are not disk-cold or concurrent-query benchmarks.

Scan planning is metadata-first. Access-path admission happens before predicate
payload decoding, and dirty markers remove replaced/deleted base candidates
before evaluation. Bounded marker probes feed admission; execution completes
visibility with ordered seeks only for its current row window, skipping large
insertion gaps without walking every marker. A limited filtered scan evaluates
the earlier dirty-row prefix before reading any base predicate pages, then
one physical predicate-page window as needed. It stops as soon as its limit is
met. Dirty cursors preserve tombstone metadata and never point-read deleted
primary rows; `overlay_tombstones_skipped` measures this work. Admission charges
point-lookup overhead only for live deltas, plus measured journal bytes for all
markers, so delete waves do not force narrow projections into wide primary reads.
Unlimited scans retain block-vectorized evaluation. Cancellation
and deadline checks run between windows, predicate nodes, and payload pages.
Boolean leaves are ordered once per expression/block using numeric pruning,
unread candidate-page bytes, and logical evaluation weights. Wide strings/blobs
are not classified as cheap scalars. Boolean thresholds and negation preserve
their semantics under ordering; repeated predicates share cached pages. These
weights guide predicate ordering only, not the primary-versus-column byte model.

Column segments carry schema epochs, null state, and numeric min/max summaries.
A manifest records the schema-bound generation and directory state.
Each primary put/delete atomically records an eight-byte mutation version and
an eight-byte packed-row size (zero for tombstones),
including raw recovery writes that do not append replay. One durable counter
increment is shared by all row mutations in a store transaction. Counter,
primary and dirty records commit together; aborts cannot expose identities,
overflow is rejected, and identical rewrites still get new committed identities.
No physical-row hashing is performed for dirty marking. Physical checksums and
semantic content hashes retain their independent integrity contracts.
A snapshot reader streams an ordered merge of immutable column rows and dirty
AROW point reads. Inserts and updates come from the pinned snapshot's primary
row; deletes and changed rows mask the corresponding old base row, even when
the new row fails the predicate or expires. Delta evaluation is independent of
base zone-map pruning. One changed row no longer forces primary reads for the
whole range, and arbitrarily large deltas retain only one row read scope/arena.
A bounded cost probe compares unchanged primary bytes plus live delta bytes
against predicate/projection payload bytes plus delta point-read costs. Shared
predicate/projection columns are charged once. Root metadata
records packed bytes per row; selected column metadata records payload sizes.
Replaced base rows are subtracted, while expired primary rows still count toward
I/O. The estimate charges 4 KiB per random read and 64 bytes per sequential row,
requiring a 25% margin and at least 16 deltas before switching to primary scans.
It inspects at most 1,024 dirty records or 256 KiB without fetching primary values;
incomplete estimates omit column payload costs and use the observed point-read cost
as an overlay lower bound, so very large insertion bursts can still choose a
sequential scan without walking every marker first. Small LIMIT queries (up to 16) skip the
probe and preserve early exit. These conservative cost weights are tuning
parameters, not measured device latency claims. Scan statistics expose estimated
costs, probe records, and the choice as `dense_delta_scans`.
Ordinary row-count
updates leave the accelerator live. Schema changes invalidate the generation.
Relational scans, including full-document fallback and maintenance, use a
row-only owner cursor. It seeks directly past each encoded owner's child prefix
instead of stepping through artifact, vector, or TTL records. Escaped binary IDs,
empty IDs, and prefix-related IDs retain their ordering and range semantics.
There is no duplicate primary-row copy or secondary row-directory index to keep
in sync. The backend-neutral cursor is the boundary for any future physical
row-keyspace change. An artifact-only owner may require reading its first record,
but child fanout does not multiply cursor steps. Owner checkpoints enforce query
cancellation/deadlines and let builds yield after 1,024 owners or their time
budget even without a live row. Owner-visit counters expose that work separately
from rows read or written. Covered-range maintenance merges typed column pages
with the same snapshot's ordered dirty journal. Only live deltas fetch primary
AROW; replaced/deleted base rows are masked before column payload loading.
Unchanged rows never fetch primary AROW or reconstruct JSON. Tombstones consume
bounded owner checkpoints but require no primary lookup. A bounded remapping window preserves
semantic hashes, timestamps, physical-size accounting, absent/null cells, and
schema identity. The builder accumulates one source selection/remapping plan
per destination block and epoch, not one per clean run between dirty rows.
It reuses complete source pages and wide-value slices directly through their
payload identities and row mappings. Small fragmented pages are transposed
once per output block/epoch; inline cells are sorted once before encoding.
This bounds bookkeeping under alternating updated/unchanged rows while avoiding
tiny scalar fragments. Wide slices are reused only when the current mapped
page averages at least 128 encoded bytes per row. Full physical pages can always
be reused; a complete *mapping* of a partial physical page is still a slice.
Partial slices are admitted only when they retain at least half of the original
payload's uncompressed cell bytes, including doc IDs, and at most eight partial
slices are retained per destination column/block. Other slices are repacked into
new byte-bounded pages. This bounds decoded-byte amplification after churn to
2x live cell bytes per reused slice (excluding fixed codec headers), without
penalizing a surviving large value merely because most small rows were deleted.
Partial source pages are decoded once for byte accounting; oversized singleton
pages keep the metadata-only path. Descriptors remain bounded by the 256-row limit.
Decoded source pages outlive destination flushes; copied cells are owned by
the writer and reused descriptors own no borrowed source memory.
Empty output ranges publish explicit empty roots; publication never widens a
neighbor's live bounds over physically retained but retired rows.
Publication, dirty-token compare-clear, retained
suffixes, cancellation, and restart use the same existing commit fences.
Maintenance exposes `covered_rows_read` and `primary_rows_read` separately so
the reduction in primary I/O is measurable for both dirty compaction and clean
coalescing. `cell_slots_examined` measures transposition windows;
`payloads_reused` and `payload_bytes_written` measure committed payload sharing
and actual new payload bytes. Typed identities are probed before compression,
including for an unchanged oversized field in a full-row scalar update.
Existing payloads skip both compression and payload writes; unchanged covered
pages also bypass payload reads and cell hashing. `payload_encoding_bytes`
reports the raw cell budget actually passed to the encoder, including staged
work that is subsequently canceled. `payload_slices_repacked` counts attempted
slice rewrites triggered by utilization or fragment limits. Repeated deletion
waves are tested against both retained payload bytes and scan bytes across
restart, including reclamation while an older scan remains pinned.
Thus neither an empty prefix nor an orphan-heavy gap after a live row can
repeatedly consume the budget before the worker reaches its successor. Uncovered
bootstrap ranges continue to use the bounded owner cursor. Dirty journal and
typed-source readers retain one snapshot; publication compare-clears only the
captured dirty images, so writes racing preparation remain visible as overlays.

Bulk-capable backends append dirty tokens in the same ingest arena as primary
rows, preserving direct ingestion without per-row sorted-map insertion.

Maintenance compacts a dirty range and up to seven eligible neighbors, staging
at most 64 replacement blocks per pass and yielding after 8 MiB of source rows
or 50 ms, checked between rows after the first block (an individual row/block can exceed the byte or
time target). Dirty-image capture is independently capped at 1,024 records or 256 KiB;
large delete bursts therefore yield even when there are no live output rows.
Large insertion bursts split into bounded passes while the
uncovered suffix retains its old block and dirty markers. Publication replaces
the affected directory entries and enqueues old blocks for retirement atomically.
The publication transaction performs no per-column retirement work. The durable
retirement queue is drained before another compaction can publish, bounding the
backlog to the at-most-eight roots removed by one publication. Each GC quantum
deletes column metadata and releases its payload references transactionally;
the last page also deletes the intent. This survives interruption/restart and
backpressures derived compaction, not foreground mutations. Partial suffix roots
remain live and are never retired prematurely. Compare-and-
clear removes only dirty images represented by the published snapshot: a
racing write keeps its marker. Checksummed cleanup pages are staged before
publication; the directory and cleanup job become visible in one transaction.
The common one-page case compare-clears within publication itself, avoiding
both a journal write and a separate cleanup commit.
Each cleanup transaction compare-clears at most 256 images (or a 256 KiB page)
and deletes that page atomically. Restart resumes at the first remaining page
without rebuilding published rows or retaining their source snapshot. Initial
generation creation uses the same bounded builder. A checksummed, generation-bound
bootstrap cursor advances atomically with each published prefix and cleanup job.
The manifest explicitly marks initialization in progress: a missing checkpoint
is corruption and triggers primary fallback/rebuild, never false full coverage.
Every quantum takes a fresh snapshot; unfinished suffixes use primary scans even
when their rows have no dirty records. A restart resumes the unpublished suffix,
and racing changes in published prefixes retain their dirty markers. No table-wide
snapshot or all-at-once final publication is required. A durable build token fences concurrent builders
and makes canceled/crashed staging reclaimable on restart. Whole-store namespace
replacement is fenced separately. Retired blocks remain available to pinned
MVCC readers. Initial creation, schema replacement, and corrupt-derived-data repair
all use incremental coverage construction. This keeps one row authority,
makes schema changes and crash recovery explicit, and prevents a user-visible
index configuration from controlling SQL/relational scan performance.

A generation-bound durable round-robin cursor advances before staging, so hot
keys, failed builds and process restarts cannot starve later dirty ranges. The
owner worker drains up to eight ranges or 50 ms per turn (checked between
bounded range builds), using a 100 ms active cadence while work remains and a
five-second idle/resource-pressure backoff. DB statistics expose pending work,
its observed age, backoff state, pass duration, failures, compacted ranges and
written blocks without a catalog scan.

The background worker admits dirty compaction when at least one quarter of the
base rows changed, delta bytes reach one eighth of source bytes, accumulated read
cost reaches the source size (at least 64 KiB), or the oldest observed deferral
reaches ten seconds. First-observation timestamps are durable and do not reset on
hot rewrites or restart; backwards wall-clock jumps admit work. Read pressure is
a bounded 256-slot atomic hint table: collisions may compact a range early, never
change visibility. Deferred ranges have checksummed state and a durable,
generation-bound due-time index. Discovery examines up to 128 candidates or
50 ms per batch and commits all new deferrals and cursor progress together.
Deferral state caches immutable block row counts and source bytes. Subsequent
wakes inspect bounded dirty-marker prefixes and use their maximum committed
mutation token as a range-local revision: a write elsewhere does not reread
the block root or rewrite unchanged deferrals. This avoids a new range-index
lookup/update in foreground transactions and preserves bulk append ingestion.
It still performs small dirty-marker probes on table-wide wakes; statistics
separate `admission_root_reads` and `admission_dirty_probes`. Restart reuses the
durable admission facts. Empty completed coverage re-enters bootstrap when a
new row arrives, rather than attempting admission against a missing directory.
Due timers are considered first; ready work discovered behind deferred ranges
can run in the same worker turn. A merge turn cannot lose a selected ready range,
and merge/cleanup work prevents entering a deferred-only wait. Publication retires
the old block's timer atomically and repoints retained suffix timers after a
partial build; stale timer entries are reclaimed in bounded batches. Timer and
merge-queue keys use separate namespaces. Restart retains the original due times.

When only future timers remain, unchanged idle checks perform no storage reads
or scheduling commits. A process-local revision advances only after successful
row/schema commits; together with read-pressure and namespace revisions, it wakes
discovery on the next active poll (100 ms maximum polling interval, excluding
resource pressure). Polling remains cooperative `std.Io` sleep, and timer expiry
or a backwards clock correction also resumes discovery. The durable mutation
counter and timer index, not process-local hints, govern restart behavior.
Explicit maintenance drains bypass
admission, but retain all quantum limits. Statistics expose deferred ranges,
bootstrap quanta, candidate checks, scheduling commits, next due time and actual
staged key/value bytes written. Age is an admission
deadline, not a completion SLA under resource pressure or a large backlog.

Underfilled blocks (fewer than 128 rows and less than 512 KiB of source rows)
retain durable occupancy markers. A separate merge queue receives every fourth
compaction scheduling turn even while dirty work remains, and all otherwise
idle turns. Its durable round-robin cursor prevents a hot first candidate from
starving later candidates. Only selected clean neighbors must be clean, not the
whole table; reaching idle does not discard occupancy information.
Later delete waves can merge with earlier underfilled neighbors. Coalescing
uses compatible schema epochs, the same bounded builder and atomic directory
publication as dirty compaction. GC targets at most 256 operations or 256 KiB of
key/value metadata per quantum, counting payload-reference release operations.
A single store record can exceed the byte target. A column's metadata and
references are also indivisible: that one record can exceed the operation
target, but contains at most 256 references (513 key operations plus intent
completion). Revoked staging tokens remain in a durable garbage job until
their entire prefix is reclaimed. The deletion itself is the resumable cursor.

Artifact metadata repair and column maintenance have independent failure and
backoff handling behind the shared portable-runtime activation gate. An artifact
decode failure cannot starve column maintenance or create a hot retry loop.
Maintenance statistics additionally expose rows written, ranges merged, dirty
markers cleared and GC records deleted; rows/blocks written measure output
occupancy and can be compared with mutation volume to assess write amplification.
Durable transaction preparation includes packed AROW bytes, not just reserved
JSON sidecars, when sizing its bounded `std.Io` task group.

### Query path

The relational base-row path compiles exact scalar and nested-JSON predicates
to stable ordinals. A scan authenticates and parses the AROW directory once,
then evaluates those predicates without reconstructing the document; unsupported
predicate shapes fall back to logical JSON for correctness. Projection uses the
same compiled ordinal layout. TTL reads the physical write timestamp directly
from the authenticated row header, and vector rebuilds decode only their target
ordinal. Joins and `GROUP BY`-over-join are unchanged — they already exist (see
`JOINS.md`, `ALGEBRAIC.md`).

Positive nested projections compile their root ordinals and path segments once
per epoch. Only referenced columns are materialized, then the existing document
projection operators run on that partial typed root. Unselected vectors and JSON
columns are not expanded; exact top-level selections retain the direct encoder.
Exclusions, wildcard selections, and special fields keep their general fallback.

Dense-vector membership and indexed element predicates run directly on binary
f32 payloads without constructing a JSON array. Other composite predicates use
the same logical-cell conversion as projection, cached once per referenced
column for the row's evaluation.

First-party cross-node scans use one response-streamed request per shard. The
remote shard therefore holds one read transaction for the requested range and
the caller applies byte-level backpressure while decoding rows. Custom request
executors that do not implement streaming retain the bounded row-paged fallback.

Table-owned `typed_doc_values` remain a complementary accelerator for broad
range scans and aggregations. They are not required for direct AROW projection
or filtering and never become a second row authority.

Covered scans evaluate bound predicates column-at-a-time, prune disjoint
numeric ranges using block summaries, and fetch projection columns only for
blocks with surviving rows. A sparse row-key directory lets resumed and bounded
scans seek directly to their starting block. Positive flat/nested projections and
hash-only scans avoid primary-row reads for unchanged rows. Hyphens inside field
names are literal; only a leading hyphen denotes exclusion. Nested predicates
traverse only their bound root column; dense-vector element predicates read a
single binary float without expanding the vector. Full-document, wildcard,
exclusion, and special-field output uses late AROW materialization after selection,
with special-field loaders sharing the same read transaction. A block with at
least a block's worth of remaining output budget costs actual surviving row bytes
and random-read overhead against sequential primary access; dense output takes
the latter. Evaluated selection masks survive that switch: an ordered cursor
reuses positive and negative decisions only for unchanged owners with matching
key, schema version, semantic hash, and timestamp. Dirty/unknown owners still
evaluate their authoritative rows. Small limits retain page-window evaluation.

`column_scan_plan.zig` binds filter roots, nested traversal, and projection
ordinals once per resident schema epoch. Clean blocks, dirty overlays, and
primary ranges share the request-local plan cache. Up to 32 resident plans use
frequency-based admission; active blocks pin their immutable views and cannot
be evicted by a different dirty-row epoch. Each of the column, dirty-overlay,
and sequential-primary streams independently pins its active plan, including
plans rejected by cache admission. A covered primary range also borrows its
block's plan across interleaved dirty epochs. Thus cache pressure cannot turn
a consecutive historical run into per-row schema compilation. Residency stays
bounded at 32 cache entries plus at most three active execution pins; acquiring
a successor may temporarily retain one extra plan. No global locks or
cross-request cache are introduced. Compiled predicates
are borrowed from the request instead of copied into every epoch plan.
`scan_plans_built`, `scan_plan_hits`, and `late_materialized_{rows,bytes}` expose
binding reuse and deferred primary I/O. `selection_reused_rows` and
`primary_predicate_rows` distinguish reused decisions from authoritative
predicate evaluations, including overlay work.

Checked directory boundary links detect missing range entries; block
and manifest checksums protect derived bytes. Corruption resumes
the same snapshot's primary scan after the last delivered key and requests a
rebuild. Request-local `ColumnarScanStats` exposes blocks read/pruned, columns and
encoded bytes read, selected column-metadata reads, overlay point reads,
selected rows, dirty ranges read, and logical values
materialized. Typed scalar kernels and direct vector membership do not build
JSON trees. Candidate masks short-circuit resolved blocks/rows, inexpensive
predicates precede composite payloads, and projected values are materialized
only for surviving rows. JSON composite values are cached per block/row.

The scan retains one immutable storage snapshot but opens a read scope per
columnar block. LSM scopes own and release their own cached-block pins/copied
values; they never accumulate in the parent transaction. LMDB borrows from its
snapshot, and other backends use bounded cursor-owned copies. Dirty overlays
check query/shard bounds before opening a row read scope. Primary
fallback intersects directory, query and shard bounds before opening its
cursor, and checks keys before schema lookup or row decoding. Scan counters
separate metadata bytes, payload bytes and primary rows decoded.

Durable intent application keeps AROW as the logical authority. The immutable
write plan requests only the graph/sparse/TTL/text fields actually consumed;
direct vectors use binary ordinals. Selected-field text indexes retain only
their required roots. JSON-only consumers opt into a request-owned lazy source:
field-based enrichment inputs project their own ordinal, while root templates,
document-extraction metadata, and whole-document text consumers explicitly
request broader materialization. Renderings are cached, and merely persisting a
typed row never forces JSON reconstruction. Dedicated columnar aggregate/sort
operators remain separate query-engine work, not part of scan publication.

Relational JSON, special-field, and physical validation failures are normalized
at the input boundary. Raft records `InvalidBatchRequest` and the durable
`TransactionTooLarge` policy rejection as terminal command outcomes, then
advances to later entries. Corruption and local resource pressure remain
execution failures rather than being silently converted into rejected writes.

API-bound and Raft portable restores select the unpublished, one-pass importer.
The request's semantic cancellation token reaches the block loop, and bound
restore plans can report transport-neutral block/row/byte progress. Cancellation
discards staging rather than publishing partial state. Complete-image identity
and public-contract checks still run before publication.

### Schema evolution

Schema changes durably write an immutable runtime layout and public validator,
then atomically switch the catalog's active version. Requests keep their pinned
epoch alive through reference counting; point reads use an RCU acquisition fast
path, and historical versions load lazily when old rows are encountered.

Historical caches use an aging frequency sketch for admission. An interleaved
scan spanning more than 32 epochs must not flush every resident query plan or
layout on each row. Scan, search, index-backfill, and shared-registry caches
apply that policy; a request-owned transient entry serves non-admitted epochs.
Repeated hits reuse that transient plan without faulting or compiling it again.
Sixteen `std.Io` fault lanes coalesce same-version misses while unrelated
versions can load concurrently. Whole-store replacement acquires all lanes
before replacing the registry namespace.

Portable export and restore share a 16 MiB decoded historical-epoch budget
(configurable through `ImportOptions.schema_cache_bytes` for restore). Export
faults serialized layouts from its immutable snapshot. Staged restore faults
from its unpublished metadata store; validation-only paths spill serialized
history to a private host file. A compact version/offset/digest directory remains
in memory. Archives are not rejected based on total schema-history size.
Arena capacity accounts for decoded layouts and validators; one transient or
oversized epoch and the active schema are additional working memory. The compact
directory remains O(schema versions), and validation-only freestanding calls
without a filesystem retain the serialized spool in memory. Canonical row and cross-schema
validation still run before staging publication. Export uses compiled physical
offsets, keeping dense-row column traversal linear in schema width.

The export data cursor seeks past the column-cache, already-exported metadata,
and replay namespaces. It examines at most the first cursor entry of each
excluded namespace, rather than walking every materialized payload. Other binary
and legacy graph key ranges retain their existing export semantics. Both the
file-spooled single-pass path and deterministic no-spool two-pass path use this
cursor. Optional `ExportStats` expose snapshot passes, examined data-cursor
entries, and namespace seeks so export work can be tested independently of
derived-cache size.

`schema_capability.classifyChange` already distinguishes additive changes
(new algebraic field → no rebuild) from breaking algebraic changes (removed or
type-changed field → rebuild). A relational-specific lifecycle classifier is a
later integration seam: it must treat nullable → `NOT NULL` as breaking and
only classify widening (for example integer → number) as additive when the
physical representation and backfill plan are compatible.

## Phased plan

- **Phase 1 — contract + catalog (complete).** `storage_mode` parsing,
  raw-schema `json` columns, and `relationalColumnPlanAlloc` produce the static
  typed-column catalog.
- **Phase 2A — authoritative packed base rows (complete).** Writes,
  transactions, recovery, replay, scans, indexing, TTL, split handoff, and
  portable backup operate on dedicated packed-row keys while returning logical
  JSON at API boundaries.
- **Phase 2B — immutable epochs, prepared rows, and staged restore (complete).**
  Versioned layouts/validators, ordinal AROW v2, transactional catalog/outbox,
  bounded concurrent preparation, and manifest-first staged restore.
- **Phase 3 — ordinal execution (complete).** Compiled predicate/projection
  plans, physical-header TTL, and direct-ordinal vector extraction avoid full
  document reconstruction on the relational hot paths.
- **Phase 4 — table-owned typed-column persistence (implemented).** Bounded,
  staged generations, null bitmaps, checksums, coverage fences, and reclamation.
- **Phase 5 — columnar scan and predicate pushdown (implemented).**
  Epoch-bound scalar/nested evaluation, numeric zone maps, and late projection.
- **Phase 6 — unified reads (implemented).**
  Column selection supports nested output and costed late AROW materialization;
  uncovered generations use authoritative AROW. Specialized aggregate/sort
  execution can extend this without introducing another row authority.

## LSM metadata epochs and bounded scan sources

SST metadata now has a persistent, reference-counted run directory, separate
from mutable cache hints. Recovery constructs its initial root before exposing
the backend; an empty store seeds it on its first flush. Normal flush,
compaction, and trivial-level-move publications copy only changed tree paths
and payloads. Capturing the immutable root is O(1), without cloning each run or
taking a file-registry reference for each reader.

Readers share a flat search projection for each published epoch. Projection
allocation and L0 topology construction happen outside the writer mutex, with
working-memory admission and generation revalidation before installation.
Concurrent first readers share one build; the build gate waits through
`std.Io.Mutex` when a read runtime is available, with the platform mutex helper
for manual/no-runtime backends. Writers never acquire that gate. Old directory
roots and read projections remain accounted and keep their files pinned until
their last reader exits; reclamation runs outside the backend mutex.

Merge cursors retain one source per overlapping L0 file and one concatenating
source per nonoverlapping lower level, plus the mutable/immutable memtables.
They binary-search file bounds and open SST indexes/blocks lazily. Forward and
reverse scans honor namespace and exclusive upper bounds without opening every
file in a level. Cursor memory is O(memtable generations + L0 files + levels),
not O(all SSTs). The shared projection is still O(all SSTs), and existing flat
run-vector publication and full administrative rewrites are not O(1).

GC eligibility is a durable objective: an eligible overlap component marks all
of its delete-bearing inputs in the manifest. Bounded windows and subsequent
compactions propagate that request until the surviving deletes are collected.
A denied job retries on its admission deadline independently of the age
trigger, including when age-based GC is disabled. This prevents a component
from losing eligibility as partial progress lowers its density. The new PR's
manifest-v10 format includes this flag; intermediate PR-only v10 layouts are
not a compatibility contract.

The checked-in ReleaseFast scaling fixture measured the following on an
Apple Silicon development host (three samples; not an end-to-end latency SLA):

| Lower-level SSTs | Shared directory bytes | One-run update, median | Off-lock projection, median | Per-cursor bookkeeping |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 628,888 | 2 µs | 46 µs | 512 bytes |
| 10,000 | 6,263,752 | 1 µs | 404 µs | 512 bytes |
| 100,000 | 62,604,760 | 3 µs | 4.97 ms | 512 bytes |

The previous cursor layout required 23,200,232 bytes at 100,000 SSTs. The new
directory adds shared metadata memory (about 626 bytes/SST in this fixture),
charged to the resource manager, in exchange for cheap epoch pins and avoiding
per-reader metadata clones. The physical churn benchmark remained at roughly
23.6–23.7 ms median per batch; SST bytes written were 29.3 MB with a zero-density
GC threshold and 17.3 MB with the 50% threshold. This validates retention of the
existing churn behavior, not a new SST write-amplification improvement from the
directory itself. Both benchmarks are reproducible via `lib-storage-test` with
`-Doptimize=ReleaseFast` and filters `persistent directory and lazy cursor scaling
benchmark` and `production LSM physical churn benchmark`.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution
