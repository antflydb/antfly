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
the exclusive apply fence. Publication checks the pinned epoch and retries a
bounded number of times if it changed; intent ordering and predicates retain
their transaction semantics.

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

**Columnar-index integration seam:** `TypedDocValuesWriter` is a segment-level
accumulator (all rows in a segment → one `build()`), not a per-document store.
The accelerator must therefore be table-owned hidden derived state, rather than
being duplicated into whichever user text index happens to exist. Prepared rows
feed per-column writers directly from their typed ordinal cells, alongside a
null bitmap; no JSON projection or reparsing belongs in this path. The typed
value stream identifies present values, the null bitmap identifies explicit
nulls, and membership in neither means the column was absent.

Column segments are bound to the table generation, schema layout version, and a
covered primary-store sequence. Insert/update/delete publication uses the same
durable replay boundary as the other derived families. A replacement generation
is built in the background and becomes readable only after its coverage fence
is durable; until then, reads fall back to AROW. This keeps one row authority,
makes schema changes and crash recovery explicit, and prevents a user-visible
index configuration from controlling SQL/relational scan performance.

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
- **Phase 4 — table-owned typed-column persistence (optional accelerator).**
  Feed per-column `TypedDocValuesWriter` accumulation from `PreparedRow` cells,
  persist null bitmaps and coverage metadata, and publish through a durable
  generation state machine (see "Columnar-index integration seam" above).
- **Phase 5 — segment columnar scan + predicate pushdown.** Table-scan operator over
  `typed_doc_values`; route typed-column predicates to it; columnar projection
  on read.
- **Phase 6 — unified columnar reads.** Serve projections from segment columns
  when available and fall back to the authoritative packed base row. There is
  no retained whole-document JSON blob for relational rows today.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution
