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

## The pivotal decision: authoritative packed rows

The base store uses one self-describing packed row per document. Declared
scalars are stored in their physical typed representation; nullable absence and
explicit null remain distinct; `json` columns retain their exact JSON subtree
bytes. The legacy zstd whole-document blob is not double-written.

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

The parsed public schema is cached once per schema generation inside the DB.
Every storage entry point validates its final post-transform rows against that
same cached contract, including embedded batches, transaction prepares,
replicated callers, and recovery resolution. API validation remains an early
feedback optimization rather than the only integrity boundary.

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

The authoritative base value is one self-describing `AROW` payload per
document. It stores each present column's path, physical type, null state, and
typed value. Point reads reconstruct canonical JSON without a schema lookup;
metadata and derived-artifact keyspaces are never passed through the row
decoder. Portable backup format v2 retains the packed payload and row key kind
exactly, including timestamps and relational schema metadata.

Portable restore performs a bounded-memory two-pass preflight. The first pass
binds the archive header, runtime schema, public schema, and physical row mode;
the second validates each packed row against those authoritative schemas.
Mixed document/relational keyspaces, v1 relational flags, schema-mode
mismatches, and schema-invalid rows fail before the destination is mutated.
After import, an already-open DB reloads the durable schema and refreshes every
schema-dependent runtime context before reads, writes, or index rebuilds resume.

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

**Remaining columnar-index integration seam:** `TypedDocValuesWriter` is a segment-level
accumulator (all docs in a segment → one `build()`), not a per-document store,
so populating columns belongs in the segment builder that owns the write batch,
not in the per-document `writeDocFacts`. The seam is: add
`relational_columns: []const FieldConfig` to the index `Config`
(`storage/db/algebraic/index.zig`), and at segment build collect, per column, a
`TypedDocValuesWriter` fed by `projectRelationalRowJsonAlloc` for each document in
the batch, plus a per-column null bitmap for explicit null document IDs, then
persist each built section. The typed value stream identifies present values,
the null bitmap identifies explicit nulls, and membership in neither means the
column was absent. Segment reconstruction must pass that null state through
`relational_row_codec.appendCellValue`. In Phase B this is also where the JSON
blob write is dropped for non-`json` columns.

### Query path

The relational win is predicate pushdown. The planner gains a **columnar table
scan** operator that reads `typed_doc_values` chunks directly (`readU64Chunk`,
range bounds, term equality) for predicates on typed columns, instead of
routing every filter through the full-text index. Projection is served by
`ColumnarReader.readDoc(projection)`. Joins and `GROUP BY`-over-join are
unchanged — they already exist (see `JOINS.md`, `ALGEBRAIC.md`).

### Schema evolution

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
- **Phase 2B — segment typed-column persistence (pending).** Wire per-column
  `TypedDocValuesWriter` accumulation into the segment builder and persist the
  sections (see "Remaining columnar-index integration seam" above).
- **Phase 3 — columnar scan + predicate pushdown.** Table-scan operator over
  `typed_doc_values`; route typed-column predicates to it; columnar projection
  on read.
- **Phase 4 — unified columnar reads.** Serve projections from segment columns
  when available and fall back to the authoritative packed base row. There is
  no retained whole-document JSON blob for relational rows today.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution
