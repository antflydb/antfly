# Relational Mode

Antfly tables are document-first by default: a document is a single
zstd-compressed JSON blob, and every index (`full_text`, `embeddings`,
`graph`, `algebraic`) is *derived* from that blob. Schema is optional and
soft.

**Relational mode** is a second table profile on the same engine. It keeps
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

Relational mode is **not** a separate store. It is a `storage_mode` on
`TableSchema`. A document-mode table behaves exactly as before.

## Why this fits

The substrate already exists:

- **Typed scalars** — `storage/db/algebraic/value.zig` (`Kind`:
  string/integer/number/boolean/datetime/bytes, canonical encodings).
- **Typed column store** — `section/typed_doc_values.zig`
  (`u64`/`f64`/`bytes`/`bool`/`geo_point`, chunked, SIMD bulk reads, range
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

## The pivotal decision: authoritative columns

There are two storage layouts, and relational mode is designed so the first is
a strict subset of the second:

- **Phase A — guaranteed-complete secondary columns.** The zstd JSON blob stays
  the source of truth, but relational mode *guarantees* every declared scalar
  column is populated into typed columns at write time. Reads still reconstruct
  documents from the blob; predicate pushdown and aggregation are served from
  columns. Low risk, reuses everything, double-writes.
- **Phase B — authoritative columns.** Typed columns become the store. Non-JSON
  columns no longer keep a blob; documents are *reconstructed* from columns on
  read via `ColumnarReader.readDoc(projection)`. Only `json` columns keep a
  byte payload. Smaller storage, true columnar scans, no double-write.

Ship Phase A first. Phase B is an internal storage swap behind the same
contract and query surface.

## Public contract

`storage_mode` is added to `TableSchema` (`specs/openapi/antfly/schema.yaml`):

- `document` (default) — current behaviour, unchanged.
- `relational` — required closed schema, typed columns, columnar predicate
  pushdown.

In `relational` mode the following are implied/enforced:

- `enforce_types = true` (documents must match a declared type).
- Each document type is treated as closed (`additionalProperties: false`)
  unless a field is explicitly typed `json`.
- `required_fields` declares `NOT NULL` columns.
- `dynamic_templates` apply only inside `json` columns.

`json` is added to `AntflyType`. A `json` column is stored as a `bytes` column
and indexed like a document subtree (path facts + dynamic templates). It is the
escape hatch for semi-structured data inside an otherwise typed row.

Constraints in scope for v1: primary key is the existing document key;
`NOT NULL` via `required_fields`. **Out of scope for v1:** cross-document unique
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
  (`bytes_val` / `u64_val` / `f64_val` / `bool_val` / `geo_point`)
- `nullable` — `false` when the field is in the type's `required_fields`
- `indexed` — whether to maintain an inverted/typed index for the column
- `is_json` — nested objects, arrays, and `json`-typed fields collapse to a
  single `json` column at their path instead of recursing

This reuses the existing `schema_capability` traversal. Unlike the algebraic
`Plan` (which emits group/measure/time *fact* roles and may emit a field under
multiple roles), the relational plan emits exactly one physical column per
property — it is the column catalog.

First-cut physical mapping:

Physical mapping (chosen to match the engine's existing doc values, so the
columns are read by the existing `search/query.zig` predicate readers):

| `column_type` | `physical`  | notes                                      |
| ------------- | ----------- | ------------------------------------------ |
| string        | `bytes_val` | keyword / link / text-as-keyword           |
| integer       | `f64_val`   | numeric range path (`getF64`), like number |
| number        | `f64_val`   |                                            |
| boolean       | `bool_val`  |                                            |
| datetime      | `u64_val`   | raw epoch ns, like timestamp doc values    |
| geopoint      | `geo_point` | packed lat/lon                             |
| geoshape      | `bytes_val` | encoded shape                              |
| json          | `bytes_val` | indexed as a document subtree              |

### Write path

`schema_capability.projectRelationalRowAlloc` turns a document into one typed
cell per declared column (`RelationalRow` / `RelationalCell` / `ColumnValue`),
ready to hand to `section/typed_doc_values.zig` at segment-build time:

- a missing or null value on a non-nullable column is rejected
  (`error.MissingRequiredColumn`) — this is `NOT NULL` enforcement;
- a value that does not match the declared column type is rejected
  (`error.InvalidColumnValue`) — relational columns are strict;
- nullable columns absent from a document produce no cell (the typed column is
  sparse, matching `typed_doc_values` doc-id semantics);
- `json` columns are stringified to bytes and flagged `is_json` so the write
  path can additionally project the subtree via `pathfact` + dynamic templates.

Numeric physical encoding is chosen to match the engine's existing doc values
(`introducer.detectTypedValue` + the `search/query.zig` readers) so range scans
reuse the existing readers rather than needing new ones:

- `number` / `integer` → `f64` (native), read via `getF64` / `readF64Chunk`;
- `datetime` → raw `u64` epoch ns, read via `getU64` (RFC3339 string parsing to
  epoch is a follow-up; epoch integers / integer-strings are accepted today);
- `boolean` → `bool`, `geopoint` → packed lat/lon, `string`/`blob`/`geoshape`
  → `bytes`.

Round-trip through the real `TypedDocValuesWriter`/`TypedDocValuesReader` is
covered by unit tests.

### How this meets the segment builder

The engine already accumulates per-field typed columns at segment-build time:
`introducer.collectTypedFieldValuesRecursiveScoped` walks each document,
`detectTypedValue` infers a `ValueType` per field, and `appendTypedFieldValue`
feeds a per-field `TypedDocValuesWriter` (`introducer.zig:906`) across the whole
batch; `segment.zig` / `merger.zig` persist and merge the sections, and
`search/query.zig` reads them for range/equality predicates.

Because relational mode **enforces types**, every declared scalar column is
type-consistent across documents, so this existing detection path already
produces a correct typed column per declared scalar column — and the physical
mapping above is deliberately aligned with what `detectTypedValue` produces, so
no parallel storage path is introduced. `projectRelationalRowAlloc` is the
schema-authoritative counterpart: it validates/normalizes the same cells
(`NOT NULL`, strict types, json capture) ahead of the detector.

**Remaining wiring (Phase 3):** (1) exclude relational `json` columns from
typed-field detection so a `json` subtree is indexed as a document rather than
exploded into typed columns; (2) make declared column types authoritative over
value-based detection (so a column never silently drops on a stray mistyped
value); (3) the columnar scan operator + predicate routing. The natural seam is
to pass the relational column catalog (and json-column paths) into the
introducer alongside `TextAnalysisConfig`. In Phase B this is also where the
JSON blob write is dropped for non-`json` columns.

### Query path

The relational win is predicate pushdown. The planner gains a **columnar table
scan** operator that reads `typed_doc_values` chunks directly (`readU64Chunk`,
range bounds, term equality) for predicates on typed columns, instead of
routing every filter through the full-text index. Projection is served by
`ColumnarReader.readDoc(projection)`. Joins and `GROUP BY`-over-join are
unchanged — they already exist (see `JOINS.md`, `ALGEBRAIC.md`).

### Schema evolution

`schema_capability.classifyChange` already distinguishes additive changes
(new nullable column → no rebuild) from breaking changes (removed or
type-changed column → rebuild). Relational mode adds: making an existing
nullable column `NOT NULL` is a breaking change; widening (e.g. integer →
number) is additive where the physical type is compatible.

## Phased plan

- **Phase 1 — contract + catalog (this change).**
  `storage_mode` on `TableSchema` (spec + Go + Zig), `json` `AntflyType`, and
  `relationalColumnPlanAlloc` producing the typed-column catalog with tests.
  No behaviour change for document-mode tables.
- **Phase 2 — write path (projection done, encoding aligned).**
  `projectRelationalRowAlloc` produces typed cells per column, enforces
  `NOT NULL`, and captures `json` subtrees, verified end-to-end against the real
  `typed_doc_values` writer/reader. The physical encoding is aligned with the
  engine's existing doc values (`detectTypedValue` + `search/query.zig`), so the
  existing segment builder (`introducer.zig`) already materializes correct typed
  columns for type-enforced relational documents (see "How this meets the
  segment builder"). Remaining wiring is folded into Phase 3.
- **Phase 3 — introducer wiring + scan/pushdown.** Pass the relational column
  catalog into the introducer: exclude `json` columns from typed-field
  detection and make declared types authoritative; add the columnar scan
  operator, route typed-column predicates to it, and serve columnar projection
  on read.
- **Phase 4 — authoritative columns (Phase B).** Drop the JSON blob for
  non-`json` columns; reconstruct documents from columns on read.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution
