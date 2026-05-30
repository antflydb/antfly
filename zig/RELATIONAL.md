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

The introducer also accepts **caller-supplied** typed columns directly:
`TextDocument.typed_fields` bypasses value-based detection entirely
(`introducer.zig:391`). This is the relational hand-off point.
`schema_capability.relationalTypedColumnsAlloc` produces exactly that input from
a relational document — one typed field per present column that is *physically*
a typed doc value, carrying the schema-declared type — which gives, for free:

- **authoritative types** (types come from the schema, not from per-value
  detection, so a column never silently drops on a stray mistyped value);
- **only the typed-doc-value kinds are emitted** — numeric/integer (`f64`),
  datetime (`u64`), boolean, geopoint. `keyword`/`text` columns are *not* typed
  doc values: they use the full-text/inverted index for term and range
  predicates (the existing schema-aware text mapping handles them). `json`
  columns are excluded too and indexed as document subtrees, never exploded into
  typed columns.

Both properties are verified at the introducer boundary: the
`buildSegmentFromText indexes caller-supplied relational typed columns` test
feeds relational typed columns through `buildSegmentFromText` and asserts the
declared columns become readable `typed_doc_values` sections while undeclared /
`json` fields produce none. `projectRelationalRowAlloc` underneath also enforces
`NOT NULL` and strict types and captures the `json` subtree bytes.

The hand-off keeps layers separate: `schema_capability` emits
`RelationalTypedField` using `typed_doc_values` types only, and the
orchestration layer renames `name` → `field_name` to get an
`introducer.TypedFieldValue` (the other fields are identical).

The write path is wired end-to-end. The *compiled* runtime schema carries the
contract: `runtime_schema.TableSchema` (`storage/schema.zig`) has `storage_mode`
and a `relational_columns` catalog (`RelationalColumn{name, path, field_type,
nullable}`), populated by `deriveRuntimeTableSchema` and round-tripped through
the versioned binary format (format version 9). `document_mapper.extractTextFieldsFromValue`
then branches on `schema.storage_mode == .relational` and sets
`TextDocument.typed_fields` from the catalog via `buildRelationalTypedFields`
(numeric/datetime/boolean/geopoint → typed; keyword/text continue through the
full-text path; json columns are subtrees), bypassing value detection. `NOT NULL`
is enforced upstream by JSON-schema `required` validation. The introducer then
materializes the typed columns. Verified by `document_mapper` and introducer
tests.

**Remaining wiring:** the read side — a columnar scan operator + predicate
routing (below). In Phase B this is also where the JSON blob write is dropped
for non-`json` columns.

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
- **Phase 3 — write path wired end-to-end (done).** The compiled runtime schema
  carries `storage_mode` + the `relational_columns` catalog (derived in
  `schema/mod.zig`, v9 binary format); `document_mapper` branches on
  `storage_mode` and builds authoritative `TextDocument.typed_fields` from the
  catalog via `buildRelationalTypedFields`; the introducer materializes the
  typed columns. Covered by runtime-schema, `document_mapper`, and introducer
  tests.
- **Phase 4 — read path (works; verified end-to-end).** Relational typed
  columns are scanned by the engine's existing typed filters (`RangeFilter`,
  `DateRangeFilter`, `BoolFieldFilter`, geo) via `typed_doc_values` by column
  name; `.range` queries already route there. Verified by the end-to-end
  range-scan test. Remaining enhancement: schema-aware auto-routing of
  predicates on typed columns (use the runtime `relational_columns` catalog).
- **Phase 5 — authoritative columns (Phase B).** Foundation done:
  `reconstructRelationalDocumentAlloc` rebuilds a JSON document from a projected
  `RelationalRow` (string/blob/geoshape → string, numeric/integer → number,
  datetime → epoch number, boolean, geopoint → `{lat,lon}`, json → embedded
  subtree; absent nullable columns omitted), verified by a doc → project →
  reconstruct round-trip test. This proves the typed columns carry enough to
  rebuild the document.

  Read primitive done: `TypedDocValuesReader.getBytes` provides per-doc
  random-access retrieval of `bytes_val` columns (variable-length entries walked
  by offset), so string/blob/geoshape and `json` columns can be read back from a
  persisted segment — the value-retrieval gap that previously only had
  bulk chunk reads. Verified by a multi-value round-trip test.

  Storage projection done: `relationalStorageColumnsAlloc` emits the *full
  reconstructable* column set — unlike `relationalTypedColumnsAlloc` (scan/index
  routing only), it includes string/blob/geoshape and `json` columns as
  `bytes_val`. The full storage cycle is verified end-to-end by the "relational
  storage columns persist and reconstruct the document" test: project → persist
  each column through the real `TypedDocValuesWriter` → read every value back
  (`getBytes`/`getU64`/`getF64`/`getBool`/`getGeoPoint`) → rebuild a
  `RelationalRow` → `reconstructRelationalDocumentAlloc` → assert the document
  matches the original. This is the complete authoritative-columns data path in
  isolation.

  Write-side wiring done (live): `document_mapper.buildRelationalTypedFields`
  now emits **every** present relational column as a `TextDocument.typed_fields`
  entry, persisting string/blob/geoshape/json columns as `bytes_val` sections
  (via `relationalStorageValueType` / `coerceRelationalStorageValue`) in addition
  to the numeric/datetime/boolean/geopoint scan columns. So a relational segment
  written today already carries a complete, reconstructable column set —
  numeric/datetime/boolean/geopoint sections double as predicate-scan columns,
  string columns also keep their analyzed inverted-index entries for term
  queries, and the `bytes_val` sections are the reconstruction source. Validated
  by the full `zig build unit-test` suite (0 failed, 0 leaked), so every segment
  reader/merger/search path tolerates the added sections.

  Read-side reconstruction done (segment-level):
  `document_mapper.reconstructRelationalDocumentFromSegmentAlloc` rebuilds a
  document's JSON from the persisted column sections of a real
  `SegmentReader` — for each declared column it reads the `typed_doc_values`
  section by name and pulls the value at the doc ordinal
  (`getBytes`/`getU64`/`getF64`/`getBool`/`getGeoPoint`), emitting JSON keyed by
  column path; absent (sectionless) nullable columns are omitted; `json` bytes
  are embedded verbatim, strings are JSON-escaped. The complete
  write → persist → read → reconstruct cycle is verified on a real
  introducer-built segment by the "relational document reconstructs from a
  persisted segment" test (plus an absent-nullable-column case).

  Remaining (the last step, deliberately not done — it changes the read source
  of truth): drop the `stored_data` blob. Today the blob (`segment.addStoredDoc`)
  is still written and remains the document source of truth, consumed in many
  read sites (`storage/db/aggregations.zig`, `db.zig`). The final step is to
  route those reads through `reconstructRelationalDocumentFromSegmentAlloc`
  (synthesizing `stored_data` on demand) and then stop writing the blob for
  non-`json` columns. The full reconstruction path now exists and is segment-
  verified; what remains is swapping the read source and removing the blob
  write, validated against the whole suite. Until then the blob is
  redundant-but-authoritative and the column work is additive.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution
