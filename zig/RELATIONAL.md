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
2. **Typed cells are first-class.** Every declared scalar property maps to a
   typed relational cell, with document-scoped column entries maintained in the
   relational base store so predicates and aggregations read committed table
   data instead of re-parsing JSON or consulting derived search segments.

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
- **Typed value encodings** — `section/typed_doc_values.zig`
  (`u64`/`f64`/`bytes`/`bool`/`geo_point`) supply the physical scalar value
  kinds reused by relational row cells and column entries.
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

## The pivotal decision: one relational base store

Relational mode uses the relational base-store keyspace as the source of truth.
Incoming JSON is validated against the closed schema, projected into typed
cells, and written through the relational participant. The store maintains both
the packed row entry used for point reads/reconstruction and document-scoped
column entries used for scalar scans. There is no legacy JSON primary-row mode
for relational tables in this feature set.

Full-text, dense, sparse, graph, and algebraic indexes remain derived artifacts.
They can be rebuilt from the relational base store, but they are not the
authoritative column store.

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
ready to serialize as the table's authoritative base-row value:

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
- `datetime` → raw `u64` epoch ns, read via `getU64` (accepts epoch integers,
  integer-strings, and RFC3339 UTC timestamp strings on ingest);
- `boolean` → `bool`, `geopoint` → packed lat/lon, `string`/`blob`/`geoshape`
  → `bytes`.

Round-trip through the real `TypedDocValuesWriter`/`TypedDocValuesReader` is
covered by unit tests.

### How this meets derived indexes

Relational storage is a one-store design. The packed row under the relational
base-row keyspace is the only authoritative copy of declared column values.
Full-text, vector, algebraic, and graph indexes are derived artifacts.

The full-text segment builder still supports typed doc values for document-mode
and schema-less indexing, but relational projection now sets
`TextDocument.typed_fields` to an explicit empty slice. That prevents the
introducer from inferring full-column typed doc values from the document body
and keeps relational scalar columns out of derived text segments. Text columns
continue through the normal analyzer/inverted-index path.

The *compiled* runtime schema carries the relational contract:
`runtime_schema.TableSchema` (`storage/schema.zig`) has `storage_mode` and a
`relational_columns` catalog (`RelationalColumn{name, path, field_type,
nullable}`), populated by `deriveRuntimeTableSchema` and round-tripped through
the versioned binary format. `NOT NULL` is enforced upstream by JSON-schema
`required` validation, and `projectRelationalRowAlloc` enforces strict physical
types before the row is serialized.

### Query path

The relational win is predicate pushdown. The planner uses a **columnar table
scan** operator over the relational base store for predicates on declared
columns, instead of routing every scalar filter through the full-text index.
Projection and stored-document reads reconstruct JSON from the committed packed
row. Full-text segments supply only text matching, scoring, doc identity, and
index-local metadata; they are not a second relational column store. Joins and
`GROUP BY`-over-join are unchanged — they already exist (see `JOINS.md`,
`ALGEBRAIC.md`).

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
  row codec and typed-value readers. The physical value kinds remain aligned
  with the engine's typed doc values so predicate code can share comparison and
  decoding behavior without making search segments authoritative.
- **Phase 3 — write path wired end-to-end (done).** The compiled runtime schema
  carries `storage_mode` + the `relational_columns` catalog (derived in
  `schema/mod.zig`, v9 binary format); relational writes serialize the declared
  columns into the dedicated base-row keyspace. Text projection intentionally
  supplies an empty typed-field list so the introducer does not infer a second
  full-column copy in derived text segments. Covered by runtime-schema,
  `document_mapper`, and DB tests.
- **Phase 4 — read path with schema-aware predicate routing (done).** Relational
  predicates are resolved from relational base-row column scans for supported
  keyword/range/bool/geo clauses. Predicate auto-routing is wired through
  `searchQueryToFilterArenaRelational`, including `bool_query` recursion.
  Document-mode queries are unchanged.
- **Phase 5 — derived text simplification (done).** Relational text projection
  no longer writes full-column `typed_doc_values` sections, no longer writes a
  segment manifest, and no longer reconstructs rows from segment-local columns.
  Full-text segments remain derived text artifacts: they carry term/scoring
  data, doc identity, and projection payloads only. Query materialization and
  `include_stored` hydrate from the committed relational base row.

- **Phase 6 — authoritative relational base row (done).** The remaining
  document copy is no longer a generic primary document value for relational
  tables. Relational rows are stored under the dedicated relational row keyspace
  as serialized typed columns, so reconstruction-on-read stays fully synchronous
  without relying on async segment reconstruction.

  - **Storage format.** A relational document is stored as a packed
    `relational_row_codec` value (magic `AROW`) under the relational row
    keyspace, with one document-scoped column entry per committed cell. The row
    entry keeps point lookups / read-modify-write transforms a single atomic
    document operation; the column entries make scalar scans independent from
    derived segment doc-values and move with the document range during
    split/merge. The row is self-describing (each cell carries path + physical
    `typed_doc_values` type + `is_json`), so reconstruction needs no schema
    lookup. Round-trip is *canonical*, not byte-exact (schema field order,
    numbers/datetime normalized) — acceptable because relational tables are
    closed-schema, so every referenceable field is a declared column.

  - **One formatter, one row source.** `relational_row_codec.appendCellValue` is
    the single canonical-JSON per-value formatter for relational base-row
    materialization. Point reads, transforms, vector `include_stored`, backfill,
    enrichment, and full-text result materialization all hydrate from the same
    committed row representation.

  - **Seam A — materialize to JSON.** `materializeDocumentValueAlloc` is the one
    decode point every document-value reader routes through. The synchronous
    `DB.get` chokepoint, and every async reader that re-reads a stored document —
    index backfill (text/dense/sparse), derived replay collectors, and the
    enrichment runtime — reconstruct JSON from the typed row (or pass a
    document-mode blob through). The initial-write indexers are unaffected: they
    carry the original JSON (`cleaned_value`/`write.value`) and never re-read the
    store. Routed reader-first, write-flipped last, so each step stayed green.

  - **Seam B — typed-column scan path.** Predicate filters and scan-based
    aggregations read document-scoped relational column entries. Where a
    consumer genuinely wants one source field (the enrichment `source_field`
    case), `relational_row_codec.findCellByPath` can read that cell straight
    from the row, skipping full reconstruction + re-parse. Consumers that need
    the whole document (templates, algebraic fact-projection, `db.get`) keep
    Seam A.

  Document-mode tables are unchanged throughout: their KV value stays a JSON
  blob, and the seam is a pure passthrough (the row magic never matches a JSON
  document). Covered by the relational write → store typed row → async
  index/catch-up → full-text query reconstruction tests, alongside the KV
  row-codec and document-mode passthrough coverage.

### Relational base-row rationale

The important design decision is that relational point reads are still served
from the synchronous base-row store, not from search segments. Search segments
are columnar, but they are built asynchronously; a transform or point lookup
immediately after a write must see the just-written document without waiting for
segment materialization. The existing two-phase commit machinery gives us the
right commit boundary; the relational participant uses that boundary for both
row reconstruction and column-entry maintenance.

The landed design keeps the DocStore transaction boundary as the synchronous
source of truth while moving relational documents to dedicated relational row
and column keys:

- **Chosen:** one packed typed-row value per document, detected by the `AROW`
  magic prefix and stored under the relational row keyspace rather than the
  generic primary document key, plus document-scoped column entries maintained
  by the same participant. This keeps `DB.get`, transforms, lookup, vector
  `include_stored`, backfill, and enrichment readers synchronous while removing
  the relational JSON blob and avoiding a second base-row copy. The row carries
  enough path/type metadata that generic store-value readers can reconstruct JSON
  without looking up live schema.
- **Target:** make relational typed storage the single physical base store,
  with point reads, transforms, predicate scans, recovery, split/merge, and
  derived-index backfill reading the same committed representation. The concrete
  work is tracked in [RELATIONAL_ROADMAP.md](RELATIONAL_ROADMAP.md).

The value-level magic check is intentional because `DB.get` is generic and also
serves non-document internal keys. Document-mode JSON blobs and internal store
values pass through untouched. Relational rows reconstruct a canonical JSON
document rather than the original byte sequence; this is acceptable because
relational tables are closed-schema and every referenceable field is a declared
column. Vector-field stripping follows the same document-value seam as document
mode: the typed row represents the stored document value, not transient vector
payloads stripped from storage.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution

---

## One-store text projection: DONE

Relational tables store one packed typed-row value under the relational base-row
keyspace. Derived full-text segments no longer carry a second copy of the full
relational column set as `typed_doc_values`, no longer emit a relational manifest
section, and no longer reconstruct JSON from segment-local columns. Segment
stored bodies are projection payloads used by the text index, while authoritative
relational reads hydrate from the committed base row.

This keeps the physical model simple:

- **Write path**: relational writes project the declared columns once into the
  base-row value. Full-text projection emits analyzed text and any text-specific
  payload needed by the derived index, but it does not duplicate scalar
  relational columns into the segment.
- **Point and mutation reads**: `DB.get`, transforms, update/delete validation,
  vector `include_stored`, and lookup/enrichment paths read the packed base row.
- **Query reads**: structured relational predicates resolve document ids from
  base-store column entries, then full-text ranking/materialization hydrates
  from the base row before returning public stored data.
- **Merge / compaction / split**: segment data movement remains schema-less
  because it only moves derived text-index state. It does not need a relational
  column catalog, and it cannot become a second source of truth for row values.

The earlier segment-manifest approach was removed rather than retained as a
compatibility path because relational mode is new. There is no supported
on-disk relational segment format to migrate.

### Validation

Covered by mapper tests proving relational text projection omits segment
typed-column copies, DB tests proving full-text results hydrate from the base
row, point-read tests over the relational base store, and the root test suite.
