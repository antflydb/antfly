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
- `datetime` → raw `u64` epoch ns, read via `getU64` (accepts epoch integers,
  integer-strings, and RFC3339 UTC timestamp strings on ingest);
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

The write and read paths are wired end-to-end. The *compiled* runtime schema
carries the contract: `runtime_schema.TableSchema` (`storage/schema.zig`) has
`storage_mode` and a `relational_columns` catalog (`RelationalColumn{name, path,
field_type, nullable}`), populated by `deriveRuntimeTableSchema` and
round-tripped through the versioned binary format (format version 9).
`document_mapper.extractTextFieldsFromValue` then branches on
`schema.storage_mode == .relational` and sets `TextDocument.typed_fields` from
the catalog via `buildRelationalTypedFields` (numeric/datetime/boolean/geopoint
→ typed; keyword/text continue through the full-text path; json columns are
subtrees), bypassing value detection. `NOT NULL` is enforced upstream by
JSON-schema `required` validation. The introducer materializes those typed
columns into segment doc-values and, for relational tables, the KV value is a
self-describing typed row rather than the original JSON blob.

### Query path

The relational win is predicate pushdown. The planner uses a **columnar table
scan** operator that reads `typed_doc_values` chunks directly (`readU64Chunk`,
range bounds, term equality) for predicates on typed columns, instead of
routing every filter through the full-text index. Projection and stored-document
reads reconstruct JSON from the typed columns at the segment and KV-row read
seams. Joins and `GROUP BY`-over-join are unchanged — they already exist (see
`JOINS.md`, `ALGEBRAIC.md`).

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
  segment builder"). The integration work is completed by Phase 3.
- **Phase 3 — write path wired end-to-end (done).** The compiled runtime schema
  carries `storage_mode` + the `relational_columns` catalog (derived in
  `schema/mod.zig`, v9 binary format); `document_mapper` branches on
  `storage_mode` and builds authoritative `TextDocument.typed_fields` from the
  catalog via `buildRelationalTypedFields`; the introducer materializes the
  typed columns. Covered by runtime-schema, `document_mapper`, and introducer
  tests.
- **Phase 4 — read path with schema-aware predicate routing (done).** Relational
  typed columns are scanned by the engine's typed filters (`RangeFilter`,
  `DateRangeFilter`, `BoolFieldFilter`, geo) via `typed_doc_values` by column
  name. Predicate auto-routing is now wired: `searchQueryToFilterArenaRelational`
  threads the declared keyword-column names through filter compilation (incl. the
  `bool_query` recursion) and routes an exact `.term` predicate on a declared
  keyword column to a columnar `TypedTermFilter` (the column-scan counterpart to
  the inverted-index `TermFilter`) instead of the analyzed full-text index.
  numeric/date/bool/geo predicates already read the columns. Document-mode queries
  are unchanged (empty keyword-column set). datetime columns accept RFC3339 UTC
  strings as well as epoch integers on ingest (shared `introducer.parseRfc3339ToNs`).
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

  Read-source swap done (live, full-text path): the full-text hit-materialization
  seam (`storage/db/query/search_exec.zig`) now, for relational tables, builds a
  hit's `stored_data` by calling `reconstructRelationalDocumentFromSegmentAlloc`
  on the resolved segment + segment-local id (`snapshot.resolveDocId`, the same
  id `typed_doc_values` is keyed by) instead of the segment stored-doc blob.
  Document-mode tables are completely unchanged (they keep the blob path). Proven
  end to end by the "relational table full-text search reconstructs stored_data
  from columns" test (real DB: relational schema → write → full-text query with
  `include_stored` → `stored_data` reconstructed from typed columns), and
  validated by the full `zig build unit-test` suite (0 failed, 0 leaked).

  Segment-blob removal (done): relational segments no longer write a stored-doc
  blob at all — the body is stored empty and reconstructed from the typed
  columns at the single `SegmentReader.storedDocDecompressed` chokepoint, via a
  self-describing per-segment manifest carried through merge and shard split.
  See "Blob-write removal" below.

- **Phase 6 — authoritative relational base row (done).** The remaining
  document copy is no longer a generic primary document value for relational
  tables. Relational rows are stored under the dedicated relational row keyspace
  as serialized typed columns, so reconstruction-on-read stays fully synchronous
  without relying on async segment reconstruction.

  - **Storage format.** A relational document is stored as one packed
    `relational_row_codec` value (magic `AROW`), not a JSON blob and not a
    key-range of per-column pairs. One relational row pair per document keeps
    point lookups / read-modify-write transforms a single atomic op and shard
    splits boundary-agnostic; the columnar predicate-pushdown tier stays in the
    search segments. The row is self-describing (each cell carries path + physical
    `typed_doc_values` type + `is_json`), so reconstruction needs no schema
    lookup. Round-trip is *canonical*, not byte-exact (schema field order,
    numbers/datetime normalized) — acceptable because relational tables are
    closed-schema, so every referenceable field is a declared column.

  - **One formatter, two read paths.** `relational_row_codec.appendCellValue` is
    the single canonical-JSON per-value formatter, shared by the relational
    base-row read path and the segment read path
    (`document_mapper.appendReconstructedColumn` delegates to it), so a document
    reconstructs *byte-for-byte identically* whether served from a segment
    (full-text reads) or the relational row store (point/transform/vector reads).

  - **Seam A — materialize to JSON.** `materializeDocumentValueAlloc` is the one
    decode point every document-value reader routes through. The synchronous
    `DB.get` chokepoint, and every async reader that re-reads a stored document —
    index backfill (text/dense/sparse), derived replay collectors, and the
    enrichment runtime — reconstruct JSON from the typed row (or pass a
    document-mode blob through). The initial-write indexers are unaffected: they
    carry the original JSON (`cleaned_value`/`write.value`) and never re-read the
    store. Routed reader-first, write-flipped last, so each step stayed green.

  - **Seam B — typed-column fast path.** Where a consumer genuinely wants one
    field (the enrichment `source_field` case), `relational_row_codec.findCellByPath`
    reads that single column straight from the row, skipping full reconstruction
    + re-parse. Consumers that legitimately need the whole document (templates,
    algebraic fact-projection, `db.get`) keep Seam A — that is the correct and
    final answer for them, not a compromise.

  Document-mode tables are unchanged throughout: their KV value stays a JSON
  blob, and the seam is a pure passthrough (the row magic never matches a JSON
  document). Covered by the relational write → store typed row → async
  index/catch-up → full-text query reconstruction tests, alongside the KV
  row-codec and document-mode passthrough coverage.

### KV typed-row rationale

The important design decision is that relational KV reads are still served from
the synchronous KV store, not from search segments. Search segments are
columnar, but they are built asynchronously; a transform or point lookup
immediately after a write must see the just-written document without waiting for
segment materialization. The existing two-phase commit machinery gives us the
right commit boundary, but the columnar storage still needs to become a
first-class synchronous participant before it can replace the generic relational
KV row.

The landed design keeps the DocStore transaction boundary as the synchronous
source of truth while moving relational documents to a dedicated relational row
key:

- **Chosen:** one packed typed-row value per document, detected by the `AROW`
  magic prefix and stored under the relational row keyspace rather than the
  generic primary document key. This keeps `DB.get`, transforms, lookup, vector
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

## Blob-write removal: DONE (self-describing segment manifest)

The segment stored-doc body is no longer written for relational tables. Each
document is stored as an empty body plus its typed columns; the JSON is
reconstructed from those columns on read. This was landed as a versioned segment
format addition (relational mode is new, so no legacy on-disk compatibility was
required).

### Design as built

- **New leaf module `section/relational_manifest.zig`** (depends only on `std`
  + `typed_doc_values`, so both the segment reader and `document_mapper` import
  it without a cycle). Holds:
  - `ManifestColumn{ name, path, value_type, is_json }` — the schema-free facts
    reconstruction needs. (Physical `value_type` + `is_json` disambiguate the
    cases the raw column type can't: a `bytes_val` column may be a string, a
    `json` subtree, or a geoshape.)
  - `serialize` / `parse` for the manifest section bytes.
  - `reconstructDocumentAlloc(reader, columns, local_doc_id)` — the single
    reconstruction routine, keyed by the **segment-local** doc id (the id
    `typed_doc_values` is keyed by, == `resolveDocId().local_id`).

- **New `SectionType.relational_manifest` (=6)** stored under the reserved field
  `\x00__antfly_relational_manifest` (mirrors the `doc_ordinals` convention).

- **Write path** (`introducer.zig`): `BuildTextOptions.relational_manifest_columns`.
  When set, the build writes an empty stored-doc body per document and attaches
  the manifest section (`SegmentWriter.addRelationalManifest`). `document_mapper`
  derives the columns from the runtime schema
  (`relationalManifestColumnsAlloc`) and threads them through every projection
  build path (single- and multi-segment). Document-mode builds pass `null` and
  are byte-for-byte unchanged.

- **Read path** (`segment.zig`): `SegmentReader.storedDocDecompressed` — the
  single chokepoint all body reads funnel through (query hit materialization,
  vector `include_stored`, IP-range JSON fallback, shard split) — checks for a
  manifest (`relationalManifest()`) and, if present, reconstructs from columns;
  the document id still comes from the stored-doc table. No call site needed
  schema access. `.id`-only borrowing `storedDoc()` callers are untouched (id is
  still stored).

### The two schema-less data-movement paths (and the bugs found fixing them)

These are why this had to be a *self-describing* segment, not just a live-schema
lookup — neither path has the runtime schema:

- **Merge / compaction** (`segment.zig`): the manifest is carried forward
  verbatim (the column catalog is identical across a table's segments, so the
  first input with one is authoritative). Reconstruction then works on the
  merged segment. **Bug found & fixed:** `mergeTypedDocValuesSections` returned
  `error.UnsupportedTypedDocValues` for `bytes_val` columns — previously bytes
  doc-values were never merge-critical (the blob carried strings), but
  relational string/json columns *are* `bytes_val`, so merge now copies them.
  Without this, compacting a relational table would have lost all string/json
  columns (silent data loss). Covered by a dedicated build→empty-body→merge→
  reconstruct test.

- **Shard split** (`index_manager.zig` `buildSplitSegment`): reads
  `storedDocDecompressed` (now reconstructing) and rebuilds the split segment.
  **Bug found & fixed:** the rebuild passed `null` schema, which would have
  degraded a relational split segment to document-mode (body present but typed
  columns + manifest lost → no columnar pushdown on split data). Now the
  `TextIndex.runtime_schema` is threaded in so the split segment re-derives its
  columns + manifest.

### Validation

Covered by manifest round-trip + merge-survival tests, the end-to-end DB
relational reconstruction test, and the merge/split/persistence suite.
Document-mode behaviour is unchanged because its code paths are not entered
when no manifest is present.

### Historical note

This section originally landed before the KV typed-row redesign. Phase 6 above
has since made the synchronous source-of-truth (`db.get`, transform reads, and
vector `include_stored`) column-derived as well. The blob-write details remain
useful because they describe the segment manifest format and the schema-less
merge/split paths.
