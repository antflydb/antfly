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

Schema create/update validation normalizes omitted `enforce_types` to `true`
for relational schemas, rejects explicit `enforce_types: false`, rejects open
top-level document types, and rejects relational schemas that would produce no
storable relational columns. This keeps relational mode a single-store contract
rather than a document-mode table with optional relational projections.

`json` is added to `AntflyType`. A `json` column is stored as a `bytes` column
and indexed like a document subtree (path facts + dynamic templates). It is the
escape hatch for semi-structured data inside an otherwise typed row.

An embedded JSON column declares its document-store indexing contract on the
property itself:

```json
{
  "type": "object",
  "properties": {
    "id": { "type": "keyword" },
    "tenant_id": { "type": "keyword" },
    "attrs": {
      "type": "json",
      "schema": {
        "type": "object",
        "properties": {
          "title": { "type": "text" },
          "plan": { "type": "keyword" },
          "score": { "type": "numeric", "doc_values": true }
        },
        "additionalProperties": true
      },
      "dynamic_templates": {
        "metrics": {
          "path_match": "metrics.*",
          "mapping": { "type": "numeric", "doc_values": true }
        }
      }
    }
  },
  "required": ["id", "tenant_id"],
  "additionalProperties": false
}
```

The embedded `schema` object is evaluated under the owning column path:
`attrs.title` is indexed as full text, `attrs.plan` as a keyword/path fact,
`attrs.score` as an algebraic-capable numeric fact, and
`attrs.metrics.cpu` can be promoted by the scoped dynamic template. Unknown
top-level fields outside `attrs` are still rejected by the closed relational
schema. Top-level dynamic templates in relational schemas stay invalid; flexible
fields belong behind an explicit `json` column.

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

## Implementation state

Relational mode now uses one authoritative base representation. A relational
document is not stored as a generic JSON primary value plus derived column
copies; it is projected once into typed cells and committed through the
relational participant.

### Relational participant

The relational base-store participant owns row and column state for relational
tables:

- **Row key:** the dedicated relational row keyspace for the document key.
- **Row payload:** a packed `relational_row_codec` value (magic `AROW`) with one
  self-describing cell per committed column. Each cell carries path, physical
  `typed_doc_values` type, and `is_json`, so reconstruction does not require a
  live schema lookup.
- **Column access:** document-scoped column entries maintained alongside the row
  entry. Scalar predicates and scan-based aggregations read these entries rather
  than derived segment doc-values.
- **Commit boundary:** prepare/commit/abort/read/scan methods participate in the
  existing two-phase commit and recovery path, so committed rows, column
  entries, and deletes resolve at the same visibility point.

The row entry keeps point lookups and read-modify-write transforms as one atomic
document operation. Column entries make scalar scans independent from derived
search segments and move with the document range during split/merge. A future
physical optimization can pack entries into larger column blocks without
changing the participant boundary.

### Write and read seams

Writes validate incoming JSON against the closed runtime schema, project it via
`schema_capability.projectRelationalRowAlloc`, and commit the resulting typed
cells through `relational_store.WriteParticipant`. The original JSON is only
transient write input for foreground derived-index projection.

`materializeDocumentValueAlloc` is the document-value seam. Document-mode values
pass through unchanged; relational values reconstruct canonical JSON from the
typed row. Point reads, transforms, lookup, vector `include_stored`, enrichment
source reads, full-text result materialization, derived replay, and backfill all
hydrate from the same committed relational row representation. Consumers that
need one declared source field, such as enrichment `source_field`, can read the
cell directly with `relational_row_codec.findCellByPath`.

Relational rows reconstruct canonical JSON rather than the original byte
sequence. That is acceptable because relational tables are closed-schema and
every referenceable field is a declared column. Vector-field stripping follows
the same stored-document seam as document mode: the typed row represents the
stored document value, not transient vector payloads stripped from storage.

### Derived index contract

Full-text, dense, sparse, graph, and algebraic indexes are derived artifacts.
They may carry their own index-local state for ranking, traversal, pruning, or
fold execution, but they are not authoritative row or column storage.

Full-text projection for relational tables sets `TextDocument.typed_fields` to
an explicit empty slice, so derived text segments do not infer and store a
second full-column `typed_doc_values` copy. Text segments keep term/scoring data,
doc identity, and projection payloads only. Query materialization and
`include_stored` hydrate from the relational base row.

Embedded `json` columns are the exception to the closed-row shape, not to the
one-store rule. A `json` property may carry its own document `schema` and
scoped `dynamic_templates`. Runtime compilation prefixes the embedded paths
with the column name (`attrs.title`, `attrs.plan`, `attrs.metrics.latency`) and
projects them into the same derived full-text/path-fact/algebraic artifacts used
for document tables. The JSON cell itself remains stored only in the relational
base row and is reprojected from that row during replay/backfill.

Changing a JSON column's embedded `schema` or `dynamic_templates` is therefore
a derived-index schema update, not a row migration. This follows the same
user-facing model as document-store `jsonschema` updates: commit the new schema
metadata, mark affected derived artifacts pending, and rebuild search and
aggregation state from stored values. The committed relational row does not move
unless the row value itself is updated; changing `attrs.schema` only changes how
the `attrs` cell is interpreted for derived indexing.

Schema-derived algebraic configs record each JSON column as a
`json_subdocument_domains` entry with a capability fingerprint and lifecycle
status. When that fingerprint changes, durable schema regeneration and live
reload mark the affected domain `rebuild_required`, and algebraic query
planning withholds fields below that JSON column until the sidecar is rebuilt
from committed rows. Full-text JSON projection follows the existing
schema-versioned index handoff.

The production invariants are:

- embedded JSON indexes are disposable and never accept writes that bypass the
  relational row;
- new derived index generations are built from committed relational rows;
- query planners only advertise index-served JSON paths after the matching
  generation is complete, or otherwise fall back/report pending capability;
- stale JSON-subdocument artifacts are safe to drop because they contain no
  authoritative data;
- restore and replication need only the relational row plus schema metadata to
  recreate JSON full-text and algebraic artifacts.

Algebraic, graph, vector, enrichment, split-shadow, catch-up, and backfill
readers that need a document body carry the relational-base-row context and read
from the relational row keyspace. Algebraic fact projection and materialization
therefore rebuild from committed typed rows, not from stale generic document KV
values or derived text segment columns.

Relational table creation and schema updates both run the same schema-aware
index preparation: if no algebraic index exists, `algebraic_index_v0` is added
with `derive_from_schema: true` and stored as a concrete derived config. That
keeps aggregation pushdown available whether the table starts relational or is
switched to relational mode later.

### Query and movement invariants

Structured relational filters for supported keyword/range/bool/geo clauses
resolve against relational column entries. Top-level supported relational
structured queries become base-row doc constraints over the text match-all path,
so scalar query results follow the committed relational row rather than stale
segment doc-values. Unsupported text-oriented shapes may still use the inverted
text index, but not segment doc-values as a relational column source.

Predicates under a `json` column route through the embedded document-derived
index for that column path, then intersect with top-level relational column
filters by document id. Result materialization still reconstructs from the
relational base row.

For example:

```text
tenant_id:acme AND attrs.plan:pro AND attrs.score:[10 TO *]
```

`tenant_id` uses relational column entries. `attrs.plan` and `attrs.score` use
the embedded JSON artifacts scoped to the `attrs` column. The final hit list is
materialized from relational rows, not from derived artifacts.

Split, merge, replay, TTL cleanup, and generated-enrichment replay treat the
relational base store as table data. Split prepare/finalize moves relational row
entries and document-scoped column entries into the destination range and removes
them from the finalized parent range. Merge bootstrap/catch-up reprojects donor
logical writes into receiver relational rows and column entries; donor deletes
remove receiver column entries during catch-up.

### No legacy relational storage

Relational mode is new in this feature set, so there is no migration or
compatibility path for older experimental relational encodings. The supported
state is the relational participant keyspace. Generic primary document rows for
relational ids are treated only as invariant cleanup state; relational readers
must not use a generic document KV value as row data. Document-mode KV values
remain JSON blobs and are preserved exactly.

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

- **Landed shape:** one packed typed-row value per document, detected by the `AROW`
  magic prefix and stored under the relational row keyspace rather than the
  generic primary document key, plus document-scoped column entries maintained
  by the same participant. This keeps `DB.get`, transforms, lookup, vector
  `include_stored`, backfill, and enrichment readers synchronous while removing
  the relational JSON blob and avoiding a second base-row copy. The row carries
  enough path/type metadata that generic store-value readers can reconstruct JSON
  without looking up live schema.
- **Invariant:** relational typed storage is the single physical base store for
  point reads, transforms, predicate scans, recovery, split/merge, and
  derived-index backfill.

The value-level magic check is intentional because `DB.get` is generic and also
serves non-document internal keys. Document-mode JSON blobs and internal store
values pass through untouched. Relational rows reconstruct a canonical JSON
document rather than the original byte sequence; this is acceptable because
relational tables are closed-schema and every referenceable field is a declared
column. Vector-field stripping follows the same document-value seam as document
mode: the typed row represents the stored document value, not transient vector
payloads stripped from storage.

## Validation coverage

The coverage expected for this feature set is:

- write -> `DB.get` -> query stored data for all scalar types plus `json`;
- transform read-modify-write on scalar and nullable columns;
- delete and overwrite remove old column values from scans;
- read-after-commit through the existing transaction/2PC path;
- abort and recovery of prepared relational writes;
- replay/backfill derived text/algebraic/graph/vector/sparse indexes from typed
  base rows;
- split and merge preserve relational base rows and column scans;
- document-mode tables continue to store/retrieve JSON blobs unchanged;
- mixed text search plus relational predicate filters;
- generated-enrichment, TTL cleanup, and graph artifact-source readers hydrate
  from committed relational rows when they need stored document data.

Current tests cover mapper projection, runtime-schema round-trip, row-codec
round-trip, document-mode passthrough, relational point reads, full-text
`include_stored` hydration from base rows, scan-based aggregations over
base-row `stored_data`, scalar filters over column entries, transaction
commit/abort/transform behavior, stale generic-primary cleanup, split movement,
and merge bootstrap/catch-up for row and column entries.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution
