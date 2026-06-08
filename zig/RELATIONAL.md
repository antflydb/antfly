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
the packed row entry used for point reads/reconstruction and secondary column
entries used for movement, cleanup, and scalar scans. Relational tables use this
relational participant keyspace from the start; they do not use generic JSON
primary rows as their authoritative storage.

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
- Exactly one document schema is allowed in v1; the physical relational row
  does not carry a document-type discriminator.
- Each document type is treated as closed (`additionalProperties: false`)
  unless a field is explicitly typed `json`.
- `required_fields` declares `NOT NULL` columns.
- `dynamic_templates` apply only inside `json` columns.

Schema create/update validation normalizes omitted `enforce_types` to `true`
for relational schemas, rejects explicit `enforce_types: false`, rejects open
top-level document types, rejects multi-document-type schemas, and rejects
relational schemas that would produce no storable relational columns. This
keeps relational mode a single-store contract rather than a document-mode table
with optional relational projections.

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

Constraints in scope: primary identity is either the existing document key or a
declared `primary_key.columns` tuple; `NOT NULL` via required schema fields;
unique constraints over one or more ordered declared non-`json` relational
columns; `on_delete: "restrict"` foreign keys; bounded local nullable-column
`on_delete: "set_null"` foreign keys; and bounded local `on_delete: "cascade"`
foreign keys from declared scalar child columns to either a parent table's `_id`,
a declared primary-key tuple, or a declared unique parent column tuple.
Cross-table primary-key and unique targets route through owner topology when
configured and fail closed when the required owner range is missing.
Existing Antfly transaction/2PC semantics still apply to relational writes.
Hosted writes register the referenced parent table/range as a 2PC participant
for enforced immediate single-column `_id` FKs and send an explicit
parent-existence validation instruction to that participant. Hosted restrict
parent deletes register every range of child tables that declare a matching
primary-key FK and send parent-delete validation checks to those child
participants. Child reference creation and restrict parent-delete checks stage
the same internal FK conflict intent, so concurrent prepares on the same parent
reference conflict. Hosted writes that create or update complete primary/unique
tuples on multi-range tables register the corresponding owner participant.
Constraint-driven large-operation cascade/set-null routing and deferrable
constraints use the action-job/participant model described in
[FOREIGN_KEYS.md](FOREIGN_KEYS.md).
Use graph indexes and the join planner for relationship queries rather than
integrity enforcement.

### Public Row Identity

Document-mode clients can continue to use `/tables/{table}/batch` with physical
document keys. Relational clients should use the row-identity endpoints:

```http
POST /tables/{table}/rows:batch
POST /tables/{table}/rows:get
POST /tables/{table}/rows:query
POST /tables/{table}/rows:aggregate
POST /tables/{table}/rows:window
POST /tables/{table}/rows:join
POST /tables/{table}/rows:lateral
```

The public identity for a table with
`primary_key.columns = ["tenant_id", "order_id"]` is the structured typed tuple,
not the physical row key:

```json
{ "primary": { "tenant_id": "t1", "order_id": "o9" } }
```

Rows can also be addressed by a declared unique constraint:

```json
{
  "unique": {
    "name": "orders_external_id_key",
    "values": {
      "tenant_id": "t1",
      "external_id": "ext-9"
    }
  }
}
```

Unique selectors use the same tuple encoder as relational unique constraints and
route to the durable unique-owner range before reading the owner row. They are
point lookups, not query scans. Missing unique selectors return `found: false`
from `rows:get`; `update` and `delete` fail rather than silently becoming
no-ops.

`rows:batch` accepts row operations that compile to the existing batch and 2PC
machinery:

```json
{
  "operations": [
    {
      "op": "insert",
      "row": {
        "tenant_id": "t1",
        "order_id": "o9",
        "status": "open"
      }
    },
    {
      "op": "update",
      "where": { "primary": { "tenant_id": "t1", "order_id": "o9" } },
      "patch": { "status": "paid" }
    },
    {
      "op": "delete",
      "where": { "primary": { "tenant_id": "t1", "order_id": "o9" } }
    }
  ]
}
```

`insert` adds an optimistic non-existence predicate for the derived row identity;
`upsert` overwrites or creates; `update` is a non-upsert transform and cannot
patch primary-key components. Primary-key changes are modeled as parent-key
updates through the storage/constraint layer, not as silent mutation of the
public row identity. `insert` and `upsert` remain primary-key based; explicit
`on_conflict` targets are separate row-operation metadata over primary or unique
owner rows, so a SQL DSL can compile `ON CONFLICT (unique_col...) DO UPDATE`
without changing primary-key `upsert` semantics. `rows:get` accepts an array of
primary or unique selectors and returns the structured identity, row JSON,
version, and optional `physical_key`.

The read-plan endpoints expose the same typed contracts used by the SQL
adapter and storage runtime. They accept JSON envelopes rather than SQL text:

- `rows:query` accepts `{ "ctes": [...], "query": { ... } }` and returns
  `{ "total": n, "rows": [...] }`.
- `rows:aggregate` accepts `{ "ctes": [...], "aggregate": { ... } }` and
  returns `{ "total_groups": n, "rows": [...] }`.
- `rows:window` accepts `{ "ctes": [...], "window": { ... } }` and returns
  `{ "total_rows": n, "rows": [...] }`.
- `rows:join` accepts `{ "ctes": [...], "join": { ... } }` and returns
  `{ "total_rows": n, "rows": [...] }`.
- `rows:lateral` accepts `{ "ctes": [...], "lateral": { ... } }` and returns
  `{ "total_rows": n, "rows": [...] }`.

`ctes` is optional and ordered. CTEs are named row-query subplans; later CTEs
and the final query/aggregate/window/join/lateral stage can reference a prior
CTE with `source_cte`. The public endpoint layer executes the local single-store
plan path today and fails closed when the active read source cannot provide a
row-plan executor. Hosted/routed production execution must use the same typed
request/response envelopes, but add coordinator fanout, output schemas,
bounded materialization, and merge/spill policy rather than changing the public
contract. If an authenticated request has an effective row filter, supported
conjunctive filters are pushed into every base row source before projection,
aggregation, joining, lateral correlation, CTE materialization, or windowing.
Supported pushdown covers `match_all`, `conjuncts`, `bool.must`,
`bool.filter`, scalar `term`/`terms`, `array_any`, `json_contains`,
`numeric_range`, and `date_range` over declared relational columns. Filters
that require a broader boolean or text-search execution model fail closed for
these row-plan endpoints so totals and derived rows cannot bypass row-level
authorization.

Row queries also accept `expression_where`, an array of typed expression
conditions using the same expression AST as projections, ordering, aggregate
filters, window ordering, and `RETURNING`. The executor treats these conditions
as residual predicates over hydrated committed row images unless the planner can
derive a safe primary, unique, generated-column, secondary, array, JSON, or
embedded-JSON pushdown from the expression. This keeps REST/SDK structs as the
source of truth while SQL syntax such as `WHERE lower(email) = $1` lowers either
to a generated-column predicate when one exists or to an expression predicate
that evaluates correctly without requiring a generated column. Numeric
arithmetic conditions such as `WHERE amount * quantity - discount > $1`,
`COALESCE` predicates such as `WHERE coalesce(status, 'pending') = $1`, and
array cardinality predicates such as `WHERE array_length(tags, 1) > $1` lower to
the same `expression_where` contract and evaluate through the shared expression
executor.

The physical key is an implementation detail derived from the canonical typed
primary-key tuple. It exists for placement, WAL, row-version ownership, and
debugging, but relational clients should not persist it as their row address.
This shape is intentionally SQL-compatible: `INSERT`, `UPDATE ... WHERE` full
primary-key or unique-key equality, `DELETE ... WHERE` full primary-key or
unique-key equality, and `REFERENCES parent(col_a, col_b)` can compile directly
to these structured API operations.

Foreign-key metadata lives in `TableSchema.foreign_keys`; unique metadata lives
in `TableSchema.unique_constraints`. Both compile into the runtime schema,
persist with the runtime schema, and can be added or dropped by constraint name
across ordinary same-table schema updates when the base relational column
catalog is unchanged. Additions synchronously validate existing rows and install
the required integrity rows before the runtime schema is persisted when the FK
is `enforced`; `unvalidated` FK additions are catalog-only and can later be
applied as the same definition with `validation_state: "enforced"` to run the
validation/build before the catalog flip. Drops remove the old backing rows.
Same-name semantic changes are rejected. FK catalog rows also persist `timing`
and `validation_state`; `immediate` timing plus `enforced` or `unvalidated`
validation states are accepted through public schema validation today, while
`deferred`, `validating`, and `invalid` remain reserved for the distributed
constraint planner.
The relational write participant enforces parent existence on child
insert/update against the participant's final planned state before durable
commit, maintains reverse-reference rows with child rows, rejects parent deletes
with live restrict references, rewrites nullable child references for `set_null`
parent deletes, recursively prepares child deletes for local cascade parent
deletes, maintains committed unique rows for present unique values, rejects
duplicate unique values, and applies those same rules while resolving committed
transaction intents. The DB can also explain a single FK parent delete through
that same participant path without applying the planned writes, reporting
restrict blocks plus planned set-null/cascade effects for local tables. See
[FOREIGN_KEYS.md](FOREIGN_KEYS.md).
Hosted transaction planning additionally registers table-resolved primary-key
FK parent participants and includes FK parent checks in prepare so missing
parents fail on the referenced table/range participant. It also registers
child-table range participants for restrict primary-key parent deletes so live
or staged child references fail prepare on child participants, and the shared
FK conflict intent prevents concurrent child-reference and parent-delete
prepares for the same parent key from both committing. Hosted distributed
planning fails before prepare for enforced non-primary FK child writes and for
enforced parent deletes that would require distributed `set_null`, `cascade`, or
non-primary target validation until those routed planners exist.
The foreign-key integrity endpoint can validate, dry-run, repair, or list either
the full FK catalog or a single named constraint through `constraint_name`; hosted
execution forwards the same scope to each resolved group. The same endpoint can
also run `action: "explain_delete"` with a required `doc_key` to route a
non-mutating parent-delete plan to the target key's owning group.

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
nullable, indexed}`), populated by `deriveRuntimeTableSchema` and round-tripped
through the versioned binary format. `NOT NULL` is enforced upstream by
JSON-schema `required` validation, and `projectRelationalRowAlloc` enforces
strict physical types before the row is serialized.

### Query path

The relational win is predicate pushdown. The planner uses a **columnar table
scan** operator over the relational base store for predicates on declared
columns, instead of routing every scalar filter through the full-text index.
Projection and stored-document reads reconstruct JSON from the committed packed
row. Full-text segments supply only text matching, scoring, doc identity, and
index-local metadata; they are not a second relational column store. Joins and
`GROUP BY`-over-join are unchanged — they already exist (see `JOINS.md`,
`ALGEBRAIC.md`).

### SQL adapter boundary

Relational mode is the storage and API substrate that a SQL DSL can target. It
is not itself a PostgreSQL wire server, SQL parser, migration runner, or PL/SQL
runtime. PostgreSQL-specific surface work such as `pgx` protocol behavior,
migration-file replay, extensions, triggers, PL/pgSQL functions, and exact
PostgreSQL DDL syntax belongs in an adapter layer above the Antfly model.

The useful split is:

- **Adapter-level PostgreSQL surface:** wire protocol, SQL text parsing,
  PostgreSQL catalog views, migration-file parsing/replay behavior, extension
  declarations, PostgreSQL-specific DDL syntax, PL/pgSQL, trigger/function
  parsing, and exact error-code/message mapping. These sit above the Antfly API
  and do not change the relational storage model.
- **Model-level SQL semantics:** the durable row, index, constraint, mutation,
  query, schema-evolution, validation, rebuild, and backfill contracts that
  multiple frontends can target. These belong in relational mode itself because
  the engine must enforce them consistently for SQL, REST, SDK, and internal
  callers.

The migration boundary follows the same split. The storage engine does not need
byte-for-byte replay of PostgreSQL migration files. The engine needs to produce
equivalent catalog state, derived-index state,
constraint state, server-update policies, and data rewrites through
Antfly-native typed plans. Exact migration-file replay can live in the adapter
by parsing those files and lowering supported statements into the same native
schema, expression, rewrite,
validation, and rebuild operations.

The long-term layering is:

1. **Antfly-native model first.**
   Define durable typed schemas, typed schema-evolution operations, expression
   trees, row selectors, row mutations, query plans, validation jobs, rebuild
   jobs, backfill/rewrite jobs, range ownership, row-claim semantics, and 2PC
   participant registration. This is the internal contract and the public
   REST/SDK contract.
2. **Compatibility adapters second.**
   A PostgreSQL-facing adapter parses SQL/DDL, resolves names and parameter
   types, normalizes PostgreSQL-specific syntax, and lowers supported shapes
   into Antfly-native plans. Unsupported syntax fails closed with stable
   unsupported-shape errors. PostgreSQL catalog views, SQLSTATEs, command tags,
   `pgx` result labels, extension declarations, dump boilerplate, and exact
   migration-file replay remain adapter behavior.
3. **No backend SQL strings.**
   Storage, repair, rebuild, constraint enforcement, query execution,
   simulation, and migration-equivalence tests consume Antfly-owned structs.
   PostgreSQL syntax is one frontend dialect, not the durable representation of
   relational behavior.

The model-level contracts for a PostgreSQL-shaped SQL surface are below. They are
implemented as explicit API/runtime contracts first, then mapped from SQL syntax
by an adapter.

#### PostgreSQL-shaped SQL completion plan

PostgreSQL-shaped SQL should be planned as Antfly relational primitives, not as
raw SQL strings flowing through the backend. PostgreSQL syntax is the adapter
input. The backend contract is a typed plan over schemas, expressions, row
selectors, mutations, indexes, joins, aggregates, windows, repair work, and
range ownership. Direct REST/SDK callers should be able to construct the same
plans without speaking SQL.

Given the composite identity, durable unique-owner, foreign-key ownership,
schema-controller repair/promotion, secondary-index lifecycle, embedded
JSON-column indexing, and 2PC participant work in this PR, the remaining
PostgreSQL-shaped SQL surface should land in these tracks:

| Track | Current model shape | Long-term production plan |
| --- | --- | --- |
| Partial, expression, and partial-unique indexes | Ordinary secondary indexes carry lifecycle state and rebuild generations; simple `lower(field)` expression indexes lower through generated-column metadata; partial predicates exist on secondary indexes and unique constraints; queries use only `ready` indexes and writes enforce only promoted unique constraints. | Move all partial predicates and expression keys onto one typed expression AST. Use one implication checker for query pushdown, uniqueness enforcement, and `ON CONFLICT ... WHERE`. Route expression-derived rebuilds through the same generation-aware catalog work, range repair, all-range readiness gate, and schema compare-and-swap promotion used by ordinary indexes. |
| Raw DML syntax: `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT`, `RETURNING` | Point row-batch plans cover primary/unique selectors, `ON CONFLICT DO NOTHING/UPDATE`, `excluded.column`, numeric conflict deltas such as `SET amount = amount + excluded.amount`, source-qualified typed `increment_expr` deltas over existing/proposed rows such as `SET amount = amount + COALESCE(excluded.amount, 0)`, and source-qualified typed `patch_expr` assignments over existing/proposed rows, literals, casts, `LOWER`, `UPPER`, `COALESCE`, `NULLIF`, searched `CASE`, `array_length`, and numeric arithmetic. Conflict-action `WHERE` predicates lower to native `where_expression` conditions over the same existing/proposed row sources; false predicates skip the update and emit no `RETURNING` row. Arithmetic updates, JSONB/array transforms, defaults, `NOW()`, table-owned updated-at policies, field `RETURNING`, and committed-row `returning_expressions` over the shared row-expression AST also lower to typed row-batch plans. Transaction-aware `mutation_source` plans update/delete rows selected from a lockable typed row query, require a `FOR UPDATE` row claim, stage through the claiming transaction, preserve the same typed source-query surface as reads including scalar OR/NOT, computed `expression_where`, `expression_any`, `expression_not`, computed-array containment, and row-claim metadata, reject CTE and physical doc-key range sources at the typed boundary, add committed-version predicates from the selected preimages, materialize `patch_expr`/`increment_expr` per selected row before staging, apply table-owned update policies and generated columns to that final image, and project field plus expression `RETURNING` output from the same planned image. The public `/tables/{tableName}/rows:mutation-source` endpoint, OpenAPI contract, and SDK models expose that typed mutation-source plan directly; PostgreSQL-adapter lowering covers bounded `UPDATE ... WHERE ... [ORDER BY] [LIMIT] [FOR UPDATE SKIP LOCKED] RETURNING ...` and `DELETE ... WHERE ...` sources. | Resolve every non-primary selector through durable unique-owner rows before prepare, then extend mutation sources to routed multi-range fill and hosted 2PC participant planning. Keep conflict actions, generated columns, server-owned policies, update transforms, and mutation result projection on the shared expression tree so local and routed execution use the same final-image semantics. |
| Query predicates, projection expressions, ordering, and pagination | Typed row-query plans cover scalar predicates, native null-safe distinct predicates, JSONB/array predicates, text-pattern predicates over keyword/text/link columns, OR/NOT groups, generated-column pushdown, null-test ordering, executable typed expression predicates, executable typed expression OR/NOT groups, executable typed expression order keys, `ORDER BY`, `LIMIT`, `OFFSET`, row claims for base-row streams, and API-native expression projections over fields, literals, statement-bound `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, `add`, `sub`, `mul`, `div`, searched `case` branches, typed `cast` nodes, typed JSON path extraction, typed array length, and typed `string_to_array`. SQL `field AS label`, `NOW() AS label`, `COALESCE(...) AS label`, `LOWER(field) AS label`, `UPPER(field) AS label`, `CONCAT(...) AS label`, `NULLIF(...) AS label`, searched `CASE WHEN ... THEN ... ELSE ... END AS label`, `CAST(expr AS text|numeric|bool) AS label`, JSON extraction such as `metadata->'flags' AS flags` and `metadata->>'source' AS label`, `array_length(tags, 1) AS label`, `string_to_array(scope, ' ') AS scope_parts`, and numeric `+`, `-`, `*`, and `/` projections lower into that same row-query expression projection while existing projection encoders continue to emit the public result shape. SQL `WHERE lower(field) = ...` still uses a stored generated column when one exists, otherwise it lowers to a typed expression predicate; `WHERE upper(field) = ...`, `COALESCE`, `array_length`, numeric arithmetic predicates, SQL `IS [NOT] DISTINCT FROM`, SQL `BETWEEN`/`NOT BETWEEN`, SQL `NOT (...)` over computed predicates, and SQL `LIKE`/`ILIKE` over declared text-like columns lower into typed predicates. Scalar `BETWEEN` becomes native `>=` plus `<=` predicates, scalar `NOT BETWEEN` becomes a native OR group of `< low` or `> high`, computed expression `BETWEEN` becomes two expression predicates over the same cloned left-hand typed expression, computed expression `NOT BETWEEN` becomes an `expression_any` OR group of `< low` or `> high` branches, and `NOT (computed expression conditions...)` becomes an `expression_not` group. `ORDER BY lower(field)` follows the same pushdown/residual split; `ORDER BY upper(field)`, `ORDER BY COALESCE(...)`, `ORDER BY array_length(...)`, and arithmetic order expressions lower to typed expression order keys. | Replace remaining shape-specific predicate/projection/order cases with one bound scalar expression tree. Planner pushdown is derived from that tree only when it maps to primary, unique, generated, column-major, text-pattern, array, JSON, or embedded-JSON access paths; otherwise evaluation happens over hydrated bounded streams or fails closed. |
| JSONB and array operators | Declared `json` and `array` columns lower to typed path predicates, structured/text extraction with `->`/`->>`, containment/existence, `jsonb_set`, JSON construction/concat, array membership/equality/containment, and append/remove/add-to-set transforms. | Treat SQL operators such as `->`, `->>`, `@>`, `jsonb_set`, `ANY`, `array_length`, and array containment as adapter sugar over typed JSON/array nodes. Indexed JSON-column schemas and dynamic-template changes schedule explicit embedded document-index rebuild work rather than bypassing the relational row model. |
| `FOR UPDATE`, `FOR UPDATE SKIP LOCKED`, and queues | Row-claim metadata exists for lockable base-row streams, and local mutation sources stage claimed update/delete intents with OCC predicates, per-row expression transforms, server-owned update policies, generated-column repair, and final-image `RETURNING` projections in the claiming transaction. | Add ordered multi-range fill, durable claim ownership beyond local transaction intents, lease/retry semantics, hosted participant registration, and range-movement chaos coverage. Keep claims illegal over joins, aggregates, windows, and materialized CTEs until those stages expose a lockable base-row source. Gate queue workloads with chaos tests that move ranges while writers and claimers run. |
| Joins, CTEs, aggregates, and windows | Local equality joins, non-recursive CTE lowering, scalar aggregate lowering, scalar `FILTER`, executable typed aggregate filter expressions, typed aggregate input expressions for scalar expression keys such as `COUNT(DISTINCT lower(status))`, `COUNT(DISTINCT coalesce(status, 'missing'))`, and numeric expression inputs such as `SUM(amount - discount)`, `SUM(coalesce(amount, 0))`, and `SUM(array_length(tags, 1))`, scalar `DISTINCT`, bounded ordered `array_agg`, `GROUP BY`, `HAVING`, aggregate ordering, aggregate pagination, native aggregate API parsing, native equality join API parsing, native ordered CTE plan parsing, local `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` windows with native window API parsing, and bounded local `LEFT JOIN LATERAL` stages with native lateral API parsing have typed plan homes. | Add routed cross-table/cross-range stream execution, CTE output-schema tracking, bounded materialization with spill/fail policy, spill-backed distinct aggregate state, broader JSON/array/complex aggregate filters and distinct keys over the shared expression tree, routed/cross-range lateral execution, richer window frame semantics and additional frame-aware functions, and routed/spill-safe window execution. |
| SQL compatibility evidence | Supported SQL fails closed into typed DDL, row-query, mutation, aggregate, join, CTE, and expression plans; unsupported syntax does not pass through storage. | Harvest a representative SQL and migration-equivalence corpus, bind it against Antfly catalog snapshots, record golden typed plans or intentional unsupported classifications, run representative row/identity/constraint/queue/JSON/usage flows, and add chaos/sim workloads that combine live writes, FK actions, unique-owner repair, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and native schema/rewrite/rebuild jobs. |

This means the important missing work is model-level, even when the visible
syntax is PostgreSQL: partial/expression index completeness, routed multi-range
DML, routed joins, CTE materialization, aggregate/window spill and routing,
durable cross-range queue ownership, and representative SQL corpus gates.
Adapter-only compatibility
such as `pgx` protocol behavior, SQLSTATE text, catalog views, extensions,
dump boilerplate, and PL/pgSQL syntax should remain above this layer.

#### Partial and expression indexes

Secondary indexes and unique constraints need optional predicates plus
expression keys:

```json
{
  "document_schemas": {
    "row": {
      "schema": {
        "type": "object",
        "properties": {
          "email": {
            "type": "keyword",
            "x-antfly-index-where": {
              "all": [
                { "field": "status", "op": "eq", "value": "active" }
              ]
            }
          },
          "email_lc": {
            "type": "keyword",
            "generated": { "op": "lower", "field": "email" }
          }
        }
      }
    }
  },
  "unique_constraints": [
    {
      "name": "users_active_slug_key",
      "columns": ["tenant_id", "slug"],
      "where": {
        "all": [
          { "field": "status", "op": "eq", "value": "active" }
        ]
      }
    },
    {
      "name": "users_lower_email_key",
      "columns": ["tenant_id"],
      "expressions": [
        { "op": "lower", "field": "email" }
      ]
    }
  ]
}
```

Partial unique constraints are required for common SQL patterns such as "one
active row" and "unique when nullable value is present". Expression indexes are
required for normalized lookup keys such as `lower(email)` and for computed
ordering/filtering terms. SQL text is parsed above this layer; the backend
catalog stores typed Antfly AST metadata such as `{ "op": "lower",
"field": "email" }` and predicate atoms such as `{ "field": "status", "op":
"eq", "value": "active" }`.

Partial secondary indexes are a per-column catalog property,
`x-antfly-index-where`. Writes evaluate the predicate against the same
committed packed row that supplies the column values. Matching rows receive the
ordinary column-major, array-element, or JSON-value side rows; non-matching rows
keep their authoritative base-row cells but do not receive secondary scan
entries. Rebuild, split, merge, and repair code use the same runtime column
catalog, so partial side rows are deterministic derived state. Query planning
uses a partial secondary index only when the typed row-query predicates imply
the index predicate, otherwise it falls back to authoritative base-row scans and
final rechecks. The predicate grammar is the same simple typed `all` form used
by partial unique constraints: `is_null`, `is_not_null`, `eq`, and `ne` atoms
over declared relational columns.

Unique expression constraints store the typed expression directly on the unique
constraint, evaluate the expression from the committed row, and maintain the
unique-owner row in the same 2PC path as ordinary column unique owners.
Non-unique expression access paths use stored generated columns as the canonical
model-level representation. For example, a SQL adapter lowers
`CREATE INDEX users_lower_email_idx ON users (lower(email))` into a generated
relational column such as `"email_lc": { "type": "keyword", "generated": {
"op": "lower", "field": "email" } }`; because generated columns are ordinary
packed-row columns, their column-major side rows are rebuilt, moved, repaired,
and queried through the same path as hand-authored indexed columns. Predicate
queries then target `email_lc = "ada@example.test"` in the typed request rather
than carrying the SQL expression string through storage. Schema validation
checks each expression, dependency tracking records the base columns it reads,
writes evaluate expression values from the committed row, and
index/unique-owner maintenance uses those computed values in the same 2PC path
as ordinary column indexes.
Foreign keys must not target partial or expression unique constraints because
they do not prove total parent uniqueness over a declared column tuple.

#### Conflict-target upsert

`ON CONFLICT (cols...) [WHERE predicate] DO UPDATE/NOTHING` should lower to an
explicit conflict-target operation over a primary or unique owner row:

```json
{
  "op": "insert",
  "row": { "tenant_id": "t1", "email": "a@example.com", "name": "Ann" },
  "on_conflict": {
    "target": {
      "unique": { "name": "users_tenant_email_key" }
    },
    "action": "update",
    "patch": { "name": "Ann" }
  }
}
```

This is distinct from the current primary-key based `upsert`, which always
addresses row identity. Conflict-target upsert first resolves the owner row for
the named primary/unique target, then applies `DO NOTHING` or a guarded
read-modify-write update to that resolved row. The backend does not accept SQL
fragments here: `{ "target": { "primary": true } }` means the inserted row's
primary-key tuple, and `{ "target": { "unique": { "name": "..." } } }` means
the named unique constraint's tuple inferred from the inserted row. That
inference supports ordinary columns, typed expressions such as `lower(email)`,
and typed partial predicates without copying SQL text into storage metadata. The
conflict target and partial-index predicate must be part of the durable plan so
concurrent inserts for the same unique value conflict on the same owner record.
Conflict update assignments compile into the same typed mutation operations as
ordinary updates: literal/default patches, numeric increments, source-qualified
`patch_expr` assignments, source-qualified `increment_expr` deltas, JSON path
sets, JSON object concat/build patches, and array append/remove/add-to-set
transforms.
The public REST/SDK contract exposes these as named typed structs, not
anonymous adapter blobs: `RowsFieldPatch`, `RowsNumericIncrement`,
`RowsExpressionAssignmentMap`, `RowsJsonSetTransform`,
`RowsArrayUpdateTransform`, `RowsConflictTarget`, `RowsConflictUniqueTarget`,
`RowsUniquePredicateGroup`, and `RowsOnConflict`. Ordinary row-batch
`RowOperation` and transaction-claimed `RowsMutationSourceRequest` share those
same update operator schemas, while `RowOperation.on_conflict` adds the conflict
target, action, and optional `where_expression` guard. That keeps SQL lowering
and direct REST/SDK writes on one typed mutation-plan surface.
`excluded.column` is resolved from the typed proposed row before prepare.
Value expressions such as
`SET status = COALESCE(excluded.next_status, status, 'fallback')`,
`SET status = CASE WHEN excluded.amount > amount THEN 'bumped' ELSE status END`,
and `SET status = LOWER(excluded.next_status)` compile to typed `patch_expr`
operations.
Numeric deltas such as `SET amount = amount + excluded.amount` and
`SET amount = amount + COALESCE(excluded.amount, 0)` compile to typed increment
operations. These conflict expressions lower through the native row-expression
AST as `patch_expr` or `increment_expr`, where `{ "source": "proposed" }` names
the inserted row and `{ "source": "existing" }` names the resolved conflict row.
Assignment expressions are bound against the target column type, while condition
subexpressions can compare any valid existing/proposed fields. Ordinary query
expressions still use unqualified row fields.
SQL conflict action predicates such as
`DO UPDATE SET status = excluded.next_status WHERE excluded.amount > amount`
compile to a native `where_expression` condition on `on_conflict`. This is
separate from partial unique target predicates: target predicates choose the
unique owner row, while action predicates decide whether the resolved conflict
row is transformed. A false action predicate appends no transform, no version
predicate, and no `RETURNING` row.
`SET field = DEFAULT` resolves through the field's declared Antfly default
instead of storing a SQL expression.
For a partial unique constraint, the row API requires the conflict target to
repeat the catalog predicate in typed form:

```json
{
  "target": {
    "unique": {
      "name": "users_active_email_key",
      "where": {
        "all": [
          { "field": "email", "op": "is_not_null" },
          { "field": "status", "op": "eq", "value": "active" }
        ]
      }
    }
  }
}
```

If the supplied predicate is missing or does not match the catalog predicate,
the request is rejected before owner lookup. Rows that do not satisfy the
partial predicate do not resolve a unique owner and follow the insert path.

#### Mutation result projection

`INSERT/UPDATE/DELETE ... RETURNING` should be represented as a mutation output
projection:

```json
{
  "op": "update",
  "where": { "primary": { "tenant_id": "t1", "id": "u1" } },
  "patch": { "status": "disabled" },
  "returning": ["tenant_id", "id", "status", "updated_at"],
  "returning_expressions": [
    {
      "as": "status_key",
      "expr": {
        "op": "concat",
        "args": [{ "field": "tenant_id" }, { "value": ":" }, { "field": "status" }]
      }
    }
  ]
}
```

The row API exposes this as typed `returning` fields and
`returning_expressions` on each mutation. Field returning covers
`{ "returning": ["tenant_id", "id", "status"] }` and `{ "returning": ["*"] }`.
Expression returning uses the same row-expression AST as query projections and
currently supports field/literal, statement-bound `now`, `coalesce`, `lower`,
`upper`, `concat`, `nullif`, numeric arithmetic, searched `case`, typed `cast`,
JSON path extraction, and array length nodes. PostgreSQL `RETURNING field`,
aliases, `NOW() AS label`, `LOWER(...)`,
`UPPER(...)`,
`CONCAT(...)`, `COALESCE(...)`, `NULLIF(...)`, searched `CASE`, `CAST(...)`,
JSON extraction, array length, and numeric arithmetic projections lower into
those typed row-batch fields/expressions. The same `returning` plus
`returning_expressions` shape is accepted on mutation-source update/delete plans
so local and routed execution can preserve projected result contracts without
carrying SQL text. Source-update plans evaluate `patch_expr` and
`increment_expr` once per selected row, apply table-owned update policies and
generated-column updates, and then evaluate `returning_expressions` from that
planned post-image. The result is emitted as a `returning` array on the batch or
mutation-source response. Inserts project the proposed row image.
Updates resolve the base row, apply the same transform logic used by storage,
and project the post-image. Deletes project the resolved pre-delete row image.
Update/delete returning adds a version predicate for the row image that was
projected, so the commit either installs/removes the value represented in the
response or fails with an OCC conflict. The projection is deliberately based on
row JSON/relational row state captured for the mutation, not a derived index
read after commit.

#### Row claiming and lock semantics

`FOR UPDATE` and `FOR UPDATE SKIP LOCKED` need a first-class row-claim contract
for queue and ledger workloads:

```json
{
  "query": { "table": "jobs", "where": { "status": "ready" } },
  "claim": {
    "mode": "for_update",
    "skip_locked": true,
    "lease_ms": 30000,
    "owner_id": "session:7",
    "transaction_id": "00112233445566778899aabbccddeeff"
  },
  "order_by": [{ "field": "created_at", "direction": "asc" }],
  "limit": 100
}
```

Claims are transaction-bound. The query/API contract carries `mode`,
`skip_locked`, `lease_ms`, `owner_id`, and a 16-byte transaction identity exposed
as `transaction_id` hex at the API boundary; `txn_id` is accepted as an adapter
alias. `mode` is currently `for_update`, `skip_locked` defaults to `false`,
`lease_ms` defaults to `30000`, and `transaction_id` must be 32 hexadecimal
characters. Storage lowers each claimed base row to a metadata key under
`txn_row_claim:<row-key>` and writes a pending 2PC delete intent for that key.
The intent is the lock: competing claimers and ordinary writers both hit the
same transaction-manager conflict check. Ordinary batch and transaction
mutations add an expected-absent predicate on the row claim key, so a pending
claim blocks direct writes, transactional writes, updates, and deletes until the
claiming transaction commits or aborts. Transaction resolution always consumes
row-claim intent keys instead of applying them as user data, so a commit
releases the lock rather than leaving a durable row-claim record behind.

The typed relational row-query request carries this as `row_claim` beside
`where`, `order_by`, `doc_key_range`, `limit`, and `offset`. The optional
`doc_key_range` is a start-inclusive/end-exclusive physical row-key span with
`start`, `end`, or both; an empty range is rejected, and a bounded range must
have `start < end`. The row-query executor clips both base-row scans and
index-derived candidates to that span before ordering and claiming, so a
distributed planner can execute the same typed query independently on each
table/range owner. The local coordinator contract accepts multiple
non-overlapping owner spans, merges the hydrated
candidate streams into one globally ordered row stream, and applies `OFFSET`,
`LIMIT`, projection, and row claiming once after that merge. `skip_locked:
false` claims all returned base rows or fails with the underlying intent
conflict. `skip_locked: true` attempts claims in result order and keeps scanning
past locked candidates until the requested limit is filled or the merged stream
is exhausted. Row claiming is deliberately base-row only: aggregate sources,
join sides, `count_only`, graph result sets, and chunk return modes are rejected
because they do not identify a single relational row to lock. SQL `SELECT ...
FOR UPDATE [SKIP LOCKED]` and API queue consumers should both compile to this
same row-claim contract. In a distributed deployment, remote owner-range routing
and paged cursor fetch are coordinator mechanics layered on this contract so a
coordinator can request more rows from later owners without over-fetching a whole
table.

#### Array and multivalue columns

Array values need canonical physical encoding, equality/containment semantics,
`ANY`-style predicates, and index support where declared:

```json
{
  "columns": {
    "tags": { "type": "array", "items": { "type": "keyword" }, "indexed": true }
  }
}
```

The engine distinguishes declared arrays from opaque JSON with a first-class
relational `array` column type. The current physical row codec stores the array
as canonical bytes and rejects scalar values for that column. Runtime schema
columns persist `array_item_type` when the JSON schema declares `items`, and row
projection rejects elements that do not match that declared type. That gives
schema/catalog state, reconstruction, and write validation a stable
`array<T>` contract instead of opaque mixed JSON.

`ANY`-style and containment predicates lower to structured typed filter shapes:

```json
{
  "all": [
    { "field": "tags", "op": "array_any", "value": "hot" },
    { "field": "tags", "op": "array_contains", "value": ["hot", "new"] },
    { "field": "tags", "op": "array_eq", "value": ["hot", "new"] }
  ]
}
```

The predicates compare requested values as typed JSON values, not as SQL string
fragments. `array_any` matches when any element equals the requested value.
`array_contains` requires an array operand and matches when every requested
element is present; an empty requested array matches present array values.
`array_eq` requires an array operand and performs exact ordered JSON array
equality, so `["hot", "new"]` is distinct from `["new", "hot"]`.
Document-pattern filters also support nested multivalue paths such as
`{ "array_any": { "path": "items.sku", "value": "sku-2" } }`.

Declared indexed array columns write value-keyed element and whole-array
equality entries beside the ordinary column scan entry:

```text
array_element_index(column_path, canonical_element_value, doc_key) -> ""
array_value_index(column_path, canonical_array_value, doc_key) -> ""
```

`array_any` resolves through that element index when the column is indexed.
`array_contains` uses one requested element as the indexed candidate source, then
hydrates the base row to prove the row still contains every requested element
before building the final result stream. Empty containment operands fall back to
the base-row scan path because there is no selective element key. `array_eq`
uses the whole-array value index as its candidate stream when available, then
hydrates the base row and rechecks exact array equality before projection.
Columns declared with `x-antfly-index: false` keep the same typed array
validation and canonical row storage but resolve array predicates by scanning
base rows instead of writing side-index entries. Row-query planning ranks array
candidate sets by estimated cardinality alongside scalar and JSON candidate sets
before intersecting them. Broader SQL operator sugar, such as dialect-specific
null treatment and nested multivalue path index selection, belongs above or
beside this typed predicate contract. Fully schemaless arrays can still live
inside a `json` column.

Array mutations use the same typed row-batch transform surface as scalar
increments and JSON patching:

```json
{
  "op": "update",
  "where": { "primary": { "tenant_id": "t1", "id": "u1" } },
  "array_update": [
    { "field": "tags", "op": "append", "value": "hot" },
    { "field": "tags", "op": "remove", "value": "stale" },
    { "field": "tags", "op": "add_to_set", "value": "new" }
  ],
  "returning": ["tags"]
}
```

The field must be a declared relational `array` column and cannot be part of the
primary key. `append` lowers to storage `$push` and preserves duplicates,
`remove` lowers to `$pull` and removes matching values, and `add_to_set` lowers
to `$addToSet` and appends only when the value is not already present. SQL
`array_append(col, value)` and `array_remove(col, value)` compile to this typed
contract for literal and parameter values. Element values are carried as JSON
values, not SQL fragments; richer expression operands such as
`array_append(tags, excluded.tag)` should be added through the general typed
expression tree once array element typing is first-class in expression metadata.

#### JSON path and update operators

JSON columns already reuse document-style indexing, but SQL-shaped operators
need stable API and planner semantics over the stored JSON subtree:

```json
{
  "op": "update",
  "where": { "primary": { "tenant_id": "t1", "id": "u1" } },
  "json_set": [
    { "field": "attrs", "path": ["billing", "plan"], "value": "pro" }
  ]
}
```

Extraction (`->`, `->>`), structured/text path equality, containment (`@>`),
existence, and path predicates compile to a JSON-column expression contract, not
SQL text. Patch/update operations such as
`jsonb_set` lower to the row API's typed `json_set` operation:
`{ "field": "attrs", "path": ["billing", "plan"], "value": "pro" }`. The field
must be a declared `json` relational column, path segments are structured
strings rather than SQL text, and the mutation lowers to the same storage
transform path as ordinary row updates. That rewrites only the JSON cell, then
reprojects derived full-text/path-fact/algebraic indexes for that subtree from
the committed row. Changing a JSON column's embedded schema remains a
derived-index rebuild, not a base-row migration.

Path equality and existence filters over a declared JSON column lower to typed
row-query atoms with structured path segments:

```json
{
  "all": [
    {
      "field": "attrs",
      "op": "json_path_eq",
      "path": ["billing", "plan"],
      "value": "pro"
    },
    {
      "field": "attrs",
      "op": "json_path_exists",
      "path": ["flags"]
    }
  ]
}
```

The public JSON API may also accept a dot path such as `"billing.plan"` when a
path segment itself does not contain a dot. SQL `attrs ->> 'plan' = 'pro'` and
`attrs -> 'flags' = $json` lower to `json_path_eq`; SQL existence checks lower
to `json_path_exists`. Object containment (`@>`) lowers to a typed containment
predicate such as:

```json
{
  "json_contains": {
    "field": "attrs",
    "value": { "billing": { "plan": "pro" }, "flags": ["active"] }
  }
}
```

Projection/extraction uses the shared row-expression AST so SQL `->` and `->>`
do not pass through the backend as SQL strings:

```json
{
  "select": ["id"],
  "expressions": [
    {
      "as": "plan",
      "expr": {
        "op": "json_extract",
        "args": [{ "field": "attrs" }],
        "path": ["billing", "plan"],
        "as_text": true
      }
    },
    {
      "as": "flags",
      "expr": {
        "op": "json_extract",
        "args": [{ "field": "attrs" }],
        "path": ["flags"]
      }
    }
  ]
}
```

`as_text: false` is the `->` shape and projects the selected JSON value.
`as_text: true` is the `->>` shape and projects a JSON string containing the
selected scalar or canonical JSON text. Missing paths and JSON null project as
`null`. The older `json_extract` projection list is still accepted as a compact
API shape, but new SQL lowering and SDKs should prefer `expressions`.
Extraction is a row projection over the committed materialized row image; it
does not read derived JSON value indexes.

Containment is recursive over actual JSON values: objects require every
requested key to be present and contained, arrays use unordered element
containment, and scalars use JSON value equality with numeric integer/float
normalization. It is intentionally not lowered to a fake term filter.

Declared indexed JSON columns write value-keyed leaf rows and path-existence
rows beside the ordinary column scan entry:

```text
json_value_index(column_path, relative_json_path, canonical_leaf_value, doc_key) -> ""
json_path_index(column_path, relative_json_path, doc_key) -> ""
```

The relational resolver uses those rows as candidate sources for
`json_contains`, scalar `json_path_eq`, and `json_path_exists`, then hydrates
the authoritative base row and rechecks recursive containment, exact path
equality, or path existence before returning a doc set. That makes stale side
rows, partial rebuilds, and intentionally lossy leaf choices safe. The path
index records object, array, and scalar paths, so existence checks are not
limited to scalar leaf values. Containment predicates that do not have an
indexable scalar leaf, object/array path equality, empty object/array
containment, or columns declared with `x-antfly-index: false`, fall back to a
base-row scan with the same evaluator. Row-query planning ranks JSON candidate
sets by estimated cardinality alongside scalar and array candidate sets before
intersecting them. When both relational JSON value/path rows and algebraic
path-fact access paths can serve the same predicate, the planner can choose
between them without changing the public typed JSON predicate contract.

#### Checks, defaults, and generated values

Required schema fields cover `NOT NULL`, but ordinary relational schemas also
need table-owned `CHECK` predicates, server-side defaults, and generated values:

```json
{
  "checks": [
    { "name": "orders_amount_nonnegative", "field": "amount", "op": "gte", "value": 0 }
  ],
  "document_schemas": {
    "row": {
      "schema": {
        "type": "object",
        "properties": {
          "id": { "type": "keyword" },
          "request_id": {
            "type": "keyword",
            "x-antfly-default": { "op": "uuid_v4" }
          },
          "email": { "type": "keyword" },
          "email_key": {
            "type": "keyword",
            "generated": { "op": "lower", "field": "email" }
          },
          "status": { "type": "keyword", "default": "active" },
          "created_at_ns": {
            "type": "numeric",
            "x-antfly-default": { "op": "now_ns" }
          }
        },
        "required": ["id", "email"],
        "additionalProperties": false
      }
    }
  }
}
```

The model-level contract is typed JSON metadata, not SQL expression text. Literal
column defaults use JSON Schema `default`. Server-owned defaults use
`x-antfly-default` so object literals remain unambiguous. The durable default
operations are:

- `{"op":"uuid_v4"}` on string-like identity columns (`keyword`, `text`, or
  `link`).
- `{"op":"now_ns"}` on `numeric` or `datetime` columns, stored as epoch
  nanoseconds.

Defaults are materialized once per planned row by the write planner before
primary-key encoding, unique tuple derivation, constraint checks, and
`RETURNING`. A generated column that references a server-defaulted column sees
the same UUID/timestamp value that is committed to the row. SQL adapters lower
`DEFAULT`, `gen_random_uuid()`/UUID defaults, and `now()`/timestamp defaults
into these typed operations instead of passing SQL expression text through
storage. The PostgreSQL adapter lowerer applies the same boundary rule for
explicit `DEFAULT` and `NOW()` expressions in inserts, updates, conflict
updates, and scalar predicates over `numeric`/`datetime` columns: it
materializes the value before producing the row-batch or row-query plan. Omitted
insert fields still use the write planner's default materialization path.

Server-maintained update policies use table-owned metadata instead of SQL
trigger text. The durable update policy field is `x-antfly-on-update`; the
current operation is `{"op":"now_ns"}` on `numeric` or `datetime` columns:

```json
{
  "updated_at_ns": {
    "type": "numeric",
    "x-antfly-on-update": { "op": "now_ns" }
  }
}
```

Row-batch update and conflict-update planning append these server-owned
transform operations after user-authored mutations. That means the table policy
wins consistently for updated-at columns, constraint checks and generated
columns see the final planned image, and `RETURNING` observes the same value
that will be committed. SQL adapters should compile known updated-at trigger
patterns into this metadata during DDL/migration lowering rather than injecting
`SET updated_at = NOW()` snippets into every mutation.

Stored generated columns currently support typed `lower(field)` and
`concat(fields, separator)` forms; user input cannot write generated columns
directly. Inserts materialize defaults and generated values into the committed
row image. Updates and conflict-target updates that touch tables with checks or
generated columns resolve the base row, apply the requested patch/JSON updates,
regenerate stored generated values as ordinary transform operations, and
validate checks against that final planned row image. `CHECK` predicates are
named typed atoms (`is_null`, `is_not_null`, `is_distinct`,
`is_not_distinct`, `eq`, `ne`, `gt`, `gte`, `lt`, `lte`) over declared columns
so REST, SDK, and future SQL adapters compile to the same backend contract.

#### Relational query lowering

Joins, aggregates, `ORDER BY`, `LIMIT`, and `OFFSET` already have engine pieces,
but they need a first-class relational query contract that plans against base
rows and declared indexes before a SQL DSL can expose them as ordinary query
clauses:

```json
{
  "from": "orders",
  "where": { "tenant_id": "t1", "status": "open" },
  "join": [
    {
      "table": "customers",
      "on": [["orders.tenant_id", "customers.tenant_id"],
             ["orders.customer_id", "customers.customer_id"]]
    }
  ],
  "select": ["orders.id", "customers.name"],
  "order_by": [{ "field": "orders.created_at", "direction": "desc" }],
  "limit": 50,
  "offset": 0
}
```

The row API now exposes the single-table form of this as a typed query request,
not as SQL text:

```json
{
  "where": {
    "all": [
      { "field": "tenant_id", "op": "eq", "value": "t1" },
      { "field": "status", "op": "eq", "value": "open" },
      { "field": "tags", "op": "array_any", "value": "hot" },
      { "field": "tags", "op": "array_contains", "value": ["hot", "new"] },
      { "field": "tags", "op": "array_eq", "value": ["hot", "new"] },
      {
        "field": "attrs",
        "op": "json_contains",
        "value": { "billing": { "plan": "pro" } }
      },
      {
        "field": "attrs",
        "op": "json_path_eq",
        "path": ["billing", "plan"],
        "value": "pro"
      },
      {
        "field": "attrs",
        "op": "json_path_exists",
        "path": ["flags"]
      }
    ],
    "any": [
      { "field": "status", "op": "eq", "value": "blocked" },
      {
        "all": [
          { "field": "status", "op": "eq", "value": "open" },
          { "field": "priority", "op": "gt", "value": 50 }
        ]
      }
    ],
    "not": [
      {
        "all": [
          { "field": "status", "op": "eq", "value": "archived" },
          { "field": "deleted_at", "op": "is_not_null" }
        ]
      }
    ]
  },
  "select": ["id", "status", "created_at"],
  "expressions": [
    {
      "as": "plan",
      "expr": {
        "op": "json_extract",
        "args": [{ "field": "attrs" }],
        "path": ["billing", "plan"],
        "as_text": true
      }
    },
    {
      "as": "tag_count",
      "expr": {
        "op": "array_length",
        "args": [{ "field": "tags" }]
      }
    },
    {
      "as": "display_name",
      "expr": {
        "op": "coalesce",
        "args": [
          { "field": "preferred_name" },
          { "field": "email" },
          { "value": "unknown" }
        ]
      }
    },
    {
      "as": "email_key",
      "expr": {
        "op": "lower",
        "args": [{ "field": "email" }]
      }
    },
    {
      "as": "display_label",
      "expr": {
        "op": "concat",
        "args": [
          { "field": "preferred_name" },
          { "value": " <" },
          { "op": "lower", "args": [{ "field": "email" }] },
          { "value": ">" }
        ]
      }
    },
    {
      "as": "usable_email",
      "expr": {
        "op": "nullif",
        "args": [
          { "op": "lower", "args": [{ "field": "email" }] },
          { "value": "blocked@example.test" }
        ]
      }
    },
    {
      "as": "net_amount",
      "expr": {
        "op": "sub",
        "args": [
          {
            "op": "add",
            "args": [
              { "field": "amount" },
              {
                "op": "mul",
                "args": [{ "field": "tax" }, { "field": "rate" }]
              }
            ]
          },
          {
            "op": "div",
            "args": [{ "field": "discount" }, { "field": "divisor" }]
          }
        ]
      }
    },
    {
      "as": "status_label",
      "expr": {
        "op": "case",
        "cases": [
          {
            "when": { "lhs": { "field": "status" }, "op": "eq", "rhs": { "value": "blocked" } },
            "then": { "value": "needs_review" }
          },
          {
            "when": { "lhs": { "field": "email" }, "op": "is_null" },
            "then": { "value": "missing_email" }
          }
        ],
        "else": { "field": "status" }
      }
    },
    {
      "as": "id_text",
      "expr": {
        "op": "cast",
        "to": "text",
        "args": [{ "field": "id" }]
      }
    }
  ],
  "field_aliases": [
    { "as": "raw_id", "field": "id" }
  ],
  "order_by": [
    { "field": "expires_at", "null_test": "is_null" },
    { "field": "created_at", "direction": "desc" }
  ],
  "row_claim": {
    "mode": "for_update",
    "skip_locked": true,
    "owner_id": "session:7",
    "transaction_id": "00112233445566778899aabbccddeeff"
  },
  "doc_key_range": { "start": "tenant:t1/order:", "end": "tenant:t1/order~" },
  "limit": 50,
  "offset": 0
}
```

The same contract also accepts shorthand equality filters such as
`{ "where": { "status": "ready" } }`. Scalar predicates are typed atoms over
declared relational columns (`is_null`, `is_not_null`, `is_distinct`,
`is_not_distinct`, `eq`, `ne`, `gt`, `gte`, `lt`, `lte`). Array and JSON operators are also typed atoms: `array_any`
requires a declared `array` column and compares one typed JSON value against
array elements; `array_contains` requires a declared `array` column and an array
operand whose elements must all be present; `array_eq` requires a declared
`array` column and an array operand for exact ordered equality; `json_contains`
requires a declared `json` column and uses recursive JSON containment semantics;
`json_path_eq` requires a declared `json` column, a structured path, and an
exact JSON value; `json_path_exists` requires a declared `json` column and a
structured path. `is_distinct` and `is_not_distinct` are null-safe equality
predicates. Missing row fields are treated as JSON `null`, so `is_not_distinct`
with a `null` value matches both absent and explicit-null fields, and
`is_distinct` is its logical inverse. The SQL adapter lowers `IS DISTINCT FROM`
and `IS NOT DISTINCT FROM` to these native row-query operators rather than
rewriting them to ordinary `eq`/`ne` predicates. `where.any` carries scalar
disjunction branches: each branch is either one scalar atom or an `all` list of scalar atoms, branches are unioned,
and the row must match at least one branch in addition to any top-level `all`
predicates. Indexed scalar OR branches are planned as independent candidate
sets and unioned before authoritative hydrated-row recheck; unindexable branches
fall back to base-row scanning rather than returning partial results. `where.not`
carries scalar negative branches: each branch is one scalar atom or an `all` list
of scalar atoms, and a hydrated row is rejected when it matches any negative
branch. Negative branches are recheck-only today; they intentionally do not
subtract candidate sets until the general typed boolean expression planner can
prove the subtraction is complete for indexed and unindexed paths.
Projections are field lists or `["*"]` plus `expressions` for the shared
row-expression AST. Compact API projection lists such as `json_extract`,
`array_length`, `coalesce`, and `field_aliases` are still accepted for direct
REST/SDK callers as typed structs: `RowsJsonExtractProjection` requires
`as`, `field`, `path`, and optional `as_text`; `RowsArrayLengthProjection`
requires `as` and `field`; `RowsCoalesceProjection` requires `as` and one or
more `RowsCoalesceOperand` entries, each exactly one of `field` or `value`; and
`RowsFieldAliasProjection` requires `as` and `field`. SQL lowering targets
row-expression projections for field aliases, JSON extraction, array length,
and COALESCE. Public OpenAPI/SDK contracts expose computed nodes through one
shared `RowsExpression` schema plus expression-condition/projection wrappers,
so query predicates, order keys, aggregate inputs/filters, window value
expressions, mutation-source expression assignments, and `RETURNING`
expressions all use the same typed AST shape.
Nested operator operands and searched `case` branches are typed recursive
`RowsExpression` nodes rather than endpoint-local JSON blobs. Public route
handlers still validate each expression against the table schema before
execution. Expression projections currently cover `field`, JSON-literal
`value`, statement-bound `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, `add`, `sub`, `mul`, `div`,
searched `case` branches, typed `cast` nodes, `json_extract`, and
`array_length`, and `string_to_array`; `now` emits the nanosecond timestamp
bound when the typed plan was parsed/lowered, `coalesce` emits the first present
non-null operand, `lower` emits a lowercased string or `null`, `upper` emits an
uppercased string or `null`, and `concat`
emits the concatenated scalar text of each operand while treating null operands
as empty strings. `nullif` emits `null` when both non-null operands are equal,
otherwise it emits the first operand. `add`, `sub`, `mul`, and `div` accept
numeric operands, propagate `null`, and reject non-numeric operands at the
typed-plan boundary. Division by zero is rejected during expression evaluation.
`case` evaluates typed searched branches in order; branch conditions use the
same scalar comparison operators as row predicates (`eq`, `ne`, `gt`, `gte`,
`lt`, `lte`, `is_null`, `is_not_null`, `is_distinct`, `is_not_distinct`) and emit the matching branch expression
or the required `else` expression. `cast` accepts one operand and a typed target
(`text`, `numeric`, or `bool`), propagates `null`, and rejects unsupported
runtime conversions instead of carrying SQL cast text into storage.
`json_extract` accepts one JSON operand, a non-empty `path`, and an optional
`as_text` flag; missing paths and JSON nulls emit `null`, object/array extraction
emits the selected JSON value, and text extraction mirrors `->>` by returning a
string for non-string scalars and structured JSON values.
`array_length` accepts one declared array operand, emits a numeric length,
propagates `null`, and rejects non-array runtime values.
`string_to_array` accepts one text-like expression and one non-empty string
delimiter expression, emits a JSON array of string parts, propagates `null` from
either operand, and rejects non-string runtime values.
`expression_array_contains` is the REST/SDK predicate form for computed arrays:
each item carries an `expr` row-expression plus an array `value`, and hydrated
rows match when the computed array contains every requested value. This is the
typed home for SQL sugar such as
`string_to_array(scope, ' ') @> $json_array`; declared array-column containment
continues to use `where` atoms with `op: "array_contains"` so index pushdown can
stay column-aware.
Exact computed-array equality and inequality use the existing `expression_where`
form with `op: "eq"` or `op: "ne"` and an array-valued `rhs`, which keeps
structured expression comparison on the shared row-expression predicate path.
SQL shapes such as
`field AS label`, `COALESCE(...) AS label`, `LOWER(field) AS label`, `UPPER(field) AS label`, `CONCAT(...) AS label`,
`NULLIF(...) AS label`, searched `CASE WHEN ... THEN ... ELSE ... END AS label`,
`CAST(expr AS text|numeric|bool) AS label`, `metadata->'flags' AS flags`,
`metadata->>'source' AS label`, `array_length(tags, 1) AS label`,
`string_to_array(scope, ' ') AS scope_parts`, numeric
arithmetic projections with `+`, `-`, `*`, and `/`, and `id::text AS id_text`
lower to typed projection nodes rather than carrying SQL cast/function strings
into storage. Ordering is over
declared scalar columns or typed null-test expressions (`null_test: "is_null"` /
`"is_not_null"`), which lets SQL shapes such as `ORDER BY (expires_at IS NULL),
expires_at ASC` compile without synthetic user columns. Null and missing order
keys sort after present scalar values for plain field ordering; null-test order
keys sort by the produced boolean value. The executor projects from the
relational row JSON/codec and applies `OFFSET`/`LIMIT` after filtering and
ordering.
Text-pattern predicates are native row-query predicates, not SQL strings. The
REST/SDK shape is:

```json
{
  "where": {
    "field": "name",
    "op": "text_pattern",
    "pattern": "a%",
    "case_insensitive": true
  }
}
```

`pattern` uses SQL `LIKE` wildcards (`%` for any span, `_` for one byte) with
backslash escapes for literal wildcard bytes. `case_insensitive` gives the
ASCII-insensitive `ILIKE` behavior, and `negated` represents `NOT LIKE` /
`NOT ILIKE`. Without a declared text-pattern access path, the planner evaluates
these predicates over hydrated bounded row streams.
`row_claim` and `doc_key_range` are internal/coordinator fields on the shared
row-query struct, not free-form public query knobs. Their REST/SDK schemas are
typed as `RowsRowClaim` and `RowsDocKeyRange` so internal/coordinator callers
get the same validation contract as the Zig parser. Public row-plan endpoints
reject `row_claim`; lockable public access goes through
`/tables/{table}/rows:mutation-source`, where the source must carry a
transaction-bound `row_claim`. Public endpoints also reject `doc_key_range`.
After durable table/range ownership routing chooses owner spans, internal
coordinators can attach a start-inclusive/end-exclusive `doc_key_range` before
storage scans so physical placement remains a planner concern instead of a SQL
syntax concern.

The DB executor uses the same relational filter pushdown machinery as search.
Exact equality predicates that fully determine the primary key or a declared
unique constraint resolve through the durable owner row first. Partial unique
constraints are only eligible when the query predicates imply the catalog
predicate; otherwise the planner falls back to ordinary row-query access paths so
non-partial rows are not hidden behind the partial owner. Indexable atoms then
select candidates from declared column-major, array-element, and JSON-value
indexes, while unindexed or non-indexable atoms fall back to base-row scans. The
non-unique planner records each candidate set's estimated cardinality and
intersects the most selective known sets first, with unknown-cardinality sets
kept after known candidates and original request order used only as a tie
breaker.
Candidate selection is not trusted as the final answer: every candidate is
hydrated from the current authoritative relational base row and every scalar,
array, and JSON predicate is rechecked against that row before projection,
ordering, offset, and limit are applied. Candidate hydration also rejects doc
keys outside `doc_key_range`, so indexed predicates cannot leak rows from
another owner span. This keeps stale side rows, partial rebuilds, and unindexed
columns correct while still letting primary/unique lookups, simple
equality/range predicates, `array_any`, `array_eq`, and `json_contains` avoid
whole-table scans when an access path exists. Scalar `json_path_eq` also uses the
JSON-value side index when the declared JSON column is indexed; complex
object/array path equality and JSON path existence use the same base-row recheck
contract and can add dedicated access paths without changing the public typed
query shape.

A coordinator can also issue one typed row query across multiple non-overlapping
physical row-key spans. Each span is resolved against the same base-row and
index contracts, then the coordinator globally orders the merged candidates and
applies pagination, projection, and optional row claims exactly once. This is
the model-level equivalent of a SQL range router: public SQL/REST/SDK adapters
target logical typed row plans, while internal coordinators choose spans from
durable table/range ownership metadata and the storage executor sees typed
`doc_key_range` bounds plus row-query atoms. The table catalog exposes that
bridge as a typed table doc-key range plan: it sorts the table's durable
`RangeRecord`s, clips a requested key span to each owner range, returns the
owning `group_id`, and stamps each span with the table topology epoch. Hosted
and provisioned routers can validate the epoch before executing the span, then
call the same storage-level row-query executor with the clipped
`doc_key_range`.

Aggregates consume the same typed row-query source. A single-table aggregate
request is a source row query plus typed group keys and named metrics:

```json
{
  "source": {
    "where": { "status": "open" }
  },
  "group_by": ["customer_id"],
  "aggregations": [
    { "name": "order_count", "op": "count" },
    { "name": "amount_sum", "op": "sum", "field": "amount" },
    { "name": "amount_avg", "op": "avg", "field": "amount" },
    { "name": "amount_min", "op": "min", "field": "amount" },
    { "name": "amount_max", "op": "max", "field": "amount" }
  ],
  "having": {
    "all": [{ "field": "amount_sum", "op": "gt", "value": 100 }]
  },
  "order_by": [{ "field": "amount_sum", "direction": "desc" }],
  "limit": 100,
  "offset": 0
}
```

The source query is evaluated through the same index-backed candidate selection
and authoritative base-row recheck path as ordinary row queries. Grouping and
metric folding then happen over the materialized row stream and emit typed JSON
projection rows such as
`{ "customer_id": "c1", "order_count": 2, "amount_sum": 30 }`. `count` without
a field is `count(*)`; `count` with a field ignores null/missing values. Numeric
metrics ignore null/missing values and reject non-numeric inputs for the metric
field. Aggregate `order_by` sorts over the emitted group/metric rows before
`offset` and `limit`, so SQL `ORDER BY amount_sum DESC LIMIT ...` lowers without
re-reading base rows. Aggregate `having` is a typed conjunction over emitted
group/metric fields and is encoded as `{ "all": [...] }`; each predicate uses
the same scalar comparison operators as row predicates (`eq`, `ne`, `gt`,
`gte`, `lt`, `lte`, `is_null`, `is_not_null`, `is_distinct`, and
`is_not_distinct`). SQL adapter lowering for ordinary `HAVING a AND b` targets
this explicit group shape. Unsupported `OR` or nested boolean aggregate-output
predicates fail closed until the shared boolean expression planner owns
aggregate-output predicates. This gives a future SQL adapter a direct lowering
target for single-table `GROUP BY` before join streams are attached.

Joins compose two typed row-query sources with explicit equality predicates and
projection fields:

```json
{
  "left": {
    "where": { "kind": "order" },
    "order_by": [{ "field": "id", "direction": "asc" }]
  },
  "right": {
    "where": { "kind": "customer" }
  },
  "on": [
    { "left_field": "tenant_id", "right_field": "tenant_id" },
    { "left_field": "customer_id", "right_field": "id" }
  ],
  "join_type": "left",
  "select": [
    { "output": "order_id", "side": "left", "field": "id" },
    { "output": "customer_name", "side": "right", "field": "name" }
  ],
  "order_by": [{ "field": "customer_name", "direction": "asc" }],
  "limit": 100,
  "offset": 0
}
```

Each side is evaluated as a normal row query, so side-local filters use the
same indexed candidate selection and base-row recheck path. The join executor
then builds a hash table from the right stream using the typed `on` columns and
probes it with the left stream. Join keys are JSON tuples; null or missing join
components do not match, which matches SQL equality-join behavior. `inner` joins
emit only matches. `left` joins emit unmatched left rows with right-side
projection fields set to `null`. Joined result `order_by` sorts over projected
joined rows, then `limit`/`offset` applies after join matching and projection.

The storage-level join executor covers typed row streams. For a single-table
store, those streams are local row-query requests against that store's runtime
schema; for a multi-table SQL/REST/SDK coordinator, table identity is resolved
before local execution by routing each source to the correct table/range owner
and feeding the resulting row streams into the same typed projection and join
contract. That keeps SQL/SDK/REST lowering unified while leaving
PostgreSQL-specific syntax and wire behavior in the adapter layer.

This is the backend AST that a SQL adapter should target for simple
single-table `SELECT`, `GROUP BY`, and local row-stream `JOIN` clauses.
The PostgreSQL adapter lowerer now covers the local row-stream shape for
`INNER JOIN` and `LEFT JOIN` between two aliases of a relational source:
qualified projection fields become typed join projections, `ON a.x = b.y`
clauses become `RelationalRowsJoinOn` equality keys, qualified `WHERE`
predicates are split into side-local row-query predicates, and final `ORDER BY`
uses projected output fields. Cross-table SQL joins should reuse this typed join
request after catalog routing has resolved each table source into the correct
range-owned row stream.
Primary-key, unique-owner, partial-unique, partial-secondary, column-major,
array-element, and JSON-value access paths are now model-level choices beneath
that contract for single-table row queries. Non-unique expression lookups are
represented as stored generated columns, so they use the same column-major
candidate extraction and base-row recheck path as other indexed scalar columns.
Multi-range ordered merge is part of the row-query contract through
`queryRelationalRowsAcrossRanges` and `TableDocKeyRangePlan`. Cross-table join
planning is a coordinator composition concern: it resolves each table source
through durable table/range ownership, executes the existing typed row-query
contract on each owner, and supplies those materialized streams to the same
join/aggregate executors.
Non-recursive CTEs are first-class query composition: a
`RelationalRowsQueryPlan`, `RelationalRowsAggregatePlan`,
`RelationalRowsJoinPlan`, `RelationalRowsWindowPlan`, or
`RelationalRowsLateralPlan` carries ordered named CTEs, each CTE is a typed
`RelationalRowsQueryRequest`, and later CTEs, aggregate/window sources,
join/lateral sides, or the final row query can name a materialized source with
`source_cte`. A CTE definition can use the normal base row-query planner,
including declared indexes, before materialization; predicates over a
materialized CTE are evaluated against that materialized row stream. Native
REST/SDK plan parsers use the same ordered typed contract and reject forward
references, duplicate CTE names, missing final-stage `source_cte` references
from query, aggregate, window, join, and lateral plans, durable row claims, and
physical doc-key ranges inside CTE definitions. Durable row claims and physical doc-key ranges are
base-row planner features and are rejected over materialized CTE sources.
The PostgreSQL adapter exposes this through `lowerQueryPlanAlloc`: non-recursive
`WITH name AS (SELECT ...) ... SELECT ... FROM name` lowers to ordered CTE
queries plus a final `source_cte` query, while plain `lowerSelectAlloc` remains
the single-select API and rejects `WITH`.
Recursive CTEs are a separate graph/fixpoint feature and should be treated as a
distinct planner extension.

Those model-level items are not PostgreSQL-specific. They are the durable
relational semantics that the storage model should expose so a future SQL
dialect, REST API, or client SDK can compile to the same operations.

#### PostgreSQL adapter roadmap

PostgreSQL-shaped SQL may be adapter input. The storage model above now has many
of the durable primitives those queries need, but SQL compatibility is not
a license to pass SQL text through the backend. The production boundary is a
Postgres-facing adapter that parses SQL into a typed frontend AST, resolves it
against Antfly catalog metadata, and lowers only supported shapes into explicit
Antfly row, mutation, query, aggregate, join, CTE, window, and expression plans.
Unsupported syntax should fail closed with a structured unsupported-shape error.

Because this feature set is new, the typed Antfly plan is the source of truth,
and Postgres syntax is a front-end dialect that compiles into that plan.

Current PR status:

- Implemented: fail-closed SQL boundary parsing for the supported PostgreSQL-shaped
  subset; typed `CREATE TABLE` DDL lowering into catalog-plan metadata for
  columns, primary keys, unique constraints, table-level foreign keys, inline
  column `REFERENCES` foreign keys, checks, defaults, JSONB, arrays, and
  supported PostgreSQL scalar types; typed `CREATE INDEX`
  DDL lowering for ordinary indexes, unique indexes, btree column elements with
  `ASC`/`DESC` and optional `NULLS FIRST`/`NULLS LAST` clauses normalized to
  column index metadata, `lower(field)` expression indexes, and simple
  parenthesized or harmlessly casted partial predicates; typed additive
  `ALTER TABLE` DDL
  lowering for `ADD COLUMN` and `ADD CONSTRAINT` unique, foreign-key, and check
  operations, including `ADD COLUMN IF NOT EXISTS ... REFERENCES ...` expansion
  into a column mutation plus unvalidated FK validation work; stored
  generated-column DDL lowering for typed `lower(field)` and
  simple `concat(field, separator, field...)` expressions; known updated-at
  trigger DDL lowering to table-owned `now_ns` update-policy metadata; DDL plan
  application to owned runtime schemas and public relational schema JSON with
  rebuild/validation flags for catalog callers; additive foreign-key DDL lowers
  to `unvalidated` constraint metadata so the existing foreign-key schema
  controller owns repair/validation and promotion to enforced; additive unique
  constraints and `CREATE UNIQUE INDEX` on existing tables lower to durable
  `unvalidated` unique-constraint metadata, while create-table unique
  constraints remain enforced because the table starts empty; row-query
  DDL `NOT VALID` check/FK constraint additions lower into durable unvalidated
  catalog metadata, and `ALTER TABLE ... VALIDATE CONSTRAINT` lowers to a typed
  catalog validation operation that promotes the named check, FK, or unique
  constraint to enforced state after validation; row-query
  lowering for equality/range/null predicates,
  JSONB extraction/containment/existence, array equality/containment/membership,
  text-pattern predicates for SQL `LIKE`/`ILIKE` over declared text-like columns,
  generated `lower(...)` pushdown, scalar OR
  predicate groups with unioned candidate planning, native null-safe
  `is_distinct`/`is_not_distinct` predicates with SQL
  `IS [NOT] DISTINCT FROM` lowering, typed null-test ordering, typed expression order keys,
  `ORDER BY`, `LIMIT`/`OFFSET`,
  `FOR UPDATE [SKIP LOCKED]`, and typed projection aliases for
  `array_length(...)`, `string_to_array(...)`, simple `COALESCE(...)`, `LOWER(field)`, `UPPER(field)`, `CONCAT(...)`,
  `NULLIF(...)`, numeric `+`/`-`/`*`/`/` projection expressions, and field/cast
  aliases such as `id::text AS id_text`, with JSON extraction via `->` and `->>`
  and `array_length(...)` plus `string_to_array(...)` lowering into the shared expression AST, and SQL
  `ORDER BY lower(field)`, `ORDER BY upper(field)`, `ORDER BY COALESCE(...)`,
  `ORDER BY array_length(...)`, and numeric arithmetic lowering to expression
  order keys when no generated-column pushdown exists;
  row-batch lowering for
  `INSERT`, `UPDATE`, `DELETE`, `RETURNING`, `ON CONFLICT` for primary, unique,
  partial-unique, and expression-unique targets, arithmetic increments, JSONB
  patch/set/concat/build/convert operations, typed array append/remove/add-to-set
  transforms, same-column and typed cross-column `excluded.column` conflict
  value reuse, numeric conflict deltas from `excluded` values, source-qualified
  typed `patch_expr` assignments over `excluded` and committed-row fields,
  including `COALESCE`, `LOWER`, `UPPER`, searched `CASE`, `NULLIF`, casts,
  `array_length`, and arithmetic, source-qualified typed `increment_expr`
  deltas from `COALESCE(excluded.column, fallback)`, source-qualified
  conflict-action `where_expression` predicates, conflict-action `DEFAULT`,
  explicit `NOW()`, explicit SQL `DEFAULT`, and table-owned `now_ns` update
  policies for updated-at columns;
  aggregate lowering for
  `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, scalar `FILTER`, typed aggregate
  filter expressions such as `FILTER (WHERE lower(status) = 'open')`, scalar
  input expressions such as `COUNT(DISTINCT lower(status))` and
  `SUM(amount - discount)`, scalar `DISTINCT`, bounded ordered scalar
  `array_agg`, `GROUP BY`, and output-alias `HAVING`;
  local equality `INNER`/`LEFT` row-stream joins; scalar `NOT (...)` predicate
  groups; non-recursive `WITH` lowering to ordered typed query plans; typed
  `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` window-plan lowering and local
  execution over filtered base-row or materialized CTE sources, with
  source row claims rejected for windowed output; native REST/SDK aggregate,
  equality join, bounded lateral, window, and ordered CTE plan parsers plus
  public `rows:query`, `rows:aggregate`, `rows:window`, `rows:join`,
  `rows:lateral`, and `rows:mutation-source` HTTP endpoints that accept the same typed
  predicate/projection/expression/order contracts as the SQL adapter, preserve
  rich join/lateral side-query predicates including expression OR/NOT groups
  and computed-array containment, validate ordered `source_cte` references for
  join/lateral plan sides, publish typed OpenAPI/SDK row-plan envelope structs
  through the Zig, Go, TypeScript, and Python SDKs instead of raw JSON-only
  client inputs, require each public operation endpoint to receive exactly its
  matching top-level envelope field plus optional ordered CTEs, return stable row-result envelopes, reject
  lockable or range-internal sources at the public boundary, push supported
  conjunctive row filters into every base row source, and fail closed for
  unsupported row-filter shapes; a seeded SQL compatibility
  corpus test that classifies representative schema,
  migration-equivalence, query, aggregate, join, lateral, window, point DML,
  claimed mutation-source, and unsupported adapter-only shapes through the
  fail-closed SQL boundary and pins stable typed-plan fingerprints for target
  tables, DDL tags, predicates, projections, expression projections,
  operations, joins, windows, insert/update/delete returning rows,
  insert/update/delete and update/delete mutation-source returning expressions, limits, row
  claims, and unsupported
  classifications, plus catalog-application fingerprints for representative DDL
  rebuild flags, validation flags, building secondary indexes, unvalidated
  constraints, and update policies; local
  unique-constraint schema-controller maintenance that repairs owner rows,
  revalidates after repair, and promotes unvalidated local unique constraints to
  enforced only after terminal valid validation; and catalog-backed
  provisioned/hosted unique schema-controller rounds that scan durable catalog
  schemas, route repair/validation through table groups, expose controller
  status, and promote valid unvalidated unique constraints through catalog table
  schema compare-and-swap updates; ordinary secondary indexes carry
  schema-level lifecycle state through
  runtime schema and public schema JSON, existing-table `CREATE INDEX` marks
  non-unique indexes `building` with deterministic rebuild generation IDs,
  writes maintain those building indexes, query planners/scan APIs use only
  `ready` secondary indexes, and metadata now has durable secondary-index
  rebuild work-range records with declared/building/ready/invalid lifecycle,
  generation-aware identity, lease/progress/error fields, Raft-log
  encode/decode, reopen persistence, projection snapshots, status counts,
  reconciler-derived per-table-range scheduling for building index generations,
  placement intents for rebuild work groups, stale-generation cleanup, and
  preservation of in-flight rebuild leases/progress across reconcile rounds;
  a local range execution primitive that repairs one building secondary
  index generation by deleting only that column's stale scalar/array/JSON/reverse
  side rows inside the ownership span and re-appending current matching rows;
  expired-lease reclaim for rebuild work records; and a reusable local worker
  execution helper that claims metadata work, runs the range repair, finishes on
  success, and invalidates the work record on storage failure; the table-write
  source boundary exposes a bounded secondary-index rebuild worker pass that
  scans projected catalog work records, claims provisioned/local group work
  through catalog metadata transitions, forwards hosted remote group-local work
  through the internal table-write route, executes the local range repair, and
  aggregates progress while leaving busy leases incomplete; and
  generation-checked secondary-index promotion that takes a fresh catalog
  snapshot, requires every table range to have a `ready` rebuild record for the
  exact building index generation, ignores stale ready records from older
  generations, and promotes the schema lifecycle to `ready` through the catalog
  source boundary using a raft-applied schema compare-and-swap command that
  carries the exact observed schema bytes and promoted table image, so stale
  promotion proposals cannot overwrite concurrent schema changes.
- Remaining: non-additive migration-equivalence compilation, broader
  trigger/function pattern compilation beyond updated-at policies, and native
  rewrite/backfill application; full typed scalar expression trees
  for broader function operands, structured JSON/array OR and NOT branches,
  mixed boolean trees beyond the current `all`/`any`/`not` group form,
  expression-predicate pushdown and boolean grouping beyond the current
  conjunctive residual row-query field, and remaining function families beyond
  the current scalar AST; cross-table/range-routed
  joins plus hosted/public routed row-plan endpoint execution beyond the local
  single-store path while preserving pre-stream row-filter pushdown;
  spill-bounded distinct aggregate state beyond the explicit in-memory
  capped scalar implementation, and broader JSON/array/complex aggregate filter
  and distinct keys over the shared expression tree; routed/cross-range lateral
  execution beyond the bounded local `LEFT JOIN LATERAL` stage; richer window
  frame semantics, additional frame-aware functions, spill/backpressure, and
  routed window execution beyond the local ranking/value stages; broader mutation-hook policies
  beyond table-owned updated-at `now_ns`; and SQL
  corpus/golden-plan/execution/chaos gates that prove compatibility.

PostgreSQL-shaped SQL parity is therefore close at the model level, but not complete
until the remaining pieces below are implemented and gated against representative
SQL/API corpora. The core storage model should not pass SQL strings around after
the adapter boundary. SQL text is parsed once by the Postgres-facing adapter,
bound against catalog metadata, and lowered into typed Antfly plans. Those typed
plans are the durable internal contract used by REST, SDK, SQL, migrations,
repair, rebuild, and simulation tests.

Planning assumptions for the remaining work:

- Relational mode starts from the current relational storage format.
  Unsupported or partially implemented SQL shapes should fail closed until they
  lower to a typed plan with catalog metadata, execution semantics, and tests.
- This feature set starts here. Do not add legacy relational encodings,
  compatibility aliases, old request fallbacks, or migration shims for
  pre-relational behavior; unsupported shapes should become typed Antfly plans,
  explicit rewrite/rebuild jobs, adapter-only no-ops, or stable unsupported
  errors.
- Migration-equivalent work means Antfly-native catalog, validation,
  rebuild, and rewrite plans. Exact PostgreSQL migration-file replay can be
  implemented as an adapter feature, but it is not the backend contract.
- SQL text is not a backend data structure. Parser output must normalize into
  Antfly-owned structs for schemas, expressions, row selectors, mutations,
  queries, joins, aggregates, CTEs, windows, repair, and rebuild.
- Composite primary keys, unique-owner routing, foreign-key ownership rows,
  embedded JSON-column indexing, and schema-controller repair/promote work are
  the baseline architecture. New SQL support should reuse those mechanisms
  rather than adding Postgres-specific side paths.
- Every planned feature needs an API/SDK shape as well as a SQL lowering path,
  because the typed plan is the product boundary and Postgres syntax is only one
  frontend.

Given the composite-primary-key, durable unique-owner, 2PC participant,
foreign-key ownership, schema-controller, embedded JSON, and secondary-index
lifecycle work in this PR, the remaining PostgreSQL-shaped SQL surface should be
planned as model-owned Antfly capabilities in this order:

| Layer | Scope | Long-term plan |
| --- | --- | --- |
| Adapter-only PostgreSQL surface | `pgx` wire behavior, placeholders, SQLSTATE errors, result labels, catalog-view shims, extension declarations, dump boilerplate, and exact migration-file replay syntax. | Keep this in the Postgres-facing adapter. These shapes either lower to typed Antfly plans, become proven no-op adapter records, or fail with stable unsupported-shape errors. They do not create backend SQL strings. |
| Schema/catalog foundation | `CREATE TABLE`, additive and non-additive `ALTER TABLE`, constraints, defaults, generated columns, checks, JSONB/array columns, trigger-derived update policies, partial indexes, expression indexes, unique/FK validation, and migration-equivalent data rewrites. | Apply schema changes through typed catalog mutations and typed rewrite/backfill jobs. Existing-table derived artifacts become explicit validation or rebuild work with deterministic generations, range ownership, repair workers, and schema compare-and-swap promotion. Since the relational format starts here, non-additive migration work should be planned rewrite/rebuild jobs, and a PostgreSQL migration runner can later lower into the same operations. |
| Row mutation foundation | `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT ... DO NOTHING/UPDATE`, `RETURNING`, `DEFAULT`, `NOW()`, `EXCLUDED.column`, JSONB transforms, array transforms, updated-at policies, and transaction-aware `mutation_source` update/delete over claimed base-row query sources, with API parser/encoder coverage, a public HTTP endpoint, SDK models, and SQL lowerer support. | Keep mutations as row-batch or mutation-source plans over structured primary/unique selectors, OCC predicates, 2PC participants, transforms, and returning projections. The remaining production work is full expression support over `excluded` and the committed row image, unique-owner routing for every non-primary selector, and routed/hosted mutation-source execution across table ranges. |
| Query and expression foundation | `WHERE`, `ANY`, `IN`, `NOT`, mixed `AND`/`OR`, casts, scalar functions, JSONB operators, array operators, projection expressions, typed row-query expression order keys, `ORDER BY`, `LIMIT`, and `OFFSET`. | Collapse remaining shape-specific lowering into one typed scalar expression tree used by predicates, projections, generated columns, checks, expression indexes, conflict actions, aggregate filters, order keys, and `RETURNING`. Planner pushdown is derived from that tree only when it maps to declared primary, unique, generated, column-major, array, JSON, or embedded-JSON access paths. |
| Lockable row streams | `FOR UPDATE`, `FOR UPDATE SKIP LOCKED`, queue claims, and local multi-row `UPDATE`/`DELETE` mutation sources have a base-row claim/OCC contract. | Add ordered multi-range fill, durable claim ownership beyond the local transaction record, lease/retry semantics, hosted participant registration, and range-movement chaos coverage. Keep claims illegal over joins, aggregates, materialized CTEs, and windows until those stages expose a lockable base-row contract. |
| Routed stream composition | Equality joins, `LEFT JOIN`, bounded local `LEFT JOIN LATERAL`, grouped rollups, aggregate `FILTER`/`DISTINCT`, `HAVING`, CTEs, and local window stages including `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()`. | Use typed routed streams with output schemas, table/range ownership routing, lookup/hash/merge join strategies, coordinator merge ordering, bounded CTE materialization, spill-aware aggregate/window state, routed window partitions, and routed correlated lateral stages with declared right-side bounds. |
| Compatibility evidence | Representative SQL, migration-equivalent schema/data changes, representative execution flows, and failure modes under repair/routing. | Gate parity with harvested SQL and native migration-equivalence golden plans, execution tests for row/identity/constraint/queue/JSON/usage flows, and chaos/sim workloads that combine live writes, FK checks/actions, unique-owner repair, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and rewrite/rebuild jobs. |

The concrete rollout should keep the backend model ahead of the SQL adapter.
Each milestone adds or hardens an Antfly typed contract first, then teaches the
Postgres-facing adapter to lower PostgreSQL syntax into that contract:

| Milestone | Scope | Existing work it builds on | Completion criteria |
| --- | --- | --- | --- |
| 0. Corpus and plan contracts | Harvest representative SQL and schema/data-change intent; define stable JSON/typed structs for DDL, row selectors, mutations, expressions, row queries, joins, aggregates, CTEs, windows, row claims, repair, rebuild, and rewrite jobs. | The adapter already fails closed, lowers supported syntax into Antfly structs, and the seeded corpus gate pins typed-plan fingerprints for representative DDL, query, aggregate, join, lateral, window, DML, mutation-source, and unsupported shapes. The seeded gate now includes computed expression predicates, computed expression OR/NOT groups, computed-array predicates, aggregate source predicates, CTE source boundaries, join/lateral side-query predicate classes, bounded lateral right-side order/limit, window value/default expressions, and mutation-source `patch_expr` / `increment_expr` counts so the corpus catches lossy adapter-to-API lowering. | Every harvested runtime statement and migration-equivalence step has a golden typed plan, an adapter-only no-op classification, or an unsupported classification that names the missing model feature. No storage path accepts SQL text. |
| 1. Shared expression spine | Replace shape-specific predicate/projection/update/index cases with one bound expression AST over fields, literals, parameters, casts, deterministic functions, boolean ops, arithmetic, null semantics, JSON, and arrays. | Existing row queries, generated columns, checks, JSONB/array predicates, order keys, aggregate filters, conflict updates, and simple `RETURNING` already have typed pieces. | The same AST is used for `WHERE`, projections, checks, generated columns, expression indexes, partial predicates, `ON CONFLICT` actions, update transforms, aggregate filters, order keys, and `RETURNING`; pushdown is derived from the AST rather than hand-coded parser cases. |
| 2. Catalog and migration lifecycle | Apply migration-equivalent DDL and data-change intent as transactional catalog mutations, typed rewrites, rebuilds, and validations. | Composite identity, unique/FK validation state, schema-controller repair/promotion, secondary-index lifecycle, CAS schema promotion, and embedded JSON-column indexing are now the baseline. | Intended final schema and migration effects compile into catalog schema JSON; every existing-table index, expression index, unique constraint, FK, check, embedded JSON schema/template change, and non-additive rewrite produces durable work with deterministic range ownership and promotion gates. Exact PostgreSQL migration-file replay can be layered on top by lowering into these same steps. |
| 3. Point and selector DML hardening | Finish `INSERT`, point/unique `UPDATE` and `DELETE`, `ON CONFLICT ... DO UPDATE/NOTHING`, `DEFAULT`, `NOW()`, `EXCLUDED`, JSONB/array transforms, server-owned update policies, and committed-image `RETURNING`. | Structured primary/unique selectors, durable unique-owner rows, point row-batch plans, 2PC participants, table-owned updated-at policies, JSONB/array transforms, and simple returning are implemented. | Ledger, auth, RBAC, billing, seed, and JSON metadata flows run through typed row-batch plans; non-primary selectors resolve through unique-owner rows before prepare; `RETURNING` is evaluated from the final committed image. |
| 4. Multi-row DML and queue claims | Local `mutation_source` plans update/delete rows selected from ordered lockable row-query sources for `UPDATE ... WHERE`, `DELETE ... WHERE`, `FOR UPDATE`, and `FOR UPDATE SKIP LOCKED`; the row API can parse/encode the typed contract and the PostgreSQL adapter can lower bounded transaction-claimed update/delete statements into it. | Base-row row claims, ordering, pagination, unique-owner routing, FK/unique 2PC participants, OCC predicates, SQL lowerer support, and transaction-staged mutation sources exist. | Queue, cleanup, re-encryption, and batch mutation workloads claim rows durably, fill across ranges in order, integrate with OCC/2PC, survive retries and range movement, and reject claims over joins, aggregates, windows, and materialized CTEs until those stages expose lockable base rows. |
| 5. Index and JSON/array completeness | Generalize partial/expression indexes, partial unique constraints, JSONB operators, array operators, and embedded JSON-column document indexes through the shared expression and catalog-work lifecycle. | Simple partial predicates, `lower(field)` expression indexes through generated columns, ordinary secondary-index rebuild work, JSON/array predicates/transforms, and embedded JSON-column design are present. | Planner uses only `ready` derived artifacts; writes enforce only `enforced` unique constraints; partial implication is shared by planner, uniqueness, and `ON CONFLICT ... WHERE`; JSON/array SQL sugar has equivalent REST/SDK typed nodes; embedded JSON schema/template changes rebuild deterministically. |
| 6. Routed stream execution | Extend local row streams, joins, CTEs, aggregates, lateral stages, and windows to routed cross-table/cross-range execution with output schemas and memory/spill bounds. | Local equality joins, non-recursive CTE lowering, native ordered CTE plan parsing, scalar aggregates, bounded `array_agg`, scalar `FILTER`/`DISTINCT`, bounded local `LEFT JOIN LATERAL`, and local `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` are modeled. | Billing, usage, RBAC, dashboard, wake-one, and migration/backfill reads execute with correct row-version visibility, ordering, pagination, coordinator merge behavior, bounded materialization, and explicit spill/fail policy. |
| 7. Compatibility proof | Make SQL/API compatibility a test gate rather than a parser breadth claim. | Focused unit/runtime tests and core storage chaos/sim coverage already exist. | Golden-plan corpus tests, representative execution flows, and sim/chaos workloads combine live writes, FK checks/actions, unique-owner repair, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and native rewrite/rebuild jobs. |

The representative SQL corpus should be treated as a compatibility workload, not
as a mandate to build a PostgreSQL executor. The recurring shapes in SQL/API
workloads and migrations are:

- schema DDL for tables, additive/non-additive `ALTER TABLE`, primary keys,
  unique constraints, foreign keys, partial indexes, expression indexes,
  defaults, checks, JSONB columns, array columns, and updated-at triggers;
- point and selector-based `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT ... DO
  NOTHING/UPDATE`, `RETURNING`, arithmetic updates, `NOW()`, explicit
  `DEFAULT`, `EXCLUDED.column`, and JSONB/array transforms;
- scalar predicates with `LOWER(...)`, `UPPER(...)`, `COALESCE(...)`, casts, null tests,
  `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`, `IN`, `ANY`, `NOT`, mixed `AND`/`OR`, and timestamp
  comparisons;
- JSONB and array operators including `->`, `->>`, `@>`, existence checks,
  `jsonb_set`, `to_jsonb`, JSONB concat/build forms, array membership,
  `array_length`, `array_agg`, and `ANY($uuid_array)`;
- ordered/paginated reads, `FOR UPDATE`, `FOR UPDATE SKIP LOCKED`, grouped
  rollups, aggregate `FILTER`, aggregate `DISTINCT`, `HAVING`, ordinary
  equality joins, `LEFT JOIN`, and dashboard-style `LEFT JOIN LATERAL`.

Given the relational, composite-identity, foreign-key, unique-owner, embedded
JSON, and secondary-index lifecycle work in this PR, the long-term plan is:

| Priority | SQL/API shape | Antfly model plan | Production gate |
| --- | --- | --- | --- |
| P0 | SQL and migration-equivalence corpus | Generate a normalized corpus from representative SQL plus the intended schema/data effects of migrations; bind every statement or native migration step against an Antfly catalog snapshot; lower supported shapes to typed Antfly DDL/query/mutation/aggregate/join/rewrite/rebuild plans; classify adapter-only no-ops and unsupported shapes explicitly. | Golden typed-plan tests must cover every harvested runtime statement and migration-equivalence step before claiming SQL/API compatibility. Unsupported classifications include feature, reason, and intended Antfly plan shape. |
| P0 | SQL boundary | Keep SQL text inside the Postgres-facing adapter. Storage, repair, rebuild, FK, unique, query, and row-write paths receive only Antfly-owned structs. | Add fail-closed lowerer tests that prove unsupported SQL never reaches storage as text or stringly-typed fragments. |
| P1 | DDL and migrations | Lower supported DDL and native migration-equivalent steps into catalog mutations over `TableSchema`, relational columns, primary keys, unique/FK/check metadata, generated columns, defaults, update policies, secondary-index lifecycle state, and explicit rewrite/rebuild/validation work. Non-additive changes become planned rewrite/rebuild jobs because the relational format starts here. | Compile the final schema and migration effects into catalog JSON; assert rebuild/validation work is scheduled for existing-table indexes/constraints; reject extension, dump, PL/pgSQL, or trigger syntax unless it compiles to a typed policy or proven no-op. Exact Postgres migration replay can be an adapter test, not the storage contract. |
| P1 | Partial/expression indexes and conflict targets | Use the common typed expression tree for partial predicates, generated columns, expression unique keys, and conflict-target inference. Ordinary secondary indexes use generation-aware rebuild work and catalog schema compare-and-swap promotion; expression-derived indexes should reuse that lifecycle as they move onto the common expression tree. | Queries use only `ready` secondary indexes; writes enforce only `enforced` unique constraints; `ON CONFLICT ... WHERE` shares the same predicate normalizer as planner pushdown and uniqueness enforcement. |
| P1 | Point DML, selector DML, and `RETURNING` | Keep row mutations as typed row-batch plans over structured primary/unique selectors, OCC predicates, transforms, conflict actions, generated/default/server-owned values, and returning projections from the committed image. | Cover ledger, RBAC, auth, billing, and user flows that depend on `RETURNING`, `EXCLUDED.column`, arithmetic increments, JSONB updates, array transforms, `NOW()`, and explicit `DEFAULT`. |
| P1 | Multi-row `UPDATE`/`DELETE` and row locks | A local `mutation_source` node now accepts a lockable typed row-query source with order/limit, row-claim metadata, OCC preimage predicates, and `SKIP LOCKED` selection, then stages update/delete intents in the claiming transaction. | Queue and re-encryption workloads using `FOR UPDATE` or `FOR UPDATE SKIP LOCKED` run without double-claiming, lost updates, or range-movement leaks across routed owner ranges. Claims stay illegal over joins, aggregates, and materialized CTEs until those sources expose lockable base rows. |
| P1 | Scalar expression tree | One expression AST covers predicates, projections, generated columns, check constraints, expression indexes, conflict actions, update transforms, aggregate filters, order keys, and `RETURNING`. Nodes include row fields, parameters, literals, casts, null tests, boolean ops, arithmetic, `LOWER`, `UPPER`, `COALESCE`, `NULLIF`, `CASE`, `NOW`, interval arithmetic, and `IS [NOT] DISTINCT FROM`. | Every expression is type-bound once at the adapter boundary; planner pushdown is an optimizer property of the AST; unsupported functions fail before execution. |
| P1 | JSONB and array operators | Treat SQL operators as sugar over typed JSON/array predicate, projection, construction, and transform nodes on declared relational columns or embedded JSON-column indexes. | JSONB path/extract/containment and array membership/equality/containment are available through REST/SDK typed plans as well as SQL; indexed paths rebuild through the same derived-artifact lifecycle as secondary indexes. |
| P2 | Joins and routed streams | Ordinary `INNER`/`LEFT` equality joins use typed row streams; distributed execution routes each table/range through durable ownership and chooses lookup/hash/merge strategies from typed hints. | Cross-table/range join tests cover live writes, owner movement, row-version visibility, ordering, and coordinator merge semantics. |
| P2 | `LEFT JOIN LATERAL` | Bounded local lateral is modeled as a correlated subquery stage: the right plan binds against left-row fields, requires a capped right-side plan, and preserves unmatched left rows with null right outputs. | Bugeye/dashboard queries using per-organization balance and stipend lookups run through typed lateral stages rather than bespoke SQL execution; routed/cross-range lateral remains future work. |
| P2 | Aggregates, `DISTINCT`, `FILTER`, and `HAVING` | Extend aggregate specs so filters and distinct keys are expression nodes; keep ordered `array_agg` explicitly capped; add spill-backed distinct/materialized state when a declared memory bound is exceeded. | Usage rollups, billing balances, RBAC membership summaries, `COUNT(DISTINCT ...)`, `array_agg(DISTINCT ...) FILTER (...)`, output-alias `HAVING`, and aggregate ordering/pagination have golden plans and execution tests. |
| P2 | CTEs and windows | Non-recursive CTEs are named typed subplans with output schemas, inlining rules, and materialization bounds. Windows are separate ordered stream stages with partition/order metadata; local `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` stages are implemented. | Migration/backfill queries either lower to bounded CTE/window plans or fail closed with a replacement plan; recursive CTEs remain unsupported until a graph/fixpoint node exists, and broader window frame semantics and additional frame-aware functions need spill/backpressure before unbounded use. |
| P3 | Compatibility chaos | Combine live writes, FK checks/actions, unique-owner movement, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and repair in one modeled workload. | A SQL/API compatibility sim suite becomes a release gate alongside SQL golden-plan and representative execution tests. |

PostgreSQL-shaped SQL coverage should be divided into three buckets:

1. **Adapter-only Postgres surface.**
   `pgx` protocol behavior, PostgreSQL catalog views, placeholder numbering,
   SQLSTATE error mapping, extension declarations, dump boilerplate,
   PL/pgSQL/function syntax, exact trigger syntax, and exact migration-file replay are
   not storage-model features. The adapter owns parsing and presentation, then
   either lowers the statement into a typed Antfly plan or
   rejects it with a stable unsupported-shape classification. These items should
   not introduce SQL strings into the backend.

2. **Model-backed SQL already covered by this PR.**
   Composite primary keys, structured row identity, durable unique-owner lookup,
   foreign keys and reverse-reference ownership rows, local/cross-range FK
   checks/actions, unvalidated unique/FK schema-controller repair and promotion,
   JSON/array relational columns, embedded JSON-column indexing design,
   generated columns, defaults, checks, point row-batch DML, primary/unique
   `ON CONFLICT`, simple `RETURNING`, scalar/JSON/array predicates, ordering,
   pagination, row claims, local equality joins, non-recursive CTE lowering, and
   scalar aggregate plans all have typed Antfly model homes. SQL syntax for
   these is frontend sugar over the same structs REST, SDK, repair, rebuild, and
   simulation tests can construct directly.

3. **Remaining model-level work for SQL/API compatibility.**
   The remaining non-Postgres-specific gaps are partial/expression index
   completeness, multi-row DML over lockable row-query sources, full expression
   support for `excluded` and `RETURNING`, routed cross-range joins, reusable CTE
   materialization bounds, spill-backed aggregate `DISTINCT`, richer aggregate
   filters, routed lateral beyond bounded local `LEFT JOIN LATERAL`, broader window frame semantics beyond local ranking/value windows, advanced
   JSONB/array expression operands, queue-safe `FOR UPDATE SKIP LOCKED`, and
   corpus/chaos gates. These should land as typed Antfly APIs first, then be
   exposed by the SQL adapter.

The detailed plan for the remaining model-level bucket is:

| Model gap | Long-term Antfly shape | Production steps |
| --- | --- | --- |
| Partial and expression indexes | Durable index/unique metadata with typed partial predicates, deterministic expression AST keys, lifecycle state, rebuild generation, collation/null semantics, and shared implication logic. | Normalize every partial predicate through the common expression tree; use one implication checker for planner pushdown, uniqueness enforcement, and `ON CONFLICT ... WHERE`; keep simple `lower(field)` indexes as generated columns when that is the better physical plan; reuse ordinary secondary-index routed rebuild workers and schema compare-and-swap promotion for expression-derived indexes. |
| Multi-row `UPDATE`/`DELETE` | Local mutation-source plans now accept an explicit typed, lockable row query with order/limit, row-claim metadata, `SKIP LOCKED` selection, and committed-version OCC predicates, then stage update/delete intents in the claiming transaction. | Extend mutation sources to routed owner ranges, hosted participant registration, durable claim lease/retry semantics, full expression transforms over the planned preimage/final image, and unique-owner routing for non-primary selectors. Keep mutation sources over joins, aggregates, or materialized CTEs rejected until those stages expose an explicit lockable-row contract. |
| `ON CONFLICT ... DO UPDATE` expressions | Conflict actions over a typed expression tree with `excluded`, committed-row, default, parameter, literal, and server-owned value inputs. | Replace shape-specific `excluded.column` paths with expression nodes; enforce type compatibility during binding; evaluate generated columns and table-owned update policies after conflict transforms; project `RETURNING` from the committed image. |
| `RETURNING` expressions | Typed projection expressions over the committed row image, including generated/default/server-updated values. | Share the common expression tree with query projections; bind result labels in the adapter; evaluate after OCC/2PC success and after server-maintained fields are applied. |
| Routed joins | Distributed row-stream execution over durable table/range ownership metadata with lookup, hash, or merge strategies. | Route each side through table/range owners; push predicates and limits only when semantics preserve output; merge ordered streams once at the coordinator; keep row-version visibility stable across both sides; add chaos coverage with owner movement and live writes. |
| CTE materialization | Named typed subplans with output schemas, inlining rules, and explicit memory/spill limits. | Inline single-use CTEs when safe; materialize reused CTEs with declared bounds; reject recursive CTEs until a separate fixpoint/graph plan exists; preserve column aliases and types for downstream joins/aggregates/windows. |
| Aggregate filters and `DISTINCT` | Aggregate specs whose filters and distinct keys are expression nodes, with bounded in-memory state and optional spill. | Move scalar `FILTER` and `DISTINCT` paths onto the expression tree; add per-group/per-metric memory accounting; spill or fail predictably when a declared bound is exceeded; keep ordered `array_agg` explicitly capped. |
| `LEFT JOIN LATERAL` | A correlated, bounded per-left-row subquery stage, not a special case inside ordinary joins; local execution supports declared correlations, right filters, right order/limit, and null-preserving left output. | Extend the same stage to routed cross-table/range execution with durable ownership routing, coordinator merge behavior, and explicit resource bounds. |
| Window functions | Ordered, partitioned stream stages with typed partition keys and order keys; local `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` over base-row or materialized CTE sources are modeled. | Add frame metadata, additional frame-aware functions, routed partition execution, and spill/backpressure before allowing unbounded partitions. |
| JSONB and array advanced operators | JSON/array predicate, projection, construction, and transform nodes over declared relational columns and embedded JSON-column indexes. | Keep SQL operators as adapter sugar; push down only through declared column, array, generated-column, or embedded JSON indexes; run schema/template changes through explicit derived-index rebuild work; evaluate unsupported operators over hydrated rows only when bounded, otherwise reject. |
| `FOR UPDATE SKIP LOCKED` queues | Lockable base-row streams with durable claim ownership, lease/retry semantics, OCC integration, and ordered multi-range fill. | Keep claims illegal over joins/aggregates/CTEs; add coordinator fill across ranges without double-claiming; persist claim ownership and lease expiry; test range movement, retries, aborts, and concurrent writers in chaos. |
| SQL/API compatibility gates | Generated SQL and migration-equivalence corpus, golden typed plans, representative execution tests, and combined repair/routing chaos workloads. | Harvest representative SQL plus native schema/data-change intent from migrations; classify every runtime statement and native migration step as supported, unsupported, or adapter-only no-op; add golden typed-plan tests; run row, queue, identity, constraint, JSON metadata, usage-rollup, and migration/backfill flows; combine live writes, FK checks/actions, unique-owner repair, secondary/embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and typed rewrite/backfill jobs in sim. |

Given the rest of this PR, SQL/API compatibility should be planned as completion gates
rather than a separate PostgreSQL executor:

| Gate | Current shape | Work needed before calling it complete |
| --- | --- | --- |
| Schema and migration-equivalence corpus | Supported `CREATE TABLE`, `CREATE INDEX`, additive `ALTER TABLE`, generated columns, defaults, checks, JSONB/array columns, FKs, unique constraints, partial indexes, expression indexes, `NOT VALID`/`VALIDATE CONSTRAINT`, and updated-at trigger shapes lower to typed schema/catalog plans. | Capture the intended final schema and migration effects, classify each native step or adapter-lowered statement, apply supported plans transactionally to catalog schema JSON, add explicit rebuild/validation jobs for every non-empty-table index/constraint change, and fail unsupported dump/extension/PL/pgSQL-only syntax with stable adapter errors. |
| Partial and expression index lifecycle | Partial predicates, simple `lower(field)` expression indexes, unique validation state, and ordinary secondary-index lifecycle metadata are modeled. Durable ordinary secondary-index rebuild ranges are scheduled and placed; expired `building` leases can be reclaimed; a generic local worker helper can claim, repair, finish, or invalidate one building generation without clearing unrelated secondary namespaces; provisioned/hosted table-write sources can run a bounded catalog-projected rebuild worker pass, including hosted remote group forwarding through an internal route; valid unvalidated unique constraints and completed ordinary secondary-index generations promote through raft-applied schema compare-and-swap updates against the exact observed schema bytes, and secondary-index promotion additionally requires every table range to have a `ready` rebuild record for the exact building generation. | Share partial-predicate implication across planner, uniqueness, and `ON CONFLICT ... WHERE`, generalize expression indexes onto the common typed expression tree, and route expression-derived rebuild/promotion through the same lifecycle contract. |
| CRUD, conflict, and returning | Point `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT DO NOTHING/UPDATE`, server defaults, updated-at policies, JSONB/array transforms, field/expression `RETURNING` over committed row images, and local claimed mutation-source update/delete with typed field/expression returning lower to typed mutation plans. | Add full expressions over `excluded`, non-primary selector routing through durable unique-owner rows, routed/hosted mutation-source execution, and mutation hooks for any remaining deterministic trigger patterns. |
| Query predicates and ordering | Scalar predicates, generated-column pushdown, JSONB/array predicates, OR/NOT groups, native null-safe `is_distinct`/`is_not_distinct` predicates with SQL `IS [NOT] DISTINCT FROM` lowering, field order keys, typed expression predicates, typed expression OR/NOT groups, typed expression order keys for lower/coalesce/array-length/arithmetic nodes, null-test ordering, limit/offset, and row claims have model homes. | Collapse remaining shape-specific lowering into one typed scalar expression tree and let the planner derive pushdown from primary, unique, generated, column-major, array, JSON, and embedded-JSON access paths. |
| Joins, CTEs, aggregates, and windows | Local equality joins, bounded local `LEFT JOIN LATERAL`, non-recursive CTE lowering, scalar aggregate lowering over field and shared expression inputs, bounded `array_agg`, scalar `FILTER`, typed aggregate filter expressions, scalar `DISTINCT`, `GROUP BY`, `HAVING`, aggregate ordering/pagination, native join/aggregate/lateral/window API parsing, native ordered CTE plan parsing, and local `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` window stages are modeled. | Add routed cross-table/cross-range stream execution, CTE output-schema tracking and spill bounds, spill-backed distinct state, broader aggregate filters and distinct keys over expression trees, routed lateral execution, additional window frames, frame-aware functions, and spill/backpressure for unbounded partitions. |
| Queue-style locking | `FOR UPDATE [SKIP LOCKED]` lowers only over lockable base-row streams. | Add ordered multi-range filling, durable claim ownership, OCC integration, retry/lease semantics, and chaos coverage with range movement before using it as a production queue primitive. |
| JSONB/document-in-row fields | Declared `json` columns are committed row cells with typed JSON predicates/transforms and an embedded document-index design under the column path. | Route embedded JSON full-text/path/algebraic rebuilds through the same catalog work-unit lifecycle as secondary indexes, expose typed JSON selectors/transforms through REST/SDK, and keep SQL JSONB syntax as adapter sugar. |
| Compatibility evidence | The backend boundary rejects unsupported SQL instead of passing SQL text through storage. | Generate SQL and migration-equivalence golden-plan tests, execute representative flows, and add chaos/sim workloads that combine live writes, FK checks/actions, unique-owner repair, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and typed rewrite/backfill jobs. |

PostgreSQL-shaped SQL parity should be tracked as these model-level implementation
tracks:

| Track | Already modeled in this PR | Remaining production work |
| --- | --- | --- |
| Composite identity, uniqueness, and foreign keys | Structured primary-key and unique selectors, composite tuple encoding, durable unique-owner rows, FK metadata, reverse-reference rows, owner-range routing, local/cross-range parent validation, restrict/cascade/set-null action jobs, and schema-controller repair/promotion are modeled as relational metadata and 2PC participants. | Keep expanding chaos coverage that combines live row writes, owner movement, FK checks, unique-owner repair, action jobs, and range movement; expose structured unique selectors consistently through REST/SDK/SQL; and keep non-primary selectors routed through durable owners rather than scans. |
| Partial and expression indexes | Typed partial predicates on secondary indexes and unique constraints; simple `lower(field)` expression indexes lowered through generated-column metadata; conflict-target inference shares named unique metadata; unique constraints carry durable validation state; unvalidated unique constraints do not derive owner ranges or participate in write-time uniqueness enforcement until promoted; local and catalog-backed unique schema-controller maintenance can repair, validate, and promote unvalidated unique constraints after owner rows are complete and valid; ordinary secondary indexes carry schema-level lifecycle state and deterministic rebuild generation IDs; metadata has durable generation-aware secondary-index rebuild work ranges with lifecycle, lease, progress, error, projection, status, reconciler scheduling, placement intents, stale-generation cleanup, and in-flight progress preservation; expired rebuild leases are reclaimable; local storage can execute a targeted generation rebuild over one row span; local worker execution can claim/finish/invalidate a work record; provisioned/hosted table-write sources can scan catalog work, claim ranges, execute local or remote-hosted repairs, aggregate progress, and promote only when every table range is ready for the exact generation; and planners use only `ready` indexes. | Add a first-class metadata CAS primitive for final rebuild promotion, richer expression-index ASTs, collation/null semantics, and implication checks shared by query pushdown, uniqueness enforcement, and `ON CONFLICT ... WHERE`. |
| Raw DML and conflict handling | Typed row-batch plans for point `INSERT`, `UPDATE`, `DELETE`, `RETURNING`, primary/unique `ON CONFLICT`, arithmetic updates, numeric `excluded` deltas, JSONB updates, array updates, `excluded.column`, `DEFAULT`, table-owned updated-at policies, local claimed mutation-source update/delete, and PostgreSQL-adapter lowering for bounded transaction-claimed multi-row `UPDATE`/`DELETE`. | Add full expressions over `excluded`, returning expressions over the committed image, broader mutation hooks, routed/hosted mutation-source execution, and unique-owner lookup for every non-primary conflict or selector path. |
| Query composition | Typed row queries for scalar, array, JSONB, OR, NOT, ordering, pagination, row claims, aggregates, local equality joins, bounded local lateral stages, non-recursive CTEs, and local ranking/value windows over ordered base/CTE streams. | Add routed cross-table/cross-range joins, richer CTE output schemas, spill bounds for materialized streams, routed lateral stages, broader frame-aware window stages, and full scalar-expression ordering/projection/filtering. |
| JSONB and array SQL operators | Declared `json` and `array` relational columns lower to typed path predicates, containment/existence, `jsonb_set`, JSON construction/concat, array equality/membership/containment, and append/remove/add-to-set transforms. | Route every remaining operator through the common expression tree, add nested path/index planning where declared, keep schemaless JSON rebuilds explicit, and reject unsupported PostgreSQL operator semantics at the adapter boundary. |
| Embedded JSON document fields | A relational `json` column is an opaque committed row cell plus derived document-style full-text/path/algebraic projection under the owning column path, with schema-versioned rebuild behavior when that embedded schema or dynamic template changes. | Make JSON-column index rebuilds visible through the same catalog work-unit and lifecycle machinery as other derived artifacts; expose typed JSON path selectors and transforms directly through API/SDK plans; and ensure SQL JSONB sugar never bypasses the relational row. |
| PostgreSQL adapter and corpus gates | Supported SQL lowers fail-closed into typed DDL, row-query, mutation, aggregate, join, CTE, and expression plans. | Generate a representative SQL and migration-equivalence corpus, record golden typed plans or intentional unsupported classifications, execute representative flows, and combine them with repair, range movement, FK checks, unique-owner movement, row claims, rewrite, and rebuild jobs in chaos/sim tests. |

The implementation order should keep correctness before breadth:

1. **Typed-plan contracts and corpus capture first.**
   Define stable structs and JSON forms for the Antfly plan shapes that the SQL
   adapter emits, then harvest representative SQL and native
   migration-equivalence steps into golden tests. Each runtime statement or
   migration step should be classified as supported with a typed plan,
   intentionally unsupported, or adapter-only no-op. This prevents
   broad parser work from drifting away from real workload requirements.

2. **Catalog lifecycle and repair work before planner trust.**
   Extend the existing schema-level lifecycle state and deterministic rebuild
   generation IDs for ordinary secondary indexes and embedded JSON-column
   derived indexes into catalog work-unit scheduling. Ordinary secondary indexes
   now use generation-checked, all-range gated promotion plus a raft-applied
   schema compare-and-swap command for the final lifecycle flip. Query planners
   may only use `ready` index generations; rebuilding or invalid indexes remain
   write-maintained but invisible to planning.

3. **One expression tree before more SQL syntax.**
   Move predicates, projections, generated columns, expression indexes, checks,
   aggregate filters, update transforms, `RETURNING`, `excluded` expressions,
   and ordering onto one typed expression AST. SQL features such as casts,
   `COALESCE`, JSONB extraction, array functions, arithmetic, and null-test
   ordering should become expression nodes, not isolated parser cases.

4. **Lockable mutation/query sources before queue scale.**
   Multi-row `UPDATE` and `DELETE`, `FOR UPDATE`, and `FOR UPDATE SKIP LOCKED`
   need explicit row-source, claim, ordering, and OCC semantics. Queue
   workloads should not be marked production-ready until ordered multi-range
   filling and range-movement chaos coverage are in place.

5. **Routed stream execution before advanced SQL composition.**
   Cross-range joins, aggregate streams, reused CTEs, lateral stages, and window
   stages should share one routed stream model with output schemas and
   memory/spill bounds. Routed lateral beyond local `LEFT JOIN LATERAL` and broader windows beyond local ranking/value functions should be planned
   as bounded stream stages, not as ad hoc SQL executor behavior.

6. **Chaos and repair gates before declaring parity.**
   The final acceptance gate is not just parser coverage. Sim/chaos workloads
   must combine live writes, composite PKs, FK validation/actions, unique-owner
   repair, ordinary index rebuilds, embedded JSON derived-index rebuilds, range
   movement, row claims, joins, aggregates, and typed rewrite/backfill jobs in
   one modeled run.

The highest-priority remaining gaps for PostgreSQL-shaped SQL are:

1. **DDL application, not just DDL lowering.**
   The lowerer now produces typed plans for common `CREATE TABLE`,
   `CREATE INDEX`, additive `ALTER TABLE`, generated-column, default, check,
   unique, FK, JSONB, array, and updated-at trigger shapes. Those plans now
   apply to owned runtime schema values and public relational schema JSON.
   `CREATE TABLE` creates a relational schema; `CREATE INDEX` mutates indexed
   column, generated-column, unique, expression, and partial-index metadata;
   `ALTER TABLE` appends and validates catalog changes; trigger-derived update
   policies set table-owned `x-antfly-on-update` metadata. Applied DDL returns
   rebuild/validation flags for catalog callers. The remaining production step
   is making table-catalog persistence, rebuild scheduling, validation jobs, and
   native migration-equivalence application transactional around that applied
   schema JSON, so non-additive migrations become visible rebuild jobs instead
   of hidden parser side effects.

2. **A single typed expression tree.**
   Current support covers many common scalar, JSONB, array, projection,
   generated-column, check, and conflict-update shapes, but some of that support
   is still represented as shape-specific lowerer paths. Production
   compatibility needs one
   reusable expression AST for predicates, projections, generated columns,
   check constraints, expression indexes, aggregate filters, `RETURNING`,
   `excluded` expressions, update transforms, and ordering. The AST carries
   parameter slots, field refs, literals, casts, deterministic functions,
   arithmetic, boolean structure, null semantics, and result type. Pushdown is
   an optimizer property of the AST, not a separate SQL parser feature.

3. **Mutation plans over lockable row sources.**
   Point `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT`, and `RETURNING` are
   represented in typed row-batch plans today. Local multi-row `UPDATE`/`DELETE`
   now has a typed mutation-source plan whose source is a claimed base-row query
   with order/limit, `SKIP LOCKED` selection, and committed-version OCC
   predicates, so it is not a hidden scan. Production coverage still
   needs routed/hosted mutation-source execution, durable multi-range fill, and
   broader expression support. Unique selectors resolve through durable owner
   rows before prepare; conflict updates evaluate `excluded` and table-owned
   update policies against the final planned row image; `RETURNING` projects
   from that committed image.

4. **Routed query execution beyond a single local stream.**
   Single-table row queries, partial/unique owner lookups, JSONB and array
   predicates, ordering, pagination, claims, aggregates, local joins, and
   non-recursive CTE lowering have model homes. Production support still
   needs routed cross-range/cross-table execution that uses durable table/range
   ownership metadata, merges ordered streams once, applies pagination once,
   preserves row-version visibility, and feeds routed streams into the existing
   typed join and aggregate executors.

5. **Lateral, window, and advanced aggregate stages.**
   routed `LEFT JOIN LATERAL` beyond the bounded local stage, broader window stages beyond local ranking/value windows, richer aggregate
   `FILTER`/`DISTINCT` expressions, and spill-backed distinct state are not
   storage primitives hidden behind SQL text. They should be explicit bounded
   stream stages: lateral is a correlated per-left-row query with required
   bounds; windows are ordered partitioned stream transforms; distinct
   aggregates declare memory/spill policy.

6. **Postgres adapter compatibility gates.**
   The adapter needs a generated corpus of representative SQL and migration
   SQL. Each statement should either lower to a stable typed Antfly golden plan
   or fail with an intentional unsupported-shape classification. Execution tests
   should cover representative flows: ledger writes, queue claims, RBAC
   membership, JSONB metadata updates, usage rollups, migrations, and
   backfills. Chaos/sim tests should combine these with live writes, FK checks,
   unique-owner movement, range movement, repair/rebuild, row claims,
   aggregates, joins, and server-owned update policies.

The remaining PostgreSQL-shaped SQL surface should be planned against Antfly primitives,
not as ad hoc Postgres passthrough:

| PostgreSQL/API surface | Antfly model home | Production plan |
| --- | --- | --- |
| `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, constraints, defaults, JSONB, arrays | Runtime schema catalog plus relational column, primary-key, unique, foreign-key, check, default, generated-column, and index metadata | `CREATE TABLE` now lowers to a typed schema plan for supported column, key, check, default, JSONB, array, and stored generated-column shapes. `CREATE INDEX` now lowers to typed index-plan metadata for ordinary, unique, partial, btree ordered column elements normalized to column membership, simple parenthesized/casted partial predicates, and `lower(field)` expression indexes. Additive `ALTER TABLE` now lowers `ADD COLUMN`, `ADD COLUMN IF NOT EXISTS ... REFERENCES ...`, `ADD CONSTRAINT` unique/FK/check operations, `NOT VALID`, and `VALIDATE CONSTRAINT` to typed catalog-plan metadata, including generated columns and unvalidated FK/check validation work. Known updated-at `CREATE TRIGGER ... BEFORE UPDATE ... EXECUTE FUNCTION touch_updated_at(...)` shapes now lower to table-owned `now_ns` update-policy metadata. Extend the same DDL boundary to broader trigger-derived policies, catalog mutation application, ordered-composite planner metadata, rebuild planning, non-additive native migration-equivalent rewrites, and later exact migration-file replay as an adapter layer; reject dump-only or unsupported syntax with stable adapter errors. |
| Partial indexes and partial unique indexes | Durable index/unique metadata with typed partial predicates | Normalize partial predicates through the scalar expression tree; use the same implication check for query pushdown, uniqueness enforcement, and `ON CONFLICT ... WHERE` inference. |
| Expression indexes and expression unique keys | Generated columns for simple stable expressions; typed expression-index metadata for the rest | Lower expressions such as `lower(email)` to generated columns when possible; otherwise persist deterministic expression ASTs with input columns, casts/functions, collation, null semantics, rebuild metadata, and write-time maintenance. |
| `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT ... DO UPDATE/NOTHING`, `RETURNING` | Row-batch mutation plans with primary/unique selectors, OCC predicates, transforms, conflict actions, table-owned update policies, and returning projections | Keep mutations typed end to end. `excluded.column` value reuse resolves through typed insert columns today, including cross-column reuse when source and destination types match. Updated-at policies compile to `x-antfly-on-update` today. The remaining work is full expression support over `excluded`, multi-row update/delete plans with explicit lockable row sources, broader mutation hooks, and returning expressions over the committed row image. |
| Point selectors and unique selectors | Structured row identity plus durable unique-owner lookup | Continue to expose primary-key and unique-key selectors as typed JSON identity, not physical row keys. Non-primary update/delete must resolve through unique-owner rows before prepare. |
| `WHERE`, `ANY`, `IN`, `NOT IN`, `NOT`, mixed `AND`/`OR`, casts/functions | Reusable typed scalar expression tree over rows, literals, parameters, and deterministic functions | Replace scattered scalar predicate special cases with a single expression plan that can evaluate over hydrated rows and advertise pushdown opportunities only when it maps to primary, unique, generated, column-major, array, or JSON indexes. |
| JSONB operators `->`, `->>`, `@>`, path exists, `jsonb_set`, concat/build, `convert_from(..., 'UTF8')::jsonb` | Typed JSON predicate, projection, construction, and transform nodes over declared `json` columns | Keep SQL operators as adapter sugar over JSON nodes. Reuse document-store JSON indexing only through explicit relational JSON column/index metadata, so schema changes rebuild the affected embedded JSON index deterministically. |
| Array operators, `ANY`, `array_length`, containment, defaults, `array_append`, `array_remove`, `string_to_array(...)` | Typed array predicate, projection, generated-column, function-expression, and transform nodes | Keep exact array equality, membership, containment, and append/remove/add-to-set transforms typed. Push down only through declared array indexes or generated-column indexes and evaluate the rest over hydrated rows. `string_to_array(...)` is a typed expression node for projections, aggregate inputs, exact computed-array equality/inequality, and computed-array containment predicates; pushdown for computed arrays comes through generated columns or later expression-index metadata. |
| `ORDER BY`, `LIMIT`, `OFFSET`, null-test order expressions | Executable typed order keys and pagination over row, join, aggregate, CTE, or window streams | Keep field, null-test, and scalar expression order keys as typed plan nodes. Pagination is applied after authoritative filtering for a single ordered stream or after coordinator merge for multi-range streams. |
| `FOR UPDATE` and `FOR UPDATE SKIP LOCKED` | Lockable base-row query plans with row-claim metadata | Keep claims limited to base-row streams. Add ordered multi-range filling, durable claim ownership, OCC integration, and chaos coverage with range movement before allowing queue workloads at production scale. |
| Equality joins, `LEFT JOIN`, `LEFT JOIN LATERAL` | Typed join plans plus bounded local correlated lateral subquery stages | Keep local equality joins as row-stream joins. Add routed cross-table/range joins using durable ownership metadata and lookup/hash/merge strategy choice; extend lateral joins as explicit bounded per-left-row subqueries over routed right-side plans. |
| CTEs | Ordered named typed subplans and bounded materialized streams | Track output schemas across row, join, and aggregate plans; inline one-use CTEs when safe; spill or reject reused materializations that exceed declared memory bounds. |
| Aggregates, `FILTER`, `DISTINCT`, `array_agg`, `HAVING` | Typed aggregate plans with per-aggregate predicate filters, executable expression filters, expression inputs, distinct state, ordering, and output predicates | Extend JSON/array and complex boolean aggregate filters/distinct keys over the shared expression tree, add spill-backed distinct state beyond the current explicit cap, and keep ordered collection aggregates bounded. |
| Window functions | Typed local `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` stages over ordered base-row or materialized CTE streams | Add richer frame metadata, additional frame-aware functions, routed partition execution, and memory/spill bounds. Use window stages for migration/backfill ranking, previous/next-row value projection, and wake-one job selection rather than folding windows into aggregate or join nodes. |
| Triggers and server-maintained columns | Explicit table-owned server-update policies and mutation hooks | Do not emulate PL/pgSQL trigger execution. Updated timestamps use `x-antfly-on-update` metadata today. Compile any remaining known trigger patterns into typed policies for denormalized JSON/array fields, action-job creation, or other deterministic mutation hooks. |

The remaining PostgreSQL/API work should land in these model-level slices:

1. **SQL frontend and corpus gate.**
   Keep SQL parsing at the adapter boundary. The frontend resolves schemas,
   aliases, placeholder positions, parameter types, result column names,
   SQLSTATE-compatible errors, and `pgx` wire expectations before producing an
   Antfly plan. Add a generated SQL corpus test that records representative SQL
   strings plus every native migration-equivalence step,
   normalizes placeholders, and asserts either a stable Antfly golden plan or an
   intentional unsupported classification with an owner and reason. This gate
   should run before broad syntax expansion so new support is measured against
   the real workload.

2. **DDL and migration compiler.**
   Compile native migration intent into Antfly schema mutations rather than replaying
   Postgres internals. Supported `CREATE TABLE` DDL now lowers to a typed schema
   plan covering relational columns, primary keys, unique constraints,
   table-level and inline-column foreign keys, `CHECK`, `DEFAULT`, JSONB,
   arrays, UUID/timestamp defaults, and
   PostgreSQL scalar type aliases. Supported `CREATE INDEX` DDL now lowers to
   typed index plans for ordinary columns, btree `ASC`/`DESC` and `NULLS`
   clauses normalized to column membership, unique constraints, simple
   parenthesized/casted partial predicates, and `lower(field)` expression keys.
   Additive `ALTER TABLE` DDL
   now lowers `ADD COLUMN`, `ADD COLUMN IF NOT EXISTS ... REFERENCES ...`, and
   `ADD CONSTRAINT` unique/FK/check operations to the same typed catalog-plan
   boundary. Inline references on added columns become an `add_column` operation
   followed by an unvalidated `add_foreign_key` operation, so catalog apply adds
   the row cell first and then hands FK validation/promotion to the existing
   schema-controller lifecycle. Stored generated-column DDL for
   `lower(field)` and simple `concat(field, separator, field...)` expressions now
   lowers to durable generated-column metadata in both `CREATE TABLE` and
   additive `ALTER TABLE ADD COLUMN` plans. Known updated-at trigger DDL now
   lowers to a typed update-policy plan carrying the target table, trigger name,
   target column, and `now_ns` policy. Supported DDL plans now apply to public
   relational schema JSON and report whether rebuild or validation work is
   required. Additive foreign keys enter the catalog as `unvalidated`, which
   makes validation durable immediately and lets the foreign-key schema
   controller perform repair/validation before promoting the constraint to
   enforced. Additive unique constraints and `CREATE UNIQUE INDEX` on existing
   tables enter the catalog as `unvalidated`; normal writes ignore those unique
   constraints for owner-range routing and uniqueness enforcement until a
   validation/promote worker has rebuilt and proven their owner rows. Local
   unique schema-controller maintenance now performs that repair, follow-up
   validation, and promotion path for local tables. Catalog-backed
   provisioned/hosted metadata rounds now reuse the same worker contract across
   table groups and promote valid unvalidated unique constraints through durable
   schema compare-and-swap updates. Ordinary non-unique indexes now get
   schema-level lifecycle state: existing-table `CREATE INDEX` writes `building` metadata
   with a deterministic rebuild generation, foreground writes keep maintaining
   the side rows, and planners ignore the index until it is promoted to `ready`.
   The metadata reconciler now derives those ordinary secondary-index rebuild
   ranges from the same catalog mutation boundary, assigns them data-group
   placement, preserves in-flight lease/progress state, and removes stale
   generations once the schema no longer declares a building index. Workers now
   execute those ordinary secondary-index rebuild ranges and promote only after
   the catalog shows every table range ready for the exact building generation;
   stale ready records for an older generation cannot make a newer schema
   queryable. Final ready promotion is now a metadata compare-and-swap against
   the exact observed schema bytes, so concurrent schema updates make stale
   promotion proposals no-op. Non-additive migrations such
   as drops, type changes, backfilled `NOT NULL`, and validation transitions
   should become explicit rebuild/validation plans rather than hidden side effects. Broader
   trigger/function patterns must either compile to explicit mutation-hook
   metadata or fail as unsupported. `CREATE EXTENSION`, PL/pgSQL helper
   functions, dump-only syntax, and Postgres catalog bookkeeping are adapter
   concerns that lower to explicit metadata or are ignored only when proven
   semantic no-ops. Golden migration-equivalence tests should compile intended
   final schema and data-change effects and compare the
   produced catalog JSON, validation work, rebuild work, and rewrite jobs. Exact
   migration-file replay can later be tested in the PostgreSQL adapter by
   lowering into the same native plans.

3. **Typed scalar expression tree.**
   Generalize the current typed projection/predicate pieces into a reusable
   expression tree with explicit input columns, literals, parameters, casts,
   deterministic functions, arithmetic, boolean operators, null tests, and
   result types. `COALESCE`, `LOWER`, `UPPER`, `id::text`, `NOW()`, `DEFAULT`,
   `convert_from(..., 'UTF8')::jsonb`, `(expires_at IS NULL)`, `NOT`, and mixed
   `AND`/`OR` combinations should all normalize through this tree. Expressions
   used for storage semantics normalize to generated columns, expression-index
   metadata, check constraints, or unique-expression keys. Expressions used only
   for projection, filtering, ordering, aggregate filters, or update transforms
   can be evaluated over hydrated row streams, with pushdown only when they map
   to a declared primary, unique, generated, column-major, array, or JSON access
   path.

4. **Index and conflict-target completeness.**
   Keep ordinary, partial, and expression indexes as durable typed metadata.
   Simple expression indexes such as `lower(email)` can compile to stored
   generated columns plus normal unique or secondary indexes when that gives the
   best rebuild and pushdown behavior. More complex deterministic expression
   indexes use typed expression-index definitions with declared input columns,
   function/cast nodes, collation rules, null handling, and optional partial
   predicates. `ON CONFLICT (cols...) [WHERE ...]` inference should share the
   same partial-predicate and expression-key normalizer as unique enforcement,
   so conflict handling and write-time uniqueness cannot drift.

   The long-term index lifecycle uses the same shape as the FK controller work:
   catalog DDL writes schema metadata first, marks rebuild or validation work
   explicitly, and then background workers claim deterministic table/range work
   units. Unique constraints are already protected by durable validation state.
   Existing-table unique additions stay `unvalidated` until owner rows have been
   repaired and then validated over the required range set. Foreground writes
   ignore unvalidated unique constraints, so partially rebuilt owner rows cannot
   reject valid relational writes or route conflict targets through incomplete
   metadata. Promotion is a catalog update from `unvalidated` to `enforced`
   after terminal valid validation, followed by normal owner-range derivation and
   write-time uniqueness enforcement. Ordinary non-unique secondary indexes need
   the same lifecycle flags and range work units, but they do not need an
   enforcement gate; queries can use only indexes whose catalog state is
   `ready`, while rebuilding indexes remain invisible to planners.

   Production work for this track is:

   1. Catalog mutation scheduling now emits secondary-index rebuild work ranges
      from schema-level `building` lifecycle and deterministic generation
      metadata, using the table's current row ranges as the rebuild partition
      set and preserving in-flight work across reconcile rounds.
   2. Local range execution now repairs one building secondary-index generation
      by deleting only the target column's stale side rows inside `[start,end)`
      and re-appending current matching rows through the same write-maintenance
      policy used by foreground writes.
   3. Metadata claims now reject busy leases, reclaim expired `building` leases,
      and the local worker helper claims a work record, calls the local execution
      primitive, finishes on success, or invalidates on storage failure.
   4. Provisioned/hosted table-write sources now select claimable
      secondary-index rebuild ranges from catalog snapshots, call the local
      execution helper on the owning table group, mutate metadata through the
      catalog abstraction, forward remote hosted group-local work through an
      internal table-write route, and aggregate progress.
   5. Reuse the schema compare-and-swap shape for expression-derived index
      promotion. Ordinary secondary-index promotion and unique validation
      promotion now carry the expected schema and promoted table image through
      raft-applied CAS commands, so stale promotion proposals no-op instead of
      overwriting concurrent schema changes.
   6. Make row-query planning consult lifecycle state: `ready` secondary indexes
      are eligible, `building` indexes are ignored, `invalid` indexes report
      diagnostics, and primary/unique owner routing uses only enforced unique
      constraints.
   7. Add chaos coverage that interleaves live writes, range movement, unique
      repair/validation, secondary-index rebuild, catalog promotion, and
      conflict-target upserts.

5. **DML and update transform hardening.**
   The lowerer should continue to compile `INSERT`, `UPDATE`, `DELETE`, `ON
   CONFLICT ... DO NOTHING`, `ON CONFLICT ... DO UPDATE`, and `RETURNING` into
   row-batch operations with explicit primary/unique selectors, expected-version
   predicates, transforms, and returning projections. Local mutation-source
   plans now provide explicit lock/claim semantics for update/delete scans over
   base-row query sources. The remaining long-term shape adds routed/hosted
   mutation-source execution, full expression resolution over `excluded`,
   broader server-update policies beyond table-owned updated-at `now_ns`,
   broader array expression transforms beyond literal/parameter
   append/remove/add-to-set, and broader generated/check/default expression
   reuse across mutation paths. Point and mutation-source `RETURNING`
   expressions already project over committed insert/update images and
   pre-delete images through the shared row-expression AST. Non-primary
   update/delete selectors must resolve through durable unique-owner lookup
   before prepare; no hidden table scan should mutate rows.

6. **JSONB and array SQL sugar.**
   Keep JSONB and array semantics in typed operators. `col -> 'k'`, `col ->>
   'k'`, `col @> $json`, path existence, `jsonb_set`, `jsonb_build_object`,
   JSONB concatenation, and `convert_from(..., 'UTF8')::jsonb` lower to typed
   JSON projection, predicate, construction, or transform nodes. `ANY`, `IN`,
   `NOT IN`, equality, containment, `array_length(col, 1)`, array defaults, and
   `string_to_array(scope, ' ') @> $json_array` lower to typed array
   membership, equality, containment, computed-array containment, scalar
   function, or generated-column plans. Operators
   that require indexes or write-time maintenance become explicit catalog
   capabilities; purely syntactic sugar stays in the adapter.

7. **Joins, lateral joins, and routed execution.**
   Equality `INNER` and `LEFT` joins lower to `RelationalRowsJoinPlan` with
   declared sources, equality keys, join type, output projection, and bounded
   execution strategy. Cross-table joins must route each side through durable
   table/range ownership metadata, retain row-version visibility rules, and
   choose lookup, hash, or merge execution from typed cardinality/index hints.
   `LEFT JOIN LATERAL` is a separate correlated-subquery stage: for each left row
   it executes a bounded right-side query with explicit `ORDER BY`, `LIMIT`, and
   required index path. It should not be hidden inside the ordinary join planner.

8. **CTEs and materialized stream bounds.**
   Non-recursive `WITH` clauses compile to ordered named typed subplans and the
   PostgreSQL adapter lowers the single-table row-query form into
   `RelationalRowsQueryPlan`. The remaining production work is richer CTE output
   schema tracking across aggregate/join plans, optional inlining for CTEs used
   once, and explicit memory/spill bounds for reused materializations. Recursive
   CTEs remain unsupported until there is a separate graph/fixpoint plan node;
   they should not be approximated with repeated SQL evaluation.

9. **Aggregates, windows, and ordered streams.**
   `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `GROUP BY`, `HAVING`, aggregate
   ordering, and aggregate pagination lower to `RelationalRowsAggregatePlan`.
   Scalar `FILTER (WHERE ...)` clauses are represented as per-aggregate
   predicate trees and evaluated before each metric updates. Scalar `DISTINCT`
   aggregate specs keep per-group/per-metric distinct state before updating the
   aggregate metric. Aggregate input expressions such as
   `COUNT(DISTINCT lower(status))` and numeric `SUM(amount - discount)` execute
   through the same row-expression evaluator used by projections and order
   keys. Scalar `array_agg` specs declare a maximum materialized
   output count plus optional aggregate-local typed ordering, so the coordinator
   never creates an unbounded or scan-order-dependent collection aggregate by
   accident. Remaining aggregate expression work is structured JSON/array and
   complex boolean distinct/filter keys plus spill-backed continuation beyond
   the explicit in-memory cap.
   Window functions such as
   `row_number() OVER (PARTITION BY ... ORDER BY ...)`,
   `rank() OVER (...)`, `dense_rank() OVER (...)`, `lag(expr, offset, default)
   OVER (...)`, and `lead(expr, offset, default) OVER (...)` are separate typed
   window stages over ordered materialized streams; they are needed for
   migration/backfill ranking, previous/next-row value projection, and wake-one
   job selection.

10. **Order, limit, offset, and row claiming.**
   `ORDER BY`, `LIMIT`, and `OFFSET` lower to typed order keys and pagination
   over either base row streams, join outputs, aggregate outputs, or CTE
   materializations. Order expressions such as `(expires_at IS NULL)`,
   `lower(status)`, `upper(status)`, and numeric arithmetic are typed scalar
   order expressions.
   `FOR UPDATE` and `FOR UPDATE SKIP LOCKED`
   lower only over base-row streams that can claim durable row ownership. They
   remain rejected over aggregate, join, CTE-materialized, graph, and count-only
   sources until there is an explicit lockable row-source contract. Queue claims
   need ordered multi-range filling, durable claim ownership, OCC integration,
   and chaos coverage with range movement.

11. **Execution and chaos gates.**
   Add execution tests for billing ledger writes, queue claims, RBAC membership
   queries, JSONB metadata updates, usage rollups, and migration/backfill
   queries. Add chaos/sim coverage that combines live writes, foreign-key
   checks, unique-owner movement, range movement, repair/rebuild, row claims,
   aggregate reads, join reads, and server-update policies in one modeled
   workload.

12. **Postgres adapter surface and typed-plan ownership.**
   The backend should not pass SQL text after the adapter boundary. SQL-facing
   APIs parse once, bind against catalog metadata, and produce typed Antfly DDL,
   query, mutation, aggregate, join, CTE, expression, and repair plans. REST and
   SDK APIs should be able to construct the same plans directly without SQL.
   That keeps SQL/API compatibility from becoming a Postgres compatibility layer
   inside storage. The adapter owns placeholder numbering, SQLSTATE-shaped
   errors, result-column labels, and PostgreSQL syntax sugar. Storage owns only
   the typed plan contract, catalog state, range routing, OCC/2PC behavior, and
   deterministic repair/rebuild semantics.

   Production work for this track is:

   1. Define stable JSON/typed structs for the public plan shapes that the
      adapter emits:
      row selectors, composite identities, expression trees,
      DDL mutations, row-batch transforms, query sources, joins, aggregates,
      CTEs, and lockable row claims.
   2. Keep the SQL parser/lowerer as a frontend module whose output is compared
      against those typed plans in golden tests.
   3. Add direct REST/SDK entry points for the same typed plans, especially
      composite primary-key selectors, named unique conflict targets, JSONB path
      transforms, array transforms, and aggregate/query plans.
   4. Reject unsupported SQL before storage execution with structured
      unsupported-shape errors that include the syntax feature, reason, and
      nearest Antfly typed-plan equivalent when one exists.
   5. Gate changes with the representative SQL and migration-equivalence corpus, so
      every new supported SQL shape or native schema/data-change step has either
      an equivalent typed API shape or an intentional reason for remaining
      SQL-adapter-only.

SQL compatibility should not be declared complete just because many individual
Postgres-shaped statements now lower. The completion bar is that every core
SQL family has a typed Antfly plan, an API/SDK shape,
transactional catalog/runtime behavior, and evidence from representative
corpora. Given the composite-identity, foreign-key, unique-owner, embedded-JSON,
secondary-index lifecycle, and 2PC work in this PR, the remaining core SQL plan
is:

| SQL family | Current Antfly baseline | Remaining long-term work | Completion signal |
| --- | --- | --- | --- |
| SQL capture and adapter boundary | Supported statements fail closed into typed DDL, row-query, mutation, aggregate, join, CTE, and expression plans; unsupported syntax is not passed to storage. Seeded corpus entries now pin typed-plan fingerprints for representative supported and unsupported shapes. | Expand the representative SQL and migration-equivalence corpus, normalize placeholders and aliases, bind each runtime statement and native migration step against Antfly catalog snapshots, and store golden typed plans or explicit unsupported classifications. | Every harvested runtime statement and migration-equivalence step is classified; adapter-only no-ops are named; unsupported shapes include the model feature they need before they can run. |
| Schema and migrations | Supported `CREATE TABLE`, `CREATE INDEX`, additive `ALTER TABLE`, defaults, checks, generated columns, JSONB/array columns, foreign keys, unique constraints, partial indexes, expression indexes, btree `ASC`/`DESC` and `NULLS` index element syntax, simple parenthesized/casted partial-index predicates, `NOT VALID` constraint additions, `VALIDATE CONSTRAINT`, and updated-at trigger shapes lower to typed catalog plans. | Apply native migration-equivalent plans transactionally through catalog schema JSON, schedule validation/rebuild work for every non-empty-table derived artifact, and model non-additive changes as rewrite/rebuild jobs because the relational format starts here. Ordered index element clauses are accepted at the adapter boundary today and normalized to column membership; durable ordered-composite access-path metadata should land with routed ordering/planner work. Simple check/FK validation state is durable today; richer check expressions still need the common expression tree before they can compile every PostgreSQL dump-style check into a native plan. | Intended final schema and migration effects compile into catalog state; all rebuild/validation/rewrite work is durable; dump, extension, PL/pgSQL, or unsupported trigger syntax is either proven no-op adapter compatibility or rejected before storage. |
| Point CRUD and conflict upserts | Point `INSERT`, `UPDATE`, `DELETE`, primary/unique `ON CONFLICT`, `excluded.column`, numeric conflict deltas from `excluded` values, defaults, `NOW()`, updated-at policies, JSONB/array transforms, and simple `RETURNING` have row-batch model homes. | Move remaining conflict actions and `RETURNING` onto the shared expression tree, evaluate them over the final committed row image, and route every non-primary selector through durable unique-owner lookup before prepare. | Ledger, RBAC, auth, billing, and seed flows run from typed row-batch plans without raw SQL or hidden scans. |
| Multi-row DML and queue claims | Base-row queries can carry row-claim metadata, `FOR UPDATE [SKIP LOCKED]` lowering exists for lockable sources, and local mutation-source update/delete stages claimed rows in the owning transaction with OCC predicates. | Add routed multi-range fill, durable claim ownership beyond local transaction intents, lease/retry semantics, and hosted participant registration. Keep claims illegal over joins, aggregates, windows, and materialized CTEs until those stages expose lockable base rows. | Queue, re-encryption, cleanup, and batch update/delete flows do not double-claim, miss rows after range movement, or mutate rows without a lockable source contract. |
| Scalar expressions | Many common predicates, text-pattern predicates, projections, casts, null tests, generated expressions, JSON/array predicates, expression order keys, aggregate filter expressions, aggregate input expressions, and returning expressions lower through typed paths. | Replace remaining scattered lowerer cases with one typed expression AST for predicates, projections, checks, generated columns, expression indexes, conflict actions, update transforms, aggregate filters/inputs, order keys, and `RETURNING`. Planner pushdown becomes an optimizer property of that AST. | Every supported expression is type-bound once at the adapter boundary and can be executed through REST/SDK plans as well as SQL. |
| JSONB and arrays | Declared `json` and `array` columns have typed predicates/transforms, and embedded JSON columns have a document-index-in-row design. | Route remaining SQL operators through typed JSON/array expression nodes, expose the same selectors/transforms directly through API/SDK plans, and run embedded JSON schema/template changes through catalog rebuild work. | JSONB metadata, array membership, rollup metadata, and embedded document-index queries work without bypassing the relational row store. |
| Indexes and planner trust | Ordinary secondary indexes have lifecycle state and rebuild generations; unique constraints have validation state; simple partial and `lower(field)` expression index metadata is modeled; planners use only `ready` secondary indexes. | Put partial predicates and expression keys onto the common expression tree, share implication logic across pushdown, uniqueness, and `ON CONFLICT ... WHERE`, and route expression-derived rebuild/promotion through the same generation/CAS lifecycle as ordinary indexes. | Planner choices depend only on ready/enforced catalog metadata; stale, building, or unvalidated derived artifacts cannot affect query answers or write enforcement. |
| Joins and routed reads | Local equality `INNER`/`LEFT` joins, non-recursive CTE lowering, native ordered CTE plan parsing, scalar aggregate plans, grouping, `HAVING`, ordering, and pagination have typed plan homes. | Add cross-table/cross-range routed stream execution with output schemas, row-version visibility, coordinator merge ordering, lookup/hash/merge strategy choice, and bounded CTE materialization. | Billing, usage, RBAC, and dashboard reads execute with correct pagination and ordering across range movement and live writes. |
| Lateral, windows, and advanced aggregates | Bounded scalar aggregates, scalar `FILTER`, expression aggregate filters/inputs, scalar `DISTINCT`, ordered capped `array_agg`, bounded local `LEFT JOIN LATERAL`, and local `row_number()`, `rank()`, `dense_rank()`, `lag()`, and `lead()` over ordered/partitioned base or CTE streams are modeled. | Add routed lateral execution, broader window frame semantics as partitioned ordered stream stages, routed window execution, and spill-backed aggregate/window distinct/filter state over JSON/array/complex expression keys. | Migration/backfill, wake-one, dashboard, and rollup queries either run through bounded typed stages or fail closed with a replacement plan. |
| Production evidence | Existing lowerer and runtime tests cover focused features; sim/chaos work exists for core storage behavior. | Add a SQL/API compatibility suite that combines corpus golden plans, representative execution flows, live writes, FK checks/actions, unique-owner repair, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and typed rewrite/backfill jobs. | SQL/API compatibility is gated by workload evidence, not parser breadth. |

The production milestones are:

1. **CRUD and queue SQL.**
   Compile and apply the live schema subset, support point reads, inserts, updates, deletes,
   conflict-target upserts, `RETURNING`, JSONB set/extract/containment, array
   membership checks, ordered pagination, scalar expression lowering for common
   predicates, and `FOR UPDATE [SKIP LOCKED]`.

2. **Schema and migration parity.**
   Produce the same schema and data effects as the migration history using
   native Antfly catalog, validation, rebuild, and rewrite plans. This includes
   composite primary keys, foreign keys, partial and expression indexes,
   defaults, checks, generated columns, JSONB/array columns, server-update
   policies, and catalog projection needed by the adapter. Exact migration-file
   replay can later lower into these same plans.

3. **Reporting and rollup SQL.**
   Complete routed equality joins, aggregate dashboards, structured aggregate
   filters, spill-backed distinct aggregate state, richer CTE output schemas, and
   execution tests for usage rollups and RBAC membership queries.

4. **Migration/backfill advanced SQL.**
   Add routed `LEFT JOIN LATERAL`, broader typed window stages beyond local ranking/value functions, complex
   expression ordering, recursive-CTE rejection with explicit alternatives, and
   bounded materialized-stream execution for migration/backfill jobs.

5. **Production compatibility hardening.**
   Keep the SQL corpus, migration-equivalence golden tests, execution tests, and
   chaos/sim suites as release gates. New Postgres-shaped syntax lands only when
   it has a typed Antfly plan, deterministic error behavior, and coverage for
   routing, repair, and concurrent writes.

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
- **Column access:** document-scoped column entries are maintained alongside the
  row entry for range movement and delete cleanup, and column-major scan entries
  (`column_path -> doc_key`) are maintained from the same packed row for scalar
  predicate scans when the column catalog marks the path `indexed`. Both are
  secondary to the packed row and can be regenerated from it. `indexed: false`
  suppresses only the column-major scan entry; the packed row and
  document-scoped cell remain authoritative, and filters over that column fall
  back to base-row evaluation.
- **Commit boundary:** prepare/commit/abort/read/scan methods participate in the
  existing two-phase commit and recovery path, so committed rows, column
  entries, and deletes resolve at the same visibility point.

The row entry keeps point lookups and read-modify-write transforms as one atomic
document operation. Column-major entries make scalar scans independent from
derived search segments without scanning every document-local column key.
Document-scoped entries keep split/merge movement and overwrite/delete cleanup
tied to the row's document range. A reverse scan-entry namespace keyed by
`doc_key -> column_path` is maintained with column-major entries so split
finalization can delete outgoing secondary scan entries by document range
without scanning every column index in the shard. A future physical optimization
can pack column-major entries into larger column blocks without changing the
participant boundary.

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
reload stage the affected domain as `rebuild_required` before the runtime schema
is durably exposed. Rebuild completion is gated on the durable schema version
matching the staged algebraic capability; schema-versioned domains remain
pending when no durable runtime schema has been adopted yet. Algebraic query
planning withholds fields below that JSON column until the sidecar is rebuilt
from committed rows. The local rebuild is serialized with DB apply work and
replays relational base rows in bounded, resumable batches using the same
`rebuild.state` cursor as other algebraic schema rebuilds, so large relational
tables do not require whole-range materialization and interrupted rebuilds
continue from the last applied row. Successful local rebuild clears the domain
back to `current`. Full-text JSON projection follows the existing
schema-versioned index handoff.

By contrast, changing the relational base-column catalog is a storage migration.
Schema updates that switch storage mode or add, remove, rename, retag, or change
nullability/indexed status for a relational base column are rejected until an
explicit row rewrite or secondary-index rebuild path exists. This keeps the
committed packed rows, column entries, and runtime schema in one physical shape.
Derived-only changes below a stable `json` column remain allowed and flow
through the rebuild lifecycle above.

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

Relational table creation and same-catalog relational schema updates both run
the same schema-aware index preparation: if no algebraic index exists,
`algebraic_index_v0` is added with `derive_from_schema: true` and stored as a
concrete derived config. Storage-mode switches are not schema updates; they need
an explicit row migration path.

### Query and movement invariants

Structured relational filters for supported keyword/range/bool/geo clauses
resolve against relational column-major scan entries when the column is indexed.
For `indexed: false` columns, the same filters evaluate against committed packed
base rows, which is slower but preserves correctness. Top-level supported
relational structured queries become base-row doc constraints over the text
match-all path, so scalar query results follow the committed relational row
rather than stale segment doc-values. Unsupported text-oriented shapes may still
use the inverted text index, but not segment doc-values as a relational column
source.

Relational column pushdown is only valid at the current identity generation,
because relational rows and column entries are the committed current row image,
not a historical value log. A stale generation must fall back to a
generation-aware source or decline pushdown rather than filtering with current
column values and historical identity visibility.

Predicates under a `json` column route through the embedded document-derived
index for that column path, then intersect with top-level relational column
filters by document id. Result materialization still reconstructs from the
relational base row.

For example:

```text
tenant_id:acme AND attrs.plan:pro AND attrs.score:[10 TO *]
```

`tenant_id` uses relational column-major scan entries. `attrs.plan` and `attrs.score` use
the embedded JSON artifacts scoped to the `attrs` column. The final hit list is
materialized from relational rows, not from derived artifacts.

Split, merge, replay, TTL cleanup, and generated-enrichment replay treat the
relational base store as table data. Split prepare/finalize moves relational row
entries and document-scoped column entries into the destination range, rebuilds
destination column-major scan entries from the destination's packed rows using
the schema's `indexed` policy, and prunes parent scan entries through the
reverse `doc_key -> column_path` scan-entry namespace only after the
authoritative parent range has been rewritten. Stale parent scan entries are
safe across crash/retry because scan reads use the secondary entry only as a
candidate document id, then hydrate the returned value from the current base
relational row. Merge-style range cutover replays donor logical writes into the
receiver's active range, where the relational participant regenerates receiver
rows and column entries through the normal commit boundary; donor fencing rejects
post-cutover writes outside the donor's remaining range. Rollback first persists
a `rolling_back` marker that fences off any further bootstrap or replay work,
then deletes donor logical ids decoded from every stored row key kind, purges
relational column-scan entries for the donor range, restores the receiver's base
range, and clears donor replay progress. The `rolling_back` path is
intentionally idempotent so a crash or failed cleanup step can be retried after
reopen until the receiver persists `rolled_back`.

Physical data movement uses the internal table-data classifier rather than
assuming every table datum lives under the document-range `0x01` namespace.
Document-scoped rows, document-scoped column entries, artifacts, and the
column-major scan namespace are all physical table data; replay and identity
metadata are separate internal metadata. Code that moves, deletes, or exports
physical table state must either handle every table-data namespace or explicitly
regenerate the secondary namespace from packed rows.

Native backups are physical snapshots and preserve relational rows plus their
secondary scan entries. The portable logical AFB path is not relational-aware in
this feature set and is rejected for relational physical rows rather than
silently exporting an empty document set. A future portable path should be
schema-aware: materialize packed relational rows into logical documents during
export and restore them through the DB write path so row, column, and derived
state are regenerated consistently.

### Relational storage baseline

Relational mode starts from the relational participant keyspace. Generic primary
document rows for relational ids are treated only as invariant cleanup state;
relational readers must not use a generic document KV value as row data.
Document-mode KV values remain JSON blobs and are preserved exactly.

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
  generic primary document key, plus secondary column entries maintained by the
  same participant. This keeps `DB.get`, transforms, lookup, vector
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
- foreign-key child writes, parent-delete rejection, reverse-reference cleanup,
  and same-transaction parent/child create-delete behavior;
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
`include_stored` hydration from base rows, scan-based aggregations over base-row
`stored_data`, scalar filters over column scan entries, transaction
commit/abort/transform behavior, stale generic-primary cleanup, split movement,
stale secondary scan-entry hydration from current base rows, and merge-style
range cutover plus resumable rollback for relational row and column entries
across reopen.

## Related docs

- [SCHEMA.md](SCHEMA.md) — schema contract and compiled runtime schema
- [ALGEBRAIC.md](ALGEBRAIC.md) — fact projection, materializations, folds
- [JOINS.md](JOINS.md) — relational join planner and distributed execution
