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
entries used for movement, cleanup, and scalar scans. There is no legacy JSON
primary-row mode for relational tables in this feature set.

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
public row identity. `insert` and `upsert` remain primary-key based unless a
future explicit conflict target is added; a later SQL DSL can compile
`ON CONFLICT (unique_col...) DO UPDATE` into that explicit path without changing
current `upsert` semantics. `rows:get` accepts an array of primary or unique
selectors and returns the structured identity, row JSON, version, and optional
`physical_key`.

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

### SQL compatibility boundary

Relational mode is the storage and API substrate that a SQL DSL can target. It
is not itself a PostgreSQL wire server, SQL parser, migration runner, or PL/SQL
runtime. PostgreSQL-specific compatibility work such as `pgx` protocol behavior,
`golang-migrate` replay, extensions, triggers, PL/pgSQL functions, and exact
PostgreSQL DDL syntax belongs in an adapter layer above this model.

The useful compatibility split is:

- **Adapter-level PostgreSQL compatibility:** wire protocol, SQL text parsing,
  PostgreSQL catalog views, migration-file replay, extension emulation,
  PostgreSQL-specific DDL syntax, PL/pgSQL, trigger/function compatibility, and
  exact error-code/message mapping. These can sit above the Antfly API and do
  not need to change the relational storage model.
- **Model-level SQL semantics:** the durable row, index, constraint, mutation,
  and query contracts that multiple frontends can target. These belong in
  relational mode itself because the engine must enforce them consistently for
  SQL, REST, SDK, and internal callers.

The remaining model-level work for a Colony-shaped SQL surface is below. These
items should be implemented as explicit API/runtime contracts first, then mapped
from SQL syntax by an adapter.

#### Partial and expression indexes

Secondary indexes and unique constraints need optional predicates plus
expression keys:

```json
{
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
"eq", "value": "active" }`. Schema validation checks each expression,
dependency tracking records the base columns it reads, writes evaluate
expression values from the committed row, and index/unique-owner maintenance
uses those computed values in the same 2PC path as ordinary column indexes.
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
  "returning": ["tenant_id", "id", "status", "updated_at"]
}
```

The row API exposes this as a typed `returning` field on each mutation, for
example `{ "returning": ["tenant_id", "id", "status"] }` or
`{ "returning": ["*"] }`. The result is emitted as a `returning` array on the
batch response. Inserts project the proposed row image. Updates resolve the base
row, apply the same transform logic used by storage, and project the post-image.
Deletes project the resolved pre-delete row image. Update/delete returning adds a
version predicate for the row image that was projected, so the commit either
installs/removes the value represented in the response or fails with an OCC
conflict. The projection is deliberately based on row JSON/relational row state
captured for the mutation, not a derived index read after commit.

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
as `transaction_id`/`txn_id` hex at the API boundary. Storage lowers each claimed
base row to a metadata key under `txn_row_claim:<row-key>` and writes a pending
2PC delete intent for that key. The intent is the lock: competing claimers and
ordinary writers both hit the same transaction-manager conflict check. Ordinary
batch and transaction mutations add an expected-absent predicate on the row claim
key, so a pending claim blocks direct writes, transactional writes, updates, and
deletes until the claiming transaction commits or aborts. Transaction resolution
always consumes row-claim intent keys instead of applying them as user data, so a
commit releases the lock rather than leaving a durable row-claim record behind.

`skip_locked: false` claims all returned base rows or fails with the underlying
intent conflict. `skip_locked: true` attempts claims in result order and returns
only the rows whose row-claim intents were installed. Row claiming is deliberately
base-row only: `count_only`, graph result sets, and chunk return modes are
rejected because they do not identify a single relational row to lock. SQL
`SELECT ... FOR UPDATE [SKIP LOCKED]` and API queue consumers should both compile
to this same row-claim contract. The remaining distributed planner work is to
make range-targeted query execution over `ORDER BY`/`LIMIT` continue scanning
past skipped rows across owner ranges so `SKIP LOCKED LIMIT n` can fill `n` rows
without over-fetching a whole table.

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

`ANY`-style predicates lower to the structured stored-doc filter shape:

```json
{ "array_any": { "field": "tags", "value": "hot" } }
```

The predicate compares the requested value as a typed JSON value, not as a SQL
string fragment, and it also works over nested multivalue paths such as
`{ "array_any": { "path": "items.sku", "value": "sku-2" } }`.

Declared indexed array columns write value-keyed element index entries beside
the ordinary column scan entry:

```text
array_element_index(column_path, canonical_element_value, doc_key) -> ""
```

`array_any` resolves through that element index when the column is indexed, then
hydrates the base row to prove the row still contains the element before building
the doc set. Columns declared with `x-antfly-index: false` keep the same typed
array validation and canonical row storage but resolve `array_any` by scanning
base rows instead of writing element index entries. The remaining production work
is planner costing and broader SQL operator coverage: canonical ordering/equality
rules, explicit null/empty semantics, nested multivalue path indexes, and
catalog-declared order-sensitive versus set-like comparison for arrays that
participate in unique constraints or generated expressions. Fully schemaless
arrays can still live inside a `json` column.

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

Extraction (`->`, `->>`), containment (`@>`), existence, and path predicates
compile to a JSON-column expression contract. Patch/update operations such as
`jsonb_set` lower to the row API's typed `json_set` operation:
`{ "field": "attrs", "path": ["billing", "plan"], "value": "pro" }`. The field
must be a declared `json` relational column, path segments are structured
strings rather than SQL text, and the mutation lowers to the same storage
transform path as ordinary row updates. That rewrites only the JSON cell, then
reprojects derived full-text/path-fact/algebraic indexes for that subtree from
the committed row. Changing a JSON column's embedded schema remains a
derived-index rebuild, not a base-row migration. The remaining query-side work
is represented by structured JSON predicates. Equality and existence filters
over a declared JSON path lower to structured filters such as
`{ "term": { "path": "attrs.billing.plan", "value": "pro" } }` and
`{ "exists": { "field": "attrs.billing.plan" } }`. Object containment (`@>`)
lowers to a typed containment predicate such as:

```json
{
  "json_contains": {
    "field": "attrs",
    "value": { "billing": { "plan": "pro" }, "flags": ["active"] }
  }
}
```

Containment is recursive over actual JSON values: objects require every
requested key to be present and contained, arrays use unordered element
containment, and scalars use JSON value equality with numeric integer/float
normalization. It is intentionally not lowered to a fake term filter.

Declared indexed JSON columns write value-keyed leaf rows beside the ordinary
column scan entry:

```text
json_value_index(column_path, relative_json_path, canonical_leaf_value, doc_key) -> ""
```

The relational resolver uses those rows as candidate sources for
`json_contains`, then hydrates the authoritative base row and rechecks recursive
containment before returning a doc set. That makes stale side rows, partial
rebuilds, and intentionally lossy leaf choices safe. Containment predicates that
do not have an indexable scalar leaf, such as empty object/array containment, or
columns declared with `x-antfly-index: false`, fall back to a base-row scan with
the same containment evaluator. The remaining production work is planner
costing, extraction/update operator breadth, and choosing between the relational
JSON value rows and algebraic path-fact access paths when both can serve the
same predicate.

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
          "email": { "type": "keyword" },
          "email_key": {
            "type": "keyword",
            "generated": { "op": "lower", "field": "email" }
          },
          "status": { "type": "keyword", "default": "active" }
        },
        "required": ["id", "email"],
        "additionalProperties": false
      }
    }
  }
}
```

The model-level contract is typed JSON metadata, not SQL expression text. Literal
column defaults are applied by the write planner before primary-key encoding,
unique tuple derivation, constraint checks, and `RETURNING`. Stored generated
columns currently support typed `lower(field)` and `concat(fields, separator)`
forms; user input cannot write generated columns directly. Inserts materialize
defaults and generated values into the committed row image. Updates and
conflict-target updates that touch tables with checks or generated columns
resolve the base row, apply the requested patch/JSON updates, regenerate stored
generated values as ordinary transform operations, and validate checks against
that final planned row image. `CHECK` predicates are named typed atoms
(`is_null`, `is_not_null`, `eq`, `ne`, `gt`, `gte`, `lt`, `lte`) over declared
columns so REST, SDK, and future SQL adapters compile to the same backend
contract.

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
      { "field": "status", "op": "eq", "value": "open" }
    ]
  },
  "select": ["id", "status", "created_at"],
  "order_by": [{ "field": "created_at", "direction": "desc" }],
  "limit": 50,
  "offset": 0
}
```

The same contract also accepts shorthand equality filters such as
`{ "where": { "status": "ready" } }`. Predicates are typed atoms over declared
relational columns (`is_null`, `is_not_null`, `eq`, `ne`, `gt`, `gte`, `lt`,
`lte`), projections are field lists or `["*"]`, and ordering is over declared
scalar columns with deterministic tie-breaking by input row order. Null and
missing order keys sort after present scalar values. The executor projects from
the relational row JSON/codec and applies `OFFSET`/`LIMIT` after filtering and
ordering.

This is the backend AST that a SQL adapter should target for simple
single-table `SELECT` clauses. The planner still needs to connect that contract
to primary-key, unique-owner, partial/expression, array-element, JSON-value, and
column-major access paths instead of materializing every candidate row first.
Joins should lower to typed row-stream composition over the same base row
contract, with join predicates represented as column references rather than SQL
fragments. Aggregates should consume typed row streams and emit typed projection
rows. Non-recursive CTEs are mostly query composition over this contract.
Recursive CTEs are a separate graph/fixpoint feature and should be treated as a
distinct planner extension.

Those model-level items are not PostgreSQL-specific. They are the durable
relational semantics that the storage model should expose so a future SQL
dialect, REST API, or client SDK can compile to the same operations.

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
