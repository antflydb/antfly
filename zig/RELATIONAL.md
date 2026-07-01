# Relational Mode

Antfly tables are document-first by default: a document is a single
zstd-compressed JSON blob, and every index (`full_text`, `embeddings`,
`graph`, `algebraic`) is *derived* from that blob. Schema is optional and
soft.

SQL-specific parser, lexer, session, PostgreSQL compatibility, lowering, and
parity-test details are documented in [SQL.md](SQL.md). This document owns the
native relational model that SQL, REST, SDK, MCP, A2A, CLI, and internal jobs
target.

Implementation ownership mirrors that contract: relational row request, plan,
result, CTE, and expression-evaluation code lives under `pkg/antfly/src/sql/`,
with API modules acting as callers or re-exporters rather than owning the
relational row runtime.

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

## Lite And C API Profile

Relational mode is profile-agnostic. A relational table opened through server
HTTP, embedded directory storage, the C API, or Lite `.aflite` storage must use
the same schema validation, typed row codec, row participant, secondary column
entries, constraint checks, and query/predicate semantics.

Lite removes distributed movement concerns, not relational correctness. Split,
merge, placement, Raft, and remote shard fanout invariants do not apply inside a
single `.aflite` file, but local validation, secondary-index rebuild,
constraint repair, row rewrite, and derived-artifact catch-up still apply.

The C API exposes this through capability JSON rather than through a separate
relational ABI. `relational.tables=true` means the handle supports relational
base rows. `relational.transactions="local"` means transaction behavior is
local to the opened database handle. `relational.portable_backup=true` means
DB-backed portable AFB export materializes packed relational rows into logical
row documents and DB-backed import restores them through the typed relational
write path. Raw DocStore-level portable import/export remains document-only and
continues to reject relational physical rows or relational schema metadata
unless a DB handle owns the typed schema/write boundary.

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

All derived-index references to relational fields use one logical path model:

```text
FieldRef{ column = "attrs", path = "billing.plan", rendered = "attrs.billing.plan" }
```

An empty `path` addresses a declared relational column. A non-empty `path`
addresses a field below a declared `json` column. The catalog validates every
derived-index `field`, graph `source_field` / `target_field` / `type_field` /
`weight_field`, embedding source, JSON predicate, and JSON projection through
that resolver. `status` resolves only to a top-level declared relational
column; `attrs.title` resolves only when `attrs` is a declared `json` column and
the embedded schema or scoped dynamic template permits `title`.

This lets JSON/JSONB fields participate in the same document-derived artifacts
as document-mode tables without creating a second storage model:

```sql
CREATE INDEX docs_attrs_title_fts
  ON docs USING antfly_full_text (attrs.title);

CREATE INDEX docs_attrs_body_semantic
  ON docs USING antfly_aknn (attrs.body)
  WITH (embedding_name = 'body_embedding_v1', model = 'local-model', dimension = 384);

CREATE INDEX doc_edges_attrs_graph
  ON doc_edges USING antfly_graph (attrs.source_doc, attrs.target_doc)
  WITH (type_field = 'attrs.edge_type', weight_field = 'attrs.confidence');
```

PostgreSQL-style JSON expression forms such as `(attrs->>'title')`,
`(attrs->'billing')`, `jsonb_extract_path_text(attrs, 'billing', 'plan')`, and
`attrs #>> '{billing,plan}'` are adapter sugar only. They lower to the same
typed `FieldRef` and typed JSON predicate/projection nodes; raw SQL expression
text is not stored as index metadata. Unsupported, dynamic, or ambiguous JSON
path expressions fail closed at catalog-application time.

Constraints in scope: primary identity is either the existing document key or a
declared `primary_key.columns` tuple with optional durable `primary_key.name`
constraint metadata, including optional non-key covering payload columns;
`NOT NULL` via required schema fields; unique constraints over one or more
ordered declared non-`json` relational columns, including optional non-key
covering payload columns on unique constraints;
`on_delete: "restrict"` foreign keys; bounded local nullable-column
`on_delete: "set_null"` foreign keys; and bounded local
`on_delete: "cascade"` foreign keys from declared scalar child columns to either
a parent table's `_id`, a declared primary-key tuple, or a declared unique
parent column tuple.
Cross-table primary-key and unique targets route through owner topology when
configured and fail closed when the required owner range is missing.
Existing Antfly transaction/2PC semantics still apply to relational writes.
Relational tables may exist temporarily without a declared primary key as
catalog metadata for migration sequencing, but row reads and writes that require
stable row identity fail closed until a primary key is installed through typed
schema metadata or `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY`. Dropping a
primary-key constraint clears that typed identity metadata and schedules
identity-owner rebuild/rewrite work; the table then returns to the same
catalog-only no-primary-key state until a replacement primary key is added.
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
POST /tables/{table}/rows/batch
POST /tables/{table}/rows/get
POST /tables/{table}/rows/query
POST /tables/{table}/rows/aggregate
POST /tables/{table}/rows/window
POST /tables/{table}/rows/join
POST /tables/{table}/rows/lateral
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

Unique selectors use the same tuple encoder as relational unique constraints
and route to the durable unique-owner range before reading the owner row. They
are point lookups, not query scans. Missing unique selectors return
`found: false` from `rows/get`; `update` and `delete` fail rather than silently
becoming no-ops.

Tables with application-time `WITHOUT OVERLAPS` primary or unique constraints
use a period-qualified identity because the scalar key alone may own multiple
valid-time rows. The public selector names the period and supplies the point in
time to resolve:

```json
{
  "primary": {
    "values": { "sku": "sku:a" },
    "period": { "name": "valid_time", "at": 25 }
  }
}
```

The same shape applies to named temporal unique constraints:

```json
{
  "unique": {
    "name": "prices_sku_valid_time_key",
    "values": { "sku": "sku:a" },
    "period": { "name": "valid_time", "at": 25 }
  }
}
```

Period-qualified selectors encode the scalar tuple and point with the same
temporal owner-key ordering used by storage, scan only the matching temporal
unique-owner prefix, and return the owner whose interval contains the point.
Scalar-only selectors remain valid for non-temporal primary and unique
constraints; temporal constraints reject scalar-only point lookup rather than
choosing an arbitrary interval.

`rows/batch` accepts row operations that compile to the existing batch and 2PC
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
      "op": "rewrite_identity",
      "where": { "primary": { "tenant_id": "t1", "order_id": "o9" } },
      "patch": { "order_id": "o10", "status": "moved" }
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
patch primary-key components. Primary-key changes use the explicit
`rewrite_identity` operation: the planner resolves the old row through a primary
or unique selector, materializes the final row image with normal defaults,
generated columns, update policies, checks, and returning projection semantics,
derives the new primary identity, then emits an internal
`relational_identity_rewrites` request with an old-key version predicate and a
new-key non-existence predicate. Execution treats the operation as one logical
identity update: primary and unique owner rows move to the new physical owner,
child reverse-reference rows move from the old child key to the new child key,
and referenced parent rows run FK `ON UPDATE` restrict/no-action/set-null/cascade
semantics rather than FK `ON DELETE` semantics. The base row still commits as an
old-key row removal plus new-key row insert inside the same 2PC, so replay,
secondary indexes, repair, and range ownership see concrete key movement without
silently mutating the public row identity in place. Bound tables execute the
rewrite directly; provisioned and hosted table-write sources route same-owner
rewrites to the owning group through the internal batch wire and fail closed
when the old and new physical keys would cross owner ranges until routed
identity rewrites carry a durable transaction marker. `insert` and `upsert`
remain primary-key based; explicit
`on_conflict` targets are separate row-operation metadata over primary or unique
owner rows, so a SQL DSL can compile `ON CONFLICT (unique_col...) DO UPDATE`
without changing primary-key `upsert` semantics. `rows/get` accepts an array of
primary or unique selectors and returns the structured identity, row JSON,
version, and optional `physical_key`.

#### Write sync levels

Relational row writes use the same public `sync_level` contract as the document
batch API. `rows/batch` accepts top-level `sync_level` with the public values
`propose`, `write`, `query`, `enrichments`, and `full_index`. The value is
a write-completion requirement, not table schema metadata. It controls how far
the write path must advance before returning: Raft proposal acceptance, durable
base-row write, query visibility, synchronous enrichment generation, or
complete derived-index visibility.

REST/SDK `rows/batch` keeps the existing batch default of `propose` when the
field is omitted:

```json
{
  "sync_level": "full_index",
  "operations": [
    {
      "op": "insert",
      "row": {
        "tenant_id": "t1",
        "order_id": "o9",
        "status": "open"
      }
    }
  ]
}
```

PostgreSQL relational SQL exposes the same contract through an explicit session
setting rather than non-PostgreSQL DML syntax:

```sql
SET antfly.sync_level = 'write';
SET LOCAL antfly.sync_level = 'full_index';

INSERT INTO orders (tenant_id, order_id, status)
VALUES ('t1', 'o9', 'open');
```

The SQL adapter resolves the effective level at the write execution boundary:
`SET LOCAL antfly.sync_level` overrides the session value for the current
transaction, `SET antfly.sync_level` sets the session default, and an unset SQL
session defaults to `write` so ordinary SQL statement completion means the
base-row write has reached the normal durable write contract. Weaker latency
semantics remain available through `SET antfly.sync_level = 'propose'`.

The effective SQL sync level is applied uniformly to every native batch emitted
by SQL lowering: point `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `INSERT ...
SELECT`, source-backed mutation stages, `TRUNCATE`-style mutation-source
batches, and `COPY FROM` imports. It is not stored as raw SQL text and is not
attached to catalog DDL. Lowered write plans carry the parsed native enum, and
batch builders set `RowsBatchRequest.sync_level` before committing through the
same local, provisioned, hosted, and 2PC write paths used by REST/SDK row
writes.

The read-plan endpoints expose the same typed contracts used by the SQL
adapter and storage runtime. They accept JSON envelopes rather than SQL text:

- `rows/plan` accepts the reusable union envelope
  `{ "ctes": [...], "query": { ... } }`,
  `{ "ctes": [...], "aggregate": { ... } }`,
  `{ "ctes": [...], "window": { ... } }`,
  `{ "ctes": [...], "join": { ... } }`, or
  `{ "ctes": [...], "lateral": { ... } }` and dispatches by the single
  operation branch present in the request.
- `rows/query` accepts `{ "ctes": [...], "ranges": [...], "query": { ... } }`
  and returns `{ "total": n, "rows": [...] }`. `select: ["*"]` may be combined
  with named expression/JSON/array/coalesce/alias projections; those extras are
  appended to the row object only when their output names do not collide with
  emitted base or CTE fields.
- `rows/aggregate` accepts
  `{ "ctes": [...], "ranges": [...], "aggregate": { ... } }` and returns
  `{ "total_groups": n, "rows": [...] }`. Group fields, group-expression
  outputs, and metric names define one JSON result object; duplicate emitted
  names are invalid even when no `having` or `order_by` clause references them.
- `rows/window` accepts `{ "ctes": [...], "ranges": [...], "window": { ... } }`
  and returns `{ "total_rows": n, "rows": [...] }`. Selected fields and window
  outputs share one JSON result object; duplicate emitted names are rejected at
  request validation before execution. SQL named-window clauses are adapter
  sugar only: `OVER usage_window WINDOW usage_window AS (...)` is resolved into
  each native window spec's partition keys, order keys, and frame metadata before
  the typed plan reaches storage.
- `rows/join` accepts
  `{ "ctes": [...], "left_table": "orders", "right_table": "customers", "left_ranges": [...], "right_ranges": [...], "join": { ... } }`
  and returns `{ "total_rows": n, "rows": [...] }`. Explicit join projection
  outputs define one JSON result object and must be unique even when no
  `order_by` clause references them. `left_table` and `right_table` are optional
  routing metadata; omitted or empty names mean the endpoint table.
- `rows/lateral` accepts
  `{ "ctes": [...], "left_table": "orders", "right_table": "customers", "left_ranges": [...], "right_ranges": [...], "lateral": { ... } }`
  and returns `{ "total_rows": n, "rows": [...] }`. Lateral projection outputs
  follow the same unique-name rule as join projections. The table-name metadata
  has the same semantics as join plans.

The reusable `RowsPlanRequest` SDK/OpenAPI type is a union of those five exact
operation envelopes, not a loose object with optional operation fields. A plan
must contain exactly one of `query`, `aggregate`, `window`, `join`, or
`lateral`. Query, aggregate, and window plans use only `ranges`; join and
lateral plans use optional `left_table`/`right_table` and paired `left_ranges`
and `right_ranges`. Table-name metadata is valid only on join and lateral
envelopes. Unknown keys, multiple operation branches, and mismatched range
families fail before planning.

`ctes` is optional and ordered. CTEs are named row-query subplans; later CTEs
and the final query/aggregate/window/join/lateral or insert-source stage can
reference a prior CTE with `source_cte`. The public endpoint layer validates
these same envelopes for every read or insert-source stream and fails closed
when the active source cannot provide a row-plan executor. Local stores execute
the plan directly. Provisioned and hosted sources resolve declared ranges, or
the full table when no range is declared, through durable table/range ownership
and run coordinator fanout over the same typed source-query contract.
Provisioned and hosted row-query, aggregate, window, join, and lateral
execution is scan-backed today: owner scans collect range-clipped base rows,
strip scan transport metadata, materialize CTEs at the coordinator, and apply
global filtering, reduction, joining, windowing, projection,
distinct/order/offset/limit semantics through the same typed executors as local
plans. Routed scan collection uses the same default materialization row and byte
caps as CTEs and fails closed before handing an unbounded owner scan to a
coordinator-side query, aggregate, window, join, lateral, insert-source, or
MERGE stage. Direct base-source row queries, aggregate sources, window sources,
join/lateral side sources, and insert-source requests may also carry one
`doc_key_range` in their native source query for local owner-scoped execution.
That embedded range is start-inclusive/end-exclusive over physical row keys, is
valid only while the source is a base table stream, and fails closed when
combined with a materialized `source_cte`, pre-materialized source rows, or
coordinator-injected routed ranges. Routed plans use their top-level `ranges`,
`left_ranges`, and `right_ranges` arrays instead, so ownership fanout remains
catalog-driven rather than user-injected.
Join and lateral plans resolve `left_table` and `right_table` through
the public catalog before validation, parse each side against that table's
runtime schema, and scan each side from its own table/range owner set before
coordinator join or lateral correlation. Omitted side table names still resolve
to the endpoint table. CTEs remain endpoint-table subplans; a side that consumes
a CTE cannot also name a different physical side table. Plans without CTEs do
not scan the endpoint table just to produce an empty CTE input. SQL adapter read
execution uses this same `TableReadSource` path after lowering: catalog-backed
`SELECT ... JOIN ...` and bounded `LEFT JOIN LATERAL` plans carry their
side-table identity in the native typed plan, derive side schemas from the
catalog, and execute through the same routed scan helpers as REST/SDK join and
lateral requests. SQL text never reaches storage execution. If a relational
schema declares a user-visible `key` column/path, the scan-backed route fails
closed until the internal raw-row stream path can carry physical row identity
separately from row JSON. Routed row-source collection carries the resolved
range topology epoch into owner-local or hosted-remote collection, sends each
owner only its catalog-clipped physical key span, and validates the resolved
range plan again after row gather, before coordinator evaluation, so range
movement fails closed with `TopologyChanged` instead of merging stale owner
rows. Routed materialized CTEs carry emitted-field metadata,
and the REST/API CTE planner carries typed output-column metadata for row-query
CTE streams, including derived expression field types and array item domains.
Storage-backed SQL read-plan tests pin chained row-query CTE execution through
`queryRelationalRowsPlan`, including a CTE expression output consumed by a later
CTE before the final projection, so SQL and REST/SDK plans share the same
materialized-row contract.
Expression output metadata also carries conservative nullability: base fields
inherit schema nullability, literals and engine-owned values such as `now` and
`uuid_v4` are non-null unless explicitly `null`, null-propagating functions
stay nullable when any required input may be null, and `coalesce`/`greatest`/
`least` become non-null when any operand is proven non-null. Unsupported or
ambiguous inference stays nullable rather than narrowing the public contract.
Aggregate result metadata follows the native executor contract: `count`, `sum`,
and bounded `array_agg` are non-null, while `avg`, `min`, `max`, `string_agg`,
`bool_or`, and `bool_and` remain nullable because empty filtered metrics can
emit `null`. Window result metadata similarly marks ranking, distribution,
`ntile`, and `count` outputs as non-null, keeps aggregate/value functions
nullable when an empty frame or missing offset row can emit `null`, and narrows
`lag`/`lead` only when a non-null default and non-null value expression prove
the output cannot be null.
Join and lateral result metadata also carries outer-stream nullability:
`LEFT JOIN` and lateral right-side projections are nullable even when the
selected right-side schema column is required, while left-side projections keep
their source nullability.
The same native metadata planner is exposed for final row-query, aggregate,
window, join, and lateral read plans: callers can derive owned
`RelationalColumn` output metadata from the typed plan, CTE output schemas, and
side-table schemas without parsing SQL text or sampling result rows. Public
`rows/query`, `rows/aggregate`, `rows/window`, `rows/join`, and `rows/lateral`
responses include this metadata as `result_schema`, and the OpenAPI/SDK structs
expose the shared `RowsResultColumn` shape alongside returned rows.
`RowsResultColumn.name` is the unique JSON result-object key. SQL-facing
adapters may also populate `display_name` with the original output label; that
label is presentation metadata and may be non-unique, so clients must not use it
as the row-object key.
Source-backed text and keyword outputs also carry optional `collation`
metadata from the relational column catalog. Derived expression, aggregate, and
window outputs omit collation metadata unless a typed expression node can prove
a single inherited collation without changing comparison semantics.
Materialized stream schemas do not inherit hidden base-table identity: primary
key metadata is preserved only when the CTE query directly selects every
primary-key component from its source stream. Derived expressions and aliases
that reuse key names do not preserve row identity.
Row-query, aggregate, window, join, lateral, and insert-source CTE consumers validate field
references against those emitted fields before coordinator reduction. Shared
text comparison semantics now live in the storage collation helper and are used
by storage and coordinator JSON execution. Relational keyword equality and
range candidate planning carries explicit collation into column-index matching.
Expression-derived row selectors, unique-owner identity, distinct/order
deduplication, aggregate ordering, CTE metadata, and supported direct-field
expressions now carry source collation/null semantics into the shared typed
encoders and comparators. Ordered or composite secondary-index metadata that
cannot be proven equivalent to a single ascending/default-null column lookup is
treated as a scan-only index candidate, so read planning fails closed to base
rows instead of widening an ordered index into different semantics. Bounded CTE,
recursive CTE, join, and set-operation materialization share explicit
memory/spill/reject admission policy, and DB-backed spill paths use
query-temporary storage instead of bypassing caps. Focused routed-read and
mutation tests cover stale-topology fail-closed behavior for remote row-query
collection, hosted lateral right-side collection, routed set-operation
collection including top-level materialized CTE sources over merged
owner-clipped side ranges, lowered cross-table set operations whose CTE-backed
branch runs against its resolved base catalog table while direct branches keep
their own catalog schema, cross-table AS-OF set operations that preserve that
CTE branch metadata through the system-time row-query vtable, and routed MERGE
target/source preimage collection including ordered global selection after
stable owner-version verification.
Stable routed
relational scan helpers require owner scan payloads to include both the
physical key and committed row version before coordinator evaluation, then
re-read those rows and reject missing, changed, or exact-version-drifted
preimages; routed MERGE target and source collectors now use that same stable
scan boundary before applying global ordering, offset, and limit, and the
previous unverified JSON-only routed row collector has been removed from the
scan-backed relational read module. Recursive insert-source, MERGE, and joined
mutation-source
execution fail closed when the materialized CTE source carries a row claim or
embedded `doc_key_range`, before coordinator-side CTE materialization starts.
Mutation-source and joined mutation-source autocommit admission rejects
non-exclusive row claims before opening the caller-supplied transaction.
REST/SQL row-query parsing plus storage row-query and mutation-source
validators require the same durable owner/lease claim envelope before planning,
so malformed claimed reads or mutations fail even when no row would be
selected.
Routed row-query, aggregate, window, join, and lateral plan admission also
rejects row claims on CTE producers or derived-stage sources before owner scans
begin, keeping claims limited to lockable base-row mutation streams. The same
routed admission layer rejects embedded `doc_key_range` filters on CTE
producers, materialized
sources, or sources that already have coordinator-injected routed ranges, so
physical key slicing stays owned by the catalog/range fanout plan.
If an authenticated request has an effective row filter, supported
filters are pushed into every base row source before projection, aggregation,
joining, lateral correlation, CTE materialization, mutation-source staging, or
windowing. Supported pushdown covers `match_all`, `conjuncts`, `bool.must`,
`bool.filter`, scalar `term`/`terms`, `array_any`, `json_contains`,
`numeric_range`, and `date_range` over declared relational columns, plus
`disjuncts` and `bool.should` / `minimum_should_match = 1` branches that lower
to native `access_or_predicates`. Filters that require nested disjunctions,
negation, a broader boolean model, or text-search execution still fail closed
for these row-plan endpoints so totals, staged writes, and derived rows cannot
bypass row-level authorization.

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
`COALESCE`/`NULLIF` predicates such as
`WHERE coalesce(nullif(status, 'blocked'), 'pending') = $1`, scalar numeric
predicates such as `WHERE round(abs(amount - quantity)) > $1`,
`WHERE trunc(amount) > $1`, `WHERE sqrt(amount) > $1`,
`WHERE sign(amount - quantity) > $1`, `WHERE power(amount, 2) > $1`,
`WHERE greatest(amount, cap, 0) > $1`, and
`ORDER BY least(amount, floor, 100)`, and array
cardinality predicates such as `WHERE array_length(tags, 1) > $1` and
`WHERE cardinality(tags) > $1`, array-position predicates such as
`WHERE array_position(tags, $1) > 0`, array-position projections such as
`array_positions(tags, $1)`, array-to-text predicates such as
`WHERE array_to_string(tags, ',') = $1`, JSON array
cardinality predicates such as `WHERE jsonb_array_length(metadata->'flags') > $1`,
JSON path-array predicates such as `WHERE metadata #>> '{billing,plan}' = 'pro'`,
and JSON type predicates such as `WHERE jsonb_typeof(metadata->'flags') = 'array'`
lower to the same `expression_where` contract and evaluate through the shared
expression executor. SQL read execution coverage pins the same contract for
array-position predicates, `array_positions` projections, computed array
append/prepend/concat/remove/replace projections, and `string_to_array`
containment predicates over committed relational rows. Ordered expression
comparisons whose left or right expression evaluates to JSON `null` are
non-matches, not query aborts; explicit null behavior stays modeled through
`IS NULL`, `IS NOT NULL`, and distinctness predicates.

Row-query `expressions` are emitted by the same storage executor rather than
being SQL-adapter-only projections. REST/SDK callers can project computed fields
with the shared expression AST, including case-folding, concat/null handling,
numeric arithmetic including modulo, casts, JSON extraction, JSON type inspection,
JSON array length, `coalesce`, `nullif`, array length / cardinality,
`array_position`, `array_positions`, read-side `array_append` / `array_prepend` / `array_cat` / `array_remove` / `array_replace`,
`array_to_string`, `string_to_array`, text transforms including `initcap`, text/numeric bridge functions such as
`ascii`, `chr`, and `md5`, text replacement/slicing functions such as
`replace`, `regexp_replace`, `regexp_like`, `regexp_count`, `regexp_instr`, `regexp_substr`, `translate`, `substring`, and `overlay`, and text-boundary predicates such as `starts_with` and
`ends_with`, searched `case`, statement-bound time nodes, and temporal bucketing
with `date_trunc`, `date_bin`, and `date_part` / `extract`, including larger
calendar units such as `decade`, `century`, and `millennium`. PostgreSQL
`DATE '...'`, `TIMESTAMP '...'`, and `TIMESTAMPTZ '...'` literals normalize to
encoded UTC-nanosecond value nodes before they enter the shared expression AST,
so projections, predicates, conflict actions, `RETURNING`, and bucketing
functions all see the same typed-plan value shape. The same expression nodes are
used for aggregate inputs, window value inputs, conflict actions, expression
`RETURNING`, and claimed mutation-source `patch_expr` assignments so supported
SQL arithmetic is not reimplemented per statement family. Aggregate filter
execution uses that same path: storage-backed SQL read-plan tests pin
expression filters such as `lower(status) = 'open'`, computed-array containment
filters such as `string_to_array(scope, ' ') @> ARRAY['write']`, JSON
containment filters, and declared-array containment filters through
`aggregateRelationalRowsPlan`, not just SQL lowerer fingerprints. Window
aggregate filters use the same contract through `windowRelationalRowsPlan`.
Distinct and ordered aggregate execution is pinned through the same SQL-to-storage
path: `COUNT(DISTINCT json_expr)`, `array_agg(DISTINCT json_expr)`,
ordered `array_agg`, and `string_agg(DISTINCT ... ORDER BY ...)` all execute
against stored rows after lowering to bounded typed aggregate specs.
Storage-backed SQL read-plan tests also pin partitioned ranking, offset, value,
and bucket windows: `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`,
`LAST_VALUE`, `NTH_VALUE`, `PERCENT_RANK`, `CUME_DIST`, and `NTILE` execute
against stored rows after PostgreSQL syntax lowers to the typed window plan.
Partitioned window `COUNT`, `SUM`, `BOOL_OR`, and `BOOL_AND` filters over
expressions, computed-array containment, JSON containment, and declared-array
containment are verified the same way. Joined mutation-source
`patch_expr` assignments use the same source-aware expression nodes, so a
source-row field and a target-row field can participate in one typed arithmetic
assignment without adapter-private evaluation. PostgreSQL row-list mutation
syntax such as `SET (status, amount) = ('active', amount + 1)`, including
`ON CONFLICT ... DO UPDATE` row-list assignments, `ROW(...)` row-constructor
spelling, and `DEFAULT` items inside row-list assignments, is adapter sugar over
the same ordered per-field typed assignments; it is rejected on duplicate
targets and never creates tuple-valued storage operations. Row-query projection
labels are a closed result-object contract: native REST/SDK requests must
provide unique selected fields, compact projection outputs, and expression
projection outputs before execution, so storage and API-side preview execution
never emit ambiguous JSON objects with repeated keys. The SQL adapter may accept
repeated presentation labels by lowering the later computed output to a
deterministic unique native key such as `id_2`. `SELECT *` extras use that same
adapter rule: the base row keeps its declared field names, and each explicit
extra projection that would collide with a base field or earlier extra output is
renamed to the next stable suffix (`status_2`, `status_3`, ...). Direct
REST/SDK duplicates and explicit non-star SQL duplicate outputs still fail
closed before execution.
Parenthesized scalar order expressions are the same typed order keys as their
unparenthesized forms: text-like, numeric, datetime, and boolean outputs are
accepted directly. JSON, object, and array outputs are also typed order keys:
storage encodes them as canonical JSON for comparison, sorting object keys
recursively and preserving array order, so SQL and REST/SDK expression ordering
share one deterministic contract. The SQL adapter applies that same guard to
every direct `SELECT` and read-classifier expression `ORDER BY` branch,
including JSON construction, `to_jsonb`, and array-producing expressions; only
expression domains with no deterministic order key still fail before a typed
plan is accepted.
PostgreSQL `ORDER BY expr USING <` / `USING <=` lowers to the same ascending
typed order direction as `ASC`, and `USING >` / `USING >=` lowers to the same
descending direction as `DESC`; other ordering operators fail closed because
the typed plan stores direction, not arbitrary comparator functions.
`DATE_BIN` accepts either a numeric nanosecond stride or a fixed-duration
`interval_ns` stride; the PostgreSQL adapter lowers syntax such as
`DATE_BIN(INTERVAL '1 hour', created_at, origin)` to the same typed `interval_ns`
node. Calendar intervals such as `INTERVAL '1 month'` remain valid for timestamp
arithmetic where month semantics are calendar-relative, but they fail closed as
`DATE_BIN` strides because buckets require a constant duration.

Row predicate atoms are exact typed objects. Scalar atoms use only `field`,
`op`, and the optional `value` member; structured `array_*`, `in`/`not_in`,
JSON path/containment, and text-pattern atoms expose only their documented
operator-specific keys. Scalar membership predicates (`in`/`not_in`, SQL
`IN`, `= ANY`/`SOME`, `<> ANY`/`SOME`, `= ALL`, and `<> ALL`) validate every
member against the selected field's declared scalar type or the computed
expression's inferred scalar result type before producing a typed plan; declared
`json` and relational `array` columns use their dedicated JSON/array operators
instead. PostgreSQL JSONB key-set operators stay typed: `json_col ?| ARRAY[...]`
lowers to `bool_or(json_path_exists(...), ...)`, and `json_col ?& ARRAY[...]`
lowers to `bool_and(json_path_exists(...), ...)`; empty key arrays, nested
dotted keys, and non-string members fail closed. PostgreSQL array overlap stays
typed as well: `array_col && ARRAY[...]` expands to bounded `array_any` access
branches, so `array_col && ARRAY['a', 'b'] AND status = 'open'` becomes
`(array_any('a') AND status = 'open') OR (array_any('b') AND status = 'open')`
rather than residual SQL. Empty overlap arrays, non-array operands, and members
outside the declared array item domain fail closed. SQL `LIKE` / `ILIKE ANY`
and `LIKE` / `ILIKE ALL` over bounded text arrays, including declared text
fields and computed text expressions such as `lower(status)`, lower to the
same shared boolean expression AST as computed text patterns: `ANY`/`SOME` becomes
`bool_or(like(...), ...)`, `ALL` becomes `bool_and(like(...), ...)`, and
`NOT LIKE` / `NOT ILIKE` wraps each element in `bool_not`. Row predicates,
aggregate filters, and window filters all use that same expression-tree shape
instead of statement-family-specific pattern evaluation. Empty arrays, non-text
members, and unsupported unbounded pattern sources fail closed before storage
sees a plan. Ordinary expression comparisons also bind both sides before planning:
RHS expressions must be comparable with the LHS result type, with null literals
allowed explicitly and datetime expressions allowed to compare with numeric
encoded timestamps. Searched `case` expressions share that predicate binding for
every `when` arm, infer their result type from the first non-null `then` or
`else` arm, and reject mixed result domains before planning, with the same
datetime/numeric encoded-timestamp compatibility used by comparisons.
`coalesce`, `nullif`, `greatest`, and `least` also bind operands as one result
domain, ignoring null literals for inference and rejecting mixed scalar domains;
`greatest` and `least` additionally require an orderable scalar domain. The
`where` envelope is closed as well: it is one
top-level atom, an `all` conjunction, `any`/`not` branch groups, or an
`all` conjunction plus branch groups. Branches may contain scalar, membership,
text-pattern, declared-array, and declared-JSON atoms; scalar-only branches are
stored as native scalar predicate groups, while branches that contain any
structured atom are stored as mixed access predicate groups so conjunctive
branch semantics remain explicit. Unknown predicate keys and ambiguous mixtures
fail request validation before planning, matching the public OpenAPI
`additionalProperties: false` contract and keeping SQL adapter lowering from
creating wider internal shapes than native REST/SDK callers can express.
Each typed `order_by` key is similarly exact: it must provide exactly one of
`field` or `expr`, plus optional `direction` and `null_test`. Missing both, or
providing both, fails before planning so result ordering has a single binding
source across row queries, aggregates, joins, lateral stages, windows, and
mutation-source target selection.

The public OpenAPI shape uses concrete one-of variants for those reusable AST
nodes instead of a single object with many optional fields. `RowsExpression` is
`RowsExpressionField`, `RowsExpressionValue`, or `RowsExpressionOperator`;
`RowsQueryOrder` is `RowsQueryOrderField` or `RowsQueryOrderExpression`;
`RowsCoalesceOperand` is `RowsCoalesceFieldOperand` or
`RowsCoalesceValueOperand`; and `RowsWhereBranch` is `RowsWhereBranchAtom` or
`RowsWhereBranchAll`. Generated SDKs therefore construct the same exact typed
plans that REST receives, without zero-value hybrid structs, raw backend SQL, or
client-only helper validation hidden inside generated files. The OpenAPI
`RowsExpressionOperator` enum is kept in lockstep with the shared storage
expression surface, including text transforms, boolean expression nodes,
numeric functions, fixed and calendar intervals, date functions, JSON helpers,
`uuid_v4`, `source`-qualified fields, and datetime casts, so REST and SDK
callers can construct the same row-local expression plans that SQL lowering
emits. When a new expression, order, predicate, or compact projection shape
lands, it should be a new typed variant or an extension of the relevant variant
schema first, then SQL syntax can lower onto that native shape.

Materialized row-query CTEs carry the same projected field contract as their
serialized result rows. A downstream `source_cte` query, aggregate, window,
join, lateral, or insert-source stage may read only fields emitted by the CTE; a reference to a
base column omitted by the CTE projection is rejected before row evaluation
rather than being treated as missing JSON. This covers row filters, selections,
distinct/order keys, expression projections, aggregate group/input/filter fields,
window partitions/order/value expressions, join keys/projections, lateral
correlations/projections, and insert-source assignment expressions. The public row-plan parser builds the same
metadata-only CTE output schema from REST/SDK typed projections, so invalid CTE
consumers fail before a plan is accepted by the API layer. The SQL adapter uses
the same validator when lowering non-recursive `WITH` row-query, aggregate, and
window plans, so SQL cannot observe a looser CTE contract than the native typed
API. PostgreSQL-style CTE column alias lists such as
`WITH source(alias_a, alias_b) AS (...)` are lowered by rewriting the producer's
typed output schema: direct selected fields become `field_aliases`, computed
projections are renamed in place, and downstream consumers bind only to the alias
names. Alias-list arity mismatches, duplicate aliases, and aliasing `SELECT *`
fail closed because the typed plan needs an explicit one-to-one emitted-field
contract. CTE producers and downstream `source_cte` consumers both keep computed
expression, JSON-extraction, and null-safe distinct predicates on the shared
typed expression path instead of lowering them through CTE-specific SQL cases.

Derived stages also validate references against their emitted result shape.
Aggregate `HAVING` and result-level `order_by` clauses, including local,
provisioned, and hosted aggregate plans, may read only emitted group,
expression-group, or metric fields. Local, provisioned, and hosted read join and
lateral result ordering may read only emitted projection names. Local,
provisioned, and hosted window result ordering may read only selected source
fields and named window outputs. Missing source columns therefore fail at plan
validation time instead of becoming silent missing sort or filter keys. Joined
mutation-source `order_by` is different by design: it selects and claims target
preimages before staging, so it binds to target-side row fields rather than
read-result projection aliases.

Read joins, bounded lateral joins, and joined mutation-source requests split
filtering into side-local pushdown and post-match predicates. Side-local
`join.left`, `join.right`, `lateral.left`, and `lateral.right` predicates run
while collecting left/right or target/source candidates, so they can use the
same scalar, JSON, array, text-pattern, and expression access paths as ordinary
row queries. Predicates that read both rows belong to request-level
`match_expression_where`, `match_expression_any`, `match_expression_not`, or
`match_expression_array_contains` fields. Those post-match filters evaluate
after equality join-key matching or correlated lateral right-side execution and
before result projection, global ordering, offset, and limit; for joined
mutation sources they also run before target claiming and staging, so `matched`
counts and selected target rows reflect the complete predicate. The expression
AST uses `{"source":"source"}` on field nodes to bind the right/source row;
unmarked fields bind to the left/target row. For `LEFT JOIN` and `LEFT JOIN
LATERAL`, a request-level post-match predicate also suppresses null-extended
rows, matching SQL `WHERE` residual semantics. SQL read joins and bounded
lateral joins bind each side against the resolved table or CTE output schema
before producing a typed request; catalog-backed planning resolves ordinary
cross-table join sources and lateral subquery sources through durable table
metadata before lowerer validation. Lower-level adapter helper entrypoints
accept explicit target/source schemas for unit tests and internal callers. SQL
read joins, bounded lateral joins, `UPDATE ... FROM`, and
`DELETE ... USING` lower simple side-local predicates into side pushdown fields,
equality cross-row key predicates into `join.on` or lateral `correlations`, and
mixed computed predicates such as
`lower(left.customer_id) = lower(right.id)`, `latest.amount + org.amount > 10`,
`lower(target.status) = lower(source.status)`, or
`target.quantity + source.quantity > 10` into the post-match expression fields.
Mixed predicates inside the correlated lateral subquery body lower to those same
lateral request-level residual fields after any field-equality correlations and
right-local predicates are extracted, so `bal.amount + org.amount > 10` has one
native plan shape whether it appears in the subquery body or the outer `WHERE`.
Parenthesized subquery-body OR groups over both rows lower to
`match_expression_any`, including arithmetic, modulo, and function-call expression
branches. Subquery-body `NOT (...)` over both rows lowers to
`match_expression_not`, and mixed computed-array containment such as
`string_to_array(right.scope || ' ' || left.scope, ' ') @> ARRAY[...]` lowers to
`match_expression_array_contains`, preserving the same typed residual contract.
Join `ON` residuals have a separate typed home because they are not the same as
post-join `WHERE` filters. Inner-join side-local `ON` predicates push into the
relevant side query. For `LEFT JOIN`, nullable right-side `ON` predicates may
push into the right query for efficient candidate reduction, but preserved
left-side `ON` predicates lower to `on_expression_where`,
`on_expression_any`, `on_expression_not`, or `on_expression_array_contains` so
they are evaluated as match eligibility. Computed or mixed-row `ON` expressions
such as `lower(left.status) = lower(right.status)` use the same residual path
rather than `WHERE`-style post-match filters. If all equality candidates fail
those `ON` residuals, the executor emits the null-extended left row. Existing
`match_expression_*` fields remain post-join filters and therefore suppress
null-extended rows when they model SQL `WHERE` residuals.

Those residual fields are part of the public REST/OpenAPI/SDK join, lateral,
and joined mutation-source request structs, not adapter-private SQL state. When
a read-side join or lateral side reads from a CTE, API validation checks
residual field references against that side's projected CTE output before
execution: unqualified fields must be present on the left CTE output, and
`source` fields must be present on the right CTE output. Join keys, lateral
correlations, and joined mutation-source field assignments compare the full
declared field domain, including array item type, before a typed plan is
accepted.

Local, provisioned, and hosted query, aggregate, window, join, and lateral plans
preflight CTE lists before CTE materialization or owner fanout. Query plan
envelopes reject final-query row claims and embedded doc-key ranges even when
the plan has no CTEs; with CTEs, those fields fail before CTEs are materialized.
CTE lists are also preflighted for empty names, duplicate names, row claims,
embedded doc-key ranges, forward references, and missing final-stage CTE
references before any CTE range is collected. They also build metadata-only CTE
output schemas from typed projections before materializing CTE rows, so
downstream CTE field references fail before an earlier CTE can read, fan out, or
hit row/byte caps. After row collection, both API JSON execution and DB-backed
execution run the same native materialization-admission decision over observed
row count, observed JSON byte size, `max_rows`, `max_bytes`, and
`spill_after_bytes`: in-memory materializations continue, over-limit
materializations reject, DB-backed spill-required materializations write
query-temporary rows to the internal durable spill namespace, and API/coordinator
JSON execution writes spill-required materializations to query-temporary spill
files before downstream CTE consumers read them through the same typed plan.
Those output schemas
preserve base identity only for streams that directly project every primary-key
component from the source stream; derived aliases or partial-key streams remain
read-only materialized streams rather than pretending to be base rows. Aggregate
`HAVING` / result-order references and join/lateral
result-order references are checked against their emitted result shape at the
same boundary. Empty aggregate plans, row-claim aggregate sources, empty window
lists, window functions with missing or illegal value expressions, and
unbounded window ordering fail before any range is collected. Mixed window
order/partition shapes are valid typed plans: execution builds an independent
partition/order/frame context for each window spec, evaluates that spec against
its own ordered stream, and maps the result back onto the final source-row
order before any outer result ordering or pagination is applied.

The physical key is an implementation detail derived from the canonical typed
primary-key tuple. It exists for placement, WAL, row-version ownership, and
debugging, but relational clients should not persist it as their row address.
This shape is intentionally SQL-lowerable: `INSERT`, `UPDATE ... WHERE` full
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
validation/build before the catalog flip. Runtime writes ignore unvalidated FK
constraints for parent checks, parent-delete actions, and reverse-reference row
maintenance; they start participating only after validation promotion marks the
FK enforced. Drops remove the old backing rows.
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

External base-source tables use the same typed row-plan envelope instead of a
separate query API. When a relational table schema carries an
`external_base_source`, the table-read source dispatches supported
`rows/query` plans through the lake row-scan hook and adapts the projected lake
rows back into the normal `RelationalRowsQueryResult` envelope. Global
`rows/aggregate` plans first try the typed lake expression-aggregate hook for
simple ungrouped `count`, `sum`, `min`, `max`, and `avg` shapes over i64 value
columns; unsupported expression-aggregate hooks, empty min/max/avg inputs, and
count-only plans fall back to scan-backed aggregate execution instead of
changing the public result contract. Unsupported plan features such as CTE
ranges, grouped aggregates, aggregate filters, distinct/ordered aggregates,
ordering, joins, windows, and broader predicates still fail closed until the
lake executor implements those typed shapes directly.

Iceberg external sources pin reads to a table metadata file and snapshot id,
then derive the row inventory from that snapshot's manifest list and data
manifests. Active Iceberg delete manifests are parsed into a typed delete plan,
and opened object-storage Iceberg scans decode both position-delete files and
schema-resolved equality-delete files into Antfly external row refs before
predicate, limit, and hydration accounting. Lake row-source scans, group-by
aggregation, sidecar hydration, and direct row-ref hydration can exclude deleted
row refs once the delete plan has produced those refs. Direct lake row-query,
set-operation, aggregate, join, lateral, and window execution now routes typed
filters, ordering, set-operation/window CTE materialization, and range slicing
through the lake scan hook or the shared materialized full-row executor. Broader
active Iceberg delete handling across every derived stage and large lake
spill/backpressure remain tracked as external/lake production hardening.

### SQL adapter boundary

Relational mode is the storage and API substrate that a SQL DSL can target. It
is not itself a PostgreSQL wire server, SQL parser, migration runner, or PL/SQL
runtime. PostgreSQL-specific surface work such as `pgx` protocol behavior,
migration-file replay, extensions, triggers, PL/pgSQL functions, and exact
PostgreSQL DDL syntax belongs in an adapter layer above the Antfly model.
The canonical SQL adapter architecture, parser/lexer design, SQL session
semantics, and parity strategy are now documented in [SQL.md](SQL.md); this
section keeps only the relational/model boundary that SQL lowers into.

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
2. **Adapters second.**
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

The PostgreSQL-facing adapter should grow into a real compiler front-end rather
than continuing to accumulate statement-specific token scans. The lexer is the
first boundary: it owns PostgreSQL comments, quoted identifiers, string
literals, dollar-quoted literals, bind placeholders, casts, operators,
keywords, and source spans. The parser should consume those tokens into a
bounded adapter AST for supported statement families plus explicit unsupported
nodes for recognized-but-unimplemented shapes. That adapter AST is not durable
state and is not an execution model; it exists only to resolve syntax, names,
parameters, and error classification before lowering into Antfly-native typed
plans.

The maintainable code shape is now a SQL-owned compiler/runtime package plus
API-owned integration coverage rather than one growing parser file. The
PostgreSQL-compatible SQL language surface lives under `pkg/antfly/src/sql/`.
`sql/mod.zig` is the facade for pure parser, binder, lowerer, diagnostic,
fixture, and catalog-plan APIs. Statement-family lowering is split across
`sql/lower_select.zig`, `sql/lower_dml.zig`, and `sql/lower_ddl.zig`; runtime
state lives in narrow SQL modules such as `sql/sessions.zig`,
`sql/transactions.zig`, `sql/prepared_statements.zig`, `sql/cursors.zig`,
`sql/notifications.zig`, `sql/routines.zig`, `sql/document_runtime.zig`, and
`sql/document.zig`. API parity, HTTP, app-parity, and storage execution tests
stay beside the API surface in `api/sql_adapter_integration.zig`. New work
should target `src/sql/` unless it is explicitly API/storage integration.

- `mod.zig`: public facade for the existing SQL lowering entrypoints and shared
  deinit helpers.
- `plan.zig`: adapter public plan structs, fingerprints, and ownership helpers
  that callers already consume, including shared row-expression clone, rewrite,
  query-order clone, predicate clone/deinit, projection conversion/deinit, join
  projection deinit, window deinit, and deinit helpers used by read, write, DDL,
  and compatibility wrappers.
- `ddl_plan.zig`: adapter public DDL/catalog plan containers and ownership
  helpers, including table, partition, index, relation-lifetime, identity
  allocator, domain, and catalog plans, plus grammar-syntax to plan-enum
  conversion helpers for cursor, prepared-statement, transaction, lock,
  foreign-key option, maintenance, bulk I/O, and routine DDL metadata. These
  stay split from read/write execution plans so catalog evolution can move
  behind the adapter facade without growing the main lowerer file.
- `token.zig`: token kinds, token source spans, keyword helpers, and simple
  token predicates.
- `lexer.zig`: PostgreSQL lexical handling for whitespace, comments, quoted
  identifiers, strings, dollar-quoted literals, casts, operators, and bind
  placeholders. Tokens should stay source-sliced except when decoding requires
  owned text.
- `ast.zig`: adapter-private PostgreSQL syntax AST nodes. These are never
  stored durably and never passed to storage.
- `parser.zig`: parser cursor, statement dispatch, and grammar entrypoints.
- `grammar.zig`: handwritten grammar helpers today, or the checked-in
  generated-parser wrapper later if a grammar generator is introduced. Grammar
  also owns PostgreSQL syntax normalization that is independent of catalog
  state, such as `public.` object-name elision, owned identifier parsing,
  comma-delimited identifier lists, non-recursive CTE column-alias list syntax,
  and `ONLY <table>` table-reference syntax.
- `binder.zig`: schema/catalog lookup, parameter binding, catalog-backed name
  resolution, output-column inference, and type validation.
- `lower_expr.zig`: shared expression lowering into Antfly row-expression,
  check, index-predicate, default, conflict-action, returning-expression ASTs,
  query predicate-surface checks, and set-operation projection/column
  compatibility and predicate-disjointness proof helpers. Golden-plan
  fingerprints for row rewrite expressions and row-security predicates also
  live here, so corpus evidence is derived from the adapter-owned expression
  surface rather than duplicated SQL facade helpers. SQL select-list,
  `ORDER BY` expression, general row-expression operand start classification,
  and binding-aware extension/routine expression starts also live here as typed
  enum dispatch helpers, so statement lowerers do not carry long
  PostgreSQL-token predicate chains for deciding which expression family starts
  at the current token.
- `lower_select.zig`, `lower_dml.zig`, and `lower_ddl.zig`: statement-family
  lowering into Antfly typed read plans, write plans, and catalog/schema plans.
- `diagnostics.zig`: span-aware unsupported-shape diagnostics and stable
  required-feature classification.
- `corpus.zig`: SQL/API parity source/golden fixture metadata, helper
  predicates, golden-plan fingerprint append mechanics, fixture parsers, and
  fingerprint assertions.

New SQL semantics should extend the adapter package boundary directly.
Create-table and create-index DDL plan parsing live in `sql/ddl_plan.zig`;
parser-local state crosses that boundary only through narrow hooks for
row-expression/default parsing until those remaining expression entrypoints are
fully adapter-owned. Binding-aware extension and routine functions follow the
same rule: function-call recognition, duplicate binding rejection, arity checks,
and row-expression lowering belong to `sql/lower_expr.zig` or adjacent adapter
modules. Ready extension
bindings passed into the adapter are typed
`RelationalRowsExpressionKind` values, not parser-local string mappings. The
test split follows the same boundary. API, storage execution,
schema-application, app-parity, catalog-backed plan, and end-to-end typed-plan
coverage should stay beside the owning integration surface, while pure adapter
regressions such as PostgreSQL syntax lowering, DDL plan shapes, expression
lowering, DML plan construction, parser grammar tails, and fail-closed adapter
diagnostics should live beside the owning `sql/` module. `SqlFunctionBindings`,
routine expression binding metadata, `argN` substitution, duplicate/arity
lookup, and null-input short-circuit helpers live in the adapter expression
layer. DDL predicate literal parsing routes through `sql/value.zig` instead of
carrying a second local token-slice parser; the grammar slice moves
partial unique-predicate token grouping and atom parsing into
`sql/grammar.zig` while keeping the runtime `UniquePredicate`
contract unchanged. Parser-local forwarding wrappers for identifier lists and
scalar JSON literals are removed as adapter facade calls replace them, with
negative-number JSON literal parsing owned by `sql/value.zig`.
Default-value validation, JSON document/array shape checks, JSON literal
validity checks, and scalar/array JSON type matching also live in
`sql/value.zig`, so DDL, DML, and expression parsing share one
adapter-owned value boundary.
Case-insensitive identifier-list uniqueness and disjointness checks move with
the list grammar into `sql/grammar.zig`. The validating DDL
constraint column-list entrypoints for primary keys, unique constraints,
temporal `WITHOUT OVERLAPS` lists, and `INCLUDE` columns also live in
`grammar.zig`, so statement lowerers do not need parser-local wrappers that
parse a list and then re-run identifier-list validation. Mutation-path
conflict validation for insert columns, dotted field paths, JSON set paths, and
typed JSON-set transform path construction now lives in
`sql/lower_dml.zig`. MERGE
target-row usage checks for expressions and predicate groups
also live in `lower_dml.zig`, so source-only clauses fail closed through
adapter-owned DML rules instead of parser-local recursion. Basic runtime-column
lookup moves to `sql/binder.zig` so parser validation, DML lowering,
and later catalog binding use the same column-name and type matching helper.
Relational catalog-existence checks, DDL column lookup, declared index-name
lookup, primary/unique/foreign-key/check constraint-name lookup, and default
primary-key naming rules also move into `binder.zig`, leaving schema mutation
code to apply typed operations rather than own catalog name-resolution
primitives. Temporal period lookup/type checks, period catalog
validation, primary-key `WITHOUT OVERLAPS` validation, and foreign-key
column/temporal-action validation follow the same binder boundary because they
bind typed catalog metadata to existing relational columns and periods.
Row-expression equality and unique-expression duplicate validation move into
`sql/lower_expr.zig`, so DDL index parsing, conflict-target parsing,
and plan/fingerprint comparisons use one adapter-owned expression comparison
surface. Aggregate-spec equivalence also lives in `lower_expr.zig`, so aggregate
deduplication compares row expressions, order keys, and typed filter predicates
through the same adapter-owned equality helpers. Generated-column,
row-expression, and unique-constraint dependency walkers also live in
`lower_expr.zig`, so schema mutation code asks the adapter which typed catalog
objects reference dropped fields instead of recursively inspecting expression
trees itself. Row-expression determinism and catalog check-expression type
validation also live in `lower_expr.zig`, so partial indexes, generated columns, and
expression checks share the same definition of catalog-safe deterministic and
type-compatible expression trees. DDL catalog validation for relational columns,
primary keys, `CHECK`, foreign-key names, generated columns, unique constraints,
index `INCLUDE` columns, and field-based unique predicates is adapter-owned as
well: lowerers ask `lower_expr.zig` and `binder.zig` to validate the typed
catalog metadata instead of retaining parallel schema-specific helpers beside
the parser. The future larger slice can move the full row-expression parser
once this contract is stable.

The internal flow is always:

```text
SQL text
  -> lexer tokens with source spans
  -> adapter-private PostgreSQL AST
  -> binder and type checker over Antfly schema/catalog metadata
  -> Antfly-native typed plans
  -> API/storage execution
```

Raw SQL must not cross the binder/lowerer boundary. The adapter AST may retain
PostgreSQL syntax details such as aliases, casts, conflict-target spelling, and
DDL clauses, but the durable/backend contract remains typed Antfly structs.
Plan structs should own only the normalized fields needed for execution,
validation, rebuild, repair, and parity fingerprints.

The split should happen incrementally to avoid a risky parser rewrite. First
extract `token.zig` and `lexer.zig` behind the existing entrypoints, preserving
the current zero-copy token behavior, source spans, dollar-quoted literal
handling, and strict placeholder validation. Statement-family dispatch is owned
by the adapter's tokenized/generated-parser path before the large lowerers
create typed plans; the remaining shared family/kind enum definitions live in
`statement_kind.zig`. `WITH` dispatch must parse past the non-recursive CTE list
and classify by the final statement rather than treating every CTE-backed write
as an insert-source plan; CTE-backed
`UPDATE`/`DELETE`/`MERGE` syntax can still fail closed until each family has a
typed execution contract, but it must fail closed under the right native family.
Stable unsupported-shape and adapter-noop reason tokens belong in
`diagnostics.zig`; the parity corpus must reject unknown reason strings, and
adapter-only reasons must not be reused as required-feature classifications.
Golden-plan fingerprint append helpers and
unsupported/adapter-noop plan matching belong in `corpus.zig`, so fixture
validation and generated corpus promotion share the same adapter-owned contract.
Exact string, integer, boolean, token-sum, optional-token-sum, and non-`none`
token predicates for golden plan assertions also live in `corpus.zig` and
reject duplicate or malformed token occurrences, so coverage checks cannot
accidentally pass on substring matches or ambiguous plan summaries; fixture
validation should call those corpus exports directly rather than maintaining
local forwarding wrappers. Exact-token regression tests
for DDL submodes, `EXPLAIN` wrappers, and empty-catalog DDL applicability live
with those corpus helpers. Coverage-token regression tests for DDL booleans,
temporal DDL counts, aggregate zero-count buckets, and truncate identity tokens
also live with the corpus coverage accumulator. Coverage examples that are more
useful as reviewable data, including table identity, pagination tails,
joined-source counts, CTE chain counts, malformed exact-token guards for DDL
booleans, count tokens, side-access tokens, conflict counters, temporal
conflict transforms, insert-source assignment expressions, merge wrappers, and
cross-table source-schema tokens for joined mutation, join, lateral, merge, and
wrapped read classifier plans, live in checked-in fixture JSON under
`sql/fixtures/` and are interpreted by a generic `corpus.zig` runner with named
expected coverage buckets. Expectation names resolve directly against boolean
fields in the coverage accumulator, so adding a reviewable regression case does
not require extending a second Zig-side name map. Required coverage-regression
buckets live in
`sql/fixtures/sql_api_coverage_regression_required_buckets.json`, must stay
sorted, and `sql/fixtures/sql_api_coverage_regressions.json` must reference
each required bucket at least once. Summary-shape regressions
that need named predicate checks live in
`sql/fixtures/sql_api_summary_regressions.json`; `corpus.zig` owns the small
assertion-name map and parser, while the entry data and positive/negative
expectations stay reviewable as JSON. Required summary assertion names live in
`sql/fixtures/sql_api_summary_required_assertions.json`, must stay sorted, and
must enumerate every summary assertion helper; the summary-regression fixture
must reference each required assertion at least once. That keeps regression examples
data-driven without hand-editing the generated parity corpus. SQL/API
parity entries that are pure workload examples move into
`sql/fixtures/sql_api_parity_source_corpus.json`, a hand-maintained source
fixture with the same entry schema as the generated golden fixture. Corpus-wide
coverage expectations live in
`sql/fixtures/sql_api_required_coverage.json`; those names resolve against the
coverage accumulator by exact field name, so strengthening or relaxing a required
coverage bucket is a fixture review instead of a Zig assertion edit. That file
is the only SQL/API corpus completeness checklist; `corpus.zig` may compute
coverage evidence from typed plan fingerprints, summaries, and structured
fixture metadata, but it must not carry a second hand-written list of required
buckets. The parser requires the fixture to enumerate every boolean and counter
accumulator field, which makes newly added coverage dimensions fail closed until
they become
explicit review policy. Migration-equivalence coverage is explicit in that
fixture: source-based insert/update/delete rewrites must stay represented as
data-backfill buckets, and schema changes must retain separate buckets for
metadata-only catalog updates, derived-artifact rebuild work, constraint
validation work, and row-image rewrite/backfill work. Those buckets are derived
from typed plan families and exact `applied_plan` lifecycle tokens, not fixture
names or raw SQL substrings, so a stale corpus entry cannot accidentally satisfy
the migration contract. Data-backfill buckets require typed fingerprint
evidence for the native rewrite shape: insert-source plans must carry exact
source-table and assignment tokens, update-source plans must carry mutation
operation tokens, joined update/delete plans must carry exact source/join
tokens, and delete-source plans must carry the lock/returning tokens that prove
the statement reached the source-driven mutation lowerer. A fixture family label
alone is not coverage. Point update/delete coverage for expression-partial
unique selectors is also derived from validated unique-index setup metadata,
point-write plan tokens, resolver row evidence, and predicate field tokens,
not fixture names. Point update expression-assignment coverage is derived from
exact point-write plan tokens plus parsed `SET field = function(...)`
assignment tokens, not fixture names. Required unsupported/invalid
native-requirement labels
live in `sql/fixtures/sql_api_required_native_requirements.json`, must stay
sorted, and must match every non-noop `classification_reason` used by the
source corpus. That turns remaining native model gaps into explicit reviewable
typed-plan work items instead of letting unsupported fingerprints appear or
disappear silently. Resolved native-requirement labels live in
`sql/fixtures/sql_api_resolved_native_requirements.json` and tie former
model-gap diagnostics to positive typed-plan coverage buckets; when a
fail-closed shape becomes implemented, the source corpus must prove the native
plan coverage instead of leaving a stale unsupported reason behind. The two
native-requirement manifests partition every stable non-noop diagnostic reason:
new reason tokens fail the corpus tests until review policy classifies them as
unresolved work or resolved typed-plan evidence. SQL adapter
edge cases that are not golden typed-plan entries, such as comment preservation,
malformed placeholder suffixes, statement-kind classification, and fail-closed
point-lowerer boundaries, live in
`sql/fixtures/sql_api_adapter_edge_cases.json` and are interpreted by one
generic lowerer runner. Required edge-case buckets live in
`sql/fixtures/sql_api_adapter_edge_required_coverage.json`; those names resolve
directly against `corpus.zig` edge-coverage fields, must stay sorted, and must
enumerate every edge-coverage dimension so adding a new edge class fails closed
until review policy is updated. Each edge-case fixture entry also carries a
sorted `coverage` list with the buckets it proves, including its action bucket
and any required error/write-kind bucket, so coverage evidence remains
reviewable data rather than SQL-substring inference. Those cases should only
move into the source corpus when they assert a durable typed plan, adapter
no-op, or unsupported model-feature classification. Prepared
statement, cursor portal, `EXPLAIN` wrapper, maintenance, transaction-control,
catalog DDL, collation/operator/aggregate/cast catalog syntax, and adapter-only
catalog no-op examples use this path because their expected behavior is fully
described by SQL text, optional setup SQL, summary metadata, and a typed-plan
fingerprint. Comment metadata, drop-table, truncate, truncate fail-closed, and
constraint metadata examples such as deferrable primary/unique keys also use
this path. Pure row-query workload examples, including JSON
path projection/filtering, structured access predicates, schema-qualified table
names, generated-column pushdown setup, alias-qualified outputs, row claims,
statement-time projections, UUID/date projections, boolean predicates,
select-all extras, membership predicates, array predicates, and scalar
`BETWEEN` lowering, plus JSON extraction membership, JSON extraction `OR`
groups, JSON type/array-length/path-existence expressions, and JSON path
function expressions, shared scalar/text expression projections, expression
ordering, null-ordering/pagination variants, output ordinal/alias ordering,
text-pattern predicates, regexp functions, and computed pattern predicates,
boolean/null predicates with fixture-local schema setup, computed predicate
null tests, arithmetic predicates, expression `BETWEEN`, `coalesce`, and numeric
function predicates, belong there as reviewable data as well. The in-code
corpus is intentionally empty; new parity cases should first be modeled through
source-fixture fields, and Zig harness code should only grow when the fixture
schema cannot express the behavior being asserted.
Promotion reads the source fixture and writes the generated
`sql_api_parity_corpus.json`; the generated fixture remains read-only except
through the promotion target. The source parser rejects empty or duplicate
entry names and applies the same family/summary/fingerprint/reason metadata
validation used by the generated fixture, except that generated-only
`applied_plan` fingerprints may be omitted when the promotion path derives
them from typed DDL application. Source data therefore fails before promotion
rather than relying on the generated-fixture validation pass to catch identity
or plan-family drift. Fixture summary-family policy and structural
summary-to-plan assertions, including DDL operation totals, DDL
select/predicate counts, mutation `RETURNING`/conflict guards,
source-assignment counts, merge arms, aggregate summaries, read/query predicate
and access summaries, select/output summaries, join/lateral/window shape
summaries, pagination, and row-claim summaries, also belong in `corpus.zig` so
the checked-in source fixture, generated fixture, and promotion path use one
adapter-owned
interpretation of golden-plan fingerprints. The corpus package
also owns structured applied-plan fingerprint validation and nested
`EXPLAIN` inner-plan matching, so rewrite/rebuild/validation evidence and
explain-wrapper assertions use the same exact-token contract as ordinary golden
plans. Execution-plan fingerprints for native COPY import/export work and
prepared-transaction recovery are also structurally validated before coverage
buckets can use their exact tokens, so bulk I/O and recovery coverage cannot be
satisfied by a prefix-only or duplicate-token fingerprint. The corpus package
also owns strict placeholder coverage
scanning for fixture SQL, including skipped numbers, parameters without
placeholders, placeholders without params, malformed suffixes, quoted
string/identifier bodies, comments, and dollar-quoted bodies. Fixture JSON
encoding, check/promote mode selection, fixture-file verification/promotion,
root metadata validation, fixture entry parsing, and fixture entry/root deinit
also belong in `corpus.zig`, so the checked-in golden fixture has one owner for
format versioning, accepted fields, enum decoding, borrowed JSON strings,
allocated slice ownership, source-entry and skipped-entry counts,
fixture-family reason/summary/source/setup policy, summary shape detection,
summary normalization, source-table fingerprint matching, explain/read prefix
matching, parameter-coverage policy, core fixture metadata validation, and the
coverage accumulator that proves required SQL/API feature buckets from exact
golden-plan tokens. The SQL lowerer layer should only add checks that require executing adapter/catalog
logic, such as deriving applied DDL fingerprints, validating setup SQL against a
runtime schema, or proving source-schema table identity.
The shared parser cursor belongs in `parser.zig`, so checkpoint/restore,
expect, match, peek, identifier-predicate matching, function-call detection,
balanced-paren normalization, wrapped identifier operands, top-level boolean
operator scans bounded by tail clauses, top-level tail-clause index scans, and
end-of-input behavior are adapter-owned before statement-specific grammar moves
out of the large lowerer file. Adapter-only control syntax such as `EXPLAIN`
prefix and option parsing belongs in grammar helpers so lowerers receive typed
wrapper intent instead of scanning raw SQL. This boundary is now partially implemented:
`sql/` owns token definitions, lexer behavior, parser cursor
helpers, statement classification, diagnostics, parity-corpus fingerprints,
catalog/source-name prebinding through parsed bound statements,
catalog-independent object identifier
normalization, owned identifier/list parsing, adapter-noop grammar tails,
`EXPLAIN` prefix and option grammar, row-security grammar syntax for
`ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY`,
`CREATE POLICY ... USING (...)`, and `DROP POLICY ... ON`,
trigger-policy grammar syntax for
`CREATE TRIGGER ... BEFORE UPDATE ... EXECUTE FUNCTION/PROCEDURE` and
`DROP TRIGGER ... ON`,
row-lock grammar for `FOR UPDATE` / `FOR NO KEY UPDATE` /
`FOR SHARE` / `FOR KEY SHARE` including `OF`, `NOWAIT`, and `SKIP LOCKED`
tails, cursor portal grammar for `DECLARE` scroll/hold prefixes plus `FETCH`
direction/count and `CLOSE ALL` tails, transaction-control grammar for `LOCK TABLE`,
`SET CONSTRAINTS`, `SET TRANSACTION`, `START TRANSACTION`, and `BEGIN`
mode clauses plus advisory-lock function-call tails, including multi-table lock
modes, named/all constraint modes, isolation/access/deferrable transaction
metadata, and one- or two-key advisory locks, maintenance-job grammar for
`VACUUM`, `ANALYZE`, `REINDEX`, and
`CLUSTER`, including vacuum options, analyze columns, reindex concurrency, and
cluster index/verbose metadata, notification-channel grammar for `LISTEN`, `NOTIFY`, and
`UNLISTEN`, database/tablespace catalog grammar for `CREATE DATABASE`, `ALTER
DATABASE ... SET`, `DROP DATABASE`, `CREATE TABLESPACE`, `ALTER TABLESPACE ...
RENAME TO`, and `DROP TABLESPACE`, including database settings/forced drops and
tablespace location/idempotent-drop metadata, bulk I/O grammar for `COPY ... FROM` and
`COPY ... TO`, schema-namespace catalog grammar for `CREATE SCHEMA`, `ALTER
SCHEMA ... RENAME TO`, and `DROP SCHEMA`, including idempotent create and drop
dependency metadata, extension catalog grammar for
`CREATE EXTENSION`, `ALTER EXTENSION ... UPDATE`, and `DROP EXTENSION`,
including idempotent/versioned create, targeted/latest update, and drop
dependency metadata,
authorization catalog grammar for `CREATE ROLE`, `ALTER ROLE ... SET`,
`DROP ROLE`, `GRANT`, and `REVOKE`,
logical-replication catalog grammar for `CREATE PUBLICATION`,
`ALTER PUBLICATION ... ADD TABLE`, `DROP PUBLICATION`,
`CREATE SUBSCRIPTION`, `ALTER SUBSCRIPTION ENABLE/DISABLE`, and
`DROP SUBSCRIPTION`, including table-list/all-tables publications,
multi-publication subscriptions, enable/disable state, and idempotent drops,
type-system catalog grammar for collation, operator, aggregate, and cast
create/drop/rename forms, including option counts, operator/aggregate
signatures, cast function/assignment metadata, and idempotent collation drops,
routine catalog grammar for `CREATE FUNCTION`, `CREATE PROCEDURE`,
`DROP FUNCTION`, and `DROP PROCEDURE`, plus native routine-catalog application
for the fail-closed subset of SQL-language expression functions. Routine
catalog records must preserve the full typed plan metadata, including
overload arity, return type, language, volatility, security, null-input policy,
parallel safety, leakproof/window flags, support/transform metadata, settings,
cost/rows hints, and the shared expression AST for executable safe bodies. The
runtime path may execute only typed expression-body plans, and
`ApiHttpServer.lowerRelationalSqlReadPlanWithRoutineBindingsAlloc` exports the
current routine runtime bindings into catalog-aware SQL read planning so
`CREATE FUNCTION ... LANGUAGE sql AS 'SELECT ...'` bodies can be used by the
ordinary row-expression lowerer. Narrow safe PL/pgSQL trigger bodies that
return `NEW` or `OLD` are stored as typed routine hooks rather than opaque
procedure text, and cataloged trigger DDL records bind those hooks to table
events through the routine runtime. Narrow safe PL/pgSQL procedure bodies that
only perform no side effects (`BEGIN NULL; END` and notice-only equivalents)
are stored as `procedure_noop` hooks and the routine runtime can execute those
procedure records directly after durable catalog recovery; SQL `CALL
procedure_name()` lowers to a typed `procedure_call` plan and `ApiHttpServer`
executes only that recovered no-argument noop hook, while argument-bearing
calls and side-effecting bodies remain fail-closed. Unsupported
procedural bodies, cascaded routine drops, and unsupported expression forms
fail closed until they have native typed execution contracts.
sequence catalog grammar for `CREATE SEQUENCE`, `ALTER SEQUENCE`, and
`DROP SEQUENCE`, including idempotent create, `IF EXISTS` alter, owned-by/type
metadata, and drop dependency metadata,
enum-type catalog grammar for `CREATE TYPE ... AS ENUM`, `ALTER TYPE ... ADD
VALUE`, and `DROP TYPE`, including idempotent positioned enum additions and drop
dependency metadata,
domain catalog grammar for `CREATE DOMAIN ... AS`, `ALTER DOMAIN`, and
`DROP DOMAIN`, including default/not-null/check metadata and drop dependency
metadata,
comment metadata grammar for `COMMENT ON TABLE`, `COMMENT ON COLUMN`,
`COMMENT ON INDEX`, and `COMMENT ON CONSTRAINT`,
drop-catalog grammar for `DROP TABLE`, `DROP TABLE IF EXISTS`, `DROP INDEX`,
`DROP INDEX CONCURRENTLY`, `DROP VIEW`, and `DROP MATERIALIZED VIEW`,
create-index header grammar for
`CREATE INDEX [CONCURRENTLY] [IF NOT EXISTS] name ON table [USING ...]`
and index-element suffix grammar for supported opclasses plus
`ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`,
alter-table header grammar for `ALTER TABLE [IF EXISTS] [ONLY] name`,
alter-table operation grammar for `VALIDATE CONSTRAINT name` and
`RENAME [COLUMN|CONSTRAINT] old TO new`, and
`DROP [COLUMN|CONSTRAINT] [IF EXISTS] name [CASCADE|RESTRICT]`, and
`ALTER [COLUMN] name SET/DROP NOT NULL`, and
`ALTER [COLUMN] name SET/DROP DEFAULT`, and
`ALTER [COLUMN] name TYPE ...` / `SET DATA TYPE ...` plus optional
identity `USING column` / `USING (column)` casts,
`ADD COLUMN [IF NOT EXISTS] ...`, and `ADD PERIOD` /
`ADD [CONSTRAINT name]` operation prefixes,
identity-allocator table header grammar for
`CREATE TABLE name (identity_column ... IDENTITY/SERIAL ...)`, including
`serial`, `bigserial`, and `GENERATED ALWAYS/BY DEFAULT AS IDENTITY (...)`
allocator specs,
view catalog grammar for `CREATE VIEW`, `CREATE MATERIALIZED VIEW`,
`ALTER VIEW ... RENAME TO`, `DROP VIEW`, `REFRESH MATERIALIZED VIEW`, and
`DROP MATERIALIZED VIEW`, including replace/idempotent/create-populate,
refresh-concurrency/populate, and drop dependency metadata, and table-clone
grammar for `CREATE TABLE ... (LIKE source INCLUDING ...)`,
create-table header grammar for `CREATE TABLE [IF NOT EXISTS] name (...)`,
create-table element prefix grammar for column entries and
`[CONSTRAINT name]` table constraint entries, plus inline column constraint
prefix grammar for `[CONSTRAINT name] PRIMARY KEY`, `UNIQUE`, `CHECK`, and
`REFERENCES`, and DDL constraint suffix grammar for `NOT VALID`,
`NULLS [NOT] DISTINCT`, primary/unique/foreign-key deferrability and
`INITIALLY IMMEDIATE` / `INITIALLY DEFERRED` timing metadata, constraint
column lists, `INCLUDE (...)`, temporal `WITHOUT OVERLAPS` column lists, and
foreign-key column lists plus option tails for `MATCH SIMPLE`, `MATCH FULL`,
`ON DELETE`, `ON UPDATE`, and deferrability, DDL `COLLATE` clauses, DDL
type-name/modifier grammar, and
`CURRENT_TIMESTAMP(p)` precision grammar, `PERIOD FOR name (start, end)`
constraint grammar, and stored generated-column value grammar for
`ALWAYS AS (...) STORED` over the deterministic row-expression subset used by
generated-column metadata, including legacy shorthand forms for
`lower(field)`, `upper(field)`, `md5(field)`, `concat(field, separator, ...)`,
and `concat_ws(separator, field, ...)`, broader scalar/function/arithmetic
expressions such as `replace(lower(field), ' ', '-')`, and JSON path extraction
such as `jsonb_extract_path_text(metadata, 'source')`, unique-expression grammar for
`lower(field)`, `upper(field)`, and `md5(field)` plus harmless expression-index
parenthesis wrappers, plus known DDL default grammar for `NULL`,
`gen_random_uuid()`, `uuid_generate_v4()`, `now()`,
`CURRENT_TIMESTAMP(p)`, and `CURRENT_DATE`,
partition catalog grammar for `CREATE TABLE ... PARTITION BY RANGE (...)`,
`CREATE TABLE ... PARTITION OF ... FOR VALUES`,
`ALTER TABLE ... ATTACH PARTITION`, and `ALTER TABLE ... DETACH PARTITION`,
relation-lifetime prefix grammar for `CREATE TEMP`, `CREATE TEMPORARY`, and
`CREATE UNLOGGED TABLE`,
truncate mutation-source grammar for single-table `TRUNCATE` with
`CONTINUE IDENTITY` or explicit `RESTART IDENTITY`,
prepared-statement subject classification for read, write, and DDL
subjects, including CTE-backed subjects classified by their final
non-recursive statement and `MERGE` as a write subject, and relation-population syntax
parsing for `SELECT INTO` and
`CREATE TABLE AS`; `sql/plan.zig` owns the lowered read-plan,
write-plan, explain-plan, write-plan option, merge-arm, returning-projection,
and relation-population containers that wrap Antfly-native typed query,
aggregate, join, lateral, window, CTE, insert, update/delete, insert-source,
mutation-source, joined-mutation-source, and merge mutation plans, plus shared
row-expression clone, source-rewrite, query-order clone, predicate-group,
access-predicate-group, query-predicate clone, window-spec, and deinit helpers
used across parser cleanup paths and lowered-plan ownership, plus projection
and join deinit/conversion helpers that operate only on Antfly-native storage
plan structs, while preserving the existing public entrypoints through facade
aliases;
`sql/ddl_plan.zig` owns the adapter-noop DDL, enum-type catalog,
domain catalog, identity-allocator catalog,
schema-namespace catalog, extension catalog, function/routine catalog, and
authorization catalog, sequence catalog, database catalog, tablespace catalog,
notification channel, logical replication, type-system catalog, maintenance
job, bulk I/O, create-table, create-index, table-clone, view catalog,
materialized-view catalog, row-security catalog, update-policy, drop-table,
drop-index, prepared-statement, cursor/portal, savepoint transaction, comment
metadata, and transaction-control plan containers, with the remaining DDL
families scheduled to move through the same facade as their lowerers are
extracted;
`sql/lower_expr.zig` owns the expression keyword, function-name, and
tail-boundary predicates that the shared expression lowerer uses, plus the
token-level rule that distinguishes a parenthesized boolean predicate group
from a parenthesized scalar expression followed by comparison, pattern, range,
membership, or JSON/path operators;
The SQL lowerer modules keep the public lowering entrypoints and map adapter
syntax into Antfly-native typed plans. Row-lock target
validation remains in the lowerer/binder boundary because it depends on table
aliases and catalog-normalized object names, but the SQL grammar emits only the
native row-claim mode and wait-policy struct used by REST/SDK plans. Continue extracting the shared expression grammar,
because expressions are reused by SELECT, DML, DDL checks, partial indexes,
defaults, generated columns, conflict actions, and RETURNING. The binder boundary should
own catalog source lookup, cross-table source-name pre-scans for read,
insert-source, and joined-write statements, non-recursive CTE source-name
resolution, cross-table source schema derivation, identifier normalization, and
scope resolution so statement lowerers consume typed schemas instead of
re-reading metadata snapshots directly. Default SQL catalog source-schema lookup
now binds unqualified table references to `default.public.<table>` instead of
first-match table names, and the adapter binder exposes a qualified runtime
schema lookup for future explicit catalog targets. Then
move statement families one at a time into AST plus binder plus lowerer modules,
with the existing SQL/API parity tests as the acceptance gate. Only after those
boundaries are stable should a generated grammar be considered.

Efficiency follows those same boundaries. Lexer and parser data should be
statement-scoped and arena-allocated where useful. Token text should reference
the original SQL buffer unless unescaping or cast normalization requires owned
text. AST nodes should avoid copying identifiers and literals until binding
normalizes them. Final typed plans should own their execution data because they
outlive the parser. Expression lowering should be shared instead of duplicating
operator precedence, type checks, or function semantics across SELECT, DML, DDL,
and index paths.

Grammar coverage should land incrementally and remain fail-closed. Statement
families can move from the current handwritten lowerers into the grammar one at
a time: DDL and catalog operations, row queries, row-batch mutations, claimed
mutation sources, joins, CTEs, lateral plans, aggregates, windows, and
insert-source flows. Each migration step keeps the existing typed lowerer and
SQL/API parity corpus as the acceptance gate. A supported SQL form must lower to
a REST/SDK-visible plan or catalog operation; an adapter-only no-op must be
named; an unsupported form must carry a stable required-feature reason. No
grammar rule may route raw SQL into storage.

If parser code is generated from a grammar, the checked-in grammar is the source
of truth and generated parser artifacts are updated only through the repository
generation target and checked by CI. Hand edits belong in the grammar, lexer
helpers, semantic binding, or typed-plan lowerers, not in generated files.

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

Read-side SQL adapter entrypoints classify supported `SELECT` statements into
one of the native typed read families before execution: row-query/CTE,
aggregate, equality join, bounded lateral, or window. The specialized lowerers
remain the implementation units for those families, but application-facing SQL
planning should use that classifier boundary so callers do not need to know that
`SELECT ... JOIN ...`, `SELECT ... OVER (...)`, and ordinary base-row reads land
in different typed request envelopes. The catalog-backed read-plan entrypoint
resolves direct and non-recursive CTE-backed cross-table equality join and
bounded lateral source schemas from the catalog before invoking the same typed
lowerers; its SQL pre-scan is limited to target/source table identity and the
full parser still owns shape validation, expression binding, and fail-closed
behavior. Recursive CTEs and CTE bodies that mix physical source tables fail
closed at the binder boundary rather than guessing a schema. Unsupported shapes
still fail closed; SQL text is never carried through storage as the backend
representation.

Write-side SQL follows the same boundary. The adapter classifies supported
`INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` statements into native write
families before execution: row-batch insert, point row-batch update/delete,
claimed mutation-source update/delete, claimed joined mutation-source
update/delete, or table-emptying mutation-source truncate. Point writes are used
only when the predicate resolves through primary or enforced unique identity;
broader predicates and table-wide raw updates/deletes route through explicit
claimed mutation-source plans. The point update/delete lowerers still fail
closed for missing predicates so adapter callers cannot accidentally turn an
identity mutation into an unclaimed scan.
For CTE-backed writes, adapter classification is based on the final statement
after the non-recursive CTE list. `WITH ... INSERT` routes to insert-source
lowering, while `WITH ... UPDATE`, `WITH ... DELETE`, and `WITH ... MERGE` route
to their own write families. Native joined mutation-source requests now carry a
bounded `ctes` list, but only the non-target/source side may read from a CTE.
The target side remains a real table query so row claims, version predicates,
OCC writes, identity rewrites, and transaction participants stay grounded in
durable row identity. Planning materializes source CTEs under the same row/byte
caps as read plans, validates source-side join fields against the materialized
output, rejects ambiguous source-to-target matches, and then stages selected
target preimages through the ordinary owner-local mutation-source path. Source
CTEs that are unreferenced, missing, recursive, over cap, target-side, or mixed
with embedded doc-key ranges fail request validation. SQL lowering now maps
source-side CTE update/delete forms into that native request when the CTE is
non-recursive and source-side only, including direct joined forms such as
`WITH ... UPDATE ... FROM source_cte` / `WITH ... DELETE ... USING source_cte`
and semijoin selectors such as `WHERE id IN (SELECT id FROM source_cte)`.
Non-recursive `WITH ... MERGE` now lowers into the same typed MERGE plan with
owned source CTE metadata and a source-side CTE reference, so the source stream
remains an Antfly row-plan input rather than SQL text attached to execution.
Local execution materializes that source CTE through the ordinary row-plan
reader, including catalog-resolved cross-schema source tables, before feeding
the resulting typed source rows into the deterministic MERGE batch builder.
Mutation-producing MERGE CTEs use the same rule at the producer boundary:
`WITH source_rows AS (UPDATE ... RETURNING ... | DELETE ... RETURNING ...)
MERGE ... USING source_rows` lowers the CTE body into an owned
mutation-source request beside the MERGE plan, records the CTE name and
returning shape, and makes the final MERGE consume `source_rows` through a
typed `source_cte` reference. The SQL adapter currently marks that producer
with an explicit claim placeholder because the coordinator, not the parser,
must bind the real row-claim owner and transaction before execution. This keeps
the long-term runtime shape honest: data-modifying CTE producers are native
mutation-source work items, not embedded SQL strings, and promotion to
executable work is responsible for replacing placeholder claim metadata with a
real transaction participant.
Recursive CTE-backed `INSERT ... SELECT` has its own typed write envelope: the
SQL adapter lowers `WITH RECURSIVE ... INSERT ... SELECT ...` into a recursive
producer plus an insert-source request, the catalog resolver loads the
recursive anchor's base source schema for cross-table inserts, and execution
materializes the bounded recursive rows under the same row/byte caps before the
final source projection feeds deterministic target-row planning. Bound table
write sources can also stage claimed joined mutations from pre-materialized
source rows through the ordinary target-row claim/OCC path. Recursive
CTE-backed SQL claimed update/delete and MERGE now lower into explicit
recursive-source envelopes over those native primitives: the wrapper owns the
bounded recursive producer, the inner mutation consumes the materialized CTE by
typed `source_cte`, and execution feeds the deterministic target-row claim/OCC
or MERGE batch builders without passing SQL text through storage.
The public SQL execution path applies that shape to recursive claimed
update/delete and recursive MERGE by materializing the recursive producer under
the read-plan caps, filtering the CTE rows through the typed row-query
evaluator, and passing the resulting source rows to the joined
mutation-source autocommit path or deterministic MERGE row-batch builder.
The catalog-backed write-plan entrypoint also resolves direct joined
`UPDATE ... FROM` and `DELETE ... USING` source schemas from table metadata
before lowering into the same claimed joined mutation-source typed requests.

`INSERT ... SELECT` is not treated as row-batch sugar because it needs a
first-class native insert-source plan: a typed source query, target-column
mapping, optional conflict action, final-image `RETURNING`, duplicate-target
detection, and the same owner/transaction routing guarantees as mutation-source
updates and deletes. The REST/SDK contract now exposes that shape as
`RowsInsertSourceRequest`: the endpoint validates source queries, target-column
assignments, conflict shape, authorization filters, and `RETURNING` through the
shared `RelationalRowsInsertSourceRequest` storage type. Bound single-DB write
sources execute same-table insert-source requests by reading the typed source
query, projecting target rows through the shared row-expression AST, lowering
the result into the normal row-batch write path, and returning final inserted
rows from the planned image. Native insert-source execution supports typed
primary/unique `ON CONFLICT DO NOTHING` and `DO UPDATE` through the same planned
proposed-row image used by ordinary row-batch inserts, with duplicate proposed
conflict-target detection before staging. Ordinary row-batch inserts use the
same proposed-target tracking: `DO NOTHING` skips later duplicate proposed
targets, while `DO UPDATE` fails closed because it would otherwise stage two
updates for one logical row. The SQL adapter now lowers
field-projected and expression-projected same-table
`INSERT INTO target (...) SELECT ... FROM target ... [ON CONFLICT (...) DO NOTHING|DO UPDATE ...] RETURNING ...`
statements to that native insert-source plan, including proposed-row
`excluded.field` conflict expressions and conflict-action guards over the
existing/proposed row images. When a conflict-action guard evaluates false, the
row is skipped and emits no `RETURNING` row. Conflict-action guards share
row-query predicate groups for scalar `OR`/`NOT` and null-inclusive boolean
`IS NOT TRUE` / `IS NOT FALSE` branches, so guarded upserts stay on typed
predicates instead of backend-specific boolean operators. `RETURNING *` and
expression-returning outputs such as `RETURNING *, lower(status) AS status_key`
project from the same planned inserted image used by ordinary row-batch writes.
SQL/API parity fingerprints for row-batch inserts and updates include total
transform-op counts plus native storage operation-family suffixes such as
`op_set`, `op_inc`, `op_push`, and `op_pull`, so conflict actions, JSON/array
transforms, and ordinary patch updates cannot collapse to the same golden write
plan when their row-batch effects differ. The corpus coverage gate requires
representative row-batch insert and update plans with those operation-family
suffixes, so removing the native transform signal is a CI-visible typed-plan
contract regression.
Ordered SELECT outputs lower to typed target assignments in projection order;
computed outputs such as `lower(status)` or `amount + 1` stay in the shared
row-expression AST with source-row field bindings before the inserted image is
planned. Source filters use the same typed row-query predicate surface as reads,
including computed text pattern sets such as `lower(status) LIKE ANY/SOME(ARRAY[...])`
and expression-backed `OR`/`NOT` groups such as
`array_length(tags, 1) NOT BETWEEN ...`, so insert-copy operations do not carry
backend SQL predicates into storage. The native insert-source validator and
row-batch builder can also bind the target table and source table
against separate runtime schemas. Native insert-source plans also accept ordered
row-query CTEs and range declarations around the source stream; source
assignments are validated against the materialized CTE output schema, and CTE
streams that do not preserve a primary key are still valid because insert-source
uses source rows as values rather than claimed source-row identities. The native
execution helper materializes those CTEs in dependency order, enforces their row
and byte caps, evaluates the final insert-source stream against either the base
source rows or the selected materialized CTE, and then lowers projected target
rows into the normal row-batch path. The SQL adapter lowers non-recursive
`WITH ... INSERT INTO ... SELECT ...` forms onto that same native plan, records
the base source table separately from the final `source_cte`, and binds target
assignments against the CTE output schema rather than the base table when the
final source is materialized. Catalog-backed write-plan lowering applies the
same rule before parsing the final source projection: a cross-table CTE chain
loads the base source table schema from the catalog, then validates the final
insert-source assignments against the materialized CTE output. Public
typed insert-source execution now uses that boundary for catalog-backed
cross-table sources: the source table is resolved through catalog ownership,
owner-clipped source rows are collected with the source schema, global source
ordering/pagination is applied once at the coordinator, projected target rows
are lowered into the normal row-batch path, and target writes route to target
owners. Cross-table insert-source `ON CONFLICT` uses the same routed
primary lookup and durable unique-owner lookup as row-batch conflict handling
before it builds transforms, so `DO NOTHING` skips existing primary or unique
targets and `DO UPDATE` fetches the committed row/version before staging an OCC
guarded transform. The routed write then reuses the same primary/unique/FK
enforcement and 2PC participant path as ordinary row batches. The SQL adapter
has a catalog-backed write-plan entrypoint for field-projected cross-table
`INSERT ... SELECT`: it pre-scans the target/source table names only to load the
source table schema from the catalog, then runs the same typed insert-source
lowerer and fail-closed validation as explicit REST/SDK callers.

PostgreSQL `MERGE` lowers to an explicit typed matched-mutation adapter plan for
the first deterministic production shape: one target table, one source table, a
target/source equality match key, a `WHEN MATCHED THEN UPDATE SET ...` arm whose
assignments come from direct source-field copies or type-bound target/source
row expressions, a `WHEN MATCHED THEN DELETE` arm, and a
`WHEN NOT MATCHED THEN INSERT (...)` arm whose proposed row fields come from
source fields, literals, or source-row expressions. Not-matched insert
expressions reject target-row references because no target preimage exists.
`WHEN MATCHED THEN DO NOTHING` and
`WHEN NOT MATCHED THEN DO NOTHING` lower to explicit no-op arm metadata rather
than disappearing from the plan. Matched and not-matched arms may carry bounded
typed scalar predicates over side-qualified target or source fields and literal
values; direct typed expression predicates over target/source row expressions
are also supported, including typed expression `OR`/`NOT` predicate groups.
Not-matched predicates are source-side only and reject target-row references
because there is no target preimage. Unsupported arms, duplicate arms,
cross-field predicate RHS values, ambiguous qualifiers, and missing source/target
fields fail closed before the plan reaches storage. `RETURNING` lowers to
target-qualified fields,
`RETURNING *`, and expression projection metadata used by other typed write
plans. The accepted shape does not lower to a loose mix of insert-source plus
joined update syntax; the adapter exposes the target table, source table, match
fields, arm predicate metadata, matched-update field mappings, matched-delete
metadata, and matched/not-matched no-op metadata, plus not-matched insert field
mappings and matched/not-matched expression assignments as one typed write-plan
family.

Bound/local MERGE execution uses a typed collected-row batch builder. The local
DB helper first collects target and source preimages through ordinary
`RelationalRowsQueryRequest` streams, preserving their physical key, committed
JSON image, version, order/distinct/limit semantics, and any typed source or
target filter. The same helper accepts independent target/source doc-key range
sets, so local execution can model the exact preimage slices that routed
owner-range fanout will collect from each table. Target and source read sources
may be separate stores, so schemas stay separate all the way through collection
and source rows keep their own physical identity even when the merge key points
at a target primary key.
It then passes those collected rows to the deterministic MERGE batch builder:
match keys are evaluated in the typed plan, matched and
not-matched scalar or expression arm predicates can skip rows,
many-source-to-one-target matches fail before any intent is prepared, matched
updates/deletes emit ordinary row-batch update/delete operations with
expected-version OCC predicates, and unmatched inserts emit ordinary row-batch
inserts. Target/source expression
assignments are evaluated over the collected source and target row images before
building the row-batch patch or insert row; explicit MERGE `DEFAULT`
assignments resolve through the target column's Antfly default and are stored as
literal typed expression values in the MERGE plan. Final writes still run
through the normal row-batch validator. Field-copy `RETURNING` and
`RETURNING *`, plus row-local expression `RETURNING`, are projected by the same
row-batch returning path used by direct insert/update/delete. Applying the MERGE
result therefore still flows through `db.batch`, preserving generated columns,
defaults, checks, secondary-index maintenance, FK hooks, and durable transaction
predicates.

MERGE now has the same typed source/target preimage contract over routed
table/range ownership that local execution uses. The routed helper collects
target and source rows through their respective table read sources, applies the
typed source and target row-query filters with full-row projection, looks up
selected target rows to attach committed versions for OCC predicates, and feeds
those preimages into the same deterministic MERGE batch builder used by local
storage. Temporal target tables use the same period-qualified primary selector
path as ordinary row APIs, so matched updates/deletes resolve the exact
`WITHOUT OVERLAPS` interval instead of collapsing every interval with the same
scalar key into one target. Catalog-backed MERGE lowering resolves a
cross-table source schema from the table catalog before validating source-field
assignments, so SQL text cannot smuggle source columns past the typed
source/target schema boundary. Public `/db/v1/sql` execution now uses this shape
for direct table-source MERGE, non-recursive source-CTE MERGE, and recursive
CTE-backed MERGE: it collects typed preimages through public read sources,
materializes source CTEs through the typed row-query planner, builds an owned
row-batch mutation, and applies that batch through the ordinary public row-batch
authorization, trigger, transaction, and commit path. Routed non-recursive
source-CTE MERGE preserves catalog-owned source range slicing by passing
owner-clipped source ranges through the same CTE row-query plan before the
deterministic MERGE batch builder sees source preimages. Data-modifying-CTE
MERGE producers remain fail-closed until their materialized write output can be
bound to row claims and enter the same preimage-to-row-batch boundary.
Remaining production MERGE work is owner-group topology hardening: provisioned/hosted
write execution needs broader chaos coverage around matched target intents and
unmatched target inserts while range movement races collection, staging,
topology epoch validation, and retry/fail-closed behavior. MERGE
arms are modeled as ordered typed arrays: the parser preserves every
`WHEN MATCHED` and `WHEN NOT MATCHED` arm in input order, and the batch builder
evaluates the first matching arm for each source row. Arms can independently
carry scalar predicates, expression predicates, grouped expression predicates,
updates, deletes, inserts, and `DO NOTHING`. Future extensions should keep
topology-epoch retry/fail-closed behavior aligned with joined mutation-source
and insert-source execution.
Direct and routed MERGE CTE source execution, plus recursive insert-source,
MERGE, and joined mutation-source CTE execution, reapply typed source filters,
ordering, offset, and limit exactly once with projection fields cleared before
feeding materialized rows into assignments, join matching, or row-batch staging,
so source CTE projections cannot replace the full row shape required by the
native write contract. Storage and JSON-plan aggregate, join, lateral, window,
and joined-mutation full-row source helpers clear every projection-only field
before internal evaluation, so derived-stage execution does not accidentally
consume projected rows where committed base rows are required.

Given the composite identity, durable unique-owner, foreign-key ownership,
schema-controller repair/promotion, secondary-index lifecycle, embedded
JSON-column indexing, and 2PC participant work in this PR, the remaining
PostgreSQL-shaped SQL surface and broader relational production work should be
tracked against the following roadmap.

### Production hardening roadmap

The remaining relational work is no longer basic feature plumbing. The model has
typed base rows, relational cells, primary/unique/FK metadata, row-batch writes,
mutation-source DML, schema rewrite jobs, table-emptying barriers, typed
expressions, joins, CTEs, windows, aggregates, and derived-index metadata. The
remaining work is making those typed contracts survive production conditions:
distributed movement, large durable jobs, rewrite/rebuild promotion,
spill/backpressure, deeper planner trust, and chaos-backed evidence.

The meaningful remaining work is:

1. **Schema and migration lifecycle.** Apply every migration-equivalent schema
   change as one durable catalog lifecycle: schema JSON update,
   validation/rebuild/rewrite/table-emptying jobs, promotion, retry, rollback,
   and observability. `CREATE OR REPLACE TABLE`, `DROP TABLE ... CASCADE`,
   non-additive rewrites, table-generation promotion, and large truncate/delete
   barriers need range-movement chaos coverage, cancellation/abort semantics,
   progress reporting, backpressure, and storage-specific identity allocator
   reset state behind the `TRUNCATE ... RESTART IDENTITY` boundary.
2. **Routed reads and distributed query execution.** Define row-version
   visibility across remote owners during live writes and range movement. Add
   routed owner-stream strategy planning from cardinality/index hints, streaming
   and backpressure over durable CTE spill, routed/window spill for workloads
   beyond current bounded caps, and topology-change hardening for hosted remote
   lateral, join, and window stages.
3. **Planner and index trust.** Grow expression-implication proofs for partial
   and expression indexes plus partial unique constraints; route
   expression-derived rebuild/promotion through the same generation-aware
   catalog compare-and-swap lifecycle as ordinary indexes; and add durable
   ordered-composite access-path metadata with explicit collation/null semantics
   instead of treating accepted ordered clauses as membership-only metadata.
4. **DML, conflict, and queue hardening.** Add range-movement chaos coverage
   around routed insert-source conflict lookup/staging and MERGE
   target/source collection. Harden mutation-source row claims with owner/lease
   retry behavior across range movement. Keep non-unique selectors on explicit
   claimed mutation-source paths, and keep claims illegal over joins,
   aggregates, windows, and materialized CTEs until those stages expose a
   lockable base-row source.
5. **Temporal and system-versioned relational semantics.** Expand chaos
   coverage that combines `FOR PORTION OF` DML, temporal FK checks, range
   movement, repair, and catalog promotion. Harden distributed retention policy
   for system-versioned history, broaden distributed `AS OF` visibility evidence
   across range movement and remote owner failure, extend distributed/remote CTE
   composition beyond the current table-read paths, and add broader distributed
   set-operation chaos/evidence on top of the now-typed system-time read paths.
6. **Derived multimodal indexes.** Add distributed orchestration for large
   derived-index builds: remote worker leases, retry accounting, cross-node
   recovery evidence, and operational controls for multi-index rebuilds. Keep
   proving managed AKNN, graph extraction, graph metrics, hybrid search, and
   embedded JSON-derived artifacts rebuild from committed relational rows only.
7. **External and lake relational sources.** Extend active Iceberg position and
   equality-delete evidence across every derived lake stage, add spill and
   backpressure for large external aggregates, joins, laterals, windows, and set
   operations, and keep lake execution on typed row-query/materialized-row
   contracts instead of storage-specific shortcuts.
8. **Expression, JSON, and array consolidation.** Continue moving remaining
   scattered SQL lowerer cases onto the shared typed row-expression AST. Route
   remaining JSONB and array operators through typed JSON/array nodes, expose
   the same selectors and transforms through REST/SDK request shapes, and ensure
   embedded JSON schema or dynamic-template changes always schedule catalog
   rebuild work.
9. **Triggers, routines, and policies.** Keep opaque PL/pgSQL rejected. Compile
   only deterministic trigger and routine patterns into typed mutation-hook or
   policy metadata, and broaden support only where the runtime has native typed
   execution semantics.
10. **SQL frontend and catalog-control boundary.** Finish the generated parser
    and phase-separated SQL frontend cutover: tokenization, parsed AST,
    binding, logical planning, span-first diagnostics, SQLSTATE/command-tag
    compatibility, fixture summaries, and variant-based dispatch instead of
    parser probes or SQL-string scans. Catalog-aware work must bind session
    state, `current_database`, `search_path`, object versions, dependencies,
    and role authorization before planning. Non-row SQL surfaces such as
    database/namespace/tablespace DDL, role and grant SQL, extension/routine
    lifecycle, sequences and identity allocators, view/materialized-view
    catalogs and refresh jobs, temporary/unlogged/table-population relation
    intents, custom type-system catalogs for collations, operators, casts,
    aggregates, enums, and domains, comment metadata, notification channels,
    logical replication catalogs, maintenance jobs, bulk I/O, backup/restore,
    lake/foreign-source control, and session commands should keep lowering into
    explicit typed catalog, role, extension, job, storage, or session plans.
11. **Evidence gates.** Grow SQL/API parity from golden plan coverage into
    mixed execution workloads. Add chaos/sim workloads that combine live writes,
    FK actions, unique-owner repair, secondary and embedded-JSON rebuilds, row
    claims, joins, aggregates, range movement, catalog promotion, and native
    rewrite/backfill jobs.

The detailed track tables below decompose this roadmap into SQL/API surfaces,
current native model homes, long-term production plans, and completion signals.

PostgreSQL 19-style application-time temporal table support is tracked as a
native model feature, not as raw SQL passthrough. The reference feature shape is
`PERIOD FOR`, primary/unique keys with `WITHOUT OVERLAPS`, temporal foreign keys
with `PERIOD`, and `UPDATE` / `DELETE ... FOR PORTION OF`, as summarized in
https://www.pgedge.com/blog/looking-forward-to-postgres-19-its-about-time.
Antfly stores the period catalog in the runtime schema, lowers temporal
primary/unique/FK DDL into typed metadata, exposes mutation-source
`temporal_portion`, and applies migration-equivalent temporal DDL through native
`ALTER TABLE` operations. `ALTER TABLE ... ADD PERIOD FOR ...` appends typed
period catalog metadata after validating its boundary columns,
`ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (..., period WITHOUT OVERLAPS)`
installs typed primary-key metadata for schemas created through non-SQL API
paths, and `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE (..., period WITHOUT
OVERLAPS)` uses the same no-overlap unique metadata and validation/rebuild
workflow as create-table DDL. `CREATE UNIQUE INDEX ... (key, period WITHOUT
OVERLAPS)` is treated as the same unvalidated temporal unique metadata as an
add-constraint migration, so index-style migrations use the durable temporal
owner rows rather than a separate SQL-only index model. The SQL/API parity corpus
pins representative temporal parent/child DDL, incremental temporal DDL,
temporal DML, temporal unique conflict upserts, and range-column query lowering.
The parity coverage asserts
range-column insert lowering for `numrange`, bounded `daterange`, upper-open
`daterange`, lower-open `daterange`, `tsrange`, and `tstzrange`, boundary
projection/order lowering for `lower(period_column)` and `upper(period_column)`,
`period_column @> point` containment lowering, `period_column && range_value`
overlap lowering, and `FOR PORTION OF` update and delete mutation-source
fingerprints. PostgreSQL range-column DDL lowers to explicit nullable boundary
columns, so a non-null range value can have an unbounded lower or upper endpoint
without forcing fake sentinel values into the public row JSON. Canonical
`numrange`, `daterange`, `tsrange`, and
`tstzrange` literals lower back into those same typed boundary fields, and the
golden corpus separately pins half-open `numrange`, `daterange`, `tsrange`, and
`tstzrange` constructors in `INSERT` values. SQL `lower(period_column)` and
`upper(period_column)` lower to typed projections, predicates, and order keys
over the start/end boundary fields. SQL `period_column @> point` lowers to two
typed boundary predicate groups: `start <= point OR start IS NULL` and
`end > point OR end IS NULL`, matching the canonical half-open period contract
without requiring a physical range value in storage. SQL `period_column &&
range_value` lowers to the same boundary predicate model:
`start < range_upper OR start IS NULL` plus
`end > range_lower OR end IS NULL`; unbounded query endpoints omit the
corresponding bound predicate, and empty range literals fail closed. Unbounded
endpoints lower to `null` at the typed API boundary and to durable
finite/infinite sentinels inside temporal unique-owner keys.
Storage enforces period validity, no-overlap primary/unique keys, temporal FK
coverage, and `FOR PORTION OF` splitting/trimming through the ordinary
row-claim, 2PC, unique-owner, FK-proof, replay, and returning paths. Temporal
FKs support restrictive actions, bounded delete-side `ON DELETE SET NULL` and
`ON DELETE CASCADE` actions, and bounded temporal `ON UPDATE SET NULL` and
`ON UPDATE CASCADE` actions through the same remaining-coverage proof used by
storage. That is deliberately
broader than the PostgreSQL 19 launch shape, which keeps temporal foreign keys
to non-mutating referential actions; the extra Antfly actions are admitted only
because they are expressed as bounded owner-routed row mutations with a
post-action coverage proof. For temporal `ON UPDATE SET NULL`, updating a
parent key or interval first rewrites the temporal unique-owner rows, then scans
the reverse-reference rows for that parent identity: children that remain fully
covered by the replacement parent intervals keep their FK columns, and children
whose application period is no longer covered have only the FK columns nulled.
Temporal `ON UPDATE CASCADE` is intentionally admitted only as a bounded
owner-routed action: when parent identity values change, children whose period
is still covered by the rewritten parent interval receive the new FK values,
and any case that would require period splitting, trimming, or multi-parent
coverage selection must fail before storage mutation. The parity corpus
explicitly pins supported temporal `SET NULL` and `CASCADE` delete-action DDL
and supported temporal `SET NULL` and `CASCADE` update-action DDL. Typed
`FOR PORTION OF` requests carry the period name plus storage-typed `from` and
`to` JSON values: numeric periods use numbers, datetime periods use encoded
UTC nanoseconds, and SQL date/timestamp literals, including `DATE '...'`,
`TIMESTAMP '...'`, and `TIMESTAMPTZ '...'`, are converted before the request
reaches storage validation. Typed
REST/SDK row selectors support period-qualified primary and unique lookups by
resolving the owning row through the temporal unique-owner API and returning
the one owner interval that contains the requested point. `zig build
db-temporal-test`, `zig build api-rows-test`, and the top-level `make
zig-db-temporal-test` wrapper are the focused storage/API gates for those
semantics. SQL `MERGE` matched-row execution uses that same period-qualified
primary selector for temporal target tables, so matched updates/deletes route to
the exact owner interval instead of rejecting `WITHOUT OVERLAPS` primary keys or
collapsing multiple intervals onto a scalar key. Hosted remote period-qualified owner lookup uses typed internal
group read routes rather than exposing internal key scans to callers; the
resolved owner group performs the temporal owner-prefix scan locally and returns
only the matching owner row key. SQL `FOR PORTION OF` update/delete coverage
executes through the same adapter-to-typed-plan-to-storage path without
requiring SQL row-lock syntax on the statement: the lowerer emits a temporal
mutation-source request carrying the adapter-supplied typed row claim, DB
staging splits or trims the matched owner interval transactionally,
`RETURNING *` is projected from the affected portion, and committed queries
observe the surviving fragments. The temporal gate includes combined
`FOR PORTION OF` + temporal-FK cases that split a child row, revalidate period
coverage through the FK proof path, validate the temporal unique-owner rows,
repair missing temporal FK reverse-reference rows from live child fragments, and
keep parent delete/update checks routed through the temporal unique-owner
coverage scan after repair. Metadata
public-chaos coverage also creates hosted temporal tables, writes parent
intervals plus a same-table period-covering FK child interval, restarts hosted
row owners, and resolves both parent and child rows through period-qualified
selectors from non-owner API nodes. It also drives an owner-routed public
`rows/mutation-source` `FOR PORTION OF` update through a transaction session,
records staged row predicates, selected-row preimages, and hidden fragment
writes/deletes/transforms as the session participant set, commits through the
normal hosted transaction path, and verifies both the split timeline fragments
through public `rows/query` and the refreshed period-qualified selector through
public `rows/get`. Distributed temporal parent-delete planning nets same-txn
replacement writes against the deleted parent span before scheduling FK
delete checks, so a `FOR PORTION OF` split that preserves period coverage does
not look like an uncovered parent delete. Remaining production work is expanded
chaos coverage that combines `FOR PORTION OF` DML, temporal FK checks, range
movement, repair, and catalog promotion in one generated workload.
System-time / transaction-time history is intentionally separate from the
application-time interval model. `CREATE TABLE ... WITH SYSTEM VERSIONING`
lowers to durable relational schema metadata (`system_versioned: true`) so the
catalog records user intent and schema round trips preserve it. Storage now
keeps the current-row table behavior enabled for system-versioned relational
tables and appends hidden `relational-system-history:v1` metadata records
atomically with direct batch writes, deletes, identity rewrites, and committed
transaction/mutation-source changes. Each record is ordered by the same durable
batch or commit sequence, stores the transaction-time timestamp, operation
(`insert`, `update`, `delete`, or `identity_rewrite`), row key, optional new row
key, and before/after row JSON. Storage exposes typed history scans and a native
commit-sequence `querySystemVersionedRelationalRowsAsOfSequence` helper that
reconstructs the visible row set from hidden history records, then runs the
normal relational query executor over that reconstructed source. Storage also
resolves transaction-time nanoseconds to the latest durable commit sequence at
or before that timestamp and exposes
`querySystemVersionedRelationalRowsAsOfTimestampNs` on top of the same
reconstruction path. The SQL read adapter accepts `FOR SYSTEM_TIME AS OF
<commit-sequence>` plus string and typed `DATE` / `TIMESTAMP` / `TIMESTAMPTZ`
timestamp literals for plain table queries, resolves timestamp literals through
the native commit-sequence resolver, and routes both forms through the typed
table-read vtable. Bound/local storage and single-local provisioned/hosted
table-read paths can also compose commit-sequence and timestamp-resolved AS-OF
with typed CTE query plans by materializing the historical base-row snapshot
before evaluating CTEs. Same-table SQL set operations can compose branches at
one shared commit-sequence or timestamp-resolved AS-OF snapshot and fail closed
when branches request mixed snapshots; when the execution path must evaluate
the branches separately through the system-time row-query vtable, each branch
preserves its full lowered side plan, including owner-clipped ranges and
top-level materialized CTE metadata. Cross-table set operations use the same
branch-local system-time dispatch, so a CTE-backed branch materializes from its
resolved base catalog table's historical snapshot while a direct branch reads
its own catalog table at the same snapshot before coordinator reduction.
Hosted single-remote-owner table reads
route commit-sequence and timestamp AS-OF snapshots through the internal
`rows/source` owner endpoint and can materialize requester-side CTEs from that
historical snapshot. Hosted multi-owner table reads now fan out the same
system-time selector to each catalog owner range, request range-clipped
historical base rows through `rows/source`, revalidate each captured topology
epoch after fan-in, and evaluate the original query/CTE plan once at the
requester so ordering, predicates, limits, and CTEs see one combined snapshot.
AS-OF fanout resolves raw catalog owner ranges independently from logical
row-query `plan.ranges`; the requester applies those logical primary-key ranges
exactly once while evaluating the combined historical snapshot.
Owner-local AS-OF reconstruction honors the raw storage `doc_key_range` while
rebuilding historical rows, so internal `rows/source` range clipping does not
depend on primary-key-plan range filtering.
Remote owner `503` responses on that internal row-source route are preserved as
typed `LeaderUnavailable` failures, and topology drift after fan-in remains a
typed `TopologyChanged` failure instead of merging partial historical rows.
Cross-table SQL set operations can compose compatible two-branch catalog reads
at one shared commit-sequence or timestamp-resolved AS-OF snapshot by reading
each catalog target through the system-time table-read vtable before applying
the native set reducer.
Local storage exposes timestamp-based history pruning that records a retained
floor, compacts older per-row history down to a safe baseline event, preserves
AS-OF reconstruction at and after the floor, and fails closed for sequence or
timestamp snapshots before the retained floor. Distributed retention policy,
broader distributed set-operation AS-OF chaos/evidence, stronger global
visibility evidence for live range movement, and bitemporal planner visibility
remain separate query/runtime work on top of the durable history records. The
SQL/API parity corpus pins the DDL metadata token while storage and table-read
tests pin native hidden history capture, commit-sequence and timestamp-resolved
SQL AS-OF reads, and retained-floor history pruning.

For SQL DML, row locking remains a typed backend contract rather than required
surface syntax. The adapter receives the mutation-source row-claim owner,
transaction id, lease, mode, and wait policy from its execution context; plain
`UPDATE` / `DELETE` statements, including temporal `FOR PORTION OF` statements,
can lower into claimed mutation-source plans without spelling `FOR UPDATE`.
Optional SQL `FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]` clauses only override
the typed claim mode or wait policy when a statement intentionally needs those
queue-style semantics.
The internal hosted row-query/preflight, row-source, join worker, read worker,
document-artifact, transaction participant, and group maintenance HTTP client
helpers now preserve remote owner `503`
responses as typed `LeaderUnavailable` errors, so that boundary no longer
collapses a temporarily unavailable owner into an unexpected status.
Internal group mutation-source stage receivers now accept explicit schema plus
public request-body envelopes for owner-local HTTP dispatch, including joined
mutation-source source rows. A native mutation-source request encoder can
round-trip the already-lowered claimed scalar/static-patch subset through the
public request-body contract and fails closed for expression-heavy shapes instead
of stringifying runtime callback state. Hosted remote mutation-source
autocommit, non-autocommit participant staging, and joined mutation-source
source-row staging, including joined non-autocommit participant staging, now
wire conservative native encoders into the internal stage routes for the
supported claimed scalar/static-patch/source-assignment subset; joined
collect/input now has internal local-group route envelopes for target-candidate
collection and source-row input reads, and hosted non-autocommit joined
execution uses remote collect plus planned-candidate stage envelopes to apply
one global deterministic target plan across owner groups. Hosted joined
autocommit now reuses that global planned stage and commits the staged
participant metadata through distributed transaction resolution. Multi-owner
joined `SKIP LOCKED` uses the same global planned stage, attempts owner-local
claims in deterministic order, and stops once the global claimed limit is
filled.
Internal group read/write route dispatch now maps propagated owner-local
`NotLeader`, `LeaderUnavailable`, `ReadUnavailable`, and `WriteUnavailable`
failures to HTTP `503`, keeping routed owners retryable instead of surfacing
generic server failures.
Public rows REST, public table/catalog query dispatch, public retrieval query
execution, public document-artifact read/reprocess callbacks, public TableApi
batch callbacks, public TableApi read callbacks, public API client
lookup/query/batch/transaction helpers, and public SQL row-plan/write
boundaries now preserve typed `LeaderUnavailable`, `ReadUnavailable`, and
`WriteUnavailable` failures as HTTP/SQL `503` responses instead of collapsing
owner unavailability into generic server failures. Public TableApi read
callbacks distinguish primary owner absence (`read unavailable`) from HA
standby lag (`standby read unavailable`) before serializing the response.
Hosted remote document-artifact manifest reads only translate an explicit
remote `404` into an absent artifact; other remote HTTP failures propagate as
typed errors instead of masquerading as missing artifact state.
Hosted join-worker routes preserve unexpected remote HTTP failures as typed
errors instead of treating a failed remote worker call as a missing local join
result.
Hosted remote point lookups only translate explicit remote `404` responses into
lookup misses; unavailable or otherwise failing owner calls propagate as typed
errors instead of being skipped as absent rows.
Hosted remote temporal unique-owner lookup routes preserve remote `503` as
typed `UniqueOwnerTopologyUnavailable`, and hosted unique-owner source routing no
longer turns remote HTTP status failures into a false absent-owner result.
Hosted mutation-source coordinators now retry a remote owner
`TopologyChanged` response once by reloading the table catalog, recomputing the
range topology epoch, and resending owner-local collect/stage requests only
before any owner stage has succeeded; repeated topology churn or post-stage
topology changes still fail closed with `TopologyChanged` instead of replaying
already staged work. `SKIP LOCKED` owner stages that return `staged:0` and no
participant metadata remain retryable because they did not claim or write owner
work. Hosted mutation-source candidate fan-in also treats a candidate returned
by the wrong owner group as topology drift, releases the temporary candidate
payload, and retries or fails closed before staging. Owner-local planned-stage
routes re-check serialized candidate group IDs against the route owner, so
direct internal stage calls with stale or malformed owner metadata fail before
touching storage.
Local and routed mutation-source plan selection transfers candidate order-key
payloads with selected candidates, so later staging and plan cleanup keep the
same ordering evidence without dangling or double-owned buffers. Planned
mutation-source staging validates every serialized candidate has a committed row
version, each target key appears at most once, the candidate row still matches
the typed source predicates, and joined candidates still satisfy target/source
filters plus join keys before acquiring row-claim intents, so malformed planned
stages fail without leaving durable claim debris or ambiguous duplicate target
work. Mutation-source primary-key identity rewrites stage one committed-version
predicate for the old row and one absence predicate for the new row, avoiding
duplicate old-row OCC predicates while preserving the rewrite's two-key
atomicity. Explicit JSON-row insert-source batch builders reapply the typed
source filters, ordering, offset, and limit before target assignment and conflict
handling, so materialized or routed source streams cannot smuggle rows that no
longer satisfy the native source query.
Routed relational read, `INSERT ... SELECT` DML source, and simple routed MERGE
target/source preimage plans verify scanned rows against the current owner row
version immediately after each range scan, so a stale owner range stops the plan
before later ranges are scanned or downstream join/window/aggregate/source-insert
work can mix epochs, including rows that would otherwise be filtered out before
MERGE matching. MERGE preimage queries with distinct/order/limit/offset retain
the full-scan selection path so global selection semantics stay intact, but
they still verify each scanned range before global selection.

| Track | Current model shape | Long-term production plan |
| --- | --- | --- |
| Partial, expression, and partial-unique indexes | Ordinary secondary indexes carry lifecycle state and rebuild generations; expression secondary indexes such as `lower(field)`, `upper(field)`, simple `concat(field, 'separator', field, ...)`, and simple `concat_ws('separator', field, ...)` lower through generated-column metadata; unique expression constraints support case-fold keys; field-only partial predicates exist on secondary indexes and unique constraints, including signed numeric literal predicates; deterministic row-expression partial predicates are durable schema metadata for secondary indexes and unique constraints. Write-time secondary-index maintenance and partial unique-owner maintenance evaluate the supported row-local typed expression subset used by case-fold, hash, simple `concat`, simple `concat_ws`, `initcap`, `trim`/`ltrim`/`rtrim`, `replace`, `regexp_replace`, `regexp_count`, `regexp_instr`, `regexp_substr`, `translate`, `substring`, `overlay`, `split_part`, `left`/`right`, `lpad`/`rpad`, `repeat`, `reverse`, `starts_with`/`ends_with`, `like`/`ilike`, boolean `and`/`or`/`not`, `length`/`octet_length`/`bit_length`, `strpos`, `ascii`, `chr`, `coalesce`, `nullif`, `greatest`, `least`, numeric unary functions, numeric arithmetic including modulo and power, typed casts, deterministic `interval_ns`/`interval_months`, `date_trunc`, `date_bin`, `date_part`, searched `case`, storage-side JSON extraction/type/path/object-construction predicates, `to_jsonb`, array length/position/text-conversion/append/prepend/concat/remove/replace expressions, and scalar equality, distinctness, null, and comparison operators. Schema validation checks each catalog predicate operation against its text, numeric, boolean, JSON, array, datetime, or null operand domain, so predicates such as `split_part(status, '-', 1) = 'active'` are durable typed metadata rather than SQL text passed through to storage. `CREATE INDEX ... WHERE lower(field) = ...` and matching unique-expression `ON CONFLICT (...) WHERE lower(field) = ...` clauses lower to the shared expression AST, are validated at catalog-application time, survive schema JSON/runtime clone/drop/rename paths, and conflict-target inference compares the stored expression predicate directly or proves it from concrete field-equality predicates rather than dropping it. Point update/delete selectors can prove stored expression partial predicates from concrete selector fields for the stable expression subset used by unique-owner identity, such as `status = 'ACTIVE'` proving `lower(status) = 'active'`, `amount = 5` proving `amount > 0`, or `tenant_id = 't1' AND status = 'active'` proving `concat_ws(':', tenant_id, status) = 't1:active'`; omitted or contradictory predicate fields fail closed. Queries use only `ready` indexes and promoted unique constraints; query pushdown and unique-owner lookup can use expression-partial indexes/owners when the typed query either carries the exact deterministic expression predicate stored in catalog metadata or supplies concrete equality values for every referenced row field and the shared expression evaluator proves the stored predicate. Keyword equality/range pushdown carries explicit predicate collation into column-index candidate matching. Missing or contradictory proof falls back to base-row scans or non-partial owner paths. | Extend the same semantic implication surface to additional safe equivalence classes as the expression AST grows, and keep every proof fail-closed rather than treating unknown fields as nullable matches. Route expression-derived rebuilds through the same generation-aware catalog work, range repair, all-range readiness gate, and schema compare-and-swap promotion used by ordinary indexes. |
| Raw DML syntax: `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT`, `RETURNING` | Row-batch plans cover single-row and multi-row `INSERT ... VALUES (...), (...)`, `INSERT ... DEFAULT VALUES` through an empty typed insert row that is expanded by schema defaults/generated columns before primary-key encoding and checks, primary/unique selectors, point `UPDATE`/`DELETE` over primary keys and enforced unique column selectors, including partial unique selectors when the point predicate proves the catalog predicate, per-row `ON CONFLICT DO NOTHING/UPDATE`, named `ON CONFLICT ON CONSTRAINT` targets for the default `<table>_pkey` primary-key name plus enforced named unique, partial-unique, and expression-unique constraints, `excluded.column` over explicit insert values and schema-produced defaults/generated values, signed numeric literals, numeric conflict deltas such as `SET amount = amount + excluded.amount`, proposed-row arithmetic such as `SET amount = excluded.amount + 3`, source-qualified typed `increment_expr` deltas over existing/proposed rows such as `SET amount = amount + COALESCE(excluded.amount, 0)`, and source-qualified typed `patch_expr` assignments over existing/proposed rows, literals, casts over full row expressions, `LOWER`, `UPPER`, `INITCAP`, `TRIM` / `BTRIM` / `LTRIM` / `RTRIM`, `REPLACE`, `REGEXP_REPLACE`, `REGEXP_LIKE`, `REGEXP_COUNT`, `REGEXP_INSTR`, `REGEXP_SUBSTR`, `LENGTH` / `CHAR_LENGTH` / `CHARACTER_LENGTH`, `OCTET_LENGTH`, `BIT_LENGTH`, `ASCII`, `CHR`, `MD5`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `ABS`, `ROUND`, `TRUNC`, `FLOOR`, `CEIL`, `SQRT`, `SIGN`, `POWER`, searched `CASE`, boolean `AND`/`OR`/`NOT`, `array_length`, JSON `jsonb_extract_path[_text]`, `jsonb_typeof`, `jsonb_array_length`, server-owned time, typed `CONCAT`, `CONCAT_WS`, and text `||` concat, numeric arithmetic including SQL `%` and `MOD(...)` modulo, and fixed-duration interval arithmetic for numeric/datetime assignments. Default-values conflict clauses resolve their primary/unique target from the same planned/defaulted row used by the write, named partial-unique conflict targets inject the catalog predicate instead of requiring SQL predicate inference, and conflict `excluded.field` references bind to the proposed row rather than adapter-synthesized literals. Multi-row `DO UPDATE` fails closed when proposed rows duplicate the same primary, unique-column, or supported unique-expression conflict target, while `DO NOTHING` may skip duplicate proposed targets because it does not mutate an existing row. Conflict-action `WHERE` predicates and boolean conflict assignments lower to native expression conditions or `patch_expr` assignments over the same existing/proposed row sources; false predicates skip the update and emit no `RETURNING` row. Arithmetic updates, boolean expression updates, JSONB/array transforms, defaults, `NOW()`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`, table-owned updated-at policies, field `RETURNING`, and committed-row `returning_expressions` over the shared row-expression AST also lower to typed row-batch plans. Transaction-aware `mutation_source` plans update/delete rows selected from a lockable typed row query, require a `for_update` or `for_no_key_update` row claim, stage through the claiming transaction, preserve the same typed source-query surface as reads including scalar OR/NOT, computed `expression_where`, `expression_any`, `expression_not`, computed-array containment, row-claim metadata, and an optional start-inclusive/end-exclusive `doc_key_range`, reject CTE sources at the typed boundary, and use direct native `doc_key_range` values to constrain local planning while internal routed planning injects catalog-owned owner ranges separately, add committed-version predicates from the selected preimages, materialize `patch_expr`/`increment_expr` per selected row before staging, apply table-owned update policies and generated columns to that final image, and project field plus expression `RETURNING` output from the same planned image. The DB execution path has an explicit plan/stage boundary: planning produces ordered preimage candidates with committed versions, and staging claims those candidates and writes the update/delete intents. Provisioned and hosted execution collect range-clipped candidates from catalog-owned ranges, apply global order/offset/limit once, and stage selected candidates back to their owner groups through owner-local calls; hosted remote mutation-source owners use internal collect plus planned-candidate stage routes for the supported native scalar/static-patch subset in both autocommit and non-autocommit participant-staging modes, while owner-local collect/input/stage calls carry the catalog topology epoch and stale owner metadata fails closed with `TopologyChanged` before planning or staging. Staging intentionally only prepares transaction intents while transaction resolution remains coordinator-owned. The public `/tables/{tableName}/rows/mutation-source` endpoint, OpenAPI contract, and SDK models expose that typed mutation-source plan directly; PostgreSQL-adapter lowering covers bounded `UPDATE ... WHERE ... [ORDER BY] [LIMIT/OFFSET/FETCH] [FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]] RETURNING ...` and `DELETE ... WHERE ...` sources. Joined mutation-source requests expose claimed target-side joins with `source_table`, `source_assignments` side-field copies, target-local patches, side predicates, join predicates, and returning projections; request parsing and SQL lowering can validate target-side fields against a target schema and source-side fields against a separate source schema, so the typed boundary no longer requires both sides to share one runtime schema. Bound single-DB write sources execute same-table joined mutation sources directly, while provisioned and hosted catalog-backed write sources resolve `source_table`, read source rows from source-table owner ranges with the source schema, and stage only target-row intents back to target owner ranges. Local DB planning materializes deterministic target/source matches, rejects ambiguous multi-source matches, claims only target rows, stages update/delete intents with committed-version predicates, and projects `RETURNING` from the final target image or deleted target row. The same public endpoint accepts the joined typed request; bound write sources execute it directly, and provisioned/hosted write sources collect target candidates and source rows from catalog owner ranges, build matches with global source visibility, apply global join ordering/pagination once, then stage selected target rows back through local or hosted remote owner staging for the supported source-row/static-patch/source-assignment subset; internal joined collect/input local-group routes expose target-candidate and source-row envelopes, hosted joined execution uses remote collect plus planned-candidate stage envelopes to apply one global deterministic target plan across owner groups, hosted joined autocommit commits staged participant metadata through distributed transaction resolution, and multi-owner joined SKIP LOCKED attempts owner-local claims in global order until the claimed limit is filled. Owner-local target and cross-table source collection validate the side-local joined request shape against the relevant schema, reject embedded physical ranges, and check side-local join plus assignment fields before scanning. The PostgreSQL adapter emits `source_table` and `source_assignments` when it lowers constrained `UPDATE ... FROM`, `DELETE ... USING`, and target-field-projected `UPDATE`/`DELETE ... WHERE target_field IN (SELECT source_field FROM source [WHERE ...])` shapes into that same typed contract with golden corpus fingerprints; projected source fields may be primary or non-primary scalar columns, and local execution rejects ambiguous multi-source matches instead of applying many-to-one writes. SQL `FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]` is optional for these joined DML forms because the REST/SDK mutation-source call supplies the required row-claim owner, transaction id, mode, and wait policy. Simple correlated subquery selectors lower target/source field equality predicates into additional joined mutation-source keys while qualified source-local filters stay on the source query, so `WHERE target_id IN (SELECT source_id FROM source WHERE source.kind = 'archived' AND source.status = target.status)` stays a deterministic claimed target-side join. Single-output computed target-dependent subquery predicates lower to joined mutation-source `match_expression_*` residuals after the target/source key dependency is extracted, so `WHERE target_id IN (SELECT source_id FROM source WHERE lower(source.status) = lower(target.status))` stays typed and deterministic. Row-value multi-column semi-joins such as `WHERE (target_id, status) IN (SELECT source_id, status FROM source)` lower to multi-key joined mutation-source dependencies. Scalar-target multi-output subqueries still fail closed as invalid selector shapes. | Route any future row-value selector variants through explicit typed dependency nodes before prepare, then harden mutation-source claims with owner/lease payloads and topology-change retry loops around the existing epoch fail-closed boundary. Catalog-backed cross-table joined mutation-source execution uses separate target/source range ownership and source-schema-aware staging; the remaining hardening is range-movement chaos coverage around planning/staging. Keep conflict actions, generated columns, server-owned policies, update transforms, and mutation result projection on the shared expression tree so local and routed execution use the same final-image semantics. |
| Query predicates, projection expressions, ordering, and pagination | Typed row-query plans cover scalar predicates, native null-safe distinct predicates, boolean `IS TRUE` / `IS FALSE`, null-inclusive `IS NOT TRUE` / `IS NOT FALSE`, boolean-null `IS UNKNOWN` / `IS NOT UNKNOWN`, PostgreSQL postfix null tests `ISNULL` / `NOTNULL`, JSONB/array predicates, text-pattern predicates, scalar OR/NOT groups, mixed access OR/NOT groups for scalar, membership, text-pattern, declared-array, and declared-JSON atoms, generated-column pushdown, null-test ordering, executable expression predicates, executable expression OR/NOT groups, expression order keys, `ORDER BY`, `LIMIT`, `OFFSET`, row claims, and API-native expression projections over fields, literals, statement-bound `now`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`, `coalesce`, `lower`, `upper`, `initcap`, `trim`, `ltrim`, `rtrim`, `replace`, `regexp_replace`, `regexp_match`, `regexp_count`, `regexp_instr`, `regexp_substr`, `translate`, `substring`, `overlay`, `split_part`, `strpos`, `left`, `right`, `lpad`, `rpad`, `repeat`, `reverse`, `ascii`, `chr`, `md5`, `starts_with`, `ends_with`, `like`, `ilike`, boolean `and`/`or`/`not`, `concat`, `concat_ws`, `length`, `octet_length`, `bit_length`, `nullif`, `greatest`, `least`, `abs`, `round`, `trunc`, `floor`, `ceil`, `sqrt`, `sign`, `power`, `add`, `sub`, `mul`, `div`, `mod`, `interval_ns`, `interval_months`, `date_trunc`, `date_bin`, `date_part`, searched `case`, typed `cast` to text, numeric, boolean, or datetime, JSON extraction, JSON path existence, JSON type inspection, JSON array length, JSON object construction, `to_jsonb`, array length / cardinality, `array_position`, `array_positions`, `array_append`, `array_prepend`, `array_cat`, `array_remove`, `array_replace`, `array_to_string`, `string_to_array`, and unary numeric negation. SQL projections, predicates, and order keys for `LOWER(text_expr)`, `UPPER(text_expr)`, `INITCAP(text_expr)`, `TRIM(text_expr[, chars])`, `BTRIM(text_expr[, chars])`, `LTRIM(text_expr[, chars])`, `RTRIM(text_expr[, chars])`, `REPLACE(text_expr, from_text, to_text)`, `REGEXP_REPLACE(text_expr, pattern_expr, replacement_expr[, flags_expr])`, PostgreSQL regex operators `~`, `~*`, `!~`, `!~*`, boolean `REGEXP_LIKE(text_expr, pattern_expr[, case_insensitive_bool])` as the native `regexp_match` expression node, numeric `REGEXP_COUNT(text_expr, pattern_expr)` as the native `regexp_count` expression node, numeric `REGEXP_INSTR(text_expr, pattern_expr)` as the native `regexp_instr` expression node, and `REGEXP_SUBSTR(text_expr, pattern_expr)` as the native nullable text `regexp_substr` expression node, `TRANSLATE(text_expr, from_text, to_text)`, `SUBSTRING(text_expr, start[, count])`, `SUBSTRING(text_expr FROM start [FOR count])`, `SUBSTR(text_expr, start[, count])`, `OVERLAY(text_expr PLACING replacement_expr FROM start [FOR count])`, `SPLIT_PART(text_expr, delimiter, field_index)`, `STRPOS(text_expr, needle)`, `POSITION(needle IN text_expr)`, `LEFT(text_expr, count)`, `RIGHT(text_expr, count)`, `LPAD(text_expr, length[, fill])`, `RPAD(text_expr, length[, fill])`, `REPEAT(text_expr, count)`, `REVERSE(text_expr)`, `STARTS_WITH(text_expr, prefix_expr)`, `ENDS_WITH(text_expr, suffix_expr)`, `ASCII(text_expr)`, `CHR(numeric_expr)`, `MD5(text_expr)`, `CONCAT(...)`, `CONCAT_WS(separator_expr, text_expr...)`, `LIKE`/`ILIKE` over computed text expressions, expression-level `AND`/`OR`/`NOT` over typed boolean operands, computed boolean `IS TRUE` / `IS FALSE`, null-inclusive computed boolean `IS NOT TRUE` / `IS NOT FALSE`, computed boolean `IS UNKNOWN` / `IS NOT UNKNOWN`, computed postfix `ISNULL` / `NOTNULL`, `LENGTH(text_expr)`, `CHAR_LENGTH(text_expr)`, `CHARACTER_LENGTH(text_expr)`, `OCTET_LENGTH(text_expr)`, `BIT_LENGTH(text_expr)`, `DATE_TRUNC(unit_expr, timestamp_expr)`, `DATE_BIN(stride_expr, timestamp_expr, origin_expr)`, `DATE_PART(unit_expr, timestamp_expr)`, `EXTRACT(unit FROM timestamp_expr)`, including `decade`, `century`, and `millennium` calendar units, `COALESCE(...)`, `NULLIF(...)`, `GREATEST`/`LEAST`, numeric functions including `ABS`, `ROUND`, `TRUNC`, `FLOOR`, `CEIL`, `SQRT`, `SIGN`, and `POWER`, numeric arithmetic including SQL `%` and `MOD(...)` modulo, fixed-duration interval arithmetic, month/year calendar interval arithmetic, `CAST` over full row expressions including `timestamp`/`timestamptz` aliases for encoded datetime values, JSON extraction including `jsonb_extract_path[_text]` and `IS NULL` / `IS NOT NULL` / `ISNULL` / `NOTNULL` tests, JSON existence `json_col ? key_expr` as either an indexable declared predicate in bare `WHERE` atoms or a boolean expression node in projections/filters/checks, `jsonb_typeof`, `jsonb_array_length`, `jsonb_build_object`, `to_jsonb`, declared JSON/array/text access predicates inside top-level `OR` and `NOT (...)` groups, `array_length`, `cardinality`, `array_position`, `array_positions`, `array_append`, `array_prepend`, `array_cat`, `array_remove`, `array_replace`, `array_to_string`, `string_to_array`, searched `CASE`, null tests, `BETWEEN`/`NOT BETWEEN`, computed `NOT (...)`, text patterns, and `NULLS FIRST` / `NULLS LAST` lower into the same typed plan surface. Simple generated-column matches such as `lower(field)`, `upper(field)`, `md5(field)`, and supported `concat(field, separator, field...)` and `concat_ws(separator, field...)` still push down to stored generated columns when those columns exist; otherwise expression evaluation stays typed and residual. |
| JSONB and array operators | Declared `json` and `array` columns lower to typed path predicates, structured/text extraction with `->`/`->>` and path-array extraction with `#>`/`#>>`, `jsonb_extract_path[_text]`, containment/existence, `jsonb_typeof`, `jsonb_array_length`, `jsonb_set`, JSON construction/concat, `to_jsonb` over row expressions, literals, and bound parameters, native point row-batch `json_set` values from typed expressions over existing/proposed mutation rows, SQL point updates such as `jsonb_set(json_col, path, to_jsonb(lower(row_field)), true)`, conflict updates such as `jsonb_set(json_col, path, to_jsonb(excluded.field), true)`, claimed mutation-source JSON-set expressions evaluated owner-side against each selected preimage, joined mutation-source JSON-set expressions evaluated owner-side against the target preimage plus explicit source row, array membership/equality/containment/cardinality/position/text conversion, and prepend/append/concat/remove/replace/add-to-set transforms. | Treat SQL operators and functions such as `->`, `->>`, `#>`, `#>>`, `@>`, `jsonb_extract_path[_text]`, `jsonb_typeof`, `jsonb_array_length`, `jsonb_set`, `to_jsonb`, `ANY`, `array_length`, `cardinality`, `array_position`, `array_positions`, `array_append`, `array_prepend`, `array_cat`, `array_remove`, `array_replace`, `array_to_string`, `string_to_array`, and array containment as adapter sugar over typed JSON/array nodes. Keep expression-valued JSON-set operations in typed requests until owner-local staging materializes them into ordinary transform ops for the selected row, so source-driven updates never flatten dynamic JSON values at parse time. Indexed JSON-column schemas and dynamic-template changes schedule explicit embedded document-index rebuild work rather than bypassing the relational row model. |
| `FOR UPDATE`, `FOR NO KEY UPDATE`, `NOWAIT`, `SKIP LOCKED`, and queues | Row-claim metadata exists for lockable base-row streams, carries explicit `mode` and `wait_policy` metadata, persists pending claim intents with durable owner/lease payloads, and local plus catalog-routed provisioned/hosted mutation sources stage claimed update/delete intents with OCC predicates, per-row expression transforms, server-owned update policies, generated-column repair, and final-image `RETURNING` projections in the claiming transaction. `mode` supports `for_update`, `for_no_key_update`, `for_share`, and `for_key_share` for lockable base-row read streams. `for_update` and `for_no_key_update` are the exclusive write-claim modes accepted by mutation-source plans; `for_share` and `for_key_share` are read-lock strengths that persist their own mode metadata while currently using the same conservative durable row-claim intent key for conflict safety. `wait_policy` supports `wait`, `nowait`, and `skip_locked`; the PostgreSQL adapter accepts `FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, and `FOR KEY SHARE` with same-target `OF <target>` plus `NOWAIT` or `SKIP LOCKED` on read queries. Mutation-source SQL fails closed on non-exclusive lock modes, and non-target `OF` lock lists fail closed as invalid semantic plans instead of being widened into target row ownership. | Add shared/key-shared read proofs only when each maps to durable owner/lease semantics, conflict compatibility, replay, repair, and routed range ownership. Add range-movement chaos coverage around lease reclaim and retry. Keep claims illegal over joins, aggregates, windows, and materialized CTEs until those stages expose a lockable base-row source. Gate queue workloads with chaos tests that move ranges while writers and claimers run. |
| Joins, CTEs, aggregates, and windows | Local equality joins, non-recursive CTE lowering with default row/byte caps, optional explicit caps, and output-field validation for downstream row-query, aggregate, join, lateral, and window `source_cte` consumers, scalar and boolean aggregate lowering including `bool_or` and `bool_and` over boolean fields or typed boolean expressions, scalar and boolean `FILTER`, executable typed aggregate filter expressions such as `FILTER (WHERE lower(status) = 'active')`, `FILTER (WHERE lower(status) LIKE 'op%')`, `FILTER (WHERE lower(status) IN ('active','pending'))`, `FILTER (WHERE coalesce(status, 'missing') = 'active')`, `FILTER (WHERE greatest(amount, 0) > 10)`, `FILTER (WHERE array_length(tags, 1) > 0)`, `FILTER (WHERE array_length(tags, 1) = ANY($1))`, `FILTER (WHERE metadata->>'source' = 'api')`, declared JSON aggregate access filters such as `FILTER (WHERE metadata @> '{"source":"api"}'::jsonb)`, JSON path equality, and `FILTER (WHERE metadata ? 'flags')`, declared array/membership/text aggregate access filters such as `FILTER (WHERE 'hot' = ANY(tags))`, `FILTER (WHERE tags @> ARRAY['hot'])`, `FILTER (WHERE tags = ARRAY['hot','new'])`, `FILTER (WHERE status IN ('open','pending'))`, and `FILTER (WHERE status ILIKE 'op%')`, and computed array-containment filters such as `FILTER (WHERE string_to_array(scope, ' ') @> ARRAY['write'])`, standalone boolean constants in aggregate `FILTER` and `HAVING`, plus boolean `IS TRUE` / `IS FALSE`, null-inclusive `IS NOT TRUE` / `IS NOT FALSE`, and boolean-null `IS UNKNOWN` / `IS NOT UNKNOWN` tests in aggregate `FILTER`, lowered through the same expression predicate contract, typed aggregate input expressions for scalar expression keys such as `COUNT(DISTINCT lower(status))`, `COUNT(DISTINCT coalesce(status, 'missing'))`, `COUNT(DISTINCT regexp_substr(status, '[A-Z]+'))`, JSON extraction keys such as `COUNT(DISTINCT metadata->>'source')`, bounded JSON extraction collection such as `ARRAY_AGG(metadata->'flags')`, bounded `ARRAY_AGG(DISTINCT status ORDER BY amount DESC) FILTER (...)`, bounded `STRING_AGG(DISTINCT status, '|' ORDER BY created_at DESC)`, exact bounded scalar and array-fraction `percentile_cont(...)` and `percentile_disc(...)` `WITHIN GROUP (ORDER BY numeric_field_or_expression [ASC|DESC] [NULLS FIRST|LAST])`, boolean folds such as `BOOL_OR(enabled)` and `BOOL_AND(starts_with(status, 'op'))`, and numeric expression inputs such as `SUM(amount - discount)`, `SUM(amount % discount)`, `SUM(MOD(amount + discount, discount))`, `SUM(abs(amount - floor))`, `SUM(coalesce(amount, 0))`, `SUM(greatest(amount, 0))`, `SUM(array_length(tags, 1))`, `SUM(octet_length(status))`, `SUM(bit_length(status))`, `SUM(regexp_count(status, '[0-9]+'))`, and `SUM(regexp_instr(status, '[A-Z]+'))`, scalar `DISTINCT` with DB-backed spill beyond declared in-memory caps, expression group keys through native `group_expressions`, `SELECT DISTINCT lower(status)`, `GROUP BY lower(status)`, and `GROUP BY` selected expression aliases such as `GROUP BY status_key` when the alias does not shadow a real column, bounded ordered `array_agg` and `string_agg`, aggregate-output `HAVING` predicates, typed `having_expressions`, `having_any`, and `having_not` over emitted group/metric fields, aggregate ordering, aggregate pagination, fail-closed output-reference validation for aggregate, join, lateral, and window result ordering/filtering, native aggregate API parsing, native equality join API parsing, native ordered CTE plan parsing, local `row_number()`, `rank()`, `dense_rank()`, `percent_rank()`, `cume_dist()`, `ntile()`, `lag()`, `lead()`, `first_value()`, `last_value()`, `nth_value()`, `count()`, `sum()`, `avg()`, `min()`, `max()`, `bool_or()`, and `bool_and()` windows with native window API parsing, typed frame metadata, conservative aggregate/window nullability metadata, source-backed result collation metadata, outer-aware join/lateral nullability metadata, independent per-window partition/order evaluation contexts, local typed lookup/hash strategy selection, and physical sorted merge join execution when both side streams prove leading ascending join-key order, and bounded local, CTE-backed, declared multi-range, and catalog-routed cross-table `LEFT JOIN` / `LEFT JOIN LATERAL` stages with native join/lateral API parsing and lowered SQL execution over the same routed scan helpers have typed plan homes. Join, lateral, and joined mutation-source side filters use the same typed scalar, JSON, array, membership, text-pattern, computed pattern, computed expression predicate, expression-group, and computed-array atoms as row queries, including negated text-pattern predicates, computed `LIKE`/`ILIKE`, exact boolean `IS TRUE` / `IS FALSE`, null-inclusive boolean `IS NOT TRUE` / `IS NOT FALSE`, and boolean-null `IS UNKNOWN` / `IS NOT UNKNOWN` predicates. Storage and coordinator JSON result-stream consumers share the same text collation comparison helper. | Extend strategy choice to routed/hosted owners with cardinality and index hints, add collation-aware index semantics for result-stream consumers, spill/backpressure policy for materializations that intentionally exceed fail-closed caps, aggregate expression pushdown, additional ordered-set aggregate families and percentile window stages, hosted remote lateral strategy hardening, and routed/spill-safe window execution. |
| SQL/API evidence | Supported SQL fails closed into typed DDL, row-query, mutation, aggregate, join, CTE, and expression plans; unsupported syntax does not pass through storage. A dedicated `zig build sql-api-parity-test` gate runs the source corpus and fixture-backed golden plans in `pkg/antfly/src/sql/fixtures/sql_api_parity_corpus.json`, so harvested SQL and migration-equivalent schema/data-change intent can be expanded without changing harness code. The fixture JSON is generated from `pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json` with `zig build sql-api-parity-fixture-promote`; `zig build sql-api-parity-fixture-check` is part of `make generated-check` and fails if the checked-in fixture is stale. The generated JSON fixture should not be hand-edited. Required coverage buckets live in `pkg/antfly/src/sql/fixtures/sql_api_required_coverage.json`, stay sorted for reviewable policy diffs, and are enforced by both source and generated fixture gates. The fixture-backed gate covers representative schema creation including named inline primary-key, unique, check, and foreign-key column constraints, table schema replacement with explicit rewrite intent, secondary, JSON/array GIN, and expression index rebuild work, idempotent index no-ops, signed partial predicates, unique-expression index validation, secondary-index drop, additive and validated unique/check/FK constraints, dropped constraints, default set/drop metadata changes, defaulted required-column rewrite work, not-null validation, drop-not-null metadata changes, rename/type rewrite work, update-policy metadata creation/removal, column rewrite work, table-drop, table-drop cascade metadata cleanup, table-emptying `TRUNCATE` plans, typed catalog metadata comment DDL, typed transaction-control table-lock/constraint-mode/advisory-lock plans, typed extension/routine/identity-allocator/partition/row-security catalog DDL, adapter-only DDL/session/reset-show/default-schema no-ops, row queries with JSON extraction, structured JSON/array access `OR` and `NOT` groups, JSON containment/path-existence access nodes, declared array containment/equality nodes, scalar `IN` / `ANY` membership nodes with golden fingerprint suffixes across row-query, mutation-source, aggregate, join, lateral, and window sources, computed `ANY`/`ALL`/`IN` membership, expression predicates, standalone boolean constants and boolean `IS TRUE` / `IS FALSE` and null-inclusive `IS NOT TRUE` / `IS NOT FALSE` predicates in row-query, aggregate `FILTER` including null-inclusive and boolean-null tests, aggregate JSON-extraction `FILTER`, aggregate JSON containment/path-existence `FILTER`, aggregate structured array/membership/text `FILTER`, aggregate computed-pattern and computed-array `FILTER`, and aggregate `HAVING` predicates, computed array predicates, unary numeric expressions, modulo expression projections, rich text-expression projection/predicate/order plans, order keys, aggregate inputs, and window value expressions, expression projections, null ordering, alias/ordinal ordering, `DISTINCT ON`, `LIMIT ALL`, and `FETCH` pagination for reads plus claimed mutation-source DML, grouped aggregates with computed source predicates, computed/boolean `HAVING`, boolean `FILTER`, distinct grouped projections, expression group keys, expression-alias `GROUP BY`, CTE definitions with structured source-access predicates, equality joins and bounded lateral stages with structured JSON/array/text side-access predicates, computed pattern, computed expression, and computed-array side predicates, mixed-side join and lateral residual predicates, negated text patterns, exact, null-inclusive, and boolean-null side predicates, catalog-backed source-schema direct and read-classifier join/lateral fingerprints, rich window frames and functions including computed-pattern window filters and boolean `BOOL_OR`/`BOOL_AND` window aggregate filters, multi-row inserts, insert-source field and computed expression assignments with expression `RETURNING`, schema-derived default-values inserts and default-values conflict updates, schema-derived primary/unique/partial/expression conflict handling, unvalidated-check write behavior, guarded/default/server-time conflict updates, point update/delete returning projections, deterministic committed-image `RETURNING` row JSON, JSONB and array mutation transforms, claimed mutation sources including structured JSON/array/text source-access predicates, computed predicate groups, and `RETURNING *`, joined update/delete mutation-source plans including target-side ordering/pagination, target/source-side scalar membership, target/source-side computed pattern and computed expression predicates, expression groups, computed-array predicates, exact, null-inclusive, and boolean-null side predicates, target-owned field and expression `RETURNING` projections, and structured JSON/array/text side-access predicates including negated text patterns, plus mixed-side match-expression and source-field, source-computed, and mixed target/source computed joined update/delete `RETURNING` fingerprints, unorderable JSON/array expression order-key rejections, and other unsupported classifications. | Keep growing the source fixture for SQL/data examples and add explicit fixture fields plus parser validation coverage when a regression needs more metadata; update the required-coverage fixture when coverage expectations intentionally change, promote the generated fixture instead of editing it directly, bind each entry against Antfly catalog snapshots, record golden typed plans, deterministic mutation result rows, or intentional unsupported classifications, run representative row/identity/constraint/queue/JSON/usage flows, and add chaos/sim workloads that combine live writes, FK actions, unique-owner repair, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and native schema/rewrite/rebuild jobs. |

The bulk I/O evidence explicitly gates both CSV and PostgreSQL text `COPY`
import/export as typed bulk plans. Text-format COPY lowers to the same native
rows-batch or rows-query execution contract with a `postgres_text` codec instead
of carrying SQL COPY syntax through storage.

SQL `UNION ALL`, `UNION`, `INTERSECT`, and `EXCEPT` result streams use the
same row/byte materialization admission model as typed CTEs before returning
rows or applying result-tail ordering and pagination. Cross-source
set-operation streams and bounded recursive CTE fixpoint streams now use the
same native admission policy as DB-owned CTE materialization: spill-classified
set-operation streams cross the shared SQL row-spill boundary, recursive CTE
streams are admitted by the stream executor, and hard row/byte cap violations
still reject. The SQL adapter's stable unsupported classifications carry native
execution-requirement metadata: `recursive_cte_stream_plan` and
`set_operation_plan` require a stream-materialization runtime with explicit
materialization, spill, and backpressure support before the parser can admit
broader recursive or general set-operation execution.

`PRIMARY KEY ... [NOT] DEFERRABLE [INITIALLY IMMEDIATE|DEFERRED]`,
`CREATE UNIQUE INDEX ... NULLS DISTINCT`, and `UNIQUE NULLS DISTINCT (...)`
constraints are accepted as explicit syntax for ordinary immediate identity and
unique-owner semantics. Primary-key and unique-constraint deferrability and
initial timing are durable catalog metadata, round-trip through schema JSON and
binary schema snapshots, and are pinned in DDL fingerprints so schema changes
cannot silently drop them. Ordinary non-temporal unique constraints and
`CREATE UNIQUE INDEX` plans also support `NULLS NOT DISTINCT`: the catalog stores
`nulls_not_distinct`, schema JSON round-trips it, unique-owner keys encode a
stable null component for omitted/null fields, validation/repair rebuilds those
owner rows, and write-time enforcement treats a second row with the same null
unique tuple as a uniqueness violation. Default unique constraints keep
PostgreSQL's ordinary `NULLS DISTINCT` behavior by skipping rows whose unique
tuple contains a null component. Combining `NULLS NOT DISTINCT` with
application-time `WITHOUT OVERLAPS` unique constraints still fails closed until
temporal null-equal span semantics are represented explicitly. `DEFERRABLE`
primary and unique constraints are not normalized away: immediate checks still
use ordinary owner-key enforcement, while transaction-level deferred checks are
modeled as a separate execution concern that must consult the stored
deferrability/timing metadata before commit-time validation.

The parity corpus includes same-table `UNION`, `INTERSECT`, and `EXCEPT`
predicate-composition read plans, including identical expression-output
branches, expression-only predicate branches, and mixed scalar/expression
predicate branches. The query-family corpus pins the same expression-output,
expression-only predicate, and mixed scalar/expression predicate set-operation
shapes so public query and read plans cannot drift. The corpus also includes
proven-disjoint scalar, scalar OR, scalar `IN`, and expression
`UNION ALL` branches, including scalar `IN` plus expression branches whose
safe membership predicates or typed expression conditions are proven disjoint
from the other branch, over canonical string, boolean, null equality, exact complementary
inequality/distinctness, exact numeric complementary predicates, adjacent
numeric field or expression range partitions such as `< bound` versus
`>= bound`, and null-test predicates whose duplicate semantics match the same
row-query `OR` plan. Top-level scalar `IN` branches stay native as
typed `in_predicates` before composition and lower to composed
`access_or_predicates` for `UNION`/proven-disjoint `UNION ALL` branch OR,
ordinary `in_predicates` for `INTERSECT` conjunction, typed
`access_not_predicates` for `EXCEPT` negation when the right branch has no
expression predicates, and expanded typed `expression_not_predicates` for
non-negated `IN` plus expression-predicate or expression-OR `EXCEPT` branches.
Plain expression-OR `UNION` branches and `UNION` branches that combine
non-negated scalar `IN` with expression predicates or expression-OR groups lower
to typed `expression_or_predicates` branch conditions, so membership and
computed filters stay in the same native expression plan. Base scalar and
expression predicates that appear beside an expression-OR group are copied into
each typed branch, preserving `base AND (expr_a OR expr_b)` as an
OR-of-conjunctions plan. `INTERSECT`
between scalar `IN` and expression-predicate branches remains a direct native
conjunction when no OR distribution is required. Scalar OR
branches also compose natively:
`INTERSECT` distributes compatible scalar OR branches into branch conjunctions,
and `EXCEPT` over a scalar OR right branch lowers to multiple typed
`not_predicates`; `EXCEPT` over an expression OR right branch lowers to multiple
typed `expression_not_predicates`. `INTERSECT` over expression OR branches uses
the same bounded branch-distribution model, promoting scalar branch predicates
and expanded scalar `IN` membership values to equivalent typed expression
conditions and producing native `expression_or_predicates` branch conjunctions.
`UNION ALL` accepts scalar `IN`
only when every branch pair is proven disjoint from same-field scalar equality,
`IS NOT DISTINCT FROM`, `IS NULL`, or another non-negated scalar `IN` array over
safe JSON scalar literals; when such a branch also has expression predicates,
the composed `UNION ALL` plan uses bounded `expression_or_predicates` and can
also prove non-overlap from contradictory typed expression conditions over the
expanded branches.
Outer set-result `ORDER BY`, `LIMIT`, `OFFSET`, and `FETCH` clauses apply after
predicate composition and lower onto the composed typed row-query plan. `FETCH
FIRST ROW ONLY`, `LIMIT NULL`, and `OFFSET NULL` use the same normalization as
ordinary row queries, so no SQL pagination state reaches storage. Branch-local
ordering/pagination remains unsupported because the native rewrite is a single
result stream, not a pair of materialized SQL branches. Overlapping
`UNION ALL` and mismatched expression outputs still have unsupported coverage.
Catalog-backed cross-table set operations execute through the explicit native
set-operation read plan when branch output shapes are compatible, and compatible
two-branch AS-OF forms route both branches through the same commit-sequence or
timestamp-resolved system-time table-read vtable before the native reducer runs.
That split keeps the SQL adapter aligned with the native typed row-query model
while the larger stream-composition plan remains explicit future work.

Scalar and computed-expression `BETWEEN` predicates keep PostgreSQL's explicit
mode keywords while staying typed. `BETWEEN ASYMMETRIC` lowers like default
`BETWEEN` into `x >= a AND x <= b`, and `NOT BETWEEN ASYMMETRIC` lowers into
bounded scalar or expression disjunctions. `BETWEEN SYMMETRIC` lowers to
native row-query OR groups as hand-written bounded disjunctions: the positive
form emits `(x >= a AND x <= b) OR (x >= b AND x <= a)`, and `NOT BETWEEN
SYMMETRIC` emits `(x < a AND x < b) OR (x > a AND x > b)`, so PostgreSQL's
order-insensitive syntax remains a typed predicate plan rather than a SQL
runtime special case.

Correlated `EXISTS` mutation selectors are adapter sugar over the same joined
mutation-source dependency nodes as `IN (SELECT ...)` semijoins. The supported
shape must expose at least one target/source key dependency, such as
`EXISTS (SELECT 1 FROM source WHERE source.source_id = target.target_id)`;
source-local filters stay on the source query and mixed target/source computed
conditions become `match_expression_*` residuals. Uncorrelated `EXISTS`
mutation selectors fail closed instead of widening into a table-wide write.

The in-code typed-plan corpus also pins same-table and catalog-backed
CTE-backed `INSERT INTO ... SELECT ...` source plans, so adapter lowering must
resolve CTE output fields through the CTE schema while preserving the durable
base source table for catalog-owned cross-table execution.

Fixture-backed SQL/API evidence explicitly includes statement-bound time
projection, JSON type inspection projection/predicate/order-key plans, JSON array length projection/predicate/order-key plans,
fixed-duration interval expression projection/predicate/order-key
lowering into `interval_ns`, month/year calendar interval projection lowering
into `interval_months`, mixed calendar plus fixed-duration interval literals
as nested month-then-fixed-duration typed arithmetic, and fixed-duration
interval conflict updates that materialize deterministic committed-image
`RETURNING` rows. Conflict-action
`WHERE` guards are pinned for both applied updates and false-predicate skips, so
skipped conflict actions emit no update intent and no `RETURNING` row. Conflict
updates also have fixture-backed evidence for expression binding across
proposed `excluded` values and the committed row, including `coalesce` fallback
to an existing row field, numeric function updates such as `greatest` over
existing/proposed values, text-derived updates such as
`length(excluded.next_status)`, `octet_length(excluded.next_status)`,
`bit_length(excluded.next_status)`, and searched `CASE` updates whose conditions
compare proposed and existing row values. Joined mutation-source selector fixtures now pin
single-output computed target-dependent subqueries as typed `match_expression`
residuals, while multi-output subqueries remain explicit fail-closed
update/delete joined-source fixtures so they cannot be silently routed through
untyped SQL until dependency nodes can represent them.

Claimed mutation-source SQL filters use the same boolean and null predicate
lowering as row reads: `IS TRUE` / `IS FALSE`, null-inclusive `IS NOT TRUE` /
`IS NOT FALSE`, and `IS UNKNOWN` / `IS NOT UNKNOWN` bind before storage. Golden
mutation-source fingerprints include nonzero scalar source OR/NOT suffixes, so
null-inclusive boolean predicates cannot collapse to indistinguishable
zero-predicate fingerprints.

Join, lateral, and joined mutation-source side filters use the same expression
predicate contract for null-safe distinct checks over computed and JSON-extracted
values, so side-local shapes such as `lower(alias.name) IS NOT DISTINCT FROM $1`
and `alias.metadata->>'source' IS DISTINCT FROM 'internal'` remain typed side
predicates rather than raw SQL fragments.
Aggregate source predicates, aggregate `FILTER` predicates, and window source
predicates follow the same contract, so computed and JSON-extracted null-safe
distinct checks remain typed before grouping, filtering, or window materializing.
The source parity corpus also requires computed `LIKE` / `ILIKE`
patterns to lower as typed expressions for row queries, aggregate filters,
join/lateral side filters, window filters, and joined mutation-source filters.

Generated REST/SDK artifacts are outputs of the OpenAPI and language-specific
generators, not authored relational plan definitions. `make generated-check`
regenerates the checked-in Zig, Go, TypeScript, and Python generated surfaces
from their specs and fails on drift, so typed REST/SDK contract changes start in
the authored OpenAPI/spec/Zig model and are then regenerated.

Aggregate filter parity includes declared JSON containment, JSON path equality,
JSON path existence, declared array element matching, array containment, array
equality, scalar membership, text-pattern filters, computed-array containment,
and expression/boolean filter groups. SQL syntax lowers into those typed
contracts where a stable operator form exists; REST and SDK callers may address
the typed aggregate filter arrays directly.

The fixture-backed gate also requires supported fingerprints for point,
claimed-source, joined-source primary-key rewrites that lower to `rewrite_identity`,
and recursive CTE read streams, plus fail-closed
unsupported classifications for view DDL and recursive CTE direct-write plans,
duplicate physical row targets inside one SQL-lowered row batch, invalid
conflict update targets, non-unique direct point update selectors, scalar-target
multi-output subquery delete selectors, so recursive read streams cannot bypass
the typed read-plan classifier, recursive insert/update/delete/merge streams
cannot bypass the typed write-plan classifier, multi-row `INSERT` cannot lower
into two writes for the same typed primary key, cross-owner DML cannot rekey rows
until it has a range-claimed identity-rewrite staging contract, unclaimed broad deletes
cannot bypass the mutation-source contract, invalid scalar-target multi-output
subqueries cannot smuggle ambiguous source tuples into a scalar semi-join
selector, and view metadata cannot pass through until it has native catalog
models.
Every public SQL lowerer normalizes native typed-plan validation failures to
stable fail-closed SQL classifications, so internal rows-request errors do not
leak through the PostgreSQL adapter boundary.

Fixture entries must have unique names, non-empty SQL, non-empty golden plan
fingerprints that match the declared typed-plan family, structured summary
anchors for supported typed-plan families (`ddl_tag` for DDL and `table_name`
for table-bound plans), fingerprints that include nonzero scalar membership and
JSON/array access-path node counts, optional deterministic `returning_rows` JSON
for row-producing mutation plans, and explicit stable reason tokens for
adapter-only or unsupported shapes that also appear in the golden fingerprint.
That keeps the external corpus from becoming a weak parser smoke test.
The focused `sql-api-parity-test` gate also includes a catalog-application
bridge for SQL DDL: representative `CREATE TABLE`, `CREATE INDEX`, `ALTER
TABLE`, trigger-policy, `DROP INDEX`, default/nullability, rename, type-change,
constraint-drop, and column-drop plans are lowered through the typed DDL
adapter and applied to public schema JSON, including rebuild, validation, and
rewrite flags.
The same gate also exercises public native row endpoints: `/rows/batch` seeds
typed relational rows, `/tables/{table}/rows/plan` runs the reusable
`RowsPlanRequest` union envelope for query, aggregate, window, join, and
lateral plans, and `/rows/mutation-source` stages a claimed typed update with
field and expression `RETURNING`, a joined typed update, and a claimed
table-emptying delete. Focused parser checks also pin the joined
mutation-source request contract and its independent target/source schema
validation. Fixture-backed parity also pins separate source-schema lowering for
direct equality join, direct bounded lateral, and top-level read-classifier join
and lateral statements, so cross-table side-qualified fields cannot regress back
to same-schema validation. SQL parity fixture coverage additionally requires joined
mutation-source lowering for non-primary semi-join selectors, simple correlated
semi-join selectors, and correlated semi-join selectors with source-local
filters for both update and delete plans. Those REST plan structs remain
executable without passing SQL text
through the backend.
The focused `sql-api-parity-test` gate also includes a DB-backed read-plan
execution bridge: representative SQL statements are classified through
`lowerReadPlanAlloc`, executed as the resulting typed relational storage plans,
and checked for deterministic left-join null-extension, CTE aggregate grouping,
window ranking, lateral null-extension, and explicit output ordering.
Catalog-backed equality join and bounded lateral regressions lower through the
table catalog, collect each side from its own DB handle under its own runtime
schema, and feed the same typed join/lateral executors, so source-schema
planning and execution cannot silently fall back to same-table assumptions.
The native REST/SDK row-query, aggregate, window, join, and lateral plan helpers
have the same DB-backed preimage bridge, so CTE-backed `RowsQueryPlan`,
`RowsAggregatePlan`, `RowsWindowPlan`, `RowsJoinPlan`, and `RowsLateralPlan`
execution can collect real storage rows through declared ranges, materialize
typed CTEs, and apply the existing JSON-row projection, aggregate, window, join,
and lateral executors without accepting SQL text.
Join and lateral execution also has side-schema-aware native helpers for direct
cross-table plans: the CTE stream, left stream, and right stream can each be
validated against their own runtime schema and collected from their own DB
handle before feeding the same join/lateral executor. The public plan envelope
carries durable side-table identity with `left_table` and `right_table`; omitted
side names resolve to the endpoint table, while explicit side names make
catalog-backed and hosted sources resolve each side stream through table/range
ownership before running the same typed executor.
The same gate includes a DB-backed write-plan execution bridge:
representative SQL `INSERT`, claimed `UPDATE`, claimed `DELETE`, joined
`UPDATE ... FROM`, and joined `DELETE ... USING` statements are classified
through `lowerWritePlanAlloc`, executed as native row-batch, mutation-source, or
joined mutation-source storage plans, committed where needed, and checked
through typed returning rows plus final relational readback.
The fixture-backed gate also accumulates corpus-level coverage and fails if the
external corpus no longer exercises DDL, each read family, direct and
source-driven mutations, joined mutations, adapter-only DDL, unsupported query,
DDL, insert, update, and delete classifications, scalar membership, JSON/array
access paths, text predicates, expression predicates including mixed
scalar/expression OR branches and mixed-side join residual expressions,
expression ordering, CTE streams, CTE-backed
row-query, aggregate, and window plans, and valid
deterministic `returning_rows` JSON for insert, point update, and point delete
families. It also requires `RETURNING *` fingerprints for insert, point update,
point delete, claimed update-source, claimed delete-source, joined
update-source, and joined delete-source plans, target-owned expression
`RETURNING` fingerprints for joined mutation-source plans, source-field,
source-computed, and mixed target/source computed `RETURNING` fingerprints for
joined mutation-source update and delete plans, and mixed-side joined
mutation-source match-expression fingerprints.
Conflict coverage is explicit:
the corpus must retain `DO NOTHING RETURNING *`, guarded `DO UPDATE WHERE`,
schema-bound unique, partial-unique, and expression-unique conflict targets, and
named `ON CONFLICT ON CONSTRAINT` coverage for enforced partial and expression
unique targets. Direct runtime-schema tests cover named primary-key and named
column-unique targets. `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE` without an
explicit deferral or `NOT VALID` clause lowers to enforced unique metadata so
the typed conflict-target resolver can use it immediately; explicitly deferred
or unvalidated additions remain outside conflict-target binding until promoted.
The corpus must also retain the unsupported duplicate-update-target
classification.
Catalog-bound entries must include setup SQL coverage and
applied-schema fingerprints derived from the same typed catalog transition that
the verifier executes. Stale hand-authored fingerprints are rejected even when
they match the `applied_plan` string grammar, so rebuild, validation, and
base-row rewrite/backfill work remain separately represented in the golden
corpus. Coverage accounting reads those lifecycle facts from exact typed
`applied_plan` tokens rather than substring probes, so counts such as
`unvalidated_unique=10` cannot satisfy a one-item coverage requirement.
Schema-json-applied DDL fixture families
(`CREATE TABLE`, `CREATE TABLE ... LIKE`, `CREATE INDEX`, `DROP INDEX`,
supported `ALTER TABLE`, `DROP TABLE`, and table-owned update policies) are
required to carry an `applied_plan` fingerprint on each fixture entry. Plain
`CREATE TABLE` fixture application starts from an empty catalog unless the
typed plan has exact `if_not_exists=true` or `replace=true` tokens; idempotent
and replacement forms must apply against the embedded base schema rather than a
substring-matched approximation of those flags. `DROP
TABLE` schema application removes the table catalog by producing an empty
schema JSON result; multi-table dependency cleanup and generation promotion
remain catalog workflow concerns.
Plain nullable `ADD COLUMN` is rebuild-only for derived artifacts. Constraint
additions, `SET NOT NULL`, and `VALIDATE CONSTRAINT` carry validation work, and
generated/default/non-null additions plus drop/rename/type replacement carry
base-row rewrite/backfill work. SQL DDL application now schedules validation
work as durable per-range claimed jobs with `validate/table/constraints`
metadata; DB workers execute those jobs by validating enforced unique owners,
foreign-key references, and check predicates over the claimed range before
marking the job ready. Migration-equivalent data changes use ordinary typed
source mutation plans (`insert_source`, `update_source`,
`delete_source`, and joined update/delete sources); the parity gate requires
those families independently from point DML so data backfill support cannot be
mistaken for single-row write coverage.

The source fixture also includes read-classifier entries for row-query,
aggregate, join, lateral, and window statements, proving that the
application-facing SQL boundary chooses the typed read envelope before storage
sees a request.

Parenthesized scalar SQL expressions are adapter-level grouping over the same
typed expression AST used by projections, order keys, aggregate inputs,
aggregate `FILTER`/`HAVING` predicates, and conflict-action assignments. The
source and fixture-backed SQL/API parity corpus includes row-query projection,
row-query order-key, aggregate-input, aggregate-predicate, and conflict-update
assignment cases so the grouped syntax cannot bypass the typed-plan boundary.
Aggregate predicate coverage includes both top-level conditions and OR/NOT
predicate groups where a branch starts with a parenthesized scalar expression.

Aggregate result ordering can also carry typed expressions over emitted
aggregate/group outputs, using the same output-column binding model as computed
`HAVING` expressions. Native REST/SDK `order_by` accepts output `field`, typed
`expr`, `direction`, and explicit null-placement keys, and the corpus pins
`ORDER BY (metric - group_count)` style plans with nonzero expression-order
fingerprints.
Scalar native `having` predicates and computed `having_expressions` both bind
against emitted aggregate outputs, so base source fields omitted from the
aggregate result fail during request validation.

Join and lateral result ordering use that same emitted-output binding model.
They can order by projected output names, ordinals, null-placement keys, or
typed expressions over projected outputs such as
`ORDER BY (right_metric - left_metric)`. Result rows keep unique JSON object
keys, while `result_schema.display_name` can preserve a non-unique SQL label for
presentation; SQL lowering must either bind references to the generated unique
native key or fail closed when a reference is genuinely ambiguous. Native read join/lateral requests expose the same output-bound
`order_by` shape; joined mutation-source requests keep target-side row ordering
for deterministic target claiming.
Left joins preserve side-local `ON` predicates by lowering
`ON left.key = right.key AND right.kind = 'customer'` into equality join keys
plus right-side source predicates, rather than moving the right-side filter into
post-join `WHERE` semantics that would accidentally collapse null-extended rows.
Mixed-side non-key `ON` expressions still require an explicit typed
match-expression lowering path and fail closed until represented that way.

Window result ordering uses the same output-column binding model. `ORDER BY`
clauses over window streams can name selected fields, named window outputs,
ordinals, null-placement keys, or typed expressions over emitted outputs such as
`ORDER BY (row_num + 1)`, and the corpus fingerprints nonzero window
expression-order plans. Native window requests expose the same result-level
`order_by` expression contract over selected source fields and window outputs,
and duplicate emitted field/window names fail during SQL lowering before a typed
window plan is accepted.

This means the important missing work is model-level, even when the visible
syntax is PostgreSQL: partial/expression index completeness, routed multi-range
DML, routed joins, CTE materialization, aggregate/window spill and routing,
durable cross-range queue ownership, and representative SQL corpus gates.
Adapter-only SQL/protocol concerns
such as `pgx` wire behavior, SQLSTATE text, catalog views, extensions,
dump boilerplate, and PL/pgSQL syntax should remain above this layer. They do
not require alternate relational storage paths; unsupported syntax either lowers to
a typed Antfly plan or fails before storage.

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
            "x-antfly-index-name": "users_email_active_idx",
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
    },
    {
      "name": "users_upper_email_key",
      "columns": ["tenant_id"],
      "expressions": [
        { "op": "upper", "field": "email" }
      ]
    }
  ]
}
```

Partial unique constraints are required for common SQL patterns such as "one
active row" and "unique when nullable value is present". Expression indexes are
required for normalized lookup keys such as `lower(email)` / `upper(email)` and
for computed ordering/filtering terms. SQL text is parsed above this layer; the
backend catalog stores typed Antfly AST metadata such as `{ "op": "lower",
"field": "email" }` or `{ "op": "upper", "field": "email" }` and predicate
atoms such as `{ "field": "status", "op": "eq", "value": "active" }`.

Partial secondary indexes are per-column catalog metadata. `x-antfly-index-name`
stores the stable DDL/catalog identity for the derived secondary index, and
`x-antfly-index-include` stores non-key covering payload columns for
`CREATE INDEX ... INCLUDE (...)` on ordinary btree secondary indexes. Unique
constraints store the same non-key covering payload columns as first-class
constraint metadata, and SQL `UNIQUE (...) INCLUDE (...)` lowers to that typed
metadata for both `CREATE TABLE` constraints and additive `ALTER TABLE` unique
constraints. Primary-key metadata has the same explicit covering payload list:
SQL `PRIMARY KEY (...) INCLUDE (...)` lowers to `primary_key.include_columns`
while row identity, physical keys, FK targets, and conflict binding continue to
use only `primary_key.columns`.
`x-antfly-index-where` stores the typed predicate. Writes evaluate the predicate against the same
committed packed row that supplies the column values. Matching rows receive the
ordinary column-major, array-element, or JSON-value side rows; non-matching rows
keep their authoritative base-row cells but do not receive secondary scan
entries. Rebuild, split, merge, and repair code use the same runtime column
catalog and the durable index identity, so partial side rows are deterministic derived state. Query planning
uses a partial secondary index only when the typed row-query predicates imply
the index predicate, otherwise it falls back to authoritative base-row scans and
final rechecks. The predicate grammar is the same simple typed `all` form used
by partial unique constraints: `is_null`, `is_not_null`, `eq`, and `ne` atoms
over declared relational columns.

Covering columns do not participate in index identity, uniqueness, ordering, or
predicate proof. They are durable catalog metadata that survives schema JSON and
runtime-schema clone/drop/rename paths, and schema validation rejects include
columns that do not exist, that repeat key columns for ordinary column indexes,
or that are JSON/array payload columns. Ordinary btree secondary indexes,
generated-expression secondary indexes, and JSON/array GIN secondary indexes all
preserve `INCLUDE (...)` payload columns as `x-antfly-index-include`, so
PostgreSQL covering-index syntax is not accepted unless the catalog can retain
the covering-column metadata exactly.

Unique expression constraints store the expression key directly on the unique
constraint, evaluate the key from the committed row, and maintain the
unique-owner row in the same 2PC path as ordinary column unique owners. The
legacy compact forms `lower(field)`, `upper(field)`, and `md5(field)` remain
encoded as `op + field` shorthand for stable case-fold and hash keys. Richer
deterministic scalar keys such as `replace(status, 'old', 'new')` are encoded as
the same shared row-expression AST used by checks, generated columns, partial
predicates, update transforms, and returning projections. AST-backed unique
keys participate in schema validation, column rename/drop dependency checks,
index generation hashing, duplicate conflict-target identity, row-write owner
tuple maintenance, and query candidate-set owner lookup through their referenced
row fields.
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
`DROP INDEX` lowers to a typed catalog mutation: named unique indexes remove
their unique constraint metadata, generated expression indexes remove the
generated index column, and ordinary secondary indexes clear their index
identity, lifecycle, generation, and predicate metadata while leaving the
authoritative row column intact and unindexed.
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
primary-key tuple, `primary_key.name` is durable typed constraint metadata for
custom primary-key names, and `{ "target": { "unique": { "name": "..." } } }`
means the named unique constraint's tuple inferred from the inserted row. SQL
`ON CONFLICT ON CONSTRAINT name` binds an explicit primary-key name or the
deterministic unnamed `<table>_pkey` adapter name to the primary target; named
unique constraints bind by their durable unique metadata. That inference
supports ordinary columns, typed expressions such as `lower(email)` and
`upper(email)`, typed partial predicates, and temporal no-overlap constraints by
deriving the proposed row interval and resolving overlapping temporal owner rows
without copying SQL text into storage metadata. The
conflict target and partial-index predicate must be part of the durable plan so
concurrent inserts for the same unique value conflict on the same owner record.
Only enforced unique constraints participate in conflict-target binding and
unique selector resolution. Unvalidated constraints remain catalog-visible
validation/rebuild work and are rejected as conflict targets until validation
promotion marks them enforced.
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
target, action, optional single `where_expression` guard, `where_expressions`
all-of guard list, and branch-group `where_any` / `where_not` guards. That keeps
SQL lowering and direct REST/SDK writes on one typed mutation-plan surface.
`excluded.column` is resolved from the typed proposed row before prepare.
Value expressions such as
`SET status = COALESCE(excluded.next_status, status, 'fallback')`,
`SET status = CASE WHEN excluded.amount > amount THEN 'bumped' ELSE status END`,
`SET status = LOWER(excluded.next_status)`, and
`SET status = INITCAP(excluded.next_status)` compile to typed `patch_expr`
operations.
Function-call `CONCAT(...)`, `CONCAT_WS(...)`, and SQL `||` conflict assignments
lower to typed row-expression nodes over existing/proposed row operands.
Array-valued conflict assignments such as
`SET tags = string_to_array(excluded.scope, ' ')` lower to the shared
`string_to_array` expression node instead of a conflict-only array transform.
JSON extraction from either source row, for example
`SET status = excluded.metadata->>'next_status'` or
`WHERE metadata->>'source' = 'old'`, lowers to the same typed `json_extract`
expression node used by ordinary read queries.
Numeric deltas such as `SET amount = amount + excluded.amount` and
`SET amount = amount + COALESCE(excluded.amount, 0)` compile to typed increment
operations. These conflict expressions lower through the native row-expression
AST as `patch_expr` or `increment_expr`, where `{ "source": "proposed" }` names
the inserted row and `{ "source": "existing" }` names the resolved conflict row.
Target-qualified existing-row references such as `u.amount` or
`public.usage_records.amount` normalize to that same existing-row source inside
conflict action expressions and guards; `excluded.amount` remains the proposed
row source. Storage receives source-tagged typed expressions, not SQL
qualifiers. Direct typed conflict-action expressions must also use explicit
`existing` or `proposed` field sources; unqualified `row` fields are reserved
for ordinary row streams and fail closed at the storage boundary in conflict
actions.
Assignment expressions are bound against the target column type, while condition
subexpressions can compare any valid existing/proposed fields. Ordinary query
expressions still use unqualified row fields.
SQL conflict action predicates such as
`DO UPDATE SET status = excluded.next_status WHERE excluded.amount > amount`
compile to a native `where_expression` condition on `on_conflict`. This is
separate from partial unique target predicates: target predicates choose the
unique owner row, while action predicates decide whether the resolved conflict
row is transformed. Conjunctive action predicates such as
`WHERE metadata->>'source' = 'old' AND excluded.metadata->>'source' = 'api'`
compile to the `where_expressions` all-of list, with each condition evaluated
against the same existing/proposed row sources. Disjunctive predicates compile
to `where_any` branch groups, and negated parenthesized predicates compile to
`where_not` branch groups, matching the row-query expression predicate group
contract while staying scoped to the resolved conflict row. Parenthesized SQL
groups such as `(existing_predicate AND proposed_predicate) OR (...)` preserve
their branch boundaries in the same typed group arrays, while individually
parenthesized terms such as `(existing_predicate) AND (proposed_predicate)`
normalize to the same all-of `where_expressions` list instead of flattening
into adapter-private SQL text. Bare boolean guards such as
`WHERE excluded.enabled AND NOT starts_with(status, 'archived')` lower to the
same `where_expression` shape as a boolean row-expression compared with `true`;
`excluded.*` fields read from the proposed row and target-qualified or
unqualified fields read from the existing row. A false action predicate appends
no transform, no version predicate, and no `RETURNING` row.
The storage validator enforces the same contract for direct REST/SDK typed
plans: `DO NOTHING` cannot carry action guards, every grouped guard condition is
bound against declared existing/proposed target fields before planning, and
empty predicate groups fail closed instead of evaluating as accidental truth.
`SET field = DEFAULT` resolves through the field's declared Antfly default
instead of storing a SQL expression.
For a partial unique constraint, the row API requires the conflict target to
repeat the catalog predicate in typed form. Field-only predicates use `where`:

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

Expression predicates use the same row-expression condition list stored on the
catalog constraint:

```json
{
  "target": {
    "unique": {
      "name": "users_active_lower_email_key",
      "where_expressions": [
        {
          "lhs": { "op": "lower", "args": [{ "field": "status" }] },
          "op": "eq",
          "rhs": { "value": "active" }
        }
      ]
    }
  }
}
```

If the supplied predicate is missing or does not match the catalog predicate,
the request is rejected before owner lookup. SQL `ON CONFLICT (...) WHERE ...`
and `ON CONFLICT ON CONSTRAINT ...` lower to the same canonical catalog
predicate metadata, so storage never receives adapter-private SQL text as a
conflict target. Rows that do not satisfy the partial predicate do not resolve a
unique owner and follow the insert path.

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
Every named field in `returning` must bind to a declared relational column on
the mutation target; unknown field names fail closed during request validation
instead of producing implicit JSON `null` slots. All-row returning can include
named extras, for example
`{ "returning": ["*"], "returning_expressions": [...] }`, as long as each extra
projection has a distinct output name that does not collide with a committed row
field. SQL `RETURNING table.*`, `RETURNING alias.*`, and normalized
`RETURNING public.table.*` lower to the same typed `{ "returning": ["*"] }`
contract for the mutation target; a qualifier that does not name the mutation
target fails closed before storage sees a plan. SQL target-qualified fields such
as `RETURNING alias.id` and `RETURNING public.table.status AS returned_status`
are normalized before planning, so storage receives ordinary typed fields and
field-expression projections rather than SQL-qualified names. Target-qualified
operands inside returning expressions, such as `RETURNING alias.status || ':' ||
alias.id AS status_key` or `RETURNING lower(alias.status) AS lower_status`, are
normalized into the same shared expression AST before storage sees the plan.
Point `UPDATE` and `DELETE` primary-key selectors apply the same normalization:
`WHERE alias.id = $1` and `WHERE public.table.id = $1` become the ordinary
structured primary selector, not a SQL-qualified storage predicate.
Single-table base-row reads, aggregates, and window sources apply that
normalization across their select lists, aggregate inputs and filters, `GROUP
BY`, source filters, window partitions/order keys, and result ordering: `SELECT
alias.id`, `SELECT alias.metadata->>'source'`, `lower(alias.status) AS
status_key`, `SUM(alias.amount)`, `COUNT(alias.id) FILTER (WHERE alias.status =
'open')`, `WHERE alias.status = $1`, JSONB/array/text source predicates,
computed source predicates, and `ORDER BY alias.created_at` lower to ordinary
typed row-query fields or typed expressions. The SQL adapter pre-binds the
single top-level `FROM` table for this purpose; joins, lateral stages, and
joined mutation sources keep their side-qualified validators instead of sharing
the single-table scope.
Single-table claimed mutation-source `UPDATE` and `DELETE` lower target-qualified
source predicates and order keys the same way: scalar predicates, JSONB path and
containment predicates, array predicates, text-pattern predicates, computed row
expressions, and `ORDER BY alias.field` / `ORDER BY public.table.field` are
bound against the mutation target and stored as ordinary typed row-query fields
or typed expressions. Joined mutation-source plans keep separate target/source
schemas and side-qualified validation instead of reusing this single-table
normalization scope.
Generated-column pushdown follows the same rule for single-table reads and
mutation sources: `WHERE lower(alias.email) = $1` can bind to a stored generated
lower column, and `ORDER BY concat(alias.tenant_id, ':', alias.status)` can bind
to a stored generated concat column before storage sees the plan. If no matching
generated column exists, supported computed predicates and order keys lower to
the shared expression AST instead of passing SQL-qualified text through storage.
Bare duplicate fields such as SQL `RETURNING *, id` are rejected because
the row-result JSON contract has no duplicate-column representation. Native
row-batch, mutation-source, and joined mutation-source plans enforce that
contract before emitting mutation result rows, and storage rechecks projected
all-row extras against the final committed row image before staging result
payloads. The PostgreSQL adapter validates the same output-label contract while
lowering `RETURNING`, so duplicate field names, aliases that collide with
selected fields, and `RETURNING *` extras that collide with real row fields fail
closed before a typed mutation plan is produced.
Ordinary row-batch and single-table mutation-source `returning_expressions`
evaluate over the committed row image only. Direct typed plans may use ordinary
row fields, or `existing` as an explicit committed-row alias; `source` and
`proposed` field sources fail closed because there is no joined source row or
proposed row image in that projection context. Joined mutation-source returning
may additionally use explicit `source` fields, and a single projection may mix
target row fields with source row fields because the joined mutation plan
carries both row images through result projection.
Expression returning uses the same row-expression AST as query projections and
currently supports field/literal, statement-bound `now`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`, `coalesce`, `lower`,
`upper`, `initcap`, `trim`, `ltrim`, `rtrim`, `replace`, `translate`, `substring`, `overlay`, `split_part`, `strpos`, `left`, `right`, `lpad`, `rpad`, `repeat`, `reverse`, `concat`, `concat_ws`, `length`, `octet_length`, `bit_length`, `ascii`, `chr`, `md5`, `starts_with`, `ends_with`, `like`, `ilike`, boolean `and`/`or`/`not`, `nullif`, `greatest`, `least`, `abs`, `round`, `trunc`, `floor`, `ceil`, `sqrt`, `sign`, `power`, numeric and fixed-duration interval arithmetic, searched `case`, typed `cast` to text, numeric, boolean, or datetime,
JSON extraction/existence/type/array-length/build-object conversion nodes, and
array length, position, append/remove, string conversion, and string split
nodes. PostgreSQL `RETURNING field`,
aliases, `NOW() AS label`, `CURRENT_TIMESTAMP AS label`, `CURRENT_DATE AS label`, `LOWER(...)`,
`UPPER(...)`, `INITCAP(...)`,
`TRIM(...)`, `BTRIM(...)`, `LTRIM(...)`, `RTRIM(...)`, `SUBSTRING(...)`, `SUBSTRING(... FROM ... [FOR ...])`, `SUBSTR(...)`, `SPLIT_PART(...)`, `STRPOS(...)`, `POSITION(... IN ...)`, `LEFT(...)`, `RIGHT(...)`, `LPAD(...)`, `RPAD(...)`, `REPEAT(...)`, `REVERSE(...)`, `STARTS_WITH(...)`, `ENDS_WITH(...)`, `ASCII(...)`, `CHR(...)`, `MD5(...)`, `DATE_TRUNC(...)`, `DATE_BIN(...)`, `DATE_PART(...)`, `EXTRACT(...)`, `CONCAT(...)`, `CONCAT_WS(...)`, text-like `lhs || rhs` concat expressions, `LENGTH(...)`, `OCTET_LENGTH(...)`, `BIT_LENGTH(...)`, `COALESCE(...)`, `NULLIF(...)`, `GREATEST(...)`, `LEAST(...)`, `ABS(...)`, `ROUND(...)`, `TRUNC(...)`, `FLOOR(...)`, `CEIL(...)`, `SQRT(...)`, `SIGN(...)`, `POWER(...)`, searched `CASE`, `CAST(...)` including encoded timestamp casts,
JSON extraction, JSON/array helper functions, numeric arithmetic, and fixed-duration
`INTERVAL` arithmetic projections lower into
those typed row-batch fields/expressions, including
`RETURNING *, named_extra_projection`. The same `returning` plus
`returning_expressions` shape is accepted on mutation-source update/delete plans
so local and routed execution can preserve projected result contracts without
carrying SQL text. Typed base-row sources used by reads, insert-source,
mutation-source, and joined mutation-source preflight every selected, filtered,
ordered, and computed field against the relevant table schema before planning,
so missing fields fail as invalid requests instead of degenerating into empty
matches or implicit null projections. Ordinary row-query, aggregate, window, and
CTE-consumer expressions bind field operands only to the current row stream;
`source`, `existing`, and `proposed` scopes are valid only in contexts that
explicitly define those row images. Join, lateral, and joined mutation-source
residual predicates define only a left/target row and a right/source row, so
unqualified or `row` fields bind left/target and explicit `source` fields bind
right/source; conflict-action `existing` and `proposed` scopes fail closed at
that boundary. Conflict actions define committed/proposed row images,
insert-source assignments define a source row, and mutation transforms define
their documented source scopes. Point,
insert-source, and single-table mutation-source
returning expressions bind only to the target row image; source-scoped fields are
valid only for joined mutation-source returning expressions, where unqualified
or `row` fields bind the target image and explicit `source` fields bind the
joined source image. Source-update plans evaluate `patch_expr` and
`increment_expr` once per selected row, apply table-owned update policies and
generated-column updates, and then evaluate `returning_expressions` from that
planned post-image. The result is emitted as a `returning` array on the batch or
mutation-source response. Inserts project the proposed row image.
Updates resolve the base row, apply the same transform logic used by storage,
and project the post-image. Deletes project the resolved pre-delete row image.
Row-batch updates, conflict-action updates, and mutation-source update plans
also enforce one explicit write family per target path before planning: static
patches, increments, expression patches, expression increments,
expression-valued JSON path updates, and joined `source_assignments` cannot
write the same field or an ancestor/descendant JSON path in one request.
Ordered array updates may repeat the same array field, and sibling JSON subpaths
remain valid because they have distinct concrete targets. The API parser rejects
ambiguous public requests, and storage repeats the check for typed
mutation-source requests constructed by internal callers. The PostgreSQL
adapter applies the same check to SQL `SET` lists before serializing them into
typed row-batch, conflict-action, mutation-source, or joined mutation-source
plans, so duplicate SQL assignment targets fail closed instead of depending on
JSON object duplicate-key behavior. SQL `INSERT` column lists are validated the
same way before row JSON is constructed, so repeated or path-colliding insert
targets cannot collapse during adapter serialization. SQL primary-key, unique,
foreign-key, and `ON CONFLICT (...)` column lists also reject duplicate members
before catalog or mutation plans are produced, keeping composite identity and
constraint metadata explicit. SQL `CREATE INDEX` element lists apply the same
rule to ordinary column members and supported unique expression members before
secondary-index or unique-expression metadata is emitted.
Update/delete returning adds a version predicate for the row image that was
projected, so the commit either installs/removes the value represented in the
response or fails with an OCC conflict. The projection is deliberately based on
row JSON/relational row state captured for the mutation, not a derived index
read after commit.

Write helper objects are exact API structs, not extension maps. `where` accepts
one row identity selector, either `primary` or `unique`; a unique selector is
exactly `{ "name": ..., "values": ... }`. `on_conflict.target` likewise accepts
only one target, either `primary: true` or a named unique target, and a unique
conflict target is exactly `{ "name": ..., "where": ... }`. Period-qualified row
identity selectors put the point-in-time period on `where.primary` or
`where.unique`; `on_conflict.target` does not carry a separate period object
because temporal conflict handling is inferred from the inserted row and the
target constraint's `without_overlaps_period` metadata. Partial unique predicate
groups are exact `{ "all": [...] }` objects and each predicate atom is exactly
`{ "field": ..., "op": ..., "value": ... }`, with `value` omitted only for
null-test operators. The row-operation envelope is also exact by operation kind:
`insert` accepts only `row`, optional `on_conflict`, and returning fields;
`upsert` accepts only `row` and returning fields; `update` accepts `where`,
typed mutation operators, returning fields, and `expected_version`; `delete`
accepts only `where`, returning fields, and `expected_version`. Computed row
filters are represented in row-query, mutation-source, or conflict-action
contracts, not as a top-level row-batch operation field. JSON and array
transforms are also exact:
`json_set` items carry only `field`, `path`, and `value`, while `array_update`
items carry only `field`, `op`, and `value`. Unknown keys fail request
validation before planning so SDK/OpenAPI structs, REST JSON, and SQL-lowered
typed plans share one canonical write contract.
Within one `rows/batch` request, row targets are set semantics rather than an
ordered script: the same derived physical row key may not appear more than once
across writes, deletes, and transforms. Duplicate primary-key inserts, repeated
point updates, update/delete pairs, and insert/delete pairs for the same row fail
before a storage batch is built; skipped `ON CONFLICT DO NOTHING` candidates do
not emit a target mutation and keep their no-op behavior.

#### Row claiming and lock semantics

`FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]` needs a first-class row-claim contract
for queue and ledger workloads:

```json
{
  "query": {
    "table": "jobs",
    "where": { "field": "status", "op": "eq", "value": "ready" }
  },
  "claim": {
    "mode": "for_update",
    "wait_policy": "skip_locked",
    "lease_ms": 30000,
    "owner_id": "session:7",
    "transaction_id": "00112233445566778899aabbccddeeff"
  },
  "order_by": [{ "field": "created_at", "direction": "asc" }],
  "limit": 100
}
```

Claims are transaction-bound. The query/API contract carries `mode`,
`wait_policy`, `lease_ms`, `owner_id`, and a 16-byte transaction identity exposed
as `transaction_id` hex at the API boundary. The row-claim object is exact:
unknown keys fail request validation. `mode` is `for_update`,
`for_no_key_update`, `for_share`, or `for_key_share`; `wait_policy` is `wait`,
`nowait`, or `skip_locked`,
`lease_ms` defaults to `30000` and must be positive, executable claims require a
non-empty `owner_id`, and the selected transaction id must be 32 hexadecimal
characters.
Storage lowers each claimed base row to a metadata key under
`txn_row_claim:<row-key>` and writes a pending 2PC intent for that key. The
intent payload records a version, `mode`, `wait_policy`, `skip_locked`,
`owner_id`, `lease_ms`, `expires_at_ns`, and the claiming transaction id, so
active ownership is durable and inspectable while the transaction is pending.
The intent is the lock:
competing claimers and ordinary writers both hit the same transaction-manager
conflict check. Ordinary batch and transaction mutations add an expected-absent
predicate on the row claim key, so a pending claim blocks direct writes,
transactional writes, updates, and deletes until the claiming transaction
commits, aborts, or expires. Later row claimers and ordinary row mutations
inspect conflicting row-claim payloads, abort expired owning transactions, and
retry the claim or predicate check once before staging. Transaction resolution
always consumes row-claim intent keys instead of applying them as user data, so a
commit releases the lock rather than leaving a durable row-claim record behind.
`for_no_key_update` currently uses the same exclusive conflict point as
`for_update`; `for_share` and `for_key_share` persist their distinct read-lock
strengths but also use the same durable intent key until a shared-lock
compatibility matrix is introduced. That is conservative for concurrency and
keeps reads from bypassing writer conflict checks.
The transaction manager rejects new writes to committed or aborted transaction
records, so an owner reclaimed through lease expiry cannot keep staging writes
with the stale transaction id.

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
`LIMIT`, projection, and row claiming once after that merge. Public row-plan
parsing and the shared query-contract parser accept the same row-claim object,
including `for_no_key_update`, `wait_policy`, and the `skip_locked`
boolean alias. `wait_policy: "wait"` claims all returned base rows or fails with
the underlying intent conflict. `wait_policy: "nowait"` uses the same durable
conflict check but returns the conflict immediately when a selected row is
already locked.
`wait_policy: "skip_locked"` attempts claims in result order and keeps scanning
past locked candidates until the requested limit is filled or the merged stream
is exhausted. Row claiming is deliberately base-row only: aggregate sources,
join sides, `count_only`, graph result sets, and chunk return modes are rejected
because they do not identify a single relational row to lock. SQL `SELECT ...
FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]` and API queue consumers should both
compile to this same row-claim contract. In a distributed deployment, remote owner-range routing
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
PostgreSQL adapter syntax such as `tags @> ARRAY['hot','new']::text[]` and
`tags = ARRAY['hot']` lowers into those same typed JSON-array predicate values.
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
before intersecting them. Native requests and SQL lowering validate `array_any`,
`array_contains`, and `array_eq` predicate values against the declared array
item type before producing typed plans; whole-array predicates must contain only
declared item values. Broader SQL operator sugar, such as dialect-specific null
treatment and nested multivalue path index selection, belongs above or beside
this typed predicate contract. Fully schemaless arrays can still live inside a
`json` column.

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
to `$addToSet` and appends only when the value is not already present. Native
API requests and SQL mutation lowering validate each transform value against the
array's declared item type before emitting a storage transform; a keyword array,
for example, rejects numeric and `null` mutation element values. SQL
`UPDATE`/`ON CONFLICT` uses `array_append(col, value)` and
`array_remove(col, value)` as transform sugar for literal, parameter, and
compatible proposed-row values. Assignments whose receiver is any computed array
expression, such as `array_append(string_to_array(status, ' '), 'new')`,
PostgreSQL `array_prepend('first', tags)`, or a parenthesized committed-row
receiver, stay on the shared typed expression tree and materialize as a
final-image `patch_expr` set during staging. Query
projections and predicates use the same function names as nullable typed
expression nodes: they preserve the receiver's array item type, append,
prepend, concatenate, remove, or replace JSON-equal values, and produce JSON
`null` when the receiver or element expression evaluates to `null`. Conflict actions also support
`array_append(tags, excluded.tag)` and `array_remove(tags, excluded.tag)` when
the proposed-row column is present and its declared type matches the target
array's declared item type;
`COALESCE(excluded.tag, 'fallback')` follows the same array-item type check
before becoming a concrete transform value. Element values are carried as JSON
values, not SQL fragments; source-row-dependent operands outside the proposed
insert row stay in the general typed expression tree.

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
the committed row. Claimed mutation-source updates and joined mutation-source
updates use the same contract, including expression-valued JSON-set operations
and `RETURNING` projections over declared JSON-column subpaths. Changing a JSON
column's embedded schema remains a derived-index rebuild, not a base-row
migration.

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
    { "name": "orders_amount_nonnegative", "field": "amount", "op": "gte", "value": 0 },
    {
      "name": "orders_net_positive",
      "expression": {
        "lhs": {
          "op": "add",
          "args": [
            { "op": "field", "field": "amount" },
            { "op": "field", "field": "fee" }
          ]
        },
        "op": "gt",
        "rhs": { "op": "value", "value": 0 }
      }
    }
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

Only enforced check constraints participate in row write validation.
Unvalidated checks remain catalog-visible validation work and are ignored by
the write planner until validation promotion marks them enforced.

- `{"op":"uuid_v4"}` on string-like identity columns (`keyword`, `text`, or
  `link`).
- `{"op":"now_ns"}` on `numeric` or `datetime` columns, stored as epoch
  nanoseconds.
- `{"op":"current_date_ns"}` on `numeric` or `datetime` columns, stored as the
  UTC day-start epoch nanosecond value.
- `{"op":"sequence_next","sequence":"name"}` on `numeric` columns, with optional
  `database` and `schema` fields carrying resolved catalog identity. This is
  durable schema metadata; public row-batch inserts, REST/SDK insert-source
  execution, SQL point/insert-source writes, `MERGE`, and `COPY FROM`
  materialize it through the server's explicit sequence-default resolver. Pure
  planner paths and unsupported write families still fail closed unless they
  receive that resolver.

Defaults are materialized once per planned row by the write planner before
primary-key encoding, unique tuple derivation, constraint checks, and
`RETURNING`. A generated column that references a server-defaulted column sees
the same UUID/timestamp value that is committed to the row. SQL adapters lower
`DEFAULT`, `gen_random_uuid()`/`uuid_generate_v4()` UUID defaults, and
`now()`/`CURRENT_TIMESTAMP[(0..6)]` timestamp defaults into typed operations
instead of passing SQL expression text through storage. The PostgreSQL adapter
lowerer applies the same boundary rule for explicit `DEFAULT`,
`gen_random_uuid()`/`uuid_generate_v4()`, `NOW()`,
`CURRENT_TIMESTAMP[(0..6)]`, and `CURRENT_DATE` expressions in inserts, updates,
conflict updates, and projections: UUID generation is valid for string-like
columns, while timestamp/date generation is valid for `numeric`/`datetime`
columns. The adapter materializes value defaults before
producing a row-batch plan, and keeps projection or conflict expressions in the
shared row-expression AST when the value must be evaluated by storage.
`CURRENT_DATE` lowers to the UTC day-start nanosecond timestamp through the same
typed expression path as `date_trunc("day", now())`. Omitted insert fields still
use the write planner's default materialization path.

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

Stored generated columns currently support typed `lower(field)`, `upper(field)`, `md5(field)`, and
`concat(fields, separator)` forms; user input cannot write generated columns
directly. Inserts materialize defaults and generated values into the committed
row image. Updates and conflict-target updates that touch tables with checks or
generated columns resolve the base row, apply the requested patch/JSON updates,
regenerate stored generated values as ordinary transform operations, and
validate checks against that final planned row image. Direct local schema
application can append stored generated columns and backfill existing rows when
every generated input is either an existing column or an appended non-generated
column with a non-null literal default; generated columns that depend on a
new nullable/no-default source or another newly appended generated column are
rejected until a catalog-owned rewrite job can order and validate the broader
dependency graph. `CHECK` predicates are
named typed constraints over declared columns. Simple checks can use atom
metadata (`field`, `op`, and optional `value`) for `is_null`, `is_not_null`,
`is_distinct`, `is_not_distinct`, `eq`, `ne`, `gt`, `gte`, `lt`, and `lte`.
Richer checks use an `expression` condition backed by the same storage-schema
row-expression AST used for predicates, `RETURNING`, conflict actions, and
mutation-source patches. Schema validation rejects checks that mix atom metadata
with expression metadata, rejects expression field references outside the
declared relational columns, and keeps expression checks row-only: they do not
capture proposed/existing/source-row scopes or adapter-private SQL text.
Enforced atom and expression checks both run after defaults, generated values,
patches, JSON/array transforms, and update policies have produced the final
planned row image. SQL DDL lowering for supported `CHECK (...)` clauses uses
the same contract: simple column comparisons stay as atom metadata, computed
conditions such as `lower(status) <> 'deleted'` or `amount + fee >= 0` lower to
native expression checks, grouped `AND`/`OR` predicate checks lower to the same
row-expression AST with boolean `and`/`or` nodes, and parenthesized or `NOT`
groups lower through native boolean expression nodes rather than adapter-owned
SQL text. The SQL adapter defers field binding while parsing DDL and relies on
catalog schema validation to bind expression fields against the completed table
definition before the schema update is accepted. The same native schema
validator infers expression-check output types, rejects incompatible comparison
operands, and requires range comparisons to use orderable types before any
runtime schema metadata is built. Schema application validates newly enforced
checks and `unvalidated` checks promoted to `enforced` against the existing
local row range before committing the catalog flip; invalid rows keep the prior
schema active.

#### Application-Time Temporal Constraints

Application-time temporal tables are a model-level target for the relational
engine, motivated by the SQL:2011/PostgreSQL 19 direction described in
[pgEdge's Postgres 19 temporal-table writeup](https://www.pgedge.com/blog/looking-forward-to-postgres-19-its-about-time).
Antfly exposes the semantics as typed catalog metadata first, with SQL
syntax lowering into that metadata instead of storing SQL text. The landed
storage contract uses finite `[start, end)` periods over two explicit boundary
columns. PostgreSQL-style single range columns such as `daterange` and
`tstzrange` are accepted by `CREATE TABLE` DDL lowering and expand into
`<period>_start` / `<period>_end` boundary columns plus period metadata before
storage sees the schema. The period metadata records the optional original
range subtype as `range_type` so SQL lowering can distinguish discrete
`daterange` normalization from continuous timestamp or numeric ranges after the
physical row model has expanded everything to scalar boundary columns. Finite canonical `numrange` literals such as
`'[1,10)'::numrange` and `'[2025-01-01,2025-07-01)'::daterange` are accepted by
`INSERT` lowering and become ordinary writes to the generated `<period>_start`
and `<period>_end` boundary fields. Timestamp range endpoints accept ISO
date/timestamp text with `Z` or numeric timezone offsets and normalize them to
UTC nanoseconds. Open-ended range bounds lower into the same typed period
metadata and row-write contract as null boundary fields before they reach
storage: upper-open literals such as `'[2026-01-01,)'::daterange` emit a finite
start and `null` end, and lower-open literals such as
`'(,2026-01-01)'::daterange` emit a `null` start and finite end.
Discrete `daterange` constructors and literals canonicalize finite exclusive
lower dates and inclusive upper dates by advancing that boundary to the next
day before writing owner metadata, matching the storage model's `[start, end)`
contract. For example, `(2025-01-01,2025-02-01]` becomes
`[2025-01-02,2025-02-02)`. Non-canonical bound flags for continuous
`numrange`, `tsrange`, and `tstzrange` stay fail-closed at SQL lowering until
the typed temporal model has explicit canonicalization rules for those range
subtypes.

The native schema shape is:

```json
{
  "periods": [
    {
      "name": "valid_time",
      "start_column": "valid_from",
      "end_column": "valid_to"
    }
  ],
  "primary_key": {
    "columns": ["tenant_id", "sku"],
    "without_overlaps_period": "valid_time"
  },
  "unique_constraints": [
    {
      "name": "sku_valid_time_key",
      "columns": ["sku"],
      "without_overlaps_period": "valid_time"
    }
  ],
  "foreign_keys": [
    {
      "name": "adjustments_price_fkey",
      "columns": ["tenant_id", "sku"],
      "period": "valid_time",
      "references": {
        "table": "account_prices",
        "columns": ["tenant_id", "sku"],
        "period": "valid_time"
      }
    }
  ]
}
```

`periods` declare application-time intervals over two `numeric` or `datetime`
columns of the same type. Bounds are half-open: `start` must compare strictly
before `end`, adjacent spans are allowed, and nullable start/end fields represent
unbounded lower/upper endpoints. Runtime storage encodes those open endpoints as
ordered finite/infinite sentinels in temporal unique-owner keys so no-overlap
checks, FK coverage, repair, and mutation splitting all use the same ordering
contract. Schema validation rejects duplicate period names, reused period
boundary columns, non-orderable boundary types, mismatched boundary types, and
period references that do not name a declared period on the local table.
`without_overlaps_period` on a primary key or unique constraint means the
scalar key tuple is unique over time: two rows with equal scalar key values
cannot have overlapping `[start, end)` intervals for the named period. Foreign
keys with `period` metadata are period-covering references: the child period
must be covered by parent rows with the referenced scalar key tuple and
referenced parent period.

Temporal primary/unique conflict targets use the same encoded owner index as
no-overlap enforcement. For an inserted row whose conflict target names a
`without_overlaps_period` constraint, planning derives the scalar key tuple and
the proposed `[start, end)` interval from the row image, scans the temporal
owner range for overlapping stored intervals, and resolves the conflict to that
owner. `DO NOTHING` suppresses any resolved overlap. `DO UPDATE` is allowed only
when the overlap resolves to one owner row; multiple source rows resolving to the
same owner or storage-visible overlap ambiguity fail closed instead of staging
multiple updates against one conflict row.

The SQL adapter lowers the supported DDL surface into that same catalog shape:

```sql
CREATE TABLE account_prices (
  tenant_id text NOT NULL,
  sku text NOT NULL,
  valid_from timestamptz NOT NULL,
  valid_to timestamptz NOT NULL,
  PERIOD FOR valid_time (valid_from, valid_to),
  PRIMARY KEY (tenant_id, sku, valid_time WITHOUT OVERLAPS)
);

CREATE TABLE price_adjustments (
  tenant_id text NOT NULL,
  sku text NOT NULL,
  adjustment_id text NOT NULL,
  valid_from timestamptz NOT NULL,
  valid_to timestamptz NOT NULL,
  PERIOD FOR valid_time (valid_from, valid_to),
  PRIMARY KEY (tenant_id, sku, adjustment_id, valid_time WITHOUT OVERLAPS),
  FOREIGN KEY (tenant_id, sku, PERIOD valid_time)
    REFERENCES account_prices (tenant_id, sku, PERIOD valid_time)
);
```

The landed execution shape is:

- Period validity is enforced as a native row constraint before ordinary
  primary-key, unique, and foreign-key work.
- `WITHOUT OVERLAPS` primary-key and unique constraints use interval owner rows
  keyed by scalar tuple plus period span. The write participant scans the owner
  prefix for the scalar tuple and rejects any staged or committed interval that
  overlaps the candidate `[start, end)` span, while allowing adjacent or
  disjoint intervals under the same scalar key.
- Schema application, unique-owner rebuild, and unique-owner repair understand
  temporal owner rows, so changing or repairing catalog-backed constraints does
  not collapse temporal intervals into scalar unique rows.
- Period foreign keys use the FK proof/reverse-reference machinery with a
  coverage proof for the child period. Local/same-table validation scans the
  parent key's temporal unique-owner spans in the current transaction and
  accepts only when adjacent or overlapping parent intervals cover the complete
  child `[start, end)` interval. Temporal FK checks deliberately do not reuse
  scalar externalized parent proofs: routed transaction parent-check payloads
  carry `child_period_start` and `child_period_end` JSON values, child
  participants accept an externalized proof only when those bounds match the
  final child row span, and parent participants validate coverage by scanning
  the referenced temporal primary/unique owner rows. This keeps hosted and
  provisioned temporal FK enforcement on durable owner metadata instead of
  trusting SQL text or child-supplied scalar existence checks. Restrictive
  `restrict` / `no_action` parent actions reject only when the child span is no
  longer covered after the pending parent change. Bounded `set_null` and
  `cascade` delete actions use the same remaining-coverage proof: if another
  parent interval still covers the child period, the child remains unchanged;
  once coverage is gone, `set_null` clears the nullable child reference columns
  and `cascade` deletes the child row through the ordinary relational write
  path.
- `UPDATE`/`DELETE ... FOR PORTION OF period FROM start TO end` lowers to the
  native mutation-source `temporal_portion` contract. The table's primary key
  must be `WITHOUT OVERLAPS` for that same period, because splitting a row
  creates multiple physical rows with the same scalar identity. Storage claims
  the qualifying physical rows, computes the overlap with the requested portion,
  and rewrites the row as left, affected, and right fragments. The affected
  fragment is updated or deleted, adjacent fragments retain the original values,
  and a portion spanning multiple qualifying source rows is evaluated per
  source row without coalescing adjacent affected fragments. This preserves the
  same application-time semantics as PostgreSQL 19 `FOR PORTION OF`: storage
  may split row counts upward, but it never merges separately sourced intervals
  just because their post-update values match. All staged writes/deletes flow
  through the ordinary relational transaction path.
  Whenever a portion mutation splits a physical row, storage deletes the source
  row and writes every remaining fragment under a canonical key derived from the
  scalar primary-key tuple, period name, and normalized `[start, end)` span
  rather than appending to the current physical key. Repeated splits therefore
  keep stable, non-nested row identities while defaults, generated columns,
  checks, temporal uniqueness, foreign keys, secondary indexes, replay, and
  returning projections observe normal row mutations.
- Temporal parent delete/update checks revalidate child rows against the
  remaining temporal unique-owner coverage instead of treating the scalar parent
  tuple as a blind reference. This allows adjacent/disjoint parent intervals to
  coexist while still rejecting deletion or interval rewrites that would leave a
  child `PERIOD` uncovered.
- Temporal `WITHOUT OVERLAPS` constraints are not treated as scalar point-unique
  selectors by the query planner. A predicate on only the scalar key can match
  multiple application-time intervals, so scalar-only selectors fail closed.
  Public row identity supports period-qualified primary and unique selectors
  that include `{ "period": { "name": "...", "at": ... } }`; those selectors
  resolve through the temporal unique-owner API, whose owner-local storage path
  scans the matching temporal unique-owner prefix and returns only the interval
  whose span contains the requested point.
- PostgreSQL range-column syntax is front-end sugar, not a second storage model.
  `CREATE TABLE` DDL for `valid_at daterange`, `PRIMARY KEY (..., valid_at
  WITHOUT OVERLAPS)`, and `FOREIGN KEY (..., PERIOD valid_at)` lowers to
  `valid_at_start` / `valid_at_end` boundary columns plus a `valid_at` period
  catalog entry. `INSERT` values may address the public period name when the
  value is a canonical range literal, for example
  `'[1,10)'::numrange`; the adapter expands that single public value into the
  generated start/end fields before row-batch validation. Date/timestamp range
  literals such as `'[2025-01-01,2025-07-01)'::daterange` and
  `'[2025-01-01T01:30:00+01:30,2025-01-02T00:00:00Z)'::tstzrange` follow the
  same path after endpoint normalization to UTC nanoseconds. Open-ended
  literals such as `'[2026-01-01,)'::daterange` and
  `'(,2026-01-01)'::daterange` emit `null` for the generated open boundary at
  the typed API boundary and ordered negative-/positive-infinity sentinels in
  temporal owner metadata. Period metadata preserves the original range subtype
  as optional `range_type`, which lets discrete `daterange` constructors and
  literals normalize inclusive upper dates to the next exclusive date before
  owner rows are written. Other non-canonical bound flags fail closed rather
  than being widened into ambiguous owner intervals.
  `UPDATE` / `DELETE ... FOR PORTION OF valid_at FROM
  ... TO ...` addresses that same public range-column period name; the typed
  request still carries only the period name plus normalized `from` / `to`
  bounds, and storage splits the generated boundary columns through the ordinary
  temporal mutation-source path.

#### Relational query lowering

Joins, aggregates, `ORDER BY`, `LIMIT`, and `OFFSET` already have engine pieces,
but they need a first-class relational query contract that plans against base
rows and declared indexes before a SQL DSL can expose them as ordinary query
clauses:

```json
{
  "from": "orders",
  "where": {
    "all": [
      { "field": "tenant_id", "op": "eq", "value": "t1" },
      { "field": "status", "op": "eq", "value": "open" }
    ]
  },
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
      "as": "active_verified",
      "expr": {
        "op": "and",
        "args": [
          { "field": "enabled" },
          { "op": "not", "args": [{ "field": "archived" }] },
          { "op": "starts_with", "args": [{ "field": "status" }, { "value": "op" }] }
        ]
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
    "wait_policy": "skip_locked",
    "owner_id": "session:7",
    "transaction_id": "00112233445566778899aabbccddeeff"
  },
  "doc_key_range": { "start": "tenant:t1/order:", "end": "tenant:t1/order~" },
  "limit": 50,
  "offset": 0
}
```

`where` is the typed `RowsWhere` predicate tree in the REST/SDK contract, not a
free-form filter map. A top-level predicate is one `RowsWhereAtom`, an `all`
conjunction of atoms, `any` / `not` branch groups, or an `all` conjunction plus
branch groups. Each branch is either one `RowsWhereAtom` or an `all`-only
conjunction of `RowsWhereAtom` predicates; scalar-only branches and branches
with structured atoms map to separate native predicate-group arrays. The
`where` key is omitted for an unfiltered scan; when present, it must be one
exact branch, and empty `where`, `all`, `any`, or `not` objects are rejected.
Bare boolean SQL predicates lower to the same typed expression-condition
contract as computed predicates: `WHERE enabled`, `WHERE enabled AND NOT
verified`, and `WHERE enabled OR starts_with(status, 'op')` become a boolean
row-expression AST compared with `true` in `expression_where`. This keeps the
boolean logic visible to REST/SDK callers as ordinary `and`/`or`/`not`
expression nodes instead of making it a SQL-only predicate form.
Predicate atom objects are exact objects over declared non-empty column
names; callers use explicit JSON `null` values for null-safe comparisons rather
than relying on omitted operands. Scalar predicates are
typed atoms over declared relational columns (`is_null`, `is_not_null`,
`is_distinct`, `is_not_distinct`, `eq`, `ne`, `gt`, `gte`, `lt`, `lte`). Array
and JSON operators are also typed atoms: `array_any` requires a declared
`array` column and compares one typed JSON value against
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
rewriting them to ordinary `eq`/`ne` predicates. The same null-safe operators
are available through the shared row-expression condition contract for computed
and JSON-extracted values such as `lower(email) IS NOT DISTINCT FROM $1` and
`metadata->>'source' IS DISTINCT FROM 'internal'`. Shorthand equality maps such as
`{ "where": { "status": "ready" } }` are rejected; callers use typed atoms such
as `{ "where": { "field": "status", "op": "eq", "value": "ready" } }`.
`where.any` carries disjunction branches: each branch is either one typed atom
or an `all` list of typed atoms, branches are unioned, and the row must match at
least one branch in addition to any top-level `all` predicates. Indexed scalar
OR branches are planned as independent candidate sets and unioned before
authoritative hydrated-row recheck; branches that contain membership,
text-pattern, declared-array, or declared-JSON atoms use the mixed access
predicate group and retain the same hydrated-row recheck contract. Unindexable
branches fall back to base-row scanning rather than returning partial results.
SQL parentheses around OR branches are accepted when each parenthesized branch
is the same supported AND-only predicate group; the parentheses do not create a
separate boolean AST, they only delimit the existing typed branch.
`field IN (...)`, `field = ANY(...)`, and `field = SOME(...)` inside scalar OR
branches lower by bounded expansion into ordinary equality branches, while
`field != ANY(...)` and `field <> SOME(...)` lower by bounded expansion into
ordinary inequality branches. Scalar `field = ALL(...)` and `field <> ALL(...)`
inside OR branches append bounded equality or inequality conjunctions to the
current branch. Scalar `field NOT IN (...)` inside OR branches lowers to a
bounded non-null inequality conjunction inside the current branch; lists with
`NULL` fail closed because SQL `NOT IN` null semantics cannot be represented as
plain inequality checks. Structured atoms such as `metadata @> ...`, `metadata
? ...`, `tags @> ARRAY[...]`, and `status ILIKE ...` lower to the same public
`where.any` branch contract and are represented internally as mixed access
groups. PostgreSQL array constructors are accepted as membership
inputs in the same places as JSON-array parameters, so `status =
ANY(ARRAY['open','pending']::text[])`, `status = ALL(ARRAY['open'])`, and
`lower(email) = ANY(ARRAY[...])` lower to the same typed scalar or expression
membership plans as their parameterized forms. Positive expression
membership such as `lower(field) IN (...)`, `lower(field) = ANY(...)`,
`lower(field) = SOME(...)`, `json_col->>'path' IN (...)`,
`json_col->>'path' = ANY(...)`, and `json_col->>'path' = SOME(...)` lowers to
bounded `expression_any` equality groups over cloned row-expression nodes.
Top-level `OR` between computed or JSON-extraction expression predicates lowers
to the same `expression_any` branch-group shape, and `AND` inside each OR branch
stays as multiple conditions in that branch. Parentheses around expression OR
branches are accepted under the same AND-only branch rule. When one top-level OR
branch needs computed or JSON-extraction evaluation and another branch is an
ordinary scalar field comparison, the whole OR lowers to `expression_any`;
scalar comparisons in those branches are represented as field-expression
conditions so the query has one authoritative disjunction representation.
Standalone SQL boolean constants at row-query predicate boundaries lower through
the same expression contract: `TRUE` is a no-op conjunction, `FALSE` is an
executable constant-false expression condition, and boolean constants inside
expression OR/NOT groups become ordinary typed expression groups rather than a
storage-side SQL special case.
Bounded `= ALL` and `<> ALL` inside an expression OR branch lower to
branch-local equality or inequality conjunctions, so
`lower(field) = ALL(...) OR json_col->>'path' <> ALL(...)` remains one
`expression_any` plan with multiple conditions inside each affected branch.
Bounded non-empty scalar `field != ANY(...)` and `field <> SOME(...)` lower to
native scalar OR inequality branches; computed and JSON-extracted
`lower(field) != ANY(...)`, `lower(field) <> SOME(...)`,
`json_col->>'path' != ANY(...)`, and `json_col->>'path' <> SOME(...)` lower to
bounded `expression_any` inequality groups. These are distinct from
`NOT IN`: SQL `!= ANY` / `<> SOME` means at least one element is unequal, so it
is an OR of inequality comparisons rather than negated set membership.
Bounded non-empty `field = ALL(...)`, `lower(field) = ALL(...)`, and
`json_col->>'path' = ALL(...)` lower to ordinary conjunctions of scalar or
expression equality predicates. Negative membership such as
`lower(field) NOT IN (...)`, `json_col->>'path' NOT IN (...)`,
`lower(field) <> ALL(...)`, `json_col->>'path' <> ALL(...)`,
`NOT (lower(field) IN (...))`, and `NOT (json_col->>'path' = ANY(...))` lower
to bounded `expression_not` equality groups. Scalar `field <> ALL(...)` lowers
to the native negated membership predicate. Function-derived and
JSON-extracted membership therefore use the same typed expression AST as
projection, ordering, and aggregate inputs. Parenthesized negated expression
groups distribute `AND` across bounded membership alternatives, so a shape like
`NOT (lower(field) IN (...) AND upper(field) = ...)` remains one typed negative
group per excluded value plus the shared `AND` condition. `where.not`
carries negative branches: each branch is one typed atom or an `all` list of
typed atoms, and a hydrated row is rejected when it matches any negative branch.
Structured negative branches such as `NOT (metadata ? 'flags' AND tags @>
ARRAY['cold'])` use the same mixed access predicate group as structured
`where.any` branches. Negative branches are recheck-only today; they
intentionally do not subtract candidate sets until the general typed boolean
expression planner can prove the subtraction is complete for indexed and
unindexed paths.
Projections are field lists, `select_all`, and named extras over the shared
row-expression AST. `SELECT *` lowers to `select_all`; `SELECT *, lower(field)
AS key, json_col->>'path' AS path_text` lowers to `select_all` plus named typed
projection entries. Bare duplicate columns after `*`, such as `SELECT *, id`,
remain rejected because the row-result JSON contract has no duplicate-column
representation. Compact API projection lists such as `json_extract`,
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
`RowsExpression` nodes rather than endpoint-local JSON blobs. The schema and
route parser are closed: expression, expression-condition, projection,
case-branch, expression-group, and array-contains predicate objects reject
unknown fields, and each expression variant accepts only the keys that belong to
that variant. `RowsExpression` is a branch contract, not a loose optional-field
object: field nodes accept `field` plus optional `source`, literal nodes accept
only `value`, and operator nodes accept `op` plus the operator-specific operand
metadata. `RowsExpressionCondition` requires `lhs` and `op`, requires `rhs` for
value-bearing comparison operators, rejects `rhs` for null-test operators, and
expression predicate groups require at least one condition. Public route
handlers still validate each expression against the table schema before
execution. Expression projections currently cover `field`, JSON-literal
`value`, statement-bound `now`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`, `coalesce`,
`lower`, `upper`, `initcap`, `trim`, `ltrim`, `rtrim`, `replace`, `regexp_replace`, `regexp_count`, `regexp_instr`, `regexp_substr`, `translate`,
`substring`, `overlay`, `split_part`, `strpos`, `left`, `right`, `lpad`,
`rpad`, `repeat`, `reverse`, `ascii`, `chr`, `md5`, `starts_with`,
`ends_with`, `like`, `ilike`, boolean `and`/`or`/`not`, `concat`,
`concat_ws`, `length`, `octet_length`, `bit_length`, `nullif`, `greatest`, `least`, `abs`,
`round`, `trunc`, `floor`, `ceil`, `sqrt`, `sign`, `power`, `add`, `sub`,
`mul`, `div`, `mod`, `interval_ns`, `interval_months`, `date_trunc`,
`date_bin`, `date_part`, searched `case` branches, typed `cast` nodes,
`json_extract`, `json_path_exists`, `json_typeof`, `json_array_length`,
`jsonb_build_object`, `to_jsonb`, `array_length`, `array_position`, `array_positions`,
`array_append`, `array_prepend`, `array_cat`, `array_remove`, `array_replace`, `array_to_string`, and
`string_to_array`.
`array_to_string(array, delimiter)` skips null array elements. The optional
third `null_text` operand preserves null-element positions by emitting that text
for each null element, while a null source array, delimiter, or `null_text`
operand still produces a null expression result.
The storage-side row-expression evaluator also executes the JSON subset needed
by schema/catalog enforcement, including `json_extract`, `json_path_exists`,
`json_typeof`, `json_array_length`, and `to_jsonb`, so check constraints,
validation jobs, and future generated/indexed metadata paths do not rely on a
weaker expression dialect than public row queries.
Projection and compact alias objects are
exact public contract objects with non-empty output aliases, and computed
array-containment predicates require a typed expression plus an array operand.
SQL `CONCAT(...)`, `CONCAT_WS(...)`, and text-like
`lhs || rhs` expressions lower to typed text-expression nodes. `lower`,
`upper`, `initcap`, `trim`/`btrim`, `ltrim`, `rtrim`, `replace`, `regexp_replace`, `regexp_count`, `regexp_instr`, `regexp_substr`, `substring`, `overlay`, `split_part`, `strpos`,
`left`, `right`, `lpad`, `rpad`, `repeat`, `reverse`, `ascii`, `chr`, `md5`, `starts_with`, `ends_with`, `like`,
`ilike`, and `length` accept validated text expressions, so operands may be
fields, literals, casts-to-text, JSON text extraction, `COALESCE`, `NULLIF`,
searched `CASE`, `CONCAT`, `CONCAT_WS`, or `lhs || rhs` expressions rather than only bare
columns. `trim`/`btrim`, `ltrim`, and `rtrim` accept one text operand plus an
optional text trim-character-set operand, default to ASCII whitespace, compare
and remove complete UTF-8 codepoints rather than bytes, and propagate `null`
when any operand is `null`. `now` emits the nanosecond timestamp bound when the typed plan was
parsed/lowered, `coalesce` emits the first present non-null operand, `lower`
emits a lowercased string or `null`, `upper` emits an uppercased string or
`null`, `initcap` emits an ASCII title-cased string or `null`, `lpad` and `rpad` pad or truncate complete UTF-8 codepoints to the
requested target length, default the fill text to a single ASCII space, propagate
`null` from any operand, and fail closed when an empty fill string would be
needed to extend the input, `repeat` repeats a validated UTF-8 text operand a
non-negative numeric count and propagates `null` from either operand, `reverse`
reverses complete UTF-8 codepoints and propagates `null`, `starts_with`
returns a boolean prefix test over validated text operands and propagates
`null` from either operand, `like` and `ilike` return SQL-pattern matches over
validated text operands and propagate `null` from either operand, `concat`
emits the concatenated scalar text of each operand while treating null operands
as empty, and `concat_ws` emits separator-joined text operands, skips null value
operands, and returns `null` when the separator is `null`.
`date_trunc` accepts a validated text unit and numeric or
datetime nanosecond timestamp, truncates in UTC for `microsecond`,
`millisecond`, `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`,
and `year`, returns a datetime nanosecond value, and propagates `null` from
either operand.
`date_part` accepts the same timestamp input, returns numeric UTC parts for
`epoch`, `year`, `isoyear`, `quarter`, `month`, `week`, `day`, `doy`, `dow`,
`isodow`, `hour`, `minute`, `second`, `millisecond`, and `microsecond`, and is
the API node used for SQL `DATE_PART(unit, timestamp)` and
`EXTRACT(unit FROM timestamp)`. SQL/API parity pins `epoch`, sub-day units, and
calendar units such as `isoyear`, `week`, `quarter`, `isodow`, and `doy`.
`regexp_replace(text, pattern, replacement[, flags])` lowers to the native
`regexp_replace` expression node. It supports first leftmost match replacement
by default and global replacement with the `g` flag through Antfly's regex
engine. Invalid patterns, empty-width matches, unsupported flags, and
backreference-style replacement strings fail closed instead of widening into
adapter-specific behavior. `regexp_count(text, pattern)` lowers to the native
numeric `regexp_count` expression node and counts non-overlapping leftmost
matches through the same compiled regex evaluator. `regexp_instr(text, pattern)`
lowers to the native numeric `regexp_instr` expression node and returns the
one-based UTF-8 codepoint position of the first leftmost match, or `0` when no
match exists. `regexp_substr(text, pattern)` lowers to the native nullable text
`regexp_substr` expression node and returns the first leftmost non-empty match,
or `null` when no match exists.
`translate` maps UTF-8 codepoints by ordinal from `from_text` to `to_text`,
removes source codepoints whose `from_text` ordinal has no corresponding
`to_text` codepoint, keeps non-matching codepoints unchanged, and propagates
`null` from any operand. `substring` emits a UTF-8 codepoint slice using a one-based
positive start and optional non-negative length, propagating `null` when any
operand is `null`. `overlay` emits the source string with a replacement string
spliced at a one-based positive start, replacing either the explicit
non-negative length or, when `for`/length is omitted, the replacement string's
UTF-8 codepoint count; it propagates `null` from any operand. `split_part` emits a text field from a delimited string,
uses one-based positive field indexes from the front, negative field indexes
from the end, returns an empty string when the requested field is absent, and
rejects a zero field index. `strpos` emits the one-based UTF-8 codepoint
position of a needle inside a string, emits `0` when the needle is absent, emits
`1` for an empty needle, and propagates `null`. `left` emits the first N UTF-8
codepoints when N is non-negative, or all but the last `abs(N)` codepoints when
N is negative. `right` emits the last N UTF-8 codepoints when N is
non-negative, or all but the first `abs(N)` codepoints when N is negative. Both
clamp oversized counts to the available string and propagate `null`. `length` emits the UTF-8 codepoint count of one string
operand, propagating `null`. `nullif` emits `null` when both non-null operands are equal,
otherwise it emits the first operand. `abs` emits the absolute value of one
numeric operand, `round` rounds one numeric operand to the nearest integral JSON
number, `trunc` drops the fractional component toward zero, `floor` and `ceil`
apply the matching rounding operation, `sign` returns -1, 0, or 1 for negative,
zero, or positive numeric input, and `sqrt` returns the square root,
and all return `null` when their operand is `null`. `power` accepts two numeric
operands, returns `null` when either operand is `null`, and rejects non-finite
results at the typed-plan boundary so JSON output never contains infinities or
NaN. `sqrt` rejects negative
numeric inputs at the typed-plan boundary instead of producing a non-finite JSON
number. `greatest` and `least` emit the greatest
or least non-null scalar from a same-family numeric, string, or boolean operand
list and return `null` when every operand is `null`. `add`, `sub`, `mul`, and `div` accept
numeric operands, propagate `null`, and reject non-numeric operands at the
typed-plan boundary. `interval_ns` wraps one numeric fixed-duration operand and
is valid only with `add`/`sub`: `numeric_or_datetime + interval_ns`,
`interval_ns + numeric_or_datetime`, or `numeric_or_datetime - interval_ns`.
It is stored and evaluated as an integer nanosecond delta, so fixed-duration SQL
interval literals lower to the same node. `interval_months` wraps one numeric
calendar-month operand for SQL month/year intervals. Calendar arithmetic is
evaluated in UTC against integer nanosecond timestamps and clamps end-of-month
dates to the destination month length. Mixed fixed-duration and calendar
interval literals lower to nested typed arithmetic, applying the calendar-month
component first and the fixed-duration nanosecond component second instead of
flattening months into nanoseconds.
`date_bin` is the exception to the general interval-arithmetic rule: its stride
may be a raw numeric nanosecond value or an `interval_ns` fixed-duration value,
but `interval_months` is rejected because calendar buckets have variable
duration. The same `date_bin` expression node is valid in projections,
predicates, order keys, and conflict-action assignments over existing and
`excluded` row sources.
Point updates, conflict actions, and claimed mutation-source updates use the
same typed expression nodes for text concat assignments, numeric/datetime
assignments, and fixed, calendar, or mixed interval arithmetic; a bare interval
is a duration operand only and fails closed as a standalone row value.
Division by zero is rejected during expression evaluation.
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
either operand, and rejects non-text operands, non-string delimiter literals, and
empty literal delimiters at the typed request boundary.
`expression_array_contains` is the REST/SDK predicate form for computed arrays:
each item carries an `expr` row-expression plus an array `value`, and hydrated
rows match when the computed array contains every requested value. For
`string_to_array` expressions the requested value is validated as an array of
strings at the typed request boundary. This is the typed home for SQL sugar such as
`string_to_array(scope, ' ') @> $json_array` and
`string_to_array(scope, ' ') @> ARRAY['read']`; declared array-column containment
continues to use `where` atoms with `op: "array_contains"` so index pushdown can
stay column-aware.
Exact computed-array equality and inequality use the existing `expression_where`
form with `op: "eq"` or `op: "ne"` and an array-valued `rhs`, which keeps
structured expression comparison on the shared row-expression predicate path.
SQL shapes such as
`field AS label`, `COALESCE(...) AS label`, `LOWER(text_expr) AS label`, `UPPER(text_expr) AS label`, `INITCAP(text_expr) AS label`, `TRIM(text_expr[, chars]) AS label`, `BTRIM(text_expr[, chars]) AS label`, `LTRIM(text_expr[, chars]) AS label`, `RTRIM(text_expr[, chars]) AS label`, `CONCAT(...) AS label`, `CONCAT_WS(...) AS label`,
`LENGTH(text_expr) AS label`, `OCTET_LENGTH(text_expr) AS label`, `BIT_LENGTH(text_expr) AS label`, `NULLIF(...) AS label`, `GREATEST(...) AS label`, `LEAST(...) AS label`, `ABS(...) AS label`, `ROUND(...) AS label`, `FLOOR(...) AS label`, `CEIL(...) AS label`, searched `CASE WHEN ... THEN ... ELSE ... END AS label`,
`CAST(expr AS text|numeric|bool) AS label`, `metadata->'flags' AS flags`,
`metadata->>'source' AS label`, `array_length(tags, 1) AS label`,
`string_to_array(scope, ' ') AS scope_parts`, numeric
arithmetic projections with `+`, `-`, `*`, `/`, `%`, and `MOD(...)`, and `id::text AS id_text`
lower to typed projection nodes rather than carrying SQL cast/function strings
into storage. Ordering is over
declared scalar columns or typed null-test expressions (`null_test: "is_null"` /
`"is_not_null"`), which lets SQL shapes such as `ORDER BY (expires_at IS NULL),
expires_at ASC` compile without synthetic user columns. PostgreSQL
`ORDER BY field NULLS FIRST/LAST` syntax lowers to the same native shape by
expanding into a leading `field IS NULL` order key followed by the requested
field or expression order key. Null and missing order keys sort after present
scalar values for plain field ordering; null-test order keys sort by the
produced boolean value. The executor projects from the
relational row JSON/codec and applies `OFFSET`/`LIMIT` after filtering and
ordering. SQL `LIMIT ALL` and `LIMIT NULL` lower to the same typed plan as an
absent `limit`; `OFFSET NULL` lowers to offset zero; `OFFSET n ROW/ROWS` lowers
to the same numeric `offset` as `OFFSET n`, and `FETCH FIRST/NEXT n ROW/ROWS
ONLY` lowers to the same numeric `limit` as `LIMIT n`. `FETCH FIRST ROW ONLY`
lowers to `limit: 1`. SQL output-order
ordinals such as `ORDER BY 2 DESC` are resolved by the adapter against the
lowered projection list before planning reaches storage; the stored plan carries
the resulting field key or cloned typed expression key, not the ordinal. SQL
output aliases such as `ORDER BY email_key` follow the same path: a unique
projected alias resolves to the underlying field key or typed expression key,
and repeated presentation labels lower to generated native keys when the
reference can be resolved deterministically. Row response metadata may carry
those labels separately as `result_schema.display_name` while retaining unique
`name` keys.
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
backslash escapes for literal wildcard bytes. SQL `LIKE`/`ILIKE ... ESCAPE ...`
is adapter sugar: the adapter accepts a one-byte literal or bound escape
character and normalizes it into this backslash-escaped typed pattern before
storage sees the request. The same normalization is used anywhere the typed
contract accepts text-pattern atoms: row filters, OR/NOT access groups,
aggregate and window filters, join/lateral side filters, and mutation-source
side filters. `case_insensitive` gives the ASCII-insensitive `ILIKE` behavior,
and `negated` represents `NOT LIKE` / `NOT ILIKE`. Without a declared
text-pattern access path, the planner evaluates these predicates over hydrated
bounded row streams. Computed expression patterns use first-class boolean
expression nodes: native REST/SDK expressions accept `{"op":"like"}` and
`{"op":"ilike"}` over text operands, and SQL such as
`lower(status) LIKE '%open!_%' ESCAPE '!'` lowers to the same typed expression
condition with normalized backslash escapes. `NOT LIKE` / `NOT ILIKE` lower to
that boolean expression compared with `false`, so full `%` and `_` semantics
are shared by SQL, REST/SDK plans, and storage execution.
`row_claim` is an internal/coordinator field on the shared row-query struct,
not a free-form public query knob. Its REST/SDK schema is typed as
`RowsRowClaim` so internal/coordinator callers get the same validation contract
as the Zig parser. Public row-plan endpoints reject `row_claim`; lockable
public access goes through `/tables/{table}/rows/mutation-source`, where the
source must carry a transaction-bound `row_claim`.
`doc_key_range` is a typed physical-span field for direct owner-scoped base-row
streams. Row queries, aggregate sources, window sources, join/lateral side
sources, mutation-source streams, and insert-source streams may carry one
start-inclusive/end-exclusive span when they read a base table directly. They
reject `doc_key_range` when the source is a materialized `source_cte`, when a
coordinator has already supplied top-level routed ranges, or when the API
receives pre-materialized source rows that no longer carry physical row
identity. Catalog-routed mutation-source execution injects owner ranges through
internal collect calls and rejects any request that already carries a physical
span, so a user-authored local span cannot be silently mixed with owner-routing
spans.
Claimed row queries require a transaction-bound `for_update`,
`for_no_key_update`, `for_share`, or `for_key_share` claim and reject
`DISTINCT ON`, because claims must bind to lockable base-row candidates rather
than a derived de-duplicated stream.
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
    "where": { "field": "status", "op": "eq", "value": "open" }
  },
  "group_by": ["customer_id"],
  "group_expressions": [
    { "as": "status_key", "expr": { "op": "lower", "args": [{ "field": "status" }] } }
  ],
  "aggregations": [
    { "name": "order_count", "op": "count" },
    {
      "name": "amount_sum",
      "op": "sum",
      "field": "amount",
      "filter_any": [
        { "all": [{ "lhs": { "field": "status" }, "op": "eq", "rhs": { "value": "open" } }] },
        { "all": [{ "lhs": { "field": "amount" }, "op": "gt", "rhs": { "value": 100 } }] }
      ],
      "filter_not": [
        { "all": [{ "lhs": { "field": "status" }, "op": "eq", "rhs": { "value": "blocked" } }] }
      ]
    },
    { "name": "amount_avg", "op": "avg", "field": "amount" },
    { "name": "amount_min", "op": "min", "field": "amount" },
    { "name": "amount_max", "op": "max", "field": "amount" }
  ],
  "having": {
    "all": [{ "field": "amount_sum", "op": "gt", "value": 100 }]
  },
  "having_expressions": [
    {
      "lhs": { "op": "sub", "args": [{ "field": "amount_sum" }, { "field": "order_count" }] },
      "op": "gt",
      "rhs": { "value": 100 }
    }
  ],
  "having_any": [
    { "all": [{ "lhs": { "field": "amount_sum" }, "op": "gt", "rhs": { "value": 100 } }] },
    { "all": [{ "lhs": { "field": "order_count" }, "op": "gt", "rhs": { "value": 10 } }] }
  ],
  "having_not": [
    { "all": [{ "lhs": { "field": "amount_sum" }, "op": "lt", "rhs": { "value": 0 } }] }
  ],
  "order_by": [{ "field": "amount_sum", "direction": "desc" }],
  "limit": 100,
  "offset": 0
}
```

The source query is evaluated through the same index-backed candidate selection
and authoritative base-row recheck path as ordinary row queries. Grouping and
metric folding then happen over the materialized row stream and emit typed JSON
projection rows such as
`{ "customer_id": "c1", "status_key": "open", "order_count": 2, "amount_sum": 30 }`. `count` without
a field is the all-row count; SQL `COUNT(*)` and non-null numeric literal forms
such as `COUNT(1)` both lower to this same native aggregate node. `count` with a
field ignores null/missing values and can count any declared scalar, JSON, or
array field. Bounded `array_agg` accepts declared scalar, JSON, or array fields,
typed expression inputs, per-aggregate `DISTINCT`, and optional aggregate-local
ordering, including JSON extraction such as
`metadata->'flags'`, and always requires an explicit or default materialized item
cap. Bounded `string_agg` accepts declared text-like scalar fields or typed text
expressions, requires a literal delimiter, skips null/missing input values,
applies the same per-aggregate `DISTINCT` and aggregate-local ordering contract,
and emits `null` when a group has no non-null text input. Numeric metrics ignore
null/missing values and reject non-numeric inputs for the metric field or
expression. Native aggregate specs are closed typed objects:
`name` and `op` are required, `name` and `field` values are non-empty strings,
`op` is one of `count`, `sum`, `min`, `max`, `avg`, `percentile_cont`,
`percentile_disc`, `array_agg`, `string_agg`, `bool_or`, or `bool_and`, `field` and
`expr` are mutually exclusive, non-`count` metrics require exactly one input,
`distinct_max_items` is legal only when `distinct` is true,
`percentile`/`percentile_max_items`/`percentile_order` are legal only for
`percentile_cont` and `percentile_disc`; omitted percentile direction defaults
to ascending, and
`array_max_items`/`array_order_by` are legal only for bounded collection
aggregates (`array_agg` and `string_agg`), while `delimiter` is legal and
required only for `string_agg`. Boolean folds ignore missing or JSON `null`
inputs, return `null` when no non-null boolean input exists for the group, and
otherwise return the PostgreSQL-shaped `bool_or`/`bool_and` result.
Per-aggregate `filter` predicates,
`filter_expressions`,
`filter_any`, and `filter_not` are evaluated against each source input row
before that row updates the metric. This gives REST/SDK plans and SQL
`FILTER (WHERE ...)` lowering the same typed shape for conjunctions,
top-level OR, and direct NOT groups without carrying SQL text into storage. A
grouped request with an empty or omitted `aggregations` list is a
first-class group-only projection: it emits one row per distinct `group_by` and
`group_expressions` key and no metric fields. `group_expressions` are named
typed expression projections evaluated for each source row, included in the
native aggregate grouping key, emitted under their `as` names, and available to
aggregate `ORDER BY` and `HAVING` just like field group keys. The SQL adapter
lowers plain `SELECT DISTINCT field[, ...] FROM ...` and expression distinct
forms such as `SELECT DISTINCT lower(status) AS status_key FROM ...` to that
group-only aggregate shape. Ordinary aggregate grouping forms such as
`SELECT lower(status) AS status_key, COUNT(*) FROM ... GROUP BY lower(status)`
lower to the same native `group_expressions` contract. Grouped or global
aggregate statements that include `SELECT DISTINCT`, such as
`SELECT DISTINCT customer, COUNT(*) ... GROUP BY customer` and
`SELECT DISTINCT COUNT(*) ...`, lower to the ordinary aggregate plan when the
aggregate output already has at most one row per group or one global result row;
the adapter treats that DISTINCT as SQL sugar rather than adding SQL text or a
second deduplication stage. `SELECT DISTINCT ON (expr[, ...]) ... ORDER BY
expr[, ...]` lowers to row-query `distinct_on_expressions`, with field-only
`distinct_on` retained as shorthand for direct REST/SDK callers. The stage keeps
the first ordered row for each declared typed key before pagination.
Metricless global aggregate requests remain invalid.
Aggregate `order_by` sorts over the emitted group/metric rows before
`offset` and `limit`, so SQL `ORDER BY amount_sum DESC LIMIT ...` lowers without
re-reading base rows. PostgreSQL-style aggregate `GROUP BY` ordinals such as
`GROUP BY 1` and output-order ordinals such as `ORDER BY 3 DESC` are resolved at
the adapter boundary to named emitted group/metric fields before the typed plan
reaches execution; ordinals that target aggregate outputs in `GROUP BY` fail
closed. Aggregate `ORDER BY` and `HAVING` field references, including fields
derived from SQL output ordinals, must name exactly one emitted group or metric
field. Duplicate emitted group, group-expression, or metric names fail during
SQL lowering before a typed aggregate plan is accepted, even when a later clause
uses an ordinal that could otherwise disambiguate the reference.
Direct aggregate-call ordering such as `ORDER BY SUM(amount) DESC`
uses the same binding rule as `HAVING`: the call must structurally match exactly
one selected metric specification, and the stored typed plan orders by the first
matching emitted metric field. Repeated equivalent selected metrics therefore
bind to the first selected output because they compute the same value; duplicate
output names, non-equivalent matches, and unselected aggregate calls still fail
closed before a typed aggregate plan is accepted. Aggregate `having`
is a typed conjunction over emitted
group/metric fields and is encoded as `{ "all": [...] }`. The `all` list must
contain at least one predicate, each predicate must name a non-empty emitted
field, null-test operators (`is_null`, `is_not_null`) omit `value`, and all
other scalar comparison operators preserve an explicit JSON `null` value as a
real comparison operand. Each predicate uses the same scalar comparison
operators as row predicates (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `is_null`,
`is_not_null`, `is_distinct`, and `is_not_distinct`). SQL boolean predicates
over emitted boolean group fields bind here too: `HAVING enabled IS TRUE` and
`HAVING enabled IS FALSE` become scalar equality checks, `HAVING enabled IS
UNKNOWN` and `HAVING enabled IS NOT UNKNOWN` become null checks, and
null-inclusive `HAVING enabled IS NOT TRUE` / `IS NOT FALSE` lower to
`having_any` predicate groups over the emitted aggregate row. `having_expressions` is a typed conjunctive
expression-condition list over the same emitted aggregate row. `having_any` is
a disjunction of expression-condition groups, and `having_not` rejects rows that
match any listed expression-condition group. REST/SDK callers and SQL lowering
can represent computed predicates such as `amount_sum - order_count > 100`,
OR-shaped predicates, and negated predicates without re-reading base rows or
carrying SQL text past the adapter. SQL adapter lowering for ordinary
`HAVING a AND b` targets the conjunctive `having`/`having_expressions` shape;
`HAVING a OR b` lowers to `having_any`; and `HAVING NOT (...)` lowers to
`having_not`. Bare boolean aggregate-output expressions such as `HAVING
any_enabled`, `HAVING any_enabled AND NOT all_openish`, and `HAVING any_enabled
OR all_openish` lower to a single `having_expressions` condition over the same
`and`/`or`/`not` row-expression AST used by row predicates and projections. Direct
aggregate-call predicates such as
`HAVING COUNT(*) > 0` and `HAVING SUM(amount) >= 10` resolve to the selected
metric output when the call structurally matches a selected aggregate spec;
repeated equivalent selected specs resolve to the first selected metric output,
while unselected or non-equivalent ambiguous aggregate calls fail closed.
Computed SQL `HAVING`
predicates over selected group/metric aliases lower to the same aggregate-output
expression condition groups. Boolean expressions that mix comparison predicates
and boolean-valued outputs still use the explicit `having`/`having_any`/`having_not`
group forms unless they can be represented as one typed boolean expression
without changing predicate semantics.
This gives the SQL adapter a direct lowering target for single-table `GROUP BY`
before join streams are attached.

Joins compose two typed row-query sources with explicit equality predicates and
projection fields:

```json
{
  "left": {
    "where": { "field": "kind", "op": "eq", "value": "order" },
    "order_by": [{ "field": "id", "direction": "asc" }]
  },
  "right": {
    "where": { "field": "kind", "op": "eq", "value": "customer" }
  },
  "on": [
    { "left_field": "tenant_id", "right_field": "tenant_id" },
    { "left_field": "customer_id", "right_field": "id" }
  ],
  "join_type": "left",
  "select": [
    { "as": "order_id", "side": "left", "field": "id" },
    { "as": "customer_name", "side": "right", "field": "name" }
  ],
  "order_by": [{ "field": "customer_name", "direction": "asc" }],
  "limit": 100,
  "offset": 0
}
```

Join and lateral contract objects are exact public objects. `on` and
`correlations` arrays must contain at least one key pair, key fields and
projection aliases are non-empty declared fields, and projection `side` is
strictly `left` or `right`.

Each side is evaluated as a normal row query, so side-local filters use the
same indexed candidate selection and base-row recheck path. The join executor
then builds a hash table from the right stream using the typed `on` columns and
probes it with the left stream. Join keys are JSON tuples; null or missing join
components do not match, which matches SQL equality-join behavior. `inner` joins
emit only matches. `left` joins emit unmatched left rows with right-side
projection fields set to `null`. Joined result `order_by` sorts over projected
joined rows, then `limit`/`offset` applies after join matching and projection.
Request-level `match_expression_*` predicates evaluate after a candidate right
row is found and before projection. They bind unmarked fields to the left row
and `{"source":"source"}` fields to the right row; a left join with any
request-level residual emits no synthetic null-right row when no candidate
passes the residual.

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
`queryRelationalRowsAcrossRanges` and `TableDocKeyRangePlan`. Aggregate and
window reads can use the same routed row-source contract through
`aggregateRelationalRowsAcrossRanges` and `windowRelationalRowsAcrossRanges`: the
coordinator gathers the declared non-overlapping table ranges, rewrites the
source stream into the ordering/projection required by the downstream typed
stage, and feeds the resulting row stream into the same aggregate or window
reducer used by local and CTE-backed plans. Equality joins can do the same
through `joinRelationalRowsAcrossRanges`, which gathers declared left/right
range sets and feeds the materialized streams into the same join reducer used by
local and CTE-backed joins. Bounded lateral joins use
`lateralRelationalRowsAcrossRanges` for the same declared-range model: the left
stream is gathered once, and each correlated right-side query executes with the
declared right range set plus the typed correlation predicates and right-side
`ORDER BY`/`LIMIT`. Lateral request-level `match_expression_*` predicates use
the same left-row plus `source` right-row binding as joins, run after each
bounded right-side result is collected, and suppress null-extension when the
residual is present. Cross-table join planning is a coordinator composition
concern: it resolves each table source through durable table/range ownership,
executes the existing typed row-query contract on each owner, and supplies those
materialized streams to the same join/aggregate/window/lateral executors.
The public REST/SDK typed plan envelopes expose the same declared-range model:
query, aggregate, and window plans can carry `ranges`; equality join and lateral
plans can carry paired `left_ranges` and `right_ranges`. Ordered CTEs use the
same declared range scope as the enclosing plan: query, aggregate, and window
CTEs materialize from the plan `ranges`, while join and lateral CTEs materialize
from a sorted, merged union of declared left/right ranges so overlapping
same-table side ranges do not duplicate CTE rows or fail a valid routed
join/lateral shape. Empty ranges mean the full table, resolved through catalog
ownership. Each declared `RowsDocKeyRange` is an exact object with only `start`
and `end` string bounds; at least one bound must be present, bounded ranges must
have `start < end`, and unknown keys fail before planning. Non-empty declared
range sets are ordered and non-overlapping; reversed, unsorted, overlapping, or
open-ended ranges followed by another range fail before catalog fanout so local
and provisioned execution share the same range contract.
Provisioned and hosted row-plan execution now resolves declared ranges, or the
full table when no range is declared, through durable table/range ownership
metadata. Each owner group evaluates the typed source predicates inside its
start-inclusive/end-exclusive row-key span and returns full row JSON to the
coordinator. Hosted remote owners expose an internal typed row-source route
(`rows/source`) that carries the original typed row query, the resolved
doc-key range, the catalog topology epoch for that range, and an optional
system-time selector. The owner validates the epoch and rewrites the query into
an owner-local source-row scan before reading, so stale routing fails closed
with `TopologyChanged` instead of serving from an old ownership view. For
hosted `FOR SYSTEM_TIME AS OF` reads, commit-sequence and timestamp selectors
resolve declared routed ranges, or the full table when no range is declared,
through that route, collect range-clipped historical base rows, and run the
original ranged query/CTE reducer once at the requester. Bound and single-owner
provisioned AS-OF row-query plans use the same historical snapshot reducer for
declared ranges instead of rejecting the plan.
Pruned historical reads fail closed as `SystemVersionedHistoryPruned` (HTTP 410
on the internal route). The coordinator
then runs the same reducers used for local and materialized-CTE plans: row
queries apply global projection, distinct, ordering, offset, limit, and
total-count semantics once; aggregate and window plans reduce one routed source
stream; equality joins gather left/right source streams separately and then
apply the same join reducer. Provisioned and hosted lateral execution gathers
right-side candidates per left row with the correlation predicates, declared
right ranges, right-side ordering, offset, and limit already applied, dedupes
that bounded candidate union, and then feeds it into the typed lateral reducer
instead of materializing an unbounded global right stream first. Hosted CTE
execution materializes ordered CTEs at the coordinator from the same routed
row-source gathers, enforces declared row/byte caps before a CTE becomes visible
to later stages, rejects row claims and physical doc-key ranges inside CTE
definitions, and then feeds CTE-backed row, aggregate, window, join, and lateral
stages through the same reducers as local execution. If a CTE omits explicit
caps, execution applies the typed-plan defaults of 65,536 rows and 64 MiB of
serialized materialized stream size, counted as the JSON array framing plus
comma separators plus row JSON payloads, so every materialized CTE has a
fail-closed resource bound.
Hosted plan execution performs coordinator-side source-row reduction through
the static typed row evaluators for row queries, CTE-backed queries, aggregate
inputs, windows, joins, and lateral stages. That means a hosted read plan can be
coordinated on a caller node even when every resolved owner range is remote; the
owner groups still own predicate evaluation over their row-key spans, while the
coordinator owns global ordering, pagination, distinct handling, CTE caps, and
final stream reduction.
Non-recursive CTEs are first-class query composition: a
`RelationalRowsQueryPlan`, `RelationalRowsAggregatePlan`,
`RelationalRowsJoinPlan`, `RelationalRowsWindowPlan`, or
`RelationalRowsLateralPlan` carries ordered named CTEs, each CTE is a typed
`RelationalRowsQueryRequest`, and later CTEs, aggregate/window sources,
join/lateral sides, or the final row query can name a materialized source with
`source_cte`. A CTE definition can use the normal base row-query planner,
including declared indexes, before materialization; predicates over a
materialized CTE are evaluated against that materialized row stream. Each CTE
can carry `max_rows` and `max_bytes`, explicit materialization caps exposed
through the native REST/SDK plan shape; omitted caps use the default row/byte
limits above. `max_bytes` is measured against the serialized materialized stream
including JSON array framing and row separators, not only the raw sum of row
payloads. Execution fails closed when a CTE produces more rows or serialized
bytes than its effective limits instead of silently building an unbounded
intermediate stream. Native REST/SDK plan parsers use the same ordered typed
contract and reject forward
references, duplicate CTE names, missing final-stage `source_cte` references
from query, aggregate, window, join, and lateral plans, durable row claims, and
physical doc-key ranges inside CTE definitions. Durable row claims and physical
doc-key ranges are base-row planner features and are rejected over materialized
CTE sources. CTE consumers validate row-query, aggregate, window, join, and
lateral field references against the CTE's emitted fields. Aggregate, join,
lateral, and window stages also validate their own result-level filters and
ordering against emitted output names. The remaining production refinements are
collation-aware index semantics beyond the shared comparison helper and a
spill/backpressure policy for
materializations that intentionally exceed the in-memory fail-closed caps.
The PostgreSQL adapter exposes this through `lowerQueryPlanAlloc`: non-recursive
`WITH name AS (SELECT ...) ... SELECT ... FROM name` lowers to ordered CTE
queries plus a final `source_cte` query, while plain `lowerSelectAlloc` remains
the single-select API and rejects `WITH`.
PostgreSQL `AS MATERIALIZED` and `AS NOT MATERIALIZED` CTE hints are accepted
as adapter-only syntax over the same bounded native CTE materialization
contract; they do not introduce SQL text or planner-private behavior into
storage.
Recursive CTEs are a separate graph/fixpoint feature and should be treated as a
distinct planner extension.

Those model-level items are not PostgreSQL-specific. They are the durable
relational semantics that the storage model should expose so a future SQL
dialect, REST API, or client SDK can compile to the same operations.

#### PostgreSQL adapter roadmap

PostgreSQL-shaped SQL may be adapter input. The storage model above now has many
of the durable primitives those queries need, but SQL adapter parity is not
a license to pass SQL text through the backend. The production boundary is a
Postgres-facing adapter that parses SQL into a typed frontend AST, resolves it
against Antfly catalog metadata, and lowers only supported shapes into explicit
Antfly row, mutation, query, aggregate, join, CTE, window, and expression plans.
Unsupported syntax should fail closed with a structured unsupported-shape error.

Because this feature set is new, the typed Antfly plan is the source of truth,
and Postgres syntax is a front-end dialect that compiles into that plan.

Implementation status is tracked by release-gated SQL and relational
inventories, not by a second checklist document. The durable rule remains: SQL
text is parsed once by the Postgres-facing adapter, bound against catalog
metadata, and lowered into typed Antfly plans. Those typed plans are the
internal contract used by REST, SDK, SQL, migrations, repair, rebuild, and
simulation tests.

The model-owned layers are adapter-only PostgreSQL surface, typed
schema/catalog changes, row-batch and mutation-source writes, shared
scalar/JSON/array expressions, lockable row streams, routed stream composition,
and parity evidence. Keep this document focused on those contracts. Current
per-feature status and remaining gaps live in the release-gated SQL and
relational fixture inventories, which are checked by `relational-release-gate`
and the SQL parity fixture gates.
Adapter-only transaction-control coverage includes `BEGIN`, `BEGIN WORK`, bare
`START TRANSACTION`, `COMMIT`, `COMMIT TRANSACTION`, `ROLLBACK`, and `ROLLBACK
WORK` as explicit no-op classifications, so migration wrappers and client
protocol boundary statements stay out of storage while still remaining visible in
the golden corpus. The boundary tail matcher lives in
`sql/grammar.zig`, including statement-end validation plus `WORK`
and `TRANSACTION` aliases. `SAVEPOINT`, `RELEASE [SAVEPOINT]`, and `ROLLBACK TO
[SAVEPOINT]` syntax is also parsed in `sql/grammar.zig` and lowers
to typed savepoint transaction-control intents that capture the savepoint name
and fail closed at schema/storage application until native nested transaction
rollback/release semantics exist. Prepared transaction commands are not boundary
no-ops: `PREPARE TRANSACTION`, `COMMIT PREPARED`, and `ROLLBACK PREPARED` lower
to typed prepared-transaction DDL plans and a generated-corpus `execution_plan`
fingerprint for the coordinator-owned recovery action
(`register_prepared`, `resolve_commit`, or `resolve_rollback`).
`sql.executePreparedTransactionRecoveryPlan` is the explicit
execution boundary for those intents: it maps the SQL GID to a domain-separated
deterministic Antfly transaction id, registers prepared transactions in the
transaction coordinator store, resolves `COMMIT PREPARED` / `ROLLBACK PREPARED`
through the same durable intent-resolution path used by native transactions,
keeps repeat same-decision recovery idempotent for crash/retry safety, and fails
closed for duplicate prepared GIDs, missing GIDs, and conflicting terminal
decisions. HTTP SQL DDL execution first routes prepared-transaction plans to a
source-provided coordinator hook; if that hook is absent, a server configured
with `session_store_path` reuses the durable opened-session `DocStore` and
transaction coordinator as the recovery store. A server without either
coordinator path still fails closed rather than treating `PREPARE TRANSACTION`
as schema/table mutation. Applying those plans to schema JSON or runtime table
storage still fails closed because prepared transactions are coordinator
actions, but the adapter boundary no longer treats the syntax as an opaque
unsupported string.

Adapter-only session cleanup covers a narrow allowlist of PostgreSQL
client/dump boilerplate as explicit `session_setting` classifications:
exact-value `SET [LOCAL|SESSION]` forms for inert client presentation settings
and `RESET` for the same inert setting allowlist. Catalog-affecting
`SET search_path`, `SET LOCAL search_path`, `RESET search_path`,
`SHOW search_path`, and `DISCARD ALL` now lower to typed session catalog plans
instead of adapter no-ops. Applying those plans mutates an explicit SQL catalog
session used by table/schema resolution. `SET [SESSION] search_path` preserves
the ordered namespace list in the typed plan; `SET LOCAL search_path` preserves
the same ordered namespace list plus transaction-local intent on the owned
session state, including the prior base path so `COMMIT` and `ROLLBACK` can
clear the local override without losing the session path. Typed `SET LOCAL`
runtime/app settings preserve the same transaction-local base-state contract,
so transaction boundaries restore the prior setting map rather than leaking
local overrides. `SET [LOCAL|SESSION] antfly.sync_level = '<level>'` is a
typed Antfly runtime setting over the public write sync-level enum; write
lowering reads it once at statement execution and maps it to the native
`RowsBatchRequest.sync_level` for every generated relational row batch, while an
unset SQL session defaults to `write`. The source parity corpus pins
public-only, multi-namespace, and transaction-local search paths so
schema-resolution metadata cannot collapse back into an adapter-only no-op.
Role/session
authorization changes, arbitrary settings, timeout settings, default storage
settings, unsupported values for otherwise inert settings, `SHOW ALL`, and
partial `DISCARD` variants still fail closed. The allowlist lives in
`sql/grammar.zig` so new accepted session syntax must be explicit
adapter grammar, not another raw token scan in the SQL lowerer. If Antfly later
owns long-lived server-side prepared
statements, cursors, temporary objects, or session-local variables, `DISCARD`
must expand from catalog-session reset into a typed cleanup request over those
native objects.

Prepared statement, cursor, and explain syntax is protocol/query-control
surface over typed plans rather than storage syntax. `PREPARE`, `EXECUTE`, and
`DEALLOCATE [PREPARE] name|ALL` cleanup tails are parsed in
`sql/grammar.zig`; the resulting typed prepared-statement syntax
captures statement name, parameter or argument count, the broad subject
(`read`, `write`, or `ddl`), and the final native statement family (`read`,
`insert`, `insert_source`, `update`, `delete`, `truncate`, `merge`, or `ddl`)
before the lowerer allocates the public intent plan. A `PREPARE ... AS WITH ...`
subject uses the same final-statement CTE classifier as ordinary SQL dispatch,
so CTE-backed `UPDATE`, `DELETE`, `INSERT`, `TRUNCATE`, and `MERGE` subjects
are writes while recursive CTE subjects fail closed. The parity corpus requires
direct prepared `SELECT`, `INSERT`, `TRUNCATE`, DDL, and CTE-backed
insert-source/update/delete/merge families to keep that metadata durable. They
now route through `ApiHttpServer` into a session-scoped prepared-statement
runtime: `PREPARE` records the statement name, parameter count, subject kind,
and statement family for the SQL protocol session; duplicate names fail closed;
`EXECUTE` verifies the prepared statement exists in the same session and that
the argument count matches; `DEALLOCATE [PREPARE] name|ALL` removes registered
session entries; and `DISCARD ALL` clears the same runtime state while resetting
catalog-session defaults. The runtime intentionally stores typed metadata, not
raw SQL text. Executing cached statement bodies still fails closed until Antfly
has a native prepared-plan cache keyed by typed plan fingerprints, parameter
schemas, catalog epochs, authorization context, and invalidation rules.

The HTTP SQL ingress for that protocol state should be `POST /db/v1/sql`, not
`/sql/v1` or `/db/v1/sql/statements`. SQL is one database-facing API surface and
should version with the rest of `/db/v1`. The endpoint is synchronous and
session-shaped: clients submit one SQL statement plus an optional logical
`session_id`, and the server returns the session id to reuse for later
psql-style requests. The first implementation routes catalog, session,
prepared-statement, notification, routine, and extension DDL/control statements
through the existing typed SQL execution path. SQL reads now join that endpoint
by parsing SQL through the adapter grammar, resolving catalog/schema state from
the logical SQL session, lowering into the shared typed row-plan executor used
by the public JSON endpoints, and returning the same
row-query/aggregate/window/join/lateral response envelopes under a SQL session
response. Point `INSERT ... VALUES`, primary-key `UPDATE`, and primary-key
`DELETE` now join that endpoint by lowering through the same DML adapter into
typed row-batch mutations, applying row-filter checks, committing through the
same row-batch transaction path, and returning the normal row-batch counts plus
`RETURNING` rows under a SQL session response. Non-recursive
`INSERT ... SELECT` joins the endpoint through the typed insert-source path: the
source side executes as a typed row-read plan with CTE and row-filter handling,
then the insert assignments, conflict policy, target row filters, transaction
commit, and `RETURNING` projection use the same row-batch mutation machinery.
Safe `BEFORE INSERT` SQL trigger hooks are applied before SQL row-batch commit,
and SQL update/delete statements fail closed while table update/delete trigger
execution still requires the shared preimage trigger path.
Claimed single-table update/delete sources now join the endpoint through the
typed mutation-source path: SQL lowering creates the same typed source query and
mutation request used by the JSON endpoint, the HTTP executor mints a SQL-owned
row-claim transaction, table write sources stage through the ordinary
owner-local mutation-source planner, and the SQL path autocommits or aborts the
transaction before returning mutation-source `matched`/`staged` counts and
`RETURNING` rows. Non-recursive joined update/delete sources (`UPDATE ... FROM`
and `DELETE ... USING`) also join the endpoint through typed side-table reads and
the native joined mutation-source planner/stager, with the SQL executor
autocommitting or aborting the row-claim transaction around the staged joined
mutation. Single-table `TRUNCATE`, multi-table truncate, and `CASCADE` join the
endpoint through the catalog-owned table-emptying barrier path; `RESTART
IDENTITY` carries allocator-reset intent on that same barrier and remains gated
on native identity-allocator reset work before table-generation promotion.
Recursive write sources and merge writes should remain fail-closed until their
SQL executor wiring can use the native recursive materialization, trigger,
catalog barrier, and 2PC paths already used by native APIs. Prepared
statements and cursors remain session state rather than REST resources;
cursor-backed fetches and asynchronous `202` statement jobs are later protocol
extensions once portal lifetime, page tokens, cancellation, result retention,
and cleanup semantics are native.
While the endpoint supports typed catalog/session/control statements, typed
reads, and point row-batch writes, it should require database-admin permission
on the default database when authentication is enabled. SQL reads and writes
should relax to statement-level table permissions only after the shared executor
derives required permissions from the lowered typed plan before execution.

`DECLARE ... [BINARY]
[NO] SCROLL CURSOR [WITH|WITHOUT HOLD]`, `FETCH [direction] [FROM|IN]
cursor`, shorthand `FETCH cursor`, bare-count `FETCH n cursor`, and `CLOSE
cursor|ALL` lower to typed cursor-portal intents that capture portal name,
scroll/hold/binary metadata, fetch direction/count, and close scope. Fetch
directions are explicit metadata for `NEXT`, `PRIOR`, `FIRST`, `LAST`,
`ABSOLUTE n`, `RELATIVE n`, `FORWARD [n|ALL]`, `BACKWARD [n]`, and `ALL`. The
`CLOSE` cleanup tail also lives in `sql/grammar.zig`, so protocol
cleanup syntax stays adapter-owned while the lowerer allocates the typed plan.
They still fail closed when applied to storage until there is a typed portal
contract with snapshot lifetime, range ownership, ordering, backpressure, resume
tokens, and transaction cleanup. Plain
`EXPLAIN <statement>` lowers to a native explain wrapper over the same typed
read-plan and write-plan lowering plus deterministic fingerprints used by the
golden parity gate, so diagnostic output is anchored to the real planned work
instead of adapter text. Read statements explain as read subjects; supported
`INSERT`, `UPDATE`, `DELETE`, joined mutation-source, insert-source, and
table-emptying write shapes explain as write subjects without staging intents or
touching storage. Non-executing PostgreSQL options that only shape diagnostic
metadata, including `FORMAT TEXT`, `FORMAT JSON`, `VERBOSE`, `COSTS`, and
`ANALYZE`, plus runtime-reporting option flags `BUFFERS`, `TIMING`, `SUMMARY`,
`SETTINGS`, and `WAL`, lower into that wrapper as typed explain-plan fields.
Unsupported explain options fail closed rather than being carried as raw SQL.
Analyzed explain plans are diagnostic intent at the adapter boundary:
execution-time runtime counters, row counts, range-routing traces, and cost
metadata still need native REST/SDK diagnostic result structs before `ANALYZE`
can report executed work instead of just the typed plan it would execute.

Maintenance and transaction-mode SQL also stays out of storage until it maps to
native typed work. Table-targeted `VACUUM`, `ANALYZE`, `REINDEX`, and `CLUSTER`
syntax parses in `sql/grammar.zig` before `sql/lower_ddl.zig` allocates typed
maintenance-job intents that capture target table, index, or catalog scope and
supported options, then fail closed when applied to table
schema or runtime storage. The production shape is a durable maintenance-job model with
table/index generations, range ownership, leases, throttling, resumable
progress, repair/rebuild integration, stats output, and catalog promotion
semantics. `LOCK TABLE`, `SET CONSTRAINTS`, `SET TRANSACTION`, `START
TRANSACTION ... ISOLATION LEVEL ...`, and `BEGIN ... ISOLATION LEVEL ...`
clauses parse in `sql/grammar.zig` before the lowerer allocates
typed transaction-control intents that capture normalized table and constraint
names, table-lock mode, constraint scope/check mode, transaction starter,
isolation level, read-only/read-write mode, and deferrability. `pg_advisory_lock`
and `pg_advisory_unlock` call tails parse in `sql/grammar.zig` and
lower to typed transaction-control intents with advisory-lock action and
advisory key values. All of these fail closed when
applied to table schema or runtime storage. The production shape is
a native table/range/advisory lock barrier that composes with row claims,
catalog epochs, range movement, and 2PC, plus transaction-scoped deferrable
constraint state and request-level isolation/access/retry options. Plain
`BEGIN`, `BEGIN WORK`, bare `START TRANSACTION`, `COMMIT`, `COMMIT
TRANSACTION`, `ROLLBACK`, and `ROLLBACK WORK` lower to typed adapter no-op
records with the stable `transaction_control` reason. Mode-bearing transaction
starts remain typed transaction-control plans. Prepared transaction
prepare/commit/rollback lower to typed recovery intents and execute through the
coordinator recovery service when called with a transaction store. Schema and
table-storage application still reject them because the authoritative side
effect is coordinator recovery state, not table schema.

Notification-channel SQL is also adapter grammar over typed native intent, not
backend SQL text. `LISTEN`, `NOTIFY`, and `UNLISTEN` tails parse in
`sql/grammar.zig`; `NOTIFY` payload literals use the shared
`sql/value.zig` untyped literal-to-JSON helper; and `sql/lower_ddl.zig` maps the
resulting channel name, optional payload JSON, or `UNLISTEN *` flag into typed
notification-channel plans. `ApiHttpServer`
executes those plans through `sql/notifications.zig`, an Antfly-owned
notification runtime that assigns stable SQL notification session ids to owned
catalog sessions, records idempotent `LISTEN` subscriptions, applies
`UNLISTEN`/`UNLISTEN *`, and appends ordered `NOTIFY` delivery events with
payload JSON and delivered session ids. Table-schema and table-storage
application still reject notification-channel DDL because the authoritative
side effect is event-channel runtime state, not schema JSON. Remaining
production hardening is durable cross-node fan-out, transaction commit ordering,
wakeup delivery APIs, backpressure, retry, and authorization policy around those
native channel events.

Schema namespace syntax is only adapter-only when it is proven boilerplate for
the default `public` namespace, such as `CREATE SCHEMA IF NOT EXISTS public`.
Non-public `CREATE SCHEMA`, `ALTER SCHEMA ... RENAME TO`, and `DROP SCHEMA`
tails parse in `sql/grammar.zig` and lower to typed
schema-namespace catalog intents that record the namespace name, idempotence
flag, rename target, and cascade/drop metadata. Metadata now has
first-class database and namespace records, table records carry
`database_name` and `namespace_name`, default table APIs resolve through
`default/public/<table>`, and non-default table identity derives separate table
and range ids instead of flattening namespaces into table-name strings. Basic
schema-namespace DDL application creates, renames, and drops default-database
namespace records through typed catalog operations, rewrites table namespace
identity on namespace rename, rejects non-empty restrict drops, and fails closed
for cascade drops until dependent-object cleanup is durable. Richer object-name
resolution rules, dependency tracking, authorization boundaries, and catalog
promotion behavior remain production work. `public.` qualification can keep
lowering away at the adapter boundary, but non-public namespaces must not be
flattened into table-name strings. The durable
product/catalog model is described in `DATABASES.md`: Antfly uses
`database / namespace / table`, with existing `/tables/{table}` APIs resolving
to the current/default database, the `public` namespace, and the requested
table.

Database and tablespace lifecycle DDL is outside the table-plan contract but
still lowers to typed catalog intent. Basic `CREATE DATABASE`,
`ALTER DATABASE ... SET`, and `DROP DATABASE` statements produce database
catalog plans so API parity can track database identity and settings without
flattening them into table names or schema JSON. Basic `CREATE TABLESPACE ...
LOCATION`, `ALTER TABLESPACE ... RENAME TO`, and `DROP TABLESPACE` statements
produce tablespace catalog plans so placement/storage-class intent is explicit
but cannot silently change table storage paths. These database/tablespace SQL
tails parse in `sql/grammar.zig`; `sql/lower_ddl.zig` maps the syntax into
typed catalog plans and owns only plan allocation/ownership transfer.
Database catalog DDL now applies
basic create, alter-settings, and empty-drop operations through durable typed
database and namespace records, including automatic `public` namespace creation
for a new database. Tablespace catalog DDL now applies durable lifecycle
metadata, rename propagation, dependency-checked drops, and supported
placement-policy projection into native table placement fields. Remaining
database/tablespace production hardening is native SQL connection state,
additional storage-class policy consumers, backup/restore scope, authorization
parity across every adapter, and catalog promotion around those explicit typed
objects. PostgreSQL syntax can later become an administrative adapter for those
objects, but SQL database or tablespace names should not silently change table
storage paths or routing behavior.

PostgreSQL logical replication and notification commands are not table DDL and
are not adapter-only no-ops. Basic `CREATE PUBLICATION`, `ALTER PUBLICATION ...
ADD TABLE`, `DROP PUBLICATION`, `CREATE SUBSCRIPTION`, `ALTER SUBSCRIPTION
ENABLE/DISABLE`, and `DROP SUBSCRIPTION` lower to typed logical-replication
catalog intents that capture publication names, selected tables, connection
strings, publication subscriptions, and enablement state, then fail closed when
applied to table schema or runtime storage. Production execution still requires
native changefeed or replication catalog objects with table/partition selection,
schema-version binding, durable resume positions, authorization, backfill
policy, delivery acknowledgements, and topology-aware range ownership.
Publication/subscription tails parse in `sql/grammar.zig` before
the SQL lowerer maps owned syntax into typed logical-replication catalog plans,
so future accepted options must become explicit native fields rather than raw
SQL carried through storage.
`LISTEN`, `NOTIFY`, and `UNLISTEN` lower to typed notification-channel intents
that capture channel names and optional payloads. The HTTP SQL execution path
now applies them through the native SQL notification runtime, while table schema
and runtime storage still fail closed because notification state is not a table
mutation. The remaining event-channel contract work is cross-node durability,
transaction commit ordering, payload schema evolution, subscriber lifetime,
backpressure, retry, and authorization. PostgreSQL syntax can lower into those
native contracts, but it must not pretend that transient SQL sessions are a
durable storage or routing primitive.

Basic PostgreSQL custom type-system object syntax lowers to typed catalog
intents and then fails closed when applied to table schema or runtime storage.
`CREATE COLLATION`, `ALTER COLLATION ... RENAME TO`, `DROP COLLATION`,
`CREATE/DROP OPERATOR`, `CREATE/DROP AGGREGATE`, and `CREATE/DROP CAST` capture
object names, argument counts, option counts, cast endpoints, cast functions,
and assignment mode. These objects change expression binding, comparison,
ordering, grouping, aggregate state, and cast semantics, so production execution
still requires native typed catalog metadata for supported collations,
operators, aggregate reducers, and casts. That metadata must bind into the
shared expression AST, index metadata, query planner, schema validation,
rebuild/validation jobs, and result-type metadata. Unsupported user-defined
functions or opaque SQL bodies must not become hidden expression behavior; every
accepted object needs deterministic execution semantics that are visible to
REST/SDK typed plans before PostgreSQL syntax can execute it. These custom
type-system tails parse in `sql/grammar.zig`; the SQL lowerer only
transfers owned grammar syntax into typed catalog plans, so future syntax growth
must become explicit native catalog metadata instead of lowerer-local token
scans.

PostgreSQL `COMMENT ON TABLE`, `COMMENT ON COLUMN`, `COMMENT ON INDEX`, and
`COMMENT ON CONSTRAINT` lower to typed `comment_metadata` DDL intents that
record target kind, object identity, optional parent table, and whether the
comment is set or cleared. Schema application stores comments under typed
public schema JSON metadata such as `comments.table`, `comments.columns`,
`comments.indexes`, and `comments.constraints` after validating referenced
columns, indexes, and constraints against the current relational schema. The
`COMMENT ON` tails parse in `sql/grammar.zig`; the SQL lowerer only
transfers owned grammar syntax into typed comment metadata plans. The
runtime row-schema parser ignores that metadata, and direct runtime-schema DDL
application validates comment targets before returning an unchanged relational
schema, so comments do not change row validity, routing, index contents,
constraint enforcement, or query execution. Schema application preserves
comments across column and constraint renames and prunes comments for dropped
columns, indexes, and constraints. The remaining durable production shape is
catalog persistence keyed by table generation and object identity, the same
dependency cleanup on durable catalog objects, and REST/SDK read APIs; comments
should not be stored as opaque SQL text.

PostgreSQL view DDL is schema-bearing and lowers to a native `view_catalog`
intent instead of storing opaque SQL text. `CREATE VIEW`,
`CREATE VIEW IF NOT EXISTS`, `CREATE OR REPLACE VIEW`, and
`CREATE OR REPLACE VIEW IF NOT EXISTS` record the view name, replacement intent,
idempotent creation intent, source table, and declared output fields for
conservative one-table projections. The combined replace/idempotent form has
the same migration-equivalence contract as tables and materialized views:
create the view if it is absent, otherwise replace the existing typed catalog
definition. PostgreSQL view column lists such as
`CREATE VIEW v(alias) AS SELECT field ...` supply the catalog output-field names
after the adapter verifies that the declared aliases match the selected-field
arity; selected source fields remain separate typed metadata so aliases do not
erase the view's source-to-output mapping. Simple select-list aliases such as
`SELECT field AS alias` and `SELECT field alias` use the same mapping: the
selected source field remains durable metadata and the alias becomes the exposed
output field. `CREATE VIEW` tails parse in `sql/grammar.zig`, so the
SQL lowerer only transfers owned grammar syntax into typed view catalog plans;
`ALTER VIEW ... RENAME TO ...` and `DROP VIEW ...` record typed catalog rename
and removal intents. The remaining production shape is the durable view catalog
executor: full stored queries over the same Antfly row-query, join, lateral,
aggregate, CTE, window, and expression contracts used by direct requests,
declared output fields, dependency metadata, rename/drop cleanup, schema
invalidation, authorization policy hooks, and deterministic promotion when
referenced table schemas change. Complex view bodies still fail closed until
catalog-aware stored-query lowering can validate the full typed plan.

Materialized views use a separate native contract from plain views.
`CREATE MATERIALIZED VIEW` and `CREATE OR REPLACE MATERIALIZED VIEW` record the
materialized view name, replacement intent, source table, declared output
fields, selected source fields, optional PostgreSQL column-list aliases,
simple select-list aliases, `IF NOT EXISTS`, and whether the initial generation
is populated. `CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS` lowers to the
same typed definition with both flags preserved: catalog execution creates the
materialized view when it is absent and replaces the existing definition when it
is present, while still keeping population/refresh work in the materialized-view
job family instead of applying SQL text directly.
`CREATE MATERIALIZED VIEW` tails parse in `sql/grammar.zig`, so the
SQL lowerer only transfers owned grammar syntax into typed materialized-view
catalog plans.
`REFRESH MATERIALIZED VIEW` records the target view plus `CONCURRENTLY` and
populate/no-data policy, and `DROP MATERIALIZED VIEW` records typed removal
metadata. The remaining production shape is the durable executor: output
schemas, source dependency tracking, refresh generations, range ownership for
materialized rows, rebuild/repair jobs, snapshot isolation for readers during
refresh, replacement-generation dependency validation, reader cutover, and
promotion/rollback metadata for failed refresh attempts. PostgreSQL syntax
lowers into that typed definition and refresh job family, but it must not bypass
the row, aggregate, and routed ownership
contracts by recording an opaque SQL definition.

Temporary and unlogged relation DDL lowers to a native `relation_lifetime`
intent instead of normal durable `CREATE TABLE` storage. `CREATE TEMP TABLE`,
`CREATE TEMPORARY TABLE`, `CREATE UNLOGGED TABLE`, and their `IF NOT EXISTS`
forms parse the lifetime prefix in `sql/grammar.zig`, then reuse the
typed `CreateTablePlan` column, primary-key, constraint, period, check, and
idempotent-creation metadata before attaching an explicit lifetime kind.
The remaining production shape is the executor for that intent: session-scoped
or unlogged durability policy, dependency cleanup, transaction visibility,
crash/replay semantics, replication behavior, and range/index ownership rules.
Until those policies exist as native REST/SDK contracts, these SQL forms must
not lower to ordinary durable relational tables.

Table-population syntax lowers to a native population intent before execution.
`CREATE TABLE ... AS SELECT ...`, `CREATE TABLE IF NOT EXISTS ... AS SELECT
...`, `CREATE TEMP[TEMPORARY] TABLE ... AS SELECT ...`, `CREATE UNLOGGED TABLE
... AS SELECT ... [WITH [NO] DATA]`, and read-path `SELECT ... INTO
[TEMP|TEMPORARY|UNLOGGED] [TABLE] new_table ...` produce a
`relation_population` plan with an explicit mode, target table, target lifetime
(`durable`, `temporary`, or `unlogged`), idempotent-creation flag, populate
flag, and source `LoweredReadPlan`; the source query is therefore pinned by the
same typed row-query/aggregate/join/window fingerprints as ordinary reads
rather than stored SQL text. `WITH NO DATA` lowers to `populate=false`, so the
durable runner can create the target table generation without scanning or
loading source rows while still validating the typed source plan. Catalog-aware
population lowering is exposed for API bridges,
and the routed execution helper evaluates the typed source plan through the
same `TableReadSource` path as ordinary query, aggregate, window, join, and
lateral reads before normalizing its rows for the population job. The remaining
production shape is the durable runner for that intent: derive or declare the
target schema, honor the `IF NOT EXISTS` target identity check before scheduling
work, create the table generation with the requested target lifetime, run a
bounded insert-source or backfill job from the typed source query, record
progress by owner range, and promote the populated generation atomically.
`CREATE TABLE ... (LIKE source INCLUDING ...)` parses in the SQL adapter
grammar and lowers to a native
`table_clone` catalog intent with explicit target table, source table,
`IF NOT EXISTS`, and clone-option metadata for columns, defaults, generated
expressions, checks, constraints, indexes, periods, and update policies. The
schema application helpers can materialize the target table as either owned
runtime schema metadata or public schema JSON from a resolved source schema
through the same `CreateTablePlan` generator used by ordinary table creation,
and the JSON path reports validation/rebuild requirements for copied
constraints and indexes. The remaining production shape is the durable executor
for that intent: resolve the source and target tables through the catalog,
enforce `IF NOT EXISTS` against target identity, schedule validation/rebuild
work for every copied artifact, and promote the new table generation atomically.
These forms must not bypass the normal catalog, rebuild, validation, ownership,
and replay paths.

PostgreSQL enum type DDL now lowers to a typed enum catalog intent.
`CREATE TYPE ... AS ENUM (...)` records the enum type name and ordered label
set, `ALTER TYPE ... ADD VALUE [IF NOT EXISTS] ... [BEFORE|AFTER ...]` records
the append/positioning intent, and `DROP TYPE [IF EXISTS] ... [CASCADE]`
records typed removal metadata. Enum-type tails parse in
`sql/grammar.zig`; the SQL lowerer only transfers owned grammar
syntax into typed enum catalog plans. Schema application remains fail-closed until
the durable type catalog exists: enum-typed columns must validate labels
through catalog metadata, record dependencies from tables/checks/defaults, and
route enum-label additions, drops, and dependency cleanup through ordinary
catalog compare-and-swap plus validation/rewrite jobs instead of rewriting raw
SQL text. PostgreSQL domain DDL lowers to a typed domain catalog intent:
`CREATE DOMAIN` records the domain name, base Antfly type, optional array item
type, default, nullability, and `VALUE` checks; `ALTER DOMAIN` records typed
nullability/default operations; and `DROP DOMAIN [IF EXISTS] ... [CASCADE]`
records typed removal metadata. Domain headers and drop tails parse in
`sql/grammar.zig`; the SQL lowerer keeps base type, default, and
check-expression semantics in typed catalog planning. Schema application remains fail-closed until
the durable domain catalog exists: domain-typed columns must validate through
catalog metadata, inherit defaults/nullability/checks through a typed expansion
step, record table/default/check dependencies, and route ALTER/DROP effects
through ordinary validation/rewrite jobs and catalog compare-and-swap promotion.
PostgreSQL standalone sequence DDL lowers to typed sequence catalog intents.
`CREATE SEQUENCE` records the sequence name, `IF NOT EXISTS`, type clauses
(`AS smallint|integer|bigint`), supported allocation options (`START`,
`INCREMENT`, `MINVALUE`, `NO MINVALUE`, `MAXVALUE`, `NO MAXVALUE`, `CACHE`,
and `CYCLE`), and `OWNED BY` dependency metadata including `OWNED BY NONE`;
`ALTER SEQUENCE` records typed type, restart, option-change, and ownership operations; and
`DROP SEQUENCE [IF EXISTS] ... [CASCADE]` records typed removal metadata.
Standalone sequence tails, generated-identity sequence option clauses, and the
identity-allocator `CREATE TABLE name (identity_column ...` header parse in
`sql/grammar.zig`; the SQL lowerer only transfers owned grammar
syntax into typed sequence and identity-allocator plans. Basic schema
application now persists durable sequence catalog records and initializes a
catalog cursor. Public row writes can consume sequence-backed defaults through
the explicit sequence resolver, which advances that cursor with a raft-applied
compare-and-swap command carrying a durable allocation token so concurrent
callers cannot claim the same reserved value. The remaining production
allocator work is to add range/block allocation, track schema-owned
dependencies, and wire identity reset semantics. PostgreSQL-shaped runtime
sequence functions (`nextval`, `currval`, and `setval`) execute through the
metadata-owned sequence catalog and SQL session currval state rather than SQL
text. Column-level `serial`/`bigserial`, `GENERATED ... AS IDENTITY`,
generated-identity option clauses, and sequence-backed column defaults now have
native `sequence_next` schema metadata for the default expression.
`serial`/`bigserial` columns and
`GENERATED ... AS IDENTITY` columns lower to typed fail-closed
identity-allocator catalog plans that preserve table identity, column identity,
allocator kind, supported sequence-style options (`START`, `INCREMENT`,
`MINVALUE`, `NO MINVALUE`, `MAXVALUE`, `NO MAXVALUE`, `CACHE`, and `CYCLE`),
primary-key intent, and the count of ordinary peer columns. Full identity-column
application remains fail-closed until those table-owned allocators can claim and
reset monotonic values through the same native allocator owner instead of raw SQL
state.

PostgreSQL table partition DDL is schema-bearing catalog intent, not adapter
text. `CREATE TABLE ... PARTITION BY RANGE (...)`, `CREATE TABLE ... PARTITION
OF ... FOR VALUES FROM ... TO ...`, `ALTER TABLE ... ATTACH PARTITION`, and
`ALTER TABLE ... DETACH PARTITION` lower to typed fail-closed partition catalog
plans that preserve parent table identity, child partition identity, partition
method, key count, and range bounds. Partition metadata tails parse in
`sql/grammar.zig`; partitioned table definitions still reuse the
typed `CreateTablePlan` lowerer for column and constraint parsing before
attaching grammar-owned partition metadata. Schema application remains
fail-closed until native partition metadata owns placement and routing. The production shape
describes logical partition keys, range/list/hash bounds, partition
generations, owner-range placement, query-pruning metadata, write-routing
validation, FK/unique/check interactions, secondary and embedded-JSON rebuild
scoping, and attach/detach promotion jobs. Child-table SQL names must remain
catalog metadata and must not become hidden physical routing state.

PostgreSQL row-level-security DDL is not adapter-only syntax. `ALTER TABLE ...
ENABLE ROW LEVEL SECURITY`, `CREATE POLICY ... USING (...)`, and `DROP POLICY`
lower to typed row-security catalog plans. The current runtime subset preserves
table identity, policy identity, enable intent, request-setting equality
predicates such as `tenant_id = current_setting('app.tenant_id')`, and scalar
literal equality predicates such as `status = 'active'` as native policy
metadata rather than raw SQL. `CREATE POLICY ... TO role[, ...]` and
`ALTER POLICY ... TO role[, ...]` preserve ordered role-target metadata on the
same typed policy plan, so role scoping is catalog data rather than SQL text
hidden in the adapter. `CREATE POLICY ... WITH CHECK (...)` and
`ALTER POLICY ... WITH CHECK (...)` preserve a separate write-check predicate
beside the read-side `USING` predicate. If a policy omits `WITH CHECK`, the
write predicate defaults to the read predicate when applied to native auth
state, matching PostgreSQL's insert/update behavior without storing raw SQL.
`CREATE POLICY` tails parse in
`sql/grammar.zig`, so the SQL lowerer only transfers owned grammar
syntax into typed row-security catalog plans. Public API execution maps each
supported policy to a hidden native row-filter policy subject, converts
`current_setting('app.<key>')` to `{"$auth":"settings.app.<key>"}`, stores
literal equality as a native `term` row filter, lowers supported `AND` chains
over those atoms to native row-filter `conjuncts`, lowers supported `OR` chains
over those atoms to native row-filter `disjuncts`, preserves nested
conjunctions/disjunctions for mixed parenthesized and SQL-precedence
`AND`/`OR` policy predicates, applies role targets during effective-policy
resolution, and merges enabled-table filters into every matching user's
effective row filters before row-query, aggregate, join, lateral, window, and
document query execution. Those setting references are
resolved from the authenticated user's effective native role settings; a missing
setting fails closed during request filter resolution. Write execution resolves
the enabled read and write filters independently: existing rows touched by a
delete/update must satisfy the read `USING` filter, while inserted rows and
final update row images must satisfy the `WITH CHECK` filter. Creating a policy stores
policy metadata but does not make the filter active until `ALTER TABLE ...
ENABLE ROW LEVEL SECURITY` marks that table as RLS-enabled in the native auth
policy store. `ALTER POLICY ... USING (...)` replaces the hidden policy
subject's native read-filter metadata, `ALTER POLICY ... WITH CHECK (...)`
replaces its write-filter metadata, and both fail if the policy does not already
exist. `DROP POLICY` removes the hidden policy subject for that table, while
`DROP POLICY IF EXISTS` is an idempotent no-op. Policy syntax outside the
current native `term`/`conjuncts`/`disjuncts` row-filter subset still fails
closed until it has durable native policy state, request-context bindings, and
planner/executor validation.
`ALTER TABLE ... DISABLE ROW LEVEL SECURITY` clears the native table enable bit
idempotently; stored policies remain catalog metadata but stop contributing
effective row filters until the table is enabled again. Storage must not
silently accept or ignore unsupported SQL RLS declarations.

PostgreSQL privilege and role DDL is also not adapter-only syntax. `GRANT`,
`REVOKE`, and role lifecycle statements lower to typed authorization-catalog
intent that captures role names, privilege counts, object kind/name, principal
identity, and role setting names, then fail closed when applied to table schema
storage. Those role and privilege tails parse in `sql/grammar.zig`;
`sql/lower_ddl.zig` maps the owned authorization syntax into typed plan fields
and validation-owned allocation. Public API execution routes
authorization-catalog SQL through the
native user-management surface instead: `CREATE ROLE app_writer` creates the
Antfly auth subject `role:app_writer`, `DROP ROLE` succeeds only after that
subject has no remaining permissions, row filters, inheritance edges, or user
assignments, and table
`GRANT`/`REVOKE` statements map PostgreSQL privileges onto Antfly
`read`/`write`/`admin` table permissions, with `ALL [PRIVILEGES]` expanded to
all three native permission bits. `GRANT`/`REVOKE ... ON ALL TABLES IN SCHEMA
public` is catalog-aware: API execution enumerates the current public table
names from the admin snapshot and applies the corresponding per-table native
permissions, so future tables are not accidentally granted without an explicit
catalog event. Multi-permission SQL grants and revokes are staged as one native
auth-policy change batch and rollback on application failure, so expanded
privileges do not leave partially-applied policy state. Non-public schema-wide
targets fail closed until schema namespaces have first-class authorization
semantics. A grant target that is
already an Antfly user is applied directly to that user; otherwise SQL
principals must resolve to a SQL-created `role:<name>` subject instead of being
created implicitly by `GRANT`. `ALTER ROLE ... SET app.<key> = 'value'`
persists the setting through the native auth policy store as role-owned
metadata, `ALTER ROLE ... RESET app.<key>` removes that metadata idempotently,
`ALTER ROLE ... IN DATABASE <db> SET/RESET app.<key>` stores the same typed
metadata with an explicit database scope, and dropping the role removes any
remaining owned settings. Authentication exposes merged effective role settings
after role inheritance plus direct user settings. This is the first native SQL
role-setting model because `app.*` settings are consumed by row-security
`current_setting('app.*')` policies.
PostgreSQL runtime defaults such as `ALTER ROLE app SET statement_timeout =
'1ms'`, database-scoped runtime defaults, and `RESET statement_timeout` now
lower to typed `alter_role` plans with `setting_kind=runtime` instead of being
collapsed into native app row-security settings. The auth execution boundary
persists supported runtime defaults (`statement_timeout`, `timezone`, and
`search_path`) through a separate role runtime-setting policy namespace with
optional database scope, so PostgreSQL GUC-style defaults do not contaminate the
`app.*` row-security setting store. If inherited roles
define the same native app setting with different values, effective-setting
resolution fails closed; a direct user setting for the same key is an explicit
override. Session `SET app.<key> = ...`, `SET statement_timeout = ...`, and
matching `RESET app.<key>` lower to typed session catalog plans, and
`ALTER ROLE ... SET app.<key> = current_setting('app.<key>')` stores the current
typed session value through the same auth execution boundary. Arbitrary
expression-valued role defaults still fail closed until they have explicit
native semantics. SQL DDL and `COPY` execution read the effective
`statement_timeout` once at statement start, fail expired statements before
dispatch, and re-check after typed execution before returning owned results.

`COPY FROM` and `COPY TO` tails parse in `sql/grammar.zig` and lower
to typed bulk import/export intent that captures table identity, selected
columns, stream endpoint, direction, format, CSV header intent, import-freeze
intent, export force-quote column policy, import force-null/not-null column
policies, row-error policy, import-side reject limit for ignored row errors,
log verbosity for ignored row errors, delimiter byte, quote byte, escape byte,
null-marker string, and import-side default replacement marker, then fails
closed when applied to table schema or runtime storage. Stream encoding names
are captured as typed bulk-I/O metadata so import/export validation can
normalize or transcode before row decoding. Reject limits and non-default log
verbosity are valid only for `COPY FROM STDIN` with `ON_ERROR ignore`;
export-side or stop-on-error row-error controls fail before a typed plan is
accepted, and default replacement is valid only for imports. Import-side
`COPY FROM ... WHERE` admits a conservative scalar `field <op> literal`
conjunction subset and stores it as shared `RelationalRowsExpressionCondition`
metadata, so later row decoding can evaluate filters without carrying SQL text.
`sql.bulkSqlIoExecutionPlanFromDdlPlan` now maps supported stream,
file, and program `COPY` plans into an explicit Antfly execution contract:
`COPY FROM STDIN` requires a write permission on the table, audits as
`copy_from`, and routes to the row-batch import surface; `COPY TO STDOUT`
requires a read permission, audits as `copy_to`, and routes to the row-query
export surface. File endpoints use an optional embedding-provided file adapter,
and `PROGRAM` endpoints use an optional embedding-provided program adapter;
both remain fail-closed when the adapter is absent. Antfly never executes shell
commands directly for `PROGRAM`; the adapter owns command allowlisting,
sandboxing, and byte production/consumption while Antfly still owns SQL target
resolution, auth, row-filter enforcement, and audit. File and program COPY
endpoints are authorized before external bytes are read or written, and the
audit stream distinguishes applied, denied, and post-authorization failed
attempts. The bridge also marks
STDIN/STDOUT as requiring an external SQL protocol stream so the SQL text cannot
masquerade as payload. `COPY FROM STDIN` has an initial native CSV executor that
converts the supplied stream into the row-batch API, and
`ApiHttpServer.executeBulkSqlWithSession` is the production SQL bulk-I/O
entrypoint. It dispatches typed `COPY FROM STDIN` plans to an owned import
result after resolving the SQL target through the current catalog session,
checking the concrete table write permission, loading the relational schema for
typed default/generated/check validation, validating imported rows against the
authenticated identity's effective row-filter policy when one applies, and
dispatching the resulting rows through the catalog-aware row-batch write
source. The direction-specific `executeBulkSqlCopyFromStdinWithSession` helper
is kept as a narrow compatibility wrapper over that same plan-level
implementation.
`COPY TO STDOUT` has the matching native CSV executor:
`ApiHttpServer.executeBulkSqlWithSession` returns an owned export stream after
resolving the SQL target through the current catalog session, checking concrete
table read permission, loading the relational schema, pushing the authenticated
identity's effective row-filter policy into the catalog-aware rows-query read
vtable, and serializing the selected result columns with `HEADER`, delimiter,
quote, escape, null-marker, UTF-8 encoding, and `FORCE_QUOTE` semantics. The
direction-specific `executeBulkSqlCopyToStdoutWithSession` helper is likewise a
compatibility wrapper over the shared plan-level path. The
decoder keeps per-field quote metadata, so default CSV `NULL ''` handling
distinguishes unquoted empty fields from quoted empty strings and
`FORCE_NULL`/`FORCE_NOT_NULL` follow PostgreSQL CSV semantics before row JSON
reaches typed schema validation. The native bridge validates CSV delimiter,
quote, and escape options before row import/export execution, including
rejecting ambiguous delimiter/quote collisions instead of carrying malformed
CSV settings into typed row planning. It also binds the execution plan against
the resolved table schema before any stream bytes are decoded or emitted:
selected import/export columns must be unique and existing, `COPY FROM` cannot
target generated columns, `FORCE_QUOTE` / `FORCE_NULL` / `FORCE_NOT_NULL`
columns must be unique, existing, and part of the selected copy column set, and
`COPY ... WHERE` expressions may only reference row fields owned by that table.
The remaining production layer is the broader streaming executor contract around
those paths: chunked protocol backpressure, 2PC staging, retryable range routing,
deterministic reject-limit accounting, and snapshot-stable export streaming.
PostgreSQL `COPY` syntax is therefore an adapter frontend for those contracts,
but it must not bypass row-batch, mutation-source, or routed read semantics.
Legacy `COPY ... WITH (OIDS false)` is accepted as an explicit compatibility
no-op and lowers to the same typed bulk-I/O plan as ordinary `COPY`; requesting
row OIDs with `OIDS true` still fails closed because Antfly has no native row OID
model. The source parity corpus pins concrete `bulk_sql_io` execution
fingerprints for `STDIN` / `STDOUT`, file, and program endpoints, text/CSV and
binary codecs, copy option validation, and row-filtered `COPY FROM ... WHERE`
intent. Invalid stream direction such as `COPY ... TO STDIN` and OID-producing
legacy options remain explicit fail-closed `bulk_io_plan` classifications, while
runtime endpoint authorization, protocol backpressure, and environment-specific
stream availability stay in the execution bridge before storage can receive or
emit bytes.

PostgreSQL function and procedure lifecycle DDL lowers to typed routine-catalog
intent that captures routine kind, name, arity, replacement, return type,
language, optional volatility (`IMMUTABLE`, `STABLE`, or `VOLATILE`), optional
security mode (`SECURITY INVOKER`, `SECURITY DEFINER`, and `EXTERNAL SECURITY`
synonyms), optional planner `COST`, optional planner row-count metadata (`ROWS`),
optional null-input behavior (`CALLED ON NULL INPUT`, `RETURNS NULL ON NULL
INPUT`, or `STRICT` normalized to returns-null semantics), optional
parallel-safety metadata (`PARALLEL SAFE`, `PARALLEL RESTRICTED`, or `PARALLEL
UNSAFE`), optional `LEAKPROOF` metadata, optional `WINDOW` function metadata,
optional planner support-function identity (`SUPPORT function_name`), optional
transform type metadata (`TRANSFORM FOR TYPE type_name[, ...]`), optional
routine-local setting metadata (`SET setting TO value[, ...]`, `SET setting =
value[, ...]`, or `SET setting FROM CURRENT`) as ordered name/value or `FROM
CURRENT` declarations, and drop dependency metadata such as `CASCADE`. When
`ApiHttpServer` applies routine catalog DDL, `SET search_path FROM CURRENT`
freezes the current ordered SQL session search path into durable routine
metadata, and other `FROM CURRENT` routine settings freeze the current value
only when it exists in the typed SQL session setting map; absent or unsupported
settings fail closed before the routine record is stored. Routine lifecycle
tails parse in `sql/grammar.zig`; `sql/lower_ddl.zig` maps that owned syntax
into typed routine-catalog plans, so accepted function/procedure options must
become native metadata before they can execute. The known updated-at helper
definition uses that same typed routine-catalog boundary; the native behavior
still lives in table-owned update-policy metadata created by `CREATE TRIGGER`.
`ApiHttpServer` applies routine catalog DDL through `sql/routines.zig`,
which stores routine records in a native runtime catalog. Safe
`LANGUAGE sql AS 'SELECT ...'` expression bodies lower only when they map to a
typed routine body hook carrying the same row-expression AST used by generated
columns, row rewrites, checks, expression indexes, predicates, update
transforms, and `RETURNING`; routine argument references normalize to
`field[source:argN]` by ordinal inside that AST, so `$1`, `$2`, `arg1`, and
`arg2` bind to the same typed argument object fields that the routine runtime
uses. The routine runtime clones those typed body expressions and can execute
expression hooks through the shared `relational_rows.expressionValueJsonAlloc`
evaluator, so supported functions such as ordinal identity, `lower`, `upper`,
`md5`, `concat`, `concat_ws`, `coalesce`, `nullif`, `greatest`, `least`,
nested allowlisted function calls, and simple bounded numeric addition over
argument or literal operands are not opaque SQL strings. The same runtime
exports executable expression-body functions as typed SQL lowerer bindings, so
ordinary read-plan lowering can inline catalog routines by SQL name and overload
arity into native row-expression projections instead of calling back into SQL
text. `STRICT` / `RETURNS NULL ON NULL INPUT` routines only plan when a null
result is statically known from a literal null argument; dynamic strict calls
fail closed until the row-expression AST has a native null-guard node that can
preserve PostgreSQL null-input semantics for nullable row fields.
The routine body model also admits narrow safe PL/pgSQL trigger bodies:
`BEGIN RETURN NEW; END` as a `plpgsql_trigger` / `trigger_return_new` hook,
`BEGIN RETURN OLD; END` as a `plpgsql_trigger` / `trigger_return_old` hook, and
`BEGIN RETURN NULL; END` as a `plpgsql_trigger` / `trigger_return_null` hook.
Benign `RAISE NOTICE 'literal';` statements may appear before the return and
are treated as deterministic adapter-level diagnostics with no storage side
effects. A trigger body may also contain `PERFORM function_name();` statements
before the return, but only when each referenced routine is an existing
zero-argument safe SQL expression function. The routine runtime records those
dependencies as `perform` entries in the typed body plan, validates them when
creating or recovering the trigger routine, executes them through the expression
routine evaluator while discarding their result, and rejects drops of referenced
functions while any cataloged routine depends on them. Qualified calls,
arguments, dynamic SQL, table writes, and unknown side effects still fail
closed rather than being persisted as opaque PL/pgSQL.
`ApiHttpServer` routes cataloged `CREATE TRIGGER` / `DROP TRIGGER` DDL that is
not a table-owned update-policy shortcut into the routine runtime, where
trigger records persist table name, trigger name, function name, and event.
Trigger creation validates that the referenced zero-argument function exists
and has a safe trigger hook for the requested event; routine drops fail while a
cataloged trigger still references the function. Trigger records recover from
the durable routine store and the HTTP row-batch executor now applies that safe
hook subset before native mutation planning: `BEFORE INSERT` may return `NEW`
or `NULL`, `BEFORE UPDATE` may return `NEW`, `OLD`, or `NULL`, and
`BEFORE DELETE` may return `OLD` or `NULL`. `RETURN NULL` drops the candidate
operation before storage sees it; `RETURN OLD` on update is a typed no-op for
the requested row change; and `RETURN NEW` preserves the planned row-batch
mutation. `COPY FROM STDIN` routes through the same insert hook path after
payload parsing and before permission checks and catalog batch writes, so bulk
import cannot bypass supported before-insert hooks. The model also admits the
side-effect-free PL/pgSQL procedure body `BEGIN NULL; END` as a
`plpgsql_procedure` / `procedure_noop` hook, plus the same benign `RAISE NOTICE
'literal';` and safe zero-argument `PERFORM function_name();` prefix statements
allowed for trigger bodies. `PERFORM` calls may also carry scalar JSON
arguments; the routine runtime validates and executes them against the exact
function arity, preserves the argument JSON in catalog metadata, and rejects
drops of referenced overloads. `CALL procedure_name()` executes that hook
through the routine runtime after catalog recovery; the hook is still
intentionally not executable through the expression-function path. Other
PL/pgSQL bodies, including dynamic SQL, procedural branches, loops, table
writes, or external side effects, fail closed under `routine_body_plan` unless
their row inputs, side effects, dependency tracking, replay behavior, and repair
semantics have native contracts.
Table-schema and table-storage application still reject routine-catalog plans
because routines are catalog/runtime objects, not schema JSON mutations.
Routine bodies and routine options are resolved only when they lower to typed
metadata over this safe catalog/runtime subset. Supported options such as
volatility, security, null-input behavior, parallel safety, support function,
transforms, cost, rows, and settings are stored as structured routine metadata;
duplicate or malformed options fail during parsing rather than becoming opaque
catalog text. Bodies outside the safe subset remain pinned in the source parity
corpus under `routine_body_plan` as deliberate fail-closed classifications,
preserving the native mutation-hook/catalog contract: deterministic hook
identity, declared row inputs and outputs, allowed side effects, dependency
tracking, schema promotion, authorization hooks, replay behavior, and
repair/rebuild semantics.
Storage should never execute or persist opaque PL/pgSQL bodies as part of the
relational model.

| 0a. SQL table-reference normalization | Keep SQL table-reference sugar out of storage and typed request contracts. | Golden corpus entries now pin `ONLY` as an adapter-only no-op for index targets, single-table reads, inserts, point deletes, claimed mutation sources, truncate, and joined mutation sources, including `public.`-qualified table names. | Table-reference spelling changes produce the same Antfly typed plan or fail closed before storage. |
| 1. Shared expression spine | Replace shape-specific predicate/projection/update/index cases with one bound expression AST over fields, literals, parameters, casts, deterministic functions, boolean ops, arithmetic, null semantics, JSON, and arrays. | Existing row queries, JSONB/array predicates, order keys, aggregate filters, conflict updates, and `RETURNING` use typed expression nodes, and those nodes now live in the shared storage schema layer instead of the db request module so schema/catalog features can adopt the same type without depending on row execution internals. Generated columns and checks still have narrower typed metadata today. | The same AST is used for `WHERE`, projections, checks, generated columns, expression indexes, partial predicates, `ON CONFLICT` actions, update transforms, aggregate filters, order keys, and `RETURNING`; pushdown is derived from the AST rather than hand-coded parser cases. |
| 2. Catalog and migration lifecycle | Apply migration-equivalent DDL and data-change intent as transactional catalog mutations, typed rewrites, rebuilds, and validations. | Composite identity, unique/FK validation state, schema-controller repair/promotion, secondary-index lifecycle, CAS schema promotion, and embedded JSON-column indexing are now the baseline. | Intended final schema and migration effects compile into catalog schema JSON; every existing-table index, expression index, unique constraint, FK, check, embedded JSON schema/template change, and non-additive rewrite produces durable work with deterministic range ownership and promotion gates. Exact PostgreSQL migration-file replay can be layered on top by lowering into these same steps. |
| 3. Point and selector DML hardening | Finish `INSERT`, point/unique `UPDATE` and `DELETE`, `ON CONFLICT ... DO UPDATE/NOTHING`, `DEFAULT`, `NOW()`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`, `EXCLUDED`, JSONB/array transforms, server-owned update policies, and committed-image `RETURNING`. | Structured primary/unique selectors, durable unique-owner rows, point row-batch plans, 2PC participants, table-owned updated-at policies, JSONB/array transforms, conflict-action expressions, and committed-image field/expression returning are implemented. | Ledger, auth, RBAC, billing, seed, and JSON metadata flows run through typed row-batch plans; non-primary selectors resolve through unique-owner rows before prepare; `RETURNING` is evaluated from the final committed image. |
| 4. Multi-row DML and queue claims | Local `mutation_source` plans update/delete rows selected from ordered lockable row-query sources for `UPDATE ... WHERE`, `DELETE ... WHERE`, and `FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]`; the row API can parse/encode the typed contract and the PostgreSQL adapter can lower bounded transaction-claimed update/delete statements into it. Joined mutation-source requests, OpenAPI/generated SDK models, and SQL lowering cover claimed target-side `UPDATE ... FROM` / `DELETE ... USING` plan shapes, including ordinary PostgreSQL-shaped statements that omit SQL row-lock syntax because the typed request supplies claim metadata; `source_table` is the public source table identity field, `source_assignments` is the public source-to-target field-copy shape, and parser/lowerer validation can bind target and source fields against separate table schemas. Local DB execution plans joined target/source candidates, applies target-side ordering and pagination, rejects ambiguous multi-source target matches, claims only target rows, stages target update/delete intents, and projects target `RETURNING` rows. Public HTTP/write-source execution accepts the joined body for bound, provisioned, and hosted catalog-routed tables with global source visibility; hosted remote owners expose internal joined input/stage routes. Catalog-backed cross-table `source_table` requests bind against the source schema, read source rows from source-table owner ranges, and stage only target-row intents back to target owner ranges. | Base-row row claims, ordering, pagination, unique-owner routing, FK/unique 2PC participants, OCC predicates, SQL lowerer support, transaction-staged mutation sources, joined mutation-source plan parsing, generated public contract types, local joined mutation-source execution, public routed joined execution, hosted remote joined input/stage routing, target/source schema-aware joined lowering, source table identity in the typed contract, catalog-routed cross-table source reads, source-schema-aware target staging, and golden SQL lowering exist. | Queue, cleanup, re-encryption, and batch mutation workloads claim rows durably, fill across ranges in order, integrate with OCC/2PC, survive retries and range movement, and reject claims over joins, aggregates, windows, and materialized CTEs until those stages expose lockable base rows. Joined mutation-source execution claims only the target side, treats the source side as read-only input, and stages target-row intents with deterministic preimage/OCC checks; remaining work is expanded topology-change chaos coverage for owner movement during joined planning/staging. |
| 5. Index and JSON/array completeness | Generalize partial/expression indexes, partial unique constraints, JSONB operators, array operators, and embedded JSON-column document indexes through the shared expression and catalog-work lifecycle. | Simple partial predicates, JSON/array GIN secondary indexes over declared JSON and array columns, generated expression secondary indexes for case-fold, stable unary hash, simple concat keys, simple `concat_ws` keys, and richer deterministic row expressions, including harmlessly parenthesized SQL expression-index elements, case-fold and stable unary hash unique expression keys, ordinary secondary-index rebuild work, JSON/array GIN index DDL, JSON/array predicates/transforms, and embedded JSON-column design are present. | Planner uses only `ready` derived artifacts; writes enforce only `enforced` unique constraints; partial implication is shared by planner, uniqueness, and `ON CONFLICT ... WHERE`; JSON/array SQL sugar has equivalent REST/SDK typed nodes; embedded JSON schema/template changes rebuild deterministically. |
| 6. Routed stream execution | Extend local row streams, joins, CTEs, aggregates, lateral stages, set operations, and windows to routed cross-table/cross-range execution with emitted field validation, richer result type metadata, and memory/spill bounds. | Local, declared multi-range, provisioned, and hosted equality joins, row queries, aggregate/window source execution, non-recursive CTE lowering, native ordered CTE plan parsing, compatible `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` stream plans with result-tail ordering/pagination, scalar aggregates, bounded ordered/distinct `array_agg` and `string_agg`, scalar/boolean `FILTER`, scalar `DISTINCT`, bounded local, CTE-backed, declared multi-range, provisioned, and hosted `LEFT JOIN LATERAL`, and local, provisioned, or hosted `row_number()`, `rank()`, `dense_rank()`, `percent_rank()`, `cume_dist()`, `ntile()`, `lag()`, `lead()`, `first_value()`, `last_value()`, `nth_value()`, `count()`, `sum()`, `avg()`, `min()`, and `max()` are modeled. Hosted remote owner row-source reads validate topology epochs and hosted plans coordinate static source-row reductions without a local owner range. Set-operation branch projections are type-checked at the adapter boundary; incompatible result shapes fail closed under `set_operation_output_shape` and never reach storage as SQL text. | Routed reads execute with correct row-version visibility, ordering, pagination, coordinator merge behavior, bounded materialization, and explicit spill/fail policy. |
| 7. Parity proof | Make SQL/API parity a test gate rather than a parser breadth claim. | Focused unit/runtime tests and core storage chaos/sim coverage already exist. | Golden-plan corpus tests, representative execution flows, and sim/chaos workloads combine live writes, FK checks/actions, unique-owner repair, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and native rewrite/rebuild jobs. |

The representative SQL corpus should be treated as a parity workload, not
as a mandate to build a PostgreSQL executor. The recurring shapes in SQL/API
workloads and migrations are:

- schema DDL for tables, additive/non-additive `ALTER TABLE`, primary keys,
  unique constraints, foreign keys, partial indexes, expression indexes,
  defaults, checks, JSONB columns, array columns, and updated-at triggers;
- point and selector-based `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT ... DO
  NOTHING/UPDATE`, `RETURNING`, arithmetic updates, `NOW()`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`, explicit
  `DEFAULT`, `EXCLUDED.column`, and JSONB/array transforms;
- scalar predicates with `LOWER(...)`, `UPPER(...)`, `INITCAP(...)`, `LENGTH(...)`, `OCTET_LENGTH(...)`, `COALESCE(...)`, `GREATEST(...)`, `LEAST(...)`, `ABS(...)`, `ROUND(...)`, `FLOOR(...)`, `CEIL(...)`, casts, null tests,
  `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`, `IN`, `ANY`, `NOT`, mixed `AND`/`OR`, and timestamp
  comparisons;
- JSONB and array operators including `->`, `->>`, `@>`, existence checks,
  `jsonb_set`, `to_jsonb`, JSONB concat/build forms, array membership,
  `array_length`, `array_agg`, `string_agg`, and `ANY($uuid_array)`;
- ordered/paginated reads, `FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]`, grouped
  rollups, aggregate `FILTER`, aggregate `DISTINCT`, `HAVING`, ordinary
  equality joins, `LEFT JOIN`, and dashboard-style `LEFT JOIN LATERAL`.

Given the relational, composite-identity, foreign-key, unique-owner, embedded
JSON, and secondary-index lifecycle work in this PR, the long-term plan is:

| Priority | SQL/API shape | Antfly model plan | Production gate |
| --- | --- | --- | --- |
| P0 | SQL and migration-equivalence corpus | Generate a normalized corpus from representative SQL plus the intended schema/data effects of migrations; bind every statement or native migration step against an Antfly catalog snapshot; lower supported shapes to typed Antfly DDL/query/mutation/aggregate/join/rewrite/rebuild plans; classify adapter-only no-ops and unsupported shapes explicitly. | Golden typed-plan tests must cover every harvested runtime statement and migration-equivalence step before claiming SQL/API parity. Adapter-only no-ops include a reason and remain outside storage; unsupported classifications include feature, reason, and intended Antfly plan shape. |
| P0 | SQL boundary | Keep SQL text inside the Postgres-facing adapter. Storage, repair, rebuild, FK, unique, query, and row-write paths receive only Antfly-owned structs. | Add fail-closed lowerer tests that prove unsupported SQL never reaches storage as text or stringly-typed fragments. |
| P1 | DDL and migrations | Lower supported DDL and native migration-equivalent steps into catalog mutations over `TableSchema`, relational columns, primary keys, unique/FK/check metadata, generated columns, defaults, update policies, secondary-index lifecycle state, and explicit rewrite/rebuild/validation work. Non-additive changes become planned rewrite/rebuild jobs because the relational format starts here. | Compile the final schema and migration effects into catalog JSON; assert rebuild/validation work is scheduled for existing-table indexes/constraints; reject extension, dump, PL/pgSQL, or trigger syntax unless it compiles to a typed policy or proven no-op. Exact Postgres migration replay can be an adapter test, not the storage contract. |
| P1 | Partial/expression indexes and conflict targets | Use the common typed expression tree for partial predicates, generated columns, expression unique keys, and conflict-target inference. Ordinary secondary indexes use generation-aware rebuild work and catalog schema compare-and-swap promotion; expression-derived indexes should reuse that lifecycle as they move onto the common expression tree. | Queries use only `ready` secondary indexes; writes enforce only `enforced` unique constraints; `ON CONFLICT ... WHERE` shares the same predicate normalizer as planner pushdown and uniqueness enforcement. |
| P1 | Point DML, selector DML, and `RETURNING` | Keep row mutations as typed row-batch plans over structured primary/unique selectors, OCC predicates, transforms, conflict actions, generated/default/server-owned values, and returning projections from the committed image. | Cover identity, authorization, billing, seed, and JSON metadata flows that depend on `RETURNING`, `EXCLUDED.column`, arithmetic increments, JSONB updates, array transforms, `NOW()`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`, and explicit `DEFAULT`. |
| P1 | Multi-row `UPDATE`/`DELETE` and row locks | A local `mutation_source` node now accepts a lockable typed row-query source with order/limit, row-claim metadata, OCC preimage predicates, and `NOWAIT` / `SKIP LOCKED` selection, then stages update/delete intents in the claiming transaction. | Queue and re-encryption workloads using `FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]` run without double-claiming, lost updates, or range-movement leaks across routed owner ranges. Claims stay illegal over joins, aggregates, and materialized CTEs until those sources expose lockable base rows. |
| P1 | Scalar expression tree | One expression AST covers predicates, projections, generated columns, check constraints, expression indexes, conflict actions, update transforms, aggregate filters, aggregate-output `having_expressions`, `having_any`, `having_not`, order keys, and `RETURNING`. Nodes include row fields, parameters, literals, casts to text/numeric/boolean/datetime, null tests, boolean ops, arithmetic, `LOWER`, `UPPER`, `TRIM` / `BTRIM` / `LTRIM` / `RTRIM`, `REPLACE`, `REGEXP_REPLACE`, `REGEXP_COUNT`, `REGEXP_INSTR`, `REGEXP_SUBSTR`, `REGEXP_LIKE` / regex-match predicates, `SUBSTRING`, `OVERLAY`, `SPLIT_PART`, `STRPOS`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REPEAT`, `REVERSE`, `STARTS_WITH`, `ENDS_WITH`, `ASCII`, `CHR`, `MD5`, `CONCAT`, `CONCAT_WS`, `LENGTH`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `ABS`, `ROUND`, `FLOOR`, `CEIL`, `TRUNC`, `SQRT`, `SIGN`, `POWER`, `CASE`, `NOW`, `DATE_TRUNC`, `DATE_BIN`, `DATE_PART` / `EXTRACT`, interval arithmetic, and `IS [NOT] DISTINCT FROM`. | Every expression is type-bound once at the adapter boundary; planner pushdown is an optimizer property of the AST; unsupported functions fail before execution. |
| P1 | JSONB and array operators | Treat SQL operators as sugar over typed JSON/array predicate, projection, construction, and transform nodes on declared relational columns or embedded JSON-column indexes. | JSONB path/extract/containment and array membership/equality/containment are available through REST/SDK typed plans as well as SQL; indexed paths rebuild through the same derived-artifact lifecycle as secondary indexes. |
| P2 | Joins and routed streams | Ordinary `INNER`/`LEFT` equality joins use typed row streams; distributed execution routes each table/range through durable ownership and chooses lookup/hash/merge strategies from typed hints. | Cross-table/range join tests cover live writes, owner movement, row-version visibility, ordering, and coordinator merge semantics. |
| P2 | `LEFT JOIN LATERAL` | Bounded lateral is modeled as a correlated subquery stage: the right plan binds against left-row fields, requires a capped right-side plan, and preserves unmatched left rows with null right outputs. Local, CTE-backed, and declared multi-range lateral execution share the same correlation loop. | Dashboard queries using per-organization balance and stipend lookups run through typed lateral stages rather than bespoke SQL execution; catalog-owned remote routing and hosted participant placement remain production work. |
| P2 | Aggregates, `DISTINCT`, `FILTER`, and `HAVING` | Aggregate specs already support expression inputs, scalar and boolean expression filters, scalar distinct keys, native expression group keys, redundant `SELECT DISTINCT` over grouped and global aggregate outputs, ordered capped `array_agg` and `string_agg`, bounded `array_agg(DISTINCT ...) FILTER (...)`, bounded `string_agg(DISTINCT ..., delimiter ORDER BY ...)`, boolean folds through `bool_or`/`bool_and`, output-field `having`, and computed/boolean aggregate-output `having_expressions`, `having_any`, and `having_not` over emitted group/metric rows. DB-backed scalar aggregate `DISTINCT` state spills to durable query-temporary storage when its declared in-memory cap is exceeded; pure helper execution remains fail-closed and ordered collection aggregates stay explicitly capped. | Usage rollups, billing balances, RBAC membership summaries, `COUNT(DISTINCT ...)`, `BOOL_OR`, `BOOL_AND`, `array_agg(DISTINCT ...) FILTER (...)`, `string_agg(DISTINCT ..., delimiter ORDER BY ...)`, `SELECT DISTINCT lower(status)`, redundant grouped/global `SELECT DISTINCT` aggregates, `GROUP BY lower(status)`, output-alias, computed-expression, OR, and NOT `HAVING`, and aggregate ordering/pagination have golden plans and execution tests. |
| P2 | CTEs and windows | Non-recursive CTEs are named typed subplans with emitted-field validation, inlining rules, and materialization bounds. Windows are separate typed stream stages with partition/order metadata. Aggregate-style windows such as `count(*) OVER ()` and `sum(amount) OVER (PARTITION BY tenant)` may omit ordering and default to the whole partition; ranking, offset, and scalar value windows remain ordered stages. Local and multi-range `row_number()`, `rank()`, `dense_rank()`, `percent_rank()`, `cume_dist()`, `ntile()`, `lag()`, `lead()`, `first_value()`, `last_value()`, and frame-aware `nth_value()`, `count()`, `sum()`, `avg()`, `min()`, and `max()` stages are implemented with typed `ROWS`/`RANGE` frames, current-row starts, unbounded ends, bounded `ROWS n PRECEDING/FOLLOWING` offsets, and bounded `RANGE n PRECEDING/FOLLOWING` offsets for a leading numeric/datetime field or expression order key plus optional trailing composite tie-breakers. Exact local `percentile_cont(...)`, `percentile_disc(...)`, and ordered-set `mode()` `WITHIN GROUP` aggregate lowering is supported through bounded typed reducers. | Migration/backfill queries either lower to bounded CTE/window plans or fail closed with a replacement plan; richer result type metadata, recursive CTEs, additional ordered-set aggregate stages, ordered-set/percentile window stages, routed percentile merge semantics, and spill/backpressure remain production work. |
| P3 | Parity chaos | Combine live writes, FK checks/actions, unique-owner movement, secondary and embedded-JSON rebuilds, row claims, joins, aggregates, range movement, catalog promotion, and repair in one modeled workload. | A SQL/API parity sim suite becomes a release gate alongside SQL golden-plan and representative execution tests. |

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

3. **Remaining model-level work for SQL/API parity.**
   The remaining non-Postgres-specific gaps are partial/expression index
   completeness, unique-owner routing for any still-unsupported non-primary
   selector path, routed cross-range joins, reusable CTE materialization bounds,
   spill-backed aggregate `DISTINCT`, richer aggregate filters, catalog-owned
   remote lateral routing beyond bounded local, CTE-backed, and declared
   multi-range `LEFT JOIN LATERAL`, broader window frame semantics beyond local
   ranking/value windows, advanced JSONB/array expression operands, broader
   deterministic mutation-hook policies, and corpus/chaos gates. Multi-row
   lockable mutation-source DML, `FOR [NO KEY] UPDATE [NOWAIT|SKIP LOCKED]` row claims, conflict
   actions over `excluded`/committed row images, and committed-image
   `RETURNING` expressions have typed API homes and routed execution paths; the
   remaining work there is production hardening around topology changes and
   expanding deterministic expression families through the shared AST. These
   should land as typed Antfly APIs first, then be exposed by the SQL adapter.

Remaining SQL/API parity gaps are tracked in release-gated fixture inventories
rather than repeated here as prose tables. Each inventory row names the model
surface, current evidence, missing evidence, and release gate that must pass
before the gap can be removed.

The read-join and bounded-lateral SQL lowerers have separate source-schema
entrypoints for cross-table planning. The remaining routed-read work is
row-version visibility under live owner movement, routed owner-stream strategy
selection for lookup/hash/merge joins, and spill/backpressure policy over those
already typed plans.

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

The same prefixed path model applies to all multimodal derived indexes. A
full-text config with `field = "attrs.title"` tokenizes that embedded JSON
field. A managed embedding config with `field = "attrs.description"` reads text
from the committed JSON cell, runs the configured enrichment, and publishes an
embedding artifact under the index generation. An external vector config with
`field = "attrs.embedding"` validates that the JSON value is a numeric array
with the configured dimension before writing vector index state. A graph config
may reference `attrs.source`, `attrs.target`, `attrs.edge_type`, and
`attrs.weight` as scalar JSON paths. Algebraic/path-fact projection uses the
same embedded schema and dynamic-template rules to decide which JSON paths are
fact-bearing. In every case, the base row remains authoritative and the derived
artifact is disposable.

Write execution follows the ordinary relational write boundary:

1. Validate the closed top-level relational row.
2. Store the packed relational row and relational column entries.
3. For each declared `json` column, project permitted embedded paths using the
   compiled `FieldRef` resolver.
4. Feed those projected values to existing full-text, embedding, graph,
   algebraic, JSON path, and JSON value index builders.
5. Publish only after the derived generation has caught up to the committed
   base-row generation.

Read execution intersects derived JSON candidates with relational predicates
and then rechecks/materializes from the authoritative row. For example,
`WHERE attrs->>'plan' = 'pro' AND status = 'active'` lowers to a
`json_path_eq(field = "attrs", path = "plan", value = "pro")` predicate plus a
relational scalar predicate on `status`. Search over `attrs.title` uses the
full-text index field `attrs.title`, intersects any top-level relational
filters by document id, and hydrates `include_stored` from the packed row.

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
- derived-index field references must resolve to either a declared relational
  column or a permitted path below a declared `json` column;
- full-text and managed-embedding JSON paths must resolve to text-compatible
  values;
- external vector JSON paths must resolve to numeric arrays with the configured
  dimension;
- graph JSON paths must resolve to keyword/text-compatible source and target
  values, with optional type and weight paths validated against string/numeric
  domains;
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

Lake algebraic materializations have two durable artifact shapes: grouped folds
for keyed rollups and expression folds for source-wide typed expressions such as
row counts, sums, minima, and maxima. Both are built from `RowSource`
column batches, encoded as algebraic artifacts, published through the artifact
store, and referenced from manifest metadata as disposable derived state. A SQL
aggregate, migration backfill, or embedded-JSON algebraic projection therefore
targets the same native build/publish path that REST/SDK callers use; PostgreSQL
syntax can choose a materialization but cannot introduce a separate SQL-only
aggregate store.

Relational table creation and same-catalog relational schema updates both run
the same schema-aware index preparation: if no algebraic index exists,
`algebraic_index_v0` is added with `derive_from_schema: true` and stored as a
concrete derived config. Storage-mode switches are not schema updates; they need
an explicit row migration path.

### SQL surface for multimodal derived indexes

The relational SQL surface should expose full-text indexes, dense and sparse
AKNN indexes, managed enrichments, automatic graph construction, graph metrics,
and algebraic indexes through PostgreSQL-shaped DDL and query composition. The
durable model remains Antfly-native: SQL records catalog intent, validates
syntax, and lowers into the same derived index, enrichment, graph, metric,
algebraic, job, and artifact manifests used by REST and SDK callers.

This is intentionally not a direct clone of any one reference system:

- [pgvector](https://github.com/pgvector/pgvector) is the closest reference for
  external vector-column ergonomics. It keeps vectors as ordinary PostgreSQL
  columns, supports exact and approximate nearest-neighbor search, exposes
  distance operators such as L2, inner product, cosine, L1, Hamming, and Jaccard,
  and adds HNSW/IVFFlat indexes with tunables such as `m`, `ef_construction`,
  and `hnsw.ef_search`.
- [ParadeDB full-text search](https://docs.paradedb.com/documentation/full-text/overview)
  is the closest reference for Postgres-native full-text ergonomics: table data
  stays relational, text is tokenized by analyzers, ranked token/phrase/term
  queries are exposed through SQL, and vector similarity is treated as a
  separate ranked source that can be composed with full-text results.
- [Neo4j vector indexes](https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/)
  and [Neo4j full-text indexes](https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/full-text-indexes/)
  are useful references for making graph, vector, and full-text indexes
  first-class semantic indexes with scores. Neo4j also demonstrates hybrid
  retrieval by combining vector, full-text, and graph-ranked result sets, but
  Cypher should not be the first Antfly relational API target.
- [SurrealDB `RELATE`](https://surrealdb.com/docs/surrealql/statements/relate)
  is a useful reference for explicit edge records: edges have `in` and `out`
  endpoints, can hold properties, can be indexed, and support bidirectional
  traversal. [SurrealDB `DEFINE INDEX`](https://surrealdb.com/docs/surrealql/statements/define/indexes)
  is a useful multimodel indexing reference because it includes full-text,
  HNSW, and DISKANN-style vector indexes, but SurrealQL should not be embedded
  inside the PostgreSQL adapter.

The Antfly shape is therefore: borrow the SQL affordances that users already
expect from Postgres extensions, but lower them to Antfly's derived artifact
system instead of making PostgreSQL syntax the storage contract.

#### Ordinary full-text and external vector indexes

When the application owns a physical text or vector column, SQL should look
close to Postgres extension DDL:

```sql
CREATE INDEX docs_body_fts ON docs USING antfly_full_text (body)
  WITH (
    analyzer = 'standard',
    language = 'english'
  );

CREATE INDEX docs_embedding_hnsw ON docs
  USING hnsw (embedding vector_cosine_ops)
  WITH (
    m = 16,
    ef_construction = 64
  );
```

`antfly_full_text` lowers to a native full-text derived index over the `body`
column. A physical vector-column index lowers to an AKNN index whose source is a
user-supplied typed vector column. These paths are compatible with pgvector and
ParadeDB-style SQL because they operate on already-materialized table columns.
They do not create hidden enrichment jobs.

#### Embedded JSON field indexes

`json` / `jsonb` columns can be indexed as embedded document fields through the
same derived-index methods. The durable catalog does not distinguish a
document-mode field from a relational embedded JSON field after resolution; both
become a typed field path plus index-specific config. The difference is that a
relational embedded field is always re-read from the packed relational row and
is scoped below one declared JSON column.

Simple path syntax is preferred for Antfly-owned SQL:

```sql
CREATE INDEX docs_attrs_title_fts
  ON docs USING antfly_full_text (attrs.title)
  WITH (analyzer = 'standard');

CREATE INDEX docs_attrs_embedding_hnsw
  ON docs USING hnsw (attrs.embedding vector_cosine_ops)
  WITH (dimension = 1536);

CREATE INDEX docs_attrs_summary_semantic
  ON docs USING antfly_aknn (attrs.summary)
  WITH (embedding_name = 'attrs_summary_v1', model = 'text-embedding-3-small', dimension = 384);

CREATE INDEX doc_edges_attrs_graph
  ON doc_edges USING antfly_graph (attrs.source_doc, attrs.target_doc)
  WITH (type_field = 'attrs.edge_type', weight_field = 'attrs.confidence', edge_policy = 'all');
```

PostgreSQL JSONB expression syntax is accepted only when the adapter can reduce
it to the same static field path:

```sql
CREATE INDEX docs_attrs_title_fts
  ON docs USING antfly_full_text ((attrs->>'title'));

CREATE INDEX docs_attrs_plan_fts
  ON docs USING antfly_full_text ((jsonb_extract_path_text(attrs, 'billing', 'plan')));
```

The lowerer stores neither `attrs->>'title'` nor
`jsonb_extract_path_text(...)` as SQL text. It stores `field =
"attrs.title"` or `field = "attrs.billing.plan"` after catalog validation. Path
segments must be string literals or identifier path elements known at DDL time;
runtime-computed JSON paths, wildcard JSONPath expressions, and expressions that
return non-scalar objects for scalar-only index types fail closed.

This also gives REST/SDK callers the same surface. Index configs may reference
`attrs.title`, query filters may use native `json_path_eq` /
`json_path_exists` / `json_contains`, and projections may use typed JSON
extract nodes. SQL operators such as `->`, `->>`, `#>`, `#>>`, `@>`, and
`jsonb_extract_path[_text]` remain adapter sugar over those typed requests.

#### Managed AKNN indexes and automatic embeddings

For automatic embeddings, the SQL surface should not pretend that an embedding
column is authoritative row storage. It should create a managed derived index
with an attached enrichment:

```sql
CREATE INDEX docs_body_semantic ON docs USING antfly_aknn (body)
  WITH (
    embedding_name = 'body_embedding_v1',
    model = 'text-embedding-3-small',
    metric = 'cosine',
    chunk_size = 512,
    refresh = 'async'
  );
```

This lowers to:

- an `EmbeddingsIndexConfig` or equivalent dense/sparse AKNN config;
- an `EnrichmentConfig` that reads committed relational rows, chunks `body`,
  invokes the configured embedding model, and writes embedding artifacts;
- a build/catch-up job that publishes an index generation only after the
  enrichment artifacts needed by that generation are complete;
- readiness metadata that can distinguish `MetricNotReady` or
  `IndexNotReady` from an empty result set.

The physical table remains `docs`. The generated embedding is a derived artifact
addressed by index/enrichment metadata, not an implicit relational column unless
the user explicitly declares a generated column in a future API.

#### Graph indexes and automatic graph construction

Graph construction should live with the graph index. A relational table can
declare explicit edge columns, edge tables, or extraction-based edges, but each
path should lower to one native graph index manifest:

```sql
CREATE GRAPH INDEX docs_rel_graph ON docs
  SOURCE ENRICHMENT relations_v1
    FROM body
    USING extractor MODEL 'relations'
  EDGES JSON_PATH '$.relations[*]'
    SOURCE _id
    TARGET target.document_id
    TYPE type
    WEIGHT confidence;
```

For explicit edge tables, the SQL can use a Postgres-shaped table plus graph
index declaration:

```sql
CREATE TABLE doc_edges (
  source_doc text NOT NULL,
  target_doc text NOT NULL,
  edge_type text NOT NULL,
  confidence float8,
  PRIMARY KEY (source_doc, target_doc, edge_type)
);

CREATE GRAPH INDEX docs_edge_graph ON doc_edges
  EDGE (source_doc -> target_doc)
  TYPE edge_type
  WEIGHT confidence;
```

The extraction form lowers to an enrichment plus edge materializer. The explicit
edge-table form lowers directly from committed relational rows. In both cases,
the graph index owns the graph artifact generation, edge schema, traversal
metadata, and metric namespace. This follows the SurrealDB lesson that edge
records are first-class data, while keeping Antfly's graph storage as a derived
artifact rather than adding a second query language.

#### Graph metrics

Metrics such as PageRank, eigenvector centrality, degree, connected components,
or community labels should be attached to a graph index:

```sql
ALTER GRAPH INDEX docs_rel_graph
  ADD METRIC pagerank_v1
  USING pagerank
  WITH (
    damping = 0.85,
    max_iterations = 40,
    tolerance = 0.000001,
    edge_types = ARRAY['cites', 'references'],
    publish = 'after_max_iterations'
  );
```

The metric config belongs to the graph index because metric values are only
meaningful for a specific graph projection, edge filter, weighting policy, and
generation. Reads that require a metric before the first qualifying generation
is published return `MetricNotReady`, not an empty result. Fixed-iteration
non-converged PageRank publishes by default with `converged: false`,
`iterations_completed`, and `delta` metadata, matching the graph metric
roadmap's PageRank-first policy. If a future metric family needs strict
convergence-only behavior, it should opt out explicitly with documented
behavior rather than changing the default for PageRank-compatible metrics.

For the first version, old metric generations should be cleaned up immediately
after the replacement generation is published and no reader holds it. Long-term,
retention can become a graph-index option for debugging, audits, or rollback:

```sql
ALTER GRAPH INDEX docs_rel_graph
  SET (metric_generation_retention = 2);
```

#### Algebraic indexes

Algebraic indexes should stay schema-derived by default and should not become a
separate SQL aggregate store:

```sql
CREATE INDEX docs_algebraic ON docs USING antfly_algebraic
  WITH (
    derive_from_schema = true
  );
```

When users request explicit algebraic materializations, the catalog should lower
them to native grouped folds or expression folds over the committed `RowSource`.
That keeps SQL aggregates, migration backfills, embedded-JSON projections, REST
queries, and SDK queries on the same artifact path.

#### Query composition

The main SQL integration point should be table-valued functions and normal SQL
joins, not a new SQL planner that tries to inline every Antfly retrieval mode on
day one:

```sql
WITH semantic AS (
  SELECT id, score
  FROM antfly.semantic_search(
    index => 'docs_body_semantic',
    query => 'refund policy for enterprise plan',
    limit => 50
  )
),
graph AS (
  SELECT key AS id, metrics->>'pagerank_v1' AS pagerank
  FROM antfly.graph_traverse(
    index => 'docs_rel_graph',
    start => 'doc:root',
    max_depth => 2,
    require_metric => 'pagerank_v1'
  )
)
SELECT d.id, d.title, semantic.score, graph.pagerank
FROM docs AS d
JOIN semantic USING (id)
LEFT JOIN graph USING (id)
WHERE d.tenant_id = 't1'
ORDER BY semantic.score * 0.8 + graph.pagerank::float8 * 0.2 DESC
LIMIT 10;
```

Higher-level hybrid search can be a convenience function over the same native
fusion machinery:

```sql
SELECT *
FROM antfly.hybrid_search(
  table_name => 'docs',
  query => 'enterprise refund policy',
  sources => ARRAY[
    antfly.source('docs_body_fts', field => 'body', weight => 0.25),
    antfly.source('docs_body_semantic', weight => 0.60),
    antfly.source('docs_rel_graph', metric => 'pagerank_v1', weight => 0.15)
  ],
  fusion => 'rrf',
  limit => 20
);
```

That function lowers to the same REST/SDK fusion configuration used by non-SQL
callers: ranked source specs, `rrf`/`rsf`, optional failover, pruning, and
reranking. `sources => ARRAY[antfly.source(...)]` is the current SQL helper
surface; the adapter lowers it to a structured native source list before query
execution. `sources_json` remains an internal/compatibility bridge for callers
that already hold a typed source array. Neither form passes SQL text through the
execution boundary. The table-valued function result can then be joined back to
base rows for filtering, authorization, and projection.

#### Graph DSL influence

SurrealDB and Cypher should influence the relational graph API, but they should
not become the first relational query language surface. The first API should
stay PostgreSQL-shaped because relational rows remain the authoritative data
model and graph indexes remain derived artifacts.

From SurrealDB, Antfly should borrow the idea that edges are explicit,
property-bearing records. The SQL form is an edge table plus `CREATE GRAPH
INDEX`, not embedded SurrealQL:

```sql
CREATE TABLE doc_edges (
  source_doc text NOT NULL,
  target_doc text NOT NULL,
  edge_type text NOT NULL,
  confidence float8,
  created_at timestamptz,
  PRIMARY KEY (source_doc, target_doc, edge_type)
);

CREATE GRAPH INDEX docs_graph ON doc_edges
  EDGE (source_doc -> target_doc)
  TYPE edge_type
  WEIGHT confidence;
```

From Cypher, Antfly should borrow graph vocabulary and pattern concepts:
nodes, relationships, direction, path length, edge-type filters, path bindings,
and metric-aware traversal. The first SQL API should expose those concepts as
typed function arguments:

```sql
SELECT *
FROM antfly.graph_traverse(
  index => 'docs_graph',
  start => 'doc:root',
  direction => 'out',
  edge_types => ARRAY['cites'],
  max_depth => 2,
  require_metric => 'pagerank_v1'
);
```

Long-term, a small pattern DSL can be added as a convenience layer over the same
native graph planner:

```sql
SELECT *
FROM antfly.graph_match(
  index => 'docs_graph',
  pattern => '(a)-[:cites*1..2]->(b)',
  start => 'doc:root'
);
```

That DSL should be intentionally smaller than full Cypher at first: no separate
transaction model, no separate graph-owned storage model, and no planner path
that bypasses relational authorization or committed base-row visibility. It is
adapter sugar for graph traversal and path binding over a named graph index. If
Antfly later adds broader GQL/Cypher compatibility, it should be an additional
adapter on top of the same graph index and relational row contracts, not the
primary contract for relational graph data.

#### Operational contract

- Catalog application validates SQL names, column references, model references,
  edge extraction paths, graph metric names, and index-specific options before
  any asynchronous build starts.
- SQL DDL stores native configs in the Antfly catalog. It must not generate a
  backend SQL string as the source of truth.
- Backfill, catch-up, refresh, split, merge, replay, restore, and replication
  rebuild derived artifacts from committed relational base rows and schema
  metadata.
- Read APIs expose readiness explicitly. Required graph metrics return
  `MetricNotReady`; required derived indexes return the matching index readiness
  error; optional sources can be omitted according to the caller's fusion policy.
- Edge extraction and managed embedding jobs must be idempotent across crash and
  retry. Publishing a graph or AKNN generation is the visibility boundary.
- Freshness names should use operation-oriented options such as
  `require_fresh => true` or `freshness => 'published'`; `published` describes
  the artifact state, while `require_fresh` describes the read requirement.
- Graph indexes should require an explicit default edge policy in config. The
  long-term default can be `edge_types = 'all'`, but the catalog should store
  that decision explicitly so adding new edge types cannot silently change old
  metrics without a new generation.

#### Implementation order

1. Parse and catalog ordinary `antfly_full_text`, external-vector, and
   `antfly_algebraic` DDL as direct derived index configs.
2. Add `antfly_aknn` DDL that creates an enrichment plus AKNN config and exposes
   readiness through SQL table-valued functions.
3. Add explicit edge-table graph indexes, then extraction-based graph indexes.
4. Attach graph metrics to graph indexes with `MetricNotReady`, immediate old
   generation cleanup, and explicit edge policy recording.
5. Add hybrid table-valued functions that lower to the native fusion planner.
6. Only after those APIs prove insufficient, evaluate optional GQL/Cypher-like
   graph query support as an additional adapter, not as the primary relational
   contract.

Current implementation status:

- SQL DDL parsing covers `antfly_full_text`, external `hnsw`, managed
  `antfly_aknn`, `antfly_algebraic`, explicit edge-table `antfly_graph`, the
  first-class `CREATE GRAPH INDEX ... EDGE (...)` form,
  extraction-backed `CREATE GRAPH INDEX ... SOURCE ENRICHMENT ... EDGES ...`
  graph manifests, first-class
  `ALTER GRAPH INDEX ... ADD METRIC ... USING ...` graph metric attachment, the
  lower-level compatibility `antfly_graph_metric` form, and `antfly_hybrid`
  configs.
- Table-record catalog application now stores Antfly-derived index DDL in
  `indexes_json` instead of schema JSON, preserving the invariant that SQL
  records native Antfly derived-index configs rather than backend SQL strings.
- Catalog application validates derived field references against the current
  relational schema before writing metadata. Full-text, AKNN/enrichment fields,
  graph source/target/type/weight fields, and embedded JSON paths fail closed
  before async build work can start.
- Catalog application also validates derived-index dependencies before publish:
  graph metric configs must reference an existing graph index, hybrid source
  configs must reference existing index configs, and drops are rejected when
  another derived index would be left with a dangling `graph_index` or
  `sources` reference.
- Extraction-backed graph DDL stores the existing graph artifact-source manifest
  shape (`source.kind = artifact`, shorthand asset enrichment, node/edge
  templates) and validates the committed row source field before catalog write.
- Extraction materialization is wired through enrichment artifact production,
  graph edge artifact writes, graph artifact journal hints, and derived graph
  replay, so graph index builds no longer depend on clients writing `_edges`
  rows directly.
- Managed AKNN lifecycle is wired through generated enrichment precompute,
  `full_index` sync-level maintenance, published dense-index visibility,
  and readiness/status encoding for replay completion.
- Remaining long-term production work is distributed orchestration: remote
  worker lease/retry accounting, cross-node recovery evidence, and operational
  controls for large multi-index rebuilds.

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
secondary scan entries. The DB-backed portable logical AFB path is
schema-aware: it materializes packed relational rows into logical documents
during export and restores them through the DB write path so row, column, and
derived state are regenerated consistently. The lower-level raw DocStore
portable path still rejects relational physical rows rather than copying a
partial physical keyspace or silently exporting an empty document set.

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
