# SQL Adapter Design

This is the design document for Antfly's SQL adapter. It records the durable
architecture, ownership boundaries, phase contracts, and implementation rules
needed to keep SQL aligned with Antfly-native services.

Track remaining implementation work in `SQL_SLICES.md`. Do not use this file
as a progress log; completed history that can be deduced from code, tests,
fixtures, or git history should stay out of this document.

## Core Contract

Antfly SQL is an adapter over the relational, catalog, document, index, role,
extension, job, lake, backup, and storage services. It is not a second storage
model or a second control plane.

SQL text is compatibility syntax. Supported syntax must lower to typed
Antfly-native plans. Unsupported PostgreSQL-compatible syntax must fail closed
with a stable diagnostic that names the missing native model or semantic gap.

Raw SQL text exists only at external ingress boundaries: HTTP, SQL wire
protocol, CLI, MCP, A2A, fixtures, tests, and compatibility wrappers. Once a
request enters the adapter, internal phases carry typed objects:

```text
SQL bytes
  -> TokenizedSql
  -> ParsedSql
  -> BoundSqlStatement
  -> LogicalSqlPlan
  -> shared Antfly service request
```

The equivalent REST, SDK, MCP, A2A, CLI, job, or internal automation path must
reach the same native service contract with the same authorization,
idempotency, audit, retry, diagnostic, and failure semantics.

## Scope

The SQL adapter owns:

- SQL lexing, parsing, source spans, and syntax diagnostics.
- PostgreSQL-shaped DDL, DML, query, role, extension, tablespace, transaction,
  cursor, prepared-statement, and session syntax.
- SQL session state such as `current_database`, `search_path`, and
  `antfly.sync_level`.
- Name resolution from SQL identifiers to typed catalog targets.
- Binding and lowering into Antfly-native schema, row, document, index,
  extension, role, job, lake, backup, and storage plans.
- SQL/API parity fixtures, unsupported-shape classification, and
  compatibility no-op policy.

The SQL adapter does not own:

- Authoritative storage layout.
- Catalog metadata semantics or lifecycle transitions.
- Role and permission evaluation.
- Extension install/update/drop lifecycle.
- Derived-index build, catch-up, or readiness state.
- Row-expression execution semantics.
- Document storage semantics.
- Backup/restore scope.
- Tablespace placement enforcement.
- Durable job admission or retry state.

Those belong to shared Antfly services and must be callable by non-SQL
surfaces without passing SQL text through the boundary.

## Phase Boundaries

Each phase receives the previous phase's typed object plus explicit context.
Hidden access to raw SQL text is migration debt.

| Phase | Owns | Must not own |
| --- | --- | --- |
| Lexer/tokenizer | borrowed tokens, source spans, keyword metadata, literal spans | catalog names, typed JSON values, durable metadata |
| Raw parser | catalog-free raw AST, nested statement nodes, parse diagnostics | catalog lookup, privileges, storage plans |
| Binder | session state, catalog identity, object versions, schemas, privileges, dependencies, placement facts | SQL string rewriting, storage execution |
| Logical planner | typed read/write/DDL/session/role/extension/job/lake/backup intent | backend-specific storage keys, compatibility SQL strings |
| Service bridge | typed calls into row, catalog, index, role, extension, job, lake, backup, and storage services | parser probes, catalog-free name guessing |
| Fixture tooling | structured summaries and coverage bits emitted by phases | substring scans that behave like another parser |

`ParsedSql` is the only parse product. It owns the token stream, keyword
metadata, raw AST, nested statement references or token ranges, source byte
spans, and parse diagnostics for one input.

`BoundSqlStatement` is the first catalog-aware object. It combines the parsed
statement with explicit SQL session state, resolved catalog targets, object
versions, schemas, dependency facts, placement facts, and role authorization.

`LogicalSqlPlan` is the only planner output. It expresses Antfly-native read,
write, DDL, session, transaction, role, extension, index, document, lake,
backup, storage, or job intent as typed structs.

Shared services are the only durable owners. SQL may parse, bind, authorize,
and build requested plans, but it must not independently persist catalog state,
schedule background work, store extension metadata, mutate roles, apply
tablespace placement, route storage, or commit document state.

## Parser And Grammar

Antfly SQL aims for PostgreSQL-compatible behavior at the user and API surface
while owning its grammar, AST, lowering, and execution semantics. PostgreSQL
and CockroachDB grammar behavior are compatibility references, not vendored
runtime parser code.

The generated grammar migration plan and parser-specific design live in
[`pkg/antfly/src/sql/grammar/GRAMMAR.md`](pkg/antfly/src/sql/grammar/GRAMMAR.md).

Parser rules:

- Parse once at ingress and preserve source byte spans through diagnostics.
- Keep raw parsing catalog-free.
- Dispatch by typed statement-family variants, not lowerer probe order.
- Represent nested SQL structurally. `EXPLAIN`, CTEs, subqueries, relation
  population, `INSERT ... SELECT`, `UPDATE ... FROM`, `DELETE ... USING`,
  `MERGE`, and future embedded statements should carry child raw nodes or
  source token ranges, not reconstructed SQL strings.
- Preserve JSON and JSONB literals as source spans until semantic phases need
  typed JSON values.
- Keep unsupported classifications cheap and allocation-light.

## Catalog And Session Semantics

SQL object names resolve through the same `database / namespace / table` model
as explicit REST routes.

```sql
CREATE TABLE users (...);
-- current_database.public.users

CREATE TABLE analytics.users (...);
-- current_database.analytics.users

CREATE SCHEMA analytics;
-- creates namespace analytics in current_database

CREATE DATABASE tenant_ops;
-- creates database tenant_ops
```

Two-part names are namespace-qualified names inside the current database.
PostgreSQL three-part references remain fail-closed until Antfly has explicit
cross-database authorization and execution semantics.

`search_path` is SQL session state, not a string rewrite. `SET search_path`,
`RESET search_path`, `SHOW search_path`, and `DISCARD ALL` apply through typed
session plans. Later resolution reads that session object and resolves
unqualified names through the ordered namespace list. The default namespace is
`public`.

`current_database` is an explicit session input until Antfly owns durable
server-side connection sessions. Protocols without SQL sessions must pass
database defaults through the shared catalog resolver.

`SET antfly.sync_level` and `SET LOCAL antfly.sync_level` are SQL syntax over
the native row-write sync-level enum. The effective value is applied when a
lowered write plan is built. It is not catalog DDL and is not stored as SQL
text.

Embedded and Lite SQL use the same parser, binder, session object, typed
lowerers, and SQL response envelope as server SQL. Lite is a storage backend,
not a dialect. Distributed-only behavior must fail closed or be omitted from
Lite capabilities.

## Public SQL Ingress

The HTTP SQL ingress is `POST /db/v1/sql`. It belongs under the database API
version because SQL is a database control and data surface, not a separate
product namespace.

Each request carries one SQL statement plus an optional logical `session_id`.
Each response returns the session id that clients should reuse when they need
prepared statements, LISTEN/NOTIFY state, search-path defaults, or later portal
state. Prepared statements and cursors are SQL session state, not durable REST
resources.

Cursor-backed fetches and asynchronous statement jobs can be layered on only
after Antfly has native portal lifetime, result paging, cancellation, and
job-status contracts. Until shared read/write execution derives table-level
permissions from lowered typed plans, authenticated HTTP SQL requires
database-admin permission on the default database.

## DDL And Lifecycle

SQL DDL is a compatibility frontend over typed lifecycle services:

- Table, database, namespace, and schema DDL produce catalog lifecycle plans.
- Tablespace DDL produces catalog placement-policy plans.
- Index and Antfly-derived artifact DDL produce native derived-index configs
  and lifecycle jobs.
- Extension DDL routes to the extension lifecycle service.
- Role DDL routes to Antfly user and role management.
- Long-running `ALTER` work routes through durable job admission owned by the
  catalog or metadata owner.

Autocommit metadata DDL is the initial safe shape for lifecycle operations that
span metadata, derived artifacts, and shard convergence. DDL inside user data
transactions remains fail-closed until Antfly has one transactional boundary
for SQL DDL, catalog transitions, derived jobs, and storage visibility.

Adapter-only no-ops are allowed only when they have a named compatibility
reason and cannot change durable catalog, document, or row state.

## DML And Query Lowering

SQL DML lowers into the same typed row APIs as REST and SDK callers:

- Point inserts, updates, deletes, `ON CONFLICT`, defaults, generated columns,
  triggers, row filters, and `RETURNING` use row-batch and shared
  row-expression plans.
- `INSERT ... SELECT`, `MERGE`, `UPDATE ... FROM`, and `DELETE ... USING` use
  typed source, joined-source, claimed mutation, and 2PC paths.
- Joins, bounded lateral joins, CTEs, aggregates, windows, `HAVING`,
  `ORDER BY`, `DISTINCT`, pagination, and projection expressions lower into
  public row-plan families.
- Row filters and role-level predicates are applied through the same access
  and expression predicate model as native callers.
- Sequence defaults and PostgreSQL-shaped sequence functions route through the
  metadata-owned sequence catalog and SQL session state.

The adapter must never pass provider-specific SQL strings into storage
execution. Foreign-source queries, lake reads, backup/restore, provisioned
tables, hosted tables, and local tables all receive typed catalog targets and
typed row plans.

## Document Table SQL

SQL can read and write document tables only by lowering to native document
plans. Document SQL must not weaken relational invariants or route document
state through relational row batches.

Document table creation uses ordinary `CREATE TABLE` with an explicit Antfly
storage profile and `DOCUMENT SCHEMA` clauses:

```sql
CREATE TABLE docs
WITH (
  antfly.storage_mode = 'document',
  antfly.default_type = 'doc'
)
DOCUMENT SCHEMA doc AS JSON '{
  "type": "object",
  "properties": {
    "title": {"type": "text"},
    "body": {"type": "text"},
    "status": {"type": "keyword"},
    "amount": {"type": "numeric"},
    "metadata": {"type": "json"}
  },
  "required": ["title"],
  "additionalProperties": true
}';
```

That lowers to native table schema metadata with `storage_mode = document`,
`default_type`, and JSON Schema-backed `document_schemas`. Multiple document
types use repeated `DOCUMENT SCHEMA` clauses and must set `default_type` to one
declared type.

The relational column-list form remains reserved for relational tables. A
future compact document-schema shorthand may be added only as syntax sugar that
lowers to the same JSON Schema-backed document plan.

### Document Source Binding

Source binding chooses one source family from catalog facts before semantic
lowering:

```zig
const SqlSourceBinding = union(enum) {
    relational: RelationalBinding,
    document: DocumentBinding,
    lake: LakeBinding,
};
```

Relational bindings continue through row lowerers. Document bindings route to
document SQL lowerers that produce native document/query/index plans. Lake
bindings route to lake-native row-source plans.

No lowerer should probe another source family by trial and error. A statement
that references more than one source family must fail closed unless there is an
explicit typed cross-source plan.

Document binding owns:

- resolved catalog target and runtime schema
- virtual SQL schema for `_id`, `_doc`, declared fields, and durable path
  metadata
- document SQL capability facts derived from ready native producers
- bounded-scan policy, when configured

Capability bits are an access-path inventory, not the language definition.
`typed_paths` can prove SQL comparison type semantics and virtual field shape,
but it is not a managed scalar index and does not prove producer readiness.

### Document Virtual Schema

Document SQL exposes a virtual schema:

- `_id` as the document key
- `_doc` as the full JSON document
- declared document-schema fields
- durable virtual-field and `typed_paths` metadata
- explicit JSON-path projections for nested fields

The virtual schema is built from durable facts in priority order: declared
document schema, index definitions and derived-index field paths,
`typed_paths` metadata for shape/type proof, explicit SQL views over document
tables, and observed statistics only as advisory fallback.

Sampling can improve UX but must not become durable semantics. Stable BI,
agent, or SQL access requires table schema, index definition, SQL view, or
`typed_paths` metadata.

Type mapping stays conservative:

| Document value | SQL-facing type |
| --- | --- |
| document id | `text` or stable Antfly `document_id` domain |
| string | `text` |
| boolean | `boolean` |
| integer | `bigint` when lossless |
| floating number | `double precision` |
| object | `jsonb` |
| array | `jsonb` unless explicitly unnested |
| mixed/unknown | `jsonb` or nullable inferred scalar only through a view |

Declared fields use declared nullability. Indexed, typed-path, or statistical
fields are nullable unless a durable schema constraint proves otherwise.

### Document Query Rules

Document SQL reads must lower to exact native producers or policy-bounded
plans with equivalent logical results. A producer's presence should make a
query cheaper, lower latency, or admissible under policy; its absence should
make the statement fail closed until another exact producer or bounded policy
can preserve semantics.

Supported document read expansion should remain deliberate:

- `_id`, `_doc`, declared field, and JSON-path projection
- `SELECT *` expansion over the virtual document schema
- table or alias qualification
- `_id` equality and membership lookup
- scalar predicates only where declared field, virtual metadata, and producer
  readiness preserve exact semantics
- bounded scans only through explicit bounded-scan policy
- ordering, aggregation, and `UNNEST` only when backed by exact native/indexed
  producers, materialized sidecars, or explicit bounded contracts

The following stay fail-closed until their native semantics are designed and
proven: broad scans without bounded policy, unsupported joins, windows,
recursive CTEs, set operations, data-modifying CTEs, locking tails, broad
ordered scans, and aggregate families without exact native or bounded
producers.

### Document Write Rules

Document SQL writes are admitted only when they lower to typed native document
insert, upsert, patch, or delete requests. SQL must not decompose documents
into relational rows or route document writes through mutation-source plans.

The base write surfaces are:

```sql
INSERT INTO docs (_id, _doc)
VALUES ('doc-1', '{"title":"Hello","status":"draft"}'::jsonb);

INSERT INTO docs (_doc)
VALUES ('{"title":"Hello","status":"draft"}'::jsonb);

DELETE FROM docs WHERE _id = 'doc-1';

UPDATE docs
SET _doc = antfly.json_patch(_doc, '{"status":"published"}'::jsonb)
WHERE _id = 'doc-1';
```

Projection-column writes are an ergonomic layer over native document requests,
not a relational write path. Writable projection columns are `_id` for insert
identity only, declared document-schema fields, and explicitly declared
writable virtual aliases that map to exactly one JSON pointer with a durable
type. `_doc` remains the full-document/patch surface. `_version`, generated
fields, observed fields, statistical fields, and index-only paths are read-only.
The admitted projection subset is `INSERT ... VALUES` over declared fields,
with either an explicit `_id` column or a generated document id. It materializes
a JSON document and lowers to the same native document insert request as `_doc`
inserts. Projection `INSERT ... VALUES` with an explicit `_id` may use
`ON CONFLICT (_id) DO NOTHING` or `ON CONFLICT (_id) DO UPDATE SET ...` when
the update action lowers to native document transform semantics: literal
declared-field assignments become transform operations, and `excluded.field`
assignments are materialized from the proposed document after a native lookup
selects insert, no-op, or version-predicated update behavior. A single
declared-field conflict target such as `ON CONFLICT (status)` is also admitted
when that field is a ready indexed scalar projection with unique cardinality
proof; runtime resolves the conflict row through a bounded native indexed
lookup and rejects duplicate matches. Guarded `DO UPDATE ... WHERE ...` actions
and deterministic expression assignments share the relational conflict
expression lowerer, then execute as native document transforms; a false guard
produces zero writes and no returned row.
Direct projection inserts may return `_id`, `_doc`, `_version`, and declared
projection fields. The admitted projection conflict subset may also return
`_id`, `_doc`, `_version`, and declared projection fields for inserted and
updated rows, including aliased JSON projection fields. Conflict update parity
covers insert-vs-update row counts, nested JSON projection returns, guarded
apply/skip behavior, expression assignment results, unique-field conflict
targets, schema validation failure mapping, and stale lookup version conflicts;
`DO NOTHING` conflicts that do not write return no row. Projection updates,
producer mutations, joined target/source mutations, and deletes may return
`_id`, `_doc`, `_version`, and declared projection fields once their target set
lowers to an admitted native producer. Insert, update, producer, joined, and
conflict `_version` rows are finalized from the committed document timestamp
after the write succeeds; delete `_version` rows report the pre-delete document
timestamp. Transform `RETURNING` rows reflect the post-transform document;
delete `RETURNING` rows reflect the pre-delete document.
Projection source-query inserts are admitted for same-table document sources
when the source producer lowers to an exact `_id` lookup, a ready indexed
scalar producer, or a bounded residual scan with row and byte caps:
`INSERT INTO docs (_id, ...) SELECT _id, ... FROM docs WHERE ...` lowers to a
native document source-insert batch when each assignment is a flat declared
projection field and no conflict action is present. An optional source `LIMIT`
caps materialized rows after residual filtering. Source producers reject
duplicate source ids instead of applying the same target twice. Source-query
projection inserts may return `_id`, `_doc`, `_version`, and declared
projection fields for materialized rows; `_version` is finalized from the
committed document timestamp after the batch write succeeds. Source-query
projection inserts without an `_id` target allocate target ids through the same
native generated document id helper used by direct generated-id document
inserts; runtime sorts materialized source ids before allocating generated
target ids so allocation order is deterministic for exact, indexed, and bounded
source producers. Ordered sources, nondeterministic sources, nested target
paths, `_doc`/`_version` source projection writes, and conflict actions remain
fail-closed until their native semantics are separately proven. Document
`MERGE` `RETURNING`, compound or expression conflict targets, partial unique
targets, and named non-primary constraints also remain fail-closed. Unsupported
document `RETURNING` shapes report specific stable diagnostics for `*`,
duplicate output names, expressions, generated fields, and unsupported
virtual/projection fields.

Projection writes must preserve native document semantics for missing fields,
JSON null, generated/defaulted fields, nested object construction, array paths,
type validation, `additionalProperties`, conflict/no-match/stale-version
reporting, authorization, row filters, audit-required ordering, and write
conflicts.

Plain `TRUNCATE docs` is admitted for document tables through the catalog-owned
table-emptying barrier. It schedules native document range deletion and rejects
guarded row-filtered identities before a table-emptying job is created. Public
SQL emits structured audit records for both applied table-emptying scheduling
and denied guarded truncate attempts.
Document `TRUNCATE` variants that require catalog expansion or allocator
mutation remain rejected: multi-table truncate, `CASCADE`, and
`RESTART IDENTITY`. `RESTART IDENTITY` stays unsupported until the catalog
barrier and document-id allocator can prove no id reuse across in-flight jobs,
replication, stale clients, and generated-id namespaces.

Document-table `MERGE` is admitted for bounded document-table producers whose
branches lower to native document writes. The admitted subset evaluates
`WHEN MATCHED` and `WHEN NOT MATCHED` branches in SQL order, rejects duplicate
source join values, materializes matched `UPDATE`, matched `DELETE`, matched
`DO NOTHING`, not-matched `_id`/`_doc` copy inserts, and not-matched
`DO NOTHING`, and emits native document batch writes, transforms, and deletes.
Matched update/delete branches attach lookup-backed target version predicates
to the native batch, and target/source `_version` branch predicates compare
against lookup metadata rather than document JSON. Public SQL records
MERGE-specific audit outcomes for applied rows-batch writes, denied row-filter
attempts, and failed write conflicts.
Expression assignments, expression predicates, unsupported projection inserts,
`RETURNING`, relational/data-modifying source plans, and unbounded producers
remain fail-closed.

All other document-table `INSERT`, `UPDATE`, `DELETE`, view-target writes, and
data-modifying CTE shapes remain fail-closed until they have a typed native
document request, diagnostics, and SQL/native parity.

## Expressions

The shared row-expression AST is the boundary for supported SQL expressions.
Checks, generated columns, partial predicates, expression indexes, conflict
actions, update transforms, aggregate filters and inputs, `HAVING`, order keys,
window inputs, `RETURNING`, document residual predicates, and row-rewrite
`USING` expressions should converge on one typed AST.
The executable contract is `sql/expr/contract.zig`: it names the durable
`RelationalRowsExpression` and `RelationalRowsExpressionCondition` types, the
published text, JSON, array, regex, datetime, numeric, boolean, and
query-function expression families, and the SQL surfaces that must bind through
that shared path.

PostgreSQL JSON syntax such as:

```sql
attrs->>'title'
attrs #>> '{billing,plan}'
jsonb_extract_path_text(attrs, 'billing', 'plan')
```

must lower to typed JSON field references and extraction nodes. Durable
metadata stores native `FieldRef` or expression nodes, not SQL spelling.

Unsupported, volatile, ambiguous, dynamic, or catalog-dependent expressions
fail before catalog application or row execution.

## Derived Index SQL

SQL can expose PostgreSQL-shaped DDL for Antfly-derived artifacts while keeping
the durable model native:

- `USING antfly_full_text` for full-text indexes.
- `USING hnsw` or `USING antfly_aknn` for external vector or managed embedding
  indexes.
- `USING antfly_algebraic` for schema-derived algebraic facts.
- `CREATE GRAPH INDEX` and `ALTER GRAPH INDEX ... ADD METRIC` for graph
  projections and graph metrics.
- Hybrid search helper functions that lower to native ranked-source fusion.

Embedded `json` / `jsonb` fields on relational rows participate through the
same field-path resolver as document-derived indexes. Full-text, AKNN, graph,
algebraic, embedding, and hybrid configs validate paths against table schema
before writing catalog metadata.

See [RELATIONAL.md](RELATIONAL.md) for the relational field and derived-index
model.

## Extensions And Roles

`CREATE EXTENSION`, `ALTER EXTENSION ... UPDATE`, and `DROP EXTENSION` are SQL
syntax over the extension lifecycle service. SQL extension names resolve
through the package catalog and manifest-declared SQL aliases. Version, digest,
capability, dependency, and member tracking live in extension metadata, not SQL
text.

Role SQL is syntax over Antfly user/role management:

- Role creation, rename, drop, membership, grants, and revokes update the
  native role service.
- Qualified resources use `database:name`, `namespace:db.ns`, and
  `table:db.ns.table`.
- `ALTER ROLE ... SET ...` is durable only for settings backed by a native role
  setting model. Unsupported settings fail closed instead of being stored as
  opaque PostgreSQL configuration text.

## Performance Requirements

The performance goal is to reduce duplicated parse/lower work and transient
allocation churn while keeping parser behavior deterministic.

- Measure tokenization time, parse time, lowering time, fixture
  encode/check time, allocation count, and allocated bytes per statement
  family.
- Use one reusable `TokenizedSql` / `ParsedSql` object so classification,
  lowering, `EXPLAIN`, fixture summaries, and catalog application share the
  token stream and raw AST.
- Use resettable per-statement arenas for parser, lowerer, fixture, and
  classifier scratch allocations.
- Move keyword and identifier normalization into token metadata.
- Defer JSON literal parsing and stringify work until semantic phases need
  typed JSON.
- Replace SQL/string scans in coverage and fixture checks with structured
  summaries or coverage bits.
- Keep unsupported classifications cheap and allocation-light.

## Diagnostics And Testing

Errors should be diagnostic objects from the phase that detected them, not
opaque lowerer failures. Parser, binder, planner, and executor diagnostics
should carry:

- stable SQLSTATE or Antfly error code
- phase (`parse`, `bind`, `plan`, or `execute`)
- source byte span
- message
- optional hint

SQL compatibility is proven by typed outcomes, not by accepting more syntax
than the engine can execute. The parity suite should assert:

- supported statements lower to typed native plans or execute through shared
  services
- unsupported statements include the missing native model feature
- adapter-only no-ops are explicit and reasoned
- SQL and native API requests produce the same row, document, schema, index,
  role, catalog, job, and extension behavior
- session state affects name resolution only through typed SQL sessions
- bind parameters, aliases, CTE output schemas, result schemas, and catalog
  targets are checked structurally
- fixtures validate structured summaries and coverage bits, not arbitrary
  non-empty strings or SQL substrings

Focused CI should keep fast parser/lowerer targets, fixture freshness targets,
and broader cross-surface parity gates for HTTP, SQL, MCP, A2A, CLI, roles,
extensions, tablespaces, backup/restore, lakes, document SQL, and derived
indexes.

## Relationship To Other Docs

- [RELATIONAL.md](RELATIONAL.md) owns the relational table, row, expression,
  constraint, query, and derived-index model.
- [DATABASES.md](DATABASES.md) owns database, namespace, table, tablespace, and
  catalog-target semantics.
- [EXTENSIONS.md](EXTENSIONS.md) owns package and installed-extension
  lifecycle.
- [LAKES.md](LAKES.md) owns external lake row sources and sidecar artifact
  semantics.
- [pkg/antfly/src/sql/grammar/GRAMMAR.md](pkg/antfly/src/sql/grammar/GRAMMAR.md)
  owns generated SQL grammar design.

SQL should reference those contracts and lower into them. It should not fork
their semantics.
