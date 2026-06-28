# SQL Adapter

Antfly SQL is an adapter over the relational/catalog engine, not a second
storage model. SQL text should parse into raw SQL shapes, resolve through the
same database/namespace/table catalog model described in [DATABASES.md](DATABASES.md),
and lower into the typed row, schema, index, extension, role, job, and storage
plans described by [RELATIONAL.md](RELATIONAL.md).

The durable contract is Antfly-native. SQL syntax is compatibility sugar when it
can be reduced to those typed contracts, and it fails closed when a PostgreSQL
feature would require semantics the engine does not yet own.

## Scope

The SQL adapter owns:

- SQL lexing and parsing.
- PostgreSQL-shaped DDL, DML, query, role, extension, tablespace, and session
  syntax.
- Raw SQL AST construction and syntax diagnostics.
- SQL session state such as `current_database`, `search_path`, and
  `antfly.sync_level`.
- Name resolution from SQL identifiers to typed catalog targets.
- Lowering into Antfly-native schema, row, index, extension, role, job, and
  lifecycle plans.
- SQL/API parity fixtures, unsupported-shape classification, and adapter
  compatibility no-ops.

The SQL adapter does not own:

- Authoritative storage layout.
- Catalog metadata semantics.
- Role and permission evaluation.
- Extension install/update/drop lifecycle.
- Derived-index build, catch-up, or readiness state.
- Row-expression execution semantics.
- Backup/restore scope.
- Tablespace placement enforcement.

Those belong to shared Antfly services and must be callable by REST, SDK, MCP,
A2A, CLI, SQL, and internal jobs without passing raw SQL text through the
boundary.

## Pipeline

Use a PostgreSQL-inspired pipeline:

```text
SQL bytes
  -> lexer
  -> raw SQL AST
  -> statement-family classifier
  -> typed semantic/lifecycle plan
  -> catalog, row, extension, role, job, or storage service
```

The raw parser should be catalog-free. It may validate syntax and produce a raw
tree, but it should not read table metadata, role grants, extension state, or
range ownership. That keeps parse behavior deterministic and lets unsupported
statements be classified before any storage or metadata side effect is possible.

### PostgreSQL Compatibility And Grammar Ownership

Antfly SQL aims for PostgreSQL-compatible behavior at the user and API surface,
while owning its grammar, AST, lowering, and execution semantics. PostgreSQL and
CockroachDB grammar behavior should be used as compatibility references, not as
vendored runtime parser code. This keeps the language boundary aligned with
Antfly-native catalogs, document/relational storage, derived indexes, graph
metrics, Lite files, and typed service contracts.

The current hand-written parser remains the production path until a generated
grammar can prove parity statement family by statement family. The desired
long-term shape is Cockroach-style: a generated Antfly SQL parser over an
Antfly-owned grammar, a SQL-aware scanner, Antfly AST nodes, typed lowering,
and explicit unsupported-shape diagnostics. The migration plan and generator
performance requirements live in
[`pkg/antfly/src/sql/grammar/GRAMMAR.md`](pkg/antfly/src/sql/grammar/GRAMMAR.md).

## Design Summary

The long-term production shape is a phase-separated SQL frontend over typed
Antfly services. SQL text is accepted at external ingress only; after that, the
adapter carries typed objects forward:

```text
SQL text
  -> TokenizedSql
  -> ParsedSql
  -> BoundSqlStatement
  -> LogicalSqlPlan
  -> shared Antfly service request
```

Each phase has a narrow job:

- `TokenizedSql` tokenizes once, records source spans, and attaches keyword
  metadata without semantic allocation.
- `ParsedSql` builds a catalog-free raw AST, including nested statement nodes
  for `EXPLAIN`, CTEs, relation population, `INSERT ... SELECT`, and future
  embedded statements.
- `BoundSqlStatement` applies SQL session state, `current_database`,
  `search_path`, catalog identity, object versions, schemas, dependency facts,
  and role authorization.
- `LogicalSqlPlan` expresses Antfly-native read, write, DDL, role, extension,
  job, lake, backup/restore, and session intent.
- Shared services own durable effects. The SQL adapter never independently
  commits catalog state, schedules lifecycle jobs, stores extension metadata,
  mutates roles, applies tablespace placement, or routes storage through raw
  SQL strings.

The metadata/catalog owner should admit lifecycle jobs and commit durable
metadata transitions for SQL and non-SQL callers alike. SQL can parse, bind,
authorize, and build the requested lifecycle plan, but the same job,
authorization, audit, idempotency, retry, and compare-and-swap path must serve
REST, SDK, MCP, A2A, CLI, internal automation, and SQL.

New SQL work should be accepted only when it adds the full typed route:
raw AST coverage, binder coverage, native logical plan coverage, span-aware
diagnostics, and parity tests from structured summaries or coverage bits.
Syntax-only support, durable SQL-string metadata, probe-based dispatch, and
fixture scans that behave like a second parser are compatibility debt.

Semantic planning is the first catalog-aware phase. It receives the raw AST,
SQL session state, authenticated principal, and a catalog snapshot. It resolves
names, checks permissions through the shared role system, type-binds
expressions, and produces one typed native plan family.

Execution consumes typed plans only:

| SQL family | Native execution target |
| --- | --- |
| `SELECT`, `WITH`, joins, lateral, aggregate, window | Typed row-plan APIs and `TableReadSource` vtables |
| `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` | Row batch, insert-source, mutation-source, claim, and 2PC paths |
| `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE` | Catalog schema lifecycle and durable validation/rewrite jobs |
| `CREATE INDEX`, graph/AKNN/full-text/algebraic DDL | Derived index lifecycle, enrichment, graph, metric, and artifact jobs |
| `CREATE DATABASE`, `CREATE SCHEMA` | Database and namespace catalog lifecycle |
| `CREATE TABLESPACE`, `ALTER/DROP TABLESPACE` | Tablespace catalog lifecycle and placement policy validation |
| `CREATE/ALTER/DROP EXTENSION` | Extension lifecycle service |
| `CREATE/ALTER/DROP ROLE`, `GRANT`, `REVOKE` | Antfly user/role management service |
| `SET`, `RESET`, `SHOW`, `DISCARD ALL` | SQL session plan application |

The first public HTTP SQL ingress should be `POST /db/v1/sql`. It belongs
under the database API version because SQL is a database control and data
surface, not a separate product namespace. The endpoint is intentionally
psql-shaped: each request carries one SQL statement plus an optional logical
`session_id`, and each response returns the session id that should be reused by
clients that need prepared statements, LISTEN/NOTIFY state, search-path
defaults, or later portal state. Prepared statements and cursors are SQL
session state, not durable REST resources, so the initial API should not expose
`/statements`, `/prepared-statements`, or `/cursors` resources. Cursor-backed
fetches and asynchronous statement jobs can be layered on later when Antfly has
native portal lifetime, result paging, cancellation, and job-status contracts.
Until shared read/write execution derives table-level permissions from lowered
typed plans, the endpoint requires database-admin permission on the default
database when authentication is enabled.

Read statements are already routed through that ingress when the adapter can
lower them to Antfly row plans. The HTTP handler resolves the logical SQL
session's database/search path, loads the table schema from the catalog, lowers
`SELECT`, set-operation, recursive CTE, aggregate, window, join, and lateral
plans into the shared typed read executor, and returns the normal JSON
relational rows response under a SQL session envelope.

Point write statements are also routed through that ingress when they lower to a
single typed row-batch mutation: `INSERT ... VALUES`, primary-key `UPDATE`, and
primary-key `DELETE` use the same row-batch validation, row-filter checks,
transaction commit path, and `RETURNING` projection encoder as the public JSON
rows APIs. Non-recursive `INSERT ... SELECT` executes through the typed
insert-source path by reading the source side through the row-plan executor,
building a typed row batch from insert assignments and conflict policy, applying
target row filters, and returning the mutation-source `matched`/`staged` counts
plus `RETURNING` rows. Safe `BEFORE INSERT` SQL trigger hooks run before the
SQL-lowered row batch reaches row filters or commit; SQL update/delete
statements fail closed when table update/delete triggers are present until
their executor can share the preimage trigger path. Claimed single-table
update/delete sources now lower into typed mutation-source requests, mint a SQL
row-claim transaction, execute through the same owner-local mutation-source
planner/stager, commit the staged intents, and return mutation-source
`matched`/`staged` counts plus `RETURNING` rows. Non-recursive joined
update/delete sources (`UPDATE ... FROM` and `DELETE ... USING`) use the same
typed path: SQL executes the side-table read as a row plan, feeds the materialized
source rows into the joined mutation-source planner/stager, and autocommits or
aborts the SQL-owned row-claim transaction before returning mutation-source
counts and `RETURNING` rows. Plain single-table `TRUNCATE` and explicit
`TRUNCATE ... CONTINUE IDENTITY RESTRICT` use the typed table-emptying
mutation-source path with the SQL-owned row claim; multi-table truncate,
`CASCADE`, and `RESTART IDENTITY` remain fail-closed until their executor
wiring can share the native cross-table catalog barrier, identity allocator,
and 2PC paths. Recursive CTE-backed claimed update/delete statements
execute by materializing the bounded recursive producer through the typed read
planner, filtering the referenced CTE output, and feeding those rows into the
same joined mutation-source autocommit path. Direct table-source MERGE,
non-recursive source-CTE MERGE, and recursive CTE-backed MERGE execute by
collecting typed target/source preimages, building the deterministic row-batch
mutation, and applying that batch through the ordinary public row-batch
authorization, trigger, transaction, and commit path. Data-modifying-CTE MERGE
producers remain fail-closed until their materialized write output can be bound
to row claims and fed through the same batch boundary without adding a SQL-text
execution path.

Raw SQL text must not be stored as index metadata, role metadata, extension
metadata, row rewrites, or storage requests. If a feature needs durable
expression behavior, lower it to the shared row-expression AST first.

## Production Design Decision

The production architecture should treat SQL as a typed compiler frontend, not
as an alternate control plane. PostgreSQL syntax is the compatibility language,
but the durable source of truth is the same Antfly-native catalog, storage,
role, extension, job, lake, backup, and derived-index model used by every other
API surface.

The design decision is:

```text
external SQL text
  -> one TokenizedSql
  -> one ParsedSql
  -> one BoundSqlStatement
  -> one LogicalSqlPlan
  -> shared Antfly service owner
```

Only the first step should know about raw request text. After `ParsedSql`,
internal APIs should carry typed parser nodes, source spans, resolved catalog
targets, session state, authorization results, and native lifecycle or row plans.
Compatibility wrappers may remain at HTTP, SQL wire, CLI, MCP, A2A, fixture,
and test ingress points, but they should immediately construct or receive
`ParsedSql` and then delegate to the shared binder/planner path.

The SQL adapter owns syntax, spans, session interpretation, binding, and
lowering. It does not own durable behavior. Durable effects are owned by the
same services that serve non-SQL callers:

- catalog/database/namespace/table/tablespace DDL goes through the catalog
  lifecycle service;
- row reads and writes go through typed row plans and storage vtables;
- role SQL goes through Antfly user and role management;
- extension SQL goes through extension package lifecycle;
- index SQL goes through derived-index lifecycle and artifact jobs;
- backup/restore and lake or foreign-source SQL goes through typed storage and
  source services;
- long-running `ALTER` work goes through the shared durable job system admitted
  by the catalog or metadata owner.

The metadata leader, or the component that owns the equivalent consensus-backed
catalog transition, is the only place that should commit metadata state changes
or durable lifecycle job records. SQL may validate and prepare the request, but
it should not independently persist catalog state, schedule background work, or
write placement metadata.

New SQL work should meet these production rules before it is considered
complete:

- parse once and preserve source byte spans through diagnostics;
- dispatch by typed statement-family variants, not by lowerer probe order;
- bind catalog-aware statements through explicit SQL session state, catalog
  snapshots, object versions, dependency checks, and role authorization;
- lower durable effects into Antfly-native request structs, never durable SQL
  fragments;
- return phase-aware parse, bind, plan, or execute diagnostics that name the
  missing native model for unsupported shapes;
- validate fixtures and parity through structured summaries or coverage bits
  emitted by parser, binder, planner, and lowerer phases, not substring scans
  that behave like a second parser.

This shape is intentionally stricter than accepting PostgreSQL-looking syntax.
A statement family is production-ready only when SQL and the equivalent REST,
SDK, MCP, A2A, CLI, internal job, or automation path reach the same native
service contract with the same authorization, idempotency, audit, retry, and
failure semantics.

### Recommended Production Shape

The best long-term design is to make SQL a thin, typed frontend over Antfly's
native services. The adapter should be responsible for syntax, spans,
PostgreSQL-compatible session interpretation, and conversion into native
intent. It should not become an alternate owner for catalog metadata, storage
routing, index lifecycle, role state, extension installs, backup state, lake
sources, or durable jobs.

The recommended architecture has four internal contracts:

1. `ParsedSql` is the only parse product. It owns the token stream, keyword
   metadata, raw AST, nested statement references, source byte spans, and
   parse diagnostics for one input.
2. `BoundSqlStatement` is the only catalog-aware SQL object. It combines the
   parsed statement with explicit SQL session state, resolved catalog targets,
   object versions, schemas, dependency facts, placement facts, and role
   authorization results.
3. `LogicalSqlPlan` is the only planner output. It expresses read, write, DDL,
   session, role, extension, index, job, lake, backup/restore, and storage
   intent as Antfly-native structs.
4. Shared Antfly services are the only durable owners. The catalog or metadata
   owner commits lifecycle transitions and admits durable jobs; row/storage
   services execute typed read/write plans; role, extension, index, lake, and
   backup services own their respective effects.

This keeps the dependency direction one-way:

```text
SQL text
  -> ParsedSql
  -> BoundSqlStatement
  -> LogicalSqlPlan
  -> shared Antfly service request

REST / SDK / MCP / A2A / CLI typed requests
  ----------------------------------------^
```

Feature work should be organized around statement-family cutovers rather than
one large parser rewrite. For each family, first add raw AST coverage, then
binding, then native planning, then service-owned execution, then structured
diagnostics and parity evidence. Compatibility wrappers may stay at ingress
while a family migrates, but they should be leaf adapters that immediately
construct or receive `ParsedSql`.

Review should reject new SQL support when it:

- stores SQL text as durable semantics;
- dispatches by substring scans, fingerprints, or lowerer probe order after a
  typed statement variant exists;
- bypasses `BoundSqlStatement` for catalog-aware behavior;
- schedules lifecycle work outside the metadata owner;
- adds fixture validation that reconstructs parser behavior from SQL strings;
- implements SQL-only behavior that the equivalent native API cannot reach.

The practical production bar is not broad PostgreSQL syntax acceptance. It is
that every supported statement reaches the same native Antfly service contract
as the corresponding REST, SDK, MCP, A2A, CLI, job, or internal automation
path, with the same authorization, idempotency, audit, retry, diagnostic, and
failure semantics.

### Production Adapter Design

The SQL adapter should be organized as a compiler frontend plus a service
handoff layer. Each statement family should move through the same typed
pipeline, and every phase boundary should make ownership explicit:

```text
external SQL request
  -> parseSql(sql) : ParsedSql
  -> bindSql(parsed, session, catalog, principal) : BoundSqlStatement
  -> planSql(bound) : LogicalSqlPlan
  -> executeSqlPlan(plan, services) : row result, metadata transition, or job
```

`parseSql` is the only place that accepts raw SQL text. It owns tokenization,
keyword metadata, source spans, raw AST nodes, nested statement structure, and
parse diagnostics. Parser output is intentionally catalog-free and must not
infer permissions, storage placement, JSON values, or durable catalog names.

`bindSql` is the first semantic boundary. It receives an explicit SQL session,
catalog snapshot, and principal, then resolves current database, search path,
catalog targets, object versions, source schemas, role grants, dependency
facts, extension identity, tablespace policy inputs, lake or foreign-source
identity, and backup/restore scope. Any statement that can touch catalog,
storage, role, extension, tablespace, lake, or backup state must pass through
binding before planning.

`planSql` converts bound statements into Antfly-native intent. Plans should be
closed unions by family: read, write, table/catalog DDL, derived index,
extension, role, session, transaction, job, lake/source, backup/restore, and
storage. A plan may reference source spans for diagnostics and provenance, but
it must not use SQL text as the durable semantic payload.

`executeSqlPlan` is a thin bridge into shared services. It should not contain
parser probes, catalog guessing, or SQL-specific metadata persistence. Durable
effects are owned by the same service that owns the equivalent non-SQL API:
row/storage vtables for reads and writes, the catalog or metadata leader for
database/namespace/table/tablespace lifecycle, derived-index services for index
builds, role management for grants and role settings, extension lifecycle for
package operations, lake/source services for external data, backup/restore
services for backup scopes, and the shared durable jobs system for asynchronous
validation or rewrite work.

This design gives every statement family a clear maturity ladder:

1. **Recognized**: statement family and unsupported shape are classified with a
   stable diagnostic.
2. **Parsed**: the family has raw AST coverage, source spans, nested statement
   nodes or token ranges, and no substring rediscovery in normal lowering.
3. **Bound**: catalog-aware forms require `BoundSqlStatement` with session,
   resolved identity, object versions, schemas, role checks, dependencies, and
   placement facts.
4. **Planned**: supported forms lower to typed Antfly-native logical plans; gaps
   fail closed with the missing native model named.
5. **Executed**: plans hand off to shared Antfly services, with metadata changes
   and durable jobs admitted only by the service that owns the state.
6. **Proven**: SQL and native REST/SDK/MCP/A2A/CLI paths share parity coverage
   through structured summaries or coverage bits, not SQL-string scans.

Statement-family migrations should follow this order:

1. Normalize ingress so HTTP SQL, SQL wire protocol, CLI, MCP, A2A, fixtures,
   tests, and internal helpers construct `ParsedSql` once.
2. Promote compatibility token scans into raw AST nodes for reads, writes, DDL,
   session commands, transactions, `EXPLAIN`, CTEs, relation population,
   `INSERT ... SELECT`, `UPDATE ... FROM`, `DELETE ... USING`, and `MERGE`.
3. Move catalog-sensitive resolution behind binder APIs that require explicit
   session, catalog snapshot, and principal inputs.
4. Replace lowerer probe order with `switch` dispatch over parsed statement
   variants and compact statement-kind enums.
5. Replace durable SQL fragments in metadata, job payloads, index definitions,
   role settings, extension state, backup scopes, and storage dispatch with
   typed plan fields.
6. Delete compatibility wrappers after the typed path has parity coverage for
   the same native service contract.

The review rule is simple: a new SQL feature is not production-shaped if it
adds another raw-string semantic bridge, bypasses `BoundSqlStatement` for a
catalog-aware operation, stores SQL as durable behavior, schedules lifecycle
work outside the metadata owner, or validates fixtures by reconstructing parser
behavior from strings.

## Catalog and Session Semantics

SQL object names resolve through the same `database / namespace / table` model
as explicit REST routes:

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

`search_path` is SQL session state, not a string rewrite. The adapter should
apply `SET search_path`, `RESET search_path`, `SHOW search_path`, and
`DISCARD ALL` through typed session plans. Later SQL catalog resolution reads
that session object and resolves unqualified names through the ordered namespace
list. The default namespace is `public`.

`current_database` is an explicit session input until Antfly owns a connection
protocol with durable server-side session state. Protocols without SQL sessions
must pass database defaults through the shared catalog resolver rather than
inventing header-only or protocol-local defaults.

`SET antfly.sync_level` and `SET LOCAL antfly.sync_level` are SQL syntax over
the native row-write sync-level enum. The effective value is applied when a
lowered write plan is built. It is not catalog DDL and is not stored as SQL
text.

### Embedded And Lite Sessions

SQL execution should bind to a storage-neutral database handle. Server HTTP,
SQL wire, CLI, embedded directory storage, and Lite `.aflite` storage should all
share the same parser, binder, session object, typed lowerers, and SQL response
envelope.

Lite does not get its own SQL dialect. A command such as
`antfly lite sql app.aflite -c "SELECT ..."` should be implemented as a thin
wrapper around the same executor used by `antfly sql --lite app.aflite`, with a
Lite-backed DB handle and local catalog defaults.

The default embedded/Lite SQL session should start with:

- `current_database = main`
- `search_path = public`
- local-only transaction semantics
- the same `antfly.sync_level` setting names as server SQL, mapped to native
  write sync levels where meaningful

Distributed-only session behavior must fail closed or be omitted from
capabilities. In particular, Lite should not advertise cross-node joins,
remote shard fanout, distributed transaction coordination, or cluster placement.
The C API capability bit `sql.embedded_exec` is true for the first Lite SQL
execution path: `antfly lite sql <db.aflite>` and
`antfly sql --lite <db.aflite>` run SQL directly against local storage instead
of requiring a localhost HTTP server.

## DDL and Lifecycle Rules

SQL DDL is a compatibility frontend over typed lifecycle services:

- Table and schema DDL produce catalog schema mutation plans with durable
  validation, rebuild, rewrite, and promotion jobs.
- Index DDL produces native derived-index configs and build/catch-up work.
- Extension DDL routes to the extension lifecycle service described in
  [EXTENSIONS.md](EXTENSIONS.md).
- Role DDL routes to Antfly role management and stores durable role settings
  only through the native role-setting model.
- Tablespace DDL routes to database catalog placement-policy metadata and
  validates supported native placement keys.

Autocommit metadata DDL is the initial safe shape for extension and catalog
lifecycle operations that span metadata, derived artifacts, and shard
convergence. DDL inside user data transactions should remain fail-closed until
Antfly has one transactional boundary for SQL DDL, catalog transitions, derived
jobs, and storage visibility.

Unsupported PostgreSQL features should be explicit classifications with the
missing native model feature, not silent no-ops. Adapter-only no-ops are allowed
only when they have a named compatibility reason and cannot change durable
catalog or row state.

## DML and Query Lowering

SQL DML lowers into the same typed row APIs as REST and SDK callers:

- Point inserts, updates, deletes, `ON CONFLICT`, defaults, generated columns,
  and `RETURNING` use row-batch and shared row-expression plans.
- `INSERT ... SELECT`, `MERGE`, `UPDATE ... FROM`, and `DELETE ... USING` use
  typed source, joined-source, and claimed mutation plans.
- Joins, bounded lateral joins, non-recursive CTEs, aggregates, windows,
  `HAVING`, `ORDER BY`, `DISTINCT`, and projection expressions lower into the
  public row-plan families.
- Row filters and role-level predicates are applied through the same access and
  expression predicate model as native callers.

The SQL adapter should never pass a provider-specific SQL string into storage
execution. Foreign-source queries, lake reads, backup/restore, provisioned
tables, hosted tables, and local tables all receive typed catalog targets and
typed row plans.

## Document Table SQL

Today the executable SQL read/write path is relational-only: SQL lowerers
require `storage_mode = relational` and a declared primary key before they
produce typed row plans. Non-relational document tables continue to use the
native document, query, lookup, index, retrieval, and serverless segment APIs.

The long-term shape should let SQL read document tables without turning
document tables into relational tables. The adapter needs a separate
document-table source binding instead of weakening relational invariants or
teaching relational row lowerers to guess at document semantics.

MongoDB's SQL Interface is the useful precedent, not because Antfly should copy
its implementation, but because the product boundary is right: live, read-only
SQL access to document data through BI/client protocols, backed by durable
schema mappings and explicit nested-object/array transforms. Antfly should use
the same broad product shape for document tables: SQL as a compatibility/query
frontend over native document storage, not a second storage format and not an
unstructured write path. See MongoDB's SQL Interface overview:
https://www.mongodb.com/docs/sql-interface/.

### Source Binding

The planner should introduce an explicit source-binding layer after parse and
before semantic lowering:

```zig
const SqlSourceBinding = union(enum) {
    relational: RelationalBinding,
    document: DocumentBinding,
    lake: LakeBinding,
};
```

Binding chooses the source family from the resolved table catalog record,
runtime schema, base-source metadata, and SQL session defaults. Relational
bindings continue through the existing typed row lowerers. Document bindings
route to a document SQL lowerer that produces native document/query/index plans.
Lake bindings route to the lake-native row-source plans described in
[LAKES.md](LAKES.md).

Implementation note: `src/sql/source_binding.zig` now defines the public
`SqlSourceBinding` union, `CatalogTableRef`, per-family binding structs, and the
conservative runtime-schema classifier used to distinguish relational,
document, and lake sources. Document bindings derive their
`DocumentSqlCapabilities` from conservative catalog facts: full-text/default
document query readiness, path-level filter readiness from real producers,
vector/search/graph families, algebraic-aggregate eligibility, and bounded-scan
policy are binding facts rather than lowerer-local defaults. They also derive a
catalog-owned `DocumentSqlSchema` from declared runtime-schema columns plus
durable virtual-field and `typed_paths` metadata, so `SELECT *` and explicit
projections can expose top-level fields even when those fields are optional
document fields rather than declared relational columns. `typed_paths` proves
SQL comparison type semantics only; it is not a managed scalar index family and
does not prove readiness. A declared or typed document field is not enough to
prove native scalar-filter capability unless the binding can identify a
compatible ready producer. `src/sql/document_plan.zig` defines the document
read-plan family for `_id` lookup, indexed document query producers, and
explicitly bounded scans. Public SQL execution can route simple document reads
through the document plan family, and later lowerers no longer need to invent
this source-family boundary.

Those capability bits are an access-path inventory, not the SQL language
definition. Full-text/default document query producers, future physical
scalar/path indexes, algebraic sidecars, vector/search/graph producers,
serverless segments, and bounded scans are interchangeable only when they can
prove the same logical result for the requested predicate, projection, grouping,
or aggregate. `typed_paths` is intentionally not in that inventory: it can be
paired with an independent ready producer for the same path, but it is never the
producer. A producer's presence should make a query cheaper, lower-latency, or
admissible under policy; its absence should not change the meaning of the SQL
statement. It should only make the statement fail closed until another exact
producer is available, a bounded scan can preserve the same semantics, or the
user opts into a policy that makes the work finite.

The review rule is that no lowerer should probe another source family by
trial-and-error. The binder selects the family once from catalog facts and every
later phase receives a typed binding. A useful initial shape is:

```zig
const DocumentBinding = struct {
    target: CatalogTableRef,
    runtime_schema: storage_schema.RuntimeTableSchema,
    virtual_schema: DocumentSqlSchema,
    capabilities: DocumentSqlCapabilities,
};

const DocumentSqlCapabilities = struct {
    doc_id_lookup: bool,
    indexed_scalar_filters: bool,
    indexed_scalar_filter_paths: []const []const u8,
    full_text_filters: bool,
    semantic_filters: bool,
    vector_filters: bool,
    hybrid_filters: bool,
    graph_filters: bool,
    graph_metric_filters: bool,
    algebraic_aggregates: bool,
    bounded_scan: ?BoundedScanPolicy,
};
```

The binder should fail closed when a SQL statement references more than one
source family unless there is an explicit cross-source plan. A future query can
join relational, document, and lake sources, but that should be represented as a
typed cross-source logical plan, not by having each family lower part of an
unstructured SQL string.

### Virtual Schema

Document SQL needs a virtual SQL schema. The minimum projection should expose:

- `_id` as the document key;
- `_doc` as the full JSON document;
- declared document-schema fields when the table has a schema;
- index-defined or statistically observed top-level fields as optional
  projected columns;
- typed-path metadata as type/shape-only proof for explicit field paths and
  virtual roots, without implying any physical access path;
- explicit JSON-path projections for nested fields.

For example:

```sql
SELECT _id, title, metadata->>'status'
FROM docs
WHERE metadata->>'status' = 'active'
LIMIT 10;
```

The virtual schema should be built from durable Antfly facts in priority order:

1. declared document schema;
2. index definitions and derived-index field paths;
3. `typed_paths` metadata for field shape and comparison typing only;
4. explicit SQL view definitions over document tables;
5. observed field statistics as an advisory fallback.

Sampling can help user experience, but it must not become the durable semantic
contract. If a field needs stable BI or agent access, the table schema, an
index definition, a SQL view, or `typed_paths` metadata for type/shape proof
should record that mapping explicitly. `typed_paths` does not record a
physical access path: it may make a path visible and type-checkable, but it
must not satisfy index readiness, boundedness, freshness, or
planner-admissibility checks by itself.

Type mapping should stay conservative:

| Document value | SQL-facing type |
| --- | --- |
| document id | `text` or a stable Antfly `document_id` domain |
| string | `text` |
| boolean | `boolean` |
| integer | `bigint` when lossless |
| floating number | `double precision` |
| object | `jsonb` |
| array | `jsonb` unless explicitly unnested |
| mixed/unknown | `jsonb` or nullable inferred scalar only through a view |

The virtual schema is a read contract, not a promise that every document has the
field. Declared fields use declared nullability. Indexed/statistical fields are
nullable unless a durable schema constraint proves otherwise.

### Supported Query Shape

The first document SQL milestone should be read-only. SQL writes over schemaless
documents raise durable semantics questions that should not be answered by the
SQL adapter alone: missing fields, partial update behavior, JSON merge
semantics, array mutation, generated fields, constraint interaction, trigger
ordering, row-filter checks, and audit records. A later write surface can be
added only through explicit document semantics such as `INSERT INTO docs (_id,
_doc) VALUES (...)` or typed JSON patch operations lowered into the same native
document write path as REST/SDK callers.

Current catalog-backed write lowering enforces this milestone with
`DocumentSqlWriteUnsupported` for document-storage targets, and the public SQL
endpoint maps that to `document_sql_write_unsupported`. That keeps document SQL
read-only until writes are explicitly lowered through native document write
semantics rather than relational row batches.

Initial document reads should support:

- single-table `SELECT` over one document table;
- `_id`, `_doc`, declared field, and JSON-path projection;
- `SELECT *` expansion over the virtual document schema as `_id`, `_doc`, and
  declared fields, including qualified `table.*` / `alias.*` expansion;
- single-table qualification through the table name or alias, for example
  `SELECT d._id FROM docs AS d WHERE d.status = 'active'`;
- `WHERE _id = ...` and `WHERE _id IN (...)`;
- simple scalar equality, inequality, membership, `LIKE`, null-test, and range
  predicates on declared or indexed field paths;
- inclusive scalar range predicates with `BETWEEN ... AND ...` where the field
  type can preserve exact range semantics;
- conjunctions of pushdown-capable predicates;
- `LIMIT` with a required bounded-scan policy when there is no selective
  lookup/index predicate;
- simple `ORDER BY` only when backed by an index, an explicitly bounded result,
  or a future materialized sidecar.

The first milestone should reject joins, windows, recursive CTEs, set
operations, data-modifying CTEs, `DISTINCT`/explicit `SELECT ALL` modifiers,
pagination tails beyond the initial `LIMIT` shape, locking tails such as
`FOR UPDATE`, and broad ordered scans for document tables. Aggregates start with
narrow `COUNT(*)` shapes and grouped counts only when they lower to an exact
native aggregate, candidate, algebraic, or bounded-scan plan. Wider aggregate
families can be added only when they lower to a typed aggregate or
materialized-sidecar plan with an explicit cost model.

Document SQL lowering should push down only behavior that Antfly can execute
natively:

- `_id` equality and `IN` predicates lower to lookup/doc-id filters;
- scalar equality/range predicates lower to existing query/index filters when a
  declared or indexed field path can prove type compatibility; `IS NULL`
  matches explicit JSON null or a missing path, `IS NOT NULL` matches present
  non-null values, ordinary scalar comparisons/ranges/patterns with NULL lower
  to no-match filters, and `IN (...)` ignores NULL members with all-NULL lists
  lowering to no-match;
- full-text predicates lower to full-text query plans when they are used as
  relational-style filters over a document table;
- ranked semantic, vector, hybrid, graph, graph-metric, and reranking searches
  use the existing `antfly.*` table functions as the canonical SQL surface
  rather than a second document-specific predicate family;
- projection, `LIMIT`, simple `ORDER BY`, and residual filters execute only over
  bounded result sets with explicit cost limits.

Anything not safely pushdown-capable should fail closed or require an explicit
bounded scan contract. The adapter should not silently run broad document scans
because a SQL BI client submitted a relational-looking query.

Document SQL should therefore separate semantics from access paths:

| Access path | Role | Correctness rule |
| --- | --- | --- |
| `_id` lookup | exact point producer | always exact for `_id` equality/`IN` |
| full-text/default document index | exact native producer for indexed text, term, boolean, prefix, wildcard, range, and geo predicates; candidate producer for mixed predicates | attach residual filters whenever the index cannot prove the whole predicate |
| future scalar secondary index | exact native producer for compatible declared/indexed scalar paths; selective candidate producer for mixed predicates | push covered scalar clauses only when schema, type, and index readiness prove exact semantics; attach residual filters for uncovered clauses under a candidate bound |
| algebraic sidecar/index | exact aggregate producer or materialized helper | use only when the sidecar covers the requested aggregate, grouping, filters, snapshot, and freshness policy |
| `antfly.*` ranked query functions | explicit SQL surface for full-text, semantic, vector, hybrid, graph, graph-metric, and rerank requests | lower to the native query request path with the same source binding, row filters, readiness, and freshness checks as REST/SDK/MCP/A2A callers |
| bounded document/LSM scan | semantic fallback and residual executor | allowed only under explicit bounds or proven finite cost; fail closed if the bound is exhausted before the answer is complete |

These access paths are optimizer choices, not separate user-visible query
features. A SQL predicate or aggregate is logically supported when Antfly has a
correct executor for its semantics; a ready full-text index, algebraic sidecar,
future scalar index, `_id` lookup, serverless segment, or scan path changes only which
physical plan is admissible and cheap enough to run. The presence of a sidecar
should never make the SQL mean something different, and the absence of a sidecar
should not make a query syntactically invalid. It may make the query
non-admissible until the user supplies a bounded-scan policy, the planner proves
a finite cost, or Antfly implements an equivalent residual evaluator.

The planner invariant is producer independence: the SQL statement defines one
logical predicate, projection, grouping, or aggregate, and each producer is only
a physical way to obtain the same rows or the same exact answer. A ready
full-text index, future scalar index, algebraic sidecar, vector sidecar, or serverless
segment should make a plan cheaper or lower-latency; it should not change which
SQL statements are meaningful. If multiple producers can prove the same
semantics, the planner should choose the cheapest eligible producer and keep any
remaining predicate as an explicit residual filter. If no producer can prove the
semantics, Antfly can still use a scan only when the scan is bounded by policy
or by a proven finite cost and the residual evaluator can implement the same
predicate semantics. Otherwise the planner fails closed.

The practical rule is a producer ladder, not a feature gate. For a point lookup,
use `_id`. For selective document predicates, prefer the default/full-text
document index or a future scalar/path index when either can prove the filter exactly.
For aggregate, facet, top-k, or summary shapes, prefer algebraic/materialized
state when it covers the requested expression, grouping, filter, snapshot, and
freshness policy. For small or explicitly bounded work, use the document/LSM
scan with the same residual evaluator. The presence of any one of these paths is
an optimization and an admissibility proof for a physical plan; the absence of
one path should cause the optimizer to try the next exact path before rejecting
the query.

That creates two separate decisions. First, decide whether Antfly knows the
logical SQL semantics for the document-table shape: predicate, projection,
aggregate, grouping, ordering, and row filters. Second, choose an admissible
physical producer for those semantics. Full-text/default indexes, algebraic
sidecars, future scalar/path indexes, serverless segments, and scans all participate in
that second decision. Their presence should make a query cheaper, fresher, or
lower-latency; it should not be the reason the SQL text itself becomes a
different feature. A scan is the semantic backstop only when the residual
evaluator is exact and the work is bounded by policy or proof.

Antfly's default document index should be treated as more than a literal
full-text search feature. It is the native document query surface for indexed
term, boolean, prefix, wildcard, range, geo, and text predicates. SQL lowering
should therefore map simple indexed document predicates such as:

```sql
SELECT _id, status FROM docs WHERE status = 'active' LIMIT 100;
```

to the same native indexed query/filter contract used by REST, SDK, MCP, A2A,
and CLI callers when a compatible ready producer is present, not because the
field is merely declared. `_id` equality remains a direct lookup. A declared
document field predicate should lower to a native scalar producer only when the
field's index is available and ready; otherwise the planner should fail closed
or require an explicit bounded scan policy with residual evaluation.

That means the optimizer can legitimately choose among:

- a full-text/default document index when it can produce the exact matching
  rows or an exact bounded candidate set;
- an algebraic materialization when it can answer an aggregate/facet/top-k
  request at the same table snapshot and freshness level;
- a future scalar/path index when it proves the requested SQL comparison semantics for
  that field, including as a bounded candidate producer for mixed predicates
  with explicit residual filters;
- an `_id` lookup when the predicate is point-like;
- a bounded scan when its residual evaluator can implement the predicate and
  its input cap is part of the plan contract.

Those choices should be costed and ordered by selectivity, freshness,
read-amplification, hydration cost, and whether the result can be produced
without touching base documents. They should all feed the same typed document
plan and SQL response envelope.

Full-text predicates follow the same rule, but the scan fallback has a higher
bar. `full_text_search(...)` is producer-independent only after the scan path
can evaluate the same analyzer, query parser, tokenization, language, and match
semantics as the full-text index. Until then, the ready full-text producer is
the only correct executor for that predicate, while ordinary scalar predicates
can fall back to the bounded residual evaluator already used for term, range,
prefix, and wildcard filters.

Explicit Antfly query functions are the canonical SQL surface for derived-index
search plans, including the ones that already exist for relational SQL. They are
not a parallel relational-only feature and document SQL should not invent a
second spelling for the same native operations: `antfly.full_text_search(...)`,
`antfly.semantic_search(...)`, `antfly.vector_search(...)`,
`antfly.hybrid_search(...)`, `antfly.graph_traverse(...)`,
`antfly.graph_neighbors(...)`, `antfly.graph_shortest_path(...)`,
`antfly.graph_k_shortest_paths(...)`, `antfly.graph_match(...)`,
`antfly.graph_metric(...)`, and `antfly.graph_metric_rerank(...)` all lower to
the same native query request used by REST, SDK, MCP, A2A, and CLI callers.
Public SQL execution routes these table functions through the native query
service, including read-schema routing and row-filter injection, then returns
one SQL row per native query hit with hit fields such as `_id`, `_score`, and
`_source`. `SELECT *` returns the native hit object; a simple projected column
list such as `SELECT _id, _score FROM antfly.full_text_search(...)` returns only
those hit fields, with missing hit fields surfaced as `NULL`. That keeps
derived-index execution on the document-query path without teaching the
relational row planner to emulate search, graph, vector, hybrid, or reranking
behavior.

Current implementation note: SQL table-function reads normalize the native
query response envelope rather than requiring every derived query to return
ordinary search hits. Full-text, vector, semantic, hybrid, and reranked queries
return hit rows from `hits.hits`. Graph metric reads can flatten
`graph_metric_results` score entries into rows with `_id`, `_score`,
`_graph_metric`, `_index`, and `_metric`. Graph traversal/path reads can flatten
`graph_results` nodes into rows with `_id`, `_source`, `_graph`, `_graph_type`,
`_depth`, `_distance`, and `_node`. Projection uses the same missing-as-`NULL`
rule as hit rows.

That means the planner has two distinct jobs. For explicit retrieval intent,
such as semantic search, graph traversal, hybrid fusion, metric reranking, or
full-text search with native analyzer semantics, SQL should bind an `antfly.*`
table function and route it to the same typed native request used everywhere
else. For compatibility predicates and aggregates, such as `WHERE status =
'active'`, `WHERE price BETWEEN 10 AND 20`, or `COUNT(*) GROUP BY tenant_id`,
SQL should produce a normal document table plan whose physical producer can be
full-text, algebraic, a future physical scalar/path index, `_id` lookup, or
scan. `typed_paths` metadata is type proof for SQL comparison semantics, not a
physical producer. The presence of an index is therefore an optimization and
proof obligation for compatible relational-looking operations, while the
`antfly.*` function family remains the user-visible shape for Antfly-owned
retrieval modes.

Relational-looking predicates over document tables still matter, but they serve
a different role. `WHERE status = 'active'` and bounded compatibility predicates
such as `WHERE full_text_search('title:alpha')` describe filters that the
document planner may implement through a full-text/default index, future scalar
path index, algebraic sidecar, lookup, or bounded residual scan. They should
not grow into separate spellings for `semantic_search`, `hybrid_search`, graph
traversal, graph metrics, reranking, or native full-text retrieval. Those remain
`antfly.*` table functions so SQL, REST, SDK, MCP, A2A, CLI, and serverless
execution all share the same native query request path. The compatibility
predicate is intentionally the unqualified `full_text_search(...)`; qualified
`antfly.full_text_search(...)` belongs in `FROM antfly.full_text_search(...)`
and is rejected as a scalar document predicate.

Current source binding records semantic, vector, hybrid, graph, and graph
metric index families separately, even when the first SQL surface routes them
through `antfly.*` table functions rather than scalar `WHERE` predicates. That
keeps the capability inventory honest for future optimizer work: `semantic`,
`vector`, `hybrid`, `graph`, and `graph_metric` producers can be costed and
selected independently instead of being collapsed into one generic vector flag.

Current catalog-backed lowering enforces that distinction through
`DocumentSqlCapabilities.full_text_filters` and
`DocumentSqlCapabilities.indexed_scalar_filters`: full-text reads and full-text
filtered `COUNT(*)` plans fail closed when the binder cannot prove a ready
full-text producer, even when a bounded scan policy exists. A ready generic
default/full-text index proves full-text execution, but it does not by itself
prove every scalar document path is indexed with exact SQL comparison
semantics. Field-scoped full-text metadata, such as an index config with
`field`, `path`, `fields`, or `paths`, is path-level proof for the documented
term/prefix/wildcard/range producer behavior on those paths only. Scalar
pushdown otherwise requires field-level proof from the runtime schema, catalog
index metadata, or a future path-capability map. Without that proof, a simple
structured predicate lowers to a bounded residual scan when policy allows it,
or fails closed. When a ready full-text producer exists but a
conjunctive scalar predicate cannot be pushed into that producer, the planner
can use full-text as a bounded candidate producer and evaluate the scalar
predicate as a residual. The same rule applies to mixed scalar predicates when
at least one scalar/path clause has a ready producer: covered clauses become the
native candidate query and uncovered clauses remain residual filters. Those
candidate paths are exact only while the candidate hit set fits under the bound;
execution fails closed if the candidate set may extend beyond the bounded
window.

Path-level type proof is separate from path readiness. Table-level
`typed_paths` metadata for `/metrics/score` can expose `metrics` as a JSON
virtual root while recording `/metrics/score` as a numeric typed path. That lets
`metrics->>'score' >= 7` lower to an exact numeric range filter without claiming
the top-level `metrics` object is numeric. Readiness still comes from
capabilities such as `indexed_scalar_filter_paths`; typed paths only prove SQL
comparison semantics for nested JSON-path expressions.

The public table contract accepts `typed_paths` for this proof without treating
it as a storage-index builder. Metadata such as
`{"numeric":["metrics.score"],"keyword":["status","metadata.plan"]}` is stored
as a reserved table metadata section, omitted from table/index status maps,
skipped by local DB index construction, and consumed by SQL source binding as
path-level type proof. It does not imply PostgreSQL `CREATE INDEX` behavior,
readiness, rebuild, catch-up, compaction, or lower latency. A future physical
scalar secondary index should get its own explicit index family and lifecycle
once build, catch-up, readiness, and compaction exist.

The same producer-selection rule applies to aggregate-style SQL. Full-text,
algebraic, vector, and future physical scalar/path indexes are optimizations and
native producers, not separate semantics. When an algebraic index can answer
`COUNT`, grouped counts/facets, selected `DISTINCT`/top-k queries, or numeric
summaries from indexed state, the document planner should prefer that
index-backed aggregate producer. When a full-text index or future physical
scalar/path index can first produce a selective candidate set, bounded local
aggregate work can run over that candidate set. When no index helps, a scan is
still a valid physical
producer only if the request carries an explicit bounded-scan policy or the
planner can prove the scan cost is acceptable. It should not hydrate every
matching document merely because the request is written as SQL.

Current implementation note: document SQL now has a distinct document aggregate
plan for narrow `COUNT(*)` shapes with optional indexed scalar filters and
optional single indexed grouping paths. Catalog-backed lowering annotates that
plan with an algebraic index and materialization name when catalog metadata
proves a matching unfiltered materialization exists; filtered aggregates remain
attached to their native candidate producer until algebraic materialized-filter
proof exists. Execution currently supports ungrouped `COUNT(*)` over `_id`
lookup and indexed/full-text query producers through the native document query
path, plus grouped `COUNT(*)` when a native candidate producer can enumerate the
matching documents. When no algebraic materialization or native candidate
producer can answer exactly, catalog-backed lowering can choose an explicit
bounded-scan aggregate producer from `DocumentSqlCapabilities.bounded_scan`;
execution fails closed if the scan reaches its cap, so counts are not returned
from partial samples. The same bounded-producer rule now applies to ungrouped
numeric `SUM(path)`, `AVG(path)`, `MIN(path)`, and `MAX(path)` over numeric
declared fields or typed paths: `_id` lookup, capped native candidates, and
policy-bounded scans can compute the exact summary, missing/JSON-null inputs
are ignored, and an empty non-null input set returns `NULL`. Grouped numeric
`SUM(path)`, `AVG(path)`, `MIN(path)`, and `MAX(path)` follow the same
bounded-producer rule and return `NULL` for groups whose matching rows have no
non-null numeric measure. Configured local algebraic materializations for
unfiltered `COUNT(*)`, `SUM(path)`, `AVG(path)`, `MIN(path)`, and `MAX(path)`
can now execute through the document SQL source boundary when the materialized
`op`, `group_by`, and `measure` exactly match the SQL plan. Schema-derived
algebraic configs emit native `avg` materializations for common group/measure
pairs; SQL must not rewrite `AVG(path)` to `SUM(path) / COUNT(*)` because
missing and JSON-null measure values are ignored by SQL `AVG`. Filtered
materialized aggregates, distributed materialization merging, and adaptive
partial/result execution still fail closed until their producer-specific
executors are implemented.

Read execution now also has a policy-gated bounded residual scan path for simple
scalar document predicates. If a predicate cannot use a ready document query,
full-text, scalar, or `_id` producer but the catalog binding supplies a
bounded-scan policy and the SQL has an output `LIMIT`, the lowerer can use a
capped document scan plus a residual evaluator for the same structured filter
subset emitted to native index queries. That keeps the semantics independent of
index presence while still refusing hidden broad scans. Policy-derived bounded
scan producers now carry both row and byte caps from `BoundedScanPolicy`; the
executor fails closed when the scan payload exceeds the byte cap instead of
returning an unbudgeted partial result.

Ordered bounded scans are stricter than unordered `LIMIT` scans. An unordered
document `LIMIT` can stop after the requested number of scanned rows because the
SQL result is intentionally an arbitrary bounded prefix. `ORDER BY ... LIMIT`
must sort over a bounded input domain instead: catalog-backed lowering uses the
bounded-scan policy as the input cap, and execution fails closed if the scan
reaches that cap. Returning the best rows from only the first N scanned keys
would make top-k results depend on physical key order, so that shape is not a
valid optimization.

Current implementation note: `ORDER BY` now resolves declared document fields,
typed-path or index-definition virtual fields, `_id`, and JSON paths rooted in
declared, typed-path, or index-derived virtual top-level fields. That remains a
bounded local sort over an `_id` lookup, a capped native candidate producer, or
a policy-capped scan; it does not claim that arbitrary document tables have a
global sorted access path.

Current implementation note: `_doc` is also a SQL JSON-path root for projection
and compatible predicates. For example, `_doc->>'status'` lowers to the same
logical path as `/status`, and `_doc#>>'{metadata,plan}'` lowers to
`/metadata/plan`. `_doc` itself is not a producer and does not prove readiness;
declared fields and typed paths can only make the resulting path typeable.
Producer readiness must still come from catalog index metadata, another native
producer capability, or an explicit bounded-scan policy.

Residual filters are allowed only after an explicit bounded producer. Current
document read lowering can attach scalar residual filters after an `_id IN (...)`
or `_id = ...` lookup, after a bounded full-text candidate query, or after a
bounded scalar/path candidate query, or after a limit-governed scan. It should
not become a hidden table scan path.

Arrays must be explicit. Document tables should not pretend nested arrays are
ordinary scalar columns. The SQL surface should require `UNNEST` or an
Antfly-named equivalent for array expansion:

```sql
SELECT d._id, tag
FROM docs AS d, UNNEST(d.tags) AS tag
WHERE tag = 'urgent';
```

This gives agents and BI tools predictable cardinality and cost behavior while
still allowing Mongo-style flatten/unwind workflows over document data.

Current implementation note: document SQL projection can still expose array
fields as JSON values, but scalar predicates over array paths fail closed with
`DocumentSqlArrayRequiresUnnest`. `UNNEST(d.array_field) AS item` is the first
explicit array-expansion shape: it expands a single declared array field over an
`_id` lookup, a bounded native document-query candidate producer, or a
policy-bounded scan producer; can apply an equality predicate on the unnest
alias; projects the expanded item as a SQL row value; and supports bounded
`ORDER BY` over either the unnest alias or a document field before applying
`LIMIT`. Broader array operators, nested unnests, and true indexed
array-element producers remain future work.

### Execution Contract

Document SQL should reuse the same external response envelope as relational SQL
while preserving a distinct internal plan family:

```zig
const LogicalSqlReadPlan = union(enum) {
    relational: RelationalReadPlan,
    document: DocumentReadPlan,
    lake: LakeReadPlan,
};

const DocumentReadPlan = struct {
    table: CatalogTableRef,
    projection: []const DocumentProjection,
    producer: DocumentProducer,
    residual_filter: ?RowExpression,
    limit: ?usize,
};

const DocumentProducer = union(enum) {
    id_lookup: []const DocumentId,
    indexed_query: DocumentIndexQuery,
    text_query: DocumentTextQuery,
    vector_query: DocumentVectorQuery,
    algebraic_aggregate: DocumentAlgebraicAggregate,
    bounded_scan: BoundedDocumentScan,
};

const BoundedDocumentScan = struct {
    max_rows: u32,
    max_bytes: ?u64,
    residual_filter: ?RowExpression,
};
```

The runtime path should be:

```text
SQL ingress
  -> TokenizedSql / ParsedSql
  -> SqlSourceBinding(document)
  -> DocumentReadPlan
  -> native document/query/index/read source
  -> SQL row envelope
```

The executor should call the same document read/query/index services used by
REST, SDK, MCP, A2A, and CLI callers. It should not reconstruct behavior by
walking storage internals from the SQL adapter. Authorization, row filters,
audit hooks, sync visibility, and session defaults should all pass through the
shared service boundary. External constraints such as auth row filters are part
of the logical predicate, so every physical producer (`_id` lookup, full-text or
scalar query, algebraic/materialized aggregate, serverless sidecar, or bounded
scan) must enforce them before projection or aggregation.

The document SQL result contract should remain the same SQL response envelope as
relational reads (`kind = read`, `statement_kind = query`, `result.rows = ...`),
but the inner plan family should identify that it came from a document source.
That keeps CLI, HTTP, SQL wire, MCP, and A2A clients uniform while preserving
source-family-specific planning and diagnostics.

Diagnostics should name the unsupported document SQL feature and the missing
native capability. Examples:

- `document_sql_requires_bounded_scan` for an unindexed predicate without an
  explicit bounded scan policy;
- `document_sql_array_requires_unnest` for scalar treatment of an array path;
- `document_sql_unsupported_join` until there is a cross-source join plan;
- `document_sql_native_search_requires_table_function` when ranked native
  search functions such as `antfly.semantic_search`, `antfly.hybrid_search`, or
  graph functions are used as scalar document predicates instead of table
  functions;
- `document_sql_projection_modifier_unsupported` for document-table
  `DISTINCT`, `DISTINCT ON`, or explicit `SELECT ALL` until those shapes lower
  to exact aggregate/materialized plans;
- `document_sql_pagination_unsupported` for `OFFSET`/`FETCH` tails until
  cursor-backed document pagination has a bounded native plan;
- `document_sql_locking_unsupported` and `document_sql_window_unsupported` for
  row-locking and window clauses until document reads have native semantics for
  those features;
- `document_sql_view_mapping_unsupported` until document-to-SQL views have
  durable catalog metadata and execution support;
- `document_sql_write_unsupported` until document writes have shared native
  write semantics.

### Relationship To Lake And Serverless

Document SQL, lake SQL, and relational SQL should converge at the typed row
contract, not at raw SQL strings. The same SQL surface can eventually query:

- native relational tables through row plans;
- native document tables through document producers plus virtual projection;
- external or Antfly-owned lake tables through lake row sources and sidecars.

The serverless segment/index system gives document SQL an advantage over a
generic document-to-SQL adapter. Full-text, sparse, dense-vector, graph, and
algebraic sidecars can be selected as producers or materialized helpers for
document SQL, just as lake SQL can use lake sidecars. The rule is the same:
sidecars may narrow candidate sets, rank rows, or answer materialized folds only
when their source binding, snapshot/generation, schema fingerprint, and
freshness policy match the document table being queried.

This is why document SQL should be built as a source family next to relational
and lake bindings. It lets Antfly share parser, session, auth, response, and
expression infrastructure while keeping storage-specific execution efficient
and auditable.

Implementation should be scaffolded in small steps:

1. Route current relational SQL binder entrypoints through `SqlSourceBinding`
   without behavior change.
2. Expand document virtual-schema construction from declared document schemas
   and `_id` / `_doc`.
3. Finish document read lowering for `_id` lookup, projection, `LIMIT`, and simple
   scalar field predicates.
4. Add JSON-path expression nodes shared with relational `json` / `jsonb`
   columns.
5. Add explicit bounded-scan and residual-filter limits.
6. Add initial array expansion through single-field `UNNEST`.
7. Add full-text, semantic, vector, hybrid, graph, graph-metric, and rerank SQL
   functions that lower to native derived-index plans.
8. Add optional SQL view definitions as stable document-to-SQL schema mappings.
9. Add e2e parity showing SQL document reads and native document query APIs
   reach the same storage/query path.
10. Consider explicit document writes only after the read path, authorization,
    row filters, and audit semantics are shared with native document writes.

## Expressions

The shared row-expression AST is the boundary for supported SQL expressions.
Checks, generated columns, partial predicates, expression indexes, conflict
actions, update transforms, aggregate filters and inputs, `HAVING`, order keys,
window inputs, `RETURNING`, and row-rewrite `USING` expressions should converge
on one typed AST.

Parser or lowerer conveniences such as PostgreSQL JSON syntax:

```sql
attrs->>'title'
attrs #>> '{billing,plan}'
jsonb_extract_path_text(attrs, 'billing', 'plan')
```

must lower to typed JSON field references and extraction nodes. Durable metadata
stores the native `FieldRef` or expression node, not the SQL spelling.

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
same field-path resolver as document-derived indexes. A reference such as
`attrs.title` resolves to a declared `json` column plus a permitted embedded
path. Full-text, AKNN, graph, algebraic, embedding, and hybrid configs validate
those paths against the table schema before writing catalog metadata.

See [RELATIONAL.md](RELATIONAL.md) for the relational field and derived-index
model.

## Extensions and Roles

`CREATE EXTENSION`, `ALTER EXTENSION ... UPDATE`, and `DROP EXTENSION` are SQL
syntax over the extension lifecycle service. SQL extension names resolve through
the package catalog and manifest-declared SQL aliases. Version, digest,
capability, dependency, and member tracking live in extension metadata, not SQL
text.

Role SQL is syntax over Antfly user/role management:

- Role creation, rename, drop, membership, grants, and revokes update the native
  role service.
- Qualified resources use `database:name`, `namespace:db.ns`, and
  `table:db.ns.table`.
- `ALTER ROLE ... SET ...` is durable only for settings backed by a native role
  setting model. Unsupported settings fail closed instead of being stored as
  opaque PostgreSQL configuration text.

## Parser and Lexer Design

Follow the PostgreSQL shape where it helps, but keep implementation native:

- The lexer should stream or produce borrowed tokens over the SQL input, with
  original byte offsets for diagnostics.
- Keyword classification should happen once at lex time, using a generated or
  table-driven lookup rather than repeated case-insensitive string chains.
- The grammar should produce a raw AST without catalog access.
- A small lookahead/filter layer should handle multi-token constructs and
  statement-family classification before deep lowering.
- Unsupported statement families should classify cheaply without allocating
  partial semantic plans.
- Identifiers should remain borrowed until they become durable catalog names or
  plan-owned data.
- Quoted identifiers, Unicode escape handling, string literals, dollar-quoted
  bodies, comments, and numeric literals should preserve exact source locations.

Do not make the lexer store durable SQL text as metadata. The parser's output is
only an intermediate representation for lowering.

## Speed and Memory Improvements

The immediate performance goal is to reduce duplicated work and transient
allocation churn, not to replace the parser.

Prioritized improvements:

1. Add measurement around the SQL parity corpus and focused parser benchmarks:
   tokenization time, parse time, lowering time, fixture encode/check time,
   allocation count, and allocated bytes per statement family.
2. Split ordinary parity classification from fixture freshness/promotion so the
   default SQL parity gate does not lower the same corpus twice.
3. Use a resettable per-statement arena for parser, lowerer, fixture, and
   classifier scratch allocations. Durable native plans still own their output
   through normal allocators.
4. Introduce a reusable `TokenizedSql` or `ParsedSql` object so classification,
   lowering, `EXPLAIN`, fixture fingerprints, and catalog application can share
   the same token stream and raw AST.
5. Add cheap statement-family dispatch before attempting deep parse/lower paths.
6. Move repeated keyword and identifier normalization into token metadata.
7. Replace repeated SQL/string scans in coverage and fixture checks with
   structured plan summaries or bitsets emitted by lowering.
8. Defer JSON literal parsing and stringify work until the semantic phase
   actually needs typed JSON.
9. Keep unsupported classifications cheap and allocation-light.

Current implementation status:

- `src/sql/tokenized.zig` owns reusable `TokenizedSql` state for a SQL input,
  including the borrowed token stream, top-level statement family, read
  statement kind, and write statement kind.
- Generated parser payloads and tokenized SQL state derive read families
  (`query`, `set_operation`, `recursive_cte`, `aggregate`, `join`, `lateral`,
  and `window`) and write families from the shared token stream. The shared
  family/kind enum definitions live in `statement_kind.zig`; there is no
  standalone classifier module in the production SQL facade.
- Parsed read lowering dispatches strictly through the parsed statement
  variant. The lower-level hook API fails closed when a caller does not supply
  a statement kind. Set-operation-shaped SQL routes through the parsed
  set-operation variant; that variant may emit a native row-query plan when the
  set algebra has a proven equivalent query rewrite, otherwise it emits the
  native set-operation plan.
- Write lowering consumes `TokenizedSql` for write-family selection instead of
  tokenizing separately from classification. Syntactic mutation-source forms
  such as row-claimed, ordered/paginated, and temporal `FOR PORTION` `UPDATE`
  and `DELETE` statements classify directly as `update_source` / `delete_source`
  variants. Ordinary `UPDATE` and `DELETE` planning now uses explicit selector
  classification before invoking point or mutation-source lowerers, so
  non-unique selectors route to claimed mutation-source planning and point
  selectors fail closed when no resolver is available.
- `ParsedSql` wraps `TokenizedSql` with a raw statement view and byte-source
  span. DDL lowering, catalog-apply test lowering, read statement dispatch, and
  write statement dispatch now use that shared wrapper as the top-level SQL
  object. `ParsedSql` owns a typed `ParsedStatement` union with read, write,
  DDL, EXPLAIN, transaction, and session variants. `ParsedExplainStatement`
  owns EXPLAIN options plus the nested subject token range, and read/write lowerer
  dispatch now routes from that parsed statement view instead of reaching back
  into tokenization metadata. Read, write, catalog-backed read/write, EXPLAIN,
  and relation population lowering callbacks consume borrowed `ParsedSql`. `EXPLAIN`,
  contiguous relation-population sources such as `CREATE TABLE AS`, and
  non-contiguous relation-population sources such as `SELECT INTO` now hand
  nested lowerers parsed child statements cloned from the parent token stream.
  The public explain and relation-population facades now parse once at ingress
  and route through parsed context entrypoints before dispatching to nested
  read/write lowerers. The data-driven SQL edge-case harness also parses each
  case once and passes `ParsedSql` through its lowering callbacks, including
  write-family classification from parsed/tokenized metadata. Relation
  population syntax now carries source token ranges only; it no longer allocates
  or stores reconstructed source SQL. App-parity catalog/source-table fixture
  resolution has parsed entrypoints and reuses the same `ParsedSql` object as
  read/write/explain lowering instead of retokenizing fixture SQL.
  Catalog-backed read/write prebinding now produces `BoundSqlStatement`, preserving the parsed statement variant while
  carrying owned session identity (`current_database` and `search_path`) plus
  resolved source schemas or write-plan catalog options before routing through
  `LogicalSqlPlan` into the typed lowerer. Session-aware binder entrypoints can
  resolve unqualified catalog names against non-default search paths. Public
  facade wrappers for direct select, insert, strict insert/update, and aggregate
  lowering now delegate through parsed helper variants instead of owning
  separate tokenization paths. DML adapter write-plan regression callbacks,
  direct DML test facades, and lower-expression query/join/lateral/window/
  aggregate/set-operation/recursive-CTE test facades consume borrowed parsed
  tokens instead of round-tripping through SQL text. DDL plan and DDL
  fingerprint regression helpers parse once through `ParsedSql` before
  computing typed plans or structured fingerprints. PostgreSQL compatibility
  no-ops that are represented as control statements, including
  `SELECT pg_advisory_*`, still enter through `ParsedSql` and then lower inside
  the adapter-owned DDL/control plan parser. Antfly query function lowering has
  parsed entrypoints, and app-parity query-function checks reuse `ParsedSql`
  instead of tokenizing fixture SQL directly.
  Corpus fixture parameter coverage parses fixture SQL once and validates
  placeholders from parsed token spans instead of using a separate raw string
  scanner. Corpus coverage observation now parses each entry once and uses
  parsed token predicates for early syntax buckets such as conflict clauses,
  query functions, computed patterns, and common function-name coverage.
  Parsed-only lowering context construction does not require a raw SQL field.
  Recursive data-modifying CTEs carry a parsed recursive-write flag, so generic
  write planning no longer reclassifies recursive write kind from the token
  stream. Catalog-aware read/write bind, lower, and resolve APIs expose parsed
  or bound statement entrypoints publicly. Bound catalog statements now resolve
  source schemas and write-plan options from the parsed statement route rather
  than a token-only compatibility route. Token-only source-table helpers remain
  private binder implementation details. Source-table discovery for corpus
  fixtures also enters through parsed source-table helpers instead of public
  token-slice adapters, and those helpers now validate the parsed statement
  family/kind before scanning source-table tokens. Catalog prebinding
  source-table scanners now use token keyword metadata for statement heads,
  top-level clauses, set-operation tails, and optional `ONLY`/`AS`/`LATERAL`
  modifiers instead of local string-keyword checks. Relation/source alias
  inference and DML target alias tail detection also use token keyword metadata
  for clause boundaries and join/source modifiers. Insert-source select tails,
  DML assignment tails, boolean expression operators, and `ON CONFLICT` /
  `RETURNING` delimiters now use token keyword predicates in lower-DML scans.
  Relation-population parsing and update-source alias inference in the grammar
  also use keyword tags for statement heads, `FROM`/`AS`, set-operation tails,
  `WITH [NO] DATA`, and DDL unique-predicate delimiters and null-test atoms.
  JSON and expression null-safe distinct, null-test, membership, and expression
  negation helpers use keyword tags for SQL operator words. WHERE disjunction,
  access-predicate, text-pattern, and access-negation probes use token keyword
  tags for `AND`/`OR`/`NOT`/`LIKE`/`ILIKE` and array markers. Aggregate
  boolean `IS NOT` and `NOT (...)` helper scans also use keyword tags.
- Identifier tokens carry optional compact keyword metadata. The shared
  parser/classifier helpers use enum-backed keyword searches for statement
  family dispatch, top-level clause discovery, mutation-source detection,
  aggregate recognition, and prepared-statement subject classification before
  falling back to text comparison for non-keyword identifiers. Antfly
  query-function parsing also consumes parsed tokens with keyword-tagged
  function, boolean, array, and structured-source fields instead of local
  keyword scanners.
- Tokens expose stable source spans, quoted identifiers keep quoted-source
  spans without being marked as keywords, and diagnostics have a span-bearing
  `SqlDiagnostic` shape for unsupported classifications.
- Fixture fingerprint checks use a structured `PlanFingerprintView` scanner with
  explicit exact-token and suffix-token modes instead of open-coded substring
  searches. Lateral and window expression coverage checks now use emitted plan
  tokens rather than matching SQL spelling. Fixture parameter coverage now uses
  `ParsedSql` placeholder tokens instead of a separate raw SQL byte scanner,
  and resolver-hint metadata validates `ON CONFLICT` from parsed statement
  tokens instead of spinning up a hidden tokenizer from fixture SQL.
  Source-corpus loading and generated-fixture metadata validation now parse
  each fixture entry once and share that `ParsedSql` through metadata,
  parameter, resolver-hint, and source-table checks.
  Corpus feature coverage parses entries once and uses SQL tokens for query
  function, conflict, multi-row insert, computed-pattern, JSONB, array, and UUID
  coverage checks instead of raw substring probes. More corpus coverage buckets,
  including pagination tails, temporal range constructors/operators, set
  operation tails, joined-source returning expressions, CTE mutation markers,
  mutation-source row assignments, temporal `FOR PORTION` clauses, boolean
  `IS [NOT]` predicates, aggregate expression functions, percentile ordering,
  aggregate boolean `HAVING`/`FILTER` clauses, query scalar-function expression
  families, JSON/array expression helpers, datetime and interval expression
  helpers, postfix null tests, `ORDER BY ... USING` tails, array-overlap access
  predicates, joined mutation-source semijoin shapes, joined-source
  regex/array/JSON expression coverage, and row-lock invalid cases, now use
  parsed token predicates instead of raw SQL substring probes.
- Expression predicate membership and boolean-start helpers also use keyword
  tags for `ANY`, `SOME`, `ALL`, `BETWEEN`, `IN`, `NOT`, `IS`, boolean atoms,
  quantifier matching, parenthesized null tests, and tail conjunctions instead
  of maintaining local case-insensitive operator scans.
- Aggregate filter/having boolean literal probes, JSON-path filter literal
  checks, coalesce/row-expression field guards, and parenthesized expression
  condition probes use token keyword metadata for `IS`, `NOT`, `AND`, `NULL`,
  `TRUE`, and `FALSE`.
- DML conflict, assignment, and merge expression-start helpers share the same
  keyword metadata for boolean atoms, `DEFAULT`, `NOT`, `OR`, and
  parenthesized conjunction/disjunction probes. Joined mutation text-pattern
  detection and DML expression-start keyword helpers now read parsed token
  keyword metadata instead of rechecking matched token text.
- Row-expression boundary detection and unsupported read-tail detection use
  keyword metadata, including CASE branch delimiters, so expression scanners
  stop on parsed token facts instead of reinterpreting identifier text.
- Conflict-update and insert-source conflict coverage checks now use parsed SQL
  tokens for function calls, keyword sequences, JSONB concatenation, row
  assignment, boolean update, and regex expression buckets instead of raw SQL
  substring probes.
- Read join side-predicate coverage, graph table-function CTE/join coverage,
  set-operation tail coverage, unsupported set-operation output-shape coverage,
  merge default-expression coverage, and recursive insert-source coverage also
  use parsed token predicates plus plan summary evidence instead of raw SQL
  substring checks.
- Insert conflict-target coverage now extracts `ON CONFLICT` target ranges from
  parsed tokens for named constraints, column targets, partial targets, and
  expression targets, including temporal named-constraint upserts, instead of
  scanning SQL text.
- ALTER TABLE coverage now uses parsed keyword tags for add/drop/rename column
  and constraint forms, defaults, not-null toggles, type changes, validation,
  and trigger-backed update-policy drops while retaining applied-plan
  rebuild/validation/rewrite evidence.
- Adapter no-op transaction/session coverage now uses parsed statement-start
  keyword tags for `COMMIT`, `ROLLBACK`, `RESET`, `SHOW`, and `DISCARD`
  instead of case-insensitive raw SQL prefix probes.
- TRUNCATE and COPY corpus coverage now uses parsed keyword/identifier tokens
  for identity handling, cascades, multi-table detection, `COPY ... TO STDIN`,
  `OIDS`, and `PROGRAM` endpoint probes while retaining native plan and
  execution-plan evidence.
- Prepared-statement CTE coverage now uses parsed `PREPARE ... AS WITH`
  keyword sequences plus plan subject/family tokens instead of raw SQL prefix
  and substring checks.
- Temporal/schema DDL coverage now uses parsed keyword sequences for
  `PERIOD`, `FOREIGN KEY`, `SYSTEM VERSIONING`, temporal foreign-key actions,
  and `UNIQUE NULLS NOT DISTINCT` instead of raw SQL substring probes.
- Temporal range-literal coverage now uses lexed string-literal tokens plus
  typed plan/table evidence for range bounds and inclusivity instead of
  scanning the raw SQL source.
- Aggregate and window duplicate-label coverage now uses parsed function calls,
  keyword tags, identifiers, and plan aggregate/window evidence instead of raw
  SQL substring probes.
- Sequence typed/owned coverage now uses parsed keyword and identifier tokens
  for `AS`, `bigint`/`integer`, `OWNED BY`, and `NONE` instead of raw SQL
  substring probes.
- Routine DDL and temporal DML coverage now uses parsed identifiers, function
  calls, string tokens, and token-aware plan fingerprint checks for external
  security, named argument expressions, routine/role/row-policy options,
  interval updates, `TIMESTAMPTZ` literals, and resolver-hint `ON CONFLICT`
  validation instead of raw SQL or raw plan substring scans.
- Relation-population syntax parsing has a `ParsedSql` entrypoint; the raw SQL
  parser is now a leaf wrapper that parses once before delegating, and planning
  consumes and exports the parsed entrypoint directly.
- Catalog read/write resolver and lowerer APIs no longer keep raw SQL wrapper
  paths in the binder or expose them from the adapter facade. Resolver-fragment
  helpers are internal to the binder; the public route is parsed, bound, or
  logical-plan based, and joined DML catalog coverage now validates
  `ParsedSql -> BoundSqlStatement -> LogicalSqlPlan` directly.
- Catalog read/write lowering contexts now bind `ParsedSql` into
  `BoundSqlStatement` explicitly before lowering; the exported lowerer helper
  consumes a real bound statement rather than a parsed statement plus catalog.
- Binder structural table-source helpers no longer expose raw SQL wrapper
  entrypoints; tests and facade exports use parsed helper variants directly.
- Relational SQL catalog/read parsed lowering helpers are public typed adapter
  APIs, and the HTTP routine-binding read path constructs `ParsedSql` once
  before dispatching to catalog or non-catalog lowering.
- Query-function app-parity assertions expose a parsed entrypoint, and the
  integration corpus runner uses that parsed route instead of reparsing fixture
  SQL inside the assertion helper.
- The integration corpus runner constructs `ParsedSql` once per entry and
  shares it across read, write, explain, DDL, invalid, unsupported,
  relation-population, adapter-noop, and query-function assertion branches.
- Common expression-start function names now carry compact token keyword
  metadata, and expression classifier probes consume those tags instead of
  repeating local case-insensitive string checks.
- Write, explain, and relation-population hook lowerers use parsed entrypoints
  only; the raw SQL wrappers were removed so callers construct `ParsedSql` once
  at ingress and pass it through. Relation-population parser tests exercise
  `ParsedSql` directly, and text-pattern predicate lowering uses keyword tags
  for `ILIKE` detection instead of re-reading token text.
- Generated-column expression matching also uses token keyword metadata for
  `lower`, `upper`, `md5`, `concat`, and `concat_ws` instead of local
  case-insensitive token-text checks.
- Window-clause discovery and JSON extraction membership probes use token
  keyword tags for `WINDOW`, `ANY`, `SOME`, and `ALL`.
- Scalar expression function classifiers now have compact token metadata for
  broader text, JSON, array, regex, datetime, and numeric function families.
  Length, JSON-type/extract/build, array-length/position, case-fold/trim,
  substring/split/strpos, left/right, and pad parser helpers consume token tags
  directly instead of rechecking identifier text, while quoted identifiers stay
  outside keyword classification.
- DML assignment expression-start classification uses the same token metadata
  for scalar, text, regex, JSON, array, datetime, and generated/hash function
  families, so write lowering no longer reinterprets those identifiers from raw
  token text before selecting an assignment-expression parser.
- Read-side expression classifiers for simple returning fields, aggregate
  inputs, aggregate filters, aggregate order keys, select projections, row
  expression operands, generated-column predicates, and general expression
  starts use the same token metadata for JSON, array, regex, text, datetime,
  generated/hash, and UUID function families.
- Parser cursors expose token-predicate identifier/function-call helpers, and
  DDL generated-expression plus DML boolean-start probes use them for
  generated/hash and text-pattern functions instead of local string callbacks.
- `ParsedSql` EXPLAIN subject and option parsing uses token keyword tags for
  option names, formats, and boolean aliases, so parse-time statement metadata
  no longer keeps a local case-insensitive keyword scanner for EXPLAIN.
- SQL value parsing uses token keyword tags for JSON scalar atoms, array
  constructors, defaults, JSON helper functions, boolean predicates, pagination
  words, datetime literal prefixes, current-time keywords, UUID functions, and
  interval literals; semantic string values such as encodings and interval
  units remain ordinary value comparisons.
- SQL plan parsing uses token keyword tags for CTE heads, set/read result-tail
  clauses, recursive CTE member joins, and final recursive SELECT detection;
  semantic output and source-name comparisons remain ordinary name matching.
- SQL insert-family parsing uses token keyword tags for `INSERT`, `VALUES`,
  `INSERT ... SELECT`, `ON CONFLICT`, `DO UPDATE`, `WHERE`, and `RETURNING`
  structural clauses; semantic column, table, and value comparisons remain
  ordinary binder/value work.
- MERGE, joined mutation-source, and data-modifying CTE parsing use token
  keyword tags for structural statement heads, CTE wrappers, match arms, action
  clauses, and returning clauses; source/target table identity stays in binder
  and plan resolution.
- Update/delete mutation-source and point-mutation parsing use token keyword
  tags for statement heads, query-tail clauses, recursive write guards, row
  claim clauses, and returning clauses; selectors and assignments remain typed
  binder/expression/value work.
- Point-selector classification, conflict-target parsing, and semijoin
  mutation-source parsing use token keyword tags for structural statement
  heads, `FROM`, `WHERE`, `IN`, `SELECT`, `FOR`, and `RETURNING` clauses.
- Lower-DML fixed keyword parsing now uses token keyword metadata throughout
  insert-source CTE lookup, conflict/joined assignment parsing, joined
  predicate conjunctions, temporal `FOR PORTION`, `MERGE ... VALUES`, and
  `jsonb_set` assignment helpers; the remaining dynamic operator-table probe is
  intentionally semantic dispatch.
- Read, join, lateral, window, aggregate, and lowerer test adapters classify
  CTE-shaped parsed statements from token keyword metadata instead of the
  compatibility string helper.
- Expression helper function-call probes use token keyword metadata for common
  scalar, array, text, UUID, datetime, and math function families instead of
  rechecking function-name text.
- `POSITION(...)` is tokenized as a first-class function keyword, keeping
  expression-start and function-call probes on the same token-tag path as
  `ARRAY_POSITION`, `STRPOS`, and the other text/array helpers.
- SQL/API parity fixture callbacks receive the already parsed corpus statement
  when deriving applied DDL fingerprints, so generated-fixture and metadata
  validation paths do not re-tokenize the statement after ingress parsing.
- HTTP relational SQL DDL execution, source-backed DDL application, bulk SQL,
  and auth-catalog DDL helpers construct `ParsedSql` once at ingress before
  lowering DDL/session control plans, and transaction-boundary session cleanup
  now uses parsed statement-start keyword tags instead of a raw SQL prefix
  probe.
- SQL routine trigger DDL runtime helpers accept `ParsedSql` directly, and the
  HTTP DDL fallback path reuses the parsed statement when installing or
  dropping trigger metadata instead of spinning up a private tokenizer.
- User-manager role and row-security DDL execution can consume parsed or
  already-lowered DDL plans directly, and HTTP SQL DDL reuses its existing
  lowered plan for auth catalog lookup, user-manager execution, extension
  lifecycle, and source-backed catalog/table DDL application.
- Catalog-backed read binding carries `SqlSourceBinding` for the resolved
  target table, including document source-family and runtime-schema facts, and
  catalog document reads now lower through the bound statement path instead of
  bypassing binding on `schema.storage_mode`.
- DDL plan lowering now lives behind `sql_adapter` exports; auth role/row
  security execution, catalog jobs, notifications, and extension lifecycle use
  adapter-native plan types instead of importing the broader relational SQL
  facade.
- Corpus coverage now uses centralized plan root/read/merge kind helpers for
  read, merge, and recursive insert-source plan-family checks instead of
  repeated raw prefix probes.
- Set-operation source-table fixture checks use the same shared exact
  plan-token scanner for nested right-side table fingerprints, so suffix
  matches and duplicate `:right=right:table=` tokens cannot claim coverage.
  The scanner also detects immediately adjacent duplicate plan tokens instead
  of skipping the delimiter between segments.
- Specialized runtime DML lowerers now validate generated-owned write family
  metadata before lowering insert, insert-source, update, delete, truncate,
  joined mutation-source, and merge plans, so mismatched generated DML ASTs fail
  closed instead of flowing into token fallback.
- DDL planner entrypoints reject generated-owned DDL/session/prepared/cursor/
  graph metadata when the retained generated AST is missing, preserving
  token fallback only for non-generated statements and explicit valid-AST
  compatibility boundaries.
- Generated-owned session, prepared-statement, and cursor ASTs now lower
  through generated-aware DDL paths for both legacy and logical plan entrypoints;
  adapter no-op session settings are mapped explicitly instead of reopening the
  token parser.
- Direct generated-DML lowering validates both write family and recursive-CTE
  state from the published generated-aware write record before dispatching
  recursive or non-recursive write plans.
- Catalog prebinding for generated top-level `INSERT ... SELECT` now consumes
  retained generated DML target and child-read source-table metadata, and fails
  closed when that retained source metadata is inconsistent instead of
  rediscovering the source through token scanning.
- Write-target binding for generated DML now consumes retained generated target
  metadata for `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, and `MERGE`, and fails
  closed when the retained target range no longer matches the generated command
  layout instead of falling back to a legacy token scan.
- Session, savepoint, notification, prepared-statement, cursor, and routine
  runtimes now live under `pkg/antfly/src/sql/`; `api/sql/mod.zig` only
  re-exports those protocol-neutral runtimes for API callers. Routine
  expression execution uses the storage row-expression evaluator directly, so
  routine catalog state no longer depends on API row handlers.
- SQL catalog identity/session helpers now live under `pkg/antfly/src/sql/`
  and are re-exported by the API module, so binder, executor, DDL, and runtime
  code no longer import catalog session types from the API package.
- Relational row request/plan/result contracts now live under
  `pkg/antfly/src/sql/`; the API module re-exports the surface for existing
  HTTP callers, while SQL lowerers and runtimes depend on the SQL-owned module
  directly.
- SQL parity, coverage, native-requirement, and adapter-edge fixture manifests
  now live under `pkg/antfly/src/sql/fixtures/`; API integration tests reference
  those SQL-owned manifests instead of owning corpus inputs.
- Catalog prebinding for generated simple read sources now consumes retained
  generated source-table token metadata and fails closed when that metadata is
  inconsistent, instead of always rediscovering the source table from tokens.
- Catalog prebinding for covered simple generated CTE reads now also consumes
  retained CTE body source-table metadata, resolves CTE aliases from generated
  CTE items, and fails closed when CTE body source metadata is inconsistent.
- Query request contracts and public query parser helpers now live under
  `pkg/antfly/src/query/`; SQL query-function lowering uses that neutral
  contract directly instead of importing the API package.
- Numeric string-cast validation stays allocation-free and does not parse JSON
  during lexing; broader JSON literal parsing remains deferred to semantic
  lowerers that actually need typed JSON.

The target shape is:

```text
one SQL input
  -> one tokenization
  -> one raw parse
  -> zero or one semantic plan
  -> reusable structured summaries for tests and fixtures
```

This mirrors the useful PostgreSQL lessons: avoid scanner backtracking, keep
raw parse separate from semantic analysis, reset transient memory in bulk, and
avoid repeated parse/lower passes over the same statement.

## Long-Term Production Shape

Raw SQL should exist only at external boundaries: HTTP, SQL wire protocol,
CLI, MCP, A2A, fixtures, and tests. Once a request enters the SQL adapter, the
first step should construct a shared parsed object. Every internal phase should
then consume typed parser, binder, and plan objects rather than reparsing SQL
text or probing independent string-oriented lowerers.

The production design is a small compiler frontend for Antfly-native plans,
not a collection of SQL-shaped helpers. PostgreSQL is the compatibility grammar
and the main reference for session/name-resolution behavior, but the durable
contract is Antfly's catalog, row, index, role, extension, job, lake, backup,
and storage services.

The design has three hard rules:

1. SQL text is an ingress format only. Durable metadata, job payloads, index
   definitions, role settings, extension state, backup scopes, and storage
   requests store typed Antfly objects, never SQL strings.
2. Parsing is catalog-free. It produces source-span-preserving syntax trees and
   diagnostics without reading tables, roles, extensions, indexes, tablespaces,
   or storage metadata.
3. Binding is the first semantic boundary. Catalog identity, session defaults,
   privileges, object versions, schemas, placement policy, and dependency
   checks are resolved once into typed bound statements before planning or
   execution.

The production pipeline should be:

```text
raw SQL
  -> ParsedSql
  -> BoundSqlStatement
  -> LogicalSqlPlan
  -> Antfly execution/storage plan
```

`ParsedSql` should own the statement's immutable parse-time view:

- original SQL source and source byte spans;
- shared token stream and compact keyword metadata;
- top-level statement family and statement kind;
- raw statement tree;
- nested statement nodes for `EXPLAIN`, CTEs, subqueries, relation population,
  `INSERT ... SELECT`, `MERGE`, and other embedded read/write sources.

The raw AST should be separate from semantic binding. Parser output should be
cheap to construct, source-span preserving, and independent of catalog state.
It should not normalize names into durable catalog identity, evaluate
privileges, infer table schemas, parse JSON literals eagerly, or allocate
storage-owned plan data.

After parsing, a catalog/session-aware binder should produce
`BoundSqlStatement`. Binding should resolve:

- current database, current namespace/schema, and `search_path`;
- table, index, extension, role, database, namespace, and tablespace names;
- catalog object ids and catalog versions;
- source and target schemas for read/write lowering;
- role and privilege checks;
- dependency and placement metadata needed by DDL.

Statement-family dispatch should happen from typed parser variants rather than
from sequential lowerer probes. The core families should be explicit:

- `ParsedReadStatement`;
- `ParsedWriteStatement`;
- `ParsedDdlStatement`;
- `ParsedExplainStatement`;
- `ParsedTransactionStatement`;
- `ParsedSessionStatement`.

Nested SQL must be represented as nested parsed nodes, not as extracted SQL
strings. For example:

- `EXPLAIN` should carry `Explain(statement: *ParsedStatement)`;
- `CREATE TABLE AS` and relation population should carry a
  `ParsedReadStatement` source;
- `INSERT ... SELECT` should carry a parsed source read statement;
- `MERGE` should carry typed target and source statement nodes;
- CTEs and subqueries should remain structured children of the enclosing
  statement.

That shape keeps nested dispatch from paying a second tokenize/parse cost and
lets diagnostics point at the original statement source instead of reconstructed
fragments.

Errors should be diagnostic objects from the phase that detected them, not
opaque lowerer failures. Parser, binder, planner, and executor errors should
carry:

- SQLSTATE or stable Antfly error code;
- phase (`parse`, `bind`, `plan`, or `execute`);
- source byte span;
- message;
- optional hint.

Fixture and coverage validation should inspect structured plan summaries or
coverage bits emitted by parser, binder, and lowerer phases. They should not
scan SQL strings or fingerprints as a second parser.

Implementation note: app-parity fixture summaries now include structured
`explain_subject` and `explain_inner_kind` fields for `EXPLAIN` coverage. The
summary regression checks prefer those fields and keep fingerprint parsing only
as compatibility coverage for old fixture rows, so refreshed fixtures can prove
explain subject/inner-kind behavior without treating the fingerprint string as
another SQL grammar.

JSON and JSONB literals should stay as source spans or token references during
lexing, classification, and raw parsing. Only semantic phases that need typed
JSON values should pay parse/stringify costs.

The end state should feel like this at API call sites:

- external callers may submit SQL text;
- adapter entrypoints immediately create `ParsedSql`;
- catalog/session-aware paths bind that object into `BoundSqlStatement`;
- planners lower bound statements into native logical plans;
- executors and lifecycle services never need to know the request started as
  SQL.

### Production Ownership Model

The clean long-term shape is to treat SQL as a compiler frontend and Antfly
services as the only owners of durable behavior. The SQL adapter should know
PostgreSQL syntax, session rules, diagnostics, and how to lower syntax into
Antfly-native intent. It should not own catalog mutations, role policy,
extension state, index builds, storage placement, lake access, backup/restore
semantics, or distributed job admission.

That means each supported statement family should have one typed handoff:

| SQL surface | Adapter output | Durable owner |
| --- | --- | --- |
| `SELECT`, joins, aggregates, windows, CTEs | Bound read plan | Row/query planner and storage read vtables |
| `INSERT`, `UPDATE`, `DELETE`, `MERGE` | Bound write or mutation-source plan | Row write, claim, 2PC, and storage write vtables |
| Table, database, namespace, tablespace DDL | Catalog lifecycle plan | Catalog service and metadata consensus path |
| Index and derived-artifact DDL | Derived-index lifecycle plan | Index lifecycle, build, catch-up, and artifact jobs |
| Role and grant SQL | Role lifecycle/authorization plan | Antfly user and role management service |
| Extension SQL | Extension lifecycle plan | Extension package/install/update/drop service |
| `ALTER ...` work that needs asynchronous validation | Typed lifecycle job request | Shared durable jobs service admitted through metadata ownership |
| Lake, foreign-source, backup, restore SQL | Catalog-targeted storage/source plan | Lake, backup/restore, and storage services |
| Session commands | Typed session mutation plan | SQL session state owner |

The metadata leader or equivalent catalog owner should be the only component
that commits catalog state transitions and lifecycle job records. SQL execution
may prepare and validate a lifecycle plan, but it should not independently
persist metadata, schedule background work, or mutate placement state outside
the shared catalog/job service. This keeps REST, SQL, MCP, A2A, CLI, SDK, and
internal automation on the same concurrency, authorization, audit, and retry
model.

Native APIs should not call back into SQL to get behavior. The dependency
direction is one-way:

```text
SQL syntax -> parsed/bound SQL objects -> native Antfly plans -> shared services
REST/SDK/MCP/A2A/CLI typed requests --------------------------^
```

If REST and SQL need the same operation, the shared service or plan builder is
the reusable API. SQL-specific code may adapt syntax into that API, but the
shared API must not accept SQL text as its semantic payload.

For long-running `ALTER` and validation-heavy DDL, the production model should
be:

1. Parse SQL into a raw statement with spans.
2. Bind catalog identity, session defaults, authorization, object versions, and
   dependency facts.
3. Build a typed lifecycle plan that describes the requested durable state.
4. Submit that plan to the catalog/job owner, which records an idempotent job,
   performs compare-and-swap catalog transitions, and owns retry/audit state.
5. Expose status through the same job and lifecycle APIs used by non-SQL
   callers.

Fail-closed behavior belongs at the first phase with enough information to make
the decision. Syntax gaps fail in parse/classify, missing catalog semantics fail
in bind, unsupported native lifecycle semantics fail in plan, and runtime
storage or placement failures fail in execution with typed diagnostics.

### Production Module Boundaries

The long-term implementation should split SQL adapter ownership by phase:

| Phase | Owns | Must not own |
| --- | --- | --- |
| Lexer/tokenizer | borrowed tokens, source spans, keyword metadata, literal spans | catalog names, typed JSON values, durable metadata |
| Raw parser | raw statement tree, nested statement nodes, parse diagnostics | catalog lookup, privilege checks, storage plans |
| Binder | session state, catalog identity, object versions, schemas, privileges | SQL string rewriting, storage execution |
| Logical planner | typed logical read/write/DDL/session plans | backend-specific storage keys, compatibility SQL strings |
| Lowerer/executor bridge | Antfly row, catalog, index, role, extension, job, and storage requests | parser probes, catalog-free name guessing |
| Fixture/coverage tooling | structured summaries and coverage bits emitted by phases | substring scans that behave like another parser |

Each phase should receive the previous phase's typed object plus explicit
context. Hidden access to raw SQL text should be treated as a compatibility
escape hatch and removed as the corresponding raw AST node becomes available.

The preferred code shape is:

- `TokenizedSql`: one owned token list for the input, with source spans and
  compact keyword metadata.
- `ParsedSql`: one raw parse tree for the input, including nested parsed
  children for embedded statements and non-contiguous token ranges where SQL
  syntax rearranges a source statement, such as `SELECT ... INTO ... FROM ...`.
- `BoundSqlStatement`: the parsed statement plus SQL session state, resolved
  catalog targets, object versions, schemas, and authorization decisions.
- `LogicalSqlPlan`: catalog-free execution intent expressed in Antfly-native
  plan families.
- `LoweredReadPlan` / `LoweredWritePlan` / lifecycle plans: direct calls into
  the shared Antfly services.

The migration order should keep behavior stable while removing compatibility
bridges:

1. Route every SQL entrypoint through `ParsedSql` before classification,
   diagnostics, fixture summaries, or lowering.
2. Replace nested SQL substrings with parsed child statements cloned from the
   parent token stream. Non-contiguous children should use token ranges instead
   of reconstructed SQL text.
3. Move all name resolution into `BoundSqlStatement` and make binder APIs take
   an explicit SQL session object.
4. Require read/write/DDL/session dispatch to use typed statement variants
   rather than sequential lowerer probes.
5. Emit fixture summaries and coverage from parsed, bound, or logical plan
   objects instead of scanning SQL strings or fingerprints.
6. Delete compatibility fallbacks only after parity fixtures cover the native
   typed path for the corresponding statement family.

### Target Adapter Architecture

The best long-term production shape is a phase-separated SQL frontend with
typed contracts between every phase. The adapter should have one sanctioned
raw-SQL ingress path and no hidden parser or lowerer islands behind individual
features.

The intended module shape is:

| Module | Primary types | Contract |
| --- | --- | --- |
| `lexer` / `tokenized` | `TokenizedSql`, `Token`, `Keyword`, `Span` | Tokenize once, attach keyword and span metadata, preserve literal source slices, and avoid semantic allocation. |
| `parser` | `ParsedSql`, `ParsedStatement`, statement-family raw nodes | Build a catalog-free raw AST with nested statement children and span-aware diagnostics. |
| `binder` | `SqlSession`, `BoundSqlStatement`, `ResolvedCatalogTarget`, `BoundSchema`, authz results | Resolve database, namespace, search path, schemas, object versions, privileges, dependencies, and placement facts once. |
| `planner` | `LogicalSqlPlan`, `ReadPlan`, `WritePlan`, lifecycle plans | Convert bound statements into native Antfly intent without storage-specific keys or SQL text. |
| `bridge` / service adapters | row, catalog, index, role, extension, job, lake, backup, storage requests | Call shared Antfly services through typed request objects only. |
| `diagnostics` | `SqlDiagnostic`, stable codes, hints | Carry phase, code, span, message, and optional hint consistently across parse, bind, plan, and execute. |
| `fixtures` / parity tooling | structured summaries, coverage bits | Validate behavior from emitted typed summaries instead of rescanning SQL strings. |

The public API boundary should be deliberately narrow:

- External facades may accept `[]const u8` SQL text. They must immediately
  construct `ParsedSql` and pass that object forward.
- Internal lowering APIs should accept `ParsedSql`, `ParsedStatement`,
  `BoundSqlStatement`, or `LogicalSqlPlan`, not raw SQL text.
- Catalog-aware operations must require `BoundSqlStatement`. A read/write/DDL
  path that can affect catalog targets, roles, extensions, tablespaces, lakes,
  backup scopes, or storage placement should not have an unbound shortcut.
- Durable service requests must be Antfly-native structs. They must not include
  SQL fragments as the source of truth for metadata, lifecycle jobs, indexes,
  role settings, extension state, backup/restore scope, or storage routing.

Statement-family dispatch should be a closed typed decision, not a sequence of
feature probes. The parser should produce explicit variants for read, write,
DDL, session, transaction, and explain statements. Each variant should carry a
compact kind enum that planners can switch on directly. Unsupported shapes
should fail from the earliest phase that can name the missing native model:
syntax in parse, catalog/session gaps in bind, lifecycle/storage gaps in plan,
and backend failures in execute.

Nested SQL should remain structural all the way through planning. `EXPLAIN`,
CTEs, subqueries, relation population, `INSERT ... SELECT`, `UPDATE ... FROM`,
`DELETE ... USING`, `MERGE`, and future procedure-like constructs should refer
to child raw nodes or token ranges in the parent `ParsedSql`. They should never
reconstruct a SQL substring and call a second parser as part of normal
execution.

Memory ownership should make repeated parity and fixture runs cheap:

- Source SQL is borrowed at ingress and owned only by `ParsedSql` when the
  caller needs it to outlive the request frame.
- Tokens, raw nodes, diagnostics, and intermediate binding data live in a
  resettable per-statement arena.
- Native plans and durable service requests own only the normalized data they
  need after SQL planning finishes.
- JSON and JSONB literals stay as source spans until expression binding or a
  semantic phase explicitly needs typed JSON values.

Distributed lifecycle work should flow through the same owner as non-SQL APIs.
SQL can parse, bind, authorize, and build the requested lifecycle plan, but the
catalog or metadata leader should commit catalog transitions, admit durable
jobs, assign idempotency keys, and own retry/audit state. This applies to
`ALTER TABLE`, derived-index builds, extension install/update/drop, role
changes, tablespace placement changes, database/namespace lifecycle,
backup/restore, and lake or foreign-source lifecycle.

Review should reject new SQL work that bypasses these boundaries. A new
statement is production-shaped only when it adds raw AST coverage, typed
binding, a native logical plan, structured diagnostics, and parity coverage for
the corresponding REST/SDK/MCP/A2A/CLI behavior. Syntax-only acceptance is not
enough.

### Completion Design

The remaining production work should converge on one typed route per statement
family. Compatibility helpers may stay as public facades for tests, CLI, or
older call sites, but they should immediately create or receive `ParsedSql` and
then delegate to parsed, bound, and logical-plan APIs. New SQL features should
not add raw-string lowerer entrypoints unless the function is explicitly an
external ingress wrapper.

The long-term endpoint for parser and lowerer work is:

```text
Adapter ingress
  -> TokenizedSql
  -> ParsedSql with complete raw statement nodes
  -> BoundSqlStatement with session/catalog/authorization facts
  -> LogicalSqlPlan with native Antfly operation intent
  -> shared row/catalog/index/role/extension/job/storage service
```

To get there, finish these tracks in order:

1. **Complete raw AST coverage.** Promote CTEs, subqueries, relation
   population sources, `INSERT ... SELECT`, `UPDATE ... FROM`,
   `DELETE ... USING`, `MERGE`, session commands, and explain subjects from
   token-range compatibility nodes into first-class parsed children. Token
   ranges are acceptable only when a SQL shape is non-contiguous and the child
   still points back to the original source spans.
2. **Make binding mandatory for catalog-aware work.** Reads, writes, DDL,
   extension operations, role operations, tablespace operations, backup/restore,
   and lake/foreign-source access should all pass through `BoundSqlStatement`
   before planning. Binding owns `current_database`, `search_path`, object
   versions, schema lookup, dependency checks, and role authorization.
3. **Replace probe dispatch with variant dispatch.** Once a statement has a
   `ParsedStatement` variant, route by that variant and statement kind. Legacy
   "try this lowerer, then the next lowerer" paths should shrink to temporary
   fallback coverage for statement families that do not yet have a complete raw
   AST.
4. **Use native plans for every durable effect.** SQL DDL should emit catalog,
   index, role, extension, job, table, namespace, database, tablespace,
   backup/restore, and lake-source plans directly. No durable service should
   inspect SQL text to understand the request.
5. **Make diagnostics span-first.** Unsupported-shape errors, parse errors,
   bind errors, and planning errors should carry stable source spans and a
   phase-specific code. String-only unsupported reasons are compatibility debt
   unless paired with a diagnostic span.
6. **Move fixture validation to structured summaries.** Fixture freshness,
   coverage, and parity checks should consume parser/binder/plan summaries or
   emitted coverage bits. They should not scan SQL text or plan fingerprints in
   ways that duplicate parser behavior.
7. **Keep lexical work cheap.** Keyword classification, quoted identifier
   handling, numeric validation, and literal source spans belong in lexical or
   raw parse metadata. JSON/JSONB literal parsing, type coercion, expression
   binding, and catalog-dependent checks belong in semantic phases only.

Acceptance for this design is not "the parser accepts PostgreSQL syntax." The
acceptance bar is that every supported SQL statement has the same durable effect
as the equivalent Antfly-native API call, every unsupported statement names the
missing native model, and no internal subsystem needs to recover semantics from
SQL strings after binding.

### Production Acceptance Checklist

The production cutoff should be based on removing adapter-local behavior, not
on adding more PostgreSQL syntax. A SQL statement family is production-shaped
only when all of these are true:

- External entrypoints parse once into `ParsedSql`; nested statements reuse
  parsed children or source token ranges, never reconstructed SQL strings.
- Catalog-aware statements bind once into `BoundSqlStatement`, carrying the
  SQL session, resolved catalog targets, object versions, source schemas,
  authorization results, and dependency facts needed by planning.
- Dispatch is variant-based. Read, write, DDL, session, explain, and
  transaction planning route from typed statement variants and explicit
  statement kinds instead of "try lowerer A, then lowerer B" probing.
- Durable behavior is native. Catalog, table, namespace, database, tablespace,
  index, extension, role, lake, backup/restore, and job effects are expressed
  as shared Antfly lifecycle or execution plans.
- The metadata/catalog owner admits and records lifecycle jobs for asynchronous
  work. SQL can prepare and validate the request, but it does not separately
  commit catalog transitions or schedule durable background work.
- Backend storage/read/write vtables receive typed catalog targets and typed
  plans. Compatibility storage keys and foreign-source query-string bridges are
  treated as migration debt.
- Diagnostics carry phase and span. Unsupported SQL has a stable parse, bind,
  plan, or execute error that names the missing native model and points at the
  relevant source range.
- Fixture and parity checks consume structured summaries or coverage bits from
  parser, binder, planner, and lowerer phases, not substring scans over SQL or
  fingerprints.

Compatibility wrappers are acceptable only at ingress boundaries and tests.
Their job is to build or receive `ParsedSql`, invoke the shared binder/planner,
and disappear once the caller can use the typed API directly. New features
should be rejected in review if they add another durable SQL-string bridge, an
adapter-owned metadata path, or a second parser hidden inside fixture checking.

### Production Migration Design

The production migration should make the typed path the easy path and make
compatibility paths visibly temporary. The stable internal API should be:

```text
parseSql(sql) -> ParsedSql
bindSql(parsed, session, catalog_snapshot, principal) -> BoundSqlStatement
planSql(bound) -> LogicalSqlPlan
executeSqlPlan(plan, services) -> typed result or lifecycle job
```

Raw SQL entrypoints may remain for HTTP, SQL wire protocol, CLI, MCP, A2A, and
tests, but they should be thin wrappers around this API. Feature code should not
grow new `[]const u8` SQL parameters after parse unless the parameter is only
for diagnostics or fixture provenance.

The migration should converge on these concrete invariants:

- Parser ownership is singular. `ParsedSql` owns the token stream, keyword
  metadata, raw statement tree, nested statement references, and diagnostic
  spans for one SQL input.
- Dispatch is closed over statement variants. Read, write, DDL, session,
  transaction, and explain planners switch on `ParsedStatement` and explicit
  kind enums instead of trying lowerers in sequence.
- Binding is required before any catalog-aware effect. The binder owns
  `current_database`, `search_path`, table/schema lookup, object versions,
  role authorization, dependency facts, extension identity, tablespace policy,
  lake/source identity, and backup/restore scope.
- Logical planning is Antfly-native. Plans carry row/query expressions,
  catalog lifecycle requests, derived-index configs, role mutations, extension
  lifecycle requests, job requests, session mutations, lake-source plans, and
  backup/restore scopes as typed structs.
- Durable execution is service-owned. The catalog/metadata leader or equivalent
  owner commits metadata transitions and admits lifecycle jobs; SQL never
  persists an alternate metadata record or schedules background work directly.
- Fixture validation is structural. Corpus freshness and parity coverage consume
  parser, binder, planner, or lowerer summaries and coverage bits rather than
  substring scans over SQL or plan fingerprints.

The preferred removal order is:

1. Convert each remaining raw-string lowerer facade into a parsed facade that
   immediately builds or receives `ParsedSql`.
2. Promote token-range compatibility nodes into raw AST children when the shape
   is contiguous; keep token ranges only for syntax that is naturally
   non-contiguous while still preserving original spans.
3. Move catalog, role, extension, tablespace, lake, and backup/restore
   decisions behind binder APIs that require an explicit SQL session and
   authenticated principal.
4. Replace probe-based planners with `switch` dispatch over parsed statement
   variants and kind enums.
5. Replace durable SQL fragments in metadata or job payloads with typed native
   plan fields.
6. Delete the compatibility path once parity fixtures cover the equivalent
   typed route across SQL and the native API surface.

This is also the review checklist for new SQL work. A change is not production
complete if it accepts syntax without a raw AST node, stores SQL text as durable
semantics, bypasses `BoundSqlStatement` for catalog-aware work, schedules jobs
outside the metadata owner, or adds fixture scans that reconstruct parser
behavior.

### Production Cutover Gates

The best long-term shape should be enforced by cutover gates instead of a
single large rewrite. Each gate removes one class of SQL-adapter debt and
should be complete for a statement family before that family is considered
production-supported.

1. **Ingress normalization gate.** Every external SQL surface constructs
   `ParsedSql` exactly once, including HTTP SQL execution, SQL wire protocol,
   CLI, MCP, A2A, fixture loading, and test helpers. Any compatibility facade
   that still accepts raw SQL must be a leaf wrapper whose first real operation
   is parse.
2. **Raw AST gate.** The parser owns the statement's structural shape, nested
   statements, source ranges, statement family, and statement kind. Lowerers
   are not allowed to rediscover statement shape by scanning SQL text.
3. **Binding gate.** Catalog-aware statements require `BoundSqlStatement`
   before planning. This includes reads and writes with table targets, DDL,
   roles, extensions, databases, namespaces, tablespaces, indexes, lakes,
   foreign sources, backup/restore, and asynchronous lifecycle work.
4. **Native plan gate.** Planning emits Antfly-native typed plans only. Durable
   jobs, metadata records, index definitions, role mutations, extension
   lifecycle changes, storage targets, backup scopes, and foreign-source
   dispatch payloads must not store SQL text as their semantic source.
5. **Owner handoff gate.** Execution hands native plans to the shared Antfly
   owner for the behavior: row/query services for reads, write/storage vtables
   for mutations, catalog metadata ownership for lifecycle transitions, durable
   jobs for long-running work, role management for authorization state,
   extension services for package lifecycle, and lake/backup/storage services
   for their domains.
6. **Diagnostic gate.** Parse, bind, plan, and execute failures return stable
   diagnostics with phase, code, span, message, and optional hint. Unsupported
   SQL must fail at the earliest phase that can name the missing native model.
7. **Parity gate.** SQL, REST, SDK, MCP, A2A, CLI, and internal automation
   exercise the same native service path. Fixtures assert structured summaries
   and coverage bits rather than SQL substrings or plan fingerprints that act
   like another parser.

After these gates are in place, feature review should be simple: syntax support
is not enough. A statement family is complete only when it has a raw AST node,
binding coverage, native planning, service-owned execution, span-aware
diagnostics, and cross-surface parity evidence.

## Parity and Test Strategy

SQL compatibility is proven by typed outcomes, not by accepting more syntax than
the engine can execute.

The parity suite should assert:

- supported statements lower to typed native plans or execute through storage;
- unsupported statements include the missing native model feature;
- adapter-only no-ops are explicit and reasoned;
- SQL and REST/SDK typed requests produce the same row, schema, index, role, and
  catalog behavior;
- session state affects name resolution only through the typed SQL session;
- fixtures validate structural plan summaries, not arbitrary non-empty strings;
- EXPLAIN fixtures validate the structured inner plan family, including MERGE,
  instead of accepting a generic write plan wrapper;
- bind parameters, aliases, CTE output schemas, result schemas, and catalog
  targets are checked structurally.

Focused CI should keep a fast parser/lowerer target, a fixture freshness target,
and broader cross-surface parity gates for HTTP, SQL, MCP, A2A, CLI, roles,
extensions, tablespaces, backup/restore, lakes, and derived indexes.

## Relationship to Other Docs

- [RELATIONAL.md](RELATIONAL.md) owns the relational table, row, expression,
  constraint, query, and derived-index model.
- [DATABASES.md](DATABASES.md) owns database, namespace, table, tablespace, and
  catalog-target semantics.
- [EXTENSIONS.md](EXTENSIONS.md) owns package and installed-extension lifecycle.
- [LAKES.md](LAKES.md) owns external lake row sources and sidecar artifact
  semantics.

SQL should reference those contracts and lower into them. It should not fork
their semantics.
