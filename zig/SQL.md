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

Raw SQL text must not be stored as index metadata, role metadata, extension
metadata, row rewrites, or storage requests. If a feature needs durable
expression behavior, lower it to the shared row-expression AST first.

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

- `sql_adapter/tokenized.zig` owns reusable `TokenizedSql` state for a SQL input,
  including the borrowed token stream, top-level statement family, read
  statement kind, and write statement kind.
- `classifier.zig` classifies read families (`query`, `set_operation`,
  `recursive_cte`, `aggregate`, `join`, `lateral`, and `window`) and write
  families from the shared token stream.
- Read lowering dispatches through the classified statement family before
  falling back to the legacy probe order for unclassified or future shapes. Set
  operation-shaped SQL still tries the query optimizer first so existing
  predicate-rewrite plans remain available before the native set-operation plan.
- Write lowering consumes `TokenizedSql` for write-family selection instead of
  tokenizing separately from classification.
- `ParsedSql` wraps `TokenizedSql` with a raw statement view and byte-source
  span. DDL lowering, catalog-apply test lowering, read statement dispatch, and
  write statement dispatch now use that shared wrapper as the top-level SQL
  object. EXPLAIN and write-plan dispatch have parsed lowering overloads.
  Existing lowerer callbacks still accept SQL text for compatibility; migrating
  those callbacks to borrowed `ParsedSql`/token slices is the next mechanical
  step toward zero selected-lowerer re-tokenization.
- Identifier tokens carry optional compact keyword metadata, and the shared
  parser/classifier helpers use it before falling back to text comparison.
- Tokens expose stable source spans, quoted identifiers keep quoted-source
  spans without being marked as keywords, and diagnostics have a span-bearing
  `SqlDiagnostic` shape for unsupported classifications.
- Fixture fingerprint checks use a structured `PlanFingerprintView` scanner with
  explicit exact-token and suffix-token modes instead of open-coded substring
  searches.
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
