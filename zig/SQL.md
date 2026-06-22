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
  separate tokenization paths. DML adapter write-plan regression callbacks and
  direct DML test facades also consume borrowed parsed tokens instead of
  round-tripping through SQL text. DDL fingerprint regression helpers parse once
  through `ParsedSql` before computing structured fingerprints. Antfly query
  function lowering has parsed entrypoints, and app-parity query-function
  checks reuse `ParsedSql` instead of tokenizing fixture SQL directly.
  Parsed-only lowering context construction does not require a raw SQL field.
- Identifier tokens carry optional compact keyword metadata, and the shared
  parser/classifier helpers use it before falling back to text comparison.
- Tokens expose stable source spans, quoted identifiers keep quoted-source
  spans without being marked as keywords, and diagnostics have a span-bearing
  `SqlDiagnostic` shape for unsupported classifications.
- Fixture fingerprint checks use a structured `PlanFingerprintView` scanner with
  explicit exact-token and suffix-token modes instead of open-coded substring
  searches. Lateral and window expression coverage checks now use emitted plan
  tokens rather than matching SQL spelling.
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
