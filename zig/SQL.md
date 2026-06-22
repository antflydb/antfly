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
  separate tokenization paths. DML adapter write-plan regression callbacks and
  direct DML test facades also consume borrowed parsed tokens instead of
  round-tripping through SQL text. DDL fingerprint regression helpers parse once
  through `ParsedSql` before computing structured fingerprints. Antfly query
  function lowering has parsed entrypoints, and app-parity query-function
  checks reuse `ParsedSql` instead of tokenizing fixture SQL directly.
  Corpus fixture parameter coverage parses fixture SQL once and validates
  placeholders from parsed token spans instead of using a separate raw string
  scanner.
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
  family/kind before scanning source-table tokens.
- Identifier tokens carry optional compact keyword metadata, and the shared
  parser/classifier helpers use it before falling back to text comparison.
- Tokens expose stable source spans, quoted identifiers keep quoted-source
  spans without being marked as keywords, and diagnostics have a span-bearing
  `SqlDiagnostic` shape for unsupported classifications.
- Fixture fingerprint checks use a structured `PlanFingerprintView` scanner with
  explicit exact-token and suffix-token modes instead of open-coded substring
  searches. Lateral and window expression coverage checks now use emitted plan
  tokens rather than matching SQL spelling. Fixture parameter coverage now uses
  `ParsedSql` placeholder tokens instead of a separate raw SQL byte scanner.
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
