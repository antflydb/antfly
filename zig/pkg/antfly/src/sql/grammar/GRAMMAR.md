# Antfly SQL Grammar Migration

Antfly SQL should become PostgreSQL-compatible at the user surface without
embedding PostgreSQL's parser as the engine boundary. PostgreSQL grammar and
CockroachDB grammar behavior are references for compatibility. Antfly owns the
grammar, token model, AST, lowering, diagnostics, and execution semantics.

The production target is:

```text
SQL bytes
  -> Antfly SQL scanner
  -> generated Antfly SQL parser
  -> catalog-free Antfly SQL AST
  -> statement-family classifier
  -> binder
  -> typed logical plan
  -> shared Antfly service
```

The generated parser must not become a second control plane. It recognizes
syntax and builds raw AST nodes with source spans. Catalog lookup, role checks,
storage visibility, derived-index lifecycle, graph metric state, Lite behavior,
and execution all stay in the binder, planner, and shared service layers.

The reusable generator machinery lives under `zig/lib/yacc`, following the same
library-plus-codegen shape as `zig/lib/openapi`. Antfly SQL owns only the input
grammar, migration docs, and checked-in generated output under this directory.
Use `zig build regen-sql-grammar` to refresh checked-in grammar metadata and
`zig build sql-grammar-generated-check` to verify it is current.

The first generator implementation builds deterministic SLR parser tables:
grammar parsing, production validation, nullable/FIRST/FOLLOW sets, LR(0)
states, indexed action/goto tables, source-aware syntax diagnostics, and
structured conflict reporting. The current broad Antfly SQL seed grammar
generates conflict-free parser metadata. The first runtime integration observes
the generated parser on the `ParsedSql` path but does not require grammar parity
before the existing parser can handle a statement. When the generated parser
accepts a statement, `ParsedSql` retains a source-span-bearing generated raw
node and uses it for the first migrated statement variants. Session,
transaction, and prepared statements now require generated parser success;
they also carry the first generated AST payload with source spans and token
ranges for command names, values, prepared-statement arguments, and nested
prepared statements. This AST is retained by `ParsedSql`; prepared statements
now have an AST-to-plan conversion path with parity coverage against the
existing token-based lowerer, including typed `PREPARE name(type, ...) AS ...`
parameter lists. Session catalog commands now have generated AST-to-plan parity
for the generated-covered `SET`, `RESET`, `SHOW`, and `DISCARD ALL` forms.
Transaction boundary commands now have generated AST-to-plan parity for
generated-covered `BEGIN`, `COMMIT`, and `ROLLBACK` adapter-noop boundaries.
Simple DDL has generated-parser corpus coverage but still falls back to the
existing parser when the seed grammar does not yet cover the shape; generated
simple DDL ASTs now carry structured object, option, and behavior fields for
database, schema, and extension create/drop catalog plans and lower those
catalog plans directly from generated AST ranges, plus generated AST-to-plan
parity for seed `CREATE TABLE` and `CREATE INDEX` forms using the same parser
options as the existing lowerer. Simple DML now has generated-parser corpus
coverage, retained generated raw and AST nodes for covered write statements,
structured generated DML ranges for target tables, sources, assignments,
predicates, conflict clauses, returning clauses, values lists, default-values
inserts, and truncate options. Supported explicit-column `INSERT ... VALUES`
plans, including `ON CONFLICT` actions and field/all-field/expression
`RETURNING` lists, and `INSERT ... DEFAULT VALUES` plans, including
`ON CONFLICT` actions and returning lists, now lower directly from generated
AST ranges into relational row batches. Supported explicit-column
`INSERT ... SELECT` plans, including `ON CONFLICT` actions and returning lists,
now validate generated source, conflict, and returning ranges before direct
insert-source lowering. Single-table point `UPDATE` and `DELETE` statements with generated
`WHERE` ranges and field/all-field/expression `RETURNING` lists now also lower
directly from generated AST ranges into relational row batches. Table-wide and
single-table source `UPDATE` and `DELETE` statements without joined
`FROM`/`USING` sources now validate generated AST ranges before direct
mutation-source lowering. Explicit `UPDATE ... FROM` and `DELETE ... USING`
joined mutation-source statements now validate generated target, source,
predicate, and returning ranges before direct joined mutation-source lowering,
non-CTE `MERGE` statements now validate generated target, source, and
`ON`/arm ranges before direct merge-plan lowering, and `TRUNCATE` lowers
directly from generated AST ranges into mutation-source plans. Other DML shapes still use an initial generated
AST-to-plan wrapper that fails closed if the generated DML family does not
match the existing write classifier before delegating to the current typed DML
lowerer. Unsupported DML still falls back, and deeper DML cutover still
requires replacing token-based command-body parsing with complete generated AST
payloads for broader `INSERT ... SELECT` source bodies, semijoin/exists joined
mutation bodies, and full `MERGE` arm bodies.
Representative
read queries now have generated-parser corpus coverage, retained generated raw
and AST nodes for covered read statements, top-level generated AST ranges for
covered `SELECT` projections, sources, predicates, grouping, having filters,
window clauses, ordering, pagination, set-operation tails, and CTE prefixes,
list-level generated metadata for top-level projection, grouping, and ordering
items, and first-join generated metadata for left input, right input, and join
predicate ranges, plus simple top-level comparison expression metadata for
covered `WHERE`, `HAVING`, and join predicates. Normal function-call argument
lists are accepted in generated expression grammar, including top-level
projection functions with comma-separated arguments, and positive `LIKE`,
`ILIKE`, `IN (...)`, and `BETWEEN ... AND ...` predicates are accepted and
classified in generated expression metadata along with their `NOT` negated
forms. `ANY`/`ALL`/`SOME` quantified comparison predicates over parenthesized
expression lists are also accepted and classified with explicit quantifier
token ranges. `IS NULL` and `IS NOT NULL` predicates are accepted and
classified as explicit null-test expression kinds. Top-level `AND` and `OR`
predicates are classified as logical-expression metadata with left and right
token ranges and child expression-kind summaries, while `BETWEEN ... AND ...`
remains classified as a range predicate. Prefix `NOT` predicates are accepted
and classified with right-side expression-kind summaries,
and an initial generated AST-to-plan wrapper that validates those ranges and
fails closed if the generated read family is incompatible with the existing
read classifier. Simple query, aggregate, join, and lateral reads now validate
generated clause ranges before calling their typed read-plan lowerers directly;
basic `OVER (PARTITION BY ... ORDER BY ...)` window reads now classify as a
generated window family, minimal named `WINDOW ... AS (PARTITION BY ... ORDER
BY ...)` clauses are accepted and ranged, seed `ROWS`/`RANGE` frame tails are
accepted, and window reads dispatch directly after validating projection/source
ranges;
plain `DISTINCT` and `DISTINCT ON (...)` reads now carry generated distinct
ranges and match the production aggregate/query-family split;
generated set-operation reads now classify as a distinct read family and
validate the left query and set-operation tail before calling the set-operation
lowerer directly;
single- and multi-CTE reads now expose generated CTE-list, first-CTE, and
last-CTE name/body ranges, recursive CTE reads carry an explicit generated
recursive flag, and simple non-recursive CTE reads dispatch directly when those
ranges validate; generated pagination grammar now covers `LIMIT`, `OFFSET`,
and `FETCH FIRST`/`FETCH NEXT` query tails.
Unsupported read shapes
still fall back, and deeper read cutover still requires full generated
query-body AST payloads for expression-level projections and predicates,
complete join trees, complete expression AST nodes, complete per-CTE body AST
arrays, recursive CTE planning, aggregates, windows, ordering, pagination, and
direct generated read-plan lowering. The generated parser now also treats seed
graph DDL as a distinct graph statement family and `ParsedSql` retains those
generated raw and AST nodes. Seed graph index and graph metric statements now
have graph-specific generated AST-to-plan wrappers that lower to typed index
plans instead of only routing through the generic DDL family. The generated
facade now returns closed statement-family nodes for the covered families and
an explicit unsupported statement node for seed `ANALYZE` and simple `EXPLAIN`
forms with stable reason metadata; full production AST construction remains the
next migration boundary for larger DDL, query, DML, and Antfly extension
families.

## Compatibility Policy

PostgreSQL compatibility is a behavioral contract, not a source-code dependency
on PostgreSQL parser internals. PostgreSQL's grammar is tightly coupled to C
Bison actions, PostgreSQL parse-node types, catalog assumptions, extension
semantics, and release-specific server behavior. Antfly needs a smaller grammar
that maps directly to Antfly-native plans and fails closed for unsupported
semantics.

CockroachDB is the closer model: it keeps a SQL scanner, owns a generated
dialect grammar, produces its own AST, and uses PostgreSQL-compatible syntax as
a compatibility target. Antfly should follow that shape in Zig instead of
vendoring PostgreSQL grammar files wholesale.

Rules for new grammar work:

- Accept PostgreSQL syntax only when it maps to an Antfly-native typed plan or
  to an explicit unsupported-shape diagnostic.
- Preserve source byte spans for every AST node that can produce a diagnostic.
- Keep parser output catalog-free.
- Do not store raw SQL text as durable metadata, index definitions, graph
  metric configs, role settings, extension state, backup scopes, or job
  payloads.
- Prefer statement-family variants over lowerer probe order.
- Treat Antfly-specific graph, full-text, vector, enrichment, Lite, and
  algebraic-index syntax as first-class grammar branches, not post-parse string
  scans.

## Migration Plan

The migration should be incremental. The current parser stays on the production
path until generated coverage has parity for a statement family.

1. Define the supported Antfly SQL grammar subset in a checked-in grammar file.
   Start with statement families Antfly already executes: session commands,
   transactions, prepared statements, DDL, simple DML, row reads, graph DSL,
   derived indexes, full-text, Lite SQL, and extension/index options.
2. Add a parser compatibility corpus with accepted PostgreSQL-compatible
   examples, accepted Antfly extensions, and intentionally rejected PostgreSQL
   forms.
3. Run the generated parser in shadow tests against the corpus while the
   hand-written parser remains production.
4. For each statement family, prove that generated AST output lowers to the
   same typed plan shape as the current parser.
5. Switch one statement family at a time from hand-written parsing to generated
   parsing.
6. Delete obsolete hand-written parsing branches only after parity tests cover
   diagnostics, AST shape, binding, planning, and SQL/API behavior.

Suggested migration order:

1. Session and control statements: `SET`, `RESET`, `SHOW`, `DISCARD ALL`,
   `BEGIN`, `COMMIT`, `ROLLBACK`, `PREPARE`, `EXECUTE`, `DEALLOCATE`.
2. DDL: `CREATE DATABASE`, `CREATE SCHEMA`, `CREATE TABLE`, `ALTER TABLE`,
   `DROP`, `CREATE INDEX`, scalar/vector/full-text/graph index forms, graph
   metric declarations, and extension declarations. Simple database, schema,
   table, index, and extension DDL now has generated-parser corpus coverage
   when it matches the seed grammar. Database, schema, and extension create/drop
   catalog DDL now has structured generated AST payloads and direct generated
   AST-to-plan lowering. Unsupported DDL remains on the existing parser until
   each shape has raw AST parity.
3. Simple DML: `INSERT ... VALUES`, primary-key `UPDATE`, primary-key
   `DELETE`, `RETURNING`, and `ON CONFLICT`. Initial generated-parser coverage
   now retains raw and AST DML nodes for representative `INSERT ... VALUES`,
   `INSERT ... SELECT`, `UPDATE`, `DELETE`, `TRUNCATE`, and `MERGE` statements;
   generated DML ASTs now carry structured body ranges and conflict ranges.
   Explicit-column `INSERT ... VALUES` has direct generated AST-to-plan
   lowerers for supported row batches, including column-list, partial, and
   named constraint conflict targets, conflict actions, and field, all-field,
   and expression `RETURNING` lists; `INSERT ... DEFAULT VALUES` has a direct
   generated AST-to-plan lowerer for default row batches with conflict actions
   and returning lists; supported explicit-column `INSERT ... SELECT` has a
   generated range-validated direct insert-source lowerer for same-table and
   configured cross-table sources, conflict actions, and returning lists;
   single-table point `UPDATE` and `DELETE` have direct
   generated AST-to-plan lowerers for generated primary/unique selector ranges
   with field, all-field, and expression returning lists; table-wide and
   single-table source `UPDATE`/`DELETE` without joined sources have generated
   range-validated direct mutation-source lowerers; explicit `UPDATE ... FROM`
   and `DELETE ... USING` have generated range-validated direct joined
   mutation-source lowerers; non-CTE `MERGE` has a generated range-validated
   direct merge-plan lowerer; and `TRUNCATE` has a direct generated AST-to-plan
   lowerer for the supported table-list, identity, and drop-behavior surface.
   Other DML still has a validated wrapper into the current typed DML lowerer
   for representative generated-covered write plans. Switching the full DML
   family from fallback to required generated parsing still requires generated
   command-body ASTs for broader insert-select source bodies,
   semijoin/exists joined mutation bodies, full merge arms, and CTE/recursive
   merge forms, plus broader unsupported-shape diagnostics.
4. Read queries: projections, predicates, joins, CTEs, aggregates, windows,
   set operations, lateral, ordering, limits, and document-table sources.
   Initial generated-parser coverage now retains raw and AST read nodes for
   representative projection/filter/order/limit, grouped, join, lateral, and
   non-recursive CTE query shapes; generated read ASTs now carry top-level
   ranges for covered `SELECT` projection, source, `WHERE`, `GROUP BY`,
   `HAVING`, `WINDOW`, `ORDER BY`, `LIMIT`, `OFFSET`, `FETCH`, set-operation,
   and CTE-prefix bodies, plus list-level metadata for top-level projection,
   grouping, and ordering items and first-join metadata for left input, right
   input, and join predicate ranges, plus simple top-level comparison
   expression metadata for covered `WHERE`, `HAVING`, and join predicates; and
   normal function-call argument lists are accepted by the generated expression
   grammar for covered read projections, and positive `LIKE`, `ILIKE`,
   `IN (...)`, and `BETWEEN ... AND ...` predicates are accepted and classified
   in generated expression metadata along with their `NOT` negated forms.
   `ANY`/`ALL`/`SOME` quantified comparison predicates over parenthesized
   expression lists are accepted and classified with explicit quantifier token
   ranges. `IS NULL` and `IS NOT NULL` predicates are accepted and classified
   as explicit null-test expression kinds. Top-level `AND` and `OR` predicates
   are classified as logical-expression metadata with left and right token
   ranges and child expression-kind summaries, while `BETWEEN ... AND ...`
   remains classified as a range predicate. Prefix `NOT` predicates are
   accepted and classified with right-side expression-kind summaries.
   Generated read ASTs now have a validated wrapper
   into the current typed read lowerer for representative covered read plans
   that rejects malformed generated range payloads. Simple query reads now have
   a direct generated AST-to-query-plan lowering boundary after clause-range
   validation, and aggregate, join, and lateral reads now have direct generated
   AST-to-read-family dispatch boundaries after clause-range validation.
   Basic `OVER (PARTITION BY ... ORDER BY ...)` and named
   `WINDOW ... AS (PARTITION BY ... ORDER BY ...)` reads now classify as
   generated window reads, seed `ROWS`/`RANGE` frame tails are accepted in
   inline and named windows, and generated window reads dispatch to the typed
   window lowerer after range validation.
   Plain `DISTINCT` and `DISTINCT ON (...)` reads now carry generated distinct
   ranges and preserve the production aggregate/query-family split.
   Set-operation reads now classify as their own generated read family and
   dispatch to the native set-operation lowerer after validating the generated
   left-query and set-operation-tail ranges. Single- and multi-CTE reads now
   carry generated CTE-list, first-CTE, and last-CTE name/body ranges; recursive
   CTE reads carry an explicit recursive flag; and simple non-recursive CTE
   reads dispatch to the typed read lowerer after validating those ranges.
   Generated pagination coverage now accepts and ranges `LIMIT`, `OFFSET`, and
   `FETCH FIRST`/`FETCH NEXT` tails.
   Switching reads from fallback to required generated parsing still requires
   broader PostgreSQL-compatible grammar coverage, expression-level and
   full join-tree generated query-body ASTs, full expression AST nodes beyond
   list-level and simple-comparison/operator metadata, broader function
   coverage, broader boolean expression-tree coverage, quantified subquery and
   array predicate coverage, per-CTE generated body arrays, recursive CTE
   planning, direct generated read-plan lowering, and
   unsupported-shape diagnostics.
5. Advanced DML: `INSERT ... SELECT`, `UPDATE ... FROM`, `DELETE ... USING`,
   `TRUNCATE`, and `MERGE`.
6. Antfly extensions: graph traversal DSL, graph metric query surfaces,
   automatic embeddings, full-text ranking, algebraic indexes, enrichment
   clauses, lake/source syntax, and Lite-specific capability checks. Seed
   `CREATE GRAPH INDEX` and `CREATE GRAPH METRIC` statements now have generated
   graph-family corpus coverage, retained generated AST nodes, and generated
   AST-to-plan wrappers for typed graph index and graph metric index plans.
   Broader graph traversal, graph metric query, and graph DSL cutover still
   requires graph-specific query AST payloads and unsupported-shape diagnostics.

## Generator Performance

The generator and generated parser should be optimized for build speed,
incremental development, runtime latency, and memory locality. SQL parsing is
on request paths, CLI paths, tests, and dashboard REPL paths, so generator
choices should be measured rather than treated as purely build-time details.

Important performance requirements:

- **Small generated surface**: generate only the supported Antfly grammar, not
  the full PostgreSQL grammar. Large unreachable productions slow builds,
  increase binary size, and create unsupported syntax that later phases must
  reject.
- **Deterministic generation**: generated files must be stable across runs so
  reviews and cache keys do not churn.
- **Fast incremental builds**: grammar generation should depend only on grammar
  inputs and generator code. Ordinary SQL lowerer or executor edits should not
  regenerate parser artifacts.
- **Scanner/parser split**: keep lexical scanning separate from grammar
  reduction so tokenization can remain allocation-light and reusable for
  fingerprinting, diagnostics, read-only classification, and query formatting.
- **Compact token ids**: use dense token and nonterminal ids so parse tables are
  cache-friendly and cheap to serialize into generated Zig.
- **Indexed parse tables**: generated action/goto tables carry per-state row
  ranges and use binary search inside the active row instead of scanning the
  full table. Future compression can move from row ranges to row displacement,
  packed transition arrays, default reductions, or another measured scheme if
  table size or lookup cost becomes material.
- **Allocation-light AST construction**: allocate AST nodes in an arena owned
  by `ParsedSql`; avoid per-token heap allocation and avoid copying token text
  unless normalized text is required.
- **Span by offset**: store byte offsets and lengths into the original SQL
  buffer instead of materializing substrings for every identifier, literal, or
  diagnostic target.
- **Keyword metadata once**: classify keywords during scanning or tokenization
  and reuse that metadata for parsing, binding, read-only classification, and
  diagnostics.
- **Error recovery bounds**: cap recovery work after syntax errors. Interactive
  SQL REPL and dashboard use need useful diagnostics, but malformed input must
  not trigger quadratic parser behavior.
- **No runtime grammar loading**: generated parse tables should be compiled into
  the binary. Runtime should not read grammar files or dynamically build parser
  tables.
- **Shadow parser budget**: while the generated parser runs in tests or debug
  validation beside the current parser, keep it behind explicit test/debug
  paths so production requests parse once.

The first benchmark target should be a parser-only microbenchmark over the SQL
compatibility corpus. Track tokens per second, parse latency distribution,
allocated bytes per statement, generated table size, generated Zig compile
time, and binary size impact. Add larger end-to-end SQL benchmarks only after a
statement family actually switches to the generated parser.

## Parser Shape

The first generated output is a small AST-compatible facade, not a full rewrite
of binding and planning. The active target is:

```text
current SQL fixture
  -> generated parser
  -> raw statement-family facade with source spans
  -> existing binder/lowerer entry point
  -> same typed plan or same unsupported diagnostic
```

This avoids mixing syntax migration with execution changes. If a statement
family currently performs ad hoc token inspection, migrate that inspection into
an AST node before switching the family to the generated parser.

The generated parser facade currently produces source-span-bearing closed
variants for:

- session statement, including a generated AST payload for command, name, and
  value token ranges, plus generated AST-to-plan parity for generated-covered
  session catalog commands
- transaction statement, including a generated AST payload for command spans,
  plus generated AST-to-plan parity for generated-covered transaction boundary
  commands
- prepared statement, including a generated AST payload for command, name,
  argument, and nested-statement token ranges, plus generated AST-to-plan parity
  for typed `PREPARE`, `EXECUTE`, and `DEALLOCATE`
- DDL statement, including generated AST payloads for command spans, object
  names, catalog option fields, drop behavior, and generated AST-to-plan parity
  for database, schema, and extension create/drop catalog plans and seed
  `CREATE TABLE` / `CREATE INDEX` plans
- DML statement, including generated AST payloads for command spans, target
  tables, source/body ranges, predicates, conflict clauses, returning clauses,
  values lists, default-values inserts, truncate options, direct generated AST-to-plan
  lowerers for supported explicit-column `INSERT ... VALUES`,
  `INSERT ... VALUES ... ON CONFLICT`, `INSERT ... DEFAULT VALUES`, insert
  `RETURNING`, single-table point `UPDATE`/`DELETE` with returning projections,
  and `TRUNCATE`, and an initial AST-to-plan wrapper for the other
  generated-covered write statements
- read statement, including a generated AST payload for command spans and an
  initial AST-to-plan wrapper for generated-covered read statements
- graph statement, including a generated AST payload for command spans and
  graph-specific AST-to-plan wrappers for seed graph index and graph metric DDL
- unsupported statement, including a generated AST payload for seed `ANALYZE`
  and simple `EXPLAIN` forms with command spans, subject ranges where present,
  and stable unsupported reason metadata

Later statement-family cutovers should add closed variants for:

- extension/index statement
- broader unsupported PostgreSQL-compatible statements with diagnostic reasons

Those variants should become the normal dispatch boundary for binder and lowerer
code.

Generated parser diagnostics expose the parser state, lookahead symbol, token
index, source byte span, actual token text, and expected terminal names. That is
the parse-phase shape the dashboard REPL, CLI, HTTP SQL endpoint, and corpus
tests should use instead of string-only unsupported reasons.

## Testing And Evidence

Generated grammar work needs evidence at multiple levels:

- Corpus tests for accepted PostgreSQL-compatible syntax. The initial checked
  corpus covers session commands, transaction commands, prepared statements,
  simple database/schema/table/index/extension DDL, representative DML, and
  representative read queries. It also covers seed graph statements as a
  distinct generated family. Runtime parsing also enforces generated parser
  success for the session, transaction, and prepared statement corpus.
- Corpus tests for accepted Antfly-specific syntax.
- Corpus tests for intentionally unsupported PostgreSQL syntax with stable
  diagnostics. Seed `ANALYZE` and simple `EXPLAIN` forms now produce generated
  unsupported AST nodes with stable reason metadata while richer existing
  `EXPLAIN` syntax still falls back to the current parser until generated
  coverage catches up.
- AST shape tests for source spans, identifier normalization, literals,
  placeholders, casts, operators, and nested statements. The first AST shape
  tests cover generated session, transaction, prepared, DDL, DML, read, and
  graph statement payloads, including top-level generated read-list metadata.
- Plan parity tests showing generated ASTs lower to the same typed plans as the
  current parser for migrated statement families. Session catalog commands,
  transaction boundaries, prepared statements, and simple DDL database/schema/
  extension catalog plans plus seed `CREATE TABLE` / `CREATE INDEX` plans have
  generated AST-to-plan parity tests for their generated-covered forms; simple
  catalog DDL also has generated field-level checks for object names, option
  flags, version strings, drop behavior, and fail-closed unsupported clauses.
  Simple DML has generated field-level checks for update and truncate body
  ranges, direct generated AST-to-plan parity for truncate mutation-source
  plans, direct resolver-free generated AST-to-plan coverage for supported
  explicit-column insert-values row batches with column-list, partial, and
  named constraint conflict targets, conflict actions, and returning lists,
  default-values row batches with conflict actions and returning lists, direct
  generated AST-to-plan coverage for supported same-table and cross-table
  insert-select requests with conflict and returning lists, direct generated
  AST-to-plan coverage for single-table point update/delete batches with
  returning lists, direct generated AST-to-plan coverage for table-wide and
  single-table source update/delete mutation-source requests without joined
  sources, direct generated AST-to-plan coverage for explicit update-from and
  delete-using joined mutation-source requests, direct generated AST-to-plan
  coverage for non-CTE merge plans, and initial generated AST-to-plan parity
  through a generated-family validation wrapper over other representative write
  plans. Read plans have initial
  generated AST-to-plan parity through a generated-family validation wrapper
  over representative query, aggregate, join, lateral, and non-recursive CTE
  plans, AST-shape coverage for generated-ranged multi-CTE and recursive CTE
  prefixes, first-join component range coverage, and simple comparison plus
  positive/negated predicate expression-shape coverage for read predicates.
  Seed graph DDL has generated AST-to-plan parity for graph index and graph
  metric index plans.
- SQL/API parity tests showing SQL and native API requests reach the same
  service contracts.
- Fuzz or mutation tests for scanner/parser crash resistance and bounded error
  recovery. A deterministic malformed SQL corpus now exercises generated
  source-aware diagnostics for incomplete read, CTE, DDL, DML, and unsupported
  statement shapes; broader randomized scanner/parser fuzzing remains future
  evidence.
- Parser microbenchmarks for corpus throughput, allocation count, parse-table
  size, generated-code compile time, and binary size.

The migration is complete only when production SQL ingress no longer depends on
hand-written statement parsing for the migrated families and compatibility debt
is represented as explicit unsupported diagnostics rather than parser probes or
string scans.
