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
simple DDL has generated-parser corpus coverage but still falls back to the
existing parser when the seed grammar does not yet cover the shape. The
generated facade now returns closed statement-family nodes for the covered
families; full production AST construction remains the next migration boundary
for larger DDL, query, and DML families.

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
   when it matches the seed grammar; unsupported DDL remains on the existing
   parser until each shape has raw AST parity.
3. Simple DML: `INSERT ... VALUES`, primary-key `UPDATE`, primary-key
   `DELETE`, `RETURNING`, and `ON CONFLICT`.
4. Read queries: projections, predicates, joins, CTEs, aggregates, windows,
   set operations, lateral, ordering, limits, and document-table sources.
5. Advanced DML: `INSERT ... SELECT`, `UPDATE ... FROM`, `DELETE ... USING`,
   `TRUNCATE`, and `MERGE`.
6. Antfly extensions: graph traversal DSL, graph metric query surfaces,
   automatic embeddings, full-text ranking, algebraic indexes, enrichment
   clauses, lake/source syntax, and Lite-specific capability checks.

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

- session statement
- transaction statement
- prepared statement
- DDL statement

Later statement-family cutovers should add closed variants for:

- read query
- DML statement
- graph statement
- extension/index statement
- unsupported PostgreSQL-compatible statement with diagnostic reason

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
  simple database/schema/table/index/extension DDL. Runtime parsing also
  enforces generated parser success for the session, transaction, and prepared
  statement corpus.
- Corpus tests for accepted Antfly-specific syntax.
- Corpus tests for intentionally unsupported PostgreSQL syntax with stable
  diagnostics.
- AST shape tests for source spans, identifier normalization, literals,
  placeholders, casts, operators, and nested statements.
- Plan parity tests showing generated ASTs lower to the same typed plans as the
  current parser for migrated statement families.
- SQL/API parity tests showing SQL and native API requests reach the same
  service contracts.
- Fuzz or mutation tests for scanner/parser crash resistance and bounded error
  recovery.
- Parser microbenchmarks for corpus throughput, allocation count, parse-table
  size, generated-code compile time, and binary size.

The migration is complete only when production SQL ingress no longer depends on
hand-written statement parsing for the migrated families and compatibility debt
is represented as explicit unsupported diagnostics rather than parser probes or
string scans.
