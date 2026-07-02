# Antfly SQL Grammar Design

This is the design document for Antfly's SQL grammar migration. It records the
architecture, ownership boundaries, invariants, and implementation rules needed
to keep the generated parser aligned with the rest of the SQL stack.

Track remaining work in `GRAMMAR_SLICES.md`. Do not use this file as a progress
log; completed history that can be deduced from code, tests, fixtures, or git
history should stay out of this document.

## Target Architecture

Antfly SQL should be PostgreSQL-compatible at the user surface without using
PostgreSQL's parser as an engine boundary. PostgreSQL grammar behavior and
CockroachDB's owned-parser model are references for compatibility; Antfly owns
the grammar, scanner, AST, diagnostics, lowering, and execution semantics.

The production path is:

```text
SQL bytes
  -> Antfly SQL scanner
  -> generated Antfly SQL parser
  -> catalog-free Antfly SQL AST
  -> generated statement-kind dispatch
  -> binder
  -> typed logical plan
  -> shared Antfly service
```

The generated parser is a syntax boundary, not a second control plane. It
recognizes syntax and builds raw AST nodes with source spans. Catalog lookup,
role checks, storage visibility, derived-index lifecycle, graph metric state,
Lite behavior, and execution stay in binder, planner, and service layers.

SQL catalog identity, session state, relational row contracts, CTE contracts,
expression-evaluation contracts, query-function lowering contracts, and native
requirement fixtures are owned under `pkg/antfly/src/sql` or the neutral query
package. API callers consume SQL-owned types through re-exports instead of
owning parser or row-plan contracts.

## Grammar Ownership

The reusable generator machinery lives under `zig/lib/yacc`, following the
library-plus-codegen pattern used by `zig/lib/openapi`. Antfly SQL owns:

- the SQL grammar input under this directory
- migration/design documentation
- checked-in generated parser metadata and facade code
- SQL corpus and native-requirement fixtures under `pkg/antfly/src/sql`

Use `zig build regen-sql-grammar` to refresh checked-in grammar metadata.
Use `zig build sql-grammar-generated-check` to verify generated output is
current.

Checked-in generated metadata must retain provenance for the referenced
PostgreSQL and CockroachDB grammar inputs without vendoring those grammars.
Source compatibility features accepted by the generator, such as Bison
directives, semantic action blocks, `%expect`, `%prec`, literal terminals, and
typed declaration tags, are parser-generator input compatibility only. Antfly
does not import PostgreSQL semantic actions or parse-node ownership.

## Compatibility Policy

PostgreSQL compatibility is a behavioral contract, not a source dependency on
PostgreSQL parser internals. Accept PostgreSQL syntax only when it maps to one
of these outcomes:

- an Antfly-native typed plan
- an explicit generated unsupported diagnostic with a stable reason
- a syntax diagnostic from the generated parser

Rules for grammar and AST work:

- Preserve source byte spans for every AST node that can produce diagnostics or
  route execution.
- Keep parser output catalog-free.
- Prefer closed statement-family variants over lowerer probe order.
- Do not store raw SQL text as durable metadata, index definitions, graph
  metric configs, role settings, extension state, backup scopes, or job
  payloads.
- Treat Antfly-specific graph, full-text, vector, enrichment, Lite, and
  algebraic-index syntax as first-class grammar branches, not post-parse string
  scans.
- Route valid-but-unimplemented generated-owned syntax to generated unsupported
  statements instead of letting it fall through to generic DDL/read/write
  parsing.

## Parser Contract

Generated parser APIs should expose stable, compact interpretation of accepted
syntax without exposing parser tables as runtime control data. Public helpers
may expose token names, rule names, production metadata, RHS symbols, nullable
symbol checks, reduction traces for tests/debugging, and accepted statement
families. Raw parser states, action/goto tables, and internal symbol tables
stay private.

Generated AST payloads must carry enough structure for later phases to validate
that they are still consistent with the token stream. If retained generated
metadata is missing, stale, or internally contradictory, generated-owned
statements must fail closed before legacy token scanners can recover.

Parser diagnostics should be bounded and source-aware. They should report token
index, byte span, actual token text, and expected terminal names through the SQL
facade without requiring callers to inspect parse-table internals.

## Statement Dispatch

Statement publication should be derived from accepted generated AST families
for generated-covered syntax. Hand-written token-head classification may remain
only for syntax outside generated grammar coverage during migration.

Generated-owned heads should follow this rule:

```text
accepted generated AST
  -> publish closed parsed family or generated unsupported family
  -> validate retained AST payload before lowerer dispatch
  -> lower to typed plan or fail closed
```

Generated-covered malformed statements must not recover by re-entering legacy
DDL, read, write, session, transaction, prepared-statement, cursor, graph, or
unsupported parsing.

Closed parsed families should remain the binder/lowerer boundary for:

- session and transaction controls
- prepared statements and prepared transactions
- cursor statements
- DDL and extension/index DDL
- DML writes
- reads, including CTE and set-operation reads
- graph DDL and graph/table-function reads
- generated unsupported PostgreSQL utility/admin/catalog shapes

## Lowering Rules

Lowering should consume generated AST metadata where generated ownership exists
and should reuse existing typed lowerers only after validating generated clause
layout and source spans.

Implementation requirements:

- Rebase child parsers on retained child ranges instead of reparsing arbitrary
  SQL text.
- Validate command spans, subject spans, list item ranges, delimiter layout,
  clause order, and child payload ownership before planning.
- Keep point-vs-source, schema, uniqueness, type coercion, role, catalog, and
  execution decisions outside the parser.
- Keep unsupported semantics explicit. A syntax shape that parses but lacks
  executable semantics should produce a stable unsupported diagnostic, not a
  generic syntax error or legacy-parser fallback.
- Source-bound native graph syntax, such as
  `MATCH ... WITH GRAPH ... ON ... START ... RETURN ...`, lowers through the
  same typed graph table-function semantics as `antfly.graph_match`. Dotted
  node-alias fields in `RETURN` and `ORDER BY`, currently `alias.key`,
  `alias.depth`, and `alias.distance`, bind to per-match graph table-function
  columns such as `target_key` and `target_depth`. Bare alias-object projection
  such as `RETURN target` remains unsupported, and bare `MATCH ... RETURN ...`
  without explicit table, index, and start binding remains generated-owned
  unsupported until a native binding contract exists.
- Preserve Antfly API parity: generated SQL plans and native API requests
  should reach the same service contracts.

## Performance Requirements

SQL parsing is on request paths, CLI paths, tests, and dashboard REPL paths.
Generator choices should be measured.

- Generate only the supported Antfly grammar surface.
- Keep generated files deterministic across runs.
- Keep grammar generation dependent only on grammar inputs and generator code.
- Keep scanning separate from grammar reduction.
- Use dense token and nonterminal ids.
- Keep parse tables compact and lookup-friendly.
- Allocate AST nodes in an arena owned by `ParsedSql`.
- Store spans as byte offsets into the original SQL buffer.
- Classify keywords once during scanning/tokenization and reuse that metadata.
- Bound syntax error recovery work.
- Compile parse tables into the binary; do not load grammar files at runtime.
- Avoid production double-parsing except for explicit migration/debug paths.

Use `zig build sql-parser-bench -- <iterations>` for parser-only corpus
benchmarking. Track throughput, latency percentiles, allocation totals, peak
live parser bytes, generated table counts, generated parse-table size,
compile-time impact, and binary-size impact.

## Testing Requirements

Generated grammar work needs evidence at these levels before a family is
considered migrated:

- corpus coverage for accepted PostgreSQL-compatible syntax
- corpus coverage for accepted Antfly-specific syntax
- corpus coverage for intentionally unsupported PostgreSQL shapes with stable
  diagnostics
- AST shape tests for spans, identifiers, literals, placeholders, casts,
  operators, nested statements, and family-specific payloads
- fail-closed tests for missing, stale, or contradictory retained AST payloads
- SQL/API parity tests for supported behavior
- plan parity tests where generated ASTs should lower to the same typed plans
  as existing parser paths
- deterministic malformed SQL diagnostics for incomplete covered syntax
- bounded scanner/parser mutation or fuzz coverage
- parser performance benchmarks before production cutover

The migration is complete only when production SQL ingress no longer depends on
hand-written statement parsing for migrated families, and compatibility debt is
represented as explicit unsupported diagnostics rather than parser probes or
string scans.
