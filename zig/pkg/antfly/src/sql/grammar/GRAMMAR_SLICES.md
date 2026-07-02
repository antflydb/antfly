# Antfly SQL Grammar Remaining Slices

This is the tracking document for the SQL grammar migration. It lists only
remaining migration work and should stay checklist-shaped. Keep design
rationale, architecture decisions, ownership boundaries, and implementation
invariants in `GRAMMAR.md`. Completed history that is deducible from code,
tests, fixtures, or git history should stay out of both docs.

Completed read-plan and DML cutover slices are intentionally omitted from the
active checklist.

Feature work is listed first, followed by parser cutover, diagnostics, and
evidence/performance hardening. Check a slice only when the implementation,
diagnostics, and verification evidence are all in place.

When a remaining slice admits a new grammar terminal or promotes generated
syntax that production tokenization cannot already represent, keep the
production lexer path in the same patch: `TokenKind`/`TokenKeyword` in
`token.zig`, lexer recognition in `lexer.zig`, generated-token bridge mapping,
and focused lexer/parser fixtures. Prefer contextual keyword bridge handling for
generated-only words that must still be valid identifiers outside the new
grammar context. Lowering-only changes for terminals that production
tokenization already recognizes do not need lexer churn.

- [ ] Native subquery semantics
  - [ ] Graduate the `unsupported_read_scalar_subquery_predicate` and `unsupported_read_scalar_subquery_projection` fixtures to typed scalar subquery plan/execution nodes in projections, predicates, assignments, defaults, and generated expressions, including cardinality errors.
  - [ ] Graduate the `unsupported_read_subquery_predicate`, `unsupported_read_not_exists_subquery_predicate`, `unsupported_read_in_subquery_predicate`, and `unsupported_read_not_in_subquery_predicate` fixtures to semijoin/antijoin/existence plan nodes, including null semantics and correlated references.
  - [ ] Graduate the `unsupported_read_quantified_subquery_predicate`/`subquery_quantified_plan` fixture to quantified comparison/pattern execution for `ANY`/`SOME`/`ALL`, including array-vs-subquery distinction and null/empty-set behavior.
  - [ ] Lower generated subquery expression payloads directly from retained child read ASTs, and keep malformed payloads fail-closed before legacy expression parsing.

- [ ] Function and expression semantic completion
  - [ ] Finish direct generated-payload lowering for expression families that still rely on token-layout fallback: `CASE` branch values, casts/type coercion, arrays, `EXTRACT`, temporal literals, generic functions, and aggregate/window argument, `FILTER`, `WITHIN GROUP`, and `OVER` payloads.
  - [ ] Complete executable semantics for arithmetic, comparison, boolean, null-safe, pattern, regex, JSON/path, containment, overlap, and array operators from generated operator metadata.
  - [ ] Complete aggregate/function planning for `FILTER`, `WITHIN GROUP`, ordered-set aggregates, aggregate-local ordering, variadic/generic functions, casts, and type coercion.
  - [ ] For any newly admitted expression operator, keyword, or punctuation, update `TokenKind`/`TokenKeyword` in `token.zig`, lexer recognition in `lexer.zig`, generated-token mapping, and focused lexer/parser fixtures before relying on generated expression lowering.
  - [ ] Add stable unsupported diagnostics for generated expression shapes that parse but do not yet have executable semantics.
  - [ ] Add missing SQL/API parity rows or typed unsupported rows, plus any missing fail-closed corrupted child-expression/list payload fixtures, for each newly lowered expression family.

- [ ] Rich DDL metadata and semantic coverage
  - [ ] Route every valid-but-unplanned generated-owned DDL shape to a typed unsupported statement with stable reason, exact subject span, and retained-AST validation before any catalog/DDL fallback can run.
  - [ ] For each newly admitted DDL keyword, clause delimiter, or option operator, update `TokenKind`/`TokenKeyword` in `token.zig`, lexer recognition in `lexer.zig`, generated-token mapping, and grammar-tail fixtures in the same patch as the generated AST metadata.
  - [ ] For every new DDL metadata field, add generated parser span coverage, grammar tail fixtures, parsed-entrypoint corruption tests, logical-DDL corruption tests where applicable, SQL/API parity rows or unsupported-reason fixture coverage, and required manifest entries.

- [ ] Evidence and performance hardening
  - [ ] For each generated-owned SQL shape admitted at the public SQL/API boundary, add or graduate rows in `fixtures/sql_api_parity_source_corpus.json`, promote `fixtures/sql_api_parity_corpus.json`, and update required coverage or unsupported-reason manifests in the same patch.
  - [ ] For Antfly-specific query functions, graph, Lite, table APIs, and routed execution paths, add SQL/API parity rows when the public adapter contract changes and focused Zig runtime tests when seeded execution behavior is the proof.
  - [ ] For document SQL-specific read/write behavior, use `fixtures/document_sql_corpus.json` as the canonical behavior fixture, update `fixtures/sql_document_dependency_guard.json`, keep the document plan/runtime corpus tests passing, and still include lexer/generated-token bridge fixtures in the same patch when the document SQL shape admits new production syntax.
  - [ ] Add SQL/API corpus rows for document SQL only when public adapter routing, coverage buckets, unsupported reasons, or plan summaries change.
  - [ ] For document SQL bounded-scan compatibility, update `fixtures/document_sql_bounded_scan_inventory.json` only when changing the `mapped-view-residual-bounded-scan` contract and keep its SQL/API parity row in the same patch.
  - [ ] For document SQL read expansion, update `fixtures/document_sql_read_expansion_gate.json` only when changing the `additional-array-unnest-patterns` gate and keep matching SQL/API parity rows in the same patch.
  - [ ] Add deterministic malformed SQL diagnostics for each touched family, with parser diagnostics and public SQL adapter fail-closed coverage in the same patch.
  - [ ] Keep short mutation/fuzz coverage in default tests; when parser fuzzing changes, update the `sql-parser-fuzz` seed/replay workflow in the same patch.
  - [ ] Before production cutover, record parser throughput, allocation count, parse-table size, compile-time impact, and binary size from parser benchmarks.
  - [ ] Before marking any migrated family complete, run the relevant SQL/API parity check, unsupported-reason fixture check, and fail-closed retained-AST corruption tests.
