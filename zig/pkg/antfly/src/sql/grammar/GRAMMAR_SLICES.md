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
  - [ ] Extend scalar subquery expression nodes beyond the promoted generated SELECT projection and predicate paths into generated expressions, assignments, defaults, and remaining expression entrypoints. Generated SELECT scalar-subquery projections already lower from retained child read ASTs into typed scalar-subquery projections and execute with single-column validation, zero-row `NULL`, multi-row cardinality errors, malformed retained-AST fail-closed coverage, runtime coverage, and SQL/API parity.
  - [ ] Add correlated reference binding for scalar, `EXISTS`/`NOT EXISTS`, `IN`/`NOT IN`, and quantified subqueries, including outer-scope name resolution, per-row child execution, null semantics, malformed retained-AST fail-closed tests, and public SQL/API parity coverage. Non-correlated predicate subqueries already lower to typed plans and execute with SQL null/empty-set behavior.
  - [ ] Broaden quantified public coverage for remaining comparison and pattern operator variants beyond the promoted non-correlated `ANY`/`SOME`/`ALL` corpus rows, including array-vs-subquery distinction and malformed retained-AST fail-closed tests at every generated expression entrypoint. Public read parity and seeded row execution now cover `= ANY`, `= SOME`, `<> ALL`, `> ANY`, `>= SOME`, `< ALL`, `<= ALL`, `LIKE ANY`, `ILIKE ANY`, `LIKE ALL`, `NOT LIKE ALL`, and `NOT ILIKE ALL`.

- [ ] Function and expression semantic completion
  - [ ] Finish direct generated-payload lowering for expression families that still rely on token-layout fallback: `CASE` branch values, casts/type coercion, arrays, `EXTRACT`, temporal literals, generic functions, and aggregate/window argument, `FILTER`, `WITHIN GROUP`, and `OVER` payloads.
  - [ ] Complete executable semantics for arithmetic, comparison, boolean, null-safe, pattern, regex, JSON/path, containment, overlap, and array operators from generated operator metadata.
  - [ ] Complete aggregate/function planning for `FILTER`, `WITHIN GROUP`, ordered-set aggregates, aggregate-local ordering, variadic/generic functions, casts, and type coercion.
  - [ ] For any newly admitted expression operator, keyword, or punctuation, update `TokenKind`/`TokenKeyword` in `token.zig`, lexer recognition in `lexer.zig`, generated-token mapping, and focused lexer/parser fixtures before relying on generated expression lowering.
  - [ ] Add stable unsupported diagnostics for generated expression shapes that parse but do not yet have executable semantics.
  - [ ] Add missing SQL/API parity rows or typed unsupported rows, plus any missing fail-closed corrupted child-expression/list payload fixtures, for each newly lowered expression family.

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
