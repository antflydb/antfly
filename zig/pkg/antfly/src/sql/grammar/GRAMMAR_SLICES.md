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
  - [ ] Extend scalar subquery expression nodes beyond the promoted generated SELECT projection and predicate paths into generated expressions, defaults, remaining assignment families, and remaining expression entrypoints. Generated SELECT scalar-subquery projections already lower from retained child read ASTs into typed scalar-subquery projections, including mixed field plus scalar-subquery projection lists, and execute with single-column validation, zero-row `NULL`, multi-row cardinality errors, malformed retained-AST fail-closed coverage, runtime coverage, and SQL/API parity. `INSERT ... SELECT` source assignments now retain scalar-subquery source projections, bind the scalar alias as a typed source field for assignment/type validation, preserve runtime cardinality behavior, and have focused DML lowering coverage.
  - [ ] Broaden correlated reference binding beyond source-bound simple equality and generated `AND` child predicates, and broaden malformed retained-AST fail-closed tests for each correlated subquery entrypoint. Source-bound simple equality correlations now lower from retained generated child read ASTs into typed correlation metadata and execute per outer row with null/missing outer values producing empty child results for `EXISTS`, `NOT EXISTS`, scalar comparison, `IN`/`NOT IN`, and quantified predicates, with focused lowering/runtime plus SQL parity coverage. Generated `AND` child predicates now split equality correlations from child-local filters before lowering the retained child read AST across `EXISTS`, `NOT EXISTS`, scalar comparison, `IN`/`NOT IN`, and quantified predicates, including grouped correlation expressions, with focused lowering, runtime, fail-closed, and SQL/API parity coverage.
  - [ ] Broaden quantified subquery coverage for malformed retained-AST fail-closed tests at every generated expression entrypoint. Public read parity, required coverage buckets, seeded row execution, and focused generated lowerer coverage now cover `= ANY`, `= SOME`, `<> ANY`, `<> ALL`, `> ANY`, `> ALL`, `>= SOME`, `>= ALL`, `< ANY`, `< ALL`, `<= ANY`, `<= ALL`, `LIKE ANY`, `LIKE SOME`, `ILIKE ANY`, `ILIKE SOME`, `LIKE ALL`, `ILIKE ALL`, `NOT LIKE ANY`, `NOT LIKE ALL`, `NOT ILIKE ANY`, and `NOT ILIKE ALL`. Array `ANY`/`ALL` predicate paths now assert they remain non-subquery predicates and fail closed when retained right-expression metadata is corrupted to `.subquery`.

- [ ] Function and expression semantic completion
  - [ ] Finish direct generated-payload lowering for expression families that still rely on token-layout fallback: `CASE` branch values, casts/type coercion, arrays, `EXTRACT`, temporal literals, generic functions, and aggregate/window argument, `FILTER`, `WITHIN GROUP`, and `OVER` payloads.
  - [ ] Complete executable semantics for arithmetic, comparison, boolean, null-safe, pattern, regex, JSON/path, containment, overlap, and array operators from generated operator metadata.
  - [ ] Complete aggregate/function planning for `FILTER`, `WITHIN GROUP`, ordered-set aggregates, aggregate-local ordering, variadic/generic functions, casts, and type coercion.
  - [ ] For any newly admitted expression operator, keyword, or punctuation, update `TokenKind`/`TokenKeyword` in `token.zig`, lexer recognition in `lexer.zig`, generated-token mapping, and focused lexer/parser fixtures before relying on generated expression lowering.
  - [ ] Add stable unsupported diagnostics for generated expression shapes that parse but do not yet have executable semantics.
  - [ ] Add missing SQL/API parity rows or typed unsupported rows, plus any missing fail-closed corrupted child-expression/list payload fixtures, for each newly lowered expression family.

- [ ] Evidence and performance hardening
  - [ ] For each generated-owned SQL shape admitted at the public SQL/API boundary, add or graduate rows in `fixtures/sql_api_parity_source_corpus.json`, promote `fixtures/sql_api_parity_corpus.json`, and update required coverage or unsupported-reason manifests in the same patch.
  - [ ] When a required coverage bucket relies on native-equivalence metadata derived during fixture promotion, either pin explicit native evidence in the source row or make the source-corpus observer assert the structural/public-plan contract without requiring explicit source evidence.
  - [ ] For Antfly-specific query functions, graph, Lite, table APIs, and routed execution paths, add SQL/API parity rows when the public adapter contract changes and focused Zig runtime tests when seeded execution behavior is the proof.
  - [ ] For document SQL-specific read/write behavior, use `fixtures/document_sql_corpus.json` as the canonical behavior fixture, update `fixtures/sql_document_dependency_guard.json`, keep the document plan/runtime corpus tests passing, and still include lexer/generated-token bridge fixtures in the same patch when the document SQL shape admits new production syntax.
  - [ ] Add SQL/API corpus rows for document SQL only when public adapter routing, coverage buckets, unsupported reasons, or plan summaries change.
  - [ ] For document SQL bounded-scan compatibility, update `fixtures/document_sql_bounded_scan_inventory.json` only when changing admitted scan contracts and keep matching SQL/API parity rows in the same patch.
  - [ ] For document SQL read expansion, update `fixtures/document_sql_read_expansion_gate.json` only when changing the `additional-array-unnest-patterns` gate and keep matching SQL/API parity rows in the same patch.
  - [ ] Add deterministic malformed SQL diagnostics for each touched family, with parser diagnostics and public SQL adapter fail-closed coverage in the same patch.
  - [ ] Keep short mutation/fuzz coverage in default tests; when parser fuzzing changes, update the `sql-parser-fuzz` seed/replay workflow in the same patch.
  - [ ] Before production cutover, record parser throughput, allocation count, parse-table size, compile-time impact, and binary size from parser benchmarks.
  - [ ] Before marking any migrated family complete, run the relevant SQL/API parity check, unsupported-reason fixture check, and fail-closed retained-AST corruption tests.
