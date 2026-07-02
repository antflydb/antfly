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

- [ ] Join execution model expansion
  - [ ] Represent generated join trees as executable N-way or binary-tree row-plan nodes instead of validating only the current binary join contract.
  - [ ] Add row engine/API semantics for right and full outer joins, including null-extension behavior, projection binding, predicate evaluation, and pagination/order interactions.
  - [ ] Lower generated `RIGHT [OUTER] JOIN` and `FULL [OUTER] JOIN` from generated join-kind metadata only after execution semantics and parity fixtures exist.
  - [ ] Expand join binding/parity fixtures for `JOIN ... ON`, `JOIN ... USING (...)`, `CROSS JOIN`, `NATURAL JOIN`, lateral joins, aliases, CTE-body joins, and mixed multi-join trees.

- [ ] Native subquery semantics
  - [ ] Add typed plan/execution nodes for scalar subqueries in projections, predicates, assignments, defaults, and generated expressions, including cardinality errors.
  - [ ] Add semijoin/antijoin/existence plan nodes for `IN`, `NOT IN`, `EXISTS`, and `NOT EXISTS`, including null semantics and correlated references.
  - [ ] Add quantified comparison/pattern execution for `ANY`/`SOME`/`ALL`, including array-vs-subquery distinction and null/empty-set behavior.
  - [ ] Lower generated subquery expression payloads directly from retained child read ASTs, and keep malformed payloads fail-closed before legacy expression parsing.
  - [ ] Add SQL/API parity and malformed-payload fixtures for scalar, semijoin, quantified, correlated, nested, and CTE-contained subqueries.

- [ ] Function and expression semantic completion
  - [ ] Replace token-layout validation with direct lowering for generated expression nodes that already carry child payloads: `CASE`, casts, arrays, `EXTRACT`, temporal literals, generic functions, and aggregate/window function arguments.
  - [ ] Complete executable semantics for arithmetic, comparison, boolean, null-safe, pattern, regex, JSON/path, containment, overlap, and array operators from generated operator metadata.
  - [ ] Complete aggregate/function planning for `FILTER`, `WITHIN GROUP`, ordered-set aggregates, aggregate-local ordering, variadic/generic functions, casts, and type coercion.
  - [ ] Add stable unsupported diagnostics for generated expression shapes that parse but do not yet have executable semantics.
  - [ ] Add parity/fail-closed fixtures for each newly lowered expression family, including corrupted child-expression/list payloads.

- [ ] Graph DSL and Antfly extension cutover
  - [ ] Add generated AST and typed planning for non-table-function graph syntax such as `MATCH ... RETURN`, including graph source binding, projection binding, filters, ordering, and limits.
  - [ ] Keep unsupported graph syntax as generated unsupported statements with stable reason and exact subject span, including unsupported `MATCH` variants, path predicates, and return forms.
  - [ ] Keep `antfly.*` graph table-function sources generated-owned through binder, planner, HTTP, Lite, document SQL, and row execution entrypoints; remove raw SQL reparsing from these paths.
  - [ ] Add parity and fail-closed fixtures for graph subjects, graph metrics, graph metric rerank, joined graph sources, CTE graph sources, and corrupted graph semantic payloads.

- [ ] Rich DDL metadata and semantic coverage
  - [ ] Route every valid-but-unplanned generated-owned DDL shape to a typed unsupported statement with stable reason, exact subject span, and retained-AST validation before any catalog/DDL fallback can run.
  - [ ] For every new DDL metadata field, add generated parser span coverage, grammar tail fixtures, parsed-entrypoint corruption tests, logical-DDL corruption tests where applicable, and SQL/API parity or unsupported-reason fixture coverage.

- [ ] Production ingress parser cutover
  - [ ] Keep `fixtures/sql_parser_migration_table.json` current while inventorying public SQL entrypoints (`tokenized`, SQL adapter, pgwire, HTTP, Lite, binder, document SQL, durable/executor paths) and marking which statement families still call hand-written parser probes.
  - [ ] Replace remaining generated-covered token scans/string probes with generated AST dispatch or generated unsupported diagnostics; leave legacy admission only for shapes outside generated grammar coverage.
  - [ ] Add public-boundary corruption tests for retained AST payload removal/staleness across read, DML, DDL, graph, unsupported, prepared, cursor, session, and transaction families.
  - [ ] Add regression fixtures proving generated-owned malformed statements cannot recover by re-entering legacy DDL/read/write parsing.

- [ ] Broader generated unsupported PostgreSQL diagnostics
  - [ ] Audit standalone trigger variants not already covered by the trigger-catalog and update-policy planning paths; separate supported `CREATE/DROP TRIGGER` forms from valid PostgreSQL trigger variants that still require generated unsupported diagnostics.
  - [ ] For each remaining unsupported trigger variant, add a generated unsupported kind/reason pair with exact command and subject spans before it can reach parser/DDL fallback.
  - [ ] Add parsed-entrypoint corruption tests for each new unsupported trigger family, covering kind, reason, command span, subject span, explain options, and any family-specific retained AST payload.
  - [ ] Add SQL/API source-corpus rows in `fixtures/sql_api_parity_source_corpus.json`, promote `fixtures/sql_api_parity_corpus.json`, and add/extend the matching required coverage observer in `corpus.zig`.

- [ ] Evidence and performance hardening
  - [ ] Expand accepted PostgreSQL-compatible corpus coverage as each generated family is cut over, with required coverage manifests updated in the same change.
  - [ ] Expand Antfly-specific corpus coverage for query functions, graph, Lite, table APIs, and routed execution paths.
  - [ ] For document SQL-specific read/write behavior, add the case to `fixtures/document_sql_corpus.json`, update `fixtures/sql_document_dependency_guard.json`, and keep the document plan/runtime corpus tests passing.
  - [ ] For document SQL bounded-scan compatibility, update `fixtures/document_sql_bounded_scan_inventory.json` only when changing the `mapped-view-residual-bounded-scan` contract and keep its SQL/API parity row in the same patch.
  - [ ] For document SQL read expansion, update `fixtures/document_sql_read_expansion_gate.json` only when changing the `additional-array-unnest-patterns` gate and keep matching SQL/API parity rows in the same patch.
  - [ ] Add deterministic malformed SQL diagnostics for the next touched family only: incomplete DDL, DML, read, CTE, unsupported, graph, or expression payload shapes.
  - [ ] Keep short mutation/fuzz coverage in default tests; when parser fuzzing changes, update the `sql-parser-fuzz` seed/replay workflow in the same patch.
  - [ ] Before production cutover, record parser throughput, allocation count, parse-table size, compile-time impact, and binary size from parser benchmarks.
  - [ ] Before marking any migrated family complete, run the relevant SQL/API parity check, unsupported-reason fixture check, and fail-closed retained-AST corruption tests.
