# Antfly SQL Grammar Remaining Slices

This tracker lists only remaining migration work. Completed read-plan and DML
cutover slices are intentionally omitted from the active checklist; keep durable
evidence for completed work in grammar notes, tests, and corpus manifests.

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
  - [ ] Add native generated AST fields for `GRANT`/`REVOKE`: privilege lists, grant targets, grantees, grant options, revoke grant-option mode, and cascade/restrict behavior.
  - [ ] Extend generated `COMMENT ON` beyond the current table/column/index/constraint metadata: routine signatures, type/domain/extension/schema/database objects, security labels, and unsupported-object diagnostics with exact subject spans.
  - [ ] Extend routine metadata beyond signature/language/return type: body/`AS` clauses, volatility, security, strictness, cost/rows, support, transform, parallel, leakproof, and `SET` options.
  - [ ] Extend row-policy metadata beyond table and role targets: `FOR` command, `USING` predicate, and `WITH CHECK` predicate retained expression ranges/payloads.
  - [ ] Extend publication/subscription metadata beyond current table/publication/enabled fields: publication publish/options, table filters/column lists, subscription options, refresh/copy/slot state, and owner/connection mutations.
  - [ ] Extend generated `ALTER TABLE` operation items beyond currently typed add/drop/rename/alter/constraint/row-security paths: partitions, inheritance, ownership/schema/storage/persistence/tablespace, trigger state, replica identity, and statistics/storage parameters.
  - [ ] Route every valid-but-unplanned generated-owned DDL shape to a typed unsupported statement with stable reason, exact subject span, and retained-AST validation before any catalog/DDL fallback can run.
  - [ ] For every new DDL metadata field, add generated AST shape coverage, parsed-entrypoint corruption tests, logical-DDL corruption tests where applicable, and SQL/API parity or unsupported-reason fixture coverage.

- [ ] Production ingress parser cutover
  - [ ] Inventory public SQL entrypoints (`tokenized`, SQL adapter, pgwire, HTTP, Lite, binder, document SQL, durable/executor paths) and mark which statement families still call hand-written parser probes.
  - [ ] Replace remaining generated-covered token scans/string probes with generated AST dispatch or generated unsupported diagnostics; leave legacy admission only for shapes outside generated grammar coverage.
  - [ ] Add public-boundary corruption tests for retained AST payload removal/staleness across read, DML, DDL, graph, unsupported, prepared, cursor, session, and transaction families.
  - [ ] Add regression fixtures proving generated-owned malformed statements cannot recover by re-entering legacy DDL/read/write parsing.

- [ ] Broader generated unsupported PostgreSQL diagnostics
  - [ ] Audit generated unsupported enum/reason coverage against PostgreSQL utility/admin/catalog heads used by dumps and migrations; add missing heads before they can fall through to parser/DDL fallback.
  - [ ] For each unsupported family, validate kind, reason, command span, subject span, explain options, and family-specific payloads at parsed-statement publication.
  - [ ] Add corpus rows for PostgreSQL dump/admin compatibility shapes: ownership, privileges, comments, security labels, extensions, foreign-data objects, text search, operator classes/families, maintenance, and bulk I/O.
  - [ ] Add corruption tests that mutate unsupported kind/reason/subject/option payloads and verify parsed entrypoints fail closed.

- [ ] Evidence and performance hardening
  - [ ] Expand accepted PostgreSQL-compatible corpus coverage as each generated family is cut over, with required coverage manifests updated in the same change.
  - [ ] Expand Antfly-specific corpus coverage for query functions, graph, Lite, table APIs, and routed execution paths; for document SQL, update the dedicated `document_sql_corpus.json`, `sql_document_dependency_guard.json`, `document_sql_bounded_scan_inventory.json`, and `document_sql_read_expansion_gate.json` fixtures alongside SQL/API parity rows.
  - [ ] Expand deterministic malformed SQL diagnostics for incomplete DDL, DML, read, CTE, unsupported, graph, and expression payload shapes.
  - [ ] Keep short mutation/fuzz coverage in default tests; keep longer generated parser fuzzing behind `sql-parser-fuzz` with documented seed/replay workflow.
  - [ ] Track parser throughput, allocation count, parse-table size, compile-time impact, and binary size through parser benchmarks before production cutover.
  - [ ] Require SQL/API parity evidence, unsupported-reason fixture updates, and fail-closed retained-AST corruption tests before marking any migrated family complete.
