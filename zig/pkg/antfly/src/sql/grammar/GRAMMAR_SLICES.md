# Antfly SQL Grammar Remaining Slices

Ordered largest first. Check a slice only when the implementation, diagnostics,
and verification evidence are all in place.

- [x] Full generated read-plan lowering cutover
  - [x] Add SQL/API parity coverage for generated-backed simple/select reads, including projection expressions, predicates, grouping, ordering, pagination, and row locks.
  - [x] Add SQL/API parity coverage for generated-backed direct set-operation reads, including result-tail ordering, pagination, and chained set-operation shapes.
  - [x] Add SQL/API parity coverage for generated-backed aggregate reads, including aggregate arguments, aggregate-local ordering, `FILTER`, `WITHIN GROUP`, grouping, `HAVING`, ordering, and pagination.
  - [x] Add SQL/API parity coverage for generated-backed window reads, including inline `OVER`, named windows, partition/order lists, frame bounds, predicates, ordering, and pagination.
  - [x] Add SQL/API parity coverage for generated-backed non-recursive and recursive CTE reads, including CTE aliases, body projections, anchor/member/final reads, and outer result tails.
  - [x] Add SQL/API parity coverage for generated-backed join and lateral reads, including source aliases, join predicates, lateral subquery payloads, projections, ordering, and pagination.
  - [x] Add fail-closed retained-AST corruption coverage for simple/select and direct set-operation reads.
  - [x] Add fail-closed retained-AST corruption coverage for aggregate reads, including argument lists, aggregate-local ordering, `FILTER`, and `WITHIN GROUP`.
  - [x] Add fail-closed retained-AST corruption coverage for window reads, including inline `OVER`, named windows, partition/order lists, and frame payload semantics.
  - [x] Add fail-closed retained-AST corruption coverage for non-recursive CTE bodies, recursive CTE anchor/member/final reads, and generated CTE aliases.
  - [x] Add fail-closed retained-AST corruption coverage for join and lateral reads, including join-source, predicate, projection, pagination, and result-tail metadata.
  - [x] Audit generated-backed read lowerers for remaining token-first dispatch that should be generated AST dispatch or an explicit unsupported-shape diagnostic.

- [x] Full generated DML semantic lowering cutover
  - [x] Lower assignment expressions from generated expression ASTs for top-level `UPDATE`, conflict actions, and `MERGE` update arms.
  - [x] Lower mutation predicates from generated expression ASTs for `UPDATE`, `DELETE`, conflict predicates, and `MERGE` arm predicates.
  - [x] Lower `RETURNING` projections from generated list/expression ASTs without reparsing returning tails.
  - [x] Lower conflict targets/actions from generated target, predicate, and assignment payloads end to end.
  - [x] Lower `MERGE` action bodies from generated arm-local payloads end to end.
  - [x] Promote generated-covered DML statement heads only after coverage matches the currently supported typed DML surface.
  - [x] Expand unsupported-shape diagnostics for valid PostgreSQL DML shapes that Antfly does not execute.

- [ ] Rich DDL metadata and semantic coverage
  - [ ] Represent remaining DDL semantic subshapes natively in generated AST metadata instead of broad operation tails.
  - [ ] Broaden `ALTER TABLE` generated operation coverage beyond the currently supported runtime operation families.
  - [ ] Broaden generated metadata for rich `CREATE`/`ALTER`/`DROP` catalog variants that still delegate through coarse tails.
  - [ ] Convert valid-but-unplanned DDL shapes into explicit generated unsupported diagnostics with stable reasons.
  - [ ] Add field-level fail-closed tests for each newly represented DDL subshape.

- [ ] Join execution model expansion
  - [ ] Add generated-aware planning for N-way join trees beyond the current validated binary join contract.
  - [ ] Add row-plan/API semantics for right and full outer joins.
  - [ ] Lower right/full joins directly from generated join-kind/operator metadata once execution semantics exist.
  - [ ] Expand join binding tests for aliases, `ON`, `USING`, natural joins, lateral inputs, and mixed join trees.

- [ ] Native subquery semantics
  - [ ] Add plan nodes for scalar subqueries in expressions.
  - [ ] Add semijoin/existence predicate plan nodes for `IN`, `EXISTS`, and `NOT EXISTS`.
  - [ ] Add quantified comparison and quantified pattern predicate execution for `ANY`/`ALL`/`SOME`.
  - [ ] Lower subquery predicates from generated subquery AST payloads instead of fail-closing after validation.
  - [ ] Add SQL/API parity cases for scalar, semijoin, quantified, and nested subquery execution.

- [ ] Function and expression semantic completion
  - [ ] Lower richer generated expression AST nodes directly rather than only validating token layout.
  - [ ] Complete specialized operator semantics for generated arithmetic, JSON/path, containment, overlap, regex, pattern, null-safe, and boolean families.
  - [ ] Complete semantic planning for generic functions, aggregate functions, ordered-set aggregates, `FILTER`, `WITHIN GROUP`, casts, `CASE`, arrays, `EXTRACT`, and temporal literals.
  - [ ] Keep malformed or unsupported generated expression shapes fail-closed with stable diagnostics.

- [ ] Graph DSL and Antfly extension cutover
  - [ ] Add semantic planning for non-table-function graph query syntax such as `MATCH ... RETURN`.
  - [ ] Expand graph unsupported diagnostics for valid graph syntax that is not executable.
  - [ ] Keep table-function graph sources generated-owned through binding, planning, HTTP, Lite, and document paths.
  - [ ] Add parity and fail-closed tests for graph DSL subjects, graph metrics, graph rerank, and joined graph sources.

- [ ] Broader generated unsupported PostgreSQL diagnostics
  - [ ] Add closed generated variants for remaining PostgreSQL utility, administration, and catalog statements that Antfly should recognize but not execute.
  - [ ] Give each unsupported family a stable diagnostic reason and exact subject span.
  - [ ] Ensure generated unsupported statements are only published after LR parser acceptance and retained AST validation.
  - [ ] Expand corpus coverage for PostgreSQL dump/admin compatibility shapes.

- [ ] Production ingress parser cutover
  - [ ] Remove production dependence on hand-written statement parsing for every migrated family.
  - [ ] Keep legacy-only admission only for statement shapes outside generated grammar coverage.
  - [ ] Replace remaining parser probes and string scans with generated AST dispatch or explicit unsupported diagnostics.
  - [ ] Audit public SQL, pgwire, HTTP, Lite, binder, and typed lowerer entrypoints for raw SQL reparsing.
  - [ ] Add regression tests that corrupt retained generated AST payloads at each public boundary and verify fail-closed behavior.

- [ ] Evidence and performance hardening
  - [ ] Expand accepted PostgreSQL-compatible corpus coverage as each family is cut over.
  - [ ] Expand Antfly-specific corpus coverage for query functions, graph, document, Lite, and API paths.
  - [ ] Expand deterministic malformed SQL diagnostics coverage for incomplete DDL, DML, read, CTE, unsupported, and graph shapes.
  - [ ] Keep short mutation/fuzz coverage in default tests and longer generated parser fuzzing behind `sql-parser-fuzz`.
  - [ ] Track parser throughput, allocation count, parse-table size, compile-time impact, and binary size through parser benchmarks.
  - [ ] Require SQL/API parity evidence before marking a migrated family complete.
