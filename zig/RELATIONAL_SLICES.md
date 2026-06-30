# RELATIONAL.md Remaining Work Slices

This tracks the remaining production work from `RELATIONAL.md`, ordered with
the largest and highest-risk items first. Each slice should land with typed-plan
tests, runtime/execution coverage where applicable, and parity evidence that
SQL and non-SQL callers use the same native Antfly contracts.

## Largest Production Slices

- [ ] **Production parity and release gates**
  - [x] Build a SQL/API parity suite that combines corpus golden plans with
        representative execution flows.
        Evidence: `zig build sql-api-parity-test` runs the SQL/API corpus
        classifier alongside relational HTTP, storage read/write, join,
        lateral, and typed SQL execution tests from
        `pkg/antfly/build/tests.zig`.
  - [ ] Cover live writes, FK checks/actions, unique-owner repair, secondary
        index rebuilds, embedded-JSON rebuilds, row claims, joins, aggregates,
        range movement, catalog promotion, and typed rewrite/backfill jobs.
        - [x] Promote existing relational-row execution coverage into the
              SQL/API parity gate for unique-selector/upsert paths, defaults
              and returning, JSON mutation, joins, laterals, windows, CTEs,
              lake aggregates, hosted/range-movement fail-closed reads,
              system-time visibility, mutation-source execution, joined
              mutation-source execution, and row-claim lease recovery.
              Evidence: `zig build sql-api-parity-test` now includes the
              corresponding focused filters from `APITestFilters.rows`.
  - [ ] Gate new PostgreSQL-shaped syntax on typed Antfly plans, deterministic
        errors, and routing/repair/concurrent-write coverage.
  - [x] Split parser/binder/plan fixture freshness from behavioral parity
        checks.
        Evidence: `zig build sql-api-parity-fixture-check` owns fixture
        freshness, while `zig build sql-api-parity-test` owns behavioral
        parity.

- [ ] **Schema and migration production execution**
  - [ ] Apply migration-equivalent plans transactionally through native catalog
        schema JSON.
  - [ ] Schedule validation, rebuild, rewrite, and backfill work for every
        non-empty-table derived artifact or non-additive schema change.
  - [ ] Bind `CREATE OR REPLACE TABLE` rebuild/validation/rewrite work to the
        same durable job and promotion path as index, constraint, and row
        rewrite jobs.
  - [ ] Attach `DROP TABLE ... CASCADE` multi-table catalog changes to one
        durable schema job and table-generation promotion.
  - [ ] Finish production hardening for catalog-owned table-emptying barriers:
        range movement, retries, aborts, concurrent writers, and operational
        controls for large table-emptying jobs.
  - [ ] Attach storage-specific identity allocator reset state behind the
        native reset boundary for `TRUNCATE ... RESTART IDENTITY`.

- [ ] **Routed reads, joins, CTEs, and stream execution**
  - [ ] Add row-version visibility rules across remote owners.
  - [ ] Plan routed owner-stream strategies from cardinality and index hints
        for lookup, hash, and merge joins.
  - [ ] Integrate streaming and routed backpressure over the durable CTE spill
        store.
  - [ ] Harden routed ordering, pagination, and range-collected candidate
        reduction under range movement and live writes.
  - [ ] Keep explicit merge requests fail-closed until sorted inputs are proven
        for every routed source shape.

- [ ] **Lateral, windows, advanced aggregates, and rollups**
  - [ ] Add routed/window spill and backpressure for workloads that exceed the
        current bounded caps.
  - [ ] Add aggregate expression pushdown where the native producer can prove
        exact semantics.
  - [ ] Add per-group and per-metric memory accounting for spill-backed
        aggregate state.
  - [ ] Cover usage rollups, RBAC membership queries, wake-one, dashboard, and
        migration/backfill workloads with execution tests.
  - [ ] Keep recursive CTEs rejected with explicit alternatives until a bounded
        native execution model exists.

- [ ] **Shared scalar expression AST completion**
  - [ ] Replace remaining scattered lowerer cases with one typed expression AST
        for checks, generated columns, expression indexes, partial predicates,
        conflict actions, update transforms, aggregate filters/inputs,
        aggregate-output predicates, order keys, windows, `RETURNING`, and
        rewrite `USING`.
  - [ ] Type-bind each supported expression once at the adapter boundary.
  - [ ] Ensure every supported expression is executable from REST/SDK typed
        plans as well as SQL.
  - [ ] Make planner pushdown an optimizer property of typed expressions, not a
        parser or SQL-spelling decision.

- [ ] **Planner trust and derived index lifecycle**
  - [ ] Extend the shared semantic expression implication checker across more
        safe equivalence classes.
  - [ ] Extend collation and null semantics to expression-derived and
        ordered-composite access paths.
  - [ ] Route expression-derived rebuild and promotion through the same
        generation/CAS lifecycle as ordinary indexes.
  - [ ] Add durable ordered-composite access-path metadata for accepted ordered
        index element clauses.
  - [ ] Prove stale, building, or unvalidated derived artifacts cannot affect
        query answers or write enforcement.

## DML And Data-Model Slices

- [ ] **Point CRUD and conflict upsert hardening**
  - [ ] Keep conflict actions and `RETURNING` on the shared expression tree.
  - [ ] Evaluate conflict actions and `RETURNING` over the final committed row
        image.
  - [ ] Harden range-movement chaos coverage around routed insert-source
        conflict lookup and staging.
  - [ ] Keep non-unique selectors on explicit claimed mutation-source plans.

- [ ] **Multi-row DML and queue-claim hardening**
  - [ ] Add routed range-movement hardening for mutation-source update/delete
        and queue-claim execution.
  - [ ] Keep claims illegal over joins, aggregates, windows, and materialized
        CTEs until those stages expose lockable base rows.
        - [x] Add typed API regression coverage proving claimed CTE producers
              are rejected before aggregate, join, lateral, or window consumers
              can materialize them.
              Evidence: `api-rows-test` includes
              `relational rows cte plan contract accepts ordered typed
              subplans`; direct filtered execution passed.
  - [ ] Harden table-emptying barrier workers under range movement, retries,
        aborts, and concurrent writers.
  - [ ] Prove claims do not double-claim, miss rows after range movement, or
        mutate rows without a lockable source contract.
        - [x] Revalidate planned joined mutation-source candidates against the
              target and source schemas before staging, including source-side
              field collation for separate-schema source rows.
              Evidence: `api-rows-test` includes
              `relational joined mutation source stages target updates with
              separate source schema` and passed. `db-relational-rows-test`
              also covers direct, OR, NOT, access-OR, and access-NOT
              source-side predicate rechecks inheriting schema collation.

- [ ] **JSONB and arrays**
  - [ ] Route remaining SQL JSON/array operators through typed JSON/array
        expression nodes.
  - [ ] Expose the same JSON/array selectors and transforms directly through
        API/SDK typed plans.
  - [ ] Run embedded JSON schema/template changes through catalog rebuild work.
  - [ ] Prove embedded document-index queries do not bypass relational row-store
        consistency.

## SQL Adapter And Compatibility Slices

- [ ] **SQL capture and adapter boundary completion**
  - [ ] Expand representative SQL and migration-equivalence corpus coverage.
  - [ ] Normalize placeholders and aliases structurally rather than through SQL
        string scans.
  - [ ] Bind each runtime statement and native migration step against Antfly
        catalog snapshots.
  - [ ] Store golden typed plans, explicit adapter-only no-ops, or explicit
        unsupported classifications with the native model feature required.

- [ ] **Compatibility wrapper removal**
  - [ ] Inventory string-only and legacy compatibility wrappers.
  - [ ] Gate removal on typed parser, binder, plan, and runtime parity evidence.
  - [ ] Delete one wrapper class at a time.
  - [ ] Keep wrappers only when they have a named compatibility reason and
        cannot change durable catalog or row state.

- [ ] **Document SQL dependency tracking**
  - [ ] Keep document SQL source-family work tracked in `SQL_SLICES.md`.
  - [ ] Ensure document SQL read parity, derived-index function parity, views,
        residual exactness, and eventual document writes do not introduce
        SQL-only storage semantics.
  - [ ] Reuse the same parser, session, auth, response, expression, and parity
        evidence rules as relational SQL.
