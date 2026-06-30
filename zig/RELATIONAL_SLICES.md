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
              lake aggregates, local and hosted system-time read routing,
              mutation-source owner-range planning and execution, joined
              mutation-source owner-range planning and execution, and row-claim
              lease recovery.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include the corresponding exact
              focused filters from `APITestFilters.rows`.
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
        - [x] Gate the current native catalog schema JSON path in rows and
              SQL/API parity: clone/replace table DDL, incremental relational
              DDL, cross-table FK validation, parent-table drop restriction,
              and `DROP TABLE ... CASCADE` child schema updates.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include the corresponding
              `catalog apply` and `metadata catalog validation` filters.
  - [ ] Schedule validation, rebuild, rewrite, and backfill work for every
        non-empty-table derived artifact or non-additive schema change.
        - [x] Gate current schema work scheduling for typed row rewrites,
              full-row rewrite marking, graph/graph-metric rebuild metadata,
              `ALTER COLUMN ... USING` per-range rewrite/validation jobs, and
              FK validation-state promotion through schema updates.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include typed row rewrite,
              graph metric rebuild, SQL `ALTER COLUMN USING`, and metadata
              FK validation-state filters.
  - [ ] Bind `CREATE OR REPLACE TABLE` rebuild/validation/rewrite work to the
        same durable job and promotion path as index, constraint, and row
        rewrite jobs.
        - [x] Gate the current `CREATE OR REPLACE TABLE` schema-planning
              contract: replacement applies to public schema JSON and records
              rebuild, validation, and rewrite work requirements.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include
              `catalog apply creates clones and replaces public schema json`.
  - [ ] Attach `DROP TABLE ... CASCADE` multi-table catalog changes to one
        durable schema job and table-generation promotion.
        - [x] Gate the current catalog CASCADE shape: referenced parent drops
              are rejected without CASCADE, while CASCADE removes the parent
              and updates child FK metadata through the catalog drop/update
              request path.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include the two metadata catalog
              parent-drop/CASCADE validation filters.
  - [ ] Finish production hardening for catalog-owned table-emptying barriers:
        range movement, retries, aborts, concurrent writers, and operational
        controls for large table-emptying jobs.
        - [x] Gate the current catalog-owned table-emptying lifecycle in rows
              and SQL/API parity: deterministic per-range jobs, malformed job
              rejection, range-topology repair, worker leases, stale topology
              invalidation, affected-table validation, barrier completeness,
              one-barrier-per-mutation promotion, and durable job cleanup.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include the corresponding
              `table manager`, `catalog jobs`, and `table emptying worker`
              filters.
  - [ ] Attach storage-specific identity allocator reset state behind the
        native reset boundary for `TRUNCATE ... RESTART IDENTITY`.
        - [x] Gate the current `TRUNCATE ... RESTART IDENTITY` path in rows and
              SQL/API parity: SQL lowering into claimed table-emptying deletes,
              typed rows API validation that restart identity is legal only for
              whole-table claimed deletes, worker completion before catalog
              reset, barrier promotion through the catalog reset hook, and
              table-owned sequence-default reset.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include truncate lowering,
              restart-identity rows validation, table-emptying worker, catalog
              jobs, and table-manager reset filters.

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
        - [x] Gate the current typed join-strategy contract in SQL/API parity:
              automatic joins select lookup/hash/merge only from planner
              evidence, explicit merge returns no selected strategy unless both
              inputs prove leading ascending join-key order, and descending or
              wrong-key ordering does not count as sorted.
              Evidence: `zig build sql-api-parity-test` includes
              `relational rows join strategy selection is explicit and fail
              closed for unproven merge` and `relational rows join sorted input
              proof requires leading ascending join key order`.

- [ ] **Lateral, windows, advanced aggregates, and rollups**
  - [ ] Add routed/window spill and backpressure for workloads that exceed the
        current bounded caps.
        - [x] Gate the bounded recursive CTE stream/materialization contract in
              the rows and SQL/API parity suites, including lowering metadata,
              bounded execution, spill-required admission, and hard row/byte
              caps.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include recursive CTE stream
              contract and materialization spill-policy filters.
  - [ ] Add aggregate expression pushdown where the native producer can prove
        exact semantics.
  - [ ] Add per-group and per-metric memory accounting for spill-backed
        aggregate state.
  - [ ] Cover usage rollups, RBAC membership queries, wake-one, dashboard, and
        migration/backfill workloads with execution tests.
  - [x] Keep recursive CTEs rejected with explicit alternatives until a bounded
        native execution model exists.
        Evidence: bounded native recursive CTE lowering and execution are now
        gated by the rows and SQL/API parity suites.

- [ ] **Shared scalar expression AST completion**
  - [ ] Replace remaining scattered lowerer cases with one typed expression AST
        for checks, generated columns, expression indexes, partial predicates,
        conflict actions, update transforms, aggregate filters/inputs,
        aggregate-output predicates, order keys, windows, `RETURNING`, and
        rewrite `USING`.
        - [x] Gate the current shared expression AST surfaces in rows and
              SQL/API parity: expression checks, default/generated-column batch
              returning coverage, conflict-action expression updates, typed
              `RETURNING` expressions, aggregate inputs and filters, scalar
              order keys, and row-number window plans.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include the focused shared
              expression AST, conflict expression, typed returning, aggregate
              expression, scalar order-key, and row-number window filters.
  - [ ] Type-bind each supported expression once at the adapter boundary.
  - [ ] Ensure every supported expression is executable from REST/SDK typed
        plans as well as SQL.
  - [ ] Make planner pushdown an optimizer property of typed expressions, not a
        parser or SQL-spelling decision.

- [ ] **Planner trust and derived index lifecycle**
  - [ ] Extend the shared semantic expression implication checker across more
        safe equivalence classes.
        - [x] Gate current implication behavior in rows and SQL/API parity:
              partial secondary indexes and partial unique owners are used only
              when predicates imply their conditions; SQL partial conflict
              targets and point selectors lower through typed predicates; and
              generated expression columns can serve as non-unique expression
              indexes.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include partial secondary index,
              partial unique owner, expression partial implication, partial
              conflict-target, partial point-selector, and generated expression
              index filters.
  - [ ] Extend collation and null semantics to expression-derived and
        ordered-composite access paths.
        - [x] Gate current collation/null semantics for trusted access paths:
              unique constraints, partial unique predicates, expression partial
              predicates, expression unique tuples, unique-owner lookup, and
              JSON `null` implication all honor the active relational collation
              and null-distinctness rules.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include relational store and rows
              filters for case-insensitive unique constraints, partial unique
              predicates, expression unique tuples, unique-owner lookup, and
              partial `is_not_null` implication.
  - [ ] Route expression-derived rebuild and promotion through the same
        generation/CAS lifecycle as ordinary indexes.
        - [x] Gate current secondary-index rebuild lifecycle in rows and
              SQL/API parity: metadata planning derives rebuild ranges for
              building relational indexes, placement assigns rebuild ranges,
              table manager owns lifecycle state, workers claim/repair/finish
              ranges, stale ranges are invalidated before rebuild, and stale
              ready-generation promotion is ignored.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include metadata reconciler,
              placement planner, table-manager secondary-index rebuild, worker,
              and stale-promotion filters.
  - [ ] Add durable ordered-composite access-path metadata for accepted ordered
        index element clauses.
        - [x] Gate current fail-closed ordered-index behavior: ordered
              secondary index metadata can be present on relational columns, but
              row queries still scan base rows instead of trusting ordered
              access paths before the ordered-composite lifecycle is complete.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include `relational rows query
              ignores ordered secondary indexes and scans base rows`.
  - [ ] Prove stale, building, or unvalidated derived artifacts cannot affect
        query answers or write enforcement.
        - [x] Gate current stale/building artifact behavior: building secondary
              indexes are ignored for query answers, ordered secondary indexes
              fail closed to base scans, and unvalidated unique owners cannot
              satisfy conflict-target upserts.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include building-index,
              ordered-index, and unvalidated unique-owner filters.

## DML And Data-Model Slices

- [ ] **Point CRUD and conflict upsert hardening**
  - [ ] Keep conflict actions and `RETURNING` on the shared expression tree.
        - [x] Gate current SQL/API conflict-action and `RETURNING` lowering:
              conflict expression updates, explicit/default assignments,
              insert-value `RETURNING`, typed returning expressions, and
              generated metadata validation all lower into typed row batches or
              expression plans instead of string-only compatibility paths.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include conflict expression,
              explicit/default conflict assignment, insert returning, typed
              returning expression, and generated returning metadata filters.
  - [ ] Evaluate conflict actions and `RETURNING` over the final committed row
        image.
        - [x] Gate current committed-row image behavior in rows and SQL/API
              parity: batch `RETURNING` projects committed mutation images,
              defaults and generated columns materialize into returned rows,
              sequence defaults resolve through explicit write options, and SQL
              default insert/update values lower into native typed row writes.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include committed batch
              returning, server/sequence defaults, generated-column returning,
              default insert, default update, and sequence-default write-option
              filters.
  - [ ] Harden range-movement chaos coverage around routed insert-source
        conflict lookup and staging.
        - [x] Gate current insert-source conflict execution in rows and
              SQL/API parity: SQL insert-source plans build typed batches from
              routed scans, unique and temporal unique conflicts execute
              through relational storage, and storage validates grouped
              conflict guard expressions before admitting staged writes.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include insert-source plan,
              insert-source unique conflict, temporal unique conflict, and
              storage-bound conflict guard filters.
  - [ ] Keep non-unique selectors on explicit claimed mutation-source plans.
        - [x] Gate current claimed mutation-source contract in rows and
              SQL/API parity: typed mutation-source requests parse and encode
              claimed update/delete plans explicitly, SQL lowers claimed update
              and delete mutation sources, and row-claim execution blocks
              conflicting direct/transactional writes until claim resolution.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include mutation-source contract,
              mutation-source encoder, claimed update/delete lowering, row
              claim blocking, and skip-locked row claim filters.

- [ ] **Multi-row DML and queue-claim hardening**
  - [ ] Add routed range-movement hardening for mutation-source update/delete
        and queue-claim execution.
        - [x] Gate current queue-claim execution behavior in rows and SQL/API
              parity: skip-locked row claims return only claimed subsets, fill
              limits from later candidates, and transaction claim search uses
              the same lock-aware skip-locked behavior.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include rows query skip-locked
              claim and db transaction skip-locked claim filters.
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
        - [x] Gate current table-emptying worker behavior for claimed row
              deletes, stale range invalidation before mutation, malformed
              affected-table invalidation, restart-identity completion before
              catalog reset, and same-name table selection by table id.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include the focused
              `table emptying worker` filters.
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
        - [x] Gate the current relational SQL JSONB/array lowering surface in
              rows and SQL/API parity: JSONB literals/builders/`convert_from`,
              JSONB concat and `jsonb_set` update plans, conflict-action JSONB
              updates, array update transforms, JSONB containment/extraction
              predicates/projections, array containment/equality predicates,
              array projection functions, and `string_to_array` predicates.
              Evidence: `zig build api-rows-test` and
              `zig build sql-api-parity-test` include the corresponding
              `sql adapter lower dml` and `sql adapter lower expr` JSONB/array
              filters.
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
