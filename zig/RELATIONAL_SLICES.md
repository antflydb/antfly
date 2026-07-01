# RELATIONAL.md Remaining Work Slices

This is the active implementation queue for the remaining production work from
`RELATIONAL.md`. It is ordered largest-first. Keep completed work out of the
checklist; when a slice lands, move only the short gate-relevant fact into the
baseline section if future work depends on it.

Every unchecked bullet should close with one of: production implementation,
release-gated execution evidence, a release-gated unsupported fixture, or a
release-gated inventory/assertion that keeps the remaining gap visible.

## Current Gate

Run this before moving or closing any slice:

- `zig build relational-release-gate --summary failures`

The release gate currently depends on:

- `api-rows-test`
- `sql-api-parity-test`
- `sql-api-parity-fixture-check`

## Baseline Already Covered

The release gate already covers enough of these areas that they do not need
open checklist items unless a bullet below names a remaining gap:

- SQL/API parity harnesses, fixture freshness, and representative typed
  execution flows.
- Unique selectors, conflict upserts, committed `RETURNING`, JSON/array DML,
  joins, laterals, windows, CTEs, aggregates, system-time reads, row claims,
  mutation-source owner-range planning, and joined mutation-source execution.
- Hosted mutation-source and joined-mutation remote-owner staging, stale
  topology epoch rejection, collect retry before staging, wrong-owner candidate
  revalidation, global planned candidate staging across remote owners,
  zero-stage retry, post-stage topology-change fail-closed behavior without
  replay, and autocommit prepare topology-change no-replay handling.
- Unique-owner topology lifecycle and distributed handoff coverage, including
  schema-derived ranges, split/merge/rebuild, catalog routing, row-version
  proofs, and retry after topology-change prepare failure.
- FK enforcement, ref-owner topology, set-null/cascade action pages, temporal
  FK coverage, repair/action jobs, transitional topology fail-closed behavior,
  action-page transaction-id invariants, and relational identity churn
  workloads.
- Native catalog schema JSON apply, typed row rewrites,
  `ALTER COLUMN ... USING`, schema progress/hold-open/finalization,
  table-emptying barriers, `TRUNCATE ... RESTART IDENTITY`, table-emptying
  repair catch-up, table-emptying worker live-lease busy and stale-lease
  takeover behavior, table-emptying worker retry after row-claim contention,
  post-claim table-emptying worker abort invalidation, and secondary-index
  rebuild lifecycle persistence.
- Lockable row-source contracts through typed row-claim parsing, SQL lowering,
  joins, aggregates, windows, joined mutation sources, non-lockable source
  rejection, wrong-target joined mutation rejection, and hosted/provisioned
  runtime rejection before remote routing, owner-local collection, or
  owner-local staging. Direct transaction row claims fail closed against live
  double-claim attempts until the prior claim resolves, and live row claims
  survive DB reopen before being reclaimed after lease expiry without exposing
  mixed state to later writers. Mutation-source skip-locked claims across
  injected owner ranges continue past locked earlier candidates and fill limits
  from later eligible rows. Local API mutation-source row claims staged with
  row mutations survive DB reopen before commit, remain invisible until
  resolution, and can be reclaimed after lease expiry by a later
  mutation-source claimer without exposing mixed state. Committed orphaned
  mutation-source row claims and row mutations survive DB reopen as unresolved
  committed intents, block later writers/claimers until recovery, recover
  idempotently, and preserve the committed row image without exposing mixed
  state. Hosted mutation-source skip-locked routing proves a locked earlier
  remote owner does not hide a later eligible owner, and a released earlier
  claim becomes visible on retry without over-staging later owners once the
  limit is filled.
- Shared scalar expression AST coverage for currently promoted surfaces,
  including typed casts across REST/SDK-style row plans and SQL lowering.
- Routed typed read execution, scanned-owner CTE query plans, SQL cross-table
  routed scans, system-time reads, recursive CTE bounded materialization, CTE
  spill admission, and global ordered window merging across ranges. Routed CTE
  reads revalidate scanned owner rows before trusting them and fail closed on
  missing rows, changed row bodies, changed versions, missing scan versions, or
  zero scan versions. Routed join and lateral plans that materialize scanned
  CTE owner rows fail closed on missing owners, changed row bodies, and
  changed row versions before executing the local join stage. Routed merge
  joins reject range fanout, input filters, and input pagination before
  scanning even when both inputs claim leading ascending join-key order.
- Partial unique/index implication, collation/null semantics, secondary-index
  rebuild lifecycle, stale/building artifact fail-closed behavior, and
  ordered-index fail-closed scans.
- Deterministic generated-DML rejection fixtures for currently promoted
  PostgreSQL-shaped insert-source, conflict/upsert, point update, joined update,
  merge arms, returning expressions, targetless conflict, and generated
  CTE-prefix metadata paths.

## Remaining Work

- [ ] **Release parity and production chaos gate**
  - [ ] Build one release-gated chaos harness for hosted/provisioned
        insert-source upsert that moves or changes unique owners between
        conflict lookup, grouped conflict guard evaluation, staging, and commit;
        assert no duplicate unique entries, stale owner writes, or divergent SQL
        versus typed-plan results.
  - [ ] Build one release-gated FK action-page chaos harness that covers remote
        ref-owner unavailability, partial page progress, concurrent parent
        updates/deletes, concurrent child writes, lease handoff, and idempotent
        replay after resume.
  - [ ] Build one release-gated table-emptying and secondary-index rebuild
        chaos harness with concurrent row writers, abort paths, stale lease
        takeover, range repair, durable reopen, and deterministic terminal
        state assertions.
  - [ ] Add a release-gated SQL/API coverage inventory that enumerates every
        remaining PostgreSQL-shaped catalog or row-mutation syntax path that can
        affect durable state; each entry must point to a golden typed plan,
        execution/parity test, or deterministic unsupported fixture.

- [ ] **Durable schema and migration execution**
  - [ ] Implement `CREATE OR REPLACE TABLE` as one durable schema job that owns
        rebuild, validation, rewrite, backfill, CAS promotion, retry, abort,
        reopen, and idempotent replay state; gate it with partial-progress,
        crash/reopen, retry, abort, and concurrent-reader/writer tests.
  - [ ] Implement `DROP TABLE ... CASCADE` as one recoverable multi-table schema
        job that owns table-generation promotion, child table updates, range
        cleanup, sequence cleanup, partial-progress recovery, and idempotent
        replay; gate parent/child crash points and restart convergence.
  - [ ] Add operator controls for large table-emptying and schema rewrite jobs:
        progress API, pause, resume, abort, retry, stale lease takeover, worker
        admission bounds, and surfaced failure classes.
  - [ ] Add concurrent-writer tests proving no reader or writer can observe or
        write mixed schema generations during rewrite/backfill, table-emptying,
        CAS promotion, or restart-identity sequence reset.

- [ ] **Routed reads, joins, CTEs, and streaming**
  - [ ] Define and gate row-version visibility rules across remote owners for
        system-time reads, live-write pagination, and non-CTE multi-stage joins
        under range movement; after the routed CTE and CTE-backed join
        fail-closed baseline, include stale-owner and changed owner-version
        cases that require distributed topology retry or deterministic
        fail-closed behavior.
  - [ ] Implement owner-stream join planning from cardinality and index hints,
        with explicit lookup/hash/merge choices in the typed plan and
        deterministic fallback when stats are missing; add parity cases where
        the same query chooses each strategy and where missing stats fail over.
  - [ ] Keep routed merge joins fail-closed until every non-materialized routed
        source shape has a durable proof of global leading ascending join-key
        order after owner scans, topology retry, filtering, and pagination.
  - [ ] Stream CTE and recursive CTE results through the durable spill store
        with backpressure, resumable cursors, bounded memory, and spill
        accounting instead of bounded in-memory materialization only.
  - [ ] Add live-write and range-movement pagination tests for routed reads that
        prove stable ordering, no duplicates, no missing rows, and deterministic
        retry or fail-closed behavior.

- [ ] **Advanced aggregates, windows, and rollups**
  - [ ] Implement spill-backed window execution for partitions that exceed
        current bounded caps, including per-partition memory limits, spill
        bytes, reload accounting, and failure classes.
  - [ ] Add aggregate expression pushdown only when the native producer proves
        exact typed-expression semantics, collation behavior, null behavior,
        and deterministic fallback to non-pushdown execution.
  - [ ] Track per-group and per-metric memory, spill bytes, reload cost, and
        failure class for aggregate state; expose the counters in the same
        diagnostic path used by release-gated execution tests.
  - [ ] Add execution fixtures for usage rollups, RBAC membership queries,
        wake-one queries, dashboards, and migration/backfill validation queries;
        each fixture should compare SQL and REST/SDK typed-plan behavior.

- [ ] **Shared scalar expression AST completion**
  - [ ] Inventory every remaining SQL-only or string-shaped expression lowerer
        case for checks, generated columns, expression indexes, partial
        predicates, conflict actions, update transforms, casts, aggregate
        inputs/filters, order keys, windows, `RETURNING`, and rewrite `USING`;
        gate the inventory so new string-shaped cases cannot be added silently.
  - [ ] Move each remaining expression case to one typed expression AST node and
        one binder path at the adapter boundary; delete or fail-close the
        string-shaped compatibility path in the same change.
  - [ ] Expose the same typed expression nodes through REST/SDK row plans and
        prove SQL and non-SQL callers execute the same expression contract.
  - [ ] Make expression pushdown an optimizer decision over typed expression
        properties, never a parser spelling decision; add negative tests for
        unsafe collation, null, volatility, and type-coercion cases.

- [ ] **Planner trust and derived access paths**
  - [ ] Extend semantic implication beyond the currently gated partial-index
        cases with explicit equivalence classes, collation rules, null
        semantics, and negative tests for unsafe implication.
  - [ ] Add durable ordered-composite access-path metadata for accepted ordered
        index element clauses, including table-generation ownership,
        validation state, and promotion state.
  - [ ] Route ordered-composite and expression-derived rebuild/promotion through
        the same generation/CAS lifecycle as ordinary secondary indexes.
  - [ ] Prove stale, building, unvalidated, or partially promoted derived
        artifacts cannot affect query answers, uniqueness enforcement, FK
        enforcement, or upsert conflict selection.

- [ ] **Point CRUD and conflict upsert hardening**
  - [ ] Add range-movement chaos around routed insert-source conflict lookup,
        conflict guard evaluation, staged write commit, and temporal unique
        overlap selection.
  - [ ] Add concurrent-write tests for conflict actions that read `excluded`,
        existing row state, defaults, generated columns, sequence values, and
        committed-row images.
  - [ ] Prove committed-row `RETURNING` is identical for SQL, REST/SDK typed
        plans, local storage execution, and routed execution.
  - [ ] Keep non-unique point mutations on explicit claimed mutation-source
        plans and reject fallback scans that cannot prove lockable base rows.

- [ ] **Multi-row DML and queue claims**
  - [ ] Add release-gated range-movement chaos for mutation-source update and
        delete using real hosted/provisioned owner changes between collect,
        plan, owner-local stage, claim renewal, commit/abort, and claim
        resolution; assert no stale owner writes, duplicate claims, lost
        eligible rows, or SQL/typed-plan divergence.
  - [ ] Add routed queue-claim pagination and fairness tests under live
        split/merge/leader handoff: after the stable-topology hosted fairness
        baseline, prove locked earlier rows do not permanently hide eligible
        later rows and limit/order semantics continue to match local storage
        across topology changes.
  - [ ] Add hosted/provisioned API crash/reopen coverage for mutation-source
        participant resolution after a committed owner-local stage: remote
        resolve retry must be idempotent, participant markers must converge,
        and stale topology/lease-expiry races must fail closed without
        duplicate claims or stale owner writes.
  - [ ] Add routed/API replay coverage for expired mutation-source claims after
        reopen: a later hosted/provisioned claimer must reclaim or fail closed
        deterministically without duplicate claims, stale owner writes, or
        visible mixed state.
  - [ ] For each new derived mutation-source stage after the current typed API,
        SQL lowering, hosted routing, owner-local collection/staging, joins,
        aggregates, windows, and materialized CTE contracts, add a
        filter-listed negative test proving non-lockable source plans are
        rejected before remote routing and owner-local staging.

- [ ] **JSONB, arrays, and embedded document fields**
  - [ ] Expose SQL-supported JSON/array selectors and transforms directly
        through REST/SDK typed plans with the same typed nodes used by SQL
        lowering.
  - [ ] Move any remaining JSON/array SQL operators to typed expression or
        transform nodes with shared execution and deterministic unsupported
        fixtures for unsupported operators.
  - [ ] Run embedded JSON schema/template changes through durable catalog
        rebuild or rewrite work with generation ownership, retry, abort, and
        promotion evidence.
  - [ ] Prove embedded document-index queries cannot bypass relational
        row-store consistency, visibility, row claims, or typed-column
        enforcement.

- [ ] **SQL adapter and compatibility wrapper removal**
  - [ ] Expand the application SQL and migration-equivalence corpus with every
        PostgreSQL-shaped syntax path that can affect relational catalog or row
        state; fail the fixture check when a required case lacks a typed-plan,
        adapter-only no-op, or unsupported classification.
  - [ ] Normalize placeholders, aliases, table targets, generated names, and
        conflict targets structurally instead of through SQL string scans.
  - [ ] Bind each runtime statement and native migration step against Antfly
        catalog snapshots before planning; fail closed on stale or ambiguous
        catalog references.
  - [ ] Store each corpus case as one of: golden typed plan, explicit
        adapter-only no-op, or explicit unsupported classification naming the
        missing native feature.
  - [ ] Inventory legacy string-only compatibility wrappers, then remove one
        wrapper class at a time behind parser, binder, plan, and runtime parity
        evidence.

- [ ] **Document SQL dependency guard**
  - [ ] Add release-gated dependency assertions proving document SQL source
        families tracked in `SQL_SLICES.md` do not introduce SQL-only parser,
        session, auth, response, expression, or durable storage semantics that
        bypass the relational evidence rules above.
