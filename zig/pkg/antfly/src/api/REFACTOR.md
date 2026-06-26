# API Refactor Plan

This note captures the long-term shape for splitting the large API table read
and write modules without destabilizing the public Zig API surface.

## Current Problem

`table_reads.zig` and `table_writes.zig` have become orchestration modules for
several mostly independent systems:

- table source interfaces and compatibility facades
- bound, provisioned, and hosted source implementations
- catalog-aware namespace routing
- SQL and relational row execution
- document SQL adapters
- external lake row sources
- remote internal HTTP wire encoding
- graph and algebraic fan-out/fan-in
- runtime status and cache lifecycle
- schema/index maintenance jobs
- foreign key and unique constraint integrity workers
- backup, restore, bulk ingest, and transaction helpers
- large focused regression test suites

The files are difficult to review because source interface contracts, local DB
open/cache lifecycle, query execution, remote wire code, and test fixtures all
live in one compilation unit. The refactor should reduce coupling while keeping
callers stable.

## Architectural Direction

Keep these files as compatibility facades:

- `src/api/table_reads.zig`
- `src/api/table_writes.zig`

Move implementation into sibling directories:

- `src/api/table_reads/*.zig`
- `src/api/table_writes/*.zig`

The facades should re-export stable public names used by `api/mod.zig`,
`http_server.zig`, metadata services, serverless handlers, commands, and tests.
Leaf modules should import lower-level leaf modules directly. They should not
import the facade module that re-exports them.

This gives us a real dependency graph instead of only smaller files.

## Read-Side Shape

Target module graph:

```text
table_reads/core.zig
  -> cache.zig
  -> document_sql.zig
  -> relational_rows.zig
      -> external_lake.zig
  -> remote_wire.zig
  -> fanout.zig
  -> graph.zig
  -> sources.zig
table_reads.zig facade re-exports public API
```

Proposed modules:

- `core.zig`
  - `TableReadSource`
  - `LookupResponse`
  - `ScanResponse`
  - `TextStatsResponse`
  - `BackgroundTextStatsResponse`
  - source option structs and low-level source capabilities
  - small shared helper types that do not depend on concrete source
    implementations
- `cache.zig`
  - `ProvisionedTableReadCache`
  - query DB open helpers used by the read cache
  - runtime status snapshots for cached read DBs
  - identity namespace and visible-root validation for cache keys
  - read cache lifecycle diagnostics and focused cache tests
- `document_sql.zig`
  - `DocumentSqlRuntimeSourceAdapter`
  - catalog-aware document SQL lookup/scan/query glue
  - document algebraic aggregate execution and fan-in merge helpers
  - document SQL-specific tests
- `relational_rows.zig`
  - `executeLoweredSqlReadPlan*`
  - routed relational row materialization helpers
  - insert-source and merge-source batch builders that depend on routed scans
  - set operation, recursive CTE, join, lateral, window, and aggregate row-plan
    helpers
- `external_lake.zig`
  - `PinnedExternalLakeRowsScanner`
  - object-storage lake source implementations
  - pinned/opened lake source state and resolver structs
  - Iceberg/Parquet footer discovery and delete-plan routing glue
  - lake-specific tests
- `remote_wire.zig`
  - remote lookup/scan/query encoders
  - remote document algebraic aggregate parser
  - remote graph and join response parsers if they are still read-source-owned
  - internal group read client helpers that do not belong in `http_client.zig`
- `fanout.zig`
  - provisioned and hosted shard fan-out helpers
  - text stats merge
  - algebraic partial merge
  - preflight merge
  - fan-out metrics
- `graph.zig`
  - graph expand/hydrate/edges group-local wrappers
  - graph metric fan-in helpers and compatibility checks
- `sources.zig`
  - `BoundTableReadSource`
  - `ProvisionedTableReadSource`
  - `HostedProvisionedTableReadSource`
  - source constructors and vtable wiring

Important dependency rule: `external_lake.zig` should depend on
`relational_rows.zig`, not the reverse. The current lake source wrappers call
the routed relational row-plan helpers, so extracting relational helpers first
prevents circular imports.

## Write-Side Shape

Target module graph:

```text
table_writes/integrity_types.zig
table_writes/core.zig
  -> index_config.zig
  -> integrity.zig
  -> schema_jobs.zig
  -> cache.zig
  -> managed_db.zig
  -> relational_mutation.zig
  -> backup_restore.zig
  -> bulk_ingest.zig
  -> remote_wire.zig
  -> sources.zig
table_writes.zig facade re-exports public API
```

Proposed modules:

- `integrity_types.zig`
  - public FK/unique integrity enums, result structs, progress structs,
    schedule/job status structs, and deinit helpers
  - type-only module used by `core.zig`, `integrity.zig`, metadata services,
    and route handlers
- `core.zig`
  - `TableWriteSource`
  - source vtable contract
  - stable source-facing request/result aliases
- `index_config.zig`
  - index config parsing and validation
  - managed embedding dimension normalization helpers
  - algebraic config validation entry points
  - tests currently around the index parser
- `integrity.zig`
  - foreign key and unique constraint integrity planning
  - worker execution
  - durable FK action jobs and schedules
  - schema controller maintenance
  - integrity diagnostics and stable job IDs
- `schema_jobs.zig`
  - secondary index rebuild workers
  - schema rewrite workers
  - promotion of ready secondary indexes
- `cache.zig`
  - `ProvisionedTableWriteCache`
  - hosted managed DB cache diagnostics
  - retirement, leasing, adoption, runtime hook synchronization
- `managed_db.zig`
  - `openManagedDbForTable*`
  - index/schema/identity loading helpers
  - HA gate/mirror open options
  - local schema application and validation glue
- `relational_mutation.zig`
  - relational row mutation staging
  - joined mutation and merge mutation helpers
  - recursive CTE mutation helpers
- `backup_restore.zig`
  - backup/restore helpers
  - dropped-table trash paths
  - restore repair work coordination
- `bulk_ingest.zig`
  - auto bulk ingest policy
  - bulk ingest session tracking
  - coalesced group batch helpers
- `remote_wire.zig`
  - internal group write encoders
  - remote batch request serialization
  - write-side wire-format tests that do not need source routing
- `sources.zig`
  - `BoundTableWriteSource`
  - `ProvisionedTableWriteSource`
  - `HostedProvisionedTableWriteSource`
  - source constructors and vtable wiring

The `integrity_types.zig` split is important. `TableWriteSource.VTable` references
integrity result and request types, while the integrity implementation also uses
those types. A type-only layer prevents `core.zig` and `integrity.zig` from
depending on each other.

## What Should Stay in the Facades

The facade files should contain:

- imports of leaf modules
- `pub const` re-exports for existing public names
- tiny compatibility aliases
- temporary compile-time imports of migrated test modules if needed

They should not retain business logic after a chunk has been migrated.

During the migration, the facades may still contain unmigrated code. The end
state should make the facades boring and small.

## Migration Order

1. Extract `table_writes/index_config.zig`.
   - This is the cleanest first move because callers already use these helpers
     directly and the code is mostly independent.
2. Extract `table_reads/core.zig`.
   - Move `TableReadSource`, response aliases, and shared source option types.
   - Keep `table_reads.zig` re-exporting all public names.
3. Extract `table_reads/cache.zig`.
   - Move `ProvisionedTableReadCache`, query DB open helpers, identity namespace
     validation, runtime status snapshots, and pure read-cache lifecycle tests.
   - Keep source-boundary tests in the facade until `sources.zig` owns the
     concrete provisioned/hosted source lifecycle.
4. Extract `table_reads/document_sql.zig`.
   - This is recently hardened and has focused tests.
   - Move document SQL runtime adapter and algebraic aggregate fan-in together.
5. Extract `table_reads/relational_rows.zig`.
   - Move routed row-plan execution before extracting lake wrappers.
6. Extract `table_reads/external_lake.zig`.
   - Keep lake source wrappers depending on relational row helpers.
7. Extract `table_reads/remote_wire.zig`.
   - Move remote parsers and encoders once the source and document SQL types are
     in leaf modules.
8. Extract `table_writes/integrity_types.zig`.
   - Re-export types from the facade before moving implementation.
9. Extract `table_writes/integrity.zig`.
   - Move planning, worker, schema-controller, and FK action job logic.
10. Extract `table_writes/schema_jobs.zig`.
   - Move secondary index rebuild and schema rewrite jobs.
11. Extract write cache and managed DB lifecycle:
    - `cache.zig`
    - `managed_db.zig`
    - `bulk_ingest.zig`
12. Move concrete source structs into `sources.zig`.
    - Do this late, after the helper logic has been pulled out. The source
      structs are the orchestration knots.
13. Revisit capability-group vtables.
    - Only after the files are split, consider replacing the large optional
      method bags with grouped capabilities.

## Testing Strategy

Keep the existing focused test roots stable:

- `api-table-reads-docid-test`
- `api-table-writes-docid-test`

Keep `zig/build.zig` at suite granularity. The API read/write refactor should
not add one `b.addTest` block or one top-level build step per extracted leaf
module or per regression. New read/write leaf tests should be imported by the
stable focused test roots and registered through the grouped API test helper in
`pkg/antfly/build/tests.zig`. `zig/build.zig` should know about durable suites
such as API table reads, API table writes, rows, DOCID lifecycle, and graph
metric coverage, not individual implementation test names.

Treat this as a build-file ownership contract:

- `zig/build.zig` may create modules, pass them to grouped helper functions, and
  connect durable suite-level steps into the default test aggregates.
- `pkg/antfly/build/tests.zig` owns focused test inventories, exact test-title
  filters, and API read/write focused-step registration.
- Focused test roots import the facade and migrated leaf modules so moved tests
  keep running under the existing `zig build api-table-reads-docid-test` and
  `zig build api-table-writes-docid-test` commands.
- A new extracted helper should not introduce a new top-level `zig/build.zig`
  test step unless it is a durable product-level suite rather than an
  implementation regression bucket.

The build also enforces this convention with guardrails in
`pkg/antfly/build/tests.zig`: inline filter lists, direct `--test-filter`
arguments, and manifest-owned API read/write test steps are rejected from
`zig/build.zig`. If a refactor needs another implementation-focused bucket, add
it to the package test manifest or an existing grouped helper instead of growing
the top-level build file.

Yes: tests should generally move with the production behavior they prove. The
important caveat is that this is an ownership move, not a mechanical file move.
Leaf implementation tests should travel with the extracted leaf module; boundary
and integration tests should stay at the boundary until that boundary itself is
extracted.

As modules move, the tests that exercise the moved behavior should usually move
in the same extraction chunk. Test movement is part of the refactor, not a
follow-up cleanup. The goal is that a reader can open the module that owns a
behavior and find the focused regressions for that behavior next to it.

The practical context is that the test tree should track production ownership,
not historical file location. If the production code leaves `table_reads.zig` or
`table_writes.zig`, the tests that prove only that moved implementation should
leave with it in the same patch. If a test still proves the facade, source
vtable, server route, storage boundary, SQL/app-parity path, or catalog
orchestration, it should stay at that boundary until the boundary itself moves.
This keeps the split honest without turning the migration into a blind test-file
shuffle.

This is the default for the migration, not an optional polish pass. Each
implementation extraction should include its owned tests, its narrow fixtures,
and the focused test-root import that keeps the existing `zig build` target
running the moved tests. Leaving tests behind in the facade is acceptable only
as a temporary checkpoint when the owner has not moved yet, or when the test is
intentionally proving a public API, route, storage, or source boundary.

This does not mean every test leaves the facade or the current focused roots.
The rule is ownership, not mechanical relocation:

- Leaf implementation tests move with the leaf implementation.
- Shared fixture tests move with the smallest shared test helper or module that
  owns the fixture.
- Integration tests stay at the boundary they prove.
- Public compatibility tests stay with the public facade only when they are
  proving the facade contract itself.

That means not all tests move at once, and not every test that mentions a moved
helper should move. A pure cache retirement test should move with
`table_reads/cache.zig`; a provisioned source test that proves catalog routing,
namespace resolution, cache invalidation, and source vtable dispatch should stay
with the source boundary until `table_reads/sources.zig` exists.

The focused test roots should become stable test aggregators. They should import
the facade and the leaf modules that own tests, but they should not become the
long-term home for implementation-specific regressions. This keeps `zig build`
targets stable while still making test ownership obvious in the source tree.

The facade or focused test root should import the leaf modules so filtered tests
continue to run through the same `zig build` targets. Moving a test into a leaf
module must not make it disappear from the normal API read/write test targets.

The practical rule is: move the test when its subject moves, but do not move a
boundary test just because it happens to call the moved helper. A helper-level
schema job regression belongs in `table_writes/schema_jobs.zig`; a provisioned
source test that proves the same schema job is reachable through catalog
routing, group-local DB opening, and vtable dispatch belongs with the source
boundary until `sources.zig` or `managed_db.zig` owns that path.

Each extraction chunk should leave behind an explicit test-placement audit:

- Name the production owner that moved.
- Move the focused implementation tests and narrow fixtures with that owner.
- Keep integration or facade tests only when they still prove a real boundary.
- Import the new test owner from the stable focused test root or facade so the
  existing `zig build` target still runs it.
- Run the focused read/write test targets and mention any intentionally deferred
  boundary tests in the change summary.

In other words, the refactor should not leave a shadow test suite behind in the
old facade files. The facade test surface should shrink as implementation leaves
it. What remains there should be intentionally named boundary coverage: public
API compatibility, source-vtable dispatch, end-to-end route behavior, and
cross-module integration that would become less clear if pinned to a narrow leaf
module.

Use these ownership rules while moving tests:

- Unit and regression tests for a moved implementation should move with that
  implementation module. For example, index config parser tests should move to
  `table_writes/index_config.zig`, document SQL aggregate fan-in tests should
  move to `table_reads/document_sql.zig`, and lake scanner tests should move to
  `table_reads/external_lake.zig`.
- Helper fixtures should move with the narrowest module that owns them. If a
  fixture is only used by lake scanner tests, keep it in `external_lake.zig`. If
  it is shared by several read modules, promote it to a small read-side test
  helper rather than keeping it in the facade by accident.
- Source integration tests should stay near the concrete source once
  `sources.zig` exists. Provisioned/hosted routing, cache invalidation, local
  group behavior, and source vtable wiring tests belong with the source or cache
  module that owns the behavior.
- Route and server endpoint tests should stay in route/server modules. For
  example, internal group route response mapping belongs in
  `http_internal_group_read_routes.zig`, not in a table read implementation
  module.
- Cross-subsystem SQL/app parity tests should stay in integration modules or
  focused test roots. A test that proves SQL lowering through a table source into
  storage should not be forced into a small leaf module just because one helper
  moved.
- Public facade compatibility tests should stay with the facade only when they
  are proving the facade API itself. They should not be used as a holding pen for
  tests whose implementation owner is now a leaf module.

Apply that split to the current write-side migration:

- `table_writes/index_config.zig` owns index config parser and validator unit
  tests.
- `table_writes/integrity.zig` owns FK/unique planning, diagnostics, stable job
  IDs, aggregation, and worker-helper regressions.
- `table_writes/schema_jobs.zig` owns secondary-index rebuild helper tests,
  schema rewrite helper tests, and ready-index promotion generation checks.
- `table_writes.zig` may temporarily keep public facade tests and source
  integration tests while source structs still live there.
- Provisioned/hosted tests that open managed DBs, exercise group-local routing,
  or prove source vtable wiring should move later with `sources.zig`,
  `managed_db.zig`, or `cache.zig`, depending on which module owns the behavior
  after the split.

When a test fixture serves several migrated modules, prefer creating a tiny
test-only helper beside the narrowest shared owner instead of leaving the fixture
in the facade. If the fixture constructs a real provisioned source or managed DB
cache, it probably belongs with source/cache integration coverage rather than a
pure leaf module.

Expected long-term test placement:

- `table_reads/core.zig` owns source contract tests that can run against a small
  fake source without catalog, HTTP, or storage setup.
- `table_reads/cache.zig` owns read cache lifecycle tests, pending-open
  coordination, generation/identity cache-key tests, retirement tests, cache
  runtime status snapshot tests, and query DB open helper tests that do not need
  the full source vtable boundary.
- `table_reads/document_sql.zig` owns document SQL adapter tests, document
  aggregate planning/fan-in tests, and optimizer-selection tests that prove full
  text, algebraic, or scan-backed document SQL paths choose the right execution
  strategy.
- `table_reads/relational_rows.zig` owns row-plan execution tests that do not
  need a real API source boundary. End-to-end SQL/storage/app-parity tests stay
  in the SQL integration test module or focused API test root.
- `table_reads/external_lake.zig` owns lake scanner, pinned lake source, footer
  discovery, object metadata, and delete-plan routing tests.
- `table_reads/remote_wire.zig` owns encoding/decoding tests for internal group
  read requests and responses.
- `table_reads/fanout.zig` owns merge and fan-out tests for text stats,
  algebraic partials, shard preflight, and shard diagnostics.
- `table_reads/graph.zig` owns graph read fan-in and metric compatibility tests.
- `table_reads/sources.zig` owns provisioned/hosted/bound source integration
  tests that prove catalog routing, namespace resolution, vtable dispatch, and
  source lifecycle behavior.
- `table_writes/core.zig` owns write source contract tests that can run against a
  fake source without opening a managed DB.
- `table_writes/index_config.zig` owns parser, validation, normalization, and
  enrichment index config tests.
- `table_writes/integrity.zig` owns foreign-key, unique, durable action,
  diagnostics, and integrity worker tests.
- `table_writes/schema_jobs.zig` owns secondary index rebuild, schema rewrite,
  pending/ready promotion, and schema job worker tests.
- `table_writes/cache.zig` owns managed write cache adoption, retirement,
  runtime hook, hosted cache, and status diagnostics tests.
- `table_writes/managed_db.zig` owns DB open option, schema/index/identity
  loading, HA gate, local schema application, and managed runtime construction
  tests that do not require the full source vtable boundary.
- `table_writes/relational_mutation.zig` owns relational mutation staging,
  joined mutation, merge mutation, and recursive CTE mutation tests.
- `table_writes/backup_restore.zig` owns backup, restore, dropped table trash,
  and restore repair coordination tests.
- `table_writes/bulk_ingest.zig` owns batch sizing, drain policy, coalescing,
  and bulk ingest session policy tests.
- `table_writes/remote_wire.zig` owns write-side internal wire encoding tests.
- `table_writes/sources.zig` owns provisioned/hosted/bound write source
  integration tests that prove catalog routing, namespace resolution, managed DB
  opening through the source, vtable dispatch, and write lifecycle behavior.
- `api-table-reads-docid-test` and `api-table-writes-docid-test` remain stable
  test aggregators. They import the facade and leaf modules so the command-line
  test targets remain stable while implementation tests live with their owners.

Each extraction should run at least:

```sh
zig build api-table-reads-docid-test
zig build api-table-writes-docid-test
```

When a chunk touches SQL lowering, HTTP routing, or generated OpenAPI behavior,
also run the relevant SQL/API parity target.

## Long-Term Interface Cleanup

After the mechanical split is complete, split the large source vtables into
capability groups.

Read capabilities:

- document key/value reads
- search/query
- document SQL
- relational rows
- external lake rows
- graph reads
- runtime status
- local group/internal worker operations

Write capabilities:

- DDL and catalog mutation
- batch writes
- transactions
- relational row mutations
- bulk ingest
- integrity maintenance
- artifact and graph maintenance
- runtime status
- local group/internal worker operations

This should be a second-stage cleanup, not the first extraction. Doing it after
the file split keeps the migration reviewable and avoids changing behavior while
moving code.

## Non-Goals

- Do not rename the public facade modules as part of the split.
- Do not move API table logic into `src/sql` unless it no longer imports API,
  catalog, routing, HTTP, or concrete table source types.
- Do not start by moving `ProvisionedTableReadSource` or
  `ProvisionedTableWriteSource`; pull leaf logic out first.
- Do not rewrite the vtable architecture during the initial file split.
