# API Layer

This directory owns Antfly's public API/runtime boundary. Public product APIs
are routed through `/db/v1/...`; internal shard and hosted-node routing stays
under `/internal/v1/...`. Generated OpenAPI types shape public requests and
responses, while the runtime behavior remains handwritten in Zig modules.

Use [PLAN.md](PLAN.md) for roadmap and remaining API-layer work. This README
documents the durable module boundaries and ownership rules for the current API
shape.

## Table Read And Write Facades

`table_reads.zig` and `table_writes.zig` are compatibility facades:

- [table_reads.zig](table_reads.zig)
- [table_writes.zig](table_writes.zig)

They should contain imports of implementation modules and `pub const`
re-exports for stable public names used by `mod.zig`, `http_server.zig`,
metadata services, serverless handlers, commands, and tests. They should not
own business logic. Leaf implementation modules should import lower-level leaf
modules directly and should not import these facade modules.

This keeps the public Zig API stable while giving table reads and writes a real
dependency graph.

## Read Modules

Read implementation lives under [table_reads/](table_reads/):

```text
table_reads/core.zig
  -> cache.zig
  -> document_sql.zig
      -> ../../sql/document_runtime.zig
      -> ../../sql/document.zig
  -> relational_rows.zig
      -> external_lake.zig
  -> remote_wire.zig
  -> fanout.zig
  -> graph.zig
  -> sources.zig
table_reads.zig facade re-exports public API
```

Ownership:

- `core.zig`
  - `TableReadSource`
  - stable lookup, scan, text-stats, and background text-stats response types
  - source option structs and low-level source capabilities
  - small shared helper types independent of concrete sources
- `cache.zig`
  - `ProvisionedTableReadCache`
  - query DB open helpers used by the read cache
  - runtime status snapshots for cached read DBs
  - identity namespace and visible-root cache-key validation
  - read-cache lifecycle diagnostics and focused cache tests
- `document_sql.zig`
  - catalog-aware document SQL lookup, scan, query, and provisioned routing glue
  - adapts `TableReadSource` to `sql/document_runtime.zig`
  - delegates document algebraic aggregate execution and fan-in merge helpers to
    `sql/document.zig`
- `relational_rows.zig`
  - lowered SQL read-plan execution
  - routed relational row materialization helpers
  - insert-source and merge-source batch builders that depend on routed scans
  - set operation, recursive CTE, join, lateral, window, and aggregate row-plan helpers
- `external_lake.zig`
  - external lake row scanners and object-storage lake sources
  - pinned/opened lake source state and resolver structs
  - Iceberg/Parquet footer discovery and delete-plan routing glue
  - lake-specific tests
- `remote_wire.zig`
  - remote lookup, scan, and query encoders
  - remote document algebraic aggregate parser
  - read-side internal group client helpers
- `fanout.zig`
  - provisioned and hosted shard fan-out helpers
  - text-stats, algebraic partial, and preflight merge logic
  - fan-out metrics
- `graph.zig`
  - graph expand, hydrate, and edge group-local wrappers
  - graph metric fan-in helpers and compatibility checks
- `sources.zig`
  - `BoundTableReadSource`
  - `ProvisionedTableReadSource`
  - `HostedProvisionedTableReadSource`
  - concrete source constructors, vtable wiring, and source-boundary tests

Important dependency rule: `external_lake.zig` may depend on
`relational_rows.zig`; `relational_rows.zig` should not depend on
`external_lake.zig`.

## Write Modules

Write implementation lives under [table_writes/](table_writes/):

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

Ownership:

- `integrity_types.zig`
  - public FK/unique integrity enums, requests, results, progress, schedules,
    job status structs, and deinit helpers
  - type-only layer used by `core.zig`, `integrity.zig`, metadata services,
    and route handlers
- `core.zig`
  - `TableWriteSource`
  - source vtable contract
  - stable source-facing request/result aliases
- `index_config.zig`
  - index config parsing and validation
  - managed embedding dimension normalization helpers
  - algebraic config validation entry points
- `integrity.zig`
  - foreign key and unique constraint planning
  - integrity worker execution
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
  - retirement, leasing, adoption, and runtime hook synchronization
- `managed_db.zig`
  - `openManagedDbForTable*`
  - index, schema, and identity loading helpers
  - HA gate/mirror open options
  - local schema application and validation glue
- `relational_mutation.zig`
  - relational row mutation staging
  - joined mutation and merge mutation helpers
  - recursive CTE mutation helpers
- `backup_restore.zig`
  - backup and restore helpers
  - dropped-table trash paths
  - restore repair work coordination
- `bulk_ingest.zig`
  - auto bulk ingest policy
  - bulk ingest session tracking
  - coalesced group batch helpers
- `remote_wire.zig`
  - write-side internal wire encoding
  - remote batch request serialization
- `sources.zig`
  - `BoundTableWriteSource`
  - `ProvisionedTableWriteSource`
  - `HostedProvisionedTableWriteSource`
  - concrete source constructors, vtable wiring, and source-boundary tests

The `integrity_types.zig` split is intentional. `TableWriteSource.VTable`
references integrity request/result types, while integrity implementation code
also uses those types. Keeping a type-only layer prevents `core.zig` and
`integrity.zig` from depending on each other.

## Build And Test Registration

`zig/build.zig` should stay at suite granularity. It may create durable
aggregates such as `api-table-reads-test`, `api-table-writes-test`,
`unit-test`, and `lib-api-test`, but it should not become an inventory of every
migrated leaf module or regression.

Focused API test inventory belongs in
[../../build/tests.zig](../../build/tests.zig):

- root test module paths live in the API test-root manifest
- exact or prefix filters live in `APITestFilters`
- aggregate dependency rules live in grouped helper functions

Stable focused test roots:

- [../api_table_reads_test_root.zig](../api_table_reads_test_root.zig)
- [../api_table_writes_test_root.zig](../api_table_writes_test_root.zig)

When adding a new split module under `table_reads/` or `table_writes/`, import
it from the relevant focused test root and cover it with a stable module-prefix
filter in `pkg/antfly/build/tests.zig`. Do not add a top-level `zig/build.zig`
step for each implementation file, and avoid one-off exact test-title filters
unless the test intentionally belongs to a narrow focused suite.

## Test Ownership

Tests should live with the behavior they prove. This is an ownership rule, not
a mechanical move rule.

- Leaf implementation tests live with the leaf implementation.
- Shared fixture tests live with the smallest shared test helper or module that
  owns the fixture.
- Source integration tests live near the concrete source that owns catalog
  routing, namespace resolution, vtable dispatch, and lifecycle behavior.
- Route and server endpoint tests stay in route/server modules.
- Cross-subsystem SQL/app parity tests stay in integration modules or focused
  test roots.
- Public facade compatibility tests stay with a facade only when they prove the
  facade contract itself.

The stable focused roots import the facade and leaf modules so moved tests keep
running under:

```sh
zig build api-table-reads-test
zig build api-table-writes-test
```

If a chunk touches SQL lowering, HTTP routing, or generated OpenAPI behavior,
also run the relevant SQL/API parity target.

## Long-Term Interface Cleanup

The large read and write source vtables should eventually split into capability
groups after the file boundaries are stable.

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

Do not rename the public facade modules as part of this cleanup. Do not move
API table logic into `src/sql` unless it no longer imports API, catalog,
routing, HTTP, or concrete table source types.
