# DB Package

`src/storage/db` is the high-level database orchestration layer. It sits above
`DocStore`, storage backends, derived index runtimes, schema/catalog metadata,
SQL/relational execution, HA ownership, Lite open profiles, split/restore, and
background maintenance.

The concrete `DB` type is the composition boundary. Lower-level stores and
index modules should remain usable without knowing whether a handle is primary,
standby, Lite, server-backed, or fenced. The `DB` layer is where local mutation
semantics are combined with durability, visibility, HA, and runtime ownership
semantics.

For the broader project-level DB contract and roadmap, see
[DB.md](../../../../../DB.md).

## Core Contracts

- `db.zig` owns the concrete `DB` type, field layout, public forwarding
  methods, and cross-subsystem workflow tests.
- Implementation modules own behavior, not storage layout. They operate on
  `*DB` through `Impl(comptime DB: type)`.
- Implementation modules must not import `db.zig` to recover the concrete
  `DB` type. This avoids import cycles and keeps the dependency graph readable.
- Public caller syntax should remain `db.method(...)`; implementation ownership
  is made explicit by thin forwarding methods in `db.zig`.
- Cross-module behavior should go through public `DB` forwarding methods,
  explicitly shared helpers in `internal.zig`, or lower-level domain modules.
  Sibling implementation modules should not instantiate each other's private
  `Impl` declarations.
- Shared internal support must stay below `db.zig` in the dependency graph:
  `db.zig` and implementation modules may import it, but it must not import
  `db.zig`.

The preferred implementation pattern is:

```zig
const write_path = @import("write_path.zig");

pub const DB = struct {
    const Self = @This();
    const write_path_impl = write_path.Impl(Self);

    pub fn batch(self: *Self, req: types.BatchRequest) anyerror!void {
        return write_path_impl.batch(self, req);
    }
};
```

```zig
pub fn Impl(comptime DB: type) type {
    return struct {
        pub fn batch(self: *DB, req: types.BatchRequest) anyerror!void {
            return batchInternal(self, req);
        }
    };
}
```

Do not use `pub usingnamespace` mixins as the default extraction mechanism.
Injected methods make it harder to answer where a method came from, which is one
of the problems this split is meant to solve.

## Module Map

- `db.zig`
  Concrete `DB` fields, public forwarding methods, composition-level helpers,
  and workflow tests that prove multiple subsystems together.
- `types.zig`, `config.zig`
  Public request/response/config types for the DB surface.
- `internal.zig`
  Shared DB orchestration state and helper plumbing used by multiple coarse
  implementation modules.
- `lifecycle.zig`
  Open, close, runtime initialization, optional runtime startup/teardown, async
  infrastructure, status hooks, LSM maintenance, and runtime stats snapshots.
- `write_path.zig`
  Batch writes, bulk ingest sessions, batch coalescing, derived append,
  generated enrichment precompute, document-artifact child-range application,
  and write-side sync-level handling.
- `split_restore.zig`
  Range state, split deltas, shadow index management, split/finalize, snapshot,
  restore, deferred restore markers, and restore-time runtime repair.
- `schema_runtime.zig`
  Schema apply, schema rewrite jobs, schema transition validation, generated
  column backfill, relational storage-mode checks, Lite local schema/table
  metadata, and algebraic schema reload.
- `relational_rows.zig`
  Relational row query, mutation-source planning, joined mutation sources, set
  operations, windows, aggregates, joins, lateral queries, expression
  evaluation, ordering, and projection.
- `relational_integrity.zig`
  Foreign key and unique constraint validation, repair, integrity progress,
  work claims, action jobs, action schedules, and related durable metadata.
- `search_runtime.zig`
  Search entry points, planning stats, text search, dense/sparse search, graph
  search composition, doc-set filters, algebraic doc filters, and hydrated
  result projection callbacks.
- `graph_runtime.zig`, `maintenance/graph_metric_runtime.zig`
  DB-facing graph maintenance entry points and the lower-level graph metric
  runtime, scheduler, role, lease, worker, and planned-build coverage.
- `ha_replication.zig`, `ha_types.zig`
  HA write-gate evaluation, mirror preflight, commit gating, best-effort mirror
  helpers, and shared HA context plumbing.
- `aggregations.zig`
  Search aggregation request/result types, document-scan aggregation execution,
  algebraic/distributed aggregation adapters, and pipeline aggregation
  execution. This is a lower-level domain module exported through `mod.zig`, not
  primarily a `DB.Impl` module.
- `document_mapper.zig`, `document_query.zig`
  Parse-once document preprocessing and parsed lookup/projection helpers shared
  by lookup and hydrated search results.
- `catalog/`
  Persisted index, enrichment, resolver, and text-maintenance catalog metadata.
- `derived/`
  Change journal, replay stream, replay source, derived workers, applied
  watermark state, and async/runtime adapters for sequence-based deterministic
  index application.
- `enrichment/`
  Generated enrichment pipeline, artifacts, chunking, embedding provider
  interfaces, worker state, and leased enrichment runtime.
- `maintenance/`
  Background workers that are not index-specific, including TTL cleanup,
  transaction recovery, text merge, sparse compaction, and graph metric runtime.
- `query/`, `algebraic/`
  Lower-level search execution, projection, graph execution, algebraic planning,
  value representation, and distributed/algebraic query helpers.
- `relational_store.zig`, `core.zig`
  Lower-level durable relational row storage and canonical document storage
  building blocks used by DB orchestration.

## Write, HA, And Mirror Semantics

Any DB method that can mutate durable state must keep the mutation guard sequence
local and visible:

- reject read-only open modes with `openModeRequiresReadOnlyBackends`
- call the HA write gate unless the path is an explicit replicated-apply path
  with a narrowly scoped bypass option
- preflight synchronous HA commit gates before applying local mutations
- mirror committed batch/effect/metadata payloads after the local mutation has
  reached the same durability point as today

HA helper plumbing belongs in shared DB orchestration support. HA policy should
not disappear into lower-level stores. `core.zig`, `relational_store.zig`, Lite
page stores, and query/index modules should not need to know whether a DB handle
is primary, standby, or fenced.

## Sync-Level Semantics

`sync_level` is the caller's requested visibility boundary for a write. It is
not just a tuning flag.

- `.write` means the primary document/row mutation is durable and visible at the
  base store boundary, while derived work may continue asynchronously.
- Text, dense, sparse, graph, enrichment, and full-index levels request stronger
  visibility from the relevant derived runtimes before the write returns.
- `.full_index` is the broad derived visibility boundary for callers that need
  search/index results to observe the write immediately.
- Implementation modules must preserve the existing order: commit base state,
  append replay/derived work, notify workers, apply backlog pressure, and wait
  for the requested sync target only after the local durability point is reached.
- Internal repair, restore, metadata, and replicated-apply paths may deliberately
  force a lower sync level, but that downgrade must be explicit at the call site.

Callers should not infer derived visibility from successful write return unless
the requested `sync_level` says so.

## Lite And Local Metadata

Antfly Lite is not a separate mini-DB. It opens the same `DB` API over a Lite
backend with an `OpenMode` profile such as writer, query-readonly, or
status-only.

Future DB changes must keep these constraints first-class:

- read APIs work with read-only Lite backends
- write, schema, restore, and maintenance APIs reject read-only modes
- lifecycle/open code keeps runtime startup consistent with Lite profiles that
  disable background or distributed behavior
- schema/catalog metadata used by Lite SQL stays durable with the schema
  transition that makes it valid

The local metadata keys `local_schema_json_key` and
`local_lite_sql_table_record_json_key` are DB-owned schema/catalog metadata, not
CLI state. `applyLiteSqlTableRecord` must persist the full table record and the
runtime schema together so Lite SQL reopens with the same table identity and
generated index metadata that DDL produced. Legacy `local_schema_json_key`-only
databases may be read through the compatibility path, but new DDL should write
the full table record.

## Replay And Derived Runtime

Replay/runtime code consumes replay rows through `DocStore` methods instead of
reaching into backend-native commit streams. Replay rows live in the primary
store keyspace and are the authoritative DB-level replay surface.

The DB write path owns the order in which committed base mutations become
derived work. Derived runtimes own idempotent sequence-based catch-up from the
active replay source. Background execution is an optimization over inline
progress; correctness must not depend on OS threads existing.

The remaining future direction is to collapse backend-native recovery-only
differences into the replay-row surface where those differences still matter.

## Split, Restore, And Identity

Split and restore are DB orchestration responsibilities because they cross base
documents, derived indexes, range metadata, doc identity namespaces, schema, and
runtime repair.

The split/restore layer owns:

- split range state and split deltas
- shadow index preparation and cutover
- snapshot export/restore
- deferred restore markers
- restore-time runtime repair and replay coordination
- doc identity namespace preservation and validation

Lower-level stores may provide efficient primitives, but they do not own the DB
contract for when a restored or split range is visible to public reads/writes.

## Testing And Build Contract

Keep narrow tests inline with the implementation module they exercise. Inline
tests can access private declarations without widening the production API.

Examples:

- search and planning tests live with `search_runtime.zig`
- relational query, mutation, and expression tests live with
  `relational_rows.zig`
- split and restore tests live with `split_restore.zig` or the coarse
  split/restore workflow test file
- schema transition and rewrite tests live with `schema_runtime.zig`
- foreign key and unique constraint tests live with `relational_integrity.zig`

Cross-subsystem workflow tests can remain in `db.zig` when they prove the whole
`DB` composition through public behavior. Examples include restore plus
enrichment plus dense rebuild, split plus relational rows plus index replay, and
transaction plus foreign key action scheduling.

Build target wiring should follow the same ownership rule. Keep filter buckets
and focused DB/API test registration in `pkg/antfly/build/tests.zig`; root
`build.zig` should call coarse helpers instead of accumulating a hand-maintained
list of individual test titles.

Use stable test-name prefixes as the fine-grained selection mechanism. DB tests
should start with an owning category such as `db lifecycle`, `db write path`,
`db schema runtime`, `db relational rows`, or `db search runtime`. Exact test
title filters are for temporary local diagnosis, not long-lived build
contracts.

HA and Lite tests should stay where they prove the boundary:

- HA write-gate and mirror-helper tests live with HA helper code when possible;
  workflow tests stay with `db.zig` when they prove public DB write paths are
  correctly gated or mirrored.
- Lite open-mode, native backend, and `.aflite` page-store tests live under
  `storage/lite/`.
- Lite DB metadata tests that prove schema/table metadata durability live with
  schema-runtime code.
- CLI-only Lite SQL parsing and splitting tests live with `cmd/lite_sql.zig`.

If implementation-local tests move out of `db.zig`, make reachability explicit
with an imported test aggregator so normal DB test runs cannot silently drop
them.

## Design Rules

- Base documents remain canonical in `DocStore`.
- Derived artifacts live under binary internal artifact keys and can be shared
  across indexes.
- Public APIs expose `ArtifactRef` plus an opaque `artifact_id` token, never raw
  internal artifact keys.
- Expensive generated work is lease-owned and async.
- Lease-owned workers share ownership semantics and observability surfaces.
- Deterministic index application is sequence-based and replayable from the
  active replay source.
- Background maintenance routes through normal DB semantics instead of raw
  side-channel deletion.
- Transaction recovery repairs local coordinator intents first and only deletes
  finalized transaction metadata once all tracked participants are resolved.
- Distributed transaction recovery stays transport-agnostic at the DB layer.
- Do not refactor lower-level storage or index behavior merely because a helper
  moved out of `db.zig`.

## Related Docs

- [BATCH.md](../../../../../BATCH.md)
  Batch coalescing semantics, bulk ingest scope, and dense HBC replay-window
  policy.
- [FULL_TEXT.md](../../../../../FULL_TEXT.md)
  Full-text visibility and merge-maintenance policy.
- [ENRICHMENTS.md](../../../../../ENRICHMENTS.md)
  Enrichment architecture and artifact identity contract.
- [SIM.md](../SIM.md)
  Storage simulation testing guidance.
