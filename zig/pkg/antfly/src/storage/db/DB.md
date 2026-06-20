# DB Implementation Refactor Direction

`db.zig` is the public `DB` implementation and the integration point for core
storage, index replay, schema, relational, search, transaction, enrichment,
maintenance, split, and restore behavior. The file has grown large enough that
its architecture is hard to read even when individual pieces are locally
reasonable.

This note captures the preferred direction for breaking it up without turning
the DB layer into many tiny files.

## Goals

- Keep the public `DB` API stable for callers.
- Make `db.zig` explain what `DB` is composed of, not every implementation
  detail of every subsystem.
- Split by architectural ownership, not by helper-size or line-count.
- Prefer a handful of coarse implementation modules over many narrow files.
- Avoid making call-site navigation worse while improving local readability.

## Preferred Pattern

Do not start with `pub usingnamespace` mixins.

The preferred first step is generic implementation modules plus explicit, thin
forwarding methods on `DB`:

```zig
const write_path = @import("db/write_path.zig");

pub const DB = struct {
    // fields stay here

    const Self = @This();
    const write_path_impl = write_path.Impl(Self);

    pub fn batch(self: *Self, req: types.BatchRequest) anyerror!void {
        return write_path_impl.batch(self, req);
    }
};
```

```zig
// db/write_path.zig
const types = @import("../types.zig");

pub fn Impl(comptime DB: type) type {
    return struct {
        pub fn batch(self: *DB, req: types.BatchRequest) anyerror!void {
            return batchInternal(self, req);
        }

        fn batchInternal(self: *DB, req: types.BatchRequest) anyerror!void {
            // implementation moved from db.zig
        }
    };
}
```

This keeps existing caller syntax such as `db.batch(...)`, but makes the owning
implementation module obvious. The forwarding methods also preserve `db.zig` as
the public method table for `DB`.

Implementation modules should not import `../db.zig` to name `DB`; that creates
an avoidable circular dependency. Parameterize implementation modules with
`Impl(comptime DB: type)` instead. This is usually better than `anytype` because
the implementation still compiles against the concrete `DB` type after
instantiation, while avoiding an import cycle.

Mixins can be reconsidered later for a very cohesive method family if the
forwarding boilerplate becomes a real burden. They should not be the default
because injected methods make it harder to answer "where did this method come
from?", which is one of the problems this refactor is meant to solve.

## Implementation Module Contract

The extracted implementation modules should follow a small contract so the split
does not recreate the same coupling across files:

- `db.zig` owns the concrete `DB` type, field layout, public forwarding methods,
  and cross-subsystem workflow tests.
- Implementation modules own behavior, not storage layout. They should operate
  on `*DB` passed through `Impl(comptime DB: type)`.
- Implementation modules must not import `../db.zig` to recover the `DB` type.
- `db/internal.zig` or `db/context.zig` must also not import `db.zig`. Shared
  internal types should be DB-agnostic or generic over `DB` when they truly need
  the concrete type.
- Sibling implementation modules should not call each other's private `Impl`
  declarations. Cross-module behavior should go through public `DB` forwarding
  methods, explicitly shared helpers in `internal.zig`, or lower-level domain
  modules such as `core.zig`, `query/`, `relational_store.zig`, `derived/`,
  `catalog/`, `enrichment/`, and `maintenance/`.
- If two coarse modules need the same helper, prefer moving that helper to the
  smallest shared owner instead of importing one implementation module from the
  other.

## Proposed Coarse Modules

Start with these large modules, adjusting names as the code settles:

- `db/lifecycle.zig`
  `open`, `close`, runtime initialization, optional runtime startup/teardown,
  async infrastructure, status hooks, LSM maintenance, and runtime stats
  snapshots.

- `db/write_path.zig`
  `batch`, `batchInternal`, bulk ingest sessions, batch coalescing, derived
  append, generated enrichment precompute, and document-artifact child-range
  application.

- `db/split_restore.zig`
  Range state, split deltas, shadow index manager, split/finalize, snapshot,
  restore, deferred restore markers, and restore-time runtime repair.

- `db/schema_runtime.zig`
  Schema apply, schema rewrite jobs, schema transition validation, generated
  column backfill, relational storage-mode checks, and algebraic schema reload.

- `db/relational_integrity.zig`
  Foreign key and unique constraint validation, repair, integrity progress,
  work claims, action jobs, action schedules, and related durable metadata
  records.

- `db/relational_rows.zig`
  Relational row query, mutation-source planning, joined mutation sources,
  set operations, windows, aggregates, joins, lateral queries, expression
  evaluation, ordering, and projection.

- `db/search_runtime.zig`
  Search entry points, planning stats, text search, dense/sparse search,
  graph search composition, doc-set filters, algebraic doc filters, and
  hydrated-result projection callbacks.

Keep existing domain modules such as `core.zig`, `types.zig`,
`relational_store.zig`, `query/`, `catalog/`, `derived/`, `enrichment/`, and
`maintenance/` as the lower-level building blocks. The new modules should
organize `DB` orchestration code, not absorb everything underneath them.

## Shared Internal State

Create a small internal support module early, before moving the first large
implementation family. Some current private structs and helpers are
cross-cutting enough that moved modules will need them immediately:

- `AsyncContext`
- `BatchExecutionContext`
- `EnrichmentAppendContext`
- replay/apply context structs
- profile/stat structs
- lock/backoff helpers
- shared runtime notification helpers

Use a deliberately boring name such as `db/internal.zig` or `db/context.zig`.
Keep this module narrow: it should hold shared DB orchestration state and helper
plumbing that at least two coarse modules need. Avoid creating a generic dumping
ground for unrelated helpers. It should remain below `db.zig` in the dependency
graph: `db.zig` and implementation modules may import it, but it must not import
`db.zig`.

## Suggested Migration Order

1. Move pure or mostly standalone top-level helper clusters first.
   Good candidates include restore marker parsing, projection helpers, and
   test-only fixtures.

2. Create the minimal shared internal module.
   Move only the cross-cutting structs/helpers needed by the first extraction.

3. Move `split_restore.zig`.
   It has a clear public surface and is relatively separate from the core write
   path.

4. Move `schema_runtime.zig`.
   It is large, but its ownership is clear and many routines already route
   through schema and relational-store helpers.

5. Move `search_runtime.zig`.
   Existing `query/` modules already provide natural lower-level boundaries.

6. Move `relational_rows.zig`.
   This is likely the biggest single readability win, but it has many helper
   dependencies, so it benefits from the earlier structure.

7. Move `relational_integrity.zig`.
   This overlaps transactions and schema semantics, so split it after the
   surrounding modules are stable.

8. Move `write_path.zig` last.
   It touches nearly every subsystem and should be split only once the target
   ownership boundaries are proven.

## Tests

Keep narrow tests inline with the implementation module they exercise. This is
the most Zig-like default because inline tests can access private declarations
without widening the production API.

Examples:

- Search/planning tests move with `db/search_runtime.zig`.
- Relational query, mutation, and expression tests move with
  `db/relational_rows.zig`.
- Split and restore tests move with `db/split_restore.zig`.
- Schema transition and rewrite tests move with `db/schema_runtime.zig`.
- Foreign key / unique constraint integrity tests move with
  `db/relational_integrity.zig`.

Cross-subsystem workflow tests can remain in `db.zig` when they prove the whole
`DB` composition through public behavior. Examples include restore plus
enrichment plus dense rebuild, split plus relational rows plus index replay, and
transaction plus foreign key action scheduling.

If `db.zig` is still too noisy after implementation-local tests move, introduce
a small coarse workflow test root such as `db/workflow_tests.zig`. If tests move
to separate files, make reachability explicit with an imported test aggregator
so normal DB test runs cannot silently drop them.

## Non-Goals

- Do not change the external `storage/db/mod.zig` export surface just to support
  the split.
- Do not introduce dozens of small files for individual helpers.
- Do not use mixins as the default mechanism.
- Do not have implementation modules import `db.zig` to recover the `DB` type.
- Do not refactor lower-level storage/index behavior merely because a helper was
  moved out of `db.zig`.
- Do not combine this cleanup with behavioral changes unless a move exposes a
  real bug.
