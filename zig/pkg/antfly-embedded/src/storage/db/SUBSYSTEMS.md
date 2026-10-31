# DB subsystems

`DB` in `db.zig` held about 120 fields and 1,600 members. Most fields belonged
to one area, such as graph indexes, TTL cleanup or entity resolution, and only
that area's methods touched them. This document describes how `DB` is split into
subsystem structs that own that state, and where each field went.

## Layout

Subsystems are grouped by what they own. Index state is grouped by index kind
under `indexes`, so everything about one kind (its runtimes, background work and
replacement) lives in one place. Dense vector state lives in the catalog's index
manager, so `indexes` has no `dense` subsystem yet.

```
DB              core: identity, backends, runtime wiring, status
├─ indexes      range split shadow index manager and replacement lock
│  ├─ text      segment merge runtime
│  ├─ sparse    compaction runtime
│  ├─ graph     replacement, metrics, endpoint cleanup
│  ├─ relational column maintenance, rewrite caches
│  ├─ algebraic HLL maintenance lane
│  └─ repair    repair intents and scheduler, leases, shadow rebuilds, activation
├─ documents    authority backfill and repair, group sources, bulk ingest
├─ query        schema identity, retained query cut, warming, row policy
├─ enrichment   runtime, artifact production, resolution, promotion
├─ recovery     transactions, publication, portable activation, restore, source pins
├─ ttl          TTL cleanup context and runtime
└─ maintenance  quarantine retries, independent maintenance, local source
                recovery, run-until-idle diagnostics
```

Call sites read as the path to the state they act on:

```zig
_ = try db.indexes.graph.advanceReplacement();
db.indexes.sparse.restartCompaction("drop", name);
_ = try db.indexes.repair.advanceIntent(alloc, repair_id, .{});
_ = try db.documents.advanceAuthorityBackfillPage();
var cut = try db.query.openCut(request, cancellation);
try db.enrichment.addEnrichment(cfg);
_ = try db.recovery.runSourcePinCleanupStep();
```

## Pattern

A subsystem is a struct embedded in its parent as a field. It owns its state
and the methods that operate on it. It reaches `DB` through `@fieldParentPtr`,
so it needs no stored back-pointer and stays valid when `DB` is moved. A nested
subsystem steps up through its parent.

```zig
// indexes/mod.zig
pub const Indexes = struct {
    text: TextIndexes = .{},
    sparse: SparseIndexes = .{},
    graph: GraphIndexes = .{},
    ...

    pub fn dbPtr(self: *Indexes) *DB {
        return @alignCast(@fieldParentPtr("indexes", self));
    }
};

// indexes/graph.zig
pub const GraphIndexes = struct {
    replacement: Replacement = .{},

    pub const Replacement = struct {
        mutex: std.Io.Mutex = .init,
        manager: ?*IndexManager = null,
        catalog_generation: u64 = 0,
        progress: ?GraphReplacementProgress = null,
    };

    fn dbPtr(self: *GraphIndexes) *DB {
        const indexes: *Indexes = @alignCast(@fieldParentPtr("graph", self));
        return indexes.dbPtr();
    }

    pub fn advanceReplacement(self: *GraphIndexes) !bool {
        const db = self.dbPtr();
        ...
    }
};

// db.zig
pub const DB = struct {
    indexes: Indexes = .{},
    documents: Documents = .{},
    ...
};
```

Rules:

- **Callers name the path.** `DB` keeps no forwarding declarations for moved
  methods.
- **Methods are named for the action within their subsystem.** The subsystem
  path supplies the noun, so `advanceGraphReplacement` becomes
  `indexes.graph.advanceReplacement` and `quiesceSparseCompactionForStructuralMutation`
  becomes `indexes.sparse.quiesceCompaction`.
- **One feature's fields form a nested state struct** when the subsystem will
  hold more than one feature: `indexes.graph.replacement.mutex`, not
  `indexes.graph.replacement_mutex`.
- **Every subsystem field has a default**, so `DB.open` initializes the whole
  subsystem with `.{}`.
- **Methods are private unless something outside the file calls them.**
- **Engine boundary names are unchanged.** Moved methods still call
  `storage_engine.call("advanceGraphReplacement", ...)`, so the engine vtable
  and ABI are not affected.
- **Shared `db.zig` helpers are reached through one namespace import**
  (`db_mod.helper`), not per-name aliases.
- **A method stays on `DB` when it coordinates several subsystems**, such as
  `open`, `deinitWrapperState` or `startOptionalRuntimes`, and reaches into
  each subsystem's fields. When most of a method's work is one subsystem's,
  move it there.
- **Tests live with the subsystem** once its methods move.

## Field map

Each field's path within its subsystem. `DB` keeps only the core fields.

| Subsystem | DB fields |
|---|---|
| `DB` (core) | `alloc`, `runtime_alloc`, `core`, `root_incarnation`, `async_context`, `backend_runtime`, `backend_owner_id`, `open_mode`, `primary_backend`, `primary_lsm_storage`, `physical_root_mode`, `index_backends`, `generation_read_lease`, `closed`, `stable_address`, `local_execution`, `executor`, `owned_backend_runtime`, `owned_resource_manager`, `capacity_source`, `secret_store`, `remote_content`, `start_index_workers`, `start_independent_maintenance`, `resolver_workers_enabled`, `source_vector_storage`, `status_owner_epoch`, `status_publication_mutex`, `status_publication_revision` |
| `indexes` | `generation_replace_mutex`, `split_shadow` (was `shadow`) |
| `indexes.text` | `merge_owner`, `merge_runtime` |
| `indexes.sparse` | `compaction_owner`, `compaction_runtime` |
| `indexes.graph` | `replacement.mutex`, `replacement.manager`, `replacement.catalog_generation`, `replacement.progress`, `cleanup_owner`, `metrics.owner`, `metrics.runtime`, `metrics.idle_maintenance`, `metrics.idle_planned_options`, `metrics.idle_auto_options`, `metrics.idle_degree_canary_options` |
| `indexes.relational` | `rewrite_program_cache`, `rewrite_tail_cache`, `columns_building`, `columns_rebuild_requested`, `column_maintenance` |
| `indexes.algebraic` | `hll_owner_id` |
| `indexes.repair` | `clock`, `cleanup_owner_id`, `mutex`, `active`, `shadow_hook` |
| `enrichment` | `owner`, `append_context`, `runtime`, `embedding_activity`, `managed_admission`, `artifacts.repair_metadata_due_ns`, `artifacts.repair_metadata_pending`, `artifacts.footprint_pending`, `artifacts.producer_baseline_pending`, `artifacts.producer_scheduler`, `resolution.append_context`, `resolution.owner`, `resolution.runtime`, `resolution.candidate_source`, `resolution.embedder`, `promotion.owner_bundle`, `promotion.runtime`, `promotion.entity_sink`, `promotion.owner`, `promotion.entity_sink_missing_policy` |
| `documents` | `authority.backfill_active`, `authority.backfill_requested`, `authority.backfill_mutex`, `authority.backfill_restart`, `authority.repairs`, `authority.repairs_yield_sweep`, `group_sources.owner`, `group_sources.borrowed_state`, `bulk_ingest.session`, `bulk_ingest.identity` |
| `query` | `schema.identity`, `schema.resolver_mutex`, `schema.resolver_epoch`, `schema.resolver_digest`, `cut.value_scope`, `cut.lease`, `cut.remote_lease`, `warming`, `row_policy.authority_secret`, `row_policy.authority_issuer`, `row_policy.table_name` |
| `recovery` | `transactions.identity_context`, `transactions.local_context`, `transactions.owner`, `transactions.runtime`, `publication.owner`, `publication.snapshot_mutex`, `portable_activation.attempts`, `portable_activation.mutex`, `portable_activation.owner`, `restore.decoder_cache`, `restore.graph_parse_cache`, `restore.graph_parse_count_for_test`, `source_pin_cleanup` |
| `ttl` | `owner`, `cleanup_context`, `runtime` |
| `maintenance` | `quarantine`, `independent`, `local_source.completion_audit_pending`, `local_source.recovery_pending`, `run_until_idle.no_progress_timeout_ns`, `run_until_idle.last_no_progress` |

## What stays on `DB`

`DB` keeps its core fields and the methods that span subsystems: `open` and
teardown, the write and read paths, search, status, and the `runUntilIdle`
family that drives every subsystem's background work. Splitting those further
is a separate change from moving state.

Some methods stay on `DB` even though their state moved:

- The public bulk ingest entry points (`beginBulkIngestSession`,
  `finishBulkIngestSessionWithOptions`, `abortBulkIngestSession`,
  `bulkSessionActive`) share their names with index backends and the local
  mutation execution view, which callers use interchangeably. Their state is in
  `documents.bulk_ingest`; the dense and primary-store auto sessions moved.
- Search and query-time graph metric methods (`searchLockedWithExecutionContextImpl`,
  `executeGraphMetricQueries`, `applyGraphMetricRerank`) stay on `DB` until the
  search path moves into `query`.
- `runUntilIdle` and its variants, `runMaintenanceUntilWithEnrichmentCompletion`,
  and the index catalog operations that pause enrichment
  (`installIndexWhileEnrichmentQuiesced`, `deleteIndexWhileEnrichmentQuiesced`)
  coordinate several subsystems.

`AsyncContext` mirrors several runtime pointers (for example
`async_context.sparse_compaction_runtime`) for background workers that only
hold the async context. Those copies stay until each worker moves into its
subsystem.

`local_mutation.zig`'s prepared-batch view mirrors `DB` field paths so shared
code can read either owner through `ownerField`. When a moved field is read
that way, the view carries the same subsystem path (it now has an `indexes`
field with the text and sparse runtimes, the repair cleanup owner, the
relational rewrite caches, the split shadow and the active repair set; a
`documents` field with the bulk ingest identity scratch; an `enrichment` field
with the enrichment, resolution and promotion runtimes and embedding activity; a
`recovery` field with the transaction recovery contexts and restore caches; a
`ttl` field with the cleanup context; and a `maintenance` field with the last
no-progress diagnostic).
