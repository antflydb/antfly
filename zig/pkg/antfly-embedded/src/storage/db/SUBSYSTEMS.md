# DB subsystems

`DB` in `db.zig` holds about 120 fields and 1,600 members. Most fields belong
to one area, such as graph indexes, TTL cleanup or entity resolution, and only
that area's methods touch them. This document describes how `DB` is being split
into subsystem structs that own that state, and where each field goes.

## Layout

Subsystems are grouped by what they own. Index state is grouped by index kind
under `indexes`, so everything about one kind (its runtimes, background work and
replacement) lives in one place.

```
DB              core: identity, backends, runtime wiring, status
├─ indexes      catalog operations shared by all kinds; pause and restart index
│  │            runtimes around structural mutations
│  ├─ text      merge runtime
│  ├─ dense     auto bulk ingest
│  ├─ sparse    compaction runtime
│  ├─ graph     replacement, metrics, cleanup
│  ├─ relational column maintenance, rewrite caches
│  ├─ algebraic HLL owner
│  └─ repair    cross-kind repair intents, shadow repairs
├─ enrichment   enrichment, resolution and promotion runtimes, artifact scheduling
├─ documents    authority backfill and repair, group sources
├─ query        schema identity, leases, warming, row policy
├─ recovery     transactions, publication, portable activation, restore, source pins
├─ ttl          TTL cleanup runtime
└─ maintenance  quarantine, independent maintenance, run-until-idle tracking
```

Call sites read as the path to the state they act on:

```zig
_ = try db.indexes.graph.advanceReplacement();
db.indexes.sparse.restartCompaction("drop", name);
_ = try db.documents.advanceAuthorityBackfillPage();
```

## Pattern

A subsystem is a struct embedded in its parent as a field. It owns its state
and the methods that operate on it. It reaches `DB` through `@fieldParentPtr`,
so it needs no stored back-pointer and stays valid when `DB` is moved. A nested
subsystem steps up through its parent.

```zig
// indexes/mod.zig
pub const Indexes = struct {
    graph: GraphIndexes = .{},
    sparse: SparseIndexes = .{},

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

Moved fields show their new path. The rest show where they will go.

| Subsystem | DB fields |
|---|---|
| `DB` (core) | `alloc`, `runtime_alloc`, `core`, `root_incarnation`, `async_context`, `backend_runtime`, `backend_owner_id`, `open_mode`, `primary_backend`, `primary_lsm_storage`, `physical_root_mode`, `index_backends`, `generation_read_lease`, `closed`, `stable_address`, `local_execution`, `executor`, `owned_backend_runtime`, `owned_resource_manager`, `capacity_source`, `secret_store`, `remote_content`, `start_index_workers`, `start_independent_maintenance`, `resolver_workers_enabled`, `source_vector_storage`, `status_owner_epoch`, `status_publication_mutex`, `status_publication_revision` |
| `indexes` | `generation_replace_mutex` |
| `indexes.text` | `text_merge_owner`, `text_merge_runtime` |
| `indexes.dense` | `bulk_ingest_session`, `bulk_identity` |
| `indexes.sparse` (moved) | `compaction_owner`, `compaction_runtime` |
| `indexes.graph` (replacement moved) | `replacement.mutex`, `replacement.manager`, `replacement.catalog_generation`, `replacement.progress`; to move: `graph_cleanup_owner`, `graph_metric_idle_maintenance`, `graph_metric_idle_planned_options`, `graph_metric_idle_auto_options`, `graph_metric_idle_degree_canary_options`, `graph_metric_owner`, `graph_metric_runtime` |
| `indexes.relational` | `rewrite_program_cache`, `rewrite_tail_cache`, `relational_columns_building`, `relational_columns_rebuild_requested`, `relational_column_maintenance` |
| `indexes.algebraic` | `algebraic_hll_owner_id` |
| `indexes.repair` | `index_repair_clock`, `repair_cleanup_owner_id`, `index_repair_mutex`, `active_index_repairs`, `shadow_index_repair_hook`, `shadow` |
| `enrichment` | `enrichment_owner`, `enrichment_append_context`, `enrichment_runtime`, `artifact_repair_metadata_due_ns`, `artifact_repair_metadata_pending`, `artifact_footprint_pending`, `artifact_producer_baseline_pending`, `artifact_producer_scheduler`, `embedding_activity`, `managed_admission` |
| `enrichment.resolution` | `resolution_append_context`, `resolution_owner`, `resolution_runtime`, `resolution_candidate_source`, `resolution_embedder` |
| `enrichment.promotion` | `promotion_owner_bundle`, `promotion_runtime`, `entity_sink`, `promotion_owner`, `entity_sink_missing_policy` |
| `documents` (authority moved) | `authority.backfill_active`, `authority.backfill_requested`, `authority.backfill_mutex`, `authority.backfill_restart`, `authority.repairs`, `authority.repairs_yield_sweep`; to move: `group_source_owner`, `borrowed_group_source_state` |
| `query` | `query_schema_identity`, `resolver_query_schema_mutex`, `resolver_query_schema_epoch`, `resolver_query_schema_digest`, `retained_value_scope`, `retained_query_lease`, `retained_remote_query_lease`, `native_query_warming`, `owned_row_policy_authority_secret`, `owned_row_policy_authority_issuer`, `owned_row_policy_table_name` |
| `recovery` | `transaction_recovery_identity_context`, `transaction_recovery_local_context`, `transaction_owner`, `transaction_runtime`, `publication_recovery`, `snapshot_publication_mutex`, `portable_runtime_activation_attempts`, `portable_runtime_activation_mutex`, `portable_activation_recovery`, `restore_decoder_cache`, `graph_restore_parse_cache`, `graph_restore_parse_count_for_test`, `source_pin_cleanup` |
| `ttl` | `ttl_owner`, `ttl_cleanup_context`, `ttl_runtime` |
| `maintenance` | `quarantine_recovery`, `independent_maintenance`, `local_source_completion_audit_pending`, `local_source_recovery_pending`, `run_until_idle_no_progress_timeout_ns`, `last_run_until_idle_no_progress` |

## Migration order

Move self-contained areas first; they need no changes to `DB.open` beyond
dropping fields that now have defaults.

1. `indexes.graph.replacement`, `indexes.sparse` and `documents.authority`
   (done).
2. The rest of `indexes`: `text`, `graph` metrics and cleanup, `algebraic`,
   `dense`, then `relational` and `repair`, which touch the catalog more.
3. `ttl`, `maintenance` and the rest of `documents`.
4. `enrichment` with `resolution` and `promotion`, which share append contexts
   and must move together.
5. `recovery` and `query`.

`AsyncContext` mirrors several runtime pointers (for example
`async_context.sparse_compaction_runtime`) for background workers that only
hold the async context. Those copies stay until each worker moves into its
subsystem.

`local_mutation.zig`'s prepared-batch view mirrors `DB` field paths so shared
code can read either owner through `ownerField`. When a moved field is read
that way, the view carries the same subsystem path (it now has an `indexes`
field whose `sparse.compaction_runtime` is set from the async context).
