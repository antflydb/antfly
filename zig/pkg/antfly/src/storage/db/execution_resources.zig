// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Canonical local execution resources, invocation scratch and owned results.
//! Foreground DB and recovery borrow the same owners. This module imports no
//! DB wrapper or mutation pipeline, and owns no server coordination policy.

const materialized_sources = @import("materialized_sources.zig");

const document_child_range_outbox = @import("document_child_range_outbox.zig");

const std = @import("std");

const replication_contract = @import("replication_contract.zig");

const builtin = @import("builtin");

const build_options = @import("build_options");

const platform = @import("antfly_platform");

pub const Allocator = std.mem.Allocator;

pub const Io = std.Io;

pub const AtomicU64 = platform.atomic.Value(u64);

const backend_types = @import("../backend_types.zig");

const docstore_mod = @import("../docstore.zig");

const table_storage_mod = @import("../../common/table_storage.zig");

const vector_payload_store_mod = @import("../vector_payload_store.zig");

const graph_asset_state = @import("graph_asset_state.zig");

const graph_edge_contender = @import("graph_edge_contender.zig");

const apply_rw_lock_mod = @import("apply_rw_lock.zig");

const snapshot_admission_mod = @import("snapshot_admission.zig");

const db_core = @import("core.zig");

const internal_keys = @import("../internal_keys.zig");

const doc_identity = @import("doc_identity.zig");

const document_content_hash = @import("document_content_hash.zig");

const doc_set = @import("doc_set.zig");

const shard_mod = @import("../shard.zig");

const index_manager_mod = @import("catalog/index_manager.zig");

const resolution_runtime_mod = @import("resolution_runtime.zig");

const promotion_runtime_mod = @import("promotion_runtime.zig");

const types = @import("types.zig");

const index_repair_state = @import("derived/index_repair_state.zig");

const change_journal_mod = @import("derived/change_journal.zig");

const derived_types = @import("derived/derived_types.zig");

const derived_executor_mod = @import("derived/derived_executor.zig");

const replay_source_mod = @import("derived/replay_source.zig");

const embedder_mod = @import("enrichment/embedder.zig");

const asset_producer_mod = @import("enrichment/asset_producer.zig");

pub const document_extraction_client = if (!builtin.is_test and build_options.linked_storage)
    @import("enrichment/document_extraction_client.zig")
else
    struct {};

const chunker_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("enrichment/chunker_stub.zig")
else
    @import("enrichment/chunker.zig");

const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");

const enrichment_types = @import("enrichment/enrichment_types.zig");

const resource_manager_mod = @import("../resource_manager.zig");

const table_catalog_mod = @import("table_catalog.zig");

const row_policy_gate_mod = @import("row_policy_gate.zig");

const row_policy_bundle_mod = @import("row_policy_bundle.zig");

const row_policy_authority_mod = @import("../../usermgr/row_policy_authority.zig");

const schema_registry_mod = @import("schema_registry.zig");

const relational_index_plans = @import("relational_index_plan.zig");

const transactions_mod = @import("../transactions.zig");

const relational_store = @import("relational_store.zig");

const relational_row_codec = @import("algebraic/relational_row_codec.zig");

const coordinated_ttl = @import("../coordinated_ttl.zig");

const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");

const sparse_compaction_runtime_mod = @import("maintenance/sparse_compaction_runtime.zig");

const platform_clock = @import("antfly_platform").clock;

const platform_time = @import("antfly_platform").time;

const ArtifactPublicationDispatcher = @import("artifact_publication.zig").Dispatcher;

pub const ReplicationAsyncEffectMirror = replication_contract.AsyncEffectMirror;

pub const ReplicationAsyncBatchMirror = replication_contract.AsyncBatchMirror;

pub const ReplicationAsyncMetadataMirror = replication_contract.AsyncMetadataMirror;

pub const ReplicationWriteGate = replication_contract.WriteGate;

pub const DocumentArtifactChildRangeDispatcher = document_child_range_outbox.DocumentArtifactChildRangeDispatcher;

pub const CommittedBatchEffectsObserver = struct {
    ptr: *anyopaque,
    apply: *const fn (ptr: *anyopaque, replay_payload: []const u8) anyerror!void,

    pub fn observe(self: CommittedBatchEffectsObserver, replay_payload: []const u8) !void {
        return try self.apply(self.ptr, replay_payload);
    }
};

const IndexTargetVisibility = @import("query_visibility.zig").IndexTargetVisibility;

const IndexRepairSchedulerDirectory = @import("index_repair_scheduler.zig").Directory;

pub const AsyncContext = struct {
    alloc: Allocator,
    io: ?std.Io = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    store: *docstore_mod.DocStore,
    relational_base_rows: bool = false,
    snapshot_read_txn: ?*docstore_mod.DocStore.Txn = null,
    applied_sequence_checkpoint_path: ?[]const u8 = null,
    index_repair_checkpoint: ?index_repair_state.Location = null,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    // Heap-stable admission state shared by the DB wrapper and callbacks that
    // outlive DB.open's by-value return. Activation publishes this flag while
    // holding apply exclusive; every catalog-sensitive lease revalidates it
    // after admission.
    portable_runtime_activation_pending: std.atomic.Value(bool) = .init(false),
    /// Stable notification shared by TTL callbacks and the resident hot-standby owner.
    primary_replication_outbox_pending: std.atomic.Value(bool) = .init(false),
    primary_replication_append_pending: std.atomic.Value(bool) = .init(false),
    snapshot_replay_admission: ?*snapshot_admission_mod.SnapshotAdmission = null,
    repair_replay_mutex: ?*std.Io.Mutex = null,
    repair_sequence: u64 = 0,
    repair_issue_counter: ?*AtomicU64 = null,
    allow_graph_materialization: bool = true,
    require_graph_resolution_contract: bool = false,
    /// Apply only physical projection mutations. Shadow split generations read
    /// committed source rows/artifacts from the primary store but must never
    /// rewrite its coverage, repair, or graph-binding metadata.
    projection_only: bool = false,
    visibility_observer: @import("query_visibility.zig").Observer = .{},
    /// Serializes durable repair-control transitions with their process-local
    /// quarantine binding effects. The scheduler directory remains a fallible
    /// projection and is never allowed to race claim ownership decisions.
    index_repair_control_mutex: std.atomic.Mutex = .unlocked,
    index_repair_scheduler_mutex: std.atomic.Mutex = .unlocked,
    /// Avoid taking the scheduler mutex on the common derived-watermark path
    /// when no repair is waiting for index progress.
    index_repair_progress_wait_pending: std.atomic.Value(bool) = .init(false),
    index_repair_scheduler_revision: @import("antfly_platform").atomic.Value(u64) = @import("antfly_platform").atomic.Value(u64).init(0),
    index_repair_scheduler: IndexRepairSchedulerDirectory = .{},
    text_merge_deferred: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    applied_sequence_mutex: std.atomic.Mutex = .unlocked,
    dense_admission: @import("dense_publication_admission.zig").Owner = .{},
    native_projection_owner: @import("native_projection_owner.zig").Owner = .{},
    dense_bulk_session_scope: DenseBulkSessionScope = .auto,
    index_repair_replay_pinned: std.atomic.Value(bool) = .init(false),
    index_repair_state_corrupt: std.atomic.Value(bool) = .init(false),
    index_artifact_cleanup_mutex: std.atomic.Mutex = .unlocked,
    index_artifact_finalization_mutex: std.atomic.Mutex = .unlocked,
    /// Serializes repair-issue revision checks and completion with terminal
    /// enrichment coverage publication. Provider calls never run under this
    /// lock; healthy produced/skipped coverage stays off this path.
    artifact_repair_issue_mutex: std.atomic.Mutex = .unlocked,
    background_closing: std.atomic.Value(bool) = .init(false),
    enrichment_lifecycle_mutex: std.atomic.Mutex = .unlocked,
    enrichment_runtime: ?*enrichment_runtime_mod.EnrichmentRuntime = null,
    enrichment_desired_running: std.atomic.Value(bool) = .init(false),
    enrichment_restart_owner: @import("runtime_restart_owner.zig").Owner = .{},
    target_advance: @import("target_advance_tracker.zig").Tracker = .{},
    text_merge_runtime: ?*text_merge_runtime_mod.TextMergeRuntime = null,
    text_merge_restart_owner: @import("runtime_restart_owner.zig").Owner = .{},
    sparse_compaction_runtime: ?*sparse_compaction_runtime_mod.SparseCompactionRuntime = null,
    sparse_compaction_restart_owner: @import("runtime_restart_owner.zig").Owner = .{},
    resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime = null,
    promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime = null,
    repair_options: types.ArtifactRepairRunOptions = .{},
    applied_sequence_coalescer: AppliedSequenceCoalescer = .{},
    stats: AsyncContentionStats = .{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        stopNativeProjectionMaintenance(self);
        self.index_repair_scheduler.deinit(alloc);
        self.applied_sequence_coalescer.deinit(alloc);
        self.dense_admission.deinit(alloc, self.index_manager);
        self.target_advance.deinit(alloc);
    }
};

pub const DocSetPlanningRuntimeStats = struct {
    resolved_set_count: AtomicU64 = AtomicU64.init(0),
    all_set_count: AtomicU64 = AtomicU64.init(0),
    none_set_count: AtomicU64 = AtomicU64.init(0),
    doc_key_list_count: AtomicU64 = AtomicU64.init(0),
    ordinal_list_count: AtomicU64 = AtomicU64.init(0),
    ordinal_bitmap_count: AtomicU64 = AtomicU64.init(0),
    doc_key_list_docs: AtomicU64 = AtomicU64.init(0),
    ordinal_list_docs: AtomicU64 = AtomicU64.init(0),
    ordinal_bitmap_docs: AtomicU64 = AtomicU64.init(0),
    missing_ordinal_coverage_count: AtomicU64 = AtomicU64.init(0),
    bitmap_promotion_count: AtomicU64 = AtomicU64.init(0),
    unsupported_filter_shape_count: AtomicU64 = AtomicU64.init(0),
    stale_identity_generation_rejection_count: AtomicU64 = AtomicU64.init(0),

    pub fn recordResolvedSet(self: *@This(), set: *const doc_set.ResolvedDocSet, missing_ordinal_coverage: bool) void {
        _ = self.resolved_set_count.fetchAdd(1, .monotonic);
        switch (set.*) {
            .all => _ = self.all_set_count.fetchAdd(1, .monotonic),
            .none => _ = self.none_set_count.fetchAdd(1, .monotonic),
            .doc_keys => |keys| {
                _ = self.doc_key_list_count.fetchAdd(1, .monotonic);
                _ = self.doc_key_list_docs.fetchAdd(@intCast(keys.len), .monotonic);
            },
            .ordinals => |ordinals| {
                _ = self.ordinal_list_count.fetchAdd(1, .monotonic);
                _ = self.ordinal_list_docs.fetchAdd(@intCast(ordinals.len), .monotonic);
            },
            .ordinal_bitmap => |*bitmap| {
                _ = self.ordinal_bitmap_count.fetchAdd(1, .monotonic);
                _ = self.ordinal_bitmap_docs.fetchAdd(@intCast(bitmap.cardinality()), .monotonic);
                _ = self.bitmap_promotion_count.fetchAdd(1, .monotonic);
            },
        }
        if (missing_ordinal_coverage) _ = self.missing_ordinal_coverage_count.fetchAdd(1, .monotonic);
    }

    pub fn recordUnsupportedFilterShape(self: *@This()) void {
        _ = self.unsupported_filter_shape_count.fetchAdd(1, .monotonic);
    }

    pub fn recordStaleIdentityGenerationRejection(self: *@This()) void {
        _ = self.stale_identity_generation_rejection_count.fetchAdd(1, .monotonic);
    }

    pub fn snapshot(self: *@This()) types.DocSetPlanningStats {
        return .{
            .resolved_set_count = self.resolved_set_count.load(.monotonic),
            .all_set_count = self.all_set_count.load(.monotonic),
            .none_set_count = self.none_set_count.load(.monotonic),
            .doc_key_list_count = self.doc_key_list_count.load(.monotonic),
            .ordinal_list_count = self.ordinal_list_count.load(.monotonic),
            .ordinal_bitmap_count = self.ordinal_bitmap_count.load(.monotonic),
            .doc_key_list_docs = self.doc_key_list_docs.load(.monotonic),
            .ordinal_list_docs = self.ordinal_list_docs.load(.monotonic),
            .ordinal_bitmap_docs = self.ordinal_bitmap_docs.load(.monotonic),
            .missing_ordinal_coverage_count = self.missing_ordinal_coverage_count.load(.monotonic),
            .bitmap_promotion_count = self.bitmap_promotion_count.load(.monotonic),
            .unsupported_filter_shape_count = self.unsupported_filter_shape_count.load(.monotonic),
            .stale_identity_generation_rejection_count = self.stale_identity_generation_rejection_count.load(.monotonic),
        };
    }
};

pub const VisibilityRuntimeStats = struct {
    cache_hits_total: AtomicU64 = AtomicU64.init(0),
    cache_misses_total: AtomicU64 = AtomicU64.init(0),
    mask_build_ns_total: AtomicU64 = AtomicU64.init(0),
    mask_builds_total: AtomicU64 = AtomicU64.init(0),
    full_scan_fallbacks_total: AtomicU64 = AtomicU64.init(0),
    overflow_total: AtomicU64 = AtomicU64.init(0),

    pub fn recordCacheHit(self: *@This()) void {
        _ = self.cache_hits_total.fetchAdd(1, .monotonic);
    }

    pub fn recordCacheMiss(self: *@This()) void {
        _ = self.cache_misses_total.fetchAdd(1, .monotonic);
    }

    pub fn recordBuild(self: *@This(), duration_ns: u64) void {
        _ = self.mask_builds_total.fetchAdd(1, .monotonic);
        _ = self.mask_build_ns_total.fetchAdd(duration_ns, .monotonic);
    }

    pub fn recordOverflow(self: *@This()) void {
        _ = self.overflow_total.fetchAdd(1, .monotonic);
        _ = self.full_scan_fallbacks_total.fetchAdd(1, .monotonic);
    }

    pub fn snapshot(self: *@This(), cache_entries: u64) types.VisibilityStats {
        return .{
            .cache_entries = cache_entries,
            .cache_hits_total = self.cache_hits_total.load(.monotonic),
            .cache_misses_total = self.cache_misses_total.load(.monotonic),
            .mask_build_ns_total = self.mask_build_ns_total.load(.monotonic),
            .mask_builds_total = self.mask_builds_total.load(.monotonic),
            .full_scan_fallbacks_total = self.full_scan_fallbacks_total.load(.monotonic),
            .overflow_total = self.overflow_total.load(.monotonic),
        };
    }
};

pub const DenseBulkSessionScope = enum {
    auto,
    external,
};

pub const OverwriteProbeEntry = struct {
    key: []const u8,
    value: []const u8,
    write_index: usize,
    semantic_hash: ?document_content_hash.Digest = null,
};

pub var test_graph_overlay_examined: ?*usize = null;

const AppliedSequenceCoalescer = @import("applied_sequence_coalescer.zig").Coalescer;

pub const MutexContentionStats = struct {
    lock_calls: AtomicU64 = .init(0),
    contended_calls: AtomicU64 = .init(0),
    current_waiters: AtomicU64 = .init(0),
    max_waiters: AtomicU64 = .init(0),
    spin_loops: AtomicU64 = .init(0),
    yield_loops: AtomicU64 = .init(0),
    sleep_loops: AtomicU64 = .init(0),
    wait_ns: AtomicU64 = .init(0),
    max_wait_ns: AtomicU64 = .init(0),
    hold_ns: AtomicU64 = .init(0),
    max_hold_ns: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.DBMutexStats {
        return .{
            .lock_calls = self.lock_calls.load(.monotonic),
            .contended_calls = self.contended_calls.load(.monotonic),
            .max_waiters = self.max_waiters.load(.monotonic),
            .spin_loops = self.spin_loops.load(.monotonic),
            .yield_loops = self.yield_loops.load(.monotonic),
            .sleep_loops = self.sleep_loops.load(.monotonic),
            .wait_ns = self.wait_ns.load(.monotonic),
            .max_wait_ns = self.max_wait_ns.load(.monotonic),
            .hold_ns = self.hold_ns.load(.monotonic),
            .max_hold_ns = self.max_hold_ns.load(.monotonic),
        };
    }
};

pub const AppliedSequenceContentionStats = struct {
    note_calls: AtomicU64 = .init(0),
    forced_flush_calls: AtomicU64 = .init(0),
    skipped_flush_calls: AtomicU64 = .init(0),
    flush_calls: AtomicU64 = .init(0),
    flushed_indexes: AtomicU64 = .init(0),
    sync_ns: AtomicU64 = .init(0),
    posting_publish_ns: AtomicU64 = .init(0),
    projection_metadata_ns: AtomicU64 = .init(0),
    checkpoint_file_ns: AtomicU64 = .init(0),
    status_snapshot_ns: AtomicU64 = .init(0),
    save_ns: AtomicU64 = .init(0),
    flush_ns: AtomicU64 = .init(0),
    max_flush_ns: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.AppliedSequenceStats {
        return .{
            .note_calls = self.note_calls.load(.monotonic),
            .forced_flush_calls = self.forced_flush_calls.load(.monotonic),
            .skipped_flush_calls = self.skipped_flush_calls.load(.monotonic),
            .flush_calls = self.flush_calls.load(.monotonic),
            .flushed_indexes = self.flushed_indexes.load(.monotonic),
            .sync_ns = self.sync_ns.load(.monotonic),
            .posting_publish_ns = self.posting_publish_ns.load(.monotonic),
            .projection_metadata_ns = self.projection_metadata_ns.load(.monotonic),
            .checkpoint_file_ns = self.checkpoint_file_ns.load(.monotonic),
            .status_snapshot_ns = self.status_snapshot_ns.load(.monotonic),
            .save_ns = self.save_ns.load(.monotonic),
            .flush_ns = self.flush_ns.load(.monotonic),
            .max_flush_ns = self.max_flush_ns.load(.monotonic),
        };
    }
};

pub const DenseCatchUpContentionStats = struct {
    begin_calls: AtomicU64 = .init(0),
    finish_calls: AtomicU64 = .init(0),
    abort_calls: AtomicU64 = .init(0),
    active: AtomicU64 = .init(0),
    phase: std.atomic.Value(u8) = .init(@backingInt(types.DenseCatchUpStats.Phase.idle)),
    current_sequence: AtomicU64 = .init(0),
    current_target_sequence: AtomicU64 = .init(0),
    current_scanned_entries: AtomicU64 = .init(0),
    current_applied_entries: AtomicU64 = .init(0),
    replay_scan_batches: AtomicU64 = .init(0),
    replay_hint_filter_skips: AtomicU64 = .init(0),
    progress_updates: AtomicU64 = .init(0),
    bulk_finish_windows: AtomicU64 = .init(0),
    bulk_finish_split_steps: AtomicU64 = .init(0),
    bulk_finish_deferred_leaf_splits: AtomicU64 = .init(0),
    bulk_finish_current_window: AtomicU64 = .init(0),
    bulk_finish_current_window_split_steps: AtomicU64 = .init(0),
    bulk_finish_current_window_ns: AtomicU64 = .init(0),
    bulk_finish_max_window_ns: AtomicU64 = .init(0),
    finish_ns: AtomicU64 = .init(0),
    max_finish_ns: AtomicU64 = .init(0),
    finalize_ns: AtomicU64 = .init(0),
    max_finalize_ns: AtomicU64 = .init(0),
    maintenance_calls: AtomicU64 = .init(0),
    maintenance_steps: AtomicU64 = .init(0),
    maintenance_ns: AtomicU64 = .init(0),
    max_maintenance_ns: AtomicU64 = .init(0),
    manifest_writes: AtomicU64 = .init(0),
    manifest_ns: AtomicU64 = .init(0),
    write_pressure_compactions: AtomicU64 = .init(0),
    write_pressure_ns: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.DenseCatchUpStats {
        return .{
            .begin_calls = self.begin_calls.load(.monotonic),
            .finish_calls = self.finish_calls.load(.monotonic),
            .abort_calls = self.abort_calls.load(.monotonic),
            .active = self.active.load(.monotonic) != 0,
            .phase = @fromBackingInt(@intCast(self.phase.load(.monotonic))),
            .current_sequence = self.current_sequence.load(.monotonic),
            .current_target_sequence = self.current_target_sequence.load(.monotonic),
            .current_scanned_entries = self.current_scanned_entries.load(.monotonic),
            .current_applied_entries = self.current_applied_entries.load(.monotonic),
            .replay_scan_batches = self.replay_scan_batches.load(.monotonic),
            .replay_hint_filter_skips = self.replay_hint_filter_skips.load(.monotonic),
            .progress_updates = self.progress_updates.load(.monotonic),
            .bulk_finish_windows = self.bulk_finish_windows.load(.monotonic),
            .bulk_finish_split_steps = self.bulk_finish_split_steps.load(.monotonic),
            .bulk_finish_deferred_leaf_splits = self.bulk_finish_deferred_leaf_splits.load(.monotonic),
            .bulk_finish_current_window = self.bulk_finish_current_window.load(.monotonic),
            .bulk_finish_current_window_split_steps = self.bulk_finish_current_window_split_steps.load(.monotonic),
            .bulk_finish_current_window_ns = self.bulk_finish_current_window_ns.load(.monotonic),
            .bulk_finish_max_window_ns = self.bulk_finish_max_window_ns.load(.monotonic),
            .finish_ns = self.finish_ns.load(.monotonic),
            .max_finish_ns = self.max_finish_ns.load(.monotonic),
            .finalize_ns = self.finalize_ns.load(.monotonic),
            .max_finalize_ns = self.max_finalize_ns.load(.monotonic),
            .maintenance_calls = self.maintenance_calls.load(.monotonic),
            .maintenance_steps = self.maintenance_steps.load(.monotonic),
            .maintenance_ns = self.maintenance_ns.load(.monotonic),
            .max_maintenance_ns = self.max_maintenance_ns.load(.monotonic),
            .manifest_writes = self.manifest_writes.load(.monotonic),
            .manifest_ns = self.manifest_ns.load(.monotonic),
            .write_pressure_compactions = self.write_pressure_compactions.load(.monotonic),
            .write_pressure_ns = self.write_pressure_ns.load(.monotonic),
        };
    }
};

pub const StartupOpenStats = struct {
    wal_retention_known: std.atomic.Value(bool) = .init(false),
    wal_retained_segments: AtomicU64 = .init(0),
    wal_retained_bytes: AtomicU64 = .init(0),
    wal_checkpoint_oldest_retained_segment: AtomicU64 = .init(0),
    wal_checkpoint_covered_through_segment: AtomicU64 = .init(0),
    wal_checkpoint_current_segment: AtomicU64 = .init(0),
    wal_checkpoint_lag_segments: AtomicU64 = .init(0),
    wal_replay_retained_segments: AtomicU64 = .init(0),
    wal_replay_retained_bytes: AtomicU64 = .init(0),
    wal_replay_current_segment: AtomicU64 = .init(0),
    configured_indexes: std.atomic.Value(u32) = .init(0),
    configured_dense_indexes: std.atomic.Value(u32) = .init(0),
    configured_sparse_indexes: std.atomic.Value(u32) = .init(0),
    configured_full_text_indexes: std.atomic.Value(u32) = .init(0),
    configured_graph_indexes: std.atomic.Value(u32) = .init(0),
    opened_indexes: std.atomic.Value(u32) = .init(0),
    db_open_ns: AtomicU64 = .init(0),
    load_indexes_ns: AtomicU64 = .init(0),
    lsm_open_stores: AtomicU64 = .init(0),
    lsm_open_completed: AtomicU64 = .init(0),
    lsm_open_failed: AtomicU64 = .init(0),
    lsm_open_total_ns: AtomicU64 = .init(0),
    lsm_open_initializing_storage_ns: AtomicU64 = .init(0),
    lsm_open_recovered_temp_cleanup_ns: AtomicU64 = .init(0),
    lsm_open_manifest_ns: AtomicU64 = .init(0),
    lsm_open_ensuring_dirs_ns: AtomicU64 = .init(0),
    lsm_open_wal_replay_ns: AtomicU64 = .init(0),
    lsm_open_mounting_runs_ns: AtomicU64 = .init(0),
    lsm_open_loaded_runs: AtomicU64 = .init(0),
    lsm_open_obsolete_paths: AtomicU64 = .init(0),
    lsm_open_mutable_entries_after_replay: AtomicU64 = .init(0),
    lsm_open_immutable_memtables_after_replay: AtomicU64 = .init(0),
    lsm_open_recovered_temp_files_deleted: AtomicU64 = .init(0),
    lsm_open_recovered_temp_bytes_deleted: AtomicU64 = .init(0),
    wal_replay_records: AtomicU64 = .init(0),
    wal_replay_entries: AtomicU64 = .init(0),
    wal_replay_bytes: AtomicU64 = .init(0),
    wal_replay_ns: AtomicU64 = .init(0),
    wal_replay_truncated_tail_bytes: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.StartupCatchUpStats {
        return .{
            .wal_retention_known = self.wal_retention_known.load(.monotonic),
            .wal_retained_segments = self.wal_retained_segments.load(.monotonic),
            .wal_retained_bytes = self.wal_retained_bytes.load(.monotonic),
            .wal_checkpoint_oldest_retained_segment = self.wal_checkpoint_oldest_retained_segment.load(.monotonic),
            .wal_checkpoint_covered_through_segment = self.wal_checkpoint_covered_through_segment.load(.monotonic),
            .wal_checkpoint_current_segment = self.wal_checkpoint_current_segment.load(.monotonic),
            .wal_checkpoint_lag_segments = self.wal_checkpoint_lag_segments.load(.monotonic),
            .wal_replay_retained_segments = self.wal_replay_retained_segments.load(.monotonic),
            .wal_replay_retained_bytes = self.wal_replay_retained_bytes.load(.monotonic),
            .wal_replay_current_segment = self.wal_replay_current_segment.load(.monotonic),
            .configured_indexes = self.configured_indexes.load(.monotonic),
            .configured_dense_indexes = self.configured_dense_indexes.load(.monotonic),
            .configured_sparse_indexes = self.configured_sparse_indexes.load(.monotonic),
            .configured_full_text_indexes = self.configured_full_text_indexes.load(.monotonic),
            .configured_graph_indexes = self.configured_graph_indexes.load(.monotonic),
            .opened_indexes = self.opened_indexes.load(.monotonic),
            .db_open_ns = self.db_open_ns.load(.monotonic),
            .load_indexes_ns = self.load_indexes_ns.load(.monotonic),
            .lsm_open_stores = self.lsm_open_stores.load(.monotonic),
            .lsm_open_completed = self.lsm_open_completed.load(.monotonic),
            .lsm_open_failed = self.lsm_open_failed.load(.monotonic),
            .lsm_open_total_ns = self.lsm_open_total_ns.load(.monotonic),
            .lsm_open_initializing_storage_ns = self.lsm_open_initializing_storage_ns.load(.monotonic),
            .lsm_open_recovered_temp_cleanup_ns = self.lsm_open_recovered_temp_cleanup_ns.load(.monotonic),
            .lsm_open_manifest_ns = self.lsm_open_manifest_ns.load(.monotonic),
            .lsm_open_ensuring_dirs_ns = self.lsm_open_ensuring_dirs_ns.load(.monotonic),
            .lsm_open_wal_replay_ns = self.lsm_open_wal_replay_ns.load(.monotonic),
            .lsm_open_mounting_runs_ns = self.lsm_open_mounting_runs_ns.load(.monotonic),
            .lsm_open_loaded_runs = self.lsm_open_loaded_runs.load(.monotonic),
            .lsm_open_obsolete_paths = self.lsm_open_obsolete_paths.load(.monotonic),
            .lsm_open_mutable_entries_after_replay = self.lsm_open_mutable_entries_after_replay.load(.monotonic),
            .lsm_open_immutable_memtables_after_replay = self.lsm_open_immutable_memtables_after_replay.load(.monotonic),
            .lsm_open_recovered_temp_files_deleted = self.lsm_open_recovered_temp_files_deleted.load(.monotonic),
            .lsm_open_recovered_temp_bytes_deleted = self.lsm_open_recovered_temp_bytes_deleted.load(.monotonic),
            .wal_replay_records = self.wal_replay_records.load(.monotonic),
            .wal_replay_entries = self.wal_replay_entries.load(.monotonic),
            .wal_replay_bytes = self.wal_replay_bytes.load(.monotonic),
            .wal_replay_ns = self.wal_replay_ns.load(.monotonic),
            .wal_replay_truncated_tail_bytes = self.wal_replay_truncated_tail_bytes.load(.monotonic),
        };
    }
};

pub const AsyncContentionStats = struct {
    apply_mutex: MutexContentionStats = .{},
    applied_sequence_mutex: MutexContentionStats = .{},
    dense_finish_mutex: MutexContentionStats = .{},
    applied_sequence: AppliedSequenceContentionStats = .{},
    startup: StartupOpenStats = .{},
    dense_catch_up: DenseCatchUpContentionStats = .{},

    pub fn snapshot(self: *const @This()) types.AsyncIndexingStats {
        return .{
            .apply_mutex = self.apply_mutex.snapshot(),
            .applied_sequence_mutex = self.applied_sequence_mutex.snapshot(),
            .dense_finish_mutex = self.dense_finish_mutex.snapshot(),
            .applied_sequence = self.applied_sequence.snapshot(),
            .startup = self.startup.snapshot(),
            .dense_catch_up = self.dense_catch_up.snapshot(),
        };
    }
};

pub const ProfiledLock = struct {
    mutex: *std.atomic.Mutex,
    stats: *MutexContentionStats,
    hold_start_ns: u64,
    profiled: bool,

    pub fn unlock(self: *@This()) void {
        if (!self.profiled) {
            self.mutex.unlock();
            self.* = undefined;
            return;
        }
        const hold_ns = elapsedSince(self.hold_start_ns);
        self.mutex.unlock();
        _ = self.stats.hold_ns.fetchAdd(hold_ns, .monotonic);
        atomicMaxU64(&self.stats.max_hold_ns, hold_ns);
        self.* = undefined;
    }
};

pub const BatchExecutionContext = struct {
    alloc: Allocator,
    io: ?std.Io = null,
    store: *docstore_mod.DocStore,
    applied_sequence_checkpoint_path: ?[]const u8,
    index_repair_checkpoint: ?index_repair_state.Location = null,
    shard_manager: *shard_mod.ShardManager,
    change_journal: *change_journal_mod.Journal,
    replay_source: replay_source_mod.Source,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    portable_runtime_activation_pending: ?*const std.atomic.Value(bool) = null,
    snapshot_admission: ?*snapshot_admission_mod.SnapshotAdmission = null,
    snapshot_replay_admission: ?*snapshot_admission_mod.SnapshotAdmission = null,
    repair_replay_mutex: ?*std.Io.Mutex = null,
    log_mutex: *std.atomic.Mutex,
    identity_namespace: doc_identity.Namespace,
    root_generation: u64 = 0,
    artifact_cleanup_maybe: ?*std.atomic.Value(bool) = null,
    executor: *derived_executor_mod.Executor,
    enrichment_runtime: ?*enrichment_runtime_mod.EnrichmentRuntime,
    resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime = null,
    promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime = null,
    async_context: ?*AsyncContext = null,
    relational_base_rows: bool = false,
    table_catalog: ?*table_catalog_mod.Catalog = null,
    dense_bulk_session_scope: DenseBulkSessionScope = .auto,
    replication_async_effect_mirror: ?ReplicationAsyncEffectMirror = null,
    replication_async_batch_mirror: ?ReplicationAsyncBatchMirror = null,
    replication_async_metadata_mirror: ?ReplicationAsyncMetadataMirror = null,
    replication_write_gate: ?ReplicationWriteGate = null,
    identity_visibility: ?*db_core.IdentityVisibilityState = null,
};

pub const TtlCleanupContext = struct {
    batch: BatchExecutionContext,
    grace_period_ns: u64,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
    schema_registry: ?*schema_registry_mod.Registry = null,
    hook_mutex: Io.Mutex = .init,
    coordinated_port: ?coordinated_ttl.Port = null,
};

pub const ManagedSyncTargets = struct {
    full_text_indexes: []const []const u8 = &.{},
    all_indexes: []const []const u8 = &.{},
    // Visibility targets are broader than synchronous wait targets: a record
    // with a missing dependency, or source work that schedules a generated
    // downstream artifact, still advances that index's convergence target.
    // Each identity owns its name so target collection does not need a second
    // parallel allocation solely to manage name lifetime.
    target_identities: []const IndexTargetVisibility = &.{},
    target_scope_known: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.full_text_indexes) |name| alloc.free(@constCast(name));
        if (self.full_text_indexes.len > 0) alloc.free(self.full_text_indexes);
        for (self.all_indexes) |name| alloc.free(@constCast(name));
        if (self.all_indexes.len > 0) alloc.free(self.all_indexes);
        for (self.target_identities) |identity| alloc.free(@constCast(identity.index_name));
        if (self.target_identities.len > 0) alloc.free(self.target_identities);
        self.* = undefined;
    }
};

pub const ManagedIndexCandidate = struct {
    ref: index_manager_mod.ManagedIndexRef,
    config: *const types.IndexConfig,
    consumes_generated_enrichment: bool,
};

pub const BatchProfile = struct {
    source_sequence: u64 = 0,
    total_ns: u64 = 0,
    apply_lock_wait_ns: u64 = 0,
    apply_lock_held_ns: u64 = 0,
    resolve_transforms_ns: u64 = 0,
    merge_effective_req_ns: u64 = 0,
    predicates_ns: u64 = 0,
    validate_range_ns: u64 = 0,
    extract_writes_ns: u64 = 0,
    relational_prepare_ns: u64 = 0,
    relational_rows_prepared: u64 = 0,
    relational_logical_bytes: u64 = 0,
    relational_encoded_bytes: u64 = 0,
    extract_vector_field_names_ns: u64 = 0,
    extract_mapper_ns: u64 = 0,
    extract_graph_fields_ns: u64 = 0,
    extract_index_field_embeddings_ns: u64 = 0,
    extract_embedding_artifacts_ns: u64 = 0,
    extract_graph_artifacts_ns: u64 = 0,
    extract_strip_store_value_ns: u64 = 0,
    extract_timestamp_ns: u64 = 0,
    overwrite_probe_ns: u64 = 0,
    delete_artifacts_ns: u64 = 0,
    precompute_generated_ns: u64 = 0,
    identity_capacity_check_ns: u64 = 0,
    identity_metadata_ns: u64 = 0,
    identity_metadata_writes: u64 = 0,
    identity_upsert_keys: u64 = 0,
    identity_delete_keys: u64 = 0,
    store_write_ns: u64 = 0,
    store_write_count: u64 = 0,
    store_delete_count: u64 = 0,
    split_delta_ns: u64 = 0,
    build_derived_ns: u64 = 0,
    apply_shadow_ns: u64 = 0,
    collect_sync_targets_ns: u64 = 0,
    backlog_admission_ns: u64 = 0,
    append_replay_journal_ns: u64 = 0,
    wait_sync_ns: u64 = 0,
    backlog_pressure_ns: u64 = 0,
    executor_notify_ns: u64 = 0,
    derived_apply_ns: u64 = 0,
    sync_wait_ns: u64 = 0,
    full_text_apply_ns: u64 = 0,
    full_text_prepare_ns: u64 = 0,
    full_text_apply_lock_wait_ns: u64 = 0,
    full_text_apply_lock_held_ns: u64 = 0,
    dense_apply_ns: u64 = 0,
    dense_delete_ns: u64 = 0,
    dense_doc_index_ns: u64 = 0,
    dense_embedding_apply_ns: u64 = 0,
    sparse_apply_ns: u64 = 0,
    graph_apply_ns: u64 = 0,
    index_sync_ns: u64 = 0,
    applied_sequence_save_ns: u64 = 0,
    replay_journal_truncate_ns: u64 = 0,
    notify_enrichment_ns: u64 = 0,
    hbc_insert_calls: u64 = 0,
    hbc_batch_route_calls: u64 = 0,
    hbc_batch_route_internal_nodes: u64 = 0,
    hbc_batch_route_leaf_groups: u64 = 0,
    hbc_batch_route_items: u64 = 0,
    hbc_batch_route_quantized_nodes: u64 = 0,
    hbc_batch_route_exact_child_scores: u64 = 0,
    hbc_batch_route_fallback_nodes: u64 = 0,
    hbc_grouped_items: u64 = 0,
    hbc_grouped_fallback_items: u64 = 0,
    hbc_grouped_leaf_groups: u64 = 0,
    hbc_grouped_split_candidates: u64 = 0,
    hbc_grouped_recursive_splits: u64 = 0,
    hbc_grouped_split_scan_iterations: u64 = 0,
    hbc_grouped_split_queue_peak_total: u64 = 0,
    hbc_grouped_leaf_range_writes: u64 = 0,
    hbc_grouped_ancestor_range_refreshes: u64 = 0,
    hbc_grouped_ancestor_range_nodes: u64 = 0,
    hbc_grouped_node_body_writes: u64 = 0,
    hbc_grouped_vec_leaf_writes: u64 = 0,
    hbc_split_leaf_input_members_total: u64 = 0,
    hbc_split_leaf_input_overflow_members_total: u64 = 0,
    hbc_save_node_calls: u64 = 0,
    hbc_split_leaf_calls: u64 = 0,
    hbc_split_internal_calls: u64 = 0,
    hbc_range_put_calls: u64 = 0,
    hbc_range_delete_calls: u64 = 0,
    hbc_nodes_put_calls: u64 = 0,
    hbc_nodes_append_calls: u64 = 0,
    hbc_nodes_delete_calls: u64 = 0,
    hbc_meta_put_calls: u64 = 0,
    hbc_meta_append_calls: u64 = 0,
    hbc_meta_delete_calls: u64 = 0,
    hbc_quant_put_calls: u64 = 0,
    hbc_quant_append_calls: u64 = 0,
    hbc_quant_delete_calls: u64 = 0,
    hbc_vecs_put_calls: u64 = 0,
    hbc_vecs_append_calls: u64 = 0,
    hbc_vecs_delete_calls: u64 = 0,
    hbc_insert_transform_ns: u64 = 0,
    hbc_insert_store_vector_ns: u64 = 0,
    hbc_insert_find_leaf_ns: u64 = 0,
    hbc_insert_mutate_leaf_ns: u64 = 0,
    hbc_insert_flush_metadata_ns: u64 = 0,
    hbc_insert_commit_ns: u64 = 0,
    hbc_save_node_ns: u64 = 0,
    hbc_save_split_range_ns: u64 = 0,
    hbc_update_parent_ns: u64 = 0,
    hbc_split_leaf_ns: u64 = 0,
    hbc_split_leaf_vector_load_ns: u64 = 0,
    hbc_split_leaf_partition_ns: u64 = 0,
    hbc_split_leaf_finalize_ns: u64 = 0,
    hbc_split_internal_ns: u64 = 0,
    hbc_refresh_quantized_ns: u64 = 0,
    hbc_quantized_vector_load_ns: u64 = 0,
    hbc_quantized_compute_ns: u64 = 0,
    hbc_quantized_store_ns: u64 = 0,
    hbc_quantized_encode_ns: u64 = 0,
    hbc_quantized_put_ns: u64 = 0,
    hbc_bulk_build_store_ns: u64 = 0,
    hbc_bulk_build_tree_ns: u64 = 0,
    hbc_posting_maintenance_scanned_nodes: u64 = 0,
    hbc_posting_maintenance_scanned_postings: u64 = 0,
    hbc_posting_maintenance_dirty_postings: u64 = 0,
    hbc_posting_maintenance_repaired_postings: u64 = 0,
    hbc_posting_maintenance_centroid_refreshed: u64 = 0,
    hbc_posting_maintenance_payload_refreshed: u64 = 0,
    hbc_posting_maintenance_ancestor_refresh_roots: u64 = 0,
    hbc_posting_maintenance_split_postings: u64 = 0,
    hbc_posting_maintenance_merged_postings: u64 = 0,
    hbc_posting_maintenance_boundary_reassigned_vectors: u64 = 0,
    hbc_posting_lazy_centroid_deferrals: u64 = 0,
    hbc_posting_lazy_payload_deferrals: u64 = 0,
    hbc_posting_lazy_ancestor_deferrals: u64 = 0,
};

pub const PreparedRowAllocator = struct {
    child: Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,

    pub fn allocator(self: *@This()) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    pub fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    pub fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.child.rawResize(memory, alignment, new_len, ret_addr);
    }

    pub fn remapFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.child.rawRemap(memory, alignment, new_len, ret_addr);
    }

    pub fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

pub const RequestPreparationContext = struct {
    budget: ?resource_manager_mod.BudgetedAllocator,
    guard: PreparedRowAllocator,

    pub fn init(self: *@This(), db: anytype) void {
        self.budget = if (db.core.index_manager.resource_manager) |manager|
            resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, db.alloc, 1)
        else
            null;
        self.guard = .{
            .child = if (self.budget) |*tracked| tracked.allocator() else db.alloc,
            .io = db.backend_runtime.io() orelse std.Options.debug_io,
        };
    }

    pub fn mapError(self: *@This(), err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.budget) |*tracked|
            if (tracked.denied()) return error.ResourceBudgetExceeded;
        return err;
    }

    pub fn deinit(self: *@This()) void {
        if (self.budget) |*tracked| {
            std.debug.assert(tracked.live_bytes == 0);
            tracked.deinit();
        }
        self.* = undefined;
    }
};

pub const PreparedRowEffects = struct {
    embedding_writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    graph_writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    graph_clears: std.ArrayListUnmanaged(GraphArtifactClear) = .empty,
    store_key: ?[]u8 = null,
    timestamp_key: ?[]u8 = null,
    timestamp_value: ?[]u8 = null,
};

pub const RelationalPriorMembership = struct {
    const Source = @import("relational_index_predicate.zig").Source;
    alloc: Allocator,
    members: []bool,
    sources: []?Source,
    historical: ?schema_registry_mod.SchemaView = null,
    scratch: std.ArrayList(u8) = .empty,

    pub fn init(alloc: Allocator, plan: ?relational_index_plans.View) !@This() {
        const count = if (plan) |view| blk: {
            for (view.boundIndexes()) |index| if (index.predicate != null) break :blk view.boundIndexes().len;
            break :blk 0;
        } else 0;
        const members = try alloc.alloc(bool, count);
        errdefer alloc.free(members);
        const sources = try alloc.alloc(?Source, count);
        @memset(sources, null);
        return .{ .alloc = alloc, .members = members, .sources = sources };
    }

    pub fn clearHistorical(self: *@This()) void {
        for (self.sources) |*source| {
            if (source.*) |*projection| projection.deinit();
            source.* = null;
        }
        if (self.historical) |*view| view.release();
        self.historical = null;
    }

    pub fn deinit(self: *@This()) void {
        self.clearHistorical();
        self.scratch.deinit(self.alloc);
        self.alloc.free(self.sources);
        self.alloc.free(self.members);
    }

    pub fn read(self: *@This(), db: anytype, txn: *docstore_mod.DocStore.Txn, plan: relational_index_plans.View, ready: []const bool, document: []const u8) ![]const bool {
        if (self.members.len == 0) return self.members;
        @memset(self.members, false);
        const needed = for (plan.boundIndexes(), ready) |index, is_ready| {
            if (index.predicate != null and is_ready) break true;
        } else false;
        if (!needed) return self.members;
        const key = try encodeStoreLookupKeyWithPinnedSchemaAlloc(db, self.alloc, document, plan.schemaView().*);
        defer self.alloc.free(key);
        const bytes = txn.get(key) catch |err| switch (err) {
            error.NotFound => return self.members,
            else => return err,
        };
        const version = try relational_store.rowSchemaVersion(bytes);
        const current = plan.schemaView();
        const view = if (version == current.version()) current else blk: {
            if (self.historical == null or self.historical.?.version() != version) {
                self.clearHistorical();
                self.historical = (try db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            }
            break :blk &self.historical.?;
        };
        const row = if (db.core.store.valuesAreAuthenticated())
            try relational_row_codec.ordinalRowViewTrusted(bytes, view.tableSchema().*, view.physicalLayout())
        else
            try relational_row_codec.ordinalRowView(bytes, view.tableSchema().*, view.physicalLayout());
        for (plan.boundIndexes(), ready, self.members, self.sources) |index, is_ready, *member, *source| {
            if (!is_ready) continue;
            if (index.predicate) |condition| {
                if (version == current.version()) {
                    member.* = try condition.active.matches(self.alloc, &self.scratch, row);
                } else {
                    if (source.* == null) source.* = try condition.projectSource(self.alloc, view.tableSchema().*, view.physicalLayout());
                    member.* = try source.*.?.matches(self.alloc, &self.scratch, row);
                }
            }
        }
        return self.members;
    }
};

pub const BatchExecutionOptions = struct {
    row_policy_principal: ?*const row_policy_authority_mod.Payload = null,
    row_policy_lease: ?*const row_policy_gate_mod.Gate.Lease = null,
    restore_staging: ?@import("restore_staging.zig").BatchAdmission = null,
    restore_artifacts: []const @import("restore_staging.zig").Artifact = &.{},
    restore_timestamps: ?*const std.StringHashMapUnmanaged(u64) = null,
    preserve_logical_values: bool = false,
    restore_replication_request: ?types.BatchRequest = null,
    /// Borrowed upload identity. Finalization retires staged bytes in the
    /// same transaction as either accepted effects or a durable rejection.
    artifact_upload_finalize: ?@import("artifact_publication_transport.zig").Finalization = null,
    validate_range_ownership: bool = true,
    store_batch_options: backend_types.BatchOptions = .{},
    snapshot_mutation: ?*const snapshot_admission_mod.SnapshotAdmission.MutationLease = null,
    wait_for_sync_level: bool = true,
    force_generated_artifact_names: []const []const u8 = &.{},
    document_child_range_dispatcher: ?DocumentArtifactChildRangeDispatcher = null,
    committed_batch_effects_observer: ?CommittedBatchEffectsObserver = null,
    bypass_replication_write_gate: bool = false,
    replication_applied_lsn_marker: ?u64 = null,
    online_source_applied_index: ?u64 = null,
    ordered_apply_receipt: ?OrderedApplyReceipt = null,
    /// Metadata-authorized native hidden-child receipt identity. Never
    /// writes the data-Raft watermark into a native source-authority root.
    native_initial_child_entry: ?OrderedApplyReceipt = null,
    native_fk_generation_entry: ?OrderedApplyReceipt = null,
    native_topology_position: ?@import("receipt_position.zig").Native = null,
    suppress_derived_replay_append: bool = false,
    extra_store_writes: []const docstore_mod.KVPair = &.{},
    extra_store_deletes: []const []const u8 = &.{},
    merge_chunk_expected_progress: ?[32]u8 = null,
    transaction_resolution: ?TransactionResolution = null,
    durable_rows: ?*const std.StringHashMapUnmanaged([]const u8) = null,
    /// Borrowed by this synchronous call and consumed only after primary
    /// durability, while waiting for requested derived visibility.
    visibility_cancellation: types.CancellationToken = .none,
};

pub const OrderedApplyReceipt = types.OrderedApplyReceipt;

pub const TransactionResolution = struct {
    txn_id: transactions_mod.TxnId,
    status: transactions_mod.TxnStatus,
    commit_version: u64,
    expected_intent_revision: u64,
    intent_keys: []const []const u8,
    resolved_participant: ?[]const u8 = null,
    schema_binding: ?transactions_mod.SchemaBinding = null,
    schema_namespace_generation: ?u64 = null,
};

pub fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

pub fn elapsedSince(start_ns: u64) u64 {
    return monotonicTimeNs() - start_ns;
}

pub fn atomicMaxU64(value: *AtomicU64, candidate: u64) void {
    var current = value.load(.monotonic);
    while (candidate > current) {
        current = value.cmpxchgWeak(current, candidate, .monotonic, .monotonic) orelse return;
    }
}

pub const ShadowState = struct {
    manager: *index_manager_mod.IndexManager,
    base_path: []u8,
    indexes_path: []u8,
    range_start: []u8,
    range_end: []u8,
    /// Primary commits reserve tickets while holding the global apply fence,
    /// then apply their derived effects here after releasing it. The ticket
    /// order preserves replay order without making shadow index I/O part of the
    /// serialized primary transaction.
    apply_mutex: std.Io.Mutex = .init,
    apply_advanced: std.Io.Condition = .init,
    next_ticket: u64 = 0,
    applied_ticket: u64 = 0,
    repair_required: bool = false,
};

pub const LocalExecutionState = struct {
    table_storage: table_storage_mod.Settings = .{},
    vector_migration_active: std.atomic.Value(bool) = .init(false),
    vector_migration_offline_candidate: bool = false,
    artifact_publication_dispatcher: ?ArtifactPublicationDispatcher = null,
    /// Planning and visibility observations have one shared owner.
    doc_set_planning_stats: DocSetPlanningRuntimeStats = .{},
    visibility_runtime_stats: VisibilityRuntimeStats = .{},
    online_merge_reader: @import("online_merge_io.zig").Cache = .{},
    row_policy_gate: row_policy_gate_mod.Gate = .{},
    row_policy_bundle: ?row_policy_bundle_mod.Installed = null,
    /// Borrowed from the opaque owner handle; Lite leaves these unset and
    /// therefore cannot turn a raw local read into an authenticated one.
    row_policy_authority_secret: ?[]const u8 = null,
    row_policy_authority_issuer: ?[]const u8 = null,
    row_policy_table_name: ?[]const u8 = null,
    // Preparation pins this immutable map independently of the apply lock.
    // No borrowed store buffers or provider work survives the prepare boundary.
    merge_artifact_layout_mutex: std.Io.Mutex = .init,
    merge_artifact_layout: @import("merge_artifact_catalog.zig").Cache = .{},
    source_publication: @import("source_publication_job.zig").Job = .{},
    restore_staging_required: std.atomic.Value(bool) = .init(false),
    initial_child_hidden: std.atomic.Value(bool) = .init(false),
    initial_child_bootstrap: ?@import("relational_initial_child_publication.zig").Bootstrap = null,
    vector_migration_reopen_required: std.atomic.Value(bool) = .init(false),
    graph_merge_import_recovery_pending: std.atomic.Value(bool) = .init(false),
    source_vectors: std.atomic.Value(?*vector_payload_store_mod.Store) = .init(null),
    replication_async_effect_mirror: ?ReplicationAsyncEffectMirror = null,
    replication_async_batch_mirror: ?ReplicationAsyncBatchMirror = null,
    replication_async_metadata_mirror: ?ReplicationAsyncMetadataMirror = null,
    replication_write_gate: ?ReplicationWriteGate = null,
    /// Fast negative cache for crash-recovery scans. Writers publish `true`
    /// under apply before committing an outbox; only an apply-fenced empty scan
    /// may return it to false.
    durable_replication_outbox_maybe: std.atomic.Value(bool) = .init(true),
    /// A committed policy publication cannot be followed by row mutations in
    /// the hot-standby tail until its metadata record has been appended. The ordinary
    /// startup barrier also serves as this transient publication barrier.
    row_policy_replication_outbox_pending: std.atomic.Value(bool) = .init(false),
    durable_replication_flush_mutex: std.Io.Mutex = .init,
    /// Crash-left records must be replayed before this process publishes its
    /// first hot-standby mutation. Once crossed, foreground commits append under the
    /// apply/log ordering fences and may await independent LSNs concurrently.
    durable_replication_startup_barrier_pending: std.atomic.Value(bool) = .init(true),
    /// 0 = idle, 1 = queued/running, 2 = rerun requested. The owner-scoped
    /// durable lane is drained before DB teardown, so jobs may safely borrow
    /// this DB while keeping schema publication latency independent of index
    /// reconciliation cost.
    schema_reconcile: @import("schema_reconcile_owner.zig").Owner = .{},
    relational_index_maintenance_cursor: std.atomic.Value(usize) = .init(0),
    relational_index_maintenance_sweep: @import("relational_index_maintenance_sweep.zig").Sweep = .{},
    relational_index_retry_after_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),
    index_structural_mutation_mutex: std.atomic.Mutex = .unlocked,
    optional_runtime_workers_enabled: bool = false,
};

pub fn encodeStoreLookupKeyWithPinnedSchemaAlloc(
    self: anytype,
    alloc: Allocator,
    key: []const u8,
    schema_view: ?schema_registry_mod.SchemaView,
) ![]u8 {
    if (self.core.store.portableImportPublicationInProgress())
        return error.PortableImportPublicationInProgress;
    if (internal_keys.isInternalUserKey(key) or std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or isSplitMetadataKey(key))
        return try alloc.dupe(u8, key);
    if (schema_view) |view| {
        if (view.storageMode() == .relational) return try relational_store.keyAlloc(alloc, key);
    }
    return try internal_keys.documentKeyAlloc(alloc, key);
}

pub const NeighborContextReplayHints = struct {
    keys: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.keys) |key| alloc.free(key);
        alloc.free(self.keys);
    }
};

pub const PrecomputeAssetProducerBatchItem = struct {
    request: enrichment_types.GeneratedEnrichmentRequest,
    producer_type: asset_producer_mod.ProducerType,
    config_json: []u8,
    source_text: []u8,
    source_parts_json: ?[]u8 = null,
    artifact_key: []u8,
    state_key: []u8,
    state_value: []u8,

    pub fn asRequest(self: *const @This()) asset_producer_mod.Request {
        return .{
            .producer_type = self.producer_type,
            .config_json = self.config_json,
            .source_text = self.source_text,
            .source_parts_json = self.source_parts_json,
            .content_type = self.request.content_type,
        };
    }
};

pub const DocumentExtractionEmbeddingView = struct {
    name: []const u8 = &.{},
    source_field: []const u8 = &.{},
    expected_dims: u32 = 0,
    producer_json: []const u8 = &.{},
    consumer_indexes: [][]u8 = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.name.len > 0) alloc.free(@constCast(self.name));
        if (self.source_field.len > 0) alloc.free(@constCast(self.source_field));
        if (self.producer_json.len > 0) alloc.free(@constCast(self.producer_json));
        for (self.consumer_indexes) |name| alloc.free(name);
        if (self.consumer_indexes.len > 0) alloc.free(self.consumer_indexes);
        self.* = undefined;
    }

    /// Takes ownership of consumer_indexes on success; the caller owns them on failure.
    pub fn cloneFromEnrichment(alloc: Allocator, entry: anytype, consumer_indexes: [][]u8) !DocumentExtractionEmbeddingView {
        const name = try alloc.dupe(u8, entry.name);
        errdefer alloc.free(name);
        const source_field = try alloc.dupe(u8, entry.source_field);
        errdefer alloc.free(source_field);
        const producer_json = if (entry.producer_json.len > 0) try alloc.dupe(u8, entry.producer_json) else "";
        errdefer if (producer_json.len > 0) alloc.free(@constCast(producer_json));
        return .{
            .name = name,
            .source_field = source_field,
            .expected_dims = entry.expected_dims,
            .producer_json = producer_json,
            .consumer_indexes = consumer_indexes,
        };
    }
};

pub const DocumentExtractionChunkView = struct {
    name: []const u8 = &.{},
    source_field: []const u8 = &.{},
    chunker_json: []const u8 = &.{},
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    text_indexes: [][]u8 = &.{},
    dense_embeddings: []DocumentExtractionEmbeddingView = &.{},
    sparse_embeddings: []DocumentExtractionEmbeddingView = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.name.len > 0) alloc.free(@constCast(self.name));
        if (self.source_field.len > 0) alloc.free(@constCast(self.source_field));
        if (self.chunker_json.len > 0) alloc.free(@constCast(self.chunker_json));
        for (self.text_indexes) |name| alloc.free(name);
        if (self.text_indexes.len > 0) alloc.free(self.text_indexes);
        for (self.dense_embeddings) |*embedding| embedding.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.sparse_embeddings) |*embedding| embedding.deinit(alloc);
        if (self.sparse_embeddings.len > 0) alloc.free(self.sparse_embeddings);
        self.* = undefined;
    }
};

pub const DocumentExtractionCatalogView = struct {
    chunks: []DocumentExtractionChunkView = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.chunks) |*chunk| chunk.deinit(alloc);
        if (self.chunks.len > 0) alloc.free(self.chunks);
        self.* = undefined;
    }
};

pub const DocumentExtractionUnitDescriptor = struct {
    key: []const u8,
    fingerprint: []const u8,
};

pub const DocumentExtractionPreviousState = struct {
    unit_keys: []const []const u8 = &.{},
    unit_descriptors: []DocumentExtractionUnitDescriptor = &.{},
    chunk_keys: []const []const u8 = &.{},
    navigation_block_count: u32 = 0,
    recovered_from_store_scan: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        freeOwnedConstKeySlice(alloc, self.unit_keys);
        freeDocumentExtractionUnitDescriptors(alloc, self.unit_descriptors);
        freeOwnedConstKeySlice(alloc, self.chunk_keys);
        self.* = undefined;
    }
};

pub const DocumentExtractionRangeRoute = struct {
    range_id: []const u8,
    route_status: []const u8 = "local_committed",
    owner_group_id: u64 = 0,
};

pub fn freeDocumentExtractionUnitDescriptors(alloc: Allocator, descriptors: []DocumentExtractionUnitDescriptor) void {
    for (descriptors) |descriptor| {
        if (descriptor.key.len > 0) alloc.free(@constCast(descriptor.key));
        if (descriptor.fingerprint.len > 0) alloc.free(@constCast(descriptor.fingerprint));
    }
    if (descriptors.len > 0) alloc.free(descriptors);
}

pub const PendingDocumentUnitDenseChunkEmbedding = struct {
    embedding_name: []const u8,
    source_field: []const u8,
    expected_dims: u32,
    producer_json: []const u8,
    consumer_indexes: [][]u8,
    sources: std.ArrayListUnmanaged(ChunkEmbeddingSource) = .empty,
    chunk_texts: std.ArrayListUnmanaged([]const u8) = .empty,
    source_indexes: std.ArrayListUnmanaged(usize) = .empty,
    batch_source_bytes: usize = 0,

    pub fn deinit(self: *PendingDocumentUnitDenseChunkEmbedding, alloc: Allocator) void {
        clearChunkEmbeddingSourceList(alloc, &self.sources);
        self.sources.deinit(alloc);
        self.chunk_texts.deinit(alloc);
        self.source_indexes.deinit(alloc);
        for (self.consumer_indexes) |name| alloc.free(name);
        alloc.free(self.consumer_indexes);
    }
};

pub const PendingDocumentUnitSparseChunkEmbedding = struct {
    embedding_name: []const u8,
    producer_json: []const u8,
    consumer_indexes: [][]u8,
    sources: std.ArrayListUnmanaged(ChunkEmbeddingSource) = .empty,
    chunk_texts: std.ArrayListUnmanaged([]const u8) = .empty,
    source_indexes: std.ArrayListUnmanaged(usize) = .empty,
    batch_source_bytes: usize = 0,

    pub fn deinit(self: *PendingDocumentUnitSparseChunkEmbedding, alloc: Allocator) void {
        clearChunkEmbeddingSourceList(alloc, &self.sources);
        self.sources.deinit(alloc);
        self.chunk_texts.deinit(alloc);
        self.source_indexes.deinit(alloc);
        for (self.consumer_indexes) |name| alloc.free(name);
        alloc.free(self.consumer_indexes);
    }
};

pub const GraphEdgeWinner = struct {
    owner_state_key: []u8,
    payload: []u8,
    source_priority: usize,
};

pub const GraphEdgeWinners = struct {
    map: std.StringHashMapUnmanaged(GraphEdgeWinner) = .empty,

    pub fn deinit(self: *GraphEdgeWinners, alloc: Allocator) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            alloc.free(entry.value_ptr.owner_state_key);
            alloc.free(entry.value_ptr.payload);
        }
        self.map.deinit(alloc);
        self.* = undefined;
    }
};

pub const GraphContenderChange = struct {
    state_key: []const u8,
    source_priority: usize,
    payload: ?[]const u8,
};

pub const PendingGraphContenderOverlay = struct {
    write_positions: std.StringHashMapUnmanaged(usize) = .empty,
    delete_keys: std.StringHashMapUnmanaged(void) = .empty,
    global_writes_by_edge: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(usize)) = .empty,
    local_writes_by_edge: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(usize)) = .empty,
    indexed_writes: usize = 0,
    indexed_deletes: usize = 0,

    pub fn init(
        alloc: Allocator,
        pending_writes: []const docstore_mod.KVPair,
        pending_deletes: []const []const u8,
        expected_generation: u64,
    ) !@This() {
        var overlay = @This(){};
        errdefer overlay.deinit(alloc);
        try overlay.extend(alloc, pending_writes, pending_deletes, expected_generation);
        return overlay;
    }

    /// Incremental batches retain positions and key identities when replacing
    /// values. Only the appended suffix needs indexing; callers must preserve
    /// each existing key's contender generation and edge identity.
    pub fn extend(self: *@This(), alloc: Allocator, pending_writes: []const docstore_mod.KVPair, pending_deletes: []const []const u8, expected_generation: u64) !void {
        const overlay = self;
        if (builtin.is_test) if (test_graph_overlay_examined) |examined| {
            examined.* += pending_writes.len - self.indexed_writes + pending_deletes.len - self.indexed_deletes;
        };
        for (pending_writes[self.indexed_writes..], self.indexed_writes..) |write, i| {
            try overlay.write_positions.put(alloc, write.key, i);
            const bucket = if (internal_keys.isGraphGlobalEdgeContenderKey(write.key))
                &overlay.global_writes_by_edge
            else if (internal_keys.isGraphEdgeContenderMembershipKey(write.key))
                &overlay.local_writes_by_edge
            else
                continue;
            // One document batch may reconcile multiple graph indexes or
            // generations. Only index writes authenticated for this
            // generation; unrelated overlay records remain addressable by
            // full key but do not participate in its edge buckets.
            const view = (try graph_edge_contender.decode(write.value, expected_generation)) orelse continue;
            // Payloads may be replaced in place between owners. Bucket keys
            // must outlive those values, independently of the mutation arena.
            const positions = bucket.getPtr(view.edge_key) orelse blk: {
                const owned = try alloc.dupe(u8, view.edge_key);
                errdefer alloc.free(owned);
                try bucket.put(alloc, owned, .empty);
                break :blk bucket.getPtr(owned).?;
            };
            try positions.append(alloc, i);
        }
        for (pending_deletes[self.indexed_deletes..]) |key| try overlay.delete_keys.put(alloc, key, {});
        self.indexed_writes = pending_writes.len;
        self.indexed_deletes = pending_deletes.len;
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        var it = self.global_writes_by_edge.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(alloc);
        }
        self.global_writes_by_edge.deinit(alloc);
        var local = self.local_writes_by_edge.iterator();
        while (local.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(alloc);
        }
        self.local_writes_by_edge.deinit(alloc);
        self.write_positions.deinit(alloc);
        self.delete_keys.deinit(alloc);
        self.* = undefined;
    }
};

pub const GraphContenderReconcileResult = struct {
    winners: GraphEdgeWinners = .{},
    writes: std.ArrayListUnmanaged(docstore_mod.KVPair) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    visible_count: usize = 0,

    pub fn deinit(self: *GraphContenderReconcileResult, alloc: Allocator) void {
        self.winners.deinit(alloc);
        for (self.writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        self.writes.deinit(alloc);
        for (self.deletes.items) |key| alloc.free(@constCast(key));
        self.deletes.deinit(alloc);
        self.* = undefined;
    }
};

pub const GraphContenderMutation = struct {
    writes: std.ArrayListUnmanaged(docstore_mod.KVPair) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        self.writes.deinit(alloc);
        for (self.deletes.items) |key| alloc.free(@constCast(key));
        self.deletes.deinit(alloc);
        self.* = .{};
    }
};

pub fn freeOwnedConstKeySlice(alloc: Allocator, keys: []const []const u8) void {
    for (keys) |key| alloc.free(@constCast(key));
    if (keys.len > 0) alloc.free(keys);
}

pub const PendingArtifactWriteIndex = struct {
    values: std.StringHashMapUnmanaged([]const u8) = .empty,
    chunk_writes_by_doc: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(types.BatchWrite)) = .empty,

    pub fn init(alloc: Allocator, writes: []const types.BatchWrite) !PendingArtifactWriteIndex {
        var index = PendingArtifactWriteIndex{};
        errdefer index.deinit(alloc);
        for (writes) |write| try index.add(alloc, write);
        return index;
    }

    pub fn deinit(self: *PendingArtifactWriteIndex, alloc: Allocator) void {
        var it = self.chunk_writes_by_doc.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(alloc);
        }
        self.chunk_writes_by_doc.deinit(alloc);
        self.values.deinit(alloc);
        self.* = .{};
    }

    pub fn add(self: *PendingArtifactWriteIndex, alloc: Allocator, write: types.BatchWrite) !void {
        try self.values.put(alloc, write.key, write.value);
        if (!internal_keys.isChunkArtifactRecordKey(write.key)) return;
        const doc_key = (try internal_keys.decodeDocumentComponentAlloc(alloc, write.key)) orelse unreachable;
        if (self.chunk_writes_by_doc.getPtr(doc_key)) |list| {
            alloc.free(doc_key);
            try list.append(alloc, write);
        } else {
            var owned = true;
            errdefer if (owned) alloc.free(doc_key);
            try self.chunk_writes_by_doc.put(alloc, doc_key, .empty);
            owned = false;
            try self.chunk_writes_by_doc.getPtr(doc_key).?.append(alloc, write);
        }
    }

    pub fn chunkWritesForDoc(self: *const PendingArtifactWriteIndex, doc_key: []const u8) []const types.BatchWrite {
        return if (self.chunk_writes_by_doc.get(doc_key)) |list| list.items else &.{};
    }

    pub fn get(self: *const PendingArtifactWriteIndex, key: []const u8) ?[]const u8 {
        return self.values.get(key);
    }
};

pub const PendingChunkDeleteIndex = struct {
    keys: std.StringHashMapUnmanaged(void) = .empty,
    by_doc: std.StringHashMapUnmanaged(std.ArrayListUnmanaged([]const u8)) = .empty,
    indexed_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        var it = self.by_doc.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(alloc);
        }
        self.by_doc.deinit(alloc);
        self.keys.deinit(alloc);
        self.* = .{};
    }

    pub fn extend(self: *@This(), alloc: Allocator, deletes: []const []const u8) !void {
        // Batch lists grow across documents. Each chunk deletion is indexed
        // once, then cleanup visits only deletions for its own document.
        for (deletes[self.indexed_count..]) |key| {
            if (!internal_keys.isChunkArtifactRecordKey(key)) continue;
            try self.keys.put(alloc, key, {});
            const doc_key = (try internal_keys.decodeDocumentComponentAlloc(alloc, key)) orelse unreachable;
            if (self.by_doc.getPtr(doc_key)) |list| {
                alloc.free(doc_key);
                try list.append(alloc, key);
            } else {
                var doc_key_owned = true;
                errdefer if (doc_key_owned) alloc.free(doc_key);
                try self.by_doc.put(alloc, doc_key, .empty);
                doc_key_owned = false;
                try self.by_doc.getPtr(doc_key).?.append(alloc, key);
            }
        }
        self.indexed_count = deletes.len;
    }

    pub fn forDoc(self: *const @This(), doc_key: []const u8) []const []const u8 {
        return if (self.by_doc.get(doc_key)) |list| list.items else &.{};
    }
};

pub const BorrowedGraphMaterializationBatch = struct {
    writes: []docstore_mod.KVPair = &.{},
    deletes: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.writes.len > 0) alloc.free(self.writes);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        self.* = .{};
    }
};

pub const GraphArtifactClear = struct {
    doc_key: []u8,
    index_name: []u8,

    pub fn initAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8) !GraphArtifactClear {
        const owned_doc_key = try alloc.dupe(u8, doc_key);
        errdefer alloc.free(owned_doc_key);
        return .{
            .doc_key = owned_doc_key,
            .index_name = try alloc.dupe(u8, index_name),
        };
    }

    pub fn deinit(self: *GraphArtifactClear, alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.index_name);
        self.* = undefined;
    }
};

pub const ChunkEmbeddingSource = materialized_sources.ChunkEmbeddingSource;

pub const clearChunkEmbeddingSourceList = materialized_sources.clearChunkEmbeddingSourceList;

pub const PrecomputedCoverageOutcome = struct {
    index_name: []u8,
    doc_key: []u8,
    outcome: DerivedCoverageOutcome,

    pub fn deinit(self: @This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.doc_key);
    }
};

pub const PrecomputedCoverageCandidate = struct {
    request: enrichment_types.GeneratedEnrichmentRequest,
    produced: bool,

    pub fn deinit(self: @This(), alloc: Allocator) void {
        enrichment_types.freeGeneratedRequest(alloc, self.request);
    }
};

pub const GeneratedBatchWritePlan = struct {
    alloc: Allocator,
    rows: []index_manager_mod.IndexManager.WritePlanSnapshot.BorrowedGeneratedRowPlan,
    initialized: usize = 0,

    pub fn deinit(self: *GeneratedBatchWritePlan) void {
        for (self.rows[0..self.initialized]) |*row| row.deinit();
        self.alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const GeneratedDenseMemoJob = struct {
    key: GeneratedEmbeddingMemo.Key,
    text: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
};

pub const GeneratedSparseMemoJob = struct {
    key: GeneratedEmbeddingMemo.Key,
    text: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
};

pub const ChunkCacheEntry = struct {
    key: []u8,
    chunks: []chunker_mod.Chunk,
};

pub const GeneratedEmbeddingMemo = struct {
    pub const Key = [std.crypto.hash.sha2.Sha256.digest_length]u8;
    pub const SparseValue = struct { indices: []u32, values: []f32 };

    alloc: Allocator,
    reuse_stored_artifacts: bool = false,
    dense: std.AutoHashMapUnmanaged(Key, []f32) = .empty,
    sparse: std.AutoHashMapUnmanaged(Key, SparseValue) = .empty,

    pub fn init(alloc: Allocator) GeneratedEmbeddingMemo {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        var dense_values = self.dense.valueIterator();
        while (dense_values.next()) |vector| self.alloc.free(vector.*);
        self.dense.deinit(self.alloc);
        var sparse_values = self.sparse.valueIterator();
        while (sparse_values.next()) |value| {
            self.alloc.free(value.indices);
            self.alloc.free(value.values);
        }
        self.sparse.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn key(
        kind: enrichment_types.GeneratedEnrichmentKind,
        embedding_name: []const u8,
        producer_json: []const u8,
        execution_json: []const u8,
        expected_dims: u32,
        source: []const u8,
    ) Key {
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update(&.{@backingInt(kind)});
        var length_buf: [8]u8 = undefined;
        inline for (.{ embedding_name, producer_json, execution_json, source }) |part| {
            std.mem.writeInt(u64, &length_buf, part.len, .little);
            hasher.update(&length_buf);
            hasher.update(part);
        }
        var dims_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &dims_buf, expected_dims, .little);
        hasher.update(&dims_buf);
        var digest: Key = undefined;
        hasher.final(&digest);
        return digest;
    }

    pub fn adoptDense(self: *@This(), key_value: Key, owned: []f32) ![]const f32 {
        const gop = try self.dense.getOrPut(self.alloc, key_value);
        if (gop.found_existing) {
            self.alloc.free(owned);
            return gop.value_ptr.*;
        }
        gop.value_ptr.* = owned;
        return owned;
    }

    pub fn putDenseCopy(self: *@This(), key_value: Key, vector: []const f32) !void {
        if (self.dense.contains(key_value)) return;
        const owned = try self.alloc.dupe(f32, vector);
        errdefer self.alloc.free(owned);
        _ = try self.adoptDense(key_value, owned);
    }

    pub fn adoptSparse(
        self: *@This(),
        key_value: Key,
        owned: embedder_mod.SparseEmbedding,
    ) !SparseValue {
        const gop = try self.sparse.getOrPut(self.alloc, key_value);
        if (gop.found_existing) {
            var duplicate = owned;
            duplicate.deinit(self.alloc);
            return gop.value_ptr.*;
        }
        gop.value_ptr.* = .{ .indices = owned.indices, .values = owned.values };
        return gop.value_ptr.*;
    }

    pub fn putSparseCopy(self: *@This(), key_value: Key, value: embedder_mod.SparseEmbedding) !void {
        if (self.sparse.contains(key_value)) return;
        const indices = try self.alloc.dupe(u32, value.indices);
        errdefer self.alloc.free(indices);
        const values = try self.alloc.dupe(f32, value.values);
        errdefer self.alloc.free(values);
        _ = try self.adoptSparse(key_value, .{ .indices = indices, .values = values });
    }
};

pub const GeneratedPrecomputeMode = enum {
    none,
    all,
};

pub const PrecomputedGeneratedBatch = struct {
    artifact_writes: []types.BatchWrite = &.{},
    artifact_delete_keys: []const []const u8 = &.{},
    documents: []const derived_types.DerivedDocument = &.{},
    dense_embeddings: []const derived_types.DerivedDenseEmbeddingWrite = &.{},
    sparse_embeddings: []const derived_types.DerivedSparseEmbeddingWrite = &.{},
    generated_enrichment_refs: []const enrichment_types.GeneratedEnrichmentRef = &.{},
    coverage_outcomes: []PrecomputedCoverageOutcome = &.{},

    pub fn deinit(self: *PrecomputedGeneratedBatch, alloc: Allocator) void {
        for (self.artifact_writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.artifact_writes.len > 0) alloc.free(self.artifact_writes);
        for (self.artifact_delete_keys) |key| alloc.free(key);
        if (self.artifact_delete_keys.len > 0) alloc.free(self.artifact_delete_keys);

        var derived_batch = derived_types.DerivedBatch{
            .documents = self.documents,
            .dense_embeddings = self.dense_embeddings,
            .sparse_embeddings = self.sparse_embeddings,
            .generated_enrichment_refs = self.generated_enrichment_refs,
        };
        derived_types.deinitDerivedBatch(alloc, &derived_batch);
        for (self.coverage_outcomes) |outcome| outcome.deinit(alloc);
        if (self.coverage_outcomes.len > 0) alloc.free(self.coverage_outcomes);
        self.* = undefined;
    }
};

pub const GraphArtifactRefView = struct {
    name: []const u8,
    kind: types.ArtifactKind,
    unit_id_present: bool = false,
};

pub const EnrichmentTerminalFailureMarkerPair = struct {
    sequence_key: []u8,
    issue_key: []u8,

    pub fn init(alloc: Allocator, repair_issue_key: []const u8, sequence: u64) !@This() {
        const sequence_key = try internal_keys.enrichmentTerminalFailureSequenceKeyAlloc(alloc, sequence, repair_issue_key);
        errdefer alloc.free(sequence_key);
        return .{
            .sequence_key = sequence_key,
            .issue_key = try internal_keys.enrichmentTerminalFailureIssueKeyAlloc(alloc, repair_issue_key, sequence),
        };
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.sequence_key);
        alloc.free(self.issue_key);
        self.* = undefined;
    }
};

pub const enrichment_terminal_failure_marker_value_version: u8 = 1;

pub const enrichment_terminal_failure_marker_generation_len = @sizeOf(u64);

pub const enrichment_terminal_failure_retirement_page_size: usize = 64;

pub const EnrichmentTerminalFailureMarkerValue = struct {
    generation: u64,
    issue_key: []const u8,
};

pub fn decodeEnrichmentTerminalFailureMarkerValue(value: []const u8) !EnrichmentTerminalFailureMarkerValue {
    if (value.len > 0 and value[0] == enrichment_terminal_failure_marker_value_version) {
        if (value.len <= 1 + enrichment_terminal_failure_marker_generation_len)
            return error.InvalidEnrichmentTerminalFailureMarker;
        return .{
            .generation = std.mem.readInt(u64, value[1..][0..enrichment_terminal_failure_marker_generation_len], .big),
            .issue_key = value[1 + enrichment_terminal_failure_marker_generation_len ..],
        };
    }
    // Development/rollback compatibility for markers written before durable
    // incarnation fencing. Repair issue keys live in replay namespace 0x02,
    // so they cannot alias the version byte above.
    if (value.len == 0) return error.InvalidEnrichmentTerminalFailureMarker;
    return .{ .generation = 0, .issue_key = value };
}

pub fn encodeEnrichmentTerminalFailureMarkerValueAlloc(
    alloc: Allocator,
    repair_issue_key: []const u8,
    generation: u64,
) ![]u8 {
    const value = try alloc.alloc(u8, 1 + enrichment_terminal_failure_marker_generation_len + repair_issue_key.len);
    value[0] = enrichment_terminal_failure_marker_value_version;
    std.mem.writeInt(u64, value[1..][0..enrichment_terminal_failure_marker_generation_len], generation, .big);
    @memcpy(value[1 + enrichment_terminal_failure_marker_generation_len ..], repair_issue_key);
    return value;
}

pub fn loadEnrichmentTerminalFailureGeneration(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    repair_issue_key: []const u8,
) !u64 {
    const key = try internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(alloc, repair_issue_key);
    defer alloc.free(key);
    const value = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    defer alloc.free(value);
    if (value.len != enrichment_terminal_failure_marker_generation_len)
        return error.InvalidEnrichmentTerminalFailureGeneration;
    return std.mem.readInt(u64, value[0..enrichment_terminal_failure_marker_generation_len], .big);
}

pub fn loadEnrichmentTerminalFailureGenerationCounter(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
) !u64 {
    const value = store.get(
        alloc,
        &internal_keys.enrichment_terminal_failure_generation_counter_key,
    ) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    defer alloc.free(value);
    if (value.len != enrichment_terminal_failure_marker_generation_len)
        return error.InvalidEnrichmentTerminalFailureGenerationCounter;
    return std.mem.readInt(u64, value[0..enrichment_terminal_failure_marker_generation_len], .big);
}

pub fn encodeEnrichmentTerminalFailureGeneration(
    out: *[enrichment_terminal_failure_marker_generation_len]u8,
    generation: u64,
) void {
    std.mem.writeInt(u64, out, generation, .big);
}

pub fn nextEnrichmentTerminalFailureGeneration(current: u64) !u64 {
    if (current == std.math.maxInt(u64)) return error.EnrichmentTerminalFailureGenerationExhausted;
    return current + 1;
}

pub fn enrichmentTerminalFailureMarkerPairExists(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    repair_issue_key: []const u8,
    generation: u64,
    sequence: u64,
) !bool {
    var marker = try EnrichmentTerminalFailureMarkerPair.init(alloc, repair_issue_key, sequence);
    defer marker.deinit(alloc);

    const primary_value = store.get(alloc, marker.sequence_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(primary_value);
    const decoded = try decodeEnrichmentTerminalFailureMarkerValue(primary_value);
    if (decoded.generation != generation or !std.mem.eql(u8, decoded.issue_key, repair_issue_key)) return false;

    const reverse_value = store.get(alloc, marker.issue_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(reverse_value);
    return std.mem.eql(u8, reverse_value, marker.sequence_key);
}

pub const EnrichmentTerminalFailureGenerationSelection = struct {
    issue_generation: u64,
    counter: u64,
};

pub fn enrichmentTerminalFailureGenerationForWrite(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    repair_issue_key: []const u8,
    previous_sequence: ?u64,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
) !EnrichmentTerminalFailureGenerationSelection {
    const current_generation = loadEnrichmentTerminalFailureGeneration(alloc, store, repair_issue_key) catch |err| switch (err) {
        // A new counter token safely supersedes an unreadable live token, and
        // the final atomic write repairs this sidecar. Preserve ordinary
        // storage failures so parking cannot acknowledge debt that was never
        // made durable.
        error.InvalidEnrichmentTerminalFailureGeneration => 0,
        else => return err,
    };
    const persisted_counter = try loadEnrichmentTerminalFailureGenerationCounter(alloc, store);
    const high_water = @max(current_generation, persisted_counter);
    if (previous_sequence) |previous| {
        if (try enrichmentTerminalFailureMarkerPairExists(
            alloc,
            store,
            repair_issue_key,
            current_generation,
            previous,
        )) return .{
            .issue_generation = current_generation,
            .counter = high_water,
        };
    }
    try appendEnrichmentTerminalFailureMarkerDeletesForIssue(
        alloc,
        store,
        repair_issue_key,
        deletes,
        owned_delete_keys,
        false,
    );
    // Allocate from one durable store-wide high-water mark. This survives
    // bounded retirement without retaining a tombstone for every repaired
    // artifact, and cannot reuse an old token when an identical source
    // sequence is replayed later.
    const next = try nextEnrichmentTerminalFailureGeneration(high_water);
    return .{ .issue_generation = next, .counter = next };
}

pub const EnrichmentTerminalFailureMarkerWrite = struct {
    marker: EnrichmentTerminalFailureMarkerPair,
    generation_key: []u8,
    marker_value: []u8,
    generation_value: [enrichment_terminal_failure_marker_generation_len]u8,
    generation_counter_value: [enrichment_terminal_failure_marker_generation_len]u8,

    pub fn init(
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        repair_issue_key: []const u8,
        previous_sequence: ?u64,
        sequence: u64,
        deletes: *std.ArrayListUnmanaged([]const u8),
        owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
    ) !@This() {
        const generation = try enrichmentTerminalFailureGenerationForWrite(
            alloc,
            store,
            repair_issue_key,
            previous_sequence,
            deletes,
            owned_delete_keys,
        );
        var marker = try EnrichmentTerminalFailureMarkerPair.init(alloc, repair_issue_key, sequence);
        errdefer marker.deinit(alloc);
        const generation_key = try internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(alloc, repair_issue_key);
        errdefer alloc.free(generation_key);
        const marker_value = try encodeEnrichmentTerminalFailureMarkerValueAlloc(
            alloc,
            repair_issue_key,
            generation.issue_generation,
        );
        errdefer alloc.free(marker_value);
        var generation_value: [enrichment_terminal_failure_marker_generation_len]u8 = undefined;
        encodeEnrichmentTerminalFailureGeneration(&generation_value, generation.issue_generation);
        var generation_counter_value: [enrichment_terminal_failure_marker_generation_len]u8 = undefined;
        encodeEnrichmentTerminalFailureGeneration(&generation_counter_value, generation.counter);
        return .{
            .marker = marker,
            .generation_key = generation_key,
            .marker_value = marker_value,
            .generation_value = generation_value,
            .generation_counter_value = generation_counter_value,
        };
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.marker.deinit(alloc);
        alloc.free(self.generation_key);
        alloc.free(self.marker_value);
        self.* = undefined;
    }
};

pub fn appendEnrichmentTerminalFailureMarkerDeletesForIssue(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    repair_issue_key: []const u8,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
    retire_generation: bool,
) !void {
    _ = try appendEnrichmentTerminalFailureMarkerDeletePageForIssue(
        alloc,
        store,
        repair_issue_key,
        deletes,
        owned_delete_keys,
    );
    if (retire_generation) {
        const generation_key = try internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(alloc, repair_issue_key);
        errdefer alloc.free(generation_key);
        const owned_len = owned_delete_keys.items.len;
        try owned_delete_keys.append(alloc, generation_key);
        errdefer owned_delete_keys.shrinkRetainingCapacity(owned_len);
        try deletes.append(alloc, generation_key);
    }
}

pub fn appendEnrichmentTerminalFailureMarkerDeletePageForIssue(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    repair_issue_key: []const u8,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
) !usize {
    const prefix = try internal_keys.enrichmentTerminalFailureIssuePrefixAlloc(alloc, repair_issue_key);
    defer alloc.free(prefix);
    const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
    defer if (upper) |value| alloc.free(value);

    const ScanState = struct {
        alloc: Allocator,
        repair_issue_key: []const u8,
        deletes: *std.ArrayListUnmanaged([]const u8),
        owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
        entries_examined: usize = 0,

        pub fn appendOwned(self: *@This(), value: []const u8) !void {
            const owned = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(owned);
            const owned_len = self.owned_delete_keys.items.len;
            try self.owned_delete_keys.append(self.alloc, owned);
            errdefer self.owned_delete_keys.shrinkRetainingCapacity(owned_len);
            try self.deletes.append(self.alloc, owned);
        }

        pub fn scanEntry(raw_ctx: ?*anyopaque, secondary_key: []const u8, sequence_key: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(raw_ctx orelse return error.InvalidArgument));
            try state.appendOwned(secondary_key);

            const sequence = internal_keys.enrichmentTerminalFailureSequence(sequence_key) catch {
                state.entries_examined += 1;
                return if (state.entries_examined >= enrichment_terminal_failure_retirement_page_size) .stop else .@"continue";
            };

            // A reverse entry is untrusted durable metadata. Reconstruct the
            // only primary key this issue can own before allowing its value to
            // name a second deletion. A malformed or cross-issue value retires
            // only the corrupt reverse entry, preserving the other issue's
            // authoritative marker and therefore failing visibility closed.
            const expected_sequence_key = try internal_keys.enrichmentTerminalFailureSequenceKeyAlloc(
                state.alloc,
                sequence,
                state.repair_issue_key,
            );
            defer state.alloc.free(expected_sequence_key);
            if (std.mem.eql(u8, sequence_key, expected_sequence_key)) try state.appendOwned(sequence_key);

            state.entries_examined += 1;
            return if (state.entries_examined >= enrichment_terminal_failure_retirement_page_size) .stop else .@"continue";
        }
    };

    var state = ScanState{
        .alloc = alloc,
        .repair_issue_key = repair_issue_key,
        .deletes = deletes,
        .owned_delete_keys = owned_delete_keys,
    };
    try store.scanWithContext(prefix, if (upper) |value| value else "", .{}, &state, ScanState.scanEntry);
    return state.entries_examined;
}

pub const DerivedCoverageOutcome = enum { produced, skipped, terminal_failed };

pub const DerivedCoverageDocOutcome = struct {
    doc_key: []const u8,
    outcome: DerivedCoverageOutcome,
};

pub const ArtifactRepairCompletionState = struct {
    /// Incremented for every failure publication for this physical artifact.
    /// A repair may commit only if the epoch observed before provider work is
    /// still current afterward.
    epoch: u64 = 0,
    completed_sequence: u64 = 0,
    pending_issues: u64 = 0,
};

pub const ManagedIndexBatchApplicability = enum {
    irrelevant,
    relevant,
    missing_dependency,
};

pub const GraphMaterializationOptions = struct {
    require_resolution_contract: bool = false,
    repair_ctx: ?*const AsyncContext = null,
    sequence: u64 = 0,
    max_input_bytes: usize = graph_asset_state.hard_max_relation_artifact_bytes,
    max_relation_items: usize = graph_asset_state.hard_max_relation_items_per_artifact,
    max_materialized_edges: usize = graph_asset_state.hard_max_edges_per_document,
};

pub const MentionEdgeAggregate = struct {
    target: []u8,
    target_table: []u8,
    mention_confidence: f64,
    mention_artifact_keys: std.ArrayListUnmanaged([]u8) = .empty,

    pub fn deinit(self: *MentionEdgeAggregate, alloc: Allocator) void {
        alloc.free(self.target);
        alloc.free(self.target_table);
        for (self.mention_artifact_keys.items) |key| alloc.free(key);
        self.mention_artifact_keys.deinit(alloc);
        self.* = undefined;
    }

    pub fn appendMentionArtifactKey(self: *MentionEdgeAggregate, alloc: Allocator, key: []u8) !void {
        try self.mention_artifact_keys.append(alloc, key);
    }
};

pub const OwnedGraphMutations = struct {
    alloc: Allocator,
    writes: []types.GraphEdgeWrite = &.{},
    deletes: []types.GraphEdgeDelete = &.{},
    generation_bindings: []docstore_mod.KVPair = &.{},

    pub fn deinit(self: *OwnedGraphMutations) void {
        for (self.writes) |write| {
            self.alloc.free(@constCast(write.index_name));
            self.alloc.free(@constCast(write.source));
            self.alloc.free(@constCast(write.target));
            self.alloc.free(@constCast(write.edge_type));
            if (write.edge_id.len > 0) self.alloc.free(@constCast(write.edge_id));
            if (write.owner_document.len > 0) self.alloc.free(@constCast(write.owner_document));
            if (write.metadata_json.len > 0) self.alloc.free(@constCast(write.metadata_json));
            if (write.owner.len > 0) self.alloc.free(@constCast(write.owner));
        }
        if (self.writes.len > 0) self.alloc.free(self.writes);

        for (self.deletes) |delete| {
            self.alloc.free(@constCast(delete.index_name));
            self.alloc.free(@constCast(delete.source));
            self.alloc.free(@constCast(delete.target));
            self.alloc.free(@constCast(delete.edge_type));
            if (delete.edge_id.len > 0) self.alloc.free(@constCast(delete.edge_id));
            if (delete.owner_document.len > 0) self.alloc.free(@constCast(delete.owner_document));
            if (delete.owner.len > 0) self.alloc.free(@constCast(delete.owner));
        }
        if (self.deletes.len > 0) self.alloc.free(self.deletes);

        for (self.generation_bindings) |binding| {
            self.alloc.free(@constCast(binding.key));
            self.alloc.free(@constCast(binding.value));
        }
        if (self.generation_bindings.len > 0) self.alloc.free(self.generation_bindings);
        self.* = undefined;
    }
};

pub const GraphMutationCollectionOptions = struct {
    expected_generation: u64,
    repair_ctx: ?*const AsyncContext = null,
    sequence: u64 = 0,
};

pub const GeneratedEnrichmentNameLookup = struct {
    all: std.StringHashMapUnmanaged(void) = .empty,
    embeddings: std.StringHashMapUnmanaged(void) = .empty,

    pub fn init(
        alloc: Allocator,
        index_manager: *const index_manager_mod.IndexManager,
    ) !GeneratedEnrichmentNameLookup {
        var lookup = GeneratedEnrichmentNameLookup{};
        errdefer lookup.deinit(alloc);
        try lookup.all.ensureTotalCapacity(alloc, @intCast(index_manager.enrichments.items.len));
        try lookup.embeddings.ensureTotalCapacity(alloc, @intCast(index_manager.enrichments.items.len));
        for (index_manager.enrichments.items) |entry| {
            lookup.all.putAssumeCapacity(entry.name, {});
            if (entry.kind == .embedding) lookup.embeddings.putAssumeCapacity(entry.name, {});
        }
        return lookup;
    }

    pub fn deinit(self: *GeneratedEnrichmentNameLookup, alloc: Allocator) void {
        self.all.deinit(alloc);
        self.embeddings.deinit(alloc);
        self.* = undefined;
    }
};

pub fn isSplitMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "splitstate:") or std.mem.startsWith(u8, key, "splitdelta:");
}

pub fn stopNativeProjectionMaintenance(ctx: *AsyncContext) void {
    ctx.background_closing.store(true, .release);
    ctx.native_projection_owner.stop(ctx.io orelse return);
}

const graph_restore_materialization = @import("graph_restore_materialization.zig");

const document_child_range_effects = @import("document_child_range_effects.zig");

const durable_outbox = @import("durable_outbox.zig");

const replication_commit = @import("commit_integration.zig");

const db_config = @import("config.zig");

const graph_edge_ttl_expiration = @import("graph_edge_ttl_expiration.zig");

const replication_mutation_barrier_mod = @import("antfly_runtime_abi").mutation_barrier;

pub const DurableReplicationOutbox = durable_outbox.DurableReplicationOutbox;

pub const MutationBarrier = replication_mutation_barrier_mod.MutationBarrier;

pub const ReplicationDeferredCommitGate = replication_commit.ReplicationDeferredCommitGate;

pub const ReplicationDeferredCommitGates = replication_commit.ReplicationDeferredCommitGates;

pub const DocumentArtifactChildRangeOutboxDrainResult = document_child_range_outbox.DocumentArtifactChildRangeOutboxDrainResult;

pub const PrimaryBackend = db_config.PrimaryBackend;

pub const GraphRestoreParseCache = graph_restore_materialization.Cache;

pub const GraphContenderChanges = std.StringHashMapUnmanaged(std.ArrayListUnmanaged(GraphContenderChange));

pub const EmbeddingArtifactOrigin = union(enum) {
    authored,
    generated: ?u64,
};

pub const StoreWritePositions = std.StringHashMapUnmanaged(usize);

pub const DocumentChildRangeDispatchGroup = document_child_range_effects.DocumentChildRangeDispatchGroup;

pub const GraphTtlCandidate = graph_edge_ttl_expiration.Candidate;

pub const NoProgressDiagnostic = struct {
    index_name: []u8,
    indexed: u64,
    expected: u64,
    stuck_ns: u64,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        self.* = undefined;
    }
};
