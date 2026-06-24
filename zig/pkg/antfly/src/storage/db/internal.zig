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

const std = @import("std");
const platform = @import("antfly_platform");

const doc_set = @import("doc_set.zig");
const relational_store_mod = @import("relational_store.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);

pub const DenseBulkSessionScope = enum {
    auto,
    external,
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

pub const ForeignKeyRuntimeStats = struct {
    child_write_rejects: AtomicU64 = AtomicU64.init(0),
    parent_delete_rejects: AtomicU64 = AtomicU64.init(0),
    validation_runs: AtomicU64 = AtomicU64.init(0),
    dry_run_runs: AtomicU64 = AtomicU64.init(0),
    repair_runs: AtomicU64 = AtomicU64.init(0),
    scanned_child_rows: AtomicU64 = AtomicU64.init(0),
    referenced_child_rows: AtomicU64 = AtomicU64.init(0),
    scanned_ref_rows: AtomicU64 = AtomicU64.init(0),
    missing_parent_rows: AtomicU64 = AtomicU64.init(0),
    missing_ref_rows: AtomicU64 = AtomicU64.init(0),
    stale_ref_rows: AtomicU64 = AtomicU64.init(0),
    repaired_ref_rows: AtomicU64 = AtomicU64.init(0),
    deleted_stale_ref_rows: AtomicU64 = AtomicU64.init(0),

    pub fn recordChildWriteReject(self: *@This()) void {
        _ = self.child_write_rejects.fetchAdd(1, .monotonic);
    }

    pub fn recordParentDeleteReject(self: *@This()) void {
        _ = self.parent_delete_rejects.fetchAdd(1, .monotonic);
    }

    pub fn recordIntegrityReport(
        self: *@This(),
        mode: relational_store_mod.ForeignKeyIntegrityMode,
        report: relational_store_mod.ForeignKeyIntegrityReport,
    ) void {
        switch (mode) {
            .validate => _ = self.validation_runs.fetchAdd(1, .monotonic),
            .dry_run => _ = self.dry_run_runs.fetchAdd(1, .monotonic),
            .repair => _ = self.repair_runs.fetchAdd(1, .monotonic),
        }
        _ = self.scanned_child_rows.fetchAdd(report.scanned_child_rows, .monotonic);
        _ = self.referenced_child_rows.fetchAdd(report.referenced_child_rows, .monotonic);
        _ = self.scanned_ref_rows.fetchAdd(report.scanned_ref_rows, .monotonic);
        _ = self.missing_parent_rows.fetchAdd(report.missing_parent_rows, .monotonic);
        _ = self.missing_ref_rows.fetchAdd(report.missing_ref_rows, .monotonic);
        _ = self.stale_ref_rows.fetchAdd(report.stale_ref_rows, .monotonic);
        _ = self.repaired_ref_rows.fetchAdd(report.repaired_ref_rows, .monotonic);
        _ = self.deleted_stale_ref_rows.fetchAdd(report.deleted_stale_ref_rows, .monotonic);
    }

    pub fn snapshot(self: *@This()) types.ForeignKeyStats {
        return .{
            .child_write_rejects = self.child_write_rejects.load(.monotonic),
            .parent_delete_rejects = self.parent_delete_rejects.load(.monotonic),
            .validation_runs = self.validation_runs.load(.monotonic),
            .dry_run_runs = self.dry_run_runs.load(.monotonic),
            .repair_runs = self.repair_runs.load(.monotonic),
            .scanned_child_rows = self.scanned_child_rows.load(.monotonic),
            .referenced_child_rows = self.referenced_child_rows.load(.monotonic),
            .scanned_ref_rows = self.scanned_ref_rows.load(.monotonic),
            .missing_parent_rows = self.missing_parent_rows.load(.monotonic),
            .missing_ref_rows = self.missing_ref_rows.load(.monotonic),
            .stale_ref_rows = self.stale_ref_rows.load(.monotonic),
            .repaired_ref_rows = self.repaired_ref_rows.load(.monotonic),
            .deleted_stale_ref_rows = self.deleted_stale_ref_rows.load(.monotonic),
        };
    }
};

pub const applied_sequence_flush_interval_ns: u64 = 100 * std.time.ns_per_ms;

pub const AppliedSequenceCoalescer = struct {
    pending: std.StringHashMapUnmanaged(u64) = .empty,
    last_flush_ns: u64 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.clearPending(alloc);
        self.pending.deinit(alloc);
        self.* = .{};
    }

    pub fn note(self: *@This(), alloc: Allocator, index_name: []const u8, sequence: u64) !void {
        const gop = try self.pending.getOrPut(alloc, index_name);
        if (gop.found_existing) {
            gop.value_ptr.* = @max(gop.value_ptr.*, sequence);
            return;
        }
        errdefer _ = self.pending.remove(index_name);
        gop.key_ptr.* = try alloc.dupe(u8, index_name);
        gop.value_ptr.* = sequence;
    }

    pub fn shouldFlush(self: *const @This(), now_ns: u64) bool {
        if (self.pending.count() == 0) return false;
        return self.last_flush_ns == 0 or now_ns -| self.last_flush_ns >= applied_sequence_flush_interval_ns;
    }

    pub fn clearPending(self: *@This(), alloc: Allocator) void {
        var it = self.pending.iterator();
        while (it.next()) |entry| alloc.free(@constCast(entry.key_ptr.*));
        self.pending.clearRetainingCapacity();
    }

    pub fn removePending(self: *@This(), alloc: Allocator, index_name: []const u8) void {
        const removed = self.pending.fetchRemove(index_name) orelse return;
        alloc.free(@constCast(removed.key));
    }

    pub fn takePending(self: *@This(), index_name: []const u8) ?struct { owned_name: []const u8, sequence: u64 } {
        const removed = self.pending.fetchRemove(index_name) orelse return null;
        return .{
            .owned_name = @constCast(removed.key),
            .sequence = removed.value,
        };
    }
};

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
    phase: std.atomic.Value(u8) = .init(@intFromEnum(types.DenseCatchUpStats.Phase.idle)),
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
            .phase = @enumFromInt(self.phase.load(.monotonic)),
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
    lsm_open_manifest_ns: AtomicU64 = .init(0),
    lsm_open_ensuring_dirs_ns: AtomicU64 = .init(0),
    lsm_open_wal_replay_ns: AtomicU64 = .init(0),
    lsm_open_mounting_runs_ns: AtomicU64 = .init(0),
    lsm_open_loaded_runs: AtomicU64 = .init(0),
    lsm_open_obsolete_paths: AtomicU64 = .init(0),
    lsm_open_mutable_entries_after_replay: AtomicU64 = .init(0),
    lsm_open_immutable_memtables_after_replay: AtomicU64 = .init(0),
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
            .lsm_open_manifest_ns = self.lsm_open_manifest_ns.load(.monotonic),
            .lsm_open_ensuring_dirs_ns = self.lsm_open_ensuring_dirs_ns.load(.monotonic),
            .lsm_open_wal_replay_ns = self.lsm_open_wal_replay_ns.load(.monotonic),
            .lsm_open_mounting_runs_ns = self.lsm_open_mounting_runs_ns.load(.monotonic),
            .lsm_open_loaded_runs = self.lsm_open_loaded_runs.load(.monotonic),
            .lsm_open_obsolete_paths = self.lsm_open_obsolete_paths.load(.monotonic),
            .lsm_open_mutable_entries_after_replay = self.lsm_open_mutable_entries_after_replay.load(.monotonic),
            .lsm_open_immutable_memtables_after_replay = self.lsm_open_immutable_memtables_after_replay.load(.monotonic),
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
