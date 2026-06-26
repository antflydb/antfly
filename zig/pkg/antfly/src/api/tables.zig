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
const group_ids = @import("../common/group_ids.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const schema_openapi = @import("antfly_schema_openapi");
const schema_mod = @import("../schema/mod.zig");
const runtime_schema_mod = @import("../storage/schema.zig");
const algebraic_mod = @import("../storage/db/algebraic/mod.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const full_text_indexes = @import("full_text_indexes.zig");
const indexes_api = @import("indexes.zig");
const json_helpers = @import("json_helpers.zig");
const catalog_resources = @import("catalog_resources.zig");
const sql_adapter = @import("../sql/mod.zig");
const table_reads = @import("table_reads.zig");

pub const default_full_text_index_name = full_text_indexes.default_full_text_index_name;
pub const default_indexes_json = "{\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}}";
pub const default_database_name = metadata_table_manager.default_database_name;
pub const default_namespace_name = metadata_table_manager.default_namespace_name;

/// Name of the algebraic aggregation index auto-created for relational tables.
pub const default_relational_algebraic_index_name = "algebraic_index_v0";
pub const default_schema_json = "{\"version\":0,\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true,\"x-antfly-dynamic-indexing\":{\"mode\":\"infer_types\"}}}}}";

pub fn effectiveSchemaJson(schema_json: ?[]const u8) []const u8 {
    if (schema_json) |value| {
        if (value.len > 0) return value;
    }
    return default_schema_json;
}

pub const ParsedTableSchema = schema_mod.ParsedTableSchema;
pub const AppliedRelationalSqlDdlRecord = struct {
    table: metadata_table_manager.TableRecord,
    created_table: bool = false,
    dropped_table: bool = false,
    created_database: bool = false,
    dropped_database: bool = false,
    created_namespace: bool = false,
    renamed_namespace: bool = false,
    dropped_namespace: bool = false,
    created_tablespace: bool = false,
    renamed_tablespace: bool = false,
    dropped_tablespace: bool = false,
    noop: bool = false,
    requires_rebuild: bool = false,
    validation_required: bool = false,
    rewrite_required: bool = false,
    work_items: []const sql_adapter.AppliedDdlWorkItem = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        metadata_table_manager.freeTable(alloc, self.table);
        if (self.work_items.len > 0) {
            for (self.work_items) |item| {
                var mutable = item;
                mutable.deinit(alloc);
            }
            alloc.free(self.work_items);
        }
        self.* = undefined;
    }
};

pub fn emptyAppliedRelationalSqlDdlRecordAlloc(alloc: std.mem.Allocator) !AppliedRelationalSqlDdlRecord {
    return .{
        .table = try metadata_table_manager.cloneTable(alloc, .{
            .table_id = 0,
            .name = "",
        }),
    };
}

pub const RelationalSqlDdlAction = enum {
    create_table,
    update_table,
    drop_table,
};

pub const RelationalSqlDdlTarget = struct {
    database_name: []u8,
    namespace_name: []u8,
    table_name: []u8,
    action: RelationalSqlDdlAction = .update_table,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.database_name);
        alloc.free(self.namespace_name);
        alloc.free(self.table_name);
        self.* = undefined;
    }

    pub fn createsTable(self: @This()) bool {
        return self.action == .create_table;
    }

    pub fn dropsTable(self: @This()) bool {
        return self.action == .drop_table;
    }
};

const RelationalSqlDdlTableRef = struct {
    database_name: []u8,
    namespace_name: []u8,
    table_name: []u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.database_name);
        alloc.free(self.namespace_name);
        alloc.free(self.table_name);
        self.* = undefined;
    }
};
pub const LsmStorageStatus = struct {
    // Table status intentionally exposes a compact operational snapshot. Full
    // low-level WAL and scheduler counters remain available through metrics.
    run_count: u64 = 0,
    run_bytes: u64 = 0,
    l0_run_count: u64 = 0,
    l0_bytes: u64 = 0,
    lower_level_run_count: u64 = 0,
    lower_level_bytes: u64 = 0,
    max_level: u64 = 0,
    compactable_l0_run_count: u64 = 0,
    overlapping_l0_run_count: u64 = 0,
    soft_limit_l0_run_count: u64 = 0,
    hard_limit_l0_run_count: u64 = 0,
    write_stall_l0_run_debt: u64 = 0,
    soft_limit_l0_bytes: u64 = 0,
    hard_limit_l0_bytes: u64 = 0,
    write_stall_l0_byte_debt: u64 = 0,
    level_overflow_run_count: u64 = 0,
    level_overflow_bytes: u64 = 0,
    obsolete_path_count: u64 = 0,
    obsolete_paths_pinned_by_readers: u64 = 0,
    obsolete_paths_pinned_by_versions: u64 = 0,
    obsolete_paths_waiting_for_retry: u64 = 0,
    obsolete_paths_reclaimable: u64 = 0,
    obsolete_delete_failures: u64 = 0,
    obsolete_delete_retries: u64 = 0,
    current_manifest_bytes: u64 = 0,
    mutable_entry_count: u64 = 0,
    mutable_bytes: u64 = 0,
    immutable_memtable_count: u64 = 0,
    immutable_entry_count: u64 = 0,
    immutable_bytes: u64 = 0,
    mutable_snapshot_clone_count: u64 = 0,
    mutable_snapshot_clone_bytes: u64 = 0,
    mutable_snapshot_clone_peak_bytes: u64 = 0,
    read_snapshot_mutable_rotation_count: u64 = 0,
    read_snapshot_mutable_rotation_bytes: u64 = 0,
    wal_retained_bytes: u64 = 0,
    compaction_backlog_bytes: u64 = 0,
    active_readers: u64 = 0,
    active_readers_bound_read_txn: u64 = 0,
    active_readers_namespace_read_txn: u64 = 0,
    active_readers_probe_txn: u64 = 0,
    active_readers_current_scan: u64 = 0,
    active_readers_write_txn: u64 = 0,
    active_readers_compaction: u64 = 0,
    active_readers_other: u64 = 0,
    obsolete_paths_pinned_by_reader_bound_read_txn: u64 = 0,
    obsolete_paths_pinned_by_reader_namespace_read_txn: u64 = 0,
    obsolete_paths_pinned_by_reader_probe_txn: u64 = 0,
    obsolete_paths_pinned_by_reader_current_scan: u64 = 0,
    obsolete_paths_pinned_by_reader_write_txn: u64 = 0,
    obsolete_paths_pinned_by_reader_compaction: u64 = 0,
    obsolete_paths_pinned_by_reader_other: u64 = 0,
    active_bulk_ingest_batches: u64 = 0,
    manifest_dirty: bool = false,
    obsolete_manifest_dirty: bool = false,
    maintenance_score: u64 = 0,
    maintenance_debt_hint: u64 = 0,
    flush_count: u64 = 0,
    flush_output_run_count: u64 = 0,
    flush_output_bytes: u64 = 0,
    sorted_ingest_run_count: u64 = 0,
    sorted_ingest_bytes: u64 = 0,
    manifest_write_count: u64 = 0,
    manifest_bytes: u64 = 0,
    write_pressure_event_count: u64 = 0,
    write_pressure_compaction_count: u64 = 0,
    write_pressure_compaction_step_count: u64 = 0,
    write_pressure_overload_count: u64 = 0,
    write_pressure_overload_l0_run_debt: u64 = 0,
    immutable_rotation_count: u64 = 0,
    immutable_flush_count: u64 = 0,
    direct_bulk_ingest_attempt_count: u64 = 0,
    direct_bulk_ingest_success_count: u64 = 0,
    direct_bulk_ingest_entry_count: u64 = 0,
    bulk_append_attempt_count: u64 = 0,
    bulk_append_entry_count: u64 = 0,
    bulk_append_direct_success_count: u64 = 0,
    bulk_append_direct_entry_count: u64 = 0,
    bulk_append_fallback_backend_pending_count: u64 = 0,
    bulk_append_fallback_below_threshold_count: u64 = 0,
    bulk_append_fallback_duplicate_key_count: u64 = 0,
    bulk_append_fallback_to_mutable_entry_count: u64 = 0,
    direct_bulk_ingest_direct_entry_count: u64 = 0,
    direct_bulk_ingest_fallback_unsupported_count: u64 = 0,
    direct_bulk_ingest_fallback_backend_mutable_count: u64 = 0,
    direct_bulk_ingest_fallback_below_threshold_count: u64 = 0,
};

pub const TableStorageStatus = struct {
    table_name: []const u8,
    empty: bool,
    lsm: ?LsmStorageStatus = null,
};

fn readerPinCount(counts: [lsm_backend.reader_pin_kind_count]u64, kind: lsm_backend.ReaderPinKind) u64 {
    return counts[@intFromEnum(kind)];
}

pub fn lsmStorageStatusFromStats(stats: table_reads.LsmStorageStats) LsmStorageStatus {
    const maintenance = stats.maintenance;
    const write = stats.write;
    return .{
        .run_count = maintenance.total_runs,
        .run_bytes = maintenance.total_run_bytes,
        .l0_run_count = maintenance.l0_runs,
        .l0_bytes = maintenance.l0_bytes,
        .lower_level_run_count = maintenance.lower_level_runs,
        .lower_level_bytes = maintenance.lower_level_bytes,
        .max_level = maintenance.max_level,
        .compactable_l0_run_count = maintenance.compactable_l0_runs,
        .overlapping_l0_run_count = maintenance.overlapping_l0_runs,
        .soft_limit_l0_run_count = maintenance.soft_limit_l0_runs,
        .hard_limit_l0_run_count = maintenance.hard_limit_l0_runs,
        .write_stall_l0_run_debt = maintenance.write_stall_l0_run_debt,
        .soft_limit_l0_bytes = maintenance.soft_limit_l0_bytes,
        .hard_limit_l0_bytes = maintenance.hard_limit_l0_bytes,
        .write_stall_l0_byte_debt = maintenance.write_stall_l0_byte_debt,
        .level_overflow_run_count = maintenance.level_overflow_runs,
        .level_overflow_bytes = maintenance.level_overflow_bytes,
        .obsolete_path_count = maintenance.obsolete_paths,
        .obsolete_paths_pinned_by_readers = maintenance.obsolete_paths_pinned_by_readers,
        .obsolete_paths_pinned_by_versions = maintenance.obsolete_paths_pinned_by_versions,
        .obsolete_paths_waiting_for_retry = maintenance.obsolete_paths_waiting_for_retry,
        .obsolete_paths_reclaimable = maintenance.obsolete_paths_reclaimable,
        .obsolete_delete_failures = maintenance.obsolete_delete_failures,
        .obsolete_delete_retries = maintenance.obsolete_delete_retries,
        .current_manifest_bytes = maintenance.current_manifest_bytes,
        .mutable_entry_count = maintenance.mutable_entries,
        .mutable_bytes = maintenance.mutable_bytes,
        .immutable_memtable_count = maintenance.immutable_memtables,
        .immutable_entry_count = maintenance.immutable_entries,
        .immutable_bytes = maintenance.immutable_bytes,
        .mutable_snapshot_clone_count = maintenance.mutable_snapshot_clone_calls,
        .mutable_snapshot_clone_bytes = maintenance.mutable_snapshot_clone_bytes_total,
        .mutable_snapshot_clone_peak_bytes = maintenance.mutable_snapshot_clone_peak_bytes,
        .read_snapshot_mutable_rotation_count = maintenance.read_snapshot_mutable_rotations,
        .read_snapshot_mutable_rotation_bytes = maintenance.read_snapshot_mutable_rotation_bytes_total,
        .wal_retained_bytes = maintenance.wal_retained_bytes,
        .compaction_backlog_bytes = maintenance.compaction_scheduler_remembered_pending_bytes,
        .active_readers = maintenance.active_readers,
        .active_readers_bound_read_txn = readerPinCount(maintenance.active_readers_by_kind, .bound_read_txn),
        .active_readers_namespace_read_txn = readerPinCount(maintenance.active_readers_by_kind, .namespace_read_txn),
        .active_readers_probe_txn = readerPinCount(maintenance.active_readers_by_kind, .probe_txn),
        .active_readers_current_scan = readerPinCount(maintenance.active_readers_by_kind, .current_scan),
        .active_readers_write_txn = readerPinCount(maintenance.active_readers_by_kind, .write_txn),
        .active_readers_compaction = readerPinCount(maintenance.active_readers_by_kind, .compaction),
        .active_readers_other = readerPinCount(maintenance.active_readers_by_kind, .other),
        .obsolete_paths_pinned_by_reader_bound_read_txn = readerPinCount(maintenance.obsolete_paths_pinned_by_reader_kind, .bound_read_txn),
        .obsolete_paths_pinned_by_reader_namespace_read_txn = readerPinCount(maintenance.obsolete_paths_pinned_by_reader_kind, .namespace_read_txn),
        .obsolete_paths_pinned_by_reader_probe_txn = readerPinCount(maintenance.obsolete_paths_pinned_by_reader_kind, .probe_txn),
        .obsolete_paths_pinned_by_reader_current_scan = readerPinCount(maintenance.obsolete_paths_pinned_by_reader_kind, .current_scan),
        .obsolete_paths_pinned_by_reader_write_txn = readerPinCount(maintenance.obsolete_paths_pinned_by_reader_kind, .write_txn),
        .obsolete_paths_pinned_by_reader_compaction = readerPinCount(maintenance.obsolete_paths_pinned_by_reader_kind, .compaction),
        .obsolete_paths_pinned_by_reader_other = readerPinCount(maintenance.obsolete_paths_pinned_by_reader_kind, .other),
        .active_bulk_ingest_batches = maintenance.active_bulk_ingest_batches,
        .manifest_dirty = maintenance.manifest_dirty,
        .obsolete_manifest_dirty = maintenance.obsolete_manifest_dirty,
        .maintenance_score = stats.maintenance_score,
        .maintenance_debt_hint = stats.maintenance_debt_hint,
        .flush_count = write.flushes,
        .flush_output_run_count = write.flush_output_runs,
        .flush_output_bytes = write.flush_output_bytes,
        .sorted_ingest_run_count = write.sorted_ingest_runs,
        .sorted_ingest_bytes = write.sorted_ingest_bytes,
        .manifest_write_count = write.manifest_writes,
        .manifest_bytes = write.manifest_bytes,
        .write_pressure_event_count = write.write_pressure_events,
        .write_pressure_compaction_count = write.write_pressure_compactions,
        .write_pressure_compaction_step_count = write.write_pressure_compaction_steps,
        .write_pressure_overload_count = write.write_pressure_overloads,
        .write_pressure_overload_l0_run_debt = write.write_pressure_overload_l0_run_debt,
        .immutable_rotation_count = write.immutable_rotations,
        .immutable_flush_count = write.immutable_flushes,
        .bulk_append_attempt_count = write.bulk_append_attempts,
        .bulk_append_entry_count = write.bulk_append_entries,
        .bulk_append_direct_success_count = write.bulk_append_direct_successes,
        .bulk_append_direct_entry_count = write.bulk_append_direct_entries,
        .bulk_append_fallback_backend_pending_count = write.bulk_append_fallback_backend_pending,
        .bulk_append_fallback_below_threshold_count = write.bulk_append_fallback_below_threshold,
        .bulk_append_fallback_duplicate_key_count = write.bulk_append_fallback_duplicate_keys,
        .bulk_append_fallback_to_mutable_entry_count = write.bulk_append_fallback_to_mutable_entries,
        .direct_bulk_ingest_attempt_count = write.direct_bulk_ingest_attempts,
        .direct_bulk_ingest_success_count = write.direct_bulk_ingest_successes,
        .direct_bulk_ingest_entry_count = write.direct_bulk_ingest_entries,
        .direct_bulk_ingest_direct_entry_count = write.direct_bulk_ingest_entries_direct,
        .direct_bulk_ingest_fallback_unsupported_count = write.direct_bulk_ingest_fallback_unsupported,
        .direct_bulk_ingest_fallback_backend_mutable_count = write.direct_bulk_ingest_fallback_backend_mutable,
        .direct_bulk_ingest_fallback_below_threshold_count = write.direct_bulk_ingest_fallback_below_threshold,
    };
}

pub fn lsmStorageStatusFromMaintenanceStats(maintenance: lsm_backend.Backend.MaintenanceStats) LsmStorageStatus {
    return lsmStorageStatusFromStats(.{ .maintenance = maintenance, .write = .{} });
}

fn generatedLsmStorageStatus(status: LsmStorageStatus) metadata_openapi.LsmStorageStatus {
    return .{
        .run_count = u64ToI64(status.run_count),
        .run_bytes = u64ToI64(status.run_bytes),
        .l0_run_count = u64ToI64(status.l0_run_count),
        .l0_bytes = u64ToI64(status.l0_bytes),
        .lower_level_run_count = u64ToI64(status.lower_level_run_count),
        .lower_level_bytes = u64ToI64(status.lower_level_bytes),
        .max_level = u64ToI64(status.max_level),
        .compactable_l0_run_count = u64ToI64(status.compactable_l0_run_count),
        .overlapping_l0_run_count = u64ToI64(status.overlapping_l0_run_count),
        .soft_limit_l0_run_count = u64ToI64(status.soft_limit_l0_run_count),
        .hard_limit_l0_run_count = u64ToI64(status.hard_limit_l0_run_count),
        .write_stall_l0_run_debt = u64ToI64(status.write_stall_l0_run_debt),
        .soft_limit_l0_bytes = u64ToI64(status.soft_limit_l0_bytes),
        .hard_limit_l0_bytes = u64ToI64(status.hard_limit_l0_bytes),
        .write_stall_l0_byte_debt = u64ToI64(status.write_stall_l0_byte_debt),
        .level_overflow_run_count = u64ToI64(status.level_overflow_run_count),
        .level_overflow_bytes = u64ToI64(status.level_overflow_bytes),
        .obsolete_path_count = u64ToI64(status.obsolete_path_count),
        .obsolete_paths_pinned_by_readers = u64ToI64(status.obsolete_paths_pinned_by_readers),
        .obsolete_paths_pinned_by_versions = u64ToI64(status.obsolete_paths_pinned_by_versions),
        .obsolete_paths_waiting_for_retry = u64ToI64(status.obsolete_paths_waiting_for_retry),
        .obsolete_paths_reclaimable = u64ToI64(status.obsolete_paths_reclaimable),
        .obsolete_delete_failures = u64ToI64(status.obsolete_delete_failures),
        .obsolete_delete_retries = u64ToI64(status.obsolete_delete_retries),
        .current_manifest_bytes = u64ToI64(status.current_manifest_bytes),
        .mutable_entry_count = u64ToI64(status.mutable_entry_count),
        .mutable_bytes = u64ToI64(status.mutable_bytes),
        .immutable_memtable_count = u64ToI64(status.immutable_memtable_count),
        .immutable_entry_count = u64ToI64(status.immutable_entry_count),
        .immutable_bytes = u64ToI64(status.immutable_bytes),
        .mutable_snapshot_clone_count = u64ToI64(status.mutable_snapshot_clone_count),
        .mutable_snapshot_clone_bytes = u64ToI64(status.mutable_snapshot_clone_bytes),
        .mutable_snapshot_clone_peak_bytes = u64ToI64(status.mutable_snapshot_clone_peak_bytes),
        .read_snapshot_mutable_rotation_count = u64ToI64(status.read_snapshot_mutable_rotation_count),
        .read_snapshot_mutable_rotation_bytes = u64ToI64(status.read_snapshot_mutable_rotation_bytes),
        .wal_retained_bytes = u64ToI64(status.wal_retained_bytes),
        .compaction_backlog_bytes = u64ToI64(status.compaction_backlog_bytes),
        .active_readers = u64ToI64(status.active_readers),
        .active_readers_bound_read_txn = u64ToI64(status.active_readers_bound_read_txn),
        .active_readers_namespace_read_txn = u64ToI64(status.active_readers_namespace_read_txn),
        .active_readers_probe_txn = u64ToI64(status.active_readers_probe_txn),
        .active_readers_current_scan = u64ToI64(status.active_readers_current_scan),
        .active_readers_write_txn = u64ToI64(status.active_readers_write_txn),
        .active_readers_compaction = u64ToI64(status.active_readers_compaction),
        .active_readers_other = u64ToI64(status.active_readers_other),
        .obsolete_paths_pinned_by_reader_bound_read_txn = u64ToI64(status.obsolete_paths_pinned_by_reader_bound_read_txn),
        .obsolete_paths_pinned_by_reader_namespace_read_txn = u64ToI64(status.obsolete_paths_pinned_by_reader_namespace_read_txn),
        .obsolete_paths_pinned_by_reader_probe_txn = u64ToI64(status.obsolete_paths_pinned_by_reader_probe_txn),
        .obsolete_paths_pinned_by_reader_current_scan = u64ToI64(status.obsolete_paths_pinned_by_reader_current_scan),
        .obsolete_paths_pinned_by_reader_write_txn = u64ToI64(status.obsolete_paths_pinned_by_reader_write_txn),
        .obsolete_paths_pinned_by_reader_compaction = u64ToI64(status.obsolete_paths_pinned_by_reader_compaction),
        .obsolete_paths_pinned_by_reader_other = u64ToI64(status.obsolete_paths_pinned_by_reader_other),
        .active_bulk_ingest_batches = u64ToI64(status.active_bulk_ingest_batches),
        .manifest_dirty = status.manifest_dirty,
        .obsolete_manifest_dirty = status.obsolete_manifest_dirty,
        .maintenance_score = u64ToI64(status.maintenance_score),
        .maintenance_debt_hint = u64ToI64(status.maintenance_debt_hint),
        .flush_count = u64ToI64(status.flush_count),
        .flush_output_run_count = u64ToI64(status.flush_output_run_count),
        .flush_output_bytes = u64ToI64(status.flush_output_bytes),
        .sorted_ingest_run_count = u64ToI64(status.sorted_ingest_run_count),
        .sorted_ingest_bytes = u64ToI64(status.sorted_ingest_bytes),
        .manifest_write_count = u64ToI64(status.manifest_write_count),
        .manifest_bytes = u64ToI64(status.manifest_bytes),
        .write_pressure_event_count = u64ToI64(status.write_pressure_event_count),
        .write_pressure_compaction_count = u64ToI64(status.write_pressure_compaction_count),
        .write_pressure_compaction_step_count = u64ToI64(status.write_pressure_compaction_step_count),
        .write_pressure_overload_count = u64ToI64(status.write_pressure_overload_count),
        .write_pressure_overload_l0_run_debt = u64ToI64(status.write_pressure_overload_l0_run_debt),
        .immutable_rotation_count = u64ToI64(status.immutable_rotation_count),
        .immutable_flush_count = u64ToI64(status.immutable_flush_count),
        .bulk_append_attempt_count = u64ToI64(status.bulk_append_attempt_count),
        .bulk_append_entry_count = u64ToI64(status.bulk_append_entry_count),
        .bulk_append_direct_success_count = u64ToI64(status.bulk_append_direct_success_count),
        .bulk_append_direct_entry_count = u64ToI64(status.bulk_append_direct_entry_count),
        .bulk_append_fallback_backend_pending_count = u64ToI64(status.bulk_append_fallback_backend_pending_count),
        .bulk_append_fallback_below_threshold_count = u64ToI64(status.bulk_append_fallback_below_threshold_count),
        .bulk_append_fallback_duplicate_key_count = u64ToI64(status.bulk_append_fallback_duplicate_key_count),
        .bulk_append_fallback_to_mutable_entry_count = u64ToI64(status.bulk_append_fallback_to_mutable_entry_count),
        .direct_bulk_ingest_attempt_count = u64ToI64(status.direct_bulk_ingest_attempt_count),
        .direct_bulk_ingest_success_count = u64ToI64(status.direct_bulk_ingest_success_count),
        .direct_bulk_ingest_entry_count = u64ToI64(status.direct_bulk_ingest_entry_count),
        .direct_bulk_ingest_direct_entry_count = u64ToI64(status.direct_bulk_ingest_direct_entry_count),
        .direct_bulk_ingest_fallback_unsupported_count = u64ToI64(status.direct_bulk_ingest_fallback_unsupported_count),
        .direct_bulk_ingest_fallback_backend_mutable_count = u64ToI64(status.direct_bulk_ingest_fallback_backend_mutable_count),
        .direct_bulk_ingest_fallback_below_threshold_count = u64ToI64(status.direct_bulk_ingest_fallback_below_threshold_count),
    };
}

fn u64ToI64(value: u64) i64 {
    return if (value > std.math.maxInt(i64)) std.math.maxInt(i64) else @intCast(value);
}

const RuntimeSchemaDebugBinding = struct {
    index_name: []const u8,
    status: []const u8,
    schema_version: ?u32 = null,
    schema_slot: ?[]const u8 = null,
    runtime_schema: ?std.json.Value = null,
};

const RuntimeSchemaDebugSchemaEntry = struct {
    slot: []const u8,
    status: []const u8,
    @"error": ?[]const u8 = null,
    schema_version: ?u32 = null,
    runtime_schema: ?std.json.Value = null,
};

const AlgebraicCapabilityDebugEntry = struct {
    slot: []const u8,
    status: []const u8,
    @"error": ?[]const u8 = null,
    schema_version: ?u32 = null,
    capability_fingerprint: ?[]const u8 = null,
    lifecycle_status: ?[]const u8 = null,
    change_added_fields: ?u32 = null,
    change_removed_fields: ?u32 = null,
    change_changed_type_fields: ?u32 = null,
    compatible_additive: ?bool = null,
    requires_rebuild: ?bool = null,
    group_field_count: u32 = 0,
    measure_field_count: u32 = 0,
    time_field_count: u32 = 0,
    skipped_dynamic_fields: u32 = 0,
    skipped_complex_fields: u32 = 0,
    skipped_unbounded_fields: u32 = 0,
    config: ?std.json.Value = null,
};

const TableRuntimeSchemaDebug = struct {
    runtime_schemas: []const RuntimeSchemaDebugSchemaEntry,
    full_text_index_bindings: []const RuntimeSchemaDebugBinding,
    algebraic_capabilities: []const AlgebraicCapabilityDebugEntry,
};

const IndexRuntimeSchemaDebug = struct {
    binding: RuntimeSchemaDebugBinding,
};

pub const TableStatusWithRuntimeSchemaDebug = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    indexes: std.json.ArrayHashMap(indexes_openapi.IndexConfig),
    shards: std.json.ArrayHashMap(metadata_openapi.ShardConfig),
    schema: ?schema_openapi.TableSchema = null,
    migration: ?metadata_openapi.TableMigration = null,
    replication_sources: ?[]const metadata_openapi.ReplicationSource = null,
    storage_status: metadata_openapi.StorageStatus,
    debug: TableRuntimeSchemaDebug,
};

pub fn encodeTableList(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    prefix: ?[]const u8,
) ![]u8 {
    return try encodeTableListWithStorageStatuses(alloc, snapshot, prefix, null);
}

pub fn encodeTableListWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    prefix: ?[]const u8,
    storage_statuses: ?[]const TableStorageStatus,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const listed = try buildTableListWithStorageStatuses(arena_impl.allocator(), snapshot, prefix, storage_statuses);
    const encoded = try std.json.Stringify.valueAlloc(alloc, listed, .{ .emit_null_optional_fields = false });
    defer alloc.free(encoded);
    return try projectInlineEnrichmentConfigsInTableStatusJson(alloc, encoded);
}

pub fn encodeSingleTableStatus(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
) !?[]u8 {
    return try encodeSingleTableStatusWithStorageStatuses(alloc, snapshot, table_name, null);
}

pub fn encodeSingleTableStatusWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const status = (try buildSingleTableStatusWithStorageStatuses(arena_impl.allocator(), snapshot, table_name, storage_statuses)) orelse return null;
    const encoded = try std.json.Stringify.valueAlloc(alloc, status, .{ .emit_null_optional_fields = false });
    defer alloc.free(encoded);
    const table = findTableByName(snapshot, table_name).?;
    return try projectSingleTableStatusJson(alloc, encoded, table.indexes_json);
}

pub fn buildTableListWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    prefix: ?[]const u8,
    storage_statuses: ?[]const TableStorageStatus,
) ![]metadata_openapi.TableStatus {
    var count: usize = 0;
    for (snapshot.tables) |*table| {
        if (prefix) |pfx| {
            if (!std.mem.startsWith(u8, table.name, pfx)) continue;
        }
        count += 1;
    }

    const listed = try alloc.alloc(metadata_openapi.TableStatus, count);
    var index: usize = 0;
    for (snapshot.tables) |*table| {
        if (prefix) |pfx| {
            if (!std.mem.startsWith(u8, table.name, pfx)) continue;
        }
        listed[index] = try buildTableStatus(alloc, snapshot, table, findTableStorageStatus(storage_statuses, table.name), false);
        index += 1;
    }
    return listed;
}

pub fn buildSingleTableStatusWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?metadata_openapi.TableStatus {
    return try buildSingleQualifiedTableStatusWithStorageStatuses(
        alloc,
        snapshot,
        default_database_name,
        default_namespace_name,
        table_name,
        storage_statuses,
    );
}

pub fn buildSingleQualifiedTableStatusWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?metadata_openapi.TableStatus {
    const table = findTableByQualifiedName(snapshot, database_name, namespace_name, table_name) orelse return null;
    return try buildTableStatus(alloc, snapshot, table, findTableStorageStatus(storage_statuses, table.name), true);
}

pub fn encodeSingleTableStatusWithRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = (try buildSingleTableStatusWithRuntimeSchemaDebug(arena_impl.allocator(), snapshot, table_name, storage_statuses)) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, response, .{ .emit_null_optional_fields = false });
}

pub fn buildSingleTableStatusWithRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?TableStatusWithRuntimeSchemaDebug {
    const table = findTableByName(snapshot, table_name) orelse return null;
    const base = (try buildSingleTableStatusWithStorageStatuses(alloc, snapshot, table_name, storage_statuses)) orelse return null;
    const debug = try buildTableRuntimeSchemaDebug(alloc, table);
    return .{
        .name = base.name,
        .description = base.description,
        .indexes = base.indexes,
        .shards = base.shards,
        .schema = base.schema,
        .migration = base.migration,
        .replication_sources = base.replication_sources,
        .storage_status = base.storage_status,
        .debug = debug,
    };
}

pub fn encodeSingleTableIndexWithRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    index_name: []const u8,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const value = (try buildSingleTableIndexWithRuntimeSchemaDebugValue(arena_impl.allocator(), snapshot, table_name, index_name)) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
}

pub fn buildSingleTableIndexWithRuntimeSchemaDebugValue(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    index_name: []const u8,
) !?std.json.Value {
    const table = findTableByName(snapshot, table_name) orelse return null;
    var value = (try buildSingleTableIndexValue(alloc, table, index_name)) orelse return null;
    errdefer deinitJsonValue(alloc, &value);
    if (value != .object) return error.InvalidTableIndexMetadata;
    try value.object.put(alloc, try alloc.dupe(u8, "debug"), try buildTableIndexRuntimeSchemaDebugValue(alloc, table, index_name));
    return value;
}

pub const CreateTableRequest = struct {
    num_shards: ?u32 = null,
    description: ?[]u8 = null,
    indexes_json: ?[]u8 = null,
    schema_json: ?[]u8 = null,
    replication_sources_json: ?[]u8 = null,
    tablespace_name: ?[]u8 = null,

    pub fn deinit(self: *CreateTableRequest, alloc: std.mem.Allocator) void {
        if (self.description) |value| alloc.free(value);
        if (self.indexes_json) |value| alloc.free(value);
        if (self.schema_json) |value| alloc.free(value);
        if (self.replication_sources_json) |value| alloc.free(value);
        if (self.tablespace_name) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn parseCreateTableRequest(alloc: std.mem.Allocator, body: []const u8) !CreateTableRequest {
    if (body.len == 0) return .{};
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var req: CreateTableRequest = .{};
    errdefer req.deinit(alloc);

    if (root.get("num_shards")) |value| {
        if (value != .null) req.num_shards = try parseU32Field(value);
    }
    if (root.get("description")) |value| {
        req.description = switch (value) {
            .null => null,
            .string => |str| try alloc.dupe(u8, str),
            else => return error.InvalidCreateTableRequest,
        };
    }
    if (root.get("indexes")) |value| {
        if (value != .null)
            req.indexes_json = try stringifyJsonValue(alloc, value)
        else
            req.indexes_json = try alloc.dupe(u8, default_indexes_json);
    } else {
        req.indexes_json = try alloc.dupe(u8, default_indexes_json);
    }
    if (root.get("typed_paths")) |value| {
        if (value != .null) {
            const indexes_with_typed_paths = try mergeTypedPathsIntoIndexesJsonAlloc(alloc, req.indexes_json.?, value);
            alloc.free(req.indexes_json.?);
            req.indexes_json = indexes_with_typed_paths;
        }
    }
    if (root.get("schema")) |value| {
        if (value != .null) {
            const encoded_schema = try stringifyJsonValue(alloc, value);
            defer alloc.free(encoded_schema);
            const validated_schema = parseSchemaUpdateRequest(alloc, encoded_schema) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableRequest,
                else => return err,
            };
            defer alloc.free(validated_schema);
            req.schema_json = try normalizeSchemaVersion(alloc, validated_schema, 0);
        }
    }
    if (root.get("replication_sources")) |value| {
        if (value != .null) {
            const encoded_replication_sources = try stringifyJsonValue(alloc, value);
            defer alloc.free(encoded_replication_sources);
            req.replication_sources_json = try validateReplicationSourcesJson(alloc, encoded_replication_sources);
        }
    }
    if (root.get("tablespace_name")) |value| {
        req.tablespace_name = switch (value) {
            .null => null,
            .string => |str| try alloc.dupe(u8, str),
            else => return error.InvalidCreateTableRequest,
        };
        if (req.tablespace_name) |name| {
            if (name.len == 0 or std.mem.indexOfScalar(u8, name, '.') != null) return error.InvalidCreateTableRequest;
        }
    }

    if (req.num_shards) |num_shards| {
        if (num_shards == 0) return error.InvalidCreateTableRequest;
    }
    return req;
}

pub fn mergeTypedPathsIntoIndexesJsonAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    typed_paths: std.json.Value,
) ![]u8 {
    try validateTypedPathsMetadataValue(typed_paths);
    const typed_paths_json = try stringifyJsonValue(alloc, typed_paths);
    defer alloc.free(typed_paths_json);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len > 0) indexes_json else default_indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "typed_paths")) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendJsonValue(alloc, &out, entry.value_ptr.*);
    }
    if (!first) try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "typed_paths");
    try out.append(alloc, ':');
    try out.appendSlice(alloc, typed_paths_json);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn validateTypedPathsMetadataValue(value: std.json.Value) !void {
    switch (value) {
        .object, .array => {},
        else => return error.InvalidCreateTableRequest,
    }
}

pub fn expandSchemaDerivedAlgebraicIndexesAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    indexes_json: []const u8,
    schema_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len > 0) indexes_json else default_indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return try alloc.dupe(u8, indexes_json),
    };

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var object = std.json.ObjectMap.empty;
    var changed = false;
    var it = root.iterator();
    while (it.next()) |entry| {
        const value = if (isSchemaDerivedAlgebraicIndex(entry.value_ptr.*)) blk: {
            if (schema_json.len == 0) return error.InvalidCreateTableRequest;
            changed = true;
            break :blk try schemaDerivedAlgebraicIndexValueAlloc(arena, table_name, schema_json, entry.value_ptr.*);
        } else try cloneJsonValueAlloc(arena, entry.value_ptr.*);
        try object.put(arena, try arena.dupe(u8, entry.key_ptr.*), value);
    }
    if (!changed) return try alloc.dupe(u8, indexes_json);
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = object }, .{ .emit_null_optional_fields = false });
}

pub fn prepareTableIndexesForSchemaAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    indexes_json: []const u8,
    schema_json: []const u8,
) ![]u8 {
    const with_relational_default = if (try ensureRelationalAlgebraicIndexAlloc(alloc, indexes_json, schema_json)) |with_default|
        with_default
    else
        try alloc.dupe(u8, indexes_json);
    defer alloc.free(with_relational_default);
    return try expandSchemaDerivedAlgebraicIndexesAlloc(alloc, table_name, with_relational_default, schema_json);
}

pub fn expandSchemaDerivedAlgebraicIndexAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_json: []const u8,
    schema_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    if (!isSchemaDerivedAlgebraicIndex(parsed.value)) return try alloc.dupe(u8, index_json);
    if (schema_json.len == 0) return error.InvalidCreateTableRequest;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const value = try schemaDerivedAlgebraicIndexValueAlloc(arena_impl.allocator(), table_name, schema_json, parsed.value);
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
}

pub fn validateDerivedIndexFieldRefsForSchemaAlloc(
    alloc: std.mem.Allocator,
    index_json: []const u8,
    schema_json: []const u8,
) !void {
    if (schema_json.len == 0) return error.InvalidTableIndexMetadata;
    var parsed_index = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed_index.deinit();
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    try validateDerivedIndexFieldRefsValue(runtime, parsed_index.value);
}

fn validateDerivedIndexFieldRefsValue(schema: runtime_schema_mod.TableSchema, value: std.json.Value) !void {
    if (value != .object) return error.InvalidTableIndexMetadata;
    const type_value = value.object.get("type") orelse return;
    if (type_value != .string) return error.InvalidTableIndexMetadata;
    if (std.mem.eql(u8, type_value.string, "full_text")) {
        if (value.object.get("field")) |field| try validateDerivedIndexFieldRefJson(schema, field);
    } else if (std.mem.eql(u8, type_value.string, "embeddings")) {
        if (value.object.get("field")) |field| try validateDerivedIndexFieldRefJson(schema, field);
        if (value.object.get("embedder")) |embedder| try validateEmbedderModelRefValue(embedder);
    } else if (std.mem.eql(u8, type_value.string, "graph")) {
        if (value.object.get("edge_policy")) |edge_policy| try validateGraphEdgePolicyValue(edge_policy);
        if (value.object.get("edge_table")) |edge_table| {
            if (edge_table != .object) return error.InvalidTableIndexMetadata;
            try validateRequiredDerivedIndexFieldRef(schema, edge_table.object, "source_field");
            try validateRequiredDerivedIndexFieldRef(schema, edge_table.object, "target_field");
            if (edge_table.object.get("type_field")) |type_field| try validateOptionalDerivedIndexFieldRefJson(schema, type_field);
            if (edge_table.object.get("weight_field")) |weight_field| try validateOptionalDerivedIndexFieldRefJson(schema, weight_field);
        }
        if (value.object.get("artifact")) |artifact| {
            if (artifact != .object) return error.InvalidTableIndexMetadata;
            if (artifact.object.get("field")) |field| try validateDerivedIndexFieldRefJson(schema, field);
            if (artifact.object.get("producer_json")) |producer| try validateProducerModelRefValue(producer);
        }
        if (value.object.get("context")) |context| {
            if (context != .object) return error.InvalidTableIndexMetadata;
            if (context.object.get("doc_fields")) |doc_fields| {
                if (doc_fields != .array) return error.InvalidTableIndexMetadata;
                for (doc_fields.array.items) |field| try validateDerivedIndexFieldRefJson(schema, field);
            }
        }
    }

    if (value.object.get("enrichments")) |enrichments| {
        if (enrichments != .array) return error.InvalidTableIndexMetadata;
        for (enrichments.array.items) |enrichment| {
            if (enrichment != .object) return error.InvalidTableIndexMetadata;
            if (enrichment.object.get("field")) |field| try validateDerivedIndexFieldRefJson(schema, field);
            if (enrichment.object.get("model")) |model| try validateModelRefValue(model);
        }
    }
}

fn validateEmbedderModelRefValue(value: std.json.Value) !void {
    if (value != .object) return error.InvalidTableIndexMetadata;
    if (value.object.get("model")) |model| try validateModelRefValue(model);
}

fn validateProducerModelRefValue(value: std.json.Value) !void {
    if (value != .object) return error.InvalidTableIndexMetadata;
    if (value.object.get("model")) |model| try validateModelRefValue(model);
}

fn validateModelRefValue(value: std.json.Value) !void {
    if (value != .string or value.string.len == 0) return error.InvalidTableIndexMetadata;
    for (value.string) |ch| {
        if (ch < 0x20 or ch == 0x7f) return error.InvalidTableIndexMetadata;
    }
}

fn validateRequiredDerivedIndexFieldRef(
    schema: runtime_schema_mod.TableSchema,
    object: std.json.ObjectMap,
    field_name: []const u8,
) !void {
    const value = object.get(field_name) orelse return error.InvalidTableIndexMetadata;
    try validateDerivedIndexFieldRefJson(schema, value);
}

fn validateDerivedIndexFieldRefJson(schema: runtime_schema_mod.TableSchema, value: std.json.Value) !void {
    if (value != .string) return error.InvalidTableIndexMetadata;
    try validateDerivedIndexFieldRef(schema, value.string);
}

fn validateOptionalDerivedIndexFieldRefJson(schema: runtime_schema_mod.TableSchema, value: std.json.Value) !void {
    if (value != .string) return error.InvalidTableIndexMetadata;
    if (std.mem.indexOfScalar(u8, value.string, '.') == null) return;
    try validateDerivedIndexFieldRef(schema, value.string);
}

fn validateDerivedIndexFieldRef(schema: runtime_schema_mod.TableSchema, field: []const u8) !void {
    if (!validRenderedFieldRef(field)) return error.InvalidTableIndexMetadata;
    const dot = std.mem.indexOfScalar(u8, field, '.');
    if (dot) |dot_index| {
        const root = field[0..dot_index];
        const suffix = field[dot_index + 1 ..];
        if (suffix.len == 0) return error.InvalidTableIndexMetadata;
        const column = findRuntimeRelationalColumn(schema, root) orelse {
            if (documentFieldRefAllowed(schema, field)) return;
            return error.InvalidTableIndexMetadata;
        };
        if (column.field_type != .json) return error.InvalidTableIndexMetadata;
        if (!documentFieldRefAllowed(schema, field)) return error.InvalidTableIndexMetadata;
        return;
    }
    if (findRuntimeRelationalColumn(schema, field) != null) return;
    if (documentFieldRefAllowed(schema, field)) return;
    return error.InvalidTableIndexMetadata;
}

fn findRuntimeRelationalColumn(schema: runtime_schema_mod.TableSchema, name: []const u8) ?runtime_schema_mod.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn documentFieldRefAllowed(schema: runtime_schema_mod.TableSchema, field: []const u8) bool {
    for (schema.full_text_documents) |document| {
        for (document.fields) |document_field| {
            if (std.mem.eql(u8, document_field.path, field)) return true;
        }
        for (document.open_dynamic_paths) |open_path| {
            if (pathFallsUnderOpenDynamicPath(field, open_path)) return true;
        }
        for (document.infer_type_dynamic_paths) |open_path| {
            if (pathFallsUnderOpenDynamicPath(field, open_path)) return true;
        }
    }
    return false;
}

fn pathFallsUnderOpenDynamicPath(field: []const u8, open_path: []const u8) bool {
    if (open_path.len == 0) return true;
    return std.mem.eql(u8, field, open_path) or
        (field.len > open_path.len and
            std.mem.eql(u8, field[0..open_path.len], open_path) and
            field[open_path.len] == '.');
}

fn validRenderedFieldRef(field: []const u8) bool {
    if (field.len == 0 or field[0] == '.' or field[field.len - 1] == '.') return false;
    var prev_dot = false;
    for (field) |ch| {
        if (ch == '.') {
            if (prev_dot) return false;
            prev_dot = true;
        } else {
            prev_dot = false;
        }
    }
    return true;
}

pub fn validatePublicAlgebraicIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidCreateTableRequest;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        try validatePublicAlgebraicIndexValue(entry.value_ptr.*);
    }
}

pub fn validatePublicAlgebraicIndexJson(alloc: std.mem.Allocator, index_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    try validatePublicAlgebraicIndexValue(parsed.value);
}

fn isSchemaDerivedAlgebraicIndex(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "algebraic")) return false;
    const derive_value = value.object.get("derive_from_schema") orelse return false;
    return derive_value == .bool and derive_value.bool;
}

fn validatePublicAlgebraicIndexValue(value: std.json.Value) !void {
    if (value != .object) return;
    const type_value = value.object.get("type") orelse return;
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (!std.mem.eql(u8, type_value.string, "algebraic")) return;

    const derive_value = value.object.get("derive_from_schema") orelse return error.InvalidCreateTableRequest;
    if (derive_value != .bool or !derive_value.bool) return error.InvalidCreateTableRequest;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (isAlgebraicInternalConfigField(entry.key_ptr.*)) return error.InvalidCreateTableRequest;
    }
}

fn schemaDerivedAlgebraicIndexValueAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema_json: []const u8,
    source: std.json.Value,
) !std.json.Value {
    const config_json = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, table_name, schema_json);
    defer alloc.free(config_json);
    var derived = try parseJsonValueAlloc(alloc, config_json);
    if (derived != .object) return error.InvalidCreateTableRequest;
    try derived.object.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, "algebraic") });

    var it = source.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "derive_from_schema")) continue;
        if (isAlgebraicInternalConfigField(entry.key_ptr.*)) continue;
        try derived.object.put(
            alloc,
            try alloc.dupe(u8, entry.key_ptr.*),
            try cloneJsonValueAlloc(alloc, entry.value_ptr.*),
        );
    }
    return derived;
}

/// Regenerate the schema-derived config for every algebraic index in
/// `indexes_json` from `schema_json`. Used on schema update so that a
/// dynamic-template change refreshes the durable algebraic `dynamic_field_rules`
/// (and capability fingerprint) without requiring the table to be recreated.
///
/// Public algebraic indexes are always schema-derived, so each is regenerated
/// in full; only the user-tunable runtime knobs (adaptive policy, planner/result
/// limits) are preserved from the stored config. Returns the original bytes when
/// there are no algebraic indexes to refresh.
pub fn regenerateAlgebraicIndexesFromSchemaAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    indexes_json: []const u8,
    schema_json: []const u8,
) ![]u8 {
    if (indexes_json.len == 0) return try alloc.dupe(u8, indexes_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return try alloc.dupe(u8, indexes_json),
    };

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var object = std.json.ObjectMap.empty;
    var changed = false;
    var it = root.iterator();
    while (it.next()) |entry| {
        const value = if (isAlgebraicIndexValue(entry.value_ptr.*)) blk: {
            if (schema_json.len == 0) return error.InvalidSchemaUpdateRequest;
            changed = true;
            break :blk try regenerateAlgebraicIndexValueAlloc(arena, table_name, schema_json, entry.value_ptr.*);
        } else try cloneJsonValueAlloc(arena, entry.value_ptr.*);
        try object.put(arena, try arena.dupe(u8, entry.key_ptr.*), value);
    }
    if (!changed) return try alloc.dupe(u8, indexes_json);
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = object }, .{ .emit_null_optional_fields = false });
}

fn isAlgebraicIndexValue(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, "algebraic");
}

/// Runtime knobs a user may tune on an algebraic index that are NOT derived from
/// the schema and must survive a regeneration.
fn isAlgebraicUserTunableField(field: []const u8) bool {
    const tunable = [_][]const u8{
        "adaptive",
        "pathfact_policy",
        "max_result_buckets",
        "max_planner_scan_rows",
        "max_batch_accumulator_entries",
        "min_max_candidate_cache_size",
        "enable_temporal_range_pruning",
        "hll_cardinalities",
    };
    for (tunable) |name| {
        if (std.mem.eql(u8, field, name)) return true;
    }
    return false;
}

fn regenerateAlgebraicIndexValueAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema_json: []const u8,
    source: std.json.Value,
) !std.json.Value {
    const config_json = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, table_name, schema_json);
    defer alloc.free(config_json);
    var derived = try parseJsonValueAlloc(alloc, config_json);
    if (derived != .object) return error.InvalidSchemaUpdateRequest;
    try derived.object.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, "algebraic") });

    // Schema-derived fields stay authoritative; only carry forward user knobs.
    var it = source.object.iterator();
    while (it.next()) |entry| {
        if (!isAlgebraicUserTunableField(entry.key_ptr.*)) continue;
        try derived.object.put(
            alloc,
            try alloc.dupe(u8, entry.key_ptr.*),
            try cloneJsonValueAlloc(alloc, entry.value_ptr.*),
        );
    }

    // If the schema-derived capability changed (static fields, dynamic templates,
    // or embedded JSON domains), existing documents have not been re-projected
    // through the new capability. Persist an index-level lifecycle marker so the
    // algebraic planner falls back to row scans until a rebuild refreshes the
    // facts. Dynamic-template and JSON-domain flags remain as more specific
    // diagnostics and field-level guards.
    const derived_fp = jsonStringField(derived, "capability_fingerprint") orelse "";
    const source_fp = jsonStringField(source, "capability_fingerprint") orelse "";
    const capability_changed = source_fp.len > 0 and !std.mem.eql(u8, derived_fp, source_fp);
    const source_lifecycle_pending = algebraicLifecyclePending(source);
    if (capability_changed or source_lifecycle_pending) {
        try derived.object.put(
            alloc,
            try alloc.dupe(u8, "capability_lifecycle_status"),
            .{ .string = try alloc.dupe(u8, "rebuild_required") },
        );
    }
    const source_pending = jsonBoolField(source, "dynamic_rules_backfill_pending") orelse false;
    const has_dynamic_rules = blk: {
        const rules = derived.object.get("dynamic_field_rules") orelse break :blk false;
        break :blk rules == .array and rules.array.items.len > 0;
    };
    if (has_dynamic_rules and (capability_changed or source_pending)) {
        try derived.object.put(alloc, try alloc.dupe(u8, "dynamic_rules_backfill_pending"), .{ .bool = true });
    }
    _ = try markJsonSubdocumentDomainLifecycles(alloc, &derived, source);
    return derived;
}

fn markJsonSubdocumentDomainLifecycles(
    alloc: std.mem.Allocator,
    derived: *std.json.Value,
    source: std.json.Value,
) !bool {
    if (derived.* != .object) return false;
    const domains = derived.object.getPtr("json_subdocument_domains") orelse return false;
    if (domains.* != .array) return false;

    var marked = false;
    for (domains.array.items) |*domain| {
        if (domain.* != .object) continue;
        const path = jsonStringField(domain.*, "path") orelse continue;
        const source_domain = findJsonSubdocumentDomain(source, path);
        const source_pending = if (source_domain) |existing| jsonDomainLifecyclePending(existing) else false;
        const source_fp = if (source_domain) |existing| jsonStringField(existing, "capability_fingerprint") orelse "" else "";
        const derived_fp = jsonStringField(domain.*, "capability_fingerprint") orelse "";
        const domain_changed = source_fp.len == 0 or !std.mem.eql(u8, source_fp, derived_fp);
        if (domain_changed or source_pending) {
            try domain.object.put(
                alloc,
                try alloc.dupe(u8, "lifecycle_status"),
                .{ .string = try alloc.dupe(u8, "rebuild_required") },
            );
            marked = true;
        }
    }
    return marked;
}

fn findJsonSubdocumentDomain(value: std.json.Value, path: []const u8) ?std.json.Value {
    if (value != .object) return null;
    const domains = value.object.get("json_subdocument_domains") orelse return null;
    if (domains != .array) return null;
    for (domains.array.items) |domain| {
        if (domain != .object) continue;
        const domain_path = jsonStringField(domain, "path") orelse continue;
        if (std.mem.eql(u8, domain_path, path)) return domain;
    }
    return null;
}

fn jsonDomainLifecyclePending(value: std.json.Value) bool {
    const status = jsonStringField(value, "lifecycle_status") orelse return false;
    return status.len != 0 and
        !std.mem.eql(u8, status, "current") and
        !std.mem.eql(u8, status, "compatible_additive");
}

fn algebraicLifecyclePending(value: std.json.Value) bool {
    const status = jsonStringField(value, "capability_lifecycle_status") orelse return false;
    return status.len != 0 and
        !std.mem.eql(u8, status, "current") and
        !std.mem.eql(u8, status, "compatible_additive");
}

fn jsonStringField(value: std.json.Value, key: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const field = value.object.get(key) orelse return null;
    return if (field == .string) field.string else null;
}

fn jsonBoolField(value: std.json.Value, key: []const u8) ?bool {
    if (value != .object) return null;
    const field = value.object.get(key) orelse return null;
    return if (field == .bool) field.bool else null;
}

fn isAlgebraicInternalConfigField(field: []const u8) bool {
    const internal_fields = [_][]const u8{
        "materializations",
        "group_fields",
        "measure_fields",
        "time_fields",
        "joins",
        "laws",
        "capability_fingerprint",
        "capability_lifecycle_status",
        "capability_change_added_fields",
        "capability_change_removed_fields",
        "capability_change_changed_type_fields",
        "json_subdocument_domains",
    };
    for (internal_fields) |internal| {
        if (std.mem.eql(u8, field, internal)) return true;
    }
    return false;
}

pub fn deriveTableRecord(table_name: []const u8, req: CreateTableRequest) metadata_table_manager.TableRecord {
    return deriveQualifiedTableRecord(default_database_name, default_namespace_name, table_name, req);
}

pub fn deriveQualifiedTableRecord(
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
    req: CreateTableRequest,
) metadata_table_manager.TableRecord {
    const min_ranges = req.num_shards orelse 1;
    return .{
        .table_id = deriveQualifiedTableId(database_name, namespace_name, table_name),
        .name = table_name,
        .database_name = database_name,
        .namespace_name = namespace_name,
        .description = req.description orelse "",
        .schema_json = effectiveSchemaJson(req.schema_json),
        .indexes_json = req.indexes_json orelse default_indexes_json,
        .replication_sources_json = req.replication_sources_json orelse "[]",
        .placement_role = "data",
        .tablespace_name = req.tablespace_name orelse "",
        .desired_replica_count = 3,
        .min_ranges = min_ranges,
    };
}

pub fn deriveTableId(table_name: []const u8) u64 {
    return deriveQualifiedTableId(default_database_name, default_namespace_name, table_name);
}

pub fn deriveQualifiedTableId(
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) u64 {
    if (isDefaultCatalogIdentity(database_name, namespace_name)) return deriveId(table_name, 0x54424c45);
    var hasher = std.hash.Wyhash.init(0x54424c45);
    updateCatalogQualifiedHasher(&hasher, database_name, namespace_name, table_name);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

pub fn deriveInitialRange(table: metadata_table_manager.TableRecord) metadata_table_manager.RangeRecord {
    const group_id = deriveQualifiedInitialDataGroupId(table.database_name, table.namespace_name, table.name);
    return .{
        .group_id = group_id,
        .range_id = group_id,
        .table_id = table.table_id,
        .start_key = "",
        .end_key = null,
    };
}

pub fn deriveInitialRanges(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
) ![]metadata_table_manager.RangeRecord {
    if (table.min_ranges <= 1) {
        const initial_range = deriveInitialRange(table);
        const out = try alloc.alloc(metadata_table_manager.RangeRecord, 1);
        out[0] = .{
            .group_id = initial_range.group_id,
            .range_id = initial_range.range_id,
            .table_id = table.table_id,
            .start_key = try alloc.dupe(u8, ""),
            .end_key = null,
        };
        return out;
    }

    const shard_count = table.min_ranges;
    const out = try alloc.alloc(metadata_table_manager.RangeRecord, shard_count);

    var i: u32 = 0;
    errdefer {
        var cleanup_index: u32 = 0;
        while (cleanup_index < i) : (cleanup_index += 1) {
            metadata_table_manager.freeRange(alloc, out[cleanup_index]);
        }
        alloc.free(out);
    }
    while (i < shard_count) : (i += 1) {
        const start_key = if (i == 0)
            try alloc.dupe(u8, "")
        else
            try deriveShardBoundaryKey(alloc, i, shard_count);
        errdefer alloc.free(start_key);

        const end_key = if (i + 1 == shard_count)
            null
        else
            try deriveShardBoundaryKey(alloc, i + 1, shard_count);
        errdefer if (end_key) |value| alloc.free(value);

        const group_id = deriveQualifiedShardGroupId(table.database_name, table.namespace_name, table.name, i);
        out[i] = .{
            .group_id = group_id,
            .range_id = group_id,
            .table_id = table.table_id,
            .start_key = start_key,
            .end_key = end_key,
        };
    }

    return out;
}

pub fn parseSchemaUpdateRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    return try schema_mod.parseSchemaUpdateRequest(alloc, body);
}

pub fn parseValidatedTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !ParsedTableSchema {
    return try schema_mod.parseValidatedTableSchema(alloc, schema_json);
}

pub fn validateBatchWritesAgainstTableSchema(
    alloc: std.mem.Allocator,
    schema: ParsedTableSchema,
    writes: []const db_mod.types.BatchWrite,
) !void {
    try schema_mod.validateBatchWritesAgainstTableSchema(alloc, schema, writes);
}

pub fn validateWritesAgainstTableSchema(
    alloc: std.mem.Allocator,
    schema: ParsedTableSchema,
    writes: anytype,
) !void {
    try schema_mod.validateWritesAgainstTableSchema(alloc, schema, writes);
}

pub fn deriveRuntimeTableSchema(alloc: std.mem.Allocator, schema: ParsedTableSchema) !@import("../storage/schema.zig").TableSchema {
    return try schema_mod.deriveRuntimeTableSchema(alloc, schema);
}

/// True if a raw table-schema JSON declares `storage_mode: "relational"`.
fn schemaJsonIsRelational(alloc: std.mem.Allocator, schema_json: []const u8) bool {
    if (schema_json.len == 0) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const value = parsed.value.object.get("storage_mode") orelse return false;
    return value == .string and std.mem.eql(u8, value.string, "relational");
}

/// True if a table's index set already contains an algebraic index.
fn indexesJsonHasAlgebraicIndex(alloc: std.mem.Allocator, indexes_json: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const type_value = entry.value_ptr.object.get("type") orelse continue;
        if (type_value == .string and std.mem.eql(u8, type_value.string, "algebraic")) return true;
    }
    return false;
}

/// Ensure a relational table has an aggregation index: if the new schema is
/// relational and no algebraic index exists yet, add a schema-derived one
/// (`derive_from_schema: true`). Returns an updated indexes_json the caller owns
/// (still carrying the unexpanded marker), or null if no change is needed
/// (document mode, or an algebraic index already present).
fn ensureRelationalAlgebraicIndexAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    schema_json: []const u8,
) !?[]u8 {
    if (!schemaJsonIsRelational(alloc, schema_json)) return null;
    if (indexesJsonHasAlgebraicIndex(alloc, indexes_json)) return null;
    return try indexes_api.addIndexToTableIndexesJson(
        alloc,
        indexes_json,
        default_relational_algebraic_index_name,
        "{\"type\":\"algebraic\",\"derive_from_schema\":true}",
    );
}

pub fn applySchemaUpdateRecord(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    schema_json: []const u8,
) !metadata_table_manager.TableRecord {
    try validateRelationalStorageModeUpdateAlloc(alloc, table.schema_json, schema_json);

    var updated = try metadata_table_manager.cloneTable(alloc, table.*);
    errdefer metadata_table_manager.freeTable(alloc, updated);

    const current_version = try schemaVersion(table.schema_json);
    const doc_schemas_changed = try documentSchemasChanged(alloc, table.schema_json, schema_json);
    const next_version = if (doc_schemas_changed) current_version + 1 else current_version;

    const normalized_schema_json = try normalizeSchemaVersion(alloc, schema_json, next_version);
    alloc.free(updated.schema_json);
    updated.schema_json = normalized_schema_json;

    // Refresh schema-derived algebraic configs (dynamic_field_rules + capability
    // fingerprint) on every update, including template-only changes that do not
    // bump the version, so the algebraic sidecar tracks dynamic templates without
    // a recreate.
    const refreshed_indexes_json = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, table.name, updated.indexes_json, updated.schema_json);
    alloc.free(updated.indexes_json);
    updated.indexes_json = refreshed_indexes_json;

    if (doc_schemas_changed) {
        if (table.read_schema_json.len == 0) {
            const normalized_read_schema_json = if (table.schema_json.len > 0)
                try normalizeSchemaVersion(alloc, table.schema_json, current_version)
            else
                try normalizeSchemaVersion(alloc, "{}", 0);
            alloc.free(updated.read_schema_json);
            updated.read_schema_json = normalized_read_schema_json;
        }

        const next_indexes_json = try upsertVersionedFullTextIndex(alloc, updated.indexes_json, current_version, next_version);
        alloc.free(updated.indexes_json);
        updated.indexes_json = next_indexes_json;
    }

    // Relational tables get an auto-created schema-derived algebraic index for
    // aggregation pushdown. Done last, on the (possibly full-text-migrated)
    // index set, and independently of doc-schema version changes so a table
    // that already uses relational storage gets one; idempotent if an algebraic
    // index already exists. Storage-mode switches are rejected until an explicit
    // row migration path exists. The injected marker is expanded against the
    // schema so the stored indexes_json carries the concrete derived config,
    // provisioned like any explicit algebraic index.
    const prepared_indexes_json = try prepareTableIndexesForSchemaAlloc(alloc, table.name, updated.indexes_json, updated.schema_json);
    alloc.free(updated.indexes_json);
    updated.indexes_json = prepared_indexes_json;

    return updated;
}

pub fn applyRelationalSqlDdlToTableRecordAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    sql: []const u8,
) !AppliedRelationalSqlDdlRecord {
    return try applyRelationalSqlDdlToTableRecordWithSessionAlloc(alloc, table, sql, catalog_resources.SqlCatalogSession.default());
}

pub fn applyRelationalSqlDdlToTableRecordWithSessionAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    sql: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !AppliedRelationalSqlDdlRecord {
    return try applyRelationalSqlDdlToTableRecordWithSessionAndFunctionBindingsAlloc(alloc, table, sql, session, .{});
}

pub fn applyRelationalSqlDdlToTableRecordWithSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    sql: []const u8,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !AppliedRelationalSqlDdlRecord {
    var plan = try sql_adapter.lowerDdlPlanWithFunctionBindingsAlloc(alloc, sql, function_bindings);
    defer plan.deinit(alloc);
    return try applyRelationalSqlDdlPlanToTableRecordWithSessionAlloc(alloc, table, &plan, session);
}

pub fn applyRelationalSqlDdlPlanToTableRecordWithSessionAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    plan: *sql_adapter.LoweredDdlPlan,
    session: catalog_resources.SqlCatalogSession,
) !AppliedRelationalSqlDdlRecord {
    if (try relationalSqlDdlPlanTableRefWithSessionAlloc(alloc, plan.*, session)) |table_ref_value| {
        var table_ref = table_ref_value;
        defer table_ref.deinit(alloc);
        if (!tableCatalogIdentityMatches(table.*, table_ref.database_name, table_ref.namespace_name, table_ref.table_name)) return error.InvalidSchemaUpdateRequest;
        try retargetRelationalSqlDdlPlanTableNameAlloc(alloc, plan, table.name);
    }

    switch (plan.*) {
        .create_index => |create_index| {
            if (create_index.derived_index_config_json) |index_json| {
                return try applyRelationalDerivedIndexCreateToTableRecordAlloc(alloc, table, create_index, index_json);
            }
        },
        .drop_index => |drop_index| {
            if (try tableIndexesJsonContainsIndex(alloc, table.indexes_json, drop_index.index_name)) {
                return try applyRelationalDerivedIndexDropToTableRecordAlloc(alloc, table, drop_index.index_name);
            }
        },
        else => {},
    }

    const current_schema_json: []const u8 = switch (plan.*) {
        .create_table => blk: {
            if (table.schema_json.len != 0) return error.InvalidSchemaUpdateRequest;
            break :blk "";
        },
        .drop_table => return error.UnsupportedSqlShape,
        .create_index, .drop_index, .alter_table, .create_update_policy => table.schema_json,
        else => return error.UnsupportedSqlShape,
    };
    var applied = try sql_adapter.applyDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, plan.*);
    defer applied.deinit(alloc);

    const updated = try applyRelationalDdlSchemaRecordAlloc(alloc, table, applied.schema_json);
    const work_items = applied.work_items;
    applied.work_items = &.{};
    return .{
        .table = updated,
        .requires_rebuild = applied.requires_rebuild,
        .validation_required = applied.validation_required,
        .rewrite_required = applied.rewrite_required,
        .work_items = work_items,
    };
}

pub fn applyUntargetedRelationalDerivedIndexDdlOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: *sql_adapter.LoweredDdlPlan,
    session: catalog_resources.SqlCatalogSession,
) !?AppliedRelationalSqlDdlRecord {
    const create_index = switch (plan.*) {
        .create_index => |value| value,
        else => return null,
    };
    if (create_index.table_name.len != 0) return null;
    const index_json = create_index.derived_index_config_json orelse return null;
    const graph_index_name = try graphMetricGraphIndexNameFromConfigAlloc(alloc, index_json) orelse return null;
    defer alloc.free(graph_index_name);
    const table = (try findTableContainingIndexConfigAlloc(alloc, snapshot, graph_index_name)) orelse return error.TableNotFound;

    var applied = try applyRelationalSqlDdlPlanToTableRecordWithSessionAlloc(
        alloc,
        table,
        plan,
        session,
    );
    errdefer applied.deinit(alloc);
    try svc.upsertTable(applied.table);
    return applied;
}

fn graphMetricGraphIndexNameFromConfigAlloc(
    alloc: std.mem.Allocator,
    index_json: []const u8,
) !?[]const u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTableIndexMetadata;
    const type_value = parsed.value.object.get("type") orelse return null;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "graph_metric")) return null;
    const graph_index_value = parsed.value.object.get("graph_index") orelse return error.InvalidTableIndexMetadata;
    if (graph_index_value != .string) return error.InvalidTableIndexMetadata;
    return try alloc.dupe(u8, graph_index_value.string);
}

fn findTableContainingIndexConfigAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    index_name: []const u8,
) !?*const metadata_table_manager.TableRecord {
    var match: ?*const metadata_table_manager.TableRecord = null;
    for (snapshot.tables) |*table| {
        if (try indexes_api.hasIndexConfig(alloc, table.indexes_json, index_name)) {
            if (match != null) return error.InvalidTableIndexMetadata;
            match = table;
        }
    }
    return match;
}

pub fn applyRelationalDerivedIndexDdlOnServiceWithPlanAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    table: *const metadata_table_manager.TableRecord,
    target: RelationalSqlDdlTarget,
    plan: sql_adapter.LoweredDdlPlan,
) !?AppliedRelationalSqlDdlRecord {
    _ = target;
    const create_index = switch (plan) {
        .create_index => |value| value,
        else => return null,
    };
    const index_json = create_index.derived_index_config_json orelse return null;
    if (try indexes_api.hasIndexConfig(alloc, table.indexes_json, create_index.index_name)) {
        if (!create_index.if_not_exists) return error.InvalidTableIndexMetadata;
        return .{
            .table = try metadata_table_manager.cloneTable(alloc, table.*),
            .noop = true,
        };
    }

    try validateDerivedIndexFieldRefsForSchemaAlloc(alloc, index_json, table.schema_json);
    const expanded_index_json = try expandSchemaDerivedAlgebraicIndexAlloc(alloc, table.name, index_json, table.schema_json);
    defer alloc.free(expanded_index_json);

    var updated_record = table.*;
    updated_record.indexes_json = try indexes_api.addIndexToTableIndexesJson(alloc, table.indexes_json, create_index.index_name, expanded_index_json);
    defer alloc.free(updated_record.indexes_json);
    try svc.upsertTable(updated_record);

    return .{
        .table = try metadata_table_manager.cloneTable(alloc, updated_record),
        .requires_rebuild = true,
    };
}

fn tableIndexesJsonContainsIndex(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !bool {
    const source = if (indexes_json.len > 0) indexes_json else "{}";
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    return switch (parsed.value) {
        .object => |object| object.contains(index_name),
        else => error.InvalidTableIndexMetadata,
    };
}

fn applyRelationalDerivedIndexCreateToTableRecordAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    plan: sql_adapter.CreateIndexPlan,
    index_json: []const u8,
) !AppliedRelationalSqlDdlRecord {
    if (table.schema_json.len == 0) return error.InvalidSchemaUpdateRequest;
    if (try tableIndexesJsonContainsIndex(alloc, table.indexes_json, plan.index_name)) {
        if (plan.if_not_exists) {
            return .{
                .table = try metadata_table_manager.cloneTable(alloc, table.*),
                .noop = true,
            };
        }
        return error.InvalidSqlCatalog;
    }
    try validateDerivedIndexFieldRefsForSchemaAlloc(alloc, index_json, table.schema_json);
    try validateDerivedIndexCatalogRefsForTableIndexesAlloc(alloc, index_json, table.indexes_json);

    const next_indexes_json = try indexes_api.addIndexToTableIndexesJson(alloc, table.indexes_json, plan.index_name, index_json);
    defer alloc.free(next_indexes_json);
    const prepared_indexes_json = try prepareTableIndexesForSchemaAlloc(alloc, table.name, next_indexes_json, table.schema_json);
    defer alloc.free(prepared_indexes_json);

    var updated = try metadata_table_manager.cloneTable(alloc, table.*);
    errdefer metadata_table_manager.freeTable(alloc, updated);
    alloc.free(updated.indexes_json);
    updated.indexes_json = try alloc.dupe(u8, prepared_indexes_json);

    return .{
        .table = updated,
        .requires_rebuild = true,
        .work_items = try sql_adapter.appliedDdlTableWorkItemsForFlagsAlloc(alloc, true, false, false),
    };
}

fn applyRelationalDerivedIndexDropToTableRecordAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !AppliedRelationalSqlDdlRecord {
    try validateDerivedIndexHasNoDependentsAlloc(alloc, table.indexes_json, index_name);
    const next_indexes_json = (try indexes_api.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.InvalidSqlCatalog;
    defer alloc.free(next_indexes_json);
    const prepared_indexes_json = try prepareTableIndexesForSchemaAlloc(alloc, table.name, next_indexes_json, table.schema_json);
    defer alloc.free(prepared_indexes_json);

    var updated = try metadata_table_manager.cloneTable(alloc, table.*);
    errdefer metadata_table_manager.freeTable(alloc, updated);
    alloc.free(updated.indexes_json);
    updated.indexes_json = try alloc.dupe(u8, prepared_indexes_json);

    return .{
        .table = updated,
        .requires_rebuild = true,
        .work_items = try sql_adapter.appliedDdlTableWorkItemsForFlagsAlloc(alloc, true, false, false),
    };
}

fn validateDerivedIndexHasNoDependentsAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !void {
    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len > 0) indexes_json else "{}", .{});
    defer parsed_indexes.deinit();
    const indexes = switch (parsed_indexes.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    var it = indexes.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, index_name)) continue;
        if (entry.value_ptr.* != .object) continue;
        const type_name = requiredJsonStringField(entry.value_ptr.object, "type") catch continue;
        if (std.mem.eql(u8, type_name, "graph_metric")) {
            const graph_index = requiredJsonStringField(entry.value_ptr.object, "graph_index") catch continue;
            if (std.mem.eql(u8, graph_index, index_name)) return error.InvalidTableIndexMetadata;
        } else if (std.mem.eql(u8, type_name, "hybrid")) {
            const sources = entry.value_ptr.object.get("sources") orelse continue;
            if (sources != .array) continue;
            for (sources.array.items) |source| {
                if (source == .string and std.mem.eql(u8, source.string, index_name)) return error.InvalidTableIndexMetadata;
            }
        }
    }
}

fn validateDerivedIndexCatalogRefsForTableIndexesAlloc(
    alloc: std.mem.Allocator,
    index_json: []const u8,
    indexes_json: []const u8,
) !void {
    var parsed_index = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed_index.deinit();
    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len > 0) indexes_json else "{}", .{});
    defer parsed_indexes.deinit();

    if (parsed_index.value != .object) return error.InvalidTableIndexMetadata;
    const indexes = switch (parsed_indexes.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const type_name = try requiredJsonStringField(parsed_index.value.object, "type");
    if (std.mem.eql(u8, type_name, "graph_metric")) {
        const graph_index = try requiredJsonStringField(parsed_index.value.object, "graph_index");
        const graph_config = indexes.get(graph_index) orelse return error.InvalidTableIndexMetadata;
        try validateIndexConfigType(graph_config, "graph");
        const metric = try requiredJsonStringField(parsed_index.value.object, "metric");
        try validateGraphMetricName(metric);
        if (parsed_index.value.object.get("algorithm")) |algorithm| {
            if (algorithm != .string) return error.InvalidTableIndexMetadata;
            try validateGraphMetricAlgorithm(algorithm.string);
        }
        if (parsed_index.value.object.get("metric_freshness")) |freshness| try validateGraphMetricFreshnessValue(freshness);
        if (parsed_index.value.object.get("publish")) |publish| try validateGraphMetricPublishValue(publish);
    } else if (std.mem.eql(u8, type_name, "hybrid")) {
        const sources = parsed_index.value.object.get("sources") orelse return error.InvalidTableIndexMetadata;
        if (sources != .array) return error.InvalidTableIndexMetadata;
        if (sources.array.items.len == 0) return error.InvalidTableIndexMetadata;
        for (sources.array.items) |source| {
            if (source != .string) return error.InvalidTableIndexMetadata;
            const source_config = indexes.get(source.string) orelse return error.InvalidTableIndexMetadata;
            try validateHybridSourceIndexConfigType(source_config);
        }
    }
}

fn validateGraphEdgePolicyValue(value: std.json.Value) !void {
    if (value != .string) return error.InvalidTableIndexMetadata;
    if (std.ascii.eqlIgnoreCase(value.string, "all")) return;
    return error.InvalidTableIndexMetadata;
}

fn validateGraphMetricName(metric: []const u8) !void {
    if (!validRenderedFieldRef(metric)) return error.InvalidTableIndexMetadata;
}

fn validateGraphMetricAlgorithm(algorithm: []const u8) !void {
    if (std.ascii.eqlIgnoreCase(algorithm, "degree")) return;
    if (std.ascii.eqlIgnoreCase(algorithm, "pagerank")) return;
    if (std.ascii.eqlIgnoreCase(algorithm, "eigenvector")) return;
    if (std.ascii.eqlIgnoreCase(algorithm, "eigenvector_centrality")) return;
    if (std.ascii.eqlIgnoreCase(algorithm, "hits")) return;
    if (std.ascii.eqlIgnoreCase(algorithm, "authority")) return;
    if (std.ascii.eqlIgnoreCase(algorithm, "hub")) return;
    return error.InvalidTableIndexMetadata;
}

fn validateGraphMetricFreshnessValue(value: std.json.Value) !void {
    if (value != .string) return error.InvalidTableIndexMetadata;
    if (std.ascii.eqlIgnoreCase(value.string, "published")) return;
    if (std.ascii.eqlIgnoreCase(value.string, "fresh")) return;
    return error.InvalidTableIndexMetadata;
}

fn validateGraphMetricPublishValue(value: std.json.Value) !void {
    if (value != .string) return error.InvalidTableIndexMetadata;
    if (std.ascii.eqlIgnoreCase(value.string, "after_max_iterations")) return;
    if (std.ascii.eqlIgnoreCase(value.string, "on_convergence")) return;
    if (std.ascii.eqlIgnoreCase(value.string, "never")) return;
    return error.InvalidTableIndexMetadata;
}

fn requiredJsonStringField(object: std.json.ObjectMap, field_name: []const u8) ![]const u8 {
    const value = object.get(field_name) orelse return error.InvalidTableIndexMetadata;
    return switch (value) {
        .string => |string| string,
        else => error.InvalidTableIndexMetadata,
    };
}

fn validateIndexConfigType(config: std.json.Value, expected_type: []const u8) !void {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const type_name = try requiredJsonStringField(config.object, "type");
    if (!std.mem.eql(u8, type_name, expected_type)) return error.InvalidTableIndexMetadata;
}

fn validateHybridSourceIndexConfigType(config: std.json.Value) !void {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const type_name = try requiredJsonStringField(config.object, "type");
    if (std.mem.eql(u8, type_name, "full_text")) return;
    if (std.mem.eql(u8, type_name, "embeddings")) return;
    if (std.mem.eql(u8, type_name, "graph_metric")) return;
    return error.InvalidTableIndexMetadata;
}

pub fn relationalSqlDdlTargetAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
) !RelationalSqlDdlTarget {
    return try relationalSqlDdlTargetWithSessionAlloc(alloc, sql, catalog_resources.SqlCatalogSession.default());
}

pub fn relationalSqlDdlTargetWithSessionAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlDdlTarget {
    return try relationalSqlDdlTargetWithSessionAndFunctionBindingsAlloc(alloc, sql, session, .{});
}

pub fn relationalSqlDdlTargetWithSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !RelationalSqlDdlTarget {
    var plan = try sql_adapter.lowerDdlPlanWithFunctionBindingsAlloc(alloc, sql, function_bindings);
    defer plan.deinit(alloc);
    return try relationalSqlDdlTargetForPlanWithSessionAlloc(alloc, plan, session);
}

pub fn relationalSqlDdlTargetForPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    plan: sql_adapter.LoweredDdlPlan,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlDdlTarget {
    var table_ref = (try relationalSqlDdlPlanTableRefWithSessionAlloc(alloc, plan, session)) orelse return error.UnsupportedSqlShape;
    errdefer table_ref.deinit(alloc);
    return .{
        .database_name = table_ref.database_name,
        .namespace_name = table_ref.namespace_name,
        .table_name = table_ref.table_name,
        .action = switch (plan) {
            .create_table => .create_table,
            .drop_table => .drop_table,
            .create_index, .drop_index, .alter_table, .create_update_policy => .update_table,
            else => return error.UnsupportedSqlShape,
        },
        .if_exists = switch (plan) {
            .drop_table => |drop_table| drop_table.if_exists,
            else => false,
        },
        .cascade = switch (plan) {
            .drop_table => |drop_table| drop_table.cascade,
            else => false,
        },
    };
}

fn relationalSqlDdlPlanTableRefAlloc(
    alloc: std.mem.Allocator,
    plan: sql_adapter.LoweredDdlPlan,
) !?RelationalSqlDdlTableRef {
    return try relationalSqlDdlPlanTableRefWithSessionAlloc(alloc, plan, catalog_resources.SqlCatalogSession.default());
}

fn relationalSqlDdlPlanTableRefWithSessionAlloc(
    alloc: std.mem.Allocator,
    plan: sql_adapter.LoweredDdlPlan,
    session: catalog_resources.SqlCatalogSession,
) !?RelationalSqlDdlTableRef {
    const table_name = relationalSqlDdlPlanTableName(plan) orelse return null;
    return try parseRelationalSqlDdlTableRefWithSessionAlloc(alloc, table_name, session);
}

fn relationalSqlDdlPlanTableName(plan: sql_adapter.LoweredDdlPlan) ?[]const u8 {
    return switch (plan) {
        .create_table => |create_table| create_table.table_name,
        .create_index => |create_index| if (create_index.table_name.len == 0) null else create_index.table_name,
        .drop_index => null,
        .drop_table => |drop_table| drop_table.table_name,
        .alter_table => |alter_table| alter_table.table_name,
        .create_update_policy => |update_policy| update_policy.table_name,
        else => null,
    };
}

fn parseRelationalSqlDdlTableRefAlloc(
    alloc: std.mem.Allocator,
    raw_table_name: []const u8,
) !RelationalSqlDdlTableRef {
    return try parseRelationalSqlDdlTableRefWithSessionAlloc(alloc, raw_table_name, catalog_resources.SqlCatalogSession.default());
}

fn parseRelationalSqlDdlTableRefWithSessionAlloc(
    alloc: std.mem.Allocator,
    raw_table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlDdlTableRef {
    const target = try session.tableTargetFromObjectName(raw_table_name);
    const owned_database_name = try alloc.dupe(u8, target.database_name);
    errdefer alloc.free(owned_database_name);
    const owned_namespace_name = try alloc.dupe(u8, target.namespace_name);
    errdefer alloc.free(owned_namespace_name);
    const owned_table_name = try alloc.dupe(u8, target.table_name);
    errdefer alloc.free(owned_table_name);
    return .{
        .database_name = owned_database_name,
        .namespace_name = owned_namespace_name,
        .table_name = owned_table_name,
    };
}

fn retargetRelationalSqlDdlPlanTableNameAlloc(
    alloc: std.mem.Allocator,
    plan: *sql_adapter.LoweredDdlPlan,
    table_name: []const u8,
) !void {
    const owned = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned);
    switch (plan.*) {
        .create_table => |*create_table| {
            alloc.free(create_table.table_name);
            create_table.table_name = owned;
        },
        .create_index => |*create_index| {
            alloc.free(create_index.table_name);
            create_index.table_name = owned;
        },
        .alter_table => |*alter_table| {
            alloc.free(alter_table.table_name);
            alter_table.table_name = owned;
        },
        .create_update_policy => |*update_policy| {
            alloc.free(update_policy.table_name);
            update_policy.table_name = owned;
        },
        .drop_table => |*drop_table| {
            alloc.free(drop_table.table_name);
            drop_table.table_name = owned;
        },
        else => {
            alloc.free(owned);
        },
    }
}

pub fn validateRelationalSqlDdlNamespace(
    snapshot: *const metadata_api.AdminSnapshot,
    target: RelationalSqlDdlTarget,
) !void {
    if (findDatabaseByName(snapshot, target.database_name) == null) return error.DatabaseNotFound;
    if (findNamespaceByName(snapshot, target.database_name, target.namespace_name) == null) return error.NamespaceNotFound;
}

pub fn deriveRelationalSqlDdlTargetTableRecord(target: RelationalSqlDdlTarget) metadata_table_manager.TableRecord {
    return .{
        .table_id = deriveQualifiedTableId(target.database_name, target.namespace_name, target.table_name),
        .name = target.table_name,
        .database_name = target.database_name,
        .namespace_name = target.namespace_name,
        .schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
        .desired_replica_count = 3,
        .min_ranges = 1,
    };
}

fn applyRelationalDdlSchemaRecordAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    schema_json: []const u8,
) !metadata_table_manager.TableRecord {
    var validated = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer validated.deinit(alloc);
    if (validated.storage_mode != .relational) return error.InvalidSchemaUpdateRequest;

    var updated = try metadata_table_manager.cloneTable(alloc, table.*);
    errdefer metadata_table_manager.freeTable(alloc, updated);

    const current_version = try schemaVersion(table.schema_json);
    const doc_schemas_changed = try documentSchemasChanged(alloc, table.schema_json, schema_json);
    const next_version = if (table.schema_json.len == 0) 0 else if (doc_schemas_changed) current_version + 1 else current_version;

    const normalized_schema_json = try normalizeSchemaVersion(alloc, schema_json, next_version);
    alloc.free(updated.schema_json);
    updated.schema_json = normalized_schema_json;

    const refreshed_indexes_json = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, table.name, updated.indexes_json, updated.schema_json);
    alloc.free(updated.indexes_json);
    updated.indexes_json = refreshed_indexes_json;

    if (doc_schemas_changed and table.schema_json.len > 0) {
        if (table.read_schema_json.len == 0) {
            const normalized_read_schema_json = try normalizeSchemaVersion(alloc, table.schema_json, current_version);
            alloc.free(updated.read_schema_json);
            updated.read_schema_json = normalized_read_schema_json;
        }

        const next_indexes_json = try upsertVersionedFullTextIndex(alloc, updated.indexes_json, current_version, next_version);
        alloc.free(updated.indexes_json);
        updated.indexes_json = next_indexes_json;
    }

    const prepared_indexes_json = try prepareTableIndexesForSchemaAlloc(alloc, table.name, updated.indexes_json, updated.schema_json);
    alloc.free(updated.indexes_json);
    updated.indexes_json = prepared_indexes_json;

    return updated;
}

fn validateRelationalStorageModeUpdateAlloc(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    next_schema_json: []const u8,
) !void {
    var current_parsed = try schema_mod.parseValidatedTableSchema(alloc, effectiveSchemaJson(if (current_schema_json.len > 0) current_schema_json else null));
    defer current_parsed.deinit(alloc);
    var next_parsed = try schema_mod.parseValidatedTableSchema(alloc, next_schema_json);
    defer next_parsed.deinit(alloc);

    if (current_parsed.storage_mode != next_parsed.storage_mode) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (next_parsed.storage_mode != .relational) return;

    const current_runtime = try schema_mod.deriveRuntimeTableSchema(alloc, current_parsed);
    defer runtime_schema_mod.freeSchema(alloc, current_runtime);
    const next_runtime = try schema_mod.deriveRuntimeTableSchema(alloc, next_parsed);
    defer runtime_schema_mod.freeSchema(alloc, next_runtime);

    if (!runtime_schema_mod.relationalColumnCatalogsEqual(current_runtime.relational_columns, next_runtime.relational_columns)) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (!runtime_schema_mod.relationalCheckCatalogsEqual(current_runtime.checks, next_runtime.checks)) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (current_runtime.system_versioned != next_runtime.system_versioned) {
        return error.InvalidSchemaUpdateRequest;
    }
    try validateConstraintCatalogTransition(current_runtime, next_runtime);
}

fn validateConstraintCatalogTransition(current_runtime: runtime_schema_mod.TableSchema, next_runtime: runtime_schema_mod.TableSchema) !void {
    if (!primaryKeysEqual(current_runtime.primary_key, next_runtime.primary_key)) return error.InvalidSchemaUpdateRequest;
    for (current_runtime.unique_constraints) |constraint| {
        if (findUniqueConstraintByName(next_runtime.unique_constraints, constraint.name)) |next_constraint| {
            if (!uniqueConstraintsEqual(constraint, next_constraint)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_runtime.unique_constraints) |constraint| {
        if (findUniqueConstraintByName(current_runtime.unique_constraints, constraint.name)) |current_constraint| {
            if (!uniqueConstraintsEqual(current_constraint, constraint)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (current_runtime.foreign_keys) |foreign_key| {
        if (findForeignKeyByName(next_runtime.foreign_keys, foreign_key.name)) |next_foreign_key| {
            if (!foreignKeysSameDefinition(foreign_key, next_foreign_key)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_runtime.foreign_keys) |foreign_key| {
        if (findForeignKeyByName(current_runtime.foreign_keys, foreign_key.name)) |current_foreign_key| {
            if (!foreignKeysSameDefinition(current_foreign_key, foreign_key)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn primaryKeysEqual(a: ?runtime_schema_mod.PrimaryKey, b: ?runtime_schema_mod.PrimaryKey) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return optionalStringsEqual(a.?.name, b.?.name) and
        stringSlicesEqual(a.?.columns, b.?.columns) and
        stringSlicesEqual(a.?.include_columns, b.?.include_columns) and
        optionalStringsEqual(a.?.without_overlaps_period, b.?.without_overlaps_period) and
        a.?.deferrable == b.?.deferrable and
        a.?.timing == b.?.timing;
}

fn findUniqueConstraintByName(unique_constraints: []const runtime_schema_mod.UniqueConstraint, name: []const u8) ?runtime_schema_mod.UniqueConstraint {
    for (unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

fn findForeignKeyByName(foreign_keys: []const runtime_schema_mod.ForeignKey, name: []const u8) ?runtime_schema_mod.ForeignKey {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return foreign_key;
    }
    return null;
}

fn uniqueConstraintsEqual(a: runtime_schema_mod.UniqueConstraint, b: runtime_schema_mod.UniqueConstraint) bool {
    return std.mem.eql(u8, a.name, b.name) and
        stringSlicesEqual(a.columns, b.columns) and
        uniqueExpressionSlicesEqual(a.expressions, b.expressions) and
        stringSlicesEqual(a.include_columns, b.include_columns) and
        optionalStringsEqual(a.without_overlaps_period, b.without_overlaps_period) and
        a.nulls_not_distinct == b.nulls_not_distinct and
        a.deferrable == b.deferrable and
        a.timing == b.timing and
        uniquePredicateSlicesEqual(a.where, b.where) and
        relationalRowsExpressionConditionSlicesEqual(a.where_expressions, b.where_expressions);
}

fn foreignKeysSameDefinition(a: runtime_schema_mod.ForeignKey, b: runtime_schema_mod.ForeignKey) bool {
    return std.mem.eql(u8, a.name, b.name) and
        stringSlicesEqual(a.child_columns, b.child_columns) and
        optionalStringsEqual(a.child_period, b.child_period) and
        std.mem.eql(u8, a.parent_table, b.parent_table) and
        stringSlicesEqual(a.parent_columns, b.parent_columns) and
        optionalStringsEqual(a.parent_period, b.parent_period) and
        a.on_delete == b.on_delete and
        a.on_update == b.on_update and
        a.timing == b.timing and
        a.deferrable == b.deferrable and
        a.match == b.match;
}

test "foreign key definition equality includes SQL compatibility fields" {
    const base = runtime_schema_mod.ForeignKey{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .restrict,
        .on_update = .restrict,
        .timing = .immediate,
        .match = .simple,
    };

    var changed_update = base;
    changed_update.on_update = .no_action;
    try std.testing.expect(!foreignKeysSameDefinition(base, changed_update));

    var changed_match = base;
    changed_match.match = .full;
    try std.testing.expect(!foreignKeysSameDefinition(base, changed_match));

    var changed_deferrable = base;
    changed_deferrable.deferrable = true;
    try std.testing.expect(!foreignKeysSameDefinition(base, changed_deferrable));
}

fn foreignKeyValidationStateString(state: runtime_schema_mod.ForeignKeyValidationState) ![]const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating, .invalid => error.InvalidSchemaUpdateRequest,
    };
}

fn uniqueConstraintValidationStateString(state: runtime_schema_mod.UniqueConstraintValidationState) ![]const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating, .invalid => error.InvalidSchemaUpdateRequest,
    };
}

pub fn schemaWithForeignKeyValidationStateAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    constraint_name: []const u8,
    state: runtime_schema_mod.ForeignKeyValidationState,
) ![]u8 {
    const state_text = try foreignKeyValidationStateString(state);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const foreign_keys = root.getPtr("foreign_keys") orelse return error.ForeignKeyNotFound;
    const foreign_key_items = switch (foreign_keys.*) {
        .array => |*array| array.items,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var found = false;
    for (foreign_key_items) |*foreign_key| {
        const object = switch (foreign_key.*) {
            .object => |*object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string) return error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, name.string, constraint_name)) continue;

        const validation_state = object.getPtr("validation_state") orelse return error.InvalidSchemaUpdateRequest;
        validation_state.* = .{ .string = state_text };
        found = true;
        break;
    }
    if (!found) return error.ForeignKeyNotFound;

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    validated.deinit(alloc);
    return updated;
}

pub fn schemaWithUniqueConstraintValidationStateAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    constraint_name: []const u8,
    state: runtime_schema_mod.UniqueConstraintValidationState,
) ![]u8 {
    const state_text = try uniqueConstraintValidationStateString(state);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const unique_constraints = root.getPtr("unique_constraints") orelse return error.UniqueConstraintNotFound;
    const constraint_items = switch (unique_constraints.*) {
        .array => |*array| array.items,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var found = false;
    for (constraint_items) |*constraint| {
        const object = switch (constraint.*) {
            .object => |*object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string) return error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, name.string, constraint_name)) continue;

        const validation_state = object.getPtr("validation_state") orelse return error.InvalidSchemaUpdateRequest;
        validation_state.* = .{ .string = state_text };
        found = true;
        break;
    }
    if (!found) return error.UniqueConstraintNotFound;

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    validated.deinit(alloc);
    return updated;
}

pub fn schemaWithSecondaryIndexReadyAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    index_name: []const u8,
    expected_generation: u64,
) ![]u8 {
    if (expected_generation == 0) return error.InvalidSchemaUpdateRequest;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const default_type_value = root.get("default_type") orelse return error.InvalidSchemaUpdateRequest;
    if (default_type_value != .string) return error.InvalidSchemaUpdateRequest;
    const document_schemas = root.getPtr("document_schemas") orelse return error.InvalidSchemaUpdateRequest;
    if (document_schemas.* != .object) return error.InvalidSchemaUpdateRequest;
    const document_schema = document_schemas.object.getPtr(default_type_value.string) orelse return error.InvalidSchemaUpdateRequest;
    if (document_schema.* != .object) return error.InvalidSchemaUpdateRequest;
    const schema = document_schema.object.getPtr("schema") orelse return error.InvalidSchemaUpdateRequest;
    if (schema.* != .object) return error.InvalidSchemaUpdateRequest;
    const properties = schema.object.getPtr("properties") orelse return error.InvalidSchemaUpdateRequest;
    if (properties.* != .object) return error.InvalidSchemaUpdateRequest;
    const property = schemaPropertyForSecondaryIndex(properties, index_name) orelse return error.SecondaryIndexNotFound;
    if (property.* != .object) return error.InvalidSchemaUpdateRequest;

    const generation_value = property.object.get("x-antfly-index-generation") orelse return error.SecondaryIndexGenerationMismatch;
    if (generation_value != .integer or generation_value.integer <= 0) return error.InvalidSchemaUpdateRequest;
    const generation: u64 = @intCast(generation_value.integer);
    if (generation != expected_generation) return error.SecondaryIndexGenerationMismatch;

    const lifecycle_value = property.object.getPtr("x-antfly-index-lifecycle") orelse return error.SecondaryIndexNotBuilding;
    if (lifecycle_value.* != .string) return error.InvalidSchemaUpdateRequest;
    if (!std.mem.eql(u8, lifecycle_value.string, "building")) return error.SecondaryIndexNotBuilding;
    lifecycle_value.* = .{ .string = "ready" };

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    validated.deinit(alloc);
    return updated;
}

fn schemaPropertyForSecondaryIndex(properties: *std.json.Value, index_name: []const u8) ?*std.json.Value {
    if (properties.* != .object) return null;
    var it = properties.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const declared = entry.value_ptr.object.get("x-antfly-index-name") orelse continue;
        if (declared == .string and std.mem.eql(u8, declared.string, index_name)) return entry.value_ptr;
    }
    return properties.object.getPtr(index_name);
}

fn relationalRowsExpressionConditionSlicesEqual(
    a: []const runtime_schema_mod.RelationalRowsExpressionCondition,
    b: []const runtime_schema_mod.RelationalRowsExpressionCondition,
) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!relationalRowsExpressionConditionsEqual(left, right)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionsEqual(
    a: runtime_schema_mod.RelationalRowsExpressionCondition,
    b: runtime_schema_mod.RelationalRowsExpressionCondition,
) bool {
    if (a.op != b.op or a.rhs.len != b.rhs.len) return false;
    if (!relationalRowsExpressionsEqual(a.lhs, b.lhs)) return false;
    for (a.rhs, b.rhs) |left, right| {
        if (!relationalRowsExpressionsEqual(left, right)) return false;
    }
    return true;
}

fn relationalRowsExpressionsEqual(
    a: runtime_schema_mod.RelationalRowsExpression,
    b: runtime_schema_mod.RelationalRowsExpression,
) bool {
    if (a.kind != b.kind or
        !std.mem.eql(u8, a.field, b.field) or
        a.field_source != b.field_source or
        !std.mem.eql(u8, a.value_json, b.value_json) or
        !std.mem.eql(u8, a.json_path, b.json_path) or
        a.json_as_text != b.json_as_text or
        a.cast_type != b.cast_type or
        a.operands.len != b.operands.len or
        a.case_branches.len != b.case_branches.len or
        a.case_else.len != b.case_else.len)
    {
        return false;
    }
    for (a.operands, b.operands) |left, right| {
        if (!relationalRowsExpressionsEqual(left, right)) return false;
    }
    for (a.case_branches, b.case_branches) |left, right| {
        if (!relationalRowsExpressionConditionsEqual(left.when, right.when)) return false;
        if (!relationalRowsExpressionsEqual(left.then, right.then)) return false;
    }
    for (a.case_else, b.case_else) |left, right| {
        if (!relationalRowsExpressionsEqual(left, right)) return false;
    }
    return true;
}

fn uniqueExpressionSlicesEqual(a: []const runtime_schema_mod.UniqueExpression, b: []const runtime_schema_mod.UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (left.expression == null and right.expression == null) continue;
        if (left.expression == null or right.expression == null) return false;
        if (!relationalRowsExpressionsEqual(left.expression.?, right.expression.?)) return false;
    }
    return true;
}

fn uniquePredicateSlicesEqual(a: []const runtime_schema_mod.UniquePredicate, b: []const runtime_schema_mod.UniquePredicate) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (!optionalStringsEqual(left.value_json, right.value_json)) return false;
    }
    return true;
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

pub fn routeQueryRequestToActiveReadIndex(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    req: *db_mod.types.SearchRequest,
) !void {
    if (!queryNeedsPrimaryTextIndex(req.*)) return;

    const active_name = (try selectActiveFullTextIndexName(alloc, table)) orelse return;
    errdefer alloc.free(active_name);

    if (req.primary_text_index_name == null) {
        req.primary_text_index_name = try alloc.dupe(u8, active_name);
    }
    if (req.index_name == null) {
        req.index_name = active_name;
    } else {
        alloc.free(active_name);
    }
}

fn buildTableStatus(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
    storage_status: ?TableStorageStatus,
    include_replication_runtime: bool,
) !metadata_openapi.TableStatus {
    const ranges = try metadata_admin.listTableRanges(alloc, snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    var shards = std.json.ArrayHashMap(metadata_openapi.ShardConfig){};
    for (ranges) |range_ref| {
        const key = try std.fmt.allocPrint(alloc, "{d}", .{range_ref.group_id});
        const byte_range = try alloc.alloc([]const u8, 2);
        byte_range[0] = range_ref.start_key;
        byte_range[1] = range_ref.end_key orelse "";
        try shards.map.put(alloc, key, .{ .byte_range = byte_range });
    }

    const empty = if (storage_status) |status| status.empty else ranges.len == 0;
    const lsm_status = if (storage_status) |status|
        if (status.lsm) |lsm| generatedLsmStorageStatus(lsm) else null
    else
        null;
    return .{
        .name = table.name,
        .description = if (table.description.len > 0) table.description else null,
        .tablespace_name = if (table.tablespace_name.len > 0) table.tablespace_name else null,
        .indexes = try parseTableIndexes(alloc, table.indexes_json),
        .shards = shards,
        .schema = try parseOptionalTableSchema(alloc, table.schema_json),
        .migration = if (table.read_schema_json.len > 0) .{
            .state = "rebuilding",
            .read_schema = try parseTableSchema(alloc, table.read_schema_json),
        } else null,
        .replication_sources = try parseReplicationSources(alloc, snapshot, table, include_replication_runtime),
        .storage_status = .{
            .disk_usage = 0,
            .empty = empty,
            .lsm = lsm_status,
        },
    };
}

fn parseOptionalTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !?schema_openapi.TableSchema {
    if (schema_json.len == 0) return null;
    return try parseTableSchema(alloc, schema_json);
}

fn parseTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !schema_openapi.TableSchema {
    return try std.json.parseFromSliceLeaky(schema_openapi.TableSchema, alloc, schema_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn parseTableIndexes(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) !std.json.ArrayHashMap(indexes_openapi.IndexConfig) {
    const canonical_json = try encodeTableIndexesObject(alloc, indexes_json);
    defer alloc.free(canonical_json);
    return try std.json.parseFromSliceLeaky(std.json.ArrayHashMap(indexes_openapi.IndexConfig), alloc, canonical_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn parseReplicationSources(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
    include_replication_runtime: bool,
) ![]const metadata_openapi.ReplicationSource {
    const raw = if (include_replication_runtime)
        try encodeTableReplicationSourcesAlloc(alloc, snapshot, table)
    else
        table.replication_sources_json;
    return try std.json.parseFromSliceLeaky([]metadata_openapi.ReplicationSource, alloc, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn encodeTableReplicationSourcesAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, table.replication_sources_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return try alloc.dupe(u8, table.replication_sources_json);

    for (parsed.value.array.items, 0..) |*item, source_ordinal| {
        if (item.* != .object) continue;
        if (findReplicationSourceStatus(snapshot, table.table_id, @intCast(source_ordinal))) |status| {
            try item.object.put(alloc, "status", try replicationSourceStatusJsonValueAlloc(alloc, status.*));
        }
        if (findReplicationSourceActionHint(snapshot, table.table_id, @intCast(source_ordinal))) |hint| {
            try item.object.put(alloc, "action_hint", try replicationSourceActionHintJsonValueAlloc(alloc, hint.*));
        }
    }
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn replicationSourceStatusJsonValueAlloc(
    alloc: std.mem.Allocator,
    status: metadata_table_manager.ReplicationSourceStatusRecord,
) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    errdefer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        object.deinit(alloc);
    }
    try putJsonStringField(alloc, &object, "source_kind", status.source_kind);
    try putJsonStringField(alloc, &object, "external_table", status.external_table);
    try putJsonStringField(alloc, &object, "cutover_mode", status.cutover_mode);
    try putJsonStringField(alloc, &object, "slot_name", status.slot_name);
    try putJsonStringField(alloc, &object, "publication_name", status.publication_name);
    try putJsonStringField(alloc, &object, "phase", status.phase);
    try putJsonStringField(alloc, &object, "checkpoint", status.checkpoint);
    try putJsonIntegerField(alloc, &object, "snapshot_offset", status.snapshot_offset);
    try putJsonStringField(alloc, &object, "prepared_checkpoint", status.prepared_checkpoint);
    try putJsonStringField(alloc, &object, "stream_checkpoint", status.stream_checkpoint);
    try putJsonStringField(alloc, &object, "last_error", status.last_error);
    try putJsonStringField(alloc, &object, "failure_class", status.failure_class);
    try putJsonIntegerField(alloc, &object, "lag_records", status.lag_records);
    try putJsonIntegerField(alloc, &object, "lag_millis", status.lag_millis);
    try putJsonIntegerField(alloc, &object, "consecutive_failures", status.consecutive_failures);
    try putJsonIntegerField(alloc, &object, "last_source_commit_at_ms", status.last_source_commit_at_ms);
    try putJsonIntegerField(alloc, &object, "last_success_at_ms", status.last_success_at_ms);
    try putJsonIntegerField(alloc, &object, "last_change_applied_at_ms", status.last_change_applied_at_ms);
    try putJsonIntegerField(alloc, &object, "updated_at_ms", status.updated_at_ms);
    return .{ .object = object };
}

fn replicationSourceActionHintJsonValueAlloc(
    alloc: std.mem.Allocator,
    hint: metadata_api.ReplicationSourceActionHint,
) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    errdefer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        object.deinit(alloc);
    }
    try putJsonStringField(alloc, &object, "action", hint.action);
    try putJsonStringField(alloc, &object, "reason", hint.reason);
    try putJsonStringField(alloc, &object, "reseed_exact_cutover_path", hint.reseed_exact_cutover_path);
    return .{ .object = object };
}

fn putJsonStringField(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    value: []const u8,
) !void {
    try object.put(alloc, try alloc.dupe(u8, key), .{ .string = try alloc.dupe(u8, value) });
}

fn putJsonIntegerField(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    value: u64,
) !void {
    try object.put(alloc, try alloc.dupe(u8, key), .{ .integer = @intCast(value) });
}

fn findReplicationSourceStatus(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
    source_ordinal: u32,
) ?*const metadata_table_manager.ReplicationSourceStatusRecord {
    for (snapshot.replication_source_statuses) |*status| {
        if (status.table_id == table_id and status.source_ordinal == source_ordinal) return status;
    }
    return null;
}

fn findReplicationSourceActionHint(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
    source_ordinal: u32,
) ?*const metadata_api.ReplicationSourceActionHint {
    for (snapshot.replication_source_action_hints) |*hint| {
        if (hint.table_id == table_id and hint.source_ordinal == source_ordinal) return hint;
    }
    return null;
}

fn findTableStorageStatus(
    storage_statuses: ?[]const TableStorageStatus,
    table_name: []const u8,
) ?TableStorageStatus {
    const items = storage_statuses orelse return null;
    for (items) |status| {
        if (std.mem.eql(u8, status.table_name, table_name)) return status;
    }
    return null;
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn appendJsonValue(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    const encoded = try stringifyJsonValue(alloc, value);
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn deinitJsonValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
    json_helpers.deinitJsonValue(alloc, value);
    value.* = .null;
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn encodeTableIndexesObject(alloc: std.mem.Allocator, indexes_json: []const u8) ![]u8 {
    const source = if (indexes_json.len > 0) indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        // Reserved metadata sections are not index configs; the provisioner
        // reads them from the stored indexes_json.
        if (isReservedIndexMetadataEntry(entry.key_ptr.*) or isLegacyTypedPathMetadataConfig(entry.value_ptr.*)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendCanonicalIndexConfig(alloc, &out, entry.key_ptr.*, entry.value_ptr.*);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn isReservedIndexMetadataEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, "resolvers") or
        std.mem.eql(u8, name, "enrichments") or
        std.mem.eql(u8, name, "typed_paths");
}

fn isLegacyTypedPathMetadataConfig(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    if (type_value != .string) return false;
    // Earlier branch builds accepted scalar-shaped entries under `indexes`.
    // They are metadata only and must not be surfaced as public index configs.
    return std.mem.eql(u8, type_value.string, "scalar") or
        std.mem.eql(u8, type_value.string, "path") or
        std.mem.eql(u8, type_value.string, "secondary") or
        std.mem.eql(u8, type_value.string, "keyword") or
        std.mem.eql(u8, type_value.string, "numeric") or
        std.mem.eql(u8, type_value.string, "boolean") or
        std.mem.eql(u8, type_value.string, "datetime") or
        std.mem.eql(u8, type_value.string, "term");
}

fn encodeSingleTableIndex(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const value = (try buildSingleTableIndexValue(arena_impl.allocator(), table, index_name)) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

fn buildSingleTableIndexValue(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !?std.json.Value {
    const source = if (table.indexes_json.len > 0) table.indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const config = root.get(index_name) orelse return null;
    if (isLegacyTypedPathMetadataConfig(config)) return null;
    return try buildCanonicalIndexConfigValue(alloc, index_name, config);
}

const ApiIndexType = enum {
    full_text,
    embeddings,
    graph,
    algebraic,
};

fn appendCanonicalIndexConfig(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
) !void {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;

    try out.append(alloc, '{');
    try appendJsonString(alloc, out, "name");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, index_name);
    if (config.object.get("type") == null) {
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, switch (index_type) {
            .full_text => "full_text",
            .embeddings => "embeddings",
            .graph => "graph",
            .algebraic => "algebraic",
        });
    }

    var it = config.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "name")) continue;
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, entry.value_ptr.*);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
}

fn buildCanonicalIndexConfigValue(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
) !std.json.Value {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;

    var object = std.json.ObjectMap.empty;
    errdefer {
        var value: std.json.Value = .{ .object = object };
        deinitJsonValue(alloc, &value);
    }

    try object.put(alloc, try alloc.dupe(u8, "name"), .{ .string = try alloc.dupe(u8, index_name) });
    if (config.object.get("type") == null) {
        try object.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, switch (index_type) {
            .full_text => "full_text",
            .embeddings => "embeddings",
            .graph => "graph",
            .algebraic => "algebraic",
        }) });
    }

    var it = config.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "name")) continue;
        try object.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValueAlloc(alloc, entry.value_ptr.*));
    }
    return .{ .object = object };
}

fn canonicalIndexEnrichmentsValue(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    if (value != .array) return try cloneJsonValueAlloc(alloc, value);
    var array = std.json.Array.init(alloc);
    errdefer {
        var owned: std.json.Value = .{ .array = array };
        deinitJsonValue(alloc, &owned);
    }
    for (value.array.items) |item| {
        switch (item) {
            .string => |name| try array.append(.{ .string = try alloc.dupe(u8, name) }),
            .object => |object| {
                const name = object.get("name") orelse return error.InvalidTableIndexMetadata;
                if (name != .string) return error.InvalidTableIndexMetadata;
                try array.append(.{ .string = try alloc.dupe(u8, name.string) });
            },
            else => return error.InvalidTableIndexMetadata,
        }
    }
    return .{ .array = array };
}

fn projectInlineEnrichmentConfigsInTableStatusJson(alloc: std.mem.Allocator, encoded: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer parsed.deinit();
    var owned = try cloneJsonValueAlloc(alloc, parsed.value);
    defer deinitJsonValue(alloc, &owned);
    try projectInlineEnrichmentConfigsInTableStatusValue(alloc, &owned);
    return try std.json.Stringify.valueAlloc(alloc, owned, .{ .emit_null_optional_fields = false });
}

fn projectSingleTableStatusJson(alloc: std.mem.Allocator, encoded: []const u8, indexes_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer parsed.deinit();
    var owned = try cloneJsonValueAlloc(alloc, parsed.value);
    defer deinitJsonValue(alloc, &owned);
    try projectInlineEnrichmentConfigsInTableStatusValue(alloc, &owned);
    try attachArtifactEnrichmentsToTableStatus(alloc, &owned, indexes_json);
    return try std.json.Stringify.valueAlloc(alloc, owned, .{ .emit_null_optional_fields = false });
}

fn attachArtifactEnrichmentsToTableStatus(alloc: std.mem.Allocator, value: *std.json.Value, indexes_json: []const u8) !void {
    if (value.* != .object) return;
    const source = if (indexes_json.len > 0) indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTableIndexMetadata;
    const enrichments = parsed.value.object.get("enrichments") orelse return;
    if (enrichments != .array or enrichments.array.items.len == 0) return;
    try value.object.put(alloc, try alloc.dupe(u8, "artifact_enrichments"), try cloneJsonValueAlloc(alloc, enrichments));
}

fn projectInlineEnrichmentConfigsInTableStatusValue(alloc: std.mem.Allocator, value: *std.json.Value) !void {
    switch (value.*) {
        .array => |*array| {
            for (array.items) |*item| try projectInlineEnrichmentConfigsInTableStatusValue(alloc, item);
        },
        .object => |*object| {
            const indexes_value = object.getPtr("indexes") orelse return;
            if (indexes_value.* != .object) return;
            var index_it = indexes_value.object.iterator();
            while (index_it.next()) |index_entry| {
                if (index_entry.value_ptr.* != .object) continue;
                const enrichments_value = index_entry.value_ptr.object.getPtr("enrichments") orelse continue;
                const projected = try canonicalIndexEnrichmentsValue(alloc, enrichments_value.*);
                deinitJsonValue(alloc, enrichments_value);
                enrichments_value.* = projected;
            }
        },
        else => {},
    }
}

fn inferIndexType(index_name: []const u8, config: std.json.Value) ?ApiIndexType {
    if (config != .object) return null;
    if (config.object.get("type")) |type_value| {
        if (type_value != .string) return null;
        if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
        if (std.mem.eql(u8, type_value.string, "embeddings")) return .embeddings;
        if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
        if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
        return null;
    }
    if (std.mem.eql(u8, index_name, default_full_text_index_name)) return .full_text;
    if (std.mem.startsWith(u8, index_name, "full_text_index_v")) return .full_text;
    if (std.mem.eql(u8, index_name, "default")) return .full_text;
    return null;
}

test "table index encoder omits typed path metadata" {
    const encoded = try encodeTableIndexesObject(
        std.testing.allocator,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"typed_paths\":{\"numeric\":[\"metrics.score\"]}}",
    );
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "typed_paths") == null);
}

fn parseJsonValueAlloc(alloc: std.mem.Allocator, body: []const u8) !std.json.Value {
    return try json_helpers.parseOwnedJsonValueAllocAlways(alloc, body);
}

fn cloneJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    return try json_helpers.cloneJsonValue(alloc, value);
}

fn buildTableRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) !TableRuntimeSchemaDebug {
    const runtime_schemas = try alloc.alloc(RuntimeSchemaDebugSchemaEntry, 2);
    runtime_schemas[0] = try buildTableSchemaDebugEntry(alloc, "active", table.schema_json);
    runtime_schemas[1] = try buildTableSchemaDebugEntry(alloc, "read", table.read_schema_json);
    const algebraic_capabilities = try alloc.alloc(AlgebraicCapabilityDebugEntry, 2);
    algebraic_capabilities[0] = try buildAlgebraicCapabilityDebugEntry(alloc, "active", table.name, table.schema_json);
    algebraic_capabilities[1] = try buildAlgebraicCapabilityDebugEntry(alloc, "read", table.name, table.read_schema_json);
    try annotateAlgebraicCapabilityLifecycle(alloc, table.schema_json, table.read_schema_json, algebraic_capabilities);
    return .{
        .runtime_schemas = runtime_schemas,
        .full_text_index_bindings = try buildFullTextIndexBindings(alloc, table),
        .algebraic_capabilities = algebraic_capabilities,
    };
}

fn encodeTableRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const debug = try buildTableRuntimeSchemaDebug(arena_impl.allocator(), table);
    return try std.json.Stringify.valueAlloc(alloc, debug, .{ .emit_null_optional_fields = false });
}

fn buildTableIndexRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !IndexRuntimeSchemaDebug {
    return .{
        .binding = try buildSingleIndexBinding(alloc, table, index_name),
    };
}

pub fn buildTableIndexRuntimeSchemaDebugValue(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !std.json.Value {
    const debug = try buildTableIndexRuntimeSchemaDebug(alloc, table, index_name);
    var object = std.json.ObjectMap.empty;
    errdefer {
        var value: std.json.Value = .{ .object = object };
        deinitJsonValue(alloc, &value);
    }
    try object.put(alloc, try alloc.dupe(u8, "binding"), try jsonValueFromRuntimeSchemaDebugBinding(alloc, debug.binding));
    return .{ .object = object };
}

fn encodeTableIndexRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const debug = try buildTableIndexRuntimeSchemaDebug(arena_impl.allocator(), table, index_name);
    return try std.json.Stringify.valueAlloc(alloc, debug, .{ .emit_null_optional_fields = false });
}

fn jsonValueFromRuntimeSchemaDebugBinding(
    alloc: std.mem.Allocator,
    binding: RuntimeSchemaDebugBinding,
) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    errdefer {
        var value: std.json.Value = .{ .object = object };
        deinitJsonValue(alloc, &value);
    }
    try object.put(alloc, try alloc.dupe(u8, "index_name"), .{ .string = try alloc.dupe(u8, binding.index_name) });
    try object.put(alloc, try alloc.dupe(u8, "status"), .{ .string = try alloc.dupe(u8, binding.status) });
    if (binding.schema_version) |schema_version| {
        try object.put(alloc, try alloc.dupe(u8, "schema_version"), .{ .integer = schema_version });
    }
    if (binding.schema_slot) |schema_slot| {
        try object.put(alloc, try alloc.dupe(u8, "schema_slot"), .{ .string = try alloc.dupe(u8, schema_slot) });
    }
    if (binding.runtime_schema) |runtime_schema| {
        try object.put(alloc, try alloc.dupe(u8, "runtime_schema"), try cloneJsonValueAlloc(alloc, runtime_schema));
    }
    return .{ .object = object };
}

fn buildTableSchemaDebugEntry(
    alloc: std.mem.Allocator,
    slot: []const u8,
    schema_json: []const u8,
) !RuntimeSchemaDebugSchemaEntry {
    if (schema_json.len == 0) {
        return .{
            .slot = slot,
            .status = "missing",
        };
    }

    var parsed_schema = schema_mod.parseValidatedTableSchema(alloc, schema_json) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer parsed_schema.deinit(alloc);

    const runtime_schema = schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer runtime_schema_mod.freeSchema(alloc, runtime_schema);

    const runtime_schema_json = try runtimeSchemaJsonAlloc(alloc, runtime_schema);
    defer alloc.free(runtime_schema_json);
    return .{
        .slot = slot,
        .status = "ok",
        .schema_version = runtime_schema.version,
        .runtime_schema = try parseJsonValueAlloc(alloc, runtime_schema_json),
    };
}

fn buildAlgebraicCapabilityDebugEntry(
    alloc: std.mem.Allocator,
    slot: []const u8,
    table_name: []const u8,
    schema_json: []const u8,
) !AlgebraicCapabilityDebugEntry {
    if (schema_json.len == 0) {
        return .{
            .slot = slot,
            .status = "missing",
        };
    }

    var parsed_schema = schema_mod.parseValidatedTableSchema(alloc, schema_json) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer parsed_schema.deinit(alloc);

    var plan = algebraic_mod.schema_capability.compilePlanAlloc(alloc, parsed_schema) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer plan.deinit(alloc);
    const fingerprint = algebraic_mod.schema_capability.capabilityFingerprintAlloc(alloc, plan) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };

    const config_json = algebraic_mod.schema_capability.configJsonFromPlanAlloc(alloc, table_name, plan) catch |err| {
        alloc.free(fingerprint);
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer alloc.free(config_json);

    return .{
        .slot = slot,
        .status = "ok",
        .schema_version = plan.schema_version,
        .capability_fingerprint = fingerprint,
        .lifecycle_status = "current",
        .group_field_count = countAlgebraicRole(plan, .group),
        .measure_field_count = countAlgebraicRole(plan, .measure),
        .time_field_count = countAlgebraicRole(plan, .time),
        .skipped_dynamic_fields = plan.skipped_dynamic_fields,
        .skipped_complex_fields = plan.skipped_complex_fields,
        .skipped_unbounded_fields = plan.skipped_unbounded_fields,
        .config = try parseJsonValueAlloc(alloc, config_json),
    };
}

fn annotateAlgebraicCapabilityLifecycle(
    alloc: std.mem.Allocator,
    active_schema_json: []const u8,
    read_schema_json: []const u8,
    entries: []AlgebraicCapabilityDebugEntry,
) !void {
    if (entries.len < 2 or active_schema_json.len == 0 or read_schema_json.len == 0) return;
    if (!std.mem.eql(u8, entries[0].status, "ok") or !std.mem.eql(u8, entries[1].status, "ok")) return;

    var active_schema = try schema_mod.parseValidatedTableSchema(alloc, active_schema_json);
    defer active_schema.deinit(alloc);
    var read_schema = try schema_mod.parseValidatedTableSchema(alloc, read_schema_json);
    defer read_schema.deinit(alloc);
    var active_plan = try algebraic_mod.schema_capability.compilePlanAlloc(alloc, active_schema);
    defer active_plan.deinit(alloc);
    var read_plan = try algebraic_mod.schema_capability.compilePlanAlloc(alloc, read_schema);
    defer read_plan.deinit(alloc);

    const impact = algebraic_mod.schema_capability.classifyChange(active_plan, read_plan);
    entries[1].change_added_fields = impact.added_fields;
    entries[1].change_removed_fields = impact.removed_fields;
    entries[1].change_changed_type_fields = impact.changed_type_fields;
    entries[1].compatible_additive = impact.compatible_additive;
    entries[1].requires_rebuild = impact.requires_rebuild;
    entries[1].lifecycle_status = if (impact.requires_rebuild)
        "rebuild_required"
    else if (impact.added_fields > 0)
        "compatible_additive"
    else
        "current";
}

fn countAlgebraicRole(
    plan: algebraic_mod.schema_capability.Plan,
    role: algebraic_mod.schema_capability.FieldRole,
) u32 {
    var count: u32 = 0;
    for (plan.fields) |field| {
        if (field.role == role) count += 1;
    }
    return count;
}

fn buildFullTextIndexBindings(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) ![]RuntimeSchemaDebugBinding {
    const source = if (table.indexes_json.len > 0) table.indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var count: usize = 0;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        count += 1;
    }
    const bindings = try alloc.alloc(RuntimeSchemaDebugBinding, count);
    it = root.iterator();
    var index: usize = 0;
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        bindings[index] = try buildSingleIndexBinding(alloc, table, entry.key_ptr.*);
        index += 1;
    }
    return bindings;
}

fn buildSingleIndexBinding(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !RuntimeSchemaDebugBinding {
    const binding = try resolveFullTextIndexBinding(alloc, table, index_name);
    defer if (binding.runtime_schema_json) |value| alloc.free(value);
    return .{
        .index_name = index_name,
        .status = binding.status,
        .schema_version = binding.schema_version,
        .schema_slot = binding.schema_slot,
        .runtime_schema = if (binding.runtime_schema_json) |runtime_schema_json|
            try parseJsonValueAlloc(alloc, runtime_schema_json)
        else
            null,
    };
}

const FullTextIndexBinding = struct {
    status: []const u8,
    schema_version: ?u32 = null,
    schema_slot: ?[]const u8 = null,
    runtime_schema_json: ?[]u8 = null,
};

fn resolveFullTextIndexBinding(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !FullTextIndexBinding {
    const source = if (table.indexes_json.len > 0) table.indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const config = root.get(index_name) orelse return .{ .status = "missing_index" };
    if (!isFullTextIndexConfig(config)) return .{ .status = "not_full_text" };

    const schema_version = fullTextSchemaVersionForIndex(alloc, table, index_name) orelse {
        return .{ .status = "unavailable" };
    };
    const compiled = try compileRuntimeSchemaJsonForVersion(alloc, table, schema_version);
    if (compiled) |entry| {
        return .{
            .status = "ok",
            .schema_version = schema_version,
            .schema_slot = entry.slot,
            .runtime_schema_json = entry.runtime_schema_json,
        };
    }
    return .{
        .status = "unavailable",
        .schema_version = schema_version,
    };
}

fn fullTextSchemaVersionForIndex(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) ?u32 {
    if (std.mem.eql(u8, index_name, "default") or std.mem.eql(u8, index_name, "full_text_index")) return 0;
    const prefix = "full_text_index_v";
    if (std.mem.startsWith(u8, index_name, prefix)) {
        return std.fmt.parseInt(u32, index_name[prefix.len..], 10) catch null;
    }

    const active_name = full_text_indexes.selectActiveFullTextIndexNameAlloc(
        alloc,
        table.schema_json,
        table.read_schema_json,
        table.indexes_json,
    ) catch return null;
    defer if (active_name) |value| alloc.free(value);
    if (active_name == null or !std.mem.eql(u8, active_name.?, index_name)) return null;
    return if (table.read_schema_json.len > 0)
        schemaVersion(table.read_schema_json) catch null
    else
        schemaVersion(table.schema_json) catch null;
}

const CompiledRuntimeSchemaJson = struct {
    slot: []const u8,
    runtime_schema_json: []u8,
};

fn compileRuntimeSchemaJsonForVersion(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    version: u32,
) !?CompiledRuntimeSchemaJson {
    if (table.schema_json.len > 0) {
        const active_version = schemaVersion(table.schema_json) catch null;
        if (active_version != null and active_version.? == version) {
            const runtime_schema_json = try compileRuntimeSchemaJson(alloc, table.schema_json);
            return .{ .slot = "active", .runtime_schema_json = runtime_schema_json };
        }
    }
    if (table.read_schema_json.len > 0) {
        const read_version = schemaVersion(table.read_schema_json) catch null;
        if (read_version != null and read_version.? == version) {
            const runtime_schema_json = try compileRuntimeSchemaJson(alloc, table.read_schema_json);
            return .{ .slot = "read", .runtime_schema_json = runtime_schema_json };
        }
    }
    return null;
}

fn compileRuntimeSchemaJson(alloc: std.mem.Allocator, schema_json: []const u8) ![]u8 {
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema_mod.freeSchema(alloc, runtime_schema);

    return try runtimeSchemaJsonAlloc(alloc, runtime_schema);
}

fn runtimeSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema_mod.TableSchema,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendRuntimeSchemaObject(alloc, &out, schema);
    return try out.toOwnedSlice(alloc);
}

fn appendRuntimeSchemaObject(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schema: runtime_schema_mod.TableSchema,
) !void {
    try out.append(alloc, '{');
    try appendJsonString(alloc, out, "version");
    try out.append(alloc, ':');
    const version_text = try std.fmt.allocPrint(alloc, "{d}", .{schema.version});
    defer alloc.free(version_text);
    try out.appendSlice(alloc, version_text);
    try out.appendSlice(alloc, ",\"default_type\":");
    try appendJsonString(alloc, out, schema.default_type);
    try out.appendSlice(alloc, ",\"ttl_field\":");
    try appendJsonString(alloc, out, schema.ttl_field);
    try out.appendSlice(alloc, ",\"ttl_duration_ns\":");
    const ttl_text = try std.fmt.allocPrint(alloc, "{d}", .{schema.ttl_duration_ns});
    defer alloc.free(ttl_text);
    try out.appendSlice(alloc, ttl_text);
    try out.appendSlice(alloc, ",\"dynamic_templates\":[");
    for (schema.dynamic_templates, 0..) |tmpl, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, tmpl.name);
        if (tmpl.match_pattern) |value| {
            try out.appendSlice(alloc, ",\"match\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.unmatch_pattern) |value| {
            try out.appendSlice(alloc, ",\"unmatch\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.path_match) |value| {
            try out.appendSlice(alloc, ",\"path_match\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.path_unmatch) |value| {
            try out.appendSlice(alloc, ",\"path_unmatch\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.match_mapping_type) |value| {
            try out.appendSlice(alloc, ",\"match_mapping_type\":");
            try appendJsonString(alloc, out, value);
        }
        try out.appendSlice(alloc, ",\"mapping\":{");
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, antflyTypeName(tmpl.mapping.field_type));
        try out.appendSlice(alloc, ",\"index\":");
        try out.appendSlice(alloc, if (tmpl.mapping.do_index) "true" else "false");
        try out.appendSlice(alloc, ",\"store\":");
        try out.appendSlice(alloc, if (tmpl.mapping.store) "true" else "false");
        try out.appendSlice(alloc, ",\"doc_values\":");
        try out.appendSlice(alloc, if (tmpl.mapping.doc_values) "true" else "false");
        try out.appendSlice(alloc, ",\"include_in_all\":");
        try out.appendSlice(alloc, if (tmpl.mapping.include_in_all) "true" else "false");
        try out.appendSlice(alloc, ",\"analyzer\":");
        try appendJsonString(alloc, out, tmpl.mapping.analyzer);
        try out.appendSlice(alloc, "}}");
    }
    try out.appendSlice(alloc, "],\"full_text_documents\":[");
    for (schema.full_text_documents, 0..) |doc, doc_idx| {
        if (doc_idx > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, doc.name);
        try out.appendSlice(alloc, ",\"fields\":[");
        for (doc.fields, 0..) |field, field_idx| {
            if (field_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            try appendJsonString(alloc, out, "path");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, field.path);
            try out.appendSlice(alloc, ",\"emitted_name\":");
            try appendJsonString(alloc, out, field.emitted_name);
            try out.appendSlice(alloc, ",\"analyzer\":");
            try appendJsonString(alloc, out, field.analyzer);
            try out.appendSlice(alloc, ",\"include_in_all\":");
            try out.appendSlice(alloc, if (field.include_in_all) "true" else "false");
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "],\"dynamic_rules\":[");
        for (doc.dynamic_rules, 0..) |rule, rule_idx| {
            if (rule_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            try appendJsonString(alloc, out, "parent_path");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, rule.parent_path);
            if (rule.segment_pattern) |segment_pattern| {
                try out.appendSlice(alloc, ",\"segment_pattern\":");
                try appendJsonString(alloc, out, segment_pattern);
            }
            try out.appendSlice(alloc, ",\"relative_path\":");
            try appendJsonString(alloc, out, rule.relative_path);
            try out.appendSlice(alloc, ",\"variants\":[");
            for (rule.variants, 0..) |variant, variant_idx| {
                if (variant_idx > 0) try out.append(alloc, ',');
                try out.append(alloc, '{');
                try appendJsonString(alloc, out, "suffix");
                try out.append(alloc, ':');
                try appendJsonString(alloc, out, variant.suffix);
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, variant.analyzer);
                try out.appendSlice(alloc, ",\"include_in_all\":");
                try out.appendSlice(alloc, if (variant.include_in_all) "true" else "false");
                try out.append(alloc, '}');
            }
            try out.appendSlice(alloc, "]}");
        }
        try out.appendSlice(alloc, "],\"open_dynamic_paths\":[");
        for (doc.open_dynamic_paths, 0..) |path, open_idx| {
            if (open_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, path);
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
}

fn antflyTypeName(value: runtime_schema_mod.AntflyType) []const u8 {
    return switch (value) {
        .text => "text",
        .keyword => "keyword",
        .numeric => "numeric",
        .embedding => "embedding",
        .link => "link",
        .boolean => "boolean",
        .datetime => "datetime",
        .geopoint => "geopoint",
        .geoshape => "geoshape",
        .blob => "blob",
        .html => "html",
        .search_as_you_type => "search_as_you_type",
        .json => "json",
        .array => "array",
    };
}

fn queryNeedsPrimaryTextIndex(req: db_mod.types.SearchRequest) bool {
    if (req.full_text != null) return true;
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) return true;
    if (req.full_text_queries.len > 0) return false;

    return switch (req.query) {
        .match_none,
        .match_all,
        .phrase,
        .multi_phrase,
        .term,
        .fuzzy,
        .numeric_range,
        .date_range,
        .doc_id,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .term_range,
        .ip_range,
        .geo_shape,
        .match,
        .match_phrase,
        .prefix,
        .wildcard,
        .regexp,
        => true,
        else => false,
    };
}

fn schemaVersion(schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const version_value = root.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| std.math.cast(u32, value) orelse error.InvalidSchemaUpdateRequest,
        else => error.InvalidSchemaUpdateRequest,
    };
}

fn documentSchemasChanged(alloc: std.mem.Allocator, current_schema_json: []const u8, next_schema_json: []const u8) !bool {
    const current = try extractCanonicalObjectField(alloc, current_schema_json, "document_schemas");
    defer if (current) |value| alloc.free(value);
    const next = try extractCanonicalObjectField(alloc, next_schema_json, "document_schemas");
    defer if (next) |value| alloc.free(value);

    if (current == null and next == null) return false;
    if (current == null or next == null) return true;
    return !std.mem.eql(u8, current.?, next.?);
}

fn extractCanonicalObjectField(alloc: std.mem.Allocator, schema_json: []const u8, field_name: []const u8) !?[]u8 {
    if (schema_json.len == 0) return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const value = root.get(field_name) orelse return null;
    return try stringifyJsonValue(alloc, value);
}

pub fn normalizeSchemaVersion(alloc: std.mem.Allocator, schema_json: []const u8, version: u32) ![]u8 {
    const source = if (schema_json.len > 0) schema_json else "{}";
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    try appendJsonString(alloc, &out, "version");
    try out.append(alloc, ':');
    const encoded_version = try std.fmt.allocPrint(alloc, "{d}", .{version});
    defer alloc.free(encoded_version);
    try out.appendSlice(alloc, encoded_version);

    const relational_storage = blk: {
        const storage_mode = root.get("storage_mode") orelse break :blk false;
        break :blk storage_mode == .string and std.mem.eql(u8, storage_mode.string, "relational");
    };
    if (relational_storage) {
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "enforce_types");
        try out.appendSlice(alloc, ":true");
    }

    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "version")) continue;
        if (relational_storage and std.mem.eql(u8, entry.key_ptr.*, "enforce_types")) continue;
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, entry.value_ptr.*);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn upsertVersionedFullTextIndex(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    current_version: u32,
    next_version: u32,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const stale_name = try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{current_version});
    defer alloc.free(stale_name);
    const next_name = try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{next_version});
    defer alloc.free(next_name);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var full_text_config: ?std.json.Value = null;
    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (full_text_config == null and isFullTextIndexConfig(entry.value_ptr.*)) {
            full_text_config = entry.value_ptr.*;
        }
        if (std.mem.eql(u8, entry.key_ptr.*, next_name)) continue;

        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, entry.value_ptr.*);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    if (full_text_config) |config| {
        if (!first) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, next_name);
        try out.append(alloc, ':');
        try appendCanonicalIndexConfig(alloc, &out, next_name, config);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn selectActiveFullTextIndexName(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) !?[]u8 {
    return try full_text_indexes.selectActiveFullTextIndexNameAlloc(
        alloc,
        table.schema_json,
        table.read_schema_json,
        table.indexes_json,
    );
}

fn selectFullTextIndexNameForVersion(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    version: u32,
) !?[]u8 {
    return try full_text_indexes.selectFullTextIndexNameForVersionAlloc(alloc, indexes_json, version);
}

fn isFullTextIndexConfig(value: std.json.Value) bool {
    return full_text_indexes.isFullTextIndexConfig(value);
}

fn validateReplicationSourcesJson(alloc: std.mem.Allocator, replication_sources_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed.deinit();
    const items = switch (parsed.value) {
        .array => |array| array.items,
        else => return error.InvalidCreateTableRequest,
    };
    for (items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidCreateTableRequest,
        };
        try requireStringField(object, "type");
        try requireStringField(object, "dsn");
        try requireStringField(object, "postgres_table");
        if (object.get("key_template")) |value| if (value != .string) return error.InvalidCreateTableRequest;
        if (object.get("slot_name")) |value| if (value != .string) return error.InvalidCreateTableRequest;
        if (object.get("publication_name")) |value| if (value != .string) return error.InvalidCreateTableRequest;
        if (object.get("require_exact_cutover")) |value| if (value != .bool) return error.InvalidCreateTableRequest;
    }
    return try alloc.dupe(u8, replication_sources_json);
}

fn requireStringField(object: std.json.ObjectMap, field_name: []const u8) !void {
    const value = object.get(field_name) orelse return error.InvalidCreateTableRequest;
    if (value != .string) return error.InvalidCreateTableRequest;
}

fn parseU32Field(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |int_value| std.math.cast(u32, int_value) orelse error.InvalidCreateTableRequest,
        else => error.InvalidCreateTableRequest,
    };
}

pub fn findTableByName(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?*const metadata_table_manager.TableRecord {
    if (qualifiedTableNameParts(table_name)) |parts| {
        if (findTableByQualifiedName(snapshot, parts.database_name, parts.namespace_name, parts.table_name)) |table| return table;
    }
    return findTableByQualifiedName(snapshot, default_database_name, default_namespace_name, table_name);
}

pub const QualifiedTableNameParts = struct {
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
};

pub fn qualifiedTableNameParts(table_name: []const u8) ?QualifiedTableNameParts {
    const first_dot = std.mem.indexOfScalar(u8, table_name, '.') orelse return null;
    if (first_dot == 0) return null;
    const rest = table_name[first_dot + 1 ..];
    const second_dot_rel = std.mem.indexOfScalar(u8, rest, '.') orelse return null;
    if (second_dot_rel == 0) return null;
    const second_dot = first_dot + 1 + second_dot_rel;
    if (second_dot + 1 >= table_name.len) return null;
    const leaf = table_name[second_dot + 1 ..];
    if (std.mem.indexOfScalar(u8, leaf, '.') != null) return null;
    return .{
        .database_name = table_name[0..first_dot],
        .namespace_name = table_name[first_dot + 1 .. second_dot],
        .table_name = leaf,
    };
}

pub fn findDatabaseByName(snapshot: *const metadata_api.AdminSnapshot, database_name: []const u8) ?*const metadata_table_manager.DatabaseRecord {
    const database_id = metadata_table_manager.deriveDatabaseId(database_name);
    for (snapshot.databases) |*record| {
        if (record.database_id == database_id and std.mem.eql(u8, record.name, database_name)) return record;
    }
    return null;
}

pub fn findNamespaceByName(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
) ?*const metadata_table_manager.NamespaceRecord {
    const database_id = metadata_table_manager.deriveDatabaseId(database_name);
    const namespace_id = metadata_table_manager.deriveNamespaceId(database_id, namespace_name);
    for (snapshot.namespaces) |*record| {
        if (record.namespace_id == namespace_id and record.database_id == database_id and std.mem.eql(u8, record.name, namespace_name)) return record;
    }
    return null;
}

pub fn findTablespaceByName(snapshot: *const metadata_api.AdminSnapshot, tablespace_name: []const u8) ?*const metadata_table_manager.TablespaceRecord {
    const tablespace_id = metadata_table_manager.deriveTablespaceId(tablespace_name);
    for (snapshot.tablespaces) |*record| {
        if (record.tablespace_id == tablespace_id and std.mem.eql(u8, record.name, tablespace_name)) return record;
    }
    return null;
}

pub fn effectiveTablespaceForTarget(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    explicit_tablespace_name: ?[]const u8,
) ?*const metadata_table_manager.TablespaceRecord {
    if (explicit_tablespace_name) |name| {
        if (name.len > 0) return findTablespaceByName(snapshot, name);
    }
    if (findNamespaceByName(snapshot, database_name, namespace_name)) |namespace| {
        if (namespace.tablespace_name.len > 0) return findTablespaceByName(snapshot, namespace.tablespace_name);
    }
    if (findDatabaseByName(snapshot, database_name)) |database| {
        if (database.tablespace_name.len > 0) return findTablespaceByName(snapshot, database.tablespace_name);
    }
    return null;
}

pub fn applyTablespacePlacementPolicyAlloc(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
    tablespace: *const metadata_table_manager.TablespaceRecord,
) !metadata_table_manager.TableRecord {
    var updated = try metadata_table_manager.cloneTable(alloc, table);
    errdefer metadata_table_manager.freeTable(alloc, updated);
    alloc.free(updated.tablespace_name);
    updated.tablespace_name = try alloc.dupe(u8, tablespace.name);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, tablespace.placement_policy_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTablespacePlacementPolicy,
    };
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!tablespacePlacementPolicyKeySupported(entry.key_ptr.*)) return error.InvalidTablespacePlacementPolicy;
    }
    if (root.get("placement_role")) |value| {
        const role = try parseTablespacePlacementRole(value);
        alloc.free(updated.placement_role);
        updated.placement_role = try alloc.dupe(u8, role);
    }
    if (root.get("desired_replica_count")) |value| {
        updated.desired_replica_count = try parseTablespaceReplicaCount(value);
    } else if (root.get("replica_count")) |value| {
        updated.desired_replica_count = try parseTablespaceReplicaCount(value);
    }
    if (root.get("min_ranges")) |value| {
        updated.min_ranges = try parseTablespaceMinRanges(value);
    }
    return updated;
}

fn tablespacePlacementPolicyKeySupported(key: []const u8) bool {
    return std.mem.eql(u8, key, "placement_role") or
        std.mem.eql(u8, key, "desired_replica_count") or
        std.mem.eql(u8, key, "replica_count") or
        std.mem.eql(u8, key, "min_ranges");
}

fn parseTablespacePlacementRole(value: std.json.Value) ![]const u8 {
    const raw = switch (value) {
        .string => |str| str,
        else => return error.InvalidTablespacePlacementPolicy,
    };
    if (std.mem.eql(u8, raw, "data")) return "data";
    if (std.mem.eql(u8, raw, "hot")) return "hot";
    if (std.mem.eql(u8, raw, "cold")) return "cold";
    if (std.mem.eql(u8, raw, "serving")) return "serving";
    if (std.mem.eql(u8, raw, "bulk")) return "bulk";
    if (std.mem.eql(u8, raw, "archive")) return "archive";
    return error.InvalidTablespacePlacementPolicy;
}

fn parseTablespaceReplicaCount(value: std.json.Value) !u16 {
    const int_value = switch (value) {
        .integer => |integer| integer,
        else => return error.InvalidTablespacePlacementPolicy,
    };
    if (int_value <= 0) return error.InvalidTablespacePlacementPolicy;
    return std.math.cast(u16, int_value) orelse error.InvalidTablespacePlacementPolicy;
}

fn parseTablespaceMinRanges(value: std.json.Value) !u32 {
    const int_value = switch (value) {
        .integer => |integer| integer,
        else => return error.InvalidTablespacePlacementPolicy,
    };
    if (int_value <= 0) return error.InvalidTablespacePlacementPolicy;
    return std.math.cast(u32, int_value) orelse error.InvalidTablespacePlacementPolicy;
}

pub fn findTableByQualifiedName(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ?*const metadata_table_manager.TableRecord {
    for (snapshot.tables) |*record| {
        if (tableCatalogIdentityMatches(record.*, database_name, namespace_name, table_name)) return record;
    }
    return null;
}

pub fn tableCatalogIdentityMatches(
    record: metadata_table_manager.TableRecord,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) bool {
    return std.mem.eql(u8, record.database_name, database_name) and
        std.mem.eql(u8, record.namespace_name, namespace_name) and
        std.mem.eql(u8, record.name, table_name);
}

pub fn missingDropTableIfExistsNoopAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
) !AppliedRelationalSqlDdlRecord {
    return try missingQualifiedDropTableIfExistsNoopAlloc(alloc, default_database_name, default_namespace_name, table_name);
}

pub fn missingQualifiedDropTableIfExistsNoopAlloc(
    alloc: std.mem.Allocator,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) !AppliedRelationalSqlDdlRecord {
    return .{
        .table = try metadata_table_manager.cloneTable(alloc, .{
            .table_id = deriveQualifiedTableId(database_name, namespace_name, table_name),
            .name = table_name,
            .database_name = database_name,
            .namespace_name = namespace_name,
        }),
        .noop = true,
    };
}

pub fn applyRelationalCatalogDdlOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    sql: []const u8,
) !?AppliedRelationalSqlDdlRecord {
    return try applyRelationalCatalogDdlOnServiceWithSessionAlloc(alloc, svc, snapshot, sql, catalog_resources.SqlCatalogSession.default());
}

pub fn applyRelationalCatalogDdlOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    sql: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !?AppliedRelationalSqlDdlRecord {
    return try applyRelationalCatalogDdlOnServiceWithSessionAndFunctionBindingsAlloc(alloc, svc, snapshot, sql, session, .{});
}

pub fn applyRelationalCatalogDdlOnServiceWithSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    sql: []const u8,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !?AppliedRelationalSqlDdlRecord {
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (comptime !(@hasDecl(ServiceDeclType, "upsertDatabase") and
        @hasDecl(ServiceDeclType, "removeDatabase") and
        @hasDecl(ServiceDeclType, "upsertNamespace") and
        @hasDecl(ServiceDeclType, "removeNamespace") and
        @hasDecl(ServiceDeclType, "upsertTablespace") and
        @hasDecl(ServiceDeclType, "removeTablespace")))
    {
        return null;
    }

    var plan = try sql_adapter.lowerDdlPlanWithFunctionBindingsAlloc(alloc, sql, function_bindings);
    defer plan.deinit(alloc);
    return try applyRelationalCatalogDdlPlanOnServiceWithSessionAlloc(alloc, svc, snapshot, plan, session);
}

pub fn applyRelationalCatalogDdlPlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: sql_adapter.LoweredDdlPlan,
    session: catalog_resources.SqlCatalogSession,
) !?AppliedRelationalSqlDdlRecord {
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (comptime !(@hasDecl(ServiceDeclType, "upsertDatabase") and
        @hasDecl(ServiceDeclType, "removeDatabase") and
        @hasDecl(ServiceDeclType, "upsertNamespace") and
        @hasDecl(ServiceDeclType, "removeNamespace") and
        @hasDecl(ServiceDeclType, "upsertTablespace") and
        @hasDecl(ServiceDeclType, "removeTablespace")))
    {
        return null;
    }

    switch (plan) {
        .database_catalog => |database_plan| return try applyDatabaseCatalogPlanOnServiceAlloc(alloc, svc, snapshot, database_plan),
        .schema_namespace_catalog => |namespace_plan| return try applyNamespaceCatalogPlanOnServiceAlloc(alloc, svc, snapshot, namespace_plan, session),
        .tablespace_catalog => |tablespace_plan| return try applyTablespaceCatalogPlanOnServiceAlloc(alloc, svc, snapshot, tablespace_plan),
        else => return null,
    }
}

pub fn applyDatabaseCatalogPlanOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: sql_adapter.DatabaseCatalogPlan,
) !AppliedRelationalSqlDdlRecord {
    var applied = try emptyAppliedRelationalSqlDdlRecordAlloc(alloc);
    errdefer applied.deinit(alloc);
    switch (plan) {
        .create => |create| {
            if (findDatabaseByName(snapshot, create.database_name) != null) return error.DatabaseAlreadyExists;
            const database_id = metadata_table_manager.deriveDatabaseId(create.database_name);
            try svc.upsertDatabase(.{
                .database_id = database_id,
                .name = create.database_name,
            });
            try svc.upsertNamespace(.{
                .namespace_id = metadata_table_manager.deriveNamespaceId(database_id, default_namespace_name),
                .database_id = database_id,
                .name = default_namespace_name,
            });
            applied.created_database = true;
        },
        .alter => |alter| {
            const existing = findDatabaseByName(snapshot, alter.database_name) orelse return error.DatabaseNotFound;
            const settings_json = try databaseSettingsJsonAfterAlterAlloc(alloc, existing.settings_json, alter.operations);
            defer alloc.free(settings_json);
            try svc.upsertDatabase(.{
                .database_id = existing.database_id,
                .name = existing.name,
                .settings_json = settings_json,
            });
        },
        .drop => |drop| {
            const existing = findDatabaseByName(snapshot, drop.database_name) orelse {
                if (drop.if_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.DatabaseNotFound;
            };
            if (databaseHasTables(snapshot, existing.database_id)) return error.DatabaseNotEmpty;
            if (drop.force) return error.UnsupportedSqlShape;
            for (snapshot.namespaces) |namespace| {
                if (namespace.database_id == existing.database_id) try svc.removeNamespace(namespace.namespace_id);
            }
            try svc.removeDatabase(existing.database_id);
            applied.dropped_database = true;
        },
    }
    return applied;
}

fn applyNamespaceCatalogPlanOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: sql_adapter.SchemaNamespaceCatalogPlan,
    session: catalog_resources.SqlCatalogSession,
) !AppliedRelationalSqlDdlRecord {
    var applied = try emptyAppliedRelationalSqlDdlRecordAlloc(alloc);
    errdefer applied.deinit(alloc);
    const database_name = session.currentDatabase();
    const database_id = metadata_table_manager.deriveDatabaseId(database_name);
    switch (plan) {
        .create => |create| {
            const target = try session.namespaceTargetFromSchemaName(create.schema_name);
            if (findNamespaceByName(snapshot, target.database_name, target.namespace_name) != null) {
                if (create.if_not_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.NamespaceAlreadyExists;
            }
            if (findDatabaseByName(snapshot, target.database_name) == null) {
                try svc.upsertDatabase(.{
                    .database_id = database_id,
                    .name = target.database_name,
                });
            }
            try svc.upsertNamespace(.{
                .namespace_id = metadata_table_manager.deriveNamespaceId(database_id, target.namespace_name),
                .database_id = database_id,
                .name = target.namespace_name,
            });
            applied.created_namespace = true;
        },
        .rename => |rename| {
            const existing_target = try session.namespaceTargetFromSchemaName(rename.schema_name);
            const new_target = try session.namespaceTargetFromSchemaName(rename.new_schema_name);
            const existing = findNamespaceByName(snapshot, existing_target.database_name, existing_target.namespace_name) orelse return error.NamespaceNotFound;
            if (findNamespaceByName(snapshot, new_target.database_name, new_target.namespace_name) != null) return error.NamespaceAlreadyExists;
            try svc.upsertNamespace(.{
                .namespace_id = metadata_table_manager.deriveNamespaceId(database_id, new_target.namespace_name),
                .database_id = database_id,
                .name = new_target.namespace_name,
            });
            for (snapshot.tables) |table| {
                if (!std.mem.eql(u8, table.database_name, existing_target.database_name)) continue;
                if (!std.mem.eql(u8, table.namespace_name, existing_target.namespace_name)) continue;
                var renamed_table = table;
                renamed_table.namespace_name = new_target.namespace_name;
                try svc.upsertTable(renamed_table);
            }
            try svc.removeNamespace(existing.namespace_id);
            applied.renamed_namespace = true;
        },
        .drop => |drop| {
            const target = try session.namespaceTargetFromSchemaName(drop.schema_name);
            const existing = findNamespaceByName(snapshot, target.database_name, target.namespace_name) orelse {
                if (drop.if_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.NamespaceNotFound;
            };
            if (namespaceHasTables(snapshot, database_id, target.namespace_name)) {
                if (drop.cascade) return error.UnsupportedSqlShape;
                return error.NamespaceNotEmpty;
            }
            try svc.removeNamespace(existing.namespace_id);
            applied.dropped_namespace = true;
        },
    }
    return applied;
}

pub fn applyTablespaceCatalogPlanOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: sql_adapter.TablespaceCatalogPlan,
) !AppliedRelationalSqlDdlRecord {
    var applied = try emptyAppliedRelationalSqlDdlRecordAlloc(alloc);
    errdefer applied.deinit(alloc);
    switch (plan) {
        .create => |create| {
            if (findTablespaceByName(snapshot, create.tablespace_name) != null) return error.TablespaceAlreadyExists;
            try validateTablespaceLocationJson(alloc, create.location_json);
            try validateTablespacePlacementPolicyJson(alloc, create.placement_policy_json);
            try svc.upsertTablespace(.{
                .tablespace_id = metadata_table_manager.deriveTablespaceId(create.tablespace_name),
                .name = create.tablespace_name,
                .location_json = create.location_json,
                .placement_policy_json = create.placement_policy_json,
            });
            applied.created_tablespace = true;
        },
        .rename => |rename| {
            const existing = findTablespaceByName(snapshot, rename.tablespace_name) orelse return error.TablespaceNotFound;
            if (findTablespaceByName(snapshot, rename.new_tablespace_name) != null) return error.TablespaceAlreadyExists;
            try svc.upsertTablespace(.{
                .tablespace_id = metadata_table_manager.deriveTablespaceId(rename.new_tablespace_name),
                .name = rename.new_tablespace_name,
                .location_json = existing.location_json,
                .placement_policy_json = existing.placement_policy_json,
            });
            try rewriteTablespaceReferencesOnService(svc, snapshot, rename.tablespace_name, rename.new_tablespace_name);
            try svc.removeTablespace(existing.tablespace_id);
            applied.renamed_tablespace = true;
        },
        .drop => |drop| {
            const existing = findTablespaceByName(snapshot, drop.tablespace_name) orelse {
                if (drop.if_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.TablespaceNotFound;
            };
            if (tablespaceHasReferences(snapshot, drop.tablespace_name)) return error.TablespaceInUse;
            try svc.removeTablespace(existing.tablespace_id);
            applied.dropped_tablespace = true;
        },
    }
    return applied;
}

fn tablespaceHasReferences(snapshot: *const metadata_api.AdminSnapshot, tablespace_name: []const u8) bool {
    for (snapshot.databases) |database| {
        if (std.mem.eql(u8, database.tablespace_name, tablespace_name)) return true;
    }
    for (snapshot.namespaces) |namespace| {
        if (std.mem.eql(u8, namespace.tablespace_name, tablespace_name)) return true;
    }
    for (snapshot.tables) |table| {
        if (std.mem.eql(u8, table.tablespace_name, tablespace_name)) return true;
    }
    return false;
}

fn rewriteTablespaceReferencesOnService(
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    old_tablespace_name: []const u8,
    new_tablespace_name: []const u8,
) !void {
    for (snapshot.databases) |database| {
        if (!std.mem.eql(u8, database.tablespace_name, old_tablespace_name)) continue;
        var updated = database;
        updated.tablespace_name = new_tablespace_name;
        try svc.upsertDatabase(updated);
    }
    for (snapshot.namespaces) |namespace| {
        if (!std.mem.eql(u8, namespace.tablespace_name, old_tablespace_name)) continue;
        var updated = namespace;
        updated.tablespace_name = new_tablespace_name;
        try svc.upsertNamespace(updated);
    }
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.tablespace_name, old_tablespace_name)) continue;
        var updated = table;
        updated.tablespace_name = new_tablespace_name;
        try svc.upsertTable(updated);
    }
}

fn validateTablespaceLocationJson(alloc: std.mem.Allocator, location_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, location_json, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .null, .string, .object => {},
        else => return error.InvalidTablespaceLocation,
    }
}

fn validateTablespacePlacementPolicyJson(alloc: std.mem.Allocator, placement_policy_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, placement_policy_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTablespacePlacementPolicy,
    };
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!tablespacePlacementPolicyKeySupported(entry.key_ptr.*)) return error.InvalidTablespacePlacementPolicy;
    }
    if (root.get("placement_role")) |value| _ = try parseTablespacePlacementRole(value);
    if (root.get("desired_replica_count")) |value| _ = try parseTablespaceReplicaCount(value);
    if (root.get("replica_count")) |value| _ = try parseTablespaceReplicaCount(value);
    if (root.get("min_ranges")) |value| _ = try parseTablespaceMinRanges(value);
}

fn databaseHasTables(snapshot: *const metadata_api.AdminSnapshot, database_id: u64) bool {
    for (snapshot.tables) |table| {
        if (metadata_table_manager.deriveDatabaseId(table.database_name) == database_id) return true;
    }
    return false;
}

fn namespaceHasTables(snapshot: *const metadata_api.AdminSnapshot, database_id: u64, namespace_name: []const u8) bool {
    for (snapshot.tables) |table| {
        if (metadata_table_manager.deriveDatabaseId(table.database_name) != database_id) continue;
        if (std.mem.eql(u8, table.namespace_name, namespace_name)) return true;
    }
    return false;
}

fn databaseSettingsJsonAfterAlterAlloc(
    alloc: std.mem.Allocator,
    settings_json: []const u8,
    operations: []const sql_adapter.DatabaseAlterOperation,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, settings_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSchemaUpdateRequest;

    for (operations) |operation| {
        switch (operation) {
            .set_parameter => |set| {
                const value = try std.json.parseFromSliceLeaky(std.json.Value, parsed.arena.allocator(), set.value_json, .{});
                const setting_value = try sqlDatabaseSettingTextFromJsonValueAlloc(alloc, value);
                defer alloc.free(setting_value);
                try sql_adapter.validateSqlDatabaseSettingValue(set.name, setting_value);
                try parsed.value.object.put(parsed.arena.allocator(), try parsed.arena.allocator().dupe(u8, set.name), value);
            },
        }
    }
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn sqlDatabaseSettingTextFromJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .bool => |enabled| try alloc.dupe(u8, if (enabled) "true" else "false"),
        else => error.InvalidRoleSetting,
    };
}

pub fn validateRelationalTableDropAllowed(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    target_table: metadata_table_manager.TableRecord,
) !void {
    for (snapshot.tables) |candidate_table| {
        if (candidate_table.table_id == target_table.table_id) continue;
        if (candidate_table.schema_json.len == 0) continue;
        var parsed_child = try parseValidatedTableSchema(alloc, candidate_table.schema_json);
        defer parsed_child.deinit(alloc);
        const child_schema = try deriveRuntimeTableSchema(alloc, parsed_child);
        defer runtime_schema_mod.freeSchema(alloc, child_schema);
        if (child_schema.storage_mode != .relational) continue;
        for (child_schema.foreign_keys) |foreign_key| {
            if (std.mem.eql(u8, foreign_key.parent_table, target_table.name)) return error.TableReferencedByForeignKey;
        }
    }
}

pub fn schemaWithoutForeignKeysReferencingTableAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    parent_table_name: []const u8,
) !?[]u8 {
    if (schema_json.len == 0) return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const foreign_keys = root.getPtr("foreign_keys") orelse return null;
    const foreign_key_array = switch (foreign_keys.*) {
        .array => |*array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var changed = false;
    var i: usize = 0;
    while (i < foreign_key_array.items.len) {
        const foreign_key = switch (foreign_key_array.items[i]) {
            .object => |*object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const references = foreign_key.get("references") orelse return error.InvalidSchemaUpdateRequest;
        if (references != .object) return error.InvalidSchemaUpdateRequest;
        const table = references.object.get("table") orelse return error.InvalidSchemaUpdateRequest;
        if (table != .string) return error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, table.string, parent_table_name)) {
            i += 1;
            continue;
        }
        _ = foreign_key_array.orderedRemove(i);
        changed = true;
    }
    if (!changed) return null;
    if (foreign_key_array.items.len == 0) _ = root.orderedRemove("foreign_keys");

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    validated.deinit(alloc);
    return updated;
}

pub fn validateRelationalForeignKeyCatalogReferences(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    candidate_table: metadata_table_manager.TableRecord,
) !void {
    if (candidate_table.schema_json.len == 0) return;
    var parsed_child = try parseValidatedTableSchema(alloc, candidate_table.schema_json);
    defer parsed_child.deinit(alloc);
    const child_schema = try deriveRuntimeTableSchema(alloc, parsed_child);
    defer runtime_schema_mod.freeSchema(alloc, child_schema);
    if (child_schema.storage_mode != .relational) return;

    for (child_schema.foreign_keys) |foreign_key| {
        const parent_table_name = if (std.mem.eql(u8, foreign_key.parent_table, child_schema.default_type))
            candidate_table.name
        else
            foreign_key.parent_table;
        const parent_table = if (std.mem.eql(u8, parent_table_name, candidate_table.name))
            candidate_table
        else
            (findTableByName(snapshot, parent_table_name) orelse return error.InvalidSchemaUpdateRequest).*;

        var parsed_parent = try parseValidatedTableSchema(alloc, parent_table.schema_json);
        defer parsed_parent.deinit(alloc);
        const parent_schema = try deriveRuntimeTableSchema(alloc, parsed_parent);
        defer runtime_schema_mod.freeSchema(alloc, parent_schema);
        if (parent_schema.storage_mode != .relational) return error.InvalidSchemaUpdateRequest;

        if (foreignKeyReferencesPrimaryKey(foreign_key)) continue;
        if (!primaryKeyColumnsEqual(parent_schema.primary_key, foreign_key.parent_columns)) {
            _ = findUniqueConstraintByColumns(parent_schema.unique_constraints, foreign_key.parent_columns) orelse return error.InvalidSchemaUpdateRequest;
        }
        if (foreign_key.child_columns.len != foreign_key.parent_columns.len) return error.InvalidSchemaUpdateRequest;
        for (foreign_key.child_columns, foreign_key.parent_columns) |child_column_name, parent_column_name| {
            const child_column = findRelationalColumn(child_schema.relational_columns, child_column_name) orelse return error.InvalidSchemaUpdateRequest;
            const parent_column = findRelationalColumn(parent_schema.relational_columns, parent_column_name) orelse return error.InvalidSchemaUpdateRequest;
            if (child_column.field_type != parent_column.field_type) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn foreignKeyReferencesPrimaryKey(foreign_key: runtime_schema_mod.ForeignKey) bool {
    return foreign_key.parent_columns.len == 1 and std.mem.eql(u8, foreign_key.parent_columns[0], "_id");
}

fn primaryKeyColumnsEqual(primary_key: ?runtime_schema_mod.PrimaryKey, columns: []const []const u8) bool {
    const key = primary_key orelse return false;
    return stringSlicesEqual(key.columns, columns);
}

fn findUniqueConstraintByColumns(constraints: []const runtime_schema_mod.UniqueConstraint, columns: []const []const u8) ?runtime_schema_mod.UniqueConstraint {
    for (constraints) |constraint| {
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

fn findRelationalColumn(columns: []const runtime_schema_mod.RelationalColumn, name: []const u8) ?runtime_schema_mod.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn deriveId(name: []const u8, seed: u64) u64 {
    const id = std.hash.Wyhash.hash(seed, name);
    return if (id == 0) 1 else id;
}

fn deriveDataGroupId(name: []const u8, seed: u64) u64 {
    return group_ids.dataGroupIdFromHash(std.hash.Wyhash.hash(seed, name));
}

fn deriveQualifiedInitialDataGroupId(database_name: []const u8, namespace_name: []const u8, table_name: []const u8) u64 {
    if (isDefaultCatalogIdentity(database_name, namespace_name)) return deriveDataGroupId(table_name, 0x47525031);
    var hasher = std.hash.Wyhash.init(0x47525031);
    updateCatalogQualifiedHasher(&hasher, database_name, namespace_name, table_name);
    return group_ids.dataGroupIdFromHash(hasher.final());
}

fn deriveShardGroupId(table_name: []const u8, shard_index: u32) u64 {
    var hasher = std.hash.Wyhash.init(0x47525031);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(std.mem.asBytes(&shard_index));
    return group_ids.dataGroupIdFromHash(hasher.final());
}

fn deriveQualifiedShardGroupId(
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
    shard_index: u32,
) u64 {
    if (isDefaultCatalogIdentity(database_name, namespace_name)) return deriveShardGroupId(table_name, shard_index);
    var hasher = std.hash.Wyhash.init(0x47525031);
    updateCatalogQualifiedHasher(&hasher, database_name, namespace_name, table_name);
    hasher.update(&[_]u8{0});
    hasher.update(std.mem.asBytes(&shard_index));
    return group_ids.dataGroupIdFromHash(hasher.final());
}

fn isDefaultCatalogIdentity(database_name: []const u8, namespace_name: []const u8) bool {
    return std.mem.eql(u8, database_name, default_database_name) and
        std.mem.eql(u8, namespace_name, default_namespace_name);
}

fn updateCatalogQualifiedHasher(
    hasher: *std.hash.Wyhash,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) void {
    hasher.update(database_name);
    hasher.update(&[_]u8{0});
    hasher.update(namespace_name);
    hasher.update(&[_]u8{0});
    hasher.update(table_name);
}

fn deriveShardBoundaryKey(alloc: std.mem.Allocator, shard_index: u32, shard_count: u32) ![]u8 {
    const pos = (@as(u32, shard_index) * 65536) / shard_count;
    const hi: u8 = @truncate(pos >> 8);
    const lo: u8 = @truncate(pos & 0xff);
    if (lo == 0) {
        return try std.fmt.allocPrint(alloc, "{x:0>2}", .{hi});
    }
    return try std.fmt.allocPrint(alloc, "{x:0>2}{x:0>2}", .{ hi, lo });
}

test "metadata.table status encoder emits antfly-style shard map" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .description = "docs table", .schema_json = "{\"kind\":\"demo\"}", .read_schema_json = "{\"version\":0}", .indexes_json = "{\"full_text_index_v0\":{}}", .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://seed\",\"postgres_table\":\"seed_docs\"}]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "doc:a", .end_key = "doc:z" }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try std.testing.expectEqualStrings("docs", root.get("name").?.string);
    try std.testing.expectEqualStrings("docs table", root.get("description").?.string);
    try std.testing.expect(root.get("schema") != null);
    try std.testing.expect(root.get("migration") != null);
    try std.testing.expect(root.get("indexes") != null);
    try std.testing.expect(root.get("replication_sources") != null);
    try std.testing.expect(root.get("shards") != null);

    const shards = root.get("shards").?.object;
    const shard = shards.get("7001").?.object;
    const byte_range = shard.get("byte_range").?.array.items;
    try std.testing.expectEqualStrings("doc:a", byte_range[0].string);
    try std.testing.expectEqualStrings("doc:z", byte_range[1].string);

    const replication_sources = root.get("replication_sources").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), replication_sources.len);
    try std.testing.expectEqualStrings("postgres", replication_sources[0].object.get("type").?.string);
}

test "metadata.table detail encoder includes replication source status and action hint" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = default_indexes_json, .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        .replication_source_statuses = @constCast((&[_]metadata_table_manager.ReplicationSourceStatusRecord{.{
            .table_id = 7,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .cutover_mode = "slot_resumed",
            .slot_name = "slot_old",
            .publication_name = "pub_old",
            .phase = "streaming",
            .checkpoint = "lsn:0/10",
            .last_error = "",
        }})[0..]),
        .replication_source_action_hints = @constCast((&[_]metadata_api.ReplicationSourceActionHint{.{
            .table_id = 7,
            .table_name = @constCast("docs"),
            .source_ordinal = 0,
            .action = "reseed_exact_cutover",
            .reason = "existing_slot_non_exact_cutover",
            .reseed_exact_cutover_path = @constCast("/internal/v1/tables/docs/replication-sources/0/reseed-exact-cutover"),
        }})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replication_sources\":[{\"type\":\"postgres\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"status\":{\"source_kind\":\"postgres\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"cutover_mode\":\"slot_resumed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"action_hint\":{\"action\":\"reseed_exact_cutover\"") != null);

    const listed = try encodeTableList(std.testing.allocator, &snapshot, null);
    defer std.testing.allocator.free(listed);
    try std.testing.expect(std.mem.indexOf(u8, listed, "\"action_hint\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, listed, "\"status\":{\"source_kind\":\"postgres\"") == null);
}

test "metadata.table status encoder honors storage status overrides" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = default_indexes_json, .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = true,
        .lsm = .{
            .run_count = 3,
            .run_bytes = 44,
            .l0_run_count = 1,
            .l0_bytes = 33,
            .lower_level_run_count = 2,
            .lower_level_bytes = 11,
            .max_level = 3,
            .compactable_l0_run_count = 4,
            .overlapping_l0_run_count = 5,
            .soft_limit_l0_run_count = 6,
            .hard_limit_l0_run_count = 7,
            .write_stall_l0_run_debt = 8,
            .soft_limit_l0_bytes = 9,
            .hard_limit_l0_bytes = 10,
            .write_stall_l0_byte_debt = 12,
            .level_overflow_run_count = 13,
            .level_overflow_bytes = 14,
            .obsolete_path_count = 15,
            .obsolete_paths_pinned_by_readers = 130,
            .obsolete_paths_pinned_by_versions = 131,
            .obsolete_paths_waiting_for_retry = 132,
            .obsolete_paths_reclaimable = 133,
            .obsolete_delete_failures = 134,
            .obsolete_delete_retries = 135,
            .current_manifest_bytes = 16,
            .mutable_entry_count = 17,
            .mutable_bytes = 18,
            .immutable_memtable_count = 19,
            .immutable_entry_count = 20,
            .immutable_bytes = 21,
            .mutable_snapshot_clone_count = 22,
            .mutable_snapshot_clone_bytes = 23,
            .mutable_snapshot_clone_peak_bytes = 24,
            .read_snapshot_mutable_rotation_count = 25,
            .read_snapshot_mutable_rotation_bytes = 26,
            .wal_retained_bytes = 55,
            .compaction_backlog_bytes = 10,
            .active_readers = 2,
            .active_bulk_ingest_batches = 1,
            .manifest_dirty = true,
            .obsolete_manifest_dirty = true,
            .maintenance_score = 99,
            .maintenance_debt_hint = 88,
            .flush_count = 101,
            .flush_output_run_count = 102,
            .flush_output_bytes = 103,
            .sorted_ingest_run_count = 104,
            .sorted_ingest_bytes = 105,
            .manifest_write_count = 106,
            .manifest_bytes = 107,
            .write_pressure_event_count = 108,
            .write_pressure_compaction_count = 109,
            .write_pressure_compaction_step_count = 110,
            .write_pressure_overload_count = 111,
            .write_pressure_overload_l0_run_debt = 112,
            .immutable_rotation_count = 113,
            .immutable_flush_count = 114,
            .bulk_append_attempt_count = 119,
            .bulk_append_entry_count = 120,
            .bulk_append_direct_success_count = 121,
            .bulk_append_direct_entry_count = 122,
            .bulk_append_fallback_backend_pending_count = 123,
            .bulk_append_fallback_below_threshold_count = 124,
            .bulk_append_fallback_duplicate_key_count = 125,
            .bulk_append_fallback_to_mutable_entry_count = 126,
            .direct_bulk_ingest_attempt_count = 115,
            .direct_bulk_ingest_success_count = 116,
            .direct_bulk_ingest_entry_count = 117,
            .direct_bulk_ingest_direct_entry_count = 118,
            .direct_bulk_ingest_fallback_unsupported_count = 127,
            .direct_bulk_ingest_fallback_backend_mutable_count = 128,
            .direct_bulk_ingest_fallback_below_threshold_count = 129,
        },
    }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"storage_status\":{\"disk_usage\":0,\"empty\":true,\"lsm\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"run_count\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"l0_bytes\":33") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lower_level_run_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"max_level\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"compactable_l0_run_count\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"overlapping_l0_run_count\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"hard_limit_l0_run_count\":7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"write_stall_l0_run_debt\":8") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"level_overflow_run_count\":13") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_path_count\":15") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_paths_pinned_by_readers\":130") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_paths_pinned_by_versions\":131") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_paths_waiting_for_retry\":132") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_paths_reclaimable\":133") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_delete_failures\":134") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_delete_retries\":135") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_manifest_bytes\":16") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"mutable_entry_count\":17") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"mutable_bytes\":18") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"immutable_memtable_count\":19") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"immutable_bytes\":21") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"mutable_snapshot_clone_count\":22") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"read_snapshot_mutable_rotation_count\":25") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_retained_bytes\":55") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"compaction_backlog_bytes\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_readers\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_bulk_ingest_batches\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"manifest_dirty\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"obsolete_manifest_dirty\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"maintenance_score\":99") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"maintenance_debt_hint\":88") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"flush_count\":101") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"flush_output_run_count\":102") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"flush_output_bytes\":103") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"sorted_ingest_run_count\":104") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"sorted_ingest_bytes\":105") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"manifest_write_count\":106") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"manifest_bytes\":107") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"write_pressure_event_count\":108") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"write_pressure_compaction_count\":109") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"write_pressure_compaction_step_count\":110") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"write_pressure_overload_count\":111") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"write_pressure_overload_l0_run_debt\":112") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"immutable_rotation_count\":113") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"immutable_flush_count\":114") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"bulk_append_attempt_count\":119") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"bulk_append_direct_success_count\":121") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"bulk_append_fallback_backend_pending_count\":123") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"bulk_append_fallback_below_threshold_count\":124") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"direct_bulk_ingest_attempt_count\":115") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"direct_bulk_ingest_success_count\":116") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"direct_bulk_ingest_entry_count\":117") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"direct_bulk_ingest_direct_entry_count\":118") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"direct_bulk_ingest_fallback_backend_mutable_count\":128") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"direct_bulk_ingest_fallback_below_threshold_count\":129") != null);
}

test "metadata.table status encoder canonicalizes embeddings indexes without inline names" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = "{\"semantic_kg\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"url\":\"http://127.0.0.1:11434/v1\"}}}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"semantic_kg\":{\"name\":\"semantic_kg\",\"type\":\"embeddings\"") != null);
}

test "metadata.table status encoder projects inline enrichment configs as names" {
    const indexes_json =
        \\{
        \\  "document_text":{
        \\    "type":"full_text",
        \\    "artifact_name":"document_chunks_v1",
        \\    "enrichments":[
        \\      {"name":"document_units_v1","kind":"asset","field":"url","producer_json":"{\"type\":\"document_extraction\"}"},
        \\      {"name":"document_chunks_v1","kind":"chunk","source_artifact_name":"document_units_v1","field":"text"}
        \\    ]
        \\  },
        \\  "document_vectors":{
        \\    "type":"embeddings",
        \\    "field":"embedding",
        \\    "dims":768,
        \\    "metric":"cosine",
        \\    "enrichments":[
        \\      {"name":"document_chunk_dense_v1","kind":"embedding","source_artifact_name":"document_chunks_v1","field":"text"}
        \\    ]
        \\  }
        \\}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = indexes_json, .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"enrichments\":[\"document_units_v1\",\"document_chunks_v1\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"enrichments\":[\"document_chunk_dense_v1\"]") != null);
}

test "metadata.table debug encoder emits runtime schemas and index bindings" {
    const schema_v1 =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"string","x-antfly-types":["text"],"x-antfly-analyzer":"french"}}}}}}
    ;
    const schema_v0 =
        \\{"version":0,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"name":{"type":"string","x-antfly-types":["search_as_you_type"]}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .schema_json = schema_v1,
            .read_schema_json = schema_v0,
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
            .replication_sources_json = "[]",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatusWithRuntimeSchemaDebug(std.testing.allocator, &snapshot, "docs", null)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"debug\":{\"runtime_schemas\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"slot\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"slot\":\"read\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_name\":\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"schema_slot\":\"read\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"schema_slot\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"analyzer\":\"french\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"algebraic_capabilities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"capability_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lifecycle_status\":\"rebuild_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"requires_rebuild\":true") != null);
    // The schema-derived config carries default materializations (a per-group
    // count for the single string group field).
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\":[]") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"op\":\"count\"") != null);
}

test "create table parser preserves supported metadata fields" {
    var parsed = try parseCreateTableRequest(std.testing.allocator, "{\"num_shards\":1,\"description\":\"docs table\",\"schema\":{\"kind\":\"demo\"},\"indexes\":{\"default\":{}},\"typed_paths\":{\"numeric\":[\"metrics.score\"],\"keyword\":\"status\"},\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]}");
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u32, 1), parsed.num_shards);
    try std.testing.expectEqualStrings("docs table", parsed.description.?);
    try std.testing.expectEqualStrings("{\"version\":0,\"kind\":\"demo\"}", parsed.schema_json.?);
    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"default\":{}") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"typed_paths\":{\"numeric\":[\"metrics.score\"],\"keyword\":\"status\"}") != null);
    try std.testing.expectEqualStrings("[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]", parsed.replication_sources_json.?);
}

test "schema-derived algebraic indexes expand into explicit capability config" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"customer":{"type":"keyword"},"amount":{"type":"number"},"created_at":{"type":"datetime"}}}}}}
    ;
    const indexes_json =
        \\{"sales_rollup":{"type":"algebraic","derive_from_schema":true}}
    ;

    try validatePublicAlgebraicIndexesJson(alloc, indexes_json);
    const expanded = try expandSchemaDerivedAlgebraicIndexesAlloc(alloc, "orders", indexes_json, schema_json);
    defer alloc.free(expanded);

    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"type\":\"algebraic\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"table\":\"orders\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"measure_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"time_fields\"") != null);
    // Default materializations are derived from the schema's group/measure fields.
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"materializations\":[]") == null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"op\":\"count\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"op\":\"sum\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"op\":\"avg\"") != null);
    // No user-named materializations are injected.
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"sum_by_customer\"") == null);
}

test "schema-derived algebraic indexes require a schema" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        expandSchemaDerivedAlgebraicIndexesAlloc(
            std.testing.allocator,
            "orders",
            "{\"sales_rollup\":{\"type\":\"algebraic\",\"derive_from_schema\":true}}",
            "",
        ),
    );
}

test "single schema-derived algebraic index expands into explicit capability config" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"customer":{"type":"keyword"},"amount":{"type":"number"}}}}}}
    ;
    const index_json =
        \\{"type":"algebraic","derive_from_schema":true}
    ;
    try validatePublicAlgebraicIndexJson(alloc, index_json);
    const expanded = try expandSchemaDerivedAlgebraicIndexAlloc(
        alloc,
        "orders",
        index_json,
        schema_json,
    );
    defer alloc.free(expanded);

    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"measure_fields\"") != null);
    // Default materializations are derived from the schema's group/measure fields.
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"materializations\":[]") == null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"op\":\"count\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"op\":\"avg\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"sum_by_customer\"") == null);
}

test "public algebraic index definitions cannot declare internal materializations" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        validatePublicAlgebraicIndexJson(alloc, "{\"type\":\"algebraic\",\"materializations\":[]}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        validatePublicAlgebraicIndexJson(alloc, "{\"type\":\"algebraic\",\"derive_from_schema\":true,\"materializations\":[]}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        validatePublicAlgebraicIndexJson(alloc, "{\"type\":\"algebraic\",\"derive_from_schema\":true,\"group_fields\":[]}"),
    );
}

test "derived index field refs validate against relational and embedded json schema" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"body":{"type":"text"},"embedding":{"type":"embedding"},"source_doc":{"type":"keyword"},"target_doc":{"type":"keyword"},"edge_type":{"type":"keyword"},"confidence":{"type":"numeric"},"attrs":{"type":"json","schema":{"type":"object","properties":{"title":{"type":"text"},"plan":{"type":"keyword"},"source":{"type":"keyword"},"target":{"type":"keyword"},"edge_type":{"type":"keyword"},"confidence":{"type":"numeric"}},"additionalProperties":true}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}}
    ;

    try validateDerivedIndexFieldRefsForSchemaAlloc(
        alloc,
        "{\"type\":\"full_text\",\"field\":\"body\"}",
        schema_json,
    );
    try validateDerivedIndexFieldRefsForSchemaAlloc(
        alloc,
        "{\"type\":\"full_text\",\"field\":\"attrs.title\"}",
        schema_json,
    );
    try validateDerivedIndexFieldRefsForSchemaAlloc(
        alloc,
        "{\"type\":\"embeddings\",\"field\":\"attrs.plan\",\"enrichments\":[{\"name\":\"plan_embedding\",\"kind\":\"embedding\",\"field\":\"attrs.plan\"}]}",
        schema_json,
    );
    try validateDerivedIndexFieldRefsForSchemaAlloc(
        alloc,
        "{\"type\":\"graph\",\"edge_table\":{\"source_field\":\"attrs.source\",\"target_field\":\"attrs.target\",\"type_field\":\"attrs.edge_type\",\"weight_field\":\"attrs.confidence\"}}",
        schema_json,
    );
    try validateDerivedIndexFieldRefsForSchemaAlloc(
        alloc,
        "{\"type\":\"graph\",\"edge_table\":{\"source_field\":\"source_doc\",\"target_field\":\"target_doc\"},\"edge_policy\":\"all\"}",
        schema_json,
    );
    try validateDerivedIndexFieldRefsForSchemaAlloc(
        alloc,
        "{\"type\":\"graph\",\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"body\",\"content_type\":\"application/json\",\"producer_json\":{\"type\":\"extractor\",\"model\":\"relations\"}},\"nodes\":{\"model\":\"document\",\"source\":\"{{ _doc.key }}\",\"target\":\"{{ _item.target.document_id }}\"},\"edge\":{\"type\":\"{{ _item.type }}\",\"weight\":\"{{ _item.confidence }}\"},\"context\":{\"doc_fields\":[\"id\"]}}",
        schema_json,
    );

    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"full_text\",\"field\":\"missing\"}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"graph\",\"edge_table\":{\"source_field\":\"body.source\",\"target_field\":\"target_doc\"}}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"graph\",\"edge_table\":{\"source_field\":\"source_doc\"}}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"graph\",\"edge_table\":{\"source_field\":\"source_doc\",\"target_field\":\"target_doc\",\"type_field\":\"missing_kind\"}}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"graph\",\"edge_table\":{\"source_field\":\"source_doc\",\"target_field\":\"target_doc\"},\"edge_policy\":\"surprise\"}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"graph\",\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"missing_body\"}}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"embeddings\",\"field\":\"body\",\"embedder\":{\"model\":\"\"}}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"embeddings\",\"field\":\"body\",\"enrichments\":[{\"name\":\"body_embedding\",\"kind\":\"embedding\",\"field\":\"body\",\"model\":\"\"}]}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"graph\",\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"body\",\"content_type\":\"application/json\",\"producer_json\":{\"type\":\"extractor\",\"model\":\"\"}},\"nodes\":{\"model\":\"document\",\"source\":\"{{ _doc.key }}\",\"target\":\"{{ _item.target.document_id }}\"},\"edge\":{\"type\":\"{{ _item.type }}\",\"weight\":\"{{ _item.confidence }}\"},\"context\":{\"doc_fields\":[\"id\"]}}",
            schema_json,
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        validateDerivedIndexFieldRefsForSchemaAlloc(
            alloc,
            "{\"type\":\"embeddings\",\"field\":\"attrs..title\"}",
            schema_json,
        ),
    );
}

test "create table parser preserves postgres slot and publication metadata fields" {
    var parsed = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"slot_name\":\"custom_slot\",\"publication_name\":\"custom_pub\"}]}",
    );
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"slot_name\":\"custom_slot\",\"publication_name\":\"custom_pub\"}]",
        parsed.replication_sources_json.?,
    );
}

test "create table parser preserves postgres exact cutover requirement metadata field" {
    var parsed = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"require_exact_cutover\":true}]}",
    );
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"require_exact_cutover\":true}]",
        parsed.replication_sources_json.?,
    );
}

test "create table parser accepts durable tablespace binding" {
    var parsed = try parseCreateTableRequest(std.testing.allocator, "{\"tablespace_name\":\"fastspace\"}");
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("fastspace", parsed.tablespace_name.?);
    const record = deriveTableRecord("docs", parsed);
    try std.testing.expectEqualStrings("fastspace", record.tablespace_name);
    try std.testing.expectError(error.InvalidCreateTableRequest, parseCreateTableRequest(std.testing.allocator, "{\"tablespace_name\":\"tenant.fast\"}"));
}

test "tablespace policy inherits through namespace and maps to native placement" {
    var databases = [_]metadata_table_manager.DatabaseRecord{.{
        .database_id = metadata_table_manager.deriveDatabaseId("tenant_ops"),
        .name = "tenant_ops",
        .tablespace_name = "coldspace",
    }};
    var namespaces = [_]metadata_table_manager.NamespaceRecord{.{
        .namespace_id = metadata_table_manager.deriveNamespaceId(databases[0].database_id, "analytics"),
        .database_id = databases[0].database_id,
        .name = "analytics",
        .tablespace_name = "hotspace",
    }};
    var tablespaces = [_]metadata_table_manager.TablespaceRecord{
        .{
            .tablespace_id = metadata_table_manager.deriveTablespaceId("hotspace"),
            .name = "hotspace",
            .placement_policy_json = "{\"placement_role\":\"hot\",\"desired_replica_count\":2,\"min_ranges\":4}",
        },
        .{
            .tablespace_id = metadata_table_manager.deriveTablespaceId("coldspace"),
            .name = "coldspace",
            .placement_policy_json = "{\"placement_role\":\"cold\",\"replica_count\":1}",
        },
    };
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .databases = databases[0..],
        .namespaces = namespaces[0..],
        .tablespaces = tablespaces[0..],
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };

    const inherited = effectiveTablespaceForTarget(&snapshot, "tenant_ops", "analytics", null).?;
    try std.testing.expectEqualStrings("hotspace", inherited.name);
    const base = deriveQualifiedTableRecord("tenant_ops", "analytics", "events", .{});
    const applied = try applyTablespacePlacementPolicyAlloc(std.testing.allocator, base, inherited);
    defer metadata_table_manager.freeTable(std.testing.allocator, applied);
    try std.testing.expectEqualStrings("hotspace", applied.tablespace_name);
    try std.testing.expectEqualStrings("hot", applied.placement_role);
    try std.testing.expectEqual(@as(u16, 2), applied.desired_replica_count);
    try std.testing.expectEqual(@as(u32, 4), applied.min_ranges);

    const explicit = effectiveTablespaceForTarget(&snapshot, "tenant_ops", "analytics", "coldspace").?;
    try std.testing.expectEqualStrings("coldspace", explicit.name);

    try std.testing.expectError(
        error.InvalidTablespacePlacementPolicy,
        applyTablespacePlacementPolicyAlloc(std.testing.allocator, base, &.{
            .tablespace_id = metadata_table_manager.deriveTablespaceId("badspace"),
            .name = "badspace",
            .placement_policy_json = "{\"placement_role\":\"unknown\"}",
        }),
    );
    try std.testing.expectError(
        error.InvalidTablespacePlacementPolicy,
        validateTablespacePlacementPolicyJson(std.testing.allocator, "{\"future_scheduler_knob\":true}"),
    );
}

test "derive initial ranges honors shard count" {
    const table = deriveTableRecord("docs", .{ .num_shards = 4 });
    const ranges = try deriveInitialRanges(std.testing.allocator, table);
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(std.testing.allocator, record);
        std.testing.allocator.free(ranges);
    }

    try std.testing.expectEqual(@as(usize, 4), ranges.len);
    try std.testing.expectEqual(table.table_id, ranges[0].table_id);
    try std.testing.expectEqualStrings("", ranges[0].start_key);
    try std.testing.expectEqualStrings("40", ranges[0].end_key.?);
    try std.testing.expectEqualStrings("40", ranges[1].start_key);
    try std.testing.expectEqualStrings("80", ranges[1].end_key.?);
    try std.testing.expectEqualStrings("80", ranges[2].start_key);
    try std.testing.expectEqualStrings("c0", ranges[2].end_key.?);
    try std.testing.expectEqualStrings("c0", ranges[3].start_key);
    try std.testing.expect(ranges[3].end_key == null);
}

test "table catalog identity defaults table records to public namespace in default database" {
    const table = deriveTableRecord("docs", .{});
    try std.testing.expectEqual(deriveTableId("docs"), table.table_id);
    try std.testing.expectEqualStrings(default_database_name, table.database_name);
    try std.testing.expectEqualStrings(default_namespace_name, table.namespace_name);
    try std.testing.expectEqualStrings("docs", table.name);
}

test "table catalog identity scopes lookup and derived ids" {
    const default_table = deriveTableRecord("docs", .{});
    const tenant_table: metadata_table_manager.TableRecord = .{
        .table_id = deriveQualifiedTableId("tenant_ops", "billing", "docs"),
        .name = "docs",
        .database_name = "tenant_ops",
        .namespace_name = "billing",
    };
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{ tenant_table, default_table })[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    try std.testing.expect(deriveTableId("docs") != tenant_table.table_id);
    try std.testing.expectEqual(default_table.table_id, findTableByName(&snapshot, "docs").?.table_id);
    try std.testing.expectEqual(
        tenant_table.table_id,
        findTableByQualifiedName(&snapshot, "tenant_ops", "billing", "docs").?.table_id,
    );
    try std.testing.expect(findTableByQualifiedName(&snapshot, "tenant_ops", default_namespace_name, "docs") == null);
}

test "table catalog identity applies SQL database and namespace catalog lifecycle" {
    const CatalogService = struct {
        manager: metadata_table_manager.TableManager,

        fn init(alloc: std.mem.Allocator) @This() {
            return .{ .manager = metadata_table_manager.TableManager.init(alloc) };
        }

        fn deinit(self: *@This()) void {
            self.manager.deinit();
        }

        fn snapshot(self: *@This(), alloc: std.mem.Allocator) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .databases = try self.manager.listDatabases(alloc),
                .namespaces = try self.manager.listNamespaces(alloc),
                .tablespaces = try self.manager.listTablespaces(alloc),
                .tables = try self.manager.listTables(alloc),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeSnapshot(self: *@This(), alloc: std.mem.Allocator, snapshot_value: *metadata_api.AdminSnapshot) void {
            self.manager.freeDatabases(alloc, snapshot_value.databases);
            self.manager.freeNamespaces(alloc, snapshot_value.namespaces);
            self.manager.freeTablespaces(alloc, snapshot_value.tablespaces);
            self.manager.freeTables(alloc, snapshot_value.tables);
            snapshot_value.* = undefined;
        }

        pub fn upsertDatabase(self: *@This(), record: metadata_table_manager.DatabaseRecord) !void {
            try self.manager.upsertDatabase(record);
        }

        pub fn removeDatabase(self: *@This(), database_id: u64) !void {
            _ = try self.manager.removeDatabase(database_id);
        }

        pub fn upsertNamespace(self: *@This(), record: metadata_table_manager.NamespaceRecord) !void {
            try self.manager.upsertNamespace(record);
        }

        pub fn removeNamespace(self: *@This(), namespace_id: u64) !void {
            _ = try self.manager.removeNamespace(namespace_id);
        }

        pub fn upsertTable(self: *@This(), record: metadata_table_manager.TableRecord) !void {
            try self.manager.upsertTable(record);
        }

        pub fn upsertTablespace(self: *@This(), record: metadata_table_manager.TablespaceRecord) !void {
            try self.manager.upsertTablespace(record);
        }

        pub fn removeTablespace(self: *@This(), tablespace_id: u64) !void {
            _ = try self.manager.removeTablespace(tablespace_id);
        }

        fn apply(self: *@This(), alloc: std.mem.Allocator, sql: []const u8) !AppliedRelationalSqlDdlRecord {
            return try self.applyWithSession(alloc, sql, .{});
        }

        fn applyWithSession(self: *@This(), alloc: std.mem.Allocator, sql: []const u8, session: catalog_resources.SqlCatalogSession) !AppliedRelationalSqlDdlRecord {
            var snapshot_value = try self.snapshot(alloc);
            defer self.freeSnapshot(alloc, &snapshot_value);
            return (try applyRelationalCatalogDdlOnServiceWithSessionAlloc(alloc, self, &snapshot_value, sql, session)) orelse error.UnsupportedSqlShape;
        }
    };

    var service = CatalogService.init(std.testing.allocator);
    defer service.deinit();

    var created_database = try service.apply(std.testing.allocator, "CREATE DATABASE tenant_ops;");
    defer created_database.deinit(std.testing.allocator);
    try std.testing.expect(created_database.created_database);

    const tenant_database_id = metadata_table_manager.deriveDatabaseId("tenant_ops");
    {
        const databases = try service.manager.listDatabases(std.testing.allocator);
        defer service.manager.freeDatabases(std.testing.allocator, databases);
        try std.testing.expectEqual(@as(usize, 1), databases.len);
        try std.testing.expectEqual(tenant_database_id, databases[0].database_id);
        try std.testing.expectEqualStrings("tenant_ops", databases[0].name);
    }

    var altered_database = try service.apply(std.testing.allocator, "ALTER DATABASE tenant_ops SET app.tenant_id TO 'tenant-a';");
    defer altered_database.deinit(std.testing.allocator);
    {
        const databases = try service.manager.listDatabases(std.testing.allocator);
        defer service.manager.freeDatabases(std.testing.allocator, databases);
        var parsed_settings = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, databases[0].settings_json, .{});
        defer parsed_settings.deinit();
        try std.testing.expectEqualStrings("tenant-a", parsed_settings.value.object.get("app.tenant_id").?.string);
    }
    var altered_runtime_database = try service.apply(std.testing.allocator, "ALTER DATABASE tenant_ops SET statement_timeout TO '5s';");
    defer altered_runtime_database.deinit(std.testing.allocator);
    {
        const databases = try service.manager.listDatabases(std.testing.allocator);
        defer service.manager.freeDatabases(std.testing.allocator, databases);
        var parsed_settings = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, databases[0].settings_json, .{});
        defer parsed_settings.deinit();
        try std.testing.expectEqualStrings("5s", parsed_settings.value.object.get("statement_timeout").?.string);
    }
    try std.testing.expectError(error.UnsupportedRoleSetting, service.apply(std.testing.allocator, "ALTER DATABASE tenant_ops SET unknown_guc TO 'on';"));
    try std.testing.expectError(error.InvalidRoleSetting, service.apply(std.testing.allocator, "ALTER DATABASE tenant_ops SET statement_timeout TO 'five seconds';"));

    var created_tablespace = try service.apply(std.testing.allocator, "CREATE TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace';");
    defer created_tablespace.deinit(std.testing.allocator);
    try std.testing.expect(created_tablespace.created_tablespace);
    try service.manager.upsertTable(.{
        .table_id = deriveQualifiedTableId(default_database_name, default_namespace_name, "fast_docs"),
        .name = "fast_docs",
        .tablespace_name = "fastspace",
    });
    try std.testing.expectError(error.TablespaceInUse, service.apply(std.testing.allocator, "DROP TABLESPACE fastspace;"));
    var renamed_tablespace = try service.apply(std.testing.allocator, "ALTER TABLESPACE fastspace RENAME TO archive_space;");
    defer renamed_tablespace.deinit(std.testing.allocator);
    try std.testing.expect(renamed_tablespace.renamed_tablespace);
    {
        const table = service.manager.tables.get(deriveQualifiedTableId(default_database_name, default_namespace_name, "fast_docs")).?;
        try std.testing.expectEqualStrings("archive_space", table.tablespace_name);
    }
    try std.testing.expectError(error.TablespaceInUse, service.apply(std.testing.allocator, "DROP TABLESPACE archive_space;"));
    _ = service.manager.removeTable(deriveQualifiedTableId(default_database_name, default_namespace_name, "fast_docs"));
    var dropped_tablespace = try service.apply(std.testing.allocator, "DROP TABLESPACE archive_space;");
    defer dropped_tablespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_tablespace.dropped_tablespace);

    var created_namespace = try service.apply(std.testing.allocator, "CREATE SCHEMA analytics;");
    defer created_namespace.deinit(std.testing.allocator);
    try std.testing.expect(created_namespace.created_namespace);
    var tenant_namespace = try service.applyWithSession(std.testing.allocator, "CREATE SCHEMA private;", .{ .current_database_name = "tenant_ops" });
    defer tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(tenant_namespace.created_namespace);
    {
        var snapshot_value = try service.snapshot(std.testing.allocator);
        defer service.freeSnapshot(std.testing.allocator, &snapshot_value);
        try std.testing.expect(findNamespaceByName(&snapshot_value, "tenant_ops", "private") != null);
    }
    {
        var snapshot_value = try service.snapshot(std.testing.allocator);
        defer service.freeSnapshot(std.testing.allocator, &snapshot_value);

        var analytics_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "CREATE TABLE analytics.events (id uuid PRIMARY KEY);");
        defer analytics_target.deinit(std.testing.allocator);
        try validateRelationalSqlDdlNamespace(&snapshot_value, analytics_target);

        var missing_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "CREATE TABLE missing.events (id uuid PRIMARY KEY);");
        defer missing_target.deinit(std.testing.allocator);
        try std.testing.expectError(error.NamespaceNotFound, validateRelationalSqlDdlNamespace(&snapshot_value, missing_target));
    }
    try service.manager.upsertTable(.{
        .table_id = deriveQualifiedTableId(default_database_name, "analytics", "events"),
        .name = "events",
        .database_name = default_database_name,
        .namespace_name = "analytics",
    });

    try std.testing.expectError(error.NamespaceNotEmpty, service.apply(std.testing.allocator, "DROP SCHEMA analytics;"));
    try std.testing.expectError(error.UnsupportedSqlShape, service.apply(std.testing.allocator, "DROP SCHEMA analytics CASCADE;"));

    var renamed_namespace = try service.apply(std.testing.allocator, "ALTER SCHEMA analytics RENAME TO reporting;");
    defer renamed_namespace.deinit(std.testing.allocator);
    try std.testing.expect(renamed_namespace.renamed_namespace);
    const table = service.manager.tables.get(deriveQualifiedTableId(default_database_name, "analytics", "events")).?;
    try std.testing.expectEqualStrings("reporting", table.namespace_name);

    _ = service.manager.removeTable(table.table_id);
    var dropped_namespace = try service.apply(std.testing.allocator, "DROP SCHEMA reporting;");
    defer dropped_namespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_namespace.dropped_namespace);

    var dropped_tenant_namespace = try service.applyWithSession(std.testing.allocator, "DROP SCHEMA private;", .{ .current_database_name = "tenant_ops" });
    defer dropped_tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_tenant_namespace.dropped_namespace);

    var dropped_database = try service.apply(std.testing.allocator, "DROP DATABASE tenant_ops;");
    defer dropped_database.deinit(std.testing.allocator);
    try std.testing.expect(dropped_database.dropped_database);
}

test "create table parser rejects zero shards" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"num_shards\":0}"),
    );
}

test "create table parser rejects malformed replication sources" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"replication_sources\":{\"type\":\"postgres\"}}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":123,\"postgres_table\":\"users\"}]}"),
    );
}

test "schema update parser preserves object payload" {
    const parsed = try parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}}}");
    defer std.testing.allocator.free(parsed);
    try std.testing.expectEqualStrings("{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}}}", parsed);
}

test "schema update parser rejects malformed document schema payloads" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{}}}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{\"schema\":true}}}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"string\"}}}}"),
    );
}

test "schema update parser rejects invalid top-level schema fields" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"version\":-1}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"ttl_duration_ns\":-1}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"dynamic_templates\":true}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"dynamic_templates\":{\"body\":{\"mapping\":true}}}"),
    );
}

test "validated table schema parses default type and dynamic templates" {
    var parsed = try parseValidatedTableSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"published\":{\"type\":\"boolean\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("doc", parsed.default_type);
    try std.testing.expect(parsed.enforce_types);
    try std.testing.expectEqual(@as(usize, 1), parsed.document_schemas.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.dynamic_templates.len);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseValidatedTableSchema(std.testing.allocator, "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"},\"customer_email\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\",\"customer_email\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\",\"email\"]}}]}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseValidatedTableSchema(std.testing.allocator, "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"match\":\"partial\"}]}"),
    );
}

test "relational schema implies enforced closed validation" {
    var parsed = try parseValidatedTableSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"}},\"required\":[\"id\"]}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expect(parsed.enforce_types);
    try validateBatchWritesAgainstTableSchema(
        std.testing.allocator,
        parsed,
        &.{.{ .key = "row:ok", .value = "{\"id\":\"a\"}" }},
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateBatchWritesAgainstTableSchema(
            std.testing.allocator,
            parsed,
            &.{.{ .key = "row:bad", .value = "{\"id\":\"a\",\"extra\":1}" }},
        ),
    );
}

test "relational schema rejects explicit soft validation and open root objects" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"storage_mode\":\"relational\",\"enforce_types\":false,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"}}}}}}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"storage_mode\":\"relational\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"id\":{\"type\":\"keyword\"}}}}}}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"storage_mode\":\"relational\",\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"}}}}}}"),
    );
}

test "relational schema rejects empty column catalogs" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"storage_mode\":\"relational\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"embedding\":{\"type\":\"embedding\"}}}}}}"),
    );
}

test "validated table schema parses and rejects mistyped relational expression checks" {
    const valid_schema =
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"fee":{"type":"numeric"},"enabled":{"type":"boolean"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"amount_fee_nonnegative","expression":{"lhs":{"op":"add","args":[{"field":"amount"},{"field":"fee"}]},"op":"gte","rhs":{"value":0}}},{"name":"status_not_deleted","expression":{"lhs":{"op":"lower","args":[{"field":"status"}]},"op":"ne","rhs":{"value":"deleted"}}},{"name":"enabled_known","expression":{"lhs":{"field":"enabled"},"op":"is_not_null"}},{"name":"tag_count_nonnegative","expression":{"lhs":{"op":"array_length","args":[{"field":"tags"}]},"op":"gte","rhs":{"value":0}}},{"name":"metadata_source_present","expression":{"lhs":{"op":"json_extract","args":[{"field":"metadata"}],"path":"source","as_text":true},"op":"is_not_null"}}]}
    ;
    var parsed = try parseValidatedTableSchema(std.testing.allocator, valid_schema);
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 5), parsed.checks.len);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"amount_text_mismatch","expression":{"lhs":{"field":"amount"},"op":"eq","rhs":{"value":"open"}}}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"amount_atom_text_mismatch","field":"amount","op":"eq","value":"open"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"json_not_orderable","expression":{"lhs":{"field":"metadata"},"op":"gt","rhs":{"value":{"source":"api"}}}}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"boolean_operand_mismatch","expression":{"lhs":{"op":"and","args":[{"field":"enabled"},{"field":"status"}]},"op":"eq","rhs":{"value":true}}}]}
    ));
}

test "table schema write validation rejects unknown fields when enforce_types is enabled" {
    var parsed = try parseValidatedTableSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateBatchWritesAgainstTableSchema(std.testing.allocator, parsed, &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }}),
    );
}

test "table schema write validation uses explicit type and basic field type checks" {
    var parsed = try parseValidatedTableSchema(
        std.testing.allocator,
        "{\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}},\"event\":{\"schema\":{\"type\":\"object\",\"properties\":{\"starts_at\":{\"type\":\"datetime\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateBatchWritesAgainstTableSchema(std.testing.allocator, parsed, &.{.{ .key = "doc:a", .value = "{\"_type\":\"article\",\"title\":\"alpha\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateBatchWritesAgainstTableSchema(std.testing.allocator, parsed, &.{.{ .key = "doc:b", .value = "{\"title\":\"missing type\"}" }}),
    );
}

test "metadata.schema update preserves read schema and adds versioned full-text index" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "{\"version\":0,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"text\"}}}}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}",
        .replication_sources_json = "[\"seed\"]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\"}}}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"text\"}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.read_schema_json, "\"version\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.read_schema_json, "\"document_schemas\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v0\":{\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v1\":{\"name\":\"full_text_index_v1\",\"type\":\"full_text\"}") != null);
}

test "metadata.schema update auto-creates an algebraic index for relational tables" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 9,
        .name = "sales",
        .schema_json = "{\"version\":0,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\",\"amount\"],\"additionalProperties\":false}}}}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    // A relational table that predates the automatic aggregation sidecar gets a
    // schema-derived algebraic index without changing the relational base columns.
    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\",\"amount\"],\"additionalProperties\":false}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"algebraic_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"type\":\"algebraic\"") != null);
    // The derive_from_schema marker is expanded to a concrete config at write time.
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"capability_fingerprint\"") != null);
}

test "metadata.schema update sql ddl creates relational table record schema and derived indexes" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 12,
        .name = "users",
        .schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    var applied = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &table,
        \\CREATE TABLE users (
        \\  id uuid PRIMARY KEY,
        \\  tenant_id text NOT NULL,
        \\  amount numeric DEFAULT 0
        \\);
        ,
    );
    defer applied.deinit(std.testing.allocator);

    try std.testing.expect(!applied.requires_rebuild);
    try std.testing.expect(!applied.validation_required);
    try std.testing.expect(std.mem.indexOf(u8, applied.table.schema_json, "\"version\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, applied.table.schema_json, "\"storage_mode\":\"relational\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, applied.table.read_schema_json, "\"document_schemas\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, applied.table.indexes_json, "\"algebraic_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, applied.table.indexes_json, "\"derive_from_schema\"") == null);
}

test "metadata.schema update sql ddl applies Antfly derived indexes to table metadata" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 12,
        .name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    var created = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &table,
        \\CREATE TABLE docs (
        \\  id text PRIMARY KEY,
        \\  body text,
        \\  source_doc text,
        \\  target_doc text,
        \\  edge_type text,
        \\  confidence numeric
        \\);
        ,
    );
    defer created.deinit(std.testing.allocator);

    var full_text = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &created.table,
        "CREATE INDEX docs_body_fts ON docs USING antfly_full_text (body) WITH (analyzer = 'standard');",
    );
    defer full_text.deinit(std.testing.allocator);
    try std.testing.expect(full_text.requires_rebuild);
    try std.testing.expectEqual(@as(usize, 1), full_text.work_items.len);
    try std.testing.expect(std.mem.indexOf(u8, full_text.table.indexes_json, "\"docs_body_fts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, full_text.table.indexes_json, "\"type\":\"full_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, full_text.table.schema_json, "\"docs_body_fts\"") == null);

    var semantic = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &full_text.table,
        "CREATE INDEX docs_body_semantic ON docs USING antfly_aknn (body) WITH (embedding_name = 'body_embedding_v1', model = 'local-model', dimension = 384);",
    );
    defer semantic.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, semantic.table.indexes_json, "\"docs_body_semantic\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, semantic.table.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, semantic.table.indexes_json, "\"body_embedding_v1\"") != null);

    var graph = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &semantic.table,
        "CREATE GRAPH INDEX docs_graph ON docs EDGE (source_doc -> target_doc) TYPE edge_type WEIGHT confidence WITH (edge_policy = 'all');",
    );
    defer graph.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, graph.table.indexes_json, "\"docs_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, graph.table.indexes_json, "\"type\":\"graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, graph.table.indexes_json, "\"edge_policy\":\"all\"") != null);

    var extraction_graph = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &graph.table,
        "CREATE GRAPH INDEX docs_rel_graph ON docs SOURCE ENRICHMENT relations_v1 FROM body USING extractor MODEL 'relations' EDGES JSON_PATH '$.relations[*]' SOURCE _id TARGET target.document_id TYPE type WEIGHT confidence WITH (edge_policy = 'all');",
    );
    defer extraction_graph.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, extraction_graph.table.indexes_json, "\"docs_rel_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extraction_graph.table.indexes_json, "\"kind\":\"artifact\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extraction_graph.table.indexes_json, "\"artifact\":\"relations_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extraction_graph.table.indexes_json, "\"field\":\"body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extraction_graph.table.indexes_json, "\"target\":\"{{ _item.target.document_id }}\"") != null);

    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &extraction_graph.table,
            "CREATE GRAPH INDEX docs_bad_rel_graph ON docs SOURCE ENRICHMENT bad_relations FROM missing_body USING extractor MODEL 'relations' EDGES JSON_PATH '$.relations[*]' SOURCE _id TARGET target.document_id;",
        ),
    );

    var graph_metric = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &extraction_graph.table,
        "CREATE INDEX docs_graph_pagerank ON docs USING antfly_graph_metric () WITH (graph_index = 'docs_graph', metric = 'pagerank');",
    );
    defer graph_metric.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, graph_metric.table.indexes_json, "\"docs_graph_pagerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, graph_metric.table.indexes_json, "\"type\":\"graph_metric\"") != null);

    var graph_metric_alter = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &graph_metric.table,
        "ALTER GRAPH INDEX docs_graph ADD METRIC pagerank_v1 USING pagerank WITH (damping = 0.85, max_iterations = 40);",
    );
    defer graph_metric_alter.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, graph_metric_alter.table.indexes_json, "\"pagerank_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, graph_metric_alter.table.indexes_json, "\"graph_index\":\"docs_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, graph_metric_alter.table.indexes_json, "\"metric\":\"pagerank_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, graph_metric_alter.table.indexes_json, "\"algorithm\":\"pagerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, graph_metric_alter.table.indexes_json, "\"max_iterations\":40") != null);

    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &graph_metric_alter.table,
            "ALTER GRAPH INDEX missing_graph ADD METRIC missing_pagerank USING pagerank;",
        ),
    );
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &graph_metric_alter.table,
            "ALTER GRAPH INDEX docs_graph ADD METRIC bad_metric USING shortest_path;",
        ),
    );

    var hybrid = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &graph_metric_alter.table,
        "CREATE INDEX docs_hybrid ON docs USING antfly_hybrid () WITH (sources = 'docs_body_fts,docs_body_semantic,docs_graph_pagerank', fusion = 'rrf');",
    );
    defer hybrid.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, hybrid.table.indexes_json, "\"docs_hybrid\"") != null);
    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &graph_metric_alter.table,
            "CREATE INDEX docs_bad_hybrid ON docs USING antfly_hybrid () WITH (sources = 'docs_graph', fusion = 'rrf');",
        ),
    );
    try std.testing.expect(std.mem.indexOf(u8, hybrid.table.indexes_json, "\"sources\"") != null);

    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &hybrid.table,
            "CREATE INDEX docs_bad_hybrid ON docs USING antfly_hybrid () WITH (sources = 'docs_body_fts,missing_source', fusion = 'rrf');",
        ),
    );

    var duplicate_noop = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &hybrid.table,
        "CREATE INDEX IF NOT EXISTS docs_body_fts ON docs USING antfly_full_text (body);",
    );
    defer duplicate_noop.deinit(std.testing.allocator);
    try std.testing.expect(duplicate_noop.noop);
    try std.testing.expectEqualStrings(hybrid.table.indexes_json, duplicate_noop.table.indexes_json);

    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &hybrid.table,
            "CREATE INDEX docs_missing_fts ON docs USING antfly_full_text (missing_field);",
        ),
    );

    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &hybrid.table,
            "DROP INDEX docs_graph;",
        ),
    );

    var dropped = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &hybrid.table,
        "DROP INDEX docs_hybrid;",
    );
    defer dropped.deinit(std.testing.allocator);
    try std.testing.expect(dropped.requires_rebuild);
    try std.testing.expect(std.mem.indexOf(u8, dropped.table.indexes_json, "\"docs_hybrid\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, dropped.table.indexes_json, "\"docs_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, dropped.table.indexes_json, "\"docs_body_semantic\"") != null);
}

test "metadata.schema update sql ddl exposes catalog target and create intent" {
    var create_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "CREATE TABLE users (id uuid PRIMARY KEY);");
    defer create_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(default_database_name, create_target.database_name);
    try std.testing.expectEqualStrings(default_namespace_name, create_target.namespace_name);
    try std.testing.expectEqualStrings("users", create_target.table_name);
    try std.testing.expect(create_target.createsTable());

    var schema_create_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "CREATE TABLE analytics.users (id uuid PRIMARY KEY);");
    defer schema_create_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(default_database_name, schema_create_target.database_name);
    try std.testing.expectEqualStrings("analytics", schema_create_target.namespace_name);
    try std.testing.expectEqualStrings("users", schema_create_target.table_name);
    try std.testing.expect(schema_create_target.createsTable());

    var alter_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "ALTER TABLE users ADD COLUMN status text;");
    defer alter_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(default_database_name, alter_target.database_name);
    try std.testing.expectEqualStrings(default_namespace_name, alter_target.namespace_name);
    try std.testing.expectEqualStrings("users", alter_target.table_name);
    try std.testing.expect(!alter_target.createsTable());

    var schema_alter_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "ALTER TABLE analytics.users ADD COLUMN status text;");
    defer schema_alter_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(default_database_name, schema_alter_target.database_name);
    try std.testing.expectEqualStrings("analytics", schema_alter_target.namespace_name);
    try std.testing.expectEqualStrings("users", schema_alter_target.table_name);
    try std.testing.expect(!schema_alter_target.createsTable());

    var drop_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "DROP TABLE users;");
    defer drop_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("users", drop_target.table_name);
    try std.testing.expect(drop_target.dropsTable());
    try std.testing.expect(!drop_target.if_exists);
    try std.testing.expect(!drop_target.cascade);

    var drop_if_exists_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "DROP TABLE IF EXISTS users;");
    defer drop_if_exists_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("users", drop_if_exists_target.table_name);
    try std.testing.expect(drop_if_exists_target.dropsTable());
    try std.testing.expect(drop_if_exists_target.if_exists);
    try std.testing.expect(!drop_if_exists_target.cascade);

    var drop_cascade_target = try relationalSqlDdlTargetAlloc(std.testing.allocator, "DROP TABLE users CASCADE;");
    defer drop_cascade_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("users", drop_cascade_target.table_name);
    try std.testing.expect(drop_cascade_target.dropsTable());
    try std.testing.expect(!drop_cascade_target.if_exists);
    try std.testing.expect(drop_cascade_target.cascade);

    try std.testing.expectError(error.UnsupportedSqlShape, relationalSqlDdlTargetAlloc(std.testing.allocator, "CREATE TABLE tenant_ops.analytics.users (id uuid PRIMARY KEY);"));
}

test "metadata.schema update sql ddl applies relational catalog changes through table record path" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 13,
        .name = "users",
        .schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    var created = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &table,
        "CREATE TABLE users (id uuid PRIMARY KEY, tenant_id text NOT NULL, email text, updated_at_ns bigint);",
    );
    defer created.deinit(std.testing.allocator);

    const qualified_table: metadata_table_manager.TableRecord = .{
        .table_id = deriveQualifiedTableId(default_database_name, "analytics", "users"),
        .name = "users",
        .database_name = default_database_name,
        .namespace_name = "analytics",
        .schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };
    var qualified_created = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &qualified_table,
        "CREATE TABLE analytics.users (id uuid PRIMARY KEY);",
    );
    defer qualified_created.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("users", qualified_created.table.name);
    try std.testing.expectEqualStrings("analytics", qualified_created.table.namespace_name);
    try std.testing.expect(std.mem.indexOf(u8, qualified_created.table.schema_json, "\"analytics.users_pkey\"") == null);

    var qualified_dropped_pk = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &qualified_created.table,
        "ALTER TABLE analytics.users DROP CONSTRAINT users_pkey;",
    );
    defer qualified_dropped_pk.deinit(std.testing.allocator);
    try std.testing.expect(qualified_dropped_pk.requires_rebuild);
    try std.testing.expect(qualified_dropped_pk.rewrite_required);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applyRelationalSqlDdlToTableRecordAlloc(std.testing.allocator, &qualified_table, "ALTER TABLE public.users ADD COLUMN status text;"),
    );

    var altered = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &created.table,
        "ALTER TABLE users ADD COLUMN status text;",
    );
    defer altered.deinit(std.testing.allocator);

    try std.testing.expect(altered.requires_rebuild);
    try std.testing.expect(!altered.validation_required);
    try std.testing.expect(!altered.rewrite_required);
    try std.testing.expectEqual(@as(usize, 1), altered.work_items.len);
    try std.testing.expectEqual(sql_adapter.AppliedDdlWorkAction.rebuild, altered.work_items[0].action);
    try std.testing.expect(std.mem.indexOf(u8, altered.table.schema_json, "\"version\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, altered.table.schema_json, "\"status\":{\"type\":\"keyword\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, altered.table.read_schema_json, "\"version\":0") != null);

    var generated = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &altered.table,
        "ALTER TABLE users ADD COLUMN tenant_status_key text GENERATED ALWAYS AS (concat(tenant_id, ':', status)) STORED;",
    );
    defer generated.deinit(std.testing.allocator);

    try std.testing.expect(generated.requires_rebuild);
    try std.testing.expect(!generated.validation_required);
    try std.testing.expect(generated.rewrite_required);
    try std.testing.expectEqual(@as(usize, 2), generated.work_items.len);
    try std.testing.expectEqual(sql_adapter.AppliedDdlWorkAction.rebuild, generated.work_items[0].action);
    try std.testing.expectEqual(sql_adapter.AppliedDdlWorkAction.rewrite, generated.work_items[1].action);
    const concat_rewrite = generated.work_items[1].rewrite_expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("tenant_status_key", concat_rewrite.target_column);
    try std.testing.expectEqual(runtime_schema_mod.RelationalRowsExpressionKind.concat, concat_rewrite.expression.kind);
    try std.testing.expectEqual(@as(usize, 3), concat_rewrite.expression.operands.len);
    try std.testing.expectEqualStrings("tenant_id", concat_rewrite.expression.operands[0].field);
    try std.testing.expectEqualStrings("\":\"", concat_rewrite.expression.operands[1].value_json);
    try std.testing.expectEqualStrings("status", concat_rewrite.expression.operands[2].field);

    var default_backfill = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &altered.table,
        "ALTER TABLE users ADD COLUMN status_default text DEFAULT 'pending';",
    );
    defer default_backfill.deinit(std.testing.allocator);
    try std.testing.expect(default_backfill.rewrite_required);
    try std.testing.expectEqual(@as(usize, 2), default_backfill.work_items.len);
    const default_rewrite = default_backfill.work_items[1].rewrite_expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("status_default", default_rewrite.target_column);
    try std.testing.expectEqual(runtime_schema_mod.RelationalRowsExpressionKind.value, default_rewrite.expression.kind);
    try std.testing.expectEqualStrings("\"pending\"", default_rewrite.expression.value_json);

    var generated_backfill = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &altered.table,
        "ALTER TABLE users ADD COLUMN status_lower text GENERATED ALWAYS AS (lower(status)) STORED;",
    );
    defer generated_backfill.deinit(std.testing.allocator);
    try std.testing.expect(generated_backfill.rewrite_required);
    try std.testing.expectEqual(@as(usize, 2), generated_backfill.work_items.len);
    const generated_rewrite = generated_backfill.work_items[1].rewrite_expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("status_lower", generated_rewrite.target_column);
    try std.testing.expectEqual(runtime_schema_mod.RelationalRowsExpressionKind.lower, generated_rewrite.expression.kind);
    try std.testing.expectEqual(@as(usize, 1), generated_rewrite.expression.operands.len);
    try std.testing.expectEqualStrings("status", generated_rewrite.expression.operands[0].field);

    var generated_concat_ws_backfill = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &altered.table,
        "ALTER TABLE users ADD COLUMN tenant_status_joined text GENERATED ALWAYS AS (concat_ws(':', tenant_id, status)) STORED;",
    );
    defer generated_concat_ws_backfill.deinit(std.testing.allocator);
    try std.testing.expect(generated_concat_ws_backfill.rewrite_required);
    try std.testing.expectEqual(@as(usize, 2), generated_concat_ws_backfill.work_items.len);
    const concat_ws_rewrite = generated_concat_ws_backfill.work_items[1].rewrite_expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("tenant_status_joined", concat_ws_rewrite.target_column);
    try std.testing.expectEqual(runtime_schema_mod.RelationalRowsExpressionKind.concat_ws, concat_ws_rewrite.expression.kind);
    try std.testing.expectEqual(@as(usize, 3), concat_ws_rewrite.expression.operands.len);
    try std.testing.expectEqualStrings("\":\"", concat_ws_rewrite.expression.operands[0].value_json);
    try std.testing.expectEqualStrings("tenant_id", concat_ws_rewrite.expression.operands[1].field);
    try std.testing.expectEqualStrings("status", concat_ws_rewrite.expression.operands[2].field);

    var indexed = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &generated.table,
        "CREATE UNIQUE INDEX users_tenant_email_key ON users (tenant_id, email);",
    );
    defer indexed.deinit(std.testing.allocator);

    try std.testing.expect(indexed.requires_rebuild);
    try std.testing.expect(indexed.validation_required);
    try std.testing.expect(!indexed.rewrite_required);
    try std.testing.expectEqual(@as(usize, 2), indexed.work_items.len);
    try std.testing.expectEqual(sql_adapter.AppliedDdlWorkAction.rebuild, indexed.work_items[0].action);
    try std.testing.expectEqual(sql_adapter.AppliedDdlWorkAction.validate, indexed.work_items[1].action);
    try std.testing.expect(std.mem.indexOf(u8, indexed.table.schema_json, "\"users_tenant_email_key\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, indexed.table.schema_json, "\"version\":2") != null);

    var trigger = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &indexed.table,
        "CREATE TRIGGER users_updated_at BEFORE UPDATE ON users EXECUTE FUNCTION touch_updated_at('updated_at_ns');",
    );
    defer trigger.deinit(std.testing.allocator);

    try std.testing.expect(!trigger.requires_rebuild);
    try std.testing.expect(!trigger.validation_required);
    try std.testing.expect(std.mem.indexOf(u8, trigger.table.schema_json, "\"x-antfly-on-update\":{\"op\":\"now_ns\"}") != null);
}

test "metadata.schema update rejects relational storage mode and base column changes" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 9,
        .name = "sales",
        .schema_json = "{\"version\":0,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}}}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(
            std.testing.allocator,
            &table,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}}}",
        ),
    );

    const relational_table: metadata_table_manager.TableRecord = .{
        .table_id = 10,
        .name = "sales",
        .schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\"],\"additionalProperties\":false}}}}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(
            std.testing.allocator,
            &relational_table,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"},\"region\":{\"type\":\"keyword\"}},\"required\":[\"tenant\"],\"additionalProperties\":false}}}}",
        ),
    );

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(
            std.testing.allocator,
            &relational_table,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"keyword\"}},\"required\":[\"tenant\"],\"additionalProperties\":false}}}}",
        ),
    );

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(
            std.testing.allocator,
            &relational_table,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"tenant\"]}}",
        ),
    );

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(
            std.testing.allocator,
            &relational_table,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"system_versioned\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\"],\"additionalProperties\":false}}}}",
        ),
    );

    const relational_fk_table: metadata_table_manager.TableRecord = .{
        .table_id = 11,
        .name = "orders",
        .schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"restrict\"}]}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const dropped_fk = try applySchemaUpdateRecord(
        std.testing.allocator,
        &relational_fk_table,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"id\"],\"additionalProperties\":false}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, dropped_fk);
    try std.testing.expect(std.mem.indexOf(u8, dropped_fk.schema_json, "\"foreign_keys\"") == null);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(
            std.testing.allocator,
            &relational_fk_table,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"id\",\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"cascade\"}]}",
        ),
    );
}

test "metadata.schema update rejects relational constraint definition drift" {
    const alloc = std.testing.allocator;
    const indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}";
    const replication_sources_json = "[]";

    const pk_table: metadata_table_manager.TableRecord = .{
        .table_id = 12,
        .name = "orders",
        .schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"name":"orders_pkey","columns":["id"],"include_columns":["status"]}}
        ,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = "data",
    };

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &pk_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"name":"orders_id_pkey","columns":["id"],"include_columns":["status"]}}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &pk_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"name":"orders_pkey","columns":["id"],"include_columns":["email"]}}
    ));

    const unique_table: metadata_table_manager.TableRecord = .{
        .table_id = 13,
        .name = "users",
        .schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_lower_key","expressions":[{"op":"lower","field":"email"}],"validation_state":"unvalidated"},{"name":"users_active_email_key","columns":["email"],"where":{"all":[{"field":"status","op":"eq","value":"active"}]}},{"name":"users_status_email_key","columns":["email"],"where_expressions":[{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"active"}}]}]}
        ,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = "data",
    };

    const promoted_unique = try applySchemaUpdateRecord(alloc, &unique_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_lower_key","expressions":[{"op":"lower","field":"email"}],"validation_state":"enforced"},{"name":"users_active_email_key","columns":["email"],"where":{"all":[{"field":"status","op":"eq","value":"active"}]}},{"name":"users_status_email_key","columns":["email"],"where_expressions":[{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"active"}}]}]}
    );
    defer metadata_table_manager.freeTable(alloc, promoted_unique);
    try std.testing.expect(std.mem.indexOf(u8, promoted_unique.schema_json, "\"validation_state\":\"enforced\"") != null);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &unique_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_lower_key","expressions":[{"op":"upper","field":"email"}],"validation_state":"unvalidated"},{"name":"users_active_email_key","columns":["email"],"where":{"all":[{"field":"status","op":"eq","value":"active"}]}},{"name":"users_status_email_key","columns":["email"],"where_expressions":[{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"active"}}]}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &unique_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_lower_key","expressions":[{"op":"lower","field":"email"}],"validation_state":"unvalidated"},{"name":"users_active_email_key","columns":["email"],"where":{"all":[{"field":"status","op":"eq","value":"pending"}]}},{"name":"users_status_email_key","columns":["email"],"where_expressions":[{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"active"}}]}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &unique_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_lower_key","expressions":[{"op":"lower","field":"email"}],"validation_state":"unvalidated"},{"name":"users_active_email_key","columns":["email"],"where":{"all":[{"field":"status","op":"eq","value":"active"}]}},{"name":"users_status_email_key","columns":["email"],"where_expressions":[{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"pending"}}]}]}
    ));

    const period_table: metadata_table_manager.TableRecord = .{
        .table_id = 14,
        .name = "prices",
        .schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"},"other_from":{"type":"datetime"},"other_to":{"type":"datetime"}},"required":["tenant_id","sku","valid_from","valid_to","other_from","other_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"},{"name":"other_time","start_column":"other_from","end_column":"other_to"}],"primary_key":{"columns":["tenant_id","sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"prices_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"},"on_delete":"restrict"}]}
        ,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = "data",
    };

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &period_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"},"other_from":{"type":"datetime"},"other_to":{"type":"datetime"}},"required":["tenant_id","sku","valid_from","valid_to","other_from","other_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"},{"name":"other_time","start_column":"other_from","end_column":"other_to"}],"primary_key":{"columns":["tenant_id","sku"],"without_overlaps_period":"other_time"},"foreign_keys":[{"name":"prices_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"},"on_delete":"restrict"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &period_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"},"other_from":{"type":"datetime"},"other_to":{"type":"datetime"}},"required":["tenant_id","sku","valid_from","valid_to","other_from","other_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"},{"name":"other_time","start_column":"other_from","end_column":"other_to"}],"primary_key":{"columns":["tenant_id","sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"prices_parent_time_fkey","columns":["tenant_id","sku"],"period":"other_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"},"on_delete":"restrict"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applySchemaUpdateRecord(alloc, &period_table,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"},"other_from":{"type":"datetime"},"other_to":{"type":"datetime"}},"required":["tenant_id","sku","valid_from","valid_to","other_from","other_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"},{"name":"other_time","start_column":"other_from","end_column":"other_to"}],"primary_key":{"columns":["tenant_id","sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"prices_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"other_time"},"on_delete":"restrict"}]}
    ));
}

test "metadata.schema update cascade drop cleanup removes child foreign keys by parent table" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"account_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["id"]},"on_delete":"restrict","validation_state":"enforced"},{"name":"orders_account_id_fkey","columns":["account_id"],"references":{"table":"accounts","columns":["id"]},"on_delete":"restrict","validation_state":"enforced"}]}
    ;

    const updated = (try schemaWithoutForeignKeysReferencingTableAlloc(alloc, schema_json, "customers")) orelse return error.TestUnexpectedResult;
    defer alloc.free(updated);
    try std.testing.expect(std.mem.indexOf(u8, updated, "orders_customer_id_fkey") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "orders_account_id_fkey") != null);

    const unchanged = try schemaWithoutForeignKeysReferencingTableAlloc(alloc, updated, "customers");
    try std.testing.expect(unchanged == null);
}

test "metadata.schema update rewrites foreign key validation state and preserves definition" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","validation_state":"unvalidated"}]}
    ;

    const updated = try schemaWithForeignKeyValidationStateAlloc(
        alloc,
        schema_json,
        "orders_customer_id_fkey",
        .enforced,
    );
    defer alloc.free(updated);

    var parsed = try parseValidatedTableSchema(alloc, updated);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);

    try std.testing.expectEqual(@as(usize, 1), runtime.foreign_keys.len);
    try std.testing.expectEqual(runtime_schema_mod.ForeignKeyValidationState.enforced, runtime.foreign_keys[0].validation_state);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", runtime.foreign_keys[0].name);
    try std.testing.expectEqualStrings("customers", runtime.foreign_keys[0].parent_table);
    try std.testing.expectEqual(runtime_schema_mod.ForeignKeyAction.restrict, runtime.foreign_keys[0].on_delete);

    try std.testing.expectError(
        error.ForeignKeyNotFound,
        schemaWithForeignKeyValidationStateAlloc(alloc, schema_json, "missing_fkey", .enforced),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        schemaWithForeignKeyValidationStateAlloc(alloc, schema_json, "orders_customer_id_fkey", .validating),
    );
}

test "metadata.schema update promotes secondary index only for matching building generation" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"building","x-antfly-index-generation":9},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    const updated = try schemaWithSecondaryIndexReadyAlloc(alloc, schema_json, "amount", 9);
    defer alloc.free(updated);
    var parsed = try parseValidatedTableSchema(alloc, updated);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    var found_amount = false;
    for (runtime.relational_columns) |column| {
        if (!std.mem.eql(u8, column.name, "amount")) continue;
        found_amount = true;
        try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.ready, column.index_lifecycle);
        try std.testing.expectEqual(@as(u64, 9), column.index_generation);
    }
    try std.testing.expect(found_amount);

    try std.testing.expectError(
        error.SecondaryIndexGenerationMismatch,
        schemaWithSecondaryIndexReadyAlloc(alloc, schema_json, "amount", 10),
    );
    try std.testing.expectError(
        error.SecondaryIndexNotFound,
        schemaWithSecondaryIndexReadyAlloc(alloc, schema_json, "missing", 9),
    );
    try std.testing.expectError(
        error.SecondaryIndexNotBuilding,
        schemaWithSecondaryIndexReadyAlloc(alloc, updated, "amount", 9),
    );
}

test "create table preparation auto-creates an algebraic index for relational schemas" {
    const alloc = std.testing.allocator;
    const schema_json = try normalizeSchemaVersion(
        alloc,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\",\"amount\"]}}}}",
        0,
    );
    defer alloc.free(schema_json);

    const indexes_json = try prepareTableIndexesForSchemaAlloc(alloc, "sales", default_indexes_json, schema_json);
    defer alloc.free(indexes_json);

    try std.testing.expect(std.mem.indexOf(u8, schema_json, "\"enforce_types\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"algebraic_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"group_fields\"") != null);
}

test "metadata.schema update does not add an algebraic index for document tables" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 9,
        .name = "docs",
        .schema_json = "{\"version\":0,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}}}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\"}}}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"type\":\"algebraic\"") == null);
}

test "metadata.schema update does not duplicate an existing algebraic index" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 9,
        .name = "sales",
        .schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\",\"amount\"],\"additionalProperties\":false}}}}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"my_alg\":{\"type\":\"algebraic\",\"derive_from_schema\":true}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    // Re-applying a relational schema that already has an algebraic index must
    // not add a second one (idempotent).
    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"numeric\"}},\"required\":[\"tenant\",\"amount\"],\"additionalProperties\":false}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"algebraic_index_v0\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"my_alg\"") != null);
}

test "metadata.schema update keeps version for template-only changes" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":2,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"text\"}}}}",
        .read_schema_json = "{\"version\":1,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\"}}}}}}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"keyword\"}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":2") != null);
    try std.testing.expectEqualStrings(table.read_schema_json, updated.read_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v2\":{\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v3\"") == null);
}

test "metadata.schema update refreshes algebraic dynamic templates without recreate" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 8,
        .name = "docs",
        .schema_json =
        \\{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}},"dynamic_templates":[{"name":"ext","match":"ext_*","mapping":{"type":"keyword"}}]}
        ,
        .indexes_json =
        \\{"full_text_index_v2":{"type":"full_text"},"alg":{"type":"algebraic","schema_version":2,"capability_fingerprint":"stale","group_fields":[],"dynamic_field_rules":[{"name":"ext","match":"ext_*","type":"string"}]}}
        ,
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    // Template-only change: ext_* now maps to numeric instead of keyword.
    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        \\{"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}},"dynamic_templates":[{"name":"ext","match":"ext_*","mapping":{"type":"numeric"}}]}
        ,
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    // Version is preserved (no document_schemas change) ...
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":2") != null);
    // ... but the durable algebraic config now carries the numeric rule, proving
    // the dynamic template propagated without a recreate or version bump.
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"match\":\"ext_*\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"type\":\"number\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"capability_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"stale\"") == null);
    // The capability changed (fingerprint differs from the stored one), so the
    // durable config records that existing docs need re-projection; query-time
    // resolution of dynamic fields is withheld until rebuild.
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"dynamic_rules_backfill_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"capability_lifecycle_status\":\"rebuild_required\"") != null);
    // The full-text index entry is left untouched.
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v2\"") != null);
}

test "metadata.schema update regenerates algebraic config and preserves user knobs" {
    // The stored algebraic config carries a tuned adaptive policy and a real
    // fingerprint. Re-applying the SAME schema must regenerate the config,
    // preserve the user knob, and (since the capability is unchanged) NOT set the
    // backfill-pending flag.
    const schema =
        \\{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}},"dynamic_templates":[{"name":"ext","match":"ext_*","mapping":{"type":"keyword"}}]}
    ;
    // First compute the canonical config the regenerator produces for this schema
    // so the stored fingerprint matches (simulating an already-current index).
    const canonical = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(std.testing.allocator, "docs", schema);
    defer std.testing.allocator.free(canonical);
    var canon_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, canonical, .{});
    defer canon_parsed.deinit();
    const fp = canon_parsed.value.object.get("capability_fingerprint").?.string;

    const stored = try std.fmt.allocPrint(std.testing.allocator, "{{\"alg\":{{\"type\":\"algebraic\",\"capability_fingerprint\":\"{s}\",\"adaptive\":{{\"observe\":false,\"min_observations\":9}},\"hll_cardinalities\":[{{\"name\":\"customers_by_region\",\"group_by\":[\"region\"],\"value_field\":\"customer\",\"precision\":12}}],\"dynamic_field_rules\":[{{\"name\":\"ext\",\"match\":\"ext_*\",\"type\":\"string\"}}]}}}}", .{fp});
    defer std.testing.allocator.free(stored);

    const refreshed = try regenerateAlgebraicIndexesFromSchemaAlloc(std.testing.allocator, "docs", stored, schema);
    defer std.testing.allocator.free(refreshed);

    // User knob preserved through regeneration.
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"min_observations\":9") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"hll_cardinalities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"customers_by_region\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"value_field\":\"customer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"precision\":12") != null);
    // Capability unchanged (matching fingerprint) => no backfill flag forced on.
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"dynamic_rules_backfill_pending\":true") == null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"capability_lifecycle_status\":\"rebuild_required\"") == null);
}

test "metadata.schema update marks static algebraic capability changes rebuild required" {
    const alloc = std.testing.allocator;
    const schema_v1 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["tenant"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":3,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"amount":{"type":"keyword"}},"required":["tenant"],"additionalProperties":false}}}}
    ;

    const canonical = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, "rows", schema_v1);
    defer alloc.free(canonical);
    const stored = try std.fmt.allocPrint(alloc, "{{\"alg\":{{\"type\":\"algebraic\",{s}}}", .{canonical[1..]});
    defer alloc.free(stored);

    const refreshed = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, "rows", stored, schema_v2);
    defer alloc.free(refreshed);

    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"capability_lifecycle_status\":\"rebuild_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"schema_version\":3") != null);
}

test "metadata.schema update marks changed relational json subdocument domains pending" {
    const alloc = std.testing.allocator;
    const schema_v1 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"attrs":{"type":"json","schema":{"type":"object","properties":{"plan":{"type":"keyword"},"score":{"type":"numeric"}},"additionalProperties":true}}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":3,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"attrs":{"type":"json","schema":{"type":"object","properties":{"plan":{"type":"keyword"},"score":{"type":"keyword"}},"additionalProperties":true}}},"required":["id"],"additionalProperties":false}}}}
    ;

    const canonical = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, "rows", schema_v1);
    defer alloc.free(canonical);
    const stored = try std.fmt.allocPrint(alloc, "{{\"alg\":{{\"type\":\"algebraic\",{s}}}", .{canonical[1..]});
    defer alloc.free(stored);

    const refreshed = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, "rows", stored, schema_v2);
    defer alloc.free(refreshed);

    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"json_subdocument_domains\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"path\":\"attrs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"lifecycle_status\":\"rebuild_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"capability_lifecycle_status\":\"rebuild_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, refreshed, "\"schema_version\":3") != null);
}

test "metadata.query routing selects read schema full text index" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":3}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v3\":{\"type\":\"full_text\"}}",
        .placement_role = "data",
    };
    var req: db_mod.types.SearchRequest = .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
    };
    defer if (req.index_name) |index_name| std.testing.allocator.free(index_name);
    defer if (req.primary_text_index_name) |index_name| std.testing.allocator.free(index_name);
    defer {
        std.testing.allocator.free(req.query.match.field);
        std.testing.allocator.free(req.query.match.text);
    }
    req.query.match.field = try std.testing.allocator.dupe(u8, req.query.match.field);
    req.query.match.text = try std.testing.allocator.dupe(u8, req.query.match.text);

    try routeQueryRequestToActiveReadIndex(std.testing.allocator, &table, &req);
    try std.testing.expectEqualStrings("full_text_index_v0", req.index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v0", req.primary_text_index_name.?);
}

test "metadata.query routing selects current versioned full text index" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":2}",
        .indexes_json = "{\"default\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .placement_role = "data",
    };
    var req: db_mod.types.SearchRequest = .{
        .full_text = .{ .match = .{ .field = try std.testing.allocator.dupe(u8, "body"), .text = try std.testing.allocator.dupe(u8, "hello") } },
    };
    defer if (req.index_name) |index_name| std.testing.allocator.free(index_name);
    defer if (req.primary_text_index_name) |index_name| std.testing.allocator.free(index_name);
    defer {
        std.testing.allocator.free(req.full_text.?.match.field);
        std.testing.allocator.free(req.full_text.?.match.text);
    }

    try routeQueryRequestToActiveReadIndex(std.testing.allocator, &table, &req);
    try std.testing.expectEqualStrings("full_text_index_v2", req.index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v2", req.primary_text_index_name.?);
}

test "metadata.query routing preserves vector index and records read schema text index for filters" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":3}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v3\":{\"type\":\"full_text\"}}",
        .placement_role = "data",
    };
    var req: db_mod.types.SearchRequest = .{
        .index_name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .full_text = .{ .match_all = {} },
        .filter_query_json = try std.testing.allocator.dupe(u8, "{\"term\":{\"status\":\"active\"}}"),
    };
    defer if (req.index_name) |index_name| std.testing.allocator.free(index_name);
    defer if (req.primary_text_index_name) |index_name| std.testing.allocator.free(index_name);
    defer std.testing.allocator.free(req.filter_query_json);

    try routeQueryRequestToActiveReadIndex(std.testing.allocator, &table, &req);
    try std.testing.expectEqualStrings("dense_idx", req.index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v0", req.primary_text_index_name.?);
}

test "metadata.query routing selects read schema text index for vector-only structured filters" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":3}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v3\":{\"type\":\"full_text\"}}",
        .placement_role = "data",
    };
    var req: db_mod.types.SearchRequest = .{
        .query = .{ .dense_knn = .{
            .vector = try std.testing.allocator.dupe(f32, &.{ 1, 2, 3 }),
            .k = 10,
        } },
        .index_name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .filter_query_json = try std.testing.allocator.dupe(u8, "{\"term\":{\"status\":\"active\"}}"),
        .exclusion_query_json = try std.testing.allocator.dupe(u8, "{\"term\":{\"archived\":true}}"),
    };
    defer if (req.index_name) |index_name| std.testing.allocator.free(index_name);
    defer if (req.primary_text_index_name) |index_name| std.testing.allocator.free(index_name);
    defer {
        std.testing.allocator.free(req.query.dense_knn.vector);
        std.testing.allocator.free(req.filter_query_json);
        std.testing.allocator.free(req.exclusion_query_json);
    }

    try routeQueryRequestToActiveReadIndex(std.testing.allocator, &table, &req);
    try std.testing.expectEqualStrings("dense_idx", req.index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v0", req.primary_text_index_name.?);
}
