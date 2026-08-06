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
const group_ids = @import("../../common/group_ids.zig");
const metadata_api = @import("snapshot.zig");
const metadata_admin = @import("../admin.zig");
const metadata_catalog_lookup = @import("lookup.zig");
const metadata_table_manager = @import("../table_manager.zig");
const metadata_transition_state = @import("../transition_state.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const db_mod = @import("../../storage/db/mod.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const schema_openapi = @import("antfly_schema_openapi");
const schema_mod = @import("../../schema/mod.zig");
const runtime_schema_mod = @import("../../storage/schema.zig");
const algebraic_mod = @import("../../storage/db/algebraic/mod.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const table_catalog = @import("source.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const indexes_api = @import("../../api/indexes.zig");
const json_helpers = @import("../../common/json_helpers.zig");
const catalog_resources = @import("resources.zig");
const sql_adapter = @import("../../sql/mod.zig");
const sql_schema_mutation = @import("../../sql/schema_mutation.zig");
const table_reads = @import("../../api/table_reads.zig");
const coverage_policy_mod = @import("../../api/coverage_policy.zig");

pub const default_full_text_index_name = full_text_indexes.default_full_text_index_name;
pub const default_indexes_json = "{\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}}";
pub const default_database_name = metadata_table_manager.default_database_name;
pub const default_namespace_name = metadata_table_manager.default_namespace_name;

/// Name of the algebraic aggregation index auto-created for relational tables.
pub const default_relational_algebraic_index_name = "algebraic_index_v0";

fn isIndexCatalogMetadata(name: []const u8) bool {
    return std.mem.eql(u8, name, "resolvers") or
        std.mem.eql(u8, name, "enrichments") or
        std.mem.eql(u8, name, "typed_paths");
}

fn validateIndexesValue(value: std.json.Value, comptime trusted_catalog: bool) !void {
    if (value != .object) return error.InvalidCreateTableRequest;
    var index_it = value.object.iterator();
    while (index_it.next()) |entry| {
        if (isIndexCatalogMetadata(entry.key_ptr.*)) continue;
        if (trusted_catalog) {
            coverage_policy_mod.validateStoredIndexConfig(entry.value_ptr.*) catch return error.InvalidCreateTableRequest;
        } else {
            coverage_policy_mod.validateIndexConfig(entry.value_ptr.*) catch return error.InvalidCreateTableRequest;
        }
    }
}

pub fn validateIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch return error.InvalidCreateTableRequest;
    defer parsed.deinit();
    try validateIndexesValue(parsed.value, false);
}

/// Validates metadata read from Antfly-owned durable catalogs or backup
/// manifests. Public request paths must use validateIndexesJson instead.
pub fn validateStoredIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch return error.InvalidCreateTableRequest;
    defer parsed.deinit();
    try validateIndexesValue(parsed.value, true);
}

fn normalizeRawCreateTableIndexesAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .object) return error.InvalidCreateTableRequest;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, default_indexes_json);

    var it = value.object.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const config = entry.value_ptr.*;
        if (!isIndexCatalogMetadata(name)) {
            const index_type = if (config == .object) config.object.get("type") else null;
            const is_full_text = index_type == null or
                (index_type.? == .string and std.mem.eql(u8, index_type.?.string, "full_text"));
            if (std.mem.eql(u8, name, "full_text_index_v0")) {
                if (!is_full_text) return error.InvalidCreateTableRequest;
                continue;
            }
            if (std.mem.startsWith(u8, name, "full_text_index")) return error.InvalidCreateTableRequest;
            if (is_full_text) continue;
        }

        out.items.len -= 1;
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, name);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, config);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
        try out.append(alloc, '}');
    }
    return try out.toOwnedSlice(alloc);
}

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
    created_sequence: bool = false,
    altered_sequence: bool = false,
    dropped_sequence: bool = false,
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

pub const RelationalSqlSchemaNamespaceDdlAction = enum {
    create_namespace,
    rename_namespace,
    drop_namespace,
};

pub const RelationalSqlSchemaNamespaceDdlTarget = struct {
    database_name: []u8,
    namespace_name: []u8,
    new_namespace_name: ?[]u8 = null,
    action: RelationalSqlSchemaNamespaceDdlAction,
    if_not_exists: bool = false,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.database_name);
        alloc.free(self.namespace_name);
        if (self.new_namespace_name) |new_namespace_name| alloc.free(new_namespace_name);
        self.* = undefined;
    }

    pub fn createsNamespace(self: @This()) bool {
        return self.action == .create_namespace;
    }

    pub fn renamesNamespace(self: @This()) bool {
        return self.action == .rename_namespace;
    }

    pub fn dropsNamespace(self: @This()) bool {
        return self.action == .drop_namespace;
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

const SingleTableCatalogSource = struct {
    tables: [1]metadata_table_manager.TableRecord,

    fn initAlloc(alloc: std.mem.Allocator, table: *const metadata_table_manager.TableRecord) !SingleTableCatalogSource {
        return .{
            .tables = .{try metadata_table_manager.cloneTable(alloc, table.*)},
        };
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        metadata_table_manager.freeTable(alloc, self.tables[0]);
        self.* = undefined;
    }

    fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .status = .{
                .metadata_group_id = 0,
                .metrics = .{},
            },
            .tables = self.tables[0..],
            .ranges = &.{},
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const BorrowedAdminSnapshotCatalogSource = struct {
    snapshot: *const metadata_api.AdminSnapshot,

    fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.snapshot.*;
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
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
    wal_checkpoint_pending: bool = false,
    wal_pressure_blocked: bool = false,
    wal_checkpoint_retry_reason: []const u8 = "none",
    wal_checkpoint_retry_attempts: u64 = 0,
    wal_checkpoint_retry_delay_ns: u64 = 0,
    active_immutable_logical_bytes: u64 = 0,
    unpublished_wal_logical_bytes: u64 = 0,
    unpublished_wal_max_batch_logical_bytes: u64 = 0,
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
    disk_usage: ?u64 = null,
    lsm: ?LsmStorageStatus = null,
    observed_dynamic_field_capability_sets: []table_reads.ObservedDynamicFieldCapabilitySet = &.{},
};

pub fn freeTableStorageStatuses(alloc: std.mem.Allocator, statuses: []TableStorageStatus) void {
    for (statuses) |*status| {
        for (status.observed_dynamic_field_capability_sets) |*set| set.deinit(alloc);
        if (status.observed_dynamic_field_capability_sets.len > 0) {
            alloc.free(status.observed_dynamic_field_capability_sets);
        }
    }
    if (statuses.len > 0) alloc.free(statuses);
}

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
        .wal_checkpoint_pending = maintenance.wal_checkpoint_pending,
        .wal_pressure_blocked = maintenance.wal_pressure_blocked,
        .wal_checkpoint_retry_reason = @tagName(maintenance.wal_checkpoint_retry_reason),
        .wal_checkpoint_retry_attempts = maintenance.wal_checkpoint_retry_attempts,
        .wal_checkpoint_retry_delay_ns = maintenance.wal_checkpoint_retry_delay_ns,
        .active_immutable_logical_bytes = maintenance.active_immutable_logical_bytes,
        .unpublished_wal_logical_bytes = maintenance.unpublished_wal_logical_bytes,
        .unpublished_wal_max_batch_logical_bytes = maintenance.unpublished_wal_max_batch_logical_bytes,
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

test "metadata.table lsm status exposes wal retry and publication debt" {
    const status = lsmStorageStatusFromMaintenanceStats(.{
        .wal_checkpoint_pending = true,
        .wal_pressure_blocked = true,
        .wal_checkpoint_retry_reason = .checkpoint_failure,
        .wal_checkpoint_retry_attempts = 4,
        .wal_checkpoint_retry_delay_ns = 500,
        .active_immutable_logical_bytes = 600,
        .unpublished_wal_logical_bytes = 700,
        .unpublished_wal_max_batch_logical_bytes = 350,
    });
    try std.testing.expect(status.wal_checkpoint_pending);
    try std.testing.expect(status.wal_pressure_blocked);
    try std.testing.expectEqualStrings("checkpoint_failure", status.wal_checkpoint_retry_reason);
    try std.testing.expectEqual(@as(u64, 4), status.wal_checkpoint_retry_attempts);
    try std.testing.expectEqual(@as(u64, 500), status.wal_checkpoint_retry_delay_ns);
    try std.testing.expectEqual(@as(u64, 600), status.active_immutable_logical_bytes);
    try std.testing.expectEqual(@as(u64, 700), status.unpublished_wal_logical_bytes);
    try std.testing.expectEqual(@as(u64, 350), status.unpublished_wal_max_batch_logical_bytes);
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
        .wal_checkpoint_pending = status.wal_checkpoint_pending,
        .wal_pressure_blocked = status.wal_pressure_blocked,
        .wal_checkpoint_retry_reason = status.wal_checkpoint_retry_reason,
        .wal_checkpoint_retry_attempts = u64ToI64(status.wal_checkpoint_retry_attempts),
        .wal_checkpoint_retry_delay_ns = u64ToI64(status.wal_checkpoint_retry_delay_ns),
        .active_immutable_logical_bytes = u64ToI64(status.active_immutable_logical_bytes),
        .unpublished_wal_logical_bytes = u64ToI64(status.unpublished_wal_logical_bytes),
        .unpublished_wal_max_batch_logical_bytes = u64ToI64(status.unpublished_wal_max_batch_logical_bytes),
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
    observed_dynamic_field_capabilities: ?std.json.Value = null,
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
    field_capabilities: ?[]const metadata_openapi.FieldCapability = null,
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
    const debug = try buildTableRuntimeSchemaDebug(alloc, table, findTableStorageStatus(storage_statuses, table.name));
    return .{
        .name = base.name,
        .description = base.description,
        .indexes = base.indexes,
        .shards = base.shards,
        .schema = base.schema,
        .migration = base.migration,
        .replication_sources = base.replication_sources,
        .field_capabilities = base.field_capabilities,
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
    return parseCreateTableRequestWithOptions(alloc, body, false);
}

pub fn parseStoredCreateTableRequest(alloc: std.mem.Allocator, body: []const u8) !CreateTableRequest {
    return parseCreateTableRequestWithOptions(alloc, body, true);
}

pub fn validateCreateSchemaVersion(value: std.json.Value, comptime allow_normalized_version_zero: bool) !void {
    if (value != .object) return error.InvalidCreateTableRequest;
    const version = value.object.get("version") orelse return;
    switch (version) {
        .null => return,
        .integer => |integer| {
            // The public create contract is intentionally stricter than the
            // trusted metadata hop. Metadata receives the normalized v0 schema
            // produced by the public API, but no caller may choose a generation.
            if (!allow_normalized_version_zero or integer != 0) return error.SchemaVersionManagedByBackend;
        },
        else => return error.InvalidCreateTableRequest,
    }
}

fn parseCreateTableRequestWithOptions(
    alloc: std.mem.Allocator,
    body: []const u8,
    comptime allow_private_index_fields: bool,
) !CreateTableRequest {
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
        if (value != .null) {
            try validateIndexesValue(value, allow_private_index_fields);
            const normalized_indexes_json = try normalizeRawCreateTableIndexesAlloc(alloc, value);
            defer alloc.free(normalized_indexes_json);
            req.indexes_json = try coverage_policy_mod.withMissingIncarnationsAlloc(alloc, normalized_indexes_json);
        } else req.indexes_json = try alloc.dupe(u8, default_indexes_json);
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
            try validateCreateSchemaVersion(value, allow_private_index_fields);
            const normalized_schema = normalizeSchemaVersion(alloc, validated_schema, 0) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableRequest,
                else => return err,
            };
            var normalized_schema_owned = true;
            errdefer if (normalized_schema_owned) alloc.free(normalized_schema);
            validateRuntimeDerivableSchemaJson(alloc, normalized_schema) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableRequest,
                else => return err,
            };
            req.schema_json = normalized_schema;
            normalized_schema_owned = false;
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
    var parsed_index = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed_index.deinit();
    if (schema_json.len == 0) {
        try validateSchemaIndependentIndexRefsValue(parsed_index.value);
        return;
    }
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    try validateDerivedIndexFieldRefsValue(runtime, parsed_index.value);
}

fn validateSchemaIndependentIndexRefsValue(value: std.json.Value) !void {
    if (value != .object) return error.InvalidTableIndexMetadata;
    const type_value = value.object.get("type") orelse return;
    if (type_value != .string) return error.InvalidTableIndexMetadata;
    if (std.mem.eql(u8, type_value.string, "full_text")) {
        if (value.object.get("field") != null) return error.InvalidTableIndexMetadata;
    } else if (std.mem.eql(u8, type_value.string, "embeddings")) {
        if (value.object.get("field") != null) return error.InvalidTableIndexMetadata;
        if (value.object.get("embedder")) |embedder| try validateEmbedderModelRefValue(embedder);
    } else if (std.mem.eql(u8, type_value.string, "graph")) {
        if (value.object.get("edge_table") != null) return error.InvalidTableIndexMetadata;
        if (value.object.get("artifact")) |artifact| {
            if (artifact != .object) return error.InvalidTableIndexMetadata;
            if (artifact.object.get("field") != null) return error.InvalidTableIndexMetadata;
            if (artifact.object.get("producer_json")) |producer| try validateProducerModelRefValue(producer);
        }
        if (value.object.get("context")) |context| {
            if (context != .object) return error.InvalidTableIndexMetadata;
            if (context.object.get("doc_fields") != null) return error.InvalidTableIndexMetadata;
        }
    }

    if (value.object.get("enrichments")) |enrichments| {
        if (enrichments != .array) return error.InvalidTableIndexMetadata;
        for (enrichments.array.items) |enrichment| {
            if (enrichment != .object) return error.InvalidTableIndexMetadata;
            if (enrichment.object.get("field") != null) return error.InvalidTableIndexMetadata;
            if (enrichment.object.get("model")) |model| try validateModelRefValue(model);
        }
    }
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

pub fn deriveRuntimeTableSchema(alloc: std.mem.Allocator, schema: ParsedTableSchema) !@import("../../storage/schema.zig").TableSchema {
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

    const current_version = try schemaVersion(table.schema_json);
    const schema_changed = !try schemasSemanticallyEqual(alloc, table.schema_json, schema_json);
    const next_version = if (schema_changed)
        std.math.add(u32, current_version, 1) catch return error.SchemaVersionExhausted
    else
        current_version;

    var updated = try metadata_table_manager.cloneTable(alloc, table.*);
    errdefer metadata_table_manager.freeTable(alloc, updated);

    const normalized_schema_json = try normalizeSchemaVersion(alloc, schema_json, next_version);
    var normalized_schema_json_owned = true;
    errdefer if (normalized_schema_json_owned) alloc.free(normalized_schema_json);
    alloc.free(updated.schema_json);
    updated.schema_json = normalized_schema_json;
    normalized_schema_json_owned = false;

    // Refresh schema-derived algebraic configs (dynamic_field_rules + capability
    // fingerprint) on every update so the algebraic sidecar tracks equivalent
    // source-schema rewrites without requiring a recreate.
    const refreshed_indexes_json = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, table.name, updated.indexes_json, updated.schema_json);
    alloc.free(updated.indexes_json);
    updated.indexes_json = refreshed_indexes_json;

    if (schema_changed) {
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

fn validateRuntimeDerivableSchemaJson(alloc: std.mem.Allocator, schema_json: []const u8) !void {
    var parsed_schema = try parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);

    const runtime_schema = try deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema_mod.freeSchema(alloc, runtime_schema);
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
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try applyRelationalSqlDdlParsedSqlToTableRecordWithSessionAndFunctionBindingsAlloc(alloc, table, &parsed_sql, session, function_bindings);
}

pub fn applyRelationalSqlDdlParsedSqlToTableRecordAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    parsed_sql: *const sql_adapter.ParsedSql,
) !AppliedRelationalSqlDdlRecord {
    return try applyRelationalSqlDdlParsedSqlToTableRecordWithSessionAlloc(alloc, table, parsed_sql, catalog_resources.SqlCatalogSession.default());
}

pub fn applyRelationalSqlDdlParsedSqlToTableRecordWithSessionAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    parsed_sql: *const sql_adapter.ParsedSql,
    session: catalog_resources.SqlCatalogSession,
) !AppliedRelationalSqlDdlRecord {
    return try applyRelationalSqlDdlParsedSqlToTableRecordWithSessionAndFunctionBindingsAlloc(alloc, table, parsed_sql, session, .{});
}

pub fn applyRelationalSqlDdlParsedSqlToTableRecordWithSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    parsed_sql: *const sql_adapter.ParsedSql,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !AppliedRelationalSqlDdlRecord {
    var durable_plan = try durableRelationalSqlDdlPlanFromParsedSqlAlloc(alloc, parsed_sql, function_bindings);
    defer durable_plan.deinit(alloc);
    return try applyRelationalSqlDdlDurablePlanToTableRecordWithSessionAlloc(alloc, table, &durable_plan, session);
}

fn durableRelationalSqlDdlPlanFromParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !sql_adapter.DurableSqlPlan {
    var logical_plan = try sql_adapter.logicalDdlPlanParsedSqlWithFunctionBindingsAlloc(alloc, parsed_sql, function_bindings);
    errdefer logical_plan.deinit(alloc);
    return try sql_adapter.takeDurableSqlPlanFromLogical(&logical_plan);
}

pub fn applyRelationalSqlDdlDurablePlanToTableRecordWithSessionAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    durable_plan: *sql_adapter.DurableSqlPlan,
    session: catalog_resources.SqlCatalogSession,
) !AppliedRelationalSqlDdlRecord {
    return switch (durable_plan.*) {
        .table_ddl => |*table_plan| try applyTableDdlPlanToTableRecordWithSessionAlloc(alloc, table, table_plan, session),
        else => error.UnsupportedSqlShape,
    };
}

pub fn applyTableDdlPlanToTableRecordWithSessionAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    plan: *sql_adapter.TableDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !AppliedRelationalSqlDdlRecord {
    if (try relationalSqlTableDdlPlanTableRefWithSessionAlloc(alloc, plan.*, session)) |table_ref_value| {
        var table_ref = table_ref_value;
        defer table_ref.deinit(alloc);
        if (!tableCatalogIdentityMatches(table.*, table_ref.database_name, table_ref.namespace_name, table_ref.table_name)) return error.InvalidSchemaUpdateRequest;
        try retargetRelationalSqlTableDdlPlanTableNameAlloc(alloc, plan, table.name);
    }

    switch (plan.*) {
        .create_index => |create_index| {
            if (create_index.derived_index_config_json) |index_json| if (sql_adapter.createIndexPlanIsSchemaDerivedAlgebraic(create_index)) {
                return try applyRelationalAlgebraicIndexCreateToTableRecordAlloc(alloc, table, create_index, index_json);
            } else if (create_index.method != .antfly_full_text) {
                return try applyRelationalDerivedIndexCreateToTableRecordAlloc(alloc, table, create_index, index_json);
            };
        },
        .drop_index => |drop_index| {
            if (try tableIndexesJsonContainsIndex(alloc, table.indexes_json, drop_index.index_name)) {
                return try applyRelationalDerivedIndexDropToTableRecordAlloc(alloc, table, drop_index.index_name);
            }
            try validateDerivedIndexHasNoDependentsAlloc(alloc, table.indexes_json, drop_index.index_name);
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
    var applied = try sql_adapter.applyTableDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, plan.*);
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

fn applyRelationalAlgebraicIndexCreateToTableRecordAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    plan: sql_adapter.CreateIndexPlan,
    index_json: []const u8,
) !AppliedRelationalSqlDdlRecord {
    if (table.schema_json.len == 0) return error.InvalidSchemaUpdateRequest;

    var applied = try sql_adapter.applyTableDdlPlanToSchemaJsonAlloc(alloc, table.schema_json, .{ .create_index = plan });
    defer applied.deinit(alloc);

    var updated = try applyRelationalDdlSchemaRecordAlloc(alloc, table, applied.schema_json);
    errdefer metadata_table_manager.freeTable(alloc, updated);

    if (!(try tableIndexesJsonContainsIndex(alloc, updated.indexes_json, plan.index_name))) {
        try validateDerivedIndexFieldRefsForSchemaAlloc(alloc, index_json, updated.schema_json);
        try validateDerivedIndexCatalogRefsForTableIndexesAlloc(alloc, index_json, updated.indexes_json, updated.schema_json);

        const next_indexes_json = try indexes_api.addIndexToTableIndexesJson(alloc, updated.indexes_json, plan.index_name, index_json);
        defer alloc.free(next_indexes_json);
        const prepared_indexes_json = try prepareTableIndexesForSchemaAlloc(alloc, updated.name, next_indexes_json, updated.schema_json);
        defer alloc.free(prepared_indexes_json);

        const owned_indexes_json = try alloc.dupe(u8, prepared_indexes_json);
        alloc.free(updated.indexes_json);
        updated.indexes_json = owned_indexes_json;
    }

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

pub fn applyUntargetedRelationalDerivedIndexTablePlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: *sql_adapter.TableDdlLogicalPlan,
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

    var applied = try applyTableDdlPlanToTableRecordWithSessionAlloc(
        alloc,
        table,
        plan,
        session,
    );
    errdefer applied.deinit(alloc);
    try applyTableCatalogUpdateWithoutSchemaRewriteJobsOnService(svc, applied.table);
    return applied;
}

fn applyTableCatalogUpdateWithoutSchemaRewriteJobsOnService(
    svc: anytype,
    table: metadata_table_manager.TableRecord,
) !void {
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (comptime @hasDecl(ServiceDeclType, "applyTableCatalogUpdateWithSchemaRewriteJobs")) {
        return try svc.applyTableCatalogUpdateWithSchemaRewriteJobs(.{
            .table = table,
            .schema_rewrite_jobs = &.{},
        });
    }
    return error.UnsupportedOperation;
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

pub fn applyRelationalDerivedIndexTablePlanOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    table: *const metadata_table_manager.TableRecord,
    target: RelationalSqlDdlTarget,
    plan: sql_adapter.TableDdlLogicalPlan,
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

    var applied = AppliedRelationalSqlDdlRecord{
        .table = try metadata_table_manager.cloneTable(alloc, updated_record),
        .requires_rebuild = true,
    };
    errdefer applied.deinit(alloc);
    try applyTableCatalogUpdateWithoutSchemaRewriteJobsOnService(svc, applied.table);
    return applied;
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
    try validateDerivedIndexCatalogRefsForTableIndexesAlloc(alloc, index_json, table.indexes_json, table.schema_json);

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
    schema_json: []const u8,
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
            if (indexes.get(source.string)) |source_config| {
                try validateHybridSourceIndexConfigType(source_config);
                continue;
            }
            if (try relationalTextSearchIndexExistsAlloc(alloc, schema_json, source.string)) continue;
            return error.InvalidTableIndexMetadata;
        }
    }
}

fn relationalTextSearchIndexExistsAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    index_name: []const u8,
) !bool {
    if (schema_json.len == 0) return false;
    var parsed = try parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    const index = secondaryIndexCatalogByName(runtime.relational_indexes, index_name) orelse return false;
    return index.access_method == .text_search;
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
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try relationalSqlDdlTargetParsedSqlWithSessionAndFunctionBindingsAlloc(alloc, &parsed_sql, session, function_bindings);
}

pub fn relationalSqlDdlTargetParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
) !RelationalSqlDdlTarget {
    return try relationalSqlDdlTargetParsedSqlWithSessionAlloc(alloc, parsed_sql, catalog_resources.SqlCatalogSession.default());
}

pub fn relationalSqlDdlTargetParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlDdlTarget {
    return try relationalSqlDdlTargetParsedSqlWithSessionAndFunctionBindingsAlloc(alloc, parsed_sql, session, .{});
}

pub fn relationalSqlDdlTargetParsedSqlWithSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: sql_adapter.SqlFunctionBindings,
) !RelationalSqlDdlTarget {
    var durable_plan = try durableRelationalSqlDdlPlanFromParsedSqlAlloc(alloc, parsed_sql, function_bindings);
    defer durable_plan.deinit(alloc);
    return switch (durable_plan) {
        .table_ddl => |table_plan| try relationalSqlDdlTargetForTablePlanWithSessionAlloc(alloc, table_plan, session),
        else => error.UnsupportedSqlShape,
    };
}

pub fn relationalSqlSchemaNamespaceDdlTargetWithSessionAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlSchemaNamespaceDdlTarget {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try relationalSqlSchemaNamespaceDdlTargetParsedSqlWithSessionAlloc(alloc, &parsed_sql, session);
}

pub fn relationalSqlSchemaNamespaceDdlTargetParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlSchemaNamespaceDdlTarget {
    var durable_plan = try durableRelationalSqlDdlPlanFromParsedSqlAlloc(alloc, parsed_sql, .{});
    defer durable_plan.deinit(alloc);
    return switch (durable_plan) {
        .catalog_ddl => |catalog_plan| switch (catalog_plan) {
            .schema_namespace_catalog => |namespace_plan| try relationalSqlSchemaNamespaceDdlTargetForPlanWithSessionAlloc(alloc, namespace_plan, session),
            else => error.UnsupportedSqlShape,
        },
        else => error.UnsupportedSqlShape,
    };
}

pub fn relationalSqlSchemaNamespaceDdlTargetForPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    plan: sql_adapter.SchemaNamespaceCatalogPlan,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlSchemaNamespaceDdlTarget {
    return switch (plan) {
        .create => |create| blk: {
            var target = try ownedRelationalSqlSchemaNamespaceDdlTargetAlloc(alloc, try session.namespaceTargetFromSchemaName(create.schema_name));
            errdefer target.deinit(alloc);
            target.action = .create_namespace;
            target.if_not_exists = create.if_not_exists;
            break :blk target;
        },
        .rename => |rename| blk: {
            const old_target = try session.namespaceTargetFromSchemaName(rename.schema_name);
            const new_target = try session.namespaceTargetFromSchemaName(rename.new_schema_name);
            if (!std.mem.eql(u8, old_target.database_name, new_target.database_name)) return error.UnsupportedSqlShape;
            var target = try ownedRelationalSqlSchemaNamespaceDdlTargetAlloc(alloc, old_target);
            errdefer target.deinit(alloc);
            target.action = .rename_namespace;
            target.new_namespace_name = try alloc.dupe(u8, new_target.namespace_name);
            break :blk target;
        },
        .drop => |drop| blk: {
            var target = try ownedRelationalSqlSchemaNamespaceDdlTargetAlloc(alloc, try session.namespaceTargetFromSchemaName(drop.schema_name));
            errdefer target.deinit(alloc);
            target.action = .drop_namespace;
            target.if_exists = drop.if_exists;
            target.cascade = drop.cascade;
            break :blk target;
        },
    };
}

pub fn relationalSqlDdlTargetForTablePlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    plan: sql_adapter.TableDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !RelationalSqlDdlTarget {
    var table_ref = (try relationalSqlTableDdlPlanTableRefWithSessionAlloc(alloc, plan, session)) orelse return error.UnsupportedSqlShape;
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

fn ownedRelationalSqlSchemaNamespaceDdlTargetAlloc(
    alloc: std.mem.Allocator,
    target: catalog_resources.NamespaceTarget,
) !RelationalSqlSchemaNamespaceDdlTarget {
    const owned_database_name = try alloc.dupe(u8, target.database_name);
    errdefer alloc.free(owned_database_name);
    const owned_namespace_name = try alloc.dupe(u8, target.namespace_name);
    errdefer alloc.free(owned_namespace_name);
    return .{
        .database_name = owned_database_name,
        .namespace_name = owned_namespace_name,
        .action = .create_namespace,
    };
}

fn relationalSqlTableDdlPlanTableRefWithSessionAlloc(
    alloc: std.mem.Allocator,
    plan: sql_adapter.TableDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !?RelationalSqlDdlTableRef {
    const table_name = relationalSqlTableDdlPlanTableName(plan) orelse return null;
    return try parseRelationalSqlDdlTableRefWithSessionAlloc(alloc, table_name, session);
}

fn relationalSqlTableDdlPlanTableName(plan: sql_adapter.TableDdlLogicalPlan) ?[]const u8 {
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

fn retargetRelationalSqlTableDdlPlanTableNameAlloc(
    alloc: std.mem.Allocator,
    plan: *sql_adapter.TableDdlLogicalPlan,
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
    const schema_changed = table.schema_json.len == 0 or !try schemasSemanticallyEqual(alloc, table.schema_json, schema_json);
    const next_version = if (table.schema_json.len == 0)
        0
    else if (schema_changed)
        std.math.add(u32, current_version, 1) catch return error.SchemaVersionExhausted
    else
        current_version;

    const normalized_schema_json = try normalizeSchemaVersion(alloc, schema_json, next_version);
    alloc.free(updated.schema_json);
    updated.schema_json = normalized_schema_json;

    const refreshed_indexes_json = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, table.name, updated.indexes_json, updated.schema_json);
    alloc.free(updated.indexes_json);
    updated.indexes_json = refreshed_indexes_json;

    if (schema_changed and table.schema_json.len > 0) {
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

    if (!relationalColumnCatalogsEqualForSchemaUpdate(current_runtime.relational_columns, next_runtime.relational_columns)) {
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

fn relationalColumnCatalogsEqualForSchemaUpdate(
    current: []const runtime_schema_mod.RelationalColumn,
    next: []const runtime_schema_mod.RelationalColumn,
) bool {
    if (current.len != next.len) return false;
    for (current, next) |left, right| {
        if (!std.mem.eql(u8, left.name, right.name)) return false;
        if (!std.mem.eql(u8, left.path, right.path)) return false;
        if (runtime_schema_mod.relationalColumnDefinitionsEqual(left, right)) continue;
        if (!relationalColumnDefinitionsEqualIgnoringSecondaryIndexLifecycle(left, right)) return false;
    }
    return true;
}

fn relationalColumnDefinitionsEqualIgnoringSecondaryIndexLifecycle(
    current: runtime_schema_mod.RelationalColumn,
    next: runtime_schema_mod.RelationalColumn,
) bool {
    if (!current.indexed or !next.indexed) return false;
    var normalized_next = next;
    normalized_next.index_lifecycle = current.index_lifecycle;
    normalized_next.index_generation = current.index_generation;
    normalized_next.index_access_method = current.index_access_method;
    normalized_next.index_schema_fingerprint = current.index_schema_fingerprint;
    return runtime_schema_mod.relationalColumnDefinitionsEqual(current, normalized_next);
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

pub fn schemaWithForeignKeyValidationStateAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    constraint_name: []const u8,
    state: runtime_schema_mod.ForeignKeyValidationState,
) ![]u8 {
    return try sql_schema_mutation.schemaWithForeignKeyValidationStateAlloc(alloc, schema_json, constraint_name, state);
}

pub fn schemaWithUniqueConstraintValidationStateAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    constraint_name: []const u8,
    state: runtime_schema_mod.UniqueConstraintValidationState,
) ![]u8 {
    return try sql_schema_mutation.schemaWithUniqueConstraintValidationStateAlloc(alloc, schema_json, constraint_name, state);
}

pub fn schemaWithSecondaryIndexReadyCheckedAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    index_name: []const u8,
    expected: sql_schema_mutation.SecondaryIndexReadyExpectation,
) ![]u8 {
    return try sql_schema_mutation.schemaWithSecondaryIndexReadyCheckedAlloc(alloc, schema_json, index_name, expected);
}

pub fn schemaWithSecondaryIndexBuildingAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    index_name: []const u8,
    new_generation: u64,
) ![]u8 {
    if (new_generation == 0) return error.InvalidSchemaUpdateRequest;
    const generation_integer = std.math.cast(i64, new_generation) orelse return error.InvalidSchemaUpdateRequest;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const json_alloc = parsed.arena.allocator();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const index = relationalIndexObjectForSecondaryIndex(root, index_name) orelse return error.SecondaryIndexNotFound;

    if (index.getPtr("lifecycle")) |value| {
        value.* = .{ .string = "building" };
    } else {
        const key = try json_alloc.dupe(u8, "lifecycle");
        try index.put(json_alloc, key, .{ .string = "building" });
    }
    if (index.getPtr("generation")) |value| {
        value.* = .{ .integer = generation_integer };
    } else {
        const key = try json_alloc.dupe(u8, "generation");
        try index.put(json_alloc, key, .{ .integer = generation_integer });
    }
    const access_method_value = index.get("access_method") orelse return error.InvalidSchemaUpdateRequest;
    if (access_method_value != .string or access_method_value.string.len == 0) return error.InvalidSchemaUpdateRequest;
    const access_method = runtime_schema_mod.RelationalIndexAccessMethod.fromString(access_method_value.string) orelse return error.InvalidSchemaUpdateRequest;
    const fingerprint_value = index.get("schema_fingerprint") orelse return error.InvalidSchemaUpdateRequest;
    if (fingerprint_value != .string or fingerprint_value.string.len == 0) return error.InvalidSchemaUpdateRequest;
    try markRelationalIndexGenerationRecordBuilding(index, access_method, generation_integer);
    try ensureRelationalIndexPlannerCapabilities(json_alloc, index, access_method);

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    defer validated.deinit(alloc);
    const runtime = try schema_mod.deriveRuntimeTableSchema(alloc, validated);
    defer runtime_schema_mod.freeSchema(alloc, runtime);

    const runtime_index = secondaryIndexCatalogByName(runtime.relational_indexes, index_name) orelse return error.SecondaryIndexNotFound;
    if (runtime_index.lifecycle != .building or runtime_index.generation != new_generation) {
        return error.InvalidSchemaUpdateRequest;
    }
    return updated;
}

fn markRelationalIndexGenerationRecordBuilding(
    index: *std.json.ObjectMap,
    access_method: runtime_schema_mod.RelationalIndexAccessMethod,
    generation_integer: i64,
) !void {
    switch (access_method) {
        .scalar_column => return,
        .ordered_tuple, .text_search, .algebraic_filter => {},
    }
    const record_value = index.getPtr("generation_record") orelse return error.InvalidSchemaUpdateRequest;
    if (record_value.* != .object) return error.InvalidSchemaUpdateRequest;
    const record = &record_value.object;
    if (record.getPtr("generation")) |value| {
        value.* = .{ .integer = generation_integer };
    } else {
        return error.InvalidSchemaUpdateRequest;
    }
    if (record.getPtr("lifecycle")) |value| {
        value.* = .{ .string = "building" };
    } else {
        return error.InvalidSchemaUpdateRequest;
    }
}

fn ensureRelationalIndexPlannerCapabilities(
    json_alloc: std.mem.Allocator,
    index: *std.json.ObjectMap,
    access_method: runtime_schema_mod.RelationalIndexAccessMethod,
) !void {
    if (index.get("planner_capabilities")) |value| {
        if (value != .object) return error.InvalidSchemaUpdateRequest;
        return;
    }

    var capabilities = std.json.ObjectMap.empty;
    try putPlannerCapability(json_alloc, &capabilities, "equality", access_method == .scalar_column or access_method == .ordered_tuple or access_method == .algebraic_filter);
    try putPlannerCapability(json_alloc, &capabilities, "range", access_method == .scalar_column or access_method == .ordered_tuple);
    try putPlannerCapability(json_alloc, &capabilities, "ordering", access_method == .ordered_tuple);
    try putPlannerCapability(json_alloc, &capabilities, "prefix", access_method == .algebraic_filter);
    try putPlannerCapability(json_alloc, &capabilities, "full_text", access_method == .text_search);
    try putPlannerCapability(json_alloc, &capabilities, "array", access_method == .algebraic_filter);
    try putPlannerCapability(json_alloc, &capabilities, "json", access_method == .algebraic_filter);
    try putPlannerCapability(json_alloc, &capabilities, "covering", false);
    try putPlannerCapability(json_alloc, &capabilities, "rank", access_method == .text_search);
    try putPlannerCapability(json_alloc, &capabilities, "algebraic_dictionary", access_method == .algebraic_filter);
    try putPlannerCapability(json_alloc, &capabilities, "algebraic_fact", access_method == .algebraic_filter);
    try putPlannerCapability(json_alloc, &capabilities, "algebraic_path", access_method == .algebraic_filter);

    const key = try json_alloc.dupe(u8, "planner_capabilities");
    try index.put(json_alloc, key, .{ .object = capabilities });
}

fn putPlannerCapability(
    json_alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    name: []const u8,
    value: bool,
) !void {
    const key = try json_alloc.dupe(u8, name);
    try object.put(json_alloc, key, .{ .bool = value });
}

fn relationalIndexObjectForSecondaryIndex(root: *std.json.ObjectMap, index_name: []const u8) ?*std.json.ObjectMap {
    const indexes = root.getPtr("relational_indexes") orelse return null;
    if (indexes.* != .array) return null;
    for (indexes.array.items) |*index_value| {
        if (index_value.* != .object) continue;
        const name = index_value.object.get("name") orelse continue;
        if (name != .string or !std.mem.eql(u8, name.string, index_name)) continue;
        return &index_value.object;
    }
    return null;
}

fn secondaryIndexCatalogByName(indexes: []const runtime_schema_mod.RelationalIndex, index_name: []const u8) ?runtime_schema_mod.RelationalIndex {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) return index;
    }
    return null;
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
            .state = .rebuilding,
            .read_schema = try parseTableSchema(alloc, table.read_schema_json),
        } else null,
        .replication_sources = try parseReplicationSources(alloc, snapshot, table, include_replication_runtime),
        .field_capabilities = try generatedFieldCapabilitiesAlloc(alloc, table, storage_status),
        .storage_status = .{
            .disk_usage = if (storage_status) |status|
                if (status.disk_usage) |bytes| @intCast(@min(bytes, std.math.maxInt(i64))) else null
            else
                null,
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

fn generatedFieldCapabilitiesAlloc(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    storage_status: ?TableStorageStatus,
) !?[]const metadata_openapi.FieldCapability {
    const schema_json = if (table.read_schema_json.len > 0) table.read_schema_json else effectiveSchemaJson(table.schema_json);
    var parsed_schema = schema_mod.parseValidatedTableSchema(alloc, schema_json) catch return null;
    defer parsed_schema.deinit(alloc);

    const runtime_schema = schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema) catch return null;
    defer runtime_schema_mod.freeSchema(alloc, runtime_schema);

    const schema_capabilities = try runtime_schema_mod.fieldCapabilitiesAlloc(alloc, runtime_schema);
    defer runtime_schema_mod.freeFieldCapabilities(alloc, schema_capabilities);

    var out = std.ArrayListUnmanaged(GeneratedFieldCapability).empty;
    var capability_index = GeneratedFieldCapabilityIndex.empty;
    defer freeGeneratedFieldCapabilityIndex(alloc, &capability_index);
    errdefer freeGeneratedFieldCapabilitiesFromList(alloc, &out);

    for (schema_capabilities) |capability| {
        try appendGeneratedFieldCapability(alloc, &out, &capability_index, try generatedFieldCapabilityAlloc(alloc, capability));
    }

    for (observedDynamicFieldCapabilitySetsFromStatus(storage_status)) |set| {
        for (set.field_capabilities) |capability| {
            try appendRuntimeGeneratedFieldCapability(alloc, &out, &capability_index, try generatedFieldCapabilityAlloc(alloc, capability));
        }
    }

    return try generatedFieldCapabilitiesPublicSliceAlloc(alloc, &out);
}

const GeneratedFieldCapability = struct {
    name: ?[]const u8 = null,
    field: ?[]const u8 = null,
    path_pattern: ?[]const u8 = null,
    field_pattern: ?[]const u8 = null,
    match_mapping_type: ?[]const u8 = null,
    emitted_name: ?[]const u8 = null,
    document_schema: ?[]const u8 = null,
    type: metadata_openapi.AntflyType,
    query_modes: []const []const u8,
    sortable: bool,
    doc_value_coverage: []const u8,
    provenance: []const u8,
    missing_null_policy: []const u8,
    queryability_state: []const u8,
    sort_lifecycle_state: []const u8,
    analyzer: ?[]const u8 = null,
    index_sort_position: ?i64 = null,
    index_sort_order: ?[]const u8 = null,
};

const GeneratedFieldCapabilityIndex = std.StringHashMapUnmanaged(usize);

fn appendGeneratedFieldCapability(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(GeneratedFieldCapability),
    capability_index: *GeneratedFieldCapabilityIndex,
    capability: GeneratedFieldCapability,
) !void {
    const owned = capability;
    var owned_in_list = false;
    errdefer if (!owned_in_list) freeGeneratedFieldCapability(alloc, owned);
    const key = try generatedFieldCapabilityAggregationKeyAlloc(alloc, owned);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    if (capability_index.get(key)) |existing_index| {
        const existing = &out.items[existing_index];
        std.debug.assert(generatedFieldCapabilityAggregationKeyEqual(existing.*, owned));
        try mergeGeneratedFieldCapability(alloc, existing, owned);
        alloc.free(key);
        key_owned = false;
        freeGeneratedFieldCapability(alloc, owned);
        return;
    }
    var key_in_index = false;
    errdefer if (key_in_index) {
        _ = capability_index.remove(key);
        alloc.free(key);
    };
    try capability_index.put(alloc, key, out.items.len);
    key_owned = false;
    key_in_index = true;
    try out.append(alloc, owned);
    owned_in_list = true;
}

fn appendRuntimeGeneratedFieldCapability(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(GeneratedFieldCapability),
    capability_index: *GeneratedFieldCapabilityIndex,
    capability: GeneratedFieldCapability,
) !void {
    const owned = capability;
    var owned_in_list = false;
    errdefer if (!owned_in_list) freeGeneratedFieldCapability(alloc, owned);
    const key = try generatedFieldCapabilityAggregationKeyAlloc(alloc, owned);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    if (capability_index.get(key)) |existing_index| {
        const existing = &out.items[existing_index];
        std.debug.assert(generatedFieldCapabilityAggregationKeyEqual(existing.*, owned));
        if (runtimeCapabilityCanPromoteSchemaDeclaration(existing.*, owned)) {
            try replaceOwnedStringIfDifferent(alloc, &existing.doc_value_coverage, owned.doc_value_coverage);
            try replaceOwnedStringIfDifferent(alloc, &existing.queryability_state, owned.queryability_state);
            try replaceOwnedStringIfDifferent(alloc, &existing.sort_lifecycle_state, owned.sort_lifecycle_state);
            alloc.free(key);
            key_owned = false;
            freeGeneratedFieldCapability(alloc, owned);
            return;
        }
        try mergeGeneratedFieldCapability(alloc, existing, owned);
        alloc.free(key);
        key_owned = false;
        freeGeneratedFieldCapability(alloc, owned);
        return;
    }
    var key_in_index = false;
    errdefer if (key_in_index) {
        _ = capability_index.remove(key);
        alloc.free(key);
    };
    try capability_index.put(alloc, key, out.items.len);
    key_owned = false;
    key_in_index = true;
    try out.append(alloc, owned);
    owned_in_list = true;
}

fn runtimeCapabilityCanPromoteSchemaDeclaration(
    existing: GeneratedFieldCapability,
    incoming: GeneratedFieldCapability,
) bool {
    return std.mem.eql(u8, existing.doc_value_coverage, "schema_declared") and
        std.mem.eql(u8, existing.queryability_state, "declared") and
        std.mem.eql(u8, incoming.doc_value_coverage, "covered") and
        std.mem.eql(u8, incoming.queryability_state, "queryable") and
        !std.mem.eql(u8, incoming.provenance, "observed_dynamic") and
        generatedFieldCapabilityPromotionSurfaceEqual(existing, incoming);
}

fn generatedFieldCapabilityPromotionSurfaceEqual(
    left: GeneratedFieldCapability,
    right: GeneratedFieldCapability,
) bool {
    return queryModesEqual(left.query_modes, right.query_modes) and
        left.sortable == right.sortable and
        std.mem.eql(u8, left.missing_null_policy, right.missing_null_policy) and
        left.index_sort_position == right.index_sort_position and
        optionalStringEql(left.index_sort_order, right.index_sort_order);
}

fn queryModesEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |left_mode, right_mode| {
        if (!std.mem.eql(u8, left_mode, right_mode)) return false;
    }
    return true;
}

fn generatedFieldCapabilityAggregationKeyEqual(
    left: GeneratedFieldCapability,
    right: GeneratedFieldCapability,
) bool {
    return optionalStringEql(left.field, right.field) and
        optionalStringEql(left.name, right.name) and
        optionalStringEql(left.path_pattern, right.path_pattern) and
        optionalStringEql(left.field_pattern, right.field_pattern) and
        optionalStringEql(left.match_mapping_type, right.match_mapping_type) and
        optionalStringEql(left.emitted_name, right.emitted_name) and
        optionalStringEql(left.document_schema, right.document_schema) and
        left.type == right.type and
        std.mem.eql(u8, left.provenance, right.provenance) and
        optionalStringEql(left.analyzer, right.analyzer);
}

fn generatedFieldCapabilityAggregationKeyAlloc(
    alloc: std.mem.Allocator,
    capability: GeneratedFieldCapability,
) ![]const u8 {
    var key = std.ArrayListUnmanaged(u8).empty;
    errdefer key.deinit(alloc);

    try appendCapabilityKeyPart(alloc, &key, capability.field);
    try appendCapabilityKeyPart(alloc, &key, capability.name);
    try appendCapabilityKeyPart(alloc, &key, capability.path_pattern);
    try appendCapabilityKeyPart(alloc, &key, capability.field_pattern);
    try appendCapabilityKeyPart(alloc, &key, capability.match_mapping_type);
    try appendCapabilityKeyPart(alloc, &key, capability.emitted_name);
    try appendCapabilityKeyPart(alloc, &key, capability.document_schema);
    try appendCapabilityKeyPart(alloc, &key, @tagName(capability.type));
    try appendCapabilityKeyPart(alloc, &key, capability.provenance);
    try appendCapabilityKeyPart(alloc, &key, capability.analyzer);

    return try key.toOwnedSlice(alloc);
}

fn appendCapabilityKeyPart(
    alloc: std.mem.Allocator,
    key: *std.ArrayListUnmanaged(u8),
    value: ?[]const u8,
) !void {
    if (value) |text| {
        var len_buf: [20]u8 = undefined;
        const len_text = std.fmt.bufPrint(&len_buf, "{d}", .{text.len}) catch unreachable;
        try key.appendSlice(alloc, len_text);
        try key.append(alloc, ':');
        try key.appendSlice(alloc, text);
        try key.append(alloc, '|');
    } else {
        try key.appendSlice(alloc, "-:|");
    }
}

fn freeGeneratedFieldCapabilityIndex(
    alloc: std.mem.Allocator,
    capability_index: *GeneratedFieldCapabilityIndex,
) void {
    var it = capability_index.keyIterator();
    while (it.next()) |key| alloc.free(@constCast(key.*));
    capability_index.deinit(alloc);
    capability_index.* = .empty;
}

fn mergeGeneratedFieldCapability(
    alloc: std.mem.Allocator,
    existing: *GeneratedFieldCapability,
    incoming: GeneratedFieldCapability,
) !void {
    try intersectOwnedQueryModes(alloc, &existing.query_modes, incoming.query_modes);
    existing.sortable = existing.sortable and incoming.sortable;
    try replaceOwnedStringIfDifferent(alloc, &existing.doc_value_coverage, runtime_schema_mod.conservativeDocValueCoverage(existing.doc_value_coverage, incoming.doc_value_coverage));
    try replaceOwnedStringIfDifferent(alloc, &existing.queryability_state, runtime_schema_mod.conservativeQueryabilityState(existing.queryability_state, incoming.queryability_state));
    try replaceOwnedStringIfDifferent(alloc, &existing.sort_lifecycle_state, runtime_schema_mod.conservativeSortLifecycleState(existing.sort_lifecycle_state, incoming.sort_lifecycle_state));
    if (!std.mem.eql(u8, existing.missing_null_policy, incoming.missing_null_policy)) {
        try replaceOwnedStringIfDifferent(alloc, &existing.missing_null_policy, "mixed");
    }
    if (existing.index_sort_position != incoming.index_sort_position or
        !optionalStringEql(existing.index_sort_order, incoming.index_sort_order))
    {
        existing.index_sort_position = null;
        if (existing.index_sort_order) |order| alloc.free(@constCast(order));
        existing.index_sort_order = null;
    }
}

fn intersectOwnedQueryModes(
    alloc: std.mem.Allocator,
    target: *[]const []const u8,
    incoming: []const []const u8,
) !void {
    var intersection = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringSliceFromList(alloc, &intersection);
    for (target.*) |mode| {
        if (!stringSliceContains(incoming, mode)) continue;
        try intersection.append(alloc, try alloc.dupe(u8, mode));
    }
    freeOwnedStringSlice(alloc, target.*);
    target.* = if (intersection.items.len == 0) empty: {
        intersection.deinit(alloc);
        break :empty &.{};
    } else try intersection.toOwnedSlice(alloc);
}

fn stringSliceContains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

fn freeOwnedStringSliceFromList(alloc: std.mem.Allocator, values: *std.ArrayListUnmanaged([]const u8)) void {
    for (values.items) |value| alloc.free(@constCast(value));
    values.deinit(alloc);
}

fn generatedFieldCapabilitiesPublicSliceAlloc(
    alloc: std.mem.Allocator,
    capabilities: *std.ArrayListUnmanaged(GeneratedFieldCapability),
) ![]const metadata_openapi.FieldCapability {
    const public = try alloc.alloc(metadata_openapi.FieldCapability, capabilities.items.len);
    errdefer alloc.free(public);
    for (capabilities.items, 0..) |capability, i| {
        public[i] = .{
            .name = capability.name,
            .field = capability.field,
            .path_pattern = capability.path_pattern,
            .field_pattern = capability.field_pattern,
            .match_mapping_type = capability.match_mapping_type,
            .emitted_name = capability.emitted_name,
            .document_schema = capability.document_schema,
            .type = capability.type,
            .query_modes = capability.query_modes,
            .sortable = capability.sortable,
            .provenance = capability.provenance,
            .missing_null_policy = capability.missing_null_policy,
            .sort_lifecycle_state = metadataSortLifecycleState(capability.sort_lifecycle_state),
            .analyzer = capability.analyzer,
            .index_sort_position = capability.index_sort_position,
            .index_sort_order = metadataIndexSortOrder(capability.index_sort_order),
        };
        if (capability.doc_value_coverage.len > 0) alloc.free(@constCast(capability.doc_value_coverage));
        if (capability.queryability_state.len > 0) alloc.free(@constCast(capability.queryability_state));
        if (capability.sort_lifecycle_state.len > 0) alloc.free(@constCast(capability.sort_lifecycle_state));
        if (capability.index_sort_order) |order| alloc.free(@constCast(order));
    }
    capabilities.deinit(alloc);
    return public;
}

fn metadataIndexSortOrder(value: ?[]const u8) ?metadata_openapi.FieldCapabilityIndexSortOrder {
    if (value) |resolved| {
        if (std.mem.eql(u8, resolved, "desc")) return .desc;
        return .asc;
    }
    return null;
}

fn optionalStringEql(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}

fn generatedFieldCapabilityAlloc(
    alloc: std.mem.Allocator,
    capability: runtime_schema_mod.FieldCapability,
) !GeneratedFieldCapability {
    var owned = GeneratedFieldCapability{
        .type = metadataAntflyType(capability.field_type),
        .query_modes = try dupeStringSlice(alloc, queryModesForFieldCapability(capability)),
        .sortable = capability.sortable,
        .doc_value_coverage = "",
        .provenance = "",
        .missing_null_policy = "",
        .queryability_state = "",
        .sort_lifecycle_state = "",
        .index_sort_position = if (capability.index_sort) |membership| @intCast(membership.position) else null,
    };
    errdefer freeGeneratedFieldCapability(alloc, owned);

    owned.name = try dupeOptionalString(alloc, capability.name);
    owned.field = try dupeOptionalString(alloc, capability.field);
    owned.path_pattern = try dupeOptionalString(alloc, capability.path_pattern);
    owned.field_pattern = try dupeOptionalString(alloc, capability.field_pattern);
    owned.match_mapping_type = try dupeOptionalString(alloc, capability.match_mapping_type);
    owned.emitted_name = try dupeOptionalString(alloc, capability.emitted_name);
    owned.document_schema = try dupeOptionalString(alloc, capability.document_schema);
    owned.doc_value_coverage = try alloc.dupe(u8, capability.doc_value_coverage);
    owned.provenance = try alloc.dupe(u8, capability.provenance);
    owned.missing_null_policy = try alloc.dupe(u8, capability.missing_null_policy);
    owned.queryability_state = try alloc.dupe(u8, capability.queryability_state);
    owned.sort_lifecycle_state = try alloc.dupe(u8, capability.sort_lifecycle_state);
    owned.analyzer = try dupeOptionalString(alloc, capability.analyzer);
    owned.index_sort_order = if (capability.index_sort) |membership| try alloc.dupe(u8, if (membership.desc) "desc" else "asc") else null;
    return owned;
}

fn replaceOwnedStringIfDifferent(alloc: std.mem.Allocator, target: *[]const u8, value: []const u8) !void {
    if (std.mem.eql(u8, target.*, value)) return;
    const owned = try alloc.dupe(u8, value);
    alloc.free(@constCast(target.*));
    target.* = owned;
}

fn dupeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(@constCast(value));
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn freeOwnedStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(@constCast(values));
}

fn freeGeneratedFieldCapabilitiesFromList(
    alloc: std.mem.Allocator,
    capabilities: *std.ArrayListUnmanaged(GeneratedFieldCapability),
) void {
    for (capabilities.items) |capability| freeGeneratedFieldCapability(alloc, capability);
    capabilities.deinit(alloc);
}

fn freeGeneratedFieldCapabilities(
    alloc: std.mem.Allocator,
    capabilities: []const metadata_openapi.FieldCapability,
) void {
    for (capabilities) |capability| freePublicGeneratedFieldCapability(alloc, capability);
    if (capabilities.len > 0) alloc.free(@constCast(capabilities));
}

fn freePublicGeneratedFieldCapability(alloc: std.mem.Allocator, capability: metadata_openapi.FieldCapability) void {
    if (capability.name) |value| alloc.free(@constCast(value));
    if (capability.field) |value| alloc.free(@constCast(value));
    if (capability.path_pattern) |value| alloc.free(@constCast(value));
    if (capability.field_pattern) |value| alloc.free(@constCast(value));
    if (capability.match_mapping_type) |value| alloc.free(@constCast(value));
    if (capability.emitted_name) |value| alloc.free(@constCast(value));
    if (capability.document_schema) |value| alloc.free(@constCast(value));
    freeOwnedStringSlice(alloc, capability.query_modes);
    if (capability.provenance.len > 0) alloc.free(@constCast(capability.provenance));
    if (capability.missing_null_policy.len > 0) alloc.free(@constCast(capability.missing_null_policy));
    if (capability.analyzer) |value| alloc.free(@constCast(value));
}

fn freeGeneratedFieldCapability(alloc: std.mem.Allocator, capability: GeneratedFieldCapability) void {
    if (capability.name) |value| alloc.free(@constCast(value));
    if (capability.field) |value| alloc.free(@constCast(value));
    if (capability.path_pattern) |value| alloc.free(@constCast(value));
    if (capability.field_pattern) |value| alloc.free(@constCast(value));
    if (capability.match_mapping_type) |value| alloc.free(@constCast(value));
    if (capability.emitted_name) |value| alloc.free(@constCast(value));
    if (capability.document_schema) |value| alloc.free(@constCast(value));
    freeOwnedStringSlice(alloc, capability.query_modes);
    if (capability.doc_value_coverage.len > 0) alloc.free(@constCast(capability.doc_value_coverage));
    if (capability.provenance.len > 0) alloc.free(@constCast(capability.provenance));
    if (capability.missing_null_policy.len > 0) alloc.free(@constCast(capability.missing_null_policy));
    if (capability.queryability_state.len > 0) alloc.free(@constCast(capability.queryability_state));
    if (capability.sort_lifecycle_state.len > 0) alloc.free(@constCast(capability.sort_lifecycle_state));
    if (capability.analyzer) |value| alloc.free(@constCast(value));
    if (capability.index_sort_order) |value| alloc.free(@constCast(value));
}

fn runtimeFieldCapabilitiesJsonValueAlloc(
    alloc: std.mem.Allocator,
    capabilities: []const runtime_schema_mod.FieldCapability,
) !std.json.Value {
    const public = try alloc.alloc(metadata_openapi.FieldCapability, capabilities.len);
    defer alloc.free(public);
    for (capabilities, 0..) |capability, i| {
        public[i] = metadataFieldCapability(capability);
    }
    const encoded = try std.json.Stringify.valueAlloc(alloc, public, .{ .emit_null_optional_fields = false });
    defer alloc.free(encoded);
    return try parseJsonValueAlloc(alloc, encoded);
}

fn metadataFieldCapability(capability: runtime_schema_mod.FieldCapability) metadata_openapi.FieldCapability {
    return .{
        .name = capability.name,
        .field = capability.field,
        .path_pattern = capability.path_pattern,
        .field_pattern = capability.field_pattern,
        .match_mapping_type = capability.match_mapping_type,
        .emitted_name = capability.emitted_name,
        .document_schema = capability.document_schema,
        .type = metadataAntflyType(capability.field_type),
        .query_modes = queryModesForFieldCapability(capability),
        .sortable = capability.sortable,
        .provenance = capability.provenance,
        .missing_null_policy = capability.missing_null_policy,
        .sort_lifecycle_state = metadataSortLifecycleState(capability.sort_lifecycle_state),
        .analyzer = capability.analyzer,
        .index_sort_position = if (capability.index_sort) |membership| @intCast(membership.position) else null,
        .index_sort_order = if (capability.index_sort) |membership| if (membership.desc) .desc else .asc else null,
    };
}

fn metadataFieldCapabilityAlloc(
    alloc: std.mem.Allocator,
    capability: runtime_schema_mod.FieldCapability,
) !metadata_openapi.FieldCapability {
    var public = metadataFieldCapability(capability);
    public.name = try dupeOptionalString(alloc, public.name);
    public.field = try dupeOptionalString(alloc, public.field);
    public.path_pattern = try dupeOptionalString(alloc, public.path_pattern);
    public.field_pattern = try dupeOptionalString(alloc, public.field_pattern);
    public.match_mapping_type = try dupeOptionalString(alloc, public.match_mapping_type);
    public.emitted_name = try dupeOptionalString(alloc, public.emitted_name);
    public.document_schema = try dupeOptionalString(alloc, public.document_schema);
    public.missing_null_policy = try alloc.dupe(u8, public.missing_null_policy);
    public.analyzer = try dupeOptionalString(alloc, public.analyzer);
    return public;
}

fn dupeOptionalString(alloc: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    return if (value) |resolved| try alloc.dupe(u8, resolved) else null;
}

fn metadataAntflyType(value: runtime_schema_mod.AntflyType) metadata_openapi.AntflyType {
    return switch (value) {
        .text => .text,
        .keyword => .keyword,
        .numeric => .numeric,
        .embedding => .embedding,
        .link => .link,
        .boolean => .boolean,
        .datetime => .datetime,
        .geopoint => .geopoint,
        .geoshape => .geoshape,
        .blob => .blob,
        .html => .html,
        .search_as_you_type => .search_as_you_type,
        .json => .keyword,
        .array => .keyword,
    };
}

fn queryModesForFieldCapability(capability: runtime_schema_mod.FieldCapability) []const []const u8 {
    return switch (capability.field_type) {
        .text, .html => if (capability.searchable) &.{"full_text"} else &.{},
        .search_as_you_type => if (capability.searchable) &.{ "full_text", "autocomplete" } else &.{"autocomplete"},
        .keyword, .link, .boolean, .json, .array => if (capability.filterable) &.{"exact"} else &.{},
        .numeric, .datetime => if (capability.filterable) &.{ "exact", "range" } else &.{},
        .geopoint, .geoshape => if (capability.filterable) &.{"geo"} else &.{},
        .embedding, .blob => &.{},
    };
}

fn metadataSortLifecycleState(value: []const u8) metadata_openapi.FieldCapabilitySortLifecycleState {
    if (std.mem.eql(u8, value, "declared")) return .declared;
    if (std.mem.eql(u8, value, "indexed")) return .indexed;
    if (std.mem.eql(u8, value, "covered")) return .covered;
    if (std.mem.eql(u8, value, "queryable")) return .queryable;
    if (std.mem.eql(u8, value, "accelerated")) return .accelerated;
    return .unsupported;
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
    storage_status: ?TableStorageStatus,
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
        .full_text_index_bindings = try buildFullTextIndexBindings(alloc, table, observedDynamicFieldCapabilitySetsFromStatus(storage_status)),
        .algebraic_capabilities = algebraic_capabilities,
    };
}

fn encodeTableRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const debug = try buildTableRuntimeSchemaDebug(arena_impl.allocator(), table, null);
    return try std.json.Stringify.valueAlloc(alloc, debug, .{ .emit_null_optional_fields = false });
}

fn buildTableIndexRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    observed_sets: []const table_reads.ObservedDynamicFieldCapabilitySet,
) !IndexRuntimeSchemaDebug {
    return .{
        .binding = try buildSingleIndexBinding(alloc, table, index_name, observed_sets),
    };
}

pub fn buildTableIndexRuntimeSchemaDebugValue(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !std.json.Value {
    return try buildTableIndexRuntimeSchemaDebugValueWithObserved(alloc, table, index_name, &.{});
}

pub fn buildTableIndexRuntimeSchemaDebugValueWithObserved(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    observed_sets: []const table_reads.ObservedDynamicFieldCapabilitySet,
) !std.json.Value {
    const debug = try buildTableIndexRuntimeSchemaDebug(alloc, table, index_name, observed_sets);
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
    const debug = try buildTableIndexRuntimeSchemaDebug(arena_impl.allocator(), table, index_name, &.{});
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
    if (binding.observed_dynamic_field_capabilities) |capabilities| {
        try object.put(alloc, try alloc.dupe(u8, "observed_dynamic_field_capabilities"), try cloneJsonValueAlloc(alloc, capabilities));
    }
    return .{ .object = object };
}

fn observedDynamicFieldCapabilitySetsFromStatus(
    storage_status: ?TableStorageStatus,
) []const table_reads.ObservedDynamicFieldCapabilitySet {
    return if (storage_status) |status| status.observed_dynamic_field_capability_sets else &.{};
}

fn observedDynamicFieldCapabilitiesForIndex(
    observed_sets: []const table_reads.ObservedDynamicFieldCapabilitySet,
    index_name: []const u8,
) ?[]const runtime_schema_mod.FieldCapability {
    for (observed_sets) |set| {
        if (std.mem.eql(u8, set.index_name, index_name)) return set.field_capabilities;
    }
    return null;
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
    observed_sets: []const table_reads.ObservedDynamicFieldCapabilitySet,
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
        bindings[index] = try buildSingleIndexBinding(alloc, table, entry.key_ptr.*, observed_sets);
        index += 1;
    }
    return bindings;
}

fn buildSingleIndexBinding(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    observed_sets: []const table_reads.ObservedDynamicFieldCapabilitySet,
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
        .observed_dynamic_field_capabilities = if (observedDynamicFieldCapabilitiesForIndex(observed_sets, index_name)) |capabilities|
            try runtimeFieldCapabilitiesJsonValueAlloc(alloc, capabilities)
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
    try out.appendSlice(alloc, ",\"enforce_types\":");
    try out.appendSlice(alloc, if (schema.enforce_types) "true" else "false");
    try out.appendSlice(alloc, ",\"index_sort\":[");
    for (schema.index_sort, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "field");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.field);
        try out.appendSlice(alloc, ",\"order\":");
        try appendJsonString(alloc, out, if (field.desc) "desc" else "asc");
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "],\"dynamic_templates\":[");
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
        try out.appendSlice(alloc, "],\"infer_type_dynamic_paths\":[");
        for (doc.infer_type_dynamic_paths, 0..) |path, infer_idx| {
            if (infer_idx > 0) try out.append(alloc, ',');
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

pub fn schemaVersion(schema_json: []const u8) !u32 {
    return try sql_schema_mutation.schemaVersion(schema_json);
}

pub fn normalizeSchemaVersion(alloc: std.mem.Allocator, schema_json: []const u8, version: u32) ![]u8 {
    return try sql_schema_mutation.normalizeSchemaVersion(alloc, schema_json, version);
}

pub fn schemasSemanticallyEqual(alloc: std.mem.Allocator, current_schema_json: []const u8, next_schema_json: []const u8) !bool {
    // Version is backend-owned, so derive both schemas at the same synthetic
    // generation and compare their canonical runtime behavior. This treats
    // omitted fields and their explicit defaults as equal, ignores object key
    // order, and still preserves meaningful ordering such as template rules.
    const current_normalized = try normalizeSchemaVersion(alloc, current_schema_json, 0);
    defer alloc.free(current_normalized);
    const next_normalized = try normalizeSchemaVersion(alloc, next_schema_json, 0);
    defer alloc.free(next_normalized);

    const next_runtime_json = try compileRuntimeSchemaJson(alloc, next_normalized);
    defer alloc.free(next_runtime_json);
    const current_runtime_json = compileRuntimeSchemaJson(alloc, current_normalized) catch |err| switch (err) {
        // Let a valid update repair catalog state written by an older schema
        // implementation. Allocation failure remains operational and must not
        // be mistaken for a semantic difference.
        error.OutOfMemory => return err,
        else => return false,
    };
    defer alloc.free(current_runtime_json);

    if (!try sourceSchemasSemanticallyEqual(alloc, current_normalized, next_normalized)) return false;

    var current = try json_helpers.parseJsonValueAlloc(alloc, current_runtime_json);
    defer current.deinit();
    var next = try json_helpers.parseJsonValueAlloc(alloc, next_runtime_json);
    defer next.deinit();

    const current_canonical = try canonicalSchemaJsonAlloc(alloc, current.value, null, .runtime);
    defer alloc.free(current_canonical);
    const next_canonical = try canonicalSchemaJsonAlloc(alloc, next.value, null, .runtime);
    defer alloc.free(next_canonical);
    return std.mem.eql(u8, current_canonical, next_canonical);
}

const CanonicalSchemaKind = enum {
    source,
    runtime,
};

fn sourceSchemasSemanticallyEqual(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    next_schema_json: []const u8,
) !bool {
    var current = try json_helpers.parseJsonValueAlloc(alloc, current_schema_json);
    defer current.deinit();
    var next = try json_helpers.parseJsonValueAlloc(alloc, next_schema_json);
    defer next.deinit();
    normalizeSourceSchemaTopLevelDefaults(&current.value);
    normalizeSourceSchemaTopLevelDefaults(&next.value);

    const current_canonical = try canonicalSchemaJsonAlloc(alloc, current.value, null, .source);
    defer alloc.free(current_canonical);
    const next_canonical = try canonicalSchemaJsonAlloc(alloc, next.value, null, .source);
    defer alloc.free(next_canonical);
    return std.mem.eql(u8, current_canonical, next_canonical);
}

fn normalizeSourceSchemaTopLevelDefaults(value: *std.json.Value) void {
    if (value.* != .object) return;
    const object = &value.object;
    const removable = [_][]const u8{
        "default_type",
        "ttl_duration_ns",
        "ttl_field",
        "enforce_types",
        "document_schemas",
        "dynamic_templates",
        "index_sort",
    };
    for (removable) |name| {
        const field = object.get(name) orelse continue;
        const is_default = if (field == .null)
            true
        else if (std.mem.eql(u8, name, "default_type"))
            field == .string and field.string.len == 0
        else if (std.mem.eql(u8, name, "ttl_duration_ns"))
            field == .integer and field.integer == 0
        else if (std.mem.eql(u8, name, "ttl_field"))
            field == .string and std.mem.eql(u8, field.string, "_timestamp")
        else if (std.mem.eql(u8, name, "enforce_types"))
            field == .bool and !field.bool
        else if (std.mem.eql(u8, name, "document_schemas"))
            field == .object and field.object.count() == 0
        else
            field == .array and field.array.items.len == 0;
        if (is_default) _ = object.orderedRemove(name);
    }
}

fn schemaArrayIsUnordered(kind: CanonicalSchemaKind, field_name: ?[]const u8) bool {
    const name = field_name orelse return false;
    return switch (kind) {
        .source => std.mem.eql(u8, name, "required") or
            std.mem.eql(u8, name, "enum") or
            std.mem.eql(u8, name, "type") or
            std.mem.eql(u8, name, "x-antfly-types") or
            std.mem.eql(u8, name, "x-antfly-include-in-all") or
            std.mem.eql(u8, name, "allOf") or
            std.mem.eql(u8, name, "anyOf") or
            std.mem.eql(u8, name, "oneOf"),
        // Keep dynamic_rules ordered: the document mapper uses first-match
        // precedence, so swapping two overlapping rules changes indexing.
        .runtime => std.mem.eql(u8, name, "field_capabilities") or
            std.mem.eql(u8, name, "full_text_documents") or
            std.mem.eql(u8, name, "fields") or
            std.mem.eql(u8, name, "variants") or
            std.mem.eql(u8, name, "open_dynamic_paths") or
            std.mem.eql(u8, name, "infer_type_dynamic_paths"),
    };
}

fn canonicalSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    field_name: ?[]const u8,
    kind: CanonicalSchemaKind,
) std.mem.Allocator.Error![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendCanonicalSchemaJson(alloc, &out, value, field_name, kind);
    return try out.toOwnedSlice(alloc);
}

fn appendCanonicalSchemaJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
    field_name: ?[]const u8,
    kind: CanonicalSchemaKind,
) std.mem.Allocator.Error!void {
    switch (value) {
        .object => |object| {
            const keys = try alloc.alloc([]const u8, object.count());
            defer alloc.free(keys);
            var key_index: usize = 0;
            var it = object.iterator();
            while (it.next()) |entry| : (key_index += 1) keys[key_index] = entry.key_ptr.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.lessThan);

            try out.append(alloc, '{');
            for (keys, 0..) |key, index| {
                if (index > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendCanonicalSchemaJson(alloc, out, object.get(key).?, key, kind);
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            if (schemaArrayIsUnordered(kind, field_name)) {
                // Canonicalize and sort once instead of performing quadratic
                // pairwise matching for schemas with many derived fields.
                const items = try alloc.alloc([]u8, array.items.len);
                var initialized: usize = 0;
                defer {
                    for (items[0..initialized]) |item| alloc.free(item);
                    alloc.free(items);
                }
                for (array.items) |item| {
                    items[initialized] = try canonicalSchemaJsonAlloc(alloc, item, null, kind);
                    initialized += 1;
                }
                std.mem.sort([]u8, items, {}, struct {
                    fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                        return std.mem.order(u8, lhs, rhs) == .lt;
                    }
                }.lessThan);
                for (items, 0..) |item, index| {
                    if (index > 0) try out.append(alloc, ',');
                    try out.appendSlice(alloc, item);
                }
            } else {
                for (array.items, 0..) |item, index| {
                    if (index > 0) try out.append(alloc, ',');
                    try appendCanonicalSchemaJson(alloc, out, item, null, kind);
                }
            }
            try out.append(alloc, ']');
        },
        else => {
            const encoded = try stringifyJsonValue(alloc, value);
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        },
    }
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
    return metadata_catalog_lookup.findTableByName(snapshot, table_name);
}

pub const QualifiedTableNameParts = metadata_catalog_lookup.QualifiedTableNameParts;

pub fn qualifiedTableNameParts(table_name: []const u8) ?QualifiedTableNameParts {
    return metadata_catalog_lookup.qualifiedTableNameParts(table_name);
}

pub fn findDatabaseByName(snapshot: *const metadata_api.AdminSnapshot, database_name: []const u8) ?*const metadata_table_manager.DatabaseRecord {
    return metadata_catalog_lookup.findDatabaseByName(snapshot, database_name);
}

pub fn findNamespaceByName(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
) ?*const metadata_table_manager.NamespaceRecord {
    return metadata_catalog_lookup.findNamespaceByName(snapshot, database_name, namespace_name);
}

pub fn findTablespaceByName(snapshot: *const metadata_api.AdminSnapshot, tablespace_name: []const u8) ?*const metadata_table_manager.TablespaceRecord {
    return metadata_catalog_lookup.findTablespaceByName(snapshot, tablespace_name);
}

pub fn findSequenceByQualifiedName(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    sequence_name: []const u8,
) ?*const metadata_table_manager.SequenceRecord {
    return metadata_catalog_lookup.findSequenceByQualifiedName(snapshot, database_name, namespace_name, sequence_name);
}

pub fn effectiveTablespaceForTarget(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    explicit_tablespace_name: ?[]const u8,
) ?*const metadata_table_manager.TablespaceRecord {
    return metadata_catalog_lookup.effectiveTablespaceForTarget(snapshot, database_name, namespace_name, explicit_tablespace_name);
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
    return metadata_catalog_lookup.findTableByQualifiedName(snapshot, database_name, namespace_name, table_name);
}

pub fn tableCatalogIdentityMatches(
    record: metadata_table_manager.TableRecord,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) bool {
    return metadata_catalog_lookup.tableCatalogIdentityMatches(record, database_name, namespace_name, table_name);
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

    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try applyRelationalCatalogDdlParsedSqlOnServiceWithSessionAndFunctionBindingsAlloc(alloc, svc, snapshot, &parsed_sql, session, function_bindings);
}

pub fn applyRelationalCatalogDdlParsedSqlOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    parsed_sql: *const sql_adapter.ParsedSql,
) !?AppliedRelationalSqlDdlRecord {
    return try applyRelationalCatalogDdlParsedSqlOnServiceWithSessionAlloc(alloc, svc, snapshot, parsed_sql, catalog_resources.SqlCatalogSession.default());
}

pub fn applyRelationalCatalogDdlParsedSqlOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    parsed_sql: *const sql_adapter.ParsedSql,
    session: catalog_resources.SqlCatalogSession,
) !?AppliedRelationalSqlDdlRecord {
    return try applyRelationalCatalogDdlParsedSqlOnServiceWithSessionAndFunctionBindingsAlloc(alloc, svc, snapshot, parsed_sql, session, .{});
}

pub fn applyRelationalCatalogDdlParsedSqlOnServiceWithSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    parsed_sql: *const sql_adapter.ParsedSql,
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

    var catalog_source = BorrowedAdminSnapshotCatalogSource{ .snapshot = snapshot };
    var durable_plan = sql_adapter.planDurableSqlPlanParsedSqlWithCatalogSessionFunctionBindingsAlloc(
        alloc,
        parsed_sql,
        catalog_source.iface(),
        session,
        function_bindings,
    ) catch |err| switch (err) {
        error.UnsupportedSqlShape => return null,
        else => return err,
    };
    defer durable_plan.deinit(alloc);
    return switch (durable_plan) {
        .catalog_ddl => |catalog_plan| try applyCatalogDdlPlanOnServiceWithSessionAlloc(alloc, svc, snapshot, catalog_plan, session),
        else => null,
    };
}

pub fn applyCatalogDdlPlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: sql_adapter.CatalogDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !?AppliedRelationalSqlDdlRecord {
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    switch (plan) {
        .database_catalog => |database_plan| {
            if (comptime !(@hasDecl(ServiceDeclType, "upsertDatabase") and
                @hasDecl(ServiceDeclType, "removeDatabase") and
                @hasDecl(ServiceDeclType, "upsertNamespace") and
                @hasDecl(ServiceDeclType, "removeNamespace")))
            {
                return null;
            }
            return try applyDatabaseCatalogPlanOnServiceAlloc(alloc, svc, snapshot, database_plan);
        },
        .schema_namespace_catalog => |namespace_plan| {
            if (comptime !(@hasDecl(ServiceDeclType, "upsertDatabase") and
                @hasDecl(ServiceDeclType, "upsertNamespace") and
                @hasDecl(ServiceDeclType, "removeNamespace")))
            {
                return null;
            }
            return try applyNamespaceCatalogPlanOnServiceAlloc(alloc, svc, snapshot, namespace_plan, session);
        },
        .tablespace_catalog => |tablespace_plan| {
            if (comptime !(@hasDecl(ServiceDeclType, "upsertTablespace") and
                @hasDecl(ServiceDeclType, "removeTablespace")))
            {
                return null;
            }
            return try applyTablespaceCatalogPlanOnServiceAlloc(alloc, svc, snapshot, tablespace_plan);
        },
        .sequence_catalog => |sequence_plan| {
            if (comptime !(@hasDecl(ServiceDeclType, "upsertSequence") and
                @hasDecl(ServiceDeclType, "removeSequence")))
            {
                return null;
            }
            return try applySequenceCatalogPlanOnServiceWithSessionAlloc(alloc, svc, snapshot, sequence_plan, session);
        },
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
    switch (plan) {
        .create => |create| {
            const target = try session.namespaceTargetFromSchemaName(create.schema_name);
            const database = findDatabaseByName(snapshot, target.database_name) orelse return error.DatabaseNotFound;
            if (findNamespaceByName(snapshot, target.database_name, target.namespace_name) != null) {
                if (create.if_not_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.NamespaceAlreadyExists;
            }
            try svc.upsertNamespace(.{
                .namespace_id = metadata_table_manager.deriveNamespaceId(database.database_id, target.namespace_name),
                .database_id = database.database_id,
                .name = target.namespace_name,
            });
            applied.created_namespace = true;
        },
        .rename => |rename| {
            const existing_target = try session.namespaceTargetFromSchemaName(rename.schema_name);
            const new_target = try session.namespaceTargetFromSchemaName(rename.new_schema_name);
            if (!std.mem.eql(u8, existing_target.database_name, new_target.database_name)) return error.UnsupportedSqlShape;
            const existing = findNamespaceByName(snapshot, existing_target.database_name, existing_target.namespace_name) orelse return error.NamespaceNotFound;
            if (findNamespaceByName(snapshot, new_target.database_name, new_target.namespace_name) != null) return error.NamespaceAlreadyExists;
            try svc.upsertNamespace(.{
                .namespace_id = metadata_table_manager.deriveNamespaceId(existing.database_id, new_target.namespace_name),
                .database_id = existing.database_id,
                .name = new_target.namespace_name,
            });
            for (snapshot.tables) |table| {
                if (!std.mem.eql(u8, table.database_name, existing_target.database_name)) continue;
                if (!std.mem.eql(u8, table.namespace_name, existing_target.namespace_name)) continue;
                var renamed_table = table;
                renamed_table.namespace_name = new_target.namespace_name;
                try applyTableCatalogUpdateWithoutSchemaRewriteJobsOnService(svc, renamed_table);
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
            if (namespaceHasTables(snapshot, existing.database_id, target.namespace_name)) {
                if (drop.cascade) {
                    try dropNamespaceCatalogObjectsCascadeOnServiceAlloc(alloc, svc, snapshot, target);
                    try svc.removeNamespace(existing.namespace_id);
                    applied.dropped_namespace = true;
                    return applied;
                }
                return error.NamespaceNotEmpty;
            }
            try svc.removeNamespace(existing.namespace_id);
            applied.dropped_namespace = true;
        },
    }
    return applied;
}

fn dropNamespaceCatalogObjectsCascadeOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    target: catalog_resources.NamespaceTarget,
) !void {
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (comptime !(@hasDecl(ServiceDeclType, "applyTableCatalogBatchUpdateWithSchemaRewriteJobs") and
        @hasDecl(ServiceDeclType, "applyTableCatalogDropWithSchemaRewriteJobs") and
        @hasDecl(ServiceDeclType, "removeSequence")))
    {
        return error.UnsupportedOperation;
    }

    var namespace_table_ids = std.AutoHashMapUnmanaged(u64, void).empty;
    defer namespace_table_ids.deinit(alloc);
    var namespace_table_names = std.ArrayListUnmanaged([]const u8).empty;
    defer namespace_table_names.deinit(alloc);

    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, target.database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, target.namespace_name)) continue;
        try namespace_table_ids.put(alloc, table.table_id, {});
        try namespace_table_names.append(alloc, table.name);
    }

    if (namespace_table_names.items.len > 0) {
        var table_updates = std.ArrayListUnmanaged(metadata_table_manager.TableRecord).empty;
        defer {
            for (table_updates.items) |table| metadata_table_manager.freeTable(alloc, table);
            table_updates.deinit(alloc);
        }

        for (snapshot.tables) |candidate_table| {
            if (namespace_table_ids.contains(candidate_table.table_id)) continue;
            if (candidate_table.schema_json.len == 0) continue;
            var current_schema_json = try alloc.dupe(u8, candidate_table.schema_json);
            defer alloc.free(current_schema_json);
            var changed = false;

            for (namespace_table_names.items) |table_name| {
                const next_schema_json = (try schemaWithoutForeignKeysReferencingTableAlloc(
                    alloc,
                    current_schema_json,
                    table_name,
                )) orelse continue;
                alloc.free(current_schema_json);
                current_schema_json = next_schema_json;
                changed = true;
            }

            if (!changed) continue;
            const updated = try applySchemaUpdateRecord(alloc, &candidate_table, current_schema_json);
            errdefer metadata_table_manager.freeTable(alloc, updated);
            try table_updates.append(alloc, updated);
        }

        if (table_updates.items.len > 0) {
            try svc.applyTableCatalogBatchUpdateWithSchemaRewriteJobs(.{
                .tables = table_updates.items,
            });
        }

        for (snapshot.tables) |table| {
            if (!namespace_table_ids.contains(table.table_id)) continue;
            var range_group_ids = std.ArrayListUnmanaged(u64).empty;
            defer range_group_ids.deinit(alloc);
            for (snapshot.ranges) |range| {
                if (range.table_id != table.table_id) continue;
                try range_group_ids.append(alloc, range.group_id);
            }
            const sequence_ids = try ownedSequenceIdsForTableAlloc(alloc, snapshot, table);
            defer alloc.free(sequence_ids);
            try svc.applyTableCatalogDropWithSchemaRewriteJobs(.{
                .table_id = table.table_id,
                .sequence_ids = sequence_ids,
                .range_group_ids = range_group_ids.items,
            });
        }
    }

    for (snapshot.sequences) |sequence| {
        if (!std.mem.eql(u8, sequence.database_name, target.database_name)) continue;
        if (!std.mem.eql(u8, sequence.namespace_name, target.namespace_name)) continue;
        if (try sequenceOwnedByNamespaceTableAlloc(alloc, snapshot, target, sequence)) continue;
        try svc.removeSequence(sequence.sequence_id);
    }
}

fn sequenceOwnedByNamespaceTableAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    target: catalog_resources.NamespaceTarget,
    sequence: metadata_table_manager.SequenceRecord,
) !bool {
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, target.database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, target.namespace_name)) continue;
        if (try sequenceOwnedByTableColumnAlloc(alloc, sequence, table)) return true;
    }
    return false;
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

pub fn applySequenceCatalogPlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    plan: sql_adapter.SequenceCatalogPlan,
    session: catalog_resources.SqlCatalogSession,
) !AppliedRelationalSqlDdlRecord {
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (comptime !(@hasDecl(ServiceDeclType, "upsertSequence") and @hasDecl(ServiceDeclType, "removeSequence"))) {
        return error.UnsupportedSqlShape;
    }

    var applied = try emptyAppliedRelationalSqlDdlRecordAlloc(alloc);
    errdefer applied.deinit(alloc);
    switch (plan) {
        .create => |create| {
            const target = try session.tableTargetFromObjectName(create.sequence_name);
            const existing = findSequenceByQualifiedName(snapshot, target.database_name, target.namespace_name, target.table_name);
            if (existing != null) {
                if (create.if_not_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.SequenceAlreadyExists;
            }
            if (findNamespaceByName(snapshot, target.database_name, target.namespace_name) == null) return error.NamespaceNotFound;
            try validateSequenceOwnedByOptionsAlloc(alloc, snapshot, target, create.options);
            const options_json = try sequenceOptionsJsonFromCreateAlloc(alloc, create.options);
            defer alloc.free(options_json);
            const last_value = try metadata_table_manager.sequenceInitialLastValueFromOptionsJson(alloc, options_json);
            try svc.upsertSequence(.{
                .sequence_id = metadata_table_manager.deriveSequenceId(target.database_name, target.namespace_name, target.table_name),
                .name = target.table_name,
                .database_name = target.database_name,
                .namespace_name = target.namespace_name,
                .options_json = options_json,
                .last_value = last_value,
            });
            applied.created_sequence = true;
        },
        .alter => |alter| {
            const target = try session.tableTargetFromObjectName(alter.sequence_name);
            const existing = findSequenceByQualifiedName(snapshot, target.database_name, target.namespace_name, target.table_name) orelse {
                if (alter.if_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.SequenceNotFound;
            };
            try validateSequenceOwnedByAlterOperationsAlloc(alloc, snapshot, target, alter.operations);
            const options_json = try sequenceOptionsJsonAfterAlterAlloc(alloc, existing.options_json, alter.operations);
            defer alloc.free(options_json);
            const last_value = try sequenceLastValueAfterAlterAlloc(alloc, existing.last_value, options_json, alter.operations);
            try svc.upsertSequence(.{
                .sequence_id = existing.sequence_id,
                .name = existing.name,
                .database_name = existing.database_name,
                .namespace_name = existing.namespace_name,
                .options_json = options_json,
                .last_value = last_value,
            });
            applied.altered_sequence = true;
        },
        .drop => |drop| {
            const target = try session.tableTargetFromObjectName(drop.sequence_name);
            const existing = findSequenceByQualifiedName(snapshot, target.database_name, target.namespace_name, target.table_name) orelse {
                if (drop.if_exists) {
                    applied.noop = true;
                    return applied;
                }
                return error.SequenceNotFound;
            };
            _ = drop.cascade;
            try svc.removeSequence(existing.sequence_id);
            applied.dropped_sequence = true;
        },
    }
    return applied;
}

fn validateSequenceOwnedByOptionsAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    sequence_target: catalog_resources.TableTarget,
    options: sql_adapter.SequenceOptions,
) !void {
    if (options.owned_by) |owned_by| try validateSequenceOwnedByAlloc(alloc, snapshot, sequence_target, owned_by);
}

fn validateSequenceOwnedByAlterOperationsAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    sequence_target: catalog_resources.TableTarget,
    operations: []const sql_adapter.SequenceAlterOperation,
) !void {
    for (operations) |operation| {
        switch (operation) {
            .set_owned_by => |owned_by| try validateSequenceOwnedByAlloc(alloc, snapshot, sequence_target, owned_by),
            else => {},
        }
    }
}

fn validateSequenceOwnedByAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    sequence_target: catalog_resources.TableTarget,
    owned_by: sql_adapter.SequenceOwnedBy,
) !void {
    if (owned_by.table_name.len == 0 and owned_by.column_name.len == 0) return;
    if (owned_by.table_name.len == 0 or owned_by.column_name.len == 0) return error.InvalidSequenceCatalog;

    const table = findTableByQualifiedName(snapshot, sequence_target.database_name, sequence_target.namespace_name, owned_by.table_name) orelse return error.TableNotFound;
    var parsed = try parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    if (runtime.storage_mode != .relational) return error.InvalidSequenceCatalog;
    if (findRelationalColumn(runtime.relational_columns, owned_by.column_name) == null) return error.InvalidSqlCatalog;
}

fn sequenceLastValueAfterAlterAlloc(
    alloc: std.mem.Allocator,
    existing_last_value: i64,
    options_json: []const u8,
    operations: []const sql_adapter.SequenceAlterOperation,
) !i64 {
    for (operations) |operation| switch (operation) {
        .restart => return try metadata_table_manager.sequenceInitialLastValueFromOptionsJson(alloc, options_json),
        else => {},
    };
    return existing_last_value;
}

fn sequenceOptionsJsonFromCreateAlloc(alloc: std.mem.Allocator, options: sql_adapter.SequenceOptions) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var object = std.json.ObjectMap.empty;
    try putSequenceOptionsJson(arena, &object, options);
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = object }, .{});
}

fn sequenceOptionsJsonAfterAlterAlloc(
    alloc: std.mem.Allocator,
    options_json: []const u8,
    operations: []const sql_adapter.SequenceAlterOperation,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, options_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSequenceCatalog;
    const arena = parsed.arena.allocator();
    for (operations) |operation| {
        switch (operation) {
            .set_type => |type_name| try putJsonString(arena, &parsed.value.object, "as_type", type_name),
            .restart => |value| try putJsonOptionalInteger(arena, &parsed.value.object, "restart_with", value),
            .set_start => |value| try putJsonInteger(arena, &parsed.value.object, "start_with", value),
            .set_increment => |value| try putJsonInteger(arena, &parsed.value.object, "increment_by", value),
            .set_min => |value| try putJsonOptionalInteger(arena, &parsed.value.object, "min_value", value),
            .set_max => |value| try putJsonOptionalInteger(arena, &parsed.value.object, "max_value", value),
            .set_cache => |value| try putJsonInteger(arena, &parsed.value.object, "cache", value),
            .set_cycle => |value| try putJsonBool(arena, &parsed.value.object, "cycle", value),
            .set_owned_by => |owned_by| try putSequenceOwnedByJson(arena, &parsed.value.object, owned_by),
        }
    }
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn putSequenceOptionsJson(arena: std.mem.Allocator, object: *std.json.ObjectMap, options: sql_adapter.SequenceOptions) !void {
    if (options.as_type) |value| try putJsonString(arena, object, "as_type", value);
    if (options.start_with) |value| try putJsonInteger(arena, object, "start_with", value);
    if (options.increment_by) |value| try putJsonInteger(arena, object, "increment_by", value);
    if (options.min_value_specified) try putJsonOptionalInteger(arena, object, "min_value", options.min_value);
    if (options.max_value_specified) try putJsonOptionalInteger(arena, object, "max_value", options.max_value);
    if (options.cache) |value| try putJsonInteger(arena, object, "cache", value);
    if (options.cycle) |value| try putJsonBool(arena, object, "cycle", value);
    if (options.owned_by) |owned_by| try putSequenceOwnedByJson(arena, object, owned_by);
}

fn putSequenceOwnedByJson(arena: std.mem.Allocator, object: *std.json.ObjectMap, owned_by: sql_adapter.SequenceOwnedBy) !void {
    if (owned_by.table_name.len == 0 and owned_by.column_name.len == 0) {
        try object.put(arena, try arena.dupe(u8, "owned_by"), .null);
        return;
    }
    var owned = std.json.ObjectMap.empty;
    try putJsonString(arena, &owned, "table_name", owned_by.table_name);
    try putJsonString(arena, &owned, "column_name", owned_by.column_name);
    try object.put(arena, try arena.dupe(u8, "owned_by"), .{ .object = owned });
}

fn putJsonString(arena: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try object.put(arena, try arena.dupe(u8, key), .{ .string = try arena.dupe(u8, value) });
}

fn putJsonInteger(arena: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: i64) !void {
    try object.put(arena, try arena.dupe(u8, key), .{ .integer = value });
}

fn putJsonOptionalInteger(arena: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: ?i64) !void {
    if (value) |number| {
        try putJsonInteger(arena, object, key, number);
    } else {
        try object.put(arena, try arena.dupe(u8, key), .null);
    }
}

fn putJsonBool(arena: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: bool) !void {
    try object.put(arena, try arena.dupe(u8, key), .{ .bool = value });
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
        try applyTableCatalogUpdateWithoutSchemaRewriteJobsOnService(svc, updated);
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
    for (snapshot.sequences) |sequence| {
        if (metadata_table_manager.deriveDatabaseId(sequence.database_name) == database_id) return true;
    }
    return false;
}

fn namespaceHasTables(snapshot: *const metadata_api.AdminSnapshot, database_id: u64, namespace_name: []const u8) bool {
    for (snapshot.tables) |table| {
        if (metadata_table_manager.deriveDatabaseId(table.database_name) != database_id) continue;
        if (std.mem.eql(u8, table.namespace_name, namespace_name)) return true;
    }
    for (snapshot.sequences) |sequence| {
        if (metadata_table_manager.deriveDatabaseId(sequence.database_name) != database_id) continue;
        if (std.mem.eql(u8, sequence.namespace_name, namespace_name)) return true;
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
    try validateRelationalTableOwnedSequenceDropAllowed(alloc, snapshot, target_table);
}

pub fn validateRelationalTableOwnedSequenceDropAllowed(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    target_table: metadata_table_manager.TableRecord,
) !void {
    for (snapshot.sequences) |sequence| {
        if (try sequenceOwnedByTableColumnAlloc(alloc, sequence, target_table)) return error.TableReferencedBySequence;
    }
}

pub fn ownedSequenceIdsForTableAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    target_table: metadata_table_manager.TableRecord,
) ![]u64 {
    var out = std.ArrayListUnmanaged(u64).empty;
    errdefer out.deinit(alloc);
    for (snapshot.sequences) |sequence| {
        if (try sequenceOwnedByTableColumnAlloc(alloc, sequence, target_table)) {
            try out.append(alloc, sequence.sequence_id);
        }
    }
    return try out.toOwnedSlice(alloc);
}

pub fn removeOwnedSequencesForTableOnServiceAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    target_table: metadata_table_manager.TableRecord,
) !usize {
    const owned_sequence_ids = try ownedSequenceIdsForTableAlloc(alloc, snapshot, target_table);
    defer alloc.free(owned_sequence_ids);
    if (owned_sequence_ids.len == 0) return 0;

    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (comptime !@hasDecl(ServiceDeclType, "removeSequence")) return error.UnsupportedOperation;

    for (owned_sequence_ids) |sequence_id| try svc.removeSequence(sequence_id);
    return owned_sequence_ids.len;
}

fn sequenceOwnedByTableColumnAlloc(
    alloc: std.mem.Allocator,
    sequence: metadata_table_manager.SequenceRecord,
    target_table: metadata_table_manager.TableRecord,
) !bool {
    if (!std.mem.eql(u8, sequence.database_name, target_table.database_name)) return false;
    if (!std.mem.eql(u8, sequence.namespace_name, target_table.namespace_name)) return false;

    var parsed_options = try std.json.parseFromSlice(std.json.Value, alloc, sequence.options_json, .{});
    defer parsed_options.deinit();
    if (parsed_options.value != .object) return error.InvalidSequenceCatalog;
    const owned_by = parsed_options.value.object.get("owned_by") orelse return false;
    if (owned_by == .null) return false;
    if (owned_by != .object) return error.InvalidSequenceCatalog;
    const table_name = owned_by.object.get("table_name") orelse return error.InvalidSequenceCatalog;
    const column_name = owned_by.object.get("column_name") orelse return error.InvalidSequenceCatalog;
    if (table_name != .string or column_name != .string) return error.InvalidSequenceCatalog;
    if (table_name.string.len == 0 or column_name.string.len == 0) return error.InvalidSequenceCatalog;
    if (!std.mem.eql(u8, table_name.string, target_table.name)) return false;

    var parsed_table_schema = try parseValidatedTableSchema(alloc, target_table.schema_json);
    defer parsed_table_schema.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed_table_schema);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    if (runtime.storage_mode != .relational) return error.InvalidSequenceCatalog;
    if (findRelationalColumn(runtime.relational_columns, column_name.string) == null) return error.InvalidSequenceCatalog;
    return true;
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
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"disk_usage\"") == null);
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
        .disk_usage = 99,
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
            .wal_checkpoint_pending = true,
            .wal_pressure_blocked = true,
            .wal_checkpoint_retry_reason = "checkpoint_failure",
            .wal_checkpoint_retry_attempts = 3,
            .wal_checkpoint_retry_delay_ns = 250,
            .active_immutable_logical_bytes = 56,
            .unpublished_wal_logical_bytes = 57,
            .unpublished_wal_max_batch_logical_bytes = 58,
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
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"storage_status\":{\"disk_usage\":99,\"empty\":true,\"lsm\":{") != null);
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
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_pressure_blocked\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_retry_reason\":\"checkpoint_failure\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_retry_attempts\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_retry_delay_ns\":250") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_immutable_logical_bytes\":56") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"unpublished_wal_logical_bytes\":57") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"unpublished_wal_max_batch_logical_bytes\":58") != null);
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

pub fn testRuntimeSchemaDebugEmitsObservedDynamicCapabilities() !void {
    const schema_json =
        \\{"version":1,"default_type":"doc","dynamic_templates":[{"name":"status","path_match":"meta.status","mapping":{"type":"keyword"}}],"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .schema_json = schema_json,
            .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
            .replication_sources_json = "[]",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };
    var observed_capabilities = [_]runtime_schema_mod.FieldCapability{
        runtime_schema_mod.observedDynamicFieldCapability(null, "meta.status", .{
            .field_type = .keyword,
            .do_index = true,
            .store = true,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        }),
    };
    var observed_sets = [_]table_reads.ObservedDynamicFieldCapabilitySet{.{
        .index_name = @constCast("full_text_index_v1"),
        .field_capabilities = observed_capabilities[0..],
    }};
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = false,
        .observed_dynamic_field_capability_sets = observed_sets[0..],
    }};

    const encoded = (try encodeSingleTableStatusWithRuntimeSchemaDebug(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);

    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_name\":\"full_text_index_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"observed_dynamic_field_capabilities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"field\":\"meta.status\",\"type\":\"keyword\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_modes\":[\"exact\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"provenance\":\"observed_dynamic\"") != null);
}

pub fn testRuntimeSchemaDebugEmitsSortCapabilities() !void {
    const schema_json =
        \\{"version":1,"default_type":"doc","dynamic_templates":[{"name":"dates","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}],"index_sort":[{"field":"created_at","order":"desc"}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"string","x-antfly-types":["text"],"x-antfly-analyzer":"french"}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .schema_json = schema_json,
            .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
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

    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"field_capabilities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"field\":\"_id\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"field\":\"created_at\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"path_pattern\":\"created_at\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_modes\":[\"exact\",\"range\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"provenance\":\"dynamic_template\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"sort_lifecycle_state\":\"declared\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_sort_position\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_sort_order\":\"desc\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"type\":\"keyword\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_sort_position\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_sort_order\":\"asc\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"field\":\"title\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"emitted_name\":\"title\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"document_schema\":\"doc\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_modes\":[\"full_text\"]") != null);
}

fn testStringSliceContains(values: []const []const u8, expected: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, expected)) return true;
    }
    return false;
}

fn testJsonArrayContainsString(value: std.json.Value, expected: []const u8) bool {
    if (value != .array) return false;
    for (value.array.items) |item| {
        if (item == .string and std.mem.eql(u8, item.string, expected)) return true;
    }
    return false;
}

fn testFieldCapabilityByIdentifier(root: std.json.Value, identifier: []const u8) ?std.json.Value {
    const capabilities = root.object.get("field_capabilities") orelse return null;
    if (capabilities != .array) return null;
    for (capabilities.array.items) |capability| {
        if (capability != .object) continue;
        const object = capability.object;
        if (object.get("name")) |name| {
            if (name == .string and std.mem.eql(u8, name.string, identifier)) return capability;
        }
        if (object.get("field")) |field| {
            if (field == .string and std.mem.eql(u8, field.string, identifier)) return capability;
        }
    }
    return null;
}

fn testGeneratedCapabilityByIdentifier(
    capabilities: []const metadata_openapi.FieldCapability,
    identifier: []const u8,
) ?metadata_openapi.FieldCapability {
    for (capabilities) |capability| {
        if (capability.name) |name| {
            if (std.mem.eql(u8, name, identifier)) return capability;
        }
        if (capability.field) |field| {
            if (std.mem.eql(u8, field, identifier)) return capability;
        }
    }
    return null;
}

test "metadata.table generated field capabilities include schema dynamic templates" {
    const schema_json =
        \\{"version":1,"default_type":"doc","dynamic_templates":[{"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}],"index_sort":[{"field":"created_at","order":"desc"}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"string","x-antfly-types":["text"]}}}}}}
    ;
    const table = metadata_table_manager.TableRecord{
        .table_id = 7,
        .name = "docs",
        .schema_json = schema_json,
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const capabilities = (try generatedFieldCapabilitiesAlloc(std.testing.allocator, &table, null)) orelse return error.TestUnexpectedResult;
    defer freeGeneratedFieldCapabilities(std.testing.allocator, capabilities);

    const created = testGeneratedCapabilityByIdentifier(capabilities, "created") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", created.path_pattern.?);
    try std.testing.expectEqual(metadata_openapi.AntflyType.datetime, created.type);
    try std.testing.expect(testStringSliceContains(created.query_modes, "exact"));
    try std.testing.expect(testStringSliceContains(created.query_modes, "range"));
    try std.testing.expect(created.sortable);
    try std.testing.expectEqualStrings("missing_rejected", created.missing_null_policy);
    try std.testing.expectEqualStrings("dynamic_template", created.provenance);
    try std.testing.expectEqual(@as(?i64, 0), created.index_sort_position);
    try std.testing.expectEqual(metadata_openapi.FieldCapabilityIndexSortOrder.desc, created.index_sort_order.?);
}

test "metadata.table status exposes stable field capabilities" {
    const schema_json =
        \\{"version":1,"default_type":"doc","dynamic_templates":[{"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}],"index_sort":[{"field":"created_at","order":"desc"}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"string","x-antfly-types":["text"]}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = schema_json, .indexes_json = "{}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    const id_capability = testFieldCapabilityByIdentifier(parsed.value, "_id") orelse return error.TestUnexpectedResult;
    try std.testing.expect(id_capability.object.get("sortable").?.bool);

    const created = testFieldCapabilityByIdentifier(parsed.value, "created") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", created.object.get("path_pattern").?.string);
    try std.testing.expectEqualStrings("datetime", created.object.get("type").?.string);
    try std.testing.expect(testJsonArrayContainsString(created.object.get("query_modes").?, "exact"));
    try std.testing.expect(testJsonArrayContainsString(created.object.get("query_modes").?, "range"));
    try std.testing.expect(created.object.get("sortable").?.bool);
    try std.testing.expectEqualStrings("missing_rejected", created.object.get("missing_null_policy").?.string);
    try std.testing.expectEqualStrings("dynamic_template", created.object.get("provenance").?.string);
    try std.testing.expectEqual(@as(i64, 0), created.object.get("index_sort_position").?.integer);
    try std.testing.expectEqualStrings("desc", created.object.get("index_sort_order").?.string);

    try std.testing.expectEqualStrings("not_null", id_capability.object.get("missing_null_policy").?.string);
    try std.testing.expect(testJsonArrayContainsString(id_capability.object.get("query_modes").?, "exact"));
}

test "metadata.table status includes observed dynamic field capabilities" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = "{}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    var observed_capabilities = [_]runtime_schema_mod.FieldCapability{
        runtime_schema_mod.observedDynamicFieldCapability(null, "meta.status", .{
            .field_type = .keyword,
            .do_index = true,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        }),
    };
    var observed_sets = [_]table_reads.ObservedDynamicFieldCapabilitySet{.{
        .index_name = @constCast("full_text_index_v0"),
        .field_capabilities = observed_capabilities[0..],
    }};
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = false,
        .observed_dynamic_field_capability_sets = observed_sets[0..],
    }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"field\":\"meta.status\",\"type\":\"keyword\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_modes\":[\"exact\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"provenance\":\"observed_dynamic\"") != null);
}

test "metadata.table status promotes schema capability when runtime coverage is complete" {
    const schema_json =
        \\{"version":1,"default_type":"doc","dynamic_templates":[{"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"created_at":{"type":"string","format":"date-time"}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = schema_json, .indexes_json = "{}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const template = runtime_schema_mod.DynamicTemplate{
        .name = "created",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "standard",
        },
    };
    const runtime_schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &.{template} };
    var covered = runtime_schema_mod.dynamicTemplateFieldCapability(runtime_schema, template);
    covered.doc_value_coverage = "covered";
    covered.queryability_state = "queryable";
    runtime_schema_mod.refreshSortLifecycleState(&covered);
    var runtime_capabilities = [_]runtime_schema_mod.FieldCapability{covered};
    var observed_sets = [_]table_reads.ObservedDynamicFieldCapabilitySet{.{
        .index_name = @constCast("full_text_index_v0"),
        .field_capabilities = runtime_capabilities[0..],
    }};
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = false,
        .observed_dynamic_field_capability_sets = observed_sets[0..],
    }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const created = testFieldCapabilityByIdentifier(parsed.value, "created") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("datetime", created.object.get("type").?.string);
    try std.testing.expect(testJsonArrayContainsString(created.object.get("query_modes").?, "exact"));
    try std.testing.expect(testJsonArrayContainsString(created.object.get("query_modes").?, "range"));
    try std.testing.expectEqualStrings("dynamic_template", created.object.get("provenance").?.string);
    try std.testing.expectEqualStrings("queryable", created.object.get("sort_lifecycle_state").?.string);
}

test "metadata.table status promotes schema geo capability when runtime coverage is complete" {
    const schema_json =
        \\{"version":1,"default_type":"doc","dynamic_templates":[{"name":"location","path_match":"location","mapping":{"type":"geopoint","index":true}}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"location":{"type":"object","x-antfly-field":{"type":"geopoint"}}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = schema_json, .indexes_json = "{}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const template = runtime_schema_mod.DynamicTemplate{
        .name = "location",
        .path_match = "location",
        .mapping = .{
            .field_type = .geopoint,
            .do_index = true,
            .doc_values = true,
            .sortable = false,
            .analyzer = "standard",
        },
    };
    const runtime_schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &.{template} };
    var covered = runtime_schema_mod.dynamicTemplateFieldCapability(runtime_schema, template);
    covered.doc_value_coverage = "covered";
    covered.queryability_state = "queryable";
    runtime_schema_mod.refreshSortLifecycleState(&covered);
    var runtime_capabilities = [_]runtime_schema_mod.FieldCapability{covered};
    var observed_sets = [_]table_reads.ObservedDynamicFieldCapabilitySet{.{
        .index_name = @constCast("full_text_index_v0"),
        .field_capabilities = runtime_capabilities[0..],
    }};
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = false,
        .observed_dynamic_field_capability_sets = observed_sets[0..],
    }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const location = testFieldCapabilityByIdentifier(parsed.value, "location") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("geopoint", location.object.get("type").?.string);
    try std.testing.expect(testJsonArrayContainsString(location.object.get("query_modes").?, "geo"));
    try std.testing.expect(!location.object.get("sortable").?.bool);
    try std.testing.expectEqualStrings("dynamic_template", location.object.get("provenance").?.string);
    try std.testing.expectEqualStrings("unsupported", location.object.get("sort_lifecycle_state").?.string);
}

test "metadata.table status does not promote mismatched index sort runtime capability" {
    const schema_json =
        \\{"version":1,"default_type":"doc","dynamic_templates":[{"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}],"index_sort":[{"field":"created_at","order":"desc"}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"created_at":{"type":"string","format":"date-time"}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = schema_json, .indexes_json = "{}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const template = runtime_schema_mod.DynamicTemplate{
        .name = "created",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "standard",
        },
    };
    const runtime_schema_without_index_sort = runtime_schema_mod.TableSchema{ .dynamic_templates = &.{template} };
    var covered_without_index_sort = runtime_schema_mod.dynamicTemplateFieldCapability(runtime_schema_without_index_sort, template);
    covered_without_index_sort.doc_value_coverage = "covered";
    covered_without_index_sort.queryability_state = "queryable";
    runtime_schema_mod.refreshSortLifecycleState(&covered_without_index_sort);
    var runtime_capabilities = [_]runtime_schema_mod.FieldCapability{covered_without_index_sort};
    var observed_sets = [_]table_reads.ObservedDynamicFieldCapabilitySet{.{
        .index_name = @constCast("full_text_index_v0"),
        .field_capabilities = runtime_capabilities[0..],
    }};
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = false,
        .observed_dynamic_field_capability_sets = observed_sets[0..],
    }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const created = testFieldCapabilityByIdentifier(parsed.value, "created") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("datetime", created.object.get("type").?.string);
    try std.testing.expectEqualStrings("declared", created.object.get("sort_lifecycle_state").?.string);
    try std.testing.expect(created.object.get("index_sort_position") == null);
    try std.testing.expect(created.object.get("index_sort_order") == null);
}

test "metadata.table status does not advertise changed index sort direction before rebuild" {
    const schema_json =
        \\{"version":2,"default_type":"doc","dynamic_templates":[{"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}],"index_sort":[{"field":"created_at","order":"asc"}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"created_at":{"type":"string","format":"date-time"}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = schema_json, .indexes_json = "{}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const template = runtime_schema_mod.DynamicTemplate{
        .name = "created",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "standard",
        },
    };
    const old_index_sort = [_]runtime_schema_mod.IndexSortField{.{ .field = "created_at", .desc = true }};
    const old_runtime_schema = runtime_schema_mod.TableSchema{
        .dynamic_templates = &.{template},
        .index_sort = &old_index_sort,
    };
    var old_covered = runtime_schema_mod.dynamicTemplateFieldCapability(old_runtime_schema, template);
    old_covered.doc_value_coverage = "covered";
    old_covered.queryability_state = "queryable";
    runtime_schema_mod.refreshSortLifecycleState(&old_covered);
    try std.testing.expectEqualStrings("accelerated", old_covered.sort_lifecycle_state);
    var runtime_capabilities = [_]runtime_schema_mod.FieldCapability{old_covered};
    var observed_sets = [_]table_reads.ObservedDynamicFieldCapabilitySet{.{
        .index_name = @constCast("full_text_index_v0"),
        .field_capabilities = runtime_capabilities[0..],
    }};
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = false,
        .observed_dynamic_field_capability_sets = observed_sets[0..],
    }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const created = testFieldCapabilityByIdentifier(parsed.value, "created") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("datetime", created.object.get("type").?.string);
    try std.testing.expect(testJsonArrayContainsString(created.object.get("query_modes").?, "exact"));
    try std.testing.expect(testJsonArrayContainsString(created.object.get("query_modes").?, "range"));
    try std.testing.expect(created.object.get("sortable").?.bool);
    try std.testing.expectEqualStrings("declared", created.object.get("sort_lifecycle_state").?.string);
    try std.testing.expect(created.object.get("index_sort_position") == null);
    try std.testing.expect(created.object.get("index_sort_order") == null);
}

test "metadata.table status merges query modes conservatively" {
    var queryable = runtime_schema_mod.observedDynamicFieldCapability(null, "price", .{
        .field_type = .numeric,
        .do_index = true,
        .doc_values = true,
        .sortable = true,
    });
    queryable.doc_value_coverage = "covered";
    queryable.queryability_state = "queryable";
    runtime_schema_mod.refreshSortLifecycleState(&queryable);
    const unqueryable = runtime_schema_mod.observedDynamicFieldCapability(null, "price", .{
        .field_type = .numeric,
        .do_index = false,
        .doc_values = true,
        .sortable = false,
    });

    var merged = try generatedFieldCapabilityAlloc(std.testing.allocator, queryable);
    defer freeGeneratedFieldCapability(std.testing.allocator, merged);
    var incoming = try generatedFieldCapabilityAlloc(std.testing.allocator, unqueryable);
    defer freeGeneratedFieldCapability(std.testing.allocator, incoming);
    freeOwnedStringSlice(std.testing.allocator, incoming.query_modes);
    incoming.query_modes = &.{};

    try std.testing.expect(testStringSliceContains(merged.query_modes, "exact"));
    try std.testing.expect(testStringSliceContains(merged.query_modes, "range"));
    try mergeGeneratedFieldCapability(std.testing.allocator, &merged, incoming);
    try std.testing.expectEqual(@as(usize, 0), merged.query_modes.len);
    try std.testing.expect(!merged.sortable);
}

test "metadata.table status merges observed capabilities conservatively" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = "{}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    var covered = runtime_schema_mod.observedDynamicFieldCapability(null, "price", .{
        .field_type = .numeric,
        .do_index = true,
        .doc_values = true,
        .sortable = true,
    });
    covered.doc_value_coverage = "covered";
    covered.queryability_state = "queryable";
    runtime_schema_mod.refreshSortLifecycleState(&covered);
    const declared = runtime_schema_mod.observedDynamicFieldCapability(null, "price", .{
        .field_type = .numeric,
        .do_index = true,
        .doc_values = true,
        .sortable = true,
    });
    var observed_capabilities = [_]runtime_schema_mod.FieldCapability{ covered, declared };
    var observed_sets = [_]table_reads.ObservedDynamicFieldCapabilitySet{.{
        .index_name = @constCast("full_text_index_v0"),
        .field_capabilities = observed_capabilities[0..],
    }};
    const storage_statuses = [_]TableStorageStatus{.{
        .table_name = "docs",
        .empty = false,
        .observed_dynamic_field_capability_sets = observed_sets[0..],
    }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const price = testFieldCapabilityByIdentifier(parsed.value, "price") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("numeric", price.object.get("type").?.string);
    try std.testing.expect(testJsonArrayContainsString(price.object.get("query_modes").?, "exact"));
    try std.testing.expect(testJsonArrayContainsString(price.object.get("query_modes").?, "range"));
    try std.testing.expectEqualStrings("indexed", price.object.get("sort_lifecycle_state").?.string);
    try std.testing.expectEqualStrings("observed_dynamic", price.object.get("provenance").?.string);
}

test "create table parser preserves supported metadata fields" {
    var parsed = try parseCreateTableRequest(std.testing.allocator, "{\"num_shards\":1,\"description\":\"docs table\",\"schema\":{\"kind\":\"demo\"},\"indexes\":{\"default\":{}},\"typed_paths\":{\"numeric\":[\"metrics.score\"],\"keyword\":\"status\"},\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]}");
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u32, 1), parsed.num_shards);
    try std.testing.expectEqualStrings("docs table", parsed.description.?);
    try std.testing.expectEqualStrings("{\"version\":0,\"kind\":\"demo\"}", parsed.schema_json.?);
    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"default\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"typed_paths\":{\"numeric\":[\"metrics.score\"],\"keyword\":\"status\"}") != null);
    try std.testing.expectEqualStrings("[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]", parsed.replication_sources_json.?);
}

test "create table rejects caller-managed schema versions" {
    const body =
        "{\"schema\":{\"version\":7,\"document_schemas\":{\"file\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true}}}}}";
    try std.testing.expectError(
        error.SchemaVersionManagedByBackend,
        parseCreateTableRequest(std.testing.allocator, body),
    );
    try std.testing.expectError(
        error.SchemaVersionManagedByBackend,
        parseStoredCreateTableRequest(std.testing.allocator, body),
    );

    var stored = try parseStoredCreateTableRequest(
        std.testing.allocator,
        "{\"schema\":{\"version\":0},\"indexes\":{\"full_text_index_v0\":{\"type\":\"full_text\"}}}",
    );
    defer stored.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"version\":0}", stored.schema_json.?);
    try std.testing.expectEqualStrings(default_indexes_json, stored.indexes_json.?);
}

test "create table raw parser merges default full text with quickstart embedding index" {
    var parsed = try parseCreateTableRequest(std.testing.allocator,
        \\{
        \\  "indexes": {
        \\    "title_body": {
        \\      "type": "embeddings",
        \\      "template": "{{title}} {{body}}",
        \\      "embedder": {"provider":"antfly","model":"antflydb/clipclap"},
        \\      "chunker": {"provider":"antfly","text":{"target_tokens":200,"overlap_tokens":25}}
        \\    }
        \\  }
        \\}
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"title_body\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.indexes_json.?, "\"_coverage_incarnation\":") != null);
}

test "create table raw parser accepts its canonical full text output" {
    var first = try parseCreateTableRequest(std.testing.allocator,
        \\{"num_shards":6,"indexes":{"title_body":{"type":"embeddings","field":"body","dimension":3}}}
    );
    defer first.deinit(std.testing.allocator);

    const forwarded = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"num_shards\":6,\"indexes\":{s}}}",
        .{first.indexes_json.?},
    );
    defer std.testing.allocator.free(forwarded);
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, forwarded),
    );
    var second = try parseStoredCreateTableRequest(std.testing.allocator, forwarded);
    defer second.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(?u32, 6), second.num_shards);
    try std.testing.expect(std.mem.indexOf(u8, second.indexes_json.?, "\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, second.indexes_json.?, "\"title_body\"") != null);
}

test "create table parser rejects schemas that cannot derive runtime mappings" {
    const invalid_index_sort =
        \\{
        \\  "schema": {
        \\    "dynamic_templates": [
        \\      {"name":"rank","path_match":"rank","mapping":{"type":"numeric","sortable":false}}
        \\    ],
        \\    "index_sort": [
        \\      {"field":"rank","order":"asc"}
        \\    ]
        \\  }
        \\}
    ;
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, invalid_index_sort),
    );

    const invalid_id_order =
        \\{
        \\  "schema": {
        \\    "index_sort": [
        \\      {"field":"_id","order":"desc"}
        \\    ]
        \\  }
        \\}
    ;
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, invalid_id_order),
    );
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
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"body":{"type":"text"},"embedding":{"type":"embedding"},"source_doc":{"type":"keyword"},"target_doc":{"type":"keyword"},"edge_type":{"type":"keyword"},"confidence":{"type":"numeric"},"attrs":{"type":"json","schema":{"type":"object","properties":{"title":{"type":"text"},"plan":{"type":"keyword"},"source":{"type":"keyword"},"target":{"type":"keyword"},"edge_type":{"type":"keyword"},"confidence":{"type":"numeric"}},"additionalProperties":true}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);

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
        apply_table_update_count: usize = 0,

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
                .sequences = try self.manager.listSequences(alloc),
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
            self.manager.freeSequences(alloc, snapshot_value.sequences);
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

        pub fn upsertTable(_: *@This(), _: metadata_table_manager.TableRecord) !void {
            return error.TestUnexpectedResult;
        }

        pub fn applyTableCatalogUpdateWithSchemaRewriteJobs(
            self: *@This(),
            request: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest,
        ) !void {
            try std.testing.expectEqual(@as(usize, 0), request.schema_rewrite_jobs.len);
            self.apply_table_update_count += 1;
            try self.manager.applyTableCatalogUpdateWithSchemaRewriteJobs(request);
        }

        pub fn applyTableCatalogBatchUpdateWithSchemaRewriteJobs(
            self: *@This(),
            request: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest,
        ) !void {
            try std.testing.expectEqual(@as(usize, 0), request.schema_rewrite_jobs.len);
            self.apply_table_update_count += request.tables.len;
            try self.manager.applyTableCatalogBatchUpdateWithSchemaRewriteJobs(request);
        }

        pub fn applyTableCatalogDropWithSchemaRewriteJobs(
            self: *@This(),
            request: metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest,
        ) !void {
            try std.testing.expectEqual(@as(usize, 0), request.schema_rewrite_jobs.len);
            self.apply_table_update_count += request.table_updates.len;
            try self.manager.applyTableCatalogDropWithSchemaRewriteJobs(request);
        }

        pub fn upsertTablespace(self: *@This(), record: metadata_table_manager.TablespaceRecord) !void {
            try self.manager.upsertTablespace(record);
        }

        pub fn removeTablespace(self: *@This(), tablespace_id: u64) !void {
            _ = try self.manager.removeTablespace(tablespace_id);
        }

        pub fn upsertSequence(self: *@This(), record: metadata_table_manager.SequenceRecord) !void {
            try self.manager.upsertSequence(record);
        }

        pub fn removeSequence(self: *@This(), sequence_id: u64) !void {
            const removed = try self.manager.removeSequence(sequence_id);
            metadata_table_manager.freeSequence(self.manager.alloc, removed);
        }

        fn apply(self: *@This(), alloc: std.mem.Allocator, sql: []const u8) !AppliedRelationalSqlDdlRecord {
            return try self.applyWithSession(alloc, sql, .{});
        }

        fn applyWithSession(self: *@This(), alloc: std.mem.Allocator, sql: []const u8, session: catalog_resources.SqlCatalogSession) !AppliedRelationalSqlDdlRecord {
            var snapshot_value = try self.snapshot(alloc);
            defer self.freeSnapshot(alloc, &snapshot_value);
            var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
            defer parsed_sql.deinit(alloc);
            return (try applyRelationalCatalogDdlParsedSqlOnServiceWithSessionAlloc(alloc, self, &snapshot_value, &parsed_sql, session)) orelse error.UnsupportedSqlShape;
        }
    };

    var service = CatalogService.init(std.testing.allocator);
    defer service.deinit();
    try service.manager.ensureDefaultCatalog();

    try std.testing.expectError(error.DatabaseNotFound, service.apply(std.testing.allocator, "CREATE SCHEMA missing_db.analytics;"));

    var created_database = try service.apply(std.testing.allocator, "CREATE DATABASE tenant_ops;");
    defer created_database.deinit(std.testing.allocator);
    try std.testing.expect(created_database.created_database);

    const tenant_database_id = metadata_table_manager.deriveDatabaseId("tenant_ops");
    {
        const databases = try service.manager.listDatabases(std.testing.allocator);
        defer service.manager.freeDatabases(std.testing.allocator, databases);
        try std.testing.expectEqual(@as(usize, 2), databases.len);
        var found_tenant_database = false;
        for (databases) |database| {
            if (!std.mem.eql(u8, database.name, "tenant_ops")) continue;
            found_tenant_database = true;
            try std.testing.expectEqual(tenant_database_id, database.database_id);
        }
        try std.testing.expect(found_tenant_database);
    }

    var altered_database = try service.apply(std.testing.allocator, "ALTER DATABASE tenant_ops SET app.tenant_id TO 'tenant-a';");
    defer altered_database.deinit(std.testing.allocator);
    {
        const databases = try service.manager.listDatabases(std.testing.allocator);
        defer service.manager.freeDatabases(std.testing.allocator, databases);
        const tenant_database = for (databases) |database| {
            if (std.mem.eql(u8, database.name, "tenant_ops")) break database;
        } else return error.TestUnexpectedResult;
        var parsed_settings = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, tenant_database.settings_json, .{});
        defer parsed_settings.deinit();
        try std.testing.expectEqualStrings("tenant-a", parsed_settings.value.object.get("app.tenant_id").?.string);
    }
    var altered_runtime_database = try service.apply(std.testing.allocator, "ALTER DATABASE tenant_ops SET statement_timeout TO '5s';");
    defer altered_runtime_database.deinit(std.testing.allocator);
    {
        const databases = try service.manager.listDatabases(std.testing.allocator);
        defer service.manager.freeDatabases(std.testing.allocator, databases);
        const tenant_database = for (databases) |database| {
            if (std.mem.eql(u8, database.name, "tenant_ops")) break database;
        } else return error.TestUnexpectedResult;
        var parsed_settings = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, tenant_database.settings_json, .{});
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
    try std.testing.expectEqual(@as(usize, 1), service.apply_table_update_count);
    {
        const table = service.manager.tables.get(deriveQualifiedTableId(default_database_name, default_namespace_name, "fast_docs")).?;
        try std.testing.expectEqualStrings("archive_space", table.tablespace_name);
    }
    try std.testing.expectError(error.TablespaceInUse, service.apply(std.testing.allocator, "DROP TABLESPACE archive_space;"));
    _ = service.manager.removeTable(deriveQualifiedTableId(default_database_name, default_namespace_name, "fast_docs"));
    var dropped_tablespace = try service.apply(std.testing.allocator, "DROP TABLESPACE archive_space;");
    defer dropped_tablespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_tablespace.dropped_tablespace);

    const tenant_session = catalog_resources.SqlCatalogSession{ .current_database_name = "tenant_ops" };
    const sequence_owner_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const sequence_owner_table_id = deriveQualifiedTableId("tenant_ops", default_namespace_name, "usage_records");
    try service.manager.upsertTable(.{
        .table_id = sequence_owner_table_id,
        .name = "usage_records",
        .database_name = "tenant_ops",
        .namespace_name = default_namespace_name,
        .schema_json = sequence_owner_schema_json,
    });
    var created_sequence = try service.applyWithSession(std.testing.allocator, "CREATE SEQUENCE usage_id_seq AS bigint START WITH 10 INCREMENT BY 2 CACHE 8;", tenant_session);
    defer created_sequence.deinit(std.testing.allocator);
    try std.testing.expect(created_sequence.created_sequence);
    {
        const sequences = try service.manager.listSequences(std.testing.allocator);
        defer service.manager.freeSequences(std.testing.allocator, sequences);
        try std.testing.expectEqual(@as(usize, 1), sequences.len);
        try std.testing.expectEqualStrings("usage_id_seq", sequences[0].name);
        var parsed_options = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, sequences[0].options_json, .{});
        defer parsed_options.deinit();
        try std.testing.expectEqualStrings("bigint", parsed_options.value.object.get("as_type").?.string);
        try std.testing.expectEqual(@as(i64, 10), parsed_options.value.object.get("start_with").?.integer);
        try std.testing.expectEqual(@as(i64, 2), parsed_options.value.object.get("increment_by").?.integer);
        try std.testing.expectEqual(@as(i64, 8), parsed_options.value.object.get("cache").?.integer);
    }
    var altered_sequence = try service.applyWithSession(std.testing.allocator, "ALTER SEQUENCE usage_id_seq RESTART WITH 1000 CYCLE;", tenant_session);
    defer altered_sequence.deinit(std.testing.allocator);
    try std.testing.expect(altered_sequence.altered_sequence);
    {
        const sequences = try service.manager.listSequences(std.testing.allocator);
        defer service.manager.freeSequences(std.testing.allocator, sequences);
        var parsed_options = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, sequences[0].options_json, .{});
        defer parsed_options.deinit();
        try std.testing.expectEqual(@as(i64, 1000), parsed_options.value.object.get("restart_with").?.integer);
        try std.testing.expect(parsed_options.value.object.get("cycle").?.bool);
    }
    var dropped_sequence = try service.applyWithSession(std.testing.allocator, "DROP SEQUENCE usage_id_seq;", tenant_session);
    defer dropped_sequence.deinit(std.testing.allocator);
    try std.testing.expect(dropped_sequence.dropped_sequence);
    var missing_sequence_noop = try service.applyWithSession(std.testing.allocator, "DROP SEQUENCE IF EXISTS usage_id_seq;", tenant_session);
    defer missing_sequence_noop.deinit(std.testing.allocator);
    try std.testing.expect(missing_sequence_noop.noop);
    try std.testing.expectError(error.TableNotFound, service.applyWithSession(std.testing.allocator, "CREATE SEQUENCE missing_owned_seq OWNED BY public.missing_records.id;", tenant_session));
    try std.testing.expectError(error.InvalidSqlCatalog, service.applyWithSession(std.testing.allocator, "CREATE SEQUENCE missing_column_seq OWNED BY public.usage_records.missing_id;", tenant_session));
    var created_owned_sequence = try service.applyWithSession(std.testing.allocator, "CREATE SEQUENCE usage_owned_id_seq AS bigint START WITH 50 OWNED BY public.usage_records.id;", tenant_session);
    defer created_owned_sequence.deinit(std.testing.allocator);
    try std.testing.expect(created_owned_sequence.created_sequence);
    {
        const sequences = try service.manager.listSequences(std.testing.allocator);
        defer service.manager.freeSequences(std.testing.allocator, sequences);
        var found_owned = false;
        for (sequences) |sequence| {
            if (!std.mem.eql(u8, sequence.name, "usage_owned_id_seq")) continue;
            found_owned = true;
            var parsed_options = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, sequence.options_json, .{});
            defer parsed_options.deinit();
            try std.testing.expectEqualStrings("usage_records", parsed_options.value.object.get("owned_by").?.object.get("table_name").?.string);
            try std.testing.expectEqualStrings("id", parsed_options.value.object.get("owned_by").?.object.get("column_name").?.string);
        }
        try std.testing.expect(found_owned);
    }
    {
        var snapshot_value = try service.snapshot(std.testing.allocator);
        defer service.freeSnapshot(std.testing.allocator, &snapshot_value);
        const owner_table = findTableByQualifiedName(&snapshot_value, "tenant_ops", default_namespace_name, "usage_records").?;
        try std.testing.expectError(error.TableReferencedBySequence, validateRelationalTableDropAllowed(std.testing.allocator, &snapshot_value, owner_table.*));
    }
    var altered_owned_sequence = try service.applyWithSession(std.testing.allocator, "ALTER SEQUENCE usage_owned_id_seq OWNED BY NONE;", tenant_session);
    defer altered_owned_sequence.deinit(std.testing.allocator);
    try std.testing.expect(altered_owned_sequence.altered_sequence);
    {
        var snapshot_value = try service.snapshot(std.testing.allocator);
        defer service.freeSnapshot(std.testing.allocator, &snapshot_value);
        const owner_table = findTableByQualifiedName(&snapshot_value, "tenant_ops", default_namespace_name, "usage_records").?;
        try validateRelationalTableDropAllowed(std.testing.allocator, &snapshot_value, owner_table.*);
    }
    var dropped_owned_sequence = try service.applyWithSession(std.testing.allocator, "DROP SEQUENCE usage_owned_id_seq;", tenant_session);
    defer dropped_owned_sequence.deinit(std.testing.allocator);
    try std.testing.expect(dropped_owned_sequence.dropped_sequence);
    var created_cascade_owned_sequence = try service.applyWithSession(std.testing.allocator, "CREATE SEQUENCE usage_cascade_id_seq AS bigint START WITH 60 OWNED BY public.usage_records.id;", tenant_session);
    defer created_cascade_owned_sequence.deinit(std.testing.allocator);
    try std.testing.expect(created_cascade_owned_sequence.created_sequence);
    {
        var snapshot_value = try service.snapshot(std.testing.allocator);
        defer service.freeSnapshot(std.testing.allocator, &snapshot_value);
        const owner_table = findTableByQualifiedName(&snapshot_value, "tenant_ops", default_namespace_name, "usage_records").?;
        try std.testing.expectEqual(@as(usize, 1), try removeOwnedSequencesForTableOnServiceAlloc(std.testing.allocator, &service, &snapshot_value, owner_table.*));
    }
    {
        const sequences = try service.manager.listSequences(std.testing.allocator);
        defer service.manager.freeSequences(std.testing.allocator, sequences);
        for (sequences) |sequence| {
            try std.testing.expect(!std.mem.eql(u8, sequence.name, "usage_cascade_id_seq"));
        }
    }
    _ = service.manager.removeTable(sequence_owner_table_id);

    var created_namespace = try service.apply(std.testing.allocator, "CREATE SCHEMA analytics;");
    defer created_namespace.deinit(std.testing.allocator);
    try std.testing.expect(created_namespace.created_namespace);
    var tenant_namespace = try service.applyWithSession(std.testing.allocator, "CREATE SCHEMA private;", .{ .current_database_name = "tenant_ops" });
    defer tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(tenant_namespace.created_namespace);
    try std.testing.expectError(error.NamespaceAlreadyExists, service.applyWithSession(std.testing.allocator, "CREATE SCHEMA private;", .{ .current_database_name = "tenant_ops" }));
    var duplicate_tenant_namespace = try service.applyWithSession(std.testing.allocator, "CREATE SCHEMA IF NOT EXISTS private;", .{ .current_database_name = "tenant_ops" });
    defer duplicate_tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(duplicate_tenant_namespace.noop);
    var duplicate_qualified_tenant_namespace = try service.applyWithSession(std.testing.allocator, "CREATE SCHEMA IF NOT EXISTS tenant_ops.private;", .{ .current_database_name = "default" });
    defer duplicate_qualified_tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(duplicate_qualified_tenant_namespace.noop);
    var qualified_tenant_namespace = try service.applyWithSession(std.testing.allocator, "CREATE SCHEMA tenant_ops.reporting;", .{ .current_database_name = "default" });
    defer qualified_tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(qualified_tenant_namespace.created_namespace);
    var create_private_namespace_target = try relationalSqlSchemaNamespaceDdlTargetWithSessionAlloc(std.testing.allocator, "CREATE SCHEMA IF NOT EXISTS private;", .{ .current_database_name = "tenant_ops" });
    defer create_private_namespace_target.deinit(std.testing.allocator);
    try std.testing.expect(create_private_namespace_target.createsNamespace());
    try std.testing.expect(create_private_namespace_target.if_not_exists);
    try std.testing.expectEqualStrings("tenant_ops", create_private_namespace_target.database_name);
    try std.testing.expectEqualStrings("private", create_private_namespace_target.namespace_name);
    var rename_reporting_namespace_target = try relationalSqlSchemaNamespaceDdlTargetWithSessionAlloc(std.testing.allocator, "ALTER SCHEMA tenant_ops.reporting RENAME TO tenant_ops.archive;", .{ .current_database_name = "default" });
    defer rename_reporting_namespace_target.deinit(std.testing.allocator);
    try std.testing.expect(rename_reporting_namespace_target.renamesNamespace());
    try std.testing.expectEqualStrings("tenant_ops", rename_reporting_namespace_target.database_name);
    try std.testing.expectEqualStrings("reporting", rename_reporting_namespace_target.namespace_name);
    try std.testing.expectEqualStrings("archive", rename_reporting_namespace_target.new_namespace_name orelse return error.TestUnexpectedResult);
    var drop_reporting_namespace_target = try relationalSqlSchemaNamespaceDdlTargetWithSessionAlloc(std.testing.allocator, "DROP SCHEMA IF EXISTS tenant_ops.reporting CASCADE;", .{ .current_database_name = "default" });
    defer drop_reporting_namespace_target.deinit(std.testing.allocator);
    try std.testing.expect(drop_reporting_namespace_target.dropsNamespace());
    try std.testing.expect(drop_reporting_namespace_target.if_exists);
    try std.testing.expect(drop_reporting_namespace_target.cascade);
    try std.testing.expectEqualStrings("tenant_ops", drop_reporting_namespace_target.database_name);
    try std.testing.expectEqualStrings("reporting", drop_reporting_namespace_target.namespace_name);
    try std.testing.expectError(error.UnsupportedSqlShape, relationalSqlSchemaNamespaceDdlTargetWithSessionAlloc(std.testing.allocator, "ALTER SCHEMA tenant_ops.reporting RENAME TO default.reporting;", .{ .current_database_name = "default" }));
    try std.testing.expectError(error.UnsupportedSqlShape, service.applyWithSession(std.testing.allocator, "ALTER SCHEMA tenant_ops.reporting RENAME TO default.reporting;", .{ .current_database_name = "default" }));
    {
        var snapshot_value = try service.snapshot(std.testing.allocator);
        defer service.freeSnapshot(std.testing.allocator, &snapshot_value);
        try std.testing.expect(findNamespaceByName(&snapshot_value, "tenant_ops", "private") != null);
        try std.testing.expect(findNamespaceByName(&snapshot_value, "tenant_ops", "reporting") != null);
    }
    try std.testing.expectError(error.NamespaceNotFound, service.applyWithSession(std.testing.allocator, "DROP SCHEMA missing;", .{ .current_database_name = "tenant_ops" }));
    var missing_tenant_namespace = try service.applyWithSession(std.testing.allocator, "DROP SCHEMA IF EXISTS missing;", .{ .current_database_name = "tenant_ops" });
    defer missing_tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(missing_tenant_namespace.noop);
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

    var cascade_namespace = try service.apply(std.testing.allocator, "CREATE SCHEMA scratch;");
    defer cascade_namespace.deinit(std.testing.allocator);
    try std.testing.expect(cascade_namespace.created_namespace);
    try service.manager.upsertTable(.{
        .table_id = deriveQualifiedTableId(default_database_name, "scratch", "events"),
        .name = "events",
        .database_name = default_database_name,
        .namespace_name = "scratch",
    });
    try std.testing.expectError(error.NamespaceNotEmpty, service.apply(std.testing.allocator, "DROP SCHEMA scratch;"));
    var dropped_cascade_namespace = try service.apply(std.testing.allocator, "DROP SCHEMA scratch CASCADE;");
    defer dropped_cascade_namespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_cascade_namespace.dropped_namespace);
    try std.testing.expect(service.manager.tables.get(deriveQualifiedTableId(default_database_name, "scratch", "events")) == null);

    var renamed_namespace = try service.apply(std.testing.allocator, "ALTER SCHEMA analytics RENAME TO reporting;");
    defer renamed_namespace.deinit(std.testing.allocator);
    try std.testing.expect(renamed_namespace.renamed_namespace);
    try std.testing.expectEqual(@as(usize, 2), service.apply_table_update_count);
    const table = service.manager.tables.get(deriveQualifiedTableId(default_database_name, "analytics", "events")).?;
    try std.testing.expectEqualStrings("reporting", table.namespace_name);

    _ = service.manager.removeTable(table.table_id);
    var dropped_namespace = try service.apply(std.testing.allocator, "DROP SCHEMA reporting;");
    defer dropped_namespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_namespace.dropped_namespace);

    var dropped_tenant_namespace = try service.applyWithSession(std.testing.allocator, "DROP SCHEMA private;", .{ .current_database_name = "tenant_ops" });
    defer dropped_tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_tenant_namespace.dropped_namespace);
    var dropped_qualified_tenant_namespace = try service.applyWithSession(std.testing.allocator, "DROP SCHEMA tenant_ops.reporting;", .{ .current_database_name = "default" });
    defer dropped_qualified_tenant_namespace.deinit(std.testing.allocator);
    try std.testing.expect(dropped_qualified_tenant_namespace.dropped_namespace);

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

test "metadata.schema update versions template-only changes" {
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

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":3") != null);
    try std.testing.expectEqualStrings(table.read_schema_json, updated.read_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v2\":{\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v3\":{\"name\":\"full_text_index_v3\",\"type\":\"full_text\"}") != null);
}

test "metadata.schema update avoids a generation for semantically identical JSON" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":2,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true,\"required\":[\"title\",\"body\"],\"properties\":{\"title\":{\"type\":\"string\"},\"body\":{\"type\":\"string\"}}}}}}",
        .read_schema_json = "{\"version\":1}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"document_schemas\":{\"doc\":{\"schema\":{\"properties\":{\"body\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},\"required\":[\"body\",\"title\"],\"additionalProperties\":true,\"type\":\"object\"}}},\"version\":999,\"default_type\":\"doc\",\"enforce_types\":false}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":2") != null);
    try std.testing.expectEqualStrings(table.read_schema_json, updated.read_schema_json);
    try std.testing.expectEqualStrings(table.indexes_json, updated.indexes_json);
}

test "metadata.schema update avoids a generation for explicit runtime defaults" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":2}",
        .read_schema_json = "{\"version\":1}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"version\":999,\"default_type\":\"\",\"enforce_types\":false}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":2") != null);
    try std.testing.expectEqualStrings(table.read_schema_json, updated.read_schema_json);
    try std.testing.expectEqualStrings(table.indexes_json, updated.indexes_json);
}

test "metadata.schema update versions dynamic rule precedence changes" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json =
        \\{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","patternProperties":{
        \\  "^tag_.*":{"type":"string","x-antfly-types":["text"]},
        \\  ".*":{"type":"string","x-antfly-types":["keyword"]}
        \\}}}}}
        ,
        .read_schema_json = "{\"version\":1}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(std.testing.allocator, &table,
        \\{"document_schemas":{"doc":{"schema":{"type":"object","patternProperties":{
        \\  ".*":{"type":"string","x-antfly-types":["keyword"]},
        \\  "^tag_.*":{"type":"string","x-antfly-types":["text"]}
        \\}}}}}
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v3\"") != null);
}

test "metadata.schema update versions inferred dynamic path changes" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json =
        \\{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","properties":{
        \\  "meta":{"type":"object"}
        \\}}}}}
        ,
        .read_schema_json = "{\"version\":1}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(std.testing.allocator, &table,
        \\{"document_schemas":{"doc":{"schema":{"type":"object","properties":{
        \\  "meta":{"type":"object","additionalProperties":true,"x-antfly-dynamic-indexing":{"mode":"infer_types"}}
        \\}}}}}
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v3\"") != null);
}

test "metadata.schema update versions validation-only changes" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json =
        \\{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","properties":{
        \\  "title":{"type":"string","minLength":1}
        \\}}}}}
        ,
        .read_schema_json = "{\"version\":1}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(std.testing.allocator, &table,
        \\{"document_schemas":{"doc":{"schema":{"type":"object","properties":{
        \\  "title":{"type":"string","minLength":2}
        \\}}}}}
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v3\"") != null);
}

test "metadata.schema update rejects generation overflow" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":4294967295}",
        .indexes_json = "{\"full_text_index_v4294967295\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    try std.testing.expectError(
        error.SchemaVersionExhausted,
        applySchemaUpdateRecord(std.testing.allocator, &table, "{\"default_type\":\"doc\"}"),
    );
}

test "metadata.schema update can repair a legacy non-derivable schema" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":1,\"index_sort\":[{\"field\":\"_id\",\"order\":\"desc\"}]}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(std.testing.allocator, &table, "{}");
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expectEqualStrings("{\"version\":2}", updated.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v2\"") != null);
}

test "metadata.schema update rejects schemas that cannot derive runtime mappings" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":0}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const invalid_index_sort =
        \\{
        \\  "dynamic_templates": [
        \\    {"name":"rank","path_match":"rank","mapping":{"type":"numeric","sortable":false}}
        \\  ],
        \\  "index_sort": [
        \\    {"field":"rank","order":"asc"}
        \\  ]
        \\}
    ;

    const normalized = try parseSchemaUpdateRequest(std.testing.allocator, invalid_index_sort);
    defer std.testing.allocator.free(normalized);
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(std.testing.allocator, &table, normalized),
    );

    const invalid_id_order =
        \\{
        \\  "index_sort": [
        \\    {"field":"_id","order":"desc"}
        \\  ]
        \\}
    ;
    const normalized_id_order = try parseSchemaUpdateRequest(std.testing.allocator, invalid_id_order);
    defer std.testing.allocator.free(normalized_id_order);
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        applySchemaUpdateRecord(std.testing.allocator, &table, normalized_id_order),
    );
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
    try std.testing.expect(std.mem.indexOf(u8, full_text.table.indexes_json, "\"docs_body_fts\"") == null);
    var full_text_schema = try parseValidatedTableSchema(std.testing.allocator, full_text.table.schema_json);
    defer full_text_schema.deinit(std.testing.allocator);
    const full_text_runtime = try deriveRuntimeTableSchema(std.testing.allocator, full_text_schema);
    defer runtime_schema_mod.freeSchema(std.testing.allocator, full_text_runtime);
    const docs_body_fts = secondaryIndexCatalogByName(full_text_runtime.relational_indexes, "docs_body_fts") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexAccessMethod.text_search, docs_body_fts.access_method);
    try std.testing.expectEqualStrings("body", docs_body_fts.owner_name);
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.building, docs_body_fts.lifecycle);
    try std.testing.expect(docs_body_fts.generation != 0);
    try std.testing.expect(docs_body_fts.schema_fingerprint != null);
    try std.testing.expect(docs_body_fts.planner_capabilities.full_text);
    try std.testing.expect(docs_body_fts.planner_capabilities.rank);
    try std.testing.expect(!docs_body_fts.planner_capabilities.ordering);
    try std.testing.expect(docs_body_fts.method_config_json != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_body_fts.method_config_json.?, "\"type\":\"full_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_body_fts.method_config_json.?, "\"field\":\"body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_body_fts.method_config_json.?, "\"analyzer\":\"standard\"") != null);

    var text_search = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &full_text.table,
        "CREATE TEXT SEARCH docs_source_doc_fts ON docs (source_doc) WITH (analyzer = 'standard', scoring = 'bm25', highlight = true, snippet = true);",
    );
    defer text_search.deinit(std.testing.allocator);
    try std.testing.expect(text_search.requires_rebuild);
    try std.testing.expectEqual(@as(usize, 1), text_search.work_items.len);
    try std.testing.expect(std.mem.indexOf(u8, text_search.table.indexes_json, "\"docs_source_doc_fts\"") == null);
    var text_search_schema = try parseValidatedTableSchema(std.testing.allocator, text_search.table.schema_json);
    defer text_search_schema.deinit(std.testing.allocator);
    const text_search_runtime = try deriveRuntimeTableSchema(std.testing.allocator, text_search_schema);
    defer runtime_schema_mod.freeSchema(std.testing.allocator, text_search_runtime);
    const docs_source_doc_fts = secondaryIndexCatalogByName(text_search_runtime.relational_indexes, "docs_source_doc_fts") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexAccessMethod.text_search, docs_source_doc_fts.access_method);
    try std.testing.expectEqualStrings("source_doc", docs_source_doc_fts.owner_name);
    try std.testing.expect(docs_source_doc_fts.planner_capabilities.full_text);
    try std.testing.expect(docs_source_doc_fts.planner_capabilities.rank);
    try std.testing.expect(docs_source_doc_fts.method_config_json != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_source_doc_fts.method_config_json.?, "\"type\":\"full_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_source_doc_fts.method_config_json.?, "\"field\":\"source_doc\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_source_doc_fts.method_config_json.?, "\"analyzer\":\"standard\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_source_doc_fts.method_config_json.?, "\"scoring\":\"bm25\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_source_doc_fts.method_config_json.?, "\"highlight\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_source_doc_fts.method_config_json.?, "\"snippet\":true") != null);

    var algebraic = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &text_search.table,
        "CREATE INDEX docs_algebraic_filter ON docs USING antfly_algebraic () WITH (derive_from_schema = true);",
    );
    defer algebraic.deinit(std.testing.allocator);
    try std.testing.expect(algebraic.requires_rebuild);
    try std.testing.expectEqual(@as(usize, 1), algebraic.work_items.len);
    try std.testing.expect(std.mem.indexOf(u8, algebraic.table.indexes_json, "\"docs_algebraic_filter\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, algebraic.table.indexes_json, "\"derive_from_schema\"") == null);
    var algebraic_schema = try parseValidatedTableSchema(std.testing.allocator, algebraic.table.schema_json);
    defer algebraic_schema.deinit(std.testing.allocator);
    const algebraic_runtime = try deriveRuntimeTableSchema(std.testing.allocator, algebraic_schema);
    defer runtime_schema_mod.freeSchema(std.testing.allocator, algebraic_runtime);
    const docs_algebraic_filter = secondaryIndexCatalogByName(algebraic_runtime.relational_indexes, "docs_algebraic_filter") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexOwnerKind.table, docs_algebraic_filter.owner_kind);
    try std.testing.expectEqualStrings(runtime_schema_mod.relational_table_index_owner_name, docs_algebraic_filter.owner_name);
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexAccessMethod.algebraic_filter, docs_algebraic_filter.access_method);
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.building, docs_algebraic_filter.lifecycle);
    try std.testing.expect(docs_algebraic_filter.generation != 0);
    try std.testing.expect(docs_algebraic_filter.schema_fingerprint != null);
    try std.testing.expect(docs_algebraic_filter.planner_capabilities.equality);
    try std.testing.expect(docs_algebraic_filter.planner_capabilities.prefix);
    try std.testing.expect(docs_algebraic_filter.planner_capabilities.array);
    try std.testing.expect(docs_algebraic_filter.planner_capabilities.json);
    try std.testing.expect(docs_algebraic_filter.planner_capabilities.algebraic_dictionary);
    try std.testing.expect(docs_algebraic_filter.planner_capabilities.algebraic_fact);
    try std.testing.expect(docs_algebraic_filter.planner_capabilities.algebraic_path);
    try std.testing.expect(docs_algebraic_filter.method_config_json != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_algebraic_filter.method_config_json.?, "\"type\":\"algebraic\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs_algebraic_filter.method_config_json.?, "\"derive_from_schema\":true") != null);

    var semantic = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &algebraic.table,
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

    try std.testing.expectError(
        error.InvalidTableIndexMetadata,
        applyRelationalSqlDdlToTableRecordAlloc(
            std.testing.allocator,
            &hybrid.table,
            "DROP INDEX docs_body_fts;",
        ),
    );

    var duplicate_noop = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &hybrid.table,
        "CREATE INDEX IF NOT EXISTS docs_body_fts ON docs USING antfly_full_text (body);",
    );
    defer duplicate_noop.deinit(std.testing.allocator);
    try std.testing.expect(!duplicate_noop.requires_rebuild);
    try std.testing.expectEqualStrings(hybrid.table.indexes_json, duplicate_noop.table.indexes_json);
    try std.testing.expectEqualStrings(hybrid.table.schema_json, duplicate_noop.table.schema_json);

    try std.testing.expectError(
        error.InvalidSqlCatalog,
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

    var parsed_create_sql = try sql_adapter.ParsedSql.initAlloc(std.testing.allocator, "CREATE TABLE parsed_events (id uuid PRIMARY KEY);");
    defer parsed_create_sql.deinit(std.testing.allocator);
    var parsed_create_target = try relationalSqlDdlTargetParsedSqlAlloc(std.testing.allocator, &parsed_create_sql);
    defer parsed_create_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(default_database_name, parsed_create_target.database_name);
    try std.testing.expectEqualStrings(default_namespace_name, parsed_create_target.namespace_name);
    try std.testing.expectEqualStrings("parsed_events", parsed_create_target.table_name);
    try std.testing.expect(parsed_create_target.createsTable());

    const tenant_search_path: []const []const u8 = &.{ "analytics", default_namespace_name };
    var session_create_target = try relationalSqlDdlTargetWithSessionAlloc(std.testing.allocator, "CREATE TABLE events (id uuid PRIMARY KEY);", .{
        .current_database_name = "tenant_ops",
        .search_path = tenant_search_path,
    });
    defer session_create_target.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("tenant_ops", session_create_target.database_name);
    try std.testing.expectEqualStrings("analytics", session_create_target.namespace_name);
    try std.testing.expectEqualStrings("events", session_create_target.table_name);
    try std.testing.expect(session_create_target.createsTable());

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

    const default_table: metadata_table_manager.TableRecord = .{
        .table_id = 14,
        .name = "usage_records",
        .schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };
    var scalar_default_created = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &default_table,
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, status text DEFAULT (SELECT status FROM usage_records ORDER BY id LIMIT 1));",
    );
    defer scalar_default_created.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, scalar_default_created.table.schema_json, "\"op\":\"scalar_subquery\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, scalar_default_created.table.schema_json, "\"query\":{\"table\":\"usage_records\"") != null);
    var scalar_default_created_parsed = try schema_mod.parseValidatedTableSchema(std.testing.allocator, scalar_default_created.table.schema_json);
    defer scalar_default_created_parsed.deinit(std.testing.allocator);
    const scalar_default_created_runtime = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, scalar_default_created_parsed);
    defer runtime_schema_mod.freeSchema(std.testing.allocator, scalar_default_created_runtime);
    const scalar_default_created_status = findRuntimeRelationalColumn(scalar_default_created_runtime, "status") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalDefaultKind.scalar_subquery, scalar_default_created_status.default_value.?.kind);

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
    var qualified_create_sql = try sql_adapter.ParsedSql.initAlloc(std.testing.allocator, "CREATE TABLE analytics.users (id uuid PRIMARY KEY);");
    defer qualified_create_sql.deinit(std.testing.allocator);
    var qualified_created = try applyRelationalSqlDdlParsedSqlToTableRecordAlloc(
        std.testing.allocator,
        &qualified_table,
        &qualified_create_sql,
    );
    defer qualified_created.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("users", qualified_created.table.name);
    try std.testing.expectEqualStrings("analytics", qualified_created.table.namespace_name);
    try std.testing.expect(std.mem.indexOf(u8, qualified_created.table.schema_json, "\"analytics.users_pkey\"") == null);

    var qualified_primary_key_dropped = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &qualified_created.table,
        "ALTER TABLE analytics.users DROP CONSTRAINT users_pkey;",
    );
    defer qualified_primary_key_dropped.deinit(std.testing.allocator);
    try std.testing.expect(qualified_primary_key_dropped.requires_rebuild);
    try std.testing.expect(!qualified_primary_key_dropped.validation_required);
    try std.testing.expect(qualified_primary_key_dropped.rewrite_required);
    try std.testing.expectEqual(@as(usize, 2), qualified_primary_key_dropped.work_items.len);
    try std.testing.expectEqual(sql_adapter.AppliedDdlWorkAction.rebuild, qualified_primary_key_dropped.work_items[0].action);
    try std.testing.expectEqual(sql_adapter.AppliedDdlWorkAction.rewrite, qualified_primary_key_dropped.work_items[1].action);
    try std.testing.expect(std.mem.indexOf(u8, qualified_primary_key_dropped.table.schema_json, "\"primary_key\"") == null);

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

    var literal_default_altered = try applyRelationalSqlDdlToTableRecordAlloc(
        std.testing.allocator,
        &altered.table,
        "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'queued';",
    );
    defer literal_default_altered.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, literal_default_altered.table.schema_json, "\"queued\"") != null);
    var literal_default_altered_parsed = try schema_mod.parseValidatedTableSchema(std.testing.allocator, literal_default_altered.table.schema_json);
    defer literal_default_altered_parsed.deinit(std.testing.allocator);
    const literal_default_altered_runtime = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, literal_default_altered_parsed);
    defer runtime_schema_mod.freeSchema(std.testing.allocator, literal_default_altered_runtime);
    const literal_default_altered_status = findRuntimeRelationalColumn(literal_default_altered_runtime, "status") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalDefaultKind.literal, literal_default_altered_status.default_value.?.kind);

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
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"amount","owner_kind":"relational_column","owner_name":"amount","access_method":"scalar_column","columns":["amount"],"lifecycle":"building","generation":9,"schema_fingerprint":"secondary-index-v1:amount"}]}
    ;

    const updated = try schemaWithSecondaryIndexReadyCheckedAlloc(alloc, schema_json, "amount", .{
        .generation = 9,
        .access_method = .scalar_column,
        .schema_fingerprint = "secondary-index-v1:amount",
    });
    defer alloc.free(updated);
    var parsed = try parseValidatedTableSchema(alloc, updated);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    const amount_index = secondaryIndexCatalogByName(runtime.relational_indexes, "amount") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.ready, amount_index.lifecycle);
    try std.testing.expectEqual(@as(u64, 9), amount_index.generation);

    try std.testing.expectError(
        error.SecondaryIndexGenerationMismatch,
        schemaWithSecondaryIndexReadyCheckedAlloc(alloc, schema_json, "amount", .{
            .generation = 10,
            .access_method = .scalar_column,
            .schema_fingerprint = "secondary-index-v1:amount",
        }),
    );
    try std.testing.expectError(
        error.SecondaryIndexNotFound,
        schemaWithSecondaryIndexReadyCheckedAlloc(alloc, schema_json, "missing", .{
            .generation = 9,
            .access_method = .scalar_column,
            .schema_fingerprint = "secondary-index-v1:amount",
        }),
    );
    try std.testing.expectError(
        error.SecondaryIndexNotBuilding,
        schemaWithSecondaryIndexReadyCheckedAlloc(alloc, updated, "amount", .{
            .generation = 9,
            .access_method = .scalar_column,
            .schema_fingerprint = "secondary-index-v1:amount",
        }),
    );

    const catching_up_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"amount","owner_kind":"relational_column","owner_name":"amount","access_method":"scalar_column","columns":["amount"],"lifecycle":"catching_up","generation":11,"schema_fingerprint":"secondary-index-v1:amount"}]}
    ;
    const caught_up = try schemaWithSecondaryIndexReadyCheckedAlloc(alloc, catching_up_json, "amount", .{
        .generation = 11,
        .access_method = .scalar_column,
        .schema_fingerprint = "secondary-index-v1:amount",
    });
    defer alloc.free(caught_up);
    var caught_up_parsed = try parseValidatedTableSchema(alloc, caught_up);
    defer caught_up_parsed.deinit(alloc);
    const caught_up_runtime = try deriveRuntimeTableSchema(alloc, caught_up_parsed);
    defer runtime_schema_mod.freeSchema(alloc, caught_up_runtime);
    const caught_up_index = secondaryIndexCatalogByName(caught_up_runtime.relational_indexes, "amount") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.ready, caught_up_index.lifecycle);

    const stale_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"amount","owner_kind":"relational_column","owner_name":"amount","access_method":"scalar_column","columns":["amount"],"lifecycle":"stale","generation":12,"schema_fingerprint":"secondary-index-v1:amount"}]}
    ;
    try std.testing.expectError(
        error.SecondaryIndexNotBuilding,
        schemaWithSecondaryIndexReadyCheckedAlloc(alloc, stale_json, "amount", .{
            .generation = 12,
            .access_method = .scalar_column,
            .schema_fingerprint = "secondary-index-v1:amount",
        }),
    );

    const checked_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"amount_idx","owner_kind":"relational_column","owner_name":"amount","access_method":"scalar_column","columns":["amount"],"lifecycle":"building","generation":13,"schema_fingerprint":"secondary-index-v1:amount"}]}
    ;
    const checked_ready = try schemaWithSecondaryIndexReadyCheckedAlloc(alloc, checked_json, "amount_idx", .{
        .generation = 13,
        .access_method = .scalar_column,
        .schema_fingerprint = "secondary-index-v1:amount",
    });
    defer alloc.free(checked_ready);
    var checked_parsed = try parseValidatedTableSchema(alloc, checked_ready);
    defer checked_parsed.deinit(alloc);
    const checked_runtime = try deriveRuntimeTableSchema(alloc, checked_parsed);
    defer runtime_schema_mod.freeSchema(alloc, checked_runtime);
    const checked_index = secondaryIndexCatalogByName(checked_runtime.relational_indexes, "amount_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.ready, checked_index.lifecycle);

    try std.testing.expectError(
        error.SecondaryIndexAccessMethodMismatch,
        schemaWithSecondaryIndexReadyCheckedAlloc(alloc, checked_json, "amount_idx", .{
            .generation = 13,
            .access_method = .ordered_tuple,
            .schema_fingerprint = "secondary-index-v1:amount",
        }),
    );
    try std.testing.expectError(
        error.SecondaryIndexSchemaFingerprintMismatch,
        schemaWithSecondaryIndexReadyCheckedAlloc(alloc, checked_json, "amount_idx", .{
            .generation = 13,
            .access_method = .scalar_column,
            .schema_fingerprint = "secondary-index-v1:other",
        }),
    );

    const ordered_tuple_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"tenant_amount_idx","owner_kind":"relational_column","owner_name":"tenant","access_method":"ordered_tuple","columns":["tenant"],"keys":[{"column":"tenant"},{"column":"amount"}],"lifecycle":"building","generation":15,"schema_fingerprint":"secondary-index-v1:tenant_amount","generation_record":{"generation":15,"owner_ranges":[],"lifecycle":"building","lag":0,"ready_watermark":0}}]}
    ;
    const ordered_ready = try schemaWithSecondaryIndexReadyCheckedAlloc(alloc, ordered_tuple_json, "tenant_amount_idx", .{
        .generation = 15,
        .access_method = .ordered_tuple,
        .schema_fingerprint = "secondary-index-v1:tenant_amount",
    });
    defer alloc.free(ordered_ready);
    var ordered_parsed = try parseValidatedTableSchema(alloc, ordered_ready);
    defer ordered_parsed.deinit(alloc);
    const ordered_runtime = try deriveRuntimeTableSchema(alloc, ordered_parsed);
    defer runtime_schema_mod.freeSchema(alloc, ordered_runtime);
    const ordered_index = secondaryIndexCatalogByName(ordered_runtime.relational_indexes, "tenant_amount_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.ready, ordered_index.lifecycle);
    const ordered_record = ordered_index.generation_record orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.ready, ordered_record.lifecycle);
    try std.testing.expectEqual(@as(u64, 15), ordered_record.generation);

    const mismatched_ordered_tuple_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"tenant_amount_idx","owner_kind":"relational_column","owner_name":"tenant","access_method":"ordered_tuple","columns":["tenant"],"keys":[{"column":"tenant"},{"column":"amount"}],"lifecycle":"building","generation":15,"schema_fingerprint":"secondary-index-v1:tenant_amount","generation_record":{"generation":15,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}}]}
    ;
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        schemaWithSecondaryIndexReadyCheckedAlloc(alloc, mismatched_ordered_tuple_json, "tenant_amount_idx", .{
            .generation = 15,
            .access_method = .ordered_tuple,
            .schema_fingerprint = "secondary-index-v1:tenant_amount",
        }),
    );
}

test "metadata.schema update marks secondary index building and permits lifecycle transition" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"usage_status_idx","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"],"lifecycle":"ready","generation":1,"schema_fingerprint":"secondary-index-v1:usage_status_idx"}]}
    ;

    const building = try schemaWithSecondaryIndexBuildingAlloc(alloc, schema_json, "usage_status_idx", 10);
    defer alloc.free(building);
    var parsed = try parseValidatedTableSchema(alloc, building);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    const status_index = secondaryIndexCatalogByName(runtime.relational_indexes, "usage_status_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.building, status_index.lifecycle);
    try std.testing.expectEqual(@as(u64, 10), status_index.generation);
    try std.testing.expect(status_index.planner_capabilities.equality);
    try std.testing.expect(status_index.planner_capabilities.range);
    try std.testing.expect(!status_index.planner_capabilities.ordering);
    try std.testing.expect(!status_index.planner_capabilities.full_text);

    const table: metadata_table_manager.TableRecord = .{
        .table_id = 9,
        .name = "usage_records",
        .schema_json = schema_json,
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };
    const updated_table = try applySchemaUpdateRecord(alloc, &table, building);
    defer metadata_table_manager.freeTable(alloc, updated_table);

    const ready = try schemaWithSecondaryIndexReadyCheckedAlloc(alloc, updated_table.schema_json, "usage_status_idx", .{
        .generation = 10,
        .access_method = .scalar_column,
        .schema_fingerprint = "secondary-index-v1:usage_status_idx",
    });
    defer alloc.free(ready);
    const ready_table = try applySchemaUpdateRecord(alloc, &updated_table, ready);
    defer metadata_table_manager.freeTable(alloc, ready_table);

    try std.testing.expectError(
        error.SecondaryIndexNotFound,
        schemaWithSecondaryIndexBuildingAlloc(alloc, schema_json, "missing_idx", 10),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        schemaWithSecondaryIndexBuildingAlloc(alloc, schema_json, "usage_status_idx", 0),
    );
    const missing_access_method_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"usage_status_idx","owner_kind":"relational_column","owner_name":"status","columns":["status"],"lifecycle":"ready","generation":1,"schema_fingerprint":"secondary-index-v1:usage_status_idx"}]}
    ;
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        schemaWithSecondaryIndexBuildingAlloc(alloc, missing_access_method_json, "usage_status_idx", 10),
    );
    const missing_fingerprint_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"usage_status_idx","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"],"lifecycle":"ready","generation":1}]}
    ;
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        schemaWithSecondaryIndexBuildingAlloc(alloc, missing_fingerprint_json, "usage_status_idx", 10),
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

test "public schema catalog roundtrip preserves first-class relational indexes" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"status":{"type":"keyword"},"created_at":{"type":"datetime"}},"required":["id","tenant_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"tenant_status_idx","owner_kind":"relational_column","owner_name":"tenant_id","access_method":"ordered_tuple","columns":["tenant_id","status"],"include_columns":["created_at"],"keys":[{"column":"tenant_id"},{"column":"status","collation":"C","direction":"desc","nulls":"last"}],"lifecycle":"catching_up","generation":7,"schema_fingerprint":"secondary-index-v1:tenant_status","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"catching_up","lag":0,"ready_watermark":0},"where":{"all":[{"field":"tenant_id","op":"is_not_null"}]}}]}
    ;

    var public_arena = std.heap.ArenaAllocator.init(alloc);
    defer public_arena.deinit();
    const public_schema = try parseTableSchema(public_arena.allocator(), schema_json);
    const public_indexes = public_schema.relational_indexes orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), public_indexes.len);
    try std.testing.expectEqualStrings("tenant_status_idx", public_indexes[0].name);
    try std.testing.expectEqual(schema_openapi.RelationalIndexAccessMethod.ordered_tuple, public_indexes[0].access_method);
    try std.testing.expectEqualStrings("tenant_id", public_indexes[0].keys.?[0].column);
    try std.testing.expectEqualStrings("C", public_indexes[0].keys.?[1].collation.?);
    try std.testing.expectEqual(schema_openapi.RelationalIndexKeyDirection.desc, public_indexes[0].keys.?[1].direction.?);
    try std.testing.expectEqual(schema_openapi.RelationalIndexKeyNulls.last, public_indexes[0].keys.?[1].nulls.?);

    const table: metadata_table_manager.TableRecord = .{
        .table_id = 91,
        .name = "orders",
        .schema_json = schema_json,
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };
    const updated = try applySchemaUpdateRecord(alloc, &table, schema_json);
    defer metadata_table_manager.freeTable(alloc, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"relational_indexes\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"tenant_status_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"x-antfly-index-generation\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"tenant_status_idx\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"algebraic_index_v0\"") != null);

    var parsed = try schema_mod.parseValidatedTableSchema(alloc, updated.schema_json);
    defer parsed.deinit(alloc);
    const runtime = try schema_mod.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);
    try std.testing.expectEqual(@as(usize, 1), runtime.relational_indexes.len);
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexAccessMethod.ordered_tuple, runtime.relational_indexes[0].access_method);
    try std.testing.expectEqual(runtime_schema_mod.RelationalIndexLifecycle.catching_up, runtime.relational_indexes[0].lifecycle);
    try std.testing.expectEqual(@as(u64, 7), runtime.relational_indexes[0].generation);
}

test "public schema catalog roundtrip preserves admitted relational access methods" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"attrs":{"type":"json"},"body":{"type":"text"}},"required":["id","tenant_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"status_scalar_idx","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"],"generation":3,"schema_fingerprint":"secondary-index-v1:status_scalar"},{"name":"tenant_amount_idx","owner_kind":"relational_column","owner_name":"tenant_id","access_method":"ordered_tuple","columns":["tenant_id","amount"],"include_columns":["status"],"keys":[{"column":"tenant_id"},{"column":"amount","direction":"desc","nulls":"last"}],"lifecycle":"ready","generation":4,"schema_fingerprint":"secondary-index-v1:tenant_amount","generation_record":{"generation":4,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}},{"name":"attrs_algebraic_idx","owner_kind":"relational_column","owner_name":"attrs","access_method":"algebraic_filter","columns":["attrs"],"generation":5,"schema_fingerprint":"secondary-index-v1:attrs_algebraic","generation_record":{"generation":5,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}},{"name":"body_text_idx","owner_kind":"relational_column","owner_name":"body","access_method":"text_search","method_config":{"type":"full_text","field":"body","analyzer":"standard"},"columns":["body"],"generation":6,"schema_fingerprint":"secondary-index-v1:body_text","generation_record":{"generation":6,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}}]}
    ;

    var public_arena = std.heap.ArenaAllocator.init(alloc);
    defer public_arena.deinit();
    const public_schema = try parseTableSchema(public_arena.allocator(), schema_json);
    const public_indexes = public_schema.relational_indexes orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 4), public_indexes.len);
    try std.testing.expectEqual(schema_openapi.RelationalIndexAccessMethod.scalar_column, public_indexes[0].access_method);
    try std.testing.expectEqual(schema_openapi.RelationalIndexAccessMethod.ordered_tuple, public_indexes[1].access_method);
    try std.testing.expectEqual(schema_openapi.RelationalIndexAccessMethod.algebraic_filter, public_indexes[2].access_method);
    try std.testing.expectEqual(schema_openapi.RelationalIndexAccessMethod.text_search, public_indexes[3].access_method);
    try std.testing.expectEqualStrings("amount", public_indexes[1].keys.?[1].column);
    try std.testing.expectEqual(schema_openapi.RelationalIndexKeyDirection.desc, public_indexes[1].keys.?[1].direction.?);

    const table: metadata_table_manager.TableRecord = .{
        .table_id = 92,
        .name = "events",
        .schema_json = schema_json,
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };
    const updated = try applySchemaUpdateRecord(alloc, &table, schema_json);
    defer metadata_table_manager.freeTable(alloc, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"status_scalar_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"tenant_amount_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"attrs_algebraic_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"body_text_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"x-antfly-index-name\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"x-antfly-index-access-method\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"status_scalar_idx\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"tenant_amount_idx\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"attrs_algebraic_idx\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"body_text_idx\"") == null);

    var parsed = try schema_mod.parseValidatedTableSchema(alloc, updated.schema_json);
    defer parsed.deinit(alloc);
    const runtime = try schema_mod.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema_mod.freeSchema(alloc, runtime);

    var scalar_seen = false;
    var ordered_seen = false;
    var algebraic_seen = false;
    var text_seen = false;
    for (runtime.relational_indexes) |index| {
        switch (index.access_method) {
            .scalar_column => scalar_seen = true,
            .ordered_tuple => ordered_seen = true,
            .algebraic_filter => algebraic_seen = true,
            .text_search => text_seen = true,
        }
    }
    try std.testing.expect(scalar_seen);
    try std.testing.expect(ordered_seen);
    try std.testing.expect(algebraic_seen);
    try std.testing.expect(text_seen);
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

test "metadata.schema update versions and refreshes algebraic dynamic templates without recreate" {
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

    // Dynamic templates affect runtime schema behavior, so the update advances
    // the schema version while retaining the branch's algebraic refresh.
    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":3") != null);
    // The durable algebraic config carries the numeric rule, proving the
    // dynamic template propagated without an index recreate.
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
