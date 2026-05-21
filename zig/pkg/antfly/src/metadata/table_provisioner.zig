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
const backups_api = @import("../api/backups.zig");
const fs_paths = @import("../common/fs_paths.zig");
const metadata_api = @import("api.zig");
const table_manager = @import("table_manager.zig");
const raft_catalog = @import("../raft/catalog.zig");
const backup_restore = @import("../raft/storage/backup_restore.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const change_journal_mod = @import("../storage/db/derived/change_journal.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const table_reads = @import("../api/table_reads.zig");
const table_catalog = @import("../api/table_catalog.zig");
const tables_api = @import("../api/tables.zig");
const raft_mod = @import("../raft/mod.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const shard_db_adapter_mod = @import("shard_db_adapter.zig");
const doc_identity = @import("../storage/db/doc_identity.zig");

pub const ProvisionSummary = struct {
    groups_considered: usize = 0,
    dbs_opened: usize = 0,
    indexes_added: usize = 0,
    indexes_removed: usize = 0,
};

pub const ReconcileReplicaRootOptions = struct {
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    shard_db_adapter: ?shard_db_adapter_mod.ShardDbAdapter = null,
};

fn provisioningDbOpenOptions() db_mod.OpenOptions {
    return .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
        .text_merge = .{ .enabled = false },
    };
}

const TableProgressStatus = struct {
    table_id: u64,
    node_id: u64,
    schema_version: u32,
    range_count: usize = 0,
    all_ready: bool = true,
};

const RestoreIntentSource = struct {
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8 = "",
};

pub fn groupDbPathFromReplicaRoot(alloc: std.mem.Allocator, replica_root_dir: []const u8, group_id: u64) ![]u8 {
    return try backup_restore.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
}

pub fn applyBackupRestoreBootstrap(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: raft_catalog.BackupRestoreBootstrapRecord,
) !void {
    try backup_restore.applyBackupRestoreFromRecord(alloc, replica_root_dir, group_id, restore);
}

pub fn provisioningFingerprint(
    metadata_group_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
) u64 {
    var hasher = std.hash.Wyhash.init(0xa17f_2026_0409);
    hasher.update(std.mem.asBytes(&metadata_group_id));
    hasher.update(std.mem.asBytes(&@as(u64, @intCast(hosted_group_ids.len))));
    for (hosted_group_ids) |group_id| {
        hasher.update(std.mem.asBytes(&group_id));
        if (group_id == metadata_group_id) continue;
        const range = findRange(ranges, group_id) orelse continue;
        const table = findTable(tables, range.table_id) orelse continue;
        hasher.update(std.mem.asBytes(&range.group_id));
        hasher.update(std.mem.asBytes(&range.table_id));
        hashBytes(&hasher, range.start_key);
        if (range.end_key) |end_key| {
            hasher.update(&[_]u8{1});
            hashBytes(&hasher, end_key);
        } else {
            hasher.update(&[_]u8{0});
        }
        hashBytes(&hasher, range.restore_backup_id);
        hashBytes(&hasher, range.restore_location);
        hashBytes(&hasher, range.restore_snapshot_path);
        hasher.update(std.mem.asBytes(&table.table_id));
        hashBytes(&hasher, table.name);
        hashBytes(&hasher, table.schema_json);
        hashBytes(&hasher, table.read_schema_json);
        hashBytes(&hasher, table.indexes_json);
        hashBytes(&hasher, table.restore_backup_id);
        hashBytes(&hasher, table.restore_location);
    }
    return hasher.final();
}

fn hashBytes(hasher: *std.hash.Wyhash, bytes: []const u8) void {
    const len: u64 = @intCast(bytes.len);
    hasher.update(std.mem.asBytes(&len));
    hasher.update(bytes);
}

pub fn reconcileReplicaRoot(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
) !ProvisionSummary {
    return try reconcileReplicaRootWithOptions(alloc, replica_root_dir, metadata_group_id, hosted_group_ids, tables, ranges, .{});
}

pub fn reconcileReplicaRootWithOptions(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    options: ReconcileReplicaRootOptions,
) !ProvisionSummary {
    var summary: ProvisionSummary = .{};
    for (hosted_group_ids) |group_id| {
        if (group_id == metadata_group_id) continue;
        const range = findRange(ranges, group_id) orelse continue;
        const table = findTable(tables, range.table_id) orelse continue;
        summary.groups_considered += 1;

        const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);

        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), path);
        try applyRestoreIntentIfNeeded(alloc, path, group_id, table, range);

        var open_options = provisioningDbOpenOptions();
        open_options.backend_runtime = options.backend_runtime;
        var db = try db_mod.DB.open(alloc, path, open_options);
        defer db.close();
        summary.dbs_opened += 1;
        try applyTableSchemaJson(alloc, &db, table.schema_json);
        const index_summary = try reconcileDbIndexes(alloc, &db, table.indexes_json);
        summary.indexes_removed += index_summary.indexes_removed;
        summary.indexes_added += index_summary.indexes_added;
    }
    return summary;
}

fn applyTableSchemaJson(alloc: std.mem.Allocator, db: *db_mod.DB, schema_json: []const u8) !void {
    if (schema_json.len == 0) return;
    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer @import("../storage/schema.zig").freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);
}

pub fn reconcileDbIndexes(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
) !ProvisionSummary {
    const removed = try removeMissingIndexes(alloc, db, indexes_json);
    const added = try ensureIndexes(alloc, db, indexes_json);
    if (added > 0 or removed > 0) {
        const pending = db.pendingWorkStats();
        if (pending.enrichment.error_count == 0) {
            try db.core.index_manager.syncAll(false);
        }
    }
    return .{
        .groups_considered = 0,
        .dbs_opened = 0,
        .indexes_added = added,
        .indexes_removed = removed,
    };
}

pub fn collectLocalSchemaProgress(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
) ![]table_manager.SchemaProgressRecord {
    return try collectLocalSchemaProgressWithOptions(alloc, replica_root_dir, metadata_group_id, local_node_id, hosted_group_ids, tables, ranges, .{});
}

pub fn collectLocalSchemaProgressWithOptions(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    options: ReconcileReplicaRootOptions,
) ![]table_manager.SchemaProgressRecord {
    var progress_by_table = std.AutoHashMapUnmanaged(u64, TableProgressStatus).empty;
    defer progress_by_table.deinit(alloc);

    for (hosted_group_ids) |group_id| {
        if (group_id == metadata_group_id) continue;
        const range = findRange(ranges, group_id) orelse continue;
        const table = findTable(tables, range.table_id) orelse continue;
        if (table.read_schema_json.len == 0) continue;

        const version = try schemaVersion(alloc, table.schema_json);
        const read_version = try schemaVersion(alloc, table.read_schema_json);
        const ready = try localRangeHasSchemaVersionIndex(alloc, replica_root_dir, table.name, group_id, version, read_version, options);

        const gop = try progress_by_table.getOrPut(alloc, table.table_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .table_id = table.table_id,
                .node_id = local_node_id,
                .schema_version = version,
                .range_count = 1,
                .all_ready = ready,
            };
            continue;
        }

        gop.value_ptr.range_count += 1;
        gop.value_ptr.all_ready = gop.value_ptr.all_ready and ready;
        gop.value_ptr.schema_version = version;
    }

    var out = std.ArrayListUnmanaged(table_manager.SchemaProgressRecord).empty;
    errdefer out.deinit(alloc);

    var it = progress_by_table.valueIterator();
    while (it.next()) |status| {
        if (status.range_count == 0 or !status.all_ready) continue;
        try out.append(alloc, .{
            .table_id = status.table_id,
            .node_id = status.node_id,
            .schema_version = status.schema_version,
        });
    }

    std.mem.sort(table_manager.SchemaProgressRecord, out.items, {}, struct {
        fn lessThan(_: void, a: table_manager.SchemaProgressRecord, b: table_manager.SchemaProgressRecord) bool {
            if (a.table_id != b.table_id) return a.table_id < b.table_id;
            return a.node_id < b.node_id;
        }
    }.lessThan);
    return try out.toOwnedSlice(alloc);
}

pub fn collectLocalSchemaProgressFromRuntime(
    alloc: std.mem.Allocator,
    local_node_id: u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    stores: []const table_manager.StoreRecord,
) ![]table_manager.SchemaProgressRecord {
    var out = std.ArrayListUnmanaged(table_manager.SchemaProgressRecord).empty;
    errdefer out.deinit(alloc);

    for (tables) |table| {
        if (table.read_schema_json.len == 0) continue;
        const version = try schemaVersion(alloc, table.schema_json);
        const read_version = try schemaVersion(alloc, table.read_schema_json);

        var hosted_ranges: usize = 0;
        var ready_ranges: usize = 0;
        for (ranges) |range| {
            if (range.table_id != table.table_id) continue;
            const runtime = findLocalRuntimeStatus(stores, local_node_id, table.table_id, range.group_id) orelse continue;
            hosted_ranges += 1;
            if (runtimeHasReadySchemaVersionIndex(runtime, version, read_version)) ready_ranges += 1;
        }
        if (hosted_ranges == 0 or ready_ranges != hosted_ranges) continue;
        try out.append(alloc, .{
            .table_id = table.table_id,
            .node_id = local_node_id,
            .schema_version = version,
        });
    }

    std.mem.sort(table_manager.SchemaProgressRecord, out.items, {}, struct {
        fn lessThan(_: void, a: table_manager.SchemaProgressRecord, b: table_manager.SchemaProgressRecord) bool {
            if (a.table_id != b.table_id) return a.table_id < b.table_id;
            return a.node_id < b.node_id;
        }
    }.lessThan);
    return try out.toOwnedSlice(alloc);
}

pub fn collectLocalRestoreProgress(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
) ![]table_manager.RestoreProgressRecord {
    var out = std.ArrayListUnmanaged(table_manager.RestoreProgressRecord).empty;
    errdefer {
        for (out.items) |record| table_manager.freeRestoreProgress(alloc, record);
        out.deinit(alloc);
    }

    for (hosted_group_ids) |group_id| {
        if (group_id == metadata_group_id) continue;
        const range = findRange(ranges, group_id) orelse continue;
        const table = findTable(tables, range.table_id) orelse continue;
        const restore = resolveRestoreIntent(range, table) orelse continue;

        const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);
        var state = (try db_mod.DB.readRestoreStateForPath(alloc, path)) orelse continue;
        defer state.deinit(alloc);
        if (!std.mem.eql(u8, state.backup_id, restore.backup_id)) continue;
        if (!std.mem.eql(u8, state.location, restore.location)) continue;
        if (restore.snapshot_path.len > 0 and !std.mem.eql(u8, state.snapshot_path, restore.snapshot_path)) continue;
        if (state.group_id != group_id) continue;

        var record = table_manager.RestoreProgressRecord{
            .table_id = table.table_id,
            .node_id = local_node_id,
            .group_id = group_id,
            .backup_id = try alloc.dupe(u8, restore.backup_id),
            .snapshot_path = &.{},
            .primary_restored = state.primary_restored,
            .runtime_repair_complete = state.runtime_repair_complete,
            .phase = &.{},
            .last_error = &.{},
            .updated_at_ms = 0,
        };
        var appended = false;
        errdefer if (!appended) table_manager.freeRestoreProgress(alloc, record);
        record.snapshot_path = try alloc.dupe(u8, state.snapshot_path);
        record.phase = try alloc.dupe(u8, state.phase);
        record.last_error = try alloc.dupe(u8, state.last_error);
        try out.append(alloc, record);
        appended = true;
    }

    std.mem.sort(table_manager.RestoreProgressRecord, out.items, {}, struct {
        fn lessThan(_: void, a: table_manager.RestoreProgressRecord, b: table_manager.RestoreProgressRecord) bool {
            if (a.table_id != b.table_id) return a.table_id < b.table_id;
            if (a.node_id != b.node_id) return a.node_id < b.node_id;
            return a.group_id < b.group_id;
        }
    }.lessThan);
    return try out.toOwnedSlice(alloc);
}

fn applyRestoreIntentIfNeeded(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    table: table_manager.TableRecord,
    range: table_manager.RangeRecord,
) !void {
    const restore = resolveRestoreIntent(range, table) orelse return;
    try backup_restore.applyRestoreSnapshotToPathWithOptions(alloc, path, group_id, .{
        .backup_id = restore.backup_id,
        .location = restore.location,
        .snapshot_path = restore.snapshot_path,
    }, .{
        .expected_table_name = table.name,
        .expected_identity_namespace = doc_identity.Namespace{
            .table_id = table.table_id,
            .shard_id = table_manager.rangeDocIdentityShardId(range),
            .range_id = table_manager.rangeDocIdentityRangeId(range),
        },
    });
}

fn readFileAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_bytes));
}

fn resolveRestoreIntent(
    range: table_manager.RangeRecord,
    table: table_manager.TableRecord,
) ?RestoreIntentSource {
    if (range.restore_backup_id.len > 0 and range.restore_location.len > 0) {
        return .{
            .backup_id = range.restore_backup_id,
            .location = range.restore_location,
            .snapshot_path = range.restore_snapshot_path,
        };
    }
    if (table.restore_backup_id.len > 0 and table.restore_location.len > 0) {
        return .{
            .backup_id = table.restore_backup_id,
            .location = table.restore_location,
        };
    }
    return null;
}

fn removeMissingIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const current = try db.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, current);

    var removed: usize = 0;
    for (current) |cfg| {
        if (object.contains(cfg.name)) continue;
        if (try db.deleteIndex(cfg.name)) removed += 1;
    }
    return removed;
}

fn ensureIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var added: usize = 0;
    var it = object.iterator();
    while (it.next()) |entry| {
        const kind = try parseIndexKind(entry.value_ptr.*);
        if (db.core.index_manager.has(entry.key_ptr.*)) continue;

        const config_json = try extractIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*);
        defer alloc.free(config_json);
        try db.addIndex(.{
            .name = entry.key_ptr.*,
            .kind = kind,
            .config_json = config_json,
        });
        added += 1;
    }
    return added;
}

fn localRangeHasSchemaVersionIndex(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    table_name: []const u8,
    group_id: u64,
    schema_version: u32,
    read_schema_version: u32,
    options: ReconcileReplicaRootOptions,
) !bool {
    if (options.shard_db_adapter) |adapter| {
        return try adapter.schemaIndexReady(alloc, table_name, group_id, schema_version, read_schema_version);
    }

    const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);

    var open_options = provisioningDbOpenOptions();
    open_options.backend_runtime = options.backend_runtime;
    var db = try db_mod.DB.open(alloc, path, open_options);
    defer db.close();
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    var target_name_buf: [64]u8 = undefined;
    const target_name = if (schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        try std.fmt.bufPrint(&target_name_buf, "full_text_index_v{d}", .{schema_version});
    const target_index = findDbIndexStats(stats.indexes, target_name) orelse return false;
    if (!indexStatsReady(target_index)) return false;
    if (schema_version == read_schema_version) return true;

    var read_name_buf: [64]u8 = undefined;
    const read_name = if (read_schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        try std.fmt.bufPrint(&read_name_buf, "full_text_index_v{d}", .{read_schema_version});
    const read_index = findDbIndexStats(stats.indexes, read_name) orelse return true;
    if (!indexStatsReady(read_index)) return false;
    return true;
}

fn findDbIndexStats(indexes: []const db_mod.types.DBIndexStats, index_name: []const u8) ?db_mod.types.DBIndexStats {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) return index;
    }
    return null;
}

fn indexStatsReady(index: db_mod.types.DBIndexStats) bool {
    if (index.kind != .full_text) return false;
    if (index.backfill_active) return false;
    if (index.replay_catch_up_required) return false;
    if (index.replay_applied_sequence < index.replay_target_sequence) return false;
    return true;
}

fn findLocalRuntimeStatus(
    stores: []const table_manager.StoreRecord,
    local_node_id: u64,
    table_id: u64,
    group_id: u64,
) ?table_manager.RuntimeGroupStatusReport {
    for (stores) |store| {
        if (store.node_id != local_node_id) continue;
        for (store.runtime_statuses) |runtime| {
            if (runtime.table_id != table_id) continue;
            if (runtime.group_id != group_id) continue;
            if (runtime.node_id != 0 and runtime.node_id != local_node_id) continue;
            return runtime;
        }
    }
    return null;
}

fn runtimeHasReadySchemaVersionIndex(
    runtime: table_manager.RuntimeGroupStatusReport,
    schema_version: u32,
    read_schema_version: u32,
) bool {
    var target_name_buf: [64]u8 = undefined;
    const target_name = if (schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        std.fmt.bufPrint(&target_name_buf, "full_text_index_v{d}", .{schema_version}) catch return false;
    _ = findReadyRuntimeFullTextIndex(runtime.indexes, target_name) orelse return false;
    if (schema_version == read_schema_version) return true;

    var read_name_buf: [64]u8 = undefined;
    const read_name = if (read_schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        std.fmt.bufPrint(&read_name_buf, "full_text_index_v{d}", .{read_schema_version}) catch return false;
    _ = findReadyRuntimeFullTextIndex(runtime.indexes, read_name) orelse return true;
    return true;
}

fn findReadyRuntimeFullTextIndex(
    indexes: []const table_manager.RuntimeIndexStatusReport,
    index_name: []const u8,
) ?table_manager.RuntimeIndexStatusReport {
    for (indexes) |index| {
        if (!std.mem.eql(u8, index.name, index_name)) continue;
        if (!std.mem.eql(u8, index.kind, "full_text")) return null;
        if (index.backfill_active) return null;
        if (index.replay_catch_up_required) return null;
        if (index.replay_applied_sequence < index.replay_target_sequence) return null;
        return index;
    }
    return null;
}

fn schemaVersion(alloc: std.mem.Allocator, schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableSchema,
    };
    const version_value = object.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| blk: {
            if (value < 0) return error.InvalidTableSchema;
            break :blk std.math.cast(u32, value) orelse return error.InvalidTableSchema;
        },
        else => return error.InvalidTableSchema,
    };
}

fn parseIndexKind(value: std.json.Value) !db_mod.types.IndexKind {
    if (value != .object) return .full_text;
    const type_value = value.object.get("type") orelse return .full_text;
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
    if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
    if (std.mem.eql(u8, type_value.string, "embeddings")) {
        const sparse = if (value.object.get("sparse")) |sparse_value| switch (sparse_value) {
            .bool => sparse_value.bool,
            else => return error.InvalidCreateTableRequest,
        } else false;
        return if (sparse) .sparse_vector else .dense_vector;
    }
    return error.UnsupportedCreateTableRequest;
}

fn extractIndexConfigJson(alloc: std.mem.Allocator, index_name: []const u8, value: std.json.Value) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    switch (try parseIndexKind(value)) {
        .dense_vector, .sparse_vector => return try managed_embedder.translateEmbeddingsIndexConfigJson(alloc, index_name, value),
        else => {},
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "type") or
            std.mem.eql(u8, entry.key_ptr.*, "name") or
            std.mem.eql(u8, entry.key_ptr.*, "description") or
            std.mem.eql(u8, entry.key_ptr.*, "version") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments"))
        {
            continue;
        }
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn findRange(ranges: []const table_manager.RangeRecord, group_id: u64) ?table_manager.RangeRecord {
    for (ranges) |record| {
        if (record.group_id == group_id) return record;
    }
    return null;
}

fn findTable(tables: []const table_manager.TableRecord, table_id: u64) ?table_manager.TableRecord {
    for (tables) |record| {
        if (record.table_id == table_id) return record;
    }
    return null;
}

test "table provisioner fingerprint changes with hosted index metadata" {
    const base = provisioningFingerprint(
        100,
        &.{ 100, 2001 },
        &.{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        }},
        &.{.{
            .group_id = 2001,
            .table_id = 7,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );
    const changed_index = provisioningFingerprint(
        100,
        &.{ 100, 2001 },
        &.{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"embed_idx\":{\"type\":\"dense_vector\"}}",
        }},
        &.{.{
            .group_id = 2001,
            .table_id = 7,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );
    const changed_group = provisioningFingerprint(
        100,
        &.{ 100, 2002 },
        &.{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        }},
        &.{.{
            .group_id = 2002,
            .table_id = 7,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );

    try std.testing.expect(base != changed_index);
    try std.testing.expect(base != changed_group);
}

test "table provisioner materializes metadata indexes into hosted group dbs" {
    const path = "/tmp/antfly-metadata-table-provisioner";
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const summary = try reconcileReplicaRoot(
        std.testing.allocator,
        path,
        100,
        &.{ 100, 2001 },
        &.{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        }},
        &.{.{
            .group_id = 2001,
            .table_id = 7,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );
    try std.testing.expectEqual(@as(usize, 1), summary.groups_considered);
    try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
    try std.testing.expectEqual(@as(usize, 1), summary.indexes_added);
    try std.testing.expectEqual(@as(usize, 0), summary.indexes_removed);

    const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2001);
    defer std.testing.allocator.free(db_path);
    var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
    defer db.close();
    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);
}

test "table provisioner restores local shard data from metadata restore intent" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/table-provisioner-restore-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/table-provisioner-restore-backup", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);
    const source_db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/table-provisioner-restore-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_db_path);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};

    var source_db = try db_mod.DB.open(std.testing.allocator, source_db_path, .{});
    defer {
        source_db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }
    try source_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
        .sync_level = .full_index,
    });
    _ = try source_db.snapshot("snap1-g2001");

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, "{s}.snapshots/snap1-g2001", .{source_db_path});
    defer std.testing.allocator.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(std.testing.allocator, backup_root, "snap1", 2001);
    defer std.testing.allocator.free(dest_root);
    try backups_api.copyDirectoryRecursive(std.testing.allocator, snapshot_root, dest_root);
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(std.testing.allocator, &.{ cwd, backup_root });
    defer std.testing.allocator.free(backup_root_abs);
    const restore_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root_abs});
    defer std.testing.allocator.free(restore_location);

    const manifest = try backups_api.createManifest(
        std.testing.allocator,
        "snap1",
        &.{
            .table_id = 7,
            .name = "docs",
            .description = "docs table",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        },
        &.{.{
            .group_id = 2001,
            .start_key = "doc:a",
            .end_key = null,
            .snapshot_path = "snap1/groups/2001",
        }},
    );
    defer {
        var owned = manifest;
        owned.deinit(std.testing.allocator);
    }
    try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

    const summary = try reconcileReplicaRoot(
        std.testing.allocator,
        replica_root,
        100,
        &.{ 100, 2001 },
        &.{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .restore_backup_id = "snap1",
            .restore_location = restore_location,
        }},
        &.{.{
            .group_id = 2001,
            .table_id = 7,
            .start_key = "doc:a",
            .end_key = null,
        }},
    );
    try std.testing.expectEqual(@as(usize, 1), summary.groups_considered);

    const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, 2001);
    defer std.testing.allocator.free(db_path);
    var restored_db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
    defer restored_db.close();
    const doc = (try restored_db.get(std.testing.allocator, "doc:a")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(doc);
    try std.testing.expect(std.mem.indexOf(u8, doc, "\"alpha\"") != null);

    const FakeCatalog = struct {
        restore_location: []const u8,

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
                .status = .{ .metadata_group_id = 100, .metrics = .{} },
                .tables = @constCast((&[_]table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
                    .restore_backup_id = "snap1",
                    .restore_location = self.restore_location,
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]table_manager.RangeRecord{.{
                    .group_id = 2001,
                    .table_id = 7,
                    .start_key = "doc:a",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]@import("transition_state.zig").SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]@import("transition_state.zig").MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var fake_catalog = FakeCatalog{ .restore_location = restore_location };
    var read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        fake_catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );
    var lookup = (try read_source.source().lookup(std.testing.allocator, "docs", "doc:a", .{}, .read_index)).?;
    defer lookup.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "\"alpha\"") != null);

    var scan = (try read_source.source().scan(std.testing.allocator, "docs", "", "", .{
        .limit = 10,
        .include_documents = true,
    }, .read_index)).?;
    defer scan.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "\"alpha\"") != null);
}

test "table provisioner restore rejects mismatched doc identity namespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/table-provisioner-restore-docid-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/table-provisioner-restore-docid-backup", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);
    const source_db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/table-provisioner-restore-docid-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_db_path);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};
    }

    const source_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 2001, .range_id = 97001 };
    {
        var source_db = try db_mod.DB.open(std.testing.allocator, source_db_path, .{
            .identity_namespace = source_namespace,
        });
        defer source_db.close();
        try source_db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .timestamp_ns = 1,
            .sync_level = .full_index,
        });
        _ = try source_db.snapshot("snap1-g2001");
    }

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, "{s}.snapshots/snap1-g2001", .{source_db_path});
    defer std.testing.allocator.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(std.testing.allocator, backup_root, "snap1", 2001);
    defer std.testing.allocator.free(dest_root);
    try backups_api.copyDirectoryRecursive(std.testing.allocator, snapshot_root, dest_root);
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(std.testing.allocator, &.{ cwd, backup_root });
    defer std.testing.allocator.free(backup_root_abs);
    const restore_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root_abs});
    defer std.testing.allocator.free(restore_location);

    const manifest = try backups_api.createManifest(
        std.testing.allocator,
        "snap1",
        &.{
            .table_id = 7,
            .name = "docs",
            .description = "docs table",
            .indexes_json = tables_api.default_indexes_json,
            .placement_role = "data",
        },
        &.{.{
            .group_id = 2001,
            .start_key = "doc:a",
            .end_key = null,
            .snapshot_path = "snap1/groups/2001",
        }},
    );
    defer {
        var owned = manifest;
        owned.deinit(std.testing.allocator);
    }
    try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

    try std.testing.expectError(error.IdentityNamespaceMismatch, reconcileReplicaRoot(
        std.testing.allocator,
        replica_root,
        100,
        &.{ 100, 2001 },
        &.{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = tables_api.default_indexes_json,
            .restore_backup_id = "snap1",
            .restore_location = restore_location,
            .placement_role = "data",
        }},
        &.{.{
            .group_id = 2001,
            .table_id = 7,
            .start_key = "doc:a",
            .end_key = null,
            .range_id = 2001,
        }},
    ));
}

test "table provisioner removes indexes missing from metadata" {
    const path = "/tmp/antfly-metadata-table-provisioner-drop";
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2002);
    defer std.testing.allocator.free(db_path);
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.addIndex(.{ .name = "embed_idx", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}" });

    const summary = try reconcileReplicaRoot(
        std.testing.allocator,
        path,
        100,
        &.{ 100, 2002 },
        &.{.{
            .table_id = 8,
            .name = "docs",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        }},
        &.{.{
            .group_id = 2002,
            .table_id = 8,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );
    try std.testing.expectEqual(@as(usize, 1), summary.indexes_removed);
    try std.testing.expectEqual(@as(usize, 0), summary.indexes_added);

    var reopened = try db_mod.DB.open(std.testing.allocator, db_path, .{});
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.textIndex("full_text_index_v0") != null);
    try std.testing.expect(reopened.core.index_manager.denseIndex("embed_idx") == null);
}

test "table provisioner reconcile does not replay pending derived batches" {
    const path = "/tmp/antfly-metadata-table-provisioner-no-replay";
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2006);
    defer std.testing.allocator.free(db_path);
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{
            .start_index_workers = false,
        });
        defer db.close();
        try db.addIndex(.{
            .name = "embed_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });
        const stored_key = try db_mod.internal_keys.documentKeyAlloc(std.testing.allocator, "doc:a");
        defer std.testing.allocator.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(std.testing.allocator, "doc:a", "embed_idx");
        defer std.testing.allocator.free(artifact_key);
        const payload = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(std.testing.allocator, null, &[_]f32{ 1, 0 });
        defer std.testing.allocator.free(payload);
        try db.core.store.putBatch(&.{
            .{ .key = artifact_key, .value = payload },
        }, &.{});

        var dense_embeddings = try std.testing.allocator.alloc(db_mod.derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = db_mod.derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer db_mod.derived_types.deinitDerivedBatch(std.testing.allocator, &batch);
        dense_embeddings[0] = .{
            .index_name = try std.testing.allocator.dupe(u8, "embed_idx"),
            .doc_key = try std.testing.allocator.dupe(u8, "doc:a"),
            .artifact_key = try std.testing.allocator.dupe(u8, artifact_key),
            .vector = try std.testing.allocator.dupe(f32, &[_]f32{ 1, 0 }),
        };

        const sequence = db.core.store.nextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(std.testing.allocator, batch, sequence);
        defer change_journal_mod.deinitRecord(std.testing.allocator, &record);
        const encoded = try change_journal_mod.encodeRecord(std.testing.allocator, record);
        defer std.testing.allocator.free(encoded);
        try db.core.store.appendReplayOpaque(std.testing.allocator, sequence, encoded);
    }

    const summary = try reconcileReplicaRoot(
        std.testing.allocator,
        path,
        100,
        &.{ 100, 2006 },
        &.{.{
            .table_id = 11,
            .name = "docs",
            .indexes_json = "{\"embed_idx\":{\"type\":\"embeddings\",\"field\":\"embedding\",\"dims\":2}}",
        }},
        &.{.{
            .group_id = 2006,
            .table_id = 11,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );
    try std.testing.expectEqual(@as(usize, 1), summary.groups_considered);
    try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);

    {
        var reopened_without_replay = try db_mod.DB.open(std.testing.allocator, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
        });
        defer reopened_without_replay.close();
        const skipped_applied = try reopened_without_replay.core.loadAppliedSequence(std.testing.allocator, "embed_idx");
        try std.testing.expectEqual(@as(u64, 0), skipped_applied);

        var skipped_result = try reopened_without_replay.search(std.testing.allocator, .{
            .index_name = "embed_idx",
            .dense = .{
                .vector = &[_]f32{ 1, 0 },
                .k = 1,
            },
            .limit = 1,
        });
        defer skipped_result.deinit();
        try std.testing.expectEqual(@as(u32, 0), skipped_result.total_hits);
    }

    var reopened = try db_mod.DB.open(std.testing.allocator, db_path, .{
        .start_index_workers = false,
    });
    defer reopened.close();
    const applied = try reopened.core.loadAppliedSequence(std.testing.allocator, "embed_idx");
    try std.testing.expect(applied > 0);
}

test "table provisioner reports local schema progress once all local shards have the target full-text index" {
    const path = "/tmp/antfly-metadata-table-provisioner-progress";
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    _ = try reconcileReplicaRoot(
        std.testing.allocator,
        path,
        100,
        &.{ 100, 2003 },
        &.{.{
            .table_id = 9,
            .name = "docs",
            .schema_json = "{\"version\":1}",
            .read_schema_json = "{\"version\":0}",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
        }},
        &.{.{
            .group_id = 2003,
            .table_id = 9,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );

    const progress = try collectLocalSchemaProgress(
        std.testing.allocator,
        path,
        100,
        7,
        &.{ 100, 2003 },
        &.{.{
            .table_id = 9,
            .name = "docs",
            .schema_json = "{\"version\":1}",
            .read_schema_json = "{\"version\":0}",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
        }},
        &.{.{
            .group_id = 2003,
            .table_id = 9,
            .start_key = "doc:a",
            .end_key = "doc:z",
        }},
    );
    defer std.testing.allocator.free(progress);

    try std.testing.expectEqual(@as(usize, 1), progress.len);
    try std.testing.expectEqual(@as(u64, 9), progress[0].table_id);
    try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
    try std.testing.expectEqual(@as(u32, 1), progress[0].schema_version);
}

test "table provisioner withholds schema progress when any local shard is missing the target full-text index" {
    const path = "/tmp/antfly-metadata-table-provisioner-progress-incomplete";
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path_a = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2004);
    defer std.testing.allocator.free(db_path_a);
    try fs_paths.createDirPathPortable(io_impl.io(), db_path_a);
    var db_a = try db_mod.DB.open(std.testing.allocator, db_path_a, .{});
    defer db_a.close();
    try db_a.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db_a.addIndex(.{ .name = "full_text_index_v1", .kind = .full_text, .config_json = "{}" });

    const db_path_b = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2005);
    defer std.testing.allocator.free(db_path_b);
    try fs_paths.createDirPathPortable(io_impl.io(), db_path_b);
    var db_b = try db_mod.DB.open(std.testing.allocator, db_path_b, .{});
    defer db_b.close();
    try db_b.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });

    const progress = try collectLocalSchemaProgress(
        std.testing.allocator,
        path,
        100,
        7,
        &.{ 100, 2004, 2005 },
        &.{.{
            .table_id = 10,
            .name = "docs",
            .schema_json = "{\"version\":1}",
            .read_schema_json = "{\"version\":0}",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
        }},
        &.{
            .{
                .group_id = 2004,
                .table_id = 10,
                .start_key = "doc:a",
                .end_key = "doc:m",
            },
            .{
                .group_id = 2005,
                .table_id = 10,
                .start_key = "doc:m",
                .end_key = "doc:z",
            },
        },
    );
    defer std.testing.allocator.free(progress);

    try std.testing.expectEqual(@as(usize, 0), progress.len);
}

test "table provisioner accepts target schema index when retained read index has inflated doc count" {
    const indexes = [_]table_manager.RuntimeIndexStatusReport{
        .{
            .name = "full_text_index_v0",
            .kind = "full_text",
            .doc_count = 2000,
            .replay_applied_sequence = 7,
            .replay_target_sequence = 7,
        },
        .{
            .name = "full_text_index_v1",
            .kind = "full_text",
            .doc_count = 1000,
            .replay_applied_sequence = 7,
            .replay_target_sequence = 7,
        },
    };
    try std.testing.expect(runtimeHasReadySchemaVersionIndex(.{
        .doc_count = 1000,
        .indexes = @constCast(indexes[0..]),
    }, 1, 0));
}
