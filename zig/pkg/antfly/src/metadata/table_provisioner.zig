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
const storage_source_options = @import("storage_source_options");
const control_only_storage_sources = storage_source_options.control_only;
const stored_destination_authorization = @import("../api/stored_destination_authorization.zig");
const backups_api = @import("../api/backups.zig");
const common_config = @import("../common/config.zig");
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const metadata_api = @import("api.zig");
const table_manager = @import("table_manager.zig");
const raft_catalog = @import("../raft/storage/catalog.zig");
const backup_restore = @import("../raft/storage/backup_restore.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/selected_root.zig").db;
const change_journal_mod = @import("../storage/db/derived/change_journal.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const coverage_policy = @import("../api/coverage_policy.zig");
const table_index_config = @import("../api/table_index_config.zig");
const indexes_api = @import("../api/indexes.zig");
const enrichment_config_validation = @import("../storage/db/enrichment/config_validation.zig");
const table_reads = @import("antfly_source_root").antfly_sources.table_reads;
const table_catalog = @import("../api/table_catalog.zig");
const tables_api = @import("../api/tables.zig");
const raft_mod = @import("../raft/mod.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const shard_db_adapter_mod = @import("shard_db_adapter.zig");
const doc_identity = @import("../storage/db/doc_identity.zig");
const restore_state_contract = @import("../storage/restore_state_contract.zig");

pub const ProvisionSummary = @import("antfly_provision_contract").ProvisionSummary;

pub const ReconcileReplicaRootOptions = struct {
    drain_resolver_backfill: bool = true,
    io: std.Io = std.Options.debug_io,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    shard_db_adapter: ?shard_db_adapter_mod.ShardDbAdapter = null,
    restore_open_options: backups_api.OpenOptions = .{},
    embedding_options: managed_embedder.InitOptions = .{},
    destination_authorizer: ?stored_destination_authorization.Authorizer = null,
};

pub const RestoreProgressOptions = struct {
    shared_io: ?std.Io = null,
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
    artifact_backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8 = "",
    connection: []const u8 = "",
    artifact_size_bytes: u64 = 0,
    artifact_sha256: []const u8 = "",
    native_manifest_size_bytes: u64 = 0,
    native_manifest_sha256: []const u8 = "",
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
    return try applyBackupRestoreBootstrapWithOptions(alloc, replica_root_dir, group_id, restore, .{});
}

pub fn applyBackupRestoreBootstrapWithOptions(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: raft_catalog.BackupRestoreBootstrapRecord,
    open_options: backups_api.OpenOptions,
) !void {
    try backup_restore.applyBackupRestoreFromRecordWithOptions(
        alloc,
        replica_root_dir,
        group_id,
        restore,
        open_options,
    );
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
        hasher.update(std.mem.asBytes(&range.range_id));
        hasher.update(std.mem.asBytes(&range.table_id));
        hasher.update(std.mem.asBytes(&range.doc_identity_shard_id));
        hasher.update(std.mem.asBytes(&range.doc_identity_range_id));
        hasher.update(std.mem.asBytes(&range.split_attempt_epoch));
        hashBytes(&hasher, range.start_key);
        if (range.end_key) |end_key| {
            hasher.update(&[_]u8{1});
            hashBytes(&hasher, end_key);
        } else {
            hasher.update(&[_]u8{0});
        }
        hashBytes(&hasher, range.restore_backup_id);
        hashBytes(&hasher, range.restore_artifact_backup_id);
        hashBytes(&hasher, range.restore_location);
        hashBytes(&hasher, range.restore_snapshot_path);
        hashBytes(&hasher, range.restore_connection);
        hasher.update(std.mem.asBytes(&range.restore_artifact_size_bytes));
        hashBytes(&hasher, range.restore_artifact_sha256);
        hasher.update(std.mem.asBytes(&range.restore_native_manifest_size_bytes));
        hashBytes(&hasher, range.restore_native_manifest_sha256);
        hasher.update(&range.completed_restore_fingerprint);
        hasher.update(std.mem.asBytes(&table.table_id));
        hashBytes(&hasher, table.name);
        if (table.storage.dense_embeddings != .primary_lsm) hashBytes(&hasher, @tagName(table.storage.dense_embeddings));
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

        const io = if (options.backend_runtime) |runtime|
            runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable
        else
            options.io;
        try fs_paths.createDirPathPortable(io, path);
        var restore_open_options = options.restore_open_options;
        if (restore_open_options.filesystem_io == null) restore_open_options.filesystem_io = io;
        try applyRestoreIntentIfNeededWithRuntime(
            alloc,
            path,
            group_id,
            table,
            range,
            restore_open_options,
            options.backend_runtime,
        );

        const schema_json = tables_api.effectiveSchemaJson(table.schema_json);
        const runtime_schema = try runtimeTableSchemaFromJson(alloc, schema_json);
        defer @import("../storage/schema.zig").freeSchema(alloc, runtime_schema);
        var open_options = provisioningDbOpenOptions();
        open_options.start_resolver_workers = options.drain_resolver_backfill;
        open_options.backend_runtime = options.backend_runtime;
        open_options.schema_before_index_load = .{
            .runtime_schema = runtime_schema,
            .public_schema_json = schema_json,
        };
        open_options.table_storage = table.storage;
        var db = try db_mod.DB.open(alloc, path, open_options);
        defer db.close();
        summary.dbs_opened += 1;
        const index_summary = try reconcileDbIndexesWithOptions(alloc, &db, table.indexes_json, .{
            .drain_resolver_backfill = options.drain_resolver_backfill,
            .embedding_options = options.embedding_options,
            .source_table = table.name,
            .destination_authorizer = options.destination_authorizer,
        });
        summary.merge(.{
            .indexes_added = index_summary.indexes_added,
            .indexes_removed = index_summary.indexes_removed,
            .indexes_pending = index_summary.indexes_pending,
            .enrichments_added = index_summary.enrichments_added,
            .enrichments_updated = index_summary.enrichments_updated,
            .enrichments_removed = index_summary.enrichments_removed,
            .resolvers_added = index_summary.resolvers_added,
            .resolvers_updated = index_summary.resolvers_updated,
            .resolvers_removed = index_summary.resolvers_removed,
        });
    }
    return summary;
}

fn runtimeTableSchemaFromJson(alloc: std.mem.Allocator, schema_json: []const u8) !@import("../storage/schema.zig").TableSchema {
    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    return try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
}

pub fn reconcileDbIndexes(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
) !IndexReconcileSummary {
    return try reconcileDbIndexesWithOptions(alloc, db, indexes_json, .{});
}

pub const IndexReconcileSummary = @import("local_index_reconcile.zig").IndexReconcileSummary;

pub const ReconcileDbIndexOptions = @import("local_index_reconcile.zig").ReconcileDbIndexOptions;

const dbIndexReconciliationCanMutate = @import("local_index_reconcile.zig").dbIndexReconciliationCanMutate;

pub const reconcileDbIndexesWithOptions = @import("local_index_reconcile.zig").reconcileDbIndexesWithOptions;

pub fn reconcileDbIndexTarget(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    index_name: []const u8,
) !IndexReconcileSummary {
    return try reconcileDbIndexTargetWithOptions(alloc, db, indexes_json, index_name, .{});
}

pub const reconcileDbIndexTargetWithOptions = @import("local_index_reconcile.zig").reconcileDbIndexTargetWithOptions;

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
        const target_full_text = try hasVersionedFullTextIndex(alloc, table.indexes_json, version);
        const ready = localRangeHasSchemaVersionIndex(
            alloc,
            replica_root_dir,
            table.name,
            range,
            version,
            read_version,
            target_full_text,
            options,
        ) catch |err| switch (err) {
            // Schema progress is observational. A generation publication can
            // acquire the DB between the durable-marker check and DB.open;
            // report that shard as not ready and observe it next round.
            error.GenerationTransitionActive,
            error.InvalidRebuildState,
            error.FileNotFound,
            error.NotDir,
            => false,
            else => return err,
        };

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

/// Compare current ready observations with replicated acknowledgements, not a
/// process-local sent bit. Failed/ambiguous delivery and metadata restore are
/// repaired naturally by the next captured snapshot.
pub fn schemaProgressDelta(alloc: std.mem.Allocator, ready: []const table_manager.SchemaProgressRecord, acknowledged: []const table_manager.SchemaProgressRecord) ![]table_manager.SchemaProgressRecord {
    var known: std.AutoHashMapUnmanaged(struct { table_id: u64, node_id: u64 }, u32) = .empty;
    defer known.deinit(alloc);
    for (acknowledged) |record| try known.put(alloc, .{ .table_id = record.table_id, .node_id = record.node_id }, record.schema_version);
    var out: std.ArrayListUnmanaged(table_manager.SchemaProgressRecord) = .empty;
    errdefer out.deinit(alloc);
    for (ready) |record| {
        const version = known.get(.{ .table_id = record.table_id, .node_id = record.node_id });
        if (version != null and version.? == record.schema_version) continue;
        try out.append(alloc, record);
    }
    return out.toOwnedSlice(alloc);
}

pub fn collectLocalSchemaProgressFromRuntime(
    alloc: std.mem.Allocator,
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    stores: []const table_manager.StoreRecord,
) ![]table_manager.SchemaProgressRecord {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const State = struct { version: u32, read_version: u32, target_full_text: bool, hosted: usize = 0, ready: bool = true };
    var states: std.AutoHashMapUnmanaged(u64, State) = .empty;
    for (tables) |table| {
        if (table.read_schema_json.len == 0) continue;
        const version = try schemaVersion(alloc, table.schema_json);
        try states.put(a, table.table_id, .{
            .version = version,
            .read_version = try schemaVersion(alloc, table.read_schema_json),
            .target_full_text = try hasVersionedFullTextIndex(a, table.indexes_json, version),
        });
    }
    if (states.count() == 0) return alloc.alloc(table_manager.SchemaProgressRecord, 0);
    var ranges_by_group: std.AutoHashMapUnmanaged(u64, table_manager.RangeRecord) = .empty;
    for (ranges) |range| {
        const entry = try ranges_by_group.getOrPut(a, range.group_id);
        if (!entry.found_existing) entry.value_ptr.* = range;
    }
    var runtimes: std.AutoHashMapUnmanaged(struct { table_id: u64, group_id: u64 }, table_manager.RuntimeGroupStatusReport) = .empty;
    for (stores) |store| {
        if (store.node_id != local_node_id) continue;
        for (store.runtime_statuses) |runtime| {
            if (runtime.node_id != 0 and runtime.node_id != local_node_id) continue;
            const entry = try runtimes.getOrPut(a, .{ .table_id = runtime.table_id, .group_id = runtime.group_id });
            if (!entry.found_existing) entry.value_ptr.* = runtime;
        }
    }
    for (hosted_group_ids) |group_id| {
        const range = ranges_by_group.get(group_id) orelse continue;
        const state = states.getPtr(range.table_id) orelse continue;
        state.hosted += 1;
        const runtime = runtimes.get(.{ .table_id = range.table_id, .group_id = group_id }) orelse {
            state.ready = false;
            continue;
        };
        state.ready = state.ready and runtimeHasReadySchemaVersionIndex(runtime, range, state.version, state.read_version, state.target_full_text);
    }
    var out: std.ArrayListUnmanaged(table_manager.SchemaProgressRecord) = .empty;
    errdefer out.deinit(alloc);
    var entries = states.iterator();
    while (entries.next()) |entry| {
        if (entry.value_ptr.hosted != 0 and entry.value_ptr.ready) try out.append(alloc, .{
            .table_id = entry.key_ptr.*,
            .node_id = local_node_id,
            .schema_version = entry.value_ptr.version,
        });
    }
    std.mem.sort(table_manager.SchemaProgressRecord, out.items, {}, struct {
        fn lessThan(_: void, x: table_manager.SchemaProgressRecord, y: table_manager.SchemaProgressRecord) bool {
            return x.table_id < y.table_id or (x.table_id == y.table_id and x.node_id < y.node_id);
        }
    }.lessThan);
    return out.toOwnedSlice(alloc);
}

// Benchmark oracle for the former repeated-scan collector.
fn collectLocalSchemaProgressReference(
    alloc: std.mem.Allocator,
    local_node_id: u64,
    hosted_group_ids: []const u64,
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
        const target_full_text = try hasVersionedFullTextIndex(alloc, table.indexes_json, version);

        var hosted_ranges: usize = 0;
        var all_ready = true;
        for (hosted_group_ids) |group_id| {
            const range = findRange(ranges, group_id) orelse continue;
            if (range.table_id != table.table_id) continue;
            hosted_ranges += 1;
            const runtime = findLocalRuntimeStatus(stores, local_node_id, table.table_id, group_id) orelse {
                all_ready = false;
                continue;
            };
            all_ready = all_ready and runtimeHasReadySchemaVersionIndex(runtime, range, version, read_version, target_full_text);
        }
        if (hosted_ranges == 0 or !all_ready) continue;
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

/// Whether projected runtime observations cover every locally hosted range
/// participating in a schema migration. An explicit opening/catching-up
/// observation is coverage even though it is not ready: falling back to a
/// filesystem DB reopen in that state duplicates the live owner's work and
/// cannot make the migration ready sooner.
pub fn localSchemaRuntimeCoverageComplete(
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    stores: []const table_manager.StoreRecord,
) bool {
    var saw_migrating_group = false;
    for (hosted_group_ids) |group_id| {
        const range = findRange(ranges, group_id) orelse continue;
        const table = findTable(tables, range.table_id) orelse continue;
        if (table.read_schema_json.len == 0) continue;
        saw_migrating_group = true;
        _ = findLocalRuntimeStatus(stores, local_node_id, table.table_id, group_id) orelse return false;
    }
    return saw_migrating_group;
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
    return try collectLocalRestoreProgressWithOptions(
        alloc,
        replica_root_dir,
        metadata_group_id,
        local_node_id,
        hosted_group_ids,
        tables,
        ranges,
        .{},
    );
}

pub fn collectLocalRestoreProgressUsingIo(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
) ![]table_manager.RestoreProgressRecord {
    return try collectLocalRestoreProgressWithOptions(
        alloc,
        replica_root_dir,
        metadata_group_id,
        local_node_id,
        hosted_group_ids,
        tables,
        ranges,
        .{ .shared_io = shared_io },
    );
}

pub fn collectLocalRestoreProgressWithOptions(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    options: RestoreProgressOptions,
) ![]table_manager.RestoreProgressRecord {
    if (options.shard_db_adapter != null or comptime control_only_storage_sources) {
        return try collectLocalRestoreProgressFromSources(
            alloc,
            replica_root_dir,
            metadata_group_id,
            local_node_id,
            hosted_group_ids,
            tables,
            ranges,
            options,
        );
    }
    if (options.shared_io != null) {
        return try collectLocalRestoreProgressFromSources(
            alloc,
            replica_root_dir,
            metadata_group_id,
            local_node_id,
            hosted_group_ids,
            tables,
            ranges,
            options,
        );
    }
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var threaded_options = options;
    threaded_options.shared_io = io_impl.io();
    return try collectLocalRestoreProgressFromSources(
        alloc,
        replica_root_dir,
        metadata_group_id,
        local_node_id,
        hosted_group_ids,
        tables,
        ranges,
        threaded_options,
    );
}

fn collectLocalRestoreProgressFromSources(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    local_node_id: u64,
    hosted_group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    options: RestoreProgressOptions,
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

        var state: restore_state_contract.State = if (options.shard_db_adapter) |adapter|
            (try adapter.restoreState(alloc, table.name, group_id)) orelse continue
        else state: {
            if (comptime control_only_storage_sources) return error.StorageKernelOwnerUnavailable;
            const io = options.shared_io orelse return error.StorageKernelOwnerUnavailable;
            const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
            defer alloc.free(path);
            var physical = (try db_mod.DB.readRestoreStateForPathWithIo(alloc, io, path)) orelse continue;
            defer physical.deinit(alloc);
            break :state try (restore_state_contract.State{
                .backup_id = physical.backup_id,
                .location = physical.location,
                .artifact_sha256 = physical.artifact_sha256,
                .native_manifest_size_bytes = physical.native_manifest_size_bytes,
                .native_manifest_sha256 = physical.native_manifest_sha256,
                .snapshot_path = physical.snapshot_path,
                .group_id = physical.group_id,
                .phase = physical.phase,
                .primary_restored = physical.primary_restored,
                .runtime_repair_complete = physical.runtime_repair_complete,
                .last_error = physical.last_error,
            }).cloneAlloc(alloc);
        };
        defer state.deinit(alloc);
        if (!std.mem.eql(u8, state.backup_id, restore.backup_id)) continue;
        if (!std.mem.eql(u8, state.location, restore.location)) continue;
        if (!std.mem.eql(u8, state.artifact_sha256, restore.artifact_sha256)) continue;
        if (state.native_manifest_size_bytes != restore.native_manifest_size_bytes) continue;
        if (!std.mem.eql(u8, state.native_manifest_sha256, restore.native_manifest_sha256)) continue;
        if (restore.snapshot_path.len > 0 and !std.mem.eql(u8, state.snapshot_path, restore.snapshot_path)) continue;
        if (state.group_id != group_id) continue;

        var record: table_manager.RestoreProgressRecord = blk: {
            const progress_backup_id = try alloc.dupe(u8, restore.backup_id);
            errdefer alloc.free(progress_backup_id);
            const progress_artifact_backup_id = try alloc.dupe(u8, restore.artifact_backup_id);
            errdefer alloc.free(progress_artifact_backup_id);
            const progress_location = try alloc.dupe(u8, restore.location);
            errdefer alloc.free(progress_location);
            break :blk .{
                .table_id = table.table_id,
                .node_id = local_node_id,
                .group_id = group_id,
                .backup_id = progress_backup_id,
                .artifact_backup_id = progress_artifact_backup_id,
                .location = progress_location,
                .snapshot_path = &.{},
                .artifact_sha256 = &.{},
                .native_manifest_size_bytes = state.native_manifest_size_bytes,
                .native_manifest_sha256 = &.{},
                .primary_restored = state.primary_restored,
                .runtime_repair_complete = state.runtime_repair_complete,
                .phase = &.{},
                .last_error = &.{},
                .updated_at_ms = 0,
            };
        };
        var appended = false;
        errdefer if (!appended) table_manager.freeRestoreProgress(alloc, record);
        record.snapshot_path = try alloc.dupe(u8, state.snapshot_path);
        record.artifact_sha256 = try alloc.dupe(u8, state.artifact_sha256);
        record.native_manifest_sha256 = try alloc.dupe(u8, state.native_manifest_sha256);
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

pub fn applyRestoreIntentIfNeeded(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    table: table_manager.TableRecord,
    range: table_manager.RangeRecord,
) !void {
    return try applyRestoreIntentIfNeededWithOptions(alloc, path, group_id, table, range, .{});
}

pub fn applyRestoreIntentIfNeededWithOptions(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    table: table_manager.TableRecord,
    range: table_manager.RangeRecord,
    open_options: backups_api.OpenOptions,
) !void {
    return try applyRestoreIntentIfNeededWithRuntime(
        alloc,
        path,
        group_id,
        table,
        range,
        open_options,
        null,
    );
}

pub fn applyRestoreIntentIfNeededWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    table: table_manager.TableRecord,
    range: table_manager.RangeRecord,
    open_options: backups_api.OpenOptions,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !void {
    const restore = resolveRestoreIntent(range, table) orelse return;
    try backup_restore.applyRestoreSnapshotToPathWithOptions(alloc, path, group_id, .{
        .backup_id = restore.backup_id,
        .artifact_backup_id = restore.artifact_backup_id,
        .location = restore.location,
        .snapshot_path = restore.snapshot_path,
        .authority = .{ .external = restore.connection },
        .expected_artifact_size_bytes = restore.artifact_size_bytes,
        .expected_artifact_sha256 = restore.artifact_sha256,
        .expected_native_manifest_size_bytes = restore.native_manifest_size_bytes,
        .expected_native_manifest_sha256 = restore.native_manifest_sha256,
        .open_options = open_options,
        .backend_runtime = backend_runtime,
    }, .{
        .expected_table_name = table.name,
        .expected_identity_namespace = doc_identity.Namespace{
            .table_id = table.table_id,
            .shard_id = table_manager.rangeDocIdentityShardId(range),
            .range_id = table_manager.rangeDocIdentityRangeId(range),
        },
    });
}

fn resolveRestoreIntent(
    range: table_manager.RangeRecord,
    table: table_manager.TableRecord,
) ?RestoreIntentSource {
    if (range.restore_backup_id.len > 0 and range.restore_location.len > 0) {
        return .{
            .backup_id = range.restore_backup_id,
            .artifact_backup_id = range.restore_artifact_backup_id,
            .location = range.restore_location,
            .snapshot_path = range.restore_snapshot_path,
            .connection = range.restore_connection,
            .artifact_size_bytes = range.restore_artifact_size_bytes,
            .artifact_sha256 = range.restore_artifact_sha256,
            .native_manifest_size_bytes = range.restore_native_manifest_size_bytes,
            .native_manifest_sha256 = range.restore_native_manifest_sha256,
        };
    }
    _ = table;
    return null;
}

const removeMissingIndexes = @import("local_index_reconcile.zig").removeMissingIndexes;

const IndexEnsureSummary = @import("local_index_reconcile.zig").IndexEnsureSummary;

const ensureIndexes = @import("local_index_reconcile.zig").ensureIndexes;

const ensureIndexDefinition = @import("local_index_reconcile.zig").ensureIndexDefinition;

const desiredIndexContains = @import("local_index_reconcile.zig").desiredIndexContains;

const indexDefinitionName = @import("local_index_reconcile.zig").indexDefinitionName;

const indexDefinitionConfigValue = @import("local_index_reconcile.zig").indexDefinitionConfigValue;

const findIndexConfig = @import("local_index_reconcile.zig").findIndexConfig;

const indexConfigsEqual = @import("local_index_reconcile.zig").indexConfigsEqual;

const indexKindConfigReconcileDeferred = @import("local_index_reconcile.zig").indexKindConfigReconcileDeferred;

const fullTextIndexConfigsEqual = @import("local_index_reconcile.zig").fullTextIndexConfigsEqual;

const algebraicIndexConfigsEqual = @import("local_index_reconcile.zig").algebraicIndexConfigsEqual;

const jsonValuesEqualIgnoringTopLevelEnrichments = @import("local_index_reconcile.zig").jsonValuesEqualIgnoringTopLevelEnrichments;

const collectDesiredEnrichmentsFromJson = @import("local_index_reconcile.zig").collectDesiredEnrichmentsFromJson;

const EnrichmentEnsureSummary = @import("local_index_reconcile.zig").EnrichmentEnsureSummary;

const ensureEnrichments = @import("local_index_reconcile.zig").ensureEnrichments;

const dedupeDesiredEnrichments = @import("local_index_reconcile.zig").dedupeDesiredEnrichments;

const removeMissingEnrichments = @import("local_index_reconcile.zig").removeMissingEnrichments;

const removeAbsentEnrichments = @import("local_index_reconcile.zig").removeAbsentEnrichments;

const findEnrichmentByName = @import("local_index_reconcile.zig").findEnrichmentByName;

fn findEnrichment(
    configs: []const db_mod.types.EnrichmentConfig,
    kind: db_mod.types.EnrichmentKind,
    name: []const u8,
) ?db_mod.types.EnrichmentConfig {
    for (configs) |cfg| {
        if (cfg.kind == kind and std.mem.eql(u8, cfg.name, name)) return cfg;
    }
    return null;
}

const enrichmentConfigsEqual = @import("local_index_reconcile.zig").enrichmentConfigsEqual;

pub const ResolverReconcileSummary = @import("local_index_reconcile.zig").ResolverReconcileSummary;

pub fn ensureResolvers(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !ResolverReconcileSummary {
    return try ensureResolversWithOptions(alloc, db, indexes_json, .{});
}

pub const EnsureResolverOptions = @import("local_index_reconcile.zig").EnsureResolverOptions;

pub const ensureResolversWithOptions = @import("local_index_reconcile.zig").ensureResolversWithOptions;

const desiredResolverContains = @import("local_index_reconcile.zig").desiredResolverContains;

const collectDesiredResolvers = @import("local_index_reconcile.zig").collectDesiredResolvers;

fn localRangeHasSchemaVersionIndex(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    table_name: []const u8,
    range: table_manager.RangeRecord,
    schema_version: u32,
    read_schema_version: u32,
    target_full_text: bool,
    options: ReconcileReplicaRootOptions,
) !bool {
    const group_id = range.group_id;
    if (options.shard_db_adapter) |adapter| {
        // The older adapter contract proves only a full-text index. An
        // indexless owner must publish the V18 epoch/identity observation;
        // do not turn an unavailable observation into filesystem authority.
        if (!target_full_text) return false;
        return try adapter.schemaIndexReady(alloc, table_name, group_id, schema_version, read_schema_version);
    }
    if (comptime control_only_storage_sources) {
        return error.StorageKernelOwnerUnavailable;
    }

    const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);

    var target_name_buf: [64]u8 = undefined;
    const target_name = if (schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        try std.fmt.bufPrint(&target_name_buf, "full_text_index_v{d}", .{schema_version});

    // Preserve the cheap pre-generation marker fast path during rolling
    // upgrades. Generation-owned markers are resolved by the status-only open
    // below: its catalog identity distinguishes the current cursor from a
    // stale same-name worker without mapping retained index state.
    const rebuild_state_root = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ path, target_name });
    defer alloc.free(rebuild_state_root);
    const rebuild_state = db_mod.backfill_state.RebuildState.init(rebuild_state_root);
    if (try rebuild_state.check(alloc)) |resume_key| {
        alloc.free(resume_key);
        return false;
    }

    var open_options = provisioningDbOpenOptions();
    // Marker disappearance is followed by one catalog-only verification.
    // Status probes do not execute queries and must not load/mmap full-text
    // segment data merely to inspect persisted readiness metadata.
    open_options.open_mode = .status_only;
    open_options.backend_runtime = options.backend_runtime;
    var db = try db_mod.DB.open(alloc, path, open_options);
    defer db.close();
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    if (!target_full_text) {
        const identity = stats.doc_identity;
        return stats.schema_epoch != 0 and stats.schema_epoch == schema_version and
            identity.namespace_table_id == range.table_id and
            identity.namespace_shard_id == table_manager.rangeDocIdentityShardId(range) and
            identity.namespace_range_id == table_manager.rangeDocIdentityRangeId(range) and
            identity.next_ordinal != 0 and
            identity.next_ordinal - 1 == identity.allocated_ordinals and
            !identity.rebuild_required and !identity.ordinal_capacity_exhausted and
            (std.math.add(u64, identity.live_ordinals, identity.tombstone_ordinals) catch return false) == identity.allocated_ordinals;
    }

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
    if (!index.repair_summary_ready or index.repair_degraded) return false;
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
    range: table_manager.RangeRecord,
    schema_version: u32,
    read_schema_version: u32,
    target_full_text: bool,
) bool {
    // Schema cutover must be driven by a current observation of the complete
    // target projection. A catalog-only placeholder and a newly-created empty
    // index both look idle, but neither proves that existing documents were
    // rebuilt under the target schema. Identity mutations and their visibility
    // summary commit atomically with every primary mutation, so the reconciled
    // O(1) summary is the generation-owner proof. Requiring diagnostic
    // `complete` here would turn migration progress into a corpus scan and let
    // a later bounded status observation erase an otherwise valid proof.
    if (!std.mem.eql(u8, runtime.freshness, "fresh")) return false;
    if (!runtimeIdentitySummaryIsAuthoritative(runtime, range)) return false;

    var target_name_buf: [64]u8 = undefined;
    const target_name = if (schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        std.fmt.bufPrint(&target_name_buf, "full_text_index_v{d}", .{schema_version}) catch return false;
    if (target_full_text) {
        const target = findReadyRuntimeFullTextIndex(runtime.indexes, target_name) orelse return false;
        // A chunk/artifact-sourced full-text index routes member documents
        // (e.g. chunks) into the same index, so its doc_count is primary rows
        // plus members and can never equal live_ordinals once a chunk
        // enrichment targets it. The equality check historically existed only
        // to reject a freshly-created or catalog-only placeholder index that
        // looks idle before its own rebuild starts; findReadyRuntimeFullTextIndex
        // above already requires backfill_active=false and replay caught up,
        // which is the real proof that this incarnation's rebuild finished. A
        // placeholder that has not started yet still reports doc_count=0 and
        // is rejected by this bound; a 1:1 (no member documents) index still
        // satisfies it at equality.
        if (target.doc_count < runtime.doc_identity.live_ordinals) return false;
    } else {
        // An explicitly indexless relational table has no full-text rebuild
        // to wait for. Only a fresh owner observation of the exact applied
        // immutable schema epoch may retire the previous read schema. Older
        // wire profiles report zero and therefore fail closed here.
        if (runtime.schema_epoch == 0 or runtime.schema_epoch != schema_version or
            !runtime.target_observation_complete) return false;
    }
    if (schema_version == read_schema_version) return true;

    var read_name_buf: [64]u8 = undefined;
    const read_name = if (read_schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        std.fmt.bufPrint(&read_name_buf, "full_text_index_v{d}", .{read_schema_version}) catch return false;
    _ = findReadyRuntimeFullTextIndex(runtime.indexes, read_name) orelse return true;
    return true;
}

fn hasVersionedFullTextIndex(alloc: std.mem.Allocator, indexes_json: []const u8, version: u32) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    var name_buf: [64]u8 = undefined;
    const name = if (version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        try std.fmt.bufPrint(&name_buf, "full_text_index_v{d}", .{version});
    return try desiredIndexContains(object, name);
}

/// Only an active migration without the usual versioned full-text target
/// needs the V18 owner-epoch proof. Ordinary store heartbeats stay on their
/// negotiated predecessor profile during rolling upgrades.
pub fn indexlessSchemaEpochRequired(alloc: std.mem.Allocator, table: table_manager.TableRecord) !bool {
    if (table.read_schema_json.len == 0) return false;
    return !try hasVersionedFullTextIndex(alloc, table.indexes_json, try schemaVersion(alloc, table.schema_json));
}

fn runtimeIdentitySummaryIsAuthoritative(
    runtime: table_manager.RuntimeGroupStatusReport,
    range: table_manager.RangeRecord,
) bool {
    const identity = runtime.doc_identity;
    if (identity.ordinal_capacity_exhausted or identity.rebuild_required) return false;
    if (runtime.table_id != range.table_id or runtime.group_id != range.group_id) return false;
    if (identity.namespace_table_id != range.table_id or
        identity.namespace_shard_id != table_manager.rangeDocIdentityShardId(range) or
        identity.namespace_range_id != table_manager.rangeDocIdentityRangeId(range)) return false;
    if (identity.next_ordinal == 0) return false;
    if (@as(u64, identity.next_ordinal - 1) != identity.allocated_ordinals) return false;
    const accounted = std.math.add(u64, identity.live_ordinals, identity.tombstone_ordinals) catch return false;
    if (accounted != identity.allocated_ordinals) return false;

    return true;
}

fn findReadyRuntimeFullTextIndex(
    indexes: []const table_manager.RuntimeIndexStatusReport,
    index_name: []const u8,
) ?table_manager.RuntimeIndexStatusReport {
    for (indexes) |index| {
        if (!std.mem.eql(u8, index.name, index_name)) continue;
        if (!std.mem.eql(u8, index.kind, "full_text")) return null;
        if (index.load_error != null) return null;
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

const parseIndexKind = @import("local_index_reconcile.zig").parseIndexKind;

const embeddingIndexSparseFlag = @import("local_index_reconcile.zig").embeddingIndexSparseFlag;

const looksLikeStoredAlgebraicIndexConfig = @import("local_index_reconcile.zig").looksLikeStoredAlgebraicIndexConfig;

fn extractIndexConfigJson(alloc: std.mem.Allocator, index_name: []const u8, value: std.json.Value) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    const kind = try parseIndexKind(value);
    return try extractIndexConfigJsonForKind(alloc, index_name, kind, value);
}

const extractIndexConfigJsonForKind = @import("local_index_reconcile.zig").extractIndexConfigJsonForKind;

const extractStoredIndexConfigJson = @import("local_index_reconcile.zig").extractStoredIndexConfigJson;

const skipPublicIndexMetadataField = @import("local_index_reconcile.zig").skipPublicIndexMetadataField;

const appendJsonString = @import("local_index_reconcile.zig").appendJsonString;

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

fn testProvisionedFullTextBackfill(inject_activation_deferral: bool) !void {
    var path_tmp = try @import("../common/test_directory.zig").TestDirectory.initFast("backfill");
    defer path_tmp.cleanup();
    const path = path_tmp.path();
    const platform = @import("antfly_platform");
    var clock = platform.clock.ManualClock{ .now_realtime_ns = 10 * std.time.ns_per_s };

    var db = try db_mod.DB.open(std.testing.allocator, path, .{
        .start_index_workers = false,
        .index_repair_clock = clock.clock(),
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    const indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}";
    const first = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, indexes_json, .{});
    try std.testing.expectEqual(@as(usize, 1), first.indexes_added);
    try std.testing.expectEqual(@as(usize, 1), first.indexes_pending);
    try std.testing.expect(try db.hasPendingIndexRepairIntents(std.testing.allocator));

    var initial_state = try db.loadIndexRepairState(std.testing.allocator);
    defer initial_state.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), initial_state.entries.items.len);
    const repair_id = initial_state.entries.items[0].intent.repair_id;
    try std.testing.expectEqualStrings("full_text_index_v1", initial_state.entries.items[0].intent.index_name);

    // Reconciliation is an additional recovery trigger. Re-observing the
    // admitted empty projection must adopt the same durable generation instead
    // of dropping or duplicating it.
    const second = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, indexes_json, .{});
    try std.testing.expectEqual(@as(usize, 0), second.indexes_added);
    try std.testing.expectEqual(@as(usize, 1), second.indexes_pending);
    var repeated_state = try db.loadIndexRepairState(std.testing.allocator);
    defer repeated_state.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), repeated_state.entries.items.len);
    try std.testing.expectEqual(repair_id, repeated_state.entries.items[0].intent.repair_id);

    const Deferral = struct {
        fired: bool = false,

        fn afterSnapshot(_: *anyopaque, _: *db_mod.DB, _: []const u8, _: u64) !void {}

        fn afterPhase(ptr: *anyopaque, _: *db_mod.DB, _: u128, phase: @import("../storage/db/derived/index_repair_state.zig").Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (phase == .activating and !self.fired) {
                self.fired = true;
                // The production activation deadline returns this same error.
                // Inject it once without depending on machine speed or sleeps.
                return error.ShadowIndexCatchUpIncomplete;
            }
        }
    };
    var deferral = Deferral{};
    if (inject_activation_deferral) db.shadow_index_repair_hook = .{
        .ptr = &deferral,
        .after_snapshot_build = Deferral.afterSnapshot,
        .after_phase_persisted = Deferral.afterPhase,
    };
    defer db.shadow_index_repair_hook = null;

    // This is a functional durability test, not the production 250 ms reader
    // pause SLA. Match the storage repair tests' activation headroom, but still
    // honor any persisted retry instead of spending a fixed number of spins.
    // Bound state-machine work independently of scheduler speed.
    const max_steps = 32;
    var repaired = false;
    var observed_retry = false;
    defer if (!repaired) {
        var state = db.loadIndexRepairState(std.testing.allocator) catch null;
        if (state) |*snapshot| {
            defer snapshot.deinit(std.testing.allocator);
            for (snapshot.entries.items) |entry| {
                if (entry.intent.repair_id != repair_id) continue;
                std.debug.print("backfill repair did not complete: phase={s} source_replay={s} attempts={d} next_retry_at_ms={d} last_error={?s}\n", .{
                    @tagName(entry.intent.phase),  @tagName(entry.intent.source_replay_state), entry.intent.attempt_count,
                    entry.intent.next_retry_at_ms, entry.intent.last_error,
                });
            }
        }
    };
    for (0..max_steps) |_| {
        const step = try db.advanceIndexRepairIntent(std.testing.allocator, repair_id, .{
            .max_activation_pause_ms = 5_000,
        });
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
        const now_ms = clock.clock().nowRealtimeMs();
        const retry_delay_ms = step.next_retry_at_ms -| now_ms;
        if (retry_delay_ms != 0) {
            observed_retry = true;
            var pending_state = try db.loadIndexRepairState(std.testing.allocator);
            defer pending_state.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(usize, 1), pending_state.entries.items.len);
            const pending = pending_state.entries.items[0];
            try std.testing.expectEqual(repair_id, pending.intent.repair_id);
            try std.testing.expectEqual(step.next_retry_at_ms, pending.intent.next_retry_at_ms);
            // A retry must retain the admitted generation and its checkpoint.
            if (deferral.fired) {
                try std.testing.expect(pending.intent.candidate_relative_path != null);
                try std.testing.expect(pending.intent.last_error != null);
                try std.testing.expectEqualStrings("repair_attempt_incomplete", pending.intent.last_error.?);
                // Status and the scheduler must classify backoff against the
                // same clock that wrote the durable retry deadline.
                const stats = try db.stats(std.testing.allocator);
                defer db_mod.types.freeDBStats(std.testing.allocator, stats);
                const index_stats = findDbIndexStats(stats.indexes, "full_text_index_v1") orelse return error.TestExpectedEqual;
                try std.testing.expectEqualStrings("backoff", index_stats.index_repair_wait_reason);
                try std.testing.expectEqual(step.next_retry_at_ms, index_stats.index_repair_next_retry_at_ms);
                // Advancing before the retry time must neither attempt work
                // nor lose the persisted retry schedule.
                const early = try db.advanceIndexRepairIntent(std.testing.allocator, repair_id, .{});
                try std.testing.expect(early.deferred);
                try std.testing.expect(!early.attempted);
                try std.testing.expectEqual(step.next_retry_at_ms, early.next_retry_at_ms);
            }
        }
        if (retry_delay_ms != 0) {
            // The durable deadline is exclusive: one millisecond early still
            // defers, while the next turn at the deadline may attempt work.
            clock.setRealtimeNs((step.next_retry_at_ms - 1) * std.time.ns_per_ms);
            const early = try db.advanceIndexRepairIntent(std.testing.allocator, repair_id, .{});
            try std.testing.expect(early.deferred);
            try std.testing.expect(!early.attempted);
            try std.testing.expectEqual(step.next_retry_at_ms, early.next_retry_at_ms);
            clock.advanceMs(1);
        }
    }
    try std.testing.expect(repaired);
    try std.testing.expectEqual(inject_activation_deferral, deferral.fired);
    if (inject_activation_deferral) try std.testing.expect(observed_retry);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(std.testing.allocator));

    const complete = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, indexes_json, .{});
    try std.testing.expectEqual(@as(usize, 0), complete.indexes_added);
    try std.testing.expectEqual(@as(usize, 0), complete.indexes_pending);
}

fn testRestoreNodeConfig(alloc: std.mem.Allocator) !common_config.Config {
    return common_config.Config.parseFromSlice(alloc,
        \\{
        \\  "connections": {
        \\    "test-backups": {
        \\      "kind": "external_io",
        \\      "capabilities": ["restore.read"],
        \\      "external_io": { "protocol": "filesystem", "root": "/" }
        \\    }
        \\  }
        \\}
    );
}

pub const implementation_tests = implementationTests();
fn implementationTests() type {
    if (!@import("builtin").is_test or @import("storage_source_options").control_only) return struct {};
    const Suite = struct {
        test "schema progress runtime coverage treats opening observations as authoritative without hiding missing groups" {
            const tables = [_]table_manager.TableRecord{.{
                .table_id = 11,
                .name = "docs",
                .schema_json = "{\"version\":1}",
                .read_schema_json = "{\"version\":0}",
            }};
            const ranges = [_]table_manager.RangeRecord{
                .{ .group_id = 7, .table_id = 11, .start_key = "", .end_key = "m" },
                .{ .group_id = 8, .table_id = 11, .start_key = "m" },
            };
            const hosted = [_]u64{ 7, 8 };
            var runtimes = [_]table_manager.RuntimeGroupStatusReport{
                .{ .table_id = 11, .group_id = 7, .node_id = 3, .source = "startup_catch_up", .freshness = "opening" },
                .{ .table_id = 11, .group_id = 8, .node_id = 3, .source = "startup_catch_up", .freshness = "opening" },
            };
            var stores = [_]table_manager.StoreRecord{.{
                .store_id = 5,
                .node_id = 3,
                .runtime_statuses = runtimes[0..1],
            }};

            try std.testing.expect(!localSchemaRuntimeCoverageComplete(3, &hosted, &tables, &ranges, &stores));
            stores[0].runtime_statuses = &runtimes;
            try std.testing.expect(localSchemaRuntimeCoverageComplete(3, &hosted, &tables, &ranges, &stores));
            try std.testing.expect(!localSchemaRuntimeCoverageComplete(4, &hosted, &tables, &ranges, &stores));
        }

        test "system catalog schema progress indexed collector and acknowledged delta workload" {
            const a = std.testing.allocator;
            const benchmark = std.c.getenv("ANTFLY_CATALOG_REPORT_BENCH") != null;
            const table_count: usize = if (benchmark) 100 else 4;
            const count: usize = if (benchmark) 2000 else 16;
            const tables = try a.alloc(table_manager.TableRecord, table_count);
            defer a.free(tables);
            for (tables, 0..) |*table, i| table.* = .{ .table_id = i + 1, .name = "tenant", .schema_json = "{\"version\":1}", .read_schema_json = "{\"version\":0}", .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}" };
            const ranges = try a.alloc(table_manager.RangeRecord, count);
            defer a.free(ranges);
            const hosted = try a.alloc(u64, count);
            defer a.free(hosted);
            const runtimes = try a.alloc(table_manager.RuntimeGroupStatusReport, count);
            defer a.free(runtimes);
            var indexes = [_]table_manager.RuntimeIndexStatusReport{.{ .name = "full_text_index_v1", .kind = "full_text", .doc_count = 1 }};
            for (ranges, hosted, runtimes, 0..) |*range, *group, *runtime, i| {
                const table_id = i % table_count + 1;
                group.* = i + 100;
                range.* = .{ .table_id = table_id, .group_id = group.*, .start_key = "" };
                runtime.* = .{ .table_id = table_id, .group_id = group.*, .node_id = 3, .freshness = "fresh", .indexes = &indexes, .doc_identity = .{ .namespace_table_id = table_id, .namespace_shard_id = group.*, .namespace_range_id = group.*, .next_ordinal = 2, .allocated_ordinals = 1, .live_ordinals = 1 } };
            }
            const stores = [_]table_manager.StoreRecord{.{ .store_id = 5, .node_id = 3, .runtime_statuses = runtimes }};
            var reference_ns: [5]u64 = undefined;
            var indexed_ns: [5]u64 = undefined;
            for (0..if (benchmark) @as(usize, 6) else 1) |sample| {
                const start = @import("antfly_platform").time.monotonicNs();
                const reference = try collectLocalSchemaProgressReference(a, 3, hosted, tables, ranges, &stores);
                defer a.free(reference);
                const middle = @import("antfly_platform").time.monotonicNs();
                const ready = try collectLocalSchemaProgressFromRuntime(a, 3, hosted, tables, ranges, &stores);
                defer a.free(ready);
                const end = @import("antfly_platform").time.monotonicNs();
                try std.testing.expectEqualDeep(reference, ready);
                try std.testing.expectEqual(table_count, ready.len);
                const quiet = try schemaProgressDelta(a, ready, ready);
                defer a.free(quiet);
                try std.testing.expectEqual(@as(usize, 0), quiet.len);
                const restored = try schemaProgressDelta(a, ready, &.{});
                defer a.free(restored);
                try std.testing.expectEqualDeep(ready, restored);
                // A stale version or a different reporter is never an acknowledgement.
                const stale = [_]table_manager.SchemaProgressRecord{
                    .{ .table_id = 1, .node_id = 3, .schema_version = 0 },
                    .{ .table_id = 2, .node_id = 4, .schema_version = 1 },
                };
                const retry = try schemaProgressDelta(a, ready, &stale);
                defer a.free(retry);
                try std.testing.expectEqualDeep(ready, retry);
                if (benchmark and sample > 0) {
                    reference_ns[sample - 1] = middle - start;
                    indexed_ns[sample - 1] = end - middle;
                }
            }
            // Missing observations and non-authoritative observations both withhold cutover.
            runtimes[0].freshness = "opening";
            const partial = try collectLocalSchemaProgressFromRuntime(a, 3, hosted, tables, ranges, &stores);
            defer a.free(partial);
            const partial_reference = try collectLocalSchemaProgressReference(a, 3, hosted, tables, ranges, &stores);
            defer a.free(partial_reference);
            try std.testing.expectEqualDeep(partial_reference, partial);
            try std.testing.expectEqual(table_count - 1, partial.len);
            if (benchmark) std.debug.print("SCHEMA_PROGRESS_BENCH tables={d} groups={d} nested_ns={any} indexed_ns={any} first_proposals={d} acknowledged_proposals=0\n", .{ table_count, count, reference_ns, indexed_ns, (table_count + table_manager.max_schema_progress_batch - 1) / table_manager.max_schema_progress_batch });
        }

        test "runtime schema progress requires every hosted range" {
            const tables = [_]table_manager.TableRecord{.{
                .table_id = 11,
                .name = "docs",
                .schema_json = "{\"version\":1}",
                .read_schema_json = "{\"version\":0}",
                .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
            }};
            const ranges = [_]table_manager.RangeRecord{
                .{ .group_id = 7, .table_id = 11, .start_key = "", .end_key = "m" },
                .{ .group_id = 8, .table_id = 11, .start_key = "m" },
            };
            const hosted = [_]u64{ 7, 8 };
            var indexes = [_]table_manager.RuntimeIndexStatusReport{.{
                .name = "full_text_index_v1",
                .kind = "full_text",
                .doc_count = 1,
                .replay_applied_sequence = 1,
                .replay_target_sequence = 1,
            }};
            var runtimes = [_]table_manager.RuntimeGroupStatusReport{
                .{
                    .table_id = 11,
                    .group_id = 7,
                    .node_id = 3,
                    .freshness = "fresh",
                    .doc_identity = .{
                        .namespace_table_id = 11,
                        .namespace_shard_id = 7,
                        .namespace_range_id = 7,
                        .next_ordinal = 2,
                        .allocated_ordinals = 1,
                        .live_ordinals = 1,
                    },
                    .indexes = &indexes,
                },
                .{
                    .table_id = 11,
                    .group_id = 8,
                    .node_id = 3,
                    .freshness = "fresh",
                    .doc_identity = .{
                        .namespace_table_id = 11,
                        .namespace_shard_id = 8,
                        .namespace_range_id = 8,
                        .next_ordinal = 2,
                        .allocated_ordinals = 1,
                        .live_ordinals = 1,
                    },
                    .indexes = &indexes,
                },
            };
            var stores = [_]table_manager.StoreRecord{.{
                .store_id = 5,
                .node_id = 3,
                .runtime_statuses = runtimes[0..1],
            }};

            const partial = try collectLocalSchemaProgressFromRuntime(
                std.testing.allocator,
                3,
                &hosted,
                &tables,
                &ranges,
                &stores,
            );
            defer std.testing.allocator.free(partial);
            try std.testing.expectEqual(@as(usize, 0), partial.len);

            stores[0].runtime_statuses = &runtimes;
            const complete = try collectLocalSchemaProgressFromRuntime(
                std.testing.allocator,
                3,
                &hosted,
                &tables,
                &ranges,
                &stores,
            );
            defer std.testing.allocator.free(complete);
            try std.testing.expectEqual(@as(usize, 1), complete.len);
            try std.testing.expectEqual(@as(u32, 1), complete[0].schema_version);
        }

        test "table provisioner reads restore progress through shard owner adapter" {
            const RestoreStateAdapter = struct {
                called: bool = false,

                fn fetchMedianKey(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
                    return null;
                }

                fn schemaIndexReady(
                    _: *anyopaque,
                    _: std.mem.Allocator,
                    _: []const u8,
                    _: u64,
                    _: u32,
                    _: u32,
                ) !bool {
                    return false;
                }

                fn restoreState(
                    ptr: *anyopaque,
                    alloc: std.mem.Allocator,
                    table_name: []const u8,
                    group_id: u64,
                ) !?restore_state_contract.State {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqualStrings("docs", table_name);
                    try std.testing.expectEqual(@as(u64, 2001), group_id);
                    self.called = true;
                    return try (restore_state_contract.State{
                        .backup_id = "backup-1",
                        .location = "s3://backups/backup-1",
                        .artifact_sha256 = "abc123",
                        .snapshot_path = "backup-1/groups/2001",
                        .group_id = group_id,
                        .phase = "runtime_repaired",
                        .primary_restored = true,
                        .runtime_repair_complete = true,
                        .last_error = "",
                    }).cloneAlloc(alloc);
                }

                fn adapter(self: *@This()) shard_db_adapter_mod.ShardDbAdapter {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .fetch_median_key = fetchMedianKey,
                            .schema_index_ready = schemaIndexReady,
                            .restore_state = restoreState,
                        },
                    };
                }
            };

            var source: RestoreStateAdapter = .{};
            const progress = try collectLocalRestoreProgressWithOptions(
                std.testing.allocator,
                "/unused/control-unit-must-not-open-this-path",
                100,
                7,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 9,
                    .name = "docs",
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 9,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                    .restore_backup_id = "backup-1",
                    .restore_artifact_backup_id = "backup-1",
                    .restore_location = "s3://backups/backup-1",
                    .restore_snapshot_path = "backup-1/groups/2001",
                    .restore_artifact_sha256 = "abc123",
                }},
                .{ .shard_db_adapter = source.adapter() },
            );
            defer {
                for (progress) |record| table_manager.freeRestoreProgress(std.testing.allocator, record);
                std.testing.allocator.free(progress);
            }

            try std.testing.expect(source.called);
            try std.testing.expectEqual(@as(usize, 1), progress.len);
            try std.testing.expectEqual(@as(u64, 9), progress[0].table_id);
            try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
            try std.testing.expectEqual(@as(u64, 2001), progress[0].group_id);
            try std.testing.expect(progress[0].primary_restored);
            try std.testing.expect(progress[0].runtime_repair_complete);
            try std.testing.expectEqualStrings("runtime_repaired", progress[0].phase);
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
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/table-provisioner", .{tmp.sub_path});
            defer std.testing.allocator.free(path);

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
            const schema_json = (try db.getSchemaJson(std.testing.allocator)) orelse
                return error.TestExpectedLocalTableManifest;
            defer std.testing.allocator.free(schema_json);
            try std.testing.expectEqualStrings(tables_api.default_schema_json, schema_json);
        }

        test "table provisioner materializes array-form metadata indexes" {
            const path = "/tmp/antfly-metadata-table-provisioner-array-indexes";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            var db = try db_mod.DB.open(std.testing.allocator, path, .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();

            const indexes_json =
                \\{"indexes":[
                \\  {"name":"dense_idx","type":"embeddings","config":{"field":"embedding","dims":3,"metric":"l2_squared","external":true}},
                \\  {"name":"sparse_idx","type":"embeddings","config":{"field":"tokens","sparse":true}},
                \\  {"name":"full_text_index_v0","type":"full_text","config":{}}
                \\]}
            ;
            const summary = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, indexes_json, .{});
            try std.testing.expectEqual(@as(usize, 3), summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 0), summary.indexes_removed);

            const configs = try db.listIndexes(std.testing.allocator);
            defer db_mod.types.freeIndexConfigs(std.testing.allocator, configs);
            try std.testing.expect(findIndexConfig(configs, "dense_idx").?.kind == .dense_vector);
            try std.testing.expect(findIndexConfig(configs, "sparse_idx").?.kind == .sparse_vector);
            try std.testing.expect(findIndexConfig(configs, "full_text_index_v0").?.kind == .full_text);
        }

        test "target index reconciliation never mutates sibling indexes" {
            const path = "/tmp/antfly-metadata-table-provisioner-target-index";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            var db = try db_mod.DB.open(std.testing.allocator, path, .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();

            _ = try reconcileDbIndexes(std.testing.allocator, &db,
                \\{"keep_idx":{"type":"full_text"}}
            );
            const desired =
                \\{"target_idx":{"type":"full_text"},"unrelated_new":{"type":"full_text"}}
            ;
            const admitted = try reconcileDbIndexTarget(std.testing.allocator, &db, desired, "target_idx");
            try std.testing.expectEqual(@as(usize, 1), admitted.indexes_added);

            var configs = try db.listIndexes(std.testing.allocator);
            try std.testing.expect(findIndexConfig(configs, "keep_idx") != null);
            try std.testing.expect(findIndexConfig(configs, "target_idx") != null);
            try std.testing.expect(findIndexConfig(configs, "unrelated_new") == null);
            db_mod.types.freeIndexConfigs(std.testing.allocator, configs);

            const removed = try reconcileDbIndexTarget(std.testing.allocator, &db, desired, "keep_idx");
            try std.testing.expectEqual(@as(usize, 1), removed.indexes_removed);
            configs = try db.listIndexes(std.testing.allocator);
            defer db_mod.types.freeIndexConfigs(std.testing.allocator, configs);
            try std.testing.expect(findIndexConfig(configs, "keep_idx") == null);
            try std.testing.expect(findIndexConfig(configs, "target_idx") != null);
            try std.testing.expect(findIndexConfig(configs, "unrelated_new") == null);
        }

        test "target index reconciliation retires orphaned inline enrichments after deletion retry" {
            const alloc = std.testing.allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-target-enrichment-delete";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            var db = try db_mod.DB.open(alloc, path, .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();

            const initial =
                \\{
                \\  "target_idx":{"type":"full_text","artifact_name":"target_chunks","enrichments":[
                \\    {"name":"target_units","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"},
                \\    {"name":"target_chunks","kind":"chunk","source_artifact_name":"target_units","field":"text","chunk_size":128}
                \\  ]},
                \\  "keep_idx":{"type":"full_text","artifact_name":"keep_chunks","enrichments":[
                \\    {"name":"keep_units","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"},
                \\    {"name":"keep_chunks","kind":"chunk","source_artifact_name":"keep_units","field":"text","chunk_size":128}
                \\  ]}
                \\}
            ;
            _ = try reconcileDbIndexes(alloc, &db, initial);
            try std.testing.expect(try db.deleteIndex("target_idx"));

            // Simulate a crash after the index catalog commit but before its inline
            // enrichments were retired. The retry has no old index config to inspect,
            // so cleanup must be derived from complete current catalog ownership.
            const desired =
                \\{"keep_idx":{"type":"full_text","artifact_name":"keep_chunks","enrichments":[
                \\  {"name":"keep_units","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"},
                \\  {"name":"keep_chunks","kind":"chunk","source_artifact_name":"keep_units","field":"text","chunk_size":128}
                \\]}}
            ;
            const summary = try reconcileDbIndexTarget(alloc, &db, desired, "target_idx");
            try std.testing.expectEqual(@as(usize, 0), summary.indexes_removed);
            try std.testing.expectEqual(@as(usize, 2), summary.enrichments_removed);

            const enrichments = try db.listEnrichments(alloc);
            defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
            try std.testing.expectEqual(@as(usize, 2), enrichments.len);
            try std.testing.expect(findEnrichment(enrichments, .asset, "keep_units") != null);
            try std.testing.expect(findEnrichment(enrichments, .chunk, "keep_chunks") != null);
            try std.testing.expect(findEnrichment(enrichments, .asset, "target_units") == null);
            try std.testing.expect(findEnrichment(enrichments, .chunk, "target_chunks") == null);
        }

        test "table provisioner durably enqueues existing corpus full text backfill" {
            try testProvisionedFullTextBackfill(false);
        }

        test "table provisioner full text backfill resumes after durable activation deferral" {
            try testProvisionedFullTextBackfill(true);
        }

        test "table provisioner replaces embedding index when metadata incarnation changes" {
            const path = "/tmp/antfly-metadata-table-provisioner-coverage-incarnation";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            var db = try db_mod.DB.open(std.testing.allocator, path, .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();

            const first =
                \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"openai","model":"text-embedding-3-small","url":"http://127.0.0.1:1"},"_coverage_incarnation":41}}
            ;
            const second =
                \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"openai","model":"text-embedding-3-small","url":"http://127.0.0.1:1"},"_coverage_incarnation":42}}
            ;
            const initial = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, first, .{});
            try std.testing.expectEqual(@as(usize, 1), initial.indexes_added);

            const retired = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, second, .{});
            try std.testing.expectEqual(@as(usize, 1), retired.indexes_removed);
            try std.testing.expectEqual(@as(usize, 0), retired.indexes_added);
            try std.testing.expectEqual(@as(usize, 1), retired.indexes_pending);

            // Replacement is intentionally a multi-pass desired-state transition:
            // the durable owner retires the old artifact namespace before a later
            // reconcile admits the new coverage incarnation.
            var cleanup_io = std.Io.Threaded.init(std.testing.allocator, .{});
            defer cleanup_io.deinit();
            var cleanup_pages: usize = 0;
            var cleanup_attempts: usize = 0;
            while (true) {
                cleanup_attempts += 1;
                try std.testing.expect(cleanup_attempts < 5_000);
                switch (try db.advanceGeneratedArtifactCleanupPage("semantic_idx")) {
                    .idle => break,
                    .progressed => {
                        cleanup_pages += 1;
                        try std.testing.expect(cleanup_pages < 32);
                    },
                    .busy => cleanup_io.io().sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch {},
                }
            }
            const admitted = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, second, .{});
            try std.testing.expectEqual(@as(usize, 0), admitted.indexes_removed);
            try std.testing.expectEqual(@as(usize, 1), admitted.indexes_added);
            try std.testing.expectEqual(@as(usize, 0), admitted.indexes_pending);

            const configs = try db.listIndexes(std.testing.allocator);
            defer db_mod.types.freeIndexConfigs(std.testing.allocator, configs);
            try std.testing.expectEqual(@as(u64, 42), findIndexConfig(configs, "semantic_idx").?.coverage_generation);
        }

        test "table provisioner reconciliation is non-mutating for query read-only dbs" {
            const path = "/tmp/antfly-metadata-table-provisioner-readonly-reconcile";
            const indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            {
                var writer = try db_mod.DB.open(std.testing.allocator, path, .{
                    .start_index_workers = false,
                    .ttl_cleanup = .{ .enabled = false },
                });
                defer writer.close();
            }

            {
                var reader = try db_mod.DB.open(std.testing.allocator, path, .{
                    .open_mode = .query_readonly,
                    .start_index_workers = false,
                    .ttl_cleanup = .{ .enabled = false },
                });
                defer reader.close();

                const summary = try reconcileDbIndexesWithOptions(std.testing.allocator, &reader, indexes_json, .{});
                try std.testing.expect(!summary.indexManagerCatalogChanged());
                try std.testing.expect(reader.core.textIndex("full_text_index_v0") == null);
                try std.testing.expectError(error.ReadOnly, reader.addIndex(.{
                    .name = "full_text_index_v0",
                    .kind = .full_text,
                    .config_json = "{}",
                }));
                try std.testing.expect(reader.core.textIndex("full_text_index_v0") == null);
            }

            var writer = try db_mod.DB.open(std.testing.allocator, path, .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer writer.close();
            const summary = try reconcileDbIndexesWithOptions(std.testing.allocator, &writer, indexes_json, .{});
            try std.testing.expectEqual(@as(usize, 1), summary.indexes_added);
            try std.testing.expect(writer.core.textIndex("full_text_index_v0") != null);
        }

        test "table provisioner reconciles stored algebraic metadata without public type" {
            const path = "/tmp/antfly-metadata-table-provisioner-algebraic-existing";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2001);
            defer std.testing.allocator.free(db_path);
            try fs_paths.createDirPathPortable(io_impl.io(), db_path);

            const config_json =
                \\{
                \\  "version": 1,
                \\  "schema_version": 1,
                \\  "table": "docs",
                \\  "group_fields": [{"name":"product","path":"product","type":"string"}],
                \\  "materializations": []
                \\}
            ;
            var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
            defer db.close();
            try db.addIndex(.{ .name = "alg", .kind = .algebraic, .config_json = config_json });

            const indexes_json =
                \\{"alg":{"version":1,"table":"docs","schema_version":1,"group_fields":[{"name":"product","path":"product","type":"string"}],"materializations":[]}}
            ;
            const summary = try ensureIndexes(std.testing.allocator, &db, indexes_json);
            try std.testing.expectEqual(@as(usize, 0), summary.added);
            try std.testing.expectEqual(@as(usize, 0), summary.removed);
            try std.testing.expect(db.core.index_manager.algebraicIndex("alg") != null);
        }

        test "table provisioner admits algebraic index on a non-empty table through generation repair" {
            const path = "/tmp/antfly-metadata-table-provisioner-algebraic-non-empty";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2002);
            defer std.testing.allocator.free(db_path);
            try fs_paths.createDirPathPortable(io_impl.io(), db_path);

            var db = try db_mod.DB.open(std.testing.allocator, db_path, .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();
            try db.batch(.{
                .writes = &.{.{ .key = "doc:old", .value = "{\"product\":\"hammer\"}" }},
                .sync_level = .write,
            });

            const indexes_json =
                \\{"alg":{"version":1,"table":"docs","schema_version":1,"group_fields":[{"name":"product","path":"product","type":"string"}],"materializations":[]}}
            ;
            const summary = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, indexes_json, .{});
            try std.testing.expectEqual(@as(usize, 1), summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 1), summary.indexes_pending);
            try std.testing.expect(db.core.index_manager.algebraicIndex("alg") != null);
            try std.testing.expect(db.core.index_manager.repairUnavailable("alg"));
            try std.testing.expect(try db.hasPendingIndexRepairIntents(std.testing.allocator));
        }

        test "table provisioner extracts public algebraic metadata as internal config" {
            const alloc = std.testing.allocator;
            const index_json =
                \\{"type":"algebraic","version":1,"table":"docs","schema_version":2,"derive_from_schema":true,"_index_incarnation":42,"_coverage_incarnation":41,"group_fields":[{"name":"customer","path":"customer","type":"string"}],"materializations":[]}
            ;
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
            defer parsed.deinit();

            try std.testing.expectEqual(db_mod.types.IndexKind.algebraic, try parseIndexKind(parsed.value));
            const config_json = try extractIndexConfigJson(alloc, "alg", parsed.value);
            defer alloc.free(config_json);
            var config = try std.json.parseFromSlice(std.json.Value, alloc, config_json, .{});
            defer config.deinit();

            try std.testing.expect(config.value.object.get("type") == null);
            try std.testing.expect(config.value.object.get("derive_from_schema") == null);
            try std.testing.expect(config.value.object.get("_index_incarnation") == null);
            try std.testing.expect(config.value.object.get("_coverage_incarnation") == null);
            try std.testing.expect(std.mem.indexOf(u8, config_json, "\"version\":1") != null);
            try std.testing.expect(std.mem.indexOf(u8, config_json, "\"schema_version\":2") != null);
        }

        test "table provisioner recognizes legacy stored algebraic metadata" {
            const alloc = std.testing.allocator;
            const index_json =
                \\{"version":1,"table":"docs","materializations":[]}
            ;
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
            defer parsed.deinit();

            try std.testing.expectEqual(db_mod.types.IndexKind.algebraic, try parseIndexKind(parsed.value));
        }

        test "table provisioner registers top-level enrichments without creating enrichment index" {
            const path = "/tmp/antfly-metadata-table-provisioner-enrichments";
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
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"enrichments\":[{\"name\":\"memory_embed\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":384}]}",
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 7,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
            try std.testing.expectEqual(@as(usize, 1), summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 1), summary.enrichments_added);

            const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2001);
            defer std.testing.allocator.free(db_path);
            var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
            defer db.close();
            try std.testing.expect(db.core.index_manager.has("full_text_index_v0"));
            try std.testing.expect(!db.core.index_manager.has("enrichments"));

            const indexes = try db.listIndexes(std.testing.allocator);
            defer db_mod.types.freeIndexConfigs(std.testing.allocator, indexes);
            try std.testing.expectEqual(@as(usize, 1), indexes.len);
            try std.testing.expectEqual(
                internal_keys.derivedCoverageGeneration(indexes[0].config_json),
                indexes[0].coverage_generation,
            );

            const enrichments = try db.listEnrichments(std.testing.allocator);
            defer db_mod.types.freeEnrichmentConfigs(std.testing.allocator, enrichments);
            try std.testing.expectEqual(@as(usize, 1), enrichments.len);
            try std.testing.expectEqualStrings("memory_embed", enrichments[0].name);
            try std.testing.expectEqual(db_mod.types.EnrichmentKind.embedding, enrichments[0].kind);
            try std.testing.expectEqualStrings("body", enrichments[0].field);
        }

        test "table provisioner registers a resolver declared in the table index config" {
            const path = "/tmp/antfly-metadata-table-provisioner-resolver";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            // A graph index produces the relations_v1 extraction asset; the reserved
            // top-level `resolvers` section declares the entity resolver that consumes
            // it. ensureIndexes skips `resolvers`; ensureResolvers registers it.
            const indexes_json =
                \\{
                \\  "relations_graph":{"type":"graph",
                \\    "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
                \\    "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}},
                \\  "resolvers":[
                \\    {"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1",
                \\     "key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","candidate_search":"prefix","config_generation":1,"_antfly_destination_authorization_v1":{"principal":"service:auth-disabled","signature":"auth-disabled","destinations":["entities"]}}
                \\  ]
                \\}
            ;

            const summary = try reconcileReplicaRoot(
                std.testing.allocator,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 9,
                    .name = "docs",
                    .indexes_json = indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 9,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
            // The graph index was added; the resolvers section was not treated as one.
            try std.testing.expectEqual(@as(usize, 1), summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 1), summary.resolvers_added);
            try std.testing.expectEqual(@as(usize, 0), summary.resolvers_updated);

            const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2001);
            defer std.testing.allocator.free(db_path);
            {
                var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
                defer db.close();

                try std.testing.expect(db.core.index_manager.has("relations_graph"));
                try std.testing.expect(!db.core.index_manager.has("resolvers"));

                const resolvers = try db.listResolvers(std.testing.allocator);
                defer {
                    for (resolvers) |*cfg| cfg.deinit(std.testing.allocator);
                    std.testing.allocator.free(resolvers);
                }
                try std.testing.expectEqual(@as(usize, 1), resolvers.len);
                try std.testing.expectEqualStrings("kg", resolvers[0].name);
                try std.testing.expectEqualStrings("entities", resolvers[0].table);
                try std.testing.expectEqualStrings("relations_v1", resolvers[0].source_artifact);
                try std.testing.expectEqualStrings("prefix", resolvers[0].candidate_search);
                try std.testing.expectEqual(@as(u64, 1), resolvers[0].config_generation);
            }

            const bumped_indexes_json =
                \\{
                \\  "relations_graph":{"type":"graph",
                \\    "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
                \\    "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}},
                \\  "resolvers":[
                \\    {"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1",
                \\     "key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","candidate_search":"prefix","config_generation":2,"_antfly_destination_authorization_v1":{"principal":"service:auth-disabled","signature":"auth-disabled","destinations":["entities"]}}
                \\  ]
                \\}
            ;

            const bumped_summary = try reconcileReplicaRoot(
                std.testing.allocator,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 9,
                    .name = "docs",
                    .indexes_json = bumped_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 9,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 0), bumped_summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 0), bumped_summary.resolvers_added);
            try std.testing.expectEqual(@as(usize, 1), bumped_summary.resolvers_updated);

            {
                var bumped_db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
                defer bumped_db.close();
                const bumped_resolvers = try bumped_db.listResolvers(std.testing.allocator);
                defer {
                    for (bumped_resolvers) |*cfg| cfg.deinit(std.testing.allocator);
                    std.testing.allocator.free(bumped_resolvers);
                }
                try std.testing.expectEqual(@as(usize, 1), bumped_resolvers.len);
                try std.testing.expectEqualStrings("kg", bumped_resolvers[0].name);
                try std.testing.expectEqual(@as(u64, 2), bumped_resolvers[0].config_generation);
            }

            const removed_indexes_json =
                \\{
                \\  "relations_graph":{"type":"graph",
                \\    "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
                \\    "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}},
                \\  "resolvers":[]
                \\}
            ;

            const removed_summary = try reconcileReplicaRoot(
                std.testing.allocator,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 9,
                    .name = "docs",
                    .indexes_json = removed_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 9,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 0), removed_summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 0), removed_summary.resolvers_added);
            try std.testing.expectEqual(@as(usize, 0), removed_summary.resolvers_updated);
            try std.testing.expectEqual(@as(usize, 1), removed_summary.resolvers_removed);

            var removed_db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
            defer removed_db.close();
            const removed_resolvers = try removed_db.listResolvers(std.testing.allocator);
            defer {
                for (removed_resolvers) |*cfg| cfg.deinit(std.testing.allocator);
                std.testing.allocator.free(removed_resolvers);
            }
            try std.testing.expectEqual(@as(usize, 0), removed_resolvers.len);
        }

        test "table provisioner can admit resolver backfill without draining corpus work" {
            const alloc = std.testing.allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-async-resolver";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            var db = try db_mod.DB.open(alloc, path, .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();

            const graph_only =
                \\{
                \\  "relations_graph":{"type":"graph",
                \\    "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
                \\    "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
                \\}
            ;
            _ = try reconcileDbIndexesWithOptions(alloc, &db, graph_only, .{});
            try db.batch(.{
                .writes = &.{.{
                    .key = "doc:a",
                    .value =
                    \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
                    ,
                }},
                .sync_level = .enrichments,
            });
            try db.runUntilIdle();

            const with_resolver =
                \\{
                \\  "relations_graph":{"type":"graph",
                \\    "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
                \\    "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}},
                \\  "resolvers":[
                \\    {"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1",
                \\     "key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","candidate_search":"prefix","config_generation":1,"_antfly_destination_authorization_v1":{"principal":"service:auth-disabled","signature":"auth-disabled","destinations":["entities"]}}
                \\  ]
                \\}
            ;
            const summary = try reconcileDbIndexesWithOptions(alloc, &db, with_resolver, .{
                .drain_resolver_backfill = false,
            });
            try std.testing.expectEqual(@as(usize, 1), summary.resolvers_added);

            const resolvers = try db.listResolvers(alloc);
            defer {
                for (resolvers) |*cfg| cfg.deinit(alloc);
                alloc.free(resolvers);
            }
            try std.testing.expectEqual(@as(usize, 1), resolvers.len);
            try std.testing.expectEqualStrings("kg", resolvers[0].name);
        }

        test "table provisioner registers explicit document enrichments from index config" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-enrichments";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const indexes_json =
                \\{
                \\  "document_text":{"type":"full_text","artifact_name":"document_chunks_v1","enrichments":[
                \\    {"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"},
                \\    {"name":"document_chunks_v1","kind":"chunk","source_artifact_name":"document_units_v1","field":"text","chunk_size":512,"chunk_overlap":50}
                \\  ]}
                \\}
            ;

            const summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 11,
                    .name = "docs",
                    .indexes_json = indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 11,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
            try std.testing.expectEqual(@as(usize, 1), summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 2), summary.enrichments_added);

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
            defer alloc.free(db_path);
            var db = try db_mod.DB.open(alloc, db_path, .{});
            defer db.close();
            const enrichments = try db.listEnrichments(alloc);
            defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
            try std.testing.expectEqual(@as(usize, 2), enrichments.len);
            try std.testing.expectEqualStrings("document_units_v1", enrichments[0].name);
            try std.testing.expectEqual(.asset, enrichments[0].kind);
            try std.testing.expectEqualStrings("document_chunks_v1", enrichments[1].name);
            try std.testing.expectEqual(.chunk, enrichments[1].kind);
        }

        test "table provisioner rejects conflicting inline enrichment definitions" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-conflicting-enrichments";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const indexes_json =
                \\{
                \\  "document_text":{"type":"full_text","artifact_name":"document_chunks_v1","enrichments":[
                \\    {"name":"document_chunks_v1","kind":"chunk","field":"text","chunk_size":512,"chunk_overlap":50},
                \\    {"name":"document_chunks_v1","kind":"chunk","field":"text","chunk_size":256,"chunk_overlap":50}
                \\  ]}
                \\}
            ;

            try std.testing.expectError(error.ConflictingEnrichmentConfig, reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 12,
                    .name = "docs",
                    .indexes_json = indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 12,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            ));
        }

        test "table provisioner rejects conflicting enrichment kinds under the same artifact name" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-conflicting-enrichment-kinds";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const indexes_json =
                \\{
                \\  "enrichments":[
                \\    {"name":"document_artifact_v1","kind":"asset","field":"url","content_type":"application/json"},
                \\    {"name":"document_artifact_v1","kind":"chunk","field":"text","chunk_size":512}
                \\  ]}
            ;

            try std.testing.expectError(error.ConflictingEnrichmentConfig, reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 16,
                    .name = "docs",
                    .indexes_json = indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 16,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            ));
        }

        test "table provisioner updates changed enrichment config under the same name" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-changed-enrichment";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const first_indexes_json =
                \\{
                \\  "enrichments":[
                \\    {"name":"document_chunks_v1","kind":"chunk","field":"text","chunk_size":512,"chunk_overlap":50}
                \\  ]}
            ;
            const second_indexes_json =
                \\{
                \\  "enrichments":[
                \\    {"name":"document_chunks_v1","kind":"chunk","field":"text","chunk_size":256,"chunk_overlap":25,"full_text_index":true}
                \\  ]}
            ;

            const first_summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 13,
                    .name = "docs",
                    .indexes_json = first_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 13,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 1), first_summary.enrichments_added);

            const second_summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 13,
                    .name = "docs",
                    .indexes_json = second_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 13,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 0), second_summary.enrichments_added);
            try std.testing.expectEqual(@as(usize, 1), second_summary.enrichments_updated);

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
            defer alloc.free(db_path);
            var db = try db_mod.DB.open(alloc, db_path, .{});
            defer db.close();

            const enrichments = try db.listEnrichments(alloc);
            defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
            const cfg = findEnrichment(enrichments, .chunk, "document_chunks_v1") orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(u32, 256), cfg.chunk_size);
            try std.testing.expectEqual(@as(u32, 25), cfg.chunk_overlap);
            try std.testing.expect(cfg.full_text_index);
        }

        test "table provisioner replaces enrichment kind under the same artifact name" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-replace-enrichment-kind";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const first_indexes_json =
                \\{
                \\  "enrichments":[
                \\    {"name":"document_artifact_v1","kind":"asset","field":"url","content_type":"application/json"}
                \\  ]}
            ;
            const second_indexes_json =
                \\{
                \\  "enrichments":[
                \\    {"name":"document_artifact_v1","kind":"chunk","field":"body","chunk_size":256,"chunk_overlap":25}
                \\  ]}
            ;

            const first_summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 17,
                    .name = "docs",
                    .indexes_json = first_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 17,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 1), first_summary.enrichments_added);

            const second_summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 17,
                    .name = "docs",
                    .indexes_json = second_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 17,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 0), second_summary.enrichments_added);
            try std.testing.expectEqual(@as(usize, 1), second_summary.enrichments_updated);

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
            defer alloc.free(db_path);
            var db = try db_mod.DB.open(alloc, db_path, .{});
            defer db.close();

            const enrichments = try db.listEnrichments(alloc);
            defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
            try std.testing.expectEqual(@as(usize, 1), enrichments.len);
            try std.testing.expectEqual(.chunk, enrichments[0].kind);
            try std.testing.expectEqualStrings("document_artifact_v1", enrichments[0].name);
            try std.testing.expectEqualStrings("body", enrichments[0].field);
            try std.testing.expectEqual(@as(u32, 256), enrichments[0].chunk_size);
        }

        test "table provisioner applies artifact enrichments in dependency order" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-enrichment-dependency-order";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const indexes_json =
                \\{
                \\  "enrichments":[
                \\    {"name":"document_chunks_v1","kind":"chunk","source_artifact_name":"document_units_v1","field":"text","chunk_size":512,"full_text_index":true},
                \\    {"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json"}
                \\  ]}
            ;

            const summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 18,
                    .name = "docs",
                    .indexes_json = indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 18,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 2), summary.enrichments_added);

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
            defer alloc.free(db_path);
            var db = try db_mod.DB.open(alloc, db_path, .{});
            defer db.close();

            const enrichments = try db.listEnrichments(alloc);
            defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
            try std.testing.expectEqual(@as(usize, 2), enrichments.len);
            try std.testing.expect(findEnrichment(enrichments, .asset, "document_units_v1") != null);
            try std.testing.expect(findEnrichment(enrichments, .chunk, "document_chunks_v1") != null);
        }

        test "table provisioner compares full text index configs semantically" {
            try std.testing.expect(try fullTextIndexConfigsEqual(
                std.testing.allocator,
                "{\"type\":\"full_text\",\"artifact_name\":\"document_chunks_v1\",\"description\":\"docs\",\"enrichments\":[{\"name\":\"a\",\"kind\":\"chunk\"}]}",
                "{\"description\":\"docs\",\"enrichments\":[{\"name\":\"b\",\"kind\":\"chunk\"}],\"artifact_name\":\"document_chunks_v1\",\"type\":\"full_text\"}",
            ));
            try std.testing.expect(!try fullTextIndexConfigsEqual(
                std.testing.allocator,
                "{\"type\":\"full_text\",\"artifact_name\":\"document_chunks_v1\"}",
                "{\"type\":\"full_text\",\"artifact_name\":\"document_chunks_v2\"}",
            ));
        }

        test "table provisioner treats duplicate identical inline enrichments as one desired artifact" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-shared-enrichment";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const indexes_json =
                \\{
                \\  "document_text_a":{"type":"full_text","artifact_name":"document_chunks_v1","enrichments":[
                \\    {"name":"document_chunks_v1","kind":"chunk","field":"text","chunk_size":512,"chunk_overlap":50}
                \\  ]},
                \\  "document_text_b":{"type":"full_text","artifact_name":"document_chunks_v1","enrichments":[
                \\    {"name":"document_chunks_v1","kind":"chunk","field":"text","chunk_size":512,"chunk_overlap":50}
                \\  ]}
                \\}
            ;

            const summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 15,
                    .name = "docs",
                    .indexes_json = indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 15,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 2), summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 1), summary.enrichments_added);

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
            defer alloc.free(db_path);
            var db = try db_mod.DB.open(alloc, db_path, .{});
            defer db.close();

            const enrichments = try db.listEnrichments(alloc);
            defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
            try std.testing.expectEqual(@as(usize, 1), enrichments.len);
            try std.testing.expect(findEnrichment(enrichments, .chunk, "document_chunks_v1") != null);
        }

        test "table provisioner updates full text artifact mapping and cleans removed enrichments" {
            const alloc = std.heap.c_allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-enrichment-remap";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const first_indexes_json =
                \\{
                \\  "document_text":{"type":"full_text","artifact_name":"document_chunks_v1","enrichments":[
                \\    {"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"},
                \\    {"name":"document_chunks_v1","kind":"chunk","source_artifact_name":"document_units_v1","field":"text","chunk_size":512,"chunk_overlap":50}
                \\  ]}
                \\}
            ;
            const second_indexes_json =
                \\{
                \\  "document_text":{"type":"full_text","artifact_name":"document_chunks_v2","enrichments":[
                \\    {"name":"document_units_v2","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"},
                \\    {"name":"document_chunks_v2","kind":"chunk","source_artifact_name":"document_units_v2","field":"text","chunk_size":512,"chunk_overlap":50}
                \\  ]}
                \\}
            ;

            _ = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 14,
                    .name = "docs",
                    .indexes_json = first_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 14,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );

            {
                const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
                defer alloc.free(db_path);
                var db = try db_mod.DB.open(alloc, db_path, .{});
                defer db.close();

                const chunk_key = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v2", 0);
                defer alloc.free(chunk_key);
                try db.core.store.putBatch(&.{
                    .{ .key = chunk_key, .value = "{\"text\":\"gamma remap token\"}" },
                }, &.{});
            }

            const retired_summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 14,
                    .name = "docs",
                    .indexes_json = second_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 14,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 0), retired_summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 1), retired_summary.indexes_removed);
            try std.testing.expectEqual(@as(usize, 1), retired_summary.indexes_pending);
            try std.testing.expectEqual(@as(usize, 2), retired_summary.enrichments_added);
            try std.testing.expectEqual(@as(usize, 2), retired_summary.enrichments_removed);

            {
                const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
                defer alloc.free(db_path);
                var db = try db_mod.DB.open(alloc, db_path, .{});
                defer db.close();
                db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);
            }

            const admitted_summary = try reconcileReplicaRoot(
                alloc,
                path,
                100,
                &.{ 100, 2001 },
                &.{.{
                    .table_id = 14,
                    .name = "docs",
                    .indexes_json = second_indexes_json,
                }},
                &.{.{
                    .group_id = 2001,
                    .table_id = 14,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            try std.testing.expectEqual(@as(usize, 1), admitted_summary.indexes_added);
            try std.testing.expectEqual(@as(usize, 0), admitted_summary.indexes_removed);
            try std.testing.expectEqual(@as(usize, 0), admitted_summary.indexes_pending);

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2001);
            defer alloc.free(db_path);
            var db = try db_mod.DB.open(alloc, db_path, .{});
            defer db.close();

            const enrichments = try db.listEnrichments(alloc);
            defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
            try std.testing.expectEqual(@as(usize, 2), enrichments.len);
            try std.testing.expect(findEnrichment(enrichments, .asset, "document_units_v2") != null);
            try std.testing.expect(findEnrichment(enrichments, .chunk, "document_chunks_v2") != null);

            const old_text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, "document_chunks_v1", false);
            defer {
                for (old_text_indexes) |name| alloc.free(name);
                alloc.free(old_text_indexes);
            }
            try std.testing.expectEqual(@as(usize, 0), old_text_indexes.len);

            const new_text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, "document_chunks_v2", false);
            defer {
                for (new_text_indexes) |name| alloc.free(name);
                alloc.free(new_text_indexes);
            }
            try std.testing.expectEqual(@as(usize, 1), new_text_indexes.len);
            try std.testing.expectEqualStrings("document_text", new_text_indexes[0]);

            var result = try db.search(alloc, .{
                .index_name = "document_text",
                .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
                .limit = 1,
                .return_mode = .chunk,
            });
            defer result.deinit();
            try std.testing.expectEqual(@as(u32, 1), result.total_hits);
            try std.testing.expectEqual(@as(usize, 1), result.hits.len);
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

            const restore_namespace = doc_identity.Namespace{
                .table_id = 7,
                .shard_id = 2001,
                .range_id = 2001,
            };
            var source_db = try db_mod.DB.open(std.testing.allocator, source_db_path, .{
                .identity_namespace = restore_namespace,
            });
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
            var artifact_integrity = try backups_api.artifactIntegrityAlloc(
                std.testing.allocator,
                std.testing.io,
                .native,
                dest_root,
            );
            defer artifact_integrity.deinit(std.testing.allocator);
            var node_config = try testRestoreNodeConfig(std.testing.allocator);
            defer node_config.deinit();

            const manifest = try backups_api.createManifest(
                std.testing.allocator,
                "snap1",
                .native,
                &.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
                    .placement_role = "data",
                },
                &.{.{
                    .group_id = 2001,
                    .start_key = "",
                    .end_key = null,
                    .snapshot_path = "snap1/groups/2001",
                    .artifact_size_bytes = artifact_integrity.size_bytes,
                    .artifact_sha256 = artifact_integrity.sha256,
                }},
            );
            defer {
                var owned = manifest;
                owned.deinit(std.testing.allocator);
            }
            try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

            const summary = try reconcileReplicaRootWithOptions(
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
                    .start_key = "",
                    .end_key = null,
                    .restore_backup_id = "snap1",
                    .restore_artifact_backup_id = "snap1",
                    .restore_location = restore_location,
                    .restore_snapshot_path = "snap1/groups/2001",
                    .restore_connection = "test-backups",
                    .restore_artifact_size_bytes = artifact_integrity.size_bytes,
                    .restore_artifact_sha256 = artifact_integrity.sha256,
                }},
                .{
                    .restore_open_options = .{
                        .node_config = &node_config,
                        .filesystem_io = io_impl.io(),
                    },
                },
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
                tables: [1]table_manager.TableRecord,
                ranges: [1]table_manager.RangeRecord,

                fn iface(self: *@This()) table_catalog.CatalogSource {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                            .routing_snapshot = table_catalog.TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).routingSnapshot,
                            .linearizable_routing_snapshot = table_catalog.TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).linearizableSnapshot,
                            .free_routing_snapshot = table_catalog.TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).freeRoutingSnapshot,
                        },
                    };
                }

                fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    return .{
                        .status = .{ .metadata_group_id = 100, .metrics = .{} },
                        .tables = self.tables[0..],
                        .ranges = self.ranges[0..],
                        .stores = @constCast((&[_]table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]@import("transition_state.zig").SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]@import("transition_state.zig").MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
            };

            var fake_catalog = FakeCatalog{
                .tables = .{.{
                    .table_id = 7,
                    .name = "docs",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
                    .restore_backup_id = "snap1",
                    .restore_location = restore_location,
                    .placement_role = "data",
                }},
                .ranges = .{.{
                    .group_id = 2001,
                    .table_id = 7,
                    .start_key = "doc:a",
                    .end_key = null,
                }},
            };
            var read_source = table_reads.ProvisionedTableReadSource.init(
                replica_root,
                fake_catalog.iface(),
                raft_mod.read_gate.alreadyReadSafeBarrier(),
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
            var artifact_integrity = try backups_api.artifactIntegrityAlloc(
                std.testing.allocator,
                std.testing.io,
                .native,
                dest_root,
            );
            defer artifact_integrity.deinit(std.testing.allocator);
            var node_config = try testRestoreNodeConfig(std.testing.allocator);
            defer node_config.deinit();

            const manifest = try backups_api.createManifest(
                std.testing.allocator,
                "snap1",
                .native,
                &.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .indexes_json = tables_api.default_indexes_json,
                    .placement_role = "data",
                },
                &.{.{
                    .group_id = 2001,
                    .start_key = "",
                    .end_key = null,
                    .snapshot_path = "snap1/groups/2001",
                    .artifact_size_bytes = artifact_integrity.size_bytes,
                    .artifact_sha256 = artifact_integrity.sha256,
                }},
            );
            defer {
                var owned = manifest;
                owned.deinit(std.testing.allocator);
            }
            try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

            try std.testing.expectError(error.IdentityNamespaceMismatch, reconcileReplicaRootWithOptions(
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
                    .start_key = "",
                    .end_key = null,
                    .range_id = 2001,
                    .restore_backup_id = "snap1",
                    .restore_artifact_backup_id = "snap1",
                    .restore_location = restore_location,
                    .restore_snapshot_path = "snap1/groups/2001",
                    .restore_connection = "test-backups",
                    .restore_artifact_size_bytes = artifact_integrity.size_bytes,
                    .restore_artifact_sha256 = artifact_integrity.sha256,
                }},
                .{
                    .restore_open_options = .{
                        .node_config = &node_config,
                        .filesystem_io = io_impl.io(),
                    },
                },
            ));
        }

        test "table provisioner removes indexes missing from metadata" {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();

            const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-provisioner-drop", .{tmp.sub_path});
            defer std.testing.allocator.free(path);
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2002);
            defer std.testing.allocator.free(db_path);
            try fs_paths.createDirPathPortable(io_impl.io(), db_path);

            var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
            var db_open = true;
            defer if (db_open) db.close();
            try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
            try db.addIndex(.{ .name = "embed_idx", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}" });
            db.close();
            db_open = false;

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

            const public_index_json = "{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}";
            var parsed_public_index = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, public_index_json, .{});
            defer parsed_public_index.deinit();
            const stored_index_json = try managed_embedder.translateEmbeddingsIndexConfigJson(
                std.testing.allocator,
                "embed_idx",
                parsed_public_index.value,
            );
            defer std.testing.allocator.free(stored_index_json);

            var coverage_incarnation: u64 = 0;
            {
                var db = try db_mod.DB.open(std.testing.allocator, db_path, .{
                    .start_index_workers = false,
                });
                defer db.close();
                try db.addIndex(.{
                    .name = "embed_idx",
                    .kind = .dense_vector,
                    .config_json = stored_index_json,
                });
                const configs = try db.listIndexes(std.testing.allocator);
                defer db_mod.types.freeIndexConfigs(std.testing.allocator, configs);
                try std.testing.expectEqual(@as(usize, 1), configs.len);
                coverage_incarnation = configs[0].coverage_generation;
                try std.testing.expect(coverage_incarnation != 0);
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

            const index_config_with_incarnation = try coverage_policy.withIncarnationAlloc(
                std.testing.allocator,
                parsed_public_index.value,
                coverage_incarnation,
            );
            defer std.testing.allocator.free(index_config_with_incarnation);
            const indexes_json = try std.fmt.allocPrint(
                std.testing.allocator,
                "{{\"embed_idx\":{s}}}",
                .{index_config_with_incarnation},
            );
            defer std.testing.allocator.free(indexes_json);

            const summary = try reconcileReplicaRoot(
                std.testing.allocator,
                path,
                100,
                &.{ 100, 2006 },
                &.{.{
                    .table_id = 11,
                    .name = "docs",
                    .indexes_json = indexes_json,
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
            try std.testing.expectEqual(@as(usize, 0), summary.indexes_removed);
            try std.testing.expectEqual(@as(usize, 0), summary.indexes_added);

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
            const dense = reopened.core.index_manager.denseIndex("embed_idx") orelse
                return error.TestUnexpectedResult;
            try std.testing.expectEqual(
                @as(?u64, applied),
                dense.index.experimentalPostingDurableAppliedSequence(),
            );
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

        test "table provisioner treats an active generation transition as schema progress not ready" {
            const GenerationTransitionAdapter = struct {
                fn fetchMedianKey(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
                    return null;
                }

                fn schemaIndexReady(
                    _: *anyopaque,
                    _: std.mem.Allocator,
                    _: []const u8,
                    _: u64,
                    _: u32,
                    _: u32,
                ) !bool {
                    return error.GenerationTransitionActive;
                }

                fn adapter() shard_db_adapter_mod.ShardDbAdapter {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .fetch_median_key = fetchMedianKey,
                            .schema_index_ready = schemaIndexReady,
                        },
                    };
                }
            };

            const progress = try collectLocalSchemaProgressWithOptions(
                std.testing.allocator,
                "/tmp/unused-antfly-schema-progress-transition",
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
                .{ .shard_db_adapter = GenerationTransitionAdapter.adapter() },
            );
            defer std.testing.allocator.free(progress);

            try std.testing.expectEqual(@as(usize, 0), progress.len);
        }

        test "table provisioner schema progress probes do not take a writer lease" {
            const path = "/tmp/antfly-metadata-table-provisioner-progress-live-writer";
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const db_path = try groupDbPathFromReplicaRoot(std.testing.allocator, path, 2004);
            defer std.testing.allocator.free(db_path);
            try fs_paths.createDirPathPortable(io_impl.io(), db_path);
            var db = try db_mod.DB.open(std.testing.allocator, db_path, .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();
            try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
            try db.addIndex(.{ .name = "full_text_index_v1", .kind = .full_text, .config_json = "{}" });

            const progress = try collectLocalSchemaProgress(
                std.testing.allocator,
                path,
                100,
                7,
                &.{ 100, 2004 },
                &.{.{
                    .table_id = 9,
                    .name = "docs",
                    .schema_json = "{\"version\":1}",
                    .read_schema_json = "{\"version\":0}",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
                }},
                &.{.{
                    .group_id = 2004,
                    .table_id = 9,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            defer std.testing.allocator.free(progress);

            try std.testing.expectEqual(@as(usize, 1), progress.len);
            try std.testing.expectEqual(@as(u64, 9), progress[0].table_id);
        }

        test "table provisioner schema progress reads generation-owned rebuild state from status-only catalog" {
            const alloc = std.testing.allocator;
            const path = "/tmp/antfly-metadata-table-provisioner-progress-rebuild-marker";
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
            defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2007);
            defer alloc.free(db_path);
            const index_root = try std.fmt.allocPrint(alloc, "{s}/indexes/full_text_index_v2", .{db_path});
            defer alloc.free(index_root);
            var coverage_generation: u64 = 0;
            {
                var db = try db_mod.DB.open(alloc, db_path, .{});
                defer db.close();
                try db.addIndex(.{
                    .name = "full_text_index_v2",
                    .kind = .full_text,
                    .config_json = "{}",
                });
                const configs = try db.listIndexes(alloc);
                defer db_mod.types.freeIndexConfigs(alloc, configs);
                for (configs) |config| {
                    if (!std.mem.eql(u8, config.name, "full_text_index_v2")) continue;
                    coverage_generation = config.coverage_generation;
                    break;
                }
            }
            try std.testing.expect(coverage_generation != 0);
            const rebuild_state = db_mod.backfill_state.RebuildState.initOwned(index_root, null, coverage_generation);
            try rebuild_state.updateWithIo(io_impl.io(), "doc:m");

            // The lightweight status-only open reads the authoritative catalog
            // generation without mapping retained full-text state. That lets it ignore
            // stale same-name workers while still observing the current marker.
            try std.testing.expect(!try localRangeHasSchemaVersionIndex(
                alloc,
                path,
                "docs",
                .{ .group_id = 2007, .range_id = 2007, .table_id = 1, .start_key = "" },
                2,
                1,
                true,
                .{},
            ));
        }

        test "table provisioner schema progress quarantines a corrupt rebuild marker" {
            const alloc = std.testing.allocator;
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(
                alloc,
                ".zig-cache/tmp/{s}/metadata-table-provisioner-progress-corrupt-rebuild-marker",
                .{tmp.sub_path},
            );
            defer alloc.free(path);
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();

            const db_path = try groupDbPathFromReplicaRoot(alloc, path, 2008);
            defer alloc.free(db_path);
            const index_root = try std.fmt.allocPrint(alloc, "{s}/indexes/full_text_index_v2", .{db_path});
            defer alloc.free(index_root);
            try fs_paths.createDirPathPortable(io_impl.io(), index_root);
            const rebuild_state = db_mod.backfill_state.RebuildState.init(index_root);
            try rebuild_state.updateWithIo(io_impl.io(), "doc:m");

            const state_path = try rebuild_state.pathAlloc(alloc);
            defer alloc.free(state_path);
            const encoded = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), state_path, alloc, .limited(1024));
            defer alloc.free(encoded);
            encoded[encoded.len - 1] ^= 0xff;
            try std.Io.Dir.cwd().writeFile(io_impl.io(), .{ .sub_path = state_path, .data = encoded });

            const progress = try collectLocalSchemaProgress(
                alloc,
                path,
                100,
                7,
                &.{ 100, 2008 },
                &.{.{
                    .table_id = 9,
                    .name = "docs",
                    .schema_json = "{\"version\":2}",
                    .read_schema_json = "{\"version\":1}",
                    .indexes_json = "{}",
                }},
                &.{.{
                    .group_id = 2008,
                    .table_id = 9,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }},
            );
            defer alloc.free(progress);

            // A corrupt marker keeps the shard quarantined without aborting the
            // metadata lifecycle that collects schema progress.
            try std.testing.expectEqual(@as(usize, 0), progress.len);
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
            const range = table_manager.RangeRecord{ .group_id = 7, .table_id = 11, .start_key = "" };
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
                .table_id = range.table_id,
                .group_id = range.group_id,
                .freshness = "fresh",
                .doc_count = 1500,
                .doc_identity = .{
                    .namespace_table_id = range.table_id,
                    .namespace_shard_id = range.group_id,
                    .namespace_range_id = range.group_id,
                    .next_ordinal = 1001,
                    .allocated_ordinals = 1000,
                    .live_ordinals = 1000,
                },
                .indexes = @constCast(indexes[0..]),
            }, range, 1, 0, true));
        }

        test "table provisioner runtime schema progress requires authoritative O(1) identity coverage" {
            const range = table_manager.RangeRecord{ .group_id = 7, .table_id = 11, .start_key = "" };
            var indexes = [_]table_manager.RuntimeIndexStatusReport{
                .{
                    .name = "full_text_index_v0",
                    .kind = "full_text",
                    .doc_count = 1000,
                    .replay_applied_sequence = 7,
                    .replay_target_sequence = 7,
                },
                .{
                    .name = "full_text_index_v1",
                    .kind = "full_text",
                    .doc_count = 0,
                    .replay_applied_sequence = 7,
                    .replay_target_sequence = 7,
                },
            };
            var runtime = table_manager.RuntimeGroupStatusReport{
                .table_id = range.table_id,
                .group_id = range.group_id,
                .freshness = "fresh",
                .doc_count = 1000,
                .doc_identity = .{
                    .namespace_table_id = range.table_id,
                    .namespace_shard_id = range.group_id,
                    .namespace_range_id = range.group_id,
                    .next_ordinal = 1001,
                    .allocated_ordinals = 1000,
                    .live_ordinals = 1000,
                },
                .indexes = &indexes,
            };

            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));

            indexes[1].doc_count = 1000;
            try std.testing.expect(runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));

            runtime.freshness = "stale";
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));

            runtime.freshness = "fresh";
            runtime.doc_identity.allocated_ordinals = 999;
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));

            runtime.doc_identity.allocated_ordinals = 0;
            runtime.doc_identity.next_ordinal = 1;
            runtime.doc_count = 0;
            runtime.doc_identity.live_ordinals = 0;
            indexes[1].doc_count = 0;
            try std.testing.expect(runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));
        }

        test "table provisioner accepts chunk-inflated target full-text doc count" {
            const range = table_manager.RangeRecord{ .group_id = 7, .table_id = 11, .start_key = "" };
            var indexes = [_]table_manager.RuntimeIndexStatusReport{
                .{
                    .name = "full_text_index_v0",
                    .kind = "full_text",
                    .doc_count = 1000,
                    .replay_applied_sequence = 7,
                    .replay_target_sequence = 7,
                },
                .{
                    // 1000 primary rows + 500 chunk members routed in by a chunk
                    // enrichment with full_text_index: true.
                    .name = "full_text_index_v1",
                    .kind = "full_text",
                    .doc_count = 1500,
                    .replay_applied_sequence = 7,
                    .replay_target_sequence = 7,
                },
            };
            const runtime = table_manager.RuntimeGroupStatusReport{
                .table_id = range.table_id,
                .group_id = range.group_id,
                .freshness = "fresh",
                .doc_count = 1000,
                .doc_identity = .{
                    .namespace_table_id = range.table_id,
                    .namespace_shard_id = range.group_id,
                    .namespace_range_id = range.group_id,
                    .next_ordinal = 1001,
                    .allocated_ordinals = 1000,
                    .live_ordinals = 1000,
                },
                .indexes = &indexes,
            };

            try std.testing.expect(runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));

            // A rebuild still genuinely in progress (fewer full-text docs than
            // primary identities) must still be rejected.
            indexes[1].doc_count = 400;
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));

            // A brand-new placeholder index (nothing written yet) is still rejected.
            indexes[1].doc_count = 0;
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 1, 0, true));
        }

        test "table provisioner indexless relational schema cutover requires exact fresh owner epoch" {
            const range = table_manager.RangeRecord{ .group_id = 7, .range_id = 7, .table_id = 11, .start_key = "" };
            var runtime = table_manager.RuntimeGroupStatusReport{
                .table_id = range.table_id,
                .group_id = range.group_id,
                .freshness = "fresh",
                .doc_identity = .{
                    .namespace_table_id = range.table_id,
                    .namespace_shard_id = table_manager.rangeDocIdentityShardId(range),
                    .namespace_range_id = table_manager.rangeDocIdentityRangeId(range),
                    .next_ordinal = 1,
                },
            };
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 2, 1, false));
            runtime.schema_epoch = 1;
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 2, 1, false));
            runtime.schema_epoch = 2;
            try std.testing.expect(runtimeHasReadySchemaVersionIndex(runtime, range, 2, 1, false));
            runtime.freshness = "stale";
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 2, 1, false));
            runtime.freshness = "fresh";
            runtime.doc_identity.namespace_range_id += 1;
            try std.testing.expect(!runtimeHasReadySchemaVersionIndex(runtime, range, 2, 1, false));
        }

        test "target index reconciliation does not wait for sibling storage maintenance" {
            if (@import("builtin").single_threaded) return error.SkipZigTest;
            const alloc = std.testing.allocator;
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target-maintenance", .{tmp.sub_path});
            defer alloc.free(path);
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            const io = io_impl.io();
            var db = try db_mod.DB.open(alloc, path, .{
                .start_index_workers = false,
                .start_optional_runtime_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer db.close();
            try db.addIndex(.{ .name = "sibling", .kind = .full_text, .config_json = "{}" });
            const sibling = db.core.index_manager.textIndex("sibling").?;
            const storage = sibling.main_store_owner.lsm.backend;
            const Reconcile = struct {
                db: *db_mod.DB,
                io: std.Io,
                done: std.Io.Event = .unset,
                fn run(self: *@This()) !void {
                    defer self.done.set(self.io);
                    const created = try reconcileDbIndexTarget(std.testing.allocator, self.db,
                        \\{"sibling":{"type":"full_text"},"target":{"type":"full_text"}}
                    , "target");
                    try std.testing.expectEqual(@as(usize, 1), created.indexes_added);
                    const removed = try reconcileDbIndexTarget(std.testing.allocator, self.db,
                        \\{"sibling":{"type":"full_text"}}
                    , "target");
                    try std.testing.expectEqual(@as(usize, 1), removed.indexes_removed);
                }
            };
            var reconcile = Reconcile{ .db = &db, .io = io };
            // Hold the actual sibling storage lock as a checkpoint/maintenance task
            // would. Creating and dropping another index must finish before release.
            try std.testing.expect(storage.mu.tryLock());
            var locked = true;
            defer if (locked) storage.mu.unlock();
            var future = try io.concurrent(Reconcile.run, .{&reconcile});
            const completed = if (reconcile.done.waitTimeout(io, .{
                .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
            })) |_| true else |err| switch (err) {
                error.Timeout => false,
                error.Canceled => false,
            };
            storage.mu.unlock();
            locked = false;
            try future.await(io);
            try std.testing.expect(completed);
            try std.testing.expect(db.core.index_manager.textIndex("sibling").?.main_store_owner.lsm.backend == storage);
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = implementation_tests;
}
