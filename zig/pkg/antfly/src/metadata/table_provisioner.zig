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
const coverage_policy = @import("../api/coverage_policy.zig");
const indexes_api = @import("../api/indexes.zig");
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
    enrichments_added: usize = 0,
    enrichments_updated: usize = 0,
    enrichments_removed: usize = 0,
    resolvers_added: usize = 0,
    resolvers_updated: usize = 0,
    resolvers_removed: usize = 0,

    pub fn indexManagerCatalogChanged(self: @This()) bool {
        return self.indexes_added > 0 or
            self.indexes_removed > 0 or
            self.resolvers_added > 0 or
            self.resolvers_updated > 0 or
            self.resolvers_removed > 0;
    }
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

        const runtime_schema = try runtimeTableSchemaFromJson(alloc, table.schema_json);
        defer if (runtime_schema) |schema| @import("../storage/schema.zig").freeSchema(alloc, schema);
        var open_options = provisioningDbOpenOptions();
        open_options.backend_runtime = options.backend_runtime;
        open_options.schema_before_index_load = runtime_schema;
        var db = try db_mod.DB.open(alloc, path, open_options);
        defer db.close();
        summary.dbs_opened += 1;
        const index_summary = try reconcileDbIndexes(alloc, &db, table.indexes_json);
        summary.indexes_removed += index_summary.indexes_removed;
        summary.indexes_added += index_summary.indexes_added;
        summary.enrichments_added += index_summary.enrichments_added;
        summary.enrichments_updated += index_summary.enrichments_updated;
        summary.enrichments_removed += index_summary.enrichments_removed;
        summary.resolvers_added += index_summary.resolvers_added;
        summary.resolvers_updated += index_summary.resolvers_updated;
        summary.resolvers_removed += index_summary.resolvers_removed;
    }
    return summary;
}

fn runtimeTableSchemaFromJson(alloc: std.mem.Allocator, schema_json: []const u8) !?@import("../storage/schema.zig").TableSchema {
    if (schema_json.len == 0) return null;
    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    return try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
}

pub fn reconcileDbIndexes(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
) !ProvisionSummary {
    return try reconcileDbIndexesWithOptions(alloc, db, indexes_json, .{});
}

pub const ReconcileDbIndexOptions = struct {
    drain_resolver_backfill: bool = true,
};

fn dbIndexReconciliationCanMutate(db: *const db_mod.DB) bool {
    return db.open_mode != .query_readonly and db.open_mode != .status_only;
}

pub fn reconcileDbIndexesWithOptions(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    options: ReconcileDbIndexOptions,
) !ProvisionSummary {
    var desired_enrichments = std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig).empty;
    defer {
        for (desired_enrichments.items) |*cfg| cfg.deinit(alloc);
        desired_enrichments.deinit(alloc);
    }
    try collectDesiredEnrichmentsFromJson(alloc, indexes_json, &desired_enrichments);
    try indexes_api.validateArtifactEnrichmentConfigs(desired_enrichments.items);
    dedupeDesiredEnrichments(alloc, &desired_enrichments);
    indexes_api.sortArtifactEnrichmentsByDependency(desired_enrichments.items);

    // Read/query opens attach to already-persisted index state only. Metadata-driven
    // materialization is owned by writable provisioners so stale readers never
    // race the single-writer root contract.
    if (!dbIndexReconciliationCanMutate(db)) return .{};

    const enrichment_summary = try ensureEnrichments(db, desired_enrichments.items);
    const resolver_summary = try ensureResolversWithOptions(alloc, db, indexes_json, .{
        .drain_backfill = options.drain_resolver_backfill,
    });
    const missing_indexes_removed = try removeMissingIndexes(alloc, db, indexes_json);
    const index_summary = try ensureIndexes(alloc, db, indexes_json);
    const enrichments_removed = try removeMissingEnrichments(alloc, db, desired_enrichments.items);
    const indexes_removed = missing_indexes_removed + index_summary.removed;
    if (index_summary.added > 0 or indexes_removed > 0 or enrichment_summary.changed() or enrichments_removed > 0 or resolver_summary.changed()) {
        const pending = db.pendingWorkStats();
        if (pending.enrichment.error_count == 0) {
            // Reconciliation persists catalog/applied-sequence state through the
            // primary store. Avoid forcing every newly-created empty index WAL
            // during create-table; repair/replay paths force-sync real index
            // mutations after applying data.
            try db.core.index_manager.syncAll(false);
        }
    }
    return .{
        .groups_considered = 0,
        .dbs_opened = 0,
        .indexes_added = index_summary.added,
        .indexes_removed = indexes_removed,
        .enrichments_added = enrichment_summary.added,
        .enrichments_updated = enrichment_summary.updated,
        .enrichments_removed = enrichments_removed,
        .resolvers_added = resolver_summary.added,
        .resolvers_updated = resolver_summary.updated,
        .resolvers_removed = resolver_summary.removed,
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

        var record: table_manager.RestoreProgressRecord = blk: {
            const progress_backup_id = try alloc.dupe(u8, restore.backup_id);
            errdefer alloc.free(progress_backup_id);
            const progress_location = try alloc.dupe(u8, restore.location);
            errdefer alloc.free(progress_location);
            break :blk .{
                .table_id = table.table_id,
                .node_id = local_node_id,
                .group_id = group_id,
                .backup_id = progress_backup_id,
                .location = progress_location,
                .snapshot_path = &.{},
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
        if (try desiredIndexContains(object, cfg.name)) continue;
        if (try db.deleteIndex(cfg.name)) removed += 1;
    }
    return removed;
}

const IndexEnsureSummary = struct {
    added: usize = 0,
    removed: usize = 0,
};

fn ensureIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !IndexEnsureSummary {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const current = try db.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, current);

    var summary: IndexEnsureSummary = .{};
    if (object.get("indexes")) |indexes_value| {
        const items = switch (indexes_value) {
            .array => |array| array.items,
            else => return error.InvalidTableIndexMetadata,
        };
        for (items) |item| {
            const name = try indexDefinitionName(item);
            const kind = try parseIndexKind(item);
            const config_value = indexDefinitionConfigValue(item);
            try ensureIndexDefinition(alloc, db, current, &summary, name, kind, config_value, true);
        }
        return summary;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        // Reserved top-level sections are handled by their own reconcilers, not
        // by the index reconciler.
        if (std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        const kind = try parseIndexKind(entry.value_ptr.*);
        try ensureIndexDefinition(alloc, db, current, &summary, entry.key_ptr.*, kind, entry.value_ptr.*, false);
    }
    return summary;
}

fn ensureIndexDefinition(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    current: []const db_mod.types.IndexConfig,
    summary: *IndexEnsureSummary,
    name: []const u8,
    kind: db_mod.types.IndexKind,
    config_value: std.json.Value,
    storage_config: bool,
) !void {
    const config_json = if (storage_config)
        try extractStoredIndexConfigJson(alloc, config_value)
    else
        try extractIndexConfigJsonForKind(alloc, name, kind, config_value);
    defer alloc.free(config_json);
    const desired = db_mod.types.IndexConfig{
        .name = name,
        .kind = kind,
        .config_json = config_json,
        .coverage_generation = coverage_policy.incarnation(config_value) orelse 0,
    };
    const existing = findIndexConfig(current, name);
    if (existing) |existing_cfg| {
        if (existing_cfg.kind == kind and indexKindConfigReconcileDeferred(kind)) {
            if (kind == .graph or
                (existing_cfg.coverage_generation == desired.coverage_generation and
                    try indexConfigsEqual(alloc, existing_cfg, desired))) return;
        }
    }
    if (existing) |existing_cfg| {
        if (try indexConfigsEqual(alloc, existing_cfg, desired)) return;
        if (try db.deleteIndex(desired.name)) summary.removed += 1;
    }
    try db.addIndex(.{
        .name = desired.name,
        .kind = desired.kind,
        .config_json = desired.config_json,
        .coverage_generation = desired.coverage_generation,
    });
    summary.added += 1;
}

fn desiredIndexContains(object: std.json.ObjectMap, name: []const u8) !bool {
    if (object.get("indexes")) |indexes_value| {
        const items = switch (indexes_value) {
            .array => |array| array.items,
            else => return error.InvalidTableIndexMetadata,
        };
        for (items) |item| {
            if (std.mem.eql(u8, try indexDefinitionName(item), name)) return true;
        }
        return false;
    }
    if (std.mem.eql(u8, name, "resolvers") or std.mem.eql(u8, name, "enrichments")) return false;
    return object.contains(name);
}

fn indexDefinitionName(value: std.json.Value) ![]const u8 {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const name_value = object.get("name") orelse return error.InvalidTableIndexMetadata;
    return switch (name_value) {
        .string => |name| if (name.len > 0) name else error.InvalidTableIndexMetadata,
        else => error.InvalidTableIndexMetadata,
    };
}

fn indexDefinitionConfigValue(value: std.json.Value) std.json.Value {
    const object = switch (value) {
        .object => |object| object,
        else => return value,
    };
    return object.get("config") orelse value;
}

fn findIndexConfig(configs: []const db_mod.types.IndexConfig, name: []const u8) ?db_mod.types.IndexConfig {
    for (configs) |cfg| {
        if (std.mem.eql(u8, cfg.name, name)) return cfg;
    }
    return null;
}

fn indexConfigsEqual(alloc: std.mem.Allocator, a: db_mod.types.IndexConfig, b: db_mod.types.IndexConfig) !bool {
    if (a.kind != b.kind) return false;
    if (a.kind == .full_text) return fullTextIndexConfigsEqual(alloc, a.config_json, b.config_json);
    if (a.kind == .algebraic) return algebraicIndexConfigsEqual(alloc, a.config_json, b.config_json);
    if ((a.kind == .dense_vector or a.kind == .sparse_vector) and
        a.coverage_generation != b.coverage_generation) return false;
    return std.mem.eql(u8, a.config_json, b.config_json);
}

fn indexKindConfigReconcileDeferred(kind: db_mod.types.IndexKind) bool {
    return switch (kind) {
        .dense_vector, .sparse_vector, .graph => true,
        .full_text, .algebraic => false,
    };
}

fn fullTextIndexConfigsEqual(alloc: std.mem.Allocator, a_json: []const u8, b_json: []const u8) !bool {
    var a_parsed = try std.json.parseFromSlice(std.json.Value, alloc, a_json, .{});
    defer a_parsed.deinit();
    var b_parsed = try std.json.parseFromSlice(std.json.Value, alloc, b_json, .{});
    defer b_parsed.deinit();
    return jsonValuesEqualIgnoringTopLevelEnrichments(a_parsed.value, b_parsed.value, true);
}

fn algebraicIndexConfigsEqual(alloc: std.mem.Allocator, a_json: []const u8, b_json: []const u8) !bool {
    var a_parsed = try std.json.parseFromSlice(std.json.Value, alloc, a_json, .{});
    defer a_parsed.deinit();
    var b_parsed = try std.json.parseFromSlice(std.json.Value, alloc, b_json, .{});
    defer b_parsed.deinit();
    return jsonValuesEqualIgnoringTopLevelEnrichments(a_parsed.value, b_parsed.value, false);
}

fn jsonValuesEqualIgnoringTopLevelEnrichments(a: std.json.Value, b: std.json.Value, top_level: bool) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .bool => |value| value == b.bool,
        .integer => |value| value == b.integer,
        .float => |value| value == b.float,
        .number_string => |value| std.mem.eql(u8, value, b.number_string),
        .string => |value| std.mem.eql(u8, value, b.string),
        .array => |array| blk: {
            if (array.items.len != b.array.items.len) break :blk false;
            for (array.items, b.array.items) |a_item, b_item| {
                if (!jsonValuesEqualIgnoringTopLevelEnrichments(a_item, b_item, false)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            const b_object = b.object;
            var a_count: usize = 0;
            var a_it = object.iterator();
            while (a_it.next()) |entry| {
                if (top_level and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                a_count += 1;
                const b_value = b_object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValuesEqualIgnoringTopLevelEnrichments(entry.value_ptr.*, b_value, false)) break :blk false;
            }
            var b_count: usize = 0;
            var b_it = b_object.iterator();
            while (b_it.next()) |entry| {
                if (top_level and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                b_count += 1;
            }
            break :blk a_count == b_count;
        },
    };
}

fn collectDesiredEnrichmentsFromJson(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    out: *std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig),
) !void {
    {
        const collected = try indexes_api.collectArtifactEnrichmentsFromTableIndexesJson(alloc, indexes_json);
        errdefer db_mod.types.freeEnrichmentConfigs(alloc, collected);
        try out.appendSlice(alloc, collected);
        alloc.free(collected);
    }
}

const EnrichmentEnsureSummary = struct {
    added: usize = 0,
    updated: usize = 0,

    fn changed(self: EnrichmentEnsureSummary) bool {
        return self.added > 0 or self.updated > 0;
    }
};

fn ensureEnrichments(db: *db_mod.DB, desired: []const db_mod.types.EnrichmentConfig) !EnrichmentEnsureSummary {
    var summary: EnrichmentEnsureSummary = .{};
    for (desired) |cfg| {
        switch (try db.upsertEnrichment(cfg)) {
            .added => summary.added += 1,
            .updated => summary.updated += 1,
            .unchanged => {},
        }
    }
    return summary;
}

fn dedupeDesiredEnrichments(
    alloc: std.mem.Allocator,
    desired: *std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig),
) void {
    var i: usize = 0;
    while (i < desired.items.len) {
        const cfg = desired.items[i];
        var duplicate = false;
        for (desired.items[0..i]) |prior| {
            if (std.mem.eql(u8, prior.name, cfg.name)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            i += 1;
            continue;
        }
        var removed = desired.orderedRemove(i);
        removed.deinit(alloc);
    }
}

fn removeMissingEnrichments(alloc: std.mem.Allocator, db: *db_mod.DB, desired: []const db_mod.types.EnrichmentConfig) !usize {
    const existing = try db.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, existing);

    var removed: usize = 0;
    var i = existing.len;
    while (i > 0) {
        i -= 1;
        const cfg = existing[i];
        if (findEnrichmentByName(desired, cfg.name)) |desired_cfg| {
            if (enrichmentConfigsEqual(cfg, desired_cfg)) continue;
        }
        if (db.deleteEnrichment(cfg.kind, cfg.name)) |deleted| {
            if (deleted) removed += 1;
        } else |err| switch (err) {
            error.EnrichmentInUse => continue,
            else => return err,
        }
    }
    return removed;
}

fn findEnrichmentByName(
    configs: []const db_mod.types.EnrichmentConfig,
    name: []const u8,
) ?db_mod.types.EnrichmentConfig {
    for (configs) |cfg| {
        if (std.mem.eql(u8, cfg.name, name)) return cfg;
    }
    return null;
}

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

fn enrichmentConfigsEqual(a: db_mod.types.EnrichmentConfig, b: db_mod.types.EnrichmentConfig) bool {
    return a.kind == b.kind and
        std.mem.eql(u8, a.name, b.name) and
        std.mem.eql(u8, a.field, b.field) and
        std.mem.eql(u8, a.template, b.template) and
        std.mem.eql(u8, a.source_artifact_name, b.source_artifact_name) and
        a.expected_dims == b.expected_dims and
        a.chunk_size == b.chunk_size and
        a.chunk_overlap == b.chunk_overlap and
        std.mem.eql(u8, a.chunker_json, b.chunker_json) and
        a.full_text_index == b.full_text_index and
        std.mem.eql(u8, a.content_type, b.content_type) and
        std.mem.eql(u8, a.producer_json, b.producer_json) and
        std.meta.eql(a.execution, b.execution);
}

pub const ResolverReconcileSummary = struct {
    added: usize = 0,
    updated: usize = 0,
    removed: usize = 0,
    unchanged: usize = 0,

    fn changed(self: @This()) bool {
        return self.added > 0 or self.updated > 0 or self.removed > 0;
    }
};

pub fn ensureResolvers(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !ResolverReconcileSummary {
    return try ensureResolversWithOptions(alloc, db, indexes_json, .{});
}

pub const EnsureResolverOptions = struct {
    drain_backfill: bool = true,
};

pub fn ensureResolversWithOptions(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    options: EnsureResolverOptions,
) !ResolverReconcileSummary {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    var desired = std.ArrayListUnmanaged(db_mod.ResolverConfig).empty;
    defer {
        for (desired.items) |*cfg| cfg.deinit(alloc);
        desired.deinit(alloc);
    }
    try collectDesiredResolvers(alloc, parsed.value, &desired);

    var summary: ResolverReconcileSummary = .{};
    for (desired.items) |cfg| {
        const result = try db.upsertResolverWithResultOptions(cfg, .{
            .drain_backfill = options.drain_backfill,
        });
        switch (result) {
            .inserted => summary.added += 1,
            .updated_backfill_required => summary.updated += 1,
            .updated_no_backfill => summary.unchanged += 1,
        }
    }

    const existing = try db.listResolvers(alloc);
    defer {
        for (existing) |*cfg| cfg.deinit(alloc);
        alloc.free(existing);
    }
    for (existing) |cfg| {
        if (desiredResolverContains(desired.items, cfg.name)) continue;
        if (try db.removeResolver(cfg.name)) summary.removed += 1;
    }
    return summary;
}

fn desiredResolverContains(desired: []const db_mod.ResolverConfig, name: []const u8) bool {
    for (desired) |cfg| {
        if (std.mem.eql(u8, cfg.name, name)) return true;
    }
    return false;
}

fn collectDesiredResolvers(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.ResolverConfig),
) !void {
    switch (value) {
        .object => |object| {
            if (object.get("resolvers")) |resolvers| {
                if (resolvers == .array) {
                    for (resolvers.array.items) |item| {
                        if (item != .object) continue;
                        const parsed = try std.json.parseFromValue(db_mod.ResolverConfig, alloc, item, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        // `parsed.value` is owned by the parse arena; clone with
                        // `alloc` so `out`'s entries free correctly (and so they
                        // outlive the arena).
                        defer parsed.deinit();
                        try out.append(alloc, try db_mod.ResolverConfig.clone(alloc, parsed.value));
                    }
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "resolvers")) continue;
                try collectDesiredResolvers(alloc, entry.value_ptr.*, out);
            }
        },
        .array => |array| {
            for (array.items) |item| try collectDesiredResolvers(alloc, item, out);
        },
        else => {},
    }
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

    var target_name_buf: [64]u8 = undefined;
    const target_name = if (schema_version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        try std.fmt.bufPrint(&target_name_buf, "full_text_index_v{d}", .{schema_version});

    // A rebuild marker is the durable, authoritative statement that this
    // schema index is not ready. Check it before opening the DB: schema
    // progress is polled by the metadata lifecycle while the resident writer
    // can spend minutes backfilling a large corpus. Reopening the same DB in
    // query mode on every poll maps the complete retained read index and scans
    // primary LSM metadata even though the answer is already on disk here.
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
    const type_value = value.object.get("type") orelse {
        if (looksLikeStoredAlgebraicIndexConfig(value)) return .algebraic;
        return .full_text;
    };
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
    if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
    if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
    if (std.mem.eql(u8, type_value.string, "hypervector")) return .dense_vector;
    if (std.mem.eql(u8, type_value.string, "embeddings")) {
        const sparse = try embeddingIndexSparseFlag(value);
        return if (sparse) .sparse_vector else .dense_vector;
    }
    return error.UnsupportedCreateTableRequest;
}

fn embeddingIndexSparseFlag(value: std.json.Value) !bool {
    if (value != .object) return false;
    if (value.object.get("sparse")) |sparse_value| {
        return switch (sparse_value) {
            .bool => sparse_value.bool,
            else => error.InvalidCreateTableRequest,
        };
    }
    const config_value = value.object.get("config") orelse return false;
    const config_object = switch (config_value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };
    const sparse_value = config_object.get("sparse") orelse return false;
    return switch (sparse_value) {
        .bool => sparse_value.bool,
        else => error.InvalidCreateTableRequest,
    };
}

fn looksLikeStoredAlgebraicIndexConfig(value: std.json.Value) bool {
    if (value != .object) return false;
    if (value.object.get("schema_version") == null and
        (value.object.get("version") == null or value.object.get("table") == null)) return false;
    return value.object.get("group_fields") != null or
        value.object.get("measure_fields") != null or
        value.object.get("time_fields") != null or
        value.object.get("materializations") != null;
}

fn extractIndexConfigJson(alloc: std.mem.Allocator, index_name: []const u8, value: std.json.Value) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    const kind = try parseIndexKind(value);
    return try extractIndexConfigJsonForKind(alloc, index_name, kind, value);
}

fn extractIndexConfigJsonForKind(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    kind: db_mod.types.IndexKind,
    value: std.json.Value,
) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    switch (kind) {
        .dense_vector, .sparse_vector => return try managed_embedder.translateEmbeddingsIndexConfigJson(alloc, index_name, value),
        else => {},
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (skipPublicIndexMetadataField(kind, entry.key_ptr.*)) continue;
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

fn extractStoredIndexConfigJson(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn skipPublicIndexMetadataField(kind: db_mod.types.IndexKind, field: []const u8) bool {
    if (std.mem.eql(u8, field, "type") or
        std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "description") or
        std.mem.eql(u8, field, "enrichments") or
        std.mem.eql(u8, field, "derive_from_schema"))
    {
        return true;
    }
    return kind != .algebraic and std.mem.eql(u8, field, "version");
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

    const replaced = try reconcileDbIndexesWithOptions(std.testing.allocator, &db, second, .{});
    try std.testing.expectEqual(@as(usize, 1), replaced.indexes_removed);
    try std.testing.expectEqual(@as(usize, 1), replaced.indexes_added);

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

test "table provisioner extracts public algebraic metadata as internal config" {
    const alloc = std.testing.allocator;
    const index_json =
        \\{"type":"algebraic","version":1,"table":"docs","schema_version":2,"derive_from_schema":true,"group_fields":[{"name":"customer","path":"customer","type":"string"}],"materializations":[]}
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
        \\    "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\    "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}},
        \\  "resolvers":[
        \\    {"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1",
        \\     "key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","candidate_search":"prefix","config_generation":1}
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
        \\    "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\    "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}},
        \\  "resolvers":[
        \\    {"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1",
        \\     "key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","candidate_search":"prefix","config_generation":2}
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
        \\    "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\    "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}},
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

    const second_summary = try reconcileReplicaRoot(
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
    try std.testing.expectEqual(@as(usize, 1), second_summary.indexes_added);
    try std.testing.expectEqual(@as(usize, 1), second_summary.indexes_removed);
    try std.testing.expectEqual(@as(usize, 2), second_summary.enrichments_added);
    try std.testing.expectEqual(@as(usize, 2), second_summary.enrichments_removed);

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

test "table provisioner schema progress returns not ready from rebuild marker without opening DB" {
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
    const rebuild_state = db_mod.backfill_state.RebuildState.init(index_root);
    try rebuild_state.update("doc:m");

    // There is deliberately no DB at db_path. If the progress probe attempts
    // DB.open instead of trusting the durable marker, this call fails rather
    // than returning the expected in-progress result.
    try std.testing.expect(!try localRangeHasSchemaVersionIndex(
        alloc,
        path,
        "docs",
        2007,
        2,
        1,
        .{},
    ));
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
