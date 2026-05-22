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

const builtin = @import("builtin");
const std = @import("std");
const metadata_openapi = @import("antfly_metadata_openapi");
const scraping = @import("antfly_scraping");
const common_secrets = @import("../common/secrets.zig");
const fs_paths = @import("../common/fs_paths.zig");
const backups_api = @import("backups.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_table_provisioner = @import("../metadata/table_provisioner.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_mod = @import("../raft/mod.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const doc_identity = @import("../storage/db/doc_identity.zig");
const backend_types = @import("../storage/backend_types.zig");
const hbc_mod = @import("../storage/hbc_adapter.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const storage_schema = @import("../storage/schema.zig");
const lmdb = @import("../storage/lmdb.zig");
const table_catalog = @import("table_catalog.zig");
const table_reads = @import("table_reads.zig");
const table_router = @import("table_router.zig");
const tables_api = @import("tables.zig");
const indexes_api = @import("indexes.zig");
const query_api = @import("query.zig");
const runtime_status = @import("runtime_status.zig");
const http_server = @import("http_server.zig");
const http_client = @import("http_client.zig");
const http_common = @import("../raft/transport/http_common.zig");
const std_http_listener = @import("../raft/transport/std_http_listener.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const distributed_txn = @import("distributed_txn.zig");
const build_options = @import("build_options");
const tracing = @import("../tracing/mod.zig");
const platform_time = @import("../platform/time.zig");
const Io = std.Io;

var txn_id_nonce: std.atomic.Value(u64) = .init(0);
const local_schema_json_key = "\x00\x00__metadata__:schema_json";
const max_cached_write_tables = 64;
const auto_bulk_ingest_min_batch_ops: usize = 100;
const auto_bulk_ingest_max_window_ops: usize = 25_000;
const auto_bulk_ingest_max_hbc_leaf_splits_per_publish: usize = 256;
// Client-side bulk loads often arrive as serial HTTP chunks. Finish implicit
// dense bulk ingest windows on max ops or idle, not elapsed open time, so an
// active upload does not start HBC replay/publish work mid-stream.
const auto_bulk_ingest_max_idle_ns: u64 = 2 * std.time.ns_per_s;
const auto_bulk_ingest_finish_options: backend_types.BulkIngestFinishOptions = .{
    .compact = false,
    .flush = true,
    .max_deferred_l0_runs = 64,
    .max_deferred_hbc_leaf_splits_per_publish = auto_bulk_ingest_max_hbc_leaf_splits_per_publish,
};

fn isTransientReplayVisibilityError(err: anyerror) bool {
    return err == error.WriterLocked or err == error.ReplayDocumentNotVisible;
}

const TestExecutionHook = struct {
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque) void,
};

var test_before_batch_execution_hook: ?TestExecutionHook = null;
var test_before_drop_table_delete_hook: ?TestExecutionHook = null;
var test_before_drop_index_work_hook: ?TestExecutionHook = null;
var test_before_restore_work_hook: ?TestExecutionHook = null;

const dropped_table_trash_dir_name = ".antfly-drop-trash";

fn runTestBeforeBatchExecutionHook() void {
    if (comptime builtin.is_test) {
        if (test_before_batch_execution_hook) |hook| hook.run(hook.ptr);
    }
}

fn runTestBeforeDropTableDeleteHook() void {
    if (comptime builtin.is_test) {
        if (test_before_drop_table_delete_hook) |hook| hook.run(hook.ptr);
    }
}

fn runTestBeforeDropIndexWorkHook() void {
    if (comptime builtin.is_test) {
        if (test_before_drop_index_work_hook) |hook| hook.run(hook.ptr);
    }
}

fn runTestBeforeRestoreWorkHook() void {
    if (comptime builtin.is_test) {
        if (test_before_restore_work_hook) |hook| hook.run(hook.ptr);
    }
}

const DroppedTableDeleteWork = struct {
    path: []u8,

    fn deletePath(path: []const u8, log_failure: bool) !void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        runTestBeforeDropTableDeleteHook();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch |err| {
            if (!log_failure) return err;
            std.log.warn("background dropped-table delete failed path={s} err={s}", .{
                path,
                @errorName(err),
            });
        };
    }

    fn run(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try deletePath(self.path, true);
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        std.heap.page_allocator.free(self.path);
        std.heap.page_allocator.destroy(self);
    }
};

fn droppedTableTrashDirPath(alloc: std.mem.Allocator, replica_root_dir: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ replica_root_dir, dropped_table_trash_dir_name });
}

fn droppedTableTrashPath(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    table_name: []const u8,
    group_id: u64,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}/table-{s}-group-{d}-{d}", .{
        replica_root_dir,
        dropped_table_trash_dir_name,
        table_name,
        group_id,
        platform_time.monotonicNs(),
    });
}

fn moveDroppedGroupPathToTrash(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    table_name: []const u8,
    group_id: u64,
) !?[]u8 {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);

    const trash_dir_path = try droppedTableTrashDirPath(alloc, replica_root_dir);
    defer alloc.free(trash_dir_path);
    const trash_path = try droppedTableTrashPath(alloc, replica_root_dir, table_name, group_id);
    errdefer alloc.free(trash_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), trash_dir_path);
    std.Io.Dir.rename(std.Io.Dir.cwd(), path, std.Io.Dir.cwd(), trash_path, io_impl.io()) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    return trash_path;
}

fn deleteGroupPathIfPresent(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
) !void {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), path);
}

pub const ProvisionedTableWriteCache = struct {
    alloc: std.mem.Allocator,
    lsm_cache: ?*lsm_backend.Cache = null,
    hbc_cache: ?*hbc_mod.Cache = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    open_mutex: std.atomic.Mutex = .unlocked,
    entry_lifecycle_mutex: std.atomic.Mutex = .unlocked,
    hit_count: std.atomic.Value(u64) = .init(0),
    miss_count: std.atomic.Value(u64) = .init(0),
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    retired_entries: std.ArrayListUnmanaged(*Entry) = .empty,
    table_metadata: std.ArrayListUnmanaged(TableMetadata) = .empty,
    active_bulk_ingest_sessions: std.ArrayListUnmanaged(ActiveBulkIngestSession) = .empty,

    pub const CacheStats = struct {
        hit_count: u64 = 0,
        miss_count: u64 = 0,
    };

    pub const AutoBulkIngestStats = struct {
        cached_entries: u64 = 0,
        open_entries: u64 = 0,
        active_leases: u64 = 0,
        finish_requested_entries: u64 = 0,
        idle_expired_entries: u64 = 0,
        active_bulk_sessions: u64 = 0,
        total_ops: u64 = 0,
        oldest_idle_ns: u64 = 0,

        fn merge(self: *AutoBulkIngestStats, other: AutoBulkIngestStats) void {
            self.cached_entries += other.cached_entries;
            self.open_entries += other.open_entries;
            self.active_leases += other.active_leases;
            self.finish_requested_entries += other.finish_requested_entries;
            self.idle_expired_entries += other.idle_expired_entries;
            self.active_bulk_sessions += other.active_bulk_sessions;
            self.total_ops += other.total_ops;
            self.oldest_idle_ns = @max(self.oldest_idle_ns, other.oldest_idle_ns);
        }
    };

    pub const CachedDb = struct {
        cache: ?*ProvisionedTableWriteCache = null,
        entry: ?*Entry = null,
        db: *db_mod.DB,
        schema_json: ?[]const u8,
        owned_db: ?*db_mod.DB = null,

        pub fn deinit(self: *CachedDb, alloc: std.mem.Allocator) void {
            if (self.entry) |entry| {
                if (self.cache) |cache| cache.releaseEntry(entry);
                self.entry = null;
            }
            if (self.owned_db) |owned| {
                owned.close();
                alloc.destroy(owned);
            }
            self.* = undefined;
        }
    };

    const PreparedOpen = struct {
        indexes_json: ?[]u8 = null,
        schema_json: ?[]u8 = null,

        fn deinit(self: *PreparedOpen, alloc: std.mem.Allocator) void {
            if (self.indexes_json) |value| alloc.free(value);
            if (self.schema_json) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    const GetOrPrepareOpen = union(enum) {
        cached: CachedDb,
        prepared: PreparedOpen,
    };

    const Entry = struct {
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []u8,
        db: db_mod.DB,
        schema_json: ?[]u8 = null,
        active_leases: usize = 0,
        retired: bool = false,
        bulk_ingest_session_open: bool = false,
        auto_bulk_ingest_session_open: bool = false,
        auto_bulk_ingest_ops: usize = 0,
        auto_bulk_ingest_started_ns: u64 = 0,
        auto_bulk_ingest_last_ns: u64 = 0,
        auto_bulk_ingest_finish_requested: bool = false,

        fn deinit(self: *Entry, alloc: std.mem.Allocator) void {
            if (self.bulk_ingest_session_open) {
                if (self.auto_bulk_ingest_session_open) {
                    self.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options) catch |err| {
                        std.log.warn("auto bulk ingest finish failed before cached db close table={s} err={s}", .{
                            self.table_name,
                            @errorName(err),
                        });
                        self.db.abortDenseAutoBulkIngestSession();
                    };
                } else {
                    self.db.abortBulkIngestSession();
                }
                self.bulk_ingest_session_open = false;
                self.auto_bulk_ingest_session_open = false;
                self.auto_bulk_ingest_ops = 0;
                self.auto_bulk_ingest_started_ns = 0;
                self.auto_bulk_ingest_last_ns = 0;
                self.auto_bulk_ingest_finish_requested = false;
            }
            // Read-side cache invalidation must not turn the first query after
            // a large weak-sync load into a full derived-index drain. DB.close()
            // tears down async workers and flushes owned storage; callers that
            // require full-index visibility must request it through sync_level
            // or the index status path before reading.
            const pending = self.db.pendingWorkStats();
            if (pending.enrichment.error_count == 0) {
                self.db.core.index_manager.syncAll(false) catch |err| {
                    std.log.warn("provisioned write cache sync failed before close: {}", .{err});
                };
            }
            self.db.close();
            alloc.free(self.table_name);
            if (self.schema_json) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    const ActiveBulkIngestSession = struct {
        table_name: []u8,
        depth: usize = 1,

        fn deinit(self: *ActiveBulkIngestSession, alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            self.* = undefined;
        }
    };

    const TableMetadata = struct {
        table_name: []u8,
        indexes_json: ?[]u8,
        schema_json: ?[]u8,

        fn deinit(self: *TableMetadata, alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            if (self.indexes_json) |value| alloc.free(value);
            if (self.schema_json) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    pub fn init(alloc: std.mem.Allocator) ProvisionedTableWriteCache {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *ProvisionedTableWriteCache) void {
        for (self.entries.items) |entry| {
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
        }
        self.entries.deinit(self.alloc);
        for (self.retired_entries.items) |entry| {
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
        }
        self.retired_entries.deinit(self.alloc);
        for (self.table_metadata.items) |*metadata| metadata.deinit(self.alloc);
        self.table_metadata.deinit(self.alloc);
        for (self.active_bulk_ingest_sessions.items) |*session| session.deinit(self.alloc);
        self.active_bulk_ingest_sessions.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn clear(self: *ProvisionedTableWriteCache) void {
        for (self.entries.items) |entry| self.retireEntryLocked(entry);
        self.entries.clearRetainingCapacity();
        for (self.table_metadata.items) |*metadata| metadata.deinit(self.alloc);
        self.table_metadata.clearRetainingCapacity();
        for (self.active_bulk_ingest_sessions.items) |*session| session.deinit(self.alloc);
        self.active_bulk_ingest_sessions.clearRetainingCapacity();
    }

    pub fn cacheStats(self: *const ProvisionedTableWriteCache) CacheStats {
        return .{
            .hit_count = self.hit_count.load(.monotonic),
            .miss_count = self.miss_count.load(.monotonic),
        };
    }

    pub fn autoBulkIngestStatsLocked(self: *const ProvisionedTableWriteCache, now_ns: u64) AutoBulkIngestStats {
        var stats = AutoBulkIngestStats{
            .cached_entries = @intCast(self.entries.items.len),
            .active_bulk_sessions = @intCast(self.active_bulk_ingest_sessions.items.len),
        };
        for (self.entries.items) |entry| {
            stats.active_leases += entry.active_leases;
            if (!entry.auto_bulk_ingest_session_open) continue;
            stats.open_entries += 1;
            stats.total_ops += entry.auto_bulk_ingest_ops;
            if (entry.auto_bulk_ingest_finish_requested) stats.finish_requested_entries += 1;
            if (entry.auto_bulk_ingest_last_ns > 0) {
                const idle_ns = now_ns -| entry.auto_bulk_ingest_last_ns;
                stats.oldest_idle_ns = @max(stats.oldest_idle_ns, idle_ns);
                if (idle_ns >= auto_bulk_ingest_max_idle_ns) stats.idle_expired_entries += 1;
            }
        }
        return stats;
    }

    pub fn autoBulkIngestMaxIdleNs() u64 {
        return auto_bulk_ingest_max_idle_ns;
    }

    pub fn getOrOpenLocked(
        self: *ProvisionedTableWriteCache,
        path: []const u8,
        catalog: table_catalog.CatalogSource,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !CachedDb {
        return try self.getOrOpenLockedMode(path, catalog, group_id, lsm_root_generation, table_name, .default);
    }

    fn getOrOpenLockedMode(
        self: *ProvisionedTableWriteCache,
        path: []const u8,
        catalog: table_catalog.CatalogSource,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
    ) !CachedDb {
        const OpenedDb = struct {
            db: db_mod.DB,
            start_bulk_session: bool,
        };

        const openDbForMode = struct {
            fn run(
                allocator: std.mem.Allocator,
                db_path: []const u8,
                indexes_json: ?[]const u8,
                cache: ?*lsm_backend.Cache,
                vector_cache: ?*hbc_mod.Cache,
                root_generation: u64,
                manager: ?*resource_manager_mod.ResourceManager,
                open_mode: ManagedDbOpenMode,
                runtime: ?*db_mod.background_runtime.BackendRuntime,
                local_termite_provider: ?managed_embedder.LocalTermiteProvider,
                secret_store: ?*common_secrets.FileStore,
                identity_namespace: ?doc_identity.Namespace,
            ) !OpenedDb {
                var db = if (indexes_json) |managed_indexes_json|
                    try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(
                        allocator,
                        db_path,
                        managed_indexes_json,
                        cache,
                        vector_cache,
                        root_generation,
                        manager,
                        open_mode,
                        runtime,
                        local_termite_provider,
                        secret_store,
                        null,
                        identity_namespace,
                    )
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .identity_namespace = identity_namespace,
                        .prefer_existing_identity_namespace = identity_namespace != null,
                        .open_mode = switch (open_mode) {
                            .default => .writer,
                            .default_async, .writer_no_replay => .writer_no_replay,
                            .startup_catch_up, .restore_repair => .writer_no_replay,
                            .status_only => .status_only,
                        },
                        .index_open_parallelism = if (open_mode == .default_async or open_mode == .writer_no_replay) 1 else null,
                    });
                errdefer db.close();
                try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
                return .{
                    .db = db,
                    .start_bulk_session = switch (open_mode) {
                        .default, .default_async, .writer_no_replay => true,
                        .startup_catch_up, .restore_repair, .status_only => false,
                    },
                };
            }
        }.run;

        const metadata = try self.getOrLoadMetadataLocked(catalog, table_name);
        const identity_namespace = try loadTableIdentityNamespaceForGroup(self.alloc, catalog, table_name, group_id);
        self.pruneStaleEntriesForGroupTableLocked(group_id, lsm_root_generation, table_name);
        if (mode == .status_only) {
            const opened = try openDbForMode(
                self.alloc,
                path,
                metadata.indexes_json,
                self.lsm_cache,
                self.hbc_cache,
                lsm_root_generation,
                self.resource_manager,
                mode,
                self.backend_runtime,
                self.local_termite_provider,
                self.secret_store,
                identity_namespace,
            );
            const owned_db = try self.alloc.create(db_mod.DB);
            errdefer self.alloc.destroy(owned_db);
            owned_db.* = opened.db;
            return .{
                .cache = self,
                .db = owned_db,
                .schema_json = metadata.schema_json,
                .owned_db = owned_db,
            };
        }
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) {
                _ = self.hit_count.fetchAdd(1, .monotonic);
                lockAtomic(&self.entry_lifecycle_mutex);
                defer self.entry_lifecycle_mutex.unlock();
                entry.active_leases += 1;
                return .{
                    .cache = self,
                    .entry = entry,
                    .db = &entry.db,
                    .schema_json = entry.schema_json,
                };
            }
        }

        _ = self.miss_count.fetchAdd(1, .monotonic);
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        var opened = try openDbForMode(
            self.alloc,
            path,
            metadata.indexes_json,
            self.lsm_cache,
            self.hbc_cache,
            lsm_root_generation,
            self.resource_manager,
            mode,
            self.backend_runtime,
            self.local_termite_provider,
            self.secret_store,
            identity_namespace,
        );
        errdefer opened.db.close();
        const start_bulk_session = opened.start_bulk_session and self.bulkIngestSessionActiveForTable(table_name);
        if (start_bulk_session) {
            try opened.db.beginBulkIngestSession();
            errdefer opened.db.abortBulkIngestSession();
        }
        try self.retired_entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(owned_entry);
        owned_entry.* = .{
            .group_id = group_id,
            .lsm_root_generation = lsm_root_generation,
            .table_name = owned_table_name,
            .db = opened.db,
            .schema_json = if (metadata.schema_json) |value| try self.alloc.dupe(u8, value) else null,
            .active_leases = 1,
            .bulk_ingest_session_open = start_bulk_session,
        };
        try self.entries.append(self.alloc, owned_entry);
        return .{
            .cache = self,
            .entry = owned_entry,
            .db = &owned_entry.db,
            .schema_json = metadata.schema_json,
        };
    }

    fn getOrPrepareOpenLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !GetOrPrepareOpen {
        self.pruneStaleEntriesForGroupTableLocked(group_id, lsm_root_generation, table_name);
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) {
                _ = self.hit_count.fetchAdd(1, .monotonic);
                lockAtomic(&self.entry_lifecycle_mutex);
                defer self.entry_lifecycle_mutex.unlock();
                entry.active_leases += 1;
                return .{
                    .cached = .{
                        .cache = self,
                        .entry = entry,
                        .db = &entry.db,
                        .schema_json = entry.schema_json,
                    },
                };
            }
        }

        _ = self.miss_count.fetchAdd(1, .monotonic);
        return .{ .prepared = .{} };
    }

    fn snapshotLeaseLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) ?CachedDb {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (entry.lsm_root_generation != lsm_root_generation) continue;
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            lockAtomic(&self.entry_lifecycle_mutex);
            defer self.entry_lifecycle_mutex.unlock();
            entry.active_leases += 1;
            return .{
                .cache = self,
                .entry = entry,
                .db = &entry.db,
                .schema_json = entry.schema_json,
            };
        }
        return null;
    }

    fn leaseEntryLocked(self: *ProvisionedTableWriteCache, entry: *Entry) CachedDb {
        lockAtomic(&self.entry_lifecycle_mutex);
        defer self.entry_lifecycle_mutex.unlock();
        entry.active_leases += 1;
        return .{
            .cache = self,
            .entry = entry,
            .db = &entry.db,
            .schema_json = entry.schema_json,
        };
    }

    fn adoptPreparedOpenLocked(
        self: *ProvisionedTableWriteCache,
        opened: *?db_mod.DB,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
        prepared: *PreparedOpen,
    ) !CachedDb {
        self.pruneStaleEntriesForGroupTableLocked(group_id, lsm_root_generation, table_name);
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) {
                if (opened.*) |*db| db.close();
                opened.* = null;
                _ = self.hit_count.fetchAdd(1, .monotonic);
                lockAtomic(&self.entry_lifecycle_mutex);
                defer self.entry_lifecycle_mutex.unlock();
                entry.active_leases += 1;
                return .{
                    .cache = self,
                    .entry = entry,
                    .db = &entry.db,
                    .schema_json = entry.schema_json,
                };
            }
        }

        var db = opened.* orelse unreachable;
        errdefer db.close();

        const start_bulk_session = switch (mode) {
            .default, .default_async, .writer_no_replay => self.bulkIngestSessionActiveForTable(table_name),
            .startup_catch_up, .restore_repair, .status_only => false,
        };
        if (start_bulk_session) {
            try db.beginBulkIngestSession();
            errdefer db.abortBulkIngestSession();
        }

        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        try self.retired_entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(owned_entry);
        owned_entry.* = .{
            .group_id = group_id,
            .lsm_root_generation = lsm_root_generation,
            .table_name = owned_table_name,
            .db = db,
            .schema_json = prepared.schema_json,
            .active_leases = 1,
            .bulk_ingest_session_open = start_bulk_session,
        };
        prepared.schema_json = null;
        errdefer owned_entry.deinit(self.alloc);
        try self.entries.append(self.alloc, owned_entry);
        opened.* = null;
        return .{
            .cache = self,
            .entry = owned_entry,
            .db = &owned_entry.db,
            .schema_json = owned_entry.schema_json,
        };
    }

    pub fn getLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) ?*db_mod.DB {
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) return &entry.db;
        }
        return null;
    }

    pub fn snapshotRuntimeStatusesLocked(
        self: *ProvisionedTableWriteCache,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, entry.table_name, table_name)) count += 1;
        }
        if (count == 0) return null;

        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, count);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(items);
        }

        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            items[initialized] = .{
                .group_id = entry.group_id,
                .stats = try entry.db.runtimeStatusStatsConsistent(alloc),
            };
            initialized += 1;
        }
        return .{ .items = items };
    }

    fn pruneStaleEntriesForGroupTableLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) void {
        var i: usize = 0;
        while (i < self.entries.items.len) {
            const entry = self.entries.items[i];
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) {
                i += 1;
                continue;
            }
            if (entry.lsm_root_generation == lsm_root_generation) {
                i += 1;
                continue;
            }
            _ = self.entries.orderedRemove(i);
            self.retireEntryLocked(entry);
        }
    }

    fn invalidateTable(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        self.removeDbEntriesForTable(table_name);

        var i: usize = 0;
        while (i < self.table_metadata.items.len) {
            if (!std.mem.eql(u8, self.table_metadata.items[i].table_name, table_name)) {
                i += 1;
                continue;
            }
            var removed = self.table_metadata.orderedRemove(i);
            removed.deinit(self.alloc);
        }
    }

    fn removeDbEntriesForTable(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (!std.mem.eql(u8, self.entries.items[i].table_name, table_name)) {
                i += 1;
                continue;
            }
            const removed = self.entries.orderedRemove(i);
            self.retireEntryLocked(removed);
        }
    }

    fn hasLiveEntryForGroupTableLocked(
        self: *const ProvisionedTableWriteCache,
        group_id: u64,
        table_name: []const u8,
    ) bool {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            return true;
        }
        return false;
    }

    fn hasLiveEntryForTableLocked(
        self: *const ProvisionedTableWriteCache,
        table_name: []const u8,
    ) bool {
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            return true;
        }
        return false;
    }

    fn hasForegroundStateForGroupTableLocked(
        self: *const ProvisionedTableWriteCache,
        group_id: u64,
        table_name: []const u8,
    ) bool {
        if (self.bulkIngestSessionActiveForTable(table_name)) return true;
        return self.hasLiveEntryForGroupTableLocked(group_id, table_name);
    }

    pub fn beginBulkIngestLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) !void {
        for (self.active_bulk_ingest_sessions.items) |*session| {
            if (!std.mem.eql(u8, session.table_name, table_name)) continue;
            session.depth += 1;
            return;
        }

        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        try self.active_bulk_ingest_sessions.ensureUnusedCapacity(self.alloc, 1);
        var started_any = false;
        errdefer if (started_any) {
            for (self.entries.items) |entry| {
                if (!std.mem.eql(u8, entry.table_name, table_name) or !entry.bulk_ingest_session_open) continue;
                entry.db.abortBulkIngestSession();
                entry.bulk_ingest_session_open = false;
            }
        };
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name) or entry.bulk_ingest_session_open) continue;
            try entry.db.beginBulkIngestSession();
            entry.bulk_ingest_session_open = true;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
            started_any = true;
        }
        self.active_bulk_ingest_sessions.appendAssumeCapacity(.{ .table_name = owned_table_name });
    }

    pub fn finishBulkIngestLocked(
        self: *ProvisionedTableWriteCache,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        const idx = self.findActiveBulkIngestSession(table_name) orelse return;
        if (self.active_bulk_ingest_sessions.items[idx].depth > 1) {
            self.active_bulk_ingest_sessions.items[idx].depth -= 1;
            return;
        }

        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name) or !entry.bulk_ingest_session_open) continue;
            if (entry.auto_bulk_ingest_session_open) {
                try entry.db.finishDenseAutoBulkIngestSessionWithOptions(options);
            } else {
                try entry.db.finishBulkIngestSessionWithOptions(options);
            }
            entry.bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
        }
        var removed = self.active_bulk_ingest_sessions.orderedRemove(idx);
        removed.deinit(self.alloc);
    }

    pub fn abortBulkIngestLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        const idx = self.findActiveBulkIngestSession(table_name) orelse return;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name) or !entry.bulk_ingest_session_open) continue;
            if (entry.auto_bulk_ingest_session_open) {
                entry.db.abortDenseAutoBulkIngestSession();
            } else {
                entry.db.abortBulkIngestSession();
            }
            entry.bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
        }
        var removed = self.active_bulk_ingest_sessions.orderedRemove(idx);
        removed.deinit(self.alloc);
    }

    pub fn ensureAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8, now_ns: u64) !void {
        if (self.bulkIngestSessionActiveForTable(table_name)) return;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.bulk_ingest_session_open) {
                try entry.db.beginDenseAutoBulkIngestSession();
                entry.bulk_ingest_session_open = true;
            }
            entry.auto_bulk_ingest_session_open = true;
            if (entry.auto_bulk_ingest_started_ns == 0) entry.auto_bulk_ingest_started_ns = now_ns;
            entry.auto_bulk_ingest_last_ns = now_ns;
            return;
        }
    }

    pub fn recordAutoBulkIngestOpsLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8, ops: usize, now_ns: u64) !void {
        if (ops == 0) return;
        var should_finish = false;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or !entry.auto_bulk_ingest_session_open) continue;
            entry.auto_bulk_ingest_ops +|= ops;
            entry.auto_bulk_ingest_last_ns = now_ns;
            should_finish = should_finish or entry.auto_bulk_ingest_ops >= auto_bulk_ingest_max_window_ops;
            if (should_finish) entry.auto_bulk_ingest_finish_requested = true;
        }
    }

    pub fn rollRequestedAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8, now_ns: u64) !bool {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.auto_bulk_ingest_session_open or !entry.auto_bulk_ingest_finish_requested) return false;
            if (entry.active_leases > 1) return false;
            try entry.db.rollDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options);
            entry.auto_bulk_ingest_session_open = true;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = now_ns;
            entry.auto_bulk_ingest_last_ns = now_ns;
            entry.auto_bulk_ingest_finish_requested = false;
            return true;
        }
        return false;
    }

    pub fn finishAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8) !void {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or !entry.auto_bulk_ingest_session_open) continue;
            try entry.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options);
            entry.bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
        }
        self.removeInactiveBulkIngestSessionLocked(table_name);
    }

    pub fn finishExpiredAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, now_ns: u64) !bool {
        return self.finishExpiredAutoBulkIngestLockedWithStatusLeases(now_ns, std.heap.page_allocator, null);
    }

    fn finishExpiredAutoBulkIngestLockedWithStatusLeases(
        self: *ProvisionedTableWriteCache,
        now_ns: u64,
        lease_alloc: std.mem.Allocator,
        finished_leases: ?*std.ArrayListUnmanaged(CachedDb),
    ) !bool {
        var first_err: ?anyerror = null;
        var finished_any = false;
        for (self.entries.items) |entry| {
            if (!entry.auto_bulk_ingest_session_open) continue;
            if (entry.active_leases != 0) continue;
            const idle_expired = entry.auto_bulk_ingest_last_ns > 0 and now_ns -| entry.auto_bulk_ingest_last_ns >= auto_bulk_ingest_max_idle_ns;
            if (!entry.auto_bulk_ingest_finish_requested and !idle_expired) continue;
            if (!idle_expired and entry.auto_bulk_ingest_finish_requested) {
                continue;
            } else {
                entry.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options) catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
                entry.bulk_ingest_session_open = false;
                entry.auto_bulk_ingest_session_open = false;
                entry.auto_bulk_ingest_ops = 0;
                entry.auto_bulk_ingest_started_ns = 0;
                entry.auto_bulk_ingest_last_ns = 0;
                entry.auto_bulk_ingest_finish_requested = false;
                self.removeInactiveBulkIngestSessionLocked(entry.table_name);
            }
            if (finished_leases) |leases| {
                self.appendRuntimeStatusLeaseForEntryLocked(lease_alloc, entry, leases) catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
            }
            finished_any = true;
        }
        if (first_err) |err| return err;
        return finished_any;
    }

    fn appendRuntimeStatusLeaseForEntryLocked(
        self: *ProvisionedTableWriteCache,
        alloc: std.mem.Allocator,
        entry: *Entry,
        out: *std.ArrayListUnmanaged(CachedDb),
    ) !void {
        lockAtomic(&self.entry_lifecycle_mutex);
        entry.active_leases += 1;
        self.entry_lifecycle_mutex.unlock();
        errdefer self.releaseEntry(entry);

        try out.append(alloc, .{
            .cache = self,
            .entry = entry,
            .db = &entry.db,
            .schema_json = entry.schema_json,
        });
    }

    fn leaseLsmMaintenanceEntryLocked(self: *ProvisionedTableWriteCache, comptime best_effort: bool) ?CachedDb {
        var best_entry: ?*Entry = null;
        var best_score: u64 = 0;
        for (self.entries.items) |entry| {
            if (entry.bulk_ingest_session_open) continue;
            if (entry.db.hasActiveDenseBulkWork()) continue;
            const score = if (best_effort) entry.db.lsmMaintenanceDebtHint() else entry.db.lsmMaintenanceScore();
            if (score > best_score) {
                best_score = score;
                best_entry = entry;
            }
        }
        if (best_score == 0) return null;
        return self.leaseEntryLocked(best_entry.?);
    }

    pub fn leaseLsmMaintenanceRoundLocked(self: *ProvisionedTableWriteCache) ?CachedDb {
        return self.leaseLsmMaintenanceEntryLocked(false);
    }

    pub fn leaseLsmMaintenanceRoundBestEffortLocked(self: *ProvisionedTableWriteCache) ?CachedDb {
        return self.leaseLsmMaintenanceEntryLocked(true);
    }

    pub fn maxLsmMaintenanceScoreLocked(self: *const ProvisionedTableWriteCache) u64 {
        var score: u64 = 0;
        for (self.entries.items) |entry| {
            if (entry.bulk_ingest_session_open) continue;
            if (entry.db.hasActiveDenseBulkWork()) continue;
            score = @max(score, entry.db.lsmMaintenanceDebtHint());
        }
        return score;
    }

    fn bulkIngestSessionActiveForTable(self: *const ProvisionedTableWriteCache, table_name: []const u8) bool {
        return self.findActiveBulkIngestSession(table_name) != null;
    }

    fn bulkIngestSessionOpenForTable(self: *const ProvisionedTableWriteCache, table_name: []const u8) bool {
        if (self.bulkIngestSessionActiveForTable(table_name)) return true;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) return true;
        }
        return false;
    }

    fn removeInactiveBulkIngestSessionLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) return;
        }
        const idx = self.findActiveBulkIngestSession(table_name) orelse return;
        var removed = self.active_bulk_ingest_sessions.orderedRemove(idx);
        removed.deinit(self.alloc);
    }

    fn findActiveBulkIngestSession(self: *const ProvisionedTableWriteCache, table_name: []const u8) ?usize {
        for (self.active_bulk_ingest_sessions.items, 0..) |session, i| {
            if (std.mem.eql(u8, session.table_name, table_name)) return i;
        }
        return null;
    }

    fn evictOldestTable(self: *ProvisionedTableWriteCache) void {
        if (self.table_metadata.items.len == 0) return;
        const table_name = self.table_metadata.items[0].table_name;
        self.removeDbEntriesForTable(table_name);
        var removed = self.table_metadata.orderedRemove(0);
        removed.deinit(self.alloc);
    }

    fn getOrLoadMetadataLocked(
        self: *ProvisionedTableWriteCache,
        catalog: table_catalog.CatalogSource,
        table_name: []const u8,
    ) !*const TableMetadata {
        for (self.table_metadata.items) |*metadata| {
            if (std.mem.eql(u8, metadata.table_name, table_name)) return metadata;
        }

        if (self.table_metadata.items.len >= max_cached_write_tables) self.evictOldestTable();
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);

        var snapshot = try catalog.adminSnapshot();
        defer catalog.freeAdminSnapshot(&snapshot);

        var indexes_json: ?[]u8 = null;
        errdefer if (indexes_json) |value| self.alloc.free(value);
        var schema_json: ?[]u8 = null;
        errdefer if (schema_json) |value| self.alloc.free(value);

        if (tables_api.findTableByName(&snapshot, table_name)) |table| {
            indexes_json = try self.alloc.dupe(u8, table.indexes_json);
            schema_json = try self.alloc.dupe(u8, table.schema_json);
        }

        try self.table_metadata.append(self.alloc, .{
            .table_name = owned_table_name,
            .indexes_json = indexes_json,
            .schema_json = schema_json,
        });
        return &self.table_metadata.items[self.table_metadata.items.len - 1];
    }

    fn releaseEntry(self: *ProvisionedTableWriteCache, entry: *Entry) void {
        lockAtomic(&self.entry_lifecycle_mutex);
        defer self.entry_lifecycle_mutex.unlock();
        std.debug.assert(entry.active_leases > 0);
        entry.active_leases -= 1;
        if (entry.active_leases == 0 and entry.retired) {
            self.destroyRetiredEntryLocked(entry);
        }
    }

    fn retireEntryLocked(self: *ProvisionedTableWriteCache, entry: *Entry) void {
        lockAtomic(&self.entry_lifecycle_mutex);
        defer self.entry_lifecycle_mutex.unlock();
        if (entry.retired) return;
        entry.retired = true;
        if (entry.active_leases == 0) {
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
            return;
        }
        self.retired_entries.appendAssumeCapacity(entry);
    }

    fn destroyRetiredEntryLocked(self: *ProvisionedTableWriteCache, entry: *Entry) void {
        var i: usize = 0;
        while (i < self.retired_entries.items.len) : (i += 1) {
            if (self.retired_entries.items[i] != entry) continue;
            _ = self.retired_entries.orderedRemove(i);
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
            return;
        }
        unreachable;
    }
};

const HostedManagedDbCache = struct {
    replica_root_dir: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    write_cache: ProvisionedTableWriteCache,

    fn init(alloc: std.mem.Allocator, replica_root_dir: []const u8) !*HostedManagedDbCache {
        const cache = try alloc.create(HostedManagedDbCache);
        errdefer alloc.destroy(cache);
        cache.* = .{
            .replica_root_dir = try alloc.dupe(u8, replica_root_dir),
            .write_cache = ProvisionedTableWriteCache.init(alloc),
        };
        return cache;
    }
};

var hosted_managed_db_cache_registry_mutex: std.atomic.Mutex = .unlocked;
var hosted_managed_db_cache_registry: std.ArrayListUnmanaged(*HostedManagedDbCache) = .empty;

pub fn closeHostedManagedDbCacheForRoot(replica_root_dir: []const u8) void {
    const alloc = std.heap.page_allocator;
    var removed: ?*HostedManagedDbCache = null;
    lockAtomic(&hosted_managed_db_cache_registry_mutex);
    {
        defer hosted_managed_db_cache_registry_mutex.unlock();
        for (hosted_managed_db_cache_registry.items, 0..) |cache, idx| {
            if (!std.mem.eql(u8, cache.replica_root_dir, replica_root_dir)) continue;
            removed = hosted_managed_db_cache_registry.orderedRemove(idx);
            break;
        }
    }

    const cache = removed orelse return;
    lockAtomic(&cache.mutex);
    cache.write_cache.deinit();
    cache.mutex.unlock();
    alloc.free(cache.replica_root_dir);
    alloc.destroy(cache);
}

fn hostedManagedDbCacheForRoot(replica_root_dir: []const u8) !*HostedManagedDbCache {
    const alloc = std.heap.page_allocator;
    lockAtomic(&hosted_managed_db_cache_registry_mutex);
    defer hosted_managed_db_cache_registry_mutex.unlock();

    for (hosted_managed_db_cache_registry.items) |cache| {
        if (std.mem.eql(u8, cache.replica_root_dir, replica_root_dir)) return cache;
    }

    const cache = try HostedManagedDbCache.init(alloc, replica_root_dir);
    errdefer {
        alloc.free(cache.replica_root_dir);
        alloc.destroy(cache);
    }
    try hosted_managed_db_cache_registry.append(alloc, cache);
    return cache;
}

fn hostedManagedDbCacheForRootIfPresent(replica_root_dir: []const u8) ?*HostedManagedDbCache {
    lockAtomic(&hosted_managed_db_cache_registry_mutex);
    defer hosted_managed_db_cache_registry_mutex.unlock();

    for (hosted_managed_db_cache_registry.items) |cache| {
        if (std.mem.eql(u8, cache.replica_root_dir, replica_root_dir)) return cache;
    }
    return null;
}

fn parseJsonBodyIgnoreUnknown(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{ .ignore_unknown_fields = true });
}

fn jsonValueContainsText(value: std.json.Value, needle: []const u8) bool {
    switch (value) {
        .string => |text| return std.mem.indexOf(u8, text, needle) != null,
        .array => |items| {
            for (items.items) |item| {
                if (jsonValueContainsText(item, needle)) return true;
            }
            return false;
        },
        .object => |obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (std.mem.indexOf(u8, entry.key_ptr.*, needle) != null) return true;
                if (jsonValueContainsText(entry.value_ptr.*, needle)) return true;
            }
            return false;
        },
        else => return false,
    }
}

const TestEmbeddingRequest = struct {
    model: std.json.Value,
    input: std.json.Value,
};

pub const TableWriteSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: tables_api.CreateTableRequest,
        ) anyerror!?void = null,
        update_schema: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            schema_json: []const u8,
        ) anyerror!?void = null,
        create_index: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            index_json: []const u8,
        ) anyerror!?void = null,
        drop_index: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
        ) anyerror!?void = null,
        drop_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            group_ids: []const u64,
        ) anyerror!?void = null,
        backup_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            plan: backups_api.TableBackupPlan,
        ) anyerror!?[]backups_api.ShardSnapshot = null,
        restore_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            plan: backups_api.TableRestorePlan,
        ) anyerror!?void = null,
        commit_transaction: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        commit_transaction_with_id: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        batch: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void,
        begin_bulk_ingest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?void = null,
        finish_bulk_ingest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            options: backend_types.BulkIngestFinishOptions,
        ) anyerror!?void = null,
        abort_bulk_ingest: ?*const fn (
            ptr: *anyopaque,
            table_name: []const u8,
        ) void = null,
        batch_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void = null,
        txn_begin_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            topology_epoch: u64,
            participants: []const []const u8,
        ) anyerror!?void = null,
        txn_prepare_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            topology_epoch: u64,
            req: db_mod.types.TransactionIntentRequest,
        ) anyerror!?void = null,
        txn_resolve_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            status: db_mod.types.TxnStatus,
            commit_version: u64,
        ) anyerror!?void = null,
        txn_status_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
        ) anyerror!?db_mod.types.TxnStatus = null,
        corrupt_embedding_artifact: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            index_name: []const u8,
        ) anyerror!?void = null,
        local_runtime_statuses: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
    };

    pub fn batch(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try self.vtable.batch(self.ptr, alloc, table_name, req);
    }

    pub fn beginBulkIngest(self: TableWriteSource, alloc: std.mem.Allocator, table_name: []const u8) !?void {
        const fn_ptr = self.vtable.begin_bulk_ingest orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn finishBulkIngest(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const fn_ptr = self.vtable.finish_bulk_ingest orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, options);
    }

    pub fn abortBulkIngest(self: TableWriteSource, table_name: []const u8) void {
        const fn_ptr = self.vtable.abort_bulk_ingest orelse return;
        fn_ptr(self.ptr, table_name);
    }

    pub fn createTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const fn_ptr = self.vtable.create_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn updateSchema(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.update_schema orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, schema_json);
    }

    pub fn createIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.create_index orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, index_name, index_json);
    }

    pub fn dropIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.drop_index orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, index_name);
    }

    pub fn dropTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_ids: []const u64,
    ) !?void {
        const fn_ptr = self.vtable.drop_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, group_ids);
    }

    pub fn backupTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, plan);
    }

    pub fn restoreTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const fn_ptr = self.vtable.restore_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, plan);
    }

    pub fn commitTransaction(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_transaction orelse return null;
        return try fn_ptr(self.ptr, alloc, tables, sync_level);
    }

    pub fn commitTransactionWithId(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_transaction_with_id orelse return null;
        return try fn_ptr(self.ptr, alloc, txn_id, begin_timestamp, tables, sync_level);
    }

    pub fn batchGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const fn_ptr = self.vtable.batch_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn txnBeginGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        participants: []const []const u8,
    ) !?void {
        const fn_ptr = self.vtable.txn_begin_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, participants);
    }

    pub fn txnPrepareGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const fn_ptr = self.vtable.txn_prepare_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, topology_epoch, req);
    }

    pub fn txnResolveGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const fn_ptr = self.vtable.txn_resolve_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, status, commit_version);
    }

    pub fn txnStatusGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const fn_ptr = self.vtable.txn_status_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id);
    }

    pub fn corruptEmbeddingArtifact(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.corrupt_embedding_artifact orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, index_name);
    }

    pub fn localRuntimeStatuses(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const fn_ptr = self.vtable.local_runtime_statuses orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }
};

pub const RaftBatcher = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        batch_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!void,
        batch_group_local: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!void,
    };

    pub fn batchGroup(
        self: RaftBatcher,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !void {
        return try self.vtable.batch_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn batchGroupLocal(
        self: RaftBatcher,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !void {
        return try self.vtable.batch_group_local(self.ptr, alloc, group_id, table_name, req);
    }
};

pub const BoundTableWriteSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,

    pub fn init(table_name: []const u8, db: *db_mod.DB) BoundTableWriteSource {
        return .{
            .table_name = table_name,
            .db = db,
        };
    }

    pub fn source(self: *BoundTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_table = createTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .drop_index = dropIndex,
                .backup_table = backupTable,
                .restore_table = restoreTable,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_id = commitTransactionWithId,
                .batch = batch,
                .begin_bulk_ingest = beginBulkIngest,
                .finish_bulk_ingest = finishBulkIngest,
                .abort_bulk_ingest = abortBulkIngest,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .local_runtime_statuses = localRuntimeStatuses,
            },
        };
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
        items[0] = .{
            .group_id = 0,
            .stats = try self.db.runtimeStatusStatsConsistent(alloc),
        };
        return .{ .items = items };
    }

    fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!try corruptEmbeddingArtifactInDb(alloc, self.db, doc_key, index_name)) return error.NotFound;
    }

    fn createTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const raw_indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
        const schema_json = tables_api.effectiveSchemaJson(req.schema_json);
        const expanded_indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(alloc, table_name, raw_indexes_json, schema_json);
        defer alloc.free(expanded_indexes_json);
        const indexes_json = expanded_indexes_json;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        const object = switch (parsed.value) {
            .object => |object| object,
            else => return error.InvalidCreateTableRequest,
        };

        var it = object.iterator();
        while (it.next()) |entry| {
            const kind = try parseIndexKind(entry.value_ptr.*);
            const config_json = try extractIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*);
            defer alloc.free(config_json);
            try self.db.addIndex(.{
                .name = entry.key_ptr.*,
                .kind = kind,
                .config_json = config_json,
            });
        }

        try applyLocalTableSchemaJson(alloc, self.db, schema_json);
    }

    fn updateSchema(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try applyLocalTableSchemaJson(alloc, self.db, schema_json);
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try validateTableBatchAgainstLocalSchema(alloc, self.db, req.writes, req.transforms);
        try self.db.batch(req);
    }

    fn beginBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.db.beginBulkIngestSession();
    }

    fn finishBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.db.finishBulkIngestSessionWithOptions(options);
    }

    fn abortBulkIngest(ptr: *anyopaque, table_name: []const u8) void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return;
        self.db.abortBulkIngestSession();
    }

    fn backupTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const snapshot_token = try std.fmt.allocPrint(alloc, "{s}-local", .{plan.backup_id});
        defer alloc.free(snapshot_token);
        _ = try self.db.snapshot(snapshot_token);

        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ self.db.core.path, snapshot_token });
        defer alloc.free(snapshot_root);
        const dest_root = try backups_api.shardSnapshotPath(alloc, plan.backup_root, plan.backup_id, 0);
        defer alloc.free(dest_root);
        try backups_api.copyDirectoryRecursive(alloc, snapshot_root, dest_root);

        const rel_path = try backups_api.shardSnapshotRelPath(alloc, plan.backup_id, 0);
        errdefer alloc.free(rel_path);
        const byte_range = self.db.getRange();
        const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
        shards[0] = .{
            .group_id = 0,
            .start_key = try alloc.dupe(u8, byte_range.start),
            .end_key = if (byte_range.end.len > 0) try alloc.dupe(u8, byte_range.end) else null,
            .snapshot_path = rel_path,
        };
        return shards;
    }

    fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (plan.manifest.shards.len != 1) return error.UnsupportedBackupFormat;

        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ plan.backup_root, plan.manifest.shards[0].snapshot_path });
        defer alloc.free(snapshot_root);

        const db_path = try alloc.dupe(u8, self.db.core.path);
        defer alloc.free(db_path);
        const primary_backend = self.db.primary_backend;
        var owned_backend_runtime = self.db.owned_backend_runtime;
        self.db.owned_backend_runtime = null;
        errdefer if (owned_backend_runtime) |*runtime| runtime.deinit();
        const backend_runtime = if (owned_backend_runtime) |*runtime|
            runtime.runtime
        else
            self.db.backend_runtime;
        const identity_namespace = self.db.core.identity_namespace;

        self.db.close();
        try db_mod.DB.restoreSnapshotTo(alloc, snapshot_root, db_path, .{
            .primary_backend = primary_backend,
            .identity_namespace = identity_namespace,
        });
        self.db.* = try db_mod.DB.open(alloc, db_path, .{
            .primary_backend = primary_backend,
            .backend_runtime = backend_runtime,
        });
        self.db.owned_backend_runtime = owned_backend_runtime;
        owned_backend_runtime = null;
    }

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const txn_id = nextTxnId();
        return try commitTransactionWithId(ptr, alloc, txn_id, nextTxnTimestamp(), tables, sync_level);
    }

    fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        _: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (tables.len != 1) return error.UnsupportedOperation;
        const table = tables[0];
        if (!std.mem.eql(u8, self.table_name, table.table_name)) return null;
        try validateTransactionAgainstLocalSchema(alloc, self.db, table.writes, table.transforms);

        const commit_version = begin_timestamp + 1;

        _ = try self.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, &.{});
        self.db.writeTransaction(txn_id, .{
            .writes = table.writes,
            .deletes = table.deletes,
            .transforms = table.transforms,
            .predicates = table.predicates,
        }) catch |err| switch (err) {
            error.VersionConflict, error.IntentConflict => {
                self.db.resolveTransactionIntents(txn_id, .aborted, commit_version) catch {};
                return .{ .conflict = boundConflict(table, err) };
            },
            else => return err,
        };
        try self.db.resolveTransactionIntents(txn_id, .committed, commit_version);
        return .{ .committed = .{ .participant_count = 1 } };
    }

    fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        const schema_json = try loadLocalTableSchemaJson(alloc, self.db);
        defer if (schema_json) |value| alloc.free(value);
        const expanded_index_json = try tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table_name, index_json, tables_api.effectiveSchemaJson(schema_json));
        defer alloc.free(expanded_index_json);
        const cfg = try parseIndexConfig(alloc, index_name, expanded_index_json);
        defer {
            alloc.free(cfg.name);
            alloc.free(cfg.config_json);
        }
        try self.db.addIndex(cfg);
    }

    fn dropIndex(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try self.db.deleteIndex(index_name);
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try batch(ptr, alloc, table_name, req);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        _: u64,
        participants: []const []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try self.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        _: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try validateTransactionAgainstLocalSchema(alloc, self.db, req.writes, req.transforms);
        try self.db.writeTransaction(txn_id, req);
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.db.resolveTransactionIntents(txn_id, status, commit_version);
        const participant = try std.fmt.allocPrint(self.db.alloc, "group:{d}", .{group_id});
        defer self.db.alloc.free(participant);
        try self.db.markTransactionParticipantResolved(txn_id, participant);
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.getTransactionStatus(txn_id);
    }
};

pub const ProvisionedTableWriteSource = struct {
    pub const StartupCatchUpResult = struct {
        had_debt: bool = false,
        cleared_debt: bool = false,
        busy: bool = false,
    };

    pub const LocalChangeKind = enum {
        data,
        structural,
    };

    pub const LocalChangeHook = struct {
        ptr: *anyopaque,
        on_change: *const fn (ptr: *anyopaque, table_name: []const u8, kind: LocalChangeKind) void,
    };

    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    local_db_mutex: std.atomic.Mutex = .unlocked,
    table_activity_threaded: Io.Threaded,
    table_activity_mutex: Io.Mutex = .init,
    table_activity_ready: Io.Condition = .init,
    read_cache: ?*table_reads.ProvisionedTableReadCache = null,
    write_cache: ?*ProvisionedTableWriteCache = null,
    startup_write_cache: ?*ProvisionedTableWriteCache = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    dropped_table_delete_owner_id: u64 = 0,
    restore_repair_work_group: Io.Group = .init,
    restore_repair_completion_mutex: Io.Mutex = .init,
    restore_repair_completion_group: Io.Group = .init,
    restore_repair_completion_scheduled: std.atomic.Value(bool) = .init(false),
    restore_repair_completions: std.ArrayListUnmanaged([]u8) = .empty,
    runtime_status_cache: ?*runtime_status.TableRuntimeSnapshotCache = null,
    local_change_hook: ?LocalChangeHook = null,
    raft_batcher: ?RaftBatcher = null,
    group_lsm_generation: ?table_reads.GroupLsmGenerationSource = null,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    dirty_write_tables_mutex: std.atomic.Mutex = .unlocked,
    dirty_write_table_count: std.atomic.Value(u32) = .init(0),
    startup_catch_up_active: std.atomic.Value(bool) = .init(false),
    dirty_write_table_hashes: [max_cached_write_tables]u64 = [_]u64{0} ** max_cached_write_tables,
    dirty_write_table_hashes_len: usize = 0,
    active_table_activities: std.ArrayListUnmanaged(TableActivity) = .empty,

    const TableActivity = struct {
        // Owned by active_table_activities; entries must be created via activityEntryLocked.
        table_name: []const u8,
        group_id: ?u64 = null,
        table_request_active: usize = 0,
        operation_active: bool = false,
        structural_active: bool = false,
    };

    pub fn init(replica_root_dir: []const u8, catalog: table_catalog.CatalogSource) ProvisionedTableWriteSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .table_activity_threaded = Io.Threaded.init(std.heap.page_allocator, .{}),
        };
    }

    pub fn withLocalTermiteProvider(
        self: *ProvisionedTableWriteSource,
        provider: ?managed_embedder.LocalTermiteProvider,
    ) *ProvisionedTableWriteSource {
        self.local_termite_provider = provider;
        if (self.write_cache) |cache| cache.local_termite_provider = provider;
        if (self.startup_write_cache) |cache| cache.local_termite_provider = provider;
        return self;
    }

    pub fn withSecretStore(
        self: *ProvisionedTableWriteSource,
        secret_store: ?*common_secrets.FileStore,
    ) *ProvisionedTableWriteSource {
        self.secret_store = secret_store;
        if (self.write_cache) |cache| cache.secret_store = secret_store;
        if (self.startup_write_cache) |cache| cache.secret_store = secret_store;
        return self;
    }

    pub fn withRemoteContent(
        self: *ProvisionedTableWriteSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *ProvisionedTableWriteSource {
        self.remote_content = remote_content;
        if (self.write_cache) |cache| cache.remote_content = remote_content;
        if (self.startup_write_cache) |cache| cache.remote_content = remote_content;
        return self;
    }

    pub fn withRaftBatcher(self: *ProvisionedTableWriteSource, batcher: ?RaftBatcher) *ProvisionedTableWriteSource {
        self.raft_batcher = batcher;
        return self;
    }

    pub fn deinit(self: *ProvisionedTableWriteSource) void {
        self.drainDroppedTableDeletes();
        const io = self.table_activity_threaded.io();
        self.restore_repair_work_group.await(io) catch {};
        self.restore_repair_completion_group.await(io) catch {};
        self.freeRestoreRepairCompletions();
        self.table_activity_mutex.lockUncancelable(io);
        for (self.active_table_activities.items) |entry| {
            std.heap.page_allocator.free(entry.table_name);
        }
        self.active_table_activities.deinit(std.heap.page_allocator);
        self.active_table_activities = .empty;
        self.table_activity_mutex.unlock(io);
        self.table_activity_threaded.deinit();
        self.* = undefined;
    }

    pub fn localDbMutex(self: *ProvisionedTableWriteSource) *std.atomic.Mutex {
        return &self.local_db_mutex;
    }

    fn lsmRootGeneration(self: *const ProvisionedTableWriteSource, group_id: u64) u64 {
        return if (self.group_lsm_generation) |generation_source| generation_source.generationForGroup(group_id) else 0;
    }

    fn droppedTableDeleteOwnerId(self: *ProvisionedTableWriteSource, runtime: *db_mod.background_runtime.BackendRuntime) u64 {
        if (self.dropped_table_delete_owner_id == 0) {
            self.dropped_table_delete_owner_id = runtime.allocOwnerId();
        }
        return self.dropped_table_delete_owner_id;
    }

    fn drainDroppedTableDeletes(self: *ProvisionedTableWriteSource) void {
        if (self.dropped_table_delete_owner_id == 0) return;
        if (self.backend_runtime) |runtime| {
            runtime.durable_jobs.drainOwner(self.dropped_table_delete_owner_id);
        }
    }

    fn scheduleDroppedGroupDelete(self: *ProvisionedTableWriteSource, path: []const u8) !bool {
        const runtime = self.backend_runtime orelse return false;
        const work = try std.heap.page_allocator.create(DroppedTableDeleteWork);
        errdefer std.heap.page_allocator.destroy(work);
        const owned_path = try std.heap.page_allocator.dupe(u8, path);
        errdefer std.heap.page_allocator.free(owned_path);
        work.* = .{ .path = owned_path };
        try runtime.durable_jobs.submit(.{
            .owner_id = self.droppedTableDeleteOwnerId(runtime),
            .class = .cleanup,
            .ptr = work,
            .run = DroppedTableDeleteWork.run,
            .deinit = DroppedTableDeleteWork.deinit,
        });
        return true;
    }

    fn deleteDroppedGroupPath(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator, path: []u8) !void {
        defer alloc.free(path);
        if (self.scheduleDroppedGroupDelete(path) catch false) return;
        try DroppedTableDeleteWork.deletePath(path, false);
    }

    fn findTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: ?u64) ?usize {
        for (self.active_table_activities.items, 0..) |entry, i| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.group_id != group_id) continue;
            return i;
        }
        return null;
    }

    fn activityEntryLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: ?u64) *TableActivity {
        if (self.findTableActivityLocked(table_name, group_id)) |index| return &self.active_table_activities.items[index];
        const index = self.active_table_activities.items.len;
        const owned_table_name = std.heap.page_allocator.dupe(u8, table_name) catch {
            @panic("failed to allocate table activity name");
        };
        self.active_table_activities.append(std.heap.page_allocator, .{ .table_name = owned_table_name, .group_id = group_id }) catch {
            std.heap.page_allocator.free(owned_table_name);
            @panic("failed to allocate table activity entry");
        };
        return &self.active_table_activities.items[index];
    }

    fn pruneTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: ?u64) void {
        const index = self.findTableActivityLocked(table_name, group_id) orelse return;
        const entry = self.active_table_activities.items[index];
        if (entry.table_request_active > 0 or entry.operation_active or entry.structural_active) return;
        const removed = self.active_table_activities.swapRemove(index);
        std.heap.page_allocator.free(removed.table_name);
        if (self.active_table_activities.items.len == 0) {
            self.active_table_activities.deinit(std.heap.page_allocator);
            self.active_table_activities = .empty;
        }
    }

    fn hasAnyActiveGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        for (self.active_table_activities.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.group_id == null) continue;
            if (entry.operation_active) return true;
        }
        return false;
    }

    fn waitForNoStructuralActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                if (self.active_table_activities.items[index].structural_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            return;
        }
    }

    fn beginTableRequestLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                if (self.active_table_activities.items[index].structural_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            const entry = self.activityEntryLocked(table_name, null);
            entry.table_request_active += 1;
            return;
        }
    }

    fn endTableRequestLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        const index = self.findTableActivityLocked(table_name, null) orelse {
            self.table_activity_ready.broadcast(io);
            return;
        };
        if (self.active_table_activities.items[index].table_request_active == 0) {
            self.table_activity_ready.broadcast(io);
            return;
        }
        self.active_table_activities.items[index].table_request_active -= 1;
        self.pruneTableActivityLocked(table_name, null);
        self.table_activity_ready.broadcast(io);
    }

    fn beginGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.structural_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            if (self.findTableActivityLocked(table_name, group_id)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.operation_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            const entry = self.activityEntryLocked(table_name, group_id);
            entry.operation_active = true;
            return;
        }
    }

    fn tryBeginGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active) return false;
        }
        if (self.findTableActivityLocked(table_name, group_id)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.operation_active) return false;
        }
        const entry = self.activityEntryLocked(table_name, group_id);
        entry.operation_active = true;
        return true;
    }

    fn endGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        const index = self.findTableActivityLocked(table_name, group_id) orelse unreachable;
        std.debug.assert(self.active_table_activities.items[index].operation_active);
        self.active_table_activities.items[index].operation_active = false;
        self.pruneTableActivityLocked(table_name, group_id);
        self.table_activity_ready.broadcast(io);
    }

    fn tryBeginStructuralTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active or entry.table_request_active > 0) return false;
        }
        if (self.hasAnyActiveGroupOperationLocked(table_name)) return false;
        const entry = self.activityEntryLocked(table_name, null);
        entry.structural_active = true;
        return true;
    }

    fn beginStructuralTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.structural_active or entry.table_request_active > 0) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            if (self.hasAnyActiveGroupOperationLocked(table_name)) {
                self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                continue;
            }
            const entry = self.activityEntryLocked(table_name, null);
            entry.structural_active = true;
            return;
        }
    }

    fn endStructuralTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        const index = self.findTableActivityLocked(table_name, null) orelse unreachable;
        self.active_table_activities.items[index].structural_active = false;
        self.pruneTableActivityLocked(table_name, null);
        self.table_activity_ready.broadcast(io);
    }

    fn waitForNoStructuralActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.waitForNoStructuralActivityLocked(table_name);
    }

    fn waitForNoReadBlockingActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.structural_active or entry.table_request_active > 0) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            if (self.hasAnyActiveGroupOperationLocked(table_name)) {
                self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                continue;
            }
            return;
        }
    }

    fn waitForNoReadBlockingActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.waitForNoReadBlockingActivityLocked(table_name);
    }

    fn beginTableRequest(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.beginTableRequestLocked(table_name);
    }

    fn endTableRequest(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.endTableRequestLocked(table_name);
    }

    fn beginGroupOperation(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.beginGroupOperationLocked(table_name, group_id);
    }

    pub fn testingMarkTableRequestActive(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (!builtin.is_test) @compileError("testingMarkTableRequestActive is test-only");
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        const entry = self.activityEntryLocked(table_name, null);
        entry.table_request_active += 1;
    }

    pub fn testingMarkGroupOperationActive(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        if (!builtin.is_test) @compileError("testingMarkGroupOperationActive is test-only");
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        const entry = self.activityEntryLocked(table_name, group_id);
        std.debug.assert(!entry.operation_active);
        entry.operation_active = true;
    }

    fn tryBeginGroupOperation(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        return self.tryBeginGroupOperationLocked(table_name, group_id);
    }

    fn endGroupOperation(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.endGroupOperationLocked(table_name, group_id);
    }

    fn tryBeginStructuralTableActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        return self.tryBeginStructuralTableActivityLocked(table_name);
    }

    fn beginStructuralTableActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.beginStructuralTableActivityLocked(table_name);
    }

    fn endStructuralTableActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.endStructuralTableActivityLocked(table_name);
    }

    fn beginLocalStructuralMutation(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.beginStructuralTableActivity(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateWriteCache(table_name);
        self.invalidateReadCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
    }

    fn finishLocalStructuralMutation(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.invalidateReadCache(table_name);
        self.local_db_mutex.unlock();
        self.endStructuralTableActivity(table_name);
    }

    fn abortLocalStructuralMutation(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.invalidateReadCache(table_name);
        self.invalidateWriteCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
        self.local_db_mutex.unlock();
        self.endStructuralTableActivity(table_name);
    }

    fn getOrOpenCachedDbMode(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        cache: *ProvisionedTableWriteCache,
        path: []const u8,
        group_id: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
        finish_expired_auto_bulk_now_ns: ?u64,
        ensure_auto_bulk_now_ns: ?u64,
    ) !ProvisionedTableWriteCache.CachedDb {
        _ = alloc;
        if (cache.backend_runtime == null) cache.backend_runtime = self.backend_runtime;
        cache.local_termite_provider = self.local_termite_provider;
        cache.remote_content = self.remote_content;
        const lsm_root_generation = self.lsmRootGeneration(group_id);
        const identity_namespace = try loadTableIdentityNamespaceForGroup(cache.alloc, self.catalog, table_name, group_id);
        const expected_identity_namespace = if (mode == .startup_catch_up or mode == .restore_repair)
            null
        else
            identity_namespace;
        if (mode == .status_only) {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            if (finish_expired_auto_bulk_now_ns) |now_ns| {
                _ = try cache.finishExpiredAutoBulkIngestLocked(now_ns);
            }
            const cached = try cache.getOrOpenLockedMode(path, self.catalog, group_id, lsm_root_generation, table_name, .status_only);
            try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
            return cached;
        }

        var prepared_open: ?ProvisionedTableWriteCache.PreparedOpen = null;
        defer if (prepared_open) |*prepared| prepared.deinit(cache.alloc);

        {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            if (finish_expired_auto_bulk_now_ns) |now_ns| {
                _ = cache.finishExpiredAutoBulkIngestLocked(now_ns) catch |err| {
                    if (!isTransientReplayVisibilityError(err)) return err;
                    std.log.warn("auto bulk ingest expired finish deferred table={s} group_id={} err={s}", .{
                        table_name,
                        group_id,
                        @errorName(err),
                    });
                };
            }
            switch (try cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    if (mode == .default or mode == .default_async) {
                        cached.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(cached.entry.?.table_name, group_id, cached.db));
                    }
                    if (ensure_auto_bulk_now_ns) |now_ns| try cache.ensureAutoBulkIngestLocked(group_id, table_name, now_ns);
                    return cached;
                },
                .prepared => |prepared| prepared_open = prepared,
            }
        }

        lockAtomic(&cache.open_mutex);
        defer cache.open_mutex.unlock();

        {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            switch (try cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    prepared_open.?.deinit(cache.alloc);
                    prepared_open = null;
                    if (mode == .default or mode == .default_async) {
                        cached.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(cached.entry.?.table_name, group_id, cached.db));
                    }
                    if (ensure_auto_bulk_now_ns) |now_ns| try cache.ensureAutoBulkIngestLocked(group_id, table_name, now_ns);
                    return cached;
                },
                .prepared => |prepared| {
                    prepared_open.?.deinit(cache.alloc);
                    prepared_open = prepared;
                },
            }
        }

        if (try loadTableManagedMetadata(cache.alloc, self.catalog, table_name)) |metadata| {
            prepared_open.?.indexes_json = metadata.indexes_json;
            prepared_open.?.schema_json = metadata.schema_json;
        }

        var opened: ?db_mod.DB = if (prepared_open.?.indexes_json) |value|
            try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(cache.alloc, path, value, cache.lsm_cache, cache.hbc_cache, lsm_root_generation, cache.resource_manager, mode, cache.backend_runtime, self.local_termite_provider, self.secret_store, cache.remote_content, identity_namespace)
        else
            try db_mod.DB.open(cache.alloc, path, .{
                .lsm_cache = cache.lsm_cache,
                .hbc_cache = cache.hbc_cache,
                .lsm_root_generation = lsm_root_generation,
                .resource_manager = cache.resource_manager,
                .backend_runtime = cache.backend_runtime,
                .identity_namespace = identity_namespace,
                .prefer_existing_identity_namespace = identity_namespace != null,
                .open_mode = switch (mode) {
                    .default => .writer,
                    .default_async, .writer_no_replay => .writer_no_replay,
                    .startup_catch_up, .restore_repair => .writer_no_replay,
                    .status_only => .status_only,
                },
                .index_open_parallelism = if (mode == .default_async or mode == .writer_no_replay) 1 else null,
                .start_index_workers = if (mode == .startup_catch_up) false else true,
                .ttl_cleanup = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .transaction_recovery = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .text_merge = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
            });
        defer if (opened) |*db| db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &opened.?);

        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cached = try cache.adoptPreparedOpenLocked(&opened, group_id, lsm_root_generation, table_name, mode, &prepared_open.?);
        if (mode == .default or mode == .default_async) {
            cached.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(cached.entry.?.table_name, group_id, cached.db));
        }
        if (ensure_auto_bulk_now_ns) |now_ns| try cache.ensureAutoBulkIngestLocked(group_id, table_name, now_ns);
        return cached;
    }

    pub fn setLocalChangeHook(self: *ProvisionedTableWriteSource, hook: ?LocalChangeHook) void {
        self.local_change_hook = hook;
    }

    fn invalidateReadCache(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (self.read_cache) |cache| cache.invalidateTable(table_name);
    }

    fn invalidateWriteCacheForTable(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        if (self.write_cache) |cache| cache.invalidateTable(table_name);
    }

    fn invalidateRuntimeStatusCache(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (self.runtime_status_cache) |snapshot_cache| snapshot_cache.invalidateTable(table_name);
    }

    fn managedDerivedVisibilityHook(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) db_mod.QueryVisibilityHook {
        return .{
            .ptr = self,
            .table_name = table_name,
            .group_id = group_id,
            .db = db,
            .on_change = onManagedDerivedVisibilityChanged,
        };
    }

    fn publishManagedRuntimeStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) bool {
        const snapshot_cache = self.runtime_status_cache orelse return false;
        publishRuntimeStatusSnapshot(self, snapshot_cache.alloc, table_name, group_id, db) catch |err| {
            std.log.warn("managed runtime status publish failed table={s} group_id={} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return false;
        };
        return true;
    }

    fn overlayCachedManagedRuntimeStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) bool {
        const snapshot_cache = self.runtime_status_cache orelse return false;
        var status = (snapshot_cache.snapshotGroupStatus(snapshot_cache.alloc, table_name, group_id) catch |err| {
            std.log.warn("managed runtime status cached snapshot lookup failed table={s} group_id={} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return false;
        }) orelse return false;
        defer status.deinit(snapshot_cache.alloc);

        db.overlayRuntimeStatusBestEffort(snapshot_cache.alloc, &status.stats);
        snapshot_cache.upsertGroupStatus(table_name, status) catch |err| {
            std.log.warn("managed runtime status overlay publish failed table={s} group_id={} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return false;
        };
        return true;
    }

    fn onManagedDerivedVisibilityChanged(
        ptr: *anyopaque,
        table_name: []const u8,
        group_id: u64,
        db: ?*db_mod.DB,
        change: db_mod.QueryVisibilityChange,
    ) void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        switch (change) {
            .publish => {
                if (db) |managed_db| {
                    if (self.publishManagedRuntimeStatusBestEffort(table_name, group_id, managed_db)) {
                        self.invalidateReadCache(table_name);
                        self.clearDirtyWriteTable(table_name);
                        self.notifyLocalChange(table_name, .data);
                        return;
                    }
                }
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.local_db_mutex.unlock();
            },
            .publish_consistent => {
                if (db) |managed_db| {
                    if (self.runtime_status_cache) |snapshot_cache| {
                        publishRuntimeStatusSnapshotConsistent(self, snapshot_cache.alloc, table_name, group_id, managed_db) catch |err| {
                            std.log.warn("managed runtime status consistent publish failed table={s} group_id={} err={s}", .{
                                table_name,
                                group_id,
                                @errorName(err),
                            });
                            lockAtomic(&self.local_db_mutex);
                            self.markWriteCacheDirty(table_name);
                            self.local_db_mutex.unlock();
                            return;
                        };
                        self.invalidateReadCache(table_name);
                        self.clearDirtyWriteTable(table_name);
                        self.notifyLocalChange(table_name, .data);
                        return;
                    }
                }
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.local_db_mutex.unlock();
            },
            .invalidate => {},
        }
        self.invalidateReadCache(table_name);
        self.notifyLocalChange(table_name, .data);
    }

    fn invalidateWriteCache(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        _ = self.publishWriteCacheStatusBeforeInvalidate(table_name);
        self.detachWriteCacheVisibilityHooksBeforeInvalidate(table_name);
        if (self.write_cache) |cache| cache.invalidateTable(table_name);
        if (self.startup_write_cache) |cache| cache.invalidateTable(table_name);
        self.clearDirtyWriteTable(table_name);
    }

    fn publishWriteCacheStatusBeforeInvalidate(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.runtime_status_cache == null) return true;
        var published = true;
        published = self.publishCacheStatusBeforeInvalidate(table_name, self.write_cache) and published;
        published = self.publishCacheStatusBeforeInvalidate(table_name, self.startup_write_cache) and published;
        return published;
    }

    fn detachWriteCacheVisibilityHooksBeforeInvalidate(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.detachCacheVisibilityHooksBeforeInvalidate(table_name, self.write_cache);
        self.detachCacheVisibilityHooksBeforeInvalidate(table_name, self.startup_write_cache);
    }

    fn detachCacheVisibilityHooksBeforeInvalidate(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        maybe_cache: ?*ProvisionedTableWriteCache,
    ) void {
        const cache = maybe_cache orelse return;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.lsmRootGeneration(entry.group_id)) continue;
            entry.db.setQueryVisibilityHook(null);
        }
    }

    fn publishCacheStatusBeforeInvalidate(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        maybe_cache: ?*ProvisionedTableWriteCache,
    ) bool {
        const cache = maybe_cache orelse return true;
        var published = true;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.lsmRootGeneration(entry.group_id)) continue;
            self.finishEntryAutoBulkIngestBeforeStatusPublish(cache, entry) catch |err| {
                if (!isTransientReplayVisibilityError(err)) {
                    std.log.warn("managed writer auto bulk finish before status publish failed table={s} group_id={} err={s}", .{
                        entry.table_name,
                        entry.group_id,
                        @errorName(err),
                    });
                }
                published = false;
                continue;
            };
            published = self.publishManagedRuntimeStatusBestEffort(entry.table_name, entry.group_id, &entry.db) and published;
        }
        return published;
    }

    fn finishEntryAutoBulkIngestBeforeStatusPublish(
        self: *ProvisionedTableWriteSource,
        cache: *ProvisionedTableWriteCache,
        entry: *ProvisionedTableWriteCache.Entry,
    ) !void {
        if (!entry.auto_bulk_ingest_session_open) return;
        try entry.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options);
        entry.bulk_ingest_session_open = false;
        entry.auto_bulk_ingest_session_open = false;
        entry.auto_bulk_ingest_ops = 0;
        entry.auto_bulk_ingest_started_ns = 0;
        entry.auto_bulk_ingest_last_ns = 0;
        entry.auto_bulk_ingest_finish_requested = false;
        cache.removeInactiveBulkIngestSessionLocked(entry.table_name);
        _ = self;
    }

    pub fn pruneStaleWriteCacheLocked(self: *ProvisionedTableWriteSource) void {
        const pruneCache = struct {
            fn run(write_source: *ProvisionedTableWriteSource, cache: *ProvisionedTableWriteCache) void {
                var i: usize = 0;
                while (i < cache.entries.items.len) {
                    const entry = cache.entries.items[i];
                    if (entry.lsm_root_generation == write_source.lsmRootGeneration(entry.group_id)) {
                        i += 1;
                        continue;
                    }
                    _ = cache.entries.orderedRemove(i);
                    cache.retireEntryLocked(entry);
                }

                var session_index: usize = 0;
                while (session_index < cache.active_bulk_ingest_sessions.items.len) {
                    const table_name = cache.active_bulk_ingest_sessions.items[session_index].table_name;
                    for (cache.entries.items) |entry| {
                        if (std.mem.eql(u8, entry.table_name, table_name)) break;
                    } else {
                        var removed = cache.active_bulk_ingest_sessions.orderedRemove(session_index);
                        removed.deinit(cache.alloc);
                        continue;
                    }
                    session_index += 1;
                }
            }
        }.run;

        if (self.write_cache) |cache| pruneCache(self, cache);
        if (self.startup_write_cache) |cache| pruneCache(self, cache);
    }

    pub fn clearStartupWriteCacheLocked(self: *ProvisionedTableWriteSource) void {
        if (self.startup_write_cache) |cache| cache.clear();
    }

    pub fn clearStartupWriteCache(self: *ProvisionedTableWriteSource) void {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        self.clearStartupWriteCacheLocked();
    }

    pub fn clearWriteCacheLocked(self: *ProvisionedTableWriteSource) void {
        if (self.write_cache) |cache| cache.clear();
        self.clearStartupWriteCacheLocked();
        self.clearAllDirtyWriteTables();
    }

    pub fn clearWriteCache(self: *ProvisionedTableWriteSource) void {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        self.clearWriteCacheLocked();
    }

    pub fn warmTableGroup(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8) !void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            return;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        db.close();
    }

    pub fn catchUpTableGroupBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !StartupCatchUpResult {
        return try self.catchUpTableGroupBestEffortWithIndexesJson(alloc, group_id, table_name, null);
    }

    pub fn catchUpTableGroupBestEffortWithIndexesJson(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        cached_indexes_json: ?[]const u8,
    ) !StartupCatchUpResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const lsm_root_generation = self.lsmRootGeneration(group_id);

        if (!self.tryBeginGroupOperation(table_name, group_id)) return .{ .busy = true };
        if (!self.local_db_mutex.tryLock()) {
            self.endGroupOperation(table_name, group_id);
            return .{ .busy = true };
        }
        if (self.hasDirtyWriteTableWithLocalDbLocked(table_name)) {
            self.local_db_mutex.unlock();
            self.endGroupOperation(table_name, group_id);
            return .{ .busy = true };
        }
        if (self.write_cache) |cache| {
            if (cache.hasForegroundStateForGroupTableLocked(group_id, table_name)) {
                self.local_db_mutex.unlock();
                self.endGroupOperation(table_name, group_id);
                return .{ .busy = true };
            }
        }
        self.local_db_mutex.unlock();
        defer {
            lockAtomic(&self.local_db_mutex);
            self.clearStartupWriteCacheLocked();
            self.local_db_mutex.unlock();
            self.endGroupOperation(table_name, group_id);
        }
        self.startup_catch_up_active.store(true, .monotonic);
        defer self.startup_catch_up_active.store(false, .monotonic);

        const owned_indexes_json = if (cached_indexes_json == null) try loadTableIndexesJson(alloc, self.catalog, table_name) else null;
        defer if (owned_indexes_json) |value| alloc.free(value);
        const indexes_json = cached_indexes_json orelse owned_indexes_json;
        var configured_indexes_storage: ?StartupConfiguredIndexes = null;
        if (indexes_json) |value| configured_indexes_storage = try parseStartupConfiguredIndexes(alloc, value);
        defer if (configured_indexes_storage) |*summary| summary.deinit(alloc);
        const configured_indexes = if (configured_indexes_storage) |*summary| summary else null;
        const opening_db_startup = startupCatchUpStatsForPath(path, .opening_db, configured_indexes) catch db_mod.types.StartupCatchUpStats{
            .active = true,
            .phase = .opening_db,
        };
        try publishStartupCatchUpRuntimeStatusSnapshot(self, alloc, table_name, group_id, opening_db_startup, null, configured_indexes);
        errdefer publishStartupCatchUpRuntimeStatusSnapshot(self, alloc, table_name, group_id, .{}, null, null) catch {};
        _ = db_mod.DB.recoverIncompleteRestoreImportIfNeeded(alloc, path, .{}) catch |err| {
            std.log.warn("managed startup catch-up restore import recovery failed table={s} err={}", .{ table_name, err });
            return err;
        };
        const restore_repair_needed = db_mod.DB.restoreRuntimeRepairNeededForPath(alloc, path) catch |err| {
            std.log.warn("managed startup catch-up restore repair probe failed table={s} err={}", .{ table_name, err });
            return err;
        };
        const startup_open_mode: ManagedDbOpenMode = if (restore_repair_needed) .restore_repair else .startup_catch_up;
        const startup_cache = self.startup_write_cache;
        var cached_db: ?ProvisionedTableWriteCache.CachedDb = null;
        defer if (cached_db) |*cached| cached.deinit(alloc);
        var uncached_db: ?db_mod.DB = null;
        const db = db_blk: {
            if (startup_cache) |cache| {
                cached_db = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, startup_open_mode, null, null);
                break :db_blk cached_db.?.db;
            }

            const identity_namespace = if (cached_indexes_json == null)
                try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id)
            else
                null;
            uncached_db = if (indexes_json) |value|
                try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(alloc, path, value, null, null, lsm_root_generation, null, startup_open_mode, self.backend_runtime, self.local_termite_provider, self.secret_store, self.remote_content, identity_namespace)
            else
                try db_mod.DB.open(alloc, path, .{
                    .open_mode = .writer_no_replay,
                    .lsm_root_generation = lsm_root_generation,
                    .backend_runtime = self.backend_runtime,
                    .start_index_workers = false,
                    .ttl_cleanup = .{ .enabled = false },
                    .transaction_recovery = .{ .enabled = false },
                    .text_merge = .{ .enabled = false },
                    .identity_namespace = identity_namespace,
                    .prefer_existing_identity_namespace = identity_namespace != null,
                });
            errdefer if (uncached_db) |*owned| owned.close();
            try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &uncached_db.?);
            break :db_blk &uncached_db.?;
        };
        defer if (uncached_db) |*owned| owned.close();
        try publishStartupCatchUpRuntimeStatusSnapshot(
            self,
            alloc,
            table_name,
            group_id,
            startupCatchUpStatsForPhase(.opening_db, db),
            db,
            configured_indexes,
        );
        const result = try catchUpManagedDb(self, alloc, group_id, table_name, db);
        try publishRuntimeStatusSnapshotWithStartupPhase(self, alloc, table_name, group_id, .idle, db);
        return result;
    }

    pub fn runLsmMaintenanceRound(self: *ProvisionedTableWriteSource) !bool {
        var leased = blk: {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            const cache = self.write_cache orelse return false;
            if (cache.maxLsmMaintenanceScoreLocked() == 0) return false;
            break :blk cache.leaseLsmMaintenanceRoundLocked() orelse return false;
        };
        defer {
            const release_alloc = if (leased.cache) |cache| cache.alloc else std.heap.page_allocator;
            leased.deinit(release_alloc);
        }
        return try leased.db.runLsmMaintenanceStep();
    }

    pub fn runLsmMaintenanceRoundBestEffort(self: *ProvisionedTableWriteSource) !bool {
        if (!self.local_db_mutex.tryLock()) return false;
        var leased = blk: {
            defer self.local_db_mutex.unlock();
            const cache = self.write_cache orelse return false;
            if (cache.maxLsmMaintenanceScoreLocked() == 0) return false;
            break :blk cache.leaseLsmMaintenanceRoundBestEffortLocked() orelse return false;
        };
        defer {
            const release_alloc = if (leased.cache) |cache| cache.alloc else std.heap.page_allocator;
            leased.deinit(release_alloc);
        }
        return try leased.db.runLsmMaintenanceStepBestEffort();
    }

    pub fn finishExpiredAutoBulkIngestBestEffort(self: *ProvisionedTableWriteSource) bool {
        return self.tryFinishExpiredAutoBulkIngest() orelse false;
    }

    pub fn tryFinishExpiredAutoBulkIngestAndPublishStatus(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator) ?bool {
        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        if (!self.local_db_mutex.tryLock()) return null;
        const finished = self.finishExpiredAutoBulkIngestLockedCollectingStatusLeases(alloc, &leases);
        self.local_db_mutex.unlock();

        self.publishRuntimeStatusLeaseSnapshots(alloc, leases.items);
        return finished;
    }

    pub fn tryFinishExpiredAutoBulkIngest(self: *ProvisionedTableWriteSource) ?bool {
        if (!self.local_db_mutex.tryLock()) return null;
        defer self.local_db_mutex.unlock();

        return self.finishExpiredAutoBulkIngestLocked();
    }

    pub fn finishExpiredAutoBulkIngest(self: *ProvisionedTableWriteSource) bool {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();

        return self.finishExpiredAutoBulkIngestLocked();
    }

    fn finishExpiredAutoBulkIngestLocked(self: *ProvisionedTableWriteSource) bool {
        return self.finishExpiredAutoBulkIngestLockedCollectingStatusLeases(std.heap.page_allocator, null);
    }

    fn finishExpiredAutoBulkIngestLockedCollectingStatusLeases(
        self: *ProvisionedTableWriteSource,
        lease_alloc: std.mem.Allocator,
        finished_leases: ?*std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb),
    ) bool {
        const now_ns = platform_time.monotonicNs();
        var finished_any = false;
        if (self.write_cache) |cache| {
            finished_any = (cache.finishExpiredAutoBulkIngestLockedWithStatusLeases(now_ns, lease_alloc, finished_leases) catch |err| {
                if (!isTransientReplayVisibilityError(err)) {
                    std.log.warn("auto bulk ingest background finish failed err={s}", .{@errorName(err)});
                }
                return false;
            }) or finished_any;
        }
        if (self.startup_write_cache) |cache| {
            finished_any = (cache.finishExpiredAutoBulkIngestLockedWithStatusLeases(now_ns, lease_alloc, finished_leases) catch |err| {
                if (!isTransientReplayVisibilityError(err)) {
                    std.log.warn("startup auto bulk ingest background finish failed err={s}", .{@errorName(err)});
                }
                return finished_any;
            }) or finished_any;
        }
        return finished_any;
    }

    pub fn publishCachedWriterRuntimeStatusesBestEffort(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator) void {
        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        if (!self.local_db_mutex.tryLock()) return;
        self.collectAllRuntimeStatusLeasesFromCacheLocked(alloc, self.write_cache, &leases) catch {
            self.local_db_mutex.unlock();
            return;
        };
        self.collectAllRuntimeStatusLeasesFromCacheLocked(alloc, self.startup_write_cache, &leases) catch {
            self.local_db_mutex.unlock();
            return;
        };
        self.local_db_mutex.unlock();

        self.publishRuntimeStatusLeaseSnapshots(alloc, leases.items);
    }

    fn publishDirtyWriteCacheRuntimeStatusesBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) void {
        if (!self.isWriteCacheDirtyForTable(table_name)) return;
        self.publishWriteCacheRuntimeStatusesForTableBestEffort(alloc, table_name);
    }

    fn publishWriteCacheRuntimeStatusesForTableBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) void {
        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        lockAtomic(&self.local_db_mutex);
        const has_leases = self.collectRuntimeStatusLeasesFromWriteCacheLocked(alloc, table_name, &leases) catch {
            self.local_db_mutex.unlock();
            return;
        };
        self.local_db_mutex.unlock();
        if (!has_leases) return;

        self.publishRuntimeStatusLeaseSnapshots(alloc, leases.items);
    }

    fn publishRuntimeStatusLeaseSnapshots(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        leases: []const ProvisionedTableWriteCache.CachedDb,
    ) void {
        for (leases) |lease| {
            const entry = lease.entry orelse continue;
            publishRuntimeStatusSnapshot(self, alloc, entry.table_name, entry.group_id, lease.db) catch |err| {
                std.log.warn("cached writer runtime status publish failed table={s} group_id={} err={s}", .{
                    entry.table_name,
                    entry.group_id,
                    @errorName(err),
                });
            };
        }
    }

    fn collectAllRuntimeStatusLeasesFromCacheLocked(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        maybe_cache: ?*ProvisionedTableWriteCache,
        out: *std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb),
    ) !void {
        const cache = maybe_cache orelse return;
        for (cache.entries.items) |entry| {
            if (entry.lsm_root_generation != self.lsmRootGeneration(entry.group_id)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) continue;
            try cache.appendRuntimeStatusLeaseForEntryLocked(alloc, entry, out);
        }
    }

    pub fn lsmMaintenanceScore(self: *ProvisionedTableWriteSource) u64 {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        return if (self.write_cache) |cache| cache.maxLsmMaintenanceScoreLocked() else 0;
    }

    pub fn lsmMaintenanceScoreBestEffort(self: *ProvisionedTableWriteSource) u64 {
        if (!self.local_db_mutex.tryLock()) return 0;
        defer self.local_db_mutex.unlock();
        return if (self.write_cache) |cache| cache.maxLsmMaintenanceScoreLocked() else 0;
    }

    pub fn hasActiveBulkIngestSession(self: *ProvisionedTableWriteSource) bool {
        if (!self.local_db_mutex.tryLock()) return true;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return false;
        for (cache.entries.items) |entry| {
            if (entry.bulk_ingest_session_open) return true;
        }
        return false;
    }

    fn hasActiveBulkIngestSessionForTableBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
    ) bool {
        if (!self.local_db_mutex.tryLock()) return true;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return false;
        return cache.bulkIngestSessionOpenForTable(table_name);
    }

    pub const ManagedWriterGroupProbe = union(enum) {
        absent,
        unknown,
        leased: ProvisionedTableWriteCache.CachedDb,
    };

    pub fn probeManagedWriterGroupBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
    ) ManagedWriterGroupProbe {
        if (!self.local_db_mutex.tryLock()) return .unknown;
        const lsm_root_generation = self.lsmRootGeneration(group_id);
        var leased: ?ProvisionedTableWriteCache.CachedDb = null;
        if (self.write_cache) |cache| {
            leased = cache.snapshotLeaseLocked(group_id, lsm_root_generation, table_name);
        }
        if (leased == null) {
            if (self.startup_write_cache) |cache| {
                leased = cache.snapshotLeaseLocked(group_id, lsm_root_generation, table_name);
            }
        }
        self.local_db_mutex.unlock();
        if (leased) |cached| return .{ .leased = cached };
        return .absent;
    }

    pub fn snapshotManagedWriterGroupStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
    ) !?runtime_status.LocalTableRuntimeStatus {
        return switch (self.probeManagedWriterGroupBestEffort(table_name, group_id)) {
            .absent, .unknown => null,
            .leased => |cached| blk: {
                var owned = cached;
                const release_alloc = if (owned.cache) |cache| cache.alloc else std.heap.page_allocator;
                defer owned.deinit(release_alloc);
                if (self.runtime_status_cache) |snapshot_cache| {
                    if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |cached_status| {
                        var status = cached_status;
                        errdefer status.deinit(alloc);
                        owned.db.overlayRuntimeStatusBestEffort(alloc, &status.stats);
                        self.markManagedWriterRuntimeStatus(&status);
                        if (status.created_at_millis == 0) {
                            status.created_at_millis = (owned.db.getGroupCreatedAtMillis(alloc, group_id) catch null) orelse 0;
                        }
                        break :blk status;
                    }
                }
                var status = runtime_status.LocalTableRuntimeStatus{
                    .group_id = group_id,
                    .created_at_millis = (owned.db.getGroupCreatedAtMillis(alloc, group_id) catch null) orelse 0,
                    .stats = try owned.db.runtimeStatusStatsConsistent(alloc),
                };
                self.markManagedWriterRuntimeStatus(&status);
                break :blk status;
            },
        };
    }

    pub fn overlayManagedWriterGroupStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) void {
        switch (self.probeManagedWriterGroupBestEffort(table_name, group_id)) {
            .absent, .unknown => {},
            .leased => |cached| {
                var owned = cached;
                const release_alloc = if (owned.cache) |cache| cache.alloc else std.heap.page_allocator;
                defer owned.deinit(release_alloc);
                owned.db.overlayRuntimeStatusBestEffort(alloc, &status.stats);
                self.markManagedWriterRuntimeStatus(status);
                if (status.created_at_millis == 0) {
                    status.created_at_millis = (owned.db.getGroupCreatedAtMillis(std.heap.page_allocator, group_id) catch null) orelse 0;
                }
            },
        }
    }

    fn markManagedWriterRuntimeStatus(
        self: *ProvisionedTableWriteSource,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) void {
        status.metadata = .{
            .updated_at_ns = platform_time.monotonicNs(),
            .source = if (self.startup_catch_up_active.load(.monotonic)) .startup_catch_up else .live_writer_publish,
            .freshness = .fresh,
        };
    }

    pub fn hasGroupActivityBestEffort(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        if (self.findTableActivityLocked(table_name, null)) |index| {
            if (self.active_table_activities.items[index].structural_active) return true;
        }
        if (self.findTableActivityLocked(table_name, group_id)) |index| {
            if (self.active_table_activities.items[index].operation_active) return true;
        }
        return false;
    }

    pub fn hasReadBlockingActivityBestEffort(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active or entry.table_request_active > 0) return true;
        }
        if (self.findTableActivityLocked(table_name, group_id)) |index| {
            if (self.active_table_activities.items[index].operation_active) return true;
        }
        return false;
    }

    fn activeOperationGroupsForTable(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !struct {
        structural_busy: bool,
        request_busy: bool,
        groups: []u64,
    } {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);

        var structural_busy = false;
        var request_busy = false;
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active) structural_busy = true;
            if (entry.table_request_active > 0) request_busy = true;
        }

        var groups = std.ArrayListUnmanaged(u64).empty;
        errdefer groups.deinit(alloc);
        for (self.active_table_activities.items) |activity| {
            if (!std.mem.eql(u8, activity.table_name, table_name)) continue;
            const group_id = activity.group_id orelse continue;
            if (!activity.operation_active) continue;
            try groups.append(alloc, group_id);
        }

        return .{
            .structural_busy = structural_busy,
            .request_busy = request_busy,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    fn takeStatusesWithoutActiveGroups(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
        active_groups: []const u64,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        _ = self;
        if (active_groups.len == 0) {
            const owned = statuses.*;
            statuses.* = .{};
            return owned;
        }

        var keep_count: usize = 0;
        for (statuses.items) |item| {
            for (active_groups) |group_id| {
                if (item.group_id == group_id) break;
            } else {
                keep_count += 1;
            }
        }
        if (keep_count == statuses.items.len) {
            const owned = statuses.*;
            statuses.* = .{};
            return owned;
        }
        if (keep_count == 0) {
            statuses.deinit(alloc);
            statuses.* = .{};
            return null;
        }

        const original_items = statuses.items;
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, keep_count);
        var write_index: usize = 0;
        for (original_items) |*item| {
            for (active_groups) |group_id| {
                if (item.group_id == group_id) break;
            } else {
                items[write_index] = item.*;
                item.* = undefined;
                write_index += 1;
                continue;
            }
            item.deinit(alloc);
        }

        alloc.free(original_items);
        statuses.* = .{};
        return .{ .items = items };
    }

    fn statusesWithoutActiveGroups(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        statuses: runtime_status.LocalTableRuntimeStatuses,
        active_groups: []const u64,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        var owned = statuses;
        defer owned.deinit(alloc);
        return try self.takeStatusesWithoutActiveGroups(alloc, &owned, active_groups);
    }

    fn appendOrReplaceRuntimeStatusClone(
        alloc: std.mem.Allocator,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        item: runtime_status.LocalTableRuntimeStatus,
    ) !void {
        for (items.items) |*existing| {
            if (existing.group_id != item.group_id) continue;
            var cloned = try item.clone(alloc);
            errdefer cloned.deinit(alloc);
            existing.deinit(alloc);
            existing.* = cloned;
            return;
        }
        var cloned = try item.clone(alloc);
        errdefer cloned.deinit(alloc);
        try items.append(alloc, cloned);
    }

    fn appendOrReplaceRuntimeStatusClones(
        alloc: std.mem.Allocator,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        statuses: *const runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        for (statuses.items) |item| {
            try appendOrReplaceRuntimeStatusClone(alloc, items, item);
        }
    }

    fn mergedRuntimeStatusReplacement(
        alloc: std.mem.Allocator,
        current: *const runtime_status.LocalTableRuntimeStatuses,
        refresh: *const runtime_status.LocalTableRuntimeStatuses,
    ) !runtime_status.LocalTableRuntimeStatuses {
        var merged = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (merged.items) |*item| item.deinit(alloc);
            merged.deinit(alloc);
        }

        try merged.ensureTotalCapacity(alloc, current.items.len + refresh.items.len);
        try appendOrReplaceRuntimeStatusClones(alloc, &merged, current);
        try appendOrReplaceRuntimeStatusClones(alloc, &merged, refresh);
        return .{ .items = try merged.toOwnedSlice(alloc) };
    }

    fn replaceRuntimeStatusesWithMergedRefresh(
        alloc: std.mem.Allocator,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
        refresh: *const runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        var replacement = try mergedRuntimeStatusReplacement(alloc, statuses, refresh);
        errdefer replacement.deinit(alloc);
        statuses.deinit(alloc);
        statuses.* = replacement;
    }

    pub fn snapshotRuntimeStatusesBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        // Keep HTTP status reads on the cached status plane. See STATUS.md.
        const snapshot_cache = self.runtime_status_cache orelse return null;
        return try snapshot_cache.snapshot(alloc, table_name);
    }

    fn refreshRuntimeStatusesFromDirtyWriteCache(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        if (!self.isWriteCacheDirtyForTable(table_name)) return;

        const now_ns = platform_time.monotonicNs();
        if (!runtimeStatusesNeedWriterRefresh(statuses, now_ns)) return;

        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        lockAtomic(&self.local_db_mutex);
        const has_leases = self.collectRuntimeStatusLeasesFromWriteCacheLocked(alloc, table_name, &leases) catch |err| {
            self.local_db_mutex.unlock();
            return err;
        };
        self.local_db_mutex.unlock();
        if (!has_leases) return;

        var live_statuses = (try self.runtimeStatusesFromCachedDbLeasesBestEffort(alloc, table_name, leases.items)) orelse return;
        defer live_statuses.deinit(alloc);

        try replaceRuntimeStatusesWithMergedRefresh(alloc, statuses, &live_statuses);
    }

    fn refreshStaleRuntimeStatusesFromStorage(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        if (self.isWriteCacheDirtyForTable(table_name)) return;

        const now_ns = platform_time.monotonicNs();
        if (!runtimeStatusesNeedWriterRefresh(statuses, now_ns)) return;

        var uncached = (try self.snapshotUncachedRuntimeStatusesAndUpdateCache(alloc, table_name)) orelse return;
        defer uncached.deinit(alloc);

        try replaceRuntimeStatusesWithMergedRefresh(alloc, statuses, &uncached);
    }

    fn runtimeStatusesNeedWriterRefresh(statuses: *const runtime_status.LocalTableRuntimeStatuses, now_ns: u64) bool {
        const min_refresh_interval_ns = std.time.ns_per_s;
        if (statuses.items.len == 0) return true;
        for (statuses.items) |status| {
            if (!runtime_status.statusRuntimeFresh(status)) return true;
            if (status.metadata.updated_at_ns == 0) return true;
            if (now_ns -| status.metadata.updated_at_ns >= min_refresh_interval_ns) return true;
        }
        return false;
    }

    fn snapshotUncachedRuntimeStatusesAndUpdateCache(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const snapshot_cache = self.runtime_status_cache orelse return null;
        var uncached = (try snapshotLocalTableRuntimeStatusesUncached(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name)) orelse return null;
        errdefer uncached.deinit(alloc);
        for (uncached.items) |item| {
            try snapshot_cache.upsertGroupStatus(table_name, item);
        }
        return uncached;
    }

    fn overlayManagedWriterReplayTargetsBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) void {
        if (!self.local_db_mutex.tryLock()) return;
        defer self.local_db_mutex.unlock();
        self.overlayManagedWriterReplayTargetsFromCacheLocked(table_name, self.write_cache, statuses);
        self.overlayManagedWriterReplayTargetsFromCacheLocked(table_name, self.startup_write_cache, statuses);
    }

    fn overlayManagedWriterReplayTargetsFromCacheLocked(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        maybe_cache: ?*ProvisionedTableWriteCache,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) void {
        const cache = maybe_cache orelse return;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.lsmRootGeneration(entry.group_id)) continue;
            for (statuses.items) |*status| {
                if (status.group_id != entry.group_id) continue;
                overlayRuntimeStatusReplayTargetFromDb(status, &entry.db);
                break;
            }
        }
    }

    pub fn lsmMaintenanceStats(self: *ProvisionedTableWriteSource) lsm_backend.Backend.MaintenanceStats {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        var stats = lsm_backend.Backend.MaintenanceStats{};
        if (self.write_cache) |cache| {
            for (cache.entries.items) |entry| {
                lsm_backend.Backend.accumulateMaintenanceStats(&stats, entry.db.snapshotLsmMaintenanceStats());
            }
        }
        return stats;
    }

    pub fn lsmMaintenanceStatsBestEffort(self: *ProvisionedTableWriteSource) lsm_backend.Backend.MaintenanceStats {
        if (!self.local_db_mutex.tryLock()) return .{};
        defer self.local_db_mutex.unlock();
        var stats = lsm_backend.Backend.MaintenanceStats{};
        if (self.write_cache) |cache| {
            for (cache.entries.items) |entry| {
                if (entry.db.trySnapshotLsmMaintenanceStats()) |entry_stats| {
                    lsm_backend.Backend.accumulateMaintenanceStats(&stats, entry_stats);
                }
            }
        }
        return stats;
    }

    pub fn lsmNativeStorageStatsBestEffort(self: *ProvisionedTableWriteSource) ?lsm_backend.NativeStorageStats {
        if (!self.local_db_mutex.tryLock()) return null;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return null;
        var stats = lsm_backend.NativeStorageStats{};
        var observed = false;
        for (cache.entries.items) |entry| {
            const entry_stats = entry.db.trySnapshotLsmNativeStorageStats() orelse continue;
            observed = true;
            stats.fd_cache_entries +|= entry_stats.fd_cache_entries;
            stats.fd_cache_capacity +|= entry_stats.fd_cache_capacity;
        }
        if (!observed) return null;
        return stats;
    }

    pub fn asyncIndexingStats(self: *ProvisionedTableWriteSource) db_mod.types.AsyncIndexingStats {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return .{};
        var stats = db_mod.types.AsyncIndexingStats{};
        for (cache.entries.items) |entry| {
            db_mod.types.accumulateAsyncIndexingStats(&stats, entry.db.snapshotAsyncIndexingStats());
        }
        return stats;
    }

    pub fn asyncIndexingStatsBestEffort(self: *ProvisionedTableWriteSource) db_mod.types.AsyncIndexingStats {
        if (!self.local_db_mutex.tryLock()) {
            if (self.startup_catch_up_active.load(.monotonic)) {
                if (self.runtime_status_cache) |snapshot_cache| return snapshot_cache.summary().async_indexing;
            }
            return .{};
        }
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return .{};
        var stats = db_mod.types.AsyncIndexingStats{};
        for (cache.entries.items) |entry| {
            db_mod.types.accumulateAsyncIndexingStats(&stats, entry.db.snapshotAsyncIndexingStats());
        }
        return stats;
    }

    pub fn autoBulkIngestStatsBestEffort(self: *ProvisionedTableWriteSource) ProvisionedTableWriteCache.AutoBulkIngestStats {
        if (!self.local_db_mutex.tryLock()) return .{};
        defer self.local_db_mutex.unlock();
        const now_ns = platform_time.monotonicNs();
        var stats = ProvisionedTableWriteCache.AutoBulkIngestStats{};
        if (self.write_cache) |cache| stats.merge(cache.autoBulkIngestStatsLocked(now_ns));
        if (self.startup_write_cache) |cache| stats.merge(cache.autoBulkIngestStatsLocked(now_ns));
        return stats;
    }

    pub fn cachedWriteDbCount(self: *ProvisionedTableWriteSource) usize {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return 0;
        return cache.entries.items.len;
    }

    pub fn cachedWriteDbCountBestEffort(self: *ProvisionedTableWriteSource) usize {
        if (!self.local_db_mutex.tryLock()) return 0;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return 0;
        return cache.entries.items.len;
    }

    pub fn readPreparation(self: *ProvisionedTableWriteSource) table_reads.ReadPreparation {
        return .{
            .ptr = self,
            .vtable = &.{
                .prepare_for_read = prepareForRead,
            },
        };
    }

    pub fn primaryLookupDbSource(self: *ProvisionedTableWriteSource) table_reads.PrimaryLookupDbSource {
        return .{
            .ptr = self,
            .lease_group = leasePrimaryLookupDb,
        };
    }

    fn prepareForRead(ptr: *anyopaque, table_name: []const u8, kind: table_reads.ReadPreparation.Kind) void {
        _ = kind;
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.waitForNoStructuralActivity(table_name);
        if (!self.isWriteCacheDirtyForTable(table_name)) return;

        // Data writes and derived catch-up are intentionally eventually visible
        // to queries. A read can discard its cached reader and reopen the latest
        // published view, but it must not wait behind writer-cache maintenance
        // or close the live writer cache from the query path.
        self.invalidateReadCache(table_name);
        if (self.hasActiveBulkIngestSessionForTableBestEffort(table_name)) return;
        self.clearDirtyWriteTable(table_name);
    }

    fn leasePrimaryLookupDb(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
    ) !?table_reads.PrimaryLookupDbLease {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.local_db_mutex);
        var cached: ?ProvisionedTableWriteCache.CachedDb = null;
        if (self.write_cache) |cache| {
            cached = cache.snapshotLeaseLocked(group_id, lsm_root_generation, table_name);
        }
        if (cached == null) {
            if (self.startup_write_cache) |cache| {
                cached = cache.snapshotLeaseLocked(group_id, lsm_root_generation, table_name);
            }
        }
        self.local_db_mutex.unlock();

        var cached_value = cached orelse return null;
        const lease_ctx = alloc.create(ProvisionedTableWriteCache.CachedDb) catch |err| {
            cached_value.deinit(alloc);
            return err;
        };
        lease_ctx.* = cached_value;
        return .{
            .ptr = lease_ctx,
            .db = lease_ctx.db,
            .release_fn = releasePrimaryLookupDb,
        };
    }

    fn releasePrimaryLookupDb(ptr: *anyopaque, alloc: std.mem.Allocator) void {
        const lease_ctx: *ProvisionedTableWriteCache.CachedDb = @ptrCast(@alignCast(ptr));
        lease_ctx.deinit(alloc);
        alloc.destroy(lease_ctx);
    }

    fn writeCacheTableHash(table_name: []const u8) u64 {
        return std.hash.Wyhash.hash(0, table_name);
    }

    fn hasDirtyWriteTableLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        const table_hash = writeCacheTableHash(table_name);
        for (self.dirty_write_table_hashes[0..self.dirty_write_table_hashes_len]) |candidate| {
            if (candidate == table_hash) return true;
        }
        return false;
    }

    fn isWriteCacheDirtyForTable(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.dirty_write_table_count.load(.acquire) == 0) return false;
        lockAtomic(&self.dirty_write_tables_mutex);
        defer self.dirty_write_tables_mutex.unlock();
        return self.hasDirtyWriteTableLocked(table_name);
    }

    fn clearAllDirtyWriteTablesLocked(self: *ProvisionedTableWriteSource) void {
        self.dirty_write_table_hashes_len = 0;
        self.dirty_write_table_count.store(0, .release);
    }

    fn clearAllDirtyWriteTables(self: *ProvisionedTableWriteSource) void {
        lockAtomic(&self.dirty_write_tables_mutex);
        defer self.dirty_write_tables_mutex.unlock();
        self.clearAllDirtyWriteTablesLocked();
    }

    fn clearDirtyWriteTable(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        lockAtomic(&self.dirty_write_tables_mutex);
        defer self.dirty_write_tables_mutex.unlock();
        const table_hash = writeCacheTableHash(table_name);
        var i: usize = 0;
        while (i < self.dirty_write_table_hashes_len) {
            if (self.dirty_write_table_hashes[i] != table_hash) {
                i += 1;
                continue;
            }
            self.dirty_write_table_hashes_len -= 1;
            self.dirty_write_table_hashes[i] = self.dirty_write_table_hashes[self.dirty_write_table_hashes_len];
            break;
        }
        self.dirty_write_table_count.store(@intCast(self.dirty_write_table_hashes_len), .release);
    }

    fn hasDirtyWriteTableWithLocalDbLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.dirty_write_table_count.load(.acquire) == 0) return false;
        const dirty = dirty_blk: {
            lockAtomic(&self.dirty_write_tables_mutex);
            defer self.dirty_write_tables_mutex.unlock();
            break :dirty_blk self.hasDirtyWriteTableLocked(table_name);
        };
        if (!dirty) return false;

        const cache = self.write_cache orelse {
            self.clearDirtyWriteTable(table_name);
            return false;
        };
        if (cache.bulkIngestSessionOpenForTable(table_name)) return true;
        if (cache.hasLiveEntryForTableLocked(table_name)) return true;

        self.clearDirtyWriteTable(table_name);
        return false;
    }

    fn markWriteCacheDirty(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (self.write_cache == null) return;
        lockAtomic(&self.dirty_write_tables_mutex);
        const table_hash = writeCacheTableHash(table_name);
        for (self.dirty_write_table_hashes[0..self.dirty_write_table_hashes_len]) |candidate| {
            if (candidate == table_hash) {
                self.dirty_write_table_count.store(@intCast(self.dirty_write_table_hashes_len), .release);
                self.dirty_write_tables_mutex.unlock();
                return;
            }
        }
        if (self.dirty_write_table_hashes_len >= self.dirty_write_table_hashes.len) {
            self.dirty_write_tables_mutex.unlock();
            self.clearWriteCacheLocked();
            return;
        }
        self.dirty_write_table_hashes[self.dirty_write_table_hashes_len] = table_hash;
        self.dirty_write_table_hashes_len += 1;
        self.dirty_write_table_count.store(@intCast(self.dirty_write_table_hashes_len), .release);
        self.dirty_write_tables_mutex.unlock();
    }

    fn invalidateDirtyWriteCacheForRead(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        lockAtomic(&self.dirty_write_tables_mutex);
        const dirty = self.hasDirtyWriteTableLocked(table_name);
        self.dirty_write_tables_mutex.unlock();
        if (!dirty) return;
        self.invalidateWriteCache(table_name);
    }

    fn notifyLocalChange(self: *ProvisionedTableWriteSource, table_name: []const u8, kind: LocalChangeKind) void {
        if (self.local_change_hook) |hook| hook.on_change(hook.ptr, table_name, kind);
    }

    fn publishRestoreRepairComplete(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.invalidateReadCache(table_name);
        self.invalidateWriteCacheForTable(table_name);
        self.clearDirtyWriteTable(table_name);
        self.notifyLocalChange(table_name, .data);
    }

    fn enqueueRestoreRepairComplete(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const alloc = std.heap.page_allocator;
        const owned_table_name = alloc.dupe(u8, table_name) catch |err| {
            std.log.warn("restore repair completion allocation failed table={s} err={}", .{ table_name, err });
            return;
        };

        const io = self.table_activity_threaded.io();
        self.restore_repair_completion_mutex.lockUncancelable(io);
        self.restore_repair_completions.append(alloc, owned_table_name) catch |err| {
            self.restore_repair_completion_mutex.unlock(io);
            alloc.free(owned_table_name);
            std.log.warn("restore repair completion enqueue failed table={s} err={}", .{ table_name, err });
            return;
        };
        self.restore_repair_completion_mutex.unlock(io);
        self.scheduleRestoreRepairCompletionDrain();
    }

    fn scheduleRestoreRepairCompletionDrain(self: *ProvisionedTableWriteSource) void {
        if (self.restore_repair_completion_scheduled.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
        const io = self.table_activity_threaded.io();
        self.restore_repair_completion_group.concurrent(io, drainRestoreRepairCompletionsTask, .{self}) catch |err| {
            self.restore_repair_completion_scheduled.store(false, .release);
            std.log.warn("restore repair completion drain schedule failed err={}", .{err});
        };
    }

    fn drainRestoreRepairCompletionsTask(self: *ProvisionedTableWriteSource) !void {
        self.drainRestoreRepairCompletionsScheduled();
    }

    fn drainRestoreRepairCompletionsScheduled(self: *ProvisionedTableWriteSource) void {
        const alloc = std.heap.page_allocator;
        const io = self.table_activity_threaded.io();
        while (true) {
            self.restore_repair_completion_mutex.lockUncancelable(io);
            if (self.restore_repair_completions.items.len == 0) {
                self.restore_repair_completion_scheduled.store(false, .release);
                self.restore_repair_completion_mutex.unlock(io);
                return;
            }
            var pending = self.restore_repair_completions;
            self.restore_repair_completions = .empty;
            self.restore_repair_completion_mutex.unlock(io);

            for (pending.items) |table_name| {
                self.publishRestoreRepairComplete(table_name);
                alloc.free(table_name);
            }
            pending.deinit(alloc);
        }
    }

    fn freeRestoreRepairCompletions(self: *ProvisionedTableWriteSource) void {
        const alloc = std.heap.page_allocator;
        const io = self.table_activity_threaded.io();
        self.restore_repair_completion_mutex.lockUncancelable(io);
        defer self.restore_repair_completion_mutex.unlock(io);
        for (self.restore_repair_completions.items) |table_name| alloc.free(table_name);
        self.restore_repair_completions.deinit(alloc);
        self.restore_repair_completions = .empty;
    }

    fn openRestoreRepairDbForGroup(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        path: []const u8,
        group_id: u64,
        table_name: []const u8,
        indexes_json: []const u8,
    ) !db_mod.DB {
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
        var db = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(
            alloc,
            path,
            indexes_json,
            null,
            null,
            self.lsmRootGeneration(group_id),
            null,
            .restore_repair,
            self.backend_runtime,
            self.local_termite_provider,
            self.secret_store,
            self.remote_content,
            identity_namespace,
        );
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        return db;
    }

    const RestoreRepairCatchUpWork = struct {
        alloc: std.mem.Allocator,
        source: *ProvisionedTableWriteSource,
        group_id: u64,
        table_name: []u8,

        fn sleepRetry(self: *@This()) void {
            self.source.table_activity_threaded.io().sleep(Io.Duration.fromMilliseconds(100), .awake) catch {};
        }

        fn runAndDeinit(work: *@This()) Io.Cancelable!void {
            defer RestoreRepairCatchUpWork.deinit(work);
            work.run() catch |err| {
                std.log.warn("restore background catch-up failed table={s} group_id={d} err={s}", .{
                    work.table_name,
                    work.group_id,
                    @errorName(err),
                });
            };
        }

        fn run(work: *@This()) !void {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(work.alloc, work.source.replica_root_dir, work.group_id);
            defer work.alloc.free(path);

            var attempts: usize = 0;
            std.log.info("restore background catch-up begin table={s} group_id={d}", .{ work.table_name, work.group_id });
            while (true) {
                attempts += 1;
                const busy = try work.repairOnce(path);
                const still_needed = try db_mod.DB.restoreRuntimeRepairNeededForPath(work.alloc, path);
                if (!busy and !still_needed) {
                    work.source.enqueueRestoreRepairComplete(work.table_name);
                    std.log.info("restore background catch-up complete table={s} group_id={d} attempts={d}", .{
                        work.table_name,
                        work.group_id,
                        attempts,
                    });
                    return;
                }
                work.sleepRetry();
            }
        }

        fn repairOnce(self: *@This(), path: []const u8) !bool {
            if (!try db_mod.DB.restoreRuntimeRepairNeededForPath(self.alloc, path)) return false;

            if (!self.source.tryBeginGroupOperation(self.table_name, self.group_id)) return true;
            defer self.source.endGroupOperation(self.table_name, self.group_id);

            if (!self.source.local_db_mutex.tryLock()) return true;
            if (self.source.hasDirtyWriteTableWithLocalDbLocked(self.table_name)) {
                self.source.local_db_mutex.unlock();
                return true;
            }
            if (self.source.write_cache) |cache| {
                if (cache.hasForegroundStateForGroupTableLocked(self.group_id, self.table_name)) {
                    self.source.local_db_mutex.unlock();
                    return true;
                }
            }
            self.source.local_db_mutex.unlock();

            const indexes_json = (try loadTableIndexesJson(self.alloc, self.source.catalog, self.table_name)) orelse return true;
            defer self.alloc.free(indexes_json);

            var db = try self.source.openRestoreRepairDbForGroup(
                self.alloc,
                path,
                self.group_id,
                self.table_name,
                indexes_json,
            );
            defer db.close();

            if (try db.repairRestoreRuntimeStateStepIfNeeded(self.alloc)) {
                db.clearDenseHbcCaches();
            }
            return false;
        }

        fn deinit(ptr: *anyopaque) void {
            const work: *@This() = @ptrCast(@alignCast(ptr));
            const alloc = work.alloc;
            alloc.free(work.table_name);
            alloc.destroy(work);
        }
    };

    fn requestRestoreRepairCatchUp(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const alloc = std.heap.page_allocator;
        const work = alloc.create(RestoreRepairCatchUpWork) catch |err| {
            std.log.warn("restore background catch-up allocation failed table={s} group_id={d} err={}", .{ table_name, group_id, err });
            return;
        };
        const owned_table_name = alloc.dupe(u8, table_name) catch |err| {
            alloc.destroy(work);
            std.log.warn("restore background catch-up table name allocation failed table={s} group_id={d} err={}", .{ table_name, group_id, err });
            return;
        };
        work.* = .{
            .alloc = alloc,
            .source = self,
            .group_id = group_id,
            .table_name = owned_table_name,
        };
        self.restore_repair_work_group.concurrent(self.table_activity_threaded.io(), RestoreRepairCatchUpWork.runAndDeinit, .{work}) catch |err| {
            RestoreRepairCatchUpWork.deinit(work);
            std.log.warn("restore background catch-up submit failed table={s} group_id={d} err={}", .{ table_name, group_id, err });
            return;
        };
    }

    fn beginBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return null;
        try cache.beginBulkIngestLocked(table_name);
    }

    fn finishBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return null;
        try cache.finishBulkIngestLocked(table_name, options);
    }

    fn abortBulkIngest(ptr: *anyopaque, table_name: []const u8) void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return;
        cache.abortBulkIngestLocked(table_name);
    }

    pub fn source(self: *ProvisionedTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_table = createTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .drop_index = dropIndex,
                .drop_table = dropTable,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_id = commitTransactionWithId,
                .backup_table = backupTable,
                .restore_table = restoreTable,
                .batch = batch,
                .begin_bulk_ingest = beginBulkIngest,
                .finish_bulk_ingest = finishBulkIngest,
                .abort_bulk_ingest = abortBulkIngest,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .local_runtime_statuses = localRuntimeStatuses,
            },
        };
    }

    fn createTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        std.log.info("provisioned create table local begin table={s}", .{table_name});
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);

        const raw_indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
        const schema_json = tables_api.effectiveSchemaJson(req.schema_json);
        const indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(alloc, table_name, raw_indexes_json, schema_json);
        defer alloc.free(indexes_json);
        for (group_ids) |group_id| {
            std.log.info("provisioned create table local group begin table={s} group_id={d}", .{ table_name, group_id });
            try deleteGroupPathIfPresent(alloc, self.replica_root_dir, group_id);
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);

            const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
            var db = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(alloc, path, indexes_json, null, null, 0, null, .default, self.backend_runtime, self.local_termite_provider, self.secret_store, self.remote_content, identity_namespace);
            defer db.close();
            try applyLocalTableSchemaJson(alloc, &db, schema_json);
            std.log.info("provisioned create table local group ready table={s} group_id={d}", .{ table_name, group_id });
        }

        self.finishLocalStructuralMutation(table_name);
        std.log.info("provisioned create table local notify table={s}", .{table_name});
        self.notifyLocalChange(table_name, .structural);
        std.log.info("provisioned create table local done table={s}", .{table_name});
    }

    fn updateSchema(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        const indexes_json = try loadTableIndexesJson(alloc, self.catalog, table_name);
        defer if (indexes_json) |value| alloc.free(value);

        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);

        for (group_ids) |group_id| {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);

            var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            try applyLocalTableSchemaJson(alloc, &db, schema_json);
            if (indexes_json) |value| try rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate(alloc, &db, value);
            try drainManagedDbBeforeClose(&db);
            try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, &db);
        }

        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
    }

    fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        _: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);
        const managed_visibility_changed = try reconcileLocalTableIndexCreate(self, alloc, table_name, index_name);
        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
        if (managed_visibility_changed) {
            self.publishWriteCacheRuntimeStatusesForTableBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
        }
    }

    fn dropIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);
        runTestBeforeDropIndexWorkHook();
        try dropLocalTableIndex(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name, index_name);
        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
    }

    fn dropTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_ids: []const u64,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (group_ids.len == 0) return null;

        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);

        for (group_ids) |group_id| {
            const trash_path = try moveDroppedGroupPathToTrash(alloc, self.replica_root_dir, table_name, group_id);
            if (trash_path) |path| {
                try self.deleteDroppedGroupPath(alloc, path);
            }
        }
        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        errdefer {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            self.invalidateReadCache(table_name);
            self.invalidateWriteCache(table_name);
        }
        var grouped = std.ArrayListUnmanaged(GroupBatch).empty;
        defer {
            for (grouped.items) |*group| group.deinit(alloc);
            grouped.deinit(alloc);
        }

        for (req.writes) |write| {
            const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, write.key)) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.writes.append(alloc, write);
        }
        for (req.deletes) |key| {
            const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, key)) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.deletes.append(alloc, key);
        }
        for (req.transforms) |transform| {
            const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, transform.key)) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.transforms.append(alloc, transform);
        }

        if (self.raft_batcher) |batcher| {
            for (grouped.items) |group| {
                try batcher.batchGroup(alloc, group.group_id, table_name, .{
                    .writes = group.writes.items,
                    .deletes = group.deletes.items,
                    .transforms = group.transforms.items,
                    .sync_level = req.sync_level,
                });
            }
            return {};
        }

        for (grouped.items) |group| {
            self.beginGroupOperation(table_name, group.group_id);
            {
                defer self.endGroupOperation(table_name, group.group_id);
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group.group_id);
                defer alloc.free(path);
                const group_auto_bulk_ops = autoBulkIngestGroupBatchOps(group, req.sync_level);
                const auto_bulk_now_ns = platform_time.monotonicNs();
                if (self.write_cache) |cache| {
                    var cached = try self.getOrOpenCachedDbMode(
                        alloc,
                        cache,
                        path,
                        group.group_id,
                        table_name,
                        .default_async,
                        if (group_auto_bulk_ops > 0) auto_bulk_now_ns else null,
                        if (group_auto_bulk_ops > 0) auto_bulk_now_ns else null,
                    );
                    defer cached.deinit(alloc);
                    try applyGroupBatchWithSchemaJson(alloc, cached.db, cached.schema_json, group, req);
                    lockAtomic(&self.local_db_mutex);
                    defer self.local_db_mutex.unlock();
                    if (group_auto_bulk_ops > 0) {
                        const record_now_ns = platform_time.monotonicNs();
                        cache.recordAutoBulkIngestOpsLocked(group.group_id, table_name, group_auto_bulk_ops, record_now_ns) catch |err| {
                            std.log.err("provisioned batch auto bulk accounting failed table={s} group_id={} ops={} err={s}", .{
                                table_name,
                                group.group_id,
                                group_auto_bulk_ops,
                                @errorName(err),
                            });
                            return err;
                        };
                        const rolled = try cache.rollRequestedAutoBulkIngestLocked(group.group_id, table_name, platform_time.monotonicNs());
                        if (rolled) {
                            self.local_db_mutex.unlock();
                            publishRuntimeStatusSnapshot(self, alloc, table_name, group.group_id, cached.db) catch |err| {
                                std.log.warn("auto bulk roll runtime status publish failed table={s} group_id={} err={s}", .{
                                    table_name,
                                    group.group_id,
                                    @errorName(err),
                                });
                            };
                            lockAtomic(&self.local_db_mutex);
                        }
                    }
                } else {
                    var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group.group_id, self.backend_runtime);
                    defer db.close();
                    try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group.group_id, &db);
                    try applyGroupBatch(alloc, self.catalog, &db, table_name, group, req);
                    self.finishTransientManagedDbWriteBeforeClose(table_name, group.group_id, &db);
                }
            }
        }
        lockAtomic(&self.local_db_mutex);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
        self.notifyLocalChange(table_name, .data);
    }

    fn backupTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveSingleRangeGroup(alloc, self.catalog, table_name)) orelse return null;
        self.beginGroupOperation(table_name, group_id);
        lockAtomic(&self.local_db_mutex);
        self.invalidateWriteCache(table_name);
        self.local_db_mutex.unlock();
        defer {
            self.endGroupOperation(table_name, group_id);
        }
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();

        const snapshot_token = try std.fmt.allocPrint(alloc, "{s}-g{d}", .{ plan.backup_id, group_id });
        defer alloc.free(snapshot_token);
        _ = try db.snapshot(snapshot_token);

        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ path, snapshot_token });
        defer alloc.free(snapshot_root);
        const dest_root = try backups_api.shardSnapshotPath(alloc, plan.backup_root, plan.backup_id, group_id);
        defer alloc.free(dest_root);
        try backups_api.copyDirectoryRecursive(alloc, snapshot_root, dest_root);

        const rel_path = try backups_api.shardSnapshotRelPath(alloc, plan.backup_id, group_id);
        errdefer alloc.free(rel_path);
        const byte_range = db.getRange();
        const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
        shards[0] = .{
            .group_id = group_id,
            .start_key = try alloc.dupe(u8, byte_range.start),
            .end_key = if (byte_range.end.len > 0) try alloc.dupe(u8, byte_range.end) else null,
            .snapshot_path = rel_path,
        };
        return shards;
    }

    fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (plan.manifest.shards.len != 1) return error.UnsupportedBackupFormat;

        const group_id = (try table_catalog.resolveSingleRangeGroup(alloc, self.catalog, table_name)) orelse return null;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ plan.backup_root, plan.manifest.shards[0].snapshot_path });
        defer alloc.free(snapshot_root);
        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);
        var restore_io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer restore_io_impl.deinit();

        const ready_deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
        while (true) {
            if (std.Io.Dir.cwd().statFile(restore_io_impl.io(), path, .{})) |_| break else |_| {}
            if (platform_time.monotonicNs() >= ready_deadline_ns) break;
            sleepNs(50 * std.time.ns_per_ms);
        }

        runTestBeforeRestoreWorkHook();
        try prepareLocalTablePathForRestore(alloc, path);
        db_mod.DB.restoreSnapshotToDeferredRuntimeRepair(alloc, snapshot_root, path, .{
            .identity_namespace = identity_namespace,
        }, .{
            .backup_id = plan.manifest.backup_id,
            .location = plan.backup_root,
            .snapshot_path = plan.manifest.shards[0].snapshot_path,
            .group_id = group_id,
        }) catch |err| {
            if (err == error.IdentityNamespaceMismatch) {
                std.log.warn("provisioned restoreTable failed table={s} group_id={d} path={s} snapshot_root={s} err={}", .{
                    table_name,
                    group_id,
                    path,
                    snapshot_root,
                    err,
                });
            } else {
                std.log.err("provisioned restoreTable failed table={s} group_id={d} path={s} snapshot_root={s} err={}", .{
                    table_name,
                    group_id,
                    path,
                    snapshot_root,
                    err,
                });
            }
            return err;
        };

        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
        self.notifyLocalChange(table_name, .data);
        self.requestRestoreRepairCatchUp(table_name, group_id);
    }

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const txn_id = nextTxnId();
        return try commitTransactionWithId(ptr, alloc, txn_id, nextTxnTimestamp(), tables, sync_level);
    }

    fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        _: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var worker_impl = distributed_txn.LocalTableWriteParticipantWorker.init(self.source());
        const commit_version = begin_timestamp + 1;
        return try distributed_txn.executeMultiTableCommit(
            alloc,
            self.catalog,
            worker_impl.worker(),
            txn_id,
            begin_timestamp,
            commit_version,
            tables,
            if (comptime build_options.with_tla) tracing.stderrAntflyTraceWriter() else null,
        );
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (self.raft_batcher) |batcher| {
            try batcher.batchGroupLocal(alloc, group_id, table_name, req);
            return {};
        }
        return try self.applyReplicatedBatchGroupLocal(alloc, group_id, table_name, req);
    }

    pub fn applyReplicatedBatchGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var apply_req = req;
        apply_req.sync_level = .write;
        const auto_bulk_ops = autoBulkIngestBatchOps(apply_req);
        const auto_bulk_now_ns = platform_time.monotonicNs();
        self.beginGroupOperation(table_name, group_id);
        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        defer {
            self.endGroupOperation(table_name, group_id);
        }
        errdefer {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            self.invalidateReadCache(table_name);
            self.invalidateWriteCache(table_name);
        }
        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(
                alloc,
                cache,
                path,
                group_id,
                table_name,
                .default_async,
                if (auto_bulk_ops > 0) auto_bulk_now_ns else null,
                if (auto_bulk_ops > 0) auto_bulk_now_ns else null,
            );
            defer cached.deinit(alloc);
            try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, apply_req.writes, apply_req.transforms);
            runTestBeforeBatchExecutionHook();
            try cached.db.batchWithoutRangeValidation(apply_req);
            {
                lockAtomic(&self.local_db_mutex);
                defer self.local_db_mutex.unlock();
                if (auto_bulk_ops > 0) {
                    const record_now_ns = platform_time.monotonicNs();
                    try cache.recordAutoBulkIngestOpsLocked(group_id, table_name, auto_bulk_ops, record_now_ns);
                    const rolled = try cache.rollRequestedAutoBulkIngestLocked(group_id, table_name, platform_time.monotonicNs());
                    if (rolled) {
                        self.local_db_mutex.unlock();
                        publishRuntimeStatusSnapshot(self, alloc, table_name, group_id, cached.db) catch |err| {
                            std.log.warn("auto bulk roll runtime status publish failed table={s} group_id={} err={s}", .{
                                table_name,
                                group_id,
                                @errorName(err),
                            });
                        };
                        lockAtomic(&self.local_db_mutex);
                    }
                }
                self.markWriteCacheDirty(table_name);
            }
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
        } else {
            var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            try validateTableBatchAgainstCatalogSchema(alloc, self.catalog, &db, table_name, apply_req.writes, apply_req.transforms);
            runTestBeforeBatchExecutionHook();
            try db.batchWithoutRangeValidation(apply_req);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
    }

    pub fn syncReplicatedBatchGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        sync_level: db_mod.types.SyncLevel,
    ) !void {
        switch (sync_level) {
            .propose, .write => return,
            .enrichments, .full_text, .aknn, .full_index => {},
        }

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(
                alloc,
                cache,
                path,
                group_id,
                table_name,
                .default_async,
                null,
                null,
            );
            defer cached.deinit(alloc);
            try cached.db.waitForCurrentSyncLevel(sync_level);
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
        } else {
            var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            try db.waitForCurrentSyncLevel(sync_level);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
        }
    }

    fn finishTransientManagedDbWriteBeforeClose(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) void {
        if (self.runtime_status_cache == null) return;
        drainManagedDbBeforeClose(db) catch |err| {
            if (!isTransientReplayVisibilityError(err)) {
                std.log.warn("transient managed writer drain before status publish failed table={s} group_id={} err={s}", .{
                    table_name,
                    group_id,
                    @errorName(err),
                });
            }
        };
        _ = self.publishManagedRuntimeStatusBestEffort(table_name, group_id, db);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        participants: []const []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        try recoverProvisionedTransactionsOnce(self, alloc, &db);
        _ = try db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        try recoverProvisionedTransactionsOnce(self, alloc, &db);
        try validateTransactionAgainstCatalogSchema(alloc, self.catalog, &db, table_name, req.writes, req.transforms);
        try db.writeTransaction(txn_id, req);
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        if (status == .committed) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateReadCache(table_name);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            errdefer {
                lockAtomic(&self.local_db_mutex);
                defer self.local_db_mutex.unlock();
                self.invalidateReadCache(table_name);
                self.invalidateWriteCache(table_name);
            }
        }
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        try db.resolveTransactionIntents(txn_id, status, commit_version);
        if (status == .committed) try drainManagedDbBeforeClose(&db);
        const participant = try std.fmt.allocPrint(alloc, "group:{d}", .{group_id});
        defer alloc.free(participant);
        try db.markTransactionParticipantResolved(txn_id, participant);
        if (status == .committed) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateWriteCache(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        try recoverProvisionedTransactionsOnce(self, alloc, &db);
        return try db.getTransactionStatus(txn_id);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.snapshotRuntimeStatusesBestEffort(alloc, table_name);
    }

    fn collectRuntimeStatusLeasesFromWriteCacheLocked(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        out: *std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb),
    ) !bool {
        const cache = self.write_cache orelse return false;
        out.clearRetainingCapacity();

        if (self.isWriteCacheDirtyForTable(table_name)) {
            _ = try cache.finishExpiredAutoBulkIngestLocked(platform_time.monotonicNs());
        }

        var i: usize = 0;
        while (i < cache.entries.items.len) {
            const entry = cache.entries.items[i];
            if (!std.mem.eql(u8, entry.table_name, table_name)) {
                i += 1;
                continue;
            }
            if (entry.lsm_root_generation == self.lsmRootGeneration(entry.group_id)) {
                i += 1;
                continue;
            }
            _ = cache.entries.orderedRemove(i);
            cache.retireEntryLocked(entry);
        }

        var matching_entries: usize = 0;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.lsmRootGeneration(entry.group_id)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) continue;
            matching_entries += 1;
        }
        try out.ensureTotalCapacity(alloc, matching_entries);

        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.lsmRootGeneration(entry.group_id)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) continue;
            lockAtomic(&cache.entry_lifecycle_mutex);
            entry.active_leases += 1;
            cache.entry_lifecycle_mutex.unlock();
            out.appendAssumeCapacity(.{
                .cache = cache,
                .entry = entry,
                .db = &entry.db,
                .schema_json = entry.schema_json,
            });
        }
        return out.items.len != 0;
    }

    fn runtimeStatusesFromCachedDbLeasesBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        leases: []const ProvisionedTableWriteCache.CachedDb,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const snapshot_cache = self.runtime_status_cache orelse return null;
        if (self.isWriteCacheDirtyForTable(table_name)) {
            if (self.write_cache) |cache| {
                lockAtomic(&self.local_db_mutex);
                defer self.local_db_mutex.unlock();
                _ = try cache.finishExpiredAutoBulkIngestLocked(platform_time.monotonicNs());
            }
        }
        var items = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (items.items) |*item| item.deinit(alloc);
            items.deinit(alloc);
        }

        try items.ensureTotalCapacity(alloc, leases.len);
        for (leases) |lease| {
            const group_id = lease.entry.?.group_id;
            try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, lease.db);
            var owned_status = (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) orelse continue;
            errdefer owned_status.deinit(alloc);
            try self.overlayReadCacheIndexVisibilityBestEffort(alloc, table_name, group_id, &owned_status);
            items.appendAssumeCapacity(owned_status);
        }
        if (items.items.len == 0) {
            items.deinit(alloc);
            return try snapshot_cache.snapshot(alloc, table_name);
        }
        return .{ .items = try items.toOwnedSlice(alloc) };
    }

    fn overlayReadCacheHbcStatsBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) !void {
        const read_cache = self.read_cache orelse return;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var read_lease = read_cache.getOrOpen(
            path,
            self.catalog,
            group_id,
            self.lsmRootGeneration(group_id),
            table_name,
        ) catch return;
        defer read_lease.release();

        overlayDenseHbcCacheStatsFromDb(&status.stats, read_lease.db);
        if (self.runtime_status_cache) |snapshot_cache| {
            try snapshot_cache.upsertGroupStatus(table_name, status.*);
        }
    }

    fn overlayReadCacheIndexVisibilityBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) !void {
        if (self.isWriteCacheDirtyForTable(table_name)) return;
        if (self.hasActiveBulkIngestSessionForTableBestEffort(table_name)) return;

        const read_cache = self.read_cache orelse return;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var read_lease = read_cache.getOrOpen(
            path,
            self.catalog,
            group_id,
            self.lsmRootGeneration(group_id),
            table_name,
        ) catch return;
        defer read_lease.release();

        const visible_stats = try read_lease.db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, visible_stats);

        for (status.stats.indexes) |*item| {
            const visible = for (visible_stats.indexes) |candidate| {
                if (std.mem.eql(u8, candidate.name, item.name)) break candidate;
            } else continue;

            item.doc_count = visible.doc_count;
            item.term_count = visible.term_count;
            item.edge_count = visible.edge_count;
            item.node_count = visible.node_count;
            item.hbc_cache = visible.hbc_cache;
        }

        if (self.runtime_status_cache) |snapshot_cache| {
            try snapshot_cache.upsertGroupStatus(table_name, status.*);
        }
    }

    fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        for (group_ids) |group_id| {
            self.beginGroupOperation(table_name, group_id);
            {
                defer self.endGroupOperation(table_name, group_id);
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
                defer alloc.free(path);

                if (self.write_cache) |cache| {
                    var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
                    defer cached.deinit(alloc);
                    if (try corruptEmbeddingArtifactInDb(alloc, cached.db, doc_key, index_name)) {
                        lockAtomic(&self.local_db_mutex);
                        self.invalidateReadCache(table_name);
                        self.markWriteCacheDirty(table_name);
                        self.local_db_mutex.unlock();
                        self.notifyLocalChange(table_name, .data);
                        return;
                    }
                } else {
                    var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
                    defer db.close();
                    try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
                    if (try corruptEmbeddingArtifactInDb(alloc, &db, doc_key, index_name)) {
                        lockAtomic(&self.local_db_mutex);
                        self.invalidateReadCache(table_name);
                        self.markWriteCacheDirty(table_name);
                        self.local_db_mutex.unlock();
                        self.notifyLocalChange(table_name, .data);
                        return;
                    }
                }
            }
        }

        return error.NotFound;
    }
};

pub const HostedProvisionedTableWriteSource = struct {
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    router: table_router.HostedGroupRouter,
    executor: http_common.RequestExecutor,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    foreground_derived_progress: bool = false,

    pub fn init(
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        router: table_router.HostedGroupRouter,
        executor: http_common.RequestExecutor,
    ) HostedProvisionedTableWriteSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .router = router,
            .executor = executor,
        };
    }

    pub fn withBackendRuntime(self: *HostedProvisionedTableWriteSource, backend_runtime: *db_mod.background_runtime.BackendRuntime) *HostedProvisionedTableWriteSource {
        self.backend_runtime = backend_runtime;
        return self;
    }

    pub fn withSecretStore(
        self: *HostedProvisionedTableWriteSource,
        secret_store: ?*common_secrets.FileStore,
    ) *HostedProvisionedTableWriteSource {
        self.secret_store = secret_store;
        if (hostedManagedDbCacheForRootIfPresent(self.replica_root_dir)) |cache| {
            cache.write_cache.secret_store = secret_store;
        }
        return self;
    }

    pub fn withRemoteContent(
        self: *HostedProvisionedTableWriteSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *HostedProvisionedTableWriteSource {
        self.remote_content = remote_content;
        if (hostedManagedDbCacheForRootIfPresent(self.replica_root_dir)) |cache| {
            cache.write_cache.remote_content = remote_content;
        }
        return self;
    }

    pub fn withForegroundDerivedProgress(self: *HostedProvisionedTableWriteSource) *HostedProvisionedTableWriteSource {
        self.foreground_derived_progress = true;
        return self;
    }

    fn shouldDrainAfterBatch(self: *const HostedProvisionedTableWriteSource, sync_level: db_mod.types.SyncLevel) bool {
        return self.foreground_derived_progress or shouldDrainCachedManagedDbAfterBatch(sync_level);
    }

    fn invalidateManagedCache(self: *HostedProvisionedTableWriteSource, table_name: []const u8) void {
        const hosted_cache = hostedManagedDbCacheForRootIfPresent(self.replica_root_dir) orelse return;
        lockAtomic(&hosted_cache.mutex);
        defer hosted_cache.mutex.unlock();
        hosted_cache.write_cache.invalidateTable(table_name);
    }

    fn getOrOpenCachedDbMode(
        self: *HostedProvisionedTableWriteSource,
        cache: *HostedManagedDbCache,
        path: []const u8,
        group_id: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
    ) !ProvisionedTableWriteCache.CachedDb {
        const lsm_root_generation: u64 = 0;
        if (cache.write_cache.backend_runtime == null) cache.write_cache.backend_runtime = self.backend_runtime;
        cache.write_cache.secret_store = self.secret_store;
        cache.write_cache.remote_content = self.remote_content;
        const identity_namespace = try loadTableIdentityNamespaceForGroup(cache.write_cache.alloc, self.catalog, table_name, group_id);
        const expected_identity_namespace = if (mode == .startup_catch_up or mode == .restore_repair)
            null
        else
            identity_namespace;
        if (mode == .status_only) {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            const cached = try cache.write_cache.getOrOpenLockedMode(path, self.catalog, group_id, lsm_root_generation, table_name, .status_only);
            try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
            return cached;
        }

        var prepared_open: ?ProvisionedTableWriteCache.PreparedOpen = null;
        defer if (prepared_open) |*prepared| prepared.deinit(cache.write_cache.alloc);

        {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            switch (try cache.write_cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    return cached;
                },
                .prepared => |prepared| prepared_open = prepared,
            }
        }

        lockAtomic(&cache.write_cache.open_mutex);
        defer cache.write_cache.open_mutex.unlock();

        {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            switch (try cache.write_cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    return cached;
                },
                .prepared => |prepared| {
                    prepared_open.?.deinit(cache.write_cache.alloc);
                    prepared_open = prepared;
                },
            }
        }

        if (try loadTableManagedMetadata(cache.write_cache.alloc, self.catalog, table_name)) |metadata| {
            prepared_open.?.indexes_json = metadata.indexes_json;
            prepared_open.?.schema_json = metadata.schema_json;
        }

        var opened: ?db_mod.DB = if (prepared_open.?.indexes_json) |value|
            try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(
                cache.write_cache.alloc,
                path,
                value,
                cache.write_cache.lsm_cache,
                cache.write_cache.hbc_cache,
                lsm_root_generation,
                cache.write_cache.resource_manager,
                mode,
                cache.write_cache.backend_runtime,
                cache.write_cache.local_termite_provider,
                cache.write_cache.secret_store,
                cache.write_cache.remote_content,
                identity_namespace,
            )
        else
            try db_mod.DB.open(cache.write_cache.alloc, path, .{
                .lsm_cache = cache.write_cache.lsm_cache,
                .hbc_cache = cache.write_cache.hbc_cache,
                .lsm_root_generation = lsm_root_generation,
                .resource_manager = cache.write_cache.resource_manager,
                .backend_runtime = cache.write_cache.backend_runtime,
                .identity_namespace = identity_namespace,
                .prefer_existing_identity_namespace = identity_namespace != null,
                .open_mode = switch (mode) {
                    .default => .writer,
                    .default_async, .writer_no_replay => .writer_no_replay,
                    .startup_catch_up, .restore_repair => .writer_no_replay,
                    .status_only => .status_only,
                },
                .start_index_workers = if (mode == .startup_catch_up) false else true,
                .ttl_cleanup = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .transaction_recovery = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .text_merge = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
            });
        defer if (opened) |*db| db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &opened.?);

        lockAtomic(&cache.mutex);
        defer cache.mutex.unlock();
        return try cache.write_cache.adoptPreparedOpenLocked(&opened, group_id, lsm_root_generation, table_name, mode, &prepared_open.?);
    }

    fn reconcileCachedIndexCreate(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !void {
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);

        var hosted_cache: ?*HostedManagedDbCache = null;
        for (group_ids) |group_id| {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);

            // Public routes can run on nodes with stale on-disk group paths.
            // Reconcile only groups the hosted router says this process actively owns.
            if (self.router.localStatus(group_id) != .active) continue;

            const cache = hosted_cache orelse blk: {
                const cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
                hosted_cache = cache;
                break :blk cache;
            };

            var cached = try self.getOrOpenCachedDbMode(cache, path, group_id, table_name, .default_async);
            defer cached.deinit(cache.write_cache.alloc);

            if (try cached.db.core.indexRequiresEnrichmentReplay(index_name)) {
                _ = try seedManagedIndexReplayFromStoredDocsIfNeeded(alloc, cached.db, index_name);
            }

            if (try cached.db.hasPendingDenseArtifactRebuild(alloc)) {
                _ = try cached.db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
            }
            try drainManagedDbBeforeClose(cached.db);
        }
    }

    pub fn source(self: *HostedProvisionedTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_index = createIndex,
                .drop_index = dropIndex,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_id = commitTransactionWithId,
                .backup_table = backupTable,
                .restore_table = restoreTable,
                .batch = batch,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .local_runtime_statuses = localRuntimeStatuses,
            },
        };
    }

    fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        _: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.invalidateManagedCache(table_name);
        try self.reconcileCachedIndexCreate(alloc, table_name, index_name);
    }

    fn dropIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.invalidateManagedCache(table_name);
        try dropLocalTableIndex(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name, index_name);
        self.invalidateManagedCache(table_name);
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var grouped = std.ArrayListUnmanaged(GroupBatch).empty;
        defer {
            for (grouped.items) |*group| group.deinit(alloc);
            grouped.deinit(alloc);
        }

        for (req.writes) |write| {
            const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, write.key)) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.writes.append(alloc, write);
        }
        for (req.deletes) |key| {
            const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, key)) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.deletes.append(alloc, key);
        }
        for (req.transforms) |transform| {
            const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, transform.key)) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.transforms.append(alloc, transform);
        }

        for (grouped.items) |group| {
            var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group.group_id, .prefer_leader);
            if (resolved_route) |*route| {
                defer route.deinit(alloc);

                switch (route.*) {
                    .local => {
                        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group.group_id);
                        defer alloc.free(path);
                        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
                        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group.group_id, table_name, .default_async);
                        defer cached.deinit(hosted_cache.write_cache.alloc);
                        try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, group.writes.items, group.transforms.items);
                        try cached.db.batch(.{
                            .writes = group.writes.items,
                            .deletes = group.deletes.items,
                            .transforms = group.transforms.items,
                            .graph_writes = req.graph_writes,
                            .graph_deletes = req.graph_deletes,
                            .predicates = req.predicates,
                            .timestamp_ns = req.timestamp_ns,
                            .sync_level = req.sync_level,
                        });
                        if (self.shouldDrainAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(cached.db);
                    },
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try encodeRemoteBatchRequest(alloc, .{
                            .writes = group.writes.items,
                            .deletes = group.deletes.items,
                            .transforms = group.transforms.items,
                            .graph_writes = req.graph_writes,
                            .graph_deletes = req.graph_deletes,
                            .predicates = req.predicates,
                            .timestamp_ns = req.timestamp_ns,
                            .sync_level = req.sync_level,
                        });
                        defer alloc.free(body);
                        var response = try client.fetchGroupBatch(remote.base_uri, group.group_id, table_name, body);
                        response.deinit(alloc);
                    },
                }
            } else {
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group.group_id);
                defer alloc.free(path);
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
                    error.FileNotFound => return null,
                    else => return err,
                };
                const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
                var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group.group_id, table_name, .default_async);
                defer cached.deinit(hosted_cache.write_cache.alloc);
                try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, group.writes.items, group.transforms.items);
                try cached.db.batch(.{
                    .writes = group.writes.items,
                    .deletes = group.deletes.items,
                    .transforms = group.transforms.items,
                    .graph_writes = req.graph_writes,
                    .graph_deletes = req.graph_deletes,
                    .predicates = req.predicates,
                    .timestamp_ns = req.timestamp_ns,
                    .sync_level = req.sync_level,
                });
                if (self.shouldDrainAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(cached.db);
            }
        }
    }

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const txn_id = nextTxnId();
        return try commitTransactionWithId(ptr, alloc, txn_id, nextTxnTimestamp(), tables, sync_level);
    }

    fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        _: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var worker_impl = distributed_txn.HostedParticipantWorker.init(self.catalog, self.router, self.source(), self.executor);
        const commit_version = begin_timestamp + 1;
        return try distributed_txn.executeMultiTableCommit(
            alloc,
            self.catalog,
            worker_impl.worker(),
            txn_id,
            begin_timestamp,
            commit_version,
            tables,
            if (comptime build_options.with_tla) tracing.stderrAntflyTraceWriter() else null,
        );
    }

    fn backupTable(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        return error.UnsupportedOperation;
    }

    fn restoreTable(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: backups_api.TableRestorePlan,
    ) !?void {
        return error.UnsupportedOperation;
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default_async);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, req.writes, req.transforms);
        try cached.db.batchWithoutRangeValidation(req);
        if (self.shouldDrainAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(cached.db);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        participants: []const []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try recoverHostedTransactionsOnce(self, alloc, cached.db);
        _ = try cached.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try recoverHostedTransactionsOnce(self, alloc, cached.db);
        try validateTransactionAgainstCatalogSchema(alloc, self.catalog, cached.db, table_name, req.writes, req.transforms);
        try cached.db.writeTransaction(txn_id, req);
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try cached.db.resolveTransactionIntents(txn_id, status, commit_version);
        if (status == .committed) try drainManagedDbBeforeClose(cached.db);
        const participant = try std.fmt.allocPrint(alloc, "group:{d}", .{group_id});
        defer alloc.free(participant);
        try cached.db.markTransactionParticipantResolved(txn_id, participant);
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try recoverHostedTransactionsOnce(self, alloc, cached.db);
        return try cached.db.getTransactionStatus(txn_id);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (hostedManagedDbCacheForRootIfPresent(self.replica_root_dir)) |hosted_cache| {
            lockAtomic(&hosted_cache.mutex);
            defer hosted_cache.mutex.unlock();
            const statuses = try hosted_cache.write_cache.snapshotRuntimeStatusesLocked(alloc, table_name);
            if (statuses) |owned| return owned;
        }
        return try snapshotLocalTableRuntimeStatusesUncached(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name);
    }

    fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        for (group_ids) |group_id| {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);
            const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
            var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
            defer cached.deinit(hosted_cache.write_cache.alloc);
            if (try corruptEmbeddingArtifactInDb(alloc, cached.db, doc_key, index_name)) return;
        }

        return error.NotFound;
    }
};

const GroupBatch = struct {
    group_id: u64,
    writes: std.ArrayListUnmanaged(db_mod.types.BatchWrite) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    transforms: std.ArrayListUnmanaged(db_mod.types.DocumentTransform) = .empty,

    fn deinit(self: *GroupBatch, alloc: std.mem.Allocator) void {
        self.writes.deinit(alloc);
        self.deletes.deinit(alloc);
        self.transforms.deinit(alloc);
        self.* = undefined;
    }
};

fn ensureGroupBatch(
    alloc: std.mem.Allocator,
    grouped: *std.ArrayListUnmanaged(GroupBatch),
    group_id: u64,
) !*GroupBatch {
    for (grouped.items) |*group| {
        if (group.group_id == group_id) return group;
    }
    try grouped.append(alloc, .{ .group_id = group_id });
    return &grouped.items[grouped.items.len - 1];
}

fn applyGroupBatch(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    group: GroupBatch,
    req: db_mod.types.BatchRequest,
) !void {
    try validateTableBatchAgainstCatalogSchema(alloc, catalog, db, table_name, group.writes.items, group.transforms.items);
    try applyGroupBatchUnchecked(db, group, req);
}

fn applyGroupBatchWithSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: ?[]const u8,
    group: GroupBatch,
    req: db_mod.types.BatchRequest,
) !void {
    try validateTableBatchAgainstSchemaJson(alloc, db, schema_json, group.writes.items, group.transforms.items);
    try applyGroupBatchUnchecked(db, group, req);
}

fn applyGroupBatchUnchecked(
    db: *db_mod.DB,
    group: GroupBatch,
    req: db_mod.types.BatchRequest,
) !void {
    try db.batch(.{
        .writes = group.writes.items,
        .deletes = group.deletes.items,
        .transforms = group.transforms.items,
        .graph_writes = req.graph_writes,
        .graph_deletes = req.graph_deletes,
        .predicates = req.predicates,
        .timestamp_ns = req.timestamp_ns,
        .sync_level = req.sync_level,
    });
    if (shouldDrainManagedDbAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(db);
}

fn parseIndexKind(value: std.json.Value) !db_mod.types.IndexKind {
    if (value != .object) return .full_text;
    const type_value = value.object.get("type") orelse return .full_text;
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
    if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
    if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
    if (std.mem.eql(u8, type_value.string, "embeddings")) {
        const sparse = if (value.object.get("sparse")) |sparse_value| switch (sparse_value) {
            .bool => sparse_value.bool,
            else => return error.InvalidCreateTableRequest,
        } else false;
        return if (sparse) .sparse_vector else .dense_vector;
    }
    return error.UnsupportedCreateTableRequest;
}

fn parseIndexConfig(alloc: std.mem.Allocator, index_name: []const u8, index_json: []const u8) !db_mod.types.IndexConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    const kind = try parseIndexKind(parsed.value);
    const config_json = try extractIndexConfigJson(alloc, index_name, parsed.value);
    errdefer alloc.free(config_json);
    return .{
        .name = try alloc.dupe(u8, index_name),
        .kind = kind,
        .config_json = config_json,
    };
}

pub fn validateIndexConfig(alloc: std.mem.Allocator, index_name: []const u8, index_json: []const u8) !void {
    const cfg = try parseIndexConfig(alloc, index_name, index_json);
    defer {
        alloc.free(cfg.name);
        alloc.free(cfg.config_json);
    }
    if (cfg.kind == .algebraic) {
        var parsed = std.json.parseFromSlice(db_mod.algebraic.index.Config, alloc, cfg.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return error.InvalidCreateTableRequest;
        defer parsed.deinit();
        db_mod.algebraic.index.validateConfig(parsed.value) catch return error.InvalidCreateTableRequest;
    }
}

fn extractIndexConfigJson(alloc: std.mem.Allocator, index_name: []const u8, value: std.json.Value) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    const kind = try parseIndexKind(value);
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
        if (std.mem.eql(u8, entry.key_ptr.*, "type") or
            std.mem.eql(u8, entry.key_ptr.*, "name") or
            std.mem.eql(u8, entry.key_ptr.*, "description") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments"))
        {
            continue;
        }
        if (std.mem.eql(u8, entry.key_ptr.*, "version") and kind != .algebraic) continue;
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

test "table write index parser extracts expanded algebraic capability config" {
    const alloc = std.testing.allocator;
    const cfg = try parseIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","version":1,"table":"sales","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[]}
    );
    defer {
        alloc.free(cfg.name);
        alloc.free(cfg.config_json);
    }

    try std.testing.expectEqual(db_mod.types.IndexKind.algebraic, cfg.kind);
    try std.testing.expectEqualStrings("sales_rollup", cfg.name);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"version\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"measure_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"materializations\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"type\":\"algebraic\"") == null);
}

test "table write index validation checks expanded algebraic config semantics" {
    const alloc = std.testing.allocator;

    try validateIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","table":"sales","schema_version":1,"capability_fingerprint":"sales:v1","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[]}
    );

    try std.testing.expectError(error.InvalidCreateTableRequest, validateIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","table":"sales","schema_version":1,"capability_fingerprint":"sales:v1","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[{"name":"sum_by_customer","op":"median","group_by":["customer"],"measure":"amount"}]}
    ));

    try std.testing.expectError(error.InvalidCreateTableRequest, validateIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","table":"sales","schema_version":1,"capability_fingerprint":"sales:v1","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[{"name":"sum_by_customer","op":"sum","group_by":["missing"],"measure":"amount"}]}
    ));
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn nextTxnTimestamp() u64 {
    // Transaction timestamps are stored in shard metadata and later compared
    // against transaction recovery cutoffs, so they must stay on realtime.
    return platform_time.realtimeNs();
}

fn nextTxnId() db_mod.types.TxnId {
    const nonce = txn_id_nonce.fetchAdd(1, .monotonic);
    var txn_id: db_mod.types.TxnId = undefined;
    std.mem.writeInt(u64, txn_id[0..8], nextTxnTimestamp(), .big);
    std.mem.writeInt(u64, txn_id[8..16], nonce, .big);
    return txn_id;
}

fn boundConflict(table: distributed_txn.TableCommitRequest, err: anyerror) distributed_txn.CommitConflict {
    if (table.predicates.len > 0) {
        return .{
            .table_name = table.table_name,
            .key = table.predicates[0].key,
            .message = "version conflict",
            .phase = .prepare,
        };
    }
    const message = switch (err) {
        error.IntentConflict => "intent conflict",
        else => "transaction conflict",
    };
    if (table.writes.len > 0) {
        return .{ .table_name = table.table_name, .key = table.writes[0].key, .message = message, .phase = .prepare };
    }
    if (table.deletes.len > 0) {
        return .{ .table_name = table.table_name, .key = table.deletes[0], .message = message, .phase = .prepare };
    }
    return .{ .table_name = table.table_name, .key = "", .message = message, .phase = .prepare };
}

fn openManagedDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !db_mod.DB {
    return try openManagedDbForTableWithRuntime(alloc, path, catalog, table_name, null);
}

fn openManagedDbForTableWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbForTableWithCacheAndRuntime(alloc, path, catalog, table_name, null, null, 0, null, backend_runtime);
}

fn openManagedDbForTableGroupWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbForTableGroupWithCacheAndRuntime(alloc, path, catalog, table_name, group_id, null, null, 0, null, backend_runtime);
}

fn openManagedDbForTableWithCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbForTableWithCacheAndRuntime(alloc, path, catalog, table_name, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, null);
}

fn openManagedDbForTableWithCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse return try db_mod.DB.open(alloc, path, .{
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
    });
    defer alloc.free(indexes_json);

    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, backend_runtime);
}

fn openManagedDbForTableGroupWithCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse {
        var db = try db_mod.DB.open(alloc, path, .{
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
        });
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        return db;
    };
    defer alloc.free(indexes_json);

    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, .default, backend_runtime, identity_namespace);
}

fn openManagedDbForTableWithIndexesJson(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
) !db_mod.DB {
    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, null, null, 0, null, null);
}

fn openManagedDbForTableWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, null);
}

fn openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const raw_indexes_json = indexes_json orelse return try db_mod.DB.open(alloc, path, .{
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
    });
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(alloc, path, raw_indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, .default, backend_runtime);
}

fn reconcileLocalTableIndexes(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
) !void {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, catalog, table_name, group_id, backend_runtime);
        db.close();
    }
}

fn reconcileCachedLocalTableIndexCreate(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    cache: *ProvisionedTableWriteCache,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        self.catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    var managed_visibility_changed = false;
    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var cached = try cache.getOrOpenLockedMode(path, self.catalog, group_id, self.lsmRootGeneration(group_id), table_name, .default_async);
        defer cached.deinit(alloc);
        cached.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(cached.entry.?.table_name, group_id, cached.db));

        try catchUpManagedIndexCreate(alloc, cached.db, index_name);
        try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, cached.db);
        managed_visibility_changed = true;
    }
    return managed_visibility_changed;
}

fn reconcileUncachedLocalTableIndexCreate(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        self.catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    var managed_visibility_changed = false;
    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();

        try catchUpManagedIndexCreate(alloc, &db, index_name);
        try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, &db);
        managed_visibility_changed = true;
    }
    return managed_visibility_changed;
}

fn reconcileLocalTableIndexCreate(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    if (self.write_cache) |cache| {
        return try reconcileCachedLocalTableIndexCreate(self, alloc, cache, table_name, index_name);
    }
    return try reconcileUncachedLocalTableIndexCreate(self, alloc, table_name, index_name);
}

fn dropLocalTableIndex(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    index_name: []const u8,
) !void {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try db_mod.DB.open(alloc, path, .{
            .backend_runtime = backend_runtime,
        });
        defer db.close();
        _ = db.deleteIndex(index_name) catch |err| switch (err) {
            error.IndexNotFound => {},
            else => return err,
        };
    }
}

fn replayManagedIndexForTableIfNeeded(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    var managed_visibility_changed = false;
    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, catalog, table_name, group_id, backend_runtime);
        defer db.close();
        if (!try db.core.indexRequiresEnrichmentReplay(index_name)) continue;
        managed_visibility_changed = true;

        _ = try seedManagedIndexReplayFromStoredDocsIfNeeded(alloc, &db, index_name);

        if (try db.hasPendingDenseArtifactRebuild(alloc)) {
            _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
            try db.runUntilIdle();
        }
    }
    return managed_visibility_changed;
}

fn catchUpManagedIndexCreate(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !void {
    if (try db.core.indexRequiresEnrichmentReplay(index_name)) {
        if (db.enrichment_runtime != null) {
            _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        } else {
            _ = try seedManagedIndexReplayFromStoredDocsIfNeeded(alloc, db, index_name);
        }
    }

    try db.runUntilIdle();
    try db.catchUpPendingDerivedReplay();
    try db.runUntilIdle();

    if (try db.hasPendingDenseArtifactRebuild(alloc)) {
        _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
        try db.runUntilIdle();
        try db.catchUpPendingDerivedReplay();
        try db.runUntilIdle();
    }
    try db.core.index_manager.syncAll(false);
}

fn managedIndexReplayDebtRequired(replay_debt: anytype, index_name: []const u8) bool {
    for (replay_debt) |status| {
        if (!std.mem.eql(u8, status.index_name, index_name)) continue;
        return status.catch_up_required;
    }
    return false;
}

fn managedIndexCoverageIncomplete(alloc: std.mem.Allocator, db: *db_mod.DB, index_name: []const u8) !bool {
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    const primary_doc_count = try db.primaryDocCount(alloc);
    if (primary_doc_count == 0) return false;
    for (stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, index_name)) continue;
        return item.doc_count < primary_doc_count;
    }
    return false;
}

fn seedManagedIndexReplayFromStoredDocsIfNeeded(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !bool {
    const replay_debt = try db.listDerivedReplayDebt(alloc);
    defer {
        for (replay_debt) |*status| status.deinit(alloc);
        alloc.free(replay_debt);
    }
    if (managedIndexReplayDebtRequired(replay_debt, index_name)) return false;
    if (!try managedIndexCoverageIncomplete(alloc, db, index_name)) return false;
    _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
    return true;
}

fn corruptEmbeddingArtifactInDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    doc_key: []const u8,
    index_name: []const u8,
) !bool {
    const dense_name = db.core.index_manager.denseEmbeddingName(index_name);
    const sparse_name = db.core.index_manager.sparseEmbeddingName(index_name);
    const candidate_names = [_]?[]const u8{
        index_name,
        dense_name,
        sparse_name,
    };
    const prefix = try db_mod.internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "embedding");
    defer alloc.free(prefix);
    const artifacts = try db.core.scanStorePrefix(alloc, prefix);
    defer db_mod.docstore.DocStore.freeResults(alloc, artifacts);

    var fallback_key: ?[]const u8 = null;
    for (artifacts) |entry| {
        if (!db_mod.internal_keys.isEmbeddingArtifactKey(entry.key)) continue;
        if (fallback_key == null) fallback_key = entry.key;
        for (candidate_names) |candidate_opt| {
            const candidate = candidate_opt orelse continue;
            if (!db_mod.internal_keys.matchesEmbeddingArtifactName(entry.key, candidate)) continue;
            try db.core.store.put(entry.key, "bad-artifact");
            return true;
        }
    }

    if (fallback_key) |artifact_key| {
        try db.core.store.put(artifact_key, "bad-artifact");
        return true;
    }

    const injection_names = [_]?[]const u8{
        dense_name,
        sparse_name,
        index_name,
    };
    for (injection_names) |candidate_opt| {
        const candidate = candidate_opt orelse continue;
        const artifact_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, candidate);
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");
        return true;
    }

    return false;
}

fn snapshotLocalTableRuntimeStatusesUncached(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
) !?runtime_status.LocalTableRuntimeStatuses {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);
    if (group_ids.len == 0) return null;

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, group_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (items[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(items);
    }

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try openManagedDbForStatusWithCache(alloc, path, catalog, table_name, group_id, null, null, 0, null, backend_runtime);
        errdefer db.close();
        items[initialized] = .{
            .group_id = group_id,
            .stats = try db.runtimeStatusStatsConsistent(alloc),
        };
        initialized += 1;
        db.close();
    }

    return .{ .items = items };
}

fn openManagedDbWithIndexesJson(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCache(alloc, path, indexes_json, null, null, 0, null);
}

fn openManagedDbForStatusWithCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse {
        var db = try db_mod.DB.open(alloc, path, .{
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .transaction_recovery = .{ .enabled = false },
            .text_merge = .{ .enabled = false },
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
        });
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        return db;
    };
    defer alloc.free(indexes_json);

    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .status_only,
        backend_runtime,
        identity_namespace,
    );
}

pub fn openManagedDbForStatusWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .status_only,
        backend_runtime,
    );
}

fn openManagedDbWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .default,
    );
}

const ManagedDbOpenMode = enum {
    default,
    default_async,
    writer_no_replay,
    startup_catch_up,
    restore_repair,
    status_only,
};

fn openManagedDbWithIndexesJsonAndCacheMode(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        null,
    );
}

fn openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        null,
    );
}

fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?doc_identity.Namespace,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        null,
        null,
        null,
        identity_namespace,
    );
}

fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermite(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        local_termite_provider,
        secret_store,
        remote_content,
        null,
    );
}

fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalTermiteAndIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    identity_namespace: ?doc_identity.Namespace,
) !db_mod.DB {
    const EmbedderSet = struct {
        dense: ?db_embedder.DenseEmbedder = null,
        sparse: ?db_embedder.SparseEmbedder = null,

        fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (self.dense) |owned| owned.deinit(allocator);
            if (self.sparse) |owned| owned.deinit(allocator);
        }
    };

    const createEmbedders = struct {
        fn run(
            allocator: std.mem.Allocator,
            raw_indexes_json: []const u8,
            local_provider: ?managed_embedder.LocalTermiteProvider,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
        ) !EmbedderSet {
            return .{
                .dense = try managed_embedder.ManagedEmbedder.createDenseEmbedderWithOptions(allocator, raw_indexes_json, .{ .local_termite_provider = local_provider, .secret_store = store, .remote_content = remote }),
                .sparse = try managed_embedder.ManagedEmbedder.createSparseEmbedderWithOptions(allocator, raw_indexes_json, .{ .local_termite_provider = local_provider, .secret_store = store, .remote_content = remote }),
            };
        }
    }.run;

    var embedders = if (mode == .startup_catch_up)
        EmbedderSet{}
    else
        try createEmbedders(alloc, indexes_json, local_termite_provider, secret_store, remote_content);
    errdefer embedders.deinit(alloc);

    const openDb = struct {
        fn run(
            allocator: std.mem.Allocator,
            db_path: []const u8,
            dense: ?db_embedder.DenseEmbedder,
            sparse: ?db_embedder.SparseEmbedder,
            cache: ?*lsm_backend.Cache,
            vector_cache: ?*hbc_mod.Cache,
            root_generation: u64,
            manager: ?*resource_manager_mod.ResourceManager,
            open_mode: ManagedDbOpenMode,
            runtime: ?*db_mod.background_runtime.BackendRuntime,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
            namespace: ?doc_identity.Namespace,
        ) !db_mod.DB {
            const base: db_mod.OpenOptions = .{
                .lsm_cache = cache,
                .hbc_cache = vector_cache,
                .lsm_root_generation = root_generation,
                .resource_manager = manager,
                .backend_runtime = runtime,
                .secret_store = store,
                .remote_content = remote,
                .identity_namespace = namespace,
                .prefer_existing_identity_namespace = namespace != null,
                .enrichment = .{
                    .dense_embedder = dense,
                    .sparse_embedder = sparse,
                },
            };
            return switch (open_mode) {
                .default => if (dense != null or sparse != null)
                    try db_mod.DB.open(allocator, db_path, base)
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                    }),
                .default_async, .writer_no_replay => if (dense != null or sparse != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = .{
                            .dense_embedder = dense,
                            .sparse_embedder = sparse,
                        },
                        .open_mode = .writer_no_replay,
                        // The managed write cache opens DBs synchronously while
                        // table/index metadata can still be settling. Keep
                        // index catalog opens serial on this no-replay path
                        // until the parallel index opener is allocator-safe.
                        .index_open_parallelism = 1,
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .open_mode = .writer_no_replay,
                        .index_open_parallelism = 1,
                    }),
                .startup_catch_up => try db_mod.DB.open(allocator, db_path, .{
                    .lsm_cache = cache,
                    .hbc_cache = vector_cache,
                    .lsm_root_generation = root_generation,
                    .resource_manager = manager,
                    .backend_runtime = runtime,
                    .secret_store = store,
                    .remote_content = remote,
                    .identity_namespace = namespace,
                    .prefer_existing_identity_namespace = namespace != null,
                    .open_mode = .writer_no_replay,
                    .start_index_workers = false,
                    .ttl_cleanup = .{ .enabled = false },
                    .transaction_recovery = .{ .enabled = false },
                    .text_merge = .{ .enabled = false },
                }),
                .restore_repair => if (dense != null or sparse != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = .{
                            .dense_embedder = dense,
                            .sparse_embedder = sparse,
                        },
                        .open_mode = .writer_no_replay,
                        .start_index_workers = true,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .open_mode = .writer_no_replay,
                        .start_index_workers = true,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
                .status_only => if (dense != null or sparse != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = .{
                            .dense_embedder = dense,
                            .sparse_embedder = sparse,
                        },
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
            };
        }
    }.run;

    var db = blk: {
        const dense = embedders.dense;
        const sparse = embedders.sparse;
        const opened = try openDb(alloc, path, dense, sparse, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace);
        embedders.dense = null;
        embedders.sparse = null;
        break :blk opened;
    };
    var db_open = true;
    errdefer if (db_open) db.close();

    try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
    if (mode == .status_only) return db;

    const summary = try metadata_table_provisioner.reconcileDbIndexes(alloc, &db, indexes_json);
    if (summary.indexes_added > 0 or summary.indexes_removed > 0) {
        // First-open provisioning can mutate the live index manager. Reopen so
        // request work runs against the stabilized post-reconcile state.
        db.close();
        db_open = false;
        embedders = try createEmbedders(alloc, indexes_json, local_termite_provider, secret_store, remote_content);
        db = blk: {
            const dense = embedders.dense;
            const sparse = embedders.sparse;
            const opened = try openDb(alloc, path, dense, sparse, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace);
            embedders.dense = null;
            embedders.sparse = null;
            break :blk opened;
        };
        db_open = true;
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
    }

    // Metadata-driven index reconciliation happens during open/reopen rather
    // than through DB.addIndex(), so managed enrichment replay must be re-armed
    // here for pre-existing documents after an index is added.
    if ((mode == .default or mode == .default_async) and summary.indexes_added > 0) {
        if (db.enrichment_runtime != null) {
            _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        }
    }
    return db;
}

fn drainManagedDbBeforeClose(db: *db_mod.DB) !void {
    // Provisioned writes open a managed DB per request, so queued enrichment
    // must drain before the DB is closed or semantic indexes can stay empty.
    try db.runUntilIdle();
    try db.core.index_manager.syncAll(false);
}

fn publishRuntimeStatusSnapshot(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !void {
    try publishRuntimeStatusSnapshotWithStartupPhaseMode(
        source,
        alloc,
        table_name,
        group_id,
        if (source.startup_catch_up_active.load(.monotonic)) .startup_catch_up else .idle,
        .best_effort,
        db,
    );
}

fn publishRuntimeStatusSnapshotConsistent(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !void {
    try publishRuntimeStatusSnapshotWithStartupPhaseMode(
        source,
        alloc,
        table_name,
        group_id,
        if (source.startup_catch_up_active.load(.monotonic)) .startup_catch_up else .idle,
        .consistent,
        db,
    );
}

fn publishRuntimeStatusSnapshotWithStartupPhase(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    phase: db_mod.types.StartupCatchUpPhase,
    db: *db_mod.DB,
) !void {
    try publishRuntimeStatusSnapshotWithStartupPhaseMode(source, alloc, table_name, group_id, phase, .best_effort, db);
}

const RuntimeStatusSnapshotMode = enum {
    best_effort,
    consistent,
};

fn publishRuntimeStatusSnapshotWithStartupPhaseMode(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    phase: db_mod.types.StartupCatchUpPhase,
    mode: RuntimeStatusSnapshotMode,
    db: *db_mod.DB,
) !void {
    const snapshot_cache = source.runtime_status_cache orelse return;
    const async_stats = db.snapshotAsyncIndexingStats();
    var cached_startup: db_mod.types.StartupCatchUpStats = .{};
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = group_id,
        .stats = .{},
    };
    var status_initialized = false;
    defer {
        if (status_initialized) {
            var owned = status;
            owned.deinit(alloc);
        }
    }
    if (phase != .idle) {
        cached_startup = try cachedStartupCatchUpStats(snapshot_cache, alloc, table_name, group_id);
    }
    if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |cached_status| {
        switch (mode) {
            .best_effort => {
                status = cached_status;
                status_initialized = true;
                db.overlayRuntimeStatusBestEffort(alloc, &status.stats);
            },
            .consistent => {
                const disk_bytes = cached_status.disk_bytes;
                const created_at_millis = cached_status.created_at_millis;
                var discard = cached_status;
                discard.deinit(alloc);
                status = .{
                    .group_id = group_id,
                    .disk_bytes = disk_bytes,
                    .created_at_millis = created_at_millis,
                    .stats = try db.runtimeStatusStatsConsistent(alloc),
                };
                status_initialized = true;
            },
        }
        markRuntimeStatusFromDb(source, &status, phase);
    }
    if (!status_initialized) {
        status = .{
            .group_id = group_id,
            .stats = switch (mode) {
                .best_effort => try db.stats(alloc),
                .consistent => try db.runtimeStatusStatsConsistent(alloc),
            },
        };
        status_initialized = true;
        markRuntimeStatusFromDb(source, &status, phase);
    }
    var startup = startupCatchUpStatsForPhase(phase, db);
    if (!startup.wal_retention_known and cached_startup.wal_retention_known) {
        startup.wal_retention_known = true;
        startup.wal_retained_segments = cached_startup.wal_retained_segments;
        startup.wal_retained_bytes = cached_startup.wal_retained_bytes;
    }
    applyStartupCatchUpAsyncOverlay(&status, async_stats, startup);
    try snapshot_cache.upsertGroupStatus(table_name, status);
}

fn markRuntimeStatusFromDb(
    source: *ProvisionedTableWriteSource,
    status: *runtime_status.LocalTableRuntimeStatus,
    phase: db_mod.types.StartupCatchUpPhase,
) void {
    status.metadata = .{
        .updated_at_ns = platform_time.monotonicNs(),
        .source = if (phase != .idle or source.startup_catch_up_active.load(.monotonic))
            .startup_catch_up
        else
            .live_writer_publish,
        .freshness = .fresh,
    };
}

fn cachedStartupCatchUpStats(
    snapshot_cache: *runtime_status.TableRuntimeSnapshotCache,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
) !db_mod.types.StartupCatchUpStats {
    if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |owned_status| {
        defer {
            var to_free = owned_status;
            to_free.deinit(alloc);
        }
        return owned_status.stats.async_indexing.startup;
    }
    return .{};
}

fn dbHbcCacheKindStatsFromIndex(cache_stats: hbc_mod.HbcCacheKindStats) db_mod.types.HbcCacheKindStats {
    return .{
        .used_bytes = cache_stats.used_bytes,
        .peak_bytes = cache_stats.peak_bytes,
        .insertions = cache_stats.insertions,
        .admission_skips = cache_stats.admission_skips,
        .evictions = cache_stats.evictions,
    };
}

fn dbHbcCacheStatsFromIndex(cache_stats: hbc_mod.HbcCacheStats) db_mod.types.HbcCacheStats {
    return .{
        .total_bytes = cache_stats.total_bytes,
        .accounted_bytes = cache_stats.accounted_bytes,
        .node = dbHbcCacheKindStatsFromIndex(cache_stats.node),
        .quantized = dbHbcCacheKindStatsFromIndex(cache_stats.quantized),
        .vector = dbHbcCacheKindStatsFromIndex(cache_stats.vector),
        .metadata = dbHbcCacheKindStatsFromIndex(cache_stats.metadata),
    };
}

fn dbHbcPostingStatsFromIndex(backlog: hbc_mod.PostingBacklogStats, profile: hbc_mod.WriteProfile) db_mod.types.HbcPostingStats {
    return .{
        .scanned_nodes = backlog.scanned_nodes,
        .scanned_postings = backlog.scanned_postings,
        .dirty_postings = backlog.dirty_postings,
        .centroid_dirty_postings = backlog.centroid_dirty_postings,
        .payload_dirty_postings = backlog.payload_dirty_postings,
        .max_centroid_version_lag = backlog.max_centroid_version_lag,
        .max_payload_version_lag = backlog.max_payload_version_lag,
        .max_mutation_version = backlog.max_mutation_version,
        .skipped_missing = backlog.skipped_missing,
        .maintenance_scanned_nodes = profile.posting_maintenance_scanned_nodes,
        .maintenance_scanned_postings = profile.posting_maintenance_scanned_postings,
        .maintenance_dirty_postings = profile.posting_maintenance_dirty_postings,
        .maintenance_repaired_postings = profile.posting_maintenance_repaired_postings,
        .maintenance_centroid_refreshed = profile.posting_maintenance_centroid_refreshed,
        .maintenance_payload_refreshed = profile.posting_maintenance_payload_refreshed,
        .maintenance_ancestor_refresh_roots = profile.posting_maintenance_ancestor_refresh_roots,
        .maintenance_split_postings = profile.posting_maintenance_split_postings,
        .maintenance_merged_postings = profile.posting_maintenance_merged_postings,
        .maintenance_boundary_reassigned_vectors = profile.posting_maintenance_boundary_reassigned_vectors,
        .lazy_centroid_deferrals = profile.posting_lazy_centroid_deferrals,
        .lazy_payload_deferrals = profile.posting_lazy_payload_deferrals,
        .lazy_ancestor_deferrals = profile.posting_lazy_ancestor_deferrals,
    };
}

fn overlayDenseHbcCacheStatsFromDb(stats: *db_mod.types.DBStats, db: *db_mod.DB) void {
    if (!db.core.tryLockApplyShared()) return;
    defer db.core.unlockApplyShared();

    for (stats.indexes) |*item| {
        if (item.kind != .dense_vector) continue;
        if (db.core.denseIndex(item.name)) |entry| {
            item.hbc_cache = dbHbcCacheStatsFromIndex(entry.index.hbcCacheStats());
        }
    }
}

fn overlayRuntimeStatusReplayTargetFromDb(status: *runtime_status.LocalTableRuntimeStatus, db: *db_mod.DB) void {
    const target_sequence = db.core.nextDerivedSequence();
    const async_stats = db.snapshotAsyncIndexingStats();
    status.stats.async_indexing = async_stats;
    for (status.stats.indexes) |*item| {
        if (target_sequence > item.replay_target_sequence) {
            item.replay_target_sequence = target_sequence;
            item.catch_up_target_sequence = target_sequence;
        }
        if (item.catch_up_target_sequence < item.replay_target_sequence) {
            item.catch_up_target_sequence = item.replay_target_sequence;
        }
        item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
        item.catch_up_applied_sequence = item.replay_applied_sequence;
        item.catch_up_active = item.kind == .dense_vector and async_stats.dense_catch_up.active;
        item.catch_up_phase = if (item.kind == .dense_vector) async_stats.dense_catch_up.phase else .idle;
    }
}

fn startupCatchUpStatsForPhase(
    phase: db_mod.types.StartupCatchUpPhase,
    db: ?*db_mod.DB,
) db_mod.types.StartupCatchUpStats {
    var stats: db_mod.types.StartupCatchUpStats = if (db) |managed_db|
        managed_db.snapshotAsyncIndexingStats().startup
    else
        .{};
    stats.active = phase != .idle;
    stats.phase = phase;
    if (db) |managed_db| {
        const maintenance = managed_db.snapshotLsmMaintenanceStats();
        stats.wal_retention_known = true;
        stats.wal_retained_segments = maintenance.wal_retained_segments;
        stats.wal_retained_bytes = maintenance.wal_retained_bytes;
    }
    return stats;
}

const StartupConfiguredIndex = struct {
    name: []u8 = &.{},
    kind: db_mod.types.IndexKind = .dense_vector,
    algebraic_schema_version: u32 = 0,
    algebraic_capability_fingerprint: ?[]u8 = null,
    algebraic_capability_lifecycle_status: ?[]u8 = null,
    algebraic_capability_change_added_fields: u32 = 0,
    algebraic_capability_change_removed_fields: u32 = 0,
    algebraic_capability_change_changed_type_fields: u32 = 0,
    algebraic_skipped_dynamic_fields: u32 = 0,
    algebraic_skipped_complex_fields: u32 = 0,
    algebraic_skipped_unbounded_fields: u32 = 0,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        if (self.algebraic_capability_fingerprint) |value| alloc.free(value);
        if (self.algebraic_capability_lifecycle_status) |value| alloc.free(value);
        self.* = undefined;
    }

    fn populateStats(self: *const @This(), alloc: std.mem.Allocator, stats: *db_mod.types.DBIndexStats) !void {
        if (self.kind != .algebraic) return;
        stats.algebraic_schema_version = self.algebraic_schema_version;
        stats.algebraic_capability_change_added_fields = self.algebraic_capability_change_added_fields;
        stats.algebraic_capability_change_removed_fields = self.algebraic_capability_change_removed_fields;
        stats.algebraic_capability_change_changed_type_fields = self.algebraic_capability_change_changed_type_fields;
        stats.algebraic_skipped_dynamic_fields = self.algebraic_skipped_dynamic_fields;
        stats.algebraic_skipped_complex_fields = self.algebraic_skipped_complex_fields;
        stats.algebraic_skipped_unbounded_fields = self.algebraic_skipped_unbounded_fields;
        if (self.algebraic_capability_fingerprint) |value| {
            stats.algebraic_capability_fingerprint = try alloc.dupe(u8, value);
        }
        if (self.algebraic_capability_lifecycle_status) |value| {
            stats.algebraic_capability_lifecycle_status = try alloc.dupe(u8, value);
        }
    }
};

const StartupConfiguredIndexes = struct {
    items: []StartupConfiguredIndex,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.items) |*item| item.deinit(alloc);
        alloc.free(self.items);
        self.* = undefined;
    }

    fn populateConfiguredCounts(self: *const @This(), stats: *db_mod.types.StartupCatchUpStats) void {
        for (self.items) |item| incrementStartupConfiguredIndexCounts(stats, item.kind);
    }

    fn accumulateRetention(
        self: *const @This(),
        storage: lsm_backend.Storage,
        alloc: std.mem.Allocator,
        table_path: []const u8,
        stats: *db_mod.types.StartupCatchUpStats,
    ) !void {
        for (self.items) |item| {
            const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ table_path, item.name });
            defer alloc.free(index_path);
            const main_retention = try lsm_backend.wal.snapshotRetention(storage, alloc, index_path);
            const replay_retention = try lsm_backend.wal.snapshotReplayRetention(storage, alloc, index_path);
            stats.wal_retained_segments += main_retention.segments + replay_retention.segments;
            stats.wal_retained_bytes += main_retention.bytes + replay_retention.bytes;
        }
    }
};

fn parseStartupConfiguredIndexes(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) !StartupConfiguredIndexes {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };
    const array_form = object.get("indexes");
    const index_count: usize = if (array_form) |value|
        switch (value) {
            .array => value.array.items.len,
            else => return error.InvalidCreateTableRequest,
        }
    else
        object.count();
    const items = try alloc.alloc(StartupConfiguredIndex, index_count);
    errdefer {
        for (items[0..index_count]) |*item| item.deinit(alloc);
        alloc.free(items);
    }
    @memset(items, .{});
    var initialized: usize = 0;

    if (array_form) |value| {
        const array_items = switch (value) {
            .array => value.array.items,
            else => return error.InvalidCreateTableRequest,
        };
        for (array_items) |item| {
            if (item != .object) return error.InvalidCreateTableRequest;
            const name_value = item.object.get("name") orelse return error.InvalidCreateTableRequest;
            if (name_value != .string) return error.InvalidCreateTableRequest;
            const kind = try parseIndexKind(item);
            var configured = StartupConfiguredIndex{
                .name = try alloc.dupe(u8, name_value.string),
                .kind = kind,
            };
            errdefer configured.deinit(alloc);
            try populateStartupAlgebraicCapability(alloc, &configured, item);
            items[initialized] = configured;
            initialized += 1;
        }
        return .{ .items = items };
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const kind = try parseIndexKind(entry.value_ptr.*);
        var configured = StartupConfiguredIndex{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .kind = kind,
        };
        errdefer configured.deinit(alloc);
        try populateStartupAlgebraicCapability(alloc, &configured, entry.value_ptr.*);
        items[initialized] = configured;
        initialized += 1;
    }
    return .{ .items = items };
}

fn startupAlgebraicField(value: std.json.Value, field: []const u8) ?std.json.Value {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get(field)) |direct| return direct;
    const config = object.get("config") orelse return null;
    const config_object = switch (config) {
        .object => |config_object| config_object,
        else => return null,
    };
    return config_object.get(field);
}

fn startupAlgebraicString(value: std.json.Value, field: []const u8) ?[]const u8 {
    const field_value = startupAlgebraicField(value, field) orelse return null;
    return switch (field_value) {
        .string => |string| string,
        else => null,
    };
}

fn startupAlgebraicU32(value: std.json.Value, field: []const u8) ?u32 {
    const field_value = startupAlgebraicField(value, field) orelse return null;
    return switch (field_value) {
        .integer => |integer| if (integer >= 0 and integer <= std.math.maxInt(u32)) @intCast(integer) else null,
        else => null,
    };
}

fn populateStartupAlgebraicCapability(
    alloc: std.mem.Allocator,
    item: *StartupConfiguredIndex,
    value: std.json.Value,
) !void {
    if (item.kind != .algebraic) return;
    item.algebraic_schema_version = startupAlgebraicU32(value, "schema_version") orelse 0;
    item.algebraic_capability_change_added_fields = startupAlgebraicU32(value, "capability_change_added_fields") orelse 0;
    item.algebraic_capability_change_removed_fields = startupAlgebraicU32(value, "capability_change_removed_fields") orelse 0;
    item.algebraic_capability_change_changed_type_fields = startupAlgebraicU32(value, "capability_change_changed_type_fields") orelse 0;
    item.algebraic_skipped_dynamic_fields = startupAlgebraicU32(value, "skipped_dynamic_fields") orelse 0;
    item.algebraic_skipped_complex_fields = startupAlgebraicU32(value, "skipped_complex_fields") orelse 0;
    item.algebraic_skipped_unbounded_fields = startupAlgebraicU32(value, "skipped_unbounded_fields") orelse 0;
    if (startupAlgebraicString(value, "capability_fingerprint")) |fingerprint| {
        if (fingerprint.len > 0) item.algebraic_capability_fingerprint = try alloc.dupe(u8, fingerprint);
    }
    if (startupAlgebraicString(value, "capability_lifecycle_status")) |status| {
        if (status.len > 0) item.algebraic_capability_lifecycle_status = try alloc.dupe(u8, status);
    }
}

fn startupCatchUpStatsForPath(
    path: []const u8,
    phase: db_mod.types.StartupCatchUpPhase,
    configured_indexes: ?*const StartupConfiguredIndexes,
) !db_mod.types.StartupCatchUpStats {
    var stats: db_mod.types.StartupCatchUpStats = .{
        .active = phase != .idle,
        .phase = phase,
        .wal_retention_known = phase != .idle,
    };
    if (phase == .idle) return stats;

    var native = try lsm_backend.storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();

    const main_retention = try lsm_backend.wal.snapshotRetention(native.storage(), std.heap.page_allocator, path);
    const replay_retention = try lsm_backend.wal.snapshotReplayRetention(native.storage(), std.heap.page_allocator, path);
    stats.wal_retained_segments = main_retention.segments + replay_retention.segments;
    stats.wal_retained_bytes = main_retention.bytes + replay_retention.bytes;
    if (configured_indexes) |summary| {
        summary.populateConfiguredCounts(&stats);
        try summary.accumulateRetention(native.storage(), std.heap.page_allocator, path, &stats);
    }
    return stats;
}

fn applyStartupCatchUpAsyncOverlay(
    status: *runtime_status.LocalTableRuntimeStatus,
    async_stats: db_mod.types.AsyncIndexingStats,
    startup: db_mod.types.StartupCatchUpStats,
) void {
    status.stats.async_indexing = async_stats;
    var merged_startup = status.stats.async_indexing.startup;
    db_mod.types.accumulateStartupCatchUpStats(&merged_startup, startup);
    status.stats.async_indexing.startup = merged_startup;
}

fn incrementStartupConfiguredIndexCounts(
    stats: *db_mod.types.StartupCatchUpStats,
    kind: db_mod.types.IndexKind,
) void {
    stats.configured_indexes += 1;
    switch (kind) {
        .dense_vector => stats.configured_dense_indexes += 1,
        .sparse_vector => stats.configured_sparse_indexes += 1,
        .full_text => stats.configured_full_text_indexes += 1,
        .graph => stats.configured_graph_indexes += 1,
        .algebraic => {},
    }
}

fn publishStartupCatchUpRuntimeStatusSnapshot(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    startup: db_mod.types.StartupCatchUpStats,
    db: ?*db_mod.DB,
    configured_indexes: ?*const StartupConfiguredIndexes,
) !void {
    const snapshot_cache = source.runtime_status_cache orelse return;
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = group_id,
        .stats = .{},
    };
    var status_initialized = false;
    defer {
        if (status_initialized) {
            var owned = status;
            owned.deinit(alloc);
        }
    }

    if (startup.active) {
        if (db) |managed_db| {
            status = .{
                .group_id = group_id,
                .stats = try managed_db.runtimeStatusStatsConsistent(alloc),
            };
            status_initialized = true;
            var merged_startup = startup;
            if (!merged_startup.wal_retention_known) {
                const cached_startup = try cachedStartupCatchUpStats(snapshot_cache, alloc, table_name, group_id);
                merged_startup.wal_retention_known = cached_startup.wal_retention_known;
                merged_startup.wal_retained_segments = cached_startup.wal_retained_segments;
                merged_startup.wal_retained_bytes = cached_startup.wal_retained_bytes;
            }
            applyStartupCatchUpAsyncOverlay(&status, managed_db.snapshotAsyncIndexingStats(), merged_startup);
        } else if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |owned_status| {
            status = owned_status;
            status_initialized = true;
            var merged_startup = startup;
            if (!merged_startup.wal_retention_known) {
                const cached_startup = status.stats.async_indexing.startup;
                merged_startup.wal_retention_known = cached_startup.wal_retention_known;
                merged_startup.wal_retained_segments = cached_startup.wal_retained_segments;
                merged_startup.wal_retained_bytes = cached_startup.wal_retained_bytes;
            }
            var merged_existing = status.stats.async_indexing.startup;
            db_mod.types.accumulateStartupCatchUpStats(&merged_existing, merged_startup);
            status.stats.async_indexing.startup = merged_existing;
        } else if (configured_indexes) |summary| {
            status = try syntheticStartupRuntimeStatusFromConfiguredIndexes(alloc, group_id, summary, startup);
            status_initialized = true;
        }
    } else if (db) |managed_db| {
        status = .{
            .group_id = group_id,
            .stats = try managed_db.runtimeStatusStatsConsistent(alloc),
        };
        applyStartupCatchUpAsyncOverlay(&status, managed_db.snapshotAsyncIndexingStats(), startup);
        status_initialized = true;
    } else if (try snapshot_cache.snapshot(alloc, table_name)) |owned_statuses| {
        var statuses = owned_statuses;
        defer statuses.deinit(alloc);
        for (statuses.items) |item| {
            if (item.group_id != group_id) continue;
            status = try item.clone(alloc);
            status_initialized = true;
            break;
        }
    }

    if (!status_initialized) return;

    status.group_id = group_id;
    if (!startup.active and db == null) {
        var merged_startup = status.stats.async_indexing.startup;
        db_mod.types.accumulateStartupCatchUpStats(&merged_startup, startup);
        status.stats.async_indexing.startup = merged_startup;
    }
    try snapshot_cache.upsertGroupStatus(table_name, status);
}

fn syntheticStartupRuntimeStatusFromConfiguredIndexes(
    alloc: std.mem.Allocator,
    group_id: u64,
    configured_indexes: *const StartupConfiguredIndexes,
    startup: db_mod.types.StartupCatchUpStats,
) !runtime_status.LocalTableRuntimeStatus {
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, configured_indexes.items.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |index| freeSyntheticStartupIndexStatsItem(alloc, index);
        alloc.free(indexes);
    }

    for (configured_indexes.items) |item| {
        var stats = db_mod.types.DBIndexStats{
            .name = try alloc.dupe(u8, item.name),
            .kind = item.kind,
        };
        errdefer freeSyntheticStartupIndexStatsItem(alloc, stats);
        try item.populateStats(alloc, &stats);
        indexes[initialized] = stats;
        initialized += 1;
    }

    return .{
        .group_id = group_id,
        .stats = .{
            .index_count = @intCast(indexes.len),
            .indexes = indexes,
            .async_indexing = .{ .startup = startup },
        },
    };
}

fn freeSyntheticStartupIndexStatsItem(alloc: std.mem.Allocator, item: db_mod.types.DBIndexStats) void {
    alloc.free(item.name);
    if (item.algebraic_capability_fingerprint) |value| alloc.free(value);
    if (item.algebraic_capability_lifecycle_status) |value| alloc.free(value);
}

fn catchUpManagedDb(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    db: *db_mod.DB,
) !ProvisionedTableWriteSource.StartupCatchUpResult {
    const before = db.listDerivedReplayDebt(alloc) catch |err| {
        std.log.warn("managed startup catch-up list debt failed table={s} err={}", .{ table_name, err });
        return err;
    };
    defer {
        for (before) |*status| status.deinit(alloc);
        alloc.free(before);
    }

    var had_debt = false;
    for (before) |status| {
        if (!status.catch_up_required) continue;
        had_debt = true;
        break;
    }
    const ProgressCtx = struct {
        source: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        db: *db_mod.DB,
        phase: db_mod.types.StartupCatchUpPhase,

        fn run(ptr: *anyopaque, _: []const u8, _: db_mod.ReplayProgress) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            try publishRuntimeStatusSnapshotWithStartupPhase(ctx.source, ctx.alloc, ctx.table_name, ctx.group_id, ctx.phase, ctx.db);
        }
    };

    var progress_ctx = ProgressCtx{
        .source = source,
        .alloc = alloc,
        .group_id = group_id,
        .table_name = table_name,
        .db = db,
        .phase = .startup_catch_up,
    };

    const restore_repair_needed = db.restoreRuntimeRepairNeeded() catch |err| {
        std.log.warn("managed startup catch-up restore repair probe failed table={s} err={}", .{ table_name, err });
        return err;
    };
    const needs_dense_artifact_rebuild = db.hasPendingDenseArtifactRebuild(alloc) catch |err| {
        std.log.warn("managed startup catch-up dense rebuild probe failed table={s} err={}", .{ table_name, err });
        return err;
    };

    var repaired_restore_runtime = false;
    var repaired_dense_artifacts: usize = 0;
    if (restore_repair_needed) {
        std.log.info("managed restore repair begin table={s} group_id={d}", .{ table_name, group_id });
        progress_ctx.phase = .artifact_rebuild;
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
        repaired_restore_runtime = db.repairRestoreRuntimeStateStepIfNeeded(alloc) catch |err| {
            std.log.warn("managed startup catch-up restore repair failed table={s} err={}", .{ table_name, err });
            return err;
        };
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
        std.log.info("managed restore repair step complete table={s} group_id={d} repaired={}", .{ table_name, group_id, repaired_restore_runtime });
    } else if (had_debt) {
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .startup_catch_up, db);
        db.catchUpPendingDerivedReplayWithProgress(&progress_ctx, ProgressCtx.run) catch |err| {
            std.log.warn("managed startup catch-up replay failed table={s} err={}", .{ table_name, err });
            if (err == error.WriterLocked or err == error.ReplayDocumentNotVisible) {
                return .{
                    .had_debt = true,
                    .cleared_debt = false,
                    .busy = true,
                };
            }
            return err;
        };
        db.runUntilIdle() catch |err| {
            std.log.warn("managed startup catch-up replay idle drain failed table={s} err={}", .{ table_name, err });
            if (err == error.WriterLocked or err == error.ReplayDocumentNotVisible) {
                return .{
                    .had_debt = true,
                    .cleared_debt = false,
                    .busy = true,
                };
            }
            return err;
        };
        try db.core.index_manager.syncAll(false);
    }

    if (!had_debt and !restore_repair_needed and !needs_dense_artifact_rebuild) {
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .idle, db);
        return .{};
    }

    if (!restore_repair_needed and needs_dense_artifact_rebuild) {
        progress_ctx.phase = .artifact_rebuild;
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
        repaired_dense_artifacts = db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, &progress_ctx, ProgressCtx.run) catch |err| {
            std.log.warn("managed startup catch-up dense rebuild failed table={s} err={}", .{ table_name, err });
            return err;
        };
        if (repaired_dense_artifacts > 0) {
            db.runUntilIdle() catch |err| {
                std.log.warn("managed startup catch-up dense rebuild idle drain failed table={s} err={}", .{ table_name, err });
                return err;
            };
            try db.core.index_manager.syncAll(false);
        }
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
    }

    if (!had_debt and !repaired_restore_runtime and repaired_dense_artifacts == 0) {
        return .{};
    }

    if (repaired_restore_runtime) db.clearDenseHbcCaches();

    // Startup catch-up mutates on-disk index/runtime state through an isolated
    // DB instance. Drop shared cached handles for this table so future reads
    // and writer-side status probes reopen against the updated state. Do not
    // invalidate the startup cache here because `db` may still alias its live
    // entry until the caller's deferred startup-cache clear runs.
    source.invalidateReadCache(table_name);
    source.invalidateWriteCacheForTable(table_name);
    source.clearDirtyWriteTable(table_name);

    const after = db.listDerivedReplayDebt(alloc) catch |err| {
        std.log.warn("managed startup catch-up post-check debt failed table={s} err={}", .{ table_name, err });
        return err;
    };
    defer {
        for (after) |*status| status.deinit(alloc);
        alloc.free(after);
    }

    for (after) |status| {
        if (status.catch_up_required) {
            return .{
                .had_debt = had_debt or restore_repair_needed,
                .cleared_debt = false,
            };
        }
    }
    if (db.hasPendingDenseArtifactRebuild(alloc) catch |err| {
        std.log.warn("managed startup catch-up post-check dense rebuild probe failed table={s} err={}", .{ table_name, err });
        return err;
    }) {
        return .{
            .had_debt = had_debt or restore_repair_needed,
            .cleared_debt = false,
        };
    }
    if (db.restoreRuntimeRepairNeeded() catch |err| {
        std.log.warn("managed startup catch-up post-check restore repair probe failed table={s} err={}", .{ table_name, err });
        return err;
    }) {
        return .{
            .had_debt = had_debt or restore_repair_needed,
            .cleared_debt = false,
        };
    }
    return .{
        .had_debt = had_debt or restore_repair_needed,
        .cleared_debt = had_debt or repaired_restore_runtime,
    };
}

fn shouldDrainManagedDbAfterBatch(sync_level: db_mod.types.SyncLevel) bool {
    // Request latency for weak sync levels must not depend on derived replay.
    // Pending replay is durable in the journal and is resumed by later writes,
    // explicit catch-up, or bulk-session finish.
    return switch (sync_level) {
        .propose, .write, .enrichments => false,
        .full_text, .aknn, .full_index => false,
    };
}

fn shouldDrainCachedManagedDbAfterBatch(sync_level: db_mod.types.SyncLevel) bool {
    _ = sync_level;
    return false;
}

fn autoBulkIngestBatchOps(req: db_mod.types.BatchRequest) usize {
    _ = req;
    // Weak-sync writes are already durable in the primary store plus replay
    // journal. Opening a foreground HBC bulk session here suppresses dense
    // replay notifications for the entire active upload, so indexing only
    // becomes query-visible after the writer goes idle. Let the background
    // derived executor own dense bulk sessions and publish bounded windows.
    return 0;
}

fn autoBulkIngestGroupBatchOps(group: GroupBatch, sync_level: db_mod.types.SyncLevel) usize {
    _ = group;
    _ = sync_level;
    return 0;
}

test "weak sync levels do not drain managed db after batch" {
    try std.testing.expect(!shouldDrainManagedDbAfterBatch(.propose));
    try std.testing.expect(!shouldDrainManagedDbAfterBatch(.write));
}

fn prepareLocalTablePathForRestore(alloc: std.mem.Allocator, path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    try fs_paths.createDirPathPortable(io, path);

    const indexes_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{path});
    defer alloc.free(indexes_path);
    std.Io.Dir.cwd().deleteTree(io, indexes_path) catch {};

    const snapshots_path = try std.fmt.allocPrint(alloc, "{s}.snapshots", .{path});
    defer alloc.free(snapshots_path);
    std.Io.Dir.cwd().deleteTree(io, snapshots_path) catch {};
}

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn recoverProvisionedTransactionsOnce(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
) !void {
    var worker_impl = distributed_txn.LocalTableWriteParticipantWorker.init(self.source());
    var resolver = distributed_txn.RecoveryResolver{
        .alloc = alloc,
        .worker = worker_impl.worker(),
        .owner_id = "api-provisioned",
        .lease_owned = true,
    };
    _ = try db.runTransactionRecoveryOnce(resolver.config());
}

fn recoverHostedTransactionsOnce(
    self: *HostedProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
) !void {
    var worker_impl = distributed_txn.HostedParticipantWorker.init(self.catalog, self.router, self.source(), self.executor);
    var resolver = distributed_txn.RecoveryResolver{
        .alloc = alloc,
        .worker = worker_impl.worker(),
        .owner_id = "api-hosted",
        .lease_owned = true,
    };
    _ = try db.runTransactionRecoveryOnce(resolver.config());
}

fn loadLocalTableSchemaJson(alloc: std.mem.Allocator, db: *db_mod.DB) !?[]u8 {
    return db.core.store.get(alloc, local_schema_json_key) catch |err| switch (err) {
        lmdb.Error.NotFound => null,
        else => return err,
    };
}

fn validateTableWritesAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: anytype,
) !void {
    if (writes.len == 0) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, writes);
}

fn freeOwnedBatchWrites(alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(@constCast(writes));
}

fn removeOwnedBatchWriteByKey(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(db_mod.types.BatchWrite),
    key: []const u8,
) void {
    var i: usize = 0;
    while (i < list.items.len) {
        if (!std.mem.eql(u8, list.items[i].key, key)) {
            i += 1;
            continue;
        }
        alloc.free(@constCast(list.items[i].key));
        alloc.free(@constCast(list.items[i].value));
        _ = list.swapRemove(i);
    }
}

fn freeBackupShards(alloc: std.mem.Allocator, shards: []const backups_api.ShardSnapshot) void {
    for (shards) |shard| shard.deinit(alloc);
    alloc.free(@constCast(shards));
}

fn resolveWritesForSchemaValidation(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    base_writes: []const db_mod.types.BatchWrite,
    transforms: []const db_mod.types.DocumentTransform,
) ![]db_mod.types.BatchWrite {
    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer writes.deinit(alloc);

    for (base_writes) |write| {
        try writes.append(alloc, .{
            .key = try alloc.dupe(u8, write.key),
            .value = try alloc.dupe(u8, write.value),
        });
    }

    for (transforms) |transform| {
        const existing = try db.get(alloc, transform.key);
        defer if (existing) |body| alloc.free(body);
        const resolved = db_mod.transform.resolveDocumentTransform(alloc, existing, transform) catch |err| switch (err) {
            error.InvalidArgument => return error.InvalidBatchRequest,
            else => return err,
        } orelse continue;
        errdefer alloc.free(resolved);

        removeOwnedBatchWriteByKey(alloc, &writes, transform.key);
        try writes.append(alloc, .{
            .key = try alloc.dupe(u8, transform.key),
            .value = resolved,
        });
    }

    return try writes.toOwnedSlice(alloc);
}

fn transactionWritesToBatchWrites(
    alloc: std.mem.Allocator,
    writes: []const db_mod.types.TransactionWrite,
) ![]db_mod.types.BatchWrite {
    var out = try alloc.alloc(db_mod.types.BatchWrite, writes.len);
    for (writes, 0..) |write, i| {
        out[i] = .{
            .key = write.key,
            .value = write.value,
        };
    }
    return out;
}

fn validateTableBatchAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: []const db_mod.types.BatchWrite,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and transforms.len == 0) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    const effective_writes = try resolveWritesForSchemaValidation(alloc, db, writes, transforms);
    defer freeOwnedBatchWrites(alloc, effective_writes);
    if (effective_writes.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, effective_writes);
}

fn validateTransactionAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: []const db_mod.types.TransactionWrite,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    const batch_writes = try transactionWritesToBatchWrites(alloc, writes);
    defer alloc.free(batch_writes);
    try validateTableBatchAgainstLocalSchema(alloc, db, batch_writes, transforms);
}

fn applyLocalTableSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: []const u8,
) !void {
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);

    const runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);

    try db.setSchema(runtime_schema);
    try db.core.store.put(local_schema_json_key, schema_json);
}

fn rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len == 0) "{}" else indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    var it = root.iterator();
    while (it.next()) |entry| {
        const index_name = entry.key_ptr.*;
        if (!std.mem.startsWith(u8, index_name, "full_text_index_v")) continue;
        if ((try parseIndexKind(entry.value_ptr.*)) != .full_text) continue;
        const current = findDbIndexStats(stats.indexes, index_name) orelse continue;
        if (current.doc_count != 0) continue;

        _ = try db.deleteIndex(index_name);
        const config_json = try extractIndexConfigJson(alloc, index_name, entry.value_ptr.*);
        defer alloc.free(config_json);
        try db.addIndex(.{
            .name = index_name,
            .kind = .full_text,
            .config_json = config_json,
        });
    }
}

fn findDbIndexStats(indexes: []const db_mod.types.DBIndexStats, index_name: []const u8) ?db_mod.types.DBIndexStats {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) return index;
    }
    return null;
}

fn loadTableIndexesJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.indexes_json);
}

fn loadTableManagedMetadata(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?struct { indexes_json: ?[]u8, schema_json: ?[]u8 } {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    const indexes_json = try alloc.dupe(u8, table.indexes_json);
    errdefer alloc.free(indexes_json);
    const schema_json = try alloc.dupe(u8, table.schema_json);
    return .{
        .indexes_json = indexes_json,
        .schema_json = schema_json,
    };
}

fn loadTableIdentityNamespaceForGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
) !?doc_identity.Namespace {
    _ = alloc;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    for (snapshot.ranges) |range| {
        if (range.table_id != table.table_id or range.group_id != group_id) continue;
        return .{
            .table_id = table.table_id,
            .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
            .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
        };
    }
    return null;
}

fn validateProvisionedDbIdentityNamespaceExpected(expected: ?doc_identity.Namespace, db: *const db_mod.DB) !void {
    const namespace = expected orelse return;
    if (!db.core.identity_namespace.eql(namespace)) return error.DocIdentityNamespaceMismatch;
}

fn validateProvisionedDbIdentityNamespace(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    db: *const db_mod.DB,
) !void {
    const expected = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    try validateProvisionedDbIdentityNamespaceExpected(expected, db);
}

fn loadTableSchemaJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.schema_json);
}

fn validateTableWritesAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    writes: anytype,
) !void {
    if (writes.len == 0) return;
    const schema_json = (try loadTableSchemaJson(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, writes);
}

fn validateTableBatchAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    writes: []const db_mod.types.BatchWrite,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and transforms.len == 0) return;
    const schema_json = (try loadTableSchemaJson(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    try validateTableBatchAgainstSchemaJson(alloc, db, schema_json, writes, transforms);
}

fn validateTableBatchAgainstSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: ?[]const u8,
    writes: []const db_mod.types.BatchWrite,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and transforms.len == 0) return;
    const raw_schema_json = schema_json orelse return;
    if (raw_schema_json.len == 0) return;

    const effective_writes = try resolveWritesForSchemaValidation(alloc, db, writes, transforms);
    defer freeOwnedBatchWrites(alloc, effective_writes);
    if (effective_writes.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, raw_schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, effective_writes);
}

fn validateTransactionAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    const batch_writes = try transactionWritesToBatchWrites(alloc, writes);
    defer alloc.free(batch_writes);
    try validateTableBatchAgainstCatalogSchema(alloc, catalog, db, table_name, batch_writes, transforms);
}

fn encodeRemoteBatchRequest(alloc: std.mem.Allocator, req: db_mod.types.BatchRequest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"inserts\":{");
    for (req.writes, 0..) |write, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, write.key);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, write.value);
    }
    try out.append(alloc, '}');
    if (req.deletes.len > 0) {
        try out.appendSlice(alloc, ",\"deletes\":[");
        for (req.deletes, 0..) |key, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, key);
        }
        try out.append(alloc, ']');
    }
    if (req.transforms.len > 0) {
        try out.appendSlice(alloc, ",\"transforms\":[");
        for (req.transforms, 0..) |transform, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"key\":");
            try appendJsonString(alloc, &out, transform.key);
            try out.appendSlice(alloc, ",\"operations\":[");
            for (transform.operations, 0..) |op, op_index| {
                if (op_index > 0) try out.append(alloc, ',');
                try out.appendSlice(alloc, "{\"op\":");
                try appendJsonString(alloc, &out, db_mod.transform.transformOpText(op.op));
                try out.appendSlice(alloc, ",\"path\":");
                try appendJsonString(alloc, &out, op.path);
                if (op.value_json) |value_json| {
                    try out.appendSlice(alloc, ",\"value\":");
                    try out.appendSlice(alloc, value_json);
                }
                try out.append(alloc, '}');
            }
            try out.append(alloc, ']');
            if (transform.upsert) try out.appendSlice(alloc, ",\"upsert\":true");
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

test "bound table write source applies batch writes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "bound table write source resolves internal group transactions into visible documents" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-txn-group-local";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");

    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    _ = try source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
    _ = try source.source().txnResolveGroupLocal(alloc, 7, "docs", txn_id, .committed, 10_001);

    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, (try source.source().txnStatusGroupLocal(alloc, 7, "docs", txn_id)).?);

    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "bound table write source provisions default full text index on create" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-create";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{};
    defer req.deinit(alloc);
    req.indexes_json = try alloc.dupe(u8, tables_api.default_indexes_json);

    _ = try source.source().createTable(alloc, "docs", req);
    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);
}

test "provisioned table write source create table clears stale local group state" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-create-table-clears-stale-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var stale_db = try db_mod.DB.open(alloc, db_path, .{});
        defer stale_db.close();
        try stale_db.batch(.{
            .writes = &.{.{ .key = "doc:stale", .value = "{\"title\":\"stale\"}" }},
        });
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const stale_change_journal_dir = try std.fmt.allocPrint(alloc, "{s}/change_journal", .{db_path});
    defer alloc.free(stale_change_journal_dir);
    try fs_paths.createDirPathPortable(io_impl.io(), stale_change_journal_dir);
    const stale_marker_path = try std.fmt.allocPrint(alloc, "{s}/stale.marker", .{stale_change_journal_dir});
    defer alloc.free(stale_marker_path);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{ .sub_path = stale_marker_path, .data = "stale" });

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});

    var recreated_db = try db_mod.DB.open(alloc, db_path, .{});
    defer recreated_db.close();
    try std.testing.expect((try recreated_db.lookup(alloc, "doc:stale", .{})) == null);
    try std.testing.expect(recreated_db.core.index_manager.textIndex("full_text_index_v0") != null);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), stale_marker_path, .{}));
}

test "provisioned table write source seeds doc identity namespace from table range" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-identity-namespace-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
        defer source.deinit();
        _ = try source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        });
    }

    var db = try db_mod.DB.open(alloc, db_path, .{ .start_index_workers = false });
    defer db.close();
    try std.testing.expect(db.core.identity_namespace.eql(.{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7101,
    }));
}

test "provisioned table write source rejects stale doc identity namespace before write" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const stale_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 9999,
    };

    const setupStaleDb = struct {
        fn run(allocator: std.mem.Allocator, path: []const u8, namespace: doc_identity.Namespace) !void {
            var db = try db_mod.DB.open(allocator, path, .{ .identity_namespace = namespace });
            defer db.close();
            try db.batch(.{
                .writes = &.{.{ .key = "doc:stale", .value = "{\"title\":\"stale\"}" }},
            });
        }
    }.run;

    const uncached_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-stale-identity-uncached-root", .{tmp.sub_path});
    defer alloc.free(uncached_root);
    const uncached_db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, uncached_root, 7001);
    defer alloc.free(uncached_db_path);
    try setupStaleDb(alloc, uncached_db_path, stale_namespace);

    {
        var source = ProvisionedTableWriteSource.init(uncached_root, Catalog.iface());
        defer source.deinit();
        try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        }));
    }

    var uncached_db = try db_mod.DB.open(alloc, uncached_db_path, .{ .start_index_workers = false });
    defer uncached_db.close();
    try std.testing.expect((try uncached_db.lookup(alloc, "doc:b", .{})) == null);

    const cached_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-stale-identity-cached-root", .{tmp.sub_path});
    defer alloc.free(cached_root);
    const cached_db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, cached_root, 7001);
    defer alloc.free(cached_db_path);
    try setupStaleDb(alloc, cached_db_path, stale_namespace);

    {
        var source = ProvisionedTableWriteSource.init(cached_root, Catalog.iface());
        defer source.deinit();
        var write_cache = ProvisionedTableWriteCache.init(alloc);
        defer write_cache.deinit();
        source.write_cache = &write_cache;
        try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        }));
    }

    var cached_db = try db_mod.DB.open(alloc, cached_db_path, .{ .start_index_workers = false });
    defer cached_db.close();
    try std.testing.expect((try cached_db.lookup(alloc, "doc:b", .{})) == null);
}

test "bound table write source rejects invalid batch writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    try std.testing.expect(db.core.schema != null);
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, storage_schema.resolveFieldType(db.core.schema.?, "meta_status").?.field_type);
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "bound table write source enforces nested required fields and array items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nested-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"required\":[\"author\",\"tags\"],\"properties\":{\"author\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"active\":{\"type\":\"boolean\"}}},\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"keyword\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"author\":{\"name\":\"ann\",\"active\":true},\"tags\":[\"a\",\"b\"]}" }},
    });

    var written = (try db.lookup(alloc, "doc:good", .{})).?;
    defer written.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, written.json, "\"name\":\"ann\"") != null);

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing", .value = "{\"author\":{\"active\":true},\"tags\":[\"a\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-tag", .value = "{\"author\":{\"name\":\"ann\"},\"tags\":[1]}" }},
    }));
}

test "bound table write source enforces enums numeric bounds and anyOf" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-enum-bounds-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\",\"enum\":[\"draft\",\"published\"]},\"score\":{\"type\":\"numeric\",\"minimum\":0,\"maximum\":10},\"metric\":{\"anyOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"keyword\",\"enum\":[\"n/a\"]}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"n/a\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-status", .value = "{\"status\":\"archived\",\"score\":8,\"metric\":\"n/a\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-score", .value = "{\"status\":\"draft\",\"score\":11,\"metric\":\"n/a\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-metric", .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"bad\"}" }},
    }));
}

test "bound table write source enforces oneOf allOf pattern and item cardinality" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-pattern-compose-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"keyword\",\"pattern\":\"^[A-Z]{3}-[0-9]{2}$\"},\"tags\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":2,\"items\":{\"type\":\"keyword\"}},\"code\":{\"oneOf\":[{\"type\":\"keyword\",\"enum\":[\"A\"]},{\"type\":\"keyword\",\"enum\":[\"B\"]}]},\"score\":{\"allOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"numeric\",\"maximum\":5}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern", .value = "{\"sku\":\"bad\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-min-items", .value = "{\"sku\":\"ABC-12\",\"tags\":[],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-max-items", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\",\"y\",\"z\"],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-oneof", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"C\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-allof", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":8}" }},
    }));
}

test "bound table write source enforces string length and object cardinality" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-length-cardinality-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"minProperties\":2,\"maxProperties\":3,\"properties\":{\"title\":{\"type\":\"text\",\"minLength\":3,\"maxLength\":5},\"meta\":{\"type\":\"object\",\"minProperties\":1,\"maxProperties\":2,\"properties\":{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"keyword\"},\"c\":{\"type\":\"keyword\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-min-length", .value = "{\"title\":\"hi\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-max-length", .value = "{\"title\":\"alphabet\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-cardinality", .value = "{\"title\":\"alpha\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-object-cardinality", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\",\"c\":\"z\"}}" }},
    }));
}

test "bound table write source backs up and restores a local table" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-backup-restore";
    const backup_root = "/tmp/antfly-api-table-backup-restore-out";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    var source = BoundTableWriteSource.init("docs", &db);
    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 1,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    db.close();
    db = try db_mod.DB.open(alloc, path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "provisioned table write source backs up and restores a local table" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-table-backup-restore";
    const backup_root = "/tmp/antfly-api-provisioned-table-backup-restore-out";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "provisioned table restore rejects mismatched doc identity namespace" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-table-backup-restore-docid-mismatch";
    const backup_root = "/tmp/antfly-api-provisioned-table-backup-restore-docid-mismatch-out";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const source_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 97001 };
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = source_namespace,
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .timestamp_ns = 1,
        });
        _ = try db.snapshot("snap1-local");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1-local", .{db_path});
    defer alloc.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(alloc, backup_root, "snap1", 7001);
    defer alloc.free(dest_root);
    try backups_api.copyDirectoryRecursive(alloc, snapshot_root, dest_root);

    const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
    shards[0] = .{
        .group_id = 7001,
        .start_key = try alloc.dupe(u8, ""),
        .end_key = null,
        .snapshot_path = try backups_api.shardSnapshotRelPath(alloc, "snap1", 7001),
    };
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                    .range_id = 7001,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    try std.testing.expectError(error.IdentityNamespaceMismatch, source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    }));
}

test "provisioned table write source backs up and restores full_text writes from the write cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-cache-backup-restore";
    const backup_root = "/tmp/antfly-api-provisioned-write-cache-backup-restore-out";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"content\":\"distributed consensus\"}" }},
        .timestamp_ns = 1,
        .sync_level = .full_text,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\",\"content\":\"vector search\"}" }},
        .timestamp_ns = 2,
        .sync_level = .full_text,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    var read_source = table_reads.ProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );

    var restored_lookup = (try read_source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer restored_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored_lookup.json, "\"alpha\"") != null);

    var restored_scan = (try read_source.source().scan(alloc, "docs", "", "", .{
        .limit = 10,
        .include_documents = true,
    }, .read_index)).?;
    defer restored_scan.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored_scan.ndjson, "\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, restored_scan.ndjson, "\"alpha\"") != null);

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "provisioned read preparation invalidates readers without closing dirty writer cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-cache-read-prep";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("other", .general);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    {
        var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default, null, null);
        defer cached.deinit(alloc);
    }
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    const large_writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (large_writes) |write| alloc.free(@constCast(write.key));
        alloc.free(large_writes);
    }
    for (large_writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk:{d}", .{i}),
            .value = "{\"title\":\"bulk\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = large_writes,
        .timestamp_ns = 2,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].*.auto_bulk_ingest_ops);

    const next_writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (next_writes) |write| alloc.free(@constCast(write.key));
        alloc.free(next_writes);
    }
    for (next_writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-next:{d}", .{i}),
            .value = "{\"title\":\"bulk next\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = next_writes,
        .timestamp_ns = 3,
        .sync_level = .write,
    });

    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].*.auto_bulk_ingest_ops);

    source.readPreparation().prepareForRead("docs", .general);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "auto bulk best-effort finish does not spin when writer cache lock is busy" {
    var source = ProvisionedTableWriteSource.init(
        "/tmp/unused-antfly-auto-bulk-finish-busy",
        table_catalog.emptyCatalogSource(),
    );

    try std.testing.expectEqual(@as(?bool, false), source.tryFinishExpiredAutoBulkIngest());

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(?bool, null), source.tryFinishExpiredAutoBulkIngest());
    try std.testing.expect(!source.finishExpiredAutoBulkIngestBestEffort());
}

test "auto bulk max-window request waits for idle finish" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-auto-bulk-roll-without-next-write";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-roll:{d}", .{i}),
            .value = "{\"title\":\"bulk roll\"}",
        };
    }

    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_session_open);

    const requested_finish_ns = write_cache.entries.items[0].*.auto_bulk_ingest_last_ns + 1;
    try write_cache.recordAutoBulkIngestOpsLocked(7001, "docs", auto_bulk_ingest_max_window_ops, requested_finish_ns);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);

    try std.testing.expect(!try write_cache.finishExpiredAutoBulkIngestLocked(requested_finish_ns));
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_ops >= auto_bulk_ingest_max_window_ops);

    const idle_finish_ns = write_cache.entries.items[0].*.auto_bulk_ingest_last_ns + auto_bulk_ingest_max_idle_ns;
    try std.testing.expect(try write_cache.finishExpiredAutoBulkIngestLocked(idle_finish_ns));
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.active_bulk_ingest_sessions.items.len);
}

test "auto bulk background finish skips entries with active foreground leases" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-auto-bulk-active-lease-skip", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:auto-bulk-active-lease:{d}", .{i}),
            .value = "{\"title\":\"active lease\"}",
        };
    }

    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    const entry = write_cache.entries.items[0];
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try write_cache.recordAutoBulkIngestOpsLocked(7001, "docs", auto_bulk_ingest_max_window_ops, platform_time.monotonicNs());
    try std.testing.expect(entry.auto_bulk_ingest_finish_requested);

    entry.active_leases += 1;
    const skipped = try write_cache.finishExpiredAutoBulkIngestLocked(platform_time.monotonicNs());
    try std.testing.expect(!skipped);
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try std.testing.expect(entry.auto_bulk_ingest_finish_requested);

    const foreground_roll = try write_cache.rollRequestedAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(foreground_roll);
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try std.testing.expect(!entry.auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), entry.auto_bulk_ingest_ops);

    entry.active_leases -= 1;
}

test "auto bulk group writes release leases so idle finish can publish" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/auto-bulk-group-write-release", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const batch_size = 250;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"_embeddings\":{{\"dense_idx\":[1.0,0.0]}}}}", .{}),
        };
    }

    var offset: usize = 0;
    while (offset < 1000) : (offset += batch_size) {
        for (writes, 0..) |*write, i| {
            alloc.free(@constCast(write.key));
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{offset + i});
        }

        _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
            .writes = writes,
            .timestamp_ns = @intCast(offset + 1),
            .sync_level = .write,
        });

        try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
        try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

        if (try source.source().localRuntimeStatuses(alloc, "docs")) |statuses| {
            var owned = statuses;
            owned.deinit(alloc);
        }
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    }

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].auto_bulk_ingest_ops);
    const replay_target = write_cache.entries.items[0].db.core.nextDerivedSequence();
    try std.testing.expect(replay_target > 0);
    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    try write_cache.entries.items[0].db.executor.waitForAll(replay_target);
    const stats = try write_cache.entries.items[0].db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1000), stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, stats.indexes[0].replay_applied_sequence);

    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &write_cache.entries.items[0].db));
    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 1000), statuses.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, statuses.items[0].stats.indexes[0].replay_applied_sequence);
}

test "weak-sync group writes publish all docs after background dense catch-up" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/auto-bulk-threshold-visible", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const batch_size = 250;
    const expected_docs = 1000;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"_embeddings\":{{\"dense_idx\":[1.0,0.0]}}}}", .{}),
        };
    }

    var offset: usize = 0;
    while (offset < expected_docs) : (offset += batch_size) {
        for (writes, 0..) |*write, i| {
            alloc.free(@constCast(write.key));
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{offset + i});
        }

        _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
            .writes = writes,
            .timestamp_ns = @intCast(offset + 1),
            .sync_level = .write,
        });

        try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    }

    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].auto_bulk_ingest_ops);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    const replay_target = write_cache.entries.items[0].db.core.nextDerivedSequence();
    try write_cache.entries.items[0].db.executor.waitForAll(replay_target);
    const stats = try write_cache.entries.items[0].db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, expected_docs), stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, stats.indexes[0].replay_applied_sequence);

    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &write_cache.entries.items[0].db));
    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, expected_docs), statuses.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, statuses.items[0].stats.indexes[0].replay_applied_sequence);
}

test "dirty auto bulk writer publishes runtime status before read invalidation closes it" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-provisioned-write-cache-publish-before-invalidate", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-status:{d}", .{i}),
            .value = "{\"title\":\"bulk status\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("docs", .general);

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(statuses.items[0]));
    try std.testing.expect(statuses.items[0].stats.index_count > 0);
}

test "managed visibility publish hook updates runtime status cache from live writer" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-managed-visibility-publish-status", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, table_catalog.emptyCatalogSource());
    source.runtime_status_cache = &snapshot_cache;

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const hook = source.managedDerivedVisibilityHook("docs", 7001, &db);
    hook.notify(.publish);

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(statuses.items[0]));
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned read preparation does not block on same-table batch after early dirty publication" {
    const alloc = std.testing.allocator;
    const replica_root_dir = "/tmp/antfly-api-provisioned-read-prep-active-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};

    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const BatchProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeBatch(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batch(self.alloc, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
                .timestamp_ns = 1,
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    lockAtomic(&source.local_db_mutex);
    {
        var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default, null, null);
        defer cached.deinit(alloc);
    }
    source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    var probe = BatchProbe{};
    test_before_batch_execution_hook = .{
        .ptr = &probe,
        .run = BatchProbe.beforeBatch,
    };
    defer test_before_batch_execution_hook = null;

    var batch_worker = BatchWorker{ .source = &source, .alloc = alloc };
    const batch_thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&batch_worker});
    defer batch_thread.join();

    const ReadWorker = struct {
        source: *ProvisionedTableWriteSource,
        started: std.atomic.Value(bool) = .init(false),
        completed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.source.readPreparation().prepareForRead("docs", .general);
            self.completed.store(true, .release);
        }
    };

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();

    var read_worker = ReadWorker{ .source = &source };
    const read_thread = try std.Thread.spawn(.{}, ReadWorker.run, .{&read_worker});
    defer read_thread.join();

    while (!read_worker.started.load(.acquire)) std.atomic.spinLoopHint();
    for (0..1000) |_| std.atomic.spinLoopHint();
    try std.testing.expect(read_worker.completed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    probe.release.store(true, .release);
    if (batch_worker.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));
}

test "provisioned txn resolve invalidates cached writer state on commit" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-txn-resolve-invalidates-write-cache";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);

    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");
    _ = try source.source().txnBeginGroupLocal(alloc, 7001, "docs", txn_id, 10_000, 0, &.{"group:7001"});
    _ = try source.source().txnPrepareGroupLocal(alloc, 7001, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
    });
    _ = try source.source().txnResolveGroupLocal(alloc, 7001, "docs", txn_id, .committed, 10_001);

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "bound table write source enforces root conditionals not and unique items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-conditional-unique-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"if\":{\"required\":[\"kind\"],\"properties\":{\"kind\":{\"enum\":[\"story\"]}}},\"then\":{\"required\":[\"headline\"]},\"else\":{\"required\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\",\"enum\":[\"story\",\"note\"]},\"headline\":{\"type\":\"text\"},\"slug\":{\"type\":\"keyword\"},\"tags\":{\"type\":\"array\",\"uniqueItems\":true,\"items\":{\"type\":\"keyword\"}},\"status\":{\"type\":\"keyword\",\"not\":{\"enum\":[\"archived\"]}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:story", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:note", .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-headline", .value = "{\"kind\":\"story\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"note\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:duplicate-tags", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"a\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-status", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"archived\"}" }},
    }));
}

test "bound table write source enforces property names and dependent required" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-property-names-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"dependentRequired\":{\"kind\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\",\"propertyNames\":{\"pattern\":\"^meta_[a-z]+$\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"attrs\":{\"meta_color\":\"red\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-dependent", .value = "{\"kind\":\"story\",\"attrs\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-property-name", .value = "{\"slug\":\"alpha\",\"attrs\":{\"bad\":\"red\"}}" }},
    }));
}

test "bound table write source enforces dependent schemas" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-dependent-schemas";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependentSchemas\":{\"kind\":{\"required\":[\"slug\"],\"properties\":{\"kind\":{\"const\":\"story\"},\"slug\":{\"type\":\"keyword\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"ok\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:no-trigger", .value = "{\"details\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"details\":\"ok\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-kind", .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"details\":\"ok\"}" }},
    }));
}

test "bound table write source enforces additional properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-additional-properties";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"title\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"keyword\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":1}}" }},
    }));
}

test "bound table write source enforces contains semantics" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-contains";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tags\":{\"type\":\"array\",\"contains\":{\"type\":\"keyword\",\"const\":\"hot\"},\"minContains\":1,\"maxContains\":2},\"scores\":{\"type\":\"array\",\"contains\":{\"type\":\"numeric\",\"minimum\":10}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"tags\":[\"hot\",\"warm\"],\"scores\":[1,10,20]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-contains", .value = "{\"tags\":[\"warm\"],\"scores\":[10]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:too-many-contains", .value = "{\"tags\":[\"hot\",\"hot\",\"hot\"],\"scores\":[10]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-score-match", .value = "{\"tags\":[\"hot\"],\"scores\":[1,2,3]}" }},
    }));
}

test "bound table write source enforces prefix items and pattern properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prefix-pattern";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"items\":{\"type\":\"numeric\"}},\"meta\":{\"type\":\"object\",\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"},\"^flag_[a-z]+$\":{\"type\":\"boolean\"}},\"additionalProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"coords\":[\"point\",1,2,3],\"meta\":{\"meta_color\":\"red\",\"flag_ready\":true}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-prefix-const", .value = "{\"coords\":[1,1,2],\"meta\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-prefix-type", .value = "{\"coords\":[\"point\",\"bad\"],\"meta\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern-type", .value = "{\"coords\":[\"point\",1],\"meta\":{\"meta_color\":1}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern-extra", .value = "{\"coords\":[\"point\",1],\"meta\":{\"other\":\"x\"}}" }},
    }));
}

test "bound table write source enforces exclusive numeric bounds and multipleOf" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-exclusive-multiple";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"score\":{\"type\":\"numeric\",\"exclusiveMinimum\":0,\"exclusiveMaximum\":10,\"multipleOf\":0.5}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"score\":5.5}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-exclusive-min", .value = "{\"score\":0}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-exclusive-max", .value = "{\"score\":10}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-multiple", .value = "{\"score\":5.25}" }},
    }));
}

test "bound table write source enforces nullable and type-array fields" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nullable-types";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"subtitle\":{\"type\":[\"text\",\"null\"]},\"score\":{\"type\":\"numeric\",\"nullable\":true},\"flag\":{\"type\":[\"boolean\"]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-nullable", .value = "{\"title\":\"alpha\",\"subtitle\":null,\"score\":null,\"flag\":true}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title-null", .value = "{\"title\":null,\"subtitle\":\"beta\",\"score\":1,\"flag\":true}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-flag-null", .value = "{\"title\":\"alpha\",\"subtitle\":\"beta\",\"score\":1,\"flag\":null}" }},
    }));
}

test "bound table write source enforces local defs and refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-defs-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"metaField\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}}},\"scoreField\":{\"type\":\"numeric\",\"nullable\":true}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\"},\"meta\":{\"$ref\":\"#/$defs/metaField\"},\"score\":{\"$ref\":\"#/$defs/scoreField\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"status\":\"ready\"},\"score\":null}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title", .value = "{\"title\":1,\"meta\":{\"status\":\"ready\"},\"score\":null}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta", .value = "{\"title\":\"alpha\",\"meta\":{\"status\":1},\"score\":null}" }},
    }));
}

test "bound table write source enforces ref siblings and nested local defs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-siblings-local-defs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"sharedText\":{\"type\":\"text\",\"minLength\":8}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\",\"minLength\":3},\"meta\":{\"type\":\"object\",\"$defs\":{\"sharedText\":{\"type\":\"text\",\"minLength\":4}},\"properties\":{\"note\":{\"$ref\":\"#/$defs/sharedText\",\"maxLength\":6}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"short\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title", .value = "{\"title\":\"ab\",\"meta\":{\"note\":\"short\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-local-def", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"abc\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-sibling", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"toolong\"}}" }},
    }));
}

test "bound table write source enforces recursive root refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-recursive-root-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:good", .value = "{\"name\":\"root\",\"children\":[{\"name\":\"leaf\",\"children\":[]},{\"name\":\"branch\",\"children\":[{\"name\":\"twig\"}]}]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:bad-child-type", .value = "{\"name\":\"root\",\"children\":[{\"name\":1}]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:bad-null-child", .value = "{\"name\":\"root\",\"children\":[null]}" }},
    }));
}

test "bound table write source enforces format and additionalItems" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-format-additional-items";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\",\"format\":\"email\"},\"site\":{\"type\":\"keyword\",\"format\":\"uri\"},\"id\":{\"type\":\"keyword\",\"format\":\"uuid\"},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"additionalItems\":false},\"labels\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\"}],\"additionalItems\":{\"type\":\"keyword\",\"pattern\":\"^[a-z]+$\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\",\"beta\"]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-email", .value = "{\"email\":\"bad\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-uri", .value = "{\"email\":\"a@example.com\",\"site\":\"not a uri\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-uuid", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"bad-uuid\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-extra-items", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1,2],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-additional-schema", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",1]}" }},
    }));
}

test "bound table write source enforces broader string formats" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-broader-formats";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"published_at\":{\"type\":\"keyword\",\"format\":\"date-time\"},\"birthday\":{\"type\":\"keyword\",\"format\":\"date\"},\"v4\":{\"type\":\"keyword\",\"format\":\"ipv4\"},\"v6\":{\"type\":\"keyword\",\"format\":\"ipv6\"},\"host\":{\"type\":\"keyword\",\"format\":\"hostname\"},\"ref\":{\"type\":\"keyword\",\"format\":\"uri-reference\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-datetime", .value = "{\"published_at\":\"2024-01-02\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-date", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-13-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ipv4", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"999.1.1.1\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ipv6", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"invalid\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-host", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"-bad-host\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ref", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs bad\"}" }},
    }));
}

test "bound table write source enforces unevaluated properties and items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"meta\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"}],\"unevaluatedItems\":{\"type\":\"numeric\"}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",1,2]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":1},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-unevaluated-item", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",\"bad\"]}" }},
    }));
}

test "bound table write source enforces composed unevaluated coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-unevaluated-composed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"kind\":{\"type\":\"keyword\"}}},{\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"title\":{\"type\":\"text\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}],\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-point", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-line", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"line\",1,2]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"bad\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-array-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1,2]}" }},
    }));
}

test "bound table write source enforces root unevaluated properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-root-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad", .value = "{\"kind\":\"story\",\"slug\":1}" }},
    }));
}

test "bound table write source enforces conditional and dependency unevaluated coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-conditional-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"if\":{\"properties\":{\"kind\":{\"const\":\"story\"}}},\"then\":{\"required\":[\"slug\"],\"properties\":{\"slug\":{\"type\":\"keyword\"}}},\"else\":{\"required\":[\"rating\"],\"properties\":{\"rating\":{\"type\":\"numeric\"}}},\"dependentSchemas\":{\"kind\":{\"properties\":{\"details\":{\"type\":\"text\"}}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:story", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:score", .value = "{\"kind\":\"score\",\"rating\":5,\"details\":\"body\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"details\":\"body\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-rating", .value = "{\"kind\":\"score\",\"details\":\"body\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:extra", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\",\"extra\":\"bad\"}" }},
    }));
}

test "bound table write source enforces anyOf and oneOf branch evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-branch-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"allOf\":[{\"properties\":{\"meta\":{\"type\":\"object\",\"anyOf\":[{\"properties\":{\"mode\":{\"const\":\"alpha\"},\"a\":{\"type\":\"keyword\"}}},{\"properties\":{\"mode\":{\"const\":\"beta\"},\"b\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"choice\":{\"type\":\"object\",\"oneOf\":[{\"properties\":{\"mode\":{\"const\":\"left\"},\"left\":{\"type\":\"keyword\"}},\"required\":[\"mode\",\"left\"]},{\"properties\":{\"mode\":{\"const\":\"right\"},\"right\":{\"type\":\"numeric\"}},\"required\":[\"mode\",\"right\"]}],\"unevaluatedProperties\":false}}}],\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:alpha-left", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:beta-right", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"b\":3},\"choice\":{\"mode\":\"right\",\"right\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-anyof-alpha", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"b\":3},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-anyof-beta", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"a\":\"oops\"},\"choice\":{\"mode\":\"right\",\"right\":9}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-oneof", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"right\":9}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:extra", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\",\"extra\":\"bad\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    }));
}

test "bound table write source enforces anyOf and oneOf array evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-array-branch-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"minItems\":3,\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]},\"choice\":{\"type\":\"array\",\"oneOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"left\"},{\"type\":\"keyword\"}],\"unevaluatedItems\":false},{\"minItems\":2,\"prefixItems\":[{\"const\":\"right\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:point-left", .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",\"ok\"]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:line-right", .value = "{\"coords\":[\"line\",1,2],\"choice\":[\"right\",9]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:coords-extra", .value = "{\"coords\":[\"point\",1,2],\"choice\":[\"left\",\"ok\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:coords-short", .value = "{\"coords\":[\"line\",1],\"choice\":[\"right\",9]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-wrong-branch", .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",9]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-extra", .value = "{\"coords\":[\"point\",1],\"choice\":[\"right\",9,10]}" }},
    }));
}

test "bound table write source enforces composed contains-driven array evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-array-contains-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"series\":{\"type\":\"array\",\"allOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"set\"}]},{\"contains\":{\"type\":\"numeric\",\"minimum\":10},\"minContains\":1}],\"unevaluatedItems\":false},\"selector\":{\"type\":\"array\",\"anyOf\":[{\"contains\":{\"const\":\"hot\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"cold\"},\"minContains\":1,\"unevaluatedItems\":false}]},\"exclusive\":{\"type\":\"array\",\"oneOf\":[{\"contains\":{\"const\":\"left\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"right\"},\"minContains\":1,\"unevaluatedItems\":false}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:hot-left", .value = "{\"series\":[\"set\",10,11],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:cold-right", .value = "{\"series\":[\"set\",12],\"selector\":[\"cold\"],\"exclusive\":[\"right\"]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:series-leftover", .value = "{\"series\":[\"set\",10,1],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:selector-no-branch", .value = "{\"series\":[\"set\",12],\"selector\":[\"warm\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:selector-overlap", .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\",\"cold\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:exclusive-overlap", .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\"],\"exclusive\":[\"left\",\"right\"]}" }},
    }));
}

test "bound table write source enforces composed pattern and additional properties evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-pattern-additional-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},{\"additionalProperties\":{\"type\":\"numeric\"}}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}],\"unevaluatedProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:pattern", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:additional", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:meta-extra", .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-typed-wrong", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-overlap", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true,\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:exclusive-overlap", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\",\"score\":9}}" }},
    }));
}

test "bound table write source enforces composed ref closure evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-pattern-additional";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"meta_patterns\":{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},\"meta_count\":{\"properties\":{\"count\":{\"type\":\"numeric\"}}},\"choice_flags\":{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},\"choice_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"}},\"exclusive_names\":{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},\"exclusive_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}},\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"$ref\":\"#/$defs/meta_patterns\"},{\"$ref\":\"#/$defs/meta_count\"}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"$ref\":\"#/$defs/choice_flags\"},{\"$ref\":\"#/$defs/choice_numbers\"}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"$ref\":\"#/$defs/exclusive_names\"},{\"$ref\":\"#/$defs/exclusive_numbers\"}],\"unevaluatedProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:pattern", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:additional", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:meta-extra", .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-bad", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
}

test "bound table write source enforces nullable composed refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nullable-composed-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"nullable_keyword\":{\"type\":[\"keyword\",\"null\"]},\"null_or_x\":{\"anyOf\":[{\"const\":null},{\"type\":\"keyword\",\"enum\":[\"x\"]}]}},\"properties\":{\"maybe\":{\"allOf\":[{\"$ref\":\"#/$defs/nullable_keyword\"},{\"$ref\":\"#/$defs/null_or_x\"}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:null", .value = "{\"maybe\":null}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:x", .value = "{\"maybe\":\"x\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:y", .value = "{\"maybe\":\"y\"}" }},
    }));
}

test "bound table write source enforces recursive ref closure semantics" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-recursive-closure";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("nodes", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^tag_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "nodes", req);
    _ = try source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:root", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":\"leaf\"},\"children\":[]},{\"name\":\"branch\",\"meta\":{\"tag_kind\":\"branch\",\"count\":1},\"children\":[{\"name\":\"twig\",\"meta\":{\"tag_kind\":\"twig\"}}]}]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:extra", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"extra\":\"bad\"}]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:meta-extra", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"other\":\"bad\"},\"children\":[]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:meta-type", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":1}}]}" }},
    }));
}

test "bound table write source enforces escaped ref tokens and direct fragment refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-escaped-hash";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"slash/name\":{\"type\":\"text\"},\"tilde~name\":{\"type\":\"keyword\"}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/slash~1name\"},\"kind\":{\"$ref\":\"#/$defs/tilde~0name\"},\"meta\":{\"type\":\"object\",\"$defs\":{\"local/name\":{\"type\":\"text\"}},\"properties\":{\"note\":{\"$ref\":\"#/properties/meta/$defs/local~1name\"},\"shadow\":{\"$ref\":\"#/properties/title\"}},\"required\":[\"note\",\"shadow\"]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-slash-ref", .value = "{\"title\":1,\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-tilde-ref", .value = "{\"title\":\"alpha\",\"kind\":true,\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-direct-fragment", .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":1}}" }},
    }));
}

test "bound table write source enforces legacy dependencies keyword" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-legacy-dependencies";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"mode\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependencies\":{\"kind\":[\"slug\"],\"mode\":{\"required\":[\"details\"],\"properties\":{\"mode\":{\"const\":\"long\"},\"details\":{\"type\":\"text\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\",\"details\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"mode\":\"long\",\"details\":\"ok\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-details", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-mode-const", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"short\",\"details\":\"ok\"}" }},
    }));
}

test "bound table write source rejects invalid commit writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-commit-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    try std.testing.expectError(error.InvalidBatchRequest, source.source().commitTransaction(alloc, &.{.{
        .table_name = "docs",
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }}, .write));
}

test "bound table write source rejects invalid commit transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-commit-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().commitTransaction(alloc, &.{.{
        .table_name = "docs",
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }}, .write));
}

test "bound table write source rejects invalid txn prepare writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prepare-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    const txn_id = try distributed_txn.parseTxnIdHex("11112222333344445555666677778888");
    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    try std.testing.expectError(error.InvalidBatchRequest, source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "bound table write source rejects invalid txn prepare transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prepare-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const txn_id = try distributed_txn.parseTxnIdHex("11112222333344445555666677779999");
    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    try std.testing.expectError(error.InvalidBatchRequest, source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }));
}

test "bound table write source rejects invalid batch transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }));
}

test "bound table write source derives ttl timestamps from ttl_field values" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ttl-field-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"ttl_duration_ns\":1000000000,\"ttl_field\":\"expires_at\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"expires_at\":{\"type\":\"datetime\"},\"title\":{\"type\":\"text\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"expires_at\":5}" }},
        .timestamp_ns = 999,
    });
    try std.testing.expectEqual(@as(u64, 5), try db.getTimestamp(alloc, "doc:a"));

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"expires_at\":\"2024-01-02T03:04:05Z\"}" }},
        .timestamp_ns = 999,
    });
    try std.testing.expectEqual(@as(u64, 1_704_164_645_000_000_000), try db.getTimestamp(alloc, "doc:b"));

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"expires_at\":\"not-a-time\"}" }},
    }));
}

test "provisioned table write source routes batch writes across ranges" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
    });

    left_db.close();
    left_db = try db_mod.DB.open(alloc, left_path, .{});
    right_db.close();
    right_db = try db_mod.DB.open(alloc, right_path, .{});

    var left = (try left_db.lookup(alloc, "doc:a", .{})).?;
    defer left.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, left.json, "\"alpha\"") != null);

    var right = (try right_db.lookup(alloc, "doc:z", .{})).?;
    defer right.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, right.json, "\"zeta\"") != null);
}

test "provisioned table write source rejects writes that violate enforced document schemas" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-batch-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .schema_json = "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "provisioned table write source drains managed dense enrichment before close" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-drain";

    const FakeEmbeddingProvider = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha body"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json_buf,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" }},
    });

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var reopened = try db_mod.DB.open(alloc, db_path, .{});
    defer reopened.close();

    try std.testing.expect(reopened.core.index_manager.denseIndex("semantic_idx").?.index.metadata.active_count > 0);

    const query_vec = [_]f32{ 1, 0, 0 };
    var result = try reopened.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec[0..],
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "provisioned table write cache eventually runs managed dense enrichment for write sync" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-write-cache";

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));
            _ = request_count.fetchAdd(1, .monotonic);

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha body"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json_buf,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.write_cache = &write_cache;
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" }},
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.request_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }

    try std.testing.expect(FakeEmbeddingProvider.request_count.load(.monotonic) > 0);
}

test "provisioned write cache invalidation closes failed managed enrichment db without aborting" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-write-cache-failed-close";

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);
        var rate_limited_count: std.atomic.Value(u32) = .init(0);

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const request_index = request_count.fetchAdd(1, .monotonic);
            if (request_index == 0) {
                const body = try std.fmt.allocPrint(
                    arena,
                    "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":[1,0,0]}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                    .{},
                );
                return .{
                    .status = 200,
                    .content_type = try arena.dupe(u8, "application/json"),
                    .body = body,
                };
            }

            _ = rate_limited_count.fetchAdd(1, .monotonic);
            const body = try arena.dupe(u8,
                \\{"error":{"message":"rate limited","type":"rate_limit_exceeded"}}
            );
            return .{
                .status = 429,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json_buf,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);
    FakeEmbeddingProvider.rate_limited_count.store(0, .monotonic);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.write_cache = &write_cache;
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta body\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.rate_limited_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }

    try std.testing.expect(FakeEmbeddingProvider.rate_limited_count.load(.monotonic) > 0);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);

    source.invalidateWriteCache("docs");

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
}

test "provisioned table write source invalidates cached query db after managed dense replay becomes visible" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-query-visibility";

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);
        var rate_limited_count: std.atomic.Value(u32) = .init(0);
        var allow_all: std.atomic.Value(bool) = .init(false);

        fn vectorForInput(input: std.json.Value) []const u8 {
            if (jsonValueContainsText(input, "alpha")) return "[1,0,0]";
            if (jsonValueContainsText(input, "beta")) return "[0,1,0]";
            return "[0,0,1]";
        }

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const request_index = request_count.fetchAdd(1, .monotonic);
            if (request_index != 0 and !allow_all.load(.acquire)) {
                _ = rate_limited_count.fetchAdd(1, .monotonic);
                const body = try arena.dupe(u8,
                    \\{"error":{"message":"rate limited","type":"rate_limit_exceeded"}}
                );
                return .{
                    .status = 429,
                    .content_type = try arena.dupe(u8, "application/json"),
                    .body = body,
                };
            }

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vectorForInput(parsed_req.value.input)},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }

        fn allowAll() void {
            allow_all.store(true, .release);
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json_buf,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);
    FakeEmbeddingProvider.rate_limited_count.store(0, .monotonic);
    FakeEmbeddingProvider.allow_all.store(false, .monotonic);

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.read_cache = &read_cache;
    source.write_cache = &write_cache;

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta body\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"gamma body\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.rate_limited_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }
    try std.testing.expect(FakeEmbeddingProvider.rate_limited_count.load(.monotonic) > 0);

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    {
        var read_lease = try read_cache.getOrOpen(db_path, FakeCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        var initial = try read_lease.db.search(alloc, .{
            .index_name = "semantic_idx",
            .dense = .{
                .vector = &.{ 1.0, 0.0, 0.0 },
                .k = 3,
            },
            .limit = 3,
        });
        defer initial.deinit();
        try std.testing.expect(initial.total_hits < 3);
    }

    FakeEmbeddingProvider.allowAll();

    var ready = false;
    attempts = 0;
    while (attempts < 200) : (attempts += 1) {
        {
            var read_lease = try read_cache.getOrOpen(db_path, FakeCatalog.iface(), 7001, 0, "docs");
            defer read_lease.release();

            var result = try read_lease.db.search(alloc, .{
                .index_name = "semantic_idx",
                .dense = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 3,
                },
                .limit = 3,
            });
            defer result.deinit();
            if (result.total_hits == 3 and result.hits.len == 3) {
                try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
                ready = true;
                break;
            }
        }

        sleepNs(25 * std.time.ns_per_ms);
    }

    try std.testing.expect(ready);
}

test "provisioned table write source persists chunk artifacts when chunker enables full text indexing" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-chunk-full-text";

    const FakeEmbeddingProvider = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json_buf,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_chunked_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}},"chunker":{{"provider":"antfly","model":"fixed-bert-tokenizer","store_chunks":false,"full_text_index":{{}},"text":{{"target_tokens":4,"overlap_tokens":1,"separator":" "}}}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"Alpha with full text chunks\",\"body\":\"alpha alpha alpha alpha beta beta beta beta beta beta\"}" }},
        .sync_level = .write,
    });

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var reopened = try db_mod.DB.open(alloc, db_path, .{});
    defer reopened.close();

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "semantic_chunked_idx_chunks");
    defer alloc.free(chunk_prefix);
    const artifacts = try reopened.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expect(chunk_count >= 2);
}

test "provisioned table write source runtime status is best effort when local db is busy" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status", FakeCatalog.iface());
    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    const statuses = try source.source().localRuntimeStatuses(std.testing.allocator, "docs");
    try std.testing.expect(statuses == null);
}

test "provisioned table write source runtime statuses reconcile empty embeddings indexes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-provisioned-write-runtime-status-managed", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;
    lockAtomic(&source.local_db_mutex);
    var cached = try write_cache.getOrOpenLocked(group_path, FakeCatalog.iface(), 7001, 0, "docs");
    source.local_db_mutex.unlock();
    defer cached.deinit(alloc);
    try publishRuntimeStatusSnapshot(&source, alloc, "docs", 7001, cached.db);
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(false, statuses.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status stays cache-only without shared snapshot" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-runtime-cache";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, NoCatalog.iface());
    source.write_cache = &write_cache;

    lockAtomic(&source.local_db_mutex);
    var cached = try write_cache.getOrOpenLocked(group_path, WarmCatalog.iface(), 7001, 0, "docs");
    source.local_db_mutex.unlock();
    defer cached.deinit(alloc);

    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table write source runtime status prefers shared snapshot cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-runtime-prefers-snapshot";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"cached_handle_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var cached = try write_cache.getOrOpenLocked(path, WarmCatalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "snapshot_idx"),
        .kind = .dense_vector,
        .doc_count = 42,
    };
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 42,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    defer status.deinit(alloc);
    try snapshot_cache.upsertGroupStatus("docs", status);

    var source = ProvisionedTableWriteSource.init(path, NoCatalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 42), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("snapshot_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table write source structural mutation invalidates shared runtime status cache" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    try snapshot_cache.upsertGroupStatus("docs", .{
        .group_id = 7001,
        .stats = .{},
    });
    {
        var before = (try snapshot_cache.snapshot(alloc, "docs")).?;
        defer before.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), before.items.len);
    }

    var source = ProvisionedTableWriteSource.init("/tmp/antfly-runtime-status-structural-invalidation", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginLocalStructuralMutation("docs");
    source.finishLocalStructuralMutation("docs");

    try std.testing.expect((try snapshot_cache.snapshot(alloc, "docs")) == null);
}

test "provisioned table write source runtime status falls back to shared snapshot cache" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table write source cached runtime status does not fetch catalog coverage" {
    const alloc = std.testing.allocator;

    const CountingCatalog = struct {
        calls: usize = 0,

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
            self.calls += 1;
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var cached_status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer cached_status.deinit(alloc);
    cached_status.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 101,
        .replay_catch_up_required = true,
        .backfill_active = true,
    };
    try snapshot_cache.upsertGroupStatus("docs", cached_status);

    var catalog = CountingCatalog{};
    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status-no-catalog-coverage", catalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), catalog.calls);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 25_000), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("dense_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "provisioned table write source runtime status does not cold-open uncached db" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-uncached-fallback", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        _ = try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table write source runtime status serves cached snapshot during active same-table work" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-active", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty and request busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-request-busy-dirty", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves sibling groups while one group is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 2);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };
    items[1] = .{
        .group_id = 7002,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[1].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-active-sibling", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 7002), statuses.items[1].group_id);
    try std.testing.expectEqual(@as(u64, 3), statuses.items[1].stats.doc_count);
}

test "provisioned table write source runtime status filtering transfers owned statuses" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status-filter-ownership", NoCatalog.iface());

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    errdefer alloc.free(items);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    errdefer items[0].deinit(alloc);
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    var owned: runtime_status.LocalTableRuntimeStatuses = .{ .items = items };
    defer owned.deinit(alloc);

    var filtered = (try source.takeStatusesWithoutActiveGroups(alloc, &owned, &.{7002})).?;
    defer filtered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), owned.items.len);
    try std.testing.expectEqual(@as(usize, 1), filtered.items.len);
    try std.testing.expectEqual(@as(u64, 7001), filtered.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), filtered.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves unrelated table snapshot while source mutex is busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-unrelated-busy", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty and busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty without source contention" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty-unlocked", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves clean sibling table while another table is dirty" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const docs_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    docs_items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    docs_items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const logs_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    logs_items[0] = .{
        .group_id = 8001,
        .stats = .{
            .doc_count = 4,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    logs_items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 4,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 2);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = docs_items },
    };
    snapshots[1] = .{
        .table_name = try alloc.dupe(u8, "logs"),
        .statuses = .{ .items = logs_items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty-sibling", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "logs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 8001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 4), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves shared snapshot cache while clean and busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-clean-busy", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source serializes same-table same-group operations" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-activity", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(!source.tryBeginGroupOperation("docs", 7001));
}

test "provisioned table write source structural activity waits for table request lease" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-request", NoCatalog.iface());
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));
    try std.testing.expect(source.hasReadBlockingActivityBestEffort("docs", 7001));
}

test "provisioned table write source keeps structural activity blocked until last table request exits" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-request-refcount", NoCatalog.iface());
    source.beginTableRequest("docs");
    source.beginTableRequest("docs");

    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));

    source.endTableRequest("docs");
    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));

    source.endTableRequest("docs");
    try std.testing.expect(source.tryBeginStructuralTableActivity("docs"));
    source.endStructuralTableActivity("docs");
}

test "provisioned table write source allows different groups of same table to proceed independently" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-activity-different-groups", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(source.tryBeginGroupOperation("docs", 7002));
    source.endGroupOperation("docs", 7002);
}

test "provisioned table write source reports only same-group activity as busy" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-group-activity", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(source.hasGroupActivityBestEffort("docs", 7001));
    try std.testing.expect(!source.hasGroupActivityBestEffort("docs", 7002));
    try std.testing.expect(source.hasReadBlockingActivityBestEffort("docs", 7001));
    try std.testing.expect(!source.hasReadBlockingActivityBestEffort("docs", 7002));
}

test "provisioned table write source managed writer probe is unknown while source mutex is busy" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-managed-writer-state", NoCatalog.iface());
    lockAtomic(&source.local_db_mutex);
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(ProvisionedTableWriteSource.ManagedWriterGroupProbe, .unknown), source.probeManagedWriterGroupBestEffort("docs", 7001));
}

test "provisioned table write source runtime status stays null while dirty and busy during startup catch-up" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .artifact_rebuild,
                    .wal_retention_known = true,
                    .wal_retained_segments = 4,
                    .wal_retained_bytes = 99,
                },
                .dense_catch_up = .{
                    .active = true,
                    .current_sequence = 4,
                    .current_target_sequence = 9,
                    .current_scanned_entries = 12,
                    .current_applied_entries = 4,
                    .progress_updates = 3,
                },
            },
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-startup", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");
    source.startup_catch_up_active.store(true, .monotonic);

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);

    const async_stats = source.asyncIndexingStatsBestEffort();
    try std.testing.expect(async_stats.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, async_stats.startup.phase);
    try std.testing.expectEqual(@as(u64, 99), async_stats.startup.wal_retained_bytes);
    try std.testing.expect(async_stats.dense_catch_up.active);
    try std.testing.expectEqual(@as(u64, 9), async_stats.dense_catch_up.current_target_sequence);
}

test "provisioned table write source runtime status serves cached snapshot when request is idle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-snapshot", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status remains cache-only when dirty and idle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status does not inspect read cache hbc stats when dirty" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-read-cache-hbc", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.read_cache = &read_cache;
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    const read_cache_stats_before = read_cache.cacheStats();
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    const read_cache_stats_after = read_cache.cacheStats();

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].hbc_cache.total_bytes);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].hbc_cache.vector.used_bytes);
    try std.testing.expectEqual(read_cache_stats_before.hit_count, read_cache_stats_after.hit_count);
    try std.testing.expectEqual(read_cache_stats_before.miss_count, read_cache_stats_after.miss_count);
}

test "provisioned table write source read cache overlay preserves live replay status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-read-cache-preserve-replay", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    {
        var read_lease = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        const query_vec = [_]f32{ 1.0, 2.0 };
        const req: db_mod.types.SearchRequest = .{
            .index_name = "semantic_idx",
            .dense = .{ .vector = query_vec[0..], .k = 1 },
            .limit = 1,
        };
        var profiled = try read_lease.db.searchDenseProfiled(alloc, req, req.dense.?);
        defer profiled.result.deinit();
        try std.testing.expect(profiled.result.hits.len >= 1);
    }

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.read_cache = &read_cache;

    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer status.deinit(alloc);
    status.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .replay_applied_sequence = 42,
        .replay_target_sequence = 42,
        .replay_catch_up_required = false,
        .backfill_active = false,
    };

    try source.overlayReadCacheIndexVisibilityBestEffort(alloc, "docs", 7001, &status);

    try std.testing.expectEqual(@as(u64, 1), status.stats.indexes[0].doc_count);
    try std.testing.expect(status.stats.indexes[0].hbc_cache.total_bytes > 0);
    try std.testing.expect(status.stats.indexes[0].hbc_cache.vector.used_bytes > 0);
    try std.testing.expectEqual(@as(u64, 42), status.stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 42), status.stats.indexes[0].replay_target_sequence);
    try std.testing.expect(!status.stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(!status.stats.indexes[0].backfill_active);
}

test "provisioned table write source restore repair completion retires cached vector read state" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-repair-read-state", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    {
        var read_lease = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        const query_vec = [_]f32{ 1.0, 2.0 };
        var result = try read_lease.db.search(alloc, .{
            .index_name = "semantic_idx",
            .dense = .{ .vector = query_vec[0..], .k = 1 },
            .limit = 1,
        });
        defer result.deinit();
        try std.testing.expect(result.hits.len >= 1);
    }

    try std.testing.expect(hbc_cache.global_stats.total_bytes > 0);
    const stats_before = read_cache.cacheStats();
    {
        var repair_db = try openManagedDbWithIndexesJsonAndCache(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
            null,
            &hbc_cache,
            0,
            null,
        );
        defer repair_db.close();
        repair_db.clearDenseHbcCaches();
    }

    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,

        fn onChange(ptr: *anyopaque, _: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
        }
    };
    var hook = Hook{};
    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.read_cache = &read_cache;
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    source.enqueueRestoreRepairComplete("docs");
    source.restore_repair_completion_group.await(source.table_activity_threaded.io()) catch {};

    try std.testing.expectEqual(@as(u64, 0), hbc_cache.global_stats.total_bytes);
    try std.testing.expectEqual(@as(usize, 1), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);

    {
        var reopened = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer reopened.release();
    }
    const stats_after = read_cache.cacheStats();
    try std.testing.expectEqual(stats_before.miss_count + 1, stats_after.miss_count);
}

test "provisioned restore repair open rejects stale doc identity namespace" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 97001,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-repair-stale-docid", .{tmp.sub_path});
    defer alloc.free(path);

    const stale_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 71001 };
    {
        var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = stale_namespace });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"stale\"}" }},
            .sync_level = .write,
        });
    }

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-restore-repair-stale-docid", Catalog.iface());
    defer source.deinit();

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.openRestoreRepairDbForGroup(
        alloc,
        path,
        7001,
        "docs",
        "{}",
    ));
}

test "provisioned table write source visibility hook publishes owner db status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-visibility-hook", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .full_index,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,
        table_name: []const u8 = "",

        fn onChange(ptr: *anyopaque, table_name: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
            self.table_name = table_name;
        }
    };
    var hook = Hook{};
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, &db, .publish);

    try std.testing.expectEqual(@as(usize, 1), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
    try std.testing.expectEqualStrings("docs", hook.table_name);
    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, published.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, published.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.indexes[0].doc_count);

    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, null, .invalidate);

    try std.testing.expectEqual(@as(usize, 2), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
    try std.testing.expectEqualStrings("docs", hook.table_name);
    var retained = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer retained.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), retained.items.len);
    try std.testing.expectEqual(@as(u64, 1), retained.items[0].stats.doc_count);
}

test "provisioned table write source promotes synthetic placeholder when publishing live db status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-promote-synthetic", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var placeholder = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .metadata = .{
            .updated_at_ns = 1,
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer placeholder.deinit(alloc);
    placeholder.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
    };
    try snapshot_cache.upsertGroupStatus("docs", placeholder);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    try publishRuntimeStatusSnapshot(&source, alloc, "docs", 7001, &db);

    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, published.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, published.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(published.items[0]));
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source publishes replay debt from owner db handle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-replay-debt-publish", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        null,
        null,
        0,
        null,
        .writer_no_replay,
    );
    defer db.close();
    _ = try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
        .sync_level = .write,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &db));

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("dv_v1", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_target_sequence > statuses.items[0].stats.indexes[0].replay_applied_sequence);
}

test "provisioned runtime status overlays live writer replay target without republishing stats" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-live-target-overlay", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var seeded = try openManagedDbWithIndexesJsonAndCacheMode(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
            null,
            null,
            0,
            null,
            .writer_no_replay,
        );
        defer seeded.close();
        _ = try seeded.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    {
        var initial = try openManagedDbWithIndexesJsonAndCacheMode(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
            null,
            null,
            0,
            null,
            .writer_no_replay,
        );
        defer initial.close();
        try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &initial));
    }

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);
    _ = try cached.db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"embedding\":[2,3]}" }},
        .sync_level = .write,
    });

    var cached_only = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer cached_only.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), cached_only.items[0].stats.indexes[0].replay_target_sequence);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, 2), statuses.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "provisioned table read source serves profiled dense query without runtime status warmup" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-warmed-read-cache-profiled-query", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" },
                .{ .key = "doc:b", .value = "{\"_embeddings\":{\"semantic_idx\":[2,1]}}" },
            },
            .sync_level = .full_index,
        });
    }

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    _ = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    write_source.read_cache = &read_cache;
    write_source.write_cache = &write_cache;
    write_source.runtime_status_cache = &snapshot_cache;
    write_source.markWriteCacheDirty("docs");

    try std.testing.expect((try write_source.source().localRuntimeStatuses(alloc, "docs")) == null);

    var read_source = table_reads.ProvisionedTableReadSource.init(replica_root_dir, Catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    read_source.cache = &read_cache;

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"semantic_idx":[1.0,2.0]},"indexes":["semantic_idx"],"limit":2,"profile":true}
    );
    defer owned.deinit(alloc);

    var response = (try read_source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, response.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.responses.?[0].hits != null);
    try std.testing.expect(parsed.value.responses.?[0].profile != null);
}

test "hosted provisioned table read source serves profiled dense query after external write-sync batch without index-not-found" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (group_id == 7001) .active else .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 1 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            _ = node_id;
            return if (group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-profiled-external-write-sync", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    _ = try write_source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" },
            .{ .key = "doc:b", .value = "{\"_embeddings\":{\"semantic_idx\":[2,1]}}" },
        },
        .sync_level = .write,
    });

    var executor_state = ExecutorState{};
    var hosted = table_reads.HostedProvisionedTableReadSource.init(
        replica_root_dir,
        Catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"semantic_idx":[1.0,2.0]},"indexes":["semantic_idx"],"limit":2,"profile":true}
    );
    defer owned.deinit(alloc);

    var response = (try hosted.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("responses") != null);
}

test "provisioned table read source survives many external write-sync batches before first profiled dense query" {
    const alloc = std.testing.allocator;
    const total_docs: usize = 50_000;
    const batch_size: usize = 250;
    const dims: usize = 384;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":384}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeStatusSource = struct {
        fn iface() http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return try Catalog.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            Catalog.freeAdminSnapshot(undefined, snapshot);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-profiled-external-write-sync-many", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    write_source.read_cache = &read_cache;
    write_source.write_cache = &write_cache;
    write_source.runtime_status_cache = &snapshot_cache;

    const dense_doc_json = blk: {
        var out: std.Io.Writer.Allocating = .init(alloc);
        defer out.deinit();
        try out.writer.writeAll("{\"_embeddings\":{\"semantic_idx\":[");
        for (0..dims) |i| {
            if (i != 0) try out.writer.writeByte(',');
            try out.writer.writeAll("1");
        }
        try out.writer.writeAll("]}}");
        break :blk try out.toOwnedSlice();
    };
    defer alloc.free(dense_doc_json);

    const query_json = blk: {
        var out: std.Io.Writer.Allocating = .init(alloc);
        defer out.deinit();
        try out.writer.writeAll("{\"embeddings\":{\"semantic_idx\":[");
        for (0..dims) |i| {
            if (i != 0) try out.writer.writeByte(',');
            try out.writer.writeAll("1.0");
        }
        try out.writer.writeAll("]},\"indexes\":[\"semantic_idx\"],\"limit\":10,\"profile\":true}");
        break :blk try out.toOwnedSlice();
    };
    defer alloc.free(query_json);

    {
        var cold_read_source = table_reads.ProvisionedTableReadSource.init(
            replica_root_dir,
            Catalog.iface(),
            raft_mod.read_gate.noopReadableLeaseRequester(),
        );
        cold_read_source.cache = &read_cache;

        var cold_owned = try query_api.parseQueryRequest(alloc, null, "docs", query_json);
        defer cold_owned.deinit(alloc);

        var cold_response = (try cold_read_source.source().query(alloc, "docs", cold_owned.req, .read_index)).?;
        defer cold_response.deinit(alloc);
    }

    for (0..(total_docs / batch_size)) |batch_idx| {
        const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
        defer {
            for (writes) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            alloc.free(writes);
        }
        for (writes, 0..) |*write, i| {
            const doc_idx = batch_idx * batch_size + i;
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
            write.value = try alloc.dupe(u8, dense_doc_json);
        }
        _ = try write_source.source().batch(alloc, "docs", .{
            .writes = writes,
            .sync_level = .write,
        });
    }

    var read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root_dir,
        Catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );
    read_source.cache = &read_cache;

    var server = http_server.ApiHttpServer.init(
        alloc,
        .{},
        FakeStatusSource.iface(),
        read_source.source(),
        write_source.source(),
    );

    const IndexDetail = struct {
        status: ?struct {
            doc_count: ?u64 = null,
            total_indexed: ?u64 = null,
            replay_target_sequence: ?u64 = null,
            replay_applied_sequence: ?u64 = null,
            replay_catch_up_required: ?bool = null,
            backfill_active: ?bool = null,
            rebuilding: ?bool = null,
        } = null,
    };

    var ready = false;
    for (0..200) |_| {
        var detail = try server.handlePublicTableGetIndex("docs", "semantic_idx");
        defer detail.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), detail.status);
        var parsed_detail = try std.json.parseFromSlice(IndexDetail, alloc, detail.body, .{ .ignore_unknown_fields = true });
        defer parsed_detail.deinit();
        if (parsed_detail.value.status) |idx| {
            if ((idx.doc_count orelse 0) == total_docs and
                (idx.total_indexed orelse 0) == total_docs and
                (idx.replay_applied_sequence orelse 0) == (idx.replay_target_sequence orelse 0) and
                !(idx.replay_catch_up_required orelse false) and
                !(idx.backfill_active orelse false) and
                !(idx.rebuilding orelse false))
            {
                ready = true;
                break;
            }
        }
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(ready);

    var response = try server.handlePublicTableQuery("docs", query_json, null);
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("responses") != null);
}

test "provisioned table write source runtime status keeps cached snapshot while request is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-request-busy", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status remains cache-only while bulk ingest session is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-bulk-ingest-active", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    try write_cache.beginBulkIngestLocked("docs");

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status does not lease writer during auto bulk ingest" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-auto-bulk-active", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
}

test "read preparation keeps write cache dirty while auto bulk ingest is active" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/read-prep-auto-bulk-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    source.markWriteCacheDirty("docs");

    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "runtime status request does not finish expired auto bulk ingest" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-auto-bulk-expired", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    const now_ns = platform_time.monotonicNs();
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", now_ns -| auto_bulk_ingest_max_idle_ns);
    write_cache.entries.items[0].auto_bulk_ingest_last_ns = now_ns -| auto_bulk_ingest_max_idle_ns;
    source.markWriteCacheDirty("docs");
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{},
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
}

test "runtime status refresh preserves current snapshot on allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const current_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
            var current = runtime_status.LocalTableRuntimeStatuses{ .items = current_items };
            defer current.deinit(alloc);
            current.items[0] = .{
                .group_id = 7001,
                .stats = .{ .doc_count = 11 },
            };

            const refresh_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
            var refresh = runtime_status.LocalTableRuntimeStatuses{ .items = refresh_items };
            defer refresh.deinit(alloc);
            refresh.items[0] = .{
                .group_id = 7001,
                .stats = .{ .doc_count = 99 },
            };

            ProvisionedTableWriteSource.replaceRuntimeStatusesWithMergedRefresh(alloc, &current, &refresh) catch |err| switch (err) {
                error.OutOfMemory => {
                    try std.testing.expectEqual(@as(usize, 1), current.items.len);
                    try std.testing.expectEqual(@as(u64, 11), current.items[0].stats.doc_count);
                    return;
                },
            };

            try std.testing.expectEqual(@as(usize, 1), current.items.len);
            try std.testing.expectEqual(@as(u64, 99), current.items[0].stats.doc_count);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "provisioned table write source startup snapshot preserves existing group status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-startup-overlay", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{
        .active = true,
        .phase = .opening_db,
        .wal_retention_known = true,
        .wal_retained_segments = 5,
        .wal_retained_bytes = 123,
    }, null, null);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.opening_db, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 5), statuses.items[0].stats.async_indexing.startup.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 123), statuses.items[0].stats.async_indexing.startup.wal_retained_bytes);
}

test "startup async overlay replaces async stats while preserving cached table stats" {
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = &.{},
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retention_known = true,
                    .wal_retained_segments = 5,
                    .wal_retained_bytes = 123,
                },
                .dense_catch_up = .{
                    .active = false,
                    .current_sequence = 1,
                    .current_target_sequence = 2,
                    .current_scanned_entries = 3,
                    .current_applied_entries = 4,
                    .progress_updates = 5,
                },
            },
        },
    };

    applyStartupCatchUpAsyncOverlay(&status, .{
        .startup = .{
            .active = true,
            .phase = .artifact_rebuild,
            .configured_indexes = 2,
            .opened_indexes = 2,
        },
        .dense_catch_up = .{
            .active = true,
            .current_sequence = 10880,
            .current_target_sequence = 1001001,
            .current_scanned_entries = 10880,
            .current_applied_entries = 10880,
            .progress_updates = 91,
        },
    }, .{
        .active = true,
        .phase = .artifact_rebuild,
        .wal_retention_known = true,
        .wal_retained_segments = 7,
        .wal_retained_bytes = 456,
    });

    try std.testing.expectEqual(@as(u64, 9), status.stats.doc_count);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, status.stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 7), status.stats.async_indexing.startup.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 456), status.stats.async_indexing.startup.wal_retained_bytes);
    try std.testing.expect(status.stats.async_indexing.dense_catch_up.active);
    try std.testing.expectEqual(@as(u64, 10880), status.stats.async_indexing.dense_catch_up.current_applied_entries);
}

test "startup catch-up stats for path include table and index-local wal retention" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/startup-path-retention/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);

    var native = try lsm_backend.storage_io.NativeStorage.init(alloc, .threaded);
    defer native.deinit();
    try native.storage().createDirPath(db_path);

    const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/vec", .{db_path});
    defer alloc.free(index_path);
    try native.storage().createDirPath(index_path);

    _ = try lsm_backend.wal.appendReplay(
        native.storage(),
        alloc,
        db_path,
        1,
        "first",
        false,
        .{},
    );

    var index_state: lsm_backend.state.State = .{};
    defer index_state.deinit(alloc);
    try index_state.appendUpsert(alloc, .{ .name = "docs" }, "doc:a", "A", false);
    _ = try lsm_backend.wal.appendState(native.storage(), alloc, index_path, index_state, false);

    var configured_indexes = try parseStartupConfiguredIndexes(
        alloc,
        "{\"indexes\":[{\"name\":\"vec\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":3}}]}",
    );
    defer configured_indexes.deinit(alloc);
    const stats = try startupCatchUpStatsForPath(db_path, .opening_db, &configured_indexes);
    try std.testing.expect(stats.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.opening_db, stats.phase);
    try std.testing.expectEqual(@as(u64, 2), stats.wal_retained_segments);
    try std.testing.expect(stats.wal_retained_bytes > 0);
    try std.testing.expectEqual(@as(u64, 1), stats.configured_indexes);
    try std.testing.expectEqual(@as(u64, 1), stats.configured_dense_indexes);
}

test "runtime status snapshot with startup phase refreshes live table stats for active group" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-startup-phase-overlay/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = &.{},
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retention_known = true,
                    .wal_retained_segments = 5,
                    .wal_retained_bytes = 123,
                },
                .dense_catch_up = .{
                    .active = false,
                    .current_sequence = 1,
                    .current_target_sequence = 2,
                    .current_scanned_entries = 3,
                    .current_applied_entries = 4,
                    .progress_updates = 5,
                },
            },
        },
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-startup-phase-overlay", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try publishRuntimeStatusSnapshotWithStartupPhase(&source, alloc, "docs", 7001, .artifact_rebuild, &db);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retention_known);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retained_segments > 0);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retained_bytes > 0);
}

test "startup runtime status snapshot with live db refreshes table stats during active startup" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/startup-runtime-status-live-db/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 999,
            .index_count = 0,
            .indexes = &.{},
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retention_known = true,
                    .wal_retained_segments = 5,
                    .wal_retained_bytes = 123,
                },
            },
        },
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-startup-runtime-status-live-db", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{
        .active = true,
        .phase = .artifact_rebuild,
    }, &db, null);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retention_known);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retained_segments > 0);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retained_bytes > 0);
}

test "runtime status snapshot with idle phase refreshes live stats after startup catch-up" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-startup-phase-idle/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 999,
            .index_count = 1,
            .indexes = &.{},
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .artifact_rebuild,
                    .wal_retained_segments = 7,
                    .wal_retained_bytes = 456,
                },
                .dense_catch_up = .{
                    .active = true,
                    .current_sequence = 10880,
                    .current_target_sequence = 1001001,
                    .current_scanned_entries = 10880,
                    .current_applied_entries = 10880,
                    .progress_updates = 91,
                },
            },
        },
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-startup-phase-idle", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try publishRuntimeStatusSnapshotWithStartupPhase(&source, alloc, "docs", 7001, .idle, &db);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.idle, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expect(!statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retained_segments > 0);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.wal_retained_bytes > 0);
}

test "provisioned table write source startup snapshot builds synthetic status from object-form indexes json" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-startup-overlay-empty", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    var configured_indexes = try parseStartupConfiguredIndexes(
        alloc,
        "{\"vec\":{\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":768}},\"fts\":{\"type\":\"full_text\"},\"alg\":{\"type\":\"algebraic\",\"version\":2,\"schema_version\":42,\"capability_fingerprint\":\"cap:v1\",\"capability_lifecycle_status\":\"rebuild_required\",\"capability_change_added_fields\":1,\"capability_change_removed_fields\":2,\"capability_change_changed_type_fields\":3,\"skipped_dynamic_fields\":4,\"skipped_complex_fields\":5,\"skipped_unbounded_fields\":6,\"materializations\":[]}}",
    );
    defer configured_indexes.deinit(alloc);

    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{
        .active = true,
        .phase = .opening_db,
        .configured_indexes = 3,
        .configured_dense_indexes = 1,
        .configured_full_text_indexes = 1,
        .wal_retained_segments = 5,
        .wal_retained_bytes = 123,
    }, null, &configured_indexes);
    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{}, null, null);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 3), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("vec", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.dense_vector, statuses.items[0].stats.indexes[0].kind);
    try std.testing.expectEqualStrings("fts", statuses.items[0].stats.indexes[1].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.full_text, statuses.items[0].stats.indexes[1].kind);
    try std.testing.expectEqualStrings("alg", statuses.items[0].stats.indexes[2].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.algebraic, statuses.items[0].stats.indexes[2].kind);
    try std.testing.expectEqual(@as(u32, 42), statuses.items[0].stats.indexes[2].algebraic_schema_version);
    try std.testing.expectEqualStrings("cap:v1", statuses.items[0].stats.indexes[2].algebraic_capability_fingerprint.?);
    try std.testing.expectEqualStrings("rebuild_required", statuses.items[0].stats.indexes[2].algebraic_capability_lifecycle_status.?);
    try std.testing.expectEqual(@as(u32, 1), statuses.items[0].stats.indexes[2].algebraic_capability_change_added_fields);
    try std.testing.expectEqual(@as(u32, 2), statuses.items[0].stats.indexes[2].algebraic_capability_change_removed_fields);
    try std.testing.expectEqual(@as(u32, 3), statuses.items[0].stats.indexes[2].algebraic_capability_change_changed_type_fields);
    try std.testing.expectEqual(@as(u32, 4), statuses.items[0].stats.indexes[2].algebraic_skipped_dynamic_fields);
    try std.testing.expectEqual(@as(u32, 5), statuses.items[0].stats.indexes[2].algebraic_skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 6), statuses.items[0].stats.indexes[2].algebraic_skipped_unbounded_fields);
    try std.testing.expect(!statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expectEqual(@as(u64, 3), statuses.items[0].stats.async_indexing.startup.configured_indexes);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.async_indexing.startup.configured_dense_indexes);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.async_indexing.startup.configured_full_text_indexes);
}

test "provisioned table write source startup snapshot builds synthetic status from array-form indexes json" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-startup-overlay-array", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    var configured_indexes = try parseStartupConfiguredIndexes(
        alloc,
        "{\"indexes\":[{\"name\":\"vec\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":768}},{\"name\":\"fts\",\"type\":\"full_text\",\"config\":{}}]}",
    );
    defer configured_indexes.deinit(alloc);

    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{
        .active = true,
        .phase = .opening_db,
        .configured_indexes = 2,
        .configured_dense_indexes = 1,
        .configured_full_text_indexes = 1,
        .wal_retained_segments = 7,
        .wal_retained_bytes = 321,
    }, null, &configured_indexes);
    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{}, null, null);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 2), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("vec", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.dense_vector, statuses.items[0].stats.indexes[0].kind);
    try std.testing.expectEqualStrings("fts", statuses.items[0].stats.indexes[1].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.full_text, statuses.items[0].stats.indexes[1].kind);
    try std.testing.expect(!statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expectEqual(@as(u64, 2), statuses.items[0].stats.async_indexing.startup.configured_indexes);
}

test "provisioned table write source maintenance probes are best effort when local db is busy" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-maintenance-probe", FakeCatalog.iface());
    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(u64, 0), source.lsmMaintenanceScoreBestEffort());
    try std.testing.expect(source.hasActiveBulkIngestSession());
    try std.testing.expect(!try source.runLsmMaintenanceRoundBestEffort());
}

test "managed startup catch-up open disables optional runtimes and workers" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        null,
        null,
        0,
        null,
        .startup_catch_up,
    );
    defer db.close();

    try std.testing.expect(!db.start_index_workers);
    try std.testing.expect(db.enrichment_runtime == null);
    try std.testing.expect(db.ttl_runtime == null);
    try std.testing.expect(db.transaction_runtime == null);
    try std.testing.expect(db.text_merge_runtime == null);
}

test "managed startup catch-up uses provided indexes json without catalog fetch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-provided-indexes", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);
    const indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}";

    {
        var db = try openManagedDbWithIndexesJsonAndCacheMode(alloc, path, indexes_json, null, null, 0, null, .default);
        defer db.close();
    }

    const CountingCatalog = struct {
        calls: usize = 0,

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
            self.calls += 1;
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog = CountingCatalog{};
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, catalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    const result = try source.catchUpTableGroupBestEffortWithIndexesJson(alloc, 7001, "docs", indexes_json);

    try std.testing.expectEqual(@as(usize, 0), catalog.calls);
    try std.testing.expect(!result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expect(!statuses.items[0].stats.async_indexing.startup.active);
}

test "managed startup catch-up bypasses shared write cache" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);
}

test "managed startup catch-up invalidates stale cached writer status after replay clears debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-invalidates-stale-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    {
        var seeded = try openManagedDbWithIndexesJsonAndCacheMode(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
            null,
            null,
            0,
            null,
            .writer_no_replay,
        );
        defer seeded.close();
        _ = try seeded.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var db = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    const before = try db.db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, before);
    try std.testing.expectEqual(@as(usize, 1), before.indexes.len);
    try std.testing.expect(before.indexes[0].replay_catch_up_required);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(result.had_debt);
    try std.testing.expect(result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, statuses.items[0].stats.indexes[0].replay_target_sequence), statuses.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expect(!statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "managed startup catch-up defers while shared writer cache owns the table" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-defers-shared-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
}

test "managed startup catch-up defers while foreground writer state is dirty" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-defers-dirty-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    source.markWriteCacheDirty("docs");

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
}

test "write cache invalidation retires leased entry until release" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-lease-retire", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);

    write_cache.removeDbEntriesForTable("docs");
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), write_cache.retired_entries.items.len);

    cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);
}

test "write cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-retire-oom", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    write_cache.removeDbEntriesForTable("docs");

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len + write_cache.retired_entries.items.len);

    cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);
}

test "provisioned table write source group batch does not hold local db mutex during db batch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-group-batch-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const BatchProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeBatch(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batchGroupLocal(std.heap.page_allocator, 7001, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var probe = BatchProbe{};
    test_before_batch_execution_hook = .{
        .ptr = &probe,
        .run = BatchProbe.beforeBatch,
    };
    defer test_before_batch_execution_hook = null;

    var worker = BatchWorker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "provisioned table write source drop table does not hold local db mutex during background delete" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-table-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const DropProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeDeleteTree(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const DropWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropTable(std.heap.page_allocator, "docs", &.{7001}) catch |err| {
                self.err = err;
            };
        }
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), path);
    const marker_path = try std.fmt.allocPrint(alloc, "{s}/marker.txt", .{path});
    defer alloc.free(marker_path);
    var marker = try std.Io.Dir.cwd().createFile(io_impl.io(), marker_path, .{});
    marker.close(io_impl.io());

    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.backend_runtime = runtime.ptr();
    var probe = DropProbe{};
    test_before_drop_table_delete_hook = .{
        .ptr = &probe,
        .run = DropProbe.beforeDeleteTree,
    };
    defer test_before_drop_table_delete_hook = null;

    var worker = DropWorker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, DropWorker.run, .{&worker});
    thread.join();

    if (worker.err) |err| return err;

    while (!probe.entered.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    source.drainDroppedTableDeletes();

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), path, .{}));
}

test "provisioned table write source drop table waits for in-flight group batch on same table" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-table-waits-for-batch", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batchGroupLocal(std.heap.page_allocator, 7001, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            }) catch |err| {
                self.err = err;
            };
        }
    };

    const DropWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropTable(std.heap.page_allocator, "docs", &.{7001}) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var batch_probe = Probe{};
    var drop_probe = Probe{};
    test_before_batch_execution_hook = .{ .ptr = &batch_probe, .run = Probe.run };
    defer test_before_batch_execution_hook = null;
    test_before_drop_table_delete_hook = .{ .ptr = &drop_probe, .run = Probe.run };
    defer test_before_drop_table_delete_hook = null;

    var batch_worker = BatchWorker{ .source = &source };
    const batch_thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&batch_worker});
    while (!batch_probe.entered.load(.acquire)) std.atomic.spinLoopHint();

    var drop_worker = DropWorker{ .source = &source };
    const drop_thread = try std.Thread.spawn(.{}, DropWorker.run, .{&drop_worker});

    sleepNs(10 * std.time.ns_per_ms);
    try std.testing.expect(!drop_probe.entered.load(.acquire));

    batch_probe.release.store(true, .release);
    batch_thread.join();
    if (batch_worker.err) |err| return err;

    while (!drop_probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    drop_probe.release.store(true, .release);
    drop_thread.join();
    if (drop_worker.err) |err| return err;
}

test "provisioned table write source drop index does not hold local db mutex during index deletion work" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-index-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = tables_api.default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const Worker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropIndex(std.heap.page_allocator, "docs", "full_text_index_v0") catch |err| {
                self.err = err;
            };
        }
    };

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    var probe = Probe{};
    test_before_drop_index_work_hook = .{
        .ptr = &probe,
        .run = Probe.run,
    };
    defer test_before_drop_index_work_hook = null;

    var worker = Worker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    var reopened = try db_mod.DB.open(alloc, path, .{});
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.textIndex("full_text_index_v0") == null);
}

test "provisioned table write source create table provisions local indexes and schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-create-schema";
    const schema_json =
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = schema_json,
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, Catalog.iface());
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, schema_json),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);
    try std.testing.expect(db.core.schema != null);
}

test "provisioned table write source restore table does not hold local db mutex during restore work" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-table-restore-mutex";
    const backup_root = "/tmp/antfly-api-provisioned-table-restore-mutex-out";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const Worker = struct {
        source: *ProvisionedTableWriteSource,
        manifest: *const backups_api.TableBackupManifest,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().restoreTable(std.heap.page_allocator, "docs", .{
                .backup_root = backup_root,
                .manifest = self.manifest,
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var source = ProvisionedTableWriteSource.init(path, Catalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    var probe = Probe{};
    test_before_restore_work_hook = .{
        .ptr = &probe,
        .run = Probe.run,
    };
    defer test_before_restore_work_hook = null;

    var worker = Worker{
        .source = &source,
        .manifest = &manifest,
    };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});
    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "provisioned table write source deinit drains restore repair work group" {
    if (builtin.os.tag == .freestanding) return;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogFetch;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const DrainCtx = struct {
        started: std.atomic.Value(u32) = .init(0),
        finished: std.atomic.Value(u32) = .init(0),

        fn run(self: *@This()) !void {
            _ = self.started.fetchAdd(1, .release);
            sleepNs(20 * std.time.ns_per_ms);
            _ = self.finished.fetchAdd(1, .release);
        }
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-restore-repair-drain", NoCatalog.iface());

    var ctx = DrainCtx{};
    try source.restore_repair_work_group.concurrent(source.table_activity_threaded.io(), DrainCtx.run, .{&ctx});

    while (ctx.started.load(.acquire) == 0) std.Thread.yield() catch {};
    source.deinit();

    try std.testing.expectEqual(@as(u32, 1), ctx.started.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.finished.load(.acquire));
}

test "managed startup catch-up repairs external dense doc gaps from stored artifacts without replay debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-dense-artifacts", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dense_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}},{\"name\":\"ft_v1\",\"type\":\"full_text\",\"config\":{}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{path});
    defer alloc.free(dense_index_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(result.had_debt);
    try std.testing.expect(result.cleared_debt);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);

    var dense_doc_count: ?u64 = null;
    for (statuses.items[0].stats.indexes) |index| {
        if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
        dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 3), dense_doc_count);
}

test "managed startup catch-up defers while shared bulk ingest state is active" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-bulk", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    try write_cache.beginBulkIngestLocked("docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    var cached_seed = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached_seed.deinit(alloc);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    const hits_before = write_cache.hit_count.load(.monotonic);

    const first = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(first.busy);
    try std.testing.expect(!first.had_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
    const startup_hits_before = startup_write_cache.hit_count.load(.monotonic);

    const second = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(second.busy);
    try std.testing.expect(!second.had_debt);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    try std.testing.expectEqual(hits_before, write_cache.hit_count.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
    try std.testing.expectEqual(startup_hits_before, startup_write_cache.hit_count.load(.monotonic));
}

test "managed startup catch-up ignores stale dirty bit after writer cache entry is gone" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-stale-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();
    }

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;
    source.markWriteCacheDirty("docs");
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "managed status-only cache open skips shared bulk ingest session state" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-status-only-bulk", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    try write_cache.beginBulkIngestLocked("docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    var cached_seed = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached_seed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);

    var cached = try write_cache.getOrOpenLockedMode(path, Catalog.iface(), 7001, 0, "docs", .status_only);
    defer cached.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
}

test "managed source status-only open bypasses shared writer cache entry" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-source-status-only", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var seeded = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer seeded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);

    var status_only = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .status_only, null, null);
    defer status_only.deinit(alloc);

    try std.testing.expect(status_only.owned_db != null);
    try std.testing.expect(status_only.entry == null);
    try std.testing.expect(!status_only.db.start_index_workers);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);
}

test "write cache prunes stale generations instead of clearing current entries" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-generations", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const GenerationSource = struct {
        fn iface(value: *u64) table_reads.GroupLsmGenerationSource {
            return .{
                .ptr = value,
                .generation_for_group = generationForGroup,
            };
        }

        fn generationForGroup(ptr: *anyopaque, _: u64) u64 {
            return (@as(*u64, @ptrCast(@alignCast(ptr)))).*;
        }
    };

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.group_lsm_generation = GenerationSource.iface(&generation);

    var cached_first = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    cached_first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), write_cache.entries.items[0].lsm_root_generation);

    generation = 2;
    var cached_second = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    cached_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
}
