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
const scraping = @import("antfly_scraping");
const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_table_provisioner = @import("../../metadata/table_provisioner.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const common_secrets = @import("../../common/secrets.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const db_embedder = @import("../../storage/db/enrichment/embedder.zig");
const hbc_mod = @import("../../storage/hbc_adapter.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const platform_time = @import("../../platform/time.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const runtime_status = @import("../runtime_status.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const Io = std.Io;

const backend_current_root_generation: u64 = 0;

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

pub const ProvisionedTableReadCache = struct {
    alloc: std.mem.Allocator,
    threaded: Io.Threaded,
    lsm_cache: ?*lsm_backend.Cache = null,
    hbc_cache: ?*hbc_mod.Cache = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    inference_api_url: ?[]const u8 = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    hit_count: std.atomic.Value(u64) = .init(0),
    miss_count: std.atomic.Value(u64) = .init(0),
    mutex: Io.Mutex = .init,
    ready: Io.Condition = .init,
    table_epochs: std.StringHashMapUnmanaged(u64) = .empty,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    retired_entries: std.ArrayListUnmanaged(*Entry) = .empty,
    pending_opens: std.ArrayListUnmanaged(PendingOpen) = .empty,

    const max_cached_tables = 64;
    const pending_open_wait_poll_ns: u64 = 25 * std.time.ns_per_ms;
    const pending_open_wait_timeout_ns: u64 = 5 * std.time.ns_per_s;

    fn epochForTableLocked(self: *ProvisionedTableReadCache, table_name: []const u8) !u64 {
        const gop = try self.table_epochs.getOrPut(self.alloc, table_name);
        if (!gop.found_existing) {
            gop.key_ptr.* = self.alloc.dupe(u8, table_name) catch |err| {
                self.table_epochs.removeByPtr(gop.key_ptr);
                return err;
            };
            gop.value_ptr.* = 1;
        }
        return gop.value_ptr.*;
    }

    fn bumpEpochLocked(self: *ProvisionedTableReadCache, table_name: []const u8) void {
        if (self.table_epochs.getPtr(table_name)) |epoch| epoch.* +%= 1;
    }

    pub const CacheStats = struct {
        hit_count: u64 = 0,
        miss_count: u64 = 0,
    };

    const Entry = struct {
        group_id: u64,
        lsm_root_generation: u64,
        identity_namespace: ?db_mod.DocIdentityNamespace = null,
        table_name: []u8,
        db: db_mod.DB,
        active_leases: usize = 0,
        retired: bool = false,

        fn deinit(self: *Entry, alloc: std.mem.Allocator) void {
            self.db.close();
            alloc.free(self.table_name);
            self.* = undefined;
        }
    };

    pub const Lease = struct {
        cache: *ProvisionedTableReadCache,
        entry: ?*Entry,
        db: *db_mod.DB,

        pub fn release(self: *Lease) void {
            const entry = self.entry orelse return;
            self.cache.releaseEntry(entry);
            self.entry = null;
        }
    };

    const PendingOpen = struct {
        group_id: u64,
        identity_namespace: ?db_mod.DocIdentityNamespace = null,
        table_name: []u8,

        fn deinit(self: *PendingOpen, alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            self.* = undefined;
        }
    };

    pub fn init(alloc: std.mem.Allocator) ProvisionedTableReadCache {
        return .{
            .alloc = alloc,
            .threaded = Io.Threaded.init(alloc, .{}),
        };
    }

    pub fn deinit(self: *ProvisionedTableReadCache) void {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
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
        for (self.pending_opens.items) |*pending| pending.deinit(self.alloc);
        self.pending_opens.deinit(self.alloc);
        var epoch_keys = self.table_epochs.keyIterator();
        while (epoch_keys.next()) |key| self.alloc.free(key.*);
        self.table_epochs.deinit(self.alloc);
        self.mutex.unlock(io);
        self.threaded.deinit();
        self.* = undefined;
    }

    pub fn cacheStats(self: *const ProvisionedTableReadCache) CacheStats {
        return .{
            .hit_count = self.hit_count.load(.monotonic),
            .miss_count = self.miss_count.load(.monotonic),
        };
    }

    pub fn getOrOpen(
        self: *ProvisionedTableReadCache,
        path: []const u8,
        catalog: table_catalog.CatalogSource,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !Lease {
        const io = self.threaded.io();
        var stale_epoch_retries: u8 = 0;
        var pending_open_wait_started_ns: u64 = 0;
        while (true) {
            const identity_namespace = try loadTableIdentityNamespaceForGroup(self.alloc, catalog, table_name, group_id);
            self.mutex.lockUncancelable(io);
            const open_epoch = self.epochForTableLocked(table_name) catch |err| {
                self.mutex.unlock(io);
                return err;
            };
            if (self.findEntryForNamespaceLocked(group_id, lsm_root_generation, identity_namespace, table_name)) |entry| {
                entry.active_leases += 1;
                _ = self.hit_count.fetchAdd(1, .monotonic);
                self.mutex.unlock(io);
                return .{
                    .cache = self,
                    .entry = entry,
                    .db = &entry.db,
                };
            }
            if (self.hasPendingOpenForNamespaceLocked(group_id, identity_namespace, table_name)) {
                self.mutex.unlock(io);
                const now_ns = platform_time.monotonicNs();
                if (pending_open_wait_started_ns == 0) pending_open_wait_started_ns = now_ns;
                const waited_ns = now_ns -| pending_open_wait_started_ns;
                if (waited_ns >= pending_open_wait_timeout_ns) return error.TableReadChurn;
                io.sleep(Io.Duration.fromNanoseconds(@min(pending_open_wait_poll_ns, pending_open_wait_timeout_ns - waited_ns)), .awake) catch {};
                continue;
            }
            pending_open_wait_started_ns = 0;
            const owned_pending_name = try self.alloc.dupe(u8, table_name);
            var pending_name_owned_locally = true;
            errdefer if (pending_name_owned_locally) self.alloc.free(owned_pending_name);
            try self.pending_opens.append(self.alloc, .{
                .group_id = group_id,
                .identity_namespace = identity_namespace,
                .table_name = owned_pending_name,
            });
            pending_name_owned_locally = false;
            self.mutex.unlock(io);
            _ = self.miss_count.fetchAdd(1, .monotonic);

            var db = openProvisionedQueryDbForTableWithCache(
                self.alloc,
                path,
                catalog,
                table_name,
                self.lsm_cache,
                self.hbc_cache,
                lsm_root_generation,
                self.resource_manager,
                self.backend_runtime,
                self.antfly_provider,
                self.inference_api_url,
                self.secret_store,
                self.remote_content,
                identity_namespace,
            ) catch |err| {
                self.mutex.lockUncancelable(io);
                self.removePendingOpenForNamespaceLocked(group_id, identity_namespace, table_name);
                self.ready.broadcast(io);
                self.mutex.unlock(io);
                return err;
            };
            errdefer db.close();

            self.mutex.lockUncancelable(io);
            self.removePendingOpenForNamespaceLocked(group_id, identity_namespace, table_name);
            if ((self.table_epochs.get(table_name) orelse open_epoch +% 1) != open_epoch) {
                stale_epoch_retries += 1;
                self.ready.broadcast(io);
                if (stale_epoch_retries > 2) {
                    self.mutex.unlock(io);
                    return error.TableReadChurn;
                }
                db.close();
                self.mutex.unlock(io);
                continue;
            }
            if (self.findEntryForNamespaceLocked(group_id, lsm_root_generation, identity_namespace, table_name)) |entry| {
                entry.active_leases += 1;
                _ = self.hit_count.fetchAdd(1, .monotonic);
                self.ready.broadcast(io);
                db.close();
                self.mutex.unlock(io);
                return .{
                    .cache = self,
                    .entry = entry,
                    .db = &entry.db,
                };
            }

            if (!self.hasTableLocked(table_name) and self.cachedTableCountLocked() >= max_cached_tables) self.evictOldestTableLocked();
            const owned_table_name = try self.alloc.dupe(u8, table_name);
            errdefer self.alloc.free(owned_table_name);
            try self.retired_entries.ensureUnusedCapacity(self.alloc, 1);
            const owned_entry = try self.alloc.create(Entry);
            errdefer self.alloc.destroy(owned_entry);
            owned_entry.* = .{
                .group_id = group_id,
                .lsm_root_generation = lsm_root_generation,
                .identity_namespace = identity_namespace,
                .table_name = owned_table_name,
                .db = db,
                .active_leases = 1,
            };
            try self.entries.append(self.alloc, owned_entry);
            self.ready.broadcast(io);
            const opened = self.entries.items[self.entries.items.len - 1];
            self.mutex.unlock(io);
            return .{
                .cache = self,
                .entry = opened,
                .db = &opened.db,
            };
        }
    }

    pub fn getIfPresent(
        self: *ProvisionedTableReadCache,
        group_id: u64,
        lsm_root_generation: u64,
        identity_namespace: ?db_mod.DocIdentityNamespace,
        table_name: []const u8,
    ) ?Lease {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        if (self.findEntryForNamespaceLocked(group_id, lsm_root_generation, identity_namespace, table_name)) |entry| {
            entry.active_leases += 1;
            _ = self.hit_count.fetchAdd(1, .monotonic);
            self.mutex.unlock(io);
            return .{
                .cache = self,
                .entry = entry,
                .db = &entry.db,
            };
        }
        self.mutex.unlock(io);
        return null;
    }

    pub fn ensureOpen(
        self: *ProvisionedTableReadCache,
        path: []const u8,
        catalog: table_catalog.CatalogSource,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !Lease {
        return self.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
    }

    pub fn invalidateTable(self: *ProvisionedTableReadCache, table_name: []const u8) void {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.bumpEpochLocked(table_name);
        self.removeEntriesForTableLocked(table_name);
        self.ready.broadcast(io);
    }

    pub fn snapshotRuntimeStatuses(
        self: *ProvisionedTableReadCache,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return try self.snapshotRuntimeStatusesLocked(alloc, table_name);
    }

    pub fn clear(self: *ProvisionedTableReadCache) void {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var epochs = self.table_epochs.valueIterator();
        while (epochs.next()) |epoch| epoch.* +%= 1;
        for (self.entries.items) |entry| self.retireEntryLocked(entry);
        self.entries.clearRetainingCapacity();
        self.ready.broadcast(io);
    }

    fn findEntryLocked(
        self: *ProvisionedTableReadCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) ?*Entry {
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) return entry;
        }
        return null;
    }

    fn findEntryForNamespaceLocked(
        self: *ProvisionedTableReadCache,
        group_id: u64,
        lsm_root_generation: u64,
        identity_namespace: ?db_mod.DocIdentityNamespace,
        table_name: []const u8,
    ) ?*Entry {
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and
                entry.lsm_root_generation == lsm_root_generation and
                identityNamespacesEqual(entry.identity_namespace, identity_namespace) and
                std.mem.eql(u8, entry.table_name, table_name)) return entry;
        }
        return null;
    }

    fn hasPendingOpenLocked(
        self: *ProvisionedTableReadCache,
        group_id: u64,
        table_name: []const u8,
    ) bool {
        for (self.pending_opens.items) |pending| {
            if (pending.group_id == group_id and std.mem.eql(u8, pending.table_name, table_name)) return true;
        }
        return false;
    }

    fn hasPendingOpenForNamespaceLocked(
        self: *ProvisionedTableReadCache,
        group_id: u64,
        identity_namespace: ?db_mod.DocIdentityNamespace,
        table_name: []const u8,
    ) bool {
        for (self.pending_opens.items) |pending| {
            if (pending.group_id == group_id and
                identityNamespacesEqual(pending.identity_namespace, identity_namespace) and
                std.mem.eql(u8, pending.table_name, table_name)) return true;
        }
        return false;
    }

    fn removePendingOpenLocked(
        self: *ProvisionedTableReadCache,
        group_id: u64,
        table_name: []const u8,
    ) void {
        var i: usize = 0;
        while (i < self.pending_opens.items.len) {
            const pending = self.pending_opens.items[i];
            if (pending.group_id == group_id and std.mem.eql(u8, pending.table_name, table_name)) {
                var removed = self.pending_opens.orderedRemove(i);
                removed.deinit(self.alloc);
                return;
            }
            i += 1;
        }
    }

    fn removePendingOpenForNamespaceLocked(
        self: *ProvisionedTableReadCache,
        group_id: u64,
        identity_namespace: ?db_mod.DocIdentityNamespace,
        table_name: []const u8,
    ) void {
        var i: usize = 0;
        while (i < self.pending_opens.items.len) {
            const pending = self.pending_opens.items[i];
            if (pending.group_id == group_id and
                identityNamespacesEqual(pending.identity_namespace, identity_namespace) and
                std.mem.eql(u8, pending.table_name, table_name))
            {
                var removed = self.pending_opens.orderedRemove(i);
                removed.deinit(self.alloc);
                return;
            }
            i += 1;
        }
    }

    fn hasTableLocked(self: *ProvisionedTableReadCache, table_name: []const u8) bool {
        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, entry.table_name, table_name)) return true;
        }
        return false;
    }

    fn cachedTableCountLocked(self: *ProvisionedTableReadCache) usize {
        var count: usize = 0;
        for (self.entries.items, 0..) |entry, i| {
            var seen = false;
            for (self.entries.items[0..i]) |prior| {
                if (std.mem.eql(u8, prior.table_name, entry.table_name)) {
                    seen = true;
                    break;
                }
            }
            if (!seen) count += 1;
        }
        return count;
    }

    fn removeEntriesForTableLocked(self: *ProvisionedTableReadCache, table_name: []const u8) void {
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

    fn snapshotRuntimeStatusesLocked(
        self: *ProvisionedTableReadCache,
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

    fn evictOldestTableLocked(self: *ProvisionedTableReadCache) void {
        if (self.entries.items.len == 0) return;
        const oldest = self.entries.orderedRemove(0);
        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (!std.mem.eql(u8, self.entries.items[i].table_name, oldest.table_name)) {
                i += 1;
                continue;
            }
            const removed = self.entries.orderedRemove(i);
            self.retireEntryLocked(removed);
        }
        self.retireEntryLocked(oldest);
    }

    fn releaseEntry(self: *ProvisionedTableReadCache, entry: *Entry) void {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        std.debug.assert(entry.active_leases > 0);
        entry.active_leases -= 1;
        if (entry.active_leases == 0 and entry.retired) {
            self.destroyRetiredEntryLocked(entry);
        }
    }

    fn retireEntryLocked(self: *ProvisionedTableReadCache, entry: *Entry) void {
        if (entry.retired) return;
        entry.retired = true;
        if (entry.active_leases == 0) {
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
            return;
        }
        self.retired_entries.appendAssumeCapacity(entry);
    }

    fn destroyRetiredEntryLocked(self: *ProvisionedTableReadCache, entry: *Entry) void {
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

fn identityNamespacesEqual(left: ?db_mod.DocIdentityNamespace, right: ?db_mod.DocIdentityNamespace) bool {
    if (left == null or right == null) return left == null and right == null;
    return left.?.eql(right.?);
}

pub fn openProvisionedQueryDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_root_generation: u64,
) !db_mod.DB {
    return try openProvisionedQueryDbForTableWithCache(alloc, path, catalog, table_name, null, null, lsm_root_generation, null, null, null, null, null, null);
}

pub fn openProvisionedQueryDbForTableWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openProvisionedQueryDbForTableWithCache(
        alloc,
        path,
        catalog,
        table_name,
        null,
        null,
        lsm_root_generation,
        null,
        backend_runtime,
        null,
        null,
        null,
        null,
        try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id),
    );
}

pub fn openProvisionedQueryDbForTableWithCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    inference_api_url: ?[]const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    identity_namespace: ?db_mod.DocIdentityNamespace,
) !db_mod.DB {
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse {
        var db = try db_mod.DB.open(alloc, path, .{
            .open_mode = .query_readonly,
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .secret_store = secret_store,
            .remote_content = remote_content,
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
        });
        errdefer db.close();
        try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
        return db;
    };
    defer alloc.free(indexes_json);

    const EnrichmentSet = struct {
        dense: ?db_embedder.DenseEmbedder = null,
        sparse: ?db_embedder.SparseEmbedder = null,
        generated: bool = false,

        fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (self.dense) |owned| owned.deinit(allocator);
            if (self.sparse) |owned| owned.deinit(allocator);
        }

        fn enabled(self: @This()) bool {
            return self.dense != null or self.sparse != null or self.generated;
        }

        fn config(self: @This()) db_mod.enrichment_runtime.Config {
            return .{
                .dense_embedder = self.dense,
                .sparse_embedder = self.sparse,
                .asset_producer = null,
                .enable_without_producers = self.generated,
            };
        }

        fn take(self: *@This()) void {
            self.dense = null;
            self.sparse = null;
            self.generated = false;
        }
    };

    const createEnrichments = struct {
        fn run(
            allocator: std.mem.Allocator,
            raw_indexes_json: []const u8,
            runtime: ?*db_mod.background_runtime.BackendRuntime,
            local_provider: ?managed_embedder.AntflyProvider,
            local_inference_api_url: ?[]const u8,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
        ) !EnrichmentSet {
            _ = runtime;
            const dense = try managed_embedder.ManagedEmbedder.createDenseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote, .inference_api_url = local_inference_api_url });
            errdefer if (dense) |owned| owned.deinit(allocator);
            const sparse = try managed_embedder.ManagedEmbedder.createSparseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote, .inference_api_url = local_inference_api_url });
            errdefer if (sparse) |owned| owned.deinit(allocator);
            const generated = try indexesJsonHasGeneratedEnrichment(allocator, raw_indexes_json);
            return .{
                .dense = dense,
                .sparse = sparse,
                .generated = generated,
            };
        }
    }.run;

    var enrichments = try createEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, inference_api_url, secret_store, remote_content);
    defer enrichments.deinit(alloc);

    var db = if (enrichments.enabled()) blk: {
        const enrichment_cfg = enrichments.config();
        const opened = try db_mod.DB.open(alloc, path, .{
            .open_mode = .query_readonly,
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .secret_store = secret_store,
            .remote_content = remote_content,
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
            .enrichment = enrichment_cfg,
        });
        if (opened.enrichment_runtime != null) enrichments.take();
        break :blk opened;
    } else try db_mod.DB.open(alloc, path, .{
        .open_mode = .query_readonly,
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
        .secret_store = secret_store,
        .remote_content = remote_content,
        .identity_namespace = identity_namespace,
        .prefer_existing_identity_namespace = identity_namespace != null,
    });
    errdefer db.close();
    try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
    const summary = try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, &db, indexes_json, .{
        .drain_resolver_backfill = false,
    });
    if (summary.indexManagerCatalogChanged()) return error.ReadOnly;
    return db;
}

pub fn openProvisionedWarmStatusDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?db_mod.DocIdentityNamespace,
) !db_mod.DB {
    var db = try db_mod.DB.open(alloc, path, .{
        .open_mode = .status_only,
        .lsm_root_generation = lsm_root_generation,
        .backend_runtime = backend_runtime,
        .identity_namespace = identity_namespace,
        .prefer_existing_identity_namespace = identity_namespace != null,
    });
    errdefer db.close();
    try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
    return db;
}

pub fn openProvisionedLookupDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?db_mod.DocIdentityNamespace,
) !db_mod.DB {
    var db = try db_mod.DB.open(alloc, path, .{
        .open_mode = .status_only,
        .lsm_cache = lsm_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
        .identity_namespace = identity_namespace,
        .prefer_existing_identity_namespace = identity_namespace != null,
    });
    errdefer db.close();
    try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
    return db;
}

pub fn loadTableIndexesJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.indexes_json);
}

fn indexesJsonHasGeneratedEnrichment(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    return jsonValueHasGeneratedEnrichment(parsed.value);
}

fn jsonValueHasGeneratedEnrichment(value: std.json.Value) bool {
    switch (value) {
        .object => |object| {
            if (object.get("kind")) |kind| {
                if (kind == .string and (std.mem.eql(u8, kind.string, "asset") or std.mem.eql(u8, kind.string, "chunk"))) return true;
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (jsonValueHasGeneratedEnrichment(entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (jsonValueHasGeneratedEnrichment(item)) return true;
            }
            return false;
        },
        else => return false,
    }
}

pub fn loadTableIdentityNamespaceForGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
) !?db_mod.DocIdentityNamespace {
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

pub fn validateProvisionedDbIdentityNamespace(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    db: *const db_mod.DB,
) !void {
    const expected = (try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id)) orelse return;
    try validateOpenedProvisionedDbIdentityNamespace(db, expected);
}

pub fn validateOpenedProvisionedDbIdentityNamespace(
    db: *const db_mod.DB,
    expected: ?db_mod.DocIdentityNamespace,
) !void {
    const namespace = expected orelse return;
    if (!db.core.identity_namespace.eql(namespace)) return error.DocIdentityNamespaceMismatch;
}

test "provisioned query runtime db opens with catalog identity namespace" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-runtime-identity-namespace");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const CatalogState = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
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
                    .range_id = 7105,
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

    var catalog_state = CatalogState{};
    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog_state.iface(), "docs", 7001, backend_current_root_generation, null);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(.{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7105,
    }));
}

test "provisioned query runtime db rejects stale identity namespace" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-runtime-stale-identity-namespace");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const stale_namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7197,
    };
    {
        var db = try db_mod.DB.open(alloc, path, .{
            .start_index_workers = false,
            .identity_namespace = stale_namespace,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        db.close();
    }

    const CatalogState = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
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
                    .range_id = 7105,
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

    var catalog_state = CatalogState{};
    if (openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog_state.iface(), "docs", 7001, backend_current_root_generation, null)) |opened| {
        var db = opened;
        db.close();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.DocIdentityNamespaceMismatch, err);
}

test "provisioned lookup db opens with identity namespace" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-lookup-identity-namespace");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7103,
    };
    var db = try openProvisionedLookupDbForTable(alloc, path, null, backend_current_root_generation, null, null, namespace);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(namespace));
}

test "provisioned warm status db opens with identity namespace" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-warm-status-identity-namespace");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7104,
    };
    var db = try openProvisionedWarmStatusDbForTable(alloc, path, backend_current_root_generation, null, namespace);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(namespace));
}

test "provisioned direct read db opens reject stale identity namespace" {
    const alloc = std.testing.allocator;
    const lookup_path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-lookup-stale-identity-namespace");
    defer alloc.free(lookup_path);
    const status_path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-status-stale-identity-namespace");
    defer alloc.free(status_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), lookup_path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), status_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), lookup_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), status_path) catch {};

    const stale_namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7198,
    };
    const expected_namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7199,
    };

    {
        var db = try db_mod.DB.open(alloc, lookup_path, .{
            .start_index_workers = false,
            .identity_namespace = stale_namespace,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        db.close();
    }
    if (openProvisionedLookupDbForTable(alloc, lookup_path, null, backend_current_root_generation, null, null, expected_namespace)) |opened| {
        var db = opened;
        db.close();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.DocIdentityNamespaceMismatch, err);

    {
        var db = try db_mod.DB.open(alloc, status_path, .{
            .start_index_workers = false,
            .identity_namespace = stale_namespace,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        db.close();
    }
    if (openProvisionedWarmStatusDbForTable(alloc, status_path, backend_current_root_generation, null, expected_namespace)) |opened| {
        var db = opened;
        db.close();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.DocIdentityNamespaceMismatch, err);
}

test "provisioned read cache keys entries by lsm root generation" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-cache-generation");
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

    var lsm_cache = lsm_backend.Cache.init(alloc, lsm_backend.DefaultCacheSizeBytes);
    defer lsm_cache.deinit();
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    cache.lsm_cache = &lsm_cache;

    var lease1 = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 1, "docs");
    defer lease1.release();
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);

    var lease2 = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 1, "docs");
    defer lease2.release();
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);

    var lease3 = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 2, "docs");
    defer lease3.release();
    try std.testing.expectEqual(@as(usize, 2), cache.entries.items.len);
}

test "provisioned read cache keys entries by identity namespace" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-cache-identity-namespace");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const CatalogState = struct {
        range_id: u64,

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
                    .range_id = self.range_id,
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

    var catalog_state = CatalogState{ .range_id = 7101 };
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();

    var lease1 = try cache.getOrOpen(path, catalog_state.iface(), 7001, 1, "docs");
    defer lease1.release();
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 7101), lease1.db.core.identity_namespace.range_id);

    catalog_state.range_id = 7102;
    try std.testing.expect(cache.getIfPresent(7001, 1, .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7102,
    }, "docs") == null);
    var lease2 = try cache.getOrOpen(path, catalog_state.iface(), 7001, 1, "docs");
    defer lease2.release();
    try std.testing.expectEqual(@as(usize, 2), cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 7102), lease2.db.core.identity_namespace.range_id);
}

test "provisioned read cache invalidates repeated ownership moves with pinned leases" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-cache-ownership-moves");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const CatalogState = struct {
        range_id: u64,

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
                    .range_id = self.range_id,
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

    var catalog_state = CatalogState{ .range_id = 7201 };
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();

    var first = try cache.getOrOpen(path, catalog_state.iface(), 7001, 1, "docs");
    defer first.release();
    try std.testing.expectEqual(@as(u64, 7201), first.db.core.identity_namespace.range_id);
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);

    cache.invalidateTable("docs");
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), cache.retired_entries.items.len);
    try std.testing.expect(cache.getIfPresent(7001, 1, .{ .table_id = 7, .shard_id = 7001, .range_id = 7201 }, "docs") == null);

    catalog_state.range_id = 7202;
    var second = try cache.getOrOpen(path, catalog_state.iface(), 7001, 1, "docs");
    defer second.release();
    try std.testing.expectEqual(@as(u64, 7202), second.db.core.identity_namespace.range_id);
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), cache.retired_entries.items.len);

    cache.invalidateTable("docs");
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 2), cache.retired_entries.items.len);

    catalog_state.range_id = 7203;
    var third = try cache.getOrOpen(path, catalog_state.iface(), 7001, 1, "docs");
    defer third.release();
    try std.testing.expectEqual(@as(u64, 7203), third.db.core.identity_namespace.range_id);
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 2), cache.retired_entries.items.len);

    first.release();
    try std.testing.expectEqual(@as(usize, 1), cache.retired_entries.items.len);
    second.release();
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);
}

test "provisioned read cache clear preserves in-flight pending opens and bumps table epoch" {
    const alloc = std.testing.allocator;

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();

    try cache.pending_opens.append(alloc, .{
        .group_id = 7001,
        .table_name = try alloc.dupe(u8, "docs"),
    });

    const io = cache.threaded.io();
    cache.mutex.lockUncancelable(io);
    const before_epoch = try cache.epochForTableLocked("docs");
    cache.mutex.unlock(io);

    cache.clear();

    try std.testing.expectEqual(before_epoch +% 1, cache.table_epochs.get("docs").?);
    try std.testing.expectEqual(@as(usize, 1), cache.pending_opens.items.len);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expect(cache.hasPendingOpenLocked(7001, "docs"));
}

test "provisioned read cache invalidate removes entries without dropping pending opens" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-cache-invalidate");
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

    var lsm_cache = lsm_backend.Cache.init(alloc, lsm_backend.DefaultCacheSizeBytes);
    defer lsm_cache.deinit();
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    cache.lsm_cache = &lsm_cache;

    var lease = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 1, "docs");
    defer lease.release();
    try cache.pending_opens.append(alloc, .{
        .group_id = 7001,
        .table_name = try alloc.dupe(u8, "docs"),
    });

    const before_epoch = cache.table_epochs.get("docs").?;
    cache.invalidateTable("docs");

    try std.testing.expectEqual(before_epoch +% 1, cache.table_epochs.get("docs").?);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expect(cache.hasPendingOpenLocked(7001, "docs"));
}

test "provisioned read cache retires invalidated entries until the last lease is released" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-cache-no-retire");
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

    var lsm_cache = lsm_backend.Cache.init(alloc, lsm_backend.DefaultCacheSizeBytes);
    defer lsm_cache.deinit();
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    cache.lsm_cache = &lsm_cache;

    var lease = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 1, "docs");
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);

    cache.invalidateTable("docs");
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), cache.retired_entries.items.len);

    lease.release();
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);

    var reopened = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 1, "docs");
    defer reopened.release();
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);

    cache.clear();
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), cache.retired_entries.items.len);

    reopened.release();
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);
}

test "provisioned read cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();
    const path = try uniqueTestTmpPathAlloc(std.testing.allocator, "antfly-api-provisioned-read-cache-retire-oom");
    defer std.testing.allocator.free(path);

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

    var lsm_cache = lsm_backend.Cache.init(alloc, lsm_backend.DefaultCacheSizeBytes);
    defer lsm_cache.deinit();
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    cache.lsm_cache = &lsm_cache;

    var lease = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 1, "docs");
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    cache.invalidateTable("docs");

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len + cache.retired_entries.items.len);

    lease.release();
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);
}
