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
const introducer_mod = @import("../../introducer.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const index_manager_mod = @import("../../storage/db/catalog/index_manager.zig");
const hbc_mod = @import("../../storage/hbc_adapter.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const platform_time = @import("antfly_platform").time;
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const storage_schema = @import("../../storage/schema.zig");
const runtime_status = @import("../runtime_status.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const Io = std.Io;

const backend_current_root_generation: u64 = 0;
const ManagedReadRuntimeConfig = @import("core.zig").ManagedReadRuntimeConfig;

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
    exclusive_table_access: std.StringHashMapUnmanaged(usize) = .empty,
    exclusive_group_access: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    retired_entries: std.ArrayListUnmanaged(*Entry) = .empty,
    pending_opens: std.ArrayListUnmanaged(PendingOpen) = .empty,

    const max_cached_tables = 64;
    const pending_open_wait_poll_ns: u64 = 25 * std.time.ns_per_ms;
    const pending_open_wait_timeout_ns: u64 = 5 * std.time.ns_per_s;
    const exclusive_wait_poll_ns: u64 = 25 * std.time.ns_per_ms;
    const exclusive_wait_timeout_ns: u64 = 30 * std.time.ns_per_s;

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

    pub const ExclusiveTableAccess = struct {
        cache: *ProvisionedTableReadCache,
        table_name: []const u8,
        active: bool = true,

        pub fn deinit(self: *ExclusiveTableAccess) void {
            if (!self.active) return;
            self.cache.endExclusiveTableAccess(self.table_name);
            self.active = false;
        }
    };

    pub const ExclusiveGroupAccess = struct {
        cache: *ProvisionedTableReadCache,
        group_id: u64,
        active: bool = true,

        pub fn deinit(self: *ExclusiveGroupAccess) void {
            if (!self.active) return;
            self.cache.endExclusiveGroupAccess(self.group_id);
            self.active = false;
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
        var exclusive_keys = self.exclusive_table_access.keyIterator();
        while (exclusive_keys.next()) |key| self.alloc.free(key.*);
        self.exclusive_table_access.deinit(self.alloc);
        self.exclusive_group_access.deinit(self.alloc);
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

    fn managedReadRuntimeConfig(self: *const ProvisionedTableReadCache) ManagedReadRuntimeConfig {
        return .{
            .backend_runtime = self.backend_runtime,
            .antfly_provider = self.antfly_provider,
            .inference_api_url = self.inference_api_url,
            .secret_store = self.secret_store,
            .remote_content = self.remote_content,
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
        var exclusive_wait_started_ns: u64 = 0;
        while (true) {
            // Reload every attempt because a stale-epoch retry can mean the
            // table identity changed while the previous open was in flight.
            const identity_namespace = try loadTableIdentityNamespaceForGroup(self.alloc, catalog, table_name, group_id);
            self.mutex.lockUncancelable(io);
            if (self.hasExclusiveTableAccessLocked(table_name) or self.hasExclusiveGroupAccessLocked(group_id)) {
                self.mutex.unlock(io);
                const now_ns = platform_time.monotonicNs();
                if (exclusive_wait_started_ns == 0) exclusive_wait_started_ns = now_ns;
                const waited_ns = now_ns -| exclusive_wait_started_ns;
                if (waited_ns >= exclusive_wait_timeout_ns) return error.TableReadTransitionTimeout;
                io.sleep(Io.Duration.fromNanoseconds(@min(exclusive_wait_poll_ns, exclusive_wait_timeout_ns - waited_ns)), .awake) catch {};
                continue;
            }
            exclusive_wait_started_ns = 0;
            const open_table_epoch = self.epochForTableLocked(table_name) catch |err| {
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
                self.managedReadRuntimeConfig(),
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
            if ((self.table_epochs.get(table_name) orelse open_table_epoch +% 1) != open_table_epoch or
                self.hasExclusiveGroupAccessLocked(group_id))
            {
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
            const retirement_capacity = try std.math.add(
                usize,
                try std.math.add(usize, self.entries.items.len, self.retired_entries.items.len),
                1,
            );
            try self.retired_entries.ensureTotalCapacity(self.alloc, retirement_capacity);
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

    pub fn beginExclusiveTableAccess(self: *ProvisionedTableReadCache, table_name: []const u8) !ExclusiveTableAccess {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        errdefer self.mutex.unlock(io);

        const gop = try self.exclusive_table_access.getOrPut(self.alloc, table_name);
        if (!gop.found_existing) {
            gop.key_ptr.* = self.alloc.dupe(u8, table_name) catch |err| {
                self.exclusive_table_access.removeByPtr(gop.key_ptr);
                return err;
            };
            gop.value_ptr.* = 0;
        }
        gop.value_ptr.* += 1;

        self.bumpEpochLocked(table_name);
        self.removeEntriesForTableLocked(table_name);
        self.ready.broadcast(io);
        const drain_started_ns = platform_time.monotonicNs();
        while (self.hasPendingOpenForTableLocked(table_name) or
            self.hasTableLocked(table_name) or
            self.hasRetiredEntryForTableLocked(table_name))
        {
            const waited_ns = platform_time.monotonicNs() -| drain_started_ns;
            if (waited_ns >= exclusive_wait_timeout_ns) {
                const pending_opens = self.pendingOpenCountForTableLocked(table_name);
                const retired_entries = self.retiredEntryCountForTableLocked(table_name);
                const active_leases = self.activeLeaseCountForTableLocked(table_name);
                self.releaseExclusiveTableAccessLocked(table_name);
                self.ready.broadcast(io);
                std.log.err("table read generation drain timed out table={s} pending_opens={} retired_entries={} active_leases={} wait_ms={}", .{
                    table_name,
                    pending_opens,
                    retired_entries,
                    active_leases,
                    @divTrunc(waited_ns, std.time.ns_per_ms),
                });
                return error.TableReadDrainTimeout;
            }
            self.mutex.unlock(io);
            io.sleep(Io.Duration.fromNanoseconds(@min(exclusive_wait_poll_ns, exclusive_wait_timeout_ns - waited_ns)), .awake) catch {};
            self.mutex.lockUncancelable(io);
        }
        self.mutex.unlock(io);

        return .{
            .cache = self,
            .table_name = table_name,
        };
    }

    pub fn beginExclusiveGroupAccess(self: *ProvisionedTableReadCache, group_id: u64) !ExclusiveGroupAccess {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        errdefer self.mutex.unlock(io);

        const gop = try self.exclusive_group_access.getOrPut(self.alloc, group_id);
        if (gop.found_existing) {
            gop.value_ptr.* = std.math.add(usize, gop.value_ptr.*, 1) catch return error.TooManyExclusiveReaders;
        } else {
            gop.value_ptr.* = 1;
        }
        self.removeEntriesForGroupLocked(group_id);
        self.ready.broadcast(io);

        const drain_started_ns = platform_time.monotonicNs();
        while (self.hasPendingOpenForGroupLocked(group_id) or self.hasGroupLocked(group_id) or self.hasRetiredEntryForGroupLocked(group_id)) {
            const waited_ns = platform_time.monotonicNs() -| drain_started_ns;
            if (waited_ns >= exclusive_wait_timeout_ns) {
                self.releaseExclusiveGroupAccessLocked(group_id);
                self.ready.broadcast(io);
                return error.TableReadDrainTimeout;
            }
            self.mutex.unlock(io);
            io.sleep(Io.Duration.fromNanoseconds(@min(exclusive_wait_poll_ns, exclusive_wait_timeout_ns - waited_ns)), .awake) catch {};
            self.mutex.lockUncancelable(io);
        }
        self.mutex.unlock(io);
        return .{ .cache = self, .group_id = group_id };
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

    fn hasPendingOpenForTableLocked(
        self: *ProvisionedTableReadCache,
        table_name: []const u8,
    ) bool {
        for (self.pending_opens.items) |pending| {
            if (std.mem.eql(u8, pending.table_name, table_name)) return true;
        }
        return false;
    }

    fn hasExclusiveTableAccessLocked(
        self: *ProvisionedTableReadCache,
        table_name: []const u8,
    ) bool {
        return self.exclusive_table_access.get(table_name) != null;
    }

    fn hasExclusiveGroupAccessLocked(self: *ProvisionedTableReadCache, group_id: u64) bool {
        return self.exclusive_group_access.get(group_id) != null;
    }

    fn hasPendingOpenForGroupLocked(self: *ProvisionedTableReadCache, group_id: u64) bool {
        for (self.pending_opens.items) |pending| if (pending.group_id == group_id) return true;
        return false;
    }

    fn hasGroupLocked(self: *ProvisionedTableReadCache, group_id: u64) bool {
        for (self.entries.items) |entry| if (entry.group_id == group_id) return true;
        return false;
    }

    fn hasRetiredEntryForGroupLocked(self: *ProvisionedTableReadCache, group_id: u64) bool {
        for (self.retired_entries.items) |entry| if (entry.group_id == group_id) return true;
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

    fn hasRetiredEntryForTableLocked(self: *ProvisionedTableReadCache, table_name: []const u8) bool {
        for (self.retired_entries.items) |entry| {
            if (std.mem.eql(u8, entry.table_name, table_name)) return true;
        }
        return false;
    }

    fn pendingOpenCountForTableLocked(self: *ProvisionedTableReadCache, table_name: []const u8) usize {
        var count: usize = 0;
        for (self.pending_opens.items) |pending| {
            if (std.mem.eql(u8, pending.table_name, table_name)) count += 1;
        }
        return count;
    }

    fn retiredEntryCountForTableLocked(self: *ProvisionedTableReadCache, table_name: []const u8) usize {
        var count: usize = 0;
        for (self.retired_entries.items) |entry| {
            if (std.mem.eql(u8, entry.table_name, table_name)) count += 1;
        }
        return count;
    }

    fn activeLeaseCountForTableLocked(self: *ProvisionedTableReadCache, table_name: []const u8) usize {
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, entry.table_name, table_name)) count +|= entry.active_leases;
        }
        for (self.retired_entries.items) |entry| {
            if (std.mem.eql(u8, entry.table_name, table_name)) count +|= entry.active_leases;
        }
        return count;
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

    fn removeEntriesForGroupLocked(self: *ProvisionedTableReadCache, group_id: u64) void {
        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (self.entries.items[i].group_id != group_id) {
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
            self.ready.broadcast(io);
        }
    }

    fn endExclusiveTableAccess(self: *ProvisionedTableReadCache, table_name: []const u8) void {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.releaseExclusiveTableAccessLocked(table_name);
        self.ready.broadcast(io);
    }

    fn endExclusiveGroupAccess(self: *ProvisionedTableReadCache, group_id: u64) void {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.releaseExclusiveGroupAccessLocked(group_id);
        self.ready.broadcast(io);
    }

    fn releaseExclusiveGroupAccessLocked(self: *ProvisionedTableReadCache, group_id: u64) void {
        const count = self.exclusive_group_access.getPtr(group_id) orelse unreachable;
        if (count.* > 1) count.* -= 1 else _ = self.exclusive_group_access.remove(group_id);
    }

    fn releaseExclusiveTableAccessLocked(self: *ProvisionedTableReadCache, table_name: []const u8) void {
        const count = self.exclusive_table_access.getPtr(table_name) orelse unreachable;
        if (count.* > 1) {
            count.* -= 1;
        } else {
            const removed = self.exclusive_table_access.fetchRemove(table_name) orelse unreachable;
            self.alloc.free(removed.key);
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
        self.retired_entries.append(self.alloc, entry) catch |err| {
            std.log.err("failed to queue retired read-cache entry table={s} err={s}", .{ entry.table_name, @errorName(err) });
            @panic("failed to queue retired read-cache entry");
        };
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
    return try openProvisionedQueryDbForTableWithCache(alloc, path, catalog, table_name, null, null, lsm_root_generation, null, .{}, null);
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
        .{ .backend_runtime = backend_runtime },
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
    runtime_cfg: ManagedReadRuntimeConfig,
    identity_namespace: ?db_mod.DocIdentityNamespace,
) !db_mod.DB {
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse {
        var db = try db_mod.DB.open(alloc, path, .{
            .open_mode = .query_readonly,
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = runtime_cfg.backend_runtime,
            .secret_store = runtime_cfg.secret_store,
            .remote_content = runtime_cfg.remote_content,
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
        });
        errdefer db.close();
        try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
        return db;
    };
    defer alloc.free(indexes_json);

    // Query-readonly DBs never initialize optional enrichment runtimes. Those
    // workers produce derived artifacts and remain writer-owned.
    var db = try db_mod.DB.open(alloc, path, .{
        .open_mode = .query_readonly,
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = runtime_cfg.backend_runtime,
        .secret_store = runtime_cfg.secret_store,
        .remote_content = runtime_cfg.remote_content,
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

fn catalogValueIsFullTextIndex(value: std.json.Value) !bool {
    if (value != .object) return error.InvalidTableIndexMetadata;
    const kind = value.object.get("type") orelse return true;
    if (kind != .string) return error.InvalidTableIndexMetadata;
    return std.mem.eql(u8, kind.string, "full_text");
}

pub fn loadTableAggregationTextAnalysis(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    requested_index_name: ?[]const u8,
) !introducer_mod.TextAnalysisConfig {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, alloc, table.indexes_json, .{});
    defer parsed_indexes.deinit();
    if (parsed_indexes.value != .object) return error.InvalidTableIndexMetadata;
    const indexes = parsed_indexes.value.object;

    var selected: ?std.json.Value = null;
    if (requested_index_name) |name| {
        if (indexes.get(name)) |value| {
            if (try catalogValueIsFullTextIndex(value)) selected = value;
        }
    }
    if (selected == null) {
        if (indexes.get(tables_api.default_full_text_index_name)) |value| {
            if (try catalogValueIsFullTextIndex(value)) selected = value;
        }
    }
    if (selected == null) {
        var it = indexes.iterator();
        while (it.next()) |entry| {
            if (!try catalogValueIsFullTextIndex(entry.value_ptr.*)) continue;
            if (selected != null) return error.InvalidQueryRequest;
            selected = entry.value_ptr.*;
        }
    }
    const config_json = try std.json.Stringify.valueAlloc(alloc, selected orelse return error.IndexNotFound, .{});
    defer alloc.free(config_json);

    if (table.schema_json.len == 0) {
        return try index_manager_mod.parseTextAnalysisForIndexConfig(alloc, config_json, null);
    }
    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    return try index_manager_mod.parseTextAnalysisForIndexConfig(alloc, config_json, runtime_schema);
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

test "provisioned read cache clear preserves in-flight pending opens and bumps epoch" {
    const alloc = std.testing.allocator;

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();

    try cache.pending_opens.append(alloc, .{
        .group_id = 7001,
        .table_name = try alloc.dupe(u8, "docs"),
    });

    // Seed the table's epoch slot the way an in-flight open would, then
    // verify clear() bumps it (epochs are per-table, not global).
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

test "provisioned read cache exclusive access drains active read leases" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-cache-exclusive-drain";

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

    const ExclusiveThread = struct {
        cache: *ProvisionedTableReadCache,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            var exclusive = self.cache.beginExclusiveTableAccess("docs") catch |err| {
                self.err = err;
                return;
            };
            exclusive.deinit();
        }
    };

    var lsm_cache = lsm_backend.Cache.init(alloc, lsm_backend.DefaultCacheSizeBytes);
    defer lsm_cache.deinit();
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    cache.lsm_cache = &lsm_cache;

    var lease = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 1, "docs");
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);

    var ctx = ExclusiveThread{ .cache = &cache };
    const thread = try std.Thread.spawn(.{}, ExclusiveThread.run, .{&ctx});

    var observed_exclusive = false;
    var observed_retired_count: usize = 0;
    for (0..100) |_| {
        const io = cache.threaded.io();
        cache.mutex.lockUncancelable(io);
        observed_exclusive = cache.hasExclusiveTableAccessLocked("docs");
        observed_retired_count = cache.retired_entries.items.len;
        cache.mutex.unlock(io);
        if (observed_exclusive and observed_retired_count == 1) break;
        io_impl.io().sleep(Io.Duration.fromMilliseconds(1), .awake) catch {};
    }

    lease.release();
    thread.join();
    if (ctx.err) |err| return err;
    try std.testing.expect(observed_exclusive);
    try std.testing.expectEqual(@as(usize, 1), observed_retired_count);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);
    try std.testing.expect(!cache.hasExclusiveTableAccessLocked("docs"));
}

test "provisioned read cache group exclusive drains only the published group" {
    const alloc = std.testing.allocator;
    const root = "/tmp/antfly-api-provisioned-read-cache-group-exclusive";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};

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
                    .indexes_json = tables_api.default_indexes_json,
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const ExclusiveThread = struct {
        cache: *ProvisionedTableReadCache,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            var exclusive = self.cache.beginExclusiveGroupAccess(7001) catch |err| {
                self.err = err;
                return;
            };
            exclusive.deinit();
        }
    };

    const path_one = try std.fmt.allocPrint(alloc, "{s}/group-7001", .{root});
    defer alloc.free(path_one);
    const path_two = try std.fmt.allocPrint(alloc, "{s}/group-7002", .{root});
    defer alloc.free(path_two);
    var lsm_cache = lsm_backend.Cache.init(alloc, lsm_backend.DefaultCacheSizeBytes);
    defer lsm_cache.deinit();
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    cache.lsm_cache = &lsm_cache;

    var lease_one = try cache.getOrOpen(path_one, FakeCatalog.iface(), 7001, 1, "docs");
    var lease_two = try cache.getOrOpen(path_two, FakeCatalog.iface(), 7002, 1, "docs");
    defer lease_two.release();
    const table_epoch = cache.table_epochs.get("docs").?;

    var ctx = ExclusiveThread{ .cache = &cache };
    const thread = try std.Thread.spawn(.{}, ExclusiveThread.run, .{&ctx});
    var observed = false;
    for (0..100) |_| {
        const io = cache.threaded.io();
        cache.mutex.lockUncancelable(io);
        observed = cache.hasExclusiveGroupAccessLocked(7001) and
            cache.entries.items.len == 1 and cache.entries.items[0].group_id == 7002 and
            cache.retired_entries.items.len == 1;
        cache.mutex.unlock(io);
        if (observed) break;
        io_impl.io().sleep(Io.Duration.fromMilliseconds(1), .awake) catch {};
    }

    lease_one.release();
    thread.join();
    if (ctx.err) |err| return err;
    try std.testing.expect(observed);
    try std.testing.expectEqual(table_epoch, cache.table_epochs.get("docs").?);
    try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 7002), cache.entries.items[0].group_id);
}

test "provisioned read cache retirement is allocation-free after entry installation" {
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
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), cache.retired_entries.items.len);

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);

    lease.release();
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), cache.retired_entries.items.len);
}
