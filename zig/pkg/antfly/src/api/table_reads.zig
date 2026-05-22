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
const builtin = @import("builtin");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const scraping = @import("antfly_scraping");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const common_secrets = @import("../common/secrets.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_table_provisioner = @import("../metadata/table_provisioner.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const raft_mod = @import("../raft/mod.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const doc_set = @import("../storage/db/doc_set.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const hbc_mod = @import("../storage/hbc_adapter.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const db_query_search = @import("../storage/db/query/search_exec.zig");
const graph_mod = @import("../graph/graph.zig");
const graph_paths = @import("../graph/paths.zig");
const graph_query_mod = @import("../graph/query.zig");
const reranking_runtime = @import("../reranking/mod.zig");
const template_mod = @import("../template.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const distributed_graph = @import("distributed_graph.zig");
const runtime_status = @import("runtime_status.zig");
const http_client = @import("http_client.zig");
const http_common = @import("../raft/transport/http_common.zig");
const platform_time = @import("../platform/time.zig");
const distributed_stats_mod = @import("../search/distributed_stats.zig");
const regex_mod = @import("../search/regex.zig");
const httpx = @import("httpx");
const Io = std.Io;
const json_helpers = @import("json_helpers.zig");
const ParsedJsonPathValue = json_helpers.ParsedJsonPathValue;
const parseJsonValueAlloc = json_helpers.parseJsonValueAlloc;
const parseJsonPathValueAlloc = json_helpers.parseJsonPathValueAlloc;
const algebraic_ir = db_mod.algebraic.ir;

fn benchQueryApiPhaseProfileEnabled() bool {
    return std.c.getenv("ANTFLY_BENCH_QUERY_API_PHASES\x00") != null or
        std.c.getenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY\x00") != null;
}

fn nsToUsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1000.0;
}
const algebraic_law = db_mod.algebraic.law;
const algebraic_planner = db_mod.algebraic.planner;

pub const LookupResponse = struct {
    json: []u8,
    version: u64,

    pub fn deinit(self: *LookupResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const ScanResponse = struct {
    ndjson: []u8,

    pub fn deinit(self: *ScanResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.ndjson);
        self.* = undefined;
    }
};

pub const TextStatsResponse = struct {
    fields: []const distributed_stats_mod.TextFieldStats,

    pub fn deinit(self: *TextStatsResponse, alloc: std.mem.Allocator) void {
        distributed_stats_mod.deinitTextFieldStats(alloc, self.fields);
        self.* = undefined;
    }
};

pub const BackgroundTextStatsResponse = struct {
    background_fields: []const db_mod.aggregations.DistributedBackgroundTextStats,

    pub fn deinit(self: *BackgroundTextStatsResponse, alloc: std.mem.Allocator) void {
        db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, self.background_fields);
        self.* = undefined;
    }
};

pub const ParsedTextStatsHttpResponse = union(enum) {
    fields: TextStatsResponse,
    background_fields: BackgroundTextStatsResponse,

    pub fn deinit(self: *ParsedTextStatsHttpResponse, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .fields => |*value| value.deinit(alloc),
            .background_fields => |*value| value.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const testing = if (builtin.is_test) struct {
    pub fn rejectResolvedDocFilterForCrossGroup(req: db_mod.types.SearchRequest, group_count: usize) !void {
        return rejectCrossGroupResolvedDocFilter(req, group_count);
    }

    pub fn rejectResolvedDocFilterForRemoteRoute(req: db_mod.types.SearchRequest, route: table_router.GroupRoute) !void {
        return rejectRemoteRouteResolvedDocFilter(req, route);
    }

    pub fn validateDocIdentityReadyForMultiGroupRead(
        alloc: std.mem.Allocator,
        catalog: table_catalog.CatalogSource,
        table_name: []const u8,
        group_count: usize,
    ) !void {
        return tableReadsValidateDocIdentityReadyForMultiGroup(alloc, catalog, table_name, group_count);
    }
} else struct {};

pub const ProvisionedTableReadCache = struct {
    alloc: std.mem.Allocator,
    threaded: Io.Threaded,
    lsm_cache: ?*lsm_backend.Cache = null,
    hbc_cache: ?*hbc_mod.Cache = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    hit_count: std.atomic.Value(u64) = .init(0),
    miss_count: std.atomic.Value(u64) = .init(0),
    mutex: Io.Mutex = .init,
    ready: Io.Condition = .init,
    epoch: u64 = 1,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    retired_entries: std.ArrayListUnmanaged(*Entry) = .empty,
    pending_opens: std.ArrayListUnmanaged(PendingOpen) = .empty,

    const max_cached_tables = 64;

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
        const identity_namespace = try loadTableIdentityNamespaceForGroup(self.alloc, catalog, table_name, group_id);
        const io = self.threaded.io();
        while (true) {
            self.mutex.lockUncancelable(io);
            const open_epoch = self.epoch;
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
                self.ready.waitUncancelable(io, &self.mutex);
                self.mutex.unlock(io);
                continue;
            }
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
                self.local_termite_provider,
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
            if (self.epoch != open_epoch) {
                self.ready.broadcast(io);
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
        self.epoch +%= 1;
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
        self.epoch +%= 1;
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

pub const ReadPreparation = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Kind = enum {
        general,
        dense_query,
    };

    pub const VTable = struct {
        prepare_for_read: *const fn (ptr: *anyopaque, table_name: []const u8, kind: Kind) void,
    };

    pub fn prepareForRead(self: ReadPreparation, table_name: []const u8, kind: Kind) void {
        self.vtable.prepare_for_read(self.ptr, table_name, kind);
    }
};

pub const GroupLsmGenerationSource = struct {
    ptr: *anyopaque,
    generation_for_group: *const fn (ptr: *anyopaque, group_id: u64) u64,

    pub fn generationForGroup(self: GroupLsmGenerationSource, group_id: u64) u64 {
        return self.generation_for_group(self.ptr, group_id);
    }
};

pub const PrimaryLookupDbLease = struct {
    ptr: *anyopaque,
    db: *db_mod.DB,
    release_fn: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) void,

    pub fn release(self: *PrimaryLookupDbLease, alloc: std.mem.Allocator) void {
        self.release_fn(self.ptr, alloc);
        self.* = undefined;
    }
};

pub const PrimaryLookupDbSource = struct {
    ptr: *anyopaque,
    lease_group: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
    ) anyerror!?PrimaryLookupDbLease,

    pub fn leaseGroup(
        self: PrimaryLookupDbSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
    ) !?PrimaryLookupDbLease {
        return try self.lease_group(self.ptr, alloc, table_name, group_id, lsm_root_generation);
    }
};

const LocalQueryExecution = struct {
    request: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
    dense_profile: ?query_api.QueryResponseMeta.DenseSearchProfile = null,
};

const ProfiledDenseQuery = struct {
    req: db_mod.types.SearchRequest,
    query: db_mod.types.DenseKnnQuery,
};

const ParallelFanoutKind = enum {
    text_stats,
    query,
    preflight,
};

const FanoutPlanReason = enum {
    no_io,
    single_group,
    small_request,
    parallel,
};

const FanoutPlan = struct {
    parallel: bool,
    width: usize,
    reason: FanoutPlanReason,
};

pub const ParallelFanoutMetricsSnapshot = struct {
    text_stats_parallel_total: u64 = 0,
    text_stats_parallel_ns_total: u64 = 0,
    text_stats_fallback_total: u64 = 0,
    text_stats_planned_parallel_total: u64 = 0,
    text_stats_planned_sequential_total: u64 = 0,
    text_stats_planned_width_total: u64 = 0,
    text_stats_plan_no_io_total: u64 = 0,
    text_stats_plan_single_group_total: u64 = 0,
    text_stats_plan_small_request_total: u64 = 0,
    query_parallel_total: u64 = 0,
    query_parallel_ns_total: u64 = 0,
    query_fallback_total: u64 = 0,
    query_planned_parallel_total: u64 = 0,
    query_planned_sequential_total: u64 = 0,
    query_planned_width_total: u64 = 0,
    query_plan_no_io_total: u64 = 0,
    query_plan_single_group_total: u64 = 0,
    query_plan_small_request_total: u64 = 0,
    preflight_parallel_total: u64 = 0,
    preflight_parallel_ns_total: u64 = 0,
    preflight_fallback_total: u64 = 0,
    preflight_planned_parallel_total: u64 = 0,
    preflight_planned_sequential_total: u64 = 0,
    preflight_planned_width_total: u64 = 0,
    preflight_plan_no_io_total: u64 = 0,
    preflight_plan_single_group_total: u64 = 0,
    preflight_plan_small_request_total: u64 = 0,
};

var parallel_text_stats_total: std.atomic.Value(u64) = .init(0);
var parallel_text_stats_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_width_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_text_stats_total: std.atomic.Value(u64) = .init(0);
var parallel_query_total: std.atomic.Value(u64) = .init(0);
var parallel_query_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_query_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_query_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_query_total: std.atomic.Value(u64) = .init(0);
var planned_width_query_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_query_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_query_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_query_total: std.atomic.Value(u64) = .init(0);
var parallel_preflight_total: std.atomic.Value(u64) = .init(0);
var parallel_preflight_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_width_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_preflight_total: std.atomic.Value(u64) = .init(0);

fn recordFanoutPlan(kind: ParallelFanoutKind, plan: FanoutPlan) void {
    switch (kind) {
        .text_stats => {
            if (plan.parallel) {
                _ = planned_parallel_text_stats_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_text_stats_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_text_stats_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_text_stats_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_text_stats_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_text_stats_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
        .query => {
            if (plan.parallel) {
                _ = planned_parallel_query_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_query_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_query_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_query_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_query_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_query_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
        .preflight => {
            if (plan.parallel) {
                _ = planned_parallel_preflight_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_preflight_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_preflight_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_preflight_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_preflight_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_preflight_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
    }
}

fn recordParallelFanout(kind: ParallelFanoutKind, elapsed_ns: u64) void {
    switch (kind) {
        .text_stats => {
            _ = parallel_text_stats_total.fetchAdd(1, .monotonic);
            _ = parallel_text_stats_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
        .query => {
            _ = parallel_query_total.fetchAdd(1, .monotonic);
            _ = parallel_query_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
        .preflight => {
            _ = parallel_preflight_total.fetchAdd(1, .monotonic);
            _ = parallel_preflight_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
    }
}

fn recordParallelFanoutFallback(kind: ParallelFanoutKind) void {
    switch (kind) {
        .text_stats => _ = fallback_text_stats_total.fetchAdd(1, .monotonic),
        .query => _ = fallback_query_total.fetchAdd(1, .monotonic),
        .preflight => _ = fallback_preflight_total.fetchAdd(1, .monotonic),
    }
}

pub fn parallelFanoutMetricsSnapshot() ParallelFanoutMetricsSnapshot {
    return .{
        .text_stats_parallel_total = parallel_text_stats_total.load(.monotonic),
        .text_stats_parallel_ns_total = parallel_text_stats_ns_total.load(.monotonic),
        .text_stats_fallback_total = fallback_text_stats_total.load(.monotonic),
        .text_stats_planned_parallel_total = planned_parallel_text_stats_total.load(.monotonic),
        .text_stats_planned_sequential_total = planned_sequential_text_stats_total.load(.monotonic),
        .text_stats_planned_width_total = planned_width_text_stats_total.load(.monotonic),
        .text_stats_plan_no_io_total = planned_no_io_text_stats_total.load(.monotonic),
        .text_stats_plan_single_group_total = planned_single_group_text_stats_total.load(.monotonic),
        .text_stats_plan_small_request_total = planned_small_request_text_stats_total.load(.monotonic),
        .query_parallel_total = parallel_query_total.load(.monotonic),
        .query_parallel_ns_total = parallel_query_ns_total.load(.monotonic),
        .query_fallback_total = fallback_query_total.load(.monotonic),
        .query_planned_parallel_total = planned_parallel_query_total.load(.monotonic),
        .query_planned_sequential_total = planned_sequential_query_total.load(.monotonic),
        .query_planned_width_total = planned_width_query_total.load(.monotonic),
        .query_plan_no_io_total = planned_no_io_query_total.load(.monotonic),
        .query_plan_single_group_total = planned_single_group_query_total.load(.monotonic),
        .query_plan_small_request_total = planned_small_request_query_total.load(.monotonic),
        .preflight_parallel_total = parallel_preflight_total.load(.monotonic),
        .preflight_parallel_ns_total = parallel_preflight_ns_total.load(.monotonic),
        .preflight_fallback_total = fallback_preflight_total.load(.monotonic),
        .preflight_planned_parallel_total = planned_parallel_preflight_total.load(.monotonic),
        .preflight_planned_sequential_total = planned_sequential_preflight_total.load(.monotonic),
        .preflight_planned_width_total = planned_width_preflight_total.load(.monotonic),
        .preflight_plan_no_io_total = planned_no_io_preflight_total.load(.monotonic),
        .preflight_plan_single_group_total = planned_single_group_preflight_total.load(.monotonic),
        .preflight_plan_small_request_total = planned_small_request_preflight_total.load(.monotonic),
    };
}

fn ioAsyncLimitWidth(io_impl: *std.Io.Threaded, group_count: usize) usize {
    const raw = @intFromEnum(io_impl.async_limit);
    if (raw == 0) return 1;
    if (raw == std.math.maxInt(usize)) return @max(@as(usize, 1), group_count);
    return @max(@as(usize, 1), @min(group_count, raw));
}

fn ioAsyncLimitCap(io_impl: *std.Io.Threaded) usize {
    const raw = @intFromEnum(io_impl.async_limit);
    if (raw == 0) return 1;
    if (raw == std.math.maxInt(usize)) return std.math.maxInt(usize);
    return @max(@as(usize, 1), raw);
}

fn planFanout(kind: ParallelFanoutKind, io_impl: ?*std.Io.Threaded, group_count: usize) FanoutPlan {
    const attached_io = io_impl orelse return .{
        .parallel = false,
        .width = 1,
        .reason = .no_io,
    };
    if (group_count <= 1) return .{
        .parallel = false,
        .width = 1,
        .reason = .single_group,
    };

    const width_cap = ioAsyncLimitWidth(attached_io, group_count);
    const target_width = switch (kind) {
        .text_stats, .preflight => @min(width_cap, @min(group_count, @as(usize, 4))),
        .query => @min(width_cap, @min(group_count, @as(usize, 4))),
    };
    return .{
        .parallel = target_width > 1,
        .width = if (target_width > 0) target_width else 1,
        .reason = if (target_width > 1) .parallel else .small_request,
    };
}

fn planQueryFanout(
    io_impl: ?*std.Io.Threaded,
    group_count: usize,
    req: db_mod.types.SearchRequest,
) FanoutPlan {
    const attached_io = io_impl orelse return .{
        .parallel = false,
        .width = 1,
        .reason = .no_io,
    };
    if (group_count <= 1) return .{
        .parallel = false,
        .width = 1,
        .reason = .single_group,
    };
    if (group_count <= 2 and req.limit > 0 and req.limit <= 32) return .{
        .parallel = false,
        .width = 1,
        .reason = .small_request,
    };

    const width_cap = ioAsyncLimitWidth(attached_io, group_count);
    const result_window = req.limit + req.offset;
    const target_width: usize = if (result_window > 0 and result_window <= 32)
        @min(width_cap, @min(group_count, @as(usize, 4)))
    else
        @min(width_cap, @min(group_count, @as(usize, 8)));
    return .{
        .parallel = target_width > 1,
        .width = if (target_width > 0) target_width else 1,
        .reason = if (target_width > 1) .parallel else .small_request,
    };
}

const TextStatsRequestMode = enum {
    query_request,
    explicit_fields,
    background_fields,
};

const OwnedTextStatsFieldRequest = struct {
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: [][]const u8 = &.{},

    fn deinit(self: *OwnedTextStatsFieldRequest, alloc: std.mem.Allocator) void {
        if (self.index_name) |index_name| alloc.free(index_name);
        alloc.free(self.field);
        for (self.terms) |term| alloc.free(term);
        if (self.terms.len > 0) alloc.free(self.terms);
        self.* = undefined;
    }
};

const OwnedBackgroundTextStatsFieldRequest = struct {
    aggregation_name: []const u8,
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: [][]const u8 = &.{},
    background_query: db_mod.aggregations.BackgroundQuery,

    fn deinit(self: *OwnedBackgroundTextStatsFieldRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.aggregation_name);
        if (self.index_name) |index_name| alloc.free(index_name);
        alloc.free(self.field);
        for (self.terms) |term| alloc.free(term);
        if (self.terms.len > 0) alloc.free(self.terms);
        switch (self.background_query) {
            .match_all => {},
            .match => |query| {
                alloc.free(query.field);
                alloc.free(query.text);
            },
            .term => |query| {
                alloc.free(query.field);
                alloc.free(query.term);
            },
        }
        self.* = undefined;
    }
};

const TextStatsFieldRequestInput = struct {
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8,
};

const BackgroundTextStatsFieldRequestInput = struct {
    aggregation_name: []const u8,
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8,
    background_query: std.json.Value,
};

const TextStatsRequestInput = struct {
    _identity_read_generation: ?u64 = null,
    _resolved_doc_filter: ?std.json.Value = null,
    query_request: ?std.json.Value = null,
    fields: ?[]const TextStatsFieldRequestInput = null,
    background_fields: ?[]const BackgroundTextStatsFieldRequestInput = null,
};

const AlgebraicPartialsRequestInput = struct {
    index_name: ?[]const u8 = null,
    _identity_read_generation: ?u64 = null,
    tensor_access_paths: ?[]const AlgebraicTensorAccessPathInput = null,
    tensor_exprs: ?[]const AlgebraicTensorExprInput = null,
    tensor_program: ?AlgebraicTensorProgramInput = null,
    cardinality: ?std.json.Value = null,
    terms_cardinality: ?std.json.Value = null,
    range_cardinality: ?std.json.Value = null,
    histogram_cardinality: ?std.json.Value = null,
};

const AlgebraicTensorAccessPathInput = query_contract.AlgebraicTensorAccessPathEnvelopeInput;
const AlgebraicTensorExprInput = query_contract.AlgebraicTensorExprEnvelopeInput;
const AlgebraicTensorProgramInput = query_contract.AlgebraicTensorProgramEnvelopeInput;

const AlgebraicPartialResponseInput = struct {
    canonical_axis: []const u8,
    metric: []const u8 = "",
    law: []const u8,
    value: []const u8,
};

const AlgebraicPartialsResponseInput = struct {
    partials: []const AlgebraicPartialResponseInput,
};

const ParsedAlgebraicPartialsRequest = struct {
    index_name: ?[]u8 = null,
    identity_read_generation: ?u64 = null,
    tensor_access_paths: []OwnedAlgebraicTensorAccessPath = &.{},
    tensor_exprs: []OwnedAlgebraicTensorExpr = &.{},
    tensor_program: ?OwnedAlgebraicTensorProgram = null,

    fn deinit(self: *ParsedAlgebraicPartialsRequest, alloc: std.mem.Allocator) void {
        if (self.index_name) |value| alloc.free(value);
        for (self.tensor_access_paths) |*item| item.deinit(alloc);
        if (self.tensor_access_paths.len > 0) alloc.free(self.tensor_access_paths);
        for (self.tensor_exprs) |*item| item.deinit(alloc);
        if (self.tensor_exprs.len > 0) alloc.free(self.tensor_exprs);
        if (self.tensor_program) |*program| program.deinit(alloc);
        self.* = undefined;
    }
};

const OwnedAlgebraicTensorAccessPath = query_contract.OwnedAlgebraicTensorAccessPathEnvelope;
const OwnedAlgebraicTensorExpr = query_contract.OwnedAlgebraicTensorExprEnvelope;
const OwnedAlgebraicTensorProgram = query_contract.OwnedAlgebraicTensorProgramEnvelope;

const ParsedExplicitTextStatsRequest = struct {
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null,
    items: []OwnedTextStatsFieldRequest = &.{},

    fn deinit(self: *ParsedExplicitTextStatsRequest, alloc: std.mem.Allocator) void {
        if (self.resolved_doc_filter) |*filter| filter.deinit(alloc);
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

const ParsedBackgroundTextStatsRequest = struct {
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null,
    items: []OwnedBackgroundTextStatsFieldRequest = &.{},

    fn deinit(self: *ParsedBackgroundTextStatsRequest, alloc: std.mem.Allocator) void {
        if (self.resolved_doc_filter) |*filter| filter.deinit(alloc);
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

const TextStatsTermDocFreqInput = struct {
    term: []const u8,
    doc_freq: u32,
};

const TextStatsFieldResponseInput = struct {
    field: []const u8,
    global_doc_count: u32,
    global_total_field_len: u64,
    term_doc_freqs: []const TextStatsTermDocFreqInput,
};

const TextStatsResponseInput = struct {
    fields: []const TextStatsFieldResponseInput,
};

const BackgroundTextStatsFieldResponseInput = struct {
    aggregation_name: []const u8,
    field: []const u8,
    background_doc_count: u32,
    term_doc_freqs: []const TextStatsTermDocFreqInput,
};

const BackgroundTextStatsResponseInput = struct {
    background_fields: []const BackgroundTextStatsFieldResponseInput,
};

const ParsedTextStatsRequest = union(TextStatsRequestMode) {
    query_request: query_api.OwnedQueryRequest,
    explicit_fields: ParsedExplicitTextStatsRequest,
    background_fields: ParsedBackgroundTextStatsRequest,

    fn deinit(self: *ParsedTextStatsRequest, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query_request => |*request| request.deinit(alloc),
            .explicit_fields => |*request| request.deinit(alloc),
            .background_fields => |*request| request.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const TableReadSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        lookup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?LookupResponse,
        scan: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?ScanResponse,
        query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse,
        preflight_query: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
            max_work: u32,
        ) anyerror!?db_mod.RuntimePreflightSummary = null,
        preflight_query_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
            max_work: u32,
        ) anyerror!?db_mod.RuntimePreflightSummary = null,
        lookup_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?LookupResponse = null,
        scan_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?ScanResponse = null,
        query_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse = null,
        search_result_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.SearchResult = null,
        text_stats_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        algebraic_partials_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_partition_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_rows_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_unmatched_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_finalize_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_job_state_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        graph_expand_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphExpandRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?distributed_graph.GraphExpandResponse = null,
        graph_hydrate_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphHydrateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?distributed_graph.GraphHydrateResponse = null,
        graph_edges_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphEdgesRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?distributed_graph.GraphEdgesResponse = null,
        local_runtime_statuses: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
    };

    pub fn lookup(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        return try self.vtable.lookup(self.ptr, alloc, table_name, key, opts, consistency);
    }

    pub fn scan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        return try self.vtable.scan(self.ptr, alloc, table_name, from_key, to_key, opts, consistency);
    }

    pub fn query(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        return try self.vtable.query(self.ptr, alloc, table_name, req, consistency);
    }

    pub fn preflightQuery(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const fn_ptr = self.vtable.preflight_query orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, req, consistency, max_work);
    }

    pub fn preflightQueryGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const fn_ptr = self.vtable.preflight_query_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency, max_work);
    }

    pub fn lookupGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const fn_ptr = self.vtable.lookup_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, key, opts, consistency);
    }

    pub fn scanGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const fn_ptr = self.vtable.scan_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency);
    }

    pub fn queryGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.query_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn searchResultGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const fn_ptr = self.vtable.search_result_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn textStatsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.text_stats_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn algebraicPartialsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.algebraic_partials_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinPartitionGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_partition_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinRowsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_rows_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinUnmatchedGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_unmatched_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinFinalizeGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_finalize_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinJobStateGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_job_state_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn graphExpandGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const fn_ptr = self.vtable.graph_expand_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn graphHydrateGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const fn_ptr = self.vtable.graph_hydrate_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn graphEdgesGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const fn_ptr = self.vtable.graph_edges_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn localRuntimeStatuses(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const fn_ptr = self.vtable.local_runtime_statuses orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }
};

pub fn searchRequestFromVectorWorkerEnvelope(envelope: *const query_contract.OwnedAlgebraicVectorWorkerRequestEnvelope) db_mod.types.SearchRequest {
    var req = switch (envelope.query) {
        .dense => |dense| db_mod.types.SearchRequest{
            .index_name = envelope.index_name,
            .limit = envelope.options.limit,
            .offset = envelope.options.offset,
            .count_only = envelope.options.count_only,
            .profile = envelope.options.profile,
            .include_stored = envelope.options.include_stored,
            .fields = envelope.options.fields,
            .filter_query_json = envelope.options.filter_query_json,
            .exclusion_query_json = envelope.options.exclusion_query_json,
            .filter_prefix = envelope.options.filter_prefix,
            .filter_ids = envelope.options.filter_ids,
            .exclude_ids = envelope.options.exclude_ids,
            .require_algebraic_filter_resolution = envelope.options.require_algebraic_filter_resolution,
            .include_all_fields = envelope.options.include_all_fields,
            .defer_stored_projection = envelope.options.defer_stored_projection,
            .search_effort = envelope.options.search_effort,
            .distance_over = envelope.options.distance_over,
            .distance_under = envelope.options.distance_under,
            .return_mode = envelope.options.return_mode,
            .max_chunks_per_parent = envelope.options.max_chunks_per_parent,
            .identity_read_generation = envelope.options.identity_read_generation,
            .resolved_doc_filter = envelope.resolved_doc_filter,
            .resolved_doc_filter_wire_context = envelope.resolved_doc_filter_wire_context,
            .query = .{ .dense_knn = dense },
        },
        .sparse => |sparse| db_mod.types.SearchRequest{
            .index_name = envelope.index_name,
            .limit = envelope.options.limit,
            .offset = envelope.options.offset,
            .count_only = envelope.options.count_only,
            .profile = envelope.options.profile,
            .include_stored = envelope.options.include_stored,
            .fields = envelope.options.fields,
            .filter_query_json = envelope.options.filter_query_json,
            .exclusion_query_json = envelope.options.exclusion_query_json,
            .filter_prefix = envelope.options.filter_prefix,
            .filter_ids = envelope.options.filter_ids,
            .exclude_ids = envelope.options.exclude_ids,
            .require_algebraic_filter_resolution = envelope.options.require_algebraic_filter_resolution,
            .include_all_fields = envelope.options.include_all_fields,
            .defer_stored_projection = envelope.options.defer_stored_projection,
            .search_effort = envelope.options.search_effort,
            .distance_over = envelope.options.distance_over,
            .distance_under = envelope.options.distance_under,
            .return_mode = envelope.options.return_mode,
            .max_chunks_per_parent = envelope.options.max_chunks_per_parent,
            .identity_read_generation = envelope.options.identity_read_generation,
            .resolved_doc_filter = envelope.resolved_doc_filter,
            .resolved_doc_filter_wire_context = envelope.resolved_doc_filter_wire_context,
            .query = .{ .sparse_knn = sparse },
        },
    };
    query_contract.applyNativeDocIdConstraintEnvelope(&req, envelope.native_doc_id_constraints.constraints);
    return req;
}

const AlgebraicVectorWorkerCandidate = struct {
    index_name: []const u8,
    layout: algebraic_ir.PhysicalLayout,
    query: query_contract.AlgebraicVectorWorkerQuery,
    k: u32,
};

fn algebraicVectorWorkerCandidateForSearchRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ?AlgebraicVectorWorkerCandidate {
    if (req.aggregations_json.len != 0 or
        req.full_text != null or
        req.full_text_queries.len != 0 or
        req.dense_queries.len != 0 or
        req.sparse_queries.len != 0 or
        req.graph_queries.len != 0 or
        req.merge_config != null or
        req.reranker != null or
        req.pruner != null or
        req.expand_strategy != null or
        req.distributed_text_stats.len != 0 or
        searchRequestHasUnserializableResolvedDocFilter(req))
    {
        return null;
    }
    if (req.filter_query_json.len != 0 and !algebraicVectorWorkerFilterJsonSupported(alloc, req.filter_query_json)) return null;
    if (req.exclusion_query_json.len != 0 and !algebraicVectorWorkerFilterJsonSupported(alloc, req.exclusion_query_json)) return null;

    if (req.dense) |dense| {
        if (req.sparse != null) return null;
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
    }
    if (req.sparse) |sparse| {
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
    }

    switch (req.query) {
        .dense_knn => |dense| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
        },
        .sparse_knn => |sparse| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
        },
        else => return null,
    }
}

fn algebraicVectorWorkerFilterJsonSupported(alloc: std.mem.Allocator, filter_query_json: []const u8) bool {
    if (filter_query_json.len == 0) return true;
    const constraints = algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match_all = {} },
        .filter_query_json = filter_query_json,
    }) catch return false;
    const owned = constraints orelse return false;
    defer freeAlgebraicConstraints(alloc, owned);
    return true;
}

fn annotateVectorWorkerPreflight(
    alloc: std.mem.Allocator,
    summary: *db_mod.RuntimePreflightSummary,
    req: db_mod.types.SearchRequest,
) void {
    if (!searchRequestHasSingleVectorWorkerKnn(req)) return;
    summary.vector_worker_filter_constraint_count +|= vectorWorkerFilterConstraintCount(req);
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) {
        summary.vector_worker_requires_algebraic_filter_resolution = true;
    }
    if (algebraicVectorWorkerCandidateForSearchRequest(alloc, req) != null) {
        summary.vector_worker_candidate_count +|= 1;
    } else {
        summary.vector_worker_fallback_count +|= 1;
    }
}

fn searchRequestHasSingleVectorWorkerKnn(req: db_mod.types.SearchRequest) bool {
    var count: u32 = 0;
    if (req.dense != null) count += 1;
    if (req.sparse != null) count += 1;
    switch (req.query) {
        .dense_knn, .sparse_knn => count += 1,
        else => {},
    }
    return count == 1;
}

fn vectorWorkerFilterConstraintCount(req: db_mod.types.SearchRequest) u32 {
    var count: u32 = 0;
    if (req.filter_query_json.len > 0) count += 1;
    if (req.exclusion_query_json.len > 0) count += 1;
    if (req.filter_ids.len > 0) count += 1;
    if (req.exclude_ids.len > 0) count += 1;
    if (req.filter_doc_ids_positive or req.filter_doc_ids.len > 0) count += 1;
    if (req.exclude_doc_ids.len > 0) count += 1;
    if (searchRequestHasResolvedDocFilter(req)) count += 1;
    return count;
}

fn encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !?[]u8 {
    const candidate = algebraicVectorWorkerCandidateForSearchRequest(alloc, req) orelse return null;
    const constraints = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest(req);
    var tensor_program = (try algebraic_planner.planVectorSearchTensorProgramAlloc(alloc, candidate.index_name, candidate.layout, constraints.hasConstraints())) orelse return null;
    defer tensor_program.deinit(alloc);
    return try query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        candidate.index_name,
        candidate.layout,
        candidate.query,
        .{
            .limit = req.limit,
            .offset = req.offset,
            .count_only = req.count_only,
            .profile = req.profile,
            .include_stored = req.include_stored,
            .fields = @constCast(req.fields),
            .filter_query_json = req.filter_query_json,
            .exclusion_query_json = req.exclusion_query_json,
            .filter_prefix = req.filter_prefix,
            .filter_ids = req.filter_ids,
            .exclude_ids = req.exclude_ids,
            .require_algebraic_filter_resolution = req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0,
            .include_all_fields = req.include_all_fields,
            .defer_stored_projection = req.defer_stored_projection,
            .search_effort = req.search_effort,
            .distance_over = req.distance_over,
            .distance_under = req.distance_under,
            .return_mode = req.return_mode,
            .max_chunks_per_parent = req.max_chunks_per_parent,
            .identity_read_generation = req.identity_read_generation,
        },
        constraints,
        req.resolved_doc_filter,
        req.resolved_doc_filter_wire_context,
        tensor_program.access_paths,
        tensor_program.asProgram(),
    );
}

pub const BoundTableReadSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,
    reads: raft_mod.FeatureDBReads,

    pub fn init(
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
        requester: raft_mod.ReadableLeaseRequester,
    ) BoundTableReadSource {
        return .{
            .table_name = table_name,
            .db = db,
            .reads = raft_mod.FeatureDBReads.init(group_id, requester),
        };
    }

    pub fn source(self: *BoundTableReadSource) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = query,
                .preflight_query = preflightQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .join_partition_group_local = null,
                .join_rows_group_local = null,
                .join_unmatched_group_local = null,
                .join_finalize_group_local = null,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
            },
        };
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
        items[0] = .{
            .group_id = self.reads.group_id,
            .stats = try self.db.runtimeStatusStatsConsistent(alloc),
        };
        return .{ .items = items };
    }

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        var result = (try self.reads.lookupWithConsistency(alloc, self.db, key, opts, consistency)) orelse return null;
        defer result.deinit(alloc);

        return .{
            .json = try alloc.dupe(u8, result.json),
            .version = try self.db.getTimestamp(alloc, key),
        };
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        var result = try self.reads.scanWithConsistency(alloc, self.db, from_key, to_key, opts, consistency);
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        for (result.hashes, 0..) |entry, i| {
            const json = if (opts.include_documents) result.documents[i].json else null;
            try appendScanLine(alloc, &out, entry.id, json);
        }

        return .{
            .ndjson = try out.toOwnedSlice(alloc),
        };
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const start_ns = platform_time.monotonicNs();
        const phase_profile = benchQueryApiPhaseProfileEnabled();
        const prepare_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        try self.reads.reads.prepareSearchWithConsistency(self.reads.group_id, req, consistency);
        const prepare_ns = if (phase_profile) platform_time.monotonicNs() - prepare_start_ns else 0;
        const snapshot_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        const snapshot_req = try self.db.searchRequestAtCurrentIdentityGeneration(req);
        const snapshot_ns = if (phase_profile) platform_time.monotonicNs() - snapshot_start_ns else 0;
        var execution: LocalQueryExecution = .{ .request = snapshot_req, .result = undefined };
        const search_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        if (profiledDenseQuery(snapshot_req)) |dense| {
            const profiled = try self.db.searchDenseProfiled(alloc, dense.req, dense.query);
            execution = .{
                .request = snapshot_req,
                .result = profiled.result,
                .dense_profile = mapDenseSearchProfile(profiled.profile),
            };
        } else {
            execution = .{
                .request = snapshot_req,
                .result = try self.db.search(alloc, snapshot_req),
            };
        }
        const search_ns = if (phase_profile) platform_time.monotonicNs() - search_start_ns else 0;
        var result = execution.result;
        defer result.deinit();
        const response_req = execution.request;
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = 1,
            .dense_search = execution.dense_profile,
        };
        defer meta.deinit(alloc);
        const agg_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        try applyBoundQueryAggregations(self, alloc, response_req, &result, &meta, consistency);
        const agg_ns = if (phase_profile) platform_time.monotonicNs() - agg_start_ns else 0;
        const post_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        try applyQueryPostProcessing(alloc, response_req, &result, &meta, null, null);
        const post_ns = if (phase_profile) platform_time.monotonicNs() - post_start_ns else 0;
        const encode_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        const response = try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
        if (phase_profile) {
            const encode_ns = platform_time.monotonicNs() - encode_start_ns;
            const total_ns = platform_time.monotonicNs() - start_ns;
            std.debug.print(
                "antfly_bench_query_api_phases prepare_us={d:.3} snapshot_us={d:.3} search_us={d:.3} aggregation_us={d:.3} post_us={d:.3} encode_us={d:.3} total_us={d:.3} hits={d} total_hits={d} response_bytes={d}\n",
                .{
                    nsToUsFloat(prepare_ns),
                    nsToUsFloat(snapshot_ns),
                    nsToUsFloat(search_ns),
                    nsToUsFloat(agg_ns),
                    nsToUsFloat(post_ns),
                    nsToUsFloat(encode_ns),
                    nsToUsFloat(total_ns),
                    result.hits.len,
                    result.total_hits,
                    response.json.len,
                },
            );
        }
        return response;
    }

    fn preflightQuery(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.reads.reads.prepareSearchWithConsistency(self.reads.group_id, req, consistency);
        var summary = try self.db.preflightSearchRequest(alloc, req, max_work);
        annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        return try preflightQuery(ptr, alloc, table_name, req, consistency, max_work);
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        return try lookup(ptr, alloc, table_name, key, opts, consistency);
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        return try scan(ptr, alloc, table_name, from_key, to_key, opts, consistency);
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        return try query(ptr, alloc, table_name, req, consistency);
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.reads.searchWithConsistency(alloc, self.db, req, consistency);
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectBoundLocalTextStats(self, alloc, table_name, body);
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectBoundLocalAlgebraicPartials(self, alloc, table_name, body);
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const expansions = try alloc.alloc(distributed_graph.GraphExpansion, req.frontier.len);
        var initialized: usize = 0;
        errdefer {
            for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc);
            alloc.free(expansions);
        }
        for (req.frontier, 0..) |item, i| {
            const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
            defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
            var result = try self.reads.searchWithConsistency(alloc, self.db, search_req, consistency);
            defer result.deinit();

            expansions[i] = .{
                .frontier_id = item.id,
                .frontier_key = try alloc.dupe(u8, item.key),
                .graph_result = graph_result_blk: {
                    var graph_result = if (result.graph_results.len > 0)
                        try distributed_graph.filterGraphSearchResult(alloc, result.graph_results[0], req.exclude_keys, req.exclude_edges)
                    else
                        try distributed_graph.emptyGraphSearchResult(alloc, req.name);
                    for (graph_result.hits) |*hit| hit.deinit(alloc);
                    if (graph_result.hits.len > 0) alloc.free(graph_result.hits);
                    graph_result.hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]);
                    break :graph_result_blk graph_result;
                },
            };
            initialized += 1;
        }

        return .{ .expansions = expansions };
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (req.topology_epoch != 0) return error.TopologyChanged;

        var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(alloc);
            hits.deinit(alloc);
        }

        for (req.keys) |key| {
            var result = (try self.reads.lookupWithConsistency(alloc, self.db, key, .{}, consistency)) orelse continue;
            defer result.deinit(alloc);
            try hits.append(alloc, .{
                .id = try alloc.dupe(u8, key),
                .doc_ordinal = try self.db.lookupLiveDocOrdinalForInternalRead(alloc, key, req.identity_read_generation),
                .stored_data = try alloc.dupe(u8, result.json),
            });
        }
        return .{ .hits = try hits.toOwnedSlice(alloc) };
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        _: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (req.topology_epoch != 0) return error.TopologyChanged;
        try distributed_graph.validateGraphEdgesTensorAccessPath(alloc, req);
        const edges = try self.db.getEdges(alloc, req.index_name, req.key, "", req.direction);
        return .{ .edges = edges };
    }
};

pub const ProvisionedTableReadSource = struct {
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    io_impl: ?*std.Io.Threaded = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    cache: ?*ProvisionedTableReadCache = null,
    runtime_status_cache: ?*runtime_status.TableRuntimeSnapshotCache = null,
    prepare_for_read: ?ReadPreparation = null,
    group_lsm_generation: ?GroupLsmGenerationSource = null,
    primary_lookup_db: ?PrimaryLookupDbSource = null,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,

    pub fn init(
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        requester: raft_mod.ReadableLeaseRequester,
    ) ProvisionedTableReadSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .requester = requester,
        };
    }

    pub fn withIo(self: *ProvisionedTableReadSource, io_impl: *std.Io.Threaded) *ProvisionedTableReadSource {
        self.io_impl = io_impl;
        return self;
    }

    pub fn withLocalTermiteProvider(
        self: *ProvisionedTableReadSource,
        provider: ?managed_embedder.LocalTermiteProvider,
    ) *ProvisionedTableReadSource {
        self.local_termite_provider = provider;
        if (self.cache) |cache| cache.local_termite_provider = provider;
        return self;
    }

    pub fn withSecretStore(
        self: *ProvisionedTableReadSource,
        secret_store: ?*common_secrets.FileStore,
    ) *ProvisionedTableReadSource {
        self.secret_store = secret_store;
        if (self.cache) |cache| cache.secret_store = secret_store;
        return self;
    }

    pub fn withRemoteContent(
        self: *ProvisionedTableReadSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *ProvisionedTableReadSource {
        self.remote_content = remote_content;
        if (self.cache) |cache| cache.remote_content = remote_content;
        return self;
    }

    pub fn source(self: *ProvisionedTableReadSource) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = query,
                .preflight_query = preflightQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .join_partition_group_local = null,
                .join_rows_group_local = null,
                .join_unmatched_group_local = null,
                .join_finalize_group_local = null,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
            },
        };
    }

    pub fn warmTableGroup(self: *ProvisionedTableReadSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8) !void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);

        // Warmup must not pin a full managed query handle or run metadata-driven
        // query-open reconciliation for a just-created table. That work is
        // heavier than the startup/cache warmup needs, and it can block or
        // destabilize startup before the first real read. Keep warmup to a
        // lightweight status-only open instead; actual query handles still open
        // lazily on demand.
        var db = try openProvisionedWarmStatusDbForTable(
            alloc,
            path,
            self.lsmRootGeneration(group_id),
            self.backend_runtime,
            try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id),
        );
        db.close();
    }

    fn lsmRootGeneration(self: *const ProvisionedTableReadSource, group_id: u64) u64 {
        return if (self.group_lsm_generation) |generation_source| generation_source.generationForGroup(group_id) else 0;
    }

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, key)) orelse return null;
        return try lookupProvisionedHostedLocal(self.primary_lookup_db, self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, key, opts, consistency);
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, from_key, to_key);
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        var emitted: u32 = 0;
        for (group_ids) |group_id| {
            var group_opts = opts;
            if (opts.limit > 0) {
                if (emitted >= opts.limit) break;
                group_opts.limit = opts.limit - emitted;
            }

            var result = (try scanProvisionedHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, from_key, to_key, group_opts, consistency)) orelse continue;
            defer result.deinit(alloc);
            try out.appendSlice(alloc, result.ndjson);
            emitted += @intCast(std.mem.count(u8, result.ndjson, "\n"));
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const start_ns = platform_time.monotonicNs();
        if (group_ids.len == 1 and !distributed_graph.supportsCrossRange(req)) {
            const execution = try queryHostedLocalDetailed(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_ids[0], self.lsmRootGeneration(group_ids[0]), self.backend_runtime, self.local_termite_provider, self.secret_store, self.remote_content, table_name, req, consistency);
            var result = execution.result;
            defer result.deinit();
            const response_req = execution.request;
            var meta: query_api.QueryResponseMeta = .{
                .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                .shard_count = 1,
                .dense_search = execution.dense_profile,
            };
            defer meta.deinit(alloc);
            try applyProvisionedQueryAggregations(self, alloc, group_ids, table_name, response_req, &result, &meta, consistency);
            try applyQueryPostProcessing(alloc, response_req, &result, &meta, self.local_termite_provider, self.secret_store);
            return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
        }

        if (group_ids.len > 1 and distributed_graph.supportsCrossRange(req)) {
            var base_req = req;
            base_req.graph_queries = &.{};
            base_req.expand_strategy = null;
            var merged = try queryProvisionedAcrossGroups(self, alloc, group_ids, base_req, table_name, consistency);
            defer merged.deinit();
            const graph_req = requestWithResultIdentityGeneration(req, merged);

            const worker = provisionedGraphWorker(self);
            const graph_results = try distributed_graph.executeCrossRange(alloc, self.catalog, worker, table_name, graph_req, merged, consistency);
            merged.graph_results = graph_results;

            var meta: query_api.QueryResponseMeta = .{
                .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                .shard_count = @intCast(group_ids.len),
                .merged = true,
            };
            defer meta.deinit(alloc);
            try applyProvisionedQueryAggregations(self, alloc, group_ids, table_name, graph_req, &merged, &meta, consistency);
            try applyQueryPostProcessing(alloc, graph_req, &merged, &meta, self.local_termite_provider, self.secret_store);
            return try query_api.encodeQueryResponses(alloc, table_name, graph_req, meta, merged);
        }
        var merged = try queryProvisionedAcrossGroups(self, alloc, group_ids, req, table_name, consistency);
        defer merged.deinit();
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = @intCast(group_ids.len),
            .merged = group_ids.len > 1,
        };
        defer meta.deinit(alloc);
        try applyProvisionedQueryAggregations(self, alloc, group_ids, table_name, req, &merged, &meta, consistency);
        try applyQueryPostProcessing(alloc, req, &merged, &meta, self.local_termite_provider, self.secret_store);
        return try query_api.encodeQueryResponses(alloc, table_name, req, meta, merged);
    }

    fn preflightQuery(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const plan = planFanout(.preflight, self.io_impl, group_ids.len);
        recordFanoutPlan(.preflight, plan);
        if (plan.parallel) {
            return try preflightProvisionedGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, req, consistency, max_work);
        }
        if (plan.reason == .no_io and group_ids.len > 1) recordParallelFanoutFallback(.preflight);
        return try preflightProvisionedGroups(self, alloc, group_ids, table_name, req, consistency, max_work);
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        return try lookupProvisionedHostedLocal(self.primary_lookup_db, self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, key, opts, consistency);
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try preflightHostedLocal(
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.lsmRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            req,
            consistency,
            max_work,
        );
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        return try scanProvisionedHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, from_key, to_key, opts, consistency);
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        const start_ns = platform_time.monotonicNs();
        const execution = try queryHostedLocalDetailed(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, self.local_termite_provider, self.secret_store, self.remote_content, table_name, req, consistency);
        var result = execution.result;
        defer result.deinit();
        const response_req = execution.request;
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = 1,
            .dense_search = execution.dense_profile,
        };
        defer meta.deinit(alloc);
        try applyProvisionedQueryAggregations(self, alloc, &.{group_id}, table_name, response_req, &result, &meta, consistency);
        try applyQueryPostProcessing(alloc, response_req, &result, &meta, self.local_termite_provider, self.secret_store);
        return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        return try queryHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, self.local_termite_provider, self.secret_store, self.remote_content, table_name, req, consistency);
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, body);
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalAlgebraicPartials(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, body);
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const expansions = try alloc.alloc(distributed_graph.GraphExpansion, req.frontier.len);
        var initialized: usize = 0;
        errdefer {
            for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc);
            alloc.free(expansions);
        }
        for (req.frontier, 0..) |item, i| {
            const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
            defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
            var result = try queryHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, self.local_termite_provider, self.secret_store, self.remote_content, table_name, search_req, consistency);
            defer result.deinit();

            expansions[i] = .{
                .frontier_id = item.id,
                .frontier_key = try alloc.dupe(u8, item.key),
                .graph_result = graph_result_blk: {
                    var graph_result = if (result.graph_results.len > 0)
                        try distributed_graph.filterGraphSearchResult(alloc, result.graph_results[0], req.exclude_keys, req.exclude_edges)
                    else
                        try distributed_graph.emptyGraphSearchResult(alloc, req.name);
                    for (graph_result.hits) |*hit| hit.deinit(alloc);
                    if (graph_result.hits.len > 0) alloc.free(graph_result.hits);
                    graph_result.hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]);
                    break :graph_result_blk graph_result;
                },
            };
            initialized += 1;
        }

        return .{ .expansions = expansions };
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        return try executeProvisionedGraphHydrate(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.backend_runtime, table_name, req, consistency);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.runtime_status_cache) |snapshot_cache| {
            return try snapshot_cache.snapshot(alloc, table_name);
        }
        return null;
    }
};

pub const HostedProvisionedTableReadSource = struct {
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    router: table_router.HostedGroupRouter,
    executor: http_common.RequestExecutor,
    io_impl: ?*std.Io.Threaded = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,

    pub fn init(
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        requester: raft_mod.ReadableLeaseRequester,
        router: table_router.HostedGroupRouter,
        executor: http_common.RequestExecutor,
    ) HostedProvisionedTableReadSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .requester = requester,
            .router = router,
            .executor = executor,
        };
    }

    pub fn withIo(self: *HostedProvisionedTableReadSource, io_impl: *std.Io.Threaded) *HostedProvisionedTableReadSource {
        self.io_impl = io_impl;
        return self;
    }

    pub fn withBackendRuntime(self: *HostedProvisionedTableReadSource, backend_runtime: *db_mod.background_runtime.BackendRuntime) *HostedProvisionedTableReadSource {
        self.backend_runtime = backend_runtime;
        return self;
    }

    pub fn source(self: *HostedProvisionedTableReadSource) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = query,
                .preflight_query = preflightQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .join_partition_group_local = joinPartitionGroupLocal,
                .join_rows_group_local = joinRowsGroupLocal,
                .join_unmatched_group_local = joinUnmatchedGroupLocal,
                .join_finalize_group_local = joinFinalizeGroupLocal,
                .join_job_state_group_local = joinJobStateGroupLocal,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
            },
        };
    }

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, key)) orelse {
            return null;
        };
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
            return null;
        };
        defer route.deinit(alloc);

        if (try lookupViaRoute(self, alloc, route, group_id, table_name, key, opts, consistency)) |result| return result;
        return try lookupAcrossActivePlacements(self, alloc, group_id, table_name, key, opts, consistency, route);
    }

    fn lookupViaRoute(
        self: *HostedProvisionedTableReadSource,
        alloc: std.mem.Allocator,
        route: table_router.GroupRoute,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        return switch (route) {
            .local => try lookupProvisionedHostedLocal(null, null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, table_name, key, opts, consistency),
            .remote => |remote| lookupRemote(self.executor, alloc, remote.base_uri, group_id, table_name, key, opts) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn lookupAcrossActivePlacements(
        self: *HostedProvisionedTableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
        initial_route: table_router.GroupRoute,
    ) !?LookupResponse {
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);
        const placements = try metadata_admin.listGroupPlacement(alloc, &snapshot, group_id);
        defer metadata_admin.freePlacementRefs(alloc, placements);

        const local_node_id = self.router.localNodeId();
        const tried_local = initial_route == .local;
        const tried_remote_node_id = switch (initial_route) {
            .remote => |remote| remote.node_id,
            else => 0,
        };

        for (placements) |intent| {
            const node_id = intent.record.local_node_id;
            if (node_id == local_node_id) {
                if (tried_local or self.router.localStatus(group_id) != .active) continue;
                if (try lookupProvisionedHostedLocal(null, null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, table_name, key, opts, consistency)) |result| return result;
                continue;
            }
            if (node_id == tried_remote_node_id) continue;
            if (self.router.nodeStatus(node_id, group_id)) |status| {
                if (status != .active) continue;
            }
            const base_uri = (try self.router.nodeBaseUriForGroup(alloc, group_id, node_id)) orelse continue;
            defer alloc.free(base_uri);
            if (lookupRemote(self.executor, alloc, base_uri, group_id, table_name, key, opts)) |result| {
                return result;
            } else |err| switch (err) {
                error.UnexpectedHttpStatus => continue,
                else => return err,
            }
        }
        return null;
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, from_key, to_key);
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        var emitted: u32 = 0;

        for (group_ids) |group_id| {
            var group_opts = opts;
            if (opts.limit > 0) {
                if (emitted >= opts.limit) break;
                group_opts.limit = opts.limit - emitted;
            }

            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
            defer route.deinit(alloc);

            var result = switch (route) {
                .local => try scanProvisionedHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, table_name, from_key, to_key, group_opts, consistency),
                .remote => |remote| try scanRemote(self.executor, alloc, remote.base_uri, group_id, table_name, from_key, to_key, group_opts),
            } orelse return null;
            defer result.deinit(alloc);

            try out.appendSlice(alloc, result.ndjson);
            emitted += @intCast(std.mem.count(u8, result.ndjson, "\n"));
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const start_ns = platform_time.monotonicNs();
        if (group_ids.len == 1 and !distributed_graph.supportsCrossRange(req)) {
            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_ids[0], routePolicyForConsistency(consistency))) orelse return null;
            defer route.deinit(alloc);

            if (route == .local) {
                const execution = try queryHostedLocalDetailed(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_ids[0], 0, self.backend_runtime, null, null, null, table_name, req, consistency);
                var result = execution.result;
                defer result.deinit();
                const response_req = execution.request;
                var meta: query_api.QueryResponseMeta = .{
                    .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                    .shard_count = 1,
                    .dense_search = execution.dense_profile,
                };
                defer meta.deinit(alloc);
                try applyHostedProvisionedQueryAggregations(self, alloc, group_ids, table_name, response_req, &result, &meta, consistency);
                try applyQueryPostProcessing(alloc, response_req, &result, &meta, null, null);
                return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
            }
        }

        if (group_ids.len > 1 and distributed_graph.supportsCrossRange(req)) {
            var base_req = req;
            base_req.graph_queries = &.{};
            base_req.expand_strategy = null;
            var merged = try queryHostedAcrossGroups(self, alloc, group_ids, base_req, table_name, consistency);
            defer merged.deinit();
            const graph_req = requestWithResultIdentityGeneration(req, merged);

            const worker = hostedGraphWorker(self);
            const graph_results = try distributed_graph.executeCrossRange(alloc, self.catalog, worker, table_name, graph_req, merged, consistency);
            merged.graph_results = graph_results;

            var meta: query_api.QueryResponseMeta = .{
                .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                .shard_count = @intCast(group_ids.len),
                .merged = true,
            };
            defer meta.deinit(alloc);
            try applyHostedProvisionedQueryAggregations(self, alloc, group_ids, table_name, graph_req, &merged, &meta, consistency);
            try applyQueryPostProcessing(alloc, graph_req, &merged, &meta, null, null);
            return try query_api.encodeQueryResponses(alloc, table_name, graph_req, meta, merged);
        }
        var merged = try queryHostedAcrossGroups(self, alloc, group_ids, req, table_name, consistency);
        defer merged.deinit();
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = @intCast(group_ids.len),
            .merged = group_ids.len > 1,
        };
        defer meta.deinit(alloc);
        try applyHostedProvisionedQueryAggregations(self, alloc, group_ids, table_name, req, &merged, &meta, consistency);
        try applyQueryPostProcessing(alloc, req, &merged, &meta, null, null);
        return try query_api.encodeQueryResponses(alloc, table_name, req, meta, merged);
    }

    fn preflightQuery(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const plan = planFanout(.preflight, self.io_impl, group_ids.len);
        recordFanoutPlan(.preflight, plan);
        if (plan.parallel) {
            return try preflightHostedGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, req, consistency, max_work);
        }
        if (plan.reason == .no_io and group_ids.len > 1) recordParallelFanoutFallback(.preflight);

        var first_summary: ?db_mod.RuntimePreflightSummary = null;
        var keep_summary = false;
        defer if (!keep_summary) {
            if (first_summary) |*summary| summary.deinit(alloc);
        };
        for (group_ids) |group_id| {
            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
                return null;
            };
            defer route.deinit(alloc);
            switch (route) {
                .local => {
                    const summary = try preflightHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, table_name, req, consistency, max_work);
                    if (first_summary == null) {
                        first_summary = summary;
                    } else {
                        try mergeRuntimePreflightSummary(alloc, &first_summary.?, summary);
                    }
                },
                .remote => |remote| {
                    const summary = try preflightRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req, max_work);
                    if (first_summary == null) {
                        first_summary = summary;
                    } else {
                        try mergeRuntimePreflightSummary(alloc, &first_summary.?, summary);
                    }
                },
            }
        }
        keep_summary = true;
        return first_summary;
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try lookupProvisionedHostedLocal(null, null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, table_name, key, opts, consistency);
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try preflightHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, table_name, req, consistency, max_work);
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try scanProvisionedHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, table_name, from_key, to_key, opts, consistency);
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const start_ns = platform_time.monotonicNs();
        const execution = try queryHostedLocalDetailed(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, null, null, null, table_name, req, consistency);
        var result = execution.result;
        defer result.deinit();
        const response_req = execution.request;
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = 1,
            .dense_search = execution.dense_profile,
        };
        defer meta.deinit(alloc);
        try applyHostedProvisionedQueryAggregations(self, alloc, &.{group_id}, table_name, response_req, &result, &meta, consistency);
        try applyQueryPostProcessing(alloc, response_req, &result, &meta, null, null);
        return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, null, null, null, table_name, req, consistency),
            .remote => null,
        };
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, 0, self.backend_runtime, table_name, body);
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalAlgebraicPartials(null, self.replica_root_dir, self.catalog, alloc, group_id, 0, self.backend_runtime, table_name, body);
    }

    fn joinPartitionGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinPartitionRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinRowsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinRowsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinUnmatchedGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinUnmatchedRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinFinalizeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinFinalizeRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinJobStateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinJobStateRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn localRuntimeStatuses(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        return null;
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => blk: {
                try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
                const expansions = try alloc.alloc(distributed_graph.GraphExpansion, req.frontier.len);
                var initialized: usize = 0;
                errdefer {
                    for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc);
                    alloc.free(expansions);
                }
                for (req.frontier, 0..) |item, i| {
                    const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
                    defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
                    var result = try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, null, null, null, table_name, search_req, consistency);
                    defer result.deinit();

                    expansions[i] = .{
                        .frontier_id = item.id,
                        .frontier_key = try alloc.dupe(u8, item.key),
                        .graph_result = graph_blk: {
                            var graph_result = if (result.graph_results.len > 0)
                                try distributed_graph.filterGraphSearchResult(alloc, result.graph_results[0], req.exclude_keys, req.exclude_edges)
                            else
                                try distributed_graph.emptyGraphSearchResult(alloc, req.name);
                            for (graph_result.hits) |*hit| hit.deinit(alloc);
                            if (graph_result.hits.len > 0) alloc.free(graph_result.hits);
                            graph_result.hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]);
                            break :graph_blk graph_result;
                        },
                    };
                    initialized += 1;
                }
                break :blk .{ .expansions = expansions };
            },
            .remote => |remote| blk: {
                if (req.resolved_doc_filter != null) {
                    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
                    try table_catalog.validateResolvedDocFilterContextForGroups(
                        alloc,
                        self.catalog,
                        table_name,
                        &.{group_id},
                        ctx.namespace.table_id,
                        ctx.namespace.shard_id,
                        ctx.namespace.range_id,
                    );
                }
                break :blk try graphExpandRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req);
            },
        };
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        return try executeHostedGraphHydrate(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => try graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.backend_runtime, table_name, req, consistency),
            .remote => |remote| try graphEdgesRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req),
        };
    }
};

fn routePolicyForConsistency(consistency: raft_mod.ReadConsistency) table_router.RoutePolicy {
    return switch (consistency) {
        .stale => .any_active,
        .leader_lease, .read_index => .prefer_leader,
    };
}

const TextStatsFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    fields: []const distributed_stats_mod.TextFieldStats = &.{},
    err: ?anyerror = null,

    fn init() TextStatsFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *TextStatsFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const SearchFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    result: ?db_mod.types.SearchResult = null,
    err: ?anyerror = null,

    fn init() SearchFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *SearchFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const PreflightFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    summary: ?db_mod.RuntimePreflightSummary = null,
    err: ?anyerror = null,

    fn init() PreflightFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *PreflightFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn initTextStatsFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]TextStatsFanoutSlot {
    const slots = try alloc.alloc(TextStatsFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

fn deinitTextStatsFanoutSlots(alloc: std.mem.Allocator, slots: []TextStatsFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

fn initSearchFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]SearchFanoutSlot {
    const slots = try alloc.alloc(SearchFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

fn deinitSearchFanoutSlots(alloc: std.mem.Allocator, slots: []SearchFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

fn initPreflightFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]PreflightFanoutSlot {
    const slots = try alloc.alloc(PreflightFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

fn deinitPreflightFanoutSlots(alloc: std.mem.Allocator, slots: []PreflightFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

fn collectProvisionedSearchRequestTextStatsParallel(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    body: []const u8,
) ![]const distributed_stats_mod.TextFieldStats {
    const start_ns = platform_time.monotonicNs();
    const slots = try initTextStatsFanoutSlots(alloc, group_ids.len);
    defer deinitTextStatsFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *ProvisionedTableReadSource,
            slot: *TextStatsFanoutSlot,
            group_id: u64,
            table_name_inner: []const u8,
            body_inner: []const u8,
        ) void {
            const arena = slot.arena.allocator();
            var response = collectProvisionedHostedLocalTextStats(
                source.cache,
                source.replica_root_dir,
                source.catalog,
                arena,
                group_id,
                source.lsmRootGeneration(group_id),
                source.backend_runtime,
                table_name_inner,
                body_inner,
            ) catch |err| {
                slot.err = err;
                return;
            } orelse {
                slot.err = error.TableNotFound;
                return;
            };
            defer response.deinit(arena);
            slot.fields = parseTextStatsResponse(arena, response.json) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], group_id, table_name, body });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    defer alloc.free(shard_stats);
    for (slots, 0..) |slot, i| shard_stats[i] = slot.fields;
    const merged = try mergeDistributedTextStats(alloc, shard_stats);
    recordParallelFanout(.text_stats, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn resolveHostedShardRoutes(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    consistency: raft_mod.ReadConsistency,
) ![]table_router.GroupRoute {
    const routes = try alloc.alloc(table_router.GroupRoute, group_ids.len);
    errdefer alloc.free(routes);
    var initialized: usize = 0;
    errdefer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
    }
    for (group_ids, 0..) |group_id, i| {
        routes[i] = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        initialized += 1;
    }
    return routes;
}

fn deinitHostedShardRoutes(alloc: std.mem.Allocator, routes: []table_router.GroupRoute) void {
    for (routes) |*route| route.deinit(alloc);
    alloc.free(routes);
}

fn collectHostedSearchRequestTextStatsParallel(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    body: []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]const distributed_stats_mod.TextFieldStats {
    const start_ns = platform_time.monotonicNs();
    const routes = try resolveHostedShardRoutes(self, alloc, group_ids, consistency);
    defer deinitHostedShardRoutes(alloc, routes);

    const slots = try initTextStatsFanoutSlots(alloc, group_ids.len);
    defer deinitTextStatsFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *HostedProvisionedTableReadSource,
            slot: *TextStatsFanoutSlot,
            route: table_router.GroupRoute,
            group_id: u64,
            table_name_inner: []const u8,
            body_inner: []const u8,
        ) void {
            const arena = slot.arena.allocator();
            var response = switch (route) {
                .local => collectProvisionedHostedLocalTextStats(
                    null,
                    source.replica_root_dir,
                    source.catalog,
                    arena,
                    group_id,
                    0,
                    source.backend_runtime,
                    table_name_inner,
                    body_inner,
                ),
                .remote => |remote| textStatsRemote(source.executor, arena, remote.base_uri, group_id, table_name_inner, body_inner),
            } catch |err| {
                slot.err = err;
                return;
            } orelse {
                slot.err = error.TableNotFound;
                return;
            };
            defer response.deinit(arena);
            slot.fields = parseTextStatsResponse(arena, response.json) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], routes[i], group_id, table_name, body });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    defer alloc.free(shard_stats);
    for (slots, 0..) |slot, i| shard_stats[i] = slot.fields;
    const merged = try mergeDistributedTextStats(alloc, shard_stats);
    recordParallelFanout(.text_stats, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn queryProvisionedAcrossGroupsParallel(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    shard_req: *const db_mod.types.SearchRequest,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const start_ns = platform_time.monotonicNs();
    const slots = try initSearchFanoutSlots(alloc, group_ids.len);
    defer deinitSearchFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *ProvisionedTableReadSource,
            slot: *SearchFanoutSlot,
            group_id: u64,
            table_name_inner: []const u8,
            shard_req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
        ) void {
            const arena = slot.arena.allocator();
            slot.result = queryHostedLocal(
                source.cache,
                source.replica_root_dir,
                source.catalog,
                source.requester,
                arena,
                group_id,
                source.lsmRootGeneration(group_id),
                source.backend_runtime,
                source.local_termite_provider,
                source.secret_store,
                source.remote_content,
                table_name_inner,
                shard_req_inner.*,
                consistency_inner,
            ) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], group_id, table_name, shard_req, consistency });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    defer alloc.free(shard_results);
    for (slots, 0..) |slot, i| shard_results[i] = slot.result.?;
    const merged = try query_api.mergeSearchResults(alloc, req, shard_results, req.offset, req.limit);
    recordParallelFanout(.query, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn queryHostedAcrossGroupsParallel(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    shard_req: *const db_mod.types.SearchRequest,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const start_ns = platform_time.monotonicNs();
    const routes = try resolveHostedShardRoutes(self, alloc, group_ids, consistency);
    defer deinitHostedShardRoutes(alloc, routes);

    const slots = try initSearchFanoutSlots(alloc, group_ids.len);
    defer deinitSearchFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *HostedProvisionedTableReadSource,
            slot: *SearchFanoutSlot,
            route: table_router.GroupRoute,
            group_id: u64,
            table_name_inner: []const u8,
            shard_req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
        ) void {
            const arena = slot.arena.allocator();
            slot.result = switch (route) {
                .local => queryHostedLocal(
                    null,
                    source.replica_root_dir,
                    source.catalog,
                    source.requester,
                    arena,
                    group_id,
                    0,
                    source.backend_runtime,
                    null,
                    null,
                    null,
                    table_name_inner,
                    shard_req_inner.*,
                    consistency_inner,
                ),
                .remote => |remote| queryRemote(source.executor, arena, remote.base_uri, group_id, table_name_inner, shard_req_inner.*),
            } catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], routes[i], group_id, table_name, shard_req, consistency });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    defer alloc.free(shard_results);
    for (slots, 0..) |slot, i| shard_results[i] = slot.result.?;
    const merged = try query_api.mergeSearchResults(alloc, req, shard_results, req.offset, req.limit);
    recordParallelFanout(.query, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn cloneRuntimePreflightSummary(
    alloc: std.mem.Allocator,
    summary: db_mod.RuntimePreflightSummary,
) !db_mod.RuntimePreflightSummary {
    var cloned: db_mod.RuntimePreflightSummary = .{};
    errdefer cloned.deinit(alloc);
    try mergeRuntimePreflightSummaryNoFree(alloc, &cloned, summary);
    return cloned;
}

fn preflightProvisionedGroupsParallel(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !?db_mod.RuntimePreflightSummary {
    const start_ns = platform_time.monotonicNs();
    const slots = try initPreflightFanoutSlots(alloc, group_ids.len);
    defer deinitPreflightFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *ProvisionedTableReadSource,
            slot: *PreflightFanoutSlot,
            group_id: u64,
            table_name_inner: []const u8,
            req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
            max_work_inner: u32,
        ) void {
            const arena = slot.arena.allocator();
            slot.summary = preflightHostedLocal(
                source.cache,
                source.replica_root_dir,
                source.catalog,
                source.requester,
                arena,
                group_id,
                source.lsmRootGeneration(group_id),
                source.backend_runtime,
                table_name_inner,
                req_inner.*,
                consistency_inner,
                max_work_inner,
            ) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], group_id, table_name, &req, consistency, max_work });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    var merged = try cloneRuntimePreflightSummary(alloc, slots[0].summary.?);
    errdefer merged.deinit(alloc);
    for (slots[1..]) |slot| {
        try mergeRuntimePreflightSummaryNoFree(alloc, &merged, slot.summary.?);
    }
    recordParallelFanout(.preflight, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn preflightHostedGroupsParallel(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !?db_mod.RuntimePreflightSummary {
    const start_ns = platform_time.monotonicNs();
    const routes = try alloc.alloc(table_router.GroupRoute, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
        alloc.free(routes);
    }
    for (group_ids, 0..) |group_id, i| {
        routes[i] = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
            return null;
        };
        initialized += 1;
    }

    const slots = try initPreflightFanoutSlots(alloc, group_ids.len);
    defer deinitPreflightFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *HostedProvisionedTableReadSource,
            slot: *PreflightFanoutSlot,
            route: table_router.GroupRoute,
            group_id: u64,
            table_name_inner: []const u8,
            req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
            max_work_inner: u32,
        ) void {
            const arena = slot.arena.allocator();
            slot.summary = switch (route) {
                .local => preflightHostedLocal(
                    null,
                    source.replica_root_dir,
                    source.catalog,
                    source.requester,
                    arena,
                    group_id,
                    0,
                    source.backend_runtime,
                    table_name_inner,
                    req_inner.*,
                    consistency_inner,
                    max_work_inner,
                ),
                .remote => |remote| preflightRemote(
                    source.executor,
                    arena,
                    remote.base_uri,
                    group_id,
                    table_name_inner,
                    req_inner.*,
                    max_work_inner,
                ),
            } catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], routes[i], group_id, table_name, &req, consistency, max_work });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    var merged = try cloneRuntimePreflightSummary(alloc, slots[0].summary.?);
    errdefer merged.deinit(alloc);
    for (slots[1..]) |slot| {
        try mergeRuntimePreflightSummaryNoFree(alloc, &merged, slot.summary.?);
    }
    recordParallelFanout(.preflight, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn rejectCrossGroupResolvedDocFilter(req: db_mod.types.SearchRequest, group_count: usize) !void {
    if (group_count > 1 and searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
}

fn validateResolvedDocFilterForGroups(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
) !void {
    if (!searchRequestHasResolvedDocFilter(req)) return;
    if (searchRequestHasUnserializableResolvedDocFilter(req)) {
        if (group_ids.len > 1) return error.UnsupportedQueryRequest;
        return;
    }
    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
    try table_catalog.validateResolvedDocFilterContextForGroups(
        alloc,
        catalog,
        table_name,
        group_ids,
        ctx.namespace.table_id,
        ctx.namespace.shard_id,
        ctx.namespace.range_id,
    );
}

fn tableReadsValidateDocIdentityReadyForMultiGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_count: usize,
) !void {
    if (group_count <= 1) return;
    try table_catalog.validateDocIdentityReadyForTable(alloc, catalog, table_name);
}

fn rejectRemoteRouteResolvedDocFilter(req: db_mod.types.SearchRequest, route: table_router.GroupRoute) !void {
    if (!searchRequestHasUnserializableResolvedDocFilter(req)) return;
    switch (route) {
        .local => {},
        .remote => return error.UnsupportedQueryRequest,
    }
}

fn validateResolvedDocFilterForRemoteRoute(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.SearchRequest,
    route: table_router.GroupRoute,
) !void {
    switch (route) {
        .local => {},
        .remote => {
            if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
            try validateResolvedDocFilterForGroups(alloc, catalog, table_name, &.{group_id}, req);
        },
    }
}

fn rejectHostedRemoteResolvedDocFilter(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (!searchRequestHasResolvedDocFilter(req) or group_ids.len != 1) return;
    var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_ids[0], routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
    defer route.deinit(alloc);
    try validateResolvedDocFilterForRemoteRoute(alloc, self.catalog, table_name, group_ids[0], req, route);
}

const DocIdentityInternalWorkerBoundary = enum {
    query,
    vector_worker,
    preflight,
    graph_expand,
    graph_hydrate,
    graph_edges,
    search_request_text_stats,
    explicit_text_stats,
    background_text_stats,
    aggregation_context,
    aggregation_full_result_rerun,
    algebraic_partials,
    distributed_join_right_fanout,
    distributed_join_worker,
    distributed_join_unmatched_followup,
    distributed_join_finalizer,
    shuffle_worker,
    shuffle_finalizer,
    graph_result_ref,
};

const DocIdentityInternalWorkerPolicy = enum {
    carries_shard_doc_set,
    validates_generation_projection,
    fail_closed_before_fanout,
};

fn docIdentityInternalWorkerPolicy(boundary: DocIdentityInternalWorkerBoundary) DocIdentityInternalWorkerPolicy {
    return switch (boundary) {
        .query,
        .vector_worker,
        .preflight,
        .graph_expand,
        .graph_hydrate,
        .search_request_text_stats,
        .explicit_text_stats,
        .background_text_stats,
        => .carries_shard_doc_set,

        .graph_edges,
        .aggregation_context,
        .aggregation_full_result_rerun,
        .algebraic_partials,
        .distributed_join_right_fanout,
        .distributed_join_worker,
        .distributed_join_unmatched_followup,
        .distributed_join_finalizer,
        .shuffle_worker,
        .shuffle_finalizer,
        .graph_result_ref,
        => .validates_generation_projection,
    };
}

fn searchRequestHasResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    if (comptime @hasField(db_mod.types.SearchRequest, "resolved_doc_filter")) {
        return req.resolved_doc_filter != null;
    }
    return false;
}

fn searchRequestHasUnserializableResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    return searchRequestHasResolvedDocFilter(req) and req.resolved_doc_filter_wire_context == null;
}

fn graphHydrateRequestHasResolvedDocFilter(req: distributed_graph.GraphHydrateRequest) bool {
    return req.resolved_doc_filter != null;
}

fn validateGraphHydrateResolvedDocFilterForDb(req: distributed_graph.GraphHydrateRequest, db: *db_mod.DB) !void {
    if (!graphHydrateRequestHasResolvedDocFilter(req)) return;
    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
    if (!ctx.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
    const generation = try db.currentIdentityReadGenerationForRequest(req.identity_read_generation);
    if (generation != ctx.identity_read_generation) return error.UnsupportedQueryRequest;
}

fn graphHydrateResolvedDocFilterAllows(req: distributed_graph.GraphHydrateRequest, key: []const u8, ordinal: ?doc_set.DocOrdinal) bool {
    const ptr = req.resolved_doc_filter orelse return true;
    const filter: *const doc_set.ResolvedDocFilter = @ptrCast(@alignCast(ptr));
    return graphHydrateResolvedDocSetIncludes(&filter.include, key, ordinal) and
        !graphHydrateResolvedDocSetIncludes(&filter.exclude, key, ordinal);
}

fn graphHydrateResolvedDocSetIncludes(set: *const doc_set.ResolvedDocSet, key: []const u8, ordinal: ?doc_set.DocOrdinal) bool {
    return switch (set.*) {
        .all => true,
        .none => false,
        .doc_keys => |keys| blk: {
            for (keys) |candidate| {
                if (std.mem.eql(u8, candidate, key)) break :blk true;
            }
            break :blk false;
        },
        .ordinals, .ordinal_bitmap => if (ordinal) |value| set.containsOrdinal(value) else false,
    };
}

fn queryProvisionedAcrossGroups(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
    const distributed_text_stats = try collectProvisionedSearchRequestTextStats(self, alloc, group_ids, req, table_name);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, distributed_text_stats);
    const shard_limit = req.limit + req.offset;
    const shard_req = blk: {
        var copy = req;
        copy.offset = 0;
        copy.limit = if (shard_limit == 0) req.limit else shard_limit;
        copy.distributed_text_stats = distributed_text_stats;
        break :blk copy;
    };

    const plan = planQueryFanout(self.io_impl, group_ids.len, req);
    recordFanoutPlan(.query, plan);
    if (plan.parallel) {
        return try queryProvisionedAcrossGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, &shard_req, req, table_name, consistency);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.query);

    var shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_results[0..initialized]) |*result| result.deinit();
        alloc.free(shard_results);
    }

    for (group_ids, 0..) |group_id, i| {
        shard_results[i] = try queryHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, self.local_termite_provider, self.secret_store, self.remote_content, table_name, shard_req, consistency);
        initialized += 1;
    }
    return try query_api.mergeSearchResults(alloc, req, shard_results[0..initialized], req.offset, req.limit);
}

fn queryHostedAcrossGroups(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
    try rejectHostedRemoteResolvedDocFilter(self, alloc, group_ids, table_name, req, consistency);
    const distributed_text_stats = try collectHostedSearchRequestTextStats(self, alloc, group_ids, req, table_name, consistency);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, distributed_text_stats);
    const shard_limit = req.limit + req.offset;
    const shard_req = blk: {
        var copy = req;
        copy.offset = 0;
        copy.limit = if (shard_limit == 0) req.limit else shard_limit;
        copy.distributed_text_stats = distributed_text_stats;
        break :blk copy;
    };

    const plan = planQueryFanout(self.io_impl, group_ids.len, req);
    recordFanoutPlan(.query, plan);
    if (plan.parallel) {
        return try queryHostedAcrossGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, &shard_req, req, table_name, consistency);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.query);

    var shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_results[0..initialized]) |*result| result.deinit();
        alloc.free(shard_results);
    }

    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        shard_results[i] = switch (route) {
            .local => try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, null, null, null, table_name, shard_req, consistency),
            .remote => |remote| try queryRemote(self.executor, alloc, remote.base_uri, group_id, table_name, shard_req),
        };
        initialized += 1;
    }
    return try query_api.mergeSearchResults(alloc, req, shard_results[0..initialized], req.offset, req.limit);
}

fn provisionedGraphWorker(self: *ProvisionedTableReadSource) distributed_graph.Worker {
    return .{
        .ptr = self,
        .vtable = &.{
            .execute_graph_expand = executeProvisionedGraphExpand,
            .execute_graph_hydrate = executeProvisionedGraphHydrate,
            .execute_graph_get_edges = executeProvisionedGraphGetEdges,
            .fanout_io = provisionedGraphFanoutIo,
            .fanout_width_cap = provisionedGraphFanoutWidthCap,
        },
    };
}

fn hostedGraphWorker(self: *HostedProvisionedTableReadSource) distributed_graph.Worker {
    return .{
        .ptr = self,
        .vtable = &.{
            .execute_graph_expand = executeHostedGraphExpand,
            .execute_graph_hydrate = executeHostedGraphHydrate,
            .execute_graph_get_edges = executeHostedGraphGetEdges,
            .fanout_io = hostedGraphFanoutIo,
            .fanout_width_cap = hostedGraphFanoutWidthCap,
        },
    };
}

fn provisionedGraphFanoutIo(ptr: *anyopaque) ?std.Io {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return null;
    return io_impl.io();
}

fn provisionedGraphFanoutWidthCap(ptr: *anyopaque) usize {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return 1;
    return ioAsyncLimitCap(io_impl);
}

fn hostedGraphFanoutIo(ptr: *anyopaque) ?std.Io {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return null;
    return io_impl.io();
}

fn hostedGraphFanoutWidthCap(ptr: *anyopaque) usize {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return 1;
    return ioAsyncLimitCap(io_impl);
}

fn executeProvisionedGraphExpand(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphExpandResponse {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
    const expansions = try alloc.alloc(distributed_graph.GraphExpansion, req.frontier.len);
    var initialized: usize = 0;
    errdefer {
        for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc);
        alloc.free(expansions);
    }

    for (req.frontier, 0..) |item, i| {
        const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
        defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
        var result = try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, 0, self.backend_runtime, null, null, null, table_name, search_req, consistency);
        defer result.deinit();

        expansions[i] = .{
            .frontier_id = item.id,
            .frontier_key = try alloc.dupe(u8, item.key),
            .graph_result = graph_result_blk: {
                var graph_result = if (result.graph_results.len > 0)
                    try distributed_graph.filterGraphSearchResult(alloc, result.graph_results[0], req.exclude_keys, req.exclude_edges)
                else
                    try distributed_graph.emptyGraphSearchResult(alloc, req.name);
                for (graph_result.hits) |*hit| hit.deinit(alloc);
                if (graph_result.hits.len > 0) alloc.free(graph_result.hits);
                graph_result.hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]);
                break :graph_result_blk graph_result;
            },
        };
        initialized += 1;
    }
    return .{ .expansions = expansions };
}

fn executeProvisionedGraphHydrate(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphHydrateRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphHydrateResponse {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, self.lsmRootGeneration(group_id), self.backend_runtime);
    defer db.close();
    try validateGraphHydrateResolvedDocFilterForDb(req, &db);

    var reads = raft_mod.FeatureDBReads.init(group_id, self.requester);
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (req.keys) |key| {
        var result = (reads.lookupWithConsistency(alloc, &db, key, .{}, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale) return err else try reads.lookupWithConsistency(alloc, &db, key, .{}, .stale),
            else => return err,
        }) orelse continue;
        defer result.deinit(alloc);
        const ordinal = try db.lookupLiveDocOrdinalForInternalRead(alloc, key, req.identity_read_generation);
        if (!graphHydrateResolvedDocFilterAllows(req, key, ordinal)) continue;
        try hits.append(alloc, .{
            .id = try alloc.dupe(u8, key),
            .doc_ordinal = ordinal,
            .stored_data = try alloc.dupe(u8, result.json),
        });
    }
    return .{ .hits = try hits.toOwnedSlice(alloc) };
}

fn executeHostedGraphExpand(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphExpandResponse {
    return (try HostedProvisionedTableReadSource.graphExpandGroupLocal(ptr, alloc, group_id, table_name, req, consistency)) orelse return error.TableNotFound;
}

fn executeHostedGraphHydrate(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphHydrateRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphHydrateResponse {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
    defer route.deinit(alloc);

    return switch (route) {
        .local => blk: {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);
            const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
            var db = try db_mod.DB.open(alloc, path, .{
                .backend_runtime = self.backend_runtime,
                .identity_namespace = identity_namespace,
                .prefer_existing_identity_namespace = identity_namespace != null,
            });
            defer db.close();
            try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
            try validateGraphHydrateResolvedDocFilterForDb(req, &db);

            var reads = raft_mod.FeatureDBReads.init(group_id, self.requester);
            var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            errdefer {
                for (hits.items) |*hit| hit.deinit(alloc);
                hits.deinit(alloc);
            }

            for (req.keys) |key| {
                var result = (try reads.lookupWithConsistency(alloc, &db, key, .{}, consistency)) orelse continue;
                defer result.deinit(alloc);
                const ordinal = try db.lookupLiveDocOrdinalForInternalRead(alloc, key, req.identity_read_generation);
                if (!graphHydrateResolvedDocFilterAllows(req, key, ordinal)) continue;
                try hits.append(alloc, .{
                    .id = try alloc.dupe(u8, key),
                    .doc_ordinal = ordinal,
                    .stored_data = try alloc.dupe(u8, result.json),
                });
            }
            break :blk .{ .hits = try hits.toOwnedSlice(alloc) };
        },
        .remote => |remote| blk: {
            if (req.resolved_doc_filter != null) {
                const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
                try table_catalog.validateResolvedDocFilterContextForGroups(
                    alloc,
                    self.catalog,
                    table_name,
                    &.{group_id},
                    ctx.namespace.table_id,
                    ctx.namespace.shard_id,
                    ctx.namespace.range_id,
                );
            }
            break :blk try graphHydrateRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req);
        },
    };
}

fn executeProvisionedGraphGetEdges(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
    consistency: raft_mod.ReadConsistency,
) anyerror!distributed_graph.GraphEdgesResponse {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    return graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.backend_runtime, table_name, req, consistency);
}

fn executeHostedGraphGetEdges(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
    consistency: raft_mod.ReadConsistency,
) anyerror!distributed_graph.GraphEdgesResponse {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
    defer route.deinit(alloc);

    return switch (route) {
        .local => graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.backend_runtime, table_name, req, consistency),
        .remote => |remote| try graphEdgesRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req),
    };
}

fn graphGetEdgesLocal(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    group_id: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
    consistency: raft_mod.ReadConsistency,
) anyerror!distributed_graph.GraphEdgesResponse {
    try table_catalog.validateTopologyEpoch(alloc, catalog, table_name, req.topology_epoch);
    try distributed_graph.validateGraphEdgesTensorAccessPath(alloc, req);

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    var db = try db_mod.DB.open(alloc, path, .{
        .backend_runtime = backend_runtime,
        .identity_namespace = identity_namespace,
        .prefer_existing_identity_namespace = identity_namespace != null,
    });
    defer db.close();
    try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
    _ = try currentIdentityReadGenerationForDb(req.identity_read_generation, &db);

    const reads = raft_mod.FeatureDBReads.init(group_id, requester);
    try reads.reads.prepareLookupWithConsistency(group_id, req.key, .{}, consistency);

    const graph_entry = db.core.graphIndex(req.index_name) orelse return error.IndexNotFound;
    const edges = try graph_entry.index.getEdges(alloc, req.key, "", req.direction);
    return .{ .edges = edges };
}

fn lookupLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    var result = (try reads.lookupWithConsistency(alloc, &db, key, opts, consistency)) orelse return null;
    defer result.deinit(alloc);
    return .{
        .json = try alloc.dupe(u8, result.json),
        .version = try db.getTimestamp(alloc, key),
    };
}

fn lookupProvisionedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    if (primary_lookup_db) |source| {
        if (try source.leaseGroup(alloc, table_name, group_id, lsm_root_generation)) |lease_value| {
            var lease = lease_value;
            defer lease.release(alloc);
            try validateProvisionedDbIdentityNamespace(alloc, catalog, table_name, group_id, lease.db);

            var reads = raft_mod.FeatureDBReads.init(group_id, requester);
            var result = (try reads.lookupWithConsistency(alloc, lease.db, key, opts, consistency)) orelse return null;
            defer result.deinit(alloc);
            return .{
                .json = try alloc.dupe(u8, result.json),
                .version = try lease.db.getTimestamp(alloc, key),
            };
        }
    }

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
        if (cached.getIfPresent(group_id, lsm_root_generation, identity_namespace, table_name)) |db_lease_value| {
            var db_lease = db_lease_value;
            defer db_lease.release();
            const db = db_lease.db;

            var reads = raft_mod.FeatureDBReads.init(group_id, requester);
            var result = (try reads.lookupWithConsistency(alloc, db, key, opts, consistency)) orelse return null;
            defer result.deinit(alloc);
            return .{
                .json = try alloc.dupe(u8, result.json),
                .version = try db.getTimestamp(alloc, key),
            };
        }
    }

    var db = try openProvisionedLookupDbForTable(
        alloc,
        path,
        if (cache) |cached| cached.lsm_cache else null,
        lsm_root_generation,
        if (cache) |cached| cached.resource_manager else null,
        backend_runtime,
        try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id),
    );
    defer db.close();

    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    var result = (try reads.lookupWithConsistency(alloc, &db, key, opts, consistency)) orelse return null;
    defer result.deinit(alloc);
    return .{
        .json = try alloc.dupe(u8, result.json),
        .version = try db.getTimestamp(alloc, key),
    };
}

fn lookupHostedLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    return lookupLocal(replica_root_dir, requester, alloc, group_id, key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try lookupLocal(replica_root_dir, requester, alloc, group_id, key, opts, .stale),
        else => err,
    };
}

fn lookupProvisionedHostedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    return lookupProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try lookupProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, key, opts, .stale),
        else => err,
    };
}

fn scanLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    var result = try reads.scanWithConsistency(alloc, &db, from_key, to_key, opts, consistency);
    defer result.deinit(alloc);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (result.hashes, 0..) |entry, i| {
        const json = if (opts.include_documents) result.documents[i].json else null;
        try appendScanLine(alloc, &out, entry.id, json);
    }
    return .{ .ndjson = try out.toOwnedSlice(alloc) };
}

fn scanProvisionedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        var result = try reads.scanWithConsistency(alloc, db, from_key, to_key, opts, consistency);
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        for (result.hashes, 0..) |entry, i| {
            const json = if (opts.include_documents) result.documents[i].json else null;
            try appendScanLine(alloc, &out, entry.id, json);
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    } else {
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
        defer db.close();

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        var result = try reads.scanWithConsistency(alloc, &db, from_key, to_key, opts, consistency);
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        for (result.hashes, 0..) |entry, i| {
            const json = if (opts.include_documents) result.documents[i].json else null;
            try appendScanLine(alloc, &out, entry.id, json);
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    }
}

fn scanHostedLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    return scanLocal(replica_root_dir, requester, alloc, group_id, from_key, to_key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try scanLocal(replica_root_dir, requester, alloc, group_id, from_key, to_key, opts, .stale),
        else => err,
    };
}

fn scanProvisionedHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    return scanProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, from_key, to_key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try scanProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, from_key, to_key, opts, .stale),
        else => err,
    };
}

fn queryLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const detailed = try queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, local_termite_provider, secret_store, remote_content, table_name, req, consistency);
    var result = detailed.result;
    result.identity_read_generation = detailed.request.identity_read_generation;
    return result;
}

fn mapDenseSearchProfile(profile: db_query_search.DenseSearchProfile) query_api.QueryResponseMeta.DenseSearchProfile {
    return .{
        .total_ns = profile.total_ns,
        .index_lookup_ns = profile.index_lookup_ns,
        .hbc_search_ns = profile.hbc_search_ns,
        .hbc_runtime_txn_ns = profile.hbc_runtime_txn_ns,
        .hbc_scratch_acquire_ns = profile.hbc_scratch_acquire_ns,
        .hbc_node_cache_lookup_ns = profile.hbc_node_cache_lookup_ns,
        .hbc_quantized_cache_lookup_ns = profile.hbc_quantized_cache_lookup_ns,
        .resolved_search_width = profile.resolved_search_width,
        .resolved_epsilon = profile.resolved_epsilon,
        .hbc_nodes_visited = profile.hbc_nodes_visited,
        .hbc_leaves_explored = profile.hbc_leaves_explored,
        .hbc_approx_vectors_scored = profile.hbc_approx_vectors_scored,
        .hbc_exact_vectors_scored = profile.hbc_exact_vectors_scored,
        .hbc_reranked_vectors = profile.hbc_reranked_vectors,
        .hbc_approx_candidate_count = profile.hbc_approx_candidate_count,
        .hbc_rerank_candidate_count = profile.hbc_rerank_candidate_count,
        .hbc_ambiguous_top_k_pairs = profile.hbc_ambiguous_top_k_pairs,
        .hbc_ambiguous_boundary_pairs = profile.hbc_ambiguous_boundary_pairs,
        .hbc_ambiguous_distance_over_hits = profile.hbc_ambiguous_distance_over_hits,
        .hbc_ambiguous_distance_under_hits = profile.hbc_ambiguous_distance_under_hits,
        .hbc_full_rerank_due_to_threshold = profile.hbc_full_rerank_due_to_threshold,
        .hbc_top_k_count = profile.hbc_top_k_count,
        .hbc_min_distance_gap_top_k = profile.hbc_min_distance_gap_top_k,
        .hbc_min_interval_gap_top_k = profile.hbc_min_interval_gap_top_k,
        .hbc_closest_pair_top_k = if (profile.hbc_closest_pair_top_k) |pair| mapDenseDebugPair(pair) else null,
        .hbc_boundary_pair = if (profile.hbc_boundary_pair) |pair| mapDenseDebugPair(pair) else null,
        .hbc_boundary_tail_error_avg = profile.hbc_boundary_tail_error_avg,
        .hbc_boundary_tail_error_max = profile.hbc_boundary_tail_error_max,
        .hbc_boundary_tail_distance_gap_avg = profile.hbc_boundary_tail_distance_gap_avg,
        .hbc_boundary_tail_distance_gap_min = profile.hbc_boundary_tail_distance_gap_min,
        .hbc_boundary_tail_distance_gap_max = profile.hbc_boundary_tail_distance_gap_max,
        .hbc_boundary_tail_interval_gap_avg = profile.hbc_boundary_tail_interval_gap_avg,
        .hbc_boundary_tail_interval_gap_min = profile.hbc_boundary_tail_interval_gap_min,
        .hbc_boundary_tail_interval_gap_max = profile.hbc_boundary_tail_interval_gap_max,
        .hbc_approx_top_count = profile.hbc_approx_top_count,
        .hbc_approx_top = blk: {
            var out: [5]query_api.QueryResponseMeta.DenseSearchProfile.DebugHit = .{ .{}, .{}, .{}, .{}, .{} };
            for (profile.hbc_approx_top, 0..) |hit, i| out[i] = mapDenseDebugHit(hit);
            break :blk out;
        },
        .hbc_rerank_external_score_ns = profile.hbc_rerank_external_score_ns,
        .hbc_rerank_vector_load_ns = profile.hbc_rerank_vector_load_ns,
        .hbc_rerank_metadata_lookup_ns = profile.hbc_rerank_metadata_lookup_ns,
        .hbc_rerank_artifact_key_ns = profile.hbc_rerank_artifact_key_ns,
        .hbc_rerank_artifact_read_ns = profile.hbc_rerank_artifact_read_ns,
        .hbc_rerank_artifact_decode_ns = profile.hbc_rerank_artifact_decode_ns,
        .hbc_rerank_artifact_distance_ns = profile.hbc_rerank_artifact_distance_ns,
        .hbc_rerank_lsm_cache_hits = profile.hbc_rerank_lsm_cache_hits,
        .hbc_rerank_lsm_cache_misses = profile.hbc_rerank_lsm_cache_misses,
        .hbc_rerank_distance_ns = profile.hbc_rerank_distance_ns,
        .doc_key_resolve_ns = profile.doc_key_resolve_ns,
        .load_projected_document_ns = profile.load_projected_document_ns,
        .postprocess_ns = profile.postprocess_ns,
        .raw_hit_count = profile.raw_hit_count,
        .returned_hit_count = profile.returned_hit_count,
        .inline_metadata_hits = profile.inline_metadata_hits,
        .fetched_metadata_hits = profile.fetched_metadata_hits,
        .lookup_doc_key_hits = profile.lookup_doc_key_hits,
    };
}

fn mapDenseDebugHit(hit: db_query_search.DenseSearchProfile.DebugHit) query_api.QueryResponseMeta.DenseSearchProfile.DebugHit {
    return .{
        .id = hit.id,
        .distance = hit.distance,
        .error_bound = hit.error_bound,
        .lower_bound = hit.lower_bound,
        .upper_bound = hit.upper_bound,
    };
}

fn mapDenseDebugPair(pair: db_query_search.DenseSearchProfile.DebugPair) query_api.QueryResponseMeta.DenseSearchProfile.DebugPair {
    return .{
        .left = mapDenseDebugHit(pair.left),
        .right = mapDenseDebugHit(pair.right),
        .distance_gap = pair.distance_gap,
        .interval_gap = pair.interval_gap,
        .overlaps = pair.overlaps,
    };
}

fn profiledDenseQuery(req: db_mod.types.SearchRequest) ?ProfiledDenseQuery {
    if (!req.profile) return null;
    if (req.full_text != null or req.full_text_queries.len > 0) return null;
    if (req.sparse != null or req.sparse_queries.len > 0) return null;
    if (req.graph_queries.len > 0) return null;
    if (req.dense_queries.len > 1) return null;
    if (req.merge_config != null) return null;
    if (req.reranker != null) return null;
    if (req.dense_queries.len == 1) {
        var dense_req = req;
        dense_req.index_name = req.dense_queries[0].index_name;
        return .{
            .req = dense_req,
            .query = req.dense_queries[0].query,
        };
    }
    return if (req.dense) |dense|
        .{ .req = req, .query = dense }
    else switch (req.query) {
        .dense_knn => |dense| .{ .req = req, .query = dense },
        else => null,
    };
}

fn readPreparationKindForQuery(req: db_mod.types.SearchRequest) ReadPreparation.Kind {
    return if (isDenseOnlyQuery(req)) .dense_query else .general;
}

fn isDenseOnlyQuery(req: db_mod.types.SearchRequest) bool {
    if (req.full_text != null or req.full_text_queries.len > 0) return false;
    if (req.sparse != null or req.sparse_queries.len > 0) return false;
    if (req.graph_queries.len > 0) return false;
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) return false;

    const query_is_dense_or_neutral = switch (req.query) {
        .match_all, .dense_knn => true,
        else => false,
    };
    if (!query_is_dense_or_neutral) return false;

    return req.dense != null or req.dense_queries.len > 0 or switch (req.query) {
        .dense_knn => true,
        else => false,
    };
}

fn queryLocalDetailed(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !LocalQueryExecution {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
        const snapshot_req = try db.searchRequestAtCurrentIdentityGeneration(req);
        if (profiledDenseQuery(snapshot_req)) |dense| {
            const profiled = try db.searchDenseProfiled(alloc, dense.req, dense.query);
            return .{
                .request = snapshot_req,
                .result = profiled.result,
                .dense_profile = mapDenseSearchProfile(profiled.profile),
            };
        }
        return .{
            .request = snapshot_req,
            .result = try db.search(alloc, snapshot_req),
        };
    } else {
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
        var db = try openProvisionedQueryDbForTableWithCache(alloc, path, catalog, table_name, null, null, lsm_root_generation, null, backend_runtime, local_termite_provider, secret_store, remote_content, identity_namespace);
        defer db.close();

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
        const snapshot_req = try db.searchRequestAtCurrentIdentityGeneration(req);
        if (profiledDenseQuery(snapshot_req)) |dense| {
            const profiled = try db.searchDenseProfiled(alloc, dense.req, dense.query);
            return .{
                .request = snapshot_req,
                .result = profiled.result,
                .dense_profile = mapDenseSearchProfile(profiled.profile),
            };
        }
        return .{
            .request = snapshot_req,
            .result = try db.search(alloc, snapshot_req),
        };
    }
}

fn preflightHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !db_mod.RuntimePreflightSummary {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;
        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
        var summary = try db.preflightSearchRequest(alloc, req, max_work);
        annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
    defer db.close();
    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
    var summary = try db.preflightSearchRequest(alloc, req, max_work);
    annotateVectorWorkerPreflight(alloc, &summary, req);
    return summary;
}

fn mergeRuntimePreflightSummary(
    alloc: std.mem.Allocator,
    target: *db_mod.RuntimePreflightSummary,
    extra: db_mod.RuntimePreflightSummary,
) !void {
    defer {
        var owned = extra;
        owned.deinit(alloc);
    }

    try mergeRuntimePreflightSummaryNoFree(alloc, target, extra);
}

fn mergeRuntimePreflightSummaryNoFree(
    alloc: std.mem.Allocator,
    target: *db_mod.RuntimePreflightSummary,
    extra: db_mod.RuntimePreflightSummary,
) !void {
    try mergeRuntimePreflightStrings(alloc, &target.result_refs, extra.result_refs);
    try mergeRuntimePreflightStrings(alloc, &target.graph_query_order, extra.graph_query_order);
    try mergeRuntimePreflightTextEstimates(alloc, &target.text_indexes, extra.text_indexes);
    try mergeRuntimePreflightEmbeddingEstimates(alloc, &target.embedding_indexes, extra.embedding_indexes);
    try mergeRuntimePreflightGraphEstimates(alloc, &target.graph_indexes, extra.graph_indexes);
    try mergeRuntimePreflightTextQueryStats(alloc, &target.text_query_stats, extra.text_query_stats);
    target.doc_id_value_count = @max(target.doc_id_value_count, extra.doc_id_value_count);
    target.filter_id_count = @max(target.filter_id_count, extra.filter_id_count);
    target.exclude_id_count = @max(target.exclude_id_count, extra.exclude_id_count);
    target.numeric_range_clause_count = @max(target.numeric_range_clause_count, extra.numeric_range_clause_count);
    target.term_range_clause_count = @max(target.term_range_clause_count, extra.term_range_clause_count);
    target.ip_range_clause_count = @max(target.ip_range_clause_count, extra.ip_range_clause_count);
    target.bool_field_clause_count = @max(target.bool_field_clause_count, extra.bool_field_clause_count);
    target.geo_filter_clause_count = @max(target.geo_filter_clause_count, extra.geo_filter_clause_count);
    target.positive_id_result_upper_bound = if (target.positive_id_result_upper_bound) |existing|
        if (extra.positive_id_result_upper_bound) |incoming|
            @min(existing, incoming)
        else
            existing
    else
        extra.positive_id_result_upper_bound;
    const target_pre_merge_lower_bound = if (target.structured_filter_doc_count_lower_bound) |value|
        value
    else if (target.structured_filter_count_exact)
        target.structured_filter_doc_count_estimate
    else
        target.structured_filter_doc_count_estimate;
    const extra_pre_merge_lower_bound = if (extra.structured_filter_doc_count_lower_bound) |value|
        value
    else if (extra.structured_filter_count_exact)
        extra.structured_filter_doc_count_estimate
    else
        extra.structured_filter_doc_count_estimate;
    if (target.structured_filter_count_exact and extra.structured_filter_count_exact) {
        if (target.structured_filter_doc_count_estimate) |existing| {
            if (extra.structured_filter_doc_count_estimate) |incoming| {
                target.structured_filter_doc_count_estimate = existing + incoming;
                target.structured_filter_count_exact = true;
            } else {
                target.structured_filter_doc_count_estimate = null;
                target.structured_filter_count_exact = false;
            }
        } else if (extra.structured_filter_doc_count_estimate) |incoming| {
            target.structured_filter_doc_count_estimate = incoming;
            target.structured_filter_count_exact = true;
        } else {
            target.structured_filter_doc_count_estimate = null;
            target.structured_filter_count_exact = false;
        }
    } else {
        target.structured_filter_doc_count_estimate = null;
        target.structured_filter_count_exact = false;
    }
    target.structured_filter_doc_count_sample_estimate = if (target.structured_filter_doc_count_sample_estimate) |existing|
        if (extra.structured_filter_doc_count_sample_estimate) |incoming|
            existing + incoming
        else
            existing
    else
        extra.structured_filter_doc_count_sample_estimate;
    target.structured_filter_count_sample_size += extra.structured_filter_count_sample_size;
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_sample_estimate = null;
        target.structured_filter_count_sample_size = 0;
    }
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_lower_bound = null;
    } else {
        target.structured_filter_doc_count_lower_bound = if (target_pre_merge_lower_bound != null or extra_pre_merge_lower_bound != null)
            (target_pre_merge_lower_bound orelse 0) + (extra_pre_merge_lower_bound orelse 0)
        else
            null;
    }
    target.structured_filter_count_budget_limit = if (target.structured_filter_count_budget_limit) |existing|
        if (extra.structured_filter_count_budget_limit) |incoming|
            @max(existing, incoming)
        else
            existing
    else
        extra.structured_filter_count_budget_limit;
    target.shard_result_window = @max(target.shard_result_window, extra.shard_result_window);
    target.shard_result_window_total += extra.shard_result_window_total;
    target.stored_projection_doc_upper_bound_total += extra.stored_projection_doc_upper_bound_total;
    target.rerank_doc_upper_bound = @max(target.rerank_doc_upper_bound, extra.rerank_doc_upper_bound);
    target.aggregation_may_scan_full_results = target.aggregation_may_scan_full_results or extra.aggregation_may_scan_full_results;
    target.shard_count += extra.shard_count;
    target.remote_shard_count += extra.remote_shard_count;
    target.dense_query_count += extra.dense_query_count;
    target.vector_worker_candidate_count += extra.vector_worker_candidate_count;
    target.vector_worker_fallback_count += extra.vector_worker_fallback_count;
    target.vector_worker_filter_constraint_count += extra.vector_worker_filter_constraint_count;
    target.vector_worker_requires_algebraic_filter_resolution = target.vector_worker_requires_algebraic_filter_resolution or
        extra.vector_worker_requires_algebraic_filter_resolution;
    target.dense_effective_k_total += extra.dense_effective_k_total;
    target.dense_search_width_total += extra.dense_search_width_total;
    target.dense_search_width_max = @max(target.dense_search_width_max, extra.dense_search_width_max);
    target.dense_epsilon_max = @max(target.dense_epsilon_max, extra.dense_epsilon_max);
    db_mod.deriveRuntimePreflightEstimates(target);
}

fn mergeRuntimePreflightTextQueryStats(
    alloc: std.mem.Allocator,
    target: *[]const distributed_stats_mod.TextFieldStats,
    extra: []const distributed_stats_mod.TextFieldStats,
) !void {
    const merged = try mergeDistributedTextStats(alloc, &[_][]const distributed_stats_mod.TextFieldStats{
        target.*,
        extra,
    });
    distributed_stats_mod.deinitTextFieldStats(alloc, target.*);
    target.* = merged;
}

fn mergeRuntimePreflightStrings(
    alloc: std.mem.Allocator,
    target: *[]const []const u8,
    extra: []const []const u8,
) !void {
    var items = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (items.items) |item| alloc.free(item);
        items.deinit(alloc);
    }
    for (target.*) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    for (extra) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    freeRuntimePreflightStringSlice(alloc, target.*);
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn appendUniqueRuntimePreflightString(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try items.append(alloc, try alloc.dupe(u8, value));
}

fn freeRuntimePreflightStringSlice(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(@constCast(item));
    if (items.len > 0) alloc.free(@constCast(items));
}

fn mergeRuntimePreflightTextEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.TextIndexEstimate,
    extra: []const db_mod.TextIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.TextIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .doc_count = item.doc_count,
        .chunk_backed = item.chunk_backed,
        .group_chunk_parents = item.group_chunk_parents,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            existing.group_chunk_parents = existing.group_chunk_parents or item.group_chunk_parents;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .doc_count = item.doc_count,
                .chunk_backed = item.chunk_backed,
                .group_chunk_parents = item.group_chunk_parents,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn mergeRuntimePreflightEmbeddingEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.EmbeddingIndexEstimate,
    extra: []const db_mod.EmbeddingIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.EmbeddingIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .sparse = item.sparse,
        .doc_count = item.doc_count,
        .dims = item.dims,
        .chunk_backed = item.chunk_backed,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name) or existing.sparse != item.sparse) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            if (existing.dims == 0) existing.dims = item.dims;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .sparse = item.sparse,
                .doc_count = item.doc_count,
                .dims = item.dims,
                .chunk_backed = item.chunk_backed,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn mergeRuntimePreflightGraphEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.GraphIndexEstimate,
    extra: []const db_mod.GraphIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.GraphIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .edge_count = item.edge_count,
        .node_count = item.node_count,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.edge_count += item.edge_count;
            existing.node_count += item.node_count;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .edge_count = item.edge_count,
                .node_count = item.node_count,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn preflightProvisionedGroups(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !?db_mod.RuntimePreflightSummary {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    var first_summary: ?db_mod.RuntimePreflightSummary = null;
    errdefer if (first_summary) |*summary| summary.deinit(alloc);
    for (group_ids) |group_id| {
        const summary = try preflightHostedLocal(
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.lsmRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            req,
            consistency,
            max_work,
        );
        if (first_summary == null) {
            first_summary = summary;
        } else {
            try mergeRuntimePreflightSummary(alloc, &first_summary.?, summary);
        }
    }
    return first_summary;
}

fn queryHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const detailed = try queryHostedLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, local_termite_provider, secret_store, remote_content, table_name, req, consistency);
    return detailed.result;
}

fn queryHostedLocalDetailed(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !LocalQueryExecution {
    return queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, local_termite_provider, secret_store, remote_content, table_name, req, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, local_termite_provider, secret_store, remote_content, table_name, req, .stale),
        else => err,
    };
}

fn openProvisionedQueryDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_root_generation: u64,
) !db_mod.DB {
    return try openProvisionedQueryDbForTableWithCache(alloc, path, catalog, table_name, null, null, lsm_root_generation, null, null, null, null, null, null);
}

fn openProvisionedQueryDbForTableWithRuntime(
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
        try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id),
    );
}

fn openProvisionedWarmStatusDbForTable(
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

fn openProvisionedLookupDbForTable(
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

fn openProvisionedQueryDbForTableWithCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
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

    var embedders = try createEmbedders(alloc, indexes_json, local_termite_provider, secret_store, remote_content);
    errdefer embedders.deinit(alloc);

    var db = if (embedders.dense != null or embedders.sparse != null) blk: {
        const dense = embedders.dense;
        const sparse = embedders.sparse;
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
            .enrichment = .{
                .dense_embedder = dense,
                .sparse_embedder = sparse,
            },
        });
        embedders.dense = null;
        embedders.sparse = null;
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

    const summary = try metadata_table_provisioner.reconcileDbIndexes(alloc, &db, indexes_json);
    if (summary.indexes_added > 0 or summary.indexes_removed > 0) {
        // Query/status paths can be the first readers to observe a newly-added
        // index from metadata. Reopen after reconcile so searches run against
        // the stabilized post-reconcile index-manager state.
        db.close();
        embedders = try createEmbedders(alloc, indexes_json, local_termite_provider, secret_store, remote_content);
        db = if (embedders.dense != null or embedders.sparse != null) blk: {
            const dense = embedders.dense;
            const sparse = embedders.sparse;
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
                .enrichment = .{
                    .dense_embedder = dense,
                    .sparse_embedder = sparse,
                },
            });
            embedders.dense = null;
            embedders.sparse = null;
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
        try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
    }
    if (summary.indexes_added > 0) {
        if (db.enrichment_runtime != null) {
            _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
            try db.runUntilIdle();
        }
    }
    return db;
}

fn loadTableIndexesJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = @import("tables.zig").findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.indexes_json);
}

fn loadTableIdentityNamespaceForGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
) !?db_mod.DocIdentityNamespace {
    _ = alloc;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = @import("tables.zig").findTableByName(&snapshot, table_name) orelse return null;
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

fn validateProvisionedDbIdentityNamespace(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    db: *const db_mod.DB,
) !void {
    const expected = (try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id)) orelse return;
    try validateOpenedProvisionedDbIdentityNamespace(db, expected);
}

fn validateOpenedProvisionedDbIdentityNamespace(
    db: *const db_mod.DB,
    expected: ?db_mod.DocIdentityNamespace,
) !void {
    const namespace = expected orelse return;
    if (!db.core.identity_namespace.eql(namespace)) return error.DocIdentityNamespaceMismatch;
}

fn aggregationContextForDb(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    db: *db_mod.DB,
) !db_mod.aggregations.Context {
    const identity_read_generation = try currentIdentityReadGenerationForDb(req.identity_read_generation, db);
    return .{
        .index_manager = db.core.index_manager,
        .doc_store = db.core.store,
        .full_text_index_name = req.index_name,
        .algebraic_index_name = req.index_name,
        .algebraic_available = try algebraicIndexFreshEnoughForRequest(alloc, req, db),
        .identity_read_generation = identity_read_generation,
    };
}

fn currentIdentityReadGenerationForDb(requested: ?u64, db: *db_mod.DB) !u64 {
    return try db.currentIdentityReadGenerationForRequest(requested);
}

fn algebraicIndexFreshEnoughForRequest(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    db: *db_mod.DB,
) !bool {
    return try algebraicIndexFreshEnoughForName(alloc, req.index_name, db);
}

fn algebraicIndexFreshEnoughForName(
    alloc: std.mem.Allocator,
    index_name_opt: ?[]const u8,
    db: *db_mod.DB,
) !bool {
    const entry = if (index_name_opt) |index_name|
        db.core.index_manager.algebraicIndex(index_name) orelse return false
    else
        db.core.index_manager.algebraicIndex(null) orelse return false;
    if (entry.index.hasErrors()) return false;
    const target_sequence = db.core.nextDerivedSequence();
    var applied_sequence = try db.core.loadAppliedSequence(alloc, entry.config.name);
    if (db.executor.appliedSequence(entry.config.name)) |live_applied| {
        applied_sequence = @max(applied_sequence, live_applied);
    }
    return applied_sequence >= target_sequence;
}

fn canConsiderAlgebraicAggregations(req: db_mod.types.SearchRequest) bool {
    return req.full_text == null and
        req.exclusion_query_json.len == 0 and
        req.full_text_queries.len == 0 and
        req.dense == null and
        req.sparse == null and
        req.dense_queries.len == 0 and
        req.sparse_queries.len == 0 and
        req.graph_queries.len == 0 and
        req.merge_config == null and
        req.reranker == null and
        req.pruner == null and
        req.filter_prefix.len == 0 and
        req.filter_ids.len == 0 and
        req.exclude_ids.len == 0 and
        req.filter_doc_ids.len == 0 and
        !req.filter_doc_ids_positive and
        req.exclude_doc_ids.len == 0 and
        !searchRequestHasResolvedDocFilter(req) and
        req.distance_over == null and
        req.distance_under == null;
}

fn algebraicConstraintsForRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !?[]db_mod.aggregations.FixedConstraint {
    var out = std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint).empty;
    errdefer freeAlgebraicConstraints(alloc, out.items);

    switch (req.query) {
        .match_all => {},
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return null;
            const value_text = db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term }) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, term.field, value_text) catch return null;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return null;
            const value_text = db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, match.field, value_text) catch return null;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return null;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return null;
            const value_text = db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, fuzzy.field, value_text) catch return null;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, prefix.field, value_text) catch return null;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return null;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return null;
            const value_text = db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, wildcard.field, value_text) catch return null;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return null;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return null;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return null;
            defer compiled.deinit();
            const value_text = db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, regexp.field, value_text) catch return null;
        },
        .bool_field => |field| {
            const value_text = algebraicConstraintBoolValueAlloc(alloc, field.field, field.value) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, field.field, value_text) catch return null;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return null;
            const value_text = db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return null;
            const value_text = db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, bbox.field, value_text) catch return null;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, distance.field, value_text) catch return null;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, shape.field, value_text) catch return null;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const start_text = if (range.start_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        else => return null,
    }

    if (req.full_text) |text_query| {
        if (!(collectAlgebraicTextQueryConstraints(alloc, text_query, &out) catch return null)) return null;
    }

    if (req.filter_query_json.len > 0) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.filter_query_json, .{}) catch return null;
        defer parsed.deinit();
        if (!(collectAlgebraicFilterConstraints(alloc, parsed.value, &out) catch return null)) return null;
    }

    return try out.toOwnedSlice(alloc);
}

fn freeAlgebraicConstraints(
    alloc: std.mem.Allocator,
    constraints: []db_mod.aggregations.FixedConstraint,
) void {
    for (constraints) |constraint| {
        alloc.free(@constCast(constraint.field));
        alloc.free(@constCast(constraint.value));
    }
    if (constraints.len > 0) alloc.free(constraints);
}

const AlgebraicConstraintCollectError = std.mem.Allocator.Error || error{UnsupportedQueryRequest};

fn collectAlgebraicTextBoolQueryConstraints(
    alloc: std.mem.Allocator,
    bool_query: db_mod.types.TextBoolQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (bool_query.must_not.len > 0) return false;
    if (bool_query.should.len > 0) {
        if (bool_query.must.len > 0) {
            if (bool_query.min_should != 0) return false;
        } else {
            if (bool_query.min_should != 0 and bool_query.min_should != 1) return false;
            return try collectAlgebraicTextShouldTermConstraint(alloc, bool_query.should, out);
        }
    }
    if (bool_query.min_should > 0) return false;
    for (bool_query.must) |query| {
        if (!(try collectAlgebraicTextQueryConstraints(alloc, query, out))) return false;
    }
    return true;
}

fn collectAlgebraicTextShouldTermConstraint(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (queries.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, queries.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (queries, 0..) |query, i| {
        const term = switch (query) {
            .term => |term| term,
            else => return false,
        };
        if (!std.mem.startsWith(u8, term.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, term.field)) return false;
        } else {
            field = term.field;
        }
        typed_values[i] = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

fn collectAlgebraicTextQueryConstraints(
    alloc: std.mem.Allocator,
    query: db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    switch (query) {
        .match_all => return true,
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return false;
            const value_text = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, term.field, value_text);
            return true;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, match.field, value_text);
            return true;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return false;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, fuzzy.field, value_text);
            return true;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, prefix.field, value_text);
            return true;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return false;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, wildcard.field, value_text);
            return true;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return false;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return false;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return false;
            defer compiled.deinit();
            const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, regexp.field, value_text);
            return true;
        },
        .bool_field => |field| {
            const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.field, field.value);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, field.field, value_text);
            return true;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const start_text = if (range.start_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return false;
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return false;
            const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, bbox.field, value_text);
            return true;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, distance.field, value_text);
            return true;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, shape.field, value_text);
            return true;
        },
        .bool_query => |nested| return try collectAlgebraicTextBoolQueryConstraints(alloc, nested, out),
        else => return false,
    }
}

fn appendAlgebraicConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    value: []const u8,
) AlgebraicConstraintCollectError!void {
    for (out.items) |existing| {
        if (!std.mem.eql(u8, existing.field, field)) continue;
        if (std.mem.eql(u8, existing.value, value)) return;
        return error.UnsupportedQueryRequest;
    }
    try out.append(alloc, .{
        .field = try alloc.dupe(u8, field),
        .value = try alloc.dupe(u8, value),
    });
}

fn collectAlgebraicFilterConstraints(
    alloc: std.mem.Allocator,
    filter: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (filter != .object) return false;
    if (filter.object.get("match_all") != null) return true;
    if (filter.object.get("term")) |term| {
        const predicate = algebraicFilterTermPredicate(term, filter.object.get("field") orelse filter.object.get("path")) orelse return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.field, value_text);
        return true;
    }
    if (filter.object.get("terms")) |terms| {
        if (!(try collectSingleValueTermsConstraint(alloc, terms, out))) return false;
        return true;
    }
    if (filter.object.get("match")) |match| {
        const predicate = algebraicPathMatchPredicate(match, filter.object.get("field")) orelse return false;
        if (predicate.text.len == 0) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("bool_field")) |bool_field| {
        if (bool_field != .object) return false;
        const field = bool_field.object.get("field") orelse return false;
        const value = bool_field.object.get("value") orelse return false;
        if (field != .string or value != .bool) return false;
        const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.string, value.bool);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field.string, value_text);
        return true;
    }
    if (filter.object.get("exists")) |exists| {
        const path = algebraicExistsPath(exists) orelse return false;
        if (!std.mem.startsWith(u8, path, "/")) return false;
        try appendAlgebraicConstraint(out, alloc, path, db_mod.algebraic.index.path_fact_exists_constraint_value);
        return true;
    }
    if (filter.object.get("prefix")) |prefix| {
        const predicate = algebraicPathPrefixPredicate(prefix, filter.object.get("field")) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, predicate.prefix);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("wildcard")) |wildcard| {
        const predicate = algebraicPathPatternPredicate(wildcard, "pattern") orelse return false;
        const literal_prefix = algebraicWildcardLiteralPrefix(predicate.text);
        if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(predicate.text)) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("regexp")) |regexp| {
        const predicate = algebraicPathPatternPredicate(regexp, "pattern") orelse return false;
        if (algebraicRegexpLiteralPrefix(predicate.text).len == 0) return false;
        var compiled = regex_mod.compile(alloc, predicate.text) catch return false;
        defer compiled.deinit();
        const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("fuzzy")) |fuzzy| {
        const predicate = algebraicPathFuzzyPredicate(fuzzy) orelse return false;
        if (predicate.query.prefix_len == 0) return false;
        const prefix_len: usize = @intCast(predicate.query.prefix_len);
        if (predicate.query.term.len < prefix_len) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
            alloc,
            predicate.query.term,
            predicate.query.max_edits,
            predicate.query.prefix_len,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("numeric_range")) |range| {
        const predicate = algebraicPathNumericRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("date_range")) |range| {
        const predicate = algebraicPathDateRangePredicate(range) orelse return false;
        const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
        defer if (start_text) |value| alloc.free(value);
        const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
        defer if (end_text) |value| alloc.free(value);
        const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
            alloc,
            start_text,
            end_text,
            predicate.inclusive_start,
            predicate.inclusive_end,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("ip_range")) |range| {
        const predicate = algebraicPathIpRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, predicate.cidr);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_bbox")) |bbox| {
        const predicate = algebraicPathGeoBBoxPredicate(bbox) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
            alloc,
            predicate.min_lat,
            predicate.min_lon,
            predicate.max_lat,
            predicate.max_lon,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_distance")) |distance| {
        const predicate = algebraicPathGeoDistancePredicate(distance) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
            alloc,
            predicate.lat,
            predicate.lon,
            predicate.radius_meters,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_shape")) |shape| {
        var predicate = (try algebraicPathGeoShapePredicateAlloc(alloc, shape)) orelse return false;
        defer predicate.deinit(alloc);
        const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
            alloc,
            @tagName(predicate.relation),
            predicate.polygons,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("term_range")) |range| {
        const predicate = algebraicPathTermRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("range")) |range| {
        if (algebraicPathStandardNumericRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardDateRangePredicate(range)) |predicate| {
            const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
            defer if (start_text) |value| alloc.free(value);
            const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                predicate.inclusive_start,
                predicate.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardTermRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        return false;
    }
    if (filter.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return false;
        for (conjuncts.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    if (filter.object.get("disjuncts")) |disjuncts| {
        return try collectAlgebraicFilterShouldTermConstraint(alloc, disjuncts, out);
    }
    if (filter.object.get("bool")) |bool_query| {
        if (bool_query != .object) return false;
        if (bool_query.object.get("must_not") != null) return false;
        const must = bool_query.object.get("must");
        const filter_clause = bool_query.object.get("filter");
        const should = bool_query.object.get("should");
        if (should) |clause| {
            const min_should_value = bool_query.object.get("minimum_should_match") orelse bool_query.object.get("min_should");
            if (must != null or filter_clause != null) {
                if (!algebraicBoolShouldMinIsOptional(min_should_value)) return false;
            } else {
                if (!algebraicBoolShouldMinIsOne(min_should_value)) return false;
                return try collectAlgebraicFilterShouldTermConstraint(alloc, clause, out);
            }
        }
        if (must == null and filter_clause == null) return false;
        if (must) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        if (filter_clause) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        return true;
    }
    return false;
}

fn collectAlgebraicFilterConstraintClause(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (clause == .array) {
        if (clause.array.items.len == 0) return false;
        for (clause.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    return try collectAlgebraicFilterConstraints(alloc, clause, out);
}

fn algebraicBoolShouldMinIsOptional(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 0,
        .float => |number| number == 0.0,
        .string => |text| std.mem.eql(u8, text, "0"),
        else => false,
    };
}

fn algebraicBoolShouldMinIsOne(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 1,
        .float => |number| number == 1.0,
        .string => |text| std.mem.eql(u8, text, "1"),
        else => false,
    };
}

fn collectAlgebraicFilterShouldTermConstraint(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    const items = switch (clause) {
        .array => |array| array.items,
        else => return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, &.{clause}, out),
    };
    return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, items, out);
}

fn collectAlgebraicFilterShouldTermItemsConstraint(
    alloc: std.mem.Allocator,
    items: []const std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (items.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return false,
        };
        const predicate = algebraicFilterTermPredicate(object.get("term") orelse return false, object.get("field") orelse object.get("path")) orelse return false;
        if (!std.mem.startsWith(u8, predicate.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, predicate.field)) return false;
        } else {
            field = predicate.field;
        }
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

const AlgebraicFilterTermPredicate = struct {
    field: []const u8,
    value: std.json.Value,
};

fn algebraicFilterTermPredicate(term: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicFilterTermPredicate {
    if (term == .object) {
        if (term.object.get("field") orelse term.object.get("path")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            const value = term.object.get("term") orelse term.object.get("value") orelse return null;
            return .{ .field = field, .value = value };
        }
        if (term.object.count() == 1) {
            var it = term.object.iterator();
            const entry = it.next() orelse return null;
            return .{ .field = entry.key_ptr.*, .value = entry.value_ptr.* };
        }
    }
    const field = algebraicJsonString(sibling_field_value orelse return null) orelse return null;
    return .{ .field = field, .value = term };
}

fn algebraicExistsPath(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |field| field,
        .object => |object| blk: {
            const path = object.get("path") orelse object.get("field") orelse break :blk null;
            if (path != .string) break :blk null;
            break :blk path.string;
        },
        else => null,
    };
}

const AlgebraicPathPrefixPredicate = struct {
    path: []const u8,
    prefix: []const u8,
};

const AlgebraicPathTextPredicate = struct {
    path: []const u8,
    text: []const u8,
};

const AlgebraicFuzzyQuery = struct {
    term: []const u8,
    max_edits: u8,
    prefix_len: u8,
};

const AlgebraicPathFuzzyPredicate = struct {
    path: []const u8,
    query: AlgebraicFuzzyQuery,
};

const AlgebraicPathNumericRangePredicate = struct {
    path: []const u8,
    min: ?f64 = null,
    max: ?f64 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

const AlgebraicPathTermRangePredicate = struct {
    path: []const u8,
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

const AlgebraicPathDateRangePredicate = struct {
    path: []const u8,
    start: ?std.json.Value = null,
    end: ?std.json.Value = null,
    inclusive_start: bool = true,
    inclusive_end: bool = false,
};

const AlgebraicPathIpRangePredicate = struct {
    path: []const u8,
    cidr: []const u8,
};

const AlgebraicPathGeoBBoxPredicate = struct {
    path: []const u8,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
};

const AlgebraicPathGeoDistancePredicate = struct {
    path: []const u8,
    lat: f64,
    lon: f64,
    radius_meters: f64,
};

const AlgebraicPathGeoShapePredicate = struct {
    path: []const u8,
    relation: db_mod.types.GeoShapeRelation,
    polygons: []const []const db_mod.types.GeoPoint,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (self.polygons.len > 0) alloc.free(@constCast(self.polygons));
        self.* = undefined;
    }
};

fn algebraicJsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn algebraicPathPrefixPredicate(prefix_value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathPrefixPredicate {
    return switch (prefix_value) {
        .object => |object| blk: {
            if (object.get("path")) |path_value| {
                const path = algebraicJsonString(path_value) orelse break :blk null;
                if (!std.mem.startsWith(u8, path, "/")) break :blk null;
                const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                break :blk .{ .path = path, .prefix = prefix };
            }
            if (object.get("role") == null) {
                if (object.get("field")) |field_value| {
                    const field = algebraicJsonString(field_value) orelse break :blk null;
                    if (std.mem.startsWith(u8, field, "/")) {
                        const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                        break :blk .{ .path = field, .prefix = prefix };
                    }
                }
            }
            if (object.count() == 1) {
                var it = object.iterator();
                const entry = it.next() orelse break :blk null;
                if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) break :blk null;
                const prefix = algebraicJsonString(entry.value_ptr.*) orelse break :blk null;
                break :blk .{ .path = entry.key_ptr.*, .prefix = prefix };
            }
            break :blk null;
        },
        .string => |prefix| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .prefix = prefix } else null;
        },
        else => null,
    };
}

fn algebraicPathFromPredicateObject(object: anytype) ?[]const u8 {
    if (object.get("path")) |path_value| {
        const path = algebraicJsonString(path_value) orelse return null;
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return path;
    }
    return null;
}

fn algebraicPathPatternPredicate(value: std.json.Value, pattern_field: []const u8) ?AlgebraicPathTextPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
        return .{ .path = path, .text = text };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
                return .{ .path = field, .text = text };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const text = algebraicJsonString(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .text = text };
    }
    return null;
}

fn algebraicPathMatchPredicate(value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathTextPredicate {
    return switch (value) {
        .object => algebraicPathPatternPredicate(value, "query"),
        .string => |text| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .text = text } else null;
        },
        else => null,
    };
}

fn algebraicWildcardLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        if (ch == '*' or ch == '?') return pattern[0..i];
    }
    return pattern;
}

fn algebraicWildcardPatternHasMeta(pattern: []const u8) bool {
    return std.mem.indexOfAny(u8, pattern, "*?") != null;
}

fn algebraicRegexpLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        switch (ch) {
            '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\' => return pattern[0..i],
            else => {},
        }
    }
    return pattern;
}

fn algebraicJsonU8(value: std.json.Value) ?u8 {
    return switch (value) {
        .integer => |number| std.math.cast(u8, number),
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) break :blk null;
            const parsed: i64 = @intFromFloat(number);
            break :blk std.math.cast(u8, parsed);
        },
        else => null,
    };
}

fn algebraicParseFuzzyOptions(object: anytype, out: *AlgebraicFuzzyQuery) bool {
    if (object.get("max_edits")) |edits| {
        out.max_edits = algebraicJsonU8(edits) orelse return false;
    }
    if (object.get("prefix_length")) |prefix| {
        out.prefix_len = algebraicJsonU8(prefix) orelse return false;
    }
    if (object.get("auto_fuzzy")) |auto| {
        if (auto != .bool) return false;
        if (auto.bool) out.max_edits = if (out.term.len > 5) 2 else if (out.term.len > 2) 1 else 0;
    }
    return true;
}

fn algebraicParseFuzzyQuery(value: std.json.Value) ?AlgebraicFuzzyQuery {
    return switch (value) {
        .string => |text| .{ .term = text, .max_edits = 1, .prefix_len = 0 },
        .object => |object| blk: {
            var out = AlgebraicFuzzyQuery{
                .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse break :blk null) orelse break :blk null,
                .max_edits = 1,
                .prefix_len = 0,
            };
            if (!algebraicParseFuzzyOptions(object, &out)) break :blk null;
            break :blk out;
        },
        else => null,
    };
}

fn algebraicPathFuzzyPredicate(value: std.json.Value) ?AlgebraicPathFuzzyPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        var query = AlgebraicFuzzyQuery{
            .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
            .max_edits = 1,
            .prefix_len = 0,
        };
        if (!algebraicParseFuzzyOptions(object, &query)) return null;
        return .{ .path = path, .query = query };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                var query = AlgebraicFuzzyQuery{
                    .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
                    .max_edits = 1,
                    .prefix_len = 0,
                };
                if (!algebraicParseFuzzyOptions(object, &query)) return null;
                return .{ .path = field, .query = query };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const query = algebraicParseFuzzyQuery(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .query = query };
    }
    return null;
}

fn algebraicOptionalBool(value: ?std.json.Value) ?bool {
    const actual = value orelse return null;
    return switch (actual) {
        .bool => |flag| flag,
        .null => null,
        else => null,
    };
}

fn algebraicOptionalF64(value: ?std.json.Value) ?f64 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .null => null,
        else => null,
    };
}

fn algebraicOptionalString(value: ?std.json.Value) ?[]const u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .string => |text| text,
        .null => null,
        else => null,
    };
}

fn algebraicNumericJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float => true,
        else => false,
    };
}

fn algebraicStringJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .string => true,
        else => false,
    };
}

fn algebraicDateJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer => |number| number >= 0,
        .string => |text| (algebraicParseDateTimeOptionalToNs(text) catch null) != null,
        else => false,
    };
}

fn algebraicDateBoundTextAlloc(alloc: std.mem.Allocator, value: ?std.json.Value) !?[]u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| if (number >= 0) try std.fmt.allocPrint(alloc, "{d}", .{number}) else error.UnsupportedQueryRequest,
        .string => |text| if ((try algebraicParseDateTimeOptionalToNs(text)) != null) try alloc.dupe(u8, text) else error.UnsupportedQueryRequest,
        .null => null,
        else => error.UnsupportedQueryRequest,
    };
}

fn algebraicValidIpRange(text: []const u8) bool {
    return algebraicParseIpCidr(text) != null or algebraicParseIPv4(text) != null;
}

fn algebraicValidLatitude(lat: f64) bool {
    return std.math.isFinite(lat) and lat >= -90.0 and lat <= 90.0;
}

fn algebraicValidLongitude(lon: f64) bool {
    return std.math.isFinite(lon) and lon >= -180.0 and lon <= 180.0;
}

fn algebraicValidGeoBBox(min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) bool {
    return algebraicValidLatitude(min_lat) and
        algebraicValidLatitude(max_lat) and
        algebraicValidLongitude(min_lon) and
        algebraicValidLongitude(max_lon) and
        min_lat <= max_lat;
}

fn algebraicValidGeoDistance(lat: f64, lon: f64, radius_meters: f64) bool {
    return algebraicValidLatitude(lat) and
        algebraicValidLongitude(lon) and
        std.math.isFinite(radius_meters) and
        radius_meters >= 0;
}

fn algebraicGeoShapeRelationSupported(relation: db_mod.types.GeoShapeRelation) bool {
    return switch (relation) {
        .intersects, .within => true,
        .contains => false,
    };
}

const AlgebraicIpCidr = struct {
    network: [4]u8,
    prefix_len: u8,
};

fn algebraicParseIpCidr(text: []const u8) ?AlgebraicIpCidr {
    const slash_pos = std.mem.indexOfScalar(u8, text, '/') orelse return null;
    const ip = algebraicParseIPv4(text[0..slash_pos]) orelse return null;
    const prefix_len = std.fmt.parseInt(u8, text[slash_pos + 1 ..], 10) catch return null;
    if (prefix_len > 32) return null;
    const mask = algebraicIpMask(prefix_len);
    return .{
        .network = .{ ip[0] & mask[0], ip[1] & mask[1], ip[2] & mask[2], ip[3] & mask[3] },
        .prefix_len = prefix_len,
    };
}

fn algebraicParseIPv4(text: []const u8) ?[4]u8 {
    var parts = std.mem.splitScalar(u8, text, '.');
    var out: [4]u8 = undefined;
    var i: usize = 0;
    while (parts.next()) |part| {
        if (i >= 4 or part.len == 0) return null;
        out[i] = std.fmt.parseInt(u8, part, 10) catch return null;
        i += 1;
    }
    if (i != 4) return null;
    return out;
}

fn algebraicIpMask(prefix_len: u8) [4]u8 {
    var mask = [_]u8{ 0, 0, 0, 0 };
    var remaining = prefix_len;
    for (&mask) |*byte| {
        if (remaining >= 8) {
            byte.* = 0xff;
            remaining -= 8;
        } else if (remaining > 0) {
            byte.* = @as(u8, 0xff) << @intCast(8 - remaining);
            remaining = 0;
        }
    }
    return mask;
}

fn algebraicParseDateTimeOptionalToNs(text: []const u8) !?u64 {
    if (try algebraicParseRfc3339ToNs(text)) |ts| return ts;
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    return algebraicCivilDateTimeToNs(year, month, day, 0, 0, 0, 0);
}

fn algebraicParseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;
    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;
    return algebraicCivilDateTimeToNs(year, month, day, hour, minute, second, nanos);
}

fn algebraicCivilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour < 0 or hour > 23 or minute < 0 or minute > 59 or second < 0 or second > 60) return null;
    const days = algebraicDaysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn algebraicDaysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) 1 else 0;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn algebraicPathIpRangePredicate(value: std.json.Value) ?AlgebraicPathIpRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const cidr = algebraicJsonString(object.get("cidr") orelse return null) orelse return null;
    if (!algebraicValidIpRange(cidr)) return null;
    if (algebraicPathFromPredicateObject(object)) |path| {
        return .{ .path = path, .cidr = cidr };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) return .{ .path = field, .cidr = cidr };
        }
    }
    return null;
}

fn algebraicPathGeoBBoxPredicate(value: std.json.Value) ?AlgebraicPathGeoBBoxPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min_lat = algebraicOptionalF64(object.get("min_lat")) orelse return null;
    const min_lon = algebraicOptionalF64(object.get("min_lon")) orelse return null;
    const max_lat = algebraicOptionalF64(object.get("max_lat")) orelse return null;
    const max_lon = algebraicOptionalF64(object.get("max_lon")) orelse return null;
    if (!algebraicValidGeoBBox(min_lat, min_lon, max_lat, max_lon)) return null;
    return .{
        .path = path,
        .min_lat = min_lat,
        .min_lon = min_lon,
        .max_lat = max_lat,
        .max_lon = max_lon,
    };
}

fn algebraicPathGeoDistancePredicate(value: std.json.Value) ?AlgebraicPathGeoDistancePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const lat = algebraicOptionalF64(object.get("lat")) orelse return null;
    const lon = algebraicOptionalF64(object.get("lon")) orelse return null;
    const radius_meters = algebraicOptionalF64(object.get("radius_meters")) orelse return null;
    if (!algebraicValidGeoDistance(lat, lon, radius_meters)) return null;
    return .{
        .path = path,
        .lat = lat,
        .lon = lon,
        .radius_meters = radius_meters,
    };
}

fn algebraicPathGeoShapePredicateAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?AlgebraicPathGeoShapePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const relation_text = if (object.get("relation")) |relation_value|
        algebraicJsonString(relation_value) orelse return null
    else
        "intersects";
    const relation = std.meta.stringToEnum(db_mod.types.GeoShapeRelation, relation_text) orelse return null;
    if (!algebraicGeoShapeRelationSupported(relation)) return null;
    const polygons_value = object.get("polygons") orelse object.get("polygon") orelse return null;
    const polygons = try algebraicGeoShapePolygonsAlloc(alloc, polygons_value);
    errdefer {
        for (polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (polygons.len > 0) alloc.free(polygons);
    }
    return .{
        .path = path,
        .relation = relation,
        .polygons = polygons,
    };
}

fn algebraicGeoShapePolygonsAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const []const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len == 0) return error.UnsupportedQueryRequest;
    const first_is_point = array.items[0] == .object;
    const polygon_count: usize = if (first_is_point) 1 else array.items.len;
    var out = try alloc.alloc([]const db_mod.types.GeoPoint, polygon_count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (out.len > 0) alloc.free(out);
    }
    if (first_is_point) {
        out[0] = try algebraicGeoShapePolygonAlloc(alloc, value);
        initialized = 1;
    } else {
        for (array.items, 0..) |item, i| {
            out[i] = try algebraicGeoShapePolygonAlloc(alloc, item);
            initialized += 1;
        }
    }
    return out;
}

fn algebraicGeoShapePolygonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len < 3) return error.UnsupportedQueryRequest;
    var out = try alloc.alloc(db_mod.types.GeoPoint, array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (array.items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.UnsupportedQueryRequest,
        };
        const lat = algebraicOptionalF64(object.get("lat")) orelse return error.UnsupportedQueryRequest;
        const lon = algebraicOptionalF64(object.get("lon")) orelse return error.UnsupportedQueryRequest;
        if (!algebraicValidLatitude(lat) or !algebraicValidLongitude(lon)) return error.UnsupportedQueryRequest;
        out[i] = .{ .lat = lat, .lon = lon };
    }
    return out;
}

fn algebraicPathNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min = algebraicOptionalF64(object.get("min"));
    const max = algebraicOptionalF64(object.get("max"));
    if (min == null and max == null) return null;
    return .{
        .path = path,
        .min = min,
        .max = max,
        .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
        .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
    };
}

fn algebraicPathTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const min = algebraicOptionalString(object.get("min"));
        const max = algebraicOptionalString(object.get("max"));
        if (min == null and max == null) return null;
        return .{
            .path = path,
            .min = min,
            .max = max,
            .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
            .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    return algebraicTermRangeFromBounds(entry.key_ptr.*, switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    });
}

fn algebraicPathDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const start = object.get("start_ns") orelse object.get("start");
        const end = object.get("end_ns") orelse object.get("end");
        if (start == null and end == null) return null;
        if (start) |bound| if (!algebraicDateJsonValue(bound)) return null;
        if (end) |bound| if (!algebraicDateJsonValue(bound)) return null;
        return .{
            .path = path,
            .start = start,
            .end = end,
            .inclusive_start = algebraicOptionalBool(object.get("inclusive_start")) orelse true,
            .inclusive_end = algebraicOptionalBool(object.get("inclusive_end")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

const AlgebraicRangeBound = struct {
    value: std.json.Value,
    inclusive: bool,
};

fn algebraicSetRangeBound(found: *?AlgebraicRangeBound, value: std.json.Value, inclusive: bool) ?void {
    if (found.* != null) return null;
    found.* = .{ .value = value, .inclusive = inclusive };
}

fn algebraicStandardRangeLowerBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("gt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("gte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

fn algebraicStandardRangeUpperBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("lt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("lte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

fn algebraicTermRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathTermRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .min = if (lower) |bound| algebraicOptionalString(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalString(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicDateRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathDateRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .start = if (lower) |bound| bound.value else null,
        .end = if (upper) |bound| bound.value else null,
        .inclusive_start = if (lower) |bound| bound.inclusive else true,
        .inclusive_end = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicPathStandardNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const lower = algebraicStandardRangeLowerBound(object);
        const upper = algebraicStandardRangeUpperBound(object);
        if (lower == null and upper == null) return null;
        if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        return .{
            .path = path,
            .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
            .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
            .inclusive_min = if (lower) |bound| bound.inclusive else true,
            .inclusive_max = if (upper) |bound| bound.inclusive else false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    const lower = algebraicStandardRangeLowerBound(range_object);
    const upper = algebraicStandardRangeUpperBound(range_object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    return .{
        .path = entry.key_ptr.*,
        .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicPathStandardDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicDateRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

fn algebraicPathStandardTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicTermRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicTermRangeFromBounds(entry.key_ptr.*, range_object);
}

fn collectSingleValueTermsConstraint(
    alloc: std.mem.Allocator,
    terms: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (terms != .object) return false;
    if (terms.object.count() == 1) {
        var it = terms.object.iterator();
        const entry = it.next() orelse return false;
        return try collectTermsValuesConstraint(alloc, entry.key_ptr.*, entry.value_ptr.*, out);
    }
    const field = terms.object.get("path") orelse terms.object.get("field") orelse return false;
    const values = terms.object.get("values") orelse terms.object.get("terms") orelse return false;
    if (field != .string) return false;
    return try collectTermsValuesConstraint(alloc, field.string, values, out);
}

fn collectTermsValuesConstraint(
    alloc: std.mem.Allocator,
    field: []const u8,
    values: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (values != .array or values.array.items.len == 0) return false;
    if (!std.mem.startsWith(u8, field, "/")) {
        if (values.array.items.len != 1) return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    if (values.array.items.len == 1) {
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    const typed_values = try alloc.alloc([]const u8, values.array.items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }
    for (values.array.items, 0..) |item, i| {
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, field, item);
        initialized += 1;
    }
    try appendAlgebraicTypedAnyConstraint(out, alloc, field, typed_values);
    return true;
}

fn appendAlgebraicTypedAnyConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    typed_values: []const []const u8,
) AlgebraicConstraintCollectError!void {
    if (typed_values.len == 0) return error.UnsupportedQueryRequest;
    if (typed_values.len == 1) {
        try appendAlgebraicConstraint(out, alloc, field, typed_values[0]);
        return;
    }
    const any_value = db_mod.algebraic.index.pathFactAnyConstraintValueAlloc(alloc, typed_values) catch return error.UnsupportedQueryRequest;
    defer alloc.free(any_value);
    try appendAlgebraicConstraint(out, alloc, field, any_value);
}

fn algebraicConstraintValueTextAlloc(alloc: std.mem.Allocator, field: []const u8, value: std.json.Value) AlgebraicConstraintCollectError![]u8 {
    const raw = switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .bool => |flag| try alloc.dupe(u8, if (flag) "true" else "false"),
        .null => if (std.mem.startsWith(u8, field, "/")) try alloc.dupe(u8, "") else return error.UnsupportedQueryRequest,
        else => return error.UnsupportedQueryRequest,
    };
    errdefer alloc.free(raw);
    if (!std.mem.startsWith(u8, field, "/")) return raw;
    const kind = switch (value) {
        .string => "string",
        .integer, .float => "number",
        .bool => "bool",
        .null => "null",
        else => unreachable,
    };
    const typed = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ kind, raw });
    alloc.free(raw);
    return typed;
}

fn algebraicConstraintBoolValueAlloc(alloc: std.mem.Allocator, field: []const u8, value: bool) AlgebraicConstraintCollectError![]u8 {
    const raw = if (value) "true" else "false";
    if (!std.mem.startsWith(u8, field, "/")) return try alloc.dupe(u8, raw);
    return try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "bool", raw });
}

test "algebraic constraints accept scalar bool query and structured filters" {
    const alloc = std.testing.allocator;

    const from_bool = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .bool_field = .{ .field = "published", .value = true } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_bool);
    try std.testing.expectEqual(@as(usize, 1), from_bool.len);
    try std.testing.expectEqualStrings("published", from_bool[0].field);
    try std.testing.expectEqualStrings("true", from_bool[0].value);

    const bool_must_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .prefix = .{ .field = "/tenant", .prefix = "ac" } },
    };
    const from_path_bool_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .must = bool_must_queries[0..] } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_bool_query);
    try std.testing.expectEqual(@as(usize, 2), from_path_bool_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_bool_query[0].field);
    const top_level_bool_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_bool_query[0].value);
    defer {
        for (top_level_bool_term_parts) |part| alloc.free(part);
        alloc.free(top_level_bool_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), top_level_bool_term_parts.len);
    try std.testing.expectEqualStrings("string", top_level_bool_term_parts[0]);
    try std.testing.expectEqualStrings("gold", top_level_bool_term_parts[1]);
    try std.testing.expectEqualStrings("/tenant", from_path_bool_query[1].field);
    const top_level_bool_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_bool_query[1].value);
    defer {
        for (top_level_bool_prefix_parts) |part| alloc.free(part);
        alloc.free(top_level_bool_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_bool_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", top_level_bool_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_bool_prefix_parts[1]);
    try std.testing.expectEqualStrings("ac", top_level_bool_prefix_parts[2]);

    const bool_optional_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/region", .term = "west" } },
    };
    const from_path_bool_optional_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{
            .must = bool_must_queries[0..],
            .should = bool_optional_should_queries[0..],
            .min_should = 0,
        } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_bool_optional_should_query);
    try std.testing.expectEqual(@as(usize, 2), from_path_bool_optional_should_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_bool_optional_should_query[0].field);
    try std.testing.expectEqualStrings("/tenant", from_path_bool_optional_should_query[1].field);

    const required_optional_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{
            .must = bool_must_queries[0..],
            .should = bool_optional_should_queries[0..],
            .min_should = 1,
        } },
    });
    try std.testing.expect(required_optional_should_query == null);

    const bool_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .term = .{ .field = "/tier", .term = "silver" } },
    };
    const top_level_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..], .min_should = 1 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, top_level_should_query);
    try std.testing.expectEqual(@as(usize, 1), top_level_should_query.len);
    try std.testing.expectEqualStrings("/tier", top_level_should_query[0].field);
    const top_level_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, top_level_should_query[0].value);
    defer {
        for (top_level_should_parts) |part| alloc.free(part);
        alloc.free(top_level_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", top_level_should_parts[0]);

    const implicit_top_level_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..] } },
    })).?;
    defer freeAlgebraicConstraints(alloc, implicit_top_level_should_query);
    try std.testing.expectEqual(@as(usize, 1), implicit_top_level_should_query.len);
    try std.testing.expectEqualStrings("/tier", implicit_top_level_should_query[0].field);
    const implicit_top_level_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, implicit_top_level_should_query[0].value);
    defer {
        for (implicit_top_level_should_parts) |part| alloc.free(part);
        alloc.free(implicit_top_level_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), implicit_top_level_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", implicit_top_level_should_parts[0]);

    const two_required_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..], .min_should = 2 } },
    });
    try std.testing.expect(two_required_should_query == null);

    const mixed_field_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .term = .{ .field = "/region", .term = "west" } },
    };
    const mixed_field_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = mixed_field_should_queries[0..], .min_should = 1 } },
    });
    try std.testing.expect(mixed_field_should_query == null);

    const bool_must_not_queries = [_]db_mod.types.TextQuery{.{ .term = .{ .field = "/tier", .term = "gold" } }};
    const top_level_must_not_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .must_not = bool_must_not_queries[0..] } },
    });
    try std.testing.expect(top_level_must_not_query == null);

    const from_path_term_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term = .{ .field = "/tier", .term = "gold" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_term_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_term_query[0].field);
    const top_level_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_query[0].value);
    defer {
        for (top_level_term_parts) |part| alloc.free(part);
        alloc.free(top_level_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), top_level_term_parts.len);
    try std.testing.expectEqualStrings("string", top_level_term_parts[0]);
    try std.testing.expectEqualStrings("gold", top_level_term_parts[1]);

    const from_path_match_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match = .{ .field = "/tier", .text = "OLD" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_match_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_match_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_match_query[0].field);
    const top_level_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_query[0].value);
    defer {
        for (top_level_match_parts) |part| alloc.free(part);
        alloc.free(top_level_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", top_level_match_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_match_parts[1]);
    try std.testing.expectEqualStrings("OLD", top_level_match_parts[2]);

    const analyzed_path_match_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match = .{ .field = "/tier", .text = "gold", .analyzer = "default" } },
    });
    try std.testing.expect(analyzed_path_match_query == null);

    const from_path_fuzzy_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gild", .max_edits = 1, .prefix_len = 1 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_fuzzy_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_fuzzy_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_fuzzy_query[0].field);
    const top_level_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_query[0].value);
    defer {
        for (top_level_fuzzy_parts) |part| alloc.free(part);
        alloc.free(top_level_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), top_level_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", top_level_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("gild", top_level_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", top_level_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_fuzzy_parts[4]);

    const unbounded_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gold", .max_edits = 1 } },
    });
    try std.testing.expect(unbounded_path_fuzzy_query == null);

    const auto_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gold", .auto_fuzzy = true, .prefix_len = 1 } },
    });
    try std.testing.expect(auto_path_fuzzy_query == null);

    const non_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "tier", .term = "gold", .max_edits = 1, .prefix_len = 1 } },
    });
    try std.testing.expect(non_path_fuzzy_query == null);

    const from_path_prefix_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .prefix = .{ .field = "/tier", .prefix = "go" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_prefix_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_prefix_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_prefix_query[0].field);
    const top_level_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_query[0].value);
    defer {
        for (top_level_prefix_parts) |part| alloc.free(part);
        alloc.free(top_level_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", top_level_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_prefix_parts[1]);
    try std.testing.expectEqualStrings("go", top_level_prefix_parts[2]);

    const non_path_prefix_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .prefix = .{ .field = "tier", .prefix = "go" } },
    });
    try std.testing.expect(non_path_prefix_query == null);

    const from_path_wildcard_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "/tier", .pattern = "go*" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_wildcard_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_wildcard_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_wildcard_query[0].field);
    const top_level_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_query[0].value);
    defer {
        for (top_level_wildcard_parts) |part| alloc.free(part);
        alloc.free(top_level_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", top_level_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_wildcard_parts[1]);
    try std.testing.expectEqualStrings("go*", top_level_wildcard_parts[2]);

    const leading_wildcard_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "/tier", .pattern = "*old" } },
    });
    try std.testing.expect(leading_wildcard_query == null);

    const non_path_wildcard_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "tier", .pattern = "go*" } },
    });
    try std.testing.expect(non_path_wildcard_query == null);

    const from_path_regexp_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = "go.*" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_regexp_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_regexp_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_regexp_query[0].field);
    const top_level_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_query[0].value);
    defer {
        for (top_level_regexp_parts) |part| alloc.free(part);
        alloc.free(top_level_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", top_level_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_regexp_parts[1]);
    try std.testing.expectEqualStrings("go.*", top_level_regexp_parts[2]);

    const leading_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = ".*old" } },
    });
    try std.testing.expect(leading_regexp_query == null);

    const invalid_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = "go(" } },
    });
    try std.testing.expect(invalid_regexp_query == null);

    const non_path_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "tier", .pattern = "go.*" } },
    });
    try std.testing.expect(non_path_regexp_query == null);

    const from_path_numeric_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .numeric_range = .{ .field = "/amount", .min = 10, .max = 30, .inclusive_min = true, .inclusive_max = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_numeric_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_numeric_query.len);
    try std.testing.expectEqualStrings("/amount", from_path_numeric_query[0].field);
    const top_level_amount_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_query[0].value);
    defer {
        for (top_level_amount_range_parts) |part| alloc.free(part);
        alloc.free(top_level_amount_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_amount_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", top_level_amount_range_parts[0]);
    try std.testing.expectEqualStrings("number", top_level_amount_range_parts[1]);
    try std.testing.expectEqualStrings("10", top_level_amount_range_parts[2]);
    try std.testing.expectEqualStrings("30", top_level_amount_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_amount_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_amount_range_parts[5]);

    const non_path_numeric_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .numeric_range = .{ .field = "amount", .min = 10 } },
    });
    try std.testing.expect(non_path_numeric_query == null);

    const from_path_term_range_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "/status", .min = "active", .max = "archived", .inclusive_min = true, .inclusive_max = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_range_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_term_range_query.len);
    try std.testing.expectEqualStrings("/status", from_path_term_range_query[0].field);
    const top_level_term_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_query[0].value);
    defer {
        for (top_level_term_range_parts) |part| alloc.free(part);
        alloc.free(top_level_term_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_term_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", top_level_term_range_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_term_range_parts[1]);
    try std.testing.expectEqualStrings("active", top_level_term_range_parts[2]);
    try std.testing.expectEqualStrings("archived", top_level_term_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_term_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_term_range_parts[5]);

    const unbounded_term_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "/status" } },
    });
    try std.testing.expect(unbounded_term_range_query == null);

    const non_path_term_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "status", .min = "active" } },
    });
    try std.testing.expect(non_path_term_range_query == null);

    const from_path_ip_range_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "/client_ip", .cidr = "10.1.0.0/16" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_ip_range_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_ip_range_query.len);
    try std.testing.expectEqualStrings("/client_ip", from_path_ip_range_query[0].field);
    const top_level_ip_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_query[0].value);
    defer {
        for (top_level_ip_range_parts) |part| alloc.free(part);
        alloc.free(top_level_ip_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_ip_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", top_level_ip_range_parts[0]);
    try std.testing.expectEqualStrings("ipv4", top_level_ip_range_parts[1]);
    try std.testing.expectEqualStrings("10.1.0.0/16", top_level_ip_range_parts[2]);

    const invalid_ip_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "/client_ip", .cidr = "10.999.0.0/16" } },
    });
    try std.testing.expect(invalid_ip_range_query == null);

    const non_path_ip_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "client_ip", .cidr = "10.1.0.0/16" } },
    });
    try std.testing.expect(non_path_ip_range_query == null);

    const from_path_geo_bbox_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_bbox = .{ .field = "/location", .min_lat = 37.70, .min_lon = -122.50, .max_lat = 37.80, .max_lon = -122.30 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_bbox_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_bbox_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_bbox_query[0].field);
    const top_level_geo_bbox_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_bbox_query[0].value);
    defer {
        for (top_level_geo_bbox_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_bbox_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_geo_bbox_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-bbox:v1", top_level_geo_bbox_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_bbox_parts[1]);
    try std.testing.expectEqualStrings("37.7", top_level_geo_bbox_parts[2]);
    try std.testing.expectEqualStrings("-122.5", top_level_geo_bbox_parts[3]);
    try std.testing.expectEqualStrings("37.8", top_level_geo_bbox_parts[4]);
    try std.testing.expectEqualStrings("-122.3", top_level_geo_bbox_parts[5]);

    const from_path_geo_distance_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_distance = .{ .field = "/location", .lat = 37.7749, .lon = -122.4194, .radius_meters = 2000 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_distance_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_distance_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_distance_query[0].field);
    const top_level_geo_distance_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_distance_query[0].value);
    defer {
        for (top_level_geo_distance_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_distance_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), top_level_geo_distance_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-distance:v1", top_level_geo_distance_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_distance_parts[1]);
    try std.testing.expectEqualStrings("37.7749", top_level_geo_distance_parts[2]);
    try std.testing.expectEqualStrings("-122.4194", top_level_geo_distance_parts[3]);
    try std.testing.expectEqualStrings("2000", top_level_geo_distance_parts[4]);

    const direct_shape_polygon = [_]db_mod.types.GeoPoint{
        .{ .lat = 37.0, .lon = -123.0 },
        .{ .lat = 38.0, .lon = -123.0 },
        .{ .lat = 38.0, .lon = -122.0 },
        .{ .lat = 37.0, .lon = -122.0 },
    };
    const direct_shape_polygons = [_][]const db_mod.types.GeoPoint{direct_shape_polygon[0..]};
    const from_path_geo_shape_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_shape = .{
            .field = "/location",
            .relation = .intersects,
            .polygons = direct_shape_polygons[0..],
        } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_shape_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_shape_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_shape_query[0].field);
    const top_level_geo_shape_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_shape_query[0].value);
    defer {
        for (top_level_geo_shape_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_shape_parts);
    }
    try std.testing.expectEqual(@as(usize, 13), top_level_geo_shape_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-shape:v1", top_level_geo_shape_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_shape_parts[1]);
    try std.testing.expectEqualStrings("intersects", top_level_geo_shape_parts[2]);
    try std.testing.expectEqualStrings("1", top_level_geo_shape_parts[3]);
    try std.testing.expectEqualStrings("4", top_level_geo_shape_parts[4]);

    const contains_geo_shape_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_shape = .{
            .field = "/location",
            .relation = .contains,
            .polygons = direct_shape_polygons[0..],
        } },
    });
    try std.testing.expect(contains_geo_shape_query == null);

    const non_path_geo_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_bbox = .{ .field = "location", .min_lat = 37.70, .min_lon = -122.50, .max_lat = 37.80, .max_lon = -122.30 } },
    });
    try std.testing.expect(non_path_geo_query == null);

    const from_path_date_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .date_range = .{ .field = "/published_at", .start_ns = 1767225600000000000, .end_ns = 1767312000000000000, .inclusive_start = true, .inclusive_end = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_date_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_date_query.len);
    try std.testing.expectEqualStrings("/published_at", from_path_date_query[0].field);
    const top_level_date_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_query[0].value);
    defer {
        for (top_level_date_range_parts) |part| alloc.free(part);
        alloc.free(top_level_date_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_date_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", top_level_date_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", top_level_date_range_parts[1]);
    try std.testing.expectEqualStrings("1767225600000000000", top_level_date_range_parts[2]);
    try std.testing.expectEqualStrings("1767312000000000000", top_level_date_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_date_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_date_range_parts[5]);

    const non_path_date_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .date_range = .{ .field = "published_at", .start_ns = 1767225600000000000 } },
    });
    try std.testing.expect(non_path_date_query == null);

    const from_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":[{\"term\":{\"tenant\":\"t1\"}},{\"bool_field\":{\"field\":\"paid\",\"value\":false}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_filter);
    try std.testing.expectEqual(@as(usize, 2), from_filter.len);
    try std.testing.expectEqualStrings("tenant", from_filter[0].field);
    try std.testing.expectEqualStrings("t1", from_filter[0].value);
    try std.testing.expectEqualStrings("paid", from_filter[1].field);
    try std.testing.expectEqualStrings("false", from_filter[1].value);

    const from_numeric_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"amount\":42}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_numeric_filter);
    try std.testing.expectEqual(@as(usize, 1), from_numeric_filter.len);
    try std.testing.expectEqualStrings("amount", from_numeric_filter[0].field);
    try std.testing.expectEqualStrings("42", from_numeric_filter[0].value);

    const from_path_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term\":{\"/active\":true}},{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/deleted_at\":null}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_filter);
    try std.testing.expectEqual(@as(usize, 3), from_path_filter.len);
    try std.testing.expectEqualStrings("/active", from_path_filter[0].field);
    const active_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[0].value);
    defer {
        for (active_parts) |part| alloc.free(part);
        alloc.free(active_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), active_parts.len);
    try std.testing.expectEqualStrings("bool", active_parts[0]);
    try std.testing.expectEqualStrings("true", active_parts[1]);
    try std.testing.expectEqualStrings("/tier", from_path_filter[1].field);
    const tier_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[1].value);
    defer {
        for (tier_parts) |part| alloc.free(part);
        alloc.free(tier_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), tier_parts.len);
    try std.testing.expectEqualStrings("string", tier_parts[0]);
    try std.testing.expectEqualStrings("gold", tier_parts[1]);
    try std.testing.expectEqualStrings("/deleted_at", from_path_filter[2].field);
    const null_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[2].value);
    defer {
        for (null_parts) |part| alloc.free(part);
        alloc.free(null_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), null_parts.len);
    try std.testing.expectEqualStrings("null", null_parts[0]);
    try std.testing.expectEqualStrings("", null_parts[1]);

    const from_direct_path_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":\"gold\",\"field\":\"/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_term_filter[0].field);
    const direct_path_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_term_filter[0].value);
    defer {
        for (direct_path_term_parts) |part| alloc.free(part);
        alloc.free(direct_path_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), direct_path_term_parts.len);
    try std.testing.expectEqualStrings("string", direct_path_term_parts[0]);
    try std.testing.expectEqualStrings("gold", direct_path_term_parts[1]);

    const from_direct_path_alias_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":\"gold\",\"path\":\"/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_alias_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_alias_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_alias_term_filter[0].field);
    const direct_path_alias_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_alias_term_filter[0].value);
    defer {
        for (direct_path_alias_term_parts) |part| alloc.free(part);
        alloc.free(direct_path_alias_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), direct_path_alias_term_parts.len);
    try std.testing.expectEqualStrings("string", direct_path_alias_term_parts[0]);
    try std.testing.expectEqualStrings("gold", direct_path_alias_term_parts[1]);

    const from_field_value_path_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"field\":\"/tier\",\"value\":\"gold\"}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_field_value_path_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_field_value_path_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_field_value_path_term_filter[0].field);
    const field_value_path_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_field_value_path_term_filter[0].value);
    defer {
        for (field_value_path_term_parts) |part| alloc.free(part);
        alloc.free(field_value_path_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), field_value_path_term_parts.len);
    try std.testing.expectEqualStrings("string", field_value_path_term_parts[0]);
    try std.testing.expectEqualStrings("gold", field_value_path_term_parts[1]);

    const from_wrapped_path_alias_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"path\":\"/tier\",\"value\":\"gold\"}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_wrapped_path_alias_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_wrapped_path_alias_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_wrapped_path_alias_term_filter[0].field);

    const from_terms_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":{\"terms\":{\"tenant\":[\"t1\"]}},\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}}}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_terms_filter);
    try std.testing.expectEqual(@as(usize, 2), from_terms_filter.len);
    var saw_tenant_terms = false;
    var saw_region_terms = false;
    for (from_terms_filter) |constraint| {
        if (std.mem.eql(u8, constraint.field, "tenant") and std.mem.eql(u8, constraint.value, "t1")) saw_tenant_terms = true;
        if (std.mem.eql(u8, constraint.field, "region") and std.mem.eql(u8, constraint.value, "west")) saw_region_terms = true;
    }
    try std.testing.expect(saw_tenant_terms);
    try std.testing.expect(saw_region_terms);

    const from_optional_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":{\"terms\":{\"tenant\":[\"t1\"]}},\"should\":[{\"term\":{\"region\":\"west\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_optional_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_optional_should_filter.len);
    try std.testing.expectEqualStrings("tenant", from_optional_should_filter[0].field);
    try std.testing.expectEqualStrings("t1", from_optional_should_filter[0].value);

    const from_explicit_optional_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}},\"should\":[{\"term\":{\"tenant\":\"t1\"}}],\"minimum_should_match\":0}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_explicit_optional_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_explicit_optional_should_filter.len);
    try std.testing.expectEqualStrings("region", from_explicit_optional_should_filter[0].field);
    try std.testing.expectEqualStrings("west", from_explicit_optional_should_filter[0].value);

    const unsupported_required_optional_should_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}},\"should\":[{\"term\":{\"tenant\":\"t1\"}}],\"minimum_should_match\":1}}",
    });
    try std.testing.expect(unsupported_required_optional_should_filter == null);

    const from_path_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"should\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/tier\":\"silver\"}}],\"minimum_should_match\":1}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_should_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_should_filter[0].field);
    const filter_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_should_filter[0].value);
    defer {
        for (filter_should_parts) |part| alloc.free(part);
        alloc.free(filter_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), filter_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", filter_should_parts[0]);

    const from_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/tier\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_disjuncts_filter[0].field);
    const disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_disjuncts_filter[0].value);
    defer {
        for (disjuncts_parts) |part| alloc.free(part);
        alloc.free(disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", disjuncts_parts[0]);

    const from_direct_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":\"gold\",\"field\":\"/tier\"},{\"term\":\"bronze\",\"field\":\"/tier\"}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_disjuncts_filter[0].field);
    const direct_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_disjuncts_filter[0].value);
    defer {
        for (direct_disjuncts_parts) |part| alloc.free(part);
        alloc.free(direct_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), direct_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", direct_disjuncts_parts[0]);

    const from_field_value_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"field\":\"/tier\",\"term\":\"gold\"}},{\"term\":{\"field\":\"/tier\",\"value\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_field_value_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_field_value_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_field_value_path_disjuncts_filter[0].field);
    const field_value_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_field_value_path_disjuncts_filter[0].value);
    defer {
        for (field_value_disjuncts_parts) |part| alloc.free(part);
        alloc.free(field_value_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), field_value_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", field_value_disjuncts_parts[0]);

    const from_path_alias_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":\"gold\",\"path\":\"/tier\"},{\"term\":{\"path\":\"/tier\",\"value\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_alias_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_alias_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_alias_disjuncts_filter[0].field);
    const path_alias_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_alias_disjuncts_filter[0].value);
    defer {
        for (path_alias_disjuncts_parts) |part| alloc.free(part);
        alloc.free(path_alias_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), path_alias_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", path_alias_disjuncts_parts[0]);

    const mixed_path_should_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"should\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/region\":\"west\"}}],\"minimum_should_match\":1}}",
    });
    try std.testing.expect(mixed_path_should_filter == null);

    const from_path_terms_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"path\":\"/tier\",\"values\":[\"gold\",\"silver\",null]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_terms_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_terms_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_terms_filter[0].field);
    const any_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_terms_filter[0].value);
    defer {
        for (any_parts) |part| alloc.free(part);
        alloc.free(any_parts);
    }
    try std.testing.expectEqual(@as(usize, 4), any_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", any_parts[0]);
    const any_first = try db_mod.algebraic.token.decodeTupleAlloc(alloc, any_parts[1]);
    defer {
        for (any_first) |part| alloc.free(part);
        alloc.free(any_first);
    }
    try std.testing.expectEqualStrings("string", any_first[0]);
    try std.testing.expectEqualStrings("gold", any_first[1]);
    const any_null = try db_mod.algebraic.token.decodeTupleAlloc(alloc, any_parts[3]);
    defer {
        for (any_null) |part| alloc.free(part);
        alloc.free(any_null);
    }
    try std.testing.expectEqualStrings("null", any_null[0]);
    try std.testing.expectEqualStrings("", any_null[1]);

    const non_path_multi_terms_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"tenant\":[\"t1\",\"t2\"]}}",
    });
    try std.testing.expect(non_path_multi_terms_filter == null);

    const from_path_exists_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"exists\":{\"path\":\"/metadata/tier\"}},{\"exists\":\"/tenant\"}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_exists_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_exists_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_exists_filter[0].field);
    try std.testing.expectEqualStrings(db_mod.algebraic.index.path_fact_exists_constraint_value, from_path_exists_filter[0].value);
    try std.testing.expectEqualStrings("/tenant", from_path_exists_filter[1].field);
    try std.testing.expectEqualStrings(db_mod.algebraic.index.path_fact_exists_constraint_value, from_path_exists_filter[1].value);

    const from_path_prefix_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"prefix\":{\"path\":\"/metadata/tier\",\"prefix\":\"go\"}},{\"prefix\":{\"/tenant\":\"ac\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_prefix_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_prefix_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_prefix_filter[0].field);
    const tier_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_filter[0].value);
    defer {
        for (tier_prefix_parts) |part| alloc.free(part);
        alloc.free(tier_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", tier_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", tier_prefix_parts[1]);
    try std.testing.expectEqualStrings("go", tier_prefix_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_prefix_filter[1].field);
    const tenant_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_filter[1].value);
    defer {
        for (tenant_prefix_parts) |part| alloc.free(part);
        alloc.free(tenant_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", tenant_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_prefix_parts[1]);
    try std.testing.expectEqualStrings("ac", tenant_prefix_parts[2]);

    const non_path_prefix_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"prefix\":{\"tenant\":\"ac\"}}",
    });
    try std.testing.expect(non_path_prefix_filter == null);

    const from_path_match_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"match\":{\"path\":\"/metadata/tier\",\"value\":\"OLD\"}},{\"match\":{\"/tenant\":\"ICE\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_match_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_match_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_match_filter[0].field);
    const tier_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_filter[0].value);
    defer {
        for (tier_match_parts) |part| alloc.free(part);
        alloc.free(tier_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", tier_match_parts[0]);
    try std.testing.expectEqualStrings("string", tier_match_parts[1]);
    try std.testing.expectEqualStrings("OLD", tier_match_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_match_filter[1].field);
    const tenant_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_filter[1].value);
    defer {
        for (tenant_match_parts) |part| alloc.free(part);
        alloc.free(tenant_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", tenant_match_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_match_parts[1]);
    try std.testing.expectEqualStrings("ICE", tenant_match_parts[2]);

    const sibling_path_match_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"match\":\"old\",\"field\":\"/metadata/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, sibling_path_match_filter);
    try std.testing.expectEqual(@as(usize, 1), sibling_path_match_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", sibling_path_match_filter[0].field);

    const non_path_match_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"match\":{\"tenant\":\"ICE\"}}",
    });
    try std.testing.expect(non_path_match_filter == null);

    const from_path_wildcard_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"wildcard\":{\"path\":\"/metadata/tier\",\"pattern\":\"go*\"}},{\"wildcard\":{\"/tenant\":\"ac?e\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_wildcard_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_wildcard_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_wildcard_filter[0].field);
    const tier_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_filter[0].value);
    defer {
        for (tier_wildcard_parts) |part| alloc.free(part);
        alloc.free(tier_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", tier_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", tier_wildcard_parts[1]);
    try std.testing.expectEqualStrings("go*", tier_wildcard_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_wildcard_filter[1].field);
    const tenant_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_filter[1].value);
    defer {
        for (tenant_wildcard_parts) |part| alloc.free(part);
        alloc.free(tenant_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", tenant_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_wildcard_parts[1]);
    try std.testing.expectEqualStrings("ac?e", tenant_wildcard_parts[2]);

    const leading_wildcard_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    });
    try std.testing.expect(leading_wildcard_filter == null);

    const non_path_wildcard_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"wildcard\":{\"tenant\":\"ac*\"}}",
    });
    try std.testing.expect(non_path_wildcard_filter == null);

    const from_path_regexp_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"regexp\":{\"path\":\"/metadata/tier\",\"pattern\":\"go.*\"}},{\"regexp\":{\"/tenant\":\"ac.e\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_regexp_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_regexp_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_regexp_filter[0].field);
    const tier_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_filter[0].value);
    defer {
        for (tier_regexp_parts) |part| alloc.free(part);
        alloc.free(tier_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", tier_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", tier_regexp_parts[1]);
    try std.testing.expectEqualStrings("go.*", tier_regexp_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_regexp_filter[1].field);
    const tenant_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_filter[1].value);
    defer {
        for (tenant_regexp_parts) |part| alloc.free(part);
        alloc.free(tenant_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", tenant_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_regexp_parts[1]);
    try std.testing.expectEqualStrings("ac.e", tenant_regexp_parts[2]);

    const leading_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"/tenant\":\".*ice\"}}",
    });
    try std.testing.expect(leading_regexp_filter == null);

    const invalid_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"/tenant\":\"ac(\"}}",
    });
    try std.testing.expect(invalid_regexp_filter == null);

    const non_path_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"tenant\":\"ac.*\"}}",
    });
    try std.testing.expect(non_path_regexp_filter == null);

    const from_path_fuzzy_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"fuzzy\":{\"path\":\"/metadata/tier\",\"query\":\"gild\",\"prefix_length\":1,\"max_edits\":1}},{\"fuzzy\":{\"/tenant\":{\"query\":\"alpine\",\"prefix_length\":2,\"max_edits\":1}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_fuzzy_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_fuzzy_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_fuzzy_filter[0].field);
    const tier_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_filter[0].value);
    defer {
        for (tier_fuzzy_parts) |part| alloc.free(part);
        alloc.free(tier_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), tier_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", tier_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", tier_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("gild", tier_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", tier_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("1", tier_fuzzy_parts[4]);
    try std.testing.expectEqualStrings("/tenant", from_path_fuzzy_filter[1].field);
    const tenant_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_filter[1].value);
    defer {
        for (tenant_fuzzy_parts) |part| alloc.free(part);
        alloc.free(tenant_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), tenant_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", tenant_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("alpine", tenant_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", tenant_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("2", tenant_fuzzy_parts[4]);

    const unbounded_fuzzy_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"fuzzy\":{\"/tenant\":{\"query\":\"alice\",\"max_edits\":1}}}",
    });
    try std.testing.expect(unbounded_fuzzy_filter == null);

    const non_path_fuzzy_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"fuzzy\":{\"tenant\":{\"query\":\"alice\",\"prefix_length\":1}}}",
    });
    try std.testing.expect(non_path_fuzzy_filter == null);

    const from_path_numeric_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"max\":30,\"inclusive_min\":true,\"inclusive_max\":false}},{\"range\":{\"/score\":{\"gte\":7,\"lt\":9}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_numeric_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_numeric_range_filter.len);
    try std.testing.expectEqualStrings("/amount", from_path_numeric_range_filter[0].field);
    const amount_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_range_filter[0].value);
    defer {
        for (amount_range_parts) |part| alloc.free(part);
        alloc.free(amount_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), amount_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", amount_range_parts[0]);
    try std.testing.expectEqualStrings("number", amount_range_parts[1]);
    try std.testing.expectEqualStrings("10", amount_range_parts[2]);
    try std.testing.expectEqualStrings("30", amount_range_parts[3]);
    try std.testing.expectEqualStrings("1", amount_range_parts[4]);
    try std.testing.expectEqualStrings("0", amount_range_parts[5]);
    try std.testing.expectEqualStrings("/score", from_path_numeric_range_filter[1].field);
    const score_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_range_filter[1].value);
    defer {
        for (score_range_parts) |part| alloc.free(part);
        alloc.free(score_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), score_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", score_range_parts[0]);
    try std.testing.expectEqualStrings("number", score_range_parts[1]);
    try std.testing.expectEqualStrings("7", score_range_parts[2]);
    try std.testing.expectEqualStrings("9", score_range_parts[3]);
    try std.testing.expectEqualStrings("1", score_range_parts[4]);
    try std.testing.expectEqualStrings("0", score_range_parts[5]);

    const upper_only_path_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"range\":{\"field\":\"/score\",\"lte\":9}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, upper_only_path_range_filter);
    try std.testing.expectEqual(@as(usize, 1), upper_only_path_range_filter.len);
    const upper_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, upper_only_path_range_filter[0].value);
    defer {
        for (upper_range_parts) |part| alloc.free(part);
        alloc.free(upper_range_parts);
    }
    try std.testing.expectEqualStrings("", upper_range_parts[2]);
    try std.testing.expectEqualStrings("9", upper_range_parts[3]);
    try std.testing.expectEqualStrings("1", upper_range_parts[4]);
    try std.testing.expectEqualStrings("1", upper_range_parts[5]);

    const non_path_numeric_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"numeric_range\":{\"field\":\"amount\",\"min\":10}}",
    });
    try std.testing.expect(non_path_numeric_range_filter == null);

    const from_path_date_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"date_range\":{\"path\":\"/published_at\",\"start\":\"2026-01-02T00:00:00Z\",\"end\":\"2026-01-03T00:00:00Z\",\"inclusive_start\":true,\"inclusive_end\":false}},{\"range\":{\"/created_at\":{\"gte\":\"2026-01-04\",\"lt\":\"2026-01-05\"}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_date_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_date_range_filter.len);
    try std.testing.expectEqualStrings("/published_at", from_path_date_range_filter[0].field);
    const published_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_range_filter[0].value);
    defer {
        for (published_range_parts) |part| alloc.free(part);
        alloc.free(published_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), published_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", published_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", published_range_parts[1]);
    try std.testing.expectEqualStrings("2026-01-02T00:00:00Z", published_range_parts[2]);
    try std.testing.expectEqualStrings("2026-01-03T00:00:00Z", published_range_parts[3]);
    try std.testing.expectEqualStrings("1", published_range_parts[4]);
    try std.testing.expectEqualStrings("0", published_range_parts[5]);
    try std.testing.expectEqualStrings("/created_at", from_path_date_range_filter[1].field);
    const created_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_range_filter[1].value);
    defer {
        for (created_range_parts) |part| alloc.free(part);
        alloc.free(created_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), created_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", created_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", created_range_parts[1]);
    try std.testing.expectEqualStrings("2026-01-04", created_range_parts[2]);
    try std.testing.expectEqualStrings("2026-01-05", created_range_parts[3]);
    try std.testing.expectEqualStrings("1", created_range_parts[4]);
    try std.testing.expectEqualStrings("0", created_range_parts[5]);

    const non_path_date_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"date_range\":{\"field\":\"published_at\",\"start\":\"2026-01-02T00:00:00Z\"}}",
    });
    try std.testing.expect(non_path_date_range_filter == null);

    const from_path_ip_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"ip_range\":{\"path\":\"/client_ip\",\"cidr\":\"10.1.0.0/16\"}},{\"ip_range\":{\"field\":\"/gateway_ip\",\"cidr\":\"192.168.1.10\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_ip_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_ip_range_filter.len);
    try std.testing.expectEqualStrings("/client_ip", from_path_ip_range_filter[0].field);
    const client_ip_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_filter[0].value);
    defer {
        for (client_ip_parts) |part| alloc.free(part);
        alloc.free(client_ip_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), client_ip_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", client_ip_parts[0]);
    try std.testing.expectEqualStrings("ipv4", client_ip_parts[1]);
    try std.testing.expectEqualStrings("10.1.0.0/16", client_ip_parts[2]);
    try std.testing.expectEqualStrings("/gateway_ip", from_path_ip_range_filter[1].field);
    const gateway_ip_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_filter[1].value);
    defer {
        for (gateway_ip_parts) |part| alloc.free(part);
        alloc.free(gateway_ip_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), gateway_ip_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", gateway_ip_parts[0]);
    try std.testing.expectEqualStrings("ipv4", gateway_ip_parts[1]);
    try std.testing.expectEqualStrings("192.168.1.10", gateway_ip_parts[2]);

    const non_path_ip_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"ip_range\":{\"field\":\"client_ip\",\"cidr\":\"10.1.0.0/16\"}}",
    });
    try std.testing.expect(non_path_ip_range_filter == null);

    const invalid_ip_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"ip_range\":{\"path\":\"/client_ip\",\"cidr\":\"10.999.0.0/16\"}}",
    });
    try std.testing.expect(invalid_ip_range_filter == null);

    const from_path_geo_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"geo_bbox\":{\"path\":\"/location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}},{\"geo_distance\":{\"field\":\"/warehouse\",\"lat\":40.7128,\"lon\":-74.006,\"radius_meters\":5000}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_geo_filter.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_filter[0].field);
    const location_geo_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_filter[0].value);
    defer {
        for (location_geo_parts) |part| alloc.free(part);
        alloc.free(location_geo_parts);
    }
    try std.testing.expectEqualStrings("pathfact-geo-bbox:v1", location_geo_parts[0]);
    try std.testing.expectEqualStrings("/warehouse", from_path_geo_filter[1].field);
    const warehouse_geo_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_filter[1].value);
    defer {
        for (warehouse_geo_parts) |part| alloc.free(part);
        alloc.free(warehouse_geo_parts);
    }
    try std.testing.expectEqualStrings("pathfact-geo-distance:v1", warehouse_geo_parts[0]);

    const from_path_geo_shape_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"within\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_shape_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_shape_filter.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_shape_filter[0].field);
    const location_geo_shape_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_shape_filter[0].value);
    defer {
        for (location_geo_shape_parts) |part| alloc.free(part);
        alloc.free(location_geo_shape_parts);
    }
    try std.testing.expectEqual(@as(usize, 13), location_geo_shape_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-shape:v1", location_geo_shape_parts[0]);
    try std.testing.expectEqualStrings("geo_point", location_geo_shape_parts[1]);
    try std.testing.expectEqualStrings("within", location_geo_shape_parts[2]);

    const unsupported_geo_shape_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"contains\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0}]]}}",
    });
    try std.testing.expect(unsupported_geo_shape_filter == null);

    const non_path_geo_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_bbox\":{\"field\":\"location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}}",
    });
    try std.testing.expect(non_path_geo_filter == null);

    const invalid_geo_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_distance\":{\"path\":\"/location\",\"lat\":95,\"lon\":-122.4194,\"radius_meters\":2000}}",
    });
    try std.testing.expect(invalid_geo_filter == null);

    const from_path_term_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term_range\":{\"path\":\"/tenant\",\"min\":\"alpi\",\"max\":\"alpj\",\"inclusive_min\":true,\"inclusive_max\":false}},{\"range\":{\"/status\":{\"gte\":\"active\",\"lt\":\"archived\"}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_term_range_filter.len);
    try std.testing.expectEqualStrings("/tenant", from_path_term_range_filter[0].field);
    const tenant_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_filter[0].value);
    defer {
        for (tenant_range_parts) |part| alloc.free(part);
        alloc.free(tenant_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), tenant_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", tenant_range_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_range_parts[1]);
    try std.testing.expectEqualStrings("alpi", tenant_range_parts[2]);
    try std.testing.expectEqualStrings("alpj", tenant_range_parts[3]);
    try std.testing.expectEqualStrings("1", tenant_range_parts[4]);
    try std.testing.expectEqualStrings("0", tenant_range_parts[5]);
    try std.testing.expectEqualStrings("/status", from_path_term_range_filter[1].field);
    const status_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_filter[1].value);
    defer {
        for (status_range_parts) |part| alloc.free(part);
        alloc.free(status_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), status_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", status_range_parts[0]);
    try std.testing.expectEqualStrings("string", status_range_parts[1]);
    try std.testing.expectEqualStrings("active", status_range_parts[2]);
    try std.testing.expectEqualStrings("archived", status_range_parts[3]);
    try std.testing.expectEqualStrings("1", status_range_parts[4]);
    try std.testing.expectEqualStrings("0", status_range_parts[5]);

    const non_path_term_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term_range\":{\"field\":\"tenant\",\"min\":\"a\"}}",
    });
    try std.testing.expect(non_path_term_range_filter == null);

    const non_path_exists_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"exists\":\"tenant\"}",
    });
    try std.testing.expect(non_path_exists_filter == null);

    const multi_terms_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"tenant\":[\"t1\",\"t2\"]}}",
    });
    try std.testing.expect(multi_terms_filter == null);
}

test "algebraic constraints reject top-level text term query" {
    const alloc = std.testing.allocator;
    const constraints = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term = .{ .field = "body", .term = "published" } },
    });
    try std.testing.expect(constraints == null);
}

fn applyAggregationResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
    ctx: db_mod.aggregations.Context,
    meta: *query_api.QueryResponseMeta,
) !void {
    if (req.aggregations_json.len == 0) return;
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    var aggregation_ctx = ctx;
    const constraints = if (canConsiderAlgebraicAggregations(req))
        try algebraicConstraintsForRequestAlloc(alloc, req)
    else
        null;
    defer if (constraints) |items| freeAlgebraicConstraints(alloc, items);
    if (constraints) |items| if (aggregation_ctx.algebraic_available) {
        aggregation_ctx.algebraic_scope = .root;
        aggregation_ctx.algebraic_constraints = items;
    };
    meta.aggregation_results = try db_mod.aggregations.computeSearchAggregations(alloc, requests, result, aggregation_ctx);
}

fn requestWithResultIdentityGeneration(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
) db_mod.types.SearchRequest {
    var out = req;
    if (out.identity_read_generation == null) out.identity_read_generation = result.identity_read_generation;
    return out;
}

fn identityGenerationForAggregationFullResultRerun(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
) !?u64 {
    if (!req.count_only and result.hits.len == result.total_hits) return req.identity_read_generation orelse result.identity_read_generation;
    if (result.total_hits == 0) return req.identity_read_generation orelse result.identity_read_generation;
    return req.identity_read_generation orelse result.identity_read_generation orelse error.UnsupportedQueryRequest;
}

fn applyBoundQueryAggregations(
    self: *BoundTableReadSource,
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (req.aggregations_json.len == 0) return;
    const aggregation_req = requestWithResultIdentityGeneration(req, result.*);
    if (!req.count_only and result.hits.len == result.total_hits) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, self.db), meta);
    }
    if (result.total_hits == 0) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, self.db), meta);
    }

    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = result.total_hits;
    full_req.include_stored = true;
    full_req.count_only = false;
    var full_result = try self.reads.searchWithConsistency(alloc, self.db, full_req, consistency);
    defer full_result.deinit();
    return try applyAggregationResults(alloc, full_req, full_result, try aggregationContextForDb(alloc, full_req, self.db), meta);
}

fn applyProvisionedQueryAggregations(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (req.aggregations_json.len == 0) return;
    const aggregation_req = requestWithResultIdentityGeneration(req, result.*);
    if (group_ids.len == 1) {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
        defer alloc.free(path);
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_ids[0], 0, self.backend_runtime);
        defer db.close();

        if (!req.count_only and result.hits.len == result.total_hits) {
            return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, &db), meta);
        }
        if (result.total_hits == 0) {
            return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, &db), meta);
        }

        var reads = raft_mod.FeatureDBReads.init(group_ids[0], self.requester);
        const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
        var full_req = req;
        full_req.identity_read_generation = identity_read_generation;
        full_req.offset = 0;
        full_req.limit = result.total_hits;
        full_req.include_stored = true;
        full_req.count_only = false;
        var full_result = try reads.searchWithConsistency(alloc, &db, full_req, consistency);
        defer full_result.deinit();
        return try applyAggregationResults(alloc, full_req, full_result, try aggregationContextForDb(alloc, full_req, &db), meta);
    }

    if (try tryApplyProvisionedAlgebraicDistributedAggregations(self, alloc, group_ids, table_name, aggregation_req, meta)) return;

    const current_agg_stats = try collectProvisionedAggregationTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, current_agg_stats);
    const current_bg_stats = try collectProvisionedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, current_bg_stats);
    if (!req.count_only and result.hits.len == result.total_hits) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, .{
            .distributed_text_stats = current_agg_stats,
            .distributed_background_text_stats = current_bg_stats,
        }, meta);
    }
    if (result.total_hits == 0) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, .{
            .distributed_text_stats = current_agg_stats,
            .distributed_background_text_stats = current_bg_stats,
        }, meta);
    }

    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = result.total_hits;
    full_req.include_stored = true;
    full_req.count_only = false;
    var full_result = try queryProvisionedAcrossGroups(self, alloc, group_ids, full_req, table_name, consistency);
    defer full_result.deinit();
    const full_agg_stats = try collectProvisionedAggregationTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, full_agg_stats);
    const full_bg_stats = try collectProvisionedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, full_bg_stats);
    return try applyAggregationResults(alloc, full_req, full_result, .{
        .distributed_text_stats = full_agg_stats,
        .distributed_background_text_stats = full_bg_stats,
    }, meta);
}

fn tryApplyProvisionedAlgebraicDistributedAggregations(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    meta: *query_api.QueryResponseMeta,
) !bool {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return false;
    if (!canConsiderAlgebraicAggregations(req)) return false;
    const constraints = (try algebraicConstraintsForRequestAlloc(alloc, req)) orelse return false;
    defer freeAlgebraicConstraints(alloc, constraints);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    if (requests.len == 0) return false;

    const first_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
    defer alloc.free(first_path);
    var first_db = try openProvisionedQueryDbForTableWithRuntime(alloc, first_path, self.catalog, table_name, group_ids[0], self.lsmRootGeneration(group_ids[0]), self.backend_runtime);
    defer first_db.close();
    if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &first_db))) return false;
    const first_entry = if (req.index_name) |index_name|
        first_db.core.index_manager.algebraicIndex(index_name) orelse return false
    else
        first_db.core.index_manager.algebraicIndex(null) orelse return false;
    if (!first_entry.index.plannerLifecycleReady()) return false;

    var primary_count: usize = 0;
    var pipeline_count: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) pipeline_count += 1 else primary_count += 1;
    }
    if (primary_count == 0) return false;

    var primary_results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_count);
    defer if (primary_results.len > 0) alloc.free(primary_results);
    var primary_filled: usize = 0;
    errdefer {
        for (primary_results[0..primary_filled]) |*result| result.deinit(alloc);
    }
    var pipeline_requests = try alloc.alloc(db_mod.aggregations.SearchAggregationRequest, pipeline_count);
    defer if (pipeline_requests.len > 0) alloc.free(pipeline_requests);
    var pipeline_filled: usize = 0;

    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) {
            pipeline_requests[pipeline_filled] = request;
            pipeline_filled += 1;
            continue;
        }
        var request_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &first_entry.index, request, constraints, req.identity_read_generation)) orelse return false;
        defer request_plan.deinit(alloc);
        var merged = (try collectProvisionedAlgebraicDistributedPartials(self, alloc, group_ids, table_name, req, first_entry.index.name, request_plan.access_paths, request_plan.asProgram())) orelse return false;
        defer merged.deinit(alloc);
        var result = (try algebraicAggregationFromDistributedPartialsAlloc(alloc, &first_entry.index, request, constraints, merged)) orelse return false;
        var result_owned = true;
        errdefer if (result_owned) result.deinit(alloc);
        try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, &result);
        primary_results[primary_filled] = result;
        result_owned = false;
        primary_filled += 1;
    }

    var pipeline_results: []db_mod.aggregations.SearchAggregationResult = &.{};
    defer if (pipeline_results.len > 0) alloc.free(pipeline_results);
    var pipeline_items_owned = false;
    errdefer if (pipeline_items_owned) {
        for (pipeline_results) |*result| result.deinit(alloc);
    };
    if (pipeline_requests.len > 0) {
        pipeline_results = try db_mod.aggregations.computeRootPipelineAggregations(alloc, pipeline_requests, primary_results);
        pipeline_items_owned = true;
        for (pipeline_results) |*result| try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, result);
    }

    var results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_results.len + pipeline_results.len);
    errdefer alloc.free(results);
    for (primary_results, 0..) |result, i| results[i] = result;
    primary_filled = 0;
    for (pipeline_results, 0..) |result, i| results[primary_results.len + i] = result;
    pipeline_items_owned = false;

    meta.aggregation_results = results;
    return true;
}

fn collectProvisionedAlgebraicDistributedPartials(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    selected_index_name: []const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_program: algebraic_ir.TensorProgram,
) !?db_mod.algebraic.distributed.MergeSet {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    if (searchRequestHasResolvedDocFilter(req)) return null;
    const body = try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, req.index_name orelse selected_index_name, req.identity_read_generation, access_paths, &.{}, tensor_program);
    defer alloc.free(body);
    var partials = std.ArrayListUnmanaged(db_mod.algebraic.distributed.Partial).empty;
    errdefer {
        for (partials.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        partials.deinit(alloc);
    }

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, self.lsmRootGeneration(group_id), self.backend_runtime);
        defer db.close();
        if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &db))) return null;
        var parsed = try parseAlgebraicPartialsRequest(alloc, body);
        defer parsed.deinit(alloc);
        const shard_partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed);
        defer if (shard_partials.len > 0) alloc.free(shard_partials);
        for (shard_partials) |partial| try partials.append(alloc, partial);
    }

    const partial_slice = try partials.toOwnedSlice(alloc);
    defer db_mod.algebraic.distributed.freePartials(alloc, partial_slice);
    return try db_mod.algebraic.distributed.mergePartialsAlloc(alloc, partial_slice);
}

fn applyHostedProvisionedQueryAggregations(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (req.aggregations_json.len == 0) return;
    const aggregation_req = requestWithResultIdentityGeneration(req, result.*);
    if (group_ids.len == 1) {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_ids[0], routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);

        switch (route) {
            .local => {
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
                defer alloc.free(path);
                var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_ids[0], 0, self.backend_runtime);
                defer db.close();

                if (!req.count_only and result.hits.len == result.total_hits) {
                    return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, &db), meta);
                }
                if (result.total_hits == 0) {
                    return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, &db), meta);
                }

                var reads = raft_mod.FeatureDBReads.init(group_ids[0], self.requester);
                const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
                var full_req = req;
                full_req.identity_read_generation = identity_read_generation;
                full_req.offset = 0;
                full_req.limit = result.total_hits;
                full_req.include_stored = true;
                full_req.count_only = false;
                var full_result = try reads.searchWithConsistency(alloc, &db, full_req, consistency);
                defer full_result.deinit();
                return try applyAggregationResults(alloc, full_req, full_result, try aggregationContextForDb(alloc, full_req, &db), meta);
            },
            .remote => {},
        }
    }

    if (try tryApplyHostedAlgebraicDistributedAggregations(self, alloc, group_ids, table_name, aggregation_req, meta, consistency)) return;

    const current_agg_stats = try collectHostedAggregationTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits, consistency);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, current_agg_stats);
    const current_bg_stats = try collectHostedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits, consistency);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, current_bg_stats);
    if (!req.count_only and result.hits.len == result.total_hits) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, .{
            .distributed_text_stats = current_agg_stats,
            .distributed_background_text_stats = current_bg_stats,
        }, meta);
    }
    if (result.total_hits == 0) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, .{
            .distributed_text_stats = current_agg_stats,
            .distributed_background_text_stats = current_bg_stats,
        }, meta);
    }

    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = result.total_hits;
    full_req.include_stored = true;
    full_req.count_only = false;
    var full_result = try queryHostedAcrossGroups(self, alloc, group_ids, full_req, table_name, consistency);
    defer full_result.deinit();
    const full_agg_stats = try collectHostedAggregationTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits, consistency);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, full_agg_stats);
    const full_bg_stats = try collectHostedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits, consistency);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, full_bg_stats);
    return try applyAggregationResults(alloc, full_req, full_result, .{
        .distributed_text_stats = full_agg_stats,
        .distributed_background_text_stats = full_bg_stats,
    }, meta);
}

fn tryApplyHostedAlgebraicDistributedAggregations(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !bool {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return false;
    if (!canConsiderAlgebraicAggregations(req)) return false;
    const representative_group_id: ?u64 = blk: {
        for (group_ids) |group_id| {
            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return false;
            defer route.deinit(alloc);
            switch (route) {
                .local => break :blk group_id,
                .remote => {},
            }
        }
        break :blk null;
    };

    const constraints = (try algebraicConstraintsForRequestAlloc(alloc, req)) orelse return false;
    defer freeAlgebraicConstraints(alloc, constraints);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    if (requests.len == 0) return false;

    if (representative_group_id) |group_id| {
        const first_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(first_path);
        var first_db = try openProvisionedQueryDbForTableWithRuntime(alloc, first_path, self.catalog, table_name, group_id, 0, self.backend_runtime);
        defer first_db.close();
        if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &first_db))) return false;
        const first_entry = if (req.index_name) |index_name|
            first_db.core.index_manager.algebraicIndex(index_name) orelse return false
        else
            first_db.core.index_manager.algebraicIndex(null) orelse return false;
        if (!first_entry.index.plannerLifecycleReady()) return false;
        return try applyHostedAlgebraicDistributedAggregationsWithPlanner(
            self,
            alloc,
            group_ids,
            table_name,
            req,
            first_entry.index.name,
            constraints,
            requests,
            meta,
            consistency,
            &first_entry.index,
        );
    }

    var catalog_index = (try openCatalogAlgebraicPlannerIndex(alloc, self.catalog, table_name, req.index_name)) orelse return false;
    defer catalog_index.close();
    if (!catalog_index.plannerLifecycleReady()) return false;
    return try applyHostedAlgebraicDistributedAggregationsWithPlanner(
        self,
        alloc,
        group_ids,
        table_name,
        req,
        catalog_index.name,
        constraints,
        requests,
        meta,
        consistency,
        &catalog_index,
    );
}

fn applyHostedAlgebraicDistributedAggregationsWithPlanner(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    planner_index_name: []const u8,
    constraints: []const db_mod.aggregations.FixedConstraint,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
    planner_index: *db_mod.algebraic.index.Index,
) !bool {
    var primary_count: usize = 0;
    var pipeline_count: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) pipeline_count += 1 else primary_count += 1;
    }
    if (primary_count == 0) return false;

    var primary_results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_count);
    defer if (primary_results.len > 0) alloc.free(primary_results);
    var primary_filled: usize = 0;
    errdefer {
        for (primary_results[0..primary_filled]) |*result| result.deinit(alloc);
    }
    var pipeline_requests = try alloc.alloc(db_mod.aggregations.SearchAggregationRequest, pipeline_count);
    defer if (pipeline_requests.len > 0) alloc.free(pipeline_requests);
    var pipeline_filled: usize = 0;

    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) {
            pipeline_requests[pipeline_filled] = request;
            pipeline_filled += 1;
            continue;
        }
        var request_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, planner_index, request, constraints, req.identity_read_generation)) orelse return false;
        defer request_plan.deinit(alloc);
        var merged = (try collectHostedAlgebraicDistributedPartials(self, alloc, group_ids, table_name, req, planner_index_name, request_plan.access_paths, request_plan.asProgram(), consistency)) orelse return false;
        defer merged.deinit(alloc);
        var result = (try algebraicAggregationFromDistributedPartialsAlloc(alloc, planner_index, request, constraints, merged)) orelse return false;
        var result_owned = true;
        errdefer if (result_owned) result.deinit(alloc);
        try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, &result);
        primary_results[primary_filled] = result;
        result_owned = false;
        primary_filled += 1;
    }

    var pipeline_results: []db_mod.aggregations.SearchAggregationResult = &.{};
    defer if (pipeline_results.len > 0) alloc.free(pipeline_results);
    var pipeline_items_owned = false;
    errdefer if (pipeline_items_owned) {
        for (pipeline_results) |*result| result.deinit(alloc);
    };
    if (pipeline_requests.len > 0) {
        pipeline_results = try db_mod.aggregations.computeRootPipelineAggregations(alloc, pipeline_requests, primary_results);
        pipeline_items_owned = true;
        for (pipeline_results) |*result| try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, result);
    }

    var results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_results.len + pipeline_results.len);
    errdefer alloc.free(results);
    for (primary_results, 0..) |result, i| results[i] = result;
    primary_filled = 0;
    for (pipeline_results, 0..) |result, i| results[primary_results.len + i] = result;
    pipeline_items_owned = false;

    meta.aggregation_results = results;
    return true;
}

const RangeCardinalityPlan = struct {
    kind: db_mod.algebraic.index.CardinalityRangeKind,
    ranges: []db_mod.algebraic.index.CardinalityRangeRequest,
    children: []db_mod.algebraic.index.CardinalityChildRequest,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.ranges) |range| {
            alloc.free(@constCast(range.name));
            if (range.start) |value| alloc.free(@constCast(value));
            if (range.end) |value| alloc.free(@constCast(value));
        }
        if (self.ranges.len > 0) alloc.free(self.ranges);
        if (self.children.len > 0) alloc.free(self.children);
        self.* = undefined;
    }
};

const HistogramCardinalityPlan = struct {
    kind: db_mod.algebraic.index.CardinalityHistogramKind,
    interval: f64 = 0,
    date_bucket: []const u8 = "",
    children: []db_mod.algebraic.index.CardinalityChildRequest,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.date_bucket.len > 0) alloc.free(@constCast(self.date_bucket));
        if (self.children.len > 0) alloc.free(self.children);
        self.* = undefined;
    }
};

fn algebraicTermsCardinalityChildRequestsAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?[]db_mod.algebraic.index.CardinalityChildRequest {
    if (!std.mem.eql(u8, request.type, "terms") or request.field.len == 0 or request.background_query != null) return null;
    var count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        count += 1;
    }
    if (count == 0) return null;
    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, count);
    var idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[idx] = .{ .name = child.name, .field = child.field };
        idx += 1;
    }
    return children;
}

fn algebraicDistributedTensorProgramForAggregationRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    identity_read_generation: ?u64,
) !?algebraic_planner.TensorProgramQueryPlan {
    _ = identity_read_generation;

    const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
    defer if (ir_constraints.len > 0) alloc.free(ir_constraints);

    if (std.mem.eql(u8, request.type, "cardinality")) {
        return try algebraic_planner.planCardinalityPartialsTensorProgramAlloc(alloc, index, request.name, request.field, ir_constraints);
    }
    if (try algebraicTermsCardinalityChildRequestsAlloc(alloc, request)) |children| {
        defer alloc.free(children);
        return try algebraic_planner.planTermsCardinalityPartialsTensorProgramAlloc(alloc, index, request.name, request.field, children, ir_constraints);
    }
    if (try algebraicRangeCardinalityPlanAlloc(alloc, request)) |range_cardinality_value| {
        var range_cardinality = range_cardinality_value;
        defer range_cardinality.deinit(alloc);
        return try algebraic_planner.planRangeCardinalityPartialsTensorProgramAlloc(
            alloc,
            index,
            request.name,
            request.field,
            range_cardinality.kind,
            range_cardinality.ranges,
            range_cardinality.children,
            ir_constraints,
        );
    }
    if (try algebraicHistogramCardinalityPlanAlloc(alloc, request)) |histogram_cardinality_value| {
        var histogram_cardinality = histogram_cardinality_value;
        defer histogram_cardinality.deinit(alloc);
        return try algebraic_planner.planHistogramCardinalityPartialsTensorProgramAlloc(
            alloc,
            index,
            request.name,
            request.field,
            histogram_cardinality.kind,
            histogram_cardinality.interval,
            histogram_cardinality.date_bucket,
            histogram_cardinality.children,
            ir_constraints,
        );
    }
    if (try algebraicDistributedTensorProgramForRequestAlloc(alloc, index, request, constraints)) |plan| {
        return plan;
    }
    const materializations = (try algebraicDistributedMaterializationsForRequestAlloc(alloc, index, request, constraints)) orelse return null;
    defer db_mod.aggregations.freeAlgebraicDistributedMaterializations(alloc, materializations);
    return try algebraic_planner.planMaterializationPartialsTensorProgramAlloc(alloc, index, materializations);
}

fn algebraicHistogramCardinalityPlanAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?HistogramCardinalityPlan {
    if (request.field.len == 0 or request.background_query != null) return null;
    const kind: db_mod.algebraic.index.CardinalityHistogramKind = if (std.mem.eql(u8, request.type, "histogram")) blk: {
        if (request.interval <= 0) return null;
        break :blk .numeric;
    } else if (std.mem.eql(u8, request.type, "date_histogram")) blk: {
        if (db_mod.aggregations.algebraicBucketName(request) == null) return null;
        break :blk .date;
    } else return null;

    var child_count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        child_count += 1;
    }
    if (child_count == 0) return null;

    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, child_count);
    var child_idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[child_idx] = .{ .name = child.name, .field = child.field };
        child_idx += 1;
    }
    const date_bucket = if (kind == .date) try alloc.dupe(u8, db_mod.aggregations.algebraicBucketName(request).?) else "";
    errdefer if (date_bucket.len > 0) alloc.free(date_bucket);
    return .{
        .kind = kind,
        .interval = if (kind == .numeric) request.interval else 0,
        .date_bucket = date_bucket,
        .children = children,
    };
}

fn algebraicRangeCardinalityPlanAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?RangeCardinalityPlan {
    if (request.field.len == 0 or request.background_query != null or request.distance_ranges.len > 0) return null;
    const kind: db_mod.algebraic.index.CardinalityRangeKind = if (std.mem.eql(u8, request.type, "range")) blk: {
        if (request.ranges.len == 0 or request.date_ranges.len > 0) return null;
        break :blk .numeric;
    } else if (std.mem.eql(u8, request.type, "date_range")) blk: {
        if (request.date_ranges.len == 0 or request.ranges.len > 0) return null;
        break :blk .date;
    } else return null;

    var child_count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        child_count += 1;
    }
    if (child_count == 0) return null;

    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, child_count);
    errdefer if (children.len > 0) alloc.free(children);
    var child_idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[child_idx] = .{ .name = child.name, .field = child.field };
        child_idx += 1;
    }

    const range_count = if (kind == .numeric) request.ranges.len else request.date_ranges.len;
    const ranges = try alloc.alloc(db_mod.algebraic.index.CardinalityRangeRequest, range_count);
    var ranges_initialized: usize = 0;
    errdefer {
        for (ranges[0..ranges_initialized]) |range| {
            alloc.free(@constCast(range.name));
            if (range.start) |value| alloc.free(@constCast(value));
            if (range.end) |value| alloc.free(@constCast(value));
        }
        if (ranges.len > 0) alloc.free(ranges);
    }
    if (kind == .numeric) {
        for (request.ranges, 0..) |range, i| {
            const start_text = if (range.start) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
            errdefer if (start_text) |text| alloc.free(text);
            const end_text = if (range.end) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
            errdefer if (end_text) |text| alloc.free(text);
            ranges[i] = .{
                .name = try alloc.dupe(u8, range.name),
                .start = start_text,
                .end = end_text,
            };
            ranges_initialized += 1;
        }
    } else {
        for (request.date_ranges, 0..) |range, i| {
            ranges[i] = .{
                .name = try alloc.dupe(u8, range.name),
                .start = if (range.start) |value| try alloc.dupe(u8, value) else null,
                .end = if (range.end) |value| try alloc.dupe(u8, value) else null,
            };
            ranges_initialized += 1;
        }
    }
    return .{
        .kind = kind,
        .ranges = ranges,
        .children = children,
    };
}

fn algebraicDistributedMaterializationsForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
) !?[][]const u8 {
    if (std.mem.eql(u8, request.type, "stats")) {
        return try db_mod.aggregations.algebraicDistributedStatsMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type) != null) {
        return try db_mod.aggregations.algebraicDistributedMetricMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "date_histogram")) {
        return try db_mod.aggregations.algebraicDistributedDateHistogramMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "histogram")) {
        return try db_mod.aggregations.algebraicDistributedHistogramMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "range")) {
        return try db_mod.aggregations.algebraicDistributedRangeMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "date_range")) {
        return try db_mod.aggregations.algebraicDistributedDateRangeMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "terms")) {
        return try db_mod.aggregations.algebraicDistributedTermsMaterializationsAlloc(alloc, index, request, constraints);
    }
    return null;
}

fn algebraicDistributedTensorProgramForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
) !?algebraic_planner.TensorProgramQueryPlan {
    if (request.algebraic_join) |join_ref| {
        return try algebraicDistributedJoinTensorProgramForRequestAlloc(alloc, index, request, constraints, join_ref);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type)) |op| {
        return try algebraic_planner.planMetricTensorProgramAlloc(alloc, index, .{
            .kind = .metric,
            .aggregation_name = request.name,
            .constraints = constraints,
            .metric = .{ .name = request.name, .op = op, .field = request.field },
        });
    }

    if (std.mem.eql(u8, request.type, "terms")) {
        if (request.field.len == 0 or request.background_query != null) return null;
        const bucket_field = index.fieldConfig(request.field, .group) orelse {
            const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
            defer if (child_metrics.len > 0) alloc.free(child_metrics);
            const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
            defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
            if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
                return try planPathFactTermsTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
            }
            return null;
        };
        if (algebraicConstraintValueForField(index, constraints, bucket_field.name) != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = bucket_field.name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    if (std.mem.eql(u8, request.type, "date_histogram")) {
        if (request.field.len == 0) return null;
        const time_field = index.fieldConfig(request.field, .time) orelse return null;
        const bucket_name = db_mod.aggregations.algebraicBucketName(request) orelse return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .date_histogram,
            .aggregation_name = request.name,
            .time_field = time_field.name,
            .time_bucket = bucket_name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    if (std.mem.eql(u8, request.type, "date_range")) {
        if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
        defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
        if (index.fieldConfig(request.field, .time)) |time_field| {
            if (!docFactChildMetricsSupported(index, child_metrics)) return null;
            return try planDocFactDateRangeTensorProgramAlloc(alloc, index, request, time_field.name, ir_constraints, child_metrics);
        }
        if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
            return try planPathFactDateRangeTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
        }
        return null;
    }

    if (std.mem.eql(u8, request.type, "histogram") or std.mem.eql(u8, request.type, "range")) {
        if (request.field.len == 0) return null;
        if (std.mem.eql(u8, request.type, "histogram") and request.interval <= 0) return null;
        if (std.mem.eql(u8, request.type, "range") and (request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0)) return null;
        const bucket_field = index.fieldConfig(request.field, .group) orelse {
            const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
            defer if (child_metrics.len > 0) alloc.free(child_metrics);
            const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
            defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
            if (index.fieldConfig(request.field, .measure)) |measure_field| {
                if (!docFactChildMetricsSupported(index, child_metrics)) return null;
                if (std.mem.eql(u8, request.type, "histogram")) {
                    return try planDocFactHistogramTensorProgramAlloc(alloc, index, request, measure_field.name, ir_constraints, child_metrics);
                }
                if (std.mem.eql(u8, request.type, "range")) {
                    return try planDocFactRangeTensorProgramAlloc(alloc, index, request, measure_field.name, ir_constraints, child_metrics);
                }
            }
            if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
                if (std.mem.eql(u8, request.type, "histogram")) {
                    return try planPathFactHistogramTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
                }
                if (std.mem.eql(u8, request.type, "range")) {
                    return try planPathFactRangeTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
                }
            }
            return null;
        };
        const bucket_kind = db_mod.algebraic.value.kindFromFieldType(bucket_field.type);
        if (bucket_kind != .number and bucket_kind != .integer) return null;
        if (algebraicConstraintValueForField(index, constraints, bucket_field.name) != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = bucket_field.name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    return null;
}

fn algebraicDistributedJoinTensorProgramForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    join_ref: algebraic_ir.JoinRef,
) !?algebraic_planner.TensorProgramQueryPlan {
    if (db_mod.algebraic.algebra.Op.parse(request.type)) |op| {
        return try algebraic_planner.planMetricTensorProgramAlloc(alloc, index, .{
            .kind = .metric,
            .aggregation_name = request.name,
            .constraints = constraints,
            .metric = .{ .name = request.name, .op = op, .field = request.field },
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "terms")) {
        if (request.field.len == 0 or request.background_query != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = request.field,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "date_histogram")) {
        if (request.field.len == 0) return null;
        const bucket_name = db_mod.aggregations.algebraicBucketName(request) orelse return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .date_histogram,
            .aggregation_name = request.name,
            .time_field = request.field,
            .time_bucket = bucket_name,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "histogram")) {
        if (request.field.len == 0 or request.interval <= 0) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .histogram,
            .aggregation_name = request.name,
            .bucket_field = request.field,
            .bucket_interval = request.interval,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "range")) {
        if (request.field.len == 0 or request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0) return null;
        return try planDerivedJoinRangeTensorProgramAlloc(alloc, index, request, constraints, join_ref, .numeric);
    }

    if (std.mem.eql(u8, request.type, "date_range")) {
        if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
        return try planDerivedJoinRangeTensorProgramAlloc(alloc, index, request, constraints, join_ref, .date);
    }

    return null;
}

fn planDerivedJoinRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    join_ref: algebraic_ir.JoinRef,
    kind: db_mod.algebraic.index.DerivedJoinRangeKind,
) !?algebraic_planner.TensorProgramQueryPlan {
    const range_field = algebraicDistributedDerivedJoinRangeField(index, request.field, kind) orelse return null;
    const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    if (kind == .numeric) {
        const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
        defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
        return try algebraic_planner.planDerivedJoinRangeTensorProgramAlloc(alloc, index, request.name, join_ref, range_field, ranges, child_metrics, constraints);
    }
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planDerivedJoinRangeTensorProgramAlloc(alloc, index, request.name, join_ref, range_field, ranges, child_metrics, constraints);
}

fn algebraicDistributedDerivedJoinRangeField(
    index: *const db_mod.algebraic.index.Index,
    field_name: []const u8,
    kind: db_mod.algebraic.index.DerivedJoinRangeKind,
) ?algebraic_planner.DerivedJoinRangeField {
    var found: ?algebraic_planner.DerivedJoinRangeField = null;
    switch (kind) {
        .numeric => {
            if (index.fieldConfig(field_name, .measure)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) found = .{ .name = field.name, .role = .measure, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
        .date => {
            if (index.fieldConfig(field_name, .time)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) found = .{ .name = field.name, .role = .time, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
    }
    return found;
}

fn planPathFactTermsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planPathFactTermsTensorProgramAlloc(alloc, index, request.name, bucket_path, constraints, child_metrics);
}

fn algebraicDistributedChildMetricsAlloc(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
) ![]algebraic_ir.Metric {
    var count: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) continue;
        if (std.mem.eql(u8, request.type, "stats")) {
            count += 4;
            continue;
        }
        if (db_mod.algebraic.algebra.Op.parse(request.type) == null) return error.UnsupportedQueryRequest;
        count += 1;
    }
    const out = try alloc.alloc(algebraic_ir.Metric, count);
    errdefer if (out.len > 0) alloc.free(out);
    var filled: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) continue;
        if (std.mem.eql(u8, request.type, "stats")) {
            out[filled] = .{ .name = request.name, .op = .avg, .field = request.field };
            out[filled + 1] = .{ .name = request.name, .op = .min, .field = request.field };
            out[filled + 2] = .{ .name = request.name, .op = .max, .field = request.field };
            out[filled + 3] = .{ .name = request.name, .op = .sumsquares, .field = request.field };
            filled += 4;
            continue;
        }
        const op = db_mod.algebraic.algebra.Op.parse(request.type) orelse return error.UnsupportedQueryRequest;
        out[filled] = .{ .name = request.name, .op = op, .field = request.field };
        filled += 1;
    }
    return out;
}

fn docFactChildMetricsSupported(index: *const db_mod.algebraic.index.Index, metrics: []const algebraic_ir.Metric) bool {
    for (metrics) |metric| {
        if (metric.op == .count) continue;
        if (metric.field.len == 0) return false;
        if (index.fieldConfig(metric.field, .measure) == null) return false;
    }
    return true;
}

fn pathFactChildMetricsSupported(metrics: []const algebraic_ir.Metric) bool {
    for (metrics) |metric| {
        if (metric.op == .count) continue;
        if (jsonPointerField(metric.field) == null) return false;
    }
    return true;
}

fn jsonPointerField(field: []const u8) ?[]const u8 {
    if (std.mem.startsWith(u8, field, "/")) return field;
    return null;
}

fn planDocFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    measure_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planDocFactHistogramTensorProgramAlloc(alloc, index, request.name, measure_field, request.interval, constraints, child_metrics);
}

fn planDocFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    measure_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
    defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
    return try algebraic_planner.planDocFactRangeTensorProgramAlloc(alloc, index, request.name, measure_field, ranges, constraints, child_metrics);
}

fn planDocFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    time_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planDocFactDateRangeTensorProgramAlloc(alloc, index, request.name, time_field, ranges, constraints, child_metrics);
}

fn planPathFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planPathFactHistogramTensorProgramAlloc(alloc, index, request.name, bucket_path, request.interval, constraints, child_metrics);
}

fn planPathFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
    defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
    return try algebraic_planner.planPathFactRangeTensorProgramAlloc(alloc, index, request.name, bucket_path, ranges, constraints, child_metrics);
}

fn planPathFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planPathFactDateRangeTensorProgramAlloc(alloc, index, request.name, bucket_path, ranges, constraints, child_metrics);
}

fn algebraicNumericRangeBoundsAlloc(
    alloc: std.mem.Allocator,
    ranges: []const db_mod.aggregations.NumericRangeRequest,
) ![]algebraic_planner.RangeBound {
    const out = try alloc.alloc(algebraic_planner.RangeBound, ranges.len);
    @memset(out, .{});
    errdefer freeAlgebraicOwnedRangeBounds(alloc, out);
    for (ranges, 0..) |range, i| {
        out[i] = .{
            .start = if (range.start) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null,
            .end = if (range.end) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null,
        };
    }
    return out;
}

fn algebraicDateRangeBoundsAlloc(
    alloc: std.mem.Allocator,
    ranges: []const db_mod.aggregations.DateRangeRequest,
) ![]algebraic_planner.RangeBound {
    const out = try alloc.alloc(algebraic_planner.RangeBound, ranges.len);
    errdefer alloc.free(out);
    for (ranges, 0..) |range, i| {
        out[i] = .{ .start = range.start, .end = range.end };
    }
    return out;
}

fn freeAlgebraicOwnedRangeBounds(alloc: std.mem.Allocator, ranges: []algebraic_planner.RangeBound) void {
    for (ranges) |range| {
        if (range.start) |value| alloc.free(@constCast(value));
        if (range.end) |value| alloc.free(@constCast(value));
    }
    if (ranges.len > 0) alloc.free(ranges);
}

fn algebraicIrConstraintsFromFixedAlloc(
    alloc: std.mem.Allocator,
    constraints: []const db_mod.aggregations.FixedConstraint,
) ![]algebraic_ir.Constraint {
    const out = try alloc.alloc(algebraic_ir.Constraint, constraints.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (constraints, 0..) |constraint, i| {
        out[i] = .{ .field = constraint.field, .value = constraint.value };
    }
    return out;
}

fn algebraicConstraintValueForField(
    index: *const db_mod.algebraic.index.Index,
    constraints: []const db_mod.aggregations.FixedConstraint,
    field_name: []const u8,
) ?[]const u8 {
    _ = index;
    for (constraints) |constraint| {
        if (std.mem.eql(u8, constraint.field, field_name)) return constraint.value;
    }
    return null;
}

fn algebraicAggregationFromDistributedPartialsAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    merged: db_mod.algebraic.distributed.MergeSet,
) !?db_mod.aggregations.SearchAggregationResult {
    if (std.mem.eql(u8, request.type, "stats")) {
        return try db_mod.aggregations.algebraicStatsAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type) != null) {
        return try db_mod.aggregations.algebraicMetricAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "cardinality")) {
        return try db_mod.aggregations.algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, request, merged);
    }
    if (std.mem.eql(u8, request.type, "date_histogram")) {
        return try db_mod.aggregations.algebraicDateHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "histogram")) {
        return try db_mod.aggregations.algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "range")) {
        return try db_mod.aggregations.algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "date_range")) {
        return try db_mod.aggregations.algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "terms")) {
        return try db_mod.aggregations.algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    return null;
}

fn openCatalogAlgebraicPlannerIndex(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    index_name: ?[]const u8,
) !?db_mod.algebraic.index.Index {
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse return null;
    defer alloc.free(indexes_json);
    if (indexes_json.len == 0) return null;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch return null;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return null,
    };

    if (index_name) |name| {
        const value = root.get(name) orelse return null;
        if (!catalogIndexValueIsAlgebraic(value)) return null;
        const config_json = try catalogAlgebraicConfigJsonAlloc(alloc, value);
        defer alloc.free(config_json);
        return try db_mod.algebraic.index.Index.open(alloc, name, config_json);
    }

    var it = root.iterator();
    while (it.next()) |entry| {
        if (!catalogIndexValueIsAlgebraic(entry.value_ptr.*)) continue;
        const config_json = try catalogAlgebraicConfigJsonAlloc(alloc, entry.value_ptr.*);
        defer alloc.free(config_json);
        return try db_mod.algebraic.index.Index.open(alloc, entry.key_ptr.*, config_json);
    }
    return null;
}

fn catalogIndexValueIsAlgebraic(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, "algebraic");
}

fn catalogAlgebraicConfigJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) ![]u8 {
    if (value != .object) return error.InvalidTableIndexMetadata;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "type") or
            std.mem.eql(u8, entry.key_ptr.*, "name") or
            std.mem.eql(u8, entry.key_ptr.*, "description") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments") or
            std.mem.eql(u8, entry.key_ptr.*, "derive_from_schema"))
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

fn collectHostedAlgebraicDistributedPartials(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    selected_index_name: []const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_program: algebraic_ir.TensorProgram,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.algebraic.distributed.MergeSet {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    if (searchRequestHasResolvedDocFilter(req)) return null;
    const body = try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, req.index_name orelse selected_index_name, req.identity_read_generation, access_paths, &.{}, tensor_program);
    defer alloc.free(body);
    var partials = std.ArrayListUnmanaged(db_mod.algebraic.distributed.Partial).empty;
    errdefer {
        for (partials.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        partials.deinit(alloc);
    }

    for (group_ids) |group_id| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);
        const shard_partials = switch (route) {
            .local => blk: {
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
                defer alloc.free(path);
                var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, 0, self.backend_runtime);
                defer db.close();
                if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &db))) return null;
                var parsed = try parseAlgebraicPartialsRequest(alloc, body);
                defer parsed.deinit(alloc);
                break :blk try collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed);
            },
            .remote => |remote| blk: {
                var response = (algebraicPartialsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch return null) orelse return null;
                defer response.deinit(alloc);
                break :blk try parseAlgebraicPartialsResponse(alloc, response.json);
            },
        };
        defer if (shard_partials.len > 0) alloc.free(shard_partials);
        for (shard_partials) |partial| try partials.append(alloc, partial);
    }

    const partial_slice = try partials.toOwnedSlice(alloc);
    defer db_mod.algebraic.distributed.freePartials(alloc, partial_slice);
    return try db_mod.algebraic.distributed.mergePartialsAlloc(alloc, partial_slice);
}

fn queryNeedsDistributedTextStats(req: db_mod.types.SearchRequest) bool {
    if (req.distributed_text_stats.len > 0) return false;
    if (req.full_text != null) return true;
    if (db_query_search.isTextQuery(req.query) and !db_query_search.isDefaultMatchAll(req.query)) return true;
    return req.full_text_queries.len > 0;
}

fn encodeQueryTextStatsRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ![]u8 {
    const encoded_query = try encodeQueryRequest(alloc, req);
    defer alloc.free(encoded_query);
    return try std.fmt.allocPrint(alloc, "{{\"query_request\":{s}}}", .{encoded_query});
}

fn encodeExplicitTextStatsRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedTextStatsFieldRequest,
    identity_read_generation: ?u64,
) ![]u8 {
    return try encodeExplicitTextStatsRequestForSearchRequest(alloc, items, .{ .identity_read_generation = identity_read_generation });
}

fn encodeExplicitTextStatsRequestForSearchRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedTextStatsFieldRequest,
    req: db_mod.types.SearchRequest,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var top_first = true;
    if (req.identity_read_generation) |generation| try appendJsonFieldU64(alloc, &out, &top_first, "_identity_read_generation", generation);
    try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &top_first, req);
    try appendJsonFieldName(alloc, &out, &top_first, "fields");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        if (item.index_name) |index_name| {
            try appendJsonFieldString(alloc, &out, &first, "index_name", index_name);
        }
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldName(alloc, &out, &first, "terms");
        try out.append(alloc, '[');
        for (item.terms, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, term);
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn encodeBackgroundTextStatsRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedBackgroundTextStatsFieldRequest,
    identity_read_generation: ?u64,
) ![]u8 {
    return try encodeBackgroundTextStatsRequestForSearchRequest(alloc, items, .{ .identity_read_generation = identity_read_generation });
}

fn encodeBackgroundTextStatsRequestForSearchRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedBackgroundTextStatsFieldRequest,
    req: db_mod.types.SearchRequest,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var top_first = true;
    if (req.identity_read_generation) |generation| try appendJsonFieldU64(alloc, &out, &top_first, "_identity_read_generation", generation);
    try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &top_first, req);
    try appendJsonFieldName(alloc, &out, &top_first, "background_fields");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "aggregation_name", item.aggregation_name);
        if (item.index_name) |index_name| {
            try appendJsonFieldString(alloc, &out, &first, "index_name", index_name);
        }
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldName(alloc, &out, &first, "terms");
        try out.append(alloc, '[');
        for (item.terms, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, term);
        }
        try out.append(alloc, ']');
        try appendJsonFieldName(alloc, &out, &first, "background_query");
        try appendBackgroundQueryJson(alloc, &out, item.background_query);
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn encodeAlgebraicPartialsRequest(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
) ![]u8 {
    return try encodeAlgebraicPartialsRequestWithProgram(alloc, index_name, access_paths, tensor_exprs, null);
}

fn encodeAlgebraicPartialsRequestWithProgram(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
    tensor_program: ?algebraic_ir.TensorProgram,
) ![]u8 {
    return try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, index_name, null, access_paths, tensor_exprs, tensor_program);
}

fn encodeAlgebraicPartialsRequestWithProgramAtGeneration(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    identity_read_generation: ?u64,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
    tensor_program: ?algebraic_ir.TensorProgram,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    if (index_name) |name| try appendJsonFieldString(alloc, &out, &first, "index_name", name);
    if (identity_read_generation) |generation| try appendJsonFieldU64(alloc, &out, &first, "_identity_read_generation", generation);
    if (access_paths.len > 0) {
        try appendJsonFieldName(alloc, &out, &first, "tensor_access_paths");
        try out.append(alloc, '[');
        for (access_paths, 0..) |path, i| {
            if (i > 0) try out.append(alloc, ',');
            const encoded = try query_contract.encodeAlgebraicTensorAccessPathEnvelopeAlloc(alloc, path);
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
        try out.append(alloc, ']');
    }
    if (tensor_exprs.len > 0) {
        try appendJsonFieldName(alloc, &out, &first, "tensor_exprs");
        try out.append(alloc, '[');
        for (tensor_exprs, 0..) |expr, i| {
            if (i > 0) try out.append(alloc, ',');
            const encoded = try query_contract.encodeAlgebraicTensorExprEnvelopeAlloc(alloc, expr);
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
        try out.append(alloc, ']');
    }
    if (tensor_program) |program| {
        try appendJsonFieldName(alloc, &out, &first, "tensor_program");
        const encoded = try query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, program);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn encodeAlgebraicExpressionPartialsRequest(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
) ![]u8 {
    return try encodeAlgebraicPartialsRequest(alloc, index_name, access_paths, tensor_exprs);
}

fn parseTextStatsRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
) !ParsedTextStatsRequest {
    var parsed = try std.json.parseFromSlice(TextStatsRequestInput, alloc, body, .{});
    defer parsed.deinit();
    if (parsed.value.query_request) |query_value| {
        if (parsed.value._resolved_doc_filter != null) return error.InvalidQueryRequest;
        const encoded_query = try std.json.Stringify.valueAlloc(alloc, query_value, .{});
        defer alloc.free(encoded_query);
        return .{ .query_request = try query_api.parseQueryRequest(alloc, null, table_name, encoded_query) };
    }
    if (parsed.value.fields) |fields_value| {
        var resolved_doc_filter = if (parsed.value._resolved_doc_filter) |filter_value|
            try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, filter_value)
        else
            null;
        errdefer if (resolved_doc_filter) |*filter| filter.deinit(alloc);
        const items = try alloc.alloc(OwnedTextStatsFieldRequest, fields_value.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            if (items.len > 0) alloc.free(items);
        }
        for (fields_value, 0..) |field_value, i| {
            const terms = try alloc.alloc([]const u8, field_value.terms.len);
            var initialized_terms: usize = 0;
            errdefer {
                for (terms[0..initialized_terms]) |term| alloc.free(term);
                if (terms.len > 0) alloc.free(terms);
            }
            for (field_value.terms, 0..) |term_value, term_idx| {
                terms[term_idx] = try alloc.dupe(u8, term_value);
                initialized_terms += 1;
            }
            items[i] = .{
                .index_name = if (field_value.index_name) |index_name_value| try alloc.dupe(u8, index_name_value) else null,
                .field = try alloc.dupe(u8, field_value.field),
                .terms = terms,
            };
            initialized += 1;
        }
        return .{ .explicit_fields = .{
            .identity_read_generation = parsed.value._identity_read_generation,
            .resolved_doc_filter = resolved_doc_filter,
            .items = items,
        } };
    }
    if (parsed.value.background_fields) |fields_value| {
        var resolved_doc_filter = if (parsed.value._resolved_doc_filter) |filter_value|
            try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, filter_value)
        else
            null;
        errdefer if (resolved_doc_filter) |*filter| filter.deinit(alloc);
        const items = try alloc.alloc(OwnedBackgroundTextStatsFieldRequest, fields_value.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            if (items.len > 0) alloc.free(items);
        }
        for (fields_value, 0..) |field_value, i| {
            const terms = try alloc.alloc([]const u8, field_value.terms.len);
            var initialized_terms: usize = 0;
            errdefer {
                for (terms[0..initialized_terms]) |term| alloc.free(term);
                if (terms.len > 0) alloc.free(terms);
            }
            for (field_value.terms, 0..) |term_value, term_idx| {
                terms[term_idx] = try alloc.dupe(u8, term_value);
                initialized_terms += 1;
            }
            items[i] = .{
                .aggregation_name = try alloc.dupe(u8, field_value.aggregation_name),
                .index_name = if (field_value.index_name) |index_name_value| try alloc.dupe(u8, index_name_value) else null,
                .field = try alloc.dupe(u8, field_value.field),
                .terms = terms,
                .background_query = try parseBackgroundQueryRequestAlloc(alloc, field_value.background_query),
            };
            initialized += 1;
        }
        return .{ .background_fields = .{
            .identity_read_generation = parsed.value._identity_read_generation,
            .resolved_doc_filter = resolved_doc_filter,
            .items = items,
        } };
    }
    return error.InvalidQueryRequest;
}

fn parseAlgebraicPartialsRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !ParsedAlgebraicPartialsRequest {
    var parsed = try std.json.parseFromSlice(AlgebraicPartialsRequestInput, alloc, body, .{});
    defer parsed.deinit();
    const exprs_value = parsed.value.tensor_exprs orelse &.{};
    const has_program = parsed.value.tensor_program != null;
    const has_legacy_request = parsed.value.cardinality != null or
        parsed.value.terms_cardinality != null or
        parsed.value.range_cardinality != null or
        parsed.value.histogram_cardinality != null;
    if (has_legacy_request) return error.InvalidQueryRequest;
    if (exprs_value.len == 0 and !has_program) return error.InvalidQueryRequest;
    if (has_program and exprs_value.len > 0) return error.InvalidQueryRequest;
    const paths_value = parsed.value.tensor_access_paths orelse return error.InvalidQueryRequest;
    const expected_proof_count = if (exprs_value.len > 0) exprs_value.len else paths_value.len;
    if (paths_value.len != expected_proof_count) return error.InvalidQueryRequest;
    const tensor_access_paths = blk: {
        const paths = try alloc.alloc(OwnedAlgebraicTensorAccessPath, paths_value.len);
        var paths_initialized: usize = 0;
        errdefer {
            for (paths[0..paths_initialized]) |*item| item.deinit(alloc);
            if (paths.len > 0) alloc.free(paths);
        }
        for (paths_value, 0..) |path_value, i| {
            paths[i] = try parseAlgebraicTensorAccessPathAlloc(alloc, path_value);
            paths_initialized += 1;
        }
        break :blk paths;
    };
    errdefer {
        for (tensor_access_paths) |*item| item.deinit(alloc);
        if (tensor_access_paths.len > 0) alloc.free(tensor_access_paths);
    }
    const tensor_exprs = blk: {
        const exprs = try alloc.alloc(OwnedAlgebraicTensorExpr, exprs_value.len);
        var exprs_initialized: usize = 0;
        errdefer {
            for (exprs[0..exprs_initialized]) |*item| item.deinit(alloc);
            if (exprs.len > 0) alloc.free(exprs);
        }
        for (exprs_value, 0..) |expr_value, i| {
            exprs[i] = try query_contract.parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, expr_value);
            exprs_initialized += 1;
        }
        break :blk exprs;
    };
    errdefer {
        for (tensor_exprs) |*item| item.deinit(alloc);
        if (tensor_exprs.len > 0) alloc.free(tensor_exprs);
    }
    var tensor_program: ?OwnedAlgebraicTensorProgram = null;
    errdefer if (tensor_program) |*program| program.deinit(alloc);
    if (parsed.value.tensor_program) |program_value| {
        tensor_program = try query_contract.parseAlgebraicTensorProgramEnvelopeInputAlloc(alloc, program_value);
        try validateAlgebraicProgramPartialsProof(alloc, tensor_access_paths, &tensor_program.?);
    }
    return .{
        .index_name = if (parsed.value.index_name) |name| try alloc.dupe(u8, name) else null,
        .identity_read_generation = parsed.value._identity_read_generation,
        .tensor_access_paths = tensor_access_paths,
        .tensor_exprs = tensor_exprs,
        .tensor_program = tensor_program,
    };
}

fn parseAlgebraicTensorAccessPathAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorAccessPathInput,
) !OwnedAlgebraicTensorAccessPath {
    return try query_contract.parseAlgebraicTensorAccessPathEnvelopeInputAlloc(alloc, input);
}

fn encodeTextStatsResponse(alloc: std.mem.Allocator, stats: []const distributed_stats_mod.TextFieldStats) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"fields\":[");
    for (stats, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldU32(alloc, &out, &first, "global_doc_count", item.global_doc_count);
        try appendJsonFieldU64(alloc, &out, &first, "global_total_field_len", item.global_total_field_len);
        try appendJsonFieldName(alloc, &out, &first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, &out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, &out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn parseTextStatsResponse(alloc: std.mem.Allocator, body: []const u8) ![]const distributed_stats_mod.TextFieldStats {
    var parsed = try std.json.parseFromSlice(TextStatsResponseInput, alloc, body, .{});
    defer parsed.deinit();
    const fields_value = parsed.value.fields;
    const stats = try alloc.alloc(distributed_stats_mod.TextFieldStats, fields_value.len);
    var initialized: usize = 0;
    errdefer {
        for (stats[0..initialized]) |*item| item.deinit(alloc);
        if (stats.len > 0) alloc.free(stats);
    }
    for (fields_value, 0..) |entry, i| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.term_doc_freqs.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (entry.term_doc_freqs, 0..) |term_entry, term_idx| {
            term_doc_freqs[term_idx] = .{
                .term = try alloc.dupe(u8, term_entry.term),
                .doc_freq = term_entry.doc_freq,
            };
            initialized_terms += 1;
        }
        stats[i] = .{
            .field = try alloc.dupe(u8, entry.field),
            .global_doc_count = entry.global_doc_count,
            .global_total_field_len = entry.global_total_field_len,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return stats;
}

fn encodeBackgroundTextStatsResponse(
    alloc: std.mem.Allocator,
    stats: []const db_mod.aggregations.DistributedBackgroundTextStats,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"background_fields\":[");
    for (stats, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "aggregation_name", item.aggregation_name);
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldU32(alloc, &out, &first, "background_doc_count", item.background_doc_count);
        try appendJsonFieldName(alloc, &out, &first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, &out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, &out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn encodeAlgebraicPartialsResponse(
    alloc: std.mem.Allocator,
    partials: []const db_mod.algebraic.distributed.Partial,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"partials\":[");
    for (partials, 0..) |partial, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "canonical_axis", partial.canonical_axis);
        try appendJsonFieldString(alloc, &out, &first, "metric", partial.metric);
        try appendJsonFieldString(alloc, &out, &first, "law", @tagName(partial.law_id));
        try appendJsonFieldString(alloc, &out, &first, "value", partial.value);
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn parseAlgebraicPartialsResponse(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]db_mod.algebraic.distributed.Partial {
    var parsed = try std.json.parseFromSlice(AlgebraicPartialsResponseInput, alloc, body, .{});
    defer parsed.deinit();
    const partials = try alloc.alloc(db_mod.algebraic.distributed.Partial, parsed.value.partials.len);
    var initialized: usize = 0;
    errdefer {
        for (partials[0..initialized]) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        if (partials.len > 0) alloc.free(partials);
    }
    for (parsed.value.partials, 0..) |partial, i| {
        const law_id = std.meta.stringToEnum(db_mod.algebraic.law.Id, partial.law) orelse return error.InvalidQueryRequest;
        partials[i] = .{
            .canonical_axis = try alloc.dupe(u8, partial.canonical_axis),
            .metric = try alloc.dupe(u8, partial.metric),
            .law_id = law_id,
            .value = try alloc.dupe(u8, partial.value),
        };
        initialized += 1;
    }
    return partials;
}

fn parseBackgroundTextStatsResponse(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    var parsed = try std.json.parseFromSlice(BackgroundTextStatsResponseInput, alloc, body, .{});
    defer parsed.deinit();
    const fields_value = parsed.value.background_fields;
    const stats = try alloc.alloc(db_mod.aggregations.DistributedBackgroundTextStats, fields_value.len);
    var initialized: usize = 0;
    errdefer {
        for (stats[0..initialized]) |*item| item.deinit(alloc);
        if (stats.len > 0) alloc.free(stats);
    }
    for (fields_value, 0..) |entry, i| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.term_doc_freqs.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (entry.term_doc_freqs, 0..) |term_entry, term_idx| {
            term_doc_freqs[term_idx] = .{
                .term = try alloc.dupe(u8, term_entry.term),
                .doc_freq = term_entry.doc_freq,
            };
            initialized_terms += 1;
        }
        stats[i] = .{
            .aggregation_name = try alloc.dupe(u8, entry.aggregation_name),
            .field = try alloc.dupe(u8, entry.field),
            .background_doc_count = entry.background_doc_count,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return stats;
}

pub fn parseTextStatsHttpResponse(
    alloc: std.mem.Allocator,
    request_body: []const u8,
    response_body: []const u8,
) !ParsedTextStatsHttpResponse {
    var parsed = try std.json.parseFromSlice(TextStatsRequestInput, alloc, request_body, .{});
    defer parsed.deinit();

    if (parsed.value.background_fields != null) {
        return .{
            .background_fields = .{
                .background_fields = try parseBackgroundTextStatsResponse(alloc, response_body),
            },
        };
    }

    return .{
        .fields = .{
            .fields = try parseTextStatsResponse(alloc, response_body),
        },
    };
}

fn appendBackgroundQueryJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.aggregations.BackgroundQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":{");
            try appendJsonString(alloc, out, match.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, "}}");
        },
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":{");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, "}}");
        },
    }
}

fn parseBackgroundQueryRequestAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !db_mod.aggregations.BackgroundQuery {
    if (value == .object) {
        if (value.object.get("match_all") != null) return .{ .match_all = {} };
        if (value.object.get("match")) |match| {
            if (match == .object and match.object.count() == 1) {
                var it = match.object.iterator();
                const entry = it.next() orelse return error.InvalidQueryRequest;
                if (entry.value_ptr.* != .string) return error.InvalidQueryRequest;
                return .{ .match = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .text = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
        if (value.object.get("term")) |term| {
            if (term == .object and term.object.count() == 1) {
                var it = term.object.iterator();
                const entry = it.next() orelse return error.InvalidQueryRequest;
                if (entry.value_ptr.* != .string) return error.InvalidQueryRequest;
                return .{ .term = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .term = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
    }
    return error.InvalidQueryRequest;
}

fn mergeDistributedTextStats(
    alloc: std.mem.Allocator,
    groups: []const []const distributed_stats_mod.TextFieldStats,
) ![]const distributed_stats_mod.TextFieldStats {
    var fields = std.StringHashMapUnmanaged(struct {
        doc_count: u32 = 0,
        total_field_len: u64 = 0,
        terms: std.StringHashMapUnmanaged(u32) = .{},
    }){};
    defer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.key_ptr.*);
        }
        fields.deinit(alloc);
    }

    for (groups) |items| {
        for (items) |item| {
            const gop = try fields.getOrPut(alloc, item.field);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, item.field);
                gop.value_ptr.* = .{};
            }
            gop.value_ptr.doc_count +|= item.global_doc_count;
            gop.value_ptr.total_field_len +|= item.global_total_field_len;
            for (item.term_doc_freqs) |term| {
                const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term.term);
                if (!term_gop.found_existing) {
                    term_gop.key_ptr.* = try alloc.dupe(u8, term.term);
                    term_gop.value_ptr.* = 0;
                }
                term_gop.value_ptr.* +|= term.doc_freq;
            }
        }
    }

    const out = try alloc.alloc(distributed_stats_mod.TextFieldStats, fields.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = fields.iterator();
    while (it.next()) |entry| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.value_ptr.terms.count());
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        var term_it = entry.value_ptr.terms.iterator();
        while (term_it.next()) |term_entry| {
            term_doc_freqs[initialized_terms] = .{
                .term = try alloc.dupe(u8, term_entry.key_ptr.*),
                .doc_freq = term_entry.value_ptr.*,
            };
            initialized_terms += 1;
        }
        out[initialized] = .{
            .field = try alloc.dupe(u8, entry.key_ptr.*),
            .global_doc_count = entry.value_ptr.doc_count,
            .global_total_field_len = entry.value_ptr.total_field_len,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return out;
}

fn mergeDistributedBackgroundTextStats(
    alloc: std.mem.Allocator,
    groups: []const []const db_mod.aggregations.DistributedBackgroundTextStats,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    var fields = std.StringHashMapUnmanaged(struct {
        aggregation_name: []const u8,
        field: []const u8,
        background_doc_count: u32 = 0,
        terms: std.StringHashMapUnmanaged(u32) = .{},
    }){};
    defer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.value_ptr.aggregation_name);
            alloc.free(entry.value_ptr.field);
            alloc.free(entry.key_ptr.*);
        }
        fields.deinit(alloc);
    }

    for (groups) |items| {
        for (items) |item| {
            const map_key = try textStatsTupleKeyAlloc(alloc, &.{ item.aggregation_name, item.field });
            defer alloc.free(map_key);
            const gop = try fields.getOrPut(alloc, map_key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, map_key);
                gop.value_ptr.* = .{
                    .aggregation_name = try alloc.dupe(u8, item.aggregation_name),
                    .field = try alloc.dupe(u8, item.field),
                };
            }
            gop.value_ptr.background_doc_count +|= item.background_doc_count;
            for (item.term_doc_freqs) |term| {
                const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term.term);
                if (!term_gop.found_existing) {
                    term_gop.key_ptr.* = try alloc.dupe(u8, term.term);
                    term_gop.value_ptr.* = 0;
                }
                term_gop.value_ptr.* +|= term.doc_freq;
            }
        }
    }

    const out = try alloc.alloc(db_mod.aggregations.DistributedBackgroundTextStats, fields.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = fields.iterator();
    while (it.next()) |entry| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.value_ptr.terms.count());
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        var term_it = entry.value_ptr.terms.iterator();
        while (term_it.next()) |term_entry| {
            term_doc_freqs[initialized_terms] = .{
                .term = try alloc.dupe(u8, term_entry.key_ptr.*),
                .doc_freq = term_entry.value_ptr.*,
            };
            initialized_terms += 1;
        }
        out[initialized] = .{
            .aggregation_name = try alloc.dupe(u8, entry.value_ptr.aggregation_name),
            .field = try alloc.dupe(u8, entry.value_ptr.field),
            .background_doc_count = entry.value_ptr.background_doc_count,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return out;
}

fn textStatsTupleKeyAlloc(alloc: std.mem.Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

fn collectSignificantTermsFieldRequests(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) ![]OwnedTextStatsFieldRequest {
    var grouped = std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)){};
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.deinit(alloc);
            alloc.free(entry.key_ptr.*);
        }
        grouped.deinit(alloc);
    }

    try collectSignificantTermsFieldRequestsRecursive(alloc, &grouped, requests, hits);
    if (grouped.count() == 0) return &.{};

    const out = try alloc.alloc(OwnedTextStatsFieldRequest, grouped.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    var it = grouped.iterator();
    while (it.next()) |entry| {
        const terms = try alloc.alloc([]const u8, entry.value_ptr.count());
        var term_index: usize = 0;
        var term_it = entry.value_ptr.keyIterator();
        while (term_it.next()) |term| {
            terms[term_index] = try alloc.dupe(u8, term.*);
            term_index += 1;
        }
        out[initialized] = .{
            .field = try alloc.dupe(u8, entry.key_ptr.*),
            .terms = terms,
        };
        initialized += 1;
    }
    return out;
}

fn collectSignificantTermsBackgroundFieldRequests(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) ![]OwnedBackgroundTextStatsFieldRequest {
    var out = std.ArrayListUnmanaged(OwnedBackgroundTextStatsFieldRequest).empty;
    errdefer {
        for (out.items) |*item| item.deinit(alloc);
        out.deinit(alloc);
    }
    try collectSignificantTermsBackgroundFieldRequestsRecursive(alloc, &out, requests, hits);
    return try out.toOwnedSlice(alloc);
}

fn collectSignificantTermsBackgroundFieldRequestsRecursive(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(OwnedBackgroundTextStatsFieldRequest),
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) !void {
    for (requests) |request| {
        if (std.mem.eql(u8, request.type, "significant_terms") and request.background_query != null) {
            var seen_terms = std.StringHashMapUnmanaged(void){};
            defer {
                var term_it = seen_terms.keyIterator();
                while (term_it.next()) |term| alloc.free(term.*);
                seen_terms.deinit(alloc);
            }
            try collectSignificantTermsFromHits(alloc, hits, request.field, &seen_terms);
            if (seen_terms.count() > 0) {
                const terms = try alloc.alloc([]const u8, seen_terms.count());
                var term_index: usize = 0;
                var term_it = seen_terms.keyIterator();
                while (term_it.next()) |term| {
                    terms[term_index] = try alloc.dupe(u8, term.*);
                    term_index += 1;
                }
                try out.append(alloc, .{
                    .aggregation_name = try alloc.dupe(u8, request.name),
                    .field = try alloc.dupe(u8, request.field),
                    .terms = terms,
                    .background_query = try cloneBackgroundQuery(alloc, request.background_query.?),
                });
            }
        }
        try collectSignificantTermsBackgroundFieldRequestsRecursive(alloc, out, request.aggregations, hits);
    }
}

fn cloneBackgroundQuery(
    alloc: std.mem.Allocator,
    query: db_mod.aggregations.BackgroundQuery,
) !db_mod.aggregations.BackgroundQuery {
    return switch (query) {
        .match_all => .{ .match_all = {} },
        .match => |match| .{ .match = .{
            .field = try alloc.dupe(u8, match.field),
            .text = try alloc.dupe(u8, match.text),
        } },
        .term => |term| .{ .term = .{
            .field = try alloc.dupe(u8, term.field),
            .term = try alloc.dupe(u8, term.term),
        } },
    };
}

fn collectSignificantTermsFieldRequestsRecursive(
    alloc: std.mem.Allocator,
    grouped: *std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)),
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) !void {
    for (requests) |request| {
        if (std.mem.eql(u8, request.type, "significant_terms") and request.background_query == null) {
            const gop = try grouped.getOrPut(alloc, request.field);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, request.field);
                gop.value_ptr.* = .{};
            }
            try collectSignificantTermsFromHits(alloc, hits, request.field, gop.value_ptr);
        }
        try collectSignificantTermsFieldRequestsRecursive(alloc, grouped, request.aggregations, hits);
    }
}

fn collectSignificantTermsFromHits(
    alloc: std.mem.Allocator,
    hits: []const db_mod.types.SearchHit,
    field: []const u8,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    for (hits) |hit| try collectSignificantTermsFromStoredAlloc(alloc, hit.stored_data orelse continue, field, seen_terms);
}

fn collectSignificantTermsFromStoredAlloc(
    alloc: std.mem.Allocator,
    stored: []const u8,
    field: []const u8,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    var parsed = (try parseJsonPathValueAlloc(alloc, stored, field)) orelse return;
    defer parsed.deinit();
    try collectSignificantTermsFromValue(alloc, parsed.value, seen_terms);
}

fn collectSignificantTermsFromValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    switch (value) {
        .array => |arr| for (arr.items) |item| try collectSignificantTermsFromValue(alloc, item, seen_terms),
        .string => {
            const tokens = try @import("../search/analysis.zig").default_analyzer.analyze(alloc, value.string);
            defer @import("../search/analysis.zig").Analyzer.freeTokens(alloc, tokens);
            for (tokens) |tok| {
                const entry = try seen_terms.getOrPut(alloc, tok.term);
                if (entry.found_existing) continue;
                entry.key_ptr.* = try alloc.dupe(u8, tok.term);
            }
        },
        else => {},
    }
}

fn extractJsonValueAtPath(value: std.json.Value, path: []const u8) ?std.json.Value {
    return json_helpers.extractJsonPathValue(value, path);
}

fn collectTextStatsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedTextStatsRequest,
) ![]const distributed_stats_mod.TextFieldStats {
    return switch (request) {
        .query_request => |owned_query| try db.collectSearchRequestTextStats(alloc, owned_query.req),
        .explicit_fields => |parsed| blk: {
            const generation = try db.currentIdentityReadGenerationForRequest(parsed.identity_read_generation);
            if (parsed.resolved_doc_filter) |filter| {
                if (parsed.identity_read_generation == null or generation != filter.context.identity_read_generation) return error.UnsupportedQueryRequest;
                if (!filter.context.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            }
            const explicit = try alloc.alloc(db_query_search.ExplicitTextStatRequest, parsed.items.len);
            defer alloc.free(explicit);
            for (parsed.items, 0..) |item, i| {
                explicit[i] = .{
                    .index_name = item.index_name,
                    .field = item.field,
                    .terms = item.terms,
                    .resolved_doc_filter = if (parsed.resolved_doc_filter) |filter| filter.resolved_doc_filter else null,
                };
            }
            break :blk try db.collectExplicitTextStats(alloc, explicit);
        },
        .background_fields => return error.InvalidQueryRequest,
    };
}

fn collectBackgroundTextStatsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedTextStatsRequest,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    return switch (request) {
        .background_fields => |parsed| blk: {
            const generation = try db.currentIdentityReadGenerationForRequest(parsed.identity_read_generation);
            if (parsed.resolved_doc_filter) |filter| {
                if (parsed.identity_read_generation == null or generation != filter.context.identity_read_generation) return error.UnsupportedQueryRequest;
                if (!filter.context.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            }
            const explicit = try alloc.alloc(db_query_search.ExplicitBackgroundTextStatRequest, parsed.items.len);
            defer alloc.free(explicit);
            for (parsed.items, 0..) |item, i| {
                explicit[i] = .{
                    .aggregation_name = item.aggregation_name,
                    .index_name = item.index_name,
                    .field = item.field,
                    .terms = item.terms,
                    .background_query = item.background_query,
                    .resolved_doc_filter = if (parsed.resolved_doc_filter) |filter| filter.resolved_doc_filter else null,
                };
            }
            break :blk try db.collectExplicitBackgroundTextStats(alloc, explicit);
        },
        else => return error.InvalidQueryRequest,
    };
}

fn collectAlgebraicPartialsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedAlgebraicPartialsRequest,
) ![]db_mod.algebraic.distributed.Partial {
    const generation = try db.currentIdentityReadGenerationForRequest(request.identity_read_generation);
    const entry = if (request.index_name) |index_name|
        db.core.index_manager.algebraicIndex(index_name) orelse return error.UnsupportedQueryRequest
    else
        db.core.index_manager.algebraicIndex(null) orelse return error.UnsupportedQueryRequest;
    if (entry.index.hasErrors() or !entry.index.plannerLifecycleReady()) return error.UnsupportedQueryRequest;
    if (!(try algebraicIndexFreshEnoughForName(alloc, request.index_name, db))) return error.UnsupportedQueryRequest;
    if (request.tensor_program) |*program| {
        const access_path_values = try algebraicTensorAccessPathValuesAlloc(alloc, request.tensor_access_paths);
        defer if (access_path_values.len > 0) alloc.free(access_path_values);
        var view = try program.asProgramAlloc(alloc);
        defer view.deinit(alloc);
        if (try entry.index.scanDistributedPartialsForTensorProgramAtGeneration(db.core.store, access_path_values, view.program, generation)) |partials| {
            return partials;
        }
        const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, &entry.index, request.tensor_access_paths, program);
        defer if (exprs.len > 0) alloc.free(exprs);
        return try entry.index.scanDistributedPartialsForExpressions(db.core.store, exprs);
    }
    try validateAlgebraicPartialsAccessPaths(alloc, request.tensor_access_paths, request.tensor_exprs);
    const exprs = try parsedAlgebraicTensorExpressionsAlloc(alloc, request.tensor_exprs);
    defer if (exprs.len > 0) alloc.free(exprs);
    return try entry.index.scanDistributedPartialsForExpressions(db.core.store, exprs);
}

fn parsedAlgebraicTensorExpressionsAlloc(
    alloc: std.mem.Allocator,
    items: []const OwnedAlgebraicTensorExpr,
) ![]algebraic_ir.TensorExpr {
    const exprs = try alloc.alloc(algebraic_ir.TensorExpr, items.len);
    errdefer if (exprs.len > 0) alloc.free(exprs);
    for (items, 0..) |*item, i| exprs[i] = item.asExpr();
    return exprs;
}

fn validateAlgebraicPartialsAccessPaths(
    alloc: std.mem.Allocator,
    access_paths: anytype,
    tensor_exprs: anytype,
) !void {
    if (access_paths.len == 0 or access_paths.len != tensor_exprs.len) return error.InvalidQueryRequest;
    for (access_paths, tensor_exprs) |access_path, tensor_expr| {
        const expr = algebraicTensorExprValue(tensor_expr);
        var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)) orelse return error.InvalidQueryRequest;
        defer plan.deinit(alloc);
        if (!algebraicTensorAccessPathMatches(plan.access_path, access_path)) return error.InvalidQueryRequest;
    }
}

fn validateAlgebraicProgramPartialsAccessPaths(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
    program: *const OwnedAlgebraicTensorProgram,
) !void {
    try validateAlgebraicProgramPartialsProof(alloc, access_paths, program);
    const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, null, access_paths, program);
    defer if (exprs.len > 0) alloc.free(exprs);
}

fn validateAlgebraicProgramPartialsProof(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
    program: *const OwnedAlgebraicTensorProgram,
) !void {
    const path_values = try algebraicTensorAccessPathValuesAlloc(alloc, access_paths);
    defer if (path_values.len > 0) alloc.free(path_values);
    var view = try program.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const proof = try algebraic_ir.tensorProgramProof(alloc, path_values, view.program);
    if (!proof.safe()) return error.InvalidQueryRequest;
}

fn algebraicTensorProgramOutputExpressionsForIndexAlloc(
    alloc: std.mem.Allocator,
    index: ?*const db_mod.algebraic.index.Index,
    access_paths: []OwnedAlgebraicTensorAccessPath,
    program: *const OwnedAlgebraicTensorProgram,
) ![]algebraic_ir.TensorExpr {
    const path_values = try algebraicTensorAccessPathValuesAlloc(alloc, access_paths);
    defer if (path_values.len > 0) alloc.free(path_values);
    var view = try program.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const proof = try algebraic_ir.tensorProgramProof(alloc, path_values, view.program);
    if (!proof.safe()) return error.InvalidQueryRequest;
    const single_output = [_]algebraic_ir.TensorProgramRef{view.program.output};
    const refs = if (view.program.outputs.len > 0) view.program.outputs else single_output[0..];
    const exprs = try alloc.alloc(algebraic_ir.TensorExpr, refs.len);
    errdefer if (exprs.len > 0) alloc.free(exprs);
    for (refs, 0..) |ref, i| {
        const step_idx = switch (ref) {
            .step => |idx| idx,
            .input => return error.InvalidQueryRequest,
        };
        if (step_idx >= view.program.steps.len) return error.InvalidQueryRequest;
        const expr = view.program.steps[step_idx].expr;
        exprs[i] = try algebraicTensorProgramOutputExpressionForStep(alloc, index, path_values, expr);
    }
    return exprs;
}

fn algebraicTensorProgramOutputExpressionForStep(
    alloc: std.mem.Allocator,
    index: ?*const db_mod.algebraic.index.Index,
    path_values: []const algebraic_ir.PhysicalAccessPath,
    expr: algebraic_ir.TensorExpr,
) !algebraic_ir.TensorExpr {
    if (expr.layout == .materialized_expr) {
        var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)) orelse return error.InvalidQueryRequest;
        defer plan.deinit(alloc);
        if (!algebraicTensorAccessPathListHas(path_values, plan.access_path)) return error.InvalidQueryRequest;
        return expr;
    }
    if (expr.layout == .materialized_tensor) {
        const concrete_index = index orelse return error.InvalidQueryRequest;
        const materialization = expr.semantic_id orelse expr.owner orelse return error.InvalidQueryRequest;
        const mat = findAlgebraicMaterialization(concrete_index, materialization) orelse return error.InvalidQueryRequest;
        const access_path = algebraic_planner.materializationAccessPath(mat) orelse return error.InvalidQueryRequest;
        if (!algebraicTensorAccessPathListHas(path_values, access_path)) return error.InvalidQueryRequest;
        const output_expr = algebraic_planner.materializationTensorExpression(mat) orelse return error.InvalidQueryRequest;
        if (expr.law_id != null and output_expr.law_id != expr.law_id) return error.InvalidQueryRequest;
        return output_expr;
    }
    return error.InvalidQueryRequest;
}

fn algebraicTensorAccessPathListHas(paths: []const algebraic_ir.PhysicalAccessPath, expected: algebraic_ir.PhysicalAccessPath) bool {
    for (paths) |path| {
        if (algebraicTensorAccessPathMatches(expected, path)) return true;
    }
    return false;
}

fn algebraicTensorAccessPathValuesAlloc(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
) ![]algebraic_ir.PhysicalAccessPath {
    const out = try alloc.alloc(algebraic_ir.PhysicalAccessPath, access_paths.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (access_paths, 0..) |path, i| out[i] = path.asAccessPath();
    return out;
}

fn findAlgebraicMaterialization(
    index: *const db_mod.algebraic.index.Index,
    name: []const u8,
) ?db_mod.algebraic.index.MaterializationConfig {
    for (index.config().materializations) |mat| {
        if (std.mem.eql(u8, mat.name, name)) return mat;
    }
    return null;
}

fn algebraicTensorAccessPathMatches(
    expected: algebraic_ir.PhysicalAccessPath,
    actual: anytype,
) bool {
    const actual_path = algebraicTensorAccessPathValue(actual);
    return std.mem.eql(u8, expected.owner, actual_path.owner) and
        expected.layout == actual_path.layout and
        optionalDictionaryEqual(expected.dictionary, actual_path.dictionary) and
        tensorFragmentSlicesEqual(expected.fragments, actual_path.fragments) and
        tensorDimensionSlicesEqual(expected.output_dims, actual_path.output_dims) and
        lawIdSlicesEqual(expected.law_ids, actual_path.law_ids);
}

fn algebraicTensorAccessPathValue(actual: anytype) algebraic_ir.PhysicalAccessPath {
    if (@TypeOf(actual) == algebraic_ir.PhysicalAccessPath) return actual;
    return actual.asAccessPath();
}

fn optionalDictionaryEqual(
    left: ?db_mod.algebraic.lexical.DictionaryIdentity,
    right: ?db_mod.algebraic.lexical.DictionaryIdentity,
) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return left.?.eql(right.?);
}

fn algebraicTensorExprValue(actual: anytype) algebraic_ir.TensorExpr {
    if (@TypeOf(actual) == algebraic_ir.TensorExpr) return actual;
    return actual.asExpr();
}

fn algebraicTensorExprMatches(expected: algebraic_ir.TensorExpr, actual: algebraic_ir.TensorExpr) bool {
    return expected.fragment == actual.fragment and
        tensorDimensionSlicesEqual(expected.input_dims, actual.input_dims) and
        tensorDimensionSlicesEqual(expected.output_dims, actual.output_dims) and
        optionalStringEqual(expected.semantic_id, actual.semantic_id) and
        optionalStringEqual(expected.owner, actual.owner) and
        expected.layout == actual.layout and
        expected.law_id == actual.law_id;
}

fn optionalStringEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return std.mem.eql(u8, left.?, right.?);
}

fn tensorFragmentSlicesEqual(left: []const algebraic_ir.TensorFragment, right: []const algebraic_ir.TensorFragment) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn tensorDimensionSlicesEqual(left: []const algebraic_ir.Dimension, right: []const algebraic_ir.Dimension) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn lawIdSlicesEqual(left: []const algebraic_law.Id, right: []const algebraic_law.Id) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn collectBoundLocalTextStats(
    self: *BoundTableReadSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    if (!std.mem.eql(u8, self.table_name, table_name)) return null;
    var parsed = try parseTextStatsRequest(alloc, table_name, body);
    defer parsed.deinit(alloc);
    return switch (parsed) {
        .background_fields => blk: {
            const stats = try collectBackgroundTextStatsFromDbForRequest(alloc, self.db, parsed);
            defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, stats);
            break :blk .{ .json = try encodeBackgroundTextStatsResponse(alloc, stats) };
        },
        else => blk: {
            const stats = try collectTextStatsFromDbForRequest(alloc, self.db, parsed);
            defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
            break :blk .{ .json = try encodeTextStatsResponse(alloc, stats) };
        },
    };
}

fn collectBoundLocalAlgebraicPartials(
    self: *BoundTableReadSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    if (!std.mem.eql(u8, self.table_name, table_name)) return null;
    var parsed = try parseAlgebraicPartialsRequest(alloc, body);
    defer parsed.deinit(alloc);
    const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, self.db, parsed);
    defer db_mod.algebraic.distributed.freePartials(alloc, partials);
    return .{ .json = try encodeAlgebraicPartialsResponse(alloc, partials) };
}

fn collectProvisionedHostedLocalTextStats(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var parsed = try parseTextStatsRequest(alloc, table_name, body);
    defer parsed.deinit(alloc);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;
        return switch (parsed) {
            .background_fields => blk: {
                const stats = try collectBackgroundTextStatsFromDbForRequest(alloc, db, parsed);
                defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, stats);
                break :blk .{ .json = try encodeBackgroundTextStatsResponse(alloc, stats) };
            },
            else => blk: {
                const stats = try collectTextStatsFromDbForRequest(alloc, db, parsed);
                defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
                break :blk .{ .json = try encodeTextStatsResponse(alloc, stats) };
            },
        };
    } else {
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
        defer db.close();
        return switch (parsed) {
            .background_fields => blk: {
                const stats = try collectBackgroundTextStatsFromDbForRequest(alloc, &db, parsed);
                defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, stats);
                break :blk .{ .json = try encodeBackgroundTextStatsResponse(alloc, stats) };
            },
            else => blk: {
                const stats = try collectTextStatsFromDbForRequest(alloc, &db, parsed);
                defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
                break :blk .{ .json = try encodeTextStatsResponse(alloc, stats) };
            },
        };
    }
}

fn collectProvisionedHostedLocalAlgebraicPartials(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var parsed = try parseAlgebraicPartialsRequest(alloc, body);
    defer parsed.deinit(alloc);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, db_lease.db, parsed);
        defer db_mod.algebraic.distributed.freePartials(alloc, partials);
        return .{ .json = try encodeAlgebraicPartialsResponse(alloc, partials) };
    } else {
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
        defer db.close();
        const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed);
        defer db_mod.algebraic.distributed.freePartials(alloc, partials);
        return .{ .json = try encodeAlgebraicPartialsResponse(alloc, partials) };
    }
}

fn collectProvisionedSearchRequestTextStats(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
) ![]const distributed_stats_mod.TextFieldStats {
    if (!queryNeedsDistributedTextStats(req) or group_ids.len <= 1) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const body = try encodeQueryTextStatsRequest(alloc, req);
    defer alloc.free(body);

    const plan = planFanout(.text_stats, self.io_impl, group_ids.len);
    recordFanoutPlan(.text_stats, plan);
    if (plan.parallel) {
        return try collectProvisionedSearchRequestTextStatsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, body);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.text_stats);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }

    for (group_ids, 0..) |group_id, i| {
        var response = (try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }

    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectHostedSearchRequestTextStats(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]const distributed_stats_mod.TextFieldStats {
    if (!queryNeedsDistributedTextStats(req) or group_ids.len <= 1) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const body = try encodeQueryTextStatsRequest(alloc, req);
    defer alloc.free(body);

    const plan = planFanout(.text_stats, self.io_impl, group_ids.len);
    recordFanoutPlan(.text_stats, plan);
    if (plan.parallel) {
        return try collectHostedSearchRequestTextStatsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, body, consistency);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.text_stats);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }

    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        var response = switch (route) {
            .local => (try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, 0, self.backend_runtime, table_name, body)) orelse return error.TableNotFound,
            .remote => |remote| (try textStatsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body)) orelse return error.TableNotFound,
        };
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }

    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectProvisionedAggregationTextStats(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
) ![]const distributed_stats_mod.TextFieldStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeExplicitTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var response = (try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectProvisionedAggregationBackgroundTextStats(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsBackgroundFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeBackgroundTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const db_mod.aggregations.DistributedBackgroundTextStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var response = (try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.lsmRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        shard_stats[i] = try parseBackgroundTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedBackgroundTextStats(alloc, shard_stats[0..initialized]);
}

fn collectHostedAggregationTextStats(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
    consistency: raft_mod.ReadConsistency,
) ![]const distributed_stats_mod.TextFieldStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeExplicitTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        var response = switch (route) {
            .local => (try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, 0, self.backend_runtime, table_name, body)) orelse return error.TableNotFound,
            .remote => |remote| (try textStatsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body)) orelse return error.TableNotFound,
        };
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectHostedAggregationBackgroundTextStats(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
    consistency: raft_mod.ReadConsistency,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsBackgroundFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeBackgroundTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const db_mod.aggregations.DistributedBackgroundTextStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        var response = switch (route) {
            .local => (try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, 0, self.backend_runtime, table_name, body)) orelse return error.TableNotFound,
            .remote => |remote| (try textStatsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body)) orelse return error.TableNotFound,
        };
        defer response.deinit(alloc);
        shard_stats[i] = try parseBackgroundTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedBackgroundTextStats(alloc, shard_stats[0..initialized]);
}

fn applyQueryPostProcessing(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
) !void {
    if (req.reranker == null or result.hits.len == 0) return;
    try applyReranker(alloc, req, result, meta, local_termite_provider, secret_store);
}

fn applyReranker(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    secret_store: ?*common_secrets.FileStore,
) !void {
    const cfg = req.reranker orelse return;
    if (req.reranker_query_text.len == 0) return error.UnsupportedQueryRequest;

    const doc_template = if (cfg.template.len > 0)
        try alloc.dupe(u8, cfg.template)
    else
        try std.fmt.allocPrint(alloc, "{{{{{s}}}}}", .{cfg.field});
    defer alloc.free(doc_template);

    const rerank_count: usize = if (cfg.top_n) |top_n|
        @min(result.hits.len, top_n)
    else
        result.hits.len;

    const documents = try alloc.alloc([]const u8, rerank_count);
    defer alloc.free(documents);
    var initialized_docs: usize = 0;
    defer {
        for (documents[0..initialized_docs]) |document| alloc.free(document);
    }

    for (result.hits[0..rerank_count], 0..) |hit, i| {
        documents[i] = try renderRerankerDocument(alloc, doc_template, hit);
        initialized_docs += 1;
    }

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var http = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer http.deinit();

    const rerank_start_ns = platform_time.monotonicNs();
    const scores = reranking_runtime.rerankDocumentsWithOptions(
        alloc,
        &http,
        cfg,
        .{ .local_termite_provider = local_termite_provider, .secret_store = secret_store },
        req.reranker_query_text,
        documents,
    ) catch |err| switch (err) {
        error.InvalidRerankerConfig, error.UnsupportedRerankerProvider => return error.InvalidQueryRequest,
        else => return err,
    };
    defer alloc.free(scores);
    if (scores.len != rerank_count) return error.InvalidRerankerResponse;

    for (result.hits[0..rerank_count], 0..) |*hit, i| {
        hit.score = scores[i];
    }
    std.sort.pdq(db_mod.types.SearchHit, result.hits[0..rerank_count], {}, struct {
        fn lessThan(_: void, a: db_mod.types.SearchHit, b: db_mod.types.SearchHit) bool {
            const a_score = a.score orelse 0;
            const b_score = b.score orelse 0;
            if (a_score != b_score) return a_score > b_score;
            return std.mem.order(u8, a.id, b.id) == .lt;
        }
    }.lessThan);

    if (cfg.top_n) |top_n| {
        try truncateSearchHits(alloc, result, @min(top_n, result.hits.len));
        result.total_hits = @min(result.total_hits, top_n);
    }

    meta.reranker = .{
        .model = cfg.model,
        .documents_reranked = @intCast(scores.len),
        .duration_ms = @intCast(@divTrunc(platform_time.monotonicNs() - rerank_start_ns, std.time.ns_per_ms)),
    };
}

fn renderRerankerDocument(
    alloc: std.mem.Allocator,
    doc_template: []const u8,
    hit: db_mod.types.SearchHit,
) ![]const u8 {
    const raw = hit.stored_data orelse return try alloc.dupe(u8, "");
    return template_mod.renderDocument(alloc, doc_template, raw) catch try alloc.dupe(u8, "");
}

fn truncateSearchHits(
    alloc: std.mem.Allocator,
    result: *db_mod.types.SearchResult,
    keep_len: usize,
) !void {
    if (keep_len >= result.hits.len) return;
    const old_hits = result.hits;
    var kept = try alloc.alloc(db_mod.types.SearchHit, keep_len);
    for (old_hits[0..keep_len], 0..) |hit, i| {
        kept[i] = hit;
    }
    for (old_hits[keep_len..]) |*hit| hit.deinit(alloc);
    alloc.free(old_hits);
    result.hits = kept;
}

fn lookupRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
) !?LookupResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const fields = try encodeLookupFields(alloc, opts);
    defer if (fields) |value| alloc.free(value);
    var result = try client.fetchGroupLookup(base_uri, group_id, table_name, key, fields);
    defer result.deinit(alloc);
    return .{
        .json = try alloc.dupe(u8, result.body),
        .version = if (result.version) |version| try std.fmt.parseUnsigned(u64, version, 10) else 0,
    };
}

fn scanRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
) !?ScanResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try encodeScanRequest(alloc, from_key, to_key, opts);
    defer alloc.free(body);
    var result = try client.fetchGroupScan(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .ndjson = try alloc.dupe(u8, result.body) };
}

fn queryRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
) !db_mod.types.SearchResult {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    if (try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, req)) |body| {
        defer alloc.free(body);
        var result = try client.fetchGroupVectorWorker(base_uri, group_id, table_name, body);
        defer result.deinit(alloc);
        var parsed = try parseRemoteSearchResult(alloc, result.body);
        parsed.identity_read_generation = req.identity_read_generation;
        return parsed;
    }
    const body = try encodeQueryRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupQuery(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    var parsed = try parseRemoteSearchResult(alloc, result.body);
    parsed.identity_read_generation = req.identity_read_generation;
    return parsed;
}

fn preflightRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    max_work: u32,
) !db_mod.RuntimePreflightSummary {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    const body = try encodeQueryRequest(alloc, req);
    defer alloc.free(body);
    var summary = try client.fetchGroupQueryPreflight(base_uri, group_id, table_name, body, max_work);
    summary.remote_shard_count = summary.shard_count;
    return summary;
}

fn textStatsRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupTextStats(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

fn algebraicPartialsRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupAlgebraicPartials(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

fn joinPartitionRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinPartition(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

fn joinRowsRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinRows(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

fn joinUnmatchedRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinUnmatched(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

fn joinFinalizeRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinFinalize(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

fn joinJobStateRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinJobState(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

fn graphExpandRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
) !distributed_graph.GraphExpandResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try distributed_graph.encodeGraphExpandRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupGraphExpand(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return try distributed_graph.parseGraphExpandResponse(alloc, result.body);
}

fn graphHydrateRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphHydrateRequest,
) !distributed_graph.GraphHydrateResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try distributed_graph.encodeGraphHydrateRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupGraphHydrate(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return try distributed_graph.parseGraphHydrateResponse(alloc, result.body);
}

fn graphEdgesRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
) !distributed_graph.GraphEdgesResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try distributed_graph.encodeGraphEdgesRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupGraphEdges(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return try distributed_graph.parseGraphEdgesResponse(alloc, result.body);
}

fn encodeLookupFields(alloc: std.mem.Allocator, opts: db_mod.types.LookupOptions) !?[]u8 {
    if (opts.include_all_fields or opts.fields.len == 0) return null;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (opts.fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, field);
    }
    return try out.toOwnedSlice(alloc);
}

fn encodeScanRequest(
    alloc: std.mem.Allocator,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    if (from_key.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "from", from_key);
    }
    if (to_key.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "to", to_key);
    }
    if (opts.limit > 0) {
        try appendJsonFieldU32(alloc, &out, &first, "limit", opts.limit);
    }
    if (opts.fields.len > 0 and !opts.include_all_fields) {
        try appendJsonFieldNames(alloc, &out, &first, "fields", opts.fields);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn encodeQueryRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ![]u8 {
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;

    if (req.fields.len > 0 and !req.include_all_fields) {
        try appendJsonFieldNames(alloc, &out, &first, "fields", req.fields);
    }
    if (req.limit != 10) {
        try appendJsonFieldU32(alloc, &out, &first, "limit", req.limit);
    }
    if (req.offset != 0) {
        try appendJsonFieldU32(alloc, &out, &first, "offset", req.offset);
    }
    if (req.count_only) {
        try appendJsonFieldBool(alloc, &out, &first, "count", true);
    }
    if (req.profile) {
        try appendJsonFieldBool(alloc, &out, &first, "profile", true);
    }
    if (req.filter_prefix.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "filter_prefix", req.filter_prefix);
    }
    if (req.distance_over) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_over", value);
    }
    if (req.distance_under) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_under", value);
    }
    if (req.merge_config) |merge_config| {
        try appendMergeConfigField(alloc, &out, &first, merge_config);
    }
    if (req.pruner) |pruner| {
        try appendPrunerField(alloc, &out, &first, pruner);
    }
    if (req.distributed_text_stats.len > 0) {
        try appendDistributedTextStatsField(alloc, &out, &first, req.distributed_text_stats);
    }
    if (req.identity_read_generation) |generation| {
        try appendJsonFieldU64(alloc, &out, &first, "_identity_read_generation", generation);
    }
    if (req.resolved_doc_filter != null) {
        try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &first, req);
    }
    const native_doc_id_constraints = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest(req);
    if (native_doc_id_constraints.hasConstraints()) {
        try appendNativeDocIdConstraintsField(alloc, &out, &first, native_doc_id_constraints);
    }
    if (req.filter_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_filter_query_json", req.filter_query_json);
    }
    if (req.exclusion_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_exclusion_query_json", req.exclusion_query_json);
    }
    if (req.graph_queries.len > 0) {
        try appendGraphQueriesField(alloc, &out, &first, req.graph_queries);
    }
    if (req.expand_strategy) |expand_strategy| {
        try appendJsonFieldString(alloc, &out, &first, "expand_strategy", switch (expand_strategy) {
            .@"union" => "union",
            .intersection => "intersection",
        });
    }
    if (req.dense_queries.len > 0 or req.sparse_queries.len > 0) {
        try appendEmbeddingsField(alloc, &out, &first, req.dense_queries, req.sparse_queries);
    }
    if (req.full_text) |full_text| {
        try appendTextQueryField(alloc, &out, &first, "full_text_search", full_text);
    } else {
        try appendQueryField(alloc, &out, &first, req.query, req.limit);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendNativeDocIdConstraintsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    constraints: query_contract.NativeDocIdConstraintEnvelope,
) !void {
    const encoded = try query_contract.encodeNativeDocIdConstraintEnvelopeAlloc(alloc, constraints);
    defer alloc.free(encoded);
    try appendJsonFieldName(alloc, out, first, "native_doc_id_constraints");
    try out.appendSlice(alloc, encoded);
}

fn appendDistributedTextStatsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    items: []const distributed_stats_mod.TextFieldStats,
) !void {
    try appendJsonFieldName(alloc, out, first, "_distributed_text_stats");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var field_first = true;
        try appendJsonFieldString(alloc, out, &field_first, "field", item.field);
        try appendJsonFieldU32(alloc, out, &field_first, "global_doc_count", item.global_doc_count);
        try appendJsonFieldU64(alloc, out, &field_first, "global_total_field_len", item.global_total_field_len);
        try appendJsonFieldName(alloc, out, &field_first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.append(alloc, ']');
}

fn appendJsonFieldU64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    var buf: [32]u8 = undefined;
    const rendered = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try out.appendSlice(alloc, rendered);
}

fn appendMergeConfigField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    merge_config: db_mod.types.MergeConfig,
) !void {
    try appendJsonFieldName(alloc, out, first, "merge_config");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendJsonFieldString(alloc, out, &merge_first, "strategy", switch (merge_config.strategy) {
        .rrf => "rrf",
        .rsf => "rsf",
    });
    if (merge_config.rank_constant != 60.0) {
        try appendJsonFieldF64(alloc, out, &merge_first, "rank_constant", merge_config.rank_constant);
    }
    if (merge_config.window_size != 0) {
        try appendJsonFieldU32(alloc, out, &merge_first, "window_size", merge_config.window_size);
    }
    if (merge_config.weights.len > 0) {
        try appendJsonFieldName(alloc, out, &merge_first, "weights");
        try out.append(alloc, '{');
        for (merge_config.weights, 0..) |weight, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, weight.name);
            try out.append(alloc, ':');
            var weight_buf: [32]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&weight_buf, "{d}", .{weight.weight});
            try out.appendSlice(alloc, rendered);
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
}

fn appendPrunerField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    pruner: @import("../search/fusion.zig").Pruner,
) !void {
    try appendJsonFieldName(alloc, out, first, "pruner");
    try out.append(alloc, '{');
    var pruner_first = true;
    if (pruner.min_score_ratio > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_score_ratio", pruner.min_score_ratio);
    }
    if (pruner.max_score_gap_percent > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "max_score_gap_percent", pruner.max_score_gap_percent);
    }
    if (pruner.min_absolute_score > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_absolute_score", pruner.min_absolute_score);
    }
    if (pruner.require_multi_index) {
        try appendJsonFieldBool(alloc, out, &pruner_first, "require_multi_index", true);
    }
    if (pruner.std_dev_threshold > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "std_dev_threshold", pruner.std_dev_threshold);
    }
    try out.append(alloc, '}');
}

fn appendGraphQueriesField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    graph_queries: []const db_mod.types.NamedGraphQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, "graph_searches");
    try out.append(alloc, '{');
    for (graph_queries, 0..) |graph_query, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, graph_query.name);
        try out.append(alloc, ':');
        try appendGraphQueryValue(alloc, out, graph_query.query);
    }
    try out.append(alloc, '}');
}

fn appendGraphQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: graph_query_mod.GraphQuery,
) !void {
    try out.append(alloc, '{');
    var first = true;
    try appendJsonFieldString(alloc, out, &first, "type", switch (query.query_type) {
        .traverse => "traverse",
        .neighbors => "neighbors",
        .shortest_path => "shortest_path",
        .k_shortest_paths => "k_shortest_paths",
        .pattern => "pattern",
    });
    try appendJsonFieldString(alloc, out, &first, "index_name", query.index_name);
    try appendGraphNodeSelectorField(alloc, out, &first, "start_nodes", query.start_nodes);
    if (query.target_nodes) |target_nodes| {
        try appendGraphNodeSelectorField(alloc, out, &first, "target_nodes", target_nodes);
    }
    try appendGraphQueryParamsField(alloc, out, &first, query.params, query.k);
    try out.append(alloc, '}');
}

fn appendGraphNodeSelectorField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    selector: graph_query_mod.NodeSelector,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '{');
    var selector_first = true;
    switch (selector) {
        .keys => |keys| {
            try appendJsonFieldName(alloc, out, &selector_first, "keys");
            try out.append(alloc, '[');
            for (keys, 0..) |key, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, key);
            }
            try out.append(alloc, ']');
        },
        .result_ref => |result_ref| {
            try appendJsonFieldString(alloc, out, &selector_first, "result_ref", result_ref.ref);
            if (result_ref.limit > 0) try appendJsonFieldU32(alloc, out, &selector_first, "limit", result_ref.limit);
        },
    }
    try out.append(alloc, '}');
}

fn appendGraphQueryParamsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    params: graph_query_mod.QueryParams,
    k: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, "params");
    try out.append(alloc, '{');
    var params_first = true;
    if (params.edge_types.len > 0) try appendJsonFieldNames(alloc, out, &params_first, "edge_types", params.edge_types);
    if (params.direction != .out) try appendJsonFieldString(alloc, out, &params_first, "direction", switch (params.direction) {
        .out => "out",
        .in => "in",
        .both => "both",
    });
    if (params.max_depth != 3) try appendJsonFieldU32(alloc, out, &params_first, "max_depth", params.max_depth);
    if (params.min_weight != 0) try appendJsonFieldF64(alloc, out, &params_first, "min_weight", params.min_weight);
    if (params.max_weight != 0) try appendJsonFieldF64(alloc, out, &params_first, "max_weight", params.max_weight);
    if (params.max_results != 100) try appendJsonFieldU32(alloc, out, &params_first, "max_results", params.max_results);
    if (!params.deduplicate) try appendJsonFieldBool(alloc, out, &params_first, "deduplicate_nodes", false);
    if (params.include_paths) try appendJsonFieldBool(alloc, out, &params_first, "include_paths", true);
    if (params.weight_mode != .min_hops) try appendJsonFieldString(alloc, out, &params_first, "weight_mode", switch (params.weight_mode) {
        .min_hops => "min_hops",
        .min_weight => "min_weight",
        .max_weight => "max_weight",
    });
    if (k > 1) try appendJsonFieldU32(alloc, out, &params_first, "k", k);
    try out.append(alloc, '}');
}

fn appendEmbeddingsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    dense_queries: []const db_mod.types.NamedDenseQuery,
    sparse_queries: []const db_mod.types.NamedSparseQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, "embeddings");
    try out.append(alloc, '{');
    var entry_index: usize = 0;
    for (dense_queries) |dense_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, dense_query.index_name);
        try out.appendSlice(alloc, ":[");
        for (dense_query.query.vector, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.append(alloc, ']');
        entry_index += 1;
    }
    for (sparse_queries) |sparse_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, sparse_query.index_name);
        try out.appendSlice(alloc, ":{\"indices\":[");
        for (sparse_query.query.indices, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "],\"values\":[");
        for (sparse_query.query.values, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "]}");
        entry_index += 1;
    }
    try out.append(alloc, '}');
}

fn appendQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    query: db_mod.types.Query,
    default_k: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, "full_text_search");
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, '}');
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, match.field);
            try out.append(alloc, '}');
        },
        .dense_knn => |dense| {
            try out.appendSlice(alloc, "{\"dense_knn\":{\"vector\":[");
            for (dense.vector, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (dense.k == 0) default_k else dense.k});
            try out.appendSlice(alloc, "}}");
        },
        .sparse_knn => |sparse| {
            try out.appendSlice(alloc, "{\"sparse_knn\":{\"indices\":[");
            for (sparse.indices, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"values\":[");
            for (sparse.values, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (sparse.k == 0) default_k else sparse.k});
            try out.appendSlice(alloc, "}}");
        },
        else => return error.UnsupportedQueryRequest,
    }
}

fn appendTextQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    query: db_mod.types.TextQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendTextQueryValue(alloc, out, query);
}

fn appendTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.types.TextQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match_none => try out.appendSlice(alloc, "{\"match_none\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, '}');
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, match.field);
            if (match.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            try out.append(alloc, '}');
        },
        .match_phrase => |phrase| {
            try out.appendSlice(alloc, "{\"match_phrase\":");
            try appendJsonString(alloc, out, phrase.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, phrase.field);
            if (phrase.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            if (phrase.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else if (phrase.max_edits > 0) {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{phrase.max_edits});
            }
            try out.append(alloc, '}');
        },
        .fuzzy => |fuzzy| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, fuzzy.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, fuzzy.field);
            if (fuzzy.prefix_len > 0) {
                try out.appendSlice(alloc, ",\"prefix_length\":");
                try out.print(alloc, "{d}", .{fuzzy.prefix_len});
            }
            if (fuzzy.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{fuzzy.max_edits});
            }
            try out.append(alloc, '}');
        },
        .prefix => |prefix| {
            try out.appendSlice(alloc, "{\"prefix\":");
            try appendJsonString(alloc, out, prefix.prefix);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, prefix.field);
            try out.append(alloc, '}');
        },
        .wildcard => |wildcard| {
            try out.appendSlice(alloc, "{\"wildcard\":");
            try appendJsonString(alloc, out, wildcard.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, wildcard.field);
            try out.append(alloc, '}');
        },
        .regexp => |regexp| {
            try out.appendSlice(alloc, "{\"regexp\":");
            try appendJsonString(alloc, out, regexp.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, regexp.field);
            try out.append(alloc, '}');
        },
        .numeric_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| {
                try appendJsonFieldName(alloc, out, &first, "min");
                try out.print(alloc, "{d}", .{min});
            }
            if (range_query.max) |max| {
                try appendJsonFieldName(alloc, out, &first, "max");
                try out.print(alloc, "{d}", .{max});
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try out.append(alloc, '}');
        },
        .date_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.start_ns) |start_ns| {
                const text = try formatRfc3339Ns(alloc, start_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "start", text);
            }
            if (range_query.end_ns) |end_ns| {
                const text = try formatRfc3339Ns(alloc, end_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "end", text);
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_start) try appendJsonFieldBool(alloc, out, &first, "inclusive_start", false);
            if (range_query.inclusive_end) try appendJsonFieldBool(alloc, out, &first, "inclusive_end", true);
            try out.append(alloc, '}');
        },
        .term_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| try appendJsonFieldString(alloc, out, &first, "min", min);
            if (range_query.max) |max| try appendJsonFieldString(alloc, out, &first, "max", max);
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try out.append(alloc, '}');
        },
        .doc_id => |doc_id| {
            try out.appendSlice(alloc, "{\"ids\":[");
            for (doc_id.ids, 0..) |id, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, id);
            }
            try out.appendSlice(alloc, "]}");
        },
        .bool_field => |bool_field| {
            try out.appendSlice(alloc, "{\"bool\":");
            try out.appendSlice(alloc, if (bool_field.value) "true" else "false");
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, bool_field.field);
            try out.append(alloc, '}');
        },
        .bool_query => |bool_query| {
            try out.append(alloc, '{');
            var first = true;
            if (bool_query.must.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must");
                try out.appendSlice(alloc, "{\"conjuncts\":[");
                for (bool_query.must, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            if (bool_query.should.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "should");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.should, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
                if (bool_query.min_should > 0) {
                    try out.appendSlice(alloc, ",\"min\":");
                    try out.print(alloc, "{d}", .{bool_query.min_should});
                }
                try out.append(alloc, '}');
            }
            if (bool_query.must_not.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must_not");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.must_not, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            try out.append(alloc, '}');
        },
        else => return error.UnsupportedQueryRequest,
    }
}

fn parseRemoteSearchResult(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, body, .{});
    defer parsed.deinit();
    const responses = parsed.value.responses orelse return error.InvalidQueryRequest;
    if (responses.len == 0) return error.InvalidQueryRequest;
    const response = responses[0];
    const hits_obj = response.hits orelse return error.InvalidQueryRequest;
    const hits_value = hits_obj.hits orelse return error.InvalidQueryRequest;

    const hits = try alloc.alloc(db_mod.types.SearchHit, hits_value.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (hits_value, 0..) |item, i| {
        hits[i] = .{
            .id = try alloc.dupe(u8, item._id),
            .score = item._score,
            .stored_data = if (item._source) |value| try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})}) else null,
        };
        initialized += 1;
    }

    const graph_results: []db_mod.types.GraphSearchResult = if (response.graph_results) |graph_results_value|
        try parseRemoteGraphResults(alloc, graph_results_value)
    else
        @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits_obj.total orelse 0),
        .graph_results = graph_results,
    };
}

fn parseRemoteGraphResults(
    alloc: std.mem.Allocator,
    value: std.json.ArrayHashMap(indexes_openapi.GraphQueryResult),
) ![]db_mod.types.GraphSearchResult {
    const results = try alloc.alloc(db_mod.types.GraphSearchResult, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*graph_result| graph_result.deinit(alloc);
        alloc.free(results);
    }

    var it = value.map.iterator();
    while (it.next()) |entry| {
        const result_value = entry.value_ptr.*;
        const parsed_nodes = if (result_value.nodes) |nodes_value|
            try parseRemoteGraphNodes(alloc, nodes_value)
        else
            ParsedRemoteGraphNodes{};
        errdefer parsed_nodes.deinit(alloc);
        const parsed_matches = if (result_value.matches) |matches_value|
            try parseRemoteGraphMatches(alloc, matches_value)
        else
            ParsedRemoteGraphMatches{};
        errdefer parsed_matches.deinit(alloc);
        const paths: []graph_paths.Path = if (result_value.paths) |paths_value|
            try parseRemoteGraphPaths(alloc, paths_value)
        else
            @constCast((&[_]graph_paths.Path{})[0..]);
        errdefer {
            for (paths) |path| graph_paths.freePath(alloc, path);
            if (paths.len > 0) alloc.free(paths);
        }

        results[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .nodes = parsed_nodes.nodes,
            .paths = paths,
            .matches = parsed_matches.matches,
            .hits = try concatGraphResultHits(alloc, parsed_nodes.hits, parsed_matches.hits),
            .total_hits = @intCast(result_value.total),
        };
        initialized += 1;
    }

    return results;
}

const ParsedRemoteGraphNodes = struct {
    nodes: []graph_query_mod.GraphResultNode = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    fn deinit(self: ParsedRemoteGraphNodes, alloc: std.mem.Allocator) void {
        for (self.nodes) |*node| node.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

const ParsedRemoteGraphMatches = struct {
    matches: []db_mod.types.GraphPatternMatch = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    fn deinit(self: ParsedRemoteGraphMatches, alloc: std.mem.Allocator) void {
        for (self.matches) |*match| match.deinit(alloc);
        if (self.matches.len > 0) alloc.free(self.matches);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

fn parseRemoteGraphNodes(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphResultNode,
) !ParsedRemoteGraphNodes {
    const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, value.len);
    var initialized: usize = 0;
    errdefer {
        for (nodes[0..initialized]) |*node| node.deinit(alloc);
        alloc.free(nodes);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (value, 0..) |item, i| {
        nodes[i] = .{
            .key = try alloc.dupe(u8, item.key),
            .depth = @intCast(item.depth orelse 0),
            .distance = item.distance orelse 0,
            .path = if (item.path) |path| try cloneRemoteGraphNodePath(alloc, path) else null,
            .path_edges = if (item.path_edges) |path_edges| try cloneRemoteGraphNodePathEdges(alloc, path_edges) else null,
            .provenance = if (item.provenance) |provenance| try cloneRemoteGraphNodePath(alloc, provenance) else null,
        };
        if (item.document) |document| {
            try hits.append(alloc, .{
                .id = try alloc.dupe(u8, item.key),
                .score = null,
                .stored_data = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})}),
            });
        }
        initialized += 1;
    }
    return .{
        .nodes = nodes,
        .hits = try hits.toOwnedSlice(alloc),
    };
}

fn parseRemoteGraphNodeWithKey(
    alloc: std.mem.Allocator,
    key: []const u8,
    item: indexes_openapi.GraphResultNode,
) !graph_query_mod.GraphResultNode {
    return .{
        .key = try alloc.dupe(u8, key),
        .depth = @intCast(item.depth orelse 0),
        .distance = item.distance orelse 0,
        .path = if (item.path) |path| try cloneRemoteGraphNodePath(alloc, path) else null,
        .path_edges = if (item.path_edges) |path_edges| try cloneRemoteGraphNodePathEdges(alloc, path_edges) else null,
        .provenance = if (item.provenance) |provenance| try cloneRemoteGraphNodePath(alloc, provenance) else null,
    };
}

fn parseRemoteGraphMatches(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PatternMatch,
) !ParsedRemoteGraphMatches {
    const matches = try alloc.alloc(db_mod.types.GraphPatternMatch, value.len);
    var initialized_matches: usize = 0;
    errdefer {
        for (matches[0..initialized_matches]) |*match| match.deinit(alloc);
        alloc.free(matches);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (value, 0..) |item, i| {
        const bindings_value = item.bindings orelse return error.InvalidQueryRequest;

        const bindings = try alloc.alloc(db_mod.types.GraphPatternBinding, bindings_value.map.count());
        var initialized_bindings: usize = 0;
        errdefer {
            for (bindings[0..initialized_bindings]) |*binding| binding.deinit(alloc);
            if (bindings.len > 0) alloc.free(bindings);
        }

        var binding_it = bindings_value.map.iterator();
        while (binding_it.next()) |binding_entry| {
            const node_value = binding_entry.value_ptr.*;
            const node = try parseRemoteGraphNodeWithKey(alloc, node_value.key, node_value);
            bindings[initialized_bindings] = .{
                .alias = try alloc.dupe(u8, binding_entry.key_ptr.*),
                .node = node,
            };
            if (node_value.document) |document| {
                try hits.append(alloc, .{
                    .id = try alloc.dupe(u8, node_value.key),
                    .score = null,
                    .stored_data = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})}),
                });
            }
            initialized_bindings += 1;
        }

        matches[i] = .{
            .bindings = bindings,
            .path = if (item.path) |path_value| try cloneRemoteGraphNodePathEdges(alloc, path_value) else @constCast((&[_]graph_query_mod.PathEdgeInfo{})[0..]),
        };
        initialized_matches += 1;
    }

    return .{
        .matches = matches,
        .hits = try hits.toOwnedSlice(alloc),
    };
}

fn concatGraphResultHits(
    alloc: std.mem.Allocator,
    left: []db_mod.types.SearchHit,
    right: []db_mod.types.SearchHit,
) ![]db_mod.types.SearchHit {
    const out = try alloc.alloc(db_mod.types.SearchHit, left.len + right.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*hit| hit.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (left) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    for (right) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    return out;
}

fn cloneRemoteGraphNodePath(alloc: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, value.len);
    errdefer alloc.free(out);
    for (value, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
    }
    return out;
}

fn cloneRemoteGraphNodePathEdges(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PathEdge,
) ![]graph_query_mod.PathEdgeInfo {
    const edges = try alloc.alloc(graph_query_mod.PathEdgeInfo, value.len);
    errdefer alloc.free(edges);
    for (value, 0..) |item, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, item.source orelse return error.InvalidQueryRequest),
            .target = try alloc.dupe(u8, item.target orelse return error.InvalidQueryRequest),
            .edge_type = try alloc.dupe(u8, item.type orelse return error.InvalidQueryRequest),
            .weight = item.weight orelse return error.InvalidQueryRequest,
        };
    }
    return edges;
}

fn parseRemoteGraphPaths(alloc: std.mem.Allocator, value: []const indexes_openapi.Path) ![]graph_paths.Path {
    const paths = try alloc.alloc(graph_paths.Path, value.len);
    var initialized: usize = 0;
    errdefer {
        for (paths[0..initialized]) |path| graph_paths.freePath(alloc, path);
        alloc.free(paths);
    }
    for (value, 0..) |item, i| {
        paths[i] = .{
            .nodes = try cloneRemoteGraphNodePath(alloc, item.nodes orelse return error.InvalidQueryRequest),
            .edges = try parseRemotePathEdges(alloc, item.edges orelse return error.InvalidQueryRequest),
            .total_weight = item.total_weight orelse return error.InvalidQueryRequest,
            .length = @intCast(item.length orelse return error.InvalidQueryRequest),
        };
        initialized += 1;
    }
    return paths;
}

fn parseRemotePathEdges(alloc: std.mem.Allocator, value: []const indexes_openapi.PathEdge) ![]graph_paths.PathEdge {
    const edges = try alloc.alloc(graph_paths.PathEdge, value.len);
    errdefer alloc.free(edges);
    for (value, 0..) |item, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, item.source orelse return error.InvalidQueryRequest),
            .target = try alloc.dupe(u8, item.target orelse return error.InvalidQueryRequest),
            .edge_type = try alloc.dupe(u8, item.type orelse return error.InvalidQueryRequest),
            .weight = item.weight orelse return error.InvalidQueryRequest,
        };
    }
    return edges;
}

fn appendJsonFieldName(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendJsonFieldString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

fn appendJsonFieldU32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

fn appendJsonFieldF32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

fn appendJsonFieldF64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

fn appendJsonFieldBool(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: bool,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

fn appendJsonFieldNames(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    fields: []const []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, field);
    }
    try out.append(alloc, ']');
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn appendScanLine(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    projected_json: ?[]const u8,
) !void {
    const escaped_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
    defer alloc.free(escaped_key);

    try out.appendSlice(alloc, "{\"key\":");
    try out.appendSlice(alloc, escaped_key);
    if (projected_json) |json| {
        if (json.len < 2 or json[0] != '{' or json[json.len - 1] != '}') return error.InvalidProjectedDocumentJson;
        if (json.len > 2) {
            try out.append(alloc, ',');
            try out.appendSlice(alloc, json[1..]);
        } else {
            try out.append(alloc, '}');
        }
    } else {
        try out.append(alloc, '}');
    }
    try out.append(alloc, '\n');
}

fn parseJsonTestBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
}

fn parseNdjsonTestRowsAlloc(comptime T: type, alloc: std.mem.Allocator, ndjson: []const u8) ![]T {
    var count: usize = 0;
    var lines = std.mem.splitScalar(u8, ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        count += 1;
    }

    const out = try alloc.alloc(T, count);
    errdefer alloc.free(out);

    var initialized: usize = 0;
    lines = std.mem.splitScalar(u8, ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = try std.json.parseFromSlice(T, alloc, line, .{});
        defer parsed.deinit();
        out[initialized] = parsed.value;
        initialized += 1;
    }
    return out;
}

const CivilDate = struct {
    year: i64,
    month: i64,
    day: i64,
};

fn formatRfc3339Ns(alloc: std.mem.Allocator, value_ns: u64) ![]u8 {
    const secs_total: u64 = @divFloor(value_ns, std.time.ns_per_s);
    const nanos: u64 = @mod(value_ns, std.time.ns_per_s);
    const days: i64 = @intCast(@divFloor(secs_total, 86_400));
    const secs_of_day: u64 = @mod(secs_total, 86_400);
    const date = civilFromDays(days);
    const year: u64 = @intCast(date.year);
    const month: u64 = @intCast(date.month);
    const day: u64 = @intCast(date.day);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    if (nanos == 0) {
        return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            year, month, day, hour, minute, second,
        });
    }
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        year, month, day, hour, minute, second, nanos,
    });
}

fn civilFromDays(days_since_epoch: i64) CivilDate {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    const year = y + (if (month <= 2) @as(i64, 1) else @as(i64, 0));
    return .{ .year = year, .month = month, .day = day };
}

test "bound table read source uses feature db reads and returns version" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-reads";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\"}",
            },
        },
        .timestamp_ns = 1234,
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var lookup = (try source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1234), lookup.version);
}

test "bound table read source scans keys as ndjson" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-scan";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .read_index)).?;
    defer scan.deinit(alloc);
    const ScanRow = struct {
        key: []const u8,
        title: []const u8,
    };
    const rows = try parseNdjsonTestRowsAlloc(ScanRow, alloc, scan.ndjson);
    defer alloc.free(rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("doc:a", rows[0].key);
    try std.testing.expectEqualStrings("alpha", rows[0].title);
}

test "bound table read source formats query responses" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-query";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello\"}" }},
        .sync_level = .full_index,
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var response = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 5,
    }, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.responses.?.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "bound table read source preflights query requests" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-preflight";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    try std.testing.expectError(error.InvalidArgument, source.source().preflightQuery(alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0 }, .k = 5 },
    }, .read_index, 0));

    var summary = (try source.source().preflightQuery(alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0)).?;
    defer summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.result_refs.len);
    try std.testing.expectEqualStrings("$embeddings_results", summary.result_refs[0]);
    try std.testing.expectEqual(@as(u32, 1), summary.shard_count);
    try std.testing.expectEqual(@as(u32, 0), summary.remote_shard_count);
    try std.testing.expectEqual(@as(u32, 1), summary.dense_query_count);
    try std.testing.expectEqual(@as(u32, 1), summary.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), summary.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 0), summary.vector_worker_filter_constraint_count);
    try std.testing.expect(!summary.vector_worker_requires_algebraic_filter_resolution);
    try std.testing.expectEqual(@as(u64, 5), summary.dense_effective_k_total);
    try std.testing.expect(summary.dense_search_width_total >= summary.dense_effective_k_total);
    try std.testing.expect(summary.dense_search_width_max >= 64);
    try std.testing.expect(summary.dense_epsilon_max >= 1.0);
}

test "merge runtime preflight summary preserves structured filter exact counts only when every shard is exact" {
    const alloc = std.testing.allocator;

    var exact_left: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_estimate = 2,
        .structured_filter_count_exact = true,
    };
    defer exact_left.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &exact_left, .{
        .structured_filter_doc_count_estimate = 3,
        .structured_filter_count_exact = true,
    });
    try std.testing.expectEqual(@as(?u64, 5), exact_left.structured_filter_doc_count_estimate);
    try std.testing.expect(exact_left.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 5), exact_left.result_doc_estimate);

    var mixed: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_estimate = 2,
        .structured_filter_count_exact = true,
        .vector_worker_candidate_count = 1,
        .vector_worker_filter_constraint_count = 2,
        .vector_worker_requires_algebraic_filter_resolution = true,
    };
    defer mixed.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &mixed, .{
        .structured_filter_doc_count_estimate = 7,
        .structured_filter_count_exact = false,
        .vector_worker_fallback_count = 1,
        .vector_worker_filter_constraint_count = 1,
    });
    try std.testing.expectEqual(@as(?u64, null), mixed.structured_filter_doc_count_estimate);
    try std.testing.expect(!mixed.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u64, 9), mixed.structured_filter_doc_count_lower_bound);
    try std.testing.expectEqual(@as(?u32, null), mixed.result_doc_estimate);
    try std.testing.expectEqual(@as(u32, 1), mixed.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), mixed.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 3), mixed.vector_worker_filter_constraint_count);
    try std.testing.expect(mixed.vector_worker_requires_algebraic_filter_resolution);

    var lower_bounds: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_lower_bound = 4,
        .structured_filter_count_exact = false,
    };
    defer lower_bounds.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &lower_bounds, .{
        .structured_filter_doc_count_lower_bound = 6,
        .structured_filter_count_exact = false,
    });
    try std.testing.expectEqual(@as(?u64, 10), lower_bounds.structured_filter_doc_count_lower_bound);
    try std.testing.expect(!lower_bounds.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, null), lower_bounds.result_doc_estimate);

    var sampled: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_sample_estimate = 4,
        .structured_filter_count_sample_size = 8,
        .structured_filter_count_exact = false,
    };
    defer sampled.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &sampled, .{
        .structured_filter_doc_count_sample_estimate = 6,
        .structured_filter_count_sample_size = 12,
        .structured_filter_count_exact = false,
    });
    try std.testing.expectEqual(@as(?u64, 10), sampled.structured_filter_doc_count_sample_estimate);
    try std.testing.expectEqual(@as(u32, 20), sampled.structured_filter_count_sample_size);
    try std.testing.expect(!sampled.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 10), sampled.result_doc_estimate);
}

test "bound table read source reranks hits after materialization" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-rerank";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"hello beta\"}" },
        },
        .sync_level = .full_index,
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var ts = try httpx.TestServer.start(alloc, io_impl.io(), &.{
        .{ .method = .POST, .path = "/rerank", .respond = .{
            .body = "{\"scores\":[0.1,0.9]}",
        } },
    });
    defer ts.deinit();

    const url = try std.fmt.allocPrint(alloc, "{s}", .{ts.baseUrl()});
    defer alloc.free(url);

    var response: ?query_api.QueryResponse = null;
    defer if (response) |*value| value.deinit(alloc);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: std.mem.Allocator,
            read_source: *BoundTableReadSource,
            out: *?query_api.QueryResponse,
            err_out: *?anyerror,
            reranker_url: []const u8,
        ) std.Io.Cancelable!void {
            out.* = read_source.source().query(a, "docs", .{
                .query = .{ .match = .{ .field = "body", .text = "hello" } },
                .limit = 10,
                .profile = true,
                .reranker = .{
                    .provider = .termite,
                    .model = "cross-encoder/ms-marco-MiniLM-L-6-v2",
                    .field = "body",
                    .url = reranker_url,
                },
                .reranker_query_text = "hello",
            }, .read_index) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io_impl.io(), Fiber.run, .{ alloc, &source, &response, &run_err, url }) catch return;
    try ts.handleOne();
    group.await(io_impl.io()) catch {};
    if (run_err) |err| return err;

    try std.testing.expect(response != null);
    const RerankResponse = struct {
        responses: []struct {
            hits: ?struct {
                hits: ?[]struct { _id: []const u8 } = null,
            } = null,
            profile: ?struct {
                reranker: ?struct {
                    model: []const u8,
                } = null,
            } = null,
        },
    };
    var parsed = try parseJsonTestBody(RerankResponse, alloc, response.?.json);
    defer parsed.deinit();
    const inner = parsed.value.responses[0];
    try std.testing.expectEqualStrings("doc:b", inner.hits.?.hits.?[0]._id);
    try std.testing.expectEqualStrings("doc:a", inner.hits.?.hits.?[1]._id);
    try std.testing.expectEqualStrings("cross-encoder/ms-marco-MiniLM-L-6-v2", inner.profile.?.reranker.?.model);
}

test "provisioned table read source routes lookup and scan across ranges" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-reads";

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

    try left_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
    try right_db.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" }},
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var lookup = (try source.source().lookup(alloc, "docs", "doc:z", .{}, .stale)).?;
    defer lookup.deinit(alloc);
    const LookupTitle = struct { title: []const u8 };
    var parsed_lookup = try parseJsonTestBody(LookupTitle, alloc, lookup.json);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("zeta", parsed_lookup.value.title);

    var scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .stale)).?;
    defer scan.deinit(alloc);
    const ScanRow = struct {
        key: []const u8,
        title: []const u8,
    };
    const rows = try parseNdjsonTestRowsAlloc(ScanRow, alloc, scan.ndjson);
    defer alloc.free(rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("alpha", rows[0].title);
    try std.testing.expectEqualStrings("zeta", rows[1].title);
}

test "fanout planner uses io cap and request shape" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .limited(8),
    });
    defer io_impl.deinit();

    const no_io_plan = planFanout(.text_stats, null, 4);
    try std.testing.expect(!no_io_plan.parallel);
    try std.testing.expectEqual(@as(usize, 1), no_io_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.no_io, no_io_plan.reason);

    const text_stats_plan = planFanout(.text_stats, &io_impl, 6);
    try std.testing.expect(text_stats_plan.parallel);
    try std.testing.expectEqual(@as(usize, 4), text_stats_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.parallel, text_stats_plan.reason);

    const small_query_plan = planQueryFanout(&io_impl, 2, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 10,
    });
    try std.testing.expect(!small_query_plan.parallel);
    try std.testing.expectEqual(@as(usize, 1), small_query_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.small_request, small_query_plan.reason);

    const larger_query_plan = planQueryFanout(&io_impl, 6, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 100,
    });
    try std.testing.expect(larger_query_plan.parallel);
    try std.testing.expectEqual(@as(usize, 6), larger_query_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.parallel, larger_query_plan.reason);
}

test "provisioned table read source merges query results across ranges" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query";

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

    try left_db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try right_db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });

    try left_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
    });
    try right_db.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"body\":\"hello there\"}" }},
        .sync_level = .full_index,
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    _ = source.withIo(&io_impl);
    var response = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 10,
    }, .stale)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:z", hits[1]._id);
}

test "provisioned table read source serves dense queries for explicit external embeddings" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

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
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var response = (try source.source().query(alloc, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        } },
        .limit = 3,
    }, .stale)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
}

test "provisioned local query execution returns stamped identity request" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-stamped-identity";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    {
        var db = try db_mod.DB.open(alloc, group_path, .{});
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
    }

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

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
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var execution = try queryHostedLocalDetailed(
        null,
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        alloc,
        7001,
        0,
        null,
        null,
        null,
        null,
        "docs",
        .{ .limit = 1 },
        .stale,
    );
    defer execution.result.deinit();

    try std.testing.expect(execution.request.identity_read_generation != null);
    try std.testing.expectEqual(@as(u32, 1), execution.result.total_hits);
    try std.testing.expectEqualStrings("doc:a", execution.result.hits[0].id);
}

test "provisioned table read source serves public dense query requests with read_index" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-public";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
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
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
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

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":3}
    );
    defer owned.deinit(alloc);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
}

test "provisioned table read source serves profiled public dense query requests with read_index" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-public-profiled";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
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
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
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

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":3,"profile":true}
    );
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("dense_idx", owned.req.dense_queries[0].index_name);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
    try std.testing.expect(parsed.value.responses.?[0].profile != null);
}

test "provisioned table read source serves public dense query requests without explicit indexes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-public-implicit-index";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
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
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
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

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"limit":3}
    );
    defer owned.deinit(alloc);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
}

test "provisioned table read source serves benchmark-shaped packed dense query with full-text present" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-benchmark-shaped";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
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
                    .placement_role = "data",
                    .indexes_json =
                    \\{"ft_v1":{"type":"full_text"},"vec":{"type":"embeddings","external":true,"dimension":3}}
                    ,
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

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "vec",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"vec_data\",\"dims\":3,\"metric\":\"cosine\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "key:0", .value = "{\"id\":0,\"metadata\":0,\"vec_data\":\"0\",\"body\":\"alpha retrieval\",\"_embeddings\":{\"vec\":\"AACAPwAAAAAAAAAA\"}}" },
            .{ .key = "key:1", .value = "{\"id\":1,\"metadata\":1,\"vec_data\":\"1\",\"body\":\"beta retrieval\",\"_embeddings\":{\"vec\":\"AAAAAAAAgD8AAAAA\"}}" },
            .{ .key = "key:2", .value = "{\"id\":2,\"metadata\":2,\"vec_data\":\"2\",\"body\":\"gamma retrieval\",\"_embeddings\":{\"vec\":\"ZmZmP83MzD0AAAAA\"}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"vec":[1.0,0.0,0.0]},"limit":3}
    );
    defer owned.deinit(alloc);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("key:0", hits[0]._id);
    try std.testing.expectEqualStrings("key:2", hits[1]._id);
}

test "provisioned table read source preflights every local group" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-preflight-multigroup";

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

    try left_db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

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
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
                }})[0..]),
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

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    _ = source.withIo(&io_impl);
    try std.testing.expectError(error.InvalidArgument, source.source().preflightQuery(alloc, "docs", .{
        .index_name = "dense_idx",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0));
    try std.testing.expectError(error.UnsupportedQueryRequest, source.source().preflightQuery(alloc, "docs", .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$embeddings_results", .limit = 1 } },
                    .params = .{ .edge_types = &.{}, .max_depth = 1 },
                },
            },
        },
    }, .read_index, 0));
}

test "provisioned local runtime statuses reconcile empty managed embeddings indexes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-runtime-status-managed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
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

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    source.cache = &cache;
    var db_lease = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 0, "docs");
    defer db_lease.release();
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(false, statuses.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table read source runtime status stays cache-only without shared snapshot" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-runtime-cache";

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

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    var db_lease = try cache.getOrOpen(path, WarmCatalog.iface(), 7001, 0, "docs");
    defer db_lease.release();

    var source = ProvisionedTableReadSource.init(path, NoCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    source.cache = &cache;

    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table read source runtime status falls back to shared snapshot cache" {
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

    var source = ProvisionedTableReadSource.init("/tmp/unused-antfly-runtime-snapshot", NoCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table read source runtime status prefers shared snapshot cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-runtime-prefers-snapshot";

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

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    var db_lease = try cache.getOrOpen(path, WarmCatalog.iface(), 7001, 0, "docs");
    defer db_lease.release();

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

    var source = ProvisionedTableReadSource.init(path, NoCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    source.cache = &cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 42), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("snapshot_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table read source falls back from read_index to stale on not leader" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-fallback";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
        .timestamp_ns = 4321,
    });

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}" }})[0..]),
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

    const NotLeaderOnce = struct {
        count: usize = 0,

        fn requester(self: *@This()) raft_mod.ReadableLeaseRequester {
            return .{
                .ptr = self,
                .vtable = &.{
                    .request_readable_lease = requestReadableLease,
                },
            };
        }

        fn requestReadableLease(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
            return error.NotLeader;
        }
    };

    var requester = NotLeaderOnce{};
    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), requester.requester());

    var lookup = (try source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4321), lookup.version);
    const LookupTitle = struct { title: []const u8 };
    var parsed_lookup = try parseJsonTestBody(LookupTitle, alloc, lookup.json);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_lookup.value.title);

    var response = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 5,
    }, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed.value.responses.?[0].hits.?.hits.?[0]._id);
    try std.testing.expectEqual(@as(usize, 2), requester.count);
}

test "encode query request round-trips composed bleve full_text queries" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{
            .bool_query = .{
                .must = &.{
                    .{ .match = .{ .field = "body", .text = "hello" } },
                    .{ .numeric_range = .{
                        .field = "score",
                        .min = 10,
                        .max = 20,
                        .inclusive_max = true,
                    } },
                },
                .must_not = &.{
                    .{ .date_range = .{
                        .field = "created_at",
                        .start_ns = 1_772_323_200 * std.time.ns_per_s,
                        .inclusive_end = true,
                    } },
                },
            },
        },
        .limit = 5,
        .identity_read_generation = 77,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const full_text = parsed.value.object.get("full_text_search").?.object;
    try std.testing.expectEqual(@as(i64, 77), parsed.value.object.get("_identity_read_generation").?.integer);
    const must = full_text.get("must").?.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), must.len);
    try std.testing.expectEqual(true, must[1].object.get("inclusive_max").?.bool);
    try std.testing.expectEqualStrings("2026-03-01T00:00:00Z", full_text.get("must_not").?.object.get("disjuncts").?.array.items[0].object.get("start").?.string);
    try std.testing.expect(full_text.get("fuzziness") == null);

    const fuzzy = try encodeQueryRequest(alloc, .{
        .full_text = .{
            .fuzzy = .{
                .field = "body",
                .term = "helo",
                .max_edits = 1,
            },
        },
    });
    defer alloc.free(fuzzy);
    var parsed_fuzzy = try parseJsonTestBody(std.json.Value, alloc, fuzzy);
    defer parsed_fuzzy.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_fuzzy.value.object.get("full_text_search").?.object.get("fuzziness").?.integer);
}

test "encode query request includes named vector embeddings for routed semantic search" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .dense_queries = &.{
            .{
                .name = "semantic_idx",
                .index_name = "semantic_idx",
                .query = .{
                    .vector = &.{ 0.25, 0.5, 0.75 },
                    .k = 4,
                },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sparse_idx",
                .index_name = "sparse_idx",
                .query = .{
                    .indices = &.{ 1, 7 },
                    .values = &.{ 0.4, 0.9 },
                    .k = 4,
                },
            },
        },
        .limit = 4,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const embeddings = parsed.value.object.get("embeddings").?.object;
    const dense = embeddings.get("semantic_idx").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), dense.len);
    try std.testing.expectEqual(@as(f64, 0.25), dense[0].float);
    try std.testing.expectEqual(@as(f64, 0.75), dense[2].float);
    const sparse = embeddings.get("sparse_idx").?.object;
    try std.testing.expectEqual(@as(i64, 1), sparse.get("indices").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 7), sparse.get("indices").?.array.items[1].integer);
    try std.testing.expectEqual(@as(f64, 0.4), sparse.get("values").?.array.items[0].float);
    try std.testing.expectEqual(@as(f64, 0.9), sparse.get("values").?.array.items[1].float);
}

test "encode query request includes merge config and pruner but omits reranker" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .merge_config = .{
            .strategy = .rsf,
            .window_size = 25,
            .rank_constant = 42.0,
            .weights = &.{
                .{ .name = "full_text", .weight = 0.5 },
                .{ .name = "semantic_idx", .weight = 1.5 },
            },
        },
        .pruner = .{
            .min_score_ratio = 0.5,
            .require_multi_index = true,
        },
        .reranker = .{
            .provider = .termite,
            .model = "cross-encoder/ms-marco-MiniLM-L-6-v2",
            .field = "body",
        },
        .reranker_query_text = "hello",
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const merge_config = parsed.value.object.get("merge_config").?.object;
    try std.testing.expectEqualStrings("rsf", merge_config.get("strategy").?.string);
    try std.testing.expectEqual(@as(f64, 0.5), merge_config.get("weights").?.object.get("full_text").?.float);
    try std.testing.expectEqual(@as(f64, 1.5), merge_config.get("weights").?.object.get("semantic_idx").?.float);
    const pruner = parsed.value.object.get("pruner").?.object;
    try std.testing.expectEqual(@as(f64, 0.5), pruner.get("min_score_ratio").?.float);
    try std.testing.expectEqual(true, pruner.get("require_multi_index").?.bool);
    try std.testing.expect(parsed.value.object.get("reranker") == null);
}

test "encode query request includes distributed text stats for internal shard scoring" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello world" } },
        .distributed_text_stats = &.{.{
            .field = "body",
            .global_doc_count = 9,
            .global_total_field_len = 45,
            .term_doc_freqs = &.{
                .{ .term = "hello", .doc_freq = 4 },
                .{ .term = "world", .doc_freq = 2 },
            },
        }},
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const stats = parsed.value.object.get("_distributed_text_stats").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), stats.len);
    try std.testing.expectEqualStrings("body", stats[0].object.get("field").?.string);
    try std.testing.expectEqual(@as(i64, 9), stats[0].object.get("global_doc_count").?.integer);
    try std.testing.expectEqual(@as(i64, 45), stats[0].object.get("global_total_field_len").?.integer);
    const freqs = stats[0].object.get("term_doc_freqs").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), freqs.len);
    try std.testing.expectEqualStrings("hello", freqs[0].object.get("term").?.string);
    try std.testing.expectEqual(@as(i64, 4), freqs[0].object.get("doc_freq").?.integer);
}

test "distributed table reads reject resolved doc filters" {
    var sentinel: u8 = 0;
    var req: db_mod.types.SearchRequest = .{
        .resolved_doc_filter = &sentinel,
    };

    try std.testing.expectError(error.UnsupportedQueryRequest, rejectCrossGroupResolvedDocFilter(req, 2));
    try rejectCrossGroupResolvedDocFilter(req, 1);
    try rejectRemoteRouteResolvedDocFilter(req, .local);
    var remote_uri_buf = [_]u8{'h'};
    try std.testing.expectError(error.UnsupportedQueryRequest, rejectRemoteRouteResolvedDocFilter(req, .{ .remote = .{ .node_id = 2, .base_uri = remote_uri_buf[0..] } }));
    req.resolved_doc_filter = null;
    try rejectCrossGroupResolvedDocFilter(req, 2);
    try rejectRemoteRouteResolvedDocFilter(req, .{ .remote = .{ .node_id = 2, .base_uri = remote_uri_buf[0..] } });
}

test "distributed table reads reject stale doc identity before multigroup fanout" {
    const alloc = std.testing.allocator;

    const FakeCatalog = struct {
        statuses: []const metadata_reconciler.MergedGroupStatus,

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
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(self.statuses),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const healthy_statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
    };
    var healthy_catalog = FakeCatalog{ .statuses = healthy_statuses[0..] };
    try testing.validateDocIdentityReadyForMultiGroupRead(alloc, healthy_catalog.iface(), "docs", 2);
    try testing.validateDocIdentityReadyForMultiGroupRead(alloc, healthy_catalog.iface(), "docs", 1);

    var healthy_source = ProvisionedTableReadSource.init("/tmp/unused-antfly-docid-helper-guard", healthy_catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    const group_ids = [_]u64{ 7001, 7002 };
    var sentinel: u8 = 0;
    const resolved_explicit_req = db_mod.types.SearchRequest{
        .aggregations_json = "{\"sig_body\":{\"type\":\"significant_terms\",\"field\":\"body\"}}",
        .resolved_doc_filter = &sentinel,
    };
    const resolved_background_req = db_mod.types.SearchRequest{
        .aggregations_json = "{\"sig_body\":{\"type\":\"significant_terms\",\"field\":\"body\",\"background_filter\":{\"match_all\":{}}}}",
        .resolved_doc_filter = &sentinel,
    };
    const stats_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    defer {
        for (stats_hits) |*hit| hit.deinit(alloc);
        alloc.free(stats_hits);
    }
    stats_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha beta\"}"),
    };
    try std.testing.expect((try collectProvisionedAlgebraicDistributedPartials(&healthy_source, alloc, group_ids[0..], "docs", resolved_explicit_req, "alg", &.{}, .{
        .output = .{ .input = 0 },
    })) == null);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectProvisionedAggregationTextStats(&healthy_source, alloc, group_ids[0..], "docs", resolved_explicit_req, stats_hits));
    try std.testing.expectError(error.UnsupportedQueryRequest, collectProvisionedAggregationBackgroundTextStats(&healthy_source, alloc, group_ids[0..], "docs", resolved_background_req, stats_hits));

    const rebuild_required = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .rebuild_required = true } },
    };
    var rebuild_catalog = FakeCatalog{ .statuses = rebuild_required[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, testing.validateDocIdentityReadyForMultiGroupRead(alloc, rebuild_catalog.iface(), "docs", 2));

    var source = ProvisionedTableReadSource.init("/tmp/unused-antfly-docid-helper-guard", rebuild_catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedSearchRequestTextStats(&source, alloc, group_ids[0..], .{
        .full_text = .{ .match = .{ .field = "body", .text = "hello" } },
    }, "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedAggregationTextStats(&source, alloc, group_ids[0..], "docs", .{
        .aggregations_json = "unparsed because doc identity guard runs first",
    }, &.{}));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedAggregationBackgroundTextStats(&source, alloc, group_ids[0..], "docs", .{
        .aggregations_json = "unparsed because doc identity guard runs first",
    }, &.{}));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedAlgebraicDistributedPartials(&source, alloc, group_ids[0..], "docs", .{}, "alg", &.{}, .{
        .output = .{ .input = 0 },
    }));
}

test "internal worker doc identity exchange audit covers every boundary" {
    var carries_shard_doc_set: usize = 0;
    var validates_generation_projection: usize = 0;
    var fail_closed_before_fanout: usize = 0;

    inline for (std.meta.fields(DocIdentityInternalWorkerBoundary)) |field| {
        const boundary: DocIdentityInternalWorkerBoundary = @field(DocIdentityInternalWorkerBoundary, field.name);
        switch (docIdentityInternalWorkerPolicy(boundary)) {
            .carries_shard_doc_set => carries_shard_doc_set += 1,
            .validates_generation_projection => validates_generation_projection += 1,
            .fail_closed_before_fanout => fail_closed_before_fanout += 1,
        }
    }

    try std.testing.expectEqual(@as(usize, 8), carries_shard_doc_set);
    try std.testing.expectEqual(@as(usize, 11), validates_generation_projection);
    try std.testing.expectEqual(@as(usize, 0), fail_closed_before_fanout);
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.query));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.vector_worker));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.search_request_text_stats));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.distributed_join_worker));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.distributed_join_unmatched_followup));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.shuffle_worker));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.algebraic_partials));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.aggregation_full_result_rerun));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.graph_result_ref));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.explicit_text_stats));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.background_text_stats));
}

test "encode query request with distributed text stats parses through query contract" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "hello world" } },
        .fields = &.{"title"},
        .include_all_fields = false,
        .limit = 7,
        .distributed_text_stats = &.{.{
            .field = "body",
            .global_doc_count = 9,
            .global_total_field_len = 45,
            .term_doc_freqs = &.{
                .{ .term = "hello", .doc_freq = 4 },
                .{ .term = "world", .doc_freq = 2 },
            },
        }},
    });
    defer alloc.free(encoded);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 7), owned.req.limit);
    try std.testing.expectEqual(@as(usize, 1), owned.fields.len);
    try std.testing.expectEqualStrings("title", owned.fields[0]);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expectEqual(@as(usize, 1), owned.req.distributed_text_stats.len);
    try std.testing.expectEqualStrings("body", owned.req.distributed_text_stats[0].field);
    try std.testing.expectEqual(@as(u32, 9), owned.req.distributed_text_stats[0].global_doc_count);
    try std.testing.expectEqual(@as(u64, 45), owned.req.distributed_text_stats[0].global_total_field_len);
    try std.testing.expectEqual(@as(usize, 2), owned.req.distributed_text_stats[0].term_doc_freqs.len);
    try std.testing.expectEqualStrings("hello", owned.req.distributed_text_stats[0].term_doc_freqs[0].term);
}

test "encode query request carries internal native doc id constraints through query contract" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .dense_queries = &.{
            .{
                .name = "semantic_idx",
                .index_name = "semantic_idx",
                .query = .{
                    .vector = &.{ 0.25, 0.5 },
                    .k = 5,
                },
            },
        },
        .filter_doc_ids = &.{ "doc:a", "doc:b" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:c"},
        .limit = 5,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const constraints = parsed.value.object.get("native_doc_id_constraints").?.object;
    try std.testing.expectEqual(true, constraints.get("positive_filter").?.bool);
    try std.testing.expectEqualStrings("doc:a", constraints.get("include_doc_ids").?.array.items[0].string);
    try std.testing.expectEqualStrings("doc:c", constraints.get("exclude_doc_ids").?.array.items[0].string);
    try std.testing.expect(parsed.value.object.get("_filter_doc_ids_positive") == null);
    try std.testing.expect(parsed.value.object.get("_filter_doc_ids") == null);
    try std.testing.expect(parsed.value.object.get("_exclude_doc_ids") == null);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expect(owned.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 2), owned.req.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", owned.req.filter_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", owned.req.filter_doc_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), owned.req.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", owned.req.exclude_doc_ids[0]);
}

test "vector worker envelope converts to constrained search request" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const candidate_input = algebraic_ir.TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "native_doc_id_constraints",
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{candidate_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .input_dims = &.{.doc},
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .step = 0 },
    };
    const encoded = try query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .{
            .fields = @constCast((&[_][]const u8{"title"})[0..]),
            .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
            .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
            .filter_prefix = "tenant/a/",
            .filter_ids = &.{ 42, 99 },
            .exclude_ids = &.{7},
            .require_algebraic_filter_resolution = true,
            .include_all_fields = false,
            .defer_stored_projection = true,
            .limit = 8,
            .offset = 1,
            .profile = true,
            .include_stored = false,
            .search_effort = 0.5,
            .distance_over = 0.1,
            .distance_under = 0.9,
            .return_mode = .parent_with_chunks,
            .max_chunks_per_parent = 2,
            .identity_read_generation = 12345,
        },
        .{
            .positive_filter = true,
            .include_doc_ids = &.{ "doc:a", "doc:b" },
            .exclude_doc_ids = &.{"doc:c"},
        },
        null,
        null,
        &.{access_path},
        program,
    );
    defer alloc.free(encoded);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, encoded);
    defer envelope.deinit(alloc);
    const req = searchRequestFromVectorWorkerEnvelope(&envelope);

    try std.testing.expectEqualStrings("dense_idx", req.index_name.?);
    try std.testing.expectEqual(@as(u32, 8), req.limit);
    try std.testing.expectEqual(@as(u32, 1), req.offset);
    try std.testing.expect(req.profile);
    try std.testing.expect(!req.include_stored);
    try std.testing.expect(!req.include_all_fields);
    try std.testing.expect(req.defer_stored_projection);
    try std.testing.expectEqual(@as(usize, 1), req.fields.len);
    try std.testing.expectEqualStrings("title", req.fields[0]);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", req.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", req.exclusion_query_json);
    try std.testing.expect(req.require_algebraic_filter_resolution);
    try std.testing.expectEqualStrings("tenant/a/", req.filter_prefix);
    try std.testing.expectEqual(@as(usize, 2), req.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 42), req.filter_ids[0]);
    try std.testing.expectEqual(@as(u64, 99), req.filter_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), req.exclude_ids.len);
    try std.testing.expectEqual(@as(u64, 7), req.exclude_ids[0]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), req.search_effort.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), req.distance_over.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), req.distance_under.?, 0.0001);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, req.return_mode);
    try std.testing.expectEqual(@as(u32, 2), req.max_chunks_per_parent);
    try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
    try std.testing.expect(req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 2), req.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", req.filter_doc_ids[0]);
    try std.testing.expectEqual(@as(usize, 1), req.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", req.exclude_doc_ids[0]);
    switch (req.query) {
        .dense_knn => |dense| {
            try std.testing.expectEqual(@as(u32, 7), dense.k);
            try std.testing.expectEqual(@as(usize, 2), dense.vector.len);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "simple vector shard request lowers to vector worker envelope" {
    const alloc = std.testing.allocator;
    const body = (try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 11,
        .offset = 3,
        .count_only = true,
        .profile = true,
        .include_stored = false,
        .fields = &.{ "title", "score" },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_prefix = "tenant/a/",
        .filter_ids = &.{ 99, 42 },
        .exclude_ids = &.{7},
        .include_all_fields = false,
        .defer_stored_projection = true,
        .search_effort = 0.5,
        .distance_under = 0.9,
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 2,
        .identity_read_generation = 54321,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{ "doc:b", "doc:a" },
        .exclude_doc_ids = &.{"doc:c"},
    })).?;
    defer alloc.free(body);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, body);
    defer envelope.deinit(alloc);
    try std.testing.expectEqualStrings("dense_idx", envelope.index_name);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.dense_vector, envelope.layout);
    try std.testing.expectEqual(@as(u32, 11), envelope.options.limit);
    try std.testing.expectEqual(@as(u32, 3), envelope.options.offset);
    try std.testing.expect(envelope.options.count_only);
    try std.testing.expect(envelope.options.profile);
    try std.testing.expect(!envelope.options.include_stored);
    try std.testing.expect(!envelope.options.include_all_fields);
    try std.testing.expect(envelope.options.defer_stored_projection);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", envelope.options.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", envelope.options.exclusion_query_json);
    try std.testing.expect(envelope.options.require_algebraic_filter_resolution);
    try std.testing.expectEqualStrings("tenant/a/", envelope.options.filter_prefix);
    try std.testing.expectEqual(@as(usize, 2), envelope.options.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 99), envelope.options.filter_ids[0]);
    try std.testing.expectEqual(@as(u64, 42), envelope.options.filter_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), envelope.options.exclude_ids.len);
    try std.testing.expectEqual(@as(u64, 7), envelope.options.exclude_ids[0]);
    try std.testing.expectEqual(@as(usize, 2), envelope.options.fields.len);
    try std.testing.expectEqualStrings("title", envelope.options.fields[0]);
    try std.testing.expectEqualStrings("score", envelope.options.fields[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), envelope.options.search_effort.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), envelope.options.distance_under.?, 0.0001);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, envelope.options.return_mode);
    try std.testing.expectEqual(@as(u32, 2), envelope.options.max_chunks_per_parent);
    try std.testing.expectEqual(@as(?u64, 54321), envelope.options.identity_read_generation);
    try std.testing.expect(envelope.native_doc_id_constraints.constraints.positive_filter);
    try std.testing.expectEqualStrings("doc:a", envelope.native_doc_id_constraints.constraints.include_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", envelope.native_doc_id_constraints.constraints.include_doc_ids[1]);
    try std.testing.expectEqualStrings("doc:c", envelope.native_doc_id_constraints.constraints.exclude_doc_ids[0]);
    try std.testing.expect((try envelope.proveTensorProgramAlloc(alloc)).safe());

    const supported_filter = try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 7,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
    });
    try std.testing.expect(supported_filter != null);
    if (supported_filter) |body_supported| alloc.free(body_supported);

    const unsupported = try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 7,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    });
    try std.testing.expect(unsupported == null);
}

test "vector worker preflight annotation tracks eligibility and symbolic filters" {
    const alloc = std.testing.allocator;

    var supported: db_mod.RuntimePreflightSummary = .{};
    defer supported.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &supported, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_ids = &.{42},
        .exclude_ids = &.{7},
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{"doc:a"},
        .exclude_doc_ids = &.{"doc:b"},
    });
    try std.testing.expectEqual(@as(u32, 1), supported.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), supported.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 6), supported.vector_worker_filter_constraint_count);
    try std.testing.expect(supported.vector_worker_requires_algebraic_filter_resolution);

    var unsupported: db_mod.RuntimePreflightSummary = .{};
    defer unsupported.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &unsupported, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    });
    try std.testing.expectEqual(@as(u32, 0), unsupported.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), unsupported.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 1), unsupported.vector_worker_filter_constraint_count);
    try std.testing.expect(unsupported.vector_worker_requires_algebraic_filter_resolution);

    var sentinel: u8 = 0;
    var resolved_filter: db_mod.RuntimePreflightSummary = .{};
    defer resolved_filter.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &resolved_filter, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &sentinel,
    });
    try std.testing.expectEqual(@as(u32, 0), resolved_filter.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), resolved_filter.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 1), resolved_filter.vector_worker_filter_constraint_count);

    var non_vector: db_mod.RuntimePreflightSummary = .{};
    defer non_vector.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &non_vector, .{ .query = .{ .match_all = {} } });
    try std.testing.expectEqual(@as(u32, 0), non_vector.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), non_vector.vector_worker_fallback_count);
}

test "remote simple vector query uses vector worker route" {
    const alloc = std.testing.allocator;

    const ExecutorState = struct {
        vector_worker_calls: usize = 0,
        query_calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/11/tables/docs/vector-worker")) {
                self.vector_worker_calls += 1;
                var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc_inner, req.body);
                defer envelope.deinit(alloc_inner);
                try std.testing.expectEqualStrings("dense_idx", envelope.index_name);
                try std.testing.expectEqual(@as(u32, 11), envelope.options.limit);
                try std.testing.expectEqual(@as(u32, 3), envelope.options.offset);
                try std.testing.expect(envelope.options.profile);
                try std.testing.expectEqual(@as(?u64, 77), envelope.options.identity_read_generation);
                try std.testing.expect(!envelope.options.include_all_fields);
                try std.testing.expect(envelope.options.defer_stored_projection);
                try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", envelope.options.filter_query_json);
                try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", envelope.options.exclusion_query_json);
                try std.testing.expect(envelope.options.require_algebraic_filter_resolution);
                try std.testing.expectEqualStrings("tenant/a/", envelope.options.filter_prefix);
                try std.testing.expectEqual(@as(usize, 1), envelope.options.filter_ids.len);
                try std.testing.expectEqual(@as(u64, 42), envelope.options.filter_ids[0]);
                try std.testing.expectEqual(@as(usize, 1), envelope.options.exclude_ids.len);
                try std.testing.expectEqual(@as(u64, 7), envelope.options.exclude_ids[0]);
                try std.testing.expectEqual(@as(usize, 1), envelope.options.fields.len);
                try std.testing.expectEqualStrings("title", envelope.options.fields[0]);
                try std.testing.expectApproxEqAbs(@as(f32, 0.5), envelope.options.search_effort.?, 0.0001);
                try std.testing.expectApproxEqAbs(@as(f32, 0.9), envelope.options.distance_under.?, 0.0001);
                try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, envelope.options.return_mode);
                try std.testing.expectEqual(@as(u32, 2), envelope.options.max_chunks_per_parent);
                try std.testing.expect(envelope.native_doc_id_constraints.constraints.positive_filter);
            } else if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/11/tables/docs/query")) {
                self.query_calls += 1;
                try std.testing.expect(std.mem.indexOf(u8, req.body, "\"_filter_query_json\"") != null);
                try std.testing.expect(std.mem.indexOf(u8, req.body, "\"_identity_read_generation\":88") != null);
                try std.testing.expect(std.mem.indexOf(u8, req.body, "\"filter_query\"") == null);
            } else {
                return error.UnexpectedHttpRequest;
            }
            return .{
                .status = 200,
                .body = try alloc_inner.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":0,\"hits\":[]},\"took\":0,\"status\":200,\"table\":\"docs\"}]}"),
            };
        }
    };

    var state = ExecutorState{};
    var vector_result = try queryRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .limit = 11,
        .offset = 3,
        .profile = true,
        .fields = &.{"title"},
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_prefix = "tenant/a/",
        .filter_ids = &.{42},
        .exclude_ids = &.{7},
        .include_all_fields = false,
        .defer_stored_projection = true,
        .search_effort = 0.5,
        .distance_under = 0.9,
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 2,
        .identity_read_generation = 77,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{"doc:a"},
    });
    defer vector_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.vector_worker_calls);
    try std.testing.expectEqual(@as(usize, 0), state.query_calls);
    try std.testing.expectEqual(@as(?u64, 77), vector_result.identity_read_generation);

    var fallback_result = try queryRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
        .identity_read_generation = 88,
    });
    defer fallback_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.vector_worker_calls);
    try std.testing.expectEqual(@as(usize, 1), state.query_calls);
    try std.testing.expectEqual(@as(?u64, 88), fallback_result.identity_read_generation);
}

test "remote query rejects resolved doc filters before vector worker encoding" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 }) };
    defer filter.deinit(alloc);

    const ExecutorState = struct {
        calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedHttpRequest;
        }
    };

    var state = ExecutorState{};
    try std.testing.expect((try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
    })) == null);
    try std.testing.expectError(error.UnsupportedQueryRequest, queryRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
    }));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
}

test "remote preflight rejects resolved doc filters before query encoding" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 }) };
    defer filter.deinit(alloc);

    const ExecutorState = struct {
        calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedHttpRequest;
        }
    };

    var state = ExecutorState{};
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
    }, 0));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
}

test "encode query request rejects in-memory resolved doc filters" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}) };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.UnsupportedQueryRequest, encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .resolved_doc_filter = &filter,
    }));
}

test "encode query request serializes internal resolved doc filters with wire context" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 3 }) };
    defer filter.deinit(alloc);

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .identity_read_generation = 42,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    });
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"_resolved_doc_filter\"") != null);

    var parsed = try query_contract.parseQueryRequest(alloc, null, "docs", encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.req.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), parsed.req.identity_read_generation);
    try std.testing.expect(parsed.req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));

    const stats_body = try encodeQueryTextStatsRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .identity_read_generation = 42,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    });
    defer alloc.free(stats_body);
    try std.testing.expect(std.mem.indexOf(u8, stats_body, "\"_resolved_doc_filter\"") != null);

    var stats_parsed = try parseTextStatsRequest(alloc, "docs", stats_body);
    defer stats_parsed.deinit(alloc);
    const stats_query = stats_parsed.query_request.req;
    try std.testing.expect(stats_query.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), stats_query.identity_read_generation);
    try std.testing.expect(stats_query.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
}

test "simple vector shard request carries serializable resolved doc filter" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 3 }) };
    defer filter.deinit(alloc);

    const body = (try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .identity_read_generation = 42,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    })) orelse return error.TestUnexpectedResult;
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"_resolved_doc_filter\"") != null);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, body);
    defer envelope.deinit(alloc);
    const req = searchRequestFromVectorWorkerEnvelope(&envelope);
    try std.testing.expect(req.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), req.identity_read_generation);
    try std.testing.expect(req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
}

test "encode query request preserves empty positive internal doc id filter" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids_positive = true,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const constraints = parsed.value.object.get("native_doc_id_constraints").?.object;
    try std.testing.expectEqual(true, constraints.get("positive_filter").?.bool);
    try std.testing.expectEqual(@as(usize, 0), constraints.get("include_doc_ids").?.array.items.len);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expect(owned.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 0), owned.req.filter_doc_ids.len);
}

test "explicit text stats requests preserve identity generation" {
    const alloc = std.testing.allocator;

    var terms = [_][]const u8{"alpha"};
    const explicit_items = [_]OwnedTextStatsFieldRequest{.{
        .index_name = "text_v1",
        .field = "body",
        .terms = terms[0..],
    }};
    const explicit_body = try encodeExplicitTextStatsRequest(alloc, explicit_items[0..], 42);
    defer alloc.free(explicit_body);
    try std.testing.expect(std.mem.indexOf(u8, explicit_body, "\"_identity_read_generation\":42") != null);

    var parsed_explicit = try parseTextStatsRequest(alloc, "docs", explicit_body);
    defer parsed_explicit.deinit(alloc);
    switch (parsed_explicit) {
        .explicit_fields => |parsed| {
            try std.testing.expectEqual(@as(?u64, 42), parsed.identity_read_generation);
            try std.testing.expectEqual(@as(usize, 1), parsed.items.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var bg_terms = [_][]const u8{"beta"};
    const background_items = [_]OwnedBackgroundTextStatsFieldRequest{.{
        .aggregation_name = "sig",
        .index_name = "text_v1",
        .field = "body",
        .terms = bg_terms[0..],
        .background_query = .match_all,
    }};
    const background_body = try encodeBackgroundTextStatsRequest(alloc, background_items[0..], 43);
    defer alloc.free(background_body);
    try std.testing.expect(std.mem.indexOf(u8, background_body, "\"_identity_read_generation\":43") != null);

    var parsed_background = try parseTextStatsRequest(alloc, "docs", background_body);
    defer parsed_background.deinit(alloc);
    switch (parsed_background) {
        .background_fields => |parsed| {
            try std.testing.expectEqual(@as(?u64, 43), parsed.identity_read_generation);
            try std.testing.expectEqual(@as(usize, 1), parsed.items.len);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "explicit text stats requests carry resolved doc filters and apply exact projection" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-text-stats-resolved-doc-filter";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();

    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha beta\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta gamma\"}" },
        },
        .sync_level = .full_index,
        .timestamp_ns = 1,
    });

    const generation = try db.currentIdentityReadGenerationForRequest(null);
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);
    const req = db_mod.types.SearchRequest{
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = db.core.identity_namespace,
            .identity_read_generation = generation,
        },
    };

    var terms = [_][]const u8{ "alpha", "beta", "gamma" };
    const explicit_items = [_]OwnedTextStatsFieldRequest{.{
        .index_name = "full_text_index_v0",
        .field = "body",
        .terms = terms[0..],
    }};
    const explicit_body = try encodeExplicitTextStatsRequestForSearchRequest(alloc, explicit_items[0..], req);
    defer alloc.free(explicit_body);
    try std.testing.expect(std.mem.indexOf(u8, explicit_body, "\"_resolved_doc_filter\"") != null);

    var parsed_explicit = try parseTextStatsRequest(alloc, "docs", explicit_body);
    defer parsed_explicit.deinit(alloc);
    const stats = try collectTextStatsFromDbForRequest(alloc, &db, parsed_explicit);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.len);
    try std.testing.expectEqual(@as(u32, 1), stats[0].global_doc_count);
    try std.testing.expectEqual(@as(u32, 1), stats[0].termDocFreq("alpha").?);
    try std.testing.expectEqual(@as(u32, 1), stats[0].termDocFreq("beta").?);
    try std.testing.expectEqual(@as(u32, 0), stats[0].termDocFreq("gamma").?);

    const background_items = [_]OwnedBackgroundTextStatsFieldRequest{.{
        .aggregation_name = "sig_body",
        .index_name = "full_text_index_v0",
        .field = "body",
        .terms = terms[0..],
        .background_query = .match_all,
    }};
    const background_body = try encodeBackgroundTextStatsRequestForSearchRequest(alloc, background_items[0..], req);
    defer alloc.free(background_body);
    try std.testing.expect(std.mem.indexOf(u8, background_body, "\"_resolved_doc_filter\"") != null);

    var parsed_background = try parseTextStatsRequest(alloc, "docs", background_body);
    defer parsed_background.deinit(alloc);
    const background_stats = try collectBackgroundTextStatsFromDbForRequest(alloc, &db, parsed_background);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, background_stats);
    try std.testing.expectEqual(@as(usize, 1), background_stats.len);
    try std.testing.expectEqual(@as(u32, 1), background_stats[0].background_doc_count);
    const alpha_bg = for (background_stats[0].term_doc_freqs) |item| {
        if (std.mem.eql(u8, item.term, "alpha")) break item.doc_freq;
    } else return error.TestUnexpectedResult;
    const beta_bg = for (background_stats[0].term_doc_freqs) |item| {
        if (std.mem.eql(u8, item.term, "beta")) break item.doc_freq;
    } else return error.TestUnexpectedResult;
    const gamma_bg = for (background_stats[0].term_doc_freqs) |item| {
        if (std.mem.eql(u8, item.term, "gamma")) break item.doc_freq;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), alpha_bg);
    try std.testing.expectEqual(@as(u32, 1), beta_bg);
    try std.testing.expectEqual(@as(u32, 0), gamma_bg);
}

test "explicit text stats requests reject stale identity generation" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-text-stats-stale-identity-generation";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();

    const future_generation = db.core.nextDerivedSequence() + 1;
    const explicit_body = try std.fmt.allocPrint(alloc,
        \\{{"_identity_read_generation":{d},"fields":[{{"index_name":"text_v1","field":"body","terms":["alpha"]}}]}}
    , .{future_generation});
    defer alloc.free(explicit_body);

    var parsed_explicit = try parseTextStatsRequest(alloc, "docs", explicit_body);
    defer parsed_explicit.deinit(alloc);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectTextStatsFromDbForRequest(alloc, &db, parsed_explicit));

    const background_body = try std.fmt.allocPrint(alloc,
        \\{{"_identity_read_generation":{d},"background_fields":[{{"aggregation_name":"sig","index_name":"text_v1","field":"body","terms":["alpha"],"background_query":{{"match_all":{{}}}}}}]}}
    , .{future_generation});
    defer alloc.free(background_body);

    var parsed_background = try parseTextStatsRequest(alloc, "docs", background_body);
    defer parsed_background.deinit(alloc);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectBackgroundTextStatsFromDbForRequest(alloc, &db, parsed_background));
}

test "algebraic partial request preserves planner-owned materialization tensor programs" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"customer","path":"customer","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]}
    );
    defer index.close();

    const materializations = [_][]const u8{"sum_by_customer"};
    var program_plan = (try algebraic_planner.planMaterializationPartialsTensorProgramAlloc(alloc, &index, &materializations)) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);

    const encoded = try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, "alg", 91, program_plan.access_paths, &.{}, program_plan.asProgram());
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"_identity_read_generation\":91") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_access_paths\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_exprs\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"materialized_tensor\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"law_ids\":[\"sum\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"law_id\":\"sum\"") != null);

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 91), parsed.identity_read_generation);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.tensor_exprs.len);
    try std.testing.expect(parsed.tensor_program != null);
    try validateAlgebraicProgramPartialsProof(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?);

    parsed.tensor_access_paths[0].law_ids[0] = .count;
    try std.testing.expectError(error.InvalidQueryRequest, validateAlgebraicProgramPartialsProof(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?));
    parsed.tensor_access_paths[0].law_ids[0] = .sum;
    try std.testing.expectError(error.UnknownField, parseAlgebraicPartialsRequest(alloc, "{\"index_name\":\"alg\",\"materializations\":[\"sum_by_customer\"]}"));
}

test "algebraic partial request rejects legacy cardinality bodies" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"cardinality\":{\"aggregation_name\":\"x\",\"field\":\"y\"}}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"terms_cardinality\":{\"aggregation_name\":\"x\",\"bucket_field\":\"y\",\"children\":[]}}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"range_cardinality\":{\"aggregation_name\":\"x\",\"field\":\"amount\",\"kind\":\"numeric\",\"ranges\":[],\"children\":[]}}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"histogram_cardinality\":{\"aggregation_name\":\"x\",\"field\":\"amount\",\"kind\":\"numeric\",\"interval\":10,\"children\":[]}}",
    ));
}

test "aggregation context rejects non-current identity generation" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-aggregation-context-identity-generation";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"v\":1}" }},
        .sync_level = .write,
    });

    const current = db.core.nextDerivedSequence();
    const ctx = try aggregationContextForDb(alloc, .{ .identity_read_generation = current }, &db);
    try std.testing.expectEqual(@as(?u64, current), ctx.identity_read_generation);
    try std.testing.expectError(error.UnsupportedQueryRequest, aggregationContextForDb(alloc, .{
        .identity_read_generation = current + 1,
    }, &db));
}

test "aggregation full-result rerun can reuse snapped result identity generation" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a") };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
    };
    defer result.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, identityGenerationForAggregationFullResultRerun(.{}, result));
    try std.testing.expectEqual(@as(?u64, 9), try identityGenerationForAggregationFullResultRerun(.{ .identity_read_generation = 9 }, result));
    result.identity_read_generation = 11;
    try std.testing.expectEqual(@as(?u64, 11), try identityGenerationForAggregationFullResultRerun(.{}, result));
    try std.testing.expectEqual(@as(?u64, 9), try identityGenerationForAggregationFullResultRerun(.{ .identity_read_generation = 9 }, result));
    try std.testing.expectEqual(@as(?u64, 11), requestWithResultIdentityGeneration(.{}, result).identity_read_generation);
    try std.testing.expectEqual(@as(?u64, 9), requestWithResultIdentityGeneration(.{ .identity_read_generation = 9 }, result).identity_read_generation);

    const complete = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
    };
    try std.testing.expectEqual(@as(?u64, null), try identityGenerationForAggregationFullResultRerun(.{}, complete));
}

test "provisioned distributed aggregations collect path terms nested cardinality" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-api-provisioned-algebraic-path-terms-cardinality-{d}", .{platform_time.monotonicNs()});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const cfg =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "docs",
        \\  "group_fields": [{"name":"product","path":"product","type":"string"}],
        \\  "materializations": []
        \\}
    ;

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    var left_db = try db_mod.DB.open(alloc, left_path, .{
        .start_index_workers = false,
        .identity_namespace = .{
            .table_id = 7,
            .shard_id = 7001,
            .range_id = 7001,
        },
    });
    defer left_db.close();
    try left_db.addIndex(.{ .name = "alg", .kind = .algebraic, .config_json = cfg });
    try left_db.batch(.{
        .writes = &.{
            .{ .key = "l1", .value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"gold\"}}" },
            .{ .key = "l2", .value = "{\"product\":\"book\",\"meta\":{\"tier\":\"gold\"}}" },
            .{ .key = "l3", .value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"}}" },
        },
        .sync_level = .full_index,
    });

    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);
    var right_db = try db_mod.DB.open(alloc, right_path, .{
        .start_index_workers = false,
        .identity_namespace = .{
            .table_id = 7,
            .shard_id = 7002,
            .range_id = 7002,
        },
    });
    defer right_db.close();
    try right_db.addIndex(.{ .name = "alg", .kind = .algebraic, .config_json = cfg });
    try right_db.batch(.{
        .writes = &.{
            .{ .key = "r1", .value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"}}" },
            .{ .key = "r2", .value = "{\"product\":\"notebook\",\"meta\":{\"tier\":\"silver\"}}" },
        },
        .sync_level = .full_index,
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

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
                    .indexes_json =
                    \\{"alg":{"version":1,"table":"docs","schema_version":1,"group_fields":[{"name":"product","path":"product","type":"string"}],"materializations":[]}}
                    ,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "r" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "r", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var group_ids = [_]u64{ 7001, 7002 };
    var meta: query_api.QueryResponseMeta = .{};
    defer meta.deinit(alloc);
    const req = db_mod.types.SearchRequest{
        .index_name = "alg",
        .aggregations_json =
        \\{"by_tier":{"type":"terms","field":"/meta/tier","sub_aggregations":{"product_cardinality":{"type":"cardinality","field":"product"},"tier_cardinality":{"type":"cardinality","field":"/meta/tier"}}}}
        ,
    };
    try std.testing.expect(try tryApplyProvisionedAlgebraicDistributedAggregations(&source, alloc, group_ids[0..], "docs", req, &meta));
    var stamped_meta: query_api.QueryResponseMeta = .{};
    defer stamped_meta.deinit(alloc);
    const current_generation = left_db.core.nextDerivedSequence();
    try std.testing.expectEqual(current_generation, right_db.core.nextDerivedSequence());
    var stamped_req = req;
    stamped_req.identity_read_generation = current_generation;
    try std.testing.expect(try tryApplyProvisionedAlgebraicDistributedAggregations(&source, alloc, group_ids[0..], "docs", stamped_req, &stamped_meta));

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

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    var hosted_meta: query_api.QueryResponseMeta = .{};
    defer hosted_meta.deinit(alloc);
    try std.testing.expect(try tryApplyHostedAlgebraicDistributedAggregations(&hosted, alloc, group_ids[0..], "docs", stamped_req, &hosted_meta, .read_index));
    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);

    try std.testing.expectEqual(@as(usize, 1), meta.aggregation_results.len);
    const aggregation = meta.aggregation_results[0];
    try std.testing.expectEqualStrings("by_tier", aggregation.name);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"silver\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"gold\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregation.buckets[1].aggregations[1].value_json.?);
}

test "algebraic partial request accepts expression cache proofs without named materializations" {
    const alloc = std.testing.allocator;

    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "expr_sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)).?;
    defer plan.deinit(alloc);

    const encoded = try encodeAlgebraicExpressionPartialsRequest(alloc, "alg", &.{plan.access_path}, &.{expr});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_access_paths\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_exprs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"materialized_expr\"") != null);

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_exprs.len);
    try validateAlgebraicPartialsAccessPaths(alloc, parsed.tensor_access_paths, parsed.tensor_exprs);

    parsed.tensor_access_paths[0].owner[0] = if (parsed.tensor_access_paths[0].owner[0] == 'x') 'y' else 'x';
    try std.testing.expectError(error.InvalidQueryRequest, validateAlgebraicPartialsAccessPaths(alloc, parsed.tensor_access_paths, parsed.tensor_exprs));
}

test "algebraic partial request fails closed when lifecycle is stale" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-algebraic-partials-stale-lifecycle";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "orders",
        \\  "capability_lifecycle_status": "rebuild_required",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"keyword"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]
        \\}
        ,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "o1", .value = "{\"customer\":\"alice\",\"amount\":10}" }},
        .sync_level = .write,
    });

    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)).?;
    defer plan.deinit(alloc);
    const encoded = try encodeAlgebraicExpressionPartialsRequest(alloc, "alg", &.{plan.access_path}, &.{expr});
    defer alloc.free(encoded);
    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed));
}

test "algebraic partial request accepts current identity generation and rejects stale" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-algebraic-partials-stale-identity-generation";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"keyword"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]
        \\}
        ,
    });
    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)).?;
    defer plan.deinit(alloc);
    const encoded = try encodeAlgebraicExpressionPartialsRequest(alloc, "alg", &.{plan.access_path}, &.{expr});
    defer alloc.free(encoded);

    var unstamped = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer unstamped.deinit(alloc);
    const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, unstamped);
    defer db_mod.algebraic.distributed.freePartials(alloc, partials);

    var stamped = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer stamped.deinit(alloc);
    stamped.identity_read_generation = db.core.nextDerivedSequence();
    const stamped_partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, stamped);
    defer db_mod.algebraic.distributed.freePartials(alloc, stamped_partials);

    var stale = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer stale.deinit(alloc);
    stale.identity_read_generation = db.core.nextDerivedSequence() + 1;
    try std.testing.expectError(error.UnsupportedQueryRequest, collectAlgebraicPartialsFromDbForRequest(alloc, &db, stale));
}

test "algebraic partial request accepts tensor program expression outputs" {
    const alloc = std.testing.allocator;

    const count_expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{.doc},
        .output_dims = &.{.bucket},
        .semantic_id = "expr_count_by_customer",
        .layout = .materialized_expr,
        .law_id = .count,
    };
    const sum_expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "expr_sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var count_plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, count_expr)).?;
    defer count_plan.deinit(alloc);
    var sum_plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, sum_expr)).?;
    defer sum_plan.deinit(alloc);
    const access_paths = [_]algebraic_ir.PhysicalAccessPath{ count_plan.access_path, sum_plan.access_path };
    const steps = [_]algebraic_ir.TensorProgramStep{ .{ .expr = count_expr }, .{ .expr = sum_expr } };
    const outputs = [_]algebraic_ir.TensorProgramRef{ .{ .step = 0 }, .{ .step = 1 } };
    const program = algebraic_ir.TensorProgram{
        .steps = &steps,
        .output = .{ .step = 0 },
        .outputs = &outputs,
    };
    try std.testing.expect((try algebraic_ir.tensorProgramProof(alloc, &access_paths, program)).safe());

    const encoded = try encodeAlgebraicPartialsRequestWithProgram(alloc, "alg", &access_paths, &.{}, program);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"outputs\"") != null);
    const mixed_encoded = try encodeAlgebraicPartialsRequestWithProgram(alloc, "alg", &access_paths, &.{count_expr}, program);
    defer alloc.free(mixed_encoded);
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(alloc, mixed_encoded));

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.tensor_exprs.len);
    try std.testing.expect(parsed.tensor_program != null);
    try validateAlgebraicProgramPartialsAccessPaths(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?);
    const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, null, parsed.tensor_access_paths, &parsed.tensor_program.?);
    defer alloc.free(exprs);
    try std.testing.expectEqual(@as(usize, 2), exprs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, exprs[0].fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, exprs[0].law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, exprs[1].law_id.?);

    parsed.tensor_access_paths[1].law_ids[0] = .max;
    try std.testing.expectError(error.InvalidQueryRequest, validateAlgebraicProgramPartialsAccessPaths(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?));
}

test "algebraic partial request derives expression outputs from materialized tensor program" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"customer","path":"customer","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[
        \\   {"name":"customers","op":"count","group_by":["customer"]},
        \\   {"name":"amount_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}
        \\ ]}
    );
    defer index.close();

    var program_plan = (try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "customers",
        .bucket_field = "customer",
        .child_metrics = &.{.{ .name = "amount_by_customer", .op = .sum, .field = "amount" }},
    })).?;
    defer program_plan.deinit(alloc);
    const encoded = try encodeAlgebraicPartialsRequestWithProgram(alloc, "alg", program_plan.access_paths, &.{}, program_plan.asProgram());
    defer alloc.free(encoded);

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, &index, parsed.tensor_access_paths, &parsed.tensor_program.?);
    defer alloc.free(exprs);
    try std.testing.expectEqual(@as(usize, 2), exprs.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.materialized_expr, exprs[0].layout.?);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.materialized_expr, exprs[1].layout.?);
    try std.testing.expectEqualStrings("customers", exprs[0].semantic_id.?);
    try std.testing.expectEqualStrings("amount_by_customer", exprs[1].semantic_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, exprs[0].law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, exprs[1].law_id.?);
}

test "algebraic distributed planner selects derived join tensor program for metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "joined_amount",
        .type = "sum",
        .field = "amount",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("joined_amount", program_plan.steps[1].expr.semantic_id.?);
}

test "algebraic distributed planner selects identity-stamped derived join tensor program" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "joined_amount",
        .type = "sum",
        .field = "amount",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var unstamped_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &index, request, &.{}, null)) orelse return error.TestUnexpectedResult;
    defer unstamped_plan.deinit(alloc);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, unstamped_plan.steps[1].expr.fragment);
    var stamped_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &index, request, &.{}, 42)) orelse return error.TestUnexpectedResult;
    defer stamped_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), stamped_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, stamped_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), stamped_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, stamped_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, stamped_plan.steps[1].expr.law_id.?);
}

test "algebraic distributed planner selects derived join tensor program for terms child metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "sum_amount",
        .type = "sum",
        .field = "amount",
    }};
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "customer", .value = "c1" }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "regions",
        .type = "terms",
        .field = "region",
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("regions", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[2].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqualStrings("sum_amount", program_plan.steps[2].expr.semantic_id.?);
}

test "algebraic distributed planner selects derived join tensor program for histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 20,
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
}

test "algebraic distributed planner selects derived join tensor program for range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 40 },
        },
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 5), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 4), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[2].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);

    var low_count = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, low_count.request.op);
    try std.testing.expectEqual(db_mod.algebraic.index.DerivedJoinRangeKind.numeric, low_count.request.range_kind.?);
    try std.testing.expectEqual(db_mod.algebraic.fact.Role.measure, low_count.request.range_role.?);
    try std.testing.expectEqualStrings("amount", low_count.request.range_field.?);
    try std.testing.expectEqualStrings("0", low_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_count.request.range_end.?);

    var low_sum = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer low_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, low_sum.request.op);
    try std.testing.expectEqualStrings("amount", low_sum.request.measure.?);
}

test "algebraic distributed planner selects derived join tensor program for date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "may_1", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "may_2", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 5), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 4), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);

    var first_count = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, first_count.request.op);
    try std.testing.expectEqual(db_mod.algebraic.index.DerivedJoinRangeKind.date, first_count.request.range_kind.?);
    try std.testing.expectEqual(db_mod.algebraic.fact.Role.time, first_count.request.range_role.?);
    try std.testing.expectEqualStrings("created_at", first_count.request.range_field.?);
    try std.testing.expectEqualStrings("2026-05-01T00:00:00Z", first_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first_count.request.range_end.?);
}

test "algebraic distributed planner selects docfact tensor program for measure histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expect(program_plan.steps[1].expr.metadata != null);

    var fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, fold.request.op);
    try std.testing.expectEqualStrings("amount", fold.request.bucket_field);
    try std.testing.expectEqual(@as(f64, 10), fold.request.histogram_interval);
    try std.testing.expectEqual(@as(usize, 1), fold.request.constraints.len);
    try std.testing.expectEqualStrings("tenant", fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", fold.request.constraints[0].value);
}

test "algebraic distributed planner selects docfact tensor program for measure histogram child metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expectEqualStrings("amount_sum", program_plan.steps[2].expr.semantic_id.?);

    var count_fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, count_fold.request.op);
    try std.testing.expect(count_fold.request.measure == null);
    var sum_fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, sum_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqualStrings("amount", sum_fold.request.measure.?);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[3].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[4].expr.law_id.?);

    var count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, count_fold.request.op);
    try std.testing.expectEqualStrings("/amount", count_fold.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, count_fold.request.bucket_kind);
    try std.testing.expectEqual(@as(usize, 1), count_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", count_fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", count_fold.request.constraints[0].value);
    var sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqualStrings("/amount", sum_fold.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.measure_kind);
    var number_string_sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer number_string_sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, number_string_sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, number_string_sum_fold.request.measure_kind);
    var string_count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer string_count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_count_fold.request.bucket_kind);
    var string_sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer string_sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_sum_fold.request.measure_kind);
}

test "algebraic distributed planner honors pathfact numeric-string policy" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"pathfact_policy":{"allow_numeric_string_coercion":false},"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);

    var count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, count_fold.request.bucket_kind);

    var sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.measure_kind);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless terms" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "tier_terms",
        .type = "terms",
        .field = "/tier",
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);

    var string_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer string_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, string_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, string_fold.request.op);
    try std.testing.expectEqualStrings("/tier", string_fold.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_fold.request.bucket_kind);
    try std.testing.expectEqual(@as(usize, 1), string_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", string_fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", string_fold.request.constraints[0].value);

    var number_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer number_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, number_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, number_fold.request.bucket_kind);

    var bool_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer bool_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, bool_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.bool, bool_fold.request.bucket_kind);

    var null_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer null_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, null_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.null, null_fold.request.bucket_kind);

    var object_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[5].expr.metadata.?);
    defer object_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, object_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.object, object_fold.request.bucket_kind);

    var array_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer array_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, array_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.array, array_fold.request.bucket_kind);
}

test "algebraic distributed planner carries same-path disjunction as pathfact any constraint" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "tier_terms",
        .type = "terms",
        .field = "/tier",
    };
    const constraints = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"/tenant\":\"t1\"}},{\"term\":{\"/tenant\":\"t2\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, constraints);
    try std.testing.expectEqual(@as(usize, 1), constraints.len);

    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints)) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);

    var string_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer string_fold.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), string_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", string_fold.request.constraints[0].field);
    const parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, string_fold.request.constraints[0].value);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", parts[0]);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "/amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 13), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 12), program_plan.outputs.len);

    var low_number_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low_number_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, low_number_count.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, low_number_count.request.op);
    try std.testing.expectEqualStrings("/amount", low_number_count.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_count.request.bucket_kind);
    try std.testing.expectEqualStrings("0", low_number_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_number_count.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), low_number_count.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", low_number_count.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", low_number_count.request.constraints[0].value);

    var low_number_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer low_number_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, low_number_sum.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_sum.request.bucket_kind);
    try std.testing.expectEqualStrings("/amount", low_number_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_sum.request.measure_kind);

    var low_number_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer low_number_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_number_string_sum.request.measure_kind);

    var low_string_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer low_string_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_count.request.bucket_kind);
    try std.testing.expectEqualStrings("0", low_string_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_string_count.request.range_end.?);

    var low_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer low_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_sum.request.measure_kind);

    var high_number_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[7].expr.metadata.?);
    defer high_number_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, high_number_count.request.bucket_kind);
    try std.testing.expectEqualStrings("20", high_number_count.request.range_start.?);
    try std.testing.expectEqualStrings("30", high_number_count.request.range_end.?);

    var high_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[12].expr.metadata.?);
    defer high_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, high_string_sum.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, high_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, high_string_sum.request.measure_kind);
    try std.testing.expectEqualStrings("20", high_string_sum.request.range_start.?);
    try std.testing.expectEqualStrings("30", high_string_sum.request.range_end.?);
}

test "algebraic distributed planner selects multi-output docfact tensor program for measure range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[2].expr.fragment);

    var low = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, low.request.kind);
    try std.testing.expectEqualStrings("0", low.request.range_start.?);
    try std.testing.expectEqualStrings("20", low.request.range_end.?);
    var high = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer high.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, high.request.kind);
    try std.testing.expectEqualStrings("20", high.request.range_start.?);
    try std.testing.expectEqualStrings("30", high.request.range_end.?);
}

test "algebraic distributed planner selects multi-output docfact tensor program for date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T12:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[2].expr.fragment);

    var first = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, first.request.kind);
    try std.testing.expectEqualStrings("2026-05-01T12:00:00Z", first.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), first.request.constraints.len);
    try std.testing.expectEqualStrings("tenant", first.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", first.request.constraints[0].value);
    var second = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer second.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, second.request.kind);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", second.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-03T00:00:00Z", second.request.range_end.?);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);

    var first_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, first_count.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, first_count.request.op);
    try std.testing.expectEqualStrings("/created_at", first_count.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, first_count.request.bucket_kind);
    try std.testing.expectEqualStrings("2026-05-01T00:00:00Z", first_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first_count.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), first_count.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", first_count.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", first_count.request.constraints[0].value);

    var first_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer first_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, first_sum.request.op);
    try std.testing.expectEqualStrings("/amount", first_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, first_sum.request.measure_kind);

    var first_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer first_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, first_string_sum.request.op);
    try std.testing.expectEqualStrings("/amount", first_string_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, first_string_sum.request.measure_kind);

    var second_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer second_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, second_count.request.kind);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", second_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-03T00:00:00Z", second_count.request.range_end.?);

    var second_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer second_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, second_string_sum.request.op);
    try std.testing.expectEqualStrings("/amount", second_string_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, second_string_sum.request.measure_kind);
}

test "algebraic distributed planner honors pathfact datetime-string policy" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"pathfact_policy":{"allow_datetime_string_coercion":false},"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
        },
    };
    const plan = try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{});
    try std.testing.expect(plan == null);
}

test "merge distributed text stats sums shard corpus stats by field and term" {
    const alloc = std.testing.allocator;

    const merged = try mergeDistributedTextStats(alloc, &.{
        &.{.{
            .field = "body",
            .global_doc_count = 2,
            .global_total_field_len = 9,
            .term_doc_freqs = &.{
                .{ .term = "alpha", .doc_freq = 2 },
                .{ .term = "beta", .doc_freq = 1 },
            },
        }},
        &.{
            .{
                .field = "body",
                .global_doc_count = 3,
                .global_total_field_len = 15,
                .term_doc_freqs = &.{
                    .{ .term = "alpha", .doc_freq = 1 },
                    .{ .term = "gamma", .doc_freq = 2 },
                },
            },
            .{
                .field = "title",
                .global_doc_count = 3,
                .global_total_field_len = 12,
                .term_doc_freqs = &.{
                    .{ .term = "hello", .doc_freq = 3 },
                },
            },
        },
    });
    defer distributed_stats_mod.deinitTextFieldStats(alloc, merged);

    try std.testing.expectEqual(@as(usize, 2), merged.len);

    const body = for (merged) |item| {
        if (std.mem.eql(u8, item.field, "body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(u32, 5), body.global_doc_count);
    try std.testing.expectEqual(@as(u64, 24), body.global_total_field_len);
    try std.testing.expectEqual(@as(?u32, 3), body.termDocFreq("alpha"));
    try std.testing.expectEqual(@as(?u32, 1), body.termDocFreq("beta"));
    try std.testing.expectEqual(@as(?u32, 2), body.termDocFreq("gamma"));

    const title = for (merged) |item| {
        if (std.mem.eql(u8, item.field, "title")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(u32, 3), title.global_doc_count);
    try std.testing.expectEqual(@as(?u32, 3), title.termDocFreq("hello"));
}

test "merge distributed background text stats keys preserve embedded separators" {
    const alloc = std.testing.allocator;

    const merged = try mergeDistributedBackgroundTextStats(alloc, &.{
        &.{.{
            .aggregation_name = "agg\x1ffield",
            .field = "name",
            .background_doc_count = 2,
            .term_doc_freqs = &.{.{ .term = "alpha", .doc_freq = 2 }},
        }},
        &.{.{
            .aggregation_name = "agg",
            .field = "field\x1fname",
            .background_doc_count = 3,
            .term_doc_freqs = &.{.{ .term = "beta", .doc_freq = 3 }},
        }},
    });
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, merged);

    try std.testing.expectEqual(@as(usize, 2), merged.len);

    const left = for (merged) |item| {
        if (std.mem.eql(u8, item.aggregation_name, "agg\x1ffield")) break item;
    } else unreachable;
    try std.testing.expectEqualStrings("name", left.field);
    try std.testing.expectEqual(@as(u32, 2), left.background_doc_count);
    try std.testing.expectEqual(@as(?u32, 2), backgroundTermDocFreq(left.term_doc_freqs, "alpha"));

    const right = for (merged) |item| {
        if (std.mem.eql(u8, item.aggregation_name, "agg")) break item;
    } else unreachable;
    try std.testing.expectEqualStrings("field\x1fname", right.field);
    try std.testing.expectEqual(@as(u32, 3), right.background_doc_count);
    try std.testing.expectEqual(@as(?u32, 3), backgroundTermDocFreq(right.term_doc_freqs, "beta"));
}

fn backgroundTermDocFreq(items: []const distributed_stats_mod.TermDocFreq, term: []const u8) ?u32 {
    for (items) |item| {
        if (std.mem.eql(u8, item.term, term)) return item.doc_freq;
    }
    return null;
}

test "collect significant terms field requests gathers unique field terms from hits" {
    const alloc = std.testing.allocator;

    const hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha beta\",\"nested\":{\"body\":\"beta gamma\"}}"),
    };
    hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha\",\"nested\":{\"body\":\"gamma\"}}"),
    };

    const requests = [_]db_mod.aggregations.SearchAggregationRequest{
        .{
            .name = "sig_body",
            .type = "significant_terms",
            .field = "body",
        },
        .{
            .name = "outer_terms",
            .type = "terms",
            .field = "status",
            .aggregations = &.{
                .{
                    .name = "nested_sig_body",
                    .type = "significant_terms",
                    .field = "nested.body",
                },
            },
        },
    };

    const field_requests = try collectSignificantTermsFieldRequests(alloc, &requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }

    try std.testing.expectEqual(@as(usize, 2), field_requests.len);

    const body = for (field_requests) |item| {
        if (std.mem.eql(u8, item.field, "body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(usize, 2), body.terms.len);
    try std.testing.expect(std.mem.eql(u8, body.terms[0], "alpha") or std.mem.eql(u8, body.terms[1], "alpha"));
    try std.testing.expect(std.mem.eql(u8, body.terms[0], "beta") or std.mem.eql(u8, body.terms[1], "beta"));

    const nested = for (field_requests) |item| {
        if (std.mem.eql(u8, item.field, "nested.body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(usize, 2), nested.terms.len);
    try std.testing.expect(std.mem.eql(u8, nested.terms[0], "beta") or std.mem.eql(u8, nested.terms[1], "beta"));
    try std.testing.expect(std.mem.eql(u8, nested.terms[0], "gamma") or std.mem.eql(u8, nested.terms[1], "gamma"));
}

test "hosted textStatsGroupLocal serves only the local group" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-local-text-stats";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(group_path);
    var db = try db_mod.DB.open(test_alloc, group_path, .{});
    defer db.close();

    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"alpha beta\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"beta\"}" },
        },
        .sync_level = .full_index,
    });

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = null },
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

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7) 2 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            if (group_id == 7 and node_id == 2) return .active;
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            _ = alloc;
            _ = req;
            try std.testing.expect(false);
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );

    var response = (try hosted.source().textStatsGroupLocal(
        test_alloc,
        7,
        "docs",
        "{\"fields\":[{\"field\":\"body\",\"terms\":[\"alpha\"]}]}",
    )) orelse unreachable;
    defer response.deinit(test_alloc);

    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"global_doc_count\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"field\":\"body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"term\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"doc_freq\":2") != null);
}

test "hosted table read source preflights query locally" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-local-preflight";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(group_path);
    var db = try db_mod.DB.open(test_alloc, group_path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = null },
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

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            _ = alloc;
            _ = req;
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );

    try std.testing.expectError(error.InvalidArgument, hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0 }, .k = 5 },
    }, .read_index, 0));

    var summary = (try hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0)).?;
    defer summary.deinit(test_alloc);

    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);
    try std.testing.expectEqual(@as(usize, 1), summary.result_refs.len);
    try std.testing.expectEqualStrings("$embeddings_results", summary.result_refs[0]);
}

test "hosted table read source preflights every local group" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-local-preflight-multigroup";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 8);
    defer test_alloc.free(right_path);
    var left_db = try db_mod.DB.open(test_alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(test_alloc, right_path, .{});
    defer right_db.close();
    try left_db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 8, .table_id = 7, .start_key = "doc:m", .end_key = null },
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

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            _ = alloc;
            _ = req;
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    try std.testing.expectError(error.InvalidArgument, hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0));
    try std.testing.expectError(error.UnsupportedQueryRequest, hosted.source().preflightQuery(test_alloc, "docs", .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$embeddings_results", .limit = 1 } },
                    .params = .{ .edge_types = &.{}, .max_depth = 1 },
                },
            },
        },
    }, .read_index, 0));
    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);
}

test "hosted table read source preflights mixed local and remote groups" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-preflight-mixed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const local_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(local_path);
    var local_db = try db_mod.DB.open(test_alloc, local_path, .{});
    defer local_db.close();
    try local_db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 8, .table_id = 7, .start_key = "doc:m", .end_key = null },
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
            return if (group_id == 7) .active else .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7) 1 else 2;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            if (group_id == 8 and node_id == 2) return .active;
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/internal/v1/groups/8/tables/docs/query-preflight"));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            return .{
                .status = 400,
                .body = try alloc_inner.dupe(u8, "IndexNotFound"),
            };
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    try std.testing.expectError(error.IndexNotFound, hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0));
    try std.testing.expectEqual(@as(usize, 1), executor_state.call_count);
}

test "provisioned read cache keys entries by lsm root generation" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-cache-generation";

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
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
    const path = "/tmp/antfly-api-provisioned-read-cache-identity-namespace";

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
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

test "graph edge local read rejects stale identity generation" {
    const alloc = std.testing.allocator;
    const root = "/tmp/antfly-api-graph-edge-stale-identity-generation";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7107,
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

    const path = algebraic_ir.graphEdgeAccessPath("graph_v1");
    var req = distributed_graph.GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .out,
        .identity_read_generation = 999,
        .tensor_access_path = .{
            .owner = try alloc.dupe(u8, path.owner),
            .layout = path.layout,
            .fragments = try alloc.dupe(algebraic_ir.TensorFragment, path.fragments),
            .output_dims = try alloc.dupe(algebraic_ir.Dimension, path.output_dims),
            .law_ids = try alloc.dupe(algebraic_law.Id, path.law_ids),
        },
        .tensor_program = try distributed_graph.graphEdgesTensorProgramEnvelopeAlloc(alloc, "graph_v1"),
    };
    defer req.deinit(alloc);

    var catalog_state = CatalogState{};
    try std.testing.expectError(error.UnsupportedQueryRequest, graphGetEdgesLocal(
        alloc,
        root,
        catalog_state.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        7001,
        null,
        "docs",
        req,
        .stale,
    ));
}

test "graph edge local read rejects stale identity namespace" {
    const alloc = std.testing.allocator;
    const root = "/tmp/antfly-api-graph-edge-stale-identity-namespace";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, root, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .start_index_workers = false,
            .identity_namespace = .{
                .table_id = 7,
                .shard_id = 7001,
                .range_id = 7196,
            },
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
                    .indexes_json = @import("tables.zig").default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7107,
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

    const path = algebraic_ir.graphEdgeAccessPath("graph_v1");
    var req = distributed_graph.GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .out,
        .identity_read_generation = null,
        .tensor_access_path = .{
            .owner = try alloc.dupe(u8, path.owner),
            .layout = path.layout,
            .fragments = try alloc.dupe(algebraic_ir.TensorFragment, path.fragments),
            .output_dims = try alloc.dupe(algebraic_ir.Dimension, path.output_dims),
            .law_ids = try alloc.dupe(algebraic_law.Id, path.law_ids),
        },
        .tensor_program = try distributed_graph.graphEdgesTensorProgramEnvelopeAlloc(alloc, "graph_v1"),
    };
    defer req.deinit(alloc);

    var catalog_state = CatalogState{};
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, graphGetEdgesLocal(
        alloc,
        root,
        catalog_state.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        7001,
        null,
        "docs",
        req,
        .stale,
    ));
}

test "provisioned lookup db opens with identity namespace" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-lookup-identity-namespace";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7103,
    };
    var db = try openProvisionedLookupDbForTable(alloc, path, null, 0, null, null, namespace);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(namespace));
}

test "provisioned warm status db opens with identity namespace" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-warm-status-identity-namespace";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7104,
    };
    var db = try openProvisionedWarmStatusDbForTable(alloc, path, 0, null, namespace);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(namespace));
}

test "provisioned direct read db opens reject stale identity namespace" {
    const alloc = std.testing.allocator;
    const lookup_path = "/tmp/antfly-api-provisioned-lookup-stale-identity-namespace";
    const status_path = "/tmp/antfly-api-provisioned-status-stale-identity-namespace";

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
    if (openProvisionedLookupDbForTable(alloc, lookup_path, null, 0, null, null, expected_namespace)) |opened| {
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
    if (openProvisionedWarmStatusDbForTable(alloc, status_path, 0, null, expected_namespace)) |opened| {
        var db = opened;
        db.close();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.DocIdentityNamespaceMismatch, err);
}

test "provisioned query runtime db opens with catalog identity namespace" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-runtime-identity-namespace";

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
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
    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog_state.iface(), "docs", 7001, 0, null);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(.{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7105,
    }));
}

test "provisioned query runtime db rejects stale identity namespace" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-runtime-stale-identity-namespace";

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
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
    if (openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog_state.iface(), "docs", 7001, 0, null)) |opened| {
        var db = opened;
        db.close();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.DocIdentityNamespaceMismatch, err);
}

test "provisioned primary lookup lease fails on identity namespace mismatch" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-primary-lookup-identity-mismatch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7106,
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

    const PrimarySource = struct {
        db: *db_mod.DB,

        fn iface(self: *@This()) PrimaryLookupDbSource {
            return .{
                .ptr = self,
                .lease_group = leaseGroup,
            };
        }

        fn leaseGroup(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: u64,
            _: u64,
        ) !?PrimaryLookupDbLease {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .ptr = self.db,
                .db = self.db,
                .release_fn = release,
            };
        }

        fn release(_: *anyopaque, _: std.mem.Allocator) void {}
    };

    var catalog_state = CatalogState{};
    var primary_source = PrimarySource{ .db = &db };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, lookupProvisionedLocal(
        primary_source.iface(),
        null,
        "/tmp/unused-antfly-primary-lookup-mismatch",
        catalog_state.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        alloc,
        7001,
        0,
        null,
        "docs",
        "doc:a",
        .{},
        .stale,
    ));
}

test "provisioned read cache clear preserves in-flight pending opens and bumps epoch" {
    const alloc = std.testing.allocator;

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();

    try cache.pending_opens.append(alloc, .{
        .group_id = 7001,
        .table_name = try alloc.dupe(u8, "docs"),
    });

    const before_epoch = cache.epoch;
    cache.clear();

    try std.testing.expectEqual(before_epoch +% 1, cache.epoch);
    try std.testing.expectEqual(@as(usize, 1), cache.pending_opens.items.len);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expect(cache.hasPendingOpenLocked(7001, "docs"));
}

test "provisioned read cache invalidate removes entries without dropping pending opens" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-cache-invalidate";

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
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

    const before_epoch = cache.epoch;
    cache.invalidateTable("docs");

    try std.testing.expectEqual(before_epoch +% 1, cache.epoch);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expect(cache.hasPendingOpenLocked(7001, "docs"));
}

test "provisioned read cache retires invalidated entries until the last lease is released" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-cache-no-retire";

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
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
    const path = "/tmp/antfly-api-provisioned-read-cache-retire-oom";

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
                    .indexes_json = @import("tables.zig").default_indexes_json,
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
