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

//! Provisioned group-local read/write adapters for the compiled storage owner.
//! Distributed sources retain routing, admission, consistency, aggregation,
//! and lifecycle; this source owns only coarse physical operations.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const abi = @import("kernel_owner_abi");
const client = @import("../storage/kernel_owner_client.zig");
const db_types = @import("../storage/db/types.zig");
const runtime_preflight = @import("../storage/db/runtime_preflight.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const query_response = @import("query_response.zig");
const read_gate = @import("../raft/read_gate.zig");
const feature_reads = @import("../raft/feature_reads.zig");
const table_catalog = @import("table_catalog.zig");
const table_read_source = @import("table_read_source.zig");
const table_reads = @import("table_reads.zig");
const table_write_source = @import("table_write_source.zig");
const table_writes = @import("table_writes.zig");

pub const ProvisionedKernelOwnerSource = struct {
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: read_gate.ReadableLeaseRequester,
    group_visible_root_generation: ?table_reads.GroupVisibleRootGenerationSource = null,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,

    const Identity = struct {
        table_id: u64,
        shard_id: u64,
        range_id: u64,

        fn eql(left: Identity, right: Identity) bool {
            return left.table_id == right.table_id and
                left.shard_id == right.shard_id and
                left.range_id == right.range_id;
        }
    };

    const Descriptor = struct {
        path: []u8,
        schema_json: []u8,
        indexes_json: []u8,
        generation: u64,
        identity: Identity,

        fn deinit(self: *Descriptor, alloc: std.mem.Allocator) void {
            alloc.free(self.path);
            alloc.free(self.schema_json);
            alloc.free(self.indexes_json);
            self.* = undefined;
        }
    };

    const Entry = struct {
        group_id: u64,
        table_name: []u8,
        generation: u64,
        identity: Identity,
        owner: client.Owner,
        active_users: usize = 0,
    };

    const Lease = struct {
        source: *ProvisionedKernelOwnerSource,
        entry: *Entry,
        active: bool = true,

        fn owner(self: *Lease) *client.Owner {
            return &self.entry.owner;
        }

        fn deinit(self: *Lease) void {
            if (!self.active) return;
            lock(&self.source.mutex);
            std.debug.assert(self.entry.active_users > 0);
            self.entry.active_users -= 1;
            self.source.mutex.unlock();
            self.active = false;
        }
    };

    pub fn init(
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        requester: read_gate.ReadableLeaseRequester,
    ) ProvisionedKernelOwnerSource {
        return .{
            .alloc = alloc,
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .requester = requester,
        };
    }

    pub fn withGroupVisibleRootGeneration(
        self: *ProvisionedKernelOwnerSource,
        source: ?table_reads.GroupVisibleRootGenerationSource,
    ) *ProvisionedKernelOwnerSource {
        self.group_visible_root_generation = source;
        return self;
    }

    /// Call only after every attached read/write source has drained. Owner
    /// closure is deliberately centralized here so one live DB serves both
    /// operation families for its full group lifecycle.
    pub fn deinit(self: *ProvisionedKernelOwnerSource) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            std.debug.assert(entry.active_users == 0);
            entry.owner.deinit();
            self.alloc.free(entry.table_name);
            self.alloc.destroy(entry);
        }
        self.entries.deinit(self.alloc);
        self.entries = .empty;
    }

    pub fn readSource(self: *ProvisionedKernelOwnerSource) table_read_source.TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = unsupportedTopLevelLookup,
                .scan = unsupportedTopLevelScan,
                .query = unsupportedTopLevelQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
            },
        };
    }

    pub fn writeSource(self: *ProvisionedKernelOwnerSource) table_write_source.TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch = unsupportedTopLevelBatch,
                .batch_group_local = batchGroupLocal,
            },
        };
    }

    pub fn ownerCountForTest(self: *ProvisionedKernelOwnerSource) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.entries.items.len;
    }

    fn visibleRootGeneration(self: *const ProvisionedKernelOwnerSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |source|
            source.visibleRootGenerationForGroup(group_id)
        else
            table_reads.backend_current_root_generation;
    }

    fn lock(mutex: *std.atomic.Mutex) void {
        platform_sync.lockYielding(mutex);
    }

    fn loadDescriptor(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !Descriptor {
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);

        var table_record: ?metadata_table_manager.TableRecord = null;
        for (snapshot.tables) |table| {
            if (!std.mem.eql(u8, table.name, table_name)) continue;
            table_record = table;
            break;
        }
        const table = table_record orelse return error.TableNotFound;
        for (snapshot.ranges) |range| {
            if (range.table_id != table.table_id or range.group_id != group_id) continue;
            const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
            errdefer alloc.free(path);
            const schema_json = try alloc.dupe(u8, table.schema_json);
            errdefer alloc.free(schema_json);
            return .{
                .path = path,
                .schema_json = schema_json,
                .indexes_json = try alloc.dupe(u8, table.indexes_json),
                .generation = self.visibleRootGeneration(group_id),
                .identity = .{
                    .table_id = table.table_id,
                    .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
                    .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
                },
            };
        }
        return error.TableNotFound;
    }

    fn acquire(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !Lease {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);

        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.generation != descriptor.generation or !entry.identity.eql(descriptor.identity)) {
                return error.StorageKernelOwnerTransitionRequired;
            }
            entry.active_users += 1;
            return .{ .source = self, .entry = entry };
        }

        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(entry);
        var owner = try client.Owner.open(.{
            .path = abi.BorrowedBytes.fromSlice(descriptor.path),
            .table_name = abi.BorrowedBytes.fromSlice(table_name),
            .lsm_root_generation = descriptor.generation,
            .has_identity_namespace = 1,
            .identity_table_id = descriptor.identity.table_id,
            .identity_shard_id = descriptor.identity.shard_id,
            .identity_range_id = descriptor.identity.range_id,
            .schema_json = .fromSlice(descriptor.schema_json),
            .indexes_json = .fromSlice(descriptor.indexes_json),
        });
        errdefer owner.deinit();
        entry.* = .{
            .group_id = group_id,
            .table_name = owned_table_name,
            .generation = descriptor.generation,
            .identity = descriptor.identity,
            .owner = owner,
            .active_users = 1,
        };
        self.entries.appendAssumeCapacity(entry);
        return .{ .source = self, .entry = entry };
    }

    fn prepareQueryRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.requester);
        reads.prepareSearchWithConsistency(group_id, req, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareSearchWithConsistency(group_id, req, .stale),
            else => return err,
        };
    }

    fn prepareLookupRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.requester);
        reads.prepareLookupWithConsistency(group_id, key, opts, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareLookupWithConsistency(group_id, key, opts, .stale),
            else => return err,
        };
    }

    fn prepareScanRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.requester);
        reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, .stale),
            else => return err,
        };
    }

    fn executeQuery(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !client.Response {
        try self.prepareQueryRead(group_id, req, consistency);
        const request_json = try table_reads.encodeStorageKernelQueryRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try lease.owner().queryJson(table_name, request_json);
    }

    fn unsupportedTopLevelLookup(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: db_types.LookupOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelScan(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: []const u8,
        _: db_types.ScanOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelQuery(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.SearchRequest,
        _: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelBatch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.BatchRequest,
    ) !?void {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareLookupRead(group_id, key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = lease.owner().lookupJson(table_name, request_json) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return .{
            .json = try alloc.dupe(u8, response.bytes()),
            .version = response.version(),
        };
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareScanRead(group_id, from_key, to_key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelScanRequest(alloc, from_key, to_key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().scanNdjson(table_name, request_json);
        defer response.deinit();
        return .{ .ndjson = try alloc.dupe(u8, response.bytes()) };
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareQueryRead(group_id, req, consistency);
        const request_json = try table_reads.encodeStorageKernelPreflightRequest(alloc, req, max_work);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().preflightJson(table_name, request_json);
        defer response.deinit();
        var summary = try table_reads.parseStorageKernelPreflightSummary(alloc, response.bytes());
        table_reads.annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().batchJson(table_name, request_json);
        defer response.deinit();
        return {};
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().textStatsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().algebraicPartialsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.SearchResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency);
        defer response.deinit();
        return try table_reads.parseStorageKernelSearchResult(alloc, response.bytes());
    }
};
