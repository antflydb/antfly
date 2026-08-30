// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;
const metadata_api = @import("api.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_storage = @import("storage/raft_apply_store.zig");

pub const CatalogProjectionReader = struct {
    mutex: std.Io.Mutex = .init,
    cache: Cache = .{},

    pub const Source = struct {
        ptr: *anyopaque,
        vtable: *const VTable,

        pub const VTable = struct {
            ensure_listener_registered: *const fn (ptr: *anyopaque) anyerror!void,
            catalog_epoch: *const fn (ptr: *anyopaque) u64,
            projected_store: *const fn (ptr: *anyopaque) ?*metadata_storage.RaftApplyStore,
        };

        fn ensureListenerRegistered(self: Source) !void {
            try self.vtable.ensure_listener_registered(self.ptr);
        }

        fn catalogEpoch(self: Source) u64 {
            return self.vtable.catalog_epoch(self.ptr);
        }

        fn projectedStore(self: Source) ?*metadata_storage.RaftApplyStore {
            return self.vtable.projected_store(self.ptr);
        }
    };

    pub const Snapshot = struct {
        metadata_incarnation: ?metadata_api.MetadataClusterIncarnation = null,
        catalog_revision: u64 = 0,
        tables: []metadata_table_manager.TableRecord = &.{},
        ranges: []metadata_table_manager.RangeRecord = &.{},
        index: metadata_api.CatalogProjectionIndex = .{},

        pub fn deinit(self: *Snapshot, alloc: std.mem.Allocator) void {
            self.index.deinit(alloc);
            freeTables(alloc, self.tables);
            freeRanges(alloc, self.ranges);
            self.* = .{};
        }
    };

    const Cache = struct {
        catalog_epoch: u64 = 0,
        snapshot: ?Snapshot = null,

        fn deinit(self: *Cache, alloc: std.mem.Allocator) void {
            if (self.snapshot) |*snapshot| snapshot.deinit(alloc);
            self.* = .{};
        }
    };

    pub fn deinit(self: *CatalogProjectionReader, alloc: std.mem.Allocator) void {
        self.cache.deinit(alloc);
        self.* = .{};
    }

    pub fn lock(self: *CatalogProjectionReader) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
    }

    pub fn unlock(self: *CatalogProjectionReader) void {
        self.mutex.unlock(std.Options.debug_io);
    }

    pub fn lockUntil(self: *CatalogProjectionReader, deadline_ns: ?u64) bool {
        const deadline = deadline_ns orelse {
            self.lock();
            return true;
        };
        while (true) {
            if (platform_time.monotonicNs() >= deadline) return false;
            if (self.mutex.tryLock()) return true;
            platform_clock.Clock.real().sleepMs(1);
        }
    }

    pub fn validationSnapshotLocked(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
    ) !*const Snapshot {
        try ensureBeforeDeadline(deadline_ns);
        try source.ensureListenerRegistered();
        try ensureBeforeDeadline(deadline_ns);

        const current_epoch = source.catalogEpoch();
        if (self.cache.snapshot != null and self.cache.catalog_epoch == current_epoch) {
            return &(self.cache.snapshot orelse unreachable);
        }

        // The apply store captures both namespaces from one point-in-time read
        // transaction. Epoch checks stabilize publication while allowing apply
        // and Raft runtime work to proceed independently.
        for (0..4) |_| {
            try ensureBeforeDeadline(deadline_ns);
            const before = source.catalogEpoch();
            var fresh = try capture(alloc, metadata_group_id, source, deadline_ns);
            errdefer fresh.deinit(alloc);
            const after = source.catalogEpoch();
            try ensureBeforeDeadline(deadline_ns);
            if (before != after) {
                fresh.deinit(alloc);
                continue;
            }
            if (self.cache.snapshot) |*snapshot| snapshot.deinit(alloc);
            self.cache = .{ .catalog_epoch = after, .snapshot = fresh };
            return &(self.cache.snapshot orelse unreachable);
        }
        return error.CatalogProjectionUnstable;
    }

    pub fn routingSnapshot(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
    ) !metadata_api.CatalogRoutingSnapshot {
        if (!self.lockUntil(deadline_ns)) return error.CatalogRoutingSnapshotTimeout;
        defer self.unlock();

        const snapshot = try self.validationSnapshotLocked(alloc, metadata_group_id, source, deadline_ns);
        // Incarnation is immutable after first publication, but its projection
        // signal is intentionally outside table/range cache invalidation. Read
        // it on every exported snapshot so a cache populated during bootstrap
        // cannot retain a pre-incarnation null authority forever.
        const store = source.projectedStore() orelse return error.MissingMetadataStore;
        const incarnation = try store.getMetadataIncarnation(metadata_group_id);
        const tables = try cloneTables(alloc, snapshot.tables, deadline_ns);
        errdefer freeTables(alloc, tables);
        return .{
            .metadata_group_id = metadata_group_id,
            .metadata_incarnation = incarnation,
            .catalog_revision = snapshot.catalog_revision,
            .tables = tables,
            .ranges = try cloneRanges(alloc, snapshot.ranges, deadline_ns),
        };
    }

    pub fn freeRoutingSnapshot(
        _: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        snapshot: *metadata_api.CatalogRoutingSnapshot,
    ) void {
        freeTables(alloc, snapshot.tables);
        freeRanges(alloc, snapshot.ranges);
        snapshot.* = undefined;
    }

    pub fn cachedSnapshotLocked(self: *const CatalogProjectionReader) ?*const Snapshot {
        if (self.cache.snapshot) |*snapshot| return snapshot;
        return null;
    }

    pub fn cachedEpochLocked(self: *const CatalogProjectionReader) u64 {
        return self.cache.catalog_epoch;
    }

    fn capture(
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
    ) !Snapshot {
        const store = source.projectedStore() orelse return error.MissingMetadataStore;
        var snapshot: Snapshot = .{};
        errdefer snapshot.deinit(alloc);
        const projected = try store.captureCatalogProjection(alloc, metadata_group_id, deadline_ns);
        snapshot.metadata_incarnation = projected.metadata_incarnation;
        snapshot.catalog_revision = projected.catalog_revision;
        snapshot.tables = projected.tables;
        snapshot.ranges = projected.ranges;
        snapshot.index = try metadata_api.CatalogProjectionIndex.init(alloc, snapshot.tables, snapshot.ranges);
        return snapshot;
    }
};

fn ensureBeforeDeadline(deadline_ns: ?u64) !void {
    if (deadline_ns) |deadline| {
        if (platform_time.monotonicNs() >= deadline) return error.CatalogRoutingSnapshotTimeout;
    }
}

fn cloneTables(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.TableRecord,
    deadline_ns: ?u64,
) ![]metadata_table_manager.TableRecord {
    try ensureBeforeDeadline(deadline_ns);
    const out = try alloc.alloc(metadata_table_manager.TableRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        try ensureBeforeDeadline(deadline_ns);
        out[i] = try metadata_table_manager.cloneTable(alloc, record);
        cloned = i + 1;
    }
    try ensureBeforeDeadline(deadline_ns);
    return out;
}

fn cloneRanges(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.RangeRecord,
    deadline_ns: ?u64,
) ![]metadata_table_manager.RangeRecord {
    try ensureBeforeDeadline(deadline_ns);
    const out = try alloc.alloc(metadata_table_manager.RangeRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        try ensureBeforeDeadline(deadline_ns);
        out[i] = try metadata_table_manager.cloneRange(alloc, record);
        cloned = i + 1;
    }
    try ensureBeforeDeadline(deadline_ns);
    return out;
}

fn freeTables(alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
    for (records) |record| metadata_table_manager.freeTable(alloc, record);
    if (records.len > 0) alloc.free(records);
}

fn freeRanges(alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
    for (records) |record| metadata_table_manager.freeRange(alloc, record);
    if (records.len > 0) alloc.free(records);
}
