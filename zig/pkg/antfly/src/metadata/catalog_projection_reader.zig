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
            capture_projection: *const fn (
                ptr: *anyopaque,
                alloc: std.mem.Allocator,
                metadata_group_id: u64,
                deadline_ns: ?u64,
            ) anyerror!metadata_storage.CatalogProjectionSnapshot,
        };

        fn ensureListenerRegistered(self: Source) !void {
            try self.vtable.ensure_listener_registered(self.ptr);
        }

        fn catalogEpoch(self: Source) u64 {
            return self.vtable.catalog_epoch(self.ptr);
        }

        fn captureProjection(
            self: Source,
            alloc: std.mem.Allocator,
            metadata_group_id: u64,
            deadline_ns: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            return try self.vtable.capture_projection(self.ptr, alloc, metadata_group_id, deadline_ns);
        }
    };

    pub const Snapshot = struct {
        const RangeSpan = struct { start: usize, len: usize };

        metadata_incarnation: ?metadata_api.MetadataClusterIncarnation = null,
        catalog_revision: u64 = 0,
        tables: []metadata_table_manager.TableRecord = &.{},
        ranges: []metadata_table_manager.RangeRecord = &.{},
        index: metadata_api.CatalogProjectionIndex = .{},
        table_name_indexes: std.StringHashMapUnmanaged(usize) = .empty,
        table_range_spans: std.AutoHashMapUnmanaged(u64, RangeSpan) = .empty,

        pub fn deinit(self: *Snapshot, alloc: std.mem.Allocator) void {
            self.table_name_indexes.deinit(alloc);
            self.table_range_spans.deinit(alloc);
            self.index.deinit(alloc);
            freeTables(alloc, self.tables);
            freeRanges(alloc, self.ranges);
            self.* = .{};
        }
    };

    const Cache = struct {
        catalog_epoch: u64 = 0,
        reusable: bool = false,
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
        if (self.cache.reusable and self.cache.snapshot != null and self.cache.catalog_epoch == current_epoch) {
            return &(self.cache.snapshot orelse unreachable);
        }

        // The apply store captures both namespaces from one point-in-time read
        // transaction, so concurrent publication does not make the captured
        // value internally inconsistent. If the listener epoch changes while
        // cloning, return this coherent point-in-time value to the current
        // caller but mark it non-reusable; the next caller refreshes instead
        // of turning ordinary catalog churn into an internal error.
        try ensureBeforeDeadline(deadline_ns);
        const before = source.catalogEpoch();
        var fresh = try capture(alloc, metadata_group_id, source, deadline_ns);
        errdefer fresh.deinit(alloc);
        const after = source.catalogEpoch();
        try ensureBeforeDeadline(deadline_ns);
        if (self.cache.snapshot) |cached| {
            if (std.meta.eql(cached.metadata_incarnation, fresh.metadata_incarnation) and
                fresh.catalog_revision < cached.catalog_revision)
            {
                return error.CatalogProjectionRevisionRegressed;
            }
        }
        if (self.cache.snapshot) |*snapshot| snapshot.deinit(alloc);
        self.cache = .{
            .catalog_epoch = after,
            .reusable = before == after,
            .snapshot = fresh,
        };
        return &(self.cache.snapshot orelse unreachable);
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
        const tables = try cloneTables(alloc, snapshot.tables, deadline_ns);
        errdefer freeTables(alloc, tables);
        return .{
            .metadata_group_id = metadata_group_id,
            .metadata_incarnation = snapshot.metadata_incarnation,
            .catalog_revision = snapshot.catalog_revision,
            .change_token = .{
                .metadata_group_id = metadata_group_id,
                .metadata_incarnation = snapshot.metadata_incarnation,
                .revision = snapshot.catalog_revision,
            },
            .tables = tables,
            .ranges = try cloneRanges(alloc, snapshot.ranges, deadline_ns),
        };
    }

    /// Clone only one table and its ranges from the shared immutable cache.
    /// Indexed lookup is constant-time and allocation/copy cost scales with
    /// the selected table rather than total tenant count.
    pub fn tableRoutingSnapshot(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        table_name: []const u8,
        deadline_ns: ?u64,
    ) !metadata_api.CatalogRoutingSnapshot {
        if (!self.lockUntil(deadline_ns)) return error.CatalogRoutingSnapshotTimeout;
        defer self.unlock();

        const snapshot = try self.validationSnapshotLocked(alloc, metadata_group_id, source, deadline_ns);
        const table_index = snapshot.table_name_indexes.get(table_name) orelse return .{
            .metadata_group_id = metadata_group_id,
            .metadata_incarnation = snapshot.metadata_incarnation,
            .catalog_revision = snapshot.catalog_revision,
            .change_token = .{
                .metadata_group_id = metadata_group_id,
                .metadata_incarnation = snapshot.metadata_incarnation,
                .revision = snapshot.catalog_revision,
            },
            .tables = &.{},
            .ranges = &.{},
        };
        const table = snapshot.tables[table_index];

        const tables = try alloc.alloc(metadata_table_manager.TableRecord, 1);
        errdefer alloc.free(tables);
        tables[0] = try metadata_table_manager.cloneRoutingTable(alloc, table);
        errdefer metadata_table_manager.freeTable(alloc, tables[0]);
        const span = snapshot.table_range_spans.get(table.table_id) orelse Snapshot.RangeSpan{ .start = 0, .len = 0 };
        const table_ranges = snapshot.ranges[span.start..][0..span.len];
        const ranges = try alloc.alloc(metadata_table_manager.RangeRecord, table_ranges.len);
        var cloned: usize = 0;
        errdefer {
            for (ranges[0..cloned]) |range| metadata_table_manager.freeRange(alloc, range);
            alloc.free(ranges);
        }
        for (table_ranges) |range| {
            try ensureBeforeDeadline(deadline_ns);
            ranges[cloned] = try metadata_table_manager.cloneRoutingRange(alloc, range);
            cloned += 1;
        }
        try ensureBeforeDeadline(deadline_ns);
        return .{
            .metadata_group_id = metadata_group_id,
            .metadata_incarnation = snapshot.metadata_incarnation,
            .catalog_revision = snapshot.catalog_revision,
            .change_token = .{
                .metadata_group_id = metadata_group_id,
                .metadata_incarnation = snapshot.metadata_incarnation,
                .revision = snapshot.catalog_revision,
            },
            .tables = tables,
            .ranges = ranges,
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
        var snapshot: Snapshot = .{};
        errdefer snapshot.deinit(alloc);
        const projected = try source.captureProjection(alloc, metadata_group_id, deadline_ns);
        snapshot.metadata_incarnation = projected.metadata_incarnation;
        snapshot.catalog_revision = projected.catalog_revision;
        snapshot.tables = projected.tables;
        snapshot.ranges = projected.ranges;
        std.sort.pdq(metadata_table_manager.RangeRecord, snapshot.ranges, {}, rangeLessThan);
        snapshot.index = try metadata_api.CatalogProjectionIndex.init(alloc, snapshot.tables, snapshot.ranges);
        try snapshot.table_name_indexes.ensureTotalCapacity(alloc, @intCast(snapshot.tables.len));
        try snapshot.table_range_spans.ensureTotalCapacity(alloc, @intCast(snapshot.tables.len));
        for (snapshot.tables, 0..) |table, index| {
            if (snapshot.table_name_indexes.contains(table.name)) return error.InvalidCatalogProjection;
            snapshot.table_name_indexes.putAssumeCapacity(table.name, index);
        }
        var first: usize = 0;
        while (first < snapshot.ranges.len) {
            var end = first + 1;
            while (end < snapshot.ranges.len and snapshot.ranges[end].table_id == snapshot.ranges[first].table_id) : (end += 1) {}
            const table_id = snapshot.ranges[first].table_id;
            if (!snapshot.index.table_indexes.contains(table_id)) return error.InvalidCatalogProjection;
            snapshot.table_range_spans.putAssumeCapacity(table_id, .{ .start = first, .len = end - first });
            first = end;
        }
        return snapshot;
    }
};

fn rangeLessThan(_: void, a: metadata_table_manager.RangeRecord, b: metadata_table_manager.RangeRecord) bool {
    if (a.table_id != b.table_id) return a.table_id < b.table_id;
    return switch (std.mem.order(u8, a.start_key, b.start_key)) {
        .lt => true,
        .gt => false,
        .eq => a.group_id < b.group_id,
    };
}

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
        out[i] = try metadata_table_manager.cloneRoutingTable(alloc, record);
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
        out[i] = try metadata_table_manager.cloneRoutingRange(alloc, record);
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

test "catalog projection churn returns coherent snapshots and only caches stable captures" {
    const FakeSource = struct {
        epoch: u64 = 0,
        revision: u64 = 0,
        captures: usize = 0,
        advance_epoch_during_capture: bool = true,

        fn source(self: *@This()) CatalogProjectionReader.Source {
            return .{ .ptr = self, .vtable = &.{
                .ensure_listener_registered = ensureListenerRegistered,
                .catalog_epoch = catalogEpoch,
                .capture_projection = captureProjection,
            } };
        }

        fn ensureListenerRegistered(_: *anyopaque) !void {}

        fn catalogEpoch(ptr: *anyopaque) u64 {
            return cast(ptr).epoch;
        }

        fn captureProjection(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            deadline_ns: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            try ensureBeforeDeadline(deadline_ns);
            const self = cast(ptr);
            self.captures += 1;
            self.revision += 1;
            if (self.advance_epoch_during_capture) self.epoch += 1;
            return .{
                .metadata_incarnation = null,
                .catalog_revision = self.revision,
                .tables = &.{},
                .ranges = &.{},
            };
        }

        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
    };

    var fake = FakeSource{};
    var reader: CatalogProjectionReader = .{};
    defer reader.deinit(std.testing.allocator);

    var first = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &first);
    try std.testing.expectEqual(@as(usize, 1), fake.captures);
    try std.testing.expect(!reader.cache.reusable);

    var second = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &second);
    try std.testing.expectEqual(@as(usize, 2), fake.captures);
    try std.testing.expect(!reader.cache.reusable);

    fake.advance_epoch_during_capture = false;
    var stable = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &stable);
    try std.testing.expectEqual(@as(usize, 3), fake.captures);
    try std.testing.expect(reader.cache.reusable);

    var cached = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &cached);
    try std.testing.expectEqual(@as(usize, 3), fake.captures);
}

test "catalog projection cache rejects revision regression within one authority" {
    const FakeSource = struct {
        epoch: u64 = 1,
        revision: u64 = 7,

        fn source(self: *@This()) CatalogProjectionReader.Source {
            return .{ .ptr = self, .vtable = &.{
                .ensure_listener_registered = ensureListenerRegistered,
                .catalog_epoch = catalogEpoch,
                .capture_projection = captureProjection,
            } };
        }

        fn ensureListenerRegistered(_: *anyopaque) !void {}

        fn catalogEpoch(ptr: *anyopaque) u64 {
            return cast(ptr).epoch;
        }

        fn captureProjection(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            const self = cast(ptr);
            return .{
                .metadata_incarnation = null,
                .catalog_revision = self.revision,
                .tables = &.{},
                .ranges = &.{},
            };
        }

        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
    };

    var fake = FakeSource{};
    var reader: CatalogProjectionReader = .{};
    defer reader.deinit(std.testing.allocator);

    var initial = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &initial);
    fake.epoch += 1;
    fake.revision -= 1;
    try std.testing.expectError(
        error.CatalogProjectionRevisionRegressed,
        reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null),
    );
    try std.testing.expectEqual(@as(u64, 7), reader.cache.snapshot.?.catalog_revision);
}
