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
const records = @import("../metadata/table_manager.zig");
const reconciler = @import("../metadata/reconciler.zig");
const RouteBudget = @import("table_router.zig").RouteBudget;
const join_model = @import("join_model.zig");

pub const TableStats = struct {
    row_count: u64 = 0,
    size_bytes: u64 = 0,
    shard_count: usize = 0,
    row_count_known: bool = false,
    size_bytes_known: bool = false,

    pub fn hasAny(self: TableStats) bool {
        return self.row_count_known or self.size_bytes_known;
    }

    pub fn estimatedSizeBytes(self: TableStats) u64 {
        if (self.size_bytes_known and (self.size_bytes != 0 or !self.row_count_known or self.row_count == 0)) return self.size_bytes;
        if (!self.row_count_known) return 0;
        return self.row_count *| join_model.join_estimated_row_bytes;
    }
};

pub const Table = struct {
    table_id: u64,
    name: []const u8,
    ranges: []records.RangeRecord = &.{},
    group_ids: []u64 = &.{},
    stats: TableStats = .{},
    identity_ready: bool = true,

    pub fn validateIdentity(self: Table) !void {
        if (!self.identity_ready) return error.DocIdentityNamespaceMismatch;
    }

    pub fn groupForKey(self: Table, key: []const u8) ?u64 {
        var lo: usize = 0;
        var hi = self.ranges.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, self.ranges[mid].start_key, key) != .gt) lo = mid + 1 else hi = mid;
        }
        if (lo == 0) return null;
        const range = self.ranges[lo - 1];
        if (range.end_key) |end| if (std.mem.order(u8, key, end) != .lt) return null;
        return range.group_id;
    }
};

/// Immutable, reference-counted query planning data. Published once with the
/// control observation, retained under its publication lock, and consumed
/// without that lock. It owns no schemas, indexes, stores, or diagnostics.
/// Runtime statistics are estimates; routed workers still validate identity.
pub const Generation = struct {
    alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    refs: std.atomic.Value(usize) = .init(1),
    tables: std.StringHashMapUnmanaged(Table) = .empty,

    pub fn create(alloc: std.mem.Allocator, snapshot: anytype, budget: RouteBudget) !*Generation {
        try budget.check();
        const self = try alloc.create(Generation);
        self.* = .{ .alloc = alloc, .arena = .init(alloc) };
        errdefer self.release();
        const owned = self.arena.allocator();
        var ids: std.AutoHashMapUnmanaged(u64, []const u8) = .empty;
        defer ids.deinit(alloc);
        var counts: std.AutoHashMapUnmanaged(u64, usize) = .empty;
        defer counts.deinit(alloc);
        var statuses: std.AutoHashMapUnmanaged(u64, *const reconciler.MergedGroupStatus) = .empty;
        defer statuses.deinit(alloc);
        for (snapshot.merged_group_statuses, 0..) |*status, i| {
            if (i % 64 == 0) try budget.check();
            try statuses.put(alloc, status.group_id, status);
        }
        for (snapshot.tables, 0..) |record, i| {
            if (i % 64 == 0) try budget.check();
            const name = try owned.dupe(u8, record.name);
            try self.tables.put(owned, name, .{ .table_id = record.table_id, .name = name });
            try ids.put(alloc, record.table_id, name);
        }
        for (snapshot.ranges, 0..) |range, i| {
            if (i % 64 == 0) try budget.check();
            const count = try counts.getOrPut(alloc, range.table_id);
            if (!count.found_existing) count.value_ptr.* = 0;
            count.value_ptr.* += 1;
        }
        var tables = self.tables.valueIterator();
        while (tables.next()) |table| {
            try budget.check();
            const count = counts.get(table.table_id) orelse 0;
            table.ranges = try owned.alloc(records.RangeRecord, count);
            table.group_ids = try owned.alloc(u64, count);
            table.stats.row_count_known = count > 0;
            table.stats.size_bytes_known = count > 0;
        }
        for (snapshot.ranges, 0..) |range, i| {
            if (i % 64 == 0) try budget.check();
            const table = self.tables.getPtr(ids.get(range.table_id) orelse continue).?;
            const index = table.stats.shard_count;
            table.stats.shard_count += 1;
            table.ranges[index] = .{
                .table_id = range.table_id,
                .group_id = range.group_id,
                .range_id = range.range_id,
                .doc_identity_shard_id = range.doc_identity_shard_id,
                .doc_identity_range_id = range.doc_identity_range_id,
                .start_key = try owned.dupe(u8, range.start_key),
                .end_key = if (range.end_key) |end| try owned.dupe(u8, end) else null,
            };
            // Preserve catalog group order for deterministic worker assignment.
            table.group_ids[index] = range.group_id;
            if (statuses.get(range.group_id)) |status| {
                table.stats.row_count +|= status.doc_count;
                table.stats.size_bytes +|= status.disk_bytes;
                table.stats.size_bytes_known = table.stats.size_bytes_known and status.disk_bytes_known;
                const identity = status.doc_identity;
                const has_namespace = identity.namespace_table_id != 0 or identity.namespace_shard_id != 0 or identity.namespace_range_id != 0;
                table.identity_ready = table.identity_ready and
                    !status.doc_identity_reassignment_active and !status.doc_identity_namespace_conflict and !identity.rebuild_required and
                    (!has_namespace or (identity.namespace_table_id == range.table_id and
                        identity.namespace_shard_id == records.rangeDocIdentityShardId(range) and
                        identity.namespace_range_id == records.rangeDocIdentityRangeId(range)));
            } else {
                table.stats.row_count_known = false;
                table.stats.size_bytes_known = false;
            }
        }
        tables = self.tables.valueIterator();
        while (tables.next()) |table| {
            try budget.check();
            std.mem.sort(records.RangeRecord, table.ranges, {}, struct {
                fn less(_: void, a: records.RangeRecord, b: records.RangeRecord) bool {
                    return std.mem.order(u8, a.start_key, b.start_key) == .lt;
                }
            }.less);
        }
        try budget.check();
        return self;
    }

    pub fn retain(self: *Generation) *Generation {
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0);
        return self;
    }

    pub fn release(self: *Generation) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        self.arena.deinit();
        self.alloc.destroy(self);
    }

    pub fn findTable(self: *const Generation, name: []const u8) ?Table {
        return self.tables.get(name);
    }
};

test "join planning retains compact indexed ranges and conservative statistics" {
    const alloc = std.testing.allocator;
    var name = [_]u8{ 'd', 'o', 'c', 's' };
    var tables = [_]records.TableRecord{ .{ .table_id = 1, .name = &name, .schema_json = "large schema excluded" }, .{ .table_id = 2, .name = "unknown" } };
    var ranges = [_]records.RangeRecord{
        .{ .table_id = 1, .group_id = 12, .start_key = "m" },
        .{ .table_id = 2, .group_id = 20, .start_key = "" },
        .{ .table_id = 1, .group_id = 11, .start_key = "", .end_key = "m" },
    };
    var statuses = [_]reconciler.MergedGroupStatus{
        .{ .group_id = 11, .doc_count = 7, .disk_bytes = 70, .disk_bytes_known = true },
        .{ .group_id = 12, .doc_count = 3, .disk_bytes = 30, .disk_bytes_known = false },
    };
    const generation = try Generation.create(alloc, .{ .tables = &tables, .ranges = &ranges, .merged_group_statuses = &statuses }, .{});
    const pinned = generation.retain();
    generation.release();
    defer pinned.release();
    name[0] = 'x';
    ranges[0].group_id = 99;
    statuses[0].doc_count = 100;
    const table = pinned.findTable("docs").?;
    try std.testing.expectEqualSlices(u64, &.{ 12, 11 }, table.group_ids);
    try std.testing.expectEqual(@as(?u64, 11), table.groupForKey("a"));
    try std.testing.expectEqual(@as(?u64, 12), table.groupForKey("m"));
    try std.testing.expectEqual(@as(u64, 10), table.stats.row_count);
    try std.testing.expect(table.stats.row_count_known);
    try std.testing.expect(!table.stats.size_bytes_known);
    try std.testing.expect(!pinned.findTable("unknown").?.stats.hasAny());
    try table.validateIdentity();
    statuses[0].doc_identity.rebuild_required = true;
    const invalid = try Generation.create(alloc, .{ .tables = &tables, .ranges = &ranges, .merged_group_statuses = &statuses }, .{});
    defer invalid.release();
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, invalid.findTable("xocs").?.validateIdentity());
}

test "join planning construction honors deadline cancellation and allocation failures" {
    const alloc = std.testing.allocator;
    const snapshot = .{
        .tables = @as([]const records.TableRecord, &.{.{ .table_id = 1, .name = "docs" }}),
        .ranges = @as([]const records.RangeRecord, &.{.{ .table_id = 1, .group_id = 1, .start_key = "" }}),
        .merged_group_statuses = @as([]const reconciler.MergedGroupStatus, &.{}),
    };
    try std.testing.expectError(error.Timeout, Generation.create(alloc, snapshot, .{ .clock = .{ .deadline_ns = 0 } }));
    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Cancelled, Generation.create(alloc, snapshot, .{ .cancellation = .fromAtomic(&cancelled) }));
    try std.testing.checkAllAllocationFailures(alloc, struct {
        fn run(a: std.mem.Allocator, input: @TypeOf(snapshot)) !void {
            const generation = try Generation.create(a, input, .{});
            generation.release();
        }
    }.run, .{snapshot});
}
