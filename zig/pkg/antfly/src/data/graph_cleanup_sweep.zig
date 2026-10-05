// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const catalog = @import("../api/table_catalog.zig");
const metadata = @import("../metadata/api.zig");
const topology = @import("../common/topology_records.zig");

/// A bounded worker round resumes this immutable routing generation rather
/// than recapturing or searching the catalog for every visited group.
pub const Sweep = struct {
    generation: ?*catalog.RoutingGeneration = null,
    cursor: usize = 0,

    pub const Visit = struct {
        range: topology.RangeRecord,
        table: ?topology.TableRecord,
    };

    pub fn deinit(self: *Sweep) void {
        if (self.generation) |generation| generation.release();
        self.* = .{};
    }

    /// Returned strings are borrowed until the next call that ends the sweep.
    /// Null marks its end; the following call captures the current topology.
    pub fn next(self: *Sweep, alloc: std.mem.Allocator, source: catalog.CatalogSource, deadline: ?u64) !?Visit {
        if (self.generation == null) {
            self.generation = if (source.vtable.acquire_routing_generation) |acquire|
                try acquire(source.ptr, deadline, false)
            else blk: {
                const routing = try source.routingSource();
                var snapshot = try routing.eventualSnapshot(deadline);
                defer snapshot.deinit();
                break :blk try catalog.RoutingGeneration.create(alloc, snapshot.value, source.budget(deadline));
            };
        }
        const indexed = &self.generation.?.indexed;
        const snapshot = indexed.snapshot.value;
        if (self.cursor == snapshot.ranges.len) {
            self.deinit();
            return null;
        }
        const range = snapshot.ranges[self.cursor];
        self.cursor += 1;
        return .{ .range = range, .table = if (indexed.table_id_indexes.get(range.table_id)) |index| snapshot.tables[index] else null };
    }
};

test "graph endpoint cleanup routing retains one capture for ten thousand groups and refreshes topology" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const ranges = try arena.allocator().alloc(topology.RangeRecord, 10_000);
    for (ranges, 0..) |*range, i| range.* = .{
        .table_id = 7,
        .group_id = @intCast(ranges.len - i),
        .start_key = if (i == 0) "" else try std.fmt.allocPrint(arena.allocator(), "{d:0>5}", .{i}),
        .end_key = if (i + 1 == ranges.len) null else try std.fmt.allocPrint(arena.allocator(), "{d:0>5}", .{i + 1}),
    };
    var initial_tables = [_]topology.TableRecord{.{ .table_id = 7, .name = "facts" }};
    const initial = try catalog.RoutingGeneration.create(alloc, .{ .tables = &initial_tables, .ranges = ranges }, .{});
    defer initial.release();
    var replacement_tables = [_]topology.TableRecord{.{ .table_id = 9, .name = "new" }};
    var replacement_ranges = [_]topology.RangeRecord{.{ .table_id = 9, .group_id = 20_000, .start_key = "" }};
    const replacement = try catalog.RoutingGeneration.create(alloc, .{ .tables = &replacement_tables, .ranges = &replacement_ranges }, .{});
    defer replacement.release();
    const Provider = struct {
        current: *catalog.RoutingGeneration,
        captures: usize = 0,
        fn acquire(ptr: *anyopaque, _: ?u64, authoritative: bool) !*catalog.RoutingGeneration {
            try std.testing.expect(!authoritative);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.captures += 1;
            self.current.retain();
            return self.current;
        }
        fn admin(_: *anyopaque) !metadata.AdminSnapshot {
            return error.UnexpectedAdminSnapshot;
        }
        fn freeAdmin(_: *anyopaque, _: *metadata.AdminSnapshot) void {
            unreachable;
        }
    };
    var provider = Provider{ .current = initial };
    const source = catalog.CatalogSource{ .ptr = &provider, .vtable = &.{ .admin_snapshot = Provider.admin, .free_admin_snapshot = Provider.freeAdmin, .acquire_routing_generation = Provider.acquire } };
    var sweep = Sweep{};
    defer sweep.deinit();
    var seen = try alloc.alloc(bool, ranges.len);
    defer alloc.free(seen);
    @memset(seen, false);
    var count: usize = 0;
    while (try sweep.next(alloc, source, null)) |visit| {
        try std.testing.expectEqualStrings("facts", visit.table.?.name);
        const index = visit.range.group_id - 1;
        try std.testing.expect(!seen[index]);
        seen[index] = true;
        count += 1;
        // Topology replacement halfway through must not reset or skip work.
        if (count == 5000) provider.current = replacement;
        try std.testing.expectEqual(@as(usize, 1), provider.captures);
    }
    try std.testing.expectEqual(ranges.len, count);
    try std.testing.expectEqual(@as(usize, 1), initial.refs.load(.acquire));
    const next = (try sweep.next(alloc, source, null)).?;
    try std.testing.expectEqual(@as(u64, 20_000), next.range.group_id);
    try std.testing.expectEqualStrings("new", next.table.?.name);
    try std.testing.expectEqual(@as(usize, 2), provider.captures);
    // Shutdown while a sweep is retained must release its final reference.
    sweep.deinit();
    try std.testing.expectEqual(@as(usize, 1), replacement.refs.load(.acquire));
}
