// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");

/// Each partial owns its axis, optional metric and encoded value. Keep the
/// transfer generic so this ownership contract does not import a storage DB.
pub fn freeValue(alloc: std.mem.Allocator, partial: anytype) void {
    alloc.free(@constCast(partial.canonical_axis));
    if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
    alloc.free(@constCast(partial.value));
}

pub fn deinitList(alloc: std.mem.Allocator, list: anytype) void {
    for (list.items) |partial| freeValue(alloc, partial);
    list.deinit(alloc);
}

/// Consumes the shard array on success and failure. The destination owns only
/// the successfully appended prefix; any remaining payloads retire here.
pub fn appendOwned(alloc: std.mem.Allocator, destination: anytype, shard: anytype) !void {
    var transferred: usize = 0;
    defer {
        for (shard[transferred..]) |partial| freeValue(alloc, partial);
        if (shard.len > 0) alloc.free(shard);
    }
    for (shard) |partial| {
        try destination.append(alloc, partial);
        transferred += 1;
    }
}

test "algebraic partial ownership survives failure during a shard transfer" {
    const Partial = struct { canonical_axis: []const u8, metric: []const u8, value: []const u8 };
    const Case = struct {
        fn make(alloc: std.mem.Allocator, i: usize) !Partial {
            const axis = try std.fmt.allocPrint(alloc, "{d}", .{i});
            errdefer alloc.free(axis);
            const metric = if (i % 2 == 0) try alloc.dupe(u8, "count") else "";
            errdefer if (metric.len > 0) alloc.free(metric);
            return .{ .canonical_axis = axis, .metric = metric, .value = try alloc.dupe(u8, "payload") };
        }
        fn run(alloc: std.mem.Allocator) !void {
            var destination: std.ArrayListUnmanaged(Partial) = .empty;
            defer deinitList(alloc, &destination);
            const first = try make(alloc, 99);
            destination.append(alloc, first) catch |err| {
                freeValue(alloc, first);
                return err;
            };
            const shard = try alloc.alloc(Partial, 23);
            var initialized: usize = 0;
            var transferred = false;
            defer if (!transferred) {
                for (shard[0..initialized]) |partial| freeValue(alloc, partial);
                alloc.free(shard);
            };
            for (shard, 0..) |*partial, i| {
                partial.* = try make(alloc, i);
                initialized += 1;
            }
            // Transfer consumes the input even when a later growth fails.
            transferred = true;
            try appendOwned(alloc, &destination, shard);
            try std.testing.expectEqual(@as(usize, 24), destination.items.len);
            try std.testing.expectEqualStrings("99", destination.items[0].canonical_axis);
            try std.testing.expectEqualStrings("22", destination.items[23].canonical_axis);
            for (destination.items) |partial| try std.testing.expectEqualStrings("payload", partial.value);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
