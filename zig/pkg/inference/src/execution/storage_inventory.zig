// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold inventory of actual backing byte spans. Union by address avoids
//! counting aliases twice. File extents exclude page rounding, RSS and residency.
const std = @import("std");
pub const Class = enum { owned_heap, mapped_file, borrowed_bytes };
pub const Snapshot = struct {
    /// Monotonic-clock interval of a synchronized inventory collection.
    collection: ?struct { begin_ns: u64, end_ns: u64 } = null,
    /// Owned payload ranges only. General allocator metadata is outside scope.
    owned_heap_payload_bytes: u64 = 0,
    mapped_file_extent_bytes: u64 = 0,
    /// May overlap owned/mapped bytes; never add aliases to physical ownership.
    borrowed_alias_bytes: u64 = 0,
    /// False for a custom/opaque store without an authoritative visitor.
    complete_source_inventory: bool = true,
};
pub const Visitor = struct {
    ptr: *anyopaque,
    visit_fn: *const fn (*anyopaque, []const u8, Class) anyerror!void,
    pub fn visit(self: Visitor, bytes: []const u8, class: Class) !void {
        if (bytes.len != 0) try self.visit_fn(self.ptr, bytes, class);
    }
};
pub const Collector = struct {
    allocator: std.mem.Allocator,
    ranges: std.ArrayList(Range) = .empty,
    const Range = struct { start: usize, end: usize, class: Class };
    pub fn deinit(self: *Collector) void {
        self.ranges.deinit(self.allocator);
    }
    pub fn visitor(self: *Collector) Visitor {
        return .{ .ptr = self, .visit_fn = record };
    }
    fn record(raw: *anyopaque, bytes: []const u8, class: Class) !void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        const start = @intFromPtr(bytes.ptr);
        try self.ranges.append(self.allocator, .{ .start = start, .end = try std.math.add(usize, start, bytes.len), .class = class });
    }
    fn lessThan(_: void, a: Range, b: Range) bool {
        if (a.class != b.class) return @backingInt(a.class) < @backingInt(b.class);
        return if (a.start != b.start) a.start < b.start else a.end < b.end;
    }
    pub fn finish(self: *Collector, complete_source_inventory: bool) !Snapshot {
        std.mem.sort(Range, self.ranges.items, {}, lessThan);
        var bytes: [3]u64 = @splat(0);
        var previous: ?Range = null;
        for (self.ranges.items) |range| {
            if (previous) |*span| {
                if (span.class == range.class and range.start <= span.end) {
                    span.end = @max(span.end, range.end);
                    continue;
                }
                const index = @backingInt(span.class);
                bytes[index] = try std.math.add(u64, bytes[index], span.end - span.start);
            }
            previous = range;
        }
        if (previous) |span| {
            const index = @backingInt(span.class);
            bytes[index] = try std.math.add(u64, bytes[index], span.end - span.start);
        }
        return .{ .owned_heap_payload_bytes = bytes[@backingInt(Class.owned_heap)], .mapped_file_extent_bytes = bytes[@backingInt(Class.mapped_file)], .borrowed_alias_bytes = bytes[@backingInt(Class.borrowed_bytes)], .complete_source_inventory = complete_source_inventory };
    }
};

test "actual storage inventory unions overlapping aliases within each ownership class" {
    var owned: [32]u8 = undefined;
    var mapped: [64]u8 = undefined;
    var collector = Collector{ .allocator = std.testing.allocator };
    defer collector.deinit();
    const visitor = collector.visitor();
    try visitor.visit(owned[8..24], .owned_heap);
    try visitor.visit(owned[0..16], .owned_heap);
    try visitor.visit(owned[20..32], .owned_heap);
    try visitor.visit(&mapped, .mapped_file);
    try visitor.visit(mapped[8..24], .mapped_file);
    try visitor.visit(owned[4..12], .borrowed_bytes);
    const snapshot = try collector.finish(false);
    try std.testing.expectEqual(@as(u64, 32), snapshot.owned_heap_payload_bytes);
    try std.testing.expectEqual(@as(u64, 64), snapshot.mapped_file_extent_bytes);
    try std.testing.expectEqual(@as(u64, 8), snapshot.borrowed_alias_bytes);
    try std.testing.expect(!snapshot.complete_source_inventory);
}

test "actual storage inventory collection unwinds every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var data: [128]u8 = undefined;
            var collector = Collector{ .allocator = a };
            defer collector.deinit();
            for (0..32) |index| try collector.visitor().visit(data[index..][0..4], .owned_heap);
            const result = try collector.finish(true);
            try std.testing.expectEqual(@as(u64, 35), result.owned_heap_payload_bytes);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
