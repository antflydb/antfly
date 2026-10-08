// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact document-at-a-time sparse scoring. Streams retain encoded posting
//! blocks; no decoded arrays or corpus-sized score table are required. Bounds
//! include absence (zero) and both signed endpoints, and are added in the same
//! f32 order as contributions. Strict pruning preserves score ties.
const std = @import("std");
const A = std.mem.Allocator;
pub const Entry = struct {
    doc_num: u32,
    score: f32,
    pub fn worse(_: void, a: @This(), b: @This()) std.math.Order {
        const order = std.math.order(a.score, b.score);
        return if (order == .eq) std.math.order(b.doc_num, a.doc_num) else order;
    }
    pub fn better(_: void, a: @This(), b: @This()) bool {
        return worse({}, a, b) == .gt;
    }
};
pub const Stats = struct { scored: usize = 0, skipped_blocks: usize = 0 };
pub const Stream = struct {
    segment: ?u64 = null,
    version: u32 = 1,
    weight: f32,
    payload: []const u8 = &.{},
    single: ?[]const u8 = null,
    offset: usize = 0,
    chunk: []const u8 = &.{},
    index: u32 = 0,
    count: u32 = 0,
    doc: ?u32 = null,
    last: u32 = 0,
    min_weight: f32 = 0,
    step: f32 = 0,
    upper: f32 = 0,
    pub fn advance(self: *Stream) !void {
        if (self.doc != null and self.index + 1 < self.count) {
            self.index += 1;
            self.doc = std.math.add(u32, self.doc.?, std.mem.readInt(u32, self.chunk[13 + @as(usize, self.index) * 4 ..][0..4], .little)) catch return error.InvalidChunk;
            return;
        }
        try self.load();
    }
    fn load(self: *Stream) !void {
        const prior = self.doc;
        self.doc = null;
        var ordinal_bounds: ?[2]u32 = null;
        const bytes = if (self.single) |bytes| blk: {
            self.single = null;
            break :blk bytes;
        } else blk: {
            if (self.offset == self.payload.len) return;
            if (self.offset > self.payload.len or self.payload.len - self.offset < 8) return error.InvalidSparseSegment;
            const header = self.payload[self.offset..];
            const length = std.mem.readInt(u32, header[0..4], .little);
            const range_length = std.mem.readInt(u32, header[4..8], .little);
            self.offset += 8;
            if (@as(u64, length) + range_length > self.payload.len - self.offset) return error.InvalidSparseSegment;
            const bytes = self.payload[self.offset..][0..length];
            const range = self.payload[self.offset + length ..][0..range_length];
            if (range.len < 8) return error.InvalidChunk;
            const range_end = 8 + @as(u64, std.mem.readInt(u32, range[0..4], .little)) + std.mem.readInt(u32, range[4..8], .little);
            if (range_end > range.len) return error.InvalidChunk;
            const tail = range[@intCast(range_end)..];
            if (tail.len != 0) {
                if (tail.len != 12 or !std.mem.eql(u8, tail[0..4], "O32B")) return error.InvalidChunk;
                ordinal_bounds = .{ std.mem.readInt(u32, tail[4..8], .little), std.mem.readInt(u32, tail[8..12], .little) };
            }
            self.offset += @as(usize, length) + range_length;
            break :blk bytes;
        };
        if (bytes.len < 18 or bytes[0] != 1) return error.InvalidChunk;
        const count = std.mem.readInt(u32, bytes[1..5], .little);
        if (count == 0 or 13 + @as(u64, count) * 5 != bytes.len) return error.InvalidChunk;
        self.chunk = bytes;
        self.count = count;
        self.index = 0;
        const first = std.mem.readInt(u32, bytes[13..17], .little);
        if (prior) |previous| if (first <= previous) return error.InvalidChunk;
        self.doc = first;
        self.last = first;
        if (ordinal_bounds) |bounds| {
            if (bounds[0] != first or bounds[1] < first) return error.InvalidChunk;
            self.last = bounds[1];
        } else for (1..count) |i| {
            const delta = std.mem.readInt(u32, bytes[13 + i * 4 ..][0..4], .little);
            if (delta == 0) return error.InvalidChunk;
            self.last = std.math.add(u32, self.last, delta) catch return error.InvalidChunk;
        }
        const max: f32 = @bitCast(std.mem.readInt(u32, bytes[5..9], .little));
        self.min_weight = @bitCast(std.mem.readInt(u32, bytes[9..13], .little));
        self.step = (if (max > self.min_weight) max - self.min_weight else @as(f32, 1)) / 255.0;
        const end = self.min_weight + @as(f32, 255) * self.step;
        // Nonfinite input keeps the conservative path: never prune by it.
        self.upper = if (std.math.isFinite(self.weight) and std.math.isFinite(self.min_weight) and std.math.isFinite(end)) @max(0, @max(self.weight * self.min_weight, self.weight * end)) else std.math.inf(f32);
    }
    pub fn contribution(self: Stream) f32 {
        const quantized: f32 = @floatFromInt(self.chunk[13 + @as(usize, self.count) * 4 + self.index]);
        return self.weight * (self.min_weight + quantized * self.step);
    }
    fn skipThrough(self: *Stream, end: u32, stats: *Stats) !void {
        while (self.doc) |doc| {
            if (doc > end) return;
            if (self.last <= end) {
                stats.skipped_blocks += 1;
                // Retain the last ordinal as a cross-block ordering fence.
                self.doc = self.last;
                try self.load();
            } else try self.advance();
        }
    }
};

pub fn collect(a: A, streams: []Stream, k: usize, context: anytype, stats: *Stats) ![]Entry {
    var winners = std.PriorityQueue(Entry, void, Entry.worse).initContext({});
    defer winners.deinit(a);
    if (k == 0) return a.alloc(Entry, 0);
    const Navigation = struct {
        fn order(input: []Stream, left: usize, right: usize) std.math.Order {
            const relation = std.math.order(input[left].doc.?, input[right].doc.?);
            return if (relation == .eq) std.math.order(left, right) else relation;
        }
    };
    var queue = std.PriorityQueue(usize, []Stream, Navigation.order).initContext(streams);
    defer queue.deinit(a);
    for (streams, 0..) |*stream, i| {
        try context.check();
        try stream.advance();
        if (stream.doc != null) try queue.push(a, i);
    }
    var next_bounds: u64 = 0;
    while (queue.peek()) |first| {
        try context.check();
        const doc = streams[first].doc.?;
        if (comptime @hasDecl(@typeInfo(@TypeOf(context)).pointer.child, "mayMatch")) {
            if (!context.mayMatch(doc, streams[first].last)) {
                _ = queue.pop();
                try streams[first].skipThrough(streams[first].last, stats);
                if (streams[first].doc != null) try queue.push(a, first);
                continue;
            }
        }
        if (winners.items.len == k and doc >= next_bounds) {
            var upper: f32 = 0;
            var end: u32 = std.math.maxInt(u32);
            for (streams) |stream| if (stream.doc != null) {
                upper += stream.upper;
                end = @min(end, stream.last);
            };
            next_bounds = @as(u64, end) + 1;
            if (std.math.isFinite(upper) and upper < winners.peek().?.score) {
                queue.clearRetainingCapacity();
                for (streams, 0..) |*stream, i| {
                    try context.check();
                    try stream.skipThrough(end, stats);
                    if (stream.doc != null) try queue.push(a, i);
                }
                continue;
            }
        }
        var score: f32 = 0;
        var matched = false;
        while (queue.peek()) |i| {
            if (streams[i].doc.? != doc) break;
            _ = queue.pop();
            if (try context.allows(streams[i], doc)) {
                score += streams[i].contribution();
                matched = true;
            }
            try streams[i].advance();
            if (streams[i].doc != null) try queue.push(a, i);
        }
        if (!matched) continue;
        stats.scored += 1;
        const entry: Entry = .{ .doc_num = doc, .score = score };
        if (winners.items.len < k) try winners.push(a, entry) else if (Entry.better({}, entry, winners.peek().?)) {
            _ = winners.pop();
            try winners.push(a, entry);
        }
    }
    const result = try a.dupe(Entry, winners.items);
    std.mem.sort(Entry, result, {}, Entry.better);
    return result;
}

test "sparse document scoring prunes signed blocks and preserves exact ties under OOM" {
    const a = std.testing.allocator;
    var payloads: [2]std.ArrayList(u8) = @splat(.empty);
    defer for (&payloads) |*payload| payload.deinit(a);
    for (&payloads, 0..) |*payload, term| for (0..2) |block| {
        var bytes: [13 + 16 * 5]u8 = @splat(0);
        bytes[0] = 1;
        std.mem.writeInt(u32, bytes[1..5], 16, .little);
        const weight: f32 = if (term == 0) (if (block == 0) 100 else 1) else (if (block == 0) 10 else 2);
        std.mem.writeInt(u32, bytes[5..9], @bitCast(weight), .little);
        std.mem.writeInt(u32, bytes[9..13], @bitCast(weight), .little);
        for (0..16) |i| std.mem.writeInt(u32, bytes[13 + i * 4 ..][0..4], if (i == 0) @intCast(block * 16) else 1, .little);
        var header: [8]u8 = @splat(0);
        std.mem.writeInt(u32, header[4..8], 8, .little);
        std.mem.writeInt(u32, header[0..4], bytes.len, .little);
        try payload.appendSlice(a, &header);
        try payload.appendSlice(a, &bytes);
        try payload.appendSlice(a, &@as([8]u8, @splat(0)));
    };
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, doc: u32) !bool {
            return doc != 5;
        }
    };
    const Probe = struct {
        fn run(allocator: A, inputs: [2][]const u8) !void {
            var context: Context = .{};
            var streams = [_]Stream{ .{ .weight = 1, .payload = inputs[0] }, .{ .weight = -1, .payload = inputs[1] } };
            var all_stats: Stats = .{};
            const all = try collect(allocator, &streams, 32, &context, &all_stats);
            defer allocator.free(all);
            streams = .{ .{ .weight = 1, .payload = inputs[0] }, .{ .weight = -1, .payload = inputs[1] } };
            var top_stats: Stats = .{};
            const top = try collect(allocator, &streams, 7, &context, &top_stats);
            defer allocator.free(top);
            try std.testing.expectEqual(@as(usize, 31), all.len);
            try std.testing.expectEqual(@as(usize, 7), top.len);
            try std.testing.expect(top_stats.skipped_blocks > 0);
            try std.testing.expect(top_stats.scored < all_stats.scored);
            for (top, all[0..7]) |actual, expected| {
                try std.testing.expectEqual(expected.doc_num, actual.doc_num);
                try std.testing.expectEqual(@as(u32, @bitCast(expected.score)), @as(u32, @bitCast(actual.score)));
            }
        }
    };
    const inputs = [2][]const u8{ payloads[0].items, payloads[1].items };
    try Probe.run(a, inputs);
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{inputs});
    const Cancel = struct {
        calls: usize = 0,
        pub fn check(self: *@This()) !void {
            self.calls += 1;
            if (self.calls > 4) return error.Cancelled;
        }
        pub fn allows(_: *@This(), _: Stream, _: u32) !bool {
            return true;
        }
    };
    var cancel: Cancel = .{};
    var streams = [_]Stream{ .{ .weight = -1, .payload = inputs[0] }, .{ .weight = 1, .payload = inputs[1] } };
    var stats: Stats = .{};
    try std.testing.expectError(error.Cancelled, collect(a, &streams, 7, &cancel, &stats));
}

test "sparse document scoring preserves source addition order and bitmap block rejection" {
    var chunks: [3][23]u8 = @splat(@splat(0));
    const weights = [_]f32{ 16777216, -16777216, 1 };
    for (&chunks, weights) |*bytes, weight| {
        bytes[0] = 1;
        std.mem.writeInt(u32, bytes[1..5], 2, .little);
        std.mem.writeInt(u32, bytes[5..9], @bitCast(weight), .little);
        std.mem.writeInt(u32, bytes[9..13], @bitCast(weight), .little);
        std.mem.writeInt(u32, bytes[13..17], 10, .little);
        std.mem.writeInt(u32, bytes[17..21], 10, .little);
    }
    const Context = struct {
        reject: bool = false,
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, _: u32) !bool {
            return true;
        }
        pub fn mayMatch(self: *@This(), first: u32, last: u32) bool {
            return !self.reject or (first <= 99 and last >= 99);
        }
    };
    var context: Context = .{};
    var streams = [_]Stream{ .{ .weight = 1, .single = &chunks[0] }, .{ .weight = 1, .single = &chunks[1] }, .{ .weight = 1, .single = &chunks[2] } };
    var stats: Stats = .{};
    const all = try collect(std.testing.allocator, &streams, 2, &context, &stats);
    defer std.testing.allocator.free(all);
    try std.testing.expectEqual(@as(usize, 2), all.len);
    for (all) |entry| try std.testing.expectEqual(@as(f32, 1), entry.score);
    try std.testing.expectEqual(@as(u32, 10), all[0].doc_num);
    context.reject = true;
    streams = .{ .{ .weight = 1, .single = &chunks[0] }, .{ .weight = 1, .single = &chunks[1] }, .{ .weight = 1, .single = &chunks[2] } };
    stats = .{};
    const empty = try collect(std.testing.allocator, &streams, 2, &context, &stats);
    defer std.testing.allocator.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    try std.testing.expectEqual(@as(usize, 3), stats.skipped_blocks);
    try std.testing.expectEqual(@as(usize, 0), stats.scored);
}
