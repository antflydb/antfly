// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Exact document-at-a-time sparse scoring. Streams retain encoded posting
//! blocks; no decoded arrays or corpus-sized score table are required. Bounds
//! include absence (zero) and both signed endpoints, and are added in the same
//! f32 order as contributions. Ordinal-aware pruning preserves score ties.
const std = @import("std");
const A = std.mem.Allocator;
pub const Entry = struct {
    doc_num: u32,
    score: f32,
    pub fn worse(_: void, a: @This(), b: @This()) std.math.Order {
        // Keep the comparator total even for callers outside collect. Scoring
        // rejects nonfinite values before ranking; NaNs sort below real scores.
        if (std.math.isNan(a.score)) return if (std.math.isNan(b.score)) std.math.order(b.doc_num, a.doc_num) else .lt;
        if (std.math.isNan(b.score)) return .gt;
        const order = std.math.order(a.score, b.score);
        return if (order == .eq) std.math.order(b.doc_num, a.doc_num) else order;
    }
    pub fn better(_: void, a: @This(), b: @This()) bool {
        return worse({}, a, b) == .gt;
    }
};
pub fn addScore(left: f32, right: f32) !f32 {
    const result = left + right;
    if (!std.math.isFinite(right) or !std.math.isFinite(result)) return error.SparseScoreOverflow;
    return result;
}
pub const Stats = struct { scored: usize = 0, skipped_blocks: usize = 0, skipped_prefixes: usize = 0 };
pub const BlockProbe = union(enum) { legacy, end, block: struct { first: u32, last: u32, min: f32, max: f32, unique: bool = false } };
pub const BlockReader = struct {
    probe: ?*const fn (*anyopaque, u64, u32, u64) anyerror!BlockProbe = null,
    ptr: *anyopaque,
    resident: *usize,
    read: *const fn (*anyopaque, A, u64, u32, u64) anyerror!?[]u8,
};
pub const Stream = struct {
    reader: ?BlockReader = null,
    allocator: ?A = null,
    owned: ?[]u8 = null,
    term: u32 = 0,
    query_order: usize = 0,
    seek_target: u64 = 0,
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
    pub fn deinit(self: *Stream) void {
        if (self.owned) |bytes| {
            self.reader.?.resident.* -= bytes.len;
            self.allocator.?.free(bytes);
        }
        self.owned = null;
    }
    pub fn seek(self: *Stream, target: u64) !void {
        if (target > std.math.maxInt(u32)) {
            self.doc = null;
            return;
        }
        if (self.reader != null and (self.doc == null or target > self.last)) {
            self.seek_target = target;
            try self.load();
        }
        while (self.doc) |doc| {
            if (doc >= target) break;
            try self.advance();
        }
    }
    pub fn advance(self: *Stream) !void {
        if (self.doc != null and self.chunk.len == 0) try self.materialize();
        if (self.doc != null and self.index + 1 < self.count) {
            self.index += 1;
            self.doc = std.math.add(u32, self.doc.?, std.mem.readInt(u32, self.chunk[13 + @as(usize, self.index) * 4 ..][0..4], .little)) catch return error.InvalidChunk;
            return;
        }
        if (self.reader != null and self.doc != null) self.seek_target = @as(u64, self.last) + 1;
        try self.load();
    }
    fn materialize(self: *Stream) !void {
        if (self.chunk.len != 0) return;
        const first = self.doc orelse return;
        self.doc = null;
        // Probe identified the block by its last-ordinal key. Its first ordinal
        // can equal the preceding legacy block's last ordinal.
        self.seek_target = self.last;
        try self.loadEncoded();
        if (self.doc == null or self.doc.? != first) return error.InvalidChunk;
    }
    fn load(self: *Stream) !void {
        if (self.reader) |reader| if (reader.probe) |probe| {
            switch (try probe(reader.ptr, self.segment.?, self.term, self.seek_target)) {
                .end => {
                    self.deinit();
                    self.doc = null;
                    self.chunk = &.{};
                    return;
                },
                .legacy => {},
                .block => |block| {
                    if (block.first > block.last or block.last < self.seek_target) return error.InvalidChunk;
                    if (self.doc) |prior| if (block.first < prior) return error.InvalidChunk;
                    self.deinit();
                    self.chunk = &.{};
                    self.doc = block.first;
                    self.last = block.last;
                    self.upper = if (block.unique and std.math.isFinite(self.weight) and std.math.isFinite(block.min) and std.math.isFinite(block.max)) @max(0, @max(self.weight * block.min, self.weight * block.max)) else std.math.inf(f32);
                    return;
                },
            }
        };
        try self.loadEncoded();
    }
    fn loadEncoded(self: *Stream) !void {
        const prior = self.doc;
        self.doc = null;
        var ordinal_bounds: ?[2]u32 = null;
        // Old framed streams may repeat an ordinal across block boundaries.
        // Only a producer proof or a complete single chunk permits pruning.
        var unique = self.single != null;
        if (self.reader) |reader| {
            self.deinit();
            self.owned = try reader.read(reader.ptr, self.allocator.?, self.segment.?, self.term, self.seek_target);
            self.payload = self.owned orelse return;
            self.offset = 0;
        }
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
                if (tail.len != 12 or (!std.mem.eql(u8, tail[0..4], "O32B") and !std.mem.eql(u8, tail[0..4], "O32U"))) return error.InvalidChunk;
                unique = std.mem.eql(u8, tail[0..4], "O32U");
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
        if (prior) |previous| if (first < previous or (unique and first == previous)) return error.InvalidChunk;
        self.doc = first;
        self.last = first;
        if (ordinal_bounds) |bounds| {
            if (bounds[0] != first or bounds[1] < first) return error.InvalidChunk;
            self.last = bounds[1];
        } else for (1..count) |i| {
            const delta = std.mem.readInt(u32, bytes[13 + i * 4 ..][0..4], .little);
            if (delta == 0) unique = false;
            self.last = std.math.add(u32, self.last, delta) catch return error.InvalidChunk;
        }
        const max: f32 = @bitCast(std.mem.readInt(u32, bytes[5..9], .little));
        self.min_weight = @bitCast(std.mem.readInt(u32, bytes[9..13], .little));
        self.step = (if (max > self.min_weight) max - self.min_weight else @as(f32, 1)) / 255.0;
        var low: u8 = 255;
        var high: u8 = 0;
        for (bytes[13 + @as(usize, count) * 4 ..]) |value| {
            low = @min(low, value);
            high = @max(high, value);
        }
        const minimum = self.min_weight + @as(f32, @floatFromInt(low)) * self.step;
        const maximum = self.min_weight + @as(f32, @floatFromInt(high)) * self.step;
        // Nonfinite input keeps the conservative path: never prune by it.
        self.upper = if (unique and std.math.isFinite(self.weight) and std.math.isFinite(minimum) and std.math.isFinite(maximum)) @max(0, @max(self.weight * minimum, self.weight * maximum)) else std.math.inf(f32);
    }
    pub fn contribution(self: *Stream) !f32 {
        try self.materialize();
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
                if (self.reader != null) self.seek_target = @as(u64, end) + 1;
                try self.load();
            } else try self.advance();
        }
    }
};

fn cannotBeat(upper: f32, first: u32, winner: Entry) bool {
    return std.math.isFinite(upper) and (upper < winner.score or (upper == winner.score and first > winner.doc_num));
}

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
        if (comptime @hasDecl(@typeInfo(@TypeOf(context)).pointer.child, "nextCandidate")) {
            if (stream.reader != null) {
                try stream.seek(try context.nextCandidate(0));
            } else {
                try stream.advance();
                if (stream.doc) |doc| try stream.seek(try context.nextCandidate(doc));
            }
        } else try stream.advance();
        if (stream.doc != null) try queue.push(a, i);
    }
    var next_bounds: u64 = 0;
    var prefix_end: u64 = 0;
    var prefix_upper: f32 = 0;
    while (queue.peek()) |first| {
        try context.check();
        const doc = streams[first].doc.?;
        if (comptime @hasDecl(@typeInfo(@TypeOf(context)).pointer.child, "nextCandidate")) {
            const candidate = try context.nextCandidate(doc);
            if (candidate > doc) {
                _ = queue.pop();
                try streams[first].seek(candidate);
                if (streams[first].doc != null) try queue.push(a, first);
                continue;
            }
        }
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
            if (cannotBeat(upper, doc, winners.peek().?)) {
                queue.clearRetainingCapacity();
                for (streams, 0..) |*stream, i| {
                    try context.check();
                    try stream.skipThrough(end, stats);
                    if (stream.doc != null) try queue.push(a, i);
                }
                continue;
            }
        }
        if (winners.items.len == k) {
            // Terms with a later next document cannot contribute in this lead
            // range. Current bounds expire at the earliest block end. Strict
            // pruning and canonical f32 addition preserve signed scores/ties.
            // Cache until a term starts contributing or a block bound expires;
            // dense ranges do not pay an all-stream traversal per document.
            if (doc >= prefix_end) {
                prefix_upper = 0;
                prefix_end = @as(u64, std.math.maxInt(u32)) + 1;
                for (streams) |stream| if (stream.doc) |next| {
                    prefix_end = @min(prefix_end, @as(u64, stream.last) + 1);
                    if (next <= doc) prefix_upper += stream.upper else prefix_end = @min(prefix_end, next);
                };
            }
            if (cannotBeat(prefix_upper, doc, winners.peek().?)) {
                while (queue.peek()) |i| {
                    if (streams[i].doc.? != doc) break;
                    _ = queue.pop();
                    try context.check();
                    try streams[i].seek(prefix_end);
                    if (streams[i].doc != null) try queue.push(a, i);
                }
                stats.skipped_prefixes += 1;
                continue;
            }
        }
        var score: f32 = 0;
        var matched = false;
        while (queue.peek()) |i| {
            if (streams[i].doc.? != doc) break;
            _ = queue.pop();
            if (try context.allows(streams[i], doc)) {
                score = try addScore(score, try streams[i].contribution());
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
        std.mem.writeInt(u32, header[4..8], 20, .little);
        std.mem.writeInt(u32, header[0..4], bytes.len, .little);
        try payload.appendSlice(a, &header);
        try payload.appendSlice(a, &bytes);
        var range: [20]u8 = @splat(0);
        @memcpy(range[8..12], "O32U");
        std.mem.writeInt(u32, range[12..16], @intCast(block * 16), .little);
        std.mem.writeInt(u32, range[16..20], @intCast(block * 16 + 15), .little);
        try payload.appendSlice(a, &range);
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

test "finite sparse weights fail with controlled overflow before ranking" {
    var chunks: [2][23]u8 = @splat(@splat(0));
    for (&chunks, [_]f32{ 1e20, -1e20 }) |*bytes, weight| {
        bytes[0] = 1;
        std.mem.writeInt(u32, bytes[1..5], 2, .little);
        std.mem.writeInt(u32, bytes[5..9], @bitCast(weight), .little);
        std.mem.writeInt(u32, bytes[9..13], @bitCast(weight), .little);
        std.mem.writeInt(u32, bytes[17..21], 1, .little);
    }
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, _: u32) !bool {
            return true;
        }
    };
    var context: Context = .{};
    var streams = [_]Stream{ .{ .weight = 1e20, .single = &chunks[0] }, .{ .weight = 1e20, .single = &chunks[1] } };
    var stats: Stats = .{};
    try std.testing.expectError(error.SparseScoreOverflow, collect(std.testing.allocator, &streams, 2, &context, &stats));
    try std.testing.expectError(error.SparseScoreOverflow, addScore(3e38, 3e38));
    try std.testing.expectEqual(std.math.Order.lt, Entry.worse({}, .{ .doc_num = 0, .score = std.math.nan(f32) }, .{ .doc_num = 1, .score = 0 }));
}

test "sparse block prefix pivots skip low lead ranges before later high terms" {
    var first: [13 + 100 * 5]u8 = @splat(0);
    var second: [23]u8 = @splat(0);
    first[0] = 1;
    second[0] = 1;
    std.mem.writeInt(u32, first[1..5], 100, .little);
    std.mem.writeInt(u32, first[5..9], @bitCast(@as(f32, 100)), .little);
    std.mem.writeInt(u32, first[9..13], @bitCast(@as(f32, 1)), .little);
    for (1..100) |i| std.mem.writeInt(u32, first[13 + i * 4 ..][0..4], 1, .little);
    first[413] = 255;
    std.mem.writeInt(u32, second[1..5], 2, .little);
    std.mem.writeInt(u32, second[5..9], @bitCast(@as(f32, 1000)), .little);
    std.mem.writeInt(u32, second[9..13], @bitCast(@as(f32, 1000)), .little);
    std.mem.writeInt(u32, second[17..21], 100, .little);
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, _: u32) !bool {
            return true;
        }
    };
    var context: Context = .{};
    var streams = [_]Stream{ .{ .weight = 1, .single = &first }, .{ .weight = 1, .single = &second } };
    var stats: Stats = .{};
    const result = try collect(std.testing.allocator, &streams, 1, &context, &stats);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqual(@as(u32, 0), result[0].doc_num);
    try std.testing.expectEqual(@as(f32, 1100), result[0].score);
    try std.testing.expectEqual(@as(usize, 1), stats.scored);
    try std.testing.expect(stats.skipped_prefixes + stats.skipped_blocks > 0);
}

test "sparse block prefix pivots match exhaustive signed f32 scores across randomized streams" {
    const a = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x9661017);
    const random = prng.random();
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, doc: u32) !bool {
            return doc % 13 != 0;
        }
    };
    var context: Context = .{};
    const scales = [_]f32{ 0, 1, -1, 1.25, -9, 16777216, -16777216 };
    for (0..100) |_| {
        var chunks: [8][13 + 64 * 5]u8 = @splat(@splat(0));
        var weights: [8]f32 = undefined;
        for (&chunks, &weights, 0..) |*bytes, *weight, term| {
            bytes[0] = 1;
            std.mem.writeInt(u32, bytes[1..5], 64, .little);
            std.mem.writeInt(u32, bytes[5..9], @bitCast(@as(f32, 10)), .little);
            std.mem.writeInt(u32, bytes[9..13], @bitCast(@as(f32, -10)), .little);
            std.mem.writeInt(u32, bytes[13..17], @intCast(term % 4), .little);
            for (1..64) |i| std.mem.writeInt(u32, bytes[13 + i * 4 ..][0..4], 4, .little);
            random.bytes(bytes[269..]);
            weight.* = scales[random.uintLessThan(usize, scales.len)];
        }
        var streams: [8]Stream = undefined;
        for (&streams, &chunks, weights) |*stream, *bytes, weight| stream.* = .{ .weight = weight, .single = bytes };
        var stats: Stats = .{};
        const all = try collect(a, &streams, 256, &context, &stats);
        defer a.free(all);
        for (&streams, &chunks, weights) |*stream, *bytes, weight| stream.* = .{ .weight = weight, .single = bytes };
        const top = try collect(a, &streams, 3, &context, &stats);
        defer a.free(top);
        for (top, all[0..top.len]) |actual, expected| {
            try std.testing.expectEqual(expected.doc_num, actual.doc_num);
            try std.testing.expectEqual(expected.score, actual.score);
        }
    }
}

test "sparse metadata bounds skip posting payloads before decoding" {
    const Reader = struct {
        reads: usize = 0,
        resident: usize = 0,
        fn probe(_: *anyopaque, _: u64, _: u32, lower: u64) !BlockProbe {
            if (lower >= 64) return .end;
            const first: u32 = @intCast(lower / 16 * 16);
            const weight: f32 = if (first == 0) 100 else 1;
            return .{ .block = .{ .first = first, .last = first + 15, .min = weight, .max = weight, .unique = true } };
        }
        fn read(raw: *anyopaque, a: A, _: u64, _: u32, lower: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = try a.alloc(u8, 16 + 13 + 16 * 5);
            @memset(bytes, 0);
            std.mem.writeInt(u32, bytes[0..4], 13 + 16 * 5, .little);
            std.mem.writeInt(u32, bytes[4..8], 8, .little);
            const chunk = bytes[8..];
            chunk[0] = 1;
            std.mem.writeInt(u32, chunk[1..5], 16, .little);
            const first = lower / 16 * 16;
            const weight: f32 = if (first == 0) 100 else 1;
            std.mem.writeInt(u32, chunk[5..9], @bitCast(weight), .little);
            std.mem.writeInt(u32, chunk[9..13], @bitCast(weight), .little);
            for (0..16) |i| std.mem.writeInt(u32, chunk[13 + i * 4 ..][0..4], if (i == 0) @intCast(first) else 1, .little);
            self.reads += 1;
            self.resident += bytes.len;
            return bytes;
        }
    };
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, _: u32) !bool {
            return true;
        }
    };
    var reader: Reader = .{};
    var streams = [_]Stream{.{ .weight = 1, .segment = 1, .term = 1, .allocator = std.testing.allocator, .reader = .{ .ptr = &reader, .resident = &reader.resident, .read = Reader.read, .probe = Reader.probe } }};
    defer streams[0].deinit();
    var context: Context = .{};
    var stats: Stats = .{};
    const top = try collect(std.testing.allocator, &streams, 1, &context, &stats);
    defer std.testing.allocator.free(top);
    try std.testing.expectEqual(@as(u32, 0), top[0].doc_num);
    try std.testing.expectEqual(@as(usize, 1), reader.reads);
    try std.testing.expect(stats.skipped_blocks >= 3);
}

// Deliberately old O32B streams: repeated dimensions were accepted on disk.
fn appendLegacyTestBlock(a: A, payload: *std.ArrayList(u8), docs: []const u32, weight: f32) !void {
    const size = 13 + docs.len * 5;
    const start = payload.items.len;
    try payload.appendNTimes(a, 0, 8 + size + 20);
    const out = payload.items[start..];
    std.mem.writeInt(u32, out[0..4], @intCast(size), .little);
    std.mem.writeInt(u32, out[4..8], 20, .little);
    const chunk = out[8..][0..size];
    chunk[0] = 1;
    std.mem.writeInt(u32, chunk[1..5], @intCast(docs.len), .little);
    std.mem.writeInt(u32, chunk[5..9], @bitCast(weight), .little);
    std.mem.writeInt(u32, chunk[9..13], @bitCast(weight), .little);
    for (docs, 0..) |doc, i| std.mem.writeInt(u32, chunk[13 + i * 4 ..][0..4], if (i == 0) doc else doc - docs[i - 1], .little);
    const range = out[8 + size ..];
    @memcpy(range[8..12], "O32B");
    std.mem.writeInt(u32, range[12..16], docs[0], .little);
    std.mem.writeInt(u32, range[16..20], docs[docs.len - 1], .little);
}
test "sparse legacy repeated ordinals preserve top k within and across blocks" {
    const a = std.testing.allocator;
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, _: u32) !bool {
            return true;
        }
    };
    for ([_]bool{ false, true }) |split| {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(a);
        try appendLegacyTestBlock(a, &payload, &.{ 0, 1, 2 }, 10);
        if (split) {
            try appendLegacyTestBlock(a, &payload, &.{3}, 6);
            try appendLegacyTestBlock(a, &payload, &.{ 3, 4 }, 6);
        } else try appendLegacyTestBlock(a, &payload, &.{ 3, 3, 4 }, 6);
        var ctx: Context = .{};
        var streams = [_]Stream{.{ .weight = 1, .payload = payload.items }};
        var stats: Stats = .{};
        const result = try collect(a, &streams, 1, &ctx, &stats);
        defer a.free(result);
        try std.testing.expectEqual(@as(u32, 3), result[0].doc_num);
        try std.testing.expectEqual(@as(f32, 12), result[0].score);
    }
}
test "sparse exact constant bounds prune equal score losers by ordinal" {
    var bytes: [13 + 1024 * 5]u8 = @splat(0);
    bytes[0] = 1;
    std.mem.writeInt(u32, bytes[1..5], 1024, .little);
    std.mem.writeInt(u32, bytes[5..9], @bitCast(@as(f32, 1)), .little);
    std.mem.writeInt(u32, bytes[9..13], @bitCast(@as(f32, 1)), .little);
    for (1..1024) |i| std.mem.writeInt(u32, bytes[13 + i * 4 ..][0..4], 1, .little);
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: Stream, _: u32) !bool {
            return true;
        }
    };
    var ctx: Context = .{};
    var streams = [_]Stream{.{ .weight = 1, .single = &bytes }};
    var stats: Stats = .{};
    const result = try collect(std.testing.allocator, &streams, 1, &ctx, &stats);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqual(@as(u32, 0), result[0].doc_num);
    try std.testing.expectEqual(@as(usize, 1), stats.scored);
    try std.testing.expect(stats.skipped_blocks > 0);
}
