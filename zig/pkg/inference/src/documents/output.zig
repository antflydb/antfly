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

//! Bounded JSON output with amortized growth, no intermediate JSON buffers,
//! and a zero-allocation counter for shared output admission.
const std = @import("std");
const A = std.mem.Allocator;

pub const Writer = struct {
    storage: std.Io.Writer.Allocating,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain, .flush = std.Io.Writer.noopFlush }, .buffer = &.{} },
    limit: usize,
    failure: ?anyerror = null,

    pub fn init(a: A, limit: usize) Writer {
        return .{ .storage = .init(a), .limit = limit };
    }
    pub fn deinit(self: *Writer) void {
        self.storage.deinit();
    }
    pub fn len(self: *const Writer) usize {
        return self.writer.end;
    }
    pub fn setLimit(self: *Writer, limit: usize) !void {
        if (limit < self.len()) return error.DocumentOutputTooLarge;
        self.limit = limit;
        self.writer.buffer = self.storage.writer.buffer[0..@min(limit, self.storage.writer.buffer.len)];
    }
    pub fn raw(self: *Writer, bytes: []const u8) !void {
        self.writer.writeAll(bytes) catch |err| return self.failure orelse err;
    }
    pub fn value(self: *Writer, item: anytype) !void {
        if (@TypeOf(item) == std.json.Value) try validateValue(item, self.limit -| self.len());
        std.json.Stringify.value(item, .{}, &self.writer) catch |err| return self.failure orelse err;
    }
    pub fn begin(self: *Writer, object: []const u8, model: []const u8) !void {
        try self.raw("{\"object\":");
        try self.value(object);
        try self.raw(",\"model\":");
        try self.value(model);
        try self.raw(",\"data\":[");
    }
    /// Credit was already admitted when the child produced its payload. Only
    /// new decoration and punctuation consume another shared allowance.
    pub fn append(self: *Writer, item: anytype, remaining: usize, credit: usize, comma: bool) !usize {
        const before = self.len();
        const outer_limit = self.limit;
        try self.setLimit(@min(outer_limit, before +| remaining +| credit));
        defer self.setLimit(outer_limit) catch unreachable;
        if (comma) try self.raw(",");
        try self.value(item);
        return self.len() - before;
    }
    pub fn toOwnedSlice(self: *Writer) ![]u8 {
        self.storage.writer.end = self.writer.end;
        const result = try self.storage.toOwnedSlice();
        self.writer.buffer = &.{};
        self.writer.end = 0;
        return result;
    }
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *Writer = @fieldParentPtr("writer", w);
        const count = countWithin(data, splat, self.limit -| w.end) catch {
            self.failure = error.DocumentOutputTooLarge;
            return error.WriteFailed;
        };
        const needed = w.end + count;
        const capacity = self.storage.writer.buffer.len;
        const grown = @min(self.limit, @max(needed, @max(256, capacity +| capacity / 2)));
        self.storage.writer.end = w.end;
        self.storage.ensureTotalCapacityPrecise(grown) catch {
            self.failure = error.OutOfMemory;
            return error.WriteFailed;
        };
        w.buffer = self.storage.writer.buffer[0..@min(self.limit, self.storage.writer.buffer.len)];
        return w.writeSplat(data, splat);
    }
};

fn countWithin(data: []const []const u8, splat: usize, limit: usize) !usize {
    var total: usize = 0;
    for (data[0 .. data.len - 1]) |bytes| {
        if (bytes.len > limit - total) return error.DocumentOutputTooLarge;
        total += bytes.len;
    }
    const repeated = std.math.mul(usize, data[data.len - 1].len, splat) catch return error.DocumentOutputTooLarge;
    if (repeated > limit - total) return error.DocumentOutputTooLarge;
    return total + repeated;
}

pub fn measure(item: anytype, limit: usize) !usize {
    if (@TypeOf(item) == std.json.Value) try validateValue(item, limit);
    const Counter = struct {
        writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
        count: usize = 0,
        limit: usize,
        fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @fieldParentPtr("writer", w);
            const count = countWithin(data, splat, self.limit - self.count) catch return error.WriteFailed;
            self.count += count;
            return count;
        }
    };
    var counter = Counter{ .limit = limit };
    std.json.Stringify.value(item, .{}, &counter.writer) catch return error.DocumentOutputTooLarge;
    return counter.count;
}

// Untrusted metadata can be deeper than std.json.Stringify's fixed stack.
// Each value needs at least one wire byte, so the byte ceiling also bounds
// this allocation-free preflight's work before the encoder checks exact bytes.
fn validateValue(value: std.json.Value, limit: usize) !void {
    var remaining = limit;
    try validateValueDepth(value, 0, &remaining);
}
fn validateValueDepth(value: std.json.Value, depth: usize, remaining: *usize) error{DocumentOutputTooLarge}!void {
    if (depth > 128 or remaining.* == 0) return error.DocumentOutputTooLarge;
    remaining.* -= 1;
    switch (value) {
        .array => |array| for (array.items) |child| try validateValueDepth(child, depth + 1, remaining),
        .object => |object| for (object.values()) |child| try validateValueDepth(child, depth + 1, remaining),
        else => {},
    }
}

pub fn valueAlloc(a: A, item: anytype, limit: usize) ![]u8 {
    var out = Writer.init(a, limit);
    defer out.deinit();
    try out.value(item);
    return out.toOwnedSlice();
}

test "document JSON output bounds escaped and exact numeric values without intermediate buffers" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"amount\":9007199254740993.01,\"text\":\"quote\\\"\\n\\u0001\"}", .{ .parse_numbers = false });
    const expected = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(expected);
    try std.testing.expectEqual(expected.len, try measure(value, expected.len));
    try std.testing.expectError(error.DocumentOutputTooLarge, measure(value, expected.len - 1));
    const actual = try valueAlloc(a, value, expected.len);
    defer a.free(actual);
    try std.testing.expectEqualStrings(expected, actual);
    try std.testing.expectError(error.DocumentOutputTooLarge, valueAlloc(a, value, expected.len - 1));
    try std.testing.expectError(error.DocumentOutputTooLarge, valueAlloc(std.testing.failing_allocator, value, 0));
    try std.testing.expectError(error.DocumentOutputTooLarge, countWithin(&.{"xx"}, std.math.maxInt(usize), std.math.maxInt(usize)));
}

test "document JSON output limits allocation growth and credits only admitted payload bytes" {
    const Guard = struct {
        limit: usize,
        largest: usize = 0,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.largest = @max(self.largest, len);
            std.debug.assert(len <= self.limit);
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
    };
    var guard = Guard{ .limit = 1000 };
    const a = A{ .ptr = &guard, .vtable = &.{ .alloc = Guard.alloc, .free = Guard.free, .resize = Guard.resize, .remap = Guard.remap } };
    var out = Writer.init(a, guard.limit);
    defer out.deinit();
    const block: [700]u8 = @splat('x');
    try out.raw(&block);
    try out.writer.flush();
    try std.testing.expectEqual(block.len, out.len());
    try std.testing.expectError(error.DocumentOutputTooLarge, out.raw(&block));
    try std.testing.expect(guard.largest <= guard.limit);
    var admitted = Writer.init(std.testing.allocator, 64);
    defer admitted.deinit();
    try std.testing.expectEqual(@as(usize, 5), try admitted.append("abc", 0, 5, false));
    try std.testing.expectError(error.DocumentOutputTooLarge, admitted.append("x", 0, 0, true));
}

test "document JSON output allocation failures release partially serialized responses" {
    const Runner = struct {
        fn run(a: A) !void {
            var out = Writer.init(a, 8192);
            defer out.deinit();
            try out.begin("list", "reader");
            const text: [700]u8 = @splat('x');
            try out.value(.{ .text = &text });
            try out.raw("]}");
            const bytes = try out.toOwnedSlice();
            defer a.free(bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "document JSON output rejects deeply nested metadata before allocation or encoder recursion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var value: std.json.Value = .null;
    for (0..200) |_| {
        var array = std.json.Array.init(a);
        try array.append(value);
        value = .{ .array = array };
    }
    try std.testing.expectError(error.DocumentOutputTooLarge, measure(value, 8192));
    try std.testing.expectError(error.DocumentOutputTooLarge, valueAlloc(std.testing.failing_allocator, value, 8192));
}
