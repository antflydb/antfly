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

//! Query-owned caches and bounded UTF-8 scans. Immutable input allocations
//! remain alive through execution, just like value_work's structural metadata.
const std = @import("std");
const unicode = @import("unicode_string.zig");
const numbers = @import("numbers.zig");
const v = @import("value.zig");
const A = std.mem.Allocator;
const Key = struct { address: usize, length: usize };
const Record = struct {
    analysis: ?unicode.Analysis = null,
    offsets: ?[]const usize = null,
    offset_stride: usize = 0,
    reversed: ?[]const u8 = null,
    lower: ?[]const u8 = null,
    upper: ?[]const u8 = null,
    prefix: ?[]const usize = null,
    integer: ?v.Value = null,
    float: ?v.Value = null,
};
pub const Runtime = struct {
    a: A,
    // Freeable, query-budgeted storage for uncached search tables.
    scratch: A,
    // Conservative retained cost, including each admitted key's metadata.
    prefix_bytes: usize = 0,
    prefix_budget: usize = 256 * 1024,
    ptr: *anyopaque,
    charge: *const fn (*anyopaque, usize) anyerror!void,
    cache: std.AutoHashMapUnmanaged(Key, Record) = .empty,
    fn record(self: *Runtime, text: []const u8) !*Record {
        try self.charge(self.ptr, 1);
        const slot = try self.cache.getOrPut(self.a, .{ .address = @intFromPtr(text.ptr), .length = text.len });
        if (!slot.found_existing) slot.value_ptr.* = .{};
        return slot.value_ptr;
    }
    pub fn reserve(self: *Runtime, bytes: usize, passes: usize) !void {
        try self.charge(self.ptr, (bytes / 64 + @intFromBool(bytes % 64 != 0)) *| passes);
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        try self.charge(self.ptr, 0);
    }
    fn check(self: *Runtime) unicode.Checkpoint {
        return .{ .ptr = self, .call = checkpoint };
    }
    fn analyze(self: *Runtime, text: []const u8) !unicode.Analysis {
        const slot = try self.record(text);
        if (slot.analysis) |value| return value;
        try self.reserve(text.len, 1);
        const value = try unicode.analyze(text, self.check());
        slot.analysis = value;
        return value;
    }
    pub fn size(self: *Runtime, text: []const u8) !usize {
        return (try self.analyze(text)).codepoints;
    }
    pub fn trim(self: *Runtime, text: []const u8, left: bool, right: bool) ![]const u8 {
        const analysis = try self.analyze(text);
        const start = if (left) analysis.first else 0;
        return text[start..if (right) @max(start, analysis.last) else text.len];
    }
    pub fn caseConvert(self: *Runtime, text: []const u8, upper: bool) ![]const u8 {
        const slot = try self.record(text);
        if (if (upper) slot.upper else slot.lower) |value| return value;
        // Final sigma context can inspect the intervening ignorable run too.
        try self.reserve(text.len, 2);
        const value = try unicode.caseConvertWithCheckpoint(self.a, text, upper, self.check());
        if (upper) slot.upper = value else slot.lower = value;
        return value;
    }
    pub fn reverse(self: *Runtime, text: []const u8) ![]const u8 {
        const slot = try self.record(text);
        if (slot.reversed) |value| return value;
        try self.reserve(text.len, 1);
        var iterator = (std.unicode.Utf8View.init(text) catch return error.CypherTypeMismatch).iterator();
        const output = try self.a.alloc(u8, text.len);
        var end = output.len;
        var checked: usize = 0;
        while (iterator.nextCodepointSlice()) |character| {
            try unicode.Checkpoint.check(self.check(), iterator.i, &checked);
            end -= character.len;
            @memcpy(output[end..][0..character.len], character);
        }
        slot.reversed = output;
        return output;
    }
    /// The input owner's cache retains validation and a bounded sparse index.
    /// Dynamic slices borrow bytes, but never register arbitrary slice identities.
    pub fn substring(self: *Runtime, text: []const u8, start: usize, length: usize) ![]const u8 {
        const analysis = try self.analyze(text);
        const first = @min(start, analysis.codepoints);
        const last = first + @min(length, analysis.codepoints - first);
        if (analysis.codepoints == text.len) return text[first..last];
        const slot = try self.record(text);
        if (slot.offsets == null and analysis.codepoints >= 256) {
            const capacity = @min(@as(usize, 1024), (self.prefix_budget -| self.prefix_bytes) / @sizeOf(usize));
            if (capacity >= 2) {
                const stride = @max(@as(usize, 256), std.math.divCeil(usize, analysis.codepoints, capacity - 1) catch unreachable);
                const count = analysis.codepoints / stride + 1;
                const offsets = try self.a.alloc(usize, count);
                try self.reserve(text.len, 1);
                var iterator = (std.unicode.Utf8View{ .bytes = text }).iterator();
                var ordinal: usize = 0;
                var checked: usize = 0;
                offsets[0] = 0;
                while (iterator.nextCodepointSlice() != null) {
                    ordinal += 1;
                    if (ordinal % stride == 0) offsets[ordinal / stride] = iterator.i;
                    try unicode.Checkpoint.check(self.check(), iterator.i, &checked);
                }
                slot.offsets = offsets;
                slot.offset_stride = stride;
                self.prefix_bytes += count * @sizeOf(usize);
            }
        }
        const begin = try self.byteOffset(text, first, slot);
        const end = if (last == first) begin else try self.byteOffset(text, last, slot);
        return text[begin..end];
    }
    fn byteOffset(self: *Runtime, text: []const u8, ordinal: usize, slot: *const Record) !usize {
        const byte_checkpoint = if (slot.offsets) |offsets| offsets[ordinal / slot.offset_stride] else 0;
        const remaining = if (slot.offsets != null) ordinal % slot.offset_stride else ordinal;
        var iterator = (std.unicode.Utf8View{ .bytes = text[byte_checkpoint..] }).iterator();
        var charged: usize = 0;
        for (0..remaining) |_| {
            _ = iterator.nextCodepointSlice();
            if (iterator.i - charged >= 64) {
                try self.reserve(iterator.i - charged, 1);
                charged = iterator.i;
            }
        }
        try self.reserve(iterator.i - charged, 1);
        return byte_checkpoint + iterator.i;
    }
    pub fn integer(self: *Runtime, text: []const u8) !v.Value {
        const slot = try self.record(text);
        if (slot.integer) |value| return value;
        try self.reserve(text.len, 4);
        const value: v.Value = if (try numbers.decimalInteger(text)) |number| .{ .integer = number } else .null;
        slot.integer = value;
        return value;
    }
    pub fn float(self: *Runtime, text: []const u8) !v.Value {
        const slot = try self.record(text);
        if (slot.float) |value| return value;
        try self.reserve(text.len, 4);
        const value: v.Value = if (numbers.floatFromString(text)) |number| if (std.math.isFinite(number)) .{ .number = number } else .null else .null;
        slot.float = value;
        return value;
    }
    /// KMP bounds adversarial repeated-prefix searches to O(haystack+needle).
    /// Cache the needle's prefix table; each haystack scan still pays its work.
    pub fn contains(self: *Runtime, haystack: []const u8, needle: []const u8) !bool {
        try self.charge(self.ptr, 1);
        if (needle.len == 0) return true;
        if (needle.len > haystack.len) return false;
        const key: Key = .{ .address = @intFromPtr(needle.ptr), .length = needle.len };
        // Do not create memo entries for one-shot needles after exhausting the
        // byte budget. Existing cached tables remain useful on repeated calls.
        const cached = if (self.cache.get(key)) |record_value| record_value.prefix else null;
        var temporary: ?[]usize = null;
        defer if (temporary) |table| self.scratch.free(table);
        const prefix = cached orelse table: {
            try self.reserve(needle.len, 2);
            const bytes = std.math.mul(usize, needle.len, @sizeOf(usize)) catch return error.OutOfMemory;
            // Include a conservative share of hash-table metadata, so many
            // tiny needles cannot turn a byte bound into an unbounded key set.
            const cost = bytes +| (4 * (@sizeOf(Key) + @sizeOf(Record) + @sizeOf(usize)));
            const retain = cost <= self.prefix_budget -| self.prefix_bytes;
            const allocator = if (retain) self.a else self.scratch;
            const table = try allocator.alloc(usize, needle.len);
            if (!retain) temporary = table;
            table[0] = 0;
            var matched: usize = 0;
            for (needle[1..], 1..) |byte, index| {
                if (index % (16 * 1024) == 0) try checkpoint(self);
                while (matched > 0 and needle[matched] != byte) matched = table[matched - 1];
                if (needle[matched] == byte) matched += 1;
                table[index] = matched;
            }
            if (retain) {
                (try self.record(needle)).prefix = table;
                self.prefix_bytes += cost;
            }
            break :table table;
        };
        try self.reserve(haystack.len, 2);
        var matched: usize = 0;
        for (haystack, 0..) |byte, index| {
            if (index % (16 * 1024) == 0) try checkpoint(self);
            while (matched > 0 and needle[matched] != byte) matched = prefix[matched - 1];
            if (needle[matched] == byte) matched += 1;
            if (matched == needle.len) return true;
        }
        return false;
    }
};

test "immutable Unicode analysis and transforms are cached while cache hits remain cancellable" {
    const Counter = struct {
        units: usize = 0,
        canceled: bool = false,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.canceled) return error.Canceled;
            self.units += amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var counter: Counter = .{};
    var runtime: Runtime = .{ .a = a, .scratch = std.testing.allocator, .ptr = &counter, .charge = Counter.charge };
    const text = try a.alloc(u8, 400_000);
    @memset(text, 'x');
    for (0..1000) |_| {
        try std.testing.expectEqual(@as(usize, 400_000), try runtime.size(text));
        try std.testing.expectEqualStrings(text, try runtime.trim(text, true, true));
        _ = try runtime.caseConvert(text, true);
        _ = try runtime.reverse(text);
    }
    try std.testing.expect(counter.units < 30_000);
    try std.testing.expectEqual(@as(usize, 4), try runtime.size(" ÉΣ "));
    try std.testing.expectEqualStrings("ÉΣ", try runtime.trim(" ÉΣ ", true, true));
    try std.testing.expectEqualStrings("Σ", try runtime.substring(" ÉΣ ", 2, 1));
    try std.testing.expectEqualStrings("", try runtime.substring("ÉΣ", std.math.maxInt(usize), 1));
    try std.testing.expectEqual(@as(i64, 42), (try runtime.integer("42.9")).integer);
    try std.testing.expectEqual(@as(f64, 42.9), (try runtime.float("42.9")).number);
    counter.canceled = true;
    try std.testing.expectError(error.Canceled, runtime.size(text));
    try std.testing.expectError(error.Canceled, runtime.trim(text, true, true));
    try std.testing.expectError(error.Canceled, runtime.caseConvert(text, true));
    try std.testing.expectError(error.Canceled, runtime.reverse(text));
    try std.testing.expectError(error.Canceled, runtime.integer("42.9"));
}

test "uncached scans reserve work and checkpoint within long UTF-8 and repeated-prefix searches" {
    const Counter = struct {
        units: usize = 0,
        calls: usize = 0,
        limit: usize = 1_000_000,
        cancel_at: ?usize = null,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.cancel_at) |stop| if (self.calls >= stop) return error.Canceled;
            if (amount > self.limit -| self.units) return error.CypherStateBudgetExceeded;
            self.units += amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const text = try a.alloc(u8, 256_000);
    @memset(text, 'a');
    const needle = try a.dupe(u8, text[0..128_000]);
    needle[0] = 'b';
    var counter: Counter = .{};
    var runtime: Runtime = .{ .a = a, .scratch = std.testing.allocator, .ptr = &counter, .charge = Counter.charge };
    try std.testing.expect(!try runtime.contains(text, needle));
    try std.testing.expect(counter.units < 13_000);
    try std.testing.expect(counter.calls > 10);
    try std.testing.expect(try runtime.contains("ababababac", "ababac"));
    try std.testing.expect(try runtime.contains("ÉΣ", "Σ"));
    try std.testing.expect(try runtime.contains("", ""));
    try std.testing.expect(!try runtime.contains("a", "ab"));
    counter.limit = counter.units;
    try std.testing.expectError(error.CypherStateBudgetExceeded, runtime.contains(text, needle));
    counter = .{ .cancel_at = 4 };
    try std.testing.expectError(error.Canceled, runtime.size(text));
    counter = .{ .cancel_at = 4 };
    try std.testing.expectError(error.Canceled, runtime.caseConvert(text, false));
    counter = .{ .limit = 1 };
    try std.testing.expectError(error.CypherStateBudgetExceeded, runtime.reverse(text));
}

test "search memo admission is byte bounded and saturated needles use freeable scratch" {
    const Counter = struct {
        units: usize = 0,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.units += amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var counter: Counter = .{};
    var runtime: Runtime = .{ .a = arena.allocator(), .scratch = std.testing.allocator, .ptr = &counter, .charge = Counter.charge, .prefix_budget = 4096 };
    // Distinct, long-lived allocations exercise identity-key admission. Every
    // fallback is freed independently of the statement arena (including errors).
    const haystack: [2048]u8 = @splat('z');
    for (0..100) |i| {
        const needle = try arena.allocator().alloc(u8, 1024);
        @memset(needle, 'a');
        needle[1023] = @intCast(i);
        try std.testing.expect(!try runtime.contains(&haystack, needle));
    }
    try std.testing.expect(runtime.prefix_bytes <= runtime.prefix_budget);
    try std.testing.expectEqual(@as(usize, 0), runtime.cache.count());
    try std.testing.expect(try runtime.contains("ababababac", "ababac"));
    const before = counter.units;
    try std.testing.expect(try runtime.contains("ababababac", "ababac"));
    try std.testing.expect(counter.units - before < 5);
    try std.testing.expectEqual(@as(usize, 1), runtime.cache.count());
}

test "cached and temporary search tables release every failed allocation" {
    const Campaign = struct {
        fn charge(_: *anyopaque, _: usize) !void {}
        fn run(a: A, budget: usize) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var dummy: u8 = 0;
            var runtime: Runtime = .{ .a = arena.allocator(), .scratch = a, .ptr = &dummy, .charge = charge, .prefix_budget = budget };
            try std.testing.expect(try runtime.contains("ababababac", "ababac"));
            try std.testing.expect(!try runtime.contains("ababababac", "ababaz"));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{@as(usize, 0)});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{@as(usize, 4096)});
}

test "sparse substring indices are bounded and fallback preserves Unicode windows" {
    const Counter = struct {
        units: usize = 0,
        fn charge(ptr: *anyopaque, n: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.units += n;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const text = try arena.allocator().alloc(u8, 60000);
    for (0..text.len / 6) |i| @memcpy(text[i * 6 ..][0..6], "é🙂");
    for ([_]usize{ 0, 16, 8192 }) |budget| {
        var counter: Counter = .{};
        var runtime: Runtime = .{ .a = arena.allocator(), .scratch = std.testing.allocator, .ptr = &counter, .charge = Counter.charge, .prefix_budget = budget };
        try std.testing.expectEqualStrings("é🙂", try runtime.substring(text, 10000, 2));
        try std.testing.expectEqualStrings("🙂", try runtime.substring(text, 19999, 100));
        try std.testing.expectEqualStrings("", try runtime.substring(text, 20000, 1));
        try std.testing.expectEqualStrings("", try runtime.substring(text, std.math.maxInt(usize), std.math.maxInt(usize)));
        try std.testing.expectEqualStrings("", try runtime.substring(text, 3, 0));
        try std.testing.expect(runtime.prefix_bytes <= budget);
        const previous = counter.units;
        for (0..50) |_| try std.testing.expectEqualStrings("é", try runtime.substring(text, 10000, 1));
        if (budget == 8192) try std.testing.expect(counter.units - previous < 1000);
    }
}

test "substring validation and index construction release failed allocations" {
    const Campaign = struct {
        fn charge(_: *anyopaque, _: usize) !void {}
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const text = try arena.allocator().alloc(u8, 1800);
            for (0..text.len / 6) |i| @memcpy(text[i * 6 ..][0..6], "é🙂");
            var dummy: u8 = 0;
            var runtime: Runtime = .{ .a = arena.allocator(), .scratch = a, .ptr = &dummy, .charge = charge };
            try std.testing.expectEqualStrings("🙂é", try runtime.substring(text, 301, 2));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}
