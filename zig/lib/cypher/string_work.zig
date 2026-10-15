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
    reversed: ?[]const u8 = null,
    lower: ?[]const u8 = null,
    upper: ?[]const u8 = null,
    prefix: ?[]const usize = null,
    integer: ?v.Value = null,
    float: ?v.Value = null,
};
pub const Runtime = struct {
    a: A,
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
    pub fn substring(self: *Runtime, text: []const u8, start: usize, length: usize) ![]const u8 {
        // Slices can be borrowed: the input is immutable for the whole query.
        try self.reserve(text.len, 1);
        var iterator = (std.unicode.Utf8View.init(text) catch return error.CypherTypeMismatch).iterator();
        var position: usize = 0;
        var checked: usize = 0;
        while (position < start and iterator.nextCodepointSlice() != null) : (position += 1) try unicode.Checkpoint.check(self.check(), iterator.i, &checked);
        const begin = iterator.i;
        position = 0;
        while (position < length and iterator.nextCodepointSlice() != null) : (position += 1) try unicode.Checkpoint.check(self.check(), iterator.i, &checked);
        return text[begin..iterator.i];
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
        const slot = try self.record(needle);
        if (slot.prefix == null) {
            try self.reserve(needle.len, 2);
            const prefix = try self.a.alloc(usize, needle.len);
            prefix[0] = 0;
            var matched: usize = 0;
            for (needle[1..], 1..) |byte, index| {
                if (index % (16 * 1024) == 0) try checkpoint(self);
                while (matched > 0 and needle[matched] != byte) matched = prefix[matched - 1];
                if (needle[matched] == byte) matched += 1;
                prefix[index] = matched;
            }
            slot.prefix = prefix;
        }
        try self.reserve(haystack.len, 2);
        const prefix = slot.prefix.?;
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
    var runtime: Runtime = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
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
    var runtime: Runtime = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
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
