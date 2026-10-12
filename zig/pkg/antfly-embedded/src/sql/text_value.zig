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

//! Shared literal-text kernels. Inputs borrow a pinned UTF-8 owner. Search,
//! copies and unpublished allocations share invocation work and cancellation.
const std = @import("std");
const Context = @import("numeric_value.zig").Context;
const Allocator = std.mem.Allocator;

pub fn md5(input: []const u8, work: *Context) ![32]u8 {
    try work.charge(0);
    var digest = std.crypto.hash.Md5.init(.{});
    var offset: usize = 0;
    while (offset < input.len) {
        const end = offset + @min(256, input.len - offset);
        try work.charge(end - offset);
        digest.update(input[offset..end]);
        offset = end;
    }
    const bytes = digest.finalResult();
    var output: [32]u8 = undefined;
    const hex = "0123456789abcdef";
    for (bytes, 0..) |byte, i| {
        output[i * 2] = hex[byte >> 4];
        output[i * 2 + 1] = hex[byte & 15];
    }
    return output;
}

/// Caller-owned immutable failure links allow durable plans to compile once.
/// Both needle and links must remain pinned and unchanged during evaluation.
pub const Pattern = struct {
    needle: []const u8,
    links: []const usize,

    pub fn init(needle: []const u8, links: []usize, work: *Context) !Pattern {
        try work.charge(0);
        if (links.len < needle.len) return work.limit();
        if (needle.len != 0) links[0] = 0;
        var matched: usize = 0;
        var i: usize = 1;
        while (i < needle.len) : (i += 1) {
            while (true) {
                try work.charge(1);
                if (needle[i] == needle[matched]) {
                    matched += 1;
                    break;
                }
                if (matched == 0) break;
                matched = links[matched - 1];
            }
            links[i] = matched;
        }
        return .{ .needle = needle, .links = links[0..needle.len] };
    }

    fn next(self: Pattern, input: []const u8, offset: *usize, work: *Context) !?usize {
        std.debug.assert(self.needle.len != 0);
        var matched: usize = 0;
        while (offset.* < input.len) {
            const byte = input[offset.*];
            offset.* += 1;
            while (true) {
                try work.charge(1);
                if (byte == self.needle[matched]) {
                    matched += 1;
                    break;
                }
                if (matched == 0) break;
                matched = self.links[matched - 1];
            }
            // SQL REPLACE selects non-overlapping spans, not suffix overlaps.
            if (matched == self.needle.len) return offset.* - matched;
        }
        return null;
    }

    pub fn replace(self: Pattern, alloc: Allocator, input: []const u8, replacement: []const u8, byte_limit: usize, work: *Context) !Result {
        try work.charge(0);
        if (self.needle.len == 0 or self.needle.len > input.len) return .{ .bytes = input };
        var offset: usize = 0;
        var matches: usize = 0;
        while (try self.next(input, &offset, work) != null) matches += 1;
        if (matches == 0) return .{ .bytes = input };
        const removed = matches * self.needle.len; // disjoint input spans
        const added = std.math.mul(usize, matches, replacement.len) catch return work.limit();
        const size = std.math.add(usize, input.len - removed, added) catch return work.limit();
        if (size > byte_limit) return work.limit();
        const output = try alloc.alloc(u8, size);
        errdefer alloc.free(output);
        offset = 0;
        var copied: usize = 0;
        var written: usize = 0;
        while (try self.next(input, &offset, work)) |start| {
            try copy(output, &written, input[copied..start], work);
            try copy(output, &written, replacement, work);
            copied = offset;
        }
        try copy(output, &written, input[copied..], work);
        std.debug.assert(written == size);
        return .{ .bytes = output, .owned = true, .allocated_bytes = size };
    }
};

pub const Result = struct {
    bytes: []const u8,
    owned: bool = false,
    /// Include scratch capacity retained by an enclosing arena after free.
    allocated_bytes: usize = 0,
};

pub fn replace(alloc: Allocator, input: []const u8, needle: []const u8, replacement: []const u8, byte_limit: usize, work: *Context) !Result {
    try work.charge(0);
    if (needle.len == 0 or needle.len > input.len) return .{ .bytes = input };
    var local: [64]usize = undefined;
    const scratch_bytes = if (needle.len <= local.len) 0 else std.math.mul(usize, needle.len, @sizeOf(usize)) catch return work.limit();
    if (scratch_bytes > byte_limit) return work.limit();
    const links = if (scratch_bytes == 0) local[0..needle.len] else try alloc.alloc(usize, needle.len);
    defer if (scratch_bytes != 0) alloc.free(links);
    const pattern = try Pattern.init(needle, links, work);
    var result = try pattern.replace(alloc, input, replacement, byte_limit - scratch_bytes, work);
    result.allocated_bytes += scratch_bytes;
    return result;
}

fn copy(output: []u8, offset: *usize, input: []const u8, work: *Context) !void {
    var source: usize = 0;
    while (source < input.len) {
        const end = source + @min(256, input.len - source);
        try work.charge(end - source);
        @memcpy(output[offset.*..][0 .. end - source], input[source..end]);
        offset.* += end - source;
        source = end;
    }
}

test "SQL text kernels preserve nonoverlapping replacement and borrowed no-match inputs" {
    const a = std.testing.allocator;
    const cases = [_]struct { input: []const u8, needle: []const u8, replacement: []const u8, expected: []const u8 }{
        .{ .input = "aaaaa", .needle = "aa", .replacement = "X", .expected = "XXa" },
        .{ .input = "ababa", .needle = "aba", .replacement = "!", .expected = "!ba" },
        .{ .input = "hé🍎hé🍎", .needle = "hé", .replacement = "ê", .expected = "ê🍎ê🍎" },
        .{ .input = "abcabc", .needle = "b", .replacement = "XY", .expected = "aXYcaXYc" },
        .{ .input = "aaaa", .needle = "a", .replacement = "", .expected = "" },
        .{ .input = "", .needle = "", .replacement = "x", .expected = "" },
        .{ .input = "abc", .needle = "", .replacement = "x", .expected = "abc" },
        .{ .input = "abc", .needle = "missing", .replacement = "x", .expected = "abc" },
    };
    for (cases) |case| {
        var work: Context = .{ .alloc = a };
        const result = try replace(a, case.input, case.needle, case.replacement, 4096, &work);
        defer if (result.owned) a.free(result.bytes);
        try std.testing.expectEqualStrings(case.expected, result.bytes);
    }
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var work: Context = .{ .alloc = none.allocator() };
    const text = "aaaaaaaaaaaaaaaaaaaaaaaa";
    const result = try replace(none.allocator(), text, "aaaaab", "wide replacement", 0, &work);
    try std.testing.expect(!result.owned);
    try std.testing.expectEqual(@intFromPtr(text.ptr), @intFromPtr(result.bytes.ptr));
    try std.testing.expectEqual(@as(usize, 0), result.allocated_bytes);
    try std.testing.expectEqualStrings("900150983cd24fb0d6963f7d28e17f72", &(try md5("abc", &work)));
}

test "SQL text replacement has linear adversarial search work and one exact-size result" {
    const a = std.testing.allocator;
    var measured: [2]u64 = undefined;
    const needle = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab";
    for ([_]usize{ 4096, 16384 }, 0..) |size, i| {
        const input = try a.alloc(u8, size);
        defer a.free(input);
        @memset(input, 'a');
        var work: Context = .{ .alloc = a };
        const initial = work.remaining;
        var none = std.heap.FixedBufferAllocator.init(&.{});
        const result = try replace(none.allocator(), input, needle, "X", 0, &work);
        try std.testing.expect(!result.owned);
        measured[i] = initial - work.remaining;
        try std.testing.expect(measured[i] <= 2 * (size + needle.len));
    }
    try std.testing.expect(measured[1] <= 4 * measured[0]);
    var exact_buffer: [8]u8 = undefined;
    var exact_memory = std.heap.FixedBufferAllocator.init(&exact_buffer);
    var work: Context = .{ .alloc = exact_memory.allocator() };
    const result = try replace(exact_memory.allocator(), "abcabc", "b", "XY", 8, &work);
    try std.testing.expectEqualStrings("aXYcaXYc", result.bytes);
    try std.testing.expectEqual(@as(usize, 8), result.allocated_bytes);
    std.debug.print("SQL literal replacement search: input=4096 work={} input=16384 work={}\n", .{ measured[0], measured[1] });
}

test "SQL text replacement unwinds allocation faults and every cancellation checkpoint" {
    const a = std.testing.allocator;
    const needle: [80]u8 = @splat('a');
    var text: [1024]u8 = @splat('a');
    text[text.len - 1] = 'b';
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            var work: Context = .{ .alloc = alloc };
            const input: [1024]u8 = @splat('a');
            const search: [80]u8 = @splat('a');
            const result = try replace(alloc, &input, &search, "wide replacement", 4096, &work);
            defer if (result.owned) alloc.free(result.bytes);
            try std.testing.expect(result.owned);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{});
    const Control = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.QueryCanceled;
        }
    };
    var observe: Control = .{};
    var measured: Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &observe };
    const output = try replace(a, &text, &needle, "wide replacement", 4096, &measured);
    defer if (output.owned) a.free(output.bytes);
    for (1..observe.calls + 1) |cut| {
        var control: Control = .{ .fail_at = cut };
        var work: Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &control };
        try std.testing.expectError(error.QueryCanceled, replace(a, &text, &needle, "wide replacement", 4096, &work));
        try std.testing.expectError(error.QueryCanceled, work.charge(0));
    }
}

test "SQL text replacement admission is sticky and distinguishes real allocation failure" {
    const a = std.testing.allocator;
    var work: Context = .{ .alloc = a, .remaining = 4 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, replace(a, "aaaaaaaa", "ab", "x", 4096, &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, work.charge(0));
    work = .{ .alloc = a };
    try std.testing.expectError(error.SqlProgramLimitExceeded, replace(a, "aaaa", "a", "wide", 15, &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, work.charge(0));
    var none = std.heap.FixedBufferAllocator.init(&.{});
    work = .{ .alloc = none.allocator() };
    try std.testing.expectError(error.OutOfMemory, replace(none.allocator(), "aaaa", "a", "wide", 4096, &work));
    try work.charge(0);
}

test "SQL text replacement exhaustive binary witnesses match an independent direct scan" {
    const a = std.testing.allocator;
    var input: [8]u8 = undefined;
    var needle: [4]u8 = undefined;
    var expected: [24]u8 = undefined;
    for (0..256) |input_bits| {
        for (&input, 0..) |*byte, i| byte.* = if (input_bits & (@as(usize, 1) << @intCast(i)) == 0) 'a' else 'b';
        for (0..needle.len + 1) |length| {
            const variants = @as(usize, 1) << @intCast(length);
            for (0..variants) |bits| {
                for (needle[0..length], 0..) |*byte, i| byte.* = if (bits & (@as(usize, 1) << @intCast(i)) == 0) 'a' else 'b';
                for ([_][]const u8{ "", "!", "aba" }) |replacement| {
                    var read: usize = 0;
                    var written: usize = 0;
                    while (read < input.len) {
                        if (length != 0 and length <= input.len - read and std.mem.eql(u8, input[read..][0..length], needle[0..length])) {
                            @memcpy(expected[written..][0..replacement.len], replacement);
                            written += replacement.len;
                            read += length;
                        } else {
                            expected[written] = input[read];
                            written += 1;
                            read += 1;
                        }
                    }
                    var work: Context = .{ .alloc = a };
                    const result = try replace(a, &input, needle[0..length], replacement, expected.len, &work);
                    defer if (result.owned) a.free(result.bytes);
                    try std.testing.expectEqualStrings(expected[0..written], result.bytes);
                }
            }
        }
    }
}

test "SQL text replacement prepared matching uses no per-row heap and bounded throughput work" {
    const a = std.testing.allocator;
    const needle = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab";
    var links: [needle.len]usize = undefined;
    var work: Context = .{ .alloc = a, .remaining = 100_000_000 };
    const pattern = try Pattern.init(needle, &links, &work);
    const input: [16384]u8 = @splat('a');
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const before = work.remaining;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..1024) |_| {
        const result = try pattern.replace(none.allocator(), &input, "X", 0, &work);
        try std.testing.expect(!result.owned);
    }
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started;
    const used = before - work.remaining;
    try std.testing.expect(used <= 1024 * input.len * 2);
    try std.testing.expectEqual(@as(usize, 0), none.end_index);
    std.debug.print("SQL prepared literal replacement: rows=1024 input_bytes={} work={} allocated_bytes=0 elapsed_ns={}\n", .{ input.len * 1024, used, elapsed });
}
