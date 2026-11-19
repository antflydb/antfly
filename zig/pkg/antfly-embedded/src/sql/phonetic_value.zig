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

//! PostgreSQL fuzzystrmatch-compatible ASCII Soundex. UTF-8 bytes outside the
//! ASCII alphabet are separators, not transliterations or locale-dependent text.
const std = @import("std");
const Context = @import("numeric_value.zig").Context;
pub const Code = struct {
    bytes: [4]u8 = .{ '0', '0', '0', '0' },
    len: u3 = 0,
    pub fn slice(self: *const Code) []const u8 {
        return self.bytes[0..self.len];
    }
};
fn group(byte: u8) u8 {
    return switch (std.ascii.toUpper(byte)) {
        'B', 'F', 'P', 'V' => '1',
        'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z' => '2',
        'D', 'T' => '3',
        'L' => '4',
        'M', 'N' => '5',
        'R' => '6',
        else => if (std.ascii.isAlphabetic(byte)) '0' else byte,
    };
}
/// Fixed-size output and no scratch allocation. Stop after the fourth output
/// character; an irrelevant suffix must not turn a bounded code into a full scan.
pub fn soundex(input: []const u8, work: *Context) !Code {
    try work.charge(0);
    var result: Code = .{};
    var previous: u8 = '0';
    var position: usize = 0;
    var written: usize = 0;
    while (position < input.len and written < 4) : (position += 1) {
        try work.charge(1);
        const byte = input[position];
        if (written == 0) {
            if (!std.ascii.isAlphabetic(byte)) continue;
            result.bytes[0] = std.ascii.toUpper(byte);
            written = 1;
            previous = group(byte);
            continue;
        }
        const current = group(byte);
        if (std.ascii.isAlphabetic(byte) and current != '0' and current != previous) {
            result.bytes[written] = current;
            written += 1;
        }
        previous = current;
    }
    result.len = if (written == 0) 0 else 4;
    return result;
}
pub fn difference(left: []const u8, right: []const u8, work: *Context) !i32 {
    const first = try soundex(left, work);
    const second = try soundex(right, work);
    var matches: i32 = 0;
    for (first.slice()[0..@min(first.len, second.len)], second.slice()[0..@min(first.len, second.len)]) |a, b| matches += @intFromBool(a == b);
    return matches;
}

test "SQL phonetic fixed codes preserve PostgreSQL separator and ASCII semantics without scratch allocation" {
    var empty: std.heap.FixedBufferAllocator = .init(&.{});
    var work: Context = .{ .alloc = empty.allocator(), .remaining = 100000 };
    const cases = .{ .{ "Robert", "R163" }, .{ "Ashcraft", "A226" }, .{ "Pfister", "P236" }, .{ "BHB", "B100" }, .{ "BWB", "B100" }, .{ "B-B", "B100" }, .{ "123", "" }, .{ "é", "" }, .{ "Ångström", "N236" } };
    inline for (cases) |entry| {
        const code = try soundex(entry[0], &work);
        try std.testing.expectEqualStrings(entry[1], code.slice());
    }
    try std.testing.expectEqual(@as(i32, 4), try difference("Robert", "Rupert", &work));
    try std.testing.expectEqual(@as(i32, 0), try difference("", "", &work));
    var tiny: Context = .{ .alloc = empty.allocator(), .remaining = 4 };
    var suffix: [6004]u8 = @splat('x');
    @memcpy(suffix[0..4], "ABCD");
    const code = try soundex(&suffix, &tiny);
    try std.testing.expectEqualStrings("A123", code.slice());
    try std.testing.expectEqual(@as(u64, 0), tiny.remaining);
}

test "SQL phonetic native kernels match the complete independent PostgreSQL witness" {
    const a = std.testing.allocator;
    var fixture = try std.json.parseFromSlice(struct { values: []const struct { input: []const u8, soundex: []const u8, difference: i32 } }, a, @import("parity_fixtures.zig").phonetic_reference, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expect(fixture.value.values.len > 2000);
    var empty: std.heap.FixedBufferAllocator = .init(&.{});
    for (fixture.value.values) |entry| {
        var work: Context = .{ .alloc = empty.allocator(), .remaining = 10000 };
        const code = try soundex(entry.input, &work);
        errdefer std.debug.print("phonetic input={s}\n", .{entry.input});
        try std.testing.expectEqualStrings(entry.soundex, code.slice());
        try std.testing.expectEqual(entry.difference, try difference(entry.input, "Robert", &work));
    }
}

test "SQL phonetic work accounting observes cancellation limits and sticky failure" {
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var work: Context = .{ .alloc = std.testing.allocator, .remaining = 10000, .checkpoint = Cancel.check };
    try std.testing.expectError(error.Cancelled, soundex("Robert", &work));
    work = .{ .alloc = std.testing.allocator, .remaining = 2 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, soundex("000A", &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, soundex("A", &work));
}
