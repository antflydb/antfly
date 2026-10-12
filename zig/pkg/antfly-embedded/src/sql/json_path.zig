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

//! Shared PostgreSQL JSONB path component parsing. Reads treat an invalid
//! ordinal as a missing path; writes retain the invalid-input SQLSTATE.
const std = @import("std");
const Json = std.json.Value;
const Budget = @import("json_order.zig").Budget;

/// One structural read from an already decoded, pinned JSON operand. A missing
/// child is SQL NULL; a present `.null` child remains JSON null. No parsing,
/// allocation or output serialization occurs here. Callers must still evaluate
/// all selected SQL arguments after a missing child or SQL NULL path component.
pub fn extract(value: Json, key: []const u8, work: *Budget) !?Json {
    try work.consume(1);
    try work.consume(key.len);
    return switch (value) {
        .object => |object| object.get(key),
        .array => |array| element: {
            const index = ordinal(key) catch break :element null;
            const count: i64 = @intCast(array.items.len);
            const position = if (index < 0) count + index else index;
            if (position < 0 or position >= count) break :element null;
            break :element array.items[@intCast(position)];
        },
        else => null,
    };
}

pub fn ordinal(key: []const u8) !i32 {
    // strtoint accepts leading ASCII whitespace and a sign, but neither
    // trailing whitespace nor Zig's digit separators/base prefixes.
    const digits = std.mem.trimStart(u8, key, " \t\n\r\x0b\x0c");
    const start: usize = if (digits.len > 0 and (digits[0] == '+' or digits[0] == '-')) 1 else 0;
    if (digits.len == start) return error.SqlInvalidTextRepresentation;
    for (digits[start..]) |c| if (c < '0' or c > '9') return error.SqlInvalidTextRepresentation;
    return std.fmt.parseInt(i32, digits, 10) catch return error.SqlInvalidTextRepresentation;
}

test "SQL JSON path kernel preserves missing versus JSON null and borrowed exact tokens" {
    const a = std.testing.allocator;
    const text = "{\"a\":[9007199254740993.2500,null,\"last\"],\"01\":true}";
    var parsed = try std.json.parseFromSlice(Json, a, text, .{ .parse_numbers = false });
    defer parsed.deinit();
    var work: Budget = .{ .remaining = 1_000_000 };
    const array = (try extract(parsed.value, "a", &work)).?;
    const number = (try extract(array, "0", &work)).?;
    try std.testing.expectEqualStrings("9007199254740993.2500", number.number_string);
    try std.testing.expectEqual(@intFromPtr(array.array.items[0].number_string.ptr), @intFromPtr(number.number_string.ptr));
    try std.testing.expectEqual(Json.null, (try extract(array, "1", &work)).?);
    try std.testing.expectEqualStrings("last", (try extract(array, "-1", &work)).?.string);
    try std.testing.expectEqual(true, (try extract(parsed.value, "01", &work)).?.bool);
    try std.testing.expectEqual(@as(?Json, null), try extract(parsed.value, "missing", &work));
    try std.testing.expectEqual(@as(?Json, null), try extract(.null, "a", &work));
    for ([_][]const u8{ "3", "-4", "2147483648", "-2147483648", "1 ", "1_0", "", "+", "0x1" }) |key| {
        try std.testing.expectEqual(@as(?Json, null), try extract(array, key, &work));
    }
    for ([_][]const u8{ " 1", "+1", "01", "-2", "\t1" }) |key| {
        try std.testing.expectEqual(Json.null, (try extract(array, key, &work)).?);
    }
    // Repeated reads reuse the decoded source and borrow their exact payload.
    // The API intentionally has no allocator; wide source JSON is not copied.
    for (0..10000) |_| {
        const result = (try extract(array, "0", &work)).?;
        try std.testing.expectEqual(@intFromPtr(number.number_string.ptr), @intFromPtr(result.number_string.ptr));
    }
}

test "SQL JSON path kernel retains shared cancellation and work admission" {
    const Control = struct {
        polls: usize = 0,
        fail_at: usize,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    const key: [256]u8 = @splat('x');
    for (1..3) |cut| {
        var control: Control = .{ .fail_at = cut };
        var context: @import("numeric_value.zig").Context = .{ .alloc = std.testing.failing_allocator, .checkpoint = Control.check, .ptr = &control };
        var work: Budget = .{ .shared = &context };
        try std.testing.expectError(error.QueryCanceled, extract(.null, &key, &work));
        try std.testing.expectError(error.QueryCanceled, extract(.null, &key, &work));
    }
    var context: @import("numeric_value.zig").Context = .{ .alloc = std.testing.failing_allocator, .remaining = 1 };
    var work: Budget = .{ .shared = &context };
    try std.testing.expectError(error.SqlProgramLimitExceeded, extract(.null, "key", &work));
    context.remaining = 100;
    try std.testing.expectError(error.SqlProgramLimitExceeded, extract(.null, "key", &work));
}
