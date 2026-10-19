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

//! Bounded scans for legacy scalar input, retaining its exact grammar and work.
const std = @import("std");
const Context = @import("numeric_value.zig").Context;

/// Preserve UTF-8 validation semantics, including NUL, without spending the
/// scalar's node/work allowance a second time on its already admitted bytes.
pub fn validateUtf8(text: []const u8, context: *Context) !void {
    return validateUtf8Inner(text, context, false);
}
pub fn validateDomainText(text: []const u8, context: *Context) !void {
    validateUtf8Inner(text, context, true) catch |err| return if (err == error.SqlTypeMismatch) error.SqlInvalidTextEncoding else err;
}
fn validateUtf8Inner(text: []const u8, context: *Context, reject_nul: bool) !void {
    var offset: usize = 0;
    var next_poll: usize = 0;
    while (offset < text.len) {
        if (offset >= next_poll) {
            try context.checkpointNow();
            next_poll = offset +| 4096;
        }
        if (reject_nul and text[offset] == 0) return error.SqlUntranslatableCharacter;
        const length = std.unicode.utf8ByteSequenceLength(text[offset]) catch return error.SqlTypeMismatch;
        if (length > text.len - offset) return error.SqlTypeMismatch;
        _ = std.unicode.utf8Decode(text[offset..][0..length]) catch return error.SqlTypeMismatch;
        offset += length;
    }
    try context.checkpointNow();
}

/// Same decimal/sign/interior-underscore grammar and signed range as parseInt,
/// with bounded cancellation points even for arbitrarily many leading zeros.
pub fn integer(text: []const u8, context: *Context) !i64 {
    try context.checkpointNow();
    if (text.len == 0) return error.SqlTypeMismatch;
    const negative = text[0] == '-';
    const start: usize = if (negative or text[0] == '+') 1 else 0;
    if (start == text.len or text[start] == '_' or text[text.len - 1] == '_') return error.SqlTypeMismatch;
    const maximum: u64 = if (negative) @as(u64, std.math.maxInt(i64)) + 1 else std.math.maxInt(i64);
    var magnitude: u64 = 0;
    for (text[start..], 0..) |byte, index| {
        if (index % 1024 == 0) try context.checkpointNow();
        if (byte == '_') continue;
        const digit = std.fmt.charToDigit(byte, 10) catch return error.SqlTypeMismatch;
        if (magnitude > (maximum - digit) / 10) return error.SqlTypeMismatch;
        magnitude = magnitude * 10 + digit;
    }
    try context.checkpointNow();
    if (!negative) return @intCast(magnitude);
    if (magnitude == @as(u64, std.math.maxInt(i64)) + 1) return std.math.minInt(i64);
    return -@as(i64, @intCast(magnitude));
}

const Probe = struct {
    calls: usize = 0,
    refuse: usize = 0,
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls == self.refuse) return error.QueryCanceled;
    }
    fn context(self: *@This()) Context {
        return .{ .alloc = std.testing.allocator, .remaining = 17, .checkpoint = check, .ptr = self };
    }
};

test "scalar input integer preserves parseInt decimal grammar range and work" {
    var probe: Probe = .{};
    var context = probe.context();
    for ([_][]const u8{ "", "+", "-", "_0", "0_", "+_0", "-0", "00", "0__0", "01_2", " 1", "1 ", "0x10", "a", "9223372036854775807", "-9223372036854775808", "9223372036854775808", "-9223372036854775809", "\x00" }) |text| {
        if (std.fmt.parseInt(i64, text, 10)) |expected| {
            try std.testing.expectEqual(expected, try integer(text, &context));
        } else |_| try std.testing.expectError(error.SqlTypeMismatch, integer(text, &context));
    }
    // Exercise all pairs of grammar-relevant bytes, not only chosen examples.
    const alphabet = "0129_+- a";
    for (alphabet) |first| for (alphabet) |second| {
        const text = [_]u8{ first, second };
        if (std.fmt.parseInt(i64, &text, 10)) |expected| {
            try std.testing.expectEqual(expected, try integer(&text, &context));
        } else |_| try std.testing.expectError(error.SqlTypeMismatch, integer(&text, &context));
    };
    try std.testing.expectEqual(@as(u64, 17), context.remaining);
}

test "scalar input scans cancel at every checkpoint with sticky parent authority" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 4096 * 3 + 1);
    defer a.free(text);
    @memset(text, '0');
    for ([_]bool{ false, true }) |integer_mode| {
        var probe: Probe = .{};
        var parent = probe.context();
        var child: Context = .{ .alloc = a, .remaining = 11, .parent = &parent };
        if (integer_mode) {
            try std.testing.expectEqual(@as(i64, 0), try integer(text, &child));
        } else try validateUtf8(text, &child);
        const calls = probe.calls;
        try std.testing.expect(calls >= 5);
        for (1..calls + 1) |failure| {
            var interrupted: Probe = .{ .refuse = failure };
            var retained = interrupted.context();
            var nested: Context = .{ .alloc = a, .remaining = 11, .parent = &retained };
            if (integer_mode) {
                try std.testing.expectError(error.QueryCanceled, integer(text, &nested));
            } else try std.testing.expectError(error.QueryCanceled, validateUtf8(text, &nested));
            try std.testing.expectError(error.QueryCanceled, retained.charge(0));
            try std.testing.expectError(error.QueryCanceled, nested.checkpointNow());
            try std.testing.expectEqual(@as(u64, 17), retained.remaining);
            try std.testing.expectEqual(@as(u64, 11), nested.remaining);
            try std.testing.expectEqual(failure, interrupted.calls);
        }
    }
}

test "scalar input UTF8 matches standard validation including chunk crossings and NUL" {
    const a = std.testing.allocator;
    var probe: Probe = .{};
    var context = probe.context();
    for ([_][]const u8{ "", "λ\x00", "\xc0\xaf", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\x80", "\xe2\x82", "\xf0\x9f\x90\x9c" }) |text| {
        if (std.unicode.utf8ValidateSlice(text)) {
            try validateUtf8(text, &context);
        } else try std.testing.expectError(error.SqlTypeMismatch, validateUtf8(text, &context));
    }
    const text = try a.alloc(u8, 4096 + 3);
    defer a.free(text);
    @memset(text, 'x');
    @memcpy(text[4095..], "\xf0\x9f\x90\x9c");
    try validateUtf8(text, &context);
    text[text.len - 1] = 0xff;
    try std.testing.expectError(error.SqlTypeMismatch, validateUtf8(text, &context));
    try std.testing.expectEqual(@as(u64, 17), context.remaining);
}
