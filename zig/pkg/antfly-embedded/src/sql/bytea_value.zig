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

//! PostgreSQL bytea input/output. Payloads are opaque bytes, never UTF-8 text.
//! Callers retain ownership of the returned allocation and enforce work quotas.
const std = @import("std");
const A = std.mem.Allocator;
pub const Value = struct { bytes: []const u8 };

fn hex(c: u8) !u8 {
    return std.fmt.charToDigit(c, 16) catch error.SqlInvalidParameterValue;
}
fn whitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r' or c == 0x0b or c == 0x0c;
}
/// Validate and count before allocation. Hex whitespace may separate complete
/// octets; it cannot split an octet. Escape input admits only \\ and \ooo.
pub fn decodedSize(text: []const u8) !usize {
    var count: usize = 0;
    var at: usize = 0;
    if (std.mem.startsWith(u8, text, "\\x")) {
        at = 2;
        while (at < text.len) {
            if (whitespace(text[at])) {
                at += 1;
                continue;
            }
            _ = try hex(text[at]);
            if (at + 1 == text.len) return error.SqlInvalidParameterValue;
            _ = try hex(text[at + 1]);
            at += 2;
            count += 1;
        }
    } else while (at < text.len) {
        if (text[at] == '\\') {
            at += 1;
            if (at == text.len) return error.SqlInvalidTextRepresentation;
            if (text[at] != '\\') {
                if (text.len - at < 3 or text[at] < '0' or text[at] > '3' or text[at + 1] < '0' or text[at + 1] > '7' or text[at + 2] < '0' or text[at + 2] > '7') return error.SqlInvalidTextRepresentation;
                at += 2;
            }
        }
        at += 1;
        count += 1;
    }
    return count;
}
pub fn parseAlloc(a: A, text: []const u8, limit: usize) ![]u8 {
    const count = try decodedSize(text);
    if (count > limit) return error.SqlProgramLimitExceeded;
    const result = try a.alloc(u8, count);
    var at: usize = if (std.mem.startsWith(u8, text, "\\x")) 2 else 0;
    for (result) |*out| {
        if (std.mem.startsWith(u8, text, "\\x")) {
            while (whitespace(text[at])) at += 1;
            out.* = (try hex(text[at])) << 4 | try hex(text[at + 1]);
            at += 2;
        } else {
            if (text[at] == '\\') {
                at += 1;
                if (text[at] != '\\') {
                    out.* = (text[at] - '0') * 64 + (text[at + 1] - '0') * 8 + (text[at + 2] - '0');
                    at += 3;
                    continue;
                }
            }
            out.* = text[at];
            at += 1;
        }
    }
    return result;
}
pub fn formattedSize(bytes: []const u8) !usize {
    return std.math.add(usize, 2, std.math.mul(usize, bytes.len, 2) catch return error.SqlProgramLimitExceeded) catch error.SqlProgramLimitExceeded;
}
pub fn formatAlloc(a: A, bytes: []const u8, limit: usize) ![]u8 {
    const count = try formattedSize(bytes);
    if (count > limit) return error.SqlProgramLimitExceeded;
    const result = try a.alloc(u8, count);
    result[0..2].* = "\\x".*;
    const alphabet = "0123456789abcdef";
    for (bytes, 0..) |byte, i| {
        result[2 + i * 2] = alphabet[byte >> 4];
        result[3 + i * 2] = alphabet[byte & 15];
    }
    return result;
}

fn writeDigits(writer: *std.Io.Writer, bytes: []const u8) !void {
    const hex_digits = "0123456789abcdef";
    var buffer: [512]u8 = undefined;
    var at: usize = 0;
    while (at < bytes.len) {
        const chunk = bytes[at..][0..@min(bytes.len - at, buffer.len / 2)];
        for (chunk, 0..) |byte, i| {
            buffer[i * 2] = hex_digits[byte >> 4];
            buffer[i * 2 + 1] = hex_digits[byte & 15];
        }
        try writer.writeAll(buffer[0 .. chunk.len * 2]);
        at += chunk.len;
    }
}
/// Exact bounded output with constant scratch, shared by wire and JSON APIs.
pub fn writeHex(writer: *std.Io.Writer, bytes: []const u8, limit: usize) !void {
    if (try formattedSize(bytes) > limit) return error.SqlProgramLimitExceeded;
    try writer.writeAll("\\x");
    try writeDigits(writer, bytes);
}
pub fn writeJson(writer: *std.Io.Writer, bytes: []const u8, limit: usize) !void {
    const text_size = try formattedSize(bytes);
    if (text_size > limit or limit - text_size < 3) return error.SqlProgramLimitExceeded;
    try writer.writeAll("\"\\\\x");
    try writeDigits(writer, bytes);
    try writer.writeByte('"');
}

test "SQL bytea text admission preserves opaque bytes and validates before allocation" {
    const a = std.testing.allocator;
    const cases = .{
        .{ "", "" },
        .{ "\\x", "" },
        .{ "\\x00 7f\t80ff", "\x00\x7f\x80\xff" },
        .{ "a\\000\\377\\\\z", "a\x00\xff\\z" },
    };
    inline for (cases) |entry| {
        const bytes = try parseAlloc(a, entry[0], 32);
        defer a.free(bytes);
        try std.testing.expectEqualSlices(u8, entry[1], bytes);
        const display = try formatAlloc(a, bytes, 66);
        defer a.free(display);
        const roundtrip = try parseAlloc(a, display, 32);
        defer a.free(roundtrip);
        try std.testing.expectEqualSlices(u8, bytes, roundtrip);
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        try writeJson(&output.writer, bytes, display.len + 3);
        var json = try std.json.parseFromSlice(std.json.Value, a, output.written(), .{});
        defer json.deinit();
        try std.testing.expectEqualStrings(display, json.value.string);
    }
    var none = std.heap.FixedBufferAllocator.init(&.{});
    for ([_][]const u8{ "\\x0", "\\x0 0", "\\xgg" }) |invalid|
        try std.testing.expectError(error.SqlInvalidParameterValue, parseAlloc(none.allocator(), invalid, 32));
    for ([_][]const u8{ "\\400", "\\12", "\\z", "\\" }) |invalid|
        try std.testing.expectError(error.SqlInvalidTextRepresentation, parseAlloc(none.allocator(), invalid, 32));
    try std.testing.expectError(error.SqlProgramLimitExceeded, parseAlloc(none.allocator(), "\\x0001", 1));
    try std.testing.expectError(error.SqlProgramLimitExceeded, formatAlloc(none.allocator(), "a", 3));
}

test "SQL bytea streaming hex and JSON preserve every octet across chunk boundaries" {
    const a = std.testing.allocator;
    var bytes: [1025]u8 = undefined;
    for (&bytes, 0..) |*byte, i| byte.* = @truncate(i);
    for ([_]usize{ 0, 1, 255, 256, 257, 1025 }) |len| {
        const raw = bytes[0..len];
        const expected = try formatAlloc(a, raw, 4096);
        defer a.free(expected);
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        try writeHex(&output.writer, raw, expected.len);
        try std.testing.expectEqualStrings(expected, output.written());
        output.writer.end = 0;
        try writeJson(&output.writer, raw, expected.len + 3);
        var decoded = try std.json.parseFromSlice(std.json.Value, a, output.written(), .{});
        defer decoded.deinit();
        try std.testing.expectEqualStrings(expected, decoded.value.string);
        var none: std.Io.Writer = .fixed(&.{});
        try std.testing.expectError(error.SqlProgramLimitExceeded, writeHex(&none, raw, expected.len - 1));
        try std.testing.expectError(error.SqlProgramLimitExceeded, writeJson(&none, raw, expected.len + 2));
    }
}
