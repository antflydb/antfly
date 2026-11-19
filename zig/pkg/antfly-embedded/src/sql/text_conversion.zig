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

//! Strict SQL charset conversion. Never use the document-ingress decoder here:
//! its replacement and WHATWG LATIN1 aliases change PostgreSQL value semantics.
const std = @import("std");
const A = std.mem.Allocator;
const Budget = @import("json_order.zig").Budget;
const Encoding = enum { utf8, latin1, win1252, sql_ascii };
const windows = [_]u21{ 0x20ac, 0, 0x201a, 0x0192, 0x201e, 0x2026, 0x2020, 0x2021, 0x02c6, 0x2030, 0x0160, 0x2039, 0x0152, 0, 0x017d, 0, 0, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014, 0x02dc, 0x2122, 0x0161, 0x203a, 0x0153, 0, 0x017e, 0x0178 };
fn encoding(label: []const u8) !Encoding {
    var buffer: [64]u8 = undefined;
    var len: usize = 0;
    for (label) |byte| {
        if (byte == '-' or byte == '_') continue;
        if (len == buffer.len) return error.SqlInvalidParameterValue;
        buffer[len] = std.ascii.toUpper(byte);
        len += 1;
    }
    const name = buffer[0..len];
    if (std.mem.eql(u8, name, "UTF8") or std.mem.eql(u8, name, "UNICODE")) return .utf8;
    if (std.mem.eql(u8, name, "LATIN1") or std.mem.eql(u8, name, "ISO88591")) return .latin1;
    if (std.mem.eql(u8, name, "WIN1252") or std.mem.eql(u8, name, "WINDOWS1252")) return .win1252;
    if (std.mem.eql(u8, name, "SQLASCII")) return .sql_ascii;
    // Recognized PostgreSQL charsets need strict native conversion tables.
    // Keep their activation guarded instead of replacing unmappable bytes.
    for ([_][]const u8{ "EUCJP", "EUCCN", "EUCKR", "EUCTW", "EUCJIS2004", "SJIS", "SHIFTJIS2004", "BIG5", "GBK", "UHC", "GB18030", "JOHAB", "MULEINTERNAL", "KOI8R", "KOI8U", "WIN866", "WIN874", "WIN1250", "WIN1251", "WIN1253", "WIN1254", "WIN1255", "WIN1256", "WIN1257", "WIN1258", "LATIN2", "LATIN3", "LATIN4", "LATIN5", "LATIN6", "LATIN7", "LATIN8", "LATIN9", "LATIN10", "ISO88595", "ISO88596", "ISO88597", "ISO88598" }) |known| if (std.mem.eql(u8, name, known)) return error.UnsupportedSqlShape;
    return error.SqlInvalidParameterValue;
}
fn decodeByte(byte: u8, kind: Encoding) !u21 {
    if (byte == 0) return error.SqlInvalidCharacterEncoding;
    if (kind == .win1252 and byte >= 0x80 and byte < 0xa0) {
        const cp = windows[byte - 0x80];
        if (cp == 0) return error.SqlCharsetUntranslatableCharacter;
        return cp;
    }
    return byte;
}
fn encodePoint(cp: u21, kind: Encoding) !u8 {
    if (cp == 0) return error.SqlInvalidCharacterEncoding;
    if (kind == .latin1 and cp <= 255) return @intCast(cp);
    if (kind == .win1252) {
        if (cp < 128 or (cp >= 160 and cp <= 255)) return @intCast(cp);
        for (windows, 0..) |value, index| if (value != 0 and value == cp) return @intCast(128 + index);
    }
    return error.SqlCharsetUntranslatableCharacter;
}
fn utf8Point(bytes: []const u8, at: *usize) !u21 {
    const count = std.unicode.utf8ByteSequenceLength(bytes[at.*]) catch return error.SqlInvalidCharacterEncoding;
    if (count > bytes.len - at.*) return error.SqlInvalidCharacterEncoding;
    const cp = std.unicode.utf8Decode(bytes[at.*..][0..count]) catch return error.SqlInvalidCharacterEncoding;
    if (cp == 0) return error.SqlInvalidCharacterEncoding;
    at.* += count;
    return cp;
}
/// UTF-8 conversion is a checked identity. The immutable input owner must
/// outlive the borrowed result, exactly as for a TEXT identity cast.
pub fn borrowUtf8(bytes: []const u8, label: []const u8, limit: usize, work: *Budget) !?[]const u8 {
    try work.consume(label.len);
    const kind = try encoding(label);
    if (kind != .utf8 and kind != .sql_ascii) return null;
    if (bytes.len > limit) return error.SqlProgramLimitExceeded;
    var at: usize = 0;
    while (at < bytes.len) {
        try work.consume(1);
        _ = try utf8Point(bytes, &at);
    }
    return bytes;
}

/// Two bounded passes: validate/count before allocation, then emit directly
/// into the exact output size. BOMs are data and never change the encoding.
pub fn convertAlloc(a: A, bytes: []const u8, label: []const u8, to_utf8: bool, limit: usize, work: *Budget) ![]u8 {
    try work.consume(label.len);
    const kind = try encoding(label);
    var size: usize = 0;
    var at: usize = 0;
    while (at < bytes.len) {
        try work.consume(1);
        if (kind == .utf8 or kind == .sql_ascii or !to_utf8) {
            const start = at;
            const cp = try utf8Point(bytes, &at);
            size += if (kind == .utf8 or kind == .sql_ascii) at - start else blk: {
                _ = try encodePoint(cp, kind);
                break :blk 1;
            };
        } else {
            size += try std.unicode.utf8CodepointSequenceLength(try decodeByte(bytes[at], kind));
            at += 1;
        }
        if (size > limit) return error.SqlProgramLimitExceeded;
    }
    const result = try a.alloc(u8, size);
    errdefer a.free(result);
    at = 0;
    var out: usize = 0;
    while (at < bytes.len) {
        try work.consume(1);
        if (kind == .utf8 or kind == .sql_ascii) {
            const start = at;
            _ = try utf8Point(bytes, &at);
            @memcpy(result[out..][0 .. at - start], bytes[start..at]);
            out += at - start;
        } else if (to_utf8) {
            var buffer: [4]u8 = undefined;
            const count = try std.unicode.utf8Encode(try decodeByte(bytes[at], kind), &buffer);
            @memcpy(result[out..][0..count], buffer[0..count]);
            out += count;
            at += 1;
        } else {
            result[out] = try encodePoint(try utf8Point(bytes, &at), kind);
            out += 1;
        }
    }
    return result;
}

test "SQL bytea charset conversion is strict bounded and preserves BOMs and LATIN1 C1 bytes" {
    const a = std.testing.allocator;
    const cases = .{
        .{ "café", "UTF8", true, "café" },
        .{ "café", "SQL_ASCII", true, "café" },
        .{ "café", "SQL_ASCII", false, "café" },
        .{ "\xef\xbb\xbfhi", "UTF-8", true, "\xef\xbb\xbfhi" },
        .{ "\x80\xe9", "LATIN1", true, "\xc2\x80é" },
        .{ "\x80\xe9", "WIN1252", true, "€é" },
        .{ "€é", "WIN1252", false, "\x80\xe9" },
        .{ "\xc2\x80é", "LATIN1", false, "\x80\xe9" },
    };
    inline for (cases) |entry| {
        var work: Budget = .{};
        const converted = try convertAlloc(a, entry[0], entry[1], entry[2], 64, &work);
        defer a.free(converted);
        try std.testing.expectEqualSlices(u8, entry[3], converted);
    }
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var work: Budget = .{};
    for ([_][]const u8{ "UTF8", "SQL_ASCII" }) |charset| for ([_][]const u8{ "\xff", "\xc0\x80", "\xed\xa0\x80", "\x00" }) |invalid|
        try std.testing.expectError(error.SqlInvalidCharacterEncoding, convertAlloc(none.allocator(), invalid, charset, true, 32, &work));
    try std.testing.expectError(error.SqlCharsetUntranslatableCharacter, convertAlloc(none.allocator(), "😀", "LATIN1", false, 32, &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, convertAlloc(none.allocator(), "é", "UTF8", true, 1, &work));
    try std.testing.expectError(error.SqlInvalidParameterValue, convertAlloc(none.allocator(), "", "bad", true, 32, &work));
}

test "SQL bytea charset conversion polls shared cancellation and unwinds allocation failures" {
    const a = std.testing.allocator;
    const Faults = struct {
        fn run(alloc: A) !void {
            var work: Budget = .{};
            const text = try convertAlloc(alloc, "\x80\xe9", "WIN1252", true, 64, &work);
            defer alloc.free(text);
            try std.testing.expectEqualStrings("€é", text);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{});
    const Control = struct {
        calls: usize = 0,
        fail_at: usize,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.Canceled;
        }
    };
    const bytes: [1024]u8 = @splat(0xe9);
    for (1..8) |cut| {
        var control: Control = .{ .fail_at = cut };
        var shared: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &control };
        var work: Budget = .{ .shared = &shared };
        try std.testing.expectError(error.Canceled, convertAlloc(a, &bytes, "LATIN1", true, 4096, &work));
        try std.testing.expectError(error.Canceled, work.consume(0));
    }
}
