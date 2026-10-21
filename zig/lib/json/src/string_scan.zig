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

//! Complete-input value-string adapter. The std scanner remains authoritative
//! for structure, keys, diagnostics and streaming. This path validates strings
//! without emitting one temporary token per escape or UTF-8 sequence.
const std = @import("std");
const Scanner = std.json.Scanner;
const Vec = @Vector(32, u8);

fn ready(scanner: *Scanner) bool {
    return scanner.is_end_of_input and scanner.diagnostics == null and scanner.state == .value and
        scanner.cursor < scanner.input.len and scanner.input[scanner.cursor] == '"';
}
fn commit(scanner: *Scanner, end: usize) void {
    scanner.cursor = end;
    scanner.state = .post_value;
    scanner.string_is_object_key = false;
}
/// After peekNextTokenType(), borrow a plain ASCII value string if available.
/// A refusal leaves scanner position/state untouched for ordinary decoding.
pub fn borrow(scanner: *Scanner) ?[]const u8 {
    if (!ready(scanner)) return null;
    const start = scanner.cursor + 1;
    var index = start;
    while (index + 32 <= scanner.input.len) {
        const bytes: Vec = scanner.input[index..][0..32].*;
        const special = (bytes < @as(Vec, @splat(0x20))) | (bytes >= @as(Vec, @splat(0x80))) |
            (bytes == @as(Vec, @splat('"'))) | (bytes == @as(Vec, @splat('\\')));
        const mask: u32 = @bitCast(special);
        if (mask != 0) {
            index += @ctz(mask);
            break;
        }
        index += 32;
    }
    while (index < scanner.input.len) : (index += 1) {
        switch (scanner.input[index]) {
            '"' => {
                commit(scanner, index + 1);
                return scanner.input[start..index];
            },
            '\\', 0...0x1f, 0x80...0xff => return null,
            else => {},
        }
    }
    return null;
}
/// Return false for scanner modes which must use the standard token API.
/// No mutation is committed until the complete string has been validated.
pub fn skip(scanner: *Scanner) !bool {
    if (!ready(scanner)) return false;
    var index = scanner.cursor + 1;
    while (index < scanner.input.len) {
        if (scanner.input[index] != '\\' and scanner.input[index] != '"' and index + 32 <= scanner.input.len) {
            const bytes: Vec = scanner.input[index..][0..32].*;
            const special = (bytes < @as(Vec, @splat(0x20))) | (bytes >= @as(Vec, @splat(0x80))) |
                (bytes == @as(Vec, @splat('"'))) | (bytes == @as(Vec, @splat('\\')));
            const mask: u32 = @bitCast(special);
            if (mask == 0) {
                index += 32;
                continue;
            }
            index += @ctz(mask);
        }
        const byte = scanner.input[index];
        switch (byte) {
            '"' => {
                commit(scanner, index + 1);
                return true;
            },
            0...0x1f => return error.SyntaxError,
            '\\' => {
                index += 1;
                if (index == scanner.input.len) return error.UnexpectedEndOfInput;
                switch (scanner.input[index]) {
                    '"', '\\', '/', 'b', 'f', 'n', 'r', 't' => index += 1,
                    'u' => {
                        const high = try hex4(scanner.input, index + 1);
                        index += 5;
                        if (high >= 0xdc00 and high <= 0xdfff) return error.SyntaxError;
                        if (high >= 0xd800 and high <= 0xdbff) {
                            if (scanner.input.len - index < 2) return error.UnexpectedEndOfInput;
                            if (scanner.input[index] != '\\' or scanner.input[index + 1] != 'u') return error.SyntaxError;
                            const low = try hex4(scanner.input, index + 2);
                            if (low < 0xdc00 or low > 0xdfff) return error.SyntaxError;
                            index += 6;
                        }
                    },
                    else => return error.SyntaxError,
                }
            },
            0x80...0xff => {
                const length = std.unicode.utf8ByteSequenceLength(byte) catch return error.SyntaxError;
                if (scanner.input.len - index < length) return error.UnexpectedEndOfInput;
                _ = std.unicode.utf8Decode(scanner.input[index..][0..length]) catch return error.SyntaxError;
                index += length;
            },
            else => index += 1,
        }
    }
    return error.UnexpectedEndOfInput;
}
fn hex4(input: []const u8, start: usize) !u16 {
    if (input.len - start < 4) return error.UnexpectedEndOfInput;
    var result: u16 = 0;
    for (input[start..][0..4]) |byte| {
        const digit = std.fmt.charToDigit(byte, 16) catch return error.SyntaxError;
        result = result * 16 + digit;
    }
    return result;
}

test "logical value fast strings agree with standard validation and continuation" {
    const a = std.testing.allocator;
    const Cases = struct {
        fn check(input: []const u8) !void {
            var ordinary = Scanner.initCompleteInput(std.testing.allocator, input);
            defer ordinary.deinit();
            var accelerated = Scanner.initCompleteInput(std.testing.allocator, input);
            defer accelerated.deinit();
            // Exercise parent structural state and post-string continuation.
            _ = try ordinary.next();
            _ = try accelerated.next();
            const expected = ordinary.skipValue();
            const actual = blk: {
                if (accelerated.peekNextTokenType()) |kind| {
                    if (kind == .string) {
                        if (skip(&accelerated)) |done| {
                            if (done) break :blk @as(anyerror!void, {});
                        } else |err| break :blk @as(anyerror!void, err);
                    }
                } else |err| break :blk @as(anyerror!void, err);
                break :blk accelerated.skipValue();
            };
            if (expected) |_| {
                try actual;
                try std.testing.expectEqual(ordinary.cursor, accelerated.cursor);
                while (true) {
                    if (ordinary.next()) |token| {
                        try std.testing.expectEqualDeep(token, try accelerated.next());
                        if (token == .end_of_document) break;
                    } else |_| {
                        if (accelerated.next()) |_| return error.TestUnexpectedResult else |_| {}
                        break;
                    }
                }
            } else |_| {
                if (actual) |_| return error.TestUnexpectedResult else |_| {}
            }
        }
    };
    for ([_][]const u8{
        "[\"plain\",17]",          "[\"\\n\\t\\\\\\\"\\/\\b\\f\\r\",17]",
        "[\"K日本語😀\",17]",
        "[\"\\uD834\\uDD1E\",17]", "[\"\\uD800\",17]",
        "[\"\\uDC00\",17]",        "[\"\\uD800\\u0041\",17]",
        "[\"\\q\",17]",            "[\"\x01\",17]",
    }) |input| try Cases.check(input);
    // Generated valid strings, malformed mutations and every truncation share
    // the standard scanner oracle. Include UTF-8 and surrogate-pair boundaries.
    var random = std.Random.DefaultPrng.init(0x414e54464c594a53);
    const atoms = [_][]const u8{ "a", "K", "日本語", "😀", "\\n", "\\uD834\\uDD1E", "\\u0000", "\\\"", "\\\\" };
    for (0..256) |_| {
        var buffer: [1024]u8 = undefined;
        var end: usize = 2;
        @memcpy(buffer[0..2], "[\"");
        for (0..16) |_| {
            const atom = atoms[random.random().uintLessThan(usize, atoms.len)];
            @memcpy(buffer[end..][0..atom.len], atom);
            end += atom.len;
        }
        @memcpy(buffer[end..][0..5], "\",17]");
        end += 5;
        try Cases.check(buffer[0..end]);
        for (2..end) |length| try Cases.check(buffer[0..length]);
        const index = 2 + random.random().uintLessThan(usize, end - 2);
        buffer[index] = random.random().int(u8);
        try Cases.check(buffer[0..end]);
    }
    var sample: [75]u8 = undefined;
    @memset(&sample, 'a');
    @memcpy(sample[0..2], "[\"");
    @memcpy(sample[69..], "\",17] ");
    for (2..69) |index| for (0..256) |byte| {
        const previous = sample[index];
        sample[index] = @intCast(byte);
        try Cases.check(&sample);
        sample[index] = previous;
    };
    var long = try a.alloc(u8, 65540);
    defer a.free(long);
    @memset(long, 'x');
    long[0] = '"';
    long[long.len - 1] = '"';
    var scanner = Scanner.initCompleteInput(a, long);
    defer scanner.deinit();
    const value = borrow(&scanner).?;
    try std.testing.expectEqual(long.len - 2, value.len);
    try std.testing.expectEqual(.end_of_document, try scanner.next());
}
