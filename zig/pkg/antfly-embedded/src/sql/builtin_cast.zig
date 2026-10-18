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

//! PostgreSQL builtin coercion primitives; no allocation or mutable global state.
const std = @import("std");
const Kind = @import("../common/sql_builtin_type.zig").Type;
pub const whitespace = " \t\n\r\x0b\x0c";
pub fn integral(kind: Kind) bool {
    return kind == .int16 or kind == .int32 or kind == .int64;
}
pub fn floating(kind: Kind) bool {
    return kind == .float32 or kind == .float64;
}

/// PostgreSQL's implicit numeric promotion chain, including array elements.
/// Explicit casts such as text-to-integer must not participate in resolution.
pub fn commonNumeric(left: Kind, right: Kind) !Kind {
    if (!(integral(left) or floating(left) or left == .numeric) or !(integral(right) or floating(right) or right == .numeric)) return error.SqlTypeMismatch;
    return if (left == .float64 or right == .float64) .float64 else if (left == .float32 or right == .float32) .float32 else if (left == .numeric or right == .numeric) .numeric else if (left == .int64 or right == .int64) .int64 else if (left == .int32 or right == .int32) .int32 else .int16;
}

pub fn allowed(source: Kind, target: Kind) bool {
    if (source == target or source == .text or target == .text) return true;
    if ((integral(source) or floating(source) or source == .numeric) and (integral(target) or floating(target) or target == .numeric)) return true;
    if ((source == .boolean and target == .int32) or (source == .int32 and target == .boolean)) return true;
    return source == .jsonb and (integral(target) or floating(target) or target == .numeric or target == .boolean);
}

/// PostgreSQL assignment context is narrower than an explicit CAST. Numeric
/// narrowing is legal (and range checked at execution); text input is legal
/// only while still an unknown literal, not as an already typed text value.
pub fn assignmentAllowed(source: Kind, target: Kind) bool {
    return source == target or target == .text or
        ((integral(source) or floating(source) or source == .numeric) and (integral(target) or floating(target) or target == .numeric));
}

pub fn checkedInteger(value: i64, target: Kind) !i64 {
    if (target == .int16 and std.math.cast(i16, value) == null) return error.SqlNumericOutOfRange;
    if (target == .int32 and std.math.cast(i32, value) == null) return error.SqlNumericOutOfRange;
    return value;
}

pub fn integerText(input: []const u8, target: Kind) !i64 {
    const text = std.mem.trim(u8, input, whitespace);
    var at: usize = 0;
    if (text.len != 0 and (text[0] == '+' or text[0] == '-')) at = 1;
    var base: u8 = 10;
    if (text.len > at + 2 and text[at] == '0') switch (std.ascii.toLower(text[at + 1])) {
        'x' => {
            base = 16;
            at += 2;
        },
        'o' => {
            base = 8;
            at += 2;
        },
        'b' => {
            base = 2;
            at += 2;
        },
        else => {},
    };
    var digit = false;
    // PostgreSQL allows one underscore immediately after a base prefix.
    if (base != 10 and at < text.len and text[at] == '_') at += 1;
    const first = at;
    for (text[at..]) |byte| {
        if (byte == '_') {
            if (!digit) return error.SqlInvalidTextRepresentation;
            digit = false;
        } else {
            _ = std.fmt.charToDigit(byte, base) catch return error.SqlInvalidTextRepresentation;
            digit = true;
        }
    }
    if (!digit or first == text.len) return error.SqlInvalidTextRepresentation;
    // std's parser rejects an underscore directly following the prefix; the
    // normalized magnitude below handles that one PostgreSQL extension.
    const negative = text.len != 0 and text[0] == '-';
    const magnitude = std.fmt.parseInt(u64, text[first..], base) catch |err| return switch (err) {
        error.Overflow => error.SqlNumericOutOfRange,
        else => error.SqlInvalidTextRepresentation,
    };
    if (magnitude > @as(u64, std.math.maxInt(i64)) + @intFromBool(negative)) return error.SqlNumericOutOfRange;
    const result: i64 = if (negative and magnitude == @as(u64, 1) << 63) std.math.minInt(i64) else if (negative) -@as(i64, @intCast(magnitude)) else @intCast(magnitude);
    return checkedInteger(result, target);
}

pub fn floatingInteger(value: f64, target: Kind) !i64 {
    if (!std.math.isFinite(value)) return error.SqlNumericOutOfRange;
    const low = @floor(value);
    const fraction = value - low;
    // PostgreSQL float-to-integer casts use rint: ties to even, not @round's
    // ties away from zero. JSONB NUMERIC conversion has a different rule.
    const rounded = if (fraction < 0.5) low else if (fraction > 0.5 or @mod(low, 2) != 0) low + 1 else low;
    if (rounded < -9223372036854775808.0 or rounded >= 9223372036854775808.0) return error.SqlNumericOutOfRange;
    return checkedInteger(@intFromFloat(rounded), target);
}

pub fn booleanText(input: []const u8) !bool {
    const text = std.mem.trim(u8, input, whitespace);
    if (std.mem.eql(u8, text, "1")) return true;
    if (std.mem.eql(u8, text, "0")) return false;
    if (text.len == 0) return error.SqlInvalidTextRepresentation;
    for ([_][]const u8{ "true", "yes", "false", "no", "on", "off" }, [_]bool{ true, true, false, false, true, false }) |word, result| {
        if (text.len <= word.len and std.ascii.eqlIgnoreCase(text, word[0..text.len]) and !(text.len == 1 and std.ascii.toLower(text[0]) == 'o')) return result;
    }
    return error.SqlInvalidTextRepresentation;
}

pub fn floatValue(comptime T: type, value: std.json.Value) !f64 {
    var nonzero = false;
    var special = false;
    const result: T = switch (value) {
        .integer => |integer| blk: {
            nonzero = integer != 0;
            break :blk @floatFromInt(integer);
        },
        .float => |number| blk: {
            nonzero = number != 0;
            special = !std.math.isFinite(number);
            break :blk @floatCast(number);
        },
        .string, .number_string => |input| blk: {
            const text = std.mem.trim(u8, input, whitespace);
            const positive = if (text.len != 0 and (text[0] == '+' or text[0] == '-')) text[1..] else text;
            const nan_payload = positive.len >= 5 and std.ascii.eqlIgnoreCase(positive[0..4], "nan(") and positive[positive.len - 1] == ')' and payload: {
                for (positive[4 .. positive.len - 1]) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '_') break :payload false;
                break :payload true;
            };
            if (std.ascii.eqlIgnoreCase(positive, "nan") or nan_payload) {
                special = true;
                break :blk std.math.nan(T);
            }
            if (std.ascii.eqlIgnoreCase(positive, "infinity") or std.ascii.eqlIgnoreCase(positive, "inf")) {
                special = true;
                break :blk if (text.len != 0 and text[0] == '-') -std.math.inf(T) else std.math.inf(T);
            }
            // Zig accepts underscores in numbers; PostgreSQL float input does
            // not. Hex mantissas may contain e/E, unlike decimal exponents.
            if (std.mem.indexOfScalar(u8, text, '_') != null) return error.SqlInvalidTextRepresentation;
            const hex = positive.len >= 2 and std.ascii.eqlIgnoreCase(positive[0..2], "0x");
            const mantissa = if (hex) positive[2..] else positive;
            for (mantissa) |byte| {
                if ((hex and (byte == 'p' or byte == 'P')) or (!hex and (byte == 'e' or byte == 'E'))) break;
                nonzero = nonzero or (byte >= '1' and byte <= '9') or (hex and std.ascii.toLower(byte) >= 'a' and std.ascii.toLower(byte) <= 'f');
            }
            break :blk std.fmt.parseFloat(T, text) catch return error.SqlInvalidTextRepresentation;
        },
        else => return error.SqlTypeMismatch,
    };
    if (!special and (!std.math.isFinite(result) or (result == 0 and nonzero))) return error.SqlNumericOutOfRange;
    return result;
}

pub fn floatText(comptime T: type, value: T, buffer: *[64]u8) ![]const u8 {
    if (std.math.isNan(value)) return "NaN";
    if (std.math.isInf(value)) return if (value < 0) "-Infinity" else "Infinity";
    const scientific = try std.fmt.float.render(buffer, value, .{ .mode = .scientific });
    const split = std.mem.indexOfScalar(u8, scientific, 'e') orelse return error.InvalidSqlProgram;
    const exponent = std.fmt.parseInt(i32, scientific[split + 1 ..], 10) catch return error.InvalidSqlProgram;
    if (exponent >= -4 and exponent < (if (T == f32) @as(i32, 6) else @as(i32, 15))) return std.fmt.float.render(buffer, value, .{ .mode = .decimal });
    buffer[split + 1] = if (exponent < 0) '-' else '+';
    const magnitude: u32 = @intCast(@abs(exponent));
    var end = split + 2;
    if (magnitude >= 100) {
        buffer[end] = '0' + @as(u8, @intCast(magnitude / 100));
        end += 1;
    }
    buffer[end] = '0' + @as(u8, @intCast(magnitude / 10 % 10));
    buffer[end + 1] = '0' + @as(u8, @intCast(magnitude % 10));
    return buffer[0 .. end + 2];
}

test "SQL builtin casts preserve integer boundaries and PostgreSQL float input syntax" {
    try std.testing.expectEqual(std.math.minInt(i64), try integerText("-0x8000000000000000", .int64));
    try std.testing.expectEqual(std.math.maxInt(i64), try integerText("9223372036854775807", .int64));
    try std.testing.expectError(error.SqlNumericOutOfRange, integerText("9223372036854775808", .int64));
    try std.testing.expectError(error.SqlNumericOutOfRange, integerText("-32769", .int16));
    try std.testing.expectError(error.SqlInvalidTextRepresentation, floatValue(f64, .{ .string = "1_0" }));
    try std.testing.expectError(error.SqlNumericOutOfRange, floatValue(f64, .{ .string = "0xAp-20000" }));
    try std.testing.expectEqual(@as(f64, 1), try floatValue(f64, .{ .string = "0x1p0" }));
    try std.testing.expect(std.math.isNan(try floatValue(f64, .{ .string = "nan(1)" })));
    for ([_]f64{ 2.5, 3.5, -2.5, -3.5 }, [_]i64{ 2, 4, -2, -4 }) |input, expected| try std.testing.expectEqual(expected, try floatingInteger(input, .int32));
}
