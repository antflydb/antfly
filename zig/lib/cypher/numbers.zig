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

//! Numeric scalar semantics, independent of native libc and host locale.
const std = @import("std");

/// Parse decimal text without a binary floating-point intermediate. Bounds are
/// checked before truncation, including fractional values just outside i64.
/// Invalid syntax returns null; a valid out-of-range decimal is an error.
pub fn decimalInteger(text: []const u8) error{CypherNumericOverflow}!?i64 {
    if (text.len == 0) return null;
    var position: usize = 0;
    const negative = text[0] == '-';
    if (negative or text[0] == '+') position += 1;
    const begin = position;
    while (position < text.len and std.ascii.isDigit(text[position])) : (position += 1) {}
    const whole_end = position;
    var fraction_begin = position;
    if (position < text.len and text[position] == '.') {
        position += 1;
        fraction_begin = position;
        while (position < text.len and std.ascii.isDigit(text[position])) : (position += 1) {}
    }
    const fraction_end = position;
    const whole = text[begin..whole_end];
    const fraction = text[fraction_begin..fraction_end];
    if (whole.len + fraction.len == 0) return null;
    var exponent: i64 = 0;
    if (position < text.len and (text[position] == 'e' or text[position] == 'E')) {
        position += 1;
        const exponent_begin = position;
        if (position < text.len and (text[position] == '+' or text[position] == '-')) position += 1;
        const digits_begin = position;
        while (position < text.len and std.ascii.isDigit(text[position])) : (position += 1) {}
        if (position == digits_begin) return null;
        exponent = std.fmt.parseInt(i32, text[exponent_begin..position], 10) catch return null;
    }
    if (position != text.len) return null;
    // Match decimal scale's signed 32-bit domain without allocating exponent
    // padding or permitting enormous exponents to overflow index arithmetic.
    if (std.math.cast(i32, @as(i64, @intCast(fraction.len)) - exponent) == null) return null;
    var leading: usize = 0;
    for (whole) |digit| {
        if (digit != '0') break;
        leading += 1;
    }
    if (leading == whole.len) for (fraction) |digit| {
        if (digit != '0') break;
        leading += 1;
    };
    if (leading == whole.len + fraction.len) return 0;
    const integer_digits = @as(i64, @intCast(whole.len)) + exponent - @as(i64, @intCast(leading));
    if (integer_digits <= 0) return 0;
    if (integer_digits > 19) return error.CypherNumericOverflow;
    const bound: u64 = if (negative) 9223372036854775808 else 9223372036854775807;
    var magnitude: u64 = 0;
    var index: usize = 0;
    var significant: usize = 0;
    var fractional_nonzero = false;
    for ([_][]const u8{ whole, fraction }) |part| for (part) |digit| {
        defer index += 1;
        if (index < leading) continue;
        if (significant < integer_digits) {
            const n: u64 = digit - '0';
            if (magnitude > (bound - n) / 10) return error.CypherNumericOverflow;
            magnitude = magnitude * 10 + n;
        } else if (digit != '0') fractional_nonzero = true;
        significant += 1;
    };
    while (significant < integer_digits) : (significant += 1) {
        if (magnitude > bound / 10) return error.CypherNumericOverflow;
        magnitude *= 10;
    }
    if (magnitude == bound and fractional_nonzero) return error.CypherNumericOverflow;
    if (negative and magnitude == 9223372036854775808) return std.math.minInt(i64);
    const result: i64 = @intCast(magnitude);
    return if (negative) -result else result;
}

/// A cumulative mean avoids both integer sum overflow and floating-point sum
/// overflow. Splitting the weighted terms handles an overflowing difference
/// when successive finite values have opposite signs.
pub fn nextAverage(mean: f64, value: f64, count: usize) f64 {
    if (count == 1) return value;
    const denominator: f64 = @floatFromInt(count);
    const difference = value - mean;
    return if (std.math.isFinite(difference)) mean + difference / denominator else mean * (@as(f64, @floatFromInt(count - 1)) / denominator) + value / denominator;
}

/// Java/Cypher float spelling: at least one fractional digit, plain notation
/// for exponents -3 through 6, otherwise uppercase E without a plus sign.
pub fn floatString(a: std.mem.Allocator, value: f64) ![]u8 {
    if (!std.math.isFinite(value)) return error.CypherNumericOverflow;
    if (value == 0) return a.dupe(u8, if (std.math.signbit(value)) "-0.0" else "0.0");
    var buffer: [64]u8 = undefined;
    var rendered = try std.fmt.float.render(&buffer, value, .{ .mode = .scientific });
    var exponent_at = std.mem.indexOfScalar(u8, rendered, 'e').?;
    const sign: usize = if (rendered[0] == '-') 1 else 0;
    if (exponent_at - sign == 1) {
        // Java chooses the nearest decimal with one OR two significant digits
        // when a one-digit spelling suffices. Re-round the exact binary value,
        // not the already rounded shortest decimal (important for subnormals).
        rendered = try std.fmt.float.render(&buffer, @as(f128, value), .{ .mode = .scientific, .precision = 1 });
        exponent_at = std.mem.indexOfScalar(u8, rendered, 'e').?;
    }
    const exponent = try std.fmt.parseInt(i32, rendered[exponent_at + 1 ..], 10);
    var digits: [32]u8 = undefined;
    var length: usize = 0;
    for (rendered[sign..exponent_at]) |digit| if (digit != '.') {
        digits[length] = digit;
        length += 1;
    };
    while (length > 1 and digits[length - 1] == '0') length -= 1;
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(a);
    if (sign != 0) try output.append(a, '-');
    if (exponent < -3 or exponent >= 7) {
        try output.append(a, digits[0]);
        try output.append(a, '.');
        try output.appendSlice(a, if (length > 1) digits[1..length] else "0");
        var exponent_buffer: [16]u8 = undefined;
        try output.appendSlice(a, try std.fmt.bufPrint(&exponent_buffer, "E{d}", .{exponent}));
    } else if (exponent < 0) {
        try output.appendSlice(a, "0.");
        for (0..@intCast(-exponent - 1)) |_| try output.append(a, '0');
        try output.appendSlice(a, digits[0..length]);
    } else {
        const whole_digits: usize = @intCast(exponent + 1);
        try output.appendSlice(a, digits[0..@min(length, whole_digits)]);
        if (whole_digits >= length) {
            for (length..whole_digits) |_| try output.append(a, '0');
            try output.appendSlice(a, ".0");
        } else {
            try output.append(a, '.');
            try output.appendSlice(a, digits[whole_digits..length]);
        }
    }
    return output.toOwnedSlice(a);
}

test "decimal integers are exact before truncation and reject decimal overflow" {
    const cases = [_]struct { text: []const u8, value: ?i64 }{
        .{ .text = "9007199254740993.0", .value = 9007199254740993 },
        .{ .text = "9223372036854775807.0", .value = std.math.maxInt(i64) },
        .{ .text = "-9223372036854775808.000", .value = std.math.minInt(i64) },
        .{ .text = "+0009.007199254740993e15", .value = 9007199254740993 },
        .{ .text = "-3.999", .value = -3 },
        .{ .text = ".9", .value = 0 },
        .{ .text = "12e-1", .value = 1 },
        .{ .text = "1.e2", .value = 100 },
        .{ .text = "000.000e100", .value = 0 },
        .{ .text = "1e-1000000", .value = 0 },
        .{ .text = "1e2147483648", .value = null },
        .{ .text = "1e-2147483648", .value = null },
        .{ .text = "1_000", .value = null },
        .{ .text = "0x10", .value = null },
        .{ .text = "1e", .value = null },
        .{ .text = ".", .value = null },
        .{ .text = " 1", .value = null },
    };
    for (cases) |case| try std.testing.expectEqual(case.value, try decimalInteger(case.text));
    for ([_][]const u8{ "9223372036854775808", "-9223372036854775809", "9223372036854775807.1", "-9223372036854775808.1", "1e1000000" }) |text|
        try std.testing.expectError(error.CypherNumericOverflow, decimalInteger(text));
}

test "averages stay finite for extremes and cancellation" {
    try std.testing.expectEqual(@as(f64, 1e308), nextAverage(1e308, 1e308, 2));
    try std.testing.expectEqual(@as(f64, 0), nextAverage(1e308, -1e308, 2));
    try std.testing.expectEqual(@as(f64, 1.5), nextAverage(1, 2, 2));
}

test "float text preserves type, notation thresholds and subnormal precision" {
    const cases = [_]struct { value: f64, text: []const u8 }{
        .{ .value = 1.0, .text = "1.0" },
        .{ .value = -0.0, .text = "-0.0" },
        .{ .value = 1e20, .text = "1.0E20" },
        .{ .value = 1e23, .text = "1.0E23" },
        .{ .value = 1e7, .text = "1.0E7" },
        .{ .value = 9999999, .text = "9999999.0" },
        .{ .value = 0.001, .text = "0.001" },
        .{ .value = 0.0001, .text = "1.0E-4" },
        .{ .value = @bitCast(@as(u64, 1)), .text = "4.9E-324" },
        .{ .value = @bitCast(@as(u64, 2)), .text = "9.9E-324" },
        .{ .value = 1.2345678901234567, .text = "1.2345678901234567" },
    };
    for (cases) |case| {
        const text = try floatString(std.testing.allocator, case.value);
        defer std.testing.allocator.free(text);
        try std.testing.expectEqualStrings(case.text, text);
    }
}
