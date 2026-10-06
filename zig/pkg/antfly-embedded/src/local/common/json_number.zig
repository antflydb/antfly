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

//! Exact, allocation-free comparison of JSON decimal numbers. Views borrow
//! their input, retain arbitrary exponent digits, and never expand zero runs.
const std = @import("std");

pub const Number = struct {
    raw: []const u8,
    negative: bool,
    first: usize,
    end: usize,
    exponent: []const u8,
    exponent_negative: bool,
    adjustment: i128,

    pub fn parse(raw: []const u8) ?Number {
        if (raw.len == 0) return null;
        var pos: usize = 0;
        const negative = raw[0] == '-';
        if (negative) pos += 1;
        if (pos == raw.len) return null;
        const integer_start = pos;
        if (raw[pos] == '0') {
            pos += 1;
        } else if (raw[pos] >= '1' and raw[pos] <= '9') {
            while (pos < raw.len and std.ascii.isDigit(raw[pos])) pos += 1;
        } else return null;
        const integer_digits = pos - integer_start;
        if (pos < raw.len and raw[pos] == '.') {
            pos += 1;
            const fraction_start = pos;
            while (pos < raw.len and std.ascii.isDigit(raw[pos])) pos += 1;
            if (pos == fraction_start) return null;
        }
        const mantissa_end = pos;
        var exponent: []const u8 = "0";
        var exponent_negative = false;
        if (pos < raw.len and (raw[pos] == 'e' or raw[pos] == 'E')) {
            pos += 1;
            if (pos < raw.len and (raw[pos] == '+' or raw[pos] == '-')) {
                exponent_negative = raw[pos] == '-';
                pos += 1;
            }
            const exponent_start = pos;
            while (pos < raw.len and std.ascii.isDigit(raw[pos])) pos += 1;
            if (pos == exponent_start) return null;
            exponent = raw[exponent_start..pos];
            while (exponent.len > 1 and exponent[0] == '0') exponent = exponent[1..];
            if (std.mem.eql(u8, exponent, "0")) exponent_negative = false;
        }
        if (pos != raw.len) return null;

        var first = mantissa_end;
        var end = mantissa_end;
        var leading_zeros: usize = 0;
        for (raw[integer_start..mantissa_end], integer_start..) |digit, index| {
            if (digit == '.') continue;
            if (first == mantissa_end and digit == '0') {
                leading_zeros += 1;
            } else if (digit != '0') {
                if (first == mantissa_end) first = index;
                end = index + 1;
            }
        }
        return .{
            .raw = raw,
            .negative = negative,
            .first = first,
            .end = end,
            .exponent = exponent,
            .exponent_negative = exponent_negative,
            .adjustment = @as(i128, integer_digits) - @as(i128, leading_zeros),
        };
    }

    pub fn order(a: Number, b: Number) std.math.Order {
        const a_zero = a.first == a.end;
        const b_zero = b.first == b.end;
        if (a_zero and b_zero) return .eq;
        if (a_zero) return if (b.negative) .gt else .lt;
        if (b_zero) return if (a.negative) .lt else .gt;
        if (a.negative != b.negative) return if (a.negative) .lt else .gt;
        // Each adjustment is bounded by an input slice's usize length. A
        // clipped exponent difference beyond twice maxInt(u64) cannot have
        // its sign reversed by the combined mantissa adjustments.
        const magnitude = exponentDelta(a, b) + a.adjustment - b.adjustment;
        const magnitude_order = std.math.order(magnitude, @as(i128, 0));
        if (magnitude_order != .eq) return orient(magnitude_order, a.negative);
        var ai = a.first;
        var bi = b.first;
        while (ai < a.end or bi < b.end) {
            if (ai < a.end and a.raw[ai] == '.') ai += 1;
            if (bi < b.end and b.raw[bi] == '.') bi += 1;
            const ad = if (ai < a.end) a.raw[ai] else '0';
            const bd = if (bi < b.end) b.raw[bi] else '0';
            const digit_order = std.math.order(ad, bd);
            if (digit_order != .eq) return orient(digit_order, a.negative);
            if (ai < a.end) ai += 1;
            if (bi < b.end) bi += 1;
        }
        return .eq;
    }
};

/// Finite stored floats use their shortest round-trip decimal, matching the
/// public JSON weight representation rather than exposing binary rounding
/// through a wider float. The caller owns the buffer for the returned view.
pub fn fromValue(value: std.json.Value, buffer: *[64]u8) ?Number {
    const raw = switch (value) {
        .number_string => |text| text,
        .integer => |number| std.fmt.bufPrint(buffer, "{d}", .{number}) catch return null,
        .float => |number| if (std.math.isFinite(number))
            std.fmt.bufPrint(buffer, "{e}", .{number}) catch return null
        else
            return null,
        else => return null,
    };
    return Number.parse(raw);
}

fn orient(order: std.math.Order, negative: bool) std.math.Order {
    return if (!negative) order else switch (order) {
        .lt => .gt,
        .eq => .eq,
        .gt => .lt,
    };
}

const exponent_limit: u128 = @as(u128, 1) << 66;

fn clippedMagnitude(raw: []const u8) u128 {
    var result: u128 = 0;
    for (raw) |digit| {
        result = result * 10 + digit - '0';
        if (result >= exponent_limit) return exponent_limit;
    }
    return result;
}

fn magnitudeOrder(a: []const u8, b: []const u8) std.math.Order {
    const length = std.math.order(a.len, b.len);
    return if (length == .eq) std.mem.order(u8, a, b) else length;
}

// a >= b. Subtract from the right so huge exponents differing by one retain
// that exact difference, even across an arbitrarily long carry/borrow chain.
fn clippedDifference(a: []const u8, b: []const u8) u128 {
    var ai = a.len;
    var bi = b.len;
    var borrow: i16 = 0;
    var place: u128 = 1;
    var result: u128 = 0;
    while (ai > 0) {
        ai -= 1;
        const right: i16 = if (bi > 0) blk: {
            bi -= 1;
            break :blk b[bi] - '0';
        } else 0;
        var digit: i16 = @as(i16, a[ai] - '0') - right - borrow;
        borrow = if (digit < 0) 1 else 0;
        if (digit < 0) digit += 10;
        if (digit != 0) {
            result += @as(u128, @intCast(digit)) * place;
            if (result >= exponent_limit) return exponent_limit;
        }
        place = @min(place * 10, exponent_limit);
    }
    std.debug.assert(borrow == 0 and bi == 0);
    return result;
}

fn exponentDelta(a: Number, b: Number) i128 {
    if (a.exponent_negative != b.exponent_negative) {
        const sum = @min(clippedMagnitude(a.exponent) + clippedMagnitude(b.exponent), exponent_limit);
        return if (a.exponent_negative) -@as(i128, @intCast(sum)) else @intCast(sum);
    }
    const order = magnitudeOrder(a.exponent, b.exponent);
    if (order == .eq) return 0;
    const delta: i128 = @intCast(if (order == .gt) clippedDifference(a.exponent, b.exponent) else clippedDifference(b.exponent, a.exponent));
    return if ((order == .lt) != a.exponent_negative) -delta else delta;
}

test "JSON decimal comparison preserves precision signs and equivalent spellings" {
    const cases = [_]struct { a: []const u8, b: []const u8, order: std.math.Order }{
        .{ .a = "1.00000000000000000000000000000000001", .b = "1", .order = .gt },
        .{ .a = "20769187434139310514121985316880385", .b = "20769187434139310514121985316880384", .order = .gt },
        .{ .a = "-0e99999999999999999999999999", .b = "0.0", .order = .eq },
        .{ .a = "-0", .b = "-1e-99999", .order = .gt },
        .{ .a = "-0.1", .b = "0.1", .order = .lt },
        .{ .a = "-1.001", .b = "-1", .order = .lt },
        .{ .a = "0.00120", .b = "12e-4", .order = .eq },
        .{ .a = "123.000", .b = "1.23e+2", .order = .eq },
        .{ .a = "1e-9999", .b = "0", .order = .gt },
        .{ .a = "1e9999", .b = "9e9998", .order = .gt },
        .{ .a = "10e999999999999999999999999999999999999", .b = "1e1000000000000000000000000000000000000", .order = .eq },
        .{ .a = "0.1e-999999999999999999999999999999999999", .b = "1e-1000000000000000000000000000000000000", .order = .eq },
        .{ .a = "1e1000000000000000000000000000000000000", .b = "2e999999999999999999999999999999999999", .order = .gt },
        .{ .a = "1e999999999999999999999999999999999999", .b = "1e-999999999999999999999999999999999999", .order = .gt },
        .{ .a = "1e73786976294838206463", .b = "1e73786976294838206464", .order = .lt },
        .{ .a = "1e73786976294838206464", .b = "1", .order = .gt },
        .{ .a = "-1e-73786976294838206464", .b = "-1e-73786976294838206463", .order = .gt },
    };
    for (cases) |case| {
        const a = Number.parse(case.a).?;
        const b = Number.parse(case.b).?;
        try std.testing.expectEqual(case.order, a.order(b));
        try std.testing.expectEqual(orient(case.order, true), b.order(a));
    }
    for ([_][]const u8{ "", "-", "+1", "01", "1.", ".1", "1e", "1e+", "1e_2", "1 2", "NaN", "Infinity" }) |raw|
        try std.testing.expect(Number.parse(raw) == null);
}

test "JSON decimal comparison normalizes finite stored floats without widening artifacts" {
    for ([_]f64{ 0.1, 0.2, 0.3, -0.1, -0.0, 1.5, 1.7976931348623157e308, 5e-324 }) |number| {
        var buffer: [64]u8 = undefined;
        const actual = fromValue(.{ .float = number }, &buffer).?;
        const json = try std.json.Stringify.valueAlloc(std.testing.allocator, number, .{});
        defer std.testing.allocator.free(json);
        try std.testing.expectEqual(std.math.Order.eq, actual.order(Number.parse(json).?));
    }
    var buffer: [64]u8 = undefined;
    try std.testing.expectEqual(std.math.Order.eq, fromValue(.{ .float = 0.1 }, &buffer).?.order(Number.parse("0.1").?));
    try std.testing.expectEqual(std.math.Order.lt, fromValue(.{ .float = 1.5 }, &buffer).?.order(Number.parse("1.5000000000000001").?));
    try std.testing.expect(fromValue(.{ .float = std.math.inf(f64) }, &buffer) == null);
}

test "JSON decimal comparison agrees with scaled integer ordering" {
    var prng = std.Random.DefaultPrng.init(941);
    const random = prng.random();
    for (0..3000) |_| {
        var numbers: [2]Number = undefined;
        var buffers: [2][64]u8 = undefined;
        var expected: [2]i128 = undefined;
        for (0..2) |i| {
            const coefficient: i64 = random.intRangeAtMost(i64, -999999999999, 999999999999);
            const exponent = random.intRangeAtMost(i32, -8, 8);
            const magnitude: u64 = @intCast(if (coefficient < 0) -coefficient else coefficient);
            const text = try std.fmt.bufPrint(&buffers[i], "{s}{d}.{d:0>3}e{d}", .{ if (coefficient < 0) "-" else "", magnitude / 1000, magnitude % 1000, exponent });
            numbers[i] = Number.parse(text).?;
            var scale: i128 = 1;
            for (0..@intCast(exponent + 8)) |_| scale *= 10;
            expected[i] = coefficient * scale;
        }
        try std.testing.expectEqual(std.math.order(expected[0], expected[1]), numbers[0].order(numbers[1]));
    }
}

test "JSON decimal comparison bounds work by literal length without expanding exponents" {
    const alloc = std.testing.allocator;
    const number = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(number);
    @memset(number, '0');
    number[0] = '1';
    number[1] = '.';
    number[number.len - 1] = '1';
    try std.testing.expectEqual(std.math.Order.gt, Number.parse(number).?.order(Number.parse("1").?));
    const exponent = try alloc.dupe(u8, number);
    defer alloc.free(exponent);
    exponent[1] = 'e';
    @memset(exponent[2..], '9');
    try std.testing.expectEqual(std.math.Order.gt, Number.parse(exponent).?.order(Number.parse("1e9999").?));
    number[number.len - 1] = '0';
    try std.testing.expectEqual(std.math.Order.eq, Number.parse(number).?.order(Number.parse("1").?));
}
