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

//! Allocation-free bounded structural JSON ordering and semantic hashing.
//! Object order is independent of insertion order; decimal number tokens are
//! compared exactly, without collapsing integers through floating point.
const std = @import("std");
const Json = std.json.Value;
const Order = std.math.Order;
pub const Budget = struct {
    remaining: usize = 1_048_576,
    pub fn consume(self: *Budget, amount: usize) !void {
        if (amount > self.remaining) return error.SqlProgramLimitExceeded;
        self.remaining -= amount;
    }
};

const Decimal = struct {
    negative: bool,
    magnitude: i64,
    digits: []const u8,

    fn parse(text: []const u8, budget: *Budget) !Decimal {
        try budget.consume(text.len);
        if (text.len == 0) return error.SqlTypeMismatch;
        var at: usize = @intFromBool(text[0] == '-');
        const start = at;
        var before: i64 = 0;
        var leading: i64 = 0;
        var seen_dot = false;
        var first: ?usize = null;
        var last: usize = 0;
        while (at < text.len and text[at] != 'e' and text[at] != 'E') : (at += 1) {
            if (text[at] == '.' and !seen_dot) {
                seen_dot = true;
                continue;
            }
            if (!std.ascii.isDigit(text[at])) return error.SqlTypeMismatch;
            if (!seen_dot) before += 1;
            if (text[at] != '0') {
                if (first == null) first = at;
                last = at;
            } else if (first == null) leading += 1;
        }
        if (at == start) return error.SqlTypeMismatch;
        const exponent = if (at < text.len) std.fmt.parseInt(i64, text[at + 1 ..], 10) catch return error.SqlNumericOutOfRange else 0;
        if (first == null) return .{ .negative = false, .magnitude = 0, .digits = "" };
        const magnitude = std.math.add(i64, exponent, before - leading - 1) catch return error.SqlNumericOutOfRange;
        return .{ .negative = text[0] == '-', .magnitude = magnitude, .digits = text[first.? .. last + 1] };
    }

    fn compare(a: Decimal, b: Decimal) Order {
        if (a.negative != b.negative) return if (a.negative) .lt else .gt;
        if (a.digits.len == 0 or b.digits.len == 0) return if (a.digits.len == b.digits.len) .eq else if (a.digits.len == 0) (if (b.negative) .gt else .lt) else if (a.negative) .lt else .gt;
        var order = std.math.order(a.magnitude, b.magnitude);
        if (order == .eq) {
            var i: usize = 0;
            var j: usize = 0;
            while (i < a.digits.len or j < b.digits.len) {
                if (i < a.digits.len and a.digits[i] == '.') i += 1;
                if (j < b.digits.len and b.digits[j] == '.') j += 1;
                const ac: u8 = if (i < a.digits.len) a.digits[i] else '0';
                const bc: u8 = if (j < b.digits.len) b.digits[j] else '0';
                order = std.math.order(ac, bc);
                if (order != .eq) break;
                i += @intFromBool(i < a.digits.len);
                j += @intFromBool(j < b.digits.len);
            }
        }
        return if (a.negative) order.invert() else order;
    }

    fn hash(self: Decimal) u64 {
        var state = std.hash.Wyhash.init(2);
        state.update(&.{@intFromBool(self.negative)});
        var magnitude: [8]u8 = undefined;
        std.mem.writeInt(i64, &magnitude, self.magnitude, .little);
        state.update(&magnitude);
        for (self.digits) |digit| if (digit != '.') state.update(&.{digit});
        return state.final();
    }
};

fn rank(value: Json) u8 {
    return switch (value) {
        .null => 0,
        .string => 1,
        .integer, .float, .number_string => 2,
        .bool => 3,
        .array => 4,
        .object => 5,
    };
}
fn decimal(value: Json, buffer: []u8, budget: *Budget) !Decimal {
    const text = switch (value) {
        .integer => |v| try std.fmt.bufPrint(buffer, "{d}", .{v}),
        .float => |v| {
            if (!std.math.isFinite(v)) return error.SqlNumericOutOfRange;
            if (v == 0) return .{ .negative = false, .magnitude = 0, .digits = "" };
            const bits: u64 = @bitCast(v);
            const raw_exponent = (bits >> 52) & 0x7ff;
            var mantissa: u64 = bits & 0xfffffffffffff;
            if (raw_exponent != 0) mantissa |= 1 << 52;
            var exponent: i32 = if (raw_exponent == 0) -1074 else @as(i32, @intCast(raw_exponent)) - 1023 - 52;
            while (exponent < 0 and mantissa & 1 == 0) {
                mantissa >>= 1;
                exponent += 1;
            }
            var exact: u4096 = mantissa;
            if (exponent >= 0) exact <<= @intCast(exponent) else {
                try budget.consume(@intCast(-exponent));
                for (0..@intCast(-exponent)) |_| exact *= 5;
            }
            var result = try Decimal.parse(try std.fmt.bufPrint(buffer, "{d}", .{exact}), budget);
            result.negative = bits >> 63 != 0;
            if (exponent < 0) result.magnitude += exponent;
            return result;
        },
        .number_string => |v| v,
        else => return error.SqlTypeMismatch,
    };
    return Decimal.parse(text, budget);
}
fn keyOrder(a: []const u8, b: []const u8) Order {
    const lengths = std.math.order(a.len, b.len);
    return if (lengths == .eq) std.mem.order(u8, a, b) else lengths;
}

/// Every finite primitive number has one exact dyadic representation. Hash
/// that representation without formatting decimal text or using big integers.
/// Decimal JSON tokens enter this domain only after an exact equality check.
fn primitiveHash(value: Json) !u64 {
    var negative: bool = false;
    var mantissa: u64 = 0;
    var exponent: i32 = 0;
    switch (value) {
        .integer => |n| {
            negative = n < 0;
            mantissa = @intCast(@abs(n));
        },
        .float => |n| {
            if (!std.math.isFinite(n)) return error.SqlNumericOutOfRange;
            const bits: u64 = @bitCast(n);
            negative = bits >> 63 != 0;
            const raw = (bits >> 52) & 0x7ff;
            mantissa = bits & 0xfffffffffffff;
            if (raw != 0) mantissa |= 1 << 52;
            exponent = if (raw == 0) -1074 else @as(i32, @intCast(raw)) - 1023 - 52;
        },
        else => unreachable,
    }
    if (mantissa == 0) {
        negative = false;
        exponent = 0;
    } else {
        const zeros = @ctz(mantissa);
        mantissa >>= @intCast(zeros);
        exponent += zeros;
    }
    var bytes: [13]u8 = undefined;
    bytes[0] = @intFromBool(negative);
    std.mem.writeInt(u64, bytes[1..9], mantissa, .little);
    std.mem.writeInt(i32, bytes[9..13], exponent, .little);
    return std.hash.Wyhash.hash(2, &bytes);
}
fn tokenHash(text: []const u8, budget: *Budget) !u64 {
    const normalized = try Decimal.parse(text, budget);
    if (normalized.digits.len == 0) return primitiveHash(.{ .integer = 0 });
    // Reconstruct only the bounded integer domain, including spellings such
    // as 9007199254740993.0 that must not round through an f64.
    if (normalized.magnitude >= 0 and normalized.magnitude <= 18) integer: {
        var magnitude: u64 = 0;
        var digits: usize = 0;
        for (normalized.digits) |digit| {
            if (digit == '.') continue;
            magnitude = std.math.mul(u64, magnitude, 10) catch break :integer;
            magnitude = std.math.add(u64, magnitude, digit - '0') catch break :integer;
            digits += 1;
        }
        const width: usize = @intCast(normalized.magnitude + 1);
        if (digits > width) break :integer;
        for (digits..width) |_| magnitude = std.math.mul(u64, magnitude, 10) catch break :integer;
        const signed: i128 = if (normalized.negative) -@as(i128, magnitude) else magnitude;
        if (std.math.cast(i64, signed)) |n| return primitiveHash(.{ .integer = n });
    }
    const number = std.fmt.parseFloat(f64, text) catch return normalized.hash();
    if (std.math.isFinite(number)) {
        var buffer: [768]u8 = undefined;
        if (Decimal.compare(normalized, try decimal(.{ .float = number }, &buffer, budget)) == .eq) return primitiveHash(.{ .float = number });
    }
    return normalized.hash();
}

pub fn compare(a: Json, b: Json, budget: *Budget, depth: usize) anyerror!Order {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    try budget.consume(1);
    const ranks = std.math.order(rank(a), rank(b));
    if (ranks != .eq) return ranks;
    if (rank(a) == 2) {
        var left: [768]u8 = undefined;
        var right: [768]u8 = undefined;
        return Decimal.compare(try decimal(a, &left, budget), try decimal(b, &right, budget));
    }
    return switch (a) {
        .null => .eq,
        .bool => std.math.order(@intFromBool(a.bool), @intFromBool(b.bool)),
        .string => blk: {
            try budget.consume(@min(a.string.len, b.string.len));
            break :blk std.mem.order(u8, a.string, b.string);
        },
        .array => blk: {
            const sizes = std.math.order(a.array.items.len, b.array.items.len);
            if (sizes != .eq) break :blk sizes;
            for (a.array.items, b.array.items) |left, right| {
                const result = try compare(left, right, budget, depth + 1);
                if (result != .eq) break :blk result;
            }
            break :blk .eq;
        },
        .object => blk: {
            const sizes = std.math.order(a.object.count(), b.object.count());
            if (sizes != .eq) break :blk sizes;
            var smallest: ?[]const u8 = null;
            var result: Order = .eq;
            for (a.object.keys(), a.object.values()) |key, value| {
                try budget.consume(key.len + 1);
                const other = b.object.get(key);
                const difference: Order = if (other) |item| try compare(value, item, budget, depth + 1) else .lt;
                if (difference != .eq and (smallest == null or keyOrder(key, smallest.?) == .lt)) {
                    smallest = key;
                    result = difference;
                }
            }
            for (b.object.keys()) |key| {
                try budget.consume(key.len + 1);
                if (!a.object.contains(key) and (smallest == null or keyOrder(key, smallest.?) == .lt)) {
                    smallest = key;
                    result = .gt;
                }
            }
            break :blk result;
        },
        else => unreachable,
    };
}

pub fn hash(value: Json, budget: *Budget, depth: usize) anyerror!u64 {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    try budget.consume(1);
    if (rank(value) == 2) {
        return switch (value) {
            .integer, .float => primitiveHash(value),
            .number_string => |text| tokenHash(text, budget),
            else => unreachable,
        };
    }
    var state = std.hash.Wyhash.init(rank(value));
    switch (value) {
        .null => {},
        .bool => state.update(&.{@intFromBool(value.bool)}),
        .string => {
            try budget.consume(value.string.len);
            state.update(value.string);
        },
        .array => for (value.array.items) |item| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, try hash(item, budget, depth + 1), .little);
            state.update(&bytes);
        },
        .object => {
            var sum: u64 = 0;
            var mixed: u64 = 0;
            for (value.object.keys(), value.object.values()) |key, item| {
                try budget.consume(key.len + 1);
                const pair = std.hash.Wyhash.hash(try hash(item, budget, depth + 1), key);
                sum +%= pair;
                mixed ^= std.math.rotl(u64, pair, 23);
            }
            var bytes: [16]u8 = undefined;
            std.mem.writeInt(u64, bytes[0..8], sum, .little);
            std.mem.writeInt(u64, bytes[8..16], mixed, .little);
            state.update(&bytes);
        },
        else => unreachable,
    }
    return state.final();
}

test "structural JSON ordering and hashes ignore object order and exact numeric spelling" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const pairs = [_][2][]const u8{
        .{ "{\"b\":[1,2],\"a\":null}", "{\"a\":null,\"b\":[1.0,2e0]}" },
        .{ "-0.0", "0" },
        .{ "10000e-4", "1.0000" },
        .{ "9007199254740993", "9007199254740993.0" },
    };
    for (pairs) |pair| {
        const a = try std.json.parseFromSliceLeaky(Json, arena.allocator(), pair[0], .{ .parse_numbers = false });
        const b = try std.json.parseFromSliceLeaky(Json, arena.allocator(), pair[1], .{ .parse_numbers = false });
        var budget: Budget = .{};
        try std.testing.expectEqual(Order.eq, try compare(a, b, &budget, 0));
        try std.testing.expectEqual(try hash(a, &budget, 0), try hash(b, &budget, 0));
    }
    var budget: Budget = .{};
    try std.testing.expectEqual(Order.gt, try compare(.{ .number_string = "9007199254740993" }, .{ .float = 9007199254740992 }, &budget, 0));
    const rounded: Json = .{ .float = 1000000000000000128.0 };
    const exact: Json = .{ .integer = 1000000000000000128 };
    try std.testing.expectEqual(Order.eq, try compare(rounded, exact, &budget, 0));
    try std.testing.expectEqual(try hash(rounded, &budget, 0), try hash(exact, &budget, 0));
    var tiny: Budget = .{ .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, compare(.{ .string = "abc" }, .{ .string = "abc" }, &tiny, 0));
}

test "primitive numeric hashes preserve exact mixed integer float and decimal equivalence" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    for ([_]i64{ 0, 1, -1, 10000, 9007199254740993, std.math.minInt(i64), std.math.maxInt(i64) }) |integer| {
        const token = try std.fmt.allocPrint(a, "{d}.000", .{integer});
        defer a.free(token);
        try std.testing.expectEqual(try hash(.{ .integer = integer }, &budget, 0), try hash(.{ .number_string = token }, &budget, 0));
    }
    for ([_]f64{ 0, -0.0, 0.5, -1.25, 0.1, std.math.floatMin(f64), std.math.floatMax(f64), 1000000000000000128.0 }) |number| {
        var bytes: [768]u8 = undefined;
        const exact = try decimal(.{ .float = number }, &bytes, &budget);
        const digits = if (exact.digits.len == 0) "0" else exact.digits;
        const token = try std.fmt.allocPrint(a, "{s}{s}e{d}", .{ if (exact.negative) "-" else "", digits, exact.magnitude - @as(i64, @intCast(digits.len)) + 1 });
        defer a.free(token);
        try std.testing.expectEqual(Order.eq, try compare(.{ .float = number }, .{ .number_string = token }, &budget, 0));
        try std.testing.expectEqual(try hash(.{ .float = number }, &budget, 0), try hash(.{ .number_string = token }, &budget, 0));
    }
    try std.testing.expect((try hash(.{ .float = 0.1 }, &budget, 0)) != (try hash(.{ .number_string = "0.1" }, &budget, 0)));
    try std.testing.expectEqual(try hash(.{ .integer = 1 }, &budget, 0), try hash(.{ .float = 1 }, &budget, 0));
}

test "native pipeline refinements benchmark primitive numeric hashing" {
    if (@import("builtin").mode == .debug) return error.SkipZigTest;
    const count = 100_000;
    var values: [1024]Json = undefined;
    for (&values, 0..) |*value, index| value.* = if (index % 2 == 0) .{ .integer = @intCast(index * 1009) } else .{ .float = @as(f64, @floatFromInt(index)) + 0.1 };
    var prior_checksums: [2]?u64 = .{ null, null };
    for (0..3) |sample| {
        var elapsed: [2]i96 = undefined;
        for (0..2) |pass| {
            const fast = (sample + pass) % 2 != 0;
            const slot = @intFromBool(fast);
            const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            var checksum: u64 = 0;
            for (0..count) |index| {
                var budget: Budget = .{};
                const value = values[index % values.len];
                if (fast) {
                    checksum ^= try hash(value, &budget, 0);
                } else {
                    // Frozen prior primitive path, with the same exact decimal
                    // normalization and semantic hash used before this PR.
                    var buffer: [768]u8 = undefined;
                    checksum ^= (try decimal(value, &buffer, &budget)).hash();
                }
            }
            elapsed[slot] = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
            if (prior_checksums[slot]) |prior| try std.testing.expectEqual(prior, checksum) else prior_checksums[slot] = checksum;
        }
        std.debug.print("native_refinement {{\"case\":\"primitive_numeric_hash\",\"rows\":{d},\"sample\":{d},\"decimal_ns\":{d},\"primitive_ns\":{d}}}\n", .{ count, sample, elapsed[0], elapsed[1] });
    }
}
