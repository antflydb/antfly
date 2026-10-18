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

//! Fixed decimal128 coefficient validation and boundary formatting.
const std = @import("std");
const coefficient_limits = blk: {
    var out: [39]u128 = undefined;
    out[0] = 1;
    for (1..out.len) |i| out[i] = out[i - 1] * 10;
    break :blk out;
};
pub fn validate(precision: i32, scale: i32) !void {
    if (precision < 1 or precision > 38 or scale < 0 or scale > precision) return error.UnsupportedDecimal128;
}
pub fn coefficientLimit(precision: i32, scale: i32) !u128 {
    try validate(precision, scale);
    return coefficient_limits[@intCast(precision)];
}
pub fn validateCoefficient(value: i128, precision: i32, scale: i32) !void {
    if (@abs(value) >= try coefficientLimit(precision, scale)) return error.InvalidDecimal128;
}
pub fn formatAlloc(a: std.mem.Allocator, unscaled: i128, precision: i32, scale: i32) ![]u8 {
    try validate(precision, scale);
    var buffer: [39]u8 = undefined;
    const digits = try std.fmt.bufPrint(&buffer, "{d}", .{@abs(unscaled)});
    if (digits.len > @as(usize, @intCast(precision))) return error.InvalidDecimal128;
    const fractional: usize = @intCast(scale);
    const negative: usize = @intFromBool(unscaled < 0);
    const integral = if (digits.len > fractional) digits.len - fractional else 1;
    const result = try a.alloc(u8, negative + integral + (if (fractional == 0) @as(usize, 0) else fractional + 1));
    var position: usize = 0;
    if (negative != 0) {
        result[0] = '-';
        position = 1;
    }
    if (fractional == 0) {
        @memcpy(result[position..], digits);
    } else if (digits.len > fractional) {
        @memcpy(result[position..][0..integral], digits[0..integral]);
        result[position + integral] = '.';
        @memcpy(result[position + integral + 1 ..], digits[integral..]);
    } else {
        result[position] = '0';
        result[position + 1] = '.';
        @memset(result[position + 2 ..][0 .. fractional - digits.len], '0');
        @memcpy(result[result.len - digits.len ..], digits);
    }
    return result;
}

/// Parquet and Iceberg decimal metrics use signed big-endian two's complement.
pub fn signedBigEndian(bytes: []const u8) !i128 {
    if (bytes.len == 0 or bytes.len > 16) return error.InvalidDecimal128;
    var raw: u128 = if (bytes[0] & 0x80 != 0) std.math.maxInt(u128) else 0;
    for (bytes) |byte| raw = (raw << 8) | byte;
    return @bitCast(raw);
}
pub fn validateBounds(min: []const u8, max: []const u8, precision: i32, scale: i32) !void {
    const low = try signedBigEndian(min);
    const high = try signedBigEndian(max);
    try validateCoefficient(low, precision, scale);
    try validateCoefficient(high, precision, scale);
    if (low > high) return error.InvalidDecimal128;
}

/// Scale alignment in i256 is exact for every finite decimal128 coefficient.
pub fn order(left: i128, ls: u16, right: i128, rs: u16) std.math.Order {
    std.debug.assert(ls <= 38 and rs <= 38);
    const powers = comptime blk: {
        var out: [39]i256 = undefined;
        out[0] = 1;
        for (1..out.len) |i| out[i] = out[i - 1] * 10;
        break :blk out;
    };
    const scale = @max(ls, rs);
    return std.math.order(@as(i256, left) * powers[scale - ls], @as(i256, right) * powers[scale - rs]);
}

test "decimal128 metric bounds compare signed values and exact mixed scales" {
    var low: [16]u8 = undefined;
    var high: [16]u8 = undefined;
    std.mem.writeInt(i128, &low, -12345, .big);
    std.mem.writeInt(i128, &high, 98765, .big);
    try validateBounds(&low, &high, 10, 3);
    try std.testing.expectError(error.InvalidDecimal128, validateBounds(&high, &low, 10, 3));
    try std.testing.expectEqual(std.math.Order.eq, order(-123, 2, -12300, 4));
    try std.testing.expectEqual(std.math.Order.lt, order(-1, 38, 0, 0));
    try std.testing.expectEqual(std.math.Order.gt, order(std.math.maxInt(i128), 0, 1, 38));
}
