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
pub fn validate(precision: i32, scale: i32) !void {
    if (precision < 1 or precision > 38 or scale < 0 or scale > precision) return error.UnsupportedDecimal128;
}
pub fn coefficientLimit(precision: i32, scale: i32) !u128 {
    try validate(precision, scale);
    var maximum: u128 = 1;
    for (0..@intCast(precision)) |_| maximum *= 10;
    return maximum;
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
