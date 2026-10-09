// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Integer 4x4 inverse transform. Portable vectors preserve signed shift and
//! 64-bit coefficient semantics; Zig lowers each vector for the target ISA.
const std = @import("std");
pub fn scalar(input: [16]i64) [16]i64 {
    var temp: [16]i64 = undefined;
    var result: [16]i64 = undefined;
    for (0..4) |row| {
        const p = input[row * 4 ..][0..4];
        const a = p[0] + p[2];
        const b = p[0] - p[2];
        const c = (p[1] >> 1) - p[3];
        const d = p[1] + (p[3] >> 1);
        temp[row * 4 ..][0..4].* = .{ a + d, b + c, b - c, a - d };
    }
    for (0..4) |column| {
        const a = temp[column] + temp[8 + column];
        const b = temp[column] - temp[8 + column];
        const c = (temp[4 + column] >> 1) - temp[12 + column];
        const d = temp[4 + column] + (temp[12 + column] >> 1);
        result[column] = a + d;
        result[4 + column] = b + c;
        result[8 + column] = b - c;
        result[12 + column] = a - d;
    }
    return result;
}
const V = @Vector(4, i32);
fn pass(p: [4]V) [4]V {
    const a = p[0] + p[2];
    const b = p[0] - p[2];
    const c = (p[1] >> @as(@Vector(4, u5), @splat(1))) - p[3];
    const d = p[1] + (p[3] >> @as(@Vector(4, u5), @splat(1)));
    return .{ a + d, b + c, b - c, a - d };
}
pub fn simd(input: [16]i64) [16]i64 {
    const wide: @Vector(16, i64) = input;
    const limit: @Vector(16, i64) = @splat(1 << 26);
    if (@reduce(.Or, (wide < -limit) | (wide > limit))) return scalar(input);
    var small: [16]i32 = undefined;
    inline for (0..16) |i| small[i] = @intCast(input[i]);
    var columns: [4]V = undefined;
    inline for (0..4) |c| columns[c] = .{ small[c], small[4 + c], small[8 + c], small[12 + c] };
    const first = pass(columns);
    var rows: [4]V = undefined;
    inline for (0..4) |r| rows[r] = .{ first[0][r], first[1][r], first[2][r], first[3][r] };
    const second = pass(rows);
    var output: [16]i64 = undefined;
    inline for (0..4) |r| inline for (0..4) |c| {
        output[r * 4 + c] = second[r][c];
    };
    return output;
}
test "video portable SIMD inverse transform equals scalar for signed and large coefficients" {
    var random = std.Random.DefaultPrng.init(0x5a41f4);
    for (0..4096) |iteration| {
        var input: [16]i64 = undefined;
        for (&input) |*value| value.* = random.random().intRangeAtMost(i64, -(if (iteration % 2 == 0) @as(i64, 100_000) else @as(i64, 1) << 48), if (iteration % 2 == 0) @as(i64, 100_000) else @as(i64, 1) << 48);
        try std.testing.expectEqualSlices(i64, &scalar(input), &simd(input));
    }
}
