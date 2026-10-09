// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Progressive 8x8 scan, inverse scaling and integer transform, H.264 8.5.
const std = @import("std");
pub const scan = [_]usize{ 0, 1, 8, 16, 9, 2, 3, 10, 17, 24, 32, 25, 18, 11, 4, 5, 12, 19, 26, 33, 40, 48, 41, 34, 27, 20, 13, 6, 7, 14, 21, 28, 35, 42, 49, 56, 57, 50, 43, 36, 29, 22, 15, 23, 30, 37, 44, 51, 58, 59, 52, 45, 38, 31, 39, 46, 53, 60, 61, 54, 47, 55, 62, 63 };
const factors = [6][6]i64{ .{ 20, 18, 32, 19, 25, 24 }, .{ 22, 19, 35, 21, 28, 26 }, .{ 26, 23, 42, 24, 33, 31 }, .{ 28, 25, 45, 26, 35, 33 }, .{ 32, 28, 51, 30, 40, 38 }, .{ 36, 32, 58, 34, 46, 43 } };
fn scale(coefficient: i32, qp: usize, position: usize) i64 {
    const x = position % 8;
    const y = position / 8;
    const category: usize = if (x % 4 == 0 and y % 4 == 0) 0 else if (x % 2 == 1 and y % 2 == 1) 1 else if (x % 4 == 2 and y % 4 == 2) 2 else if ((x % 4 == 0 and y % 2 == 1) or (y % 4 == 0 and x % 2 == 1)) 3 else if (x % 2 == 0 and y % 2 == 0) 4 else 5;
    const value = @as(i64, coefficient) * factors[qp % 6][category];
    return if (qp >= 12) value << @as(u6, @intCast(qp / 6 - 2)) else (value + (@as(i64, 1) << @as(u6, @intCast(1 - qp / 6)))) >> @as(u6, @intCast(2 - qp / 6));
}
fn transform(d: [8]i64) [8]i64 {
    const e = [8]i64{ d[0] + d[4], -d[3] + d[5] - d[7] - (d[7] >> 1), d[0] - d[4], d[1] + d[7] - d[3] - (d[3] >> 1), (d[2] >> 1) - d[6], -d[1] + d[7] + d[5] + (d[5] >> 1), d[2] + (d[6] >> 1), d[3] + d[5] + d[1] + (d[1] >> 1) };
    const f = [8]i64{ e[0] + e[6], e[1] + (e[7] >> 2), e[2] + e[4], e[3] + (e[5] >> 2), e[2] - e[4], (e[3] >> 2) - e[5], e[0] - e[6], e[7] - (e[1] >> 2) };
    return .{ f[0] + f[7], f[2] + f[5], f[4] + f[3], f[6] + f[1], f[6] - f[1], f[4] - f[3], f[2] - f[5], f[0] - f[7] };
}
pub fn add(plane: []u8, stride: usize, x: usize, y: usize, coefficients: [64]i32, qp: usize) void {
    var rows: [64]i64 = undefined;
    for (0..8) |row| {
        var d: [8]i64 = undefined;
        for (0..8) |column| d[column] = scale(coefficients[row * 8 + column], qp, row * 8 + column);
        rows[row * 8 ..][0..8].* = transform(d);
    }
    for (0..8) |column| {
        var d: [8]i64 = undefined;
        for (0..8) |row| d[row] = rows[row * 8 + column];
        const output = transform(d);
        for (0..8) |row| {
            const index = (y + row) * stride + x + column;
            plane[index] = @intCast(std.math.clamp(@as(i64, plane[index]) + ((output[row] + 32) >> 6), 0, 255));
        }
    }
}
