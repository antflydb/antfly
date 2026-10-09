// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! H.264 8.3.1 progressive Intra4x4 sample prediction.
const std = @import("std");
const layout = @import("h264_layout.zig");
fn half(a: i32, b: i32) i32 {
    return (a + b + 1) >> 1;
}
fn quarter(a: i32, b: i32, c: i32) i32 {
    return (a + 2 * b + c + 2) >> 2;
}
fn reference(top: [16]i32, left: [8]i32, corner: i32, i: i32) i32 {
    return if (i == 0) corner else if (i > 0) top[@intCast(i - 1)] else left[@intCast(-i - 1)];
}
fn verticalRight(top: [16]i32, left: [8]i32, corner: i32, x: i32, y: i32) i32 {
    const z = 2 * x - y;
    if (z >= 0) {
        const i = x - @divTrunc(y, 2);
        return if (z & 1 == 0) half(reference(top, left, corner, i), reference(top, left, corner, i + 1)) else quarter(reference(top, left, corner, i - 1), reference(top, left, corner, i), reference(top, left, corner, i + 1));
    }
    return if (z == -1) quarter(left[0], corner, top[0]) else quarter(reference(top, left, corner, z), reference(top, left, corner, z + 1), reference(top, left, corner, z + 2));
}
pub const Availability = struct { top: bool, left: bool, corner: bool, top_right: bool = false };
pub fn predict(plane: anytype, stride: usize, x: usize, y: usize, mode: u8, neighbors: Availability, bit_depth: u8) !void {
    return predictSize(plane, stride, x, y, mode, neighbors, 4, bit_depth);
}
pub fn predict8(plane: anytype, stride: usize, x: usize, y: usize, mode: u8, neighbors: Availability, bit_depth: u8) !void {
    return predictSize(plane, stride, x, y, mode, neighbors, 8, bit_depth);
}
fn predictSize(plane: anytype, stride: usize, x: usize, y: usize, mode: u8, neighbors: Availability, size: usize, bit_depth: u8) !void {
    const has_top = neighbors.top;
    const has_left = neighbors.left;
    if (mode > 8 or ((!has_top) and (mode == 0 or mode == 3 or mode == 4 or mode == 5 or mode == 6 or mode == 7)) or ((!has_left) and (mode == 1 or mode == 4 or mode == 5 or mode == 6 or mode == 8))) return error.MalformedVideoPacket;
    if (!neighbors.corner and (mode == 4 or mode == 5 or mode == 6)) return error.MalformedVideoPacket;
    var top: [16]i32 = @splat(@as(i32, 1) << @as(u5, @intCast(bit_depth - 1)));
    var left: [8]i32 = @splat(@as(i32, 1) << @as(u5, @intCast(bit_depth - 1)));
    if (has_top) {
        for (0..size) |i| top[i] = layout.get(plane, (y - 1) * stride + x + i);
        for (size..size * 2) |i| top[i] = if (neighbors.top_right) layout.get(plane, (y - 1) * stride + x + i) else top[size - 1];
    }
    if (has_left) for (0..size) |i| {
        left[i] = layout.get(plane, (y + i) * stride + x - 1);
    };
    var corner: i32 = if (neighbors.corner) layout.get(plane, (y - 1) * stride + x - 1) else @as(i32, 1) << @as(u5, @intCast(bit_depth - 1));
    if (size == 8) {
        const original_top = top;
        const original_left = left;
        if (has_top) {
            top[0] = quarter(if (neighbors.corner) corner else original_top[0], original_top[0], original_top[1]);
            for (1..15) |i| top[i] = quarter(original_top[i - 1], original_top[i], original_top[i + 1]);
            top[15] = quarter(original_top[14], original_top[15], original_top[15]);
        }
        if (has_left) {
            left[0] = quarter(if (neighbors.corner) corner else original_left[0], original_left[0], original_left[1]);
            for (1..7) |i| left[i] = quarter(original_left[i - 1], original_left[i], original_left[i + 1]);
            left[7] = quarter(original_left[6], original_left[7], original_left[7]);
        }
        if (neighbors.corner) corner = quarter(original_left[0], corner, original_top[0]);
    }
    var sum: i32 = 0;
    for (0..size) |i| {
        if (has_top) sum += top[i];
        if (has_left) sum += left[i];
    }
    const dc: i32 = if (has_top and has_left) @divTrunc(sum + @as(i32, @intCast(size)), @as(i32, @intCast(size * 2))) else if (has_top or has_left) @divTrunc(sum + @as(i32, @intCast(size / 2)), @as(i32, @intCast(size))) else @as(i32, 1) << @as(u5, @intCast(bit_depth - 1));
    var swapped: [16]i32 = @splat(0);
    @memcpy(swapped[0..size], left[0..size]);
    for (0..size) |row| for (0..size) |column| {
        const cx: i32 = @intCast(column);
        const cy: i32 = @intCast(row);
        const value = switch (mode) {
            0 => top[column],
            1 => left[row],
            2 => dc,
            3 => quarter(top[@min(column + row, size * 2 - 1)], top[@min(column + row + 1, size * 2 - 1)], top[@min(column + row + 2, size * 2 - 1)]),
            4 => quarter(reference(top, left, corner, cx - cy - 1), reference(top, left, corner, cx - cy), reference(top, left, corner, cx - cy + 1)),
            5 => verticalRight(top, left, corner, cx, cy),
            6 => verticalRight(swapped, top[0..8].*, corner, cy, cx),
            7 => if (row & 1 == 0) half(top[column + row / 2], top[column + row / 2 + 1]) else quarter(top[column + row / 2], top[column + row / 2 + 1], top[column + row / 2 + 2]),
            8 => if (column & 1 == 0) half(left[@min(row + column / 2, size - 1)], left[@min(row + column / 2 + 1, size - 1)]) else quarter(left[@min(row + column / 2, size - 1)], left[@min(row + column / 2 + 1, size - 1)], left[@min(row + column / 2 + 2, size - 1)]),
            else => unreachable,
        };
        layout.put(plane, (y + row) * stride + x + column, @intCast(std.math.clamp(value, 0, (@as(i32, 1) << @as(u5, @intCast(bit_depth))) - 1)));
    };
}
