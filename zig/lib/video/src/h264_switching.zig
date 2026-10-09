// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! ITU-T H.264 8.6, 8-bit 4:2:0 Extended-profile switching reconstruction.
const std = @import("std");
const layout = @import("h264_layout.zig");
const inverse = @import("h264_transform4.zig");
const factors = [6][3]i64{ .{ 10, 13, 16 }, .{ 11, 14, 18 }, .{ 13, 16, 20 }, .{ 14, 18, 23 }, .{ 16, 20, 25 }, .{ 18, 23, 29 } };
const quantizers = [6][3]i64{ .{ 13107, 8066, 5243 }, .{ 11916, 7490, 4660 }, .{ 10082, 6554, 4194 }, .{ 9362, 5825, 3647 }, .{ 8192, 5243, 3355 }, .{ 7282, 4559, 2893 } };
const matrix = [4][4]i64{ .{ 1, 1, 1, 1 }, .{ 2, 1, -1, -2 }, .{ 1, -1, -1, 1 }, .{ 1, -2, 2, -1 } };
fn category(i: usize) usize {
    return (i / 4 & 1) + (i % 4 & 1);
}
fn forward(plane: anytype, stride: usize, x: usize, y: usize) [16]i64 {
    var output: [16]i64 = @splat(0);
    for (0..4) |r| for (0..4) |c| for (0..4) |a| for (0..4) |b| {
        output[r * 4 + c] += matrix[r][a] * @as(i64, layout.get(plane, (y + a) * stride + x + b)) * matrix[c][b];
    };
    return output;
}
fn quantize(value: i64, qs: usize, cat: usize, dc: bool) i64 {
    const shift: u6 = @intCast(15 + qs / 6 + @intFromBool(dc));
    const absolute: i64 = @intCast(@abs(value));
    const quantized = (absolute * quantizers[qs % 6][cat] + (@as(i64, 1) << (shift - 1))) >> shift;
    return if (value < 0) -quantized else quantized;
}
fn residual(value: i64, qp: usize, cat: usize, dc: bool) i64 {
    const a: i64 = if (cat == 0) 16 else if (cat == 1) 20 else 25;
    return (value * factors[qp % 6][cat] * 16 * a << @as(u6, @intCast(qp / 6))) >> (if (dc) @as(u6, 9) else 10);
}
fn scaled(value: i64, qs: usize, cat: usize) i64 {
    return value * factors[qs % 6][cat] << @as(u6, @intCast(qs / 6));
}
fn write(plane: anytype, stride: usize, x: usize, y: usize, coefficients: [16]i64) void {
    const result = inverse.scalar(coefficients);
    for (0..4) |r| for (0..4) |c| layout.put(plane, (y + r) * stride + x + c, @intCast(std.math.clamp((result[r * 4 + c] + 32) >> 6, 0, 255)));
}
pub fn luma(plane: anytype, stride: usize, x: usize, y: usize, levels: [16]i64, qp: usize, qs: usize, switching: bool) void {
    const prediction = forward(plane, stride, x, y);
    var coefficients: [16]i64 = undefined;
    for (0..16) |i| {
        const cat = category(i);
        const value = if (switching) quantize(prediction[i], qs, cat, false) + levels[i] else quantize(prediction[i] + residual(levels[i], qp, cat, false), qs, cat, false);
        coefficients[i] = scaled(value, qs, cat);
    }
    write(plane, stride, x, y, coefficients);
}
fn hadamard(input: [4]i64) [4]i64 {
    return .{ input[0] + input[1] + input[2] + input[3], input[0] - input[1] + input[2] - input[3], input[0] + input[1] - input[2] - input[3], input[0] - input[1] - input[2] + input[3] };
}
pub fn chroma(plane: anytype, stride: usize, x: usize, y: usize, levels: [4][16]i64, dc_levels: [4]i64, qp: usize, qs: usize, switching: bool) void {
    var predictions: [4][16]i64 = undefined;
    var dc: [4]i64 = undefined;
    for (0..4) |i| {
        predictions[i] = forward(plane, stride, x + i % 2 * 4, y + i / 2 * 4);
        dc[i] = predictions[i][0];
    }
    const predicted_dc = hadamard(dc);
    for (0..4) |i| dc[i] = if (switching) quantize(predicted_dc[i], qs, 0, true) + dc_levels[(i % 2) * 2 + i / 2] else quantize(predicted_dc[i] + residual(dc_levels[(i % 2) * 2 + i / 2], qp, 0, true), qs, 0, true);
    const combined_dc = hadamard(dc);
    for (0..4) |i| {
        var coefficients: [16]i64 = undefined;
        coefficients[0] = (combined_dc[i] * factors[qs % 6][0] * 16 << @as(u6, @intCast(qs / 6))) >> 5;
        for (1..16) |j| {
            const cat = category(j);
            const value = if (switching) quantize(predictions[i][j], qs, cat, false) + levels[i][j] else quantize(predictions[i][j] + residual(levels[i][j], qp, cat, false), qs, cat, false);
            coefficients[j] = scaled(value, qs, cat);
        }
        write(plane, stride, x + i % 2 * 4, y + i / 2 * 4, coefficients);
    }
}
pub fn skipped(planes: anytype, width: usize, x: usize, y: usize, qp: usize, qs: usize, chroma_qp: [2]usize, chroma_qs: [2]usize, switching: bool) void {
    for (0..4) |r| for (0..4) |c| luma(planes[0], width, x + c * 4, y + r * 4, @splat(0), qp, qs, switching);
    for (1..3) |p| chroma(planes[p], width / 2, x / 2, y / 2, @splat(@splat(0)), @splat(0), chroma_qp[p - 1], chroma_qs[p - 1], switching);
}
