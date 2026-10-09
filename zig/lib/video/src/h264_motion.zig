// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Native-depth interpolation over frame/field views; 4:4:4 uses luma filtering.
const std = @import("std");
const layout = @import("h264_layout.zig");
pub const Vector = struct { x: i32 = 0, y: i32 = 0 };
pub const Motion = struct { identity: u32 = std.math.maxInt(u32), direct: bool = false, decoded: bool = false, reference: i8 = -1, vector: Vector = .{}, difference: Vector = .{} };
fn sample(plane: anytype, stride: usize, x: i32, y: i32) i64 {
    const height = layout.length(plane) / stride;
    return layout.get(plane, @as(usize, @intCast(std.math.clamp(y, 0, @as(i32, @intCast(height)) - 1))) * stride + @as(usize, @intCast(std.math.clamp(x, 0, @as(i32, @intCast(stride)) - 1))));
}
fn clip(v: i64, bit_depth: u8) i64 {
    return std.math.clamp(v, 0, (@as(i64, 1) << @as(u6, @intCast(bit_depth))) - 1);
}
fn six(plane: anytype, stride: usize, x: i32, y: i32, vertical: bool) i64 {
    const taps = [_]i64{ 1, -5, 20, 20, -5, 1 };
    var result: i64 = 0;
    for (taps, 0..) |weight, i| result += weight * sample(plane, stride, x + (if (vertical) @as(i32, 0) else @as(i32, @intCast(i)) - 2), y + (if (vertical) @as(i32, @intCast(i)) - 2 else 0));
    return result;
}
fn diagonal(plane: anytype, stride: usize, x: i32, y: i32, bit_depth: u8) i64 {
    const taps = [_]i64{ 1, -5, 20, 20, -5, 1 };
    var result: i64 = 0;
    for (taps, 0..) |weight, i| result += weight * six(plane, stride, x, y + @as(i32, @intCast(i)) - 2, false);
    return clip((result + 512) >> 10, bit_depth);
}
fn luma(plane: anytype, stride: usize, x: i32, y: i32, dx: i32, dy: i32, bit_depth: u8) layout.Sample(@TypeOf(plane)) {
    if (dx == 0 and dy == 0) return @intCast(sample(plane, stride, x, y));
    const horizontal = clip((six(plane, stride, x, y, false) + 16) >> 5, bit_depth);
    const vertical = clip((six(plane, stride, x, y, true) + 16) >> 5, bit_depth);
    const center = if (dx != 0 and dy != 0 and (dx == 2 or dy == 2)) diagonal(plane, stride, x, y, bit_depth) else 0;
    const value = if (dy == 0) (if (dx == 2) horizontal else (horizontal + sample(plane, stride, x + @intFromBool(dx == 3), y) + 1) >> 1) else if (dx == 0) (if (dy == 2) vertical else (vertical + sample(plane, stride, x, y + @intFromBool(dy == 3)) + 1) >> 1) else if (dx == 2) (if (dy == 2) center else (center + clip((six(plane, stride, x, y + @intFromBool(dy == 3), false) + 16) >> 5, bit_depth) + 1) >> 1) else if (dy == 2) (center + clip((six(plane, stride, x + @intFromBool(dx == 3), y, true) + 16) >> 5, bit_depth) + 1) >> 1 else (clip((six(plane, stride, x, y + @intFromBool(dy == 3), false) + 16) >> 5, bit_depth) + clip((six(plane, stride, x + @intFromBool(dx == 3), y, true) + 16) >> 5, bit_depth) + 1) >> 1;
    return @intCast(value);
}
pub fn predict(output: [3][]u8, reference: [3][]const u8, width: usize, x: usize, y: usize, part_width: usize, part_height: usize, mv: Vector) void {
    for (0..3) |p| {
        const chroma = p != 0;
        const divisor: usize = if (chroma) 2 else 1;
        const precision: i32 = if (chroma) 8 else 4;
        const stride = width / divisor;
        _ = precision;
        for (0..part_height / divisor) |row| for (0..part_width / divisor) |column| {
            const px = x / divisor + column;
            const py = y / divisor + row;
            output[p][py * stride + px] = pixel(reference[p], stride, px, py, mv, chroma, 8);
        };
    }
}
pub fn pixel(plane: anytype, stride: usize, x: usize, y: usize, mv: Vector, chroma: bool, bit_depth: u8) layout.Sample(@TypeOf(plane)) {
    const precision: i32 = if (chroma) 8 else 4;
    const sx = @as(i32, @intCast(x)) + @divFloor(mv.x, precision);
    const sy = @as(i32, @intCast(y)) + @divFloor(mv.y, precision);
    const dx = @mod(mv.x, precision);
    const dy = @mod(mv.y, precision);
    return if (!chroma) luma(plane, stride, sx, sy, dx, dy, bit_depth) else @intCast(((8 - dx) * (8 - dy) * sample(plane, stride, sx, sy) + dx * (8 - dy) * sample(plane, stride, sx + 1, sy) + (8 - dx) * dy * sample(plane, stride, sx, sy + 1) + dx * dy * sample(plane, stride, sx + 1, sy + 1) + 32) >> 6);
}
pub fn available(motions: []const Motion, stride: usize, x: i32, y: i32) ?Motion {
    if (x < 0 or y < 0 or x >= stride or y >= motions.len / stride) return null;
    const m = motions[@as(usize, @intCast(y)) * stride + @as(usize, @intCast(x))];
    return if (!m.decoded or m.reference == -2) null else m;
}
fn median(a: i32, b: i32, c: i32) i32 {
    return a + b + c - @min(a, @min(b, c)) - @max(a, @max(b, c));
}
pub fn predictor(motions: []const Motion, stride: usize, x: usize, y: usize, width: usize, height: usize, reference: i8, skipped: bool) Vector {
    const ix: i32 = @intCast(x);
    const iy: i32 = @intCast(y);
    const a = available(motions, stride, ix - 1, iy);
    const b = available(motions, stride, ix, iy - 1);
    const c = available(motions, stride, ix + @as(i32, @intCast(width)), iy - 1) orelse available(motions, stride, ix - 1, iy - 1);
    if (skipped and (a == null or b == null or (a.?.reference == 0 and a.?.vector.x == 0 and a.?.vector.y == 0) or (b.?.reference == 0 and b.?.vector.x == 0 and b.?.vector.y == 0))) return .{};
    if (width == 4 and height == 2) {
        const preferred = if (y % 4 == 0) b else a;
        if (preferred) |m| if (m.reference == reference) return m.vector;
    }
    if (width == 2 and height == 4) {
        const preferred = if (x % 4 == 0) a else c;
        if (preferred) |m| if (m.reference == reference) return m.vector;
    }
    if (b == null and c == null) return if (a) |m| m.vector else .{};
    var matched: usize = 0;
    var candidate = Vector{};
    for ([_]?Motion{ a, b, c }) |neighbor| if (neighbor) |m| {
        if (m.reference == reference) {
            matched += 1;
            candidate = m.vector;
        }
    };
    if (matched == 1) return candidate;
    const av = if (a) |m| m.vector else Vector{};
    const bv = if (b) |m| m.vector else Vector{};
    const cv = if (c) |m| m.vector else Vector{};
    return .{ .x = median(av.x, bv.x, cv.x), .y = median(av.y, bv.y, cv.y) };
}
