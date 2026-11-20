// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Compact symmetric-window semantics for captured programs. Only the position
//! and bound vectors are literals; grids and shared heads stay graph operations.
const std = @import("std");
const ops = @import("../ops/ops.zig");

pub fn symmetricWindow(cb: *const ops.ComputeBackend, a: std.mem.Allocator, sequence: usize, heads: usize, half_window: usize) !ops.CT {
    if (sequence == 0 or heads == 0) return error.InvalidAttentionShape;
    const size = std.math.cast(i32, sequence) orelse return error.UnsupportedShape;
    _ = try std.math.mul(usize, heads, try std.math.mul(usize, sequence, sequence));
    const positions = try a.alloc(i32, try std.math.mul(usize, sequence, 3));
    defer a.free(positions);
    const columns = positions[0..sequence];
    const lower = positions[sequence..][0..sequence];
    const upper = positions[2 * sequence ..];
    const half = @min(half_window, sequence - 1);
    for (columns, lower, upper, 0..) |*column, *lo, *hi, i| {
        column.* = @intCast(i);
        lo.* = if (i > half) @as(i32, @intCast(i - half)) - 1 else -1;
        hi.* = @intCast(@min(sequence, i + half + 1));
    }
    // Comparisons are exact on every executor. Do not introduce integer abs
    // or subtraction merely to construct metadata; eager unary programs have
    // a floating-point contract even when a provider accepts integer HLO.
    const ids = (try cb.fromInt32Shape(columns, &.{size})) orelse return error.UnsupportedPrimitiveOp;
    defer cb.free(ids);
    const lower_ids = (try cb.fromInt32Shape(lower, &.{size})) orelse return error.UnsupportedPrimitiveOp;
    defer cb.free(lower_ids);
    const upper_ids = (try cb.fromInt32Shape(upper, &.{size})) orelse return error.UnsupportedPrimitiveOp;
    defer cb.free(upper_ids);
    const square = [_]i64{ size, size };
    const key = try cb.primBroadcastInDim(ids, &square, &.{1}, &.{size});
    defer cb.free(key);
    const low = try cb.primBroadcastInDim(lower_ids, &square, &.{0}, &.{size});
    defer cb.free(low);
    const high = try cb.primBroadcastInDim(upper_ids, &square, &.{0}, &.{size});
    defer cb.free(high);
    const above_lower = try cb.primLessThan(low, key);
    defer cb.free(above_lower);
    const below_upper = try cb.primLessThan(key, high);
    defer cb.free(below_upper);
    const zero = try cb.fromFloat32Shape(&.{0}, &.{});
    defer cb.free(zero);
    const negative = try cb.fromFloat32Shape(&.{-std.math.inf(f32)}, &.{});
    defer cb.free(negative);
    const bounded = try cb.primWhereSelect(below_upper, zero, negative);
    defer cb.free(bounded);
    const bias = try cb.primWhereSelect(above_lower, bounded, negative);
    defer cb.free(bias);
    return cb.primBroadcastInDim(bias, &.{ @intCast(heads), size, size }, &.{ 1, 2 }, &square);
}

test "captured symmetric window keeps large masks out of literal storage" {
    const a = std.testing.allocator;
    var trace = @import("tracing_compute.zig").TracingCompute.init(a);
    defer trace.deinit();
    const cb = trace.backend();
    const mask = try symmetricWindow(&cb, a, 2048, 28, 64);
    defer cb.free(mask);
    try std.testing.expect(trace.graph.constant_pool.items.len <= 3 * 2048 * @sizeOf(i32) + 128);
    try std.testing.expect(trace.graph.nodeCount() < 32);
}
