// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Compact symmetric-window semantics for captured programs. Only the position
//! vector is a literal; query/key grids and shared heads stay graph operations.
const std = @import("std");
const ops = @import("../ops/ops.zig");

pub fn symmetricWindow(cb: *const ops.ComputeBackend, a: std.mem.Allocator, sequence: usize, heads: usize, half_window: usize) !ops.CT {
    if (sequence == 0 or heads == 0) return error.InvalidAttentionShape;
    const size = std.math.cast(i32, sequence) orelse return error.UnsupportedShape;
    _ = try std.math.mul(usize, heads, try std.math.mul(usize, sequence, sequence));
    const positions = try a.alloc(i32, sequence);
    defer a.free(positions);
    for (positions, 0..) |*value, i| value.* = @intCast(i);
    const ids = (try cb.fromInt32Shape(positions, &.{size})) orelse return error.UnsupportedPrimitiveOp;
    defer cb.free(ids);
    const square = [_]i64{ size, size };
    const query = try cb.primBroadcastInDim(ids, &square, &.{0}, &.{size});
    defer cb.free(query);
    const key = try cb.primBroadcastInDim(ids, &square, &.{1}, &.{size});
    defer cb.free(key);
    const delta = try cb.primSubtract(query, key);
    defer cb.free(delta);
    const distance = try cb.primAbs(delta);
    defer cb.free(distance);
    const limit: i32 = @intCast(@min(half_window, sequence - 1) + 1);
    const threshold = (try cb.fromInt32Shape(&.{limit}, &.{})) orelse return error.UnsupportedPrimitiveOp;
    defer cb.free(threshold);
    const upper = try cb.primBroadcastInDim(threshold, &square, &.{}, &.{});
    defer cb.free(upper);
    const visible = try cb.primLessThan(distance, upper);
    defer cb.free(visible);
    const zero = try cb.fromFloat32Shape(&.{0}, &.{});
    defer cb.free(zero);
    const negative = try cb.fromFloat32Shape(&.{-std.math.inf(f32)}, &.{});
    defer cb.free(negative);
    const bias = try cb.primWhereSelect(visible, zero, negative);
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
    try std.testing.expect(trace.graph.constant_pool.items.len <= 2048 * @sizeOf(i32) + 128);
    try std.testing.expect(trace.graph.nodeCount() < 32);
}
