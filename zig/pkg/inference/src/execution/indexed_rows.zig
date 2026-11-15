// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Symbolic indexing for batch-local row positions. Eager callers retain their
//! tuned gather schedules; capture keeps index values as exact typed operands.
const std = @import("std");
const ops = @import("../ops/ops.zig");

fn integer(cb: *const ops.ComputeBackend, values: []const i64, shape: []const i64) !ops.CT {
    return (try cb.fromConstantBytes(std.mem.sliceAsBytes(values), .i64, shape)) orelse error.UnsupportedTypedConstant;
}

pub fn active(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, valid: []const i64, batch: usize, columns: usize) !ops.CT {
    if (valid.len != try std.math.mul(usize, batch, columns)) return error.InvalidInputShape;
    for (valid) |bit| if (bit != 0 and bit != 1) return error.InvalidInputShape;
    if (cb.kind() != .graph) {
        const values = try allocator.alloc(f32, valid.len);
        defer allocator.free(values);
        for (valid, values) |bit, *value| value.* = @floatFromInt(bit);
        const mask = try cb.fromFloat32Shape(values, &.{ @intCast(batch), @intCast(columns) });
        defer cb.free(mask);
        const zero = try cb.fromFloat32Shape(&.{0}, &.{ 1, 1 });
        defer cb.free(zero);
        const zeros = try cb.primBroadcastInDim(zero, &.{ @intCast(batch), @intCast(columns) }, &.{ 0, 1 }, &.{ 1, 1 });
        defer cb.free(zeros);
        return cb.primLessThan(zeros, mask);
    }
    const shape = [_]i64{ @intCast(batch), @intCast(columns) };
    const zero = try integer(cb, &.{0}, &.{ 1, 1 });
    defer cb.free(zero);
    const zeros = try cb.primBroadcastInDim(zero, &shape, &.{ 0, 1 }, &.{ 1, 1 });
    defer cb.free(zeros);
    const mask = try integer(cb, valid, &shape);
    defer cb.free(mask);
    return cb.primLessThan(zeros, mask);
}

pub fn gatherBatchLocal(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, hidden: ops.CT, positions: []const i64, valid: []const i64, batch: usize, sequence: usize, columns: usize, width: usize) !ops.CT {
    if (cb.kind() != .graph) return error.UnsupportedTypedIndexFrontend;
    const rows = try std.math.mul(usize, batch, columns);
    if (batch == 0 or sequence == 0 or columns == 0 or width == 0 or positions.len != rows or valid.len != rows) return error.InvalidInputShape;
    for (positions, valid) |position, bit| {
        if (bit != 0 and bit != 1) return error.InvalidInputShape;
        if (bit == 1 and (position < 0 or position >= sequence)) return error.InvalidInputShape;
    }
    const shape = [_]i64{ @intCast(batch), @intCast(columns) };
    const indices = try integer(cb, positions, &shape);
    defer cb.free(indices);
    const zero = try integer(cb, &.{0}, &.{ 1, 1 });
    defer cb.free(zero);
    const zeros = try cb.primBroadcastInDim(zero, &shape, &.{ 0, 1 }, &.{ 1, 1 });
    defer cb.free(zeros);
    const enabled = try active(cb, allocator, valid, batch, columns);
    defer cb.free(enabled);
    const safe = try cb.primWhereSelect(enabled, indices, zeros);
    defer cb.free(safe);
    const offsets = try allocator.alloc(i64, batch);
    defer allocator.free(offsets);
    for (offsets, 0..) |*value, index| value.* = std.math.cast(i64, try std.math.mul(usize, index, sequence)) orelse return error.InvalidInputShape;
    const offset = try integer(cb, offsets, &.{ @intCast(batch), 1 });
    defer cb.free(offset);
    const expanded = try cb.primBroadcastInDim(offset, &shape, &.{ 0, 1 }, &.{ @intCast(batch), 1 });
    defer cb.free(expanded);
    const global = try cb.add(safe, expanded);
    defer cb.free(global);
    const flattened = try cb.primReshape(global, &.{@intCast(rows)});
    defer cb.free(flattened);
    return cb.primGather(hidden, flattened, 0, &.{ @intCast(try std.math.mul(usize, batch, sequence)), @intCast(width) });
}
