// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

const ops = @import("../ops/ops.zig");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

/// Normalize each row/vector along the last dimension while keeping the tensor
/// in the supplied backend. Zero vectors are left at zero, matching the host
/// linalg L2-normalization behavior.
pub fn l2NormalizeLastDim(
    allocator: std.mem.Allocator,
    backend: *const ComputeBackend,
    input: CT,
    input_shape: []const i64,
) !CT {
    if (input_shape.len == 0 or input_shape.len > 8) return error.UnsupportedShape;
    const last_axis = input_shape.len - 1;
    const dim = input_shape[last_axis];
    if (dim <= 0) return error.UnsupportedShape;

    var reduced_shape_buf: [8]i64 = undefined;
    var broadcast_axes_buf: [8]u8 = undefined;
    var const_shape_buf: [8]i32 = undefined;
    for (input_shape, 0..) |d, axis| {
        if (d <= 0) return error.UnsupportedShape;
        reduced_shape_buf[axis] = if (axis == last_axis) 1 else d;
        broadcast_axes_buf[axis] = @intCast(axis);
        const_shape_buf[axis] = std.math.cast(i32, reduced_shape_buf[axis]) orelse return error.UnsupportedShape;
    }
    const reduced_shape = reduced_shape_buf[0..input_shape.len];
    const broadcast_axes = broadcast_axes_buf[0..input_shape.len];
    const const_shape = const_shape_buf[0..input_shape.len];

    const reduced_elems = checkedElementCount(reduced_shape) orelse return error.UnsupportedShape;
    const eps_values = try allocator.alloc(f32, reduced_elems);
    defer allocator.free(eps_values);
    const one_values = try allocator.alloc(f32, reduced_elems);
    defer allocator.free(one_values);
    @memset(eps_values, std.math.floatMin(f32));
    @memset(one_values, 1.0);

    const eps = try backend.fromFloat32Shape(eps_values, const_shape);
    defer backend.free(eps);
    const one = try backend.fromFloat32Shape(one_values, const_shape);
    defer backend.free(one);

    const squared = try backend.multiply(input, input);
    defer backend.free(squared);
    const sum_sq = try backend.primReduceSum(squared, &.{@intCast(last_axis)}, input_shape);
    defer backend.free(sum_sq);
    const zeroish = try backend.primLessThan(sum_sq, eps);
    defer backend.free(zeroish);
    const inv_norm_raw = try backend.primRsqrt(sum_sq);
    defer backend.free(inv_norm_raw);
    const inv_norm = try backend.primWhereSelect(zeroish, one, inv_norm_raw);
    defer backend.free(inv_norm);
    const scale = try backend.primBroadcastInDim(inv_norm, input_shape, broadcast_axes, reduced_shape);
    defer backend.free(scale);
    return backend.multiply(input, scale);
}

fn checkedElementCount(shape: []const i64) ?usize {
    var total: usize = 1;
    for (shape) |dim| {
        if (dim <= 0) return null;
        const u: usize = std.math.cast(usize, dim) orelse return null;
        total = std.math.mul(usize, total, u) catch return null;
    }
    return total;
}
