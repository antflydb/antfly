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

//! Shared CPU-evaluation primitives used by `const_fold.zig` (compile-
//! time constant folding) and `grad_check.zig` (numerical-gradient
//! eval). Both modules previously kept their own copy of the same
//! stride / unravel / broadcast / reduce code, and a real bug in the
//! grad_check copy (broadcast falling back to `i % a.len`) hid behind
//! that duplication for several months. Centralising here keeps the
//! two paths in lock-step.

const std = @import("std");
const node_mod = @import("node.zig");
const shape_mod = @import("shape.zig");

const Shape = shape_mod.Shape;
const max_rank = shape_mod.max_rank;

pub const ReduceKind = enum { sum, max, mean };

pub const Strides = [max_rank]i64;
pub const Coords = [max_rank]i64;

/// Row-major strides for a static-shape tensor of rank `rank`.
pub fn computeStrides(dims: [max_rank]i64, rank: u8) Strides {
    var strides: Strides = @splat(0);
    var s: i64 = 1;
    var k: usize = rank;
    while (k > 0) {
        k -= 1;
        strides[k] = s;
        s *= dims[k];
    }
    return strides;
}

/// Unravel a flat row-major index into per-axis coordinates.
pub fn unravelIdx(flat: usize, dims: [max_rank]i64, rank: u8) Coords {
    var coords: Coords = @splat(0);
    var rem: i64 = @intCast(flat);
    var k: usize = rank;
    while (k > 0) {
        k -= 1;
        const d = dims[k];
        if (d > 0) {
            coords[k] = @rem(rem, d);
            rem = @divFloor(rem, d);
        } else {
            coords[k] = 0;
        }
    }
    return coords;
}

/// Total element count for a fully-static shape, or null if any dim
/// is dynamic.
pub fn staticElements(shape: Shape) ?usize {
    const n = shape.numElements() orelse return null;
    if (n < 0) return null;
    return @intCast(n);
}

/// Evaluate a `broadcast_in_dim` op given typed input data. Handles
/// both the unmapped (right-aligned NumPy) and the explicit
/// `broadcast_axes` form. Returns null when shapes are inconsistent
/// or when an axis with input dim != 1 mismatches the output dim
/// — the caller is expected to leave the original node untouched in
/// that case.
pub fn evalBroadcast(
    comptime T: type,
    allocator: std.mem.Allocator,
    input: []const T,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.BroadcastAttrs,
) !?[]T {
    const out_rank = out_shape.rank();
    if (out_rank == 0) return null;
    const num_elements = staticElements(out_shape) orelse return null;
    const in_elements = staticElements(in_shape) orelse return null;
    if (input.len != in_elements) return null;

    const in_rank = in_shape.rank();
    const in_strides = computeStrides(in_shape.dims, in_rank);

    var eff_strides: Strides = @splat(0);
    if (in_rank == 0) {
        // Scalar input: every output position reads input[0].
    } else if (attrs.num_axes == 0) {
        if (in_rank > out_rank) return null;
        const offset = out_rank - in_rank;
        for (0..in_rank) |i| {
            const out_axis = offset + i;
            const in_dim = in_shape.dims[i];
            const out_dim = out_shape.dims[out_axis];
            if (in_dim == out_dim) {
                eff_strides[out_axis] = in_strides[i];
            } else if (in_dim == 1) {
                eff_strides[out_axis] = 0;
            } else {
                return null;
            }
        }
    } else {
        if (attrs.num_axes != in_rank) return null;
        for (0..in_rank) |i| {
            const out_axis = attrs.broadcast_axes[i];
            if (out_axis >= out_rank) return null;
            const in_dim = in_shape.dims[i];
            const out_dim = out_shape.dims[out_axis];
            if (in_dim == out_dim) {
                eff_strides[out_axis] = in_strides[i];
            } else if (in_dim == 1) {
                eff_strides[out_axis] = 0;
            } else {
                return null;
            }
        }
    }

    const result = try allocator.alloc(T, num_elements);
    errdefer allocator.free(result);
    for (0..num_elements) |out_idx| {
        const c = unravelIdx(out_idx, out_shape.dims, out_rank);
        var src: i64 = 0;
        for (0..out_rank) |k| src += c[k] * eff_strides[k];
        result[out_idx] = input[@intCast(src)];
    }
    return result;
}

/// Evaluate a `transpose` op given typed input data.
pub fn evalTranspose(
    comptime T: type,
    allocator: std.mem.Allocator,
    input: []const T,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.TransposeAttrs,
) !?[]T {
    const rank = in_shape.rank();
    if (rank != out_shape.rank()) return null;
    if (attrs.num_axes != rank) return null;
    const num_elements = staticElements(out_shape) orelse return null;
    if (input.len != num_elements) return null;

    const in_strides = computeStrides(in_shape.dims, rank);

    const result = try allocator.alloc(T, num_elements);
    errdefer allocator.free(result);

    for (0..num_elements) |out_idx| {
        const out_coords = unravelIdx(out_idx, out_shape.dims, rank);
        var src: i64 = 0;
        // Output axis k corresponds to input axis perm[k].
        for (0..rank) |k| src += out_coords[k] * in_strides[attrs.perm[k]];
        result[out_idx] = input[@intCast(src)];
    }
    return result;
}

/// Evaluate a `slice` op given typed input data. Returns null if any
/// computed input index is out of range — the caller should leave the
/// node alone in that case.
pub fn evalSlice(
    comptime T: type,
    allocator: std.mem.Allocator,
    input: []const T,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.SliceAttrs,
) !?[]T {
    const rank = in_shape.rank();
    if (rank != out_shape.rank()) return null;
    if (attrs.num_axes != rank) return null;
    const num_elements = staticElements(out_shape) orelse return null;
    const in_elements = staticElements(in_shape) orelse return null;
    if (input.len != in_elements) return null;

    const in_strides = computeStrides(in_shape.dims, rank);

    const result = try allocator.alloc(T, num_elements);
    errdefer allocator.free(result);
    for (0..num_elements) |out_idx| {
        const c = unravelIdx(out_idx, out_shape.dims, rank);
        var src: i64 = 0;
        for (0..rank) |k| {
            const v = attrs.starts[k] + c[k] * attrs.strides[k];
            if (v < 0 or v >= in_shape.dims[k]) return null;
            src += v * in_strides[k];
        }
        result[out_idx] = input[@intCast(src)];
    }
    return result;
}

/// Evaluate a binary `concat_prim` op (the IR's concat is binary; chain
/// for n-ary).
pub fn evalConcat(
    comptime T: type,
    allocator: std.mem.Allocator,
    a_data: []const T,
    a_shape: Shape,
    b_data: []const T,
    b_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.ConcatAttrs,
) !?[]T {
    const rank = a_shape.rank();
    if (rank != b_shape.rank() or rank != out_shape.rank()) return null;
    const axis = attrs.axis;
    if (axis >= rank) return null;
    const num_elements = staticElements(out_shape) orelse return null;
    const a_elements = staticElements(a_shape) orelse return null;
    const b_elements = staticElements(b_shape) orelse return null;
    if (a_data.len != a_elements) return null;
    if (b_data.len != b_elements) return null;

    for (0..rank) |k| {
        if (k == axis) continue;
        if (a_shape.dims[k] != b_shape.dims[k]) return null;
        if (a_shape.dims[k] != out_shape.dims[k]) return null;
    }
    if (a_shape.dims[axis] + b_shape.dims[axis] != out_shape.dims[axis]) return null;

    const a_strides = computeStrides(a_shape.dims, rank);
    const b_strides = computeStrides(b_shape.dims, rank);

    const result = try allocator.alloc(T, num_elements);
    errdefer allocator.free(result);

    const a_axis_dim = a_shape.dims[axis];

    for (0..num_elements) |out_idx| {
        var c = unravelIdx(out_idx, out_shape.dims, rank);
        const from_a = c[axis] < a_axis_dim;
        if (!from_a) c[axis] -= a_axis_dim;
        const strides = if (from_a) a_strides else b_strides;
        var src: i64 = 0;
        for (0..rank) |k| src += c[k] * strides[k];
        result[out_idx] = if (from_a) a_data[@intCast(src)] else b_data[@intCast(src)];
    }
    return result;
}

/// Evaluate a `reduce_sum` / `reduce_max` / `reduce_mean` op. Output
/// shape preserves the input rank with reduced axes set to 1
/// (matches `Builder.reduceOp`'s convention).
pub fn evalReduce(
    comptime T: type,
    allocator: std.mem.Allocator,
    input: []const T,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.ReduceAttrs,
    kind: ReduceKind,
) !?[]T {
    const rank = in_shape.rank();
    if (rank != out_shape.rank()) return null;
    const num_elements = staticElements(out_shape) orelse return null;
    const in_elements = staticElements(in_shape) orelse return null;
    if (input.len != in_elements) return null;

    var is_reduced: [max_rank]bool = @splat(false);
    var reduce_count: i64 = 1;
    for (0..attrs.num_axes) |i| {
        const ax = attrs.axes[i];
        if (ax >= rank) return null;
        if (is_reduced[ax]) return null;
        is_reduced[ax] = true;
        reduce_count *= in_shape.dims[ax];
        if (out_shape.dims[ax] != 1) return null;
    }
    if (reduce_count <= 0) return null;

    const in_strides = computeStrides(in_shape.dims, rank);

    const result = try allocator.alloc(T, num_elements);
    errdefer allocator.free(result);

    for (0..num_elements) |out_idx| {
        const c = unravelIdx(out_idx, out_shape.dims, rank);

        var coord_iter: Coords = c;
        for (0..rank) |k| if (is_reduced[k]) {
            coord_iter[k] = 0;
        };

        const minus_inf = if (@typeInfo(T) == .float) -std.math.inf(T) else std.math.minInt(T);
        var acc: T = switch (kind) {
            .sum, .mean => 0,
            .max => minus_inf,
        };
        var done = false;
        while (!done) {
            var src: i64 = 0;
            for (0..rank) |k| src += coord_iter[k] * in_strides[k];
            const v = input[@intCast(src)];
            switch (kind) {
                .sum, .mean => acc += v,
                .max => if (v > acc) {
                    acc = v;
                },
            }

            done = true;
            var k: usize = rank;
            while (k > 0) {
                k -= 1;
                if (!is_reduced[k]) continue;
                coord_iter[k] += 1;
                if (coord_iter[k] < in_shape.dims[k]) {
                    done = false;
                    break;
                }
                coord_iter[k] = 0;
            }
        }

        if (kind == .mean) {
            if (@typeInfo(T) == .float) {
                acc /= @floatFromInt(reduce_count);
            } else if (reduce_count > 0) {
                acc = @divTrunc(acc, @as(T, @intCast(reduce_count)));
            }
        }
        result[out_idx] = acc;
    }
    return result;
}
