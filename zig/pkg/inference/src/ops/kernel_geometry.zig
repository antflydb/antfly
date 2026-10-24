// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

pub const BatchedDotGeneralPlan = struct {
    batch_count: usize,
    m: usize,
    n: usize,
    k: usize,
    lhs_contract_last: bool,
    rhs_contract_last: bool,
};

pub fn planBatchedDotGeneral(
    lhs_shape: []const i64,
    rhs_shape: []const i64,
    lhs_contracting: []const u8,
    rhs_contracting: []const u8,
    lhs_batch: []const u8,
    rhs_batch: []const u8,
) !BatchedDotGeneralPlan {
    if (lhs_contracting.len != 1 or rhs_contracting.len != 1) return error.UnsupportedShape;
    if (lhs_batch.len == 0 or lhs_batch.len != rhs_batch.len) return error.UnsupportedShape;
    if (lhs_shape.len != rhs_shape.len or lhs_shape.len != lhs_batch.len + 2 or lhs_shape.len > 8) {
        return error.UnsupportedShape;
    }

    const rank = lhs_shape.len;
    const matrix_axis0 = rank - 2;
    const matrix_axis1 = rank - 1;
    const lhs_contract_axis: usize = lhs_contracting[0];
    const rhs_contract_axis: usize = rhs_contracting[0];
    if ((lhs_contract_axis != matrix_axis0 and lhs_contract_axis != matrix_axis1) or
        (rhs_contract_axis != matrix_axis0 and rhs_contract_axis != matrix_axis1))
    {
        return error.UnsupportedShape;
    }
    const lhs_free_axis = if (lhs_contract_axis == matrix_axis0) matrix_axis1 else matrix_axis0;
    const rhs_free_axis = if (rhs_contract_axis == matrix_axis0) matrix_axis1 else matrix_axis0;

    var batch_count: usize = 1;
    for (lhs_batch, 0..) |lhs_axis, idx| {
        const rhs_axis = rhs_batch[idx];
        if (lhs_axis != idx or rhs_axis != idx) return error.UnsupportedShape;
        const batch_dim = lhs_shape[idx];
        if (batch_dim <= 0 or rhs_shape[idx] != batch_dim) return error.InvalidShape;
        batch_count = try std.math.mul(usize, batch_count, @intCast(batch_dim));
    }

    const m_i64 = lhs_shape[lhs_free_axis];
    const k_i64 = lhs_shape[lhs_contract_axis];
    const rhs_k_i64 = rhs_shape[rhs_contract_axis];
    const n_i64 = rhs_shape[rhs_free_axis];
    if (m_i64 <= 0 or k_i64 <= 0 or n_i64 <= 0 or rhs_k_i64 != k_i64) return error.InvalidShape;
    return .{
        .batch_count = batch_count,
        .m = @intCast(m_i64),
        .n = @intCast(n_i64),
        .k = @intCast(k_i64),
        .lhs_contract_last = lhs_contract_axis == matrix_axis1,
        .rhs_contract_last = rhs_contract_axis == matrix_axis1,
    };
}

pub const ReductionMode = enum(u32) { sum = 0, max = 1, mean = 2 };
/// Canonical keep-dimensions geometry shared by validation and launchers.
/// Duplicate axes are a set, consistently across CPU and accelerator targets.
pub const ReductionGeometry = struct {
    dims: [8]u32 = @splat(1),
    output_dims: [8]i64 = @splat(1),
    rank: u8,
    mask: u32 = 0,
    input_count: usize = 1,
    output_count: usize = 1,
    reduce_count: usize = 1,
    pub fn init(shape: []const i64, axes: []const u8) !ReductionGeometry {
        if (shape.len > 8) return error.InvalidShape;
        var result = ReductionGeometry{ .rank = @intCast(shape.len) };
        for (shape, 0..) |dim, i| {
            if (dim <= 0 or dim > std.math.maxInt(i32)) return error.InvalidShape;
            result.dims[i] = @intCast(dim);
            result.input_count = try std.math.mul(usize, result.input_count, @intCast(dim));
        }
        for (axes) |axis| {
            if (axis >= shape.len) return error.InvalidShape;
            result.mask |= @as(u32, 1) << @intCast(axis);
        }
        for (shape, 0..) |dim, i| {
            if (result.reduced(i)) {
                result.reduce_count = try std.math.mul(usize, result.reduce_count, @intCast(dim));
            } else {
                result.output_dims[i] = dim;
                result.output_count = try std.math.mul(usize, result.output_count, @intCast(dim));
            }
        }
        return result;
    }
    pub fn reduced(self: ReductionGeometry, axis: usize) bool {
        return (self.mask & (@as(u32, 1) << @intCast(axis))) != 0;
    }
};

/// Dense rank-two or leading-batch matrix contraction. Target kernels choose
/// their own precision, tiling and GEMM implementations from this geometry.
pub fn planDenseDotGeneral(lhs: []const i64, rhs: []const i64, lc: []const u8, rc: []const u8, lb: []const u8, rb: []const u8) !BatchedDotGeneralPlan {
    if (lb.len != 0 or rb.len != 0) return planBatchedDotGeneral(lhs, rhs, lc, rc, lb, rb);
    if (lhs.len != 2 or rhs.len != 2 or lc.len != 1 or rc.len != 1 or lc[0] > 1 or rc[0] > 1) return error.UnsupportedShape;
    for (lhs) |dim| if (dim <= 0) return error.InvalidShape;
    for (rhs) |dim| if (dim <= 0) return error.InvalidShape;
    if (lhs[lc[0]] != rhs[rc[0]]) return error.InvalidShape;
    return .{ .batch_count = 1, .m = @intCast(lhs[1 - lc[0]]), .n = @intCast(rhs[1 - rc[0]]), .k = @intCast(lhs[lc[0]]), .lhs_contract_last = lc[0] == 1, .rhs_contract_last = rc[0] == 1 };
}

/// Dense row geometry; empty row sets are legal, zero width is not.
pub const DenseRows = struct {
    rows: usize,
    width: usize,
    pub inline fn init(elements: usize, width: usize) error{InvalidShape}!DenseRows {
        if (width == 0 or elements % width != 0) return error.InvalidShape;
        return .{ .rows = elements / width, .width = width };
    }
};

/// Validate element regions before any copy or device submission. Keep the
/// original product-check order so callers can preserve their error mapping.
pub const RowCopy = struct {
    destination: usize,
    source: usize,
    elements: usize,
    pub inline fn init(dst_elements: usize, src_elements: usize, dst_row: usize, src_row: usize, rows: usize, cols: usize) !RowCopy {
        const dst = try std.math.mul(usize, dst_row, cols);
        const src = try std.math.mul(usize, src_row, cols);
        const count = try std.math.mul(usize, rows, cols);
        if (cols == 0 or dst_elements % cols != 0 or src_elements % cols != 0 or
            dst > dst_elements or count > dst_elements - dst or
            src > src_elements or count > src_elements - src) return error.InvalidShape;
        return .{ .destination = dst, .source = src, .elements = count };
    }
};

/// Symmetric padding and unit dilation; native schedules retain their own
/// indexing, grouping and workspace policy.
pub inline fn convolutionOutput(input: usize, kernel: usize, stride: usize, padding: usize) !usize {
    if (kernel == 0 or stride == 0) return error.InvalidShape;
    const padded = try std.math.add(usize, input, try std.math.mul(usize, padding, 2));
    if (kernel > padded) return error.InvalidShape;
    return (padded - kernel) / stride + 1;
}

test "shared row and convolution geometry preserve empty bounds and overflow" {
    const empty = try DenseRows.init(0, 4);
    try std.testing.expectEqual(@as(usize, 0), empty.rows);
    try std.testing.expectError(error.InvalidShape, DenseRows.init(0, 0));
    try std.testing.expectError(error.InvalidShape, DenseRows.init(7, 4));
    const copy = try RowCopy.init(24, 16, 3, 1, 2, 4);
    try std.testing.expectEqual(@as(usize, 12), copy.destination);
    try std.testing.expectEqual(@as(usize, 4), copy.source);
    try std.testing.expectEqual(@as(usize, 8), copy.elements);
    _ = try RowCopy.init(24, 16, 6, 4, 0, 4);
    try std.testing.expectError(error.InvalidShape, RowCopy.init(24, 16, 6, 4, 1, 4));
    try std.testing.expectError(error.InvalidShape, RowCopy.init(24, 15, 0, 0, 1, 4));
    try std.testing.expectError(error.Overflow, RowCopy.init(0, 0, 0, std.math.maxInt(usize), 0, 2));
    try std.testing.expectEqual(@as(usize, 4), try convolutionOutput(7, 3, 2, 1));
    try std.testing.expectEqual(@as(usize, 1), try convolutionOutput(1, 1, 1, 0));
    try std.testing.expectError(error.InvalidShape, convolutionOutput(1, 3, 1, 0));
    try std.testing.expectError(error.InvalidShape, convolutionOutput(1, 1, 0, 0));
    try std.testing.expectError(error.Overflow, convolutionOutput(1, 1, 1, std.math.maxInt(usize)));
}
