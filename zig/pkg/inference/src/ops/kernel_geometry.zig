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

/// Physical dense-attention storage shared by CPU, Metal and CUDA adapters.
/// The values preserve the native Metal ABI. Unrecognized shapes remain an
/// explicit compatibility decision of the caller, rather than a model hint.
pub const DenseAttentionLayout = enum(u32) {
    head_major = 0,
    token_major = 1,

    pub fn fromShape(comptime Int: type, shape: []const Int, batch: usize, sequence: usize, heads: usize, dim: usize) ?DenseAttentionLayout {
        if (batch == 0 or sequence == 0 or heads == 0 or dim == 0) return null;
        const tokens = std.math.mul(usize, batch, sequence) catch return null;
        const hidden = std.math.mul(usize, heads, dim) catch return null;
        const batch_heads = std.math.mul(usize, batch, heads) catch return null;
        const b = std.math.cast(Int, batch) orelse return null;
        const n = std.math.cast(Int, sequence) orelse return null;
        const h = std.math.cast(Int, heads) orelse return null;
        const d = std.math.cast(Int, dim) orelse return null;
        if (shape.len == 2) {
            const rows = std.math.cast(Int, tokens) orelse return null;
            const width = std.math.cast(Int, hidden) orelse return null;
            if (shape[0] == rows and shape[1] == width) return .token_major;
        } else if (shape.len == 3) {
            const bh = std.math.cast(Int, batch_heads) orelse return null;
            if (shape[0] == bh and shape[1] == n and shape[2] == d) return .head_major;
        } else if (shape.len == 4) {
            if (shape[0] == b and shape[1] == h and shape[2] == n and shape[3] == d) return .head_major;
        }
        return null;
    }
};

test "dense attention physical layouts preserve integer widths and reject overflow" {
    const L = DenseAttentionLayout;
    try std.testing.expectEqual(L.token_major, L.fromShape(i64, &.{ 6, 14 }, 2, 3, 2, 7).?);
    try std.testing.expectEqual(L.head_major, L.fromShape(i32, &.{ 4, 3, 7 }, 2, 3, 2, 7).?);
    try std.testing.expectEqual(L.head_major, L.fromShape(i64, &.{ 2, 2, 3, 7 }, 2, 3, 2, 7).?);
    try std.testing.expect(L.fromShape(i32, &.{ 6, 15 }, 2, 3, 2, 7) == null);
    try std.testing.expect(L.fromShape(i32, &.{ 6, 14 }, 0, 3, 2, 7) == null);
    try std.testing.expect(L.fromShape(i32, &.{ 6, 14 }, std.math.maxInt(usize), 3, 2, 7) == null);
}

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

/// Resolve the shared target-shape convention after callers resolve their input.
/// Mapping legality, storage layout and dtype admission stay with each adapter.
/// Unmapped/nonpositive target extents become one; the first mapped input wins.
pub inline fn resolveBroadcastOutput(input: []const i64, target: []const i64, mapping: []const u8, output: []i64) !usize {
    return resolveBroadcastOutputBounded(i64, std.math.maxInt(i64), input, target, mapping, output);
}

/// Scalar-width admission belongs to the caller and occurs before each product.
pub inline fn resolveBroadcastOutputBounded(comptime Input: type, comptime max_dim: i64, input: []const Input, target: []const i64, mapping: []const u8, output: []i64) !usize {
    if (target.len > output.len) return error.InvalidShape;
    var count: usize = 1;
    for (target, 0..) |extent, axis| {
        var dim = extent;
        if (dim <= 0) {
            for (mapping[0..@min(mapping.len, input.len)], 0..) |mapped, in_axis| {
                if (mapped == axis) {
                    dim = @intCast(input[in_axis]);
                    break;
                }
            }
        }
        if (dim <= 0) dim = 1;
        if (dim > max_dim) return error.InvalidShape;
        output[axis] = dim;
        count = try std.math.mul(usize, count, std.math.cast(usize, dim) orelse return error.Overflow);
    }
    return count;
}

test "resolved broadcast geometry preserves scalar inferred tiled and bounded output" {
    var output: [8]i64 = undefined;
    try std.testing.expectEqual(@as(usize, 6), try resolveBroadcastOutput(&.{}, &.{ 2, 3 }, &.{}, &output));
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, output[0..2]);
    try std.testing.expectEqual(@as(usize, 6), try resolveBroadcastOutput(&.{ 2, 3 }, &.{ -1, 0, -1 }, &.{ 0, 2 }, &output));
    try std.testing.expectEqualSlices(i64, &.{ 2, 1, 3 }, output[0..3]);
    // Tiling validation stays with the caller; no implicit singleton rewrite.
    try std.testing.expectEqual(@as(usize, 12), try resolveBroadcastOutput(&.{ 2, 3 }, &.{ 2, 6 }, &.{ 0, 1 }, &output));
    try std.testing.expectEqualSlices(i64, &.{ 2, 6 }, output[0..2]);
    try std.testing.expectEqual(@as(usize, 1), try resolveBroadcastOutput(&.{}, &.{}, &.{}, &output));
    try std.testing.expectError(error.InvalidShape, resolveBroadcastOutput(&.{}, &.{ 1, 2 }, &.{}, output[0..1]));
    try std.testing.expectError(error.Overflow, resolveBroadcastOutput(&.{}, &.{ std.math.maxInt(i64), 3 }, &.{}, &output));
}

/// Permute an already resolved shape. Storage, strides and aliasing remain
/// backend decisions. Writing into caller scratch avoids a second shape owner.
pub inline fn transposeShape(input: []const i64, permutation: []const u8, output: []i64) !void {
    if (input.len > 8 or permutation.len != input.len or output.len < input.len) return error.InvalidShape;
    var seen: u8 = 0;
    for (permutation, 0..) |axis, i| {
        if (axis >= input.len) return error.InvalidShape;
        const bit = @as(u8, 1) << @intCast(axis);
        if (seen & bit != 0) return error.InvalidShape;
        seen |= bit;
        output[i] = input[axis];
    }
}

/// Positive-step slice normalization. Metal's historical host path permits
/// starts beyond the dimension; bounded CPU/CUDA maps clip both endpoints.
pub const SliceAxis = struct {
    start: usize,
    stride: usize,
    size: i64,
    pub inline fn init(comptime bounded: bool, dim: i64, raw_start: i64, raw_limit: i64, step: i64, symbolic: bool) !SliceAxis {
        if (dim < 0 or step <= 0) return error.UnsupportedShape;
        var start = raw_start;
        var limit = raw_limit;
        if (start < 0) start += dim;
        if (limit < 0) limit = if (symbolic and start == 0 and step == 1) dim else limit + dim;
        start = if (bounded) std.math.clamp(start, 0, dim) else @max(start, 0);
        limit = if (bounded) std.math.clamp(limit, 0, dim) else @max(start, @min(limit, dim));
        const size = if (limit <= start) 0 else @divTrunc(limit - start - 1, step) + 1;
        return .{ .start = std.math.cast(usize, start) orelse return error.Overflow, .stride = std.math.cast(usize, step) orelse return error.Overflow, .size = size };
    }
};

test "resolved transpose rejects duplicate axes and preserves rank zero and rank eight" {
    var output: [8]i64 = undefined;
    try transposeShape(&.{ 2, 3, 4 }, &.{ 2, 0, 1 }, &output);
    try std.testing.expectEqualSlices(i64, &.{ 4, 2, 3 }, output[0..3]);
    try transposeShape(&.{}, &.{}, &output);
    try transposeShape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 7, 6, 5, 4, 3, 2, 1, 0 }, &output);
    try std.testing.expectEqualSlices(i64, &.{ 8, 7, 6, 5, 4, 3, 2, 1 }, &output);
    try std.testing.expectError(error.InvalidShape, transposeShape(&.{ 2, 3 }, &.{ 1, 1 }, &output));
    try std.testing.expectError(error.InvalidShape, transposeShape(&.{2}, &.{1}, &output));
    try std.testing.expectError(error.InvalidShape, transposeShape(&.{2}, &.{}, &output));
    try std.testing.expectError(error.InvalidShape, transposeShape(&.{2}, &.{0}, output[0..0]));
}

test "positive slice axis preserves clipping symbolic limits and empty maps without rounding overflow" {
    try std.testing.expectEqual(SliceAxis{ .start = 1, .stride = 2, .size = 2 }, try SliceAxis.init(true, 5, -4, -1, 2, false));
    try std.testing.expectEqual(@as(i64, 5), (try SliceAxis.init(true, 5, 0, -1, 1, true)).size);
    try std.testing.expectEqual(@as(i64, 4), (try SliceAxis.init(true, 5, 0, -1, 1, false)).size);
    try std.testing.expectEqual(@as(usize, 5), (try SliceAxis.init(true, 5, 99, 100, 1, false)).start);
    try std.testing.expectEqual(@as(usize, 99), (try SliceAxis.init(false, 5, 99, 100, 1, false)).start);
    try std.testing.expectEqual(@as(i64, 0), (try SliceAxis.init(false, 5, 0, -99, 1, false)).size);
    try std.testing.expectEqual(@as(i64, 0), (try SliceAxis.init(true, 0, 0, 0, 1, false)).size);
    try std.testing.expectEqual(@as(i64, 1), (try SliceAxis.init(true, std.math.maxInt(i64), 0, std.math.maxInt(i64), std.math.maxInt(i64), false)).size);
    try std.testing.expectError(error.UnsupportedShape, SliceAxis.init(true, 3, 0, 3, 0, false));
}

/// Dense gather's three physical regions, independent of index dtype and
/// the backend's decision to execute on a device or materialize on the host.
pub const GatherAxis = struct {
    prefix: usize,
    extent: usize,
    suffix: usize,
    pub inline fn init(comptime max_extent: usize, input: []const i64, axis: usize) !GatherAxis {
        if (axis >= input.len) return error.InvalidShape;
        var prefix: usize = 1;
        for (input[0..axis]) |dim| {
            if (dim <= 0) return error.InvalidShape;
            prefix = try std.math.mul(usize, prefix, std.math.cast(usize, dim) orelse return error.Overflow);
        }
        const extent = std.math.cast(usize, input[axis]) orelse return error.InvalidShape;
        if (extent == 0 or extent > max_extent) return error.InvalidShape;
        var suffix: usize = 1;
        for (input[axis + 1 ..]) |dim| {
            if (dim <= 0) return error.InvalidShape;
            suffix = try std.math.mul(usize, suffix, std.math.cast(usize, dim) orelse return error.Overflow);
        }
        return .{ .prefix = prefix, .extent = extent, .suffix = suffix };
    }
    pub inline fn count(self: GatherAxis, indices: usize) !usize {
        return std.math.mul(usize, try std.math.mul(usize, self.prefix, indices), self.suffix);
    }
};

/// Insert the logical index shape at the selected input axis. Scalar index
/// normalization, concrete-dimension admission and gather-ND remain callers'
/// policies. The output storage belongs to the caller.
pub inline fn gatherShape(input: []const i64, indices: []const i64, axis: usize, output: []i64) !usize {
    if (axis >= input.len) return error.InvalidShape;
    const rank = try std.math.add(usize, input.len - 1, indices.len);
    if (rank > output.len) return error.InvalidShape;
    @memcpy(output[0..axis], input[0..axis]);
    @memcpy(output[axis..][0..indices.len], indices);
    @memcpy(output[axis + indices.len ..][0 .. input.len - axis - 1], input[axis + 1 ..]);
    return rank;
}

test "resolved gather geometry covers prefixes scalar indices rank bounds and overflow" {
    const geometry = try GatherAxis.init(std.math.maxInt(usize), &.{ 2, 3, 4 }, 1);
    try std.testing.expectEqual(GatherAxis{ .prefix = 2, .extent = 3, .suffix = 4 }, geometry);
    try std.testing.expectEqual(@as(usize, 16), try geometry.count(2));
    try std.testing.expectEqual(@as(usize, 0), try geometry.count(0));
    var output: [8]i64 = undefined;
    const rank = try gatherShape(&.{ 2, 3, 4 }, &.{ 5, 6 }, 1, &output);
    try std.testing.expectEqualSlices(i64, &.{ 2, 5, 6, 4 }, output[0..rank]);
    try std.testing.expectEqual(@as(usize, 2), try gatherShape(&.{ 2, 3, 4 }, &.{}, 1, &output));
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, output[0..2]);
    try std.testing.expectError(error.InvalidShape, gatherShape(&.{ 2, 3 }, &.{ 4, 5 }, 0, output[0..2]));
    try std.testing.expectError(error.InvalidShape, GatherAxis.init(2, &.{3}, 0));
    try std.testing.expectError(error.InvalidShape, GatherAxis.init(8, &.{0}, 0));
    try std.testing.expectError(error.InvalidShape, GatherAxis.init(8, &.{3}, 1));
    const huge = GatherAxis{ .prefix = std.math.maxInt(usize), .extent = 1, .suffix = 2 };
    try std.testing.expectError(error.Overflow, huge.count(1));
}

/// Resolved ordinary concat regions. Representation and imported/broadcast
/// fallback resolution remain caller policy; this record owns no storage.
pub const ConcatAxis = struct {
    outer: usize,
    inner: usize,
    a_axis: usize,
    b_axis: usize,

    pub inline fn init(a: []const i64, b: []const i64, axis: usize, output: []i64) !ConcatAxis {
        if (a.len == 0 or a.len > 8 or a.len != b.len or axis >= a.len or output.len < a.len) return error.InvalidShape;
        var result = ConcatAxis{ .outer = 1, .inner = 1, .a_axis = 0, .b_axis = 0 };
        for (a, b, 0..) |ad, bd, dim| {
            if (ad <= 0 or bd <= 0 or (dim != axis and ad != bd)) return error.InvalidShape;
            const extent = std.math.cast(usize, ad) orelse return error.Overflow;
            output[dim] = if (dim == axis) try std.math.add(i64, ad, bd) else ad;
            if (dim < axis) result.outer = try std.math.mul(usize, result.outer, extent);
            if (dim > axis) result.inner = try std.math.mul(usize, result.inner, extent);
        }
        result.a_axis = std.math.cast(usize, a[axis]) orelse return error.Overflow;
        result.b_axis = std.math.cast(usize, b[axis]) orelse return error.Overflow;
        return result;
    }
    pub inline fn aCount(self: ConcatAxis) !usize {
        return std.math.mul(usize, try std.math.mul(usize, self.outer, self.a_axis), self.inner);
    }
    pub inline fn bCount(self: ConcatAxis) !usize {
        return std.math.mul(usize, try std.math.mul(usize, self.outer, self.b_axis), self.inner);
    }
    pub inline fn count(self: ConcatAxis) !usize {
        return std.math.mul(usize, try std.math.mul(usize, self.outer, try std.math.add(usize, self.a_axis, self.b_axis)), self.inner);
    }
};

test "resolved concat regions reject incompatible axes and preserve checked output extents" {
    var output: [8]i64 = undefined;
    const axis = try ConcatAxis.init(&.{ 2, 3, 4 }, &.{ 2, 5, 4 }, 1, &output);
    try std.testing.expectEqualSlices(i64, &.{ 2, 8, 4 }, output[0..3]);
    try std.testing.expectEqual(@as(usize, 24), try axis.aCount());
    try std.testing.expectEqual(@as(usize, 40), try axis.bCount());
    try std.testing.expectEqual(@as(usize, 64), try axis.count());
    try std.testing.expectError(error.InvalidShape, ConcatAxis.init(&.{ 2, 3 }, &.{ 3, 4 }, 1, &output));
    try std.testing.expectError(error.InvalidShape, ConcatAxis.init(&.{1}, &.{1}, 1, &output));
    try std.testing.expectError(error.InvalidShape, ConcatAxis.init(&.{0}, &.{1}, 0, &output));
    try std.testing.expectError(error.Overflow, ConcatAxis.init(&.{std.math.maxInt(i64)}, &.{1}, 0, &output));
    const huge = ConcatAxis{ .outer = std.math.maxInt(usize), .inner = 2, .a_axis = 1, .b_axis = 1 };
    try std.testing.expectError(error.Overflow, huge.aCount());
    try std.testing.expectError(error.Overflow, huge.count());
}

pub const BroadcastExtentPolicy = enum { positive, nonnegative, symbolic, loose };

/// Fold one resolved trailing axis without changing each adapter's admission.
/// Symbolic CPU resolution permits matching unknown/empty dimensions; typed
/// selection permits empty dimensions; ordinary CUDA arithmetic requires > 0.
pub inline fn broadcastExtent(comptime policy: BroadcastExtentPolicy, a: i64, b: i64) !i64 {
    if (comptime policy == .loose) {
        if (a > 1 and b > 1 and a != b) return error.ShapeMismatch;
        if (a > 1) return a;
        if (b > 1) return b;
        return if (a <= 0 or b <= 0) -1 else 1;
    }
    if (policy == .positive and (a <= 0 or b <= 0)) return error.InvalidTensorShape;
    if (policy == .nonnegative and (a < 0 or b < 0)) return error.InvalidTensorShape;
    if (a != b and a != 1 and b != 1) return error.ShapeMismatch;
    return if (a == 1) b else a;
}

pub inline fn rightAlignedShape(comptime count: usize, comptime policy: BroadcastExtentPolicy, inputs: [count][]const i64, output: []i64) !usize {
    var rank: usize = 0;
    for (inputs) |shape| rank = @max(rank, shape.len);
    if (rank > 8 or rank > output.len) return error.InvalidTensorShape;
    var trailing: usize = 0;
    while (trailing < rank) : (trailing += 1) {
        var extent: i64 = 1;
        for (inputs) |shape| extent = try broadcastExtent(policy, extent, if (trailing < shape.len) shape[shape.len - 1 - trailing] else 1);
        output[rank - 1 - trailing] = extent;
    }
    return rank;
}

test "right aligned output geometry preserves scalar empty and symbolic policies" {
    var output: [8]i64 = undefined;
    const rank = try rightAlignedShape(3, .positive, .{ &.{ 2, 1 }, &.{3}, &.{1} }, &output);
    try std.testing.expectEqual(@as(usize, 2), rank);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, output[0..rank]);
    try std.testing.expectEqual(@as(usize, 0), try rightAlignedShape(2, .positive, .{ &.{}, &.{} }, &output));
    _ = try rightAlignedShape(2, .nonnegative, .{ &.{ 0, 3 }, &.{ 1, 3 } }, &output);
    try std.testing.expectEqualSlices(i64, &.{ 0, 3 }, output[0..2]);
    _ = try rightAlignedShape(2, .symbolic, .{ &.{ -1, 3 }, &.{1} }, &output);
    try std.testing.expectEqualSlices(i64, &.{ -1, 3 }, output[0..2]);
    try std.testing.expectError(error.InvalidTensorShape, rightAlignedShape(2, .positive, .{ &.{0}, &.{1} }, &output));
    try std.testing.expectError(error.ShapeMismatch, rightAlignedShape(2, .symbolic, .{ &.{-1}, &.{2} }, &output));
}

/// Subtraction avoids address-end overflow; callers use logical byte extents.
pub inline fn byteRangesOverlap(a: u64, a_bytes: usize, b: u64, b_bytes: usize) bool {
    if (a_bytes == 0 or b_bytes == 0) return false;
    return if (a <= b) b - a < a_bytes else a - b < b_bytes;
}

/// Imported graphs carry compatibility-specific reshape conventions. These are
/// compile-time metadata policies; storage, views and schedules stay at callers.
pub const ReshapeCompatibility = struct {
    rescale_leading_batch: bool = false,
    infer_batch_sequence: bool = false,
    preserve_mask_batch: bool = false,
    collapse_pair_first: bool = false,
    flatten_four_axes: bool = false,
};

pub fn inferReshapeShapeWithNumel(comptime compatibility: ReshapeCompatibility, input_shape: []const i64, target_shape: []const i64, input_numel: usize) ?[8]i64 {
    if (target_shape.len > 8) return null;
    var resolved: [8]i64 = undefined;
    var negative_count: usize = 0;
    for (target_shape, 0..) |dim, i| {
        resolved[i] = dim;
        if (dim < 0) negative_count += 1;
    }

    if (negative_count > 1 and target_shape.len == input_shape.len) {
        for (target_shape, 0..) |dim, i| {
            if (dim >= 0) continue;
            const input_dim = input_shape[i];
            if (input_dim > 0) resolved[i] = input_dim;
        }
    }
    if (comptime compatibility.rescale_leading_batch) {
        if (negative_count == 0 and input_shape.len > 0 and target_shape.len > 0 and input_shape[0] > 1 and target_shape[0] > 1 and target_shape.len != input_shape.len) {
            const target_numel = positiveElementCount(target_shape) orelse return null;
            if (target_numel > 0 and input_numel > target_numel and input_numel % target_numel == 0) {
                const batch_factor = input_numel / target_numel;
                const scaled = @as(usize, @intCast(target_shape[0])) * batch_factor;
                resolved[0] = std.math.cast(i64, scaled) orelse return null;
                if (positiveElementCount(resolved[0..target_shape.len])) |resolved_numel| {
                    if (resolved_numel == input_numel) return resolved;
                }
            }
        }
    }
    if (comptime compatibility.rescale_leading_batch) {
        if (negative_count == 0 and input_shape.len > 0 and target_shape.len > 0 and input_shape[0] > 1 and target_shape[0] == 1) {
            const target_numel = positiveElementCount(target_shape) orelse return null;
            if (target_numel > 0 and input_numel > target_numel and input_numel % target_numel == 0) {
                const batch_factor = input_numel / target_numel;
                if (batch_factor == @as(usize, @intCast(input_shape[0]))) {
                    resolved[0] = input_shape[0];
                    if (positiveElementCount(resolved[0..target_shape.len])) |resolved_numel| {
                        if (resolved_numel == input_numel) return resolved;
                    }
                }
            }
        }
    }
    if (comptime compatibility.collapse_pair_first) {
        if (negative_count > 1 and target_shape.len + 1 == input_shape.len and target_shape.len == 2) {
            var trailing_aligned = true;
            for (1..target_shape.len) |i| {
                const input_dim = input_shape[i + 1];
                if (resolved[i] < 0 and input_dim > 0) {
                    resolved[i] = input_dim;
                } else if (resolved[i] > 0 and input_dim > 0 and resolved[i] != input_dim) {
                    trailing_aligned = false;
                    break;
                }
            }
            if (trailing_aligned and resolved[0] < 0) {
                var collapsed: usize = 1;
                var valid = true;
                for (input_shape[0..2]) |dim| {
                    if (dim <= 0) {
                        valid = false;
                        break;
                    }
                    collapsed = std.math.mul(usize, collapsed, @intCast(dim)) catch {
                        valid = false;
                        break;
                    };
                }
                if (valid) resolved[0] = @intCast(collapsed);
            }
        }
    }

    // Collapse inner singleton input dims (e.g. `{77, 1, 512}` → `{77, 512}`) so
    // that attention-head splits can still resolve against the effective shape.
    if (negative_count > 1 and input_shape.len >= 3 and target_shape.len == input_shape.len + 1 and resolved[0] == 1) {
        var collapsed: [8]i64 = undefined;
        var collapsed_len: usize = 0;
        for (input_shape, 0..) |dim, i| {
            if (i > 0 and i < input_shape.len - 1 and dim == 1) continue;
            collapsed[collapsed_len] = dim;
            collapsed_len += 1;
        }
        if (collapsed_len < input_shape.len and target_shape.len == collapsed_len + 2) {
            const input_last = collapsed[collapsed_len - 1];
            const target_last = target_shape[target_shape.len - 1];
            if (input_last > 0 and target_last > 0 and @rem(input_last, target_last) == 0) {
                for (0..collapsed_len - 1) |i| {
                    const target_index = i + 1;
                    if (resolved[target_index] < 0 and collapsed[i] > 0) {
                        resolved[target_index] = collapsed[i];
                    }
                }
                if (resolved[target_shape.len - 2] < 0) {
                    resolved[target_shape.len - 2] = @divTrunc(input_last, target_last);
                }
            }
        }
    }

    if (negative_count > 1 and input_shape.len == 3 and target_shape.len == 4 and resolved[1] > 0 and resolved[3] > 0) {
        const hidden = input_shape[2];
        if (hidden > 0 and @rem(hidden, resolved[3]) == 0) {
            if (resolved[0] < 0 and input_shape[0] > 0) resolved[0] = input_shape[0];
            if (resolved[2] < 0) resolved[2] = @divTrunc(hidden, resolved[3]);
        }
    }

    // Unflatten multi-head attention outputs:
    //   [batch * heads, seq, head_dim] -> [batch, heads, seq, head_dim]
    if (negative_count > 1 and input_shape.len == 3 and target_shape.len == 4 and resolved[1] > 0) {
        const head_count: usize = @intCast(resolved[1]);
        var unflatten_known_product: usize = head_count;
        var can_unflatten = head_count > 0;
        if (input_shape[1] > 0) {
            unflatten_known_product = std.math.mul(usize, unflatten_known_product, @intCast(input_shape[1])) catch 0;
            can_unflatten = can_unflatten and unflatten_known_product > 0;
        }
        if (input_shape[2] > 0) {
            unflatten_known_product = std.math.mul(usize, unflatten_known_product, @intCast(input_shape[2])) catch 0;
            can_unflatten = can_unflatten and unflatten_known_product > 0;
        } else {
            can_unflatten = false;
        }
        if (can_unflatten and input_shape[1] > 0 and input_numel % unflatten_known_product == 0) {
            if (resolved[2] < 0) {
                resolved[2] = input_shape[1];
            }
            if (resolved[3] < 0) {
                resolved[3] = input_shape[2];
            }
            if (input_shape[0] > 0 and resolved[0] < 0) {
                const batch_heads: usize = @intCast(input_shape[0]);
                if (batch_heads % head_count == 0) {
                    resolved[0] = @intCast(batch_heads / head_count);
                }
            }
            var unresolved: usize = 0;
            var unresolved_index: usize = 0;
            var known_product: usize = head_count;
            var valid = true;
            for ([_]usize{ 0, 2, 3 }) |axis| {
                const dim = resolved[axis];
                if (dim > 0) {
                    known_product = std.math.mul(usize, known_product, @intCast(dim)) catch {
                        valid = false;
                        break;
                    };
                } else {
                    unresolved += 1;
                    unresolved_index = axis;
                }
            }
            if (valid and unresolved == 1 and known_product > 0 and input_numel % known_product == 0) {
                resolved[unresolved_index] = @intCast(input_numel / known_product);
            }
        }
    }

    if (negative_count > 1 and target_shape.len == input_shape.len + 1 and target_shape.len >= 2) {
        const input_last = input_shape[input_shape.len - 1];
        const target_last = target_shape[target_shape.len - 1];
        if (input_last > 0) {
            for (0..input_shape.len - 1) |i| {
                if (resolved[i] < 0 and input_shape[i] > 0) resolved[i] = input_shape[i];
            }
            if (resolved[target_shape.len - 1] < 0 and resolved[target_shape.len - 2] > 0 and @rem(input_last, resolved[target_shape.len - 2]) == 0) {
                resolved[target_shape.len - 1] = @divTrunc(input_last, resolved[target_shape.len - 2]);
            }
        }
        if (input_last > 0 and target_last > 0 and @rem(input_last, target_last) == 0) {
            if (resolved[0] > 0 and input_numel % @as(usize, @intCast(input_last)) == 0) {
                const prefix_numel = input_numel / @as(usize, @intCast(input_last));
                const leading_dim: usize = @intCast(resolved[0]);
                if (leading_dim > 0 and prefix_numel % leading_dim == 0 and resolved[1] < 0) {
                    resolved[1] = @intCast(prefix_numel / leading_dim);
                }
            }
            for (0..input_shape.len - 1) |i| {
                if (resolved[i] < 0 and input_shape[i] > 0) resolved[i] = input_shape[i];
            }
            if (resolved[input_shape.len - 1] < 0) {
                resolved[input_shape.len - 1] = @divTrunc(input_last, target_last);
            }
        }
    }

    if (negative_count > 1 and input_shape.len == 3 and target_shape.len == 4 and resolved[1] > 0) {
        const input_last = input_shape[2];
        if (input_last > 0 and @rem(input_last, resolved[1]) == 0) {
            if (resolved[3] < 0) {
                resolved[3] = @divTrunc(input_last, resolved[1]);
            }
            if (resolved[2] < 0) {
                if (input_shape[1] > 0) {
                    resolved[2] = input_shape[1];
                } else {
                    const prefix_numel = input_numel / @as(usize, @intCast(input_last));
                    if (resolved[0] < 0) {
                        resolved[0] = 1;
                        resolved[2] = @intCast(prefix_numel);
                    }
                }
            }
            if (resolved[0] < 0 and input_shape[0] > 0) {
                resolved[0] = input_shape[0];
            }
        }
    }
    if (comptime compatibility.infer_batch_sequence) {
        if (negative_count > 1 and target_shape.len == 4 and resolved[0] < 0 and resolved[1] < 0 and resolved[2] > 0 and resolved[3] > 0) {
            const head_count: usize = @intCast(resolved[2]);
            const head_dim: usize = @intCast(resolved[3]);
            const hidden = std.math.mul(usize, head_count, head_dim) catch 0;
            if (hidden > 0 and input_numel % hidden == 0) {
                const prefix = input_numel / hidden;
                if (input_shape.len > 0 and input_shape[0] > 0) {
                    const batch: usize = @intCast(input_shape[0]);
                    if (batch > 0 and prefix % batch == 0) {
                        resolved[0] = input_shape[0];
                        resolved[1] = @intCast(prefix / batch);
                    }
                } else {
                    resolved[0] = 1;
                    resolved[1] = @intCast(prefix);
                }
            }
        }
    }

    if (negative_count > 1 and target_shape.len == input_shape.len + 2 and target_shape.len >= 3 and resolved[0] == 1) {
        const input_last = input_shape[input_shape.len - 1];
        const target_last = target_shape[target_shape.len - 1];
        if (input_last > 0 and target_last > 0 and @rem(input_last, target_last) == 0) {
            for (0..input_shape.len - 1) |i| {
                const target_index = i + 1;
                if (resolved[target_index] < 0 and input_shape[i] > 0) {
                    resolved[target_index] = input_shape[i];
                }
            }
            if (resolved[target_shape.len - 2] < 0) {
                resolved[target_shape.len - 2] = @divTrunc(input_last, target_last);
            }
        }
    }
    if (comptime compatibility.preserve_mask_batch) {
        if (negative_count > 1 and input_shape.len == 2 and target_shape.len == 4 and target_shape[1] == 1 and target_shape[2] == 1) {
            const batch = input_shape[0];
            const seq_len = input_shape[1];
            if (batch > 0 and seq_len > 0 and input_numel == @as(usize, @intCast(batch)) * @as(usize, @intCast(seq_len))) {
                if (resolved[0] < 0) resolved[0] = batch;
                if (resolved[3] < 0) resolved[3] = seq_len;
            }
        }
    }

    if (negative_count > 1 and target_shape.len + 1 == input_shape.len and target_shape.len >= 3) {
        var valid = true;
        for (0..target_shape.len - 1) |i| {
            const input_dim = input_shape[i];
            if (resolved[i] < 0 and input_dim > 0) {
                resolved[i] = input_dim;
            } else if (resolved[i] > 0 and input_dim > 0 and resolved[i] != input_dim) {
                valid = false;
                break;
            }
        }
        if (valid) {
            var collapsed: usize = 1;
            for (input_shape[target_shape.len - 1 .. input_shape.len]) |dim| {
                if (dim <= 0) {
                    valid = false;
                    break;
                }
                collapsed = std.math.mul(usize, collapsed, @intCast(dim)) catch {
                    valid = false;
                    break;
                };
            }
            if (valid) {
                const last_index = target_shape.len - 1;
                if (resolved[last_index] < 0) {
                    resolved[last_index] = @intCast(collapsed);
                } else if (resolved[last_index] != @as(i64, @intCast(collapsed))) {
                    valid = false;
                }
            }
        }
    }
    if (comptime compatibility.flatten_four_axes) {
        if (negative_count > 1 and input_shape.len == 4 and target_shape.len == 3) {
            const input_last = input_shape[3];
            const target_last = target_shape[2];
            if (input_last > 0 and target_last > 0 and input_last == target_last) {
                const leading = if (resolved[0] > 0) resolved[0] else if (input_shape[0] > 0) input_shape[0] else 1;
                const leading_usize: usize = @intCast(leading);
                const last_usize: usize = @intCast(target_last);
                const known_product = std.math.mul(usize, leading_usize, last_usize) catch 0;
                if (known_product > 0 and input_numel % known_product == 0) {
                    resolved[0] = leading;
                    resolved[1] = @intCast(input_numel / known_product);
                    resolved[2] = target_last;
                }
            }
        }
    }

    if (negative_count > 1 and target_shape.len + 1 == input_shape.len and target_shape.len >= 2 and resolved[0] > 0 and input_shape[0] > 0 and resolved[0] == input_shape[0]) {
        var valid = true;
        for (1..target_shape.len - 1) |i| {
            const input_dim = input_shape[i];
            if (resolved[i] < 0 and input_dim > 0) {
                resolved[i] = input_dim;
            } else if (resolved[i] > 0 and input_dim > 0 and resolved[i] != input_dim) {
                valid = false;
                break;
            }
        }
        if (valid) {
            var collapsed: usize = 1;
            for (input_shape[target_shape.len - 1 .. input_shape.len]) |dim| {
                if (dim <= 0) {
                    valid = false;
                    break;
                }
                collapsed = std.math.mul(usize, collapsed, @intCast(dim)) catch {
                    valid = false;
                    break;
                };
            }
            if (valid) {
                const last_index = target_shape.len - 1;
                if (resolved[last_index] < 0) {
                    resolved[last_index] = @intCast(collapsed);
                } else if (resolved[last_index] != @as(i64, @intCast(collapsed))) {
                    valid = false;
                }
            }
        }
    }
    if (comptime !compatibility.collapse_pair_first) {
        if (negative_count > 1 and target_shape.len + 1 == input_shape.len and target_shape.len == 2) {
            var trailing_aligned = true;
            for (1..target_shape.len) |i| {
                const input_dim = input_shape[i + 1];
                if (resolved[i] < 0 and input_dim > 0) {
                    resolved[i] = input_dim;
                } else if (resolved[i] > 0 and input_dim > 0 and resolved[i] != input_dim) {
                    trailing_aligned = false;
                    break;
                }
            }
            if (trailing_aligned and resolved[0] < 0) {
                var collapsed: usize = 1;
                var valid = true;
                for (input_shape[0..2]) |dim| {
                    if (dim <= 0) {
                        valid = false;
                        break;
                    }
                    collapsed = std.math.mul(usize, collapsed, @intCast(dim)) catch {
                        valid = false;
                        break;
                    };
                }
                if (valid) resolved[0] = @intCast(collapsed);
            }
        }
    }

    if (negative_count > 1) {
        var known_product: usize = 1;
        var valid = true;
        for (resolved[0..target_shape.len]) |dim| {
            if (dim <= 0) continue;
            known_product = std.math.mul(usize, known_product, @intCast(dim)) catch {
                valid = false;
                break;
            };
        }
        if (valid and known_product == input_numel) {
            for (0..target_shape.len) |i| {
                if (resolved[i] < 0) resolved[i] = 1;
            }
            negative_count = 0;
        }
    }

    var infer_index: ?usize = null;
    var known_product: usize = 1;

    for (resolved[0..target_shape.len], 0..) |dim, i| {
        if (dim == -1) {
            if (infer_index != null) return null;
            infer_index = i;
            continue;
        }
        if (dim <= 0) return null;
        known_product = std.math.mul(usize, known_product, @intCast(dim)) catch return null;
    }

    if (infer_index) |idx| {
        if (known_product == 0 or input_numel % known_product != 0) return null;
        resolved[idx] = @intCast(input_numel / known_product);
    } else if (known_product != input_numel) {
        return null;
    }

    for (resolved[0..target_shape.len]) |dim| {
        if (dim <= 0) return null;
    }
    return resolved;
}

fn positiveElementCount(shape: []const i64) ?usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim <= 0) return null;
        count = std.math.mul(usize, count, @intCast(dim)) catch return null;
    }
    return count;
}

test "loose broadcast resolves known extents while preserving symbolic axes" {
    var output: [8]i64 = undefined;
    const rank = try rightAlignedShape(3, .loose, .{ &.{ -2, 1, 0 }, &.{ 1, 3, 1 }, &.{ 1, -1, 1 } }, &output);
    try std.testing.expectEqual(@as(usize, 3), rank);
    try std.testing.expectEqualSlices(i64, &.{ -1, 3, -1 }, output[0..rank]);
    try std.testing.expectError(error.ShapeMismatch, rightAlignedShape(3, .loose, .{ &.{2}, &.{3}, &.{-1} }, &output));
}

test "shared reshape compatibility retains batch scaling and symbolic attention policies" {
    const native = ReshapeCompatibility{ .rescale_leading_batch = true, .infer_batch_sequence = true, .preserve_mask_batch = true };
    const metal = ReshapeCompatibility{ .collapse_pair_first = true, .flatten_four_axes = true };
    const batched = inferReshapeShapeWithNumel(native, &.{ 2, 3, 4 }, &.{ 3, 4 }, 24).?;
    try std.testing.expectEqualSlices(i64, &.{ 6, 4 }, batched[0..2]);
    try std.testing.expect(inferReshapeShapeWithNumel(metal, &.{ 2, 3, 4 }, &.{ 3, 4 }, 24) == null);
    const mask = inferReshapeShapeWithNumel(native, &.{ 3, 512 }, &.{ -1, 1, 1, -1 }, 3 * 512).?;
    try std.testing.expectEqualSlices(i64, &.{ 3, 1, 1, 512 }, mask[0..4]);
    const heads = inferReshapeShapeWithNumel(metal, &.{ 1, -1, -1, 64 }, &.{ -1, -1, 64 }, 8 * 77 * 64).?;
    try std.testing.expectEqualSlices(i64, &.{ 1, 616, 64 }, heads[0..3]);
    try std.testing.expect(inferReshapeShapeWithNumel(native, &.{4}, &.{3}, 4) == null);
}

pub fn ropeChunksPerToken(logical_shape: ?[]const i64, total_chunks: usize, seq_len: usize, head_dim: usize) usize {
    if (logical_shape) |shape| {
        if (shape.len == 2 and shape[0] > 0 and shape[1] > 0 and
            @as(usize, @intCast(shape[0])) % seq_len == 0 and
            @as(usize, @intCast(shape[1])) % head_dim == 0)
        {
            const chunks_per_token = @as(usize, @intCast(shape[1])) / head_dim;
            if (chunks_per_token > 0 and chunks_per_token * @as(usize, @intCast(shape[0])) == total_chunks) {
                return chunks_per_token;
            }
        }
    }
    return total_chunks / seq_len;
}
