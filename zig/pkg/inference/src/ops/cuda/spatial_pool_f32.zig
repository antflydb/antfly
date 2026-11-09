// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Source spatial pooling: physical F32 GEMM, BF16 mean, F32 scaling and
//! final BF16 publication. Every temporary belongs to one caller-funded bank.
//! The adapter supplies dense row-major grid positions with right-tail padding;
//! arbitrary masks/positions require a distinct explicit layout contract.
const std = @import("std");
const ops = @import("../ops.zig");
const Blas = @import("libraries.zig");
pub const Phase = enum(u32) { weights, input, publish };
pub const Region = struct { offset: usize, bytes: usize };
pub const Plan = struct {
    params: ops.SpatialPoolParams,
    physical: usize,
    pooled: usize,
    weights: Region,
    input: Region,
    result: Region,
    workspace: Region,
    bytes: usize,

    pub fn resolve(params: ops.SpatialPoolParams) !Plan {
        try params.validate();
        if (params.publication != .bf16_mean_scaled) return error.InvalidShape;
        const logical = try std.math.mul(usize, params.grid_x, params.grid_y);
        const physical = if (params.physical_rows != 0) params.physical_rows else logical;
        const area = try std.math.mul(usize, params.merge, params.merge);
        if (physical % area != 0) return error.InvalidShape;
        const pooled = physical / area;
        const weight_elements = try std.math.mul(usize, physical, pooled);
        if (weight_elements > std.math.maxInt(u32)) return error.InvalidShape;
        var cursor: usize = 0;
        const weights = try region(&cursor, weight_elements);
        const input = try region(&cursor, try std.math.mul(usize, physical, params.dim));
        const result = try region(&cursor, try std.math.mul(usize, pooled, params.dim));
        const workspace = try region(&cursor, Blas.Cublas.f32_source_workspace_bytes / 4);
        const plan = Plan{ .params = params, .physical = physical, .pooled = pooled, .weights = weights, .input = input, .result = result, .workspace = workspace, .bytes = cursor };
        try plan.layout().validate();
        return plan;
    }
    fn region(cursor: *usize, elements: usize) !Region {
        const bytes = try std.math.mul(usize, elements, 4);
        const offset = try std.math.add(usize, cursor.*, 255) & ~@as(usize, 255);
        cursor.* = try std.math.add(usize, offset, bytes);
        return .{ .offset = offset, .bytes = bytes };
    }
    pub fn layout(p: Plan) Blas.F32StridedBatchedLayout {
        // Captured source: cublasSgemm_v2 N/T, M=dim,N=pooled,K=physical;
        // one-hot weights stay token-major[physical,pooled], ldb=pooled.
        return .{ .m = p.params.dim, .n = p.pooled, .k = p.physical, .batches = 1, .transpose_b = true, .lda = p.params.dim, .ldb = p.pooled, .ldc = p.params.dim, .stride_a = p.input.bytes / 4, .stride_b = p.weights.bytes / 4, .stride_c = p.result.bytes / 4, .a_elements = p.input.bytes / 4, .b_elements = p.weights.bytes / 4, .c_elements = p.result.bytes / 4 };
    }
    pub fn phaseElements(p: Plan, phase: Phase) usize {
        return switch (phase) {
            .weights => p.weights.bytes / 4,
            .input => p.input.bytes / 4,
            .publish => p.params.rows() / p.params.batch * p.params.dim,
        };
    }
};

test "F32 spatial GEMM keeps physical padding and exact source reduction geometry" {
    const p = try Plan.resolve(.{ .batch = 2, .grid_x = 60, .grid_y = 39, .merge = 3, .dim = 768, .physical_rows = 2520, .publication = .bf16_mean_scaled });
    try std.testing.expectEqual(@as(usize, 280), p.pooled);
    try std.testing.expectEqual(@as(usize, 520), p.params.rows());
    const l = p.layout();
    try std.testing.expectEqual(@as(usize, 768), l.m);
    try std.testing.expectEqual(@as(usize, 280), l.n);
    try std.testing.expectEqual(@as(usize, 2520), l.k);
    try std.testing.expectEqual(@as(usize, 1), l.batches);
    try std.testing.expect(!l.transpose_a and l.transpose_b);
    try std.testing.expectEqual(@as(usize, 280), l.ldb);
    try std.testing.expect(p.input.offset >= p.weights.offset + p.weights.bytes);
    try std.testing.expect(p.result.offset >= p.input.offset + p.input.bytes);
    try std.testing.expect(p.workspace.offset >= p.result.offset + p.result.bytes);
    try std.testing.expectEqual(@as(usize, 0), p.workspace.offset % 256);
    try std.testing.expectError(error.InvalidTensorShape, Plan.resolve(.{ .batch = 1, .grid_x = 60, .grid_y = 39, .merge = 3, .dim = 768, .physical_rows = 2341, .publication = .bf16_mean_scaled }));
}

test "spatial source pooling rejects overflow and nonintegral physical reductions" {
    try std.testing.expectError(error.InvalidTensorShape, Plan.resolve(.{ .batch = 1, .grid_x = 6, .grid_y = 6, .merge = 3, .dim = 4, .physical_rows = 37, .publication = .bf16_mean_scaled }));
    try std.testing.expectError(error.InvalidTensorShape, Plan.resolve(.{ .batch = 2, .grid_x = 6, .grid_y = 6, .merge = 3, .dim = 4, .physical_rows = 4294967292, .publication = .bf16_mean_scaled }));
    try std.testing.expectError(error.InvalidTensorShape, Plan.resolve(.{ .batch = 1, .grid_x = 6, .grid_y = 6, .merge = 3, .dim = 4, .physical_rows = 27, .publication = .bf16_mean_scaled }));
}

test "source position pooling fixture retains clamped padding and valid-row selection" {
    const p = try Plan.resolve(.{ .batch = 1, .grid_x = 6, .grid_y = 3, .merge = 3, .dim = 1, .physical_rows = 27, .publication = .bf16_mean_scaled });
    // Independent explicit source positions. The last nine positions are -1,
    // so one-hot sends them to row zero but masked activations are zero.
    const positions = [_][2]i32{
        .{ 0, 0 },   .{ 1, 0 },   .{ 2, 0 },   .{ 3, 0 },   .{ 4, 0 },   .{ 5, 0 },
        .{ 0, 1 },   .{ 1, 1 },   .{ 2, 1 },   .{ 3, 1 },   .{ 4, 1 },   .{ 5, 1 },
        .{ 0, 2 },   .{ 1, 2 },   .{ 2, 2 },   .{ 3, 2 },   .{ 4, 2 },   .{ 5, 2 },
        .{ -1, -1 }, .{ -1, -1 }, .{ -1, -1 }, .{ -1, -1 }, .{ -1, -1 }, .{ -1, -1 },
        .{ -1, -1 }, .{ -1, -1 }, .{ -1, -1 },
    };
    var counts = [_]usize{ 0, 0, 0 };
    var means = [_]f32{ 0, 0, 0 };
    var mask = [_]bool{ false, false, false };
    for (positions) |pos| {
        const group: usize = @intCast(@divTrunc(@max(pos[0], 0), 3) + 2 * @divTrunc(@max(pos[1], 0), 3));
        counts[group] += 1;
        mask[group] = true;
        const hidden: f32 = if (pos[0] < 0 or pos[1] < 0) 0 else if (group == 0) 9 else 18;
        means[group] += hidden / 9;
    }
    try std.testing.expectEqualSlices(usize, &.{ 18, 9, 0 }, &counts);
    try std.testing.expectEqualSlices(f32, &.{ 9, 18, 0 }, &means);
    try std.testing.expectEqualSlices(bool, &.{ true, true, false }, &mask);
    try std.testing.expectEqual(@as(usize, 3), p.pooled);
    try std.testing.expectEqual(@as(usize, 2), p.params.rows());
    // Holey positions cannot be represented by this adapter's validated
    // dense-grid/right-tail contract. Shape-only relabeling is not a mapping.
    var irregular = positions;
    irregular[4] = .{ -1, -1 };
    var dense = true;
    for (irregular[0..18], 0..) |pos, index| {
        if (pos[0] != @as(i32, @intCast(index % 6)) or pos[1] != @as(i32, @intCast(index / 6))) dense = false;
    }
    try std.testing.expect(!dense);
}
