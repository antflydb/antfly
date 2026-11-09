// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Physical F32 blocked local-relative attention. Source scaling, GEMM and
//! publication boundaries are explicit; no checkpoint or model policy lives here.
const std = @import("std");
const Layout = @import("libraries.zig").F32StridedBatchedLayout;
pub const Span = struct { offset: usize, bytes: usize };
pub const Plan = struct {
    rows: usize,
    heads: usize,
    dim: usize,
    chunk: usize,
    context: usize,
    relative: usize,
    blocks: usize,
    padded: usize,
    query: Span,
    packed_query: Span,
    key: Span,
    value: Span,
    scores: Span,
    relative_scores: Span,
    result: Span,
    workspace: Span,
    bytes: usize,
    pub const workspace_bytes = @import("libraries.zig").Cublas.f32_source_workspace_bytes;
    fn mul(a: usize, b: usize) !usize {
        return std.math.mul(usize, a, b);
    }
    fn append(cursor: *usize, elements: usize) !Span {
        const start = std.mem.alignBackward(usize, try std.math.add(usize, cursor.*, 255), 256);
        const bytes = try mul(elements, 4);
        cursor.* = try std.math.add(usize, start, bytes);
        return .{ .offset = start, .bytes = bytes };
    }
    pub fn resolve(rows: usize, heads: usize, dim: usize, chunk: usize, context: usize, relative: usize) !Plan {
        if (rows == 0 or heads == 0 or dim == 0 or chunk == 0 or relative < 2 or context > 24 or
            context != try std.math.add(usize, chunk, relative - 1)) return error.InvalidShape;
        const blocks = rows / chunk + @intFromBool(rows % chunk != 0);
        const padded = try mul(blocks, chunk);
        const hidden = try mul(heads, dim);
        const matrix = try mul(padded, hidden);
        const contexts = try mul(try mul(blocks, context), hidden);
        const logits = try mul(try mul(padded, heads), context);
        const relative_logits = try mul(try mul(padded, heads), relative);
        for ([_]usize{ rows, heads, dim, chunk, context, padded, matrix, contexts, logits, relative_logits }) |v|
            if (v > std.math.maxInt(u32)) return error.InvalidShape;
        var cursor: usize = 0;
        const query = try append(&cursor, matrix);
        // Q's packed copy dies before relative GEMM writes this same bank.
        const shared = try append(&cursor, @max(if (blocks > 1) matrix else 0, relative_logits));
        const key = try append(&cursor, contexts);
        const value = try append(&cursor, contexts);
        const scores = try append(&cursor, logits);
        const workspace = try append(&cursor, workspace_bytes / 4);
        const result: Plan = .{
            .rows = rows,
            .heads = heads,
            .dim = dim,
            .chunk = chunk,
            .context = context,
            .relative = relative,
            .blocks = blocks,
            .padded = padded,
            .query = query,
            .packed_query = if (blocks > 1) .{ .offset = shared.offset, .bytes = matrix * 4 } else query,
            .key = key,
            .value = value,
            .scores = scores,
            .relative_scores = .{ .offset = shared.offset, .bytes = relative_logits * 4 },
            // Relative GEMM consumes Q before context GEMM reuses its bank.
            .result = query,
            .workspace = workspace,
            .bytes = cursor,
        };
        try result.contentLayout().validate();
        try result.relativeLayout().validate();
        try result.contextLayout().validate();
        return result;
    }
    pub fn contentLayout(p: Plan) Layout {
        const direct = p.blocks == 1;
        return .{ .m = p.context, .n = p.chunk, .k = p.dim, .batches = p.heads * p.blocks, .transpose_a = direct, .lda = if (direct) p.heads * p.dim else p.context, .ldb = if (direct) p.heads * p.dim else p.dim, .ldc = p.context, .stride_a = if (direct) p.dim else p.context * p.dim, .stride_b = if (direct) p.dim else p.chunk * p.dim, .stride_c = p.chunk * p.context, .a_elements = p.key.bytes / 4, .b_elements = p.packed_query.bytes / 4, .c_elements = p.scores.bytes / 4 };
    }
    pub fn relativeLayout(p: Plan) Layout {
        return .{ .m = p.relative, .n = p.padded, .k = p.dim, .batches = p.heads, .transpose_a = true, .lda = p.heads * p.dim, .ldb = p.heads * p.dim, .ldc = p.relative, .stride_a = p.dim, .stride_b = p.dim, .stride_c = p.padded * p.relative, .a_elements = p.relative * p.heads * p.dim, .b_elements = p.query.bytes / 4, .c_elements = p.relative_scores.bytes / 4 };
    }
    pub fn contextLayout(p: Plan) Layout {
        const direct = p.blocks == 1;
        return .{ .m = p.dim, .n = p.chunk, .k = p.context, .batches = p.heads * p.blocks, .lda = if (direct) p.heads * p.dim else p.dim, .ldb = p.context, .ldc = if (direct) p.heads * p.dim else p.dim, .stride_a = if (direct) p.dim else p.context * p.dim, .stride_b = p.chunk * p.context, .stride_c = if (direct) p.dim else p.chunk * p.dim, .a_elements = p.value.bytes / 4, .b_elements = p.scores.bytes / 4, .c_elements = p.result.bytes / 4 };
    }
};

// Independent source-layout oracles belong to tests, not the serving Plan API.
const TestReference = struct {
    /// Literal pad/view/truncate mapping from [C,L] through [C,W+1].
    fn shiftedRelative(p: Plan, query: usize, key: usize) ?usize {
        const flat = query * p.context + key;
        const column = flat % (p.context + 1);
        return if (column < p.relative) flat / (p.context + 1) * p.relative + column else null;
    }
    fn allowed(p: Plan, valid: []const f32, query: usize, key: usize, exclude_farthest: bool) bool {
        const block = query / p.chunk * p.chunk;
        const absolute = @as(i64, @intCast(block + key)) - @as(i64, @intCast(p.relative - 1));
        return query < p.rows and absolute >= 0 and absolute < @as(i64, @intCast(p.rows)) and absolute <= @as(i64, @intCast(query)) and
            query - @as(usize, @intCast(absolute)) <= p.relative - 1 - @intFromBool(exclude_farthest) and valid[@intCast(absolute)] != 0;
    }
};

test "local relative source matches actual whole-model packed and interleaved GEMM layouts" {
    const p = try Plan.resolve(36, 8, 128, 12, 24, 13);
    const ac = p.contentLayout();
    const bd = p.relativeLayout();
    const pv = p.contextLayout();
    try std.testing.expectEqualSlices(usize, &.{ 24, 12, 128, 24, 24, 128, 24, 3072, 1536, 288 }, &.{ ac.m, ac.n, ac.k, ac.batches, ac.lda, ac.ldb, ac.ldc, ac.stride_a, ac.stride_b, ac.stride_c });
    try std.testing.expectEqualSlices(usize, &.{ 13, 36, 128, 8, 1024, 1024, 13, 128, 128, 468 }, &.{ bd.m, bd.n, bd.k, bd.batches, bd.lda, bd.ldb, bd.ldc, bd.stride_a, bd.stride_b, bd.stride_c });
    try std.testing.expect(bd.transpose_a and !bd.transpose_b);
    try std.testing.expectEqualSlices(usize, &.{ 128, 12, 24, 24, 128, 24, 128, 3072, 288, 1536 }, &.{ pv.m, pv.n, pv.k, pv.batches, pv.lda, pv.ldb, pv.ldc, pv.stride_a, pv.stride_b, pv.stride_c });
    try std.testing.expectEqual(p.query.offset, p.result.offset);
    try std.testing.expectEqual(p.packed_query.offset, p.relative_scores.offset);
    try std.testing.expectEqual(@as(usize, 34466816), p.bytes);
}

test "local relative source single block keeps original head-interleaved operands" {
    const p = try Plan.resolve(3, 2, 3, 4, 6, 3);
    const ac = p.contentLayout();
    const bd = p.relativeLayout();
    const pv = p.contextLayout();
    try std.testing.expect(ac.transpose_a and !ac.transpose_b);
    try std.testing.expectEqualDeep(p.query, p.packed_query);
    for (0..2) |head| for (0..4) |query| for (0..6) |key| for (0..3) |d| {
        // The reference starts from contiguous [N,W,H,D] and [N,C,H,D]
        // before permuting; one-block views need no materialized transpose.
        const source_key = key * 6 + head * 3 + d;
        const source_query = query * 6 + head * 3 + d;
        try std.testing.expectEqual(source_key, head * ac.stride_a + d + key * ac.lda);
        try std.testing.expectEqual(source_query, head * ac.stride_b + d + query * ac.ldb);
        try std.testing.expectEqual(source_key, head * pv.stride_a + d + key * pv.lda);
        try std.testing.expectEqual(source_query, head * pv.stride_c + d + query * pv.ldc);
        try std.testing.expectEqual(source_query, head * bd.stride_b + d + query * bd.ldb);
    };
}

test "local relative shift matches literal padded flatten and truncated view" {
    const p = try Plan.resolve(17, 2, 3, 12, 24, 13);
    var padded: [12 * 25]?usize = @splat(null);
    for (0..12) |q| for (0..13) |r| {
        padded[q * 25 + r] = q * 13 + r;
    };
    // Independent oracle is the original operation sequence, not a causal
    // shortcut. Values outside the live mask must still be shifted correctly.
    const truncated = padded[0 .. 12 * 24];
    for (0..12) |q| for (0..24) |k| try std.testing.expectEqual(truncated[q * 24 + k], TestReference.shiftedRelative(p, q, k));
}

test "local relative masks match full source mask then blocked gather including finite all-masked rows" {
    const p = try Plan.resolve(7, 2, 4, 4, 6, 3);
    const valid = [_]f32{ 1, 1, 0, 1, 1, 0, 1 };
    for ([_]bool{ false, true }) |exclude| {
        var full: [8][8]bool = @splat(@splat(false));
        for (0..7) |q| for (0..7) |k| {
            full[q][k] = valid[k] != 0 and k <= q and q - k <= @as(usize, 2) - @intFromBool(exclude);
        };
        for (0..8) |q| for (0..6) |c| {
            const padded_key = q / 4 * 4 + c;
            const expected = if (padded_key >= 2 and padded_key - 2 < 8) full[q][padded_key - 2] else false;
            try std.testing.expectEqual(expected, TestReference.allowed(p, &valid, q, c, exclude));
        };
    }
    // Source masked_fill uses finite -1e9: all equal logits normalize to 1/W.
    // Query validity is not an early-zero numerical shortcut in this route.
    const invalid: f32 = -1e9;
    var sum: f32 = 0;
    for (0..6) |_| sum += @exp(invalid - invalid);
    try std.testing.expectEqual(@as(f32, 1.0 / 6.0), @exp(invalid - invalid) / sum);
}

test "local relative source rejects malformed geometry before device ownership" {
    for ([_][6]usize{ .{ 0, 2, 3, 4, 6, 3 }, .{ 3, 0, 3, 4, 6, 3 }, .{ 3, 2, 0, 4, 6, 3 }, .{ 3, 2, 3, 0, 6, 3 }, .{ 3, 2, 3, 4, 7, 3 }, .{ 3, 2, 3, 24, 25, 2 } }) |v|
        try std.testing.expectError(error.InvalidShape, Plan.resolve(v[0], v[1], v[2], v[3], v[4], v[5]));
    try std.testing.expectError(error.Overflow, Plan.resolve(std.math.maxInt(usize), 2, 3, 4, 6, 3));
}

test "local relative materialized head block addresses match independent multidimensional source views" {
    const p = try Plan.resolve(5, 2, 3, 4, 6, 3);
    var q: [2][4][2][3]f32 = undefined;
    var k: [2][6][2][3]f32 = undefined;
    var v: [2][6][2][3]f32 = undefined;
    for (0..2) |block| {
        for (0..4) |row| for (0..2) |head| for (0..3) |d| {
            q[block][row][head][d] = @floatFromInt(1 + block + row + head + d);
        };
        for (0..6) |key| for (0..2) |head| for (0..3) |d| {
            k[block][key][head][d] = @floatFromInt(2 + 2 * block + key + head + d);
            v[block][key][head][d] = @floatFromInt(3 + block + key + 2 * head + d);
        };
    }
    // Materialize the source permuted views in their logical traversal order.
    // No production linear-index helper is used by this independent oracle.
    var pq: [48]f32 = undefined;
    var pk: [72]f32 = undefined;
    var pv: [72]f32 = undefined;
    var cursor: usize = 0;
    for (0..2) |h| for (0..2) |b| for (0..4) |r| for (0..3) |d| {
        pq[cursor] = q[b][r][h][d];
        cursor += 1;
    };
    cursor = 0;
    for (0..2) |h| for (0..2) |b| for (0..3) |d| for (0..6) |c| {
        pk[cursor] = k[b][c][h][d];
        cursor += 1;
    };
    cursor = 0;
    for (0..2) |h| for (0..2) |b| for (0..6) |c| for (0..3) |d| {
        pv[cursor] = v[b][c][h][d];
        cursor += 1;
    };
    const ac = p.contentLayout();
    const context = p.contextLayout();
    for (0..2) |h| for (0..2) |b| for (0..4) |r| for (0..6) |c| {
        var expected: f32 = 0;
        var actual: f32 = 0;
        for (0..3) |d| {
            expected += q[b][r][h][d] * k[b][c][h][d];
            const batch = h * 2 + b;
            actual += pq[batch * ac.stride_b + r * ac.ldb + d] * pk[batch * ac.stride_a + d * ac.lda + c];
            try std.testing.expectEqual(v[b][c][h][d], pv[batch * context.stride_a + c * context.lda + d]);
        }
        try std.testing.expectEqual(expected, actual);
    };
    // Source context output is [H,N,C,D] before its final permute/truncate.
    var packed_result: [2][2][4][3]f32 = undefined;
    for (0..2) |h| for (0..2) |b| for (0..4) |r| for (0..3) |d| {
        packed_result[h][b][r][d] = @floatFromInt(100 * h + 20 * b + 3 * r + d);
    };
    const flat: *const [48]f32 = @ptrCast(&packed_result);
    for (0..5) |row| for (0..2) |h| for (0..3) |d| {
        const index = ((h * p.blocks + row / p.chunk) * p.chunk + row % p.chunk) * p.dim + d;
        try std.testing.expectEqual(packed_result[h][row / 4][row % 4][d], flat[index]);
    };
}
