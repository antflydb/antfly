// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded DeBERTa disentangled attention with online softmax. All three score
//! terms use SGEMM; relative projections cover only the Q+K-1 positions in the
//! current tile. Workspace is independent of sequence length, batch and heads.
const std = @import("std");
const native = @import("../backends/native.zig");
const primitives = @import("inference_linalg").primitives;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Shape = struct { batch: usize, sequence: usize, heads: usize, head_dim: usize };
pub const Input = struct {
    q: []const f32, // [B,S,H*D]
    k: []const f32,
    v: []const f32,
    qr: []const f32, // [2S-1,H*D], already projected and bucket-gathered
    kr: []const f32,
    mask: []const i64, // [B,S], zero masks a key
};
pub const Options = struct {
    query_tile: usize = 128,
    key_tile: usize = 128,
    max_scratch_bytes: usize = 8 * 1024 * 1024,
    max_output_bytes: usize = 256 * 1024 * 1024,
    control: ?Control = null,
    io: ?std.Io = null,

    fn check(self: Options) !void {
        if (self.control) |control| try control.check();
    }

    fn transB(self: Options, m: usize, n: usize, k: usize, scale: f32, a: []const f32, b: []const f32, out: []f32) !void {
        try self.check();
        if (self.io) |io| {
            try native.sgemmTransB(io, m, n, k, scale, a, b, 0, out);
        } else native.sgemmTransBSync(m, n, k, scale, a, b, 0, out);
        try self.check();
    }

    fn mix(self: Options, m: usize, n: usize, k: usize, a: []const f32, b: []const f32, out: []f32) !void {
        try self.check();
        if (self.io) |io| {
            try native.sgemm(io, m, n, k, 1, a, b, 1, out);
        } else native.sgemmSync(m, n, k, 1, a, b, 1, out);
        try self.check();
    }
};

pub const Workspace = struct {
    hidden: usize,
    tokens: usize,
    relative_positions: usize,
    relative_elements: usize,
    output_elements: usize,
    output_bytes: usize,
    scratch_elements: usize,
    scratch_bytes: usize,
    query_tile: usize,
    key_tile: usize,
    relative_tile: usize,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidAttentionShape;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidAttentionShape;
}

pub fn plan(shape: Shape, options: Options) !Workspace {
    try options.check();
    if (options.query_tile == 0 or options.key_tile == 0 or options.query_tile > 512 or options.key_tile > 512)
        return error.InvalidAttentionTile;
    if (shape.heads == 0 or shape.head_dim == 0 or shape.head_dim > std.math.maxInt(i32)) return error.InvalidAttentionShape;
    if (shape.sequence > std.math.maxInt(i32)) return error.InvalidAttentionShape;
    const hidden = try mul(shape.heads, shape.head_dim);
    const tokens = try mul(shape.batch, shape.sequence);
    const output_elements = try mul(tokens, hidden);
    const output_bytes = try mul(output_elements, @sizeOf(f32));
    if (output_bytes > options.max_output_bytes) return error.AttentionOutputLimitExceeded;
    const relative_positions = if (shape.sequence == 0) 0 else (try mul(shape.sequence, 2)) - 1;
    const relative_elements = try mul(relative_positions, hidden);
    const query_tile = @min(shape.sequence, options.query_tile);
    const key_tile = @min(shape.sequence, options.key_tile);
    const relative_tile = if (query_tile == 0) 0 else (try add(query_tile, key_tile)) - 1;
    // Scores Q*K, the two relative GEMMs Q*R and K*R, Q/K/V packs,
    // two R*D packs, output Q*D, and the online max/sum vectors.
    var scratch: usize = 0;
    if (tokens > 0) {
        scratch = try mul(query_tile, key_tile);
        scratch = try add(scratch, try mul(try add(query_tile, key_tile), relative_tile));
        scratch = try add(scratch, try mul(try mul(2, try add(query_tile, key_tile)), shape.head_dim));
        scratch = try add(scratch, try mul(try mul(2, relative_tile), shape.head_dim));
        scratch = try add(scratch, try mul(2, query_tile));
    }
    const scratch_bytes = try mul(scratch, @sizeOf(f32));
    if (scratch_bytes > options.max_scratch_bytes) return error.AttentionScratchLimitExceeded;
    return .{
        .hidden = hidden,
        .tokens = tokens,
        .relative_positions = relative_positions,
        .relative_elements = relative_elements,
        .output_elements = output_elements,
        .output_bytes = output_bytes,
        .scratch_elements = scratch,
        .scratch_bytes = scratch_bytes,
        .query_tile = query_tile,
        .key_tile = key_tile,
        .relative_tile = relative_tile,
    };
}

fn finiteInput(values: []const f32, count: usize, options: Options) !void {
    if (values.len < count) return error.InvalidAttentionShape;
    const width = primitives.vec_len;
    const Bits = @Vector(width, u32);
    const exponent: Bits = @splat(0x7f800000);
    var start: usize = 0;
    while (start < count) {
        // Preserve the existing cancellation interval, including the first
        // logical element. No trailing backing capacity is inspected.
        try options.check();
        const end = start + @min(count - start, 16384);
        var i = start;
        while (end - i >= width) : (i += width) {
            const bits: Bits = @bitCast(@as(@Vector(width, f32), values[i..][0..width].*));
            if (@reduce(.Or, (bits & exponent) == exponent)) return error.NonFiniteAttentionInput;
        }
        while (i < end) : (i += 1) {
            if (!std.math.isFinite(values[i])) return error.NonFiniteAttentionInput;
        }
        start = end;
    }
}

fn expSubtractAndSum(scores: []f32, maximum: f32, minimum: f32) f32 {
    // The shared SIMD exponential flushes values below this cutoff. Preserve
    // the previous scalar behavior when a finite score can produce a tiny
    // probability: arbitrary finite V may amplify it into a visible output.
    // Masked -inf scores are excluded from minimum and do not force fallback.
    if (minimum - maximum < -87.34) {
        var sum: f32 = 0;
        for (scores) |*score| {
            score.* = @exp(score.* - maximum);
            sum += score.*;
        }
        return sum;
    }
    return primitives.expSubtractAndSum(scores, maximum);
}

const Scratch = struct {
    scores: []f32,
    c2p: []f32,
    p2c: []f32,
    q: []f32,
    k: []f32,
    v: []f32,
    qr: []f32,
    kr: []f32,
    out: []f32,
    maxima: []f32,
    sums: []f32,

    fn init(storage: []f32, workspace: Workspace, dim: usize) Scratch {
        var offset: usize = 0;
        const Take = struct {
            fn apply(all: []f32, cursor: *usize, count: usize) []f32 {
                const result = all[cursor.*..][0..count];
                cursor.* += count;
                return result;
            }
        };
        const result = Scratch{
            .scores = Take.apply(storage, &offset, workspace.query_tile * workspace.key_tile),
            .c2p = Take.apply(storage, &offset, workspace.query_tile * workspace.relative_tile),
            .p2c = Take.apply(storage, &offset, workspace.key_tile * workspace.relative_tile),
            .q = Take.apply(storage, &offset, workspace.query_tile * dim),
            .k = Take.apply(storage, &offset, workspace.key_tile * dim),
            .v = Take.apply(storage, &offset, workspace.key_tile * dim),
            .qr = Take.apply(storage, &offset, workspace.relative_tile * dim),
            .kr = Take.apply(storage, &offset, workspace.relative_tile * dim),
            .out = Take.apply(storage, &offset, workspace.query_tile * dim),
            .maxima = Take.apply(storage, &offset, workspace.query_tile),
            .sums = Take.apply(storage, &offset, workspace.query_tile),
        };
        std.debug.assert(offset == storage.len);
        return result;
    }
};

/// Returns owned FP32 output. Inputs may have trailing backing capacity;
/// only their validated logical shapes are read. Completely masked samples
/// produce zero, consistently with the portable native attention reference.
pub fn forward(allocator: std.mem.Allocator, shape: Shape, input: Input, options: Options) ![]f32 {
    const workspace = try plan(shape, options);
    try finiteInput(input.q, workspace.output_elements, options);
    try finiteInput(input.k, workspace.output_elements, options);
    try finiteInput(input.v, workspace.output_elements, options);
    if (workspace.tokens > 0) {
        try finiteInput(input.qr, workspace.relative_elements, options);
        try finiteInput(input.kr, workspace.relative_elements, options);
    }
    if (input.mask.len < workspace.tokens) return error.InvalidAttentionShape;
    const output = try allocator.alloc(f32, workspace.output_elements);
    errdefer allocator.free(output);
    if (workspace.tokens == 0) return output;
    const storage = try allocator.alloc(f32, workspace.scratch_elements);
    defer allocator.free(storage);
    const scratch = Scratch.init(storage, workspace, shape.head_dim);
    const dim = shape.head_dim;
    const hidden = workspace.hidden;
    const sequence = shape.sequence;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(dim)) * 3.0);
    for (0..shape.batch) |batch| {
        const mask = input.mask[batch * sequence ..][0..sequence];
        for (0..shape.heads) |head_index| {
            try options.check();
            const head_offset = head_index * dim;
            var query_start: usize = 0;
            while (query_start < sequence) {
                try options.check();
                const queries = @min(workspace.query_tile, sequence - query_start);
                for (0..queries) |query| {
                    const start = (batch * sequence + query_start + query) * hidden + head_offset;
                    @memcpy(scratch.q[query * dim ..][0..dim], input.q[start..][0..dim]);
                }
                @memset(scratch.maxima[0..queries], -std.math.inf(f32));
                @memset(scratch.sums[0..queries], 0);
                @memset(scratch.out[0 .. queries * dim], 0);
                var key_start: usize = 0;
                while (key_start < sequence) {
                    try options.check();
                    const keys = @min(workspace.key_tile, sequence - key_start);
                    const relative_count = queries + keys - 1;
                    const relative_start = query_start + sequence - (key_start + keys);
                    for (0..keys) |key| {
                        const start = (batch * sequence + key_start + key) * hidden + head_offset;
                        @memcpy(scratch.k[key * dim ..][0..dim], input.k[start..][0..dim]);
                        @memcpy(scratch.v[key * dim ..][0..dim], input.v[start..][0..dim]);
                    }
                    for (0..relative_count) |relative| {
                        const start = (relative_start + relative) * hidden + head_offset;
                        @memcpy(scratch.qr[relative * dim ..][0..dim], input.qr[start..][0..dim]);
                        @memcpy(scratch.kr[relative * dim ..][0..dim], input.kr[start..][0..dim]);
                    }
                    try options.transB(queries, keys, dim, scale, scratch.q[0 .. queries * dim], scratch.k[0 .. keys * dim], scratch.scores[0 .. queries * keys]);
                    try options.transB(queries, relative_count, dim, scale, scratch.q[0 .. queries * dim], scratch.kr[0 .. relative_count * dim], scratch.c2p[0 .. queries * relative_count]);
                    try options.transB(keys, relative_count, dim, scale, scratch.k[0 .. keys * dim], scratch.qr[0 .. relative_count * dim], scratch.p2c[0 .. keys * relative_count]);
                    for (0..queries) |query| {
                        const row = scratch.scores[query * keys ..][0..keys];
                        var maximum: f32 = -std.math.inf(f32);
                        var minimum: f32 = std.math.inf(f32);
                        for (row, 0..) |*value, key| {
                            if (mask[key_start + key] == 0) {
                                value.* = -std.math.inf(f32);
                                continue;
                            }
                            const relative = query + keys - 1 - key;
                            value.* += scratch.c2p[query * relative_count + relative] + scratch.p2c[key * relative_count + relative];
                            if (!std.math.isFinite(value.*)) return error.NonFiniteAttentionScore;
                            maximum = @max(maximum, value.*);
                            minimum = @min(minimum, value.*);
                        }
                        const old_maximum = scratch.maxima[query];
                        const next_maximum = @max(old_maximum, maximum);
                        if (next_maximum == -std.math.inf(f32)) {
                            @memset(row, 0);
                            continue;
                        }
                        if (scratch.sums[query] != 0) {
                            const rescale = @exp(old_maximum - next_maximum);
                            if (rescale != 1) {
                                for (scratch.out[query * dim ..][0..dim]) |*value| value.* *= rescale;
                                scratch.sums[query] *= rescale;
                            }
                        }
                        // Use the same SIMD exponential as the materialized
                        // attention path instead of a scalar expf per score.
                        const sum_exp = expSubtractAndSum(row, next_maximum, minimum);
                        scratch.maxima[query] = next_maximum;
                        scratch.sums[query] += sum_exp;
                    }
                    try options.mix(queries, dim, keys, scratch.scores[0 .. queries * keys], scratch.v[0 .. keys * dim], scratch.out[0 .. queries * dim]);
                    key_start += keys;
                }
                for (0..queries) |query| {
                    const start = (batch * sequence + query_start + query) * hidden + head_offset;
                    const row = output[start..][0..dim];
                    const sum_exp = scratch.sums[query];
                    if (sum_exp == 0) {
                        @memset(row, 0);
                    } else {
                        const inv_sum = 1.0 / sum_exp;
                        for (row, scratch.out[query * dim ..][0..dim]) |*value, accumulated| {
                            value.* = accumulated * inv_sum;
                            if (!std.math.isFinite(value.*)) return error.NonFiniteAttentionScore;
                        }
                    }
                }
                query_start += queries;
            }
        }
    }
    try options.check();
    return output;
}

fn testValues(a: std.mem.Allocator, count: usize, modulus: usize, scale: f32) ![]f32 {
    const values = try a.alloc(f32, count);
    for (values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i64, @intCast(i % modulus)) - @as(i64, @intCast(modulus / 2)))) * scale;
    return values;
}

fn scalarTestReference(a: std.mem.Allocator, shape: Shape, input: Input) ![]f32 {
    @setFloatMode(.strict);
    const hidden = shape.heads * shape.head_dim;
    const output = try a.alloc(f32, shape.batch * shape.sequence * hidden);
    errdefer a.free(output);
    const scores = try a.alloc(f32, shape.sequence);
    defer a.free(scores);
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(shape.head_dim)) * 3.0);
    for (0..shape.batch) |b| {
        for (0..shape.heads) |h| {
            for (0..shape.sequence) |q| {
                const query = (b * shape.sequence + q) * hidden + h * shape.head_dim;
                var maximum: f32 = -std.math.inf(f32);
                for (scores, 0..) |*score, k| {
                    if (input.mask[b * shape.sequence + k] == 0) {
                        score.* = -std.math.inf(f32);
                        continue;
                    }
                    const key = (b * shape.sequence + k) * hidden + h * shape.head_dim;
                    const relative = (q + shape.sequence - 1 - k) * hidden + h * shape.head_dim;
                    var cc: f32 = 0;
                    var cp: f32 = 0;
                    var pc: f32 = 0;
                    for (0..shape.head_dim) |d| {
                        cc += input.q[query + d] * input.k[key + d];
                        cp += input.q[query + d] * input.kr[relative + d];
                        pc += input.k[key + d] * input.qr[relative + d];
                    }
                    score.* = cc * scale + (cp * scale + pc * scale);
                    maximum = @max(maximum, score.*);
                }
                const out = output[query..][0..shape.head_dim];
                @memset(out, 0);
                if (maximum == -std.math.inf(f32)) continue;
                var sum: f32 = 0;
                for (scores) |*score| {
                    score.* = @exp(score.* - maximum);
                    sum += score.*;
                }
                for (scores, 0..) |score, k| {
                    const key = (b * shape.sequence + k) * hidden + h * shape.head_dim;
                    const probability = score / sum;
                    for (out, 0..) |*value, d| value.* += probability * input.v[key + d];
                }
            }
        }
    }
    return output;
}

test "tiled DeBERTa SIMD softmax matches scalar oracle across score ranges masks and tails" {
    const a = std.testing.allocator;
    for ([_]f32{ 0.0625, 2.0 }) |amplitude| {
        const shape = Shape{ .batch = 3, .sequence = 17, .heads = 2, .head_dim = 64 };
        const workspace = try plan(shape, .{});
        const q = try testValues(a, workspace.output_elements, 17, amplitude);
        defer a.free(q);
        const k = try testValues(a, workspace.output_elements, 13, amplitude);
        defer a.free(k);
        const v = try testValues(a, workspace.output_elements, 11, 0.125);
        defer a.free(v);
        const qr = try testValues(a, workspace.relative_elements, 19, amplitude);
        defer a.free(qr);
        const kr = try testValues(a, workspace.relative_elements, 23, amplitude);
        defer a.free(kr);
        var mask: [3 * 17]i64 = undefined;
        for (&mask, 0..) |*value, i| value.* = switch (i / shape.sequence) {
            0 => 1,
            1 => @intFromBool(i % 3 != 1),
            else => 0,
        };
        const input = Input{ .q = q, .k = k, .v = v, .qr = qr, .kr = kr, .mask = &mask };
        const expected = try scalarTestReference(a, shape, input);
        defer a.free(expected);
        // Both one-key-block and streaming normalization exercise SIMD tails.
        for ([_]Options{ .{}, .{ .query_tile = 7, .key_tile = 9 } }) |options| {
            const actual = try forward(a, shape, input, options);
            defer a.free(actual);
            for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        }
    }
}

test "tiled DeBERTa SIMD finite scan preserves logical bounds and cancellation chunks" {
    const a = std.testing.allocator;
    const count = 16384 + primitives.vec_len + 1;
    const values = try a.alloc(f32, count + 1);
    defer a.free(values);
    @memset(values, 0);
    // Unused backing capacity may contain arbitrary data.
    values[count] = std.math.nan(f32);
    try finiteInput(values, count, .{});
    for ([_]usize{ 0, primitives.vec_len - 1, 16383, 16384, count - 1 }) |index| {
        for ([_]f32{ std.math.nan(f32), std.math.inf(f32), -std.math.inf(f32) }) |invalid| {
            values[index] = invalid;
            try std.testing.expectError(error.NonFiniteAttentionInput, finiteInput(values, count, .{}));
        }
        values[index] = 0;
    }
    const Probe = struct {
        checks: usize = 0,
        cancel: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.cancel and self.checks == 2) return error.Cancelled;
        }
    };
    var probe = Probe{};
    const options = Options{ .control = .{ .ptr = &probe, .check_fn = Probe.check } };
    try finiteInput(values, count, options);
    try std.testing.expectEqual(@as(usize, 2), probe.checks);
    probe = .{ .cancel = true };
    try std.testing.expectError(error.Cancelled, finiteInput(values, count, options));
    try std.testing.expectEqual(@as(usize, 2), probe.checks);
    probe = .{};
    try finiteInput(values, count, options);
}

test "tiled DeBERTa SIMD softmax preserves amplified subnormal probabilities" {
    // Place the tiny probability inside a SIMD block, with a scalar tail too.
    // Accumulate the oracle in f64 so this tests exponential preservation
    // independently of the platform BLAS policy for subnormal operands.
    const width = primitives.vec_len;
    var scores = [_]f32{-std.math.inf(f32)} ** (width + 1);
    scores[0] = 0;
    scores[1] = -90;
    scores[width] = -92;
    const total = expSubtractAndSum(&scores, 0, -92);
    try std.testing.expect(scores[1] > 0);
    try std.testing.expect(scores[width] > 0);
    const large_value: f32 = 1e38;
    const actual = (@as(f64, scores[1]) + @as(f64, scores[width])) * @as(f64, large_value) / @as(f64, total);
    const expected = (@exp(@as(f64, -90)) + @exp(@as(f64, -92))) * @as(f64, large_value);
    try std.testing.expectApproxEqAbs(expected, actual, 1e-6);

    // Ordinary finite scores retain SIMD even alongside masked -inf values.
    @memset(&scores, -std.math.inf(f32));
    scores[0] = 0;
    try std.testing.expectEqual(@as(f32, 1), expSubtractAndSum(&scores, 0, 0));
    try std.testing.expectEqual(@as(f32, 1), scores[0]);
    for (scores[1..]) |score| try std.testing.expectEqual(@as(f32, 0), score);
}

test "tiled DeBERTa BLAS attention matches portable reference across ragged tiles" {
    const a = std.testing.allocator;
    for ([_]usize{ 1, 5, 131, 257 }) |sequence| {
        const shape = Shape{ .batch = 2, .sequence = sequence, .heads = 2, .head_dim = 4 };
        const workspace = try plan(shape, .{});
        const q = try testValues(a, workspace.output_elements, 17, 0.035);
        defer a.free(q);
        const k = try testValues(a, workspace.output_elements, 13, 0.027);
        defer a.free(k);
        const v = try testValues(a, workspace.output_elements, 11, 0.049);
        defer a.free(v);
        const qr = try testValues(a, workspace.relative_elements, 19, 0.013);
        defer a.free(qr);
        const kr = try testValues(a, workspace.relative_elements, 23, 0.024);
        defer a.free(kr);
        const mask = try a.alloc(i64, workspace.tokens);
        defer a.free(mask);
        for (mask, 0..) |*value, i| value.* = if (i < sequence) @intFromBool(i % 3 != 1) else 0;
        const input = Input{ .q = q, .k = k, .v = v, .qr = qr, .kr = kr, .mask = mask };
        const reference = try @import("inference_linalg").debertaDisentangledAttentionHost(a, q, k, v, qr, kr, mask, shape.batch, sequence, shape.heads, shape.head_dim);
        defer a.free(reference);
        const result = try forward(a, shape, input, .{});
        defer a.free(result);
        for (reference, result) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
        for (result[sequence * workspace.hidden ..]) |value| try std.testing.expectEqual(@as(f32, 0), value);
        if (sequence == 5) {
            const uneven = try forward(a, shape, input, .{ .query_tile = 3, .key_tile = 2 });
            defer a.free(uneven);
            for (reference, uneven) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
        }
    }
}

test "tiled DeBERTa BLAS attention workspace is bounded independently of sequence" {
    const short = try plan(.{ .batch = 1, .sequence = 256, .heads = 12, .head_dim = 64 }, .{});
    const long = try plan(.{ .batch = 2, .sequence = 16384, .heads = 12, .head_dim = 64 }, .{});
    try std.testing.expectEqual(short.scratch_bytes, long.scratch_bytes);
    try std.testing.expect(long.scratch_bytes < 600 * 1024);
    try std.testing.expectEqual(@as(usize, 2 * 16384 * 768 * 4), long.output_bytes);
    try std.testing.expectError(error.AttentionScratchLimitExceeded, plan(.{ .batch = 1, .sequence = 4096, .heads = 12, .head_dim = 64 }, .{ .max_scratch_bytes = long.scratch_bytes - 1 }));
    try std.testing.expectError(error.AttentionOutputLimitExceeded, plan(.{ .batch = 1, .sequence = 4096, .heads = 12, .head_dim = 64 }, .{ .max_output_bytes = 1 }));
    try std.testing.expectError(error.InvalidAttentionShape, plan(.{ .batch = std.math.maxInt(usize), .sequence = 2, .heads = 1, .head_dim = 1 }, .{}));
    try std.testing.expectError(error.InvalidAttentionTile, plan(.{ .batch = 1, .sequence = 5, .heads = 1, .head_dim = 4 }, .{ .query_tile = 0 }));
}

test "tiled DeBERTa BLAS attention allocations cancellation and finite validation" {
    const a = std.testing.allocator;
    const shape = Shape{ .batch = 1, .sequence = 5, .heads = 1, .head_dim = 4 };
    const workspace = try plan(shape, .{});
    const q = try testValues(a, workspace.output_elements, 17, 0.035);
    defer a.free(q);
    const relative = try testValues(a, workspace.relative_elements, 19, 0.013);
    defer a.free(relative);
    const input = Input{ .q = q, .k = q, .v = q, .qr = relative, .kr = relative, .mask = &.{ 1, 1, 0, 1, 0 } };
    const Check = struct {
        fn run(allocator: std.mem.Allocator, shape_: Shape, input_: Input) !void {
            const result = try forward(allocator, shape_, input_, .{ .query_tile = 2, .key_tile = 3 });
            defer allocator.free(result);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ shape, input });
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls >= 13) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    try std.testing.expectError(error.Cancelled, forward(a, shape, input, .{ .query_tile = 2, .key_tile = 3, .control = .{ .ptr = &cancel, .check_fn = Cancel.check } }));
    q[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteAttentionInput, forward(a, shape, input, .{}));
    q[0] = 0;
    var too_short = input;
    too_short.k = q[0..1];
    try std.testing.expectError(error.InvalidAttentionShape, forward(a, shape, too_short, .{}));
    // Both allocations fit exactly in the public plan; no sequence-square
    // scratch allocation is hidden behind the GEMM calls or online softmax.
    const memory = try a.alignedAlloc(u8, .fromByteUnits(@alignOf(f32)), workspace.output_bytes + workspace.scratch_bytes);
    defer a.free(memory);
    var bounded = std.heap.FixedBufferAllocator.init(memory);
    const result = try forward(bounded.allocator(), shape, input, .{});
    try std.testing.expectEqual(workspace.output_elements, result.len);
    // The scratch allocation was released; only the returned output remains.
    try std.testing.expectEqual(workspace.output_bytes, bounded.end_index);
}
