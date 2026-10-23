// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Experimental rotated MSE quantizer inspired by arXiv:2504.19874. Two seeded
//! signed Hadamard transforms approximate a dense random rotation; the scalar
//! codebook is Lloyd-Max for a standard Gaussian, not the exact sphere marginal.
//! This is NOT the paper's Haar rotation or unbiased QJL residual estimator.
//! Version one is distinct from legacy polar4/turbo3. Its codes, seeds, rotation
//! order and little-endian row scale are a single immutable representation.
const std = @import("std");

pub const version: u32 = 1;
pub const seeds = [2]u64{ 0x8f3f73b5cf1c9ade, 0x24d8a6511b73c409 };
// Conditional Gaussian centroids from converged Lloyd-Max iteration. They are
// data-independent. Positive/negative entries use exactly symmetric f32 bits.
pub const codebook = [16]f32{
    -2.73258957099535, -2.06901722653160, -1.61804638602210, -1.25623119734739,
    -0.94234045648714, -0.65675911853260, -0.38804829949038, -0.12839502985118,
    0.12839502985118,  0.38804829949038,  0.65675911853260,  0.94234045648714,
    1.25623119734739,  1.61804638602210,  2.06901722653160,  2.73258957099535,
};

pub fn supported(dim: usize) bool {
    return dim == 64 or dim == 128 or dim == 256 or dim == 512;
}

pub fn rowBytes(dim: usize) usize {
    return if (supported(dim)) 4 + dim / 2 else 0;
}

fn sign(round: usize, index: usize) f32 {
    var x = seeds[round] +% (@as(u64, @intCast(index)) *% 0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    return if (((x ^ (x >> 31)) & 1) == 0) 1 else -1;
}

// Seeds are part of the wire format. Cache their signs instead of repeating
// the integer hash for every coordinate of every reconstructed key row.
const sign_table: [2][512]f32 = blk: {
    @setEvalBranchQuota(100000);
    var result: [2][512]f32 = undefined;
    for (0..2) |round| for (0..512) |index| {
        result[round][index] = sign(round, index);
    };
    break :blk result;
};

fn hadamardVector(comptime dim: usize, values: *[dim]f32) void {
    // Preserve each v1 butterfly's f32 operation order and rounding. This is
    // a SIMD implementation of the same transform, not a new approximation.
    @setFloatMode(.strict);
    @setEvalBranchQuota(20000);
    const V = @Vector(dim, f32);
    var vector: V = values.*;
    const stages = comptime std.math.log2_int(usize, dim);
    inline for (0..stages) |stage| {
        const width = comptime 1 << stage;
        const indices: @Vector(dim, i32) = comptime blk: {
            var result: [dim]i32 = undefined;
            for (&result, 0..) |*index, i| index.* = @intCast(i ^ width);
            break :blk result;
        };
        const upper: @Vector(dim, bool) = comptime blk: {
            var result: [dim]bool = undefined;
            for (&result, 0..) |*high, i| high.* = i & width != 0;
            break :blk result;
        };
        const pair = @shuffle(f32, vector, undefined, indices);
        vector = @select(f32, upper, pair - vector, vector + pair);
    }
    const normalization: f32 = 1 / @sqrt(@as(f32, @floatFromInt(dim)));
    values.* = vector * @as(V, @splat(normalization));
}

fn hadamard(values: []f32) void {
    switch (values.len) {
        64 => hadamardVector(64, values[0..64]),
        128 => hadamardVector(128, values[0..128]),
        256 => hadamardVector(256, values[0..256]),
        512 => hadamardVector(512, values[0..512]),
        else => unreachable, // rotate checks the supported dimensions first.
    }
}

pub fn rotate(values: []f32, inverse: bool) !void {
    if (!supported(values.len)) return error.UnsupportedKvHeadDim;
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttention;
    for (0..2) |i| {
        const round = if (inverse) 1 - i else i;
        if (inverse) hadamard(values);
        for (values, 0..) |*value, d| value.* *= sign_table[round][d];
        if (!inverse) hadamard(values);
    }
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttention;
}

fn nearest(value: f32) u8 {
    var selected: u8 = 0;
    var error_abs = @abs(value - codebook[0]);
    for (codebook[1..], 1..) |centroid, i| {
        const distance = @abs(value - centroid);
        if (distance < error_abs) {
            error_abs = distance;
            selected = @intCast(i);
        }
    }
    return selected;
}

pub fn encode(input: []const f32, output: []u8) !void {
    if (!supported(input.len)) return error.UnsupportedKvHeadDim;
    if (output.len != rowBytes(input.len)) return error.InvalidKvRowWidth;
    var squared_norm: f64 = 0;
    for (input) |x| {
        if (!std.math.isFinite(x)) return error.NonFiniteAttention;
        squared_norm += @as(f64, x) * x;
    }
    const scale: f32 = @floatCast(@sqrt(squared_norm / @as(f64, @floatFromInt(input.len))));
    if (!std.math.isFinite(scale) or scale > maximumScale(input.len) or (scale == 0 and squared_norm != 0)) return error.InvalidQuantizedRow;
    var rotated: [512]f32 = undefined;
    for (rotated[0..input.len], input) |*out, x| out.* = if (scale == 0) 0 else x / scale;
    try rotate(rotated[0..input.len], false);
    // All checks precede publication to the caller's row buffer.
    std.mem.writeInt(u32, output[0..4], @bitCast(scale), .little);
    for (output[4..], 0..) |*out, i| {
        out.* = if (scale == 0) 0 else nearest(rotated[2 * i]) | (nearest(rotated[2 * i + 1]) << 4);
    }
}

fn readScale(input: []const u8, dim: usize) !f32 {
    if (!supported(dim)) return error.UnsupportedKvHeadDim;
    if (input.len != rowBytes(dim)) return error.InvalidKvRowWidth;
    const bits = std.mem.readInt(u32, input[0..4], .little);
    const scale: f32 = @bitCast(bits);
    if (!std.math.isFinite(scale) or bits >> 31 != 0 or scale > maximumScale(dim)) return error.InvalidQuantizedRow;
    if (scale == 0) for (input[4..]) |code| if (code != 0) return error.InvalidQuantizedRow;
    return scale;
}

fn maximumScale(dim: usize) f32 {
    // Orthogonal reconstruction is bounded by the code vector's L2 norm.
    // Keep a factor-two margin for floating-point rounding at extreme inputs.
    return std.math.floatMax(f32) / (2 * @sqrt(@as(f32, @floatFromInt(dim))) * codebook[15]);
}

fn at(input: []const u8, i: usize) f32 {
    const code = if (i & 1 == 0) input[4 + i / 2] & 15 else input[4 + i / 2] >> 4;
    return codebook[code];
}

pub fn decode(input: []const u8, output: []f32) !void {
    const scale = try readScale(input, output.len);
    var values: [512]f32 = undefined;
    for (values[0..output.len], 0..) |*value, i| value.* = if (scale == 0) 0 else at(input, i);
    try rotate(values[0..output.len], true);
    for (values[0..output.len]) |*value| {
        value.* *= scale;
        if (!std.math.isFinite(value.*)) return error.InvalidQuantizedRow;
    }
    @memcpy(output, values[0..output.len]);
}

/// Rotate a query once and reuse it for every packed row of this head. Exact
/// prefix/recent rows must still use the original, unrotated query.
pub fn prepareQuery(query: []const f32, output: []f32) !void {
    if (query.len != output.len) return error.InvalidKvRowWidth;
    if (!supported(query.len)) return error.UnsupportedKvHeadDim;
    @memcpy(output, query);
    try rotate(output, false);
}

pub fn dot(rotated_query: []const f32, row: []const u8) !f64 {
    const scale = try readScale(row, rotated_query.len);
    var result: f64 = 0;
    for (rotated_query, 0..) |q, i| {
        if (!std.math.isFinite(q)) return error.NonFiniteAttention;
        result += @as(f64, q) * at(row, i);
    }
    return result * scale;
}

test "SIMD rotation is bitwise identical to the scalar v1 transform" {
    const Scalar = struct {
        fn transform(values: []f32) void {
            var width: usize = 1;
            while (width < values.len) : (width *= 2) {
                var start: usize = 0;
                while (start < values.len) : (start += width * 2) {
                    for (0..width) |i| {
                        const a = values[start + i];
                        const b = values[start + width + i];
                        values[start + i] = a + b;
                        values[start + width + i] = a - b;
                    }
                }
            }
            const normalization = 1 / @sqrt(@as(f32, @floatFromInt(values.len)));
            for (values) |*value| value.* *= normalization;
        }

        fn apply(values: []f32, inverse: bool) !void {
            for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttention;
            for (0..2) |i| {
                const round = if (inverse) 1 - i else i;
                if (inverse) transform(values);
                for (values, 0..) |*value, d| value.* *= sign(round, d);
                if (!inverse) transform(values);
            }
            for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttention;
        }
    };
    var prng = std.Random.DefaultPrng.init(0x31a74e26);
    const random = prng.random();
    const magnitudes = [_]f32{ 1e-40, 1e-20, 0.001, 1, 1024, 1e20, 1e30 };
    var input: [512]f32 = undefined;
    var actual: [512]f32 = undefined;
    var expected: [512]f32 = undefined;
    for ([_]usize{ 64, 128, 256, 512 }) |dim| {
        for (0..262) |sample| {
            for (input[0..dim], 0..) |*value, i| {
                value.* = switch (sample) {
                    0 => if (i % 2 == 0) @as(f32, 0) else -@as(f32, 0),
                    1 => if (i % 2 == 0) @as(f32, 1) else -@as(f32, 1),
                    2 => std.math.floatMax(f32) / 4,
                    else => (random.float(f32) - 0.5) * magnitudes[(sample + i) % magnitudes.len],
                };
            }
            if (sample == 3) input[dim / 2] = std.math.nan(f32);
            if (sample == 4) input[dim / 2] = std.math.inf(f32);
            if (sample == 5) input[dim / 2] = -std.math.inf(f32);
            for ([_]bool{ false, true }) |inverse| {
                @memcpy(actual[0..dim], input[0..dim]);
                @memcpy(expected[0..dim], input[0..dim]);
                Scalar.apply(expected[0..dim], inverse) catch |err| {
                    try std.testing.expectError(err, rotate(actual[0..dim], inverse));
                    continue;
                };
                try rotate(actual[0..dim], inverse);
                try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected[0..dim]), std.mem.sliceAsBytes(actual[0..dim]));
            }
        }
    }
}

test "rotated MSE4 transform preserves norm inner products and inverse across supported dimensions" {
    inline for (.{ 64, 128, 256, 512 }) |dim| {
        var a: [dim]f32 = undefined;
        var b: [dim]f32 = undefined;
        for (&a, &b, 0..) |*x, *y, i| {
            x.* = @floatCast(@sin(@as(f64, @floatFromInt(i)) * 0.73));
            y.* = @floatCast(@cos(@as(f64, @floatFromInt(i)) * 0.21));
        }
        const original = a;
        var expected_dot: f64 = 0;
        var expected_norm: f64 = 0;
        for (a, b) |x, y| {
            expected_dot += @as(f64, x) * y;
            expected_norm += @as(f64, x) * x;
        }
        try rotate(&a, false);
        try rotate(&b, false);
        var actual_dot: f64 = 0;
        var actual_norm: f64 = 0;
        for (a, b) |x, y| {
            actual_dot += @as(f64, x) * y;
            actual_norm += @as(f64, x) * x;
        }
        try std.testing.expectApproxEqAbs(expected_dot, actual_dot, 2e-5);
        try std.testing.expectApproxEqRel(expected_norm, actual_norm, 1e-6);
        try rotate(&a, true);
        for (a, original) |actual, want| try std.testing.expectApproxEqAbs(want, actual, 1e-6);
    }
}

test "rotated MSE4 scoring matches decoded keys and has bounded empirical distortion" {
    inline for (.{ 64, 128, 256, 512 }) |dim| {
        var encoded: [rowBytes(dim)]u8 = undefined;
        var input: [dim]f32 = undefined;
        var query: [dim]f32 = undefined;
        var rotated: [dim]f32 = undefined;
        var decoded: [dim]f32 = undefined;
        for (0..16) |sample| {
            for (&input, &query, 0..) |*k, *q, i| {
                k.* = @floatCast(@sin(@as(f64, @floatFromInt(i * 17 + sample * 31))) * 0.04);
                q.* = @floatCast(@cos(@as(f64, @floatFromInt(i * 7 + sample * 11))));
            }
            try encode(&input, &encoded);
            try decode(&encoded, &decoded);
            try prepareQuery(&query, &rotated);
            var expected: f64 = 0;
            var norm: f64 = 0;
            var error_norm: f64 = 0;
            for (input, decoded, query) |k, dk, q| {
                expected += @as(f64, dk) * q;
                norm += @as(f64, k) * k;
                const delta = @as(f64, k) - dk;
                error_norm += delta * delta;
            }
            try std.testing.expectApproxEqAbs(expected, try dot(&rotated, &encoded), 2e-6);
            try std.testing.expect(error_norm / norm < 0.03);
        }
    }
}

test "rotated MSE4 rejects malformed rows and preserves zero vectors exactly" {
    const zero: [64]f32 = @splat(0);
    var row: [rowBytes(64)]u8 = undefined;
    var decoded: [64]f32 = undefined;
    try encode(&zero, &row);
    try decode(&row, &decoded);
    try std.testing.expectEqualSlices(f32, &zero, &decoded);
    row[4] = 1;
    try std.testing.expectError(error.InvalidQuantizedRow, decode(&row, &decoded));
    for ([_]f32{ -1, -0.0, std.math.floatMax(f32), std.math.inf(f32), std.math.nan(f32) }) |scale| {
        std.mem.writeInt(u32, row[0..4], @bitCast(scale), .little);
        try std.testing.expectError(error.InvalidQuantizedRow, decode(&row, &decoded));
    }
    const unsupported: [80]f32 = @splat(0);
    try std.testing.expectError(error.UnsupportedKvHeadDim, encode(&unsupported, &row));
    var invalid = zero;
    invalid[1] = std.math.nan(f32);
    @memset(&row, 0xab);
    try std.testing.expectError(error.NonFiniteAttention, encode(&invalid, &row));
    for (row) |byte| try std.testing.expectEqual(@as(u8, 0xab), byte);
}

test "rotated MSE4 v1 matches an independent float32 wire oracle" {
    // Independent Python IEEE-f32 arithmetic oracle, including every butterfly
    // rounding, the documented SplitMix signs, and the Gaussian centroids.
    var input: [512]f32 = undefined;
    for (&input, 0..) |*x, i| x.* = (@as(f32, @floatFromInt(i * 37 % 127)) - 63) / 1024;
    var row: [rowBytes(512)]u8 = undefined;
    try encode(&input, &row);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&row, &digest, .{});
    const actual = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings("d7451c962dbfccd4eeb2adace74cf4e4e7ecfe30ac556f8c8107545a56e108ad", &actual);
}
