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

//! Bounded CPU reference for independent attention-matching blocks.
//! Inputs must be real post-RoPE queries of the KV head's consumers. This
//! module never manufactures queries from keys, rebases positions, or folds
//! previously compacted blocks back into the fit. Metal qualification uses
//! this reference; it is deliberately not a full-cache host fallback.
const std = @import("std");
const native = @import("../../backends/native.zig");
const linalg = @import("linalg.zig");

pub const max_block_rows = 512;
// Shared across all GQA heads and donor consumers of one KV head. A 512-row
// total provided too few independent positions per consumer to constrain a
// 256-key fit. Keep the larger calibration envelope explicit and budgeted.
pub const max_query_rows = 4096;
pub const max_training_rows = max_query_rows * 3 / 4;
pub const max_validation_rows = max_query_rows / 4;
pub const max_head_dim = 512;

pub const Input = struct {
    /// Widened f16 keys from the actual KV store. No hidden quantization.
    keys: []const f32,
    values: []const f32,
    training_queries: []const f32,
    validation_queries: []const f32,
    /// Consecutive, equal-sized validation strata for GQA/shared-KV consumers.
    /// Null means one consumer. Each stratum must pass the output gate; a
    /// large-magnitude consumer must not hide another consumer's error.
    validation_group_rows: ?usize = null,
    rows: usize,
    head_dim: usize,
    /// The actual layer's attention scale, including model-specific scaling.
    scale: f32,
};

pub const Options = struct {
    /// Null preserves the original half-block reference profile. An explicit
    /// budget can retain any number of rows, including the exact source block.
    /// This is a CPU qualification option until the Metal descriptor/arena ABI
    /// supports independently sized heads; it must not be silently ignored.
    retained_rows: ?usize = null,
    key_selection: enum { rms, matching_pursuit } = .rms,
    max_iterations: usize = 128,
    mass_tolerance: f32 = 0.05,
    output_tolerance: f32 = 0.10,
    ridge: f32 = 1e-4,
};

/// Bounded periodic-refit matching pursuit. Select up to eight unused columns
/// from the current mass residual, then warm-start eight NNLS sweeps. The ordinary
/// final fit and held-out gates still decide whether the result is usable.
fn selectMatchingPursuit(probs: []const f32, train: usize, n: usize, ranked: []usize, scores: []f32, weights: []f32, residual: []f32) !void {
    @memset(scores, 0);
    @memset(weights, 0);
    @memset(residual, 1);
    var selected: usize = 0;
    while (selected < weights.len) {
        for (scores, 0..) |*score, key| {
            if (score.* == -std.math.inf(f32)) continue;
            var correlation: f64 = 0;
            for (0..train) |row| correlation += @as(f64, probs[row * n + key]) * residual[row];
            score.* = @floatCast(correlation);
        }
        const end = @min(selected + @min(@as(usize, 8), @max(@as(usize, 1), weights.len / 4)), weights.len);
        while (selected < end) : (selected += 1) {
            const winner = std.mem.indexOfMax(f32, scores);
            ranked[selected] = winner;
            scores[winner] = -std.math.inf(f32);
        }
        for (0..8) |_| for (0..selected) |j| {
            const key = ranked[j];
            var gradient: f64 = 0;
            var norm: f64 = 0;
            for (0..train) |row| {
                const p: f64 = probs[row * n + key];
                gradient += p * residual[row];
                norm += p * p;
            }
            if (norm < 1e-20) continue;
            const next: f32 = @floatCast(@max(1e-6, @as(f64, weights[j]) + gradient / norm));
            if (!std.math.isFinite(next) or next > 1e6) return error.CompactionFitDiverged;
            const delta = next - weights[j];
            weights[j] = next;
            for (0..train) |row| residual[row] -= probs[row * n + key] * delta;
        };
    }
    // A selected column can have negligible norm and never receive an NNLS
    // update. The published representation requires finite log mass even for
    // these columns. The final fit rebuilds its residual from these weights.
    for (weights) |*weight| weight.* = @max(weight.*, 1e-6);
    // Preserve key/weight correspondence when restoring original row order.
    for (1..weights.len) |j| {
        const key = ranked[j];
        const weight = weights[j];
        var at = j;
        while (at > 0 and ranked[at - 1] > key) : (at -= 1) {
            ranked[at] = ranked[at - 1];
            weights[at] = weights[at - 1];
        }
        ranked[at] = key;
        weights[at] = weight;
    }
}

pub const Report = struct {
    retained_rows: usize,
    exact: bool,
    mass_relative_max_error: f32,
    output_relative_rmse: f32,
    max_consumer_output_relative_rmse: f32,
    workspace_bytes: usize,
};

pub const Result = struct {
    allocator: std.mem.Allocator,
    keys: []f32,
    values: []f32,
    /// Add after attention scaling and any model logit transform, before
    /// softmax. Omitting this loses the original block's probability mass
    /// relative to the exact prefix, tail, and subsequently appended tokens.
    log_mass: []f32,
    source_rows: []usize,
    report: Report,

    pub fn deinit(self: *Result) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.values);
        self.allocator.free(self.log_mass);
        self.allocator.free(self.source_rows);
    }
};

pub fn workspaceBytes(rows: usize, queries: usize, dim: usize) !usize {
    return workspaceBytesFor(rows, queries, dim, rows / 2);
}

/// Conservative peak including owned results. Callers can reserve the largest
/// attempted retention budget before fitting; an exact retry allocates no fit
/// matrices. Bounded dimensions make every product below overflow-safe.
pub fn workspaceBytesFor(rows: usize, queries: usize, dim: usize, retained: usize) !usize {
    if (rows < 2 or rows > max_block_rows or rows % 2 != 0 or
        queries < 2 or queries > max_query_rows or dim == 0 or dim > max_head_dim or
        retained == 0 or retained > rows) return error.InvalidCompactionShape;
    const m = retained;
    if (m == rows) return @sizeOf(f32) * (2 * m * dim + m) + @sizeOf(usize) * m;
    // Peak upper bound, including result arrays and held-out evaluation.
    // All dimensions are checked and bounded above before multiplication.
    return @sizeOf(f32) * (queries * rows + 2 * queries * m + queries * (dim + 1) +
        2 * m * m + 2 * m * dim + 4 * rows + 2 * dim) + @sizeOf(usize) * (rows + m) +
        @sizeOf(f64) * (2 * max_head_dim + max_block_rows + 1);
}

pub fn compact(allocator: std.mem.Allocator, input: Input, options: Options) !Result {
    const n = input.rows;
    const d = input.head_dim;
    if (d == 0 or input.training_queries.len % d != 0 or input.validation_queries.len % d != 0)
        return error.InvalidCompactionShape;
    const train = input.training_queries.len / d;
    const held_out = input.validation_queries.len / d;
    const query_count = std.math.add(usize, train, held_out) catch return error.InvalidCompactionShape;
    const m = options.retained_rows orelse n / 2;
    const workspace = try workspaceBytesFor(n, query_count, d, m);
    if (train == 0 or held_out == 0 or input.keys.len != n * d or input.values.len != n * d)
        return error.InvalidCompactionShape;
    const group_rows = input.validation_group_rows orelse held_out;
    if (group_rows == 0 or group_rows > held_out or held_out % group_rows != 0) return error.InvalidCompactionShape;
    if (!std.math.isFinite(input.scale) or input.scale <= 0 or !std.math.isFinite(options.ridge) or options.ridge <= 0 or
        !std.math.isFinite(options.mass_tolerance) or options.mass_tolerance <= 0 or
        !std.math.isFinite(options.output_tolerance) or options.output_tolerance <= 0 or
        options.max_iterations == 0 or options.max_iterations > 4096) return error.InvalidCompactionOptions;
    for ([_][]const f32{ input.keys, input.values, input.training_queries, input.validation_queries }) |slice|
        for (slice) |v| {
            if (!std.math.isFinite(v)) return error.NonFiniteCompactionInput;
        };
    for (input.keys) |key| {
        if (@abs(key) > 65504) return error.CompactionFitDiverged;
        if (@as(f32, @floatCast(@as(f16, @floatCast(key)))) != key) return error.CompactionRequiresF16Keys;
    }
    if (m == n) {
        // Exact retention is a distinct outcome, not a successful lossy fit.
        // Preserve the original store bit-for-bit, including rank-deficient
        // blocks, rather than running a solver that could perturb values.
        for (input.values) |value| {
            if (@abs(value) > 65504 or @as(f32, @floatCast(@as(f16, @floatCast(value)))) != value)
                return error.CompactionRequiresF16Values;
        }
        const keys = try allocator.dupe(f32, input.keys);
        errdefer allocator.free(keys);
        const values = try allocator.dupe(f32, input.values);
        errdefer allocator.free(values);
        const log_mass = try allocator.alloc(f32, m);
        errdefer allocator.free(log_mass);
        @memset(log_mass, 0);
        const source_rows = try allocator.alloc(usize, m);
        for (source_rows, 0..) |*row, index| row.* = index;
        return .{ .allocator = allocator, .keys = keys, .values = values, .log_mass = log_mass, .source_rows = source_rows, .report = .{
            .retained_rows = m,
            .exact = true,
            .mass_relative_max_error = 0,
            .output_relative_rmse = 0,
            .max_consumer_output_relative_rmse = 0,
            .workspace_bytes = workspace,
        } };
    }
    const probs = try allocator.alloc(f32, train * n);
    defer allocator.free(probs);
    native.sgemmTransBSync(train, n, d, input.scale, input.training_queries, input.keys, 0, probs);
    try softmaxRows(probs, n);
    const mass = try allocator.alloc(f32, n);
    defer allocator.free(mass);
    @memset(mass, 0);
    for (0..train) |q| for (0..n) |k| {
        // RMS importance preserves keys needed by a minority of queries.
        // The common sqrt/train factors do not affect ranking.
        const probability = probs[q * n + k];
        mass[k] += probability * probability;
    };
    const ranked = try allocator.alloc(usize, n);
    defer allocator.free(ranked);
    const weights = try allocator.alloc(f32, m);
    defer allocator.free(weights);
    const residual = try allocator.alloc(f32, train);
    defer allocator.free(residual);
    if (options.key_selection == .matching_pursuit) {
        try selectMatchingPursuit(probs, train, n, ranked, mass, weights, residual);
    } else {
        for (ranked, 0..) |*index, i| index.* = i;
        std.mem.sort(usize, ranked, mass, struct {
            fn less(scores: []f32, a: usize, b: usize) bool {
                return if (scores[a] == scores[b]) a < b else scores[a] > scores[b];
            }
        }.less);
        std.mem.sort(usize, ranked[0..m], {}, std.sort.asc(usize));
        @memset(weights, @as(f32, @floatFromInt(n)) / @as(f32, @floatFromInt(m)));
    }
    const source_rows = try allocator.dupe(usize, ranked[0..m]);
    errdefer allocator.free(source_rows);
    const keys = try allocator.alloc(f32, m * d);
    errdefer allocator.free(keys);
    for (source_rows, 0..) |src, dst| @memcpy(keys[dst * d ..][0..d], input.keys[src * d ..][0..d]);
    const log_mass = try allocator.alloc(f32, m);
    errdefer allocator.free(log_mass);
    // B contains exp(q K_selected) / Z_original. Nonnegative least squares
    // B w ~= 1 fits partition mass rather than only within-block softmax.
    const b = try allocator.alloc(f32, train * m);
    defer allocator.free(b);
    for (0..train) |q| for (source_rows, 0..) |src, j| {
        b[q * m + j] = probs[q * n + src];
    };
    for (0..train) |q| {
        var sum: f32 = -1;
        for (0..m) |j| sum += b[q * m + j] * weights[j];
        residual[q] = sum;
    }
    // Deterministic bounded coordinate descent; validation below rejects
    // non-convergence rather than publishing a numerically invalid block.
    for (0..options.max_iterations) |_| {
        for (0..m) |j| {
            var gradient: f64 = 0;
            var norm: f64 = 0;
            for (0..train) |q| {
                const entry: f64 = b[q * m + j];
                gradient += entry * residual[q];
                norm += entry * entry;
            }
            if (norm < 1e-20) continue;
            const next: f32 = @floatCast(@max(1e-6, @as(f64, weights[j]) - gradient / norm));
            if (!std.math.isFinite(next) or next > 1e6) return error.CompactionFitDiverged;
            const delta = next - weights[j];
            weights[j] = next;
            for (0..train) |q| residual[q] += b[q * m + j] * delta;
        }
    }
    for (residual) |value| {
        if (!std.math.isFinite(value) or @abs(value) > options.mass_tolerance) return error.CompactionQualityRejected;
    }
    for (weights, log_mass) |weight, *bias| bias.* = @log(weight);
    const a = try allocator.alloc(f32, train * m);
    defer allocator.free(a);
    for (0..train) |q| {
        var z: f64 = 0;
        for (0..m) |j| z += @as(f64, b[q * m + j]) * weights[j];
        if (!std.math.isFinite(z) or z <= 0) return error.CompactionFitDiverged;
        for (0..m) |j| a[q * m + j] = @floatCast(@as(f64, b[q * m + j]) * weights[j] / z);
    }
    const targets = try allocator.alloc(f32, train * d);
    defer allocator.free(targets);
    native.sgemmSync(train, d, n, 1, probs, input.values, 0, targets);
    const gram = try allocator.alloc(f32, m * m);
    defer allocator.free(gram);
    linalg.matmulAtA(a, train, m, gram);
    const values = try allocator.alloc(f32, m * d);
    errdefer allocator.free(values);
    // Solve the correction to selected original V. This is equivalent to
    // ridge towards V, but preserves a constant block exactly when A is
    // nearly rank deficient instead of losing the small ridge term in A^T Y.
    for (source_rows, 0..) |src, dst| @memcpy(values[dst * d ..][0..d], input.values[src * d ..][0..d]);
    native.sgemmSync(train, d, m, -1, a, values, 1, targets);
    linalg.matmulAtB(a, targets, train, m, d, values);
    // Ridge towards each selected original value, not zero. This preserves
    // constant-value blocks even when duplicate keys make A rank deficient.
    var trace: f32 = 0;
    for (0..m) |i| trace += gram[i * m + i];
    const ridge = options.ridge * @max(trace / @as(f32, @floatFromInt(m)), 1e-6);
    for (0..m) |i| {
        gram[i * m + i] += ridge;
    }
    try linalg.choleskySolve(gram, values, m, d);
    // Validate the representation that will actually be stored, including
    // f16 rounding; pre-quantization quality is insufficient for publication.
    for (values, 0..) |*v, index| {
        v.* += input.values[source_rows[index / d] * d + index % d];
        if (!std.math.isFinite(v.*) or @abs(v.*) > 65504) return error.CompactionFitDiverged;
        v.* = @floatCast(@as(f16, @floatCast(v.*)));
    }
    // Independent queries must agree both in partition mass and output.
    // A standalone normalized-output test cannot detect missing mass bias.
    var original: [max_head_dim]f64 = undefined;
    var fitted: [max_head_dim]f64 = undefined;
    var squared_error: f64 = 0;
    var squared_target: f64 = 0;
    var mass_error: f64 = 0;
    var group_error: f64 = 0;
    var group_target: f64 = 0;
    var max_consumer_error: f64 = 0;
    for (0..held_out) |q| {
        const query = input.validation_queries[q * d ..][0..d];
        const original_log_z = try attention(query, input.keys, input.values, null, input.scale, original[0..d]);
        const fitted_log_z = try attention(query, keys, values, log_mass, input.scale, fitted[0..d]);
        mass_error = @max(mass_error, @abs(@exp(fitted_log_z - original_log_z) - 1));
        for (original[0..d], fitted[0..d]) |truth, fit| {
            squared_error += (truth - fit) * (truth - fit);
            squared_target += truth * truth;
            group_error += (truth - fit) * (truth - fit);
            group_target += truth * truth;
        }
        if ((q + 1) % group_rows == 0) {
            const relative = @sqrt(group_error / @max(group_target, 1e-12));
            if (!std.math.isFinite(relative)) return error.CompactionQualityRejected;
            max_consumer_error = @max(max_consumer_error, relative);
            group_error = 0;
            group_target = 0;
        }
    }
    const relative_error = @sqrt(squared_error / @max(squared_target, 1e-12));
    if (!std.math.isFinite(mass_error) or !std.math.isFinite(relative_error) or
        mass_error > options.mass_tolerance or relative_error > options.output_tolerance or
        max_consumer_error > options.output_tolerance) return error.CompactionQualityRejected;
    return .{ .allocator = allocator, .keys = keys, .values = values, .log_mass = log_mass, .source_rows = source_rows, .report = .{
        .retained_rows = m,
        .exact = false,
        .mass_relative_max_error = @floatCast(mass_error),
        .output_relative_rmse = @floatCast(relative_error),
        .max_consumer_output_relative_rmse = @floatCast(max_consumer_error),
        .workspace_bytes = workspace,
    } };
}

fn softmaxRows(matrix: []f32, cols: usize) !void {
    var offset: usize = 0;
    while (offset < matrix.len) : (offset += cols) {
        const row = matrix[offset..][0..cols];
        var maximum: f32 = -std.math.inf(f32);
        for (row) |x| {
            if (!std.math.isFinite(x)) return error.CompactionFitDiverged;
            maximum = @max(maximum, x);
        }
        var sum: f64 = 0;
        for (row) |*x| {
            x.* = @exp(x.* - maximum);
            sum += x.*;
        }
        for (row) |*x| x.* = @floatCast(x.* / sum);
    }
}

/// FP64 reference for biased attention, also used to validate append behavior.
/// Returns log partition mass and writes normalized output. Bias is row-wise.
pub fn attention(query: []const f32, keys: []const f32, values: []const f32, bias: ?[]const f32, scale: f32, output: []f64) !f64 {
    const d = query.len;
    if (d == 0 or keys.len == 0 or keys.len % d != 0 or keys.len != values.len or output.len != d) return error.InvalidCompactionShape;
    const n = keys.len / d;
    if (n > max_block_rows + 1 or (bias != null and bias.?.len != n)) return error.InvalidCompactionShape;
    var scores: [max_block_rows + 1]f64 = undefined;
    var maximum: f64 = -std.math.inf(f64);
    for (scores[0..n], 0..) |*score, i| {
        var dot: f64 = 0;
        for (query, keys[i * d ..][0..d]) |q, k| dot += @as(f64, q) * k;
        score.* = dot * scale + if (bias) |b| @as(f64, b[i]) else 0;
        if (!std.math.isFinite(score.*)) return error.CompactionFitDiverged;
        maximum = @max(maximum, score.*);
    }
    var z: f64 = 0;
    for (scores[0..n]) |*s| {
        s.* = @exp(s.* - maximum);
        z += s.*;
    }
    @memset(output, 0);
    for (scores[0..n], 0..) |s, i| for (output, values[i * d ..][0..d]) |*out, v| {
        if (!std.math.isFinite(v)) return error.CompactionFitDiverged;
        out.* += s * v / z;
    };
    return maximum + @log(z);
}

test "context attention matching preserves mass when new tokens are appended" {
    const alloc = std.testing.allocator;
    var result = try compact(alloc, .{
        .keys = &.{ 0, 0, 0, 0 },
        .values = &.{ 1, 1, 1, 1 },
        .training_queries = &.{ 0, 1, 2 },
        .validation_queries = &.{ 3, 4 },
        .rows = 4,
        .head_dim = 1,
        .scale = 1,
    }, .{});
    defer result.deinit();
    try std.testing.expectApproxEqAbs(@as(f32, @log(@as(f32, 2))), result.log_mass[0], 1e-4);
    var reference: [1]f64 = undefined;
    var fitted: [1]f64 = undefined;
    _ = try attention(&.{1}, &.{ 0, 0, 0, 0, 0 }, &.{ 1, 1, 1, 1, 0 }, null, 1, &reference);
    _ = try attention(&.{1}, &.{ result.keys[0], result.keys[1], 0 }, &.{ result.values[0], result.values[1], 0 }, &.{ result.log_mass[0], result.log_mass[1], 0 }, 1, &fitted);
    try std.testing.expectApproxEqAbs(reference[0], fitted[0], 1e-3);
    _ = try attention(&.{1}, &.{ 0, 0, 0 }, &.{ 1, 1, 0 }, null, 1, &fitted);
    try std.testing.expect(@abs(reference[0] - fitted[0]) > 0.1);
}

test "context attention matching retains keys needed by minority query classes" {
    var keys: [8 * 4]f32 = @splat(0);
    var values: [8 * 4]f32 = @splat(0);
    for (0..4) |row| {
        keys[row * 4] = 4;
        @memset(values[row * 4 ..][0..4], 1);
    }
    keys[4 * 4 + 1] = 8;
    @memset(values[4 * 4 ..][0..4], -2);
    for (5..8) |row| {
        keys[row * 4] = -8;
        keys[row * 4 + 1] = -8;
    }
    var queries: [10 * 4]f32 = @splat(0);
    for (0..9) |row| queries[row * 4] = 4;
    queries[9 * 4 + 1] = 4;
    const held = [_]f32{ 3, 0, 0, 0, 0, 3, 0, 0 };
    var result = try compact(std.testing.allocator, .{
        .keys = &keys,
        .values = &values,
        .training_queries = &queries,
        .validation_queries = &held,
        .rows = 8,
        .head_dim = 4,
        .scale = 1,
    }, .{});
    defer result.deinit();
    // Mean attention ranks all four duplicate majority keys above key 4,
    // making the minority query impossible to fit. RMS must preserve it.
    try std.testing.expect(std.mem.indexOfScalar(usize, result.source_rows, 4) != null);
    try std.testing.expect(result.report.mass_relative_max_error < 0.01);
    try std.testing.expect(result.report.output_relative_rmse < 0.01);
}

test "context matching pursuit recovers minority mass missed by RMS ranking" {
    var keys: [8 * 4]f32 = @splat(0);
    var values: [8 * 4]f32 = @splat(0);
    for (0..6) |row| {
        keys[row * 4] = 4;
        @memset(values[row * 4 ..][0..4], 1);
    }
    keys[6 * 4 + 1] = 8;
    @memset(values[6 * 4 ..][0..4], -2);
    keys[7 * 4] = -8;
    keys[7 * 4 + 1] = -8;
    var queries: [101 * 4]f32 = @splat(0);
    for (0..100) |row| queries[row * 4] = 4;
    queries[100 * 4 + 1] = 4;
    const held = [_]f32{ 3, 0, 0, 0, 0, 3, 0, 0 };
    const input = Input{ .keys = &keys, .values = &values, .training_queries = &queries, .validation_queries = &held, .rows = 8, .head_dim = 4, .scale = 1 };
    try std.testing.expectError(error.CompactionQualityRejected, compact(std.testing.allocator, input, .{}));
    var result = try compact(std.testing.allocator, input, .{ .key_selection = .matching_pursuit });
    defer result.deinit();
    try std.testing.expect(std.mem.indexOfScalar(usize, result.source_rows, 6) != null);
    try std.testing.expect(result.report.mass_relative_max_error < 0.01);
    try std.testing.expect(result.report.output_relative_rmse < 0.01);
}

test "context attention matching bounds workspace and rejects invalid input" {
    try std.testing.expect((try workspaceBytes(512, 512, 512)) < 8 * 1024 * 1024);
    try std.testing.expect((try workspaceBytes(512, max_query_rows, 512)) < 64 * 1024 * 1024);
    try std.testing.expectError(error.InvalidCompactionShape, workspaceBytes(512, max_query_rows + 1, 512));
    try std.testing.expectError(error.InvalidCompactionShape, workspaceBytes(131072, 512, 512));
    try std.testing.expectError(error.NonFiniteCompactionInput, compact(std.testing.allocator, .{
        .keys = &.{ 0, std.math.nan(f32) },
        .values = &.{ 1, 1 },
        .training_queries = &.{1},
        .validation_queries = &.{2},
        .rows = 2,
        .head_dim = 1,
        .scale = 1,
    }, .{}));
    // A query distribution disjoint from training must fail closed.
    try std.testing.expectError(error.CompactionQualityRejected, compact(std.testing.allocator, .{
        .keys = &.{ -5, 5 },
        .values = &.{ -1, 1 },
        .training_queries = &.{ -1, -2 },
        .validation_queries = &.{ 1, 2 },
        .rows = 2,
        .head_dim = 1,
        .scale = 1,
    }, .{}));
}

test "context attention matching validates the full enlarged query envelope" {
    const train: [max_training_rows]f32 = @splat(0);
    var held: [max_validation_rows]f32 = @splat(1);
    const input = Input{ .keys = &.{ 0, 0, 0, 0, 0, 0, 0, 0 }, .values = &.{ 1, 1, 1, 1, 1, 1, 1, 1 }, .training_queries = &train, .validation_queries = &held, .rows = 8, .head_dim = 1, .scale = 1 };
    var result = try compact(std.testing.allocator, input, .{ .key_selection = .matching_pursuit });
    result.deinit();
    held[held.len - 1] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteCompactionInput, compact(std.testing.allocator, input, .{}));
}

test "context attention matching allocation failures leave no candidate leaks" {
    const Probe = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var result = try compact(allocator, .{
                .keys = &.{ 0, 0, 0, 0 },
                .values = &.{ 1, 1, 1, 1 },
                .training_queries = &.{ 0, 1 },
                .validation_queries = &.{2},
                .rows = 4,
                .head_dim = 1,
                .scale = 1,
            }, .{});
            result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "context attention matching fits unequal mass and nonconstant values" {
    var result = try compact(std.testing.allocator, .{
        .keys = &.{ -1, -1, -1, 1 },
        .values = &.{ -1, -2, -3, 4 },
        .training_queries = &.{ -0.5, 0.5, 1 },
        .validation_queries = &.{ -1.5, 0, 2 },
        .rows = 4,
        .head_dim = 1,
        .scale = 1,
    }, .{});
    defer result.deinit();
    try std.testing.expectEqualSlices(usize, &.{ 0, 3 }, result.source_rows);
    try std.testing.expectApproxEqAbs(@as(f32, @log(@as(f32, 3))), result.log_mass[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0), result.log_mass[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, -2), result.values[0], 0.002);
    try std.testing.expectApproxEqAbs(@as(f32, 4), result.values[1], 0.002);
    try std.testing.expect(result.report.mass_relative_max_error < 1e-4);
    try std.testing.expect(result.report.output_relative_rmse < 0.002);
}

test "context attention matching explicit retention preserves mass and exact identity" {
    const input = Input{ .keys = &.{ 0, 0, 0, 0, 0, 0, 0, 0 }, .values = &.{ 1, 1, 1, 1, 1, 1, 1, 1 }, .training_queries = &.{ 1, 2 }, .validation_queries = &.{ 3, 4 }, .rows = 8, .head_dim = 1, .scale = 1 };
    for ([_]usize{ 1, 2, 3, 4, 6, 7, 8 }) |retained| {
        var result = try compact(std.testing.allocator, input, .{ .retained_rows = retained });
        defer result.deinit();
        try std.testing.expectEqual(retained, result.keys.len);
        try std.testing.expectEqual(retained, result.report.retained_rows);
        try std.testing.expectEqual(retained == 8, result.report.exact);
        var original: [1]f64 = undefined;
        var fitted: [1]f64 = undefined;
        const original_z = try attention(&.{9}, input.keys, input.values, null, 1, &original);
        const fitted_z = try attention(&.{9}, result.keys, result.values, result.log_mass, 1, &fitted);
        try std.testing.expectApproxEqAbs(original_z, fitted_z, 1e-6);
        try std.testing.expectApproxEqAbs(original[0], fitted[0], 1e-3);
    }
    // Exact fallback must bypass the value solve and preserve a nonconstant,
    // rank-deficient store. It still validates shapes and stored precision.
    var exact_input = input;
    exact_input.values = &.{ -4, -3, -2, -1, 0, 1, 2, 3 };
    var exact = try compact(std.testing.allocator, exact_input, .{ .retained_rows = 8 });
    defer exact.deinit();
    try std.testing.expectEqualSlices(f32, exact_input.values, exact.values);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2, 3, 4, 5, 6, 7 }, exact.source_rows);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 0, 0, 0 }, exact.log_mass);
    try std.testing.expectError(error.InvalidCompactionShape, compact(std.testing.allocator, input, .{ .retained_rows = 0 }));
    try std.testing.expectError(error.InvalidCompactionShape, compact(std.testing.allocator, input, .{ .retained_rows = 9 }));
    exact_input.values = &.{ 0.1, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectError(error.CompactionRequiresF16Values, compact(std.testing.allocator, exact_input, .{ .retained_rows = 8 }));
    for ([_]usize{ 128, 256, 384, 448, 512 }) |retained|
        try std.testing.expect((try workspaceBytesFor(512, max_query_rows, 512, retained)) < 64 * 1024 * 1024);
}

test "context attention matching adaptive and exact allocations fail atomically" {
    const Probe = struct {
        fn run(allocator: std.mem.Allocator, retained: usize) !void {
            var result = try compact(allocator, .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 1, 1, 1, 1 }, .training_queries = &.{ 0, 1 }, .validation_queries = &.{2}, .rows = 4, .head_dim = 1, .scale = 1 }, .{ .retained_rows = retained });
            result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(usize, 3)});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(usize, 4)});
}

test "context attention matching rejects a consumer hidden by aggregate output energy" {
    var input = Input{ .keys = &.{ -1, -1, -1, 1 }, .values = &.{ 0, 0, 1, -1 }, .training_queries = &.{ -0.5, 0.5, 1 }, .validation_queries = &.{ -1, 0 }, .rows = 4, .head_dim = 1, .scale = 1 };
    var aggregate = try compact(std.testing.allocator, input, .{});
    defer aggregate.deinit();
    try std.testing.expect(aggregate.report.output_relative_rmse < 0.01);
    // The second consumer's exact output is zero. Rounding its fitted 1/3
    // value to f16 fails its relative gate despite small aggregate error.
    input.validation_group_rows = 1;
    try std.testing.expectError(error.CompactionQualityRejected, compact(std.testing.allocator, input, .{}));
    input.validation_group_rows = 3;
    try std.testing.expectError(error.InvalidCompactionShape, compact(std.testing.allocator, input, .{}));
    input.validation_group_rows = 0;
    try std.testing.expectError(error.InvalidCompactionShape, compact(std.testing.allocator, input, .{}));
}
