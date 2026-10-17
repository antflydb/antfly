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

//! Independent FP64 oracle for the GPU asymmetric Matryoshka objective.
//! This implementation is for differential tests; the trainer uses resident
//! CUDA loss/embedding gradients and never copies encoder activations here.
const std = @import("std");

pub const Options = struct {
    temperature: f64 = 0.05,
    dimensions: []const usize = &.{ 128, 256, 512, 768 },
    norm_epsilon: f64 = 1e-12,
};
pub const Result = struct {
    allocator: std.mem.Allocator,
    loss: f64,
    per_dimension: []f64,
    query_grad: []f64,
    candidate_grad: []f64,

    pub fn deinit(self: *Result) void {
        self.allocator.free(self.per_dimension);
        self.allocator.free(self.query_grad);
        self.allocator.free(self.candidate_grad);
        self.* = undefined;
    }
};

/// Candidate pool is positives followed by optional explicit negatives.
/// `allowed[q*C+c] == 0` masks a known alternate positive/duplicate. The
/// designated positive must remain allowed, and every row needs a negative.
pub fn evaluate(
    allocator: std.mem.Allocator,
    queries: []const f64,
    candidates: []const f64,
    width: usize,
    positive_indices: []const usize,
    allowed: []const u8,
    options: Options,
) !Result {
    const nq = positive_indices.len;
    if (width == 0 or width > 768 or nq == 0 or nq > 1024 or queries.len != nq * width or
        candidates.len == 0 or candidates.len % width != 0 or options.dimensions.len == 0 or options.dimensions.len > 4)
        return error.InvalidContrastiveShape;
    const nc = candidates.len / width;
    if (nc > 17408 or allowed.len != nq * nc) return error.InvalidContrastiveShape;
    if (!std.math.isFinite(options.temperature) or options.temperature <= 0 or
        !std.math.isFinite(options.norm_epsilon) or options.norm_epsilon <= 0)
        return error.InvalidContrastiveOptions;
    for (options.dimensions, 0..) |d, i| {
        if (d == 0 or d > width) return error.InvalidMatryoshkaDimension;
        for (options.dimensions[0..i]) |prior| if (prior == d) return error.DuplicateMatryoshkaDimension;
    }
    for (queries) |v| if (!std.math.isFinite(v)) return error.NonFiniteEmbedding;
    for (candidates) |v| if (!std.math.isFinite(v)) return error.NonFiniteEmbedding;
    for (positive_indices, 0..) |p, q| {
        if (p >= nc or allowed[q * nc + p] != 1) return error.MaskedContrastivePositive;
        var count: usize = 0;
        for (allowed[q * nc ..][0..nc]) |value| {
            if (value > 1) return error.InvalidContrastiveMask;
            count += value;
        }
        if (count < 2) return error.ContrastiveBatchHasNoNegatives;
    }
    var result = Result{
        .allocator = allocator,
        .loss = 0,
        .per_dimension = try allocator.alloc(f64, options.dimensions.len),
        .query_grad = undefined,
        .candidate_grad = undefined,
    };
    errdefer allocator.free(result.per_dimension);
    result.query_grad = try allocator.alloc(f64, queries.len);
    errdefer allocator.free(result.query_grad);
    result.candidate_grad = try allocator.alloc(f64, candidates.len);
    errdefer allocator.free(result.candidate_grad);
    @memset(result.per_dimension, 0);
    @memset(result.query_grad, 0);
    @memset(result.candidate_grad, 0);
    const qhat = try allocator.alloc(f64, queries.len);
    defer allocator.free(qhat);
    const chat = try allocator.alloc(f64, candidates.len);
    defer allocator.free(chat);
    const qinv = try allocator.alloc(f64, nq);
    defer allocator.free(qinv);
    const cinv = try allocator.alloc(f64, nc);
    defer allocator.free(cinv);
    const qg = try allocator.alloc(f64, queries.len);
    defer allocator.free(qg);
    const cg = try allocator.alloc(f64, candidates.len);
    defer allocator.free(cg);
    const logits = try allocator.alloc(f64, nc);
    defer allocator.free(logits);
    const scale = 1 / options.temperature;
    const weight = 1 / @as(f64, @floatFromInt(nq * options.dimensions.len));
    for (options.dimensions, 0..) |d, di| {
        @memset(qg, 0);
        @memset(cg, 0);
        try normalize(queries, qhat, qinv, width, d, options.norm_epsilon);
        try normalize(candidates, chat, cinv, width, d, options.norm_epsilon);
        for (positive_indices, 0..) |positive, q| {
            var maximum: f64 = -std.math.inf(f64);
            for (0..nc) |c| {
                var dot: f64 = 0;
                for (0..d) |k| dot += qhat[q * width + k] * chat[c * width + k];
                logits[c] = dot * scale;
                if (allowed[q * nc + c] == 1) maximum = @max(maximum, logits[c]);
            }
            var exp_sum: f64 = 0;
            for (0..nc) |c| if (allowed[q * nc + c] == 1) {
                exp_sum += @exp(logits[c] - maximum);
            };
            const row_loss = maximum + @log(exp_sum) - logits[positive];
            result.per_dimension[di] += row_loss / @as(f64, @floatFromInt(nq));
            result.loss += row_loss * weight;
            for (0..nc) |c| {
                if (allowed[q * nc + c] == 0) continue;
                const probability = @exp(logits[c] - maximum) / exp_sum;
                const coefficient = (probability - @as(f64, if (c == positive) 1 else 0)) * scale * weight;
                for (0..d) |k| {
                    qg[q * width + k] += coefficient * chat[c * width + k];
                    cg[c * width + k] += coefficient * qhat[q * width + k];
                }
            }
        }
        normalizationVjp(qhat, qg, qinv, result.query_grad, width, d);
        normalizationVjp(chat, cg, cinv, result.candidate_grad, width, d);
    }
    if (!std.math.isFinite(result.loss)) return error.NonFiniteContrastiveLoss;
    return result;
}

fn normalize(source: []const f64, dest: []f64, inverses: []f64, width: usize, dim: usize, epsilon: f64) !void {
    for (inverses, 0..) |*inverse, row| {
        var squared: f64 = 0;
        for (0..dim) |k| squared += source[row * width + k] * source[row * width + k];
        if (squared < epsilon * epsilon or !std.math.isFinite(squared)) return error.DegenerateEmbedding;
        inverse.* = 1 / @sqrt(squared);
        for (0..dim) |k| dest[row * width + k] = source[row * width + k] * inverse.*;
    }
}

fn normalizationVjp(normalized: []const f64, gradient: []const f64, inverses: []const f64, output: []f64, width: usize, dim: usize) void {
    for (inverses, 0..) |inverse, row| {
        var dot: f64 = 0;
        for (0..dim) |k| dot += normalized[row * width + k] * gradient[row * width + k];
        for (0..dim) |k| output[row * width + k] += inverse * (gradient[row * width + k] - normalized[row * width + k] * dot);
    }
}

test "EG2 asymmetric MRL objective gradients match central differences" {
    const a = std.testing.allocator;
    var q = [_]f64{ 0.3, -0.7, 1.1, 0.5, -0.2, 0.9, -0.4, 0.7 };
    var c = [_]f64{ 0.7, -0.3, 0.5, 0.1, -0.4, 0.8, -0.6, 0.2, 0.9, 0.1, -0.7, 0.3 };
    const targets = [_]usize{ 0, 1 };
    const mask = [_]u8{ 1, 1, 1, 1, 1, 1 };
    const options = Options{ .temperature = 0.2, .dimensions = &.{ 2, 4 } };
    var result = try evaluate(a, &q, &c, 4, &targets, &mask, options);
    defer result.deinit();
    const h = 1e-5;
    for (0..q.len) |i| {
        const original = q[i];
        q[i] = original + h;
        var plus = try evaluate(a, &q, &c, 4, &targets, &mask, options);
        defer plus.deinit();
        q[i] = original - h;
        var minus = try evaluate(a, &q, &c, 4, &targets, &mask, options);
        defer minus.deinit();
        q[i] = original;
        try std.testing.expectApproxEqAbs(result.query_grad[i], (plus.loss - minus.loss) / (2 * h), 1e-8);
    }
    for (0..c.len) |i| {
        const original = c[i];
        c[i] = original + h;
        var plus = try evaluate(a, &q, &c, 4, &targets, &mask, options);
        defer plus.deinit();
        c[i] = original - h;
        var minus = try evaluate(a, &q, &c, 4, &targets, &mask, options);
        defer minus.deinit();
        c[i] = original;
        try std.testing.expectApproxEqAbs(result.candidate_grad[i], (plus.loss - minus.loss) / (2 * h), 1e-8);
    }
}

test "EG2 objective masks alternate positives and explicit negatives get gradients" {
    var r = try evaluate(std.testing.allocator, &.{ 1, 0 }, &.{ 1, 0, 1, 0, 0, 1 }, 2, &.{0}, &.{ 1, 0, 1 }, .{ .temperature = 0.5, .dimensions = &.{2} });
    defer r.deinit();
    try std.testing.expectApproxEqAbs(@log(1 + @exp(@as(f64, -2))), r.loss, 1e-12);
    try std.testing.expectEqual(@as(f64, 0), r.candidate_grad[2]);
    try std.testing.expectEqual(@as(f64, 0), r.candidate_grad[3]);
    try std.testing.expect(r.candidate_grad[4] != 0);
    try std.testing.expectError(error.ContrastiveBatchHasNoNegatives, evaluate(std.testing.allocator, &.{ 1, 0 }, &.{ 1, 0 }, 2, &.{0}, &.{1}, .{ .dimensions = &.{2} }));
}
