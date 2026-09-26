// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Typed-decision soft CE and the notebook's detached, centered Gaussian
//! policy-gradient estimator. The returned logit cotangent seeds native VJPs.
const std = @import("std");
const Kind = @import("../../models/laya.zig").QuestionType;
/// Candidate-branch packing admits up to 255 options per question.
const max_width = @import("../../models/laya.zig").max_packed_options;
pub const Config = struct {
    group_size: usize = 4,
    sigma: f32 = 0.4,
    rl_weight: f32 = 1,
    ce_weight: f32 = 1,
};
pub const Row = struct { kind: Kind, target: []const f32 };
pub const Result = struct {
    loss: f32,
    ce: f32,
    policy: f32,
    reward: f32,
    gradient: []f32,
    pub fn deinit(self: Result, a: std.mem.Allocator) void {
        a.free(self.gradient);
    }
};

pub fn validateTarget(kind: Kind, target: []const f32) !void {
    if (target.len < 2 or target.len > max_width or (kind == .noul and target.len != 2)) return error.InvalidLayaTarget;
    var sum: f64 = 0;
    for (target) |p| {
        if (!std.math.isFinite(p) or p < 0 or p > 1) return error.InvalidLayaTarget;
        sum += p;
    }
    if (@abs(sum - 1) > 1e-5) return error.InvalidLayaTarget;
}

fn probabilities(logits: []const f32, output: []f64) !f64 {
    var max: f64 = -std.math.inf(f64);
    for (logits) |z| {
        if (!std.math.isFinite(z)) return error.NonFiniteLayaLogits;
        max = @max(max, z);
    }
    var sum: f64 = 0;
    for (logits, output) |z, *p| {
        p.* = @exp(@as(f64, z) - max);
        sum += p.*;
    }
    for (output) |*p| p.* /= sum;
    return max + @log(sum);
}

pub fn properReward(kind: Kind, p: []const f64, target: []const f32) f64 {
    var log_score: f64 = 0;
    var dot: f64 = 0;
    var norm: f64 = 0;
    var cdf: f64 = 0;
    var rps: f64 = 0;
    for (p, target) |q, t| {
        log_score += t * @max(@log(@max(q, 1e-12)), -9.21);
        dot += t * q;
        norm += q * q;
        cdf += q - t;
        rps += cdf * cdf;
    }
    return log_score + 0.75 * dot / @max(@sqrt(norm), 1e-9) -
        (if (kind == .score) rps / @as(f64, @floatFromInt(p.len - 1)) else 0);
}

/// noise is G*B*K independent standard normal draws, shared with an oracle
/// when testing. Invalid padded option entries are ignored and have zero VJP.
pub fn evaluate(a: std.mem.Allocator, cfg: Config, rows: []const Row, width: usize, logits: []const f32, noise: []const f32) !Result {
    if (rows.len == 0 or rows.len > 512 or width < 2 or width > max_width or logits.len != rows.len * width or
        cfg.group_size < 2 or cfg.group_size > 64 or !std.math.isFinite(cfg.sigma) or cfg.sigma <= 0 or
        !std.math.isFinite(cfg.rl_weight) or cfg.rl_weight < 0 or !std.math.isFinite(cfg.ce_weight) or cfg.ce_weight < 0 or
        cfg.rl_weight + cfg.ce_weight == 0 or (cfg.rl_weight > 0 and noise.len != cfg.group_size * logits.len))
        return error.InvalidLayaObjective;
    for (rows) |row| {
        try validateTarget(row.kind, row.target);
        if (row.target.len > width) return error.InvalidLayaTarget;
    }
    const grad = try a.alloc(f32, logits.len);
    errdefer a.free(grad);
    @memset(grad, 0);
    const batch: f64 = @floatFromInt(rows.len);
    var ce: f64 = 0;
    var pbuf: [max_width]f64 = undefined;
    for (rows, 0..) |row, r| {
        const z = logits[r * width ..][0..row.target.len];
        const p = pbuf[0..row.target.len];
        const logsum = try probabilities(z, p);
        for (z, p, row.target, 0..) |value, q, t, k| {
            ce -= t * (@as(f64, value) - logsum) / batch;
            grad[r * width + k] = @floatCast(cfg.ce_weight * (q - t) / batch);
        }
    }
    if (cfg.rl_weight == 0) {
        const loss: f32 = @floatCast(cfg.ce_weight * ce);
        if (!std.math.isFinite(loss)) return error.NonFiniteLayaLoss;
        for (grad) |g| if (!std.math.isFinite(g)) return error.NonFiniteLayaGradient;
        return .{ .loss = loss, .ce = @floatCast(ce), .policy = 0, .reward = 0, .gradient = grad };
    }
    const samples = cfg.group_size * rows.len;
    const advantages = try a.alloc(f64, samples);
    defer a.free(advantages);
    const eps = try a.alloc(f64, noise.len);
    defer a.free(eps);
    @memset(eps, 0);
    var rewards: f64 = 0;
    for (0..cfg.group_size) |g| for (rows, 0..) |row, r| {
        const offset = g * logits.len + r * width;
        var mean: f64 = 0;
        for (noise[offset..][0..row.target.len]) |v| {
            if (!std.math.isFinite(v)) return error.InvalidLayaNoise;
            mean += v;
        }
        mean /= @as(f64, @floatFromInt(row.target.len));
        var z: [max_width]f32 = undefined;
        for (0..row.target.len) |k| {
            eps[offset + k] = cfg.sigma * (@as(f64, noise[offset + k]) - mean);
            z[k] = @floatCast(@as(f64, logits[r * width + k]) + eps[offset + k]);
        }
        const p = pbuf[0..row.target.len];
        _ = try probabilities(z[0..row.target.len], p);
        const reward = properReward(row.kind, p, row.target);
        advantages[g * rows.len + r] = reward;
        rewards += reward;
    };
    // Notebook centers per example, then uses one unbiased std over G*B.
    for (0..rows.len) |r| {
        var mean: f64 = 0;
        for (0..cfg.group_size) |g| mean += advantages[g * rows.len + r];
        mean /= @as(f64, @floatFromInt(cfg.group_size));
        for (0..cfg.group_size) |g| advantages[g * rows.len + r] -= mean;
    }
    var sq: f64 = 0;
    var mean: f64 = 0;
    for (advantages) |v| mean += v;
    mean /= @as(f64, @floatFromInt(samples));
    for (advantages) |v| sq += (v - mean) * (v - mean);
    const deviation = @sqrt(sq / @as(f64, @floatFromInt(samples - 1))) + 1e-6;
    const denominator = @as(f64, cfg.sigma) * cfg.sigma * @as(f64, @floatFromInt(samples));
    var policy: f64 = 0;
    for (0..cfg.group_size) |g| for (rows, 0..) |row, r| {
        const advantage = advantages[g * rows.len + r] / deviation;
        const offset = g * logits.len + r * width;
        for (0..row.target.len) |k| {
            const e = eps[offset + k];
            policy += advantage * e * e / (2 * denominator);
            grad[r * width + k] += @floatCast(-cfg.rl_weight * advantage * e / denominator);
        }
    };
    const loss: f32 = @floatCast(cfg.ce_weight * ce + cfg.rl_weight * policy);
    if (!std.math.isFinite(loss)) return error.NonFiniteLayaLoss;
    for (grad) |g| if (!std.math.isFinite(g)) return error.NonFiniteLayaGradient;
    return .{ .loss = loss, .ce = @floatCast(ce), .policy = @floatCast(policy), .reward = @floatCast(rewards / @as(f64, @floatFromInt(samples))), .gradient = grad };
}

test "laya training soft CE has stable loss and zero padded cotangents" {
    const rows = [_]Row{ .{ .kind = .choice, .target = &.{ 0.2, 0.3, 0.5 } }, .{ .kind = .noul, .target = &.{ 0.1, 0.9 } } };
    const logits = [_]f32{ 1000, 999, 1001, -1000, -1001, std.math.nan(f32) };
    const a = std.testing.allocator;
    const result = try evaluate(a, .{ .rl_weight = 0 }, &rows, 3, &logits, &.{});
    defer result.deinit(a);
    try std.testing.expect(std.math.isFinite(result.loss));
    try std.testing.expectEqual(@as(f32, 0), result.gradient[5]);
    for (0..5) |i| {
        var perturbed = logits;
        perturbed[i] += 0.125;
        const plus = try evaluate(a, .{ .rl_weight = 0 }, &rows, 3, &perturbed, &.{});
        defer plus.deinit(a);
        perturbed[i] -= 0.25;
        const minus = try evaluate(a, .{ .rl_weight = 0 }, &rows, 3, &perturbed, &.{});
        defer minus.deinit(a);
        try std.testing.expectApproxEqAbs(result.gradient[i], (plus.loss - minus.loss) / 0.25, 0.0002);
    }
    try std.testing.expectError(error.InvalidLayaTarget, validateTarget(.choice, &.{ 0, 0 }));
    try std.testing.expectError(error.InvalidLayaTarget, validateTarget(.noul, &.{ 0.2, 0.3, 0.5 }));
}
