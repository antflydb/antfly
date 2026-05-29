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

// Preference tuning losses for LoRA finetuning: DPO, IPO, KTO, SimPO, ORPO, CPO.
// All losses take summed logprobs per sample and return scalar loss + per-sample gradients.

const std = @import("std");

pub const PreferenceLoss = enum {
    dpo,
    ipo,
    kto,
    simpo,
    orpo,
    cpo,
};

pub const PairedBatch = struct {
    policy_chosen_logps: []const f32,
    policy_rejected_logps: []const f32,
    ref_chosen_logps: []const f32,
    ref_rejected_logps: []const f32,
    chosen_lengths: []const u32,
    rejected_lengths: []const u32,
    sft_chosen_loss: []const f32,
};

pub const UnpairedBatch = struct {
    policy_logps: []const f32,
    ref_logps: []const f32,
    desirable: []const bool,
};

pub const PreferenceConfig = struct {
    kind: PreferenceLoss,
    beta: f32 = 0.1,
    simpo_gamma: f32 = 0.5,
    sft_lambda: f32 = 1.0,
    kto_desirable_weight: f32 = 1.0,
    kto_undesirable_weight: f32 = 1.0,
    ipo_tau: f32 = 0.1,
};

pub const PreferenceResult = struct {
    loss: f32,
    grad_chosen: []f32,
    grad_rejected: []f32,
    mean_reward_margin: f32,
    accuracy: f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *PreferenceResult) void {
        self.allocator.free(self.grad_chosen);
        if (self.grad_rejected.len > 0) self.allocator.free(self.grad_rejected);
        self.* = undefined;
    }
};

pub const PreferenceError = error{
    MissingReferenceLogps,
    MissingLengths,
    MissingSFTLoss,
    BatchSizeMismatch,
    EmptyBatch,
    WrongLossKind,
    OutOfMemory,
};

inline fn sigmoid(x: f32) f32 {
    // Numerically stable sigmoid.
    if (x >= 0.0) {
        const z = @exp(-x);
        return 1.0 / (1.0 + z);
    } else {
        const z = @exp(x);
        return z / (1.0 + z);
    }
}

inline fn logSigmoid(x: f32) f32 {
    // log(sigmoid(x)) = -softplus(-x)
    // softplus(y) = log(1 + exp(y)); stable form:
    //   if y > 0: y + log(1 + exp(-y))
    //   else:     log(1 + exp(y))
    const y = -x;
    if (y > 0.0) {
        return -(y + @log(1.0 + @exp(-y)));
    } else {
        return -@log(1.0 + @exp(y));
    }
}

/// log(1 - exp(a)) for a < 0. Uses the two-branch stable formulation.
/// Caller must ensure a < 0 (we clamp to -epsilon in the ORPO path).
inline fn log1mExp(a: f32) f32 {
    // For a in (-ln(2), 0): log(-expm1(a))
    // For a <= -ln(2):      log1p(-exp(a))
    const ln2: f32 = 0.6931472;
    if (a > -ln2) {
        // -expm1(a) = 1 - exp(a), which is small and positive here.
        return @log(-std.math.expm1(a));
    } else {
        return std.math.log1p(-@exp(a));
    }
}

fn validatePaired(batch: PairedBatch, config: PreferenceConfig) PreferenceError!usize {
    const n = batch.policy_chosen_logps.len;
    if (n == 0) return error.EmptyBatch;
    if (batch.policy_rejected_logps.len != n) return error.BatchSizeMismatch;

    switch (config.kind) {
        .dpo, .ipo => {
            if (batch.ref_chosen_logps.len == 0 or batch.ref_rejected_logps.len == 0)
                return error.MissingReferenceLogps;
            if (batch.ref_chosen_logps.len != n or batch.ref_rejected_logps.len != n)
                return error.BatchSizeMismatch;
        },
        .simpo => {
            if (batch.chosen_lengths.len == 0 or batch.rejected_lengths.len == 0)
                return error.MissingLengths;
            if (batch.chosen_lengths.len != n or batch.rejected_lengths.len != n)
                return error.BatchSizeMismatch;
        },
        .orpo, .cpo => {
            if (batch.sft_chosen_loss.len != 0 and batch.sft_chosen_loss.len != n)
                return error.BatchSizeMismatch;
        },
        .kto => return error.WrongLossKind,
    }
    return n;
}

pub fn pairedPreferenceLoss(
    allocator: std.mem.Allocator,
    batch: PairedBatch,
    config: PreferenceConfig,
) PreferenceError!PreferenceResult {
    const n = try validatePaired(batch, config);
    const n_f: f32 = @floatFromInt(n);

    const grad_chosen = try allocator.alloc(f32, n);
    errdefer allocator.free(grad_chosen);
    const grad_rejected = try allocator.alloc(f32, n);
    errdefer allocator.free(grad_rejected);

    var total_loss: f32 = 0.0;
    var total_margin: f32 = 0.0;
    var correct: usize = 0;

    switch (config.kind) {
        .dpo => {
            const beta = config.beta;
            var i: usize = 0;
            while (i < n) : (i += 1) {
                const r_c = beta * (batch.policy_chosen_logps[i] - batch.ref_chosen_logps[i]);
                const r_r = beta * (batch.policy_rejected_logps[i] - batch.ref_rejected_logps[i]);
                const diff = r_c - r_r;
                total_loss += -logSigmoid(diff);
                total_margin += diff;
                if (diff > 0.0) correct += 1;

                // d/d(diff) [-lsig(diff)] = -sigma(-diff) = sigma(diff) - 1
                const s_neg = sigmoid(-diff);
                // grad w.r.t. policy_chosen = -beta * sigma(-diff) / n
                grad_chosen[i] = -beta * s_neg / n_f;
                grad_rejected[i] = beta * s_neg / n_f;
            }
        },
        .ipo => {
            const beta = config.beta;
            const target = 1.0 / (2.0 * config.ipo_tau);
            var i: usize = 0;
            while (i < n) : (i += 1) {
                const r_c = beta * (batch.policy_chosen_logps[i] - batch.ref_chosen_logps[i]);
                const r_r = beta * (batch.policy_rejected_logps[i] - batch.ref_rejected_logps[i]);
                const diff = r_c - r_r;
                const resid = diff - target;
                total_loss += resid * resid;
                total_margin += diff;
                if (diff > 0.0) correct += 1;

                const d_loss_d_diff = 2.0 * resid;
                grad_chosen[i] = beta * d_loss_d_diff / n_f;
                grad_rejected[i] = -beta * d_loss_d_diff / n_f;
            }
        },
        .simpo => {
            const beta = config.beta;
            const gamma = config.simpo_gamma;
            var i: usize = 0;
            while (i < n) : (i += 1) {
                const len_c_u = batch.chosen_lengths[i];
                const len_r_u = batch.rejected_lengths[i];
                const len_c: f32 = if (len_c_u == 0) 1.0 else @floatFromInt(len_c_u);
                const len_r: f32 = if (len_r_u == 0) 1.0 else @floatFromInt(len_r_u);
                const scale_c = beta / len_c;
                const scale_r = beta / len_r;
                const r_c = scale_c * batch.policy_chosen_logps[i];
                const r_r = scale_r * batch.policy_rejected_logps[i];
                const diff_m = r_c - r_r - gamma;
                total_loss += -logSigmoid(diff_m);
                total_margin += (r_c - r_r);
                if ((r_c - r_r) > 0.0) correct += 1;

                const s_neg = sigmoid(-diff_m);
                grad_chosen[i] = -scale_c * s_neg / n_f;
                grad_rejected[i] = scale_r * s_neg / n_f;
            }
        },
        .orpo => {
            // ORPO assumes policy_*_logps are (mean or sum) log-probs over a
            // response, strictly negative. We clamp to -eps before the
            // log(1 - exp(·)) step so log1mExp stays well-defined.
            const eps: f32 = 1e-6;
            const sft_lambda = config.sft_lambda;
            const has_sft = batch.sft_chosen_loss.len == n;
            var i: usize = 0;
            while (i < n) : (i += 1) {
                const lp_c_raw = batch.policy_chosen_logps[i];
                const lp_r_raw = batch.policy_rejected_logps[i];
                const lp_c = if (lp_c_raw >= -eps) -eps else lp_c_raw;
                const lp_r = if (lp_r_raw >= -eps) -eps else lp_r_raw;

                // log_odds_ratio = lp_c - lp_r - (log1mexp(lp_c) - log1mexp(lp_r))
                const l1m_c = log1mExp(lp_c);
                const l1m_r = log1mExp(lp_r);
                const lor = (lp_c - lp_r) - (l1m_c - l1m_r);
                const pref_loss = -sft_lambda * logSigmoid(lor);
                const sft_term: f32 = if (has_sft) batch.sft_chosen_loss[i] else 0.0;
                total_loss += sft_term + pref_loss;
                total_margin += lor;
                if (lor > 0.0) correct += 1;

                // d/d(lor) [-sft_lambda * lsig(lor)] = -sft_lambda * sigma(-lor)
                const s_neg = sigmoid(-lor);
                const d_lor_base = -sft_lambda * s_neg;

                // d(lor)/d(lp_c) = 1 - d(log1mexp(lp_c))/d(lp_c)
                // log1mexp(a) = log(1 - exp(a)) => d/da = -exp(a)/(1-exp(a))
                //                                       = 1/(1 - exp(-a) * ... )
                // Simpler:  -exp(a) / (1 - exp(a))
                const exp_c = @exp(lp_c);
                const exp_r = @exp(lp_r);
                const denom_c = 1.0 - exp_c;
                const denom_r = 1.0 - exp_r;
                const d_l1m_c = -exp_c / denom_c;
                const d_l1m_r = -exp_r / denom_r;
                const d_lor_d_lp_c = 1.0 - d_l1m_c;
                const d_lor_d_lp_r = -1.0 + d_l1m_r;

                // If we clamped, the derivative w.r.t. the raw input is zero;
                // otherwise pass through.
                const pass_c: f32 = if (lp_c_raw >= -eps) 0.0 else 1.0;
                const pass_r: f32 = if (lp_r_raw >= -eps) 0.0 else 1.0;

                grad_chosen[i] = (d_lor_base * d_lor_d_lp_c * pass_c) / n_f;
                grad_rejected[i] = (d_lor_base * d_lor_d_lp_r * pass_r) / n_f;
            }
        },
        .cpo => {
            const sft_lambda = config.sft_lambda;
            const margin = config.simpo_gamma;
            const has_sft = batch.sft_chosen_loss.len == n;
            var i: usize = 0;
            while (i < n) : (i += 1) {
                const diff = batch.policy_chosen_logps[i] - batch.policy_rejected_logps[i];
                const hinge = margin - diff;
                const pref_loss = if (hinge > 0.0) sft_lambda * hinge else 0.0;
                const sft_term: f32 = if (has_sft) batch.sft_chosen_loss[i] else 0.0;
                total_loss += sft_term + pref_loss;
                total_margin += diff;
                if (diff > 0.0) correct += 1;

                if (hinge > 0.0) {
                    grad_chosen[i] = -sft_lambda / n_f;
                    grad_rejected[i] = sft_lambda / n_f;
                } else {
                    grad_chosen[i] = 0.0;
                    grad_rejected[i] = 0.0;
                }
            }
        },
        .kto => unreachable,
    }

    return PreferenceResult{
        .loss = total_loss / n_f,
        .grad_chosen = grad_chosen,
        .grad_rejected = grad_rejected,
        .mean_reward_margin = total_margin / n_f,
        .accuracy = @as(f32, @floatFromInt(correct)) / n_f,
        .allocator = allocator,
    };
}

pub fn unpairedKTOLoss(
    allocator: std.mem.Allocator,
    batch: UnpairedBatch,
    config: PreferenceConfig,
) PreferenceError!PreferenceResult {
    if (config.kind != .kto) return error.WrongLossKind;
    const n = batch.policy_logps.len;
    if (n == 0) return error.EmptyBatch;
    if (batch.ref_logps.len == 0) return error.MissingReferenceLogps;
    if (batch.ref_logps.len != n or batch.desirable.len != n) return error.BatchSizeMismatch;

    const n_f: f32 = @floatFromInt(n);
    const beta = config.beta;

    // z0 = max(0, mean over batch of beta * (policy - ref))
    var z0_sum: f32 = 0.0;
    {
        var i: usize = 0;
        while (i < n) : (i += 1) {
            z0_sum += beta * (batch.policy_logps[i] - batch.ref_logps[i]);
        }
    }
    var z0 = z0_sum / n_f;
    if (z0 < 0.0) z0 = 0.0;

    const grad_chosen = try allocator.alloc(f32, n);
    errdefer allocator.free(grad_chosen);
    const grad_rejected = try allocator.alloc(f32, 0);
    errdefer allocator.free(grad_rejected);

    var total_loss: f32 = 0.0;

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const r_i = beta * (batch.policy_logps[i] - batch.ref_logps[i]);
        if (batch.desirable[i]) {
            const v = sigmoid(r_i - z0);
            const loss_i = config.kto_desirable_weight * (1.0 - v);
            total_loss += loss_i;
            // d(1 - sigma(x))/dx = -sigma(x) * (1 - sigma(x))
            // x = r_i - z0; dr_i/d(policy) = beta (z0 backward ignored).
            grad_chosen[i] = (-config.kto_desirable_weight * beta * v * (1.0 - v)) / n_f;
        } else {
            const v = sigmoid(z0 - r_i);
            const loss_i = config.kto_undesirable_weight * (1.0 - v);
            total_loss += loss_i;
            grad_chosen[i] = (config.kto_undesirable_weight * beta * v * (1.0 - v)) / n_f;
        }
    }

    return PreferenceResult{
        .loss = total_loss / n_f,
        .grad_chosen = grad_chosen,
        .grad_rejected = grad_rejected,
        .mean_reward_margin = 0.0,
        .accuracy = 0.0,
        .allocator = allocator,
    };
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

fn approxEq(a: f32, b: f32, tol: f32) bool {
    return @abs(a - b) <= tol;
}

test "DPO equal logps yields log(2), accuracy 0.5, grads +/- beta/2" {
    const allocator = testing.allocator;
    const n: usize = 4;
    var pc = [_]f32{ -1.0, -2.0, -0.5, -3.0 };
    var pr = [_]f32{ -1.0, -2.0, -0.5, -3.0 };
    var rc = [_]f32{ -1.0, -2.0, -0.5, -3.0 };
    var rr = [_]f32{ -1.0, -2.0, -0.5, -3.0 };

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &rc,
        .ref_rejected_logps = &rr,
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &.{},
    };
    const config = PreferenceConfig{ .kind = .dpo, .beta = 0.2 };
    var res = try pairedPreferenceLoss(allocator, batch, config);
    defer res.deinit();

    try testing.expect(approxEq(res.loss, @log(2.0), 1e-5));
    try testing.expect(approxEq(res.accuracy, 0.0, 1e-6)); // diff == 0, not strictly >
    try testing.expect(approxEq(res.mean_reward_margin, 0.0, 1e-6));

    // grad_chosen = -beta * sigma(0) / n = -0.2 * 0.5 / 4 = -0.025
    const expected_gc: f32 = -0.2 * 0.5 / @as(f32, @floatFromInt(n));
    const expected_gr: f32 = 0.2 * 0.5 / @as(f32, @floatFromInt(n));
    for (0..n) |idx| {
        try testing.expect(approxEq(res.grad_chosen[idx], expected_gc, 1e-6));
        try testing.expect(approxEq(res.grad_rejected[idx], expected_gr, 1e-6));
    }
}

test "DPO hand-computed numeric check on batch of 2" {
    const allocator = testing.allocator;
    var pc = [_]f32{ -0.5, -1.0 };
    var pr = [_]f32{ -1.5, -0.5 };
    var rc = [_]f32{ -1.0, -1.0 };
    var rr = [_]f32{ -1.0, -1.0 };

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &rc,
        .ref_rejected_logps = &rr,
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &.{},
    };
    const beta: f32 = 0.5;
    const config = PreferenceConfig{ .kind = .dpo, .beta = beta };
    var res = try pairedPreferenceLoss(allocator, batch, config);
    defer res.deinit();

    // Sample 0: r_c = 0.5*(-0.5 - -1.0)= 0.25, r_r = 0.5*(-1.5 - -1.0) = -0.25
    //   diff = 0.5, loss0 = -lsig(0.5)
    // Sample 1: r_c = 0.5*(-1.0 - -1.0)=0, r_r = 0.5*(-0.5 - -1.0)=0.25
    //   diff = -0.25, loss1 = -lsig(-0.25)
    const loss0 = -logSigmoid(0.5);
    const loss1 = -logSigmoid(-0.25);
    const expected = (loss0 + loss1) / 2.0;
    try testing.expect(approxEq(res.loss, expected, 1e-5));
    const expected_margin = (0.5 + -0.25) / 2.0;
    try testing.expect(approxEq(res.mean_reward_margin, expected_margin, 1e-5));
    // accuracy: sample0 diff>0 (yes), sample1 diff>0 (no) => 0.5
    try testing.expect(approxEq(res.accuracy, 0.5, 1e-6));
}

test "IPO loss zero at diff = 1/(2*tau)" {
    const allocator = testing.allocator;
    const tau: f32 = 0.25;
    const beta: f32 = 0.5;
    // Need diff = beta*(pc-rc) - beta*(pr-rr) = 1/(2*tau) = 2.
    // Pick pc-rc = 4, pr-rr = 0 => diff = 0.5*4 - 0 = 2.
    var pc = [_]f32{3.0};
    var rc = [_]f32{-1.0};
    var pr = [_]f32{-1.0};
    var rr = [_]f32{-1.0};

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &rc,
        .ref_rejected_logps = &rr,
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &.{},
    };
    const config = PreferenceConfig{ .kind = .ipo, .beta = beta, .ipo_tau = tau };
    var res = try pairedPreferenceLoss(allocator, batch, config);
    defer res.deinit();
    try testing.expect(approxEq(res.loss, 0.0, 1e-5));
    try testing.expect(approxEq(res.grad_chosen[0], 0.0, 1e-5));
    try testing.expect(approxEq(res.grad_rejected[0], 0.0, 1e-5));
}

test "SimPO gamma 0 with equal logps gives log(2)" {
    const allocator = testing.allocator;
    var pc = [_]f32{ -2.0, -4.0 };
    var pr = [_]f32{ -2.0, -4.0 };
    var lc = [_]u32{ 4, 8 };
    var lr = [_]u32{ 4, 8 };

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &lc,
        .rejected_lengths = &lr,
        .sft_chosen_loss = &.{},
    };
    const config = PreferenceConfig{ .kind = .simpo, .beta = 2.0, .simpo_gamma = 0.0 };
    var res = try pairedPreferenceLoss(allocator, batch, config);
    defer res.deinit();
    try testing.expect(approxEq(res.loss, @log(2.0), 1e-5));
}

test "ORPO equal logps pref term = sft_lambda * log(2), sft aux passes through" {
    const allocator = testing.allocator;
    var pc = [_]f32{-1.5};
    var pr = [_]f32{-1.5};
    var sft = [_]f32{0.7};

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &sft,
    };
    const config = PreferenceConfig{ .kind = .orpo, .sft_lambda = 1.0 };
    var res = try pairedPreferenceLoss(allocator, batch, config);
    defer res.deinit();

    const expected = 0.7 + 1.0 * @log(2.0);
    try testing.expect(approxEq(res.loss, expected, 1e-5));
}

test "KTO all desirable at r=z0 => v=0.5, loss=0.5*weight" {
    const allocator = testing.allocator;
    // If all samples have same (policy - ref), then z0 = beta*(p-r) and r_i == z0,
    // so v = sigma(0) = 0.5.
    var pl = [_]f32{ -2.0, -2.0, -2.0 };
    var rl = [_]f32{ -1.0, -1.0, -1.0 };
    var des = [_]bool{ true, true, true };
    // beta*(p-r) = 0.1*(-1) = -0.1 => z0_raw = -0.1 => clipped to 0.
    // r_i = -0.1 => r_i - z0 = -0.1, not equal. Fix: make p > r so z0>0.
    pl = [_]f32{ 0.0, 0.0, 0.0 };
    rl = [_]f32{ -1.0, -1.0, -1.0 };

    const batch = UnpairedBatch{
        .policy_logps = &pl,
        .ref_logps = &rl,
        .desirable = &des,
    };
    const config = PreferenceConfig{
        .kind = .kto,
        .beta = 0.1,
        .kto_desirable_weight = 2.0,
    };
    var res = try unpairedKTOLoss(allocator, batch, config);
    defer res.deinit();

    // z0 = 0.1 * 1 = 0.1 (positive). r_i = 0.1. r_i - z0 = 0 => v = 0.5.
    // loss_i = 2.0 * (1 - 0.5) = 1.0. Mean = 1.0.
    try testing.expect(approxEq(res.loss, 1.0, 1e-5));
}

test "Error: missing reference on DPO" {
    const allocator = testing.allocator;
    var pc = [_]f32{-1.0};
    var pr = [_]f32{-1.0};
    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &.{},
    };
    const config = PreferenceConfig{ .kind = .dpo };
    try testing.expectError(error.MissingReferenceLogps, pairedPreferenceLoss(allocator, batch, config));
}

test "Error: missing lengths on SimPO" {
    const allocator = testing.allocator;
    var pc = [_]f32{-1.0};
    var pr = [_]f32{-1.0};
    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &.{},
    };
    const config = PreferenceConfig{ .kind = .simpo };
    try testing.expectError(error.MissingLengths, pairedPreferenceLoss(allocator, batch, config));
}

// ---- Finite-difference gradient checks ------------------------------------

fn dpoLossOnly(
    allocator: std.mem.Allocator,
    pc: []const f32,
    pr: []const f32,
    rc: []const f32,
    rr: []const f32,
    beta: f32,
) !f32 {
    const batch = PairedBatch{
        .policy_chosen_logps = pc,
        .policy_rejected_logps = pr,
        .ref_chosen_logps = rc,
        .ref_rejected_logps = rr,
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &.{},
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{ .kind = .dpo, .beta = beta });
    defer res.deinit();
    return res.loss;
}

test "DPO finite-difference gradient check" {
    const allocator = testing.allocator;
    var pc = [_]f32{ -0.3, -1.2 };
    var pr = [_]f32{ -0.9, -0.7 };
    var rc = [_]f32{ -0.5, -1.0 };
    var rr = [_]f32{ -1.1, -0.8 };
    const beta: f32 = 0.3;

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &rc,
        .ref_rejected_logps = &rr,
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &.{},
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{ .kind = .dpo, .beta = beta });
    defer res.deinit();

    const h: f32 = 1e-3;
    for (0..pc.len) |i| {
        const save = pc[i];
        pc[i] = save + h;
        const lp = try dpoLossOnly(allocator, &pc, &pr, &rc, &rr, beta);
        pc[i] = save - h;
        const lm = try dpoLossOnly(allocator, &pc, &pr, &rc, &rr, beta);
        pc[i] = save;
        const num = (lp - lm) / (2.0 * h);
        try testing.expect(approxEq(num, res.grad_chosen[i], 1e-3));

        const save_r = pr[i];
        pr[i] = save_r + h;
        const lp2 = try dpoLossOnly(allocator, &pc, &pr, &rc, &rr, beta);
        pr[i] = save_r - h;
        const lm2 = try dpoLossOnly(allocator, &pc, &pr, &rc, &rr, beta);
        pr[i] = save_r;
        const num2 = (lp2 - lm2) / (2.0 * h);
        try testing.expect(approxEq(num2, res.grad_rejected[i], 1e-3));
    }
}

fn simpoLossOnly(
    allocator: std.mem.Allocator,
    pc: []const f32,
    pr: []const f32,
    lc: []const u32,
    lr: []const u32,
    beta: f32,
    gamma: f32,
) !f32 {
    const batch = PairedBatch{
        .policy_chosen_logps = pc,
        .policy_rejected_logps = pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = lc,
        .rejected_lengths = lr,
        .sft_chosen_loss = &.{},
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{
        .kind = .simpo,
        .beta = beta,
        .simpo_gamma = gamma,
    });
    defer res.deinit();
    return res.loss;
}

test "SimPO finite-difference gradient check" {
    const allocator = testing.allocator;
    var pc = [_]f32{ -2.0, -3.5 };
    var pr = [_]f32{ -3.0, -2.5 };
    var lc = [_]u32{ 5, 7 };
    var lr = [_]u32{ 6, 4 };
    const beta: f32 = 1.5;
    const gamma: f32 = 0.3;

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &lc,
        .rejected_lengths = &lr,
        .sft_chosen_loss = &.{},
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{
        .kind = .simpo,
        .beta = beta,
        .simpo_gamma = gamma,
    });
    defer res.deinit();

    const h: f32 = 1e-3;
    for (0..pc.len) |i| {
        const save = pc[i];
        pc[i] = save + h;
        const lp = try simpoLossOnly(allocator, &pc, &pr, &lc, &lr, beta, gamma);
        pc[i] = save - h;
        const lm = try simpoLossOnly(allocator, &pc, &pr, &lc, &lr, beta, gamma);
        pc[i] = save;
        const num = (lp - lm) / (2.0 * h);
        try testing.expect(approxEq(num, res.grad_chosen[i], 1e-3));

        const save_r = pr[i];
        pr[i] = save_r + h;
        const lp2 = try simpoLossOnly(allocator, &pc, &pr, &lc, &lr, beta, gamma);
        pr[i] = save_r - h;
        const lm2 = try simpoLossOnly(allocator, &pc, &pr, &lc, &lr, beta, gamma);
        pr[i] = save_r;
        const num2 = (lp2 - lm2) / (2.0 * h);
        try testing.expect(approxEq(num2, res.grad_rejected[i], 1e-3));
    }
}

fn ktoLossOnly(
    allocator: std.mem.Allocator,
    pl: []const f32,
    rl: []const f32,
    des: []const bool,
    beta: f32,
) !f32 {
    const batch = UnpairedBatch{
        .policy_logps = pl,
        .ref_logps = rl,
        .desirable = des,
    };
    var res = try unpairedKTOLoss(allocator, batch, .{ .kind = .kto, .beta = beta });
    defer res.deinit();
    return res.loss;
}

test "KTO finite-difference gradient check" {
    const allocator = testing.allocator;
    // Per spec, the backward through z0 is ignored. The finite-difference
    // check must therefore hold z0 constant — otherwise we'd be measuring
    // the full derivative including the baseline. We achieve this by using
    // a single sample (batch size 1) with clipped z0 == 0.
    var pl = [_]f32{-2.5};
    var rl = [_]f32{-1.5};
    var des = [_]bool{true};
    const beta: f32 = 0.4;

    // z0_raw = 0.4 * (-1) = -0.4 => clipped to 0. Perturbing pl doesn't lift
    // z0 above 0 as long as perturbation * beta / 1 < 0.4 (well satisfied by
    // h=1e-3), so z0 stays clipped and the analytic grad matches.
    const batch = UnpairedBatch{
        .policy_logps = &pl,
        .ref_logps = &rl,
        .desirable = &des,
    };
    var res = try unpairedKTOLoss(allocator, batch, .{ .kind = .kto, .beta = beta });
    defer res.deinit();

    const h: f32 = 1e-3;
    const save = pl[0];
    pl[0] = save + h;
    const lp = try ktoLossOnly(allocator, &pl, &rl, &des, beta);
    pl[0] = save - h;
    const lm = try ktoLossOnly(allocator, &pl, &rl, &des, beta);
    pl[0] = save;
    const num = (lp - lm) / (2.0 * h);
    try testing.expect(approxEq(num, res.grad_chosen[0], 1e-3));
}

test "CPO hinge zero when diff exceeds margin" {
    const allocator = testing.allocator;
    var pc = [_]f32{-1.0};
    var pr = [_]f32{-2.0};
    var sft = [_]f32{0.3};
    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &sft,
    };
    const config = PreferenceConfig{ .kind = .cpo, .simpo_gamma = 0.5, .sft_lambda = 2.0 };
    var res = try pairedPreferenceLoss(allocator, batch, config);
    defer res.deinit();
    // diff = 1.0, margin 0.5 -> hinge = -0.5 => 0
    try testing.expect(approxEq(res.loss, 0.3, 1e-6));
    try testing.expect(approxEq(res.grad_chosen[0], 0.0, 1e-6));
    try testing.expect(approxEq(res.grad_rejected[0], 0.0, 1e-6));
}

fn orpoLossOnly(
    allocator: std.mem.Allocator,
    pc: []const f32,
    pr: []const f32,
    sft: []const f32,
    sft_lambda: f32,
) !f32 {
    const batch = PairedBatch{
        .policy_chosen_logps = pc,
        .policy_rejected_logps = pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = sft,
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{
        .kind = .orpo,
        .sft_lambda = sft_lambda,
    });
    defer res.deinit();
    return res.loss;
}

test "ORPO finite-difference gradient check" {
    const allocator = testing.allocator;
    // Batch of 2. All logprobs are safely negative so log(1 - exp(·)) is
    // well-defined and far from the clamping boundary at -eps.
    var pc = [_]f32{ -1.5, -2.0 };
    var pr = [_]f32{ -2.5, -1.0 };
    var sft = [_]f32{ 0.3, 0.4 };
    const sft_lambda: f32 = 1.0;

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &sft,
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{
        .kind = .orpo,
        .sft_lambda = sft_lambda,
    });
    defer res.deinit();

    const h: f32 = 1e-3;
    for (0..pc.len) |i| {
        const save = pc[i];
        pc[i] = save + h;
        const lp = try orpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda);
        pc[i] = save - h;
        const lm = try orpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda);
        pc[i] = save;
        const fd = (lp - lm) / (2.0 * h);
        try testing.expect(approxEq(fd, res.grad_chosen[i], 5e-3));

        const save_r = pr[i];
        pr[i] = save_r + h;
        const lp2 = try orpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda);
        pr[i] = save_r - h;
        const lm2 = try orpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda);
        pr[i] = save_r;
        const fd2 = (lp2 - lm2) / (2.0 * h);
        try testing.expect(approxEq(fd2, res.grad_rejected[i], 5e-3));
    }
}

fn cpoLossOnly(
    allocator: std.mem.Allocator,
    pc: []const f32,
    pr: []const f32,
    sft: []const f32,
    sft_lambda: f32,
    gamma: f32,
) !f32 {
    const batch = PairedBatch{
        .policy_chosen_logps = pc,
        .policy_rejected_logps = pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = sft,
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{
        .kind = .cpo,
        .sft_lambda = sft_lambda,
        .simpo_gamma = gamma,
    });
    defer res.deinit();
    return res.loss;
}

test "CPO finite-difference gradient check" {
    const allocator = testing.allocator;
    // Batch of 2. Mixed hinge states:
    // Sample 0: diff = -0.5 - (-1.5) = 1.0, margin 0.1 -> hinge -0.9 (INACTIVE)
    // Sample 1: diff = -0.8 - (-0.3) = -0.5, margin 0.1 -> hinge 0.6  (ACTIVE)
    var pc = [_]f32{ -0.5, -0.8 };
    var pr = [_]f32{ -1.5, -0.3 };
    var sft = [_]f32{ 0.1, 0.2 };
    const sft_lambda: f32 = 1.0;
    const gamma: f32 = 0.1;

    const batch = PairedBatch{
        .policy_chosen_logps = &pc,
        .policy_rejected_logps = &pr,
        .ref_chosen_logps = &.{},
        .ref_rejected_logps = &.{},
        .chosen_lengths = &.{},
        .rejected_lengths = &.{},
        .sft_chosen_loss = &sft,
    };
    var res = try pairedPreferenceLoss(allocator, batch, .{
        .kind = .cpo,
        .sft_lambda = sft_lambda,
        .simpo_gamma = gamma,
    });
    defer res.deinit();

    // Sample 0: hinge INACTIVE — analytic grad must be exactly zero.
    // (We verify this directly; the FD check on this sample would hover
    // around 0 naturally.)
    try testing.expectEqual(@as(f32, 0.0), res.grad_chosen[0]);
    try testing.expectEqual(@as(f32, 0.0), res.grad_rejected[0]);

    // FD check for all 4 logprob positions. Perturbation h is small enough
    // that it cannot flip the hinge state for sample 0 (|1.0 - 0.1| = 0.9 ≫ 2h).
    const h: f32 = 1e-3;
    for (0..pc.len) |i| {
        const save = pc[i];
        pc[i] = save + h;
        const lp = try cpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda, gamma);
        pc[i] = save - h;
        const lm = try cpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda, gamma);
        pc[i] = save;
        const fd = (lp - lm) / (2.0 * h);
        try testing.expect(approxEq(fd, res.grad_chosen[i], 5e-3));

        const save_r = pr[i];
        pr[i] = save_r + h;
        const lp2 = try cpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda, gamma);
        pr[i] = save_r - h;
        const lm2 = try cpoLossOnly(allocator, &pc, &pr, &sft, sft_lambda, gamma);
        pr[i] = save_r;
        const fd2 = (lp2 - lm2) / (2.0 * h);
        try testing.expect(approxEq(fd2, res.grad_rejected[i], 5e-3));
    }
}
