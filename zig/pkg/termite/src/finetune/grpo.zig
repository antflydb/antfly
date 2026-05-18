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

const std = @import("std");

pub const RewardFn = *const fn (
    ctx: *anyopaque,
    prompt_idx: usize,
    completion_tokens: []const i32,
) anyerror!f32;

pub const Rewarder = struct {
    ctx: *anyopaque,
    call: RewardFn,

    pub fn score(self: Rewarder, prompt_idx: usize, tokens: []const i32) !f32 {
        return self.call(self.ctx, prompt_idx, tokens);
    }
};

pub const GRPOConfig = struct {
    group_size: usize = 8,
    clip_epsilon: f32 = 0.2,
    kl_coef: f32 = 0.04,
    advantage_eps: f32 = 1e-8,
    normalize_advantage: bool = true,
};

pub const Completion = struct {
    prompt_idx: usize,
    tokens: []const i32,
    old_logps: []const f32,
    ref_logps: []const f32,
};

pub const GroupAdvantages = struct {
    allocator: std.mem.Allocator,
    rewards: []f32,
    advantages: []f32,
    num_groups: usize,

    pub fn deinit(self: *GroupAdvantages) void {
        self.allocator.free(self.rewards);
        self.allocator.free(self.advantages);
        self.* = undefined;
    }
};

pub fn scoreGroup(
    allocator: std.mem.Allocator,
    rewarder: Rewarder,
    completions: []const Completion,
) !GroupAdvantages {
    const rewards = try allocator.alloc(f32, completions.len);
    errdefer allocator.free(rewards);
    const advantages = try allocator.alloc(f32, completions.len);
    errdefer allocator.free(advantages);

    var max_prompt: usize = 0;
    var any = false;
    for (completions, 0..) |c, i| {
        rewards[i] = try rewarder.score(c.prompt_idx, c.tokens);
        advantages[i] = 0;
        if (!any or c.prompt_idx > max_prompt) {
            max_prompt = c.prompt_idx;
            any = true;
        }
    }
    const num_groups: usize = if (any) max_prompt + 1 else 0;

    return GroupAdvantages{
        .allocator = allocator,
        .rewards = rewards,
        .advantages = advantages,
        .num_groups = num_groups,
    };
}

pub fn computeAdvantages(
    ga: *GroupAdvantages,
    completions: []const Completion,
    config: GRPOConfig,
) void {
    const n = completions.len;
    if (n == 0) return;

    var g: usize = 0;
    while (g < ga.num_groups) : (g += 1) {
        var count: usize = 0;
        var sum: f64 = 0;
        for (completions, 0..) |c, i| {
            if (c.prompt_idx == g) {
                sum += ga.rewards[i];
                count += 1;
            }
        }
        if (count == 0) continue;
        const mean: f64 = sum / @as(f64, @floatFromInt(count));

        var std_val: f64 = 0;
        if (config.normalize_advantage) {
            var var_sum: f64 = 0;
            for (completions, 0..) |c, i| {
                if (c.prompt_idx == g) {
                    const d = @as(f64, ga.rewards[i]) - mean;
                    var_sum += d * d;
                }
            }
            const variance = var_sum / @as(f64, @floatFromInt(count));
            std_val = @sqrt(variance);
        }

        for (completions, 0..) |c, i| {
            if (c.prompt_idx == g) {
                const centered = @as(f64, ga.rewards[i]) - mean;
                if (config.normalize_advantage) {
                    const denom = std_val + @as(f64, config.advantage_eps);
                    ga.advantages[i] = @floatCast(centered / denom);
                } else {
                    ga.advantages[i] = @floatCast(centered);
                }
            }
        }
    }
}

pub const GRPOLossResult = struct {
    loss: f32,
    pg_loss: f32,
    kl_loss: f32,
    clip_fraction: f32,
    grad_new_logps: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GRPOLossResult) void {
        self.allocator.free(self.grad_new_logps);
        self.* = undefined;
    }
};

pub fn grpoLoss(
    allocator: std.mem.Allocator,
    completions: []const Completion,
    new_logps: []const f32,
    advantages: []const f32,
    config: GRPOConfig,
) !GRPOLossResult {
    var total_tokens: usize = 0;
    for (completions) |c| total_tokens += c.tokens.len;

    if (new_logps.len != total_tokens) return error.LogpLenMismatch;
    if (advantages.len != completions.len) return error.AdvLenMismatch;

    const grad = try allocator.alloc(f32, total_tokens);
    errdefer allocator.free(grad);
    @memset(grad, 0);

    if (total_tokens == 0) {
        return GRPOLossResult{
            .loss = 0,
            .pg_loss = 0,
            .kl_loss = 0,
            .clip_fraction = 0,
            .grad_new_logps = grad,
            .allocator = allocator,
        };
    }

    const n_f: f32 = @floatFromInt(total_tokens);
    const inv_n: f32 = 1.0 / n_f;
    const eps = config.clip_epsilon;
    const kl = config.kl_coef;

    var pg_sum: f64 = 0;
    var kl_sum: f64 = 0;
    var clipped_count: usize = 0;

    var off: usize = 0;
    for (completions, 0..) |c, ci| {
        const adv: f32 = advantages[ci];
        var t: usize = 0;
        while (t < c.tokens.len) : (t += 1) {
            const new_lp = new_logps[off + t];
            const old_lp = c.old_logps[t];
            const ref_lp = c.ref_logps[t];

            const ratio = @exp(new_lp - old_lp);
            const pg_1 = ratio * adv;
            const clipped_ratio = std.math.clamp(ratio, 1.0 - eps, 1.0 + eps);
            const pg_2 = clipped_ratio * adv;

            // -min(pg_1, pg_2)
            const chosen = if (pg_1 < pg_2) pg_1 else pg_2;
            const pg_token = -chosen;
            pg_sum += pg_token;

            // KL k3: exp(ref - new) - (ref - new) - 1
            const diff = ref_lp - new_lp;
            const exp_diff = @exp(diff);
            const k3 = exp_diff - diff - 1.0;
            kl_sum += kl * k3;

            // Gradient w.r.t. new_lp.
            //
            // PG branch:
            //   If pg_1 <= pg_2 (unclipped chosen) OR clip is inactive,
            //   d(-pg_1)/d(new_lp) = -ratio * adv.
            //   If pg_2 < pg_1, clip binds and grad is 0.
            var g_pg: f32 = 0;
            const clip_binds = pg_2 < pg_1;
            if (clip_binds) {
                clipped_count += 1;
                g_pg = 0;
            } else {
                g_pg = -ratio * adv;
            }

            // KL branch:
            //   d k3 / d new_lp = d/d new_lp [exp(ref - new) - (ref - new) - 1]
            //                   = -exp(ref - new) + 1
            //                   = 1 - exp(ref - new)
            //   Loss contribution is +kl_coef * k3, so grad is +kl_coef * (1 - exp_diff).
            const g_kl: f32 = kl * (1.0 - exp_diff);

            grad[off + t] = (g_pg + g_kl) * inv_n;
        }
        off += c.tokens.len;
    }

    const pg_loss: f32 = @floatCast(pg_sum / @as(f64, n_f));
    const kl_loss: f32 = @floatCast(kl_sum / @as(f64, n_f));
    const loss: f32 = pg_loss + kl_loss;
    const clip_fraction: f32 = @as(f32, @floatFromInt(clipped_count)) / n_f;

    return GRPOLossResult{
        .loss = loss,
        .pg_loss = pg_loss,
        .kl_loss = kl_loss,
        .clip_fraction = clip_fraction,
        .grad_new_logps = grad,
        .allocator = allocator,
    };
}

// -------------------- tests --------------------

const testing = std.testing;

const ConstRewardCtx = struct { value: f32 };

fn constReward(
    ctx: *anyopaque,
    prompt_idx: usize,
    completion_tokens: []const i32,
) anyerror!f32 {
    _ = prompt_idx;
    _ = completion_tokens;
    const self: *ConstRewardCtx = @ptrCast(@alignCast(ctx));
    return self.value;
}

test "scoreGroup constant reward" {
    const alloc = testing.allocator;
    var ctx = ConstRewardCtx{ .value = 1.25 };
    const rewarder = Rewarder{ .ctx = &ctx, .call = constReward };

    const tokens = [_]i32{ 1, 2, 3 };
    const lp = [_]f32{ -0.1, -0.2, -0.3 };
    const comps = [_]Completion{
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
        .{ .prompt_idx = 1, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
    };

    var ga = try scoreGroup(alloc, rewarder, &comps);
    defer ga.deinit();

    try testing.expectEqual(@as(usize, 2), ga.num_groups);
    for (ga.rewards) |r| try testing.expectApproxEqAbs(@as(f32, 1.25), r, 1e-6);
}

test "computeAdvantages equal rewards -> zero" {
    const alloc = testing.allocator;
    var ctx = ConstRewardCtx{ .value = 2.0 };
    const rewarder = Rewarder{ .ctx = &ctx, .call = constReward };

    const tokens = [_]i32{1};
    const lp = [_]f32{-0.5};
    const comps = [_]Completion{
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
    };

    var ga = try scoreGroup(alloc, rewarder, &comps);
    defer ga.deinit();

    const cfg = GRPOConfig{};
    computeAdvantages(&ga, &comps, cfg);
    for (ga.advantages) |a| try testing.expectApproxEqAbs(@as(f32, 0), a, 1e-6);
}

const ArrayRewardCtx = struct { values: []const f32 };

fn arrayReward(
    ctx: *anyopaque,
    prompt_idx: usize,
    completion_tokens: []const i32,
) anyerror!f32 {
    _ = prompt_idx;
    _ = completion_tokens;
    const self: *ArrayRewardCtx = @ptrCast(@alignCast(ctx));
    // caller uses a counter stored in values... simpler: we'll return values[0] then shift.
    // Instead use index trick below.
    return self.values[0];
}

const IndexedRewardCtx = struct {
    values: []const f32,
    index: usize = 0,
};

fn indexedReward(
    ctx: *anyopaque,
    prompt_idx: usize,
    completion_tokens: []const i32,
) anyerror!f32 {
    _ = prompt_idx;
    _ = completion_tokens;
    const self: *IndexedRewardCtx = @ptrCast(@alignCast(ctx));
    const v = self.values[self.index];
    self.index += 1;
    return v;
}

test "computeAdvantages two-completion group [1,3] -> [-1,+1]" {
    const alloc = testing.allocator;
    const values = [_]f32{ 1.0, 3.0 };
    var ctx = IndexedRewardCtx{ .values = &values };
    const rewarder = Rewarder{ .ctx = &ctx, .call = indexedReward };

    const tokens = [_]i32{1};
    const lp = [_]f32{0.0};
    const comps = [_]Completion{
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
    };

    var ga = try scoreGroup(alloc, rewarder, &comps);
    defer ga.deinit();

    const cfg = GRPOConfig{};
    computeAdvantages(&ga, &comps, cfg);
    try testing.expectApproxEqAbs(@as(f32, -1.0), ga.advantages[0], 1e-4);
    try testing.expectApproxEqAbs(@as(f32, 1.0), ga.advantages[1], 1e-4);
}

test "grpoLoss zero when ratio=1, adv=0, ref=new" {
    const alloc = testing.allocator;
    const tokens = [_]i32{ 1, 2 };
    const lp = [_]f32{ -0.3, -0.7 };
    const comps = [_]Completion{
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
        .{ .prompt_idx = 0, .tokens = &tokens, .old_logps = &lp, .ref_logps = &lp },
    };
    const new_lp = [_]f32{ -0.3, -0.7, -0.3, -0.7 };
    const advs = [_]f32{ 0.0, 0.0 };

    var res = try grpoLoss(alloc, &comps, &new_lp, &advs, .{});
    defer res.deinit();

    try testing.expectApproxEqAbs(@as(f32, 0), res.loss, 1e-6);
    try testing.expectApproxEqAbs(@as(f32, 0), res.pg_loss, 1e-6);
    try testing.expectApproxEqAbs(@as(f32, 0), res.kl_loss, 1e-6);
    try testing.expectApproxEqAbs(@as(f32, 0), res.clip_fraction, 1e-6);
    for (res.grad_new_logps) |g| try testing.expectApproxEqAbs(@as(f32, 0), g, 1e-6);
}

fn lossOnly(
    alloc: std.mem.Allocator,
    comps: []const Completion,
    new_lp: []const f32,
    advs: []const f32,
    cfg: GRPOConfig,
) !f32 {
    var res = try grpoLoss(alloc, comps, new_lp, advs, cfg);
    defer res.deinit();
    return res.loss;
}

test "grpoLoss finite-difference gradient check" {
    const alloc = testing.allocator;
    const tokens0 = [_]i32{ 5, 6 };
    const tokens1 = [_]i32{ 7, 8, 9 };
    const old0 = [_]f32{ -0.4, -0.9 };
    const ref0 = [_]f32{ -0.5, -1.0 };
    const old1 = [_]f32{ -0.2, -0.8, -1.1 };
    const ref1 = [_]f32{ -0.3, -0.7, -1.2 };

    const comps = [_]Completion{
        .{ .prompt_idx = 0, .tokens = &tokens0, .old_logps = &old0, .ref_logps = &ref0 },
        .{ .prompt_idx = 0, .tokens = &tokens1, .old_logps = &old1, .ref_logps = &ref1 },
    };
    var new_lp = [_]f32{ -0.35, -0.85, -0.25, -0.82, -1.05 };
    const advs = [_]f32{ 0.7, -0.3 };

    const cfg = GRPOConfig{ .clip_epsilon = 0.5, .kl_coef = 0.1 };

    var res = try grpoLoss(alloc, &comps, &new_lp, &advs, cfg);
    defer res.deinit();

    const h: f32 = 1e-3;
    var i: usize = 0;
    while (i < new_lp.len) : (i += 1) {
        const saved = new_lp[i];
        new_lp[i] = saved + h;
        const lp = try lossOnly(alloc, &comps, &new_lp, &advs, cfg);
        new_lp[i] = saved - h;
        const lm = try lossOnly(alloc, &comps, &new_lp, &advs, cfg);
        new_lp[i] = saved;
        const num = (lp - lm) / (2.0 * h);
        try testing.expectApproxEqAbs(num, res.grad_new_logps[i], 5e-3);
    }
}
