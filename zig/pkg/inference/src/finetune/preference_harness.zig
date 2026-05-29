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

//! Model-agnostic DPO/GRPO training harness.
//!
//! Wires `preference_loss` (DPO/IPO/KTO/SimPO/ORPO/CPO) and `grpo` into a
//! per-step training driver that is architecture-agnostic: the caller supplies
//! forward/sampling callbacks and the harness does the collation, dispatch,
//! and bookkeeping. The boundary is at the scalar (or per-token) logprob
//! level, so the harness knows nothing about model internals. Gradients on
//! logprobs are returned verbatim for the caller to propagate through their
//! own backward pass and step their own optimizer.

const std = @import("std");
const preference_loss = @import("preference_loss.zig");
const grpo = @import("grpo.zig");

// ============================================================================
// PART A: DPO-family (paired) harness
// ============================================================================

/// Callback signature for computing per-sequence summed logprobs under a model.
///
/// Given a batch of prompts + completions, fill `out_logps[i]` with the sum of
/// token logprobs of `completion_tokens[i]` under the caller's model (policy or
/// reference). The harness does NOT know about the model internals; it only
/// sees the scalar summed logprob per sequence. `ctx` is an opaque user pointer.
pub const LogprobFn = *const fn (
    ctx: *anyopaque,
    prompts: []const []const i32,
    completion_tokens: []const []const i32,
    out_logps: []f32,
) anyerror!void;

pub const ModelForward = struct {
    ctx: *anyopaque,
    call: LogprobFn,

    pub fn invoke(
        self: ModelForward,
        prompts: []const []const i32,
        completions: []const []const i32,
        out_logps: []f32,
    ) !void {
        return self.call(self.ctx, prompts, completions, out_logps);
    }
};

/// One DPO-style (paired) training sample. `sft_chosen_loss` is consulted
/// only for ORPO/CPO; set to null otherwise.
pub const PreferenceSample = struct {
    prompt_tokens: []const i32,
    chosen_tokens: []const i32,
    rejected_tokens: []const i32,
    sft_chosen_loss: ?f32 = null,
};

pub const HarnessConfig = struct {
    pref: preference_loss.PreferenceConfig,
    batch_size: usize = 4,
    lr: f32 = 1e-5,
    /// If true, the reference model is assumed to be the policy model with
    /// LoRA adapters disabled (the adapter-disable trick). When this is set,
    /// `pairedStep` is called with `ref_opt = null` and the harness invokes
    /// `policy.invoke` a second time; the user's `ctx` is expected to toggle
    /// adapter state between calls (e.g. via a flag flipped inside the
    /// callback). If false, the user must pass a separate `ref` ModelForward.
    /// Reference-free losses (simpo/orpo/cpo) skip the reference call
    /// entirely regardless of this flag.
    reference_from_disabled_adapter: bool = true,
};

pub const HarnessStepResult = struct {
    loss: f32,
    mean_reward_margin: f32,
    accuracy: f32,
    /// Per-sample policy grad on chosen logprob (length = batch).
    grad_policy_chosen: []f32,
    grad_policy_rejected: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *HarnessStepResult) void {
        self.allocator.free(self.grad_policy_chosen);
        if (self.grad_policy_rejected.len > 0) self.allocator.free(self.grad_policy_rejected);
        self.* = undefined;
    }
};

fn needsReference(kind: preference_loss.PreferenceLoss) bool {
    return switch (kind) {
        .dpo, .ipo, .kto => true,
        .simpo, .orpo, .cpo => false,
    };
}

/// Run one paired DPO/IPO/SimPO/ORPO/CPO training step.
///
/// KTO is not supported here because it is unpaired; callers pass a KTO config
/// and get `error.UseUnpairedStep`. Use `unpairedStep` for KTO.
///
/// The caller is responsible for propagating `grad_policy_chosen/rejected`
/// through its own backward pass and stepping its own optimizer. This harness
/// does NOT step any optimizer internally — the boundary is deliberately at
/// the logprob level so the harness is model-architecture-agnostic.
pub fn pairedStep(
    allocator: std.mem.Allocator,
    policy: ModelForward,
    ref_opt: ?ModelForward,
    samples: []const PreferenceSample,
    config: HarnessConfig,
) !HarnessStepResult {
    if (samples.len == 0) return error.EmptyBatch;
    if (config.pref.kind == .kto) return error.UseUnpairedStep;

    const n = samples.len;

    // Collate into prompts + chosen / rejected arrays.
    const prompts = try allocator.alloc([]const i32, n);
    defer allocator.free(prompts);
    const chosens = try allocator.alloc([]const i32, n);
    defer allocator.free(chosens);
    const rejecteds = try allocator.alloc([]const i32, n);
    defer allocator.free(rejecteds);

    const chosen_lengths = try allocator.alloc(u32, n);
    defer allocator.free(chosen_lengths);
    const rejected_lengths = try allocator.alloc(u32, n);
    defer allocator.free(rejected_lengths);

    for (samples, 0..) |s, i| {
        prompts[i] = s.prompt_tokens;
        chosens[i] = s.chosen_tokens;
        rejecteds[i] = s.rejected_tokens;
        chosen_lengths[i] = @intCast(s.chosen_tokens.len);
        rejected_lengths[i] = @intCast(s.rejected_tokens.len);
    }

    // Optional per-sample SFT loss for ORPO/CPO.
    var sft_buf: []f32 = &.{};
    defer if (sft_buf.len > 0) allocator.free(sft_buf);
    const kind = config.pref.kind;
    const wants_sft = kind == .orpo or kind == .cpo;
    if (wants_sft) {
        var any = false;
        for (samples) |s| {
            if (s.sft_chosen_loss != null) {
                any = true;
                break;
            }
        }
        if (any) {
            sft_buf = try allocator.alloc(f32, n);
            for (samples, 0..) |s, i| {
                sft_buf[i] = s.sft_chosen_loss orelse 0.0;
            }
        }
    }

    // Policy forward: chosen then rejected.
    const policy_chosen = try allocator.alloc(f32, n);
    defer allocator.free(policy_chosen);
    const policy_rejected = try allocator.alloc(f32, n);
    defer allocator.free(policy_rejected);

    try policy.invoke(prompts, chosens, policy_chosen);
    try policy.invoke(prompts, rejecteds, policy_rejected);

    // Reference forward: only for kinds that need it.
    var ref_chosen: []f32 = &.{};
    var ref_rejected: []f32 = &.{};
    defer if (ref_chosen.len > 0) allocator.free(ref_chosen);
    defer if (ref_rejected.len > 0) allocator.free(ref_rejected);

    if (needsReference(kind)) {
        ref_chosen = try allocator.alloc(f32, n);
        ref_rejected = try allocator.alloc(f32, n);
        if (ref_opt) |ref| {
            try ref.invoke(prompts, chosens, ref_chosen);
            try ref.invoke(prompts, rejecteds, ref_rejected);
        } else if (config.reference_from_disabled_adapter) {
            // Second call to policy; user's ctx is expected to have toggled
            // adapter state to simulate the reference. The harness can't
            // verify this — it is a documented contract on the caller.
            try policy.invoke(prompts, chosens, ref_chosen);
            try policy.invoke(prompts, rejecteds, ref_rejected);
        } else {
            return error.MissingReference;
        }
    }

    const batch = preference_loss.PairedBatch{
        .policy_chosen_logps = policy_chosen,
        .policy_rejected_logps = policy_rejected,
        .ref_chosen_logps = ref_chosen,
        .ref_rejected_logps = ref_rejected,
        .chosen_lengths = chosen_lengths,
        .rejected_lengths = rejected_lengths,
        .sft_chosen_loss = sft_buf,
    };

    const res = try preference_loss.pairedPreferenceLoss(allocator, batch, config.pref);
    // res owns grad_chosen / grad_rejected; hand them straight to the caller.
    return HarnessStepResult{
        .loss = res.loss,
        .mean_reward_margin = res.mean_reward_margin,
        .accuracy = res.accuracy,
        .grad_policy_chosen = res.grad_chosen,
        .grad_policy_rejected = res.grad_rejected,
        .allocator = res.allocator,
    };
}

/// One unpaired (KTO) sample.
pub const UnpairedSample = struct {
    prompt_tokens: []const i32,
    completion_tokens: []const i32,
    desirable: bool,
};

pub const UnpairedStepResult = struct {
    loss: f32,
    grad_policy: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *UnpairedStepResult) void {
        self.allocator.free(self.grad_policy);
        self.* = undefined;
    }
};

/// Run one KTO (unpaired) training step.
pub fn unpairedStep(
    allocator: std.mem.Allocator,
    policy: ModelForward,
    ref_opt: ?ModelForward,
    samples: []const UnpairedSample,
    config: HarnessConfig,
) !UnpairedStepResult {
    if (samples.len == 0) return error.EmptyBatch;
    if (config.pref.kind != .kto) return error.WrongLossKind;
    const n = samples.len;

    const prompts = try allocator.alloc([]const i32, n);
    defer allocator.free(prompts);
    const completions = try allocator.alloc([]const i32, n);
    defer allocator.free(completions);
    const desirable = try allocator.alloc(bool, n);
    defer allocator.free(desirable);

    for (samples, 0..) |s, i| {
        prompts[i] = s.prompt_tokens;
        completions[i] = s.completion_tokens;
        desirable[i] = s.desirable;
    }

    const policy_logps = try allocator.alloc(f32, n);
    defer allocator.free(policy_logps);
    const ref_logps = try allocator.alloc(f32, n);
    defer allocator.free(ref_logps);

    try policy.invoke(prompts, completions, policy_logps);
    if (ref_opt) |ref| {
        try ref.invoke(prompts, completions, ref_logps);
    } else if (config.reference_from_disabled_adapter) {
        try policy.invoke(prompts, completions, ref_logps);
    } else {
        return error.MissingReference;
    }

    const batch = preference_loss.UnpairedBatch{
        .policy_logps = policy_logps,
        .ref_logps = ref_logps,
        .desirable = desirable,
    };

    var res = try preference_loss.unpairedKTOLoss(allocator, batch, config.pref);
    // KTO returns grad_chosen as the per-sample policy grad and an empty grad_rejected.
    res.allocator.free(res.grad_rejected);
    return UnpairedStepResult{
        .loss = res.loss,
        .grad_policy = res.grad_chosen,
        .allocator = res.allocator,
    };
}

// ============================================================================
// PART B: GRPO harness
// ============================================================================

/// Sampler for a single prompt. The callback must allocate each completion
/// token slice and its matching old-logp slice from `allocator` and append
/// the owning slices to the two out-lists. The harness frees them via the
/// same allocator after the step completes.
pub const SampleFn = *const fn (
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    prompt: []const i32,
    num_samples: usize,
    out_tokens: *std.ArrayList([]i32),
    out_old_logps: *std.ArrayList([]f32),
) anyerror!void;

pub const Sampler = struct {
    ctx: *anyopaque,
    call: SampleFn,
};

/// Per-token logprob callback. Fill `out_per_token_logp[t]` with the logprob
/// of `completion[t]` under the current policy (or reference) conditioned on
/// `prompt` and the completion prefix. Length of `out_per_token_logp` equals
/// `completion.len`.
pub const TokenLogpFn = *const fn (
    ctx: *anyopaque,
    prompt: []const i32,
    completion: []const i32,
    out_per_token_logp: []f32,
) anyerror!void;

pub const PolicyScorer = struct {
    ctx: *anyopaque,
    call: TokenLogpFn,

    pub fn score(
        self: PolicyScorer,
        prompt: []const i32,
        completion: []const i32,
        out: []f32,
    ) !void {
        return self.call(self.ctx, prompt, completion, out);
    }
};

pub const GRPOHarnessConfig = struct {
    grpo: grpo.GRPOConfig,
    num_prompts: usize = 8,
};

pub const GRPOStepResult = struct {
    loss: f32,
    pg_loss: f32,
    kl_loss: f32,
    clip_fraction: f32,
    mean_reward: f32,
    grad_new_logps: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GRPOStepResult) void {
        self.allocator.free(self.grad_new_logps);
        self.* = undefined;
    }
};

/// Run one GRPO step. Sequence:
///   1. For each prompt: sampler emits `group_size` completions and their
///      old-policy per-token logprobs.
///   2. Score each completion via the rewarder.
///   3. Compute group-relative advantages.
///   4. Score each completion with the new policy (per-token logps).
///   5. Score each completion with the reference (per-token logps).
///   6. Call `grpo.grpoLoss` and return.
pub fn grpoStep(
    allocator: std.mem.Allocator,
    prompts: []const []const i32,
    sampler: Sampler,
    policy_scorer: PolicyScorer,
    ref_scorer: PolicyScorer,
    rewarder: grpo.Rewarder,
    config: GRPOHarnessConfig,
) !GRPOStepResult {
    const group_size = config.grpo.group_size;
    if (prompts.len == 0 or group_size == 0) return error.EmptyBatch;

    // Per-prompt sampler buffers. We own these lists and their contained slices.
    const total_comps = prompts.len * group_size;

    var all_tokens: std.ArrayList([]i32) = .empty;
    defer {
        for (all_tokens.items) |t| allocator.free(t);
        all_tokens.deinit(allocator);
    }
    var all_old_logps: std.ArrayList([]f32) = .empty;
    defer {
        for (all_old_logps.items) |lp| allocator.free(lp);
        all_old_logps.deinit(allocator);
    }
    try all_tokens.ensureTotalCapacity(allocator, total_comps);
    try all_old_logps.ensureTotalCapacity(allocator, total_comps);

    // prompt_idx per completion, parallel to all_tokens.
    var prompt_idxs: std.ArrayList(usize) = .empty;
    defer prompt_idxs.deinit(allocator);
    try prompt_idxs.ensureTotalCapacity(allocator, total_comps);

    for (prompts, 0..) |prompt, p_idx| {
        const before_len = all_tokens.items.len;
        try sampler.call(sampler.ctx, allocator, prompt, group_size, &all_tokens, &all_old_logps);
        const added_tokens = all_tokens.items.len - before_len;
        const added_logps = all_old_logps.items.len - before_len;
        if (added_tokens != added_logps or added_tokens != group_size) {
            return error.SamplerContract;
        }
        var k: usize = 0;
        while (k < added_tokens) : (k += 1) {
            try prompt_idxs.append(allocator, p_idx);
        }
    }

    // Build the Completion array. ref_logps is filled after the ref scorer runs.
    var total_tokens: usize = 0;
    for (all_tokens.items) |toks| total_tokens += toks.len;

    const new_logps = try allocator.alloc(f32, total_tokens);
    defer allocator.free(new_logps);
    const ref_logps_flat = try allocator.alloc(f32, total_tokens);
    defer allocator.free(ref_logps_flat);

    // Fill new_logps and ref_logps via scorer callbacks.
    {
        var off: usize = 0;
        for (all_tokens.items, 0..) |toks, ci| {
            const p = prompts[prompt_idxs.items[ci]];
            if (toks.len != all_old_logps.items[ci].len) return error.SamplerContract;
            try policy_scorer.score(p, toks, new_logps[off .. off + toks.len]);
            try ref_scorer.score(p, toks, ref_logps_flat[off .. off + toks.len]);
            off += toks.len;
        }
    }

    // Build Completion slice now that ref_logps are populated.
    const completions = try allocator.alloc(grpo.Completion, all_tokens.items.len);
    defer allocator.free(completions);
    {
        var off: usize = 0;
        for (all_tokens.items, 0..) |toks, ci| {
            completions[ci] = .{
                .prompt_idx = prompt_idxs.items[ci],
                .tokens = toks,
                .old_logps = all_old_logps.items[ci],
                .ref_logps = ref_logps_flat[off .. off + toks.len],
            };
            off += toks.len;
        }
    }

    var ga = try grpo.scoreGroup(allocator, rewarder, completions);
    defer ga.deinit();
    grpo.computeAdvantages(&ga, completions, config.grpo);

    var mean_reward: f32 = 0;
    if (ga.rewards.len > 0) {
        var acc: f64 = 0;
        for (ga.rewards) |r| acc += r;
        mean_reward = @floatCast(acc / @as(f64, @floatFromInt(ga.rewards.len)));
    }

    const res = try grpo.grpoLoss(allocator, completions, new_logps, ga.advantages, config.grpo);
    // res owns grad_new_logps; hand straight to the caller.
    return GRPOStepResult{
        .loss = res.loss,
        .pg_loss = res.pg_loss,
        .kl_loss = res.kl_loss,
        .clip_fraction = res.clip_fraction,
        .mean_reward = mean_reward,
        .grad_new_logps = res.grad_new_logps,
        .allocator = res.allocator,
    };
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

// Mock model: "logprob = -(L2 distance from a target scalar per token-id)".
// Each sequence's summed logprob is -sum_t (target - token[t])^2 * scale + bias.
// With `scale` and `bias` tweakable per-call this gives deterministic, finite,
// non-degenerate numbers for the harness to consume.
const MockCtx = struct {
    scale: f32 = 0.01,
    bias: f32 = 0.0,
    calls: usize = 0,

    fn logpOf(self: *MockCtx, prompt: []const i32, completion: []const i32) f32 {
        var s: f32 = 0;
        for (completion) |t| {
            const diff: f32 = @floatFromInt(t);
            s -= diff * diff * self.scale;
        }
        // Touch the prompt so the compiler does not elide the arg.
        for (prompt) |t| {
            const d: f32 = @floatFromInt(t);
            s -= d * 0.0001;
        }
        return s + self.bias;
    }
};

fn mockLogprob(
    ctx: *anyopaque,
    prompts: []const []const i32,
    completion_tokens: []const []const i32,
    out_logps: []f32,
) anyerror!void {
    const self: *MockCtx = @ptrCast(@alignCast(ctx));
    self.calls += 1;
    for (completion_tokens, 0..) |c, i| {
        out_logps[i] = self.logpOf(prompts[i], c);
    }
}

test "pairedStep DPO end-to-end with mock model" {
    const alloc = testing.allocator;

    var policy_ctx = MockCtx{ .scale = 0.02, .bias = 0.0 };
    var ref_ctx = MockCtx{ .scale = 0.01, .bias = -0.1 };

    const policy = ModelForward{ .ctx = &policy_ctx, .call = mockLogprob };
    const ref = ModelForward{ .ctx = &ref_ctx, .call = mockLogprob };

    const p0 = [_]i32{ 1, 2, 3 };
    const c0 = [_]i32{ 4, 5, 6 };
    const r0 = [_]i32{ 7, 8, 9 };
    const p1 = [_]i32{ 10, 11 };
    const c1 = [_]i32{ 2, 3 };
    const r1 = [_]i32{ 12, 13 };

    const samples = [_]PreferenceSample{
        .{ .prompt_tokens = &p0, .chosen_tokens = &c0, .rejected_tokens = &r0 },
        .{ .prompt_tokens = &p1, .chosen_tokens = &c1, .rejected_tokens = &r1 },
    };

    const cfg = HarnessConfig{
        .pref = .{ .kind = .dpo, .beta = 0.2 },
        .reference_from_disabled_adapter = false,
    };

    var res = try pairedStep(alloc, policy, ref, &samples, cfg);
    defer res.deinit();

    try testing.expect(std.math.isFinite(res.loss));
    try testing.expect(res.accuracy >= 0.0 and res.accuracy <= 1.0);
    try testing.expectEqual(@as(usize, 2), res.grad_policy_chosen.len);
    try testing.expectEqual(@as(usize, 2), res.grad_policy_rejected.len);

    var any_nonzero = false;
    for (res.grad_policy_chosen) |g| {
        if (g != 0.0) any_nonzero = true;
        try testing.expect(std.math.isFinite(g));
    }
    for (res.grad_policy_rejected) |g| {
        if (g != 0.0) any_nonzero = true;
        try testing.expect(std.math.isFinite(g));
    }
    try testing.expect(any_nonzero);
}

test "pairedStep SimPO is reference-free (no ref calls)" {
    const alloc = testing.allocator;

    var policy_ctx = MockCtx{ .scale = 0.02 };
    const policy = ModelForward{ .ctx = &policy_ctx, .call = mockLogprob };

    const p0 = [_]i32{1};
    const c0 = [_]i32{ 5, 5, 5, 5 };
    const r0 = [_]i32{ 9, 9 };
    const samples = [_]PreferenceSample{
        .{ .prompt_tokens = &p0, .chosen_tokens = &c0, .rejected_tokens = &r0 },
    };

    const cfg = HarnessConfig{
        .pref = .{ .kind = .simpo, .beta = 2.0, .simpo_gamma = 0.1 },
        // Deliberately flip this: SimPO must still skip the ref call.
        .reference_from_disabled_adapter = false,
    };

    var res = try pairedStep(alloc, policy, null, &samples, cfg);
    defer res.deinit();

    try testing.expect(std.math.isFinite(res.loss));
    // Two policy.invoke calls: one for chosen, one for rejected. No ref.
    try testing.expectEqual(@as(usize, 2), policy_ctx.calls);
}

test "pairedStep rejects KTO with UseUnpairedStep" {
    const alloc = testing.allocator;
    var policy_ctx = MockCtx{};
    const policy = ModelForward{ .ctx = &policy_ctx, .call = mockLogprob };

    const p0 = [_]i32{1};
    const c0 = [_]i32{2};
    const r0 = [_]i32{3};
    const samples = [_]PreferenceSample{
        .{ .prompt_tokens = &p0, .chosen_tokens = &c0, .rejected_tokens = &r0 },
    };
    const cfg = HarnessConfig{
        .pref = .{ .kind = .kto, .beta = 0.1 },
    };

    try testing.expectError(error.UseUnpairedStep, pairedStep(alloc, policy, policy, &samples, cfg));
}

test "pairedStep reference-from-disabled-adapter calls policy four times" {
    const alloc = testing.allocator;
    var policy_ctx = MockCtx{ .scale = 0.02 };
    const policy = ModelForward{ .ctx = &policy_ctx, .call = mockLogprob };

    const p0 = [_]i32{1};
    const c0 = [_]i32{ 4, 5 };
    const r0 = [_]i32{ 6, 7 };
    const samples = [_]PreferenceSample{
        .{ .prompt_tokens = &p0, .chosen_tokens = &c0, .rejected_tokens = &r0 },
    };
    const cfg = HarnessConfig{
        .pref = .{ .kind = .dpo, .beta = 0.1 },
        .reference_from_disabled_adapter = true,
    };

    var res = try pairedStep(alloc, policy, null, &samples, cfg);
    defer res.deinit();
    // 2 policy calls + 2 "reference" calls = 4.
    try testing.expectEqual(@as(usize, 4), policy_ctx.calls);
}

// ---------- GRPO mocks ----------

const MockSamplerCtx = struct {
    dummy: u8 = 0,
};

fn mockSample(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    prompt: []const i32,
    num_samples: usize,
    out_tokens: *std.ArrayList([]i32),
    out_old_logps: *std.ArrayList([]f32),
) anyerror!void {
    _ = ctx;
    // Derive a per-prompt salt from the prompt tokens to keep groups distinct.
    var salt: i32 = 0;
    for (prompt) |t| salt +%= t;
    var k: usize = 0;
    while (k < num_samples) : (k += 1) {
        const tok_len: usize = 2;
        const toks = try allocator.alloc(i32, tok_len);
        toks[0] = salt + @as(i32, @intCast(k));
        toks[1] = @as(i32, @intCast(k + 1));
        const lps = try allocator.alloc(f32, tok_len);
        lps[0] = -0.5;
        lps[1] = -0.5;
        try out_tokens.append(allocator, toks);
        try out_old_logps.append(allocator, lps);
    }
}

fn mockReward(
    ctx: *anyopaque,
    prompt_idx: usize,
    completion_tokens: []const i32,
) anyerror!f32 {
    _ = ctx;
    _ = prompt_idx;
    // 1.0 if every token id is even, else 0.0.
    for (completion_tokens) |t| {
        if (@mod(t, 2) != 0) return 0.0;
    }
    return 1.0;
}

fn mockPerTokenLogp(
    ctx: *anyopaque,
    prompt: []const i32,
    completion: []const i32,
    out_per_token_logp: []f32,
) anyerror!void {
    _ = ctx;
    _ = prompt;
    _ = completion;
    for (out_per_token_logp) |*x| x.* = -1.0;
}

test "grpoStep end-to-end with mocks" {
    const alloc = testing.allocator;

    var sctx = MockSamplerCtx{};
    const sampler = Sampler{ .ctx = &sctx, .call = mockSample };

    var scorer_ctx: u8 = 0;
    const policy_scorer = PolicyScorer{ .ctx = &scorer_ctx, .call = mockPerTokenLogp };
    const ref_scorer = PolicyScorer{ .ctx = &scorer_ctx, .call = mockPerTokenLogp };

    var rew_ctx: u8 = 0;
    const rewarder = grpo.Rewarder{ .ctx = &rew_ctx, .call = mockReward };

    const p0 = [_]i32{ 1, 2 };
    const p1 = [_]i32{ 3, 4 };
    const prompts = [_][]const i32{ &p0, &p1 };

    const cfg = GRPOHarnessConfig{
        .grpo = .{ .group_size = 4, .clip_epsilon = 0.2, .kl_coef = 0.04 },
        .num_prompts = 2,
    };

    var res = try grpoStep(alloc, &prompts, sampler, policy_scorer, ref_scorer, rewarder, cfg);
    defer res.deinit();

    try testing.expect(std.math.isFinite(res.loss));
    try testing.expect(res.clip_fraction >= 0.0 and res.clip_fraction <= 1.0);
    try testing.expect(res.mean_reward >= 0.0 and res.mean_reward <= 1.0);

    // total tokens = 2 prompts * 4 completions * 2 tokens/completion = 16
    try testing.expectEqual(@as(usize, 16), res.grad_new_logps.len);
    for (res.grad_new_logps) |g| try testing.expect(std.math.isFinite(g));
}
