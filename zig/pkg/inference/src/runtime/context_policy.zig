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

//! Positional admission and KV retention are separate contracts. Reducing KV
//! rows never extends the model's positional range or changes token accounting.
const std = @import("std");
const gpt = @import("../models/gpt.zig");

/// The legacy identity setting is a no-op. Approximate legacy ratios must
/// not reach the host prototype through a direct pipeline caller.
pub fn validateLegacyRatio(ratio: ?f32) !void {
    const value = ratio orelse return;
    if (!std.math.isFinite(value) or value <= 0 or value > 1) return error.InvalidCompactionRatio;
    if (value != 1) return error.KvStorageCompactionNotSupported;
}

pub const payload = @import("kv/payload.zig");
pub const Mode = enum { exact, rolling, attention_matching };
pub const block_tokens: usize = 512;
pub const retained_block_tokens: usize = 256;
pub const reference_queries: usize = 512;

pub const Policy = struct {
    mode: Mode = .exact,
    max_context_tokens: ?u32 = null,
    max_kv_tokens: ?u32 = null,
    keep_prefix_tokens: u32 = 256,
    keep_recent_tokens: u32 = 2048,
    compaction_targets: payload.Targets = .{},

    pub fn validate(self: Policy) !void {
        if (self.compaction_targets.count > payload.max_targets or (self.compaction_targets.count != 0 and self.mode != .attention_matching)) return error.InvalidCompactionTarget;
        for (self.compaction_targets.slice(), 0..) |target, i| {
            for (self.compaction_targets.slice()[0..i]) |old| if (std.meta.eql(old, target)) return error.InvalidCompactionTarget;
        }
        if (self.max_context_tokens == 0 or self.max_kv_tokens == 0 or self.keep_recent_tokens == 0) return error.InvalidContextPolicy;
        if (self.mode == .exact) return;
        const budget = self.max_kv_tokens orelse return error.InvalidContextPolicy;
        const protected = std.math.add(u32, self.keep_prefix_tokens, self.keep_recent_tokens) catch return error.InvalidContextPolicy;
        // One complete uncompacted block must fit before it can be retired.
        const minimum = std.math.add(u32, protected, if (self.mode == .attention_matching) block_tokens else 0) catch return error.InvalidContextPolicy;
        if (budget < minimum) return error.InvalidContextPolicy;
    }

    pub fn contextLimit(self: Policy, model_limit: usize, draft_limit: ?usize) !usize {
        try self.validate();
        if (model_limit == 0) return error.InvalidContextLimit;
        var limit = model_limit;
        if (draft_limit) |draft| {
            if (draft == 0) return error.InvalidContextLimit;
            limit = @min(limit, draft);
        }
        if (self.max_context_tokens) |requested| limit = @min(limit, requested);
        return limit;
    }

    pub fn promptLimit(self: Policy, model_limit: usize, draft_limit: ?usize, output_tokens: usize, speculative_tokens: usize, media_tokens: usize) !usize {
        const limit = try self.contextLimit(model_limit, draft_limit);
        const output = std.math.add(usize, @max(output_tokens, 1), speculative_tokens) catch return error.PromptTooLong;
        const reserved = std.math.add(usize, output, media_tokens) catch return error.PromptTooLong;
        if (reserved >= limit) return error.PromptTooLong;
        return limit - reserved;
    }

    /// Upper bound after each completed maintenance point, including the
    /// incomplete old block. Every old block is compacted at most once.
    pub fn retainedRows(self: Policy, logical_tokens: usize) !usize {
        try self.validate();
        return switch (self.mode) {
            .exact => logical_tokens,
            .rolling => @min(logical_tokens, self.max_kv_tokens.?),
            .attention_matching => blk: {
                const protected = @as(usize, self.keep_prefix_tokens) + self.keep_recent_tokens;
                const old = logical_tokens -| protected;
                const saved = (old / block_tokens) * retained_block_tokens;
                break :blk logical_tokens - saved;
            },
        };
    }

    /// Enforce the whole requested continuation before doing inference. A
    /// budget cannot implicitly request recursive or stronger compaction.
    pub fn admit(self: Policy, model_limit: usize, draft_limit: ?usize, prompt_tokens: usize, output_tokens: usize, speculative_tokens: usize) !Plan {
        const prompt_limit = try self.promptLimit(model_limit, draft_limit, output_tokens, speculative_tokens, 0);
        if (prompt_tokens == 0 or prompt_tokens > prompt_limit) return error.PromptTooLong;
        const total = try std.math.add(usize, prompt_tokens, @max(output_tokens, 1));
        const total_with_speculation = try std.math.add(usize, total, speculative_tokens);
        // Include the boundary token while the original block is still live.
        // Transaction staging is a separate physical allocation budget; the
        // retained-row cap must also fit the source before it is halved.
        var rows = try self.retainedRows(total_with_speculation);
        if (self.mode == .attention_matching) {
            const protected = @as(usize, self.keep_prefix_tokens) + self.keep_recent_tokens;
            if (total_with_speculation >= protected + block_tokens) {
                const boundary = protected + ((total_with_speculation - protected) / block_tokens) * block_tokens;
                rows = @max(rows, (try self.retainedRows(boundary - 1)) + 1);
            }
        }
        if (self.max_kv_tokens) |budget| {
            if (rows > budget) return error.ContextKvBudgetExceeded;
        }
        return .{
            .policy = self,
            .context_limit = try self.contextLimit(model_limit, draft_limit),
            .prompt_tokens = prompt_tokens,
            .logical_token_limit = total_with_speculation,
            // Reserve permitted exact headroom as well as the minimum
            // compaction peak. A rejected fit can then retain the remainder
            // exactly without allocating more memory or exceeding the cap.
            .retained_row_limit = if (self.mode == .attention_matching) @min(total_with_speculation, self.max_kv_tokens.?) else rows,
        };
    }
};

/// Shared wire/CLI validation, deliberately independent of generated API
/// types so every entry point uses the same numeric and enum contract.
pub fn fromApi(raw: anytype) !Policy {
    const input = raw orelse return .{};
    var policy = Policy{
        .mode = if (input.mode) |name| std.meta.stringToEnum(Mode, name) orelse return error.InvalidContextPolicy else .exact,
        .max_context_tokens = try optionalU32(input.max_context_tokens),
        .max_kv_tokens = try optionalU32(input.max_kv_tokens),
        .keep_prefix_tokens = (try optionalU32(input.keep_prefix_tokens)) orelse 256,
        .keep_recent_tokens = (try optionalU32(input.keep_recent_tokens)) orelse 2048,
    };
    if (@hasField(@TypeOf(input), "compaction_targets")) {
        if (input.compaction_targets) |targets| for (targets) |target| try policy.compaction_targets.add(.{
            .message_index = std.math.cast(u32, target.message_index) orelse return error.InvalidCompactionTarget,
            .part_index = std.math.cast(u32, target.part_index) orelse return error.InvalidCompactionTarget,
        });
    }
    try policy.validate();
    return policy;
}

fn optionalU32(value: ?i64) !?u32 {
    return if (value) |v| std.math.cast(u32, v) orelse return error.InvalidContextPolicy else null;
}

pub const Plan = struct {
    policy: Policy,
    context_limit: usize,
    prompt_tokens: usize,
    logical_token_limit: usize,
    retained_row_limit: usize,
};

/// Translate an already admitted attention-matching request into the exact
/// plan used to ingest its original KV. Retention remains a separate,
/// profile-derived publication contract; the packed session itself must begin
/// with every prompt and continuation row available.
pub fn exactStoragePlan(admitted: Plan) !Plan {
    if (admitted.policy.mode != .attention_matching or admitted.prompt_tokens == 0 or
        admitted.prompt_tokens >= admitted.logical_token_limit or admitted.logical_token_limit > admitted.context_limit)
        return error.InvalidContextPolicy;
    return .{
        .policy = .{},
        .context_limit = admitted.context_limit,
        .prompt_tokens = admitted.prompt_tokens,
        .logical_token_limit = admitted.logical_token_limit,
        .retained_row_limit = admitted.logical_token_limit,
    };
}

pub const Capabilities = struct {
    rolling: bool = false,
    attention_matching: bool = false,
};

pub const RequestFeatures = struct {
    metal: bool = false,
    f16_kv: bool = false,
    multimodal: bool = false,
    speculation: bool = false,
    prompt_cache: bool = false,
    forced_global_trim: bool = false,
};

pub fn validateRequestFeatures(policy: Policy, features: RequestFeatures) !void {
    try policy.validate();
    if (features.forced_global_trim and policy.mode == .exact) return error.UnsafeGlobalContextTrim;
    if (policy.mode == .exact) return;
    if (!features.metal or !features.f16_kv or features.multimodal or features.speculation or features.prompt_cache)
        return error.UnsupportedContextCombination;
}

pub fn validateExecution(policy: Policy, features: RequestFeatures, capabilities: Capabilities) !void {
    try validateRequestFeatures(policy, features);
    if (policy.mode == .exact) return;
    if ((policy.mode == .rolling and !capabilities.rolling) or
        (policy.mode == .attention_matching and !capabilities.attention_matching)) return error.ContextPolicyNotQualified;
}

pub const LayerLayout = struct {
    owner: u16,
    sliding_window: u32,
    kv_heads: u32,
    query_heads: u32,
    head_dim: u32,
    rope_active_dim: u32,
    rope_frequency_dim: u32,
    rope_theta: f32,
};

pub const Layout = struct {
    layers: [gpt.max_attention_layers]LayerLayout = undefined,
    len: usize = 0,

    pub fn init(config: gpt.Config) !Layout {
        const count = config.num_hidden_layers;
        if (count == 0 or count > gpt.max_attention_layers or (config.num_kv_shared_layers != 0 and config.num_kv_shared_layers >= count) or
            config.attention_schedule_len > gpt.max_attention_layers or config.attention_schedule_len > count or
            config.num_attention_heads == 0) return error.InvalidContextLayout;
        if (config.family == .gemma and config.sliding_window > 0 and config.attention_schedule_len == 0 and
            config.sliding_window_pattern == 0) return error.InvalidContextLayout;
        if (!std.math.isFinite(config.rope_partial_factor) or config.rope_partial_factor <= 0 or
            !std.math.isFinite(config.rope_theta) or config.rope_theta <= 0 or
            !std.math.isFinite(config.rope_local_theta) or config.rope_local_theta <= 0) return error.InvalidContextLayout;
        var result = Layout{ .len = count };
        for (result.layers[0..count], 0..) |*layer, i| {
            const owner = config.kvDonorLayerIndex(i) orelse i;
            if (owner >= count or (owner != i and owner >= i and !config.gemma4_mtp_assistant)) return error.InvalidContextLayout;
            const heads = config.effectiveKVHeadsForLayer(i);
            const dim = config.effectiveHeadDimForLayer(i);
            if (heads == 0 or dim == 0 or config.num_attention_heads % heads != 0) return error.InvalidContextLayout;
            if (owner != i and (config.layerUsesSlidingAttention(owner) != config.layerUsesSlidingAttention(i) or
                config.effectiveKVHeadsForLayer(owner) != heads or config.effectiveHeadDimForLayer(owner) != dim))
                return error.InvalidContextLayout;
            layer.* = .{
                .owner = @intCast(owner),
                .sliding_window = if (config.layerUsesSlidingAttention(i)) config.sliding_window else 0,
                .kv_heads = heads,
                .query_heads = config.num_attention_heads,
                .head_dim = dim,
                .rope_active_dim = config.layerRopeActiveDim(i),
                .rope_frequency_dim = config.layerRopeFrequencyDim(i),
                .rope_theta = config.layerRopeEffectiveTheta(i),
            };
        }
        return result;
    }
};

test "context policy preserves model positions and reserves every output token" {
    const p = Policy{ .max_context_tokens = 4096 };
    try std.testing.expectEqual(@as(usize, 3839), try p.promptLimit(131072, null, 256, 1, 0));
    try std.testing.expectEqual(@as(usize, 1791), try p.promptLimit(131072, 2048, 256, 1, 0));
    try std.testing.expectError(error.PromptTooLong, p.admit(131072, null, 3841, 256, 0));
    const exact = try p.admit(131072, null, 3840, 256, 0);
    try std.testing.expectEqual(@as(usize, 4096), exact.logical_token_limit);
    try std.testing.expectError(error.PromptTooLong, p.promptLimit(131072, null, std.math.maxInt(usize), 1, 0));
}

test "context policy rejects ambiguous budgets and unsafe feature combinations" {
    try std.testing.expectError(error.InvalidContextPolicy, (Policy{ .mode = .rolling }).validate());
    try std.testing.expectError(error.InvalidContextPolicy, (Policy{ .mode = .attention_matching, .max_kv_tokens = 2500 }).validate());
    try std.testing.expectError(error.InvalidContextPolicy, (Policy{ .mode = .rolling, .max_kv_tokens = 4096, .keep_prefix_tokens = std.math.maxInt(u32) }).validate());
    try std.testing.expectError(error.UnsafeGlobalContextTrim, validateExecution(.{}, .{ .forced_global_trim = true }, .{}));
    const p = Policy{ .mode = .rolling, .max_kv_tokens = 4096 };
    try std.testing.expectError(error.UnsupportedContextCombination, validateExecution(p, .{ .metal = true, .f16_kv = true, .prompt_cache = true }, .{ .rolling = true }));
    try std.testing.expectError(error.ContextPolicyNotQualified, validateExecution(p, .{ .metal = true, .f16_kv = true }, .{}));
    // A remote client validates immutable request features without pretending
    // to own the warm server's loaded-profile capability decision.
    try validateRequestFeatures(p, .{ .metal = true, .f16_kv = true });
    const attention = Policy{ .mode = .attention_matching, .max_kv_tokens = 4096 };
    try validateRequestFeatures(attention, .{ .metal = true, .f16_kv = true });
    try std.testing.expectError(
        error.ContextPolicyNotQualified,
        validateExecution(attention, .{ .metal = true, .f16_kv = true }, .{}),
    );
}

test "context policy admission bounds every intermediate compaction peak" {
    const p = Policy{ .mode = .attention_matching, .max_kv_tokens = 10000 };
    for (2304..12000) |end| {
        const plan = try p.admit(131072, null, 1, end - 1, 0);
        try std.testing.expect((try p.retainedRows(end)) <= plan.retained_row_limit);
        if (end > 2305) try std.testing.expect((try p.retainedRows(end - 1)) <= plan.retained_row_limit);
    }
    try std.testing.expectError(error.ContextKvBudgetExceeded, p.admit(131072, null, 64000, 256, 0));
    const rolling = Policy{ .mode = .rolling, .max_kv_tokens = 4096 };
    try std.testing.expectEqual(@as(usize, 4096), (try rolling.admit(131072, null, 64000, 256, 0)).retained_row_limit);
    try std.testing.expectError(error.PromptTooLong, rolling.admit(131072, null, 131072, 1, 0));
}

test "context layout preserves irregular attention and validates KV donors" {
    var config = gpt.Config{
        .family = .gemma,
        .num_hidden_layers = 4,
        .num_attention_heads = 8,
        .num_key_value_heads = 2,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .sliding_window = 512,
        .num_kv_shared_layers = 1,
        .attention_schedule_len = 4,
    };
    config.attention_schedule[0..4].* = .{ .sliding, .global, .sliding, .global };
    const layout = try Layout.init(config);
    try std.testing.expectEqual(@as(u16, 1), layout.layers[3].owner);
    try std.testing.expectEqual(@as(u32, 512), layout.layers[3].head_dim);
    try std.testing.expectEqual(@as(u32, 0), layout.layers[3].sliding_window);
    config.num_kv_shared_layers = 5;
    try std.testing.expectError(error.InvalidContextLayout, Layout.init(config));
}

test "context admission includes the final source row before block replacement" {
    const p = Policy{ .mode = .attention_matching, .max_kv_tokens = 3071 };
    try std.testing.expectError(error.ContextKvBudgetExceeded, p.admit(131072, null, 3327, 1, 0));
    try std.testing.expectEqual(@as(usize, 2816), (try p.admit(131072, null, 2815, 1, 0)).retained_row_limit);
    try std.testing.expectError(error.InvalidContextPolicy, (Policy{ .keep_recent_tokens = 0 }).validate());
}

test "context admission reserves exact fallback headroom within the explicit cap" {
    const p = Policy{ .mode = .attention_matching, .max_kv_tokens = 1536, .keep_prefix_tokens = 0, .keep_recent_tokens = 512 };
    try std.testing.expectEqual(@as(usize, 1025), (try p.admit(4096, null, 1024, 1, 0)).retained_row_limit);
    try std.testing.expectEqual(@as(usize, 1536), (try p.admit(4096, null, 1536, 1, 0)).retained_row_limit);
    try std.testing.expectError(error.ContextKvBudgetExceeded, p.admit(4096, null, 2559, 1, 0));
}

test "attention admission translates to an exact packed storage plan" {
    const admission = try (Policy{ .mode = .attention_matching, .max_context_tokens = 8192, .max_kv_tokens = 6144, .keep_prefix_tokens = 512, .keep_recent_tokens = 1024 }).admit(8192, null, 8160, 32, 0);
    const storage = try exactStoragePlan(admission);
    try std.testing.expectEqual(Mode.exact, storage.policy.mode);
    try std.testing.expectEqual(admission.context_limit, storage.context_limit);
    try std.testing.expectEqual(admission.prompt_tokens, storage.prompt_tokens);
    try std.testing.expectEqual(admission.logical_token_limit, storage.logical_token_limit);
    try std.testing.expectEqual(storage.logical_token_limit, storage.retained_row_limit);
    try std.testing.expectError(error.InvalidContextPolicy, exactStoragePlan(try (Policy{}).admit(8192, null, 8160, 32, 0)));
}

test "context legacy identity is exact and approximate ratios fail before execution" {
    try validateLegacyRatio(null);
    try validateLegacyRatio(1);
    try std.testing.expectError(error.KvStorageCompactionNotSupported, validateLegacyRatio(0.5));
    try std.testing.expectError(error.InvalidCompactionRatio, validateLegacyRatio(std.math.nan(f32)));
    try std.testing.expectError(error.InvalidCompactionRatio, validateLegacyRatio(std.math.inf(f32)));
    try std.testing.expectError(error.InvalidCompactionRatio, validateLegacyRatio(0));
}
