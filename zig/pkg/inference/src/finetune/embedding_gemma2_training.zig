// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Backend-neutral contracts for EmbeddingGemma 2 LoRA training.
//!
//! CUDA implementations import this module rather than exposing resident
//! buffers through the generic graph API. The F32 autodiff graph is in
//! `embedding_gemma2_graph.zig`.

const std = @import("std");
const model = @import("../models/embedding_gemma2.zig");

pub const training_sequence_cap: u32 = 512;
pub const default_rank: u32 = 16;
pub const default_alpha: f32 = 32.0;
pub const default_dropout: f32 = 0.0;

/// Strict suffixes prevent the ordinary substring matcher from accidentally
/// adapting PLE projections such as `per_layer_model_projection`.
pub const default_target_suffixes = [_][]const u8{
    ".self_attn.q_proj.weight",
    ".self_attn.k_proj.weight",
    ".self_attn.v_proj.weight",
    ".self_attn.o_proj.weight",
    ".mlp.gate_proj.weight",
    ".mlp.up_proj.weight",
    ".mlp.down_proj.weight",
    "language_model.embedding_projection.weight",
};

pub const Target = enum { q_proj, k_proj, v_proj, o_proj, gate_proj, up_proj, down_proj, embedding_projection };

pub const LayerGeometry = struct {
    kind: model.LayerType,
    query_heads: u32 = 4,
    kv_heads: u32,
    head_dim: u32,

    pub fn queryWidth(self: LayerGeometry) u32 {
        return self.query_heads * self.head_dim;
    }
    pub fn kvWidth(self: LayerGeometry) u32 {
        return self.kv_heads * self.head_dim;
    }
};

pub fn officialLayerGeometry(layer: u32) !LayerGeometry {
    if (layer >= model.official_layer_count) return error.InvalidLayerIndex;
    const global = (layer + 1) % 6 == 0;
    return if (global)
        .{ .kind = .full_attention, .kv_heads = 1, .head_dim = 512 }
    else
        .{ .kind = .sliding_attention, .kv_heads = 2, .head_dim = 256 };
}

pub const LoRAShape = struct {
    a: [2]u32, // [rank, in]
    b: [2]u32, // [out, rank]
};

pub const ProjectionSpec = struct {
    target: Target,
    suffix: []const u8,
    input_dim: u32,
    output_dim: u32,

    pub fn name(self: ProjectionSpec, buffer: []u8, layer: u32) ![]const u8 {
        if (self.target == .embedding_projection)
            return std.fmt.bufPrint(buffer, "language_model.embedding_projection.weight", .{}) catch error.NameTooLong;
        return layerWeightName(buffer, layer, self.suffix);
    }
};

/// Ordered exactly as the forward/backward executor consumes adapters.
pub fn projectionSpecs(layer: u32) ![7]ProjectionSpec {
    const geometry = try officialLayerGeometry(layer);
    return .{
        .{ .target = .q_proj, .suffix = "self_attn.q_proj.weight", .input_dim = 512, .output_dim = geometry.queryWidth() },
        .{ .target = .k_proj, .suffix = "self_attn.k_proj.weight", .input_dim = 512, .output_dim = geometry.kvWidth() },
        .{ .target = .v_proj, .suffix = "self_attn.v_proj.weight", .input_dim = 512, .output_dim = geometry.kvWidth() },
        .{ .target = .o_proj, .suffix = "self_attn.o_proj.weight", .input_dim = geometry.queryWidth(), .output_dim = 512 },
        .{ .target = .gate_proj, .suffix = "mlp.gate_proj.weight", .input_dim = 512, .output_dim = 2048 },
        .{ .target = .up_proj, .suffix = "mlp.up_proj.weight", .input_dim = 512, .output_dim = 2048 },
        .{ .target = .down_proj, .suffix = "mlp.down_proj.weight", .input_dim = 2048, .output_dim = 512 },
    };
}

pub const embeddingProjectionSpec = ProjectionSpec{
    .target = .embedding_projection,
    .suffix = "embedding_projection.weight",
    .input_dim = 512,
    .output_dim = 768,
};

pub const TapeSlot = enum {
    layer_input,
    attention_norm_input,
    q_pre_norm,
    k_pre_norm,
    q_post_rope,
    k_post_rope,
    v_post_norm,
    attention_probabilities,
    attention_context,
    attention_residual,
    ffn_norm_input,
    gate_pre_activation,
    gate_post_activation,
    up_projection,
    ffn_residual,
    ple_gate_post_activation,
    layer_output_after_scalar,
};

/// Minimal semantic tape ordering. Implementations may recompute any slot,
/// but checkpoints are only valid after `layer_output_after_scalar`; placing a
/// boundary at FFN-down would omit the trained PLE/scalar tail.
pub const layer_tape_order = [_]TapeSlot{
    .layer_input,
    .attention_norm_input,
    .q_pre_norm,
    .k_pre_norm,
    .q_post_rope,
    .k_post_rope,
    .v_post_norm,
    .attention_probabilities,
    .attention_context,
    .attention_residual,
    .ffn_norm_input,
    .gate_pre_activation,
    .gate_post_activation,
    .up_projection,
    .ffn_residual,
    .ple_gate_post_activation,
    .layer_output_after_scalar,
};

pub fn loraShape(target: Target, layer: u32, rank: u32) !LoRAShape {
    if (rank == 0) return error.InvalidLoRARank;
    const geometry = try officialLayerGeometry(layer);
    const input_dim: u32, const output_dim: u32 = switch (target) {
        .q_proj => .{ 512, geometry.queryWidth() },
        .k_proj, .v_proj => .{ 512, geometry.kvWidth() },
        .o_proj => .{ geometry.queryWidth(), 512 },
        .gate_proj, .up_proj => .{ 512, 2048 },
        .down_proj => .{ 2048, 512 },
        .embedding_projection => .{ 512, 768 },
    };
    return .{ .a = .{ rank, input_dim }, .b = .{ output_dim, rank } };
}

pub const TrainingPlan = struct {
    rank: u32 = default_rank,
    alpha: f32 = default_alpha,
    dropout: f32 = default_dropout,
    max_sequence: u32 = training_sequence_cap,
    mrl_dimensions: []const u32 = &.{ 128, 256, 512, 768 },
    checkpoint_every_n_layers: u32 = 2,

    pub fn validate(self: TrainingPlan, config: model.Config) !void {
        try config.validate();
        if (!config.isOfficialTextTopology()) return error.UnsupportedEmbeddingGemma2TrainingTopology;
        if (self.rank == 0 or !std.math.isFinite(self.alpha) or self.alpha <= 0 or
            !std.math.isFinite(self.dropout) or self.dropout < 0 or self.dropout >= 1 or
            self.max_sequence == 0 or self.max_sequence > training_sequence_cap or
            self.mrl_dimensions.len == 0 or self.checkpoint_every_n_layers == 0 or
            self.checkpoint_every_n_layers > config.num_hidden_layers)
            return error.InvalidEmbeddingGemma2TrainingPlan;
        var prior: u32 = 0;
        for (self.mrl_dimensions) |dim| {
            if (dim == 0 or dim > config.embedding_dim or dim <= prior)
                return error.InvalidEmbeddingGemma2TrainingPlan;
            prior = dim;
        }
    }

    pub fn scale(self: TrainingPlan) f32 {
        return self.alpha / @as(f32, @floatFromInt(self.rank));
    }
};

pub const Pair = struct {
    query_index: u32,
    document_index: u32,
    group_id: u32 = 0,
};

/// Host representation accepted by both the F32 oracle and a CUDA session.
/// Padding tokens must have mask zero. Every row must contain at least one
/// valid token. Vision/audio tensors are deliberately absent in phase one.
pub const Microbatch = struct {
    token_ids: []const i64,
    attention_mask: []const i64,
    batch: u32,
    sequence: u32,
    pairs: []const Pair,
    task_ids: ?[]const u32 = null,

    pub fn validate(self: Microbatch, plan: TrainingPlan, vocab_size: u32) !void {
        if (self.batch == 0 or self.sequence == 0 or self.sequence > plan.max_sequence)
            return error.InvalidEmbeddingGemma2Microbatch;
        const rows = std.math.mul(usize, self.batch, self.sequence) catch
            return error.InvalidEmbeddingGemma2Microbatch;
        if (self.token_ids.len != rows or self.attention_mask.len != rows or self.pairs.len == 0)
            return error.InvalidEmbeddingGemma2Microbatch;
        if (self.task_ids) |ids| if (ids.len != self.batch) return error.InvalidEmbeddingGemma2Microbatch;
        for (self.token_ids, self.attention_mask, 0..) |token, mask, index| {
            _ = index;
            if (mask != 0 and mask != 1) return error.InvalidEmbeddingGemma2Microbatch;
            if (mask == 1) {
                if (token < 0 or token >= vocab_size) return error.InvalidEmbeddingGemma2Microbatch;
            }
        }
        for (0..self.batch) |batch_index| {
            const start = batch_index * self.sequence;
            var present = false;
            var padding = false;
            for (self.attention_mask[start..][0..self.sequence]) |mask| {
                if (mask == 0) padding = true else {
                    if (padding) return error.NonContiguousAttentionMask;
                    present = true;
                }
            }
            if (!present) return error.EmptyEmbeddingInput;
        }
        for (self.pairs) |pair| {
            if (pair.query_index >= self.batch or pair.document_index >= self.batch or
                pair.query_index == pair.document_index)
                return error.InvalidEmbeddingGemma2Microbatch;
        }
    }
};

pub const AdapterGradient = struct {
    name: []const u8,
    values: []const f32,
};

pub const StepGradients = struct {
    loss: f32,
    gradients: []const AdapterGradient,
    examples: u32,
    tokens: u32,
};

pub const CommitReceipt = struct {
    optimizer_step: u64,
    examples_seen: u64,
    tokens_seen: u64,
    adapter_identity: [32]u8,
};

pub fn isDefaultTarget(name: []const u8) bool {
    for (default_target_suffixes) |suffix| {
        if (std.mem.endsWith(u8, name, suffix)) return true;
    }
    return false;
}

pub fn layerWeightName(buffer: []u8, layer: u32, suffix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "language_model.layers.{d}.{s}", .{ layer, suffix }) catch
        error.NameTooLong;
}

/// Small deterministic F32 oracle for the exact LoRA linear equation used by
/// graph injection: y = x W^T + (alpha/rank) x A^T B^T. All matrices use the
/// checkpoint's [out,in] storage convention.
pub fn loraLinearReference(
    output: []f32,
    input: []const f32,
    base: []const f32,
    adapter_a: []const f32,
    adapter_b: []const f32,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    scale: f32,
) !void {
    if (rank == 0 or !std.math.isFinite(scale) or input.len != rows * input_dim or
        output.len != rows * output_dim or base.len != output_dim * input_dim or
        adapter_a.len != rank * input_dim or adapter_b.len != output_dim * rank)
        return error.InvalidLoRAShape;
    for (0..rows) |row| for (0..output_dim) |out| {
        var value: f32 = 0;
        for (0..input_dim) |inner| value += input[row * input_dim + inner] * base[out * input_dim + inner];
        for (0..rank) |r| {
            var low: f32 = 0;
            for (0..input_dim) |inner| low += input[row * input_dim + inner] * adapter_a[r * input_dim + inner];
            value += scale * low * adapter_b[out * rank + r];
        }
        output[row * output_dim + out] = value;
    };
}

test "EmbeddingGemma2 training defaults are strict and scoped" {
    const defaults = TrainingPlan{};
    try std.testing.expectEqual(@as(f32, 2.0), defaults.scale());
    try std.testing.expectEqualSlices(u32, &.{ 128, 256, 512, 768 }, defaults.mrl_dimensions);
    try std.testing.expect(isDefaultTarget("language_model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(isDefaultTarget("language_model.embedding_projection.weight"));
    try std.testing.expect(!isDefaultTarget("language_model.ple.per_layer_model_projection.weight"));
    try std.testing.expect(!isDefaultTarget("language_model.layers.0.ple_input_gate.weight"));
    try std.testing.expectEqual(LoRAShape{ .a = .{ 16, 512 }, .b = .{ 1024, 16 } }, try loraShape(.q_proj, 0, 16));
    try std.testing.expectEqual(LoRAShape{ .a = .{ 16, 512 }, .b = .{ 2048, 16 } }, try loraShape(.q_proj, 5, 16));
    try std.testing.expectEqual(LoRAShape{ .a = .{ 16, 2048 }, .b = .{ 512, 16 } }, try loraShape(.down_proj, 23, 16));
    const local = try projectionSpecs(0);
    const global = try projectionSpecs(5);
    try std.testing.expectEqual(@as(u32, 1024), local[0].output_dim);
    try std.testing.expectEqual(@as(u32, 2048), global[0].output_dim);
    try std.testing.expectEqual(TapeSlot.layer_output_after_scalar, layer_tape_order[layer_tape_order.len - 1]);
}

test "EmbeddingGemma2 microbatch rejects empty and malformed rows" {
    const plan = TrainingPlan{};
    const ids = [_]i64{ 2, 3, 0, 0 };
    const mask = [_]i64{ 1, 1, 0, 0 };
    const pair = [_]Pair{.{ .query_index = 0, .document_index = 1 }};
    const batch = Microbatch{ .token_ids = &ids, .attention_mask = &mask, .batch = 2, .sequence = 2, .pairs = &pair };
    try std.testing.expectError(error.EmptyEmbeddingInput, batch.validate(plan, 8));
}

test "EmbeddingGemma2 LoRA oracle gradients match finite differences" {
    const input = [_]f32{ 0.25, -0.5 };
    const base = [_]f32{ 0.1, 0.2, -0.3, 0.4 };
    var adapter_a = [_]f32{ 0.2, -0.1, 0.3, 0.4 };
    var adapter_b = [_]f32{ 0.5, -0.2, 0.1, 0.6 };
    const cotangent = [_]f32{ 0.7, -0.9 };
    const scale: f32 = 2;
    const epsilon: f32 = 1e-3;

    const loss = struct {
        fn eval(x: []const f32, w: []const f32, a: []const f32, b: []const f32, dy: []const f32, s: f32) !f32 {
            var out: [2]f32 = undefined;
            try loraLinearReference(&out, x, w, a, b, 1, 2, 2, 2, s);
            return out[0] * dy[0] + out[1] * dy[1];
        }
    }.eval;

    // dA[r,i] = scale * x[i] * sum_o(dy[o] * B[o,r])
    for (&adapter_a, 0..) |*value, index| {
        const r = index / 2;
        const inner = index % 2;
        var weighted_b: f32 = 0;
        for (0..2) |out| weighted_b += cotangent[out] * adapter_b[out * 2 + r];
        const analytical = scale * input[inner] * weighted_b;
        const original = value.*;
        value.* = original + epsilon;
        const plus = try loss(&input, &base, &adapter_a, &adapter_b, &cotangent, scale);
        value.* = original - epsilon;
        const minus = try loss(&input, &base, &adapter_a, &adapter_b, &cotangent, scale);
        value.* = original;
        try std.testing.expectApproxEqAbs(analytical, (plus - minus) / (2 * epsilon), 2e-4);
    }
    // dB[o,r] = scale * dy[o] * sum_i(x[i] * A[r,i])
    for (&adapter_b, 0..) |*value, index| {
        const out = index / 2;
        const r = index % 2;
        var low: f32 = 0;
        for (0..2) |inner| low += input[inner] * adapter_a[r * 2 + inner];
        const analytical = scale * cotangent[out] * low;
        const original = value.*;
        value.* = original + epsilon;
        const plus = try loss(&input, &base, &adapter_a, &adapter_b, &cotangent, scale);
        value.* = original - epsilon;
        const minus = try loss(&input, &base, &adapter_a, &adapter_b, &cotangent, scale);
        value.* = original;
        try std.testing.expectApproxEqAbs(analytical, (plus - minus) / (2 * epsilon), 2e-4);
    }
}
