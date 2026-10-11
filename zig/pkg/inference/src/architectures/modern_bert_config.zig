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

//! Model configuration and geometry validation without execution backends.
const std = @import("std");

pub const Config = struct {
    /// Execution preference enabled by the measured GLiNER decision path.
    /// Checkpoint parsing leaves other ModernBERT callers unchanged.
    metal_f16_weight_mirrors: bool = false,
    laya: ?@import("../models/laya.zig").Config = null,
    vocab_size: u32 = 50368,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 22,
    num_attention_heads: u32 = 12,
    /// GeGLU inner dimension.  Wi projects hidden → 2*intermediate_size, then
    /// we split the output, apply GELU to the gate half, multiply, and project
    /// the resulting [total, intermediate_size] down via Wo.
    intermediate_size: u32 = 1152,
    max_position_embeddings: u32 = 8192,
    /// RoPE theta for global (full) attention layers.
    global_rope_theta: f32 = 160000.0,
    /// RoPE theta for local (sliding-window) attention layers.
    local_rope_theta: f32 = 10000.0,
    /// Layers whose index is divisible by this value use full attention.
    /// All other layers use sliding-window (local) attention.
    global_attn_every_n_layers: u32 = 3,
    /// Full sliding-window width: each query attends ±(local_attention_window/2) tokens.
    local_attention_window: u32 = 128,
    layer_norm_eps: f32 = 1e-5,
    /// HuggingFace ModernBERT applies RoPE to the first and second halves of
    /// each head, while the legacy fused-chunker checkpoints use consecutive
    /// rotation pairs.
    rope_interleaved: bool = true,
    use_geglu: bool = true,
    /// LoRA rank for query_proj and value_proj.  0 = LoRA disabled.
    /// When non-zero the encoder tries to load lora_a/lora_b weight tensors
    /// from the active WeightStore and uses linearLoRA for Q/V projections.
    lora_rank: u32 = 0,
    /// LoRA scaling alpha.  The effective scale applied to the LoRA delta is
    /// alpha / rank.  Defaults to rank (i.e., scale = 1.0) when 0 is passed.
    lora_alpha: f32 = 0.0,
    /// The original fused-chunker checkpoint stored independent biased Q/K/V
    /// projections. HuggingFace ModernBERT checkpoints instead have a single
    /// bias-free Wqkv tensor and bias-free norms/output projection.
    checkpoint_layout: CheckpointLayout = .separate_qkv_with_bias,
};

pub const CheckpointLayout = enum {
    separate_qkv_with_bias,
    huggingface_fused_qkv_no_bias,
};

pub fn isModernBertModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "modernbert") or
        std.mem.eql(u8, model_type, "modern_bert");
}

/// Parse the structural subset of HuggingFace's ModernBERT config needed by
/// the eager encoder. The public embedding checkpoints use the fused,
/// bias-free layout.
pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidModernBertConfig;
    const obj = parsed.value.object;
    var config = Config{};
    if (obj.get("laya")) |value| config.laya = try @import("../models/laya.zig").Config.parse(value);
    if (obj.get("vocab_size")) |value| config.vocab_size = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("hidden_size")) |value| config.hidden_size = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("num_hidden_layers")) |value| config.num_hidden_layers = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("num_attention_heads")) |value| config.num_attention_heads = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("intermediate_size")) |value| config.intermediate_size = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("max_position_embeddings")) |value| config.max_position_embeddings = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("global_attn_every_n_layers")) |value| config.global_attn_every_n_layers = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("local_attention")) |value| config.local_attention_window = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("global_rope_theta")) |value| config.global_rope_theta = jsonF32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("local_rope_theta")) |value| config.local_rope_theta = jsonF32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("layer_norm_eps")) |value| config.layer_norm_eps = jsonF32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("norm_eps")) |value| config.layer_norm_eps = jsonF32(value) orelse return error.InvalidModernBertConfig;
    // Transformers 5 nests the rope settings per layer type (Ettin, for
    // example, uses 160000 for sliding layers too) and lists each layer's
    // type instead of `global_attn_every_n_layers`.
    if (obj.get("rope_parameters")) |value| {
        if (value != .object) return error.InvalidModernBertConfig;
        config.global_rope_theta = try ropeTheta(value.object, "full_attention");
        config.local_rope_theta = try ropeTheta(value.object, "sliding_attention");
    }
    if (obj.get("layer_types")) |value| try checkLayerTypes(value, config);

    // `modernbert` is Transformers' public checkpoint layout. Keep the
    // historical layout available to the fused-chunker training code.
    if (obj.get("model_type")) |value| {
        if (value != .string or !isModernBertModel(value.string)) return error.InvalidModernBertConfig;
        config.checkpoint_layout = .huggingface_fused_qkv_no_bias;
        // Transformers' `rotate_half` layout is split-half, not
        // consecutive (interleaved) pairs.
        config.rope_interleaved = false;
        try requireOptionalString(obj, "hidden_activation", "gelu");
        // `position_embedding_type` is not checked: Transformers' ModernBERT
        // neither defines nor reads it and always applies RoPE. Checkpoints
        // carry leftover values ("absolute" in answerdotai/ModernBERT-base
        // and Laya, "sans_pos" in Ettin) that do not change the model.
        try requireOptionalBool(obj, "attention_bias", false);
        try requireOptionalBool(obj, "mlp_bias", false);
        try requireOptionalBool(obj, "norm_bias", false);
    }
    if (config.laya) |laya| {
        if (config.hidden_size < 64 or config.hidden_size % 64 != 0 or config.num_attention_heads == 0 or config.hidden_size % config.num_attention_heads != 0 or config.num_hidden_layers == 0 or laya.max_len > config.max_position_embeddings) return error.InvalidLayaConfig;
    }
    try validateConfig(config);
    return config;
}

/// Validate the geometry shared by CPU, Metal, and CUDA execution. Keeping
/// this separate from JSON parsing also protects callers that construct a
/// Config directly (training fixtures and embedded manifests do both).
pub fn validateConfig(config: Config) !void {
    if (config.vocab_size == 0 or
        config.hidden_size == 0 or
        config.num_hidden_layers == 0 or
        config.num_attention_heads == 0 or
        config.intermediate_size == 0 or
        config.max_position_embeddings == 0 or
        config.global_attn_every_n_layers == 0 or
        config.local_attention_window == 0 or
        config.hidden_size % config.num_attention_heads != 0)
    {
        return error.InvalidModernBertConfig;
    }
    // Every supported ModernBERT checkpoint applies full-head RoPE. Both the
    // interleaved and split-half layouts require an even head dimension.
    const head_dim = config.hidden_size / config.num_attention_heads;
    if (head_dim < 2 or head_dim % 2 != 0) return error.InvalidModernBertConfig;
    if (!std.math.isFinite(config.global_rope_theta) or config.global_rope_theta <= 0 or
        !std.math.isFinite(config.local_rope_theta) or config.local_rope_theta <= 0 or
        !std.math.isFinite(config.layer_norm_eps) or config.layer_norm_eps <= 0 or
        !std.math.isFinite(config.lora_alpha) or config.lora_alpha < 0)
        return error.InvalidModernBertConfig;
}

fn ropeTheta(params: std.json.ObjectMap, layer_type: []const u8) !f32 {
    const entry = params.get(layer_type) orelse return error.InvalidModernBertConfig;
    if (entry != .object) return error.InvalidModernBertConfig;
    var fields = entry.object.iterator();
    while (fields.next()) |field| {
        if (!std.mem.eql(u8, field.key_ptr.*, "rope_theta") and !std.mem.eql(u8, field.key_ptr.*, "rope_type"))
            return error.UnsupportedModernBertRope;
    }
    if (entry.object.get("rope_type")) |kind| {
        if (kind != .string or !std.mem.eql(u8, kind.string, "default")) return error.UnsupportedModernBertRope;
    }
    const value = entry.object.get("rope_theta") orelse return error.InvalidModernBertConfig;
    return jsonF32(value) orelse return error.InvalidModernBertConfig;
}

fn requireOptionalString(obj: std.json.ObjectMap, key: []const u8, expected: []const u8) !void {
    const value = obj.get(key) orelse return;
    if (value != .string or !std.mem.eql(u8, value.string, expected)) return error.UnsupportedModernBertConfig;
}

fn requireOptionalBool(obj: std.json.ObjectMap, key: []const u8, expected: bool) !void {
    const value = obj.get(key) orelse return;
    if (value != .bool or value.bool != expected) return error.UnsupportedModernBertConfig;
}

/// The encoder places full attention on every `global_attn_every_n_layers`-th
/// layer; refuse any other pattern rather than run it wrongly.
fn checkLayerTypes(value: std.json.Value, config: Config) !void {
    if (value != .array or value.array.items.len != config.num_hidden_layers or config.global_attn_every_n_layers == 0) return error.UnsupportedModernBertLayerTypes;
    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.UnsupportedModernBertLayerTypes;
        const want: []const u8 = if (i % config.global_attn_every_n_layers == 0) "full_attention" else "sliding_attention";
        if (!std.mem.eql(u8, item.string, want)) return error.UnsupportedModernBertLayerTypes;
    }
}

fn jsonU32(value: std.json.Value) ?u32 {
    return switch (value) {
        .integer => |integer| std.math.cast(u32, integer),
        else => null,
    };
}

fn jsonF32(value: std.json.Value) ?f32 {
    return switch (value) {
        .float => |float| @floatCast(float),
        .integer => |integer| @floatFromInt(integer),
        else => null,
    };
}
