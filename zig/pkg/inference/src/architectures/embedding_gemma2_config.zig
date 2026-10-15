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

pub const checkpoint_revision = "914f7f89142e33e77833254d9c9b90c3cef7303b";
pub const recipe_version = "embeddinggemma2-f32-mean-v1";
pub const max_tokens: usize = 8192;

pub const Config = struct {
    resident: ?@import("../models/embedding_gemma2.zig").Config = null,
    vision: bool = false,
    audio: bool = false,
    hidden_size: usize = 512,
    embedding_dim: usize = 768,
    intermediate_size: usize = 2048,
    num_hidden_layers: usize = 24,
    num_attention_heads: usize = 4,
    vocab_size: usize = 262144,
    per_layer_input_dim: usize = 512,
    sliding_window: u32 = 512,
    rms_norm_eps: f32 = 1e-6,

    pub fn headDim(_: Config, layer: usize) usize {
        return if ((layer + 1) % 6 == 0) 512 else 256;
    }
    pub fn kvHeads(_: Config, layer: usize) usize {
        return if ((layer + 1) % 6 == 0) 1 else 2;
    }
    pub fn window(self: Config, layer: usize) u32 {
        return if ((layer + 1) % 6 == 0) std.math.maxInt(u32) else self.sliding_window;
    }
    pub fn theta(_: Config, layer: usize) f32 {
        return if ((layer + 1) % 6 == 0) 1000000 else 10000;
    }
};

pub fn isModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "embedding_gemma2") or std.mem.eql(u8, model_type, "embedding_gemma2_text");
}

// Fail closed on incompatible geometry. Merely recognizing the architecture
// must never make a future checkpoint silently use these layer assumptions.
pub fn parseConfig(allocator: std.mem.Allocator, bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidEmbeddingGemma2Config;
    const root = parsed.value.object;
    const mt = root.get("model_type") orelse return error.InvalidEmbeddingGemma2Config;
    if (mt != .string or !isModel(mt.string)) return error.InvalidEmbeddingGemma2Config;
    const text = root.get("text_config") orelse parsed.value;
    if (text != .object) return error.InvalidEmbeddingGemma2Config;
    const obj = text.object;
    var cfg = Config{};
    cfg.resident = @import("../models/embedding_gemma2.zig").parseConfig(allocator, bytes) catch null;
    const expected = .{ .hidden_size = 512, .embedding_dim = 768, .intermediate_size = 2048, .num_hidden_layers = 24, .num_attention_heads = 4, .vocab_size = 262144, .hidden_size_per_layer_input = 512, .head_dim = 256, .num_key_value_heads = 2, .sliding_window = 512 };
    inline for (@typeInfo(@TypeOf(expected)).@"struct".field_names) |field| {
        const value = obj.get(field) orelse return error.InvalidEmbeddingGemma2Config;
        if (value != .integer or value.integer != @field(expected, field)) return error.UnsupportedEmbeddingGemma2Geometry;
    }
    const layers = obj.get("layer_types") orelse return error.InvalidEmbeddingGemma2Config;
    if (layers != .array or layers.array.items.len != cfg.num_hidden_layers) return error.InvalidEmbeddingGemma2Config;
    for (layers.array.items, 0..) |value, i| {
        if (value != .string or !std.mem.eql(u8, value.string, if ((i + 1) % 6 == 0) "full_attention" else "sliding_attention")) return error.UnsupportedEmbeddingGemma2Geometry;
    }
    const overrides = obj.get("per_layer_config") orelse return error.InvalidEmbeddingGemma2Config;
    if (overrides != .object or overrides.object.count() != 4) return error.UnsupportedEmbeddingGemma2Geometry;
    for ([_][]const u8{ "05", "11", "17", "23" }) |key| {
        const value = overrides.object.get(key) orelse return error.UnsupportedEmbeddingGemma2Geometry;
        if (value != .object) return error.InvalidEmbeddingGemma2Config;
        const hd = value.object.get("head_dim") orelse return error.InvalidEmbeddingGemma2Config;
        const kv = value.object.get("num_key_value_heads") orelse return error.InvalidEmbeddingGemma2Config;
        if (hd != .integer or hd.integer != 512 or kv != .integer or kv.integer != 1) return error.UnsupportedEmbeddingGemma2Geometry;
    }
    const activation = obj.get("hidden_activation") orelse return error.InvalidEmbeddingGemma2Config;
    if (activation != .string or !std.mem.eql(u8, activation.string, "gelu_pytorch_tanh")) return error.UnsupportedEmbeddingGemma2Geometry;
    const bias = obj.get("attention_bias") orelse return error.InvalidEmbeddingGemma2Config;
    if (bias != .bool or bias.bool) return error.UnsupportedEmbeddingGemma2Geometry;
    try requireNumber(obj, "attention_dropout", 0);
    try requireNumber(obj, "rms_norm_eps", 1e-6);
    try requireNumber(obj, "bos_token_id", 2);
    try requireNumber(obj, "eos_token_id", 1);
    try requireNumber(obj, "pad_token_id", 0);
    const rope = obj.get("rope_parameters") orelse return error.InvalidEmbeddingGemma2Config;
    if (rope != .object) return error.InvalidEmbeddingGemma2Config;
    inline for (.{ "sliding_attention", "full_attention" }, .{ @as(f64, 10000), @as(f64, 1000000) }) |kind, theta_value| {
        const params = rope.object.get(kind) orelse return error.InvalidEmbeddingGemma2Config;
        if (params != .object) return error.InvalidEmbeddingGemma2Config;
        const rope_type = params.object.get("rope_type") orelse return error.InvalidEmbeddingGemma2Config;
        if (rope_type != .string or !std.mem.eql(u8, rope_type.string, "default")) return error.UnsupportedEmbeddingGemma2Geometry;
        try requireNumber(params.object, "rope_theta", theta_value);
    }
    if (root.get("vision_config")) |tower| if (tower != .null) {
        if (tower != .object) return error.InvalidEmbeddingGemma2Config;
        const geometry = .{ .hidden_size = 768, .intermediate_size = 3072, .num_hidden_layers = 16, .num_attention_heads = 12, .num_key_value_heads = 12, .head_dim = 64, .patch_size = 16, .pooling_kernel_size = 3, .position_embedding_size = 10240 };
        inline for (@typeInfo(@TypeOf(geometry)).@"struct".field_names) |field| try requireNumber(tower.object, field, @field(geometry, field));
        try requireNumber(tower.object, "rms_norm_eps", 1e-6);
        try requireString(tower.object, "model_type", "gemma4_vision");
        try requireString(tower.object, "hidden_activation", "gelu_pytorch_tanh");
        const vision_rope = tower.object.get("rope_parameters") orelse return error.InvalidEmbeddingGemma2Config;
        if (vision_rope != .object) return error.InvalidEmbeddingGemma2Config;
        try requireString(vision_rope.object, "rope_type", "axial");
        try requireNumber(vision_rope.object, "rope_theta", 100);
        try requireNumber(root, "image_token_id", 258880);
        try requireNumber(root, "video_token_id", 258884);
        try requireNumber(root, "boi_token_id", 255999);
        try requireNumber(root, "eoi_token_id", 258882);
        try requireNumber(root, "vision_soft_tokens_per_image", 280);
        for ([_][]const u8{ "standardize", "use_clipped_linears", "attention_bias" }) |field| {
            const v = tower.object.get(field) orelse return error.InvalidEmbeddingGemma2Config;
            if (v != .bool or v.bool) return error.UnsupportedEmbeddingGemma2Geometry;
        }
        cfg.vision = true;
    };
    if (root.get("audio_config")) |tower| if (tower != .null) {
        if (tower != .object) return error.InvalidEmbeddingGemma2Config;
        const geometry = .{ .hidden_size = 1024, .num_hidden_layers = 12, .num_attention_heads = 8, .output_proj_dims = 1536, .conv_kernel_size = 5, .attention_chunk_size = 12, .attention_context_left = 13, .attention_context_right = 0 };
        inline for (@typeInfo(@TypeOf(geometry)).@"struct".field_names) |field| try requireNumber(tower.object, field, @field(geometry, field));
        try requireNumber(tower.object, "rms_norm_eps", 1e-6);
        try requireNumber(tower.object, "residual_weight", 0.5);
        try requireString(tower.object, "model_type", "gemma4_audio");
        try requireString(tower.object, "hidden_act", "silu");
        try requireNumber(tower.object, "attention_logit_cap", 50);
        try requireNumber(tower.object, "attention_invalid_logits_value", -1e9);
        try requireNumber(tower.object, "gradient_clipping", 1e10);
        const clipped = tower.object.get("use_clipped_linears") orelse return error.InvalidEmbeddingGemma2Config;
        if (clipped != .bool or !clipped.bool) return error.UnsupportedEmbeddingGemma2Geometry;
        const channels = tower.object.get("subsampling_conv_channels") orelse return error.InvalidEmbeddingGemma2Config;
        if (channels != .array or channels.array.items.len != 2) return error.UnsupportedEmbeddingGemma2Geometry;
        for (channels.array.items, [_]i64{ 128, 32 }) |v, wanted| if (v != .integer or v.integer != wanted) return error.UnsupportedEmbeddingGemma2Geometry;
        try requireNumber(root, "audio_token_id", 258881);
        try requireNumber(root, "boa_token_id", 256000);
        try requireNumber(root, "eoa_token_index", 258883);
        cfg.audio = true;
    };
    return cfg;
}

fn requireNumber(obj: std.json.ObjectMap, key: []const u8, expected: f64) !void {
    const v = obj.get(key) orelse return error.InvalidEmbeddingGemma2Config;
    const n: f64 = switch (v) {
        .integer => @floatFromInt(v.integer),
        .float => v.float,
        else => return error.InvalidEmbeddingGemma2Config,
    };
    if (!std.math.isFinite(n) or n != expected) return error.UnsupportedEmbeddingGemma2Geometry;
}

fn requireString(obj: std.json.ObjectMap, key: []const u8, expected: []const u8) !void {
    const v = obj.get(key) orelse return error.InvalidEmbeddingGemma2Config;
    if (v != .string or !std.mem.eql(u8, v.string, expected)) return error.UnsupportedEmbeddingGemma2Geometry;
}
