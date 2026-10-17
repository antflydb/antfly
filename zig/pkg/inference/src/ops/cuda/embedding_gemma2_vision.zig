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

//! Typed execution plan for the official EmbeddingGemma2 vision tower.
//! CUDA owns device allocation and launches; this module keeps the admitted
//! topology and immutable Hugging Face weight contract reviewable and pure.
const std = @import("std");

pub const hidden_size: usize = 768;
pub const intermediate_size: usize = 3072;
pub const layer_count: usize = 16;
pub const head_count: usize = 12;
pub const head_dim: usize = 64;
pub const rms_norm_eps: f32 = 1e-6;
pub const rope_theta: f32 = 100.0;
pub const q_attention_scale: f32 = 8.0;
pub const max_output_tokens: usize = 280;
pub const spatial_merge_size: usize = 3;
pub const max_patches: usize = max_output_tokens * spatial_merge_size * spatial_merge_size;

pub const Plan = struct {
    sequence: usize,
    grid_x: usize,
    grid_y: usize,

    pub fn validate(self: Plan) !void {
        if (self.sequence == 0 or self.sequence > max_patches) return error.InvalidEmbeddingGemma2VisionShape;
        if (self.grid_x == 0 or self.grid_y == 0) return error.InvalidEmbeddingGemma2VisionShape;
        const patches = std.math.mul(usize, self.grid_x, self.grid_y) catch return error.InvalidEmbeddingGemma2VisionShape;
        if (patches != self.sequence) return error.InvalidEmbeddingGemma2VisionShape;
        if (self.sequence > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2VisionShape;
        _ = std.math.mul(usize, self.sequence, intermediate_size) catch return error.InvalidEmbeddingGemma2VisionShape;
    }
};

pub const LayerWeights = struct {
    input_norm: []const u8,
    q: []const u8,
    q_norm: []const u8,
    k: []const u8,
    k_norm: []const u8,
    v: []const u8,
    out: []const u8,
    post_attention_norm: []const u8,
    pre_ffn_norm: []const u8,
    gate: []const u8,
    up: []const u8,
    down: []const u8,
    post_ffn_norm: []const u8,
};

pub const WeightNameStorage = struct {
    values: [13][128]u8 = undefined,
};

pub fn layerWeights(storage: *WeightNameStorage, layer: usize) !LayerWeights {
    if (layer >= layer_count) return error.InvalidEmbeddingGemma2VisionLayer;
    const prefix = "vision_tower.encoder.layers";
    return .{
        .input_norm = try name(&storage.values[0], prefix, layer, "input_layernorm.weight"),
        .q = try name(&storage.values[1], prefix, layer, "self_attn.q_proj.linear.weight"),
        .q_norm = try name(&storage.values[2], prefix, layer, "self_attn.q_norm.weight"),
        .k = try name(&storage.values[3], prefix, layer, "self_attn.k_proj.linear.weight"),
        .k_norm = try name(&storage.values[4], prefix, layer, "self_attn.k_norm.weight"),
        .v = try name(&storage.values[5], prefix, layer, "self_attn.v_proj.linear.weight"),
        .out = try name(&storage.values[6], prefix, layer, "self_attn.o_proj.linear.weight"),
        .post_attention_norm = try name(&storage.values[7], prefix, layer, "post_attention_layernorm.weight"),
        .pre_ffn_norm = try name(&storage.values[8], prefix, layer, "pre_feedforward_layernorm.weight"),
        .gate = try name(&storage.values[9], prefix, layer, "mlp.gate_proj.linear.weight"),
        .up = try name(&storage.values[10], prefix, layer, "mlp.up_proj.linear.weight"),
        .down = try name(&storage.values[11], prefix, layer, "mlp.down_proj.linear.weight"),
        .post_ffn_norm = try name(&storage.values[12], prefix, layer, "post_feedforward_layernorm.weight"),
    };
}

fn name(buffer: []u8, prefix: []const u8, layer: usize, suffix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}.{d}.{s}", .{ prefix, layer, suffix });
}

test "official vision plan rejects inconsistent and overflowing geometry" {
    try (Plan{ .sequence = 2394, .grid_x = 57, .grid_y = 42 }).validate();
    try std.testing.expectError(error.InvalidEmbeddingGemma2VisionShape, (Plan{ .sequence = 2394, .grid_x = 42, .grid_y = 42 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2VisionShape, (Plan{ .sequence = max_patches + 1, .grid_x = max_patches + 1, .grid_y = 1 }).validate());
}

test "official vision weights use exact immutable Hugging Face names" {
    var storage: WeightNameStorage = .{};
    const weights = try layerWeights(&storage, 15);
    try std.testing.expectEqualStrings("vision_tower.encoder.layers.15.self_attn.q_proj.linear.weight", weights.q);
    try std.testing.expectEqualStrings("vision_tower.encoder.layers.15.mlp.down_proj.linear.weight", weights.down);
    try std.testing.expectError(error.InvalidEmbeddingGemma2VisionLayer, layerWeights(&storage, layer_count));
}
