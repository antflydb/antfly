// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Small real one-block patch encoder fixture with an independent scalar oracle.
const std = @import("std");
const native = @import("../../ops/native_compute.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
pub const pixels = [_]f32{ 4, 4, 4, 1, 1, 1, 1, 4, 0, 0, 0, 0 };
pub const patch_tokens = [_]f32{ 4, 1, 4, 1, 4, 1, 1, 4 };
const ones = [_]f32{ 1, 1 };
const zeros = [_]f32{ 0, 0 };
const matrix_zero = [_]f32{ 0, 0, 0, 0 };
const position_zero = [_]f32{ 0, 0, 0, 0, 0, 0, 0, 0 };
pub const projection = [_]f32{ 1, 2, 3, 4, 6, 9 };
pub const Weight = struct { name: []const u8, shape: []const i64, values: []const f32 };
pub const weights = [_]Weight{
    .{ .name = "vision_tower.vision_model.embeddings.patch_embedding.weight", .shape = &.{ 2, 3, 1, 1 }, .values = &.{ 1, 0, 0, 0, 1, 0 } },
    .{ .name = "vision_tower.vision_model.embeddings.patch_embedding.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.embeddings.position_embedding.weight", .shape = &.{ 4, 2 }, .values = &position_zero },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.layer_norm1.weight", .shape = &.{2}, .values = &ones },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.layer_norm1.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.q_proj.weight", .shape = &.{ 2, 2 }, .values = &matrix_zero },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.q_proj.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.k_proj.weight", .shape = &.{ 2, 2 }, .values = &matrix_zero },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.k_proj.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.v_proj.weight", .shape = &.{ 2, 2 }, .values = &matrix_zero },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.v_proj.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.out_proj.weight", .shape = &.{ 2, 2 }, .values = &matrix_zero },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.self_attn.out_proj.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.layer_norm2.weight", .shape = &.{2}, .values = &ones },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.layer_norm2.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.mlp.fc1.weight", .shape = &.{ 2, 2 }, .values = &matrix_zero },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.mlp.fc1.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.mlp.fc2.weight", .shape = &.{ 2, 2 }, .values = &matrix_zero },
    .{ .name = "vision_tower.vision_model.encoder.layers.0.mlp.fc2.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "vision_tower.vision_model.post_layernorm.weight", .shape = &.{2}, .values = &ones },
    .{ .name = "vision_tower.vision_model.post_layernorm.bias", .shape = &.{2}, .values = &zeros },
    .{ .name = "multi_modal_projector.mm_soft_emb_norm.weight", .shape = &.{2}, .values = &ones },
    .{ .name = "multi_modal_projector.mm_input_projection_weight", .shape = &.{ 2, 3 }, .values = &projection },
};
pub fn config() @import("../../models/gpt.zig").Config {
    return .{ .family = .gemma, .hidden_size = 3, .image_token_index = 7, .mm_tokens_per_image = 1, .vision_hidden_size = 2, .vision_patch_size = 1, .vision_image_size = 2, .vision_num_hidden_layers = 1, .vision_num_attention_heads = 1, .vision_intermediate_size = 2, .norm_weight_offset = 1 };
}
pub fn populateWithout(store: *native.WeightStore, omitted: ?[]const u8) !void {
    const a = store.allocator;
    for (weights) |weight| {
        if (omitted) |name| if (std.mem.eql(u8, name, weight.name)) continue;
        const key = try a.dupe(u8, weight.name);
        errdefer a.free(key);
        var tensor = try Tensor.initFloat32(a, key, weight.shape, weight.values);
        errdefer tensor.deinit();
        try store.resident_weights.put(a, key, .{ .tensor = tensor });
    }
}
pub fn populate(store: *native.WeightStore) !void {
    try populateWithout(store, null);
}
pub fn noProjection(store: *native.WeightStore) !void {
    try populateWithout(store, "multi_modal_projector.mm_input_projection_weight");
}
pub fn noSecondNorm(store: *native.WeightStore) !void {
    try populateWithout(store, "vision_tower.vision_model.encoder.layers.0.layer_norm2.weight");
}
/// Q/K/V and MLP are zero, so the two residual blocks are exactly identity.
/// Three [4,1] patches and one [1,4] give this independently derived pooled
/// post-layernorm vector. No production attention/norm/projector helper used.
pub fn pooled() [2]f32 {
    const normalized: f32 = 1.5 / @sqrt(@as(f32, 2.25) + 1e-6);
    return .{ normalized / 2, -normalized / 2 };
}
pub fn normed() [2]f32 {
    const pool = pooled();
    const scaled = pool[0] / @sqrt(pool[0] * pool[0] + 1e-6) * 2;
    return .{ scaled, -scaled };
}
pub fn expected() [3]f32 {
    const value = normed()[0];
    return .{ -3 * value, -4 * value, -6 * value };
}
pub fn expectProjected(actual: []const f32) !void {
    const oracle = expected();
    try std.testing.expectEqual(@as(usize, 3), actual.len);
    for (actual, oracle) |value, gold| try std.testing.expectApproxEqAbs(gold, value, 1e-5);
}

/// A distinct genuinely nonzero Q/K/V/output/MLP case. The frozen QA receipt
/// contains its independent scalar softmax/GELU/normalization calculation.
pub fn populateNonzero(store: *native.WeightStore) !void {
    const a = store.allocator;
    for (weights) |weight| {
        var values = weight.values;
        if (std.mem.endsWith(u8, weight.name, "self_attn.q_proj.weight")) values = &.{ 0.5, 0, 0, 0.5 };
        if (std.mem.endsWith(u8, weight.name, "self_attn.k_proj.weight")) values = &.{ 0.25, 0, 0, 0.25 };
        if (std.mem.endsWith(u8, weight.name, "self_attn.v_proj.weight")) values = &.{ 1, 0, 0, 1 };
        if (std.mem.endsWith(u8, weight.name, "self_attn.out_proj.weight")) values = &.{ 0.25, 0, 0, 0.25 };
        if (std.mem.endsWith(u8, weight.name, "mlp.fc1.weight")) values = &.{ 2, 0, 0, 2 };
        if (std.mem.endsWith(u8, weight.name, "mlp.fc2.weight")) values = &.{ 0, 8, 0, 0 };
        if (std.mem.endsWith(u8, weight.name, "post_layernorm.bias")) values = &.{ 0.25, 0 };
        const key = try a.dupe(u8, weight.name);
        errdefer a.free(key);
        var tensor = try Tensor.initFloat32(a, key, weight.shape, values);
        errdefer tensor.deinit();
        try store.resident_weights.put(a, key, .{ .tensor = tensor });
    }
}
