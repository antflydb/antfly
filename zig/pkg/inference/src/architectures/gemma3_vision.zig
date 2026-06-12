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
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;
const gpt_mod = @import("../models/gpt.zig");

pub const DebugOutputs = struct {
    allocator: std.mem.Allocator,
    patch_tokens: []f32,
    positioned_tokens: []f32,
    pooled_tokens: []f32,
    soft_normed_tokens: []f32,
    projected_tokens: []f32,

    pub fn deinit(self: *DebugOutputs) void {
        self.allocator.free(self.patch_tokens);
        self.allocator.free(self.positioned_tokens);
        self.allocator.free(self.pooled_tokens);
        self.allocator.free(self.soft_normed_tokens);
        self.allocator.free(self.projected_tokens);
    }
};

/// Run Gemma 3's SigLIP-style vision tower and projector.
/// `pixel_values` is [batch, 3, image_size, image_size] in CHW order.
/// Returns projected image token embeddings [batch * mm_tokens_per_image, hidden_size].
pub fn encodeProjectedImageTokens(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: gpt_mod.Config,
    pixel_values: []const f32,
    batch: usize,
) ![]f32 {
    var debug = try encodeProjectedImageTokensDebug(cb, allocator, cfg, pixel_values, batch);
    defer debug.deinit();
    return allocator.dupe(f32, debug.projected_tokens);
}

pub fn encodeProjectedImageTokensDebug(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: gpt_mod.Config,
    pixel_values: []const f32,
    batch: usize,
) !DebugOutputs {
    if (!cfg.isMultimodal()) return error.InvalidMultimodalConfig;

    const vision_hidden: usize = cfg.vision_hidden_size;
    const patch_size: usize = cfg.vision_patch_size;
    const image_size: usize = cfg.vision_image_size;
    const grid = image_size / patch_size;
    const num_patches = grid * grid;
    const patch_dim = 3 * patch_size * patch_size;

    const patch_embeddings = try patchEmbed(cb, allocator, pixel_values, batch, patch_size, image_size, grid, patch_dim, vision_hidden);
    errdefer allocator.free(patch_embeddings);

    const positioned = try addPositionEmbeddings(cb, allocator, patch_embeddings, batch, num_patches, vision_hidden);
    errdefer allocator.free(positioned);

    const hidden_shape = [_]i32{ @intCast(batch * num_patches), @intCast(vision_hidden) };
    var hidden = try cb.fromFloat32Shape(positioned, &hidden_shape);
    errdefer cb.free(hidden);

    for (0..cfg.vision_num_hidden_layers) |layer| {
        const next_hidden = try encoderBlock(cb, allocator, hidden, batch, num_patches, vision_hidden, cfg.vision_num_attention_heads, cfg.vision_intermediate_size, layer);
        cb.free(hidden);
        hidden = next_hidden;
    }

    {
        const gamma = try cb.getWeight("vision_tower.vision_model.post_layernorm.weight");
        defer cb.free(gamma);
        const beta = try cb.getWeight("vision_tower.vision_model.post_layernorm.bias");
        defer cb.free(beta);
        const normed = try cb.layerNorm(hidden, gamma, beta, vision_hidden, 1e-6);
        cb.free(hidden);
        hidden = normed;
    }

    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);
    cb.free(hidden);

    const merged = try averagePoolPatches(allocator, hidden_data, batch, grid, vision_hidden, cfg.mm_tokens_per_image);
    errdefer allocator.free(merged);

    const merged_shape = [_]i32{ @intCast(batch * cfg.mm_tokens_per_image), @intCast(vision_hidden) };
    const merged_ct = try cb.fromFloat32Shape(merged, &merged_shape);
    defer cb.free(merged_ct);

    const soft_norm_w = try cb.getWeight("multi_modal_projector.mm_soft_emb_norm.weight");
    defer cb.free(soft_norm_w);
    const soft_norm = blk: {
        if (std.math.approxEqAbs(f32, cfg.norm_weight_offset, 0.0, 1e-6)) break :blk soft_norm_w;
        const soft_norm_data = try cb.toFloat32(soft_norm_w, allocator);
        defer allocator.free(soft_norm_data);
        for (soft_norm_data) |*value| value.* += cfg.norm_weight_offset;
        const soft_norm_shape = [_]i32{ @intCast(vision_hidden) };
        break :blk try cb.fromFloat32Shape(soft_norm_data, &soft_norm_shape);
    };
    defer if (soft_norm != soft_norm_w) cb.free(soft_norm);
    const normed = try cb.rmsNorm(merged_ct, soft_norm, vision_hidden, 1e-6);
    defer cb.free(normed);
    const normed_data = try cb.toFloat32(normed, allocator);
    errdefer allocator.free(normed_data);

    const proj_w = try cb.getWeight("multi_modal_projector.mm_input_projection_weight");
    defer cb.free(proj_w);
    const proj_w_data = try cb.toFloat32(proj_w, allocator);
    defer allocator.free(proj_w_data);
    const proj_w_t = try transposeMatrix(allocator, proj_w_data, vision_hidden, cfg.hidden_size);
    defer allocator.free(proj_w_t);
    const proj_w_shape = [_]i32{ @intCast(cfg.hidden_size), @intCast(vision_hidden) };
    const proj_w_ct = try cb.fromFloat32Shape(proj_w_t, &proj_w_shape);
    defer cb.free(proj_w_ct);
    const projected = try cb.linearNoBias(normed, proj_w_ct, batch * cfg.mm_tokens_per_image, vision_hidden, cfg.hidden_size);
    defer cb.free(projected);
    const projected_data = try cb.toFloat32(projected, allocator);
    errdefer allocator.free(projected_data);

    return .{
        .allocator = allocator,
        .patch_tokens = patch_embeddings,
        .positioned_tokens = positioned,
        .pooled_tokens = merged,
        .soft_normed_tokens = normed_data,
        .projected_tokens = projected_data,
    };
}

fn transposeMatrix(allocator: std.mem.Allocator, input: []const f32, rows: usize, cols: usize) ![]f32 {
    if (input.len != rows * cols) return error.InvalidTensorShape;
    const transposed = try allocator.alloc(f32, input.len);
    for (0..rows) |row| {
        for (0..cols) |col| {
            transposed[col * rows + row] = input[row * cols + col];
        }
    }
    return transposed;
}

fn patchEmbed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    pixel_values: []const f32,
    batch: usize,
    patch_size: usize,
    image_size: usize,
    grid: usize,
    patch_dim: usize,
    vision_hidden: usize,
) ![]f32 {
    const num_patches = grid * grid;
    const patch_w = try cb.getWeight("vision_tower.vision_model.embeddings.patch_embedding.weight");
    defer cb.free(patch_w);
    const patch_b = try cb.getWeight("vision_tower.vision_model.embeddings.patch_embedding.bias");
    defer cb.free(patch_b);
    const pixel_shape = [_]i32{
        @intCast(batch),
        3,
        @intCast(image_size),
        @intCast(image_size),
    };
    const pixels_ct = try cb.fromFloat32Shape(pixel_values, &pixel_shape);
    defer cb.free(pixels_ct);

    const conv = try cb.conv2d(
        pixels_ct,
        patch_w,
        patch_b,
        batch,
        3,
        vision_hidden,
        image_size,
        image_size,
        patch_size,
        patch_size,
        patch_size,
        patch_size,
        0,
        0,
        1,
    );
    defer cb.free(conv);
    const conv_data = try cb.toFloat32(conv, allocator);
    defer allocator.free(conv_data);
    if (conv_data.len != batch * vision_hidden * num_patches) return error.InvalidPatchEmbeddingShape;

    const embedded = try allocator.alloc(f32, batch * num_patches * vision_hidden);
    for (0..batch) |b| {
        for (0..vision_hidden) |ch| {
            const src_base = ((b * vision_hidden) + ch) * num_patches;
            for (0..num_patches) |patch_idx| {
                embedded[(b * num_patches + patch_idx) * vision_hidden + ch] = conv_data[src_base + patch_idx];
            }
        }
    }
    _ = patch_dim;
    return embedded;
}

fn addPositionEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    patch_embeddings: []const f32,
    batch: usize,
    num_patches: usize,
    vision_hidden: usize,
) ![]f32 {
    const pos_w = try cb.getWeight("vision_tower.vision_model.embeddings.position_embedding.weight");
    defer cb.free(pos_w);
    const pos_data = try cb.toFloat32(pos_w, allocator);
    defer allocator.free(pos_data);

    if (pos_data.len != num_patches * vision_hidden) return error.InvalidPositionEmbeddingShape;

    const full = try allocator.alloc(f32, batch * num_patches * vision_hidden);
    for (0..batch) |b| {
        for (0..num_patches) |patch_idx| {
            const dst = (b * num_patches + patch_idx) * vision_hidden;
            const src = dst;
            const pos = patch_idx * vision_hidden;
            for (0..vision_hidden) |i| {
                full[dst + i] = patch_embeddings[src + i] + pos_data[pos + i];
            }
        }
    }
    return full;
}

fn averagePoolPatches(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    batch: usize,
    grid: usize,
    vision_hidden: usize,
    expected_tokens: u32,
) ![]f32 {
    const expected_side = exactSquareRoot(expected_tokens) orelse return error.InvalidImageTokenCount;
    if (grid % expected_side != 0) return error.InvalidPatchMergeFactor;
    const merge = grid / expected_side;
    const pooled_tokens = expected_side * expected_side;
    const pooled = try allocator.alloc(f32, batch * pooled_tokens * vision_hidden);

    for (0..batch) |b| {
        for (0..expected_side) |out_y| {
            for (0..expected_side) |out_x| {
                const dst_token = b * pooled_tokens + out_y * expected_side + out_x;
                const dst = dst_token * vision_hidden;
                @memset(pooled[dst..][0..vision_hidden], 0);
                for (0..merge) |dy| {
                    for (0..merge) |dx| {
                        const src_patch = b * grid * grid + (out_y * merge + dy) * grid + (out_x * merge + dx);
                        const src = src_patch * vision_hidden;
                        for (0..vision_hidden) |i| {
                            pooled[dst + i] += hidden[src + i];
                        }
                    }
                }
                const denom: f32 = @floatFromInt(merge * merge);
                for (0..vision_hidden) |i| pooled[dst + i] /= denom;
            }
        }
    }

    return pooled;
}

fn encoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    hidden: usize,
    num_heads: u32,
    intermediate: u32,
    layer: usize,
) !CT {
    const total = batch * seq_len;
    const head_dim = hidden / num_heads;
    var buf: [160]u8 = undefined;

    const ln1_g = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm1.weight", .{layer}));
    defer cb.free(ln1_g);
    const ln1_b = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm1.bias", .{layer}));
    defer cb.free(ln1_b);
    const normed1 = try cb.layerNorm(input, ln1_g, ln1_b, hidden, 1e-6);
    defer cb.free(normed1);

    const attn_out = try selfAttention(cb, allocator, normed1, batch, seq_len, hidden, num_heads, head_dim, layer, &buf);
    defer cb.free(attn_out);
    const res1 = try cb.add(input, attn_out);

    const ln2_g = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm2.weight", .{layer}));
    defer cb.free(ln2_g);
    const ln2_b = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm2.bias", .{layer}));
    defer cb.free(ln2_b);
    const normed2 = try cb.layerNorm(res1, ln2_g, ln2_b, hidden, 1e-6);
    defer cb.free(normed2);

    const fc1_w = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc1.weight", .{layer}));
    defer cb.free(fc1_w);
    const fc1_b = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc1.bias", .{layer}));
    defer cb.free(fc1_b);
    const fc1_out = try cb.linear(normed2, fc1_w, fc1_b, total, hidden, intermediate);
    defer cb.free(fc1_out);
    const activated = try cb.gelu(fc1_out);
    defer cb.free(activated);

    const fc2_w = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc2.weight", .{layer}));
    defer cb.free(fc2_w);
    const fc2_b = try cb.getWeight(try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc2.bias", .{layer}));
    defer cb.free(fc2_b);
    const fc2_out = try cb.linear(activated, fc2_w, fc2_b, total, intermediate, hidden);
    defer cb.free(fc2_out);

    const res2 = try cb.add(res1, fc2_out);
    cb.free(res1);
    return res2;
}

fn selfAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    hidden: usize,
    num_heads: u32,
    head_dim: usize,
    layer: usize,
    buf: *[160]u8,
) !CT {
    const total = batch * seq_len;

    const q_w = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.q_proj.weight", .{layer}));
    defer cb.free(q_w);
    const q_b = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.q_proj.bias", .{layer}));
    defer cb.free(q_b);
    const q = try cb.linear(input, q_w, q_b, total, hidden, hidden);
    defer cb.free(q);

    const k_w = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.k_proj.weight", .{layer}));
    defer cb.free(k_w);
    const k_b = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.k_proj.bias", .{layer}));
    defer cb.free(k_b);
    const k = try cb.linear(input, k_w, k_b, total, hidden, hidden);
    defer cb.free(k);

    const v_w = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.v_proj.weight", .{layer}));
    defer cb.free(v_w);
    const v_b = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.v_proj.bias", .{layer}));
    defer cb.free(v_b);
    const v = try cb.linear(input, v_w, v_b, total, hidden, hidden);
    defer cb.free(v);

    const mask = try allocator.alloc(i64, batch * seq_len);
    defer allocator.free(mask);
    @memset(mask, 1);
    const attn = try cb.scaledDotProductAttention(q, k, v, mask, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);

    const out_w = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.out_proj.weight", .{layer}));
    defer cb.free(out_w);
    const out_b = try cb.getWeight(try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.out_proj.bias", .{layer}));
    defer cb.free(out_b);
    return cb.linear(attn, out_w, out_b, total, hidden, hidden);
}

fn exactSquareRoot(value: u32) ?usize {
    var n: usize = 0;
    while (n * n < value) : (n += 1) {}
    return if (n * n == value) n else null;
}

fn fmt(buf: *[160]u8, comptime format: []const u8, args: anytype) ![]const u8 {
    return std.fmt.bufPrint(buf, format, args) catch return error.WeightNameTooLong;
}
