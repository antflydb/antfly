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

// CLIP text encoder and vision encoder (ViT) forward passes.
//
// Text encoder: token embed + position embed → causal self-attention layers → LayerNorm → EOS pooling → text_projection.
// Vision encoder: patch embed → CLS prepend → position embed → pre-LN → bidirectional attention layers → post-LN → CLS pooling → visual_projection.
//
// Both use quick GELU activation: x * sigmoid(1.702 * x).

const std = @import("std");
const ops = @import("../ops/ops.zig");
const native_compute = @import("../ops/native_compute.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;
const clip_mod = @import("../models/clip.zig");

// ---- Text Encoder ----

/// Run the CLIP text encoder. Returns [batch, projection_dim] text embeddings.
pub fn textEncoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clip_mod.Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const H: usize = cfg.text_hidden_size;
    const total = batch * seq_len;

    // Token embeddings
    const tok_w = try cb.getWeight("text_model.embeddings.token_embedding.weight");
    defer cb.free(tok_w);
    var hidden = try cb.embeddingLookup(tok_w, input_ids, total, H);

    // Position embeddings (learned, repeated across batch)
    {
        const pos_w = try cb.getWeight("text_model.embeddings.position_embedding.weight");
        defer cb.free(pos_w);
        const pos_ids = try allocator.alloc(i64, total);
        defer allocator.free(pos_ids);
        for (0..batch) |b| {
            for (0..seq_len) |i| pos_ids[b * seq_len + i] = @intCast(i);
        }

        const pos_ct = try cb.embeddingLookup(pos_w, pos_ids, total, H);
        defer cb.free(pos_ct);
        const added = try cb.add(hidden, pos_ct);
        cb.free(hidden);
        hidden = added;
    }

    // Encoder layers (causal attention)
    for (0..cfg.text_num_layers) |layer| {
        const new = try encoderBlock(cb, allocator, hidden, "text_model.encoder.layers", layer, batch, seq_len, cfg.text_num_heads, cfg.textHeadDim(), H, cfg.text_intermediate_size, true);
        cb.free(hidden);
        hidden = new;
    }

    // Final layer norm
    {
        const gamma = try cb.getWeight("text_model.final_layer_norm.weight");
        defer cb.free(gamma);
        const beta = try cb.getWeight("text_model.final_layer_norm.bias");
        defer cb.free(beta);
        const normed = try cb.layerNorm(hidden, gamma, beta, H, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    // EOS token pooling: take embedding at last non-padding position per batch
    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);
    cb.free(hidden);

    const eos_embeddings = try allocator.alloc(f32, batch * H);
    for (0..batch) |b| {
        var eos_pos: usize = 0;
        for (0..seq_len) |s| {
            if (input_ids[b * seq_len + s] != 0) eos_pos = s;
        }
        @memcpy(eos_embeddings[b * H ..][0..H], hidden_data[(b * seq_len + eos_pos) * H ..][0..H]);
    }

    // Text projection (linear, no bias)
    const proj_w = cb.getWeight("text_projection.weight") catch {
        return eos_embeddings;
    };
    defer cb.free(proj_w);
    const eos_shape = [_]i32{ @intCast(batch), @intCast(H) };
    const eos_ct = try cb.fromFloat32Shape(eos_embeddings, &eos_shape);
    defer cb.free(eos_ct);
    allocator.free(eos_embeddings);

    const projected = try cb.linearNoBias(eos_ct, proj_w, batch, H, cfg.projection_dim);
    defer cb.free(projected);
    return try cb.toFloat32(projected, allocator);
}

// ---- Vision Encoder (ViT) ----

/// Run the CLIP vision encoder. pixel_values: [batch, 3, img_size, img_size].
/// Returns [batch, projection_dim] vision embeddings.
pub fn visionEncoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clip_mod.Config,
    pixel_values: []const f32,
    batch: usize,
) ![]f32 {
    const H: usize = cfg.vision_hidden_size;
    const P: usize = cfg.patch_size;
    const img_size: usize = cfg.image_size;
    const grid = img_size / P;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;
    const full_seq = num_patches + 1; // CLS + patches

    // Extract patches: [batch, 3, img, img] → [batch*num_patches, 3*P*P]
    //
    // The innermost x-loop walks P contiguous floats in both source and
    // destination, so it collapses to a memcpy of one patch row per
    // (channel, y).  Saves the per-element index arithmetic (5 muls + adds
    // per scalar) on the hottest preprocessing step of the vision encoder.
    const patches = try allocator.alloc(f32, batch * num_patches * patch_dim);
    defer allocator.free(patches);
    const channel_stride = img_size * img_size;
    const image_stride = 3 * channel_stride;
    for (0..batch) |b| {
        const img_base = b * image_stride;
        for (0..grid) |ph| {
            const row_base = ph * P;
            for (0..grid) |pw| {
                const pidx = b * num_patches + ph * grid + pw;
                const patch_base = pidx * patch_dim;
                const col_base = pw * P;
                for (0..3) |ch| {
                    const ch_dst_base = patch_base + ch * P * P;
                    const ch_src_base = img_base + ch * channel_stride;
                    for (0..P) |y| {
                        const dst = patches[ch_dst_base + y * P ..][0..P];
                        const src = pixel_values[ch_src_base + (row_base + y) * img_size + col_base ..][0..P];
                        @memcpy(dst, src);
                    }
                }
            }
        }
    }

    // Patch embedding via linear (Conv2d weight [H, 3, P, P] treated as [H, patch_dim]).
    // Pass the original weight through so backends can use quantized linear kernels.
    const patch_w = try cb.getWeight("vision_model.embeddings.patch_embedding.weight");
    defer cb.free(patch_w);
    const patch_shape = [_]i32{ @intCast(batch * num_patches), @intCast(patch_dim) };
    const patches_ct = try cb.fromFloat32Shape(patches, &patch_shape);
    defer cb.free(patches_ct);
    const embedded = if (cb.kind() == .native)
        try cb.linearNoBias(patches_ct, patch_w, batch * num_patches, patch_dim, H)
    else blk: {
        const patch_w_data = try cb.toFloat32(patch_w, allocator);
        defer allocator.free(patch_w_data);
        const patch_w_shape = [_]i32{ @intCast(H), @intCast(patch_dim) };
        const patch_w_ct = try cb.fromFloat32Shape(patch_w_data, &patch_w_shape);
        defer cb.free(patch_w_ct);
        break :blk try cb.linearNoBias(patches_ct, patch_w_ct, batch * num_patches, patch_dim, H);
    };
    defer cb.free(embedded);

    // Prepend class token + add position embeddings
    const embed_data = try cb.toFloat32(embedded, allocator);
    defer allocator.free(embed_data);

    const cls_w = try cb.getWeight("vision_model.embeddings.class_embedding");
    defer cb.free(cls_w);
    const cls_data = try cb.toFloat32(cls_w, allocator);
    defer allocator.free(cls_data);

    const pos_w = try cb.getWeight("vision_model.embeddings.position_embedding.weight");
    defer cb.free(pos_w);
    const pos_data = try cb.toFloat32(pos_w, allocator);
    defer allocator.free(pos_data);

    const full_data = try allocator.alloc(f32, batch * full_seq * H);
    defer allocator.free(full_data);
    for (0..batch) |b| {
        const base = b * full_seq * H;
        // CLS token at position 0 + position embedding 0
        for (0..H) |i| full_data[base + i] = cls_data[i] + pos_data[i];
        // Patch embeddings + position embeddings
        for (0..num_patches) |p| {
            const dst = base + (p + 1) * H;
            const src_embed = b * num_patches * H + p * H;
            const src_pos = (p + 1) * H;
            for (0..H) |i| full_data[dst + i] = embed_data[src_embed + i] + pos_data[src_pos + i];
        }
    }

    const hidden_shape = [_]i32{ @intCast(batch * full_seq), @intCast(H) };
    var hidden = try cb.fromFloat32Shape(full_data, &hidden_shape);

    // Pre-layernorm
    {
        const gamma = try cb.getWeight("vision_model.pre_layrnorm.weight");
        defer cb.free(gamma);
        const beta = try cb.getWeight("vision_model.pre_layrnorm.bias");
        defer cb.free(beta);
        const normed = try cb.layerNorm(hidden, gamma, beta, H, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    // Encoder layers (bidirectional attention)
    for (0..cfg.vision_num_layers) |layer| {
        const new = try encoderBlock(cb, allocator, hidden, "vision_model.encoder.layers", layer, batch, full_seq, cfg.vision_num_heads, cfg.visionHeadDim(), H, cfg.vision_intermediate_size, false);
        cb.free(hidden);
        hidden = new;
    }

    // Post-layernorm
    {
        const gamma = try cb.getWeight("vision_model.post_layernorm.weight");
        defer cb.free(gamma);
        const beta = try cb.getWeight("vision_model.post_layernorm.bias");
        defer cb.free(beta);
        const normed = try cb.layerNorm(hidden, gamma, beta, H, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    // CLS token pooling (first token per batch)
    const out_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(out_data);
    cb.free(hidden);

    const cls_out = try allocator.alloc(f32, batch * H);
    for (0..batch) |b| {
        @memcpy(cls_out[b * H ..][0..H], out_data[b * full_seq * H ..][0..H]);
    }

    // Visual projection
    const proj_w = cb.getWeight("visual_projection.weight") catch {
        return cls_out;
    };
    defer cb.free(proj_w);
    const cls_shape = [_]i32{ @intCast(batch), @intCast(H) };
    const cls_ct = try cb.fromFloat32Shape(cls_out, &cls_shape);
    defer cb.free(cls_ct);
    allocator.free(cls_out);

    const projected = try cb.linearNoBias(cls_ct, proj_w, batch, H, cfg.projection_dim);
    defer cb.free(projected);
    return try cb.toFloat32(projected, allocator);
}

// ---- Shared encoder block ----

fn encoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    prefix: []const u8,
    layer: usize,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    hidden: usize,
    intermediate: usize,
    causal: bool,
) !CT {
    const total = batch * seq_len;
    var buf: [128]u8 = undefined;

    // Pre-norm → self-attention → residual
    const ln1_g = try cb.getWeight(try fmt(&buf, "{s}.{d}.layer_norm1.weight", .{ prefix, layer }));
    const ln1_b = try cb.getWeight(try fmt(&buf, "{s}.{d}.layer_norm1.bias", .{ prefix, layer }));
    const normed1 = try cb.layerNorm(input, ln1_g, ln1_b, hidden, 1e-5);
    defer cb.free(normed1);

    const res1 = try selfAttn(cb, allocator, normed1, input, prefix, layer, batch, seq_len, num_heads, head_dim, hidden, causal, &buf);

    // Pre-norm → FFN (quickGELU) → residual
    const ln2_g = try cb.getWeight(try fmt(&buf, "{s}.{d}.layer_norm2.weight", .{ prefix, layer }));
    const ln2_b = try cb.getWeight(try fmt(&buf, "{s}.{d}.layer_norm2.bias", .{ prefix, layer }));
    const normed2 = try cb.layerNorm(res1, ln2_g, ln2_b, hidden, 1e-5);
    defer cb.free(normed2);

    var fc1_w_buf: [128]u8 = undefined;
    var fc1_b_buf: [128]u8 = undefined;
    const fc1_w = try cb.getWeight(try fmt(&fc1_w_buf, "{s}.{d}.mlp.fc1.weight", .{ prefix, layer }));
    const fc1_b = try cb.getWeight(try fmt(&fc1_b_buf, "{s}.{d}.mlp.fc1.bias", .{ prefix, layer }));
    const activated = (try cb.linearQuickGelu(normed2, fc1_w, fc1_b, total, hidden, intermediate)) orelse blk: {
        const fc1_out = try cb.linear(normed2, fc1_w, fc1_b, total, hidden, intermediate);
        defer cb.free(fc1_out);
        break :blk try cb.quickGelu(fc1_out);
    };
    defer cb.free(activated);

    var fc2_w_buf: [128]u8 = undefined;
    var fc2_b_buf: [128]u8 = undefined;
    const fc2_w = try cb.getWeight(try fmt(&fc2_w_buf, "{s}.{d}.mlp.fc2.weight", .{ prefix, layer }));
    const fc2_b = try cb.getWeight(try fmt(&fc2_b_buf, "{s}.{d}.mlp.fc2.bias", .{ prefix, layer }));
    const res2 = (try cb.linearAdd(activated, fc2_w, fc2_b, res1, total, intermediate, hidden)) orelse blk: {
        const fc2_out = try cb.linear(activated, fc2_w, fc2_b, total, intermediate, hidden);
        defer cb.free(fc2_out);
        break :blk try cb.add(res1, fc2_out);
    };
    cb.free(res1);
    return res2;
}

fn selfAttn(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    residual: CT,
    prefix: []const u8,
    layer: usize,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    hidden: usize,
    causal: bool,
    buf: *[128]u8,
) !CT {
    _ = buf;
    const total = batch * seq_len;

    var q_w_buf: [128]u8 = undefined;
    var q_b_buf: [128]u8 = undefined;
    var k_w_buf: [128]u8 = undefined;
    var k_b_buf: [128]u8 = undefined;
    var v_w_buf: [128]u8 = undefined;
    var v_b_buf: [128]u8 = undefined;
    const q_w = try cb.getWeight(try fmt(&q_w_buf, "{s}.{d}.self_attn.q_proj.weight", .{ prefix, layer }));
    const q_b = try cb.getWeight(try fmt(&q_b_buf, "{s}.{d}.self_attn.q_proj.bias", .{ prefix, layer }));
    const k_w = try cb.getWeight(try fmt(&k_w_buf, "{s}.{d}.self_attn.k_proj.weight", .{ prefix, layer }));
    const k_b = try cb.getWeight(try fmt(&k_b_buf, "{s}.{d}.self_attn.k_proj.bias", .{ prefix, layer }));
    const v_w = try cb.getWeight(try fmt(&v_w_buf, "{s}.{d}.self_attn.v_proj.weight", .{ prefix, layer }));
    const v_b = try cb.getWeight(try fmt(&v_b_buf, "{s}.{d}.self_attn.v_proj.bias", .{ prefix, layer }));

    const qkv = try cb.linearTriple(input, q_w, q_b, k_w, k_b, v_w, v_b, total, hidden, hidden);
    const Q = qkv.first;
    const K = qkv.second;
    const V = qkv.third;
    defer cb.free(Q);
    defer cb.free(K);
    defer cb.free(V);

    const attn_out = if (causal)
        try cb.causalSelfAttention(Q, K, V, null, batch, seq_len, num_heads, head_dim)
    else blk: {
        const ones = try allocator.alloc(i64, batch * seq_len);
        defer allocator.free(ones);
        @memset(ones, 1);
        break :blk try cb.scaledDotProductAttention(Q, K, V, ones, null, batch, seq_len, num_heads, head_dim);
    };
    defer cb.free(attn_out);

    var o_w_buf: [128]u8 = undefined;
    var o_b_buf: [128]u8 = undefined;
    const o_w = try cb.getWeight(try fmt(&o_w_buf, "{s}.{d}.self_attn.out_proj.weight", .{ prefix, layer }));
    const o_b = try cb.getWeight(try fmt(&o_b_buf, "{s}.{d}.self_attn.out_proj.bias", .{ prefix, layer }));
    return (try cb.linearAdd(attn_out, o_w, o_b, residual, total, hidden, hidden)) orelse blk: {
        const projected = try cb.linear(attn_out, o_w, o_b, total, hidden, hidden);
        defer cb.free(projected);
        break :blk try cb.add(residual, projected);
    };
}

fn fmt(buf: *[128]u8, comptime format: []const u8, args: anytype) ![]const u8 {
    return std.fmt.bufPrint(buf, format, args) catch return error.WeightNameTooLong;
}

test "native clip vision patch weight direct path matches explicit 2d reshape" {
    const allocator = std.testing.allocator;
    var weight_store = native_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer {
        native_compute.deinitPrefetchQueue(&weight_store);
        weight_store.resident_weights.deinit(allocator);
        weight_store.lazy_weights.deinit(allocator);
    }
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const batch = 2;
    const patches = 3;
    const channels = 3;
    const patch = 2;
    const hidden = 5;
    const patch_dim = channels * patch * patch;
    const rows = batch * patches;

    var input_data: [rows * patch_dim]f32 = undefined;
    for (&input_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt((i % 11) + 1)) * 0.01;
    var weight_data: [hidden * patch_dim]f32 = undefined;
    for (&weight_data, 0..) |*value, i| value.* = @as(f32, @floatFromInt((i % 17) + 1)) * 0.02;

    const input_ct = try cb.fromFloat32Shape(&input_data, &.{ rows, patch_dim });
    defer cb.free(input_ct);
    const direct_weight = try cb.fromFloat32Shape(&weight_data, &.{ hidden, channels, patch, patch });
    defer cb.free(direct_weight);
    const reshaped_weight = try cb.fromFloat32Shape(&weight_data, &.{ hidden, patch_dim });
    defer cb.free(reshaped_weight);

    const direct = try cb.linearNoBias(input_ct, direct_weight, rows, patch_dim, hidden);
    defer cb.free(direct);
    const reshaped = try cb.linearNoBias(input_ct, reshaped_weight, rows, patch_dim, hidden);
    defer cb.free(reshaped);

    const direct_data = try cb.toFloat32(direct, allocator);
    defer allocator.free(direct_data);
    const reshaped_data = try cb.toFloat32(reshaped, allocator);
    defer allocator.free(reshaped_data);

    try std.testing.expectEqualSlices(f32, reshaped_data, direct_data);
}
