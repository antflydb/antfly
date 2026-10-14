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

    // EOS token pooling: CLIP tokenizers use the highest token id as EOT.
    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);
    cb.free(hidden);

    const eos_embeddings = try allocator.alloc(f32, batch * H);
    for (0..batch) |b| {
        const eos_pos = clipEotPosition(input_ids, b, seq_len);
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

fn clipEotPosition(input_ids: []const i64, batch_index: usize, seq_len: usize) usize {
    var eos_pos: usize = 0;
    var eos_id: i64 = std.math.minInt(i64);
    for (0..seq_len) |s| {
        const token_id = input_ids[batch_index * seq_len + s];
        if (token_id > eos_id) {
            eos_id = token_id;
            eos_pos = s;
        }
    }
    return eos_pos;
}

test "clip text pooling selects first highest token id as eot" {
    const ids = [_]i64{
        49406, 111, 49407, 49407, 0,
        49406, 222, 333,   49407, 0,
    };

    try std.testing.expectEqual(@as(usize, 2), clipEotPosition(&ids, 0, 5));
    try std.testing.expectEqual(@as(usize, 3), clipEotPosition(&ids, 1, 5));
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
    try validateVisionInput(cfg, batch);
    const expected = try std.math.mul(usize, batch, try std.math.mul(usize, 3, try std.math.mul(usize, cfg.image_size, cfg.image_size)));
    if (pixel_values.len != expected) return error.UnexpectedOutputShape;
    if ((cb.kind() == .cuda or cb.kind() == .metal) and supportsResidentVision(cb)) {
        const pixels = try uploadVisionPixels(cb, allocator, pixel_values, batch, cfg.image_size);
        defer cb.free(pixels);
        const output = try visionEncoderForwardTensor(cb, allocator, cfg, pixels, batch);
        defer cb.free(output);
        return cb.toFloat32(output, allocator);
    }
    const P: usize = cfg.patch_size;
    const img_size: usize = cfg.image_size;
    const grid = img_size / P;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;

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

    const patch_shape = [_]i32{ @intCast(batch * num_patches), @intCast(patch_dim) };
    const patches_ct = try cb.fromFloat32Shape(patches, &patch_shape);
    defer cb.free(patches_ct);
    const output = try visionEncoderFromPatches(cb, allocator, cfg, patches_ct, batch);
    defer cb.free(output);
    return cb.toFloat32(output, allocator);
}

fn uploadVisionPixels(cb: *const ComputeBackend, allocator: std.mem.Allocator, data: []const f32, batch: usize, image_size: usize) !CT {
    const shape = [_]i32{ @intCast(batch), 3, @intCast(image_size), @intCast(image_size) };
    const input = try cb.fromFloat32Shape(data, &shape);
    if (cb.kind() != .metal) return input;
    defer cb.free(input);
    // Metal's host constructor returns a host-backed handle. Stage into an
    // explicit device buffer before patch packing so its transpose primitive
    // cannot choose a host stride view for the normalized pixel tensor.
    const device = (try cb.allocUninitF32Shape(&shape)) orelse return error.UnsupportedResidentClipVision;
    errdefer cb.free(device);
    if (!try cb.copyRows2D(allocator, device, 0, input, 0, batch * 3 * image_size, image_size))
        return error.UnsupportedResidentClipVision;
    return device;
}

/// Borrow normalized F32 NCHW pixels on the compute backend and return an owned
/// embedding tensor on that same backend. No activation is exported to host.
/// Callers free the result with cb.free; pixels remain owned by the caller.
pub fn visionEncoderForwardTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clip_mod.Config,
    pixels: CT,
    batch: usize,
) !CT {
    try validateVisionInput(cfg, batch);
    if (!supportsResidentVision(cb)) return error.UnsupportedResidentClipVision;
    const shape = try cb.tensorShape(pixels, allocator);
    defer allocator.free(shape);
    const expected_shape = [_]i64{ @intCast(batch), 3, @intCast(cfg.image_size), @intCast(cfg.image_size) };
    if (!std.mem.eql(i64, shape, &expected_shape)) return error.UnexpectedOutputShape;
    const patches = try packVisionPatchesTensor(cb, cfg, pixels, batch);
    defer cb.free(patches);
    return visionEncoderFromPatches(cb, allocator, cfg, patches, batch);
}

fn packVisionPatchesTensor(cb: *const ComputeBackend, cfg: clip_mod.Config, pixels: CT, batch: usize) !CT {
    const P: usize = cfg.patch_size;
    const grid: usize = cfg.image_size / cfg.patch_size;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;
    // NCHW -> [batch, grid_y, grid_x, channel, patch_y, patch_x].
    const shape = [_]i64{ @intCast(batch), 3, @intCast(grid), @intCast(P), @intCast(grid), @intCast(P) };
    const split = try cb.primReshape(pixels, &shape);
    defer cb.free(split);
    const packed_pixels = try cb.primTranspose(split, &.{ 0, 2, 4, 1, 3, 5 }, &shape);
    defer cb.free(packed_pixels);
    const patches = try cb.primReshape(packed_pixels, &.{ @intCast(batch * num_patches), @intCast(patch_dim) });
    return patches;
}

fn supportsResidentVision(cb: *const ComputeBackend) bool {
    return cb.vtable.tensorShape != null and cb.vtable.reshapeOp != null and cb.vtable.transposeOp != null and
        cb.vtable.concatRows2D != null and cb.vtable.takeRows != null;
}

fn validateVisionInput(cfg: clip_mod.Config, batch: usize) !void {
    if (batch == 0 or cfg.patch_size == 0 or cfg.image_size == 0 or
        cfg.image_size % cfg.patch_size != 0 or cfg.vision_hidden_size == 0 or
        cfg.vision_num_heads == 0 or cfg.vision_hidden_size % cfg.vision_num_heads != 0)
        return error.InvalidClipVisionShape;
    if (cfg.image_size > std.math.maxInt(i32) or cfg.vision_hidden_size > std.math.maxInt(i32) or cfg.projection_dim > std.math.maxInt(i32))
        return error.InvalidClipVisionShape;
    const patch_dim = try std.math.mul(usize, 3, try std.math.mul(usize, cfg.patch_size, cfg.patch_size));
    if (patch_dim > std.math.maxInt(i32)) return error.InvalidClipVisionShape;
    const grid: usize = cfg.image_size / cfg.patch_size;
    const patches = try std.math.mul(usize, grid, grid);
    const rows = try std.math.mul(usize, batch, try std.math.add(usize, patches, 1));
    // Shape dimensions and takeRows indices have finite wire widths.
    if (rows > std.math.maxInt(i32)) return error.InvalidClipVisionShape;
    _ = try std.math.mul(usize, rows, cfg.vision_hidden_size);
}

fn visionEncoderFromPatches(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clip_mod.Config,
    patches_ct: CT,
    batch: usize,
) !CT {
    const H: usize = cfg.vision_hidden_size;
    const P: usize = cfg.patch_size;
    const grid: usize = cfg.image_size / cfg.patch_size;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;
    const full_seq = num_patches + 1;
    const patch_w = try cb.getWeight("vision_model.embeddings.patch_embedding.weight");
    defer cb.free(patch_w);
    const embedded = if (cb.kind() == .native or cb.kind() == .cuda)
        try cb.linearNoBias(patches_ct, patch_w, batch * num_patches, patch_dim, H)
    else if (cb.kind() == .metal and cb.vtable.reshapeOp != null) blk: {
        const reshaped = reshapeVisionWeight(cb, allocator, patch_w, H, patch_dim) catch |err| switch (err) {
            error.UnsupportedTensorType => break :blk try projectHostPatchWeight(cb, allocator, patches_ct, patch_w, batch * num_patches, patch_dim, H),
            else => return err,
        };
        defer cb.free(reshaped);
        break :blk try cb.linearNoBias(patches_ct, reshaped, batch * num_patches, patch_dim, H);
    } else try projectHostPatchWeight(cb, allocator, patches_ct, patch_w, batch * num_patches, patch_dim, H);
    defer cb.free(embedded);

    var hidden = try assembleVisionTokens(cb, allocator, embedded, batch, num_patches, H);
    defer cb.free(hidden);

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

    // Gather only the CLS rows before projection; keep the full sequence on device.
    const pooled = try poolVisionCls(cb, allocator, hidden, batch, full_seq, H);
    errdefer cb.free(pooled);
    const proj_w = cb.getWeight("visual_projection.weight") catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => return pooled,
        else => return err,
    };
    defer cb.free(proj_w);
    const projected = try cb.linearNoBias(pooled, proj_w, batch, H, cfg.projection_dim);
    cb.free(pooled);
    return projected;
}

fn reshapeVisionWeight(cb: *const ComputeBackend, allocator: std.mem.Allocator, weight: CT, rows: usize, cols: usize) !CT {
    // Metal weight handles can be pinned host buffers. Its row reshape retains
    // device views or copies static host weights without uploading and reading
    // them back just to prepare the projection. Activation reshapes use the
    // resident primitive directly.
    if (cb.kind() == .metal) return cb.reshape2D(allocator, weight, rows, cols, rows, cols);
    return cb.primReshape(weight, &.{ @intCast(rows), @intCast(cols) });
}

fn projectHostPatchWeight(cb: *const ComputeBackend, allocator: std.mem.Allocator, patches: CT, weight: CT, rows: usize, patch_dim: usize, H: usize) !CT {
    const data = try cb.toFloat32(weight, allocator);
    defer allocator.free(data);
    const reshaped = try cb.fromFloat32Shape(data, &.{ @intCast(H), @intCast(patch_dim) });
    defer cb.free(reshaped);
    return cb.linearNoBias(patches, reshaped, rows, patch_dim, H);
}

fn assembleVisionTokens(cb: *const ComputeBackend, allocator: std.mem.Allocator, embedded: CT, batch: usize, num_patches: usize, H: usize) !CT {
    if (!supportsResidentVision(cb)) return assembleVisionTokensHost(cb, allocator, embedded, batch, num_patches, H);
    const full_seq = num_patches + 1;
    const cls_w = try cb.getWeight("vision_model.embeddings.class_embedding");
    defer cb.free(cls_w);
    const cls = try reshapeVisionWeight(cb, allocator, cls_w, 1, H);
    defer cb.free(cls);
    const source = try cb.concatRows2D(allocator, cls, embedded, 1, batch * num_patches, H);
    defer cb.free(source);
    const row_ids = try allocator.alloc(u32, batch * full_seq);
    defer allocator.free(row_ids);
    const pos_ids = try allocator.alloc(i64, row_ids.len);
    defer allocator.free(pos_ids);
    for (0..batch) |b| {
        for (0..full_seq) |pos| {
            row_ids[b * full_seq + pos] = if (pos == 0) 0 else @intCast(1 + b * num_patches + pos - 1);
            pos_ids[b * full_seq + pos] = @intCast(pos);
        }
    }
    const tokens = (try cb.takeRows(source, row_ids, row_ids.len, H)) orelse return error.UnsupportedResidentClipVision;
    defer cb.free(tokens);
    const pos_w = try cb.getWeight("vision_model.embeddings.position_embedding.weight");
    defer cb.free(pos_w);
    const positions = try cb.embeddingLookup(pos_w, pos_ids, pos_ids.len, H);
    defer cb.free(positions);
    return cb.add(tokens, positions);
}

fn assembleVisionTokensHost(cb: *const ComputeBackend, allocator: std.mem.Allocator, embedded: CT, batch: usize, num_patches: usize, H: usize) !CT {
    const full_seq = num_patches + 1;
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
    return cb.fromFloat32Shape(full_data, &hidden_shape);
}

fn poolVisionCls(cb: *const ComputeBackend, allocator: std.mem.Allocator, hidden: CT, batch: usize, full_seq: usize, H: usize) !CT {
    if (cb.vtable.takeRows != null) {
        const row_ids = try allocator.alloc(u32, batch);
        defer allocator.free(row_ids);
        for (row_ids, 0..) |*row, b| row.* = @intCast(b * full_seq);
        if (try cb.takeRows(hidden, row_ids, batch, H)) |pooled| return pooled;
        if (supportsResidentVision(cb)) return error.UnsupportedResidentClipVision;
    }
    const data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(data);
    const pooled = try allocator.alloc(f32, batch * H);
    defer allocator.free(pooled);
    for (0..batch) |b| @memcpy(pooled[b * H ..][0..H], data[b * full_seq * H ..][0..H]);
    return cb.fromFloat32Shape(pooled, &.{ @intCast(batch), @intCast(H) });
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
    defer cb.free(ln1_g);
    const ln1_b = try cb.getWeight(try fmt(&buf, "{s}.{d}.layer_norm1.bias", .{ prefix, layer }));
    defer cb.free(ln1_b);
    const normed1 = try cb.layerNorm(input, ln1_g, ln1_b, hidden, 1e-5);
    defer cb.free(normed1);

    const res1 = try selfAttn(cb, allocator, normed1, input, prefix, layer, batch, seq_len, num_heads, head_dim, hidden, causal, &buf);
    defer cb.free(res1);

    // Pre-norm → FFN (quickGELU) → residual
    const ln2_g = try cb.getWeight(try fmt(&buf, "{s}.{d}.layer_norm2.weight", .{ prefix, layer }));
    defer cb.free(ln2_g);
    const ln2_b = try cb.getWeight(try fmt(&buf, "{s}.{d}.layer_norm2.bias", .{ prefix, layer }));
    defer cb.free(ln2_b);
    const normed2 = try cb.layerNorm(res1, ln2_g, ln2_b, hidden, 1e-5);
    defer cb.free(normed2);

    var fc1_w_buf: [128]u8 = undefined;
    var fc1_b_buf: [128]u8 = undefined;
    const fc1_w = try cb.getWeight(try fmt(&fc1_w_buf, "{s}.{d}.mlp.fc1.weight", .{ prefix, layer }));
    defer cb.free(fc1_w);
    const fc1_b = try cb.getWeight(try fmt(&fc1_b_buf, "{s}.{d}.mlp.fc1.bias", .{ prefix, layer }));
    defer cb.free(fc1_b);
    const activated = (try cb.linearQuickGelu(normed2, fc1_w, fc1_b, total, hidden, intermediate)) orelse blk: {
        const fc1_out = try cb.linear(normed2, fc1_w, fc1_b, total, hidden, intermediate);
        defer cb.free(fc1_out);
        break :blk try cb.quickGelu(fc1_out);
    };
    defer cb.free(activated);

    var fc2_w_buf: [128]u8 = undefined;
    var fc2_b_buf: [128]u8 = undefined;
    const fc2_w = try cb.getWeight(try fmt(&fc2_w_buf, "{s}.{d}.mlp.fc2.weight", .{ prefix, layer }));
    defer cb.free(fc2_w);
    const fc2_b = try cb.getWeight(try fmt(&fc2_b_buf, "{s}.{d}.mlp.fc2.bias", .{ prefix, layer }));
    defer cb.free(fc2_b);
    const res2 = (try cb.linearAdd(activated, fc2_w, fc2_b, res1, total, intermediate, hidden)) orelse blk: {
        const fc2_out = try cb.linear(activated, fc2_w, fc2_b, total, intermediate, hidden);
        defer cb.free(fc2_out);
        break :blk try cb.add(res1, fc2_out);
    };
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
    defer cb.free(q_w);
    const q_b = try cb.getWeight(try fmt(&q_b_buf, "{s}.{d}.self_attn.q_proj.bias", .{ prefix, layer }));
    defer cb.free(q_b);
    const k_w = try cb.getWeight(try fmt(&k_w_buf, "{s}.{d}.self_attn.k_proj.weight", .{ prefix, layer }));
    defer cb.free(k_w);
    const k_b = try cb.getWeight(try fmt(&k_b_buf, "{s}.{d}.self_attn.k_proj.bias", .{ prefix, layer }));
    defer cb.free(k_b);
    const v_w = try cb.getWeight(try fmt(&v_w_buf, "{s}.{d}.self_attn.v_proj.weight", .{ prefix, layer }));
    defer cb.free(v_w);
    const v_b = try cb.getWeight(try fmt(&v_b_buf, "{s}.{d}.self_attn.v_proj.bias", .{ prefix, layer }));
    defer cb.free(v_b);

    const qkv = try cb.linearTriple(input, q_w, q_b, k_w, k_b, v_w, v_b, total, hidden, hidden);
    const Q_token = qkv.first;
    const K_token = qkv.second;
    const V_token = qkv.third;
    defer cb.free(Q_token);
    defer cb.free(K_token);
    defer cb.free(V_token);
    const Q = if (causal) Q_token else try packTokenMajorToHeadMajor(cb, allocator, Q_token, batch, seq_len, num_heads, head_dim);
    defer if (!causal) cb.free(Q);
    const K = if (causal) K_token else try packTokenMajorToHeadMajor(cb, allocator, K_token, batch, seq_len, num_heads, head_dim);
    defer if (!causal) cb.free(K);
    const V = if (causal) V_token else try packTokenMajorToHeadMajor(cb, allocator, V_token, batch, seq_len, num_heads, head_dim);
    defer if (!causal) cb.free(V);

    const attn_out = if (causal)
        try cb.causalSelfAttention(Q, K, V, null, batch, seq_len, num_heads, head_dim)
    else blk: {
        const ones = try allocator.alloc(i64, batch * seq_len);
        defer allocator.free(ones);
        @memset(ones, 1);
        break :blk try cb.scaledDotProductAttention(Q, K, V, ones, null, batch, seq_len, num_heads, head_dim);
    };
    defer cb.free(attn_out);
    const projected_input = if (causal) attn_out else try unpackHeadMajorToTokenMajor(cb, allocator, attn_out, batch, seq_len, num_heads, head_dim);
    defer if (!causal) cb.free(projected_input);

    var o_w_buf: [128]u8 = undefined;
    var o_b_buf: [128]u8 = undefined;
    const o_w = try cb.getWeight(try fmt(&o_w_buf, "{s}.{d}.self_attn.out_proj.weight", .{ prefix, layer }));
    defer cb.free(o_w);
    const o_b = try cb.getWeight(try fmt(&o_b_buf, "{s}.{d}.self_attn.out_proj.bias", .{ prefix, layer }));
    defer cb.free(o_b);
    const projected = (try cb.linearAdd(projected_input, o_w, o_b, residual, total, hidden, hidden)) orelse blk: {
        const projected = try cb.linear(projected_input, o_w, o_b, total, hidden, hidden);
        defer cb.free(projected);
        break :blk try cb.add(residual, projected);
    };
    return projected;
}

fn packTokenMajorToHeadMajor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tensor: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) !CT {
    if (cb.vtable.transposeOp != null and cb.vtable.reshapeOp != null) {
        const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) };
        const shaped = try cb.primReshape(tensor, &shape);
        defer cb.free(shaped);
        const transposed = try cb.primTranspose(shaped, &.{ 0, 2, 1, 3 }, &shape);
        defer cb.free(transposed);
        return cb.primReshape(transposed, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) });
    }
    const token_major = try cb.toFloat32(tensor, allocator);
    defer allocator.free(token_major);
    const head_major = try packTokenMajorDataToHeadMajor(allocator, token_major, batch, seq_len, num_heads, head_dim);
    defer allocator.free(head_major);
    const shape = [_]i32{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) };
    return cb.fromFloat32Shape(head_major, &shape);
}

fn unpackHeadMajorToTokenMajor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tensor: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) !CT {
    if (cb.vtable.transposeOp != null and cb.vtable.reshapeOp != null) {
        const shape = [_]i64{ @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(head_dim) };
        const shaped = try cb.primReshape(tensor, &shape);
        defer cb.free(shaped);
        const transposed = try cb.primTranspose(shaped, &.{ 0, 2, 1, 3 }, &shape);
        defer cb.free(transposed);
        return cb.primReshape(transposed, &.{ @intCast(batch * seq_len), @intCast(num_heads * head_dim) });
    }
    const head_major = try cb.toFloat32(tensor, allocator);
    defer allocator.free(head_major);
    const token_major = try unpackHeadMajorDataToTokenMajor(allocator, head_major, batch, seq_len, num_heads, head_dim);
    defer allocator.free(token_major);
    const shape = [_]i32{ @intCast(batch * seq_len), @intCast(num_heads * head_dim) };
    return cb.fromFloat32Shape(token_major, &shape);
}

fn packTokenMajorDataToHeadMajor(
    allocator: std.mem.Allocator,
    token_major: []const f32,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    const hidden = num_heads * head_dim;
    const expected = batch * seq_len * hidden;
    if (token_major.len != expected) return error.InvalidAttentionShape;

    const head_major = try allocator.alloc(f32, expected);
    for (0..batch) |b| {
        for (0..seq_len) |s| {
            for (0..num_heads) |h| {
                const src = (b * seq_len + s) * hidden + h * head_dim;
                const dst = (b * num_heads + h) * seq_len * head_dim + s * head_dim;
                @memcpy(head_major[dst..][0..head_dim], token_major[src..][0..head_dim]);
            }
        }
    }
    return head_major;
}

fn unpackHeadMajorDataToTokenMajor(
    allocator: std.mem.Allocator,
    head_major: []const f32,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    const hidden = num_heads * head_dim;
    const expected = batch * seq_len * hidden;
    if (head_major.len != expected) return error.InvalidAttentionShape;

    const token_major = try allocator.alloc(f32, expected);
    for (0..batch) |b| {
        for (0..num_heads) |h| {
            for (0..seq_len) |s| {
                const src = (b * num_heads + h) * seq_len * head_dim + s * head_dim;
                const dst = (b * seq_len + s) * hidden + h * head_dim;
                @memcpy(token_major[dst..][0..head_dim], head_major[src..][0..head_dim]);
            }
        }
    }
    return token_major;
}

fn fmt(buf: *[128]u8, comptime format: []const u8, args: anytype) ![]const u8 {
    return std.fmt.bufPrint(buf, format, args) catch return error.WeightNameTooLong;
}

test "clip attention layout pack round-trips token major hidden rows" {
    const allocator = std.testing.allocator;
    const batch = 2;
    const seq_len = 3;
    const num_heads = 2;
    const head_dim = 2;
    const hidden = num_heads * head_dim;

    var token_major: [batch * seq_len * hidden]f32 = undefined;
    for (&token_major, 0..) |*value, i| value.* = @floatFromInt(i);

    const head_major = try packTokenMajorDataToHeadMajor(allocator, &token_major, batch, seq_len, num_heads, head_dim);
    defer allocator.free(head_major);

    try std.testing.expectEqual(@as(f32, 0), head_major[0]);
    try std.testing.expectEqual(@as(f32, 1), head_major[1]);
    try std.testing.expectEqual(@as(f32, 4), head_major[2]);
    try std.testing.expectEqual(@as(f32, 5), head_major[3]);
    try std.testing.expectEqual(@as(f32, 2), head_major[seq_len * head_dim]);
    try std.testing.expectEqual(@as(f32, 3), head_major[seq_len * head_dim + 1]);

    const round_trip = try unpackHeadMajorDataToTokenMajor(allocator, head_major, batch, seq_len, num_heads, head_dim);
    defer allocator.free(round_trip);
    try std.testing.expectEqualSlices(f32, &token_major, round_trip);
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
    defer compute.deinit();
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

fn rejectClipReadback(_: *anyopaque, _: CT, _: std.mem.Allocator) anyerror![]f32 {
    return error.UnexpectedClipReadback;
}

fn putClipTestWeight(store: *native_compute.WeightStore, name: []const u8, shape: []const i64, constant: ?f32) !void {
    const allocator = store.allocator;
    var count: usize = 1;
    for (shape) |dim| count *= @intCast(dim);
    const data = try allocator.alloc(f32, count);
    defer allocator.free(data);
    for (data, 0..) |*value, i| value.* = constant orelse (@as(f32, @floatFromInt((i * 7 + name.len) % 23)) - 11) * 0.03;
    const key = try allocator.dupe(u8, name);
    errdefer allocator.free(key);
    const tensor = try @import("../backends/tensor.zig").Tensor.initFloat32(allocator, key, shape, data);
    try store.resident_weights.put(allocator, key, .{ .tensor = tensor });
}

fn populateClipTestWeights(store: *native_compute.WeightStore, projected: bool) !void {
    try putClipTestWeight(store, "vision_model.embeddings.class_embedding", &.{4}, null);
    try putClipTestWeight(store, "vision_model.embeddings.patch_embedding.weight", &.{ 4, 3, 2, 2 }, null);
    try putClipTestWeight(store, "vision_model.embeddings.position_embedding.weight", &.{ 5, 4 }, null);
    for ([_][]const u8{ "pre_layrnorm", "post_layernorm", "encoder.layers.0.layer_norm1", "encoder.layers.0.layer_norm2" }) |prefix| {
        var buf: [128]u8 = undefined;
        try putClipTestWeight(store, try fmt(&buf, "vision_model.{s}.weight", .{prefix}), &.{4}, 1);
        try putClipTestWeight(store, try fmt(&buf, "vision_model.{s}.bias", .{prefix}), &.{4}, 0);
    }
    for ([_][]const u8{ "q_proj", "k_proj", "v_proj", "out_proj" }) |projection| {
        var buf: [128]u8 = undefined;
        try putClipTestWeight(store, try fmt(&buf, "vision_model.encoder.layers.0.self_attn.{s}.weight", .{projection}), &.{ 4, 4 }, null);
        try putClipTestWeight(store, try fmt(&buf, "vision_model.encoder.layers.0.self_attn.{s}.bias", .{projection}), &.{4}, null);
    }
    try putClipTestWeight(store, "vision_model.encoder.layers.0.mlp.fc1.weight", &.{ 8, 4 }, null);
    try putClipTestWeight(store, "vision_model.encoder.layers.0.mlp.fc1.bias", &.{8}, null);
    try putClipTestWeight(store, "vision_model.encoder.layers.0.mlp.fc2.weight", &.{ 4, 8 }, null);
    try putClipTestWeight(store, "vision_model.encoder.layers.0.mlp.fc2.bias", &.{4}, null);
    if (projected) try putClipTestWeight(store, "visual_projection.weight", &.{ 3, 4 }, null);
}

const clip_test_config = clip_mod.Config{
    .image_size = 4,
    .patch_size = 2,
    .vision_hidden_size = 4,
    .vision_num_heads = 2,
    .vision_num_layers = 1,
    .vision_intermediate_size = 8,
    .projection_dim = 3,
};

test "clip resident vision matches host reference without intermediate readbacks" {
    const allocator = std.testing.allocator;
    for ([_]bool{ false, true }) |projected| {
        var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
        defer store.deinitOwned();
        try populateClipTestWeights(&store, projected);
        var compute = native_compute.NativeCompute.init(allocator, &store, null);
        defer compute.deinit();
        const cb = compute.computeBackend();
        try checkClipResidentParity(allocator, cb, cb, projected);
    }
}

test "clip resident vision validates input and backend before execution" {
    const allocator = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer store.deinitOwned();
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const input = try cb.fromFloat32Shape(&.{0}, &.{1});
    defer cb.free(input);
    try std.testing.expectError(error.InvalidClipVisionShape, visionEncoderForwardTensor(&cb, allocator, clip_test_config, input, 0));
    var invalid = clip_test_config;
    invalid.patch_size = 0;
    try std.testing.expectError(error.InvalidClipVisionShape, visionEncoderForwardTensor(&cb, allocator, invalid, input, 1));
    try std.testing.expectError(error.UnexpectedOutputShape, visionEncoderForward(&cb, allocator, clip_test_config, &.{0}, 1));
    try std.testing.expectError(error.UnexpectedOutputShape, visionEncoderForwardTensor(&cb, allocator, clip_test_config, input, 1));
    invalid = clip_test_config;
    invalid.image_size = 5;
    try std.testing.expectError(error.InvalidClipVisionShape, visionEncoderForwardTensor(&cb, allocator, invalid, input, 1));
    var limited_vtable = cb.vtable.*;
    limited_vtable.takeRows = null;
    const limited = ComputeBackend{ .ptr = cb.ptr, .vtable = &limited_vtable };
    try std.testing.expectError(error.UnsupportedResidentClipVision, visionEncoderForwardTensor(&limited, allocator, clip_test_config, input, 1));
}

fn checkClipResidentParity(allocator: std.mem.Allocator, cb: ComputeBackend, reference: ComputeBackend, projected: bool) !void {
    var resident_vtable = cb.vtable.*;
    resident_vtable.toFloat32 = rejectClipReadback;
    const resident = ComputeBackend{ .ptr = cb.ptr, .vtable = &resident_vtable };
    var host_vtable = reference.vtable.*;
    host_vtable.takeRows = null;
    host_vtable.transposeOp = null;
    const host = ComputeBackend{ .ptr = reference.ptr, .vtable = &host_vtable };
    for ([_]usize{ 1, 2, 4 }) |batch| {
        const pixels = try allocator.alloc(f32, batch * 3 * 4 * 4);
        defer allocator.free(pixels);
        for (pixels, 0..) |*value, i| value.* = @as(f32, @floatFromInt((i * 13) % 53)) * 0.02;
        const input = try uploadVisionPixels(&cb, allocator, pixels, batch, 4);
        defer cb.free(input);
        const before_timing = cb.debugTimingSnapshot();
        const before_runtime = cb.trainingRuntimeStats();
        const result = try visionEncoderForwardTensor(&resident, allocator, clip_test_config, input, batch);
        defer cb.free(result);
        if (cb.kind() == .metal) {
            const after = cb.debugTimingSnapshot();
            try std.testing.expectEqual(before_timing.provider.metal_tensor_host_mirror_download_bytes, after.provider.metal_tensor_host_mirror_download_bytes);
            try std.testing.expectEqual(before_timing.provider.metal_tensor_to_host_device_calls, after.provider.metal_tensor_to_host_device_calls);
        }
        if (cb.kind() == .cuda) try std.testing.expectEqual(before_runtime.d2h_bytes, cb.trainingRuntimeStats().d2h_bytes);
        const actual = try cb.toFloat32(result, allocator);
        defer allocator.free(actual);
        const expected = try visionEncoderForward(&host, allocator, clip_test_config, pixels, batch);
        defer allocator.free(expected);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |e, a| try std.testing.expectApproxEqAbs(e, a, 1e-5);
        // Different images in a batch must match their independent forwards.
        const dim: usize = if (projected) 3 else 4;
        for (0..batch) |b| {
            const single = try uploadVisionPixels(&cb, allocator, pixels[b * 48 ..][0..48], 1, 4);
            defer cb.free(single);
            const single_result = try visionEncoderForwardTensor(&resident, allocator, clip_test_config, single, 1);
            defer cb.free(single_result);
            const single_data = try cb.toFloat32(single_result, allocator);
            defer allocator.free(single_data);
            for (single_data, actual[b * dim ..][0..dim]) |e, a| try std.testing.expectApproxEqAbs(e, a, 1e-5);
        }
    }
}

test "clip resident vision Metal parity on hardware" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const metal_runtime = @import("../backends/metal_runtime.zig");
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const gpu_store = @import("../ops/gpu_hosted_store.zig");
    const allocator = std.testing.allocator;
    var weights = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    try populateClipTestWeights(&weights, true);
    var native = native_compute.NativeCompute.init(allocator, &weights, null);
    defer native.deinit();
    var store = gpu_store.WeightStore{ .allocator = allocator, .prefix = "", .lazy_weights = .{} };
    defer store.lazy_weights.deinit(allocator);
    metal.initPrefetchQueue(&store, allocator);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var it = weights.resident_weights.iterator();
    while (it.next()) |entry| {
        var borrowed = entry.value_ptr.tensor;
        borrowed.owns_data = false;
        borrowed.owns_shape = false;
        try store.lazy_weights.put(allocator, entry.key_ptr.*, .{
            .tensor_ref = .{ .name = entry.key_ptr.* },
            .host_loaded = .{ .tensor = borrowed },
        });
    }
    var compute = try metal.MetalCompute.init(allocator, &store, null);
    defer compute.deinit();
    try checkClipResidentParity(allocator, compute.computeBackend(), native.computeBackend(), true);
}

test "clip resident vision CUDA parity on hardware" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const cuda = @import("../ops/cuda/cuda_compute.zig");
    const allocator = std.testing.allocator;
    var weights = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    try populateClipTestWeights(&weights, true);
    var native = native_compute.NativeCompute.init(allocator, &weights, null);
    defer native.deinit();
    var compute = try cuda.CudaCompute.init(allocator);
    defer compute.deinit();
    var it = weights.resident_weights.iterator();
    while (it.next()) |entry| {
        const key = try allocator.dupe(u8, entry.key_ptr.*);
        try compute.insertWeightFromTensor(key, &entry.value_ptr.tensor);
    }
    try checkClipResidentParity(allocator, compute.computeBackend(), native.computeBackend(), true);
}

fn failClipProjectionWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
    if (std.mem.eql(u8, name, "visual_projection.weight")) return error.InjectedClipWeightFailure;
    const compute: *native_compute.NativeCompute = @ptrCast(@alignCast(ctx));
    return compute.computeBackend().getWeight(name);
}

fn unavailableClipGather(_: *anyopaque, _: *const ops.TakeRowsRequest) anyerror!?CT {
    return null;
}

test "clip resident vision propagates execution failures and frees intermediates" {
    const allocator = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer store.deinitOwned();
    try populateClipTestWeights(&store, true);
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const pixels: [48]f32 = @splat(0.5);
    const input = try cb.fromFloat32Shape(&pixels, &.{ 1, 3, 4, 4 });
    defer cb.free(input);
    var unavailable_vtable = cb.vtable.*;
    unavailable_vtable.takeRows = unavailableClipGather;
    unavailable_vtable.toFloat32 = rejectClipReadback;
    const unavailable = ComputeBackend{ .ptr = cb.ptr, .vtable = &unavailable_vtable };
    try std.testing.expectError(error.UnsupportedResidentClipVision, visionEncoderForwardTensor(&unavailable, allocator, clip_test_config, input, 1));
    var failing_vtable = cb.vtable.*;
    failing_vtable.getWeight = failClipProjectionWeight;
    const failing = ComputeBackend{ .ptr = cb.ptr, .vtable = &failing_vtable };
    try std.testing.expectError(error.InjectedClipWeightFailure, visionEncoderForwardTensor(&failing, allocator, clip_test_config, input, 1));
    // The input is borrowed and still reusable after both errors.
    const recovered = try visionEncoderForwardTensor(&cb, allocator, clip_test_config, input, 1);
    defer cb.free(recovered);
}
