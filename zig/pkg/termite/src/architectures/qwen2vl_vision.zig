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
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;
const gpt_mod = @import("../models/gpt.zig");
const qwen2vl = @import("qwen2vl_types.zig");
const posix = std.posix;

const vision_rope_theta: f32 = 10000.0;

pub fn encodePreparedImageTokens(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: gpt_mod.Config,
    prep_cfg: qwen2vl.PreprocessorConfig,
    prepared: qwen2vl.PreparedImage,
) ![]f32 {
    const projected = try encodePreparedImageTokensTensor(cb, allocator, cfg, prep_cfg, prepared);
    defer cb.free(projected);
    return cb.toFloat32(projected, allocator);
}

pub fn encodePreparedImageTokensTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: gpt_mod.Config,
    prep_cfg: qwen2vl.PreprocessorConfig,
    prepared: qwen2vl.PreparedImage,
) !CT {
    const trace_enabled = visionTraceEnabled();
    var stage_start_ns: u64 = 0;
    if (trace_enabled) {
        stage_start_ns = try nowNs();
        std.debug.print(
            "vision_trace stage=begin resized={}x{} grid=[{}, {}, {}]\n",
            .{ prepared.resized_width, prepared.resized_height, prepared.image_grid_thw[0], prepared.image_grid_thw[1], prepared.image_grid_thw[2] },
        );
    }
    const embed_dim: usize = if (cfg.vision_embed_dim > 0) cfg.vision_embed_dim else return error.InvalidMultimodalConfig;
    const num_layers: usize = if (cfg.vision_num_hidden_layers > 0) cfg.vision_num_hidden_layers else return error.InvalidMultimodalConfig;
    const num_heads: usize = if (cfg.vision_num_attention_heads > 0) cfg.vision_num_attention_heads else return error.InvalidMultimodalConfig;
    const patch_size: usize = if (cfg.vision_patch_size > 0) cfg.vision_patch_size else prep_cfg.patch_size;
    const temporal_patch_size: usize = if (cfg.vision_temporal_patch_size > 0) cfg.vision_temporal_patch_size else prep_cfg.temporal_patch_size;
    const merge_size: usize = if (cfg.vision_spatial_merge_size > 0) cfg.vision_spatial_merge_size else prep_cfg.merge_size;
    if (merge_size == 0 or num_heads == 0 or patch_size == 0 or temporal_patch_size == 0) return error.InvalidMultimodalConfig;

    const patch_grid_t: usize = @intCast(prepared.image_grid_thw[0]);
    const patch_grid_h: usize = @intCast(prepared.image_grid_thw[1]);
    const patch_grid_w: usize = @intCast(prepared.image_grid_thw[2]);
    const seq_len = patch_grid_t * patch_grid_h * patch_grid_w;
    const merge_tokens = merge_size * merge_size;
    if (merge_tokens == 0 or (seq_len % merge_tokens) != 0) return error.InvalidPatchMergeFactor;
    const merged_tokens = seq_len / merge_tokens;
    if (merged_tokens != prepared.image_token_count) return error.ImageTokenLengthMismatch;

    const patch_rows = try buildPatchRows(allocator, prepared, patch_size, temporal_patch_size, merge_size);
    defer allocator.free(patch_rows);
    if (trace_enabled) {
        std.debug.print(
            "vision_trace stage=build_patch_rows elapsed_ms={d} seq_len={d} patch_dim={d}\n",
            .{ nsToMs(try elapsedSince(stage_start_ns)), seq_len, 3 * temporal_patch_size * patch_size * patch_size },
        );
        stage_start_ns = try nowNs();
    }

    var hidden = try patchEmbed(cb, allocator, patch_rows, seq_len, patch_size, temporal_patch_size, embed_dim);
    errdefer cb.free(hidden);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_embed elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
        stage_start_ns = try nowNs();
    }

    const head_dim = embed_dim / num_heads;
    if (head_dim == 0 or (head_dim % 2) != 0) return error.InvalidMultimodalConfig;
    const rotary = try buildVisionRotaryPosEmb(allocator, prepared.image_grid_thw, merge_size, head_dim);
    defer allocator.free(rotary);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=build_rotary elapsed_ms={d} head_dim={d}\n", .{ nsToMs(try elapsedSince(stage_start_ns)), head_dim });
    }

    for (0..num_layers) |layer| {
        if (trace_enabled) stage_start_ns = try nowNs();
        const next_hidden = try encoderBlock(cb, allocator, hidden, seq_len, embed_dim, num_heads, rotary, layer, cfg.vision_use_quick_gelu, cfg.vision_mlp_ratio, cfg.vision_intermediate_size);
        cb.free(hidden);
        hidden = next_hidden;
        if (trace_enabled) {
            std.debug.print("vision_trace stage=encoder_block layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        }
    }

    if (trace_enabled) stage_start_ns = try nowNs();
    const projected = try patchMergerTensor(cb, allocator, hidden, seq_len, embed_dim, merge_size, cfg);
    cb.free(hidden);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_merger elapsed_ms={d} output_tokens={d}\n", .{ nsToMs(try elapsedSince(stage_start_ns)), merged_tokens });
    }
    return projected;
}

fn buildPatchRows(
    allocator: std.mem.Allocator,
    prepared: qwen2vl.PreparedImage,
    patch_size: usize,
    temporal_patch_size: usize,
    merge_size: usize,
) ![]f32 {
    const grid_t: usize = @intCast(prepared.image_grid_thw[0]);
    const grid_h: usize = @intCast(prepared.image_grid_thw[1]);
    const grid_w: usize = @intCast(prepared.image_grid_thw[2]);
    const height: usize = @intCast(prepared.resized_height);
    const width: usize = @intCast(prepared.resized_width);
    const rows = grid_t * grid_h * grid_w;
    const patch_dim = 3 * temporal_patch_size * patch_size * patch_size;
    const out = try allocator.alloc(f32, rows * patch_dim);

    var row_index: usize = 0;
    const merged_h = grid_h / merge_size;
    const merged_w = grid_w / merge_size;
    for (0..grid_t) |_| {
        for (0..merged_h) |block_y| {
            for (0..merged_w) |block_x| {
                for (0..merge_size) |iy| {
                    for (0..merge_size) |ix| {
                        const patch_y = block_y * merge_size + iy;
                        const patch_x = block_x * merge_size + ix;
                        const dst = row_index * patch_dim;
                        var cursor: usize = 0;
                        for (0..3) |ch| {
                            for (0..temporal_patch_size) |_| {
                                for (0..patch_size) |py| {
                                    for (0..patch_size) |px| {
                                        const src = ch * height * width + (patch_y * patch_size + py) * width + (patch_x * patch_size + px);
                                        out[dst + cursor] = prepared.pixel_values[src];
                                        cursor += 1;
                                    }
                                }
                            }
                        }
                        row_index += 1;
                    }
                }
            }
        }
    }
    return out;
}

fn patchEmbed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    patch_rows: []const f32,
    seq_len: usize,
    patch_size: usize,
    temporal_patch_size: usize,
    embed_dim: usize,
) !CT {
    const trace_enabled = visionTraceEnabled();
    var stage_start_ns: u64 = 0;
    const patch_dim = 3 * temporal_patch_size * patch_size * patch_size;
    if (trace_enabled) stage_start_ns = try nowNs();
    const patch_shape = [_]i32{ @intCast(seq_len), @intCast(patch_dim) };
    const patch_ct = try cb.fromFloat32Shape(patch_rows, &patch_shape);
    defer cb.free(patch_ct);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_embed.from_input elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
        stage_start_ns = try nowNs();
    }

    const patch_w = try getVisionWeight(cb, "visual.patch_embed.proj.weight");
    defer cb.free(patch_w);
    const patch_w_data = try cb.toFloat32(patch_w, allocator);
    defer allocator.free(patch_w_data);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_embed.load_weight elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
        stage_start_ns = try nowNs();
    }

    const weight_shape = [_]i32{ @intCast(embed_dim), @intCast(patch_dim) };
    const weight_ct = try cb.fromFloat32Shape(patch_w_data, &weight_shape);
    defer cb.free(weight_ct);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_embed.from_weight elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
        stage_start_ns = try nowNs();
    }

    const out = try cb.linearNoBias(patch_ct, weight_ct, seq_len, patch_dim, embed_dim);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_embed.linear elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
    }
    return out;
}

fn buildVisionRotaryPosEmb(
    allocator: std.mem.Allocator,
    grid_thw: [3]u32,
    merge_size: usize,
    head_dim: usize,
) ![]f32 {
    const grid_h: usize = @intCast(grid_thw[1]);
    const grid_w: usize = @intCast(grid_thw[2]);
    const seq_len: usize = @as(usize, @intCast(grid_thw[0])) * grid_h * grid_w;
    const merged_h = grid_h / merge_size;
    const merged_w = grid_w / merge_size;
    const rot_dim = head_dim / 2;
    if (rot_dim == 0 or (rot_dim % 2) != 0) return error.InvalidMultimodalConfig;
    const inv_len = rot_dim / 2;

    var max_grid_size = grid_h;
    if (grid_w > max_grid_size) max_grid_size = grid_w;
    const inv_freq = try allocator.alloc(f32, inv_len);
    defer allocator.free(inv_freq);
    for (0..inv_len) |i| {
        const exponent = @as(f32, @floatFromInt(2 * i)) / @as(f32, @floatFromInt(rot_dim));
        inv_freq[i] = 1.0 / std.math.pow(f32, 10000.0, exponent);
    }

    const full = try allocator.alloc(f32, max_grid_size * inv_len);
    defer allocator.free(full);
    for (0..max_grid_size) |pos| {
        for (0..inv_len) |i| {
            full[pos * inv_len + i] = @as(f32, @floatFromInt(pos)) * inv_freq[i];
        }
    }

    const freqs = try allocator.alloc(f32, seq_len * rot_dim);
    var token: usize = 0;
    for (0..merged_h) |block_y| {
        for (0..merged_w) |block_x| {
            for (0..merge_size) |iy| {
                for (0..merge_size) |ix| {
                    const hpos = block_y * merge_size + iy;
                    const wpos = block_x * merge_size + ix;
                    const dst = token * rot_dim;
                    @memcpy(freqs[dst..][0..inv_len], full[hpos * inv_len ..][0..inv_len]);
                    @memcpy(freqs[dst + inv_len ..][0..inv_len], full[wpos * inv_len ..][0..inv_len]);
                    token += 1;
                }
            }
        }
    }
    return freqs;
}

fn encoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    seq_len: usize,
    embed_dim: usize,
    num_heads: usize,
    rotary: []const f32,
    layer: usize,
    use_quick_gelu: bool,
    mlp_ratio: u32,
    vision_intermediate_size: usize,
) !CT {
    const trace_enabled = visionTraceEnabled();
    var buf: [160]u8 = undefined;
    var stage_start_ns: u64 = 0;
    const hidden_dim: usize = if (mlp_ratio > 0)
        embed_dim * @as(usize, mlp_ratio)
    else if (vision_intermediate_size > 0)
        vision_intermediate_size
    else
        return error.InvalidMultimodalConfig;

    if (trace_enabled) stage_start_ns = try nowNs();
    const ln1_w = try getVisionWeightFmt(cb, &buf, "visual.blocks.{d}.norm1.weight", .{layer});
    defer cb.free(ln1_w);
    const ln1_b = try getVisionWeightFmt(cb, &buf, "visual.blocks.{d}.norm1.bias", .{layer});
    defer cb.free(ln1_b);
    const normed1 = try cb.layerNorm(input, ln1_w, ln1_b, embed_dim, 1e-6);
    defer cb.free(normed1);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=encoder_block.norm1 layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }

    const attn_out = try selfAttention(cb, allocator, normed1, seq_len, embed_dim, num_heads, rotary, layer, &buf);
    defer cb.free(attn_out);
    const res1 = try cb.add(input, attn_out);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=encoder_block.attn layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }

    const ln2_w = try getVisionWeightFmt(cb, &buf, "visual.blocks.{d}.norm2.weight", .{layer});
    defer cb.free(ln2_w);
    const ln2_b = try getVisionWeightFmt(cb, &buf, "visual.blocks.{d}.norm2.bias", .{layer});
    defer cb.free(ln2_b);
    const normed2 = try cb.layerNorm(res1, ln2_w, ln2_b, embed_dim, 1e-6);
    defer cb.free(normed2);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=encoder_block.norm2 layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }

    const fc1_w = try getVisionWeightFmt2(cb, &buf, "visual.blocks.{d}.mlp.linear_fc1.weight", "visual.blocks.{d}.mlp.fc1.weight", .{layer});
    defer cb.free(fc1_w);
    const fc1_b = try getVisionWeightFmt2(cb, &buf, "visual.blocks.{d}.mlp.linear_fc1.bias", "visual.blocks.{d}.mlp.fc1.bias", .{layer});
    defer cb.free(fc1_b);
    const fc1 = try cb.linear(normed2, fc1_w, fc1_b, seq_len, embed_dim, hidden_dim);
    defer cb.free(fc1);
    const act_ct = if (use_quick_gelu)
        try cb.quickGelu(fc1)
    else
        try cb.gelu(fc1);
    defer cb.free(act_ct);

    const fc2_w = try getVisionWeightFmt2(cb, &buf, "visual.blocks.{d}.mlp.linear_fc2.weight", "visual.blocks.{d}.mlp.fc2.weight", .{layer});
    defer cb.free(fc2_w);
    const fc2_b = try getVisionWeightFmt2(cb, &buf, "visual.blocks.{d}.mlp.linear_fc2.bias", "visual.blocks.{d}.mlp.fc2.bias", .{layer});
    defer cb.free(fc2_b);
    const fc2 = try cb.linear(act_ct, fc2_w, fc2_b, seq_len, hidden_dim, embed_dim);
    defer cb.free(fc2);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=encoder_block.mlp layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }

    const res2 = try cb.add(res1, fc2);
    cb.free(res1);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=encoder_block.residual layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
    }
    return res2;
}

fn selfAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    seq_len: usize,
    embed_dim: usize,
    num_heads: usize,
    rotary: []const f32,
    layer: usize,
    buf: *[160]u8,
) !CT {
    const trace_enabled = visionTraceEnabled();
    var stage_start_ns: u64 = 0;
    const head_dim = embed_dim / num_heads;
    if (trace_enabled) stage_start_ns = try nowNs();
    const qkv_w = try getVisionWeightFmt(cb, buf, "visual.blocks.{d}.attn.qkv.weight", .{layer});
    defer cb.free(qkv_w);
    const qkv_b = try getVisionWeightFmt(cb, buf, "visual.blocks.{d}.attn.qkv.bias", .{layer});
    defer cb.free(qkv_b);
    const qkv = try cb.linear(input, qkv_w, qkv_b, seq_len, embed_dim, embed_dim * 3);
    defer cb.free(qkv);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=self_attention.qkv_linear layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }
    const split = try cb.splitLastDim3(allocator, qkv, seq_len, embed_dim);
    defer cb.free(split.first);
    defer cb.free(split.second);
    defer cb.free(split.third);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=self_attention.qkv_split layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }

    const attn_shape = [_]i32{ @intCast(seq_len), @intCast(embed_dim) };
    const q_rot = cb.rope(split.first, seq_len, head_dim, head_dim, vision_rope_theta, 1.0, 0, false) catch blk: {
        const q_data = try cb.toFloat32(split.first, allocator);
        defer allocator.free(q_data);
        applyVisionRotary(q_data, seq_len, num_heads, head_dim, rotary);
        break :blk try cb.fromFloat32Shape(q_data, &attn_shape);
    };
    defer cb.free(q_rot);
    const k_rot = cb.rope(split.second, seq_len, head_dim, head_dim, vision_rope_theta, 1.0, 0, false) catch blk: {
        const k_data = try cb.toFloat32(split.second, allocator);
        defer allocator.free(k_data);
        applyVisionRotary(k_data, seq_len, num_heads, head_dim, rotary);
        break :blk try cb.fromFloat32Shape(k_data, &attn_shape);
    };
    defer cb.free(k_rot);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=self_attention.backend_rope layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }

    const mask = try allocator.alloc(i64, seq_len);
    defer allocator.free(mask);
    @memset(mask, 1);

    const attn_ct = cb.scaledDotProductAttention(q_rot, k_rot, split.third, mask, null, 1, seq_len, num_heads, head_dim) catch blk: {
        if (trace_enabled) {
            std.debug.print("vision_trace stage=self_attention.backend_fallback layer={d}\n", .{layer});
        }
        const qkv_data = try cb.toFloat32(qkv, allocator);
        defer allocator.free(qkv_data);
        const attn = try manualVisionAttention(allocator, qkv_data, seq_len, embed_dim, num_heads, rotary);
        defer allocator.free(attn);
        break :blk try cb.fromFloat32Shape(attn, &attn_shape);
    };
    defer cb.free(attn_ct);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=self_attention.backend_sdpa layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
        stage_start_ns = try nowNs();
    }

    const proj_w = try getVisionWeightFmt(cb, buf, "visual.blocks.{d}.attn.proj.weight", .{layer});
    defer cb.free(proj_w);
    const proj_b = try getVisionWeightFmt(cb, buf, "visual.blocks.{d}.attn.proj.bias", .{layer});
    defer cb.free(proj_b);
    const out = try cb.linear(attn_ct, proj_w, proj_b, seq_len, embed_dim, embed_dim);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=self_attention.proj layer={d} elapsed_ms={d}\n", .{ layer, nsToMs(try elapsedSince(stage_start_ns)) });
    }
    return out;
}

fn manualVisionAttention(
    allocator: std.mem.Allocator,
    qkv: []const f32,
    seq_len: usize,
    embed_dim: usize,
    num_heads: usize,
    rotary: []const f32,
) ![]f32 {
    const head_dim = embed_dim / num_heads;
    const q = try allocator.alloc(f32, seq_len * embed_dim);
    defer allocator.free(q);
    const k = try allocator.alloc(f32, seq_len * embed_dim);
    defer allocator.free(k);
    const v = try allocator.alloc(f32, seq_len * embed_dim);
    defer allocator.free(v);

    for (0..seq_len) |token| {
        const base = token * embed_dim * 3;
        @memcpy(q[token * embed_dim ..][0..embed_dim], qkv[base..][0..embed_dim]);
        @memcpy(k[token * embed_dim ..][0..embed_dim], qkv[base + embed_dim ..][0..embed_dim]);
        @memcpy(v[token * embed_dim ..][0..embed_dim], qkv[base + embed_dim * 2 ..][0..embed_dim]);
    }

    applyVisionRotary(q, seq_len, num_heads, head_dim, rotary);
    applyVisionRotary(k, seq_len, num_heads, head_dim, rotary);

    const out = try allocator.alloc(f32, seq_len * embed_dim);
    @memset(out, 0);
    const scores = try allocator.alloc(f32, seq_len);
    defer allocator.free(scores);

    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    for (0..num_heads) |head| {
        for (0..seq_len) |qi| {
            var max_score = -std.math.inf(f32);
            const q_vec = q[qi * embed_dim + head * head_dim ..][0..head_dim];
            for (0..seq_len) |ki| {
                const k_vec = k[ki * embed_dim + head * head_dim ..][0..head_dim];
                const score = dot(q_vec, k_vec) * scale;
                scores[ki] = score;
                if (score > max_score) max_score = score;
            }
            var denom: f32 = 0.0;
            for (0..seq_len) |ki| {
                const value = @exp(scores[ki] - max_score);
                scores[ki] = value;
                denom += value;
            }
            const dst = out[qi * embed_dim + head * head_dim ..][0..head_dim];
            @memset(dst, 0);
            if (denom == 0.0) continue;
            for (0..seq_len) |ki| {
                const weight = scores[ki] / denom;
                const v_vec = v[ki * embed_dim + head * head_dim ..][0..head_dim];
                for (0..head_dim) |d| dst[d] += weight * v_vec[d];
            }
        }
    }
    return out;
}

fn applyVisionRotary(data: []f32, seq_len: usize, num_heads: usize, head_dim: usize, rotary: []const f32) void {
    const half = head_dim / 2;
    for (0..seq_len) |token| {
        const freqs = rotary[token * half ..][0..half];
        for (0..num_heads) |head| {
            const base = token * num_heads * head_dim + head * head_dim;
            var tmp: [512]f32 = undefined;
            if (head_dim > tmp.len) @panic("head_dim too large for rotary buffer");
            for (0..half) |i| {
                const cosv = @cos(freqs[i]);
                const sinv = @sin(freqs[i]);
                const x1 = data[base + i];
                const x2 = data[base + half + i];
                tmp[i] = x1 * cosv - x2 * sinv;
                tmp[half + i] = x2 * cosv + x1 * sinv;
            }
            @memcpy(data[base..][0..head_dim], tmp[0..head_dim]);
        }
    }
}

fn patchMergerTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    seq_len: usize,
    embed_dim: usize,
    merge_size: usize,
    cfg: gpt_mod.Config,
) !CT {
    const trace_enabled = visionTraceEnabled();
    var stage_start_ns: u64 = 0;
    if (trace_enabled) stage_start_ns = try nowNs();
    const grouped = try groupForMerger(cb, allocator, hidden, seq_len, embed_dim, merge_size);
    defer cb.free(grouped);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_merger.group layer=-1 elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
        stage_start_ns = try nowNs();
    }

    const merge_tokens = merge_size * merge_size;
    if (merge_tokens == 0 or (seq_len % merge_tokens) != 0) return error.InvalidPatchMergeFactor;
    const merger_width = embed_dim * merge_tokens;
    const merged_tokens = seq_len / merge_tokens;
    const fc1_w = try getVisionWeight2(cb, "visual.merger.linear_fc1.weight", "visual.merger.mlp.0.weight");
    defer cb.free(fc1_w);
    const fc1_b = try getVisionWeight2(cb, "visual.merger.linear_fc1.bias", "visual.merger.mlp.0.bias");
    defer cb.free(fc1_b);
    const fc1 = try cb.linear(grouped, fc1_w, fc1_b, merged_tokens, merger_width, merger_width);
    defer cb.free(fc1);
    const gelu = try cb.gelu(fc1);
    defer cb.free(gelu);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_merger.fc1_gelu layer=-1 elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
        stage_start_ns = try nowNs();
    }

    const output_dim: usize = if (cfg.hidden_size > 0) cfg.hidden_size else if (cfg.vision_hidden_size > 0) cfg.vision_hidden_size else return error.InvalidMultimodalConfig;
    const fc2_w = try getVisionWeight2(cb, "visual.merger.linear_fc2.weight", "visual.merger.mlp.2.weight");
    defer cb.free(fc2_w);
    const fc2_b = try getVisionWeight2(cb, "visual.merger.linear_fc2.bias", "visual.merger.mlp.2.bias");
    defer cb.free(fc2_b);
    const fc2 = try cb.linear(gelu, fc2_w, fc2_b, merged_tokens, merger_width, output_dim);
    if (trace_enabled) {
        std.debug.print("vision_trace stage=patch_merger.fc2 layer=-1 elapsed_ms={d}\n", .{nsToMs(try elapsedSince(stage_start_ns))});
    }
    return fc2;
}

fn groupForMerger(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    seq_len: usize,
    embed_dim: usize,
    merge_size: usize,
) !CT {
    const ln_w = try getVisionWeight2(cb, "visual.merger.norm.weight", "visual.merger.ln_q.weight");
    defer cb.free(ln_w);
    const ln_b = try getVisionWeight2(cb, "visual.merger.norm.bias", "visual.merger.ln_q.bias");
    defer cb.free(ln_b);
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, embed_dim, 1e-6);
    defer cb.free(normed);

    const merge_tokens = merge_size * merge_size;
    if (merge_tokens == 0 or (seq_len % merge_tokens) != 0) return error.InvalidPatchMergeFactor;
    const merged_tokens = seq_len / merge_tokens;
    const grouped_width = embed_dim * merge_tokens;
    return cb.reshape2D(allocator, normed, seq_len, embed_dim, merged_tokens, grouped_width);
}

fn getVisionWeight(cb: *const ComputeBackend, name: []const u8) !CT {
    return cb.getWeight(name) catch |err| switch (err) {
        error.MissingWeight => blk: {
            var prefixed_buf: [256]u8 = undefined;
            const hf_prefixed = std.fmt.bufPrint(&prefixed_buf, "vlm.model.{s}", .{name}) catch return error.WeightNameTooLong;
            break :blk cb.getWeight(hf_prefixed) catch |hf_err| switch (hf_err) {
                error.MissingWeight => {
                    const prefixed = std.fmt.bufPrint(&prefixed_buf, "model.{s}", .{name}) catch return error.WeightNameTooLong;
                    break :blk cb.getWeight(prefixed);
                },
                else => return hf_err,
            };
        },
        else => return err,
    };
}

fn getVisionWeight2(cb: *const ComputeBackend, primary: []const u8, fallback: []const u8) !CT {
    return getVisionWeight(cb, primary) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => getVisionWeight(cb, fallback),
        else => err,
    };
}

fn getVisionWeightFmt(cb: *const ComputeBackend, buf: *[160]u8, comptime format: []const u8, args: anytype) !CT {
    const name = try fmt(buf, format, args);
    return getVisionWeight(cb, name);
}

fn getVisionWeightFmt2(cb: *const ComputeBackend, buf: *[160]u8, comptime primary_format: []const u8, comptime fallback_format: []const u8, args: anytype) !CT {
    const primary = try fmt(buf, primary_format, args);
    return getVisionWeight(cb, primary) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => blk: {
            const fallback = try fmt(buf, fallback_format, args);
            break :blk getVisionWeight(cb, fallback);
        },
        else => err,
    };
}

fn quickGelu(x: f32) f32 {
    return x / (1.0 + @exp(-1.702 * x));
}

fn geluApprox(x: f32) f32 {
    return 0.5 * x * (1.0 + std.math.tanh(@sqrt(2.0 / std.math.pi) * (x + 0.044715 * x * x * x)));
}

fn dot(a: []const f32, b: []const f32) f32 {
    var total: f32 = 0.0;
    for (a, b) |av, bv| total += av * bv;
    return total;
}

fn fmt(buf: *[160]u8, comptime format: []const u8, args: anytype) ![]const u8 {
    return std.fmt.bufPrint(buf, format, args) catch return error.WeightNameTooLong;
}

fn visionTraceEnabled() bool {
    return platform.env.getenvBool("TERMITE_COLQWEN_VISION_TRACE");
}

fn nowNs() !u64 {
    var ts: posix.timespec = undefined;
    switch (posix.errno(posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => return error.ClockGetTimeFailed,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn elapsedSince(start_ns: u64) !u64 {
    return (try nowNs()) - start_ns;
}

fn nsToMs(ns: u64) u64 {
    return @divFloor(ns, std.time.ns_per_ms);
}
