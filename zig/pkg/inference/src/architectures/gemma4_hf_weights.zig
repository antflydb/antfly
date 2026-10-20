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

//! Canonical Gemma 4 projector names to official Hugging Face tensor names.
const std = @import("std");

pub fn resolve(buffer: []u8, name: []const u8) ![]const u8 {
    const fixed = .{
        .{ "v.patch_embd.weight", "vision_tower.patch_embedder.input_proj.weight" },
        .{ "v.position_embd.weight", "vision_tower.patch_embedder.position_embedding_table" },
        .{ "mm.input_projection.weight", "embed_vision.embedding_projection.weight" },
        .{ "mm.a.input_projection.weight", "embed_audio.embedding_projection.weight" },
        .{ "a.input_projection.weight", "audio_tower.subsample_conv_projection.input_proj_linear.weight" },
        .{ "a.pre_encode.out.weight", "audio_tower.output_proj.weight" },
        .{ "a.pre_encode.out.bias", "audio_tower.output_proj.bias" },
    };
    inline for (fixed) |pair| if (std.mem.eql(u8, name, pair[0])) return pair[1];
    if (std.mem.startsWith(u8, name, "a.conv1d.")) {
        const tail = name[9..];
        const dot = std.mem.indexOfScalar(u8, tail, '.') orelse return error.TensorNotFound;
        const layer = std.fmt.parseInt(usize, tail[0..dot], 10) catch return error.TensorNotFound;
        if (layer >= 2) return error.TensorNotFound;
        const suffix = tail[dot + 1 ..];
        const target = if (std.mem.eql(u8, suffix, "weight")) "conv.weight" else if (std.mem.eql(u8, suffix, "norm.weight")) "norm.weight" else return error.TensorNotFound;
        return std.fmt.bufPrint(buffer, "audio_tower.subsample_conv_projection.layer{d}.{s}", .{ layer, target });
    }
    if (std.mem.startsWith(u8, name, "v.blk.") or std.mem.startsWith(u8, name, "a.blk.")) {
        const vision = name[0] == 'v';
        const tail = name[6..];
        const dot = std.mem.indexOfScalar(u8, tail, '.') orelse return error.TensorNotFound;
        const layer = std.fmt.parseInt(usize, tail[0..dot], 10) catch return error.TensorNotFound;
        const suffix = tail[dot + 1 ..];
        if (vision) {
            const norms = .{
                .{ "ln1.weight", "input_layernorm.weight" },
                .{ "ln2.weight", "pre_feedforward_layernorm.weight" },
                .{ "attn_post_norm.weight", "post_attention_layernorm.weight" },
                .{ "ffn_post_norm.weight", "post_feedforward_layernorm.weight" },
                .{ "attn_q_norm.weight", "self_attn.q_norm.weight" },
                .{ "attn_k_norm.weight", "self_attn.k_norm.weight" },
            };
            inline for (norms) |pair| if (std.mem.eql(u8, suffix, pair[0]))
                return std.fmt.bufPrint(buffer, "vision_tower.encoder.layers.{d}.{s}", .{ layer, pair[1] });
            const linears = .{
                .{ "attn_q", "self_attn.q_proj" }, .{ "attn_k", "self_attn.k_proj" },
                .{ "attn_v", "self_attn.v_proj" }, .{ "attn_out", "self_attn.o_proj" },
                .{ "ffn_gate", "mlp.gate_proj" },  .{ "ffn_up", "mlp.up_proj" },
                .{ "ffn_down", "mlp.down_proj" },
            };
            inline for (linears) |pair| {
                if (std.mem.eql(u8, suffix, pair[0] ++ ".weight"))
                    return std.fmt.bufPrint(buffer, "vision_tower.encoder.layers.{d}.{s}.linear.weight", .{ layer, pair[1] });
            }
        } else {
            const vectors = .{
                .{ "ln2.weight", "norm_out.weight" },
                .{ "attn_pre_norm.weight", "norm_pre_attn.weight" },
                .{ "attn_post_norm.weight", "norm_post_attn.weight" },
                .{ "norm_conv.weight", "lconv1d.pre_layer_norm.weight" },
                .{ "conv_norm.weight", "lconv1d.conv_norm.weight" },
                .{ "conv_dw.weight", "lconv1d.depthwise_conv1d.weight" },
                .{ "per_dim_scale.weight", "self_attn.per_dim_scale" },
                .{ "attn_k_rel.weight", "self_attn.relative_k_proj.weight" },
                .{ "ffn_norm.weight", "feed_forward1.pre_layer_norm.weight" },
                .{ "ffn_post_norm.weight", "feed_forward1.post_layer_norm.weight" },
                .{ "ffn_norm_1.weight", "feed_forward2.pre_layer_norm.weight" },
                .{ "ffn_post_norm_1.weight", "feed_forward2.post_layer_norm.weight" },
            };
            inline for (vectors) |pair| if (std.mem.eql(u8, suffix, pair[0]))
                return std.fmt.bufPrint(buffer, "audio_tower.layers.{d}.{s}", .{ layer, pair[1] });
            const linears = .{
                .{ "attn_q", "self_attn.q_proj" },            .{ "attn_k", "self_attn.k_proj" },
                .{ "attn_v", "self_attn.v_proj" },            .{ "attn_out", "self_attn.post" },
                .{ "ffn_up", "feed_forward1.ffw_layer_1" },   .{ "ffn_down", "feed_forward1.ffw_layer_2" },
                .{ "ffn_up_1", "feed_forward2.ffw_layer_1" }, .{ "ffn_down_1", "feed_forward2.ffw_layer_2" },
                .{ "conv_pw1", "lconv1d.linear_start" },      .{ "conv_pw2", "lconv1d.linear_end" },
            };
            inline for (linears) |pair| {
                if (std.mem.eql(u8, suffix, pair[0] ++ ".weight"))
                    return std.fmt.bufPrint(buffer, "audio_tower.layers.{d}.{s}.linear.weight", .{ layer, pair[1] });
                inline for (.{ "input_min", "input_max", "output_min", "output_max" }) |bound| {
                    if (std.mem.eql(u8, suffix, pair[0] ++ "." ++ bound))
                        return std.fmt.bufPrint(buffer, "audio_tower.layers.{d}.{s}.{s}", .{ layer, pair[1], bound });
                }
            }
        }
    }
    return error.TensorNotFound;
}

test "HF projector mapping preserves clipped linear bounds and layer IDs" {
    var buffer: [256]u8 = undefined;
    try std.testing.expectEqualStrings("audio_tower.layers.11.feed_forward2.ffw_layer_1.input_min", try resolve(&buffer, "a.blk.11.ffn_up_1.input_min"));
    try std.testing.expectEqualStrings("vision_tower.encoder.layers.15.self_attn.q_proj.linear.weight", try resolve(&buffer, "v.blk.15.attn_q.weight"));
    try std.testing.expectError(error.TensorNotFound, resolve(&buffer, "v.blk.0.ffn_up.input_min"));
}

test "HF projector mapping matches official embeddinggemma2 checkpoint roots" {
    const cases = [_]struct { canonical: []const u8, official: []const u8 }{
        .{ .canonical = "v.patch_embd.weight", .official = "vision_tower.patch_embedder.input_proj.weight" },
        .{ .canonical = "v.position_embd.weight", .official = "vision_tower.patch_embedder.position_embedding_table" },
        .{ .canonical = "mm.input_projection.weight", .official = "embed_vision.embedding_projection.weight" },
        .{ .canonical = "mm.a.input_projection.weight", .official = "embed_audio.embedding_projection.weight" },
        .{ .canonical = "a.input_projection.weight", .official = "audio_tower.subsample_conv_projection.input_proj_linear.weight" },
        .{ .canonical = "a.pre_encode.out.weight", .official = "audio_tower.output_proj.weight" },
        .{ .canonical = "a.pre_encode.out.bias", .official = "audio_tower.output_proj.bias" },
        .{ .canonical = "a.conv1d.0.weight", .official = "audio_tower.subsample_conv_projection.layer0.conv.weight" },
        .{ .canonical = "a.conv1d.1.norm.weight", .official = "audio_tower.subsample_conv_projection.layer1.norm.weight" },
        .{ .canonical = "v.blk.0.ln1.weight", .official = "vision_tower.encoder.layers.0.input_layernorm.weight" },
        .{ .canonical = "v.blk.15.attn_k_norm.weight", .official = "vision_tower.encoder.layers.15.self_attn.k_norm.weight" },
        .{ .canonical = "v.blk.7.ffn_down.weight", .official = "vision_tower.encoder.layers.7.mlp.down_proj.linear.weight" },
        .{ .canonical = "a.blk.0.attn_pre_norm.weight", .official = "audio_tower.layers.0.norm_pre_attn.weight" },
        .{ .canonical = "a.blk.11.per_dim_scale.weight", .official = "audio_tower.layers.11.self_attn.per_dim_scale" },
        .{ .canonical = "a.blk.3.conv_dw.weight", .official = "audio_tower.layers.3.lconv1d.depthwise_conv1d.weight" },
        .{ .canonical = "a.blk.5.attn_k_rel.weight", .official = "audio_tower.layers.5.self_attn.relative_k_proj.weight" },
        .{ .canonical = "a.blk.8.attn_out.weight", .official = "audio_tower.layers.8.self_attn.post.linear.weight" },
        .{ .canonical = "a.blk.2.conv_pw1.output_max", .official = "audio_tower.layers.2.lconv1d.linear_start.output_max" },
    };
    var buffer: [256]u8 = undefined;
    for (cases) |case| {
        try std.testing.expectEqualStrings(case.official, try resolve(&buffer, case.canonical));
        try std.testing.expect(!std.mem.startsWith(u8, case.official, "model."));
    }
    try std.testing.expectError(error.TensorNotFound, resolve(&buffer, "a.conv1d.2.weight"));
    try std.testing.expectError(error.TensorNotFound, resolve(&buffer, "a.blk.0.per_dim_scale.weight.input_min"));
}
