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

//! Adapt the existing Gemma 4 media kernels to the original HF tensor names.
//! No checkpoint conversion or norm-offset transformation is performed.
const std = @import("std");

pub fn name(a: std.mem.Allocator, source: []const u8) ![]u8 {
    const globals = .{
        .{ "v.patch_embd.weight", "vision_tower.patch_embedder.input_proj.weight" },
        .{ "v.position_embd.weight", "vision_tower.patch_embedder.position_embedding_table" },
        .{ "mm.input_projection.weight", "embed_vision.embedding_projection.weight" },
        .{ "a.conv1d.0.weight", "audio_tower.subsample_conv_projection.layer0.conv.weight" },
        .{ "a.conv1d.1.weight", "audio_tower.subsample_conv_projection.layer1.conv.weight" },
        .{ "a.conv1d.0.norm.weight", "audio_tower.subsample_conv_projection.layer0.norm.weight" },
        .{ "a.conv1d.1.norm.weight", "audio_tower.subsample_conv_projection.layer1.norm.weight" },
        .{ "a.input_projection.weight", "audio_tower.subsample_conv_projection.input_proj_linear.weight" },
        .{ "a.pre_encode.out.weight", "audio_tower.output_proj.weight" },
        .{ "a.pre_encode.out.bias", "audio_tower.output_proj.bias" },
        .{ "mm.a.input_projection.weight", "embed_audio.embedding_projection.weight" },
    };
    inline for (globals) |entry| if (std.mem.eql(u8, source, entry[0])) return a.dupe(u8, entry[1]);
    const vision = std.mem.startsWith(u8, source, "v.blk.");
    if (!vision and !std.mem.startsWith(u8, source, "a.blk.")) return error.InvalidEmbeddingGemma2Weight;
    const rest = source[6..];
    const dot = std.mem.indexOfScalar(u8, rest, '.') orelse return error.InvalidEmbeddingGemma2Weight;
    const layer = std.fmt.parseInt(usize, rest[0..dot], 10) catch return error.InvalidEmbeddingGemma2Weight;
    if (layer >= (if (vision) @as(usize, 16) else 12)) return error.InvalidEmbeddingGemma2Weight;
    const suffix = rest[dot + 1 ..];
    const vision_vectors = .{
        .{ "ln1.weight", "input_layernorm.weight" },
        .{ "ln2.weight", "pre_feedforward_layernorm.weight" },
        .{ "attn_post_norm.weight", "post_attention_layernorm.weight" },
        .{ "ffn_post_norm.weight", "post_feedforward_layernorm.weight" },
        .{ "attn_q_norm.weight", "self_attn.q_norm.weight" },
        .{ "attn_k_norm.weight", "self_attn.k_norm.weight" },
    };
    const vision_linears = .{
        .{ "attn_q", "self_attn.q_proj" }, .{ "attn_k", "self_attn.k_proj" },
        .{ "attn_v", "self_attn.v_proj" }, .{ "attn_out", "self_attn.o_proj" },
        .{ "ffn_gate", "mlp.gate_proj" },  .{ "ffn_up", "mlp.up_proj" },
        .{ "ffn_down", "mlp.down_proj" },
    };
    const audio_vectors = .{
        .{ "ffn_norm.weight", "feed_forward1.pre_layer_norm.weight" },
        .{ "ffn_post_norm.weight", "feed_forward1.post_layer_norm.weight" },
        .{ "ffn_norm_1.weight", "feed_forward2.pre_layer_norm.weight" },
        .{ "ffn_post_norm_1.weight", "feed_forward2.post_layer_norm.weight" },
        .{ "attn_pre_norm.weight", "norm_pre_attn.weight" },
        .{ "attn_post_norm.weight", "norm_post_attn.weight" },
        .{ "norm_conv.weight", "lconv1d.pre_layer_norm.weight" },
        .{ "conv_norm.weight", "lconv1d.conv_norm.weight" },
        .{ "conv_dw.weight", "lconv1d.depthwise_conv1d.weight" },
        .{ "ln2.weight", "norm_out.weight" },
        .{ "per_dim_scale.weight", "self_attn.per_dim_scale" },
        .{ "attn_k_rel.weight", "self_attn.relative_k_proj.weight" },
    };
    const audio_linears = .{
        .{ "ffn_up", "feed_forward1.ffw_layer_1" },   .{ "ffn_down", "feed_forward1.ffw_layer_2" },
        .{ "ffn_up_1", "feed_forward2.ffw_layer_1" }, .{ "ffn_down_1", "feed_forward2.ffw_layer_2" },
        .{ "conv_pw1", "lconv1d.linear_start" },      .{ "conv_pw2", "lconv1d.linear_end" },
        .{ "attn_q", "self_attn.q_proj" },            .{ "attn_k", "self_attn.k_proj" },
        .{ "attn_v", "self_attn.v_proj" },            .{ "attn_out", "self_attn.post" },
    };
    const prefix = if (vision) "vision_tower.encoder.layers" else "audio_tower.layers";
    if (vision) {
        inline for (vision_vectors) |entry| if (std.mem.eql(u8, suffix, entry[0])) return std.fmt.allocPrint(a, "{s}.{d}.{s}", .{ prefix, layer, entry[1] });
        inline for (vision_linears) |entry| if (std.mem.eql(u8, suffix, entry[0] ++ ".weight")) return std.fmt.allocPrint(a, "{s}.{d}.{s}.linear.weight", .{ prefix, layer, entry[1] });
    } else {
        inline for (audio_vectors) |entry| if (std.mem.eql(u8, suffix, entry[0])) return std.fmt.allocPrint(a, "{s}.{d}.{s}", .{ prefix, layer, entry[1] });
        inline for (audio_linears) |entry| {
            if (std.mem.eql(u8, suffix, entry[0] ++ ".weight")) return std.fmt.allocPrint(a, "{s}.{d}.{s}.linear.weight", .{ prefix, layer, entry[1] });
            inline for (.{ "input_min", "input_max", "output_min", "output_max" }) |bound| {
                if (std.mem.eql(u8, suffix, entry[0] ++ "." ++ bound)) return std.fmt.allocPrint(a, "{s}.{d}.{s}.{s}", .{ prefix, layer, entry[1], bound });
            }
        }
    }
    return error.InvalidEmbeddingGemma2Weight;
}

test "embeddinggemma2 HF media mapping preserves original linear and scalar names" {
    const a = std.testing.allocator;
    const linear = try name(a, "a.blk.11.ffn_up_1.weight");
    defer a.free(linear);
    const bound = try name(a, "a.blk.2.attn_q.input_max");
    defer a.free(bound);
    const norm = try name(a, "v.blk.15.ln2.weight");
    defer a.free(norm);
    try std.testing.expectEqualStrings("audio_tower.layers.11.feed_forward2.ffw_layer_1.linear.weight", linear);
    try std.testing.expectEqualStrings("audio_tower.layers.2.self_attn.q_proj.input_max", bound);
    try std.testing.expectEqualStrings("vision_tower.encoder.layers.15.pre_feedforward_layernorm.weight", norm);
    try std.testing.expectError(error.InvalidEmbeddingGemma2Weight, name(a, "v.blk.16.ln1.weight"));
}
