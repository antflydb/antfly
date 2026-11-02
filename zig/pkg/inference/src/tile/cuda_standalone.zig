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

//! Offline CUDA programs: operation, storage and fixed schedule are authoritative.
//! Historical exported symbols remain ABI adapters. This catalog is consumed by
//! host launch owners and exported to the offline producers by tile codegen.
const std = @import("std");
pub const Profile = struct {
    family: []const u8,
    file: []const u8,
    entry: [:0]const u8,
    producer: []const u8,
    exports: []const []const u8 = &.{},
    nvcc_flags: []const []const u8 = &.{ "--fmad=false", "-std=c++17" },
    target: u16 = 121,
    threads: u32 = 0,
    shared: u32 = 0,
    hidden: u32 = 0,
    wide: u32 = 0,
    bf16: bool = false,
};
pub const attention: Profile = .{ .family = "attention_inputs", .file = "attention_inputs_partial_mrope_f32.ptx", .entry = "lighton_qk_norm_partial_mrope_gate", .producer = "attention_inputs_partial_mrope_f32.cu", .target = 75 };
pub const encoder: Profile = .{ .family = "encoder_block", .file = "encoder_block_phases_bf16.ptx", .entry = "lighton_bf16_vision_norm", .producer = "encoder_block_phases_bf16.cu", .exports = &.{ encoder_entries.norm, encoder_entries.add, encoder_entries.gelu, encoder_entries.qkv_rope }, .target = 80 };
pub const encoder_entries = .{ .norm = "lighton_bf16_vision_norm", .add = "lighton_bf16_vision_add", .gelu = "lighton_bf16_vision_gelu", .qkv_rope = "lighton_bf16_vision_qkv_rope" };

fn projection(comptime is_gelu: bool, comptime hidden: u32, comptime bf16: bool) Profile {
    const wide = hidden * 4;
    return .{
        .family = if (is_gelu) "projection_gelu" else "projection_residual",
        .file = if (is_gelu) std.fmt.comptimePrint("projection_gelu_sm121_k{d}_n{d}_bias_{s}.ptx", .{ hidden, wide, if (bf16) "bf16" else "f32" }) else std.fmt.comptimePrint("projection_residual_sm121_k{d}_n{d}_io_{s}.ptx", .{ wide, hidden, if (bf16) "bf16" else "f32" }),
        .entry = if (is_gelu) "lighton_vision_up" else "lighton_vision_down",
        .producer = if (is_gelu) "projection_gelu_bf16.py" else "projection_residual_bf16.py",
        .threads = 128,
        .shared = if (is_gelu) 24576 else 49152,
        .hidden = hidden,
        .wide = wide,
        .bf16 = bf16,
    };
}
fn projections(comptime is_gelu: bool) [4]Profile {
    return .{ projection(is_gelu, 768, false), projection(is_gelu, 768, true), projection(is_gelu, 1024, false), projection(is_gelu, 1024, true) };
}
pub const gelu = projections(true);
pub const residual = projections(false);
pub const top1 = [_]Profile{
    .{ .family = "projection_top1_partial", .file = "projection_top1_bf16_k1024_n248320_partial.ptx", .entry = "lighton_head_top1_partial", .producer = "projection_top1_bf16.py", .hidden = 1024, .wide = 248320, .threads = 256, .shared = 32768 },
    .{ .family = "projection_top1_finish", .file = "projection_top1_bf16_k1024_n248320_finish.ptx", .entry = "lighton_head_top1_finish", .producer = "projection_top1_bf16.py", .threads = 128, .shared = 16 },
};

// Historical image filenames and entry symbols preserve the qualification ABI.
// New owners, producer paths and policy names describe reusable operations.
fn nativeProfile(comptime family: []const u8, comptime file: []const u8, comptime producer: []const u8, comptime target: u16, comptime entries: anytype, comptime flags: []const []const u8) Profile {
    const fields = @typeInfo(@TypeOf(entries)).@"struct".field_names;
    const symbols = comptime blk: {
        var result: [fields.len][]const u8 = undefined;
        for (fields, 0..) |field, index| result[index] = @field(entries, field);
        break :blk result;
    };
    return .{ .family = family, .file = file, .producer = producer, .entry = @field(entries, fields[0]), .exports = &symbols, .target = target, .nvcc_flags = flags };
}
pub const activation_bf16_entries = .{
    .silu_product_f32 = "lighton_silu_up_bf16",
    .packed_silu_product_f32 = "lighton_packed_silu_up_bf16",
    .rms_f32 = "antfly_rms_tree_bf16",
    .silu_product_bf16 = "lighton_silu_up_bf16_inputs",
    .residual_f32 = "lighton_bf16_residual_f32",
};
pub const activation_bf16 = nativeProfile("activation_bf16", "lighton_activation.ptx", "bf16_activation_programs.cu", 80, activation_bf16_entries, &.{});
pub const paged_attention_bf16_entries = .{
    .chunks_d128 = "lighton_attention_chunks",
    .combine_d128 = "lighton_attention_combine",
    .write_suffix = "lighton_attention_write_suffix",
    .chunks128_d128 = "lighton_attention_chunks128",
    .advance_graph = "lighton_graph_advance",
    .grouped_chunks32_d128 = "lighton_attention_grouped_chunks32",
    .grouped_chunks128_d128 = "lighton_attention_grouped_chunks128",
};
pub const paged_attention_bf16 = nativeProfile("paged_attention_bf16", "lighton_attention.ptx", "paged_attention_bf16_programs.cu", 75, paged_attention_bf16_entries, &.{ "--fmad=false", "-std=c++17" });
pub const gated_delta_entries = .{
    .causal_conv = "antfly_gdn_causal_conv",
    .normalize_qk = "lighton_normalize",
    .recurrent = "lighton_recurrent",
    .gated_norm = "lighton_gated_norm",
    .batch_causal_conv = "lighton_batch_conv",
    .batch_recurrent = "lighton_batch_recurrent",
    .batch_conv_normalize = "lighton_batch_conv_normalize",
    .batch_recurrent_norm = "lighton_batch_recurrent_norm",
    .batch_conv_normalize_cached_gates = "lighton_batch_conv_normalize_cached_gates",
    .batch_recurrent_norm_cached_gates = "lighton_batch_recurrent_norm_cached_gates",
    .conv_cached_gates = "lighton_conv_cached_gates",
    .recurrent_cached_gates = "lighton_recurrent_cached_gates",
    .split_projections = "lighton_split_projections",
    .prefix_conv_normalize_cached_gates = "lighton_prefix_conv_normalize_cached_gates",
    .prefix_batch_conv_normalize = "lighton_prefix_batch_conv_normalize",
    .prefix_batch_recurrent = "lighton_prefix_batch_recurrent",
    .prefix_batch_recurrent_shared_qk = "lighton_prefix_batch_recurrent_shared_qk",
    .ragged_conv_normalize_cached_gates = "lighton_ragged_conv_normalize_cached_gates",
    .ragged_recurrent_shared_qk = "lighton_ragged_recurrent_shared_qk",
    .ragged_prefix_conv = "lighton_ragged_prefix_conv",
    .ragged_recurrent_prefix_only = "lighton_ragged_recurrent_prefix_only",
    .ragged_prefix_gated_norm = "lighton_ragged_prefix_gated_norm",
    .decode_fused_p2 = "lighton_decode_fused_p2",
};
pub const gated_delta = nativeProfile("gated_delta", "lighton_recurrent.ptx", "gated_delta_recurrent.cu", 75, gated_delta_entries, &.{ "--fmad=false", "-std=c++17" });
pub const vision_projection_bf16_entries = .{
    .layer_norm_bf16 = "lighton_vision_norm_bf16",
    .bias_gelu_bf16 = "lighton_vision_bias_gelu_bf16",
    .bias_residual_f32 = "lighton_vision_bias_residual_f32",
    .split_qkv_rope_2d_bf16 = "antfly_qkv_rope_two_axis_bf16",
    .learned_positions = "lighton_vision_learned_positions_f32",
};
pub const vision_projection_bf16 = nativeProfile("vision_projection_bf16", "lighton_vision.ptx", "vision_projection_programs.cu", 80, vision_projection_bf16_entries, &.{});
pub const encoder_decoder_f16_entries = .{
    .token_depthwise_residual = "antfly_depthwise_3x3_residual_f32",
    .layer_norm_f16 = "antfly_layer_norm_warp_finish_f16",
    .exact_gelu_f16 = "florence_exact_gelu_f16",
    .cross_q1_h12_d64_f32 = "florence_cross_q1_f32",
    .window_norm_f16 = "antfly_window_layer_norm_warp_finish_f16",
    .split_qkv_f16 = "florence_split_qkv_f16",
    .append_self_kv_f32 = "florence_append_self_kv_f32",
};
pub const encoder_decoder_f16 = nativeProfile("encoder_decoder_f16", "florence_fused.ptx", "vision_f16_programs.cu", 80, encoder_decoder_f16_entries, &.{});

pub const profiles = [_]Profile{ attention, encoder } ++ gelu ++ residual ++ top1 ++ [_]Profile{ activation_bf16, paged_attention_bf16, gated_delta, vision_projection_bf16, encoder_decoder_f16 };
