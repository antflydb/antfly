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

//! Checkpoint layout shared by the resident attention providers.
const std = @import("std");
const model = @import("inkling2_checkpoint.zig");

pub const Weight = enum { q, k, v, r, sink, gate, out, q_scale, r_scale, k_taps, v_taps, k_dynamic, v_dynamic, relative_basis };
pub const query_weights = [_]Weight{ .q, .r, .gate, .sink };
pub const projection_weights = query_weights ++ [_]Weight{ .k, .v };

pub fn projectionWidth(text: model.TextConfig, weight: Weight) u32 {
    return switch (weight) {
        .q, .gate => @as(u32, text.num_attention_heads) * text.head_dim,
        .r => @as(u32, text.num_attention_heads) * text.d_rel,
        .sink => text.num_attention_heads,
        .k, .v => @as(u32, text.num_key_value_heads) * text.head_dim,
        .k_dynamic, .v_dynamic => @as(u32, text.num_key_value_heads) * text.sconv_kernel_size,
        else => unreachable,
    };
}

pub fn projectionSize(text: model.TextConfig, weights: []const Weight) u32 {
    var width: u32 = 0;
    for (weights) |weight| width += projectionWidth(text, weight);
    return width;
}

pub fn tensorName(weight: Weight, local: bool) []const u8 {
    return switch (weight) {
        .q => "attn.wq_du.weight",
        .k => "attn.wk_dv.weight",
        .v => "attn.wv_dv.weight",
        .r => "attn.wr_du.weight",
        .sink => "attn.attn_sink_proj.weight",
        .gate => "attn.gate_attn.weight",
        .out => "attn.wo_ud.weight",
        .q_scale => "attn.q_l2_clip.scale",
        .r_scale => "attn.rel_l2_clip.scale",
        .k_taps => if (local) "attn.k_sconv.weight" else "attn.k_sconv.static_w",
        .v_taps => if (local) "attn.v_sconv.weight" else "attn.v_sconv.static_w",
        .k_dynamic => "attn.k_sconv.weight_proj",
        .v_dynamic => "attn.v_sconv.weight_proj",
        .relative_basis => "attn.rel_logits_proj.proj",
    };
}

pub fn isLocal(text: model.TextConfig, layer: usize) !bool {
    if (layer >= model.layer_count) return error.InvalidInkling2Layer;
    return std.mem.indexOfScalar(u8, &text.local_layer_ids, @intCast(layer)) != null;
}

/// Convert the checkpoint's [relative_dim, extent] to output-major BF16.
pub fn transposeRelative(allocator: std.mem.Allocator, basis: []const u8, dim: usize, extent: usize) ![]u8 {
    const bytes = try std.math.mul(usize, try std.math.mul(usize, dim, extent), 2);
    if (basis.len != bytes) return error.InvalidShape;
    const result = try allocator.alloc(u8, bytes);
    for (0..dim) |d| for (0..extent) |relative_index| {
        const from = (d * extent + relative_index) * 2;
        const to = (relative_index * dim + d) * 2;
        @memcpy(result[to..][0..2], basis[from..][0..2]);
    };
    return result;
}

test "Inkling attention weights transpose BF16 without changing bits" {
    const input = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const got = try transposeRelative(std.testing.allocator, &input, 2, 3);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 7, 8, 3, 4, 9, 10, 5, 6, 11, 12 }, got);
    try std.testing.expectError(error.InvalidShape, transposeRelative(std.testing.allocator, &input, 3, 3));
}
