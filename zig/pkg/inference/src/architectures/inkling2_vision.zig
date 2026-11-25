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

//! Backend-independent checkpoint tensors and image grid for the Inkling visual encoder.
const std = @import("std");

pub const width = 1024;
pub const inner = 4096;
pub const text_width = 2304;
pub const patch_features = 20 * 20 * 3;
pub const layers = 23;
pub const eps = 1e-5;

pub const Layer = enum { ln1, bias1, ln2, bias2, scale1, scale2, q, q_bias, k, k_bias, v, v_bias, out, out_bias, up, up_bias, down, down_bias };
pub const Weight = enum { patch, pos, cls, cls_pos, pre, pre_bias, in, out };
const Temp = enum { rgb, patches, patch_embed, embed, hidden, normed, q, k, v, attention, update, wide, merged, blas };

const Tensor = struct { name: []const u8, shape: []const i64 };

pub const model_tensors: std.EnumArray(Weight, Tensor) = .init(.{
    .patch = .{ .name = "backbone.embeddings.patch_embedding.weight", .shape = &.{ width, patch_features } },
    .pos = .{ .name = "backbone.embeddings.position_embedding", .shape = &.{ 32 * 32, width } },
    .cls = .{ .name = "backbone.embeddings.class_embedding", .shape = &.{width} },
    .cls_pos = .{ .name = "backbone.embeddings.cls_position_embedding", .shape = &.{width} },
    .pre = .{ .name = "backbone.ln_pre.weight", .shape = &.{width} },
    .pre_bias = .{ .name = "backbone.ln_pre.bias", .shape = &.{width} },
    .in = .{ .name = "proj_in.weight", .shape = &.{ inner, width * 4 } },
    .out = .{ .name = "proj_out.weight", .shape = &.{ text_width, inner } },
});
pub const layer_tensors: std.EnumArray(Layer, Tensor) = .init(.{
    .ln1 = .{ .name = "ln_1.weight", .shape = &.{width} },
    .bias1 = .{ .name = "ln_1.bias", .shape = &.{width} },
    .ln2 = .{ .name = "ln_2.weight", .shape = &.{width} },
    .bias2 = .{ .name = "ln_2.bias", .shape = &.{width} },
    .scale1 = .{ .name = "ls_1.gamma", .shape = &.{width} },
    .scale2 = .{ .name = "ls_2.gamma", .shape = &.{width} },
    .q = .{ .name = "self_attn.q_proj.weight", .shape = &.{ width, width } },
    .q_bias = .{ .name = "self_attn.q_proj.bias", .shape = &.{width} },
    .k = .{ .name = "self_attn.k_proj.weight", .shape = &.{ width, width } },
    .k_bias = .{ .name = "self_attn.k_proj.bias", .shape = &.{width} },
    .v = .{ .name = "self_attn.v_proj.weight", .shape = &.{ width, width } },
    .v_bias = .{ .name = "self_attn.v_proj.bias", .shape = &.{width} },
    .out = .{ .name = "self_attn.out_proj.weight", .shape = &.{ width, width } },
    .out_bias = .{ .name = "self_attn.out_proj.bias", .shape = &.{width} },
    .up = .{ .name = "mlp.c_fc.weight", .shape = &.{ inner, width } },
    .up_bias = .{ .name = "mlp.c_fc.bias", .shape = &.{inner} },
    .down = .{ .name = "mlp.c_proj.weight", .shape = &.{ width, inner } },
    .down_bias = .{ .name = "mlp.c_proj.bias", .shape = &.{width} },
});

pub const Geometry = struct {
    grid_x: u32,
    grid_y: u32,

    pub fn fromRgb(rgb: []const u8, x: u32, y: u32) !Geometry {
        if (x == 0 or y == 0 or x > max_image_side or y > max_image_side or rgb.len != @as(usize, x) * y * 3)
            return error.InvalidInkling2Image;
        return fromDimensions(x, y);
    }

    pub fn fromDimensions(x: u32, y: u32) Geometry {
        return .{ .grid_x = patchGrid(x), .grid_y = patchGrid(y) };
    }

    fn patchGrid(size: u32) u32 {
        const merge_size = 40;
        return 2 * (size / merge_size + @intFromBool(size % merge_size != 0));
    }

    pub fn patchRows(self: Geometry) u32 {
        return self.grid_x * self.grid_y;
    }

    pub fn mergedRows(self: Geometry) u32 {
        return self.patchRows() / 4;
    }
};

pub const max_image_side = 4096;

test "Inkling image grid pads each dimension to complete merge blocks" {
    const rgb: [41 * 79 * 3]u8 = undefined;
    const geometry = try Geometry.fromRgb(&rgb, 41, 79);
    try std.testing.expectEqual(@as(u32, 4), geometry.grid_x);
    try std.testing.expectEqual(@as(u32, 4), geometry.grid_y);
    try std.testing.expectEqual(@as(u32, 16), geometry.patchRows());
    try std.testing.expectEqual(@as(u32, 4), geometry.mergedRows());
    try std.testing.expectError(error.InvalidInkling2Image, Geometry.fromRgb(rgb[1..], 41, 79));
    try std.testing.expectError(error.InvalidInkling2Image, Geometry.fromRgb(&.{}, 4097, 1));
}
