// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const Direct = @import("antfly_tile").native_convolution.Direct;
pub const cuda = .{ .direct2d = (Direct{ .rank = .two }).native(.{ .integer = "unsigned int", .index = "idx", .output = "dst" }) };
pub const metal = .{
    .direct2d = (Direct{ .rank = .two }).native(.{ .integer = "uint", .index = "gid", .output = "output", .prefix = "p.", .optional_bias = true, .guard_groups = true, .unsigned_bounds = true }),
    .direct1d = (Direct{ .rank = .one }).native(.{ .integer = "uint", .index = "gid", .output = "output", .prefix = "p.", .optional_bias = true, .unsigned_bounds = true }),
};

const tile = @import("antfly_tile");
const Tiled = tile.native_convolution.Stencil3x3Stride2Tile16;
const program = @import("../../native_program.zig");
pub const metal_audio = .{
    .tiled_convolution = program.metalAt("", Tiled.native(.{ .integer = "uint", .shared = "threadgroup", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .thread = "tid", .group = "group", .prefix = "p.", .optional_bias = true, .bounded_tail = true })),
    .matrix_coordinates = program.metalAt("", tile.native_convolution.Stencil3x3Stride2.coordinates("pos", "bk", "p.out_w")),
};
pub const cuda_tiled = struct {
    pub const source_0 = "extern \"C\" __global__ void antfly_audio_conv_tiled_f32(float *out,const float *input,const float *weight,const float *bias,unsigned in_channels,unsigned out_channels,unsigned height,unsigned width,unsigned out_h,unsigned out_w) {\n" ++
        Tiled.native(.{ .integer = "unsigned", .shared = "__shared__", .barrier = "__syncthreads();", .thread = "threadIdx", .group = "blockIdx", .wide_base = true, .fused = true }) ++ "}\n";
};
