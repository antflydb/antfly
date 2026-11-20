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
