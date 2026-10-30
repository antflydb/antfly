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

//! LightOnOCR-3 bf16-weight batched linear kernels (Metal).
//!
//! Both groups compute y[b, col] = dot(x[b, :], w[col, :]) + bias[col] for a
//! small batch of rows, with bf16 weights widened by `<< 16`. They differ in
//! how the batch is spelled:
//!
//! - `vec4`: one SIMD group per output column, batch up to B, one float4
//!   accumulator per row guarded by `p.rows > b`, written out unrolled.
//! - `tiled`: exact batch and column counts, instances of the hand-written
//!   `termite_lightonocr3_tiled_body<Batch, Columns>` template.

const std = @import("std");

const fmt = std.fmt.comptimePrint;

/// Batch sizes of the guarded `vec4` kernels.
pub const vec4_batches = [_]u32{ 2, 4, 8, 16 };

/// (batch, columns per threadgroup) of the exact `tiled` instances.
pub const Tiled = struct { batch: u32, columns: u32 };
pub const tiled = [_]Tiled{
    .{ .batch = 2, .columns = 2 },  .{ .batch = 3, .columns = 2 },  .{ .batch = 4, .columns = 2 },
    .{ .batch = 5, .columns = 4 },  .{ .batch = 6, .columns = 4 },  .{ .batch = 7, .columns = 4 },
    .{ .batch = 8, .columns = 4 },  .{ .batch = 9, .columns = 1 },  .{ .batch = 10, .columns = 1 },
    .{ .batch = 11, .columns = 1 }, .{ .batch = 12, .columns = 1 }, .{ .batch = 13, .columns = 1 },
    .{ .batch = 14, .columns = 1 }, .{ .batch = 15, .columns = 1 }, .{ .batch = 16, .columns = 1 },
};

fn vec4(comptime batch: u32) []const u8 {
    var s: []const u8 = fmt("kernel void termite_lightonocr3_batch{d}_vec4(device const float *x [[buffer(0)]], device const ushort *w [[buffer(1)]], device const float *b [[buffer(2)]], device float *y [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], uint lane [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {{\n", .{batch});
    s = s ++ fmt(" if(row>=p.out_dim || p.rows>{d}) return;\n", .{batch});
    for (0..batch) |i| s = s ++ fmt(" float4 a{d}=0.0f;\n", .{i});
    s = s ++
        \\ for(uint i=lane*4;i<p.in_dim;i+=128) {
        \\  ushort4 bits=*((device const ushort4 *)(w+row*p.in_dim+i));
        \\  float4 weight=as_type<float4>(uint4(bits)<<16);
        \\
    ;
    for (0..batch) |i| s = s ++ fmt("  if(p.rows>{0d}) a{0d}+=*((device const float4 *)(x+{0d}*p.in_dim+i))*weight;\n", .{i});
    s = s ++ " }\n";
    for (0..batch) |i| s = s ++ fmt(" float s{0d}=simd_sum(dot(a{0d},float4(1))); if(lane==0 && p.rows>{0d}) y[{0d}*p.out_dim+row]=s{0d}+b[row];\n", .{i});
    return s ++ "}\n";
}

pub fn vec4Source() []const u8 {
    comptime {
        @setEvalBranchQuota(1_000_000);
        var s: []const u8 = "";
        for (vec4_batches) |b| s = s ++ vec4(b);
        return s;
    }
}

pub fn tiledSource() []const u8 {
    comptime {
        @setEvalBranchQuota(1_000_000);
        var s: []const u8 = "";
        for (tiled) |t| s = s ++ fmt("kernel void termite_lightonocr3_batch{0d}_tile{1d}(device const float*x[[buffer(0)]],device const ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant termite_metal_linear_params&p[[buffer(4)]],uint lane[[thread_index_in_threadgroup]],uint group[[threadgroup_position_in_grid]]) {{ termite_lightonocr3_tiled_body<{0d},{1d}>(x,w,bias,y,p,lane,group); }}\n", .{ t.batch, t.columns });
        return s;
    }
}
