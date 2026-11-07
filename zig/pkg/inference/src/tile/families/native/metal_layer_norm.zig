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

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const norm = @import("antfly_tile").native_normalization;
fn layer(comptime e: norm.LayerExport) []const u8 {
    return program.metal(norm.metalLayer(e, "termite_metal_apply_layer_norm_params", "termite_layer_norm_family"));
}
pub const source_0 = program.join(&.{
    layer(.{ .name = "termite_apply_layer_norm_1x", .input = .plain, .save_sum = false, .statistics = .centered_two_pass }),
    layer(.{ .name = "termite_apply_layer_norm_rows", .input = .plain, .save_sum = false, .statistics = .raw_moments_tree }),
});

pub const source_1 = program.join(&.{
    layer(.{ .name = "termite_apply_add_layer_norm_1x", .input = .residual, .save_sum = false, .statistics = .centered_two_pass }),
    layer(.{ .name = "termite_apply_add_layer_norm_sum_1x", .input = .residual, .save_sum = true, .statistics = .centered_two_pass }),
    layer(.{ .name = "termite_apply_add_layer_norm_rows", .input = .residual, .save_sum = false, .statistics = .raw_moments_tree }),
    layer(.{ .name = "termite_apply_add_layer_norm_sum_rows", .input = .residual, .save_sum = true, .statistics = .raw_moments_tree }),
    layer(.{ .name = "termite_apply_bias_add_layer_norm_1x", .input = .bias_residual, .save_sum = false, .statistics = .centered_two_pass }),
    layer(.{ .name = "termite_apply_bias_add_layer_norm_rows", .input = .bias_residual, .save_sum = false, .statistics = .raw_moments_tree }),
});

pub const source_2 =
    \\           "kernel void termite_gliner_boundary_ffn_bias_add_layer_norm(device const float *projected [[buffer(0)]], device const float *bias [[buffer(1)]], device const float *residual [[buffer(2)]], device const float *gamma [[buffer(3)]], device const float *beta [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_apply_layer_norm_params &p [[buffer(6)]], uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]) {\n"
    \\           "    uint row=gid/32u;if(p.hidden_size==0u)return;uint base=row*p.hidden_size;float sum=0.0f;for(uint d=lane;d<p.hidden_size;d+=32u){float v=(projected[base+d]+bias[d])+residual[base+d];sum+=v;}float mean=simd_sum(sum)/float(p.hidden_size);float sq=0.0f;for(uint d=lane;d<p.hidden_size;d+=32u){float v=(projected[base+d]+bias[d])+residual[base+d]-mean;sq+=v*v;}float inv=rsqrt(simd_sum(sq)/float(p.hidden_size)+p.eps);for(uint d=lane;d<p.hidden_size;d+=32u){float v=(projected[base+d]+bias[d])+residual[base+d];output[base+d]=(v-mean)*inv*gamma[d]+beta[d];}\n"
    \\           "}\n"
    \\
;
