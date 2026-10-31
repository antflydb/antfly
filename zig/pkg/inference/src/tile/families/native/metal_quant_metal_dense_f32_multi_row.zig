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

pub const source_0 =
    \\           "kernel void termite_apply_linear_multi_row(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; termite_dense_scalar(input, weight, bias, output, p.in_dim, p.out_dim, gid / p.out_dim, gid % p.out_dim);\n"
    \\           "}\n"
    \\           "kernel void termite_apply_linear_multi_row_reduce(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    if (tg.x >= p.out_dim || tg.y >= p.rows) return; termite_dense_reduce(input, weight, bias, output, partials, p.in_dim, p.out_dim, tg.y, tg.x, lane);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_DENSE_TILED(termite_apply_linear_multi_row_tiled_f32) "\n"
    \\           "    threadgroup float a_tile[8][32]; threadgroup float b_tile[16][32];\n"
    \\           "    termite_dense_tile<8u, 16u, false>(input, weight, bias, output, p, 0u, &a_tile[0][0], &b_tile[0][0], tid, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_DENSE_ACTIVATION_TILED(termite_apply_linear_multi_row_tiled_activation_f32) "\n"
    \\           "    threadgroup float a_tile[8][32]; threadgroup float b_tile[16][32];\n"
    \\           "    termite_dense_tile<8u, 16u, true>(input, weight, bias, output, p, ap.activation_kind, &a_tile[0][0], &b_tile[0][0], tid, tg);\n"
    \\           "}\n"
    \\
;
