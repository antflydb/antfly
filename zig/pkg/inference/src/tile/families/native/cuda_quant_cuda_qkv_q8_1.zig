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
const cuda_phases = @import("cuda_phases.zig");

pub const source_0 =
    \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile4(ANTFLY_CUDA_Q8_INPUT_QKV_PARAMS) {
    \\    termite_packed_qkv_projection_impl<4u>(dst_q, dst_k, dst_v, q8_input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
    \\}
    \\
;

pub const source_1 =
    program.render(
        \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8(ANTFLY_CUDA_Q8_INPUT_QKV_PARAMS) {
        \\    const unsigned int cols = 8u;
        \\{{qkv_projection_tile}}
        \\
        \\{{qkv_planes}}
        \\
        \\    unsigned int tid = threadIdx.x;
        \\    unsigned int lane = tid & 31u;
        \\    unsigned int warp = tid >> 5;
        \\    unsigned int row_blocks = in_dim / 32u;
        \\    __shared__ float warp_partial[8][8];
        \\    float acc[8];
        \\    #pragma unroll
        \\    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;
        \\
        \\{{q4_q8_pair_dot}}
        \\    termite_store_q4_0_cols_warp_sum_warps<8u, 8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
        \\}
        \\
    , cuda_phases.projection);
