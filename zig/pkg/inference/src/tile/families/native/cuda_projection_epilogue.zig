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

const program = @import("../../native_program.zig");
const tile = @import("antfly_tile");
const f = tile.fexpr;
const expr = tile.native_math.cudaExpression;
pub const source_0 = program.render(
    \\template <unsigned int ROWS_PER_BLOCK, unsigned int COLS, unsigned int MODE>
    \\__device__ void termite_linear_bias_f32_tile_rows_cols(
    \\    float* dst,
    \\    const float* input,
    \\    const float* weight,
    \\    const float* bias,
    \\    const float* residual,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    unsigned int col_tile = blockIdx.x * COLS;
    \\    unsigned int row_base = blockIdx.y * ROWS_PER_BLOCK;
    \\    unsigned int tid = threadIdx.x;
    \\    __shared__ float partial[ROWS_PER_BLOCK][COLS][256];
    \\    float acc[ROWS_PER_BLOCK][COLS];
    \\#include "../kernels/projection_rows_cols_init.cuh"
    \\
    \\    for (unsigned int k = tid; k < in_dim; k += 256u) {
    \\        float x[ROWS_PER_BLOCK];
    \\        #pragma unroll
    \\        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
    \\            unsigned int row = row_base + r;
    \\            x[r] = row < rows ? input[row * in_dim + k] : 0.0f;
    \\        }
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < COLS; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                float w = weight[col * in_dim + k];
    \\                #pragma unroll
    \\                for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
    \\                    acc[r][c] += x[r] * w;
    \\                }
    \\            }
    \\        }
    \\    }
    \\
    \\    #pragma unroll
    \\    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] = acc[r][c];
    \\    }
    \\#include "../kernels/projection_rows_cols_tree.cuh"
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
    \\            unsigned int row = row_base + r;
    \\            if (row >= rows) continue;
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < COLS; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                if (col >= out_dim) continue;
    \\                float y = {{bias}};
    \\                if (MODE == 1u && y < 0.0f) y = 0.0f;
    \\                if (MODE == 2u) y = {{gelu}};
    \\                unsigned int idx = row * out_dim + col;
    \\                if (MODE == 3u) y += residual[idx];
    \\                dst[idx] = y;
    \\            }
    \\        }
    \\    }
    \\}
    \\
, .{
    .bias = expr(f.acc.plus(.bias), "partial[r][c][0]", "bias[col]", "", ""),
    .gelu = expr(tile.native_math.activation(.gelu, .left_associated).expr, "y", "", "", ""),
});
