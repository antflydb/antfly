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

// native-program:entry termite_q4_0_q8_1_rows_impl
template<unsigned int Rows>
__device__ __forceinline__ void termite_q4_0_q8_1_rows_impl(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    static_assert(Rows == 4u || Rows == 8u, "the two-row operand schedule is independently specialized");
    const unsigned int cols = 4u;
    const unsigned int rows_per_block = Rows;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row_base = blockIdx.y * rows_per_block;
    if (row_base >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[Rows][4][8];
    bool full_tile = col_tile + 3u < out_dim;
    float acc[Rows][4];
{{q4q8_rows_linear}}

}

