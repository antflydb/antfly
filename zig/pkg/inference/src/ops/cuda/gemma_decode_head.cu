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

// Rebuild with CUDA 13.0: nvcc -ptx -arch=compute_75 gemma_decode_head.cu.
// Preserve the license and refresh the source-sha256 header in the PTX.
#include <cuda_fp16.h>

__device__ __forceinline__ float termite_half_to_float(unsigned short h) {
    return __half2float(__ushort_as_half(h));
}

__device__ __forceinline__ float termite_warp_reduce_sum(float v) {
    v += __shfl_down_sync(0xffffffffu, v, 16);
    v += __shfl_down_sync(0xffffffffu, v, 8);
    v += __shfl_down_sync(0xffffffffu, v, 4);
    v += __shfl_down_sync(0xffffffffu, v, 2);
    v += __shfl_down_sync(0xffffffffu, v, 1);
    return v;
}

__device__ __forceinline__ int termite_pack_i8x4(const signed char* values) {
    unsigned int packed = (unsigned int)(unsigned char)values[0];
    packed |= ((unsigned int)(unsigned char)values[1]) << 8;
    packed |= ((unsigned int)(unsigned char)values[2]) << 16;
    packed |= ((unsigned int)(unsigned char)values[3]) << 24;
    return (int)packed;
}

__device__ __forceinline__ int termite_load_i8x4_aligned(const signed char* values) {
    return *((const int*)values);
}

__device__ __forceinline__ int termite_q6k_quant_i32(const unsigned char* bp, unsigned int value_index) {
    const unsigned char* ql = bp;
    const unsigned char* qh = bp + 128u;

    unsigned int sub = value_index / 16u;
    unsigned int i = value_index & 15u;
    unsigned int half = sub / 8u;
    unsigned int group = (sub % 8u) / 2u;
    unsigned int l_base = (sub & 1u) * 16u;
    unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    unsigned int qh_off = half * 32u;
    unsigned int qh_shift = group * 2u;
    unsigned int nibble_shift = (group / 2u) * 4u;
    unsigned int l = l_base + i;

    unsigned int low4 = (unsigned int)((ql[ql_off + l] >> nibble_shift) & 0x0fu);
    unsigned int high2 = (unsigned int)((qh[qh_off + l] >> qh_shift) & 0x03u);
    return (int)(low4 | (high2 << 4u)) - 32;
}

__device__ __forceinline__ float termite_q6k_sub_scale_f32(const unsigned char* bp, unsigned int sub) {
    const unsigned char* scales = bp + 192u;
    unsigned short dh = (unsigned short)bp[208] | ((unsigned short)bp[209] << 8);
    float d = termite_half_to_float(dh);
    return d * (float)((signed char)scales[sub]);
}

__device__ __forceinline__ int termite_pack_q6k_i8x4_sub(const unsigned char* bp, unsigned int sub, unsigned int sub_offset) {
    signed char values[4];
    unsigned int base_value_index = sub * 16u + sub_offset;
    #pragma unroll
    for (unsigned int i = 0u; i < 4u; ++i) {
        values[i] = (signed char)termite_q6k_quant_i32(bp, base_value_index + i);
    }
    return termite_pack_i8x4(values);
}

// Read/dequantize each weight tile once for two independent activation rows.
// Every row preserves the singleton DP4A product and warp-reduction order.
extern "C" __global__ void antfly_e4b_q6_q8_pair(
    float* dst, const unsigned char* input, const unsigned char* weight,
    unsigned int out_dim
) {
    const unsigned int col_tile = blockIdx.x * 8u;
    if (col_tile >= out_dim) return;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float partial[2][8][5];
    float acc[2][8];
    #pragma unroll
    for (unsigned int r = 0; r < 2u; ++r)
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) acc[r][c] = 0.0f;
    if (tid < 160u) {
        const unsigned int block = tid >> 4u;
        const unsigned int sub = tid & 15u;
        const unsigned int q8_block = sub >> 1u;
        const unsigned int q8_lane = (sub & 1u) * 16u;
        float d[2];
        int q[2][4];
        #pragma unroll
        for (unsigned int r = 0; r < 2u; ++r) {
            const unsigned char* p = input + r * 2880u + (block * 8u + q8_block) * 36u;
            d[r] = termite_half_to_float((unsigned short)p[0] | ((unsigned short)p[1] << 8));
            #pragma unroll
            for (unsigned int j = 0; j < 4u; ++j)
                q[r][j] = termite_load_i8x4_aligned((const signed char*)(p + 4u + q8_lane + j * 4u));
        }
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            const unsigned char* bp = weight + ((col_tile + c) * 10u + block) * 210u;
            int w[4];
            #pragma unroll
            for (unsigned int j = 0; j < 4u; ++j) w[j] = termite_pack_q6k_i8x4_sub(bp, sub, j * 4u);
            const float scale = termite_q6k_sub_scale_f32(bp, sub);
            #pragma unroll
            for (unsigned int r = 0; r < 2u; ++r) {
                int sum = 0;
                #pragma unroll
                for (unsigned int j = 0; j < 4u; ++j) sum = __dp4a(w[j], q[r][j], sum);
                acc[r][c] = (d[r] * scale) * (float)sum;
            }
        }
    }
    #pragma unroll
    for (unsigned int r = 0; r < 2u; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            const float sum = termite_warp_reduce_sum(acc[r][c]);
            if (lane == 0u) partial[r][c][warp] = sum;
        }
    }
    __syncthreads();
    if (tid < 8u) {
        #pragma unroll
        for (unsigned int r = 0; r < 2u; ++r)
            dst[r * out_dim + col_tile + tid] = partial[r][tid][0] + partial[r][tid][1] + partial[r][tid][2] + partial[r][tid][3] + partial[r][tid][4];
    }
}
