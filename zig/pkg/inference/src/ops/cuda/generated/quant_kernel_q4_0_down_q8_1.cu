// Copyright 2026 Antfly, Inc.
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

// Promoted generated kernel from graph/quant_kernel_compiler.zig.
// plan_id=cuda/q4_0/rows_1/gated_down/mmv
// kernel_id=antfly_q4_0_down_q8_1_mmv_v1
// production_baseline=termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down
// production_enabled=true
// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8=1.

#include <cuda_fp16.h>

static __device__ __forceinline__ float antfly_half_bits_to_float(unsigned short bits) {
    return __half2float(__ushort_as_half(bits));
}

static __device__ __forceinline__ float antfly_warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

static __device__ __forceinline__ unsigned int antfly_q4_0_word_u16(const unsigned char *payload) {
    const unsigned short *halves = (const unsigned short *)payload;
    return (unsigned int)halves[0] | ((unsigned int)halves[1] << 16);
}

static __device__ __forceinline__ float antfly_q4_0_q8_dot16(
    const unsigned char *q4_bp,
    float q8_d,
    unsigned int iqs,
    int q8_low0,
    int q8_high0,
    int q8_low1,
    int q8_high1
) {
    const float q4_d = antfly_half_bits_to_float(((const unsigned short *)q4_bp)[0]);
    const unsigned int base0 = iqs * 4u;
    const unsigned int word0 = antfly_q4_0_word_u16(q4_bp + 2u + base0);
    const unsigned int word1 = antfly_q4_0_word_u16(q4_bp + 2u + base0 + 4u);
    const unsigned int low0 = __vadd4(word0 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int high0 = __vadd4((word0 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int low1 = __vadd4(word1 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int high1 = __vadd4((word1 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    int sumi = __dp4a((int)low0, q8_low0, 0);
    sumi = __dp4a((int)high0, q8_high0, sumi);
    sumi = __dp4a((int)low1, q8_low1, sumi);
    sumi = __dp4a((int)high1, q8_high1, sumi);
    return q4_d * q8_d * (float)sumi;
}

extern "C" __global__ void antfly_q4_0_down_q8_1_mmv_v1(
    float *dst,
    const unsigned char *q8_input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / col_tiles;
    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    if (blockDim.x != 256u || row >= rows) return;

    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += 128u) {
        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int *)(q8_values + q8_base0);
        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int *)(q8_values + q8_base1);
        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < 4u) {
        float y = 0.0f;
        #pragma unroll
        for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[tid][w];
        const unsigned int col = col_tile + tid;
        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    }
}
