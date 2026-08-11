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

// Dev-only generated kernel candidate from graph/quant_kernel_compiler.zig.
// plan_id=cuda/q4_0/rows_1/argmax/mmv
// kernel_id=antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1
// production_baseline=termite_linear_q4_0_q8_1_f32_tile4+termite_argmax_last_row_f32
// production_enabled=false
// Runtime-wired E2B candidate behind ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX;
// production stays disabled until exact-token and full-chain benchmark gates pass.

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

extern "C" __global__ void antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 8u;
    const unsigned int row_blocks = 48u;
    if (rows != 1u || in_dim != 1536u || out_dim != 262144u || blockDim.x != 96u) return;

    const unsigned int global_tile = blockIdx.x;
    const unsigned int col_tile = global_tile * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float warp_partial[8][3];
    float acc[8];
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    const unsigned int block = tid >> 1u;
    if (block < row_blocks) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int*)(q8_values + q8_base0);
        const int q8_high0 = *(const int*)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int*)(q8_values + q8_base1);
        const int q8_high1 = *(const int*)(q8_values + q8_base1 + 16u);
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            const unsigned char* bp = weight + ((size_t)col * row_blocks + block) * 18u;
            acc[c] = antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid != 0u) return;

    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const unsigned int col = col_tile + c;
        float value = 0.0f;
#pragma unroll
        for (unsigned int w = 0u; w < 3u; ++w) value += warp_partial[c][w];
        bool suppressed = false;
        for (unsigned int j = 0u; j < suppress_count; ++j) {
            const int token_id = suppress_token_ids[j];
            if (token_id >= 0 && (unsigned int)token_id == col) {
                suppressed = true;
                break;
            }
        }
        if (!suppressed && (value > best_value || (value == best_value && col < best_index))) {
            best_value = value;
            best_index = col;
        }
    }
    partial_values[global_tile] = best_value;
    partial_indices[global_tile] = best_index;
}
