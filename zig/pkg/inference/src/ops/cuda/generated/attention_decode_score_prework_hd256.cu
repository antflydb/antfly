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

// Production generated attention kernel from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/decode_1x/hd256/gqa16/score-prework/max4096/consumers-serial+tiled64-max512/f32/device_scalars
// source_id=antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v2
// kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1
// serial_kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1
// tiled64_kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1
// reduction_kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1
// production_baseline=termite_gqa_attention_decode_turboquant_fast_f32
// production_enabled=true
// Default-on automatic SM89 selection; rollback: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK=0.
// Tiled consumer opt-in: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER=tiled64.

#include <math.h>
#include <cuda_fp16.h>

__device__ __forceinline__ float termite_warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float termite_block_reduce_sum_f32(float value, float* warp_sums) {
    unsigned int lane = threadIdx.x & 31u;
    unsigned int warp = threadIdx.x >> 5;
    unsigned int warp_count = (blockDim.x + 31u) >> 5;
    value = termite_warp_reduce_sum_f32(value);
    if (lane == 0u) warp_sums[warp] = value;
    __syncthreads();
    value = (warp == 0u && lane < warp_count) ? warp_sums[lane] : 0.0f;
    if (warp == 0u) value = termite_warp_reduce_sum_f32(value);
    return value;
}

__device__ __forceinline__ float termite_tq_decode_polar4_scalar(unsigned char code) {
    return ((float)(code & 0x0fu) / 7.5f) - 1.0f;
}

__device__ __forceinline__ float termite_tq_decode_polar4_at(const unsigned char* encoded, unsigned int value_index) {
    unsigned char packed = encoded[value_index >> 1];
    unsigned char code = (value_index & 1u) == 0u ? (packed & 0x0fu) : ((packed >> 4) & 0x0fu);
    return termite_tq_decode_polar4_scalar(code);
}

__device__ __forceinline__ unsigned int termite_tq_physical_token(
    unsigned int logical_token,
    const unsigned int* block_table,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int physical_token_capacity
) {
    unsigned long long physical = (unsigned long long)logical_token;
    if (block_table != 0 && block_count != 0u && page_size_tokens != 0u) {
        unsigned int block_index = logical_token / page_size_tokens;
        if (block_index >= block_count) return 0xffffffffu;
        unsigned int token_offset = logical_token - block_index * page_size_tokens;
        const unsigned long long physical_block = (unsigned long long)block_table[block_index];
        physical = physical_block * (unsigned long long)page_size_tokens +
            (unsigned long long)token_offset;
    }
    if (physical > 0xffffffffull ||
        physical >= (unsigned long long)physical_token_capacity) return 0xffffffffu;
    return (unsigned int)physical;
}

__device__ __forceinline__ float termite_tq_f16_value(const unsigned char* row, unsigned int value_index) {
    const unsigned char* src = row + value_index * 2u;
    unsigned short raw = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    return __half2float(__ushort_as_half(raw));
}

#include "../../../graph/templates/cuda_gqa_score_prework_f32.cuh"
extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1(ANTFLY_GQA_SCORE_PARAMS) {
    antfly_gqa_score_prework<256u, 256u, 16u, 32u, 128u, 4096u>(ANTFLY_GQA_SCORE_ARGS);
}
extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1(ANTFLY_GQA_SCORE_CONSUMER_PARAMS) {
    antfly_gqa_score_consumer<256u, 256u, 16u, 32u, 4096u, false>(ANTFLY_GQA_SCORE_CONSUMER_ARGS);
}
extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1(ANTFLY_GQA_SCORE_CONSUMER_PARAMS) {
    antfly_gqa_score_consumer<256u, 64u, 16u, 32u, 512u, true>(ANTFLY_GQA_SCORE_CONSUMER_ARGS);
}
