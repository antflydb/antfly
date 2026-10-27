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

// Dev-only generated attention kernel from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/decode_1x/hd512/gqa16/split2/min512/f32/device_scalars
// source_id=antfly_gqa_attention_decode_split2_kv_hd512_f32_v1
// kernel_id=antfly_gqa_attention_decode_split2_kv_hd512_f32_stage1_v1
// serial_kernel_id=antfly_gqa_attention_decode_scalars_split2_hd512_f32_v1
// reduction_kernel_id=antfly_gqa_attention_decode_split2_kv_hd512_f32_stage2_v1
// production_baseline=termite_gqa_attention_decode_scalars_fast_f32
// production_enabled=false
// Runtime opt-in: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=1.

#include <math.h>

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

#include "../../../graph/templates/cuda_gqa_split_decode_f32.cuh"
extern "C" __global__ void antfly_gqa_attention_decode_scalars_split2_hd512_f32_v1(ANTFLY_GQA_SERIAL_PARAMS) {
    antfly_gqa_serial<512u, 512u, 16u, 32u>(ANTFLY_GQA_SERIAL_ARGS);
}


extern "C" __global__ void antfly_gqa_attention_decode_split2_kv_hd512_f32_stage1_v1(ANTFLY_GQA_SPLIT_STAGE_PARAMS) {
    antfly_gqa_split_stage<512u, 512u, 16u, 2u, 32u>(ANTFLY_GQA_SPLIT_STAGE_ARGS);
}
extern "C" __global__ void antfly_gqa_attention_decode_split2_kv_hd512_f32_stage2_v1(ANTFLY_GQA_SPLIT_REDUCE_PARAMS) {
    antfly_gqa_split_reduce<512u, 512u, 2u, 32u>(ANTFLY_GQA_SPLIT_REDUCE_ARGS);
}
