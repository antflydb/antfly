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
// plan_id=cuda/q4_0/rows_1/none/mmv
// kernel_id=antfly_q4_0_mmv_f32_v1
// production_baseline=termite_linear_q4_0_f32_tile4
// production_enabled=true
// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV=1.

#include <cuda_fp16.h>
#include <stdint.h>

static __device__ __forceinline__ float antfly_half_le_to_float(const uint8_t *p) {
    const uint16_t bits = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
    return __half2float(__ushort_as_half(bits));
}

static __device__ __forceinline__ float antfly_warp_reduce_sum(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

extern "C" __global__ void antfly_q4_0_mmv_f32_v1(
    const float *input,
    const uint8_t *weight_q4_0,
    float *output,
    int rows,
    int in_dim,
    int out_dim
) {
    const int col0 = blockIdx.x << 2;
    if (rows != 1 || col0 >= out_dim) return;
    if (blockDim.x != 256) return;
    if ((in_dim & 31) != 0) return;

    const int row_blocks = in_dim >> 5;
    const int half_bytes = in_dim >> 1;
    float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    for (int byte_idx = threadIdx.x; byte_idx < half_bytes; byte_idx += 256) {
        const int block_idx = byte_idx >> 4;
        const int offset = byte_idx & 15;
        const int base = block_idx << 5;
        const float x_lo = input[base + offset];
        const float x_hi = input[base + offset + 16];
#pragma unroll
        for (int c = 0; c < 4; ++c) {
            if (col0 + c >= out_dim) continue;
            const uint8_t *block = weight_q4_0 + ((size_t)(col0 + c) * row_blocks + block_idx) * 18;
            const float d = antfly_half_le_to_float(block);
            const int packed = (int)block[2 + offset];
            acc[c] += d * (x_lo * (float)((packed & 15) - 8) + x_hi * (float)((packed >> 4) - 8));
        }
    }

    __shared__ float partial[4][8];
    const int lane = threadIdx.x & 31;
    const int warp = threadIdx.x >> 5;
#pragma unroll
    for (int c = 0; c < 4; ++c) {
        const float total = antfly_warp_reduce_sum(acc[c]);
        if (lane == 0) partial[c][warp] = total;
    }
    __syncthreads();
    if (threadIdx.x < 4) {
        float total = 0.0f;
#pragma unroll
        for (int w = 0; w < 8; ++w) total += partial[threadIdx.x][w];
        if (col0 + threadIdx.x < out_dim) output[col0 + threadIdx.x] = total;
    }
}
