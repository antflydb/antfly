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
// plan_id=cuda/q4_0/rows_9_64/none/mm
// kernel_id=antfly_q4_0_mm_f32_v1
// production_baseline=termite_linear_q4_0_f32
// production_enabled=true
// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MM=1.

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

extern "C" __global__ void antfly_q4_0_mm_f32_v1(
    const float *input,
    const uint8_t *weight_q4_0,
    float *output,
    int rows,
    int in_dim,
    int out_dim
) {
    const int col0 = blockIdx.x << 2;
    const int row0 = blockIdx.y << 3;
    if (rows < 9 || rows > 64) return;
    if (col0 >= out_dim || row0 >= rows) return;
    if (blockDim.x != 256) return;
    if ((in_dim & 31) != 0) return;

    const int row_blocks = in_dim >> 5;
    const int half_bytes = in_dim >> 1;
    float acc[4][8];
#pragma unroll
    for (int c = 0; c < 4; ++c) {
#pragma unroll
        for (int r = 0; r < 8; ++r) acc[c][r] = 0.0f;
    }

    for (int byte_idx = threadIdx.x; byte_idx < half_bytes; byte_idx += 256) {
        const int block_idx = byte_idx >> 4;
        const int offset = byte_idx & 15;
        const int base = block_idx << 5;
        float x_lo[8];
        float x_hi[8];
#pragma unroll
        for (int r = 0; r < 8; ++r) {
            const int row = row0 + r;
            x_lo[r] = row < rows ? input[(size_t)row * in_dim + base + offset] : 0.0f;
            x_hi[r] = row < rows ? input[(size_t)row * in_dim + base + offset + 16] : 0.0f;
        }
#pragma unroll
        for (int c = 0; c < 4; ++c) {
            if (col0 + c >= out_dim) continue;
            const uint8_t *block = weight_q4_0 + ((size_t)(col0 + c) * row_blocks + block_idx) * 18;
            const float d = antfly_half_le_to_float(block);
            const int packed = (int)block[2 + offset];
            const float w_lo = d * (float)((packed & 15) - 8);
            const float w_hi = d * (float)((packed >> 4) - 8);
#pragma unroll
            for (int r = 0; r < 8; ++r) acc[c][r] += w_lo * x_lo[r] + w_hi * x_hi[r];
        }
    }

    __shared__ float partial[4][8][8];
    const int lane = threadIdx.x & 31;
    const int warp = threadIdx.x >> 5;
#pragma unroll
    for (int c = 0; c < 4; ++c) {
#pragma unroll
        for (int r = 0; r < 8; ++r) {
            const float total = antfly_warp_reduce_sum(acc[c][r]);
            if (lane == 0) partial[c][r][warp] = total;
        }
    }
    __syncthreads();
    if (threadIdx.x < 32) {
        const int c = threadIdx.x >> 3;
        const int r = threadIdx.x & 7;
        float total = 0.0f;
#pragma unroll
        for (int w = 0; w < 8; ++w) total += partial[c][r][w];
        const int col = col0 + c;
        const int row = row0 + r;
        if (col < out_dim && row < rows) output[(size_t)row * out_dim + col] = total;
    }
}
