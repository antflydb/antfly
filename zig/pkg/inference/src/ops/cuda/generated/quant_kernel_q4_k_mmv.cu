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
// plan_id=cuda/q4_k/rows_1/none/mmv
// kernel_id=antfly_q4_k_mmv_f32_v1
// production_baseline=termite_linear_q4_k_f32_tile4
// production_enabled=false
// Decode candidate for Q4_K-backed Gemma 4 projections. It remains dev-only
// until target-specific microbench and full-model promotion gates pass.

#include <cuda_fp16.h>
#include <stdint.h>

struct antfly_q4_k_block_view {
    const uint8_t *d;
    const uint8_t *dmin;
    const uint8_t *scales;
    const uint8_t *qs;
};

static __device__ __forceinline__ float antfly_half_le_to_float(const uint8_t *p) {
    const uint16_t bits = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
    return __half2float(__ushort_as_half(bits));
}

static __device__ __forceinline__ void antfly_q4_k_unpack_scale_min(
    const uint8_t *scales,
    int sub,
    float *scale,
    float *min_v
) {
    if (sub < 4) {
        *scale = (float)(scales[sub] & 63u);
        *min_v = (float)(scales[sub + 4] & 63u);
        return;
    }

    *scale = (float)((scales[sub + 4] & 0x0fu) | ((scales[sub - 4] >> 6) << 4));
    *min_v = (float)((scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4));
}

static __device__ __forceinline__ float antfly_q4_k_dequant_lane(const uint8_t *block, int lane) {
    antfly_q4_k_block_view view = {
        block,
        block + 2,
        block + 4,
        block + 16,
    };
    const int sub = lane >> 5;
    const int q_index = (sub >> 1) * 32 + (lane & 31);
    const uint8_t packed = view.qs[q_index];
    const uint8_t q = (sub & 1) == 0 ? (packed & 0x0fu) : (packed >> 4);
    const float d = antfly_half_le_to_float(view.d);
    const float dmin = antfly_half_le_to_float(view.dmin);
    float raw_scale = 0.0f;
    float raw_min = 0.0f;
    antfly_q4_k_unpack_scale_min(view.scales, sub, &raw_scale, &raw_min);
    return d * raw_scale * (float)q - dmin * raw_min;
}

extern "C" __global__ void antfly_q4_k_mmv_f32_v1(
    float *output,
    const float *input,
    const uint8_t *weight_q4_k,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int col = blockIdx.x;
    const unsigned int tid = threadIdx.x;
    if (rows != 1u || col >= out_dim || blockDim.x != 256u || (in_dim & 255u) != 0u) return;

    float acc = 0.0f;
    const unsigned int block_count = in_dim >> 8u;
    for (unsigned int block_idx = 0u; block_idx < block_count; ++block_idx) {
        const uint8_t *block = weight_q4_k + ((size_t)col * block_count + block_idx) * 144u;
        const unsigned int input_index = (block_idx << 8u) + tid;
        acc += input[input_index] * antfly_q4_k_dequant_lane(block, tid);
    }

    __shared__ float partial[256];
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) output[col] = partial[0];
}
