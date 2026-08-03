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
// plan_id=cuda/q4_k/rows_2_8/bias_gelu/small_batch
// kernel_id=antfly_q4_k_small_batch_bias_gelu_f32_v1
// production_baseline=termite_linear_q4_k_bias_gelu_f32_tile4_r2
// production_enabled=false
// Not compiled into production artifacts until correctness and benchmark gates
// beat the handwritten CUDA baseline.

#include <cuda_fp16.h>
#include <math.h>
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

static __device__ __forceinline__ float antfly_gelu(float x) {
    if (!isfinite(x)) return 0.0f;
    const float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x);
    if (inner > 10.0f) return x;
    if (inner < -10.0f) return 0.0f;
    const float y = 0.5f * x * (1.0f + tanhf(inner));
    return isfinite(y) ? y : 0.0f;
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

extern "C" __global__ void antfly_q4_k_small_batch_bias_gelu_f32_v1(
    const float *input,
    const uint8_t *weight_q4_k,
    const float *bias,
    float *output,
    int rows,
    int in_dim,
    int out_dim
) {
    const int row = blockIdx.y;
    const int col = blockIdx.x;
    if (row >= rows || rows < 2 || rows > 8 || col >= out_dim) return;
    if (blockDim.x != 128) return;
    if ((in_dim & 255) != 0) return;

    float acc = 0.0f;
    const int block_count = in_dim >> 8;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) {
        const uint8_t *block = weight_q4_k + ((col * block_count + block_idx) * 144);
        const int base = block_idx << 8;
        for (int lane = threadIdx.x; lane < 256; lane += blockDim.x) {
            acc += input[row * in_dim + base + lane] * antfly_q4_k_dequant_lane(block, lane);
        }
    }

    __shared__ float partial[128];
    partial[threadIdx.x] = acc;
    __syncthreads();
    for (int stride = blockDim.x >> 1; stride > 0; stride >>= 1) {
        if (threadIdx.x < stride) partial[threadIdx.x] += partial[threadIdx.x + stride];
        __syncthreads();
    }
    if (threadIdx.x == 0) output[row * out_dim + col] = antfly_gelu(partial[0] + bias[col]);
}
