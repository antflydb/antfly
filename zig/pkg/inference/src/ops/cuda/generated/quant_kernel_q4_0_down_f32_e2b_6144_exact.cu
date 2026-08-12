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
// plan_id=cuda/q4_0/rows_1/gated_down/mmv
// kernel_id=antfly_q4_0_down_f32_e2b_6144_exact_v1
// production_baseline=termite_linear_q4_0_f32_tile4
// production_enabled=false
// Runtime-wired exact F32 E2B FFN candidate. It preserves the handwritten
// activation buffer and reduction order; promotion still requires GPU parity
// and full-model speed evidence.

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

__device__ __forceinline__ float termite_q4_0_value_nibble(
    const unsigned char* bp,
    unsigned int q_offset,
    unsigned int high_nibble
) {
    unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    float d = termite_half_to_float(h);
    unsigned char packed = bp[q_offset];
    int q = high_nibble != 0u ? (int)(packed >> 4) : (int)(packed & 0x0fu);
    return (float)(q - 8) * d;
}

__device__ __forceinline__ float termite_decoder_activation_f32(float x, unsigned int activation) {
    if (activation == 0u) {
        return 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    } else if (activation == 1u) {
        return 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    } else if (activation == 2u) {
        return x / (1.0f + expf(-x));
    } else if (activation == 3u) {
        return fmaxf(x, 0.0f);
    } else if (activation == 4u) {
        return x / (1.0f + expf(-1.702f * x));
    }
    float r = fmaxf(x, 0.0f);
    return r * r;
}

extern "C" __global__ void antfly_q4_0_down_f32_e2b_6144_exact_v1(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    const unsigned int col_tile = blockIdx.x * cols;
    const unsigned int row = 0u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int row_blocks = in_dim / 32u;
    if (rows != 1u || in_dim != 6144u || out_dim != 1536u || blockDim.x != 256u) return;

    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        const float x = input[row * in_dim + i];
        const unsigned int block = i / 32u;
        const unsigned int value_lane = i - block * 32u;
        const unsigned int q_offset = 2u + (value_lane & 15u);
        const unsigned int high_nibble = value_lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        const float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 8u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[c][w];
                dst[row * out_dim + col] = y;
            }
        }
    }
}
