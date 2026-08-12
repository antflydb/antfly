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
// plan_id=cuda/q4_0/rows_1/pair_activation/mmv
// kernel_id=antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1
// production_baseline=termite_linear_q4_0_pair_activation_f32_tile4_w4
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

extern "C" __global__ void antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1(
    float* dst,
    const float* input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    const unsigned int tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / tiles;
    const unsigned int col_tile = (blockIdx.x - row * tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int row_blocks = in_dim / 32u;
    if (rows != 1u || in_dim != 1536u || out_dim != 12288u || blockDim.x != 128u || row >= rows) return;

    __shared__ float gate_partial[4][4];
    __shared__ float up_partial[4][4];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

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
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += x * termite_q4_0_value_nibble(gate_bp, q_offset, high_nibble);
                up_acc[c] += x * termite_q4_0_value_nibble(up_bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        const float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        const float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 4u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 4u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}
