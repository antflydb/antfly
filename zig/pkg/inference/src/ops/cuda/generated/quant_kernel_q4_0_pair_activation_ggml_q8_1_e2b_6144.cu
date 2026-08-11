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
// kernel_id=antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1
// production_baseline=termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn
// production_enabled=false
// SM89-only diagnostic candidates using llama.cpp CUDA's
// block_q8_1 half2(d,raw_sum) activation contract.
// Q8_1 contract. Production is default-off pending differential parity
// and rotating-buffer end-to-end throughput evidence.

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

static __device__ __forceinline__ float antfly_warp_reduce_max_f32(float value) {
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 16));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 8));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 4));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 2));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 1));
    return __shfl_sync(0xffffffffu, value, 0);
}

static __device__ __forceinline__ float antfly_decoder_activation_f32(float x, unsigned int activation) {
    if (activation <= 1u) {
        const float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x);
        return 0.5f * x * (1.0f + tanhf(inner));
    }
    if (activation == 2u) return x / (1.0f + __expf(-x));
    if (activation == 3u) return fmaxf(x, 0.0f);
    if (activation == 4u) return x / (1.0f + __expf(-1.702f * x));
    const float r = fmaxf(x, 0.0f);
    return r * r;
}

// q4_0 payload bytes live at bp+2; bp is always 2-byte aligned (18-byte
// blocks), so every 4-byte word can be assembled from two aligned u16 loads.
static __device__ __forceinline__ unsigned int antfly_q4_0_word_u16(const unsigned char *payload) {
    const unsigned short *halves = (const unsigned short *)payload;
    return (unsigned int)halves[0] | ((unsigned int)halves[1] << 16);
}

// One of two 16-value contributions for a Q4_0/Q8_1 block. Keeping Q4
// nibbles unsigned removes four packed-byte centering instructions; each
// contribution applies half of the block-wide -8*sum correction.
static __device__ __forceinline__ float antfly_q4_0_ggml_q8_1_dot16(
    const unsigned char *q4_bp,
    float q8_d,
    float q8_sum,
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
    const unsigned int low0 = word0 & 0x0f0f0f0fu;
    const unsigned int high0 = (word0 >> 4) & 0x0f0f0f0fu;
    const unsigned int low1 = word1 & 0x0f0f0f0fu;
    const unsigned int high1 = (word1 >> 4) & 0x0f0f0f0fu;
    int sumi = __dp4a((int)low0, q8_low0, 0);
    sumi = __dp4a((int)high0, q8_high0, sumi);
    sumi = __dp4a((int)low1, q8_low1, sumi);
    sumi = __dp4a((int)high1, q8_high1, sumi);
    return q4_d * (q8_d * (float)sumi - 4.0f * q8_sum);
}

extern "C" __global__ void antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1(
    unsigned char *dst_q8,
    const unsigned char *q8_input,
    const unsigned char *weight_gate,
    const unsigned char *weight_up,
    unsigned int activation,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int out_row_blocks = out_dim >> 5;
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;

    const unsigned int out_block = blockIdx.x % out_row_blocks;
    const unsigned int row = blockIdx.x / out_row_blocks;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / 3u;
    const unsigned int group_warp = warp - group * 3u;
    if (blockDim.x != 384u || row >= rows) return;

    __shared__ float gate_partial[4][4][3];
    __shared__ float up_partial[4][4][3];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            const unsigned int local_tid = group_warp * 32u + lane;
            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            const unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 48u) {
                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
                const float q8_sum = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[1]);
                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
                const unsigned int q8_base0 = iqs * 4u;
                const unsigned int q8_base1 = q8_base0 + 4u;
                const int q8_low0 = *(const int *)(q8_values + q8_base0);
                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
                const int q8_low1 = *(const int *)(q8_values + q8_base1);
                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    const unsigned int col = col_tile + c;
                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                    gate_acc[c] += antfly_q4_0_ggml_q8_1_dot16(gate_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += antfly_q4_0_ggml_q8_1_dot16(up_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            const unsigned int out_group = tid >> 2u;
            const unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < 3u; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float sum = antfly_warp_reduce_sum_f32(x);
        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            const unsigned short d_bits = __half_as_ushort(__float2half(d));
            bp[0] = (unsigned char)(d_bits & 0xffu);
            bp[1] = (unsigned char)(d_bits >> 8);
            const unsigned short sum_bits = __half_as_ushort(__float2half(sum));
            bp[2] = (unsigned char)(sum_bits & 0xffu);
            bp[3] = (unsigned char)(sum_bits >> 8);
        }
    }
}
