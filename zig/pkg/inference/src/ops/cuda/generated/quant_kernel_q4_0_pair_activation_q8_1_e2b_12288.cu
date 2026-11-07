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
// kernel_id=antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1
// production_baseline=termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn
// production_enabled=false
// Shape-specific E2B candidate; not bundled or wired until correctness and
// sequential benchmark evidence clear the promotion gate.

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

struct antfly_q8_legacy {
    static __device__ __forceinline__ float input_sum(const unsigned char* bp) { return 0.0f; }
    static __device__ __forceinline__ float output_sum(float x) { return 0.0f; }
    static __device__ __forceinline__ float dot(const unsigned char* bp, float d, float sum, unsigned iqs, int lo0, int hi0, int lo1, int hi1) { return antfly_q4_0_q8_dot16(bp, d, iqs, lo0, hi0, lo1, hi1); }
    static __device__ __forceinline__ void store_sum(unsigned char* bp, float sum) { bp[2] = 0u; bp[3] = 0u; }
};

#include "../../../graph/templates/cuda_q4_q8_projection.cuh"


extern "C" __global__ void antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1(ANTFLY_Q4_Q8_PAIR_PARAMS) {
    antfly_q4_q8_pair<3u, antfly_q8_legacy>(ANTFLY_Q4_Q8_PAIR_ARGS);
}
