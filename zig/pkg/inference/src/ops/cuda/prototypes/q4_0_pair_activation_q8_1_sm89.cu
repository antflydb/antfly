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

// Standalone, default-off SM89 experiment. This translation unit is not part
// of the canonical CUDA bundle and all exported symbols are prototype-only.
//
// The reference E2B kernel assigns three warps to every four output columns.
// Those warps own input blocks [0,16), [16,32), and [32,48), respectively;
// lane pairs own the low/high 16-value halves of one Q4_0/Q8_1 block. Exact
// output bytes depend on both that warp reduction tree and the final ordered
// sum of the three warp partials. Every candidate below preserves that tree.

#include <cuda_fp16.h>

namespace antfly_q4_pair_prototype {

__device__ __forceinline__ float half_bits_to_float(unsigned short bits) {
    return __half2float(__ushort_as_half(bits));
}

__device__ __forceinline__ float warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float warp_reduce_max_f32(float value) {
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 16));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 8));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 4));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 2));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 1));
    return __shfl_sync(0xffffffffu, value, 0);
}

__device__ __forceinline__ float decoder_activation_f32(
    float x,
    unsigned int activation
) {
    if (activation <= 1u) {
        const float inner = 0.7978845608028654f *
            (x + 0.044715f * x * x * x);
        return 0.5f * x * (1.0f + tanhf(inner));
    }
    if (activation == 2u) return x / (1.0f + __expf(-x));
    if (activation == 3u) return fmaxf(x, 0.0f);
    if (activation == 4u) return x / (1.0f + __expf(-1.702f * x));
    const float r = fmaxf(x, 0.0f);
    return r * r;
}

// Q4_0 blocks are 18-byte records. The payload is only two-byte aligned, so
// assembling each word from aligned u16 loads avoids undefined misaligned u32
// accesses while retaining the production load contract.
__device__ __forceinline__ unsigned int q4_0_word_u16(
    const unsigned char* payload
) {
    const unsigned short* halves =
        reinterpret_cast<const unsigned short*>(payload);
    return static_cast<unsigned int>(halves[0]) |
        (static_cast<unsigned int>(halves[1]) << 16);
}

__device__ __forceinline__ float q4_0_q8_dot16_with_scale(
    const unsigned char* q4_bp,
    float q4_d,
    float q8_d,
    unsigned int iqs,
    int q8_low0,
    int q8_high0,
    int q8_low1,
    int q8_high1
) {
    const unsigned int base0 = iqs * 4u;
    const unsigned int word0 = q4_0_word_u16(q4_bp + 2u + base0);
    const unsigned int word1 = q4_0_word_u16(q4_bp + 2u + base0 + 4u);
    const unsigned int low0 =
        __vadd4(word0 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int high0 =
        __vadd4((word0 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int low1 =
        __vadd4(word1 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int high1 =
        __vadd4((word1 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    int sumi = __dp4a(static_cast<int>(low0), q8_low0, 0);
    sumi = __dp4a(static_cast<int>(high0), q8_high0, sumi);
    sumi = __dp4a(static_cast<int>(low1), q8_low1, sumi);
    sumi = __dp4a(static_cast<int>(high1), q8_high1, sumi);
    // Keep the production multiply association exactly.
    return q4_d * q8_d * static_cast<float>(sumi);
}

template <bool BroadcastScale>
__device__ __forceinline__ float load_q4_scale(
    const unsigned char* q4_bp,
    unsigned int lane
) {
    if constexpr (BroadcastScale) {
        unsigned int bits = (lane & 1u) == 0u
            ? static_cast<unsigned int>(
                reinterpret_cast<const unsigned short*>(q4_bp)[0])
            : 0u;
        bits = __shfl_sync(0xffffffffu, bits, lane & ~1u);
        return half_bits_to_float(static_cast<unsigned short>(bits));
    }
    return half_bits_to_float(
        reinterpret_cast<const unsigned short*>(q4_bp)[0]);
}

template <unsigned int OutDim, unsigned int GroupCols,
          unsigned int GroupsPerWave, unsigned int Waves,
          bool BroadcastScale>
__device__ __forceinline__ void pair_activation(
    unsigned char* dst_q8,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int activation,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    constexpr unsigned int kInDim = 1536u;
    constexpr unsigned int kRowBlocks = kInDim / 32u;
    constexpr unsigned int kOutRowBlocks = OutDim / 32u;
    constexpr unsigned int kGroupWarps = 3u;
    constexpr unsigned int kThreads =
        GroupsPerWave * kGroupWarps * 32u;
    static_assert(GroupCols * GroupsPerWave * Waves == 32u,
                  "retile must produce one Q8_1 block");
    static_assert(GroupsPerWave <= 4u,
                  "shared partial storage is bounded to four groups");
    static_assert(kThreads <= 1024u, "invalid CUDA CTA size");

    if (rows != 1u || in_dim != kInDim || out_dim != OutDim ||
        blockDim.x != kThreads) return;
    const unsigned int out_block = blockIdx.x;
    if (out_block >= kOutRowBlocks) return;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / kGroupWarps;
    const unsigned int group_warp = warp - group * kGroupWarps;

    __shared__ float gate_partial[GroupsPerWave][GroupCols][kGroupWarps];
    __shared__ float up_partial[GroupsPerWave][GroupCols][kGroupWarps];
    __shared__ float activated[32];

#pragma unroll
    for (unsigned int wave = 0u; wave < Waves; ++wave) {
        const unsigned int local_tid = group_warp * 32u + lane;
        const unsigned int col_tile = col_block +
            (wave * GroupsPerWave + group) * GroupCols;
        float gate_acc[GroupCols];
        float up_acc[GroupCols];
#pragma unroll
        for (unsigned int c = 0u; c < GroupCols; ++c) {
            gate_acc[c] = 0.0f;
            up_acc[c] = 0.0f;
        }

        // The fixed E2B shape gives each lane exactly one 16-value dot. This
        // is equivalent to the production loop, but removes its dynamic exit
        // test and makes the specialization explicit in the binary.
        const unsigned int block = local_tid >> 1u;
        const unsigned int iqs = (local_tid & 1u) * 2u;
        const unsigned char* q8_bp = q8_input + block * 36u;
        const float q8_d = half_bits_to_float(
            reinterpret_cast<const unsigned short*>(q8_bp)[0]);
        const signed char* q8_values =
            reinterpret_cast<const signed char*>(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *reinterpret_cast<const int*>(
            q8_values + q8_base0);
        const int q8_high0 = *reinterpret_cast<const int*>(
            q8_values + q8_base0 + 16u);
        const int q8_low1 = *reinterpret_cast<const int*>(
            q8_values + q8_base1);
        const int q8_high1 = *reinterpret_cast<const int*>(
            q8_values + q8_base1 + 16u);

#pragma unroll
        for (unsigned int c = 0u; c < GroupCols; ++c) {
            const unsigned int col = col_tile + c;
            const unsigned char* gate_bp = weight_gate +
                (static_cast<size_t>(col) * kRowBlocks + block) * 18u;
            const unsigned char* up_bp = weight_up +
                (static_cast<size_t>(col) * kRowBlocks + block) * 18u;
            const float gate_d = load_q4_scale<BroadcastScale>(gate_bp, lane);
            const float up_d = load_q4_scale<BroadcastScale>(up_bp, lane);
            gate_acc[c] += q4_0_q8_dot16_with_scale(
                gate_bp, gate_d, q8_d, iqs, q8_low0, q8_high0,
                q8_low1, q8_high1);
            up_acc[c] += q4_0_q8_dot16_with_scale(
                up_bp, up_d, q8_d, iqs, q8_low0, q8_high0,
                q8_low1, q8_high1);
        }

#pragma unroll
        for (unsigned int c = 0u; c < GroupCols; ++c) {
            const float gate_sum = warp_reduce_sum_f32(gate_acc[c]);
            const float up_sum = warp_reduce_sum_f32(up_acc[c]);
            if (lane == 0u) {
                gate_partial[group][c][group_warp] = gate_sum;
                up_partial[group][c][group_warp] = up_sum;
            }
        }
        __syncthreads();
        if (tid < GroupsPerWave * GroupCols) {
            const unsigned int out_group = tid / GroupCols;
            const unsigned int c = tid - out_group * GroupCols;
            float gate_y = 0.0f;
            float up_y = 0.0f;
#pragma unroll
            for (unsigned int w = 0u; w < kGroupWarps; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * GroupsPerWave * GroupCols +
                out_group * GroupCols + c] =
                decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float amax = warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char* bp = dst_q8 +
            static_cast<size_t>(out_block) * 36u;
        bp[4u + lane] = static_cast<unsigned char>(
            static_cast<signed char>(q));
        if (lane == 0u) {
            const unsigned short d_bits =
                __half_as_ushort(__float2half(d));
            bp[0] = static_cast<unsigned char>(d_bits & 0xffu);
            bp[1] = static_cast<unsigned char>(d_bits >> 8);
            bp[2] = 0u;
            bp[3] = 0u;
        }
    }
}

}  // namespace antfly_q4_pair_prototype

#define ANTFLY_DEFINE_PAIR_KERNEL(SYMBOL, OUT_DIM, GROUP_COLS, GROUPS, WAVES, BROADCAST) \
extern "C" __global__ void SYMBOL(                                                   \
    unsigned char* dst_q8, const unsigned char* q8_input,                            \
    const unsigned char* weight_gate, const unsigned char* weight_up,                \
    unsigned int activation, unsigned int rows, unsigned int in_dim,                 \
    unsigned int out_dim                                                              \
) {                                                                                   \
    antfly_q4_pair_prototype::pair_activation<                                        \
        OUT_DIM, GROUP_COLS, GROUPS, WAVES, BROADCAST>(                               \
        dst_q8, q8_input, weight_gate, weight_up, activation, rows,                   \
        in_dim, out_dim);                                                              \
}

// Fixed-shape control: production 4-column/384-thread reduction topology.
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_6144_c4_t384_fixed_prototype,
    6144u, 4u, 4u, 2u, false)
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_12288_c4_t384_fixed_prototype,
    12288u, 4u, 4u, 2u, false)

// Same topology with one scale load per lane pair. This preserves the exact
// scale bits, but trades a global-load instruction for a warp shuffle.
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_6144_c4_t384_scale_broadcast_prototype,
    6144u, 4u, 4u, 2u, true)
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_12288_c4_t384_scale_broadcast_prototype,
    12288u, 4u, 4u, 2u, true)

// Occupancy retile: two three-warp groups per CTA, four waves per Q8 block.
// This keeps eight accumulator registers rather than the C8 variant's 16.
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_6144_c4_t192_fixed_prototype,
    6144u, 4u, 2u, 4u, false)
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_12288_c4_t192_fixed_prototype,
    12288u, 4u, 2u, 4u, false)

// ILP/Q8-reuse retile: four groups produce all 32 values in one wave.
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_6144_c8_t384_fixed_prototype,
    6144u, 8u, 4u, 1u, false)
ANTFLY_DEFINE_PAIR_KERNEL(
    antfly_q4_0_pair_activation_q8_1_e2b_12288_c8_t384_fixed_prototype,
    12288u, 8u, 4u, 1u, false)

#undef ANTFLY_DEFINE_PAIR_KERNEL
