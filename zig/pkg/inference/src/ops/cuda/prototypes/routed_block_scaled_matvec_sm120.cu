// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cuda_bf16.h>
#include <stdint.h>
#include "../kernels/quantize_bf16_nvfp4.cuh"
#include "../kernels/quantize_bf16_mxfp8.cuh"
#include "../kernels/routed_swiglu.cuh"

namespace {
constexpr unsigned warp_size = 32;
constexpr unsigned output_tile = 16;
constexpr unsigned k_partitions = 4;
constexpr unsigned routes_per_group = 8;
constexpr unsigned routes_per_wide_group = 32;
constexpr unsigned routes_per_large_group = 64;

template<unsigned Routes>
struct ExpertRouteGroup {
    uint32_t expert;
    uint32_t rows[Routes];
};
using RouteGroup = ExpertRouteGroup<routes_per_group>;
using WideRouteGroup = ExpertRouteGroup<routes_per_wide_group>;
using LargeRouteGroup = ExpertRouteGroup<routes_per_large_group>;
static_assert(sizeof(RouteGroup) == 36, "routed projection group ABI");
static_assert(sizeof(WideRouteGroup) == 132, "wide routed projection group ABI");
static_assert(sizeof(LargeRouteGroup) == 260, "large routed projection group ABI");

// Each quad reads 32 adjacent packed bytes from two output rows. A is the
// expert matrix and B repeats the routed activation in the eight columns.
// Each split-K warp publishes just column zero of its 16x8 accumulator.
__device__ void block_dot_nvfp4(
    float (&sum)[4], uint4 weight,
    const uint32_t* input, unsigned quad_lane, uint32_t weight_scale, uint32_t input_scale
) {
    asm("mma.sync.aligned.m16n8k64.row.col.kind::mxf4nvf4.block_scale.scale_vec::4X.f32.e2m1.e2m1.f32.ue4m3 "
        "{%0, %1, %2, %3}, {%4, %5, %6, %7}, {%8, %9}, {%0, %1, %2, %3}, %10, {0, 0}, %11, {0, 0};"
        : "+f"(sum[0]), "+f"(sum[1]), "+f"(sum[2]), "+f"(sum[3])
        : "r"(weight.x), "r"(weight.y), "r"(weight.z), "r"(weight.w),
          "r"(input[quad_lane]), "r"(input[quad_lane + 4]), "r"(weight_scale), "r"(input_scale));
}

__device__ void block_dot_mxfp8(
    float (&sum)[4], uint4 weight,
    const uint32_t* input, unsigned quad_lane, uint32_t weight_scale, uint32_t input_scale
) {
    asm("mma.sync.aligned.m16n8k32.row.col.kind::mxf8f6f4.block_scale.scale_vec::1X.f32.e4m3.e4m3.f32.ue8m0 "
        "{%0, %1, %2, %3}, {%4, %5, %6, %7}, {%8, %9}, {%0, %1, %2, %3}, %10, {0, 0}, %11, {0, 0};"
        : "+f"(sum[0]), "+f"(sum[1]), "+f"(sum[2]), "+f"(sum[3])
        : "r"(weight.x), "r"(weight.y), "r"(weight.z), "r"(weight.w),
          "r"(input[quad_lane]), "r"(input[quad_lane + 4]), "r"(weight_scale), "r"(input_scale));
}

template<bool FP4>
__device__ void project(
    __nv_bfloat16* output, const uint8_t* act, const uint8_t* act_scales,
    const uint8_t* weights, const uint8_t* weight_scales, const uint32_t* ids,
    const float* row_outer, uint32_t k, uint32_t n, uint32_t expert_count
) {
    constexpr unsigned step = FP4 ? 64 : 32;
    constexpr unsigned scales_per_step = FP4 ? 4 : 1;
    const unsigned tid = threadIdx.x;
    const unsigned warp = tid / warp_size;
    const unsigned quad_lane = tid % 4;
    const unsigned quad = tid / 4 % 8;
    const unsigned column = blockIdx.x * output_tile;
    const unsigned row = blockIdx.y;
    const unsigned expert = ids[row];
    if (expert >= expert_count) {
        if (tid < output_tile) output[(size_t)row * n + column + tid] = __float2bfloat16_rn(0);
        return;
    }
    const unsigned tiles = k / step;
    const unsigned packed_bytes = tiles * 32;
    const unsigned scale_bytes = tiles * scales_per_step;
    const auto* a = reinterpret_cast<const uint32_t*>(act + (size_t)row * packed_bytes);
    const uint8_t* as = act_scales + (size_t)row * scale_bytes;
    const size_t tile_base = ((size_t)expert * (n / output_tile) + blockIdx.x) * tiles;
    const auto* packed_tiles = reinterpret_cast<const uint4*>(weights) + tile_base * warp_size;
    const auto* scale_tiles = weight_scales + tile_base * output_tile * scales_per_step;
    float sum[4] = {};
    for (unsigned t = warp; t < tiles; t += k_partitions) {
        const unsigned packed_offset = 8 * t;
        const uint4 packed = packed_tiles[(size_t)t * warp_size + tid % warp_size];
        const unsigned scale_row = quad + (quad_lane == 1 ? 8 : 0);
        const uint8_t* scale_fragment = scale_tiles + ((size_t)t * output_tile + scale_row) * scales_per_step;
        if (FP4) {
            const uint32_t sa = *reinterpret_cast<const uint32_t*>(as + 4 * t);
            const uint32_t sw = quad_lane < 2 ? *reinterpret_cast<const uint32_t*>(scale_fragment) : 0;
            block_dot_nvfp4(sum, packed, a + packed_offset, quad_lane, sw, sa);
        } else {
            const uint32_t sa = as[t];
            const uint32_t sw = quad_lane < 2 ? *scale_fragment : 0;
            block_dot_mxfp8(sum, packed, a + packed_offset, quad_lane, sw, sa);
        }
    }
    __shared__ float partial[k_partitions][output_tile];
    if (quad_lane == 0) {
        partial[warp][quad] = sum[0];
        partial[warp][quad + 8] = sum[2];
    }
    __syncthreads();
    if (tid < output_tile) {
        float value = (partial[0][tid] + partial[1][tid]) + (partial[2][tid] + partial[3][tid]);
        if (FP4) value *= row_outer[row];
        output[(size_t)row * n + column + tid] = __float2bfloat16_rn(value);
    }
}

template<bool FP4, unsigned Routes, unsigned OutputTiles = 1>
__device__ void project_quantized_group(
    __nv_bfloat16* output, const uint8_t* act, const uint8_t* act_scales,
    const uint8_t* weights, const uint8_t* weight_scales, const ExpertRouteGroup<Routes>* groups,
    const float* row_outer, uint32_t k, uint32_t n, uint32_t expert_count, const uint32_t* group_count
) {
    constexpr unsigned step = FP4 ? 64 : 32;
    constexpr unsigned scale_bytes = FP4 ? 4 : 1;
    constexpr unsigned matrices = Routes / routes_per_group;
    constexpr unsigned output_columns = OutputTiles * output_tile;
    const unsigned tid = threadIdx.x;
    const unsigned warp = tid / warp_size;
    const unsigned quad = tid % warp_size / 4;
    const unsigned quad_lane = tid % 4;
    const unsigned work = blockIdx.y;
    if (work >= *group_count) return;
    const auto& group = groups[work];
    const unsigned expert = group.expert;
    const unsigned column = blockIdx.x * output_columns;
    __shared__ float partial[k_partitions][Routes][output_columns];
    if (expert < expert_count) {
        const unsigned tiles = k / step;
        const size_t tile_base = ((size_t)expert * (n / output_tile) + blockIdx.x * OutputTiles) * tiles;
        const uint4* values = reinterpret_cast<const uint4*>(weights) + tile_base * warp_size;
        const uint8_t* scales = weight_scales + tile_base * output_tile * scale_bytes;
        const uint32_t* a[matrices];
        const uint8_t* as[matrices];
        #pragma unroll
        for (unsigned i = 0; i < matrices; ++i) {
            unsigned row = group.rows[i * routes_per_group + quad];
            if (row == UINT32_MAX) row = group.rows[0];
            a[i] = reinterpret_cast<const uint32_t*>(act + size_t(row) * tiles * 32);
            as[i] = act_scales + size_t(row) * tiles * scale_bytes;
        }
        float sum[OutputTiles][matrices][4] = {};
        for (unsigned t = warp; t < tiles; t += k_partitions) {
            const unsigned scale_row = quad + (quad_lane == 1 ? 8 : 0);
            uint4 packed[OutputTiles];
            uint32_t sw[OutputTiles];
            #pragma unroll
            for (unsigned tile = 0; tile < OutputTiles; ++tile) {
                const size_t index = size_t(tile) * tiles + t;
                packed[tile] = values[index * warp_size + tid % warp_size];
                const uint8_t* scale_fragment = scales + (index * output_tile + scale_row) * scale_bytes;
                if constexpr (FP4) sw[tile] = quad_lane < 2 ? *reinterpret_cast<const uint32_t*>(scale_fragment) : 0;
                else sw[tile] = quad_lane < 2 ? *scale_fragment : 0;
            }
            #pragma unroll
            for (unsigned i = 0; i < matrices; ++i) {
                const uint32_t* input = a[i] + 8 * t;
                const uint32_t sa = FP4 ? *reinterpret_cast<const uint32_t*>(as[i] + 4 * t) : as[i][t];
                #pragma unroll
                for (unsigned tile = 0; tile < OutputTiles; ++tile) {
                    if constexpr (FP4) block_dot_nvfp4(sum[tile][i], packed[tile], input, quad_lane, sw[tile], sa);
                    else block_dot_mxfp8(sum[tile][i], packed[tile], input, quad_lane, sw[tile], sa);
                }
            }
        }
        #pragma unroll
        for (unsigned i = 0; i < matrices; ++i) {
            const unsigned slot = i * routes_per_group + 2 * quad_lane;
            #pragma unroll
            for (unsigned tile = 0; tile < OutputTiles; ++tile) {
                const unsigned feature = tile * output_tile + quad;
                partial[warp][slot][feature] = sum[tile][i][0];
                partial[warp][slot + 1][feature] = sum[tile][i][1];
                partial[warp][slot][feature + 8] = sum[tile][i][2];
                partial[warp][slot + 1][feature + 8] = sum[tile][i][3];
            }
        }
        __syncthreads();
    }
    #pragma unroll
    for (unsigned index = tid; index < Routes * output_columns; index += k_partitions * warp_size) {
        const unsigned slot = index / output_columns;
        const unsigned feature = index % output_columns;
        const unsigned row = group.rows[slot];
        if (row == UINT32_MAX) continue;
        float value = 0.0f;
        if (expert < expert_count) {
            value = (partial[0][slot][feature] + partial[1][slot][feature]) +
                    (partial[2][slot][feature] + partial[3][slot][feature]);
            if constexpr (FP4) value *= row_outer[row];
        }
        output[size_t(row) * n + column + feature] = __float2bfloat16_rn(value);
    }
}
template<bool FP4, bool Grouped>
__device__ uint32_t dequantized_bf16_pair(uint32_t packet, unsigned shift, float block_scale, float outer) {
    const uint32_t code = packet >> shift;
    float2 pair;
    if constexpr (FP4 && Grouped) {
        uint32_t halves;
        asm("{.reg .b8 low, high;\n\t"
            "mov.b16 {low, high}, %1;\n\t"
            "cvt.rn.f16x2.e2m1x2 %0, low;}"
            : "=r"(halves) : "h"((uint16_t)code));
        const __half2_raw raw = {(uint16_t)halves, (uint16_t)(halves >> 16)};
        pair = __half22float2(static_cast<__half2>(raw));
    } else if constexpr (FP4) {
        pair = __half22float2(static_cast<__half2>(__nv_cvt_fp4x2_to_halfraw2(code & 255u, __NV_E2M1)));
    } else {
        pair = __half22float2(static_cast<__half2>(__nv_cvt_fp8x2_to_halfraw2(code & 65535u, __NV_E4M3)));
    }
    const __nv_bfloat162_raw bf16 = __floats2bfloat162_rn((pair.x * block_scale) * outer, (pair.y * block_scale) * outer);
    return (uint32_t)bf16.x | ((uint32_t)bf16.y << 16);
}

__device__ float e8m0_weight_scale(uint32_t exponent) {
    if (exponent == 255u) return __uint_as_float(0x7fc00000u);
    return __uint_as_float(exponent == 0u ? 0x00400000u : exponent << 23);
}

__device__ void bf16_mma(float (&sum)[4], uint32_t a0, uint32_t a1, uint32_t a2, uint32_t a3, uint32_t b0, uint32_t b1) {
    asm("mma.sync.aligned.m16n8k16.row.col.f32.bf16.bf16.f32 "
        "{%0, %1, %2, %3}, {%4, %5, %6, %7}, {%8, %9}, {%0, %1, %2, %3};"
        : "+f"(sum[0]), "+f"(sum[1]), "+f"(sum[2]), "+f"(sum[3])
        : "r"(a0), "r"(a1), "r"(a2), "r"(a3), "r"(b0), "r"(b1));
}

template<bool FP4, unsigned Routes = 0, bool SwiGlu = false>
__device__ void project_bf16(
    __nv_bfloat16* output, const uint32_t* act,
    const uint4* weights, const uint8_t* weight_scales, const uint32_t* ids,
    const float* weight_amax, const float* denominator,
    uint32_t k, uint32_t n, uint32_t expert_count, uint32_t repeat,
    const ExpertRouteGroup<Routes == 0 ? routes_per_group : Routes>* groups = nullptr,
    const uint32_t* group_count = nullptr,
    const float* route_weights = nullptr, float gate_cap = 0, float up_cap = 0
) {
    constexpr bool grouped = Routes != 0;
    static_assert(!SwiGlu || !grouped, "fused activation requires one route per block");
    constexpr unsigned route_matrices = grouped ? Routes / routes_per_group : 1;
    constexpr unsigned step = FP4 ? 64 : 32;
    constexpr unsigned scale_bytes = FP4 ? 4 : 1;
    const unsigned tid = threadIdx.x;
    const unsigned lane = tid % warp_size;
    const unsigned warp = tid / warp_size;
    const unsigned quad_lane = lane % 4;
    const unsigned quad = lane / 4;
    const unsigned column = blockIdx.x * output_tile;
    const unsigned work = blockIdx.y;
    unsigned expert;
    if constexpr (grouped) {
        if (work >= *group_count) return;
        expert = groups[work].expert;
    } else {
        expert = ids[work];
    }
    if (expert >= expert_count) {
        if constexpr (grouped) {
            #pragma unroll
            for (unsigned index = tid; index < Routes * output_tile; index += warp_size * k_partitions) {
                const unsigned dest = groups[work].rows[index / output_tile];
                if (dest != UINT32_MAX)
                    output[(size_t)dest * n + column + index % output_tile] = __float2bfloat16_rn(0);
            }
        } else if constexpr (SwiGlu) {
            if (tid < output_tile / 2) {
                const auto zero = __float2bfloat16_rn(0);
                output[((size_t)work * n + column) / 2 + tid] =
                    antfly_routed_swiglu::weighted(zero, zero, route_weights[work], gate_cap, up_cap);
            }
        } else if (tid < output_tile) output[(size_t)work * n + column + tid] = __float2bfloat16_rn(0);
        return;
    }
    const unsigned tiles = k / step;
    const uint32_t* a[route_matrices];
    #pragma unroll
    for (unsigned i = 0; i < route_matrices; ++i) {
        unsigned input_row = work;
        if constexpr (grouped) {
            input_row = groups[work].rows[i * routes_per_group + quad];
            if (input_row == UINT32_MAX) input_row = groups[work].rows[0];
        }
        a[i] = act + (size_t)(input_row / repeat) * (k / 2);
    }
    const size_t tile_base = ((size_t)expert * (n / output_tile) + blockIdx.x) * tiles;
    const uint4* packed_tiles = weights + tile_base * warp_size;
    const uint8_t* scale_tiles = weight_scales + tile_base * output_tile * scale_bytes;
    const float outer = FP4 ? __fdiv_rn(weight_amax[expert], *denominator) : 1.0f;
    float sum[route_matrices][4] = {};
    for (unsigned t = warp; t < tiles; t += k_partitions) {
        const uint4 packed = packed_tiles[(size_t)t * warp_size + lane];
        const uint8_t* sw = scale_tiles + ((size_t)t * output_tile + quad) * scale_bytes;
        const uint32_t sw0 = FP4 ? *reinterpret_cast<const uint32_t*>(sw) : *sw;
        const uint32_t sw1 = FP4 ? *reinterpret_cast<const uint32_t*>(sw + 8 * scale_bytes) : sw[8];
        #pragma unroll
        for (unsigned part = 0; part < step / 16; ++part) {
            const unsigned source = (part % 2) * 2;
            const bool first_half = FP4 ? part < 2 : part == 0;
            const uint32_t w0 = first_half ? packed.x : packed.z;
            const uint32_t w1 = first_half ? packed.y : packed.w;
            const float s0 = FP4 ? antfly_nvfp4::e4m3((sw0 >> (8 * part)) & 255u) : e8m0_weight_scale(sw0);
            const float s1 = FP4 ? antfly_nvfp4::e4m3((sw1 >> (8 * part)) & 255u) : e8m0_weight_scale(sw1);
            // The stored fragments serve the block-scaled MMA. Gather each
            // BF16 MMA's two pairs from the appropriate lanes in the same quad.
            uint32_t lo0, lo1, hi0, hi1;
            if (FP4) {
                lo0 = __shfl_sync(0xffffffffu, w0, source, 4);
                lo1 = __shfl_sync(0xffffffffu, w1, source, 4);
                hi0 = __shfl_sync(0xffffffffu, w0, source + 1, 4);
                hi1 = __shfl_sync(0xffffffffu, w1, source + 1, 4);
            } else {
                const unsigned src = quad_lane / 2;
                lo0 = __shfl_sync(0xffffffffu, w0, src, 4);
                lo1 = __shfl_sync(0xffffffffu, w1, src, 4);
                hi0 = __shfl_sync(0xffffffffu, w0, src + 2, 4);
                hi1 = __shfl_sync(0xffffffffu, w1, src + 2, 4);
            }
            const unsigned pair_shift = FP4 ? quad_lane * 8 : (quad_lane % 2) * 16;
            const unsigned b = t * (step / 2) + part * 8 + quad_lane;
            const uint32_t w_lo0 = dequantized_bf16_pair<FP4, grouped || SwiGlu>(lo0, pair_shift, s0, outer);
            const uint32_t w_lo1 = dequantized_bf16_pair<FP4, grouped || SwiGlu>(lo1, pair_shift, s1, outer);
            const uint32_t w_hi0 = dequantized_bf16_pair<FP4, grouped || SwiGlu>(hi0, pair_shift, s0, outer);
            const uint32_t w_hi1 = dequantized_bf16_pair<FP4, grouped || SwiGlu>(hi1, pair_shift, s1, outer);
            #pragma unroll
            for (unsigned i = 0; i < route_matrices; ++i) {
                bf16_mma(sum[i], w_lo0, w_lo1, w_hi0, w_hi1, a[i][b], a[i][b + 4]);
            }
        }
    }
    if constexpr (grouped) {
        // B's columns now carry different routes for the same expert. Scatter
        // each column back to its original route, with the single-route split-K tree.
        __shared__ float partial[k_partitions][Routes][output_tile];
        #pragma unroll
        for (unsigned i = 0; i < route_matrices; ++i) {
            const unsigned slot = i * routes_per_group + 2 * quad_lane;
            partial[warp][slot][quad] = sum[i][0];
            partial[warp][slot + 1][quad] = sum[i][1];
            partial[warp][slot][quad + 8] = sum[i][2];
            partial[warp][slot + 1][quad + 8] = sum[i][3];
        }
        __syncthreads();
        #pragma unroll
        for (unsigned index = tid; index < Routes * output_tile; index += warp_size * k_partitions) {
            const unsigned slot = index / output_tile;
            const unsigned feature = index % output_tile;
            const unsigned dest = groups[work].rows[slot];
            if (dest != UINT32_MAX) {
                const float value = (partial[0][slot][feature] + partial[1][slot][feature]) +
                                    (partial[2][slot][feature] + partial[3][slot][feature]);
                output[(size_t)dest * n + column + feature] = __float2bfloat16_rn(value);
            }
        }
    } else {
        __shared__ float partial[k_partitions][output_tile];
        if (quad_lane == 0) {
            partial[warp][quad] = sum[0][0];
            partial[warp][quad + 8] = sum[0][2];
        }
        __syncthreads();
        if constexpr (SwiGlu) {
            if (tid < output_tile / 2) {
                // Round both projected values exactly where the separate
                // projection writes them before applying weighted SwiGLU.
                const unsigned gate = 2 * tid;
                const unsigned up = gate + 1;
                const float gate_value = (partial[0][gate] + partial[1][gate]) + (partial[2][gate] + partial[3][gate]);
                const float up_value = (partial[0][up] + partial[1][up]) + (partial[2][up] + partial[3][up]);
                output[((size_t)work * n + column) / 2 + tid] =
                    antfly_routed_swiglu::weighted(__float2bfloat16_rn(gate_value), __float2bfloat16_rn(up_value),
                                                  route_weights[work], gate_cap, up_cap);
            }
        } else if (tid < output_tile) {
            const float value = (partial[0][tid] + partial[1][tid]) + (partial[2][tid] + partial[3][tid]);
            output[(size_t)work * n + column + tid] = __float2bfloat16_rn(value);
        }
    }
}
} // namespace

extern "C" __global__ void antfly_routed_e2m1_e4m3_m16n8k64_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as,
    const uint8_t* w, const uint8_t* ws, const uint32_t* ids,
    const float* scale, uint32_t k, uint32_t n, uint32_t expert_count
) {
    project<true>(out, a, as, w, ws, ids, scale, k, n, expert_count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_m16n8k32_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as,
    const uint8_t* w, const uint8_t* ws, const uint32_t* ids,
    const float* scale, uint32_t k, uint32_t n, uint32_t expert_count
) {
    project<false>(out, a, as, w, ws, ids, scale, k, n, expert_count);
}

extern "C" __global__ void antfly_quantize_bf16_rows_e2m1_e4m3_sm120(
    uint8_t* values, uint8_t* scales, float* outer, const __nv_bfloat16* input,
    const uint32_t* ids, const float* weight_amax, const float* weight_denominator,
    uint32_t width, uint32_t repeat, uint32_t expert_count
) {
    antfly_nvfp4::quantize_nvfp4_rows<true>(values, scales, outer, input, ids, weight_amax,
                                              weight_denominator, width, repeat, expert_count);
}

// All routes of a source token reuse its encoded values and block scales.
// Only the final outer multiplier depends on the selected expert.
extern "C" __global__ void antfly_quantize_bf16_tokens_e2m1_e4m3_routes_sm120(
    uint8_t* values, uint8_t* scales, float* outer, const __nv_bfloat16* input,
    const uint32_t* ids, const float* weight_amax, const float* weight_denominator,
    uint32_t width, uint32_t repeat, uint32_t expert_count
) {
    const unsigned lane = threadIdx.x % warp_size;
    const size_t first_row = size_t(blockIdx.x) * repeat;
    const auto* source = input + size_t(blockIdx.x) * width;
    __shared__ float warp_maxima[8];
    float maximum = 0.0f;
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x)
        maximum = fmaxf(maximum, fabsf(__bfloat162float(source[i])));
    maximum = antfly_row_warp_max(maximum);
    if (lane == 0) warp_maxima[threadIdx.x / warp_size] = maximum;
    __syncthreads();
    maximum = antfly_row_warp_max(lane < blockDim.x / warp_size ? warp_maxima[lane] : 0.0f);
    maximum = __shfl_sync(0xffffffffu, maximum, 0);

    const unsigned blocks = width / 16;
    for (unsigned group = threadIdx.x; group < blocks; group += blockDim.x) {
        uint8_t packed[8];
        const uint8_t scale = antfly_nvfp4::e2m1_block<true>(packed, source + group * 16, maximum);
        uint64_t encoded = 0;
        #pragma unroll
        for (unsigned i = 0; i < 8; ++i) encoded |= uint64_t(packed[i]) << (8 * i);
        for (unsigned route = 0; route < repeat; ++route) {
            const size_t row = first_row + route;
            reinterpret_cast<uint64_t*>(values)[row * blocks + group] = encoded;
            scales[row * blocks + group] = scale;
        }
    }
    for (unsigned route = threadIdx.x; route < repeat; route += blockDim.x) {
        const size_t row = first_row + route;
        const unsigned expert = ids[row];
        outer[row] = expert < expert_count ? (maximum * weight_amax[expert]) *
            __frcp_rn(1536.0f * *weight_denominator) : 0.0f;
    }
}

extern "C" __global__ void antfly_weighted_swiglu_1152_e2m1_e4m3_sm120(
    uint8_t* values, uint8_t* scales, float* outer, const __nv_bfloat16* gate_up,
    const float* route_weights, const uint32_t* ids, const float* weight_amax,
    const float* weight_denominator, uint32_t expert_count, float gate_cap, float up_cap
) {
    constexpr unsigned width = 1152;
    constexpr unsigned blocks = width / 16;
    const size_t row = blockIdx.x;
    const unsigned lane = threadIdx.x % warp_size;
    const auto* source = gate_up + row * width * 2;
    __shared__ __nv_bfloat16 activated[width];
    __shared__ float warp_maxima[8];
    const float routing = route_weights[row];
    float maximum = 0.0f;
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) {
        // Retain the BF16 boundary between the gate and quantization.
        const __nv_bfloat16 value = antfly_routed_swiglu::weighted(source[2 * i], source[2 * i + 1], routing, gate_cap, up_cap);
        activated[i] = value;
        maximum = fmaxf(maximum, fabsf(__bfloat162float(value)));
    }
    maximum = antfly_row_warp_max(maximum);
    if (lane == 0) warp_maxima[threadIdx.x / warp_size] = maximum;
    __syncthreads();
    maximum = antfly_row_warp_max(lane < blockDim.x / warp_size ? warp_maxima[lane] : 0.0f);
    maximum = __shfl_sync(0xffffffffu, maximum, 0);
    if (threadIdx.x < blocks) {
        const unsigned group = threadIdx.x;
        uint8_t* target = values + (row * blocks + group) * 8;
        scales[row * blocks + group] = antfly_nvfp4::e2m1_block<true>(target, activated + group * 16, maximum);
    }
    if (threadIdx.x == 0) {
        const unsigned expert = ids[row];
        outer[row] = expert < expert_count ? (maximum * weight_amax[expert]) *
            __frcp_rn(1536.0f * *weight_denominator) : 0.0f;
    }
}

namespace {

// Eight lanes share a 32-element MXFP8 scale. Each lane writes four
// consecutive bytes, using the native packed conversion for each pair.
__device__ uint32_t encode_mxfp8_quad(float (&values)[4], unsigned mask, unsigned& exponent) {
    float maximum = fmaxf(0.0f, fmaxf(fmaxf(fabsf(values[0]), fabsf(values[1])),
                                     fmaxf(fabsf(values[2]), fabsf(values[3]))));
    #pragma unroll
    for (unsigned offset = 4; offset != 0; offset /= 2)
        maximum = fmaxf(maximum, __shfl_xor_sync(mask, maximum, offset, 8));
    exponent = antfly_mxfp8::block_exponent(maximum);
    const float inverse = antfly_mxfp8::inverse_scale(exponent);
    uint16_t lo, hi;
    asm("cvt.rn.satfinite.e4m3x2.f32 %0, %2, %1;"
        : "=h"(lo) : "f"(values[0] * inverse), "f"(values[1] * inverse));
    asm("cvt.rn.satfinite.e4m3x2.f32 %0, %2, %1;"
        : "=h"(hi) : "f"(values[2] * inverse), "f"(values[3] * inverse));
    return uint32_t(lo) | (uint32_t(hi) << 16);
}

template<bool Activate>
__device__ void quantize_mxfp8(
    uint32_t* values, uint8_t* scales, const __nv_bfloat16* input,
    const float* route_weights, unsigned width, unsigned repeat, float gate_cap, float up_cap
) {
    const unsigned lane = threadIdx.x % 8;
    const size_t first_row = size_t(blockIdx.x) * repeat;
    const size_t source_row = size_t(blockIdx.x) * width;
    const unsigned blocks = width / 32;
    for (unsigned group = threadIdx.x / 8; group < blocks; group += blockDim.x / 8) {
        const size_t base = source_row + group * 32 + lane * 4;
        float source[4];
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i) {
            if constexpr (Activate) {
                const size_t element = 2 * (base + i);
                source[i] = __bfloat162float(antfly_routed_swiglu::weighted(
                    input[element], input[element + 1], route_weights[blockIdx.x], gate_cap, up_cap));
            } else {
                source[i] = __bfloat162float(input[base + i]);
            }
        }
        unsigned exponent;
        const uint32_t packed = encode_mxfp8_quad(source, 0xffu << (threadIdx.x % 32 - lane), exponent);
        for (unsigned route = 0; route < repeat; ++route) {
            const size_t row = first_row + route;
            values[row * (width / 4) + group * 8 + lane] = packed;
            if (lane == 0) scales[row * blocks + group] = exponent;
        }
    }
}

} // namespace

extern "C" __global__ void antfly_quantize_bf16_tokens_e4m3_e8m0_routes_sm120(
    uint32_t* values, uint8_t* scales, float*, const __nv_bfloat16* input,
    const uint32_t*, const float*, const float*, uint32_t width, uint32_t repeat, uint32_t
) {
    quantize_mxfp8<false>(values, scales, input, nullptr, width, repeat, 0.0f, 0.0f);
}

extern "C" __global__ void antfly_weighted_swiglu_1152_e4m3_e8m0_sm120(
    uint32_t* values, uint8_t* scales, float*, const __nv_bfloat16* gate_up,
    const float* route_weights, const uint32_t*, const float*, const float*, uint32_t,
    float gate_cap, float up_cap
) {
    quantize_mxfp8<true>(values, scales, gate_up, route_weights, 1152, 1, gate_cap, up_cap);
}

// One CTA owns all K tiles for the expert's 16 output rows. Preserve every
// native packed nibble/byte and scale in the fragment order used by MMA.
extern "C" __global__ void antfly_pack_m16n8_routed_weights_sm120(
    uint4* out, uint8_t* scales_out, const uint32_t* source, const uint8_t* scales,
    uint32_t tiles, uint32_t scale_width
) {
    const unsigned lane = threadIdx.x % warp_size;
    const unsigned quad = lane / 4;
    const unsigned quad_lane = lane % 4;
    const size_t base_row = (size_t)blockIdx.x * output_tile;
    const uint32_t* src0 = source + (base_row + quad) * tiles * 8;
    const uint32_t* src1 = src0 + 8 * tiles * 8;
    for (unsigned t = threadIdx.x / warp_size; t < tiles; t += blockDim.x / warp_size) {
        const size_t dest_tile = (size_t)blockIdx.x * tiles + t;
        const unsigned k = 8 * t + quad_lane;
        out[dest_tile * warp_size + lane] = {src0[k], src1[k], src0[k + 4], src1[k + 4]};
        if (lane < output_tile) {
            const uint8_t* from = scales + ((base_row + lane) * tiles + t) * scale_width;
            uint8_t* to = scales_out + (dest_tile * output_tile + lane) * scale_width;
            if (scale_width == 4) *reinterpret_cast<uint32_t*>(to) = *reinterpret_cast<const uint32_t*>(from);
            else *to = *from;
        }
    }
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_bf16_input_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const uint32_t* ids,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat
) {
    project_bf16<true>(out, a, w, ws, ids, amax, denominator, k, n, experts, repeat);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_bf16_input_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const uint32_t* ids,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat
) {
    project_bf16<false>(out, a, w, ws, ids, amax, denominator, k, n, experts, repeat);
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_bf16_input_swiglu_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const uint32_t* ids,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const float* route_weights, float gate_cap, float up_cap
) {
    project_bf16<true, 0, true>(out, a, w, ws, ids, amax, denominator, k, n, experts, repeat,
                               nullptr, nullptr, route_weights, gate_cap, up_cap);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_bf16_input_swiglu_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const uint32_t* ids,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const float* route_weights, float gate_cap, float up_cap
) {
    project_bf16<false, 0, true>(out, a, w, ws, ids, amax, denominator, k, n, experts, repeat,
                               nullptr, nullptr, route_weights, gate_cap, up_cap);
}

template<unsigned RouteLimit, typename Index = unsigned, unsigned Routes = routes_per_group, unsigned Warps = 8>
__device__ void group_routed_rows(
    ExpertRouteGroup<Routes>* groups, uint32_t* count, const uint32_t* ids, uint32_t rows, uint32_t experts
) {
    static_assert(RouteLimit <= UINT16_MAX);
    __shared__ Index selected[Warps][RouteLimit];
    __shared__ Index shared_ids[RouteLimit];
    __shared__ unsigned group_count;
    const unsigned lane = threadIdx.x % warp_size;
    const unsigned warp = threadIdx.x / warp_size;
    if (threadIdx.x == 0) group_count = 0;
    if constexpr (RouteLimit <= Warps * warp_size) {
        if (threadIdx.x < rows) shared_ids[threadIdx.x] = min(ids[threadIdx.x], experts);
    } else {
        for (unsigned row = threadIdx.x; row < rows; row += Warps * warp_size)
            shared_ids[row] = min(ids[row], experts);
    }
    __syncthreads();
    // The final bucket represents invalid expert ids, whose output is zero.
    for (unsigned expert = warp; expert <= experts; expert += Warps) {
        unsigned matches = 0;
        for (unsigned first = 0; first < rows; first += warp_size) {
            const unsigned row = first + lane;
            const unsigned mask = __ballot_sync(0xffffffffu, row < rows && shared_ids[row] == expert);
            if ((mask >> lane) & 1u)
                selected[warp][matches + __popc(mask & ((1u << lane) - 1u))] = row;
            matches += __popc(mask);
        }
        if (matches == 0) continue;
        __syncwarp();
        const unsigned batches = (matches + Routes - 1) / Routes;
        const unsigned reserved = lane == 0 ? atomicAdd(&group_count, batches) : 0;
        const unsigned base = __shfl_sync(0xffffffffu, reserved, 0);
        for (unsigned slot = lane; slot < batches * Routes; slot += warp_size) {
            auto& group = groups[base + slot / Routes];
            if (slot % Routes == 0) group.expert = expert;
            group.rows[slot % Routes] = slot < matches ? selected[warp][slot] : UINT32_MAX;
        }
        __syncwarp();
    }
    __syncthreads();
    if (threadIdx.x == 0) *count = group_count;
}

extern "C" __global__ void antfly_group_routed_rows8(
    RouteGroup* groups, uint32_t* count, const uint32_t* ids, uint32_t rows, uint32_t experts
) {
    group_routed_rows<128>(groups, count, ids, rows, experts);
}

extern "C" __global__ void antfly_group_routed_rows8_512(
    RouteGroup* groups, uint32_t* count, const uint32_t* ids, uint32_t rows, uint32_t experts
) {
    group_routed_rows<512>(groups, count, ids, rows, experts);
}

extern "C" __global__ void antfly_group_routed_rows32_4096(
    WideRouteGroup* groups, uint32_t* count, const uint32_t* ids, uint32_t rows, uint32_t experts
) {
    group_routed_rows<4096, uint16_t, routes_per_wide_group, 4>(groups, count, ids, rows, experts);
}

// Count and reserve each expert's group range. A second grid scatters
// individual rows to that range, so this does not use a rows-by-experts scan.
extern "C" __global__ void antfly_group_routed_rows64_count(
    LargeRouteGroup* groups, uint32_t* count, uint32_t* prefixes, uint32_t* cursors,
    const uint32_t* ids, uint32_t rows, uint32_t experts
) {
    constexpr unsigned threads = 256;
    __shared__ uint32_t histogram[threads + 1];
    __shared__ unsigned produced;
    for (unsigned expert = threadIdx.x; expert <= experts; expert += threads)
        histogram[expert] = 0;
    if (threadIdx.x == 0) produced = 0;
    __syncthreads();
    for (unsigned row = threadIdx.x; row < rows; row += threads)
        atomicAdd(&histogram[min(ids[row], experts)], 1);
    __syncthreads();
    for (unsigned expert = threadIdx.x; expert <= experts; expert += threads) {
        const unsigned matches = histogram[expert];
        const unsigned needed = (matches + routes_per_large_group - 1) / routes_per_large_group;
        prefixes[expert] = needed == 0 ? 0 : atomicAdd(&produced, needed);
        cursors[expert] = 0;
    }
    __syncthreads();
    if (threadIdx.x == 0) *count = produced;
    for (unsigned index = threadIdx.x; index < produced * routes_per_large_group; index += threads)
        groups[index / routes_per_large_group].rows[index % routes_per_large_group] = UINT32_MAX;
}

extern "C" __global__ void antfly_group_routed_rows64_scatter(
    LargeRouteGroup* groups, const uint32_t* prefixes, uint32_t* cursors,
    const uint32_t* ids, uint32_t rows, uint32_t experts
) {
    const unsigned row = blockIdx.x * blockDim.x + threadIdx.x;
    if (row >= rows) return;
    const unsigned expert = min(ids[row], experts);
    const unsigned local = atomicAdd(cursors + expert, 1);
    auto& group = groups[prefixes[expert] + local / routes_per_large_group];
    if (local % routes_per_large_group == 0) group.expert = expert;
    group.rows[local % routes_per_large_group] = row;
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_bf16_input_grouped_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const RouteGroup* groups,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const uint32_t* count
) {
    project_bf16<true, routes_per_group>(out, a, w, ws, nullptr, amax, denominator, k, n, experts, repeat, groups, count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_bf16_input_grouped_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const RouteGroup* groups,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const uint32_t* count
) {
    project_bf16<false, routes_per_group>(out, a, w, ws, nullptr, amax, denominator, k, n, experts, repeat, groups, count);
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_bf16_input_routes32_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const WideRouteGroup* groups,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const uint32_t* count
) {
    project_bf16<true, routes_per_wide_group>(out, a, w, ws, nullptr, amax, denominator, k, n, experts, repeat, groups, count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_bf16_input_routes32_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const WideRouteGroup* groups,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const uint32_t* count
) {
    project_bf16<false, routes_per_wide_group>(out, a, w, ws, nullptr, amax, denominator, k, n, experts, repeat, groups, count);
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_routes8_m16n8k64_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const RouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<true>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_routes8_m16n8k32_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const RouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<false>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_routes32_m16n8k64_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const WideRouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<true>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_routes32_m16n8k32_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const WideRouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<false>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_bf16_input_routes64_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const LargeRouteGroup* groups,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const uint32_t* count
) {
    project_bf16<true, routes_per_large_group>(out, a, w, ws, nullptr, amax, denominator, k, n, experts, repeat, groups, count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_bf16_input_routes64_m16n8k16_sm120(
    __nv_bfloat16* out, const uint32_t* a, const uint4* w, const uint8_t* ws, const LargeRouteGroup* groups,
    const float* amax, const float* denominator, uint32_t k, uint32_t n, uint32_t experts, uint32_t repeat,
    const uint32_t* count
) {
    project_bf16<false, routes_per_large_group>(out, a, w, ws, nullptr, amax, denominator, k, n, experts, repeat, groups, count);
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_routes64_m16n8k64_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const LargeRouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<true>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_routes64_m16n8k32_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const LargeRouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<false>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_routes64_cols32_k64_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const LargeRouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<true, routes_per_large_group, 2>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_routes64_cols32_k32_bf16_sm120(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as, const uint8_t* w, const uint8_t* ws,
    const LargeRouteGroup* groups, const float* row_outer, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    project_quantized_group<false, routes_per_large_group, 2>(out, a, as, w, ws, groups, row_outer, k, n, experts, count);
}

namespace {

struct Nvfp4ExpertTile {
    uint4 weights[4][32];
    uint4 activation[routes_per_large_group][2];
    uint32_t weight_scale[4][16];
    uint32_t activation_scale[routes_per_large_group];
};

__device__ void prefetch_fragment16(uint4* target, const uint4* source, bool valid) {
    const unsigned offset = __cvta_generic_to_shared(target);
    const unsigned size = valid ? 16 : 0;
    asm volatile("cp.async.cg.shared.global [%0], [%1], 16, %2;"
        :: "r"(offset), "l"(source), "r"(size) : "memory");
}

__device__ void prefetch_nvfp4_expert_tile(
    Nvfp4ExpertTile& stage, const LargeRouteGroup& group,
    const uint8_t* act, const uint32_t* as, const uint4* w, const uint32_t* ws,
    unsigned tiles, unsigned t
) {
    const unsigned tid = threadIdx.x;
    if (tid < 4 * 32) {
        const unsigned col = tid / 32;
        const unsigned lane = tid % 32;
        const size_t index = size_t(col) * tiles + t;
        prefetch_fragment16(&stage.weights[col][lane], w + index * 32 + lane, true);
        if (lane < 16) stage.weight_scale[col][lane] = ws[index * 16 + lane];
    }
    if (tid < routes_per_large_group * 2) {
        const unsigned slot = tid / 2;
        const unsigned row = group.rows[slot];
        const bool valid = row != UINT32_MAX;
        const size_t index = valid ? size_t(row) * tiles + t : 0;
        prefetch_fragment16(&stage.activation[slot][tid % 2], reinterpret_cast<const uint4*>(act) + index * 2 + tid % 2, valid);
        if ((tid & 1) == 0) stage.activation_scale[slot] = valid ? as[index] : 0;
    }
    asm volatile("cp.async.commit_group;" ::: "memory");
}

} // namespace

// Double-buffer a 64-column expert tile. Each warp keeps the full K sum for
// its 32 routed activations. The split-K program remains available to callers
// requiring its F32 summation order.
extern "C" __global__ __launch_bounds__(256)
void antfly_routed_e2m1_e4m3_routes64_cols64_pipelined_bf16_sm120(
    __nv_bfloat16* output, const uint8_t* act, const uint32_t* as,
    const uint8_t* weights, const uint32_t* ws, const LargeRouteGroup* groups,
    const float* scale, uint32_t k, uint32_t n, uint32_t experts, const uint32_t* count
) {
    constexpr unsigned warps = 8;
    constexpr unsigned panel_rows = routes_per_large_group / 2;
    constexpr unsigned panels = panel_rows / routes_per_group;
    const unsigned tid = threadIdx.x;
    const unsigned warp = tid / warp_size;
    const unsigned lane = tid % warp_size;
    const unsigned quad = lane / 4;
    const unsigned side = lane % 4;
    const unsigned first = warp / 4 * panel_rows;
    const unsigned tile_col = warp % 4;
    if (blockIdx.y >= *count) return;
    const auto& group = groups[blockIdx.y];
    const unsigned expert = group.expert;
    __shared__ Nvfp4ExpertTile stages[2];
    __shared__ __align__(16) __nv_bfloat16 published[routes_per_large_group][64];
    float sums[panels][4] = {};
    if (expert < experts) {
        const unsigned tiles = k / 64;
        const size_t base = (size_t(expert) * (n / 16) + blockIdx.x * 4) * tiles;
        const uint4* w = reinterpret_cast<const uint4*>(weights) + base * warp_size;
        const uint32_t* s = ws + base * output_tile;
        prefetch_nvfp4_expert_tile(stages[0], group, act, as, w, s, tiles, 0);
        unsigned buffer = 0;
        for (unsigned t = 0; t < tiles; ++t) {
            if (t + 1 < tiles) {
                prefetch_nvfp4_expert_tile(stages[buffer ^ 1], group, act, as, w, s, tiles, t + 1);
                asm volatile("cp.async.wait_group 1;" ::: "memory");
            } else {
                asm volatile("cp.async.wait_group 0;" ::: "memory");
            }
            __syncthreads();
            const auto& stage = stages[buffer];
            const uint4 packed = stage.weights[tile_col][lane];
            const unsigned scale_row = quad + (side == 1 ? 8 : 0);
            const uint32_t sw = side < 2 ? stage.weight_scale[tile_col][scale_row] : 0;
            #pragma unroll
            for (unsigned i = 0; i < panels; ++i) {
                const unsigned slot = first + i * routes_per_group + quad;
                const uint32_t* a = reinterpret_cast<const uint32_t*>(stage.activation[slot]);
                block_dot_nvfp4(sums[i], packed, a, side, sw, stage.activation_scale[slot]);
            }
            __syncthreads();
            buffer ^= 1;
        }
    }
    #pragma unroll
    for (unsigned i = 0; i < panels; ++i) {
        const unsigned slot = first + i * routes_per_group + 2 * side;
        #pragma unroll
        for (unsigned j = 0; j < 2; ++j) {
            const unsigned row = group.rows[slot + j];
            if (row == UINT32_MAX) continue;
            const float outer = expert < experts ? scale[row] : 1.0f;
            published[slot + j][tile_col * output_tile + quad] = __float2bfloat16_rn(sums[i][j] * outer);
            published[slot + j][tile_col * output_tile + quad + 8] = __float2bfloat16_rn(sums[i][j + 2] * outer);
        }
    }
    __syncthreads();
    for (unsigned i = tid * 8; i < routes_per_large_group * 64; i += warps * warp_size * 8) {
        const unsigned row = group.rows[i / 64];
        if (row == UINT32_MAX) continue;
        auto* dst = reinterpret_cast<uint4*>(output + size_t(row) * n + blockIdx.x * 64u + i % 64);
        *dst = *reinterpret_cast<const uint4*>(&published[i / 64][i % 64]);
    }
}
