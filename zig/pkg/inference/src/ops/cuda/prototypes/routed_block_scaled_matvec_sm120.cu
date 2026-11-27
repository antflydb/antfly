// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

#include <cuda_bf16.h>
#include <stdint.h>
#include "../kernels/quantize_bf16_nvfp4.cuh"

namespace {
constexpr unsigned warp_size = 32;
constexpr unsigned output_tile = 16;
constexpr unsigned k_partitions = 4;

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
template<bool FP4>
__device__ uint32_t dequantized_bf16_pair(uint32_t packet, unsigned shift, float block_scale, float outer) {
    const uint32_t code = packet >> shift;
    float2 pair;
    if constexpr (FP4) {
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

template<bool FP4>
__device__ void project_bf16(
    __nv_bfloat16* output, const uint32_t* act,
    const uint4* weights, const uint8_t* weight_scales, const uint32_t* ids,
    const float* weight_amax, const float* denominator,
    uint32_t k, uint32_t n, uint32_t expert_count, uint32_t repeat
) {
    constexpr unsigned step = FP4 ? 64 : 32;
    constexpr unsigned scale_bytes = FP4 ? 4 : 1;
    const unsigned tid = threadIdx.x;
    const unsigned lane = tid % warp_size;
    const unsigned warp = tid / warp_size;
    const unsigned quad_lane = lane % 4;
    const unsigned quad = lane / 4;
    const unsigned column = blockIdx.x * output_tile;
    const unsigned row = blockIdx.y;
    const unsigned expert = ids[row];
    if (expert >= expert_count) {
        if (tid < output_tile) output[(size_t)row * n + column + tid] = __float2bfloat16_rn(0);
        return;
    }
    const unsigned tiles = k / step;
    const uint32_t* a = act + (size_t)(row / repeat) * (k / 2);
    const size_t tile_base = ((size_t)expert * (n / output_tile) + blockIdx.x) * tiles;
    const uint4* packed_tiles = weights + tile_base * warp_size;
    const uint8_t* scale_tiles = weight_scales + tile_base * output_tile * scale_bytes;
    const float outer = FP4 ? __fdiv_rn(weight_amax[expert], *denominator) : 1.0f;
    float sum[4] = {};
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
            bf16_mma(sum, dequantized_bf16_pair<FP4>(lo0, pair_shift, s0, outer),
                          dequantized_bf16_pair<FP4>(lo1, pair_shift, s1, outer),
                          dequantized_bf16_pair<FP4>(hi0, pair_shift, s0, outer),
                          dequantized_bf16_pair<FP4>(hi1, pair_shift, s1, outer),
                          a[b], a[b + 4]);
        }
    }
    __shared__ float partial[k_partitions][output_tile];
    if (quad_lane == 0) {
        partial[warp][quad] = sum[0];
        partial[warp][quad + 8] = sum[2];
    }
    __syncthreads();
    if (tid < output_tile) {
        const float value = (partial[0][tid] + partial[1][tid]) + (partial[2][tid] + partial[3][tid]);
        output[(size_t)row * n + column + tid] = __float2bfloat16_rn(value);
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
