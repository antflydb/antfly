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
#include <math.h>

namespace {
constexpr unsigned vectors_per_tile = 32;
constexpr unsigned high_exponent_base = 54;

// Eight weights per vector. Store all low bytes and four bits per high byte.
// Tiles with an exponent outside the encodable range retain their raw bits.
struct PackedWeights {
    uint2* low;
    uint32_t* high;
    uint32_t* tiles;
    uint4* spill;

    __device__ uint4 operator[](size_t index) const {
        const uint32_t id = tiles[index / vectors_per_tile];
        return id != 0 ? spilled(id, index % vectors_per_tile) : compact(index);
    }

    __device__ uint4 spilled(unsigned id, unsigned vector) const {
        return spill[(size_t)(id - 1) * vectors_per_tile + vector];
    }

    __device__ uint4 compact(size_t index) const {
        const uint2 lo = low[index];
        const uint32_t code = high[index];
        const uint32_t first = expand(code);
        const uint32_t last = expand(code >> 4);
        return make_uint4(__byte_perm(lo.x, first, 0x5140), __byte_perm(lo.x, first, 0x7362),
                          __byte_perm(lo.y, last, 0x5140), __byte_perm(lo.y, last, 0x7362));
    }

    static __device__ uint32_t expand(uint32_t code) {
        const uint32_t exponent = (code & 0x07070707u) + high_exponent_base * 0x01010101u;
        return exponent | ((code & 0x08080808u) << 4);
    }
};
static_assert(sizeof(PackedWeights) == 32, "packed BF16 view ABI");

// Store the packets read by one warp together, while keeping the spill
// offsets and raw spill packets in logical row order.
__device__ size_t interleaved_index(size_t row, unsigned vector, unsigned row_vectors, unsigned lanes) {
    const unsigned rows_per_warp = 32 / lanes;
    return (row / rows_per_warp) * row_vectors * rows_per_warp +
           (vector / lanes) * 32 + (row % rows_per_warp) * lanes + vector % lanes;
}

struct InputSegments {
    const uint4* first;
    const uint4* second;
    const uint4* third;
    unsigned first_end;
    unsigned second_end;
};
static_assert(sizeof(InputSegments) == 32, "BF16 input segment ABI");

__device__ const uint4* row_input(const uint4* input, unsigned) {
    return input;
}

__device__ const uint4* row_input(InputSegments input, unsigned row) {
    return row < input.first_end ? input.first : row < input.second_end ? input.second : input.third;
}

template<bool Store>
__device__ void pack_tiles(const uint4* raw, PackedWeights packed, unsigned tile_count, unsigned* spill_count, unsigned spill_limit,
                           unsigned row_vectors = 0, unsigned lanes = 32) {
    const unsigned tile = blockIdx.x * (blockDim.x / vectors_per_tile) + threadIdx.x / vectors_per_tile;
    if (tile >= tile_count) return;
    const unsigned lane = threadIdx.x % vectors_per_tile;
    const size_t index = (size_t)tile * vectors_per_tile + lane;
    const uint4 value = raw[index];
    const unsigned words[] = { value.x, value.y, value.z, value.w };
    bool fits = true;
    #pragma unroll
    for (unsigned word : words) {
        const unsigned a = (word >> 8) & 0x7fu;
        const unsigned b = (word >> 24) & 0x7fu;
        fits &= a - high_exponent_base < 8 && b - high_exponent_base < 8;
    }
    if (__any_sync(0xffffffffu, !fits)) {
        unsigned id = lane == 0 ? atomicAdd(spill_count, 1) : 0;
        if constexpr (Store) {
            id = __shfl_sync(0xffffffffu, id, 0);
            if (id >= spill_limit) return;
            if (lane == 0) packed.tiles[tile] = id + 1;
            packed.spill[(size_t)id * vectors_per_tile + lane] = value;
        }
        return;
    }
    if constexpr (Store) {
        if (lane == 0) packed.tiles[tile] = 0;
        uint2 low = {};
        uint32_t high = 0;
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i) {
            const unsigned word = words[i];
            const unsigned a = word >> 8 & 0xffu;
            const unsigned b = word >> 24;
            const unsigned code_a = ((a & 0x7fu) - high_exponent_base) | ((a & 0x80u) >> 4);
            const unsigned code_b = ((b & 0x7fu) - high_exponent_base) | ((b & 0x80u) >> 4);
            const unsigned shift = (i % 2) * 16;
            const unsigned lower_bytes = (word & 0xffu) | ((word >> 8) & 0xff00u);
            if (i < 2) low.x |= lower_bytes << shift;
            else low.y |= lower_bytes << shift;
            high |= (code_a | code_b << 8) << (shift + (i / 2) * 4);
        }
        const unsigned row_tiles = row_vectors / vectors_per_tile;
        const size_t target = lanes == 32 ? index :
            interleaved_index(tile / row_tiles, (tile % row_tiles) * vectors_per_tile + lane, row_vectors, lanes);
        packed.low[target] = low;
        packed.high[target] = high;
    }
}

__device__ void dot_pair(float& low, float& high, uint32_t a, uint32_t w) {
    low = __fmaf_rn(__uint_as_float(a << 16), __uint_as_float(w << 16), low);
    high = __fmaf_rn(__uint_as_float(a & 0xffff0000u), __uint_as_float(w & 0xffff0000u), high);
}

template<unsigned LaneCount, unsigned Width, typename Weights>
__device__ float dense_dot(const uint4* activation, Weights weights, unsigned row) {
    constexpr unsigned vectors = Width / 8;
    const unsigned lane = threadIdx.x % LaneCount;
    const size_t base = (size_t)row * vectors;
    float s0 = 0.0f, s1 = 0.0f, s2 = 0.0f, s3 = 0.0f;
    #pragma unroll 4
    for (unsigned j = lane; j < vectors; j += LaneCount) {
        const uint4 a = activation[j];
        const uint4 weight = weights[base + j];
        dot_pair(s0, s1, a.x, weight.x);
        dot_pair(s2, s3, a.y, weight.y);
        dot_pair(s0, s1, a.z, weight.z);
        dot_pair(s2, s3, a.w, weight.w);
    }
    float sum = (s0 + s1) + (s2 + s3);
    #pragma unroll
    for (unsigned offset = LaneCount / 2; offset != 0; offset /= 2)
        sum += __shfl_down_sync(__activemask(), sum, offset, LaneCount);
    return sum;
}

template<unsigned LaneCount, unsigned Width, typename Result, typename Input, typename Weights>
__device__ void dense(Result* out, Input input, Weights weights, unsigned n) {
    const unsigned row = blockIdx.x * (blockDim.x / LaneCount) + threadIdx.x / LaneCount;
    if (row >= n) return;
    const float sum = dense_dot<LaneCount, Width>(row_input(input, row), weights, row);
    if (threadIdx.x % LaneCount == 0) out[row] = Result(sum);
}

template<unsigned Lanes, unsigned Width, unsigned Batch = 1, typename Input>
__device__ void dense_interleaved(__nv_bfloat16* out, Input input, PackedWeights weights, unsigned n) {
    constexpr unsigned vectors = Width / 8;
    const unsigned lane = threadIdx.x % Lanes;
    const unsigned row = blockIdx.x * (blockDim.x / Lanes) + threadIdx.x / Lanes;
    if (row >= n) return;
    const uint4* activation = row_input(input, row);
    const auto* tile_ids = weights.tiles + (size_t)row * (vectors / vectors_per_tile);
    const size_t first_packet = interleaved_index(row, lane, vectors, Lanes);
    float s0[Batch] = {}, s1[Batch] = {}, s2[Batch] = {}, s3[Batch] = {};
    for (unsigned tile = 0; tile < vectors / vectors_per_tile; ++tile) {
        const unsigned id = tile_ids[tile];
        const unsigned base = tile * vectors_per_tile;
        #pragma unroll
        for (unsigned x = lane; x < vectors_per_tile; x += Lanes) {
            const uint4 first = activation[base + x];
            const uint4 w = id ? weights.spilled(id, x) : weights.compact(first_packet + (base + x) / Lanes * 32);
            #pragma unroll
            for (unsigned item = 0; item < Batch; ++item) {
                const uint4 a = item == 0 ? first : activation[item * vectors + base + x];
                dot_pair(s0[item], s1[item], a.x, w.x);
                dot_pair(s2[item], s3[item], a.y, w.y);
                dot_pair(s0[item], s1[item], a.z, w.z);
                dot_pair(s2[item], s3[item], a.w, w.w);
            }
        }
    }
    #pragma unroll
    for (unsigned item = 0; item < Batch; ++item) {
        float sum = (s0[item] + s1[item]) + (s2[item] + s3[item]);
        #pragma unroll
        for (unsigned offset = Lanes / 2; offset != 0; offset /= 2)
            sum += __shfl_down_sync(__activemask(), sum, offset, Lanes);
        if (lane == 0) out[(size_t)item * n + row] = __float2bfloat16_rn(sum);
    }
}
} // namespace

extern "C" __global__ void antfly_count_bf16_exponent_spill_tiles(
    const uint4* raw, unsigned tile_count, unsigned* spill_count
) {
    pack_tiles<false>(raw, {}, tile_count, spill_count, 0);
}

extern "C" __global__ void antfly_pack_bf16_exponent_tiles(
    const uint4* raw, const __grid_constant__ PackedWeights packed,
    unsigned tile_count, unsigned* spill_count, unsigned spill_limit, unsigned row_vectors, unsigned lanes
) {
    pack_tiles<true>(raw, packed, tile_count, spill_count, spill_limit, row_vectors, lanes);
}

extern "C" __global__ void antfly_dense_bf16_matvec_f32_2304(
    float* out, const uint4* input, const uint4* weights, unsigned n
) {
    dense<32, 2304>(out, input, weights, n);
}

extern "C" __global__ void antfly_dense_bf16_matvec_bf16_2304(
    __nv_bfloat16* out, const uint4* input, const uint4* weights, unsigned n
) {
    dense<8, 2304>(out, input, weights, n);
}

extern "C" __global__ void antfly_dense_bf16_matvec_bf16_3072(
    __nv_bfloat16* out, const uint4* input, const uint4* weights, unsigned n
) {
    dense<16, 3072>(out, input, weights, n);
}

extern "C" __global__ void antfly_dense_bf16_matvec_f32_64(
    float* out, const uint4* input, const uint4* weights, unsigned n
) {
    dense<4, 64>(out, input, weights, n);
}

struct Bf16WeightTriplet { const uint4* values[3]; };
static_assert(sizeof(Bf16WeightTriplet) == 24, "BF16 weight group ABI");

extern "C" __global__ void antfly_dense_bf16_matvec_three_f32_64(
    float* out, const uint4* input, const __grid_constant__ Bf16WeightTriplet weights, unsigned n
) {
    const unsigned row = blockIdx.y;
    dense<4, 64>(out + (size_t)row * n, input + row * 8, weights.values[row], n);
}

extern "C" __global__ void antfly_dense_packed_bf16_matvec_f32_2304(
    float* out, const uint4* input, const __grid_constant__ PackedWeights weights, unsigned n
) {
    dense<32, 2304>(out, input, weights, n);
}

extern "C" __global__ void antfly_dense_interleaved_bf16_matvec_bf16_2304(
    __nv_bfloat16* out, const uint4* input, const __grid_constant__ PackedWeights weights, unsigned n
) {
    dense_interleaved<8, 2304>(out, input, weights, n);
}

extern "C" __global__ void antfly_dense_interleaved_bf16_matvec_bf16_3072(
    __nv_bfloat16* out, const uint4* input, const __grid_constant__ PackedWeights weights, unsigned n
) {
    dense_interleaved<16, 3072>(out, input, weights, n);
}

extern "C" __global__ void antfly_dense_interleaved_bf16_matvec_batch_bf16_3072(
    __nv_bfloat16* out, const uint4* input, const __grid_constant__ PackedWeights weights, unsigned n, unsigned batch
) {
    switch (batch) {
        case 2: dense_interleaved<16, 3072, 2>(out, input, weights, n); break;
        case 3: dense_interleaved<16, 3072, 3>(out, input, weights, n); break;
        case 4: dense_interleaved<16, 3072, 4>(out, input, weights, n); break;
    }
}

extern "C" __global__ void antfly_dense_interleaved_bf16_segmented_matvec_bf16_2304(
    __nv_bfloat16* out, const __grid_constant__ InputSegments input,
    const __grid_constant__ PackedWeights weights, unsigned n
) {
    dense_interleaved<8, 2304>(out, input, weights, n);
}

namespace {
constexpr unsigned shortlist_width = 2304;
constexpr unsigned shortlist_vectors = shortlist_width / 16;
constexpr unsigned selection_warps = 8;
constexpr unsigned quantize_warps = 4;

struct QuantizationBounds {
    float scale;
    float norm;
    float quantized_norm;
    float error_norm;
};
static_assert(sizeof(QuantizationBounds) == 16, "quantization bounds ABI");

struct ShortlistWeights {
    uint2* low;
    QuantizationBounds* bounds;
};
static_assert(sizeof(ShortlistWeights) == 16, "shortlist weights ABI");

__device__ float warp_sum_upper(float value) {
    #pragma unroll
    for (unsigned offset = 16; offset != 0; offset /= 2)
        value = __fadd_ru(value, __shfl_down_sync(0xffffffffu, value, offset));
    return value;
}

// A zero scale selects a full BF16 scan. In the supported exponent range
// all nonzero BF16 products and their accumulation lattice are normal FP32.
// Upper rounding also encloses the Euclidean norms used by the filter.
template<bool PackedFour>
__device__ void quantize_shortlist_row(const __nv_bfloat16* input, uint2* low, uint4* bytes, QuantizationBounds* result) {
    const unsigned tid = threadIdx.x;
    const unsigned lane = tid % 32;
    const unsigned warp = tid / 32;
    float maximum = 0.0f;
    bool invalid = false;
    for (unsigned i = tid; i < shortlist_width; i += quantize_warps * 32) {
        const float magnitude = fabsf(__bfloat162float(input[i]));
        invalid |= !isfinite(magnitude) || magnitude > 0x1p16f || (magnitude != 0.0f && magnitude < 0x1p-56f);
        maximum = fmaxf(maximum, magnitude);
    }
    if (__syncthreads_or(invalid)) {
        if (tid == 0) *result = {};
        return;
    }

    __shared__ float partials[3][quantize_warps], scale;
    #pragma unroll
    for (unsigned offset = 16; offset != 0; offset /= 2)
        maximum = fmaxf(maximum, __shfl_down_sync(0xffffffffu, maximum, offset));
    if (lane == 0) partials[0][warp] = maximum;
    __syncthreads();
    constexpr int levels = PackedFour ? 7 : 127;
    if (tid == 0) {
        #pragma unroll
        for (unsigned i = 1; i < quantize_warps; ++i) maximum = fmaxf(maximum, partials[0][i]);
        if constexpr (PackedFour) {
            // A narrower symmetric interval improves typical precision. The
            // bounds below still account for the full residual of clipped values.
            scale = maximum == 0.0f ? 1.0f : maximum * (0.75f / levels);
        } else {
            int exponent = 0;
            if (maximum != 0.0f) frexpf(maximum / levels, &exponent);
            scale = ldexpf(1.0f, exponent);
        }
    }
    __syncthreads();

    float squared = 0.0f, quantized = 0.0f, error = 0.0f;
    for (unsigned i = tid; i < shortlist_vectors; i += quantize_warps * 32) {
        uint32_t mantissa[2] = {};
        uint32_t packets[4] = {};
        #pragma unroll
        for (unsigned packet = 0; packet < 4; ++packet) {
            #pragma unroll
            for (unsigned c = 0; c < 4; ++c) {
                const float x = __bfloat162float(input[i * 16 + packet * 4 + c]);
                const int q = max(-levels, min(levels, __float2int_rn(x / scale)));
                const float lower = __fmul_rd((float)q, scale);
                const float upper = __fmul_ru((float)q, scale);
                const float magnitude = fmaxf(fabsf(lower), fabsf(upper));
                const float diff = fmaxf(__fsub_ru(x, lower), __fsub_ru(upper, x));
                squared = __fmaf_ru(x, x, squared);
                quantized = __fmaf_ru(magnitude, magnitude, quantized);
                error = __fmaf_ru(diff, diff, error);
                if constexpr (PackedFour) {
                    mantissa[packet / 2] |= (q & 15u) << (c * 8 + packet % 2 * 4);
                } else {
                    packets[packet] |= (q & 255u) << (c * 8);
                }
            }
        }
        if constexpr (PackedFour) {
            low[i] = make_uint2(mantissa[0], mantissa[1]);
        } else {
            bytes[i] = make_uint4(packets[0], packets[1], packets[2], packets[3]);
        }
    }
    squared = warp_sum_upper(squared);
    quantized = warp_sum_upper(quantized);
    error = warp_sum_upper(error);
    if (lane == 0) {
        partials[0][warp] = squared;
        partials[1][warp] = quantized;
        partials[2][warp] = error;
    }
    __syncthreads();
    if (tid == 0) {
        #pragma unroll
        for (unsigned i = 1; i < quantize_warps; ++i) {
            squared = __fadd_ru(squared, partials[0][i]);
            quantized = __fadd_ru(quantized, partials[1][i]);
            error = __fadd_ru(error, partials[2][i]);
        }
        *result = {scale, __fsqrt_ru(squared), __fsqrt_ru(quantized), __fsqrt_ru(error)};
    }
}

__device__ uint32_t unpack_four(uint32_t packed) {
    // Sign-extend the low nibble in each byte for the signed DP4A dot.
    return (packed & 0x0f0f0f0fu) | ((packed & 0x08080808u) * 30u);
}

// Finite F32 values sort by the high word. Normalize signed zero so ties
// always favor the lower original row, including ties after soft-capping.
using ScoreKey = unsigned long long;
constexpr ScoreKey invalid_score = ~ScoreKey(0);

__device__ ScoreKey score_key(float score, unsigned row) {
    if (!isfinite(score)) return invalid_score;
    const unsigned bits = score == 0.0f ? 0u : __float_as_uint(score);
    const unsigned ordered = bits >> 31 ? ~bits : bits ^ 0x80000000u;
    return (ScoreKey(ordered) << 32) | ~row;
}

__device__ ScoreKey warp_best(ScoreKey key) {
    #pragma unroll
    for (unsigned offset = 16; offset != 0; offset /= 2)
        key = max(key, __shfl_down_sync(0xffffffffu, key, offset));
    return key;
}

__device__ ScoreKey best_partial(const ScoreKey* candidates, unsigned count) {
    ScoreKey best = 0;
    for (unsigned i = threadIdx.x; i < count; i += blockDim.x) best = max(best, candidates[i]);
    best = warp_best(best);
    __shared__ ScoreKey warps[selection_warps];
    const unsigned lane = threadIdx.x % 32;
    if (lane == 0) warps[threadIdx.x / 32] = best;
    __syncthreads();
    return warp_best(lane < selection_warps ? warps[lane] : 0);
}

__device__ bool row_in_warp(unsigned n, unsigned& row) {
    row = blockIdx.x * selection_warps + threadIdx.x / 32;
    return row < n;
}

__device__ void publish_warp_candidates(ScoreKey* output, ScoreKey key) {
    __shared__ ScoreKey warps[selection_warps];
    if (threadIdx.x % 32 == 0) warps[threadIdx.x / 32] = key;
    __syncthreads();
    if (threadIdx.x == 0) {
        #pragma unroll
        for (unsigned i = 1; i < selection_warps; ++i) key = max(key, warps[i]);
        output[blockIdx.x] = key;
    }
}

__device__ float cap_score(float score, float cap) {
    float result;
    asm("tanh.approx.f32 %0, %1;" : "=f"(result) : "f"(score / cap));
    return result * cap;
}
} // namespace

extern "C" __global__ void antfly_pack_q4_projection_bounds_2304(
    const __nv_bfloat16* raw, const __grid_constant__ ShortlistWeights weights, unsigned* invalid
) {
    const size_t row = blockIdx.x;
    quantize_shortlist_row<true>(raw + row * shortlist_width, weights.low + row * shortlist_vectors,
                                nullptr, weights.bounds + row);
    if (threadIdx.x == 0 && weights.bounds[row].scale == 0.0f) atomicExch(invalid, 1u);
}

extern "C" __global__ void antfly_quantize_q8_projection_bounds_2304(
    const __nv_bfloat16* input, uint4* bytes, QuantizationBounds* bounds
) {
    quantize_shortlist_row<false>(input, nullptr, bytes, bounds);
}

extern "C" __global__ void antfly_q4_q8_projection_upper_bounds_2304(
    float* upper, ScoreKey* partial, const uint4* input, const QuantizationBounds* input_bounds,
    const __grid_constant__ ShortlistWeights weights, unsigned n
) {
    const QuantizationBounds x = *input_bounds;
    if (x.scale == 0.0f) {
        if (threadIdx.x == 0) partial[blockIdx.x] = 0;
        return;
    }
    unsigned row;
    const unsigned lane = threadIdx.x % 32;
    ScoreKey best = 0;
    if (row_in_warp(n, row)) {
        const auto* low = weights.low + (size_t)row * shortlist_vectors;
        int acc0 = 0, acc1 = 0, acc2 = 0, acc3 = 0;
        #pragma unroll 2
        for (unsigned i = lane; i < shortlist_vectors; i += 32) {
            const uint4 a = input[i];
            const uint2 lo = low[i];
            acc0 = __dp4a((int)a.x, (int)unpack_four(lo.x), acc0);
            acc1 = __dp4a((int)a.y, (int)unpack_four(lo.x >> 4), acc1);
            acc2 = __dp4a((int)a.z, (int)unpack_four(lo.y), acc2);
            acc3 = __dp4a((int)a.w, (int)unpack_four(lo.y >> 4), acc3);
        }
        int sum = (acc0 + acc1) + (acc2 + acc3);
        #pragma unroll
        for (unsigned offset = 16; offset != 0; offset /= 2)
            sum += __shfl_down_sync(0xffffffffu, sum, offset);
        if (lane == 0) {
            const QuantizationBounds w = weights.bounds[row];
            // The integer dot fits in 24 bits. The input scale is a power of
            // two, so only the final multiplication can round the bound seed.
            const float score = (float)sum * (w.scale * x.scale);
            const float approximation = __fadd_ru(__fmul_ru(x.norm, w.error_norm), __fmul_ru(x.error_norm, w.quantized_norm));
            const float rounding = __fadd_ru(__fmul_ru(0.00015f, __fmul_ru(x.norm, w.norm)), __fmul_ru(0x1p-22f, fabsf(score)));
            upper[row] = __fadd_ru(score, __fadd_ru(approximation, rounding));
            best = score_key(score, row);
        }
    }
    publish_warp_candidates(partial, best);
}

extern "C" __global__ void antfly_bf16_projection_seed_score_2304(
    float* cutoff, const ScoreKey* partial, unsigned blocks, const uint4* input,
    const __grid_constant__ PackedWeights weights, float cap
) {
    const ScoreKey best = best_partial(partial, blocks);
    if (threadIdx.x >= 32) return;
    const ScoreKey candidate = __shfl_sync(0xffffffffu, best, 0);
    const bool valid = candidate != 0 && candidate != invalid_score;
    const float exact = valid ? dense_dot<32, shortlist_width>(input, weights, ~(unsigned)candidate) : -INFINITY;
    if (threadIdx.x == 0) *cutoff = valid ? cap_score(exact, cap) : -INFINITY;
}

extern "C" __global__ void antfly_bf16_projection_refine_candidates_2304(
    ScoreKey* partial, const uint4* input, const __grid_constant__ PackedWeights weights,
    const float* upper, const float* cutoff, unsigned n, float cap
) {
    constexpr unsigned tile_rows = selection_warps * 32;
    const unsigned first = blockIdx.x * tile_rows;
    const unsigned tid = threadIdx.x;
    const unsigned lane = tid % 32;
    const unsigned warp = tid / 32;
    const float seed = *cutoff;
    const bool have_seed = isfinite(seed);
    unsigned count = min(tile_rows, n - first);
    __shared__ unsigned candidate_count;
    __shared__ unsigned candidate_rows[tile_rows];
    if (have_seed) {
        if (tid == 0) candidate_count = 0;
        __syncthreads();
        // PTX bounds tanh.approx.f32 relative error by 2^-11. Two
        // evaluations plus the FP32 products fit inside this margin.
        // Keep equality for the lower-index tie rule.
        const bool keep = tid < count && __fadd_ru(cap_score(upper[first + tid], cap), __fmul_ru(cap, 0x1p-9f)) >= seed;
        const unsigned mask = __ballot_sync(0xffffffffu, keep);
        if (mask != 0) {
            const unsigned next = lane == 0 ? atomicAdd(&candidate_count, __popc(mask)) : 0;
            const unsigned base = __shfl_sync(0xffffffffu, next, 0);
            if (keep) candidate_rows[base + __popc(mask & ((1u << lane) - 1))] = first + tid;
        }
        __syncthreads();
        count = candidate_count;
    }
    ScoreKey key = 0;
    for (unsigned i = warp; i < count; i += selection_warps) {
        const unsigned row = have_seed ? candidate_rows[i] : first + i;
        const float exact = dense_dot<32, shortlist_width>(input, weights, row);
        if (lane == 0) key = max(key, score_key(cap_score(exact, cap), row));
    }
    publish_warp_candidates(partial, key);
}

extern "C" __global__ void antfly_projection_greedy_index(
    unsigned* output, const ScoreKey* partial, unsigned blocks
) {
    const ScoreKey best = best_partial(partial, blocks);
    if (threadIdx.x == 0) *output = best != 0 && best != invalid_score ? ~(unsigned)best : UINT32_MAX;
}
