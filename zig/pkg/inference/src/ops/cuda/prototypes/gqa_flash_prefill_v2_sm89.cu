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

// Standalone-only tiled GQA Flash-prefill experiment.  This translation
// unit is never included by production codegen or runtime dispatch.  It keeps
// the production 28-argument ABI so the existing guard/poison/determinism
// harness can compare it directly with the checked-in qualified Flash-v1
// cubin.  Query and key tiles are compile-time experiment parameters in
// {16,32,64}; WMMA, softmax, and PV still advance in 16-row subtiles,
// preserving Flash-v1's merge order.  Wider query tiles reuse each paged K/V
// load across multiple query subtiles; grouped-head variants additionally
// share that tile across 2 or 4 query heads of the locked 8Q:1KV contract.
// Loads remain synchronous in this geometry/resource stage; cp.async is a
// later experiment only after correctness and material speedup qualification.

#include <cuda_fp16.h>
#include <mma.h>
#include <math.h>
#include <stdint.h>
#include <stddef.h>

#ifndef ANTFLY_FLASH_V2_QUERY_TILE
#define ANTFLY_FLASH_V2_QUERY_TILE 32
#endif
#ifndef ANTFLY_FLASH_V2_KEY_TILE
#define ANTFLY_FLASH_V2_KEY_TILE 32
#endif
#ifndef ANTFLY_FLASH_V2_HEAD_GROUP
#define ANTFLY_FLASH_V2_HEAD_GROUP 1
#endif

// Reuse the qualified scalar reference implementation under the symbols the
// standalone harness already expects.  The accompanying Flash-v1 entry points
// are intentionally named "unused" and never selected by runtime.
#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_v2_reference_hd256
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_f16_sm89_hd256_swa512_f32_v1_unused
#define ANTFLY_FLASH_REFERENCE_KERNEL antfly_gqa_attention_prefill_reference_f16_hd256_swa512_f32_prototype
#define ANTFLY_FLASH_HEAD_DIM 256
#define ANTFLY_FLASH_SLIDING_WINDOW 512
#include "../../../graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh"
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_REFERENCE_KERNEL
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE

#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_v2_reference_hd512
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_f16_sm89_hd512_global_f32_v1_unused
#define ANTFLY_FLASH_REFERENCE_KERNEL antfly_gqa_attention_prefill_reference_f16_hd512_global_f32_prototype
#define ANTFLY_FLASH_HEAD_DIM 512
#define ANTFLY_FLASH_SLIDING_WINDOW 0
#include "../../../graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh"
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_REFERENCE_KERNEL
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE

namespace antfly_flash_prefill_v2 {

using namespace nvcuda;

constexpr unsigned kHeads = 8u;
constexpr unsigned kKvHeads = 1u;
constexpr unsigned kPageTokens = 16u;
constexpr unsigned kQuerySubtile = 16u;
constexpr unsigned kKeySubtile = 16u;
constexpr unsigned kQueryTile = ANTFLY_FLASH_V2_QUERY_TILE;
constexpr unsigned kKeyTile = ANTFLY_FLASH_V2_KEY_TILE;
constexpr unsigned kHeadGroup = ANTFLY_FLASH_V2_HEAD_GROUP;
constexpr unsigned kQuerySubtiles = kQueryTile / kQuerySubtile;
constexpr unsigned kKeySubtiles = kKeyTile / kKeySubtile;
constexpr unsigned kThreads = 256u;
constexpr unsigned kWarps = kThreads / 32u;
constexpr unsigned kInvalidToken = 0xffffffffu;
constexpr float kNegativeInfinity = -3.402823466e+38F;

static_assert(kQueryTile == 16u || kQueryTile == 32u || kQueryTile == 64u,
    "query tile must be 16, 32, or 64");
static_assert(kKeyTile == 16u || kKeyTile == 32u || kKeyTile == 64u,
    "key tile must be 16, 32, or 64");
static_assert(kHeadGroup == 1u || kHeadGroup == 2u || kHeadGroup == 4u,
    "head group must be 1, 2, or 4");
static_assert(kHeads % kHeadGroup == 0u, "head group must divide query heads");

__device__ __forceinline__ unsigned ceil_div(unsigned value, unsigned divisor) {
    return value / divisor + (value % divisor != 0u ? 1u : 0u);
}

__device__ __forceinline__ unsigned physical_token(
    unsigned logical_token,
    const unsigned* block_table,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned physical_token_capacity
) {
    unsigned long long physical = logical_token;
    if (block_table != nullptr) {
        const unsigned logical_block = logical_token / page_size_tokens;
        if (logical_block >= block_count) return kInvalidToken;
        physical = static_cast<unsigned long long>(block_table[logical_block]) *
                page_size_tokens +
            logical_token % page_size_tokens;
    }
    return physical < physical_token_capacity
        ? static_cast<unsigned>(physical)
        : kInvalidToken;
}

struct VisibleRange {
    unsigned begin;
    unsigned end;
};

template <int SlidingWindow>
__device__ __forceinline__ VisibleRange visible_range(
    unsigned query_pos,
    unsigned kv_seq_len,
    unsigned kv_position_offset
) {
    VisibleRange result{0u, 0u};
    if (query_pos < kv_position_offset) return result;
    const unsigned visible = query_pos - kv_position_offset + 1u;
    result.end = visible < kv_seq_len ? visible : kv_seq_len;
    if constexpr (SlidingWindow != 0) {
        const unsigned window_begin_abs = query_pos + 1u > SlidingWindow
            ? query_pos + 1u - SlidingWindow
            : 0u;
        if (window_begin_abs > kv_position_offset) {
            result.begin = window_begin_abs - kv_position_offset;
            if (result.begin > result.end) result.begin = result.end;
        }
    }
    return result;
}

template <int HeadDim, int SlidingWindow>
__device__ void body(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned mask_len,
    unsigned bias_mode,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned format,
    unsigned value_format,
    unsigned physical_token_capacity,
    const unsigned* decode_scalars,
    unsigned char* storage
) {
    static_assert(HeadDim == 256 || HeadDim == 512, "unsupported head dimension");
    constexpr unsigned kOutputTiles = HeadDim / 16u;
    constexpr unsigned kFragmentsPerWarp = kOutputTiles / kWarps;
    static_assert(kOutputTiles % kWarps == 0, "output tiles must divide warps");

    half* q_shared = reinterpret_cast<half*>(storage);
    half* kv_shared = q_shared + kHeadGroup * kQueryTile * HeadDim;
    float* warp_scratch = reinterpret_cast<float*>(kv_shared + kKeyTile * HeadDim);
    float* scores = warp_scratch + kWarps * kQuerySubtile * kKeySubtile;
    half* probabilities = reinterpret_cast<half*>(scores + kQuerySubtile * kKeySubtile);
    unsigned* physical = reinterpret_cast<unsigned*>(
        probabilities + kHeadGroup * kQueryTile * kKeyTile);
    unsigned* row_begin = physical + kKeyTile;
    unsigned* row_end = row_begin + kQueryTile;
    float* running_max = reinterpret_cast<float*>(row_end + kQueryTile);
    float* running_denom = running_max + kHeadGroup * kQueryTile;
    float* row_alpha = running_denom + kHeadGroup * kQueryTile;
    float* row_beta = row_alpha + kHeadGroup * kQueryTile * kKeySubtiles;
    unsigned* shared_error = reinterpret_cast<unsigned*>(
        row_beta + kHeadGroup * kQueryTile * kKeySubtiles);

    const unsigned tid = threadIdx.x;
    const unsigned warp = tid >> 5u;
    const unsigned lane = tid & 31u;
    if (tid == 0u) {
        const bool table_present = block_table != nullptr;
        const bool count_present = block_count != 0u;
        const bool supported_prefix = q_seq_len == 512u
            ? (query_position_offset == 0u || query_position_offset == 512u ||
                  query_position_offset == 1024u || query_position_offset == 1536u)
            : query_position_offset == 2048u;
        *shared_error =
            dst == nullptr || q == nullptr || k == nullptr || v == nullptr ||
                batch != 1u || num_heads != kHeads || num_kv_heads != kKvHeads ||
                head_dim != HeadDim || (q_seq_len != 512u && q_seq_len != 3u) ||
                kv_seq_len != query_position_offset + q_seq_len || !supported_prefix ||
                kv_position_offset != 0u || total_sequence_len != kv_seq_len ||
                sliding_window != SlidingWindow || mask_len != 0u || bias_mode != 0u ||
                key_row_bytes != HeadDim * sizeof(half) ||
                base_key_row_bytes != key_row_bytes || value_row_bytes != key_row_bytes ||
                page_size_tokens != kPageTokens || format != 2u || value_format != 2u ||
                decode_scalars != nullptr || table_present != count_present ||
                physical_token_capacity == 0u ||
                blockDim.x != kThreads || blockDim.y != 1u || blockDim.z != 1u ||
                gridDim.x != kHeads / kHeadGroup ||
                gridDim.y != ceil_div(q_seq_len, kQueryTile) ||
                gridDim.z != 1u
            ? 1u
            : 0u;
    }
    __syncthreads();
    if (*shared_error != 0u) return;

    const unsigned first_head = blockIdx.x * kHeadGroup;
    const unsigned tile_query_begin = blockIdx.y * kQueryTile;
    const unsigned valid_rows = q_seq_len - tile_query_begin < kQueryTile
        ? q_seq_len - tile_query_begin
        : kQueryTile;

    for (unsigned index = tid;
         index < kHeadGroup * kQueryTile * HeadDim;
         index += kThreads) {
        const unsigned head_in_group = index / (kQueryTile * HeadDim);
        const unsigned head_index = index - head_in_group * kQueryTile * HeadDim;
        const unsigned row = head_index / HeadDim;
        const unsigned column = head_index - row * HeadDim;
        q_shared[index] = row < valid_rows
            ? __float2half_rn(q[(tile_query_begin + row) * kHeads * HeadDim +
                  (first_head + head_in_group) * HeadDim + column])
            : __float2half_rn(0.0f);
    }
    if (tid < kQueryTile) {
        if (tid < valid_rows) {
            const VisibleRange range = visible_range<SlidingWindow>(
                query_position_offset + tile_query_begin + tid,
                kv_seq_len,
                kv_position_offset);
            row_begin[tid] = range.begin;
            row_end[tid] = range.end;
        } else {
            row_begin[tid] = 0u;
            row_end[tid] = 0u;
        }
    }
    for (unsigned index = tid; index < kHeadGroup * kQueryTile; index += kThreads) {
        running_max[index] = kNegativeInfinity;
        running_denom[index] = 0.0f;
    }
    __syncthreads();

    if (tid == 0u) {
        unsigned union_begin = kv_seq_len;
        unsigned union_end = 0u;
        for (unsigned row = 0u; row < valid_rows; ++row) {
            if (row_begin[row] < row_end[row]) {
                if (row_begin[row] < union_begin) union_begin = row_begin[row];
                if (row_end[row] > union_end) union_end = row_end[row];
            }
        }
        if (union_end == 0u) union_begin = 0u;
        physical[0] = (union_begin / kKeySubtile) * kKeySubtile;
        physical[1] = ceil_div(union_end, kKeySubtile) * kKeySubtile;
    }
    __syncthreads();
    const unsigned first_key_tile = physical[0];
    const unsigned final_key_tile = physical[1];

    wmma::fragment<wmma::accumulator, 16, 16, 16, float>
        output_fragments[kHeadGroup][kQuerySubtiles][kFragmentsPerWarp];
#pragma unroll
    for (unsigned head_in_group = 0u; head_in_group < kHeadGroup; ++head_in_group) {
#pragma unroll
        for (unsigned query_subtile = 0u; query_subtile < kQuerySubtiles; ++query_subtile) {
#pragma unroll
            for (unsigned fragment_index = 0u;
                 fragment_index < kFragmentsPerWarp;
                 ++fragment_index) {
                wmma::fill_fragment(
                    output_fragments[head_in_group][query_subtile][fragment_index], 0.0f);
            }
        }
    }

    for (unsigned key_tile_begin = first_key_tile;
         key_tile_begin < final_key_tile;
         key_tile_begin += kKeyTile) {
        if (tid < kKeyTile) {
            const unsigned logical_token = key_tile_begin + tid;
            physical[tid] = logical_token < kv_seq_len
                ? physical_token(logical_token, block_table, block_count,
                    page_size_tokens, physical_token_capacity)
                : kInvalidToken;
            if (logical_token < kv_seq_len && physical[tid] == kInvalidToken) {
                atomicCAS(shared_error, 0u, 2u);
            }
        }
        __syncthreads();
        if (*shared_error != 0u) return;

        for (unsigned index = tid; index < kKeyTile * HeadDim; index += kThreads) {
            const unsigned key_row = index / HeadDim;
            const unsigned column = index - key_row * HeadDim;
            const unsigned token = physical[key_row];
            kv_shared[index] = token != kInvalidToken
                ? reinterpret_cast<const half*>(
                      k + static_cast<size_t>(token) * key_row_bytes)[column]
                : __float2half_rn(0.0f);
        }
        __syncthreads();

#pragma unroll
        for (unsigned head_in_group = 0u;
             head_in_group < kHeadGroup;
             ++head_in_group) {
#pragma unroll
            for (unsigned query_subtile = 0u;
                 query_subtile < kQuerySubtiles;
                 ++query_subtile) {
#pragma unroll
                for (unsigned key_subtile = 0u;
                     key_subtile < kKeySubtiles;
                     ++key_subtile) {
                wmma::fragment<wmma::accumulator, 16, 16, 16, float> score_fragment;
                wmma::fill_fragment(score_fragment, 0.0f);
#pragma unroll
                for (unsigned dimension = warp * 16u;
                     dimension < HeadDim;
                     dimension += kWarps * 16u) {
                    wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major>
                        query_fragment;
                    wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major>
                        key_fragment;
                    wmma::load_matrix_sync(
                        query_fragment,
                        q_shared + head_in_group * kQueryTile * HeadDim +
                            query_subtile * kQuerySubtile * HeadDim + dimension,
                        HeadDim);
                    wmma::load_matrix_sync(
                        key_fragment,
                        kv_shared + key_subtile * kKeySubtile * HeadDim + dimension,
                        HeadDim);
                    wmma::mma_sync(
                        score_fragment, query_fragment, key_fragment, score_fragment);
                }
                wmma::store_matrix_sync(
                    warp_scratch + warp * kQuerySubtile * kKeySubtile,
                    score_fragment,
                    kKeySubtile,
                    wmma::mem_row_major);
                __syncthreads();
                for (unsigned index = tid;
                     index < kQuerySubtile * kKeySubtile;
                     index += kThreads) {
                    float score = 0.0f;
#pragma unroll
                    for (unsigned source_warp = 0u; source_warp < kWarps; ++source_warp) {
                        score += warp_scratch[
                            source_warp * kQuerySubtile * kKeySubtile + index];
                    }
                    scores[index] = score;
                }
                __syncthreads();

                if (tid < kQuerySubtile) {
                    const unsigned row = query_subtile * kQuerySubtile + tid;
                    const unsigned row_state = head_in_group * kQueryTile + row;
                    const float scale = rsqrtf(static_cast<float>(HeadDim));
                    float tile_max = kNegativeInfinity;
                    bool tile_has_value = false;
#pragma unroll
                    for (unsigned column = 0u; column < kKeySubtile; ++column) {
                        const unsigned key_column = key_subtile * kKeySubtile + column;
                        const unsigned logical_token = key_tile_begin + key_column;
                        const bool visible = row < valid_rows && logical_token < kv_seq_len &&
                            logical_token >= row_begin[row] && logical_token < row_end[row] &&
                            physical[key_column] != kInvalidToken;
                        if (visible) {
                            tile_max = fmaxf(tile_max, scores[tid * kKeySubtile + column] * scale);
                            tile_has_value = true;
                        }
                    }
                    float tile_denom = 0.0f;
#pragma unroll
                    for (unsigned column = 0u; column < kKeySubtile; ++column) {
                        const unsigned key_column = key_subtile * kKeySubtile + column;
                        const unsigned logical_token = key_tile_begin + key_column;
                        const bool visible = tile_has_value && row < valid_rows &&
                            logical_token < kv_seq_len && logical_token >= row_begin[row] &&
                            logical_token < row_end[row] && physical[key_column] != kInvalidToken;
                        const float probability = visible
                            ? expf(scores[tid * kKeySubtile + column] * scale - tile_max)
                            : 0.0f;
                        probabilities[row_state * kKeyTile + key_column] =
                            __float2half_rn(probability);
                        tile_denom += probability;
                    }
                    const unsigned merge_index = head_in_group * kQueryTile * kKeySubtiles +
                        (query_subtile * kKeySubtiles + key_subtile) * kQuerySubtile + tid;
                    if (tile_has_value) {
                        const float old_max = running_max[row_state];
                        const float old_denom = running_denom[row_state];
                        const float next_max = fmaxf(old_max, tile_max);
                        const float alpha = old_denom > 0.0f ? expf(old_max - next_max) : 0.0f;
                        const float beta = expf(tile_max - next_max);
                        running_max[row_state] = next_max;
                        running_denom[row_state] = old_denom * alpha + tile_denom * beta;
                        row_alpha[merge_index] = alpha;
                        row_beta[merge_index] = beta;
                    } else {
                        row_alpha[merge_index] = 1.0f;
                        row_beta[merge_index] = 0.0f;
                    }
                }
                __syncthreads();
            }
        }
        }

        for (unsigned index = tid; index < kKeyTile * HeadDim; index += kThreads) {
            const unsigned key_row = index / HeadDim;
            const unsigned column = index - key_row * HeadDim;
            const unsigned token = physical[key_row];
            kv_shared[index] = token != kInvalidToken
                ? reinterpret_cast<const half*>(
                      v + static_cast<size_t>(token) * value_row_bytes)[column]
                : __float2half_rn(0.0f);
        }
        __syncthreads();

#pragma unroll
        for (unsigned head_in_group = 0u;
             head_in_group < kHeadGroup;
             ++head_in_group) {
#pragma unroll
            for (unsigned query_subtile = 0u;
                 query_subtile < kQuerySubtiles;
                 ++query_subtile) {
#pragma unroll
                for (unsigned key_subtile = 0u;
                     key_subtile < kKeySubtiles;
                     ++key_subtile) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major>
                    probability_fragment;
                wmma::load_matrix_sync(
                    probability_fragment,
                    probabilities + head_in_group * kQueryTile * kKeyTile +
                        query_subtile * kQuerySubtile * kKeyTile +
                        key_subtile * kKeySubtile,
                    kKeyTile);
                const unsigned merge_base = head_in_group * kQueryTile * kKeySubtiles +
                    (query_subtile * kKeySubtiles + key_subtile) * kQuerySubtile;
                float* const scratch =
                    warp_scratch + warp * kQuerySubtile * kKeySubtile;
#pragma unroll
                for (unsigned fragment_index = 0u;
                     fragment_index < kFragmentsPerWarp;
                     ++fragment_index) {
                    const unsigned output_tile = warp + fragment_index * kWarps;
                    wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major>
                        value_fragment;
                    wmma::fragment<wmma::accumulator, 16, 16, 16, float>
                        tile_output_fragment;
                    wmma::load_matrix_sync(
                        value_fragment,
                        kv_shared + key_subtile * kKeySubtile * HeadDim +
                            output_tile * 16u,
                        HeadDim);
                    wmma::fill_fragment(tile_output_fragment, 0.0f);
                    wmma::mma_sync(
                        tile_output_fragment,
                        probability_fragment,
                        value_fragment,
                        tile_output_fragment);

                    wmma::store_matrix_sync(
                        scratch,
                        output_fragments[head_in_group][query_subtile][fragment_index],
                        16,
                        wmma::mem_row_major);
                    __syncwarp();
                    for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                        scratch[index] *= row_alpha[merge_base + index / 16u];
                    }
                    __syncwarp();
                    wmma::load_matrix_sync(
                        output_fragments[head_in_group][query_subtile][fragment_index],
                        scratch,
                        16,
                        wmma::mem_row_major);

                    wmma::store_matrix_sync(
                        scratch, tile_output_fragment, 16, wmma::mem_row_major);
                    __syncwarp();
                    for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                        scratch[index] *= row_beta[merge_base + index / 16u];
                    }
                    __syncwarp();
                    wmma::load_matrix_sync(
                        tile_output_fragment, scratch, 16, wmma::mem_row_major);
#pragma unroll
                    for (unsigned element = 0u;
                         element < output_fragments[head_in_group][query_subtile][fragment_index].num_elements;
                         ++element) {
                        output_fragments[head_in_group][query_subtile][fragment_index].x[element] +=
                            tile_output_fragment.x[element];
                    }
                }
            }
        }
        }
        __syncthreads();
    }

#pragma unroll
    for (unsigned head_in_group = 0u; head_in_group < kHeadGroup; ++head_in_group) {
#pragma unroll
        for (unsigned query_subtile = 0u; query_subtile < kQuerySubtiles; ++query_subtile) {
#pragma unroll
            for (unsigned fragment_index = 0u;
                 fragment_index < kFragmentsPerWarp;
                 ++fragment_index) {
                const unsigned output_tile = warp + fragment_index * kWarps;
                float* const scratch = warp_scratch + warp * kQuerySubtile * kKeySubtile;
                wmma::store_matrix_sync(
                    scratch,
                    output_fragments[head_in_group][query_subtile][fragment_index],
                    16,
                    wmma::mem_row_major);
                __syncwarp();
                for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                    const unsigned row_in_subtile = index / 16u;
                    const unsigned column = index - row_in_subtile * 16u;
                    const unsigned row = query_subtile * kQuerySubtile + row_in_subtile;
                    if (row < valid_rows) {
                        const float denominator =
                            running_denom[head_in_group * kQueryTile + row];
                        const size_t output_index =
                            static_cast<size_t>(tile_query_begin + row) * kHeads * HeadDim +
                            (first_head + head_in_group) * HeadDim +
                            output_tile * 16u + column;
                        dst[output_index] = denominator > 0.0f
                            ? scratch[index] / denominator
                            : 0.0f;
                    }
                }
                __syncwarp();
            }
        }
    }
}

} // namespace antfly_flash_prefill_v2

#define ANTFLY_V2_ARGUMENTS \
    float* dst, \
    const float* q, \
    const unsigned char* k, \
    const unsigned char* v, \
    const unsigned* block_table, \
    const unsigned char* attn_or_mask, \
    const float* bias, \
    unsigned batch, \
    unsigned q_seq_len, \
    unsigned kv_seq_len, \
    unsigned num_heads, \
    unsigned num_kv_heads, \
    unsigned head_dim, \
    unsigned query_position_offset, \
    unsigned kv_position_offset, \
    unsigned sliding_window, \
    unsigned total_sequence_len, \
    unsigned mask_len, \
    unsigned bias_mode, \
    unsigned key_row_bytes, \
    unsigned base_key_row_bytes, \
    unsigned value_row_bytes, \
    unsigned block_count, \
    unsigned page_size_tokens, \
    unsigned format, \
    unsigned value_format, \
    unsigned physical_token_capacity, \
    const unsigned* decode_scalars

#define ANTFLY_V2_FORWARD \
    dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len, num_heads, \
    num_kv_heads, head_dim, query_position_offset, kv_position_offset, \
    sliding_window, total_sequence_len, mask_len, bias_mode, key_row_bytes, \
    base_key_row_bytes, value_row_bytes, block_count, page_size_tokens, format, \
    value_format, physical_token_capacity, decode_scalars

extern "C" __global__ void
antfly_gqa_attention_prefill_flash_f16_sm89_hd256_swa512_f32_prototype(
    ANTFLY_V2_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    extern __shared__ __align__(16) unsigned char storage[];
    antfly_flash_prefill_v2::body<256, 512>(ANTFLY_V2_FORWARD, storage);
}

extern "C" __global__ void
antfly_gqa_attention_prefill_flash_f16_sm89_hd512_global_f32_prototype(
    ANTFLY_V2_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    extern __shared__ __align__(16) unsigned char storage[];
    antfly_flash_prefill_v2::body<512, 0>(ANTFLY_V2_FORWARD, storage);
}

#undef ANTFLY_V2_FORWARD
#undef ANTFLY_V2_ARGUMENTS
#undef ANTFLY_FLASH_V2_HEAD_GROUP
#undef ANTFLY_FLASH_V2_KEY_TILE
#undef ANTFLY_FLASH_V2_QUERY_TILE
