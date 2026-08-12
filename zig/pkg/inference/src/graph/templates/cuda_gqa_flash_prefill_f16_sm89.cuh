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

// Typed, default-off SM89 paged-GQA flash-prefill template.
//
// The typed CUDA renderer supplies a unique namespace, one production entry
// point, head dimension, and attention policy before expanding this template.
// The standalone qualification wrapper uses the same template and additionally
// requests a scalar reference entry point, keeping qualification and runtime
// code byte-identical below the exported ABI.
//
// Candidate algorithm:
//   * one CTA owns one query head and a 16-query tile;
//   * WMMA m16n16k16 computes F16 QK^T into F32 score accumulators;
//   * causal/SWA masking and online softmax are evaluated per query row;
//   * normalized tile probabilities are rounded to F16 and WMMA computes PV;
//   * the output numerator and softmax denominator remain F32 across tiles.
//
// Supported contracts are deliberately narrow:
//   * SM89, batch=1, query heads=8, KV heads=1;
//   * q_seq_len=512 or the production tail q_seq_len=3;
//   * HD256 with sliding_window=512, or HD512 with global attention;
//   * contiguous F32 Q and paged F16 K/V, F32 output;
//   * 16-token pages, either null/identity or explicit page tables.
//
// The exported entry point uses the production 28-argument paged-GQA ABI. The
// candidate never calls a fallback. Optional/required routing is host-owned.

#include <cuda_fp16.h>
#include <mma.h>
#include <stdint.h>
#include <stddef.h>
#include <math.h>

#ifndef ANTFLY_FLASH_NAMESPACE
#error "ANTFLY_FLASH_NAMESPACE must name a unique renderer-owned namespace"
#endif
#ifndef ANTFLY_FLASH_KERNEL
#error "ANTFLY_FLASH_KERNEL must name the exported production entry point"
#endif
#ifndef ANTFLY_FLASH_HEAD_DIM
#error "ANTFLY_FLASH_HEAD_DIM must be 256 or 512"
#endif
#ifndef ANTFLY_FLASH_SLIDING_WINDOW
#error "ANTFLY_FLASH_SLIDING_WINDOW must be 512 for HD256 or 0 for HD512"
#endif

namespace ANTFLY_FLASH_NAMESPACE {

using namespace nvcuda;

constexpr unsigned kHeads = 8u;
constexpr unsigned kKvHeads = 1u;
constexpr unsigned kPageTokens = 16u;
constexpr unsigned kQueryTile = 16u;
constexpr unsigned kKeyTile = 16u;
constexpr unsigned kThreads = 256u;
constexpr unsigned kWarps = kThreads / 32u;
constexpr unsigned kInvalidToken = 0xffffffffu;
constexpr float kNegativeInfinity = -3.402823466e+38F;

enum PrototypeStatus : unsigned {
    kStatusOk = 0u,
    kStatusNullPointer = 1u,
    kStatusMisalignedPointer = 2u,
    kStatusLaunchGeometry = 3u,
    kStatusUnsupportedShape = 4u,
    kStatusUnsupportedPolicy = 5u,
    kStatusInvalidStride = 6u,
    kStatusPositionOverflow = 7u,
    kStatusInvalidPageContract = 8u,
    kStatusMappedPageOutOfBounds = 9u,
    kStatusAddressOverflow = 10u,
};

__device__ __forceinline__ bool pointer_aligned(const void* ptr, uintptr_t alignment) {
    return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1u)) == 0u;
}

__device__ __forceinline__ unsigned ceil_div_u32(unsigned value, unsigned divisor) {
    return value / divisor + (value % divisor != 0u ? 1u : 0u);
}

__device__ __forceinline__ unsigned physical_token(
    unsigned logical_token,
    const unsigned* block_table,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned physical_token_capacity
) {
    unsigned long long physical = static_cast<unsigned long long>(logical_token);
    if (block_table != nullptr) {
        const unsigned logical_block = logical_token / page_size_tokens;
        if (logical_block >= block_count) return kInvalidToken;
        const unsigned token_offset = logical_token - logical_block * page_size_tokens;
        physical = static_cast<unsigned long long>(block_table[logical_block]) *
                static_cast<unsigned long long>(page_size_tokens) +
            static_cast<unsigned long long>(token_offset);
    }
    if (physical > static_cast<unsigned long long>(0xffffffffu) ||
        physical >= static_cast<unsigned long long>(physical_token_capacity)) {
        return kInvalidToken;
    }
    return static_cast<unsigned>(physical);
}

struct VisibleRange {
    unsigned begin;
    unsigned end;
};

__device__ __forceinline__ VisibleRange visible_range(
    unsigned query_pos,
    unsigned kv_seq_len,
    unsigned kv_position_offset,
    unsigned sliding_window
) {
    VisibleRange result{0u, 0u};
    if (query_pos < kv_position_offset) return result;
    const unsigned visible = query_pos - kv_position_offset + 1u;
    result.end = visible < kv_seq_len ? visible : kv_seq_len;
    if (sliding_window != 0u) {
        const unsigned window_start_abs = query_pos + 1u > sliding_window
            ? query_pos + 1u - sliding_window
            : 0u;
        if (window_start_abs > kv_position_offset) {
            result.begin = window_start_abs - kv_position_offset;
            if (result.begin > result.end) result.begin = result.end;
        }
    }
    return result;
}

template <int HeadDim, int SlidingWindow, bool Flash>
__device__ bool validate_launch(
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
    unsigned* shared_error
) {
    if (threadIdx.x == 0u) {
        unsigned error = kStatusOk;
        if (dst == nullptr || q == nullptr || k == nullptr || v == nullptr) {
            error = kStatusNullPointer;
        } else if (!pointer_aligned(dst, alignof(float)) ||
            !pointer_aligned(q, alignof(float)) ||
            !pointer_aligned(k, alignof(half)) ||
            !pointer_aligned(v, alignof(half)) ||
            (block_table != nullptr && !pointer_aligned(block_table, alignof(unsigned)))) {
            error = kStatusMisalignedPointer;
        } else {
            const unsigned expected_grid_y = Flash
                ? ceil_div_u32(q_seq_len, kQueryTile)
                : q_seq_len;
            const unsigned expected_threads = Flash ? kThreads : static_cast<unsigned>(HeadDim);
            if (blockDim.x != expected_threads || blockDim.y != 1u || blockDim.z != 1u ||
                gridDim.x != kHeads || gridDim.y != expected_grid_y || gridDim.z != 1u) {
                error = kStatusLaunchGeometry;
            } else if (batch != 1u || num_heads != kHeads || num_kv_heads != kKvHeads ||
                head_dim != static_cast<unsigned>(HeadDim) ||
                (q_seq_len != 512u && q_seq_len != 3u) ||
                kv_seq_len == 0u || kv_seq_len > 4096u) {
                error = kStatusUnsupportedShape;
            } else {
                const bool supported_prefix = q_seq_len == 512u
                    ? (query_position_offset == 0u || query_position_offset == 512u ||
                          query_position_offset == 1024u || query_position_offset == 1536u)
                    : query_position_offset == 2048u;
                if (!supported_prefix || kv_position_offset != 0u ||
                    kv_seq_len != query_position_offset + q_seq_len ||
                    total_sequence_len != kv_seq_len) {
                    error = kStatusUnsupportedShape;
                }
            }
            if (error == kStatusOk && sliding_window != static_cast<unsigned>(SlidingWindow)) {
                error = kStatusUnsupportedPolicy;
            } else if (error == kStatusOk && (mask_len != 0u || bias_mode != 0u || format != 2u ||
                value_format != 2u || decode_scalars != nullptr)) {
                error = kStatusUnsupportedPolicy;
            } else if (error == kStatusOk && (key_row_bytes != static_cast<unsigned>(HeadDim) * sizeof(half) ||
                base_key_row_bytes != key_row_bytes || value_row_bytes != key_row_bytes)) {
                error = kStatusInvalidStride;
            } else if (error == kStatusOk && (query_position_offset > 0xffffffffu - q_seq_len ||
                kv_position_offset > 0xffffffffu - kv_seq_len ||
                query_position_offset + q_seq_len > total_sequence_len ||
                kv_position_offset + kv_seq_len > total_sequence_len)) {
                error = kStatusPositionOverflow;
            } else if (error == kStatusOk) {
                const bool table_present = block_table != nullptr;
                const bool count_present = block_count != 0u;
                const unsigned required_blocks = ceil_div_u32(kv_seq_len, kPageTokens);
                if (page_size_tokens != kPageTokens || table_present != count_present ||
                    physical_token_capacity == 0u ||
                    physical_token_capacity % kPageTokens != 0u ||
                    (!table_present && physical_token_capacity < kv_seq_len) ||
                    (table_present && block_count < required_blocks)) {
                    error = kStatusInvalidPageContract;
                } else {
                    const unsigned long long q_elements =
                        static_cast<unsigned long long>(q_seq_len) * kHeads * HeadDim;
                    const unsigned long long kv_elements =
                        static_cast<unsigned long long>(physical_token_capacity) * HeadDim;
                    const unsigned long long output_elements = q_elements;
                    if (q_elements > 0x7fffffffffffffffull / sizeof(float) ||
                        kv_elements > 0x7fffffffffffffffull / sizeof(half) ||
                        output_elements > 0x7fffffffffffffffull / sizeof(float)) {
                        error = kStatusAddressOverflow;
                    }
                }
            }
        }
        *shared_error = error;
    }
    __syncthreads();
    if (*shared_error != kStatusOk) {
        return false;
    }

    if (block_table != nullptr) {
        const unsigned required_blocks = ceil_div_u32(kv_seq_len, kPageTokens);
        for (unsigned logical_block = threadIdx.x;
             logical_block < required_blocks;
             logical_block += blockDim.x) {
            const unsigned long long physical_begin =
                static_cast<unsigned long long>(block_table[logical_block]) * kPageTokens;
            const unsigned long long physical_end = physical_begin + (kPageTokens - 1u);
            if (physical_begin > 0xffffffffull || physical_end > 0xffffffffull ||
                physical_end >= static_cast<unsigned long long>(physical_token_capacity)) {
                atomicCAS(shared_error, kStatusOk, kStatusMappedPageOutOfBounds);
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return false;
        }
    }
    return true;
}

template <int HeadDim>
__device__ __forceinline__ size_t flash_shared_bytes() {
    // Keep this layout mirrored by the standalone Zig harness.
    return
        2u * kQueryTile * HeadDim * sizeof(half) + // Q tile + K/V tile
        kQueryTile * kKeyTile * sizeof(float) +   // scores
        kQueryTile * kKeyTile * sizeof(half) +    // probabilities
        kWarps * kQueryTile * 16u * sizeof(float) + // per-warp WMMA scratch
        3u * kQueryTile * sizeof(unsigned) +       // physical, begin, end
        4u * kQueryTile * sizeof(float) +          // max, denom, alpha, beta
        sizeof(unsigned);                          // shared error
}

template <int HeadDim, int SlidingWindow>
__device__ void flash_prefill_body(
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
    half* q_shared = reinterpret_cast<half*>(storage);
    half* kv_shared = q_shared + kQueryTile * HeadDim;
    float* scores = reinterpret_cast<float*>(kv_shared + kKeyTile * HeadDim);
    half* probabilities = reinterpret_cast<half*>(scores + kQueryTile * kKeyTile);
    float* warp_scratch = reinterpret_cast<float*>(probabilities + kQueryTile * kKeyTile);
    unsigned* physical = reinterpret_cast<unsigned*>(warp_scratch + kWarps * kQueryTile * 16u);
    unsigned* row_begin = physical + kKeyTile;
    unsigned* row_end = row_begin + kQueryTile;
    float* running_max = reinterpret_cast<float*>(row_end + kQueryTile);
    float* running_denom = running_max + kQueryTile;
    float* row_alpha = running_denom + kQueryTile;
    float* row_beta = row_alpha + kQueryTile;
    unsigned* shared_error = reinterpret_cast<unsigned*>(row_beta + kQueryTile);

    if (!validate_launch<HeadDim, SlidingWindow, true>(
            dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len,
            num_heads, num_kv_heads, head_dim, query_position_offset,
            kv_position_offset, sliding_window, total_sequence_len, mask_len,
            bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes,
            block_count, page_size_tokens, format, value_format,
            physical_token_capacity, decode_scalars, shared_error)) {
        return;
    }

    const unsigned tid = threadIdx.x;
    const unsigned warp = tid >> 5u;
    const unsigned lane = tid & 31u;
    const unsigned head = blockIdx.x;
    const unsigned tile_query_begin = blockIdx.y * kQueryTile;
    const unsigned valid_rows = q_seq_len - tile_query_begin < kQueryTile
        ? q_seq_len - tile_query_begin
        : kQueryTile;

    // validate_launch has already proved the fixed 256-thread geometry.  Keep
    // the stride compile-time-visible so nvcc can unroll the cooperative
    // loads/reductions instead of emitting a runtime %ntid.x loop.
    for (unsigned index = tid; index < kQueryTile * HeadDim; index += kThreads) {
        const unsigned row = index / HeadDim;
        const unsigned column = index - row * HeadDim;
        q_shared[index] = row < valid_rows
            ? __float2half_rn(q[(tile_query_begin + row) * kHeads * HeadDim +
                  head * HeadDim + column])
            : __float2half_rn(0.0f);
    }
    if (tid < kQueryTile) {
        if (tid < valid_rows) {
            const unsigned query_pos = query_position_offset + tile_query_begin + tid;
            const VisibleRange range = visible_range(
                query_pos, kv_seq_len, kv_position_offset, SlidingWindow);
            row_begin[tid] = range.begin;
            row_end[tid] = range.end;
        } else {
            row_begin[tid] = 0u;
            row_end[tid] = 0u;
        }
        running_max[tid] = kNegativeInfinity;
        running_denom[tid] = 0.0f;
        row_alpha[tid] = 1.0f;
        row_beta[tid] = 0.0f;
    }
    __syncthreads();

    unsigned union_begin = kv_seq_len;
    unsigned union_end = 0u;
    if (tid == 0u) {
        for (unsigned row = 0u; row < valid_rows; ++row) {
            if (row_begin[row] < row_end[row]) {
                if (row_begin[row] < union_begin) union_begin = row_begin[row];
                if (row_end[row] > union_end) union_end = row_end[row];
            }
        }
        if (union_end == 0u) union_begin = 0u;
        row_alpha[0] = static_cast<float>(union_begin);
        row_beta[0] = static_cast<float>(union_end);
    }
    __syncthreads();
    // Both values are <= UINT32_MAX and exact in F32 for the supported model
    // context.  Reuse two metadata slots only until the first score tile.
    union_begin = static_cast<unsigned>(row_alpha[0]);
    union_end = static_cast<unsigned>(row_beta[0]);
    const unsigned first_key_tile = (union_begin / kKeyTile) * kKeyTile;
    const unsigned final_key_tile = ceil_div_u32(union_end, kKeyTile) * kKeyTile;

    constexpr unsigned kOutputTiles = HeadDim / 16u;
    constexpr unsigned kFragmentsPerWarp = kOutputTiles / kWarps;
    static_assert(HeadDim == 256 || HeadDim == 512, "unsupported head dimension");
    static_assert(kOutputTiles % kWarps == 0, "output tiles must divide warps");
    wmma::fragment<wmma::accumulator, 16, 16, 16, float>
        output_fragments[kFragmentsPerWarp];
#pragma unroll
    for (unsigned fragment_index = 0u;
         fragment_index < kFragmentsPerWarp;
         ++fragment_index) {
        wmma::fill_fragment(output_fragments[fragment_index], 0.0f);
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
                atomicCAS(shared_error, kStatusOk, kStatusMappedPageOutOfBounds);
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return;
        }

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

        // Split the head dimension across all eight warps.  Each warp writes
        // one F32 partial score tile into its otherwise-idle PV scratch; the
        // CTA then reduces the eight tiles before softmax.  This avoids a
        // single-warp QK bottleneck while retaining a fixed F32 merge order.
        {
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
                wmma::load_matrix_sync(query_fragment, q_shared + dimension, HeadDim);
                // K rows [key, dimension] are exactly a column-major K^T tile.
                wmma::load_matrix_sync(key_fragment, kv_shared + dimension, HeadDim);
                wmma::mma_sync(
                    score_fragment, query_fragment, key_fragment, score_fragment);
            }
            wmma::store_matrix_sync(
                warp_scratch + warp * kQueryTile * kKeyTile,
                score_fragment,
                kKeyTile,
                wmma::mem_row_major);
        }
        __syncthreads();
        for (unsigned index = tid; index < kQueryTile * kKeyTile; index += kThreads) {
            float score = 0.0f;
#pragma unroll
            for (unsigned source_warp = 0u; source_warp < kWarps; ++source_warp) {
                score += warp_scratch[source_warp * kQueryTile * kKeyTile + index];
            }
            scores[index] = score;
        }
        __syncthreads();

        if (tid < kQueryTile) {
            const unsigned row = tid;
            const float scale = rsqrtf(static_cast<float>(HeadDim));
            float tile_max = kNegativeInfinity;
            bool tile_has_value = false;
#pragma unroll
            for (unsigned key_column = 0u; key_column < kKeyTile; ++key_column) {
                const unsigned logical_token = key_tile_begin + key_column;
                const bool visible = row < valid_rows && logical_token < kv_seq_len &&
                    logical_token >= row_begin[row] && logical_token < row_end[row] &&
                    physical[key_column] != kInvalidToken;
                if (visible) {
                    tile_max = fmaxf(
                        tile_max,
                        scores[row * kKeyTile + key_column] * scale);
                    tile_has_value = true;
                }
            }
            float tile_denom = 0.0f;
#pragma unroll
            for (unsigned key_column = 0u; key_column < kKeyTile; ++key_column) {
                const unsigned logical_token = key_tile_begin + key_column;
                const bool visible = tile_has_value && row < valid_rows &&
                    logical_token < kv_seq_len && logical_token >= row_begin[row] &&
                    logical_token < row_end[row] && physical[key_column] != kInvalidToken;
                const float probability = visible
                    ? expf(scores[row * kKeyTile + key_column] * scale - tile_max)
                    : 0.0f;
                probabilities[row * kKeyTile + key_column] = __float2half_rn(probability);
                tile_denom += probability;
            }

            if (tile_has_value) {
                const float old_max = running_max[row];
                const float old_denom = running_denom[row];
                const float next_max = fmaxf(old_max, tile_max);
                const float alpha = old_denom > 0.0f ? expf(old_max - next_max) : 0.0f;
                const float beta = expf(tile_max - next_max);
                running_max[row] = next_max;
                running_denom[row] = old_denom * alpha + tile_denom * beta;
                row_alpha[row] = alpha;
                row_beta[row] = beta;
            } else {
                row_alpha[row] = 1.0f;
                row_beta[row] = 0.0f;
            }
        }
        __syncthreads();

        // Reuse the K tile allocation for V only after score consumers finish.
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

        wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major>
            probability_fragment;
        wmma::load_matrix_sync(probability_fragment, probabilities, kKeyTile);
        float* const scratch = warp_scratch + warp * kQueryTile * 16u;
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
                value_fragment, kv_shared + output_tile * 16u, HeadDim);
            wmma::fill_fragment(tile_output_fragment, 0.0f);
            wmma::mma_sync(
                tile_output_fragment,
                probability_fragment,
                value_fragment,
                tile_output_fragment);

            // Accumulator-fragment lane layouts are intentionally opaque.
            // Round-trip through warp-private shared storage to apply exact
            // row-wise online-softmax merge factors without layout assumptions.
            wmma::store_matrix_sync(
                scratch,
                output_fragments[fragment_index],
                16,
                wmma::mem_row_major);
            __syncwarp();
            for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                scratch[index] *= row_alpha[index / 16u];
            }
            __syncwarp();
            wmma::load_matrix_sync(
                output_fragments[fragment_index],
                scratch,
                16,
                wmma::mem_row_major);

            wmma::store_matrix_sync(
                scratch, tile_output_fragment, 16, wmma::mem_row_major);
            __syncwarp();
            for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                scratch[index] *= row_beta[index / 16u];
            }
            __syncwarp();
            wmma::load_matrix_sync(
                tile_output_fragment, scratch, 16, wmma::mem_row_major);
#pragma unroll
            for (unsigned element = 0u;
                 element < output_fragments[fragment_index].num_elements;
                 ++element) {
                output_fragments[fragment_index].x[element] +=
                    tile_output_fragment.x[element];
            }
        }
        __syncthreads();
    }

#pragma unroll
    for (unsigned fragment_index = 0u;
         fragment_index < kFragmentsPerWarp;
         ++fragment_index) {
        const unsigned output_tile = warp + fragment_index * kWarps;
        float* const scratch = warp_scratch + warp * kQueryTile * 16u;
        wmma::store_matrix_sync(
            scratch,
            output_fragments[fragment_index],
            16,
            wmma::mem_row_major);
        __syncwarp();
        for (unsigned index = lane; index < 16u * 16u; index += 32u) {
            const unsigned row = index / 16u;
            const unsigned column = index - row * 16u;
            if (row < valid_rows) {
                const float denominator = running_denom[row];
                const size_t output_index =
                    static_cast<size_t>(tile_query_begin + row) * kHeads * HeadDim +
                    head * HeadDim + output_tile * 16u + column;
                dst[output_index] = denominator > 0.0f
                    ? scratch[index] / denominator
                    : 0.0f;
            }
        }
        __syncwarp();
    }
}

template <int HeadDim, int SlidingWindow>
__device__ void reference_prefill_body(
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
    unsigned* shared_error,
    float* warp_sums,
    unsigned* shared_physical,
    float* shared_max,
    float* shared_denom,
    float* shared_alpha,
    float* shared_beta
) {
    if (!validate_launch<HeadDim, SlidingWindow, false>(
            dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len,
            num_heads, num_kv_heads, head_dim, query_position_offset,
            kv_position_offset, sliding_window, total_sequence_len, mask_len,
            bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes,
            block_count, page_size_tokens, format, value_format,
            physical_token_capacity, decode_scalars, shared_error)) {
        return;
    }

    const unsigned tid = threadIdx.x;
    const unsigned lane = tid & 31u;
    const unsigned warp = tid >> 5u;
    constexpr unsigned kReferenceWarps = HeadDim / 32u;
    const unsigned head = blockIdx.x;
    const unsigned query_index = blockIdx.y;
    const unsigned query_pos = query_position_offset + query_index;
    const VisibleRange range = visible_range(
        query_pos, kv_seq_len, kv_position_offset, SlidingWindow);
    const size_t q_offset =
        static_cast<size_t>(query_index) * kHeads * HeadDim + head * HeadDim;
    const float query_value = q[q_offset + tid];
    const float scale = rsqrtf(static_cast<float>(HeadDim));
    float output_accumulator = 0.0f;
    if (tid == 0u) {
        *shared_max = kNegativeInfinity;
        *shared_denom = 0.0f;
    }
    __syncthreads();

    for (unsigned logical_token = range.begin;
         logical_token < range.end;
         ++logical_token) {
        if (tid == 0u) {
            *shared_physical = physical_token(
                logical_token, block_table, block_count,
                page_size_tokens, physical_token_capacity);
            if (*shared_physical == kInvalidToken) {
                *shared_error = kStatusMappedPageOutOfBounds;
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return;
        }
        const half* const key_row = reinterpret_cast<const half*>(
            k + static_cast<size_t>(*shared_physical) * key_row_bytes);
        const half* const value_row = reinterpret_cast<const half*>(
            v + static_cast<size_t>(*shared_physical) * value_row_bytes);
        float partial = query_value * __half2float(key_row[tid]);
        for (unsigned offset = 16u; offset > 0u; offset >>= 1u) {
            partial += __shfl_down_sync(0xffffffffu, partial, offset);
        }
        if (lane == 0u) warp_sums[warp] = partial;
        __syncthreads();
        float dot = warp == 0u && lane < kReferenceWarps
            ? warp_sums[lane]
            : 0.0f;
        if (warp == 0u) {
            for (unsigned offset = 16u; offset > 0u; offset >>= 1u) {
                dot += __shfl_down_sync(0xffffffffu, dot, offset);
            }
            if (lane == 0u) {
                const float score = dot * scale;
                const float next_max = fmaxf(*shared_max, score);
                *shared_alpha = *shared_denom > 0.0f
                    ? expf(*shared_max - next_max)
                    : 0.0f;
                *shared_beta = expf(score - next_max);
                *shared_denom = *shared_denom * *shared_alpha + *shared_beta;
                *shared_max = next_max;
            }
        }
        __syncthreads();
        output_accumulator = output_accumulator * *shared_alpha +
            *shared_beta * __half2float(value_row[tid]);
        __syncthreads();
    }

    dst[q_offset + tid] = *shared_denom > 0.0f
        ? output_accumulator / *shared_denom
        : 0.0f;
}

} // namespace ANTFLY_FLASH_NAMESPACE

#define ANTFLY_FLASH_ARGUMENTS \
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

#define ANTFLY_FORWARD_ARGUMENTS \
    dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len, \
    num_heads, num_kv_heads, head_dim, query_position_offset, \
    kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, \
    key_row_bytes, base_key_row_bytes, value_row_bytes, block_count, \
    page_size_tokens, format, value_format, physical_token_capacity, decode_scalars

extern "C" __global__ void
ANTFLY_FLASH_KERNEL(
    ANTFLY_FLASH_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    extern __shared__ __align__(16) unsigned char storage[];
    ANTFLY_FLASH_NAMESPACE::flash_prefill_body<
        ANTFLY_FLASH_HEAD_DIM,
        ANTFLY_FLASH_SLIDING_WINDOW>(ANTFLY_FORWARD_ARGUMENTS, storage);
}

#ifdef ANTFLY_FLASH_REFERENCE_KERNEL
extern "C" __global__ void
ANTFLY_FLASH_REFERENCE_KERNEL(
    ANTFLY_FLASH_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    __shared__ unsigned shared_error;
    __shared__ float warp_sums[16];
    __shared__ unsigned shared_physical;
    __shared__ float shared_max;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    ANTFLY_FLASH_NAMESPACE::reference_prefill_body<
        ANTFLY_FLASH_HEAD_DIM,
        ANTFLY_FLASH_SLIDING_WINDOW>(
        ANTFLY_FORWARD_ARGUMENTS,
        &shared_error,
        warp_sums,
        &shared_physical,
        &shared_max,
        &shared_denom,
        &shared_alpha,
        &shared_beta);
}
#endif

#undef ANTFLY_FORWARD_ARGUMENTS
#undef ANTFLY_FLASH_ARGUMENTS
