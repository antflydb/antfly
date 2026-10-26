// Default-off generated Flash prefill from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/prefill_flash/sm89/e4b/hd256/gqa8-kv2/page16/q2-2048-kv32k/swa512/f32q-f16kv-hmma
// source_id=antfly_gqa_attention_prefill_flash_sm89_e4b_hd256_swa512_f32_v1
// kernel_id=antfly_gqa_attention_prefill_flash_sm89_e4b_hd256_swa512_f32_v1
// production_baseline=termite_gqa_attention_prefill_turboquant_fast_f32
// runtime_default_enabled=false
// exact_token_parity_qualified=false
#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_sm89_e4b_hd256_swa512_f32_v1
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_sm89_e4b_hd256_swa512_f32_v1
#define ANTFLY_FLASH_KV_HEADS 2
#define ANTFLY_FLASH_HEAD_DIM 256
#define ANTFLY_FLASH_SLIDING_WINDOW 512
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

// Typed SM89 paged-GQA Flash prefill: qualified E2B and opt-in E4B.
//
// The typed CUDA renderer supplies a unique namespace, one production entry
// point, head dimension, and attention policy before expanding this template.
// The standalone qualification wrapper uses the same template and additionally
// requests a scalar reference entry point, keeping qualification and runtime
// code byte-identical below the exported ABI.
//
// Candidate algorithm:
//   * one CTA owns one query head and 16 E2B or 64 E4B query rows;
//   * tensor-core MMA computes F16 QK^T into F32 score accumulators;
//   * causal/SWA masking and online softmax are evaluated per query row;
//   * tile probabilities are rounded to F16 and tensor-core MMA computes PV;
//   * the output numerator and softmax denominator remain F32 across tiles.
//
// Supported contracts are deliberately narrow:
//   * SM89, batch=1, query heads=8, KV heads=1 (E2B) or 2 (E4B);
//   * E2B q512/q3 buckets; E4B q2..8192 chunks through 32768 KV tokens;
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

// antfly-flash:begin common
#include <cuda_fp16.h>
#include <mma.h>
#include <stdint.h>
#include <stddef.h>
#include <math.h>
#ifndef ANTFLY_FLASH_COMMON_DEFINED
#define ANTFLY_FLASH_COMMON_DEFINED
namespace antfly_flash_common {

using namespace nvcuda;

// One source owner, with compile-time geometry per instantiation. A bundle
// may contain both E2B and E4B; the first wrapper must not fix all later layouts.
template <unsigned KvHeads>
struct Geometry {
static constexpr unsigned kHeads = 8u;
static constexpr unsigned kKvHeads = KvHeads;
static_assert(kKvHeads == 1u || kKvHeads == 2u, "Only Gemma E2B/E4B geometries are supported");
static constexpr unsigned kPageTokens = 16u;
static constexpr unsigned kQueryTile = kKvHeads == 2u ? 64u : 16u;
static constexpr unsigned kKeyTile = 16u;
static constexpr unsigned kThreads = 256u;
static constexpr unsigned kWarps = kThreads / 32u;
static constexpr unsigned kInvalidToken = 0xffffffffu;
static constexpr float kNegativeInfinity = -3.402823466e+38F;

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

static __device__ __forceinline__ bool pointer_aligned(const void* ptr, uintptr_t alignment) {
    return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1u)) == 0u;
}

static __device__ __forceinline__ unsigned ceil_div_u32(unsigned value, unsigned divisor) {
    return value / divisor + (value % divisor != 0u ? 1u : 0u);
}

static __device__ __forceinline__ unsigned physical_token(
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

static __device__ __forceinline__ VisibleRange visible_range(
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
static __device__ bool validate_launch(
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
                (kKvHeads == 1u ? (q_seq_len != 512u && q_seq_len != 3u)
                                  : (q_seq_len < 2u || q_seq_len > 8192u)) ||
                kv_seq_len == 0u || kv_seq_len > (kKvHeads == 1u ? 4096u : 32768u)) {
                error = kStatusUnsupportedShape;
            } else {
                const bool supported_prefix = kKvHeads == 2u ?
                    (query_position_offset <= 32768u - q_seq_len) : q_seq_len == 512u
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
            } else if (error == kStatusOk && (key_row_bytes != kKvHeads * static_cast<unsigned>(HeadDim) * sizeof(half) ||
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
                        static_cast<unsigned long long>(physical_token_capacity) * kKvHeads * HeadDim;
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
static __device__ __forceinline__ size_t flash_shared_bytes() {
    // Keep this layout mirrored by the typed renderer and qualification harness.
    if constexpr (kKvHeads == 2u) {
        return (kQueryTile + 2u * kKeyTile) * HeadDim * sizeof(half) + 3u * kQueryTile * sizeof(float) +
            kQueryTile * kKeyTile * sizeof(half) +
            (kKeyTile + 1u) * sizeof(unsigned);
    }
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
static __device__ void flash_prefill_body(
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
    if constexpr (kKvHeads == 2u) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
        // Keep a compatibility symbol in the compute_75 bundle, but never
        // emit Ampere MMA instructions there. Host routing requires SM89.
        // Four query groups each use two output warps, sharing one QK/softmax
        // calculation. XOR-swizzle 8-half vectors to avoid bank conflicts
        // without padding; this fits separate K/V tiles below 99 KiB.
        constexpr unsigned pitch = HeadDim;
        half* q_shared = reinterpret_cast<half*>(storage);
        half* kv_shared = q_shared + kQueryTile * pitch;
        half* v_shared = kv_shared + kKeyTile * pitch;
        half* probabilities = v_shared + kKeyTile * pitch;
        unsigned* physical = reinterpret_cast<unsigned*>(probabilities + kQueryTile * kKeyTile);
        float* row_state = reinterpret_cast<float*>(physical + kKeyTile);
        unsigned* shared_error = reinterpret_cast<unsigned*>(row_state + 3u * kQueryTile);
        if (!validate_launch<HeadDim, SlidingWindow, true>(
                dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len,
                num_heads, num_kv_heads, head_dim, query_position_offset,
                kv_position_offset, sliding_window, total_sequence_len, mask_len,
                bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes,
                block_count, page_size_tokens, format, value_format,
                physical_token_capacity, decode_scalars, shared_error)) return;
        const unsigned tid = threadIdx.x, warp = tid / 32u, lane = tid % 32u;
        const unsigned head = blockIdx.x, kv_head = head / (kHeads / kKvHeads);
        const unsigned tile_begin = blockIdx.y * kQueryTile;
        const unsigned valid_rows = min(kQueryTile, q_seq_len - tile_begin);
        const unsigned query_group = warp / 2u;
        const unsigned output_half = warp % 2u;
        constexpr unsigned fragments = HeadDim / 2u / 8u;
        float output[fragments][4] = {};
        float running_max[2] = { kNegativeInfinity, kNegativeInfinity };
        float running_denom[2] = {};
        for (unsigned i = tid; i < kQueryTile * HeadDim; i += kThreads) {
            const unsigned row = i / HeadDim, column = i % HeadDim;
            q_shared[row * pitch + (column ^ ((row % 8u) * 8u))] = row < valid_rows ? __float2half_rn(q[size_t(tile_begin + row) * kHeads * HeadDim + head * HeadDim + column]) : __float2half_rn(0.0f);
        }
        __syncthreads();
        const unsigned first_pos = query_position_offset + tile_begin;
        const unsigned begin = SlidingWindow && first_pos + 1u > SlidingWindow ? first_pos + 1u - SlidingWindow : 0u;
        const unsigned end = min(kv_seq_len, first_pos + valid_rows);
        for (unsigned key_begin = begin / 16u * 16u; key_begin < end; key_begin += 16u) {
            if (tid < 16u) physical[tid] = key_begin + tid < kv_seq_len ? physical_token(key_begin + tid, block_table, block_count, page_size_tokens, physical_token_capacity) : kInvalidToken;
            __syncthreads();
            // Eight adjacent halves share one aligned load/store. Keep the
            // scalar route for the ABI's valid two-byte-aligned pointers.
            if (pointer_aligned(k, 16u) && pointer_aligned(v, 16u)) {
                for (unsigned vector = tid; vector < 16u * HeadDim / 8u; vector += kThreads) {
                    const unsigned i = vector * 8u;
                    const unsigned token = physical[i / HeadDim];
                    const unsigned index = (i / HeadDim) * pitch + ((i % HeadDim) ^ (((i / HeadDim) % 8u) * 8u));
                    const size_t column_bytes = (kv_head * HeadDim + i % HeadDim) * sizeof(half);
                    const uint4 key = token != kInvalidToken ? *reinterpret_cast<const uint4*>(k + size_t(token) * key_row_bytes + column_bytes) : make_uint4(0,0,0,0);
                    const uint4 value = token != kInvalidToken ? *reinterpret_cast<const uint4*>(v + size_t(token) * value_row_bytes + column_bytes) : make_uint4(0,0,0,0);
                    *reinterpret_cast<uint4*>(kv_shared + index) = key;
                    *reinterpret_cast<uint4*>(v_shared + index) = value;
                }
            } else {
                for (unsigned i = tid; i < 16u * HeadDim; i += kThreads) {
                    const unsigned token = physical[i / HeadDim];
                    const unsigned index = (i / HeadDim) * pitch + ((i % HeadDim) ^ (((i / HeadDim) % 8u) * 8u));
                    kv_shared[index] = token != kInvalidToken ? reinterpret_cast<const half*>(k + size_t(token) * key_row_bytes)[kv_head * HeadDim + i % HeadDim] : __float2half_rn(0.0f);
                    v_shared[index] = token != kInvalidToken ? reinterpret_cast<const half*>(v + size_t(token) * value_row_bytes)[kv_head * HeadDim + i % HeadDim] : __float2half_rn(0.0f);
                }
            }
            __syncthreads();
            float alpha[2], beta[2];
            if (output_half == 0u) {
                float scores[2][4] = {};
        #pragma unroll
                for (unsigned dim = 0; dim < HeadDim; dim += 16u) {
                    unsigned qa[4];
                    const unsigned qa_addr = __cvta_generic_to_shared(q_shared + (query_group * 16u + lane % 16u) * pitch + ((dim + (lane / 16u) * 8u) ^ ((lane % 8u) * 8u)));
                    asm volatile("ldmatrix.sync.aligned.m8n8.x4.shared.b16 {%0,%1,%2,%3}, [%4];" : "=r"(qa[0]),"=r"(qa[1]),"=r"(qa[2]),"=r"(qa[3]) : "r"(qa_addr));
        #pragma unroll
                    for (unsigned half = 0; half < 2u; ++half) {
                        unsigned kb[2];
                        const unsigned kb_addr = __cvta_generic_to_shared(kv_shared + (half * 8u + lane % 8u) * pitch + ((dim + ((lane / 8u) % 2u) * 8u) ^ ((lane % 8u) * 8u)));
                        asm volatile("ldmatrix.sync.aligned.m8n8.x2.shared.b16 {%0,%1}, [%2];" : "=r"(kb[0]),"=r"(kb[1]) : "r"(kb_addr));
                        asm volatile("mma.sync.aligned.m16n8k16.row.col.f32.f16.f16.f32 {%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%0,%1,%2,%3};"
                            : "+f"(scores[half][0]),"+f"(scores[half][1]),"+f"(scores[half][2]),"+f"(scores[half][3])
                            : "r"(qa[0]),"r"(qa[1]),"r"(qa[2]),"r"(qa[3]),"r"(kb[0]),"r"(kb[1]));
                    }
                }
        #pragma unroll
                for (unsigned r = 0; r < 2u; ++r) {
                    const unsigned row = query_group * 16u + lane / 4u + r * 8u;
                    const unsigned pos = first_pos + row;
                    bool visible[4]; float value[4]; float maximum = kNegativeInfinity;
        #pragma unroll
                    for (unsigned c = 0; c < 4u; ++c) {
                        const unsigned column = (c / 2u) * 8u + (lane % 4u) * 2u + c % 2u;
                        const unsigned token = key_begin + column;
                        visible[c] = row < valid_rows && token <= pos && token < kv_seq_len &&
                            (!SlidingWindow || pos - token < SlidingWindow);
                        value[c] = scores[c / 2u][r * 2u + c % 2u] * rsqrtf(float(HeadDim));
                        maximum = fmaxf(maximum, visible[c] ? value[c] : kNegativeInfinity);
                    }
                    maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffffu, maximum, 2, 4));
                    maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffffu, maximum, 1, 4));
                    float denom = 0.0f;
        #pragma unroll
                    for (unsigned c = 0; c < 4u; ++c) {
                        const float probability = visible[c] ? expf(value[c] - maximum) : 0.0f;
                        denom += probability;
                        if (output_half == 0u) {
                            const unsigned column = (c / 2u) * 8u + (lane % 4u) * 2u + c % 2u;
                            probabilities[row * 16u + column] = __float2half_rn(probability);
                        }
                    }
                    denom += __shfl_xor_sync(0xffffffffu, denom, 2, 4);
                    denom += __shfl_xor_sync(0xffffffffu, denom, 1, 4);
                    if (denom > 0.0f) {
                        const float next_max = fmaxf(running_max[r], maximum);
                        alpha[r] = running_denom[r] > 0 ? expf(running_max[r] - next_max) : 0.0f;
                        beta[r] = expf(maximum - next_max);
                        running_denom[r] = running_denom[r] * alpha[r] + denom * beta[r];
                        running_max[r] = next_max;
                    } else { alpha[r] = 1.0f; beta[r] = 0.0f; }
                    if (lane % 4u == 0u) {
                        row_state[row * 3u] = alpha[r];
                        row_state[row * 3u + 1u] = beta[r];
                        row_state[row * 3u + 2u] = running_denom[r];
                    }
                }
            }
            __syncthreads();
    #pragma unroll
            for (unsigned r = 0; r < 2; ++r) {
                const unsigned row = query_group * 16u + lane / 4u + r * 8u;
                alpha[r] = row_state[row * 3u];
                beta[r] = row_state[row * 3u + 1u];
                running_denom[r] = row_state[row * 3u + 2u];
            }
            unsigned pa[4];
            const unsigned pa_addr = __cvta_generic_to_shared(probabilities + (query_group * 16u + lane % 16u) * 16u + (lane / 16u) * 8u);
            asm volatile("ldmatrix.sync.aligned.m8n8.x4.shared.b16 {%0,%1,%2,%3}, [%4];" : "=r"(pa[0]),"=r"(pa[1]),"=r"(pa[2]),"=r"(pa[3]) : "r"(pa_addr));
    #pragma unroll
            for (unsigned f = 0; f < fragments; ++f) {
                unsigned vb[2]; float tile[4];
                const unsigned column = output_half * (HeadDim / 2u) + f * 8u;
                const unsigned vb_addr = __cvta_generic_to_shared(v_shared + (lane % 16u) * pitch + (column ^ ((lane % 8u) * 8u)));
                asm volatile("ldmatrix.sync.aligned.m8n8.x2.trans.shared.b16 {%0,%1}, [%2];" : "=r"(vb[0]),"=r"(vb[1]) : "r"(vb_addr));
                asm volatile("mma.sync.aligned.m16n8k16.row.col.f32.f16.f16.f32 {%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%10,%10,%10,%10};"
                    : "=f"(tile[0]),"=f"(tile[1]),"=f"(tile[2]),"=f"(tile[3])
                    : "r"(pa[0]),"r"(pa[1]),"r"(pa[2]),"r"(pa[3]),"r"(vb[0]),"r"(vb[1]),"f"(0.0f));
    #pragma unroll
                for (unsigned e = 0; e < 4u; ++e) output[f][e] = output[f][e] * alpha[e / 2u] + tile[e] * beta[e / 2u];
            }
            __syncthreads();
        }
    #pragma unroll
        for (unsigned f = 0; f < fragments; ++f) {
    #pragma unroll
            for (unsigned e = 0; e < 4u; ++e) {
                const unsigned row = query_group * 16u + lane / 4u + (e / 2u) * 8u;
                const unsigned column = output_half * (HeadDim / 2u) + f * 8u + (lane % 4u) * 2u + e % 2u;
                if (row < valid_rows) dst[size_t(tile_begin + row) * kHeads * HeadDim + head * HeadDim + column] = running_denom[e / 2u] > 0 ? output[f][e] / running_denom[e / 2u] : 0.0f;
            }
        }
#endif
        return;
    }
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
    const unsigned kv_head = head / (kHeads / kKvHeads);
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
                      k + static_cast<size_t>(token) * key_row_bytes)[kv_head * HeadDim + column]
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
                      v + static_cast<size_t>(token) * value_row_bytes)[kv_head * HeadDim + column]
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
static __device__ void reference_prefill_body(
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
    const unsigned kv_head = head / (kHeads / kKvHeads);
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
            k + static_cast<size_t>(*shared_physical) * key_row_bytes) + kv_head * HeadDim;
        const half* const value_row = reinterpret_cast<const half*>(
            v + static_cast<size_t>(*shared_physical) * value_row_bytes) + kv_head * HeadDim;
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

}; // Geometry

} // namespace antfly_flash_common
#endif
// antfly-flash:end common

#ifndef ANTFLY_FLASH_KV_HEADS
#define ANTFLY_FLASH_KV_HEADS 1
#endif
using ANTFLY_FLASH_NAMESPACE = antfly_flash_common::Geometry<ANTFLY_FLASH_KV_HEADS>;

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
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_KV_HEADS
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE
