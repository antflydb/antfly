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
#include <math.h>

#include "../kernels/row_program.cuh"

struct PagedRelativeAttentionParams {
    const __nv_bfloat16* query;
    const __nv_bfloat16* keys;
    const __nv_bfloat16* values;
    const __nv_bfloat16* relative;
    const __nv_bfloat16* sinks;
    const __nv_bfloat16* gate;
    const uint32_t* page_ids;
    float* partials;
    __nv_bfloat16* output;
    const uint32_t* replay_position;
    uint32_t query_start;
    uint32_t visible_start;
    uint32_t page_size;
    uint32_t first_page;
    uint32_t page_count;
    uint32_t physical_pages;
    uint32_t q_heads;
    uint32_t kv_heads;
    uint32_t window;
    uint32_t relative_extent;
    uint32_t splits;
    uint32_t key_tile;
    float score_scale;
};
static_assert(sizeof(PagedRelativeAttentionParams) == 136, "paged relative attention ABI");

namespace {
constexpr unsigned head_dim = 128;
constexpr unsigned partial_width = head_dim + 2;
constexpr unsigned parallel_chunk_keys = 128;
constexpr unsigned parallel_chunk_warps = 8;

struct Chunk {
    unsigned position;
    unsigned begin;
    unsigned end;
};

__device__ Chunk chunk_range(const PagedRelativeAttentionParams& p, unsigned split, unsigned row) {
    const unsigned position = (p.replay_position ? *p.replay_position : p.query_start) + row;
    const unsigned prefix = position + 1;
    const unsigned local_first = p.window != 0 && prefix > p.window ? prefix - p.window : 0u;
    const unsigned first = max(p.visible_start, local_first);
    const unsigned splits = p.replay_position ? (prefix - first - 1) / 128 + 1 : p.splits;
    const unsigned tile = p.replay_position ? (prefix - first - 1) / splits + 1 : p.key_tile;
    const unsigned begin = split < splits ? first + split * tile : prefix;
    return {position, begin, min(prefix, begin + tile)};
}

__device__ bool cache_row(const PagedRelativeAttentionParams& p, unsigned position, size_t& row) {
    const unsigned logical = position / p.page_size - p.first_page;
    if (logical >= p.page_count) return false;
    const uint32_t page = p.page_ids[logical];
    if (page >= p.physical_pages) return false;
    row = (size_t)page * p.page_size + position % p.page_size;
    return true;
}

__device__ void store_partial(float* dst, unsigned lane, const float (&value)[4], float maximum, float denom) {
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) dst[i * 32u + lane] = value[i];
    if (lane == 0) {
        dst[head_dim] = maximum;
        dst[head_dim + 1] = denom;
    }
}
} // namespace

extern "C" __global__ void antfly_paged_relative_gqa_d128_bf16_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    const unsigned lane = threadIdx.x;
    const unsigned split = blockIdx.x;
    const unsigned query_head = blockIdx.y;
    const unsigned head = query_head % p.q_heads;
    const unsigned kv_head = head / (p.q_heads / p.kv_heads);
    const Chunk chunk = chunk_range(p, split, query_head / p.q_heads);
    const __nv_bfloat16* row_q = p.query + (size_t)query_head * head_dim;
    const __nv_bfloat16* rel = p.relative;
    if (p.relative_extent) rel += (size_t)query_head * p.relative_extent;
    float q[4], value[4] = {};
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) q[i] = __bfloat162float(row_q[i * 32u + lane]);
    float maximum = -INFINITY, denom = 0.0f;
    for (unsigned key_pos = chunk.begin; key_pos < chunk.end; ++key_pos) {
        size_t row;
        if (!cache_row(p, key_pos, row)) continue;
        const size_t base = (row * p.kv_heads + kv_head) * head_dim;
        const __nv_bfloat16* row_k = p.keys + base;
        const __nv_bfloat16* row_v = p.values + base;
        float dot = 0.0f;
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i)
            dot = __fmaf_rn(q[i], __bfloat162float(row_k[i * 32u + lane]), dot);
        dot = antfly_row_warp_sum(dot);
        const unsigned distance = chunk.position - key_pos;
        const float bias = distance < p.relative_extent ? __bfloat162float(rel[distance]) : 0.0f;
        const float score = __shfl_sync(0xffffffffu, dot, 0) * p.score_scale + bias;
        const float updated_max = fmaxf(score, maximum);
        const float prev = expf(maximum - updated_max);
        const float next = expf(score - updated_max);
        denom = __fmaf_rn(denom, prev, next);
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i)
            value[i] = __fmaf_rn(next, __bfloat162float(row_v[i * 32u + lane]), value[i] * prev);
        maximum = updated_max;
    }
    float* dst = p.partials + ((size_t)query_head * p.splits + split) * partial_width;
    store_partial(dst, lane, value, maximum, denom);
}

// Four adjacent global query rows load a paged key and value once. Each
// query keeps its own causal limit and the serial chunk's accumulation order.
extern "C" __global__ void antfly_paged_relative_gqa_d128_bf16_global_rows4_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    constexpr unsigned rows = 4;
    const unsigned lane = threadIdx.x;
    const unsigned split = blockIdx.x;
    const unsigned head = blockIdx.y % p.q_heads;
    const unsigned first_row = (blockIdx.y / p.q_heads) * rows;
    const unsigned kv_head = head / (p.q_heads / p.kv_heads);
    const Chunk first = chunk_range(p, split, first_row);
    const Chunk last = chunk_range(p, split, first_row + rows - 1);
    float query[rows][4];
    float value[rows][4] = {};
    float maximum[rows], denom[rows] = {};
    #pragma unroll
    for (unsigned r = 0; r < rows; ++r) {
        const size_t query_head = (size_t)(first_row + r) * p.q_heads + head;
        const __nv_bfloat16* source = p.query + query_head * head_dim;
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i) query[r][i] = __bfloat162float(source[i * 32u + lane]);
        maximum[r] = -INFINITY;
    }
    for (unsigned key_pos = first.begin; key_pos < last.end; ++key_pos) {
        size_t row;
        if (!cache_row(p, key_pos, row)) continue;
        const size_t base = (row * p.kv_heads + kv_head) * head_dim;
        float key[4], val[4];
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i) {
            const unsigned component = i * 32u + lane;
            key[i] = __bfloat162float(p.keys[base + component]);
            val[i] = __bfloat162float(p.values[base + component]);
        }
        #pragma unroll
        for (unsigned r = 0; r < rows; ++r) {
            const unsigned position = first.position + r;
            if (key_pos > position) continue;
            float dot = 0.0f;
            #pragma unroll
            for (unsigned i = 0; i < 4; ++i) dot = __fmaf_rn(query[r][i], key[i], dot);
            dot = antfly_row_warp_sum(dot);
            const size_t query_head = (size_t)(first_row + r) * p.q_heads + head;
            const unsigned distance = position - key_pos;
            const float bias = distance < p.relative_extent
                ? __bfloat162float(p.relative[query_head * p.relative_extent + distance]) : 0.0f;
            const float score = __shfl_sync(0xffffffffu, dot, 0) * p.score_scale + bias;
            const float updated_max = fmaxf(score, maximum[r]);
            const float prev = expf(maximum[r] - updated_max);
            const float next = expf(score - updated_max);
            denom[r] = __fmaf_rn(denom[r], prev, next);
            #pragma unroll
            for (unsigned i = 0; i < 4; ++i) value[r][i] = __fmaf_rn(next, val[i], value[r][i] * prev);
            maximum[r] = updated_max;
        }
    }
    #pragma unroll
    for (unsigned r = 0; r < rows; ++r) {
        const size_t query_head = (size_t)(first_row + r) * p.q_heads + head;
        float* dst = p.partials + (query_head * p.splits + split) * partial_width;
        store_partial(dst, lane, value[r], maximum[r], denom[r]);
    }
}

// Queries sharing the same head reuse the translation of each 16-token
// page. Each query still consumes its own keys in their original order.
namespace {
template<unsigned rows, bool sliding>
__device__ void page16_relative_chunks(const PagedRelativeAttentionParams& p) {
    constexpr unsigned page_tokens = 16;
    const unsigned lane = threadIdx.x;
    const unsigned split = blockIdx.x;
    const unsigned head = blockIdx.y % p.q_heads;
    const unsigned first_row = (blockIdx.y / p.q_heads) * rows;
    const unsigned kv_head = head / (p.q_heads / p.kv_heads);
    const Chunk first = chunk_range(p, split, first_row);
    const Chunk last = chunk_range(p, split, first_row + rows - 1);
    Chunk chunks[rows];
    float query[rows][4];
    float value[rows][4] = {};
    float maximum[rows], denom[rows] = {};
    #pragma unroll
    for (unsigned r = 0; r < rows; ++r) {
        if constexpr (sliding) chunks[r] = chunk_range(p, split, first_row + r);
        const size_t query_head = (size_t)(first_row + r) * p.q_heads + head;
        const __nv_bfloat16* source = p.query + query_head * head_dim;
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i) query[r][i] = __bfloat162float(source[i * 32u + lane]);
        maximum[r] = -INFINITY;
    }
    for (unsigned page_start = first.begin & ~(page_tokens - 1); page_start < last.end; page_start += page_tokens) {
        const unsigned logical = page_start / page_tokens - p.first_page;
        if (logical >= p.page_count) continue;
        const unsigned page = p.page_ids[logical];
        if (page >= p.physical_pages) continue;
        const size_t page_row = (size_t)page * page_tokens;
        const unsigned page_end = min(page_start + page_tokens, last.end);
        for (unsigned key_pos = max(first.begin, page_start); key_pos < page_end; ++key_pos) {
            const size_t base = ((page_row + key_pos - page_start) * p.kv_heads + kv_head) * head_dim;
            float key[4], val[4];
            #pragma unroll
            for (unsigned i = 0; i < 4; ++i) {
                const unsigned component = i * 32u + lane;
                key[i] = __bfloat162float(p.keys[base + component]);
                val[i] = __bfloat162float(p.values[base + component]);
            }
            #pragma unroll
            for (unsigned r = 0; r < rows; ++r) {
                const unsigned position = first.position + r;
                if constexpr (sliding) {
                    if (key_pos < chunks[r].begin || key_pos >= chunks[r].end) continue;
                } else if (key_pos > position) continue;
                float dot = 0.0f;
                #pragma unroll
                for (unsigned i = 0; i < 4; ++i) dot = __fmaf_rn(query[r][i], key[i], dot);
                dot = antfly_row_warp_sum(dot);
                const size_t query_head = (size_t)(first_row + r) * p.q_heads + head;
                const unsigned distance = position - key_pos;
                const float bias = distance < p.relative_extent
                    ? __bfloat162float(p.relative[query_head * p.relative_extent + distance]) : 0.0f;
                const float score = __shfl_sync(0xffffffffu, dot, 0) * p.score_scale + bias;
                const float updated = fmaxf(score, maximum[r]);
                // The larger term has exponent zero. Use the same negative
                // difference for the other term and retain NaN on inf - inf.
                const float unit = isfinite(updated) ? 1.0f : updated - updated;
                const float other = expf(-fabsf(score - maximum[r]));
                const bool new_max = score > maximum[r];
                const float prev = new_max ? other : unit;
                const float next = new_max ? unit : other;
                denom[r] = __fmaf_rn(denom[r], prev, next);
                #pragma unroll
                for (unsigned i = 0; i < 4; ++i) value[r][i] = __fmaf_rn(next, val[i], value[r][i] * prev);
                maximum[r] = updated;
            }
        }
    }
    #pragma unroll
    for (unsigned r = 0; r < rows; ++r) {
        const size_t query_head = (size_t)(first_row + r) * p.q_heads + head;
        float* dst = p.partials + (query_head * p.splits + split) * partial_width;
        store_partial(dst, lane, value[r], maximum[r], denom[r]);
    }
}
} // namespace

extern "C" __global__ void antfly_paged_relative_gqa_d128_bf16_page16_global_rows4_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    page16_relative_chunks<4, false>(p);
}

extern "C" __global__ void antfly_paged_relative_gqa_d128_bf16_page16_sliding_rows4_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    page16_relative_chunks<4, true>(p);
}

namespace {
constexpr unsigned mma_keys = 64;
constexpr unsigned mma_vector = 8;

__device__ unsigned cache_tile_column(unsigned row, unsigned feature) {
    // Rotate 16-byte segments in each row to distribute the 8-row MMA
    // operands across the 32 shared-memory banks.
    return feature ^ ((row & 7) * mma_vector);
}

__device__ uint32_t read_pair(const __nv_bfloat16* row, unsigned feature) {
    return *reinterpret_cast<const uint32_t*>(row + feature);
}

__device__ uint32_t pack_pair(__nv_bfloat16 a, __nv_bfloat16 b) {
    return uint32_t(__bfloat16_as_ushort(a)) | (uint32_t(__bfloat16_as_ushort(b)) << 16);
}

template<bool Transpose>
__device__ void load_matrix_pair(const __nv_bfloat16* source, uint32_t& a, uint32_t& b) {
    const uint32_t offset = __cvta_generic_to_shared(source);
    if constexpr (Transpose) {
        asm("ldmatrix.sync.aligned.m8n8.x2.trans.shared.b16 {%0, %1}, [%2];"
            : "=r"(a), "=r"(b) : "r"(offset));
    } else {
        asm("ldmatrix.sync.aligned.m8n8.x2.shared.b16 {%0, %1}, [%2];"
            : "=r"(a), "=r"(b) : "r"(offset));
    }
}

__device__ void copy_cache_vector(__nv_bfloat16* dst, const __nv_bfloat16* src, bool valid) {
    const uint32_t offset = __cvta_generic_to_shared(dst);
    const unsigned size = valid ? 16 : 0;
    asm volatile("cp.async.cg.shared.global [%0], [%1], 16, %2;" :: "r"(offset), "l"(src), "r"(size) : "memory");
}

__device__ uint32_t cache_page16(const PagedRelativeAttentionParams& p, unsigned position) {
    const unsigned logical = position / 16 - p.first_page;
    const uint32_t page = logical < p.page_count ? p.page_ids[logical] : UINT32_MAX;
    return page < p.physical_pages ? page : UINT32_MAX;
}

using RelativeTile = __nv_bfloat16[mma_keys][head_dim];

__device__ void prefetch_kv_tile(const PagedRelativeAttentionParams& p,
                                  unsigned tile, unsigned end, unsigned kv_head,
                                  RelativeTile& keys, RelativeTile& values, uint32_t* page_cache) {
    #pragma unroll
    for (unsigned index = threadIdx.x * mma_vector; index < mma_keys * head_dim; index += 256 * mma_vector) {
        const unsigned row = index / head_dim;
        const unsigned feature = index % head_dim;
        const unsigned page = cache_page16(p, tile + row);
        if (feature == 0 && row % 16 == 0) page_cache[row / 16] = page;
        const bool present = page != UINT32_MAX && tile + row < end;
        const size_t offset = present ? ((size_t(page) * 16 + row % 16) * p.kv_heads + kv_head) * head_dim + feature : 0;
        const unsigned column = cache_tile_column(row, feature);
        copy_cache_vector(&keys[row][column], p.keys + offset, present);
        copy_cache_vector(&values[row][column], p.values + offset, present);
    }
    asm volatile("cp.async.commit_group;" ::: "memory");
}

__device__ void relative_mma(
    float (&out)[4], uint32_t a0, uint32_t a1, uint32_t a2, uint32_t a3, uint32_t b0, uint32_t b1
) {
    asm("mma.sync.aligned.m16n8k16.row.col.f32.bf16.bf16.f32 "
        "{%0, %1, %2, %3}, {%4, %5, %6, %7}, {%8, %9}, {%0, %1, %2, %3};"
        : "+f"(out[0]), "+f"(out[1]), "+f"(out[2]), "+f"(out[3])
        : "r"(a0), "r"(a1), "r"(a2), "r"(a3), "r"(b0), "r"(b1));
}

__device__ float query_score(float dot, const PagedRelativeAttentionParams& p,
                             size_t query_head, unsigned position, unsigned key, bool present, bool distant) {
    if (!present) return -INFINITY;
    float bias = 0.0f;
    if (!distant) {
        const unsigned distance = position - key;
        if (key > position || (p.window && distance >= p.window)) return -INFINITY;
        if (distance < p.relative_extent)
            bias = __bfloat162float(p.relative[query_head * p.relative_extent + distance]);
    }
    return dot * p.score_scale + bias;
}

constexpr unsigned rel_width = 16;
constexpr unsigned relative_columns = 72; // 64 keys plus one tile column for the within-warp shear
struct ProbabilityPair {
    uint32_t high;
    uint32_t low;
};
__device__ ProbabilityPair probability_pair(const float (&prob)[4], unsigned i) {
    const float a = prob[i];
    const float b = prob[i + 1];
    uint32_t high, low;
    asm("cvt.rn.bf16x2.f32 %0, %2, %1;" : "=r"(high) : "f"(a), "f"(b));
    const float da = a - __uint_as_float(high << 16);
    const float db = b - __uint_as_float(high & 0xffff0000u);
    asm("cvt.rn.bf16x2.f32 %0, %2, %1;" : "=r"(low) : "f"(da), "f"(db));
    return {high, low};
}

template<bool Pipeline = false, bool FusedRelative = false>
__device__ __forceinline__ void paged_relative_tile(
    const PagedRelativeAttentionParams& p, unsigned query_rows,
    const __nv_bfloat16* relative_query = nullptr, const __nv_bfloat16* basis = nullptr
) {
    constexpr unsigned Rows = 128;
    constexpr unsigned threads = Rows * 2;
    const unsigned lane = threadIdx.x % 32;
    const unsigned warp = threadIdx.x / 32;
    const unsigned quad = lane / 4;
    const unsigned pair = (lane % 4) * 2;
    const unsigned gqa = p.q_heads / p.kv_heads;
    const unsigned tile_rows = Rows / gqa;
    const unsigned first_row = blockIdx.x * tile_rows;
    const unsigned kv_head = blockIdx.y % p.kv_heads;
    const unsigned split = blockIdx.y / p.kv_heads;
    const unsigned row_pair[2] = {warp * 16 + quad, warp * 16 + quad + 8};
    unsigned pos[2];
    size_t query_head[2];
    bool valid[2];
    #pragma unroll
    for (unsigned r = 0; r < 2; ++r) {
        const unsigned row = first_row + row_pair[r] / gqa;
        pos[r] = p.query_start + row;
        query_head[r] = size_t(row) * p.q_heads + kv_head * gqa + row_pair[r] % gqa;
        valid[r] = row < query_rows && row_pair[r] < tile_rows * gqa;
    }

    __shared__ __align__(16) RelativeTile single_keys;
    __shared__ __align__(16) RelativeTile single_values;
    extern __shared__ __align__(16) __nv_bfloat16 double_buffer_storage[];
    auto* double_buffer = reinterpret_cast<RelativeTile*>(double_buffer_storage);
    auto* bias = reinterpret_cast<__nv_bfloat16 (*)[relative_columns]>(double_buffer + (Pipeline ? 4 : 2));
    __shared__ uint32_t pages[mma_keys / 16];
    __shared__ uint32_t pipeline_pages[2][mma_keys / 16];
    uint32_t q[head_dim / 16][4];
    #pragma unroll
    for (unsigned d = 0; d < head_dim / 16; ++d) {
        const unsigned element = d * 16 + pair;
        const auto* low = p.query + (valid[0] ? query_head[0] * head_dim : 0);
        const auto* high = p.query + (valid[1] ? query_head[1] * head_dim : 0);
        q[d][0] = read_pair(low, element);
        q[d][1] = read_pair(high, element);
        q[d][2] = read_pair(low, element + 8);
        q[d][3] = read_pair(high, element + 8);
    }

    uint32_t rq[4];
    if constexpr (FusedRelative) {
        const auto* low = relative_query + (valid[0] ? query_head[0] * rel_width : 0);
        const auto* high = relative_query + (valid[1] ? query_head[1] * rel_width : 0);
        rq[0] = read_pair(low, pair);
        rq[1] = read_pair(high, pair);
        rq[2] = read_pair(low, pair + 8);
        rq[3] = read_pair(high, pair + 8);
    }
    const unsigned warp_first = p.query_start + first_row + warp * 16 / gqa;
    const unsigned first = p.query_start + first_row;
    const unsigned end = p.query_start + min(query_rows, first_row + tile_rows);
    const unsigned lower = p.window && first + 1 > p.window ? first + 1 - p.window : 0;
    const unsigned begin = max(lower, p.visible_start);
    float maximum[2] = {-INFINITY, -INFINITY};
    float denom[2] = {};
    float output[head_dim / 8][4] = {};
    const unsigned first_tile = (begin & ~(mma_keys - 1)) + split * mma_keys;
    const unsigned stride = p.splits * mma_keys;
    unsigned buffer = 0;
    if constexpr (Pipeline) {
        if (first_tile < end) prefetch_kv_tile(p, first_tile, end, kv_head, double_buffer[0], double_buffer[1], pipeline_pages[0]);
    }
    for (unsigned tile = first_tile; tile < end; tile += stride) {
        auto& keys = (Pipeline || FusedRelative) ? double_buffer[buffer * 2] : single_keys;
        auto& values = (Pipeline || FusedRelative) ? double_buffer[buffer * 2 + 1] : single_values;
        const unsigned tile_last = tile + mma_keys - 1;
        const bool distant = p.window == 0 && tile >= begin &&
            tile_last <= first && p.relative_extent <= first - tile_last;
        if constexpr (Pipeline) {
            const unsigned next_tile = tile + stride;
            if (next_tile < end) {
                const unsigned next_buffer = (buffer ^ 1) * 2;
                prefetch_kv_tile(p, next_tile, end, kv_head, double_buffer[next_buffer], double_buffer[next_buffer + 1], pipeline_pages[buffer ^ 1]);
                asm volatile("cp.async.wait_group 1;" ::: "memory");
            } else {
                asm volatile("cp.async.wait_group 0;" ::: "memory");
            }
        } else {
            if (threadIdx.x < mma_keys / 16) pages[threadIdx.x] = cache_page16(p, tile + threadIdx.x * 16);
            __syncthreads();
            #pragma unroll
            for (unsigned index = threadIdx.x * mma_vector; index < mma_keys * head_dim; index += threads * mma_vector) {
                const unsigned row = index / head_dim;
                const unsigned feature = index % head_dim;
                const unsigned page = pages[row / 16];
                const bool present = page != UINT32_MAX && tile + row < end;
                const size_t offset = present ? ((size_t(page) * 16 + row % 16) * p.kv_heads + kv_head) * head_dim + feature : 0;
                const unsigned column = cache_tile_column(row, feature);
                copy_cache_vector(&keys[row][column], p.keys + offset, present);
                copy_cache_vector(&values[row][column], p.values + offset, present);
            }
            asm volatile("cp.async.commit_group;\n\tcp.async.wait_group 0;" ::: "memory");
        }
        __syncthreads();

        float scores[mma_keys / 8][4] = {};
        #pragma unroll
        for (unsigned d = 0; d < head_dim / 16; ++d) {
            #pragma unroll
            for (unsigned n = 0; n < mma_keys / 8; ++n) {
                uint32_t b0, b1;
                const unsigned row = n * 8 + (lane & 7);
                const unsigned feature = d * 16 + (lane & 8);
                load_matrix_pair<false>(&keys[row][cache_tile_column(row, feature)], b0, b1);
                relative_mma(scores[n], q[d][0], q[d][1], q[d][2], q[d][3], b0, b1);
            }
        }
        if constexpr (FusedRelative) {
            if (!distant) {
                #pragma unroll
                // Each warp computes relative logits at a shared query position,
                // then shifts the column index by each query's position in the warp.
                for (unsigned n = 0; n < relative_columns / 8; ++n) {
                    const int distance = int(warp_first) - int(tile + n * 8 + quad) + 8;
                    const bool present = unsigned(distance) < p.relative_extent;
                    const auto* base = basis + (present ? size_t(distance) * rel_width : 0);
                    const uint32_t b0 = present ? read_pair(base, pair) : 0;
                    const uint32_t b1 = present ? read_pair(base, pair + 8) : 0;
                    float local[4] = {};
                    relative_mma(local, rq[0], rq[1], rq[2], rq[3], b0, b1);
                    #pragma unroll
                    for (unsigned r = 0; r < 2; ++r)
                        *reinterpret_cast<uint32_t*>(&bias[row_pair[r]][n * 8 + pair]) =
                            pack_pair(__float2bfloat16_rn(local[2 * r]), __float2bfloat16_rn(local[2 * r + 1]));
                }
                __syncwarp();
            }
        }
        float next_max[2] = {maximum[0], maximum[1]};
        #pragma unroll
        for (unsigned n = 0; n < mma_keys / 8; ++n) {
            const unsigned key = tile + n * 8 + pair;
            const bool present = (Pipeline ? pipeline_pages[buffer][n / 2] : pages[n / 2]) != UINT32_MAX;
            #pragma unroll
            for (unsigned r = 0; r < 2; ++r) {
                if constexpr (FusedRelative) {
                    #pragma unroll
                    for (unsigned i = 0; i < 2; ++i) {
                        const unsigned col = key + i;
                        const unsigned distance = pos[r] - col;
                        const bool allowed = valid[r] && present && col >= begin &&
                            (distant || (col <= pos[r] && (!p.window || distance < p.window)));
                        const float rel = !distant && distance < p.relative_extent ?
                            __bfloat162float(bias[row_pair[r]][n * 8 + pair + i + 8 - (pos[r] - warp_first)]) : 0.0f;
                        scores[n][2 * r + i] = allowed ? scores[n][2 * r + i] * p.score_scale + rel : -INFINITY;
                    }
                } else {
                    scores[n][2 * r] = query_score(scores[n][2 * r], p, query_head[r], pos[r], key, valid[r] && present && key >= begin, distant);
                    scores[n][2 * r + 1] = query_score(scores[n][2 * r + 1], p, query_head[r], pos[r], key + 1, valid[r] && present && key + 1 >= begin, distant);
                }
                next_max[r] = fmaxf(next_max[r], fmaxf(scores[n][2 * r], scores[n][2 * r + 1]));
            }
        }

        float carry[2];
        #pragma unroll
        for (unsigned r = 0; r < 2; ++r) {
            next_max[r] = fmaxf(next_max[r], __shfl_xor_sync(0xffffffff, next_max[r], 1));
            next_max[r] = fmaxf(next_max[r], __shfl_xor_sync(0xffffffff, next_max[r], 2));
            carry[r] = next_max[r] == -INFINITY ? 1.0f : __expf(maximum[r] - next_max[r]);
            const float center = next_max[r] == -INFINITY ? 0.0f : next_max[r];
            float sum = 0.0f;
            #pragma unroll
            for (unsigned n = 0; n < mma_keys / 8; ++n) {
                scores[n][2 * r] = __expf(scores[n][2 * r] - center);
                scores[n][2 * r + 1] = __expf(scores[n][2 * r + 1] - center);
                sum += scores[n][2 * r] + scores[n][2 * r + 1];
            }
            sum += __shfl_xor_sync(0xffffffff, sum, 1);
            sum += __shfl_xor_sync(0xffffffff, sum, 2);
            denom[r] = __fmaf_rn(denom[r], carry[r], sum);
            maximum[r] = next_max[r];
        }

        #pragma unroll
        for (unsigned d = 0; d < head_dim / 8; ++d) {
            output[d][0] *= carry[0];
            output[d][1] *= carry[0];
            output[d][2] *= carry[1];
            output[d][3] *= carry[1];
        }
        #pragma unroll
        for (unsigned n = 0; n < mma_keys / 8; n += 2) {
            const ProbabilityPair p0 = probability_pair(scores[n], 0);
            const ProbabilityPair p1 = probability_pair(scores[n], 2);
            const ProbabilityPair p2 = probability_pair(scores[n + 1], 0);
            const ProbabilityPair p3 = probability_pair(scores[n + 1], 2);
            #pragma unroll
            for (unsigned d = 0; d < head_dim / 8; ++d) {
                uint32_t b0, b1;
                const unsigned row = n * 8 + (lane & 15);
                load_matrix_pair<true>(&values[row][cache_tile_column(row, d * 8)], b0, b1);
                relative_mma(output[d], p0.high, p1.high, p2.high, p3.high, b0, b1);
                relative_mma(output[d], p0.low, p1.low, p2.low, p3.low, b0, b1);
            }
        }
        __syncthreads();
        if constexpr (Pipeline) buffer ^= 1;
    }

    #pragma unroll
    for (unsigned r = 0; r < 2; ++r) {
        if (!valid[r]) continue;
        if (p.splits == 1) {
            const float sink = __bfloat162float(p.sinks[query_head[r]]);
            const float center = fmaxf(sink, maximum[r]);
            const float mass = expf(maximum[r] - center);
            const float total = __fmaf_rn(mass, denom[r], expf(sink - center));
            #pragma unroll
            for (unsigned d = 0; d < head_dim / 8; ++d) {
                const size_t idx = query_head[r] * head_dim + d * 8 + pair;
                const float gate0 = __bfloat162float(p.gate[idx]);
                const float gate1 = __bfloat162float(p.gate[idx + 1]);
                p.output[idx] = __float2bfloat16_rn((__fmaf_rn(mass, output[d][r * 2], 0.0f) / total) / (1.0f + expf(-gate0)));
                p.output[idx + 1] = __float2bfloat16_rn((__fmaf_rn(mass, output[d][r * 2 + 1], 0.0f) / total) / (1.0f + expf(-gate1)));
            }
            continue;
        }
        float* dst = p.partials + (query_head[r] * p.splits + split) * partial_width;
        #pragma unroll
        for (unsigned d = 0; d < head_dim / 8; ++d) {
            dst[d * 8 + pair] = output[d][r * 2];
            dst[d * 8 + pair + 1] = output[d][r * 2 + 1];
        }
        if (pair == 0) {
            dst[head_dim] = maximum[r];
            dst[head_dim + 1] = denom[r];
        }
    }
}
} // namespace

// Each warp owns 16 token/head rows and keeps its softmax and MMA
// accumulators in registers. Warps share the page translations and KV tiles.
extern "C" __global__ __launch_bounds__(256)
void antfly_paged_relative_gqa_d128_bf16_page16_m128n64_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p, unsigned query_rows
) {
    paged_relative_tile<>(p, query_rows);
}

extern "C" __global__ __launch_bounds__(256)
void antfly_paged_relative_gqa_d128_bf16_page16_m128n64_pipeline(
    const __grid_constant__ PagedRelativeAttentionParams p, unsigned query_rows
) {
    paged_relative_tile<true>(p, query_rows);
}

extern "C" __global__ __launch_bounds__(256)
void antfly_paged_relative_gqa_d128_bf16_page16_m128n64_fused_relative(
    const __grid_constant__ PagedRelativeAttentionParams p, unsigned query_rows,
    const __nv_bfloat16* relative_query, const __nv_bfloat16* relative_basis
) {
    paged_relative_tile<false, true>(p, query_rows, relative_query, relative_basis);
}

extern "C" __global__ void antfly_paged_relative_gqa_d128_bf16_parallel_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    const unsigned lane = threadIdx.x % 32;
    const unsigned warp = threadIdx.x / 32;
    const unsigned split = blockIdx.x;
    const unsigned query_head = blockIdx.y;
    const unsigned kv_head = query_head / (p.q_heads / p.kv_heads);
    const Chunk chunk = chunk_range(p, split, 0);
    const unsigned count = chunk.begin < chunk.end ? chunk.end - chunk.begin : 0;
    float* dst = p.partials + ((size_t)query_head * p.splits + split) * partial_width;
    float value[4] = {};
    if (count == 0) {
        if (warp == 0) store_partial(dst, lane, value, -INFINITY, 0.0f);
        return;
    }

    const __nv_bfloat16* row_q = p.query + (size_t)query_head * head_dim;
    const __nv_bfloat16* rel = p.relative;
    if (p.relative_extent) rel += (size_t)query_head * p.relative_extent;
    __shared__ float scores[parallel_chunk_keys];
    __shared__ unsigned char valid[parallel_chunk_keys];
    __shared__ __nv_bfloat16 values[parallel_chunk_keys][head_dim];
    float q[4];
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) q[i] = __bfloat162float(row_q[i * 32u + lane]);
    for (unsigned i = warp; i < count; i += parallel_chunk_warps) {
        const unsigned key_pos = chunk.begin + i;
        size_t row;
        const bool present = cache_row(p, key_pos, row);
        if (lane == 0) valid[i] = present;
        if (!present) continue;
        const size_t base = (row * p.kv_heads + kv_head) * head_dim;
        float dot = 0.0f;
        #pragma unroll
        for (unsigned j = 0; j < 4; ++j) {
            const unsigned component = j * 32u + lane;
            dot = __fmaf_rn(q[j], __bfloat162float(p.keys[base + component]), dot);
            values[i][component] = p.values[base + component];
        }
        dot = antfly_row_warp_sum(dot);
        if (lane == 0) {
            const unsigned distance = chunk.position - key_pos;
            const float bias = distance < p.relative_extent ? __bfloat162float(rel[distance]) : 0.0f;
            scores[i] = dot * p.score_scale + bias;
        }
    }
    __syncthreads();
    if (warp != 0) return;

    // Consume the staged keys in the same order as the serial chunk.
    float maximum = -INFINITY, denom = 0.0f;
    #pragma unroll 4
    for (unsigned i = 0; i < count; ++i) {
        if (!valid[i]) continue;
        const float score = scores[i];
        const float updated_max = fmaxf(score, maximum);
        const float prev = expf(maximum - updated_max);
        const float next = expf(score - updated_max);
        denom = __fmaf_rn(denom, prev, next);
        #pragma unroll
        for (unsigned j = 0; j < 4; ++j)
            value[j] = __fmaf_rn(next, __bfloat162float(values[i][j * 32u + lane]), value[j] * prev);
        maximum = updated_max;
    }
    store_partial(dst, lane, value, maximum, denom);
}

// Each block shares a paged K/V tile across three query heads.
template<unsigned tile_keys, unsigned warps, bool HeadsFirst>
__device__ __forceinline__ void page16_shared_decode(const PagedRelativeAttentionParams& p) {
    constexpr unsigned heads = 3;
    const unsigned lane = threadIdx.x & 31;
    const unsigned warp = threadIdx.x >> 5;
    const unsigned split = HeadsFirst ? blockIdx.y : blockIdx.x;
    const unsigned kv_head = HeadsFirst ? blockIdx.x : blockIdx.y;
    const unsigned first_head = kv_head * heads;
    const Chunk chunk = chunk_range(p, split, 0);
    __shared__ float scores[heads][tile_keys];
    __shared__ __nv_bfloat16 values[tile_keys][head_dim];
    __shared__ bool valid[tile_keys];
    __shared__ float2 weights[heads][tile_keys];
    float q[heads][4];
    #pragma unroll
    for (unsigned h = 0; h < heads; ++h) {
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i)
            q[h][i] = __bfloat162float(p.query[(first_head + h) * head_dim + i * 32 + lane]);
    }
    float maximum = -INFINITY, denom = 0.0f, value[4] = {};
    for (unsigned begin = chunk.begin; begin < chunk.end; begin += tile_keys) {
        const unsigned count = min(tile_keys, chunk.end - begin);
        for (unsigned i = warp; i < count; i += warps) {
            const unsigned key_pos = begin + i;
            const uint32_t page = cache_page16(p, key_pos);
            const bool present = page != UINT32_MAX;
            if (lane == 0) valid[i] = present;
            if (!present) continue;
            const size_t row = (size_t(page) << 4) + (key_pos & 15u);
            const size_t base = (row * p.kv_heads + kv_head) * head_dim;
            float dot[heads] = {};
            #pragma unroll
            for (unsigned j = 0; j < 4; ++j) {
                const unsigned component = j * 32 + lane;
                const float key = __bfloat162float(p.keys[base + component]);
                values[i][component] = p.values[base + component];
                #pragma unroll
                for (unsigned h = 0; h < heads; ++h) dot[h] = __fmaf_rn(q[h][j], key, dot[h]);
            }
            #pragma unroll
            for (unsigned h = 0; h < heads; ++h) {
                const float sum = antfly_row_warp_sum(dot[h]);
                if (lane == 0) {
                    const unsigned distance = chunk.position - key_pos;
                    const float bias = distance < p.relative_extent ?
                        __bfloat162float(p.relative[(first_head + h) * p.relative_extent + distance]) : 0.0f;
                    scores[h][i] = sum * p.score_scale + bias;
                }
            }
        }
        __syncthreads();
        if (warp < heads) {
            // Prefix maxima give each key the same two exponential weights
            // as the scalar online softmax. Each lane computes one key.
            constexpr unsigned full_warp = 0xffffffffu;
            for (unsigned block = 0; block < count; block += 32u) {
                const unsigned key = block + lane;
                const bool present = key < count && valid[key];
                const float score = present ? scores[warp][key] : -INFINITY;
                float prefix = fmaxf(score, maximum);
                #pragma unroll
                for (unsigned distance = 1; distance < 32; distance *= 2) {
                    const float prior = __shfl_up_sync(full_warp, prefix, distance);
                    if (lane >= distance) prefix = fmaxf(prefix, prior);
                }
                const float prior = __shfl_up_sync(full_warp, prefix, 1);
                const float before = lane == 0 ? maximum : prior;
                if (present) weights[warp][key] = make_float2(expf(before - prefix), expf(score - prefix));
                maximum = __shfl_sync(full_warp, prefix, 31);
            }
            __syncwarp();

            // Preserve the ascending-key FMA order for each output feature.
            #pragma unroll 4
            for (unsigned i = 0; i < count; ++i) {
                if (!valid[i]) continue;
                const float2 w = weights[warp][i];
                denom = __fmaf_rn(denom, w.x, w.y);
                #pragma unroll
                for (unsigned j = 0; j < 4; ++j)
                    value[j] = __fmaf_rn(w.y, __bfloat162float(values[i][j * 32 + lane]), value[j] * w.x);
            }
        }
        __syncthreads();
    }
    if (warp < heads) {
        float* dst = p.partials + (size_t(first_head + warp) * p.splits + split) * partial_width;
        store_partial(dst, lane, value, maximum, denom);
    }
}

extern "C" __global__ void antfly_paged_relative_gqa3_d128_bf16_page16_global_decode_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    page16_shared_decode<48, 12, true>(p);
}

extern "C" __global__ void antfly_paged_relative_gqa3_d128_bf16_page16_sliding_decode_chunks(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    page16_shared_decode<64, 24, false>(p);
}

extern "C" __global__ void antfly_paged_relative_gqa_d128_bf16_reduce_sink_gate(
    const __grid_constant__ PagedRelativeAttentionParams p
) {
    const unsigned row = blockIdx.x;
    const unsigned lane = threadIdx.x;
    const float* partial = p.partials + (size_t)row * p.splits * partial_width;
    const float sink = __bfloat162float(p.sinks[row]);
    float max_logit = sink;
    for (unsigned split = 0; split < p.splits; ++split)
        max_logit = fmaxf(max_logit, partial[(size_t)split * partial_width + head_dim]);
    float denom = expf(sink - max_logit);
    float numerator[4] = {};
    for (unsigned split = 0; split < p.splits; ++split) {
        const float* src = partial + (size_t)split * partial_width;
        const float weight = expf(src[head_dim] - max_logit);
        denom = __fmaf_rn(weight, src[head_dim + 1], denom);
        #pragma unroll
        for (unsigned i = 0; i < 4; ++i)
            numerator[i] = __fmaf_rn(weight, src[i * 32u + lane], numerator[i]);
    }
    const size_t base = (size_t)row * head_dim + lane;
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) {
        const size_t idx = base + i * 32u;
        const float gate = __bfloat162float(p.gate[idx]);
        p.output[idx] = __float2bfloat16_rn((numerator[i] / denom) / (1.0f + expf(-gate)));
    }
}

extern "C" __global__ void antfly_paged_kv_append_bf16(
    uint32_t* key, uint32_t* value, const uint32_t* src_key, const uint32_t* src_value,
    const uint32_t* ids, const uint32_t* position, unsigned page_tokens,
    unsigned pairs, unsigned physical_pages
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= pairs) return;
    const unsigned slot = *position;
    const unsigned page = ids[slot / page_tokens];
    if (page >= physical_pages) return;
    const size_t dest = ((size_t)page * page_tokens + slot % page_tokens) * pairs + i;
    key[dest] = src_key[i];
    value[dest] = src_value[i];
}

extern "C" __global__ void antfly_paged_kv_append_rows_bf16(
    uint4* key, uint4* value, const uint4* src_key, const uint4* src_value, const uint32_t* ids,
    uint32_t start, uint32_t page_tokens, uint32_t first_page, uint32_t page_count,
    uint32_t physical_pages, uint32_t vectors
) {
    const unsigned row = blockIdx.x;
    const unsigned position = start + row;
    const unsigned logical = position / page_tokens - first_page;
    if (logical >= page_count) return;
    const unsigned physical = ids[logical];
    if (physical >= physical_pages) return;
    const size_t source = size_t(row) * vectors;
    const size_t destination = (size_t(physical) * page_tokens + position % page_tokens) * vectors;
    for (unsigned vector = threadIdx.x; vector < vectors; vector += blockDim.x) {
        key[destination + vector] = src_key[source + vector];
        value[destination + vector] = src_value[source + vector];
    }
}
