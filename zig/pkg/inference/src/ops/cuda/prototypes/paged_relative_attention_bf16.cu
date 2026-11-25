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
