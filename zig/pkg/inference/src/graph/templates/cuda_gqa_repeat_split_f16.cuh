// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "cuda_gqa_repeat_prefill_f16.cuh"

// Repeat-only HD512 global attention. Scores/alpha and beta are [query, head][key].
// State is [query, head][maximum, denominator]. The output buffer holds
// unnormalized accumulators until the last slab. All launches share one stream.
namespace antfly_repeat_split {
constexpr unsigned slab_capacity = 4096;
__device__ __forceinline__ uint64_t physical(unsigned key, const unsigned* table) {
    return table ? uint64_t(table[key / 16]) * 16 + key % 16 : key;
}
}

// Each half-warp owns one key. Its 16 lanes own the 16 groups of 32
// dimensions, reducing each group locally with the identical XOR-16/8/4/2/1
// addition tree. The final group tree stays in the half-warp. A block covers
// eight queries and at most 256 keys, using the row-major slab scratch format.
extern "C" __global__ void antfly_gqa_repeat_split_scores_f32(
    float* scores, const float* q, const half* k, const unsigned* table,
    unsigned queries, unsigned offset, unsigned begin, unsigned count,
    unsigned capacity) {
    if (queries < 2 || queries > 512 || count == 0 || count > 4096 ||
        begin > 65536 - count || offset > 65536 - queries ||
        capacity < offset + queries || capacity > 65536 || blockDim.x != 256) return;
    const unsigned lane = threadIdx.x % 32;
    const unsigned group = lane % 16, key_lane = lane / 16;
    const unsigned head = blockIdx.x % 8;
    const unsigned query_tiles = (queries + 7) / 8;
    const unsigned qi = ((blockIdx.x / 8) % query_tiles) * 8 + threadIdx.x / 32;
    if (qi >= queries || begin > offset + qi) return;
    const unsigned row = qi * 8 + head;
    const unsigned start = (blockIdx.x / (8 * query_tiles)) * 256;
    const unsigned end = min(min(count, offset + qi + 1 - begin), start + 256);
    if (start >= end) return;
    float query[32];
#pragma unroll
    for (unsigned d = 0; d < 32; ++d)
        query[d] = q[row * 512 + group * 32 + d];
    float scale = 0;
    if (group == 0) {
        volatile unsigned dimension = 512;
        scale = rsqrtf(float(dimension));
    }
    for (unsigned pair = start; pair < end; pair += 2) {
        const unsigned local = pair + key_lane;
        bool valid = local < end;
        const uint64_t mapped = valid ? antfly_repeat_split::physical(begin + local, table) : 0;
        valid = valid && mapped < capacity;
        // Fuse loading with the first addition level to bound register use.
        // Keep each remaining level explicit: a nested shift loop can cause
        // nvcc to materialize the temporary array in local memory.
        float products[16];
        if (valid) {
            const uint4* keys = reinterpret_cast<const uint4*>(k + size_t(mapped) * 1024 + (head / 4) * 512 + group * 32);
#pragma unroll
            for (unsigned pack = 0; pack < 2; ++pack) {
                const uint4 low = keys[pack], high = keys[pack + 2];
                const unsigned lows[4] = {low.x, low.y, low.z, low.w};
                const unsigned highs[4] = {high.x, high.y, high.z, high.w};
#pragma unroll
                for (unsigned word = 0; word < 4; ++word) {
                    const unsigned d = pack * 8 + word * 2;
                    products[d] = __fadd_rn(
                        __fmul_rn(query[d], __half2float(__ushort_as_half(lows[word] & 65535u))),
                        __fmul_rn(query[d + 16], __half2float(__ushort_as_half(highs[word] & 65535u))));
                    products[d + 1] = __fadd_rn(
                        __fmul_rn(query[d + 1], __half2float(__ushort_as_half(lows[word] >> 16))),
                        __fmul_rn(query[d + 17], __half2float(__ushort_as_half(highs[word] >> 16))));
                }
            }
        } else {
#pragma unroll
            for (unsigned d = 0; d < 16; ++d) products[d] = 0;
        }
#pragma unroll
        for (unsigned d = 0; d < 8; ++d)
            products[d] = __fadd_rn(products[d], products[d + 8]);
#pragma unroll
        for (unsigned d = 0; d < 4; ++d)
            products[d] = __fadd_rn(products[d], products[d + 4]);
#pragma unroll
        for (unsigned d = 0; d < 2; ++d)
            products[d] = __fadd_rn(products[d], products[d + 2]);
#pragma unroll
        for (unsigned d = 0; d < 1; ++d)
            products[d] = __fadd_rn(products[d], products[d + 1]);
        // The reference's 32-lane final tree first adds its zero-padded
        // upper half. Preserve that addition, including signed-zero behavior.
        float sum = __fadd_rn(products[0], 0.0f);
#pragma unroll
        for (unsigned shift = 8; shift > 0; shift >>= 1)
            sum = __fadd_rn(sum, __shfl_xor_sync(0xffffffffu, sum, shift));
        if (valid && group == 0) scores[row * count + local] = __fmul_rn(sum, scale);
    }
}

extern "C" __global__ void antfly_gqa_repeat_split_coefficients_f32(
    float* alpha, float* beta, float* state, unsigned queries,
    unsigned offset, unsigned begin, unsigned count) {
    const unsigned row = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned width = queries * 8;
    if (queries < 2 || queries > 512 || row >= width || count == 0 || count > 4096 ||
        begin > 65536 - count || offset > 65536 - queries) return;
    float maximum = begin == 0 ? -3.402823466e+38f : state[row * 2];
    float denominator = begin == 0 ? 0 : state[row * 2 + 1];
    const unsigned end = min(begin + count, offset + row / 8 + 1);
    for (unsigned key = begin; key < end; ++key) {
        const unsigned index = row * count + key - begin;
        const float score = alpha[index];
        const float next = fmaxf(maximum, score);
        const float a = expf(__fsub_rn(maximum, next));
        const float b = expf(__fsub_rn(score, next));
        denominator = __fmaf_rn(denominator, a, b);
        maximum = next;
        alpha[index] = a;
        beta[index] = b;
    }
    state[row * 2] = maximum;
    state[row * 2 + 1] = denominator;
}

// Each lane owns two aligned packs of eight dimensions. Vector loads reduce
// memory instructions without changing any dimension's chronological recurrence.
extern "C" __global__ void antfly_gqa_repeat_split_values_f32(
    float* output, const float* alpha, const float* beta, const float* state,
    const half* v, const unsigned* table, unsigned queries, unsigned offset,
    unsigned begin, unsigned count, unsigned capacity, unsigned final_slab) {
    const unsigned qi = (blockIdx.x / 8) * 8 + threadIdx.x / 32;
    const unsigned row = qi * 8 + blockIdx.x % 8;
    const unsigned lane = threadIdx.x % 32;
    if (queries < 2 || queries > 512 || qi >= queries || blockDim.x != 256 ||
        count == 0 || count > 4096 || begin > 65536 - count ||
        offset > 65536 - queries || capacity < offset + queries || capacity > 65536) return;
    float accumulators[16];
#pragma unroll
    for (unsigned group = 0; group < 16; ++group)
        accumulators[group] = begin == 0 ? 0 : output[row * 512 + (group / 8) * 256 + lane * 8 + group % 8];
    const unsigned end = min(begin + count, offset + qi + 1);
    for (unsigned key = begin; key < end; ++key) {
        const uint64_t mapped = antfly_repeat_split::physical(key, table);
        if (mapped >= capacity) return;
        const unsigned index = row * count + key - begin;
        const float a = alpha[index], b = beta[index];
        const half* values = v + size_t(mapped) * 1024 + (row % 8 / 4) * 512;
#pragma unroll
        for (unsigned pack = 0; pack < 2; ++pack) {
            const uint4 loaded = *reinterpret_cast<const uint4*>(values + pack * 256 + lane * 8);
            const unsigned words[4] = {loaded.x, loaded.y, loaded.z, loaded.w};
#pragma unroll
            for (unsigned word = 0; word < 4; ++word) {
                const unsigned group = pack * 8 + word * 2;
                accumulators[group] = __fmaf_rn(__half2float(__ushort_as_half(words[word] & 65535u)), b, __fmul_rn(accumulators[group], a));
                accumulators[group+1] = __fmaf_rn(__half2float(__ushort_as_half(words[word] >> 16)), b, __fmul_rn(accumulators[group+1], a));
            }
        }
    }
    const float denominator = state[row * 2 + 1];
#pragma unroll
    for (unsigned group = 0; group < 16; ++group)
        output[row * 512 + (group / 8) * 256 + lane * 8 + group % 8] = final_slab
            ? (denominator > 0 ? accumulators[group] / denominator : 0) : accumulators[group];
}
