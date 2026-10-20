// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Ragged, bias/coverage-aware attention. Integration and release qualification
// are separate from compiling this artifact. All launches use 256 threads.
#pragma once
#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <math_constants.h>
#include <stdint.h>

// Explicit ABI shared with attention_match_decode.zig. Rows can wrap in a
// local-window ring; distinct heads need not have the same physical length.
struct AntflyMatchHead {
    const void* keys;
    const void* values;
    const float* bias;
    const uint64_t* positions;
    const uint64_t* coverage_ends;
    uint64_t rows;
    uint64_t capacity;
    uint64_t start;
};
static_assert(sizeof(AntflyMatchHead) == 64, "head ABI");

template<class T> __device__ float am_load(const T* p) { return float(*p); }
template<> __device__ float am_load(const __half* p) { return __half2float(*p); }

template<class T>
__device__ void am_decode_partial(const float* queries, const AntflyMatchHead* heads,
    float* partials, int query_heads, int kv_heads, int dim, int splits,
    uint64_t first_query_position, float scale, float softcap, uint64_t window) {
    __shared__ float scores[256];
    __shared__ float scratch[256];
    const int tid = threadIdx.x, lane = tid & 31, warp = tid >> 5;
    const int query = blockIdx.y;
    const AntflyMatchHead head = heads[(query % query_heads) / (query_heads / kv_heads)];
    const uint64_t qp = first_query_position + uint64_t(query / query_heads);
    const uint64_t first = window && qp >= window - 1 ? qp - (window - 1) : 0;
    const T* keys = static_cast<const T*>(head.keys);
    const T* values = static_cast<const T*>(head.values);
    const float* q = queries + size_t(query) * dim;
    for (int r = warp; r < 256; r += 8) {
        const uint64_t logical_row = uint64_t(blockIdx.x) * 256 + r;
        const uint64_t row = (head.start + logical_row) % head.capacity;
        bool visible = logical_row < head.rows;
        if (visible) {
            const uint64_t end = head.coverage_ends[row], pos = head.positions[row];
            visible = end && end - 1 <= qp && pos <= qp && (!window || pos >= first);
        }
        float dot = 0;
        if (visible) for (int d = lane; d < dim; d += 32)
            dot = fmaf(q[d], am_load(keys + row * dim + d), dot);
        for (int offset = 16; offset; offset >>= 1)
            dot += __shfl_down_sync(0xffffffff, dot, offset);
        if (lane == 0) {
            dot *= scale;
            if (softcap > 0) dot = softcap * tanhf(dot / softcap);
            scores[r] = visible ? dot + head.bias[row] : -CUDART_INF_F;
        }
    }
    __syncthreads();
    scratch[tid] = scores[tid];
    __syncthreads();
    for (int stride = 128; stride; stride >>= 1) {
        if (tid < stride) scratch[tid] = fmaxf(scratch[tid], scratch[tid + stride]);
        __syncthreads();
    }
    const float maximum = scratch[0];
    __syncthreads(); // all readers finish before sum reduction reuses scratch
    const float probability = maximum == -CUDART_INF_F ? 0 : expf(scores[tid] - maximum);
    scores[tid] = probability;
    scratch[tid] = probability;
    __syncthreads();
    for (int stride = 128; stride; stride >>= 1) {
        if (tid < stride) scratch[tid] += scratch[tid + stride];
        __syncthreads();
    }
    float* out = partials + (size_t(query) * splits + blockIdx.x) * (dim + 2);
    if (tid == 0) { out[dim] = scratch[0]; out[dim + 1] = maximum; }
    for (int d = tid; d < dim; d += 256) {
        float sum = 0;
        for (int r = 0; r < 256; ++r) {
            const uint64_t logical_row = uint64_t(blockIdx.x) * 256 + r;
            if (logical_row < head.rows && scores[r] != 0) {
                const uint64_t row = (head.start + logical_row) % head.capacity;
                sum = fmaf(scores[r], am_load(values + row * dim + d), sum);
            }
        }
        out[d] = sum;
    }
}

#define AM_PARTIAL(NAME, TYPE) \
extern "C" __global__ void NAME(const float* q, const AntflyMatchHead* heads, float* partials, \
    int query_heads, int kv_heads, int dim, int splits, uint64_t first_query_position, \
    float scale, float softcap, uint64_t window) { \
    am_decode_partial<TYPE>(q, heads, partials, query_heads, kv_heads, dim, splits, \
        first_query_position, scale, softcap, window); \
}
AM_PARTIAL(attention_match_partial_f16, __half)
AM_PARTIAL(attention_match_partial_f32, float)
#undef AM_PARTIAL

extern "C" __global__ void attention_match_merge(const float* partials, float* output,
    float* log_mass, int dim, int splits) {
    __shared__ float scratch[256];
    const int tid = threadIdx.x;
    const float* p = partials + size_t(blockIdx.x) * splits * (dim + 2);
    float maximum = -CUDART_INF_F;
    for (int s = tid; s < splits; s += 256) maximum = fmaxf(maximum, p[size_t(s) * (dim + 2) + dim + 1]);
    scratch[tid] = maximum;
    __syncthreads();
    for (int stride = 128; stride; stride >>= 1) {
        if (tid < stride) scratch[tid] = fmaxf(scratch[tid], scratch[tid + stride]);
        __syncthreads();
    }
    maximum = scratch[0];
    __syncthreads();
    float mass = 0;
    if (maximum != -CUDART_INF_F) for (int s = tid; s < splits; s += 256)
        mass += p[size_t(s) * (dim + 2) + dim] * expf(p[size_t(s) * (dim + 2) + dim + 1] - maximum);
    scratch[tid] = mass;
    __syncthreads();
    for (int stride = 128; stride; stride >>= 1) {
        if (tid < stride) scratch[tid] += scratch[tid + stride];
        __syncthreads();
    }
    mass = scratch[0];
    for (int d = tid; d < dim; d += 256) {
        float value = 0;
        if (mass > 0) for (int s = 0; s < splits; ++s)
            value = fmaf(p[size_t(s) * (dim + 2) + d], expf(p[size_t(s) * (dim + 2) + dim + 1] - maximum), value);
        output[size_t(blockIdx.x) * dim + d] = mass > 0 ? value / mass : 0;
    }
    if (tid == 0 && log_mass) log_mass[blockIdx.x] = mass > 0 ? maximum + logf(mass) : -CUDART_INF_F;
}

template<class T>
__device__ void am_append(const float* keys, const float* values, const AntflyMatchHead* heads,
    int kv_heads, int dim, int rows, uint64_t first_position, int* invalid) {
    const size_t i = size_t(blockIdx.x) * blockDim.x + threadIdx.x;
    if (i >= size_t(rows) * kv_heads * dim) return;
    const int d = i % dim, h = (i / dim) % kv_heads;
    const size_t r = i / (size_t(dim) * kv_heads);
    const float k = keys[i], v = values[i];
    if (!isfinite(k) || !isfinite(v) || (sizeof(T) == 2 && (fabsf(k) > 65504 || fabsf(v) > 65504))) {
        atomicExch(invalid, 1);
        return;
    }
    const AntflyMatchHead head = heads[h];
    // The host reserves an unused tail before dispatch. Failed validation or
    // cancellation can discard the tail without altering any published row.
    const size_t physical = (head.start + head.rows + r) % head.capacity;
    const_cast<T*>(static_cast<const T*>(head.keys))[physical * dim + d] = T(k);
    const_cast<T*>(static_cast<const T*>(head.values))[physical * dim + d] = T(v);
    if (d == 0) {
        const_cast<float*>(head.bias)[physical] = 0;
        const_cast<uint64_t*>(head.positions)[physical] = first_position + r;
        const_cast<uint64_t*>(head.coverage_ends)[physical] = first_position + r + 1;
    }
}
#define AM_APPEND(NAME, TYPE) \
extern "C" __global__ void NAME(const float* k, const float* v, const AntflyMatchHead* heads, \
    int kv_heads, int dim, int rows, uint64_t first_position, int* invalid) { \
    am_append<TYPE>(k, v, heads, kv_heads, dim, rows, first_position, invalid); \
}
AM_APPEND(attention_match_append_f16, __half)
AM_APPEND(attention_match_append_f32, float)
#undef AM_APPEND
