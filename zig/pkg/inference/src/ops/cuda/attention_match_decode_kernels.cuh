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
struct AntflyMatchSpan { uint32_t first, rows, offset; };
static_assert(sizeof(AntflyMatchSpan) == 12, "span ABI");
struct AntflyMatchHead {
    const void* keys;
    const void* values;
    const float* bias;
    const uint64_t* positions;
    const uint64_t* coverage_ends;
    uint64_t rows;
    uint64_t capacity;
    uint64_t start;
    const uint8_t* encoded_keys;
    const uint8_t* encoded_values;
    const AntflyMatchSpan* spans;
    uint64_t compressed_rows;
    uint64_t span_count;
};
static_assert(sizeof(AntflyMatchHead) == 104, "head ABI");

// Nonnegative => packed row; negative => -(exact row + 1).
__device__ int64_t am_index(const AntflyMatchHead& head, uint64_t row) {
    uint64_t low = 0, high = head.span_count;
    while (low < high) {
        const uint64_t mid = low + (high - low) / 2;
        if (head.spans[mid].first <= row) low = mid + 1; else high = mid;
    }
    if (!low) return -int64_t(row) - 1;
    const AntflyMatchSpan span = head.spans[low - 1];
    if (row - span.first < span.rows) return int64_t(span.offset) + row - span.first;
    return -int64_t(row - (uint64_t(span.offset) + span.rows)) - 1;
}
__device__ __constant__ float am_centroids[16] = {
    -2.73258957099535f, -2.06901722653160f, -1.61804638602210f, -1.25623119734739f,
    -0.94234045648714f, -0.65675911853260f, -0.38804829949038f, -0.12839502985118f,
    0.12839502985118f, 0.38804829949038f, 0.65675911853260f, 0.94234045648714f,
    1.25623119734739f, 1.61804638602210f, 2.06901722653160f, 2.73258957099535f
};
__device__ float am_sign(int round, int d) {
    uint64_t x = (round ? 0x24d8a6511b73c409ULL : 0x8f3f73b5cf1c9adeULL) + uint64_t(d) * 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return ((x ^ (x >> 31)) & 1) ? -1.f : 1.f;
}
extern "C" __global__ void attention_match_rotate_queries(const float* input, float* output, int dim) {
    __shared__ float values[512];
    const int tid = threadIdx.x;
    const size_t base = size_t(blockIdx.x) * dim;
    for (int d = tid; d < dim; d += 256) values[d] = input[base + d];
    __syncthreads();
    const float norm = __fdiv_rn(1.f, __fsqrt_rn(float(dim)));
    for (int round = 0; round < 2; ++round) {
        for (int d = tid; d < dim; d += 256) values[d] *= am_sign(round, d);
        __syncthreads();
        for (int width = 1; width < dim; width *= 2) {
            if (tid < dim / 2) {
                const int a = (tid / width) * (2 * width) + tid % width;
                const float left = values[a], right = values[a + width];
                values[a] = left + right;
                values[a + width] = left - right;
            }
            __syncthreads();
        }
        for (int d = tid; d < dim; d += 256) values[d] *= norm;
        __syncthreads();
    }
    for (int d = tid; d < dim; d += 256) output[base + d] = values[d];
}

template<class T> __device__ float am_load(const T* p) { return float(*p); }
template<> __device__ float am_load(const __half* p) { return __half2float(*p); }

template<class T>
__device__ void am_decode_partial(const float* queries, const float* rotated_queries, const AntflyMatchHead* heads,
    float* partials, int query_heads, int kv_heads, int dim, int splits,
    uint64_t first_query_position, float scale, float softcap, uint64_t window) {
    __shared__ float scores[256];
    __shared__ int64_t row_indices[256];
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
        int64_t index = -1;
        if (visible) {
            index = am_index(head, row);
            if (index >= 0) {
                const uint8_t* k = head.encoded_keys + size_t(index) * (4 + dim / 2);
                const float* qr = rotated_queries + size_t(query) * dim;
                for (int d = lane; d < dim; d += 32) {
                    const int code = (k[4 + d / 2] >> ((d & 1) * 4)) & 15;
                    dot = fmaf(qr[d], am_centroids[code], dot);
                }
                dot *= *reinterpret_cast<const float*>(k);
            } else {
                const size_t exact = size_t(-index - 1);
                for (int d = lane; d < dim; d += 32) dot = fmaf(q[d], am_load(keys + exact * dim + d), dot);
            }
        }
        for (int offset = 16; offset; offset >>= 1)
            dot += __shfl_down_sync(0xffffffff, dot, offset);
        if (lane == 0) {
            row_indices[r] = index;
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
                const int64_t index = row_indices[r];
                float value;
                if (index >= 0) {
                    const uint8_t* v = head.encoded_values + size_t(index) * (dim + 4);
                    value = float(reinterpret_cast<const int8_t*>(v + 4)[d]) * *reinterpret_cast<const float*>(v);
                } else value = am_load(values + size_t(-index - 1) * dim + d);
                sum = fmaf(scores[r], value, sum);
            }
        }
        out[d] = sum;
    }
}

#define AM_PARTIAL(NAME, TYPE) \
extern "C" __global__ void NAME(const float* q, const AntflyMatchHead* heads, float* partials, \
    int query_heads, int kv_heads, int dim, int splits, uint64_t first_query_position, \
    float scale, float softcap, uint64_t window) { \
    am_decode_partial<TYPE>(q, nullptr, heads, partials, query_heads, kv_heads, dim, splits, \
        first_query_position, scale, softcap, window); \
}
AM_PARTIAL(attention_match_partial_f16, __half)
AM_PARTIAL(attention_match_partial_f32, float)
#undef AM_PARTIAL
#define AM_ROTATED(NAME, TYPE) \
extern "C" __global__ void NAME(const float* q, const float* qr, const AntflyMatchHead* heads, float* partials, \
    int query_heads, int kv_heads, int dim, int splits, uint64_t first_query_position, \
    float scale, float softcap, uint64_t window) { \
    am_decode_partial<TYPE>(q, qr, heads, partials, query_heads, kv_heads, dim, splits, \
        first_query_position, scale, softcap, window); \
}
AM_ROTATED(attention_match_rotated_f16, __half)
AM_ROTATED(attention_match_rotated_f32, float)
#undef AM_ROTATED

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
    const size_t exact = physical - head.compressed_rows;
    const_cast<T*>(static_cast<const T*>(head.keys))[exact * dim + d] = T(k);
    const_cast<T*>(static_cast<const T*>(head.values))[exact * dim + d] = T(v);
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
