// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Qualification reference only. Not registered in production dispatch.
// Dense head-major f32 K/V; explicit per-KV-head bias and coverage metadata.
// Three passes expose an unambiguous numerical baseline for future fused kernels.
#pragma once
#include <cuda_runtime.h>
#include <math_constants.h>
#include <stdint.h>

__global__ void antfly_attention_match_scores(
    const float* q, const float* k, const float* bias,
    const uint64_t* positions, const uint64_t* coverage_ends,
    const uint64_t* query_positions, float* scores,
    int query_rows, int query_heads, int kv_heads, int rows, int dim,
    float scale, float softcap, uint64_t window) {
    const size_t index = size_t(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t total = size_t(query_rows) * query_heads * rows;
    if (index >= total) return;
    const int row = index % rows;
    const size_t query = index / rows;
    const int qh = query % query_heads;
    const int kh = qh / (query_heads / kv_heads);
    const size_t kr = size_t(kh) * rows + row;
    const uint64_t qp = query_positions[query / query_heads];
    const uint64_t pos = positions[kr];
    const uint64_t end = coverage_ends[kr];
    const uint64_t first = window != 0 && qp >= window - 1 ? qp - (window - 1) : 0;
    if (end == 0 || end - 1 > qp || pos > qp || (window != 0 && pos < first)) {
        scores[index] = -CUDART_INF_F;
        return;
    }
    float dot = 0;
    for (int d = 0; d < dim; ++d) dot = fmaf(q[query * dim + d], k[kr * dim + d], dot);
    dot *= scale;
    if (softcap > 0) dot = softcap * tanhf(dot / softcap);
    scores[index] = dot + bias[kr]; // mass must not be soft-capped
}

// Exactly 256 threads per block; one block per query head and position.
__global__ void antfly_attention_match_normalize(float* scores, float* log_mass, int rows) {
    __shared__ float scratch[256];
    const int lane = threadIdx.x;
    float* row = scores + size_t(blockIdx.x) * rows;
    float maximum = -CUDART_INF_F;
    for (int i = lane; i < rows; i += 256) maximum = fmaxf(maximum, row[i]);
    scratch[lane] = maximum;
    __syncthreads();
    for (int stride = 128; stride; stride >>= 1) {
        if (lane < stride) scratch[lane] = fmaxf(scratch[lane], scratch[lane + stride]);
        __syncthreads();
    }
    maximum = scratch[0];
    __syncthreads();
    float sum = 0;
    for (int i = lane; i < rows; i += 256) {
        const float value = maximum == -CUDART_INF_F ? 0 : expf(row[i] - maximum);
        row[i] = value;
        sum += value;
    }
    scratch[lane] = sum;
    __syncthreads();
    for (int stride = 128; stride; stride >>= 1) {
        if (lane < stride) scratch[lane] += scratch[lane + stride];
        __syncthreads();
    }
    sum = scratch[0];
    for (int i = lane; i < rows; i += 256) row[i] = sum > 0 ? row[i] / sum : 0;
    if (lane == 0) log_mass[blockIdx.x] = sum > 0 ? maximum + logf(sum) : -CUDART_INF_F;
}

__global__ void antfly_attention_match_values(
    const float* probabilities, const float* v, float* output,
    int query_rows, int query_heads, int kv_heads, int rows, int dim) {
    const size_t index = size_t(blockIdx.x) * blockDim.x + threadIdx.x;
    if (index >= size_t(query_rows) * query_heads * dim) return;
    const int d = index % dim;
    const size_t query = index / dim;
    const int kh = int(query % query_heads) / (query_heads / kv_heads);
    float sum = 0;
    for (int row = 0; row < rows; ++row)
        sum = fmaf(probabilities[query * rows + row], v[(size_t(kh) * rows + row) * dim + d], sum);
    output[index] = sum;
}
