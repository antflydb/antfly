// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "row_program.cuh"

// ATen's contiguous vector_norm/SumOps reduction uses four accumulators
// per lane, then combines lanes in a width-dependent block tree. Squared
// values use FMA; materialized DivBackward values use plain FP32 addition.
__device__ __forceinline__ unsigned int antfly_four_lane_reduction_width(
    unsigned int width, unsigned int rows) {
    unsigned int vectors = 1, outputs = 1;
    while (vectors < 128 && vectors * 2 <= width / 4) vectors *= 2;
    while (outputs <= rows / 2) outputs *= 2;
    // ReduceConfig reserves output lanes before expanding reduction lanes.
    unsigned int lanes = vectors < 32 ? vectors : 32;
    unsigned int height = outputs < 512 / lanes ? outputs : 512 / lanes;
    lanes = vectors < 512 / height ? vectors : 512 / height;
    return lanes < 32 ? 32 : lanes;
}

__device__ __forceinline__ float antfly_four_lane_reduce(
    const float* values, unsigned int lanes) {
    __shared__ float partials[128];
    unsigned int lane = threadIdx.x;
    float value = __fadd_rn(__fadd_rn(__fadd_rn(values[0], values[1]), values[2]), values[3]);
    if (lane < lanes) partials[lane] = value;
    __syncthreads();
    for (unsigned int offset = lanes / 2; offset >= 32; offset >>= 1) {
        if (lane < offset) {
            value = __fadd_rn(value, partials[lane + offset]);
            partials[lane] = value;
        }
        __syncthreads();
    }
    if (lane < 32) {
        value = antfly_row_warp_sum(value);
        if (lane == 0) partials[0] = value;
    }
    __syncthreads();
    return partials[0];
}

template<unsigned FixedStride=0>
__device__ __forceinline__ float antfly_prefix_vector_norm(
    const float* input, unsigned int row, unsigned int width, unsigned int rows, unsigned int input_stride) {
    const unsigned int stride = FixedStride ? FixedStride : input_stride;
    const unsigned int lanes = antfly_four_lane_reduction_width(width, rows);
    float values[4] = {0, 0, 0, 0};
    if (threadIdx.x < lanes) {
        for (unsigned int vector = threadIdx.x; vector * 4 < width; vector += lanes) {
            #pragma unroll
            for (unsigned int i = 0; i < 4; ++i) {
                unsigned int d = vector * 4 + i;
                if (d < width) {
                    float value = input[(size_t)row * stride + d];
                    values[i] = __fmaf_rn(value, value, values[i]);
                }
            }
        }
    }
    return sqrtf(antfly_four_lane_reduce(values, lanes));
}

template<unsigned FixedStride>
__device__ __forceinline__ void antfly_prefix_normalize(
    float* out, const float* input, unsigned int rows, unsigned int width, unsigned int input_stride) {
    const unsigned int stride = FixedStride ? FixedStride : input_stride;
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    float norm = fmaxf(antfly_prefix_vector_norm<FixedStride>(input, row, width, rows, input_stride), 1.0e-12f);
    for (unsigned int d = threadIdx.x; d < width; d += blockDim.x)
        out[(size_t)row * width + d] = __fdiv_rn(input[(size_t)row * stride + d], norm);
}

template<unsigned FixedStride>
__device__ __forceinline__ void antfly_prefix_accumulate_vjp(
    float* dinput, const float* dout, const float* input,
    unsigned int rows, unsigned int width, float objective_weight, unsigned int input_stride) {
    const unsigned int stride = FixedStride ? FixedStride : input_stride;
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    float raw_norm = antfly_prefix_vector_norm<FixedStride>(input, row, width, rows, input_stride);
    float norm = fmaxf(raw_norm, 1.0e-12f);
    const unsigned int lanes = antfly_four_lane_reduction_width(width, rows);
    float values[4] = {0, 0, 0, 0};
    if (threadIdx.x < lanes) {
        for (unsigned int vector = threadIdx.x; vector * 4 < width; vector += lanes) {
            #pragma unroll
            for (unsigned int i = 0; i < 4; ++i) {
                unsigned int d = vector * 4 + i;
                if (d < width) {
                    float ratio = __fdiv_rn(input[(size_t)row * stride + d], norm);
                    ratio = __fdiv_rn(ratio, norm);
                    float product = __fmul_rn(dout[(size_t)row * width + d], ratio);
                    float other = __fsub_rn(0.0f, product);
                    values[i] = __fadd_rn(values[i], other);
                }
            }
        }
    }
    float norm_gradient = antfly_four_lane_reduce(values, lanes);
    if (raw_norm < 1.0e-12f) norm_gradient = 0.0f;
    for (unsigned int d = threadIdx.x; d < width; d += blockDim.x) {
        size_t full = (size_t)row * stride + d;
        float direct = __fdiv_rn(dout[(size_t)row * width + d], norm);
        float ratio = raw_norm > 0.0f ? __fdiv_rn(input[full], raw_norm) : 0.0f;
        float value = __fadd_rn(direct, __fmul_rn(norm_gradient, ratio));
        dinput[full] = __fadd_rn(dinput[full], __fmul_rn(objective_weight, value));
    }
}

// Historical exports keep the qualified 768-column specialization.
extern "C" __global__ void termite_embedding_gemma2_mrl_normalize_f32(
    float* out, const float* input, unsigned rows, unsigned width) {
    antfly_prefix_normalize<768>(out,input,rows,width,768);
}
extern "C" __global__ void termite_embedding_gemma2_mrl_normalize_vjp_f32(
    float* dinput, const float* dout, const float* input, unsigned rows, unsigned width, float objective_weight) {
    antfly_prefix_accumulate_vjp<768>(dinput,dout,input,rows,width,objective_weight,768);
}
extern "C" __global__ void antfly_prefix_l2_normalize_f32(
    float* out, const float* input, unsigned rows, unsigned width, unsigned input_stride) {
    antfly_prefix_normalize<0>(out,input,rows,width,input_stride);
}
extern "C" __global__ void antfly_prefix_l2_accumulate_vjp_f32(
    float* dinput, const float* dout, const float* input, unsigned rows, unsigned width, float objective_weight, unsigned input_stride) {
    antfly_prefix_accumulate_vjp<0>(dinput,dout,input,rows,width,objective_weight,input_stride);
}
