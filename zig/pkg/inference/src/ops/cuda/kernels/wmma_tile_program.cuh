// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Native 16x16 score product: row-major A, caller-selected B layout and scalar,
// FP32 accumulation/publication. Expand in the original kernel scope so shared
// storage, fragment lifetimes and NVCC address lowering stay unchanged. Address
// expressions may reference d0; the accumulator and fragment names are local to
// the caller's existing block. Keep warm P/V accumulators in their own schedule.
#define ANTFLY_CUDA_WMMA_DOT16_F32(Scalar, LayoutB, Depth, Left, LeftPitch, Right, RightPitch, Result, ResultPitch) \
    wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc; \
    wmma::fill_fragment(acc, 0.0f); \
    _Pragma("unroll") \
    for (unsigned int d0 = 0u; d0 < Depth; d0 += 16u) { \
        wmma::fragment<wmma::matrix_a, 16, 16, 16, Scalar, wmma::row_major> a_frag; \
        wmma::fragment<wmma::matrix_b, 16, 16, 16, Scalar, LayoutB> b_frag; \
        wmma::load_matrix_sync(a_frag, Left, LeftPitch); \
        wmma::load_matrix_sync(b_frag, Right, RightPitch); \
        wmma::mma_sync(acc, a_frag, b_frag, acc); \
    } \
    wmma::store_matrix_sync(Result, acc, ResultPitch, wmma::mem_row_major);

// Preloaded row fragments retain the native M64 register reuse. Unlike the
// two-load phase above, this never reacquires the left operand from shared RAM.
#define ANTFLY_CUDA_WMMA_PRELOADED_DOT16_F32(Accumulator,Rows,Right,Depth,Pitch,Destination,Stride,RightFragment) \
    wmma::fragment<wmma::accumulator,16,16,16,float> Accumulator; \
    wmma::fill_fragment(Accumulator,0.0f); \
    _Pragma("unroll") \
    for(unsigned int d=0u;d<Depth;++d){ \
        wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::col_major> RightFragment; \
        wmma::load_matrix_sync(RightFragment,Right+d*16u,Pitch); \
        wmma::mma_sync(Accumulator,Rows[d],RightFragment,Accumulator); \
    } \
    wmma::store_matrix_sync(Destination,Accumulator,Stride,wmma::mem_row_major)

// Row-major P*V shares one arithmetic body for FP16/BF16 probabilities and
// values. The caller owns the live FP32 accumulator, rescaling, barriers and
// publication. Keep the key loop at the call site: row/key tiles and precision
// correction schedules must remain static rather than runtime branches.
#define ANTFLY_CUDA_WMMA_ACCUMULATE16_F32(Scalar, Accumulator, Left, LeftPitch, Right, RightPitch) \
    wmma::fragment<wmma::matrix_a, 16, 16, 16, Scalar, wmma::row_major> p_frag; \
    wmma::fragment<wmma::matrix_b, 16, 16, 16, Scalar, wmma::row_major> v_frag; \
    wmma::load_matrix_sync(p_frag, Left, LeftPitch); \
    wmma::load_matrix_sync(v_frag, Right, RightPitch); \
    wmma::mma_sync(Accumulator, p_frag, v_frag, Accumulator);

#define ANTFLY_CUDA_WMMA_PV16_F32(KeyLoop, Scalar, Accumulator, Left, LeftPitch, Right, RightPitch) \
    _Pragma("unroll") \
    for (KeyLoop) { \
        ANTFLY_CUDA_WMMA_ACCUMULATE16_F32(Scalar, Accumulator, Left, LeftPitch, Right, RightPitch) \
    }

// Two scores per lane, fixed shuffle order and FP32 online state. Exp and
// the probability publication/rounding boundary are static numerical policies.
#define ANTFLY_CUDA_ATTENTION_ONLINE_ROW64(Exp, ProbabilityStore) \
    for (unsigned int shift = 16u; shift; shift >>= 1u) \
        next_max = fmaxf(next_max, __shfl_down_sync(0xffffffffu, next_max, shift)); \
    next_max = fmaxf(maximum[row], __shfl_sync(0xffffffffu, next_max, 0u)); \
    const float a = isfinite(maximum[row]) ? Exp(maximum[row] - next_max) : 0.0f; \
    float sum = 0.0f; \
    _Pragma("unroll") \
    for (unsigned int j = 0u; j < 2u; ++j) { \
        const float p = isfinite(next_max) ? Exp(values[j] - next_max) : 0.0f; \
        ProbabilityStore \
        sum += p; \
    } \
    for (unsigned int shift = 16u; shift; shift >>= 1u) \
        sum += __shfl_down_sync(0xffffffffu, sum, shift); \
    if (lane == 0u) { \
        alpha[row] = a; \
        denominator[row] = denominator[row] * a + sum; \
        maximum[row] = next_max; \
    }

// Publish aliased probabilities only after all score readers finish.
#define ANTFLY_CUDA_ATTENTION_PUBLISH_HALF_ROWS64() \
    __syncthreads(); \
    _Pragma("unroll") \
    for (unsigned int g = 0u; g < 8u; ++g) { \
        const unsigned int row = warp * 8u + g; \
        _Pragma("unroll") \
        for (unsigned int j = 0u; j < 2u; ++j) \
            probabilities[row * P + lane + j * 32u] = __float2half_rn(staged_probabilities[g][j]); \
    } \
    __syncthreads();

// Public WMMA store/rescale/reload preserves the opaque fragment layout.
#define ANTFLY_CUDA_ATTENTION_PV_ROWS64(Values) \
    _Pragma("unroll") \
    for (unsigned int g = 0u; g < 2u; ++g) { \
        const unsigned int qr = warp / 4u, dc = warp % 4u; \
        wmma::store_matrix_sync(scratch + qr * 16u * S + dc * 16u, output[g], S, wmma::mem_row_major); \
        __syncthreads(); \
        for (unsigned int i = tid; i < 32u * D; i += T) scratch[(i / D) * S + i % D] *= alpha[g * 32u + i / D]; \
        __syncthreads(); \
        wmma::load_matrix_sync(output[g], scratch + qr * 16u * S + dc * 16u, S, wmma::mem_row_major); \
        ANTFLY_CUDA_WMMA_PV16_F32( \
            unsigned int n = 0u; n < N; n += 16u, \
            half, output[g], probabilities + (g * 32u + qr * 16u) * P + n, P, Values + n * P + dc * 16u, P) \
        __syncthreads(); \
    }
