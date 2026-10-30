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
