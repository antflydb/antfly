// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cuda_bf16.h>
#include <cuda_fp16.h>
#include <mma.h>

// Same 64x32 output tile, BF16 rounding and chronological 16-wide WMMA
// accumulation as termite_qtc_hmma_tile. Stage two K slices together and
// load weights along their contiguous K dimension. No persistent workspace.
// Launch: 256 threads, ceil(out_dim/32) x ceil(rows/64), in_dim % 32 == 0.
template <unsigned K = 32>
__device__ void antfly_q4_0_bf16_staged(
    float* dst, const float* input, const unsigned char* packed,
    unsigned rows, unsigned in_dim, unsigned out_dim
) {
#if __CUDA_ARCH__ >= 800
    static_assert(K % 16 == 0 && K >= 16 && K <= 128, "invalid staging width");
    namespace wmma = nvcuda::wmma;
    const unsigned tid = threadIdx.x, warp = tid / 32;
    const unsigned row_base = blockIdx.y * 64, col_base = blockIdx.x * 32;
    const unsigned row_blocks = in_dim / 32, block_count = out_dim * row_blocks;
    constexpr unsigned stride = K + 8;
    // The output tile is used only after every warp has finished reading A/B.
    __shared__ __align__(32) union {
        struct { __nv_bfloat16 a[64 * stride], b[32 * stride]; } inputs;
        float c[64 * 32];
    } storage;
    auto* a = storage.inputs.a;
    auto* b = storage.inputs.b;
    auto* c = storage.c;
    wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
    wmma::fill_fragment(acc, 0.0f);
    for (unsigned base = 0; base < in_dim; base += K) {
        for (unsigned i = tid; i < 64 * K; i += 256) {
            const unsigned row = row_base + i / K, k = base + i % K;
            a[(i / K) * stride + i % K] = __float2bfloat16_rn(row < rows && k < in_dim ? input[size_t(row) * in_dim + k] : 0.0f);
        }
        for (unsigned i = tid; i < 32 * K; i += 256) {
            const unsigned col = col_base + i / K, k = base + i % K;
            float value = 0.0f;
            if (col < out_dim && k < in_dim) {
                const unsigned block = col * row_blocks + k / 32;
                const unsigned short scale = static_cast<unsigned short>(packed[block * 2]) |
                    (static_cast<unsigned short>(packed[block * 2 + 1]) << 8);
                const unsigned q = packed[size_t(block_count) * 2 + size_t(block) * 16 + (k & 15)];
                value = float(int((k & 16) ? q >> 4 : q & 15) - 8) * __half2float(__ushort_as_half(scale));
            }
            b[(i / K) * stride + i % K] = __float2bfloat16_rn(value);
        }
        __syncthreads();
        // Do not accumulate zero-padded slices: an extra MMA can change -0.
        #pragma unroll
        for (unsigned slice = 0; slice < K; slice += 16) {
            if (base + slice < in_dim) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, __nv_bfloat16, wmma::row_major> af;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, __nv_bfloat16, wmma::col_major> bf;
                wmma::load_matrix_sync(af, a + (warp & 3) * 16 * stride + slice, stride);
                wmma::load_matrix_sync(bf, b + (warp >> 2) * 16 * stride + slice, stride);
                wmma::mma_sync(acc, af, bf, acc);
            }
        }
        __syncthreads();
    }
    wmma::store_matrix_sync(c + (warp & 3) * 16 * 32 + (warp >> 2) * 16, acc, 32, wmma::mem_row_major);
    __syncthreads();
    for (unsigned i = tid; i < 64 * 32; i += 256) {
        const unsigned row = row_base + i / 32, col = col_base + i % 32;
        if (row < rows && col < out_dim) dst[size_t(row) * out_dim + col] = c[i];
    }
#endif
}

extern "C" __global__ void antfly_linear_q4_0_bf16_staged(
    float* dst, const float* input, const unsigned char* packed,
    unsigned rows, unsigned in_dim, unsigned out_dim
) {
    antfly_q4_0_bf16_staged<>(dst, input, packed, rows, in_dim, out_dim);
}
