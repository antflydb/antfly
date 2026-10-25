// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact four-column shared tree and publication, with caller-owned partials. No include guard: native-scope phase.
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
