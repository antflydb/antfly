// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact four-column shared tree and publication, with caller-owned partials. No include guard: native-scope phase.
// tile-codegen:begin statistics.quant_columns family (do not edit; run: zig build tile-kernel-codegen -- --write)
    __syncthreads();
    for (unsigned int stride = 256u >> 1; stride > 0u; stride >>= 1) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
            partial[c][tid] += partial[c][tid + stride];
            }
        }
        __syncthreads();
    }
// tile-codegen:end statistics.quant_columns family
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
