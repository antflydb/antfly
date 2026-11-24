// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Intentionally included in native kernels; no include guard.
// Caller selects input/weight/output, row, width and column tile. Exact four-column shared tree stays in the kernel.
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                acc[c] += x * termite_q8_0_value(bp, lane);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
// tile-codegen:begin statistics.q8_columns family (do not edit; run: zig build tile-kernel-codegen -- --write)
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
// tile-codegen:end statistics.q8_columns family
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
