// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope phase: the three tiled Q4_K epilogues retain the exact
// 256-lane partial array, reduction barriers and publication scope. No guard.
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
// tile-codegen:begin statistics.quant_scalar family (do not edit; run: zig build tile-kernel-codegen -- --write)
    __syncthreads();
    for (unsigned int stride = 256u >> 1; stride > 0u; stride >>= 1) {
        if (tid < stride) {
            partial[tid] += partial[tid + stride];
        }
        __syncthreads();
    }
// tile-codegen:end statistics.quant_scalar family
