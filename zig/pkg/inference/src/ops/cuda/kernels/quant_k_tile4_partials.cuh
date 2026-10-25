// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// K-family 256-lane dot; caller selects input and the existing codec. No include guard: native-scope phase.
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            unsigned int input_idx = row * in_dim + block * 256u + tid;
            float x = TERMITE_K_TILE_INPUT(input_idx);
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * TERMITE_K_TILE_BYTES;
                    acc[c] += x * TERMITE_K_TILE_VALUE(bp, tid);
                }
            }
        }
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    }
#include "quant_tile4_publish.cuh"
#undef TERMITE_K_TILE_INPUT
#undef TERMITE_K_TILE_BYTES
#undef TERMITE_K_TILE_VALUE
