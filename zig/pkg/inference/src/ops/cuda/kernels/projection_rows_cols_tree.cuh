// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope phase; intentionally no include guard. Caller retains storage and schedule.
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                #pragma unroll
                for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] += partial[r][c][tid + stride];
            }
        }
        __syncthreads();
    }
