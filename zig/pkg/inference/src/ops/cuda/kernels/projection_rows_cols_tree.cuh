// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope phase; intentionally no include guard. Caller retains storage and schedule.
// tile-codegen:begin projection.shared_tree family (do not edit; run: zig build tile-kernel-codegen -- --write)
    __syncthreads();
    for (unsigned int stride = 256u >> 1; stride > 0u; stride >>= 1) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
            partial[r][c][tid] += partial[r][c][tid + stride];
            }
            }
        }
        __syncthreads();
    }
// tile-codegen:end projection.shared_tree family
