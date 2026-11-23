// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope phase; intentionally no include guard. Caller retains storage and schedule.
    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) acc[r][c] = 0.0f;
    }
