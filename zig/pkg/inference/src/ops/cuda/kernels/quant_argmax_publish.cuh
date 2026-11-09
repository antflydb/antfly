// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Intentionally included in native kernels; no include guard.
// Caller owns partial storage, codec accumulators and native argmax slots.
// Static policies retain column geometry, row validity, suppression and ties.
#ifndef TERMITE_QUANT_ARGMAX_COLUMNS
#define TERMITE_QUANT_ARGMAX_COLUMNS 4u
#endif
#ifndef TERMITE_QUANT_ARGMAX_INVALID_COLUMN
#define TERMITE_QUANT_ARGMAX_INVALID_COLUMN col >= out_dim
#endif
#ifndef TERMITE_QUANT_ARGMAX_SLOT
#define TERMITE_QUANT_ARGMAX_SLOT tile
#endif
    #pragma unroll
    for (unsigned int c = 0; c < TERMITE_QUANT_ARGMAX_COLUMNS; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < TERMITE_QUANT_ARGMAX_COLUMNS; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < TERMITE_QUANT_ARGMAX_COLUMNS; ++c) {
            unsigned int col = col_tile + c;
            if (TERMITE_QUANT_ARGMAX_INVALID_COLUMN) continue;
            bool suppressed = false;
            for (unsigned int j = 0; j < suppress_count; ++j) {
                int token_id = suppress_token_ids[j];
                if (token_id >= 0 && (unsigned int)token_id == col) {
                    suppressed = true;
                    break;
                }
            }
            if (suppressed) continue;
            float value = partial[c][0];
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[TERMITE_QUANT_ARGMAX_SLOT] = best_value;
        partial_indices[TERMITE_QUANT_ARGMAX_SLOT] = best_index;
    }

#undef TERMITE_QUANT_ARGMAX_COLUMNS
#undef TERMITE_QUANT_ARGMAX_INVALID_COLUMN
#undef TERMITE_QUANT_ARGMAX_SLOT
