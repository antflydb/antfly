// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Original scalar scan and value/index tree; exact NaN/tie/sentinel policy. No include guard: native-scope phase.
    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0u;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
#ifdef TERMITE_ARGMAX_SUPPRESS
        bool suppressed = false;
        for (unsigned int j = 0; j < suppress_count; ++j) {
            int token_id = suppress_token_ids[j];
            if (token_id >= 0 && (unsigned int)token_id == i) {
                suppressed = true;
                break;
            }
        }
        if (suppressed) continue;
#endif
        float value = row_ptr[i];
        if (value > best_value || (value == best_value && i < best_index)) {
            best_value = value;
            best_index = i;
        }
    }
    best_values[tid] = best_value;
    best_indices[tid] = best_index;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float other_value = best_values[tid + stride];
            unsigned int other_index = best_indices[tid + stride];
            if (other_value > best_values[tid] || (other_value == best_values[tid] && other_index < best_indices[tid])) {
                best_values[tid] = other_value;
                best_indices[tid] = other_index;
            }
        }
        __syncthreads();
    }

#undef TERMITE_ARGMAX_SUPPRESS
