// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Four-row column-first and eight-row row-first acquisition stay separate; exact schedule extents. Native scope; no include guard.
#ifdef TERMITE_E4B_FFN_COLUMNS_FIRST
#define TERMITE_E4B_FFN_ROWS 4
#define TERMITE_E4B_FFN_COLS 4
#define TERMITE_E4B_FFN_WAVE_COLS 16u
#else
#define TERMITE_E4B_FFN_ROWS 8
#define TERMITE_E4B_FFN_COLS 2
#define TERMITE_E4B_FFN_WAVE_COLS 8u
#endif
    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            unsigned int local_tid = group_warp * 32u + lane;
            unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[TERMITE_E4B_FFN_ROWS][TERMITE_E4B_FFN_COLS];
            float up_acc[TERMITE_E4B_FFN_ROWS][TERMITE_E4B_FFN_COLS];
            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    gate_acc[r][c] = 0.0f;
                    up_acc[r][c] = 0.0f;
                }
            }

#ifdef TERMITE_E4B_FFN_COLUMNS_FIRST
            unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
                float q8_d[4];
                int q8_low0[4];
                int q8_high0[4];
                int q8_low1[4];
                int q8_high1[4];
                unsigned int q8_base0 = iqs * 4u;
                unsigned int q8_base1 = q8_base0 + 4u;
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    unsigned int row = row_base + r;
                    if (row < rows) {
                        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                        q8_d[r] = termite_half_to_float(q8_d_h);
                        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                        q8_low0[r] = termite_load_i8x4_aligned(q8_values + q8_base0);
                        q8_high0[r] = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                        q8_low1[r] = termite_load_i8x4_aligned(q8_values + q8_base1);
                        q8_high1[r] = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
                    } else {
                        q8_d[r] = 0.0f;
                        q8_low0[r] = 0;
                        q8_high0[r] = 0;
                        q8_low1[r] = 0;
                        q8_high1[r] = 0;
                    }
                }

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    unsigned int col = col_tile + c;
                    const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                    const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                    #pragma unroll
                    for (unsigned int r = 0u; r < rows_per_block; ++r) {
                        if (row_base + r < rows) {
                            gate_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                            up_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                        }
                    }
                }
            }

#else
            unsigned int iqs = (local_tid & 1u) * 2u;
            unsigned int q8_base0 = iqs * 4u;
            unsigned int q8_base1 = q8_base0 + 4u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    unsigned int row = row_base + r;
                    if (row < rows) {
                        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                        float q8_d = termite_half_to_float(q8_d_h);
                        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
                        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
                        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

                        #pragma unroll
                        for (unsigned int c = 0u; c < group_cols; ++c) {
                            unsigned int col = col_tile + c;
                            const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                            const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                            gate_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                            up_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                        }
                    }
                }
            }

#endif
            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    float gate_sum = termite_warp_reduce_sum(gate_acc[r][c]);
                    float up_sum = termite_warp_reduce_sum(up_acc[r][c]);
                    if (lane == 0u) {
                        gate_partial[r][group][c][group_warp] = gate_sum;
                        up_partial[r][group][c][group_warp] = up_sum;
                    }
                }
            }
        }
        __syncthreads();
        if (tid < 64u) {
#ifdef TERMITE_E4B_FFN_COLUMNS_FIRST
            unsigned int out_row = tid >> 4u;
            unsigned int local = tid & 15u;
            if (row_base + out_row < rows) {
                unsigned int out_group = local >> 2u;
                unsigned int c = local & 3u;
#else
            unsigned int out_row = tid >> 3u;
            unsigned int local = tid & 7u;
            if (row_base + out_row < rows) {
                unsigned int out_group = local >> 1u;
                unsigned int c = local & 1u;
#endif
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 5u; ++w) {
                    gate_y += gate_partial[out_row][out_group][c][w];
                    up_y += up_partial[out_row][out_group][c][w];
                }
                activated[out_row][wave * TERMITE_E4B_FFN_WAVE_COLS + out_group * group_cols + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
        __syncthreads();
    }

    if (warp < rows_per_block && row_base + warp < rows) {
        unsigned int row = row_base + warp;
        float x = activated[warp][lane];
        termite_store_q8_1_zero_sum_warp(dst_q8, row, out_row_blocks, q8_out_block, lane, x);
    }

#undef TERMITE_E4B_FFN_COLUMNS_FIRST

#undef TERMITE_E4B_FFN_ROWS
#undef TERMITE_E4B_FFN_COLS
#undef TERMITE_E4B_FFN_WAVE_COLS
