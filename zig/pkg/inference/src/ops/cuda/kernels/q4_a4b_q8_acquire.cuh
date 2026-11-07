// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Existing Q8_1 scale/packed-word acquisition only; row mapping and folds remain native. Native scope; no include guard.
        const unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        const float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        const int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        const int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        const int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
