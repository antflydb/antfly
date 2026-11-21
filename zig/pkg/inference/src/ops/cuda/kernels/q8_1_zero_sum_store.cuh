// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact native Q8_1 quantization/publication; caller owns the flat block expression. No include guard.
#ifndef TERMITE_Q8_OUTPUT_DST
#define TERMITE_Q8_OUTPUT_DST dst_q8
#endif
        float amax = termite_warp_reduce_max_f32(fabsf(x));
        float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char* bp = TERMITE_Q8_OUTPUT_DST + TERMITE_Q8_OUTPUT_BLOCK * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            termite_store_half_bytes(bp, d);
            bp[2u] = 0u;
            bp[3u] = 0u;
        }
#undef TERMITE_Q8_OUTPUT_BLOCK
#undef TERMITE_Q8_OUTPUT_DST
