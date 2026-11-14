// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Intentionally included in native kernels; no include guard.
// Caller provides row_blocks=10 and col_tile. Own only the five-warp dot/partials; publication remains separate.
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[8][5];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    if (tid < 160u) {
        unsigned int block = tid >> 4u;
        unsigned int sub = tid & 15u;
        unsigned int q8_sub_block = sub >> 1u;
        unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = input_q8_1 + (block * 8u + q8_sub_block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        int q8_pack0 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 0u);
        int q8_pack1 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 4u);
        int q8_pack2 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 8u);
        int q8_pack3 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 12u);
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;
            int sumi = 0;
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 0u), q8_pack0, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 4u), q8_pack1, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 8u), q8_pack2, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 12u), q8_pack3, sumi);
            acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
