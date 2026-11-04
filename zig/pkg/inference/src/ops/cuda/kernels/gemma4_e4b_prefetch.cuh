// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact, opt-in GB10 L2-prefetch variants; arithmetic and reduction order
// are copied unchanged from the corresponding canonical E4B kernels.
#pragma once


__device__ __forceinline__ void antfly_gemma4_e4b_prefetch_l2(const void* address) {
    asm volatile("prefetch.global.L2 [%0];" :: "l"(address));
}

extern "C" __global__ void antfly_gemma4_e4b_head_prefetch_l2(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* input_q8_1,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    (void)rows;
    (void)in_dim;
    (void)out_dim;
    (void)suppress_count;
    const unsigned int row_blocks = 10u;
    const unsigned int cols = 8u;
    unsigned int global_tile = blockIdx.x;
    unsigned int col_tile = global_tile * cols;
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
            if (c + 1u < cols && sub == 0u) {
                const unsigned char* future = weight + ((col_tile + c + 1u) * row_blocks + block) * 210u;
                antfly_gemma4_e4b_prefetch_l2(future);
                antfly_gemma4_e4b_prefetch_l2(future + 128u);
            }
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
    __syncthreads();
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            float value =
                warp_partial[c][0] +
                warp_partial[c][1] +
                warp_partial[c][2] +
                warp_partial[c][3] +
                warp_partial[c][4];
            unsigned int col = col_tile + c;
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}

extern "C" __global__ void antfly_gemma4_e4b_pair_prefetch_l2(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 80u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float gate_partial[4][8];
    __shared__ float up_partial[4][8];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            if (c + 1u < cols && (tid & 1u) == 0u) {
                antfly_gemma4_e4b_prefetch_l2(weight_gate + ((col_tile + c + 1u) * row_blocks + block) * 18u);
                antfly_gemma4_e4b_prefetch_l2(weight_up + ((col_tile + c + 1u) * row_blocks + block) * 18u);
            }
            unsigned int col = col_tile + c;
            const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
            const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
            gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 8u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 8u; ++w) {
                gate_y += gate_partial[c][w];
                up_y += up_partial[c][w];
            }
            dst[col_tile + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
        }
    }
}

extern "C" __global__ void antfly_gemma4_e4b_down_prefetch_l2(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 320u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[4][10];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            if (c + 1u < cols && (tid & 1u) == 0u) {
                antfly_gemma4_e4b_prefetch_l2(weight + ((col_tile + c + 1u) * row_blocks + block) * 18u);
            }
            unsigned int col = col_tile + c;
            const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
            acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 10u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 10u; ++w) y += warp_partial[c][w];
            dst[col_tile + c] = y;
        }
    }
}
