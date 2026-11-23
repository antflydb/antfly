// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native compile-time Q4_0/Q8_1 FFN schedules. Keep packed-sum precision,
// reduction order and output rounding distinct through the Ggml policy.
// Preserve each caller's address width and Decode input-sum precision.
#ifndef ANTFLY_Q8_1_NATIVE_INPUT
#define ANTFLY_Q8_1_NATIVE_INPUT(Address) \
    const unsigned char *q8_bp = Address; \
    const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]); \
    const float q8_sum = Decode::input_sum(q8_bp); \
    const signed char *q8_values = (const signed char *)(q8_bp + 4u); \
    const unsigned int q8_base0 = iqs * 4u; \
    const unsigned int q8_base1 = q8_base0 + 4u; \
    const int q8_low0 = *(const int *)(q8_values + q8_base0); \
    const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u); \
    const int q8_low1 = *(const int *)(q8_values + q8_base1); \
    const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u)
#endif

#define ANTFLY_Q4_Q8_PAIR_PARAMS unsigned char* dst_q8, const unsigned char* q8_input, const unsigned char* weight_gate, const unsigned char* weight_up, unsigned int activation, unsigned int rows, unsigned int in_dim, unsigned int out_dim
#define ANTFLY_Q4_Q8_PAIR_ARGS dst_q8, q8_input, weight_gate, weight_up, activation, rows, in_dim, out_dim
#define ANTFLY_Q4_Q8_DOWN_PARAMS float* dst, const unsigned char* q8_input, const unsigned char* weight, unsigned int rows, unsigned int in_dim, unsigned int out_dim
#define ANTFLY_Q4_Q8_DOWN_ARGS dst, q8_input, weight, rows, in_dim, out_dim
template<unsigned Warps, typename Decode>
__device__ __forceinline__ void antfly_q4_q8_pair(ANTFLY_Q4_Q8_PAIR_PARAMS) {
    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int out_row_blocks = out_dim >> 5;
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;

    const unsigned int out_block = blockIdx.x % out_row_blocks;
    const unsigned int row = blockIdx.x / out_row_blocks;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / Warps;
    const unsigned int group_warp = warp - group * Warps;
    if (blockDim.x != Warps * 128u || row >= rows) return;

    __shared__ float gate_partial[4][4][Warps];
    __shared__ float up_partial[4][4][Warps];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            const unsigned int local_tid = group_warp * 32u + lane;
            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            const unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += Warps * 16u) {
                ANTFLY_Q8_1_NATIVE_INPUT(q8_input + (row * row_blocks + block) * 36u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    const unsigned int col = col_tile + c;
                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                    gate_acc[c] += Decode::dot(gate_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += Decode::dot(up_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            const unsigned int out_group = tid >> 2u;
            const unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < Warps; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float sum = Decode::output_sum(x);
        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            const unsigned short d_bits = __half_as_ushort(__float2half(d));
            bp[0] = (unsigned char)(d_bits & 0xffu);
            bp[1] = (unsigned char)(d_bits >> 8);
            Decode::store_sum(bp, sum);
        }
    }
}
template<unsigned Threads, unsigned Cols, typename Decode>
__device__ __forceinline__ void antfly_q4_q8_down(ANTFLY_Q4_Q8_DOWN_PARAMS) {
    const unsigned int cols = Cols;
    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / col_tiles;
    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    if (blockDim.x != Threads || row >= rows) return;

    __shared__ float warp_partial[Cols][Threads / 32u];
    float acc[Cols];
    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += Threads / 2u) {
        ANTFLY_Q8_1_NATIVE_INPUT(q8_input + ((size_t)row * row_blocks + block) * 36u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += Decode::dot(bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < Cols) {
        float y = 0.0f;
        #pragma unroll
        for (unsigned int w = 0u; w < Threads / 32u; ++w) y += warp_partial[tid][w];
        const unsigned int col = col_tile + tid;
        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    }
}
