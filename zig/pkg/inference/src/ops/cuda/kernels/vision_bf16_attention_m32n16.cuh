// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Shared head-64 WMMA online attention. Native kernel scope; no include guard.
// Callers retain entry ABI and launch bounds. Storage policies preserve each
// qualified F32/BF16 acquisition and publication boundary.
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
    constexpr unsigned int kThreads = 256u;
    constexpr unsigned int kQueryTile = 32u;
    constexpr unsigned int kKeyTile = 16u;
    constexpr unsigned int kHeadDim = 64u;
    constexpr float kNegInf = -3.402823466e+38f;
    if (seq_len == 0u || head_dim != kHeadDim || blockDim.x != kThreads) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int query_tiles = (seq_len + kQueryTile - 1u) / kQueryTile;
    const unsigned int block = blockIdx.x;
    const unsigned int query_tile = block % query_tiles;
    const unsigned int matrix = block / query_tiles;
    const unsigned int head = matrix % num_heads;
    const unsigned int b = matrix / num_heads;
    if (b >= batch) return;

    const unsigned int hidden = num_heads * kHeadDim;
    const unsigned int head_off = head * kHeadDim;
    const unsigned int query_start = query_tile * kQueryTile;
    constexpr float scale = 0.125f;

    __shared__ __align__(16) __nv_bfloat16 q_tile[kQueryTile * kHeadDim];
    // K is transposed to the [D, N] row-major layout expected by WMMA B.
    __shared__ __align__(16) __nv_bfloat16 k_tile[kHeadDim * kKeyTile];
    __shared__ __align__(16) __nv_bfloat16 v_tile[kKeyTile * kHeadDim];
    __shared__ __align__(16) float scores[kQueryTile * kKeyTile];
    __shared__ __align__(16) __nv_bfloat16 probabilities[kQueryTile * kKeyTile];
    __shared__ __align__(16) float output[kQueryTile * kHeadDim];
    __shared__ float running_max[kQueryTile];
    __shared__ float running_sum[kQueryTile];
    __shared__ float row_alpha[kQueryTile];

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int row = index / kHeadDim;
        const unsigned int d = index - row * kHeadDim;
        const unsigned int qi = query_start + row;
#ifdef TERMITE_VISION_BF16_NATIVE_IO
        q_tile[index] = qi < seq_len ? q[(b * seq_len + qi) * hidden + head_off + d] : __float2bfloat16_rn(0.0f);
#else
        const float value = qi < seq_len ? q[(b * seq_len + qi) * hidden + head_off + d] : 0.0f;
        q_tile[index] = __float2bfloat16_rn(value);
#endif
        output[index] = 0.0f;
    }
    if (tid < kQueryTile) {
        running_max[tid] = kNegInf;
        running_sum[tid] = 0.0f;
    }
    __syncthreads();

    for (unsigned int key_start = 0u; key_start < seq_len; key_start += kKeyTile) {
        for (unsigned int index = tid; index < kHeadDim * kKeyTile; index += kThreads) {
            const unsigned int d = index / kKeyTile;
            const unsigned int n = index - d * kKeyTile;
            const unsigned int ki = key_start + n;
#ifdef TERMITE_VISION_BF16_NATIVE_IO
            k_tile[index] = ki < seq_len ? k[(b * seq_len + ki) * hidden + head_off + d] : __float2bfloat16_rn(0.0f);
            v_tile[n * kHeadDim + d] = ki < seq_len ? v[(b * seq_len + ki) * hidden + head_off + d] : __float2bfloat16_rn(0.0f);
#else
            const float kval = ki < seq_len ? k[(b * seq_len + ki) * hidden + head_off + d] : 0.0f;
            k_tile[index] = __float2bfloat16_rn(kval);
            v_tile[n * kHeadDim + d] = __float2bfloat16_rn(
                ki < seq_len ? v[(b * seq_len + ki) * hidden + head_off + d] : 0.0f
            );
#endif
        }
        __syncthreads();

        // Two warps form the two 16-query halves of the 32x16 score tile.
        if (warp < 2u) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::fill_fragment(acc, 0.0f);
            #pragma unroll
            for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, __nv_bfloat16, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, __nv_bfloat16, wmma::row_major> b_frag;
                wmma::load_matrix_sync(a_frag, q_tile + warp * 16u * kHeadDim + d0, kHeadDim);
                wmma::load_matrix_sync(b_frag, k_tile + d0 * kKeyTile, kKeyTile);
                wmma::mma_sync(acc, a_frag, b_frag, acc);
            }
            wmma::store_matrix_sync(scores + warp * 16u * kKeyTile, acc, kKeyTile, wmma::mem_row_major);
        }
        __syncthreads();

        // Each half-warp normalizes one row.  Eight warps cover all 32 rows
        // in two passes while preserving an FP32 online-softmax accumulator.
        #pragma unroll
        for (unsigned int group = 0u; group < 2u; ++group) {
            const unsigned int lane_group = lane >> 4u;
            const unsigned int key_lane = lane & 15u;
            const unsigned int m = warp * 4u + group * 2u + lane_group;
            const unsigned int qi = query_start + m;
            const unsigned int ki = key_start + key_lane;
            const unsigned int group_mask = lane_group == 0u ? 0x0000ffffu : 0xffff0000u;
            float score = kNegInf;
            if (qi < seq_len && ki < seq_len) score = scores[m * kKeyTile + key_lane] * scale;
            float tile_max = score;
            #pragma unroll
            for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) {
                tile_max = fmaxf(tile_max, __shfl_down_sync(group_mask, tile_max, offset));
            }
            const unsigned int leader = lane_group * 16u;
            tile_max = __shfl_sync(group_mask, tile_max, leader);
            const float old_max = running_max[m];
            const float next_max = fmaxf(old_max, tile_max);
            const float alpha = old_max > kNegInf * 0.5f ? expf(old_max - next_max) : 0.0f;
            const float beta = score > kNegInf * 0.5f ? expf(score - next_max) : 0.0f;
            float tile_sum = beta;
            #pragma unroll
            for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) {
                tile_sum += __shfl_down_sync(group_mask, tile_sum, offset);
            }
            tile_sum = __shfl_sync(group_mask, tile_sum, leader);
            probabilities[m * kKeyTile + key_lane] = __float2bfloat16_rn(beta);
            if (key_lane == 0u) {
                running_max[m] = next_max;
                running_sum[m] = running_sum[m] * alpha + tile_sum;
                row_alpha[m] = alpha;
            }
        }
        __syncthreads();

        for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
            output[index] *= row_alpha[index / kHeadDim];
        }
        __syncthreads();

        // Eight warps cover both query fragments and all four output-D
        // fragments, so P*V for the 32x64 output tile completes in one pass.
        const unsigned int output_query_chunk = warp >> 2u;
        const unsigned int output_d_chunk = warp & 3u;
        wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
        wmma::load_matrix_sync(acc, output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, kHeadDim, wmma::mem_row_major);
        wmma::fragment<wmma::matrix_a, 16, 16, 16, __nv_bfloat16, wmma::row_major> p_frag;
        wmma::fragment<wmma::matrix_b, 16, 16, 16, __nv_bfloat16, wmma::row_major> v_frag;
        wmma::load_matrix_sync(p_frag, probabilities + output_query_chunk * 16u * kKeyTile, kKeyTile);
        wmma::load_matrix_sync(v_frag, v_tile + output_d_chunk * 16u, kHeadDim);
        wmma::mma_sync(acc, p_frag, v_frag, acc);
        wmma::store_matrix_sync(output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, acc, kHeadDim, wmma::mem_row_major);
        __syncthreads();
    }

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int m = index / kHeadDim;
        const unsigned int d = index - m * kHeadDim;
        const unsigned int qi = query_start + m;
        if (qi < seq_len) {
            const float denom = running_sum[m];
#ifdef TERMITE_VISION_BF16_NATIVE_IO
            dst[(b * seq_len + qi) * hidden + head_off + d] = __float2bfloat16_rn(denom > 0.0f ? output[index] / denom : 0.0f);
#else
            dst[(b * seq_len + qi) * hidden + head_off + d] = denom > 0.0f ? output[index] / denom : 0.0f;
#endif
        }
    }
#else
    (void)dst;
    (void)q;
    (void)k;
    (void)v;
    (void)batch;
    (void)seq_len;
    (void)num_heads;
    (void)head_dim;
#endif
#undef TERMITE_VISION_BF16_NATIVE_IO
