// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include "wmma_tile_program.cuh"
// Intentionally included inside both native entrypoints; no include guard.
// QueryTile/KeyTile are compile-time schedule constants supplied by the caller.
// Direct kernel shared declarations avoid NVCC device-function address lowering
// without introducing architecture-dependent storage or maximum-sized arrays.
// The two warp/MMA/softmax phases retain their original barriers and folds.
    static_assert((QueryTile == 16u && KeyTile == 32u) || (QueryTile == 32u && KeyTile == 16u), "unsupported DeBERTa TC schedule");

    constexpr unsigned int kThreads = 256u;
    constexpr unsigned int kQueryTile = QueryTile;
    constexpr unsigned int kKeyTile = KeyTile;
    constexpr unsigned int kHeadDim = 64u;
    constexpr unsigned int kRelTile = kQueryTile + kKeyTile - 1u;
    constexpr unsigned int kRelPitch = 48u; // WMMA requires a 16-column tail.
    constexpr float kNegInf = -3.402823466e+38f;
    if (seq_len == 0u || seq_len > 256u || head_dim != kHeadDim || blockDim.x != kThreads) return;

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
    const float scale = rsqrtf((float)(kHeadDim * 3u));

    __shared__ __align__(16) half q_tile[kQueryTile * kHeadDim];
    // K is transposed so it is directly consumable as a row-major WMMA B.
    __shared__ __align__(16) half k_tile[kHeadDim * kKeyTile];
    // The p2c term uses K as a WMMA A operand, so retain its native [N, D]
    // layout as well.  Both layouts are CTA-local and replace the old global
    // head-packing buffer.
    __shared__ __align__(16) half k_rows[kKeyTile * kHeadDim];
    __shared__ __align__(16) half v_tile[kKeyTile * kHeadDim];
    // Relative tensors are [D, R] matrices. R is padded to 48 to retain
    // WMMA's 16-wide fragment contract without special tail code.
    __shared__ __align__(16) half qr_tile[kHeadDim * kRelPitch];
    __shared__ __align__(16) half kr_tile[kHeadDim * kRelPitch];
    __shared__ __align__(16) float scores[kQueryTile * kKeyTile];
    __shared__ __align__(16) float c2p_scores[kQueryTile * kRelPitch];
    __shared__ __align__(16) float p2c_scores[kKeyTile * kRelPitch];
    __shared__ __align__(16) half probabilities[kQueryTile * kKeyTile];
    __shared__ __align__(16) float output[kQueryTile * kHeadDim];
    __shared__ float running_max[kQueryTile];
    __shared__ float running_sum[kQueryTile];
    __shared__ float row_alpha[kQueryTile];

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int row = index / kHeadDim;
        const unsigned int d = index - row * kHeadDim;
        const unsigned int qi = query_start + row;
        q_tile[index] = qi < seq_len ? __float2half_rn(q[(b * seq_len + qi) * hidden + head_off + d]) : __float2half_rn(0.0f);
        output[index] = 0.0f;
    }
    if (tid < kQueryTile) {
        running_max[tid] = kNegInf;
        running_sum[tid] = 0.0f;
    }
    __syncthreads();

    for (unsigned int key_start = 0u; key_start < seq_len; key_start += kKeyTile) {
        // The relative window spans r=(query_start - key_start) +
        // [-(N-1), M-1], exactly the values needed by this score tile.
        const int rel_low = (int)query_start + (int)seq_len - 1 - ((int)key_start + (int)kKeyTile - 1);
        for (unsigned int index = tid; index < kHeadDim * kKeyTile; index += kThreads) {
            const unsigned int d = index / kKeyTile;
            const unsigned int n = index - d * kKeyTile;
            const unsigned int ki = key_start + n;
            const float kval = ki < seq_len ? k[(b * seq_len + ki) * hidden + head_off + d] : 0.0f;
            k_tile[index] = __float2half_rn(kval);
            k_rows[n * kHeadDim + d] = __float2half_rn(kval);
            v_tile[n * kHeadDim + d] = __float2half_rn(ki < seq_len ? v[(b * seq_len + ki) * hidden + head_off + d] : 0.0f);
        }
        for (unsigned int index = tid; index < kHeadDim * kRelPitch; index += kThreads) {
            const unsigned int d = index / kRelPitch;
            const unsigned int r = index - d * kRelPitch;
            const int rel = rel_low + (int)r;
            const float qrv = r < kRelTile && rel >= 0 && rel < (int)(seq_len * 2u - 1u) ? q_r[(unsigned int)rel * hidden + head_off + d] : 0.0f;
            const float krv = r < kRelTile && rel >= 0 && rel < (int)(seq_len * 2u - 1u) ? k_r[(unsigned int)rel * hidden + head_off + d] : 0.0f;
            qr_tile[index] = __float2half_rn(qrv);
            kr_tile[index] = __float2half_rn(krv);
        }
        __syncthreads();

        if constexpr (QueryTile == 16u) {
            // Q*K^T: two 16x16 fragments, one per warp.
            if (warp < 2u) {
                ANTFLY_CUDA_WMMA_DOT16_F32(
                    half, wmma::row_major, kHeadDim,
                    q_tile + d0, kHeadDim,
                    k_tile + d0 * kKeyTile + warp * 16u, kKeyTile,
                    scores + warp * 16u, kKeyTile)
            }

            // Q*Kr^T: three compact relative columns. Warp 2 handles chunks 0/2
            // and warp 3 handles chunk 1, keeping all relative gathers local.
            if (warp == 2u || warp == 3u) {
                const unsigned int first_chunk = warp == 2u ? 0u : 1u;
                for (unsigned int chunk = first_chunk; chunk < 3u; chunk += 2u) {
                    ANTFLY_CUDA_WMMA_DOT16_F32(
                        half, wmma::row_major, kHeadDim,
                        q_tile + d0, kHeadDim,
                        kr_tile + d0 * kRelPitch + chunk * 16u, kRelPitch,
                        c2p_scores + chunk * 16u, kRelPitch)
                }
            }

            // K*Qr^T: six compact relative fragments.  The result is indexed by
            // key row so the p2c diagonal is a simple shared-memory gather.
            if (warp >= 4u) {
                for (unsigned int work = warp - 4u; work < 6u; work += 4u) {
                    const unsigned int key_chunk = work / 3u;
                    const unsigned int rel_chunk = work - key_chunk * 3u;
                    ANTFLY_CUDA_WMMA_DOT16_F32(
                        half, wmma::row_major, kHeadDim,
                        k_rows + (key_chunk * 16u) * kHeadDim + d0, kHeadDim,
                        qr_tile + d0 * kRelPitch + rel_chunk * 16u, kRelPitch,
                        p2c_scores + (key_chunk * 16u) * kRelPitch + rel_chunk * 16u, kRelPitch)
                }
            }
            __syncthreads();

            // Eight warps each normalize two query rows.  Scores are kept F32;
            // only the matrix-A probabilities are narrowed for the P*V MMA.
            #pragma unroll
            for (unsigned int sub = 0u; sub < 2u; ++sub) {
                const unsigned int m = warp * 2u + sub;
                const unsigned int qi = query_start + m;
                const unsigned int ki = key_start + lane;
                float score = kNegInf;
                if (qi < seq_len && ki < seq_len && mask[b * seq_len + ki] != 0ll) {
                    const unsigned int rel = m + kKeyTile - 1u - lane;
                    score = (scores[m * kKeyTile + lane] + c2p_scores[m * kRelPitch + rel] + p2c_scores[lane * kRelPitch + rel]) * scale;
                }
                scores[m * kKeyTile + lane] = score;
                float tile_max = score;
                #pragma unroll
                for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) tile_max = fmaxf(tile_max, __shfl_down_sync(0xffffffffu, tile_max, offset));
                tile_max = __shfl_sync(0xffffffffu, tile_max, 0u);
                const float old_max = running_max[m];
                const float next_max = fmaxf(old_max, tile_max);
                const float alpha = old_max > kNegInf * 0.5f ? expf(old_max - next_max) : 0.0f;
                const float beta = score > kNegInf * 0.5f ? expf(score - next_max) : 0.0f;
                float tile_sum = beta;
                #pragma unroll
                for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) tile_sum += __shfl_down_sync(0xffffffffu, tile_sum, offset);
                tile_sum = __shfl_sync(0xffffffffu, tile_sum, 0u);
                probabilities[m * kKeyTile + lane] = __float2half_rn(beta);
                if (lane == 0u) {
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

            // P*V: four output-D fragments, accumulated into the online F32
            // output state.  This is the only value path; no probability tensor
            // ever reaches device memory.
            if (warp < 4u) {
                wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
                wmma::load_matrix_sync(acc, output + warp * 16u, kHeadDim, wmma::mem_row_major);
                #pragma unroll
                for (unsigned int key_chunk = 0u; key_chunk < 2u; ++key_chunk) {
                    wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
                    wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
                    wmma::load_matrix_sync(p_frag, probabilities + key_chunk * 16u, kKeyTile);
                    wmma::load_matrix_sync(v_frag, v_tile + (key_chunk * 16u) * kHeadDim + warp * 16u, kHeadDim);
                    wmma::mma_sync(acc, p_frag, v_frag, acc);
                }
                wmma::store_matrix_sync(output + warp * 16u, acc, kHeadDim, wmma::mem_row_major);
            }
            __syncthreads();
        } else {
            // Q*K^T has two M fragments.  The remaining warps produce the six
            // M32xR48 content-to-position fragments in parallel.
            if (warp < 2u) {
                ANTFLY_CUDA_WMMA_DOT16_F32(
                    half, wmma::row_major, kHeadDim,
                    q_tile + warp * 16u * kHeadDim + d0, kHeadDim,
                    k_tile + d0 * kKeyTile, kKeyTile,
                    scores + warp * 16u * kKeyTile, kKeyTile)
            }
            if (warp >= 2u) {
                const unsigned int work = warp - 2u;
                const unsigned int query_chunk = work / 3u;
                const unsigned int rel_chunk = work - query_chunk * 3u;
                ANTFLY_CUDA_WMMA_DOT16_F32(
                    half, wmma::row_major, kHeadDim,
                    q_tile + query_chunk * 16u * kHeadDim + d0, kHeadDim,
                    kr_tile + d0 * kRelPitch + rel_chunk * 16u, kRelPitch,
                    c2p_scores + query_chunk * 16u * kRelPitch + rel_chunk * 16u, kRelPitch)
            }
            __syncthreads();

            // K*Qr^T is M16xR48. It reuses warps after the first score phase.
            if (warp < 3u) {
                ANTFLY_CUDA_WMMA_DOT16_F32(
                    half, wmma::row_major, kHeadDim,
                    k_rows + d0, kHeadDim,
                    qr_tile + d0 * kRelPitch + warp * 16u, kRelPitch,
                    p2c_scores + warp * 16u, kRelPitch)
            }
            __syncthreads();

            // Two independent 16-lane reductions let every warp update four
            // query rows while all lanes remain useful on the N=16 score tile.
            #pragma unroll
            for (unsigned int group = 0u; group < 2u; ++group) {
                const unsigned int lane_group = lane >> 4u;
                const unsigned int m = warp * 4u + group * 2u + lane_group;
                const unsigned int qi = query_start + m;
                const unsigned int ki = key_start + (lane & 15u);
                const unsigned int group_mask = lane_group == 0u ? 0x0000ffffu : 0xffff0000u;
                float score = kNegInf;
                if (qi < seq_len && ki < seq_len && mask[b * seq_len + ki] != 0ll) {
                    const unsigned int rel = m + kKeyTile - 1u - (lane & 15u);
                    score = (scores[m * kKeyTile + (lane & 15u)] + c2p_scores[m * kRelPitch + rel] + p2c_scores[(lane & 15u) * kRelPitch + rel]) * scale;
                }
                scores[m * kKeyTile + (lane & 15u)] = score;
                float tile_max = score;
                #pragma unroll
                for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) tile_max = fmaxf(tile_max, __shfl_down_sync(group_mask, tile_max, offset));
                const unsigned int leader = lane_group * 16u;
                tile_max = __shfl_sync(group_mask, tile_max, leader);
                const float old_max = running_max[m];
                const float next_max = fmaxf(old_max, tile_max);
                const float alpha = old_max > kNegInf * 0.5f ? expf(old_max - next_max) : 0.0f;
                const float beta = score > kNegInf * 0.5f ? expf(score - next_max) : 0.0f;
                float tile_sum = beta;
                #pragma unroll
                for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) tile_sum += __shfl_down_sync(group_mask, tile_sum, offset);
                tile_sum = __shfl_sync(group_mask, tile_sum, leader);
                probabilities[m * kKeyTile + (lane & 15u)] = __float2half_rn(beta);
                if ((lane & 15u) == 0u) {
                    running_max[m] = next_max;
                    running_sum[m] = running_sum[m] * alpha + tile_sum;
                    row_alpha[m] = alpha;
                }
            }
            __syncthreads();

            for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) output[index] *= row_alpha[index / kHeadDim];
            __syncthreads();

            // Eight warps cover the two M fragments and four output-D fragments
            // of the M32xD64 P*V matrix in a single tensor-core pass.
            const unsigned int output_query_chunk = warp >> 2u;
            const unsigned int output_d_chunk = warp & 3u;
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::load_matrix_sync(acc, output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, kHeadDim, wmma::mem_row_major);
            wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
            wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
            wmma::load_matrix_sync(p_frag, probabilities + output_query_chunk * 16u * kKeyTile, kKeyTile);
            wmma::load_matrix_sync(v_frag, v_tile + output_d_chunk * 16u, kHeadDim);
            wmma::mma_sync(acc, p_frag, v_frag, acc);
            wmma::store_matrix_sync(output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, acc, kHeadDim, wmma::mem_row_major);
            __syncthreads();
        }
    }

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int m = index / kHeadDim;
        const unsigned int d = index - m * kHeadDim;
        const unsigned int qi = query_start + m;
        if (qi < seq_len) {
            const float denom = running_sum[m];
            dst[(b * seq_len + qi) * hidden + head_off + d] = denom > 0.0f ? output[index] / denom : 0.0f;
        }
    }
