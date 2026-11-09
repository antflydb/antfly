// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope family owner, deliberately included by both exported kernels.
// Query tile size is static. Admission, shared-memory extents, warp ownership,
// score masks and output geometry preserve each qualified native schedule.
#ifndef TERMITE_TQ_MMA_PREFILL_ROWS
#error "prefill query tile policy is required"
#endif

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
    (void)decode_scalars;

    const float neg_inf = -3.402823466e+38f;
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
#define TERMITE_TQ_MMA_TILE_M TERMITE_TQ_PREFILL_TILE_M
#define TERMITE_TQ_MMA_SMEM mma_prefill_smem
#define TERMITE_TQ_MMA_ALPHA_ROWS TERMITE_TQ_PREFILL_TILE_M
#define TERMITE_TQ_MMA_WARP_ROWS 2u
#define TERMITE_TQ_MMA_MAX_DIM 512u
    const unsigned int tile_n = head_dim <= 256u ? 64u : 32u;
#elif TERMITE_TQ_MMA_PREFILL_ROWS == 32u
#define TERMITE_TQ_MMA_TILE_M tile_m
#define TERMITE_TQ_MMA_SMEM mma_m32_prefill_smem
#define TERMITE_TQ_MMA_ALPHA_ROWS 32
#define TERMITE_TQ_MMA_WARP_ROWS 4u
#define TERMITE_TQ_MMA_MAX_DIM 256u
    const unsigned int tile_m = 32u;
    const unsigned int tile_n = 64u;
#else
#error "qualified query tile policy is required"
#endif
    extern __shared__ __align__(32) unsigned char TERMITE_TQ_MMA_SMEM[];
    __shared__ unsigned int tile_phys[64];
    __shared__ float alpha_sh[TERMITE_TQ_MMA_ALPHA_ROWS];

    if (batch != 1u ||
        q_seq_len <= 1u ||
        mask_len != 0u ||
        bias_mode != 0u ||
        (format != 0u && format != 2u) ||
        base_key_row_bytes != key_row_bytes ||
        head_dim > TERMITE_TQ_MMA_MAX_DIM ||
        (head_dim & 31u) != 0u ||
        blockDim.x != 256u ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        key_row_bytes == 0u ||
        value_row_bytes == 0u) return;

    unsigned int head = blockIdx.x;
    unsigned int tile_row_start = blockIdx.y * TERMITE_TQ_MMA_TILE_M;
    if (head >= num_heads || tile_row_start >= q_seq_len) return;

    unsigned int tid = threadIdx.x;
    unsigned int warp = tid >> 5;
    unsigned int lane = tid & 31u;

    const unsigned int kv_pitch = head_dim + 8u;
    const unsigned int sp_pitch = 72u;
    half* q_tile = reinterpret_cast<half*>(TERMITE_TQ_MMA_SMEM);
    half* kv_tile = q_tile + TERMITE_TQ_MMA_TILE_M * head_dim;
    float* s_tile = reinterpret_cast<float*>(kv_tile + tile_n * kv_pitch);
    half* p_tile = reinterpret_cast<half*>(s_tile + TERMITE_TQ_MMA_TILE_M * sp_pitch);
    float* o_tile = reinterpret_cast<float*>(p_tile + TERMITE_TQ_MMA_TILE_M * sp_pitch);

#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
    // Each warp owns two adjacent query rows for softmax state and output.
    unsigned int qi0 = tile_row_start + warp * 2u;
    unsigned int qi1 = qi0 + 1u;
    bool row_valid0 = qi0 < q_seq_len;
    bool row_valid1 = qi1 < q_seq_len;
    unsigned int qi0_clamped = row_valid0 ? qi0 : (q_seq_len - 1u);
    unsigned int qi1_clamped = row_valid1 ? qi1 : (q_seq_len - 1u);

    unsigned int key_start0 = 0u, key_end0 = 0u;
    unsigned int key_start1 = 0u, key_end1 = 0u;
    {
        unsigned int query_pos = query_position_offset + qi0_clamped;
        if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
            unsigned int visible = query_pos - kv_position_offset + 1u;
            key_end0 = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u) {
                unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    key_start0 = window_start_abs - kv_position_offset;
                    if (key_start0 > key_end0) key_start0 = key_end0;
                }
            }
        }
        query_pos = query_position_offset + qi1_clamped;
        if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
            unsigned int visible = query_pos - kv_position_offset + 1u;
            key_end1 = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u) {
                unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    key_start1 = window_start_abs - kv_position_offset;
                    if (key_start1 > key_end1) key_start1 = key_end1;
                }
            }
        }
    }
    if (!row_valid0) { key_start0 = 0u; key_end0 = 0u; }
    if (!row_valid1) { key_start1 = 0u; key_end1 = 0u; }

#else
    // Each warp retains two (M16) or four (M32) adjacent query rows.
    unsigned int qi_base = tile_row_start + warp * TERMITE_TQ_MMA_WARP_ROWS;
    bool row_valid[4];
    unsigned int key_start_r[4];
    unsigned int key_end_r[4];
    float m_run[4];
    float d_run[4];
    for (unsigned int r = 0u; r < TERMITE_TQ_MMA_WARP_ROWS; ++r) {
        unsigned int qi = qi_base + r;
        row_valid[r] = qi < q_seq_len;
        unsigned int qi_clamped = row_valid[r] ? qi : (q_seq_len - 1u);
        unsigned int key_start = 0u, key_end = 0u;
        unsigned int query_pos = query_position_offset + qi_clamped;
        if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
            unsigned int visible = query_pos - kv_position_offset + 1u;
            key_end = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u) {
                unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    key_start = window_start_abs - kv_position_offset;
                    if (key_start > key_end) key_start = key_end;
                }
            }
        }
        if (!row_valid[r]) { key_start = 0u; key_end = 0u; }
        key_start_r[r] = key_start;
        key_end_r[r] = key_end;
        m_run[r] = neg_inf;
        d_run[r] = 0.0f;
    }

#endif

    // Block-wide key range: causal end grows with qi and the sliding-window
    // start grows with qi, so the union is [start(first row), end(last row)].
    unsigned int qi_last = tile_row_start + TERMITE_TQ_MMA_TILE_M - 1u;
#include "../kernels/tq_mma_visible_range.cuh"

    for (unsigned int idx = tid; idx < TERMITE_TQ_MMA_TILE_M * head_dim; idx += 256u) {
        unsigned int row = idx / head_dim;
        unsigned int col = idx - row * head_dim;
        unsigned int qi = tile_row_start + row;
        unsigned int qi_clamped = qi < q_seq_len ? qi : (q_seq_len - 1u);
        q_tile[idx] = __float2half(q[qi_clamped * q_hidden + head * head_dim + col]);
        o_tile[idx] = 0.0f;
    }
    __syncthreads();

#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
    float m_run0 = neg_inf, d_run0 = 0.0f;
    float m_run1 = neg_inf, d_run1 = 0.0f;
#define TERMITE_TQ_ROW_MAX *m_run
#define TERMITE_TQ_ROW_SUM *d_run
#define TERMITE_TQ_ROW_START key_start_r
#define TERMITE_TQ_ROW_END key_end_r
#else
#define TERMITE_TQ_ROW_MAX m_run[sub]
#define TERMITE_TQ_ROW_SUM d_run[sub]
#define TERMITE_TQ_ROW_START key_start_r[sub]
#define TERMITE_TQ_ROW_END key_end_r[sub]
#endif

    for (unsigned int n0 = block_key_start; n0 < block_key_end; n0 += tile_n) {
        unsigned int n_count = block_key_end - n0;
        if (n_count > tile_n) n_count = tile_n;

#include "../kernels/tq_mma_stage_kv.cuh"

        // Preserve the separate M16 and M32 QK warp assignments.
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
        if (warp < (tile_n >> 4)) {
#else
        {
            unsigned int m_chunk = warp >> 2;
            unsigned int n_chunk = warp & 3u;
#endif
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> s_acc;
            wmma::fill_fragment(s_acc, 0.0f);
            for (unsigned int kb = 0u; kb < head_dim; kb += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major> b_frag;
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
                wmma::load_matrix_sync(a_frag, q_tile + kb, head_dim);
#else
                wmma::load_matrix_sync(a_frag, q_tile + (m_chunk * 16u) * head_dim + kb, head_dim);
#endif
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
                wmma::load_matrix_sync(b_frag, kv_tile + (warp * 16u) * kv_pitch + kb, kv_pitch);
#else
                wmma::load_matrix_sync(b_frag, kv_tile + (n_chunk * 16u) * kv_pitch + kb, kv_pitch);
#endif
                wmma::mma_sync(s_acc, a_frag, b_frag, s_acc);
            }
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
            wmma::store_matrix_sync(s_tile + warp * 16u, s_acc, sp_pitch, wmma::mem_row_major);
#else
            wmma::store_matrix_sync(s_tile + (m_chunk * 16u) * sp_pitch + n_chunk * 16u, s_acc, sp_pitch, wmma::mem_row_major);
#endif
        }
        __syncthreads();

        // Online softmax per query row; lanes cover keys {lane, lane+32}.
        for (unsigned int sub = 0u; sub < TERMITE_TQ_MMA_WARP_ROWS; ++sub) {
            unsigned int row = warp * TERMITE_TQ_MMA_WARP_ROWS + sub;
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
            unsigned int key_start_r = sub == 0u ? key_start0 : key_start1;
            unsigned int key_end_r = sub == 0u ? key_end0 : key_end1;
            float* m_run = sub == 0u ? &m_run0 : &m_run1;
            float* d_run = sub == 0u ? &d_run0 : &d_run1;
#endif
            unsigned int ja = lane;
            unsigned int jb = lane + 32u;
            unsigned int kia = n0 + ja;
            unsigned int kib = n0 + jb;
            bool valid_a = ja < n_count && tile_phys[ja] != 0xffffffffu && kia >= TERMITE_TQ_ROW_START && kia < TERMITE_TQ_ROW_END;
            bool valid_b = jb < n_count && tile_phys[jb] != 0xffffffffu && kib >= TERMITE_TQ_ROW_START && kib < TERMITE_TQ_ROW_END;
            float score_a = valid_a ? s_tile[row * sp_pitch + ja] * scale : neg_inf;
            float score_b = valid_b ? s_tile[row * sp_pitch + jb] * scale : neg_inf;

            float tile_max = fmaxf(score_a, score_b);
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
                tile_max = fmaxf(tile_max, __shfl_down_sync(0xffffffffu, tile_max, offset));
            }
            tile_max = __shfl_sync(0xffffffffu, tile_max, 0u);

            float alpha = 1.0f;
            float p_a = 0.0f, p_b = 0.0f;
            if (tile_max > neg_inf) {
                float new_max = fmaxf(TERMITE_TQ_ROW_MAX, tile_max);
                alpha = TERMITE_TQ_ROW_MAX > neg_inf ? expf(TERMITE_TQ_ROW_MAX - new_max) : 0.0f;
                p_a = valid_a ? expf(score_a - new_max) : 0.0f;
                p_b = valid_b ? expf(score_b - new_max) : 0.0f;
                TERMITE_TQ_ROW_MAX = new_max;
            }
            float sum_p = p_a + p_b;
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
                sum_p += __shfl_down_sync(0xffffffffu, sum_p, offset);
            }
            sum_p = __shfl_sync(0xffffffffu, sum_p, 0u);
            if (tile_max > neg_inf) TERMITE_TQ_ROW_SUM = TERMITE_TQ_ROW_SUM * alpha + sum_p;

            p_tile[row * sp_pitch + ja] = __float2half(p_a);
            p_tile[row * sp_pitch + jb] = __float2half(p_b);
            if (lane == 0u) alpha_sh[row] = alpha;
        }
        __syncthreads();

        for (unsigned int idx = tid; idx < TERMITE_TQ_MMA_TILE_M * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            o_tile[idx] *= alpha_sh[row];
        }
#define TERMITE_TQ_MMA_STAGE_VALUES
#include "../kernels/tq_mma_stage_kv.cuh"

        // O += P * V: 2*(head_dim/16) output tiles round-robin over 8 warps.
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
        for (unsigned int nc = warp; nc < (head_dim >> 4); nc += 8u) {
#else
        for (unsigned int wt = warp; wt < 2u * (head_dim >> 4); wt += 8u) {
            unsigned int m_chunk = wt / (head_dim >> 4);
            unsigned int n_chunk = wt - m_chunk * (head_dim >> 4);
#endif
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> o_acc;
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
            wmma::load_matrix_sync(o_acc, o_tile + nc * 16u, head_dim, wmma::mem_row_major);
#else
            wmma::load_matrix_sync(o_acc, o_tile + (m_chunk * 16u) * head_dim + n_chunk * 16u, head_dim, wmma::mem_row_major);
#endif
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
            for (unsigned int kb = 0u; kb < (tile_n >> 4); ++kb) {
#else
            for (unsigned int kb = 0u; kb < 4u; ++kb) {
#endif
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
                wmma::load_matrix_sync(p_frag, p_tile + kb * 16u, sp_pitch);
#else
                wmma::load_matrix_sync(p_frag, p_tile + (m_chunk * 16u) * sp_pitch + kb * 16u, sp_pitch);
#endif
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
                wmma::load_matrix_sync(v_frag, kv_tile + (kb * 16u) * kv_pitch + nc * 16u, kv_pitch);
#else
                wmma::load_matrix_sync(v_frag, kv_tile + (kb * 16u) * kv_pitch + n_chunk * 16u, kv_pitch);
#endif
                wmma::mma_sync(o_acc, p_frag, v_frag, o_acc);
            }
#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
            wmma::store_matrix_sync(o_tile + nc * 16u, o_acc, head_dim, wmma::mem_row_major);
#else
            wmma::store_matrix_sync(o_tile + (m_chunk * 16u) * head_dim + n_chunk * 16u, o_acc, head_dim, wmma::mem_row_major);
#endif
        }
        __syncthreads();
    }

#if TERMITE_TQ_MMA_PREFILL_ROWS == 16u
    if (row_valid0) {
        unsigned int out_base = qi0 * q_hidden + head * head_dim;
        float inv_denom = d_run0 > 0.0f ? 1.0f / d_run0 : 0.0f;
        for (unsigned int col = lane; col < head_dim; col += 32u) {
            dst[out_base + col] = o_tile[(warp * 2u) * head_dim + col] * inv_denom;
        }
    }
    if (row_valid1) {
        unsigned int out_base = qi1 * q_hidden + head * head_dim;
        float inv_denom = d_run1 > 0.0f ? 1.0f / d_run1 : 0.0f;
        for (unsigned int col = lane; col < head_dim; col += 32u) {
            dst[out_base + col] = o_tile[(warp * 2u + 1u) * head_dim + col] * inv_denom;
        }
    }
#else
    for (unsigned int r = 0u; r < TERMITE_TQ_MMA_WARP_ROWS; ++r) {
        if (!row_valid[r]) continue;
        unsigned int out_base = (qi_base + r) * q_hidden + head * head_dim;
        float inv_denom = d_run[r] > 0.0f ? 1.0f / d_run[r] : 0.0f;
        for (unsigned int col = lane; col < head_dim; col += 32u) {
            dst[out_base + col] = o_tile[(warp * TERMITE_TQ_MMA_WARP_ROWS + r) * head_dim + col] * inv_denom;
        }
    }
#endif
#undef TERMITE_TQ_ROW_MAX
#undef TERMITE_TQ_ROW_SUM
#undef TERMITE_TQ_ROW_START
#undef TERMITE_TQ_ROW_END

#undef TERMITE_TQ_MMA_PREFILL_ROWS

#undef TERMITE_TQ_MMA_TILE_M
#undef TERMITE_TQ_MMA_SMEM
#undef TERMITE_TQ_MMA_ALPHA_ROWS
#undef TERMITE_TQ_MMA_WARP_ROWS
#undef TERMITE_TQ_MMA_MAX_DIM
