// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Generic head-dimension staging schedule, shared by standalone qualification
// and the compatibility runtime ABI. Page-contiguous HD256/512 schedules remain distinct.
kernel void ANTFLY_COMPAT_PREFILL_KERNEL(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant ANTFLY_COMPAT_PREFILL_PARAMS &p [[buffer(6)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
    const uint hd = p.head_dim; const uint q0 = tg.x * 8u; const uint h = tg.y;
    if (q0 >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || p.num_kv_heads == 0u || hd % 32u != 0u) return;
    const float scale = rsqrt(float(hd)); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * hd;
    const uint q_stride = p.num_heads * hd; const uint k_row_halfs = p.key_row_bytes / 2u;
// @attention-phase staging
    threadgroup uint *sphys = (threadgroup uint *)(fb + 1536u);
    threadgroup float *sM = (threadgroup float *)(fb + 1664u);
    threadgroup float *sS = (threadgroup float *)(fb + 1696u);
    threadgroup float *sdiag = (threadgroup float *)(fb + 1760u);
    const device half *v_half = reinterpret_cast<const device half *>(v_bytes);
    const device half *k_half = reinterpret_cast<const device half *>(encoded_key);
// @attention-phase accumulators
    for (uint kc = 0u; kc < p.kv_tokens; kc += 32u) {
        threadgroup_barrier(mem_flags::mem_threadgroup);
        if (tid < 32u) { uint ki = kc + uint(tid); sphys[tid] = ki < p.kv_tokens ? ANTFLY_COMPAT_PAGE_TOKEN(block_table, p, ki) : 0xffffffffu; }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        const bool fast = (p.contiguous_blocks != 0u) && (kc + 32u <= p.kv_tokens);
        if (!fast) { for (uint i = uint(tid); i < 32u * hd; i += 128u) { uint kk = i / hd; uint d = i - kk * hd; uint phys = sphys[kk]; skv[i] = phys != 0xffffffffu ? k_half[phys * k_row_halfs + kv_head_base + d] : half(0.0f); } }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f);
        if (fast) { const device half *kbase = k_half + (p.contiguous_base_token + kc + uint(sgitg) * 8u) * k_row_halfs + kv_head_base; for (uint d = 0u; d < hd; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, hd); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); } }
        else { for (uint d = 0u; d < hd; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, hd); simdgroup_load(mk, skv + uint(sgitg) * 8u * hd + d, hd, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); } }
// @attention-phase score_rows
            bool allowed = ki < p.kv_tokens && qi < p.q_len && sphys[kk] != 0xffffffffu;
// @attention-phase online_weights
        if (!fast) { for (uint i = uint(tid); i < 32u * hd; i += 128u) { uint kk = i / hd; uint d = i - kk * hd; uint phys = sphys[kk]; skv[i] = phys != 0xffffffffu ? v_half[phys * p.v_row_stride + kv_head_base + d] : half(0.0f); } }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        if (fast) { for (uint dt = 0u; dt < d_tiles; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_float8x8 acc = mo[dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp + kk8 * 8u, 32u); simdgroup_load(mv, v_half + (p.contiguous_base_token + kc + kk8 * 8u) * p.v_row_stride + kv_head_base + d8, p.v_row_stride); simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[dt] = acc; } }
        else { for (uint dt = 0u; dt < d_tiles; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_float8x8 acc = mo[dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp + kk8 * 8u, 32u); simdgroup_load(mv, skv + kk8 * 8u * hd + d8, hd); simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[dt] = acc; } }
    }
// @attention-phase publish
}
