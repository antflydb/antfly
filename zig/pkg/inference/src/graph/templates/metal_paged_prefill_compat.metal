// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Generic head-dimension staging schedule, shared by standalone qualification
// and the compatibility runtime ABI. Page-contiguous HD256/512 schedules remain distinct.
kernel void ANTFLY_COMPAT_PREFILL_KERNEL(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant ANTFLY_COMPAT_PREFILL_PARAMS &p [[buffer(6)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
    const uint hd = p.head_dim; const uint q0 = tg.x * 8u; const uint h = tg.y;
    if (q0 >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || p.num_kv_heads == 0u || hd % 32u != 0u) return;
    const float scale = rsqrt(float(hd)); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * hd;
    const uint q_stride = p.num_heads * hd; const uint k_row_halfs = p.key_row_bytes / 2u;
    threadgroup half *sq = (threadgroup half *)shmem;
    threadgroup half *skv = sq + 8u * hd;
    threadgroup char *fb = shmem + (8u + 32u) * hd * 2u;
    threadgroup float *ss = (threadgroup float *)fb;
    threadgroup half *sp = (threadgroup half *)(fb + 1024u);
    threadgroup uint *sphys = (threadgroup uint *)(fb + 1536u);
    threadgroup float *sM = (threadgroup float *)(fb + 1664u);
    threadgroup float *sS = (threadgroup float *)(fb + 1696u);
    threadgroup float *sdiag = (threadgroup float *)(fb + 1760u);
    const device half *v_half = reinterpret_cast<const device half *>(v_bytes);
    const device half *k_half = reinterpret_cast<const device half *>(encoded_key);
    for (uint i = uint(tid); i < 8u * hd; i += 128u) { uint j = i / hd; uint d = i - j * hd; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + h * hd + d] * scale) : half(0.0f); }
    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; }
    if (tid < 64u) sdiag[tid] = 0.0f;
    const uint dslice = uint(sgitg) * (hd / 4u);
    simdgroup_float8x8 mo[8];
    for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
    const uint d_tiles = hd / 32u;
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
        simdgroup_store(ms, ss + uint(sgitg) * 8u, 32u, 0, false);
        threadgroup_barrier(mem_flags::mem_threadgroup);
        for (uint jj = 0u; jj < 2u; ++jj) {
            uint j = uint(sgitg) + jj * 4u; uint qi = q0 + j; uint query_pos = p.query_position_offset + qi; uint kk = uint(lane); uint ki = kc + kk;
            bool allowed = ki < p.kv_tokens && qi < p.q_len && sphys[kk] != 0xffffffffu;
            if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; }
            float sc = allowed ? ss[j * 32u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f;
            float row_max = simd_max(sc); float m_old = sM[j]; float m_new = max(m_old, row_max);
            float e = 0.0f; float corr = 1.0f;
            if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; e = sc > -3.0e+38f ? exp(sc - m_new) : 0.0f; }
            sp[j * 32u + kk] = half(e);
            float row_sum = simd_sum(e);
            if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; }
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u);
        for (uint i = 0u; i < d_tiles; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        if (!fast) { for (uint i = uint(tid); i < 32u * hd; i += 128u) { uint kk = i / hd; uint d = i - kk * hd; uint phys = sphys[kk]; skv[i] = phys != 0xffffffffu ? v_half[phys * p.v_row_stride + kv_head_base + d] : half(0.0f); } }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        if (fast) { for (uint dt = 0u; dt < d_tiles; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_float8x8 acc = mo[dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp + kk8 * 8u, 32u); simdgroup_load(mv, v_half + (p.contiguous_base_token + kc + kk8 * 8u) * p.v_row_stride + kv_head_base + d8, p.v_row_stride); simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[dt] = acc; } }
        else { for (uint dt = 0u; dt < d_tiles; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_float8x8 acc = mo[dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp + kk8 * 8u, 32u); simdgroup_load(mv, skv + kk8 * 8u * hd + d8, hd); simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[dt] = acc; } }
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (tid < 8u) { float denom = sS[tid]; sdiag[uint(tid) * 8u + uint(tid)] = denom > 0.0f ? 1.0f / denom : 0.0f; }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u);
    threadgroup float *so = (threadgroup float *)skv;
    for (uint dt = 0u; dt < d_tiles; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); simdgroup_store(scaled, so + uint(sgitg) * (8u * (hd / 4u)) + dt * 8u, hd / 4u, 0, false); }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint i = uint(tid); i < 8u * hd; i += 128u) { uint j = i / hd; uint d = i - j * hd; uint qi = q0 + j; if (qi >= p.q_len) continue; uint sg_of_d = d / (hd / 4u); uint d_in = d - sg_of_d * (hd / 4u); output[qi * q_stride + h * hd + d] = so[sg_of_d * (8u * (hd / 4u)) + j * (hd / 4u) + d_in]; }
}
