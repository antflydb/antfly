// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_DENSE_CAUSAL_ATTENTION(termite_attention_f32_dense_causal_sg) "\n"
    \\           "    const uint hd = p.head_dim; const uint q0 = tg.x * 8u; const uint h = tg.y;\n"
    \\           "    if (q0 >= p.q_len || h >= p.num_heads || p.num_kv_heads == 0u || hd % 32u != 0u || p.has_bias != 0u || p.has_mask != 0u) return;\n"
    \\           "    const float scale = rsqrt(float(hd)); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * hd;\n"
    \\           "    const uint q_stride = p.num_heads * hd; const uint kv_stride = p.num_kv_heads * hd;\n"
    \\           TERMITE_MSL_ATTENTION_STAGING
    \\           "    threadgroup float *sM = (threadgroup float *)(fb + 1536u);\n"
    \\           "    threadgroup float *sS = (threadgroup float *)(fb + 1568u);\n"
    \\           "    threadgroup float *sdiag = (threadgroup float *)(fb + 1600u);\n"
    \\           TERMITE_MSL_ATTENTION_ACCUMULATORS
    \\           "    const uint last_query_pos = p.query_position_offset + min(q0 + 7u, p.q_len - 1u);\n"
    \\           "    for (uint kc = 0u; kc < p.kv_len; kc += 32u) {\n"
    \\           "        if (p.kv_position_offset + kc > last_query_pos) break;\n"
    \\           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "        for (uint i = uint(tid); i < 32u * hd; i += 128u) { uint kk = i / hd; uint d = i - kk * hd; uint ki = kc + kk; skv[i] = ki < p.kv_len ? half(k[ki * kv_stride + kv_head_base + d]) : half(0.0f); }\n"
    \\           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "        simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
    \\           "        for (uint d = 0u; d < hd; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, hd); simdgroup_load(mk, skv + uint(sgitg) * 8u * hd + d, hd, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); }\n"
    \\           TERMITE_MSL_ATTENTION_SCORE_ROWS
    \\           "            bool allowed = ki < p.kv_len && qi < p.q_len;\n"
    \\           TERMITE_MSL_ATTENTION_ONLINE_WEIGHTS
    \\           "        for (uint i = uint(tid); i < 32u * hd; i += 128u) { uint kk = i / hd; uint d = i - kk * hd; uint ki = kc + kk; skv[i] = ki < p.kv_len ? half(v[ki * kv_stride + kv_head_base + d]) : half(0.0f); }\n"
    \\           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "        for (uint dt = 0u; dt < d_tiles; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_float8x8 acc = mo[dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp + kk8 * 8u, 32u); simdgroup_load(mv, skv + kk8 * 8u * hd + d8, hd); simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[dt] = acc; }\n"
    \\           "    }\n"
    \\           TERMITE_MSL_ATTENTION_PUBLISH
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_DENSE_CAUSAL_ATTENTION(termite_attention_f32_dense_causal_sg_q16) "\n"
    \\           "    termite_attention_dense_q16_impl<float, false, 0u>(q, k, v, output, p, shmem, tid, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;
