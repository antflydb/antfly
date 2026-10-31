// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// native-program:entry termite_sdpa_f32_segments_tiled
           TERMITE_MSL_ABI_SEGMENTS_MATRIX(termite_sdpa_f32_segments_tiled) "\n"
           "    constexpr uint BQ = 32u, BK = 16u, DM = 64u;\n"
           "    threadgroup float ks[BK * DM], vs[BK * DM], ss[BQ * BK], diag[4u * 64u];\n"
           "    threadgroup uint rg[BQ * 6u];\n"
           "    threadgroup int qp[BQ], kp[BK];\n"
           "    threadgroup float mrow[BQ], lrow[BQ];\n"
           "    const uint D = p.head_dim, hidden = p.num_heads * D, h = tg.y, q0 = tg.x * BQ, t = uint(tid), s8 = uint(sg) * 8u, L = uint(lane);\n"
           "    const uint nq = min(BQ, p.queries - q0), nd = D / 8u;\n"
           "    const float scale = rsqrt(float(D)), neg = -INFINITY;\n"
           "    threadgroup float *stage = ss;\n"
           "    for (uint i = t; i < BQ * 6u; i += 128u) rg[i] = i / 6u < nq ? ranges[(q0 + i / 6u) * 6u + i % 6u] : 0u;\n"
           "    if (t < BQ) { qp[t] = t < nq ? qpos[q0 + t] : 0; mrow[t] = neg; lrow[t] = 0.0f; }\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    // Block bounds for tile skipping: the union of key ranges and the\n"
           "    // query position extent. Every simdgroup computes them identically.\n"
           "    uint lo = 0xffffffffu, hi = 0u; int pmin = 2147483647, pmax = -2147483647 - 1;\n"
           "    if (L < nq) { for (uint r = 0u; r < 3u; ++r) { uint a = rg[L * 6u + 2u * r], b = rg[L * 6u + 2u * r + 1u]; if (a < b) { lo = min(lo, a); hi = max(hi, b); } } pmin = qp[L]; pmax = qp[L]; }\n"
           "    lo = simd_min(lo); hi = simd_max(hi); pmin = simd_min(pmin); pmax = simd_max(pmax);\n"
           "    // Q fragments (pre-scaled), staged through this simdgroup's score rows.\n"
           TERMITE_MSL_SEGMENT_FLASH32_BODY(
               "q",
               "    TERMITE_FOR_UNROLL(uint i = 0u; i < 8u; ++i) o[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n",
               "kpos",
               "termite_segments_visible(rg, qp, kp[c], r, kb + c, p.window)",
               "            for (uint j = 0u; j < 4u; ++j) { float w = vals[j] == neg ? 0.0f : exp(vals[j] - mnew); ss[r * BK + c0 + j] = w; sum += w; }\n",
               "            // 8 rows x 16 keys: four lanes per row, four keys per lane.\n")
           "    for (uint i = L; i < 64u; i += 32u) { uint r = s8 + i / 8u; diag[uint(sg) * 64u + i] = (i / 8u == i % 8u) ? (lrow[r] > 0.0f ? 1.0f / lrow[r] : 0.0f) : 0.0f; }\n"
           "    simdgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    simdgroup_float8x8 dg; simdgroup_load(dg, diag + uint(sg) * 64u, 8u);\n"
           "    TERMITE_FOR_UNROLL(uint dt = 0u; dt < 8u; ++dt) if (dt < nd) {\n"
           "        simdgroup_multiply(o[dt], dg, o[dt]);\n"
{{segment_tensor_publish}}
// native-program:entry termite_sdpa_f32_segments_wide
           TERMITE_MSL_ABI_SEGMENTS_MATRIX(termite_sdpa_f32_segments_wide) "\n"
           "    constexpr uint BQ = 8u, BK = 8u, DM = 512u;\n"
           "    threadgroup float kv[BK * DM], parts[4u * 64u], mrow[BQ], lrow[BQ], alpha[BQ];\n"
           "    threadgroup uint rg[BQ * 6u];\n"
           "    threadgroup int qp[BQ], kp[BK];\n"
           "    const uint D = p.head_dim, hidden = p.num_heads * D, h = tg.y, q0 = tg.x * BQ;\n"
           "    const uint t = uint(tid), s = uint(sg), L = uint(lane), panel = D / 4u, nd = panel / 8u;\n"
           "    const uint nq = min(BQ, p.queries - q0);\n"
           "    const float scale = rsqrt(float(D)), neg = -INFINITY;\n"
           "    for (uint i = t; i < BQ * 6u; i += 128u) rg[i] = i / 6u < nq ? ranges[(q0 + i / 6u) * 6u + i % 6u] : 0u;\n"
           "    if (t < BQ) { qp[t] = t < nq ? qpos[q0 + t] : 0; mrow[t] = neg; lrow[t] = 0.0f; }\n"
           "    for (uint i = t; i < BQ * D; i += 128u) { uint r = i / D, d = i % D; kv[r * DM + d] = r < nq ? q[(q0 + r) * hidden + h * D + d] : 0.0f; }\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           ANTFLY_MSL_SEGMENT_RANGE_ENVELOPE()
           "    simdgroup_float8x8 qm[16], o[16];\n"
           "    TERMITE_FOR_UNROLL(uint dt = 0u; dt < 16u; ++dt) if (dt < nd) {\n"
           "        simdgroup_load(qm[dt], kv + s * panel + dt * 8u, DM);\n"
           "        o[dt] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
           "    }\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    for (uint kb = lo; kb < hi; kb += BK) {\n"
           ANTFLY_MSL_SEGMENT_KEY_STAGE("i += 128u", "k[(kb + c) * hidden + h * D + d]")
           ANTFLY_MSL_SEGMENT_QK8("parts + s * 64u")
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (s == 0u) {\n"
           "            for (uint i = L; i < 64u; i += 32u) {\n"
           "                const uint r = i / BK, c = i % BK;\n"
           "                threadgroup const uint *x = rg + r * 6u;\n"
           "                const uint key = kb + c;\n"
           ANTFLY_MSL_SEGMENT_SCORE_VISIBLE()
           "                parts[i] = visible ? (parts[i] + parts[64u + i] + parts[128u + i] + parts[192u + i]) * scale : neg;\n"
           "            }\n"
           "            simdgroup_barrier(mem_flags::mem_threadgroup);\n"
           "            const uint r = L / 4u, c0 = (L % 4u) * 2u;\n"
           "            float x0 = parts[r * BK + c0], x1 = parts[r * BK + c0 + 1u];\n"
           ANTFLY_MSL_SEGMENT_SOFTMAX8("            parts[r * BK + c0] = w0; parts[r * BK + c0 + 1u] = w1;\n")
           "        }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (t < 64u) parts[64u + t] = t / 8u == t % 8u ? alpha[t / 8u] : 0.0f;\n"
           "        for (uint i = t; i < BK * D; i += 128u) { uint c = i / D, d = i % D; kv[c * DM + d] = c < nk ? v[(kb + c) * hidden + h * D + d] : 0.0f; }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           ANTFLY_MSL_SEGMENT_PV8("        simdgroup_load(dg, parts + 64u, 8u); simdgroup_load(pm, parts, BK);\n")
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    }\n"
           "    if (t < 64u) parts[t] = t / 8u == t % 8u && lrow[t / 8u] > 0.0f ? 1.0f / lrow[t / 8u] : 0.0f;\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    simdgroup_float8x8 dg; simdgroup_load(dg, parts, 8u);\n"
           "    TERMITE_FOR_UNROLL(uint dt = 0u; dt < 16u; ++dt) if (dt < nd) {\n"
           "        simdgroup_multiply(o[dt], dg, o[dt]);\n"
           "        simdgroup_store(o[dt], kv + s * 64u, 8u);\n"
           "        simdgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        for (uint i = L; i < 64u; i += 32u) { uint r = i / 8u; if (r < nq) output[(q0 + r) * hidden + h * D + s * panel + dt * 8u + i % 8u] = kv[s * 64u + i]; }\n"
           "        simdgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    }\n"
           "}\n"
// native-program:entry termite_sdpa_f32_segments_blocks
           TERMITE_MSL_ABI_SEGMENTS_BLOCKS(termite_sdpa_f32_segments_blocks) "\n"
           "    threadgroup float kv[8u * 512u], parts[16u * 64u], mrow[32u], lrow[32u], alpha[32u];\n"
           "    threadgroup uint rg[32u * 6u];\n"
           "    threadgroup int qp[32u], kp[8u];\n"
           "    termite_sdpa_f32_blocks_impl<false, 512u>(q, k, v, ranges, qpos, kpos, output, p, tid, sg, lane, tpg, tg, kv, parts, mrow, lrow, alpha, rg, qp, kp);\n"
           "}\n"
// native-program:entry termite_sdpa_f32_grouped_short256
           TERMITE_MSL_ABI_GROUPED_SHORT_ATTENTION(termite_sdpa_f32_grouped_short256) " termite_sdpa_f32_grouped_short_impl<256>(q,k,v,ranges,qpos,kpos,output,p,lane,tg); }\n"
// native-program:entry termite_sdpa_f32_grouped_short512
           TERMITE_MSL_ABI_GROUPED_SHORT_ATTENTION(termite_sdpa_f32_grouped_short512) " termite_sdpa_f32_grouped_short_impl<512>(q,k,v,ranges,qpos,kpos,output,p,lane,tg); }\n"
// native-program:entry termite_sdpa_f32_grouped_matrix64
           TERMITE_MSL_ABI_SEGMENTS_MATRIX(termite_sdpa_f32_grouped_matrix64) "\n"
           "    constexpr uint BQ = 16u, BK = 64u, D = 512u;\n"
           "    threadgroup float partial[4096], weights[1024], diagonal[128], mrow[BQ], lrow[BQ], alpha[BQ];\n"
           "    threadgroup uint rg[BQ * 6u];\n"
           "    threadgroup int qp[BQ];\n"
           ANTFLY_MSL_GROUPED_MATRIX_GEOMETRY()
           "    const uint nq = min(BQ, p.queries - q0), pb = qb * 2048u + s * 512u;\n"
           TERMITE_MSL_GROUPED_MATRIX_RANGES
           "    simdgroup_float8x8 qm[16], o[16];\n"
           "    TERMITE_FOR_UNROLL(uint dt = 0; dt < 16u; ++dt) {\n"
           "        simdgroup_load(qm[dt], q + (q0 + qb * 8u) * hidden + h * D + s * 128u + dt * 8u, hidden);\n"
           "        o[dt] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
           "    }\n"
           "    lo = lo == 0xffffffffu ? lo : lo / 8u * 8u;\n"
           "    for (uint kb = lo; kb < hi; kb += BK) {\n"
           ANTFLY_MSL_GROUPED_MATRIX_QK(
               "8",
               "",
               "if (key < p.keys) ",
               "                simdgroup_load(km, k + key * kv_hidden + kh * D + s * 128u + dt * 8u, kv_hidden, ulong2(0,0), true);\n",
               "")
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (s == 0u) for (uint row = 0; row < 8u; ++row) {\n"
           "            const uint r = qb * 8u + row;\n"
           "            float scores[2];\n"
           "            for (uint pair = 0; pair < 2u; ++pair) {\n"
           "                const uint col = L + pair * 32u, key = kb + col, at = qb * 2048u + row * BK + col;\n"
           "                const bool in_key = key < p.keys;\n"
           "                const int position = in_key ? kpos[key] : 0;\n"
           "                const uint distance = qp[r] >= position ? uint(qp[r]) - uint(position) : uint(position) - uint(qp[r]);\n"
           "                threadgroup const uint *x = rg + r * 6u;\n"
           "                const bool visible = r < nq && in_key && ((key >= x[0] && key < x[1]) || (key >= x[2] && key < x[3]) || (key >= x[4] && key < x[5])) && (p.window == 0xffffffffu || distance <= p.window);\n"
           "                scores[pair] = visible ? (partial[at] + partial[at + 512u] + partial[at + 1024u] + partial[at + 1536u]) * scale : neg;\n"
           "            }\n"
           "            const float maximum = max(mrow[r], simd_max(max(scores[0], scores[1])));\n"
           "            const float rescale = maximum == neg ? 1.0f : mrow[r] == neg ? 0.0f : exp(mrow[r] - maximum);\n"
           "            const float w0 = scores[0] == neg ? 0.0f : exp(scores[0] - maximum), w1 = scores[1] == neg ? 0.0f : exp(scores[1] - maximum);\n"
           "            weights[qb * 512u + row * BK + L] = w0;\n"
           "            weights[qb * 512u + row * BK + L + 32u] = w1;\n"
           "            const float sum = simd_sum(w0 + w1);\n"
           "            if (L == 0u) { alpha[r] = rescale; lrow[r] = lrow[r] * rescale + sum; mrow[r] = maximum; }\n"
           "        }\n"
           TERMITE_MSL_GROUPED_MATRIX_RESCALE
           ANTFLY_MSL_GROUPED_MATRIX_PV(
               "8",
               "            if (key >= p.keys) break;\n",
               "512",
               "                simdgroup_load(vm, v + key * kv_hidden + kh * D + s * 128u + dt * 8u, kv_hidden);\n",
               "")
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    }\n"
           TERMITE_MSL_GROUPED_MATRIX_PUBLISH
           "}\n"
// native-program:entry termite_sdpa_f32_grouped_matrix32
           TERMITE_MSL_ABI_SEGMENTS_MATRIX(termite_sdpa_f32_grouped_matrix32) "\n"
           "    constexpr uint BQ = 16u, BK = 32u, D = 512u;\n"
           "    threadgroup float partial[2048], weights[512], diagonal[128], tail[4096], mrow[BQ], lrow[BQ], alpha[BQ];\n"
           "    threadgroup uint rg[BQ * 6u];\n"
           "    threadgroup int qp[BQ];\n"
           ANTFLY_MSL_GROUPED_MATRIX_GEOMETRY()
           "    const uint nq = min(BQ, p.queries - q0), pb = qb * 1024u + s * 256u;\n"
           TERMITE_MSL_GROUPED_MATRIX_RANGES
           "    simdgroup_float8x8 qm[16], o[16];\n"
           "    for (uint b = 0; b < 2u; ++b) {\n"
           "        if (q0 + b * 8u + 8u > p.queries) {\n"
           "            for (uint i = t; i < 8u * D; i += 256u) tail[i] = b * 8u + i / D < nq ? q[(q0 + b * 8u + i / D) * hidden + h * D + i % D] : 0;\n"
           "        }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (qb == b) TERMITE_FOR_UNROLL(uint dt = 0; dt < 16u; ++dt) {\n"
           "            if (q0 + b * 8u + 8u <= p.queries) simdgroup_load(qm[dt], q + (q0 + b * 8u) * hidden + h * D + s * 128u + dt * 8u, hidden);\n"
           "            else simdgroup_load(qm[dt], tail + s * 128u + dt * 8u, D);\n"
           "            o[dt] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
           "        }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    }\n"
           "    for (uint kb = lo; kb < hi; kb += BK) {\n"
           ANTFLY_MSL_GROUPED_MATRIX_QK(
               "4",
               "            if (key + 8u > p.keys) for (uint i = t; i < 8u * D; i += 256u) tail[i] = key + i / D < p.keys ? k[(key + i / D) * kv_hidden + kh * D + i % D] : 0;\n            threadgroup_barrier(mem_flags::mem_threadgroup);\n",
               "",
               "                if (key + 8u <= p.keys) simdgroup_load(km, k + key * kv_hidden + kh * D + s * 128u + dt * 8u, kv_hidden, ulong2(0,0), true);\n                else simdgroup_load(km, tail + s * 128u + dt * 8u, D, ulong2(0,0), true);\n",
               "            threadgroup_barrier(mem_flags::mem_threadgroup);\n")
           "        if (s == 0u) for (uint row = 0; row < 8u; ++row) {\n"
           "            const uint r = qb * 8u + row, key = kb + L, at = qb * 1024u + row * BK + L;\n"
           "            const bool in_key = key < p.keys;\n"
           "            const int position = in_key ? kpos[key] : 0;\n"
           "            const uint distance = qp[r] >= position ? uint(qp[r]) - uint(position) : uint(position) - uint(qp[r]);\n"
           "            threadgroup const uint *x = rg + r * 6u;\n"
           "            const bool visible = r < nq && in_key && ((key >= x[0] && key < x[1]) || (key >= x[2] && key < x[3]) || (key >= x[4] && key < x[5])) && (p.window == 0xffffffffu || distance <= p.window);\n"
           "            const float score = visible ? (partial[at] + partial[at + 256u] + partial[at + 512u] + partial[at + 768u]) * scale : neg;\n"
           "            const float maximum = max(mrow[r], simd_max(score));\n"
           "            const float rescale = maximum == neg ? 1.0f : mrow[r] == neg ? 0.0f : exp(mrow[r] - maximum);\n"
           "            const float weight = score == neg ? 0.0f : exp(score - maximum);\n"
           "            weights[qb * 256u + row * BK + L] = weight;\n"
           "            const float sum = simd_sum(weight);\n"
           "            if (L == 0u) { alpha[r] = rescale; lrow[r] = lrow[r] * rescale + sum; mrow[r] = maximum; }\n"
           "        }\n"
           TERMITE_MSL_GROUPED_MATRIX_RESCALE
           ANTFLY_MSL_GROUPED_MATRIX_PV(
               "4",
               "            if (key + 8u > p.keys) for (uint i = t; i < 8u * D; i += 256u) tail[i] = key + i / D < p.keys ? v[(key + i / D) * kv_hidden + kh * D + i % D] : 0;\n            threadgroup_barrier(mem_flags::mem_threadgroup);\n",
               "256",
               "                if (key + 8u <= p.keys) simdgroup_load(vm, v + key * kv_hidden + kh * D + s * 128u + dt * 8u, kv_hidden);\n                else simdgroup_load(vm, tail + s * 128u + dt * 8u, D);\n",
               "            threadgroup_barrier(mem_flags::mem_threadgroup);\n")
           "    }\n"
           TERMITE_MSL_GROUPED_MATRIX_PUBLISH
           "}\n"
// native-program:entry termite_sdpa_f32_grouped_blocks
           TERMITE_MSL_ABI_SEGMENTS_BLOCKS(termite_sdpa_f32_grouped_blocks) "\n"
           "    threadgroup float kv[8u * 512u], parts[16u * 64u], mrow[32u], lrow[32u], alpha[32u];\n"
           "    threadgroup uint rg[32u * 6u];\n"
           "    threadgroup int qp[32u], kp[8u];\n"
           "    termite_sdpa_f32_blocks_impl<true, 512u>(q, k, v, ranges, qpos, kpos, output, p, tid, sg, lane, tpg, tg, kv, parts, mrow, lrow, alpha, rg, qp, kp);\n"
           "}\n"
// native-program:entry termite_sdpa_f32_grouped_blocks256
           TERMITE_MSL_ABI_SEGMENTS_BLOCKS(termite_sdpa_f32_grouped_blocks256) "\n"
           "    threadgroup float kv[8u * 256u], parts[16u * 64u], mrow[32u], lrow[32u], alpha[32u];\n"
           "    threadgroup uint rg[32u * 6u];\n"
           "    threadgroup int qp[32u], kp[8u];\n"
           "    termite_sdpa_f32_blocks_impl<true, 256u>(q, k, v, ranges, qpos, kpos, output, p, tid, sg, lane, tpg, tg, kv, parts, mrow, lrow, alpha, rg, qp, kp);\n"
           "}\n"
// native-program:entry termite_sdpa_f32_grouped_blocks256x16
           TERMITE_MSL_ABI_SEGMENTS_BLOCKS(termite_sdpa_f32_grouped_blocks256x16) "\n"
           "    constexpr uint MQ = 32u, BK = 16u, DM = 256u, PS = 128u;\n"
           "    const uint width = uint(tpg.x), blocks = width / 128u, BQ = blocks * 8u;\n"
           "    threadgroup float kv[BK * DM], parts[4u * 4u * PS], mrow[MQ], lrow[MQ], alpha[MQ];\n"
           "    threadgroup uint rg[MQ * 6u];\n"
           "    threadgroup int qp[MQ], kp[BK];\n"
           "    const uint D = p.head_dim, hidden = p.num_heads * D, h = tg.y, q0 = tg.x * BQ;\n"
           "    const uint t = uint(tid), s = uint(sg) % 4u, qb = uint(sg) / 4u, pb = qb * 4u * PS, L = uint(lane), panel = D / 4u, nd = panel / 8u;\n"
           "    const uint nq = min(BQ, p.queries - q0), kv_hidden = p.reserved0 * D, kh = h / (p.num_heads / p.reserved0);\n"
           "    const float scale = as_type<float>(p.reserved1), neg = -INFINITY;\n"
           "    for (uint i = t; i < BQ * 6u; i += width) rg[i] = i / 6u < nq ? ranges[(q0 + i / 6u) * 6u + i % 6u] : 0u;\n"
           "    if (t < BQ) { qp[t] = t < nq ? qpos[q0 + t] : 0; mrow[t] = neg; lrow[t] = 0.0f; }\n"
           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           ANTFLY_MSL_SEGMENT_RANGE_ENVELOPE()
           "    simdgroup_float8x8 qm[8], o[8];\n"
           "    for (uint block = 0u; block < blocks; ++block) {\n"
           "        for (uint i = t; i < 8u * D; i += width) { uint r = i / D + block * 8u, d = i % D; kv[(i / D) * DM + d] = r < nq ? q[(q0 + r) * hidden + h * D + d] : 0.0f; }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (qb == block) TERMITE_FOR_UNROLL(uint dt = 0u; dt < 8u; ++dt) if (dt < nd) { simdgroup_load(qm[dt], kv + s * panel + dt * 8u, DM); o[dt] = make_filled_simdgroup_matrix<float, 8>(0.0f); }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    }\n"
           "    for (uint kb = lo; kb < hi; kb += BK) {\n"
           ANTFLY_MSL_SEGMENT_KEY_STAGE("i += width", "k[(kb + c) * kv_hidden + kh * D + d]")
           "        for (uint ki = 0; ki < 2; ++ki) {\n"
           "            simdgroup_float8x8 acc = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
           "            TERMITE_FOR_UNROLL(uint dt = 0u; dt < 8u; ++dt) if (dt < nd) { simdgroup_float8x8 km; simdgroup_load(km, kv + ki * 8u * DM + s * panel + dt * 8u, DM, ulong2(0, 0), true); simdgroup_multiply_accumulate(acc, qm[dt], km, acc); }\n"
           "            simdgroup_store(acc, parts + pb + s * PS + ki * 8u, BK);\n"
           "        }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (s == 0u) {\n"
           "            for (uint i = L; i < PS; i += 32u) {\n"
           "                const uint r = qb * 8u + i / BK, c = i % BK, key = kb + c;\n"
           "                threadgroup const uint *x = rg + r * 6u;\n"
           ANTFLY_MSL_SEGMENT_SCORE_VISIBLE()
           "                parts[pb + i] = visible ? (parts[pb + i] + parts[pb + PS + i] + parts[pb + 2u * PS + i] + parts[pb + 3u * PS + i]) * scale : neg;\n"
           "            }\n"
           "            simdgroup_barrier(mem_flags::mem_threadgroup);\n"
           "            const uint r = qb * 8u + L / 4u, pr = (L / 4u) * BK + pb, c0 = (L % 4u) * 4u;\n"
           "            float mx = neg;\n"
           "            for (uint j = 0; j < 4; ++j) mx = max(mx, parts[pr + c0 + j]);\n"
           "            mx = max(mx, simd_shuffle_xor(mx, 1)); mx = max(mx, simd_shuffle_xor(mx, 2));\n"
           "            const float mold = mrow[r], mnew = max(mold, mx);\n"
           "            const float a = mnew == neg ? 1.0f : mold == neg ? 0.0f : exp(mold - mnew);\n"
           "            float sum = 0.0f;\n"
           "            for (uint j = 0; j < 4; ++j) { float x = parts[pr + c0 + j], w = x == neg ? 0.0f : exp(x - mnew); parts[pr + c0 + j] = w; sum += w; }\n"
           "            sum += simd_shuffle_xor(sum, 1); sum += simd_shuffle_xor(sum, 2);\n"
           "            if (L % 4u == 0u) { lrow[r] = lrow[r] * a + sum; mrow[r] = mnew; alpha[r] = a; }\n"
           "        }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (t < blocks * 64u) { uint b = t / 64u, i = t % 64u; parts[b * 4u * PS + PS + i] = i / 8u == i % 8u ? alpha[b * 8u + i / 8u] : 0.0f; }\n"
           "        for (uint i = t; i < BK * D; i += width) { uint c = i / D, d = i % D; kv[c * DM + d] = c < nk ? v[(kb + c) * kv_hidden + kh * D + d] : 0.0f; }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        simdgroup_float8x8 dg; simdgroup_load(dg, parts + pb + PS, 8u);\n"
           "        TERMITE_FOR_UNROLL(uint dt = 0u; dt < 8u; ++dt) if (dt < nd) simdgroup_multiply(o[dt], dg, o[dt]);\n"
           "        for (uint ki = 0; ki < 2; ++ki) {\n"
           "            simdgroup_float8x8 pm; simdgroup_load(pm, parts + pb + ki * 8u, BK);\n"
           "            TERMITE_FOR_UNROLL(uint dt = 0u; dt < 8u; ++dt) if (dt < nd) { simdgroup_float8x8 vm; simdgroup_load(vm, kv + ki * 8u * DM + s * panel + dt * 8u, DM); simdgroup_multiply_accumulate(o[dt], pm, vm, o[dt]); }\n"
           "        }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    }\n"
           "    if (t < blocks * 64u) { uint b = t / 64u, i = t % 64u, r = b * 8u + i / 8u; parts[b * 4u * PS + i] = i / 8u == i % 8u && lrow[r] > 0.0f ? 1.0f / lrow[r] : 0.0f; }\n"
           ANTFLY_MSL_SEGMENT_BLOCK_PUBLISH("8u","        simdgroup_multiply(o[dt], dg, o[dt]); simdgroup_store(o[dt], kv + uint(sg) * 64u, 8u);")
           "}\n"
// native-program:entry termite_sdpa_f32_vision_hd64_q8
           TERMITE_MSL_ABI_VISION_SDPA(termite_sdpa_f32_vision_hd64_q8) " threadgroup float weights[8][32], kv_tile[32][64]; termite_sdpa_vision_hd64_impl<1>(q, k, v, bias, mask, output, p, tid, lane, sgitg, tg, weights, kv_tile); }\n"
// native-program:entry termite_sdpa_f32_vision_hd64_q32
           TERMITE_MSL_ABI_VISION_SDPA(termite_sdpa_f32_vision_hd64_q32) " threadgroup float weights[32][32], kv_tile[32][64]; termite_sdpa_vision_hd64_impl<4>(q, k, v, bias, mask, output, p, tid, lane, sgitg, tg, weights, kv_tile); }\n"
// native-program:entry termite_sdpa_f32_vision_hd64_flash_q16
           TERMITE_MSL_ABI_VISION_SDPA(termite_sdpa_f32_vision_hd64_flash_q16) "\n"
           "    (void)bias; (void)mask; if (p.seq_len == 0u || p.head_dim != 64u || p.bias_mode != 0u || p.has_mask != 0u || p.layout != 1u || sgitg >= 8u) return;\n"
           "    const uint query_tile_count = (p.seq_len + 15u) / 16u; const uint query_tile = tg.x % query_tile_count; const uint tmp = tg.x / query_tile_count; const uint h = tmp % p.num_heads; const uint b = tmp / p.num_heads; if (b >= p.batch) return;\n"
           "    const uint q0 = query_tile * 16u; const uint hidden = p.num_heads * 64u; const uint qgroup = uint(sgitg) >> 2u; const uint subgroup = uint(sgitg) & 3u; const uint dslice = subgroup * 16u; const float neg_inf = -3.402823466e+38f;\n"
           "    threadgroup half sq[1024]; threadgroup half skv[2048]; threadgroup float ss[512]; threadgroup half sp[512]; threadgroup float sM[16]; threadgroup float sS[16]; threadgroup float sdiag[128]; threadgroup float so[1024];\n"
           "    for (uint i = uint(tid); i < 1024u; i += 256u) { const uint qr = i >> 6u; const uint d = i & 63u; const uint qi = q0 + qr; sq[i] = qi < p.seq_len ? half(q[(b * p.seq_len + qi) * hidden + h * 64u + d] * 0.125f) : half(0.0f); }\n"
           "    if (tid < 16u) { sM[tid] = neg_inf; sS[tid] = 0.0f; } if (tid < 128u) sdiag[tid] = 0.0f; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    simdgroup_float8x8 mo[2]; mo[0] = make_filled_simdgroup_matrix<float, 8>(0.0f); mo[1] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
           "    for (uint key_start = 0u; key_start < p.seq_len; key_start += 32u) {\n"
           "        " ANTFLY_MSL_VISION64_STAGE("half", "k[(b * p.seq_len + ki) * hidden + h * 64u + d]") "\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        " ANTFLY_MSL_VISION64_QK("simdgroup_half8x8") "\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        " ANTFLY_MSL_VISION64_ONLINE("2u", "half") "\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag + qgroup * 64u, 8u); " ANTFLY_MSL_VISION64_SCALE("mcorr", "mo[dt]") "\n"
           "        " ANTFLY_MSL_VISION64_STAGE("half", "v[(b * p.seq_len + ki) * hidden + h * 64u + d]") "\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        " ANTFLY_MSL_VISION64_PV("simdgroup_half8x8", "mo[dt]") "\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    }\n"
           ANTFLY_MSL_VISION64_DIAGONAL("16u")
           "    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag + qgroup * 64u, 8u); " ANTFLY_MSL_VISION64_SCALE("minv", "mo[dt]") "\n"
           "    const uint output_base = (b * p.seq_len + q0 + qgroup * 8u) * hidden + h * 64u + dslice; if (q0 + 16u <= p.seq_len) { for (uint dt = 0u; dt < 2u; ++dt) simdgroup_store(mo[dt], output + output_base + dt * 8u, hidden); return; }\n"
           "    for (uint dt = 0u; dt < 2u; ++dt) simdgroup_store(mo[dt], so + uint(sgitg) * 128u + dt * 8u, 16u); " ANTFLY_MSL_VISION64_TAIL() "\n"
           "}\n"
// native-program:entry termite_sdpa_f32_bert_prefill_s256_hd64_q8
           TERMITE_MSL_ABI_VISION_SDPA(termite_sdpa_f32_bert_prefill_s256_hd64_q8) "\n"
           "    (void)bias; (void)mask; if (p.seq_len != 256u || p.head_dim != 64u || p.bias_mode != 0u || p.has_mask != 0u || sgitg >= 8u) return;\n"
           "    const uint tile = tg.x & 31u; const uint tmp = tg.x >> 5u; const uint h = tmp % p.num_heads; const uint b = tmp / p.num_heads; if (b >= p.batch) return;\n"
           "    const uint qi = tile * 8u + uint(sgitg); const uint hidden = p.num_heads * 64u; const uint bh = b * p.num_heads + h; const uint q_base = p.layout == 1u ? (b * 256u + qi) * hidden + h * 64u : (bh * 256u + qi) * 64u;\n"
           "    const float q0 = q[q_base + uint(lane)]; const float q1 = q[q_base + uint(lane) + 32u]; threadgroup float scores[8][256]; threadgroup float kv_tile[32][64]; float max_score = -3.402823466e+38f;\n"
           ANTFLY_MSL_Q8_STAGED_LOOP("256u",
           "        for (uint element = uint(tid); element < 2048u; element += 256u) { uint key = key_start + element / 64u; uint d = element & 63u; uint src = p.layout == 1u ? (b * 256u + key) * hidden + h * 64u + d : (bh * 256u + key) * 64u + d; kv_tile[element / 64u][d] = k[src]; }\n"
           ,
           "        for (uint key = 0u; key < 32u; ++key) { float score = " ANTFLY_MSL_Q8_DOT "; max_score = max(max_score, score); if (lane == 0u) scores[sgitg][key_start + key] = score; }\n"
           )
           "    float denom = 0.0f; for (uint key = uint(lane); key < 256u; key += 32u) { float weight = exp(scores[sgitg][key] - max_score); scores[sgitg][key] = weight; denom += weight; } denom = simd_sum(denom);\n"
           "    float acc0 = 0.0f; float acc1 = 0.0f;\n"
           ANTFLY_MSL_Q8_STAGED_LOOP("256u",
           "        for (uint element = uint(tid); element < 2048u; element += 256u) { uint key = key_start + element / 64u; uint d = element & 63u; uint src = p.layout == 1u ? (b * 256u + key) * hidden + h * 64u + d : (bh * 256u + key) * 64u + d; kv_tile[element / 64u][d] = v[src]; }\n"
           ,
           "        for (uint key = 0u; key < 32u; ++key) { float weight = scores[sgitg][key_start + key]; " ANTFLY_MSL_Q8_ACCUMULATE " }\n"
           )
           "    output[q_base + uint(lane)] = acc0 / denom; output[q_base + uint(lane) + 32u] = acc1 / denom;\n"
           "}\n"
// native-program:entry termite_sdpa_f32_nomic_q8
           TERMITE_MSL_ABI_VISION_SDPA(termite_sdpa_f32_nomic_q8) "\n"
           "    (void)bias; if (p.seq_len == 0u || p.seq_len > 128u || (p.seq_len & 7u) != 0u || p.head_dim != 64u || p.bias_mode != 0u || p.layout != 1u || sgitg >= 8u) return;\n"
           "    const uint query_tiles = p.seq_len >> 3u; const uint query_tile = tg.x % query_tiles; const uint tmp = tg.x / query_tiles; const uint h = tmp % p.num_heads; const uint b = tmp / p.num_heads; if (b >= p.batch) return;\n"
           "    const uint qi = query_tile * 8u + uint(sgitg); const uint hidden = p.num_heads * 64u; const uint q_base = (b * p.seq_len + qi) * hidden + h * 64u;\n"
           "    const float q0 = q[q_base + uint(lane)]; const float q1 = q[q_base + uint(lane) + 32u]; threadgroup float scores[8][128]; threadgroup float kv_tile[32][64]; const float neg_inf = -3.402823466e+38f; float max_score = neg_inf;\n"
           ANTFLY_MSL_Q8_STAGED_LOOP("p.seq_len",
           "        const uint tile_keys = min(32u, p.seq_len - key_start); const uint tile_elements = tile_keys * 64u;\n"
           "        for (uint element = uint(tid); element < tile_elements; element += 256u) { const uint key = key_start + element / 64u; const uint d = element & 63u; kv_tile[element / 64u][d] = k[(b * p.seq_len + key) * hidden + h * 64u + d]; }\n"
           ,
           "        for (uint key = 0u; key < tile_keys; ++key) { float score = " ANTFLY_MSL_Q8_DOT "; const uint absolute_key = key_start + key; if (p.has_mask != 0u && mask[b * p.seq_len + absolute_key] == 0.0f) score = neg_inf; max_score = max(max_score, score); if (lane == 0u) scores[sgitg][absolute_key] = score; }\n"
           )
           "    float denom = 0.0f; for (uint key = uint(lane); key < p.seq_len; key += 32u) { const float score = scores[sgitg][key]; const float weight = score > neg_inf ? exp(score - max_score) : 0.0f; scores[sgitg][key] = weight; denom += weight; } denom = simd_sum(denom);\n"
           "    float acc0 = 0.0f; float acc1 = 0.0f;\n"
           ANTFLY_MSL_Q8_STAGED_LOOP("p.seq_len",
           "        const uint tile_keys = min(32u, p.seq_len - key_start); const uint tile_elements = tile_keys * 64u;\n"
           "        for (uint element = uint(tid); element < tile_elements; element += 256u) { const uint key = key_start + element / 64u; const uint d = element & 63u; kv_tile[element / 64u][d] = v[(b * p.seq_len + key) * hidden + h * 64u + d]; }\n"
           ,
           "        for (uint key = 0u; key < tile_keys; ++key) { const float weight = scores[sgitg][key_start + key]; " ANTFLY_MSL_Q8_ACCUMULATE " }\n"
           )
           "    output[q_base + uint(lane)] = denom > 0.0f ? acc0 / denom : 0.0f; output[q_base + uint(lane) + 32u] = denom > 0.0f ? acc1 / denom : 0.0f;\n"
           "}\n"
// native-program:entry termite_disentangled_relative_attention_f32_tg
           TERMITE_MSL_ABI_RELATIVE_ATTENTION(termite_disentangled_relative_attention_f32_tg) "\n"
           "    if (p.seq_len == 0u || p.head_dim == 0u || p.num_heads == 0u) return;\n"
           "    uint qi = tg.x; uint h = tg.y; uint b = tg.z; if (qi >= p.seq_len || h >= p.num_heads || b >= p.batch) return;\n"
           "    uint hidden = p.num_heads * p.head_dim; uint head_off = h * p.head_dim; uint q_base = (b * p.seq_len + qi) * hidden + head_off; float scale = rsqrt(float(p.head_dim) * 3.0f);\n"
           "    uint lid = uint(tid); uint width = uint(tpg.x); threadgroup float *scores = scratch; threadgroup float *partials = scratch + p.seq_len;\n"
           "    float local_best = -3.402823466e+38f;\n"
           "    for (uint ki = lid; ki < p.seq_len; ki += width) {\n"
           "        bool allowed = p.has_mask == 0u || mask[b * p.seq_len + ki] != 0.0f; float score = -3.402823466e+38f;\n"
           "        if (allowed) { uint k_base = (b * p.seq_len + ki) * hidden + head_off; uint rel_idx = qi + p.seq_len - 1u - ki; uint rel_row = p.reserved1 != 0u ? uint(relative_index[rel_idx]) : rel_idx; uint rel_base = rel_row * hidden + head_off; float acc = 0.0f;\n"
           "            if (p.head_dim == 64u) { const device float4 *q4 = reinterpret_cast<const device float4 *>(q + q_base); const device float4 *k4 = reinterpret_cast<const device float4 *>(k + k_base); const device float4 *kr4 = reinterpret_cast<const device float4 *>(k_r + rel_base); const device float4 *qr4 = reinterpret_cast<const device float4 *>(q_r + rel_base); for (uint x4 = 0u; x4 < 16u; ++x4) { float4 qx = q4[x4]; float4 kx = k4[x4]; acc += dot(qx, kx) + dot(qx, kr4[x4]) + dot(qr4[x4], kx); } } else { for (uint x = 0u; x < p.head_dim; ++x) { float qx = q[q_base + x]; acc += qx * k[k_base + x] + qx * k_r[rel_base + x] + q_r[rel_base + x] * k[k_base + x]; } }\n"
           "            score = acc * scale; local_best = max(local_best, score); }\n"
{{attention_row_maximum}}
           // Barrier before partials is reused for the sum reduction: without it a
           // fast thread's write races the `best` read of a slower SIMD group.
           "    float best = partials[0]; threadgroup_barrier(mem_flags::mem_threadgroup); float local_sum = 0.0f;\n"
           "    for (uint ki = lid; ki < p.seq_len; ki += width) { float s = scores[ki]; if (s > -3.0e38f) local_sum += exp(s - best); }\n"
           "    partials[lid] = local_sum; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) { if (lid < stride) partials[lid] += partials[lid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
           "    float denom = partials[0]; float inv_sum = denom > 0.0f ? 1.0f / denom : 0.0f;\n"
           "    for (uint ki = lid; ki < p.seq_len; ki += width) { float s = scores[ki]; scores[ki] = s > -3.0e38f ? exp(s - best) * inv_sum : 0.0f; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    for (uint d = lid; d < p.head_dim; d += width) { float accum = 0.0f; for (uint ki = 0u; ki < p.seq_len; ++ki) { uint k_base = (b * p.seq_len + ki) * hidden + head_off; accum += scores[ki] * v[k_base + d]; } output[(b * p.seq_len + qi) * hidden + head_off + d] = accum; }\n"
           "}\n"
// native-program:entry termite_disentangled_relative_attention_f32_flash4
           TERMITE_MSL_ABI_RELATIVE_ATTENTION(termite_disentangled_relative_attention_f32_flash4) "\n"
           "    if (p.seq_len == 0u || p.head_dim == 0u || p.num_heads == 0u) return;\n"
           "    uint q0 = tg.x * 4u; uint h = tg.y; uint b = tg.z; if (q0 >= p.seq_len || h >= p.num_heads || b >= p.batch) return;\n"
           "    uint block_m = min(4u, p.seq_len - q0); uint lid = uint(tid); uint width = uint(tpg.x); uint hidden = p.num_heads * p.head_dim; uint head_off = h * p.head_dim; float scale = rsqrt(float(p.head_dim) * 3.0f);\n"
           "    threadgroup float *scores = scratch; threadgroup float *bests = scratch + 4u * p.seq_len; threadgroup float *partials = bests + 4u;\n"
           "    float local_best0 = -3.402823466e+38f; float local_best1 = -3.402823466e+38f; float local_best2 = -3.402823466e+38f; float local_best3 = -3.402823466e+38f;\n"
           "    for (uint ki = lid; ki < p.seq_len; ki += width) {\n"
           "        bool allowed = p.has_mask == 0u || mask[b * p.seq_len + ki] != 0.0f; float s0 = -3.402823466e+38f; float s1 = -3.402823466e+38f; float s2 = -3.402823466e+38f; float s3 = -3.402823466e+38f;\n"
           "        if (allowed) { uint k_base = (b * p.seq_len + ki) * hidden + head_off;\n"
           "            if (p.head_dim == 64u) { const device float4 *k4 = reinterpret_cast<const device float4 *>(k + k_base); float acc0 = 0.0f; float acc1 = 0.0f; float acc2 = 0.0f; float acc3 = 0.0f;\n"
           "                for (uint x4 = 0u; x4 < 16u; ++x4) { float4 kx = k4[x4];\n"
           TERMITE_MSL_RELATIVE_QUERY_ROWS(TERMITE_MSL_RELATIVE_QUERY_VECTOR)
           "                } s0 = acc0 * scale; s1 = acc1 * scale; s2 = acc2 * scale; s3 = acc3 * scale;\n"
           "            } else { float acc0 = 0.0f; float acc1 = 0.0f; float acc2 = 0.0f; float acc3 = 0.0f; for (uint x = 0u; x < p.head_dim; ++x) { float kx = k[k_base + x];\n"
           TERMITE_MSL_RELATIVE_QUERY_ROWS(TERMITE_MSL_RELATIVE_QUERY_SCALAR)
           "                } s0 = acc0 * scale; s1 = acc1 * scale; s2 = acc2 * scale; s3 = acc3 * scale; }\n"
           "        }\n"
           "        if (block_m > 0u) { scores[ki] = s0; local_best0 = max(local_best0, s0); } if (block_m > 1u) { scores[p.seq_len + ki] = s1; local_best1 = max(local_best1, s1); } if (block_m > 2u) { scores[2u * p.seq_len + ki] = s2; local_best2 = max(local_best2, s2); } if (block_m > 3u) { scores[3u * p.seq_len + ki] = s3; local_best3 = max(local_best3, s3); }\n"
           "    }\n"
           "    partials[lid] = local_best0; partials[width + lid] = local_best1; partials[2u * width + lid] = local_best2; partials[3u * width + lid] = local_best3; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) { if (lid < stride) { partials[lid] = max(partials[lid], partials[lid + stride]); partials[width + lid] = max(partials[width + lid], partials[width + lid + stride]); partials[2u * width + lid] = max(partials[2u * width + lid], partials[2u * width + lid + stride]); partials[3u * width + lid] = max(partials[3u * width + lid], partials[3u * width + lid + stride]); } threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
           "    if (lid == 0u) { bests[0] = partials[0]; bests[1] = partials[width]; bests[2] = partials[2u * width]; bests[3] = partials[3u * width]; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    float sum0 = 0.0f; float sum1 = 0.0f; float sum2 = 0.0f; float sum3 = 0.0f; for (uint ki = lid; ki < p.seq_len; ki += width) { if (block_m > 0u) { float s = scores[ki]; if (s > -3.0e38f) sum0 += exp(s - bests[0]); } if (block_m > 1u) { float s = scores[p.seq_len + ki]; if (s > -3.0e38f) sum1 += exp(s - bests[1]); } if (block_m > 2u) { float s = scores[2u * p.seq_len + ki]; if (s > -3.0e38f) sum2 += exp(s - bests[2]); } if (block_m > 3u) { float s = scores[3u * p.seq_len + ki]; if (s > -3.0e38f) sum3 += exp(s - bests[3]); } }\n"
           "    partials[lid] = sum0; partials[width + lid] = sum1; partials[2u * width + lid] = sum2; partials[3u * width + lid] = sum3; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) { if (lid < stride) { partials[lid] += partials[lid + stride]; partials[width + lid] += partials[width + lid + stride]; partials[2u * width + lid] += partials[2u * width + lid + stride]; partials[3u * width + lid] += partials[3u * width + lid + stride]; } threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
           "    float inv0 = partials[0] > 0.0f ? 1.0f / partials[0] : 0.0f; float inv1 = partials[width] > 0.0f ? 1.0f / partials[width] : 0.0f; float inv2 = partials[2u * width] > 0.0f ? 1.0f / partials[2u * width] : 0.0f; float inv3 = partials[3u * width] > 0.0f ? 1.0f / partials[3u * width] : 0.0f;\n"
           "    for (uint ki = lid; ki < p.seq_len; ki += width) { if (block_m > 0u) { float s = scores[ki]; scores[ki] = s > -3.0e38f ? exp(s - bests[0]) * inv0 : 0.0f; } if (block_m > 1u) { float s = scores[p.seq_len + ki]; scores[p.seq_len + ki] = s > -3.0e38f ? exp(s - bests[1]) * inv1 : 0.0f; } if (block_m > 2u) { float s = scores[2u * p.seq_len + ki]; scores[2u * p.seq_len + ki] = s > -3.0e38f ? exp(s - bests[2]) * inv2 : 0.0f; } if (block_m > 3u) { float s = scores[3u * p.seq_len + ki]; scores[3u * p.seq_len + ki] = s > -3.0e38f ? exp(s - bests[3]) * inv3 : 0.0f; } } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    for (uint d = lid; d < p.head_dim; d += width) { float accum0 = 0.0f; float accum1 = 0.0f; float accum2 = 0.0f; float accum3 = 0.0f;\n"
           "        for (uint ki = 0u; ki < p.seq_len; ++ki) { uint v_base = (b * p.seq_len + ki) * hidden + head_off; float vx = v[v_base + d];\n"
           "            if (block_m > 0u) accum0 += scores[ki] * vx; if (block_m > 1u) accum1 += scores[p.seq_len + ki] * vx; if (block_m > 2u) accum2 += scores[2u * p.seq_len + ki] * vx; if (block_m > 3u) accum3 += scores[3u * p.seq_len + ki] * vx; }\n"
           "        if (block_m > 0u) output[(b * p.seq_len + q0) * hidden + head_off + d] = accum0; if (block_m > 1u) output[(b * p.seq_len + q0 + 1u) * hidden + head_off + d] = accum1;\n"
           "        if (block_m > 2u) output[(b * p.seq_len + q0 + 2u) * hidden + head_off + d] = accum2; if (block_m > 3u) output[(b * p.seq_len + q0 + 3u) * hidden + head_off + d] = accum3; }\n"
           "}\n"
// native-program:entry termite_attention_f32_dense_causal_sg_q16_f16kv
           TERMITE_MSL_ABI_ATTENTION_F16_KV(termite_attention_f32_dense_causal_sg_q16_f16kv) "\n"
           "    termite_attention_dense_q16_impl<half, true, 0u>(q, k, v, output, p, shmem, tid, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_attention_f32_dense_causal_sg_q16_f16kv_hd128
           TERMITE_MSL_ABI_ATTENTION_F16_KV(termite_attention_f32_dense_causal_sg_q16_f16kv_hd128) "\n"
           "    termite_attention_dense_q16_impl<half, true, 128u>(q, k, v, output, p, shmem, tid, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_attention_f32_dense_causal_sg_q16_f16kv_gqa2
           TERMITE_MSL_ABI_ATTENTION_F16_KV(termite_attention_f32_dense_causal_sg_q16_f16kv_gqa2) "\n"
           "    const uint hd = p.head_dim; const uint q0 = tg.x * 16u; const uint kv_h = tg.y;\n"
           "    if (q0 >= p.q_len || kv_h >= p.num_kv_heads || p.num_heads != 2u * p.num_kv_heads || hd % 32u != 0u || hd > 128u || p.has_bias != 0u || p.has_mask != 0u) return;\n"
           "    const float scale = rsqrt(float(hd)); const uint kv_head_base = kv_h * hd; const uint h0 = kv_h * 2u;\n"
           "    const uint q_stride = p.num_heads * hd; const uint kv_stride = p.num_kv_heads * hd;\n"
           "    threadgroup half *sq = (threadgroup half *)shmem;\n"
           "    threadgroup half *skv = sq + 32u * hd;\n"
           "    threadgroup char *fb = shmem + (32u + 32u) * hd * 2u;\n"
           "    threadgroup float *ss = (threadgroup float *)fb;\n"
           "    threadgroup half *sp = (threadgroup half *)(fb + 4096u);\n"
           "    threadgroup float *sM = (threadgroup float *)(fb + 6144u);\n"
           "    threadgroup float *sS = (threadgroup float *)(fb + 6272u);\n"
           "    threadgroup float *sdiag = (threadgroup float *)(fb + 6400u);\n"
           "    for (uint i = uint(tid); i < 32u * hd; i += 256u) { uint r = i / hd; uint d = i - r * hd; uint head = r >> 4u; uint j = r & 15u; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + (h0 + head) * hd + d] * scale) : half(0.0f); }\n"
           "    if (tid < 32u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; }\n"
           "    if (tid < 256u) sdiag[tid] = 0.0f;\n"
           "    const uint head_sel = uint(sgitg) >> 2u; const uint quarter = uint(sgitg) & 3u; const uint dslice = quarter * (hd / 4u);\n"
           TERMITE_MSL_DENSE_Q16_ACCUMULATORS
           "        const bool fast = (kc + 32u <= p.kv_len);\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        if (!fast) { " ANTFLY_MSL_DENSE_Q16_STAGE("256u", "k[ki * kv_stride + kv_head_base + d]") " threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
           ANTFLY_MSL_DENSE_Q16_QK("k + (kc + quarter * 8u) * kv_stride + kv_head_base", "sq + head_sel * 16u * hd + rb * 8u * hd + d", "skv + quarter * 8u * hd + d", "ss + head_sel * 512u + rb * 256u + quarter * 8u")
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "        for (uint jj = 0u; jj < 4u; ++jj) {\n"
           "            uint r = uint(sgitg) + jj * 8u; uint j = r & 15u; uint qi = q0 + j; uint query_pos = p.query_position_offset + qi; uint kk = uint(lane); uint ki = kc + kk;\n"
           ANTFLY_MSL_DENSE_CAUSAL_SCORE("ss[(r >> 4u) * 512u + j * 32u + kk]")
           "            float m_old; float m_new; float corr; float row_sum;\n"
           "            antfly_attention_online_row32(sc, sM, r, sp, (r >> 4u) * 512u + j * 32u + kk, m_old, m_new, corr, row_sum);\n"
           "            if (lane == 0u) { uint rbg = r >> 3u; uint jr = r & 7u; sS[r] = sS[r] * corr + row_sum; sM[r] = m_new; sdiag[rbg * 64u + jr * 8u + jr] = corr; }\n"
           "        }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           ANTFLY_MSL_DENSE_Q16_RESCALE("sdiag + (head_sel * 2u + rb) * 64u")
           "        if (!fast) { threadgroup_barrier(mem_flags::mem_threadgroup); " ANTFLY_MSL_DENSE_Q16_STAGE("256u", "v[ki * kv_stride + kv_head_base + d]") " }\n"
           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           ANTFLY_MSL_DENSE_Q16_PV("if (fast)", "sp + head_sel * 512u + rb * 256u + kk8 * 8u", "simdgroup_load(mv, v + (kc + kk8 * 8u) * kv_stride + kv_head_base + d8, kv_stride);")
           ANTFLY_MSL_DENSE_Q16_PV("else", "sp + head_sel * 512u + rb * 256u + kk8 * 8u", "simdgroup_load(mv, skv + kk8 * 8u * hd + d8, hd);")
           "    }\n"
// native-program:entry termite_polar4_attention_span
           TERMITE_MSL_ABI_COMPRESSED_ATTENTION_SPAN(termite_polar4_attention_span) "\n"
           "    termite_compressed_attention_span<termite_polar4_span_codec>(q, encoded_key, v, output, p, h);\n"
           "}\n"
// native-program:entry termite_turbo3_attention_span
           TERMITE_MSL_ABI_COMPRESSED_ATTENTION_SPAN(termite_turbo3_attention_span) "\n"
           "    termite_compressed_attention_span<termite_turbo3_span_codec>(q, encoded_key, v, output, p, h);\n"
           "}\n"];
