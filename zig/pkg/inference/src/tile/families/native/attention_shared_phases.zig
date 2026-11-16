// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Native schedule shells retain their exact compiler inputs. Online arithmetic
//! is supplied by the shared operation; masks, matrix staging and folds stay
//! in the independently qualified hardware schedules.
const program = @import("../../native_program.zig");
const online = @import("antfly_tile").native_attention.Online;

pub const metal = struct {
    pub const source_0 = program.render(
        \\#define TERMITE_MSL_SEGMENT_SCALAR_WEIGHTS \
        \\    "            float new_max = max(run_max, chunk_best); float rescale = run_max <= neg ? 0.0f : {{online_0}}; float local_sum = 0.0f;\n" \
        \\    "            for (uint j = lid; j < n; j += width) { float score = scores[j]; float w = score <= neg ? 0.0f : {{online_1}}; scores[j] = w; local_sum += w; }\n"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "run_max", .new_max = "new_max" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "score", .new_max = "new_max" }, null),
    });

    pub const source_1 = program.render(
        \\#define TERMITE_MSL_SEGMENT_FLASH32_BODY(Query, OutputInit, KeyPosition, Visible, Probability, ScoreComment) \
        \\    "    simdgroup_float8x8 qm[8];\n" \
        \\    "    TERMITE_FOR_UNROLL(uint dt = 0u; dt < 8u; ++dt) if (dt < nd) {\n" \
        \\    "        for (uint i = L; i < 64u; i += 32u) { uint r = s8 + i / 8u, d = dt * 8u + i % 8u; stage[s8 * BK + i] = r < nq ? " Query "[(q0 + r) * hidden + h * D + d] * scale : 0.0f; }\n" \
        \\    "        simdgroup_barrier(mem_flags::mem_threadgroup);\n" \
        \\    "        simdgroup_load(qm[dt], stage + s8 * BK, 8u);\n" \
        \\    "        simdgroup_barrier(mem_flags::mem_threadgroup);\n" \
        \\    "    }\n" \
        \\    "    simdgroup_float8x8 o[8];\n" \
        \\    OutputInit \
        \\    "    for (uint kb = lo; kb < hi; kb += BK) {\n" \
        \\    "        const uint nk = min(BK, hi - kb);\n" \
        \\    "        const int key_pos = L < nk ? " KeyPosition "[kb + L] : 0;\n" \
        \\    "        const bool near = L < nk && (p.window == 0xffffffffu || (key_pos >= pmin - int(p.window) && key_pos <= pmax + int(p.window)));\n" \
        \\    "        if (!simd_any(near)) continue;\n" \
        \\    "        if (t < BK) kp[t] = t < nk ? " KeyPosition "[kb + t] : 0;\n" \
        \\    TERMITE_MSL_SEGMENT_MMA_QK \
        \\    "        {\n" \
        \\    ScoreComment \
        \\    "            const uint r = s8 + L / 4u, c0 = (L % 4u) * 4u;\n" \
        \\    "            float vals[4]; float mx = neg;\n" \
        \\    "            for (uint j = 0u; j < 4u; ++j) { uint c = c0 + j; bool vis = r < nq && c < nk && " Visible "; vals[j] = vis ? ss[r * BK + c] : neg; mx = max(mx, vals[j]); }\n" \
        \\    "            mx = max(mx, simd_shuffle_xor(mx, 1)); mx = max(mx, simd_shuffle_xor(mx, 2));\n" \
        \\    "            const float mold = mrow[r], mnew = max(mold, mx);\n" \
        \\    "            float sum = 0.0f;\n" \
        \\    Probability \
        \\    "            sum += simd_shuffle_xor(sum, 1); sum += simd_shuffle_xor(sum, 2);\n" \
        \\    "            const float alpha = mnew == neg ? 1.0f : (mold == neg ? 0.0f : {{online_0}});\n" \
        \\    "            if (L % 4u == 0u) { lrow[r] = {{online_1}}; mrow[r] = mnew; diag[uint(sg) * 64u + (L / 4u) * 9u] = alpha; }\n" \
        \\    "            for (uint i = L; i < 64u; i += 32u) if (i / 8u != i % 8u) diag[uint(sg) * 64u + i] = 0.0f;\n" \
        \\    "        }\n" \
        \\    TERMITE_MSL_SEGMENT_MMA_PV \
        \\    "    }\n"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "mold", .new_max = "mnew" }, null),
        .online_1 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "lrow[r]", .correction = "alpha", .tile_sum = "sum" }, null),
    });

    pub const source_2 = program.render(
        \\        "                  c = isfinite(maxima[row]) && isfinite(nm) ? {{online_0}} : 0;\n"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "maxima[row]", .new_max = "nm" }, null),
    });

    pub const source_3 = program.render(
        \\        "            float e = isfinite(x) ? {{online_0}} : 0;\n"
        \\
    , .{
        .online_0 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "x", .new_max = "nm" }, null),
    });

    pub const source_4 = program.render(
        \\        "                denom[row] = {{online_0}};\n"
        \\
    , .{
        .online_0 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "denom[row]", .correction = "c", .tile_sum = "sum" }, null),
    });

    pub const source_5 = program.render(
        \\#define TERMITE_MSL_PAGED_GQA_SPLIT_STAGE_TILE \
        \\    "    const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint q_stride = p.num_heads * hd; const uint kv_head_base = kv_h * hd; const uint k_row_halfs = p.key_row_bytes / 2u; const uint query_pos = p.query_position_offset + qi; const float scale = rsqrt(float(hd));\n" \
        \\    "    threadgroup half *sq = (threadgroup half *)shmem; threadgroup char *fb = shmem + 8u * hd * 2u;\n" \
        \\    "    threadgroup float *ss = (threadgroup float *)fb; threadgroup half *sp = (threadgroup half *)(fb + 1024u); threadgroup uint *sphys = (threadgroup uint *)(fb + 1536u);\n" \
        \\    "    threadgroup float *sM = (threadgroup float *)(fb + 1664u); threadgroup float *sS = (threadgroup float *)(fb + 1696u); threadgroup float *sdiag = (threadgroup float *)(fb + 1728u); threadgroup float *so = (threadgroup float *)(fb + 1984u);\n" \
        \\    "    const device half *k_half = reinterpret_cast<const device half *>(encoded_key); const device half *v_half = reinterpret_cast<const device half *>(v_bytes);\n" \
        \\    "    for (uint i = uint(tid); i < 8u * hd; i += 256u) { uint j = i / hd; uint d = i - j * hd; uint h = kv_h * heads_per_group + j; sq[i] = j < heads_per_group ? half(q[qi * q_stride + h * hd + d] * scale) : half(0.0f); }\n" \
        \\    "    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; } if (tid < 64u) sdiag[tid] = 0.0f;\n" \
        \\    "    const uint dslice = hd / 8u; const uint d_tiles = dslice / 8u; simdgroup_float8x8 mo[8]; for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n" \
        \\    "    for (uint kc = split * schedule.key_chunk; kc < p.kv_tokens; kc += split_count * schedule.key_chunk) {\n" \
        \\    "        threadgroup_barrier(mem_flags::mem_threadgroup); if (tid < 32u) { uint ki = kc + uint(tid); sphys[tid] = ki < p.kv_tokens ? termite_attention_page_token(block_table, p, ki) : 0xffffffffu; } threadgroup_barrier(mem_flags::mem_threadgroup);\n" \
        \\    "        if (sgitg < 4u) { uint ktile = uint(sgitg); uint phys = sphys[ktile * 8u]; const device half *kbase = k_half + (phys != 0xffffffffu ? phys : 0u) * k_row_halfs + kv_head_base; simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f); for (uint d = 0u; d < hd; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, hd); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); } simdgroup_store(ms, ss + ktile * 8u, 32u, 0, false); }\n" \
        \\    "        threadgroup_barrier(mem_flags::mem_threadgroup);\n" \
        \\    "        if (sgitg < 4u) { for (uint jj = 0u; jj < 2u; ++jj) { uint j = uint(sgitg) + jj * 4u; uint kk = uint(lane); uint ki = kc + kk; bool allowed = j < heads_per_group && ki < p.kv_tokens && sphys[kk] != 0xffffffffu; if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; } float sc = allowed ? ss[j * 32u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f; float row_max = simd_max(sc); float m_old = sM[j]; float m_new = max(m_old, row_max); float e = 0.0f; float corr = 1.0f; if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? {{online_0}} : 0.0f; e = sc > -3.0e+38f ? {{online_1}} : 0.0f; } sp[j * 32u + kk] = half(e); float row_sum = simd_sum(e); if (lane == 0u) { sS[j] = {{online_2}}; sM[j] = m_new; sdiag[j * 8u + j] = corr; } } }\n" \
        \\    "        threadgroup_barrier(mem_flags::mem_threadgroup); simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u); for (uint dt = 0u; dt < d_tiles; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[dt]); mo[dt] = scaled; }\n" \
        \\    "        for (uint dt = 0u; dt < d_tiles; ++dt) { uint d8 = uint(sgitg) * dslice + dt * 8u; simdgroup_float8x8 acc = mo[dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp + kk8 * 8u, 32u); if (kc + kk8 * 8u + 8u <= p.kv_tokens) { uint phys = sphys[kk8 * 8u]; const device half *vbase = v_half + phys * p.v_row_stride + kv_head_base + d8; simdgroup_load(mv, vbase, p.v_row_stride); } else { threadgroup half *sv = (threadgroup half *)ss + uint(sgitg) * 64u; for (uint vi = uint(lane); vi < 64u; vi += 32u) { uint vr = vi / 8u; uint vc = vi - vr * 8u; uint vphys = sphys[kk8 * 8u + vr]; sv[vi] = vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride + kv_head_base + d8 + vc] : half(0.0f); } simdgroup_barrier(mem_flags::mem_threadgroup); simdgroup_load(mv, sv, 8u); } simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[dt] = acc; }\n" \
        \\    "    }\n" \
        \\    "    device float *partial_stats = reinterpret_cast<device float *>(partial_o + p.q_len * p.num_heads * (hd / 4u) * split_count); if (tid < heads_per_group) { uint h = kv_h * heads_per_group + uint(tid); uint qh = qi * p.num_heads + h; partial_stats[qh * (2u * split_count) + split] = sS[tid]; partial_stats[qh * (2u * split_count) + split_count + split] = sM[tid]; }\n" \
        \\    "    threadgroup float *so_sg = so + uint(sgitg) * 8u * dslice; for (uint dt = 0u; dt < d_tiles; ++dt) simdgroup_store(mo[dt], so_sg + dt * 8u, dslice);\n" \
        \\    "    threadgroup_barrier(mem_flags::mem_threadgroup);\n" \
        \\    "    const uint hd4 = hd / 4u; const uint dslice4 = dslice / 4u; for (uint i = uint(tid); i < heads_per_group * hd4; i += 256u) { uint j = i / hd4; uint d4 = i - j * hd4; uint owner = d4 / dslice4; uint dv = d4 - owner * dslice4; uint h = kv_h * heads_per_group + j; uint qh = qi * p.num_heads + h; uint so_base = owner * 8u * dslice + j * dslice + dv * 4u; partial_o[(qh * hd4 + d4) * split_count + split] = float4(so[so_base], so[so_base + 1u], so[so_base + 2u], so[so_base + 3u]); }\n" \
        \\    "}\n"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "m_old", .new_max = "m_new" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "sc", .new_max = "m_new" }, null),
        .online_2 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "sS[j]", .correction = "corr", .tile_sum = "row_sum" }, null),
    });

    pub const source_6 = program.render(
        \\#define TERMITE_MSL_PAGED_GQA_SPLIT_REDUCE_TILE \
        \\    "    const uint qh = qi * p.num_heads + h; const uint hd4 = hd / 4u; const device float *partial_stats = reinterpret_cast<const device float *>(partial_o + p.q_len * p.num_heads * hd4 * split_count);\n" \
        \\    "    float S = lane < split_count ? partial_stats[qh * (2u * split_count) + uint(lane)] : 0.0f; float M = lane < split_count ? partial_stats[qh * (2u * split_count) + split_count + uint(lane)] : -3.402823466e+38f; float merged_m = simd_max(M); float weight = lane < split_count && S > 0.0f ? {{online_0}} : 0.0f; float denom = simd_sum(S * weight); float inv_denom = denom > 0.0f ? 1.0f / denom : 0.0f;\n" \
        \\    "    for (uint d4 = uint(sgitg); d4 < hd4; d4 += schedule.reduce_simdgroups) { float4 value = lane < split_count ? partial_o[(qh * hd4 + d4) * split_count + uint(lane)] : float4(0.0f); float4 merged = simd_sum(value * weight); if (lane == 0u) output[qh * hd4 + d4] = merged * inv_denom; }\n" \
        \\    "}\n"
        \\
    , .{
        .online_0 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "M", .new_max = "merged_m" }, null),
    });

    pub const source_7 = program.render(
        \\#define ANTFLY_MSL_SEGMENT_SOFTMAX8(Weights) \
        \\    "            float mx = max(x0, x1);\n" \
        \\    "            mx = max(mx, simd_shuffle_xor(mx, 1)); mx = max(mx, simd_shuffle_xor(mx, 2));\n" \
        \\    "            const float mold = mrow[r], mnew = max(mold, mx);\n" \
        \\    "            const float a = mnew == neg ? 1.0f : mold == neg ? 0.0f : {{online_0}};\n" \
        \\    "            const float w0 = x0 == neg ? 0.0f : {{online_1}}, w1 = x1 == neg ? 0.0f : {{online_2}};\n" \
        \\    Weights "            float sum = w0 + w1;\n" \
        \\    "            sum += simd_shuffle_xor(sum, 1); sum += simd_shuffle_xor(sum, 2);\n" \
        \\    "            if (L % 4u == 0u) { lrow[r] = {{online_3}}; mrow[r] = mnew; alpha[r] = a; }\n"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "mold", .new_max = "mnew" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "x0", .new_max = "mnew" }, null),
        .online_2 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "x1", .new_max = "mnew" }, null),
        .online_3 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "lrow[r]", .correction = "a", .tile_sum = "sum" }, null),
    });

    pub const source_8 = program.render(
        \\#define ANTFLY_MSL_VISION64_ONLINE(Passes,Tile) \
        \\    "for (uint pass = 0u; pass < " Passes "; ++pass) { const uint j = uint(sgitg) + pass * 8u; const uint qi = q0 + j; const uint kk = uint(lane); const uint ki = key_start + kk; float sc = qi < p.seq_len && ki < p.seq_len ? ss[j * 32u + kk] : neg_inf; if (!isfinite(sc)) sc = neg_inf; const float row_max = simd_max(sc); const float m_old = sM[j]; const float m_new = max(m_old, row_max); float corr = 0.0f; float e = 0.0f; if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? {{online_0}} : 0.0f; e = sc > -3.0e+38f ? {{online_1}} : 0.0f; } sp[j * 32u + kk] = " Tile "(e); const float row_sum = simd_sum(e); if (lane == 0u) { sS[j] = {{online_2}}; sM[j] = m_new; const uint local_j = j & 7u; sdiag[(j >> 3u) * 64u + local_j * 8u + local_j] = corr; } }"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "m_old", .new_max = "m_new" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "sc", .new_max = "m_new" }, null),
        .online_2 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "sS[j]", .correction = "corr", .tile_sum = "row_sum" }, null),
    });

    pub const source_9 = program.render(
        \\           "    if (v > m) { s = s * {{online_0}} + 1.0f; m = v; } else { s += {{online_1}}; }\n"
        \\
    , .{
        .online_0 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "m", .new_max = "v" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "v", .new_max = "m" }, null),
    });

    pub const source_10 = program.render(
        \\           "    if (m2 > m) { s = s * {{online_0}} + s2; m = m2; } else { s += s2 * {{online_1}}; }\n"
        \\
    , .{
        .online_0 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "m", .new_max = "m2" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "m2", .new_max = "m" }, null),
    });

    pub const source_11 = program.render(
        \\           "        for(uint c=0;c<context;++c){float e={{online_0}};scores[c]=e;sum_exp+=e;}\n"
        \\
    , .{
        .online_0 = online.probability.renderNative(.{ .exp = "exp", .compact = true }, .{ .score = "scores[c]", .new_max = "max_score" }, null),
    });

    pub const source_12 = program.render(
        \\           "            const float a = maximum == -INFINITY ? 0.0f : {{online_0}}, b = {{online_1}};\n"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "maximum", .new_max = "next" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "score", .new_max = "next" }, null),
    });

    pub const source_13 = program.render(
        \\           "            denominator = {{online_0}};\n"
        \\
    , .{
        .online_0 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "denominator", .correction = "a", .tile_sum = "b" }, null),
    });

    pub const source_14 = program.render(
        \\           "        float old_scale[Rows]; for (uint r = 0u; r < Rows; ++r) { const uint row = uint(sgitg) * Rows + r; old_scale[r] = 0.0f; if (active[r]) { const float score = weights[row][uint(lane)]; const float tile_max = simd_max(score); const float next_max = max(running_max[r], tile_max); old_scale[r] = key_start == 0u ? 0.0f : {{online_0}}; const uint key = key_start + uint(lane); const float weight = key < p.seq_len ? {{online_1}} : 0.0f; weights[row][uint(lane)] = weight; denom[r] = {{online_2}}; running_max[r] = next_max; } else { weights[row][uint(lane)] = 0.0f; } }\n"
        \\
    , .{
        .online_0 = online.correction.renderNative(.{ .exp = "exp", .compact = false }, .{ .old_max = "running_max[r]", .new_max = "next_max" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "score", .new_max = "next_max" }, null),
        .online_2 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "denom[r]", .correction = "old_scale[r]", .tile_sum = "simd_sum(weight)" }, null),
    });

    pub const source_15 = program.render(
        \\           "        for (uint r = 0u; r < Rows; ++r) { if (active[r]) { const uint row = uint(sgitg) * Rows + r; float tile_acc0 = 0.0f; float tile_acc1 = 0.0f; for (uint key_local = 0u; key_local < 32u; ++key_local) { const float weight = weights[row][key_local]; tile_acc0 += weight * kv_tile[key_local][uint(lane)]; tile_acc1 += weight * kv_tile[key_local][uint(lane) + 32u]; } acc0[r] = {{online_0}}; acc1[r] = {{online_1}}; } }\n"
        \\
    , .{
        .online_0 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "acc0[r]", .correction = "old_scale[r]", .tile_sum = "tile_acc0" }, null),
        .online_1 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "acc1[r]", .correction = "old_scale[r]", .tile_sum = "tile_acc1" }, null),
    });

    pub const source_16 = program.render(
        \\           "    float partial = 0.0f; for (uint ki = uint(lane); ki < p.seq_len; ki += 32u) { float weight = {{online_0}}; scores[ki] = weight; partial += weight; }\n"
        \\
    , .{
        .online_0 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "scores[ki]", .new_max = "best" }, null),
    });

    pub const source_17 = program.render(
        \\           "    if (lane == 0u) { float denom = 0.0f; for (uint ki = 0u; ki < p.seq_len; ++ki) { const float weight = {{online_0}}; scores[ki] = weight; denom += weight; } scores[p.seq_len] = denom; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
        \\
    , .{
        .online_0 = online.probability.renderNative(.{ .exp = "exp", .compact = false }, .{ .score = "scores[ki]", .new_max = "best" }, null),
    });

    pub const source_18 = program.render(
        \\           "            if (lane == 0u) { uint rb = j >> 3u; uint jr = j & 7u; sS[j] = {{online_0}}; sM[j] = m_new; sdiag[rb * 64u + jr * 8u + jr] = corr; }\n"
        \\
    , .{
        .online_0 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "sS[j]", .correction = "corr", .tile_sum = "row_sum" }, null),
    });
};

pub const cuda = struct {
    pub const source_0 = program.render(
        \\// Copyright 2026 Antfly, Inc.
        \\// SPDX-License-Identifier: Apache-2.0
        \\// Included after native route guards and live device-scalar acquisition; no include guard.
        \\// Decode supplies b=qi=0; prefill supplies its original batch/query indices.
        \\// The caller retains exact shared storage. Online folds and publication are one native phase.
        \\    unsigned int key_start = 0u;
        \\    unsigned int key_end = 0u;
        \\{{key_window}}
        \\
        \\    unsigned int heads_per_group = num_heads / num_kv_heads;
        \\    unsigned int kv_head = head / heads_per_group;
        \\    unsigned int q_hidden = num_heads * head_dim;
        \\    unsigned int kv_hidden = num_kv_heads * head_dim;
        \\    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
        \\    float scale = rsqrtf((float)head_dim);
        \\    if (lane == 0u) {
        \\        shared_max_score = -3.402823466e+38f;
        \\        shared_denom = 0.0f;
        \\    }
        \\    __syncthreads();
        \\
        \\    float acc = 0.0f;
        \\    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        \\        float partial = 0.0f;
        \\        if (lane < head_dim) {
        \\            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
        \\            partial = q[q_base + lane] * k[k_base + lane];
        \\        }
        \\        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        \\        if (lane == 0u) {
        \\            float score = dot * scale;
        \\            float next_max = fmaxf(shared_max_score, score);
        \\            shared_alpha = {{online_0}};
        \\            shared_beta = {{online_1}};
        \\            shared_denom = {{online_2}};
        \\            shared_max_score = next_max;
        \\        }
        \\        __syncthreads();
        \\        if (lane < head_dim) {
        \\            unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + lane;
        \\            acc = {{weighted_output}};
        \\        }
        \\        __syncthreads();
        \\    }
        \\
        \\    if (lane < head_dim) {
        \\        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        \\        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
        \\    }
        \\
    , .{
        .key_window = @import("cuda_phases.zig").attention.visible_key_window,
        .weighted_output = online.mass.renderNative(.{}, .{ .old_sum = "acc", .correction = "shared_alpha", .tile_sum = "shared_beta * v[v_idx]" }, null),
        .online_0 = online.correction.renderNative(.{ .exp = "expf", .compact = false }, .{ .old_max = "shared_max_score", .new_max = "next_max" }, null),
        .online_1 = online.probability.renderNative(.{ .exp = "expf", .compact = false }, .{ .score = "score", .new_max = "next_max" }, null),
        .online_2 = online.mass.renderNative(.{ .compact = false }, .{ .old_sum = "shared_denom", .correction = "shared_alpha", .tile_sum = "shared_beta" }, null),
    });
};
