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

//! Split-K GQA decode attention on Metal: a stage kernel per (query, KV
//! head, split) that runs the 8x8 SIMD-matrix online softmax over every
//! `split_count`-th 32-key chunk and writes split-local (O, S, M), and a
//! reduce kernel per (query, head) that merges the splits with the global
//! stable softmax. Variants differ only in the geometry they accept and in
//! whether their buffers come from kernel arguments or a batch row.

const template = @import("attention_template.zig");
const metalLines = @import("region.zig").metalLines;

pub const Pair = struct {
    stage: []const u8,
    reduce: []const u8,
    /// Buffers and params come from `rows[tg.x]` (a `termite_lighton_batch_row`)
    /// instead of kernel arguments; the query index is then 0.
    batch: bool = false,
    /// Geometry predicates: their declarations, and the expression the guard
    /// requires.
    geometry: []const u8,
    fits: []const u8,
    /// Extra leading guard terms (each ending in ` || `).
    kv_cap: []const u8 = "",
    /// Guard term bounding the query rows.
    q_len: []const u8,
};

const legacy_a4b = Pair{
    .stage = "termite_paged_attention_kv_decode_gqa_split_stage",
    .reduce = "termite_paged_attention_kv_decode_gqa_split_reduce",
    .geometry = "const bool legacy_geometry = p.num_heads == 8u && (p.num_kv_heads == 1u || p.num_kv_heads == 2u) && ((hd == 256u && p.sliding_window == 512u) || (hd == 512u && p.sliding_window == 0u)); const bool a4b_geometry = p.num_heads == 16u && ((p.num_kv_heads == 8u && hd == 256u && p.sliding_window == 1024u) || (p.num_kv_heads == 2u && hd == 512u && p.sliding_window == 0u));",
    .fits = "(!legacy_geometry && !a4b_geometry)",
    .q_len = "p.q_len > 2u",
};

const lightonocr3 = Pair{
    .stage = "termite_lightonocr3_gqa_split_stage",
    .reduce = "termite_lightonocr3_gqa_split_reduce",
    .geometry = "const bool lightonocr3_geometry = p.q_len == 1u && hd == 256u && p.sliding_window == 0u && ((p.num_heads == 8u && p.num_kv_heads == 2u) || (p.num_heads == 16u && p.num_kv_heads == 4u));",
    .fits = "!lightonocr3_geometry",
    .kv_cap = "p.kv_tokens > 2048u || ",
    .q_len = "p.q_len != 1u",
};

const lightonocr3_batch = blk: {
    var pair = lightonocr3;
    pair.stage = "termite_lightonocr3_gqa_batch_stage";
    pair.reduce = "termite_lightonocr3_gqa_batch_reduce";
    pair.batch = true;
    break :blk pair;
};

const stage_comment =
    \\// Stage 1 of small-row split-K GQA attention. The z grid partitions
    \\// context while each TG reuses one KV head across four or eight query heads.
    \\
;
const reduce_comment =
    \\// Stage 2 merges split-local (O,S,M) with the global stable softmax.
    \\// Split is innermost so every SIMD lane reads one coalesced float4.
    \\
;

/// Paged decode (Gemma legacy and A4B geometries) and LightOnOCR-3 decode.
pub const split_region = metalLines(metal_indent, stage_comment ++ kernel(stage, legacy_a4b) ++
    reduce_comment ++ kernel(reduce, legacy_a4b) ++
    "// Qualified OCR global attention reuses the staged SIMD matrix algorithm.\n" ++
    kernel(stage, lightonocr3) ++ reduce_comment ++ kernel(reduce, lightonocr3));

/// LightOnOCR-3 decode over a batch of rows; follows `termite_lighton_batch_row`.
pub const batch_region = metalLines(metal_indent, kernel(stage, lightonocr3_batch) ++ kernel(reduce, lightonocr3_batch));

const metal_indent = @import("region.zig").metal_indent;

fn kernel(comptime tmpl: []const u8, comptime pair: Pair) []const u8 {
    return template.render(tmpl, .{
        .stage = pair.stage,
        .reduce = pair.reduce,
        .batch = pair.batch,
        .row = row,
        .qi = if (pair.batch) "0u" else "tg.x",
        .geometry = pair.geometry,
        .fits = pair.fits,
        .kv_cap = pair.kv_cap,
        .q_len = pair.q_len,
    });
}

const row =
    \\    constant termite_lighton_batch_row &row=rows[tg.x];
    \\    constant termite_metal_paged_attention_params &p=row.params;
    \\    constant termite_metal_decode_gqa_split_params &schedule=row.schedule;
    \\
;

const stage =
    \\kernel void {{stage}}({{#batch}}constant termite_lighton_batch_row *rows [[buffer(0)]], {{/batch}}{{^batch}}device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float4 *partial_o [[buffer(5)]], constant termite_metal_paged_attention_params &p [[buffer(6)]], constant termite_metal_decode_gqa_split_params &schedule [[buffer(7)]], {{/batch}}threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\{{#batch}}{{row}}    device const float *q=row.q; device const uchar *encoded_key=row.k; device const uchar *v_bytes=row.v;
    \\    device const uint *block_table=row.blocks; device float4 *partial_o=row.partial_o;
    \\
    \\{{/batch}}    const uint qi = {{qi}}; const uint kv_h = tg.y; const uint split = tg.z; const uint hd = p.head_dim; const uint split_count = schedule.split_count;
    \\    {{geometry}}
    \\    if (p.kv_tokens == 0u || {{kv_cap}}schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || kv_h >= p.num_kv_heads || split >= split_count || p.q_len == 0u || {{q_len}} || p.format != 3u || p.v_element_bytes != 2u || p.page_size == 0u || (p.page_size & 7u) != 0u || {{fits}} || p.key_row_bytes != 2u * p.num_kv_heads * hd || p.v_row_stride != p.num_kv_heads * hd || p.has_sinks != 0u || p.softcap != 0.0f) return;
    \\    const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint q_stride = p.num_heads * hd; const uint kv_head_base = kv_h * hd; const uint k_row_halfs = p.key_row_bytes / 2u; const uint query_pos = p.query_position_offset + qi; const float scale = rsqrt(float(hd));
    \\    threadgroup half *sq = (threadgroup half *)shmem; threadgroup char *fb = shmem + 8u * hd * 2u;
    \\    threadgroup float *ss = (threadgroup float *)fb; threadgroup half *sp = (threadgroup half *)(fb + 1024u); threadgroup uint *sphys = (threadgroup uint *)(fb + 1536u);
    \\    threadgroup float *sM = (threadgroup float *)(fb + 1664u); threadgroup float *sS = (threadgroup float *)(fb + 1696u); threadgroup float *sdiag = (threadgroup float *)(fb + 1728u); threadgroup float *so = (threadgroup float *)(fb + 1984u);
    \\    const device half *k_half = reinterpret_cast<const device half *>(encoded_key); const device half *v_half = reinterpret_cast<const device half *>(v_bytes);
    \\    for (uint i = uint(tid); i < 8u * hd; i += 256u) { uint j = i / hd; uint d = i - j * hd; uint h = kv_h * heads_per_group + j; sq[i] = j < heads_per_group ? half(q[qi * q_stride + h * hd + d] * scale) : half(0.0f); }
    \\    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; } if (tid < 64u) sdiag[tid] = 0.0f;
    \\    const uint dslice = hd / 8u; const uint d_tiles = dslice / 8u; simdgroup_float8x8 mo[8]; for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
    \\    for (uint kc = split * schedule.key_chunk; kc < p.kv_tokens; kc += split_count * schedule.key_chunk) {
    \\        threadgroup_barrier(mem_flags::mem_threadgroup); if (tid < 32u) { uint ki = kc + uint(tid); sphys[tid] = ki < p.kv_tokens ? termite_attention_page_token(block_table, p, ki) : 0xffffffffu; } threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        if (sgitg < 4u) { uint ktile = uint(sgitg); uint phys = sphys[ktile * 8u]; const device half *kbase = k_half + (phys != 0xffffffffu ? phys : 0u) * k_row_halfs + kv_head_base; simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f); for (uint d = 0u; d < hd; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, hd); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); } simdgroup_store(ms, ss + ktile * 8u, 32u, 0, false); }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        if (sgitg < 4u) { for (uint jj = 0u; jj < 2u; ++jj) { uint j = uint(sgitg) + jj * 4u; uint kk = uint(lane); uint ki = kc + kk; bool allowed = j < heads_per_group && ki < p.kv_tokens && sphys[kk] != 0xffffffffu; if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; } float sc = allowed ? ss[j * 32u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f; float row_max = simd_max(sc); float m_old = sM[j]; float m_new = max(m_old, row_max); float e = 0.0f; float corr = 1.0f; if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; e = sc > -3.0e+38f ? exp(sc - m_new) : 0.0f; } sp[j * 32u + kk] = half(e); float row_sum = simd_sum(e); if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; } } }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup); simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u); for (uint dt = 0u; dt < d_tiles; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[dt]); mo[dt] = scaled; }
    \\        for (uint dt = 0u; dt < d_tiles; ++dt) { uint d8 = uint(sgitg) * dslice + dt * 8u; simdgroup_float8x8 acc = mo[dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp + kk8 * 8u, 32u); if (kc + kk8 * 8u + 8u <= p.kv_tokens) { uint phys = sphys[kk8 * 8u]; const device half *vbase = v_half + phys * p.v_row_stride + kv_head_base + d8; simdgroup_load(mv, vbase, p.v_row_stride); } else { threadgroup half *sv = (threadgroup half *)ss + uint(sgitg) * 64u; for (uint vi = uint(lane); vi < 64u; vi += 32u) { uint vr = vi / 8u; uint vc = vi - vr * 8u; uint vphys = sphys[kk8 * 8u + vr]; sv[vi] = vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride + kv_head_base + d8 + vc] : half(0.0f); } simdgroup_barrier(mem_flags::mem_threadgroup); simdgroup_load(mv, sv, 8u); } simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[dt] = acc; }
    \\    }
    \\    device float *partial_stats = reinterpret_cast<device float *>(partial_o + p.q_len * p.num_heads * (hd / 4u) * split_count); if (tid < heads_per_group) { uint h = kv_h * heads_per_group + uint(tid); uint qh = qi * p.num_heads + h; partial_stats[qh * (2u * split_count) + split] = sS[tid]; partial_stats[qh * (2u * split_count) + split_count + split] = sM[tid]; }
    \\    threadgroup float *so_sg = so + uint(sgitg) * 8u * dslice; for (uint dt = 0u; dt < d_tiles; ++dt) simdgroup_store(mo[dt], so_sg + dt * 8u, dslice);
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    const uint hd4 = hd / 4u; const uint dslice4 = dslice / 4u; for (uint i = uint(tid); i < heads_per_group * hd4; i += 256u) { uint j = i / hd4; uint d4 = i - j * hd4; uint owner = d4 / dslice4; uint dv = d4 - owner * dslice4; uint h = kv_h * heads_per_group + j; uint qh = qi * p.num_heads + h; uint so_base = owner * 8u * dslice + j * dslice + dv * 4u; partial_o[(qh * hd4 + d4) * split_count + split] = float4(so[so_base], so[so_base + 1u], so[so_base + 2u], so[so_base + 3u]); }
    \\}
    \\
;

const reduce =
    \\kernel void {{reduce}}({{#batch}}constant termite_lighton_batch_row *rows [[buffer(0)]], {{/batch}}{{^batch}}device const float4 *partial_o [[buffer(0)]], device float4 *output [[buffer(1)]], constant termite_metal_paged_attention_params &p [[buffer(2)]], constant termite_metal_decode_gqa_split_params &schedule [[buffer(3)]], {{/batch}}ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
    \\{{#batch}}{{row}}    device const float4 *partial_o=row.partial_o; device float4 *output=row.output;
    \\
    \\{{/batch}}    const uint qi = {{qi}}; const uint h = tg.y; const uint hd = p.head_dim; const uint split_count = schedule.split_count; {{geometry}} if (p.kv_tokens == 0u || {{kv_cap}}schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || h >= p.num_heads || p.q_len == 0u || {{q_len}} || {{fits}}) return;
    \\    const uint qh = qi * p.num_heads + h; const uint hd4 = hd / 4u; const device float *partial_stats = reinterpret_cast<const device float *>(partial_o + p.q_len * p.num_heads * hd4 * split_count);
    \\    float S = lane < split_count ? partial_stats[qh * (2u * split_count) + uint(lane)] : 0.0f; float M = lane < split_count ? partial_stats[qh * (2u * split_count) + split_count + uint(lane)] : -3.402823466e+38f; float merged_m = simd_max(M); float weight = lane < split_count && S > 0.0f ? exp(M - merged_m) : 0.0f; float denom = simd_sum(S * weight); float inv_denom = denom > 0.0f ? 1.0f / denom : 0.0f;
    \\    for (uint d4 = uint(sgitg); d4 < hd4; d4 += schedule.reduce_simdgroups) { float4 value = lane < split_count ? partial_o[(qh * hd4 + d4) * split_count + uint(lane)] : float4(0.0f); float4 merged = simd_sum(value * weight); if (lane == 0u) output[qh * hd4 + d4] = merged * inv_denom; }
    \\}
    \\
;
