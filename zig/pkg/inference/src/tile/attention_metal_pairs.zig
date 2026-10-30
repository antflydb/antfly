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

//! Metal attention kernels generated in pairs: each family keeps one copy of
//! the kernel text and marks where its two variants differ (see
//! attention_template.zig). The text equals the hand-written kernels it
//! replaced, so their tuned performance is unchanged.

const template = @import("attention_template.zig");

/// Dense causal prefill with simdgroup matrices: 8-query tiles over 32-key
/// chunks with an online softmax, f32 K/V staged as f16. `q16` runs two
/// 8-row blocks per simdgroup (head_dim <= 128).
pub const dense_causal_sg = struct {
    pub fn kernel(comptime name: []const u8, comptime q16: bool) []const u8 {
        return template.metal(template.render(text, .{ .name = name, .q16 = q16 }));
    }
    const text =
        \\// {{#q16}}16-query variant: halves {{/q16}}{{^q16}}Dense contiguous-{{/q16}}K/V {{#q16}}traffic per token versus the 8-query{{/q16}}{{^q16}}port of termite_paged_attention_kv_prefill_sg:{{/q16}}
        \\//{{#q16}} tile. Two 8-row simdgroup blocks per SG cap head_dim at 128 (8
        \\// accumulators); the dispatcher falls back to the{{/q16}} 8-query {{#q16}}kernel{{/q16}}{{^q16}}tiles x 32-key chunks with online softmax and simdgroup{{/q16}}
        \\// {{#q16}}for larger heads or shorter tiles{{/q16}}{{^q16}}matmuls, staging f32 K/V to f16 in threadgroup memory. Constant
        \\// threadgroup footprint, so no kv-length ceiling. 128 threads/TG{{/q16}}.
        \\kernel void {{name}}(device const float *q [[buffer(0)]], device const float *k [[buffer(1)]], device const float *v [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_attention_f32_params &p [[buffer(4)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
        \\    const uint hd = p.head_dim; const uint q0 = tg.x * {{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}}; const uint h = tg.y;
        \\    if (q0 >= p.q_len || h >= p.num_heads || p.num_kv_heads == 0u || hd % 32u != 0u{{#q16}} || hd > 128u{{/q16}} || p.has_bias != 0u || p.has_mask != 0u) return;
        \\    const float scale = rsqrt(float(hd)); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * hd;
        \\    const uint q_stride = p.num_heads * hd; const uint kv_stride = p.num_kv_heads * hd;
        \\    threadgroup half *sq = (threadgroup half *)shmem;
        \\    threadgroup half *skv = sq + {{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}} * hd;
        \\    threadgroup char *fb = shmem + ({{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}} + 32u) * hd * 2u;
        \\    threadgroup float *ss = (threadgroup float *)fb;
        \\    threadgroup half *sp = (threadgroup half *)(fb + {{#q16}}2048u{{/q16}}{{^q16}}1024u{{/q16}});
        \\    threadgroup float *sM = (threadgroup float *)(fb + {{#q16}}3072u{{/q16}}{{^q16}}1536u{{/q16}});
        \\    threadgroup float *sS = (threadgroup float *)(fb + {{#q16}}3136u{{/q16}}{{^q16}}1568u{{/q16}});
        \\    threadgroup float *sdiag = (threadgroup float *)(fb + {{#q16}}3200u{{/q16}}{{^q16}}1600u{{/q16}});
        \\    for (uint i = uint(tid); i < {{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}} * hd; i += 128u) { uint j = i / hd; uint d = i - j * hd; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + h * hd + d] * scale) : half(0.0f); }
        \\    if (tid < {{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}}) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; }
        \\    if (tid < {{#q16}}128u{{/q16}}{{^q16}}64u{{/q16}}) sdiag[tid] = 0.0f;
        \\    const uint dslice = uint(sgitg) * (hd / 4u);
        \\    simdgroup_float8x8 mo[8];
        \\    for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
        \\    const uint d_tiles = hd / 32u;
        \\    const uint last_query_pos = p.query_position_offset + min(q0 + {{#q16}}15u{{/q16}}{{^q16}}7u{{/q16}}, p.q_len - 1u);
        \\    for (uint kc = 0u; kc < p.kv_len; kc += 32u) {
        \\        if (p.kv_position_offset + kc > last_query_pos) break;
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint i = uint(tid); i < 32u * hd; i += 128u) { uint kk = i / hd; uint d = i - kk * hd; uint ki = kc + kk; skv[i] = ki < p.kv_len ? half(k[ki * kv_stride + kv_head_base + d]) : half(0.0f); }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        {{#q16}}for (uint rb = 0u; rb < 2u; ++rb) {
        \\            {{/q16}}simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f);{{#q16}}
        \\            {{/q16}}{{^q16}}
        \\        {{/q16}}for (uint d = 0u; d < hd; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq{{#q16}} + rb * 8u * hd{{/q16}} + d, hd); simdgroup_load(mk, skv + uint(sgitg) * 8u * hd + d, hd, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); }{{#q16}}
        \\            {{/q16}}{{^q16}}
        \\        {{/q16}}simdgroup_store(ms, ss{{#q16}} + rb * 256u{{/q16}} + uint(sgitg) * 8u, 32u, 0, false);{{#q16}}
        \\        }{{/q16}}
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint jj = 0u; jj < {{#q16}}4u{{/q16}}{{^q16}}2u{{/q16}}; ++jj) {
        \\            uint j = uint(sgitg) + jj * 4u; uint qi = q0 + j; uint query_pos = p.query_position_offset + qi; uint kk = uint(lane); uint ki = kc + kk;
        \\            bool allowed = ki < p.kv_len && qi < p.q_len;
        \\            if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; }
        \\            float sc = allowed ? ss[j * 32u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f;
        \\            float row_max = simd_max(sc); float m_old = sM[j]; float m_new = max(m_old, row_max);
        \\            float e = 0.0f; float corr = 1.0f;
        \\            if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; e = sc > -3.0e+38f ? exp(sc - m_new) : 0.0f; }
        \\            sp[j * 32u + kk] = half(e);
        \\            float row_sum = simd_sum(e);
        \\            if (lane == 0u) { {{#q16}}uint rb = j >> 3u; uint jr = j & 7u; {{/q16}}sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[{{#q16}}rb * 64u + jr{{/q16}}{{^q16}}j{{/q16}} * 8u + {{#q16}}jr{{/q16}}{{^q16}}j{{/q16}}] = corr; }
        \\        }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        {{#q16}}for (uint rb = 0u; rb < 2u; ++rb) { {{/q16}}simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag{{#q16}} + rb * 64u{{/q16}}, 8u);{{#q16}} {{/q16}}{{^q16}}
        \\        {{/q16}}for (uint i = 0u; i < d_tiles; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[{{#q16}}rb * d_tiles + {{/q16}}i]); mo[{{#q16}}rb * d_tiles + {{/q16}}i] = scaled;{{#q16}} }{{/q16}} }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint i = uint(tid); i < 32u * hd; i += 128u) { uint kk = i / hd; uint d = i - kk * hd; uint ki = kc + kk; skv[i] = ki < p.kv_len ? half(v[ki * kv_stride + kv_head_base + d]) : half(0.0f); }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint {{#q16}}rb = 0u; rb < 2u; ++rb) { for (uint {{/q16}}dt = 0u; dt < d_tiles; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_float8x8 acc = mo[{{#q16}}rb * d_tiles + {{/q16}}dt]; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { simdgroup_half8x8 mp; simdgroup_half8x8 mv; simdgroup_load(mp, sp{{#q16}} + rb * 256u{{/q16}} + kk8 * 8u, 32u); simdgroup_load(mv, skv + kk8 * 8u * hd + d8, hd); simdgroup_multiply_accumulate(acc, mp, mv, acc); } mo[{{#q16}}rb * d_tiles + {{/q16}}dt] = acc;{{#q16}} }{{/q16}} }
        \\    }
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (tid < {{#q16}}16u) { uint rb = uint(tid) >> 3u; uint jr = uint(tid) & 7u;{{/q16}}{{^q16}}8u) {{{/q16}} float denom = sS[tid]; sdiag[{{#q16}}rb * 64u + jr{{/q16}}{{^q16}}uint(tid){{/q16}} * 8u + {{#q16}}jr{{/q16}}{{^q16}}uint(tid){{/q16}}] = denom > 0.0f ? 1.0f / denom : 0.0f; }
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    {{^q16}}simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u);
        \\    {{/q16}}threadgroup float *so = (threadgroup float *){{#q16}}shmem{{/q16}}{{^q16}}skv{{/q16}};
        \\    {{#q16}}for (uint rb = 0u; rb < 2u; ++rb) { simdgroup_float8x8 minv; simdgroup_load(minv, sdiag + rb * 64u, 8u); {{/q16}}for (uint dt = 0u; dt < d_tiles; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[{{#q16}}rb * d_tiles + {{/q16}}dt]); simdgroup_store(scaled, so + uint(sgitg) * ({{#q16}}16u * (hd / 4u)) + rb * {{/q16}}8u * (hd / 4u{{^q16}}){{/q16}}) + dt * 8u, hd / 4u, 0, false);{{#q16}} }{{/q16}} }
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint i = uint(tid); i < {{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}} * hd; i += 128u) { uint j = i / hd; uint d = i - j * hd; uint qi = q0 + j; if (qi >= p.q_len) continue; uint sg_of_d = d / (hd / 4u); uint d_in = d - sg_of_d * (hd / 4u); output[qi * q_stride + h * hd + d] = so[sg_of_d * ({{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}} * (hd / 4u)) + j * (hd / 4u) + d_in]; }
        \\}
        \\
    ;
};

/// Qwen3-VL vision FlashAttention, head_dim 64, 32 queries per threadgroup.
/// `full` keeps f32 throughout and reuses Q storage for the output tail.
pub const vision_flash_q32 = struct {
    pub fn kernel(comptime name: []const u8, comptime full: bool) []const u8 {
        return template.metal(template.render(text, .{ .name = name, .full = full }));
    }
    const text =
        \\// {{#full}}Full F32 variant for retrieval-sensitive vision. Reuse Q storage{{/full}}{{^full}}Q32 version of the dense vision FlashAttention tile. Each SIMD{{/full}}
        \\// {{#full}}for the output tail after the final attention barrier: 25.25 KiB{{/full}}{{^full}}group owns the same 16-column output slice for two eight-query{{/full}}
        \\// {{#full}}of threadgroup memory rather than 33.25 KiB with a separate tail{{/full}}{{^full}}groups, halving repeated K/V loads relative to Q16 while staying
        \\// below the Apple M4 threadgroup-memory limit{{/full}}.
        \\kernel void {{name}}(device const float *q [[buffer(0)]], device const float *k [[buffer(1)]], device const float *v [[buffer(2)]], device const float *bias [[buffer(3)]], device const float *mask [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_sdpa_f32_params &p [[buffer(6)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
        \\    (void)bias; (void)mask; if (p.seq_len == 0u || p.head_dim != 64u || p.bias_mode != 0u || p.has_mask != 0u || p.layout != 1u || sgitg >= 8u) return;
        \\    const uint query_tile_count = (p.seq_len + 31u) / 32u; const uint query_tile = tg.x % query_tile_count; const uint tmp = tg.x / query_tile_count; const uint h = tmp % p.num_heads; const uint b = tmp / p.num_heads; if (b >= p.batch) return;
        \\    const uint q0 = query_tile * 32u; const uint hidden = p.num_heads * 64u; const uint qgroup_base = uint(sgitg) >> 2u; const uint subgroup = uint(sgitg) & 3u; const uint dslice = subgroup * 16u; const float neg_inf = -3.402823466e+38f;
        \\    threadgroup {{#full}}float{{/full}}{{^full}}half{{/full}} sq[2048]; threadgroup {{#full}}float{{/full}}{{^full}}half{{/full}} skv[2048]; threadgroup float ss[1024]; threadgroup {{#full}}float{{/full}}{{^full}}half{{/full}} sp[1024]; threadgroup float sM[32]; threadgroup float sS[32]; threadgroup float sdiag[256];{{#full}} 
        \\    {{/full}}{{^full}} threadgroup float so[2048];
        \\    {{/full}}for (uint i = uint(tid); i < 2048u; i += 256u) { const uint qr = i >> 6u; const uint d = i & 63u; const uint qi = q0 + qr; sq[i] = qi < p.seq_len ? {{#full}}float{{/full}}{{^full}}half{{/full}}(q[(b * p.seq_len + qi) * hidden + h * 64u + d] * 0.125f) : {{#full}}float{{/full}}{{^full}}half{{/full}}(0.0f); }
        \\    if (tid < 32u) { sM[tid] = neg_inf; sS[tid] = 0.0f; } if (tid < 256u) sdiag[tid] = 0.0f; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    simdgroup_float8x8 mo[2][2]; for (uint qp = 0u; qp < 2u; ++qp) for (uint dt = 0u; dt < 2u; ++dt) mo[qp][dt] = make_filled_simdgroup_matrix<float, 8>(0.0f);
        \\    for (uint key_start = 0u; key_start < p.seq_len; key_start += 32u) {
        \\        for (uint i = uint(tid); i < 2048u; i += 256u) { const uint kr = i >> 6u; const uint d = i & 63u; const uint ki = key_start + kr; skv[i] = ki < p.seq_len ? {{#full}}float{{/full}}{{^full}}half{{/full}}(k[(b * p.seq_len + ki) * hidden + h * 64u + d]) : {{#full}}float{{/full}}{{^full}}half{{/full}}(0.0f); }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint qp = 0u; qp < 2u; ++qp) { const uint qgroup = qgroup_base + qp * 2u; simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f); for (uint d = 0u; d < 64u; d += 8u) { {{#full}}simdgroup_float8x8{{/full}}{{^full}}simdgroup_half8x8{{/full}} mq; {{#full}}simdgroup_float8x8{{/full}}{{^full}}simdgroup_half8x8{{/full}} mk; simdgroup_load(mq, sq + qgroup * 512u + d, 64u); simdgroup_load(mk, skv + subgroup * 512u + d, 64u, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); } simdgroup_store(ms, ss + qgroup * 256u + subgroup * 8u, 32u); }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint pass = 0u; pass < 4u; ++pass) { const uint j = uint(sgitg) + pass * 8u; const uint qi = q0 + j; const uint kk = uint(lane); const uint ki = key_start + kk; float sc = qi < p.seq_len && ki < p.seq_len ? ss[j * 32u + kk] : neg_inf; if (!isfinite(sc)) sc = neg_inf; const float row_max = simd_max(sc); const float m_old = sM[j]; const float m_new = max(m_old, row_max); float corr = 0.0f; float e = 0.0f; if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; e = sc > -3.0e+38f ? exp(sc - m_new) : 0.0f; } sp[j * 32u + kk] = {{#full}}float{{/full}}{{^full}}half{{/full}}(e); const float row_sum = simd_sum(e); if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; const uint local_j = j & 7u; sdiag[(j >> 3u) * 64u + local_j * 8u + local_j] = corr; } }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint qp = 0u; qp < 2u; ++qp) { const uint qgroup = qgroup_base + qp * 2u; simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag + qgroup * 64u, 8u); for (uint dt = 0u; dt < 2u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[qp][dt]); mo[qp][dt] = scaled; } }
        \\        for (uint i = uint(tid); i < 2048u; i += 256u) { const uint kr = i >> 6u; const uint d = i & 63u; const uint ki = key_start + kr; skv[i] = ki < p.seq_len ? {{#full}}float{{/full}}{{^full}}half{{/full}}(v[(b * p.seq_len + ki) * hidden + h * 64u + d]) : {{#full}}float{{/full}}{{^full}}half{{/full}}(0.0f); }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        for (uint qp = 0u; qp < 2u; ++qp) { const uint qgroup = qgroup_base + qp * 2u; for (uint kk8 = 0u; kk8 < 4u; ++kk8) { {{#full}}simdgroup_float8x8{{/full}}{{^full}}simdgroup_half8x8{{/full}} mp; simdgroup_load(mp, sp + qgroup * 256u + kk8 * 8u, 32u); for (uint dt = 0u; dt < 2u; ++dt) { {{#full}}simdgroup_float8x8{{/full}}{{^full}}simdgroup_half8x8{{/full}} mv; simdgroup_load(mv, skv + kk8 * 512u + dslice + dt * 8u, 64u); simdgroup_multiply_accumulate(mo[qp][dt], mp, mv, mo[qp][dt]); } } }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    }
        \\    if (tid < 32u) { const uint local_j = uint(tid) & 7u; const float denom = sS[tid]; sdiag[(uint(tid) >> 3u) * 64u + local_j * 8u + local_j] = denom > 0.0f ? 1.0f / denom : 0.0f; } threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint qp = 0u; qp < 2u; ++qp) { const uint qgroup = qgroup_base + qp * 2u; simdgroup_float8x8 minv; simdgroup_load(minv, sdiag + qgroup * 64u, 8u); for (uint dt = 0u; dt < 2u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[qp][dt]); mo[qp][dt] = scaled; } }
        \\    if (q0 + 32u <= p.seq_len) { for (uint qp = 0u; qp < 2u; ++qp) { const uint qgroup = qgroup_base + qp * 2u; const uint output_base = (b * p.seq_len + q0 + qgroup * 8u) * hidden + h * 64u + dslice; for (uint dt = 0u; dt < 2u; ++dt) simdgroup_store(mo[qp][dt], output + output_base + dt * 8u, hidden); } return; }
        \\    for (uint qp = 0u; qp < 2u; ++qp) for (uint dt = 0u; dt < 2u; ++dt) simdgroup_store(mo[qp][dt], {{#full}}sq{{/full}}{{^full}}so{{/full}} + (uint(sgitg) + qp * 8u) * 128u + dt * 8u, 16u); threadgroup_barrier(mem_flags::mem_threadgroup); const uint valid_rows = p.seq_len - q0; for (uint i = uint(tid); i < valid_rows * 64u; i += 256u) { const uint qr = i >> 6u; const uint d = i & 63u; const uint owner = (qr >> 3u) * 4u + (d >> 4u); const uint local_r = qr & 7u; const uint local_d = d & 15u; output[(b * p.seq_len + q0 + qr) * hidden + h * 64u + d] = {{#full}}sq{{/full}}{{^full}}so{{/full}}[owner * 128u + local_r * 16u + local_d]; }
        \\}
        \\
    ;
};

/// Florence-2 window attention, head_dim 32, one SIMD group per query.
/// `parallel` sums the softmax denominator across the lanes instead of on
/// lane 0.
pub const florence_window = struct {
    pub fn kernel(comptime name: []const u8, comptime parallel: bool) []const u8 {
        return template.metal(template.render(text, .{ .name = name, .parallel = parallel }));
    }
    const text =
        \\kernel void {{name}}(device const float *q [[buffer(0)]], device const float *k [[buffer(1)]], device const float *v [[buffer(2)]], device const float *bias [[buffer(3)]], device const float *mask [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_sdpa_f32_params &p [[buffer(6)]], threadgroup float *scores [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], uint tg [[threadgroup_position_in_grid]]) {
        \\    (void)bias; (void)mask; if (p.seq_len == 0u || p.seq_len > 256u || p.head_dim != 32u || p.bias_mode != 0u || p.has_mask != 0u) return;
        \\    const uint qi = tg % p.seq_len; const uint tmp = tg / p.seq_len; const uint h = tmp % p.num_heads; const uint b = tmp / p.num_heads; if (b >= p.batch) return;
        \\    const uint hidden = p.num_heads * 32u; const uint bh = b * p.num_heads + h; const uint q_base = p.layout == 1u ? (b * p.seq_len + qi) * hidden + h * 32u : (bh * p.seq_len + qi) * 32u; const float scale = rsqrt(32.0f); const float neg_inf = -3.402823466e+38f; float local_best = neg_inf;
        \\    for (uint ki = uint(lane); ki < p.seq_len; ki += 32u) { const uint k_base = p.layout == 1u ? (b * p.seq_len + ki) * hidden + h * 32u : (bh * p.seq_len + ki) * 32u; float dot = 0.0f; for (uint d = 0u; d < 32u; ++d) dot += q[q_base + d] * k[k_base + d]; const float score = dot * scale; scores[ki] = score; local_best = max(local_best, score); }
        \\    const float best = simd_max(local_best); threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    {{#parallel}}float partial{{/parallel}}{{^parallel}}if (lane == 0u) { float denom{{/parallel}} = 0.0f; for (uint ki = {{#parallel}}uint(lane){{/parallel}}{{^parallel}}0u{{/parallel}}; ki < p.seq_len; {{#parallel}}ki += 32u) {{{/parallel}}{{^parallel}}++ki) { const{{/parallel}} float weight = exp(scores[ki] - best); scores[ki] = weight; {{#parallel}}partial{{/parallel}}{{^parallel}}denom{{/parallel}} += weight; }{{^parallel}} scores[p.seq_len] = denom; } threadgroup_barrier(mem_flags::mem_threadgroup);{{/parallel}}
        \\    const float denom = {{#parallel}}simd_sum(partial); threadgroup_barrier(mem_flags::mem_threadgroup){{/parallel}}{{^parallel}}scores[p.seq_len]{{/parallel}}; float accum = 0.0f; for (uint ki = 0u; ki < p.seq_len; ++ki) { const uint v_base = p.layout == 1u ? (b * p.seq_len + ki) * hidden + h * 32u : (bh * p.seq_len + ki) * 32u; accum += scores[ki] * v[v_base + uint(lane)]; } output[q_base + uint(lane)] = denom > 0.0f ? accum / denom : 0.0f;
        \\}
        \\
    ;
};

/// Copies suffix K/V rows into their paged cache slots; `half_cache` stores
/// the cache as half.
pub const kv_seed = struct {
    pub fn kernel(comptime name: []const u8, comptime half_cache: bool) []const u8 {
        return template.metal(template.render(text, .{ .name = name, .half_cache = half_cache }));
    }
    const text =
        \\kernel void {{name}}(device const float *src_k [[buffer(0)]], device const float *src_v [[buffer(1)]], device {{#half_cache}}half{{/half_cache}}{{^half_cache}}float{{/half_cache}} *dst_k [[buffer(2)]], device {{#half_cache}}half{{/half_cache}}{{^half_cache}}float{{/half_cache}} *dst_v [[buffer(3)]], device const uint *block_table [[buffer(4)]], constant termite_metal_paged_f32_kv_seed_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {
        \\    uint values = p.values_per_row; uint width = max(values, p.v_row_stride); if (values == 0u || width == 0u) return; uint row = gid / width; uint col = gid - row * width; if (row >= p.suffix_tokens) return;
        \\    uint logical = p.total_tokens - p.suffix_tokens + row; uint logical_block = logical / p.page_size; uint token_in_block = logical - logical_block * p.page_size; uint physical = block_table[logical_block] + token_in_block;
        \\    if (col < values) dst_k[physical * p.key_row_floats + col] = {{#half_cache}}half({{/half_cache}}src_k[row * values + col]{{#half_cache}}){{/half_cache}};
        \\    if (col < p.v_row_stride) dst_v[physical * p.v_row_stride + col] = {{#half_cache}}half({{/half_cache}}src_v[row * p.v_row_stride + col]{{#half_cache}}){{/half_cache}};
        \\}
        \\
    ;
};

/// Splits a packed Q/K/V projection into its three outputs; `bias` adds the
/// projection bias.
pub const qkv_split = struct {
    pub fn kernel(comptime name: []const u8, comptime bias: bool) []const u8 {
        return template.metal(template.render(text, .{ .name = name, .bias = bias }));
    }
    const text =
        \\kernel void {{name}}(device const float *packed [[buffer(0)]], device {{#bias}}const float *q_bias [[buffer(1)]], device const float *k_bias [[buffer(2)]], device const float *v_bias [[buffer(3)]], device {{/bias}}float *q_out [[buffer({{#bias}}4{{/bias}}{{^bias}}1{{/bias}})]], device float *k_out [[buffer({{#bias}}5{{/bias}}{{^bias}}2{{/bias}})]], device float *v_out [[buffer({{#bias}}6{{/bias}}{{^bias}}3{{/bias}})]], constant termite_metal_dense_qkv_split_bias_params &p [[buffer({{#bias}}7{{/bias}}{{^bias}}4{{/bias}})]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.rows * p.total_out_dim; if (gid >= total || p.total_out_dim == 0u) return; uint row = gid / p.total_out_dim; uint col = gid - row * p.total_out_dim; float value = packed[gid];
        \\    if (col < p.q_out_dim) { q_out[row * p.q_out_dim + col] = value{{#bias}} + q_bias[col]{{/bias}}; return; }
        \\    uint k_col = col - p.q_out_dim; if (k_col < p.kv_out_dim) { k_out[row * p.kv_out_dim + k_col] = value{{#bias}} + k_bias[k_col]{{/bias}}; return; }
        \\    uint v_col = k_col - p.kv_out_dim; if (v_col < p.kv_out_dim) v_out[row * p.kv_out_dim + v_col] = value{{#bias}} + v_bias[v_col]{{/bias}};
        \\}
        \\
    ;
};
