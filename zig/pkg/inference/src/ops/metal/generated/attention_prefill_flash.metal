// Copyright 2026 Antfly, Inc.
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

// Generated Metal attention artifact from graph/quant_kernel_compiler.zig.
// plan_id=metal/attention/prefill_flash
// kernel_id=antfly_paged_attention_prefill_flash_generated_msl_v1
// production_baseline=termite_paged_attention_kv_prefill_sg
// production_enabled=true
// Descriptor-driven low-memory simdgroup-MMA flash prefill attention.
// The key_chunk=32/skip_rescale=false baseline preserves the hand-written
// arithmetic order while using page-local K/V loads and direct output stores.
// Production defaults to this route only for Gemma4 E4B local attention;
// capability checks and the handwritten fallback remain authoritative.

#include <metal_stdlib>
using namespace metal;

struct antfly_paged_attention_1x_params { uint q_len; uint kv_tokens; uint num_heads; uint num_kv_heads; uint head_dim; uint key_row_bytes; uint base_key_row_bytes; uint query_position_offset; uint kv_position_offset; uint sliding_window; uint v_row_stride; uint page_size; uint block_count; uint contiguous_base_token; uint contiguous_blocks; uint format; uint v_element_bytes; uint has_sinks; float softcap; uint swa_scan_clamp; };
inline uint antfly_paged_attention_1x_page_token(device const uint *block_table, constant antfly_paged_attention_1x_params &p, uint logical_token) { if (p.contiguous_blocks != 0u) return p.contiguous_base_token + logical_token; uint logical_block = logical_token / p.page_size; uint token_in_block = logical_token - logical_block * p.page_size; if (logical_block >= p.block_count) return 0xffffffffu; return block_table[logical_block] + token_in_block; }
kernel void antfly_paged_attention_prefill_flash_generated_msl_v1(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
    const uint q0 = tg.x * 8u; const uint h = tg.y;
    if (q0 >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || (p.page_size & 7u) != 0u || p.num_kv_heads == 0u || p.num_heads % p.num_kv_heads != 0u || p.head_dim != 256u) return;
    const float scale = rsqrt(256.0f); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * 256u;
    const uint q_stride = p.num_heads * 256u; const uint k_row_halfs = p.key_row_bytes / 2u;
    threadgroup half *sq = (threadgroup half *)shmem;
    threadgroup char *fb = shmem + 8u * 256u * 2u;
    threadgroup float *ss = (threadgroup float *)fb;
    threadgroup half *sp = (threadgroup half *)(fb + 1024u);
    threadgroup uint *sphys = (threadgroup uint *)(fb + 1536u);
    threadgroup float *sM = (threadgroup float *)(fb + 1664u);
    threadgroup float *sS = (threadgroup float *)(fb + 1696u);
    threadgroup float *sdiag = (threadgroup float *)(fb + 1760u);
    threadgroup float *so = (threadgroup float *)(fb + 2016u);
    const device half *v_half = reinterpret_cast<const device half *>(v_bytes);
    const device half *k_half = reinterpret_cast<const device half *>(encoded_key);
    for (uint i = uint(tid); i < 8u * 256u; i += 128u) { uint j = i / 256u; uint d = i - j * 256u; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + h * 256u + d] * scale) : half(0.0f); }
    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; }
    if (tid < 64u) sdiag[tid] = 0.0f;
    const uint dslice = uint(sgitg) * 64u;
    simdgroup_float8x8 mo[8];
    for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
    const uint q_last = min(q0 + 7u, p.q_len - 1u); const uint tile_first_q = p.query_position_offset + q0; const uint tile_last_q = p.query_position_offset + q_last;
    uint kc_start = 0u; if (p.swa_scan_clamp != 0u && p.sliding_window != 0u) { const uint window_minus_one = p.sliding_window - 1u; const uint earliest_live_key = tile_first_q >= window_minus_one ? tile_first_q - window_minus_one : 0u; if (earliest_live_key > p.kv_position_offset) { const uint earliest_logical_key = earliest_live_key - p.kv_position_offset; kc_start = (earliest_logical_key / 32u) * 32u; } }
    for (uint kc = kc_start; kc < p.kv_tokens; kc += 32u) {
        threadgroup_barrier(mem_flags::mem_threadgroup);
        uint key_first = p.kv_position_offset + kc; uint key_last = p.kv_position_offset + min(kc + 31u, p.kv_tokens - 1u); bool wholly_future = key_first > tile_last_q; bool wholly_expired = p.sliding_window != 0u && tile_first_q >= key_last && tile_first_q - key_last >= p.sliding_window; if (wholly_future) break; if (wholly_expired) continue;
        if (tid < 32u) { uint ki = kc + uint(tid); sphys[tid] = ki < p.kv_tokens ? antfly_paged_attention_1x_page_token(block_table, p, ki) : 0xffffffffu; }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f);
        uint phys = sphys[uint(sgitg) * 8u]; const device half *kbase = k_half + (phys != 0xffffffffu ? phys : 0u) * k_row_halfs + kv_head_base; for (uint d = 0u; d < 256u; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, 256u); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); }
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
        for (uint i = 0u; i < 8u; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; }
        for (uint kk8 = 0u; kk8 < 4u; ++kk8) { uint phys = sphys[kk8 * 8u]; simdgroup_half8x8 mp; simdgroup_load(mp, sp + kk8 * 8u, 32u); for (uint dt = 0u; dt < 8u; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_half8x8 mv; if (kc + kk8 * 8u + 8u <= p.kv_tokens) { const device half *vbase = v_half + phys * p.v_row_stride + kv_head_base + d8; simdgroup_load(mv, vbase, p.v_row_stride); } else { threadgroup half *sv = (threadgroup half *)ss + uint(sgitg) * 64u; for (uint vi = uint(lane); vi < 64u; vi += 32u) { uint vr = vi / 8u; uint vc = vi - vr * 8u; uint vphys = sphys[kk8 * 8u + vr]; sv[vi] = vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride + kv_head_base + d8 + vc] : half(0.0f); } simdgroup_barrier(mem_flags::mem_threadgroup); simdgroup_load(mv, sv, 8u); } simdgroup_multiply_accumulate(mo[dt], mp, mv, mo[dt]); } }
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (tid < 8u) { float denom = sS[tid]; sdiag[uint(tid) * 8u + uint(tid)] = denom > 0.0f ? 1.0f / denom : 0.0f; }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u);
    for (uint dt = 0u; dt < 8u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); mo[dt] = scaled; }
    if (q0 + 8u <= p.q_len) { for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], output + q0 * q_stride + h * 256u + dslice + dt * 8u, q_stride); return; }
    const uint valid_rows = p.q_len - q0; for (uint owner = 0u; owner < 4u; ++owner) { threadgroup_barrier(mem_flags::mem_threadgroup); if (uint(sgitg) == owner) for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], so + dt * 8u, 64u); threadgroup_barrier(mem_flags::mem_threadgroup); for (uint i = uint(tid); i < valid_rows * 64u; i += 128u) { uint j = i / 64u; uint d = i - j * 64u; output[(q0 + j) * q_stride + h * 256u + owner * 64u + d] = so[i]; } }
}
