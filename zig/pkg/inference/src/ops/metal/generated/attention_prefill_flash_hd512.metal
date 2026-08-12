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
// plan_id=metal/attention/prefill_flash_hd512
// kernel_id=antfly_paged_attention_prefill_flash_hd512_generated_msl_v1
// production_baseline=termite_paged_attention_kv
// production_enabled=true
// Low-threadgroup-memory Gemma4 global-attention specialization.
// Runtime routing is capability-checked and falls back to scalar paged attention.

#include <metal_stdlib>
using namespace metal;

struct antfly_paged_attention_1x_params { uint q_len; uint kv_tokens; uint num_heads; uint num_kv_heads; uint head_dim; uint key_row_bytes; uint base_key_row_bytes; uint query_position_offset; uint kv_position_offset; uint sliding_window; uint v_row_stride; uint page_size; uint block_count; uint contiguous_base_token; uint contiguous_blocks; uint format; uint v_element_bytes; uint has_sinks; float softcap; uint swa_scan_clamp; };
inline uint antfly_paged_attention_1x_page_token(device const uint *block_table, constant antfly_paged_attention_1x_params &p, uint logical_token) { if (p.contiguous_blocks != 0u) return p.contiguous_base_token + logical_token; uint logical_block = logical_token / p.page_size; uint token_in_block = logical_token - logical_block * p.page_size; if (logical_block >= p.block_count) return 0xffffffffu; return block_table[logical_block] + token_in_block; }
kernel void antfly_paged_attention_prefill_flash_hd512_generated_msl_v1(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
    const uint hd = p.head_dim; const uint q0 = tg.x * 8u; const uint h = tg.y;
    if (q0 >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || (p.page_size & 7u) != 0u || p.num_kv_heads == 0u || p.num_heads % p.num_kv_heads != 0u || hd != 512u) return;
    const float scale = rsqrt(512.0f); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * hd;
    const uint q_stride = p.num_heads * hd; const uint k_row_halfs = p.key_row_bytes / 2u;
    threadgroup half *sq = (threadgroup half *)shmem;
    threadgroup char *fb = shmem + 8u * 512u * 2u;
    threadgroup float *ss = (threadgroup float *)fb;
    threadgroup half *sp = (threadgroup half *)(fb + 2048u);
    threadgroup uint *sphys = (threadgroup uint *)(fb + 3072u);
    threadgroup float *sM = (threadgroup float *)(fb + 3328u);
    threadgroup float *sS = (threadgroup float *)(fb + 3360u);
    threadgroup float *sdiag = (threadgroup float *)(fb + 3392u);
    threadgroup float *so = (threadgroup float *)(fb + 3648u);
    const device half *k_half = reinterpret_cast<const device half *>(encoded_key); const device half *v_half = reinterpret_cast<const device half *>(v_bytes);
    for (uint i = uint(tid); i < 8u * 512u; i += 256u) { uint j = i / 512u; uint d = i - j * 512u; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + h * 512u + d] * scale) : half(0.0f); }
    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; } if (tid < 64u) sdiag[tid] = 0.0f;
    const uint dslice = uint(sgitg) * 64u; simdgroup_float8x8 mo[8]; for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
    for (uint kc = 0u; kc < p.kv_tokens; kc += 64u) {
        threadgroup_barrier(mem_flags::mem_threadgroup);
        uint q_last = min(q0 + 7u, p.q_len - 1u); uint tile_first_q = p.query_position_offset + q0; uint tile_last_q = p.query_position_offset + q_last; uint key_first = p.kv_position_offset + kc; uint key_last = p.kv_position_offset + min(kc + 63u, p.kv_tokens - 1u); bool wholly_future = key_first > tile_last_q; bool wholly_expired = p.sliding_window != 0u && tile_first_q >= key_last && tile_first_q - key_last >= p.sliding_window; if (wholly_future) break; if (wholly_expired) continue;
        if (tid < 64u) { uint ki = kc + uint(tid); sphys[tid] = ki < p.kv_tokens ? antfly_paged_attention_1x_page_token(block_table, p, ki) : 0xffffffffu; }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        uint ktile = uint(sgitg); uint phys = sphys[ktile * 8u]; const device half *kbase = k_half + (phys != 0xffffffffu ? phys : 0u) * k_row_halfs + kv_head_base; simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f); for (uint d = 0u; d < 512u; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, 512u); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); } simdgroup_store(ms, ss + ktile * 8u, 64u, 0, false);
        threadgroup_barrier(mem_flags::mem_threadgroup);
        uint j = uint(sgitg); uint qi = q0 + j; uint query_pos = p.query_position_offset + qi; float scores[2]; float row_max = -3.402823466e+38f; for (uint kl = 0u; kl < 2u; ++kl) { uint kk = uint(lane) + kl * 32u; uint ki = kc + kk; bool allowed = ki < p.kv_tokens && qi < p.q_len && sphys[kk] != 0xffffffffu; if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; } float sc = allowed ? ss[j * 64u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f; scores[kl] = sc; row_max = max(row_max, sc); } row_max = simd_max(row_max); float m_old = sM[j]; float m_new = max(m_old, row_max); float corr = 1.0f; if (m_new > -3.0e+38f) corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; float row_sum_local = 0.0f; for (uint kl = 0u; kl < 2u; ++kl) { uint kk = uint(lane) + kl * 32u; float sc = scores[kl]; float e = (m_new > -3.0e+38f && sc > -3.0e+38f) ? exp(sc - m_new) : 0.0f; sp[j * 64u + kk] = half(e); row_sum_local += e; } float row_sum = simd_sum(row_sum_local); if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u); for (uint i = 0u; i < 8u; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; }
        for (uint kk8 = 0u; kk8 < 8u; ++kk8) { uint phys_v = sphys[kk8 * 8u]; simdgroup_half8x8 mp; simdgroup_load(mp, sp + kk8 * 8u, 64u); for (uint dt = 0u; dt < 8u; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_half8x8 mv; if (kc + kk8 * 8u + 8u <= p.kv_tokens) { const device half *vbase = v_half + phys_v * p.v_row_stride + kv_head_base + d8; simdgroup_load(mv, vbase, p.v_row_stride); } else { threadgroup half *sv = (threadgroup half *)ss + uint(sgitg) * 64u; for (uint vi = uint(lane); vi < 64u; vi += 32u) { uint vr = vi / 8u; uint vc = vi - vr * 8u; uint vphys = sphys[kk8 * 8u + vr]; sv[vi] = vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride + kv_head_base + d8 + vc] : half(0.0f); } simdgroup_barrier(mem_flags::mem_threadgroup); simdgroup_load(mv, sv, 8u); } simdgroup_multiply_accumulate(mo[dt], mp, mv, mo[dt]); } }
    }
    threadgroup_barrier(mem_flags::mem_threadgroup); if (tid < 8u) { float denom = sS[tid]; sdiag[uint(tid) * 8u + uint(tid)] = denom > 0.0f ? 1.0f / denom : 0.0f; } threadgroup_barrier(mem_flags::mem_threadgroup);
    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u); for (uint dt = 0u; dt < 8u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); mo[dt] = scaled; }
    if (q0 + 8u <= p.q_len) { for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], output + q0 * q_stride + h * 512u + dslice + dt * 8u, q_stride); return; }
    const uint valid_rows = p.q_len - q0; for (uint owner = 0u; owner < 8u; ++owner) { threadgroup_barrier(mem_flags::mem_threadgroup); if (uint(sgitg) == owner) for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], so + dt * 8u, 64u); threadgroup_barrier(mem_flags::mem_threadgroup); for (uint i = uint(tid); i < valid_rows * 64u; i += 256u) { uint j = i / 64u; uint d = i - j * 64u; output[(q0 + j) * q_stride + h * 512u + owner * 64u + d] = so[i]; } }
}
