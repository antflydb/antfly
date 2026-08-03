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
// plan_id=metal/attention/decode_1x
// kernel_id=antfly_paged_attention_1x_generated_msl_v1
// production_baseline=termite_paged_attention_kv_1x
// production_enabled=false
// Descriptor-driven paged decode attention (first op_kind=.attention route).
// Production decode attention stays on the hand-written termite_paged_attention_kv_1x
// until this candidate clears its bit-identical model-token acceptance gate.

#include <metal_stdlib>
using namespace metal;

struct antfly_paged_attention_1x_params { uint q_len; uint kv_tokens; uint num_heads; uint num_kv_heads; uint head_dim; uint key_row_bytes; uint base_key_row_bytes; uint query_position_offset; uint kv_position_offset; uint sliding_window; uint v_row_stride; uint page_size; uint block_count; uint contiguous_base_token; uint contiguous_blocks; uint format; uint v_element_bytes; uint has_sinks; float softcap; uint swa_scan_clamp; };
inline uint antfly_paged_attention_1x_page_token(device const uint *block_table, constant antfly_paged_attention_1x_params &p, uint logical_token) { if (p.contiguous_blocks != 0u) return p.contiguous_base_token + logical_token; uint logical_block = logical_token / p.page_size; uint token_in_block = logical_token - logical_block * p.page_size; if (logical_block >= p.block_count) return 0xffffffffu; return block_table[logical_block] + token_in_block; }
kernel void antfly_paged_attention_1x_generated_msl_v1(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup float *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
    uint qi = tg.x; uint h = tg.y; if (qi >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || p.num_kv_heads == 0u) return;
    const uint NT = 256u; const uint NW = 32u; const uint NSG = 8u; const float scale = rsqrt(float(p.head_dim)); uint heads_per_group = p.num_heads / p.num_kv_heads; uint kv_h = h / heads_per_group; uint q_stride = p.num_heads * p.head_dim; uint q_base = qi * q_stride + h * p.head_dim; uint out_base = qi * q_stride + h * p.head_dim; uint kv_head_base = kv_h * p.head_dim; uint query_pos = p.query_position_offset + qi; uint kv_end_pos = p.kv_position_offset + p.kv_tokens; bool all_tokens_allowed = query_pos + 1u >= kv_end_pos && (p.sliding_window == 0u || p.sliding_window >= p.kv_tokens); threadgroup float *scores = shmem; threadgroup float *partials = shmem + p.kv_tokens; threadgroup float *phys_tokens = shmem + p.kv_tokens + 256u; const device half *v_half = reinterpret_cast<const device half *>(v_bytes);
    for (uint base = 0u; base < p.kv_tokens; base += NSG) { uint ki = base + uint(sgitg); bool allowed = ki < p.kv_tokens && all_tokens_allowed; if (ki < p.kv_tokens && !allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; } uint physical_token = allowed ? antfly_paged_attention_1x_page_token(block_table, p, ki) : 0xffffffffu; if (physical_token == 0xffffffffu) allowed = false; float acc = 0.0f; if (allowed) { const device half *k_half = reinterpret_cast<const device half *>(encoded_key + physical_token * p.key_row_bytes); for (uint d = uint(lane); d < p.head_dim; d += NW) acc += q[q_base + d] * float(k_half[kv_head_base + d]); } float score = allowed ? simd_sum(acc) * scale : -3.402823466e+38f; if (!isfinite(score)) score = -3.402823466e+38f; if (lane == 0u && ki < p.kv_tokens) { scores[ki] = score; phys_tokens[ki] = as_type<float>(allowed ? physical_token : 0u); } }
    threadgroup_barrier(mem_flags::mem_threadgroup); float local_best = -3.402823466e+38f; for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) { float score = scores[ki]; local_best = score > local_best ? score : local_best; } partials[tid] = local_best; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = NT >> 1u; stride > 0u; stride >>= 1u) { if (uint(tid) < stride) { float rhs = partials[tid + stride]; partials[tid] = rhs > partials[tid] ? rhs : partials[tid]; } threadgroup_barrier(mem_flags::mem_threadgroup); } float best = partials[0];
    float local_sum = 0.0f; for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) { float score = scores[ki]; float e = (best > -3.0e+38f && score > -3.0e+38f) ? exp(score - best) : 0.0f; e = isfinite(e) ? e : 0.0f; scores[ki] = e; local_sum += e; } partials[tid] = local_sum; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = NT >> 1u; stride > 0u; stride >>= 1u) { if (uint(tid) < stride) partials[tid] += partials[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); } const float inv_sum = partials[0] > 0.0f ? 1.0f / partials[0] : 0.0f;
    for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) scores[ki] *= inv_sum; threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint d = uint(tid); d < p.head_dim; d += NT) { const device half *v_col = v_half + kv_head_base + d; float value = 0.0f; for (uint ki = 0u; ki < p.kv_tokens; ++ki) { float weight = scores[ki]; if (weight != 0.0f) value += weight * float(v_col[as_type<uint>(phys_tokens[ki]) * p.v_row_stride]); } output[out_base + d] = value; }
}
