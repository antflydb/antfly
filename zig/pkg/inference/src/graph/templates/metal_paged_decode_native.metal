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

template<typename Params, uint NT>
__attribute__((always_inline)) inline void antfly_paged_decode_body(device const float *q , device const uchar *encoded_key , device const uchar *v_bytes , device const uint *block_table , device const float *sinks , device float *output , constant Params &p , threadgroup float *shmem , ushort tid , ushort lane , ushort sgitg , uint2 tg) {
    uint qi = tg.x; uint h = tg.y; if (qi >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || p.num_kv_heads == 0u) return;
    const uint NW = 32u; const uint NSG = NT / 32u; const float scale = rsqrt(float(p.head_dim)); uint heads_per_group = p.num_heads / p.num_kv_heads; uint kv_h = h / heads_per_group; uint q_stride = p.num_heads * p.head_dim; uint q_base = qi * q_stride + h * p.head_dim; uint out_base = qi * q_stride + h * p.head_dim; uint kv_head_base = kv_h * p.head_dim; uint query_pos = p.query_position_offset + qi; uint oldest_delta = query_pos - p.kv_position_offset; bool all_tokens_allowed = p.kv_tokens != 0u && query_pos >= p.kv_position_offset && oldest_delta >= p.kv_tokens - 1u && (p.sliding_window == 0u || oldest_delta < p.sliding_window); threadgroup float *scores = shmem; threadgroup float *partials = shmem + p.kv_tokens; threadgroup float *phys_tokens = shmem + p.kv_tokens + NT; const device half *v_half = reinterpret_cast<const device half *>(v_bytes);
    for (uint base = 0u; base < p.kv_tokens; base += NSG) { uint ki = base + uint(sgitg); bool allowed = ki < p.kv_tokens && all_tokens_allowed; if (ki < p.kv_tokens && !allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; } uint physical_token = allowed ? antfly_paged_attention_1x_page_token(block_table, p, ki) : 0xffffffffu; if (physical_token == 0xffffffffu) allowed = false; float acc = 0.0f; if (allowed) { const device half *k_half = reinterpret_cast<const device half *>(encoded_key + physical_token * p.key_row_bytes); for (uint d = uint(lane); d < p.head_dim; d += NW) acc += q[q_base + d] * float(k_half[kv_head_base + d]); } float score = allowed ? simd_sum(acc) * scale : -3.402823466e+38f; if (!isfinite(score)) score = -3.402823466e+38f; if (lane == 0u && ki < p.kv_tokens) { scores[ki] = score; phys_tokens[ki] = as_type<float>(allowed ? physical_token : 0u); } }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float local_best = -3.402823466e+38f;
    for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) {
        float score = scores[ki]; local_best = score > local_best ? score : local_best;
    }
    // Only the maximum changes reduction topology. All inputs are finite
    // (invalid scores use the sentinel); softmax sums retain their exact tree.
    local_best = simd_max(local_best);
    if (lane == 0u) partials[sgitg] = local_best;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    const float best = simd_max(uint(lane) < NSG ? partials[lane] : -3.402823466e+38f);
    // Every SIMD group must load the maximum before partials[0] is reused.
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float local_sum = 0.0f; for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) { float score = scores[ki]; float e = (best > -3.0e+38f && score > -3.0e+38f) ? exp(score - best) : 0.0f; e = isfinite(e) ? e : 0.0f; scores[ki] = e; local_sum += e; } partials[tid] = local_sum; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = NT >> 1u; stride > 0u; stride >>= 1u) { if (uint(tid) < stride) partials[tid] += partials[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); } const float inv_sum = partials[0] > 0.0f ? 1.0f / partials[0] : 0.0f;
    for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) scores[ki] *= inv_sum; threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint d = uint(tid); d < p.head_dim; d += NT) { const device half *v_col = v_half + kv_head_base + d; float value = 0.0f; for (uint ki = 0u; ki < p.kv_tokens; ++ki) { float weight = scores[ki]; value += weight * select(float(v_col[as_type<uint>(phys_tokens[ki]) * p.v_row_stride]), 0.0f, weight == 0.0f); } output[out_base + d] = value; }
}
