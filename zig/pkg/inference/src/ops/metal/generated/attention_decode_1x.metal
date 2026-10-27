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

struct antfly_paged_attention_1x_params { uint q_len; uint kv_tokens; uint num_heads; uint num_kv_heads; uint head_dim; uint key_row_bytes; uint base_key_row_bytes; uint query_position_offset; uint kv_position_offset; uint sliding_window; uint v_row_stride; uint page_size; uint block_count; uint contiguous_base_token; uint contiguous_blocks; uint format; uint v_element_bytes; uint has_sinks; float softcap; uint swa_scan_clamp; uint has_explicit_positions; };
template<typename Params> inline uint antfly_paged_attention_1x_page_token(device const uint *block_table, constant Params &p, uint logical_token) { if (p.contiguous_blocks != 0u) return p.contiguous_base_token + logical_token; uint logical_block = logical_token / p.page_size; uint token_in_block = logical_token - logical_block * p.page_size; if (logical_block >= p.block_count) return 0xffffffffu; return block_table[logical_block] + token_in_block; }
// Exact online row phase. Score admission, masks, diagonal publication and
// matrix schedules remain with each caller.
__attribute__((always_inline)) inline void antfly_attention_online_row32(float sc, threadgroup const float *maxima, uint state_row, threadgroup half *probability, uint probability_index, thread float &m_old, thread float &m_new, thread float &corr, thread float &row_sum) {
    float row_max = simd_max(sc); m_old = maxima[state_row]; m_new = max(m_old, row_max);
    float e = 0.0f; corr = 1.0f;
    if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; e = sc > -3.0e+38f ? exp(sc - m_new) : 0.0f; }
    probability[probability_index] = half(e);
    row_sum = simd_sum(e);
}
#include "../../../graph/templates/metal_paged_decode_native.metal"

kernel void antfly_paged_attention_1x_generated_msl_v1(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup float *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
    antfly_paged_decode_body<antfly_paged_attention_1x_params, 256u>(q, encoded_key, v_bytes, block_table, sinks, output, p, shmem, tid, lane, sgitg, tg);
}
