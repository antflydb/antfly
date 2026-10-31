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

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.join(&.{
        \\           TERMITE_MSL_ABI_PAGED_GQA_SPLIT_STAGE(termite_paged_attention_kv_decode_gqa_split_stage)
        \\
        ,
        program.metal(
            \\    const uint qi = tg.x; const uint kv_h = tg.y; const uint split = tg.z; const uint hd = p.head_dim; const uint split_count = schedule.split_count;
            \\    const bool legacy_geometry = p.num_heads == 8u && (p.num_kv_heads == 1u || p.num_kv_heads == 2u) && ((hd == 256u && p.sliding_window == 512u) || (hd == 512u && p.sliding_window == 0u)); const bool a4b_geometry = p.num_heads == 16u && ((p.num_kv_heads == 8u && hd == 256u && p.sliding_window == 1024u) || (p.num_kv_heads == 2u && hd == 512u && p.sliding_window == 0u));
            \\    if (p.kv_tokens == 0u || schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || kv_h >= p.num_kv_heads || split >= split_count || p.q_len == 0u || p.q_len > 2u || p.format != 3u || p.v_element_bytes != 2u || p.page_size == 0u || (p.page_size & 7u) != 0u || (!legacy_geometry && !a4b_geometry) || p.key_row_bytes != 2u * p.num_kv_heads * hd || p.v_row_stride != p.num_kv_heads * hd || p.has_sinks != 0u || p.softcap != 0.0f) return;
            \\    termite_paged_gqa_split_stage_body(q, encoded_key, v_bytes, block_table, partial_o, p, schedule, shmem, tid, lane, sgitg, qi, kv_h, split, hd, split_count);
            \\}
            \\// Stage 2 merges split-local (O,S,M) with the global stable softmax.
            \\// Split is innermost so every SIMD lane reads one coalesced float4.
            \\
        ),

        \\           TERMITE_MSL_ABI_PAGED_GQA_SPLIT_REDUCE(termite_paged_attention_kv_decode_gqa_split_reduce)
        \\
        ,
        program.metal(
            \\    const uint qi = tg.x; const uint h = tg.y; const uint hd = p.head_dim; const uint split_count = schedule.split_count; const bool legacy_geometry = p.num_heads == 8u && (p.num_kv_heads == 1u || p.num_kv_heads == 2u) && ((hd == 256u && p.sliding_window == 512u) || (hd == 512u && p.sliding_window == 0u)); const bool a4b_geometry = p.num_heads == 16u && ((p.num_kv_heads == 8u && hd == 256u && p.sliding_window == 1024u) || (p.num_kv_heads == 2u && hd == 512u && p.sliding_window == 0u)); if (p.kv_tokens == 0u || schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || h >= p.num_heads || p.q_len == 0u || p.q_len > 2u || (!legacy_geometry && !a4b_geometry)) return;
            \\    termite_paged_gqa_split_reduce_body(partial_o, output, p, schedule, lane, sgitg, qi, h, hd, split_count);
            \\}
            \\
        ),

        \\           // Qualified OCR global attention reuses the staged SIMD matrix algorithm.
        \\           TERMITE_MSL_ABI_PAGED_GQA_SPLIT_STAGE(termite_lightonocr3_gqa_split_stage)
        \\
        ,
        program.metal(
            \\    const uint qi = tg.x; const uint kv_h = tg.y; const uint split = tg.z; const uint hd = p.head_dim; const uint split_count = schedule.split_count;
            \\    const bool lightonocr3_geometry = p.q_len == 1u && hd == 256u && p.sliding_window == 0u && ((p.num_heads == 8u && p.num_kv_heads == 2u) || (p.num_heads == 16u && p.num_kv_heads == 4u));
            \\    if (p.kv_tokens == 0u || p.kv_tokens > 16384u || schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || kv_h >= p.num_kv_heads || split >= split_count || p.q_len == 0u || p.q_len != 1u || p.format != 3u || p.v_element_bytes != 2u || p.page_size == 0u || (p.page_size & 7u) != 0u || !lightonocr3_geometry || p.key_row_bytes != 2u * p.num_kv_heads * hd || p.v_row_stride != p.num_kv_heads * hd || p.has_sinks != 0u || p.softcap != 0.0f) return;
            \\    termite_paged_gqa_split_stage_body(q, encoded_key, v_bytes, block_table, partial_o, p, schedule, shmem, tid, lane, sgitg, qi, kv_h, split, hd, split_count);
            \\}
            \\// Stage 2 merges split-local (O,S,M) with the global stable softmax.
            \\// Split is innermost so every SIMD lane reads one coalesced float4.
            \\
        ),

        \\           TERMITE_MSL_ABI_PAGED_GQA_SPLIT_REDUCE(termite_lightonocr3_gqa_split_reduce)
        \\
        ,
        program.metal(
            \\    const uint qi = tg.x; const uint h = tg.y; const uint hd = p.head_dim; const uint split_count = schedule.split_count; const bool lightonocr3_geometry = p.q_len == 1u && hd == 256u && p.sliding_window == 0u && ((p.num_heads == 8u && p.num_kv_heads == 2u) || (p.num_heads == 16u && p.num_kv_heads == 4u)); if (p.kv_tokens == 0u || p.kv_tokens > 16384u || schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || h >= p.num_heads || p.q_len == 0u || p.q_len != 1u || !lightonocr3_geometry) return;
            \\    termite_paged_gqa_split_reduce_body(partial_o, output, p, schedule, lane, sgitg, qi, h, hd, split_count);
            \\}
            \\struct termite_lighton_batch_row {
            \\    device const float *q;
            \\    device const uchar *k;
            \\    device const uchar *v;
            \\    device const uint *blocks;
            \\    device float4 *partial_o;
            \\    device float4 *output;
            \\    termite_metal_paged_attention_params params;
            \\    termite_metal_decode_gqa_split_params schedule;
            \\};
            \\kernel void termite_lightonocr3_gqa_batch_stage(constant termite_lighton_batch_row *rows [[buffer(0)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
            \\    constant termite_lighton_batch_row &row=rows[tg.x];
            \\    constant termite_metal_paged_attention_params &p=row.params;
            \\    constant termite_metal_decode_gqa_split_params &schedule=row.schedule;
            \\    device const float *q=row.q; device const uchar *encoded_key=row.k; device const uchar *v_bytes=row.v;
            \\    device const uint *block_table=row.blocks; device float4 *partial_o=row.partial_o;
            \\
            \\    const uint qi = 0u; const uint kv_h = tg.y; const uint split = tg.z; const uint hd = p.head_dim; const uint split_count = schedule.split_count;
            \\    const bool lightonocr3_geometry = p.q_len == 1u && hd == 256u && p.sliding_window == 0u && ((p.num_heads == 8u && p.num_kv_heads == 2u) || (p.num_heads == 16u && p.num_kv_heads == 4u));
            \\    if (p.kv_tokens == 0u || p.kv_tokens > 16384u || schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || kv_h >= p.num_kv_heads || split >= split_count || p.q_len == 0u || p.q_len != 1u || p.format != 3u || p.v_element_bytes != 2u || p.page_size == 0u || (p.page_size & 7u) != 0u || !lightonocr3_geometry || p.key_row_bytes != 2u * p.num_kv_heads * hd || p.v_row_stride != p.num_kv_heads * hd || p.has_sinks != 0u || p.softcap != 0.0f) return;
            \\    termite_paged_gqa_split_stage_body(q, encoded_key, v_bytes, block_table, partial_o, p, schedule, shmem, tid, lane, sgitg, qi, kv_h, split, hd, split_count);
            \\}
            \\kernel void termite_lightonocr3_gqa_batch_reduce(constant termite_lighton_batch_row *rows [[buffer(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {
            \\    constant termite_lighton_batch_row &row=rows[tg.x];
            \\    constant termite_metal_paged_attention_params &p=row.params;
            \\    constant termite_metal_decode_gqa_split_params &schedule=row.schedule;
            \\    device const float4 *partial_o=row.partial_o; device float4 *output=row.output;
            \\
            \\    const uint qi = 0u; const uint h = tg.y; const uint hd = p.head_dim; const uint split_count = schedule.split_count; const bool lightonocr3_geometry = p.q_len == 1u && hd == 256u && p.sliding_window == 0u && ((p.num_heads == 8u && p.num_kv_heads == 2u) || (p.num_heads == 16u && p.num_kv_heads == 4u)); if (p.kv_tokens == 0u || p.kv_tokens > 16384u || schedule.max_splits == 0u || schedule.max_splits > 32u || schedule.key_chunk != 32u || schedule.reduce_simdgroups != 8u || split_count == 0u || split_count != min(schedule.max_splits, 1u + (p.kv_tokens - 1u) / schedule.key_chunk) || qi >= p.q_len || h >= p.num_heads || p.q_len == 0u || p.q_len != 1u || !lightonocr3_geometry) return;
            \\    termite_paged_gqa_split_reduce_body(partial_o, output, p, schedule, lane, sgitg, qi, h, hd, split_count);
            \\}
            \\// Codec state is exact: Polar4 has no residual projection plane.
            \\struct termite_polar4_span_codec {
            \\    struct State {};
            \\    static inline void prepare(thread State &state, device const float *q, uint q_base, uint kv_h, uint head_dim) {}
            \\    static inline float score(thread const State &state, device const float *q, device const uchar *encoded_key, constant termite_metal_attention_span_params &p, uint q_base, uint k_base, uint kv_h, uint kv_head_off, float scale) {
            \\            float acc = 0.0f;
            \\            for (uint d = 0; d < p.head_dim; ++d) {
            \\                uint value_index = kv_head_off + d;
            \\                uchar packed = encoded_key[k_base + (value_index >> 1)];
            \\                uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);
            \\                float k = float(code) / 7.5f - 1.0f;
            \\                acc += q[q_base + d] * k;
            \\            }
            \\            return acc * scale;
            \\    }
            \\};
            \\struct termite_turbo3_span_codec {
            \\    struct State { float residual_projection[32]; };
            \\    static inline void prepare(thread State &state, device const float *q, uint q_base, uint kv_h, uint head_dim) {
            \\    for (uint projection = 0; projection < 32u; ++projection) {
            \\        float projected_query = 0.0f;
            \\        for (uint d = 0; d < head_dim; ++d) {
            \\            ulong x = (ulong(kv_h + 1u) * 0x9e3779b97f4a7c15UL);
            \\            x ^= (ulong(projection + 1u) * 0xbf58476d1ce4e5b9UL);
            \\            x ^= (ulong(d + 1u) * 0x94d049bb133111ebUL);
            \\            x ^= x >> 30;
            \\            x *= 0xbf58476d1ce4e5b9UL;
            \\            x ^= x >> 27;
            \\            x *= 0x94d049bb133111ebUL;
            \\            x ^= x >> 31;
            \\            float sign = ((x & 1UL) == 0UL) ? 1.0f : -1.0f;
            \\            projected_query += sign * q[q_base + d];
            \\        }
            \\        state.residual_projection[projection] = projected_query;
            \\    }
            \\    }
            \\    static inline float score(thread const State &state, device const float *q, device const uchar *encoded_key, constant termite_metal_attention_span_params &p, uint q_base, uint k_base, uint kv_h, uint kv_head_off, float scale) {
            \\            uint residual_base = k_base + p.base_key_row_bytes;
            \\            float acc = 0.0f;
            \\            for (uint d = 0; d < p.head_dim; ++d) {
            \\                uint value_index = kv_head_off + d;
            \\                uint bit_offset = value_index * 3u;
            \\                uint byte_index = k_base + (bit_offset >> 3);
            \\                uint shift = bit_offset & 7u;
            \\                uint bits = uint(encoded_key[byte_index]) >> shift;
            \\                if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift);
            \\                float k = termite_decode_turbo3_scalar(bits);
            \\                acc += q[q_base + d] * k;
            \\            }
            \\            float residual_acc = 0.0f;
            \\            for (uint projection = 0; projection < 32u; ++projection) {
            \\                uint residual_bit = kv_h * 32u + projection;
            \\                uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)];
            \\                float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
            \\                residual_acc += residual_sign * state.residual_projection[projection];
            \\            }
            \\            return (acc + 0.125f * (residual_acc / 32.0f)) * scale;
            \\    }
            \\};
            \\template<typename Codec> inline void termite_compressed_attention_span(device const float *q, device const uchar *encoded_key, device const float *v, device float *output, constant termite_metal_attention_span_params &p, uint h) {
            \\    if (h >= p.num_heads) return;
            \\    uint heads_per_group = p.num_heads / p.num_kv_heads;
            \\    uint kv_h = h / heads_per_group;
            \\    uint kv_head_off = kv_h * p.head_dim;
            \\    uint q_base = h * p.head_dim;
            \\    typename Codec::State state; Codec::prepare(state, q, q_base, kv_h, p.head_dim);
            \\    float best = -3.402823466e+38f;
            \\    float scale = rsqrt(float(p.head_dim));
            \\    for (uint ki = 0; ki < p.kv_tokens; ++ki) {
            \\        uint key_pos = p.kv_position_offset + ki;
            \\        bool allowed = key_pos <= p.query_position;
            \\        if (p.sliding_window != 0u && allowed) {
            \\            allowed = (p.query_position - key_pos) < p.sliding_window;
            \\        }
            \\        float score = -3.402823466e+38f;
            \\        if (allowed) {
            \\            uint k_base = ki * p.key_row_bytes;
            \\            score = Codec::score(state, q, encoded_key, p, q_base, k_base, kv_h, kv_head_off, scale);
            \\            if (!isfinite(score)) score = -3.402823466e+38f;
            \\        }
            \\        if (score > best) best = score;
            \\    }
            \\    for (uint d = 0; d < p.head_dim; ++d) {
            \\        output[h * p.head_dim + d] = 0.0f;
            \\    }
            \\    float sum = 0.0f;
            \\    for (uint ki = 0; ki < p.kv_tokens; ++ki) {
            \\        uint key_pos = p.kv_position_offset + ki;
            \\        bool allowed = key_pos <= p.query_position;
            \\        if (p.sliding_window != 0u && allowed) {
            \\            allowed = (p.query_position - key_pos) < p.sliding_window;
            \\        }
            \\        if (!allowed) continue;
            \\        uint k_base = ki * p.key_row_bytes;
            \\        float score = Codec::score(state, q, encoded_key, p, q_base, k_base, kv_h, kv_head_off, scale);
            \\        if (!isfinite(score)) continue;
            \\        float e = exp(score - best);
            \\        if (!isfinite(e)) continue;
            \\        sum += e;
            \\        uint v_base = ki * p.v_row_stride + kv_head_off;
            \\        for (uint d = 0; d < p.head_dim; ++d) {
            \\            output[h * p.head_dim + d] += e * v[v_base + d];
            \\        }
            \\    }
            \\    float inv_sum = sum > 0.0f ? 1.0f / sum : 0.0f;
            \\    for (uint d = 0; d < p.head_dim; ++d) {
            \\        output[h * p.head_dim + d] *= inv_sum;
            \\    }
            \\}
            \\
        ),
    });
