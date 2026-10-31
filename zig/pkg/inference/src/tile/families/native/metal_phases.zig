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

//! Exact Metal numerical phases, expanded before native compilation.
//! Raw MSL phases are encoded once at the Objective-C source boundary.

const program = @import("../../native_program.zig");

pub const phases = .{
    .self_attention_scores =
    \\    for (uint ki = 0u; ki < p.seq_len; ++ki) {
    \\        bool allowed = p.has_mask == 0u || mask[b * p.seq_len + ki] != 0.0f; if (!allowed) continue;
    \\        uint k_base = p.layout == 1u ? (b * p.seq_len + ki) * hidden + h * p.head_dim : (bh * p.seq_len + ki) * p.head_dim; float score = 0.0f;
    \\        for (uint x = 0u; x < p.head_dim; ++x) score += q[q_base + x] * k[k_base + x];
    \\        score *= scale;
    \\        if (p.bias_mode == 1u) score += bias[(h * p.seq_len + qi) * p.seq_len + ki];
    \\        else if (p.bias_mode == 2u) score += bias[(bh * p.seq_len + qi) * p.seq_len + ki];
    \\        else if (p.bias_mode == 3u) score += bias[(b * p.seq_len + qi) * p.seq_len + ki];
    ,
    .relative_attention_scores =
    \\    for (uint ki = 0u; ki < p.seq_len; ++ki) {
    \\        bool allowed = p.has_mask == 0u || mask[b * p.seq_len + ki] != 0.0f; if (!allowed) continue;
    \\        uint k_base = (b * p.seq_len + ki) * hidden + head_off; uint rel_idx = qi + p.seq_len - 1u - ki; uint rel_row = p.reserved1 != 0u ? uint(relative_index[rel_idx]) : rel_idx; uint rel_base = rel_row * hidden + head_off; float score = 0.0f;
    \\        for (uint x = 0u; x < p.head_dim; ++x) score += q[q_base + x] * k[k_base + x] + q[q_base + x] * k_r[rel_base + x] + q_r[rel_base + x] * k[k_base + x];
    ,
    .causal_attention_scores =
    \\        uint key_pos = p.kv_position_offset + ki;
    \\        bool allowed = key_pos <= query_pos;
    \\        if (!allowed && p.has_mask != 0u && query_pos < p.total_sequence_len && key_pos < p.total_sequence_len) {
    \\            uint mask_idx = query_pos * p.total_sequence_len + key_pos;
    \\            allowed = mask[mask_idx] != 0u;
    \\        }
    \\        if (key_pos > query_pos) allowed = false;
    \\        if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window;
    ,
    .segment_tensor_geometry =
    \\    const uint D = p.head_dim, hidden = p.num_heads * D, h = tg.y;
    \\    const uint QB = (p.seq_len + BQ - 1u) / BQ;
    \\    const uint row = tg.x / QB, blk = tg.x % QB;
    \\    const uint row_start = row * p.seq_len, q0 = row_start + blk * BQ;
    \\    const uint t = uint(tid), s8 = uint(sg) * 8u, L = uint(lane);
    \\    const uint nq = min(BQ, p.seq_len - blk * BQ), nd = D / 8u;
    \\    const float scale = p.scale, neg = -3.402823466e+38f;
    \\    device const float *k = qkv + p.tokens * hidden;
    ,
    .attention_row_geometry =
    \\    const uint chunk = 256u; uint row = tg.x; uint qi = row / p.num_heads; uint h = row - qi * p.num_heads; if (qi >= p.tokens) return;
    \\    uint lid = uint(tid); uint width = uint(tpg.x); uint hidden = p.num_heads * p.head_dim; uint q_base = qi * hidden + h * p.head_dim;
    \\    device const float *k = qkv + p.tokens * hidden; device const float *v = qkv + 2u * p.tokens * hidden;
    ,
    .q4k_pair_reduce =
    \\    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; } acc_a0 = simd_sum(acc_a0); acc_a1 = simd_sum(acc_a1); acc_b0 = simd_sum(acc_b0); acc_b1 = simd_sum(acc_b1); threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    if (lane == 0u) { shmem[sgitg] = acc_a0; shmem[32u + sgitg] = acc_a1; shmem[64u + sgitg] = acc_b0; shmem[96u + sgitg] = acc_b1; } threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    float total_a0 = simd_sum(shmem[lane]); float total_a1 = simd_sum(shmem[32u + lane]); float total_b0 = simd_sum(shmem[64u + lane]); float total_b1 = simd_sum(shmem[96u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) { output_a[idx] = total_a0; output_b[idx] = total_b0; } if (first_o + 1u < p.out_dim) { output_a[idx + 1u] = total_a1; output_b[idx + 1u] = total_b1; } }
    \\}
    ,
    .segment_tensor_publish =
    \\        simdgroup_store(o[dt], stage + s8 * BK, 8u);
    \\        simdgroup_barrier(mem_flags::mem_threadgroup);
    \\        for (uint i = L; i < 64u; i += 32u) { uint r = s8 + i / 8u; if (r < nq) output[(q0 + r) * hidden + h * D + dt * 8u + i % 8u] = stage[s8 * BK + i]; }
    \\        simdgroup_barrier(mem_flags::mem_threadgroup);
    \\    }
    \\}
    ,
    .attention_row_maximum =
    \\        scores[ki] = score;
    \\    }
    \\    partials[lid] = local_best; threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) { if (lid < stride) partials[lid] = max(partials[lid], partials[lid + stride]); threadgroup_barrier(mem_flags::mem_threadgroup); }
    ,
    .suppressed_argmax_candidates =
    \\    uint end = min(start + block_size, p.out_dim);
    \\    float best = -INFINITY;
    \\    uint best_idx = 0xffffffffu;
    \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) {
    \\        if (termite_argmax_suppressed_token(i, suppress_ids, p.suppress_count)) continue;
    \\        float value = logits[i];
    \\        if (value > best || (value == best && i < best_idx)) { best = value; best_idx = i; }
    \\    }
    \\    if (best_idx == 0xffffffffu) best_idx = 0u;
    \\    sh_vals[tid] = best;
    \\    sh_ids[tid] = best_idx;
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    ,
    .argmax_partial_merge =
    \\    if (partial_count == 0u) { output[0] = 0u; return; }
    \\    float best = partial_values[0];
    \\    uint best_idx = partial_ids[0];
    \\    for (uint i = 1u; i < partial_count; ++i) {
    \\        float value = partial_values[i];
    \\        uint idx = partial_ids[i];
    \\        if (value > best || (value == best && idx < best_idx)) { best = value; best_idx = idx; }
    \\    }
    \\    output[0] = best_idx;
    ,
    .bf16_gate_up_scalar_stage =
    \\        termite_bf16_matrix_stage_input_scalar(x, a, p, row0, k0, tid);
    \\        for(uint i=uint(tid);i<1024u;i+=256u){
    \\            uint c=i/32u,k=i%32u,j=(k/8u*4u+c/8u)*64u+k%8u*8u+c%8u;
    \\            bool valid=col0+c<p.out_dim&&k0+k<p.in_dim;
    \\            g[j]=valid?termite_bf16_to_f32(wg[(col0+c)*p.in_dim+k0+k]):0.0f;
    \\            u[j]=valid?termite_bf16_to_f32(wu[(col0+c)*p.in_dim+k0+k]):0.0f;
    \\        }
    ,
    .bf16_gate_up_tile =
    \\    threadgroup float *a=shared, *g=shared+2048, *u=g+1024;
    \\    uint row0=tg.y*64u, col0=tg.x*32u;
    \\    uint sr=(uint(sg)/2u)*16u, sc=(uint(sg)%2u)*16u;
    \\    simdgroup_float8x8 ag[4],au[4];
    \\    TERMITE_FOR_UNROLL(uint i=0;i<4u;i++){ag[i]=make_filled_simdgroup_matrix<float,8>(0.0f);au[i]=make_filled_simdgroup_matrix<float,8>(0.0f);}
    ,
};

fn encoded(comptime text: []const u8) []const u8 {
    const lines = program.metal(text ++ "\n");
    return lines[0 .. lines.len - 1]; // The template supplies the final source newline.
}

pub const boundary = .{
    .self_attention_scores = encoded(phases.self_attention_scores),
    .relative_attention_scores = encoded(phases.relative_attention_scores),
    .causal_attention_scores = encoded(phases.causal_attention_scores),
    .segment_tensor_geometry = encoded(phases.segment_tensor_geometry),
    .attention_row_geometry = encoded(phases.attention_row_geometry),
    .q4k_pair_reduce = encoded(phases.q4k_pair_reduce),
    .segment_tensor_publish = encoded(phases.segment_tensor_publish),
    .attention_row_maximum = encoded(phases.attention_row_maximum),
};
