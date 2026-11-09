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

// native-program:entry termite_lm_head_top8_suppress_partials
// native-program:indent 11
kernel void termite_lm_head_top8_suppress_partials(device const float *logits [[buffer(0)]], device const int *suppress_ids [[buffer(1)]], device float *partial_values [[buffer(2)]], device uint *partial_ids [[buffer(3)]], constant termite_metal_argmax_suppress_params &p [[buffer(4)]], threadgroup float *shared_values [[threadgroup(0)]], threadgroup uint *shared_ids [[threadgroup(1)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
    const uint top_k = 8u; const uint block_size = 1024u;
    thread float best_values[8]; thread uint best_ids[8];
    for (uint k = 0u; k < top_k; ++k) { best_values[k] = -INFINITY; best_ids[k] = 0xffffffffu; }
    uint start = block * block_size; uint end = min(start + block_size, p.out_dim);
    for (uint token_id = start + uint(tid); token_id < end; token_id += uint(tg_size)) {
        if (termite_argmax_suppressed_token(token_id, suppress_ids, p.suppress_count)) continue;
        float value = logits[token_id]; if (!termite_lm_head_candidate_better(value, token_id, best_values[top_k - 1u], best_ids[top_k - 1u])) continue;
        uint at = top_k - 1u; while (at > 0u && termite_lm_head_candidate_better(value, token_id, best_values[at - 1u], best_ids[at - 1u])) { best_values[at] = best_values[at - 1u]; best_ids[at] = best_ids[at - 1u]; --at; }
        best_values[at] = value; best_ids[at] = token_id;
    }
    for (uint k = 0u; k < top_k; ++k) { shared_values[k * uint(tg_size) + uint(tid)] = best_values[k]; shared_ids[k * uint(tg_size) + uint(tid)] = best_ids[k]; }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = uint(tg_size) >> 1u; stride > 0u; stride >>= 1u) {
        if (uint(tid) < stride) {
            for (uint candidate = 0u; candidate < top_k; ++candidate) { float value = shared_values[candidate * uint(tg_size) + uint(tid) + stride]; uint token_id = shared_ids[candidate * uint(tg_size) + uint(tid) + stride]; if (!termite_lm_head_candidate_better(value, token_id, best_values[top_k - 1u], best_ids[top_k - 1u])) continue; uint at = top_k - 1u; while (at > 0u && termite_lm_head_candidate_better(value, token_id, best_values[at - 1u], best_ids[at - 1u])) { best_values[at] = best_values[at - 1u]; best_ids[at] = best_ids[at - 1u]; --at; } best_values[at] = value; best_ids[at] = token_id; }
            for (uint k = 0u; k < top_k; ++k) { shared_values[k * uint(tg_size) + uint(tid)] = best_values[k]; shared_ids[k * uint(tg_size) + uint(tid)] = best_ids[k]; }
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (tid == 0u) { uint base = block * top_k; for (uint k = 0u; k < top_k; ++k) { partial_values[base + k] = best_values[k]; partial_ids[base + k] = best_ids[k]; } }
}
// native-program:entry termite_lm_head_top8_reduce
// native-program:indent 11
kernel void termite_lm_head_top8_reduce(device const float *partial_values [[buffer(0)]], device uint *partial_ids [[buffer(1)]], constant uint &candidate_count [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    if (gid != 0u) return; const uint top_k = 8u; thread float best_values[8]; thread uint best_ids[8];
    for (uint k = 0u; k < top_k; ++k) { best_values[k] = -INFINITY; best_ids[k] = 0xffffffffu; }
    for (uint candidate = 0u; candidate < candidate_count; ++candidate) { float value = partial_values[candidate]; uint token_id = partial_ids[candidate]; if (!termite_lm_head_candidate_better(value, token_id, best_values[top_k - 1u], best_ids[top_k - 1u])) continue; uint at = top_k - 1u; while (at > 0u && termite_lm_head_candidate_better(value, token_id, best_values[at - 1u], best_ids[at - 1u])) { best_values[at] = best_values[at - 1u]; best_ids[at] = best_ids[at - 1u]; --at; } best_values[at] = value; best_ids[at] = token_id; }
    for (uint k = 0u; k < top_k; ++k) partial_ids[k] = best_ids[k];
}
// native-program:entry termite_lm_head_q6_k_rescore_top8
// native-program:indent 11
kernel void termite_lm_head_q6_k_rescore_top8(device const float *input [[buffer(0)]], device const uchar *weight_raw [[buffer(1)]], device const uint *candidate_ids [[buffer(2)]], device uint *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], threadgroup float *scores [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]]) {
    const uint top_k = 8u; if (uint(sgitg) >= top_k) return; device const uchar *weight = weight_raw + p.weight_offset; uint token_id = candidate_ids[sgitg]; bool valid = token_id < p.out_dim; const uint nb = p.row_blocks; const uint row_bytes = nb * 210u; device const uchar *x0 = valid ? weight + token_id * row_bytes : weight; float sumf = 0.0f; float yl[16];
    const ushort tid2 = lane / 2u; const ushort ix = lane % 2u; const ushort ip = tid2 / 8u; const ushort il = tid2 % 8u; const ushort l0 = 4u * il; const ushort is = 8u * ip + l0 / 16u; const uint y_offset = 128u * uint(ip) + uint(l0); const uint q_offset_l = 64u * uint(ip) + uint(l0); const uint q_offset_h = 32u * uint(ip) + uint(l0);
    for (uint i = ix; i < nb; i += 2u) { device const float *y = input + i * 256u + y_offset; for (ushort l = 0u; l < 4u; ++l) { yl[4u * l + 0u] = y[l]; yl[4u * l + 1u] = y[l + 32u]; yl[4u * l + 2u] = y[l + 64u]; yl[4u * l + 3u] = y[l + 96u]; } device const uchar *q1 = x0 + i * 210u + q_offset_l; device const uchar *q2 = q1 + 32u; device const uchar *qh = x0 + i * 210u + 128u + q_offset_h; device const char *sc = reinterpret_cast<device const char *>(x0 + i * 210u + 192u) + is; device const uchar *dp = x0 + i * 210u + 208u; float4 sums = {0.0f, 0.0f, 0.0f, 0.0f}; for (ushort l = 0u; l < 4u; ++l) { sums[0] += yl[4u * l + 0u] * float(char((q1[l] & 0x0fu) | ((qh[l] & 0x03u) << 4)) - 32); sums[1] += yl[4u * l + 1u] * float(char((q2[l] & 0x0fu) | ((qh[l] & 0x0cu) << 2)) - 32); sums[2] += yl[4u * l + 2u] * float(char((q1[l] >> 4) | (qh[l] & 0x30u)) - 32); sums[3] += yl[4u * l + 3u] * float(char((q2[l] >> 4) | ((qh[l] & 0xc0u) >> 2)) - 32); } ushort d_bits = (ushort(dp[1]) << 8) | ushort(dp[0]); float d = float(as_type<half>(d_bits)); sumf += d * (sums[0] * float(sc[0]) + sums[1] * float(sc[2]) + sums[2] * float(sc[4]) + sums[3] * float(sc[6])); }
    float total = valid ? simd_sum(sumf) : -INFINITY; if (lane == 0u) scores[sgitg] = total; threadgroup_barrier(mem_flags::mem_threadgroup);
    if (sgitg == 0u && lane == 0u) { float best = scores[0]; uint best_id = candidate_ids[0]; for (uint k = 1u; k < top_k; ++k) { float value = scores[k]; uint id = candidate_ids[k]; if (value > best || (value == best && id < best_id)) { best = value; best_id = id; } } output[0] = best_id < p.out_dim ? best_id : 0u; }
}
// native-program:entry termite_selection_score_norm
// native-program:indent 11
kernel void termite_selection_score_norm(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uint *query_positions [[buffer(2)]], device const uint *key_positions [[buffer(3)]], device float2 *norm [[buffer(4)]], constant termite_selection_score_params &s [[buffer(5)]], uint gid [[thread_position_in_grid]]) { uint total=s.p.q_len*s.p.num_heads; if(gid>=total)return; uint qi=gid/s.p.num_heads; uint h=gid-qi*s.p.num_heads; uint kvh=h/(s.p.num_heads/s.p.num_kv_heads); uint qb=gid*s.p.head_dim; thread float rp[32]; for(uint x=0;x<32;x++){float a=0.0f;if(s.p.format==1u)for(uint d=0;d<s.p.head_dim;d++){ulong z=(ulong(kvh+1u)*0x9e3779b97f4a7c15UL)^(ulong(x+1u)*0xbf58476d1ce4e5b9UL)^(ulong(d+1u)*0x94d049bb133111ebUL);z^=z>>30;z*=0xbf58476d1ce4e5b9UL;z^=z>>27;z*=0x94d049bb133111ebUL;z^=z>>31;a+=((z&1UL)==0UL?1.0f:-1.0f)*q[qb+d];}rp[x]=a;} bool invalid=false; float best=-3.402823466e+38f; for(uint k=0;k<s.p.kv_tokens;k++){if(key_positions[k]>query_positions[qi])continue;float v=termite_paged_attention_score(q,encoded_key,s.p,qb,k,kvh,rp)*(s.score_scale*sqrt(float(s.p.head_dim)));if(s.p.softcap>0.0f)v=tanh(v/s.p.softcap)*s.p.softcap;if(!isfinite(v))invalid=true;else if(v>best)best=v;} float sum=0.0f;for(uint k=0;k<s.p.kv_tokens;k++){if(key_positions[k]>query_positions[qi])continue;float v=termite_paged_attention_score(q,encoded_key,s.p,qb,k,kvh,rp)*(s.score_scale*sqrt(float(s.p.head_dim)));if(s.p.softcap>0.0f)v=tanh(v/s.p.softcap)*s.p.softcap;if(!isfinite(v))invalid=true;else sum+=exp(v-best);}norm[gid]=float2(best,invalid ? -1.0f : sum);}
// native-program:entry termite_selection_score_reduce
// native-program:indent 11
kernel void termite_selection_score_reduce(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uint *query_positions [[buffer(2)]], device const uint *key_positions [[buffer(3)]], device const float2 *norm [[buffer(4)]], device float *out [[buffer(5)]], constant termite_selection_score_params &s [[buffer(6)]], uint k [[thread_position_in_grid]]) {if(k>=s.p.kv_tokens)return;float total=0.0f;for(uint qi=0;qi<s.p.q_len;qi++){if(key_positions[k]>query_positions[qi])continue;for(uint h=0;h<s.p.num_heads;h++){uint kvh=h/(s.p.num_heads/s.p.num_kv_heads);uint qb=(qi*s.p.num_heads+h)*s.p.head_dim;thread float rp[32];for(uint x=0;x<32;x++){float a=0.0f;if(s.p.format==1u)for(uint d=0;d<s.p.head_dim;d++){ulong z=(ulong(kvh+1u)*0x9e3779b97f4a7c15UL)^(ulong(x+1u)*0xbf58476d1ce4e5b9UL)^(ulong(d+1u)*0x94d049bb133111ebUL);z^=z>>30;z*=0xbf58476d1ce4e5b9UL;z^=z>>27;z*=0x94d049bb133111ebUL;z^=z>>31;a+=((z&1UL)==0UL?1.0f:-1.0f)*q[qb+d];}rp[x]=a;}float v=termite_paged_attention_score(q,encoded_key,s.p,qb,k,kvh,rp)*(s.score_scale*sqrt(float(s.p.head_dim)));if(s.p.softcap>0.0f)v=tanh(v/s.p.softcap)*s.p.softcap;float2 n=norm[qi*s.p.num_heads+h];if(isfinite(v)&&n.y>0.0f)total+=exp(v-n.x)/n.y;}}out[k]=total/float(s.p.q_len*s.p.num_heads);}
