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

// native-program:entry antfly_mpp_bf16_gate_up0
// native-program:indent 8
kernel void antfly_mpp_bf16_gate_up0(device bfloat*x[[buffer(0)]],device bfloat*g[[buffer(1)]],device bfloat*u[[buffer(2)]],device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device ushort*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 group[[threadgroup_position_in_grid]]){antfly_mpp_bf16_gate_up<32,64,4>(x,g,u,gb,ub,y,p,group);}
// native-program:entry antfly_mpp_bf16_gate_up1
// native-program:indent 8
kernel void antfly_mpp_bf16_gate_up1(device bfloat*x[[buffer(0)]],device bfloat*g[[buffer(1)]],device bfloat*u[[buffer(2)]],device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device ushort*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 group[[threadgroup_position_in_grid]]){antfly_mpp_bf16_gate_up<64,32,4>(x,g,u,gb,ub,y,p,group);}
// native-program:entry termite_gate_split_reduce
// native-program:indent 8
kernel void termite_gate_split_reduce(device const float*s[[buffer(0)]],device const float*gb[[buffer(1)]],device const float*ub[[buffer(2)]],device float*y[[buffer(3)]],constant GateSplitParams&p[[buffer(4)]],uint i[[thread_position_in_grid]]){uint count=p.rows*p.out_dim;if(i>=count)return;float g=0,u=0;for(uint j=0;j<p.pad;j++){g+=s[j*count+i];u+=s[(p.pad+j)*count+i];}g+=gb[i%p.out_dim];u+=ub[i%p.out_dim];y[i]=(g/(1.0f+exp(-g)))*u;}
// native-program:entry termite_lightonocr3_down_split_reduce
// native-program:indent 8
kernel void termite_lightonocr3_down_split_reduce(device const float*s[[buffer(0)]], device const float*bias[[buffer(1)]], device float*y[[buffer(2)]], constant Params&p[[buffer(3)]],uint i[[thread_position_in_grid]]) {
 if(i>=p.rows*p.out_dim)return;float sum=0;for(uint j=0;j<p.pad;j++)sum+=s[j*p.rows*p.out_dim+i];y[i]=sum+bias[i%p.out_dim];
}
// native-program:entry termite_lightonocr3_down_split32
// native-program:indent 8
kernel void termite_lightonocr3_down_split32(device float*x[[buffer(0)]],device bfloat*w[[buffer(1)]],device float*s[[buffer(2)]],constant Params&p[[buffer(3)]],uint2 g[[threadgroup_position_in_grid]]){ antfly_mpp_split_affine<32>(x,w,s,p,g); }
// native-program:entry termite_lightonocr3_down_split64
// native-program:indent 8
kernel void termite_lightonocr3_down_split64(device float*x[[buffer(0)]],device bfloat*w[[buffer(1)]],device float*s[[buffer(2)]],constant Params&p[[buffer(3)]],uint2 g[[threadgroup_position_in_grid]]){ antfly_mpp_split_affine<64>(x,w,s,p,g); }
// native-program:entry termite_apply_linear_bias_2d
// native-program:indent 11
kernel void termite_apply_linear_bias_2d(device float *output [[buffer(0)]], device const float *bias [[buffer(1)]], constant termite_metal_linear_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.rows * p.out_dim; if (gid >= total || p.out_dim == 0u) return;
    uint col = gid - (gid / p.out_dim) * p.out_dim;
    output[gid] += bias[col];
}
// native-program:entry termite_q6_k_high_row_mm_matrix_m64_n64
// native-program:indent 11
kernel void termite_q6_k_high_row_mm_matrix_m64_n64(device const float *input [[buffer(0)]], device const uchar *weight_q6_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    threadgroup uchar shared[16384];
    termite_k_mm_matrix_m64_n64<float, float, true>(input, weight_q6_k, output, rows, in_dim, out_dim, thread_pos, group_pos, simdgroup_id, shared);
}
// native-program:entry termite_q8_0_qkv_linear_mmv
// native-program:indent 11
kernel void termite_q8_0_qkv_linear_mmv(device const float *input [[buffer(0)]], device const uchar *weight_q [[buffer(1)]], device const uchar *weight_k [[buffer(2)]], device const uchar *weight_v [[buffer(3)]], device float *output_q [[buffer(4)]], device float *output_k [[buffer(5)]], device float *output_v [[buffer(6)]], constant termite_metal_qkv_linear_params &p [[buffer(7)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    uint q_groups = (p.q_out_dim + 1u) / 2u;
    if (tg.x < q_groups) {
        termite_q8_0_mmv_single(input, weight_q + p.q_weight_offset, output_q, p.rows, p.in_dim, p.q_out_dim, p.row_blocks, shmem, lane, sgitg, tg.x * 2u, tg.y);
    } else {
        termite_q8_0_mmv_pair(input, weight_k + p.k_weight_offset, weight_v + p.v_weight_offset, output_k, output_v, p.rows, p.in_dim, p.kv_out_dim, p.row_blocks, shmem, lane, sgitg, (tg.x - q_groups) * 2u, tg.y);
    }
}
// native-program:entry termite_i2_s_linear_i8
// native-program:indent 11
kernel void termite_i2_s_linear_i8(device const char *qinput [[buffer(0)]], device const uchar *weight [[buffer(1)]], device const float *scales [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
    termite_i2_s_projection<false, true >(qinput, weight, nullptr, scales, output, nullptr, p, gid);
}
// native-program:entry termite_apply_linear_1x_reduce_simd
// native-program:indent 11
kernel void termite_apply_linear_1x_reduce_simd(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], ushort lane [[thread_index_in_simdgroup]], ushort simdgroup [[simdgroup_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
    const uint row = group * 4u + uint(simdgroup);
    if (row >= p.out_dim) return;
    float acc = lane == 0u ? bias[row] : 0.0f;
    const uint row_base = row * p.in_dim;
    for (uint i = uint(lane); i < p.in_dim; i += 32u) acc += input[i] * weight[row_base + i];
    for (ushort delta = 16u; delta > 0u; delta >>= 1u) acc += simd_shuffle_xor(acc, delta);
    if (lane == 0u) output[row] = acc;
}
// native-program:entry termite_lightonocr3_gemv_vec4
// native-program:indent 11
kernel void termite_lightonocr3_gemv_vec4(device const float *x [[buffer(0)]], device const ushort *w [[buffer(1)]], device const float *b [[buffer(2)]], device float *y [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], uint lane [[thread_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
 antfly_bf16_gemv_vec4<1,1>(x,w,w,b,b,y,p.in_dim,p.out_dim,group,lane);
}
// native-program:entry termite_lightonocr3_gemv_vec4_rows2
// native-program:indent 11
kernel void termite_lightonocr3_gemv_vec4_rows2(device const float *x [[buffer(0)]], device const ushort *w [[buffer(1)]], device const float *b [[buffer(2)]], device float *y [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], uint lane [[thread_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
 antfly_bf16_gemv_vec4<2,1>(x,w,w,b,b,y,p.in_dim,p.out_dim,group,lane);
}
// native-program:entry termite_lightonocr3_gate_up_gemv
// native-program:indent 11
kernel void termite_lightonocr3_gate_up_gemv(device const float *x [[buffer(0)]], device const ushort *gate_w [[buffer(1)]], device const ushort *up_w [[buffer(2)]], device const float *gate_b [[buffer(3)]], device const float *up_b [[buffer(4)]], device float *y [[buffer(5)]], constant termite_metal_linear_params &p [[buffer(6)]], uint lane [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
 if(row>=p.out_dim)return;
 antfly_bf16_gemv_vec4<1,2>(x,gate_w,up_w,gate_b,up_b,y,p.in_dim,p.out_dim,row,lane);
}
// native-program:entry termite_lightonocr3_four_row_segment
// native-program:indent 11
kernel void termite_lightonocr3_four_row_segment(device const float*x[[buffer(0)]],device const ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant termite_metal_linear_params&p[[buffer(4)]],uint lane[[thread_index_in_threadgroup]],uint group[[threadgroup_position_in_grid]]){termite_lightonocr3_segment_body<4>(x,w,bias,y,p,lane,group);}
// native-program:entry termite_apply_linear_pair_1x_reduce
// native-program:indent 11
kernel void termite_apply_linear_pair_1x_reduce(device const float *input [[buffer(0)]], device const float *weight_a [[buffer(1)]], device const float *weight_b [[buffer(2)]], device const float *bias_a [[buffer(3)]], device const float *bias_b [[buffer(4)]], device float *output_a [[buffer(5)]], device float *output_b [[buffer(6)]], constant termite_metal_apply_linear_params &p [[buffer(7)]], uint lane [[thread_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
    if (group >= p.out_dim) return;
    threadgroup float partials_a[32];
    threadgroup float partials_b[32];
    float acc_a = lane == 0u ? bias_a[group] : 0.0f;
    float acc_b = lane == 0u ? bias_b[group] : 0.0f;
    uint row_base = group * p.in_dim;
    for (uint i = lane; i < p.in_dim; i += 32u) {
        float x = input[i];
        acc_a += x * weight_a[row_base + i];
        acc_b += x * weight_b[row_base + i];
    }
    partials_a[lane] = acc_a;
    partials_b[lane] = acc_b;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = 16u; stride > 0u; stride >>= 1u) {
        if (lane < stride) {
            partials_a[lane] += partials_a[lane + stride];
            partials_b[lane] += partials_b[lane + stride];
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (lane == 0u) { output_a[group] = partials_a[0]; output_b[group] = partials_b[0]; }
}
// native-program:entry termite_apply_linear_bf16_multi_row_shared_reduce
// native-program:indent 11
kernel void termite_apply_linear_bf16_multi_row_shared_reduce(device const float *input [[buffer(0)]], device const ushort *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_threadgroup]], uint o [[threadgroup_position_in_grid]]) {
    if (o >= p.out_dim || p.rows == 0u || p.rows > 64u) return;
    if (p.rows <= 4u) {
        float init = lane == 0u ? bias[o] : 0.0f;
        float a0 = init, a1 = init, a2 = init, a3 = init;
        uint w_base = o * p.in_dim;
        for (uint i = lane; i < p.in_dim; i += 32u) {
            float w = termite_bf16_to_f32(weight[w_base + i]);
            a0 += input[i] * w;
            if (p.rows > 1u) a1 += input[p.in_dim + i] * w;
            if (p.rows > 2u) a2 += input[2u * p.in_dim + i] * w;
            if (p.rows > 3u) a3 += input[3u * p.in_dim + i] * w;
        }
        for (uint stride = 16u; stride > 0u; stride >>= 1u) {
            float b0 = simd_shuffle_down(a0, stride);
            float b1 = simd_shuffle_down(a1, stride);
            float b2 = simd_shuffle_down(a2, stride);
            float b3 = simd_shuffle_down(a3, stride);
            if (lane < stride) { a0 += b0; a1 += b1; a2 += b2; a3 += b3; }
        }
        if (lane == 0u) {
            output[o] = a0;
            if (p.rows > 1u) output[p.out_dim + o] = a1;
            if (p.rows > 2u) output[2u * p.out_dim + o] = a2;
            if (p.rows > 3u) output[3u * p.out_dim + o] = a3;
        }
        return;
    }
    for (uint r = 0u; r < p.rows; ++r) partials[r * 32u + lane] = lane == 0u ? bias[o] : 0.0f;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    uint w_base = o * p.in_dim;
    for (uint i = lane; i < p.in_dim; i += 32u) {
        float w = termite_bf16_to_f32(weight[w_base + i]);
        for (uint r = 0u; r < p.rows; ++r) partials[r * 32u + lane] += input[r * p.in_dim + i] * w;
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = 16u; stride > 0u; stride >>= 1u) {
        if (lane < stride) {
            for (uint r = 0u; r < p.rows; ++r) partials[r * 32u + lane] += partials[r * 32u + lane + stride];
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (lane == 0u) {
        for (uint r = 0u; r < p.rows; ++r) output[r * p.out_dim + o] = partials[r * 32u];
    }
}
// native-program:entry termite_apply_linear_argmax_1x
// native-program:indent 11
kernel void termite_apply_linear_argmax_1x(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device uint *output [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
    if (gid != 0) return;
    float best = -INFINITY;
    uint best_idx = 0u;
    for (uint o = 0; o < p.out_dim; ++o) {
        float acc = bias[o];
        uint row_base = o * p.in_dim;
        for (uint i = 0; i < p.in_dim; ++i) acc += input[i] * weight[row_base + i];
        if (acc > best) { best = acc; best_idx = o; }
    }
    output[0] = best_idx;
}
// native-program:entry termite_whisper_logits_partials
// native-program:indent 11
kernel void termite_whisper_logits_partials(device const float *logits [[buffer(0)]], device const int *suppress_ids [[buffer(1)]], device float *partials [[buffer(2)]], constant termite_metal_whisper_logits_params &p [[buffer(3)]], device const uint *grammar [[buffer(4)]], threadgroup float *sh [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
    const uint block_size = 1024u;
    uint text_allowed = p.text_allowed; uint ts_min = p.ts_min; uint ts_max = p.ts_max;
    if ((p.mode & 1u) != 0u) { text_allowed = grammar[0]; ts_min = grammar[1]; ts_max = grammar[2]; }
    const uint start = block * block_size;
    const uint end = min(start + block_size, p.out_dim);
    float best_v = -INFINITY; uint best_i = 0xffffffffu;
    float text_v = -INFINITY; uint text_i = 0xffffffffu;
    float ts_v = -INFINITY; uint ts_i = 0xffffffffu;
    float raw_v = -INFINITY; uint raw_i = 0xffffffffu;
    float all_m = -INFINITY; float all_s = 0.0f; float tsl_m = -INFINITY; float tsl_s = 0.0f; float raw_m = -INFINITY; float raw_s = 0.0f;
    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) {
        float v = logits[i];
        termite_whisper_best_push(raw_v, raw_i, v, i);
        termite_whisper_lse_push(raw_m, raw_s, v);
        bool is_ts = i >= p.ts_begin;
        bool allowed = (i == p.eot) || (is_ts ? (i >= ts_min && i < ts_max) : (text_allowed != 0u));
        if (allowed && p.suppress_count != 0u && termite_argmax_suppressed_token(i, suppress_ids, p.suppress_count)) allowed = false;
        if (!allowed) continue;
        termite_whisper_best_push(best_v, best_i, v, i);
        termite_whisper_lse_push(all_m, all_s, v);
        if (is_ts) { termite_whisper_best_push(ts_v, ts_i, v, i); termite_whisper_lse_push(tsl_m, tsl_s, v); }
        else { termite_whisper_best_push(text_v, text_i, v, i); }
    }
    const uint n = uint(tg_size);
    sh[0u * n + tid] = best_v; sh[1u * n + tid] = as_type<float>(best_i);
    sh[2u * n + tid] = text_v; sh[3u * n + tid] = as_type<float>(text_i);
    sh[4u * n + tid] = ts_v; sh[5u * n + tid] = as_type<float>(ts_i);
    sh[6u * n + tid] = raw_v; sh[7u * n + tid] = as_type<float>(raw_i);
    sh[8u * n + tid] = all_m; sh[9u * n + tid] = all_s;
    sh[10u * n + tid] = tsl_m; sh[11u * n + tid] = tsl_s;
    sh[12u * n + tid] = raw_m; sh[13u * n + tid] = raw_s;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = n >> 1u; stride > 0u; stride >>= 1u) {
        if (uint(tid) < stride) {
            const uint o = uint(tid) + stride;
            for (uint c = 0u; c < 4u; ++c) {
                uint id = as_type<uint>(sh[(2u * c + 1u) * n + o]);
                if (id != 0xffffffffu) {
                    float bv = sh[(2u * c) * n + tid]; uint bi = as_type<uint>(sh[(2u * c + 1u) * n + tid]);
                    termite_whisper_best_push(bv, bi, sh[(2u * c) * n + o], id);
                    sh[(2u * c) * n + tid] = bv; sh[(2u * c + 1u) * n + tid] = as_type<float>(bi);
                }
            }
            for (uint c = 4u; c < 7u; ++c) {
                float m = sh[(2u * c) * n + tid]; float sm = sh[(2u * c + 1u) * n + tid];
                termite_whisper_lse_merge(m, sm, sh[(2u * c) * n + o], sh[(2u * c + 1u) * n + o]);
                sh[(2u * c) * n + tid] = m; sh[(2u * c + 1u) * n + tid] = sm;
            }
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (tid == 0u) { for (uint c = 0u; c < 14u; ++c) partials[block * 16u + c] = sh[c * n]; partials[block * 16u + 14u] = 0.0f; partials[block * 16u + 15u] = 0.0f; }
}
// native-program:entry termite_whisper_logits_reduce
// native-program:indent 11
kernel void termite_whisper_logits_reduce(device const float *logits [[buffer(0)]], device const float *partials [[buffer(1)]], device float *output_base [[buffer(2)]], constant termite_metal_whisper_logits_params &p [[buffer(3)]], constant uint &partial_count [[buffer(4)]], device uint *grammar [[buffer(5)]], device uint *token_out [[buffer(6)]], uint gid [[thread_position_in_grid]]) {
    if (gid != 0u) return;
    device float *output = output_base + p.stats_slot * 16u;
    float best_v = -INFINITY; uint best_i = 0xffffffffu; float text_v = -INFINITY; uint text_i = 0xffffffffu; float ts_v = -INFINITY; uint ts_i = 0xffffffffu; float raw_v = -INFINITY; uint raw_i = 0xffffffffu;
    float all_m = -INFINITY; float all_s = 0.0f; float tsl_m = -INFINITY; float tsl_s = 0.0f; float raw_m = -INFINITY; float raw_s = 0.0f;
    for (uint b = 0u; b < partial_count; ++b) {
        const uint q = b * 16u;
        uint id;
        id = as_type<uint>(partials[q + 1u]); if (id != 0xffffffffu) termite_whisper_best_push(best_v, best_i, partials[q + 0u], id);
        id = as_type<uint>(partials[q + 3u]); if (id != 0xffffffffu) termite_whisper_best_push(text_v, text_i, partials[q + 2u], id);
        id = as_type<uint>(partials[q + 5u]); if (id != 0xffffffffu) termite_whisper_best_push(ts_v, ts_i, partials[q + 4u], id);
        id = as_type<uint>(partials[q + 7u]); if (id != 0xffffffffu) termite_whisper_best_push(raw_v, raw_i, partials[q + 6u], id);
        termite_whisper_lse_merge(all_m, all_s, partials[q + 8u], partials[q + 9u]);
        termite_whisper_lse_merge(tsl_m, tsl_s, partials[q + 10u], partials[q + 11u]);
        termite_whisper_lse_merge(raw_m, raw_s, partials[q + 12u], partials[q + 13u]);
    }
    output[0] = best_v; output[1] = as_type<float>(best_i); output[2] = text_v; output[3] = as_type<float>(text_i);
    output[4] = ts_v; output[5] = as_type<float>(ts_i); output[6] = raw_v; output[7] = as_type<float>(raw_i);
    output[8] = all_m; output[9] = all_s; output[10] = tsl_m; output[11] = tsl_s; output[12] = raw_m; output[13] = raw_s;
    const float eot_logit = (p.eot < p.out_dim) ? logits[p.eot] : -INFINITY;
    output[14] = (p.probe_id < p.out_dim) ? logits[p.probe_id] : 0.0f; output[15] = eot_logit;
    if ((p.mode & 2u) == 0u) return;
    // Device token choice (mirrors the host chooseFromStats): the
    // timestamp-mass rule, then eot competing with the timestamps.
    const bool ts_on = (p.mode & 4u) != 0u; const bool eot_ok = (p.mode & 8u) != 0u;
    uint tok = 0u; float lp = -100.0f; bool chosen = false;
    if (ts_on && ts_i != 0xffffffffu) {
        const float lse_ts = (tsl_s <= 0.0f) ? -INFINITY : (tsl_m + log(tsl_s));
        if (text_i == 0xffffffffu || lse_ts > text_v) {
            if (eot_ok && eot_logit != -INFINITY) {
                const float hi = max(lse_ts, eot_logit); const float lse = hi + log(exp(lse_ts - hi) + exp(eot_logit - hi));
                if (eot_logit >= ts_v) { tok = p.eot; lp = eot_logit - lse; } else { tok = ts_i; lp = ts_v - lse; }
            } else { tok = ts_i; lp = ts_v - lse_ts; }
            chosen = true;
        }
    }
    if (!chosen && best_i != 0xffffffffu) { tok = best_i; lp = best_v - ((all_s <= 0.0f) ? -INFINITY : (all_m + log(all_s))); }
    output[14] = as_type<float>(tok); output[15] = lp;
    token_out[p.token_slot] = tok;
    // Advance the timestamp grammar so the next step's window is ready
    // before the host has seen this token (ruleWindow on the device).
    const bool is_ts = tok >= p.ts_begin;
    const uint penult_is_ts = grammar[3]; const uint last_is_ts = is_ts ? 1u : 0u;
    uint has_last_ts = grammar[5]; uint last_ts = grammar[6];
    if (is_ts) { has_last_ts = 1u; last_ts = tok; }
    uint text_allowed = 1u; uint ts_min = p.ts_begin; uint ts_max = p.out_dim;
    if (last_is_ts != 0u) { if (penult_is_ts != 0u) ts_max = ts_min; else text_allowed = 0u; }
    if (has_last_ts != 0u) { const uint first_allowed = (last_is_ts != 0u && penult_is_ts == 0u) ? last_ts : (last_ts + 1u); ts_min = max(ts_min, min(first_allowed, p.out_dim)); }
    if (ts_max < ts_min) ts_max = ts_min;
    grammar[0] = text_allowed; grammar[1] = ts_min; grammar[2] = ts_max; grammar[3] = last_is_ts; grammar[4] = penult_is_ts; grammar[5] = has_last_ts; grammar[6] = last_ts; grammar[7] = tok;
}
// native-program:entry termite_tl1_linear
// native-program:indent 11
kernel void termite_tl1_linear(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_tl1_params &p [[buffer(3)]], uint2 tid [[thread_position_in_grid]]) {
    uint o = tid.x; uint r = tid.y; if (o >= p.out_dim || r >= p.rows) return;
    uint input_base = r * p.in_dim; float abs_max = 0.0f;
    for (uint i = 0; i < p.in_dim; ++i) abs_max = fmax(abs_max, fabs(input[input_base + i]));
    float act_scale = abs_max == 0.0f ? 1.0f : abs_max / 127.0f;
    uint scale_bits = uint(weight[p.packed_len]) | (uint(weight[p.packed_len + 1u]) << 8) | (uint(weight[p.packed_len + 2u]) << 16) | (uint(weight[p.packed_len + 3u]) << 24);
    float weight_scale = as_type<float>(scale_bits); float acc = 0.0f;
    uint row_outer = o / p.bm; uint row_in_bm = o % p.bm; uint bm_block = row_in_bm / p.bmm; uint row_in_bmm = row_in_bm % p.bmm; uint tl_by = 256u / p.bmm; uint row16 = row_in_bmm % 16u; uint bmm16 = row_in_bmm / 16u; uint col_blocks = p.in_dim / p.cfg_by; uint pair_count = p.in_dim / 2u;
    for (uint pair_col = 0; pair_col < pair_count; ++pair_col) { uint col_block = pair_col / (p.cfg_by / 2u); uint col_in_block = pair_col % (p.cfg_by / 2u); uint by_block = col_in_block / (tl_by / 2u); uint pair_in_by = col_in_block % (tl_by / 2u); uint by4 = pair_in_by / 2u; uint nibble = pair_in_by % 2u; uint index = ((((((row_outer * col_blocks + col_block) * (p.bm / p.bmm) + bm_block) * (p.cfg_by / tl_by) + by_block) * (p.bmm / 16u) + bmm16) * (tl_by / 4u) + by4) * 16u + row16); uchar packed = weight[index]; uint code = nibble == 0u ? uint(packed >> 4) : uint(packed & 0x0Fu); float w0 = floor(float(code) / 3.0f) - 1.0f; float w1 = float(code % 3u) - 1.0f; uint col = pair_col * 2u; float a0 = clamp(round(input[input_base + col] / act_scale), -127.0f, 127.0f) * act_scale; float a1 = clamp(round(input[input_base + col + 1u] / act_scale), -127.0f, 127.0f) * act_scale; acc += a0 * w0 + a1 * w1; }
    output[r * p.out_dim + o] = acc * weight_scale;
}
// native-program:entry termite_tl2_linear
// native-program:indent 11
kernel void termite_tl2_linear(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_tl2_params &p [[buffer(3)]], uint2 tid [[thread_position_in_grid]]) {
    uint o = tid.x; uint r = tid.y; if (o >= p.out_dim || r >= p.rows) return;
    uint input_base = r * p.in_dim; float abs_max = 0.0f;
    for (uint i = 0; i < p.in_dim; ++i) abs_max = fmax(abs_max, fabs(input[input_base + i]));
    float act_scale = abs_max == 0.0f ? 1.0f : abs_max / 127.0f;
    uint scale_bits = uint(weight[p.scale_off]) | (uint(weight[p.scale_off + 1u]) << 8) | (uint(weight[p.scale_off + 2u]) << 16) | (uint(weight[p.scale_off + 3u]) << 24);
    float weight_scale = as_type<float>(scale_bits); float acc = 0.0f;
    uint row_outer = o / p.bm; uint row_in_bm = o % p.bm; uint bm_block = row_in_bm / p.bmm; uint row_in_bmm = row_in_bm % p.bmm; uint row_group = row_in_bmm / 8u; uint row_offset = row_in_bmm % 8u; uint final_group = row_group == 1u ? 2u : (row_group == 2u ? 1u : row_group); uint three_by = 192u / p.bmm; uint col_blocks = p.in_dim / p.cfg_by; uint three_count = p.three_cols / 3u;
    for (uint triple_col = 0; triple_col < three_count; ++triple_col) { uint col_block = triple_col / (p.cfg_by / 3u); uint col_in_block = triple_col % (p.cfg_by / 3u); uint by_block = col_in_block / (three_by / 3u); uint triple_in_by = col_in_block % (three_by / 3u); uint value_index = ((((row_outer * col_blocks + col_block) * (p.bm / p.bmm) + bm_block) * (p.cfg_by / three_by) + by_block) * p.bmm + final_group * 8u + row_offset); uchar packed = weight[value_index]; uint code = triple_in_by == 0u ? uint(packed >> 4) : uint(packed & 0x0Fu); uint sub_count = (three_by / 3u) * 4u; uint sign_block = col_in_block / sub_count; uint sign_sub = col_in_block % sub_count; uint line = sign_sub * p.bmm + row_in_bmm; uint bit_lane = line / (p.bmm / 2u); uint half_idx = line % (p.bmm / 2u); uint bit_index = 15u - bit_lane; uint sign_index = ((((row_outer * col_blocks + col_block) * (p.bm / p.bmm) + bm_block) * (p.cfg_by / (three_by * 4u)) + sign_block) * p.bmm + half_idx * 2u + bit_index / 8u); uchar sign_byte = weight[p.three_value_len + sign_index]; bool negative = ((sign_byte >> (bit_index % 8u)) & 1u) != 0u; float w0 = 0.0f; float w1 = 0.0f; float w2 = 0.0f; switch (code) { case 1u: w2 = 1.0f; break; case 2u: w1 = 1.0f; w2 = -1.0f; break; case 3u: w1 = 1.0f; break; case 4u: w1 = 1.0f; w2 = 1.0f; break; case 5u: w0 = 1.0f; w1 = -1.0f; w2 = -1.0f; break; case 6u: w0 = 1.0f; w1 = -1.0f; break; case 7u: w0 = 1.0f; w1 = -1.0f; w2 = 1.0f; break; case 8u: w0 = 1.0f; w2 = -1.0f; break; case 9u: w0 = 1.0f; break; case 10u: w0 = 1.0f; w2 = 1.0f; break; case 11u: w0 = 1.0f; w1 = 1.0f; w2 = -1.0f; break; case 12u: w0 = 1.0f; w1 = 1.0f; break; case 13u: w0 = 1.0f; w1 = 1.0f; w2 = 1.0f; break; default: break; } if (negative) { w0 = -w0; w1 = -w1; w2 = -w2; } uint col = triple_col * 3u; float a0 = clamp(round(input[input_base + col] / act_scale), -127.0f, 127.0f) * act_scale; float a1 = clamp(round(input[input_base + col + 1u] / act_scale), -127.0f, 127.0f) * act_scale; float a2 = clamp(round(input[input_base + col + 2u] / act_scale), -127.0f, 127.0f) * act_scale; acc += a0 * w0 + a1 * w1 + a2 * w2; }
    if (p.two_cols != 0u) { uint tail_base = p.three_value_len + p.three_sign_len; uint tail_pair_count = p.two_cols / 2u; uint tail_row_outer = o / 32u; uint tail_row_in_bm = o % 32u; uint tail_row_group = tail_row_in_bm / 8u; uint tail_row_offset = tail_row_in_bm % 8u; uint tail_final_group = tail_row_group == 1u ? 2u : (tail_row_group == 2u ? 1u : tail_row_group); for (uint pair_col = 0; pair_col < tail_pair_count; ++pair_col) { uint col_block = pair_col / 16u; uint col_in_block = pair_col % 16u; uint by_block = col_in_block / 2u; uint pair_in_by = col_in_block % 2u; uint tail_col_blocks = p.two_cols / 32u; uint index = (((tail_row_outer * tail_col_blocks + col_block) * 8u + by_block) * 32u + tail_final_group * 8u + tail_row_offset); uchar packed = weight[tail_base + index]; uint code = pair_in_by == 0u ? uint(packed >> 4) : uint(packed & 0x0Fu); float w0 = floor(float(code) / 3.0f) - 1.0f; float w1 = float(code % 3u) - 1.0f; uint col = p.three_cols + pair_col * 2u; float a0 = clamp(round(input[input_base + col] / act_scale), -127.0f, 127.0f) * act_scale; float a1 = clamp(round(input[input_base + col + 1u] / act_scale), -127.0f, 127.0f) * act_scale; acc += a0 * w0 + a1 * w1; } }
    output[r * p.out_dim + o] = acc * weight_scale;
}
// native-program:entry antfly_masked_mean_batch
// native-program:indent 11
kernel void antfly_masked_mean_batch(device const float *input [[buffer(0)]], device const long *mask [[buffer(1)]], device float *output [[buffer(2)]], constant uint4 &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    bool segmented = (p.w & 0x80000000u) != 0; uint batch = p.w & 0x7fffffffu; if (gid >= p.z * batch) return; uint b = gid / p.z, d = gid % p.z; uint start = segmented ? uint(mask[2u * b]) : b * p.x, end = segmented ? uint(mask[2u * b + 1u]) : start + p.x; float sum = 0.0f; float count = segmented ? float(end - start) : float(mask[p.x * batch + b]);
    for (uint r = start; r < end; ++r) if (segmented || mask[r] != 0) sum += input[r * p.y + d] / count;
    output[gid] = sum;
}
// native-program:entry termite_lora_rank1_linear_f32
// native-program:indent 11
kernel void termite_lora_rank1_linear_f32(device const float *input [[buffer(0)]], device const float *base [[buffer(1)]], device const float *lora_a [[buffer(2)]], device const float *lora_b [[buffer(3)]], device float *after_a [[buffer(4)]], device float *after_b [[buffer(5)]], device float *output [[buffer(6)]], constant termite_metal_lora_linear_f32_params &p [[buffer(7)]], threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
    if (p.rank != 1u || row >= p.rows || p.in_dim == 0u) return; float acc = 0.0f;
    for (uint i = uint(sgitg) * 32u + uint(lane); i < p.in_dim; i += 256u) acc += input[row * p.in_dim + i] * lora_a[i];
    if (sgitg == 0u) partials[lane] = 0.0f; acc = simd_sum(acc); threadgroup_barrier(mem_flags::mem_threadgroup);
    if (lane == 0u) partials[sgitg] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);
    float a = simd_sum(partials[lane]); if (lane == 0u && sgitg == 0u) { partials[0] = a; after_a[row] = a; } threadgroup_barrier(mem_flags::mem_threadgroup);
    a = partials[0]; for (uint out = uint(sgitg) * 32u + uint(lane); out < p.out_dim; out += 256u) { uint idx = row * p.out_dim + out; float b = a * lora_b[out]; after_b[idx] = b; output[idx] = base[idx] + b * p.scale; }
}
// native-program:entry termite_dot_general_batched_f32
// native-program:indent 11
kernel void termite_dot_general_batched_f32(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_dot_general_batched_f32_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint per_batch = p.m * p.n; uint total = p.batch_count * per_batch; if (gid >= total || p.k == 0u || per_batch == 0u) return; uint batch = gid / per_batch; uint inner = gid - batch * per_batch; uint row = inner / p.n; uint col = inner - row * p.n; uint lhs_base = batch * p.m * p.k; uint rhs_base = batch * p.n * p.k; float acc = 0.0f;
    for (uint kk = 0u; kk < p.k; ++kk) { float r = p.rhs_contract_axis == 0u ? rhs[rhs_base + kk * p.n + col] : rhs[rhs_base + col * p.k + kk]; acc += lhs[lhs_base + row * p.k + kk] * r; }
    output[gid] = acc;
}
// native-program:entry antfly_clipped_linear
// native-program:indent 0
kernel void antfly_clipped_linear(device const float *input [[buffer(0)]],device const float *weight [[buffer(1)]],device float *out [[buffer(2)]],constant antfly_clipped_linear_params &p [[buffer(3)]],uint tid [[thread_index_in_threadgroup]],ushort sg [[simdgroup_index_in_threadgroup]],uint3 group [[threadgroup_position_in_grid]]) {
threadgroup float a[1024],b[1024],c[1024];uint r0=group.y*32,o0=group.x*32;
simdgroup_float8x8 ma,mb,acc0=make_filled_simdgroup_matrix<float,8>(0.f),acc1=make_filled_simdgroup_matrix<float,8>(0.f);
for(uint base=0;base<p.in_dim;base+=32) {
for(uint i=tid;i<1024;i+=256) {uint r=r0+i/32,k=base+i%32;float x=(r<p.rows && k<p.in_dim) ? input[r*p.in_dim+k] : 0.f;
if(r<p.rows && k<p.in_dim) {if(p.flags&1) x=max(x,p.input_min);if(p.flags&2) x=min(x,p.input_max);} a[i]=x;
uint bk=base+i/32,o=o0+i%32;b[i]=(o<p.out_dim && bk<p.in_dim) ? weight[o*p.in_dim+bk] : 0.f;}
threadgroup_barrier(mem_flags::mem_threadgroup);
for(uint k=0;k<32;k+=8) {simdgroup_load(mb,b+k*32+(uint(sg)%4)*8,32);simdgroup_load(ma,a+(uint(sg)/4)*16*32+k,32);simdgroup_multiply_accumulate(acc0,ma,mb,acc0);simdgroup_load(ma,a+((uint(sg)/4)*16+8)*32+k,32);simdgroup_multiply_accumulate(acc1,ma,mb,acc1);}
threadgroup_barrier(mem_flags::mem_threadgroup);}
simdgroup_store(acc0,c+(uint(sg)/4)*16*32+(uint(sg)%4)*8,32);simdgroup_store(acc1,c+((uint(sg)/4)*16+8)*32+(uint(sg)%4)*8,32);threadgroup_barrier(mem_flags::mem_threadgroup);
for(uint i=tid;i<1024;i+=256) {uint r=r0+i/32,o=o0+i%32;if(r<p.rows && o<p.out_dim) {float x=c[i];if(p.flags&4) x=max(x,p.output_min);if(p.flags&8) x=min(x,p.output_max);if(p.flags&16) x=x/(1.f+exp(-x));out[r*p.out_dim+o]=x;}}
}
