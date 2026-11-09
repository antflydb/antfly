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

// native-program:entry termite_quantize_rows_i8
// native-program:indent 11
kernel void termite_quantize_rows_i8(device const float *input [[buffer(0)]], device char *qinput [[buffer(1)]], device float *scales [[buffer(2)]], constant termite_metal_quantize_rows_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.rows) return; uint row = gid; uint row_base = row * p.dim; float abs_max = 0.0f; for (uint i = 0; i < p.dim; ++i) { abs_max = fmax(abs_max, fabs(input[row_base + i])); } float scale = abs_max == 0.0f ? 1.0f : abs_max / 127.0f; scales[row] = scale; float inv = scale == 0.0f ? 0.0f : 1.0f / scale; for (uint i = 0; i < p.dim; ++i) { float q = clamp(round(input[row_base + i] * inv), -127.0f, 127.0f); qinput[row_base + i] = char(int(q)); }
}
// native-program:entry termite_apply_parallel_ffn_post_residual_simd4
// native-program:indent 11
kernel void termite_apply_parallel_ffn_post_residual_simd4(device const float *shared [[buffer(0)]], device const float *shared_weight [[buffer(1)]], device const float *routed [[buffer(2)]], device const float *routed_weight [[buffer(3)]], device const float *combined_weight [[buffer(4)]], device const float *residual [[buffer(5)]], device float *output [[buffer(6)]], constant termite_metal_apply_row_norm_params &p [[buffer(7)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint simd_group [[simdgroup_index_in_threadgroup]], uint simd_lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]) {
    if (row >= p.rows || p.hidden_size == 0u || (p.hidden_size & 3u) != 0u) return;
    if (simd_group == 0u) { shmem[simd_lane] = 0.0f; shmem[32u + simd_lane] = 0.0f; }
    uint base = row * p.hidden_size; uint vectors = p.hidden_size >> 2u;
    float shared_mean_square = 0.0f; float routed_mean_square = 0.0f;
    for (uint v = tid; v < vectors; v += tpg) {
        uint i = base + (v << 2u);
        float4 s = float4(shared[i], shared[i + 1u], shared[i + 2u], shared[i + 3u]);
        float4 r = float4(routed[i], routed[i + 1u], routed[i + 2u], routed[i + 3u]);
        shared_mean_square += dot(s, s); routed_mean_square += dot(r, r);
    }
    shared_mean_square = simd_sum(shared_mean_square); routed_mean_square = simd_sum(routed_mean_square);
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (simd_lane == 0u) { shmem[simd_group] = shared_mean_square; shmem[32u + simd_group] = routed_mean_square; }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    shared_mean_square = simd_sum(shmem[simd_lane]); routed_mean_square = simd_sum(shmem[32u + simd_lane]);
    float shared_inv_rms = rsqrt(shared_mean_square / float(p.hidden_size) + p.eps);
    float routed_inv_rms = rsqrt(routed_mean_square / float(p.hidden_size) + p.eps);
    device volatile float *rounded = output; float combined_mean_square = 0.0f;
    for (uint v = tid; v < vectors; v += tpg) {
        uint j = v << 2u; uint i = base + j;
        float4 s = float4(shared[i], shared[i + 1u], shared[i + 2u], shared[i + 3u]);
        float4 sw = float4(shared_weight[j], shared_weight[j + 1u], shared_weight[j + 2u], shared_weight[j + 3u]);
        float4 shared_norm = s * (shared_inv_rms * sw);
        rounded[i] = shared_norm.x; rounded[i + 1u] = shared_norm.y; rounded[i + 2u] = shared_norm.z; rounded[i + 3u] = shared_norm.w;
        float4 shared_rounded = float4(rounded[i], rounded[i + 1u], rounded[i + 2u], rounded[i + 3u]);
        float4 r = float4(routed[i], routed[i + 1u], routed[i + 2u], routed[i + 3u]);
        float4 rw = float4(routed_weight[j], routed_weight[j + 1u], routed_weight[j + 2u], routed_weight[j + 3u]);
        float4 routed_norm = r * (routed_inv_rms * rw);
        rounded[i] = routed_norm.x; rounded[i + 1u] = routed_norm.y; rounded[i + 2u] = routed_norm.z; rounded[i + 3u] = routed_norm.w;
        float4 routed_rounded = float4(rounded[i], rounded[i + 1u], rounded[i + 2u], rounded[i + 3u]);
        float4 combined = shared_rounded + routed_rounded;
        rounded[i] = combined.x; rounded[i + 1u] = combined.y; rounded[i + 2u] = combined.z; rounded[i + 3u] = combined.w;
        float4 combined_rounded = float4(rounded[i], rounded[i + 1u], rounded[i + 2u], rounded[i + 3u]);
        combined_mean_square += dot(combined_rounded, combined_rounded);
    }
    combined_mean_square = simd_sum(combined_mean_square);
    if (simd_group == 0u) shmem[simd_lane] = 0.0f;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (simd_lane == 0u) shmem[simd_group] = combined_mean_square;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    combined_mean_square = simd_sum(shmem[simd_lane]);
    float combined_inv_rms = rsqrt(combined_mean_square / float(p.hidden_size) + p.eps);
    for (uint v = tid; v < vectors; v += tpg) {
        uint j = v << 2u; uint i = base + j;
        float4 x = float4(rounded[i], rounded[i + 1u], rounded[i + 2u], rounded[i + 3u]);
        float4 w = float4(combined_weight[j], combined_weight[j + 1u], combined_weight[j + 2u], combined_weight[j + 3u]);
        float4 normalized = x * (combined_inv_rms * w);
        rounded[i] = normalized.x; rounded[i + 1u] = normalized.y; rounded[i + 2u] = normalized.z; rounded[i + 3u] = normalized.w;
    }
    threadgroup_barrier(mem_flags::mem_device);
    for (uint v = tid; v < vectors; v += tpg) {
        uint i = base + (v << 2u);
        float4 normalized = float4(rounded[i], rounded[i + 1u], rounded[i + 2u], rounded[i + 3u]);
        float4 res = float4(residual[i], residual[i + 1u], residual[i + 2u], residual[i + 3u]);
        float4 y = normalized + res;
        output[i] = y.x; output[i + 1u] = y.y; output[i + 2u] = y.z; output[i + 3u] = y.w;
    }
}
// native-program:entry termite_sample_nucleus_radix_select
// native-program:indent 11
kernel void termite_sample_nucleus_radix_select(device const float *histograms [[buffer(0)]], device termite_metal_nucleus_state *state [[buffer(1)]], threadgroup float *bins [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]]) {
    for (uint b = uint(tid); b < 16u; b += uint(tg_size)) { float mass = 0.0f; for (uint block = 0u; block < state->block_count; ++block) mass += histograms[block * 16u + b]; bins[b] = mass; } threadgroup_barrier(mem_flags::mem_threadgroup); if (tid != 0u) return; uint lowest_nonzero = 0u; for (uint b = 0u; b < 16u; ++b) if (bins[b] > 0.0f) lowest_nonzero = b; uint selected = lowest_nonzero; float above = state->mass_above; bool found = false; for (int b = 15; b >= 0; --b) { if (bins[uint(b)] <= 0.0f) continue; lowest_nonzero = uint(b); if (above + bins[uint(b)] >= state->target_mass) { selected = uint(b); found = true; break; } above += bins[uint(b)]; } if (!found) { selected = lowest_nonzero; above -= bins[selected]; } uint shift = 28u - state->pass * 4u; state->prefix |= selected << shift; state->mass_above = above; state->pass += 1u; if (state->pass == 8u) state->cutoff_key = state->prefix;
}
// native-program:entry termite_apply_activation_1x
// native-program:indent 11
kernel void termite_apply_activation_1x(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_apply_activation_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.rows * p.dim; if (gid >= total) return;
    output[gid] = termite_apply_activation_scalar(input[gid], p.activation_kind);
}
// native-program:entry termite_apply_bias_activation_2d
// native-program:indent 11
kernel void termite_apply_bias_activation_2d(device const float *input [[buffer(0)]], device const float *bias [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_activation_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.rows * p.dim; if (gid >= total || p.dim == 0u) return;
    uint col = gid % p.dim;
    output[gid] = termite_apply_activation_scalar(input[gid] + bias[col], p.activation_kind);
}
// native-program:entry antfly_causal_depthwise_segmented_f32
// native-program:indent 11
kernel void antfly_causal_depthwise_segmented_f32(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], device const float *starts [[buffer(3)]], constant termite_metal_gemma4_audio_rows_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
    uint total=p.rows*p.dim;if(gid>=total)return;
    uint t=gid/p.dim,h=gid-t*p.dim,start=p.reserved?uint(starts[t]):0u;
    output[gid]=antfly_causal_depthwise<false,false,false,false>(input,weight,input,t,h,p.dim,p.extra,0u,start);
}
// native-program:entry Kernel
// native-program:indent 11
kernel void Kernel(device const float *src_k [[buffer(0)]], device const float *src_v [[buffer(1)]], device Out *dst_k [[buffer(2)]], device Out *dst_v [[buffer(3)]], device const uint *block_table [[buffer(4)]], constant termite_metal_paged_f32_kv_seed_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) { \
    antfly_paged_kv_seed(src_k, src_v, dst_k, dst_v, block_table, p, gid); \
}
// native-program:entry termite_dense_pair_split_f32_2d
// native-program:indent 11
kernel void termite_dense_pair_split_f32_2d(device const float *packed [[buffer(0)]], device float *a_out [[buffer(1)]], device float *b_out [[buffer(2)]], constant termite_metal_dense_pair_split_bias_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    antfly_dense_split<false, false>(packed, packed, packed, packed, a_out, b_out, b_out, p.rows, p.total_out_dim, p.out_dim, p.out_dim, gid);
}
// native-program:entry termite_dense_pair_split_bias_f32_2d
// native-program:indent 11
kernel void termite_dense_pair_split_bias_f32_2d(device const float *packed [[buffer(0)]], device const float *a_bias [[buffer(1)]], device const float *b_bias [[buffer(2)]], device float *a_out [[buffer(3)]], device float *b_out [[buffer(4)]], constant termite_metal_dense_pair_split_bias_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {
    antfly_dense_split<false, true>(packed, a_bias, b_bias, b_bias, a_out, b_out, b_out, p.rows, p.total_out_dim, p.out_dim, p.out_dim, gid);
}
// native-program:entry termite_integer_binary
// native-program:indent 11
kernel void termite_integer_binary(device const uchar *lhs [[buffer(0)]], device const uchar *rhs [[buffer(1)]], device uchar *output [[buffer(2)]], constant uint *p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p[0]) return;
    uint li = p[3] != 0u ? 0u : gid, ri = p[4] != 0u ? 0u : gid;
    if (p[6] != 0u) {
        li = 0u; ri = 0u; uint remaining = gid;
        for (uint axis = 0u; axis < p[6]; ++axis) {
            uint coord = remaining / p[7u + axis]; remaining %= p[7u + axis];
            li += coord * p[15u + axis]; ri += coord * p[23u + axis];
        }
    }
    long a = p[1] != 0u ? termite_read_integer(lhs, p[1], li) : 0;
    long b = p[2] != 0u ? termite_read_integer(rhs, p[2], ri) : 0;
    if (p[5] == 3u) {
        bool result = p[1] == 0u ? termite_float_lt_integer(((device const float *)lhs)[li], b) : p[2] == 0u ? termite_integer_lt_float(a, ((device const float *)rhs)[ri]) : a < b;
        ((device float *)output)[gid] = result ? 1.0f : 0.0f;
        return;
    }
    ulong result = p[5] == 0u ? ulong(a) + ulong(b) : p[5] == 1u ? ulong(a) - ulong(b) : ulong(a) * ulong(b);
    switch (p[1]) {
        case 1: ((device uint *)output)[gid] = uint(result); break;
        case 2: ((device ulong *)output)[gid] = result; break;
        case 3: case 5: output[gid] = uchar(result); break;
        case 4: ((device ushort *)output)[gid] = ushort(result); break;
    }
}
// native-program:entry termite_masked_mean_rows
// native-program:indent 11
kernel void termite_masked_mean_rows(device const float *input [[buffer(0)]], device const long *mask [[buffer(1)]], device float *output [[buffer(2)]], constant uint4 &p [[buffer(3)]], uint d [[thread_position_in_grid]]) {
    if (d >= p.z) return;
    float sum = 0.0f;
    float unused_count = 0.0f; sum = antfly_masked_column_sum<false,false,true>(input, mask, 0u, p.x, p.y, d, float(p.w), unused_count);
    output[d] = sum;
}
// native-program:entry termite_florence_channel_softmax_f32
// native-program:indent 11
kernel void termite_florence_channel_softmax_f32(device float *scores [[buffer(0)]], constant termite_metal_florence_channel_params &p [[buffer(1)]], uint gid [[thread_position_in_grid]]) {
    uint cpg = p.channels_per_group; uint rows = p.batch * p.groups * cpg; if (gid >= rows || cpg == 0u) return; uint base = gid * cpg;
    float best = -3.402823466e+38f; for (uint vc = 0u; vc < cpg; ++vc) best = max(best, scores[base + vc]);
    float sum = 0.0f; for (uint vc = 0u; vc < cpg; ++vc) { float weight = exp(scores[base + vc] - best); scores[base + vc] = weight; sum += weight; }
    scores[p.batch * p.groups * cpg * cpg + gid] = sum;
}
// native-program:entry termite_dot_general_2d_f32_reduce
// native-program:indent 11
kernel void termite_dot_general_2d_f32_reduce(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_dot_general_2d_f32_params &p [[buffer(3)]], threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
    uint total = p.m * p.n; if (group >= total || p.k == 0u) return; uint row = group / p.n; uint col = group - row * p.n; float acc = 0.0f;
    for (uint kk = uint(lane); kk < p.k; kk += 32u) { float r = p.rhs_contract_axis == 0u ? rhs[kk * p.n + col] : rhs[col * p.k + kk]; acc += lhs[row * p.k + kk] * r; }
    partials[lane] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = 16u; stride > 0u; stride >>= 1u) { if (lane < stride) partials[lane] += partials[lane + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }
    if (lane == 0u) output[group] = partials[0];
}
// native-program:entry antfly_video_position
// native-program:indent 11
kernel void antfly_video_position(device const float *input [[buffer(0)]], device const float *positions [[buffer(1)]], device float *output [[buffer(2)]], constant uint4 &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.x) return; uint row=gid/768u, h=gid%768u; uint x=row%p.y, y=(row/p.y)%p.z;
    output[gid]=input[gid]+(positions[x*768u+h]+positions[(p.w+y)*768u+h]);
}
