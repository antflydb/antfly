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

// native-program:entry termite_apply_head_rms_rope
// native-program:indent 11
kernel void termite_apply_head_rms_rope(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_head_rms_rope_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint head [[threadgroup_position_in_grid]]) {
    if (head >= p.total_heads || p.head_dim == 0u || p.rope_dim == 0u) return;
    uint base = head * p.head_dim;
    float inv_rms = antfly_rms_tree_inverse<float, false>(input, base, p.head_dim, p.eps, shmem, tid, tpg, nullptr, 0u);
    for (uint d = tid; d < p.head_dim; d += tpg) {
        float value = input[base + d] * inv_rms * weight[d] * p.value_scale;
        if (p.rope_dim < 2u) { output[base + d] = value; continue; }
        uint idx0 = 0u; uint idx1 = 0u; uint j = 0u;
        if (!termite_rope_pair_indices(d, p.head_dim, p.rope_dim, p.consecutive_pairs, idx0, idx1, j)) { output[base + d] = value; continue; }
        float x0 = input[base + idx0] * inv_rms * weight[idx0] * p.value_scale;
        float x1 = input[base + idx1] * inv_rms * weight[idx1] * p.value_scale;
        float freq = 1.0f / pow(p.theta, float(2u * j) / float(p.rope_dim));
        uint row = p.heads_per_row == 0u ? 0u : head / p.heads_per_row;
        if (p.position_period != 0u) row = row % p.position_period;
        float angle = float(p.position + row) * p.freq_scale * freq;
        float c = cos(angle); float s = sin(angle);
        if (d == idx0) output[base + d] = x0 * c - x1 * s;
        else if (d == idx1) output[base + d] = x0 * s + x1 * c;
        else output[base + d] = value;
    }
}
// native-program:entry termite_apply_head_rms_rope_hd128_sg
// native-program:indent 11
kernel void termite_apply_head_rms_rope_hd128_sg(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_head_rms_rope_params &p [[buffer(3)]], ushort lane [[thread_index_in_simdgroup]], ushort sg [[simdgroup_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
    uint head = group * 4u + uint(sg);
    if (head >= p.total_heads) return;
    uint base = head * 128u;
    float inv_rms = antfly_row_rms_simd_inverse<128u>(input,base,128u,p.eps,uint(lane));
    uint row = head / p.heads_per_row;
    if (p.position_period != 0u) row %= p.position_period;
    for (uint j = uint(lane); j < 64u; j += 32u) {
        float x0 = input[base + j] * inv_rms * weight[j] * p.value_scale;
        float x1 = input[base + 64u + j] * inv_rms * weight[64u + j] * p.value_scale;
        float freq = 1.0f / pow(p.theta, float(2u * j) / 128.0f);
        float angle = float(p.position + row) * p.freq_scale * freq;
        float c = cos(angle); float sn = sin(angle);
        output[base + j] = x0 * c - x1 * sn;
        output[base + 64u + j] = x0 * sn + x1 * c;
    }
}
// native-program:entry termite_nomic_pool_normalize
// native-program:indent 11
kernel void termite_nomic_pool_normalize(device const float *hidden [[buffer(0)]], device const uint *mask [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_nomic_pool_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint b [[threadgroup_position_in_grid]]) {
    if (b >= p.batch || p.seq_len == 0u || p.hidden_size == 0u) return;
    const uint threads = 256u; const uint token_base = b * p.seq_len;
    if (tid == 0u) { float count = 0.0f; for (uint t = 0u; t < p.seq_len; ++t) count += mask[token_base + t] != 0u ? 1.0f : 0.0f; shmem[0] = count; }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    const float count = shmem[0]; const float inv_count = count > 0.0f ? 1.0f / count : 0.0f; float local_sq = 0.0f;
    for (uint d = tid; d < p.hidden_size; d += threads) { float unused_count = 0.0f; float sum = antfly_masked_column_sum<false,false,false>(hidden, mask, token_base, token_base + p.seq_len, p.hidden_size, d, 1.0f, unused_count); const float mean = sum * inv_count; output[b * p.hidden_size + d] = mean; local_sq += mean * mean; }
    shmem[tid] = local_sq; threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) { if (tid < stride) shmem[tid] += shmem[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }
    const float scale = (p.normalize != 0u && shmem[0] > 0.0f) ? rsqrt(shmem[0]) : 1.0f;
    for (uint d = tid; d < p.hidden_size; d += threads) output[b * p.hidden_size + d] *= scale;
}
// native-program:entry termite_norm_residual
// native-program:indent 11
kernel void termite_norm_residual(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_norm_residual_params &p [[buffer(4)]], uint tid [[thread_index_in_threadgroup]], uint lane [[thread_index_in_simdgroup]], uint sg [[simdgroup_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
#pragma clang fp contract(off)
    threadgroup float partials[32];
    uint base = row * p.dim; float sum = 0.0f;
    if ((p.dim & 3u) == 0u) { for (uint v = tid; v < p.dim/4u; v += tpg) { uint c = base + v*4u; float4 x = float4(input[c], input[c+1u], input[c+2u], input[c+3u]); sum += dot(x,x); } }
    else { for (uint c = tid; c < p.dim; c += tpg) { float v = input[base+c]; sum += v*v; } }
    float total = simd_sum(sum); if (lane == 0u) partials[sg] = total;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    total = lane < (tpg+31u)/32u ? partials[lane] : 0.0f;
    total = simd_sum(total);
    float inv = 1.0f / sqrt(total / float(p.dim) + p.eps);
    if ((p.dim & 3u) == 0u) { for (uint v = tid; v < p.dim/4u; v += tpg) { uint c = v*4u, i = base+c; float4 x = float4(input[i],input[i+1u],input[i+2u],input[i+3u]); float4 w = float4(weight[c],weight[c+1u],weight[c+2u],weight[c+3u]); float4 r = float4(residual[i],residual[i+1u],residual[i+2u],residual[i+3u]); float4 y = (r + ((x*inv)*w)*p.norm_scale)*p.output_scale; output[i] = y.x; output[i+1u] = y.y; output[i+2u] = y.z; output[i+3u] = y.w; } }
    else { for (uint c = tid; c < p.dim; c += tpg) { float n = (input[base+c] * inv) * weight[c]; output[base+c] = (residual[base+c] + n * p.norm_scale) * p.output_scale; } }
}
// native-program:entry termite_normalize_embedding
// native-program:indent 11
kernel void termite_normalize_embedding(device float *values [[buffer(0)]], constant uint &dimensions [[buffer(1)]], uint t [[thread_index_in_threadgroup]], uint3 group [[threads_per_threadgroup]], uint3 row [[threadgroup_position_in_grid]]) {
    values += row.x * dimensions;
    threadgroup float scratch[256];
    uint n = group.x;
    float peak = 0.0f;
    for (uint d = t; d < dimensions; d += n) {
        float value = values[d];
        peak = isfinite(value) ? max(peak, abs(value)) : INFINITY;
    }
    scratch[t] = peak;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = n / 2; stride > 0; stride /= 2) {
        if (t < stride) scratch[t] = max(scratch[t], scratch[t + stride]);
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    peak = scratch[0];
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float sum = 0.0f;
    if (peak > 0.0f && isfinite(peak)) {
        for (uint d = t; d < dimensions; d += n) {
            float scaled = values[d] / peak;
            sum += scaled * scaled;
        }
    }
    scratch[t] = sum;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = n / 2; stride > 0; stride /= 2) {
        if (t < stride) scratch[t] += scratch[t + stride];
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    sum = scratch[0];
    // All lanes finish reading the original vector before any in-place write.
    threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    bool valid = peak > 0.0f && isfinite(peak) && sum > 0.0f && isfinite(sum);
    for (uint d = t; d < dimensions; d += n) values[d] = valid ? (values[d] / peak) / sqrt(sum) : NAN;
}
// native-program:entry antfly_rotary_table
// native-program:indent 11
kernel void antfly_rotary_table(device float2 *out [[buffer(0)]], constant antfly_rotary_params &p [[buffer(1)]], uint i [[thread_position_in_grid]]) {
    uint pairs = p.dim / (p.vision ? 4u : 2u); if (i >= (p.x+p.y)*pairs) return; uint pos = i/pairs, j = i%pairs;
    float position = float(p.vision && pos >= p.x ? pos-p.x : pos); float angle;
    if (p.vision) angle = position / pow(p.theta, float(2u*j)/float(p.dim/2u));
    else { float frequency = 1.0f / pow(p.theta, float(2u*j)/float(p.dim)); angle = position * frequency; }
    out[i] = float2(cos(angle), sin(angle));
}
// native-program:entry termite_embedding_head_prepare
// native-program:indent 11
kernel void termite_embedding_head_prepare(device const float *x [[buffer(0)]], device const float *w [[buffer(1)]], device const int *ids [[buffer(2)]], device float *y [[buffer(3)]], constant termite_embedding_head_params &p [[buffer(4)]], device const float2 *rotary [[buffer(5)]], uint head [[threadgroup_position_in_grid]], uint lane [[thread_index_in_simdgroup]]) {
    uint row = head / p.heads, h = head % p.heads;
    uint src_row = p.gather == 1u ? uint(ids[row]) : row;
    uint base = src_row * p.stride + p.start + (h / (p.heads / p.source_heads)) * p.dim;
    float inv = antfly_row_rms_simd_inverse(x,base,p.dim,p.eps,uint(lane));
    uint half_dim = p.dim / 2u;
    for (uint d = lane; d < p.dim; d += 32u) {
        float value = x[base + d] * inv * (p.weighted ? w[d] : 1.0f);
        if (p.rotate) {
            uint left = d % half_dim, right = left + half_dim;
            uint position = p.gather == 2u ? uint(ids[row]) : src_row % p.sequence; float c, s;
            if (p.cached_rotary) { float2 value = rotary[position * half_dim + left]; c = value.x; s = value.y; }
            else { float frequency = 1.0f / pow(p.theta, float(2u * left) / float(p.dim)); float angle = float(position) * frequency; c = cos(angle); s = sin(angle); }
            float a = x[base + left] * inv * (p.weighted ? w[left] : 1.0f);
            float b = x[base + right] * inv * (p.weighted ? w[right] : 1.0f);
            value = d < half_dim ? antfly_rotary_component<false>(a,b,c,s) : antfly_rotary_component<true>(a,b,c,s);
        }
        y[head * p.dim + d] = value * p.scale;
    }
}
// native-program:entry termite_vision_norm_rope
// native-program:indent 11
kernel void termite_vision_norm_rope(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_vision_norm_rope_params &p [[buffer(3)]], device const float2 *rotary [[buffer(4)]], uint head [[threadgroup_position_in_grid]], uint lane [[thread_index_in_simdgroup]]) {
    uint heads = p.hidden / p.head_dim; uint row = head / heads; uint out_base = head * p.head_dim; uint base = row * (p.stride ? p.stride : p.hidden) + p.start + (head % heads) * p.head_dim;
    float inv = antfly_row_rms_simd_inverse(input,base,p.head_dim,p.eps,uint(lane));
    if (!p.rotate) { for (uint d=lane; d<p.head_dim; d+=32u) output[out_base+d]=input[base+d]*inv*(p.weighted ? weight[d] : 1.0f)*p.scale; return; }
    uint quarter = p.head_dim / 4u;
    for (uint pair = lane; pair < p.head_dim / 2u; pair += 32u) {
        uint4 rotary_pair=antfly_rotary_pair_indices<true,false>(pair,quarter); uint axis=rotary_pair.x,j=rotary_pair.y,a=rotary_pair.z,b=rotary_pair.w;
        float position = axis == 0u ? float(row % p.grid_x) : float((row / p.grid_x) % p.grid_y);
        float c, s;
        if (p.cached_rotary) { uint index = (axis == 0u ? uint(position) : p.grid_x + uint(position)) * quarter + j; float2 value = rotary[index]; c = value.x; s = value.y; }
        else { float angle = position / pow(p.theta, float(2u*j)/float(p.head_dim/2u)); c = cos(angle); s = sin(angle); }
        float x = input[base+a] * inv * (p.weighted ? weight[a] : 1.0f); float y = input[base+b] * inv * (p.weighted ? weight[b] : 1.0f);
        output[out_base+a] = antfly_rotary_component<false>(x,y,c,s)*p.scale;
        output[out_base+b] = antfly_rotary_component<true,true>(x,y,c,s)*p.scale;
    }
}
// native-program:entry termite_apply_mrope
// native-program:indent 11
kernel void termite_apply_mrope(device const float *input [[buffer(0)]], device const uint *positions [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_mrope_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.total_chunks * p.head_dim;
    if (gid >= total) return;
    uint chunk = gid / p.head_dim;
    uint d = gid - chunk * p.head_dim;
    uint half_dim = p.head_dim / 2u;
    uint pair = d < half_dim ? d : d - half_dim;
    uint axis = 0u;
    uint local_pair = pair;
    uint exponent_dim = p.head_dim;
    if (p.mode == 1u) {
        uint pairs_per_axis = half_dim / 2u;
        uint4 rotary_pair=antfly_rotary_pair_indices<false,true>(pair,pairs_per_axis);
        axis=rotary_pair.x;local_pair=rotary_pair.y;
        exponent_dim = half_dim;
    } else {
        if ((pair % 3u) == 1u && pair < p.section_h * 3u) axis = 1u;
        else if ((pair % 3u) == 2u && pair < p.section_w * 3u) axis = 2u;
    }
    uint token = chunk / p.heads_per_token;
    float pos = float(positions[axis * p.token_count + token]);
    float freq = 1.0f / pow(p.theta, float(2u * local_pair) / float(exponent_dim));
    float angle = pos * p.freq_scale * freq;
    float cos_val = cos(angle);
    float sin_val = sin(angle);
    uint base = chunk * p.head_dim;
    float x0 = input[base + pair];
    float x1 = input[base + half_dim + pair];
    output[gid] = d < half_dim ? antfly_rotary_component<false>(x0,x1,cos_val,sin_val) : antfly_rotary_component<true>(x0,x1,cos_val,sin_val);
}
