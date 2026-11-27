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

// native-program:entry antfly_rgb_patch_rows_bf16
struct PatchTransformerParams {
    uint rows, width, gx, gy, image_x, image_y;
    float epsilon;
};

kernel void antfly_rgb_patch_rows_bf16(
    device bfloat *out [[buffer(0)]], device const uchar *rgb [[buffer(1)]],
    constant PatchTransformerParams &p [[buffer(5)]],
    uint row [[threadgroup_position_in_grid]], uint lane [[thread_index_in_threadgroup]]) {
    constexpr uint patch = 20u, features = patch * patch * 3u;
    const float3 mean = float3(0.48145466f, 0.4578275f, 0.40821073f);
    const float3 processor_std = float3(0.26862954f, 0.2613026f, 0.2757771f);
    const float3 scale = float3(0.26862954f, 0.26130258f, 0.27577711f);
    const uint x = (row % p.gx) * patch, y = (row / p.gx) * patch;
    for (uint i = lane; i < features; i += 128u) {
        const uint px = x + i / 3u % patch, py = y + i / (3u * patch), c = i % 3u;
        const float v = px < p.image_x && py < p.image_y ? float(rgb[(ulong(py) * p.image_x + px) * 3u + c]) : -1.0f;
        const float normalized = float(bfloat((v / 255.0f - mean[c]) / processor_std[c]));
        out[ulong(row) * features + i] = bfloat(2.0f * (normalized * scale[c] + mean[c]) - 1.0f);
    }
}

inline float antfly_patch_lerp(float a, float b, float t) {
    return t < 0.5f ? fma(a, 1.0f - t, b * t) : fma(b, t, a * (1.0f - t));
}

// native-program:entry antfly_patch_position_bf16
kernel void antfly_patch_position_bf16(
    device bfloat *out [[buffer(0)]], device const bfloat *patch [[buffer(1)]],
    device const bfloat *pos [[buffer(2)]], device const bfloat *cls [[buffer(3)]],
    device const bfloat *cls_pos [[buffer(4)]], constant PatchTransformerParams &p [[buffer(5)]],
    uint row [[threadgroup_position_in_grid]], uint lane [[thread_index_in_threadgroup]]) {
    const float x = row == 0u ? 0.0f : clamp(32.0f * (float((row - 1u) % p.gx) + 0.5f) / float(p.gx) - 0.5f, 0.0f, 31.0f);
    const float y = row == 0u ? 0.0f : clamp(32.0f * (float((row - 1u) / p.gx) + 0.5f) / float(p.gy) - 0.5f, 0.0f, 31.0f);
    const uint x0 = uint(x), x1 = min(x0 + 1u, 31u), y0 = uint(y), y1 = min(y0 + 1u, 31u);
    for (uint i = lane; i < p.width; i += 128u) {
        float v;
        if (row == 0u) v = float(cls[i]) + float(cls_pos[i]);
        else {
            const float top = antfly_patch_lerp(float(pos[(y0 * 32u + x0) * p.width + i]), float(pos[(y0 * 32u + x1) * p.width + i]), x - float(x0));
            const float bottom = antfly_patch_lerp(float(pos[(y1 * 32u + x0) * p.width + i]), float(pos[(y1 * 32u + x1) * p.width + i]), x - float(x0));
            v = float(patch[ulong(row - 1u) * p.width + i]) + float(bfloat(antfly_patch_lerp(top, bottom, y - float(y0))));
        }
        out[ulong(row) * p.width + i] = bfloat(v);
    }
}

// native-program:entry antfly_patch_rotary_bf16
kernel void antfly_patch_rotary_bf16(
    device bfloat *q [[buffer(0)]], device bfloat *k [[buffer(1)]],
    constant PatchTransformerParams &p [[buffer(5)]],
    uint row [[threadgroup_position_in_grid]], uint lane [[thread_index_in_threadgroup]]) {
    if (row == 0u) return;
    const uint x = (row - 1u) % p.gx + 1u, y = (row - 1u) / p.gx + 1u;
    for (uint i = lane; i < p.width / 2u; i += 128u) {
        const uint pair = i % 32u;
        const float angle = float(pair < 16u ? x : y) * pow(10000.0f, -float(pair % 16u) / 16.0f);
        const float sn = sin(angle), cs = cos(angle);
        const ulong at = ulong(row) * p.width + i * 2u;
        const float qe = float(q[at]), qo = float(q[at + 1u]), ke = float(k[at]), ko = float(k[at + 1u]);
        q[at] = bfloat(fma(qe, cs, -qo * sn)); q[at + 1u] = bfloat(fma(qo, cs, qe * sn));
        k[at] = bfloat(fma(ke, cs, -ko * sn)); k[at + 1u] = bfloat(fma(ko, cs, ke * sn));
    }
}

// native-program:entry antfly_bidirectional_attention_d64_bf16
kernel void antfly_bidirectional_attention_d64_bf16(
    device bfloat *out [[buffer(0)]], device const bfloat *q [[buffer(1)]],
    device const bfloat *k [[buffer(2)]], device const bfloat *v [[buffer(3)]],
    constant PatchTransformerParams &p [[buffer(5)]], uint2 tg [[threadgroup_position_in_grid]],
    uint warp [[simdgroup_index_in_threadgroup]], uint lane [[thread_index_in_simdgroup]]) {
    const uint head = tg.y * 4u + warp;
    if (head >= p.width / 64u) return;
    const uint offset = head * 64u + lane;
    const ulong at = ulong(tg.x) * p.width + offset;
    const float q0 = float(q[at]), q1 = float(q[at + 32u]);
    float maximum = -INFINITY, mass = 0.0f, acc0 = 0.0f, acc1 = 0.0f;
    for (uint j = 0; j < p.rows; ++j) {
        const ulong i = ulong(j) * p.width + offset;
        const float score = simd_sum(fma(q0, float(k[i]), q1 * float(k[i + 32u]))) * 0.125f;
        const float next = max(score, maximum);
        const float old = exp(maximum - next), add = exp(score - next);
        acc0 = fma(add, float(v[i]), acc0 * old);
        acc1 = fma(add, float(v[i + 32u]), acc1 * old);
        mass = fma(mass, old, add);
        maximum = next;
    }
    out[at] = bfloat(acc0 / mass);
    out[at + 32u] = bfloat(acc1 / mass);
}

// native-program:entry antfly_layer_scale_residual_bf16
kernel void antfly_layer_scale_residual_bf16(
    device bfloat *hidden [[buffer(0)]], device const bfloat *update [[buffer(1)]],
    device const bfloat *gamma [[buffer(2)]], constant PatchTransformerParams &p [[buffer(5)]],
    uint i [[thread_position_in_grid]]) {
    if (i < p.rows * p.width)
        hidden[i] = bfloat(float(hidden[i]) + float(bfloat(float(update[i]) * float(gamma[i % p.width]))));
}

inline float antfly_vision_erf(float x) {
    const float a = abs(x), t = 1.0f / (1.0f + 0.3275911f * a);
    const float poly = (((((1.061405429f * t - 1.453152027f) * t) + 1.421413741f) * t - 0.284496736f) * t + 0.254829592f) * t;
    return copysign(1.0f - poly * exp(-a * a), x);
}

// native-program:entry antfly_erf_gelu_bf16
kernel void antfly_erf_gelu_bf16(
    device bfloat *values [[buffer(0)]], constant PatchTransformerParams &p [[buffer(5)]],
    uint i [[thread_position_in_grid]]) {
    if (i < p.rows * p.width) {
        const float x = float(values[i]);
        values[i] = bfloat(x * 0.5f * (1.0f + antfly_vision_erf(x * 0.7071067811865476f)));
    }
}

// native-program:entry antfly_merge_patch_rows_bf16
kernel void antfly_merge_patch_rows_bf16(
    device bfloat *out [[buffer(0)]], device const bfloat *hidden [[buffer(1)]],
    constant PatchTransformerParams &p [[buffer(5)]],
    uint row [[threadgroup_position_in_grid]], uint lane [[thread_index_in_threadgroup]]) {
    const uint half_x = p.gx / 2u;
    const uint first = 1u + 2u * (row / half_x) * p.gx + 2u * (row % half_x);
    for (uint i = lane; i < p.width * 4u; i += 128u) {
        const uint part = i / p.width;
        out[ulong(row) * p.width * 4u + i] = hidden[ulong(first + (part / 2u) * p.gx + part % 2u) * p.width + i % p.width];
    }
}

// native-program:entry antfly_layer_norm_bf16
kernel void antfly_layer_norm_bf16(
    device bfloat *out [[buffer(0)]], device const bfloat *input [[buffer(1)]],
    device const bfloat *gamma [[buffer(2)]], device const bfloat *beta [[buffer(3)]],
    constant PatchTransformerParams &p [[buffer(5)]], uint row [[threadgroup_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]], uint warp [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]) {
    threadgroup float sums[4];
    threadgroup float mean, inv;
    const ulong base = ulong(row) * p.width;
    float sum = 0.0f;
    for (uint i = tid; i < p.width; i += 128u) sum += float(input[base + i]);
    sum = simd_sum(sum);
    if (lane == 0u) sums[warp] = sum;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (tid == 0u) mean = ((sums[0] + sums[1]) + (sums[2] + sums[3])) / float(p.width);
    threadgroup_barrier(mem_flags::mem_threadgroup);
    sum = 0.0f;
    for (uint i = tid; i < p.width; i += 128u) {
        const float v = float(input[base + i]) - mean;
        sum = fma(v, v, sum);
    }
    sum = simd_sum(sum);
    if (lane == 0u) sums[warp] = sum;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (tid == 0u) inv = rsqrt(((sums[0] + sums[1]) + (sums[2] + sums[3])) / float(p.width) + p.epsilon);
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint i = tid; i < p.width; i += 128u) {
        const float centered = float(input[base + i]) - mean;
        out[base + i] = bfloat(fma(float(gamma[i]), centered * inv, float(beta[i])));
    }
}
