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

#include <metal_stdlib>
using namespace metal;

struct Params {
    uint op, n, dim, groups, position, stride, mode, index;
    float epsilon, scale, theta, reserved;
};
struct Metadata { float bias; uint position_lo, position_hi, coverage_lo, coverage_hi; };
struct Fp8Header { uint first_row, rows; float key_scale, value_scale; };

constant uint split_rows = 256u;

inline float fp8_decode(uchar bits, float scale, device atomic_uint *status) {
    // E4M3FN reserves both signed exponent/mantissa 0x7f encodings. They must
    // fail the dispatch rather than alias finite +/-480 values.
    if ((bits & 0x7fu) == 0x7fu) {
        atomic_store_explicit(status, 1u, memory_order_relaxed);
        return as_type<float>(0x7fc00000u);
    }
    const uint exponent = (bits >> 3) & 15u;
    const uint mantissa = bits & 7u;
    const float magnitude = exponent == 0u
        ? float(mantissa) * (1.0f / 512.0f)
        : as_type<float>(((exponent + 120u) << 23) | (mantissa << 20));
    const float decoded = (bits & 128u) != 0u ? -magnitude * scale : magnitude * scale;
    return float(half(decoded));
}

// One SIMD group owns one (query head, layout split). The host dispatches the
// same max-head split count for every ragged KV head. Splits beyond this
// head's local row count stay zero from the host clear and are ignored by the
// reducer. Scratch records contain dim unnormalised numerator values followed
// by denominator and local maximum.
kernel void metal_attention_match_split_stage(
    device const float *queries [[buffer(0)]],
    device const half *keys [[buffer(1)]],
    device const half *values [[buffer(2)]],
    device const uchar *encoded_keys [[buffer(3)]],
    device const uchar *encoded_values [[buffer(4)]],
    device const Metadata *metadata [[buffer(5)]],
    device atomic_uint *status [[buffer(6)]],
    device float *partials [[buffer(7)]],
    constant Params &p [[buffer(8)]],
    uint3 group_position [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_threadgroup]]) {
    if (lane >= 32u || p.n == 0u) return;
    const uint layout_splits = p.n;
    const uint split = group_position.x % layout_splits;
    const uint query_kv_ordinal = group_position.x / layout_splits;
    const uint gqa = p.stride >> 16;
    const uint first_head = p.stride & 0xffffu;
    if (gqa == 0u || query_kv_ordinal >= gqa) return;
    const uint local_splits = (p.index + split_rows - 1u) / split_rows;
    if (split >= local_splits) return;

    const uint head = first_head + query_kv_ordinal;
    const uint query_base = head * p.dim;
    const ulong query_position = ulong(p.position);
    const uint ring_start = as_type<uint>(p.theta);
    const uint capacity = as_type<uint>(p.reserved);
    const uint row_begin = split * split_rows;
    const uint row_end = min(p.index, row_begin + split_rows);
    const Fp8Header fp8 = *reinterpret_cast<device const Fp8Header *>(encoded_keys);

    float accum[16];
    for (uint i = 0; i < 16u; ++i) accum[i] = 0.0f;
    float maximum = -INFINITY;
    float denominator = 0.0f;
    for (uint row = row_begin; row < row_end; ++row) {
        const uint physical = (ring_start + row) % capacity;
        const Metadata meta = metadata[physical];
        const ulong position = ulong(meta.position_lo) | (ulong(meta.position_hi) << 32);
        const ulong coverage = ulong(meta.coverage_lo) | (ulong(meta.coverage_hi) << 32);
        if (position > query_position || coverage == 0u || coverage > query_position + 1u) continue;
        if (p.mode != 0u && query_position - position >= p.mode) continue;

        const bool encoded = physical >= fp8.first_row && physical < fp8.first_row + fp8.rows;
        const uint storage_row = encoded ? physical - fp8.first_row
            : (physical < fp8.first_row ? physical : physical - fp8.rows);
        const uint base = storage_row * p.dim;
        float dot = 0.0f;
        for (uint i = 0; i < 16u; ++i) {
            const uint d = lane + i * 32u;
            if (d < p.dim) {
                const float key = encoded
                    ? fp8_decode(encoded_keys[sizeof(Fp8Header) + base + d], fp8.key_scale, status)
                    : float(keys[base + d]);
                dot += queries[query_base + d] * key;
            }
        }
        float score = simd_sum(dot) * p.scale;
        if (!isfinite(score)) {
            if (lane == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
            score = 0.0f;
        }
        if (p.epsilon > 0.0f) score = p.epsilon * tanh(score / p.epsilon);
        // Fitted log mass is outside the model softcap, matching the serial
        // installed-attention kernel.
        score += meta.bias;
        if (!isfinite(score)) {
            if (lane == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
            score = 0.0f;
        }
        const float next_maximum = max(maximum, score);
        const float previous_scale = exp(maximum - next_maximum);
        const float weight = exp(score - next_maximum);
        denominator = denominator * previous_scale + weight;
        for (uint i = 0; i < 16u; ++i) {
            const uint d = lane + i * 32u;
            if (d < p.dim) {
                float value = encoded
                    ? fp8_decode(encoded_values[base + d], fp8.value_scale, status)
                    : float(values[base + d]);
                if (!isfinite(value)) {
                    atomic_store_explicit(status, 1u, memory_order_relaxed);
                    value = 0.0f;
                }
                accum[i] = accum[i] * previous_scale + weight * value;
            }
        }
        maximum = next_maximum;
    }

    const uint record = (head * layout_splits + split) * (p.dim + 2u);
    for (uint i = 0; i < 16u; ++i) {
        const uint d = lane + i * 32u;
        if (d < p.dim) partials[record + d] = accum[i];
    }
    if (lane == 0u) {
        partials[record + p.dim] = denominator;
        partials[record + p.dim + 1u] = maximum;
    }
}

// One SIMD group merges one query head's fixed-layout records in ascending
// split order. Empty records (including ragged-head tail splits) have a zero
// denominator and do not participate.
kernel void metal_attention_match_split_reduce(
    device const float *partials [[buffer(0)]],
    device float *log_mass [[buffer(1)]],
    device atomic_uint *status [[buffer(2)]],
    device const float *unused3 [[buffer(3)]],
    device const float *unused4 [[buffer(4)]],
    device const float *unused5 [[buffer(5)]],
    device const float *unused6 [[buffer(6)]],
    device float *output [[buffer(7)]],
    constant Params &p [[buffer(8)]],
    uint3 group_position [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_threadgroup]]) {
    const uint ordinal = group_position.x;
    if (lane >= 32u || ordinal >= p.n || p.index == 0u) return;
    const uint record_width = p.dim + 2u;
    float maximum = -INFINITY;
    for (uint split = 0; split < p.index; ++split) {
        const uint record = (ordinal * p.index + split) * record_width;
        const float partial_denominator = partials[record + p.dim];
        const float partial_maximum = partials[record + p.dim + 1u];
        if (!isfinite(partial_denominator) || partial_denominator < 0.0f ||
            (partial_denominator > 0.0f && !isfinite(partial_maximum))) {
            if (lane == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
            continue;
        }
        if (partial_denominator > 0.0f) maximum = max(maximum, partial_maximum);
    }

    float denominator = 0.0f;
    float accum[16];
    for (uint i = 0; i < 16u; ++i) accum[i] = 0.0f;
    for (uint split = 0; split < p.index; ++split) {
        const uint record = (ordinal * p.index + split) * record_width;
        const float partial_denominator = partials[record + p.dim];
        const float partial_maximum = partials[record + p.dim + 1u];
        if (partial_denominator <= 0.0f || !isfinite(partial_denominator) || !isfinite(partial_maximum)) continue;
        const float weight = exp(partial_maximum - maximum);
        denominator += partial_denominator * weight;
        for (uint i = 0; i < 16u; ++i) {
            const uint d = lane + i * 32u;
            if (d < p.dim) accum[i] += partials[record + d] * weight;
        }
    }

    const bool valid = isfinite(maximum) && isfinite(denominator) && denominator > 0.0f;
    if (!valid && lane == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
    for (uint i = 0; i < 16u; ++i) {
        const uint d = lane + i * 32u;
        if (d < p.dim) {
            const float value = valid ? accum[i] / denominator : 0.0f;
            if (!isfinite(value)) atomic_store_explicit(status, 1u, memory_order_relaxed);
            output[ordinal * p.dim + d] = isfinite(value) ? value : 0.0f;
        }
    }
    if (lane == 0u && (p.mode & 1u) != 0u)
        log_mass[ordinal] = valid ? log(denominator) + maximum : 0.0f;
}
