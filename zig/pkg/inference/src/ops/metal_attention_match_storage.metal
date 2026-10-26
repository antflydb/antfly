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

inline float fp8_decode(uchar bits, float scale, device atomic_uint *status) {
    // E4M3FN reserves both signed encodings with exponent/mantissa 0x7f.
    // Match the common checkpoint codec: never reinterpret them as finite
    // +/-480 values, and make the dispatch fail even when later arithmetic
    // would otherwise mask the non-finite value.
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
    // Common V3 codec validation requires every decoded FP8 value to be
    // exactly representable by serving f16. Preserve that rounding contract.
    return float(half(decoded));
}

// op 1: append one KV head from token-major f32 input into an f16 ring.
// `stride` is the source KV-head ordinal, `groups` the source KV-head count,
// `mode` the destination capacity and `index` the currently published rows.
kernel void metal_attention_match_storage(
    device const float *source_keys [[buffer(0)]],
    device float *source_values [[buffer(1)]],
    device half *keys [[buffer(2)]],
    device half *values [[buffer(3)]],
    device uchar *encoded_keys [[buffer(4)]],
    device uchar *encoded_values [[buffer(5)]],
    device Metadata *metadata [[buffer(6)]],
    device atomic_uint *status [[buffer(7)]],
    constant Params &p [[buffer(8)]],
    uint tid [[thread_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]) {
    if (tid >= p.n) return;
    if (p.op == 1) {
        const uint row = tid / p.dim;
        const uint d = tid - row * p.dim;
        const uint source = (row * p.groups + p.stride) * p.dim + d;
        const Fp8Header fp8 = *reinterpret_cast<device const Fp8Header *>(encoded_keys);
        const uint physical = (as_type<uint>(p.theta) + p.index + row) % p.mode;
        const uint exact_row = physical < fp8.first_row ? physical : physical - fp8.rows;
        const uint destination = exact_row * p.dim + d;
        const float k = source_keys[source];
        const float v = source_values[source];
        const half hk = half(k);
        const half hv = half(v);
        if (!isfinite(k) || !isfinite(v) || fabs(k) > 65504.0f || fabs(v) > 65504.0f) {
            atomic_store_explicit(status, 1u, memory_order_relaxed);
            return;
        }
        keys[destination] = hk;
        values[destination] = hv;
        if (d == 0) {
            const ulong position = ulong(p.position) + row;
            const ulong coverage = position + 1;
            metadata[physical] = Metadata{0, uint(position), uint(position >> 32), uint(coverage), uint(coverage >> 32)};
        }
        return;
    }

    // op 2: one SIMD group per query head. `stride` is the first query head
    // mapped to this KV head, `mode` is its local window, `index` is row count,
    // theta/reserved bitcast the ring start/capacity. Output is private scratch
    // with dim values followed by denominator and maximum for each query head.
    if (p.op != 2) return;
    const uint group = tid / 32;
    const uint gqa = p.stride >> 16;
    const uint first_head = p.stride & 0xffffu;
    if (gqa == 0) return;
    const uint query_row = group / gqa;
    const uint head = first_head + group - query_row * gqa;
    const ulong query_position = ulong(p.position) + query_row;
    const uint query_base = (query_row * p.groups + head) * p.dim;
    const uint start = as_type<uint>(p.theta);
    const uint capacity = as_type<uint>(p.reserved);
    float accum[32];
    for (uint i = 0; i < 32; ++i) accum[i] = 0;
    float maximum = -INFINITY;
    float denominator = 0;
    for (uint row = 0; row < p.index; ++row) {
        const uint physical = (start + row) % capacity;
        const Metadata meta = metadata[physical];
        const ulong pos = ulong(meta.position_lo) | (ulong(meta.position_hi) << 32);
        const ulong end = ulong(meta.coverage_lo) | (ulong(meta.coverage_hi) << 32);
        if (pos > query_position || end == 0 || end > query_position + 1) continue;
        if (p.mode != 0 && query_position - pos >= p.mode) continue;
        const Fp8Header fp8 = *reinterpret_cast<device const Fp8Header *>(encoded_keys);
        const bool compressed = physical >= fp8.first_row && physical < fp8.first_row + fp8.rows;
        const uint storage_row = compressed ? physical - fp8.first_row
                                            : (physical < fp8.first_row ? physical : physical - fp8.rows);
        const uint base = storage_row * p.dim;
        float dot = 0;
        for (uint i = 0; i < 32; ++i) {
            const uint d = lane + i * 32;
            if (d < p.dim) {
                const float key = compressed
                    ? fp8_decode(encoded_keys[sizeof(Fp8Header) + base + d], fp8.key_scale, status)
                    : float(keys[base + d]);
                dot += source_keys[query_base + d] * key;
            }
        }
        float score = simd_sum(dot) * p.scale;
        // Match the native attention contract: overflow is invalid even when
        // a finite softcap would otherwise turn infinity into a finite value.
        if (!isfinite(score)) {
            if (lane == 0) atomic_store_explicit(status, 1u, memory_order_relaxed);
            score = 0;
        }
        if (p.epsilon > 0) score = p.epsilon * tanh(score / p.epsilon);
        // Fitted mass is outside the model logit softcap.
        score += meta.bias;
        if (!isfinite(score)) {
            if (lane == 0) atomic_store_explicit(status, 1u, memory_order_relaxed);
            score = 0;
        }
        const float next_max = max(maximum, score);
        const float previous_scale = exp(maximum - next_max);
        const float weight = exp(score - next_max);
        denominator = denominator * previous_scale + weight;
        for (uint i = 0; i < 32; ++i) {
            const uint d = lane + i * 32;
            if (d < p.dim) {
                float value = compressed
                    ? fp8_decode(encoded_values[base + d], fp8.value_scale, status)
                    : float(values[base + d]);
                if (!isfinite(value)) {
                    atomic_store_explicit(status, 1u, memory_order_relaxed);
                    value = 0;
                }
                accum[i] = accum[i] * previous_scale + weight * value;
            }
        }
        maximum = next_max;
    }
    const uint ordinal = query_row * p.groups + head;
    const uint scratch_base = ordinal * (p.dim + 2);
    for (uint i = 0; i < 32; ++i) {
        const uint d = lane + i * 32;
        if (d < p.dim) {
            const float value = accum[i] / denominator;
            if (!isfinite(value) || denominator <= 0) atomic_store_explicit(status, 1u, memory_order_relaxed);
            source_values[scratch_base + d] = isfinite(value) && denominator > 0 ? value : 0;
        }
    }
    if (lane == 0) {
        source_values[scratch_base + p.dim] = denominator;
        source_values[scratch_base + p.dim + 1] = maximum;
    }
}

// Separate copy kernels let the attention stage retain both output and log-mass
// while staying within the generated-dispatch eight-buffer ABI.
kernel void metal_attention_match_copy(
    device const float *scratch [[buffer(0)]],
    device const float *unused1 [[buffer(1)]],
    device const float *unused2 [[buffer(2)]],
    device const float *unused3 [[buffer(3)]],
    device const float *unused4 [[buffer(4)]],
    device const float *unused5 [[buffer(5)]],
    device const float *unused6 [[buffer(6)]],
    device float *output [[buffer(7)]],
    constant Params &p [[buffer(8)]],
    uint tid [[thread_position_in_grid]]) {
    if (tid >= p.n) return;
    if (p.op == 3) {
        const uint row = tid / p.dim;
        const uint d = tid - row * p.dim;
        output[tid] = scratch[row * (p.dim + 2) + d];
    } else if (p.op == 4) {
        const float denominator = scratch[tid * (p.dim + 2) + p.dim];
        const float maximum = scratch[tid * (p.dim + 2) + p.dim + 1];
        output[tid] = log(denominator) + maximum;
    }
}
