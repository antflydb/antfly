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
struct Head { uint kv_start, capacity_rows, bias_start, bias_rows, span_start, span_count; };

// Exact immutable packed base plus a private token-major f16 tail. No base KV
// copy, score matrix or host readback. The replay's absolute positions are
// supplied by the caller; only the private tail is writable.
kernel void context_replay(
    device const float *queries [[buffer(0)]],
    device const half *base_keys [[buffer(1)]],
    device const half *base_values [[buffer(2)]],
    device const half *tail_keys [[buffer(3)]],
    device const half *tail_values [[buffer(4)]],
    device atomic_uint *status [[buffer(5)]],
    device const Head *heads [[buffer(6)]],
    device float *output [[buffer(7)]],
    constant Params &p [[buffer(8)]],
    uint tid [[thread_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]) {
    if (tid >= p.n) return;
    if (p.op == 0) {
        // Append the producer's f32 projected KV, matching ordinary f16 KV
        // storage. Both tensors are copied before the tail count advances.
        device half *destination = (device half *)output;
        float value = queries[tid];
        half stored = half(value);
        if (!isfinite(value) || !isfinite(float(stored))) {
            atomic_store_explicit(status, 1u, memory_order_relaxed);
            stored = half(0);
        }
        destination[tid] = stored;
        return;
    }
    if (p.op == 2) {
        const uint split_count = as_type<uint>(p.reserved);
        const uint group = tid / 32;
        const uint split = group % split_count;
        const uint vector = group / split_count;
        const uint q_head = vector % p.groups;
        const uint query_position = p.position + vector / p.groups;
        const uint kv_head = q_head / (p.groups / p.stride);
        const uint query_base = vector * p.dim;
        const Head h = heads[kv_head];
        const ulong total = ulong(query_position) + 1ul;
        const uint begin = uint((total * ulong(split)) / ulong(split_count));
        const uint end = uint((total * ulong(split + 1u)) / ulong(split_count));
        float accum[16];
        for (uint i = 0; i < 16; ++i) accum[i] = 0;
        float maximum = -INFINITY, denominator = 0;
        for (uint pos = begin; pos < end; ++pos) {
            const bool original = pos < p.index;
            const uint base = original ? (h.kv_start + pos) * p.dim
                                       : ((pos - p.index) * p.stride + kv_head) * p.dim;
            device const half *keys = original ? base_keys : tail_keys;
            device const half *values = original ? base_values : tail_values;
            float dot = 0;
            for (uint i = 0; i < 16; ++i) {
                const uint d = lane + i * 32;
                if (d < p.dim) dot += queries[query_base + d] * float(keys[base + d]);
            }
            float score = simd_sum(dot) * p.scale;
            const bool invalid = !isfinite(score);
            if (p.epsilon > 0) score = p.epsilon * tanh(score / p.epsilon);
            if (invalid || !isfinite(score)) {
                atomic_store_explicit(status, 1u, memory_order_relaxed);
                score = 0;
            }
            const float next_max = max(maximum, score);
            const float previous_scale = exp(maximum - next_max);
            const float weight = exp(score - next_max);
            denominator = denominator * previous_scale + weight;
            for (uint i = 0; i < 16; ++i) {
                const uint d = lane + i * 32;
                if (d < p.dim) {
                    float value = float(values[base + d]);
                    if (!isfinite(value)) {
                        atomic_store_explicit(status, 1u, memory_order_relaxed);
                        value = 0;
                    }
                    accum[i] = accum[i] * previous_scale + weight * value;
                }
            }
            maximum = next_max;
        }
        const uint scratch_base = (vector * split_count + split) * (p.dim + 2u);
        for (uint i = 0; i < 16; ++i) {
            const uint d = lane + i * 32;
            if (d < p.dim) output[scratch_base + d] = accum[i];
        }
        if (lane == 0) {
            output[scratch_base + p.dim] = maximum;
            output[scratch_base + p.dim + 1u] = denominator;
        }
        return;
    }
    if (p.op == 3) {
        const uint split_count = as_type<uint>(p.reserved);
        const uint vector = tid / 32;
        const uint query_base = vector * p.dim;
        float maximum = -INFINITY;
        for (uint split = 0; split < split_count; ++split) {
            const uint scratch_base = (vector * split_count + split) * (p.dim + 2u);
            const float denominator = queries[scratch_base + p.dim + 1u];
            if (denominator > 0) maximum = max(maximum, queries[scratch_base + p.dim]);
        }
        float denominator = 0;
        float accum[16];
        for (uint i = 0; i < 16; ++i) accum[i] = 0;
        for (uint split = 0; split < split_count; ++split) {
            const uint scratch_base = (vector * split_count + split) * (p.dim + 2u);
            const float partial_denominator = queries[scratch_base + p.dim + 1u];
            if (!(partial_denominator > 0)) continue;
            const float factor = exp(queries[scratch_base + p.dim] - maximum);
            denominator += partial_denominator * factor;
            for (uint i = 0; i < 16; ++i) {
                const uint d = lane + i * 32;
                if (d < p.dim) accum[i] += queries[scratch_base + d] * factor;
            }
        }
        for (uint i = 0; i < 16; ++i) {
            const uint d = lane + i * 32;
            if (d < p.dim) {
                const float value = accum[i] / denominator;
                const bool valid = denominator > 0 && isfinite(denominator) && isfinite(value);
                if (!valid) atomic_store_explicit(status, 1u, memory_order_relaxed);
                output[query_base + d] = valid ? value : 0;
            }
        }
        return;
    }
    const uint q_head = tid / 32;
    const uint head = q_head % p.groups;
    const uint query_position = p.position + q_head / p.groups;
    const uint kv_head = head / (p.groups / p.stride);
    const uint query_base = q_head * p.dim;
    const Head h = heads[kv_head];
    float accum[16];
    for (uint i = 0; i < 16; ++i) accum[i] = 0;
    float maximum = -INFINITY, denominator = 0;
    for (uint pos = 0; pos <= query_position; ++pos) {
        const bool original = pos < p.index;
        const uint base = original ? (h.kv_start + pos) * p.dim
                                  : ((pos - p.index) * p.stride + kv_head) * p.dim;
        device const half *keys = original ? base_keys : tail_keys;
        device const half *values = original ? base_values : tail_values;
        float dot = 0;
        for (uint i = 0; i < 16; ++i) {
            const uint d = lane + i * 32;
            if (d < p.dim) dot += queries[query_base + d] * float(keys[base + d]);
        }
        float score = simd_sum(dot) * p.scale;
        const bool invalid = !isfinite(score);
        if (p.epsilon > 0) score = p.epsilon * tanh(score / p.epsilon);
        if (invalid || !isfinite(score)) {
            atomic_store_explicit(status, 1u, memory_order_relaxed);
            score = 0;
        }
        const float next_max = max(maximum, score);
        const float previous_scale = exp(maximum - next_max);
        const float weight = exp(score - next_max);
        denominator = denominator * previous_scale + weight;
        for (uint i = 0; i < 16; ++i) {
            const uint d = lane + i * 32;
            if (d < p.dim) {
                float value = float(values[base + d]);
                if (!isfinite(value)) {
                    atomic_store_explicit(status, 1u, memory_order_relaxed);
                    value = 0;
                }
                accum[i] = accum[i] * previous_scale + weight * value;
            }
        }
        maximum = next_max;
    }
    for (uint i = 0; i < 16; ++i) {
        const uint d = lane + i * 32;
        if (d < p.dim) {
            const float value = accum[i] / denominator;
            const bool valid = denominator > 0 && isfinite(denominator) && isfinite(value);
            if (!valid) atomic_store_explicit(status, 1u, memory_order_relaxed);
            output[query_base + d] = valid ? value : 0;
        }
    }
}
