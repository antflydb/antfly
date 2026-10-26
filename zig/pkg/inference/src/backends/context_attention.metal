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

// Existing generated-dispatch ABI; fields are mapped in context_kernels.zig.
struct Params {
    uint op, n, dim, groups, position, stride, mode, index;
    float epsilon, scale, theta, reserved;
};
// ABI 1 spans; ABI 2 adds per-head arena and span ranges. Mirrored in context_view.zig.
struct Span {
    uint physical_start, row_count, logical_start, logical_count, kind, bias_start;
};

struct Head { uint kv_start, capacity_rows, bias_start, bias_rows, span_start, span_count; };

// One SIMD group per query/head. FP32 online softmax uses O(head_dim)
// registers, independent of context length, and never materializes KV or an
// O(context) score vector. This is a correctness kernel pending optimized
// split/flash integration and model-level qualification.
kernel void context_attention(
    device const float *queries [[buffer(0)]],
    device const half *keys [[buffer(1)]],
    device const half *values [[buffer(2)]],
    device const float *log_mass [[buffer(3)]],
    device const Span *spans [[buffer(4)]],
    device atomic_uint *status [[buffer(5)]],
    device const Head *heads [[buffer(6)]],
    device float *output [[buffer(7)]],
    constant Params &p [[buffer(8)]],
    uint tid [[thread_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]) {
    if (tid >= p.n) return;
    const bool split_stage = p.op == 3 || p.op == 4;
    const bool split_reduce = p.op == 5 || p.op == 6;
    const uint split_count = as_type<uint>(p.reserved);
    if (split_reduce) {
        const uint q_head = tid / 32;
        if (q_head >= p.groups || split_count == 0 || split_count > 32) return;
        // Split is the innermost dimension so the SIMD group reads partial
        // statistics and each output component from contiguous addresses.
        const uint values_count = p.groups * p.dim * split_count;
        const uint stats_base = values_count + q_head * 2 * split_count;
        const float partial_sum = lane < split_count ? queries[stats_base + lane] : 0;
        const float partial_max = lane < split_count ? queries[stats_base + split_count + lane] : -INFINITY;
        const bool valid_stats = lane >= split_count ||
            ((partial_sum == 0 || (partial_sum > 0 && isfinite(partial_sum))) &&
             (partial_sum == 0 || isfinite(partial_max)));
        if (!valid_stats) atomic_store_explicit(status, 1u, memory_order_relaxed);
        const float local_max = valid_stats ? partial_max : -INFINITY;
        const float maximum = simd_max(local_max);
        const uint merge_active = lane < split_count && valid_stats && partial_sum > 0 ? 1u : 0u;
        const float merge_weight = merge_active != 0 ? exp(partial_max - maximum) : 0;
        const float denominator = simd_sum(partial_sum * merge_weight);
        // Every lane executes every collective, including the final partial
        // dimension group. Value validity only masks arithmetic and stores.
        for (uint d_base = 0; d_base < p.dim; d_base += 32) {
            const uint d = d_base + lane;
            const bool dimension_active = d < p.dim;
            float numerator = 0;
            for (uint split = 0; split < split_count; ++split) {
                const uint split_active = simd_broadcast(merge_active, split);
                const float split_weight = simd_broadcast(merge_weight, split);
                const float value = dimension_active
                    ? queries[(q_head * p.dim + d) * split_count + split] : 0;
                const bool valid_value = !dimension_active || isfinite(value);
                if (!valid_value) atomic_store_explicit(status, 1u, memory_order_relaxed);
                if (dimension_active && valid_value && split_active != 0)
                    numerator += value * split_weight;
            }
            const float value = numerator / denominator;
            const bool valid = denominator > 0 && isfinite(denominator) && isfinite(value);
            if (dimension_active && !valid) atomic_store_explicit(status, 1u, memory_order_relaxed);
            if (dimension_active) output[q_head * p.dim + d] = valid ? value : 0;
        }
        return;
    }
    if (split_stage) {
        const uint group = tid / 32;
        if (split_count == 0 || split_count > 32) return;
        const uint q_head = group / split_count;
        const uint split = group - q_head * split_count;
        if (q_head >= p.groups) return;
        const uint head = q_head % p.groups;
        const uint kv_head = head / (p.groups / p.stride);
        const uint query_base = q_head * p.dim;
        const bool packed = p.op == 4;
        const Head h = packed ? heads[kv_head] : Head{0, 0, 0, 0, 0, p.index};
        uint total_rows = 0;
        for (uint si = h.span_start; si < h.span_start + h.span_count; ++si)
            total_rows += spans[si].row_count;
        const uint rows_per_split = total_rows / split_count;
        const uint remainder = total_rows - rows_per_split * split_count;
        const uint first = split * rows_per_split + min(split, remainder);
        const uint last = first + rows_per_split + (split < remainder ? 1u : 0u);
        float accum[16];
        for (uint i = 0; i < 16; ++i) accum[i] = 0;
        float maximum = -INFINITY, denominator = 0;
        uint ordinal = 0;
        for (uint si = h.span_start; si < h.span_start + h.span_count; ++si) {
            const Span s = spans[si];
            const uint span_first = ordinal;
            const uint span_last = ordinal + s.row_count;
            ordinal = span_last;
            if (span_last <= first || span_first >= last) continue;
            const uint row_first = max(first, span_first) - span_first;
            const uint row_last = min(last, span_last) - span_first;
            // A compacted block has no internal causal semantics. It is only
            // published for queries beyond its original closed age interval.
            if (s.kind == 1 && p.position < s.logical_start + s.logical_count) continue;
            for (uint row = row_first; row < row_last; ++row) {
                if (s.kind == 0) {
                    const uint pos = s.logical_start + row;
                    if (pos > p.position) continue;
                    if (p.mode != 0 && pos >= as_type<uint>(p.theta) && p.position - pos >= p.mode) continue;
                }
                const uint base = packed ? (h.kv_start + s.physical_start + row) * p.dim
                                         : ((s.physical_start + row) * p.stride + kv_head) * p.dim;
                float dot = 0;
                for (uint i = 0; i < 16; ++i) {
                    const uint d = lane + i * 32;
                    if (d < p.dim) dot += queries[query_base + d] * float(keys[base + d]);
                }
                float score = simd_sum(dot) * p.scale;
                const bool invalid_score = !isfinite(score);
                if (p.epsilon > 0) score = p.epsilon * tanh(score / p.epsilon);
                if (s.kind == 1) score += log_mass[packed ? h.bias_start + s.bias_start + row : (s.bias_start + row) * p.stride + kv_head];
                if (invalid_score || !isfinite(score)) {
                    if (lane == 0) atomic_store_explicit(status, 1u, memory_order_relaxed);
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
        }
        const uint values_count = p.groups * p.dim * split_count;
        const uint stats_base = values_count + q_head * 2 * split_count;
        if (lane == 0) {
            output[stats_base + split] = denominator;
            output[stats_base + split_count + split] = maximum;
        }
        for (uint i = 0; i < 16; ++i) {
            const uint d = lane + i * 32;
            if (d < p.dim) output[(q_head * p.dim + d) * split_count + split] = accum[i];
        }
        return;
    }
    const uint q_head = tid / 32;
    const uint head = q_head % p.groups;
    const uint query_position = p.position + q_head / p.groups;
    const uint kv_head = head / (p.groups / p.stride);
    const uint query_base = q_head * p.dim;
    const bool packed = p.op == 2;
    const Head h = packed ? heads[kv_head] : Head{0, 0, 0, 0, 0, p.index};
    float accum[16];
    for (uint i = 0; i < 16; ++i) accum[i] = 0;
    float maximum = -INFINITY, denominator = 0;
    for (uint si = h.span_start; si < h.span_start + h.span_count; ++si) {
        const Span s = spans[si];
        // A compacted block has no internal causal semantics. It is only
        // published for queries beyond its original closed age interval.
        if (s.kind == 1 && query_position < s.logical_start + s.logical_count) continue;
        for (uint row = 0; row < s.row_count; ++row) {
            if (s.kind == 0) {
                const uint pos = s.logical_start + row;
                if (pos > query_position) continue;
                if (p.mode != 0 && pos >= as_type<uint>(p.theta) && query_position - pos >= p.mode) continue;
            }
            const uint base = packed ? (h.kv_start + s.physical_start + row) * p.dim
                                     : ((s.physical_start + row) * p.stride + kv_head) * p.dim;
            float dot = 0;
            for (uint i = 0; i < 16; ++i) {
                const uint d = lane + i * 32;
                if (d < p.dim) dot += queries[query_base + d] * float(keys[base + d]);
            }
            float score = simd_sum(dot) * p.scale;
            const bool invalid_score = !isfinite(score);
            if (p.epsilon > 0) score = p.epsilon * tanh(score / p.epsilon);
            if (s.kind == 1) score += log_mass[packed ? h.bias_start + s.bias_start + row : (s.bias_start + row) * p.stride + kv_head];
            if (invalid_score || !isfinite(score)) {
                if (lane == 0) atomic_store_explicit(status, 1u, memory_order_relaxed);
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
