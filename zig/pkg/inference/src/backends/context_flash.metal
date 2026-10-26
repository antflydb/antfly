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

// ABI 2: per-head packed arenas and logical span descriptors. Matches the
// generated Params ABI and runtime/kv/context_view.zig exactly.
struct Params {
    uint op, n, dim, groups, position, stride, mode, index;
    float epsilon, scale, theta, reserved;
};
struct Span { uint physical_start, row_count, logical_start, logical_count, kind, bias_start; };
struct Head { uint kv_start, capacity_rows, bias_start, bias_rows, span_start, span_count; };

template<uint D, uint NT> struct FlashWorkspace {
    half q[8 * D];
    float scores[8 * 32];
    half probabilities[8 * 32];
    float maximum[8];
    float denominator[8];
    float correction[8 * 8];
    // Nonoverlapping SIMD-private staging for ragged K/V tiles and final
    // finite-output validation. Never aliases the live probabilities.
    float staging[(NT / 32) * 64];
};

template<uint D, uint NT>
inline void context_flash_impl(
    device const float *queries, device const half *keys,
    device const half *values, device const float *log_mass,
    device const Span *spans, device atomic_uint *status,
    device const Head *heads, device float *output,
    constant Params &p, threadgroup FlashWorkspace<D, NT> &w,
    uint tid, uint lane, uint sg, uint2 grid) {
    const uint q0 = grid.x * 8;
    const uint head = grid.y;
    if (p.op != 2 || p.dim != D || p.stride == 0 || p.groups % p.stride != 0 ||
        q0 >= p.n || head >= p.groups) return;
    const Head h = heads[head / (p.groups / p.stride)];
    const uint q_stride = p.groups * D;
    const uint first_query = p.position + q0;
    const uint last_query = p.position + min(q0 + 7, p.n - 1);
    const uint prefix = as_type<uint>(p.theta);
    constexpr uint NSG = NT / 32;
    const float absent = -3.402823466e+38f;
    for (uint i = tid; i < 8 * D; i += NT) {
        const uint row = i / D, d = i % D;
        const float q = q0 + row < p.n ? queries[(q0 + row) * q_stride + head * D + d] * p.scale : 0;
        const half packed = half(q);
        const bool finite = isfinite(q) && isfinite(float(packed));
        if (!finite) atomic_store_explicit(status, 1u, memory_order_relaxed);
        w.q[i] = finite ? packed : half(0);
    }
    if (tid < 8) { w.maximum[tid] = absent; w.denominator[tid] = 0; }
    if (tid < 64) w.correction[tid] = 0;
    simdgroup_float8x8 accumulated[8];
    for (uint tile = 0; tile < 8; ++tile)
        accumulated[tile] = make_filled_simdgroup_matrix<float, 8>(0);

    for (uint si = h.span_start; si < h.span_start + h.span_count; ++si) {
        const Span span = spans[si];
        const uint closed_end = span.logical_start + span.logical_count;
        // A compacted interval is only valid after its original closed range.
        // The host validates the whole request before any dispatch.
        if (span.kind == 1 && first_query < closed_end) {
            if (tid == 0) atomic_store_explicit(status, 1u, memory_order_relaxed);
            continue;
        }
        for (uint base = 0; base < span.row_count; base += 32) {
            const uint first_key = span.logical_start + base;
            const uint last_key = span.logical_start + min(base + 31, span.row_count - 1);
            if (span.kind == 0 && first_key > last_query) break;
            if (span.kind == 0 && p.mode != 0 && first_key >= prefix &&
                first_query >= last_key && first_query - last_key >= p.mode) continue;
            threadgroup_barrier(mem_flags::mem_threadgroup);
            // Four SIMD groups calculate the 8x32 score tile. A direct matrix
            // load is safe only inside this span's physical extent; ragged
            // tails use a fixed 8x8 staging area, never adjacent head storage.
            if (sg < 4) {
                simdgroup_float8x8 score = make_filled_simdgroup_matrix<float, 8>(0);
                const uint key0 = base + sg * 8;
                threadgroup half *scratch = reinterpret_cast<threadgroup half *>(w.staging + sg * 64);
                for (uint d = 0; d < D; d += 8) {
                    simdgroup_half8x8 mq, mk;
                    simdgroup_load(mq, w.q + d, D);
                    if (key0 + 8 <= span.row_count) {
                        const uint physical = h.kv_start + span.physical_start + key0;
                        simdgroup_load(mk, keys + physical * D + d, D, 0, true);
                    } else {
                        for (uint i = lane; i < 64; i += 32) {
                            const uint row = key0 + i / 8;
                            scratch[i] = row < span.row_count
                                ? keys[(h.kv_start + span.physical_start + row) * D + d + i % 8] : half(0);
                        }
                        simdgroup_barrier(mem_flags::mem_threadgroup);
                        simdgroup_load(mk, scratch, 8, 0, true);
                        simdgroup_barrier(mem_flags::mem_threadgroup);
                    }
                    simdgroup_multiply_accumulate(score, mq, mk, score);
                }
                simdgroup_store(score, w.scores + sg * 8, 32);
            }
            threadgroup_barrier(mem_flags::mem_threadgroup);
            for (uint row = sg; row < 8; row += NSG) {
                const uint query = first_query + row;
                const uint key_row = base + lane;
                bool allowed = q0 + row < p.n && key_row < span.row_count;
                if (allowed && span.kind == 0) {
                    const uint key = span.logical_start + key_row;
                    allowed = key <= query && (p.mode == 0 || key < prefix || query - key < p.mode);
                }
                float score = absent;
                if (allowed) {
                    score = w.scores[row * 32 + lane];
                    // Softcapping must not turn an overflow into an apparently
                    // valid finite score and hide a failed numerical input.
                    if (!isfinite(score)) {
                        atomic_store_explicit(status, 1u, memory_order_relaxed);
                        score = absent;
                    } else if (p.epsilon > 0) score = p.epsilon * tanh(score / p.epsilon);
                    // Mass bias belongs outside the model's logit softcap.
                    if (span.kind == 1) score += log_mass[h.bias_start + span.bias_start + key_row];
                    if (!isfinite(score)) {
                        atomic_store_explicit(status, 1u, memory_order_relaxed);
                        score = absent;
                    }
                }
                const float row_maximum = simd_max(score);
                const float old_maximum = w.maximum[row];
                const float new_maximum = max(old_maximum, row_maximum);
                float correction = 1, probability = 0;
                if (new_maximum > -3.0e+38f) {
                    correction = old_maximum > -3.0e+38f ? exp(old_maximum - new_maximum) : 0;
                    probability = score > -3.0e+38f ? exp(score - new_maximum) : 0;
                }
                w.probabilities[row * 32 + lane] = half(probability);
                const float row_sum = simd_sum(probability);
                if (lane == 0) {
                    w.denominator[row] = w.denominator[row] * correction + row_sum;
                    w.maximum[row] = new_maximum;
                    w.correction[row * 8 + row] = correction;
                }
            }
            threadgroup_barrier(mem_flags::mem_threadgroup);
            simdgroup_float8x8 correction;
            simdgroup_load(correction, w.correction, 8);
            for (uint tile = 0; tile < 8; ++tile) {
                simdgroup_float8x8 scaled;
                simdgroup_multiply(scaled, correction, accumulated[tile]);
                accumulated[tile] = scaled;
            }
            threadgroup half *scratch = reinterpret_cast<threadgroup half *>(w.staging + sg * 64);
            for (uint key_tile = 0; key_tile < 4; ++key_tile) {
                const uint key0 = base + key_tile * 8;
                simdgroup_half8x8 probabilities;
                simdgroup_load(probabilities, w.probabilities + key_tile * 8, 32);
                for (uint tile = 0; tile < 8; ++tile) {
                    const uint d = sg * 64 + tile * 8;
                    simdgroup_half8x8 value;
                    if (key0 + 8 <= span.row_count) {
                        const uint physical = h.kv_start + span.physical_start + key0;
                        simdgroup_load(value, values + physical * D + d, D);
                    } else {
                        for (uint i = lane; i < 64; i += 32) {
                            const uint row = key0 + i / 8;
                            scratch[i] = row < span.row_count
                                ? values[(h.kv_start + span.physical_start + row) * D + d + i % 8] : half(0);
                        }
                        simdgroup_barrier(mem_flags::mem_threadgroup);
                        simdgroup_load(value, scratch, 8);
                        simdgroup_barrier(mem_flags::mem_threadgroup);
                    }
                    simdgroup_multiply_accumulate(accumulated[tile], probabilities, value, accumulated[tile]);
                }
            }
        }
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (tid < 8) {
        const float denominator = w.denominator[tid];
        const bool valid = denominator > 0 && isfinite(denominator);
        if (!valid && q0 + tid < p.n) atomic_store_explicit(status, 1u, memory_order_relaxed);
        w.correction[tid * 8 + tid] = valid ? 1 / denominator : 0;
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    simdgroup_float8x8 inverse;
    simdgroup_load(inverse, w.correction, 8);
    threadgroup float *scratch = w.staging + sg * 64;
    for (uint tile = 0; tile < 8; ++tile) {
        simdgroup_float8x8 normalized;
        simdgroup_multiply(normalized, inverse, accumulated[tile]);
        simdgroup_store(normalized, scratch, 8);
        simdgroup_barrier(mem_flags::mem_threadgroup);
        for (uint i = lane; i < 64; i += 32) {
            const uint row = q0 + i / 8;
            if (row >= p.n) continue;
            const float value = scratch[i];
            const bool valid = isfinite(value);
            if (!valid) atomic_store_explicit(status, 1u, memory_order_relaxed);
            output[row * q_stride + head * D + sg * 64 + tile * 8 + i % 8] = valid ? value : 0;
        }
        simdgroup_barrier(mem_flags::mem_threadgroup);
    }
}

#define CONTEXT_FLASH_ENTRY(D, NT) \
kernel void context_flash_##D( \
    device const float *queries [[buffer(0)]], device const half *keys [[buffer(1)]], \
    device const half *values [[buffer(2)]], device const float *log_mass [[buffer(3)]], \
    device const Span *spans [[buffer(4)]], device atomic_uint *status [[buffer(5)]], \
    device const Head *heads [[buffer(6)]], device float *output [[buffer(7)]], \
    constant Params &p [[buffer(8)]], uint tid [[thread_index_in_threadgroup]], \
    uint lane [[thread_index_in_simdgroup]], uint sg [[simdgroup_index_in_threadgroup]], \
    uint2 grid [[threadgroup_position_in_grid]]) { \
    threadgroup FlashWorkspace<D, NT> workspace; \
    context_flash_impl<D, NT>(queries, keys, values, log_mass, spans, status, heads, output, p, workspace, tid, lane, sg, grid); \
}

CONTEXT_FLASH_ENTRY(256, 128)
CONTEXT_FLASH_ENTRY(512, 256)
