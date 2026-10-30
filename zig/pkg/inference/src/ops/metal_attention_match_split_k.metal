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
struct StorageHeader {
    uint encoding, first_row, rows, span_count, compressed_rows;
    uint key_payload_offset, key_row_bytes, value_row_bytes;
    float key_scale, value_scale;
    uint reserved0, reserved1;
};
struct StorageSpan { uint first, rows, offset; };

// Nonnegative means a packed row; negative means -(exact row + 1).
inline int storage_index(device const uchar *encoded, StorageHeader h, uint row) {
    if (h.encoding == 1u) {
        if (row >= h.first_row && row - h.first_row < h.rows)
            return int(row - h.first_row);
        return -int(row < h.first_row ? row : row - h.rows) - 1;
    }
    if (h.encoding == 2u) {
        device const StorageSpan *spans = reinterpret_cast<device const StorageSpan *>(encoded + sizeof(StorageHeader));
        uint low = 0u, high = h.span_count;
        while (low < high) {
            const uint mid = low + (high - low) / 2u;
            if (spans[mid].first <= row) low = mid + 1u; else high = mid;
        }
        if (low != 0u) {
            const StorageSpan span = spans[low - 1u];
            if (row - span.first < span.rows) return int(span.offset + row - span.first);
            return -int(row - (span.offset + span.rows)) - 1;
        }
    }
    return -int(row) - 1;
}

constant float rotated_centroids[16] = {
    -2.73258957099535f, -2.06901722653160f, -1.61804638602210f, -1.25623119734739f,
    -0.94234045648714f, -0.65675911853260f, -0.38804829949038f, -0.12839502985118f,
     0.12839502985118f,  0.38804829949038f,  0.65675911853260f,  0.94234045648714f,
     1.25623119734739f,  1.61804638602210f,  2.06901722653160f,  2.73258957099535f
};
inline float rotated_sign(uint round, uint coordinate) {
    ulong x = (round == 0u ? 0x8f3f73b5cf1c9adeul : 0x24d8a6511b73c409ul)
        + ulong(coordinate) * 0x9e3779b97f4a7c15ul;
    x = (x ^ (x >> 30u)) * 0xbf58476d1ce4e5b9ul;
    x = (x ^ (x >> 27u)) * 0x94d049bb133111ebul;
    return ((x ^ (x >> 31u)) & 1ul) == 0ul ? 1.0f : -1.0f;
}

// SIMD lanes own coordinates lane + 32*i. All butterflies preserve v1's
// f32 operation order, including pair - self in the upper half. Rotate once
// per query and retain the original query for protected/exact rows.
inline void rotate_query(device const float *query, uint dim, uint lane,
                         thread float *rotated, device atomic_uint *status) {
    const uint count = dim / 32u;
    for (uint i = 0; i < count; ++i) rotated[i] = query[lane + i * 32u];
    const float normalization = 1.0f / sqrt(float(dim));
    for (uint round = 0; round < 2u; ++round) {
        for (uint i = 0; i < count; ++i) rotated[i] *= rotated_sign(round, lane + i * 32u);
        for (uint width = 1u; width < 32u; width *= 2u) {
            for (uint i = 0; i < count; ++i) {
                const float own = rotated[i];
                const float pair = simd_shuffle_xor(own, ushort(width));
                rotated[i] = (lane & width) != 0u ? pair - own : own + pair;
            }
        }
        for (uint width = 1u; width < count; width *= 2u) {
            for (uint i = 0; i < count; ++i) if ((i & width) == 0u) {
                const float left = rotated[i], right = rotated[i + width];
                rotated[i] = left + right;
                rotated[i + width] = left - right;
            }
        }
        for (uint i = 0; i < count; ++i) {
            rotated[i] *= normalization;
            if (!isfinite(rotated[i])) atomic_store_explicit(status, 1u, memory_order_relaxed);
        }
    }
}
inline float packed_scale(device const uchar *payload, device atomic_uint *status, bool allow_zero) {
    const float scale = *reinterpret_cast<device const float *>(payload);
    if (!isfinite(scale) || scale < 0.0f || (!allow_zero && scale == 0.0f)) {
        atomic_store_explicit(status, 1u, memory_order_relaxed);
        return 0.0f;
    }
    return scale;
}
inline float rotated_key(device const uchar *encoded, StorageHeader h, uint row, uint d) {
    const uint offset = h.key_payload_offset + row * h.key_row_bytes;
    const uint code = (encoded[offset + 4u + d / 2u] >> ((d & 1u) * 4u)) & 15u;
    return rotated_centroids[code];
}
inline float rotated_value(device const uchar *encoded, StorageHeader h, uint row, uint d,
                           device atomic_uint *status) {
    const uint offset = row * h.value_row_bytes;
    const char code = as_type<char>(encoded[offset + 4u + d]);
    if (code == -128) atomic_store_explicit(status, 1u, memory_order_relaxed);
    return float(code) * packed_scale(encoded + offset, status, false);
}

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
    const uint rows_per_split = p.op == 2u ? 32u : split_rows;
    const uint local_splits = (p.index + rows_per_split - 1u) / rows_per_split;
    if (split >= local_splits) return;

    const uint head = first_head + query_kv_ordinal;
    const uint query_base = head * p.dim;
    const ulong query_position = ulong(p.position);
    const uint ring_start = as_type<uint>(p.theta);
    const uint capacity = as_type<uint>(p.reserved);
    const uint row_begin = split * rows_per_split;
    const uint row_end = min(p.index, row_begin + rows_per_split);
    const StorageHeader storage = *reinterpret_cast<device const StorageHeader *>(encoded_keys);

    float rotated[16];
    if (storage.encoding == 2u) rotate_query(queries + query_base, p.dim, lane, rotated, status);
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

        const int index = storage_index(encoded_keys, storage, physical);
        const bool encoded = index >= 0;
        const bool rotated_row = encoded && storage.encoding == 2u;
        const uint storage_row = encoded ? uint(index) : uint(-index - 1);
        const uint base = storage_row * p.dim;
        float dot = 0.0f;
        for (uint i = 0; i < 16u; ++i) {
            const uint d = lane + i * 32u;
            if (d < p.dim) {
                const float key = rotated_row ? rotated_key(encoded_keys, storage, storage_row, d)
                    : (encoded ? fp8_decode(encoded_keys[storage.key_payload_offset + base + d], storage.key_scale, status)
                               : float(keys[base + d]));
                dot += (rotated_row ? rotated[i] : queries[query_base + d]) * key;
            }
        }
        float score = simd_sum(dot);
        if (rotated_row) score *= packed_scale(encoded_keys + storage.key_payload_offset + storage_row * storage.key_row_bytes, status, true);
        score *= p.scale;
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
                float value = rotated_row ? rotated_value(encoded_values, storage, storage_row, d, status)
                    : (encoded ? fp8_decode(encoded_values[base + d], storage.value_scale, status)
                               : float(values[base + d]));
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

// Parallel maximum and split weights. Each output coordinate still accumulates
// its numerator in ascending split order. FP32 throughout, with a 2048 bound.
// The host always dispatches 256 threads (eight SIMD groups) per query head.
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
    uint tid [[thread_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]],
    uint simd_group [[simdgroup_index_in_threadgroup]]) {
    const uint ordinal = group_position.x;
    if (ordinal >= p.n || p.index == 0u) return;
    if (p.index > 2048u) {
        if (tid == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
        return;
    }
    if (p.op == 2u && (p.dim != 512u || p.n != 8u)) {
        if (tid == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
        return;
    }
    const uint storage_ordinal = p.op == 2u ? (ordinal / 4u) * 8u + ordinal : ordinal;
    const uint width = p.dim + 2u;
    threadgroup float weights[2048], group_max[8], group_den[8];
    float local_max = -INFINITY;
    for (uint split = tid; split < p.index; split += 256u) {
        const uint record = (storage_ordinal * p.index + split) * width;
        const float den = partials[record + p.dim];
        const float maximum = partials[record + p.dim + 1u];
        if (!isfinite(den) || den < 0.0f || (den > 0.0f && !isfinite(maximum))) {
            atomic_store_explicit(status, 1u, memory_order_relaxed);
        } else if (den > 0.0f) {
            local_max = max(local_max, maximum);
        }
    }
    const float simd_maximum = simd_max(local_max);
    if (lane == 0u) group_max[simd_group] = simd_maximum;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float maximum = -INFINITY;
    for (uint i = 0u; i < 8u; ++i) maximum = max(maximum, group_max[i]);

    float local_den = 0.0f;
    for (uint split = tid; split < p.index; split += 256u) {
        const uint record = (storage_ordinal * p.index + split) * width;
        const float den = partials[record + p.dim];
        const float split_maximum = partials[record + p.dim + 1u];
        // A negative sentinel marks empty records. Active records whose weight
        // underflows to zero must still read and check their numerator values.
        const float weight = den > 0.0f && isfinite(den) && isfinite(split_maximum)
            ? exp(split_maximum - maximum) : -1.0f;
        weights[split] = weight;
        if (weight >= 0.0f) local_den += den * weight;
    }
    const float simd_denominator = simd_sum(local_den);
    if (lane == 0u) group_den[simd_group] = simd_denominator;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float denominator = 0.0f;
    for (uint i = 0u; i < 8u; ++i) denominator += group_den[i];
    const bool valid = isfinite(maximum) && isfinite(denominator) && denominator > 0.0f;
    if (!valid && tid == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);

    if (p.dim == 512u || (p.dim == 256u && p.index >= 256u)) {
        if (tid < p.dim / 2u) {
            const uint d = tid * 2u;
            float2 numerator = float2(0.0f);
            for (uint split = 0u; split < p.index; ++split) {
                const float weight = weights[split];
                if (weight < 0.0f) continue;
                const uint record = (storage_ordinal * p.index + split) * width + d;
                // Scalar accesses retain the four-byte Ref alignment contract.
                numerator += float2(partials[record], partials[record + 1u]) * weight;
            }
            const float2 value = valid ? numerator / denominator : float2(0.0f);
            if (!isfinite(value.x) || !isfinite(value.y))
                atomic_store_explicit(status, 1u, memory_order_relaxed);
            output[ordinal * p.dim + d] = isfinite(value.x) ? value.x : 0.0f;
            output[ordinal * p.dim + d + 1u] = isfinite(value.y) ? value.y : 0.0f;
        }
    } else {
        for (uint d = tid; d < p.dim; d += 256u) {
            float numerator = 0.0f;
            for (uint split = 0u; split < p.index; ++split) {
                if (weights[split] < 0.0f) continue;
                numerator += partials[(storage_ordinal * p.index + split) * width + d] * weights[split];
            }
            const float value = valid ? numerator / denominator : 0.0f;
            if (!isfinite(value)) atomic_store_explicit(status, 1u, memory_order_relaxed);
            output[ordinal * p.dim + d] = isfinite(value) ? value : 0.0f;
        }
    }
    if (tid == 0u && (p.mode & 1u) != 0u)
        log_mass[ordinal] = valid ? log(denominator) + maximum : 0.0f;
}
