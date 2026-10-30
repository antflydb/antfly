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

#ifndef ATTENTION_MATCH_SPECIAL_DIM
#define ATTENTION_MATCH_SPECIAL_DIM 0u
#endif
#ifndef ATTENTION_MATCH_SPECIAL_ENCODING
#define ATTENTION_MATCH_SPECIAL_ENCODING 3u
#endif
#ifndef ATTENTION_MATCH_SPECIAL_ROWS
#define ATTENTION_MATCH_SPECIAL_ROWS 0u
#endif
constant uint stage_rows = ATTENTION_MATCH_SPECIAL_ROWS == 0u ? 256u : ATTENTION_MATCH_SPECIAL_ROWS;

// One threadgroup owns one (query head, split). Its eight SIMD groups compute
// scores independently, then share a stable softmax and parallel value sums.
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
    uint tid [[thread_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]) {
    if (p.n == 0u) return;
    const uint dim = ATTENTION_MATCH_SPECIAL_DIM == 0u ? p.dim : ATTENTION_MATCH_SPECIAL_DIM;
    const uint layout_splits = p.n;
    const uint split = group_position.x % layout_splits;
    const uint query_kv_ordinal = group_position.x / layout_splits;
    const uint gqa = p.stride >> 16;
    const uint first_head = p.stride & 0xffffu;
    if (gqa == 0u || query_kv_ordinal >= gqa) return;
    const uint rows_per_split = ATTENTION_MATCH_SPECIAL_ROWS != 0u ? ATTENTION_MATCH_SPECIAL_ROWS : (p.op == 2u ? 32u : split_rows);
    const uint local_splits = (p.index + rows_per_split - 1u) / rows_per_split;
    if (split >= local_splits) {
        // Ragged heads must overwrite their own empty records on the GPU.
        // A forward reuses scratch without host writes between dispatches.
        const uint record = ((first_head + query_kv_ordinal) * layout_splits + split) * (dim + 2u);
        for (uint d = tid; d < dim; d += 256u) partials[record + d] = 0.0f;
        if (tid == 0u) { partials[record + dim] = 0.0f; partials[record + dim + 1u] = -INFINITY; }
        return;
    }
    const uint subgroup = tid / 32u;
    const uint head = first_head + query_kv_ordinal;
    const uint query_base = head * dim;
    const ulong query_position = ulong(p.position);
    const uint ring_start = as_type<uint>(p.theta);
    const uint capacity = as_type<uint>(p.reserved);
    const uint row_begin = split * rows_per_split;
    const uint count = min(p.index - row_begin, rows_per_split);
    StorageHeader storage = *reinterpret_cast<device const StorageHeader *>(encoded_keys);
    if (ATTENTION_MATCH_SPECIAL_ENCODING < 3u) storage.encoding = ATTENTION_MATCH_SPECIAL_ENCODING;
    threadgroup float scores[stage_rows];
    threadgroup int indices[stage_rows];
    threadgroup uint visible_rows[stage_rows];
    threadgroup float reduction[16];
    float rotated[16];
    if (storage.encoding == 2u) rotate_query(queries + query_base, dim, lane, rotated, status);
    for (uint local = subgroup; local < count; local += 8u) {
        // Validated start and row offsets are each below capacity, so their
        // sum needs at most one wrap and cannot overflow the context bounds.
        uint physical = ring_start + row_begin + local;
        if (physical >= capacity) physical -= capacity;
        const Metadata meta = metadata[physical];
        const ulong position = ulong(meta.position_lo) | (ulong(meta.position_hi) << 32);
        const ulong coverage = ulong(meta.coverage_lo) | (ulong(meta.coverage_hi) << 32);
        const bool visible = position <= query_position && coverage != 0u && coverage <= query_position + 1u &&
            (p.mode == 0u || query_position - position < p.mode);
        const int index = visible ? storage_index(encoded_keys, storage, physical) : -1;
        const bool encoded = index >= 0;
        const bool rotated_row = encoded && storage.encoding == 2u;
        const uint storage_row = encoded ? uint(index) : uint(-index - 1);
        const uint base = storage_row * dim;
        float dot = 0.0f;
        if (visible) {
            for (uint i = 0; i < 16u; ++i) {
                const uint d = lane + i * 32u;
                if (d < dim) {
                    const float key = rotated_row ? rotated_key(encoded_keys, storage, storage_row, d)
                        : (encoded ? fp8_decode(encoded_keys[storage.key_payload_offset + base + d], storage.key_scale, status)
                                   : float(keys[base + d]));
                    dot += (rotated_row ? rotated[i] : queries[query_base + d]) * key;
                }
            }
        }
        float score = simd_sum(dot);
        if (visible) {
            if (rotated_row) score *= packed_scale(encoded_keys + storage.key_payload_offset + storage_row * storage.key_row_bytes, status, true);
            score *= p.scale;
            if (!isfinite(score)) {
                if (lane == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
                score = 0.0f;
            }
            if (p.epsilon > 0.0f) score = p.epsilon * tanh(score / p.epsilon);
            score += meta.bias;
            if (!isfinite(score)) {
                if (lane == 0u) atomic_store_explicit(status, 1u, memory_order_relaxed);
                score = 0.0f;
            }
        }
        if (lane == 0u) {
            scores[local] = visible ? score : -INFINITY;
            indices[local] = index;
            visible_rows[local] = visible ? 1u : 0u;
        }
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float local_max = tid < count ? scores[tid] : -INFINITY;
    const float subgroup_max = simd_max(local_max);
    if (lane == 0u) reduction[subgroup] = subgroup_max;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float maximum = -INFINITY;
    for (uint i = 0u; i < 8u; ++i) maximum = max(maximum, reduction[i]);
    float weight = tid < count && isfinite(maximum) ? exp(scores[tid] - maximum) : 0.0f;
    if (tid < count) scores[tid] = weight;
    const float subgroup_sum = simd_sum(weight);
    if (lane == 0u) reduction[8u + subgroup] = subgroup_sum;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float denominator = 0.0f;
    for (uint i = 0u; i < 8u; ++i) denominator += reduction[8u + i];
    const uint record = (head * layout_splits + split) * (dim + 2u);
    if (dim == 256u || dim == 512u) {
        if (tid < dim / 2u) {
            // Pair adjacent values without changing either coordinate's FP32
            // accumulation order; validate both even at zero softmax weight.
            float2 numerator = float2(0.0f);
            for (uint local = 0u; local < count; ++local) {
                if (visible_rows[local] == 0u) continue;
                const int index = indices[local];
                const bool encoded = index >= 0;
                const bool rotated_row = encoded && storage.encoding == 2u;
                const uint row = encoded ? uint(index) : uint(-index - 1);
                const uint d = tid * 2u;
                float2 value;
                if (rotated_row) value = float2(rotated_value(encoded_values, storage, row, d, status), rotated_value(encoded_values, storage, row, d + 1u, status));
                else if (encoded) value = float2(fp8_decode(encoded_values[row * dim + d], storage.value_scale, status), fp8_decode(encoded_values[row * dim + d + 1u], storage.value_scale, status));
                else value = float2(reinterpret_cast<device const half2 *>(values)[row * (dim / 2u) + tid]);
                for (uint i = 0u; i < 2u; ++i) if (!isfinite(value[i])) {
                    atomic_store_explicit(status, 1u, memory_order_relaxed);
                    value[i] = 0.0f;
                }
                numerator += scores[local] * value;
            }
            partials[record + tid * 2u] = numerator.x;
            partials[record + tid * 2u + 1u] = numerator.y;
        }
    } else {
        for (uint d = tid; d < dim; d += 256u) {
            float numerator = 0.0f;
            for (uint local = 0u; local < count; ++local) {
                if (visible_rows[local] == 0u) continue;
                const int index = indices[local];
                const bool encoded = index >= 0;
                const bool rotated_row = encoded && storage.encoding == 2u;
                const uint row = encoded ? uint(index) : uint(-index - 1);
                float value = rotated_row ? rotated_value(encoded_values, storage, row, d, status)
                    : (encoded ? fp8_decode(encoded_values[row * dim + d], storage.value_scale, status)
                               : float(values[row * dim + d]));
                // Validate even when softmax underflow gives a visible row zero weight.
                if (!isfinite(value)) {
                    atomic_store_explicit(status, 1u, memory_order_relaxed);
                    value = 0.0f;
                }
                numerator += scores[local] * value;
            }
            partials[record + d] = numerator;
        }
    }
    if (tid == 0u) {
        partials[record + dim] = denominator;
        partials[record + dim + 1u] = maximum;
    }
}
