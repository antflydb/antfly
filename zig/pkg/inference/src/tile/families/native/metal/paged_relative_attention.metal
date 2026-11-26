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

// native-program:entry antfly_paged_relative_gqa_d128_bf16_partial
struct PagedRelativeGqaParams {
    uint start, visible_start, page_tokens, first_page, page_count, physical_pages;
    uint q_heads, kv_heads, window, relative_extent, splits, q4;
    float score_scale;
};

template <bool compressed>
inline float4 antfly_relative_kv_head(device const ushort *storage, ulong head, uint lane) {
    if (compressed) {
        const ulong base = head * 33u;
        const uint packed = storage[base + lane];
        const float scale = float(as_type<half>(storage[base + 32u]));
        return float4(float(int(packed & 15u) - 8), float(int((packed >> 4u) & 15u) - 8),
            float(int((packed >> 8u) & 15u) - 8), float(int((packed >> 12u) & 15u) - 8)) * scale;
    }
    const device bfloat *values = reinterpret_cast<device const bfloat *>(storage) + head * 128u + lane;
    return float4(float(values[0]), float(values[32]), float(values[64]), float(values[96]));
}

template <bool compressed, uint heads, uint rows = 1>
inline void antfly_paged_relative_gqa_d128_partial(
    device const bfloat *query,
    device const ushort *keys,
    device const ushort *values,
    device const bfloat *relative,
    device const uint *page_ids,
    device float *partials,
    constant PagedRelativeGqaParams &p,
    uint2 group,
    uint lane
) {
    const uint groups_per_row = p.q_heads / heads;
    const uint row_head = rows == 1u ? group.x * heads :
        (group.x / groups_per_row) * rows * p.q_heads + (group.x % groups_per_row) * heads;
    const uint batch_row = row_head / p.q_heads;
    const uint head = row_head % p.q_heads;
    const uint kv_head = head / (p.q_heads / p.kv_heads);
    uint begin[rows], limit[rows];
    uint first_key = ~0u;
    uint last_key = 0u;
    // Each row retains its original split boundary and accumulation order.
    #pragma unroll
    for (uint r = 0u; r < rows; ++r) {
        const uint end = p.start + batch_row + r + 1u;
        const uint first = max(p.visible_start, p.window != 0u && end > p.window ? end - p.window : 0u);
        const uint live = max(end - first, 1u);
        const uint row_splits = min(p.splits, (live - 1u) / 128u + 1u);
        const uint keys_per_split = (live - 1u) / row_splits + 1u;
        begin[r] = first + group.y * keys_per_split;
        limit[r] = min(end, begin[r] + keys_per_split);
        if (rows == 1u) {
            first_key = begin[r];
            last_key = limit[r];
        } else if (begin[r] < limit[r]) {
            first_key = min(first_key, begin[r]);
            last_key = max(last_key, limit[r]);
        }
    }
    float4 q[heads * rows], numerator[heads * rows];
    float maximum[heads * rows], denominator[heads * rows];
    #pragma unroll
    for (uint h = 0u; h < heads * rows; ++h) {
        const ulong q_base = ulong(row_head + (h / heads) * p.q_heads + h % heads) * 128u + lane;
        for (uint i = 0; i < 4; ++i) q[h][i] = float(query[q_base + i * 32u]);
        numerator[h] = float4(0.0f);
        maximum[h] = -INFINITY;
        denominator[h] = 0.0f;
    }
    for (uint next_key = first_key; next_key < last_key;) {
        const uint index = next_key / p.page_tokens - p.first_page;
        const uint in_page = next_key % p.page_tokens;
        const uint first = next_key;
        next_key = min(last_key, first + p.page_tokens - in_page);
        if (index >= p.page_count) continue;
        const uint page = page_ids[index];
        if (page >= p.physical_pages) continue;
        ulong stored_head = (ulong(page) * p.page_tokens + in_page) * p.kv_heads + kv_head;
        for (uint pos = first; pos < next_key; ++pos, stored_head += p.kv_heads) {
            const float4 key = antfly_relative_kv_head<compressed>(keys, stored_head, lane);
            const float4 value = antfly_relative_kv_head<compressed>(values, stored_head, lane);
            #pragma unroll
            for (uint h = 0u; h < heads * rows; ++h) {
                const uint r = h / heads;
                if (rows > 1 && (pos < begin[r] || pos >= limit[r])) continue;
                const uint distance = p.start + batch_row + r - pos;
                float dot = 0.0f;
                for (uint i = 0u; i < 4u; ++i) dot = fma(q[h][i], key[i], dot);
                const float reduced = simd_sum(dot);
                const float bias = distance < p.relative_extent ? float(relative[ulong(row_head + (h / heads) * p.q_heads + h % heads) * p.relative_extent + distance]) : 0.0f;
                const float score = reduced * p.score_scale + bias;
                const float next_max = max(maximum[h], score);
                const float retain = exp(maximum[h] - next_max);
                const float add = exp(score - next_max);
                denominator[h] = fma(denominator[h], retain, add);
                numerator[h] = fma(float4(add), value, numerator[h] * retain);
                maximum[h] = next_max;
            }
        }
    }
    #pragma unroll
    for (uint h = 0u; h < heads * rows; ++h) {
        const ulong dst = (ulong(row_head + (h / heads) * p.q_heads + h % heads) * p.splits + group.y) * 130u;
        for (uint i = 0u; i < 4u; ++i) partials[dst + i * 32u + lane] = numerator[h][i];
        if (lane == 0u) {
            partials[dst + 128u] = maximum[h];
            partials[dst + 129u] = denominator[h];
        }
    }
}

kernel void antfly_paged_relative_gqa_d128_bf16_partial(
    device const bfloat *query [[buffer(0)]],
    device const ushort *keys [[buffer(1)]],
    device const ushort *values [[buffer(2)]],
    device const bfloat *relative [[buffer(3)]],
    device const uint *page_ids [[buffer(4)]],
    device float *partials [[buffer(5)]],
    constant PagedRelativeGqaParams &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_paged_relative_gqa_d128_partial<false, 1>(query, keys, values, relative, page_ids, partials, p, group, lane);
}

// native-program:entry antfly_paged_relative_gqa_d128_bf16_reduce
kernel void antfly_paged_relative_gqa_d128_bf16_reduce(
    device const float *partials [[buffer(0)]],
    device const bfloat *sinks [[buffer(1)]],
    device const bfloat *gates [[buffer(2)]],
    device bfloat *output [[buffer(3)]],
    constant uint &splits [[buffer(4)]],
    uint row_head [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    const ulong partial = ulong(row_head) * splits * 130u;
    const float sink = float(sinks[row_head]);
    float maximum = sink;
    for (uint split = 0; split < splits; ++split)
        maximum = max(maximum, partials[partial + split * 130u + 128u]);
    float denominator = exp(sink - maximum);
    float4 numerator = 0.0f;
    for (uint split = 0; split < splits; ++split) {
        const ulong src = partial + split * 130u;
        if (partials[src + 129u] == 0.0f) continue;
        const float weight = exp(partials[src + 128u] - maximum);
        denominator = fma(weight, partials[src + 129u], denominator);
        for (uint i = 0; i < 4; ++i)
            numerator[i] = fma(weight, partials[src + i * 32u + lane], numerator[i]);
    }
    const ulong base = ulong(row_head) * 128u + lane;
    for (uint i = 0; i < 4; ++i) {
        const ulong index = base + i * 32u;
        output[index] = bfloat((numerator[i] / denominator) / (1.0f + exp(-float(gates[index]))));
    }
}

// native-program:entry antfly_paged_relative_gqa_d128_q4_partial
kernel void antfly_paged_relative_gqa_d128_q4_partial(
    device const bfloat *query [[buffer(0)]],
    device const ushort *keys [[buffer(1)]],
    device const ushort *values [[buffer(2)]],
    device const bfloat *relative [[buffer(3)]],
    device const uint *page_ids [[buffer(4)]],
    device float *partials [[buffer(5)]],
    constant PagedRelativeGqaParams &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_paged_relative_gqa_d128_partial<true, 1>(query, keys, values, relative, page_ids, partials, p, group, lane);
}

// native-program:entry antfly_pack_bf16_kv_d128_q4
kernel void antfly_pack_bf16_kv_d128_q4(
    device const bfloat *key [[buffer(0)]], device const bfloat *value [[buffer(1)]],
    device ushort *key_out [[buffer(2)]], device ushort *value_out [[buffer(3)]],
    constant uint2 &geometry [[buffer(4)]],
    uint2 group [[threadgroup_position_in_grid]], uint lane [[thread_index_in_simdgroup]]
) {
    const uint head = group.x;
    if (head >= geometry.x * geometry.y) return;
    const device bfloat *src = group.y == 0u ? key : value;
    device ushort *dst = group.y == 0u ? key_out : value_out;
    const ulong begin = ulong(head) * 128u + lane;
    const float4 values = float4(float(src[begin]), float(src[begin + 32u]),
        float(src[begin + 64u]), float(src[begin + 96u]));
    const float4 magnitude = abs(values);
    const float maximum = simd_max(max(max(magnitude.x, magnitude.y), max(magnitude.z, magnitude.w)));
    const half scale = half(maximum / 7.0f);
    const float inverse = scale == half(0.0f) ? 0.0f : 1.0f / float(scale);
    const uint4 q = uint4(int4(clamp(rint(values * inverse), -8.0f, 7.0f)) + 8);
    const ulong offset = ulong(head) * 33u;
    dst[offset + lane] = ushort(q.x | (q.y << 4u) | (q.z << 8u) | (q.w << 12u));
    if (lane == 0u) dst[offset + 32u] = as_type<ushort>(scale);
}


// native-program:entry antfly_paged_relative_gqa_d128_bf16_partial_reuse3
kernel void antfly_paged_relative_gqa_d128_bf16_partial_reuse3(
    device const bfloat *query [[buffer(0)]],
    device const ushort *keys [[buffer(1)]],
    device const ushort *values [[buffer(2)]],
    device const bfloat *relative [[buffer(3)]],
    device const uint *page_ids [[buffer(4)]],
    device float *partials [[buffer(5)]],
    constant PagedRelativeGqaParams &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_paged_relative_gqa_d128_partial<false, 3>(query, keys, values, relative, page_ids, partials, p, group, lane);
}

// native-program:entry antfly_paged_relative_gqa_d128_q4_partial_reuse3
kernel void antfly_paged_relative_gqa_d128_q4_partial_reuse3(
    device const bfloat *query [[buffer(0)]],
    device const ushort *keys [[buffer(1)]],
    device const ushort *values [[buffer(2)]],
    device const bfloat *relative [[buffer(3)]],
    device const uint *page_ids [[buffer(4)]],
    device float *partials [[buffer(5)]],
    constant PagedRelativeGqaParams &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_paged_relative_gqa_d128_partial<true, 3>(query, keys, values, relative, page_ids, partials, p, group, lane);
}

// native-program:entry antfly_paged_relative_gqa_d128_bf16_partial_reuse3_rows2
kernel void antfly_paged_relative_gqa_d128_bf16_partial_reuse3_rows2(
    device const bfloat *query [[buffer(0)]],
    device const ushort *keys [[buffer(1)]],
    device const ushort *values [[buffer(2)]],
    device const bfloat *relative [[buffer(3)]],
    device const uint *page_ids [[buffer(4)]],
    device float *partials [[buffer(5)]],
    constant PagedRelativeGqaParams &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_paged_relative_gqa_d128_partial<false, 3, 2>(query, keys, values, relative, page_ids, partials, p, group, lane);
}

// native-program:entry antfly_paged_relative_gqa_d128_q4_partial_reuse3_rows2
kernel void antfly_paged_relative_gqa_d128_q4_partial_reuse3_rows2(
    device const bfloat *query [[buffer(0)]],
    device const ushort *keys [[buffer(1)]],
    device const ushort *values [[buffer(2)]],
    device const bfloat *relative [[buffer(3)]],
    device const uint *page_ids [[buffer(4)]],
    device float *partials [[buffer(5)]],
    constant PagedRelativeGqaParams &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_paged_relative_gqa_d128_partial<true, 3, 2>(query, keys, values, relative, page_ids, partials, p, group, lane);
}
