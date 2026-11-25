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
    uint q_heads, kv_heads, window, relative_extent, splits, keys_per_split;
    float score_scale;
};

kernel void antfly_paged_relative_gqa_d128_bf16_partial(
    device const bfloat *query [[buffer(0)]],
    device const bfloat *keys [[buffer(1)]],
    device const bfloat *values [[buffer(2)]],
    device const bfloat *relative [[buffer(3)]],
    device const uint *page_ids [[buffer(4)]],
    device float *partials [[buffer(5)]],
    constant PagedRelativeGqaParams &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    const uint row_head = group.x;
    const uint head = row_head % p.q_heads;
    const uint kv_head = head / (p.q_heads / p.kv_heads);
    const uint position = p.start + row_head / p.q_heads;
    const uint end = position + 1u;
    const uint first = max(p.visible_start, p.window != 0u && end > p.window ? end - p.window : 0u);
    const uint begin = first + group.y * p.keys_per_split;
    const uint limit = min(end, begin + p.keys_per_split);
    const ulong q_base = ulong(row_head) * 128u + lane;
    float4 q;
    for (uint i = 0; i < 4; ++i) q[i] = float(query[q_base + i * 32u]);
    float4 numerator = 0.0f;
    float maximum = -INFINITY;
    float denominator = 0.0f;
    for (uint pos = begin; pos < limit; ++pos) {
        const uint index = pos / p.page_tokens - p.first_page;
        if (index >= p.page_count) continue;
        const uint page = page_ids[index];
        if (page >= p.physical_pages) continue;
        const ulong row = ulong(page) * p.page_tokens + pos % p.page_tokens;
        const ulong base = (row * p.kv_heads + kv_head) * 128u + lane;
        float dot = 0.0f;
        for (uint i = 0; i < 4; ++i) dot = fma(q[i], float(keys[base + i * 32u]), dot);
        const float reduced = simd_sum(dot);
        const uint distance = position - pos;
        const float bias = distance < p.relative_extent ? float(relative[ulong(row_head) * p.relative_extent + distance]) : 0.0f;
        const float score = reduced * p.score_scale + bias;
        const float next_max = max(maximum, score);
        const float retain = exp(maximum - next_max);
        const float add = exp(score - next_max);
        denominator = fma(denominator, retain, add);
        for (uint i = 0; i < 4; ++i)
            numerator[i] = fma(add, float(values[base + i * 32u]), numerator[i] * retain);
        maximum = next_max;
    }
    const ulong dst = (ulong(row_head) * p.splits + group.y) * 130u;
    for (uint i = 0; i < 4; ++i) partials[dst + i * 32u + lane] = numerator[i];
    if (lane == 0u) {
        partials[dst + 128u] = maximum;
        partials[dst + 129u] = denominator;
    }
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
