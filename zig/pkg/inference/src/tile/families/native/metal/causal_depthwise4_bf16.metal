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

// native-program:entry antfly_causal_depthwise4_head_norm_d128_bf16
struct CausalDepthwise4Bf16Params {
    uint channels, rows, valid_history, dynamic, normalize;
    float output_scale, clip, epsilon;
};

inline bfloat antfly_causal_depthwise4_sample(
    device const bfloat *input, device const bfloat *history,
    int row, uint channel, constant CausalDepthwise4Bf16Params &p
) {
    if (row >= 0) return input[ulong(row) * p.channels + channel];
    if (uint(-row) > p.valid_history) return bfloat(0.0f);
    return history[ulong(row + 3) * p.channels + channel];
}

struct CausalPagedKvParams {
    uint start, page_tokens, first_page, page_count, physical_pages, q4;
};

struct CausalDepthwise4PagedWire {
    CausalDepthwise4Bf16Params convolution;
    CausalPagedKvParams pages;
};

template <bool paged>
inline void antfly_causal_depthwise4_apply(
    device const bfloat *input, device const bfloat *weight,
    device const bfloat *history, device const bfloat *dynamic_taps,
    device bfloat *output, device bfloat *next_history,
    constant CausalDepthwise4Bf16Params &p,
    device const uint *page_ids, constant CausalPagedKvParams *pages,
    uint2 group, uint lane
) {
    const uint head = group.x;
    const int row = int(group.y);
    const uint heads = p.channels / 128u;
    const bool dynamic = p.dynamic != 0u;
    float4 rounded;
    float square = 0.0f;
    for (uint i = 0; i < 4; ++i) {
        const uint channel = head * 128u + lane + i * 32u;
        float sum = 0.0f;
        for (uint tap = 0; tap < 4; ++tap) {
            const uint lag = dynamic ? tap : 3u - tap;
            const float sample = float(antfly_causal_depthwise4_sample(input, history, row - int(lag), channel, p));
            float w = float(weight[dynamic ? ulong(tap) * p.channels + channel : ulong(channel) * 4u + tap]);
            if (dynamic) w += float(dynamic_taps[(ulong(row) * heads + head) * 4u + tap]);
            sum = fma(sample, w, sum);
        }
        const ulong index = ulong(row) * p.channels + channel;
        rounded[i] = float(bfloat((sum + float(input[index])) * p.output_scale));
        if (p.normalize) square = fma(rounded[i], rounded[i], square);
        else output[index] = bfloat(rounded[i]);
        if (row == 0) {
            for (uint j = 0; j < 3; ++j)
                next_history[ulong(j) * p.channels + channel] =
                    antfly_causal_depthwise4_sample(input, history, int(p.rows) - 3 + int(j), channel, p);
        }
    }
    if (!p.normalize) return;
    for (uint delta = 16u; delta != 0u; delta >>= 1u)
        square += simd_shuffle_down(square, delta);
    square = simd_broadcast(square, 0);
    const float factor = p.clip > 0.0f ? min(1.0f, p.clip / (sqrt(square) + p.epsilon))
                                       : rsqrt(square / 128.0f + p.epsilon);
    ulong target = ulong(row) * heads + head;
    if (paged) {
        const uint position = pages->start + uint(row);
        const uint logical = position / pages->page_tokens - pages->first_page;
        if (logical >= pages->page_count) return;
        const uint physical = page_ids[logical];
        if (physical >= pages->physical_pages) return;
        target = (ulong(physical) * pages->page_tokens + position % pages->page_tokens) * heads + head;
        if (pages->q4) {
            // Keep the BF16 boundary that preceded cache quantization.
            float4 values;
            for (uint i = 0; i < 4; ++i) values[i] = float(bfloat(rounded[i] * factor));
            const float4 magnitude = abs(values);
            const float maximum = simd_max(max(max(magnitude.x, magnitude.y), max(magnitude.z, magnitude.w)));
            const half scale = half(maximum / 7.0f);
            const float inverse = scale == half(0.0f) ? 0.0f : 1.0f / float(scale);
            const uint4 q = uint4(int4(clamp(rint(values * inverse), -8.0f, 7.0f)) + 8);
            device ushort *bits = reinterpret_cast<device ushort *>(output);
            const ulong offset = target * 33u;
            bits[offset + lane] = ushort(q.x | (q.y << 4u) | (q.z << 8u) | (q.w << 12u));
            if (lane == 0u) bits[offset + 32u] = as_type<ushort>(scale);
            return;
        }
    }
    const ulong base = target * 128u + lane;
    for (uint i = 0; i < 4; ++i) output[base + i * 32u] = bfloat(rounded[i] * factor);
}

kernel void antfly_causal_depthwise4_head_norm_d128_bf16(
    device const bfloat *input [[buffer(0)]],
    device const bfloat *weight [[buffer(1)]],
    device const bfloat *history [[buffer(2)]],
    device const bfloat *dynamic_taps [[buffer(3)]],
    device bfloat *output [[buffer(4)]],
    device bfloat *next_history [[buffer(5)]],
    constant CausalDepthwise4Bf16Params &p [[buffer(6)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_causal_depthwise4_apply<false>(input, weight, history, dynamic_taps, output, next_history,
        p, nullptr, nullptr, group, lane);
}

// native-program:entry antfly_causal_depthwise4_paged_d128_bf16
kernel void antfly_causal_depthwise4_paged_d128_bf16(
    device const bfloat *input [[buffer(0)]],
    device const bfloat *weight [[buffer(1)]],
    device const bfloat *history [[buffer(2)]],
    device const bfloat *dynamic_taps [[buffer(3)]],
    device bfloat *output [[buffer(4)]],
    device bfloat *next_history [[buffer(5)]],
    device const uint *page_ids [[buffer(6)]],
    constant CausalDepthwise4PagedWire &wire [[buffer(7)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_causal_depthwise4_apply<true>(input, weight, history, dynamic_taps, output, next_history,
        wire.convolution, page_ids, &wire.pages, group, lane);
}
