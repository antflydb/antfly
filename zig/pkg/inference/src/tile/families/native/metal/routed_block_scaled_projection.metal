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

// native-program:entry antfly_routed_block_scaled_bf16
struct RoutedBlockScaledParams {
    uint rows;
    uint input_width;
    uint output_width;
    uint expert_count;
    uint input_repeat;
    uint format;
};

inline float antfly_routed_decode_e2m1(uint nibble) {
    constexpr float magnitude[8] = {0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 3.0f, 4.0f, 6.0f};
    const float value = magnitude[nibble & 7u];
    return (nibble & 8u) == 0 ? value : -value;
}

inline float antfly_routed_decode_e4m3(uint bits) {
    const uint exponent = (bits >> 3u) & 15u;
    const uint mantissa = bits & 7u;
    if (exponent == 15u && mantissa == 7u) return as_type<float>(0x7fc00000u);
    const uint normal = ((exponent + 120u) << 23u) | (mantissa << 20u);
    const uint magnitude = exponent == 0u ? as_type<uint>(float(mantissa) * 0.001953125f) : normal;
    return as_type<float>(magnitude | ((bits & 128u) << 24u));
}

inline float antfly_routed_decode_e8m0(uint bits) {
    if (bits == 255u) return as_type<float>(0x7fc00000u);
    return as_type<float>(bits == 0u ? 0x00400000u : bits << 23u);
}

inline bfloat antfly_routed_weight(
    device const uchar *weight, device const uchar *scales, uint index, bool fp4, float outer
) {
    const uint packet = weight[fp4 ? index / 2u : index];
    const uint scale = scales[index / (fp4 ? 16u : 32u)];
    const float value = fp4
        ? (antfly_routed_decode_e2m1(packet >> (4u * (index & 1u))) * antfly_routed_decode_e4m3(scale)) * outer
        : antfly_routed_decode_e4m3(packet) * antfly_routed_decode_e8m0(scale);
    return bfloat(value);
}

kernel void antfly_routed_block_scaled_bf16(
    device const bfloat *input [[buffer(0)]],
    device const uchar *values [[buffer(1)]],
    device const uchar *scales [[buffer(2)]],
    device const uint *expert_ids [[buffer(3)]],
    device const float *outer_amax [[buffer(4)]],
    device const float *outer_denominator [[buffer(5)]],
    device bfloat *output [[buffer(6)]],
    constant RoutedBlockScaledParams &p [[buffer(7)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint simd_group [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]],
    uint simd_width [[threads_per_simdgroup]]
) {
    const uint column = group.x * 4u + simd_group;
    const uint row = group.y;
    if (column >= p.output_width || row >= p.rows) return;
    const ulong output_index = ulong(row) * p.output_width + column;
    const uint expert = expert_ids[row];
    if (expert >= p.expert_count) {
        if (lane == 0) output[output_index] = bfloat(0.0f);
        return;
    }
    const bool fp4 = p.format == 0;
    const uint block = fp4 ? 16u : 32u;
    const uint blocks = p.input_width / block;
    const uint packed_width = fp4 ? p.input_width / 2u : p.input_width;
    const ulong weight_row = ulong(expert) * p.output_width + column;
    const device uchar *weight = values + weight_row * packed_width;
    const device uchar *weight_scales = scales + weight_row * blocks;
    const device bfloat *activation = input + ulong(row / p.input_repeat) * p.input_width;
    const float outer = fp4 ? outer_amax[expert] / outer_denominator[0] : 1.0f;
    float accumulator = 0.0f;
    for (uint index = lane; index < p.input_width; index += simd_width) {
        accumulator = fma(float(activation[index]), float(antfly_routed_weight(weight, weight_scales, index, fp4, outer)), accumulator);
    }
    const float total = simd_sum(accumulator);
    if (lane == 0) output[output_index] = bfloat(total);
}

// native-program:entry antfly_routed_biased_sigmoid_topk
struct RoutedExpertSelectionParams {
    uint experts;
    uint count;
    float scale;
};

inline float antfly_routed_log_sigmoid(float value) {
    return min(value, 0.0f) - log(1.0f + exp(-abs(value)));
}

kernel void antfly_routed_biased_sigmoid_topk(
    device const float *logits [[buffer(0)]],
    device const float *bias [[buffer(1)]],
    device uint *indices [[buffer(2)]],
    device float *weights [[buffer(3)]],
    constant RoutedExpertSelectionParams &p [[buffer(4)]],
    uint row [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    float selected[8];
    const ulong start = ulong(row) * p.experts;
    uint local_indices[8];
    float scores[8];
    for (uint tile = 0; tile < 8; ++tile) {
        const uint index = tile * 32u + lane;
        local_indices[tile] = index < p.experts ? index : 0xffffffffu;
        const float value = index < p.experts ? logits[start + index] : -INFINITY;
        const float score = index < p.experts ? 1.0f / (1.0f + exp(-value)) + bias[index] : -INFINITY;
        scores[tile] = isnan(score) ? -INFINITY : score;
    }
    for (uint rank = 0; rank < p.count; ++rank) {
        float best = -INFINITY;
        uint best_index = 0xffffffffu;
        for (uint tile = 0; tile < 8; ++tile) {
            const float score = scores[tile];
            if (score > best || (score == best && score > -INFINITY && local_indices[tile] < best_index)) {
                best = score;
                best_index = local_indices[tile];
            }
        }
        const float row_best = simd_max(best);
        const uint picked = simd_min(best == row_best ? best_index : 0xffffffffu);
        const uint selected_index = picked < p.experts ? picked : 0u;
        selected[rank] = antfly_routed_log_sigmoid(logits[start + selected_index]);
        if (lane == 0) indices[ulong(row) * p.count + rank] = selected_index;
        for (uint tile = 0; tile < 8; ++tile) {
            if (local_indices[tile] == picked) scores[tile] = -INFINITY;
        }
    }
    const float value = lane < p.count ? selected[lane] : -INFINITY;
    const float maximum = simd_max(value);
    const float probability = (lane < p.count && maximum > -INFINITY) ? exp(value - maximum) : 0.0f;
    const float sum = simd_sum(probability);
    if (lane < p.count) weights[ulong(row) * p.count + lane] = sum > 0.0f
        ? probability * (p.scale / sum)
        : p.scale / float(p.count);
}

// native-program:entry antfly_routed_capped_gate_bf16
struct RoutedGateParams {
    uint width;
    uint rows;
    float gate_cap;
    float up_cap;
};

kernel void antfly_routed_capped_gate_bf16(
    device const bfloat *pairs [[buffer(0)]],
    device const float *route_weights [[buffer(1)]],
    device bfloat *output [[buffer(2)]],
    constant RoutedGateParams &p [[buffer(3)]],
    uint2 index [[thread_position_in_grid]]
) {
    if (index.x >= p.width || index.y >= p.rows) return;
    const ulong offset = ulong(index.y) * p.width + index.x;
    const float gate = float(pairs[offset * 2]);
    const float up = float(pairs[offset * 2 + 1]);
    const float capped_gate = p.gate_cap * tanh(gate / p.gate_cap);
    const float capped_up = p.up_cap * tanh(up / p.up_cap);
    output[offset] = bfloat((capped_gate / (1.0f + exp(-gate))) * capped_up * route_weights[index.y]);
}

// native-program:entry antfly_routed_reduce_bf16
struct RoutedReduceParams {
    uint width;
    uint rows;
    uint count;
};

kernel void antfly_routed_reduce_bf16(
    device const bfloat *input [[buffer(0)]],
    device bfloat *output [[buffer(1)]],
    constant RoutedReduceParams &p [[buffer(2)]],
    uint2 index [[thread_position_in_grid]]
) {
    if (index.x >= p.width || index.y >= p.rows) return;
    const ulong offset = ulong(index.y) * p.count * p.width + index.x;
    float sum = 0.0f;
    for (uint route = 0; route < p.count; ++route) sum += float(input[offset + ulong(route) * p.width]);
    output[ulong(index.y) * p.width + index.x] = bfloat(sum);
}

struct RoutedGroupParams {
    uint rows;
    uint experts;
};

inline void antfly_write_routed_group(device uint *groups, thread const uint *routes, uint count) {
    if (count >= 4u) {
        device uint *leader = groups + ulong(routes[0]) * 9u;
        leader[0] = count;
        for (uint i = 0u; i < count; ++i) leader[i + 1u] = routes[i];
        for (uint i = 1u; i < count; ++i) groups[ulong(routes[i]) * 9u] = 0u;
    } else {
        for (uint i = 0u; i < count; ++i) groups[ulong(routes[i]) * 9u] = 1u;
    }
}

// native-program:entry antfly_routed_build_groups
kernel void antfly_routed_build_groups(
    device const uint *expert_ids [[buffer(0)]], device uint *groups [[buffer(1)]],
    constant RoutedGroupParams &p [[buffer(2)]], uint expert [[thread_position_in_grid]]
) {
    if (expert > p.experts) return;
    uint pending[8];
    uint count = 0;
    for (uint route = 0u; route < p.rows; ++route) {
        const uint id = expert_ids[route];
        if (id != expert && !(expert == p.experts && id > p.experts)) continue;
        pending[count++] = route;
        if (count == 8u) {
            antfly_write_routed_group(groups, pending, count);
            count = 0;
        }
    }
    antfly_write_routed_group(groups, pending, count);
}

template<bool full_group>
inline void antfly_routed_group_dot(
    device const bfloat *input, device const uchar *weight, device const uchar *weight_scales,
    device bfloat *output, device const uint *entry, constant RoutedBlockScaledParams &p,
    uint count, uint column, uint lane, bool fp4, float outer
) {
    uint rows[8];
    float accum[8] = {0.0f};
    ulong offsets[8];
    #pragma unroll
    for (uint i = 0u; i < 8u; ++i) {
        rows[i] = (full_group || i < count) ? entry[1u + i] : 0u;
        offsets[i] = ulong(rows[i] / p.input_repeat) * p.input_width;
    }
    for (uint k = lane; k < p.input_width; k += 32u) {
        const float w = float(antfly_routed_weight(weight, weight_scales, k, fp4, outer));
        // Keep the per-lane accumulation and reduction order of the one-row
        // kernel. Only the decoded weight is shared across grouped rows.
        #pragma unroll
        for (uint i = 0u; i < 8u; ++i)
            if ((full_group || i < count)) accum[i] = fma(float(input[offsets[i] + k]), w, accum[i]);
    }
    #pragma unroll
    for (uint i = 0u; i < 8u; ++i) {
        if ((full_group || i < count)) {
            const float dot = simd_sum(accum[i]);
            if (lane == 0u) output[ulong(rows[i]) * p.output_width + column] = bfloat(dot);
        }
    }
}

template<bool specialize>
inline void antfly_routed_group_projection(
    device const bfloat *input,
    device const uchar *values,
    device const uchar *scales,
    device const uint *expert_ids,
    device const float *outer_amax,
    device const float *outer_denominator,
    device bfloat *output,
    device const uint *groups,
    constant RoutedBlockScaledParams &p,
    uint2 group,
    uint warp,
    uint lane
) {
    const uint column = group.x * 4u + warp;
    const uint route = group.y;
    if (column >= p.output_width) return;
    const device uint *entry = groups + ulong(route) * 9u;
    const uint count = entry[0];
    if (count == 0u) return;
    const uint expert = expert_ids[route];
    if (expert >= p.expert_count) {
        if (lane == 0u) {
            for (uint i = 0u; i < count; ++i) {
                const uint destination = count == 1u ? route : entry[1u + i];
                output[ulong(destination) * p.output_width + column] = bfloat(0.0f);
            }
        }
        return;
    }

    const bool fp4 = p.format == 0u;
    const uint packed_width = fp4 ? p.input_width / 2u : p.input_width;
    const uint blocks = p.input_width / (fp4 ? 16u : 32u);
    const ulong weight_row = ulong(expert) * p.output_width + column;
    const device uchar *weight = values + weight_row * packed_width;
    const device uchar *weight_scales = scales + weight_row * blocks;
    const float outer = fp4 ? outer_amax[expert] / outer_denominator[0] : 1.0f;
    if (count == 1u) {
        const device bfloat *activation = input + ulong(route / p.input_repeat) * p.input_width;
        float accumulator = 0.0f;
        for (uint k = lane; k < p.input_width; k += 32u) {
            const float w = float(antfly_routed_weight(weight, weight_scales, k, fp4, outer));
            accumulator = fma(float(activation[k]), w, accumulator);
        }
        const float dot = simd_sum(accumulator);
        if (lane == 0u) output[ulong(route) * p.output_width + column] = bfloat(dot);
        return;
    }

    if (specialize && count == 8u) {
        antfly_routed_group_dot<true>(input, weight, weight_scales, output, entry, p, count, column, lane, fp4, outer);
    } else {
        antfly_routed_group_dot<false>(input, weight, weight_scales, output, entry, p, count, column, lane, fp4, outer);
    }
}

// native-program:entry antfly_routed_block_scaled_bf16_grouped
kernel void antfly_routed_block_scaled_bf16_grouped(
    device const bfloat *input [[buffer(0)]],
    device const uchar *values [[buffer(1)]],
    device const uchar *scales [[buffer(2)]],
    device const uint *expert_ids [[buffer(3)]],
    device const float *outer_amax [[buffer(4)]],
    device const float *outer_denominator [[buffer(5)]],
    device bfloat *output [[buffer(6)]],
    device const uint *groups [[buffer(7)]],
    constant RoutedBlockScaledParams &p [[buffer(8)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint warp [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_routed_group_projection<false>(input, values, scales, expert_ids, outer_amax,
        outer_denominator, output, groups, p, group, warp, lane);
}

// native-program:entry antfly_routed_block_scaled_bf16_grouped_wide
kernel void antfly_routed_block_scaled_bf16_grouped_wide(
    device const bfloat *input [[buffer(0)]],
    device const uchar *values [[buffer(1)]],
    device const uchar *scales [[buffer(2)]],
    device const uint *expert_ids [[buffer(3)]],
    device const float *outer_amax [[buffer(4)]],
    device const float *outer_denominator [[buffer(5)]],
    device bfloat *output [[buffer(6)]],
    device const uint *groups [[buffer(7)]],
    constant RoutedBlockScaledParams &p [[buffer(8)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint warp [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_routed_group_projection<true>(input, values, scales, expert_ids, outer_amax,
        outer_denominator, output, groups, p, group, warp, lane);
}
