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

// native-program:entry antfly_routed_build_groups
kernel void antfly_routed_build_groups(
    device const uint *experts [[buffer(0)]], device uint *groups [[buffer(1)]],
    constant uint &rows [[buffer(2)]], uint route [[thread_position_in_grid]]
) {
    if (route >= rows) return;
    const uint expert = experts[route];
    uint before = 0u;
    for (uint i = 0u; i < route; ++i) before += uint(experts[i] == expert);
    const uint local = before % 8u;
    device uint *entry = groups + ulong(route) * 9u;
    if (local == 0u) {
        uint count = 0u;
        for (uint i = route; i < rows && count < 8u; ++i) {
            if (experts[i] == expert) entry[1u + count++] = i;
        }
        entry[0] = count >= 4u ? count : 1u;
    } else {
        uint count = local + 1u;
        for (uint i = route + 1u; i < rows && count < 4u; ++i) count += uint(experts[i] == expert);
        entry[0] = count >= 4u ? 0u : 1u;
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
    uint lane [[thread_index_in_simdgroup]]
) {
    const uint first_col = group.x * 8u, route = group.y;
    const device uint *entry = groups + ulong(route) * 9u;
    const uint count = entry[0];
    if (count == 0u) return;
    const uint expert = expert_ids[route];
    if (expert >= p.expert_count) {
        for (uint i = lane; i < count * 8u; i += 32u) {
            const uint destination = count == 1u ? route : entry[1u + i / 8u];
            output[ulong(destination) * p.output_width + first_col + i % 8u] = bfloat(0.0f);
        }
        return;
    }
    const bool fp4 = p.format == 0u;
    const uint packed_width = fp4 ? p.input_width / 2u : p.input_width;
    const uint blocks = p.input_width / (fp4 ? 16u : 32u);
    const ulong first_weight = ulong(expert) * p.output_width + first_col;
    const float outer = fp4 ? outer_amax[expert] / outer_denominator[0] : 1.0f;
    if (count == 1u) {
        const device bfloat *a = input + ulong(route / p.input_repeat) * p.input_width;
        for (uint c = 0u; c < 8u; ++c) {
            const device uchar *w = values + (first_weight + c) * packed_width;
            const device uchar *s = scales + (first_weight + c) * blocks;
            float sum = 0.0f;
            for (uint k = lane; k < p.input_width; k += 32u)
                sum = fma(float(a[k]), float(antfly_routed_weight(w, s, k, fp4, outer)), sum);
            const float dot = simd_sum(sum);
            if (lane == 0u) output[ulong(route) * p.output_width + first_col + c] = bfloat(dot);
        }
        return;
    }

    threadgroup bfloat activation_tile[8][32];
    threadgroup bfloat weight_tile[8][32];
    simdgroup_float8x8 acc = make_filled_simdgroup_matrix<float, 8>(0.0f);
    for (uint base = 0u; base < p.input_width; base += 32u) {
        const uint k = base + lane;
        for (uint i = 0u; i < 8u; ++i) {
            const uint src = i < count ? entry[1u + i] / p.input_repeat : 0u;
            activation_tile[i][lane] = i < count ? input[ulong(src) * p.input_width + k] : bfloat(0.0f);
            const device uchar *w = values + (first_weight + i) * packed_width;
            const device uchar *s = scales + (first_weight + i) * blocks;
            weight_tile[i][lane] = antfly_routed_weight(w, s, k, fp4, outer);
        }
        simdgroup_barrier(mem_flags::mem_threadgroup);
        for (uint tile = 0u; tile < 32u; tile += 8u) {
            simdgroup_matrix<bfloat, 8, 8> a, b;
            simdgroup_load(a, &activation_tile[0][tile], 32u);
            simdgroup_load(b, &weight_tile[0][tile], 32u, ulong2(0, 0), true);
            simdgroup_multiply_accumulate(acc, a, b, acc);
        }
        simdgroup_barrier(mem_flags::mem_threadgroup);
    }
    threadgroup float dots[64];
    simdgroup_store(acc, dots, 8u);
    simdgroup_barrier(mem_flags::mem_threadgroup);
    for (uint i = lane; i < count * 8u; i += 32u)
        output[ulong(entry[1u + i / 8u]) * p.output_width + first_col + i % 8u] = bfloat(dots[i]);
}
