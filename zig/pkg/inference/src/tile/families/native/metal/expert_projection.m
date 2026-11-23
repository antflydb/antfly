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

// native-program:entry termite_moe_merged_gate_up_activation
           "kernel void termite_moe_merged_gate_up_activation(device const float *merged [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_apply_activation_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {\n"
           "    uint total = p.rows * p.dim; if (gid >= total) return; uint r = gid / p.dim; uint c = gid - r * p.dim; uint base = r * p.dim * 2u; output[gid] = termite_gated_activation_product(merged[base + c], merged[base + p.dim + c], p.activation_kind);\n"
           "}\n"
// native-program:entry termite_moe_scatter_add
           "kernel void termite_moe_scatter_add(device const float *base [[buffer(0)]], device const float *updates [[buffer(1)]], device const uint *row_ids [[buffer(2)]], device const float *route_weights [[buffer(3)]], device float *output [[buffer(4)]], constant termite_metal_moe_scatter_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {\n"
           "    uint total = p.base_rows * p.dim; if (gid >= total) return; uint row = gid / p.dim; uint col = gid % p.dim; float value = base[gid]; for (uint route = 0u; route < p.route_rows; ++route) { if (row_ids[route] == row) value += route_weights[route] * updates[route * p.dim + col]; } output[gid] = value;\n"
           "}\n"
// native-program:entry termite_moe_route_select
           "kernel void termite_moe_route_select(device const float *logits [[buffer(0)]], device uint *route_ids [[buffer(1)]], device float *route_weights [[buffer(2)]], constant termite_metal_moe_route_select_params &p [[buffer(3)]], uint row [[thread_position_in_grid]]) {\n"
           "    if (row >= p.rows || p.num_experts == 0u || p.top_k == 0u || p.top_k > 8u) return;\n"
           "    const uint base = row * p.num_experts;\n"
           "    float row_max = -3.4028234663852886e+38f;\n"
           "    for (uint expert = 0u; expert < p.num_experts; ++expert) row_max = max(row_max, logits[base + expert] * p.logit_scale);\n"
           "    float denom = 0.0f;\n"
           "    for (uint expert = 0u; expert < p.num_experts; ++expert) denom += termite_exp_approx(logits[base + expert] * p.logit_scale - row_max);\n"
           "    denom = max(denom, 1.0e-30f);\n"
           "    uint selected_ids[8];\n"
           "    float selected_probs[8];\n"
           "    float selected_sum = 0.0f;\n"
           "    for (uint slot = 0u; slot < p.top_k; ++slot) {\n"
           "        uint best_id = 0u; float best_prob = -3.4028234663852886e+38f;\n"
           "        for (uint expert = 0u; expert < p.num_experts; ++expert) {\n"
           "            bool used = false;\n"
           "            for (uint prior = 0u; prior < slot; ++prior) used = used || selected_ids[prior] == expert;\n"
           "            if (used) continue;\n"
           "            float prob = termite_exp_approx(logits[base + expert] * p.logit_scale - row_max) / denom;\n"
           "            if (prob > best_prob) { best_prob = prob; best_id = expert; }\n"
           "        }\n"
           "        selected_ids[slot] = best_id; selected_probs[slot] = best_prob; selected_sum += best_prob;\n"
           "    }\n"
           "    const float inv_selected_sum = selected_sum > 0.0f ? 1.0f / selected_sum : 0.0f;\n"
           "    const uint route_base = row * p.top_k;\n"
           "    for (uint slot = 0u; slot < p.top_k; ++slot) { route_ids[route_base + slot] = selected_ids[slot]; route_weights[route_base + slot] = selected_probs[slot] * inv_selected_sum; }\n"
           "}\n"
// native-program:entry termite_moe_route_select_tg_map
           "kernel void termite_moe_route_select_tg_map(device const float *logits [[buffer(0)]], device uint *route_ids [[buffer(1)]], device float *route_weights [[buffer(2)]], constant termite_metal_moe_route_select_params &p [[buffer(3)]], device const uint *expert_to_slot [[buffer(4)]], device uint *route_slots [[buffer(5)]], device uint *miss_rows [[buffer(6)]], ushort lane [[thread_index_in_simdgroup]], uint3 tg [[threadgroup_position_in_grid]]) { threadgroup float shared_values[2]; threadgroup float expert_exps[128]; threadgroup float expert_probs[128]; threadgroup uint selected_ids_tg[8]; threadgroup float selected_probs_tg[8]; termite_moe_route_select_tg_impl(logits, route_ids, route_weights, p, expert_to_slot, route_slots, miss_rows, true, shared_values, expert_exps, expert_probs, selected_ids_tg, selected_probs_tg, lane, tg); }\n"
// native-program:entry termite_moe_route_map_slots
           "kernel void termite_moe_route_map_slots(device const uint *route_ids [[buffer(0)]], device const uint *expert_to_slot [[buffer(1)]], device uint *route_slots [[buffer(2)]], device uint *miss_rows [[buffer(3)]], constant termite_metal_moe_route_map_slots_params &p [[buffer(4)]], uint row [[thread_position_in_grid]]) {\n"
           "    if (row >= p.rows || p.num_experts == 0u || p.top_k == 0u) return;\n"
           "    const uint base = row * p.top_k;\n"
           "    uint misses = 0u;\n"
           "    for (uint route = 0u; route < p.top_k; ++route) {\n"
           "        const uint expert = route_ids[base + route];\n"
           "        const uint mapped = expert < p.num_experts ? expert_to_slot[expert] : 0xFFFFFFFFu;\n"
           "        route_slots[base + route] = mapped;\n"
           "        misses += mapped == 0xFFFFFFFFu ? 1u : 0u;\n"
           "    }\n"
           "    miss_rows[row] = misses;\n"
           "}\n"
// native-program:entry termite_moe_slot_q4_0_gate_up
           "kernel void termite_moe_slot_q4_0_gate_up(device const float *input [[buffer(0)]], device const uchar *arena [[buffer(1)]], device const uint *route_slots [[buffer(2)]], device float *gate_out [[buffer(3)]], constant termite_metal_moe_slot_q4_0_gate_up_params &p [[buffer(4)]], device float *up_out [[buffer(5)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
           "    const uint NR0 = 2u; const uint NSG = 4u; const uint NQ = 16u; const uint NW = 32u; uint route = tg.y; uint first_o = tg.x * NR0; if (route >= p.route_rows || lane >= NW) return;\n"
           "    uint slot = route_slots[route]; uint input_row = p.top_k == 0u ? 0u : route / p.top_k; bool mapped = slot < p.slot_count && input_row < p.total_rows;\n"
           "    if (!mapped) { if (lane == 0u && sgitg == 0u) { uint idx = route * p.out_dim + first_o; if (first_o < p.out_dim) { gate_out[idx] = 0.0f; up_out[idx] = 0.0f; } if (first_o + 1u < p.out_dim) { gate_out[idx + 1u] = 0.0f; up_out[idx + 1u] = 0.0f; } } return; }\n"
           "    ulong expert_base = ulong(slot) * ulong(p.expert_stride); device const uchar *weight_gate = arena + expert_base + ulong(p.gate_offset); device const uchar *weight_up = arena + expert_base + ulong(p.up_offset);\n"
           "    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint ib0 = sgitg * NQ + ix; uint in_row = input_row * p.in_dim; float gate0 = 0.0f; float gate1 = 0.0f; float up0 = 0.0f; float up1 = 0.0f;\n"
           "    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float yl[16]; float sumy = 0.0f; uint in_off = in_row + b * 32u + il; "
           TERMITE_MSL_Q4_YL16("for")
           " for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 18u + b * 18u; ushort gate_bits = (ushort(weight_gate[off + 1u]) << 8) | ushort(weight_gate[off]); ushort up_bits = (ushort(weight_up[off + 1u]) << 8) | ushort(weight_up[off]); float d_gate = float(as_type<half>(gate_bits)); float d_up = float(as_type<half>(up_bits)); const device ushort *qs_gate = reinterpret_cast<const device ushort *>(weight_gate + off + 2u) + (il >> 1); const device ushort *qs_up = reinterpret_cast<const device ushort *>(weight_up + off + 2u) + (il >> 1); float qacc_gate = 0.0f; float qacc_up = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { uint packed_gate = uint(qs_gate[i >> 1]); uint packed_up = uint(qs_up[i >> 1]); qacc_gate += yl[i] * float(packed_gate & 0x000Fu); qacc_gate += yl[i + 1u] * float(packed_gate & 0x0F00u); qacc_gate += yl[i + 8u] * float(packed_gate & 0x00F0u); qacc_gate += yl[i + 9u] * float(packed_gate & 0xF000u); qacc_up += yl[i] * float(packed_up & 0x000Fu); qacc_up += yl[i + 1u] * float(packed_up & 0x0F00u); qacc_up += yl[i + 8u] * float(packed_up & 0x00F0u); qacc_up += yl[i + 9u] * float(packed_up & 0xF000u); } if (row == 0u) { gate0 += d_gate * (qacc_gate - 8.0f * sumy); up0 += d_up * (qacc_up - 8.0f * sumy); } else { gate1 += d_gate * (qacc_gate - 8.0f * sumy); up1 += d_up * (qacc_up - 8.0f * sumy); } } }\n"
           "    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; } gate0 = simd_sum(gate0); gate1 = simd_sum(gate1); up0 = simd_sum(up0); up1 = simd_sum(up1); threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    if (lane == 0u) { shmem[sgitg] = gate0; shmem[32u + sgitg] = gate1; shmem[64u + sgitg] = up0; shmem[96u + sgitg] = up1; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    float total_gate0 = simd_sum(shmem[lane]); float total_gate1 = simd_sum(shmem[32u + lane]); float total_up0 = simd_sum(shmem[64u + lane]); float total_up1 = simd_sum(shmem[96u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = route * p.out_dim + first_o; if (first_o < p.out_dim) { gate_out[idx] = total_gate0; up_out[idx] = total_up0; } if (first_o + 1u < p.out_dim) { gate_out[idx + 1u] = total_gate1; up_out[idx + 1u] = total_up1; } }\n"
           "}\n"
// native-program:entry termite_moe_slot_q4_0_down
           "kernel void termite_moe_slot_q4_0_down(device const float *input [[buffer(0)]], device const uchar *arena [[buffer(1)]], device const uint *route_slots [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_moe_slot_q4_0_down_params &p [[buffer(4)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
           "    const uint NR0 = 8u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; uint route = tg.y; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; if (route >= p.route_rows || lane >= NW) return; uint slot = route_slots[route]; bool mapped = slot < p.slot_count;\n"
           "    if (!mapped) { if (lane == 0u) { uint idx = route * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) if (first_o + row < p.out_dim) output[idx + row] = 0.0f; } return; }\n"
           "    device const uchar *weight_down = arena + ulong(slot) * ulong(p.expert_stride) + ulong(p.down_offset); uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint in_row = route * p.in_dim; float acc[8] = {0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};\n"
           "    for (uint b = ix; b < p.row_blocks; b += NQ) { float yl[16]; float sumy = 0.0f; uint in_off = in_row + b * 32u + il; "
           TERMITE_MSL_Q4_YL16("for")
           " for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 18u + b * 18u; ushort bits = (ushort(weight_down[off + 1u]) << 8) | ushort(weight_down[off]); float d = float(as_type<half>(bits)); const device ushort *qs = reinterpret_cast<const device ushort *>(weight_down + off + 2u) + (il >> 1); float qacc = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { uint packed = uint(qs[i >> 1]); qacc += yl[i] * float(packed & 0x000Fu); qacc += yl[i + 1u] * float(packed & 0x0F00u); qacc += yl[i + 8u] * float(packed & 0x00F0u); qacc += yl[i + 9u] * float(packed & 0xF000u); } acc[row] += d * (qacc - 8.0f * sumy); } }\n"
           "    for (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { uint idx = route * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) if (first_o + row < p.out_dim) output[idx + row] = acc[row]; }\n"
           "}\n"
// native-program:entry termite_moe_slot_reduce
           "kernel void termite_moe_slot_reduce(device const float *route_output [[buffer(0)]], device const uint *expert_ids [[buffer(1)]], device const float *route_weights [[buffer(2)]], device const float *expert_scale [[buffer(3)]], device float *output [[buffer(4)]], constant termite_metal_moe_slot_reduce_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {\n"
           "#pragma clang fp contract(off)\n"
           "    uint total = p.rows * p.dim; if (gid >= total) return; uint row = gid / p.dim; uint col = gid % p.dim; float value = 0.0f; uint route_base = row * p.top_k; for (uint k = 0u; k < p.top_k; ++k) { uint route = route_base + k; float scale = p.has_expert_scale != 0u ? expert_scale[expert_ids[route]] : 1.0f; value += (route_output[route * p.dim + col] * scale) * route_weights[route]; } output[gid] = value;\n"
           "}\n"
