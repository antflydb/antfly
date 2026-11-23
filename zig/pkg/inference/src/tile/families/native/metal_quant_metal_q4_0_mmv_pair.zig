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

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_SIMD({{name}}) " termite_q4_0_pair_activation_mmv_impl(input, weight_gate, weight_up, output, p, ap, lane, sgitg, tg, {{literal_0}}, {{literal_1}}); }\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_q4_0_pair_activation_1x_reduce_nr4_nsg2", .literal_0 = "4u", .literal_1 = "2u" }), program.render(program_0, .{ .name = "termite_q4_0_pair_activation_1x_reduce_nr8_nsg2", .literal_0 = "8u", .literal_1 = "2u" }), program.render(program_0, .{ .name = "termite_q4_0_pair_activation_1x_reduce_nr4_nsg4", .literal_0 = "4u", .literal_1 = "4u" }), program.render(program_0, .{ .name = "termite_q4_0_pair_activation_1x_reduce_nr8_nsg4", .literal_0 = "8u", .literal_1 = "4u" }) });

pub const source_1 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_SIMD(termite_q4_0_pair_activation_1x_reduce_nr4_nsg2_nq8) "\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 8u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * NQ; uint in_row = r * p.in_dim; float gate[4] = {0.0f, 0.0f, 0.0f, 0.0f}; float up[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
    \\           "    for (uint b = ix; b < p.row_blocks; b += NQ) { float x[8]; uint in_off = in_row + b * 32u + il; for (uint i = 0u; i < NQ; ++i) x[i] = input[in_off + i]; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 18u + b * 18u; ushort gate_bits = (ushort(weight_gate[off + 1u]) << 8) | ushort(weight_gate[off]); ushort up_bits = (ushort(weight_up[off + 1u]) << 8) | ushort(weight_up[off]); float d_gate = float(as_type<half>(gate_bits)); float d_up = float(as_type<half>(up_bits)); const device uchar *qs_gate = weight_gate + off + 2u; const device uchar *qs_up = weight_up + off + 2u; float block_gate = 0.0f; float block_up = 0.0f; for (uint i = 0u; i < NQ; ++i) { uint j = il + i; uchar packed_gate = qs_gate[j & 15u]; uchar packed_up = qs_up[j & 15u]; float q_gate = j < 16u ? float(int(packed_gate & 0x0Fu) - 8) : float(int(packed_gate >> 4) - 8); float q_up = j < 16u ? float(int(packed_up & 0x0Fu) - 8) : float(int(packed_up >> 4) - 8); block_gate += x[i] * q_gate; block_up += x[i] * q_up; } gate[row] += d_gate * block_gate; up[row] += d_up * block_up; } }\n"
    \\           "    termite_q4_0_pair_simd_publish(output, p, ap, NR0, r, first_o, lane, gate, up);\n"
    \\           "}\n"
    \\
;

pub const source_2 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_SIMD(termite_q4_0_pair_activation_1x_packed_t4_nsg2) "\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint in_row = r * p.in_dim; float gate[4] = {0.0f, 0.0f, 0.0f, 0.0f}; float up[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
    \\           "    for (uint b = ix; b < p.row_blocks; b += NQ) { float yl[16]; float sumy = 0.0f; uint in_off = in_row + b * 32u + il; for (uint i = 0u; i < 8u; i += 2u) { float x0 = input[in_off + i]; float x1 = input[in_off + i + 1u]; float x16 = input[in_off + 16u + i]; float x17 = input[in_off + 16u + i + 1u]; sumy += x0 + x1 + x16 + x17; yl[i] = x0; yl[i + 1u] = x1 / 256.0f; yl[i + 8u] = x16 / 16.0f; yl[i + 9u] = x17 / 4096.0f; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = (((o >> 2u) * p.row_blocks + b) * 4u + (o & 3u)) * 18u; ushort gate_bits = (ushort(weight_gate[off + 1u]) << 8) | ushort(weight_gate[off]); ushort up_bits = (ushort(weight_up[off + 1u]) << 8) | ushort(weight_up[off]); float d_gate = float(as_type<half>(gate_bits)); float d_up = float(as_type<half>(up_bits)); const device ushort *qs_gate = reinterpret_cast<const device ushort *>(weight_gate + off + 2u) + (il >> 1); const device ushort *qs_up = reinterpret_cast<const device ushort *>(weight_up + off + 2u) + (il >> 1); float qacc_gate = 0.0f; float qacc_up = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { uint packed_gate = uint(qs_gate[i >> 1]); uint packed_up = uint(qs_up[i >> 1]); qacc_gate += yl[i] * float(packed_gate & 0x000Fu); qacc_gate += yl[i + 1u] * float(packed_gate & 0x0F00u); qacc_gate += yl[i + 8u] * float(packed_gate & 0x00F0u); qacc_gate += yl[i + 9u] * float(packed_gate & 0xF000u); qacc_up += yl[i] * float(packed_up & 0x000Fu); qacc_up += yl[i + 1u] * float(packed_up & 0x0F00u); qacc_up += yl[i + 8u] * float(packed_up & 0x00F0u); qacc_up += yl[i + 9u] * float(packed_up & 0xF000u); } gate[row] += d_gate * (qacc_gate - 8.0f * sumy); up[row] += d_up * (qacc_up - 8.0f * sumy); } }\n"
    \\           "    termite_q4_0_pair_simd_publish(output, p, ap, NR0, r, first_o, lane, gate, up);\n"
    \\           "}\n"
    \\
;

pub const source_3 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_PAIR_REDUCE(termite_q4_0_pair_activation_1x_reduce) "\n"
    \\           "    termite_q4_0_pair_activation_reduce<float>(input, weight_gate, weight_up, output, p, ap, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
    ,

    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_F16(termite_q4_0_pair_activation_1x_reduce_out_f16) "\n"
    \\           "    termite_q4_0_pair_activation_reduce<half>(input, weight_gate, weight_up, output, p, ap, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
    ,

    \\           "kernel void termite_q4_0_pair_activation_rms_scale_1x_reduce_out_f16(device const float *input [[buffer(0)]], device const uchar *weight_gate [[buffer(1)]], device const uchar *weight_up [[buffer(2)]], device half *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], constant termite_metal_apply_activation_params &ap [[buffer(5)]], device const float *norm_weight [[buffer(6)]], device const float *inv_scale [[buffer(7)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint ib0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; float scale = inv_scale[r]; float gate[4] = {0.0f, 0.0f, 0.0f, 0.0f}; float up[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
    \\           "    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float x[8]; uint qj0 = il * NQ; uint in_off = in_row + b * 32u + qj0; uint norm_off = b * 32u + qj0; for (uint i = 0u; i < NQ; ++i) { x[i] = input[in_off + i] * norm_weight[norm_off + i] * scale; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 18u + b * 18u; ushort gate_bits = (ushort(weight_gate[off + 1]) << 8) | ushort(weight_gate[off]); ushort up_bits = (ushort(weight_up[off + 1]) << 8) | ushort(weight_up[off]); float d_gate = float(as_type<half>(gate_bits)); float d_up = float(as_type<half>(up_bits)); const device uchar *qs_gate = weight_gate + off + 2u; const device uchar *qs_up = weight_up + off + 2u; float block_gate = 0.0f; float block_up = 0.0f; for (uint i = 0u; i < NQ; ++i) { uint j = qj0 + i; uchar packed_gate = qs_gate[j & 15u]; uchar packed_up = qs_up[j & 15u]; float q_gate = j < 16u ? float(int(packed_gate & 0x0Fu) - 8) : float(int((packed_gate >> 4) & 0x0Fu) - 8); float q_up = j < 16u ? float(int(packed_up & 0x0Fu) - 8) : float(int((packed_up >> 4) & 0x0Fu) - 8); block_gate += x[i] * q_gate; block_up += x[i] * q_up; } gate[row] += d_gate * block_gate; up[row] += d_up * block_up; } }\n"
    \\           "    if (sgitg == 0u) { for (uint row = 0u; row < NR0; ++row) { shmem[row * 64u + lane] = 0.0f; shmem[row * 64u + 32u + lane] = 0.0f; } } for (uint row = 0u; row < NR0; ++row) { gate[row] = simd_sum(gate[row]); up[row] = simd_sum(up[row]); } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    if (lane == 0u) { for (uint row = 0u; row < NR0; ++row) { shmem[row * 64u + sgitg] = gate[row]; shmem[row * 64u + 32u + sgitg] = up[row]; } } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    float total_gate[4]; float total_up[4]; for (uint row = 0u; row < NR0; ++row) { total_gate[row] = simd_sum(shmem[row * 64u + lane]); total_up[row] = simd_sum(shmem[row * 64u + 32u + lane]); } if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = half(termite_gated_activation_product(total_gate[row], total_up[row], ap.activation_kind)); } }\n"
    \\           "}\n"
    \\
});

pub const source_4 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_RHS_REDUCE(termite_q4_0_activation_rhs_1x_reduce) "\n"
    \\           "    termite_q4_0_activation_rhs_reduce<float, 16u>(input, weight, rhs, output, p, ap, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
    ,

    \\           "kernel void termite_q4_0_activation_rhs_1x_reduce_out_f16(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device const float *rhs [[buffer(2)]], device half *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], constant termite_metal_apply_activation_params &ap [[buffer(5)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    termite_q4_0_activation_rhs_reduce<half, 4u>(input, weight, rhs, output, p, ap, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
});
