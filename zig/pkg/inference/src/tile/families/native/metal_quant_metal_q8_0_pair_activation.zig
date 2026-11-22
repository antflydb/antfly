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

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_REDUCE(termite_q8_0_pair_activation_multiply_1x_reduce4) "\n"
    \\           "    weight_gate += p.weight_offset; weight_up += p.second_weight_offset;\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint ib0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; float gate0 = 0.0f; float up0 = 0.0f; float gate1 = 0.0f; float up1 = 0.0f; float gate2 = 0.0f; float up2 = 0.0f; float gate3 = 0.0f; float up3 = 0.0f;\n"
    \\           "    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float x[8]; uint in_off = in_row + b * 32u + il * NQ; for (uint i = 0; i < NQ; ++i) { x[i] = input[in_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 34u + b * 34u; ushort bits_g = (ushort(weight_gate[off + 1]) << 8) | ushort(weight_gate[off]); ushort bits_u = (ushort(weight_up[off + 1]) << 8) | ushort(weight_up[off]); float dg = float(as_type<half>(bits_g)); float du = float(as_type<half>(bits_u)); float block_g = 0.0f; float block_u = 0.0f; for (uint i = 0; i < NQ; ++i) { float xv = x[i]; block_g += xv * float(as_type<char>(weight_gate[off + 2u + il * NQ + i])); block_u += xv * float(as_type<char>(weight_up[off + 2u + il * NQ + i])); } if (row == 0u) { gate0 += dg * block_g; up0 += du * block_u; } else if (row == 1u) { gate1 += dg * block_g; up1 += du * block_u; } else if (row == 2u) { gate2 += dg * block_g; up2 += du * block_u; } else { gate3 += dg * block_g; up3 += du * block_u; } } }\n"
    \\           "    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; shmem[128u + lane] = 0.0f; shmem[160u + lane] = 0.0f; shmem[192u + lane] = 0.0f; shmem[224u + lane] = 0.0f; } gate0 = simd_sum(gate0); up0 = simd_sum(up0); gate1 = simd_sum(gate1); up1 = simd_sum(up1); gate2 = simd_sum(gate2); up2 = simd_sum(up2); gate3 = simd_sum(gate3); up3 = simd_sum(up3); threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    if (lane == 0u) { shmem[sgitg] = gate0; shmem[32u + sgitg] = up0; shmem[64u + sgitg] = gate1; shmem[96u + sgitg] = up1; shmem[128u + sgitg] = gate2; shmem[160u + sgitg] = up2; shmem[192u + sgitg] = gate3; shmem[224u + sgitg] = up3; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    float total_gate0 = simd_sum(shmem[lane]); float total_up0 = simd_sum(shmem[32u + lane]); float total_gate1 = simd_sum(shmem[64u + lane]); float total_up1 = simd_sum(shmem[96u + lane]); float total_gate2 = simd_sum(shmem[128u + lane]); float total_up2 = simd_sum(shmem[160u + lane]); float total_gate3 = simd_sum(shmem[192u + lane]); float total_up3 = simd_sum(shmem[224u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) output[idx] = termite_gated_activation_product(total_gate0, total_up0, ap.activation_kind); if (first_o + 1u < p.out_dim) output[idx + 1u] = termite_gated_activation_product(total_gate1, total_up1, ap.activation_kind); if (first_o + 2u < p.out_dim) output[idx + 2u] = termite_gated_activation_product(total_gate2, total_up2, ap.activation_kind); if (first_o + 3u < p.out_dim) output[idx + 3u] = termite_gated_activation_product(total_gate3, total_up3, ap.activation_kind); }\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_SIMD(termite_q8_0_pair_activation_multiply_1r_ext) "\n"
    \\           "    termite_q8_0_pair_scalar_impl<false, float>(input, weight_gate, weight_up, output, p, ap, input, input, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           "kernel void termite_q8_0_pair_activation_multiply_rms_scale_1r_ext(device const float *input [[buffer(0)]], device const uchar *weight_gate [[buffer(1)]], device const uchar *weight_up [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], constant termite_metal_apply_activation_params &ap [[buffer(5)]], device const float *norm_weight [[buffer(6)]], device const float *inv_scale [[buffer(7)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    termite_q8_0_pair_scalar_impl<true, float>(input, weight_gate, weight_up, output, p, ap, norm_weight, inv_scale, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           "kernel void termite_q8_0_pair_activation_multiply_rms_scale_1r_ext_out_f16(device const float *input [[buffer(0)]], device const uchar *weight_gate [[buffer(1)]], device const uchar *weight_up [[buffer(2)]], device half *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], constant termite_metal_apply_activation_params &ap [[buffer(5)]], device const float *norm_weight [[buffer(6)]], device const float *inv_scale [[buffer(7)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    termite_q8_0_pair_scalar_impl<true, half>(input, weight_gate, weight_up, output, p, ap, norm_weight, inv_scale, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;
