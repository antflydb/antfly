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
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q1_0_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 18u; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 18u; ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); float signed_sum = 0.0f; uint in_off = r * p.in_dim + b * 128u; for (uint by = 0; by < 16u; ++by) { uchar sbits = weight[off + 2u + by]; uint base = in_off + by * 8u; signed_sum += (sbits & 0x01u) != 0u ? input[base] : -input[base]; signed_sum += (sbits & 0x02u) != 0u ? input[base + 1u] : -input[base + 1u]; signed_sum += (sbits & 0x04u) != 0u ? input[base + 2u] : -input[base + 2u]; signed_sum += (sbits & 0x08u) != 0u ? input[base + 3u] : -input[base + 3u]; signed_sum += (sbits & 0x10u) != 0u ? input[base + 4u] : -input[base + 4u]; signed_sum += (sbits & 0x20u) != 0u ? input[base + 5u] : -input[base + 5u]; signed_sum += (sbits & 0x40u) != 0u ? input[base + 6u] : -input[base + 6u]; signed_sum += (sbits & 0x80u) != 0u ? input[base + 7u] : -input[base + 7u]; } acc += d * signed_sum; }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_i8_s_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { char q = as_type<char>(weight[row_offset + b]); acc += input[r * p.in_dim + b] * float(q); }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q4_0_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 18u; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 18u; ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); const device uchar *qs = weight + off + 2u; uint in_off = r * p.in_dim + b * 32u; for (uint i = 0; i < 16; ++i) { uchar packed = qs[i]; float lo = float(int(packed & 0x0Fu) - 8); acc += input[in_off + i] * (d * lo); } for (uint i = 0; i < 16; ++i) { uchar packed = qs[i]; float hi = float(int((packed >> 4) & 0x0Fu) - 8); acc += input[in_off + 16u + i] * (d * hi); } }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_EXPERT_SIMD(termite_q4_0_linear_id) "\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return; uint expert = expert_ids[r];\n"
    \\           "    if (expert >= p.expert_count) { if (lane == 0u) { uint idx = r * p.out_dim + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) if (first_o + row < p.out_dim) output[idx + row] = 0.0f; } return; }\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint ib0 = ix; uint input_row = p.reserved == 0u ? r : r / p.reserved; uint in_row = input_row * p.in_dim; uint expert_row = expert * p.source_out_dim + p.row_offset; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
    \\           "    for (uint b = ib0; b < p.row_blocks; b += NQ) { float yl[16]; float sumy = 0.0f; uint in_off = in_row + b * 32u + il; TERMITE_FOR_UNROLL (uint i = 0u; i < 8u; i += 2u) { float x0 = input[in_off + i]; float x1 = input[in_off + i + 1u]; float x16 = input[in_off + 16u + i]; float x17 = input[in_off + 16u + i + 1u]; sumy += x0 + x1 + x16 + x17; yl[i] = x0; yl[i + 1u] = x1 / 256.0f; yl[i + 8u] = x16 / 16.0f; yl[i + 9u] = x17 / 4096.0f; } TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = (expert_row + o) * p.row_blocks * 18u + b * 18u; ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); const device ushort *qs = reinterpret_cast<const device ushort *>(weight + off + 2u) + (il >> 1); float qacc[4] = {0.0f, 0.0f, 0.0f, 0.0f}; TERMITE_FOR_UNROLL (uint i = 0u; i < 8u; i += 2u) { uint packed = uint(qs[i >> 1]); qacc[0] += yl[i] * float(packed & 0x000Fu); qacc[1] += yl[i + 1u] * float(packed & 0x0F00u); qacc[2] += yl[i + 8u] * float(packed & 0x00F0u); qacc[3] += yl[i + 9u] * float(packed & 0xF000u); } acc[row] += d * (qacc[0] + qacc[1] + qacc[2] + qacc[3] - 8.0f * sumy); } }\n"
    \\           "    TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { uint idx = r * p.out_dim + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) if (first_o + row < p.out_dim) output[idx + row] = acc[row]; }\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_QUANT_EXPERT_SIMD(termite_q4_0_linear_id_a4b_gate_up) "\n"
    \\           "    termite_q4_0_a4b_projection_impl<88u, 1408u, false>(input, weight, output, expert_ids, p, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_EXPERT_SIMD(termite_q4_0_linear_id_a4b_down) "\n"
    \\           "    termite_q4_0_a4b_projection_impl<22u, 2816u, true>(input, weight, output, expert_ids, p, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;
