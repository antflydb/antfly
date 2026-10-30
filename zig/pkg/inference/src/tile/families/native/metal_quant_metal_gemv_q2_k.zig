// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q2_k_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 84u; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 84u; const device uchar *scales_raw = weight + off; ushort d_bits = (ushort(weight[off + 17]) << 8) | ushort(weight[off + 16]); ushort dmin_bits = (ushort(weight[off + 19]) << 8) | ushort(weight[off + 18]); float d = float(as_type<half>(d_bits)); float dmin = float(as_type<half>(dmin_bits)); const device uchar *qs = weight + off + 20u; for (uint sub = 0; sub < 16; ++sub) { float sc = float(scales_raw[sub] & 0x0Fu); float m = float(scales_raw[sub] >> 4); float dsc = d * sc; float dmn = dmin * m; uint chunk = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint q_base = chunk * 32u; uint shift = group * 2u; uint input_off = r * p.in_dim + b * 256u + sub * 16u; for (uint i = 0; i < 16; ++i) { int q = int((qs[q_base + l_base + i] >> shift) & 0x03u); acc += input[input_off + i] * (dsc * float(q) - dmn); } } }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q3_k_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 110u; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 110u; const device uchar *hmask = weight + off; const device uchar *qs = weight + off + 32u; const device uchar *scale_data = weight + off + 96u; ushort d_bits = (ushort(weight[off + 109]) << 8) | ushort(weight[off + 108]); float d = float(as_type<half>(d_bits)); int raw_scales[16]; for (uint i = 0; i < 4; ++i) { raw_scales[i] = int(scale_data[i] & 0x0Fu) | (int(scale_data[8 + i] & 0x03u) << 4); raw_scales[i + 4] = int(scale_data[4 + i] & 0x0Fu) | (int((scale_data[8 + i] >> 2) & 0x03u) << 4); raw_scales[i + 8] = int((scale_data[i] >> 4) & 0x0Fu) | (int((scale_data[8 + i] >> 4) & 0x03u) << 4); raw_scales[i + 12] = int((scale_data[4 + i] >> 4) & 0x0Fu) | (int((scale_data[8 + i] >> 6) & 0x03u) << 4); } for (uint sub = 0; sub < 16; ++sub) { float scale = d * float(raw_scales[sub] - 32); uint chunk = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint q_base = chunk * 32u; uint shift = group * 2u; uint hm_bit = chunk * 4u + group; uint input_off = r * p.in_dim + b * 256u + sub * 16u; for (uint i = 0; i < 16; ++i) { uint l = l_base + i; int low2 = int((qs[q_base + l] >> shift) & 0x03u); int high1 = int((hmask[l] >> hm_bit) & 0x01u); int q = low2 + high1 * 4 - 4; acc += input[input_off + i] * (scale * float(q)); } } }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q4_k_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_q4_k, 144u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_k_linear_1x_reduce) "\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
    \\           "    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint input_off = in_row + b * 256u + sub * 32u + j0; float x[8]; for (uint i = 0; i < NQ; ++i) { x[i] = input[input_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; float block = termite_q4k_scalar_dot8(x, weight, p.row_blocks, o, b, sub, chunk, high, j0); acc[row] += block; } }\n"
    \\           "    if (sgitg == 0u) { for (uint row = 0u; row < NR0; ++row) shmem[row * 32u + lane] = 0.0f; } for (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    if (lane == 0u) { for (uint row = 0u; row < NR0; ++row) shmem[row * 32u + sgitg] = acc[row]; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    float total[4]; for (uint row = 0u; row < NR0; ++row) total[row] = simd_sum(shmem[row * 32u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = total[row]; } }\n"
    \\           "}\n"
    \\
;
