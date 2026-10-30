// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_iq4_nl_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_iq4_nl, 18u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_iq4_nl_linear_1x_reduce) "\n"
    \\           "    termite_legacy_reduce<termite_legacy_iq4_nl, 18u, 2u, 4u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_iq4_xs_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_iq4_xs, 136u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_iq4_xs_linear_1x_reduce) "\n"
    \\           "    const uint NR0 = 2u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; constexpr int iq4_nl[16] = {-127, -104, -83, -65, -49, -35, -22, -10, 1, 13, 25, 38, 53, 69, 89, 113}; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = uint(sgitg) * 8u + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; float acc0 = 0.0f; float acc1 = 0.0f;\n"
    \\           "    for (uint sb = sb0; sb < total_subblocks; sb += NSG * 8u) { uint b = sb >> 3; uint sub = sb & 7u; uint qj0 = il * NQ; uint in_off = in_row + b * 256u + sub * 32u + qj0; float x[8]; for (uint i = 0u; i < NQ; ++i) { x[i] = input[in_off + i]; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 136u + b * 136u; ushort d_bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); float d = float(as_type<half>(d_bits)); ushort scales_h = (ushort(weight[off + 3u]) << 8) | ushort(weight[off + 2u]); const device uchar *scales_l = weight + off + 4u; const device uchar *qs = weight + off + 8u; uint low = (uint(scales_l[sub >> 1u]) >> ((sub & 1u) * 4u)) & 0x0Fu; uint high = (uint(scales_h) >> (sub * 2u)) & 0x03u; float dl = d * float(int(low | (high << 4)) - 32); uint q_base = sub * 16u; float block = 0.0f; for (uint i = 0u; i < NQ; ++i) { uint j = qj0 + i; uchar packed = qs[q_base + (j & 15u)]; uint code = j < 16u ? uint(packed & 0x0Fu) : uint(packed >> 4); block += x[i] * float(iq4_nl[code]); } if (row == 0u) acc0 += dl * block; else acc1 += dl * block; } }\n"
    \\           "    float acc[2] = {acc0, acc1};\n"
    \\           "    termite_legacy_reduce_store<2u>(acc, shmem, output, p, r, first_o, lane, sgitg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_mxfp4_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_mxfp4, 17u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_mxfp4_linear_1x_reduce) "\n"
    \\           "    termite_legacy_reduce<termite_legacy_mxfp4, 17u, 2u, 4u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;
