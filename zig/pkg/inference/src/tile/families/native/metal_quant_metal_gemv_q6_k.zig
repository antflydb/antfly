// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q6_k_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 210u; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 210u; const device uchar *ql = weight + off; const device uchar *qh = weight + off + 128u; const device char *scales = reinterpret_cast<const device char *>(weight + off + 192u); ushort d_bits = (ushort(weight[off + 209]) << 8) | ushort(weight[off + 208]); float d = float(as_type<half>(d_bits)); for (uint sub = 0; sub < 16; ++sub) { float scale = d * float(scales[sub]); uint half_idx = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint ql_off = half_idx * 64u + (group & 1u) * 32u; uint qh_off = half_idx * 32u; uint qh_shift = group * 2u; uint nibble_shift = (group / 2u) * 4u; uint input_off = r * p.in_dim + b * 256u + sub * 16u; for (uint i = 0; i < 16; ++i) { uint l = l_base + i; int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0Fu); int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u); int q = (low4 | (high2 << 4)) - 32; acc += input[input_off + i] * (scale * float(q)); } } }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\
;
