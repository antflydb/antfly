// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_nvfp4_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 36u; float acc = 0.0f; constexpr int lut[16] = {0, 1, 2, 3, 4, 6, 8, 12, 0, -1, -2, -3, -4, -6, -8, -12};\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 36u; const device uchar *scales = weight + off; const device uchar *qs = weight + off + 4u; uint input_off = r * p.in_dim + b * 64u; for (uint sub = 0; sub < 4u; ++sub) { float d = termite_nvfp4_scale(scales[sub]); uint q_base = sub * 8u; uint x_base = input_off + sub * 16u; for (uint j = 0; j < 8u; ++j) { uchar packed = qs[q_base + j]; acc += input[x_base + j] * (d * float(lut[packed & 0x0Fu])); acc += input[x_base + 8u + j] * (d * float(lut[packed >> 4])); } } }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\
;
