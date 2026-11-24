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
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_iq2_xs_linear) "\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 74u; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 74u; ushort d_bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); float d = float(as_type<half>(d_bits)); const device uchar *qs = weight + off + 2u; const device uchar *scales = weight + off + 66u; uint input_off = r * p.in_dim + b * 256u; for (uint ib32 = 0u; ib32 < 8u; ++ib32) { float db0 = d * (0.5f + float(scales[ib32] & 0x0Fu)) * 0.25f; float db1 = d * (0.5f + float(scales[ib32] >> 4)) * 0.25f; for (uint lane = 0u; lane < 4u; ++lane) { uint qoff = (4u * ib32 + lane) * 2u; ushort packed = ushort(qs[qoff]) | (ushort(qs[qoff + 1u]) << 8); ushort grid = termite_iq2_xs_grid[packed & 0x01FFu]; uchar signs = termite_iq2_sign_mask(uint(packed >> 9)); float dl = lane < 2u ? db0 : db1; uint base = input_off + ib32 * 32u + lane * 8u; for (uint j = 0u; j < 8u; ++j) { float raw = float(2u * ((uint(grid) >> (2u * j)) & 0x03u) + 1u); float sign = ((uint(signs) >> j) & 1u) != 0u ? -1.0f : 1.0f; acc += input[base + j] * (dl * raw * sign); } } } }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\
;
