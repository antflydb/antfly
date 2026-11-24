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
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q8_0_linear) "\n"
    \\           "    weight += p.weight_offset;\n"
    \\           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 34u; float acc = 0.0f;\n"
    \\           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 34u; float d = termite_q8_0_block_scale(weight, off); float block_acc = 0.0f; uint in_off = r * p.in_dim + b * 32u; for (uint i = 0; i < 32; ++i) { block_acc += input[in_off + i] * termite_q8_0_block_value(weight, off, i); } acc += d * block_acc; }\n"
    \\           "    output[gid] = acc;\n"
    \\           "}\n"
    \\
;
