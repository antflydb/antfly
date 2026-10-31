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
const metal_phases = @import("metal_phases.zig");

pub const source_0 =
    \\           TERMITE_MSL_ABI_DENSE_TILED(termite_apply_linear_multi_row_tiled32_m16_f32) "\n"
    \\           "    threadgroup float a_tile[16][32]; threadgroup float b_tile[32][32];\n"
    \\           "    termite_dense_tile<16u, 32u, false>(input, weight, bias, output, p, 0u, &a_tile[0][0], &b_tile[0][0], tid, tg);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    program.metal(program.render(metal_phases.dense_rows, .{ .scalar_name = "termite_apply_linear_f16_multi_row", .reduce_name = "termite_apply_linear_f16_multi_row_reduce", .weight_type = "half" }));
