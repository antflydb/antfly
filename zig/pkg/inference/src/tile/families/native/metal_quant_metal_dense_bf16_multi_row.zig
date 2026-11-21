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
    program.join(&.{
        \\           "kernel void termite_apply_linear_bf16_multi_row_tiled32_m16(device const float *input [[buffer(0)]], device const ushort *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], ushort2 tid [[thread_position_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {\n"
        \\           "    threadgroup float a_tile[16][32]; threadgroup float b_tile[32][32];\n"
        \\           "    termite_dense_tile<16u, 32u, false>(input, weight, bias, output, p, 0u, &a_tile[0][0], &b_tile[0][0], tid, tg);\n"
        \\           "}\n"
        \\
        ,
        program.metal(program.render(metal_phases.dense_rows, .{ .scalar_name = "termite_apply_linear_bf16_multi_row", .reduce_name = "termite_apply_linear_bf16_multi_row_reduce", .weight_type = "ushort" })),
    });
