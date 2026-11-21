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

const program_0 =
    \\           "kernel void {{name}}(device const float *x [[buffer(0)]], device const ushort *w [[buffer(1)]], device const float *b [[buffer(2)]], device float *y [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], uint lane [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           " termite_bf16_affine_vec4<{{literal_5}}>(x,w,b,y,p,lane,row);\n"
    \\           "}\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_lightonocr3_batch2_vec4", .literal_5 = "2" }), program.render(program_0, .{ .name = "termite_lightonocr3_batch4_vec4", .literal_5 = "4" }), program.render(program_0, .{ .name = "termite_lightonocr3_batch8_vec4", .literal_5 = "8" }), program.render(program_0, .{ .name = "termite_lightonocr3_batch16_vec4", .literal_5 = "16" }) });
