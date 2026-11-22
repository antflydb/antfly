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
    \\           "kernel void {{name}}(device const float *q [[buffer(0)]], device const float *k [[buffer(1)]], device const float *v [[buffer(2)]], device const float *bias [[buffer(3)]], device const float *mask [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_sdpa_f32_params &p [[buffer(6)]], threadgroup float *scores [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], uint tg [[threadgroup_position_in_grid]]) { antfly_window_hd32<{{literal_8}}>(q,k,v,bias,mask,output,p,scores,lane,tg); }\n"
    \\
;

pub const source_0 = program.join(&.{
    program.render(program_0, .{ .name = "termite_sdpa_f32_florence_window_hd32", .literal_8 = "false" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_sdpa_f32_florence_window_parallel", .literal_8 = "true" }),
});
