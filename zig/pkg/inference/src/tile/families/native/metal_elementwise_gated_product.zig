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
    \\        "kernel void {{name}}(device float*x[[buffer(0)]],device bfloat*gw[[buffer(1)]],device bfloat*uw[[buffer(2)]],device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device float*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 group[[threadgroup_position_in_grid]]){antfly_mpp_gate_up_f32<{{literal_7}}>(x,gw,uw,gb,ub,y,p,group);}\n"
    \\
;

pub const source_0 = program.join(&.{
    program.render(program_0, .{ .name = "termite_lightonocr3_gate_up_small16", .literal_7 = "16" }),
    program.render(program_0, .{ .name = "termite_lightonocr3_gate_up_small32", .literal_7 = "32" }),

    \\        "kernel void termite_lightonocr3_gate_up_wide16(device float*x[[buffer(0)]],device bfloat*gw[[buffer(1)]],device bfloat*uw[[buffer(2)]],device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device float*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 group[[threadgroup_position_in_grid]]){antfly_mpp_gate_up_f32<16,16>(x,gw,uw,gb,ub,y,p,group);}\n"
    \\
    ,

    \\        "kernel void termite_lightonocr3_gate_up_wide32(device float*x[[buffer(0)]],device bfloat*gw[[buffer(1)]],device bfloat*uw[[buffer(2)]],device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device float*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 group[[threadgroup_position_in_grid]]){antfly_mpp_gate_up_f32<16,32,2>(x,gw,uw,gb,ub,y,p,group);}\n"
    \\
    ,

    \\        "kernel void termite_lightonocr3_gate_up_wide64(device float*x[[buffer(0)]],device bfloat*gw[[buffer(1)]],device bfloat*uw[[buffer(2)]],device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device float*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 group[[threadgroup_position_in_grid]]){antfly_mpp_gate_up_f32<32,64,4>(x,gw,uw,gb,ub,y,p,group);}\n"
    \\
});
