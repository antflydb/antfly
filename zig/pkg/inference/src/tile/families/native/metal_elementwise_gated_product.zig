// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
