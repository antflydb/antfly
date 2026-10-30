// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           "kernel void {{name}}(device const float *x [[buffer(0)]], device const ushort *w [[buffer(1)]], device const float *b [[buffer(2)]], device float *y [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], uint lane [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           " termite_bf16_affine_vec4<{{literal_5}}>(x,w,b,y,p,lane,row);\n"
    \\           "}\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_lightonocr3_batch2_vec4", .literal_5 = "2" }), program.render(program_0, .{ .name = "termite_lightonocr3_batch4_vec4", .literal_5 = "4" }), program.render(program_0, .{ .name = "termite_lightonocr3_batch8_vec4", .literal_5 = "8" }), program.render(program_0, .{ .name = "termite_lightonocr3_batch16_vec4", .literal_5 = "16" }) });
