// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           "kernel void {{name}}(device const float4 *x [[buffer(0)]], device const float4 *w [[buffer(1)]], device float4 *y [[buffer(2)]], constant termite_metal_apply_row_norm_params &p [[buffer(3)]], threadgroup float *partial [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint sg [[simdgroup_index_in_threadgroup]], uint lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]) { termite_lightonocr3_norm_cached_body<{{literal_5}}>(x,w,y,p,partial,tid,sg,lane,row); }\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_lightonocr3_norm_cached1024", .literal_5 = "1024" }), program.render(program_0, .{ .name = "termite_lightonocr3_norm_cached2560", .literal_5 = "2560" }) });
