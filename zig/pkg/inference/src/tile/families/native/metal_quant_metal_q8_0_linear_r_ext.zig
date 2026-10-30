// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD({{name}}) " termite_q8_0_ext_projection<{{literal_0}}, false, false>(input, weight, nullptr, output, nullptr, p, 0u, lane, sgitg, tg); }\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_q8_0_linear_r2_ext", .literal_0 = "2u" }), program.render(program_0, .{ .name = "termite_q8_0_linear_r3_ext", .literal_0 = "3u" }), program.render(program_0, .{ .name = "termite_q8_0_linear_r4_ext", .literal_0 = "4u" }), program.render(program_0, .{ .name = "termite_q8_0_linear_r5_ext", .literal_0 = "5u" }) });
