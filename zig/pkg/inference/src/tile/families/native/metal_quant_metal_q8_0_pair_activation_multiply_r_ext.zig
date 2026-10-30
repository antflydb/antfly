// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_SIMD({{name}}) " termite_q8_0_ext_projection<{{literal_0}}, true, true>(input, weight_gate, weight_up, output, nullptr, p, ap.activation_kind, lane, sgitg, tg); }\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_q8_0_pair_activation_multiply_r2_ext", .literal_0 = "2u" }), program.render(program_0, .{ .name = "termite_q8_0_pair_activation_multiply_r3_ext", .literal_0 = "3u" }), program.render(program_0, .{ .name = "termite_q8_0_pair_activation_multiply_r4_ext", .literal_0 = "4u" }), program.render(program_0, .{ .name = "termite_q8_0_pair_activation_multiply_r5_ext", .literal_0 = "5u" }) });
