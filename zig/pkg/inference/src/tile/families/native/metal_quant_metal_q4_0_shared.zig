// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_CACHE({{name}}) " termite_q4_0_shared_mmv<{{literal_0}}, {{literal_1}}, {{literal_2}}, false>(input, weight, weight, output, p, 0u, cache, lane, sgitg, tg); }\n"
    \\
;

const program_1 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_CACHE({{name}}) " termite_q4_0_shared_mmv<{{literal_0}}, {{literal_1}}, {{literal_2}}, true>(input, weight, up, output, p, ap.activation_kind, cache, lane, sgitg, tg); }\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr4_nsg2_r2", .literal_0 = "2u", .literal_1 = "4u", .literal_2 = "2u" }), program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr4_nsg2_r4", .literal_0 = "4u", .literal_1 = "4u", .literal_2 = "2u" }), program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr8_nsg2_r2", .literal_0 = "2u", .literal_1 = "8u", .literal_2 = "2u" }), program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr8_nsg2_r4", .literal_0 = "4u", .literal_1 = "8u", .literal_2 = "2u" }), program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr4_nsg4_r2", .literal_0 = "2u", .literal_1 = "4u", .literal_2 = "4u" }), program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr4_nsg4_r4", .literal_0 = "4u", .literal_1 = "4u", .literal_2 = "4u" }), program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr8_nsg4_r2", .literal_0 = "2u", .literal_1 = "8u", .literal_2 = "4u" }), program.render(program_0, .{ .name = "termite_q4_0_shared_linear_nr8_nsg4_r4", .literal_0 = "4u", .literal_1 = "8u", .literal_2 = "4u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr4_nsg2_r2", .literal_0 = "2u", .literal_1 = "4u", .literal_2 = "2u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr4_nsg2_r4", .literal_0 = "4u", .literal_1 = "4u", .literal_2 = "2u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr8_nsg2_r2", .literal_0 = "2u", .literal_1 = "8u", .literal_2 = "2u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr8_nsg2_r4", .literal_0 = "4u", .literal_1 = "8u", .literal_2 = "2u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr4_nsg4_r2", .literal_0 = "2u", .literal_1 = "4u", .literal_2 = "4u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr4_nsg4_r4", .literal_0 = "4u", .literal_1 = "4u", .literal_2 = "4u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr8_nsg4_r2", .literal_0 = "2u", .literal_1 = "8u", .literal_2 = "4u" }), program.render(program_1, .{ .name = "termite_q4_0_shared_pair_nr8_nsg4_r4", .literal_0 = "4u", .literal_1 = "8u", .literal_2 = "4u" }) });
