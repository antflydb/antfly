// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           TERMITE_MSL_ABI_QUANT_REDUCE({{name}}) " termite_q6_k_linear_1x_reduce_nbodd_impl(input, weight, output, p, shmem, lane, sgitg, tg, {{literal_0}}, {{literal_1}}); }\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_q6_k_linear_1x_reduce_nbodd", .literal_0 = "2u", .literal_1 = "2u" }), program.render(program_0, .{ .name = "termite_q6_k_linear_1x_reduce_nbodd_nr1", .literal_0 = "1u", .literal_1 = "2u" }), program.render(program_0, .{ .name = "termite_q6_k_linear_1x_reduce_nbodd_nr4_nsg1", .literal_0 = "4u", .literal_1 = "1u" }), program.render(program_0, .{ .name = "termite_q6_k_linear_1x_reduce_nbodd_nr4_nsg2", .literal_0 = "4u", .literal_1 = "2u" }), program.render(program_0, .{ .name = "termite_q6_k_linear_1x_reduce_nbodd_nr6_nsg1", .literal_0 = "6u", .literal_1 = "1u" }), program.render(program_0, .{ .name = "termite_q6_k_linear_1x_reduce_nbodd_nr8_nsg1", .literal_0 = "8u", .literal_1 = "1u" }) });
