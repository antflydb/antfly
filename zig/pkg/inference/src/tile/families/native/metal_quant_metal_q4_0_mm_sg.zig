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
    \\           TERMITE_MSL_ABI_QUANT_MATRIX({{name}}) "\n"
    \\           "    termite_q4_0_matrix_aligned_impl<{{literal_0}}>(input, weight, output, p, shmem, tiitg, sgitg, tg);\n"
    \\           "}\n"
    \\
;

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_MATRIX(termite_q4_0_linear_mm_sg) "\n"
    \\           "    termite_quant_linear_mm_sg<termite_block_q4_0, 18u>(input, weight, output, p, shmem, tiitg, sgitg, tg);\n"
    \\           "}\n"
    \\
;

pub const source_1 = program.join(&.{ program.render(program_0, .{ .name = "termite_q4_0_linear_mm_sg_unrolled", .literal_0 = "2u" }), program.render(program_0, .{ .name = "termite_q4_0_linear_mm_sg_aligned", .literal_0 = "0u" }), program.render(program_0, .{ .name = "termite_q4_0_linear_mm_sg_aligned_tail", .literal_0 = "1u" }) });
