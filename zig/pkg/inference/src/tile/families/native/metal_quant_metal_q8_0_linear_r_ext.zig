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
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD({{name}}) " termite_q8_0_ext_projection<{{literal_0}}, false, false>(input, weight, nullptr, output, nullptr, p, 0u, lane, sgitg, tg); }\n"
    \\
;

pub const source_0 = program.join(&.{ program.render(program_0, .{ .name = "termite_q8_0_linear_r2_ext", .literal_0 = "2u" }), program.render(program_0, .{ .name = "termite_q8_0_linear_r3_ext", .literal_0 = "3u" }), program.render(program_0, .{ .name = "termite_q8_0_linear_r4_ext", .literal_0 = "4u" }), program.render(program_0, .{ .name = "termite_q8_0_linear_r5_ext", .literal_0 = "5u" }) });
