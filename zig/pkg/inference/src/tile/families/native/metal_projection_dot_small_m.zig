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

pub const source_0 =
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_tile) "\n"
    \\           "    antfly_dot_rows_cols<1u, 8u>(lhs, rhs, output, p, partials, tid, tpg, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_rows4_cols4) "\n"
    \\           "    antfly_dot_rows_cols<4u, 4u>(lhs, rhs, output, p, partials, tid, tpg, tg);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_rows4_cols8) "\n"
    \\           "    antfly_dot_rows_cols<4u, 8u>(lhs, rhs, output, p, partials, tid, tpg, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_rows4_cols16) "\n"
    \\           "    antfly_dot_rows_cols<4u, 16u>(lhs, rhs, output, p, partials, tid, tpg, tg);\n"
    \\           "}\n"
    \\
;
