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
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q4_0_linear_r2_ext4) " termite_q4_0_linear_rows_impl<2u, 4u>(input, weight, output, p, lane, sgitg, tg); }\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q4_0_linear_r4_ext4) " termite_q4_0_linear_rows_impl<4u, 4u>(input, weight, output, p, lane, sgitg, tg); }\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q4_0_linear_r8_ext4) " termite_q4_0_linear_rows_impl<5u, 4u>(input, weight, output, p, lane, sgitg, tg); }\n"
    \\
;
