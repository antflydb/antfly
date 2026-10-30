// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q4_0_linear_r2_ext4) " termite_q4_0_linear_rows_impl<2u, 4u>(input, weight, output, p, lane, sgitg, tg); }\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q4_0_linear_r4_ext4) " termite_q4_0_linear_rows_impl<4u, 4u>(input, weight, output, p, lane, sgitg, tg); }\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q4_0_linear_r8_ext4) " termite_q4_0_linear_rows_impl<5u, 4u>(input, weight, output, p, lane, sgitg, tg); }\n"
    \\
;
