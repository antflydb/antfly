// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_MATRIX(termite_q8_0_linear_mm_sg_v2) " termite_q8_0_linear_mm_sg_typed_impl<false, false>(input, weight, output, p, shmem, tiitg, sgitg, tg); }\n"
    \\           TERMITE_MSL_ABI_QUANT_MATRIX(termite_q8_0_linear_mm_sg_v2_tail) " termite_q8_0_linear_mm_sg_typed_impl<false, true>(input, weight, output, p, shmem, tiitg, sgitg, tg); }\n"
    \\           TERMITE_MSL_ABI_QUANT_F16_MATRIX(termite_q8_0_linear_mm_sg_v2_in_f16) " termite_q8_0_linear_mm_sg_typed_impl<false, false>(input, weight, output, p, shmem, tiitg, sgitg, tg); }\n"
    \\           TERMITE_MSL_ABI_QUANT_F16_MATRIX(termite_q8_0_linear_mm_sg_v2_in_f16_tail) " termite_q8_0_linear_mm_sg_typed_impl<false, true>(input, weight, output, p, shmem, tiitg, sgitg, tg); }\n"
    \\
;
