// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_MATRIX(termite_q4_0_pair_activation_mm_m32_n32_aligned) " termite_q4_0_pair_activation_mm_m32_n32_impl(input, weight_gate, weight_up, output, p, ap, shmem, tiitg, sgitg, tg, false); }\n"
    \\           TERMITE_MSL_ABI_QUANT_PAIR_MATRIX(termite_q4_0_pair_activation_mm_m32_n32_tail) " termite_q4_0_pair_activation_mm_m32_n32_impl(input, weight_gate, weight_up, output, p, ap, shmem, tiitg, sgitg, tg, true); }\n"
    \\
;
