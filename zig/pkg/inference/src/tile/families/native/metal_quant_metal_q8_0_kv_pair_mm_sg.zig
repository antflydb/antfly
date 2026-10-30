// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_Q8_KV_PAIR_MATRIX(termite_q8_0_kv_pair_mm_sg) " termite_q8_0_kv_pair_mm_sg_impl(input, weight_k, weight_v, output_k, output_v, p, shmem, tiitg, sgitg, tg, false); }\n"
    \\           TERMITE_MSL_ABI_Q8_KV_PAIR_MATRIX(termite_q8_0_kv_pair_mm_sg_tail) " termite_q8_0_kv_pair_mm_sg_impl(input, weight_k, weight_v, output_k, output_v, p, shmem, tiitg, sgitg, tg, true); }\n"
    \\
;
