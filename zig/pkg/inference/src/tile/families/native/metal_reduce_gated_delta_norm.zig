// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\        ANTFLY_MSL_GDN_NORM(termite_qwen35_norm, termite_qwen35_params, 1, p.seq*p.value_heads, "", "0")
    \\
;

pub const source_1 =
    \\        ANTFLY_MSL_GDN_NORM(termite_qwen35_norm_parallel, termite_qwen35_params, 32, p.value_heads, ", ushort lane [[thread_index_in_simdgroup]]", "uint(lane)")
    \\
;
