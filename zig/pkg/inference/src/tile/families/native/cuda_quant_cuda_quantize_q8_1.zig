// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_quantize_f32_q8_1_rows(
    \\    unsigned char* dst,
    \\    const float* input,
    \\    unsigned int rows,
    \\    unsigned int in_dim
    \\) {
    \\    termite_quantize_q8_1_rows_impl<false>(dst, input, nullptr, rows, in_dim, 0u);
    \\}
    \\
    \\extern "C" __global__ void termite_quantize_gated_f32_q8_1_rows(
    \\    unsigned char* dst,
    \\    const float* gate,
    \\    const float* up,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int activation
    \\) {
    \\    termite_quantize_q8_1_rows_impl<true>(dst, gate, up, rows, in_dim, activation);
    \\}
    \\
;
