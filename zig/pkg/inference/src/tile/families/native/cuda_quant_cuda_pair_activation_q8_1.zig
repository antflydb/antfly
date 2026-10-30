// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4(ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS) {
    \\    termite_q4_0_pair_activation_q8_1_tile4_impl<4u>(dst, q8_input, weight_gate, weight_up, rows, in_dim, out_dim, activation);
    \\}
    \\
    \\extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8(ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS) {
    \\    termite_q4_0_pair_activation_q8_1_tile4_impl<8u>(dst, q8_input, weight_gate, weight_up, rows, in_dim, out_dim, activation);
    \\}
    \\
;
