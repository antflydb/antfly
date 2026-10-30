// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_layer_norm_f32(ANTFLY_CUDA_LAYER_NORM_PARAMS) {
    \\    termite_layer_norm_impl<false>(dst, input, nullptr, gamma, beta, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_add_layer_norm_f32(ANTFLY_CUDA_ADD_LAYER_NORM_PARAMS) {
    \\    termite_layer_norm_impl<true>(dst, a, b, gamma, beta, rows, dim, eps);
    \\}
    \\
;
