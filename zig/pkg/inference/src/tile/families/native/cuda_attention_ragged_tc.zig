// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ __launch_bounds__(256) void termite_gliner_boundary_attention_tc_f16_m64(ANTFLY_CUDA_GLINER_BOUNDARY_PARAMS) {
    \\    constexpr bool Compact = false;
    \\#include "../kernels/relative_attention_tensor_rows64.cuh"
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ __launch_bounds__(256) void termite_gliner_boundary_attention_tc_f16_compact(ANTFLY_CUDA_GLINER_BOUNDARY_PARAMS) {
    \\    constexpr bool Compact = true;
    \\#include "../kernels/relative_attention_tensor_rows64.cuh"
    \\}
    \\
;
