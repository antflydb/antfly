// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_attention_f32_block(ANTFLY_CUDA_DENSE_ATTENTION_PARAMS) {
    \\    constexpr bool Local = false;
    \\    const unsigned int radius = 0u;
    \\#include "../kernels/block_local_attention.cuh"
    \\}
    \\
;
