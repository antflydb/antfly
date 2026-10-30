// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_linear_f32_qkv_nobias_tiled(ANTFLY_CUDA_LINEAR_F32_QKV_NOBIAS_TILED_PARAMS) {
    \\    typedef float Weight;
    \\#include "../kernels/dense_qkv_tiled.cuh"
    \\}
    \\
    \\extern "C" __global__ void termite_linear_bf16_weight_f32_qkv_nobias_tiled(ANTFLY_CUDA_LINEAR_BF16_WEIGHT_F32_QKV_NOBIAS_TILED_PARAMS) {
    \\    typedef unsigned short Weight;
    \\#include "../kernels/dense_qkv_tiled.cuh"
    \\}
    \\
;
