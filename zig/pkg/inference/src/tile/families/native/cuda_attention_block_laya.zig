// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_laya_local_attention_f32(
    \\    float* dst,
    \\    const float* q,
    \\    const float* k,
    \\    const float* v,
    \\    const long long* mask,
    \\    unsigned int batch,
    \\    unsigned int seq_len,
    \\    unsigned int num_heads,
    \\    unsigned int head_dim,
    \\    unsigned int radius
    \\) {
    \\    constexpr bool Local = true;
    \\    const unsigned int head_major = 0u, causal = 0u, has_mask = 1u, bias_mode = 0u;
    \\    const float* bias = nullptr;
    \\#include "../kernels/block_local_attention.cuh"
    \\}
    \\
;
