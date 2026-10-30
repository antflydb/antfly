// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_laya_attention_warp_f32(
    \\    float* dst, const float* q, const float* k, const float* v,
    \\    const long long* mask, unsigned int batch, unsigned int seq,
    \\    unsigned int heads, unsigned int dim, unsigned int radius
    \\) {
    \\    __shared__ float scores[512];
    \\    termite_small_warp_attention_impl<512u, false>(dst, q, k, v, mask, batch, seq, heads, dim, radius, scores);
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_gliner_encoder_attention_f32(
    \\    float* dst, const float* q, const float* k, const float* v,
    \\    const long long* mask, unsigned int batch, unsigned int seq,
    \\    unsigned int heads, unsigned int dim, unsigned int radius
    \\) {
    \\    extern __shared__ float scores[];
    \\    termite_small_warp_attention_impl<8192u, true>(dst, q, k, v, mask, batch, seq, heads, dim, radius, scores);
    \\}
    \\
;
