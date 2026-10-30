// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_embedding_gemma2_pack_bshd_bf16_f32(
    \\    float* out, const __nv_bfloat16* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<float,__nv_bfloat16,false>(out, input, batch, sequence, heads, dim);
    \\}
    \\
    \\extern "C" __global__ void termite_embedding_gemma2_pack_bshd_f32(
    \\    float* out, const float* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<float,float,false>(out, input, batch, sequence, heads, dim);
    \\}
    \\
    \\extern "C" __global__ void termite_embedding_gemma2_unpack_bhsd_f32(
    \\    float* out, const float* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<float,float,true>(out, input, batch, sequence, heads, dim);
    \\}
    \\
    \\extern "C" __global__ void termite_embedding_gemma2_unpack_bhsd_f32_bf16(
    \\    __nv_bfloat16* out, const float* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<__nv_bfloat16,float,true>(out, input, batch, sequence, heads, dim);
    \\}
    \\
;
