// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_gqa_attention_decode_f32(
    \\    float* dst,
    \\    const float* q,
    \\    const float* k,
    \\    const float* v,
    \\    const unsigned char* attn_or_mask,
    \\    const float* bias,
    \\    unsigned int batch,
    \\    unsigned int q_seq_len,
    \\    unsigned int kv_seq_len,
    \\    unsigned int num_heads,
    \\    unsigned int num_kv_heads,
    \\    unsigned int head_dim,
    \\    unsigned int query_position_offset,
    \\    unsigned int kv_position_offset,
    \\    unsigned int sliding_window,
    \\    unsigned int total_sequence_len,
    \\    unsigned int mask_len,
    \\    unsigned int bias_mode
    \\) {
    \\    termite_gqa_scalar_decode_impl<false>(dst, q, k, v, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, nullptr);
    \\}
    \\
    \\extern "C" __global__ void termite_gqa_attention_decode_scalars_f32(
    \\    float* dst,
    \\    const float* q,
    \\    const float* k,
    \\    const float* v,
    \\    const unsigned char* attn_or_mask,
    \\    const float* bias,
    \\    unsigned int batch,
    \\    unsigned int q_seq_len,
    \\    unsigned int kv_seq_len,
    \\    unsigned int num_heads,
    \\    unsigned int num_kv_heads,
    \\    unsigned int head_dim,
    \\    unsigned int query_position_offset,
    \\    unsigned int kv_position_offset,
    \\    unsigned int sliding_window,
    \\    unsigned int total_sequence_len,
    \\    unsigned int mask_len,
    \\    unsigned int bias_mode,
    \\    const unsigned int* decode_scalars
    \\) {
    \\    termite_gqa_scalar_decode_impl<true>(dst, q, k, v, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, decode_scalars);
    \\}
    \\
;
