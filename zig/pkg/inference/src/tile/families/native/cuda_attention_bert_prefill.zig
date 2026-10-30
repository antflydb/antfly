// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64_q8(ANTFLY_CUDA_DENSE_ATTENTION_PARAMS) {
    \\    termite_bert_prefill_hd64_impl<8, unsigned int>(dst, q, k, v, mask, bias, batch, seq_len, num_heads, head_dim, causal, has_mask, bias_mode, head_major);
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64_q16(ANTFLY_CUDA_DENSE_ATTENTION_PARAMS) {
    \\    termite_bert_prefill_hd64_impl<16, unsigned long long>(dst, q, k, v, mask, bias, batch, seq_len, num_heads, head_dim, causal, has_mask, bias_mode, head_major);
    \\}
    \\
;
