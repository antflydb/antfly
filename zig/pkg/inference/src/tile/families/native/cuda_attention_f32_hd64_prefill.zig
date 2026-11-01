// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
