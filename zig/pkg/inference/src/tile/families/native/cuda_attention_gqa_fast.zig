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
    \\extern "C" __global__ void termite_gqa_attention_decode_scalars_fast_f32(
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
    \\    if (decode_scalars != 0) {
    \\        kv_position_offset = decode_scalars[4];
    \\        query_position_offset = decode_scalars[1];
    \\        kv_seq_len = decode_scalars[2];
    \\        total_sequence_len = decode_scalars[3];
    \\    }
    \\
    \\    __shared__ float warp_sums[16];
    \\    __shared__ float shared_max_score;
    \\    __shared__ float shared_denom;
    \\    __shared__ float shared_alpha;
    \\    __shared__ float shared_beta;
    \\    unsigned int lane = threadIdx.x;
    \\    unsigned int head = blockIdx.x;
    \\    if (batch != 1u ||
    \\        q_seq_len != 1u ||
    \\        mask_len != 0u ||
    \\        bias_mode != 0u ||
    \\        head >= num_heads ||
    \\        head_dim > 512u ||
    \\        (head_dim & 31u) != 0u ||
    \\        blockDim.x < head_dim ||
    \\        num_kv_heads == 0u ||
    \\        (num_heads % num_kv_heads) != 0u) return;
    \\
    \\    const unsigned int b = 0u, qi = 0u;
    \\    unsigned int query_pos = query_position_offset;
    \\#include "../kernels/dense_fast_attention_online.cuh"
    \\}
    \\
    \\extern "C" __global__ void termite_gqa_attention_prefill_fast_f32(
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
    \\    (void)attn_or_mask;
    \\    (void)bias;
    \\    (void)total_sequence_len;
    \\
    \\    __shared__ float warp_sums[16];
    \\    __shared__ float shared_max_score;
    \\    __shared__ float shared_denom;
    \\    __shared__ float shared_alpha;
    \\    __shared__ float shared_beta;
    \\    unsigned int lane = threadIdx.x;
    \\    unsigned int block = blockIdx.x;
    \\    unsigned int total_blocks = batch * q_seq_len * num_heads;
    \\    if (batch == 0u ||
    \\        q_seq_len <= 1u ||
    \\        block >= total_blocks ||
    \\        mask_len != 0u ||
    \\        bias_mode != 0u ||
    \\        head_dim > 512u ||
    \\        (head_dim & 31u) != 0u ||
    \\        blockDim.x < head_dim ||
    \\        num_kv_heads == 0u ||
    \\        (num_heads % num_kv_heads) != 0u) return;
    \\
    \\    unsigned int head = block % num_heads;
    \\    unsigned int tmp = block / num_heads;
    \\    unsigned int qi = tmp % q_seq_len;
    \\    unsigned int b = tmp / q_seq_len;
    \\    unsigned int query_pos = query_position_offset + qi;
    \\
    \\#include "../kernels/dense_fast_attention_online.cuh"
    \\}
    \\
;
