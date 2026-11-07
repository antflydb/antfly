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
    \\extern "C" __global__ void termite_rope_f32(ANTFLY_CUDA_ROPE_PARAMS) {
    \\    termite_rope_impl<false, false>(dst, input, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, 1.0f, nullptr);
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_qwen3vl_mrope_f32(ANTFLY_CUDA_QWEN3VL_MROPE_F32_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = total_chunks * head_dim;
    \\    if (idx >= total) return;
    \\
    \\    unsigned int chunk = idx / head_dim;
    \\    unsigned int d = idx - chunk * head_dim;
    \\    unsigned int rotary_pairs = head_dim / 2u;
    \\    unsigned int pair = d < rotary_pairs ? d : d - rotary_pairs;
    \\    unsigned int axis = 0u;
    \\    if ((pair % 3u) == 1u && pair < section_height * 3u) {
    \\        axis = 1u;
    \\    } else if ((pair % 3u) == 2u && pair < section_width * 3u) {
    \\        axis = 2u;
    \\    }
    \\    unsigned int token = chunk / chunks_per_token;
    \\    unsigned int position = positions[axis * token_count + token];
    \\    float exponent = (2.0f * (float)pair) / (float)head_dim;
    \\    float frequency = 1.0f / powf(theta, exponent);
    \\    float angle = (float)position * freq_scale * frequency;
    \\    dst[idx] = termite_qwen3vl_rotate(input, chunk * head_dim, pair, rotary_pairs, d, angle);
    \\}
    \\
;

pub const source_2 =
    \\extern "C" __global__ void termite_qwen3vl_vision_rope_f32(ANTFLY_CUDA_QWEN3VL_VISION_ROPE_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = total_chunks * head_dim;
    \\    if (idx >= total) return;
    \\
    \\    unsigned int chunk = idx / head_dim;
    \\    unsigned int d = idx - chunk * head_dim;
    \\    unsigned int rotary_pairs = head_dim / 2u;
    \\    unsigned int pairs_per_axis = rotary_pairs / 2u;
    \\    unsigned int pair = d < rotary_pairs ? d : d - rotary_pairs;
    \\    unsigned int axis = pair < pairs_per_axis ? 0u : 1u;
    \\    unsigned int local_pair = pair % pairs_per_axis;
    \\    unsigned int token = chunk / heads_per_token;
    \\    unsigned int position = positions[axis * token_count + token];
    \\    float exponent = (2.0f * (float)local_pair) / (float)(head_dim / 2u);
    \\    float frequency = 1.0f / powf(theta, exponent);
    \\    float angle = (float)position * frequency;
    \\    dst[idx] = termite_qwen3vl_rotate(input, chunk * head_dim, pair, rotary_pairs, d, angle);
    \\}
    \\
    \\extern "C" __global__ void termite_rope_decode_scalars_f32(ANTFLY_CUDA_ROPE_DECODE_SCALARS_PARAMS) {
    \\    termite_rope_impl<false, true>(dst, input, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, 1.0f, decode_scalars);
    \\}
    \\
;

pub const source_3 =
    \\extern "C" __global__ void termite_rope_scaled_f32(ANTFLY_CUDA_ROPE_SCALED_PARAMS) {
    \\    termite_rope_impl<true, false>(dst, input, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, scale, nullptr);
    \\}
    \\
    \\extern "C" __global__ void termite_rope_scaled_decode_scalars_f32(ANTFLY_CUDA_ROPE_SCALED_DECODE_SCALARS_PARAMS) {
    \\    termite_rope_impl<true, true>(dst, input, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, scale, decode_scalars);
    \\}
    \\
    \\extern "C" __global__ void termite_rope_per_item_f32(ANTFLY_CUDA_ROPE_PER_ITEM_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total_chunks = batch * max_seq_len * num_heads;
    \\    unsigned int total = total_chunks * head_dim;
    \\    if (idx >= total) return;
    \\    unsigned int chunk = idx / head_dim;
    \\    unsigned int d = idx - chunk * head_dim;
    \\    unsigned int token = chunk / num_heads;
    \\    unsigned int item = token / max_seq_len;
    \\    unsigned int pos = token - item * max_seq_len;
    \\    unsigned int position = 0u;
    \\    if (item < batch && pos < query_lengths[item]) {
    \\        position = position_offsets[item] + pos;
    \\    }
    \\    unsigned int base = chunk * head_dim;
    \\    dst[idx] = termite_rope_value(input, base, d, head_dim, rope_dim, position, theta, freq_scale, consecutive_pairs);
    \\}
    \\
;

pub const source_4 =
    \\extern "C" __global__ void termite_rms_norm_heads_rope_f32(ANTFLY_CUDA_RMS_NORM_HEADS_ROPE_PARAMS) {
    \\    termite_rms_norm_heads_rope_impl<false>(dst, input, weight, total_chunks, head_dim, rope_dim, eps, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, output_scale, nullptr);
    \\}
    \\
    \\extern "C" __global__ void termite_rms_norm_heads_rope_decode_scalars_f32(ANTFLY_CUDA_RMS_NORM_HEADS_ROPE_DECODE_SCALARS_PARAMS) {
    \\    termite_rms_norm_heads_rope_impl<true>(dst, input, weight, total_chunks, head_dim, rope_dim, eps, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, output_scale, decode_scalars);
    \\}
    \\
;
