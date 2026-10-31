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

// native-program:entry termite_decode_scalars_advance
extern "C" __global__ void termite_decode_scalars_advance(
    unsigned int* decode_scalars,
    unsigned int position_delta,
    unsigned int query_position_delta,
    unsigned int kv_seq_delta,
    unsigned int total_sequence_delta,
    unsigned int kv_position_delta
) {
    if (blockIdx.x != 0 || threadIdx.x != 0 || decode_scalars == 0) return;
    decode_scalars[0] += position_delta;
    decode_scalars[1] += query_position_delta;
    decode_scalars[2] += kv_seq_delta;
    decode_scalars[3] += total_sequence_delta;
    decode_scalars[4] += kv_position_delta;
}
// native-program:entry termite_kv_write_suffix_decode_scalars_f32
extern "C" __global__ void termite_kv_write_suffix_decode_scalars_f32(ANTFLY_CUDA_KV_SUFFIX_DENSE_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = suffix_token_count * row_width;
    if (idx >= total || suffix_token_count == 0u || row_width == 0u) return;
    unsigned int token_start;
    if (!termite_kv_suffix_start(decode_scalars, suffix_token_count, fallback_total_token_count, token_start)) return;
    unsigned int dst_idx = token_start * row_width + idx;
    k_dst[dst_idx] = k_src[idx];
    v_dst[dst_idx] = v_src[idx];
}
// native-program:entry termite_kv_write_suffix_turboquant_f32
extern "C" __global__ void termite_kv_write_suffix_turboquant_f32(ANTFLY_CUDA_KV_SUFFIX_TQ_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (suffix_token_count == 0u || row_width == 0u || key_row_bytes == 0u || base_key_row_bytes == 0u || value_row_bytes == 0u) return;
    unsigned int token_start;
    if (!termite_kv_suffix_start(decode_scalars, suffix_token_count, fallback_total_token_count, token_start)) return;

    unsigned int key_total = suffix_token_count * key_row_bytes;
    if (idx < key_total) {
        unsigned int token = idx / key_row_bytes;
        unsigned int byte = idx - token * key_row_bytes;
        const float* src_row = k_src + token * row_width;
        unsigned int physical_token = termite_tq_physical_token(token_start + token, block_table, block_count, page_size_tokens, physical_token_capacity);
        if (physical_token == 0xffffffffu) return;
        unsigned char* dst_row = k_dst + physical_token * key_row_bytes;
        if (format == 0u) {
            unsigned int value_index = byte * 2u;
            unsigned char lo = value_index < row_width ? termite_tq_encode_polar4_scalar(src_row[value_index]) : 0u;
            unsigned char hi = (value_index + 1u) < row_width ? termite_tq_encode_polar4_scalar(src_row[value_index + 1u]) : 0u;
            dst_row[byte] = lo | (unsigned char)(hi << 4);
        } else if (format == 1u) {
            if (byte < base_key_row_bytes) {
                dst_row[byte] = termite_tq_packed3_byte(src_row, row_width, byte);
            } else {
                dst_row[byte] = termite_tq_turbo3_residual_byte(src_row, num_kv_heads, head_dim, byte - base_key_row_bytes);
            }
        } else if (format == 2u) {
            unsigned int value_index = byte >> 1;
            if (value_index < row_width) termite_tq_store_f16_byte(dst_row + value_index * 2u, src_row[value_index], byte & 1u);
        }
    }

    unsigned int value_total = suffix_token_count * (value_format == 0u ? row_width : value_row_bytes);
    if (idx < value_total) {
        unsigned int token;
        unsigned int lane_or_byte;
        if (value_format == 0u) {
            token = idx / row_width;
            lane_or_byte = idx - token * row_width;
        } else {
            token = idx / value_row_bytes;
            lane_or_byte = idx - token * value_row_bytes;
        }
        unsigned int physical_token = termite_tq_physical_token(token_start + token, block_table, block_count, page_size_tokens, physical_token_capacity);
        if (physical_token == 0xffffffffu) return;
        const float* src_row = v_src + token * row_width;
        unsigned char* dst_row = v_dst + physical_token * value_row_bytes;
        if (value_format == 0u) {
            reinterpret_cast<float*>(dst_row)[lane_or_byte] = src_row[lane_or_byte];
        } else if (value_format == 1u) {
            unsigned int head_stride = head_dim + 4u;
            unsigned int kv_head = lane_or_byte / head_stride;
            unsigned int head_byte = lane_or_byte - kv_head * head_stride;
            if (kv_head >= num_kv_heads) return;
            const float* src_head = src_row + kv_head * head_dim;
            float scale = termite_tq_int8_head_scale(src_head, head_dim);
            if (head_byte < 4u) {
                termite_tq_store_f32_byte(dst_row + kv_head * head_stride, scale, head_byte);
            } else {
                unsigned int lane = head_byte - 4u;
                if (lane >= head_dim) return;
                float scaled = roundf(src_head[lane] / scale);
                scaled = fminf(fmaxf(scaled, -127.0f), 127.0f);
                signed char q = (signed char)scaled;
                dst_row[kv_head * head_stride + 4u + lane] = (unsigned char)q;
            }
        } else if (value_format == 2u) {
            unsigned int value_index = lane_or_byte >> 1;
            if (value_index < row_width) termite_tq_store_f16_byte(dst_row + value_index * 2u, src_row[value_index], lane_or_byte & 1u);
        } else if (value_format == 3u) {
            const unsigned int group_size = 32u;
            const unsigned int group_bytes = 18u;
            unsigned int group = lane_or_byte / group_bytes;
            unsigned int group_byte = lane_or_byte - group * group_bytes;
            unsigned int value_start = group * group_size;
            if (value_start >= row_width) return;
            unsigned int remaining = row_width - value_start;
            if (remaining > group_size) remaining = group_size;
            const float* src_group = src_row + value_start;
            float scale_f32 = termite_tq_int4_group_scale(src_group, remaining);
            if (group_byte < 2u) {
                termite_tq_store_f16_byte(dst_row + group * group_bytes, scale_f32, group_byte);
            } else {
                unsigned int pair = (group_byte - 2u) * 2u;
                if (pair >= group_size) return;
                float inv_scale = 1.0f / scale_f32;
                float lo_f = pair < remaining ? roundf(src_group[pair] * inv_scale) : 0.0f;
                float hi_f = (pair + 1u) < remaining ? roundf(src_group[pair + 1u] * inv_scale) : 0.0f;
                lo_f = fminf(fmaxf(lo_f, -7.0f), 7.0f);
                hi_f = fminf(fmaxf(hi_f, -7.0f), 7.0f);
                signed char lo_i = (signed char)lo_f;
                signed char hi_i = (signed char)hi_f;
                dst_row[group * group_bytes + group_byte] =
                    ((unsigned char)lo_i & 0x0fu) | (unsigned char)(((unsigned char)hi_i & 0x0fu) << 4);
            }
        }
    }
}
