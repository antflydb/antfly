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

// native-program:entry termite_apply_subtract_1x
           TERMITE_MSL_ABI_BINARY_ELEMENT(termite_apply_subtract_1x) "\n"
           "    termite_binary_impl<1u>(lhs, rhs, output, p, gid);\n"
           "}\n"
// native-program:entry termite_apply_divide_1x
           TERMITE_MSL_ABI_BINARY_ELEMENT(termite_apply_divide_1x) "\n"
           "    termite_binary_impl<2u>(lhs, rhs, output, p, gid);\n"
           "}\n"
// native-program:entry termite_apply_less_than_1x
           TERMITE_MSL_ABI_BINARY_ELEMENT(termite_apply_less_than_1x) "\n"
           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
           "    uint li = (p.flags & 1u) != 0u ? 0u : gid, ri = (p.flags & 2u) != 0u ? 0u : gid;\n"
           "    bool lint = (p.flags & 8u) != 0u, rint = (p.flags & 16u) != 0u;\n"
           "    bool less;\n"
           "    if (lint && rint) less = ((device const int *)lhs)[li] < ((device const int *)rhs)[ri];\n"
           "    else if (lint) less = termite_int_float_less(((device const int *)lhs)[li], rhs[ri]);\n"
           "    else if (rint) less = termite_float_int_less(lhs[li], ((device const int *)rhs)[ri]);\n"
           "    else less = lhs[li] < rhs[ri];\n"
           "    output[gid] = less ? 1.0f : 0.0f;\n"
           "}\n"
// native-program:entry termite_clamp_silu
           TERMITE_MSL_ABI_CLAMP_ACTIVATION(termite_clamp_silu) "\n"
           "    if (gid >= p.total) return; float v = input[gid];\n"
           "    if ((p.flags & 1u) != 0u) v = max(v, p.min_value);\n"
           "    if ((p.flags & 2u) != 0u) v = min(v, p.max_value);\n"
           "    output[gid] = v / (1.0f + exp(-v));\n"
           "}\n"
// native-program:entry termite_polar4_key_scores
           TERMITE_MSL_ABI_COMPRESSED_KEY_SCORES(termite_polar4_key_scores) "\n"
           "    uint total = p.num_heads * p.q_len * p.block_tokens; if (gid >= total) return; uint ki = gid % p.block_tokens; uint tmp = gid / p.block_tokens; uint qi = tmp % p.q_len; uint h = tmp / p.q_len; uint heads_per_group = p.num_heads / p.num_kv_heads; uint kv_h = h / heads_per_group; uint kv_head_off = kv_h * p.head_dim; uint q_base = qi * p.num_heads * p.head_dim + h * p.head_dim; uint k_base = ki * p.key_row_bytes; float acc = 0.0f;\n"
           "    for (uint d = 0; d < p.head_dim; ++d) { uint value_index = kv_head_off + d; uchar packed = encoded_key[k_base + (value_index >> 1)]; uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu); float k = float(code) / 7.5f - 1.0f; acc += q[q_base + d] * k; }\n"
           "    scores[h * p.q_len * p.block_tokens + qi * p.block_tokens + ki] = acc;\n"
           "}\n"
// native-program:entry termite_turbo3_key_scores
           TERMITE_MSL_ABI_COMPRESSED_KEY_SCORES(termite_turbo3_key_scores) "\n"
           "    uint total = p.num_heads * p.q_len * p.block_tokens; if (gid >= total) return; uint ki = gid % p.block_tokens; uint tmp = gid / p.block_tokens; uint qi = tmp % p.q_len; uint h = tmp / p.q_len; uint heads_per_group = p.num_heads / p.num_kv_heads; uint kv_h = h / heads_per_group; uint kv_head_off = kv_h * p.head_dim; uint q_base = qi * p.num_heads * p.head_dim + h * p.head_dim; uint k_base = ki * p.key_row_bytes; uint base_key_row_bytes = (p.num_kv_heads * p.head_dim * 3u + 7u) / 8u; uint residual_base = k_base + base_key_row_bytes; thread float residual_projection[32];\n"
           "    for (uint projection = 0; projection < 32u; ++projection) { float projected_query = 0.0f; for (uint d = 0; d < p.head_dim; ++d) { ulong x = (ulong(kv_h + 1u) * 0x9e3779b97f4a7c15UL); x ^= (ulong(projection + 1u) * 0xbf58476d1ce4e5b9UL); x ^= (ulong(d + 1u) * 0x94d049bb133111ebUL); x ^= x >> 30; x *= 0xbf58476d1ce4e5b9UL; x ^= x >> 27; x *= 0x94d049bb133111ebUL; x ^= x >> 31; float sign = ((x & 1UL) == 0UL) ? 1.0f : -1.0f; projected_query += sign * q[q_base + d]; } residual_projection[projection] = projected_query; }\n"
           "    float acc = 0.0f; for (uint d = 0; d < p.head_dim; ++d) { uint value_index = kv_head_off + d; uint bit_offset = value_index * 3u; uint byte_index = k_base + (bit_offset >> 3); uint shift = bit_offset & 7u; uint bits = uint(encoded_key[byte_index]) >> shift; if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift); float k = float(bits & 0x07u) / 3.5f - 1.0f; acc += q[q_base + d] * k; }\n"
           "    float residual_acc = 0.0f; for (uint projection = 0; projection < 32u; ++projection) { uint residual_bit = kv_h * 32u + projection; uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)]; float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f; residual_acc += residual_sign * residual_projection[projection]; }\n"
           "    scores[h * p.q_len * p.block_tokens + qi * p.block_tokens + ki] = acc + 0.125f * (residual_acc / 32.0f);\n"
           "}\n"
// native-program:entry termite_encode_polar4_key
           TERMITE_MSL_ABI_COMPRESSED_KEY_ENCODING(termite_encode_polar4_key) "\n"
           "    uint row_bytes = p.base_key_row_bytes; uint total = p.rows * row_bytes; if (gid >= total) return; uint row = gid / row_bytes; uint byte_in_row = gid - row * row_bytes; uint row_values = p.num_kv_heads * p.head_dim; uint value0 = byte_in_row * 2u; uint k_base = row * row_values; uint lo = value0 < row_values ? termite_encode_polar4_scalar(k[k_base + value0]) : 0u; uint hi = (value0 + 1u) < row_values ? termite_encode_polar4_scalar(k[k_base + value0 + 1u]) : 0u; encoded_key[row * p.key_row_bytes + byte_in_row] = uchar(lo | (hi << 4));\n"
           "}\n"
// native-program:entry termite_encode_turbo3_key
           TERMITE_MSL_ABI_COMPRESSED_KEY_ENCODING(termite_encode_turbo3_key) "\n"
           "    uint base_bytes = p.base_key_row_bytes; uint residual_bytes = (p.num_kv_heads * 32u + 7u) / 8u; uint row_work = base_bytes + residual_bytes; uint total = p.rows * row_work; if (gid >= total) return; uint row = gid / row_work; uint local = gid - row * row_work; uint row_values = p.num_kv_heads * p.head_dim; uint k_base = row * row_values; uint encoded_base = row * p.key_row_bytes;\n"
           "    if (local < base_bytes) { uint out = 0u; uint bit_base = local * 8u; for (uint bit = 0; bit < 8u; ++bit) { uint packed_bit = bit_base + bit; uint value_index = packed_bit / 3u; if (value_index >= row_values) continue; uint code_bit = packed_bit - value_index * 3u; uint code = termite_encode_turbo3_scalar(k[k_base + value_index]); out |= ((code >> code_bit) & 1u) << bit; } encoded_key[encoded_base + local] = uchar(out); return; }\n"
           "    uint residual_byte = local - base_bytes; uint out = 0u; for (uint bit = 0; bit < 8u; ++bit) { uint residual_bit = residual_byte * 8u + bit; if (residual_bit >= p.num_kv_heads * 32u) continue; uint kv_h = residual_bit / 32u; uint projection = residual_bit - kv_h * 32u; uint head_base = kv_h * p.head_dim; float projected_residual = 0.0f; for (uint d = 0; d < p.head_dim; ++d) { float value = k[k_base + head_base + d]; uint code = termite_encode_turbo3_scalar(value); float decoded = termite_decode_turbo3_scalar(code); projected_residual += termite_random_sign(kv_h, projection, d) * (value - decoded); } if (projected_residual >= 0.0f) out |= 1u << bit; } encoded_key[encoded_base + base_bytes + residual_byte] = uchar(out);\n"
           "}\n"
// native-program:entry termite_dot_general_2d_f32
           TERMITE_MSL_ABI_DOT_2D(termite_dot_general_2d_f32) "\n"
           "    uint total = p.m * p.n; if (gid >= total || p.k == 0u) return; uint row = gid / p.n; uint col = gid - row * p.n; float acc = 0.0f;\n"
           "    for (uint kk = 0u; kk < p.k; ++kk) { float r = p.rhs_contract_axis == 0u ? rhs[kk * p.n + col] : rhs[col * p.k + kk]; acc += lhs[row * p.k + kk] * r; }\n"
           "    output[gid] = acc;\n"
           "}\n"
// native-program:entry termite_dot_general_rank1_outer_f32
           TERMITE_MSL_ABI_DOT_2D(termite_dot_general_rank1_outer_f32) "\n"
           "    uint total = p.m * p.n; if (gid >= total || p.k != 1u) return; uint row = gid / p.n; uint col = gid - row * p.n; output[gid] = lhs[row] * rhs[col];\n"
           "}\n"
