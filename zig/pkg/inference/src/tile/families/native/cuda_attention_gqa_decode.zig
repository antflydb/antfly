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

const program = @import("../../native_program.zig");
const cuda_phases = @import("cuda_phases.zig");

pub const source_0 =
    program.render(
        \\extern "C" __global__ void termite_gqa_attention_decode_f32(
        \\{{dense_gqa_parameters}}
        \\    unsigned int bias_mode
        \\) {
        \\    termite_gqa_scalar_decode_impl<false>(dst, q, k, v, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, nullptr);
        \\}
        \\
        \\extern "C" __global__ void termite_gqa_attention_decode_scalars_f32(
        \\{{dense_gqa_parameters}}
        \\    unsigned int bias_mode,
        \\    const unsigned int* decode_scalars
        \\) {
        \\    termite_gqa_scalar_decode_impl<true>(dst, q, k, v, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, decode_scalars);
        \\}
        \\
    , cuda_phases.attention);
