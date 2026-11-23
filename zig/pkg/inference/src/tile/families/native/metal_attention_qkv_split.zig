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
    \\           "kernel void termite_dense_qkv_split_f32_2d(device const float *packed [[buffer(0)]], device float *q_out [[buffer(1)]], device float *k_out [[buffer(2)]], device float *v_out [[buffer(3)]], constant termite_metal_dense_qkv_split_bias_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    antfly_dense_split<true, false>(packed, packed, packed, packed, q_out, k_out, v_out, p.rows, p.total_out_dim, p.q_out_dim, p.kv_out_dim, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_dense_qkv_split_bias_f32_2d(device const float *packed [[buffer(0)]], device const float *q_bias [[buffer(1)]], device const float *k_bias [[buffer(2)]], device const float *v_bias [[buffer(3)]], device float *q_out [[buffer(4)]], device float *k_out [[buffer(5)]], device float *v_out [[buffer(6)]], constant termite_metal_dense_qkv_split_bias_params &p [[buffer(7)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    antfly_dense_split<true, true>(packed, q_bias, k_bias, v_bias, q_out, k_out, v_out, p.rows, p.total_out_dim, p.q_out_dim, p.kv_out_dim, gid);\n"
    \\           "}\n"
    \\
;
