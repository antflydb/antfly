// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
