// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           "kernel void termite_apply_linear_1x(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    if (gid >= p.out_dim) return; termite_dense_scalar(input, weight, bias, output, p.in_dim, p.out_dim, 0u, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_apply_linear_1x_reduce(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], uint lane [[thread_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {\n"
    \\           "    if (group >= p.out_dim) return; threadgroup float partials[32]; termite_dense_reduce(input, weight, bias, output, partials, p.in_dim, p.out_dim, 0u, group, lane);\n"
    \\           "}\n"
    \\
;
