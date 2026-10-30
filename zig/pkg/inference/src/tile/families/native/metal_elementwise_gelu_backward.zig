// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           "kernel void termite_apply_gelu_backward_1x(device const float *input [[buffer(0)]], device const float *upstream_grad [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_activation_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
    \\           "    output[gid] = termite_gelu_backward_scalar(input[gid], upstream_grad, gid, p.activation_kind);\n"
    \\           "}\n"
    \\           "kernel void termite_ffn_gelu_backward_rank1_branches(device const float *first_lhs [[buffer(0)]], device const float *first_rhs [[buffer(1)]], device const float *second_lhs [[buffer(2)]], device const float *second_rhs [[buffer(3)]], device const float *gelu_input [[buffer(4)]], device float *first_output [[buffer(5)]], device float *second_output [[buffer(6)]], device float *upstream_output [[buffer(7)]], device float *gelu_output [[buffer(8)]], constant termite_metal_apply_activation_params &p [[buffer(9)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    termite_ffn_gelu_backward_rank1_impl<false, true, true>(first_lhs, first_rhs, second_lhs, second_rhs, nullptr, gelu_input, first_output, second_output, upstream_output, gelu_output, p, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_ffn_gelu_backward_rank1_output(device const float *first_lhs [[buffer(0)]], device const float *first_rhs [[buffer(1)]], device const float *second_lhs [[buffer(2)]], device const float *second_rhs [[buffer(3)]], device const float *gelu_input [[buffer(4)]], device float *first_output [[buffer(5)]], device float *gelu_output [[buffer(6)]], constant termite_metal_apply_activation_params &p [[buffer(7)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    termite_ffn_gelu_backward_rank1_impl<false, false, false>(first_lhs, first_rhs, second_lhs, second_rhs, nullptr, gelu_input, first_output, nullptr, nullptr, gelu_output, p, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_ffn_gelu_backward_rank1_first_output(device const float *first_lhs [[buffer(0)]], device const float *first_rhs [[buffer(1)]], device const float *second_output [[buffer(2)]], device const float *gelu_input [[buffer(3)]], device float *first_output [[buffer(4)]], device float *gelu_output [[buffer(5)]], constant termite_metal_apply_activation_params &p [[buffer(6)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    termite_ffn_gelu_backward_rank1_impl<true, false, false>(first_lhs, first_rhs, nullptr, nullptr, second_output, gelu_input, first_output, nullptr, nullptr, gelu_output, p, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_ffn_gelu_backward_rank1_first_branch(device const float *first_lhs [[buffer(0)]], device const float *first_rhs [[buffer(1)]], device const float *second_output [[buffer(2)]], device const float *gelu_input [[buffer(3)]], device float *first_output [[buffer(4)]], device float *upstream_output [[buffer(5)]], device float *gelu_output [[buffer(6)]], constant termite_metal_apply_activation_params &p [[buffer(7)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    termite_ffn_gelu_backward_rank1_impl<true, false, true>(first_lhs, first_rhs, nullptr, nullptr, second_output, gelu_input, first_output, nullptr, upstream_output, gelu_output, p, gid);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           "kernel void termite_apply_activation_multiply_1x(device const float *gate [[buffer(0)]], device const float *up [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_activation_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
    \\           "    float y = termite_apply_activation_scalar(gate[gid], p.activation_kind);\n"
    \\           "    output[gid] = y == 0.0f ? 0.0f : y * up[gid];\n"
    \\           "}\n"
    \\           "kernel void termite_apply_activation_multiply_f16_output(device const float *gate [[buffer(0)]], device const float *up [[buffer(1)]], device half *output [[buffer(2)]], constant termite_metal_apply_activation_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
    \\           "    float y = termite_apply_activation_scalar(gate[gid], p.activation_kind);\n"
    \\           "    output[gid] = half(y == 0.0f ? 0.0f : y * up[gid]);\n"
    \\           "}\n"
    \\
;
