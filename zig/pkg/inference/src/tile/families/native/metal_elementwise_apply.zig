// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_BINARY_ELEMENT(termite_apply_add_1x) "\n"
    \\           "    termite_binary_impl<0u>(lhs, rhs, output, p, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_apply_add_scale_1x(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_add_params &p [[buffer(3)]], constant float &scale [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
    \\           "    output[gid] = (lhs[gid] + rhs[gid]) * scale;\n"
    \\           "}\n"
    \\           "kernel void termite_apply_scaled_add_scale_1x(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_add_params &p [[buffer(3)]], constant float2 &scales [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
    \\           "    output[gid] = (lhs[gid] * scales.x + rhs[gid]) * scales.y;\n"
    \\           "}\n"
    \\           "kernel void termite_apply_multiply_add_1x(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device const float *addend [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_add_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
    \\           "    output[gid] = lhs[gid] * rhs[gid] + addend[gid];\n"
    \\           "}\n"
    \\           "kernel void termite_apply_multiply_add2_1x(device const float *lhs0 [[buffer(0)]], device const float *rhs0 [[buffer(1)]], device const float *lhs1 [[buffer(2)]], device const float *rhs1 [[buffer(3)]], device float *output [[buffer(4)]], constant termite_metal_apply_add_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.rows * p.dim; if (gid >= total) return;\n"
    \\           "    output[gid] = lhs0[gid] * rhs0[gid] + lhs1[gid] * rhs1[gid];\n"
    \\           "}\n"
    \\           "kernel void termite_training_accumulate_f32(device float *accum [[buffer(0)]], device const float *grad [[buffer(1)]], constant termite_metal_training_accumulate_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    if (gid >= p.elem_count) return;\n"
    \\           "    float g = grad[gid] * p.scale;\n"
    \\           "    accum[gid] = (p.first != 0u) ? g : (accum[gid] + g);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           "kernel void termite_training_adamw_f32(device float *weight [[buffer(0)]], device float *grad [[buffer(1)]], device float *m [[buffer(2)]], device float *v [[buffer(3)]], constant termite_metal_training_adamw_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    termite_training_adamw_impl<false>(weight, grad, m, v, weight, m, v, p, grad, gid);\n"
    \\           "}\n"
    \\
;

pub const source_2 =
    \\           "kernel void termite_training_accumulate_into_f32(device const float *previous [[buffer(0)]], device const float *grad [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_training_accumulate_into_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    if (gid >= p.elem_count) return;\n"
    \\           "    float value = (p.flags & 1u) != 0u ? previous[gid] : 0.0f;\n"
    \\           "    if ((p.flags & 2u) != 0u) value = value + grad[gid] / p.divisor;\n"
    \\           "    if ((p.flags & 4u) != 0u) value = value * p.renormalize;\n"
    \\           "    output[gid] = value;\n"
    \\           "}\n"
    \\
;

pub const source_3 =
    \\           "kernel void termite_training_adamw_into_f32(device const float *weight [[buffer(0)]], device float *grad [[buffer(1)]], device const float *m [[buffer(2)]], device const float *v [[buffer(3)]], device float *weight_out [[buffer(4)]], device float *m_out [[buffer(5)]], device float *v_out [[buffer(6)]], constant termite_metal_training_adamw_params &p [[buffer(7)]], device float *flag [[buffer(8)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    termite_training_adamw_impl<true>(weight, grad, m, v, weight_out, m_out, v_out, p, flag, gid);\n"
    \\           "}\n"
    \\
;

pub const source_4 =
    \\           TERMITE_MSL_ABI_BINARY_ELEMENT(termite_apply_multiply_1x) "\n"
    \\           "    termite_binary_impl<3u>(lhs, rhs, output, p, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_apply_scale_1x(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant float &scale [[buffer(2)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    output[gid] = input[gid] * scale;\n"
    \\           "}\n"
    \\
;
