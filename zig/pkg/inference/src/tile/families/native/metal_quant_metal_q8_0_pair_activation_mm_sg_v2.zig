// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_Q8_PAIR_MATRIX(termite_q8_0_pair_activation_mm_sg_v2) " termite_q8_0_pair_activation_mm_sg_v2_impl(input, weight_gate, weight_up, output, p, ap, shmem, tiitg, sgitg, tg, false); }\n"
    \\           TERMITE_MSL_ABI_Q8_PAIR_MATRIX(termite_q8_0_pair_activation_mm_sg_v2_out_f16) " termite_q8_0_pair_activation_mm_sg_v2_impl(input, weight_gate, weight_up, output, p, ap, shmem, tiitg, sgitg, tg, true); }\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q8_0_linear_mm) "\n"
    \\           "    termite_q8_0_scalar_matrix_impl<float>(input, weight, output, p, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           "kernel void termite_q8_0_linear_mm_in_f16(device const half *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_linear_params &p [[buffer(3)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    termite_q8_0_scalar_matrix_impl<half>(input, weight, output, p, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;

pub const source_2 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_SIMD(termite_q8_0_pair_activation_multiply_mm) "\n"
    \\           "    termite_q8_0_pair_scalar_matrix_impl<float>(input, weight_gate, weight_up, output, p, ap, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           "kernel void termite_q8_0_pair_activation_multiply_mm_out_f16(device const float *input [[buffer(0)]], device const uchar *weight_gate [[buffer(1)]], device const uchar *weight_up [[buffer(2)]], device half *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], constant termite_metal_apply_activation_params &ap [[buffer(5)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    termite_q8_0_pair_scalar_matrix_impl<half>(input, weight_gate, weight_up, output, p, ap, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;
