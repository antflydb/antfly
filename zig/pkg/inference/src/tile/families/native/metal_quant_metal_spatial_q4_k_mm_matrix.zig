// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           "kernel void termite_florence_q4_k_mm_matrix_m64_n64(device const float *input [[buffer(0)]], device const uchar *weight_q4_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {\n"
    \\           "    threadgroup uchar shared[16384];\n"
    \\           "    termite_k_mm_matrix_m64_n64<float, half, false>(input, weight_q4_k, output, rows, in_dim, out_dim, thread_pos, group_pos, simdgroup_id, shared);\n"
    \\           "}\n"
    \\
;
