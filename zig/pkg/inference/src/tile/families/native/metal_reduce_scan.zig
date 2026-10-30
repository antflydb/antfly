// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           "kernel void termite_cumulative_sum_f32(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_scan_params &p [[buffer(2)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           "    threadgroup float totals[256];\n"
    \\           "    termite_cumulative_sum_impl<float>(input, output, p, tid, row, totals);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           "kernel void termite_cumulative_sum_i64(device const ulong *input [[buffer(0)]], device ulong *output [[buffer(1)]], constant termite_scan_params &p [[buffer(2)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           "    threadgroup ulong totals[256];\n"
    \\           "    termite_cumulative_sum_impl<ulong>(input, output, p, tid, row, totals);\n"
    \\           "}\n"
    \\
;
