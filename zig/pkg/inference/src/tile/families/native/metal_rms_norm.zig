// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 = "\t           \"kernel void termite_apply_rms_norm_1x(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_layer_norm_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {\\n\"\n           \"    if (gid != 0) return;\\n\"\n           \"    if (p.hidden_size == 0) return;\\n\"\n           \"    float mean_square = 0.0f;\\n\"\n           \"    for (uint i = 0; i < p.hidden_size; ++i) { float x = input[i]; mean_square += x * x; }\\n\"\n           \"    mean_square /= float(p.hidden_size);\\n\"\n           \"    float inv_rms = rsqrt(mean_square + p.eps);\\n\"\n           \"    for (uint i = 0; i < p.hidden_size; ++i) output[i] = input[i] * inv_rms * weight[i];\\n\"\n           \"}\\n\"\n";

pub const source_1 =
    program.join(&.{
        program.metal(
            \\kernel void termite_apply_rms_norm_1x_reduce(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_layer_norm_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]) {
            \\    termite_rms_norm_tree<false, false>(input, weight, input, output, shmem, 0u, p.hidden_size, p.eps, 1.0f, tid, tpg);
            \\}
            \\kernel void termite_compute_rms_inv_scale_1x(device const float *input [[buffer(0)]], device float *inv_scale [[buffer(1)]], constant termite_metal_apply_layer_norm_params &p [[buffer(2)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]) {
            \\    if (p.hidden_size == 0) return;
            \\    float inv_rms = antfly_rms_tree_inverse<float, false>(input, 0u, p.hidden_size, p.eps, shmem, tid, tpg, nullptr, 0u);
            \\    if (tid == 0u) inv_scale[0] = inv_rms;
            \\}
            \\
        ),

        \\
        \\
        ,
        program.metal(
            \\kernel void termite_apply_rms_norm_rows(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_row_norm_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
            \\    if (gid >= p.rows) return;
            \\    if (p.hidden_size == 0) return;
            \\    uint row_base = gid * p.hidden_size;
            \\    float mean_square = 0.0f;
            \\    for (uint i = 0; i < p.hidden_size; ++i) { float x = input[row_base + i]; mean_square += x * x; }
            \\    mean_square /= float(p.hidden_size);
            \\    float inv_rms = rsqrt(mean_square + p.eps);
            \\    for (uint i = 0; i < p.hidden_size; ++i) output[row_base + i] = input[row_base + i] * inv_rms * weight[i];
            \\}
            \\kernel void termite_apply_rms_norm_rows_reduce(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_row_norm_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
            \\    if (row >= p.rows) return; termite_rms_norm_tree<false, false>(input, weight, input, output, shmem, row * p.hidden_size, p.hidden_size, p.eps, 1.0f, tid, tpg);
            \\}
            \\
        ),
    });

pub const source_2 =
    \\           "kernel void termite_apply_rms_norm_rows_simd4_reduce(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_row_norm_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint simd_group [[simdgroup_index_in_threadgroup]], uint simd_lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           "    termite_rms_norm_simd4_impl<false>(input, weight, output, p.rows, p.hidden_size, p.eps, 0u, 0.0f, 0.0f, shmem, tid, tpg, simd_group, simd_lane, row);\n"
    \\           "}\n"
    \\
;

pub const source_3 =
    program.metal(
        \\kernel void antfly_norm_clamp(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant antfly_norm_clamp_params &p [[buffer(3)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint simd_group [[simdgroup_index_in_threadgroup]], uint simd_lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    threadgroup float shmem[32];
        \\    termite_rms_norm_simd4_impl<true>(input, weight, output, p.rows, p.dim, p.eps, p.flags, p.lo, p.hi, shmem, tid, tpg, simd_group, simd_lane, row);
        \\}
        \\kernel void termite_apply_rms_norm_rows_triple_simd4_reduce(device const float *input [[buffer(0)]], device const float *first_weight [[buffer(1)]], device const float *second_weight [[buffer(2)]], device const float *third_weight [[buffer(3)]], device float *first_output [[buffer(4)]], device float *second_output [[buffer(5)]], device float *third_output [[buffer(6)]], constant termite_metal_apply_row_norm_params &p [[buffer(7)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint simd_group [[simdgroup_index_in_threadgroup]], uint simd_lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.hidden_size == 0u || (p.hidden_size & 3u) != 0u) return;
        \\    uint row_base = row * p.hidden_size;
        \\    uint vectors = p.hidden_size >> 2u;
        \\    float inv_rms = termite_rms_norm_simd4_inverse(input, row_base, p.hidden_size, p.eps, shmem, tid, tpg, simd_group, simd_lane);
        \\    for (uint v = tid; v < vectors; v += tpg) {
        \\        uint j = v << 2u;
        \\        uint i = row_base + j;
        \\        float4 x = float4(input[i], input[i + 1u], input[i + 2u], input[i + 3u]);
        \\        float4 first_y = x * (inv_rms * float4(first_weight[j], first_weight[j + 1u], first_weight[j + 2u], first_weight[j + 3u]));
        \\        float4 second_y = x * (inv_rms * float4(second_weight[j], second_weight[j + 1u], second_weight[j + 2u], second_weight[j + 3u]));
        \\        float4 third_y = x * (inv_rms * float4(third_weight[j], third_weight[j + 1u], third_weight[j + 2u], third_weight[j + 3u]));
        \\        first_output[i] = first_y.x; first_output[i + 1u] = first_y.y; first_output[i + 2u] = first_y.z; first_output[i + 3u] = first_y.w;
        \\        second_output[i] = second_y.x; second_output[i + 1u] = second_y.y; second_output[i + 2u] = second_y.z; second_output[i + 3u] = second_y.w;
        \\        third_output[i] = third_y.x; third_output[i + 1u] = third_y.y; third_output[i + 2u] = third_y.z; third_output[i + 3u] = third_y.w;
        \\    }
        \\}
        \\
    );

pub const source_4 =
    program.metal(
        \\kernel void termite_apply_rms_norm_add_1x(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_layer_norm_params &p [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]) {
        \\    termite_rms_norm_tree<true, false>(input, weight, residual, output, shmem, 0u, p.hidden_size, p.eps, 1.0f, tid, tpg);
        \\}
        \\kernel void termite_apply_rms_norm_add_rows_canonical(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_layer_norm_params &p [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint r [[threadgroup_position_in_grid]]) {
        \\    termite_rms_norm_tree<true, false>(input, weight, residual, output, shmem, r * p.hidden_size, p.hidden_size, p.eps, 1.0f, tid, tpg);
        \\}
        \\kernel void termite_apply_rms_norm_add_1x_sumsq(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], device const float *sumsq [[buffer(4)]], constant termite_metal_apply_layer_norm_params &p [[buffer(5)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]) {
        \\    termite_rms_norm_tree<true, false, float, true, true>(input, weight, residual, output, shmem, 0u, p.hidden_size, p.eps, 1.0f, tid, tpg, sumsq, 0u);
        \\}
        \\kernel void termite_apply_rms_norm_add_rows_sumsq(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], device const float *sumsq [[buffer(4)]], constant termite_metal_apply_layer_norm_params &p [[buffer(5)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint r [[threadgroup_position_in_grid]]) {
        \\    termite_rms_norm_tree<true, false, float, true, true>(input, weight, residual, output, shmem, r * p.hidden_size, p.hidden_size, p.eps, 1.0f, tid, tpg, sumsq, r * ((p.hidden_size + 7u) / 8u));
        \\}
        \\kernel void termite_apply_rms_norm_add_scale_1x_sumsq(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], device const float *sumsq [[buffer(4)]], constant termite_metal_apply_layer_norm_scale_params &p [[buffer(5)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]) {
        \\    termite_rms_norm_tree<true, true, float, true, true>(input, weight, residual, output, shmem, 0u, p.hidden_size, p.eps, p.scale, tid, tpg, sumsq, 0u);
        \\}
        \\kernel void termite_apply_rms_norm_add_1x_f16_input(device const half *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_layer_norm_params &p [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]) {
        \\    termite_rms_norm_tree<true, false, half, false, true>(input, weight, residual, output, shmem, 0u, p.hidden_size, p.eps, 1.0f, tid, tpg);
        \\}
        \\kernel void termite_apply_rms_norm_add_scale_1x(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_layer_norm_scale_params &p [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]) {
        \\    termite_rms_norm_tree<true, true>(input, weight, residual, output, shmem, 0u, p.hidden_size, p.eps, p.scale, tid, tpg);
        \\}
        \\kernel void termite_apply_rms_norm_add_scale_rows(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_row_norm_scale_params &p [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]], uint tpg [[threads_per_threadgroup]]) {
        \\    if (row >= p.rows) return; termite_rms_norm_tree<true, true>(input, weight, residual, output, shmem, row * p.hidden_size, p.hidden_size, p.eps, p.scale, tid, tpg);
        \\}
        \\kernel void termite_apply_rms_norm_add_scale_simd4(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *residual [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_row_norm_scale_params &p [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint simd_group [[simdgroup_index_in_threadgroup]], uint simd_lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.hidden_size == 0u || (p.hidden_size & 3u) != 0u) return;
        \\    uint base = row * p.hidden_size;
        \\    uint vectors = p.hidden_size >> 2u;
        \\    float inv_rms = termite_rms_norm_simd4_inverse(input, base, p.hidden_size, p.eps, shmem, tid, tpg, simd_group, simd_lane);
        \\    device volatile float *rounded = output;
        \\    for (uint v = tid; v < vectors; v += tpg) {
        \\        uint j = v << 2u; uint i = base + j;
        \\        float4 x = float4(input[i], input[i + 1u], input[i + 2u], input[i + 3u]);
        \\        float4 w = float4(weight[j], weight[j + 1u], weight[j + 2u], weight[j + 3u]);
        \\        float4 normalized = x * (inv_rms * w);
        \\        rounded[i] = normalized.x; rounded[i + 1u] = normalized.y; rounded[i + 2u] = normalized.z; rounded[i + 3u] = normalized.w;
        \\    }
        \\    threadgroup_barrier(mem_flags::mem_device);
        \\    for (uint v = tid; v < vectors; v += tpg) {
        \\        uint j = v << 2u; uint i = base + j;
        \\        float4 normalized = float4(rounded[i], rounded[i + 1u], rounded[i + 2u], rounded[i + 3u]);
        \\        float4 r = float4(residual[i], residual[i + 1u], residual[i + 2u], residual[i + 3u]);
        \\        float4 y = normalized + r;
        \\        if (p.scale != 1.0f) y *= p.scale;
        \\        output[i] = y.x; output[i + 1u] = y.y; output[i + 2u] = y.z; output[i + 3u] = y.w;
        \\    }
        \\}
        \\
    );
