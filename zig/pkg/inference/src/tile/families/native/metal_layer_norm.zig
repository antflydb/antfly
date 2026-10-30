// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.metal(
        \\kernel void termite_apply_layer_norm_1x(device const float *input [[buffer(0)]], device const float *gamma [[buffer(1)]], device const float *beta [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_layer_norm_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
        \\    termite_layer_norm_family<0u, false, false>(input, input, input, gamma, beta, output, output, p, nullptr, gid, 0u, 1u);
        \\}
        \\kernel void termite_apply_layer_norm_rows(device const float *input [[buffer(0)]], device const float *gamma [[buffer(1)]], device const float *beta [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_layer_norm_params &p [[buffer(4)]], threadgroup float *scratch [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]], uint3 threads_per_tg [[threads_per_threadgroup]]) {
        \\    termite_layer_norm_family<0u, false, true>(input, input, input, gamma, beta, output, output, p, scratch, tg.x, tid, threads_per_tg.x);
        \\}
        \\
    );

pub const source_1 =
    program.metal(
        \\kernel void termite_apply_add_layer_norm_1x(device const float *a [[buffer(0)]], device const float *b [[buffer(1)]], device const float *gamma [[buffer(2)]], device const float *beta [[buffer(3)]], device float *output [[buffer(4)]], constant termite_metal_apply_layer_norm_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {
        \\    termite_layer_norm_family<1u, false, false>(a, b, a, gamma, beta, output, output, p, nullptr, gid, 0u, 1u);
        \\}
        \\kernel void termite_apply_add_layer_norm_sum_1x(device const float *a [[buffer(0)]], device const float *b [[buffer(1)]], device const float *gamma [[buffer(2)]], device const float *beta [[buffer(3)]], device float *sum_out [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_apply_layer_norm_params &p [[buffer(6)]], uint gid [[thread_position_in_grid]]) {
        \\    termite_layer_norm_family<1u, true, false>(a, b, a, gamma, beta, sum_out, output, p, nullptr, gid, 0u, 1u);
        \\}
        \\kernel void termite_apply_add_layer_norm_rows(device const float *a [[buffer(0)]], device const float *b [[buffer(1)]], device const float *gamma [[buffer(2)]], device const float *beta [[buffer(3)]], device float *output [[buffer(4)]], constant termite_metal_apply_layer_norm_params &p [[buffer(5)]], threadgroup float *scratch [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]], uint3 threads_per_tg [[threads_per_threadgroup]]) {
        \\    termite_layer_norm_family<1u, false, true>(a, b, a, gamma, beta, output, output, p, scratch, tg.x, tid, threads_per_tg.x);
        \\}
        \\kernel void termite_apply_add_layer_norm_sum_rows(device const float *a [[buffer(0)]], device const float *b [[buffer(1)]], device const float *gamma [[buffer(2)]], device const float *beta [[buffer(3)]], device float *sum_out [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_apply_layer_norm_params &p [[buffer(6)]], threadgroup float *scratch [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]], uint3 threads_per_tg [[threads_per_threadgroup]]) {
        \\    termite_layer_norm_family<1u, true, true>(a, b, a, gamma, beta, sum_out, output, p, scratch, tg.x, tid, threads_per_tg.x);
        \\}
        \\kernel void termite_apply_bias_add_layer_norm_1x(device const float *projected [[buffer(0)]], device const float *bias [[buffer(1)]], device const float *residual [[buffer(2)]], device const float *gamma [[buffer(3)]], device const float *beta [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_apply_layer_norm_params &p [[buffer(6)]], uint gid [[thread_position_in_grid]]) {
        \\    termite_layer_norm_family<2u, false, false>(projected, residual, bias, gamma, beta, output, output, p, nullptr, gid, 0u, 1u);
        \\}
        \\kernel void termite_apply_bias_add_layer_norm_rows(device const float *projected [[buffer(0)]], device const float *bias [[buffer(1)]], device const float *residual [[buffer(2)]], device const float *gamma [[buffer(3)]], device const float *beta [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_apply_layer_norm_params &p [[buffer(6)]], threadgroup float *scratch [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]], uint3 threads_per_tg [[threads_per_threadgroup]]) {
        \\    termite_layer_norm_family<2u, false, true>(projected, residual, bias, gamma, beta, output, output, p, scratch, tg.x, tid, threads_per_tg.x);
        \\}
        \\
    );

pub const source_2 =
    \\           "kernel void termite_gliner_boundary_ffn_bias_add_layer_norm(device const float *projected [[buffer(0)]], device const float *bias [[buffer(1)]], device const float *residual [[buffer(2)]], device const float *gamma [[buffer(3)]], device const float *beta [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_apply_layer_norm_params &p [[buffer(6)]], uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]) {\n"
    \\           "    uint row=gid/32u;if(p.hidden_size==0u)return;uint base=row*p.hidden_size;float sum=0.0f;for(uint d=lane;d<p.hidden_size;d+=32u){float v=(projected[base+d]+bias[d])+residual[base+d];sum+=v;}float mean=simd_sum(sum)/float(p.hidden_size);float sq=0.0f;for(uint d=lane;d<p.hidden_size;d+=32u){float v=(projected[base+d]+bias[d])+residual[base+d]-mean;sq+=v*v;}float inv=rsqrt(simd_sum(sq)/float(p.hidden_size)+p.eps);for(uint d=lane;d<p.hidden_size;d+=32u){float v=(projected[base+d]+bias[d])+residual[base+d];output[base+d]=(v-mean)*inv*gamma[d]+beta[d];}\n"
    \\           "}\n"
    \\
;
