// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.metal(
        \\kernel void termite_apply_softmax_rows(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_apply_softmax_params &p [[buffer(2)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.dim == 0u) return;
        \\    const uint threads = 256u;
        \\    const uint base = row * p.dim;
        \\    float local_max = -3.4028234663852886e+38f;
        \\    for (uint col = tid; col < p.dim; col += threads) local_max = max(local_max, input[base + col]);
        \\    shmem[tid] = local_max;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {
        \\        if (tid < stride) shmem[tid] = max(shmem[tid], shmem[tid + stride]);
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    }
        \\    const float row_max = shmem[0]; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    float local_sum = 0.0f;
        \\    for (uint col = tid; col < p.dim; col += threads) local_sum += exp(input[base + col] - row_max);
        \\    shmem[tid] = local_sum;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {
        \\        if (tid < stride) shmem[tid] += shmem[tid + stride];
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    }
        \\    const float row_sum = max(shmem[0], 1.0e-30f);
        \\    const float log_sum = log(row_sum);
        \\    for (uint col = tid; col < p.dim; col += threads) {
        \\        const float shifted = input[base + col] - row_max;
        \\        output[base + col] = p.log_softmax != 0u ? shifted - log_sum : exp(shifted) / row_sum;
        \\    }
        \\}
        \\
    );

pub const source_1 =
    program.metal(
        \\kernel void termite_reduce_last_dim_rows(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_reduce_last_dim_params &p [[buffer(2)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.dim == 0u) return;
        \\    const uint threads = 256u;
        \\    const uint base = row * p.dim;
        \\    float local = p.kind == 1u ? -3.4028234663852886e+38f : 0.0f;
        \\    if (p.kind == 1u) {
        \\        for (uint col = tid; col < p.dim; col += threads) local = max(local, input[base + col]);
        \\    } else {
        \\        for (uint col = tid; col < p.dim; col += threads) local += input[base + col];
        \\    }
        \\    shmem[tid] = local;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {
        \\        if (tid < stride) {
        \\            if (p.kind == 1u) shmem[tid] = max(shmem[tid], shmem[tid + stride]);
        \\            else shmem[tid] += shmem[tid + stride];
        \\        }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    }
        \\    if (tid == 0u) output[row] = p.kind == 2u ? shmem[0] / float(p.dim) : shmem[0];
        \\}
        \\
    );

pub const source_2 =
    program.metal(
        \\kernel void termite_multiply_reduce_last_dim_rows(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_reduce_last_dim_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.dim == 0u) return;
        \\    const uint threads = 256u;
        \\    const uint base = row * p.dim;
        \\    float local = 0.0f;
        \\    for (uint col = tid; col < p.dim; col += threads) local += lhs[base + col] * rhs[base + col];
        \\    shmem[tid] = local;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {
        \\        if (tid < stride) shmem[tid] += shmem[tid + stride];
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    }
        \\    if (tid == 0u) output[row] = shmem[0];
        \\}
        \\
    );
