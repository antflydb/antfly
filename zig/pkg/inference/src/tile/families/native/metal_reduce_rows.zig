// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           "kernel void termite_apply_softmax_rows(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_apply_softmax_params &p [[buffer(2)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           "    if (row >= p.rows || p.dim == 0u) return;\n"
    \\           "    const uint threads = 256u;\n"
    \\           "    const uint base = row * p.dim;\n"
    \\           "    float local_max = -3.4028234663852886e+38f;\n"
    \\           "    for (uint col = tid; col < p.dim; col += threads) local_max = max(local_max, input[base + col]);\n"
    \\           "    shmem[tid] = local_max;\n"
    \\           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {\n"
    \\           "        if (tid < stride) shmem[tid] = max(shmem[tid], shmem[tid + stride]);\n"
    \\           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    }\n"
    \\           "    const float row_max = shmem[0]; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    float local_sum = 0.0f;\n"
    \\           "    for (uint col = tid; col < p.dim; col += threads) local_sum += exp(input[base + col] - row_max);\n"
    \\           "    shmem[tid] = local_sum;\n"
    \\           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {\n"
    \\           "        if (tid < stride) shmem[tid] += shmem[tid + stride];\n"
    \\           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    }\n"
    \\           "    const float row_sum = max(shmem[0], 1.0e-30f);\n"
    \\           "    const float log_sum = log(row_sum);\n"
    \\           "    for (uint col = tid; col < p.dim; col += threads) {\n"
    \\           "        const float shifted = input[base + col] - row_max;\n"
    \\           "        output[base + col] = p.log_softmax != 0u ? shifted - log_sum : exp(shifted) / row_sum;\n"
    \\           "    }\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           "kernel void termite_reduce_last_dim_rows(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_reduce_last_dim_params &p [[buffer(2)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           "    if (row >= p.rows || p.dim == 0u) return;\n"
    \\           "    const uint threads = 256u;\n"
    \\           "    const uint base = row * p.dim;\n"
    \\           "    float local = p.kind == 1u ? -3.4028234663852886e+38f : 0.0f;\n"
    \\           "    if (p.kind == 1u) {\n"
    \\           "        for (uint col = tid; col < p.dim; col += threads) local = max(local, input[base + col]);\n"
    \\           "    } else {\n"
    \\           "        for (uint col = tid; col < p.dim; col += threads) local += input[base + col];\n"
    \\           "    }\n"
    \\           "    shmem[tid] = local;\n"
    \\           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {\n"
    \\           "        if (tid < stride) {\n"
    \\           "            if (p.kind == 1u) shmem[tid] = max(shmem[tid], shmem[tid + stride]);\n"
    \\           "            else shmem[tid] += shmem[tid + stride];\n"
    \\           "        }\n"
    \\           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    }\n"
    \\           "    if (tid == 0u) output[row] = p.kind == 2u ? shmem[0] / float(p.dim) : shmem[0];\n"
    \\           "}\n"
    \\
;

pub const source_2 =
    \\           "kernel void termite_multiply_reduce_last_dim_rows(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_reduce_last_dim_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n"
    \\           "    if (row >= p.rows || p.dim == 0u) return;\n"
    \\           "    const uint threads = 256u;\n"
    \\           "    const uint base = row * p.dim;\n"
    \\           "    float local = 0.0f;\n"
    \\           "    for (uint col = tid; col < p.dim; col += threads) local += lhs[base + col] * rhs[base + col];\n"
    \\           "    shmem[tid] = local;\n"
    \\           "    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {\n"
    \\           "        if (tid < stride) shmem[tid] += shmem[tid + stride];\n"
    \\           "        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    }\n"
    \\           "    if (tid == 0u) output[row] = shmem[0];\n"
    \\           "}\n"
    \\
;
