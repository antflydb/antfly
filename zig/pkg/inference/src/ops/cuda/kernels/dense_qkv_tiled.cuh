// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact 256-lane Q/K/V selection and reduction; Weight is selected by the caller. No include guard: native-scope phase.
    unsigned int q_plane = rows * q_out_dim;
    unsigned int kv_plane = rows * kv_out_dim;
    unsigned int total = q_plane + kv_plane * 2u;
    unsigned int global = blockIdx.x;
    if (global >= total) return;

    const Weight* weight;
    float* dst;
    unsigned int out_dim;
    unsigned int local;
    if (global < q_plane) {
        weight = weight_q;
        dst = dst_q;
        out_dim = q_out_dim;
        local = global;
    } else if (global < q_plane + kv_plane) {
        weight = weight_k;
        dst = dst_k;
        out_dim = kv_out_dim;
        local = global - q_plane;
    } else {
        weight = weight_v;
        dst = dst_v;
        out_dim = kv_out_dim;
        local = global - q_plane - kv_plane;
    }

    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        acc += input[row * in_dim + i] * termite_qkv_weight_value(weight[col * in_dim + i]);
    }
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0];
