// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Pinned PyTorch/CUTLASS implementation; see THIRD_PARTY_NOTICES.md.
#include <ATen/native/transformers/cuda/mem_eff_attention/kernel_forward.h>
using BoundaryForward = PyTorchMemEffAttention::AttentionKernel<float, cutlass::arch::Sm80, true, 64, 64, 64, true, true>;
extern "C" __global__ void boundary_forward_metadata(unsigned int* out) {
    out[0] = sizeof(BoundaryForward::SharedStorage);
}
extern "C" __global__ __launch_bounds__(128) void boundary_forward(
    float* output, float* lse, const float* packed, const float* bias,
    int batch, int sequence, int heads, int bias_columns) {
    BoundaryForward::Params p;
    p.query_ptr=packed; p.key_ptr=packed+heads*32; p.value_ptr=packed+heads*64;
    p.attn_bias_ptr=bias;
    p.output_ptr=output; p.logsumexp_ptr=lse;
    p.scale=0.17677669529663687f;
    p.head_dim=32; p.head_dim_value=32; p.num_queries=sequence; p.num_keys=sequence;
    p.num_keys_absolute=sequence;
    p.q_strideM=p.k_strideM=p.v_strideM=3*heads*32;
    p.q_strideH=p.k_strideH=p.v_strideH=32;
    p.q_strideB=p.k_strideB=p.v_strideB=(long long)sequence*3*heads*32;
    p.o_strideM=heads*32;
    p.bias_strideM=bias_columns; p.bias_strideH=0; p.bias_strideB=sequence*bias_columns;
    p.num_batches=batch; p.num_heads=heads;
    if(p.advance_to_block()) BoundaryForward::attention_kernel(p);
}

static_assert(sizeof(BoundaryForward::SharedStorage) == 36352, "forward ABI changed");
// Build padded additive bias from the existing binary boundary mask. Diagonal
// entries remain allowed even for padding, so every query has a finite row.
extern "C" __global__ void boundary_bias(float* bias, const float* valid,
    int batch, int sequence, int columns, int window) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= batch * sequence * columns) return;
    int k = i % columns, q = (i / columns) % sequence, b = i / (columns * sequence);
    bool allowed = k < sequence && (q == k || (valid[b * sequence + k] != 0.0f && (window == 0 || abs(q-k) <= window)));
    bias[i] = allowed ? 0.0f : -__int_as_float(0x7f800000);
}
