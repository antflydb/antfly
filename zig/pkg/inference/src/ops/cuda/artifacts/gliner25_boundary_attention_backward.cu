// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Pinned PyTorch/CUTLASS implementation; see THIRD_PARTY_NOTICES.md.
#include <ATen/native/transformers/cuda/mem_eff_attention/kernel_backward.h>
using BoundaryBackward = PyTorchMemEffAttention::AttentionBackwardKernel<cutlass::arch::Sm80, float, true, false, false, 64, 64, 32, false>;
// FP32 multiplication followed by the pinned ascending warp reduction.
extern "C" __global__ void boundary_delta32(float* delta,const float* output,const float* grad_output,int batch,int sequence,int heads) {
    int row=blockIdx.x*blockDim.y+threadIdx.y;
    if(row>=batch*heads*sequence)return;
    int bh=row/sequence, q=row%sequence, b=bh/heads, h=bh%heads;
    int offset=((b*sequence+q)*heads+h)*32+threadIdx.x;
    float value=__fadd_rn(0.0f,__fmul_rn(output[offset],grad_output[offset]));
    for(int shift=1;shift<32;shift*=2)value=__fadd_rn(value,__shfl_down_sync(0xffffffff,value,shift));
    if(threadIdx.x==0)delta[row]=value;
}
extern "C" __global__ void boundary_backward_metadata(unsigned int* out,int batch,int sequence,int heads) {
    BoundaryBackward::Params p;
    p.head_dim=32; p.head_dim_value=32; p.num_queries=sequence; p.num_keys=sequence;
    p.num_batches=batch; p.num_heads=heads; p.num_splits_key=1;
    out[0]=sizeof(BoundaryBackward::SharedStorage);
    out[1]=p.workspace_size();
}
extern "C" __global__ __launch_bounds__(128) void boundary_backward(float* grad_packed,const float* output,const float* grad_output,
    const float* lse,float* delta,const float* packed,const float* bias,float* workspace,
    int batch,int sequence,int heads,int bias_columns) {
    BoundaryBackward::Params p;
    p.query_ptr=packed; p.key_ptr=packed+heads*32; p.value_ptr=packed+heads*64;
    p.bias_ptr=bias; p.output_ptr=output; p.grad_output_ptr=grad_output;
    p.logsumexp_ptr=lse; p.delta_ptr=delta; p.workspace=workspace;
    p.grad_query_ptr=grad_packed; p.grad_key_ptr=grad_packed+heads*32; p.grad_value_ptr=grad_packed+heads*64;
    p.scale=0.17677669529663687f; p.head_dim=p.head_dim_value=32;
    p.num_queries=p.num_keys=sequence; p.num_batches=batch; p.num_heads=heads; p.num_splits_key=1;
    p.q_strideM=p.k_strideM=p.v_strideM=3*heads*32;
    p.q_strideH=p.k_strideH=p.v_strideH=32;
    p.q_strideB=p.k_strideB=p.v_strideB=(long long)sequence*3*heads*32;
    p.bias_strideM=bias_columns; p.bias_strideH=0; p.bias_strideB=sequence*bias_columns;
    p.o_strideB=p.gO_strideB=(long long)sequence*heads*32;
    p.o_strideH=p.gO_strideH=32; p.gO_strideM=heads*32;
    p.lse_strideH=(sequence+31)/32*32; p.lse_strideB=heads*p.lse_strideH;
    p.delta_strideH=sequence; p.delta_strideB=heads*sequence;
    p.gQKV_strideM_multiplier=3;
    p.gQ_strideB=p.gK_strideB=p.gV_strideB=(long long)sequence*3*heads*32;
    p.gQ_strideH=p.gK_strideH=p.gV_strideH=32;
    if(p.advance_to_block()) BoundaryBackward::attention_kernel(p);
}

static_assert(sizeof(BoundaryBackward::SharedStorage) == 53504, "backward ABI changed");
static_assert(sizeof(BoundaryBackward::GradQTempStorage) == 16400, "workspace ABI changed");
static_assert(BoundaryBackward::kNeedsAccumGradQ && !BoundaryBackward::kNeedsAccumGradK && !BoundaryBackward::kNeedsAccumGradV, "workspace formula changed");
