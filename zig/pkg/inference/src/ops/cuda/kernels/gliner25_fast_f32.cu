// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// SM89-only accurate Tensor Core projections. Parameters, activation state,
// bias and readback remain FP32. No global scratch or weight mirrors are used.
#include "cutlass/gemm/device/gemm.h"
#include "gliner25_fast_f32_mma.cuh"
namespace gliner_tc {
template<int TM,int TN,int WM,int WN,class Op>
using Gemm=cutlass::gemm::device::Gemm<float,cutlass::layout::RowMajor,float,cutlass::layout::ColumnMajor,float,cutlass::layout::RowMajor,float,cutlass::arch::OpClassTensorOp,cutlass::arch::Sm80,cutlass::gemm::GemmShape<TM,TN,16>,cutlass::gemm::GemmShape<WM,WN,16>,cutlass::gemm::GemmShape<16,8,8>,cutlass::epilogue::thread::LinearCombination<float,4,float,float>,cutlass::gemm::threadblock::GemmIdentityThreadblockSwizzle<>,3,4,4,false,Op>;
using TS=Gemm<32,32,16,16,cutlass::arch::OpMultiplyAddFastF32>;
using BS=Gemm<32,32,16,16,cutlass::arch::OpGlinerCompensatedBF16>;
using BM=Gemm<32,64,16,32,cutlass::arch::OpGlinerCompensatedBF16>;
using BL=Gemm<64,64,32,32,cutlass::arch::OpGlinerCompensatedBF16>;
template<class G>__device__ void body(float*out,const float*input,const float*weight,const float*bias,unsigned rows,unsigned width,unsigned columns){
 using K=typename G::GemmKernel;
 __shared__ typename K::SharedStorage storage;
 typename K::Params params({(int)rows,(int)columns,(int)width},{(int)((rows+G::ThreadblockShape::kM-1)/G::ThreadblockShape::kM),(int)((columns+G::ThreadblockShape::kN-1)/G::ThreadblockShape::kN),1},
 {const_cast<float*>(input),typename G::LayoutA(width)},{const_cast<float*>(weight),typename G::LayoutB(width)},
 {const_cast<float*>(bias?bias:out),typename G::LayoutC(bias?0:columns)},{out,typename G::LayoutC(columns)}, {1.0f,bias?1.0f:0.0f});
 K kernel;kernel(params,storage);
}
}
#define GLINER_WRAP(name,type) extern "C" __global__ void name(float*out,const float*input,const float*weight,const float*bias,unsigned rows,unsigned width,unsigned columns){gliner_tc::body<gliner_tc::type>(out,input,weight,bias,rows,width,columns);}
GLINER_WRAP(antfly_gliner_tf32x3_small_v1,TS)
GLINER_WRAP(antfly_gliner_bf16x3_small_v1,BS)
GLINER_WRAP(antfly_gliner_bf16x3_medium_v1,BM)
GLINER_WRAP(antfly_gliner_bf16x3_large_v1,BL)
