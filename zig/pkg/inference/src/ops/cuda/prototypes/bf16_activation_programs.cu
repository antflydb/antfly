// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <math.h>
#include "../kernels/row_program.cuh"
__device__ __forceinline__ unsigned short lighton_activation_bf16(float x) {
    unsigned bits=__float_as_uint(x);
    return (unsigned short)((bits+0x7fffu+((bits>>16)&1u))>>16);
}
extern "C" __global__ void lighton_silu_up_bf16(
    unsigned short* out,const float* gate,const float* up,unsigned count) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i>=count)return;
    const float g=gate[i];
    const float value=(g/(1.f+expf(-g)))*up[i];
    out[i]=lighton_activation_bf16(value);
}
extern "C" __global__ void lighton_packed_silu_up_bf16(
    unsigned short* out,const float* joined,unsigned rows,unsigned dim) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i>=rows*dim)return;
    const unsigned row=i/dim,col=i%dim;
    const float g=joined[row*(2*dim)+col];
    const float value=(g/(1.f+expf(-g)))*joined[row*(2*dim)+dim+col];
    out[i]=lighton_activation_bf16(value);
}

extern "C" __global__ void antfly_rms_tree_bf16(
    unsigned short* out, const float* input, const float* raw_weight,
    unsigned rows, unsigned dim, float eps, float weight_offset) {
    unsigned row=blockIdx.x;if(row>=rows)return;
    unsigned tid=threadIdx.x,base=row*dim;
    __shared__ float partial[256];
    const float scale=antfly_row_rms_tree_inverse(dim,eps,partial,[&](unsigned i){return input[base+i];});
    for(unsigned i=tid;i<dim;i+=blockDim.x){
        const float w=weight_offset==0.f?raw_weight[i]:__fadd_rn(raw_weight[i],weight_offset);
        out[base+i]=lighton_activation_bf16(input[base+i]*scale*w);
    }
}

// Experimental BF16 projection policy: consume compact projection outputs
// directly rather than widening gate/up only to narrow their product again.
extern "C" __global__ void lighton_silu_up_bf16_inputs(
    unsigned short* out,const unsigned short* gate,const unsigned short* up,unsigned count) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i>=count)return;
    const float g=__uint_as_float((unsigned)gate[i]<<16);
    const float u=__uint_as_float((unsigned)up[i]<<16);
    out[i]=lighton_activation_bf16((g/(1.f+expf(-g)))*u);
}
extern "C" __global__ void lighton_bf16_residual_f32(
    float* out,const unsigned short* projected,const float* residual,unsigned count) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i>=count)return;
    out[i]=__uint_as_float((unsigned)projected[i]<<16)+residual[i];
}
