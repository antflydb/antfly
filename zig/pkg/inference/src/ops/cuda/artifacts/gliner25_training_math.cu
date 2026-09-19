// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0 AND BSD-3-Clause
/*
Activation and reduction arithmetic adapted from PyTorch, under the following license:

From PyTorch:

Copyright (c) 2016-     Facebook, Inc            (Adam Paszke)
Copyright (c) 2014-     Facebook, Inc            (Soumith Chintala)
Copyright (c) 2011-2014 Idiap Research Institute (Ronan Collobert)
Copyright (c) 2012-2014 Deepmind Technologies    (Koray Kavukcuoglu)
Copyright (c) 2011-2012 NEC Laboratories America (Koray Kavukcuoglu)
Copyright (c) 2011-2013 NYU                      (Clement Farabet)
Copyright (c) 2006-2010 NEC Laboratories America (Ronan Collobert, Leon Bottou, Iain Melvin, Jason Weston)
Copyright (c) 2006      Idiap Research Institute (Samy Bengio)
Copyright (c) 2001-2004 Idiap Research Institute (Ronan Collobert, Samy Bengio, Johnny Mariethoz)

From Caffe2:

Copyright (c) 2016-present, Facebook Inc. All rights reserved.

All contributions by Facebook:
Copyright (c) 2016 Facebook Inc.

All contributions by Google:
Copyright (c) 2015 Google Inc.
All rights reserved.

All contributions by Yangqing Jia:
Copyright (c) 2015 Yangqing Jia
All rights reserved.

All contributions by Kakao Brain:
Copyright 2019-2020 Kakao Brain

All contributions by Cruise LLC:
Copyright (c) 2022 Cruise LLC.
All rights reserved.

All contributions by Tri Dao:
Copyright (c) 2024 Tri Dao.
All rights reserved.

All contributions by Arm:
Copyright (c) 2021, 2023-2024 Arm Limited and/or its affiliates

All contributions from Caffe:
Copyright(c) 2013, 2014, 2015, the respective contributors
All rights reserved.

All other contributions:
Copyright(c) 2015, 2016 the respective contributors
All rights reserved.

Caffe2 uses a copyright model similar to Caffe: each contributor holds
copyright over their contributions to Caffe2. The project versioning records
all such contribution and copyright details. If a contributor wants to further
mark their specific copyright on a particular contribution, they should
indicate their copyright solely in the commit message of the change when it is
committed.

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

3. Neither the names of Facebook, Deepmind Technologies, NYU, NEC Laboratories America
   and IDIAP Research Institute nor the names of its contributors may be
   used to endorse or promote products derived from this software without
   specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

// Isolated CUDA 12.8 math profile for PyTorch 2.9.1+cu128 training parity.
// Build with scripts/regen-cuda-training-math.py, not the CUDA 13.2 bundle.
// NVRTC supplies CUDA builtins; no runtime or PyTorch headers are required.
// Reference: aten/src/ATen/native/cuda/ActivationGeluKernel.cu (v2.9.1).
extern "C" __global__ void termite_gliner25_gelu_f32_cuda128(
    float* output, const float* input, const float* upstream,
    unsigned count, unsigned backward) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const float x = input[i];
    constexpr float alpha = 0.70710678118654752440;
    constexpr float beta = 1.12837916709551257390 * 0.70710678118654752440 * 0.5f;
    if (backward) {
        const float cdf = 0.5f * (1.0f + ::erf(x * alpha));
        const float pdf = ::exp(-0.5f * x * x) * beta;
        output[i] = upstream[i] * (cdf + x * pdf);
    } else {
        output[i] = x * 0.5f * (1.0f + ::erf(x * alpha));
    }
}

// Reference: aten/src/ATen/native/cuda/ActivationSiluKernel.cu (v2.9.1).
// Direct division and the fused derivative are intentional arithmetic choices.
extern "C" __global__ void termite_gliner25_silu_f32_cuda128(
    float* output, const float* input, const float* upstream,
    unsigned count, unsigned backward) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const float x = input[i];
    if (backward) {
        const float s = 1.0f / (1.0f + ::exp(-x));
        output[i] = upstream[i] * s * (1.0f + x * (1.0f - s));
    } else {
        output[i] = x / (1.0f + ::exp(-x));
    }
}

// PyTorch v2.9.1 UnarySpecialOpsKernel.cu and BinaryMiscBackwardOpsKernels.cu.
// Saving the rounded output is essential at saturation. Explicit rounding
// preserves the reference's (upstream * (1 - output)) * output order.
extern "C" __global__ void termite_gliner25_sigmoid_f32_cuda128(
    float* output, const float* input, const float* upstream,
    unsigned count, unsigned backward) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const float x = input[i];
    output[i] = backward
        ? __fmul_rn(__fmul_rn(upstream[i], __fsub_rn(1.0f, x)), x)
        : __fdiv_rn(1.0f, __fadd_rn(1.0f, expf(-x)));
}

// Prefix scans: PyTorch v2.9.1 ScanUtils.cuh and deterministic ATen/cuda/cub.cuh.
// Detached GLiNER span geometry, evaluated with CUDA 12.8 scalar math rather
// than host libm. Counts are shared across each sample's candidate capacity.
extern "C" __global__ void termite_gliner25_frozen_span_features_v1(
    float* out, const float* lengths, const float* counts, unsigned rows, unsigned capacity) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= rows) return;
    const float length = fmaxf(lengths[i], 1.0f);
    const float count = fmaxf(counts[i / capacity], 1.0f);
    out[3 * i] = log1pf(length);
    out[3 * i + 1] = length / count;
    out[3 * i + 2] = rsqrtf(length);
}

extern "C" __global__ void termite_gliner25_scan_outer_v1(float* out, const float* x, unsigned batch, unsigned width, unsigned dim, unsigned reverse) {
 unsigned channel=blockIdx.x*blockDim.x+threadIdx.x;
 if(channel>=batch*dim)return;
 unsigned base=(channel/dim)*width*dim+channel%dim;
 float acc=0.0f;
 for(unsigned step=0;step<width;++step){unsigned col=reverse?width-1-step:step;unsigned i=base+col*dim;acc=acc+x[i];out[i]=acc;}
}
extern "C" __global__ void termite_gliner25_scan_inner_v1(float* out, const float* x, unsigned rows, unsigned width, unsigned log_x, unsigned reverse) {
 __shared__ float shared[1024];
 unsigned tx=threadIdx.x, nt=1u<<log_x;
 float* buf=shared+2*nt*threadIdx.y;
 unsigned row=blockIdx.x*blockDim.y+threadIdx.y;
 bool exists=row<rows;
 float total=0.0f;
 for(unsigned start=0;start<width;start+=2*nt){
  unsigned c1=start+tx,c2=start+nt+tx;
  if(exists){
   buf[tx]=c1<width?x[row*width+(reverse?width-1-c1:c1)]:0.0f;
   buf[nt+tx]=c2<width?x[row*width+(reverse?width-1-c2:c2)]:0.0f;
   if(tx==0)buf[0]=buf[0]+total;
  }
  __syncthreads();
  for(unsigned m=0;m<=log_x;++m){
   if(exists){unsigned s=1u<<m;unsigned a=((tx>>m)<<(m+1))|s;buf[a+tx%s]=buf[a+tx%s]+buf[a-1];}
   __syncthreads();
  }
  if(exists){
   if(c1<width)out[row*width+(reverse?width-1-c1:c1)]=buf[tx];
   if(c2<width)out[row*width+(reverse?width-1-c2:c2)]=buf[nt+tx];
  }
  total=exists?buf[2*nt-1]:0.0f;
  __syncthreads();
 }
}

/*
Copyright (c) 2011, Duane Merrill. All rights reserved.
Copyright (c) 2011-2018, NVIDIA CORPORATION. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

// FP32 specialization of PyTorch's deterministic single-vector scan ordering.
// 512 threads, 16 items per thread. No CUB or host runtime dependencies.
__device__ float scan_block_reduce(float value, float* warps) {
    unsigned lane=threadIdx.x%32, warp=threadIdx.x/32;
    for(unsigned offset=1;offset<32;offset*=2){
        float other=__shfl_down_sync(0xffffffff,value,offset);
        if(lane+offset<32)value=value+other;
    }
    if(lane==0)warps[warp]=value;
    __syncthreads();
    float sum=warps[0];
    for(unsigned w=1;w<16;++w)sum=sum+warps[w];
    __syncthreads();
    return sum;
}
extern "C" __global__ void termite_gliner25_scan_vector_sums_v1(float* sums,const float* input,unsigned count,unsigned iterations,unsigned reverse){
    __shared__ float warps[16];
    unsigned start=blockIdx.x*8192u*iterations;
    float total=0.0f;
    for(unsigned tile=0;tile<iterations && start<count;++tile,start+=8192){
        float local=0.0f;
        for(unsigned j=0;j<16;++j){
            unsigned index=start+threadIdx.x+j*512;
            float value=index<count?input[reverse?count-1-index:index]:0.0f;
            local=j==0?value:local+value;
        }
        float sum=scan_block_reduce(local,warps);
        total=total+sum;
    }
    if(threadIdx.x==0)sums[blockIdx.x]=total;
}
extern "C" __global__ void termite_gliner25_scan_vector_v1(float* output,const float* input,const float* sums,unsigned count,unsigned iterations,unsigned reverse){
    __shared__ float warps[16];
    unsigned tid=threadIdx.x,lane=tid%32,warp=tid/32;
    float aggregate=tid<blockIdx.x?sums[tid]:0.0f;
    if(tid+512<blockIdx.x)aggregate=aggregate+sums[tid+512];
    float prefix=scan_block_reduce(aggregate,warps);
    unsigned start=blockIdx.x*8192u*iterations;
    for(unsigned tile=0;tile<iterations && start<count;++tile,start+=8192){
        float data[16];
        for(unsigned j=0;j<16;++j){unsigned i=start+tid*16+j;data[j]=i<count?input[reverse?count-1-i:i]:0.0f;}
        float local=data[0];
        for(unsigned j=1;j<16;++j)local=local+data[j];
        float inclusive=local;
        for(unsigned offset=1;offset<32;offset*=2){float other=__shfl_up_sync(0xffffffff,inclusive,offset);if(lane>=offset)inclusive=other+inclusive;}
        float exclusive=__shfl_up_sync(0xffffffff,inclusive,1);
        if(lane==31)warps[warp]=inclusive;
        __syncthreads();
        float block_sum=warps[0],warp_prefix=0.0f;
        for(unsigned w=1;w<16;++w){if(warp==w)warp_prefix=block_sum;block_sum=block_sum+warps[w];}
        if(warp>0)exclusive=lane==0?warp_prefix:warp_prefix+exclusive;
        exclusive=tid==0?prefix:prefix+exclusive;
        float running=exclusive;
        for(unsigned j=0;j<16;++j){running=running+data[j];unsigned i=start+tid*16+j;if(i<count)output[reverse?count-1-i:i]=running;}
        prefix=prefix+block_sum;
        __syncthreads();
    }
}

// FP32 specialization of PyTorch 2.9.1 Reduce.cuh (5811a8d7da87).
// Dense row-major inputs; launch geometry comes from reduction_plan.zig.
// A second launch replaces the reference semaphore while retaining CTA order.
struct Gliner25ReduceConfig {
 unsigned rank, mask, dims[8], inputs, outputs;
 unsigned step_input, step_output, input_x, input_y, input_cta, output_x, output_y;
 unsigned vector_input, vector_output, ctas, red_contiguous, red_stride;
 float factor;
};
__device__ unsigned gliner25_reduce_input_offset(Gliner25ReduceConfig c, unsigned output, unsigned reduced) {
 unsigned offset=0, stride=1;
 for(int d=c.rank-1;d>=0;--d){
  unsigned coord;
  if(c.mask&(1u<<d)){coord=reduced%c.dims[d];reduced/=c.dims[d];}
  else {coord=output%c.dims[d];output/=c.dims[d];}
  offset+=coord*stride;stride*=c.dims[d];
 }
 return offset;
}
__device__ void gliner25_reduce_block(float (&v)[4], float* shared, Gliner25ReduceConfig c) {
 unsigned t=threadIdx.x+threadIdx.y*blockDim.x, vec=c.vector_output;
 if(c.input_y){
  for(unsigned j=0;j<vec;++j)shared[t*vec+j]=v[j];
  for(unsigned offset=blockDim.y/2;offset;offset>>=1){
   __syncthreads();
   if(threadIdx.y<offset){
    for(unsigned j=0;j<vec;++j){v[j]+=shared[(t+offset*blockDim.x)*vec+j];shared[t*vec+j]=v[j];}
   }
  }
 }
 __syncthreads();
 if(c.input_x){
  unsigned width=blockDim.x;
  if(width>32){
   for(unsigned j=0;j<vec;++j)shared[t*vec+j]=v[j];
   for(unsigned offset=width/2;offset>=32;offset>>=1){
    __syncthreads();
    if(threadIdx.x<offset){for(unsigned j=0;j<vec;++j){v[j]+=shared[(t+offset)*vec+j];shared[t*vec+j]=v[j];}}
   }
   width=32;
  }
  __syncthreads();
  for(unsigned offset=1;offset<width;offset<<=1)
   for(unsigned j=0;j<vec;++j)v[j]+=__shfl_down_sync(__activemask(),v[j],offset);
 }
}
__device__ unsigned gliner25_reduce_staging_offset(Gliner25ReduceConfig c,unsigned tile){
 unsigned offset=tile+blockIdx.x*c.ctas;
 if(!c.input_x)offset=threadIdx.x+offset*blockDim.x;
 return offset*c.vector_output;
}
extern "C" __global__ void termite_gliner25_reduce_part_v1(float* output,const float* input,float* staging,Gliner25ReduceConfig c){
 __shared__ float shared[512];
 unsigned oi=(threadIdx.x*c.output_x+threadIdx.y*c.output_y+blockIdx.x*c.step_output)*c.vector_output;
 unsigned idx=threadIdx.x*c.input_x+threadIdx.y*c.input_y+blockIdx.y*c.input_cta;
 float v[4]={0,0,0,0};
 if(oi<c.outputs && idx<c.inputs){
  unsigned base=gliner25_reduce_input_offset(c,oi,0);
  float a[4][4]={{0}};
  if(c.vector_input){
   const float* data=input+base;
   unsigned end=c.inputs,shift=base%4;
   bool tail=(!c.input_y||threadIdx.y==0)&&(!c.input_cta||blockIdx.y==0);
   if(shift){
    data-=shift;end+=shift;
    if(threadIdx.x>=shift&&threadIdx.x<4&&tail)a[0][0]+=data[threadIdx.x];
    end-=4;data+=4;
   }
   for(unsigned i=idx;i*4+3<end;i+=c.step_input)
    for(unsigned k=0;k<4;++k)a[k][0]+=data[i*4+k];
   unsigned ti=end-end%4+threadIdx.x;
   if(tail&&ti<end)a[0][0]+=data[ti];
  }else{
   for(unsigned i=idx;i<c.inputs;i+=4*c.step_input)
    for(unsigned k=0;k<4;++k){
     unsigned ri=i+k*c.step_input;
     if(ri<c.inputs){
      unsigned offset=c.red_contiguous?base+ri*c.red_stride:gliner25_reduce_input_offset(c,oi,ri);
      for(unsigned j=0;j<c.vector_output;++j)a[k][j]+=input[offset+j];
     }
    }
  }
  for(unsigned j=0;j<c.vector_output;++j){v[j]=a[0][j];for(unsigned k=1;k<4;++k)v[j]+=a[k][j];}
 }
 gliner25_reduce_block(v,shared,c);
 if(oi<c.outputs&&(!c.input_x||threadIdx.x==0)&&(!c.input_y||threadIdx.y==0)){
  if(c.ctas>1){unsigned offset=gliner25_reduce_staging_offset(c,blockIdx.y);for(unsigned j=0;j<c.vector_output;++j)staging[offset+j]=v[j];}
  else for(unsigned j=0;j<c.vector_output;++j)output[oi+j]=v[j]*c.factor;
 }
}
extern "C" __global__ void termite_gliner25_reduce_finish_v1(float* output,const float* staging,Gliner25ReduceConfig c){
 __shared__ float shared[512];
 unsigned oi=(threadIdx.x*c.output_x+threadIdx.y*c.output_y+blockIdx.x*c.step_output)*c.vector_output;
 float v[4]={0,0,0,0};
 unsigned i=c.input_x?threadIdx.x+threadIdx.y*blockDim.x:threadIdx.y;
 unsigned step=c.input_x?blockDim.x*blockDim.y:blockDim.y;
 if(oi<c.outputs)for(;i<c.ctas;i+=step){unsigned offset=gliner25_reduce_staging_offset(c,i);for(unsigned j=0;j<c.vector_output;++j)v[j]+=staging[offset+j];}
 gliner25_reduce_block(v,shared,c);
 if(oi<c.outputs&&(!c.input_x||threadIdx.x==0)&&(!c.input_y||threadIdx.y==0))
  for(unsigned j=0;j<c.vector_output;++j)output[oi+j]=v[j]*c.factor;
}

// Tensor.gather backward: PyTorch v2.9.1 cuda/Indexing.cu stride-one order.
// Native integer grouping avoids scalar-index sorting for every feature.
// Advanced row indexing retains its separate serial reduction.
extern "C" __global__ void termite_gliner25_scatter_gather_v1(
    float* out, const float* values, const int* rows, const int* offsets,
    const int* order, unsigned groups, unsigned width) {
    const unsigned element = blockIdx.x * (blockDim.x / 32) + threadIdx.x / 32;
    if (element >= groups * width) return;
    const unsigned lane = threadIdx.x % 32;
    const unsigned group = element / width;
    const unsigned column = element % width;
    const int lo = offsets[group], count = offsets[group + 1] - lo;
    const int whole = count / 32 * 32;
    float sum = 0.0f;
    for (int i = lane; i < whole; i += 32) sum += values[order[lo + i] * width + column];
    for (int delta = 16; delta; delta /= 2) sum += __shfl_down_sync(0xffffffffu, sum, delta);
    if (lane == 0) {
        for (int i = whole; i < count; ++i) sum += values[order[lo + i] * width + column];
        out[rows[group] * width + column] += sum;
    }
}

// Embedding backward follows the pinned small-input and large-input partial
// reduction orders, sharing stable integer groups with other row scatters.
extern "C" __global__ void termite_gliner25_scatter_embedding_v1(float* out,const float* values,const int* rows,const int* offsets,const int* order,unsigned groups,unsigned width,unsigned value_rows,int padding_index) {
 unsigned j=blockIdx.x*blockDim.x+threadIdx.x;if(j>=groups*width)return;
 unsigned g=j/width,c=j%width;if(rows[g]==padding_index)return;int lo=offsets[g],hi=offsets[g+1];float sum=0;
 if(value_rows<=3072){
  for(int i=lo;i<hi;){int chunk=order[i]/32;float partial=values[(unsigned)order[i]*width+c];++i;
   while(i<hi&&order[i]/32==chunk){partial=__fadd_rn(partial,values[(unsigned)order[i]*width+c]);++i;}
   sum=__fadd_rn(sum,partial);
  }
 }else{
  for(int i=lo;i<hi;){float partial=0;int end=i+10<hi?i+10:hi;
   while(i<end){partial=__fadd_rn(partial,values[(unsigned)order[i]*width+c]);++i;}
   sum=__fadd_rn(sum,partial);
  }
 }
 out[(unsigned)rows[g]*width+c]=sum;
}

// Binary-loss VJP arithmetic and branch accumulation follow pinned PyTorch
// 2.9.1 autograd. Explicit rounding prevents cross-operation FMA contraction.
__device__ float gliner25_loss_mul(float a, float b) { return __fmul_rn(a,b); }
__device__ float gliner25_loss_add(float a, float b) { return __fadd_rn(a,b); }
__device__ float gliner25_loss_sub(float a, float b) { return __fsub_rn(a,b); }
__device__ float gliner25_loss_divide(float a, float b) { return __fdiv_rn(a,b); }
__device__ float gliner25_loss_power(float x,float exponent) {
 if(exponent==0) return 1;
 if(exponent==1) return x;
 if(exponent==2) return gliner25_loss_mul(x,x);
 if(exponent==3) return gliner25_loss_mul(gliner25_loss_mul(x,x),x);
 if(exponent==.5f) return sqrtf(x);
 if(exponent==-.5f) return rsqrtf(x);
 if(exponent==-1) return gliner25_loss_divide(1,x);
 if(exponent==-2) return gliner25_loss_divide(1,gliner25_loss_mul(x,x));
 return powf(x,exponent);
}
extern "C" __global__ void termite_gliner25_elementwise_vjp_v1(float* out,const float* logits,const float* targets,const float* seeds,unsigned count,unsigned kind,float gp,float gn,float clip,float negative_weight,float gp_backward,float gn_backward) {
 unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i>=count)return;
 float seed=seeds[i];if(seed==0){out[i]=0;return;}
 float x=logits[i],y=targets[i];
 // Poisson NLL (log_input=true, full=false): preserve the separate exp and
 // target-product adjoints. seed*(exp(x)-target) rounds differently.
 if(kind==2){out[i]=gliner25_loss_add(gliner25_loss_mul(seed,expf(x)),gliner25_loss_mul(-seed,y));return;}
 float p=gliner25_loss_divide(1,gliner25_loss_add(1,expf(-x)));
 if(kind==0) {out[i]=gliner25_loss_mul(gliner25_loss_sub(p,y),gliner25_loss_mul(seed,y>.5f?1:negative_weight));return;}
 float positive=gliner25_loss_sub(1,p),negative_raw=clip>0?gliner25_loss_add(positive,clip):positive;
 float negative=clip>0?fminf(negative_raw,1):negative_raw;
 float pos_clamped=fmaxf(p,1e-8f),neg_clamped=fmaxf(negative,1e-8f);
 float lp=logf(pos_clamped),ln=logf(neg_clamped);
 float pp=gliner25_loss_power(positive,gp),pn=gliner25_loss_power(p,gn),g=-seed,ny=gliner25_loss_sub(1,y);
 float a=p>=1e-8f?gliner25_loss_divide(gliner25_loss_mul(gliner25_loss_mul(g,pp),y),pos_clamped):0;
 float b=gp==0?-0.0f:-gliner25_loss_mul(gliner25_loss_mul(g,gliner25_loss_mul(y,lp)),gliner25_loss_mul(gp,gliner25_loss_power(positive,gp_backward)));
 float ng=gliner25_loss_mul(g,negative_weight);
 float c=negative>=1e-8f?gliner25_loss_divide(gliner25_loss_mul(gliner25_loss_mul(ng,pn),ny),neg_clamped):0;
 if(clip>0&&negative_raw>1)c=0;
 c=-c;
 float d=gn==0?0:gliner25_loss_mul(gliner25_loss_mul(ng,gliner25_loss_mul(ny,ln)),gliner25_loss_mul(gn,gliner25_loss_power(p,gn_backward)));
 float dp=gliner25_loss_add(gliner25_loss_add(gliner25_loss_add(b,d),a),c);
 out[i]=gliner25_loss_mul(gliner25_loss_mul(dp,positive),p);
}

// CUDA listwise preparation in canonical [B,Q,C] storage. Maxima and row
// cotangents are supplied by the shared masking/reduction implementation.
extern "C" __global__ void termite_gliner25_listwise_prepare_v1(
 float* all_exp, float* gold_exp, const float* x, const unsigned* masks,
 const float* maxima, const float* seeds, unsigned rows, unsigned cols,
 unsigned queries, unsigned candidate_major) {
 unsigned j=blockIdx.x*blockDim.x+threadIdx.x;
 if(j>=rows*cols)return;
 unsigned row=j/cols, col=j%cols;
 if(seeds[row]==0){all_exp[j]=gold_exp[j]=0;return;}
 unsigned i=candidate_major?((row/queries)*cols+col)*queries+row%queries:j;
 unsigned mask=masks[i];
 float value=(mask&1)?x[i]:-1e4f;
 all_exp[j]=expf(__fsub_rn(value,maxima[row]));
 gold_exp[j]=expf(__fsub_rn((mask&2)?value:-1e4f,maxima[rows+row]));
}
extern "C" __global__ void termite_gliner25_listwise_vjp_v1(
 float* out, const float* x, const unsigned* masks, const float* maxima,
 const float* seeds, const float* all_sum, const float* gold_sum,
 unsigned rows, unsigned cols, unsigned queries, unsigned candidate_major,
 unsigned canonical_output) {
 unsigned j=blockIdx.x*blockDim.x+threadIdx.x;
 if(j>=rows*cols)return;
 unsigned row=j/cols,col=j%cols;
 unsigned i=candidate_major?((row/queries)*cols+col)*queries+row%queries:j;
 unsigned target=canonical_output?j:i;
 unsigned mask=masks[i];float seed=seeds[row];
 if(seed==0 || !(mask&1)){out[target]=0;return;}
 float all_lse=__fadd_rn(logf(all_sum[row]),maxima[row]);
 float gold_lse=__fadd_rn(logf(gold_sum[row]),maxima[rows+row]);
 float negative=(mask&2)?__fmul_rn(-seed,expf(__fsub_rn(x[i],gold_lse))):0;
 out[target]=__fadd_rn(negative,__fmul_rn(seed,expf(__fsub_rn(x[i],all_lse))));
}

#include "../kernels/gliner25_softmax.cuh"

extern "C" __global__ void termite_gliner25_record_target_exp_v1(
    float* terms, const float* logp, const int* masks,
    const int* target_columns, unsigned n, unsigned width
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    const unsigned row = i / width;
    const int column = target_columns[row];
    if (column < 0) { terms[i] = 0; return; }
    const float selected = logp[row * width + column];
    const float maximum = selected < -10000.0f ? -10000.0f : selected;
    const float value = masks[i] & 2 ? logp[i] : -10000.0f;
    terms[i] = expf(__fsub_rn(value, maximum));
}
extern "C" __global__ void termite_gliner25_record_logp_vjp_v1(
    float* grad, float* losses, const float* logp, const int* masks,
    const int* target_columns, const float* sums, const float* seeds,
    unsigned n, unsigned width
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    const unsigned row = i / width;
    const int column = target_columns[row];
    if (column < 0) { grad[i] = 0; if (i % width == 0) losses[row] = 0; return; }
    const float selected = logp[row * width + column];
    const float maximum = selected < -10000.0f ? -10000.0f : selected;
    const float log_mass = __fadd_rn(maximum, logf(sums[row]));
    if (i % width == 0) losses[row] = -log_mass;
    grad[i] = masks[i] & 2 ? __fmul_rn(expf(__fsub_rn(logp[i], log_mass)), -seeds[row]) : 0;
}
extern "C" __global__ void termite_gliner25_record_mask_vjp_v1(
    float* grad, const int* masks, const float* seeds, unsigned n, unsigned width
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n && (!(masks[i] & 1) || seeds[i / width] == 0)) grad[i] = 0;
}

// Pinned ATen fused AdamW: FP32 state, FP64 configuration, explicit moment FMAs.
extern "C" __global__ void termite_gliner25_adamw_pytorch_v1(float* w,const float* g,float* m,float* v,unsigned n,
 double lr,double beta1,double beta2,double eps,double decay,unsigned step) {
 __shared__ float corrections[2];
 if(threadIdx.x==0){corrections[0]=1-pow(beta1,(double)(float)step);corrections[1]=sqrt(1-pow(beta2,(double)(float)step));}
 __syncthreads();
 unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i>=n)return;
 float bias1=corrections[0],bias2sqrt=corrections[1];
 float param=w[i],grad=g[i],mean=m[i],variance=v[i];
 if(decay!=0)param-=lr*decay*param;
 mean=__fma_rn(beta1,(double)mean,__dmul_rn(1-beta1,(double)grad));
 variance=__fma_rn(beta2,(double)variance,__dmul_rn(__dmul_rn(1-beta2,(double)grad),(double)grad));
 float step_size=lr/bias1;
 float denom=(sqrtf(variance)/bias2sqrt)+eps;
 param-=step_size*mean/denom;
 w[i]=param;m[i]=mean;v[i]=variance;
}

// Noisy-OR consistency VJP, pinned Fastino boundary/losses.py and PyTorch
// FP32 autograd operation order. Integer reaches/grouping remain shared host
// metadata; log-survival sums reuse scatter_gather_v1 above.
extern "C" __global__ void termite_gliner25_consistency_prepare_v1(
    float* probabilities, float* log_survival, const float* logits,
    const int* valid, unsigned count) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    if (!valid[i]) { probabilities[i] = 0.0f; log_survival[i] = -0.0f; return; }
    const float p = gliner25_loss_divide(1.0f, gliner25_loss_add(1.0f, expf(-logits[i])));
    probabilities[i] = p;
    log_survival[i] = log1pf(-fminf(p, 1.0f - 1e-6f));
}
extern "C" __global__ void termite_gliner25_consistency_boundary_v1(
    float* survival_gradient, float* marginal_gradient, const float* sums,
    const float* margins, const int* keep, unsigned count,
    unsigned kept_count, float weight) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    if (!keep[i]) { survival_gradient[i] = 0.0f; marginal_gradient[i] = 0.0f; return; }
    const float target = gliner25_loss_divide(1.0f, gliner25_loss_add(1.0f, expf(-margins[i])));
    const float survived = expf(sums[i]);
    const float difference = gliner25_loss_sub(gliner25_loss_sub(1.0f, survived), target);
    const float seed = gliner25_loss_divide(gliner25_loss_mul(weight, 0.5f), float(kept_count ? kept_count : 1));
    const float dy = gliner25_loss_mul(seed, gliner25_loss_mul(2.0f, difference));
    marginal_gradient[i] = gliner25_loss_mul(gliner25_loss_mul(-dy, gliner25_loss_sub(1.0f, target)), target);
    survival_gradient[i] = gliner25_loss_mul(-dy, survived);
}
extern "C" __global__ void termite_gliner25_consistency_pair_v1(
    float* output, const float* probabilities, const float* start_gradient,
    const float* end_gradient, const int* starts, const int* ends,
    const int* valid, unsigned count) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    if (!valid[i]) { output[i] = 0.0f; return; }
    const float p = probabilities[i];
    float dy = gliner25_loss_add(start_gradient[starts[i]], end_gradient[ends[i]]);
    dy = -gliner25_loss_divide(dy, gliner25_loss_sub(1.0f, fminf(p, 1.0f - 1e-6f)));
    if (p > 1.0f - 1e-6f) dy = 0.0f;
    output[i] = gliner25_loss_mul(gliner25_loss_mul(dy, gliner25_loss_sub(1.0f, p)), p);
}

// FP32 norm profile following PyTorch 2.9.1 ForeachReduceOp.cu,
// MultiTensorApply.cuh, block_reduce.cuh, and Reduce.cuh.
// 65536-element chunks and 512-thread reductions are arithmetic contracts.
// The final reduction accepts 1..16384 ordered per-tensor norms.
__device__ float gliner25_norm_warp(float x) {
 for(unsigned d=16;d;d>>=1)x=__fadd_rn(x,__shfl_down_sync(0xffffffff,x,d));
 return x;
}
__device__ float gliner25_norm_block(float x,float* shared){
 const unsigned t=threadIdx.x;
 x=gliner25_norm_warp(x);__syncthreads();
 if(t%32==0)shared[t/32]=x;
 __syncthreads();
 x=t<16?shared[t]:0;
 if(t<32)x=gliner25_norm_warp(x);
 return x;
}
extern "C" __global__ void termite_gliner25_norm_chunks_v1(float* out,const float* input,unsigned n){
 __shared__ float shared[16];
 unsigned base=blockIdx.x*65536u,left=n-base;
 const float* x=input+base;
 float a[4]={0,0,0,0};
 if(left%4==0 && ((unsigned long long)x)%16==0){
  for(unsigned i=threadIdx.x;i*4<left&&i*4<65536;i+=512)
   for(unsigned k=0;k<4;++k){float v=x[i*4+k];a[k]=__fmaf_rn(v,v,a[k]);}
 }else{
  for(unsigned i=0;i<left&&i<65536;i+=2048)
   for(unsigned k=0;k<4;++k){unsigned j=i+threadIdx.x+k*512;if(j<left&&j<65536){float v=x[j];a[k]=__fmaf_rn(v,v,a[k]);}}
 }
 float v=0;for(unsigned k=0;k<4;++k)v=__fadd_rn(v,a[k]);
 v=gliner25_norm_block(v,shared);
 if(threadIdx.x==0)out[blockIdx.x]=v;
}
extern "C" __global__ void termite_gliner25_norm_finish_v1(float* out,const float* partial,unsigned n){
 __shared__ float shared[16];float v=0;
 for(unsigned i=threadIdx.x;i<n;i+=512)v=__fadd_rn(v,partial[i]);
 v=gliner25_norm_block(v,shared);if(threadIdx.x==0)out[0]=__fsqrt_rn(v);
}
extern "C" __global__ void termite_gliner25_norm_total_v1(float* out,const float* input,unsigned n){
 __shared__ float shared[512];
 float a[4]={0,0,0,0};unsigned t=threadIdx.x,step=blockDim.x;
 if(n>128){
  for(unsigned i=t;i*4+3<n;i+=step)
   for(unsigned k=0;k<4;++k){float x=input[i*4+k];a[k]=__fmaf_rn(x,x,a[k]);}
  unsigned tail=n-n%4+t;if(tail<n){float x=input[tail];a[0]=__fmaf_rn(x,x,a[0]);}
 }else{
  for(unsigned i=t;i<n;i+=4*step)
   for(unsigned k=0;k<4;++k){unsigned j=i+k*step;if(j<n){float x=input[j];a[k]=__fmaf_rn(x,x,a[k]);}}
 }
 float v[4]={a[0],0,0,0};for(unsigned k=1;k<4;++k)v[0]=__fadd_rn(v[0],a[k]);
 Gliner25ReduceConfig c={};c.input_x=1;c.vector_output=1;
 gliner25_reduce_block(v,shared,c);
 if(t==0)out[0]=__fsqrt_rn(v[0]);
}
