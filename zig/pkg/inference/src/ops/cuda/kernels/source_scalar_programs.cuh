// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cuda_bf16.h>
#include <math.h>
__device__ __forceinline__ float antfly_source_bf16_value(float x) {
    return __bfloat162float(__float2bfloat16_rn(x));
}
// flags bit0 selects column-major output, bit1 repeats the input row.
__device__ __forceinline__ void antfly_source_pack_bf16(
    __nv_bfloat16* output,const void* input,unsigned rows,unsigned columns,
    unsigned input_bf16,unsigned flags) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if(i>=(unsigned long long)rows*columns) return;
    unsigned row=i/columns,column=i%columns,source=(flags&2u)?column:i;
    __nv_bfloat16 value=input_bf16 ? ((const __nv_bfloat16*)input)[source] : __float2bfloat16_rn(((const float*)input)[source]);
    output[(flags&1u)?(size_t)column*rows+row:i]=value;
}
__device__ __forceinline__ void antfly_source_widen_bf16(float* output,const __nv_bfloat16* input,unsigned count) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if(i<count) output[i]=__bfloat162float(input[i]);
}
__device__ __forceinline__ void antfly_source_softplus_bf16_values_f32(float* output,const float* input,unsigned count) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if(i<count) {
        float value=antfly_source_bf16_value(input[i]);
        output[i]=antfly_source_bf16_value(value>20.f ? value : log1pf(expf(value)));
    }
}
struct AntflySourceMoments { float avg, m2, n; };
__device__ __forceinline__ AntflySourceMoments antfly_source_add_moment(AntflySourceMoments s,float value) {
 float count=s.n+1.0f, delta=value-s.avg;
 float mean=s.avg+delta*(1.0f/count);
 return {mean,s.m2+delta*(value-mean),count};
}
__device__ __forceinline__ AntflySourceMoments antfly_source_merge_moments(AntflySourceMoments own,AntflySourceMoments other) {
 float count=other.n+own.n;
 if(count==0.0f) return {0.0f,0.0f,0.0f};
 float reciprocal=1.0f/count;
 float other_weight=other.n*reciprocal, own_weight=own.n*reciprocal;
 float delta=own.avg-other.avg;
 return {other_weight*other.avg+own_weight*own.avg,
         other.m2+own.m2+delta*delta*other.n*own_weight,count};
}
__device__ __forceinline__ void antfly_layer_norm_source_bf16_values_f32(
 float *output,const float *input,const float *gamma,
 unsigned rows,unsigned width,float epsilon,unsigned column_major,unsigned relu) {
 unsigned row=blockIdx.x,lane=threadIdx.x,warp=threadIdx.y;
 if(row>=rows) return;
 AntflySourceMoments value={0.f,0.f,0.f};
 for(unsigned v=warp*32+lane;v<width/4;v+=128) {
  unsigned base=row*width+v*4;
  #pragma unroll
  for(unsigned j=0;j<4;j++) {
    unsigned col=v*4+j;
    size_t at=column_major ? (size_t)col*rows+row : base+j;
    value=antfly_source_add_moment(value,antfly_source_bf16_value(input[at]));
  }
 }
 for(int distance=16;distance;distance>>=1) {
  AntflySourceMoments other={__shfl_down_sync(0xffffffffu,value.avg,distance),__shfl_down_sync(0xffffffffu,value.m2,distance),__shfl_down_sync(0xffffffffu,value.n,distance)};
  value=antfly_source_merge_moments(value,other);
 }
 __shared__ AntflySourceMoments exchange[4];
 __shared__ float mean,variance;
 for(unsigned distance=2;distance;distance>>=1) {
  if(lane==0 && warp>=distance && warp<2*distance) exchange[warp-distance]=value;
  __syncthreads();
  if(lane==0 && warp<distance) value=antfly_source_merge_moments(value,exchange[warp]);
  __syncthreads();
 }
 if(lane==0 && warp==0) {mean=value.avg;variance=value.m2/(float)width;}
 __syncthreads();
 float inverse=rsqrtf(variance+epsilon);
 for(unsigned v=warp*32+lane;v<width/4;v+=128) {
  unsigned base=row*width+v*4;
  #pragma unroll
  for(unsigned j=0;j<4;j++) {
   unsigned column=v*4+j;
   size_t at=column_major ? (size_t)column*rows+row : base+j;
   float x=antfly_source_bf16_value(input[at]);
   float result=antfly_source_bf16_value((x-mean)*inverse*antfly_source_bf16_value(gamma[column]));
   output[at]=(relu && result<0.f) ? 0.f : result;
  }
 }
}

// Channel-major source reduction. Each x lane owns V output rows; y
// walks channels in four independent planes. A multi-CTA plan stores only
// folded partial sums, then combines cta[y], cta[y+block.y], ... in stage 2.
// Splitting the launch rather than a last-CTA semaphore preserves that exact
// arithmetic order while keeping all invocation state in admitted buffers.
template<unsigned V>
__device__ __forceinline__ void antfly_source_channel_fold(float (&sum)[V],float* shared) {
    unsigned x=threadIdx.x,y=threadIdx.y,index=(y*blockDim.x+x)*V;
    #pragma unroll
    for(unsigned v=0;v<V;v++) shared[index+v]=sum[v];
    for(unsigned distance=blockDim.y/2;distance;distance>>=1) {
        __syncthreads();
        if(y<distance) {
            #pragma unroll
            for(unsigned v=0;v<V;v++) {
                sum[v]=__fadd_rn(sum[v],shared[index+distance*blockDim.x*V+v]);
                shared[index+v]=sum[v];
            }
        }
    }
    __syncthreads();
}
template<unsigned V>
__device__ __forceinline__ void antfly_source_channel_publish(float* output,
    const float* input,const float* gamma,unsigned rows,unsigned width,float epsilon,
    const float* shared) {
    unsigned x=threadIdx.x,y=threadIdx.y,first=(blockIdx.x*blockDim.x+x)*V;
    #pragma unroll
    for(unsigned v=0;v<V;v++) if(first+v<rows) {
        float mean=__fmul_rn(shared[x*V+v],__fdiv_rn(1.f,(float)width));
        float inverse=rsqrtf(__fadd_rn(mean,epsilon));
        for(unsigned col=y;col<width;col+=blockDim.y) {
            size_t at=(size_t)(first+v)*width+col;
            float result=__fmul_rn(antfly_source_bf16_value(input[at]),inverse);
            if(gamma) result=__fmul_rn(result,antfly_source_bf16_value(gamma[col]));
            output[at]=antfly_source_bf16_value(result);
        }
    }
}
template<unsigned V>
__device__ __forceinline__ void antfly_source_channel_rms(float* output,
    const float* input,const float* gamma,float* partial,unsigned rows,unsigned width,
    float epsilon,unsigned ctas,float* shared) {
    unsigned x=threadIdx.x,y=threadIdx.y,first=(blockIdx.x*blockDim.x+x)*V;
    unsigned step=blockDim.y*ctas;
    float a[4][V];
    #pragma unroll
    for(unsigned u=0;u<4;u++) for(unsigned v=0;v<V;v++) a[u][v]=0.f;
    for(unsigned base=y+blockIdx.y*blockDim.y;base<width;base+=4*step) {
        #pragma unroll
        for(unsigned u=0;u<4;u++) {
            unsigned col=base+u*step;
            #pragma unroll
            for(unsigned v=0;v<V;v++) if(col<width && first+v<rows) {
                float value=antfly_source_bf16_value(input[(size_t)(first+v)*width+col]);
                a[u][v]=__fadd_rn(a[u][v],__fmul_rn(value,value));
            }
        }
    }
    float sum[V];
    #pragma unroll
    for(unsigned v=0;v<V;v++) sum[v]=__fadd_rn(__fadd_rn(__fadd_rn(a[0][v],a[1][v]),a[2][v]),a[3][v]);
    antfly_source_channel_fold<V>(sum,shared);
    if(ctas==1) antfly_source_channel_publish<V>(output,input,gamma,rows,width,epsilon,shared);
    else if(y==0) {
        #pragma unroll
        for(unsigned v=0;v<V;v++) if(first+v<rows)
            partial[(size_t)(first+v)*ctas+blockIdx.y]=sum[v];
    }
}
__device__ __forceinline__ void antfly_rms_channel_source_bf16_values_f32(
    float* output,const float* input,const float* gamma,float* partial,unsigned rows,
    unsigned width,float epsilon,unsigned output_vector,unsigned ctas) {
    __shared__ float shared[512];
    if(output_vector==4) antfly_source_channel_rms<4>(output,input,gamma,partial,rows,width,epsilon,ctas,shared);
    else if(output_vector==2) antfly_source_channel_rms<2>(output,input,gamma,partial,rows,width,epsilon,ctas,shared);
    else antfly_source_channel_rms<1>(output,input,gamma,partial,rows,width,epsilon,ctas,shared);
}
template<unsigned V>
__device__ __forceinline__ void antfly_source_channel_finish(float* output,
    const float* input,const float* gamma,const float* partial,unsigned rows,unsigned width,
    float epsilon,unsigned ctas,float* shared) {
    unsigned x=threadIdx.x,y=threadIdx.y,first=(blockIdx.x*blockDim.x+x)*V;
    float sum[V];
    #pragma unroll
    for(unsigned v=0;v<V;v++) {
        sum[v]=0.f;
        if(first+v<rows) for(unsigned cta=y;cta<ctas;cta+=blockDim.y)
            sum[v]=__fadd_rn(sum[v],partial[(size_t)(first+v)*ctas+cta]);
    }
    antfly_source_channel_fold<V>(sum,shared);
    antfly_source_channel_publish<V>(output,input,gamma,rows,width,epsilon,shared);
}
__device__ __forceinline__ void antfly_rms_channel_source_finish_bf16_values_f32(
    float* output,const float* input,const float* gamma,const float* partial,unsigned rows,
    unsigned width,float epsilon,unsigned output_vector,unsigned ctas) {
    __shared__ float shared[512];
    if(output_vector==4) antfly_source_channel_finish<4>(output,input,gamma,partial,rows,width,epsilon,ctas,shared);
    else if(output_vector==2) antfly_source_channel_finish<2>(output,input,gamma,partial,rows,width,epsilon,ctas,shared);
    else antfly_source_channel_finish<1>(output,input,gamma,partial,rows,width,epsilon,ctas,shared);
}

__device__ __forceinline__ void antfly_source_sinusoidal_bf16_values_f32(
    float* output,const float* frequencies,unsigned positions,unsigned components,unsigned reverse,unsigned bf16_angle) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if(i>=(unsigned long long)positions*components) return;
    unsigned row=i/components,column=i%components;
    float position=(float)(reverse ? positions-1-row : row);
    float frequency=bf16_angle ? antfly_source_bf16_value(frequencies[column]) : frequencies[column];
    float angle=__fmul_rn(position,frequency);
    if(bf16_angle) angle=antfly_source_bf16_value(angle);
    output[(size_t)row*components*2+column]=antfly_source_bf16_value(sinf(angle));
    output[(size_t)row*components*2+components+column]=antfly_source_bf16_value(cosf(angle));
}

__device__ __forceinline__ void antfly_source_clamp_bf16_values_f32(
    float* output,const float* input,unsigned count,unsigned flags,float low,float high) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if(i<count) {
        float value=antfly_source_bf16_value(input[i]);
        float lo=antfly_source_bf16_value(low),hi=antfly_source_bf16_value(high);
        if((flags&1u) && value<lo) value=lo;
        if((flags&2u) && value>hi) value=hi;
        output[i]=antfly_source_bf16_value(value);
    }
}
