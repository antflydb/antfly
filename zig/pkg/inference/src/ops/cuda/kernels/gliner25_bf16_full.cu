// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Experimental full-BF16 graph boundaries, independent of the FP32 route.
#include <cuda_bf16.h>
#include <math.h>
#include <stdint.h>
__device__ __forceinline__ float rbf(float x) { return __bfloat162float(__float2bfloat16_rn(x)); }
template<bool bf16_dot> __device__ __forceinline__ float norm_input(const float* a,const float* b,const float* projection_bias,unsigned i,unsigned width,unsigned flags) {
 float x=bf16_dot?__bfloat162float(((const __nv_bfloat16*)a)[i]):a[i];
 if(bf16_dot)x=rbf(x+projection_bias[i%width]);
 return b ? ((flags&16384u)?rbf(x+b[i]):rbf(rbf(x)+rbf(b[i]))) : rbf(x);
}
__device__ __forceinline__ float warp_sum(float x) {
  for (unsigned d=16;d;d>>=1) x+=__shfl_xor_sync(0xffffffffu,x,d);
  return x;
}
extern "C" __global__ void antfly_gliner_bf16_full_round_v1(float* x,unsigned n) {
  unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i<n)x[i]=rbf(x[i]);
}
template<bool bf16_dot> __device__ __forceinline__ void norm_core(
 float* out,__nv_bfloat16* mirror,const float* a,const float* b,const float* projection_bias,const float* gamma,const float* beta,
 unsigned rows,unsigned width,float eps,unsigned flags) {
 unsigned row=blockIdx.x,tid=threadIdx.x;if(row>=rows)return;
 __shared__ float sums[256];float sum=0;
 for(unsigned d=tid;d<width;d+=blockDim.x){unsigned i=row*width+d;sum+=norm_input<bf16_dot>(a,b,projection_bias,i,width,flags);}
 sums[tid]=sum;__syncthreads();
 for(unsigned s=128;s;s>>=1){if(tid<s)sums[tid]+=sums[tid+s];__syncthreads();}
 const float mean=sums[0]/float(width);float square=0;
 for(unsigned d=tid;d<width;d+=blockDim.x){unsigned i=row*width+d;float x=(norm_input<bf16_dot>(a,b,projection_bias,i,width,flags))-mean;square+=x*x;}
 sums[tid]=square;__syncthreads();
 for(unsigned s=128;s;s>>=1){if(tid<s)sums[tid]+=sums[tid+s];__syncthreads();}
 float variance=sums[0]/float(width),m=mean,inv;
 if(flags&1u){
  if(!(flags&512u))m=rbf(m);
  if(!(flags&1024u))variance=rbf(variance);
  float v=variance+rbf(eps);if(!(flags&4096u))v=rbf(v);
  inv=rsqrtf(v);if(!(flags&2048u))inv=rbf(inv);
 }else inv=rsqrtf(variance+eps);
 for(unsigned d=tid;d<width;d+=blockDim.x){unsigned i=row*width+d;float x=norm_input<bf16_dot>(a,b,projection_bias,i,width,flags);
  float y;
  if(flags&1u){float c=x-m;if(!(flags&128u))c=rbf(c);y=c*inv;if(!(flags&256u))y=rbf(y);y*=gamma[d];if(!(flags&64u))y=rbf(y);}
  else y=(x-m)*inv*gamma[d];
  y=rbf(y+beta[d]);out[i]=y;if(mirror)mirror[i]=__float2bfloat16_rn(y);
 }
}
extern "C" __global__ void antfly_gliner_bf16_full_norm_v1(
 float* out,__nv_bfloat16* mirror,const float* a,const float* b,const float* gamma,const float* beta,
 unsigned rows,unsigned width,float eps,unsigned flags) {
 norm_core<false>(out,mirror,a,b,nullptr,gamma,beta,rows,width,eps,flags);
}
extern "C" __global__ void antfly_gliner_bf16_full_norm_dot_bias_v1(
 float* out,__nv_bfloat16* mirror,const __nv_bfloat16* dot,const float* bias,const float* residual,const float* gamma,const float* beta,
 unsigned rows,unsigned width,float eps,unsigned flags) {
 norm_core<true>(out,mirror,(const float*)dot,residual,bias,gamma,beta,rows,width,eps,flags);
}
extern "C" __global__ void antfly_gliner_bf16_full_scale_key_v1(
 __nv_bfloat16* out,const __nv_bfloat16* in,unsigned n,unsigned flags){
 unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i<n){float x=__bfloat162float(in[i]),scale=rbf(sqrtf(192.f));out[i]=__float2bfloat16_rn((flags&8u)?((flags&65536u)?x*rbf(1.f/scale):x/scale):x);}
}
__device__ __forceinline__ void fill_views(unsigned i, uint64_t* arrays,const __nv_bfloat16* q,const __nv_bfloat16* k,const __nv_bfloat16* v,
 const __nv_bfloat16* qr,const __nv_bfloat16* kr,float* output,__nv_bfloat16* cc,__nv_bfloat16* cp,
 __nv_bfloat16* pc,__nv_bfloat16* probabilities,const __nv_bfloat16* scaled,
 unsigned sequence,unsigned heads,unsigned relative,unsigned first,unsigned count){
 if(i>=count)return;
 unsigned score_pitch=(sequence+7u)&~7u,head=(first+i)%heads,document=(first+i)/heads;
 size_t offset=((size_t)document*sequence*heads+head)*64u,rel=(size_t)head*64u;
 arrays[i]=(uint64_t)(q+offset);arrays[count+i]=(uint64_t)(k+offset);arrays[2*count+i]=(uint64_t)(v+offset);
 arrays[3*count+i]=(uint64_t)(qr+rel);arrays[4*count+i]=(uint64_t)(kr+rel);
 arrays[5*count+i]=(uint64_t)(cc+(size_t)i*sequence*score_pitch);
 arrays[6*count+i]=(uint64_t)(cp+(size_t)i*sequence*relative);
 arrays[7*count+i]=(uint64_t)(pc+(size_t)i*score_pitch*relative);
 arrays[8*count+i]=(uint64_t)(output+offset);arrays[9*count+i]=(uint64_t)(probabilities+(size_t)i*sequence*score_pitch);
 arrays[10*count+i]=(uint64_t)(scaled+offset);
}
extern "C" __global__ void antfly_gliner_bf16_full_views_v1( uint64_t* arrays,const __nv_bfloat16* q,const __nv_bfloat16* k,const __nv_bfloat16* v,
 const __nv_bfloat16* qr,const __nv_bfloat16* kr,float* output,__nv_bfloat16* cc,__nv_bfloat16* cp,
 __nv_bfloat16* pc,__nv_bfloat16* probabilities,const __nv_bfloat16* scaled,
 unsigned sequence,unsigned heads,unsigned relative,unsigned first,unsigned count){
 fill_views(blockIdx.x*blockDim.x+threadIdx.x,arrays,q,k,v,qr,kr,output,cc,cp,pc,probabilities,scaled,sequence,heads,relative,first,count);
}
extern "C" __global__ void antfly_gliner_bf16_full_scale_views_v1( uint64_t* arrays,const __nv_bfloat16* q,const __nv_bfloat16* k,const __nv_bfloat16* v,
 const __nv_bfloat16* qr,const __nv_bfloat16* kr,float* output,__nv_bfloat16* cc,__nv_bfloat16* cp,
 __nv_bfloat16* pc,__nv_bfloat16* probabilities,__nv_bfloat16* scaled,
 unsigned sequence,unsigned heads,unsigned relative,unsigned first,unsigned count,unsigned tokens,unsigned flags){
 unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
 if(i<tokens){float x=__bfloat162float(k[i]),scale=rbf(sqrtf(192.f));scaled[i]=__float2bfloat16_rn((flags&8u)?((flags&65536u)?x*rbf(1.f/scale):x/scale):x);}
 fill_views(i,arrays,q,k,v,qr,kr,output,cc,cp,pc,probabilities,scaled,sequence,heads,relative,first,count);
}
extern "C" __global__ void antfly_gliner_bf16_full_softmax_v1(
 __nv_bfloat16* probabilities,const __nv_bfloat16* scores,const __nv_bfloat16* cp,const __nv_bfloat16* pc,
 const void* mask_data,const int* buckets,unsigned sequence,unsigned relative,unsigned heads,unsigned first,
 unsigned boundary,unsigned model_heads,unsigned pitch,unsigned flags){
 unsigned lane=threadIdx.x&31u,row=blockIdx.x*8u+(threadIdx.x>>5u);if(row>=heads*sequence)return;
 unsigned qi=row%sequence,h=row/sequence,doc=(first+h)/model_heads,score_pitch=(sequence+7u)&~7u;
 bool validq=boundary?((const int*)mask_data)[doc*sequence+qi]!=0:((const long long*)mask_data)[doc*sequence+qi]!=0;
 float values[16],maximum=-INFINITY,scale=rbf(sqrtf(192.0f)),inv=(flags&65536u)?rbf(1.f/scale):1.f/scale;
 #pragma unroll
 for(unsigned j=0;j<16;++j){unsigned ki=lane+j*32u;float x=-INFINITY;
  if(ki<sequence){bool validk=boundary?((const int*)mask_data)[doc*sequence+ki]!=0:((const long long*)mask_data)[doc*sequence+ki]!=0;
   if(validq&&validk){unsigned off=qi+sequence-1u-ki;int bucket=buckets?buckets[off]:int(off);
    if(bucket<0||unsigned(bucket)>=relative)x=NAN;
    else{float cc=__bfloat162float(scores[row*score_pitch+ki]),c=__bfloat162float(cp[row*pitch+bucket]),p=__bfloat162float(pc[(h*pitch+bucket)*score_pitch+ki]);
     if(flags&16u){float relsum=rbf(rbf(c*inv)+rbf(p*inv));x=rbf(((flags&8u)?cc:rbf(cc*inv))+relsum);}
     else x=((flags&8u)?cc:cc*inv)+(c+p)*inv;
     if(flags&262144u)x=rbf(x);
    }
   }else x=-3.3895313892515355e38f;
  }values[j]=x;maximum=fmaxf(maximum,x);
 }
 for(unsigned d=16;d;d>>=1)maximum=fmaxf(maximum,__shfl_xor_sync(0xffffffffu,maximum,d));
 float sum=0;
 #pragma unroll
 for(unsigned j=0;j<16;++j){float diff=values[j]-maximum;values[j]=(values[j]==-INFINITY)?0:((flags&2u)?rbf(expf((flags&524288u)?diff:rbf(diff))):expf(diff));sum+=values[j];}
 sum=warp_sum(sum);if(flags&2u)sum=rbf(sum);
 #pragma unroll
 for(unsigned j=0;j<16;++j){unsigned ki=lane+j*32u;if(ki<sequence)probabilities[row*score_pitch+ki]=__float2bfloat16_rn(values[j]/sum);}
}
extern "C" __global__ void antfly_gliner_bf16_full_gelu_v1(
 __nv_bfloat16* out,const __nv_bfloat16* in,const float* bias,unsigned n,unsigned width,unsigned exact,unsigned flags){
 unsigned i=blockIdx.x*blockDim.x+threadIdx.x;if(i>=n)return;
 bool biased=(flags&2097152u)!=0;
 float x=((flags&131072u)&&!biased)?((const float*)in)[i]:__bfloat162float(in[i]);
 if(!biased)x+=bias[i%width];
 float y;if(!(flags&8192u))x=rbf(x);
 if(flags&32u){if(exact)y=rbf(rbf(.5f*x)*rbf(erfcf(rbf(-x*rbf(0.7071067811865476f)))));
  else{float cube=rbf(rbf(x*x)*x),s=rbf(x+rbf(rbf(.044715f)*cube));float t=rbf(tanhf(rbf(rbf(.7978845608028654f)*s)));y=rbf(x*rbf(.5f*rbf(1.f+t)));}}
 else y=exact?((flags&1048576u)?.5f*x*(1.f+erff(x*.7071067811865476f)):.5f*x*erfcf(-x*rbf(.7071067811865476f))):.5f*x*(1.f+tanhf(.7978845608028654f*(x+.044715f*x*x*x)));
 out[i]=__float2bfloat16_rn(y);
}
