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

#include <cuda_fp16.h>
#include <mma.h>
using namespace nvcuda;
struct GlideDeltaParams {
    const float *conv, *gates;
    half *w, *attention, *scaled_k;
    float *u, *decay;
    float *state, *out;
    unsigned tokens, heads;
};
// WY representation of 32 rank-one recurrent updates. Chunks and heads are
// independent here; the lower-triangular solve retains F32 arithmetic.
extern "C" __global__ void glide_delta_prepare(GlideDeltaParams p) {
    unsigned chunk=blockIdx.x, head=blockIdx.y, tid=threadIdx.x, warp=tid/32;
    unsigned first=chunk*32, length=min(32u,p.tokens-first), width=p.heads*128;
    unsigned base=(chunk*p.heads+head)*32;
    // The fully unrolled triangular solve retains previous rows in
    // registers; shared memory holds only the two tensor-core dot products.
    __shared__ __align__(32) half k[32*128],q[32*128];
    __shared__ __align__(32) float qk[32*32];
    __shared__ __align__(32) float kk[32*32];
    __shared__ float g[32], beta[32], cumulative[32];
    for(unsigned t=0;t<32;++t) {
        unsigned index=(first+t)*3*width+head*128+tid;
        k[t*128+tid]=__float2half_rn(t<length ? p.conv[index+width] : 0.f);
        q[t*128+tid]=__float2half_rn(t<length ? p.conv[index] : 0.f);
    }
    if(tid<32) {
        g[tid]=tid<length ? p.gates[(first+tid)*width+head*128] : 1.f;
        beta[tid]=tid<length ? p.gates[(first+tid)*width+head*128+1] : 0.f;
    }
    __syncthreads();
    if(tid<32) {
        float value=1;
        for(unsigned j=0;j<=tid;++j) value*=g[j];
        cumulative[tid]=value;
        p.decay[base+tid]=value;
    }
    unsigned r=(warp/2)*16,c=(warp%2)*16;
    wmma::fragment<wmma::accumulator,16,16,16,float> keys,queries;
    wmma::fill_fragment(keys,0.f);wmma::fill_fragment(queries,0.f);
    #pragma unroll
    for(unsigned d=0;d<128;d+=16) {
        wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::row_major> ka,qa;
        wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::col_major> kb;
        wmma::load_matrix_sync(ka,k+r*128+d,128);
        wmma::load_matrix_sync(qa,q+r*128+d,128);
        wmma::load_matrix_sync(kb,k+c*128+d,128);
        wmma::mma_sync(keys,ka,kb,keys);wmma::mma_sync(queries,qa,kb,queries);
    }
    wmma::store_matrix_sync(kk+r*32+c,keys,32,wmma::mem_row_major);
    wmma::store_matrix_sync(qk+r*32+c,queries,32,wmma::mem_row_major);
    __syncthreads();
    for(unsigned index=tid;index<32*32;index+=128) {
        unsigned t=index/32,i=index%32;
        float decay=1;
        for(unsigned j=i+1;j<=t;++j) decay*=g[j];
        kk[index]=(i<t ? kk[index]*beta[t]*decay : 0.f);
        p.attention[base*32+index]=__float2half_rn(i<=t ? qk[index]*decay : 0.f);
    }
    __syncthreads();
    float previous_w[32], previous_u[32];
    #pragma unroll
    for(unsigned t=0;t<32;++t) {
        unsigned index=(first+t)*3*width+head*128+tid;
        float w=t<length ? beta[t]*cumulative[t]*p.conv[index+width] : 0.f;
        float u=t<length ? beta[t]*p.conv[index+2*width] : 0.f;
        #pragma unroll
        for(unsigned i=0;i<t;++i) {
            w-=kk[t*32+i]*previous_w[i];
            u-=kk[t*32+i]*previous_u[i];
        }
        previous_w[t]=w;
        previous_u[t]=u;
        p.w[(base+t)*128+tid]=__float2half_rn(w);
        p.u[(base+t)*128+tid]=u;
        float decay=1;
        for(unsigned j=t+1;j<length;++j) decay*=g[j];
        p.scaled_k[(base+t)*128+tid]=__float2half_rn(t<length ? p.conv[index+width]*decay : 0.f);
    }
}

// Each block owns 32 value columns of one head. Tensor cores apply an entire
// chunk, preserving the recurrent state in F32 between chunks.
extern "C" __global__ void glide_delta_scan(GlideDeltaParams p) {
    unsigned head=blockIdx.y, column=blockIdx.x*32, tid=threadIdx.x,warp=tid/32;
    unsigned width=p.heads*128;
    __shared__ __align__(32) float state[128*32],delta[32*32],output[32*32];
    __shared__ __align__(32) half staged[128*32],dh[32*32],q[32*128];
    wmma::fragment<wmma::accumulator,16,16,16,float> history[2];
    #pragma unroll
    for(unsigned tile=0;tile<2;++tile) wmma::fill_fragment(history[tile],0.f);
    for(unsigned chunk=0;chunk<(p.tokens+31)/32;++chunk) {
        unsigned first=chunk*32,length=min(32u,p.tokens-first),base=(chunk*p.heads+head)*32;
        #pragma unroll
        for(unsigned j=0;j<2;++j) {
            unsigned tile=warp+j*8,sr=(tile/2)*16,sc=(tile%2)*16;
            wmma::store_matrix_sync(state+sr*32+sc,history[j],32,wmma::mem_row_major);
        }
        __syncthreads();
        for(unsigned i=tid;i<128*32;i+=256) {
            staged[i]=__float2half_rn(state[i]);
            unsigned t=i/128,d=i%128;
            q[i]=__float2half_rn(t<length ? p.conv[(first+t)*3*width+head*128+d] : 0.f);
        }
        __syncthreads();
        unsigned r=((warp%4)/2)*16,c=(warp%2)*16;
        wmma::fragment<wmma::accumulator,16,16,16,float> product;
        wmma::fill_fragment(product,0.f);
        #pragma unroll
        for(unsigned d=0;d<128;d+=16) {
            wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::row_major> a;
            wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::row_major> s;
            if(warp<4) wmma::load_matrix_sync(a,p.w+(base+r)*128+d,128);
            else wmma::load_matrix_sync(a,q+r*128+d,128);
            wmma::load_matrix_sync(s,staged+d*32+c,32);
            wmma::mma_sync(product,a,s,product);
        }
        wmma::store_matrix_sync((warp<4 ? delta : output)+r*32+c,product,32,wmma::mem_row_major);
        __syncthreads();
        for(unsigned i=tid;i<32*32;i+=256) {
            unsigned t=i/32,d=i%32;
            delta[i]=p.u[(base+t)*128+column+d]-delta[i];
            dh[i]=__float2half_rn(delta[i]);
            output[i]*=p.decay[base+t];
        }
        __syncthreads();
        if(warp<4) {
        wmma::fragment<wmma::accumulator,16,16,16,float> result;
        wmma::load_matrix_sync(result,output+r*32+c,32,wmma::mem_row_major);
        #pragma unroll
        for(unsigned d=0;d<32;d+=16) {
            wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::row_major> a;
            wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::row_major> b;
            wmma::load_matrix_sync(a,p.attention+base*32+r*32+d,32);
            wmma::load_matrix_sync(b,dh+d*32+c,32);
            wmma::mma_sync(result,a,b,result);
        }
        wmma::store_matrix_sync(output+r*32+c,result,32,wmma::mem_row_major);
        }
        __syncthreads();
        for(unsigned i=tid;i<32*32;i+=256) if(i/32<length)
            p.out[(first+i/32)*width+head*128+column+i%32]=output[i];
        float decay=p.decay[base+length-1];
        #pragma unroll
        for(unsigned j=0;j<2;++j) {
            unsigned tile=warp+j*8,sr=(tile/2)*16,sc=(tile%2)*16;
            #pragma unroll
            for(unsigned i=0;i<history[j].num_elements;++i) history[j].x[i]*=decay;
            #pragma unroll
            for(unsigned d=0;d<32;d+=16) {
                wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::col_major> k;
                wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::row_major> v;
                wmma::load_matrix_sync(k,p.scaled_k+(base+d)*128+sr,128);
                wmma::load_matrix_sync(v,dh+d*32+sc,32);
                wmma::mma_sync(history[j],k,v,history[j]);
            }

        }
        __syncthreads();
    }
    #pragma unroll
    for(unsigned j=0;j<2;++j) {
        unsigned tile=warp+j*8,sr=(tile/2)*16,sc=(tile%2)*16;
        wmma::store_matrix_sync(state+sr*32+sc,history[j],32,wmma::mem_row_major);
    }
    __syncthreads();
    for(unsigned i=tid;i<128*32;i+=256) p.state[(head*128+i/32)*128+column+i%32]=state[i];
}
