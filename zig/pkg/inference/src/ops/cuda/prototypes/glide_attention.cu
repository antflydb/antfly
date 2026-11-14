// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Packed bidirectional GQA. One warp owns a query/head; online softmax
// keeps scratch independent of context length. Each lane owns eight channels.
struct GlideAttentionParams {
    const float *q, *k, *v;
    float *out;
    const unsigned *ranges;
    unsigned queries, heads, kv_heads;
    float scale;
};
extern "C" __global__ void glide_attention_f32(GlideAttentionParams p) {
    const unsigned lane = threadIdx.x & 31;
    const unsigned row = (blockIdx.x * blockDim.x + threadIdx.x) / 32;
    if (row >= p.queries * p.heads) return;
    const unsigned qi = row / p.heads, h = row % p.heads;
    const unsigned kh = h / (p.heads / p.kv_heads);
    float q[8], acc[8] = {};
    #pragma unroll
    for (int d = 0; d < 8; ++d) q[d] = p.q[row * 256 + d * 32 + lane];
    float maximum = -__int_as_float(0x7f800000), denominator = 0;
    for (unsigned segment = 0; segment < 3; ++segment) {
        const unsigned begin = p.ranges[qi * 6 + segment * 2];
        const unsigned end = p.ranges[qi * 6 + segment * 2 + 1];
        for (unsigned ki = begin; ki < end; ++ki) {
            const unsigned offset = (ki * p.kv_heads + kh) * 256 + lane;
            float score = 0;
            #pragma unroll
            for (int d = 0; d < 8; ++d) score = fmaf(q[d], p.k[offset + d * 32], score);
            #pragma unroll
            for (int step = 16; step; step >>= 1) score += __shfl_xor_sync(0xffffffff, score, step);
            score *= p.scale;
            const float next = fmaxf(maximum, score);
            const float correction = expf(maximum - next), weight = expf(score - next);
            denominator = denominator * correction + weight;
            #pragma unroll
            for (int d = 0; d < 8; ++d) acc[d] = fmaf(weight, p.v[offset + d * 32], acc[d] * correction);
            maximum = next;
        }
    }
    #pragma unroll
    for (int d = 0; d < 8; ++d) p.out[row * 256 + d * 32 + lane] = denominator > 0 ? acc[d] / denominator : 0;
}

#include <cuda_fp16.h>
#include <mma.h>
using namespace nvcuda;
// Tensor-core candidate: F16 operands, F32 online softmax and accumulation.
// A block owns 16 queries of one head. Query tiles may cross row boundaries;
// visibility is tested independently for every query, including tail tiles.
extern "C" __global__ void glide_attention_f16(GlideAttentionParams p) {
    const unsigned tid = threadIdx.x, warp = tid / 32, lane = tid % 32;
    const unsigned first = blockIdx.x * 16, head = blockIdx.y;
    const unsigned kh = head / (p.heads / p.kv_heads);
    __shared__ __align__(32) half q[16*256], k[16*256], v[16*256];
    __shared__ __align__(32) float out[16*256], score[16*16];
    __shared__ __align__(32) half prob[16*16];
    __shared__ float maximum[16], denominator[16], alpha[16];
    __shared__ unsigned begin, end;
    if (tid == 0) {
        begin = 0xffffffff; end = 0;
        for (unsigned qi = first; qi < first + 16 && qi < p.queries; ++qi)
            for (unsigned s = 0; s < 3; ++s) {
                unsigned a=p.ranges[qi*6+2*s], b=p.ranges[qi*6+2*s+1];
                if (a < b) { begin=min(begin,a); end=max(end,b); }
            }
    }
    for (unsigned i=tid; i<16*256; i+=128) {
        unsigned qi=first+i/256, d=i%256;
        q[i]=__float2half_rn(qi<p.queries ? p.q[(qi*p.heads+head)*256+d] : 0.f);
        out[i]=0;
    }
    if (tid<16) { maximum[tid]=-3.402823466e38f; denominator[tid]=0; }
    __syncthreads();
    for (unsigned start=begin; start<end; start+=16) {
        for (unsigned i=tid; i<16*256; i+=128) {
            unsigned ki=start+i/256, d=i%256;
            k[i]=__float2half_rn(ki<end ? p.k[(ki*p.kv_heads+kh)*256+d] : 0.f);
            v[i]=__float2half_rn(ki<end ? p.v[(ki*p.kv_heads+kh)*256+d] : 0.f);
        }
        __syncthreads();
        if (warp==0) {
            wmma::fragment<wmma::accumulator,16,16,16,float> acc;
            wmma::fill_fragment(acc,0.f);
            #pragma unroll
            for (unsigned d=0; d<256; d+=16) {
                wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::row_major> a;
                wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::col_major> b;
                wmma::load_matrix_sync(a,q+d,256);
                wmma::load_matrix_sync(b,k+d,256);
                wmma::mma_sync(acc,a,b,acc);
            }
            wmma::store_matrix_sync(score,acc,16,wmma::mem_row_major);
        }
        __syncthreads();
        for (unsigned group=0;group<2;++group) {
            unsigned row=warp*4+group*2+lane/16, col=lane%16, qi=first+row, ki=start+col;
            bool valid=false;
            if (qi<p.queries) for (unsigned s=0;s<3;++s)
                valid |= ki>=p.ranges[qi*6+2*s] && ki<p.ranges[qi*6+2*s+1];
            float x=valid ? score[row*16+col]*p.scale : -3.402823466e38f;
            float top=x;
            #pragma unroll
            for (unsigned step=8;step;step>>=1) top=fmaxf(top,__shfl_xor_sync(0xffffffff,top,step,16));
            float next=fmaxf(maximum[row],top);
            float correction=denominator[row]>0 ? expf(maximum[row]-next) : 0.f;
            float weight=valid ? expf(x-next) : 0.f;
            float sum=weight;
            #pragma unroll
            for (unsigned step=8;step;step>>=1) sum+=__shfl_xor_sync(0xffffffff,sum,step,16);
            prob[row*16+col]=__float2half_rn(weight);
            if (col==0) {
                maximum[row]=next;
                denominator[row]=denominator[row]*correction+sum;
                alpha[row]=correction;
            }
        }
        __syncthreads();
        for (unsigned i=tid;i<16*256;i+=128) out[i]*=alpha[i/256];
        __syncthreads();
        #pragma unroll
        for (unsigned d=warp*16;d<256;d+=64) {
            wmma::fragment<wmma::accumulator,16,16,16,float> acc;
            wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::row_major> a;
            wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::row_major> b;
            wmma::load_matrix_sync(acc,out+d,256,wmma::mem_row_major);
            wmma::load_matrix_sync(a,prob,16);
            wmma::load_matrix_sync(b,v+d,256);
            wmma::mma_sync(acc,a,b,acc);
            wmma::store_matrix_sync(out+d,acc,256,wmma::mem_row_major);
        }
        __syncthreads();
    }
    for (unsigned i=tid;i<16*256;i+=128) {
        unsigned qi=first+i/256,d=i%256;
        if (qi<p.queries) p.out[(qi*p.heads+head)*256+d]=denominator[i/256]>0 ? out[i]/denominator[i/256] : 0.f;
    }
}

struct GlideDenseParams {
    const float *q, *k, *v;
    float *out;
    half *qp, *kp, *vp;
    float *scores;
    half *probabilities;
    float *op;
    unsigned query_start, key_start, queries, keys, heads, kv_heads;
    float scale;
    unsigned padded_keys;
};
// Transpose V in shared memory so both input reads and packed writes
// are coalesced. The extra column avoids transpose bank conflicts.
extern "C" __global__ void glide_pack_kv(GlideDenseParams p) {
    unsigned tile=blockIdx.x, chunks=(p.padded_keys+31)/32;
    unsigned head=tile/(chunks*8), first_key=(tile/8%chunks)*32, first_dim=(tile%8)*32;
    if(head>=p.kv_heads) return;
    unsigned x=threadIdx.x%32,y=threadIdx.x/32;
    __shared__ half values[32][33];
    #pragma unroll
    for(unsigned j=0;j<32;j+=8) {
        unsigned token=first_key+y+j,dim=first_dim+x;
        unsigned source=((p.key_start+token)*p.kv_heads+head)*256+dim;
        half key=__float2half_rn(token<p.keys ? p.k[source] : 0.f);
        values[y+j][x]=__float2half_rn(token<p.keys ? p.v[source] : 0.f);
        if(token<p.padded_keys) p.kp[(head*p.padded_keys+token)*256+dim]=key;
    }
    __syncthreads();
    #pragma unroll
    for(unsigned j=0;j<32;j+=8) {
        unsigned token=first_key+x,dim=first_dim+y+j;
        if(token<p.padded_keys) p.vp[(head*256+dim)*p.padded_keys+token]=values[x][y+j];
    }
}
extern "C" __global__ void glide_pack_q(GlideDenseParams p) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if (i>=p.heads*p.queries*256) return;
    unsigned d=i%256, token=(i/256)%p.queries, h=i/(256*p.queries);
    p.qp[i]=__float2half_rn(p.q[((p.query_start+token)*p.heads+h)*256+d]);
}
extern "C" __global__ void glide_unpack_o(GlideDenseParams p) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if (i>=p.heads*p.queries*256) return;
    unsigned d=i%256, token=(i/256)%p.queries, h=i/(256*p.queries);
    p.out[((p.query_start+token)*p.heads+h)*256+d]=p.op[i];
}
extern "C" __global__ void glide_softmax(GlideDenseParams p) {
    unsigned row=blockIdx.x, tid=threadIdx.x;
    __shared__ float reduced[256];
    float maximum=-3.402823466e38f;
    for (unsigned col=tid;col<p.keys;col+=256) maximum=fmaxf(maximum,p.scores[row*p.padded_keys+col]*p.scale);
    reduced[tid]=maximum;
    __syncthreads();
    for (unsigned step=128;step;step>>=1) { if(tid<step) reduced[tid]=fmaxf(reduced[tid],reduced[tid+step]); __syncthreads(); }
    maximum=reduced[0];
    __syncthreads();
    float sum=0;
    for (unsigned col=tid;col<p.keys;col+=256) sum+=expf(p.scores[row*p.padded_keys+col]*p.scale-maximum);
    reduced[tid]=sum;
    __syncthreads();
    for (unsigned step=128;step;step>>=1) { if(tid<step) reduced[tid]+=reduced[tid+step]; __syncthreads(); }
    sum=reduced[0];
    for (unsigned col=tid;col<p.padded_keys;col+=256)
        p.probabilities[row*p.padded_keys+col]=col<p.keys ? __float2half_rn(expf(p.scores[row*p.padded_keys+col]*p.scale-maximum)/sum) : __float2half_rn(0.f);
}
