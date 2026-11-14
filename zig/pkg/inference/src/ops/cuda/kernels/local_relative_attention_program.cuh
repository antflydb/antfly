// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <math.h>
#include "row_program.cuh"

// One warp owns a query/head and a bounded causal relative-position window.
// The historical fused dot and independent F32 dot/softmax schedules share
// addressing and masks; publication/association remain explicit static policy.
enum class AntflyLocalRelativeArithmetic : unsigned { CombinedF32, IndependentF32 };
template<AntflyLocalRelativeArithmetic Arithmetic>
__device__ __forceinline__ void antfly_local_relative_attention_program(
    float* out,const float* q,const float* k,const float* v,const float* rel,
    const float* q_scales,const float* valid,unsigned rows,unsigned hidden,unsigned heads,
    unsigned head_dim,unsigned chunk,unsigned context_left,unsigned context,float k_scale,
    float logit_cap,float invalid_value,unsigned flags,float query_scale) {
    unsigned int lane = threadIdx.x;
    unsigned int query = blockIdx.x, head = blockIdx.y;
    if (query >= rows || head >= heads || lane >= 32u) return;
    size_t out_base = (size_t)query * hidden + (size_t)head * head_dim;
    if ((flags & 2u) ? valid[query] <= 0.0f : valid[query] == 0.0f) {
        for (unsigned int d = lane; d < head_dim; d += 32u) out[out_base + d] = 0.0f;
        return;
    }
    float scores[24];
    if constexpr (Arithmetic == AntflyLocalRelativeArithmetic::CombinedF32) {
        #pragma unroll
        for (unsigned int i = 0; i < 24; ++i) scores[i] = invalid_value;
    }
    // Source probabilities are distributed over warp lanes rather than a
    // dynamically indexed 24-element array in every thread.
    float lane_score = lane<context?invalid_value:-INFINITY;
    unsigned int clip_start = (flags & 2u) ? (unsigned int)valid[query] - 1u : 0u;
    unsigned int block_start = clip_start + ((query - clip_start) / chunk) * chunk;
    unsigned int q_off = query - block_start;
    unsigned int past = context_left - 1u;
    float max_score = -INFINITY;
    for (unsigned int c = 0; c < context; ++c) {
        int rel_idx = (int)c - (int)q_off;
        int key_idx = (int)block_start + (int)c - (int)past;
        // Official sliding mask uses distance < (context_left - 1), so a
        // distance exactly equal to `past` is excluded.
        if (rel_idx < 0 || rel_idx >= (int)context_left || key_idx < 0 ||
            key_idx >= (int)rows || ((flags & 2u) ? valid[key_idx] != valid[query] : valid[key_idx] == 0.0f) || key_idx > (int)query ||
            query - (unsigned int)key_idx > past - (flags & 1u)) continue;
        float score = 0.0f;
        float relative_score = 0.0f;
        size_t qb = (size_t)query * hidden + (size_t)head * head_dim;
        size_t kb = (size_t)key_idx * hidden + (size_t)head * head_dim;
        size_t rb = (size_t)rel_idx * hidden + (size_t)head * head_dim;
        for (unsigned int d = lane; d < head_dim; d += 32u) {
            if constexpr (Arithmetic == AntflyLocalRelativeArithmetic::IndependentF32) {
                // Source projections have already crossed their BF16 stores
                // before widening. These scaling and dot nodes remain F32.
                const float qs = __fmul_rn(__fmul_rn(q[qb+d],query_scale),q_scales[d]);
                score = __fmaf_rn(qs,__fmul_rn(k[kb+d],k_scale),score);
                relative_score = __fmaf_rn(qs,rel[rb+d],relative_score);
            } else {
                float qs = q[qb + d] * q_scales[d];
                score += qs * (k[kb + d] * k_scale + rel[rb + d]);
            }
        }
        score = antfly_row_warp_sum(score);
        score = __shfl_sync(0xffffffffu, score, 0);
        if constexpr (Arithmetic == AntflyLocalRelativeArithmetic::IndependentF32) {
            relative_score = antfly_row_warp_sum(relative_score);
            relative_score = __shfl_sync(0xffffffffu,relative_score,0);
            score = __fadd_rn(score,relative_score);
            score = __fmul_rn(tanhf(__fdiv_rn(score,logit_cap)),logit_cap);
        } else {
            score = tanhf(score / logit_cap) * logit_cap;
        }
        if constexpr (Arithmetic == AntflyLocalRelativeArithmetic::IndependentF32) {
            if(lane==c)lane_score=score;
        } else {
            scores[c] = score;
            max_score = fmaxf(max_score, score);
        }
    }
    if constexpr (Arithmetic == AntflyLocalRelativeArithmetic::IndependentF32) {
        float maximum=lane_score;
        for(int offset=16;offset>0;offset>>=1)maximum=fmaxf(maximum,__shfl_xor_sync(0xffffffffu,maximum,offset));
        float probability=lane<context?expf(__fsub_rn(lane_score,maximum)):0.f;
        float denominator=probability;
        for(int offset=16;offset>0;offset>>=1)denominator=__fadd_rn(denominator,__shfl_xor_sync(0xffffffffu,denominator,offset));
        probability=__fdiv_rn(probability,denominator);
        // Uniform iterations keep every lane in each shuffle, including when
        // the head width has a tail. Quotient/remainder avoids width+31 overflow.
        const unsigned tiles=head_dim/32+unsigned(head_dim%32!=0);
        for(unsigned tile=0;tile<tiles;++tile) {
            const unsigned d=tile*32+lane;
            float value=0.f;
            for(unsigned c=0;c<context;++c) {
                const float weight=__shfl_sync(0xffffffffu,probability,c);
                const int key_idx=(int)block_start+(int)c-(int)past;
                if(key_idx<0||key_idx>=(int)rows)continue;
                if((flags&2u)?valid[key_idx]!=valid[query]:valid[key_idx]==0.f)continue;
                if(key_idx>(int)query||query-(unsigned)key_idx>past-(flags&1u))continue;
                if(d<head_dim)value=__fmaf_rn(weight,v[(size_t)key_idx*hidden+(size_t)head*head_dim+d],value);
            }
            if(d<head_dim) {
                const float result=denominator>0.f?value:0.f;
                out[out_base+d]=(flags&4u)?__bfloat162float(__float2bfloat16_rn(result)):result;
            }
        }
    } else {
        float denominator = 0.0f;
        #pragma unroll
        for (unsigned int c = 0; c < 24; ++c) {
            if (c < context) {
                scores[c] = expf(scores[c] - max_score);
                denominator += scores[c];
            }
        }
        for (unsigned int d = lane; d < head_dim; d += 32u) {
            float value = 0.0f;
            for (unsigned int c = 0; c < context; ++c) {
                int key_idx = (int)block_start + (int)c - (int)past;
                if (key_idx < 0 || key_idx >= (int)rows) continue;
                if ((flags & 2u) ? valid[key_idx] != valid[query] : valid[key_idx] == 0.0f) continue;
                if (key_idx > (int)query || query - (unsigned int)key_idx > past - (flags & 1u)) continue;
                value += (scores[c] / denominator) * v[(size_t)key_idx * hidden + (size_t)head * head_dim + d];
            }
            out[out_base + d] = denominator > 0.0f ? value : 0.0f;
        }
    }
}

// tile-codegen:begin attention.local_relative_source family (do not edit; run: zig build tile-kernel-codegen -- --write)
// Caller-funded source F32 GEMMs share the existing local-relative owner.
__device__ __forceinline__ float antfly_local_relative_bf16_value(float x) {
    return __bfloat162float(__float2bfloat16_rn(x));
}
extern "C" __global__ void antfly_local_relative_source_pack_f32(
    float* query, float* packed_query, float* key, float* value,
    const float* q, const float* k, const float* v, const float* scales,
    unsigned rows, unsigned heads, unsigned dim, unsigned chunk, unsigned context,
    unsigned relative, unsigned blocks, float query_scale, float key_scale) {
    const size_t i=(size_t)blockIdx.x*blockDim.x+threadIdx.x;
    const size_t hidden=(size_t)heads*dim, padded=(size_t)blocks*chunk;
    if(i<padded*hidden) {
        const unsigned row=i/hidden,head=(i/dim)%heads,d=i%dim;
        const float input=row<rows?q[i]:0.f;
        const float scaled=input * query_scale * scales[d];
        query[i]=scaled;
        if(blocks>1)packed_query[((size_t)head*blocks+row/chunk)*chunk*dim+(row%chunk)*dim+d]=scaled;
    }
    if(i<(size_t)blocks*context*hidden) {
        const unsigned block=i/((size_t)context*hidden),c=(i/hidden)%context,head=(i/dim)%heads,d=i%dim;
        const long long row=(long long)block*chunk+c-(relative-1);
        const bool present=row>=0&&row<rows;
        const size_t source=present?(size_t)row*hidden+head*dim+d:0;
        const float key_input=present?k[source]:0.f;
        const float key_value=key_input * key_scale;
        const float plain_value=present?v[source]:0.f;
        const size_t ki=blocks==1?i:(((size_t)head*blocks+block)*dim+d)*context+c;
        const size_t vi=blocks==1?i:(((size_t)head*blocks+block)*context+c)*dim+d;
        key[ki]=key_value;value[vi]=plain_value;
    }
}
extern "C" __global__ void antfly_local_relative_source_softmax_f32(
    float* scores,const float* relative_scores,const float* valid,
    unsigned rows,unsigned heads,unsigned chunk,unsigned context,unsigned relative,unsigned blocks,
    float cap,float invalid_value,unsigned exclude_farthest) {
    const unsigned lane=threadIdx.x,padded=blocks*chunk;
    const unsigned head=blockIdx.x/padded,query=blockIdx.x%padded;
    if(head>=heads||lane>=32)return;
    const unsigned block=query/chunk,offset=query%chunk;
    const size_t score_index=(((size_t)head*blocks+block)*chunk+offset)*context+lane;
    float score=-INFINITY;
    if(lane<context) {
        // Literal pad(...W+1-L)/flatten/truncate/view relative shift.
        const unsigned flat=offset*context+lane,position=flat%(context+1);
        const float relative_value=position<relative?relative_scores[((size_t)head*padded+block*chunk+flat/(context+1))*relative+position]:0.f;
        const float content=scores[score_index];
        score=tanhf((content + relative_value) / cap) * cap;
        const long long key=(long long)block*chunk+lane-(relative-1);
        if(query>=rows||key<0||key>=rows||key>query||query-key>relative-1-exclude_farthest||valid[key]==0.f)score=invalid_value;
    }
    float maximum=score;
    for(int offset=16;offset>0;offset>>=1)maximum=fmaxf(maximum,__shfl_xor_sync(0xffffffffu,maximum,offset));
    float probability=lane<context?expf(__fsub_rn(score,maximum)):0.f;
    float denominator=probability;
    for(int offset=16;offset>0;offset>>=1)denominator=__fadd_rn(denominator,__shfl_xor_sync(0xffffffffu,denominator,offset));
    if(lane<context)scores[score_index]=__fdiv_rn(probability,denominator);
}
extern "C" __global__ void antfly_local_relative_source_publish_bf16_values_f32(
    float* out,const float* input,unsigned rows,unsigned heads,unsigned dim,unsigned chunk,unsigned blocks) {
    const size_t i=(size_t)blockIdx.x*blockDim.x+threadIdx.x,hidden=(size_t)heads*dim;
    if(i>=(size_t)rows*hidden)return;
    const unsigned row=i/hidden,head=(i/dim)%heads,d=i%dim;
    const size_t source=blocks==1?i:(((size_t)head*blocks+row/chunk)*chunk+row%chunk)*dim+d;
    const float value=input[source];out[i]=antfly_local_relative_bf16_value(value);
}
// tile-codegen:end attention.local_relative_source family
