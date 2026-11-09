// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cuda_bf16.h>
#include "rotary_frequency_training.cuh"
enum class antfly_rotary_products { f32, bf16, bf16_source };
template<antfly_rotary_products Products=antfly_rotary_products::f32>
__device__ __forceinline__ float antfly_rotary_component(float lo,float hi,float c,float s,bool upper) {
    if constexpr(Products==antfly_rotary_products::bf16_source){
        const float stored_c=__bfloat162float(__float2bfloat16_rn(c));
        const float stored_s=__bfloat162float(__float2bfloat16_rn(s));
        const float a=__bfloat162float(__float2bfloat16_rn(__fmul_rn(upper?hi:lo,stored_c)));
        const float b=__bfloat162float(__float2bfloat16_rn(__fmul_rn(upper?lo:-hi,stored_s)));
        return __bfloat162float(__float2bfloat16_rn(__fadd_rn(a,b)));
    }else if constexpr(Products==antfly_rotary_products::bf16){
        __nv_bfloat16 a=__float2bfloat16_rn((upper?hi:lo)*c);
        __nv_bfloat16 b=__float2bfloat16_rn((upper?lo:-hi)*s);
        return __bfloat162float(a)+__bfloat162float(b);
    }else return upper?lo*s+hi*c:lo*c-hi*s;
}

struct antfly_rotary_pair { unsigned axis,local,lo,hi; };
template<bool AxisLocalHalves,bool TwoAxes> __device__ __forceinline__ antfly_rotary_pair antfly_rotary_pair_indices(unsigned pair,unsigned pairs_per_axis){
    const unsigned axis=TwoAxes?(pair<pairs_per_axis?0u:1u):pair/pairs_per_axis;
    const unsigned local=pair%pairs_per_axis;
    const unsigned lo=AxisLocalHalves?axis*2u*pairs_per_axis+local:pair;
    return {axis,local,lo,lo+(AxisLocalHalves?pairs_per_axis:2u*pairs_per_axis)};
}

// This retained table is selected only by the explicitly qualified source
// product policy. Ordinary F32 and historical BF16 products keep their math.
__device__ __forceinline__ float antfly_rotary_source_frequency(float theta,unsigned dim,unsigned pair) {
    if(dim==32u && theta==100.0f && pair<16u)
        return __uint_as_float(antfly_rotary_axis32_theta100_bits[pair]);
    return __fdiv_rn(1.0f,powf(theta,(2.0f*pair)/(float)dim));
}

// Indexed coordinate/layout family shared with the existing component policy.
// The export instantiates Products at compile time; no device policy argument.
template<antfly_rotary_products Products>
__device__ __forceinline__ void antfly_rotary_indexed(
    float* dst,const float* input,const unsigned int* positions,
    unsigned int total_chunks,unsigned int token_count,unsigned int chunks_per_token,
    unsigned int head_dim,float theta,float freq_scale,
    unsigned int section_height,unsigned int section_width) {
    unsigned int idx=blockIdx.x*blockDim.x+threadIdx.x;
    unsigned int total=total_chunks*head_dim;
    if(idx>=total)return;
    unsigned int chunk=idx/head_dim,d=idx-chunk*head_dim;
    unsigned int rotary_pairs=head_dim/2u;
    unsigned int pair=d<rotary_pairs?d:d-rotary_pairs;
    unsigned int axis=0u;
    if((pair%3u)==1u && pair<section_height*3u)axis=1u;
    else if((pair%3u)==2u && pair<section_width*3u)axis=2u;
    unsigned int token=chunk/chunks_per_token;
    unsigned int position=positions[axis*token_count+token];
    float frequency;
    if constexpr(Products==antfly_rotary_products::bf16_source)
        frequency=antfly_rotary_source_frequency(theta,head_dim,pair);
    else frequency=1.0f/powf(theta,(2.0f*(float)pair)/(float)head_dim);
    float angle=(float)position*freq_scale*frequency;
    const float lo=input[chunk*head_dim+pair],hi=input[chunk*head_dim+rotary_pairs+pair];
    dst[idx]=antfly_rotary_component<Products>(lo,hi,cosf(angle),sinf(angle),d>=rotary_pairs);
}
