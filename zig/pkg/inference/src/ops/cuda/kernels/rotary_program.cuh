// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cuda_bf16.h>
enum class antfly_rotary_products { f32, bf16 };
template<antfly_rotary_products Products=antfly_rotary_products::f32>
__device__ __forceinline__ float antfly_rotary_component(float lo,float hi,float c,float s,bool upper) {
    if constexpr(Products==antfly_rotary_products::bf16){
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
