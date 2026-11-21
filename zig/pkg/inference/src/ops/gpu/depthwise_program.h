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

#pragma once
// One native causal stencil owner. Layout, history, activation and separate
// product rounding and independent coordinate/address widths are static; each caller retains its launch and storage ABI.
#define ANTFLY_DEPTHWISE_PROGRAM \
template<bool ChannelMajor,bool History,bool Silu,bool ExplicitMultiplyAdd,unsigned FixedKernel=0,typename Index,typename Address=Index> \
ANTFLY_GPU_FUNCTION float antfly_causal_depthwise(ANTFLY_GPU_DEVICE const float*input, \
    ANTFLY_GPU_DEVICE const float*weight,ANTFLY_GPU_DEVICE const float*history, \
    Index time,unsigned channel,unsigned channels,unsigned kernel_size,unsigned initialized,unsigned start=0){ \
    const unsigned size=FixedKernel?FixedKernel:kernel_size;float sum=0.0f; \
    for(unsigned k=0;k<size;++k){ \
        Index padded=time+Index(k); \
        if constexpr(!History){if(padded<Index(size-1)+Index(start))continue;} \
        float x; \
        if constexpr(!History)x=input[Address(padded-Index(size-1))*channels+channel]; \
        else x=padded>=Index(size-1)?input[Address(padded-Index(size-1))*channels+channel]: \
            (initialized?history[Address(channel)*size+padded+1]:0.0f); \
        float w=weight[ChannelMajor?Address(channel)*size+k:Address(k)*channels+channel]; \
        if constexpr(ExplicitMultiplyAdd)sum=ANTFLY_GPU_MADD(sum,x,w); \
        else sum+=x*w; \
    } \
    if constexpr(Silu)return sum/(1.0f+ANTFLY_GPU_EXP(-sum));else return sum; \
}

#if defined(__CUDACC__)
#define ANTFLY_GPU_FUNCTION __device__ __forceinline__
#define ANTFLY_GPU_DEVICE
#define ANTFLY_GPU_MADD(sum,x,w) __fadd_rn(sum,__fmul_rn(x,w))
#define ANTFLY_GPU_EXP(x) expf(x)
ANTFLY_DEPTHWISE_PROGRAM
#else
#pragma push_macro("bool")
#undef bool
#define ANTFLY_GPU_FUNCTION __attribute__((always_inline)) inline
#define ANTFLY_GPU_DEVICE device
#define ANTFLY_GPU_MADD(sum,x,w) antfly_separate_product_add(sum,x,w)
#define ANTFLY_GPU_EXP(x) (exp)(x)
#define ANTFLY_GPU_STRINGIFY_INNER(...) #__VA_ARGS__
#define ANTFLY_GPU_STRINGIFY(...) ANTFLY_GPU_STRINGIFY_INNER(__VA_ARGS__)
static const char antfly_msl_depthwise_source[] =
    "#include <metal_stdlib>\nusing namespace metal;\n"
    "inline float antfly_separate_product_add(float sum,float x,float w){\n"
    "#pragma clang fp contract(off) reassociate(off)\nreturn sum+x*w;\n}\n"
    ANTFLY_GPU_STRINGIFY(ANTFLY_DEPTHWISE_PROGRAM) "\n";
#undef ANTFLY_GPU_STRINGIFY
#undef ANTFLY_GPU_STRINGIFY_INNER
#pragma pop_macro("bool")
#endif
#undef ANTFLY_GPU_FUNCTION
#undef ANTFLY_GPU_DEVICE
#undef ANTFLY_GPU_MADD
#undef ANTFLY_GPU_EXP
#undef ANTFLY_DEPTHWISE_PROGRAM
