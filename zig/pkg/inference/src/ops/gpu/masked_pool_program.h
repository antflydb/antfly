// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
// Shared token-major column traversal. Division placement and mask/count
// contracts are static policies; callers retain their row reduction and fusion.
#define ANTFLY_MASKED_POOL_PROGRAM \
template<bool OptionalMask,bool Count,bool PerTermDivision,typename Index,typename Address=Index,typename Input,typename Mask> \
ANTFLY_POOL_FUNCTION float antfly_masked_column_sum(ANTFLY_POOL_DEVICE const Input*input, \
    ANTFLY_POOL_DEVICE const Mask*mask,Index start,Index end,Index width,Index column, \
    float divisor,ANTFLY_POOL_THREAD float&count,Address row_base=0){ \
    float sum=0.0f; \
    for(Index token=start;token<end;++token){ \
        if((OptionalMask&&!mask)||mask[row_base+token]!=0){ \
            float value=float(input[(row_base+token)*width+column]); \
            if constexpr(PerTermDivision)sum+=value/divisor;else sum+=value; \
            if constexpr(Count)count+=1.0f; \
        } \
    } \
    return sum; \
}
#if defined(__CUDACC__)
#define ANTFLY_POOL_FUNCTION __device__ __forceinline__
#define ANTFLY_POOL_DEVICE
#define ANTFLY_POOL_THREAD
ANTFLY_MASKED_POOL_PROGRAM
#else
#pragma push_macro("bool")
#undef bool
#define ANTFLY_POOL_FUNCTION __attribute__((always_inline)) inline
#define ANTFLY_POOL_DEVICE device
#define ANTFLY_POOL_THREAD thread
#define ANTFLY_POOL_STRING_INNER(...) #__VA_ARGS__
#define ANTFLY_POOL_STRING(...) ANTFLY_POOL_STRING_INNER(__VA_ARGS__)
static const char antfly_msl_masked_pool_source[] = ANTFLY_POOL_STRING(ANTFLY_MASKED_POOL_PROGRAM) "\n";
#undef ANTFLY_POOL_STRING
#undef ANTFLY_POOL_STRING_INNER
#pragma pop_macro("bool")
#endif
#undef ANTFLY_POOL_FUNCTION
#undef ANTFLY_POOL_DEVICE
#undef ANTFLY_POOL_THREAD
#undef ANTFLY_MASKED_POOL_PROGRAM
