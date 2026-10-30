// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Native warp/block policy. Callers retain their own shared storage and exact
// load/store stages; no model identity or runtime program interpreter is used.
__device__ __forceinline__ float antfly_row_warp_sum(float x) {
    for (int d = 16; d; d >>= 1) x += __shfl_down_sync(0xffffffffu, x, d);
    return x;
}

__device__ __forceinline__ float antfly_row_warp_max(float x) {
    for (int d = 16; d; d >>= 1) x = fmaxf(x, __shfl_down_sync(0xffffffffu, x, d));
    return x;
}

// Exact warp-then-block statistics; planes preserve interleaving and barriers.
// Four-lane training reductions remain separate qualified arithmetic policies.
template<bool Maximum, unsigned Planes, unsigned FixedWarps = 0, typename Finish>
__device__ __forceinline__ void antfly_warp_block_statistics(
    float (&values)[Planes], float* const (&partials)[Planes], Finish finish, float neutral = 0.0f) {
    #pragma unroll
    for (unsigned p = 0; p < Planes; ++p)
        values[p] = Maximum ? antfly_row_warp_max(values[p]) : antfly_row_warp_sum(values[p]);
    if ((threadIdx.x & 31u) == 0) {
        #pragma unroll
        for (unsigned p = 0; p < Planes; ++p) partials[p][threadIdx.x >> 5] = values[p];
    }
    __syncthreads();
    if (threadIdx.x < 32) {
        #pragma unroll
        for (unsigned p = 0; p < Planes; ++p)
            values[p] = threadIdx.x < (FixedWarps ? FixedWarps : blockDim.x >> 5) ? partials[p][threadIdx.x] : neutral;
        #pragma unroll
        for (unsigned p = 0; p < Planes; ++p)
            values[p] = Maximum ? antfly_row_warp_max(values[p]) : antfly_row_warp_sum(values[p]);
        if (threadIdx.x == 0) finish(values);
    }
}

template<unsigned FixedWarps = 0, typename Load, typename Store>
__device__ __forceinline__ void antfly_row_rms(
    unsigned width, float eps, float* partials, float& inv, Load load, Store store) {
    float sum = 0.0f;
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) {
        float x = load(i); sum += x * x;
    }
    float statistics[] = {sum}; float* const planes[] = {partials};
    antfly_warp_block_statistics<false, 1, FixedWarps>(statistics, planes,
        [&](const float* totals) { inv = rsqrtf(totals[0] / (float)width + eps); },
        0.0f);
    __syncthreads();
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) store(i, load(i), inv);
}

// Interleaved planes retain the original shared-tree schedule and barriers.
template<unsigned Planes>
__device__ __forceinline__ void antfly_row_tree_statistics(
    float (&values)[Planes], float* const (&partials)[Planes], unsigned threads = blockDim.x) {
    const unsigned tid = threadIdx.x;
    #pragma unroll
    for (unsigned p = 0; p < Planes; ++p) partials[p][tid] = values[p];
    __syncthreads();
    for (unsigned stride = threads >> 1; stride; stride >>= 1) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned p = 0; p < Planes; ++p) partials[p][tid] += partials[p][tid + stride];
        }
        __syncthreads();
    }
}

template<typename Load>
__device__ __forceinline__ float antfly_row_rms_tree_inverse(
    unsigned width, float eps, float* partial, Load load) {
    float sum = 0.0f;
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) { float x = load(i); sum += x*x; }
    float values[] = {sum}; float* const planes[] = {partial};
    antfly_row_tree_statistics(values, planes);
    return rsqrtf(partial[0] / (float)width + eps);
}

// The fixed 256-thread tree may finish its final five stages in warp 0.
// This is the same association as the original expanded vision row program.
template<unsigned Planes> __device__ __forceinline__ void antfly_row_warp_finish_256(
    float (&values)[Planes],float*const (&partials)[Planes]) {
    const unsigned tid=threadIdx.x;
    #pragma unroll
    for(unsigned p=0;p<Planes;++p)partials[p][tid]=values[p];
    __syncthreads();
    if(tid<32){
        #pragma unroll
        for(unsigned p=0;p<Planes;++p){
            float* v=partials[p];
            values[p]=((v[tid]+v[tid+128])+(v[tid+64]+v[tid+192]))+
                      ((v[tid+32]+v[tid+160])+(v[tid+96]+v[tid+224]));
        }
        for(unsigned step=16;step;step>>=1){
            #pragma unroll
            for(unsigned p=0;p<Planes;++p)values[p]+=__shfl_down_sync(0xffffffffu,values[p],step);
        }
        if(tid==0){
            #pragma unroll
            for(unsigned p=0;p<Planes;++p)partials[p][0]=values[p];
        }
    }
    __syncthreads();
}

// Shared-tree LayerNorm policy, preserving fixed-256 and dynamic-block trees.
// Load owns any residual add; Store owns affine arithmetic and final conversion.
template<unsigned FixedThreads = 0, bool WarpFinish256 = false, typename Load, typename Store>
__device__ __forceinline__ void antfly_row_layer_norm(
    unsigned width, float eps, float* sums, float* squares, Load load, Store store) {
    const unsigned tid = threadIdx.x;
    const unsigned threads = FixedThreads ? FixedThreads : blockDim.x;
    float sum = 0.0f, square = 0.0f;
    for (unsigned i = tid; i < width; i += threads) {
        float x = load(i); sum += x; square += x * x;
    }
    float values[] = {sum, square}; float* const planes[] = {sums, squares};
    static_assert(!WarpFinish256 || FixedThreads==256,"warp finish requires 256 threads");
    if constexpr(WarpFinish256)antfly_row_warp_finish_256(values,planes);
    else antfly_row_tree_statistics(values, planes, threads);
    const float mean = sums[0] / (float)width;
    const float variance = fmaxf(squares[0] / (float)width - mean * mean, 0.0f);
    const float inverse = rsqrtf(variance + eps);
    for (unsigned i = tid; i < width; i += threads) store(i, load(i), mean, inverse);
}
