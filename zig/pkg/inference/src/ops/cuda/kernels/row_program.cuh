// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cuda_bf16.h>

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

// Native source phase: preserve scalar liveness, plane interleaving and the
// caller's block-y slice. Moving these planes into arrays changes VJP lowering.
#define ANTFLY_CUDA_TRAINING_FOLD(Store,Reduce,Reload,Shuffle) \
    Store \
    for(unsigned int d=blockDim.x/2;d>=32;d>>=1){__syncthreads();if(lane<d){Reduce}} \
    Reload \
    for(int d=16;d>0;d>>=1){Shuffle}

// Contiguous source RMS schedules share their fold and publication stages.
// Vector loads accumulate adjacent groups of four; scalar loads accumulate
// four independent block-x strides. Mean publication is a separate policy:
// the historical training adapter divides, while source mean multiplies by
// the rounded F32 reciprocal. These differ at non-power-of-two widths.
enum class AntflyRmsReadSchedule : unsigned { ContiguousVector4, ContiguousScalar4 };
template<AntflyRmsReadSchedule Read, bool ReciprocalMean, typename Load, typename Weight, typename Store>
__device__ __forceinline__ void antfly_row_contiguous_rms(
    unsigned rows, unsigned width, float eps, float* shared, float* inv, Load load, Weight weight, Store store) {
    const unsigned row = blockIdx.x * blockDim.y + threadIdx.y;
    if (row >= rows) return;
    const size_t base = (size_t)row * width;
    float s0=0.f,s1=0.f,s2=0.f,s3=0.f;
    if constexpr (Read == AntflyRmsReadSchedule::ContiguousVector4) {
        for (unsigned v=threadIdx.x; v<width/4; v+=blockDim.x) {
            const unsigned i=v*4;
            const float x0=load(base+i),x1=load(base+i+1),x2=load(base+i+2),x3=load(base+i+3);
            s0=__fadd_rn(s0,__fmul_rn(x0,x0));s1=__fadd_rn(s1,__fmul_rn(x1,x1));
            s2=__fadd_rn(s2,__fmul_rn(x2,x2));s3=__fadd_rn(s3,__fmul_rn(x3,x3));
        }
    } else {
        for (unsigned i=threadIdx.x; i<width; i+=4*blockDim.x) {
            float x=load(base+i);s0=__fadd_rn(s0,__fmul_rn(x,x));
            if(i+blockDim.x<width){x=load(base+i+blockDim.x);s1=__fadd_rn(s1,__fmul_rn(x,x));}
            if(i+2*blockDim.x<width){x=load(base+i+2*blockDim.x);s2=__fadd_rn(s2,__fmul_rn(x,x));}
            if(i+3*blockDim.x<width){x=load(base+i+3*blockDim.x);s3=__fadd_rn(s3,__fmul_rn(x,x));}
        }
    }
    float sum=((s0+s1)+s2)+s3;
    const unsigned lane=threadIdx.x,offset=threadIdx.y*blockDim.x;
    ANTFLY_CUDA_TRAINING_FOLD(
        shared[offset+lane]=sum;,
        shared[offset+lane]+=shared[offset+lane+d];,
        sum=shared[offset+lane];,
        sum+=__shfl_down_sync(0xffffffffu,sum,d);)
    if(lane==0) {
        if constexpr(ReciprocalMean) inv[threadIdx.y]=rsqrtf(__fadd_rn(__fmul_rn(sum,1.f/(float)width),eps));
        else inv[threadIdx.y]=rsqrtf(sum/(float)width+eps);
    }
    __syncthreads();
    for(unsigned i=lane;i<width;i+=blockDim.x) {
        const float normalized=load(base+i)*inv[threadIdx.y];
        store(base+i,normalized*weight(i));
    }
}

// A BF16 attention value GEMM consumes unnormalized softmax weights in BF16;
// its normalization denominator remains F32. Keep this storage boundary typed
// and shared by scalar/tail and tensor-core traversals.
__device__ __forceinline__ __nv_bfloat16 antfly_attention_probability_bf16(float probability) {
    return __float2bfloat16_rn(probability);
}

// Numerical schedules selected by the typed attention descriptor, independent
// of model names. F32 row policies above retain their original arithmetic.
enum class AntflyAttentionSchedule : unsigned { FlashBF16 = 0, EfficientBF16 = 1 };
__device__ __forceinline__ float antfly_attention_log2_score(float x) {
    return __fmul_rn(x, 1.4426950408889634074f);
}
__device__ __forceinline__ float antfly_attention_exp2_bf16(float x) {
    // Match the declared CUDA exp2f node; do not substitute the approximate
    // FTZ instruction or change compiler flags for unrelated F32 programs.
    return exp2f(x);
}
template<AntflyAttentionSchedule Schedule>
__device__ __forceinline__ float antfly_attention_exponent_bf16(float score, float maximum) {
    if (Schedule == AntflyAttentionSchedule::FlashBF16)
        return antfly_attention_exp2_bf16(__fsub_rn(antfly_attention_log2_score(score), antfly_attention_log2_score(maximum)));
    return antfly_attention_exp2_bf16(__fsub_rn(score, maximum));
}
template<AntflyAttentionSchedule Schedule>
__device__ __forceinline__ float antfly_attention_rescale_bf16(float previous, float maximum) {
    const float delta = __fsub_rn(previous, maximum);
    return antfly_attention_exp2_bf16(Schedule == AntflyAttentionSchedule::FlashBF16 ? antfly_attention_log2_score(delta) : delta);
}

// CUTLASS-compatible BF16 K8 accumulator quantum. This is deliberately not a
// K16 MMA followed by an independent partial-sum merge.
__device__ __forceinline__ void antfly_attention_mma_bf16_k8(float (&c)[4], unsigned a0, unsigned a1, unsigned b0) {
    asm volatile("mma.sync.aligned.m16n8k8.row.col.f32.bf16.bf16.f32 "
        "{%0,%1,%2,%3}, {%4,%5}, {%6}, {%0,%1,%2,%3};"
        : "+f"(c[0]), "+f"(c[1]), "+f"(c[2]), "+f"(c[3]) : "r"(a0), "r"(a1), "r"(b0));
}
