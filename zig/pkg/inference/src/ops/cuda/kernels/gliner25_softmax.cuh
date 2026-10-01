// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0 AND BSD-3-Clause
/*
Reduction arithmetic adapted from PyTorch, under the following license:

From PyTorch:

Copyright (c) 2016-     Facebook, Inc            (Adam Paszke)
Copyright (c) 2014-     Facebook, Inc            (Soumith Chintala)
Copyright (c) 2011-2014 Idiap Research Institute (Ronan Collobert)
Copyright (c) 2012-2014 Deepmind Technologies    (Koray Kavukcuoglu)
Copyright (c) 2011-2012 NEC Laboratories America (Koray Kavukcuoglu)
Copyright (c) 2011-2013 NYU                      (Clement Farabet)
Copyright (c) 2006-2010 NEC Laboratories America (Ronan Collobert, Leon Bottou, Iain Melvin, Jason Weston)
Copyright (c) 2006      Idiap Research Institute (Samy Bengio)
Copyright (c) 2001-2004 Idiap Research Institute (Ronan Collobert, Samy Bengio, Johnny Mariethoz)

From Caffe2:

Copyright (c) 2016-present, Facebook Inc. All rights reserved.

All contributions by Facebook:
Copyright (c) 2016 Facebook Inc.

All contributions by Google:
Copyright (c) 2015 Google Inc.
All rights reserved.

All contributions by Yangqing Jia:
Copyright (c) 2015 Yangqing Jia
All rights reserved.

All contributions by Kakao Brain:
Copyright 2019-2020 Kakao Brain

All contributions by Cruise LLC:
Copyright (c) 2022 Cruise LLC.
All rights reserved.

All contributions by Tri Dao:
Copyright (c) 2024 Tri Dao.
All rights reserved.

All contributions by Arm:
Copyright (c) 2021, 2023-2024 Arm Limited and/or its affiliates

All contributions from Caffe:
Copyright(c) 2013, 2014, 2015, the respective contributors
All rights reserved.

All other contributions:
Copyright(c) 2015, 2016 the respective contributors
All rights reserved.

Caffe2 uses a copyright model similar to Caffe: each contributor holds
copyright over their contributions to Caffe2. The project versioning records
all such contribution and copyright details. If a contributor wants to further
mark their specific copyright on a particular contribution, they should
indicate their copyright solely in the commit message of the change when it is
committed.

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

3. Neither the names of Facebook, Deepmind Technologies, NYU, NEC Laboratories America
   and IDIAP Research Institute nor the names of its contributors may be
   used to endorse or promote products derived from this software without
   specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/
#pragma once

// Pinned PyTorch 2.9.1 PersistentSoftmax.cuh arithmetic, FP32 rows <= 1024.
// Forward takes logits. Backward takes an already-rounded dy * probability,
// matching the separate multiplication preceding the reference backward kernel.
// One warp/subwarp owns each row; there are no floating point atomics.
template<unsigned LogWidth, bool Backward, bool Logarithmic = false>
__device__ __forceinline__ void gliner25_softmax_warp(
    float* output, const float* input, const float* probability,
    unsigned rows, unsigned width
) {
    constexpr unsigned padded_width = 1u << LogWidth;
    constexpr unsigned lanes = padded_width < 32 ? padded_width : 32;
    constexpr unsigned iterations = padded_width / lanes;
    constexpr unsigned batches = padded_width <= 128 ? 2 : 1;
    const unsigned first = (blockIdx.x * blockDim.y + threadIdx.y) * batches;
    const unsigned lane = threadIdx.x;
    float values[batches][iterations], maximum[batches], sum[batches];
    #pragma unroll
    for (unsigned b = 0; b < batches; ++b) {
        maximum[b] = __int_as_float(0xff800000);
        sum[b] = 0.0f;
        #pragma unroll
        for (unsigned i = 0; i < iterations; ++i) {
            const unsigned column = lane + i * lanes;
            values[b][i] = first + b < rows && column < width ?
                input[(first + b) * width + column] : (Backward ? 0.0f : __int_as_float(0xff800000));
            maximum[b] = maximum[b] > values[b][i] ? maximum[b] : values[b][i];
        }
    }
    if (!Backward) {
        #pragma unroll
        for (unsigned offset = lanes / 2; offset != 0; offset /= 2) {
            #pragma unroll
            for (unsigned b = 0; b < batches; ++b) {
                const float other = __shfl_xor_sync(0xffffffff, maximum[b], offset, lanes);
                maximum[b] = maximum[b] < other ? other : maximum[b];
            }
        }
    }
    #pragma unroll
    for (unsigned b = 0; b < batches; ++b) {
        #pragma unroll
        for (unsigned i = 0; i < iterations; ++i) {
            if (!Backward && !Logarithmic) values[b][i] = expf(values[b][i] - maximum[b]);
            sum[b] += !Backward && Logarithmic ? expf(values[b][i] - maximum[b]) : values[b][i];
        }
    }
    #pragma unroll
    for (unsigned offset = lanes / 2; offset != 0; offset /= 2) {
        #pragma unroll
        for (unsigned b = 0; b < batches; ++b)
            sum[b] += __shfl_xor_sync(0xffffffff, sum[b], offset, lanes);
    }
    #pragma unroll
    for (unsigned b = 0; b < batches; ++b) {
        #pragma unroll
        for (unsigned i = 0; i < iterations; ++i) {
            const unsigned column = lane + i * lanes;
            if (first + b < rows && column < width) {
                const unsigned index = (first + b) * width + column;
                if constexpr (Logarithmic) {
                    output[index] = Backward ? values[b][i] - expf(probability[index]) * sum[b] : (values[b][i] - maximum[b]) - logf(sum[b]);
                } else {
                    output[index] = Backward ? values[b][i] - probability[index] * sum[b] : values[b][i] / sum[b];
                }
            }
        }
    }
}

extern "C" __global__ void termite_gliner25_softmax_f32(
    float* output, const float* input, const float* probability,
    unsigned rows, unsigned width, unsigned backward
) {
    if (width == 0 || width > 1024) return; // Host admission rejects these shapes.
    const unsigned log_width = width == 1 ? 0 : 32 - __clz(width - 1);
    #define GLINER25_SOFTMAX_CASE(n) case n: \
        if (backward) gliner25_softmax_warp<n, true>(output, input, probability, rows, width); \
        else gliner25_softmax_warp<n, false>(output, input, probability, rows, width); break;
    switch (log_width) {
        GLINER25_SOFTMAX_CASE(0)
        GLINER25_SOFTMAX_CASE(1)
        GLINER25_SOFTMAX_CASE(2)
        GLINER25_SOFTMAX_CASE(3)
        GLINER25_SOFTMAX_CASE(4)
        GLINER25_SOFTMAX_CASE(5)
        GLINER25_SOFTMAX_CASE(6)
        GLINER25_SOFTMAX_CASE(7)
        GLINER25_SOFTMAX_CASE(8)
        GLINER25_SOFTMAX_CASE(9)
        GLINER25_SOFTMAX_CASE(10)
    }
    #undef GLINER25_SOFTMAX_CASE
}

// Log-softmax uses the same persistent reduction, with PyTorch's asymmetric
// forward/backward thresholds (2048/1024 FP32 elements respectively).
extern "C" __global__ void termite_gliner25_log_softmax_warp_f32(
    float* output, const float* input, const float* log_probability,
    unsigned rows, unsigned width, unsigned backward
) {
    if (width == 0 || width > (backward ? 1024u : 2048u)) return;
    const unsigned log_width = width == 1 ? 0 : 32 - __clz(width - 1);
    #define GLINER25_LOG_SOFTMAX_CASE(n) case n: \
        if (backward) gliner25_softmax_warp<n, true, true>(output, input, log_probability, rows, width); \
        else gliner25_softmax_warp<n, false, true>(output, input, log_probability, rows, width); break;
    switch (log_width) {
        GLINER25_LOG_SOFTMAX_CASE(0)
        GLINER25_LOG_SOFTMAX_CASE(1)
        GLINER25_LOG_SOFTMAX_CASE(2)
        GLINER25_LOG_SOFTMAX_CASE(3)
        GLINER25_LOG_SOFTMAX_CASE(4)
        GLINER25_LOG_SOFTMAX_CASE(5)
        GLINER25_LOG_SOFTMAX_CASE(6)
        GLINER25_LOG_SOFTMAX_CASE(7)
        GLINER25_LOG_SOFTMAX_CASE(8)
        GLINER25_LOG_SOFTMAX_CASE(9)
        GLINER25_LOG_SOFTMAX_CASE(10)
        GLINER25_LOG_SOFTMAX_CASE(11)
    }
    #undef GLINER25_LOG_SOFTMAX_CASE
}

// FP32 contiguous-row arithmetic from PyTorch 2.9.1 SoftMax.cu and
// block_reduce.cuh. Shared-memory caching and vector loads can be omitted
// without changing each thread's reduction order; the launch planner retains
// the reference block size and reduction strategy.
template<bool Maximum>
__device__ __forceinline__ float gliner25_softmax_combine(float a, float b) {
    return Maximum ? (a < b ? b : a) : a + b;
}
template<bool Maximum>
__device__ __forceinline__ float gliner25_softmax_block_reduce(float value, float* shared) {
    constexpr float identity = Maximum ? -3.40282346638528859812e+38f : 0.0f;
    const unsigned lane = threadIdx.x % 32, warp = threadIdx.x / 32;
    #pragma unroll
    for (unsigned offset = 16; offset; offset /= 2)
        value = gliner25_softmax_combine<Maximum>(value, __shfl_down_sync(0xffffffff, value, offset));
    __syncthreads();
    if (lane == 0) shared[warp] = value;
    __syncthreads();
    value = threadIdx.x < blockDim.x / 32 ? shared[lane] : identity;
    if (warp == 0) {
        #pragma unroll
        for (unsigned offset = 16; offset; offset /= 2)
            value = gliner25_softmax_combine<Maximum>(value, __shfl_down_sync(0xffffffff, value, offset));
    }
    if (threadIdx.x == 0) shared[0] = value;
    __syncthreads();
    return shared[0];
}
__device__ __forceinline__ float gliner25_softmax_serial_warps(float value, float* shared) {
    __syncthreads();
    shared[threadIdx.x] = value;
    __syncthreads();
    if (threadIdx.x < blockDim.x / 32) {
        float sum = 0;
        #pragma unroll
        for (unsigned i = 0; i < 32; ++i) sum += shared[threadIdx.x * 32 + i];
        // All first-warp readers finish before any writer reuses shared[0..].
        __syncwarp((unsigned)((1ull << (blockDim.x / 32)) - 1ull));
        shared[threadIdx.x] = sum;
    }
    __syncthreads();
    if (threadIdx.x == 0) {
        float sum = 0;
        for (unsigned i = 0; i < blockDim.x / 32; ++i) sum += shared[i];
        shared[0] = sum;
    }
    __syncthreads();
    return shared[0];
}
// Operation: 0=max, 1=sum exp(x-maximum), 2=sum x.
template<unsigned Operation>
__device__ __forceinline__ float gliner25_softmax_accumulate(float sum, float value, float maximum) {
    if constexpr (Operation == 0) return sum < value ? value : sum;
    if constexpr (Operation == 1) return sum + expf(value - maximum);
    return sum + value;
}
template<unsigned Operation>
__device__ __forceinline__ float gliner25_softmax_ilp(const float* input, unsigned width, float maximum) {
    float sum = Operation == 0 ? -3.40282346638528859812e+38f : 0.0f;
    const unsigned shift = ((unsigned long long)input % 16) / 4;
    unsigned offset = threadIdx.x;
    if (shift) {
        input -= shift;
        width += shift;
        if (offset >= shift && offset < width) sum = gliner25_softmax_accumulate<Operation>(sum, input[offset], maximum);
        width -= blockDim.x > width ? width : blockDim.x;
        input += blockDim.x;
    }
    const unsigned last = width % (4 * blockDim.x);
    for (; offset * 4 < width - last; offset += blockDim.x) {
        #pragma unroll
        for (unsigned j = 0; j < 4; ++j) sum = gliner25_softmax_accumulate<Operation>(sum, input[offset * 4 + j], maximum);
    }
    for (offset = width - last + threadIdx.x; offset < width; offset += blockDim.x)
        sum = gliner25_softmax_accumulate<Operation>(sum, input[offset], maximum);
    return sum;
}
template<unsigned Operation>
__device__ __forceinline__ float gliner25_softmax_vector_rows(const float* input, unsigned width, float maximum) {
    float sum = Operation == 0 ? -3.40282346638528859812e+38f : 0.0f;
    for (unsigned offset = threadIdx.x; offset * 4 < width; offset += blockDim.x) {
        #pragma unroll
        for (unsigned j = 0; j < 4; ++j) sum = gliner25_softmax_accumulate<Operation>(sum, input[offset * 4 + j], maximum);
    }
    return sum;
}
extern "C" __global__ void termite_gliner25_log_softmax_block_f32(
    float* output, const float* input, const float* log_probability,
    unsigned width, unsigned backward, unsigned warp_reduction
) {
    __shared__ float shared[1024];
    input += (unsigned long long)blockIdx.x * width;
    output += (unsigned long long)blockIdx.x * width;
    if (backward) {
        log_probability += (unsigned long long)blockIdx.x * width;
        float sum = warp_reduction ? gliner25_softmax_vector_rows<2>(input, width, 0) : gliner25_softmax_ilp<2>(input, width, 0);
        sum = warp_reduction ? gliner25_softmax_block_reduce<false>(sum, shared) : gliner25_softmax_serial_warps(sum, shared);
        for (unsigned i = threadIdx.x; i < width; i += blockDim.x)
            output[i] = input[i] - expf(log_probability[i]) * sum;
    } else {
        float maximum = -3.40282346638528859812e+38f;
        if (width <= 9 * blockDim.x) {
            for (unsigned i = threadIdx.x; i < width; i += blockDim.x) maximum = maximum < input[i] ? input[i] : maximum;
        } else maximum = warp_reduction ? gliner25_softmax_vector_rows<0>(input, width, 0) : gliner25_softmax_ilp<0>(input, width, 0);
        maximum = gliner25_softmax_block_reduce<true>(maximum, shared);
        float sum = 0;
        if (width <= 9 * blockDim.x) {
            for (unsigned i = threadIdx.x; i < width; i += blockDim.x) sum += expf(input[i] - maximum);
        } else sum = warp_reduction ? gliner25_softmax_vector_rows<1>(input, width, maximum) : gliner25_softmax_ilp<1>(input, width, maximum);
        sum = gliner25_softmax_block_reduce<false>(sum, shared);
        const float log_sum = logf(sum);
        for (unsigned i = threadIdx.x; i < width; i += blockDim.x) output[i] = (input[i] - maximum) - log_sum;
    }
}
