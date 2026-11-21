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

// Resident FP32 training normalization. Four consecutive elements per lane,
// 128 lanes per row, Welford statistics, and fixed reductions match the pinned
// PyTorch CUDA arithmetic. No atomics or host tensor transfers are used.
// Algorithm reference: pytorch v2.9.1 aten/src/ATen/native/cuda/layer_norm_kernel.cu.
namespace termite_training_norm {
struct Moment { float mean, m2, count; };
__device__ __forceinline__ Moment join(Moment b, Moment a) {
    float count = a.count + b.count;
    if (count == 0) return {0, 0, 0};
    float inverse = 1.0f / count, delta = b.mean - a.mean;
    float wa = a.count * inverse, wb = b.count * inverse;
    return {wa * a.mean + wb * b.mean,
            a.m2 + b.m2 + delta * delta * a.count * wb, count};
}
__device__ float2 moments(const float* x, unsigned width, float eps, float* shared) {
    const unsigned lane = threadIdx.x % 32, warp = threadIdx.x / 32;
    Moment m = {0, 0, 0};
    for (unsigned v = threadIdx.x; v < width / 4; v += 128) {
        #pragma unroll
        for (unsigned j = 0; j < 4; ++j) {
            float value = x[v * 4 + j], delta = value - m.mean;
            m.count += 1.0f;
            float mean = m.mean + delta * (1.0f / m.count);
            m.m2 += delta * (value - mean);
            m.mean = mean;
        }
    }
    for (unsigned step = 16; step; step /= 2) {
        Moment other = {__shfl_down_sync(0xffffffff, m.mean, step),
                        __shfl_down_sync(0xffffffff, m.m2, step),
                        __shfl_down_sync(0xffffffff, m.count, step)};
        m = join(m, other);
    }
    for (unsigned step = 2; step; step /= 2) {
        if (lane == 0 && warp >= step && warp < 2 * step) {
            unsigned slot = (warp - step) * 3;
            shared[slot] = m.mean; shared[slot + 1] = m.m2; shared[slot + 2] = m.count;
        }
        __syncthreads();
        if (lane == 0 && warp < step) {
            unsigned slot = warp * 3;
            m = join(m, {shared[slot], shared[slot + 1], shared[slot + 2]});
        }
        __syncthreads();
    }
    if (threadIdx.x == 0) { shared[0] = m.mean; shared[1] = rsqrtf(m.m2 / float(width) + eps); }
    __syncthreads();
    return make_float2(shared[0], shared[1]);
}
__device__ float row_sum(float value, float* shared) {
    const unsigned lane = threadIdx.x % 32, warp = threadIdx.x / 32;
    for (unsigned step = 16; step; step /= 2) value += __shfl_down_sync(0xffffffff, value, step);
    __syncthreads();
    if (lane == 0) shared[warp] = value;
    __syncthreads();
    value = threadIdx.x < 4 ? shared[lane] : 0.0f;
    if (warp == 0)
        for (unsigned step = 16; step; step /= 2) value += __shfl_down_sync(0xffffffff, value, step);
    return value;
}
__device__ void parameters(float* output, const float* x, const float* dy,
                          const float* stats, unsigned rows, unsigned width, float* shared) {
    const unsigned col = blockIdx.x * 32 + threadIdx.x;
    const unsigned partitions = blockDim.y, tile = partitions * 8;
    float a = 0, b = 0;
    for (unsigned first = 0; first < rows; first += tile) {
        #pragma unroll
        for (unsigned j = 0; j < 8; ++j) {
            unsigned row = first + threadIdx.y * 8 + j;
            if (row < rows && col < width) {
                a += dy[row * width + col] * (x[row * width + col] - stats[row]) * stats[rows + row];
                b += dy[row * width + col];
            }
        }
    }
    float* dg = output + rows * width;
    float* db = dg + width;
    if (partitions == 1) { if (col < width) { dg[col] = a; db[col] = b; } return; }
    float* partial_a = shared;
    float* partial_b = shared + partitions * 33;
    partial_a[threadIdx.y * 33 + threadIdx.x] = a;
    partial_b[threadIdx.y * 33 + threadIdx.x] = b;
    __syncthreads();
    for (unsigned i = threadIdx.y; i < 32; i += partitions) {
        a = threadIdx.x < partitions ? partial_a[threadIdx.x * 33 + i] : 0;
        b = threadIdx.x < partitions ? partial_b[threadIdx.x * 33 + i] : 0;
        for (unsigned step = partitions / 2; step; step /= 2) {
            a += __shfl_xor_sync(0xffffffff, a, step);
            b += __shfl_xor_sync(0xffffffff, b, step);
        }
        if (threadIdx.x == 0 && blockIdx.x * 32 + i < width) {
            dg[blockIdx.x * 32 + i] = a;
            db[blockIdx.x * 32 + i] = b;
        }
    }
}
} // namespace termite_training_norm

// phase 0: forward. phase 1: recompute statistics and write dX.
// phase 2: deterministically reduce parameter gradients into the packed output.
extern "C" __global__ void termite_gliner25_layer_norm_f32(
    float* output, const float* x, const float* gamma, const float* beta,
    const float* dy, float* stats, unsigned rows, unsigned width, float eps, unsigned phase
) {
    extern __shared__ float shared[];
    if (phase == 2) {
        termite_training_norm::parameters(output, x, dy, stats, rows, width, shared);
        return;
    }
    const unsigned row = blockIdx.x;
    if (row >= rows) return;
    float2 normalized = termite_training_norm::moments(x + row * width, width, eps, shared);
    const float mean = normalized.x, inverse = normalized.y;
    if (phase == 0) {
        for (unsigned j = threadIdx.x; j < width; j += 128)
            output[row * width + j] = gamma[j] * (inverse * (x[row * width + j] - mean)) + beta[j];
        return;
    }
    if (threadIdx.x == 0) { stats[row] = mean; stats[rows + row] = inverse; }
    float a = 0, b = 0;
    for (unsigned start = threadIdx.x * 4; start < width; start += 512) {
        #pragma unroll
        for (unsigned j = start; j < start + 4; ++j) {
            float value = dy[row * width + j] * gamma[j];
            a += dy[row * width + j] * gamma[j];
            b += value * (x[row * width + j] - mean) * inverse;
        }
    }
    a = termite_training_norm::row_sum(a, shared);
    b = termite_training_norm::row_sum(b, shared);
    if (threadIdx.x == 0) { shared[0] = a; shared[1] = b; }
    __syncthreads();
    a = shared[0]; b = shared[1];
    const float factor = (1.0f / float(width)) * inverse;
    for (unsigned j = threadIdx.x; j < width; j += 128) {
        float result = float(width) * gamma[j] * dy[row * width + j];
        result -= (x[row * width + j] - mean) * inverse * b;
        result -= a;
        output[row * width + j] = result * factor;
    }
}
