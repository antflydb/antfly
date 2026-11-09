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

extern "C" __global__ void antfly_depthwise_3x3_residual_f32(
    float* out, const float* input, const float* weight, const float* bias,
    unsigned count, unsigned height, unsigned width, unsigned channels) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned c = i % channels;
    const unsigned token = i / channels;
    const unsigned x = token % width;
    const unsigned y = (token / width) % height;
    const unsigned page = token / (width * height);
    float acc = bias[c];
    #pragma unroll
    for (int ky = 0; ky < 3; ++ky) {
        const int iy = (int)y + ky - 1;
        if (iy < 0 || iy >= (int)height) continue;
        #pragma unroll
        for (int kx = 0; kx < 3; ++kx) {
            const int ix = (int)x + kx - 1;
            if (ix < 0 || ix >= (int)width) continue;
            const unsigned src = ((page * height + (unsigned)iy) * width + (unsigned)ix) * channels + c;
            acc = __fmaf_rn(input[src], weight[c * 9 + ky * 3 + kx], acc);
        }
    }
    out[i] = __fadd_rn(input[i], acc);
}

#include <cuda_fp16.h>
#include <math.h>
#include "../kernels/row_program.cuh"
// Row addressing is owned by the ordinary/window entrypoint. Statistics and
// conversion use the shared fixed-256, warp-finish LayerNorm schedule.
#define ANTFLY_F16_ROW_NORM(Destination) \
__shared__ float sums[256], sumsq[256]; \
antfly_row_layer_norm<256,true>(dim,eps,sums,sumsq, \
    [&](unsigned i){return input[base+i];}, \
    [&](unsigned i,float x,float mean,float inverse){ \
        out[Destination+i]=__float2half_rn((x-mean)*inverse*gamma[i]+beta[i]); \
    });

// Same reduction tree and FP32 expression as termite_layer_norm_f32;
// only the final storage cast changes (the next GEMM already consumes FP16).
extern "C" __global__ void antfly_layer_norm_warp_finish_f16(
    __half* out, const float* input, const float* gamma, const float* beta,
    unsigned rows, unsigned dim, float eps) {
    const unsigned row = blockIdx.x;
    if (row >= rows) return;
    const unsigned base = row * dim;
    ANTFLY_F16_ROW_NORM(base)
}

extern "C" __global__ void florence_exact_gelu_f16(__half* out, const float* input, unsigned count) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const float x = input[i];
    const float value = isfinite(x) ? 0.5f*x*(1.f+erff(x*0.7071067811865476f)) : 0.f;
    out[i] = __float2half_rn(value);
}

// Decoder Q1 cross attention, exact FP32 storage/softmax. Cooperate across
// each contiguous 64-value key, then across four key partitions for the value
// reduction. This avoids the generic kernel's strided per-thread key loads.
extern "C" __global__ void florence_cross_q1_f32(
    float* out, const float* q, const float* k, const float* v,
    unsigned batch, unsigned sequence) {
    constexpr unsigned heads = 12, dim = 64, hidden = heads * dim;
    const unsigned head = blockIdx.x % heads;
    const unsigned page = blockIdx.x / heads;
    if (page >= batch) return;
    const unsigned tid = threadIdx.x;
    const unsigned lane = tid & 31;
    const unsigned warp = tid >> 5;
    __shared__ float scores[1024];
    __shared__ float scratch[256];
    const unsigned qb = page * hidden + head * dim;
    const float q0 = q[qb + lane], q1 = q[qb + lane + 32];
    float local_max = -3.402823466e38f;
    for (unsigned token = warp; token < sequence; token += 8) {
        const unsigned base = (page * sequence + token) * hidden + head * dim;
        float dot = q0 * k[base + lane];
        dot = __fmaf_rn(q1, k[base + lane + 32], dot);
        for (int distance = 16; distance; distance >>= 1)
            dot += __shfl_down_sync(0xffffffffu, dot, distance);
        if (lane == 0) {
            const float score = dot * 0.125f;
            scores[token] = score;
            local_max = fmaxf(local_max, score);
        }
    }
    scratch[tid] = local_max;
    __syncthreads();
    for (unsigned stride = 128; stride; stride >>= 1) {
        if (tid < stride) scratch[tid] = fmaxf(scratch[tid], scratch[tid+stride]);
        __syncthreads();
    }
    const float maximum = scratch[0];
    // Every warp must snapshot the maximum before scratch[0] is reused
    // for the denominator, even when warp scheduling is uneven.
    __syncthreads();
    float local_denom = 0.f;
    for (unsigned token = tid; token < sequence; token += 256) {
        const float e = expf(scores[token] - maximum);
        scores[token] = e;
        local_denom += e;
    }
    scratch[tid] = local_denom;
    __syncthreads();
    for (unsigned stride = 128; stride; stride >>= 1) {
        if (tid < stride) scratch[tid] += scratch[tid+stride];
        __syncthreads();
    }
    const float denominator = scratch[0];
    const unsigned channel = tid % dim;
    const unsigned partition = tid / dim;
    float value = 0.f;
    for (unsigned token = partition; token < sequence; token += 4) {
        const unsigned base = (page * sequence + token) * hidden + head * dim;
        value = __fmaf_rn(scores[token], v[base + channel], value);
    }
    __syncthreads();
    scratch[tid] = value;
    __syncthreads();
    if (tid < dim) {
        const float sum = ((scratch[tid] + scratch[tid+64]) + scratch[tid+128]) + scratch[tid+192];
        out[qb+tid] = denominator > 0.f ? sum / denominator : 0.f;
    }
}

extern "C" __global__ void antfly_window_layer_norm_warp_finish_f16(
    __half* out, const float* input, const float* gamma, const float* beta,
    unsigned rows, unsigned dim, float eps, unsigned height, unsigned width, unsigned window) {
    const unsigned row = blockIdx.x;
    if (row >= rows) return;
    const unsigned base = row * dim;
    // Input rows are page/y/x; destination rows are page/window_y/window_x/y/x.
    // Qualified shapes have no padding, so every half is written exactly once.
    const unsigned page = row / (height*width);
    const unsigned y = row / width % height, x = row % width;
    const unsigned window_row = (((page*(height/window)+y/window)*(width/window)+x/window)*window+y%window)*window+x%window;
    const unsigned destination = window_row * dim;
    ANTFLY_F16_ROW_NORM(destination)
}

// Fuse the existing FP32 QKV split with its three independent FP16 casts.
extern "C" __global__ void florence_split_qkv_f16(
    __half* q, __half* k, __half* v, const float* input,
    unsigned count, unsigned dim) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned source = (i/dim)*(3*dim)+i%dim;
    q[i] = __float2half_rn(input[source]);
    k[i] = __float2half_rn(input[source+dim]);
    v[i] = __float2half_rn(input[source+2*dim]);
}

// Append both rows and gather live page prefixes in one launch. Move raw F32
// bits; the KV cache and attention input retain their original precision.
extern "C" __global__ void florence_append_self_kv_f32(
    float* out_k, float* out_v, float* slab_k, float* slab_v,
    const float* next_k, const float* next_v,
    unsigned count, unsigned capacity, unsigned old_length, unsigned dim) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned row = i / dim;
    const unsigned channel = i % dim;
    const unsigned length = old_length + 1;
    const unsigned page = row / length;
    const unsigned token = row % length;
    const unsigned source = (page * capacity + token) * dim + channel;
    unsigned k, v;
    if (token == old_length) {
        const unsigned next = page * dim + channel;
        k = reinterpret_cast<const unsigned*>(next_k)[next];
        v = reinterpret_cast<const unsigned*>(next_v)[next];
        reinterpret_cast<unsigned*>(slab_k)[source] = k;
        reinterpret_cast<unsigned*>(slab_v)[source] = v;
    } else {
        k = reinterpret_cast<const unsigned*>(slab_k)[source];
        v = reinterpret_cast<const unsigned*>(slab_v)[source];
    }
    reinterpret_cast<unsigned*>(out_k)[i] = k;
    reinterpret_cast<unsigned*>(out_v)[i] = v;
}

#undef ANTFLY_F16_ROW_NORM
