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

#include <cuda_bf16.h>
#include <math.h>
// Independent Welford implementation. Four contiguous BF16 values per lane,
// four warps, and a fixed reduction tree match the pinned reference geometry:
// https://github.com/pytorch/pytorch/blob/cf30153c4c131c8164ee7798e5022d810682e2cb/aten/src/ATen/native/cuda/layer_norm_kernel.cu
struct Moments {
  float avg, spread, items;
};
__device__ __forceinline__ Moments observe(Moments a, float x) {
  float n = a.items + 1.f, delta = x - a.avg;
  float mean = __fmaf_rn(delta, 1.f / n, a.avg);
  return {mean, __fmaf_rn(delta, x - mean, a.spread), n};
}
__device__ __forceinline__ Moments merge(Moments left, Moments right) {
  float n = left.items + right.items;
  if (n == 0.f)
    return {0.f, 0.f, 0.f};
  float inverse = 1.f / n, a = right.items * inverse, b = left.items * inverse,
        difference = left.avg - right.avg;
  return {__fmaf_rn(a, right.avg, b * left.avg),
          __fmaf_rn(difference * difference * right.items, b,
                    right.spread + left.spread),
          n};
}
__device__ __forceinline__ float element(uint2 p, unsigned j) {
  unsigned bits = j < 2 ? p.x : p.y;
  return __uint_as_float(((bits >> ((j & 1) * 16)) & 0xffffu) << 16);
}
extern "C" __global__ __launch_bounds__(128) void lighton_bf16_vision_norm(
    __nv_bfloat16 *out, const __nv_bfloat16 *input, const float *gamma,
    const float *beta, unsigned rows, unsigned dim, float eps) {
  unsigned row = blockIdx.x, thread = threadIdx.x, lane = thread & 31u,
           warp = thread >> 5;
  if (row >= rows)
    return;
  __shared__ float exchange[12];
  Moments values = {0.f, 0.f, 0.f};
  for (unsigned vector = thread; vector < dim / 4; vector += 128) {
    uint2 packed = reinterpret_cast<const uint2 *>(input + row * dim)[vector];
#pragma unroll
    for (unsigned j = 0; j < 4; ++j)
      values = observe(values, element(packed, j));
  }
  for (unsigned distance = 16; distance; distance >>= 1) {
    Moments other = {__shfl_down_sync(0xffffffffu, values.avg, distance),
                     __shfl_down_sync(0xffffffffu, values.spread, distance),
                     __shfl_down_sync(0xffffffffu, values.items, distance)};
    values = merge(values, other);
  }
  for (unsigned distance = 2; distance; distance >>= 1) {
    if (lane == 0 && warp >= distance && warp < 2 * distance) {
      unsigned slot = warp - distance;
      exchange[2 * slot] = values.avg;
      exchange[2 * slot + 1] = values.spread;
      exchange[4 + slot] = values.items;
    }
    __syncthreads();
    if (lane == 0 && warp < distance)
      values = merge(values, {exchange[2 * warp], exchange[2 * warp + 1],
                              exchange[4 + warp]});
    __syncthreads();
  }
  if (thread == 0) {
    exchange[0] = values.avg;
    exchange[1] = values.spread / float(dim);
  }
  __syncthreads();
  float average = exchange[0], inverse = rsqrtf(exchange[1] + eps);
  for (unsigned vector = thread; vector < dim / 4; vector += 128) {
    uint2 packed = reinterpret_cast<const uint2 *>(input + row * dim)[vector];
    float4 g = reinterpret_cast<const float4 *>(gamma)[vector],
           b = reinterpret_cast<const float4 *>(beta)[vector];
    float gains[4] = {g.x, g.y, g.z, g.w}, offsets[4] = {b.x, b.y, b.z, b.w};
    unsigned bits[4];
#pragma unroll
    for (unsigned j = 0; j < 4; ++j) {
      float value = __fmaf_rn((element(packed, j) - average) * inverse,
                              gains[j], offsets[j]);
      bits[j] = __bfloat16_as_ushort(__float2bfloat16_rn(value));
    }
    reinterpret_cast<uint2 *>(out + row * dim)[vector] =
        make_uint2(bits[0] | bits[1] << 16, bits[2] | bits[3] << 16);
  }
}

extern "C" __global__ void lighton_bf16_vision_add(__nv_bfloat16 *out,
                                                   const __nv_bfloat16 *a,
                                                   const __nv_bfloat16 *b,
                                                   unsigned count) {
  unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i < count)
    out[i] =
        __float2bfloat16_rn(__bfloat162float(a[i]) + __bfloat162float(b[i]));
}
extern "C" __global__ void lighton_bf16_vision_gelu(__nv_bfloat16 *out,
                                                    const __nv_bfloat16 *input,
                                                    unsigned count) {
  unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i >= count)
    return;
  float x = __bfloat162float(input[i]);
  float value =
      .5f * x * (1.f + tanhf(.7978845608028654f * (x + .044715f * x * x * x)));
  out[i] = __float2bfloat16_rn(value);
}

extern "C" __global__ void
lighton_bf16_vision_qkv_rope(__nv_bfloat16 *q, __nv_bfloat16 *k,
                             __nv_bfloat16 *v, const __nv_bfloat16 *packed,
                             const unsigned *positions, unsigned rows,
                             unsigned heads, unsigned dim, float theta) {
  const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
  const unsigned half = dim / 2;
  const unsigned pairs = rows * heads * half;
  if (i >= pairs)
    return;
  const unsigned chunk = i / half, pair = i % half;
  const unsigned token = chunk / heads, head = chunk % heads;
  const unsigned width = heads * dim;
  const unsigned axis_pairs = half / 2;
  const unsigned axis = pair < axis_pairs ? 0u : 1u;
  const unsigned local_pair = pair % axis_pairs;
  const unsigned position = positions[axis * rows + token];
  const float exponent = (2.f * (float)local_pair) / (float)(dim / 2u);
  const float frequency = 1.f / powf(theta, exponent);
  const float angle = (float)position * frequency;
  const float s = sinf(angle), c = cosf(angle);
  const unsigned src = token * 3 * width + head * dim + pair;
  const unsigned dst = chunk * dim + pair;
  const float ql = __bfloat162float(packed[src]),
              qr = __bfloat162float(packed[src + half]);
  const float kl = __bfloat162float(packed[src + width]),
              kr = __bfloat162float(packed[src + width + half]);
  q[dst] = __float2bfloat16_rn(ql * c - qr * s);
  q[dst + half] = __float2bfloat16_rn(ql * s + qr * c);
  k[dst] = __float2bfloat16_rn(kl * c - kr * s);
  k[dst + half] = __float2bfloat16_rn(kl * s + kr * c);
  v[dst] = __float2bfloat16_rn(__bfloat162float(packed[src + 2 * width]));
  v[dst + half] =
      __float2bfloat16_rn(__bfloat162float(packed[src + 2 * width + half]));
}
