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

#include <cuda_fp16.h>
#include <math.h>
#include <stdint.h>

struct LightOnAttentionPage {
  uint32_t tokens;
  uint32_t query_position;
  uint32_t block_count;
  uint32_t blocks[64];
};
struct LightOnAttentionParams {
  const float *q;
  const __half *k;
  const __half *v;
  float *out;
  float *partials;
  float *statistics;
  const float *new_k;
  const float *new_v;
  uint32_t items;
  uint32_t heads;
  uint32_t kv_heads;
  uint32_t chunks;
  uint32_t page_size;
  uint32_t row_values;
  uint32_t capacity;
  LightOnAttentionPage pages[64];
};
static_assert(sizeof(LightOnAttentionPage) == 268, "page ABI");
static_assert(sizeof(LightOnAttentionParams) == 17248, "packet ABI");

// Each page owns an exclusive writable tail. Host validation proves bounds,
// physical capacity and non-aliasing before a single suffix is changed.
extern "C" __global__ void lighton_attention_write_suffix(
    const __grid_constant__ LightOnAttentionParams p) {
  const uint32_t item = blockIdx.y;
  const uint32_t d = blockIdx.x * blockDim.x + threadIdx.x;
  if (item >= p.items || d >= p.row_values)
    return;
  const LightOnAttentionPage &page = p.pages[item];
  const uint32_t token = page.tokens - 1;
  const uint32_t physical =
      page.blocks[token / p.page_size] * p.page_size + token % p.page_size;
  const uint32_t destination = physical * p.row_values + d;
  const uint32_t source = item * p.row_values + d;
  const_cast<__half *>(p.k)[destination] = __float2half_rn(p.new_k[source]);
  const_cast<__half *>(p.v)[destination] = __float2half_rn(p.new_v[source]);
}

__device__ __forceinline__ float warp_sum(float x) {
  for (int offset = 16; offset; offset /= 2)
    x += __shfl_down_sync(0xffffffff, x, offset);
  return x;
}
__device__ __forceinline__ float warp_max(float x) {
  for (int offset = 16; offset; offset /= 2)
    x = fmaxf(x, __shfl_down_sync(0xffffffff, x, offset));
  return x;
}

// One warp owns each of four query heads sharing a KV head. Each lane
// preserves the original F32 dot-product reduction and value summation order.
template <unsigned Chunk>
__device__ __forceinline__ void
attention_chunks(const LightOnAttentionParams &p) {
  const unsigned item = blockIdx.y / p.chunks, chunk = blockIdx.y % p.chunks;
  const unsigned lane = threadIdx.x & 31, warp = threadIdx.x >> 5,
                 kh = blockIdx.x, head = kh * 4 + warp;
  if (item >= p.items || head >= p.heads || p.heads != p.kv_heads * 4 ||
      blockDim.x != 128)
    return;
  const auto &page = p.pages[item];
  __shared__ float scores[4][Chunk];
  float query[8];
#pragma unroll
  for (unsigned d = 0; d < 8; ++d)
    query[d] = p.q[(item * p.heads + head) * 256 + lane + d * 32];
#pragma unroll 1
  for (unsigned local = 0; local < Chunk; ++local) {
    unsigned key = chunk * Chunk + local, physical = p.capacity;
    if (key < page.tokens && key <= page.query_position &&
        key / p.page_size < page.block_count)
      physical =
          page.blocks[key / p.page_size] * p.page_size + key % p.page_size;
    float score = 0.f;
    if (physical < p.capacity) {
      unsigned base = physical * p.row_values + kh * 256;
#pragma unroll
      for (unsigned d = 0; d < 8; ++d)
        score += query[d] * __half2float(p.k[base + lane + d * 32]);
    }
    score = warp_sum(score) * 0.0625f;
    if (lane == 0)
      scores[warp][local] = physical < p.capacity ? score : -INFINITY;
  }
  __syncwarp();
  float maximum = -INFINITY;
#pragma unroll
  for (unsigned local = lane; local < Chunk; local += 32)
    maximum = fmaxf(maximum, scores[warp][local]);
  maximum = warp_max(maximum);
  maximum = __shfl_sync(0xffffffffu, maximum, 0);
  float denominator = 0.f;
#pragma unroll
  for (unsigned local = lane; local < Chunk; local += 32) {
    float e = isfinite(maximum) ? expf(scores[warp][local] - maximum) : 0.f;
    scores[warp][local] = e;
    denominator += e;
  }
  denominator = warp_sum(denominator);
  denominator = __shfl_sync(0xffffffffu, denominator, 0);
  __syncwarp();
  float values[8] = {};
#pragma unroll 1
  for (unsigned local = 0; local < Chunk; ++local) {
    unsigned key = chunk * Chunk + local;
    if (key < page.tokens && key <= page.query_position &&
        scores[warp][local] != 0.f) {
      unsigned physical =
          page.blocks[key / p.page_size] * p.page_size + key % p.page_size;
      if (physical < p.capacity) {
        unsigned base = physical * p.row_values + kh * 256;
#pragma unroll
        for (unsigned d = 0; d < 8; ++d)
          values[d] +=
              scores[warp][local] * __half2float(p.v[base + lane + d * 32]);
      }
    }
  }
  unsigned row = (item * p.heads + head) * p.chunks + chunk;
#pragma unroll
  for (unsigned d = 0; d < 8; ++d)
    p.partials[row * 256 + lane + d * 32] = values[d];
  if (lane == 0) {
    p.statistics[row * 2] = maximum;
    p.statistics[row * 2 + 1] = denominator;
  }
}

extern "C" __global__ void
lighton_attention_chunks(const __grid_constant__ LightOnAttentionParams p) {
  attention_chunks<32>(p);
}
extern "C" __global__ void
lighton_attention_chunks128(const __grid_constant__ LightOnAttentionParams p) {
  attention_chunks<128>(p);
}

extern "C" __global__ void
lighton_attention_combine(const __grid_constant__ LightOnAttentionParams p) {
  const uint32_t row = blockIdx.x;
  const uint32_t tid = threadIdx.x;
  if (row >= p.items * p.heads || blockDim.x != 128)
    return;
  __shared__ float maximum;
  __shared__ float denominator;
  __shared__ float scales[32];
  if (tid < 32) {
    float m =
        tid < p.chunks ? p.statistics[(row * p.chunks + tid) * 2] : -INFINITY;
    m = warp_max(m);
    if (tid == 0)
      maximum = m;
  }
  __syncthreads();
  if (tid < 32) {
    float sum = 0;
    if (tid < p.chunks) {
      const uint32_t index = (row * p.chunks + tid) * 2;
      const float scale = expf(p.statistics[index] - maximum);
      scales[tid] = scale;
      sum = scale * p.statistics[index + 1];
    }
    sum = warp_sum(sum);
    if (tid == 0)
      denominator = sum;
  }
  __syncthreads();
  float first = 0, second = 0;
  for (uint32_t chunk = 0; chunk < p.chunks; ++chunk) {
    const uint32_t base = (row * p.chunks + chunk) * 256;
    first += scales[chunk] * p.partials[base + tid];
    second += scales[chunk] * p.partials[base + tid + 128];
  }
  p.out[row * 256 + tid] = first / denominator;
  p.out[row * 256 + tid + 128] = second / denominator;
}
