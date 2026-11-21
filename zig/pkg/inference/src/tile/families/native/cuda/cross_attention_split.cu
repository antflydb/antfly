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

// native-program:entry termite_cross_split_hd64_f32
#include <cuda_fp16.h>
// Dense BSHD layout and KV dtype are static program properties. The key tile
// is 128 and its four warps retain the tuned two-lane dot/reduction order.
// No runtime dtype/layout switch occurs in the arithmetic body.
template<typename Kv> struct AntflyCrossKvLoad;
template<> struct AntflyCrossKvLoad<float> {
    __device__ static float get(const float* data, size_t index) { return data[index]; }
};
template<> struct AntflyCrossKvLoad<__half> {
    __device__ static float get(const __half* data, size_t index) { return __half2float(data[index]); }
};
struct AntflyCrossBshd64 {
    __device__ static size_t row(unsigned batch, unsigned token, unsigned tokens, unsigned head, unsigned heads) {
        return ((size_t(batch) * tokens + token) * heads + head) * 64;
    }
};
template<typename Kv, typename Layout, unsigned KeyTile>
__device__ void antfly_cross_split_hd64_impl(float* partial, const float* q,
    const Kv* k, const Kv* v, const long long* mask,
    unsigned queries, unsigned keys, unsigned heads, unsigned splits,
    float* scores, float* weights) {
    static_assert(KeyTile == 128, "qualified four-warp key schedule");
    const unsigned t = threadIdx.x, lane = t % 32, warp = t / 32;
    const unsigned h = blockIdx.x % heads, row = blockIdx.x / heads;
    const unsigned b = row / queries, qi = row % queries, start = blockIdx.y * KeyTile;
    const float* qr = q + Layout::row(b, qi, queries, h, heads);
    for (unsigned j = warp; j < KeyTile; j += 4) {
        const unsigned key = start + j;
        float dot = 0;
        if (key < keys && (!mask || mask[size_t(b) * keys + key])) {
            const size_t base = Layout::row(b, key, keys, h, heads);
            dot = qr[lane] * AntflyCrossKvLoad<Kv>::get(k, base + lane)
                + qr[lane + 32] * AntflyCrossKvLoad<Kv>::get(k, base + lane + 32);
            for (unsigned offset = 16; offset; offset >>= 1)
                dot += __shfl_down_sync(0xffffffff, dot, offset);
        }
        if (lane == 0)
            scores[j] = key < keys && (!mask || mask[size_t(b) * keys + key]) ? dot * 0.125f : -INFINITY;
    }
    __syncthreads();
    float maximum = scores[t];
    for (unsigned off = 16; off; off >>= 1)
        maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffff, maximum, off));
    if (lane == 0) weights[warp] = maximum;
    __syncthreads();
    maximum = fmaxf(fmaxf(weights[0], weights[1]), fmaxf(weights[2], weights[3]));
    float weight = isfinite(maximum) ? expf(scores[t] - maximum) : 0;
    __syncthreads();
    weights[t] = weight;
    __syncthreads();
    float* out = partial + (size_t(blockIdx.x) * splits + blockIdx.y) * 66;
    if (t < 64) {
        float sum = 0;
        for (unsigned j = 0; j < KeyTile && start + j < keys; ++j)
            sum += weights[j] * AntflyCrossKvLoad<Kv>::get(v, Layout::row(b, start + j, keys, h, heads) + t);
        out[t + 2] = sum;
    }
    for (unsigned off = 16; off; off >>= 1)
        weight += __shfl_down_sync(0xffffffff, weight, off);
    if (lane == 0) scores[warp] = weight;
    __syncthreads();
    if (t == 0) { out[0] = maximum; out[1] = scores[0] + scores[1] + scores[2] + scores[3]; }
}
extern "C" __global__ void termite_cross_split_hd64_f32(float* partial,
    const float* q, const float* k, const float* v, const long long* mask,
    unsigned queries, unsigned keys, unsigned heads, unsigned splits) {
    __shared__ float scores[128], weights[128];
    antfly_cross_split_hd64_impl<float, AntflyCrossBshd64, 128>(partial, q, k, v, mask,
        queries, keys, heads, splits, scores, weights);
}
// native-program:entry termite_cross_split_hd64_f16
extern "C" __global__ void termite_cross_split_hd64_f16(float* partial,
    const float* q, const __half* k, const __half* v, const long long* mask,
    unsigned queries, unsigned keys, unsigned heads, unsigned splits) {
    __shared__ float scores[128], weights[128];
    antfly_cross_split_hd64_impl<__half, AntflyCrossBshd64, 128>(partial, q, k, v, mask,
        queries, keys, heads, splits, scores, weights);
}
// native-program:entry termite_cross_split_hd64_merge_f32
extern "C" __global__ void termite_cross_split_hd64_merge_f32(float* out,
    const float* partial, unsigned splits) {
    const unsigned c = threadIdx.x;
    const float* base = partial + size_t(blockIdx.x) * splits * 66;
    float maximum = -INFINITY;
    for (unsigned s = 0; s < splits; ++s) maximum = fmaxf(maximum, base[s * 66]);
    float numerator = 0, denominator = 0;
    for (unsigned s = 0; s < splits; ++s) {
        const float* p = base + s * 66;
        const float scale = isfinite(p[0]) ? expf(p[0] - maximum) : 0;
        numerator += p[c + 2] * scale;
        denominator += p[1] * scale;
    }
    out[size_t(blockIdx.x) * 64 + c] = denominator > 0 ? numerator / denominator : 0;
}
