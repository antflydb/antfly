// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

#include <cuda_bf16.h>
#include <math.h>
#include <stdint.h>
#include "../kernels/row_program.cuh"

using Bf16 = __nv_bfloat16;

__device__ float bf(Bf16 x) { return __bfloat162float(x); }
__device__ Bf16 rounded(float x) { return __float2bfloat16_rn(x); }

__device__ float row_sum(float value, float* warp_sums) {
    const unsigned tid = threadIdx.x, lane = tid % 32, warp = tid / 32;
    value = antfly_row_warp_sum(value);
    if (lane == 0) warp_sums[warp] = value;
    __syncthreads();
    return warp_sums[0] + warp_sums[1] + warp_sums[2] + warp_sums[3];
}

struct Bf16Planes4 { const Bf16* ptr[4]; };
struct Weights4 { float value[4]; };

extern "C" __global__ void antfly_weighted_four_rows_rms_bf16(
    Bf16* out, const __grid_constant__ Bf16Planes4 inputs,
    const __grid_constant__ Weights4 weights, const Bf16* gamma, unsigned width, float eps
) {
    const size_t base = (size_t)blockIdx.x * width;
    __shared__ float partials[4];
    __shared__ float mixed[4096];
    float squares = 0;
    for (unsigned i = threadIdx.x; i < width; i += 128) {
        float x = 0;
        #pragma unroll
        for (unsigned tap = 0; tap < 4; ++tap)
            x += weights.value[tap] * bf(inputs.ptr[tap][base + i]);
        mixed[i] = x;
        squares = __fmaf_rn(x, x, squares);
    }
    const float scale = rsqrtf(row_sum(squares, partials) / width + eps);
    for (unsigned i = threadIdx.x; i < width; i += 128)
        out[base + i] = rounded((mixed[i] * scale) * bf(gamma[i]));
}

extern "C" __global__ void antfly_residual_add_rms_bf16(
    Bf16* output, Bf16* residual, const Bf16* update, const Bf16* gamma, unsigned width, float eps
) {
    const size_t base = (size_t)blockIdx.x * width;
    __shared__ float partials[4];
    float squares = 0;
    __shared__ float added[4096];
    for (unsigned i = threadIdx.x; i < width; i += 128) {
        const float sum = bf(residual[base + i]) + bf(update[base + i]);
        residual[base + i] = rounded(sum);
        added[i] = sum;
        squares = __fmaf_rn(sum, sum, squares);
    }
    const float scale = rsqrtf(row_sum(squares, partials) / width + eps);
    for (unsigned i = threadIdx.x; i < width; i += 128)
        output[base + i] = rounded((added[i] * scale) * bf(gamma[i]));
}

extern "C" __global__ void antfly_head_rms_neox_rope_d128_bf16(
    Bf16* data, const Bf16* gamma, unsigned heads, unsigned start, float theta, float eps
) {
    const unsigned lane = threadIdx.x;
    const size_t offset = ((size_t)blockIdx.x * heads + blockIdx.y) * 128 + lane;
    float x = bf(data[offset]), y = bf(data[offset + 64]);
    const float total = antfly_row_warp_sum(__fmaf_rn(x, x, y * y));
    __shared__ float sums[2];
    if (lane == 0 || lane == 32) sums[lane / 32] = total;
    __syncthreads();
    const float scale = rsqrtf((sums[0] + sums[1]) / 128.0f + eps);
    x = bf(rounded((x * scale) * bf(gamma[lane])));
    y = bf(rounded((y * scale) * bf(gamma[lane + 64])));
    const float angle = (start + blockIdx.x) * powf(theta, -float(lane) / 64.0f);
    float sine, cosine;
    sincosf(angle, &sine, &cosine);
    data[offset] = rounded(x * cosine - y * sine);
    data[offset + 64] = rounded(y * cosine + x * sine);
}

extern "C" __global__ void antfly_swiglu_separate_bf16(
    Bf16* gate, const Bf16* up, unsigned values
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < values) {
        const float x = bf(gate[i]);
        gate[i] = rounded((x / (1.0f + expf(-x))) * bf(up[i]));
    }
}

// All queries see the same interval, including all of the block's own keys.
extern "C" __global__ void antfly_bshd_block_gqa_d128_bf16(
    Bf16* output, const Bf16* queries, const Bf16* keys, const Bf16* values,
    unsigned query_heads, unsigned kv_heads, unsigned key_count
) {
    constexpr unsigned key_tile = 32, dim = 128;
    const unsigned tid = threadIdx.x, lane = tid % 32, warp = tid / 32;
    const unsigned head = blockIdx.y, kv_head = head / (query_heads / kv_heads);
    const size_t qbase = ((size_t)blockIdx.x * query_heads + head) * dim;
    const unsigned stride = kv_heads * dim;
    float q[4];
    #pragma unroll
    for (unsigned d = 0; d < 4; ++d) q[d] = bf(queries[qbase + lane + 32 * d]);
    __shared__ float probabilities[key_tile];
    __shared__ float running_max, running_sum, old_scale;
    if (tid == 0) { running_max = -INFINITY; running_sum = 0; }
    float out = 0;
    for (unsigned first = 0; first < key_count; first += key_tile) {
        const unsigned valid = min(key_tile, key_count - first);
        for (unsigned item = warp; item < key_tile; item += 4) {
            float dot = 0;
            if (item < valid) {
                const size_t index = (size_t)(first + item) * stride + kv_head * dim + lane;
                #pragma unroll
                for (unsigned d = 0; d < 4; ++d)
                    dot = __fmaf_rn(q[d], bf(keys[index + d * 32]), dot);
            }
            dot = antfly_row_warp_sum(dot);
            if (lane == 0) probabilities[item] = item < valid ? dot * 0.08838834764831845f : -INFINITY;
        }
        __syncthreads();
        if (warp == 0) {
            const float score = probabilities[lane];
            float maximum = score;
            for (unsigned offset = 16; offset > 0; offset /= 2)
                maximum = fmaxf(maximum, __shfl_down_sync(0xffffffffu, maximum, offset));
            maximum = fmaxf(running_max, __shfl_sync(0xffffffffu, maximum, 0));
            const float weight = __expf(score - maximum);
            const float sum = antfly_row_warp_sum(weight);
            probabilities[lane] = weight;
            if (lane == 0) {
                old_scale = __expf(running_max - maximum);
                running_sum = __fmaf_rn(running_sum, old_scale, sum);
                running_max = maximum;
            }
        }
        __syncthreads();
        out *= old_scale;
        for (unsigned item = 0; item < valid; ++item) {
            const size_t index = (size_t)(first + item) * stride + kv_head * dim + tid;
            out = __fmaf_rn(bf(values[index]), probabilities[item], out);
        }
        __syncthreads();
    }
    output[qbase + tid] = rounded(out / running_sum);
}

extern "C" __global__ void antfly_gather_indexed_row_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* table, const uint32_t* index,
    unsigned row_count, unsigned width
) {
    const unsigned column = blockIdx.x * blockDim.x + threadIdx.x;
    if (column >= width) return;
    const unsigned row = *index;
    out[column] = row < row_count ? table[(size_t)row * width + column] : __float2bfloat16_rn(0.0f);
}

extern "C" __global__ void antfly_low_rank_recurrent_gate_bf16(
    Bf16* state, Bf16* latent, const float* raw, unsigned rank
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= rank) return;
    const float gain = bf(rounded(1.0f / (1.0f + expf(-bf(rounded(raw[i]))))));
    const float candidate = bf(rounded(tanhf(bf(rounded(raw[rank + i])))));
    const float keep = bf(rounded(gain * bf(state[i])));
    const float update = bf(rounded(bf(rounded(1.0f - gain)) * candidate));
    state[i] = rounded(keep + update);
    latent[i] = rounded(tanhf(bf(rounded(raw[2 * rank + i]))));
}

struct Winner { float score; unsigned token; };
__device__ Winner better(Winner a, Winner b) {
    return b.score > a.score || (b.score == a.score && b.token < a.token) ? b : a;
}
__device__ Winner warp_winner(Winner win) {
    for (unsigned offset = 16; offset > 0; offset /= 2) {
        Winner other{ __shfl_down_sync(0xffffffffu, win.score, offset),
                      __shfl_down_sync(0xffffffffu, win.token, offset) };
        win = better(win, other);
    }
    return win;
}
__device__ Winner block_winner(Winner win, Winner* warps) {
    const unsigned lane = threadIdx.x % 32;
    win = warp_winner(win);
    if (lane == 0) warps[threadIdx.x / 32] = win;
    __syncthreads();
    return warp_winner(lane < 8 ? warps[lane] : Winner{-INFINITY, UINT32_MAX});
}

extern "C" __global__ void antfly_softcapped_sum_argmax_bf16_tiles(
    Winner* out, const Bf16* base, const Bf16* bias, unsigned count, float cap
) {
    constexpr unsigned tile = 4096;
    const unsigned start = blockIdx.x * tile;
    Winner win{-INFINITY, UINT32_MAX};
    for (unsigned i = start + threadIdx.x; i < min(start + tile, count); i += 256) {
        const float sum = bf(rounded(bf(base[i]) + bf(bias[i])));
        const float score = bf(rounded(tanhf(sum / cap) * cap));
        if (isfinite(score)) win = better(win, {score, i});
    }
    __shared__ Winner warps[8];
    win = block_winner(win, warps);
    if (threadIdx.x == 0) out[blockIdx.x] = win;
}

extern "C" __global__ void antfly_softcapped_argmax_rows_f32_tiles(
    Winner* out, const float* logits, unsigned count, unsigned stride, float cap
) {
    constexpr unsigned tile = 4096;
    const unsigned start = blockIdx.x * tile;
    const float* row = logits + (size_t)blockIdx.y * stride;
    Winner win{-INFINITY, UINT32_MAX};
    for (unsigned i = start + threadIdx.x; i < min(start + tile, count); i += 256) {
        float score;
        asm("tanh.approx.f32 %0, %1;" : "=f"(score) : "f"(row[i] / cap));
        score *= cap;
        if (isfinite(score)) win = better(win, {score, i});
    }
    __shared__ Winner warps[8];
    win = block_winner(win, warps);
    if (threadIdx.x == 0) out[(size_t)blockIdx.y * gridDim.x + blockIdx.x] = win;
}

extern "C" __global__ void antfly_argmax_merge_tiles_f32(
    unsigned* out, const Winner* tiles, unsigned count
) {
    Winner win{-INFINITY, UINT32_MAX};
    for (unsigned i = threadIdx.x; i < count; i += 256) win = better(win, tiles[(size_t)blockIdx.y * count + i]);
    __shared__ Winner warps[8];
    win = block_winner(win, warps);
    if (threadIdx.x == 0) out[blockIdx.y] = win.token;
}
